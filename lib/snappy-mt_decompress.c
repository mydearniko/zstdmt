#include "snappy.h"
#include "snappy-mt.h"

#include "memmt.h"
#include "jobcontrol.h"
#include "threading.h"
#include "list.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define IN_ALLOC_SIZE (1024*1024)

/**
 * multi threaded snappy - multiple workers version
 *
 * - each thread works on his own
 * - no main thread which does reading and then starting the work
 * - needs a callback for reading / writing
 * - each worker does his:
 *   1) get read mutex and read some input
 *   2) release read mutex and do compression
 *   3) get write mutex and write result
 *   4) begin with step 1 again, until no input
 */

/* worker for compression */
typedef struct {
	SNAPPYMT_DCtx *ctx;
	pthread_t pthread;
	SNAPPYMT_Buffer in;
} cwork_t;

struct writelist {
	size_t frame;
	SNAPPYMT_Buffer out;
	struct list_head node;
};

static void *pt_decompress(void *arg);

struct SNAPPYMT_DCtx_s {

	/* threads: 1..SNAPPYMT_THREAD_MAX */
	int threads;

	/* should be used for read from input */
	size_t inputsize;

	/* statistic */
	size_t insize;
	size_t outsize;
	size_t curframe;
	size_t frames;

	/* threading */
	cwork_t *cwork;

	/* reading input */
	pthread_mutex_t read_mutex;
	fnRead *fn_read;
	void *arg_read;

	/* writing output */
	pthread_mutex_t write_mutex;
	fnWrite *fn_write;
	void *arg_write;

	/* worker lifecycle */
	MT_JobControl job;

	/* lists for writing queue */
	struct writelist *writelist;
	struct list_head writelist_free;
	struct list_head writelist_busy;
	struct list_head writelist_done;
};

/* **************************************
 * Decompression
 ****************************************/

SNAPPYMT_DCtx *SNAPPYMT_createDCtx(int threads, int inputsize)
{
	SNAPPYMT_DCtx *ctx;
	int t;
	int started = 0;

	/* allocate ctx */
	ctx = (SNAPPYMT_DCtx *) calloc(1, sizeof(SNAPPYMT_DCtx));
	if (!ctx)
		return 0;

	/* check threads value */
	if (threads < 1 || threads > SNAPPYMT_THREAD_MAX)
		goto err_job;

	/* setup ctx */
	ctx->threads = threads;
	ctx->insize = 0;
	ctx->outsize = 0;
	ctx->frames = 0;
	ctx->curframe = 0;

	/* will be used for single stream only */
	if (inputsize)
		ctx->inputsize = inputsize;
	else
		ctx->inputsize = 1024 * 64;	/* 64K buffer */

	pthread_mutex_init(&ctx->read_mutex, NULL);
	pthread_mutex_init(&ctx->write_mutex, NULL);
	MTJobControl_init(&ctx->job);

	INIT_LIST_HEAD(&ctx->writelist_free);
	INIT_LIST_HEAD(&ctx->writelist_busy);
	INIT_LIST_HEAD(&ctx->writelist_done);

	ctx->cwork = (cwork_t *) malloc(sizeof(cwork_t) * threads);
	if (!ctx->cwork)
		goto err_job;

	for (t = 0; t < threads; t++) {
		cwork_t *w = &ctx->cwork[t];
		w->in.allocated = 0;
		w->in.buf = NULL;
		w->in.size = 0;
		w->ctx = ctx;
	}

	ctx->writelist =
	    (struct writelist *)calloc((size_t)threads, sizeof(struct writelist));
	if (!ctx->writelist)
		goto err_cwork;

	for (t = 0; t < ctx->threads; t++) {
		struct writelist *wl = &ctx->writelist[t];
		wl->out.buf = NULL;
		wl->out.size = 0;
		wl->out.allocated = 0;
		list_add_tail(&wl->node, &ctx->writelist_free);
	}

	for (t = 0; t < ctx->threads; t++) {
		cwork_t *w = &ctx->cwork[t];
		if (pthread_create(&w->pthread, NULL, pt_decompress, w) != 0)
			goto err_threads;
		started++;
	}

	return ctx;

 err_threads:
	MTJobControl_shutdown(&ctx->job);
	while (started-- > 0)
		pthread_join(ctx->cwork[started].pthread, NULL);
	free(ctx->writelist);
 err_cwork:
	free(ctx->cwork);
 err_job:
	MTJobControl_destroy(&ctx->job);
	pthread_mutex_destroy(&ctx->write_mutex);
	pthread_mutex_destroy(&ctx->read_mutex);
	free(ctx);
	return 0;
}

/**
 * mt_error - return mt lib specific error code
 */
static size_t mt_error(int rv)
{
	switch (rv) {
	case -1:
		return MT_ERROR(read_fail);
	case -2:
		return MT_ERROR(canceled);
	case -3:
		return MT_ERROR(memory_allocation);
	}

	/* XXX, some catch all other errors */
	return MT_ERROR(read_fail);
}

/**
 * pt_write - queue for decompressed output
 */
static size_t pt_write(SNAPPYMT_DCtx * ctx, struct writelist *wl)
{
	struct list_head *entry;

	/* move the entry to the done list */
	list_move(&wl->node, &ctx->writelist_done);

    /* the entry isn't the currently needed, return...  */
    if (wl->frame != ctx->curframe)
		return 0;

 again:
	/* check, what can be written ... */
	list_for_each(entry, &ctx->writelist_done) {
		wl = list_entry(entry, struct writelist, node);
		if (wl->frame == ctx->curframe) {
			int rv = ctx->fn_write(ctx->arg_write, &wl->out);
			if (rv != 0)
				return mt_error(rv);
			ctx->outsize += wl->out.size;
			ctx->curframe++;
			list_move(entry, &ctx->writelist_free);
			pthread_cond_signal(&ctx->job.free_cond);
			goto again;
		}
	}

	return 0;
}

static void reset_writelists(SNAPPYMT_DCtx *ctx)
{
	pthread_mutex_lock(&ctx->write_mutex);
	while (!list_empty(&ctx->writelist_busy)) {
		struct list_head *entry = list_first(&ctx->writelist_busy);
		list_move(entry, &ctx->writelist_free);
	}
	while (!list_empty(&ctx->writelist_done)) {
		struct list_head *entry = list_first(&ctx->writelist_done);
		list_move(entry, &ctx->writelist_free);
	}
	pthread_cond_broadcast(&ctx->job.free_cond);
	pthread_mutex_unlock(&ctx->write_mutex);
}

/**
 * pt_read - read compressed output Verify header information
 */
static size_t pt_read(SNAPPYMT_DCtx *ctx, SNAPPYMT_Buffer *in, size_t *frame, 
                      size_t *uncompressed)
{
	unsigned char hdrbuf[16];
	SNAPPYMT_Buffer hdr;
	int rv;

	/* read skippable frame (12 or 16 bytes) */
	pthread_mutex_lock(&ctx->read_mutex);

	/* special case, first 4 bytes already read */
	if (ctx->frames == 0) {
		hdr.buf = hdrbuf + 4;
		hdr.size = 12;
		rv = ctx->fn_read(ctx->arg_read, &hdr);
		if (rv != 0) {
			pthread_mutex_unlock(&ctx->read_mutex);
			return mt_error(rv);
		}
		if (hdr.size != 12)
			goto error_read;
		hdr.buf = hdrbuf;
	} else {
		hdr.buf = hdrbuf;
		hdr.size = 16;
		rv = ctx->fn_read(ctx->arg_read, &hdr);
		if (rv != 0) {
			pthread_mutex_unlock(&ctx->read_mutex);
			return mt_error(rv);
		}
		/* eof reached ? */
		if (hdr.size == 0) {
			pthread_mutex_unlock(&ctx->read_mutex);
			in->size = 0;
			return 0;
		}
		if (hdr.size != 16)
			goto error_read;
		if (MEM_readLE32((unsigned char *)hdr.buf + 0) !=
		    SNAPPYMT_MAGIC_SKIPPABLE)
			goto error_data;
	}

	/* check header data */
	if (MEM_readLE32((unsigned char *)hdr.buf + 4) != 8)
		goto error_data;
	if (MEM_readLE16((unsigned char *)hdr.buf + 12) != SNAPPYMT_MAGICNUMBER)
		goto error_data;

	// /* get uncompressed size for output buffer */
	// {
	// 	U16 hintsize = MEM_readLE16((unsigned char *)hdr.buf + 14);
	// 	*uncompressed = hintsize << 16;
	// }

	ctx->insize += 16;
	/* read new inputsize */
	{
		size_t toRead = MEM_readLE32((unsigned char *)hdr.buf + 8);
		if (in->allocated < toRead) {
			/* need bigger input buffer */
			if (in->allocated)
				in->buf = realloc(in->buf, toRead);
			else
				in->buf = malloc(toRead);
			if (!in->buf)
				goto error_nomem;
			in->allocated = toRead;
		}

		in->size = toRead;
		rv = ctx->fn_read(ctx->arg_read, in);
		/* generic read failure! */
		if (rv != 0) {
			pthread_mutex_unlock(&ctx->read_mutex);
			return mt_error(rv);
		}
        // size_t output_length = 0;
        // if(snappy_validate_compressed_buffer((char *)in->buf, in->size) 
        //     != SNAPPY_OK){
        //         return MT_ERROR(data_error);
        // }
        snappy_uncompressed_length((char *)in->buf, in->size, uncompressed);
        //*uncompressed = output_length;
		/* needed more bytes! */
		if (in->size != toRead)
			goto error_data;

		ctx->insize += in->size;
	}
	*frame = ctx->frames++;
	pthread_mutex_unlock(&ctx->read_mutex);

	/* done, no error */
	return 0;

 error_data:
	pthread_mutex_unlock(&ctx->read_mutex);
	return MT_ERROR(data_error);
 error_read:
	pthread_mutex_unlock(&ctx->read_mutex);
	return MT_ERROR(read_fail);
 error_nomem:
	pthread_mutex_unlock(&ctx->read_mutex);
	return MT_ERROR(memory_allocation);
}

static void *pt_decompress(void *arg)
{
	cwork_t *w = (cwork_t *) arg;
	SNAPPYMT_Buffer *in = &w->in;
	SNAPPYMT_DCtx *ctx = w->ctx;
	unsigned generation = 0;

	while (MTJobControl_wait(&ctx->job, &generation)) {
		size_t result = 0;
		struct writelist *wl = NULL;
		int failed = 0;

		for (;;) {
			struct list_head *entry;
			SNAPPYMT_Buffer *out;
			int rv;

			if (MTJobControl_should_stop(&ctx->job))
				break;

			pthread_mutex_lock(&ctx->write_mutex);
			while (list_empty(&ctx->writelist_free)
			       && !MTJobControl_should_stop(&ctx->job))
				pthread_cond_wait(&ctx->job.free_cond,
						  &ctx->write_mutex);
			if (list_empty(&ctx->writelist_free)
			    && MTJobControl_should_stop(&ctx->job)) {
				pthread_mutex_unlock(&ctx->write_mutex);
				break;
			}
			entry = list_first(&ctx->writelist_free);
			wl = list_entry(entry, struct writelist, node);
			list_move(entry, &ctx->writelist_busy);
			pthread_mutex_unlock(&ctx->write_mutex);
			out = &wl->out;

			result = pt_read(ctx, in, &wl->frame, &out->size);
			if (SNAPPYMT_isError(result)) {
				failed = 1;
				break;
			}

			if (in->size == 0) {
				pthread_mutex_lock(&ctx->write_mutex);
				list_move(&wl->node, &ctx->writelist_free);
				pthread_cond_signal(&ctx->job.free_cond);
				pthread_mutex_unlock(&ctx->write_mutex);
				wl = NULL;
				break;
			}

			if (out->allocated < out->size) {
				void *buf = realloc(out->buf, out->size);
				if (!buf) {
					result = MT_ERROR(memory_allocation);
					failed = 1;
					break;
				}
				out->buf = buf;
				out->allocated = out->size;
			}

			rv = snappy_uncompress((char *)(in->buf), in->size,
					       (char *)(out->buf));

			if (rv != SNAPPY_OK) {
				result = MT_ERROR(frame_decompress);
				failed = 1;
				break;
			}

			pthread_mutex_lock(&ctx->write_mutex);
			result = pt_write(ctx, wl);
			pthread_mutex_unlock(&ctx->write_mutex);
			wl = NULL;
			if (SNAPPYMT_isError(result)) {
				failed = 1;
				break;
			}
		}

		if (failed) {
			if (wl) {
				pthread_mutex_lock(&ctx->write_mutex);
				list_move(&wl->node, &ctx->writelist_free);
				pthread_cond_signal(&ctx->job.free_cond);
				pthread_mutex_unlock(&ctx->write_mutex);
			}
			MTJobControl_set_result(&ctx->job, result);
		}
		MTJobControl_finish_worker(&ctx->job);
	}

	return 0;
}

size_t SNAPPYMT_decompressDCtx(SNAPPYMT_DCtx * ctx, SNAPPYMT_RdWr_t * rdwr)
{
	unsigned char buf[4]; // first frame SNAPPYMT_MAGIC_SKIPPABLE
	int rv;
	cwork_t *w = &ctx->cwork[0];
	SNAPPYMT_Buffer *in = &w->in;
	size_t result;

	if (!ctx)
		return MT_ERROR(compressionParameter_unsupported);

	/* init reading and writing functions */
	ctx->fn_read = rdwr->fn_read;
	ctx->fn_write = rdwr->fn_write;
	ctx->arg_read = rdwr->arg_read;
	ctx->arg_write = rdwr->arg_write;
	ctx->insize = 0;
	ctx->outsize = 0;
	ctx->frames = 0;
	ctx->curframe = 0;

	/* check for SNAPPYMT_MAGIC_SKIPPABLE  read the first frame
										   SNAPPYMT_MAGIC_SKIPPABLE*/
	in->buf = buf;
	in->size = 4;
	rv = ctx->fn_read(ctx->arg_read, in);
	if (rv != 0)
		return mt_error(rv);
	if (in->size != 4)
		return MT_ERROR(data_error);

	/* single threaded with unknown sizes */
	if (MEM_readLE32(buf) != SNAPPYMT_MAGIC_SKIPPABLE)
		return MT_ERROR(data_error);

	in->buf = 0;
	in->size = 0;

	MTJobControl_start(&ctx->job, ctx->threads);
	result = MTJobControl_wait_done(&ctx->job);
	reset_writelists(ctx);

	return result;
}

/* returns current uncompressed data size */
size_t SNAPPYMT_GetInsizeDCtx(SNAPPYMT_DCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->insize;
}

/* returns the current compressed data size */
size_t SNAPPYMT_GetOutsizeDCtx(SNAPPYMT_DCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->outsize;
}

/* returns the current compressed frames */
size_t SNAPPYMT_GetFramesDCtx(SNAPPYMT_DCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->curframe;
}

void SNAPPYMT_freeDCtx(SNAPPYMT_DCtx * ctx)
{
	int t;

	if (!ctx)
		return;

	MTJobControl_shutdown(&ctx->job);
	for (t = 0; t < ctx->threads; t++)
		pthread_join(ctx->cwork[t].pthread, NULL);
	for (t = 0; t < ctx->threads; t++)
		free(ctx->cwork[t].in.buf);
	for (t = 0; t < ctx->threads; t++)
		free(ctx->writelist[t].out.buf);
	free(ctx->writelist);
	MTJobControl_destroy(&ctx->job);
	pthread_mutex_destroy(&ctx->read_mutex);
	pthread_mutex_destroy(&ctx->write_mutex);
	free(ctx->cwork);
	free(ctx);
}

// /* API example */
// static int ReadData(void *arg, SNAPPYMT_Buffer * in)
// {
// 	FILE *fd = (FILE *) arg;
// 	size_t done = fread(in->buf, 1, in->size, fd);
// 	in->size = done;

// 	return 0;
// }

// static int WriteData(void *arg, SNAPPYMT_Buffer * out)
// {
// 	FILE *fd = (FILE *) arg;
// 	ssize_t done = fwrite(out->buf, 1, out->size, fd);
// 	out->size = done;

// 	return 0;
// }


// int main(int argc, char *argv[]){

//     FILE *fin, *fout;
//     fin = fopen(argv[1], "r");
//     if(!fin){
//         std::cout << "fin open faild!" << std::endl;
//     }
//     fout = fopen(argv[2], "wb");
//     if(!fout){
//         std::cout << "fout open faild!" << std::endl;
//     }

//     SNAPPYMT_RdWr_t rdwr;
// 	/* 1) setup read/write functions */
// 	rdwr.fn_read = ReadData;
// 	rdwr.fn_write = WriteData;
// 	rdwr.arg_read = (void *)fin;
// 	rdwr.arg_write = (void *)fout;

// 	SNAPPYMT_DCtx *dctx = SNAPPYMT_createDCtx(2, 0);
// 	if (!dctx){
// 		std::cout << "Allocating compression context failed!" << std::endl;
// 		return -1;
// 	}

// 	size_t ret = SNAPPYMT_decompressDCtx(dctx, &rdwr);
// 	if (SNAPPYMT_isError(ret)){
// 		std::cout << SNAPPYMT_getErrorString(ret) << std::endl;
// 		return -1;
// 	}

// 	SNAPPYMT_freeDCtx(dctx);

//     fclose(fin);
//     fclose(fout);

//     return 0;
// }
