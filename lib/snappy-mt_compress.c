#include "snappy.h"
#include "snappy-mt.h"

#include "memmt.h"
#include "jobcontrol.h"
#include "threading.h"
#include "list.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SNAPPY_IN_ALLOC_SIZE (1024*64)

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

typedef struct {
	SNAPPYMT_CCtx *ctx;
	struct snappy_env zpref;
	pthread_t pthread;
} cwork_t;

struct writelist {
	size_t frame;
	SNAPPYMT_Buffer out;
	struct list_head node;
};

static void *pt_compress(void *arg);


struct SNAPPYMT_CCtx_s {

	/* levels: 1..SNAPPYMT NOT USE  DELETE level maybe later*/
	int level;

	/* threads: 1..SNAPPYMT_THREAD_MAX */
	int threads;

	/* should be used for read from input */
	int inputsize;

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
 * Compression
 ****************************************/

SNAPPYMT_CCtx *SNAPPYMT_createCCtx(int threads, __attribute__((unused)) int level,/*Not use*/ 
								   int inputsize)
{
	SNAPPYMT_CCtx *ctx;
	int t;
	int started = 0;

	/* allocate ctx */
	ctx = (SNAPPYMT_CCtx *) calloc(1, sizeof(SNAPPYMT_CCtx));
	if (!ctx)
		return 0;

	/* check threads value */
	if (threads < 1 || threads > SNAPPYMT_THREAD_MAX)
		goto err_job;

	/* check level */
	/* None level */

	/* calculate chunksize for one thread */
	if (inputsize)
		ctx->inputsize = inputsize;
	else
		ctx->inputsize = SNAPPY_IN_ALLOC_SIZE;  /* 64K frame */

	/* setup ctx */
	ctx->level = 0; 
	ctx->threads = threads;
	ctx->insize = 0;
	ctx->outsize = 0;
	ctx->frames = 0;
	ctx->curframe = 0;

	pthread_mutex_init(&ctx->read_mutex, NULL);
	pthread_mutex_init(&ctx->write_mutex, NULL);
	MTJobControl_init(&ctx->job);

	/* free -> busy -> out -> free -> ... */
	INIT_LIST_HEAD(&ctx->writelist_free);	/* free, can be used */
	INIT_LIST_HEAD(&ctx->writelist_busy);	/* busy */
	INIT_LIST_HEAD(&ctx->writelist_done);	/* can be written */

	ctx->cwork = (cwork_t *) malloc(sizeof(cwork_t) * threads);
	if (!ctx->cwork)
		goto err_job;

	for (t = 0; t < threads; t++) {
		cwork_t *w = &ctx->cwork[t];
		w->ctx = ctx;
		if (snappy_init_env(&w->zpref) != 0)
			goto err_env;

	}

	ctx->writelist =
	    (struct writelist *)calloc((size_t)threads, sizeof(struct writelist));
	if (!ctx->writelist)
		goto err_env;

	for (t = 0; t < ctx->threads; t++) {
		struct writelist *wl = &ctx->writelist[t];
		wl->out.buf = 0;
		wl->out.size = 0;
		wl->out.allocated = 0;
		list_add_tail(&wl->node, &ctx->writelist_free);
	}

	for (t = 0; t < ctx->threads; t++) {
		cwork_t *w = &ctx->cwork[t];
		if (pthread_create(&w->pthread, NULL, pt_compress, w) != 0)
			goto err_threads;
		started++;
	}

	return ctx;

 err_threads:
	MTJobControl_shutdown(&ctx->job);
	while (started-- > 0)
		pthread_join(ctx->cwork[started].pthread, NULL);
	free(ctx->writelist);
 err_env:
	while (t-- > 0)
		snappy_free_env(&ctx->cwork[t].zpref);
	free(ctx->cwork);
 err_job:
	MTJobControl_destroy(&ctx->job);
	pthread_mutex_destroy(&ctx->write_mutex);
	pthread_mutex_destroy(&ctx->read_mutex);
	free(ctx);

	return NULL;
}

/**
 * mt_error - return mt lib specific error code read write ERROR
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

	return MT_ERROR(read_fail);
}

/**
 * pt_write - queue for compressed output
 */
static size_t pt_write(SNAPPYMT_CCtx *ctx, struct writelist *wl)
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

static void reset_writelists(SNAPPYMT_CCtx *ctx)
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

static void *pt_compress(void *arg)
{
	cwork_t *w = (cwork_t *) arg;
	SNAPPYMT_CCtx *ctx = w->ctx;
	size_t result;
	SNAPPYMT_Buffer in;
	unsigned generation = 0;
	const size_t max_out =
	    snappy_max_compressed_length((size_t)ctx->inputsize) + 16;

	/* inbuf is constant */
	in.size = ctx->inputsize;
	in.buf = malloc(in.size);
	if (!in.buf) {
		MTJobControl_set_result(&ctx->job,
					MT_ERROR(memory_allocation));
		return 0;
	}

	while (MTJobControl_wait(&ctx->job, &generation)) {
		struct list_head *entry;
		struct writelist *wl;
		int rv;
		int failed = 0;

		for (;;) {
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

			if (wl->out.allocated < max_out) {
				void *buf = realloc(wl->out.buf, max_out);
				if (!buf) {
					result = MT_ERROR(memory_allocation);
					failed = 1;
					break;
				}
				wl->out.buf = buf;
				wl->out.allocated = max_out;
			}
			wl->out.size = max_out;

			/* read new input */
			pthread_mutex_lock(&ctx->read_mutex);
			in.size = ctx->inputsize;
			rv = ctx->fn_read(ctx->arg_read, &in);
			if (rv != 0) {
				pthread_mutex_unlock(&ctx->read_mutex);
				result = mt_error(rv);
				failed = 1;
				break;
			}

			/* eof */
			if (in.size == 0 && ctx->frames > 0) {
				pthread_mutex_unlock(&ctx->read_mutex);

				pthread_mutex_lock(&ctx->write_mutex);
				list_move(&wl->node, &ctx->writelist_free);
				pthread_cond_signal(&ctx->job.free_cond);
				pthread_mutex_unlock(&ctx->write_mutex);
				break;
			}
			ctx->insize += in.size;
			wl->frame = ctx->frames++;
			pthread_mutex_unlock(&ctx->read_mutex);

			/* compress whole frame */
			{
				const char *ibuf = (char *)(in.buf);
				char *obuf = (char *)(wl->out.buf) + 16;
				wl->out.size -= 16;

				rv =
				    snappy_compress(&w->zpref, ibuf, in.size,
						    obuf, &wl->out.size);
				if (rv != SNAPPY_OK) {
					result = MT_ERROR(frame_compress);
					failed = 1;
					break;
				}
			}

			/* write skippable frame */
			MEM_writeLE32((unsigned char *)wl->out.buf + 0,
				      SNAPPYMT_MAGIC_SKIPPABLE);
			MEM_writeLE32((unsigned char *)wl->out.buf + 4, 8);
			MEM_writeLE32((unsigned char *)wl->out.buf + 8,
				      (U32) wl->out.size);
			MEM_writeLE16((unsigned char *)wl->out.buf + 12,
				      (U16) SNAPPYMT_MAGICNUMBER);

			/* number of 64KB blocks needed for decompression */
			{
			U16 hintsize;
			if (ctx->inputsize > (int)in.size) {
				hintsize = (U16)(in.size >> 16);
				hintsize += 1;
			} else
				hintsize = ctx->inputsize >> 16;
			MEM_writeLE16((unsigned char *)wl->out.buf + 14,
				      hintsize);
			}

			wl->out.size += 16;

			/* write result */
			pthread_mutex_lock(&ctx->write_mutex);
			result = pt_write(ctx, wl);
			pthread_mutex_unlock(&ctx->write_mutex);
			if (SNAPPYMT_isError(result)) {
				failed = 1;
				break;
			}
		}

		if (failed) {
			pthread_mutex_lock(&ctx->write_mutex);
			list_move(&wl->node, &ctx->writelist_free);
			pthread_cond_signal(&ctx->job.free_cond);
			pthread_mutex_unlock(&ctx->write_mutex);
			MTJobControl_set_result(&ctx->job, result);
		}
		MTJobControl_finish_worker(&ctx->job);
	}

	free(in.buf);
	return 0;
}

size_t SNAPPYMT_compressCCtx(SNAPPYMT_CCtx *ctx, SNAPPYMT_RdWr_t *rdwr)
{
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

	MTJobControl_start(&ctx->job, ctx->threads);
	result = MTJobControl_wait_done(&ctx->job);
	reset_writelists(ctx);

	return result;
}

/* returns current uncompressed data size */
size_t SNAPPYMT_GetInsizeCCtx(SNAPPYMT_CCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->insize;
}

/* returns the current compressed data size */
size_t SNAPPYMT_GetOutsizeCCtx(SNAPPYMT_CCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->outsize;
}

/* returns the current compressed frames */
size_t SNAPPYMT_GetFramesCCtx(SNAPPYMT_CCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->curframe;
}

void SNAPPYMT_freeCCtx(SNAPPYMT_CCtx * ctx)
{
	int t;

	if (!ctx)
		return;

	MTJobControl_shutdown(&ctx->job);
	for (t = 0; t < ctx->threads; t++)
		pthread_join(ctx->cwork[t].pthread, NULL);
	for (t = 0; t < ctx->threads; t++) {
		free(ctx->writelist[t].out.buf);
		snappy_free_env(&ctx->cwork[t].zpref);
	}
	free(ctx->writelist);
	MTJobControl_destroy(&ctx->job);
	pthread_mutex_destroy(&ctx->read_mutex);
	pthread_mutex_destroy(&ctx->write_mutex);
	free(ctx->cwork);
	free(ctx);
	ctx = 0;

	return;
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

// 	SNAPPYMT_RdWr_t rdwr;
// 	/* 1) setup read/write functions */
// 	rdwr.fn_read = ReadData;
// 	rdwr.fn_write = WriteData;
// 	rdwr.arg_read = (void *)fin;
// 	rdwr.arg_write = (void *)fout;

// 	SNAPPYMT_CCtx *cctx = SNAPPYMT_createCCtx(2, 0);
// 	if (!cctx){
// 		std::cout << "Allocating compression context failed!" << std::endl;
// 		return -1;
// 	}

// 	size_t ret = SNAPPYMT_compressCCtx(cctx, &rdwr);
// 	if (SNAPPYMT_isError(ret)){
// 		std::cout << SNAPPYMT_getErrorString(ret) << std::endl;
// 		return -1;
// 	}

// 	SNAPPYMT_freeCCtx(cctx);

//     fclose(fin);
//     fclose(fout);


//     return 0;
// }
