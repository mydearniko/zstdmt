#include "lzfse.h"
#include "lzfse-mt.h"

#include "memmt.h"
#include "jobcontrol.h"
#include "threading.h"
#include "list.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define IN_ALLOC_SIZE (1024*1024)

/**
 * multi threaded lzfse - multiple workers version
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
	LZFSEMT_DCtx *ctx;
	pthread_t pthread;
	LZFSEMT_Buffer in;
	void *scratch;
} cwork_t;

struct writelist {
	size_t frame;
	LZFSEMT_Buffer out;
	struct list_head node;
};

static void *pt_decompress(void *arg);

struct LZFSEMT_DCtx_s {

	/* threads: 1..LZFSEMT_THREAD_MAX */
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

LZFSEMT_DCtx *LZFSEMT_createDCtx(int threads, int inputsize)
{
	LZFSEMT_DCtx *ctx;
	int t;
	int started = 0;
	size_t scratch_size = lzfse_decode_scratch_size() + 1;

	/* allocate ctx */
	ctx = (LZFSEMT_DCtx *) calloc(1, sizeof(LZFSEMT_DCtx));
	if (!ctx)
		return 0;

	/* check threads value */
	if (threads < 1 || threads > LZFSEMT_THREAD_MAX)
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
		w->scratch = malloc(scratch_size);
		if (!w->scratch)
			goto err_cwork;
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
	for (t = 0; t < threads; t++) {
		if (ctx->cwork)
			free(ctx->cwork[t].scratch);
	}
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
static size_t pt_write(LZFSEMT_DCtx * ctx, struct writelist *wl)
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

static void reset_writelists(LZFSEMT_DCtx *ctx)
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
static size_t pt_read(LZFSEMT_DCtx *ctx, LZFSEMT_Buffer *in, size_t *frame, 
                      size_t *uncompressed)
{
	unsigned char hdrbuf[16];
	LZFSEMT_Buffer hdr;
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
		    LZFSEMT_MAGIC_SKIPPABLE)
			goto error_data;
	}

	/* check header data */
	if (MEM_readLE32((unsigned char *)hdr.buf + 4) != 8)
		goto error_data;
	if (MEM_readLE16((unsigned char *)hdr.buf + 12) != LZFSEMT_MAGICNUMBER)
		goto error_data;

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

		/* get uncompressed size for output buffer. */
        {
			U16 hintsize = MEM_readLE16((unsigned char *)hdr.buf + 14);
			*uncompressed = hintsize << 16;
		}

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
	LZFSEMT_Buffer *in = &w->in;
	LZFSEMT_DCtx *ctx = w->ctx;
	unsigned generation = 0;

	while (MTJobControl_wait(&ctx->job, &generation)) {
		size_t result = 0;
		struct writelist *wl = NULL;
		int failed = 0;

		for (;;) {
			struct list_head *entry;
			LZFSEMT_Buffer *out;
			size_t realsize;

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
			if (LZFSEMT_isError(result)) {
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

			realsize =
			    lzfse_decode_buffer(out->buf, out->size, in->buf,
						in->size, w->scratch);
			if (realsize == 0) {
				result = MT_ERROR(frame_decompress);
				failed = 1;
				break;
			}

			out->size = realsize;
			pthread_mutex_lock(&ctx->write_mutex);
			result = pt_write(ctx, wl);
			pthread_mutex_unlock(&ctx->write_mutex);
			wl = NULL;
			if (LZFSEMT_isError(result)) {
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

size_t LZFSEMT_decompressDCtx(LZFSEMT_DCtx * ctx, LZFSEMT_RdWr_t * rdwr)
{
	unsigned char buf[4]; // first frame LZFSEMT_MAGIC_SKIPPABLE
	int rv;
	cwork_t *w = &ctx->cwork[0];
	LZFSEMT_Buffer *in = &w->in;
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

	/* check for LZFSEMT_MAGIC_SKIPPABLE  read the first frame
										   LZFSEMT_MAGIC_SKIPPABLE*/
	in->buf = buf;
	in->size = 4;
	rv = ctx->fn_read(ctx->arg_read, in);
	if (rv != 0)
		return mt_error(rv);
	if (in->size != 4)
		return MT_ERROR(data_error);

	/* single threaded with unknown sizes */
	if (MEM_readLE32(buf) != LZFSEMT_MAGIC_SKIPPABLE)
		return MT_ERROR(data_error);

	in->buf = 0;
	in->size = 0;

	MTJobControl_start(&ctx->job, ctx->threads);
	result = MTJobControl_wait_done(&ctx->job);
	reset_writelists(ctx);

	return result;
}

/* returns current uncompressed data size */
size_t LZFSEMT_GetInsizeDCtx(LZFSEMT_DCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->insize;
}

/* returns the current compressed data size */
size_t LZFSEMT_GetOutsizeDCtx(LZFSEMT_DCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->outsize;
}

/* returns the current compressed frames */
size_t LZFSEMT_GetFramesDCtx(LZFSEMT_DCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->curframe;
}

void LZFSEMT_freeDCtx(LZFSEMT_DCtx * ctx)
{
	int t;

	if (!ctx)
		return;

	MTJobControl_shutdown(&ctx->job);
	for (t = 0; t < ctx->threads; t++)
		pthread_join(ctx->cwork[t].pthread, NULL);
	for (t = 0; t < ctx->threads; t++) {
		free(ctx->cwork[t].in.buf);
		free(ctx->cwork[t].scratch);
	}
	for (t = 0; t < ctx->threads; t++)
		free(ctx->writelist[t].out.buf);
	free(ctx->writelist);
	MTJobControl_destroy(&ctx->job);
	pthread_mutex_destroy(&ctx->read_mutex);
	pthread_mutex_destroy(&ctx->write_mutex);
	free(ctx->cwork);
	free(ctx);
}
