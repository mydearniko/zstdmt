
/**
 * Copyright (c) 2016 - 2017 Tino Reichardt
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 *
 * You can contact the author at:
 * - zstdmt source repository: https://github.com/mcmilk/zstdmt
 */

#include <stdio.h>
#include <stdlib.h>

#define ZSTD_STATIC_LINKING_ONLY
#include "zstd.h"

#include "memmt.h"
#include "jobcontrol.h"
#include "threading.h"
#include "list.h"
#include "zstd-mt.h"

/**
 * multi threaded zstd compression
 *
 * - each thread works on his own
 * - needs a callback for reading / writing
 * - each worker does this:
 *   1) get read mutex and read some input
 *   2) release read mutex and do compression
 *   3) get write mutex and write result
 *   4) begin with step 1 again, until no input
 */

/* worker for compression */
typedef struct {
	ZSTDCB_CCtx *ctx;
	pthread_t pthread;
} cwork_t;

struct writelist;
struct writelist {
	size_t frame;
	ZSTDCB_Buffer out;
	struct list_head node;
};

static void *pt_compress(void *arg);

struct ZSTDCB_CCtx_s {

	/* level: 1..ZSTDCB_LEVEL_MAX */
	int level;

	/* threads: 1..ZSTDCB_THREAD_MAX */
	int threads;

	/* buffersize for reading input */
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
	fn_read *fn_read;
	void *arg_read;

	/* writing output */
	pthread_mutex_t write_mutex;
	fn_write *fn_write;
	void *arg_write;

	/* error handling */
	pthread_mutex_t error_mutex;
	size_t zstdmt_errcode;

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

ZSTDCB_CCtx *ZSTDCB_createCCtx(int threads, int level, int inputsize)
{
	ZSTDCB_CCtx *ctx;
	int t;
	int started = 0;

	/* allocate ctx */
	ctx = (ZSTDCB_CCtx *) calloc(1, sizeof(ZSTDCB_CCtx));
	if (!ctx)
		return 0;

	/* check threads value */
	if (threads < 1 || threads > ZSTDCB_THREAD_MAX)
		goto err_job;

	/* check level */
	if (level < ZSTDCB_LEVEL_MIN || level > ZSTDCB_LEVEL_MAX)
		goto err_job;




	/* calculate chunksize for one thread */
	if (inputsize)
		ctx->inputsize = inputsize;
	else {
		const int windowLog[] = {
			19, 19, 20, 20, 20, /*  1 -  5 */
			21, 21, 21, 21, 21, /*  6 - 10 */
			22, 22, 22, 22, 22, /* 11 - 15 */
			23, 23, 23, 23, 25, /* 16 - 20 */
			26, 27
		};
		ctx->inputsize = 1 << (windowLog[level] + 1);
	}

	/* setup ctx */
	ctx->level = level;
	ctx->threads = threads;

	pthread_mutex_init(&ctx->read_mutex, NULL);
	pthread_mutex_init(&ctx->write_mutex, NULL);
	pthread_mutex_init(&ctx->error_mutex, NULL);
	MTJobControl_init(&ctx->job);

	INIT_LIST_HEAD(&ctx->writelist_free);
	INIT_LIST_HEAD(&ctx->writelist_busy);
	INIT_LIST_HEAD(&ctx->writelist_done);

	ctx->cwork = (cwork_t *) malloc(sizeof(cwork_t) * threads);
	if (!ctx->cwork)
		goto err_job;

	for (t = 0; t < ctx->threads; t++) {
		cwork_t *w = &ctx->cwork[t];
		w->ctx = ctx;
	}

	ctx->writelist =
	    (struct writelist *)calloc((size_t)threads, sizeof(struct writelist));
	if (!ctx->writelist)
		goto err_cwork;

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
 err_cwork:
	free(ctx->cwork);
 err_job:
	MTJobControl_destroy(&ctx->job);
	pthread_mutex_destroy(&ctx->error_mutex);
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
		return ZSTDCB_ERROR(read_fail);
	case -2:
		return ZSTDCB_ERROR(canceled);
	case -3:
		return ZSTDCB_ERROR(memory_allocation);
	}

	return ZSTDCB_ERROR(read_fail);
}

/**
 * pt_write - queue for compressed output
 */
static size_t pt_write(ZSTDCB_CCtx * ctx, struct writelist *wl)
{
	struct list_head *entry;
	int rv;

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
			rv = ctx->fn_write(ctx->arg_write, &wl->out);
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

static void reset_writelists(ZSTDCB_CCtx *ctx)
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

/* parallel compression worker */
static void *pt_compress(void *arg)
{
	cwork_t *w = (cwork_t *) arg;
	ZSTDCB_CCtx *ctx = w->ctx;
	struct writelist *wl;
	size_t result;
	ZSTDCB_Buffer in;
	unsigned generation = 0;
	ZSTD_CCtx *zctx;
	const size_t max_out = ZSTD_compressBound(ctx->inputsize) + 12;

	/* inbuf is constant */
	in.size = ctx->inputsize;
	in.buf = malloc(in.size);
	if (!in.buf) {
		MTJobControl_set_result(&ctx->job,
					ZSTDCB_ERROR(memory_allocation));
		return 0;
	}
	zctx = ZSTD_createCCtx();
	if (!zctx) {
		free(in.buf);
		MTJobControl_set_result(&ctx->job,
					ZSTDCB_ERROR(memory_allocation));
		return 0;
	}

	while (MTJobControl_wait(&ctx->job, &generation)) {
		struct list_head *entry;
		ZSTDCB_Buffer *out;
		int rv;
		int failed = 0;

		for (;;) {
			wl = 0;
			result = 0;
			if (MTJobControl_should_stop(&ctx->job))
				break;

			/* allocate space for new output */
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

			if (out->allocated < max_out) {
				void *buf = realloc(out->buf, max_out);
				if (!buf) {
					result = ZSTDCB_ERROR(memory_allocation);
					failed = 1;
					break;
				}
				out->buf = buf;
				out->allocated = max_out;
			}
			out->size = max_out;

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
				unsigned char *outbuf = out->buf;
				result =
				    ZSTD_compressCCtx(zctx, outbuf + 12,
						      out->size - 12, in.buf,
						      in.size, ctx->level);
				if (ZSTD_isError(result)) {
					zstdmt_errcode = result;
					result =
					    ZSTDCB_ERROR
					    (compression_library);
					failed = 1;
					break;
				}
			}
			if (failed)
				break;

			/* write skippable frame */
			{
				unsigned char *outbuf = out->buf;

				MEM_writeLE32(outbuf + 0,
					      ZSTDCB_MAGIC_SKIPPABLE);
				MEM_writeLE32(outbuf + 4, 4);
				MEM_writeLE32(outbuf + 8, (U32) result);
				out->size = result + 12;
			}

			/* write result */
			pthread_mutex_lock(&ctx->write_mutex);
			result = pt_write(ctx, wl);
			pthread_mutex_unlock(&ctx->write_mutex);
			if (ZSTDCB_isError(result)) {
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

	ZSTD_freeCCtx(zctx);
	free(in.buf);
	return 0;
}

/* compress data, until input ends */
size_t ZSTDCB_compressCCtx(ZSTDCB_CCtx * ctx, ZSTDCB_RdWr_t * rdwr)
{
	size_t result;

	if (!ctx)
		return ZSTDCB_ERROR(init_missing);

	/* setup reading and writing functions */
	ctx->fn_read = rdwr->fn_read;
	ctx->fn_write = rdwr->fn_write;
	ctx->arg_read = rdwr->arg_read;
	ctx->arg_write = rdwr->arg_write;

	/* init counter and error codes */
	ctx->insize = 0;
	ctx->outsize = 0;
	ctx->frames = 0;
	ctx->curframe = 0;
	ctx->zstdmt_errcode = 0;
	MTJobControl_start(&ctx->job, ctx->threads);
	result = MTJobControl_wait_done(&ctx->job);
	reset_writelists(ctx);

	return result;
}

/* returns current uncompressed data size */
size_t ZSTDCB_GetInsizeCCtx(ZSTDCB_CCtx * ctx)
{
	if (!ctx)
		return ZSTDCB_ERROR(init_missing);

	/* no mutex needed here */
	return ctx->insize;
}

/* returns the current compressed data size */
size_t ZSTDCB_GetOutsizeCCtx(ZSTDCB_CCtx * ctx)
{
	if (!ctx)
		return ZSTDCB_ERROR(init_missing);

	/* no mutex needed here */
	return ctx->outsize;
}

/* returns the current compressed data frame count */
size_t ZSTDCB_GetFramesCCtx(ZSTDCB_CCtx * ctx)
{
	if (!ctx)
		return ZSTDCB_ERROR(init_missing);

	/* no mutex needed here */
	return ctx->curframe;
}

/* free all allocated buffers and structures */
void ZSTDCB_freeCCtx(ZSTDCB_CCtx * ctx)
{
	int t;

	if (!ctx)
		return;

	MTJobControl_shutdown(&ctx->job);
	for (t = 0; t < ctx->threads; t++)
		pthread_join(ctx->cwork[t].pthread, NULL);
	for (t = 0; t < ctx->threads; t++)
		free(ctx->writelist[t].out.buf);
	free(ctx->writelist);
	MTJobControl_destroy(&ctx->job);
	pthread_mutex_destroy(&ctx->error_mutex);
	pthread_mutex_destroy(&ctx->write_mutex);
	pthread_mutex_destroy(&ctx->read_mutex);
	free(ctx->cwork);
	free(ctx);
	ctx = 0;

	return;
}
