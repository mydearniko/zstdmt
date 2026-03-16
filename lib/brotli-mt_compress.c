
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

#include <stdlib.h>
#include <string.h>

#include "brotli/encode.h"

#include "brotli-mt.h"
#include "memmt.h"
#include "jobcontrol.h"
#include "threading.h"
#include "list.h"

extern void BrotliEncoderResetInstance(BrotliEncoderState *state);

/**
 * multi threaded brotli - multiple workers version
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
	BROTLIMT_CCtx *ctx;
	pthread_t pthread;
	BrotliEncoderState *state;
} cwork_t;

struct writelist;
struct writelist {
	size_t frame;
	BROTLIMT_Buffer out;
	struct list_head node;
};

static void *pt_compress(void *arg);

struct BROTLIMT_CCtx_s {
	int level;

	/* threads: 1..BROTLIMT_THREAD_MAX */
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
	fn_read *fn_read;
	void *arg_read;

	/* writing output */
	pthread_mutex_t write_mutex;
	fn_write *fn_write;
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

BROTLIMT_CCtx *BROTLIMT_createCCtx(int threads, int level, int inputsize)
{
	BROTLIMT_CCtx *ctx;
	int t;
	int started = 0;

	/* allocate ctx */
	ctx = (BROTLIMT_CCtx *) calloc(1, sizeof(BROTLIMT_CCtx));
	if (!ctx)
		return 0;

	/* check threads value */
	if (threads < 1 || threads > BROTLIMT_THREAD_MAX)
		goto err_job;

	/* check level */
	if (level < BROTLIMT_LEVEL_MIN || level > BROTLIMT_LEVEL_MAX)
		goto err_job;

	/* calculate chunksize for one thread */
	if (inputsize)
		ctx->inputsize = inputsize;
	else
		ctx->inputsize = 1024 * 1024 * (level ? level : 1);

	/* setup ctx */
	ctx->level = level;
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
		w->state = BrotliEncoderCreateInstance(0, 0, 0);
		if (!w->state)
			goto err_cwork;
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
	for (t = 0; t < threads; t++) {
		if (ctx->cwork && ctx->cwork[t].state)
			BrotliEncoderDestroyInstance(ctx->cwork[t].state);
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

	return MT_ERROR(read_fail);
}

/**
 * pt_write - queue for compressed output
 */
static size_t pt_write(BROTLIMT_CCtx * ctx, struct writelist *wl)
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

static void reset_writelists(BROTLIMT_CCtx *ctx)
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
	BROTLIMT_CCtx *ctx = w->ctx;
	BROTLIMT_Buffer in;
	unsigned generation = 0;
	const size_t max_out =
	    BrotliEncoderMaxCompressedSize(ctx->inputsize) + 16;

	/* inbuf is constant */
	in.size = ctx->inputsize;
	in.buf = malloc(in.size);
	if (!in.buf) {
		MTJobControl_set_result(&ctx->job, MT_ERROR(memory_allocation));
		return 0;
	}

	while (MTJobControl_wait(&ctx->job, &generation)) {
		size_t result = 0;
		struct writelist *wl = 0;
		int failed = 0;

		for (;;) {
			struct list_head *entry;
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
			if (wl->out.allocated < max_out) {
				void *buf = realloc(wl->out.buf, max_out);
				if (!buf) {
					pthread_mutex_unlock(&ctx->write_mutex);
					result = MT_ERROR(memory_allocation);
					failed = 1;
					break;
				}
				wl->out.buf = buf;
				wl->out.allocated = max_out;
			}
			wl->out.size = max_out;
			pthread_mutex_unlock(&ctx->write_mutex);

			pthread_mutex_lock(&ctx->read_mutex);
			in.size = ctx->inputsize;
			rv = ctx->fn_read(ctx->arg_read, &in);
			if (rv != 0) {
				pthread_mutex_unlock(&ctx->read_mutex);
				result = mt_error(rv);
				failed = 1;
				break;
			}

			if (in.size == 0 && ctx->frames > 0) {
				pthread_mutex_unlock(&ctx->read_mutex);
				pthread_mutex_lock(&ctx->write_mutex);
				list_move(&wl->node, &ctx->writelist_free);
				pthread_cond_signal(&ctx->job.free_cond);
				pthread_mutex_unlock(&ctx->write_mutex);
				wl = 0;
				break;
			}
			ctx->insize += in.size;
			wl->frame = ctx->frames++;
			pthread_mutex_unlock(&ctx->read_mutex);

			{
				size_t available_in = in.size;
				const uint8_t *next_in = in.buf;
				size_t available_out = wl->out.size - 16;
				uint8_t *next_out = (uint8_t *)wl->out.buf + 16;
				size_t total_out = 0;

				BrotliEncoderResetInstance(w->state);
				if (!BrotliEncoderSetParameter
				    (w->state, BROTLI_PARAM_QUALITY,
				     (uint32_t)ctx->level)
				    || !BrotliEncoderSetParameter
				    (w->state, BROTLI_PARAM_LGWIN,
				     BROTLI_MAX_WINDOW_BITS)
				    || !BrotliEncoderSetParameter
				    (w->state, BROTLI_PARAM_MODE,
				     BROTLI_MODE_GENERIC)
				    || !BrotliEncoderSetParameter
				    (w->state, BROTLI_PARAM_SIZE_HINT,
				     (uint32_t)in.size)) {
					result = MT_ERROR(frame_compress);
					failed = 1;
					break;
				}

				rv = BrotliEncoderCompressStream(w->state,
								 BROTLI_OPERATION_FINISH,
								 &available_in,
								 &next_in,
								 &available_out,
								 &next_out,
								 &total_out);
				if (rv == BROTLI_FALSE || available_in != 0
				    || !BrotliEncoderIsFinished(w->state)) {
					result = MT_ERROR(frame_compress);
					failed = 1;
					break;
				}
				wl->out.size = total_out;
			}

			MEM_writeLE32((unsigned char *)wl->out.buf + 0,
				      BROTLIMT_MAGIC_SKIPPABLE);
			MEM_writeLE32((unsigned char *)wl->out.buf + 4, 8);
			MEM_writeLE32((unsigned char *)wl->out.buf + 8,
				      (U32) wl->out.size);
			MEM_writeLE16((unsigned char *)wl->out.buf + 12,
				      (U16) BROTLIMT_MAGICNUMBER);

			{
				U16 hintsize;
				if (ctx->inputsize > (int)in.size) {
					hintsize = (U16)(in.size >> 16);
					hintsize += 1;
				} else {
					hintsize = ctx->inputsize >> 16;
				}
				MEM_writeLE16((unsigned char *)wl->out.buf + 14,
					      hintsize);
			}

			wl->out.size += 16;

			pthread_mutex_lock(&ctx->write_mutex);
			result = pt_write(ctx, wl);
			pthread_mutex_unlock(&ctx->write_mutex);
			wl = 0;
			if (BROTLIMT_isError(result)) {
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

	free(in.buf);
	return 0;
}

size_t BROTLIMT_compressCCtx(BROTLIMT_CCtx * ctx, BROTLIMT_RdWr_t * rdwr)
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
size_t BROTLIMT_GetInsizeCCtx(BROTLIMT_CCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->insize;
}

/* returns the current compressed data size */
size_t BROTLIMT_GetOutsizeCCtx(BROTLIMT_CCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->outsize;
}

/* returns the current compressed frames */
size_t BROTLIMT_GetFramesCCtx(BROTLIMT_CCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->curframe;
}

void BROTLIMT_freeCCtx(BROTLIMT_CCtx * ctx)
{
	int t;

	if (!ctx)
		return;

	MTJobControl_shutdown(&ctx->job);
	for (t = 0; t < ctx->threads; t++)
		pthread_join(ctx->cwork[t].pthread, NULL);
	for (t = 0; t < ctx->threads; t++) {
		BrotliEncoderDestroyInstance(ctx->cwork[t].state);
		free(ctx->writelist[t].out.buf);
	}
	free(ctx->writelist);
	MTJobControl_destroy(&ctx->job);
	pthread_mutex_destroy(&ctx->read_mutex);
	pthread_mutex_destroy(&ctx->write_mutex);
	free(ctx->cwork);
	free(ctx);
}
