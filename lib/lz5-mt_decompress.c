
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

#define LZ5F_DISABLE_OBSOLETE_ENUMS
#include "lz5frame.h"

#include "memmt.h"
#include "jobcontrol.h"
#include "threading.h"
#include "list.h"
#include "lz5-mt.h"

/**
 * multi threaded lz5 - multiple workers version
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
	LZ5MT_DCtx *ctx;
	pthread_t pthread;
	LZ5MT_Buffer in;
	LZ5F_decompressionContext_t dctx;
} cwork_t;

struct writelist;
struct writelist {
	size_t frame;
	LZ5MT_Buffer out;
	struct list_head node;
};

static void *pt_decompress(void *arg);

struct LZ5MT_DCtx_s {

	/* threads: 1..LZ5MT_THREAD_MAX */
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
 * Decompression
 ****************************************/

LZ5MT_DCtx *LZ5MT_createDCtx(int threads, int inputsize)
{
	LZ5MT_DCtx *ctx;
	int t;
	int started = 0;

	/* allocate ctx */
	ctx = (LZ5MT_DCtx *) calloc(1, sizeof(LZ5MT_DCtx));
	if (!ctx)
		return 0;

	/* check threads value */
	if (threads < 1 || threads > LZ5MT_THREAD_MAX)
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
		w->ctx = ctx;
		w->in.buf = 0;
		w->in.size = 0;
		w->in.allocated = 0;

		/* setup thread work */
		if (LZ5F_isError(LZ5F_createDecompressionContext(&w->dctx,
							      LZ5F_VERSION)))
			goto err_cwork;
	}

	ctx->writelist =
	    (struct writelist *)calloc((size_t)threads, sizeof(struct writelist));
	if (!ctx->writelist)
		goto err_dctx;

	for (t = 0; t < ctx->threads; t++) {
		struct writelist *wl = &ctx->writelist[t];
		wl->out.buf = 0;
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
 err_dctx:
	for (t = 0; t < threads; t++) {
		cwork_t *w = &ctx->cwork[t];
		if (w->dctx)
			LZ5F_freeDecompressionContext(w->dctx);
	}
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
		return ERROR(read_fail);
	case -2:
		return ERROR(canceled);
	case -3:
		return ERROR(memory_allocation);
	}

	/* XXX, some catch all other errors */
	return ERROR(read_fail);
}

/**
 * pt_write - queue for decompressed output
 */
static size_t pt_write(LZ5MT_DCtx * ctx, struct writelist *wl)
{
	struct list_head *entry;

	/* move the entry to the done list */
	list_move(&wl->node, &ctx->writelist_done);
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

static void reset_writelists(LZ5MT_DCtx *ctx)
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
 * pt_read - read compressed output
 */
static size_t pt_read(LZ5MT_DCtx * ctx, LZ5MT_Buffer * in, size_t * frame)
{
	unsigned char hdrbuf[12];
	LZ5MT_Buffer hdr;
	int rv;

	/* read skippable frame (8 or 12 bytes) */
	pthread_mutex_lock(&ctx->read_mutex);

	/* special case, first 4 bytes already read */
	if (ctx->frames == 0) {
		hdr.buf = hdrbuf + 4;
		hdr.size = 8;
		rv = ctx->fn_read(ctx->arg_read, &hdr);
		if (rv != 0) {
			pthread_mutex_unlock(&ctx->read_mutex);
			return mt_error(rv);
		}
		if (hdr.size != 8)
			goto error_read;
		hdr.buf = hdrbuf;
	} else {
		hdr.buf = hdrbuf;
		hdr.size = 12;
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
		if (hdr.size != 12)
			goto error_read;
		if (MEM_readLE32((unsigned char *)hdr.buf + 0) !=
		    LZ5FMT_MAGIC_SKIPPABLE)
			goto error_data;
	}

	/* check header data */
	if (MEM_readLE32((unsigned char *)hdr.buf + 4) != 4)
		goto error_data;

	ctx->insize += 12;
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
	return ERROR(data_error);
 error_read:
	pthread_mutex_unlock(&ctx->read_mutex);
	return ERROR(read_fail);
 error_nomem:
	pthread_mutex_unlock(&ctx->read_mutex);
	return ERROR(memory_allocation);
}

static void *pt_decompress(void *arg)
{
	cwork_t *w = (cwork_t *) arg;
	LZ5MT_Buffer *in = &w->in;
	LZ5MT_DCtx *ctx = w->ctx;
	unsigned generation = 0;

	while (MTJobControl_wait(&ctx->job, &generation)) {
		size_t result = 0;
		struct writelist *wl = 0;
		int failed = 0;

		for (;;) {
			struct list_head *entry;
			LZ5MT_Buffer *out;

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

			/* zero should not happen here! */
			result = pt_read(ctx, in, &wl->frame);
			if (LZ5MT_isError(result)) {
				failed = 1;
				break;
			}

			if (in->size == 0) {
				pthread_mutex_lock(&ctx->write_mutex);
				list_move(&wl->node, &ctx->writelist_free);
				pthread_cond_signal(&ctx->job.free_cond);
				pthread_mutex_unlock(&ctx->write_mutex);
				wl = 0;
				break;
			}

			/* minimal frame */
			if (in->size < 40 && ctx->frames == 1)
				out->size = 1024 * 64;
			else {
				unsigned char *src = (unsigned char *)in->buf + 6;
				out->size = (size_t) MEM_readLE64(src);
			}

			if (out->allocated < out->size) {
				void *buf = realloc(out->buf, out->size);
				if (!buf) {
					result = ERROR(memory_allocation);
					failed = 1;
					break;
				}
				out->buf = buf;
				out->allocated = out->size;
			}

			result =
			    LZ5F_decompress(w->dctx, out->buf, &out->size,
					    in->buf, &in->size, 0);

			if (LZ5F_isError(result)) {
				lz5mt_errcode = result;
				result = ERROR(compression_library);
				failed = 1;
				break;
			}

			if (result != 0) {
				result = ERROR(frame_decompress);
				failed = 1;
				break;
			}

			/* write result */
			pthread_mutex_lock(&ctx->write_mutex);
			result = pt_write(ctx, wl);
			pthread_mutex_unlock(&ctx->write_mutex);
			wl = 0;
			if (LZ5MT_isError(result)) {
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

/* single threaded */
static size_t st_decompress(void *arg)
{
	LZ5MT_DCtx *ctx = (LZ5MT_DCtx *) arg;
	LZ5F_errorCode_t result = 0;
	cwork_t *w = &ctx->cwork[0];
	LZ5MT_Buffer Out;
	LZ5MT_Buffer *out = &Out;
	LZ5MT_Buffer *in = &w->in;
	void *magic = in->buf;
	int rv;

	/* allocate space for input buffer */
	in->size = ctx->inputsize;
	in->buf = malloc(in->size);
	if (!in->buf)
		return ERROR(memory_allocation);

	/* allocate space for output buffer */
	out->size = ctx->inputsize;
	out->buf = malloc(out->size);
	if (!out->buf) {
		free(in->buf);
		return ERROR(memory_allocation);
	}

	/* we have read already 4 bytes */
	in->size = 4;
	memcpy(in->buf, magic, in->size);

	/* stats */
	ctx->insize = 4;
	ctx->outsize = 0;

	/* decompress loop */
	for (;;) {
		size_t srcPos = 0;
		for (;;) {
			size_t srcSize = in->size - srcPos;
			out->size = ctx->inputsize;

			result = LZ5F_decompress(w->dctx, out->buf, &out->size, (unsigned char *)in->buf + srcPos, &srcSize, NULL);
			if (LZ5F_isError(result)) {
				free(in->buf);
				free(out->buf);
				return ERROR(compression_library);
			}

			/* update stats */
			srcPos += srcSize;
			ctx->insize += srcSize;
			ctx->outsize += out->size;

			/* have some output */
			if (out->size) {
				rv = ctx->fn_write(ctx->arg_write, out);
				if (rv != 0) {
					free(in->buf);
					free(out->buf);
					return mt_error(rv);
				}
			}

			/* consumed all input */
			if (srcPos == in->size)
				break;
		}

		/* read new input */
		if (result)
			in->size = result;
		else
			in->size = ctx->inputsize;

		if (in->size > ctx->inputsize)
			in->size = ctx->inputsize;

		rv = ctx->fn_read(ctx->arg_read, in);
		ctx->insize += in->size;
		if (rv != 0) {
			free(in->buf);
			free(out->buf);
			return mt_error(rv);
		}

		if (in->size == 0)
			break;
	}

	/* no error */
	free(out->buf);
	free(in->buf);
	return 0;
}

size_t LZ5MT_decompressDCtx(LZ5MT_DCtx * ctx, LZ5MT_RdWr_t * rdwr)
{
	unsigned char buf[4];
	int rv;
	cwork_t *w = &ctx->cwork[0];
	LZ5MT_Buffer *in = &w->in;
	size_t result;

	if (!ctx)
		return ERROR(compressionParameter_unsupported);

	/* init reading and writing functions */
	ctx->fn_read = rdwr->fn_read;
	ctx->fn_write = rdwr->fn_write;
	ctx->arg_read = rdwr->arg_read;
	ctx->arg_write = rdwr->arg_write;
	ctx->insize = 0;
	ctx->outsize = 0;
	ctx->frames = 0;
	ctx->curframe = 0;

	/* check for LZ5FMT_MAGIC_SKIPPABLE */
	in->buf = buf;
	in->size = 4;
	rv = ctx->fn_read(ctx->arg_read, in);
	if (rv != 0)
		return mt_error(rv);
	if (in->size != 4)
		return ERROR(data_error);

	/* single threaded with unknown sizes */
	if (MEM_readLE32(buf) != LZ5FMT_MAGIC_SKIPPABLE) {

		/* look for correct magic */
		if (MEM_readLE32(buf) != LZ5FMT_MAGICNUMBER)
			return ERROR(data_error);

		/* decompress single threaded */
		return st_decompress(ctx);
	}

	/* worker input buffers are managed by the persistent workers */
	in->buf = 0;
	in->size = 0;

	MTJobControl_start(&ctx->job, ctx->threads);
	result = MTJobControl_wait_done(&ctx->job);
	reset_writelists(ctx);

	return result;
}

/* returns current uncompressed data size */
size_t LZ5MT_GetInsizeDCtx(LZ5MT_DCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->insize;
}

/* returns the current compressed data size */
size_t LZ5MT_GetOutsizeDCtx(LZ5MT_DCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->outsize;
}

/* returns the current compressed frames */
size_t LZ5MT_GetFramesDCtx(LZ5MT_DCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->curframe;
}

void LZ5MT_freeDCtx(LZ5MT_DCtx * ctx)
{
	int t;

	if (!ctx)
		return;

	MTJobControl_shutdown(&ctx->job);
	for (t = 0; t < ctx->threads; t++)
		pthread_join(ctx->cwork[t].pthread, NULL);
	for (t = 0; t < ctx->threads; t++) {
		cwork_t *w = &ctx->cwork[t];
		free(w->in.buf);
		LZ5F_freeDecompressionContext(w->dctx);
	}

	for (t = 0; t < ctx->threads; t++)
		free(ctx->writelist[t].out.buf);
	free(ctx->writelist);
	MTJobControl_destroy(&ctx->job);
	pthread_mutex_destroy(&ctx->read_mutex);
	pthread_mutex_destroy(&ctx->write_mutex);
	free(ctx->cwork);
	free(ctx);
	ctx = 0;

	return;
}
