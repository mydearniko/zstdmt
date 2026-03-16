
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

#define ZSTD_STATIC_LINKING_ONLY
#include "zstd.h"

#include "memmt.h"
#include "jobcontrol.h"
#include "threading.h"
#include "list.h"
#include "zstd-mt.h"

/**
 * multi threaded zstd decompression
 *
 * - each thread works on his own
 * - needs a callback for reading / writing
 * - each worker does this:
 *   1) get read mutex and read some input
 *   2) release read mutex and do decompression
 *   3) get write mutex and write result
 *   4) begin with step 1 again, until no input
 */

#if 0
#include <stdio.h>
#define dprintf(fmt, arg...) do { printf(fmt, ## arg); } while (0)
#else
#define dprintf(fmt, ...)
#endif				/* DEBUG */

extern size_t zstdmt_errcode;

/* worker for compression */
typedef struct {
	ZSTDCB_DCtx *ctx;
	pthread_t pthread;
	ZSTDCB_Buffer in;
	ZSTD_DStream *dctx;
} cwork_t;

struct writelist;
struct writelist {
	size_t frame;
	ZSTDCB_Buffer out;
	struct list_head node;
};

static void *pt_decompress(void *arg);

struct ZSTDCB_DCtx_s {

	/* threads: 1..ZSTDCB_THREAD_MAX */
	int threads;
	int threadswanted;

	/* input buffer, used at single threading */
	size_t inputsize;

	/* buffersize used for output */
	size_t outputsize;

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

	/* worker lifecycle */
	MT_JobControl job;

	/* initial bytes used to classify the stream */
	unsigned char first_input[16];
	size_t first_input_size;

	/* lists for writing queue */
	struct writelist *writelist;
	struct list_head writelist_free;
	struct list_head writelist_busy;
	struct list_head writelist_done;
};

/* **************************************
 * Decompression
 ****************************************/

ZSTDCB_DCtx *ZSTDCB_createDCtx(int threads, int inputsize)
{
	ZSTDCB_DCtx *ctx;
	int t;
	int started = 0;

	/* allocate ctx */
	ctx = (ZSTDCB_DCtx *) calloc(1, sizeof(ZSTDCB_DCtx));
	if (!ctx)
		return 0;

	/* check threads value */
	if (threads < 1 || threads > ZSTDCB_THREAD_MAX)
		goto err_job;

	/* setup ctx */
	ctx->threadswanted = threads;
	ctx->threads = threads;
	ctx->insize = 0;
	ctx->outsize = 0;
	ctx->frames = 0;
	ctx->curframe = 0;

	/* will be used for single stream only */
	if (inputsize)
		ctx->inputsize = inputsize;
	else
		ctx->inputsize = 1024 * 512;

	/* frame size (will get higher, when needed) */
	ctx->outputsize = 1024 * 512;
	ctx->cwork = 0;

	pthread_mutex_init(&ctx->read_mutex, NULL);
	pthread_mutex_init(&ctx->write_mutex, NULL);
	pthread_mutex_init(&ctx->error_mutex, NULL);
	MTJobControl_init(&ctx->job);

	INIT_LIST_HEAD(&ctx->writelist_free);
	INIT_LIST_HEAD(&ctx->writelist_busy);
	INIT_LIST_HEAD(&ctx->writelist_done);

	ctx->cwork = (cwork_t *)calloc((size_t)threads, sizeof(cwork_t));
	if (!ctx->cwork)
		goto err_job;

	for (t = 0; t < ctx->threads; t++) {
		cwork_t *w = &ctx->cwork[t];
		w->ctx = ctx;
		w->in.buf = 0;
		w->in.size = 0;
		w->in.allocated = 0;
		w->dctx = ZSTD_createDStream();
		if (!w->dctx)
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
	if (ctx->cwork) {
		for (t = 0; t < ctx->threads; t++) {
			if (ctx->cwork[t].dctx)
				ZSTD_freeDStream(ctx->cwork[t].dctx);
		}
	}
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
 * IsZstd_Magic - check, if 4 bytes are valid ZSTD MAGIC
 */
static int IsZstd_Magic(unsigned char *buf)
{
	U32 magic = MEM_readLE32(buf);
	if (magic == ZSTDCB_MAGICNUMBER_V01)
		return 1;
	return (magic >= ZSTDCB_MAGICNUMBER_MIN
		&& magic <= ZSTDCB_MAGICNUMBER_MAX);
}

/**
 * IsZstd_Skippable - check, if 4 bytes are MAGIC_SKIPPABLE
 */
static int IsZstd_Skippable(unsigned char *buf)
{
	return (MEM_readLE32(buf) == ZSTDCB_MAGIC_SKIPPABLE);
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

	/* XXX, some catch all other errors */
	return ZSTDCB_ERROR(read_fail);
}

/**
 * pt_write - queue for decompressed output
 */
static size_t pt_write(ZSTDCB_DCtx * ctx, struct writelist *wl)
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

static void reset_writelists(ZSTDCB_DCtx *ctx)
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

static size_t append_collect(ZSTDCB_Buffer *collect, const void *src, size_t size)
{
	size_t needed;

	if (size == 0)
		return 0;

	needed = collect->size + size;
	if (collect->allocated < needed) {
		size_t new_size = collect->allocated ? collect->allocated : (1024 * 512);
		void *buf;

		while (new_size < needed)
			new_size <<= 1;

		buf = realloc(collect->buf, new_size);
		if (!buf)
			return ZSTDCB_ERROR(memory_allocation);
		collect->buf = buf;
		collect->allocated = new_size;
	}

	memcpy((char *)collect->buf + collect->size, src, size);
	collect->size += size;
	return 0;
}

/**
 * pt_read - read compressed input
 */
static size_t pt_read(ZSTDCB_DCtx * ctx, ZSTDCB_Buffer * in, size_t * frame)
{
	unsigned char hdrbuf[12];
	ZSTDCB_Buffer hdr;
	size_t toRead;
	int rv;

	pthread_mutex_lock(&ctx->read_mutex);

	/* special case, some bytes were read by magic check */
	if (unlikely(ctx->frames == 0)) {
		in->buf = ctx->first_input;
		in->size = ctx->first_input_size;

		/* the magic check reads exactly 16 bytes! */
		if (unlikely(in->size != 16))
			goto error_data;
		ctx->insize += 16;

		/**
		 * zstdmt mode, with zstd magic prefix
		 * 9 bytes zero byte frame + 12 byte skippable
		 * - 21 bytes to read, 16 bytes done
		 * - read 5 bytes, put them together (12 byte hdr)
		 */
		if (!IsZstd_Skippable(in->buf)) {
			memcpy(hdrbuf, in->buf, 7);
			hdr.buf = hdrbuf + 7;
			hdr.size = 5;
			rv = ctx->fn_read(ctx->arg_read, &hdr);
			if (rv != 0) {
				pthread_mutex_unlock(&ctx->read_mutex);
				return mt_error(rv);
			}
			if (hdr.size != 5)
				goto error_data;
			hdr.buf = hdrbuf;
			ctx->insize += 16 + 5;

			/* read data */
			toRead = MEM_readLE32((unsigned char *)hdr.buf + 8);
			in->size = toRead;
			in->buf = malloc(in->size);
			if (!in->buf)
				goto error_nomem;
			in->allocated = in->size;
			rv = ctx->fn_read(ctx->arg_read, in);
			if (rv != 0) {
				pthread_mutex_unlock(&ctx->read_mutex);
				return mt_error(rv);
			}
			if (in->size != toRead)
				goto error_data;
			ctx->insize += in->size;
			*frame = ctx->frames++;
			pthread_mutex_unlock(&ctx->read_mutex);
			return 0;	/* done! */
		}

		/**
		 * pzstd mode, no prefix
		 * - start directly with 12 byte skippable frame
		 */
		if (IsZstd_Skippable(in->buf)) {
			unsigned char *start = in->buf;	/* 16 bytes data */
			toRead = MEM_readLE32((unsigned char *)start + 8);
			in->size = toRead;
			in->buf = malloc(in->size);
			if (!in->buf)
				goto error_nomem;
			in->allocated = in->size;
			/* copy 4 bytes user data to new buf */
			memcpy(in->buf, start + 12, 4);
			start = in->buf;	/* point to in->buf now */

			/* 12 byte skippable, so 4 bytes data done */
			in->buf = start + 4;
			in->size = toRead - 4;
			rv = ctx->fn_read(ctx->arg_read, in);
			if (rv != 0) {
				pthread_mutex_unlock(&ctx->read_mutex);
				return mt_error(rv);
			}
			if (in->size != toRead - 4)
				goto error_data;
			ctx->insize += in->size;
			in->buf = start;	/* restore inbuf */
			in->size += 4;
			*frame = ctx->frames++;
			pthread_mutex_unlock(&ctx->read_mutex);
			return 0;	/* done! */
		}
	}

	/**
	 * read next skippable frame (12 bytes)
	 * 4 bytes skippable magic
	 * 4 bytes little endian, must be: 4 (user data size)
	 * 4 bytes little endian, size to read (user data)
	 */
	hdr.buf = hdrbuf;
	hdr.size = 12;
	rv = ctx->fn_read(ctx->arg_read, &hdr);
	if (rv != 0) {
		pthread_mutex_unlock(&ctx->read_mutex);
		return mt_error(rv);
	}

	/* eof reached ? */
	if (unlikely(hdr.size == 0)) {
		pthread_mutex_unlock(&ctx->read_mutex);
		in->size = 0;
		return 0;
	}

	/* check header data */
	if (unlikely(hdr.size != 12))
		goto error_read;
	if (unlikely(!IsZstd_Skippable(hdr.buf)))
		goto error_data;
	ctx->insize += 12;

	/* read new input (size should be _toRead_ bytes */
	toRead = MEM_readLE32((unsigned char *)hdr.buf + 8);
	{
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
	return ZSTDCB_ERROR(data_error);
 error_read:
	pthread_mutex_unlock(&ctx->read_mutex);
	return ZSTDCB_ERROR(read_fail);
 error_nomem:
	pthread_mutex_unlock(&ctx->read_mutex);
	return ZSTDCB_ERROR(memory_allocation);
}

static void *pt_decompress(void *arg)
{
	cwork_t *w = (cwork_t *) arg;
	ZSTDCB_Buffer *in = &w->in;
	ZSTDCB_DCtx *ctx = w->ctx;
	ZSTDCB_Buffer collect;
	unsigned generation = 0;

	collect.buf = 0;
	collect.size = 0;
	collect.allocated = 0;

	while (MTJobControl_wait(&ctx->job, &generation)) {
		size_t result = 0;
		struct writelist *wl = 0;
		int failed = 0;

		for (;;) {
			ZSTDCB_Buffer *out;
			ZSTD_inBuffer zIn;
			ZSTD_outBuffer zOut;
			unsigned long long frame_size;

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
			{
				struct list_head *entry =
				    list_first(&ctx->writelist_free);
				wl = list_entry(entry, struct writelist, node);
				list_move(entry, &ctx->writelist_busy);
			}
			out = &wl->out;
			if (out->allocated < ctx->outputsize) {
				void *buf = realloc(out->buf, ctx->outputsize);
				if (!buf) {
					pthread_mutex_unlock(&ctx->write_mutex);
					result = ZSTDCB_ERROR(memory_allocation);
					failed = 1;
					break;
				}
				out->buf = buf;
				out->allocated = ctx->outputsize;
			}
			out->size = out->allocated;
			pthread_mutex_unlock(&ctx->write_mutex);

			result = ZSTD_resetDStream(w->dctx);
			if (ZSTD_isError(result)) {
				zstdmt_errcode = result;
				result = ZSTDCB_ERROR(compression_library);
				failed = 1;
				break;
			}

			result = pt_read(ctx, in, &wl->frame);
			if (ZSTDCB_isError(result)) {
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

			frame_size = ZSTD_getFrameContentSize(in->buf, in->size);
			if (frame_size != ZSTD_CONTENTSIZE_ERROR
			    && frame_size != ZSTD_CONTENTSIZE_UNKNOWN
			    && frame_size <= (unsigned long long)(size_t)-1) {
				size_t target = (size_t)frame_size;

				if (out->allocated < target) {
					void *buf = realloc(out->buf, target);
					if (!buf) {
						result =
						    ZSTDCB_ERROR(memory_allocation);
						failed = 1;
						break;
					}
					out->buf = buf;
					out->allocated = target;
				}

				result =
				    ZSTD_decompressDCtx(w->dctx, out->buf,
							target, in->buf,
							in->size);
				if (ZSTD_isError(result)) {
					zstdmt_errcode = result;
					result =
					    ZSTDCB_ERROR(compression_library);
					failed = 1;
					break;
				}
				out->size = result;

				pthread_mutex_lock(&ctx->write_mutex);
				result = pt_write(ctx, wl);
				pthread_mutex_unlock(&ctx->write_mutex);
				wl = 0;
				if (ZSTDCB_isError(result)) {
					failed = 1;
					break;
				}
				continue;
			}

			zIn.size = in->size;
			zIn.src = in->buf;
			zIn.pos = 0;
			collect.size = 0;

			for (;;) {
 again:
				zOut.size = out->allocated;
				zOut.dst = out->buf;
				zOut.pos = 0;

				result = ZSTD_decompressStream(w->dctx, &zOut, &zIn);
				if (ZSTD_isError(result)) {
					zstdmt_errcode = result;
					result =
					    ZSTDCB_ERROR(compression_library);
					failed = 1;
					break;
				}

				if (zOut.pos) {
					result =
					    append_collect(&collect, out->buf,
							   zOut.pos);
					if (ZSTDCB_isError(result)) {
						failed = 1;
						break;
					}
				}

				if (result == 0) {
					if (collect.size) {
						void *tmp_buf = out->buf;
						size_t tmp_alloc = out->allocated;

						out->buf = collect.buf;
						out->size = collect.size;
						out->allocated = collect.allocated;
						collect.buf = tmp_buf;
						collect.size = 0;
						collect.allocated = tmp_alloc;
					} else {
						out->size = 0;
					}
					pthread_mutex_lock(&ctx->write_mutex);
					result = pt_write(ctx, wl);
					pthread_mutex_unlock(&ctx->write_mutex);
					wl = 0;
					if (ZSTDCB_isError(result))
						failed = 1;
					break;
				}

				if (result != 0) {
					size_t new_size = out->allocated ? out->allocated : ctx->outputsize;
					void *buf;

					new_size <<= 1;
					buf = realloc(out->buf, new_size);
					if (!buf) {
						result =
						    ZSTDCB_ERROR(memory_allocation);
						failed = 1;
						break;
					}
					out->buf = buf;
					out->allocated = new_size;
					goto again;
				}

				if (zIn.pos == zIn.size)
					break;
			}

			if (failed)
				break;
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

	free(collect.buf);
	return 0;
}

/* single threaded */
static size_t st_decompress(void *arg)
{
	ZSTDCB_DCtx *ctx = (ZSTDCB_DCtx *) arg;
	cwork_t *w = &ctx->cwork[0];
	ZSTDCB_Buffer In, Out;
	ZSTDCB_Buffer *in = &In;
	ZSTDCB_Buffer *out = &Out;
	ZSTDCB_Buffer *magic = &w->in;
	size_t result;
	int rv;

	ZSTD_inBuffer zIn;
	ZSTD_outBuffer zOut;

	/* init dstream stream */
	result = ZSTD_initDStream(w->dctx);
	if (ZSTD_isError(result)) {
		zstdmt_errcode = result;
		return ZSTDCB_ERROR(compression_library);
	}

	/* allocate space for input buffer */
	in->size = ZSTD_DStreamInSize();
	in->buf = malloc(in->size);
	if (!in->buf)
		return ZSTDCB_ERROR(memory_allocation);
	in->allocated = in->size;

	/* allocate space for output buffer */
	out->size = ZSTD_DStreamOutSize();
	out->buf = malloc(out->size);
	if (!out->buf) {
		free(in->buf);
		return ZSTDCB_ERROR(memory_allocation);
	}
	out->allocated = out->size;

	/* we read already some bytes, handle that: */
	{
		/* remember in->buf */
		unsigned char *buf = in->buf;

		/* fill first read bytes to buffer... */
		memcpy(in->buf, magic->buf, magic->size);
		magic->buf = in->buf;
		in->buf = buf + magic->size;
		in->size = in->allocated - magic->size;

		/* read more bytes, to fill buffer */
		rv = ctx->fn_read(ctx->arg_read, in);
		if (rv != 0) {
			result = mt_error(rv);
			goto error;
		}

		/* ready, first buffer complete */
		in->buf = buf;
		in->size += magic->size;
		ctx->insize += in->size;
	}

	zIn.src = in->buf;
	zIn.size = in->size;
	zIn.pos = 0;

	zOut.dst = out->buf;

	for (;;) {
		for (;;) {
			/* decompress loop */
			zOut.size = out->allocated;
			zOut.pos = 0;

			result = ZSTD_decompressStream(w->dctx, &zOut, &zIn);
			if (ZSTD_isError(result))
				goto error_clib;

			if (zOut.pos) {
				ZSTDCB_Buffer wb;
				wb.size = zOut.pos;
				wb.buf = zOut.dst;
				rv = ctx->fn_write(ctx->arg_write, &wb);
				if (rv != 0) {
					result = mt_error(rv);
					goto error;
				}
				ctx->outsize += zOut.pos;
			}

			/* one more round */
			if ((zIn.pos == zIn.size) && (result == 1) && zOut.pos)
				continue;

			/* finished */
			if (zIn.pos == zIn.size)
				break;

			/* end of frame */
			if (result == 0) {
				result = ZSTD_resetDStream(w->dctx);
				if (ZSTD_isError(result))
					goto error_clib;
			}
		}		/* decompress */

		/* read next input */
		in->size = in->allocated;
		rv = ctx->fn_read(ctx->arg_read, in);
		if (rv != 0) {
			result = mt_error(rv);
			goto error;
		}

		if (in->size == 0)
			goto okay;
		ctx->insize += in->size;

		zIn.size = in->size;
		zIn.pos = 0;
	}			/* read */

 error_clib:
	zstdmt_errcode = result;
	result = ZSTDCB_ERROR(compression_library);
	/* fall through */
 error:
	/* return with error */
	free(out->buf);
	free(in->buf);
	return result;
 okay:
	/* no error */
	free(out->buf);
	free(in->buf);
	return 0;
}

#define TYPE_UNKNOWN       0
#define TYPE_SINGLE_THREAD 1
#define TYPE_MULTI_THREAD  2

size_t ZSTDCB_decompressDCtx(ZSTDCB_DCtx * ctx, ZSTDCB_RdWr_t * rdwr)
{
	unsigned char buf[16];
	ZSTDCB_Buffer In;
	ZSTDCB_Buffer *in = &In;
	cwork_t *w;
	int rv, type = TYPE_UNKNOWN;
	size_t result;

	if (!ctx)
		return ZSTDCB_ERROR(compressionParameter_unsupported);

	/* init reading and writing functions */
	ctx->fn_read = rdwr->fn_read;
	ctx->fn_write = rdwr->fn_write;
	ctx->arg_read = rdwr->arg_read;
	ctx->arg_write = rdwr->arg_write;
	ctx->insize = 0;
	ctx->outsize = 0;
	ctx->frames = 0;
	ctx->curframe = 0;
	ctx->first_input_size = 0;

	/**
	 * possible valid magic's for us, we need 16 bytes, for checking
	 *
	 * 1) ZSTDCB_MAGIC @0 -> ST Stream
	 * 2) ZSTDCB_MAGIC @0 + MAGIC_SKIPPABLE @9 -> MT Stream else ST
	 * 3) MAGIC_SKIPPABLE @0 + ZSTDCB_MAGIC @12 -> MT Stream
	 * 4) all other: not valid!
	 */

	/* check for ZSTDCB_MAGIC_SKIPPABLE */
	in->buf = buf;
	in->size = 16;
	rv = ctx->fn_read(ctx->arg_read, in);
	if (rv != 0)
		return mt_error(rv);
	ctx->first_input_size = in->size;
	memcpy(ctx->first_input, buf, in->size);

	/* must be single threaded standard zstd, when smaller 16 bytes */
	if (in->size < 16) {
		if (!IsZstd_Magic(buf))
			return ZSTDCB_ERROR(data_error);
		dprintf("single thread style, current pos=%zu\n", in->size);
		type = TYPE_SINGLE_THREAD;
		if (in->size == 9) {
			/* create empty file */
			return 0;
		}
	} else {
		if (IsZstd_Skippable(buf) && IsZstd_Magic(buf + 12)) {
			/* pzstd */
			dprintf("pzstd style\n");
			type = TYPE_MULTI_THREAD;
		} else if (IsZstd_Magic(buf) && IsZstd_Skippable(buf + 9)) {
			/* zstdmt */
			dprintf("zstdmt style\n");
			type = TYPE_MULTI_THREAD;
			/* set buffer to the */
		} else if (IsZstd_Magic(buf)) {
			/* some std zstd stream */
			dprintf("single thread style, current pos=%zu\n",
				in->size);
			type = TYPE_SINGLE_THREAD;
		} else {
			/* invalid */
			dprintf("not valid\n");
			return ZSTDCB_ERROR(data_error);
		}
	}

	/* use single thread extraction, when only one thread is there */
	if (ctx->threadswanted == 1)
		type = TYPE_SINGLE_THREAD;

	/* single threaded, but with known sizes */
	if (type == TYPE_SINGLE_THREAD) {
		w = &ctx->cwork[0];
		w->in.buf = ctx->first_input;
		w->in.size = in->size;
		w->in.allocated = 0;
		return st_decompress(ctx);
	}

	for (rv = 0; rv < ctx->threads; rv++) {
		w = &ctx->cwork[rv];
		w->in.size = 0;
	}

	MTJobControl_start(&ctx->job, ctx->threads);
	result = MTJobControl_wait_done(&ctx->job);
	reset_writelists(ctx);

	return result;
}

/* returns current uncompressed data size */
size_t ZSTDCB_GetInsizeDCtx(ZSTDCB_DCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->insize;
}

/* returns the current compressed data size */
size_t ZSTDCB_GetOutsizeDCtx(ZSTDCB_DCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->outsize;
}

/* returns the current compressed frames */
size_t ZSTDCB_GetFramesDCtx(ZSTDCB_DCtx * ctx)
{
	if (!ctx)
		return 0;

	return ctx->curframe;
}

void ZSTDCB_freeDCtx(ZSTDCB_DCtx * ctx)
{
	int t;

	if (!ctx)
		return;

	MTJobControl_shutdown(&ctx->job);
	for (t = 0; t < ctx->threads; t++)
		pthread_join(ctx->cwork[t].pthread, NULL);
	for (t = 0; t < ctx->threads; t++) {
		cwork_t *w = &ctx->cwork[t];
		if (w->in.buf && w->in.buf != ctx->first_input)
			free(w->in.buf);
		ZSTD_freeDStream(w->dctx);
	}

	for (t = 0; t < ctx->threads; t++)
		free(ctx->writelist[t].out.buf);
	free(ctx->writelist);
	MTJobControl_destroy(&ctx->job);
	pthread_mutex_destroy(&ctx->error_mutex);
	pthread_mutex_destroy(&ctx->write_mutex);
	pthread_mutex_destroy(&ctx->read_mutex);
	free(ctx->cwork);
	free(ctx);
}
