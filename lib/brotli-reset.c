#include <string.h>

#include "../programs/brotli/c/common/constants.h"
#include "../programs/brotli/c/enc/state.h"

static void BrotliEncoderInitParamsMT(BrotliEncoderParams *params)
{
	params->mode = BROTLI_DEFAULT_MODE;
	params->large_window = BROTLI_FALSE;
	params->quality = BROTLI_DEFAULT_QUALITY;
	params->lgwin = BROTLI_DEFAULT_WINDOW;
	params->lgblock = 0;
	params->stream_offset = 0;
	params->size_hint = 0;
	params->disable_literal_context_modeling = BROTLI_FALSE;
	BrotliInitSharedEncoderDictionary(&params->dictionary);
	params->dist.distance_postfix_bits = 0;
	params->dist.num_direct_distance_codes = 0;
	params->dist.alphabet_size_max =
		BROTLI_DISTANCE_ALPHABET_SIZE(0, 0, BROTLI_MAX_DISTANCE_BITS);
	params->dist.alphabet_size_limit = params->dist.alphabet_size_max;
	params->dist.max_distance = BROTLI_MAX_DISTANCE;
}

static void BrotliEncoderCleanupParamsMT(MemoryManager *m,
					 BrotliEncoderParams *params)
{
	BrotliCleanupSharedEncoderDictionary(m, &params->dictionary);
}

static void BrotliEncoderInitStateMT(BrotliEncoderState *state)
{
	BrotliEncoderInitParamsMT(&state->params);
	state->input_pos_ = 0;
	state->num_commands_ = 0;
	state->num_literals_ = 0;
	state->last_insert_len_ = 0;
	state->last_flush_pos_ = 0;
	state->last_processed_pos_ = 0;
	state->prev_byte_ = 0;
	state->prev_byte2_ = 0;
	state->storage_size_ = 0;
	state->storage_ = 0;
	HasherInit(&state->hasher_);
	state->large_table_ = NULL;
	state->large_table_size_ = 0;
	state->one_pass_arena_ = NULL;
	state->two_pass_arena_ = NULL;
	state->command_buf_ = NULL;
	state->literal_buf_ = NULL;
	state->total_in_ = 0;
	state->next_out_ = NULL;
	state->available_out_ = 0;
	state->total_out_ = 0;
	state->stream_state_ = BROTLI_STREAM_PROCESSING;
	state->is_last_block_emitted_ = BROTLI_FALSE;
	state->is_initialized_ = BROTLI_FALSE;

	RingBufferInit(&state->ringbuffer_);

	state->commands_ = NULL;
	state->cmd_alloc_size_ = 0;

	state->dist_cache_[0] = 4;
	state->dist_cache_[1] = 11;
	state->dist_cache_[2] = 15;
	state->dist_cache_[3] = 16;
	memcpy(state->saved_dist_cache_, state->dist_cache_,
	       sizeof(state->saved_dist_cache_));
}

static void BrotliEncoderCleanupStateMT(BrotliEncoderState *state)
{
	MemoryManager *m = &state->memory_manager_;

	if (BROTLI_IS_OOM(m)) {
		BrotliWipeOutMemoryManager(m);
		return;
	}

	BROTLI_FREE(m, state->storage_);
	BROTLI_FREE(m, state->commands_);
	RingBufferFree(m, &state->ringbuffer_);
	DestroyHasher(m, &state->hasher_);
	BROTLI_FREE(m, state->large_table_);
	BROTLI_FREE(m, state->one_pass_arena_);
	BROTLI_FREE(m, state->two_pass_arena_);
	BROTLI_FREE(m, state->command_buf_);
	BROTLI_FREE(m, state->literal_buf_);
	BrotliEncoderCleanupParamsMT(m, &state->params);
}

/* Keep worker-local Brotli state reusable without patching the cloned source. */
void BrotliEncoderResetInstance(BrotliEncoderState *state)
{
	brotli_alloc_func alloc_func;
	brotli_free_func free_func;
	void *opaque;

	if (!state)
		return;

	alloc_func = state->memory_manager_.alloc_func;
	free_func = state->memory_manager_.free_func;
	opaque = state->memory_manager_.opaque;
	BrotliEncoderCleanupStateMT(state);
	BrotliInitMemoryManager(&state->memory_manager_, alloc_func, free_func,
				opaque);
	BrotliEncoderInitStateMT(state);
}
