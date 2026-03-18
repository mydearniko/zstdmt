
/**
 * Copyright (c) 2016 Tino Reichardt
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 *
 * You can contact the author at:
 * - zstdmt source repository: https://github.com/mcmilk/zstdmt
 */

#ifndef THREADING_H
#define THREADING_H

#if defined (__cplusplus)
extern "C" {
#endif

#ifdef _WIN32

/**
 * Windows Pthread Wrapper, based on this site:
 * http://www.cse.wustl.edu/~schmidt/win32-cv-1.html
 */

#ifndef WINVER
#define WINVER 0x0600
#endif
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0600
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

static inline int pthread_cond_init_win(CONDITION_VARIABLE *cond)
{
	InitializeConditionVariable(cond);
	return 0;
}

/* mutex */
#define pthread_mutex_t CRITICAL_SECTION
#define pthread_mutex_init(a,b)   InitializeCriticalSection((a))
#define pthread_mutex_destroy(a)  DeleteCriticalSection((a))
#define pthread_mutex_lock        EnterCriticalSection
#define pthread_mutex_unlock      LeaveCriticalSection

/* condition variables */
#define pthread_cond_t CONDITION_VARIABLE
#define pthread_cond_init(a,b)    pthread_cond_init_win((a))
#define pthread_cond_destroy(a)   ((void)0)
#define pthread_cond_signal       WakeConditionVariable
#define pthread_cond_broadcast    WakeAllConditionVariable
#define pthread_cond_wait(a,b)    (SleepConditionVariableCS((a), (b), INFINITE) ? 0 : -1)

/* pthread_create() and pthread_join() */
typedef struct {
	HANDLE handle;
	void *(*start_routine) (void *);
	void *arg;
} pthread_t;

extern int pthread_create(pthread_t * thread, const void *unused,
			  void *(*start_routine) (void *), void *arg);

#define pthread_join(a, b) _pthread_join(&(a), (b))
extern int _pthread_join(pthread_t * thread, void **value_ptr);

/**
 * add here more systems as required
 */

#else

/* POSIX Systems */
#include <pthread.h>

#endif /* POSIX Systems */

#if defined (__cplusplus)
}
#endif

#endif				/* PTHREAD_H */
