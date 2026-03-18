
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

/**
 * This file will hold wrapper for systems, which do not support Pthreads
 */

#ifdef _WIN32

/**
 * Windows Pthread Wrapper, based on this site:
 * http://www.cse.wustl.edu/~schmidt/win32-cv-1.html
 */

#include "threading.h"

#include <process.h>
#include <errno.h>

int pthread_cond_init(pthread_cond_t *cond, const void *unused)
{
	(void)unused;

	cond->waiters_count = 0;
	cond->was_broadcast = 0;
	InitializeCriticalSection(&cond->waiters_count_lock);

	cond->semaphore = CreateSemaphore(NULL, 0, 0x7fffffff, NULL);
	if (cond->semaphore == NULL) {
		DeleteCriticalSection(&cond->waiters_count_lock);
		return (int)GetLastError();
	}

	cond->waiters_done = CreateEvent(NULL, FALSE, FALSE, NULL);
	if (cond->waiters_done == NULL) {
		CloseHandle(cond->semaphore);
		DeleteCriticalSection(&cond->waiters_count_lock);
		return (int)GetLastError();
	}

	return 0;
}

int pthread_cond_destroy(pthread_cond_t *cond)
{
	CloseHandle(cond->waiters_done);
	CloseHandle(cond->semaphore);
	DeleteCriticalSection(&cond->waiters_count_lock);
	return 0;
}

int pthread_cond_signal(pthread_cond_t *cond)
{
	int have_waiters;

	EnterCriticalSection(&cond->waiters_count_lock);
	have_waiters = (cond->waiters_count > 0);
	LeaveCriticalSection(&cond->waiters_count_lock);

	if (have_waiters)
		ReleaseSemaphore(cond->semaphore, 1, NULL);

	return 0;
}

int pthread_cond_broadcast(pthread_cond_t *cond)
{
	int have_waiters;
	LONG waiters_to_release;

	EnterCriticalSection(&cond->waiters_count_lock);
	have_waiters = (cond->waiters_count > 0);
	waiters_to_release = (LONG)cond->waiters_count;
	if (have_waiters)
		cond->was_broadcast = 1;
	LeaveCriticalSection(&cond->waiters_count_lock);

	if (!have_waiters)
		return 0;

	ReleaseSemaphore(cond->semaphore, waiters_to_release, NULL);
	WaitForSingleObject(cond->waiters_done, INFINITE);

	EnterCriticalSection(&cond->waiters_count_lock);
	cond->was_broadcast = 0;
	LeaveCriticalSection(&cond->waiters_count_lock);

	return 0;
}

int pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex)
{
	int last_waiter;

	EnterCriticalSection(&cond->waiters_count_lock);
	cond->waiters_count++;
	LeaveCriticalSection(&cond->waiters_count_lock);

	LeaveCriticalSection(mutex);
	WaitForSingleObject(cond->semaphore, INFINITE);

	EnterCriticalSection(&cond->waiters_count_lock);
	cond->waiters_count--;
	last_waiter = cond->was_broadcast && cond->waiters_count == 0;
	LeaveCriticalSection(&cond->waiters_count_lock);

	if (last_waiter)
		SetEvent(cond->waiters_done);

	EnterCriticalSection(mutex);
	return 0;
}

static unsigned __stdcall worker(void *arg)
{
	pthread_t *thread = (pthread_t *) arg;
	thread->arg = thread->start_routine(thread->arg);
	return 0;
}

int
pthread_create(pthread_t * thread, const void *unused,
	       void *(*start_routine) (void *), void *arg)
{
	(void)unused;
	thread->arg = arg;
	thread->start_routine = start_routine;
	thread->handle =
	    (HANDLE) _beginthreadex(NULL, 0, worker, thread, 0, NULL);

	if (!thread->handle)
		return errno;
	else
		return 0;
}

int _pthread_join(pthread_t * thread, void **value_ptr)
{
	DWORD result;

	if (!thread->handle)
		return 0;

	result = WaitForSingleObject(thread->handle, INFINITE);
	switch (result) {
	case WAIT_OBJECT_0:
		if (value_ptr)
			*value_ptr = thread->arg;
		CloseHandle(thread->handle);
		return 0;
	case WAIT_ABANDONED:
		return EINVAL;
	default:
		return GetLastError();
	}
}

#endif
