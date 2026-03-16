#ifndef JOBCONTROL_H
#define JOBCONTROL_H

#include <stddef.h>

#include "threading.h"

typedef struct {
	pthread_mutex_t mutex;
	pthread_cond_t start_cond;
	pthread_cond_t done_cond;
	pthread_cond_t free_cond;
	unsigned generation;
	int active_workers;
	size_t result;
	int shutdown;
} MT_JobControl;

static int MTJobControl_init(MT_JobControl *job)
{
	pthread_mutex_init(&job->mutex, NULL);
	pthread_cond_init(&job->start_cond, NULL);
	pthread_cond_init(&job->done_cond, NULL);
	pthread_cond_init(&job->free_cond, NULL);
	job->generation = 0;
	job->active_workers = 0;
	job->result = 0;
	job->shutdown = 0;
	return 0;
}

static void MTJobControl_destroy(MT_JobControl *job)
{
	pthread_cond_destroy(&job->free_cond);
	pthread_cond_destroy(&job->done_cond);
	pthread_cond_destroy(&job->start_cond);
	pthread_mutex_destroy(&job->mutex);
}

static int MTJobControl_wait(MT_JobControl *job, unsigned *generation)
{
	pthread_mutex_lock(&job->mutex);
	while (*generation == job->generation && !job->shutdown)
		pthread_cond_wait(&job->start_cond, &job->mutex);

	if (job->shutdown) {
		pthread_mutex_unlock(&job->mutex);
		return 0;
	}

	*generation = job->generation;
	pthread_mutex_unlock(&job->mutex);
	return 1;
}

static void MTJobControl_start(MT_JobControl *job, int workers)
{
	pthread_mutex_lock(&job->mutex);
	job->active_workers = workers;
	job->result = 0;
	job->generation++;
	pthread_cond_broadcast(&job->start_cond);
	pthread_mutex_unlock(&job->mutex);
}

static size_t MTJobControl_wait_done(MT_JobControl *job)
{
	size_t result;

	pthread_mutex_lock(&job->mutex);
	while (job->active_workers > 0)
		pthread_cond_wait(&job->done_cond, &job->mutex);
	result = job->result;
	pthread_mutex_unlock(&job->mutex);

	return result;
}

static void MTJobControl_finish_worker(MT_JobControl *job)
{
	pthread_mutex_lock(&job->mutex);
	if (job->active_workers > 0) {
		job->active_workers--;
		if (job->active_workers == 0)
			pthread_cond_signal(&job->done_cond);
	}
	pthread_mutex_unlock(&job->mutex);
}

static int MTJobControl_should_stop(MT_JobControl *job)
{
	int stop;

	pthread_mutex_lock(&job->mutex);
	stop = (job->result != 0 || job->shutdown);
	pthread_mutex_unlock(&job->mutex);

	return stop;
}

static size_t MTJobControl_set_result(MT_JobControl *job, size_t result)
{
	if (!result)
		return 0;

	pthread_mutex_lock(&job->mutex);
	if (job->result == 0)
		job->result = result;
	result = job->result;
	pthread_cond_broadcast(&job->free_cond);
	pthread_cond_signal(&job->done_cond);
	pthread_mutex_unlock(&job->mutex);

	return result;
}

static void MTJobControl_shutdown(MT_JobControl *job)
{
	pthread_mutex_lock(&job->mutex);
	job->shutdown = 1;
	pthread_cond_broadcast(&job->start_cond);
	pthread_cond_broadcast(&job->free_cond);
	pthread_cond_broadcast(&job->done_cond);
	pthread_mutex_unlock(&job->mutex);
}

#endif
