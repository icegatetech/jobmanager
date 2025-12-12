package jobmanager

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// need to take lock on specific job to avoid trying to save and get job simultaneously. Concurrent saves are doomed to fail. With simultaneous save/get there can be a race and outdated job state will be saved in cache.
type jobWithLock struct {
	l *sync.RWMutex
	j *Job
}

// CachedStorage wrapper over Storage with in-memory caching
// Allows reducing number of storage requests.
type CachedStorage struct {
	inner   Storage
	lock    sync.RWMutex
	cache   map[JobCode]*jobWithLock
	lgr     Logger
	metrics Metrics
}

func NewCachedStorage(inner Storage, lgr Logger, metrics Metrics) *CachedStorage {
	return &CachedStorage{
		inner:   inner,
		cache:   make(map[JobCode]*jobWithLock),
		lgr:     lgr.With(slog.String("component", "cached_storage")),
		metrics: metrics,
	}
}

// GetJob retrieves latest job version
func (s *CachedStorage) GetJob(ctx context.Context, code JobCode) (*Job, error) {
	// TODO(low): try to split the lock into read at the beginning and write at the end

	s.lock.Lock()
	cachedJob, cachedJobFound := s.cache[code]
	if !cachedJobFound {
		cachedJob = &jobWithLock{l: &sync.RWMutex{}}
		s.cache[code] = cachedJob
	}
	cachedJob.l.Lock()
	defer cachedJob.l.Unlock()
	s.lock.Unlock()

	meta, err := s.inner.FindJobMeta(ctx, code)
	if err != nil {
		return nil, err
	}

	// If state hasn't changed - return state from cache.
	if cachedJob.j != nil && cachedJob.j.IterationNum() == meta.IterNum && cachedJob.j.Version() == meta.Version {
		s.recordCacheHit(ctx, "GetJob")
		s.lgr.DebugContext(ctx, fmt.Sprintf("Get job '%s' from cache (id: %s, iter: %d, version: %s, tasks: %s)", cachedJob.j.Code(), cachedJob.j.ID(), cachedJob.j.IterationNum(), cachedJob.j.Version(), cachedJob.j.tasksAsString()))

		return cachedJob.j.Clone(), nil
	}
	s.recordCacheMiss(ctx, "GetJob")

	job, err := s.inner.GetJobByMeta(ctx, meta)
	if err != nil {
		return nil, err
	}

	cachedJob.j = job.Clone()

	return job, nil
}

// GetJobByMeta retrieves specific job iteration
func (s *CachedStorage) GetJobByMeta(ctx context.Context, meta JobMeta) (*Job, error) {
	s.lock.Lock()
	cachedJob, cachedJobFound := s.cache[meta.Code]
	if !cachedJobFound {
		cachedJob = &jobWithLock{l: &sync.RWMutex{}}
		s.cache[meta.Code] = cachedJob
	}
	cachedJob.l.Lock()
	defer cachedJob.l.Unlock()
	s.lock.Unlock()

	// If state hasn't changed - return state from cache.
	if cachedJob.j != nil && cachedJob.j.IterationNum() == meta.IterNum && cachedJob.j.Version() == meta.Version {
		s.recordCacheHit(ctx, "GetJobByMeta")
		s.lgr.DebugContext(ctx, fmt.Sprintf("Get job '%s' from cache (id: %s, iter: %d, version: %s, tasks: %s)", cachedJob.j.Code(), cachedJob.j.ID(), cachedJob.j.IterationNum(), cachedJob.j.Version(), cachedJob.j.tasksAsString()))

		return cachedJob.j.Clone(), nil
	}
	s.recordCacheMiss(ctx, "GetJobByMeta")

	job, err := s.inner.GetJobByMeta(ctx, meta)
	if err != nil {
		return nil, err
	}

	s.updateCacheIfNewer(job, cachedJob)

	return job.Clone(), nil
}

// FindJobMeta calls s3 FindJobMeta (pass-through call)
func (s *CachedStorage) FindJobMeta(ctx context.Context, jobCode JobCode) (JobMeta, error) {
	return s.inner.FindJobMeta(ctx, jobCode)
}

// SaveJob saves to s3, on successful save we update cache.
func (s *CachedStorage) SaveJob(ctx context.Context, job *Job) error {
	// TODO(low): if we add modification version (not etag, but sequence) to job, we can use caching more aggressively - return cached version on write if it's fresher, thus reducing misses on save.

	s.lock.Lock()
	cachedJob, cachedJobFound := s.cache[job.Code()]
	if !cachedJobFound {
		cachedJob = &jobWithLock{l: &sync.RWMutex{}}
		s.cache[job.Code()] = cachedJob
	}
	cachedJob.l.Lock()
	defer cachedJob.l.Unlock()
	s.lock.Unlock()

	if err := s.inner.SaveJob(ctx, job); err != nil {
		return err
	}

	cachedJob.j = job.Clone()

	s.lgr.DebugContext(ctx, fmt.Sprintf("Job '%s' saved to storage and cache (id: %s, iter: %d, version: %s, tasks: %s)", job.Code(), job.ID(), job.IterationNum(), job.Version(), job.tasksAsString()))

	return nil
}

// updateCacheIfNewer updates cache only if passed job iteration is newer or equal to current in cache
func (s *CachedStorage) updateCacheIfNewer(storageJob *Job, cachedJob *jobWithLock) {
	if cachedJob.j != nil {
		cachedJob.j = storageJob.Clone()
		return
	}

	if storageJob.IterationNum() >= cachedJob.j.IterationNum() {
		cachedJob.j = storageJob.Clone()
		return
	}
}

func (s *CachedStorage) recordCacheHit(ctx context.Context, method string) {
	s.metrics.RecordCacheHit(ctx, method)
}

func (s *CachedStorage) recordCacheMiss(ctx context.Context, method string) {
	s.metrics.RecordCacheMiss(ctx, method)
}
