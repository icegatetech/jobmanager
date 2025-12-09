package jobmanager

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// TODO(low): if we add modification version to job, we can use caching more aggressively - return cached version on write if it's fresher, thus reducing misses on save.

// CachedStorage wrapper over Storage with in-memory caching
// Allows reducing number of storage requests.
type CachedStorage struct {
	inner   Storage
	mu      sync.RWMutex
	cache   map[JobCode]*Job
	lgr     Logger
	metrics Metrics
}

func NewCachedStorage(inner Storage, lgr Logger, metrics Metrics) *CachedStorage {
	return &CachedStorage{
		inner:   inner,
		cache:   make(map[JobCode]*Job),
		lgr:     lgr.With(slog.String("component", "cached_storage")),
		metrics: metrics,
	}
}

// GetJob retrieves latest job version
func (s *CachedStorage) GetJob(ctx context.Context, jobCode JobCode) (*Job, error) {
	meta, err := s.inner.FindJobMeta(ctx, jobCode)
	if err != nil {
		return nil, err
	}

	// If state hasn't changed - return state from cache.
	s.mu.RLock()
	cachedJob, ok := s.cache[jobCode]
	if ok && cachedJob.IterationNum() == meta.IterNum && cachedJob.Version() == meta.Version {
		s.mu.RUnlock()
		s.recordCacheHit(ctx, "GetJob")
		s.lgr.DebugContext(ctx, fmt.Sprintf("Get job '%s' from cache, tasks: %s", cachedJob.Code(), cachedJob.tasksAsString()))
		return cachedJob.Clone(), nil
	}
	s.mu.RUnlock()
	s.recordCacheMiss(ctx, "GetJob")

	job, err := s.inner.GetJobByMeta(ctx, meta)
	if err != nil {
		return nil, err
	}

	s.updateCache(job)

	return job, nil
}

// GetJobByMeta retrieves specific job iteration
func (s *CachedStorage) GetJobByMeta(ctx context.Context, jobMeta JobMeta) (*Job, error) {
	// If state hasn't changed - return state from cache.
	s.mu.RLock()
	cachedJob, ok := s.cache[jobMeta.Code]
	if ok && cachedJob.IterationNum() == jobMeta.IterNum && cachedJob.Version() == jobMeta.Version {
		s.mu.RUnlock()
		s.recordCacheHit(ctx, "GetJobByMeta")
		s.lgr.DebugContext(ctx, fmt.Sprintf("Get job by meta '%s' from cache, tasks: %s", cachedJob.Code(), cachedJob.tasksAsString()))
		return cachedJob.Clone(), nil
	}
	s.mu.RUnlock()
	s.recordCacheMiss(ctx, "GetJobByMeta")

	job, err := s.inner.GetJobByMeta(ctx, jobMeta)
	if err != nil {
		return nil, err
	}

	s.updateCacheIfNewer(job)

	return job.Clone(), nil
}

// FindJobMeta calls s3 FindJobMeta (pass-through call)
func (s *CachedStorage) FindJobMeta(ctx context.Context, jobCode JobCode) (JobMeta, error) {
	return s.inner.FindJobMeta(ctx, jobCode)
}

// SaveJob saves to s3, on successful save we update cache.
func (s *CachedStorage) SaveJob(ctx context.Context, job *Job) error {
	// TODO(med): need to take lock on specific job to avoid trying to save and get job simultaneously. Concurrent saves are doomed to fail. With simultaneous save/get there can be a race and outdated job state will be saved in cache.
	if err := s.inner.SaveJob(ctx, job); err != nil {
		return err
	}

	s.updateCache(job)

	s.lgr.DebugContext(ctx, fmt.Sprintf("Job '%s' saved to storage and cache (tasks: %s)", job.Code(), job.tasksAsString()))

	return nil
}

func (s *CachedStorage) updateCache(job *Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache[job.Code()] = job.Clone()
}

// updateCacheIfNewer updates cache only if passed job iteration is newer or equal to current in cache
func (s *CachedStorage) updateCacheIfNewer(job *Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.cache[job.Code()]
	if !ok {
		s.cache[job.Code()] = job.Clone()
		return
	}

	if job.IterationNum() >= existing.IterationNum() {
		s.cache[job.Code()] = job.Clone()
		return
	}
}

func (s *CachedStorage) recordCacheHit(ctx context.Context, method string) {
	s.metrics.RecordCacheHit(ctx, method)
}

func (s *CachedStorage) recordCacheMiss(ctx context.Context, method string) {
	s.metrics.RecordCacheMiss(ctx, method)
}
