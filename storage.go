package jobmanager

import (
	"context"
	"errors"
)

var (
	// ErrJobNotFound is returned when job is not found in storage
	ErrJobNotFound = errors.New("job not found")

	// ErrConcurrentModification is returned on version conflict (Version mismatch)
	ErrConcurrentModification = errors.New("concurrent modification detected")

	// ErrJobNotModified is returned when job hasn't changed (304 Not Modified)
	ErrJobNotModified = errors.New("job not modified")
)

type JobMeta struct {
	Code    JobCode
	IterNum uint64
	Version string
}

// Storage interface for storing job state
// Implementations must ensure operation atomicity through optimistic locking (Version)
type Storage interface {
	// GetJob retrieves latest job version (state with maximum iterNum and latest version)
	// ErrJobNotFound if job doesn't exist
	GetJob(ctx context.Context, jobCode JobCode) (*Job, error)

	// GetJobByMeta retrieves specific job iteration and checks version
	// ErrJobNotFound if job doesn't exist
	// ErrConcurrentModification if job doesn't exist
	GetJobByMeta(ctx context.Context, jobMeta JobMeta) (*Job, error)

	// FindJobMeta retrieves iterNum, version for job
	// ErrJobNotFound if job doesn't exist
	FindJobMeta(ctx context.Context, jobCode JobCode) (JobMeta, error)

	// SaveJob saves job atomically with Version check
	// ErrConcurrentModification if job was already modified
	SaveJob(ctx context.Context, job *Job) error
}

type JobDefinitionRegistry interface {
	GetJob(code JobCode) (JobDefinition, error)
}

type StorageStub struct{}

func (StorageStub) GetJob(ctx context.Context, jobCode JobCode) (*Job, error) {
	return nil, ErrJobNotFound
}

func (StorageStub) GetJobByMeta(ctx context.Context, jobMeta JobMeta) (*Job, error) {
	return nil, ErrJobNotFound
}

func (StorageStub) FindJobMeta(ctx context.Context, jobCode JobCode) (JobMeta, error) {
	return JobMeta{}, ErrJobNotFound
}

func (StorageStub) SaveJob(ctx context.Context, job *Job) error {
	return nil
}
