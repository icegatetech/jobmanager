package testenv

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/minio"
)

// MinIOEnv provides a test environment with MinIO
type MinIOEnv struct {
	t         *testing.T
	container *minio.MinioContainer
	endpoint  string
	username  string
	password  string
}

// NewMinIOEnv creates a new MinIO test environment
func NewMinIOEnv(ctx context.Context, t *testing.T) (*MinIOEnv, error) {
	minioContainer, err := minio.Run(ctx,
		"quay.io/minio/minio:RELEASE.2025-01-20T14-49-07Z", // Release 2024-01-16 doesn't support If-None-Match: *
		minio.WithUsername("minioadmin"),
		minio.WithPassword("minioadmin"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start MinIO container: %w", err)
	}

	endpoint, err := minioContainer.ConnectionString(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get MinIO endpoint: %w", err)
	}

	return &MinIOEnv{
		t:         t,
		container: minioContainer,
		endpoint:  endpoint,
		username:  "minioadmin",
		password:  "minioadmin",
	}, nil
}

// Endpoint returns the MinIO endpoint
func (e *MinIOEnv) Endpoint() string {
	return e.endpoint
}

// Username returns the MinIO username
func (e *MinIOEnv) Username() string {
	return e.username
}

// Password returns the MinIO password
func (e *MinIOEnv) Password() string {
	return e.password
}

// Close stops the MinIO container
// Note: we use a fresh context because the test context might be cancelled
func (e *MinIOEnv) Close() error {
	if e.container != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := e.container.Terminate(ctx); err != nil {
			return fmt.Errorf("failed to terminate MinIO container: %w", err)
		}
	}
	return nil
}
