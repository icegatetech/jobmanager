package jobmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// TODO(low): need mechanism to clean up old job states. Required for iter num restart and reduced storage load

const jobStateFilePrefix = "state-"
const jobStateFileExtension = ".json"

// S3StorageConfig configuration for S3 storage
type S3StorageConfig struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	BucketName      string
	UseSSL          bool
	Region          string
	BucketPrefix    string // Prefix for all jobs (e.g., "jobs/")
}

// jobStateFile represents information about job state file in S3
type jobStateFile struct {
	IterNum uint64
	ETag    string
	Key     string
}

type s3Storage struct {
	client       *minio.Client
	bucketName   string
	bucketPrefix string
	lgr          Logger
	metrics      Metrics
	registry     JobDefinitionRegistry
	retrier      Retrier
}

func NewS3Storage(ctx context.Context, cfg S3StorageConfig, lgr Logger, metrics Metrics, registry JobDefinitionRegistry, retrier Retrier) (Storage, error) {
	minioClient, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO client: %w", err)
	}

	// TODO(med): add check that conditional requests work for specific S3. For example, old Minio versions ignore the header and atomicity breaks

	// Create bucket if it doesn't exist
	bucketExists, err := minioClient.BucketExists(ctx, cfg.BucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to check bucket existence: %w", err)
	}

	if !bucketExists {
		err = minioClient.MakeBucket(ctx, cfg.BucketName, minio.MakeBucketOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
		lgr.InfoContext(ctx, fmt.Sprintf("Created bucket %s", cfg.BucketName))
	}

	return s3Storage{
		client:       minioClient,
		bucketName:   cfg.BucketName,
		bucketPrefix: cfg.BucketPrefix,
		lgr:          lgr.With(slog.String("component", "s3_storage")),
		metrics:      metrics,
		registry:     registry,
		retrier:      retrier,
	}, nil
}

func (s s3Storage) GetJob(ctx context.Context, jobCode JobCode) (*Job, error) {
	// TODO(low): perhaps should try to get current job iteration first and read new file on miss. But if job has few tasks, we'll miss often and make extra requests

	// TODO(high): add error type that allows request retries
	var job *Job
	err := s.retrier.Retry(ctx, func() (needRetry bool, err error) {
		jobMeta, err := s.FindJobMeta(ctx, jobCode)
		if err != nil {
			return false, err
		}

		job, err = s.GetJobByMeta(ctx, jobMeta)
		if err != nil {
			if errors.Is(err, ErrConcurrentModification) {
				return true, err
			}
			return false, err
		}

		return false, nil
	})
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (s s3Storage) FindJobMeta(ctx context.Context, jobCode JobCode) (JobMeta, error) {
	prefix := s.buildJobPath(jobCode)

	startTime := time.Now()
	objectsCh := s.client.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
		MaxKeys:   1, // Take only first file since inverted name order is used (DESC)
	})

	for object := range objectsCh {
		if object.Err != nil {
			s.requestComplete(ctx, "LIST", object.Err, time.Since(startTime))
			return JobMeta{}, fmt.Errorf("failed to listJobs objects: %w", object.Err)
		}

		// Parse iterNum from filename
		iterNum, err := s.parseIterNumFromFilePath(object.Key)
		if err != nil {
			return JobMeta{}, err
		}

		return JobMeta{
			Code:    jobCode,
			IterNum: iterNum,
			Version: object.ETag,
		}, nil
	}

	s.requestComplete(ctx, "LIST", nil, time.Since(startTime))

	return JobMeta{}, ErrJobNotFound
}

// ErrConcurrentModification
func (s s3Storage) GetJobByMeta(ctx context.Context, jobMeta JobMeta) (*Job, error) {
	opts := minio.GetObjectOptions{}
	err := opts.SetMatchETag(jobMeta.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to set etag to get job state: %w", err)
	}
	key := s.buildStatePath(jobMeta.Code, jobMeta.IterNum)

	startTime := time.Now()
	object, err := s.client.GetObject(ctx, s.bucketName, key, opts)
	if err != nil {
		s.requestComplete(ctx, http.MethodGet, err, time.Since(startTime))
		return nil, fmt.Errorf("failed to get job object %s state: %w", key, s.checkConcurrentModification(err))
	}
	defer func(object *minio.Object) {
		if err := object.Close(); err != nil {
			s.lgr.WarnContext(ctx, fmt.Errorf("failed to close job %s: %w", key, err).Error())
		}
	}(object)

	data, err := io.ReadAll(object)
	if err != nil {
		s.requestComplete(ctx, http.MethodGet, err, time.Since(startTime))
		return nil, fmt.Errorf("failed to read job object %s state: %w", key, s.checkConcurrentModification(err))
	}
	s.requestComplete(ctx, http.MethodGet, nil, time.Since(startTime))

	job, err := s.deserializeJob(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize job: %w", err)
	}

	job.UpdateVersion(jobMeta.Version)

	err = s.enrichJob(job)
	if err != nil {
		return nil, err
	}

	return job, nil
}

// SaveJob saves job atomically with Version check
func (s s3Storage) SaveJob(ctx context.Context, job *Job) error {
	if job.IsStarted() {
		s.lgr.DebugContext(ctx, fmt.Sprintf("Saving next job iteration (id: %s, code: %s; status: %s; version: %s; tasks: %s)", job.id, job.Code(), job.Status(), job.Version(), job.tasksAsString()))
		return s.putNextIteration(ctx, job)
	}

	s.lgr.DebugContext(ctx, fmt.Sprintf("Saving current job iteration (id: %s, code: %s; status: %s; version: %s; tasks: %s)", job.id, job.Code(), job.Status(), job.Version(), job.tasksAsString()))
	return s.putCurrentIteration(ctx, job)
}

// ErrConcurrentModification
func (s s3Storage) putNextIteration(ctx context.Context, job *Job) error {
	key := s.buildStatePath(job.Code(), job.IterationNum())

	data, err := s.serializeJob(job)
	if err != nil {
		return fmt.Errorf("failed to serialize job %s: %w", job.Code(), err)
	}

	opts := minio.PutObjectOptions{
		ContentType: "application/json",
	}
	opts.SetMatchETagExcept("*")

	startTime := time.Now()
	info, err := s.client.PutObject(ctx, s.bucketName, key, bytes.NewReader(data), int64(len(data)), opts)

	if err != nil {
		s.requestComplete(ctx, http.MethodPut, err, time.Since(startTime))
		return fmt.Errorf("failed to create job %s iteration: %w", job.Code(), s.checkConcurrentModification(err))
	}
	s.requestComplete(ctx, http.MethodPut, nil, time.Since(startTime))

	job.UpdateVersion(info.ETag)

	return nil
}

// ErrConcurrentModification
func (s s3Storage) putCurrentIteration(ctx context.Context, job *Job) error {
	key := s.buildStatePath(job.Code(), job.IterationNum())

	data, err := s.serializeJob(job)
	if err != nil {
		return fmt.Errorf("failed to serialize job: %w", err)
	}

	// Atomic write with If-Match
	opts := minio.PutObjectOptions{
		ContentType: "application/json",
	}
	opts.SetMatchETag(job.Version())

	startTime := time.Now()
	info, err := s.client.PutObject(ctx, s.bucketName, key, bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		s.requestComplete(ctx, http.MethodPut, err, time.Since(startTime))
		return fmt.Errorf("failed to save job: %w", s.checkConcurrentModification(err))
	}
	s.requestComplete(ctx, http.MethodPut, nil, time.Since(startTime))

	job.UpdateVersion(info.ETag)

	return nil
}

// ErrConcurrentModification
func (s s3Storage) checkConcurrentModification(err error) error {
	if err == nil {
		return nil
	}
	var errResp minio.ErrorResponse
	if errors.As(err, &errResp) && (errResp.Code == "PreconditionFailed" || errResp.StatusCode == http.StatusPreconditionFailed) {
		return fmt.Errorf("%w: %s", ErrConcurrentModification, errResp.Resource)
	}
	return err
}

func (s s3Storage) buildJobPath(jobCode JobCode) string {
	return fmt.Sprintf("%s%s/", s.bucketPrefix, jobCode)
}

// buildStatePath builds path to state file with inverted iterNum for S3 sorting
func (s s3Storage) buildStatePath(jobCode JobCode, iterNum uint64) string {
	// Invert iterNum so new files are at the beginning of list (S3 sorts by name) and LIST request is fast
	invIterNum := math.MaxUint64 - iterNum
	return fmt.Sprintf("%s%s%020d%s", s.buildJobPath(jobCode), jobStateFilePrefix, invIterNum, jobStateFileExtension)
}

// parseIterNumFromFilePath parses iterNum from file path
func (s s3Storage) parseIterNumFromFilePath(filePath string) (uint64, error) {
	// Expected format: {prefix}/{jobCode}/state-00001.json
	parts := strings.Split(filePath, "/")
	if len(parts) < 2 {
		return 0, fmt.Errorf("cannot split file path %s", filePath)
	}

	filename := parts[len(parts)-1]
	if !strings.HasPrefix(filename, jobStateFilePrefix) || !strings.HasSuffix(filename, jobStateFileExtension) {
		return 0, fmt.Errorf("invalid filename format %q", filename)
	}

	iterNumStr := strings.TrimPrefix(filename, "state-")
	iterNumStr = strings.TrimSuffix(iterNumStr, jobStateFileExtension)

	invIterNum, err := strconv.ParseUint(iterNumStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse iterNum: %w", err)
	}

	// Restore original iterNum
	return math.MaxUint64 - invIterNum, nil
}

// enrichJob enriches job with data from JobDefinition
func (s s3Storage) enrichJob(job *Job) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}

	jobDef, err := s.registry.GetJob(job.Code())
	if err != nil {
		return err
	}
	job.maxIterations = jobDef.MaxIterations()

	return nil
}

func (s s3Storage) requestComplete(ctx context.Context, operation string, err error, duration time.Duration) {
	status := "OK"
	if err != nil {
		var errResp minio.ErrorResponse
		if errors.As(err, &errResp) {
			status = errResp.Code
		} else {
			if len(err.Error()) > 20 {
				status = err.Error()[:20]
			} else {
				status = err.Error()
			}
		}
	}
	s.metrics.RecordS3Operation(ctx, operation, status, duration)
}

// Structures for JSON serialization

type taskJSON struct {
	ID                string     `json:"id"`
	Code              string     `json:"code"`
	Status            TaskStatus `json:"status"`
	ProcessedByWorker string     `json:"processed_by,omitempty"`
	StartedAt         *time.Time `json:"started_at,omitempty"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`
	DeadlineAt        *time.Time `json:"deadline_at,omitempty"`
	HeartbeatAt       *time.Time `json:"heartbeat_at,omitempty"`
	Attempt           int        `json:"attempt"`
	Input             []byte     `json:"input,omitempty"`
	Output            []byte     `json:"output,omitempty"`
	Error             string     `json:"error,omitempty"`
}

type jobJSON struct {
	ID          string         `json:"id"`
	Code        string         `json:"code"`
	IterNum     uint64         `json:"iter_num"`
	Status      JobStatus      `json:"status"`
	Tasks       []taskJSON     `json:"tasks"`
	UpdatedBy   string         `json:"updated_by,omitempty"`
	StartedAt   *time.Time     `json:"started_at"`
	RunningAt   *time.Time     `json:"running_at,omitempty"`
	CompletedAt *time.Time     `json:"completed_at,omitempty"`
	NextStartAt *time.Time     `json:"next_start_at,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

func (s s3Storage) serializeJob(job *Job) ([]byte, error) {
	// TODO(high): create interface for job to extract only needed fields without ability to modify job from here
	j := jobJSON{
		ID:        job.id,
		Code:      job.code.String(),
		IterNum:   job.iterNum,
		Status:    job.status,
		UpdatedBy: job.updatedByWorkerID,
		Metadata:  job.metadata,
	}

	// Convert time.Time to pointers
	if !job.startedAt.IsZero() {
		j.StartedAt = &job.startedAt
	}
	if !job.runningAt.IsZero() {
		j.RunningAt = &job.runningAt
	}
	if !job.completedAt.IsZero() {
		j.CompletedAt = &job.completedAt
	}
	if !job.nextStartAt.IsZero() {
		j.NextStartAt = &job.nextStartAt
	}

	// Serialize tasks
	j.Tasks = make([]taskJSON, 0, len(job.tasksById))
	for _, tsk := range job.tasksById {
		t := taskJSON{
			ID:                tsk.id,
			Code:              tsk.code.String(),
			Status:            tsk.status,
			ProcessedByWorker: tsk.processedByWorker,
			Attempt:           tsk.attempt,
			Input:             tsk.input,
			Output:            tsk.output,
			Error:             tsk.errorMsg,
		}

		if !tsk.startedAt.IsZero() {
			t.StartedAt = &tsk.startedAt
		}
		if !tsk.completedAt.IsZero() {
			t.CompletedAt = &tsk.completedAt
		}
		if !tsk.deadlineAt.IsZero() {
			t.DeadlineAt = &tsk.deadlineAt
		}
		if !tsk.heartbeatAt.IsZero() {
			t.HeartbeatAt = &tsk.heartbeatAt
		}

		j.Tasks = append(j.Tasks, t)
	}

	return json.MarshalIndent(j, "", "  ")
}

// deserializeJob deserializes job from JSON
func (s s3Storage) deserializeJob(data []byte) (*Job, error) {
	// TODO(med): replace native json format with binary format like MessagePack or CBOR for best performance.

	var j jobJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, err
	}

	// TODO(high): use constructor
	job := &Job{
		id:                j.ID,
		code:              JobCode(j.Code),
		iterNum:           j.IterNum,
		status:            j.Status,
		tasksById:         make(map[string]*task),
		updatedByWorkerID: j.UpdatedBy,
		metadata:          j.Metadata,
	}

	// Convert pointers to time.Time
	if j.StartedAt != nil {
		job.startedAt = *j.StartedAt
	}
	if j.RunningAt != nil {
		job.runningAt = *j.RunningAt
	}
	if j.CompletedAt != nil {
		job.completedAt = *j.CompletedAt
	}
	if j.NextStartAt != nil {
		job.nextStartAt = *j.NextStartAt
	}

	// Deserialize tasks
	for _, t := range j.Tasks {
		// TODO(high): use constructor
		tsk := &task{
			id:                t.ID,
			code:              TaskCode(t.Code),
			status:            t.Status,
			processedByWorker: t.ProcessedByWorker,
			attempt:           t.Attempt,
			input:             t.Input,
			output:            t.Output,
			errorMsg:          t.Error,
		}

		if t.StartedAt != nil {
			tsk.startedAt = *t.StartedAt
		}
		if t.CompletedAt != nil {
			tsk.completedAt = *t.CompletedAt
		}
		if t.DeadlineAt != nil {
			tsk.deadlineAt = *t.DeadlineAt
		}
		if t.HeartbeatAt != nil {
			tsk.heartbeatAt = *t.HeartbeatAt
		}

		job.tasksById[tsk.id] = tsk
	}

	return job, nil
}
