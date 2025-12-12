package jobmanager

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
)

var ErrTaskWorkerMismatch = errors.New("task worker mismatch")

// ImmutableTask - client read-only interface for reading task information
type ImmutableTask interface {
	ID() string
	Code() TaskCode
	GetInput() []byte
	GetOutput() []byte
	GetError() string
	IsExpired() bool
	IsCompleted() bool
	IsFailed() bool
}

// TaskDefinition defines task specification
type TaskDefinition struct {
	// TODO(med): add maximum number of failed execution attempts
	code  TaskCode
	input []byte
}

func NewTaskDefinition(code TaskCode, input []byte) (TaskDefinition, error) {
	return TaskDefinition{
		code:  code,
		input: input,
	}, nil
}

func (td TaskDefinition) Code() TaskCode {
	return td.code
}

func (td TaskDefinition) Input() []byte {
	return td.input
}

type TaskCode string

func (c TaskCode) String() string {
	return string(c)
}

type TaskStatus string

const (
	TaskTodo      TaskStatus = "todo"      // task is waiting to be picked up by a worker
	TaskStarted   TaskStatus = "started"   // task is currently being executed by a worker
	TaskCompleted TaskStatus = "completed" // task finished successfully
	TaskFailed    TaskStatus = "failed"    // task execution failed, task will be processed again
)

func (s TaskStatus) String() string {
	return string(s)
}

// task represents a unit of work within a job
type task struct {
	// IMPORTANT: when modifying fields don't forget about Clone
	id                string
	code              TaskCode
	status            TaskStatus
	processedByWorker string
	createdByWorker   string
	startedAt         time.Time
	completedAt       time.Time
	deadlineAt        time.Time
	heartbeatAt       time.Time
	attempt           int
	input             []byte
	output            []byte
	errorMsg          string
}

func newTask(code TaskCode, createdByWorkerID string, input []byte) *task {
	return &task{
		id:              uuid.New().String(),
		code:            code,
		status:          TaskTodo,
		input:           input,
		createdByWorker: createdByWorkerID,
	}
}

func (t *task) ID() string {
	return t.id
}

func (t *task) Code() TaskCode {
	return t.code
}

func (t *task) Status() TaskStatus {
	return t.status
}

func (t *task) GetInput() []byte {
	return t.input
}

func (t *task) GetOutput() []byte {
	return t.output
}

func (t *task) GetError() string {
	return t.errorMsg
}

// IsExpired checks if task deadline has expired
func (t *task) IsExpired() bool {
	if t.deadlineAt.IsZero() {
		return false
	}
	return time.Now().After(t.deadlineAt)
}

func (t *task) IsCompleted() bool {
	return t.Status() == TaskCompleted
}

func (t *task) IsFailed() bool {
	return t.Status() == TaskFailed
}

func (t *task) IsStarted() bool {
	return t.Status() == TaskStarted
}

func (t *task) IsProcessed() bool {
	return t.IsCompleted() || t.IsFailed()
}

func (t *task) ProcessedByWorker() string {
	return t.processedByWorker
}

func (t *task) Attempt() int {
	return t.attempt
}

func (t *task) StartedAt() time.Time {
	return t.startedAt
}

func (t *task) CompletedAt() time.Time {
	return t.completedAt
}

func (t *task) DeadlineAt() time.Time {
	return t.deadlineAt
}

func (t *task) HeartbeatAt() time.Time {
	return t.heartbeatAt
}

// ErrTaskWorkerMismatch
func (t *task) start(workerID string, deadline time.Duration) error {
	if !t.canBePickedUp() {
		if t.ProcessedByWorker() != workerID {
			return fmt.Errorf("cannot start task (id: %s; code: %s; status: %s): %w", t.id, t.code, t.status, ErrTaskWorkerMismatch)
		}
		return fmt.Errorf("cannot start task (id: %s; code: %s; status %s; deadline at: %s)", t.id, t.code, t.status, t.DeadlineAt().String())
	}

	t.status = TaskStarted
	t.processedByWorker = workerID
	t.startedAt = time.Now()
	t.deadlineAt = time.Now().Add(deadline)
	t.heartbeatAt = time.Now()
	t.attempt++

	return nil
}

func (t *task) complete(output []byte) error {
	if t.status != TaskStarted {
		return fmt.Errorf("cannot complete task (id: %s; code: %s) with status %s", t.id, t.code, t.status)
	}

	t.output = output
	t.status = TaskCompleted
	t.completedAt = time.Now()

	return nil
}

func (t *task) fail(errMsg string) error {
	if t.status != TaskStarted {
		return fmt.Errorf("cannot fail task (id: %s; code: %s) with status %s", t.id, t.code, t.status)
	}

	t.errorMsg = errMsg
	t.status = TaskFailed
	t.completedAt = time.Now()

	return nil
}

func (t *task) updateHeartbeat(workerID string, deadline time.Duration) error {
	if t.ProcessedByWorker() != workerID {
		return fmt.Errorf("cannot update task '%s' heartbeat: %w", t.Code(), ErrTaskWorkerMismatch)
	}
	if t.Status() != TaskStarted {
		return fmt.Errorf("cannot update task '%s' heartbeat - task not in runnig status (status: '%s')", t.Code(), t.Status())
	}

	t.heartbeatAt = time.Now()
	t.deadlineAt = time.Now().Add(deadline)

	return nil
}

func (t *task) canBePickedUp() bool {
	switch t.status {
	case TaskTodo:
		return true
	case TaskStarted:
		return t.IsExpired()
	case TaskFailed:
		return true // TODO(low): add limit on number of attempts
	default:
		return false
	}
}

func (t *task) Clone() *task {
	clone := *t
	clone.input = slices.Clone(t.input)
	clone.output = slices.Clone(t.output)

	return &clone
}
