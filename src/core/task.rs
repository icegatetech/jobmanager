use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{JobDefinitionId, JobError, TaskLimits};

/// Task identifier used in job definitions and execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskCode(String);

impl TaskCode {
    /// Wraps a raw code as-is; nothing is validated here.
    ///
    /// The code is the lookup key for the task's executor within a job. For an initial task the
    /// two are bound together by [`JobBuilder::add_task`](crate::JobBuilder::add_task); for a task
    /// created at runtime the code must match one registered with
    /// [`JobBuilder::add_task_executor`](crate::JobBuilder::add_task_executor), and a mismatch
    /// surfaces only when the task is about to be executed.
    pub fn new(code: impl Into<String>) -> Self {
        Self(code.into())
    }

    /// Borrows the raw code.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TaskCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for TaskCode {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for TaskCode {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Task lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    // TODO(med): add status transitions
    /// Task is waiting to be picked up by a worker.
    Todo,
    /// Task is blocked by dependencies.
    ///
    /// Assigned at creation to any task declaring dependencies. It becomes `Todo` when every
    /// dependency has reached `Completed`; the unblocking happens lazily while a worker looks
    /// for work, not at the moment the last dependency finishes.
    Blocked,
    /// Task is currently being executed by a worker.
    ///
    /// Ownership is bounded by the task deadline: once it passes, another worker may take the
    /// task over as long as the task still has attempts left, so a slow executor can end up
    /// running concurrently with its replacement. Once the budget is spent, an expired task is
    /// failed instead of taken over - but the executor already running keeps running either way.
    Started,
    /// Task finished successfully.
    ///
    /// Terminal - such a task is never picked up again, and a job iteration completes only when
    /// all of its tasks are in this state.
    Completed,
    /// Task execution failed, task will be processed again.
    ///
    /// Retried until the task's attempt budget is spent; after that it is terminal - it is never
    /// picked up again and its job iteration ends as `JobStatus::Failed`.
    Failed,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Todo => write!(f, "todo"),
            Self::Blocked => write!(f, "blocked"),
            Self::Started => write!(f, "started"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

/// Number of execution attempts a task gets before it is terminally failed.
///
/// Sized for transient failures (a flaky object-store call, a lost worker): five
/// attempts absorb those, while a task failing deterministically stops retrying
/// instead of blocking its dependents and the job's next iteration forever.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;

/// How a task names another task it waits for.
///
/// Handed out by [`JobBuilder::add_task`](crate::JobBuilder::add_task) for an initial task and by
/// [`JobHandle::add_task`](crate::JobHandle::add_task) for one created at runtime; it cannot be
/// constructed by a caller, so a reference always names a task that was really declared.
///
/// An initial task is named by its position, not by an identifier: identifiers are minted per
/// iteration, while the position is what the description carries. The two forms are therefore not
/// interchangeable - a reference to an initial task is rejected by `JobHandle::add_task`, and a
/// reference to a runtime task is rejected by `JobsManagerBuilder::build`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskRef(TaskRefKind);

/// The two forms a [`TaskRef`] takes. Kept private so the invariant above cannot be bypassed by
/// constructing a variant directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskRefKind {
    /// Position of an initial task in the description of `job_definition`.
    Initial {
        job_definition: JobDefinitionId,
        position: usize,
    },
    /// Identifier of a task that already exists in the current iteration.
    Created(Uuid),
}

impl TaskRef {
    /// Names the initial task sitting at `position` in the description of `job_definition`.
    pub(crate) const fn initial(job_definition: JobDefinitionId, position: usize) -> Self {
        Self(TaskRefKind::Initial {
            job_definition,
            position,
        })
    }

    /// Names a task that already exists in the current iteration.
    pub(crate) const fn created(id: Uuid) -> Self {
        Self(TaskRefKind::Created(id))
    }

    /// Borrows the form of the reference, for the layer that has to tell the two apart.
    pub(crate) const fn kind(self) -> TaskRefKind {
        self.0
    }
}

/// Definition of a task to be executed.
#[derive(Debug, Clone)]
pub struct TaskDefinition {
    code: TaskCode,
    input: Vec<u8>,
    timeout: std::time::Duration,
    depends_on: Vec<TaskRef>,
    max_attempts: u32,
}

impl TaskDefinition {
    /// Builds a definition with no input, no dependencies and the default attempt budget.
    ///
    /// `timeout` is not a cancellation budget: it sets the deadline after which another worker is
    /// allowed to take the task over, while the original executor keeps running.
    ///
    /// Nothing is rejected here. A definition is checked where a task is actually created from it -
    /// by [`JobsManagerBuilder::build`](crate::JobsManagerBuilder::build) for an initial task and by
    /// [`JobHandle::add_task`](crate::JobHandle::add_task) for a runtime one - because the limits it
    /// is checked against belong to the job, which a bare definition knows nothing about.
    pub fn new(code: impl Into<TaskCode>, timeout: std::time::Duration) -> Self {
        Self {
            code: code.into(),
            input: Vec::new(),
            timeout,
            depends_on: Vec::new(),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        }
    }

    /// Attaches the payload the executor reads through
    /// [`TaskContext::input`](crate::TaskContext::input), replacing whatever was attached before.
    #[must_use]
    pub fn with_input(mut self, input: Vec<u8>) -> Self {
        self.input = input;
        self
    }

    /// Code under which this task's executor is looked up in the job definition.
    pub const fn code(&self) -> &TaskCode {
        &self.code
    }

    /// Return the input payload.
    pub fn input(&self) -> &[u8] {
        &self.input
    }

    /// Time after which a started task may be taken over by another worker.
    pub const fn timeout(&self) -> std::time::Duration {
        self.timeout
    }

    /// Makes the task wait for every reference in `depends_on`, replacing whatever it waited for
    /// before.
    ///
    /// The replacement covers this definition only: for an initial task,
    /// [`JobBuilder::depends_on`](crate::JobBuilder::depends_on) adds its own declarations to this
    /// list at build time rather than overwriting it, so a task declared through both channels
    /// waits for the union.
    ///
    /// A reference handed out by [`JobBuilder::add_task`](crate::JobBuilder::add_task) belongs to a
    /// job description and is only legal on an initial task; one handed out by
    /// [`JobHandle::add_task`](crate::JobHandle::add_task) is only legal on a runtime task. Mixing
    /// them is rejected where the task is created.
    #[must_use]
    pub fn with_dependencies(mut self, depends_on: Vec<TaskRef>) -> Self {
        self.depends_on = depends_on;
        self
    }

    /// Caps how many times the task may be started before it is terminally
    /// failed and never picked up again. Defaults to [`DEFAULT_MAX_ATTEMPTS`].
    ///
    /// The budget covers every start of the task, not only failures: a takeover
    /// of an expired task spends an attempt too. A zero budget is rejected where the task is
    /// created, in the same place as the rest of the definition.
    #[must_use]
    pub const fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Returns the execution attempt budget.
    const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    pub(crate) fn depends_on(&self) -> &[TaskRef] {
        &self.depends_on
    }

    /// Checks the definition against the limits of the job it is about to join.
    ///
    /// Called from the two points a definition enters a job: `JobDefinition::new` for an initial
    /// task and `Job::add_task` for one created at runtime. There are two because a job description
    /// is checked once, when it is assembled, and every iteration is then planned from the same
    /// checked description, while a runtime definition first exists at the moment it is added.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Other`] if the timeout is zero or beyond the millisecond range the
    /// stored state uses, if the attempt budget is zero, or if the input exceeds `limits`.
    pub(crate) fn validate(&self, limits: TaskLimits) -> Result<(), JobError> {
        if self.timeout.is_zero() {
            return Err(JobError::Other("task timeout must be positive".into()));
        }
        if Duration::from_std(self.timeout).is_err() {
            return Err(JobError::Other("task timeout is too large".into()));
        }
        if self.max_attempts == 0 {
            return Err(JobError::Other("task max attempts must be positive".into()));
        }
        if self.input.len() > limits.max_input_bytes {
            return Err(JobError::Other(format!(
                "task input size {} exceeds limit {}",
                self.input.len(),
                limits.max_input_bytes
            )));
        }
        Ok(())
    }
}

/// Read-only view of a task for workers and clients.
pub trait ImmutableTask: Send + Sync {
    /// Identifier of the task, unique within its job and stable across retries.
    ///
    /// It is assigned when the task is created, so it is only known after
    /// [`JobHandle::add_task`](crate::JobHandle::add_task) returns - a definition alone cannot be
    /// addressed by id.
    fn id(&self) -> &Uuid;

    /// Code the task was defined with, used to resolve its executor.
    ///
    /// Not unique: a job may hold several tasks sharing one code, which is why
    /// [`JobHandle::get_tasks_by_code`](crate::JobHandle::get_tasks_by_code) returns a collection.
    fn code(&self) -> &TaskCode;

    /// Input payload the task was created with. Empty if none was supplied.
    fn get_input(&self) -> &[u8];

    /// Output payload, empty until the task is completed.
    ///
    /// An empty slice is therefore ambiguous on its own - check [`Self::is_completed`] to tell a
    /// pending task from one that completed with no output.
    fn get_output(&self) -> &[u8];

    /// Message of the last failure, empty if the task never failed.
    ///
    /// It is not cleared when the task is retried, so it may describe an earlier attempt of a
    /// task that is currently running.
    fn get_error(&self) -> &str;

    /// Ids of tasks that must complete before this one becomes runnable.
    ///
    /// An existing task always has its dependencies resolved, so this is an id rather than a
    /// [`TaskRef`]: a reference would be wider than the domain here.
    // TODO(med): return TaskId once the newtype exists
    fn depends_on(&self) -> &[Uuid];

    /// Whether the task's execution deadline has passed.
    ///
    /// True only while a deadline is set, i.e. after the task has been started at least once.
    /// An expired task may already have been taken over by another worker.
    fn is_expired(&self) -> bool;

    /// Whether the task finished successfully. Terminal - it will not be executed again.
    fn is_completed(&self) -> bool;

    /// Whether the last attempt failed.
    ///
    /// Not terminal on its own: the task is retried while [`Self::attempts`] is below
    /// [`Self::max_attempts`], and becomes terminal only once that budget is spent.
    fn is_failed(&self) -> bool;

    /// Number of times the task has been started, including the attempt in progress.
    ///
    /// Counts takeovers of expired tasks as well, so it can exceed the number of failures. It is
    /// compared against [`Self::max_attempts`] to decide whether the task may run again.
    fn attempts(&self) -> u32;

    /// Attempt budget the task was defined with: once [`Self::attempts`] reaches it,
    /// the task is never picked up again and its job iteration ends as failed.
    fn max_attempts(&self) -> u32;
}

// Task - internal task representation
#[derive(Debug, Clone)]
pub(crate) struct Task {
    id: Uuid,
    code: TaskCode,
    status: TaskStatus,
    processing_by_worker: Option<Uuid>,
    created_by_worker: Uuid,
    timeout: Duration,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    deadline_at: Option<DateTime<Utc>>,
    attempt: u32,
    max_attempts: u32,
    input: Vec<u8>,
    output: Vec<u8>,
    error_msg: String,
    depends_on: Vec<Uuid>,
}

impl Task {
    /// Creates a task in its starting state: `Blocked` when it has dependencies, `Todo` otherwise.
    ///
    /// The identifier is passed in rather than minted here, because the dependencies of an initial
    /// task are positions and can only be resolved once every task of the iteration has one.
    pub(crate) fn new(id: Uuid, created_by_worker: Uuid, task_def: &TaskDefinition, depends_on: Vec<Uuid>) -> Self {
        let status = if depends_on.is_empty() {
            TaskStatus::Todo
        } else {
            TaskStatus::Blocked
        };

        Self {
            id,
            code: task_def.code().clone(),
            status,
            processing_by_worker: None,
            created_by_worker,
            // The definition passed `TaskDefinition::validate` before reaching here, so the
            // conversion cannot fail; the fallback only stands in for the lint-forbidden `unwrap`.
            timeout: Duration::from_std(task_def.timeout()).unwrap_or_else(|_| Duration::zero()),
            started_at: None,
            completed_at: None,
            deadline_at: None,
            attempt: 0,
            max_attempts: task_def.max_attempts(),
            input: task_def.input().to_vec(),
            output: Vec::new(),
            error_msg: String::new(),
            depends_on,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn restore(
        id: Uuid,
        code: TaskCode,
        status: TaskStatus,
        processing_by_worker: Option<Uuid>,
        created_by_worker: Uuid,
        timeout: Duration,
        started_at: Option<DateTime<Utc>>,
        completed_at: Option<DateTime<Utc>>,
        deadline_at: Option<DateTime<Utc>>,
        attempt: u32,
        max_attempts: u32,
        input: Vec<u8>,
        output: Vec<u8>,
        error_msg: String,
        depends_on: Vec<Uuid>,
    ) -> Self {
        Self {
            id,
            code,
            status,
            processing_by_worker,
            created_by_worker,
            timeout,
            started_at,
            completed_at,
            deadline_at,
            attempt,
            max_attempts,
            input,
            output,
            error_msg,
            depends_on,
        }
    }

    // Accessors
    pub(crate) const fn id(&self) -> &Uuid {
        &self.id
    }

    pub(crate) const fn code(&self) -> &TaskCode {
        &self.code
    }

    pub(crate) const fn status(&self) -> &TaskStatus {
        &self.status
    }

    pub(crate) const fn processing_by_worker(&self) -> Option<Uuid> {
        self.processing_by_worker
    }

    pub(crate) const fn created_by_worker(&self) -> Uuid {
        self.created_by_worker
    }

    pub(crate) const fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) const fn attempt(&self) -> u32 {
        self.attempt
    }

    pub(crate) const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    pub(crate) const fn started_at(&self) -> Option<DateTime<Utc>> {
        self.started_at
    }

    pub(crate) const fn completed_at(&self) -> Option<DateTime<Utc>> {
        self.completed_at
    }

    pub(crate) const fn deadline_at(&self) -> Option<DateTime<Utc>> {
        self.deadline_at
    }

    pub(crate) fn input(&self) -> &[u8] {
        &self.input
    }

    pub(crate) fn output(&self) -> &[u8] {
        &self.output
    }

    pub(crate) fn error_msg(&self) -> &str {
        &self.error_msg
    }

    pub(crate) fn depends_on(&self) -> &[Uuid] {
        &self.depends_on
    }

    // State checks
    pub(crate) fn is_expired(&self) -> bool {
        self.deadline_at.is_some_and(|deadline| Utc::now() > deadline)
    }

    pub(crate) const fn is_completed(&self) -> bool {
        matches!(self.status, TaskStatus::Completed)
    }

    pub(crate) const fn is_failed(&self) -> bool {
        matches!(self.status, TaskStatus::Failed)
    }

    pub(crate) const fn is_started(&self) -> bool {
        matches!(self.status, TaskStatus::Started)
    }

    /// Whether the task failed and spent its whole attempt budget, so it will
    /// never run again. Tasks blocked behind it can never be unblocked either,
    /// which is what ends the job iteration (see `Job::pick_task_to_execute`).
    pub(crate) const fn is_terminally_failed(&self) -> bool {
        self.is_failed() && self.attempt >= self.max_attempts
    }

    pub(crate) fn can_be_picked_up(&self) -> bool {
        match self.status {
            TaskStatus::Todo => true,
            TaskStatus::Failed => !self.is_terminally_failed(),
            TaskStatus::Blocked | TaskStatus::Completed => false,
            TaskStatus::Started => self.is_expired() && self.attempt < self.max_attempts,
        }
    }

    pub(crate) const fn unblock(&mut self) {
        if matches!(self.status, TaskStatus::Blocked) {
            self.status = TaskStatus::Todo;
        }
    }

    pub(crate) fn start(&mut self, worker_id: Uuid) -> Result<(), JobError> {
        if !self.can_be_picked_up() {
            if self.processing_by_worker != Some(worker_id) {
                return Err(JobError::TaskWorkerMismatch);
            }
            return Err(JobError::Other(format!(
                "cannot start task (id: {}; code: {}; status: {:?}; deadline at: {:?})",
                self.id, self.code, self.status, self.deadline_at
            )));
        }

        self.status = TaskStatus::Started;
        self.processing_by_worker = Some(worker_id);
        let now = Utc::now();
        self.started_at = Some(now);
        self.deadline_at = Some(now + self.timeout);
        // TODO(high): the attempt budget is spent by takeovers as well as by failures.
        // A task whose worker dies repeatedly is terminally failed without ever having
        // failed on its own. Split the counters, or exclude takeovers from the budget.
        self.attempt += 1;

        Ok(())
    }

    pub(crate) fn complete(&mut self, output: Vec<u8>) -> Result<(), JobError> {
        if !matches!(self.status, TaskStatus::Started) {
            return Err(JobError::Other(format!(
                "cannot complete task (id: {}; code: {}) with status {:?}",
                self.id, self.code, self.status
            )));
        }

        self.output = output;
        self.status = TaskStatus::Completed;
        self.completed_at = Some(Utc::now());

        Ok(())
    }

    pub(crate) fn fail(&mut self, error_msg: &str) -> Result<(), JobError> {
        if !matches!(self.status, TaskStatus::Started) {
            return Err(JobError::Other(format!(
                "cannot fail task (id: {}; code: {}) with status {:?}",
                self.id, self.code, self.status
            )));
        }

        self.error_msg = error_msg.to_string();
        self.status = TaskStatus::Failed;
        self.completed_at = Some(Utc::now());

        Ok(())
    }
}

// ImmutableTask implementation for Task
impl ImmutableTask for Task {
    fn id(&self) -> &Uuid {
        &self.id
    }

    fn code(&self) -> &TaskCode {
        self.code()
    }

    fn get_input(&self) -> &[u8] {
        self.input()
    }

    fn get_output(&self) -> &[u8] {
        self.output()
    }

    fn get_error(&self) -> &str {
        self.error_msg()
    }

    fn depends_on(&self) -> &[Uuid] {
        self.depends_on()
    }

    fn is_expired(&self) -> bool {
        self.is_expired()
    }

    fn is_completed(&self) -> bool {
        self.is_completed()
    }

    fn is_failed(&self) -> bool {
        self.is_failed()
    }

    fn attempts(&self) -> u32 {
        self.attempt()
    }

    fn max_attempts(&self) -> u32 {
        self.max_attempts()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_definition_keeps_std_duration() {
        let def = TaskDefinition::new("t", std::time::Duration::from_millis(1500));
        assert_eq!(def.timeout(), std::time::Duration::from_millis(1500));
    }

    #[test]
    fn new_leaves_the_input_empty() {
        let def = TaskDefinition::new("t", std::time::Duration::from_secs(1));
        assert!(def.input().is_empty());
    }

    /// The payload is a single value, not an accumulating buffer: a second call must not append to
    /// what the first one attached.
    #[test]
    fn with_input_replaces_the_payload() {
        let def = TaskDefinition::new("t", std::time::Duration::from_secs(1))
            .with_input(vec![1, 2])
            .with_input(vec![3]);
        assert_eq!(def.input(), &[3]);
    }

    #[test]
    fn validate_rejects_zero_timeout() {
        let def = TaskDefinition::new("t", std::time::Duration::ZERO);
        let error = def.validate(TaskLimits::default()).unwrap_err();
        assert!(error.to_string().contains("must be positive"), "got: {error}");
    }

    /// The stored state carries the timeout as milliseconds in an `i64`, so a definition above that
    /// range cannot round-trip and is refused rather than silently truncated.
    #[test]
    fn validate_rejects_timeout_beyond_millisecond_range() {
        let def = TaskDefinition::new("t", std::time::Duration::MAX);
        let error = def.validate(TaskLimits::default()).unwrap_err();
        assert!(error.to_string().contains("too large"), "got: {error}");
    }

    #[test]
    fn validate_rejects_zero_max_attempts() {
        let def = TaskDefinition::new("t", std::time::Duration::from_secs(1)).with_max_attempts(0);
        let error = def.validate(TaskLimits::default()).unwrap_err();
        assert!(error.to_string().contains("max attempts"), "got: {error}");
    }

    #[test]
    fn validate_rejects_input_above_the_limit() {
        let limits = TaskLimits {
            max_input_bytes: 4,
            max_output_bytes: 10,
        };
        let def = TaskDefinition::new("t", std::time::Duration::from_secs(1)).with_input(vec![0; 5]);
        let error = def.validate(limits).unwrap_err();
        assert!(error.to_string().contains("input size"), "got: {error}");
    }

    #[test]
    fn validate_accepts_input_at_the_limit() {
        let limits = TaskLimits {
            max_input_bytes: 4,
            max_output_bytes: 10,
        };
        let def = TaskDefinition::new("t", std::time::Duration::from_secs(1))
            .with_input(vec![0; 4])
            .with_max_attempts(2);
        assert!(def.validate(limits).is_ok());
    }

    /// The task carries the deadline budget the definition declared: a definition in milliseconds
    /// must not be rounded to whole seconds on its way into the task.
    #[test]
    fn task_carries_the_definition_timeout() {
        let def = TaskDefinition::new("t", std::time::Duration::from_millis(1500));
        let task = Task::new(Uuid::from_u128(2), Uuid::from_u128(1), &def, Vec::new());
        assert_eq!(task.timeout(), Duration::milliseconds(1500));
    }

    /// Blocking used to be assigned after construction; the constructor now owns it, so the rule
    /// keeps a test of its own.
    #[test]
    fn new_blocks_a_task_that_declares_dependencies() {
        let def = TaskDefinition::new("t", std::time::Duration::from_secs(1));

        let blocked = Task::new(Uuid::from_u128(2), Uuid::from_u128(1), &def, vec![Uuid::from_u128(3)]);
        let free = Task::new(Uuid::from_u128(4), Uuid::from_u128(1), &def, Vec::new());

        assert_eq!(*blocked.status(), TaskStatus::Blocked);
        assert_eq!(*free.status(), TaskStatus::Todo);
    }
}
