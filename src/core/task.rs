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
    /// task over as long as the task has not outlived its maximum lifetime, so a slow executor
    /// can end up running concurrently with its replacement. A takeover spends no attempt; once
    /// the lifetime is spent, the task is failed instead of taken over - but the executor
    /// already running keeps running either way, and the result it returns afterwards is refused.
    Started,
    /// Task finished successfully.
    ///
    /// Terminal - such a task is never picked up again, and a job iteration completes only when
    /// all of its tasks are in this state.
    Completed,
    /// Task execution failed, task will be processed again.
    ///
    /// Retried until the task's attempt budget is spent or its maximum lifetime has passed; after
    /// that it is terminal - it is never picked up again and its job iteration ends as
    /// `JobStatus::Failed`.
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
/// An attempt is spent by the first start of the task and by every start following a refusal of
/// the executor, never by a takeover of an expired task, so this budget bounds how often the task's
/// own work may fail. Sized for transient failures (a
/// flaky object-store call): five attempts absorb those, while a task failing deterministically
/// stops retrying instead of blocking its dependents and the job's next iteration forever.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;

/// How many times the default maximum lifetime of a task exceeds its deadline.
///
/// Takeovers are bounded by the lifetime rather than by the attempt budget, and five deadlines
/// repeat the ceiling a budget of five starts used to give them. For a task that keeps failing the
/// lifetime is a second, independent limit: it runs from the first start, so retries that are slow
/// or start late can run out of it while attempts are still left.
pub const DEFAULT_LIFETIME_MULTIPLIER: u32 = 5;

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
    max_lifetime: std::time::Duration,
    depends_on: Vec<TaskRef>,
    max_attempts: u32,
}

impl TaskDefinition {
    /// Builds a definition with no input, no dependencies, the default attempt budget and a
    /// maximum lifetime of [`DEFAULT_LIFETIME_MULTIPLIER`] times `timeout`.
    ///
    /// `timeout` is not a cancellation budget: it sets the deadline after which another worker is
    /// allowed to take the task over. The deadline does cancel the executor's token, but an
    /// executor that does not select on it keeps running.
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
            // A product that does not fit is left for `validate` to reject, which it does through
            // the same range check the timeout goes through.
            max_lifetime: timeout
                .checked_mul(DEFAULT_LIFETIME_MULTIPLIER)
                .unwrap_or(std::time::Duration::MAX),
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

    /// Longest the task may occupy its iteration, counted from its first start.
    ///
    /// Unless [`Self::with_max_lifetime`] replaces it, this is [`DEFAULT_LIFETIME_MULTIPLIER`]
    /// times [`Self::timeout`].
    pub const fn max_lifetime(&self) -> std::time::Duration {
        self.max_lifetime
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

    /// Caps how many times the executor may refuse the task before it is terminally
    /// failed and never picked up again. Defaults to [`DEFAULT_MAX_ATTEMPTS`].
    ///
    /// An attempt is spent by the first start of the task and by every start following a refusal -
    /// an error or a panic. A takeover of an expired task spends nothing here and is bounded by
    /// [`Self::with_max_lifetime`] instead. A zero budget is rejected where the task is created,
    /// in the same place as the rest of the definition.
    #[must_use]
    pub const fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Caps how long the task may occupy its iteration, counted from its first start. Defaults to
    /// [`DEFAULT_LIFETIME_MULTIPLIER`] times the timeout.
    ///
    /// This is what bounds takeovers of an expired task, which spend no attempt: once the lifetime
    /// has passed the task is failed and its iteration ends as failed, whatever is left of the
    /// attempt budget.
    ///
    /// The bound is absolute rather than approached one deadline at a time: every deadline the task
    /// is started with is capped by it, so the executor holding the task is signalled no later than
    /// the lifetime passes, the task is failed on the first pick afterwards whatever its deadline
    /// says, and a result returned past the lifetime is refused instead of stored. A lifetime that
    /// is zero, below the timeout, or beyond the millisecond range the stored state uses is rejected
    /// where the task is created.
    #[must_use]
    pub const fn with_max_lifetime(mut self, max_lifetime: std::time::Duration) -> Self {
        self.max_lifetime = max_lifetime;
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
    /// Returns [`JobError::Other`] if the timeout or the maximum lifetime is zero or beyond the
    /// millisecond range the stored state uses, if the lifetime is below the timeout, if the
    /// attempt budget is zero, or if the input exceeds `limits`.
    pub(crate) fn validate(&self, limits: TaskLimits) -> Result<(), JobError> {
        if self.timeout.is_zero() {
            return Err(JobError::Other("task timeout must be positive".into()));
        }
        if Duration::from_std(self.timeout).is_err() {
            return Err(JobError::Other("task timeout is too large".into()));
        }
        if self.max_lifetime.is_zero() {
            return Err(JobError::Other("task max lifetime must be positive".into()));
        }
        if Duration::from_std(self.max_lifetime).is_err() {
            return Err(JobError::Other("task max lifetime is too large".into()));
        }
        if self.max_lifetime < self.timeout {
            return Err(JobError::Other(
                "task max lifetime must not be below the task timeout".into(),
            ));
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
    /// True only while a deadline is set, i.e. after the task has been started at least once. An
    /// expired task may already have been taken over by another worker, and its executor's
    /// cancellation token is cancelled - which the executor is free to ignore.
    fn is_expired(&self) -> bool;

    /// Whether the task finished successfully. Terminal - it will not be executed again.
    fn is_completed(&self) -> bool;

    /// Whether the last attempt failed.
    ///
    /// Not terminal on its own: the task is retried while [`Self::attempts`] is below
    /// [`Self::max_attempts`] and its maximum lifetime has not passed, and becomes terminal once
    /// either of the two runs out.
    fn is_failed(&self) -> bool;

    /// Number of attempts the task has spent, including the one in progress.
    ///
    /// An attempt is spent by the first start of the task and by every start after a refusal of
    /// the executor; a takeover of an expired task spends none, so this never exceeds the number
    /// of refusals by more than one. It is compared against [`Self::max_attempts`] to decide
    /// whether the task may run again.
    fn attempts(&self) -> u32;

    /// Attempt budget the task was defined with, bounding refusals of the executor alone: once
    /// [`Self::attempts`] reaches it, a failed task is terminal and its job iteration ends as
    /// failed.
    ///
    /// It does not bound takeovers. A task left `Started` past its deadline is taken over whatever
    /// this budget says, so a spent budget is no guarantee against a second run of the same work;
    /// what stops the takeovers is the task's maximum lifetime, set with
    /// [`TaskDefinition::with_max_lifetime`].
    fn max_attempts(&self) -> u32;
}

/// What a worker may do with a task right now, as the task itself sees it.
///
/// One answer rather than two predicates: whether a task may be started and whether it must be
/// failed are outcomes of a single rule, so a caller cannot act on a combination that rule never
/// produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskAvailability {
    /// The task may be started. For one already `Started` past its deadline that start is a
    /// takeover, which spends no attempt.
    Pickable,
    /// The task has outlived its maximum lifetime, so it is to be failed rather than taken over.
    ExpiredPastLifetime,
    /// Nothing to do with it: running within its deadline, blocked, completed, or terminally failed.
    Unavailable,
}

// Task - internal task representation
#[derive(Debug, Clone)]
pub(crate) struct Task {
    id: Uuid,
    code: TaskCode,
    status: TaskStatus,
    processing_by_worker: Option<Uuid>,
    created_by_worker: Uuid,
    /// Task whose execution created this one, and only while that execution is still running.
    ///
    /// The value belongs to that execution rather than to the task: [`Self::restore`] puts `None`
    /// here, because a task restored into a fresh copy of its job is claimed by no execution, and a
    /// later failure of the task that once created it leaves it alone.
    // Named for its pair `created_by_worker` rather than for the lint: the two answer the same
    // question about a task's origin, and only one of them repeating the type name would hide that.
    #[allow(clippy::struct_field_names)]
    created_by_task: Option<Uuid>,
    timeout: Duration,
    max_lifetime: Duration,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    deadline_at: Option<DateTime<Utc>>, // TODO(low): seems like we can calculate deadline from started_at, timeout, lifetime_deadline_at
    lifetime_deadline_at: Option<DateTime<Utc>>,
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
    ///
    /// `created_by_task` names the task whose execution is creating this one, and is `None` for an
    /// initial task of an iteration - see the field's own comment for how long it lives.
    pub(crate) fn new(
        id: Uuid,
        created_by_worker: Uuid,
        created_by_task: Option<Uuid>,
        task_def: &TaskDefinition,
        depends_on: Vec<Uuid>,
    ) -> Self {
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
            created_by_task,
            // The definition passed `TaskDefinition::validate` before reaching here, so the
            // conversions cannot fail; the fallbacks only stand in for the lint-forbidden `unwrap`.
            timeout: Duration::from_std(task_def.timeout()).unwrap_or_else(|_| Duration::zero()),
            max_lifetime: Duration::from_std(task_def.max_lifetime()).unwrap_or_else(|_| Duration::zero()),
            started_at: None,
            completed_at: None,
            deadline_at: None,
            lifetime_deadline_at: None,
            attempt: 0,
            max_attempts: task_def.max_attempts(),
            input: task_def.input().to_vec(),
            output: Vec::new(),
            error_msg: String::new(),
            depends_on,
        }
    }

    // TODO(med): carry the fields in a parameter struct; the list has outgrown what a positional
    // call site can be read at.
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn restore(
        id: Uuid,
        code: TaskCode,
        status: TaskStatus,
        processing_by_worker: Option<Uuid>,
        created_by_worker: Uuid,
        timeout: Duration,
        max_lifetime: Duration,
        started_at: Option<DateTime<Utc>>,
        completed_at: Option<DateTime<Utc>>,
        deadline_at: Option<DateTime<Utc>>,
        lifetime_deadline_at: Option<DateTime<Utc>>,
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
            // A restored task has no parent to be rolled back by: whatever created it is long past
            // its own execution, and the task is in storage rather than in a worker's copy alone.
            created_by_task: None,
            timeout,
            max_lifetime,
            started_at,
            completed_at,
            deadline_at,
            lifetime_deadline_at,
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

    pub(crate) const fn created_by_task(&self) -> Option<Uuid> {
        self.created_by_task
    }

    pub(crate) const fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) const fn max_lifetime(&self) -> Duration {
        self.max_lifetime
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

    pub(crate) const fn lifetime_deadline_at(&self) -> Option<DateTime<Utc>> {
        self.lifetime_deadline_at
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

    // State checks. The two deadline predicates take the moment to judge by, so the verdict built
    // from them is one moment's worth and a test can place the boundary exactly; the callers below
    // read the clock once and pass it on.
    fn is_expired_at(&self, now: DateTime<Utc>) -> bool {
        self.deadline_at.is_some_and(|deadline| now > deadline)
    }

    /// Whether the task has outlived the time it may occupy its iteration. False while the task
    /// has never been started, since the lifetime runs from the first start.
    fn is_lifetime_expired_at(&self, now: DateTime<Utc>) -> bool {
        self.lifetime_deadline_at.is_some_and(|deadline| now > deadline)
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

    pub(crate) const fn is_resolved(&self) -> bool {
        self.is_completed() || self.is_failed()
    }

    /// Whether the task failed and ran out of either limit - its attempt budget or its maximum
    /// lifetime - so it will never run again. Tasks blocked behind it can never be unblocked
    /// either, which is what ends the job iteration (see `Job::pick_task_to_execute`).
    ///
    /// The lifetime is the second reason because a task failed *for* outliving it keeps attempts
    /// to spare: without it that task would be pickable again and the iteration would never end.
    pub(crate) fn is_terminally_failed(&self) -> bool {
        self.is_failed() && (self.attempt >= self.max_attempts || self.is_lifetime_expired_at(Utc::now()))
    }

    /// What a worker may do with this task right now.
    ///
    /// The maximum lifetime is answered first and on its own: it is the absolute bound, so a task
    /// past it is [`TaskAvailability::ExpiredPastLifetime`] whatever its deadline says. A `Started`
    /// task within its lifetime is [`TaskAvailability::Pickable`] once its deadline has passed -
    /// that start is a takeover.
    ///
    /// The two limits can only disagree on state written before the deadline was capped by the
    /// lifetime, since [`Self::start`] leaves the deadline at or below it.
    pub(crate) fn check_availability(&self) -> TaskAvailability {
        match self.status {
            TaskStatus::Todo => TaskAvailability::Pickable,
            TaskStatus::Blocked | TaskStatus::Completed => TaskAvailability::Unavailable,
            TaskStatus::Failed => {
                if self.is_terminally_failed() {
                    TaskAvailability::Unavailable
                } else {
                    TaskAvailability::Pickable
                }
            }
            TaskStatus::Started => {
                // Both deadlines are judged by one reading: taken apart, a task could be found
                // takeable by the first predicate and past its lifetime by the second.
                let now = Utc::now();
                if self.is_lifetime_expired_at(now) {
                    TaskAvailability::ExpiredPastLifetime
                } else if self.is_expired_at(now) {
                    TaskAvailability::Pickable
                } else {
                    TaskAvailability::Unavailable
                }
            }
        }
    }

    pub(crate) const fn unblock(&mut self) {
        if matches!(self.status, TaskStatus::Blocked) {
            self.status = TaskStatus::Todo;
        }
    }

    pub(crate) fn can_be_picked_up(&self) -> bool {
        matches!(self.check_availability(), TaskAvailability::Pickable)
    }

    /// Hands the task to `worker_id`, placing the deadline of that ownership and, on the first
    /// start, the moment the task's lifetime expires.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::TaskWorkerMismatch`] if another worker holds the task, or
    /// [`JobError::Other`] if the task cannot be started right now or if either moment falls outside
    /// the range of dates. The task is left as it was in every error case: both moments are computed
    /// before anything is assigned.
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

        // The lifetime runs from the first start rather than from creation, so waiting in `Blocked`
        // behind a dependency does not eat into it. Both additions are checked: a definition placing
        // a moment outside the range of dates passes validation - a duration is counted in a wider
        // range than a date is - and the plain `+` panics on it.
        let now = Utc::now();
        let (Some(lifetime_deadline_at), Some(timeout_deadline_at)) = (
            self.lifetime_deadline_at.or_else(|| now.checked_add_signed(self.max_lifetime)),
            now.checked_add_signed(self.timeout),
        ) else {
            return Err(JobError::Other(format!(
                "cannot start task (id: {}; code: {}): its timeout of {} ms or its maximum lifetime of {} ms \
                 falls outside the range of dates",
                self.id,
                self.code,
                self.timeout.num_milliseconds(),
                self.max_lifetime.num_milliseconds()
            )));
        };
        // Capped by the absolute bound, which is what keeps a takeover from handing the task a
        // deadline reaching past the lifetime - and the executor an unsignalled window there.
        let deadline_at = timeout_deadline_at.min(lifetime_deadline_at);

        // A task already `Started` can only be reached here by a takeover, which spends no
        // attempt: the budget answers for refusals of the executor, not for lost workers.
        let is_takeover = self.is_started();

        self.status = TaskStatus::Started;
        self.processing_by_worker = Some(worker_id);
        self.started_at = Some(now);
        self.deadline_at = Some(deadline_at);
        self.lifetime_deadline_at = Some(lifetime_deadline_at);
        if !is_takeover {
            self.attempt += 1;
        }

        Ok(())
    }

    /// Stores `output` as the task's result.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::Other`] if the task is not running, or if it has outlived its maximum
    /// lifetime - the bound is absolute, so work finished past it is refused rather than stored,
    /// and the task is failed by whoever picks it up next.
    pub(crate) fn complete(&mut self, output: Vec<u8>) -> Result<(), JobError> {
        let now = Utc::now();
        if !matches!(self.status, TaskStatus::Started) {
            return Err(JobError::Other(format!(
                "cannot complete task (id: {}; code: {}) with status {:?}",
                self.id, self.code, self.status
            )));
        }
        if self.is_lifetime_expired_at(now) {
            return Err(JobError::Other(format!(
                "cannot complete task (id: {}; code: {}): it outlived its maximum lifetime of {} ms",
                self.id,
                self.code,
                self.max_lifetime.num_milliseconds()
            )));
        }

        self.output = output;
        self.status = TaskStatus::Completed;
        self.completed_at = Some(now);

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
        self.is_expired_at(Utc::now())
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
    fn validate_rejects_zero_max_lifetime() {
        let def =
            TaskDefinition::new("t", std::time::Duration::from_secs(1)).with_max_lifetime(std::time::Duration::ZERO);
        let error = def.validate(TaskLimits::default()).unwrap_err();
        assert!(
            error.to_string().contains("max lifetime must be positive"),
            "got: {error}"
        );
    }

    /// The lifetime rides the same millisecond `i64` in the stored state as the timeout does.
    #[test]
    fn validate_rejects_max_lifetime_beyond_millisecond_range() {
        let def =
            TaskDefinition::new("t", std::time::Duration::from_secs(1)).with_max_lifetime(std::time::Duration::MAX);
        let error = def.validate(TaskLimits::default()).unwrap_err();
        assert!(error.to_string().contains("max lifetime is too large"), "got: {error}");
    }

    /// A lifetime shorter than the deadline would kill the task before its very first deadline
    /// passed, so no takeover could ever happen.
    #[test]
    fn validate_rejects_max_lifetime_below_the_timeout() {
        let def = TaskDefinition::new("t", std::time::Duration::from_secs(10))
            .with_max_lifetime(std::time::Duration::from_secs(9));
        let error = def.validate(TaskLimits::default()).unwrap_err();
        assert!(error.to_string().contains("must not be below"), "got: {error}");
    }

    /// The boundary itself is legal: exactly one deadline's worth of lifetime means the task is
    /// failed at the moment it could first be taken over.
    #[test]
    fn validate_accepts_max_lifetime_equal_to_the_timeout() {
        let def = TaskDefinition::new("t", std::time::Duration::from_secs(10))
            .with_max_lifetime(std::time::Duration::from_secs(10));
        assert!(def.validate(TaskLimits::default()).is_ok());
    }

    /// The default is stated literally rather than derived from the multiplier, so a changed
    /// multiplier shows up here instead of in production behavior alone.
    #[test]
    fn new_sets_the_max_lifetime_to_five_timeouts() {
        let def = TaskDefinition::new("t", std::time::Duration::from_millis(300));
        assert_eq!(def.max_lifetime(), std::time::Duration::from_millis(1500));
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
        let task = Task::new(Uuid::from_u128(2), Uuid::from_u128(1), None, &def, Vec::new());
        assert_eq!(task.timeout(), Duration::milliseconds(1500));
    }

    /// Blocking used to be assigned after construction; the constructor now owns it, so the rule
    /// keeps a test of its own.
    #[test]
    fn new_blocks_a_task_that_declares_dependencies() {
        let def = TaskDefinition::new("t", std::time::Duration::from_secs(1));

        let blocked = Task::new(
            Uuid::from_u128(2),
            Uuid::from_u128(1),
            None,
            &def,
            vec![Uuid::from_u128(3)],
        );
        let free = Task::new(Uuid::from_u128(4), Uuid::from_u128(1), None, &def, Vec::new());

        assert_eq!(*blocked.status(), TaskStatus::Blocked);
        assert_eq!(*free.status(), TaskStatus::Todo);
    }

    /// A task in the state a lost worker leaves behind: started, past its deadline, with the
    /// lifetime deadline `lifetime_left` away.
    fn expired_started_task(attempt: u32, lifetime_left: Duration) -> Task {
        let now = Utc::now();
        Task::restore(
            Uuid::from_u128(2),
            TaskCode::new("t"),
            TaskStatus::Started,
            Some(Uuid::from_u128(1)),
            Uuid::from_u128(1),
            Duration::seconds(5),
            Duration::seconds(25),
            Some(now - Duration::seconds(10)),
            None,
            Some(now - Duration::seconds(1)),
            Some(now + lifetime_left),
            attempt,
            DEFAULT_MAX_ATTEMPTS,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        )
    }

    #[test]
    fn start_from_todo_spends_an_attempt_and_sets_the_lifetime_deadline() {
        let def = TaskDefinition::new("t", std::time::Duration::from_secs(2));
        let mut task = Task::new(Uuid::from_u128(2), Uuid::from_u128(1), None, &def, Vec::new());

        let before = Utc::now();
        task.start(Uuid::from_u128(3)).unwrap();

        assert_eq!(task.attempt(), 1);
        let lifetime_deadline = task.lifetime_deadline_at().expect("the first start sets the lifetime");
        assert!(
            (before + Duration::seconds(10)..=Utc::now() + Duration::seconds(10)).contains(&lifetime_deadline),
            "the lifetime deadline must be five timeouts past the start, got {lifetime_deadline}"
        );
    }

    /// The regression the attempt accounting exists for: a worker that dies leaves the task
    /// started, and whoever takes it over must not be charged for that.
    #[test]
    fn takeover_of_an_expired_task_spends_no_attempt_and_keeps_the_lifetime_deadline() {
        let mut task = expired_started_task(1, Duration::seconds(20));
        let lifetime_deadline = task.lifetime_deadline_at();

        task.start(Uuid::from_u128(4)).unwrap();

        assert_eq!(task.attempt(), 1);
        assert_eq!(task.lifetime_deadline_at(), lifetime_deadline);
    }

    /// The lifetime runs from the *first* start, so a retry after a refusal spends an attempt but
    /// leaves the deadline that bounds the task's occupancy of its iteration where it was - the same
    /// rule a takeover follows, on the path that does spend an attempt.
    #[test]
    fn start_after_a_failure_spends_an_attempt_and_keeps_the_lifetime_deadline() {
        let mut task = Task::restore(
            Uuid::from_u128(2),
            TaskCode::new("t"),
            TaskStatus::Failed,
            Some(Uuid::from_u128(1)),
            Uuid::from_u128(1),
            Duration::seconds(5),
            Duration::seconds(25),
            Some(Utc::now() - Duration::seconds(10)),
            Some(Utc::now() - Duration::seconds(9)),
            Some(Utc::now() - Duration::seconds(5)),
            Some(Utc::now() + Duration::seconds(15)),
            1,
            DEFAULT_MAX_ATTEMPTS,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );
        let lifetime_deadline = task.lifetime_deadline_at();

        task.start(Uuid::from_u128(4)).unwrap();

        assert_eq!(task.attempt(), 2);
        assert_eq!(task.lifetime_deadline_at(), lifetime_deadline);
    }

    /// The parent lives no longer than the execution that created the task, so restoring one takes
    /// no parent at all: a task read back from storage belongs to no open execution, and a later
    /// failure of whatever created it must leave it alone.
    ///
    /// What this pins down is the *absence of an input*, so it can only fail on the day `restore`
    /// grows one - the same rule under a real backend is asserted in
    /// `task_lifetime_persistence_test`, where a parent added to the stored representation would
    /// come back through the shared mapping in `storage::state`.
    #[test]
    fn a_restored_task_belongs_to_no_open_execution() {
        let task = Task::restore(
            Uuid::from_u128(2),
            TaskCode::new("t"),
            TaskStatus::Todo,
            None,
            Uuid::from_u128(1),
            Duration::seconds(5),
            Duration::seconds(25),
            None,
            None,
            None,
            None,
            0,
            DEFAULT_MAX_ATTEMPTS,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );

        assert_eq!(task.created_by_task(), None);
    }

    /// The lifetime is what bounds takeovers now that they spend no attempt, and the two sides of
    /// that boundary are two different verdicts rather than one predicate turning false.
    #[test]
    fn an_expired_task_is_failed_instead_of_taken_over_once_its_lifetime_has_passed() {
        assert_eq!(
            expired_started_task(1, Duration::seconds(20)).check_availability(),
            TaskAvailability::Pickable
        );
        assert_eq!(
            expired_started_task(1, -Duration::seconds(1)).check_availability(),
            TaskAvailability::ExpiredPastLifetime
        );
    }

    /// Which side of the lifetime deadline the instant itself falls on, which the verdict above
    /// cannot pin down: it reads the clock itself, and by the time it does, a deadline placed at
    /// "now" is already in the past. The predicate takes the moment, so all three cases are exact -
    /// and the boundary is the same one the ownership deadline uses.
    #[test]
    fn the_lifetime_deadline_belongs_to_the_task_until_the_moment_after_it() {
        let now = Utc::now();
        let task = expired_started_task(1, Duration::zero());
        let lifetime_deadline = task.lifetime_deadline_at().expect("the fixture places the lifetime deadline");

        assert!(!task.is_lifetime_expired_at(lifetime_deadline - Duration::milliseconds(1)));
        assert!(!task.is_lifetime_expired_at(lifetime_deadline));
        assert!(task.is_lifetime_expired_at(lifetime_deadline + Duration::milliseconds(1)));
        assert!(
            !task.is_lifetime_expired_at(now),
            "the fixture must place the deadline no earlier than the moment the test started"
        );
    }

    /// The bound is absolute: state written before the deadline was capped by the lifetime can hold
    /// a task whose deadline outlasts it, and such a task is failed rather than left to the executor
    /// holding it.
    #[test]
    fn a_task_past_its_lifetime_is_failed_even_while_its_deadline_holds() {
        let now = Utc::now();
        let task = Task::restore(
            Uuid::from_u128(2),
            TaskCode::new("t"),
            TaskStatus::Started,
            Some(Uuid::from_u128(1)),
            Uuid::from_u128(1),
            Duration::seconds(5),
            Duration::seconds(25),
            Some(now - Duration::seconds(30)),
            None,
            Some(now + Duration::seconds(5)),
            Some(now - Duration::seconds(1)),
            1,
            DEFAULT_MAX_ATTEMPTS,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );

        assert_eq!(task.check_availability(), TaskAvailability::ExpiredPastLifetime);
    }

    /// A takeover close to the lifetime deadline must not hand the task a deadline reaching past it:
    /// the window between the two would be one nobody signalled the executor in, and the lifetime
    /// would be exceeded by almost a whole deadline. The task's own timeout is five seconds, so a
    /// lifetime two seconds away is what the deadline has to fall back to.
    #[test]
    fn a_start_caps_the_deadline_at_the_lifetime_deadline() {
        let mut task = expired_started_task(1, Duration::seconds(2));
        let lifetime_deadline = task.lifetime_deadline_at();

        task.start(Uuid::from_u128(4)).unwrap();

        assert_eq!(task.deadline_at(), lifetime_deadline);
    }

    /// The ordinary start, where the deadline is the one the definition asked for: capping must not
    /// shorten a deadline the lifetime leaves room for.
    #[test]
    fn a_start_keeps_the_deadline_the_timeout_asks_for_while_the_lifetime_allows_it() {
        let before = Utc::now();
        let mut task = expired_started_task(1, Duration::seconds(20));

        task.start(Uuid::from_u128(4)).unwrap();

        let deadline = task.deadline_at().expect("a start places the deadline");
        assert!(
            (before + Duration::seconds(5)..=Utc::now() + Duration::seconds(5)).contains(&deadline),
            "the deadline must be one timeout past the start, got {deadline}"
        );
    }

    /// A definition wide enough to place a boundary outside the range of dates passes validation -
    /// durations are counted in a wider range than dates are - so the start has to answer with an
    /// error rather than with the panic the plain addition would raise.
    #[test]
    fn a_start_whose_lifetime_falls_outside_the_range_of_dates_is_refused() {
        let mut task = Task::restore(
            Uuid::from_u128(2),
            TaskCode::new("t"),
            TaskStatus::Todo,
            None,
            Uuid::from_u128(1),
            Duration::seconds(5),
            Duration::MAX,
            None,
            None,
            None,
            None,
            0,
            DEFAULT_MAX_ATTEMPTS,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );

        let error = task.start(Uuid::from_u128(4)).unwrap_err();

        assert!(error.to_string().contains("outside the range of dates"), "got: {error}");
        assert_eq!(*task.status(), TaskStatus::Todo, "a refused start must change nothing");
        assert_eq!(task.attempt(), 0);
        assert_eq!(task.deadline_at(), None);
    }

    /// The other half of the absolute bound: work finished past the lifetime is refused, so an
    /// executor that ignored its cancellation cannot store a result the limit already ruled out.
    ///
    /// Both sides of the boundary. Which side the instant itself belongs to is decided by the same
    /// predicate this reads, asserted exactly in
    /// `the_lifetime_deadline_belongs_to_the_task_until_the_moment_after_it`.
    #[test]
    fn a_task_cannot_be_completed_past_its_lifetime_deadline() {
        for (lifetime_left, is_accepted) in [(Duration::seconds(10), true), (-Duration::seconds(1), false)] {
            let mut task = expired_started_task(1, lifetime_left);

            let outcome = task.complete(b"done".to_vec());

            assert_eq!(
                outcome.is_ok(),
                is_accepted,
                "a lifetime deadline {lifetime_left} away: {outcome:?}"
            );
            assert_eq!(task.is_completed(), is_accepted);
        }
    }
}
