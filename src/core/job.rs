use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::core::task::TaskRefKind;
use crate::{Error, ImmutableTask, JobError, Task, TaskCode, TaskDefinition, TaskExecutor, TaskRef, TaskStatus};

/// Job identifier used to select a job definition and persisted state.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobCode(String);

impl JobCode {
    /// Wraps a raw code as-is; nothing is validated here.
    ///
    /// Emptiness and uniqueness are enforced later, by
    /// [`JobsManagerBuilder::build`](crate::JobsManagerBuilder::build).
    pub fn new(code: impl Into<String>) -> Self {
        Self(code.into())
    }

    /// Borrows the raw code.
    ///
    /// The code is used verbatim as a path segment of the job's state prefix in object storage,
    /// so it must be a valid object-key component.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for JobCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for JobCode {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for JobCode {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Job lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    /// New job created or new iteration started - tasks can be picked up for work.
    ///
    /// Entry state of every iteration. May move to `Running` or `Failed`.
    Started,
    /// Job is in progress: tasks are executing.
    ///
    /// Self-transition is legal, so a second worker picking up the same job does not fail.
    /// May move to `Completed` or `Failed`.
    Running,
    /// Job completed successfully: every task of the iteration reached `TaskStatus::Completed`.
    ///
    /// Terminal for the iteration; the only way out is back to `Started` when the next
    /// iteration becomes due.
    Completed,
    /// The iteration ended in failure.
    ///
    /// Terminal for the iteration in the same way as `Completed`, and re-enterable only via
    /// `Started`. A failed *task* moves the iteration here only once it has spent its attempt
    /// budget and nothing else is executing: until then it stays pickable and is retried within
    /// the same iteration.
    Failed,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Started => write!(f, "started"),
            Self::Running => write!(f, "running"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

impl JobStatus {
    // Checks if transition to new status is allowed and returns error if not
    fn can_transition_to(&self, new: &Self) -> Result<(), JobError> {
        let allowed = match self {
            Self::Started => matches!(new, Self::Running | Self::Failed),
            Self::Running => matches!(new, Self::Running | Self::Completed | Self::Failed),
            Self::Completed | Self::Failed => matches!(new, Self::Started),
        };

        if allowed {
            Ok(())
        } else {
            Err(JobError::InvalidStatusTransition {
                from: self.clone(),
                to: new.clone(),
            })
        }
    }

    fn transition_to(&mut self, new: Self) -> Result<(), JobError> {
        self.can_transition_to(&new)?;
        *self = new;
        Ok(())
    }
}

/// Outcome of selecting a task to execute in the current job iteration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TaskPickup {
    /// This task is ready to be started by the caller.
    Ready(Uuid),
    /// Nothing can be started right now: the remaining tasks are either
    /// in flight or blocked behind tasks that are still running.
    Waiting,
    /// The iteration cannot progress because tasks spent their attempt budget;
    /// the job has been moved to [`JobStatus::Failed`]. The caller must persist
    /// the job so the scheduler starts the next iteration, which replans from
    /// scratch.
    Exhausted,
}

/// Payload size caps applied to every task of a job.
///
/// Oversized payloads are rejected with an error, never truncated: an input above the cap fails
/// job creation or [`JobHandle::add_task`](crate::JobHandle::add_task), an output above the cap
/// fails `complete_task` and leaves the task unfinished. Since the limits are not persisted with
/// the job state but re-read from the job's description on every load, changing them also affects
/// jobs that already exist in storage.
#[derive(Debug, Clone, Copy)]
pub struct TaskLimits {
    /// Maximum size of a task input payload. Defaults to 10 `MiB`.
    pub max_input_bytes: usize,
    /// Maximum size of a task output payload. Defaults to 10 `MiB`.
    pub max_output_bytes: usize,
}

impl Default for TaskLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: 10 * 1024 * 1024,  // 10MB
            max_output_bytes: 10 * 1024 * 1024, // 10MB
        }
    }
}

/// Number of most recent iterations of a job kept in storage.
///
/// Sized so an operator can still inspect the recent history of a job while the tail of a
/// long-running job does not grow without bound. Override per job with
/// [`JobBuilder::keep_iterations`](crate::JobBuilder::keep_iterations).
pub const DEFAULT_ITERATION_RETENTION: u64 = 100;

/// Smallest accepted [`JobDefinition::with_iteration_retention`] value.
///
/// A retention window has to stay wider than the gap a worker can lag behind the current
/// iteration; see the builder's doc comment for what a too-narrow window costs.
const MIN_ITERATION_RETENTION: u64 = 5;

/// Identity of a job description, distinct from that of every other description.
///
/// A [`TaskRef`] to an initial task names a position, and a position only means something together
/// with the description it belongs to. This is what carries that "which description" part, so a
/// reference handed out by one job cannot be resolved against another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JobDefinitionId(Uuid);

impl JobDefinitionId {
    /// Mints an identity no other description carries.
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Immutable job definition with initial tasks and executors.
#[derive(Clone)]
pub struct JobDefinition {
    code: JobCode,
    initial_tasks: Vec<TaskDefinition>,
    task_executors: HashMap<TaskCode, Arc<dyn TaskExecutor>>,
    max_iterations: Option<u64>, // None = unlimited
    iteration_interval: Option<std::time::Duration>,
    task_limits: TaskLimits,
    iteration_retention: u64,
}

impl JobDefinition {
    /// Builds a validated definition with unlimited iterations and no iteration interval; use the
    /// `with_*` builders to override those.
    ///
    /// This is the only place an initial task definition is checked, so a definition that cannot
    /// produce a legal task is rejected before any worker runs. An initial task is described
    /// together with the executor that runs it; `runtime_executors` covers the codes a task created
    /// through [`JobHandle::add_task`](crate::JobHandle::add_task) may use, and a runtime task
    /// whose code is missing there fails at execution time, not here.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`].
    pub(crate) fn new(
        id: JobDefinitionId,
        code: JobCode,
        initial_tasks: Vec<(TaskDefinition, Arc<dyn TaskExecutor>)>,
        runtime_executors: Vec<(TaskCode, Arc<dyn TaskExecutor>)>,
        declared_dependencies: Vec<(TaskRef, Vec<TaskRef>)>,
        task_limits: TaskLimits,
    ) -> Result<Self, Error> {
        if initial_tasks.is_empty() {
            return Err(Error::Other(format!(
                "job '{code}' has no initial task: declare at least one with add_task()"
            )));
        }

        let mut task_executors: HashMap<TaskCode, Arc<dyn TaskExecutor>> = HashMap::new();
        let mut initial_definitions = Vec::with_capacity(initial_tasks.len());
        for (position, (definition, executor)) in initial_tasks.into_iter().enumerate() {
            definition
                .validate(task_limits)
                .map_err(|e| Error::Other(format!("job '{code}' initial task {position}: {e}")))?;
            Self::register_executor(&mut task_executors, &code, definition.code().clone(), executor)?;
            initial_definitions.push(definition);
        }

        for (task_code, executor) in runtime_executors {
            Self::register_executor(&mut task_executors, &code, task_code, executor)?;
        }

        let initial_definitions =
            Self::merge_declared_dependencies(id, &code, initial_definitions, declared_dependencies)?;
        Self::validate_initial_task_dependencies(id, &code, &initial_definitions)?;

        Ok(Self {
            code,
            initial_tasks: initial_definitions,
            task_executors,
            max_iterations: None,
            iteration_interval: None,
            task_limits,
            iteration_retention: DEFAULT_ITERATION_RETENTION,
        })
    }

    /// Code identifying this job, unique within its [`JobRegistry`](crate::JobRegistry) and used
    /// as the storage prefix of the job's persisted state.
    pub const fn code(&self) -> &JobCode {
        &self.code
    }

    /// Task definitions a job iteration starts with.
    ///
    /// These are re-instantiated as fresh tasks at the start of *every* iteration, not only the
    /// first one, so they must describe work that is safe to repeat.
    pub fn initial_tasks(&self) -> &[TaskDefinition] {
        &self.initial_tasks
    }

    /// Executors available to this job, keyed by task code.
    ///
    /// Covers both initial and dynamically added tasks; a task whose code is absent here can be
    /// created but never executed.
    pub fn task_executors(&self) -> &HashMap<TaskCode, Arc<dyn TaskExecutor>> {
        &self.task_executors
    }

    /// Iteration cap, or `None` for an endlessly repeating job.
    pub const fn max_iterations(&self) -> Option<u64> {
        self.max_iterations
    }

    /// Minimum delay between the start of consecutive iterations, or `None` to start the next
    /// iteration as soon as the previous one finishes.
    pub const fn iteration_interval(&self) -> Option<std::time::Duration> {
        self.iteration_interval
    }

    /// Payload size caps applied to the job's tasks.
    pub const fn task_limits(&self) -> TaskLimits {
        self.task_limits
    }

    /// Newest iteration number of this job that may be deleted while its current iteration is
    /// `iter_num`, or `None` when the retention window still covers the whole history.
    ///
    /// `iter_num` is the domain iteration number - 1, 2, 3, … - not a storage key. The inverted
    /// numbering that makes the current iteration findable in one `LIST` belongs to `S3Storage`
    /// and never reaches this rule.
    pub const fn calculate_retention_boundary(&self, iter_num: u64) -> Option<u64> {
        match iter_num.checked_sub(self.iteration_retention) {
            // Iteration numbers start at 1, so a zero boundary names nothing deletable.
            None | Some(0) => None,
            Some(retention_boundary) => Some(retention_boundary),
        }
    }

    /// Caps the number of iterations; once reached, the job is no longer polled.
    ///
    /// The count includes the first iteration, so `1` means the job runs exactly once. The limit
    /// is compared against the persisted iteration number, so lowering it can retire a job that
    /// is already past the new bound.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] if `max_iterations` is zero.
    pub(crate) fn with_max_iterations(mut self, max_iterations: u64) -> Result<Self, Error> {
        if max_iterations == 0 {
            return Err(Error::Other("job max iterations must be positive".into()));
        }
        self.max_iterations = Some(max_iterations);
        Ok(self)
    }

    /// Keeps only the given number of most recent iterations of the job in storage; older ones
    /// are deleted in the background. Defaults to [`DEFAULT_ITERATION_RETENTION`].
    ///
    /// Like [`TaskLimits`], the value is not persisted with the job state but re-read from the
    /// definition, so changing it in code also applies to jobs that already exist in storage.
    ///
    /// The floor is 5, not 1, because deletion races with workers rather than excluding them: a
    /// worker that read the job's metadata before an iteration finished may still write its own
    /// iteration afterwards, and a conditional create cannot tell "never existed" from "existed
    /// and was deleted". With a narrow window that write recreates an already-deleted iteration,
    /// which then carries a duplicate copy of the initial tasks.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] if `iteration_retention` is below 5.
    pub(crate) fn with_iteration_retention(mut self, iteration_retention: u64) -> Result<Self, Error> {
        if iteration_retention < MIN_ITERATION_RETENTION {
            return Err(Error::Other(format!(
                "job iteration retention must be at least {MIN_ITERATION_RETENTION}"
            )));
        }
        self.iteration_retention = iteration_retention;
        Ok(self)
    }

    /// Requires the given delay to elapse from the *start* of an iteration before the next one
    /// may begin, so a long iteration does not extend the schedule.
    ///
    /// The anchor is the persisted start time, which survives process restarts. An explicit
    /// [`JobHandle::set_next_start_at`](crate::JobHandle::set_next_start_at) overrides this interval
    /// in both directions - it can delay the next iteration past the interval or release it earlier.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] if `iteration_interval` is zero, or so large that it does not fit
    /// the millisecond resolution the scheduler uses.
    pub(crate) fn with_iteration_interval(mut self, iteration_interval: std::time::Duration) -> Result<Self, Error> {
        if iteration_interval.is_zero() {
            return Err(Error::Other("job iteration interval must be positive".into()));
        }
        if Duration::from_std(iteration_interval).is_err() {
            return Err(Error::Other("job iteration interval is too large".into()));
        }
        self.iteration_interval = Some(iteration_interval);
        Ok(self)
    }

    /// Binds `task_code` to `executor` within `job_code`, refusing a second, different executor for the
    /// same code.
    ///
    /// Registering the same executor twice is legal - a job may start several tasks sharing a code -
    /// but two different ones would make which of them runs depend on registration order.
    fn register_executor(
        executors_by_code: &mut HashMap<TaskCode, Arc<dyn TaskExecutor>>,
        job_code: &JobCode,
        task_code: TaskCode,
        executor: Arc<dyn TaskExecutor>,
    ) -> Result<(), Error> {
        if let Some(registered) = executors_by_code.get(&task_code) {
            if Arc::ptr_eq(registered, &executor) {
                return Ok(());
            }
            return Err(Error::Other(format!(
                "job '{job_code}' registers two different executors for task '{task_code}'"
            )));
        }
        executors_by_code.insert(task_code, executor);
        Ok(())
    }

    /// Adds every declaration in `declared_dependencies` to the task it names.
    ///
    /// The declarations add to what a definition already carries rather than replacing it, so a
    /// task described through both channels waits for the union of the two lists.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] if a declaration names a task this description does not have; the
    /// dependencies themselves are checked by [`Self::validate_initial_task_dependencies`], which
    /// sees them once they are merged in.
    fn merge_declared_dependencies(
        id: JobDefinitionId,
        job_code: &JobCode,
        initial_tasks: Vec<TaskDefinition>,
        declared_dependencies: Vec<(TaskRef, Vec<TaskRef>)>,
    ) -> Result<Vec<TaskDefinition>, Error> {
        let mut dependencies_by_position: Vec<Vec<TaskRef>> =
            initial_tasks.iter().map(|task| task.depends_on().to_vec()).collect();

        for (task, dependencies) in declared_dependencies {
            let position = Self::resolve_initial_position(id, job_code, task, initial_tasks.len())?;
            dependencies_by_position[position].extend(dependencies);
        }

        Ok(initial_tasks
            .into_iter()
            .zip(dependencies_by_position)
            .map(|(definition, dependencies)| {
                if dependencies == definition.depends_on() {
                    return definition;
                }
                definition.with_dependencies(dependencies)
            })
            .collect())
    }

    /// Checks the dependency graph a job description declares.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] if an initial task depends on a task of another description or on
    /// one created at runtime, if a positional reference names a position the description does not
    /// have, or if the references form a cycle - any of which would leave a task waiting for
    /// something that never completes.
    fn validate_initial_task_dependencies(
        id: JobDefinitionId,
        job_code: &JobCode,
        initial_tasks: &[TaskDefinition],
    ) -> Result<(), Error> {
        let mut positions_by_task = Vec::with_capacity(initial_tasks.len());
        for task in initial_tasks {
            let mut positions = Vec::with_capacity(task.depends_on().len());
            for dependency in task.depends_on() {
                positions.push(Self::resolve_initial_position(
                    id,
                    job_code,
                    *dependency,
                    initial_tasks.len(),
                )?);
            }
            positions_by_task.push(positions);
        }

        if let Some(position) = Self::find_dependency_cycle(&positions_by_task) {
            return Err(Error::Other(format!(
                "job '{job_code}' has a dependency cycle through initial task {position}"
            )));
        }

        Ok(())
    }

    /// Position `task` names among the `initial_task_count` initial tasks of this description.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] if the reference was handed out by another job description or by
    /// [`JobHandle::add_task`](crate::JobHandle::add_task) - neither of which names a task of this
    /// description - or if it names a position the description does not have.
    fn resolve_initial_position(
        id: JobDefinitionId,
        job_code: &JobCode,
        task: TaskRef,
        initial_task_count: usize,
    ) -> Result<usize, Error> {
        match task.kind() {
            TaskRefKind::Initial {
                job_definition,
                position,
            } => {
                if job_definition != id {
                    return Err(Error::Other(format!(
                        "job '{job_code}' declares a dependency on a task outside its own job"
                    )));
                }
                if position >= initial_task_count {
                    return Err(Error::Other(format!(
                        "job '{job_code}' declares a dependency on initial task position \
                         {position}, which it does not have"
                    )));
                }
                Ok(position)
            }
            // Reported apart from a reference to another description, and in the same words a
            // runtime task's own rejection uses: a task created at runtime belongs to no
            // description at all, so pointing at the job layout would send the reader looking for
            // an error that is not there.
            TaskRefKind::Created(task_id) => Err(Error::Other(format!(
                "job '{job_code}' declares an initial task depending on task '{task_id}' created at runtime"
            ))),
        }
    }

    /// Position of a task that lies on a dependency cycle, or `None` when the graph is acyclic.
    ///
    /// Depth-first search with three states: a position reached while it is still on the stack closes a
    /// cycle. Called only after every reference is known to be in range.
    fn find_dependency_cycle(positions_by_task: &[Vec<usize>]) -> Option<usize> {
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum Visit {
            Unseen,
            OnStack,
            Done,
        }

        // TODO(med): optimize
        let mut states = vec![Visit::Unseen; positions_by_task.len()];
        // Explicit stack rather than recursion: a job may declare arbitrarily many initial tasks, and
        // the depth of the graph is not bounded by anything the description's author controls.
        for start in 0..positions_by_task.len() {
            if states[start] != Visit::Unseen {
                continue;
            }
            let mut stack = vec![(start, 0usize)];
            states[start] = Visit::OnStack;
            while let Some((position, next_edge)) = stack.pop() {
                match positions_by_task[position].get(next_edge) {
                    Some(&dependency) => {
                        stack.push((position, next_edge + 1));
                        match states[dependency] {
                            Visit::OnStack => return Some(dependency),
                            Visit::Unseen => {
                                states[dependency] = Visit::OnStack;
                                stack.push((dependency, 0));
                            }
                            Visit::Done => {}
                        }
                    }
                    None => states[position] = Visit::Done,
                }
            }
        }

        None
    }
}

#[derive(Clone)]
pub(crate) struct Job {
    // TODO(low): extract settings fields to new settings structure
    // TODO(low): make UUID as microtype (different for job, task)
    id: Uuid,
    code: JobCode,
    iter_num: u64, // for every new job start, the value increases
    status: JobStatus,
    tasks_by_id: HashMap<Uuid, Arc<Task>>, // Arc makes cloning cheap - only pointer is cloned
    updated_by_worker_id: Uuid,
    started_at: DateTime<Utc>,
    running_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    next_start_at: Option<DateTime<Utc>>,
    metadata: HashMap<String, serde_json::Value>,
    version: String,
    max_iterations: Option<u64>,                     // None = unlimited
    iteration_interval: Option<std::time::Duration>, // None = no minimum delay between iterations
    task_limits: TaskLimits,
}

impl Job {
    /// Creates the first iteration of a job from its description.
    ///
    /// Takes the description rather than its parts: a [`JobDefinition`] exists only after
    /// [`JobDefinition::new`] validated it, so the definitions reaching here are not re-validated
    /// against the job's limits. The reference checks below are the domain constructor's own guard
    /// against a description assembled past that validation, and are unreachable in normal use. A
    /// runtime task has no such guarantee and is validated in [`Self::add_task`].
    pub(crate) fn new(
        job_def: &JobDefinition,
        metadata: HashMap<String, serde_json::Value>,
        worker_id: Uuid,
    ) -> Result<Self, JobError> {
        let task_defs = job_def.initial_tasks();
        // Every task gets its identifier first: a positional reference cannot be resolved before
        // the task it points at has one.
        let ids_by_position: Vec<Uuid> = task_defs.iter().map(|_| Uuid::new_v4()).collect();

        let mut tasks_by_id = HashMap::with_capacity(task_defs.len());
        for (position, task_def) in task_defs.iter().enumerate() {
            let mut resolved = Vec::with_capacity(task_def.depends_on().len());
            for dependency in task_def.depends_on() {
                match dependency.kind() {
                    // The range is guaranteed by `JobDefinition::new`; the fallback only stands in
                    // for the lint-forbidden `unwrap`.
                    TaskRefKind::Initial { position, .. } => {
                        let dependency_id = ids_by_position.get(position).ok_or_else(|| {
                            JobError::Other(format!("dependency reference {position} is out of range"))
                        })?;
                        resolved.push(*dependency_id);
                    }
                    TaskRefKind::Created(id) => {
                        return Err(JobError::Other(format!(
                            "initial task cannot depend on task '{id}' created at runtime"
                        )));
                    }
                }
            }

            let id = *ids_by_position
                .get(position)
                .ok_or_else(|| JobError::Other(format!("initial task {position} has no identifier")))?;
            tasks_by_id.insert(id, Arc::new(Task::new(id, worker_id, task_def, resolved)));
        }

        Ok(Self {
            id: Uuid::new_v4(),
            code: job_def.code().clone(),
            iter_num: 1,
            status: JobStatus::Started,
            tasks_by_id,
            updated_by_worker_id: worker_id,
            started_at: Utc::now(),
            running_at: None,
            completed_at: None,
            next_start_at: None,
            metadata,
            version: String::new(),
            max_iterations: job_def.max_iterations(),
            iteration_interval: job_def.iteration_interval(),
            task_limits: job_def.task_limits(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore(
        id: Uuid,
        code: JobCode,
        version: String,
        iter_num: u64,
        status: JobStatus,
        tasks: Vec<Task>,
        updated_by_worker_id: Uuid,
        started_at: DateTime<Utc>,
        running_at: Option<DateTime<Utc>>,
        completed_at: Option<DateTime<Utc>>,
        next_start_at: Option<DateTime<Utc>>,
        metadata: HashMap<String, serde_json::Value>,
        max_iterations: Option<u64>,
        iteration_interval: Option<std::time::Duration>,
        task_limits: TaskLimits,
    ) -> Self {
        let mut tasks_by_id = HashMap::new();
        for task in tasks {
            tasks_by_id.insert(*task.id(), Arc::new(task));
        }

        Self {
            id,
            code,
            iter_num,
            status,
            tasks_by_id,
            updated_by_worker_id,
            started_at,
            running_at,
            completed_at,
            next_start_at,
            metadata,
            version,
            max_iterations,
            iteration_interval,
            task_limits,
        }
    }

    // Prepares the job for the next iteration
    pub(crate) fn next_iteration(&mut self, job_def: &JobDefinition, worker_id: Uuid) -> Result<(), JobError> {
        if !self.is_ready_to_next_iteration() {
            return Err(JobError::Other("job is not ready to next iteration".into()));
        }

        self.status.can_transition_to(&JobStatus::Started)?;

        let old_id = self.id;
        let old_iter_num = self.iter_num;
        let old_metadata = self.metadata.clone();

        let mut new_job = Self::new(job_def, old_metadata, worker_id)?;
        new_job.id = old_id;
        // TODO(low): in the future, a mechanism for restarting the sequence is needed (currently the maximum sequence is 10^20).
        // Sequential uuid will not work, as there may be a race when creating a new job by different workers.
        new_job.iter_num = old_iter_num + 1;
        new_job.started_at = Utc::now();
        *self = new_job;

        Ok(())
    }

    pub(crate) fn add_task(&mut self, task_def: &TaskDefinition, worker_id: Uuid) -> Result<Uuid, JobError> {
        task_def.validate(self.task_limits)?;

        // Resolve the dependencies before the task exists. A positional reference cannot appear
        // here: it names a slot in the job description, and a task created at runtime is outside
        // that description.
        let mut resolved = Vec::with_capacity(task_def.depends_on().len());
        for dependency in task_def.depends_on() {
            match dependency.kind() {
                TaskRefKind::Initial { position, .. } => {
                    return Err(JobError::Other(format!(
                        "task created at runtime cannot depend on initial task position {position}"
                    )));
                }
                TaskRefKind::Created(dependency_id) => {
                    if !self.tasks_by_id.contains_key(&dependency_id) {
                        return Err(JobError::Other(format!("dependency task '{dependency_id}' not found")));
                    }
                    resolved.push(dependency_id);
                }
            }
        }

        let task_id = Uuid::new_v4();
        let task = Task::new(task_id, worker_id, task_def, resolved);
        self.tasks_by_id.insert(task_id, Arc::new(task));
        Ok(task_id)
    }

    pub(crate) fn start_task(&mut self, task_id: &Uuid, worker_id: Uuid) -> Result<(), JobError> {
        let task_arc = self.get_task_arc_mut(task_id)?;

        let task = Arc::make_mut(task_arc); // Copy on write: clone only if refcount > 1
        task.start(worker_id)?;
        self.updated_by_worker_id = worker_id;

        Ok(())
    }

    pub(crate) fn complete_task(&mut self, task_id: &Uuid, output: Vec<u8>) -> Result<(), JobError> {
        Self::validate_task_output(&output, self.task_limits)?;
        let task_arc = self.get_task_arc_mut(task_id)?;

        let task = Arc::make_mut(task_arc);
        task.complete(output)
    }

    pub(crate) fn fail_task(&mut self, task_id: &Uuid, error_msg: &str) -> Result<(), JobError> {
        let task_arc = self.get_task_arc_mut(task_id)?;

        let task = Arc::make_mut(task_arc);
        task.fail(error_msg)
    }

    pub(crate) fn pick_task_to_execute(&mut self, worker_id: &Uuid) -> Result<TaskPickup, JobError> {
        if !matches!(self.status, JobStatus::Running) {
            self.work(worker_id)?;
        }

        // TODO(low): with a large number of tasks in the job, iteration can add overhead. Solution: pending tasks can be cached.
        // Since map iteration is randomized, no additional randomization is needed.
        let mut blocked_to_unblock: Option<Uuid> = None;
        let mut expired_to_fail: Vec<Uuid> = Vec::new();
        for (task_id, task_arc) in &self.tasks_by_id {
            let status = task_arc.status();

            match status {
                TaskStatus::Completed => {}
                TaskStatus::Blocked => {
                    if !self.dependencies_satisfied(task_arc.as_ref()) {
                        continue;
                    }
                    blocked_to_unblock = Some(*task_id);
                    break;
                }
                TaskStatus::Todo | TaskStatus::Failed | TaskStatus::Started => {
                    if task_arc.can_be_picked_up() {
                        return Ok(TaskPickup::Ready(*task_id));
                    }
                    if matches!(status, TaskStatus::Started) && task_arc.is_expired() {
                        expired_to_fail.push(*task_id);
                    }
                }
            }
        }

        // Fail the expired, budget-exhausted tasks gathered above. This mutates
        // task state only; the iteration verdict below (`has_exhausted_task`) then
        // sees them as terminal and ends the iteration as Failed.
        for task_id in expired_to_fail {
            let task_arc = self.get_task_arc_mut(&task_id)?;
            let task = Arc::make_mut(task_arc);
            task.fail("task expired after exhausting its attempt budget")?;
        }

        if let Some(task_id) = blocked_to_unblock {
            let task_arc = self.get_task_arc_mut(&task_id)?;
            let task = Arc::make_mut(task_arc);
            task.unblock();
            if task.can_be_picked_up() {
                return Ok(TaskPickup::Ready(task_id));
            }
        }

        if self.all_tasks_completed() {
            return Err(JobError::Other(format!(
                "wrong running job {} state - all tasks complete",
                self.code
            )));
        }

        // Tasks that spent their attempt budget can never run again, and neither
        // can whatever is blocked behind them. Wait while another task is still in
        // flight (it may yet unblock work), otherwise end the iteration: the next
        // scheduled one replans from scratch, which is what keeps a permanently
        // failing task from blocking its dependents forever.
        if !self.has_started_task() {
            if self.has_exhausted_task() {
                self.fail(worker_id)?;
                return Ok(TaskPickup::Exhausted);
            }

            if self.is_deadlocked() {
                return Err(JobError::Other(format!(
                    "job {} deadlock: blocked tasks with unmet dependencies",
                    self.code
                )));
            }
        }

        Ok(TaskPickup::Waiting)
    }

    // Accessors
    pub(crate) const fn id(&self) -> &Uuid {
        &self.id
    }

    pub(crate) const fn code(&self) -> &JobCode {
        &self.code
    }

    pub(crate) const fn iter_num(&self) -> u64 {
        self.iter_num
    }

    pub(crate) const fn status(&self) -> &JobStatus {
        &self.status
    }

    pub(crate) fn version(&self) -> &str {
        &self.version
    }

    pub(crate) const fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }

    pub(crate) const fn completed_at(&self) -> Option<DateTime<Utc>> {
        self.completed_at
    }

    pub(crate) const fn running_at(&self) -> Option<DateTime<Utc>> {
        self.running_at
    }

    pub(crate) const fn next_start_at(&self) -> Option<DateTime<Utc>> {
        self.next_start_at
    }

    pub(crate) const fn metadata(&self) -> &HashMap<String, serde_json::Value> {
        &self.metadata
    }

    pub(crate) const fn updated_by_worker_id(&self) -> Uuid {
        self.updated_by_worker_id
    }

    // State checks
    pub(crate) const fn is_processed(&self) -> bool {
        matches!(self.status, JobStatus::Completed | JobStatus::Failed)
    }

    pub(crate) const fn is_ready_for_processing(&self) -> bool {
        matches!(self.status, JobStatus::Started | JobStatus::Running)
    }

    pub(crate) fn is_ready_to_next_iteration(&self) -> bool {
        if !matches!(self.status, JobStatus::Completed | JobStatus::Failed) {
            return false;
        }

        if self.is_iteration_limit_reached() {
            return false;
        }

        if let Some(next_start) = self.next_start_at {
            return Utc::now() > next_start;
        }

        // A failed conversion holds the next iteration back rather than releasing it: the interval
        // is validated in `JobDefinition::with_iteration_interval`, so this cannot happen, and
        // starting early would be the more damaging way to be wrong.
        self.iteration_interval.is_none_or(|interval| {
            Duration::from_std(interval).is_ok_and(|interval| Utc::now() >= self.started_at + interval)
        })
    }

    // State mutations
    pub(crate) fn update_version(&mut self, version: String) {
        self.version = version;
    }

    pub(crate) const fn set_next_start_at(&mut self, next_start_at: DateTime<Utc>) {
        self.next_start_at = Some(next_start_at);
    }

    fn validate_task_output(output: &[u8], task_limits: TaskLimits) -> Result<(), JobError> {
        if output.len() > task_limits.max_output_bytes {
            return Err(JobError::Other(format!(
                "task output size {} exceeds limit {}",
                output.len(),
                task_limits.max_output_bytes
            )));
        }
        Ok(())
    }

    pub(crate) fn work(&mut self, worker_id: &Uuid) -> Result<(), JobError> {
        self.status.transition_to(JobStatus::Running)?;
        self.updated_by_worker_id = *worker_id;
        self.running_at = Some(Utc::now());
        Ok(())
    }

    pub(crate) fn try_to_complete(&mut self, worker_id: &Uuid) -> Result<bool, JobError> {
        if !self.all_tasks_completed() {
            return Ok(false);
        }

        self.status.transition_to(JobStatus::Completed)?;
        self.updated_by_worker_id = *worker_id;
        self.completed_at = Some(Utc::now());

        Ok(true)
    }

    pub(crate) fn fail(&mut self, worker_id: &Uuid) -> Result<(), JobError> {
        self.status.transition_to(JobStatus::Failed)?;
        self.updated_by_worker_id = *worker_id;
        self.completed_at = Some(Utc::now());
        Ok(())
    }

    // Merging. Call this method when worker picked and started a task but failed to save due to conflict.
    pub(crate) fn merge_with_picked_task(
        &mut self,
        worker_job: &Self,
        worker_id: &Uuid,
        task_id: &Uuid,
    ) -> Result<(), JobError> {
        if self.id != worker_job.id {
            return Err(JobError::Other(format!(
                "merge picked task for job '{}' failed - IDs are different",
                self.code
            )));
        }

        self.check_task_stolen(task_id, worker_id)?;

        let worker_task = worker_job.get_task_arc(task_id)?;
        if !worker_task.is_started() {
            return Err(JobError::Other(format!(
                "merge picked task for job '{}' failed - worker task is not started by worker '{}'",
                self.code, worker_id
            )));
        }

        self.status.transition_to(worker_job.status.clone()).map_err(|e| {
            JobError::Other(format!(
                "merge picked task for job '{}' status failed: {}",
                self.code, e
            ))
        })?;

        self.tasks_by_id.insert(*task_id, Arc::clone(worker_task));
        self.updated_by_worker_id = *worker_id;
        if self.running_at.is_none() {
            self.running_at = worker_job.running_at;
        }

        Ok(())
    }

    // Merging. Call this method after worker handled a task.
    pub(crate) fn merge_with_processed_task(
        &mut self,
        worker_job: &Self,
        worker_id: &Uuid,
        task_id: &Uuid,
    ) -> Result<(), JobError> {
        if self.id != worker_job.id {
            return Err(JobError::Other(format!(
                "merge job '{}' failed - IDs are different",
                self.code
            )));
        }

        self.check_task_stolen(task_id, worker_id)?;

        self.status.transition_to(worker_job.status.clone()).map_err(|e| {
            // TODO(low): its may be ok when job was already saved as completed, but current worker is late with
            // failed task (current worker job status is running)
            JobError::Other(format!("merge job '{}' status failed: {}", self.code, e))
        })?;

        // Merge tasks created or processed by this worker
        // IMPORTANT. Since tasks can be created in the executor, we need to merge of all tasks that the
        // current worker has created or updated.
        for (exist_task_id, task_arc) in &worker_job.tasks_by_id {
            // A task can be created by an executor or a worker can update a task.
            // A task is processed by one worker, but created tasks must be merged too.
            if (task_arc.created_by_worker() == *worker_id && task_arc.processing_by_worker().is_none())
                || task_arc.processing_by_worker() == Some(*worker_id)
            {
                self.tasks_by_id.insert(*exist_task_id, Arc::clone(task_arc));
            }
        }

        self.updated_by_worker_id = *worker_id;
        if let Some(completed) = worker_job.completed_at {
            self.completed_at = Some(completed);
        }
        if let Some(running) = worker_job.running_at {
            self.running_at = Some(running);
        }
        if let Some(next_start_at) = worker_job.next_start_at {
            self.next_start_at = Some(next_start_at);
        }

        Ok(())
    }

    pub(crate) const fn is_iteration_limit_reached(&self) -> bool {
        // TODO(low): add a special status that we no longer run the job and remove this check at the
        // complete stage
        match self.max_iterations {
            Some(max_iterations) => self.iter_num >= max_iterations,
            None => false,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn should_poll(&self) -> bool {
        self.next_start_at.is_none_or(|next_start| Utc::now() > next_start)
    }

    pub(crate) fn tasks_as_iter(&self) -> impl Iterator<Item = &Task> {
        self.tasks_by_id.values().map(std::convert::AsRef::as_ref)
    }

    pub(crate) fn get_task(&self, task_id: &Uuid) -> Result<Arc<dyn ImmutableTask>, JobError> {
        self.get_task_arc(task_id)
            .map(|task| Arc::clone(task) as Arc<dyn ImmutableTask>)
    }

    pub(crate) fn get_tasks_by_code(&self, code: &TaskCode) -> Vec<Arc<dyn ImmutableTask>> {
        self.tasks_by_id
            .values()
            .filter(|task| task.code() == code)
            .map(|task| Arc::clone(task) as Arc<dyn ImmutableTask>)
            .collect()
    }

    pub(crate) fn tasks_as_string(&self) -> String {
        use std::fmt::Write as _;
        let mut summary = String::new();
        let mut count = 0;
        for (id, task) in &self.tasks_by_id {
            let _ = write!(
                summary,
                "id: {}; code: {}; status: {}; ",
                id,
                task.code(),
                task.status()
            );
            count += 1;
            if count > 3 {
                summary.push_str("...");
                break;
            }
        }
        summary
    }

    pub(crate) fn all_tasks_completed(&self) -> bool {
        !self.tasks_by_id.is_empty() && self.tasks_by_id.values().all(|task| task.is_completed())
    }

    fn dependencies_satisfied(&self, task: &Task) -> bool {
        task.depends_on()
            .iter()
            .all(|dep_id| self.tasks_by_id.get(dep_id).is_some_and(|t| t.is_completed()))
    }

    /// Whether any task is currently being executed by a worker.
    fn has_started_task(&self) -> bool {
        self.tasks_by_id
            .values()
            .any(|task| matches!(task.status(), TaskStatus::Started))
    }

    /// Whether any task failed and spent its whole attempt budget.
    fn has_exhausted_task(&self) -> bool {
        self.tasks_by_id.values().any(|task| task.is_terminally_failed())
    }

    fn is_deadlocked(&self) -> bool {
        self.tasks_by_id
            .values()
            .any(|task| matches!(task.status(), TaskStatus::Blocked) && !self.dependencies_satisfied(task))
    }

    fn get_task_arc(&self, task_id: &Uuid) -> Result<&Arc<Task>, JobError> {
        self.tasks_by_id.get(task_id).ok_or(JobError::TaskNotFound)
    }

    fn get_task_arc_mut(&mut self, task_id: &Uuid) -> Result<&mut Arc<Task>, JobError> {
        self.tasks_by_id.get_mut(task_id).ok_or(JobError::TaskNotFound)
    }

    fn check_task_stolen(&self, task_id: &Uuid, worker_id: &Uuid) -> Result<(), JobError> {
        // This validates the job fetched from storage.
        // If another worker already owns this task there, current worker must stop merging.
        let exist_task = self.get_task_arc(task_id)?;
        if exist_task.processing_by_worker().is_some() && exist_task.processing_by_worker() != Some(*worker_id) {
            return Err(JobError::TaskWorkerMismatch);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, LazyLock},
    };

    use chrono::{DateTime, Duration, Utc};
    use uuid::Uuid;

    use super::*;
    use crate::core::task::DEFAULT_MAX_ATTEMPTS;
    use crate::{TaskOutcome, TaskRef, task_fn};

    fn noop_executor() -> Arc<dyn TaskExecutor> {
        task_fn(|_ctx| async { Ok(TaskOutcome::empty()) })
    }

    fn task_definition(code: &str) -> TaskDefinition {
        TaskDefinition::new(TaskCode::new(code), std::time::Duration::from_secs(5))
    }

    /// Identity every description in these tests is built under, so a reference made with
    /// [`initial_task_ref`] belongs to it. One identity for the whole module, because a minted one
    /// differs on every call and a reference would then belong to no description at all.
    fn test_definition_id() -> JobDefinitionId {
        static ID: LazyLock<JobDefinitionId> = LazyLock::new(JobDefinitionId::new);
        *ID
    }

    fn initial_task_ref(position: usize) -> TaskRef {
        TaskRef::initial(test_definition_id(), position)
    }

    /// Wraps definitions into a validated description under the default limits, so a test about
    /// tasks does not have to spell out an executor for each of them. The definitions themselves
    /// stay in the test - only the executor is supplied here.
    fn job_definition(tasks: Vec<TaskDefinition>) -> JobDefinition {
        job_definition_with_limits(tasks, TaskLimits::default())
    }

    fn job_definition_with_limits(tasks: Vec<TaskDefinition>, limits: TaskLimits) -> JobDefinition {
        let executor = noop_executor();
        let initial_tasks = tasks.into_iter().map(|task| (task, Arc::clone(&executor))).collect();
        JobDefinition::new(
            test_definition_id(),
            JobCode::new("job"),
            initial_tasks,
            Vec::new(),
            Vec::new(),
            limits,
        )
        .expect("the test description must be legal")
    }

    fn make_task(id: Uuid, code: &str, status: TaskStatus, depends_on: Vec<Uuid>) -> Task {
        make_task_with_attempts(id, code, status, depends_on, 0, DEFAULT_MAX_ATTEMPTS)
    }

    /// A task restored with an explicit attempt count and budget, so tests can
    /// build a task that is one retry away from — or already past — its cap.
    fn make_task_with_attempts(
        id: Uuid,
        code: &str,
        status: TaskStatus,
        depends_on: Vec<Uuid>,
        attempt: u32,
        max_attempts: u32,
    ) -> Task {
        Task::restore(
            id,
            TaskCode::new(code),
            status,
            None,
            Uuid::new_v4(),
            Duration::seconds(5),
            None,
            None,
            None,
            attempt,
            max_attempts,
            Vec::new(),
            Vec::new(),
            String::new(),
            depends_on,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn restore_job(
        id: Uuid,
        status: JobStatus,
        tasks: Vec<Task>,
        iter_num: u64,
        max_iterations: Option<u64>,
        iteration_interval: Option<std::time::Duration>,
        updated_by_worker_id: Uuid,
        running_at: Option<DateTime<Utc>>,
        completed_at: Option<DateTime<Utc>>,
        next_start_at: Option<DateTime<Utc>>,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Job {
        Job::restore(
            id,
            JobCode::new("job"),
            String::new(),
            iter_num,
            status,
            tasks,
            updated_by_worker_id,
            Utc::now(),
            running_at,
            completed_at,
            next_start_at,
            metadata,
            max_iterations,
            iteration_interval,
            TaskLimits::default(),
        )
    }

    #[test]
    fn test_pick_task_to_execute_todo() {
        let task_id = Uuid::from_u128(1);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(101),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Ready(task_id));
    }

    #[test]
    fn test_pick_task_to_execute_blocked_with_unmet_deps() {
        let shift_id = Uuid::from_u128(2);
        let commit_id = Uuid::from_u128(3);
        let shift = make_task(shift_id, "shift", TaskStatus::Todo, Vec::new());
        let commit = make_task(commit_id, "commit", TaskStatus::Blocked, vec![shift_id]);
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(102),
            JobStatus::Started,
            vec![shift, commit],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Ready(shift_id));
        let commit_status = job.get_task_arc(&commit_id).unwrap().status().clone();
        assert_eq!(commit_status, TaskStatus::Blocked);
    }

    #[test]
    fn test_pick_task_to_execute_unblocks_when_deps_complete() {
        let shift_id = Uuid::from_u128(4);
        let commit_id = Uuid::from_u128(5);
        let shift = make_task(shift_id, "shift", TaskStatus::Completed, Vec::new());
        let commit = make_task(commit_id, "commit", TaskStatus::Blocked, vec![shift_id]);
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(103),
            JobStatus::Started,
            vec![shift, commit],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Ready(commit_id));
        let commit_status = job.get_task_arc(&commit_id).unwrap().status().clone();
        assert_eq!(commit_status, TaskStatus::Todo);
    }

    #[test]
    fn test_pick_task_to_execute_returns_none_when_no_pickable() {
        let started = make_task(Uuid::from_u128(6), "started", TaskStatus::Started, Vec::new());
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(104),
            JobStatus::Started,
            vec![started],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Waiting);
    }

    #[test]
    fn test_pick_task_to_execute_failed_task() {
        let failed_id = Uuid::from_u128(7);
        let failed = make_task(failed_id, "failed", TaskStatus::Failed, Vec::new());
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(105),
            JobStatus::Started,
            vec![failed],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Ready(failed_id));
    }

    #[test]
    fn test_pick_task_to_execute_expired_started_task() {
        let expired = Task::restore(
            Uuid::from_u128(8),
            TaskCode::new("expired"),
            TaskStatus::Started,
            None,
            Uuid::from_u128(101),
            Duration::seconds(5),
            Some(Utc::now() - Duration::seconds(10)),
            None,
            Some(Utc::now() - Duration::seconds(1)),
            0,
            DEFAULT_MAX_ATTEMPTS,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );

        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(106),
            JobStatus::Started,
            vec![expired],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Ready(Uuid::from_u128(8)));
    }

    /// Build an expired Started task (deadline in the past) with an explicit
    /// attempt count and budget, so a test can place it just below or exactly at
    /// its cap.
    fn make_expired_started_task(id: Uuid, attempt: u32, max_attempts: u32) -> Task {
        Task::restore(
            id,
            TaskCode::new("expired"),
            TaskStatus::Started,
            Some(Uuid::from_u128(101)),
            Uuid::from_u128(101),
            Duration::seconds(5),
            Some(Utc::now() - Duration::seconds(10)),
            None,
            Some(Utc::now() - Duration::seconds(1)),
            attempt,
            max_attempts,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        )
    }

    #[test]
    fn test_pick_task_to_execute_retries_expired_started_below_attempt_limit() {
        // An expired Started task with attempts left is re-picked (its worker is
        // presumed lost), exactly as before the attempt budget was added.
        let expired_id = Uuid::from_u128(140);
        let expired = make_expired_started_task(expired_id, 1, 2);
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(141),
            JobStatus::Started,
            vec![expired],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Ready(expired_id));
    }

    #[test]
    fn test_pick_task_to_execute_fails_iteration_when_expired_started_exhausts_attempts() {
        // An expired Started task that has spent its whole attempt budget is failed
        // and ends the iteration, instead of being re-picked forever (the timeout
        // counterpart of the Failed-task exhaustion path).
        let expired_id = Uuid::from_u128(142);
        let expired = make_expired_started_task(expired_id, 2, 2);
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(143),
            JobStatus::Started,
            vec![expired],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Exhausted);
        assert!(matches!(job.status(), JobStatus::Failed));
        let expired_status = job.get_task_arc(&expired_id).unwrap().status().clone();
        assert_eq!(expired_status, TaskStatus::Failed);
    }

    #[test]
    fn test_pick_task_to_execute_all_tasks_completed_error() {
        let completed = make_task(Uuid::from_u128(9), "done", TaskStatus::Completed, Vec::new());
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(107),
            JobStatus::Started,
            vec![completed],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let err = job.pick_task_to_execute(&worker_id).unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_pick_task_to_execute_deadlock_blocked_error() {
        let commit = make_task(
            Uuid::from_u128(10),
            "commit",
            TaskStatus::Blocked,
            vec![Uuid::from_u128(11)],
        );
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(108),
            JobStatus::Started,
            vec![commit],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let err = job.pick_task_to_execute(&worker_id).unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_pick_task_to_execute_retries_failed_task_below_attempt_limit() {
        let failed_id = Uuid::from_u128(14);
        let failed = make_task_with_attempts(failed_id, "failed", TaskStatus::Failed, Vec::new(), 1, 2);
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(111),
            JobStatus::Started,
            vec![failed],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Ready(failed_id));
    }

    #[test]
    fn test_pick_task_to_execute_fails_iteration_when_attempts_exhausted() {
        let failed_id = Uuid::from_u128(15);
        let failed = make_task_with_attempts(failed_id, "failed", TaskStatus::Failed, Vec::new(), 2, 2);
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(112),
            JobStatus::Started,
            vec![failed],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Exhausted);
        assert!(matches!(job.status(), JobStatus::Failed));
        assert!(job.completed_at().is_some());
    }

    #[test]
    fn test_pick_task_to_execute_fails_iteration_for_task_blocked_behind_exhausted_task() {
        // The dependent task must never run once its dependency is terminal, and
        // the iteration must end as Failed rather than as a "deadlock" error.
        let failed_id = Uuid::from_u128(16);
        let dependent_id = Uuid::from_u128(17);
        let failed = make_task_with_attempts(failed_id, "failed", TaskStatus::Failed, Vec::new(), 2, 2);
        let dependent = make_task(dependent_id, "dependent", TaskStatus::Blocked, vec![failed_id]);
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(113),
            JobStatus::Started,
            vec![failed, dependent],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Exhausted);
        assert!(matches!(job.status(), JobStatus::Failed));
        let dependent_status = job.get_task_arc(&dependent_id).unwrap().status().clone();
        assert_eq!(dependent_status, TaskStatus::Blocked);
    }

    #[test]
    fn test_pick_task_to_execute_waits_while_another_task_runs_after_exhausted_task() {
        // An in-flight task may still unblock work, so an exhausted task ends the
        // iteration only once nothing is running.
        let failed = make_task_with_attempts(Uuid::from_u128(18), "failed", TaskStatus::Failed, Vec::new(), 2, 2);
        let started = Task::restore(
            Uuid::from_u128(19),
            TaskCode::new("started"),
            TaskStatus::Started,
            Some(Uuid::from_u128(114)),
            Uuid::from_u128(114),
            Duration::seconds(5),
            Some(Utc::now()),
            None,
            Some(Utc::now() + Duration::seconds(60)),
            1,
            DEFAULT_MAX_ATTEMPTS,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(115),
            JobStatus::Started,
            vec![failed, started],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Waiting);
        assert!(matches!(job.status(), JobStatus::Running));
    }

    #[test]
    fn test_pick_task_to_execute_completed_job_invalid_transition() {
        let completed = make_task(Uuid::from_u128(12), "done", TaskStatus::Completed, Vec::new());
        let worker_id = Uuid::from_u128(101);
        let mut job = restore_job(
            Uuid::from_u128(109),
            JobStatus::Completed,
            vec![completed],
            1,
            Some(1),
            None,
            worker_id,
            None,
            Some(Utc::now()),
            None,
            HashMap::new(),
        );

        let err = job.pick_task_to_execute(&worker_id).unwrap_err();
        assert!(matches!(err, JobError::InvalidStatusTransition { .. }));
    }

    #[test]
    fn test_pick_task_to_execute_failed_job_invalid_transition() {
        let failed = make_task(Uuid::from_u128(13), "failed", TaskStatus::Failed, Vec::new());
        let worker_id = Uuid::from_u128(102);
        let mut job = restore_job(
            Uuid::from_u128(110),
            JobStatus::Failed,
            vec![failed],
            1,
            Some(1),
            None,
            worker_id,
            None,
            Some(Utc::now()),
            None,
            HashMap::new(),
        );

        let err = job.pick_task_to_execute(&worker_id).unwrap_err();
        assert!(matches!(err, JobError::InvalidStatusTransition { .. }));
    }

    #[test]
    fn test_next_iteration_success() {
        let old_task_id = Uuid::from_u128(20);
        let old_task = make_task(old_task_id, "old", TaskStatus::Completed, Vec::new());
        let mut metadata = HashMap::new();
        metadata.insert("key".to_string(), serde_json::Value::String("value".to_string()));
        let job_id = Uuid::from_u128(21);
        let worker_id = Uuid::from_u128(22);

        let mut job = restore_job(
            job_id,
            JobStatus::Completed,
            vec![old_task],
            3,
            Some(5),
            None,
            Uuid::from_u128(200),
            None,
            Some(Utc::now()),
            None,
            metadata.clone(),
        );

        // The settings of the new iteration come from the description, which is what a worker
        // re-reads on every load - not from the iteration being replaced.
        let job_def = job_definition(vec![task_definition("new")]).with_max_iterations(5).unwrap();
        let before = Utc::now();
        job.next_iteration(&job_def, worker_id).unwrap();

        assert_eq!(job.id, job_id);
        assert_eq!(job.iter_num, 4);
        assert!(matches!(job.status, JobStatus::Started));
        assert_eq!(job.updated_by_worker_id, worker_id);
        assert_eq!(job.max_iterations, Some(5));
        assert_eq!(job.metadata, metadata);
        assert!(job.started_at >= before);
        assert_eq!(job.tasks_by_id.len(), 1);
        assert!(!job.tasks_by_id.contains_key(&old_task_id));
    }

    #[test]
    fn test_next_iteration_not_ready_status() {
        let task = make_task(Uuid::from_u128(30), "todo", TaskStatus::Todo, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(31),
            JobStatus::Running,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(300),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );

        let err = job
            .next_iteration(&job_definition(vec![task_definition("t")]), Uuid::from_u128(301))
            .unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_next_iteration_limit_reached() {
        let task = make_task(Uuid::from_u128(32), "done", TaskStatus::Completed, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(33),
            JobStatus::Completed,
            vec![task],
            2,
            Some(2),
            None,
            Uuid::from_u128(302),
            None,
            Some(Utc::now()),
            None,
            HashMap::new(),
        );

        let err = job
            .next_iteration(&job_definition(vec![task_definition("t")]), Uuid::from_u128(303))
            .unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_next_iteration_next_start_in_future() {
        let task = make_task(Uuid::from_u128(34), "done", TaskStatus::Completed, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(35),
            JobStatus::Completed,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(304),
            None,
            Some(Utc::now()),
            Some(Utc::now() + Duration::seconds(60)),
            HashMap::new(),
        );

        let err = job
            .next_iteration(&job_definition(vec![task_definition("t")]), Uuid::from_u128(305))
            .unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_next_iteration_interval_not_elapsed() {
        let task = make_task(Uuid::from_u128(36), "done", TaskStatus::Completed, Vec::new());
        let job = Job::restore(
            Uuid::from_u128(37),
            JobCode::new("job"),
            String::new(),
            1,
            JobStatus::Completed,
            vec![task],
            Uuid::from_u128(306),
            Utc::now(),
            None,
            Some(Utc::now()),
            None,
            HashMap::new(),
            None,
            Some(std::time::Duration::from_mins(1)),
            TaskLimits::default(),
        );

        assert!(!job.is_ready_to_next_iteration());
    }

    #[test]
    fn test_is_ready_to_next_iteration_anchors_to_restored_started_at() {
        // Simulates a process restart: the job is reloaded from storage carrying the
        // started_at of the iteration that already completed. The next-iteration gate
        // must anchor on that persisted started_at, not on the reload moment, so an
        // interval that already elapsed before the restart becomes eligible immediately
        // instead of waiting another full interval from restart time.
        let task = make_task(Uuid::from_u128(370), "done", TaskStatus::Completed, Vec::new());
        let started_at = Utc::now() - Duration::seconds(120);
        let job = Job::restore(
            Uuid::from_u128(371),
            JobCode::new("job"),
            String::new(),
            1,
            JobStatus::Completed,
            vec![task],
            Uuid::from_u128(372),
            started_at,
            None,
            Some(started_at + Duration::seconds(2)),
            None,
            HashMap::new(),
            None,
            Some(std::time::Duration::from_mins(1)),
            TaskLimits::default(),
        );

        assert_eq!(job.started_at(), started_at);
        assert!(job.is_ready_to_next_iteration());
    }

    #[test]
    fn test_is_ready_to_next_iteration_waits_from_restored_started_at() {
        // Counterpart of the restart anchor test: when the persisted started_at is recent,
        // the gate stays closed for the remainder of the interval regardless of reload time.
        let task = make_task(Uuid::from_u128(373), "done", TaskStatus::Completed, Vec::new());
        let started_at = Utc::now() - Duration::seconds(5);
        let job = Job::restore(
            Uuid::from_u128(374),
            JobCode::new("job"),
            String::new(),
            1,
            JobStatus::Completed,
            vec![task],
            Uuid::from_u128(375),
            started_at,
            None,
            Some(started_at + Duration::seconds(2)),
            None,
            HashMap::new(),
            None,
            Some(std::time::Duration::from_mins(1)),
            TaskLimits::default(),
        );

        assert!(!job.is_ready_to_next_iteration());
    }

    #[test]
    fn test_set_next_start_at_blocks_next_iteration_when_future() {
        let task = make_task(Uuid::from_u128(360), "done", TaskStatus::Completed, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(361),
            JobStatus::Completed,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(362),
            None,
            Some(Utc::now()),
            None,
            HashMap::new(),
        );

        job.set_next_start_at(Utc::now() + Duration::seconds(60));
        assert!(!job.is_ready_to_next_iteration());
    }

    #[test]
    fn test_set_next_start_at_overrides_iteration_interval_floor() {
        let task = make_task(Uuid::from_u128(366), "done", TaskStatus::Completed, Vec::new());
        let mut job = Job::restore(
            Uuid::from_u128(367),
            JobCode::new("job"),
            String::new(),
            1,
            JobStatus::Completed,
            vec![task],
            Uuid::from_u128(368),
            Utc::now(),
            None,
            Some(Utc::now()),
            None,
            HashMap::new(),
            None,
            Some(std::time::Duration::from_mins(1)),
            TaskLimits::default(),
        );

        assert!(!job.is_ready_to_next_iteration());
        job.set_next_start_at(Utc::now() - Duration::seconds(1));
        assert!(job.is_ready_to_next_iteration());
    }

    #[test]
    fn test_next_iteration_resets_timestamps_and_next_start() {
        let old_task_id = Uuid::from_u128(36);
        let old_task = make_task(old_task_id, "old", TaskStatus::Completed, Vec::new());
        let job_id = Uuid::from_u128(37);
        let worker_id = Uuid::from_u128(38);

        let mut job = restore_job(
            job_id,
            JobStatus::Completed,
            vec![old_task],
            1,
            None,
            None,
            Uuid::from_u128(300),
            Some(Utc::now() - Duration::seconds(5)),
            Some(Utc::now() - Duration::seconds(1)),
            None,
            HashMap::new(),
        );
        job.set_next_start_at(Utc::now() - Duration::seconds(1));

        let new_task = TaskDefinition::new(TaskCode::new("new"), std::time::Duration::from_secs(5));
        job.next_iteration(&job_definition(vec![new_task]), worker_id).unwrap();

        assert_eq!(job.id, job_id);
        assert_eq!(job.updated_by_worker_id, worker_id);
        assert!(job.running_at.is_none());
        assert!(job.completed_at.is_none());
        assert!(job.next_start_at.is_none());
    }

    #[test]
    fn test_next_iteration_from_failed_status() {
        let old_task_id = Uuid::from_u128(40);
        let old_task = make_task(old_task_id, "old", TaskStatus::Failed, Vec::new());
        let job_id = Uuid::from_u128(41);
        let worker_id = Uuid::from_u128(42);

        let mut job = restore_job(
            job_id,
            JobStatus::Failed,
            vec![old_task],
            2,
            None,
            None,
            Uuid::from_u128(310),
            Some(Utc::now() - Duration::seconds(5)),
            Some(Utc::now() - Duration::seconds(1)),
            None,
            HashMap::new(),
        );

        let new_task = TaskDefinition::new(TaskCode::new("new"), std::time::Duration::from_secs(5));
        job.next_iteration(&job_definition(vec![new_task]), worker_id).unwrap();

        assert_eq!(job.id, job_id);
        assert_eq!(job.updated_by_worker_id, worker_id);
        assert!(matches!(job.status, JobStatus::Started));
    }

    #[test]
    fn test_next_iteration_error_keeps_state_unchanged() {
        let old_task_id = Uuid::from_u128(43);
        let old_task = make_task(old_task_id, "old", TaskStatus::Completed, Vec::new());
        // Holds the next iteration back, which is what makes `next_iteration` fail here.
        let next_start_at = Utc::now() + Duration::seconds(60);
        let metadata_key = "k".to_string();
        let metadata_value = serde_json::Value::String("v".to_string());
        let mut metadata = HashMap::new();
        metadata.insert(metadata_key.clone(), metadata_value.clone());

        let mut job = Job::restore(
            Uuid::from_u128(44),
            JobCode::new("job"),
            String::new(),
            7,
            JobStatus::Completed,
            vec![old_task],
            Uuid::from_u128(311),
            Utc::now() - Duration::seconds(5),
            Some(Utc::now() - Duration::seconds(4)),
            Some(Utc::now() - Duration::seconds(3)),
            Some(next_start_at),
            metadata.clone(),
            Some(9),
            Some(std::time::Duration::from_secs(2)),
            TaskLimits {
                max_input_bytes: 4,
                max_output_bytes: 8,
            },
        );

        let old_started_at = job.started_at();
        let old_running_at = job.running_at();
        let old_completed_at = job.completed_at();
        let old_next_start_at = job.next_start_at();
        let old_updated_by_worker_id = job.updated_by_worker_id();
        let old_task_ids: Vec<Uuid> = job.tasks_as_iter().map(|task| *task.id()).collect();

        let err = job
            .next_iteration(&job_definition(vec![task_definition("new")]), Uuid::from_u128(312))
            .unwrap_err();
        assert!(matches!(err, JobError::Other(_)));

        assert!(matches!(job.status(), JobStatus::Completed));
        assert_eq!(job.iter_num(), 7);
        assert_eq!(job.started_at(), old_started_at);
        assert_eq!(job.running_at(), old_running_at);
        assert_eq!(job.completed_at(), old_completed_at);
        assert_eq!(job.next_start_at(), old_next_start_at);
        assert_eq!(job.updated_by_worker_id(), old_updated_by_worker_id);
        assert_eq!(job.metadata().get(&metadata_key), Some(&metadata_value));
        assert_eq!(
            job.tasks_as_iter().map(|task| *task.id()).collect::<Vec<_>>(),
            old_task_ids
        );
    }

    /// An initial task names its dependency by position, because identifiers are minted per
    /// iteration; planning the iteration is what turns that position into an id.
    #[test]
    fn test_job_new_resolves_dependency_refs_into_task_ids() {
        let first = TaskDefinition::new(TaskCode::new("first"), std::time::Duration::from_secs(5));
        let second = TaskDefinition::new(TaskCode::new("second"), std::time::Duration::from_secs(5))
            .with_dependencies(vec![initial_task_ref(0)]);

        let job = Job::new(&job_definition(vec![first, second]), HashMap::new(), Uuid::from_u128(1)).unwrap();

        let first_id = *job.get_tasks_by_code(&TaskCode::new("first")).first().unwrap().id();
        let second_id = *job.get_tasks_by_code(&TaskCode::new("second")).first().unwrap().id();

        assert_eq!(job.get_task(&second_id).unwrap().depends_on(), &[first_id]);
        // The status is not on `ImmutableTask`; this test lives inside the module, so it reads the
        // stored task directly rather than widening the public surface for an assertion.
        assert_eq!(*job.tasks_by_id[&second_id].status(), TaskStatus::Blocked);
        assert_eq!(*job.tasks_by_id[&first_id].status(), TaskStatus::Todo);
    }

    /// Every iteration mints fresh identifiers, so the same definition must resolve to the ids of
    /// the tasks the *new* iteration created, never to the previous ones.
    #[test]
    fn test_next_iteration_resolves_dependency_refs_against_its_own_tasks() {
        let first = TaskDefinition::new(TaskCode::new("first"), std::time::Duration::from_secs(5));
        let second = TaskDefinition::new(TaskCode::new("second"), std::time::Duration::from_secs(5))
            .with_dependencies(vec![initial_task_ref(0)]);
        let job_def = job_definition(vec![first, second]);

        let mut job = Job::new(&job_def, HashMap::new(), Uuid::from_u128(1)).unwrap();
        let first_iteration_ids: Vec<Uuid> = job.tasks_as_iter().map(|task| *task.id()).collect();
        job.status = JobStatus::Completed;

        job.next_iteration(&job_def, Uuid::from_u128(2)).unwrap();

        let first_id = *job.get_tasks_by_code(&TaskCode::new("first")).first().unwrap().id();
        let second_id = *job.get_tasks_by_code(&TaskCode::new("second")).first().unwrap().id();
        assert_eq!(job.get_task(&second_id).unwrap().depends_on(), &[first_id]);
        assert!(
            first_iteration_ids.iter().all(|id| *id != first_id),
            "the second iteration must resolve to its own tasks"
        );
    }

    #[test]
    fn new_rejects_a_dependency_on_a_position_that_does_not_exist() {
        let only = TaskDefinition::new(TaskCode::new("only"), std::time::Duration::from_secs(5))
            .with_dependencies(vec![initial_task_ref(7)]);

        let error = JobDefinition::new(
            test_definition_id(),
            JobCode::new("job"),
            vec![(only, noop_executor())],
            Vec::new(),
            Vec::new(),
            TaskLimits::default(),
        )
        .err()
        .expect("an unresolvable reference must be refused");

        assert!(error.to_string().contains("which it does not have"), "got: {error}");
    }

    /// A runtime task addresses its dependencies by id; a position names a slot of the job
    /// definition, which such a task is not part of.
    #[test]
    fn test_add_task_rejects_a_positional_dependency() {
        let root = make_task(Uuid::from_u128(41), "root", TaskStatus::Todo, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(403),
            JobStatus::Started,
            vec![root],
            1,
            Some(1),
            None,
            Uuid::from_u128(402),
            None,
            None,
            None,
            HashMap::new(),
        );

        let task_def = TaskDefinition::new(TaskCode::new("child"), std::time::Duration::from_secs(5))
            .with_dependencies(vec![initial_task_ref(0)]);
        let error = job.add_task(&task_def, Uuid::from_u128(403)).unwrap_err();

        assert!(error.to_string().contains("initial task position"), "got: {error}");
    }

    #[test]
    fn test_add_task_with_dependencies_ok() {
        let dep_id = Uuid::from_u128(40);
        let dep_task = make_task(dep_id, "dep", TaskStatus::Completed, Vec::new());
        let worker_id = Uuid::from_u128(400);
        let mut job = restore_job(
            Uuid::from_u128(401),
            JobStatus::Started,
            vec![dep_task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let task_def = TaskDefinition::new(TaskCode::new("child"), std::time::Duration::from_secs(5))
            .with_input(vec![1, 2, 3])
            .with_dependencies(vec![TaskRef::created(dep_id)]);
        let task_id = job.add_task(&task_def, Uuid::from_u128(401)).unwrap();

        let task = job.get_task_arc(&task_id).unwrap();
        assert!(matches!(task.status(), TaskStatus::Blocked));
        assert_eq!(task.depends_on(), vec![dep_id]);
    }

    #[test]
    fn test_add_task_missing_dependency() {
        let task = make_task(Uuid::from_u128(41), "root", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(402);
        let mut job = restore_job(
            Uuid::from_u128(403),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let task_def = TaskDefinition::new(TaskCode::new("child"), std::time::Duration::from_secs(5))
            .with_dependencies(vec![TaskRef::created(Uuid::from_u128(999))]);
        let err = job.add_task(&task_def, Uuid::from_u128(403)).unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_start_task_ok() {
        let task_id = Uuid::from_u128(50);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(500);
        let mut job = restore_job(
            Uuid::from_u128(501),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let worker_id = Uuid::from_u128(501);
        job.start_task(&task_id, worker_id).unwrap();
        let task = job.get_task_arc(&task_id).unwrap();
        assert!(matches!(task.status(), TaskStatus::Started));
        assert_eq!(task.processing_by_worker(), Some(worker_id));
        assert_eq!(job.updated_by_worker_id, worker_id);
        assert_eq!(task.attempt(), 1);
    }

    #[test]
    fn test_start_task_not_found() {
        let task = make_task(Uuid::from_u128(51), "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(502);
        let mut job = restore_job(
            Uuid::from_u128(503),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let err = job.start_task(&Uuid::from_u128(999), Uuid::from_u128(503)).unwrap_err();
        assert!(matches!(err, JobError::TaskNotFound));
    }

    #[test]
    fn test_start_task_worker_mismatch() {
        let task_id = Uuid::from_u128(52);
        let task = Task::restore(
            task_id,
            TaskCode::new("started"),
            TaskStatus::Started,
            Some(Uuid::from_u128(600)),
            Uuid::from_u128(601),
            Duration::seconds(5),
            Some(Utc::now()),
            None,
            Some(Utc::now() + Duration::seconds(60)),
            1,
            DEFAULT_MAX_ATTEMPTS,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );
        let worker_id = Uuid::from_u128(504);
        let mut job = restore_job(
            Uuid::from_u128(505),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let err = job.start_task(&task_id, Uuid::from_u128(602)).unwrap_err();
        assert!(matches!(err, JobError::TaskWorkerMismatch));
    }

    #[test]
    fn test_complete_task_ok() {
        let task_id = Uuid::from_u128(60);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(600);
        let mut job = restore_job(
            Uuid::from_u128(601),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        job.start_task(&task_id, Uuid::from_u128(601)).unwrap();
        job.complete_task(&task_id, vec![1, 2]).unwrap();
        let task = job.get_task_arc(&task_id).unwrap();
        assert!(matches!(task.status(), TaskStatus::Completed));
        assert_eq!(task.output(), vec![1, 2]);
        assert!(task.completed_at().is_some());
    }

    #[test]
    fn test_complete_task_wrong_status() {
        let task_id = Uuid::from_u128(61);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(602);
        let mut job = restore_job(
            Uuid::from_u128(603),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let err = job.complete_task(&task_id, vec![1]).unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_fail_task_ok() {
        let task_id = Uuid::from_u128(70);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(700);
        let mut job = restore_job(
            Uuid::from_u128(701),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        job.start_task(&task_id, Uuid::from_u128(701)).unwrap();
        job.fail_task(&task_id, "boom").unwrap();
        let task = job.get_task_arc(&task_id).unwrap();
        assert!(matches!(task.status(), TaskStatus::Failed));
        assert_eq!(task.error_msg(), "boom");
        assert!(task.completed_at().is_some());
    }

    #[test]
    fn test_fail_task_wrong_status() {
        let task_id = Uuid::from_u128(71);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(702);
        let mut job = restore_job(
            Uuid::from_u128(703),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let err = job.fail_task(&task_id, "boom").unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_work_ok() {
        let task = make_task(Uuid::from_u128(80), "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(800);
        let mut job = restore_job(
            Uuid::from_u128(801),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let worker_id = Uuid::from_u128(801);
        job.work(&worker_id).unwrap();
        assert!(matches!(job.status, JobStatus::Running));
        assert_eq!(job.updated_by_worker_id, worker_id);
        assert!(job.running_at.is_some());
    }

    #[test]
    fn test_work_invalid_transition() {
        let task = make_task(Uuid::from_u128(81), "done", TaskStatus::Completed, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(82),
            JobStatus::Completed,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(802),
            None,
            Some(Utc::now()),
            None,
            HashMap::new(),
        );

        let err = job.work(&Uuid::from_u128(803)).unwrap_err();
        assert!(matches!(err, JobError::InvalidStatusTransition { .. }));
    }

    #[test]
    fn test_try_to_complete_ok() {
        let task = make_task(Uuid::from_u128(90), "done", TaskStatus::Completed, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(91),
            JobStatus::Running,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(900),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );

        let worker_id = Uuid::from_u128(901);
        let completed = job.try_to_complete(&worker_id).unwrap();
        assert!(completed);
        assert!(matches!(job.status, JobStatus::Completed));
        assert_eq!(job.updated_by_worker_id, worker_id);
        assert!(job.completed_at.is_some());
    }

    #[test]
    fn test_try_to_complete_not_ready() {
        let task = make_task(Uuid::from_u128(92), "todo", TaskStatus::Todo, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(93),
            JobStatus::Running,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(902),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );

        let completed = job.try_to_complete(&Uuid::from_u128(903)).unwrap();
        assert!(!completed);
        assert!(matches!(job.status, JobStatus::Running));
        assert!(job.completed_at.is_none());
    }

    #[test]
    fn test_try_to_complete_invalid_transition() {
        let task = make_task(Uuid::from_u128(94), "done", TaskStatus::Completed, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(95),
            JobStatus::Started,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(904),
            None,
            None,
            None,
            HashMap::new(),
        );

        let err = job.try_to_complete(&Uuid::from_u128(905)).unwrap_err();
        assert!(matches!(err, JobError::InvalidStatusTransition { .. }));
    }

    #[test]
    fn test_fail_ok() {
        let task = make_task(Uuid::from_u128(100), "todo", TaskStatus::Todo, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(101),
            JobStatus::Running,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(1000),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );

        let worker_id = Uuid::from_u128(1001);
        job.fail(&worker_id).unwrap();
        assert!(matches!(job.status, JobStatus::Failed));
        assert_eq!(job.updated_by_worker_id, worker_id);
        assert!(job.completed_at.is_some());
    }

    #[test]
    fn test_fail_invalid_transition() {
        let task = make_task(Uuid::from_u128(102), "done", TaskStatus::Completed, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(103),
            JobStatus::Completed,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(1002),
            None,
            Some(Utc::now()),
            None,
            HashMap::new(),
        );

        let err = job.fail(&Uuid::from_u128(1003)).unwrap_err();
        assert!(matches!(err, JobError::InvalidStatusTransition { .. }));
    }

    #[test]
    fn test_merge_with_picked_task_ok() {
        let task_id = Uuid::from_u128(1050);
        let worker_id = Uuid::from_u128(1051);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let mut saved_job = restore_job(
            Uuid::from_u128(1052),
            JobStatus::Started,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(1053),
            None,
            None,
            None,
            HashMap::new(),
        );

        let mut worker_job = saved_job.clone();
        let picked = worker_job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Ready(task_id));
        worker_job.start_task(&task_id, worker_id).unwrap();

        saved_job.merge_with_picked_task(&worker_job, &worker_id, &task_id).unwrap();

        let merged_task = saved_job.get_task_arc(&task_id).unwrap();
        assert!(matches!(saved_job.status(), JobStatus::Running));
        assert!(saved_job.running_at().is_some());
        assert!(matches!(merged_task.status(), TaskStatus::Started));
        assert_eq!(merged_task.processing_by_worker(), Some(worker_id));
        assert_eq!(merged_task.attempt(), 1);
    }

    #[test]
    fn test_merge_with_picked_task_worker_mismatch() {
        let task_id = Uuid::from_u128(1060);
        let worker_id = Uuid::from_u128(1061);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let mut worker_job = restore_job(
            Uuid::from_u128(1062),
            JobStatus::Started,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(1063),
            None,
            None,
            None,
            HashMap::new(),
        );
        let picked = worker_job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Ready(task_id));
        worker_job.start_task(&task_id, worker_id).unwrap();

        let started_by_other = Task::restore(
            task_id,
            TaskCode::new("todo"),
            TaskStatus::Started,
            Some(Uuid::from_u128(1069)),
            Uuid::from_u128(1064),
            Duration::seconds(5),
            Some(Utc::now()),
            None,
            Some(Utc::now() + Duration::seconds(60)),
            1,
            DEFAULT_MAX_ATTEMPTS,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );
        let mut saved_job = restore_job(
            *worker_job.id(),
            JobStatus::Running,
            vec![started_by_other],
            1,
            None,
            None,
            Uuid::from_u128(1065),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );

        let err = saved_job.merge_with_picked_task(&worker_job, &worker_id, &task_id).unwrap_err();
        assert!(matches!(err, JobError::TaskWorkerMismatch));
    }

    #[test]
    fn test_merge_with_picked_task_idempotent() {
        let task_id = Uuid::from_u128(1070);
        let worker_id = Uuid::from_u128(1071);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let mut saved_job = restore_job(
            Uuid::from_u128(1072),
            JobStatus::Started,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(1073),
            None,
            None,
            None,
            HashMap::new(),
        );
        let mut worker_job = saved_job.clone();
        let picked = worker_job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Ready(task_id));
        worker_job.start_task(&task_id, worker_id).unwrap();

        saved_job.merge_with_picked_task(&worker_job, &worker_id, &task_id).unwrap();
        let first_task = saved_job.get_task_arc(&task_id).unwrap();
        let first_attempt = first_task.attempt();
        let first_deadline = first_task.deadline_at();

        saved_job.merge_with_picked_task(&worker_job, &worker_id, &task_id).unwrap();
        let second_task = saved_job.get_task_arc(&task_id).unwrap();

        assert_eq!(first_attempt, second_task.attempt());
        assert_eq!(first_deadline, second_task.deadline_at());
        assert_eq!(second_task.attempt(), 1);
    }

    #[test]
    fn test_merge_with_picked_task_different_job_id() {
        let task_id = Uuid::from_u128(1080);
        let worker_id = Uuid::from_u128(1081);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let mut saved_job = restore_job(
            Uuid::from_u128(1082),
            JobStatus::Started,
            vec![task.clone()],
            1,
            None,
            None,
            Uuid::from_u128(1083),
            None,
            None,
            None,
            HashMap::new(),
        );
        let mut worker_job = restore_job(
            Uuid::from_u128(1084),
            JobStatus::Started,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(1085),
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = worker_job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Ready(task_id));
        worker_job.start_task(&task_id, worker_id).unwrap();

        let err = saved_job.merge_with_picked_task(&worker_job, &worker_id, &task_id).unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_merge_with_picked_task_worker_task_not_started() {
        let task_id = Uuid::from_u128(1086);
        let worker_id = Uuid::from_u128(1087);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let mut saved_job = restore_job(
            Uuid::from_u128(1088),
            JobStatus::Started,
            vec![task.clone()],
            1,
            None,
            None,
            Uuid::from_u128(1089),
            None,
            None,
            None,
            HashMap::new(),
        );
        let mut worker_job = restore_job(
            *saved_job.id(),
            JobStatus::Started,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(1090),
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = worker_job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, TaskPickup::Ready(task_id));

        let err = saved_job.merge_with_picked_task(&worker_job, &worker_id, &task_id).unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_merge_with_processed_task_different_id() {
        let task = make_task(Uuid::from_u128(110), "todo", TaskStatus::Todo, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(111),
            JobStatus::Running,
            vec![task.clone()],
            1,
            None,
            None,
            Uuid::from_u128(1100),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );
        let worker_job = restore_job(
            Uuid::from_u128(112),
            JobStatus::Running,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(1101),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );

        let err = job
            .merge_with_processed_task(&worker_job, &Uuid::from_u128(1101), &Uuid::from_u128(110))
            .unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_merge_with_processed_task_worker_mismatch() {
        let task_id = Uuid::from_u128(120);
        let task = Task::restore(
            task_id,
            TaskCode::new("started"),
            TaskStatus::Started,
            Some(Uuid::from_u128(1200)),
            Uuid::from_u128(1201),
            Duration::seconds(5),
            Some(Utc::now()),
            None,
            Some(Utc::now() + Duration::seconds(60)),
            1,
            DEFAULT_MAX_ATTEMPTS,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );
        let mut job = restore_job(
            Uuid::from_u128(121),
            JobStatus::Running,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(1202),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );
        let worker_job = job.clone();

        let err = job
            .merge_with_processed_task(&worker_job, &Uuid::from_u128(1203), &task_id)
            .unwrap_err();
        assert!(matches!(err, JobError::TaskWorkerMismatch));
    }

    #[test]
    fn test_merge_with_processed_task_ok() {
        let base_task_id = Uuid::from_u128(130);
        let base_task = make_task(base_task_id, "base", TaskStatus::Started, Vec::new());

        let mut job = restore_job(
            Uuid::from_u128(131),
            JobStatus::Running,
            vec![base_task],
            1,
            None,
            None,
            Uuid::from_u128(1300),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );

        let worker_id = Uuid::from_u128(1301);
        let created_task_id = Uuid::from_u128(132);
        let created_task = Task::restore(
            created_task_id,
            TaskCode::new("created"),
            TaskStatus::Todo,
            None,
            worker_id,
            Duration::seconds(5),
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
        let processed_task_id = Uuid::from_u128(133);
        let processed_task = Task::restore(
            processed_task_id,
            TaskCode::new("processed"),
            TaskStatus::Started,
            Some(worker_id),
            Uuid::from_u128(1302),
            Duration::seconds(5),
            Some(Utc::now()),
            None,
            Some(Utc::now() + Duration::seconds(60)),
            1,
            DEFAULT_MAX_ATTEMPTS,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );
        let other_task_id = Uuid::from_u128(134);
        let other_task = Task::restore(
            other_task_id,
            TaskCode::new("other"),
            TaskStatus::Todo,
            None,
            Uuid::from_u128(1303),
            Duration::seconds(5),
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

        let worker_job = restore_job(
            job.id,
            JobStatus::Completed,
            vec![created_task, processed_task, other_task],
            1,
            None,
            None,
            worker_id,
            Some(Utc::now()),
            Some(Utc::now()),
            None,
            HashMap::new(),
        );

        job.merge_with_processed_task(&worker_job, &worker_id, &base_task_id).unwrap();

        assert!(matches!(job.status, JobStatus::Completed));
        assert!(job.completed_at.is_some());
        assert!(job.running_at.is_some());
        assert!(job.tasks_by_id.contains_key(&base_task_id));
        assert!(job.tasks_by_id.contains_key(&created_task_id));
        assert!(job.tasks_by_id.contains_key(&processed_task_id));
        assert!(!job.tasks_by_id.contains_key(&other_task_id));
    }

    #[test]
    fn test_merge_with_processed_task_copies_next_start_at_from_worker() {
        let task_id = Uuid::from_u128(1340);
        let worker_id = Uuid::from_u128(1341);
        let mut job = restore_job(
            Uuid::from_u128(1342),
            JobStatus::Running,
            vec![make_task(task_id, "base", TaskStatus::Started, Vec::new())],
            1,
            None,
            None,
            worker_id,
            Some(Utc::now()),
            None,
            Some(Utc::now() - Duration::seconds(10)),
            HashMap::new(),
        );

        let next_start_at = Utc::now() + Duration::seconds(60);
        let worker_job = restore_job(
            *job.id(),
            JobStatus::Completed,
            vec![Task::restore(
                task_id,
                TaskCode::new("base"),
                TaskStatus::Started,
                Some(worker_id),
                Uuid::from_u128(1343),
                Duration::seconds(5),
                Some(Utc::now()),
                None,
                Some(Utc::now() + Duration::seconds(60)),
                1,
                DEFAULT_MAX_ATTEMPTS,
                Vec::new(),
                Vec::new(),
                String::new(),
                Vec::new(),
            )],
            1,
            None,
            None,
            worker_id,
            Some(Utc::now()),
            Some(Utc::now()),
            Some(next_start_at),
            HashMap::new(),
        );

        job.merge_with_processed_task(&worker_job, &worker_id, &task_id).unwrap();
        assert_eq!(job.next_start_at(), Some(next_start_at));
    }

    #[test]
    fn test_merge_with_processed_task_keeps_saved_next_start_at_when_worker_missing() {
        let task_id = Uuid::from_u128(1350);
        let worker_id = Uuid::from_u128(1351);
        let saved_next_start_at = Utc::now() + Duration::seconds(30);
        let mut job = restore_job(
            Uuid::from_u128(1352),
            JobStatus::Running,
            vec![make_task(task_id, "base", TaskStatus::Started, Vec::new())],
            1,
            None,
            None,
            worker_id,
            Some(Utc::now()),
            None,
            Some(saved_next_start_at),
            HashMap::new(),
        );

        let worker_job = restore_job(
            *job.id(),
            JobStatus::Completed,
            vec![Task::restore(
                task_id,
                TaskCode::new("base"),
                TaskStatus::Started,
                Some(worker_id),
                Uuid::from_u128(1353),
                Duration::seconds(5),
                Some(Utc::now()),
                None,
                Some(Utc::now() + Duration::seconds(60)),
                1,
                DEFAULT_MAX_ATTEMPTS,
                Vec::new(),
                Vec::new(),
                String::new(),
                Vec::new(),
            )],
            1,
            None,
            None,
            worker_id,
            Some(Utc::now()),
            Some(Utc::now()),
            None,
            HashMap::new(),
        );

        job.merge_with_processed_task(&worker_job, &worker_id, &task_id).unwrap();
        assert_eq!(job.next_start_at(), Some(saved_next_start_at));
    }

    #[test]
    fn test_merge_with_processed_task_invalid_transition() {
        let task_id = Uuid::from_u128(140);
        let task = make_task(task_id, "done", TaskStatus::Completed, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(141),
            JobStatus::Completed,
            vec![task.clone()],
            1,
            None,
            None,
            Uuid::from_u128(1400),
            None,
            Some(Utc::now()),
            None,
            HashMap::new(),
        );
        let worker_job = restore_job(
            job.id,
            JobStatus::Running,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(1401),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );

        let err = job
            .merge_with_processed_task(&worker_job, &Uuid::from_u128(1401), &task_id)
            .unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_job_definition_new_success_with_defaults_and_builders() {
        let task_a = task_definition("task_a");
        let task_b = task_definition("task_b");
        let task_limits = TaskLimits {
            max_input_bytes: 1,
            max_output_bytes: 2,
        };

        let job_def = job_definition_with_limits(vec![task_a, task_b], task_limits)
            .with_max_iterations(3)
            .unwrap()
            .with_iteration_interval(std::time::Duration::from_secs(7))
            .unwrap();

        assert_eq!(job_def.max_iterations(), Some(3));
        assert_eq!(job_def.iteration_interval(), Some(std::time::Duration::from_secs(7)));
        assert_eq!(job_def.task_limits().max_input_bytes, 1);
        assert_eq!(job_def.task_limits().max_output_bytes, 2);
    }

    #[test]
    fn test_job_definition_new_defaults_without_builders() {
        let job_def = job_definition(vec![task_definition("noop")]);

        assert_eq!(job_def.max_iterations(), None);
        assert_eq!(job_def.iteration_interval(), None);
        // Retention of 100 is what makes iteration 101 the first one with anything to delete.
        assert_eq!(job_def.calculate_retention_boundary(100), None);
        assert_eq!(job_def.calculate_retention_boundary(101), Some(1));
        assert_eq!(
            job_def.task_limits().max_input_bytes,
            TaskLimits::default().max_input_bytes
        );
        assert_eq!(
            job_def.task_limits().max_output_bytes,
            TaskLimits::default().max_output_bytes
        );
    }

    #[test]
    fn test_job_definition_new_rejects_empty_initial_tasks() {
        let result = JobDefinition::new(
            test_definition_id(),
            JobCode::new("job"),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            TaskLimits::default(),
        );
        assert!(matches!(result, Err(Error::Other(_))));
    }

    /// The limits belong to the job, so a definition that cannot produce a legal task under them
    /// is refused where the description is assembled, not when a worker plans an iteration.
    #[test]
    fn new_rejects_a_definition_above_the_input_limit() {
        let too_big = task_definition("noop").with_input(vec![0; 5]);

        let error = JobDefinition::new(
            test_definition_id(),
            JobCode::new("job"),
            vec![(too_big, noop_executor())],
            Vec::new(),
            Vec::new(),
            TaskLimits {
                max_input_bytes: 4,
                max_output_bytes: 10,
            },
        )
        .err()
        .expect("an oversized input must not describe a job");

        assert!(error.to_string().contains("input size"), "got: {error}");
    }

    /// Which executor runs a task would otherwise depend on registration order.
    #[test]
    fn new_rejects_two_different_executors_for_one_task_code() {
        let error = JobDefinition::new(
            test_definition_id(),
            JobCode::new("job"),
            vec![(task_definition("noop"), noop_executor())],
            vec![(TaskCode::new("noop"), noop_executor())],
            Vec::new(),
            TaskLimits::default(),
        )
        .err()
        .expect("two executors for one code must not describe a job");

        assert!(error.to_string().contains("two different executors"), "got: {error}");
    }

    /// A job may start several tasks sharing a code, so the same executor arriving twice is not a
    /// mistake.
    #[test]
    fn new_accepts_the_same_executor_registered_twice() {
        let executor = noop_executor();

        let job_def = JobDefinition::new(
            test_definition_id(),
            JobCode::new("job"),
            vec![
                (task_definition("noop"), Arc::clone(&executor)),
                (task_definition("noop"), executor),
            ],
            Vec::new(),
            Vec::new(),
            TaskLimits::default(),
        );

        assert!(job_def.is_ok(), "the same executor twice must describe a job");
    }

    /// A reference is a position, and a position only means something within the description that
    /// handed it out: resolved against another description it would silently name whatever sits
    /// there. The reference travels on the task definition here, which is the channel that used to
    /// carry it past the check.
    #[test]
    fn new_rejects_a_task_definition_depending_on_another_descriptions_task() {
        let foreign = TaskRef::initial(JobDefinitionId::new(), 0);
        let task = task_definition("noop").with_dependencies(vec![foreign]);

        let error = JobDefinition::new(
            test_definition_id(),
            JobCode::new("job"),
            vec![(task, noop_executor())],
            Vec::new(),
            Vec::new(),
            TaskLimits::default(),
        )
        .err()
        .expect("a reference from another description must not describe a job");

        assert!(error.to_string().contains("outside its own job"), "got: {error}");
    }

    /// The same reference arriving as a declaration rather than on the definition is refused by
    /// the same rule, in the dependency position and in the target position alike.
    #[test]
    fn new_rejects_a_declaration_naming_another_descriptions_task() {
        let foreign = TaskRef::initial(JobDefinitionId::new(), 0);

        for (task, dependencies) in [
            (initial_task_ref(0), vec![foreign]),
            (foreign, vec![initial_task_ref(0)]),
        ] {
            let error = JobDefinition::new(
                test_definition_id(),
                JobCode::new("job"),
                vec![(task_definition("noop"), noop_executor())],
                Vec::new(),
                vec![(task, dependencies)],
                TaskLimits::default(),
            )
            .err()
            .expect("a declaration naming another description must not describe a job");

            assert!(error.to_string().contains("outside its own job"), "got: {error}");
        }
    }

    /// The two declaration channels add up rather than replace each other, and the merged list is
    /// what the iteration is planned from.
    #[test]
    fn new_merges_declared_dependencies_into_the_task_definition() {
        let first = task_definition("first");
        let second = task_definition("second");
        let third = task_definition("third").with_dependencies(vec![initial_task_ref(0)]);

        let job_def = JobDefinition::new(
            test_definition_id(),
            JobCode::new("job"),
            vec![
                (first, noop_executor()),
                (second, noop_executor()),
                (third, noop_executor()),
            ],
            Vec::new(),
            vec![(initial_task_ref(2), vec![initial_task_ref(1)])],
            TaskLimits::default(),
        )
        .expect("both declaration channels must describe a job");

        assert_eq!(
            job_def.initial_tasks()[2].depends_on(),
            &[initial_task_ref(0), initial_task_ref(1)]
        );
    }

    /// A cycle closed through the declaration channel is the same cycle, so it is refused even
    /// though no single task definition declares it.
    #[test]
    fn new_rejects_a_dependency_cycle_closed_by_a_declaration() {
        let first = task_definition("first").with_dependencies(vec![initial_task_ref(1)]);
        let second = task_definition("second");

        let error = JobDefinition::new(
            test_definition_id(),
            JobCode::new("job"),
            vec![(first, noop_executor()), (second, noop_executor())],
            Vec::new(),
            vec![(initial_task_ref(1), vec![initial_task_ref(0)])],
            TaskLimits::default(),
        )
        .err()
        .expect("a cycle must not describe a job");

        assert!(error.to_string().contains("dependency cycle"), "got: {error}");
    }

    /// A runtime task exists only inside an iteration, so a description cannot wait for one.
    #[test]
    fn new_rejects_an_initial_task_depending_on_a_runtime_task() {
        let task = task_definition("noop").with_dependencies(vec![TaskRef::created(Uuid::from_u128(9))]);

        let error = JobDefinition::new(
            test_definition_id(),
            JobCode::new("job"),
            vec![(task, noop_executor())],
            Vec::new(),
            Vec::new(),
            TaskLimits::default(),
        )
        .err()
        .expect("a reference to a runtime task must not describe a job");

        assert!(error.to_string().contains("created at runtime"), "got: {error}");
    }

    /// Every task of a cycle would stay blocked forever, so the description is refused instead.
    #[test]
    fn new_rejects_a_dependency_cycle() {
        let first = task_definition("first").with_dependencies(vec![initial_task_ref(1)]);
        let second = task_definition("second").with_dependencies(vec![initial_task_ref(0)]);

        let error = JobDefinition::new(
            test_definition_id(),
            JobCode::new("job"),
            vec![(first, noop_executor()), (second, noop_executor())],
            Vec::new(),
            Vec::new(),
            TaskLimits::default(),
        )
        .err()
        .expect("a cycle must not describe a job");

        assert!(error.to_string().contains("dependency cycle"), "got: {error}");
    }

    /// A diamond is not a cycle: the search must not report one just because a task is reached
    /// twice by different paths.
    #[test]
    fn new_accepts_a_diamond_shaped_graph() {
        let root = task_definition("root");
        let left = task_definition("left").with_dependencies(vec![initial_task_ref(0)]);
        let right = task_definition("right").with_dependencies(vec![initial_task_ref(0)]);
        let join = task_definition("join").with_dependencies(vec![initial_task_ref(1), initial_task_ref(2)]);

        let job_def = JobDefinition::new(
            test_definition_id(),
            JobCode::new("job"),
            vec![
                (root, noop_executor()),
                (left, noop_executor()),
                (right, noop_executor()),
                (join, noop_executor()),
            ],
            Vec::new(),
            Vec::new(),
            TaskLimits::default(),
        );

        assert!(job_def.is_ok(), "a diamond must describe a job");
    }

    #[test]
    fn test_job_definition_with_max_iterations_rejects_zero() {
        let job_def = job_definition(vec![task_definition("noop")]);

        let result = job_def.with_max_iterations(0);
        assert!(matches!(result, Err(Error::Other(_))));
    }

    #[test]
    fn test_job_definition_with_iteration_retention_rejects_below_floor_and_applies_accepted_values() {
        let job_def = job_definition(vec![task_definition("noop")]);

        let below = job_def.clone().with_iteration_retention(4);
        assert!(matches!(below, Err(Error::Other(_))));

        assert!(job_def.clone().with_iteration_retention(5).is_ok());

        // The boundary tests below all build their definition with a window of 5, so this is the
        // only place where an accepted argument other than the floor is proven to be applied at
        // all: an ignored argument would leave the default window of 100 and yield `None` here.
        let above_floor = job_def.with_iteration_retention(7).unwrap();
        assert_eq!(above_floor.calculate_retention_boundary(8), Some(1));
    }

    fn build_job_definition_for_retention(iteration_retention: u64) -> JobDefinition {
        job_definition(vec![task_definition("noop")])
            .with_iteration_retention(iteration_retention)
            .unwrap()
    }

    #[test]
    fn test_retention_boundary_is_absent_while_the_window_covers_the_whole_history() {
        let job_def = build_job_definition_for_retention(5);

        assert_eq!(job_def.calculate_retention_boundary(4), None);
        assert_eq!(job_def.calculate_retention_boundary(5), None);
    }

    #[test]
    fn test_retention_boundary_is_the_newest_iteration_outside_the_window() {
        let job_def = build_job_definition_for_retention(5);

        assert_eq!(job_def.calculate_retention_boundary(6), Some(1));
        assert_eq!(job_def.calculate_retention_boundary(106), Some(101));
        assert_eq!(job_def.calculate_retention_boundary(u64::MAX), Some(u64::MAX - 5));
    }

    #[test]
    /// A negative interval is no longer representable - `std::time::Duration` is unsigned - so the
    /// remaining boundaries are zero and a value the millisecond-based scheduler cannot hold.
    fn test_job_definition_with_iteration_interval_rejects_zero_and_oversized() {
        let job_def = job_definition(vec![task_definition("noop")]);

        let zero = job_def.clone().with_iteration_interval(std::time::Duration::ZERO);
        assert!(matches!(zero, Err(Error::Other(_))));
        let oversized = job_def.with_iteration_interval(std::time::Duration::MAX);
        assert!(matches!(oversized, Err(Error::Other(_))));
    }

    #[test]
    fn test_job_new_accepts_input_at_limit() {
        let task_def = task_definition("fit").with_input(vec![0; 4]);
        let limits = TaskLimits {
            max_input_bytes: 4,
            max_output_bytes: 10,
        };

        let job = Job::new(
            &job_definition_with_limits(vec![task_def], limits),
            HashMap::new(),
            Uuid::from_u128(1599),
        )
        .unwrap();

        assert_eq!(job.tasks_as_iter().count(), 1);
    }

    #[test]
    fn test_add_task_rejects_oversized_input() {
        let limits = TaskLimits {
            max_input_bytes: 4,
            max_output_bytes: 10,
        };
        let init_def =
            TaskDefinition::new(TaskCode::new("init"), std::time::Duration::from_secs(5)).with_input(vec![0; 1]);
        let mut job = Job::new(
            &job_definition_with_limits(vec![init_def], limits),
            HashMap::new(),
            Uuid::from_u128(1700),
        )
        .unwrap();

        let task_def =
            TaskDefinition::new(TaskCode::new("child"), std::time::Duration::from_secs(5)).with_input(vec![0; 5]);
        let err = job.add_task(&task_def, Uuid::from_u128(1701)).err().unwrap();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_add_task_accepts_input_at_limit() {
        let limits = TaskLimits {
            max_input_bytes: 4,
            max_output_bytes: 10,
        };
        let init_def =
            TaskDefinition::new(TaskCode::new("init"), std::time::Duration::from_secs(5)).with_input(vec![0; 1]);
        let mut job = Job::new(
            &job_definition_with_limits(vec![init_def], limits),
            HashMap::new(),
            Uuid::from_u128(1750),
        )
        .unwrap();

        let task_def =
            TaskDefinition::new(TaskCode::new("child"), std::time::Duration::from_secs(5)).with_input(vec![0; 4]);
        let task_id = job.add_task(&task_def, Uuid::from_u128(1751)).unwrap();
        let task = job.get_task_arc(&task_id).unwrap();
        assert_eq!(task.input().len(), 4);
    }

    #[test]
    fn test_complete_task_rejects_oversized_output() {
        let limits = TaskLimits {
            max_input_bytes: 4,
            max_output_bytes: 4,
        };
        let init_def =
            TaskDefinition::new(TaskCode::new("init"), std::time::Duration::from_secs(5)).with_input(vec![0; 1]);
        let mut job = Job::new(
            &job_definition_with_limits(vec![init_def], limits),
            HashMap::new(),
            Uuid::from_u128(1800),
        )
        .unwrap();

        let task_id = *job.tasks_as_iter().next().unwrap().id();
        job.start_task(&task_id, Uuid::from_u128(1801)).unwrap();

        let err = job.complete_task(&task_id, vec![0; 5]).err().unwrap();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_complete_task_accepts_output_at_limit() {
        let limits = TaskLimits {
            max_input_bytes: 4,
            max_output_bytes: 4,
        };
        let init_def =
            TaskDefinition::new(TaskCode::new("init"), std::time::Duration::from_secs(5)).with_input(vec![0; 1]);
        let mut job = Job::new(
            &job_definition_with_limits(vec![init_def], limits),
            HashMap::new(),
            Uuid::from_u128(1850),
        )
        .unwrap();

        let task_id = *job.tasks_as_iter().next().unwrap().id();
        job.start_task(&task_id, Uuid::from_u128(1851)).unwrap();
        job.complete_task(&task_id, vec![0; 4]).unwrap();

        let task = job.get_task_arc(&task_id).unwrap();
        assert!(matches!(task.status(), TaskStatus::Completed));
        assert_eq!(task.output().len(), 4);
    }
}
