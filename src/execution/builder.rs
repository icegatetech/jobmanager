use std::{sync::Arc, time::Duration};

use crate::{
    CachedStorage, Error, InMemoryStorage, JobCleanerConfig, JobCode, JobDefinition, JobDefinitionId,
    JobDefinitionRegistry, JobRegistry, JobsManager, JobsManagerConfig, MetricsSink, NoopMetrics, RetrierConfig,
    S3Storage, S3StorageConfig, Storage, TaskCode, TaskDefinition, TaskExecutor, TaskLimits, TaskRef, WorkerConfig,
};

/// Describes one job: its schedule, its initial tasks, and every executor it may run.
///
/// Handed to the closure passed to [`JobsManagerBuilder::job`]. Nothing is validated here; a
/// mistake surfaces from [`JobsManagerBuilder::build`], so the closure needs no error handling.
pub struct JobBuilder {
    id: JobDefinitionId,
    code: JobCode,
    iteration_interval: Option<Duration>,
    max_iterations: Option<u64>,
    iteration_retention: Option<u64>,
    task_limits: Option<TaskLimits>,
    initial_tasks: Vec<(TaskDefinition, Arc<dyn TaskExecutor>)>,
    runtime_executors: Vec<(TaskCode, Arc<dyn TaskExecutor>)>,
    /// Declarations in the order they were made, not slots indexed by position: a reference from
    /// another job has no slot, and a slot layout is what used to drop such a declaration silently.
    dependencies: Vec<(TaskRef, Vec<TaskRef>)>,
}

impl JobBuilder {
    const fn new(id: JobDefinitionId, code: JobCode) -> Self {
        Self {
            id,
            code,
            iteration_interval: None,
            max_iterations: None,
            iteration_retention: None,
            task_limits: None,
            initial_tasks: Vec::new(),
            runtime_executors: Vec::new(),
            dependencies: Vec::new(),
        }
    }

    /// Requires the given delay to elapse from the start of an iteration before the next one may
    /// begin. Without it, the next iteration starts as soon as the previous one finishes.
    pub const fn every(&mut self, interval: Duration) -> &mut Self {
        self.iteration_interval = Some(interval);
        self
    }

    /// Caps how many iterations the job runs. The count includes the first one.
    pub const fn max_iterations(&mut self, count: u64) -> &mut Self {
        self.max_iterations = Some(count);
        self
    }

    /// Keeps only the given number of most recent iterations in storage.
    pub const fn keep_iterations(&mut self, count: u64) -> &mut Self {
        self.iteration_retention = Some(count);
        self
    }

    /// Replaces the default payload size caps.
    pub const fn task_limits(&mut self, limits: TaskLimits) -> &mut Self {
        self.task_limits = Some(limits);
        self
    }

    /// Declares a task created at the start of every iteration, and the executor that runs it.
    ///
    /// The returned reference names this task for [`Self::chain`] and [`Self::depends_on`]. The
    /// definition carries the task's input and attempt budget, so an initial task is described
    /// exactly like a runtime one.
    pub fn add_task(&mut self, definition: TaskDefinition, executor: Arc<dyn TaskExecutor>) -> TaskRef {
        let position = self.initial_tasks.len();
        self.initial_tasks.push((definition, executor));
        TaskRef::initial(self.id, position)
    }

    /// Registers an executor for a task this job creates at runtime through
    /// [`JobHandle::add_task`](crate::JobHandle::add_task). Such a task is not started with the
    /// iteration, so there is nothing here to hand a [`TaskRef`] out for.
    pub fn add_task_executor(&mut self, code: impl Into<TaskCode>, executor: Arc<dyn TaskExecutor>) -> &mut Self {
        self.runtime_executors.push((code.into(), executor));
        self
    }

    /// Makes `task` wait for every reference in `dependencies`.
    ///
    /// The declaration adds to whatever the task's own [`TaskDefinition`] already waits for, so a
    /// task described with [`TaskDefinition::with_dependencies`] ends up waiting for both lists.
    pub fn depends_on(&mut self, task: TaskRef, dependencies: &[TaskRef]) -> &mut Self {
        self.dependencies.push((task, dependencies.to_vec()));
        self
    }

    /// Links the tasks into a sequence: each one waits for the previous.
    ///
    /// Shorthand over [`Self::depends_on`], so both forms write the same dependency list.
    pub fn chain(&mut self, tasks: &[TaskRef]) -> &mut Self {
        for pair in tasks.windows(2) {
            self.depends_on(pair[1], &[pair[0]]);
        }
        self
    }

    /// Turns the description into a validated [`JobDefinition`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] for any reason [`JobDefinition::new`] rejects the description.
    fn build_definition(self) -> Result<JobDefinition, Error> {
        let mut job_def = JobDefinition::new(
            self.id,
            self.code,
            self.initial_tasks,
            self.runtime_executors,
            self.dependencies,
            self.task_limits.unwrap_or_default(),
        )?;
        if let Some(max_iterations) = self.max_iterations {
            job_def = job_def.with_max_iterations(max_iterations)?;
        }
        if let Some(iteration_retention) = self.iteration_retention {
            job_def = job_def.with_iteration_retention(iteration_retention)?;
        }
        if let Some(iteration_interval) = self.iteration_interval {
            job_def = job_def.with_iteration_interval(iteration_interval)?;
        }

        Ok(job_def)
    }
}

/// Which backend the pool persists job state to.
enum StorageChoice {
    Unset,
    S3(Box<S3StorageConfig>),
    InMemory,
}

/// Collects jobs and pool settings, then builds a ready [`JobsManager`].
///
/// Every setter takes and returns the builder by value, so jobs generated in a loop are added by
/// reassigning the builder. Nothing is validated until [`Self::build`].
pub struct JobsManagerBuilder {
    storage: StorageChoice,
    /// Whether an S3 backend is put behind the read cache. Kept apart from [`StorageChoice`] so
    /// `no_cache()` works whichever side of `s3()` it is called on.
    cache_storage: bool,
    jobs: Vec<JobBuilder>,
    worker_count: usize,
    /// Polling settings as they were named, each `None` until its setter is called: they become a
    /// [`WorkerConfig`] - and are checked - only in [`Self::build`], because the setters are `const`
    /// and cannot report a rejection.
    poll_interval: Option<Duration>,
    poll_jitter: Option<Duration>,
    max_poll_interval: Option<Duration>,
    retrier_config: Option<RetrierConfig>,
    cleaner_config: JobCleanerConfig,
    metrics: Arc<dyn MetricsSink>,
}

impl Default for JobsManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl JobsManagerBuilder {
    /// Starts from the pool defaults - one worker, cleanup on - with no storage and no jobs.
    pub fn new() -> Self {
        let defaults = JobsManagerConfig::default();
        Self {
            storage: StorageChoice::Unset,
            cache_storage: true,
            jobs: Vec::new(),
            worker_count: defaults.worker_count,
            poll_interval: None,
            poll_jitter: None,
            max_poll_interval: None,
            retrier_config: None,
            cleaner_config: defaults.cleaner_config,
            metrics: Arc::new(NoopMetrics),
        }
    }

    /// Persists job state to an S3-compatible store, behind the read cache.
    ///
    /// The cache is on by default because a worker re-reads the same job on every poll; drop it
    /// with [`Self::no_cache`].
    #[must_use]
    pub fn s3(mut self, config: S3StorageConfig) -> Self {
        self.storage = StorageChoice::S3(Box::new(config));
        self
    }

    /// Reads job state straight from the store, without the cache in front of it.
    ///
    /// Order-independent: it only clears a flag, so calling it before [`Self::s3`] works the same.
    /// Only [`Self::s3`] is cached, so on an [`Self::in_memory`] pool this changes nothing.
    #[must_use]
    pub const fn no_cache(mut self) -> Self {
        self.cache_storage = false;
        self
    }

    /// Keeps job state in memory only: every job of the pool, but only its current iteration, and
    /// nothing survives the process. For tests and examples, not for running work.
    #[must_use]
    pub fn in_memory(mut self) -> Self {
        self.storage = StorageChoice::InMemory;
        self
    }

    /// Routes this pool's measurements to `sink`. Without it nothing is recorded.
    #[must_use]
    pub fn metrics(mut self, sink: Arc<dyn MetricsSink>) -> Self {
        self.metrics = sink;
        self
    }

    /// Number of workers in the pool. Each runs one task at a time, so this bounds how many tasks
    /// execute concurrently across all jobs.
    #[must_use]
    pub const fn workers(mut self, count: usize) -> Self {
        self.worker_count = count;
        self
    }

    /// Base delay between storage polls of a worker that found work last time.
    ///
    /// Applies per job: a job with nothing to wait for is polled at this interval, while one whose
    /// next iteration is not due yet is not polled until it is.
    ///
    /// An unnamed backoff ceiling is ten times this interval, so a job that iterates once in
    /// minutes needs this setting alone; see [`Self::max_poll_interval`].
    ///
    /// [`Self::build`] rejects a zero interval, one longer than a year, and one above a ceiling
    /// named through [`Self::max_poll_interval`].
    #[must_use]
    pub const fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = Some(interval);
        self
    }

    /// Upper bound of the random delay added to each poll, which keeps workers from polling in
    /// lockstep.
    ///
    /// Also spreads the moment jobs sharing a schedule are picked up: without it every worker of
    /// every pool wakes at the same instant when an interval-scheduled job becomes due, and all but
    /// one pay a rejected write. A fleet of many workers on interval-scheduled jobs wants this
    /// raised.
    #[must_use]
    pub const fn poll_jitter(mut self, jitter: Duration) -> Self {
        self.poll_jitter = Some(jitter);
        self
    }

    /// Ceiling the poll interval backs off to while there is no work.
    ///
    /// Left unset, the ceiling is ten times [`Self::poll_interval`] - so a pool polled every 200 ms
    /// backs off to two seconds, and one polled every ten seconds backs off to a hundred.
    ///
    /// [`Self::build`] rejects a ceiling below the poll interval and one longer than a year.
    #[must_use]
    pub const fn max_poll_interval(mut self, interval: Duration) -> Self {
        self.max_poll_interval = Some(interval);
        self
    }

    /// Retry policy every worker applies to its storage operations.
    #[must_use]
    pub fn retrier(mut self, config: RetrierConfig) -> Self {
        self.retrier_config = Some(config);
        self
    }

    /// Replaces the settings of the background cleaner that trims old iterations.
    #[must_use]
    pub fn cleaner(mut self, config: JobCleanerConfig) -> Self {
        self.cleaner_config = config;
        self
    }

    /// Keeps every iteration in storage forever instead of trimming the tail.
    #[must_use]
    pub const fn no_cleanup(mut self) -> Self {
        self.cleaner_config.enabled = false;
        self
    }

    /// Adds a job under `code`, described by `build`.
    ///
    /// Call it once per job; a loop over specifications is how several jobs are added, since the
    /// builder is taken and returned by value.
    #[must_use]
    pub fn job<F>(mut self, code: impl Into<JobCode>, build: F) -> Self
    where
        F: FnOnce(&mut JobBuilder),
    {
        let mut job = JobBuilder::new(JobDefinitionId::new(), code.into());
        build(&mut job);
        self.jobs.push(job);
        self
    }

    /// Builds the manager. Workers are not started until [`JobsManager::start`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] if no storage backend was chosen, if the chosen backend cannot be
    /// constructed, if no job was added, if a job carries an empty code, if a job has no initial
    /// task, if two jobs share a code, if a task definition cannot produce a legal task, if one
    /// code carries two different executors, if a dependency names a task outside its own job, if a
    /// dependency names a task created at runtime, or if the declared dependencies form a cycle.
    ///
    /// The settings the `const` setters accept without looking at them are validated here too, so
    /// it also returns [`Error::Other`] for [`Self::workers`] of 0, a [`Self::poll_interval`] of
    /// zero, an explicitly set [`Self::max_poll_interval`] below the poll interval,
    /// [`JobBuilder::max_iterations`] of 0, [`JobBuilder::keep_iterations`] below 5, and a
    /// [`JobBuilder::every`] interval that is zero or too large to schedule.
    pub async fn build(self) -> Result<JobsManager, Error> {
        if self.jobs.is_empty() {
            return Err(Error::Other("at least one job must be added".into()));
        }

        let mut job_defs = Vec::with_capacity(self.jobs.len());
        for job in self.jobs {
            job_defs.push(job.build_definition()?);
        }

        // The registry has to exist before the backend: a backend reads job settings out of it
        // while deserializing persisted state.
        let registry = Arc::new(JobRegistry::new(job_defs)?);
        let storage = build_storage(self.storage, self.cache_storage, &registry, &self.metrics).await?;

        let config = JobsManagerConfig {
            worker_count: self.worker_count,
            worker_config: build_worker_config(
                self.poll_interval,
                self.poll_jitter,
                self.max_poll_interval,
                self.retrier_config,
            )?,
            cleaner_config: self.cleaner_config,
        };
        JobsManager::new(storage, config, registry, self.metrics)
    }
}

/// Hands the polling settings a caller named to [`WorkerConfig`], which is where each of them is
/// checked. A setting nobody named is left out, so the config keeps its own default for it.
fn build_worker_config(
    poll_interval: Option<Duration>,
    poll_jitter: Option<Duration>,
    max_poll_interval: Option<Duration>,
    retrier_config: Option<RetrierConfig>,
) -> Result<WorkerConfig, Error> {
    let mut config = WorkerConfig::new();
    if let Some(interval) = poll_interval {
        config = config.with_poll_interval(interval)?;
    }
    if let Some(ceiling) = max_poll_interval {
        config = config.with_max_poll_interval(ceiling)?;
    }
    if let Some(jitter) = poll_jitter {
        config = config.with_poll_jitter(jitter);
    }
    if let Some(retrier_config) = retrier_config {
        config = config.with_retrier(retrier_config);
    }

    Ok(config)
}

/// Constructs the chosen backend, wrapping it in the read cache where that was asked for.
async fn build_storage(
    choice: StorageChoice,
    cache_storage: bool,
    registry: &Arc<JobRegistry>,
    metrics: &Arc<dyn MetricsSink>,
) -> Result<Arc<dyn Storage>, Error> {
    match choice {
        StorageChoice::Unset => Err(Error::Other(
            "storage backend is not configured: call s3() or in_memory()".into(),
        )),
        StorageChoice::S3(config) => {
            let registry = Arc::clone(registry) as Arc<dyn JobDefinitionRegistry>;
            let storage = Arc::new(S3Storage::new(*config, registry, metrics.clone()).await?) as Arc<dyn Storage>;
            if cache_storage {
                Ok(Arc::new(CachedStorage::new(storage, metrics.clone())))
            } else {
                Ok(storage)
            }
        }
        StorageChoice::InMemory => Ok(Arc::new(InMemoryStorage::new())),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use super::*;
    use crate::{TaskOutcome, task_fn};

    fn noop_executor() -> Arc<dyn TaskExecutor> {
        task_fn(|_ctx| async { Ok(TaskOutcome::empty()) })
    }

    fn task(code: &str) -> TaskDefinition {
        TaskDefinition::new(code, Duration::from_secs(1))
    }

    #[tokio::test]
    async fn build_rejects_missing_storage() {
        let error = JobsManagerBuilder::new()
            .job("j", |j| {
                j.add_task(task("t"), noop_executor());
            })
            .build()
            .await
            .err()
            .expect("a pool without a backend must not build");

        assert!(error.to_string().contains("storage backend"), "got: {error}");
    }

    #[tokio::test]
    async fn build_rejects_a_pool_without_jobs() {
        let error = JobsManagerBuilder::new()
            .in_memory()
            .build()
            .await
            .err()
            .expect("a pool without jobs must not build");

        assert!(error.to_string().contains("at least one job"), "got: {error}");
    }

    /// The definition is checked at build time, not when the first iteration is planned, so a
    /// misconfigured timeout is reported before any worker starts.
    #[tokio::test]
    async fn build_rejects_an_invalid_initial_task_definition() {
        let error = JobsManagerBuilder::new()
            .in_memory()
            .job("j", |j| {
                j.add_task(TaskDefinition::new("t", Duration::ZERO), noop_executor());
            })
            .build()
            .await
            .err()
            .expect("a zero timeout must not build");

        assert!(error.to_string().contains("timeout"), "got: {error}");
    }

    #[tokio::test]
    async fn build_rejects_job_without_initial_tasks() {
        let error = JobsManagerBuilder::new()
            .in_memory()
            .job("j", |j| {
                j.add_task_executor("t", noop_executor());
            })
            .build()
            .await
            .err()
            .expect("a job with no initial task must not build");

        assert!(error.to_string().contains("no initial task"), "got: {error}");
    }

    #[tokio::test]
    async fn build_rejects_duplicate_job_code() {
        let error = JobsManagerBuilder::new()
            .in_memory()
            .job("j", |j| {
                j.add_task(task("t"), noop_executor());
            })
            .job("j", |j| {
                j.add_task(task("t"), noop_executor());
            })
            .build()
            .await
            .err()
            .expect("two jobs sharing a code must not build");

        assert!(error.to_string().contains("duplicate"), "got: {error}");
    }

    /// Which executor runs a task would otherwise depend on registration order.
    #[tokio::test]
    async fn build_rejects_two_executors_for_one_task_code() {
        let error = JobsManagerBuilder::new()
            .in_memory()
            .job("j", |j| {
                j.add_task(task("t"), noop_executor());
                j.add_task_executor("t", noop_executor());
            })
            .build()
            .await
            .err()
            .expect("two executors for one code must not build");

        assert!(error.to_string().contains("two different executors"), "got: {error}");
    }

    /// The graph rules themselves are `JobDefinition`'s and are tested there; this asserts only
    /// that a job assembled through the public closure reaches them and reports what they found.
    #[tokio::test]
    async fn build_rejects_a_dependency_on_another_jobs_task() {
        let mut stolen = None;
        let error = JobsManagerBuilder::new()
            .in_memory()
            .job("first", |j| {
                stolen = Some(j.add_task(task("a"), noop_executor()));
            })
            .job("second", |j| {
                let own = j.add_task(task("b"), noop_executor());
                j.depends_on(own, &[stolen.expect("the first job declared a task")]);
            })
            .build()
            .await
            .err()
            .expect("a cross-job dependency must not build");

        assert!(error.to_string().contains("outside its own job"), "got: {error}");
    }

    /// A [`TaskRef`] is `Copy` and outlives the builder that handed it out. While a description was
    /// identified by its place among the builder's jobs, the first job of every builder handed out
    /// the same reference, and one from a foreign builder resolved here as the local task sitting
    /// at that place.
    #[tokio::test]
    async fn build_rejects_a_definition_depending_on_another_builders_task() {
        let foreign = task_ref_from_another_builder().await;

        let error = JobsManagerBuilder::new()
            .in_memory()
            .job("j", |j| {
                j.add_task(task("a").with_dependencies(vec![foreign]), noop_executor());
            })
            .build()
            .await
            .err()
            .expect("a dependency from another builder must not build");

        assert!(error.to_string().contains("outside its own job"), "got: {error}");
    }

    /// The same reference in the position naming the task that is *given* dependencies, which the
    /// declaration channel resolves through the same rule.
    #[tokio::test]
    async fn build_rejects_a_declaration_naming_another_builders_task() {
        let foreign = task_ref_from_another_builder().await;

        let error = JobsManagerBuilder::new()
            .in_memory()
            .job("j", |j| {
                let own = j.add_task(task("a"), noop_executor());
                j.depends_on(foreign, &[own]);
            })
            .build()
            .await
            .err()
            .expect("a declaration from another builder must not build");

        assert!(error.to_string().contains("outside its own job"), "got: {error}");
    }

    /// Reference to the first task of the first job of a pool that was built and then dropped -
    /// the reference a caller is left holding, and the one that collided with the next builder's.
    async fn task_ref_from_another_builder() -> TaskRef {
        let mut handed_out = None;
        JobsManagerBuilder::new()
            .in_memory()
            .job("j", |j| {
                handed_out = Some(j.add_task(task("a"), noop_executor()));
            })
            .build()
            .await
            .expect("the first pool must build");

        handed_out.expect("the job declared a task")
    }

    /// The setters below are `const` and store their argument unexamined, so `build` is the only
    /// place a caller learns the value was rejected.
    #[tokio::test]
    async fn build_rejects_a_pool_without_workers() {
        let error = JobsManagerBuilder::new()
            .in_memory()
            .workers(0)
            .job("j", |j| {
                j.add_task(task("t"), noop_executor());
            })
            .build()
            .await
            .err()
            .expect("a pool without workers must not build");

        assert!(
            error.to_string().contains("worker count must be at least 1"),
            "got: {error}"
        );
    }

    /// A worker given a zero interval polls storage without ever pausing, which saturates a core
    /// and bills for an unbounded number of requests.
    #[tokio::test]
    async fn build_rejects_a_zero_poll_interval() {
        let error = JobsManagerBuilder::new()
            .in_memory()
            .poll_interval(Duration::ZERO)
            .job("j", |j| {
                j.add_task(task("t"), noop_executor());
            })
            .build()
            .await
            .err()
            .expect("a zero poll interval must not build");

        assert!(
            error.to_string().contains("poll interval must be positive"),
            "got: {error}"
        );
    }

    /// The maximum is the ceiling the backoff climbs to, so a value below the base would turn the
    /// backoff into a speed-up.
    #[tokio::test]
    async fn build_rejects_a_maximum_poll_interval_below_the_poll_interval() {
        let error = JobsManagerBuilder::new()
            .in_memory()
            .poll_interval(Duration::from_millis(200))
            .max_poll_interval(Duration::from_millis(100))
            .job("j", |j| {
                j.add_task(task("t"), noop_executor());
            })
            .build()
            .await
            .err()
            .expect("a ceiling below the base interval must not build");

        assert!(
            error.to_string().contains("max poll interval must not be below"),
            "got: {error}"
        );
    }

    /// An unnamed ceiling is derived from the poll interval, so however rarely a pool is polled it
    /// is never refused over a ceiling its caller never named.
    #[tokio::test]
    async fn build_accepts_a_long_poll_interval_without_a_named_ceiling() {
        JobsManagerBuilder::new()
            .in_memory()
            .poll_interval(Duration::from_secs(10))
            .job("j", |j| {
                j.add_task(task("t"), noop_executor());
            })
            .build()
            .await
            .expect("a poll interval with no named ceiling must build");
    }

    #[tokio::test]
    async fn build_rejects_a_job_with_an_empty_code() {
        let error = JobsManagerBuilder::new()
            .in_memory()
            .job("", |j| {
                j.add_task(task("t"), noop_executor());
            })
            .build()
            .await
            .err()
            .expect("a job with an empty code must not build");

        assert!(error.to_string().contains("job code cannot be empty"), "got: {error}");
    }

    #[tokio::test]
    async fn build_rejects_a_job_that_may_never_iterate() {
        let error = JobsManagerBuilder::new()
            .in_memory()
            .job("j", |j| {
                j.max_iterations(0);
                j.add_task(task("t"), noop_executor());
            })
            .build()
            .await
            .err()
            .expect("a job capped at zero iterations must not build");

        assert!(
            error.to_string().contains("max iterations must be positive"),
            "got: {error}"
        );
    }

    #[tokio::test]
    async fn build_rejects_a_retention_window_below_the_floor() {
        let error = JobsManagerBuilder::new()
            .in_memory()
            .job("j", |j| {
                j.keep_iterations(4);
                j.add_task(task("t"), noop_executor());
            })
            .build()
            .await
            .err()
            .expect("a retention window below the floor must not build");

        assert!(
            error.to_string().contains("iteration retention must be at least"),
            "got: {error}"
        );
    }

    #[tokio::test]
    async fn build_rejects_a_zero_iteration_interval() {
        let error = JobsManagerBuilder::new()
            .in_memory()
            .job("j", |j| {
                j.every(Duration::ZERO);
                j.add_task(task("t"), noop_executor());
            })
            .build()
            .await
            .err()
            .expect("a zero iteration interval must not build");

        assert!(
            error.to_string().contains("iteration interval must be positive"),
            "got: {error}"
        );
    }

    /// The shape the backend is offered for: several jobs on one pool, end to end. That their
    /// states really stay apart is asserted on the backend itself, where a lost job is observable
    /// rather than merely likely.
    #[tokio::test]
    async fn two_jobs_complete_on_the_in_memory_backend() -> Result<(), Box<dyn std::error::Error>> {
        let manager = JobsManagerBuilder::new()
            .in_memory()
            .poll_interval(Duration::from_millis(10))
            .job("first", |j| {
                j.max_iterations(1);
                j.add_task(task("a"), noop_executor());
            })
            .job("second", |j| {
                j.max_iterations(1);
                j.add_task(task("b"), noop_executor());
            })
            .build()
            .await?;

        let handle = manager.start()?;
        for job_code in ["first", "second"] {
            tokio::time::timeout(
                Duration::from_secs(10),
                handle.wait_for_job_completion(&JobCode::new(job_code)),
            )
            .await??;
        }

        handle.shutdown().await?;
        Ok(())
    }

    /// The polling setters store their argument and nothing else, so the defect they are exposed to
    /// is one landing on the setting of a neighbour - which is how a jitter came to be applied as a
    /// poll interval. What each setting then does to a worker is asserted in [`crate::Worker`]'s own
    /// tests.
    #[test]
    fn the_polling_setters_record_the_settings_they_name() {
        let builder = JobsManagerBuilder::new()
            .poll_interval(Duration::from_millis(11))
            .poll_jitter(Duration::from_millis(22))
            .max_poll_interval(Duration::from_millis(33));

        assert_eq!(builder.poll_interval, Some(Duration::from_millis(11)));
        assert_eq!(builder.poll_jitter, Some(Duration::from_millis(22)));
        assert_eq!(builder.max_poll_interval, Some(Duration::from_millis(33)));
    }

    #[test]
    fn the_retrier_setter_records_the_retry_policy() {
        let builder = JobsManagerBuilder::new().retrier(RetrierConfig {
            max_attempts: 3,
            rand_delay: Duration::from_millis(7),
            delays: vec![Duration::from_millis(9)],
        });

        let recorded = builder.retrier_config.expect("the setter recorded a policy");
        assert_eq!(recorded.max_attempts, 3);
        assert_eq!(recorded.rand_delay, Duration::from_millis(7));
        assert_eq!(recorded.delays, vec![Duration::from_millis(9)]);
    }

    #[test]
    fn the_cleaner_setter_records_the_cleanup_settings() {
        let builder = JobsManagerBuilder::new().cleaner(JobCleanerConfig {
            enabled: false,
            retrier_config: RetrierConfig {
                max_attempts: 4,
                ..Default::default()
            },
        });

        assert_eq!(builder.cleaner_config.retrier_config.max_attempts, 4);
    }

    /// Opting out of cleanup must leave the cleaner switched off rather than merely unconfigured;
    /// that a switched-off cleaner deletes nothing is asserted in `job_cleanup_test`.
    #[test]
    fn no_cleanup_switches_the_cleaner_off() {
        let builder = JobsManagerBuilder::new().no_cleanup();

        assert!(!builder.cleaner_config.enabled);
    }

    /// Opting out of the read cache must leave the flag the S3 backend is wrapped by cleared rather
    /// than merely unconfigured; what the cache itself spares the backend is asserted in
    /// `cache_invalidation_test`.
    #[test]
    fn no_cache_switches_the_read_cache_off() {
        assert!(
            JobsManagerBuilder::new().cache_storage,
            "the cache is on until opted out of"
        );
        assert!(!JobsManagerBuilder::new().no_cache().cache_storage);
    }

    /// Identity the descriptions built by [`definition_of`] carry. One for the whole module,
    /// because an identity is minted per description and the tests below both rebuild references
    /// under it and compare two descriptions against each other.
    fn test_definition_id() -> JobDefinitionId {
        static ID: LazyLock<JobDefinitionId> = LazyLock::new(JobDefinitionId::new);
        *ID
    }

    /// Builds one job through the public closure and returns its definition, so a test can assert
    /// on what was declared without reaching into the assembled manager.
    fn definition_of<F: FnOnce(&mut JobBuilder)>(build: F) -> Result<JobDefinition, Error> {
        let mut job = JobBuilder::new(test_definition_id(), JobCode::new("j"));
        build(&mut job);
        job.build_definition()
    }

    /// The reference a test compares against names a position under the identity the description
    /// was built with, so it is the reference that description handed out.
    fn initial_task_ref(position: usize) -> TaskRef {
        TaskRef::initial(test_definition_id(), position)
    }

    /// The declared order is what the planner will resolve into blocking, so the definition has to
    /// carry it literally.
    #[test]
    fn depends_on_records_the_declared_positions() {
        let job_def = definition_of(|j| {
            let first = j.add_task(task("a"), noop_executor());
            let second = j.add_task(task("b"), noop_executor());
            let third = j.add_task(task("c"), noop_executor());
            j.depends_on(third, &[first, second]);
        })
        .unwrap();

        let declared: Vec<&[TaskRef]> = job_def.initial_tasks().iter().map(TaskDefinition::depends_on).collect();
        assert_eq!(
            declared,
            vec![&[][..], &[][..], &[initial_task_ref(0), initial_task_ref(1)][..]]
        );
    }

    /// `chain` is shorthand over `depends_on`, so both forms must write the same list rather than
    /// two similar-looking ones.
    #[test]
    fn chain_writes_the_same_dependencies_as_depends_on() {
        let chained = definition_of(|j| {
            let first = j.add_task(task("a"), noop_executor());
            let second = j.add_task(task("b"), noop_executor());
            let third = j.add_task(task("c"), noop_executor());
            j.chain(&[first, second, third]);
        })
        .unwrap();
        let spelled_out = definition_of(|j| {
            let first = j.add_task(task("a"), noop_executor());
            let second = j.add_task(task("b"), noop_executor());
            let third = j.add_task(task("c"), noop_executor());
            j.depends_on(second, &[first]);
            j.depends_on(third, &[second]);
        })
        .unwrap();

        let dependencies = |job_def: &JobDefinition| -> Vec<Vec<TaskRef>> {
            job_def.initial_tasks().iter().map(|task| task.depends_on().to_vec()).collect()
        };
        assert_eq!(
            dependencies(&chained),
            vec![vec![], vec![initial_task_ref(0)], vec![initial_task_ref(1)]]
        );
        assert_eq!(dependencies(&chained), dependencies(&spelled_out));
    }

    /// A definition may declare its dependencies itself; a later `depends_on` adds to them rather
    /// than replacing them, so neither channel silently loses what the other declared.
    #[test]
    fn build_definition_keeps_dependencies_declared_on_the_definition() {
        let job_def = definition_of(|j| {
            let first = j.add_task(task("a"), noop_executor());
            let second = j.add_task(task("b"), noop_executor());
            let third = j.add_task(task("c").with_dependencies(vec![first]), noop_executor());
            j.depends_on(third, &[second]);
        })
        .unwrap();

        assert_eq!(
            job_def.initial_tasks()[2].depends_on(),
            &[initial_task_ref(0), initial_task_ref(1)]
        );
    }
}
