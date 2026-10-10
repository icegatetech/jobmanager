use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{Barrier, Notify};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::common::manager_env::ManagerEnv;
use super::common::storage_wrapper::{
    CountingStorage, FirstReadPanickingStorage, FirstResultSavePanickingStorage, ReadHoldingStorage,
};
use super::common::waiting::{CONDITION_TIMEOUT, measure_settled_requests, wait_until, wait_until_job_is_processed};
use crate::core::job::TaskPickup;
use crate::core::job::{Job, JobCode, JobDefinition, JobDefinitionId, TaskLimits};
use crate::core::registry::JobRegistry;
use crate::core::task::{FAILED_TASK_RETRY_PAUSE, TaskCode, TaskDefinition, TaskRef, TaskRetry};
use crate::execution::executor::{TaskExecutor, TaskOutcome, task_fn};
use crate::execution::job_cleaner::JobCleanerConfig;
use crate::execution::jobs_manager::JobsManagerConfig;
use crate::execution::scheduler::PollSchedulerConfig;
use crate::infra::metrics::NoopMetrics;
use crate::storage::Storage;
use crate::storage::cached::CachedStorage;
use crate::storage::in_memory::InMemoryStorage;

/// Base interval of the tests below that are about when a task starts: far above the bound they
/// assert, so a start that waited for a poll interval cannot pass for one that did not.
const LONG_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// How soon after the moment it became takeable a task must start, in the tests below that are
/// about a start not waiting for [`LONG_POLL_INTERVAL`]. Half of it, so a loaded machine has room
/// while a start that waited for the interval does not.
const PROMPT_START_BOUND: Duration = Duration::from_secs(5);

fn poll_schedule_config(
    poll_interval: Duration,
    max_poll_interval: Duration,
) -> Result<PollSchedulerConfig, crate::core::error::Error> {
    super::common::build_poll_schedule_config(poll_interval, Duration::ZERO).with_max_poll_interval(max_poll_interval)
}

/// A pool over `store`, behind the read cache when `is_cached` is set.
fn start_pool(
    store: Arc<dyn Storage>,
    is_cached: bool,
    worker_count: usize,
    poll_schedule_config: PollSchedulerConfig,
    job_defs: Vec<JobDefinition>,
) -> Result<ManagerEnv, Box<dyn std::error::Error>> {
    let storage = if is_cached {
        Arc::new(CachedStorage::new(store, Arc::new(NoopMetrics))) as Arc<dyn Storage>
    } else {
        store
    };
    ManagerEnv::new(
        storage,
        JobsManagerConfig {
            worker_count,
            scheduler_config: poll_schedule_config,
            cleaner_config: JobCleanerConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        },
        Arc::new(JobRegistry::new(job_defs.clone())?),
        job_defs,
    )
}

/// A job of `task_count` tasks running `executor`, each depending on the one before it when
/// `is_chained` is set.
fn job_of_tasks(
    job_code: &str,
    task_count: usize,
    is_chained: bool,
    task_timeout: Duration,
    executor: &Arc<dyn TaskExecutor>,
) -> Result<JobDefinition, crate::core::error::Error> {
    let definition_id = JobDefinitionId::new();
    let tasks = (0..task_count)
        .map(|position| {
            (
                TaskDefinition::new(TaskCode::new(format!("task_{position}")), task_timeout),
                Arc::clone(executor),
            )
        })
        .collect();
    let declared_dependencies = if is_chained {
        (1..task_count)
            .map(|position| {
                (
                    TaskRef::initial(definition_id, position),
                    vec![TaskRef::initial(definition_id, position - 1)],
                )
            })
            .collect()
    } else {
        Vec::new()
    };
    JobDefinition::new(
        definition_id,
        JobCode::new(job_code),
        tasks,
        Vec::new(),
        declared_dependencies,
        TaskLimits::default(),
    )
}

/// Deadline of the task another pool holds below: far beyond the window, so no pass the window
/// counts can take it over.
const HELD_TASK_TIMEOUT: Duration = Duration::from_secs(3600);

/// Stores the job of `job_def` with its task started by a worker of no pool under test.
async fn store_with_a_task_held_elsewhere(
    job_def: &JobDefinition,
) -> Result<Arc<InMemoryStorage>, Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStorage::new());
    let foreign_worker_id = Uuid::from_u128(7_101);
    let mut job = Job::new(job_def, std::collections::HashMap::new(), foreign_worker_id)?;
    let TaskPickup::Ready(task_id) = job.pick_task_to_execute(&foreign_worker_id)? else {
        return Err("the fixture must leave a task to start".into());
    };
    job.start_task(&task_id, foreign_worker_id)?;
    store.save_job(&mut job, &CancellationToken::new()).await?;
    Ok(store)
}

/// Requirement: what an idle pool costs does not depend on how many workers it has. A job whose
/// task runs in another pool is polled on one schedule, backing off from 200 ms to its ceiling of
/// 2 s: passes at 0, 0.4, 1.2, 2.8, 4.8 s and every 2 s after that, so the window `(10 s, 30 s]`
/// holds the ten at 10.8 ... 28.8 s. The tokio clock is paused, which is what makes it a number;
/// the system clock the task's deadline is judged by does not move, so no takeover lands in it.
///
/// Checked by giving every worker a schedule of its own: four workers then cost forty reads.
async fn assert_a_job_held_elsewhere_is_polled_as_often_whatever_the_worker_count(
    is_cached: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    for worker_count in [1, 4] {
        let job_def = job_of_tasks(
            "held_elsewhere_job",
            1,
            false,
            HELD_TASK_TIMEOUT,
            &task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
        )?;
        let job_code = job_def.code().clone();
        let store = store_with_a_task_held_elsewhere(&job_def).await?;
        let counting = Arc::new(CountingStorage::new(store as Arc<dyn Storage>));
        let _env = start_pool(
            Arc::clone(&counting) as Arc<dyn Storage>,
            is_cached,
            worker_count,
            poll_schedule_config(Duration::from_millis(200), Duration::from_secs(2))?,
            vec![job_def],
        )?;

        tokio::time::sleep(Duration::from_secs(10)).await;
        let reads_before = counting.read_calls_of(&job_code);
        tokio::time::sleep(Duration::from_secs(20)).await;

        assert_eq!(
            counting.read_calls_of(&job_code) - reads_before,
            10,
            "a pool of {worker_count} workers must poll the job on one schedule"
        );
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_job_held_elsewhere_is_polled_as_often_whatever_the_worker_count() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    assert_a_job_held_elsewhere_is_polled_as_often_whatever_the_worker_count(true).await
}

#[tokio::test(start_paused = true)]
async fn a_job_held_elsewhere_is_polled_as_often_whatever_the_worker_count_without_the_cache()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    assert_a_job_held_elsewhere_is_polled_as_often_whatever_the_worker_count(false).await
}

/// How long an executor below waits for its sibling: the two meet only if they run at once.
const RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(5);

/// Requirement: the claim covers the pass, not the execution, so two tasks of one job still run
/// on two workers at once. Each executor waits for the other at a barrier.
///
/// Checked by releasing the claim after the execution rather than before it: the executors then
/// never meet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_tasks_of_one_job_run_at_the_same_time_on_two_workers() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let rendezvous = Arc::new(Barrier::new(2));
    let has_met = Arc::new(AtomicBool::new(false));
    let executor_rendezvous = Arc::clone(&rendezvous);
    let executor_has_met = Arc::clone(&has_met);
    let executor = task_fn(move |_ctx| {
        let rendezvous = Arc::clone(&executor_rendezvous);
        let has_met = Arc::clone(&executor_has_met);
        async move {
            tokio::time::timeout(RENDEZVOUS_TIMEOUT, rendezvous.wait())
                .await
                .map_err(|_| "the sibling task never ran alongside this one")?;
            has_met.store(true, Ordering::SeqCst);
            Ok(TaskOutcome::empty())
        }
    });
    let job_def = job_of_tasks("parallel_job", 2, false, Duration::from_secs(30), &executor)?.with_max_iterations(1)?;
    let env = start_pool(
        Arc::new(InMemoryStorage::new()),
        true,
        2,
        super::common::build_poll_schedule_config(Duration::from_millis(20), Duration::ZERO),
        vec![job_def],
    )?;

    env.wait_for_all_jobs_completion(CONDITION_TIMEOUT).await?;

    assert!(
        has_met.load(Ordering::SeqCst),
        "the two tasks must have run at the same time"
    );
    Ok(())
}

/// Requirement: a pass held up in storage on one job does not keep the pool's other worker off
/// another job. The reads of one job are held until the other job has run its task out.
///
/// Checked by holding one claim over the whole schedule rather than one per job: the second job
/// then never runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pass_held_in_storage_on_one_job_does_not_keep_the_pool_off_another() -> Result<(), Box<dyn std::error::Error>>
{
    super::common::init_tracing();
    let executor = task_fn(|_ctx| async { Ok(TaskOutcome::empty()) });
    // The held job sorts first, so the schedule hands it out first (`PollScheduler::due_queue`): with
    // one claim over the whole schedule the free job is then never claimed.
    let held_job_def = job_of_tasks("a_held_job", 1, false, Duration::from_secs(30), &executor)?;
    let free_job_def =
        job_of_tasks("b_free_job", 1, false, Duration::from_secs(30), &executor)?.with_max_iterations(1)?;
    let holding = Arc::new(ReadHoldingStorage::new(
        Arc::new(InMemoryStorage::new()),
        held_job_def.code().clone(),
    ));
    let env = start_pool(
        Arc::clone(&holding) as Arc<dyn Storage>,
        true,
        2,
        super::common::build_poll_schedule_config(Duration::from_millis(20), Duration::ZERO),
        vec![held_job_def, free_job_def],
    )?;

    let free_job_result = env.wait_for_all_jobs_completion(CONDITION_TIMEOUT).await;
    let is_read_held = holding.is_read_held();
    holding.release_reads();

    free_job_result?;
    assert!(
        is_read_held,
        "the fixture must have held a pass over the other job in storage throughout"
    );
    Ok(())
}

/// Moments the executors below record, read by the test once they are in.
#[derive(Default)]
struct ExecutionMoments {
    started_at: Mutex<Vec<(TaskCode, Instant)>>,
    finished_at: Mutex<Vec<(TaskCode, Instant)>>,
}

impl ExecutionMoments {
    fn record_start(&self, task_code: &TaskCode) {
        self.started_at.lock().push((task_code.clone(), Instant::now()));
    }

    fn record_finish(&self, task_code: &TaskCode) {
        self.finished_at.lock().push((task_code.clone(), Instant::now()));
    }

    fn starts(&self) -> Vec<(TaskCode, Instant)> {
        self.started_at.lock().clone()
    }

    fn finishes(&self) -> Vec<(TaskCode, Instant)> {
        self.finished_at.lock().clone()
    }

    async fn wait_for_starts(
        &self,
        start_count: usize,
    ) -> Result<Vec<(TaskCode, Instant)>, Box<dyn std::error::Error>> {
        wait_until(
            || self.started_at.lock().len() >= start_count,
            &format!("{start_count} executions have started"),
        )
        .await?;
        Ok(self.starts())
    }
}

/// An executor that records when it starts and when it returns.
fn recording_executor(moments: &Arc<ExecutionMoments>) -> Arc<dyn TaskExecutor> {
    let moments = Arc::clone(moments);
    task_fn(move |ctx| {
        let moments = Arc::clone(&moments);
        async move {
            moments.record_start(ctx.code());
            moments.record_finish(ctx.code());
            Ok(TaskOutcome::empty())
        }
    })
}

/// A pass that started one task of a fan-out leaves the second for the next free worker at once.
///
/// Checked by scheduling `JobProgress::TaskPickable` at the base interval: the second task then
/// starts ten seconds after the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_tasks_of_a_fan_out_start_without_waiting_for_the_poll_interval() -> Result<(), Box<dyn std::error::Error>>
{
    super::common::init_tracing();
    let moments = Arc::new(ExecutionMoments::default());
    let executor_moments = Arc::clone(&moments);
    // Each execution runs until both have started, so the end of the first cannot be what hands the
    // second task out.
    let executor = task_fn(move |ctx| {
        let moments = Arc::clone(&executor_moments);
        async move {
            moments.record_start(ctx.code());
            wait_until(|| moments.starts().len() >= 2, "both tasks have started")
                .await
                .map_err(|e| e.to_string())?;
            Ok(TaskOutcome::empty())
        }
    });
    let job_def = job_of_tasks("fan_out_job", 2, false, Duration::from_secs(30), &executor)?;
    let pool_started_at = Instant::now();
    let _env = start_pool(
        Arc::new(InMemoryStorage::new()),
        true,
        2,
        poll_schedule_config(LONG_POLL_INTERVAL, LONG_POLL_INTERVAL)?,
        vec![job_def],
    )?;

    let starts = moments.wait_for_starts(2).await?;

    for (task_code, started_at) in starts {
        assert!(
            started_at.duration_since(pool_started_at) < PROMPT_START_BOUND,
            "task {task_code} started {:?} after the pool",
            started_at.duration_since(pool_started_at)
        );
    }
    Ok(())
}

/// The result that unblocks a dependent leaves it for the next free worker at once.
///
/// Checked by recording every execution end as `JobProgress::NothingPickable`: the dependent then
/// starts ten seconds after the result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dependent_starts_right_after_the_result_it_waited_for() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let moments = Arc::new(ExecutionMoments::default());
    let job_def = job_of_tasks(
        "chained_job",
        2,
        true,
        Duration::from_secs(30),
        &recording_executor(&moments),
    )?;
    let _env = start_pool(
        Arc::new(InMemoryStorage::new()),
        true,
        2,
        poll_schedule_config(LONG_POLL_INTERVAL, LONG_POLL_INTERVAL)?,
        vec![job_def],
    )?;

    let starts = moments.wait_for_starts(2).await?;

    let dependency_finished_at = moments
        .finishes()
        .into_iter()
        .find(|(task_code, _)| task_code.as_str() == "task_0")
        .map(|(_, finished_at)| finished_at)
        .ok_or("the dependency must have finished before its dependent started")?;
    let dependent_started_at = starts
        .into_iter()
        .find(|(task_code, _)| task_code.as_str() == "task_1")
        .map(|(_, started_at)| started_at)
        .ok_or("the dependent must have started")?;
    assert!(
        dependent_started_at.duration_since(dependency_finished_at) < PROMPT_START_BOUND,
        "the dependent started {:?} after the result it waited for",
        dependent_started_at.duration_since(dependency_finished_at)
    );
    Ok(())
}

/// Deadline of the task below, which the first execution waits out.
const RELEASED_TASK_TIMEOUT: Duration = Duration::from_millis(300);

/// An execution released by its deadline leaves the task for the next free worker at once.
///
/// Checked by recording an execution released by its deadline as `JobProgress::NothingPickable`:
/// the takeover then starts ten seconds after the first start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_released_by_its_deadline_is_taken_over_without_waiting_for_the_poll_interval()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let moments = Arc::new(ExecutionMoments::default());
    let executor_moments = Arc::clone(&moments);
    let executor = task_fn(move |ctx| {
        let moments = Arc::clone(&executor_moments);
        async move {
            moments.record_start(ctx.code());
            if moments.starts().len() == 1 {
                ctx.cancel_token().cancelled().await;
                return Ok(TaskOutcome::Cancelled);
            }
            Ok(TaskOutcome::empty())
        }
    });
    let job_def = job_of_tasks("released_job", 1, false, RELEASED_TASK_TIMEOUT, &executor)?;
    let _env = start_pool(
        Arc::new(InMemoryStorage::new()),
        true,
        2,
        poll_schedule_config(LONG_POLL_INTERVAL, LONG_POLL_INTERVAL)?,
        vec![job_def],
    )?;

    let starts = moments.wait_for_starts(2).await?;

    let takeover_delay = starts[1].1.duration_since(starts[0].1);
    assert!(
        takeover_delay < RELEASED_TASK_TIMEOUT + PROMPT_START_BOUND,
        "the takeover started {takeover_delay:?} after the first execution"
    );
    Ok(())
}

/// Base interval of the tests below: shorter than `FAILED_TASK_RETRY_PAUSE`, so a retry that waited
/// for a poll alone cannot pass for one that waited out the pause.
const SHORT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The exception to starting work at once: a refused task is retried no sooner than
/// `FAILED_TASK_RETRY_PAUSE` after the refusal.
///
/// Checked by dropping the retry pause from `Task::check_availability_at`: the retry then starts
/// one poll interval after the refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_task_is_retried_no_sooner_than_its_retry_pause() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let moments = Arc::new(ExecutionMoments::default());
    let executor_moments = Arc::clone(&moments);
    let executor = task_fn(move |ctx| {
        let moments = Arc::clone(&executor_moments);
        async move {
            moments.record_start(ctx.code());
            if moments.starts().len() == 1 {
                moments.record_finish(ctx.code());
                return Err("refused once".into());
            }
            Ok(TaskOutcome::empty())
        }
    });
    let job_def = job_of_tasks("refused_job", 1, false, Duration::from_secs(30), &executor)?;
    let _env = start_pool(
        Arc::new(InMemoryStorage::new()),
        true,
        2,
        super::common::build_poll_schedule_config(SHORT_POLL_INTERVAL, Duration::ZERO),
        vec![job_def],
    )?;

    let starts = moments.wait_for_starts(2).await?;

    let refused_at = moments
        .finishes()
        .first()
        .map(|(_, refused_at)| *refused_at)
        .ok_or("the first attempt must have been refused")?;
    let retry_delay = starts[1].1.duration_since(refused_at);
    assert!(
        retry_delay >= FAILED_TASK_RETRY_PAUSE.to_std()?,
        "the retry started {retry_delay:?} after the refusal"
    );
    Ok(())
}

/// How many jobs the test below runs at once: each one is a fresh `HashMap` the pass picks from,
/// so a rule that still picked the refused task beside a pickable one would do so in some of them.
const SIBLING_JOB_COUNT: usize = 16;

/// Moments one job of the test below records: when its refusing task was refused, and when that
/// task started again.
#[derive(Default)]
struct RetryMoments {
    refused_at: Mutex<Option<Instant>>,
    retried_at: Mutex<Option<Instant>>,
}

/// The regression the retry pause exists for: a refusal beside another task that may be started at
/// once makes its job due at once, and the pass used to pick the refused task in `HashMap` order,
/// so a refusal was repeated with no pause at all. In every job `task_0` refuses its first attempt
/// and `task_1` completes; the retry of `task_0` must still wait for `FAILED_TASK_RETRY_PAUSE` from
/// the refusal. The pool of two workers also lands execution ends on a job another worker has claimed.
///
/// Checked by dropping the retry pause from `Task::check_availability_at`: in about a quarter of the
/// jobs the retry then starts right after the refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_task_beside_a_pickable_one_is_retried_no_sooner_than_its_retry_pause()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    for worker_count in [1, 2] {
        let mut job_defs = Vec::with_capacity(SIBLING_JOB_COUNT);
        let mut job_moments = Vec::with_capacity(SIBLING_JOB_COUNT);
        for job_index in 0..SIBLING_JOB_COUNT {
            let moments = Arc::new(RetryMoments::default());
            let executor_moments = Arc::clone(&moments);
            let executor = task_fn(move |ctx| {
                let moments = Arc::clone(&executor_moments);
                async move {
                    if ctx.code().as_str() != "task_0" {
                        return Ok(TaskOutcome::empty());
                    }
                    let started_at = Instant::now();
                    let refused_at = *moments.refused_at.lock().get_or_insert(started_at);
                    if refused_at == started_at {
                        return Err("refused once".into());
                    }
                    *moments.retried_at.lock() = Some(started_at);
                    Ok(TaskOutcome::empty())
                }
            });
            job_defs.push(
                job_of_tasks(
                    &format!("sibling_job_{job_index}"),
                    2,
                    false,
                    Duration::from_secs(30),
                    &executor,
                )?
                .with_max_iterations(1)?,
            );
            job_moments.push(moments);
        }
        let env = start_pool(
            Arc::new(InMemoryStorage::new()),
            true,
            worker_count,
            super::common::build_poll_schedule_config(SHORT_POLL_INTERVAL, Duration::ZERO),
            job_defs,
        )?;

        env.wait_for_all_jobs_completion(CONDITION_TIMEOUT).await?;

        for (job_index, moments) in job_moments.iter().enumerate() {
            let refused_at = (*moments.refused_at.lock()).ok_or("every job must have refused once")?;
            let retried_at = (*moments.retried_at.lock()).ok_or("every job must have retried")?;
            let retry_delay = retried_at.duration_since(refused_at);
            assert!(
                retry_delay >= FAILED_TASK_RETRY_PAUSE.to_std()?,
                "a pool of {worker_count} workers retried job {job_index} {retry_delay:?} after the refusal"
            );
        }
    }
    Ok(())
}

/// Deadline of both tasks of the job below: the one held elsewhere is released by it well before
/// the refused one is due again.
const TAKEOVER_TASK_TIMEOUT: Duration = Duration::from_millis(200);

/// Backoff ceiling of the pool below: under `FAILED_TASK_RETRY_PAUSE`, so the pause would hold the
/// job past it.
const CEILING_BELOW_RETRY_PAUSE: Duration = Duration::from_millis(100);

/// How soon after its deadline the task held elsewhere must be taken over: the ceiling, with room
/// for a loaded machine, and far below the 800 ms the rest of the retry pause would add.
const TAKEOVER_BOUND: Duration = Duration::from_millis(500);

/// Regression: a pass that found a failed task waiting out its retry pause scheduled the next poll
/// of the job at the retry, past the backoff ceiling, so a task of the job released meanwhile by its
/// deadline waited for the retry too. The store holds the job with one task refused and the other
/// started by a worker of no pool under test, at the same moment.
///
/// Checked by scheduling the poll at the retry whatever the ceiling: the takeover then starts about
/// 800 ms after the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_released_by_its_deadline_beside_a_retry_pause_is_taken_over_within_the_backoff_ceiling()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let moments = Arc::new(ExecutionMoments::default());
    let job_def = job_of_tasks(
        "takeover_beside_retry_job",
        2,
        false,
        TAKEOVER_TASK_TIMEOUT,
        &recording_executor(&moments),
    )?;
    let foreign_worker_id = Uuid::from_u128(7_102);
    let mut job = Job::new(&job_def, std::collections::HashMap::new(), foreign_worker_id)?;
    let TaskPickup::Ready(refused_task_id) = job.pick_task_to_execute(&foreign_worker_id)? else {
        return Err("the fixture must leave a task to refuse".into());
    };
    job.start_task(&refused_task_id, foreign_worker_id)?;
    job.fail_task(
        &refused_task_id,
        "refused",
        TaskRetry::WhileBudgetLasts,
        foreign_worker_id,
    )?;
    let TaskPickup::Ready(held_task_id) = job.pick_task_to_execute(&foreign_worker_id)? else {
        return Err("the fixture must leave a second task to hold".into());
    };
    job.start_task(&held_task_id, foreign_worker_id)?;
    let held_task_code = job.find_task(&held_task_id)?.code().clone();
    let deadline_passes_at = Instant::now() + TAKEOVER_TASK_TIMEOUT;
    let store = Arc::new(InMemoryStorage::new());
    store.save_job(&mut job, &CancellationToken::new()).await?;
    let _env = start_pool(
        store,
        true,
        1,
        poll_schedule_config(Duration::from_millis(50), CEILING_BELOW_RETRY_PAUSE)?,
        vec![job_def],
    )?;

    moments.wait_for_starts(1).await?;

    let (task_code, takeover_started_at) = moments.starts()[0].clone();
    assert_eq!(
        task_code, held_task_code,
        "the task held elsewhere must be started before the refused one is due again"
    );
    let takeover_delay = takeover_started_at.saturating_duration_since(deadline_passes_at);
    assert!(
        takeover_delay < TAKEOVER_BOUND,
        "the takeover started {takeover_delay:?} after the deadline"
    );
    Ok(())
}

/// The write that closed the last iteration says the job is done, so the pool never reads it
/// again. The one read is the one that found the store empty - a listing behind the cache, a read
/// of the job without it.
///
/// Checked by recording `JobProgress::IterationsEnded` like `NothingPickable`: the pass after
/// the iteration then reads the job a second time.
async fn assert_a_pool_that_closed_the_last_iteration_does_not_read_the_job_again(
    is_cached: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_def = job_of_tasks(
        "single_run_job",
        1,
        false,
        Duration::from_secs(30),
        &task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
    )?
    .with_max_iterations(1)?;
    let job_code = job_def.code().clone();
    let counting = Arc::new(CountingStorage::new(
        Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>
    ));
    let env = start_pool(
        Arc::clone(&counting) as Arc<dyn Storage>,
        is_cached,
        1,
        super::common::build_poll_schedule_config(Duration::from_millis(20), Duration::ZERO),
        vec![job_def],
    )?;

    env.wait_for_all_jobs_completion(CONDITION_TIMEOUT).await?;
    let reads = super::common::waiting::measure_settled_requests(
        || counting.read_calls_of(&job_code),
        "the pool has stopped reading the job",
    )
    .await?;

    assert_eq!(reads, 1, "only the read that found the store empty");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_pool_that_closed_the_last_iteration_does_not_read_the_job_again() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    assert_a_pool_that_closed_the_last_iteration_does_not_read_the_job_again(true).await
}

#[tokio::test(start_paused = true)]
async fn a_pool_that_closed_the_last_iteration_does_not_read_the_job_again_without_the_cache()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    assert_a_pool_that_closed_the_last_iteration_does_not_read_the_job_again(false).await
}

/// Requirement: a shutdown waits for an execution already in flight to end rather than aborting it
/// with the pool. The executor sees the cancellation, then waits for the test to let it finish.
///
/// Checked by leaving the loop of the pool on cancellation without waiting for what it started:
/// the execution is then aborted with the pool, before the test lets it finish.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shutdown_waits_for_the_execution_in_flight() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let has_started = Arc::new(AtomicBool::new(false));
    let has_seen_cancellation = Arc::new(AtomicBool::new(false));
    let has_finished = Arc::new(AtomicBool::new(false));
    let finish_permission = Arc::new(Notify::new());
    let executor = {
        let has_started = Arc::clone(&has_started);
        let has_seen_cancellation = Arc::clone(&has_seen_cancellation);
        let has_finished = Arc::clone(&has_finished);
        let finish_permission = Arc::clone(&finish_permission);
        task_fn(move |ctx| {
            let has_started = Arc::clone(&has_started);
            let has_seen_cancellation = Arc::clone(&has_seen_cancellation);
            let has_finished = Arc::clone(&has_finished);
            let finish_permission = Arc::clone(&finish_permission);
            async move {
                has_started.store(true, Ordering::SeqCst);
                while !ctx.is_cancelled() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                has_seen_cancellation.store(true, Ordering::SeqCst);
                finish_permission.notified().await;
                has_finished.store(true, Ordering::SeqCst);
                Ok(TaskOutcome::empty())
            }
        })
    };
    let job_def = job_of_tasks("in_flight_job", 1, false, Duration::from_secs(30), &executor)?;
    let mut env = start_pool(
        Arc::new(InMemoryStorage::new()),
        false,
        1,
        super::common::build_poll_schedule_config(Duration::from_millis(20), Duration::ZERO),
        vec![job_def],
    )?;
    // Stopped only once the execution is in flight: a pass started after the cancellation sees its
    // storage calls cancelled and starts no task, so the case would not be reached.
    wait_until(|| has_started.load(Ordering::SeqCst), "the execution must have started").await?;

    let stopping = tokio::spawn(async move { env.stop().await });
    wait_until(
        || has_seen_cancellation.load(Ordering::SeqCst),
        "the execution must have seen the shutdown",
    )
    .await?;
    finish_permission.notify_one();
    tokio::time::timeout(CONDITION_TIMEOUT, stopping).await??;

    assert!(
        has_finished.load(Ordering::SeqCst),
        "the shutdown must have waited for the execution to finish"
    );
    Ok(())
}

/// Requirement: a pass that panicked gives its job back to the schedule and its worker back to the
/// pool. A pool of one worker is what makes both observable: the job completes only if the same
/// worker is handed the same job again.
///
/// Checked twice: by leaving the claim of a panicked pass unreleased - the job is then never handed
/// out again - and by leaving its worker out of the idle ones - the pool then has no worker left.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pass_that_panicked_releases_its_job_and_keeps_its_worker() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let executor = task_fn(|_ctx| async { Ok(TaskOutcome::empty()) });
    let job_def =
        job_of_tasks("panicked_pass_job", 1, false, Duration::from_secs(30), &executor)?.with_max_iterations(1)?;
    let storage = Arc::new(FirstReadPanickingStorage::new(Arc::new(InMemoryStorage::new())));
    let env = start_pool(
        Arc::clone(&storage) as Arc<dyn Storage>,
        false,
        1,
        super::common::build_poll_schedule_config(Duration::from_millis(20), Duration::ZERO),
        vec![job_def],
    )?;

    let completion = env.wait_for_all_jobs_completion(CONDITION_TIMEOUT).await;

    assert!(storage.has_panicked(), "the fixture must have panicked a pass");
    completion?;
    Ok(())
}

/// Requirement: an execution that ended without writing its result polls its job again at the base
/// interval, and one that panicked is such an execution. While the first worker runs the task, the
/// second backs the job off from 20 ms, so when the task returns at 60 s the next poll is tens of
/// seconds away; the save of its result panics, and a poll in `(59 s, 61 s]` is the one the panic
/// brought forward. The tokio clock is paused, which is what makes the moments exact.
///
/// Checked by leaving the poll of a panicked execution as it was scheduled: the window then holds
/// no poll.
#[tokio::test(start_paused = true)]
async fn a_panicked_execution_polls_its_job_again_at_the_poll_interval() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let executor = task_fn(|_ctx| async {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(TaskOutcome::empty())
    });
    let job_def = job_of_tasks("panicked_execution_job", 1, false, HELD_TASK_TIMEOUT, &executor)?;
    let job_code = job_def.code().clone();
    let panicking = Arc::new(FirstResultSavePanickingStorage::new(Arc::new(InMemoryStorage::new())));
    let counting = Arc::new(CountingStorage::new(Arc::clone(&panicking) as Arc<dyn Storage>));
    let _env = start_pool(
        Arc::clone(&counting) as Arc<dyn Storage>,
        false,
        2,
        poll_schedule_config(Duration::from_millis(20), HELD_TASK_TIMEOUT)?,
        vec![job_def],
    )?;

    tokio::time::sleep(Duration::from_secs(59)).await;
    let reads_before = counting.read_calls_of(&job_code);
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        panicking.has_panicked(),
        "the fixture must have panicked the save of the result"
    );
    assert!(
        counting.read_calls_of(&job_code) > reads_before,
        "the panicked execution must have brought the poll of its job forward"
    );
    Ok(())
}

/// An executor that marks `has_started`, then holds its worker until the test cancels `gate`.
fn gated_executor(has_started: &Arc<AtomicBool>, gate: &CancellationToken) -> Arc<dyn TaskExecutor> {
    let has_started = Arc::clone(has_started);
    let gate = gate.clone();
    task_fn(move |_ctx| {
        let has_started = Arc::clone(&has_started);
        let gate = gate.clone();
        async move {
            has_started.store(true, Ordering::SeqCst);
            gate.cancelled().await;
            Ok(TaskOutcome::empty())
        }
    })
}

/// Requirement: while no worker is free nobody polls, and a due job waits for the first worker that
/// frees up. The only worker of the pool executes the task of one job while the other job is due for
/// ten virtual seconds; the other job is read only once the executing task is let finish. Each job
/// runs one iteration, so the pool goes idle afterwards and the paused clock can move on.
///
/// Checked by handing a worker whose task start wrote a start back to the idle ones as well: the
/// worker then reads the other job while it executes.
async fn assert_a_job_due_while_every_worker_executes_is_not_read_until_a_worker_is_free(
    is_cached: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let gate = CancellationToken::new();
    let first_started = Arc::new(AtomicBool::new(false));
    let second_started = Arc::new(AtomicBool::new(false));
    let first_job_def = job_of_tasks(
        "first_job",
        1,
        false,
        HELD_TASK_TIMEOUT,
        &gated_executor(&first_started, &gate),
    )?
    .with_max_iterations(1)?;
    let second_job_def = job_of_tasks(
        "second_job",
        1,
        false,
        HELD_TASK_TIMEOUT,
        &gated_executor(&second_started, &gate),
    )?
    .with_max_iterations(1)?;
    let first_code = first_job_def.code().clone();
    let second_code = second_job_def.code().clone();
    let counting = Arc::new(CountingStorage::new(
        Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>
    ));
    let env = start_pool(
        Arc::clone(&counting) as Arc<dyn Storage>,
        is_cached,
        1,
        poll_schedule_config(Duration::from_millis(200), Duration::from_secs(2))?,
        vec![first_job_def, second_job_def],
    )?;

    wait_until(
        || first_started.load(Ordering::SeqCst) || second_started.load(Ordering::SeqCst),
        "the task of one job has started",
    )
    .await?;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let waiting_code = if first_started.load(Ordering::SeqCst) {
        &second_code
    } else {
        &first_code
    };

    assert!(
        !(first_started.load(Ordering::SeqCst) && second_started.load(Ordering::SeqCst)),
        "one worker must not execute two tasks at once"
    );
    assert_eq!(
        counting.read_calls_of(waiting_code),
        0,
        "a job due while the only worker executes must not be read"
    );
    gate.cancel();
    env.wait_for_all_jobs_completion(CONDITION_TIMEOUT).await?;
    assert!(
        first_started.load(Ordering::SeqCst) && second_started.load(Ordering::SeqCst),
        "the waiting job must have been started by the worker that freed up"
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_job_due_while_every_worker_executes_is_not_read_until_a_worker_is_free()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    assert_a_job_due_while_every_worker_executes_is_not_read_until_a_worker_is_free(true).await
}

#[tokio::test(start_paused = true)]
async fn a_job_due_while_every_worker_executes_is_not_read_until_a_worker_is_free_without_the_cache()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    assert_a_job_due_while_every_worker_executes_is_not_read_until_a_worker_is_free(false).await
}

/// Interval of the jobs below: far beyond every wait of the tests, so no read they count is the
/// poll of the next iteration.
const DISTANT_ITERATION_INTERVAL: Duration = Duration::from_secs(3600);

/// The write that closed an iteration names when the next one is due, and that moment becomes the
/// poll of the job at once: no pass reads the closed iteration to find it out. The one read is the
/// one that found the store empty - a listing behind the cache, a read of the job without it.
///
/// Checked by recording an execution end carrying `NextIterationStatus::DueAt` as
/// `JobProgress::NothingPickable`: a pass then reads the closed iteration one base interval later.
async fn assert_a_pool_whose_write_closed_an_iteration_does_not_read_the_job_before_the_next_one_is_due(
    is_cached: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_def = job_of_tasks(
        "interval_job",
        1,
        false,
        Duration::from_secs(30),
        &task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
    )?
    .with_iteration_interval(DISTANT_ITERATION_INTERVAL)?;
    let job_code = job_def.code().clone();
    let store = Arc::new(InMemoryStorage::new());
    let counting = Arc::new(CountingStorage::new(Arc::clone(&store) as Arc<dyn Storage>));
    let _env = start_pool(
        Arc::clone(&counting) as Arc<dyn Storage>,
        is_cached,
        1,
        super::common::build_poll_schedule_config(Duration::from_millis(20), Duration::ZERO),
        vec![job_def],
    )?;

    wait_until_job_is_processed(store.as_ref(), &job_code, &CancellationToken::new()).await?;
    let reads = measure_settled_requests(
        || counting.read_calls_of(&job_code),
        "the pool has stopped reading the job",
    )
    .await?;

    assert_eq!(reads, 1, "only the read that found the store empty");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_pool_whose_write_closed_an_iteration_does_not_read_the_job_before_the_next_one_is_due()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    assert_a_pool_whose_write_closed_an_iteration_does_not_read_the_job_before_the_next_one_is_due(true).await
}

#[tokio::test(start_paused = true)]
async fn a_pool_whose_write_closed_an_iteration_does_not_read_the_job_before_the_next_one_is_due_without_the_cache()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    assert_a_pool_whose_write_closed_an_iteration_does_not_read_the_job_before_the_next_one_is_due(false).await
}

/// Stores the first iteration of `job_def` already finished by a worker of no pool under test - the
/// state a pool started after a restart, or beside another instance, reads first. Built through the
/// production API, so no state a legal run cannot reach is persisted.
async fn store_with_a_finished_iteration(
    job_def: &JobDefinition,
) -> Result<Arc<InMemoryStorage>, Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStorage::new());
    let foreign_worker_id = Uuid::from_u128(7_103);
    let mut job = Job::new(job_def, std::collections::HashMap::new(), foreign_worker_id)?;
    job.start_work(&foreign_worker_id)?;
    let TaskPickup::Ready(task_id) = job.pick_task_to_execute(&foreign_worker_id)? else {
        return Err("the fixture must leave a task to finish".into());
    };
    job.start_task(&task_id, foreign_worker_id)?;
    job.complete_task(&task_id, Vec::new(), foreign_worker_id)?;
    if !job.try_settle_iteration(&foreign_worker_id)? {
        return Err("the fixture must close the iteration".into());
    }
    store.save_job(&mut job, &CancellationToken::new()).await?;
    Ok(store)
}

/// A pass that reads an iteration already closed schedules the job by the status of the next one,
/// so the pool reads it once - one cold read, which is one call without the cache and a listing
/// plus a fetch behind it - and then not until the next iteration is due, or never.
async fn assert_a_pool_starting_over_a_closed_iteration_reads_it_once(
    job_def: JobDefinition,
    is_cached: bool,
    cold_read_calls: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_code = job_def.code().clone();
    let store = store_with_a_finished_iteration(&job_def).await?;
    let counting = Arc::new(CountingStorage::new(store as Arc<dyn Storage>));
    let _env = start_pool(
        Arc::clone(&counting) as Arc<dyn Storage>,
        is_cached,
        1,
        super::common::build_poll_schedule_config(Duration::from_millis(20), Duration::ZERO),
        vec![job_def],
    )?;

    wait_until(|| counting.read_calls_of(&job_code) >= 1, "the pool has read the job").await?;
    let reads = measure_settled_requests(
        || counting.read_calls_of(&job_code),
        "the pool has stopped reading the job",
    )
    .await?;

    assert_eq!(reads, cold_read_calls, "only the read that found the iteration closed");
    Ok(())
}

/// Checked by mapping a task start carrying `NextIterationStatus::BudgetSpent` to
/// `JobProgress::Stalled`: the pool then goes on polling the job at its backoff.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_starting_over_a_job_whose_budget_is_spent_reads_it_once() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let job_def = job_of_tasks(
        "spent_job",
        1,
        false,
        Duration::from_secs(30),
        &task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
    )?
    .with_max_iterations(1)?;
    assert_a_pool_starting_over_a_closed_iteration_reads_it_once(job_def, true, 2).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_starting_over_a_job_whose_budget_is_spent_reads_it_once_without_the_cache()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let job_def = job_of_tasks(
        "spent_job",
        1,
        false,
        Duration::from_secs(30),
        &task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
    )?
    .with_max_iterations(1)?;
    assert_a_pool_starting_over_a_closed_iteration_reads_it_once(job_def, false, 1).await
}

/// Checked twice, by mapping a task start carrying `NextIterationStatus::DueAt` to
/// `JobProgress::Stalled` - the pool then goes on polling the job at its backoff - and to
/// `JobProgress::NextIterationDueAt(Utc::now())` - the pool then polls it at once, again and again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_starting_over_an_iteration_whose_successor_is_not_due_reads_it_once()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let job_def = job_of_tasks(
        "waiting_job",
        1,
        false,
        Duration::from_secs(30),
        &task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
    )?
    .with_iteration_interval(DISTANT_ITERATION_INTERVAL)?;
    assert_a_pool_starting_over_a_closed_iteration_reads_it_once(job_def, true, 2).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_starting_over_an_iteration_whose_successor_is_not_due_reads_it_once_without_the_cache()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let job_def = job_of_tasks(
        "waiting_job",
        1,
        false,
        Duration::from_secs(30),
        &task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
    )?
    .with_iteration_interval(DISTANT_ITERATION_INTERVAL)?;
    assert_a_pool_starting_over_a_closed_iteration_reads_it_once(job_def, false, 1).await
}

/// The write that closed an iteration whose successor may start at once makes the job due at once,
/// so the next iteration does not wait for the poll interval.
///
/// Checked by recording an execution end carrying `NextIterationStatus::Ready` as
/// `JobProgress::NothingPickable`: the second iteration then starts ten seconds after the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_next_iteration_starts_right_after_the_write_that_closed_the_previous_one()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let moments = Arc::new(ExecutionMoments::default());
    let job_def = job_of_tasks(
        "repeated_job",
        1,
        false,
        Duration::from_secs(30),
        &recording_executor(&moments),
    )?
    .with_max_iterations(2)?;
    let _env = start_pool(
        Arc::new(InMemoryStorage::new()),
        true,
        1,
        poll_schedule_config(LONG_POLL_INTERVAL, LONG_POLL_INTERVAL)?,
        vec![job_def],
    )?;

    let starts = moments.wait_for_starts(2).await?;

    let first_finished_at = moments
        .finishes()
        .first()
        .map(|(_, finished_at)| *finished_at)
        .ok_or("the first iteration must have finished before the second started")?;
    let next_iteration_delay = starts[1].1.duration_since(first_finished_at);
    assert!(
        next_iteration_delay < PROMPT_START_BOUND,
        "the next iteration started {next_iteration_delay:?} after the previous one finished"
    );
    Ok(())
}

/// Requirement: the shutdown wakes a pool that sleeps until its next poll. The job's next iteration
/// is an hour away, so the pool sleeps until then; the tokio clock is paused, so a pool that ignores
/// the shutdown lets the timeout below fire first.
///
/// Checked by removing the cancellation branch from the `select!` of `WorkerPool::run`.
#[tokio::test(start_paused = true)]
async fn a_shutdown_stops_a_pool_sleeping_until_its_next_poll() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let job_def = job_of_tasks(
        "sleeping_job",
        1,
        false,
        Duration::from_secs(30),
        &task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
    )?
    .with_iteration_interval(DISTANT_ITERATION_INTERVAL)?;
    let job_code = job_def.code().clone();
    let store = Arc::new(InMemoryStorage::new());
    let mut env = start_pool(
        Arc::clone(&store) as Arc<dyn Storage>,
        false,
        1,
        super::common::build_poll_schedule_config(Duration::from_millis(20), Duration::ZERO),
        vec![job_def],
    )?;
    // Returns only after a sleep of its own, which the paused clock ends once the pool is asleep.
    wait_until_job_is_processed(store.as_ref(), &job_code, &CancellationToken::new()).await?;

    tokio::time::timeout(CONDITION_TIMEOUT, env.stop())
        .await
        .map_err(|_| "the shutdown must stop a pool sleeping until its next poll")?;
    Ok(())
}

/// Requirement: the shutdown wakes a pool that has nothing left to poll and no moment to wake at.
///
/// Checked by removing the cancellation branch from the `select!` of `WorkerPool::run`.
#[tokio::test(start_paused = true)]
async fn a_shutdown_stops_a_pool_whose_jobs_are_all_exhausted() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let job_def = job_of_tasks(
        "exhausted_job",
        1,
        false,
        Duration::from_secs(30),
        &task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
    )?
    .with_max_iterations(1)?;
    let mut env = start_pool(
        Arc::new(InMemoryStorage::new()),
        false,
        1,
        super::common::build_poll_schedule_config(Duration::from_millis(20), Duration::ZERO),
        vec![job_def],
    )?;
    env.wait_for_all_jobs_completion(CONDITION_TIMEOUT).await?;
    // The end of the iteration is reported before the pool collects the execution that wrote it; a
    // sleep on the paused clock ends only once the pool has collected it and fallen asleep.
    tokio::time::sleep(Duration::from_millis(10)).await;

    tokio::time::timeout(CONDITION_TIMEOUT, env.stop())
        .await
        .map_err(|_| "the shutdown must stop a pool whose jobs are all exhausted")?;
    Ok(())
}

/// Moments on the tokio clock at which the executors below started.
#[derive(Default)]
struct PausedClockStarts {
    started_at: Mutex<Vec<tokio::time::Instant>>,
}

impl PausedClockStarts {
    fn record_start(&self) {
        self.started_at.lock().push(tokio::time::Instant::now());
    }

    fn starts(&self) -> Vec<tokio::time::Instant> {
        self.started_at.lock().clone()
    }
}

/// An executor that records on the tokio clock when it starts.
fn paused_clock_recording_executor(starts: &Arc<PausedClockStarts>) -> Arc<dyn TaskExecutor> {
    let starts = Arc::clone(starts);
    task_fn(move |_ctx| {
        let starts = Arc::clone(&starts);
        async move {
            starts.record_start();
            Ok(TaskOutcome::empty())
        }
    })
}

/// Requirement: the workers of a pool start without a delay between them. The tokio clock is paused
/// and passes over `InMemoryStorage` set no timer, so a first task started without a delay starts
/// at the very moment the pool does. Each job runs one iteration, so the pool goes idle afterwards.
///
/// Checked by sleeping 50 ms times the worker's position before every task start.
#[tokio::test(start_paused = true)]
async fn every_job_starts_its_first_task_at_the_moment_the_pool_starts() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let starts = Arc::new(PausedClockStarts::default());
    let executor = paused_clock_recording_executor(&starts);
    let job_defs = vec![
        job_of_tasks("first_job", 1, false, Duration::from_secs(30), &executor)?.with_max_iterations(1)?,
        job_of_tasks("second_job", 1, false, Duration::from_secs(30), &executor)?.with_max_iterations(1)?,
    ];
    let pool_started_at = tokio::time::Instant::now();
    let _env = start_pool(
        Arc::new(InMemoryStorage::new()),
        true,
        2,
        poll_schedule_config(LONG_POLL_INTERVAL, LONG_POLL_INTERVAL)?,
        job_defs,
    )?;

    wait_until(|| starts.starts().len() >= 2, "both first tasks have started").await?;

    for started_at in starts.starts() {
        assert_eq!(
            started_at.duration_since(pool_started_at),
            Duration::ZERO,
            "a first task must start at the moment the pool does"
        );
    }
    Ok(())
}

/// Deadline and maximum lifetime of the task held elsewhere below, which the pool finds outlived.
const OUTLIVED_TASK_LIFETIME: Duration = Duration::from_millis(50);

/// Base interval of the pool below, which a next iteration due at once must not wait for longer than.
const NEXT_ITERATION_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// A pass that fails a task outlived elsewhere closes the iteration, and the next one may start at
/// once: it starts no later than one base interval after the pool, which is what such a pass cost
/// before the pool. The tokio clock is paused, so the moment is exact; the lifetime is judged by
/// the system clock, which the fixture waits out before the pool starts.
///
/// Checked by mapping a task start carrying `NextIterationStatus::Ready` to `JobProgress::Stalled`:
/// the next iteration then starts after a doubled base interval.
#[tokio::test(start_paused = true)]
async fn a_pass_that_closed_an_iteration_starts_the_next_one_within_the_poll_interval()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let starts = Arc::new(PausedClockStarts::default());
    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        JobCode::new("outlived_job"),
        vec![(
            TaskDefinition::new(TaskCode::new("outlived"), OUTLIVED_TASK_LIFETIME)
                .with_max_lifetime(OUTLIVED_TASK_LIFETIME),
            paused_clock_recording_executor(&starts),
        )],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(2)?;
    let job_code = job_def.code().clone();
    let store = store_with_a_task_held_elsewhere(&job_def).await?;
    let outlived_at = chrono::Utc::now() + chrono::Duration::from_std(OUTLIVED_TASK_LIFETIME * 2)?;
    let outlived_wait_deadline = Instant::now() + CONDITION_TIMEOUT;
    // The paused tokio clock does not move the system one, so only a real sleep lets it pass.
    while chrono::Utc::now() < outlived_at {
        if Instant::now() >= outlived_wait_deadline {
            return Err("the system clock must pass the lifetime of the held task".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let pool_started_at = tokio::time::Instant::now();
    let env = start_pool(
        Arc::clone(&store) as Arc<dyn Storage>,
        true,
        1,
        poll_schedule_config(NEXT_ITERATION_POLL_INTERVAL, NEXT_ITERATION_POLL_INTERVAL * 10)?,
        vec![job_def],
    )?;

    env.wait_for_all_jobs_completion(CONDITION_TIMEOUT).await?;

    let starts = starts.starts();
    assert_eq!(
        starts.len(),
        1,
        "only the task of the second iteration must have run here, the first one failed by its lifetime"
    );
    let stored = store.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(stored.iter_num(), 2, "the second iteration must be the one that ran");
    let next_iteration_delay = starts[0].duration_since(pool_started_at);
    assert!(
        next_iteration_delay <= NEXT_ITERATION_POLL_INTERVAL,
        "the next iteration started {next_iteration_delay:?} after the pool"
    );
    Ok(())
}
