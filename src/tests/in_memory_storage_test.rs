use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::common::{
    manager_env::ManagerEnv,
    storage_wrapper::{ContendingStorage, CountingStorage},
};
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    Job, JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStatus, JobsManagerConfig, Storage, TaskCode,
    TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

/// Bound on the only wait in this file: a job that never finishes fails the test with a diagnostic
/// instead of hanging the suite.
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(10);

fn job_definition(job_code: &JobCode) -> JobDefinition {
    let task_def = TaskDefinition::new(TaskCode::new("task"), Duration::from_secs(5));
    let executor = task_fn(|_ctx| async { Ok(TaskOutcome::Completed(b"done".to_vec())) });

    JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(task_def, executor)],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )
    .expect("the test description must be legal")
    .with_max_iterations(1)
    .expect("one iteration must be accepted")
}

/// A job in the state a worker holds after finishing the iteration's only task, which is what makes
/// the next iteration startable.
fn completed_job(job_def: &JobDefinition, worker_id: Uuid) -> Job {
    let mut job = Job::new(job_def, HashMap::new(), worker_id).expect("the description must produce a job");
    let task_id = *job
        .get_tasks_by_code(&TaskCode::new("task"))
        .first()
        .expect("the description declares one task")
        .id();

    job.work(&worker_id).expect("a new job must accept work");
    job.start_task(&task_id, worker_id).expect("a todo task must start");
    job.complete_task(&task_id, b"done".to_vec())
        .expect("a started task must complete");
    job.try_to_complete(&worker_id).expect("a finished job must complete");
    job
}

/// Two workers that read the same version both write; without the version check the later one wins
/// silently and the earlier worker's result is gone with no error anywhere.
#[tokio::test]
async fn save_job_refuses_a_write_from_a_version_that_moved() -> Result<(), Box<dyn std::error::Error>> {
    let storage = InMemoryStorage::new();
    let cancel_token = CancellationToken::new();
    let job_code = JobCode::new("versioned_job");
    let job_def = job_definition(&job_code);

    let mut job = Job::new(&job_def, HashMap::new(), Uuid::from_u128(1))?;
    storage.save_job(&mut job, &cancel_token).await?;

    let mut winner = job.clone();
    winner.work(&Uuid::from_u128(1))?;
    storage.save_job(&mut winner, &cancel_token).await?;

    let mut loser = job;
    let error = storage
        .save_job(&mut loser, &cancel_token)
        .await
        .expect_err("a write from a version that moved must be refused");

    assert!(error.is_conflict(), "got: {error}");
    assert!(
        !error.is_retryable(),
        "a conflict is resolved by merging, not by retrying"
    );
    let stored = storage.get_job(&job_code, &cancel_token).await?;
    assert_eq!(*stored.status(), JobStatus::Running, "the winner's state must stand");
    assert_eq!(stored.version(), winner.version());
    Ok(())
}

/// Only one of the workers starting an iteration may create it; the loser has to find it already
/// there rather than replace it.
#[tokio::test]
async fn save_job_lets_one_creator_of_an_iteration_win() -> Result<(), Box<dyn std::error::Error>> {
    let storage = InMemoryStorage::new();
    let cancel_token = CancellationToken::new();
    let job_code = JobCode::new("iterating_job");
    // The cap belongs to the job's own definition; the next iteration is prepared here directly, so
    // a description without one keeps the fixture legal.
    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(
            TaskDefinition::new(TaskCode::new("task"), Duration::from_secs(5)),
            task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
        )],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?;

    let mut job = completed_job(&job_def, Uuid::from_u128(1));
    storage.save_job(&mut job, &cancel_token).await?;
    job.next_iteration(&job_def, Uuid::from_u128(1))?;
    assert_eq!(job.iter_num(), 2, "the fixture must have moved to the next iteration");

    let mut winner = job.clone();
    storage.save_job(&mut winner, &cancel_token).await?;

    let error = storage
        .save_job(&mut job, &cancel_token)
        .await
        .expect_err("a second creation of one iteration must be refused");

    assert!(error.is_conflict(), "got: {error}");
    let stored = storage.get_job(&job_code, &cancel_token).await?;
    assert_eq!(stored.version(), winner.version());
    Ok(())
}

/// The backend stands in for a store that holds every job; saving one must not make another
/// disappear, or its workers would keep recreating it from scratch.
#[tokio::test]
async fn save_job_keeps_the_jobs_of_different_codes_apart() -> Result<(), Box<dyn std::error::Error>> {
    let storage = InMemoryStorage::new();
    let cancel_token = CancellationToken::new();
    let first_code = JobCode::new("first_job");
    let second_code = JobCode::new("second_job");

    let mut first = Job::new(&job_definition(&first_code), HashMap::new(), Uuid::from_u128(1))?;
    let mut second = Job::new(&job_definition(&second_code), HashMap::new(), Uuid::from_u128(2))?;
    storage.save_job(&mut first, &cancel_token).await?;
    storage.save_job(&mut second, &cancel_token).await?;

    let stored_first = storage.get_job(&first_code, &cancel_token).await?;
    let stored_second = storage.get_job(&second_code, &cancel_token).await?;
    assert_eq!(stored_first.id(), first.id());
    assert_eq!(stored_second.id(), second.id());

    // A version belongs to a job, so the second job's write must not make the first one's stale.
    first.work(&Uuid::from_u128(1))?;
    storage.save_job(&mut first, &cancel_token).await?;
    assert_eq!(
        *storage.get_job(&first_code, &cancel_token).await?.status(),
        JobStatus::Running
    );
    assert_eq!(
        *storage.get_job(&second_code, &cancel_token).await?.status(),
        JobStatus::Started,
        "the other job must be untouched"
    );
    Ok(())
}

/// The whole arc a conflict goes through: the worker's save is refused, it re-reads the stored
/// state, merges its own task into it and saves again - and the result it carried is in the state
/// that ends up stored.
#[tokio::test]
async fn a_worker_merges_and_retries_its_save_after_a_conflict() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("contended_job");
    let job_def = job_definition(&job_code);
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);

    let backend = Arc::new(InMemoryStorage::new());
    let contending = Arc::new(ContendingStorage::new(Arc::clone(&backend) as Arc<dyn Storage>));
    let counting = Arc::new(CountingStorage::new(Arc::clone(&contending) as Arc<dyn Storage>));

    let config = JobsManagerConfig {
        worker_count: 1,
        worker_config: super::common::build_worker_config(Duration::from_millis(10), Duration::ZERO),
        ..Default::default()
    };
    let mut manager_env = ManagerEnv::new(
        Arc::clone(&counting) as Arc<dyn Storage>,
        config,
        Arc::clone(&job_registry),
        vec![job_def],
    )?;

    manager_env.wait_for_all_jobs_completion(COMPLETION_TIMEOUT).await?;
    manager_env.stop().await;

    assert_eq!(
        contending.interferences(),
        1,
        "the fixture must really have produced a conflict"
    );
    assert!(
        counting.put_attempts() > counting.put_successes(),
        "a refused save must have been retried, got {} attempts and {} successes",
        counting.put_attempts(),
        counting.put_successes()
    );

    let stored = backend.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(*stored.status(), JobStatus::Completed);
    let outputs: Vec<&[u8]> = stored.tasks_as_iter().map(crate::ImmutableTask::get_output).collect();
    assert_eq!(
        outputs,
        vec![b"done".as_slice()],
        "the merge must carry the worker's own result into the stored state"
    );
    Ok(())
}
