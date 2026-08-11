use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::common::{
    manager_env::ManagerEnv,
    storage_wrapper::{CountingStorage, SiblingCompletingStorage},
};
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStatus, JobsManagerConfig, Storage, TaskCode,
    TaskDefinition, TaskLimits, TaskOutcome, TaskStatus, task_fn,
};

/// Bound on the only wait in this file: a job that never finishes fails the test with a diagnostic
/// instead of hanging the suite.
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(10);

/// The worker the storage double finishes a sibling task on behalf of. Any identity other than the
/// pool's own worker does, and a fixed one keeps the assertion on ownership literal.
const RIVAL_WORKER_ID: Uuid = Uuid::from_u128(9001);

/// `created_by_worker` names the creator of a task forever, so it cannot tell "created during this
/// execution" from "created earlier and since finished by someone else". A worker whose executor
/// planned a task must therefore not carry its own stale copy of that task back over the stored
/// state: the task would return to `Todo` and be executed a second time.
///
/// The contention is made by the storage double, not by a timing window: it lets a rival take and
/// finish the sibling right before the worker saves its own result, which is what refuses that save
/// and sends the worker into the merge under test.
#[tokio::test]
async fn a_task_completed_by_a_rival_is_not_resurrected_by_its_creator() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let executed_task_ids: Arc<DashMap<Uuid, u32>> = Arc::new(DashMap::new());

    let plan_executor = {
        let executed_task_ids = Arc::clone(&executed_task_ids);
        task_fn(move |ctx| {
            let executed_task_ids = Arc::clone(&executed_task_ids);
            let task_id = *ctx.id();

            async move {
                *executed_task_ids.entry(task_id).or_insert(0) += 1;

                ctx.job()
                    .add_task(TaskDefinition::new(TaskCode::new("shift"), Duration::from_mins(1)))?;
                ctx.job()
                    .add_task(TaskDefinition::new(TaskCode::new("shift"), Duration::from_mins(1)))?;

                Ok(TaskOutcome::Completed(b"planned".to_vec()))
            }
        })
    };

    let shift_executor = {
        let executed_task_ids = Arc::clone(&executed_task_ids);
        task_fn(move |ctx| {
            let executed_task_ids = Arc::clone(&executed_task_ids);
            let task_id = *ctx.id();

            async move {
                *executed_task_ids.entry(task_id).or_insert(0) += 1;
                Ok(TaskOutcome::Completed(b"shifted".to_vec()))
            }
        })
    };

    let job_code = JobCode::new("task_single_execution_job");
    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(
            TaskDefinition::new(TaskCode::new("plan"), Duration::from_mins(1)),
            plan_executor,
        )],
        vec![(TaskCode::new("shift"), shift_executor)],
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;

    let backend = Arc::new(InMemoryStorage::new());
    let double = Arc::new(SiblingCompletingStorage::new(
        Arc::clone(&backend) as Arc<dyn Storage>,
        RIVAL_WORKER_ID,
        TaskCode::new("shift"),
    ));
    // Outside the double, so it counts the worker's own saves and not the rival's write.
    let counting = Arc::new(CountingStorage::new(Arc::clone(&double) as Arc<dyn Storage>));

    // One worker: every write the fixture needs besides its own comes from the double, so no
    // outcome depends on which worker got somewhere first.
    let config = JobsManagerConfig {
        worker_count: 1,
        worker_config: super::common::build_worker_config(Duration::from_millis(10), Duration::ZERO),
        ..Default::default()
    };
    let mut manager_env = ManagerEnv::new(
        Arc::clone(&counting) as Arc<dyn Storage>,
        config,
        Arc::new(JobRegistry::new(vec![job_def.clone()])?),
        vec![job_def],
    )?;

    let completion = manager_env.wait_for_all_jobs_completion(COMPLETION_TIMEOUT).await;
    manager_env.stop().await;

    // Before the wait's own verdict: a fixture that could not set up the conflict would otherwise
    // surface only as a wait that ran out, with its cause left in the worker's log.
    assert_eq!(
        double.interference_failure(),
        None,
        "the double could not set up the conflict"
    );
    completion?;

    assert_eq!(
        double.interferences(),
        1,
        "the fixture must really have produced a conflict"
    );
    assert!(
        counting.put_attempts() > counting.put_successes(),
        "the refused save must have been merged and retried, got {} attempts and {} successes",
        counting.put_attempts(),
        counting.put_successes()
    );
    let sibling_id = double.completed_sibling_id().ok_or("the rival must have finished a sibling")?;

    assert!(
        !executed_task_ids.contains_key(&sibling_id),
        "the executor ran a task the rival had already finished"
    );
    assert_eq!(
        executed_task_ids.len(),
        2,
        "the planning task and one shift are the executor's whole share, got {:?}",
        executed_task_ids.iter().map(|entry| *entry.key()).collect::<Vec<_>>()
    );
    assert!(
        executed_task_ids.iter().all(|entry| *entry.value() == 1),
        "a task was executed more than once"
    );

    let stored = backend.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(*stored.status(), JobStatus::Completed);
    assert_eq!(
        stored.tasks_as_iter().count(),
        3,
        "the merge must leave the planning task and both shifts in the iteration"
    );
    let sibling = stored
        .tasks_as_iter()
        .find(|task| task.id() == &sibling_id)
        .ok_or("the sibling must still be in the stored iteration")?;
    assert_eq!(*sibling.status(), TaskStatus::Completed);
    assert_eq!(
        sibling.processing_by_worker(),
        Some(RIVAL_WORKER_ID),
        "the merge must leave the sibling with the worker that finished it"
    );

    // The other half of the merge: keeping the rival's task is worth nothing if the worker's own
    // result did not reach storage.
    let shifted_by_worker = stored
        .tasks_as_iter()
        .find(|task| task.code() == &TaskCode::new("shift") && task.id() != &sibling_id)
        .ok_or("the shift the worker executed must be in the stored iteration")?;
    assert_eq!(*shifted_by_worker.status(), TaskStatus::Completed);
    assert_eq!(shifted_by_worker.output(), b"shifted");
    assert!(
        matches!(shifted_by_worker.processing_by_worker(), Some(owner) if owner != RIVAL_WORKER_ID),
        "the worker's own task must stay with the worker that executed it, got {:?}",
        shifted_by_worker.processing_by_worker()
    );

    Ok(())
}
