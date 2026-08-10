use std::sync::Arc;

use async_trait::async_trait;

use crate::{TaskContext, TaskError};

/// What an executor did with its task.
///
/// The ordinary path is [`Self::Completed`]: the worker closes the task itself, so an executor
/// cannot leave one hanging by forgetting to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutcome {
    /// The task finished; the worker stores this payload as its output.
    Completed(Vec<u8>),
    /// Neither finished nor failed: the worker persists nothing and stops. Use it when a shutdown
    /// was observed mid-work; the task stays open and is picked up again later, which means the
    /// attempt it already spent is not returned.
    Cancelled,
    /// The executor already resolved the task through [`JobHandle`](crate::JobHandle) and the
    /// worker must not touch it. Needed by executors that fan out work and decide themselves when
    /// to close their own task.
    ///
    /// Resolving means completing or failing the task; returning this without having done either
    /// fails the task immediately rather than leaving it open until its deadline.
    Deferred,
}

impl TaskOutcome {
    /// Completion with no output payload.
    pub const fn empty() -> Self {
        Self::Completed(Vec::new())
    }
}

impl From<Vec<u8>> for TaskOutcome {
    fn from(payload: Vec<u8>) -> Self {
        Self::Completed(payload)
    }
}

impl From<()> for TaskOutcome {
    fn from((): ()) -> Self {
        Self::empty()
    }
}

/// Result an executor returns.
pub type TaskResult = std::result::Result<TaskOutcome, TaskError>;

/// Runs one task of a job.
///
/// Implement it on a struct holding the dependencies the task needs and register that struct as
/// `Arc<Self>`, which coerces to `Arc<dyn TaskExecutor>` on its own. For work that needs no state,
/// wrap an async closure with [`task_fn`].
#[async_trait]
pub trait TaskExecutor: Send + Sync + 'static {
    /// Executes the task described by `ctx`.
    ///
    /// Returning [`TaskOutcome::Completed`] is what closes the task; a returned error fails it and
    /// spends one attempt. A panic is caught by the worker and turned into a failure, but that is
    /// a safety net, not a contract.
    async fn execute(&self, ctx: TaskContext) -> TaskResult;
}

/// Wraps an async closure as a [`TaskExecutor`].
///
/// The trait has no blanket implementation for closures: one for `Arc<T>` and one for `Fn` cannot
/// coexist under the coherence rules, and `Arc<Concrete>` already coerces to `Arc<dyn TaskExecutor>`
/// on its own. This function is the single entry point for the closure case.
pub fn task_fn<F, Fut>(f: F) -> Arc<dyn TaskExecutor>
where
    F: Fn(TaskContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = TaskResult> + Send + 'static,
{
    struct ClosureExecutor<F>(F);

    #[async_trait]
    impl<F, Fut> TaskExecutor for ClosureExecutor<F>
    where
        F: Fn(TaskContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = TaskResult> + Send + 'static,
    {
        async fn execute(&self, ctx: TaskContext) -> TaskResult {
            (self.0)(ctx).await
        }
    }

    Arc::new(ClosureExecutor(f))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use parking_lot::RwLock;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::*;
    use crate::execution::job_handle::{JobHandleImpl, JobHandleState};
    use crate::{Job, JobCode, JobDefinition, JobDefinitionId, JobHandle, TaskCode, TaskDefinition, TaskLimits};

    struct StructExecutor;

    #[async_trait]
    impl TaskExecutor for StructExecutor {
        async fn execute(&self, _ctx: TaskContext) -> TaskResult {
            Ok(TaskOutcome::Completed(b"from struct".to_vec()))
        }
    }

    fn test_context() -> TaskContext {
        let task_def =
            TaskDefinition::new(TaskCode::new("task"), std::time::Duration::from_secs(5)).with_input(b"input".to_vec());
        let job_def = JobDefinition::new(
            JobDefinitionId::new(),
            JobCode::new("job"),
            vec![(task_def, Arc::new(StructExecutor) as Arc<dyn TaskExecutor>)],
            Vec::new(),
            Vec::new(),
            TaskLimits::default(),
        )
        .unwrap();
        let job = Job::new(&job_def, HashMap::new(), Uuid::from_u128(1)).unwrap();
        let task = job.get_tasks_by_code(&TaskCode::new("task")).remove(0);
        let job_handle = Arc::new(JobHandleImpl::new(
            Arc::new(RwLock::new(JobHandleState::new(job))),
            Uuid::from_u128(2),
        ));

        TaskContext::new(task, job_handle as Arc<dyn JobHandle>, CancellationToken::new())
    }

    /// A struct executor is registered as a trait object without a wrapper, and the trait object
    /// really dispatches to it.
    #[tokio::test]
    async fn struct_executor_runs_through_the_trait_object() {
        let executor: Arc<dyn TaskExecutor> = Arc::new(StructExecutor);

        let outcome = executor.execute(test_context()).await.unwrap();

        assert_eq!(outcome, TaskOutcome::Completed(b"from struct".to_vec()));
    }

    /// The second registration path: an async closure, which reads the context it was handed.
    #[tokio::test]
    async fn closure_executor_runs_through_the_trait_object() {
        let executor = task_fn(|ctx| async move { Ok(TaskOutcome::Completed(ctx.input().to_vec())) });

        let outcome = executor.execute(test_context()).await.unwrap();

        assert_eq!(outcome, TaskOutcome::Completed(b"input".to_vec()));
    }

    #[test]
    fn outcome_converts_from_payload_and_unit() {
        assert_eq!(TaskOutcome::from(b"x".to_vec()), TaskOutcome::Completed(b"x".to_vec()));
        assert_eq!(TaskOutcome::from(()), TaskOutcome::empty());
    }
}
