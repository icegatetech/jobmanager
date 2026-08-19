// Integration tests module
// These tests are kept in src/ rather than tests/ to access pub(crate) types like Job

// `pub` so the inline test modules of the crate reach the same harness the integration tests use:
// a second counter or a second storage double, written next to the code under test, would be a
// parallel mechanism for a job this one already does.
pub mod common;

mod builder_test;
mod cache_invalidation_test;
mod concurrent_workers_test;
mod conditional_read_s3_test;
mod deadline_expiry_test;
mod dynamic_task_test;
mod escaped_job_handle_test;
mod in_memory_storage_test;
mod job_cleanup_s3_test;
mod job_cleanup_test;
mod job_handle_read_test;
mod job_iterations_test;
mod metrics_sink_test;
mod poll_scheduling_test;
mod request_quota_test;
mod shutdown_test;
mod simple_job_test;
mod task_attempt_limit_test;
mod task_deadline_cancel_test;
mod task_dependencies_test;
mod task_failure_test;
mod task_lifetime_persistence_test;
mod task_lifetime_test;
mod task_outcome_test;
mod task_rollback_test;
mod task_single_execution_test;
mod two_jobs_test;
mod wait_for_iteration_completion_test;
