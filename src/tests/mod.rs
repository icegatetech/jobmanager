// Integration tests module
// These tests are kept in src/ rather than tests/ to access pub(crate) types like Job

mod common;

mod builder_test;
mod cache_invalidation_test;
mod concurrent_workers_test;
mod deadline_expiry_test;
mod dynamic_task_test;
mod escaped_job_handle_test;
mod in_memory_storage_test;
mod job_cleanup_s3_test;
mod job_cleanup_test;
mod job_handle_read_test;
mod job_iterations_test;
mod metrics_sink_test;
mod shutdown_test;
mod simple_job_test;
mod task_attempt_limit_test;
mod task_dependencies_test;
mod task_failure_test;
mod task_outcome_test;
mod two_jobs_test;
mod wait_for_iteration_completion_test;
