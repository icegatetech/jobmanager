use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::common::meta_of;
use super::common::provider_harness::{ProviderHarness, ProviderStorageRequest};
use crate::{
    Job, JobCode, JobDefinition, JobDefinitionId, JobStateCodecKind, JobStatus, NoopMetrics, Storage, StorageError,
    TaskCode, TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

/// Registry the backend rebuilds job settings from; the description carries one task so that the
/// state under test is a legal one.
struct SingleJobRegistry {
    job_def: JobDefinition,
}

impl crate::JobDefinitionRegistry for SingleJobRegistry {
    fn get_job(&self, _code: &JobCode) -> Result<JobDefinition, crate::Error> {
        Ok(self.job_def.clone())
    }
}

fn job_definition(job_code: &JobCode) -> Result<JobDefinition, crate::Error> {
    JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(
            TaskDefinition::new(TaskCode::from("read"), Duration::from_secs(5)),
            task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
        )],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )
}

/// `run_moved_iteration_read` is the one body below run under both codecs, because it is the one
/// that writes a state and reads that state back: a key with a fixed extension finds nothing under
/// `Cbor`, and a body decoded by a fixed codec is not the state that was written. The other bodies
/// take the codec because they share this builder, and are run under `Json` alone.
async fn build_storage(
    harness: &dyn ProviderHarness,
    job_def: &JobDefinition,
    codec: JobStateCodecKind,
) -> Result<Arc<dyn Storage>, Box<dyn std::error::Error>> {
    harness
        .build_storage(&ProviderStorageRequest::new(
            "conditional-read",
            codec,
            Arc::new(SingleJobRegistry {
                job_def: job_def.clone(),
            }),
            Arc::new(NoopMetrics),
        ))
        .await
}

async fn build_storage_with_saved_job(
    harness: &dyn ProviderHarness,
    job_code: &JobCode,
    codec: JobStateCodecKind,
) -> Result<(Arc<dyn Storage>, Job), Box<dyn std::error::Error>> {
    let job_def = job_definition(job_code)?;
    let storage = build_storage(harness, &job_def, codec).await?;

    let mut job = Job::new(&job_def, HashMap::new(), Uuid::from_u128(1))?;
    storage.save_job(&mut job, &CancellationToken::new()).await?;

    Ok((storage, job))
}

/// The saving the whole change is for: a state that did not move must come back carrying nothing,
/// which means the store really answered `304` rather than sending the object again. Mocks cannot
/// show this - only a real store can.
async fn run_unmoved_iteration_read(
    harness: &dyn ProviderHarness,
    codec: JobStateCodecKind,
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, job) = build_storage_with_saved_job(harness, &JobCode::new("unmoved_job"), codec).await?;

    let read = storage.get_changed_job(&meta_of(&job), &CancellationToken::new()).await?;

    assert!(read.is_none(), "got: {:?}", read.as_ref().map(Job::version));
    Ok(())
}

async fn run_moved_iteration_read(
    harness: &dyn ProviderHarness,
    codec: JobStateCodecKind,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_code = JobCode::new("moved_job");
    let (storage, mut job) = build_storage_with_saved_job(harness, &job_code, codec).await?;
    let stale_meta = meta_of(&job);
    job.start_work(&Uuid::from_u128(1))?;
    storage.save_job(&mut job, &CancellationToken::new()).await?;

    let read = storage.get_changed_job(&stale_meta, &CancellationToken::new()).await?;

    let Some(changed) = read else {
        panic!("a moved iteration must read as changed")
    };
    assert_eq!(changed.version(), job.version());
    assert_eq!(*changed.status(), JobStatus::Running);
    Ok(())
}

/// The state object of an iteration that was never written is not there, and the caller has to be
/// able to tell that from "did not move" - it is what sends it back to a cold read.
async fn run_unwritten_iteration_read(
    harness: &dyn ProviderHarness,
    codec: JobStateCodecKind,
) -> Result<(), Box<dyn std::error::Error>> {
    let (storage, job) = build_storage_with_saved_job(harness, &JobCode::new("unwritten_job"), codec).await?;
    let mut meta = meta_of(&job);
    meta.iter_num += 1;

    let error = storage
        .get_changed_job(&meta, &CancellationToken::new())
        .await
        .err()
        .expect("an iteration nobody wrote must not be readable");

    assert!(matches!(error, StorageError::NotFound(_)), "got: {error}");
    Ok(())
}

/// The whole of the coordination this crate rests on: a job's first iteration is created only if
/// nobody created it first, and the worker that lost hears a conflict rather than overwriting the
/// winner. Asserted against a real store because a condition the provider ignores looks exactly
/// like a condition it honours, until two workers meet.
async fn run_second_creation_of_an_iteration(
    harness: &dyn ProviderHarness,
    codec: JobStateCodecKind,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_code = JobCode::new("created_once_job");
    let job_def = job_definition(&job_code)?;
    let (storage, stored) = build_storage_with_saved_job(harness, &job_code, codec).await?;
    let mut rival = Job::new(&job_def, HashMap::new(), Uuid::from_u128(2))?;

    let error = storage
        .save_job(&mut rival, &CancellationToken::new())
        .await
        .expect_err("a second creation of the same iteration must not overwrite the first");

    assert!(error.is_conflict(), "got: {error}");
    assert_eq!(
        storage.get_job(&job_code, &CancellationToken::new()).await?.version(),
        stored.version(),
        "the stored state must be the one the winner wrote"
    );
    Ok(())
}

/// A save conditioned on a version the store has moved past is refused the same way, which is what
/// sends a worker back to re-read and merge instead of dropping the update that got there first.
async fn run_save_of_a_stale_version(
    harness: &dyn ProviderHarness,
    codec: JobStateCodecKind,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_code = JobCode::new("stale_version_job");
    let (storage, job) = build_storage_with_saved_job(harness, &job_code, codec).await?;
    let mut winner = job.clone();
    winner.start_work(&Uuid::from_u128(1))?;
    storage.save_job(&mut winner, &CancellationToken::new()).await?;
    let mut loser = job;
    loser.start_work(&Uuid::from_u128(2))?;

    let error = storage
        .save_job(&mut loser, &CancellationToken::new())
        .await
        .expect_err("a save against a version the store moved past must be refused");

    assert!(error.is_conflict(), "got: {error}");
    Ok(())
}

#[cfg(feature = "storage-s3")]
mod on_s3 {
    use super::{
        JobStateCodecKind, run_moved_iteration_read, run_save_of_a_stale_version, run_second_creation_of_an_iteration,
        run_unmoved_iteration_read, run_unwritten_iteration_read,
    };
    use crate::tests::common::provider_harness::S3ProviderHarness;

    #[tokio::test]
    async fn an_unmoved_iteration_reads_as_unchanged_from_the_store_json_on_s3()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_unmoved_iteration_read(&S3ProviderHarness::start().await?, JobStateCodecKind::Json).await
    }

    #[tokio::test]
    async fn a_moved_iteration_reads_as_changed_from_the_store_json_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_moved_iteration_read(&S3ProviderHarness::start().await?, JobStateCodecKind::Json).await
    }

    #[tokio::test]
    async fn a_moved_iteration_reads_as_changed_from_the_store_cbor_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_moved_iteration_read(&S3ProviderHarness::start().await?, JobStateCodecKind::Cbor).await
    }

    #[tokio::test]
    async fn an_unwritten_iteration_reads_as_not_found_json_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_unwritten_iteration_read(&S3ProviderHarness::start().await?, JobStateCodecKind::Json).await
    }

    #[tokio::test]
    async fn a_second_creation_of_an_iteration_is_refused_json_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_second_creation_of_an_iteration(&S3ProviderHarness::start().await?, JobStateCodecKind::Json).await
    }

    #[tokio::test]
    async fn a_save_of_a_stale_version_is_refused_json_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_save_of_a_stale_version(&S3ProviderHarness::start().await?, JobStateCodecKind::Json).await
    }
}

#[cfg(feature = "storage-azure")]
mod on_azure {
    use super::{
        JobStateCodecKind, run_moved_iteration_read, run_save_of_a_stale_version, run_second_creation_of_an_iteration,
        run_unmoved_iteration_read, run_unwritten_iteration_read,
    };
    use crate::tests::common::provider_harness::AzureProviderHarness;

    #[tokio::test]
    async fn an_unmoved_iteration_reads_as_unchanged_from_the_store_json_on_azure()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_unmoved_iteration_read(&AzureProviderHarness::start().await?, JobStateCodecKind::Json).await
    }

    #[tokio::test]
    async fn a_moved_iteration_reads_as_changed_from_the_store_json_on_azure() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        run_moved_iteration_read(&AzureProviderHarness::start().await?, JobStateCodecKind::Json).await
    }

    #[tokio::test]
    async fn a_moved_iteration_reads_as_changed_from_the_store_cbor_on_azure() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        run_moved_iteration_read(&AzureProviderHarness::start().await?, JobStateCodecKind::Cbor).await
    }

    #[tokio::test]
    async fn an_unwritten_iteration_reads_as_not_found_json_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_unwritten_iteration_read(&AzureProviderHarness::start().await?, JobStateCodecKind::Json).await
    }

    #[tokio::test]
    async fn a_second_creation_of_an_iteration_is_refused_json_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_second_creation_of_an_iteration(&AzureProviderHarness::start().await?, JobStateCodecKind::Json).await
    }

    #[tokio::test]
    async fn a_save_of_a_stale_version_is_refused_json_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_save_of_a_stale_version(&AzureProviderHarness::start().await?, JobStateCodecKind::Json).await
    }
}

#[cfg(feature = "storage-gcs")]
mod on_gcs {
    use super::{
        JobStateCodecKind, run_moved_iteration_read, run_save_of_a_stale_version, run_second_creation_of_an_iteration,
        run_unmoved_iteration_read, run_unwritten_iteration_read,
    };
    use crate::tests::common::provider_harness::GcsProviderHarness;

    #[tokio::test]
    async fn an_unmoved_iteration_reads_as_unchanged_from_the_store_json_on_gcs()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_unmoved_iteration_read(&GcsProviderHarness::start().await?, JobStateCodecKind::Json).await
    }

    #[tokio::test]
    async fn a_moved_iteration_reads_as_changed_from_the_store_json_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_moved_iteration_read(&GcsProviderHarness::start().await?, JobStateCodecKind::Json).await
    }

    #[tokio::test]
    async fn a_moved_iteration_reads_as_changed_from_the_store_cbor_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_moved_iteration_read(&GcsProviderHarness::start().await?, JobStateCodecKind::Cbor).await
    }

    #[tokio::test]
    async fn an_unwritten_iteration_reads_as_not_found_json_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_unwritten_iteration_read(&GcsProviderHarness::start().await?, JobStateCodecKind::Json).await
    }

    #[tokio::test]
    async fn a_second_creation_of_an_iteration_is_refused_json_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_second_creation_of_an_iteration(&GcsProviderHarness::start().await?, JobStateCodecKind::Json).await
    }

    #[tokio::test]
    async fn a_save_of_a_stale_version_is_refused_json_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_save_of_a_stale_version(&GcsProviderHarness::start().await?, JobStateCodecKind::Json).await
    }
}
