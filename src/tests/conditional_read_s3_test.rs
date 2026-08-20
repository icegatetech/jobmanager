use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::common::meta_of;
use super::common::s3_container::S3TestContainer;
use crate::{
    Job, JobCode, JobDefinition, JobDefinitionId, JobStateCodecKind, JobStatus, NoopMetrics, S3Storage,
    S3StorageConfig, Storage, StorageError, TaskCode, TaskDefinition, TaskLimits, TaskOutcome, task_fn,
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

/// The codec is a parameter of every case below: a conditional read builds its own object key and
/// decodes its own body, and both carry the codec - a key with a fixed extension finds nothing under
/// `Cbor`, and a body decoded by a fixed codec is not the state that was written.
async fn s3_storage_with_saved_job(
    container: &S3TestContainer,
    job_code: &JobCode,
    codec: JobStateCodecKind,
) -> Result<(S3Storage, Job), Box<dyn std::error::Error>> {
    let job_def = job_definition(job_code)?;
    let config = S3StorageConfig::new(
        container.endpoint(),
        container.username(),
        container.password(),
        "conditional-read",
        "us-east-1",
    )
    .with_job_state_codec(codec);
    let storage = S3Storage::new(
        config,
        Arc::new(SingleJobRegistry {
            job_def: job_def.clone(),
        }),
        Arc::new(NoopMetrics),
    )
    .await?;

    let mut job = Job::new(&job_def, HashMap::new(), Uuid::from_u128(1))?;
    storage.save_job(&mut job, &CancellationToken::new()).await?;

    Ok((storage, job))
}

/// The saving the whole change is for: a state that did not move must come back carrying nothing,
/// which means the store really answered `304` rather than sending the object again. Mocks cannot
/// show this - only a real store can.
#[tokio::test]
async fn an_unmoved_iteration_reads_as_unchanged_from_the_store_json() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    run_unmoved_iteration_read(JobStateCodecKind::Json).await
}

#[tokio::test]
async fn an_unmoved_iteration_reads_as_unchanged_from_the_store_cbor() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    run_unmoved_iteration_read(JobStateCodecKind::Cbor).await
}

async fn run_unmoved_iteration_read(codec: JobStateCodecKind) -> Result<(), Box<dyn std::error::Error>> {
    let container = S3TestContainer::start().await?;
    let (storage, job) = s3_storage_with_saved_job(&container, &JobCode::new("unmoved_job"), codec).await?;

    let read = storage.get_changed_job(&meta_of(&job), &CancellationToken::new()).await?;

    assert!(read.is_none(), "got: {:?}", read.as_ref().map(Job::version));
    Ok(())
}

#[tokio::test]
async fn a_moved_iteration_reads_as_changed_from_the_store_json() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    run_moved_iteration_read(JobStateCodecKind::Json).await
}

#[tokio::test]
async fn a_moved_iteration_reads_as_changed_from_the_store_cbor() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    run_moved_iteration_read(JobStateCodecKind::Cbor).await
}

async fn run_moved_iteration_read(codec: JobStateCodecKind) -> Result<(), Box<dyn std::error::Error>> {
    let container = S3TestContainer::start().await?;
    let job_code = JobCode::new("moved_job");
    let (storage, mut job) = s3_storage_with_saved_job(&container, &job_code, codec).await?;
    let stale_meta = meta_of(&job);
    job.work(&Uuid::from_u128(1))?;
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
#[tokio::test]
async fn an_unwritten_iteration_reads_as_not_found_json() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    run_unwritten_iteration_read(JobStateCodecKind::Json).await
}

#[tokio::test]
async fn an_unwritten_iteration_reads_as_not_found_cbor() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    run_unwritten_iteration_read(JobStateCodecKind::Cbor).await
}

async fn run_unwritten_iteration_read(codec: JobStateCodecKind) -> Result<(), Box<dyn std::error::Error>> {
    let container = S3TestContainer::start().await?;
    let (storage, job) = s3_storage_with_saved_job(&container, &JobCode::new("unwritten_job"), codec).await?;
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
