//! Whether a backend creates the container it was pointed at, and what it does when it may not.
//!
//! Written per backend rather than through a shared body: creating the container is what a backend
//! does *before* it can be handed out, so a test of that decision has to build the backend itself -
//! which is exactly what [`ProviderHarness`](super::common::provider_harness::ProviderHarness) has
//! already done by the time it answers.

/// Container neither the image nor the test creates, so what the backend does with a missing one is
/// what the assertions see.
const ABSENT_CONTAINER_NAME: &str = "container-creation-absent";

/// Container the test itself creates through its probe, so what the backend does with one that is
/// already there is what the assertions see. Named apart from [`ABSENT_CONTAINER_NAME`] because the
/// state under test is the opposite one.
const PRESENT_CONTAINER_NAME: &str = "container-creation-present";

#[cfg(feature = "storage-s3")]
mod on_s3 {
    use std::sync::Arc;

    use super::{ABSENT_CONTAINER_NAME, PRESENT_CONTAINER_NAME};
    use crate::tests::common::UnusedJobRegistry;
    use crate::tests::common::s3_container::S3TestContainer;
    use crate::{NoopMetrics, S3Backend, S3Config};

    /// S3 client of the test itself, so what the backend did is read without going through it.
    async fn build_bucket_probe(store: &S3TestContainer) -> aws_sdk_s3::Client {
        let credentials =
            aws_sdk_s3::config::Credentials::new(store.username(), store.password(), None, None, "test-probe");
        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-1"))
            .credentials_provider(credentials)
            .load()
            .await;

        aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Builder::from(&sdk_config)
                .endpoint_url(store.endpoint().to_string())
                .force_path_style(true)
                .build(),
        )
    }

    /// Whether the bucket is there, read by the probe rather than by the code under test.
    async fn read_bucket_presence(
        probe: &aws_sdk_s3::Client,
        bucket_name: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        match probe.head_bucket().bucket(bucket_name).send().await {
            Ok(_) => Ok(true),
            Err(aws_sdk_s3::error::SdkError::ServiceError(service_error))
                if service_error.raw().status().as_u16() == 404 =>
            {
                Ok(false)
            }
            Err(e) => Err(Box::new(e)),
        }
    }

    fn build_config(store: &S3TestContainer, bucket_name: &str) -> S3Config {
        S3Config::new(
            store.endpoint(),
            store.username(),
            store.password(),
            bucket_name,
            "us-east-1",
        )
    }

    /// The default a pool gets without asking: a bucket that is not there is created, so a first
    /// deployment needs no manual step.
    #[tokio::test]
    async fn a_missing_bucket_is_created_when_container_creation_is_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        let store = S3TestContainer::start().await?;
        let probe = build_bucket_probe(&store).await;
        assert!(
            !read_bucket_presence(&probe, ABSENT_CONTAINER_NAME).await?,
            "the scenario is about a bucket nobody created yet"
        );

        S3Backend::build(
            build_config(&store, ABSENT_CONTAINER_NAME),
            Arc::new(UnusedJobRegistry),
            Arc::new(NoopMetrics),
        )
        .await?;

        assert!(
            read_bucket_presence(&probe, ABSENT_CONTAINER_NAME).await?,
            "the backend must have created the bucket it was pointed at"
        );
        Ok(())
    }

    /// The third cell of the (creation allowed × bucket present) matrix, and the one a
    /// permission-restricted deployment actually runs: the bucket is there and the pool may not
    /// create one, so the check it makes is the whole of what it owes.
    ///
    /// Checked by breaking it: refusing on `is_container_creation_allowed` before the bucket is
    /// looked at - the flag being what the refusal message names, and so the natural thing to branch
    /// on early - keeps the two cases below green and fails this one.
    #[tokio::test]
    async fn a_present_bucket_is_accepted_when_container_creation_is_off_s3() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        let store = S3TestContainer::start().await?;
        let probe = build_bucket_probe(&store).await;
        probe.create_bucket().bucket(PRESENT_CONTAINER_NAME).send().await?;
        assert!(
            read_bucket_presence(&probe, PRESENT_CONTAINER_NAME).await?,
            "the scenario is about a bucket that is already there"
        );

        S3Backend::build(
            build_config(&store, PRESENT_CONTAINER_NAME).with_container_creation(false),
            Arc::new(UnusedJobRegistry),
            Arc::new(NoopMetrics),
        )
        .await?;

        Ok(())
    }

    /// With creation turned off the backend only makes sure the bucket is reachable. Starting anyway
    /// would let a pool that has no permission to create one run against a store it cannot write to,
    /// and the failure would surface on the first save instead of at start-up.
    #[tokio::test]
    async fn a_missing_bucket_is_refused_when_container_creation_is_off_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        let store = S3TestContainer::start().await?;
        let probe = build_bucket_probe(&store).await;

        let error = S3Backend::build(
            build_config(&store, ABSENT_CONTAINER_NAME).with_container_creation(false),
            Arc::new(UnusedJobRegistry),
            Arc::new(NoopMetrics),
        )
        .await
        .err()
        .expect("a backend that may not create the bucket must not start without one");

        assert!(
            error.to_string().contains(ABSENT_CONTAINER_NAME),
            "the error must name the bucket it looked for, got: {error}"
        );
        assert!(
            !error.to_string().contains("job"),
            "a refusal about the bucket must not be worded as a missing job, got: {error}"
        );
        assert!(
            !read_bucket_presence(&probe, ABSENT_CONTAINER_NAME).await?,
            "a refused start must not have created the bucket"
        );
        Ok(())
    }
}

#[cfg(feature = "storage-gcs")]
mod on_gcs {
    use std::sync::Arc;

    use super::{ABSENT_CONTAINER_NAME, PRESENT_CONTAINER_NAME};
    use crate::tests::common::UnusedJobRegistry;
    use crate::tests::common::gcs_container::GcsTestContainer;
    use crate::{GcsBackend, GcsConfig, NoopMetrics};

    /// Project the backend creates the bucket under. The emulator accepts any name.
    const CREATION_PROJECT_ID: &str = "jobmanager-container-creation";

    /// Whether the bucket is there, read by a client of the test rather than by the code under
    /// test.
    ///
    /// Only `404` reads as a bucket that is not there. Every other refusal is reported, because an
    /// emulator that answered `429` or `500` would otherwise satisfy an assertion about an absent
    /// bucket and hide the failure the case is really about.
    async fn read_bucket_presence(
        probe: &reqwest::Client,
        store: &GcsTestContainer,
        bucket_name: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let response = probe
            .get(format!("{}/storage/v1/b/{bucket_name}", store.endpoint()))
            .send()
            .await?;
        let status = response.status();

        if status.is_success() {
            return Ok(true);
        }
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }

        Err(format!("the emulator answered {status} when asked about bucket {bucket_name}").into())
    }

    fn build_config(store: &GcsTestContainer, bucket_name: &str) -> GcsConfig {
        GcsConfig::new(store.endpoint(), bucket_name)
            .with_anonymous_access()
            .with_project_id(CREATION_PROJECT_ID)
    }

    /// The default a pool gets without asking: a bucket that is not there is created, so a first
    /// deployment needs no manual step.
    #[tokio::test]
    async fn a_missing_bucket_is_created_when_container_creation_is_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        let store = GcsTestContainer::start().await?;
        let probe = reqwest::Client::builder().build()?;
        assert!(
            !read_bucket_presence(&probe, &store, ABSENT_CONTAINER_NAME).await?,
            "the scenario is about a bucket nobody created yet"
        );

        GcsBackend::build(
            build_config(&store, ABSENT_CONTAINER_NAME),
            Arc::new(UnusedJobRegistry),
            Arc::new(NoopMetrics),
        )
        .await?;

        assert!(
            read_bucket_presence(&probe, &store, ABSENT_CONTAINER_NAME).await?,
            "the backend must have created the bucket it was pointed at"
        );
        Ok(())
    }

    /// The third cell of the (creation allowed × bucket present) matrix, and the one a
    /// permission-restricted deployment actually runs: the bucket is there and the pool may not
    /// create one, so the check it makes is the whole of what it owes.
    ///
    /// Checked by breaking it: refusing on `MissingBucketPolicy::Refuse` before the bucket is looked
    /// at keeps the two cases beside this one green and fails this one.
    #[tokio::test]
    async fn a_present_bucket_is_accepted_when_container_creation_is_off_gcs() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        let store = GcsTestContainer::start().await?;
        let probe = reqwest::Client::builder().build()?;
        probe
            .post(format!(
                "{}/storage/v1/b?project={CREATION_PROJECT_ID}",
                store.endpoint()
            ))
            .json(&serde_json::json!({ "name": PRESENT_CONTAINER_NAME }))
            .send()
            .await?
            .error_for_status()?;
        assert!(
            read_bucket_presence(&probe, &store, PRESENT_CONTAINER_NAME).await?,
            "the scenario is about a bucket that is already there"
        );

        GcsBackend::build(
            GcsConfig::new(store.endpoint(), PRESENT_CONTAINER_NAME)
                .with_anonymous_access()
                .with_container_creation(false),
            Arc::new(UnusedJobRegistry),
            Arc::new(NoopMetrics),
        )
        .await?;

        Ok(())
    }

    /// With creation turned off the backend only makes sure the bucket is reachable, for the same
    /// reason the other two backends do. The project is left unnamed here on purpose: a pool that
    /// may not create a bucket has nothing to name one for, and a backend that asked for it anyway
    /// would refuse to start over a setting its work never needs.
    #[tokio::test]
    async fn a_missing_bucket_is_refused_when_container_creation_is_off_gcs() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        let store = GcsTestContainer::start().await?;
        let probe = reqwest::Client::builder().build()?;

        let error = GcsBackend::build(
            GcsConfig::new(store.endpoint(), ABSENT_CONTAINER_NAME)
                .with_anonymous_access()
                .with_container_creation(false),
            Arc::new(UnusedJobRegistry),
            Arc::new(NoopMetrics),
        )
        .await
        .err()
        .expect("a backend that may not create the bucket must not start without one");

        assert!(
            error.to_string().contains(ABSENT_CONTAINER_NAME),
            "the error must name the bucket it looked for, got: {error}"
        );
        assert!(
            !error.to_string().contains("job"),
            "a refusal about the bucket must not be worded as a missing job, got: {error}"
        );
        assert!(
            !read_bucket_presence(&probe, &store, ABSENT_CONTAINER_NAME).await?,
            "a refused start must not have created the bucket"
        );
        Ok(())
    }
}

#[cfg(feature = "storage-azure")]
mod on_azure {
    use std::sync::Arc;

    use super::{ABSENT_CONTAINER_NAME, PRESENT_CONTAINER_NAME};
    use crate::tests::common::UnusedJobRegistry;
    use crate::tests::common::azure_container::AzureTestContainer;
    use crate::{AzureBackend, AzureConfig, NoopMetrics};

    fn build_config(store: &AzureTestContainer, container_name: &str) -> AzureConfig {
        AzureConfig::new(
            store.endpoint(),
            AzureTestContainer::account(),
            AzureTestContainer::account_key(),
            container_name,
        )
    }

    /// The default a pool gets without asking: a container that is not there is created, so a first
    /// deployment needs no manual step.
    #[tokio::test]
    async fn a_missing_container_is_created_when_container_creation_is_on_azure()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        let store = AzureTestContainer::start().await?;
        let probe = store.build_container_client(ABSENT_CONTAINER_NAME)?;
        assert!(
            !probe.exists().await?,
            "the scenario is about a container nobody created yet"
        );

        AzureBackend::build(
            build_config(&store, ABSENT_CONTAINER_NAME),
            Arc::new(UnusedJobRegistry),
            Arc::new(NoopMetrics),
        )
        .await?;

        assert!(
            probe.exists().await?,
            "the backend must have created the container it was pointed at"
        );
        Ok(())
    }

    /// The third cell of the (creation allowed × container present) matrix, and the one a
    /// permission-restricted deployment actually runs: the container is there and the pool may not
    /// create one, so the check it makes is the whole of what it owes.
    ///
    /// Checked by breaking it: refusing on `is_container_creation_allowed` before the container is
    /// looked at keeps the two cases beside this one green and fails this one.
    #[tokio::test]
    async fn a_present_container_is_accepted_when_container_creation_is_off_azure()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        let store = AzureTestContainer::start().await?;
        let probe = store.build_container_client(PRESENT_CONTAINER_NAME)?;
        probe.create(None).await?;
        assert!(
            probe.exists().await?,
            "the scenario is about a container that is already there"
        );

        AzureBackend::build(
            build_config(&store, PRESENT_CONTAINER_NAME).with_container_creation(false),
            Arc::new(UnusedJobRegistry),
            Arc::new(NoopMetrics),
        )
        .await?;

        Ok(())
    }

    /// With creation turned off the backend only makes sure the container is reachable, for the
    /// same reason the S3 backend does.
    #[tokio::test]
    async fn a_missing_container_is_refused_when_container_creation_is_off_azure()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        let store = AzureTestContainer::start().await?;
        let probe = store.build_container_client(ABSENT_CONTAINER_NAME)?;

        let error = AzureBackend::build(
            build_config(&store, ABSENT_CONTAINER_NAME).with_container_creation(false),
            Arc::new(UnusedJobRegistry),
            Arc::new(NoopMetrics),
        )
        .await
        .err()
        .expect("a backend that may not create the container must not start without one");

        assert!(
            error.to_string().contains(ABSENT_CONTAINER_NAME),
            "the error must name the container it looked for, got: {error}"
        );
        assert!(
            !error.to_string().contains("job"),
            "a refusal about the container must not be worded as a missing job, got: {error}"
        );
        assert!(
            !probe.exists().await?,
            "a refused start must not have created the container"
        );
        Ok(())
    }
}
