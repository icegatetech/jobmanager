//! What the emulator does with the listing boundary the Azure backend sends.
//!
//! `AzureBackend` names `startFrom` on every cleanup listing, which `List Blobs` documents from REST
//! version 2023-05-03 on. The emulator this crate is tested against implements an older listing and
//! drops the parameter, so every case running against it sees a job's whole prefix where the service
//! would answer from the boundary. That is the difference this case pins: a backend that needed the
//! boundary honoured to keep an iteration undeletable would be passing here and losing state against
//! the emulator, and no other case would say so.
//!
//! Named after the provider because that is what it is about - the listing contract of one store,
//! not behaviour every backend owes.

use azure_core::http::RequestContent;
use azure_storage_blob::{BlobContainerClient, models::BlobContainerClientListBlobsOptions};
use futures_util::TryStreamExt;

use super::common::azure_container::AzureTestContainer;

/// Container this case keeps its blobs in.
const BOUNDARY_CONTAINER_NAME: &str = "listing-boundary";

/// The blobs written, in the ascending order a listing answers with. Stated literally so the
/// expectation does not come from the key builder of the backend under test.
const LISTED_BLOB_NAMES: [&str; 3] = ["state-1", "state-2", "state-3"];

/// The name handed to the boundary: the middle blob, so an inclusive boundary, an exclusive one and
/// an ignored one are three different answers.
const BOUNDARY_BLOB_NAME: &str = "state-2";

/// Names the container answers `options` with, so what the store did with a parameter is read as a
/// set of names rather than as a count.
async fn read_listed_names(
    client: &BlobContainerClient,
    options: BlobContainerClientListBlobsOptions<'static>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut pages = client.list_blobs(Some(options))?.into_pages();

    let mut names = Vec::new();
    while let Some(page) = pages.try_next().await? {
        names.extend(page.into_model()?.blob_items.into_iter().filter_map(|blob| blob.name));
    }
    names.sort();

    Ok(names)
}

/// The boundary the backend sends is dropped by the emulator, so every cleanup listing run against
/// it comes back with the iterations the boundary would have cut off.
///
/// Read as "the same names come back either way", because a store that drops an unknown query
/// parameter answers a bounded listing with the whole of it - which is why what makes an iteration
/// deletable is the check `ObjectStorage::list_job_outdated_iterations` makes over the listed keys,
/// and never where the listing began.
///
/// The prefix listing is the control that makes the boundary assertion mean something: it is a
/// parameter the store does honour, so the listing demonstrably narrows when it is asked to. An
/// unchanged answer to the boundary is therefore the store ignoring it, not this case being unable
/// to see a narrower listing at all. Take the prefix out and the control stops holding, which is
/// the break this case is checked by.
///
/// Says nothing about the service, which documents `startFrom` as answering from the key it is
/// given; no case here reaches it, and the docstring of `AzureConfig` separates the two.
#[tokio::test]
async fn the_listing_boundary_the_backend_sends_is_dropped_by_the_emulator() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let container = AzureTestContainer::start().await?;
    let client = container.build_container_client(BOUNDARY_CONTAINER_NAME)?;
    client.create(None).await?;
    for name in LISTED_BLOB_NAMES {
        client
            .blob_client(name)
            .upload(RequestContent::from(b"state".to_vec()), None)
            .await?;
    }

    let listed_whole = read_listed_names(&client, BlobContainerClientListBlobsOptions::default()).await?;
    let listed_by_prefix = read_listed_names(
        &client,
        BlobContainerClientListBlobsOptions {
            prefix: Some(BOUNDARY_BLOB_NAME.to_string()),
            ..Default::default()
        },
    )
    .await?;
    let listed_from_boundary = read_listed_names(
        &client,
        BlobContainerClientListBlobsOptions {
            start_from: Some(BOUNDARY_BLOB_NAME.to_string()),
            ..Default::default()
        },
    )
    .await?;

    assert_eq!(
        listed_whole,
        LISTED_BLOB_NAMES.map(String::from).to_vec(),
        "the fixture must have written every blob the boundary is asked about"
    );
    assert_eq!(
        listed_by_prefix,
        vec![BOUNDARY_BLOB_NAME.to_string()],
        "a parameter the store honours must narrow the listing, or this case could not tell a narrower one apart"
    );
    assert_eq!(
        listed_from_boundary, listed_whole,
        "a boundary the store honoured would have dropped the blobs sorting before {BOUNDARY_BLOB_NAME}"
    );

    Ok(())
}
