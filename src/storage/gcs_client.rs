//! The Google Cloud Storage JSON API, in the operations job state is kept with.
//!
//! Knows HTTP and the JSON API and nothing above them: it builds a URL, sends one request and hands
//! back the status and the body. What a status *means* - which one is a conflict, which one is the
//! answer a conditional read asked for - is decided in [`gcs`](super::gcs), because that decision
//! belongs to the backend and not to the wire.
//!
//! Written here rather than taken from a published client because each of the four measured against
//! this API broke one of the crate's own contracts: one loses the `304` a conditional read is
//! answered with, one cannot write through the emulator at all, one spends two requests on a
//! conditional write, and one speaks the XML API, where the conditions are not honoured. `reqwest`
//! neither repeats a request nor splits a transfer on its own, so one call here is one request - the
//! invariant on what a call of `Storage` may cost in `AGENTS.md`.

use google_cloud_auth::credentials::{CacheableResource, Credentials};
use reqwest::{Client, RequestBuilder, Url, redirect};
use serde::Deserialize;

use crate::Error;

/// Header the store names an object's generation in. Answered by a read of the object's bytes,
/// where the body is the state rather than the metadata, so it is the only place the new version
/// of a conditional read arrives - reading it is what keeps that read at one request.
const GENERATION_HEADER: &str = "x-goog-generation";

/// Digits a percent-escape is spelled with. Upper case, which is the form RFC 3986 prefers.
const HEX_DIGITS: &[u8; 16] = b"0123456789ABCDEF";

/// One object as the JSON API describes it.
///
/// Only the two fields this crate uses are declared. A client that declared more broke on this very
/// API: `selfLink` is documented and absent from what the emulator answers, and a field the answer
/// does not carry fails the whole parse.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StateObjectMeta {
    pub name: String,
    pub generation: String,
}

/// One page of a listing as the JSON API answers it.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListedObjectsPage {
    /// Absent from the answer when the listing matched nothing.
    #[serde(default)]
    pub items: Vec<StateObjectMeta>,
    pub next_page_token: Option<String>,
}

/// What one request came back with.
pub(crate) struct JsonApiResponse {
    pub status: u16,
    /// Generation the answer named in [`GENERATION_HEADER`], absent where it carried none.
    pub generation: Option<String>,
    pub body: Vec<u8>,
}

/// What kept a request from coming back with an answer at all.
pub(crate) enum GcsRequestError {
    /// The authorization headers could not be produced, so nothing was sent.
    ///
    /// `is_transient` is the credentials' own answer about their failure, and the only thing telling
    /// a refresh that failed on the network from a key that cannot sign: `details` says that to a
    /// reader and to nothing else.
    Unauthorized { details: String, is_transient: bool },
    /// The request never reached the service, or its answer never came back whole.
    Transport(reqwest::Error),
}

/// One bucket of the JSON API, reached with one set of credentials.
pub(crate) struct GcsHttpClient {
    http: Client,
    credentials: Credentials,
    endpoint: String,
    bucket_name: String,
}

impl GcsHttpClient {
    /// Client of `bucket_name` at `endpoint`, authorized by `credentials`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] when `endpoint` is not a URL, and when the HTTP client cannot be
    /// built - a TLS backend the target does not provide.
    pub(crate) fn try_new(
        endpoint: &str,
        bucket_name: impl Into<String>,
        credentials: Credentials,
    ) -> Result<Self, Error> {
        let endpoint = endpoint.trim_end_matches('/');
        // An address no request can be built from is refused here rather than on the first send:
        // the bucket check hands what a send reports to the `Retrier`, which would spend its whole
        // budget before a pool learns that the address is the cause.
        Url::parse(endpoint).map_err(|e| Error::Other(format!("gcs endpoint {endpoint} is not a url: {e}")))?;

        Ok(Self {
            http: Client::builder()
                // A followed redirect is a second request nobody quoted, which is the silent shift
                // the invariant on what a call of `Storage` may cost exists to catch. Left to the
                // client it would be invisible; refused here, a store that starts redirecting
                // surfaces as a status the backend reports.
                .redirect(redirect::Policy::none())
                .build()
                .map_err(|e| Error::Other(format!("Failed to init gcs http client: {e}")))?,
            credentials,
            endpoint: endpoint.to_string(),
            bucket_name: bucket_name.into(),
        })
    }

    /// Writes `body` as the object `key`, conditioned on `if_generation_match` - the generation the
    /// caller expects to replace, or `0` for an object that must not exist yet.
    ///
    /// The answer is the object's metadata, so the generation the store gave the write is read out
    /// of the body rather than out of a header.
    pub(crate) async fn put_object(
        &self,
        key: &str,
        body: Vec<u8>,
        content_type: &str,
        if_generation_match: &str,
    ) -> Result<JsonApiResponse, GcsRequestError> {
        // `uploadType=media` is the one upload that is a single request: `multipart` needs a part
        // the emulator refuses, and `resumable` costs a session request before the bytes.
        let url = format!(
            "{}/upload/storage/v1/b/{}/o?uploadType=media&name={}&ifGenerationMatch={}",
            self.endpoint,
            encode_uri_component(&self.bucket_name),
            encode_uri_component(key),
            encode_uri_component(if_generation_match)
        );

        self.send_request(self.http.post(url).header("content-type", content_type).body(body))
            .await
    }

    /// Reads the bytes of `key`, but only while its stored generation is `if_generation_match`.
    pub(crate) async fn get_object(
        &self,
        key: &str,
        if_generation_match: &str,
    ) -> Result<JsonApiResponse, GcsRequestError> {
        let url = format!(
            "{}?alt=media&ifGenerationMatch={}",
            self.build_object_url(key),
            encode_uri_component(if_generation_match)
        );

        self.send_request(self.http.get(url)).await
    }

    /// Reads the bytes of `key` unless its stored generation is still `if_generation_not_match`,
    /// which the store answers `304`.
    pub(crate) async fn get_changed_object(
        &self,
        key: &str,
        if_generation_not_match: &str,
    ) -> Result<JsonApiResponse, GcsRequestError> {
        let url = format!(
            "{}?alt=media&ifGenerationNotMatch={}",
            self.build_object_url(key),
            encode_uri_component(if_generation_not_match)
        );

        self.send_request(self.http.get(url)).await
    }

    /// Lists one page of the objects under `prefix`, starting at `start_key` when one is named.
    ///
    /// `start_key` is inclusive: the store answers with the boundary key itself.
    pub(crate) async fn list_objects(
        &self,
        prefix: &str,
        start_key: Option<&str>,
        page_size: i32,
        cursor: Option<&str>,
    ) -> Result<JsonApiResponse, GcsRequestError> {
        let mut url = format!(
            "{}/storage/v1/b/{}/o?prefix={}&maxResults={page_size}",
            self.endpoint,
            encode_uri_component(&self.bucket_name),
            encode_uri_component(prefix)
        );
        if let Some(start_key) = start_key {
            url.push_str("&startOffset=");
            url.push_str(&encode_uri_component(start_key));
        }
        if let Some(cursor) = cursor {
            url.push_str("&pageToken=");
            url.push_str(&encode_uri_component(cursor));
        }

        self.send_request(self.http.get(url)).await
    }

    /// Deletes `key`. A key that is already gone is answered `404`, which the caller reads as the
    /// success it asked for.
    pub(crate) async fn delete_object(&self, key: &str) -> Result<JsonApiResponse, GcsRequestError> {
        self.send_request(self.http.delete(self.build_object_url(key))).await
    }

    /// Reads the bucket's metadata, which is how its reachability is checked.
    pub(crate) async fn find_bucket(&self) -> Result<JsonApiResponse, GcsRequestError> {
        let url = format!(
            "{}/storage/v1/b/{}",
            self.endpoint,
            encode_uri_component(&self.bucket_name)
        );

        self.send_request(self.http.get(url)).await
    }

    /// Creates the bucket under `project_id`, which the API requires for a creation and for nothing
    /// else.
    pub(crate) async fn create_bucket(&self, project_id: &str) -> Result<JsonApiResponse, GcsRequestError> {
        let url = format!(
            "{}/storage/v1/b?project={}",
            self.endpoint,
            encode_uri_component(project_id)
        );

        self.send_request(self.http.post(url).json(&serde_json::json!({ "name": self.bucket_name })))
            .await
    }

    /// URL of one object's resource, the name percent-encoded into a single path segment.
    fn build_object_url(&self, key: &str) -> String {
        format!(
            "{}/storage/v1/b/{}/o/{}",
            self.endpoint,
            encode_uri_component(&self.bucket_name),
            encode_uri_component(key)
        )
    }

    /// Authorizes `request`, sends it, and reads the whole of what came back.
    ///
    /// The body is read here rather than handed back unread, so that the timeout and the
    /// cancellation the caller puts around this cover what a request really costs: a body that never
    /// arrives holds a worker exactly as a request that never answers does.
    ///
    /// Producing the headers can itself reach Google's token endpoint, which the credentials cache
    /// and which no quota of this crate counts: a quota states what the *store* was asked for, and a
    /// token is neither addressed to it nor billed by it.
    async fn send_request(&self, request: RequestBuilder) -> Result<JsonApiResponse, GcsRequestError> {
        let authorization =
            self.credentials
                .headers(http::Extensions::new())
                .await
                .map_err(|e| GcsRequestError::Unauthorized {
                    details: format!("gcs credentials refused to sign a request: {e}"),
                    is_transient: e.is_transient(),
                })?;
        let authorization = match authorization {
            CacheableResource::New { data, .. } => data,
            // Answered only to a caller that supplied an `EntityTag`, and this one never does.
            // Sending the request unauthorized instead would come back `401` and read as the store
            // refusing a worker, which is a different failure with a different remedy.
            CacheableResource::NotModified => {
                return Err(GcsRequestError::Unauthorized {
                    details: "gcs credentials answered 'not modified' to a request naming no entity tag".to_string(),
                    // Nothing about the credentials failed, so the next attempt asks the same
                    // question and gets the same answer.
                    is_transient: false,
                });
            }
        };

        let response = request
            .headers(authorization)
            .send()
            .await
            .map_err(GcsRequestError::Transport)?;
        let status = response.status().as_u16();
        let generation = response
            .headers()
            .get(GENERATION_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string);
        let body = response.bytes().await.map_err(GcsRequestError::Transport)?;

        Ok(JsonApiResponse {
            status,
            generation,
            body: Vec::from(body),
        })
    }
}

/// Percent-encodes everything but the unreserved characters of RFC 3986.
///
/// An object name carries `/`, and the name is sent as one path segment: left unescaped the slash
/// would address a different resource, and the store would answer `404` for an object that is there.
fn encode_uri_component(component: &str) -> String {
    let mut encoded = String::with_capacity(component.len());
    for byte in component.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX_DIGITS[usize::from(byte & 0x0F)]));
        }
    }

    encoded
}

#[cfg(test)]
mod tests {
    use google_cloud_auth::credentials::testing::error_credentials;

    use super::*;

    /// Address the case below points its client at: the credentials fail before anything is sent,
    /// so a failure that started happening after the send would fail the case rather than hang it.
    const UNREACHED_ENDPOINT: &str = "http://127.0.0.1:1";

    /// What the credentials said about themselves travels into the failure, because the formatted
    /// text below it says it to a reader and to nothing else: the mapping that decides whether the
    /// call is repeated reads the marker, and there is no second place to recover it from.
    ///
    /// Checked by breaking it: writing a constant in place of `is_transient()` fails one of the two
    /// rounds.
    #[tokio::test]
    async fn what_the_credentials_said_about_their_failure_reaches_the_caller() {
        for declared_transient in [true, false] {
            let client = GcsHttpClient::try_new(UNREACHED_ENDPOINT, "jobs", error_credentials(declared_transient))
                .expect("a loopback address must build a client");

            let answer = client.find_bucket().await;

            let Err(GcsRequestError::Unauthorized { is_transient, .. }) = answer else {
                panic!("credentials that always fail must not answer with a response")
            };
            assert_eq!(
                is_transient, declared_transient,
                "the failure must carry what the credentials said about it"
            );
        }
    }

    /// The key layout every job's state object is named by, which is what the encoding has to
    /// survive: the separators are what a path segment would otherwise swallow.
    #[test]
    fn a_state_object_name_travels_as_one_path_segment() {
        assert_eq!(
            encode_uri_component("jobs/job/state-00000000000000000001.json"),
            "jobs%2Fjob%2Fstate-00000000000000000001.json"
        );
    }

    /// The unreserved set is kept as it is, so a name that needs no escaping is not rewritten into
    /// one the store would read as a different object.
    #[test]
    fn the_unreserved_characters_are_left_alone() {
        assert_eq!(encode_uri_component("aZ09-_.~"), "aZ09-_.~");
    }

    /// A name is bytes rather than characters, so a multi-byte one is encoded byte by byte - the
    /// form the store parses back.
    #[test]
    fn a_name_outside_ascii_is_encoded_byte_by_byte() {
        assert_eq!(encode_uri_component("é ?&=+"), "%C3%A9%20%3F%26%3D%2B");
    }

    /// A listing that matched nothing carries no `items` at all, and a page that fails to parse
    /// would read as a failed request.
    #[test]
    fn a_listing_that_matched_nothing_parses_as_an_empty_page() {
        let page: ListedObjectsPage =
            serde_json::from_slice(br#"{"kind":"storage#objects"}"#).expect("an empty listing must parse");

        assert!(page.items.is_empty());
        assert!(page.next_page_token.is_none());
    }

    /// The two fields this crate reads, out of an answer carrying the ones it does not.
    #[test]
    fn an_object_parses_out_of_an_answer_carrying_fields_this_crate_ignores() {
        let meta: StateObjectMeta = serde_json::from_slice(
            br#"{"kind":"storage#object","name":"jobs/job/state-1.json","generation":"1789314575620",
                 "metageneration":"1","size":"7","etag":"CIS="}"#,
        )
        .expect("an answer of the JSON API must parse");

        assert_eq!(meta.name, "jobs/job/state-1.json");
        assert_eq!(meta.generation, "1789314575620");
    }
}
