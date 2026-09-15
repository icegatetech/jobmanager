//! Shared-key authentication for Azure Blob Storage requests.
//!
//! The module exists because `azure_storage_blob` authenticates with Entra ID only: it takes a
//! [`TokenCredential`](azure_core::credentials::TokenCredential) and has no account-key path at
//! all. It is meant to go away whole once the SDK grows one - nothing outside `storage` names it,
//! and [`AzureConfig`](super::azure::AzureConfig) keeps its shape either way.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use azure_core::{
    base64,
    credentials::Secret,
    error::ErrorKind,
    hmac::hmac_sha256,
    http::{
        Context, Request, Url,
        policies::{Policy, PolicyResult},
    },
    time::{OffsetDateTime, to_rfc7231},
};

use crate::{StorageError, StorageResult};

/// Signs every request with the storage account key, in the `SharedKey` scheme the Blob service
/// documents under "Authorize with Shared Key".
///
/// Registered as a per-try policy, which is what puts it after the retry policy: the signature
/// covers a timestamp the service refuses once it is a quarter of an hour old, so a retried attempt
/// has to be signed again rather than repeat the signature of the attempt that failed.
#[derive(Debug)]
pub(crate) struct SharedKeySigningPolicy {
    account: String,
    key: Secret,
}

impl SharedKeySigningPolicy {
    /// Takes the account and its key, rejecting a key that is not base64.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Auth`] when `account_key` does not decode. The check is made here
    /// rather than at the first request so that a mistyped key stops the pool from being built at
    /// all, instead of turning every later request into an authorization failure.
    pub(crate) fn try_new(account: &str, account_key: &Secret) -> StorageResult<Self> {
        base64::decode(account_key.secret())
            .map_err(|e| StorageError::Auth(format!("azure account key is not base64: {e}")))?;

        Ok(Self {
            account: account.to_string(),
            key: account_key.clone(),
        })
    }

    /// The `Authorization` header value for `request` as it stands, headers included.
    fn build_authorization(&self, request: &Request) -> azure_core::Result<String> {
        let signature = hmac_sha256(&self.build_string_to_sign(request), &self.key)?;

        Ok(format!("SharedKey {}:{signature}", self.account))
    }

    /// The string the signature is taken over: twelve fixed header slots in the documented order,
    /// then the `x-ms-*` headers, then the resource the request addresses.
    ///
    /// `If-Match` and `If-None-Match` are two of the twelve slots, which is why a signature built
    /// without them fails exactly the conditional writes this crate is coordinated by, and nothing
    /// else.
    fn build_string_to_sign(&self, request: &Request) -> String {
        // Both a missing header and a zero-length body are signed as an empty field; sending "0"
        // is what the service rejects.
        let content_length = match Self::read_header(request, "content-length") {
            "0" => "",
            length => length,
        };

        // The seventh slot is `Date` and is left empty: the timestamp travels in `x-ms-date`, which
        // the canonicalized headers below carry.
        let mut string_to_sign = format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n\n{}\n{}\n{}\n{}\n{}\n",
            request.method().as_str(),
            Self::read_header(request, "content-encoding"),
            Self::read_header(request, "content-language"),
            content_length,
            Self::read_header(request, "content-md5"),
            Self::read_header(request, "content-type"),
            Self::read_header(request, "if-modified-since"),
            Self::read_header(request, "if-match"),
            Self::read_header(request, "if-none-match"),
            Self::read_header(request, "if-unmodified-since"),
            Self::read_header(request, "range"),
        );

        string_to_sign.push_str(&Self::build_canonicalized_headers(request));
        string_to_sign.push_str(&self.build_canonicalized_resource(request.url()));

        string_to_sign
    }

    /// The account and path the request addresses, followed by its query parameters.
    ///
    /// The path goes in exactly as the request carries it, still percent-encoded, while the query
    /// parameters go in decoded - the two halves of the resource are spelled differently, and
    /// signing either one the other way is a `403` on every request that needs an escape.
    ///
    /// The account is prepended to the whole path rather than replacing its first segment, which is
    /// also what an emulator addressing accounts by path expects: there the account name appears
    /// twice, and the service signs it the same way.
    fn build_canonicalized_resource(&self, url: &Url) -> String {
        let mut resource = format!("/{}{}", self.account, url.path());

        // One name can carry several values, and they are signed as one sorted, comma-joined
        // field - hence the map of lists rather than a list of pairs.
        let mut parameters: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (name, value) in url.query_pairs() {
            parameters.entry(name.to_lowercase()).or_default().push(value.into_owned());
        }

        for (name, mut values) in parameters {
            values.sort_unstable();
            resource.push('\n');
            resource.push_str(&name);
            resource.push(':');
            resource.push_str(&values.join(","));
        }

        resource
    }

    /// The `x-ms-*` headers of the request, sorted by name, one `name:value` line each.
    ///
    /// Names are lowercase by construction - [`HeaderName`](azure_core::http::headers::HeaderName)
    /// refuses an uppercase one - so sorting them is the whole of the canonical order.
    fn build_canonicalized_headers(request: &Request) -> String {
        let mut headers: Vec<(&str, &str)> = request
            .headers()
            .iter()
            .filter(|(name, _)| name.as_str().starts_with("x-ms-"))
            .map(|(name, value)| (name.as_str(), value.as_str().trim()))
            .collect();
        headers.sort_unstable_by_key(|(name, _)| *name);

        let mut canonicalized = String::new();
        for (name, value) in headers {
            canonicalized.push_str(name);
            canonicalized.push(':');
            canonicalized.push_str(value);
            canonicalized.push('\n');
        }

        canonicalized
    }

    /// The value of `name`, or an empty string when the request carries no such header - which is what
    /// the signature spells an absent field as.
    fn read_header<'a>(request: &'a Request, name: &'static str) -> &'a str {
        request.headers().get_optional_str(&name.into()).unwrap_or_default()
    }
}

#[async_trait]
impl Policy for SharedKeySigningPolicy {
    async fn send(&self, ctx: &Context, request: &mut Request, next: &[Arc<dyn Policy>]) -> PolicyResult {
        request.insert_header("x-ms-date", to_rfc7231(&OffsetDateTime::now_utc()));
        let authorization = self.build_authorization(request)?;
        request.insert_header("authorization", authorization);

        // The pipeline ends in a transport policy, but the slice does not say so, and this crate
        // does not panic to find out.
        let Some((next_policy, rest)) = next.split_first() else {
            return Err(azure_core::Error::with_message(
                ErrorKind::Other,
                "a signed request reached the end of the pipeline with no transport policy",
            ));
        };

        next_policy.send(ctx, request, rest).await
    }
}

#[cfg(test)]
mod tests {
    use azure_core::http::Method;

    use super::*;

    /// Key of the Azure storage emulator: public, fixed in every installation, and used here only
    /// because a signature test needs some key that decodes.
    const SCRIPTED_ACCOUNT_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    fn build_policy() -> SharedKeySigningPolicy {
        SharedKeySigningPolicy::try_new("myaccount", &Secret::new(SCRIPTED_ACCOUNT_KEY))
            .expect("the emulator key is base64")
    }

    fn build_request(url: &str, method: Method) -> Request {
        Request::new(Url::parse(url).expect("the scripted url must parse"), method)
    }

    /// The worked example of "Authorize with Shared Key" in the Blob service documentation: request
    /// on the left, the string its signature is taken over on the right. The oracle is the
    /// documentation's, not this module's - a string this module also produced would assert nothing.
    #[test]
    fn a_signature_follows_the_documented_string_to_sign() {
        let mut request = build_request(
            "http://myaccount.blob.core.windows.net/mycontainer?restype=container&comp=metadata&timeout=20",
            Method::Get,
        );
        request.insert_header("x-ms-date", "Fri, 26 Jun 2015 23:39:12 GMT");
        request.insert_header("x-ms-version", "2015-02-21");

        let string_to_sign = build_policy().build_string_to_sign(&request);

        assert_eq!(
            string_to_sign,
            "GET\n\n\n\n\n\n\n\n\n\n\n\n\
             x-ms-date:Fri, 26 Jun 2015 23:39:12 GMT\n\
             x-ms-version:2015-02-21\n\
             /myaccount/mycontainer\ncomp:metadata\nrestype:container\ntimeout:20"
        );
    }

    /// The conditional headers are what every write of this crate is coordinated by, and they are
    /// part of what is signed: a signature that ignored them would be accepted for an unconditional
    /// request and refused for the conditional one it was meant for.
    #[test]
    fn a_conditional_header_changes_the_signature() {
        let policy = build_policy();
        let unconditional = build_request("http://myaccount.blob.core.windows.net/c/b", Method::Put);
        let mut conditional = build_request("http://myaccount.blob.core.windows.net/c/b", Method::Put);
        conditional.insert_header("if-none-match", "*");

        let unconditional_signature = policy
            .build_authorization(&unconditional)
            .expect("the scripted request must sign");
        let conditional_signature = policy
            .build_authorization(&conditional)
            .expect("the scripted request must sign");

        assert_ne!(
            unconditional_signature, conditional_signature,
            "a conditional write must not be signed as an unconditional one"
        );
    }

    /// The fourth slot of the string to sign is the length of the body the request carries, and the
    /// service spells a zero-length body as an empty field rather than as `0` - so a request that
    /// carries `content-length: 0` and one that carries no such header are signed identically, while
    /// a request with a body is not.
    ///
    /// Oracle: the field table of "Authorize with Shared Key", not this module. Every other signing
    /// case builds a request with no body at all, so neither arm is stated anywhere else - and the
    /// zero-length arm is the one container creation reaches.
    ///
    /// Checked by breaking it: signing `content-length` as it stands, without the `"0" => ""`
    /// substitution, makes the zero-length request sign differently from the bodiless one and the
    /// second assertion fails.
    #[test]
    fn a_zero_length_body_is_signed_as_an_empty_field_and_a_body_by_its_length() {
        let policy = build_policy();
        let mut with_body = build_request("http://myaccount.blob.core.windows.net/c/b", Method::Put);
        with_body.insert_header("content-length", "5");
        let mut zero_length = build_request("http://myaccount.blob.core.windows.net/c/b", Method::Put);
        zero_length.insert_header("content-length", "0");
        let bodiless = build_request("http://myaccount.blob.core.windows.net/c/b", Method::Put);

        let signed_with_body = policy.build_string_to_sign(&with_body);

        assert_eq!(
            signed_with_body.lines().nth(3),
            Some("5"),
            "the fourth slot carries the length of the body, got: {signed_with_body:?}"
        );
        assert_eq!(
            policy.build_string_to_sign(&zero_length),
            policy.build_string_to_sign(&bodiless),
            "a zero-length body is signed as an empty field, the way a request carrying no length is"
        );
    }

    #[test]
    fn canonicalized_headers_are_sorted_by_name() {
        let policy = build_policy();
        let mut ascending = build_request("http://myaccount.blob.core.windows.net/c/b", Method::Get);
        ascending.insert_header("x-ms-client-request-id", "id");
        ascending.insert_header("x-ms-version", "2015-02-21");
        let mut descending = build_request("http://myaccount.blob.core.windows.net/c/b", Method::Get);
        descending.insert_header("x-ms-version", "2015-02-21");
        descending.insert_header("x-ms-client-request-id", "id");

        assert_eq!(
            policy.build_string_to_sign(&ascending),
            policy.build_string_to_sign(&descending),
            "the order headers were added in must not reach the signature"
        );
    }

    /// A listing addresses the same path as the container itself and differs only by its query, so
    /// a resource built from the path alone would sign two different requests identically.
    #[test]
    fn a_canonicalized_resource_carries_query_parameters() {
        let policy = build_policy();
        let plain = build_request("http://myaccount.blob.core.windows.net/mycontainer", Method::Get);
        let listing = build_request(
            "http://myaccount.blob.core.windows.net/mycontainer?comp=list&restype=container&maxresults=1",
            Method::Get,
        );

        assert_ne!(
            policy.build_string_to_sign(&plain),
            policy.build_string_to_sign(&listing)
        );
    }

    /// A path segment that had to be escaped in the URL - a job code with a space - is signed
    /// escaped, which is how the service canonicalizes what it received.
    #[test]
    fn a_canonicalized_resource_carries_the_encoded_path() {
        let policy = build_policy();
        let request = build_request(
            "http://myaccount.blob.core.windows.net/mycontainer/jobs/simple%20job/state.json",
            Method::Get,
        );

        assert!(
            policy
                .build_string_to_sign(&request)
                .ends_with("/myaccount/mycontainer/jobs/simple%20job/state.json"),
            "got: {}",
            policy.build_string_to_sign(&request)
        );
    }

    #[test]
    fn an_account_key_that_is_not_base64_is_refused() {
        let error = SharedKeySigningPolicy::try_new("myaccount", &Secret::new("not base64!"))
            .expect_err("a key that does not decode must not build a policy");

        assert!(matches!(error, StorageError::Auth(_)), "got: {error}");
    }
}
