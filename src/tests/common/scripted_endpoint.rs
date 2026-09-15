// A local endpoint that answers a fixed script of HTTP responses, shared by the backend cases that
// state what a provider's client put on the wire and what it made of an answer only a canned one
// can carry.
//
// Every backend reaches this through the `endpoint` its own configuration already takes, which is
// why one helper serves all three: what a case scripts is the store, not the transport of one SDK.

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

/// An endpoint that answers each request with the next scripted response and keeps the targets it
/// was asked for.
///
/// The clients under test are the providers' own, over URLs this crate builds, so what the endpoint
/// received is the only place the URL - the conditions, the page token, the listing boundary - is
/// observable; and a canned answer is the only way to reach a store that names no version or refuses
/// a creation, neither of which a real store can be made to do on demand.
pub struct ScriptedEndpoint {
    endpoint: String,
    /// Kept only for [`ScriptedEndpoint::rewrite_first_response`], which one provider's cases need
    /// and the others do not.
    #[cfg(feature = "storage-gcs")]
    responses: Arc<Mutex<Vec<String>>>,
    requested_targets: Arc<Mutex<Vec<String>>>,
    accepting: JoinHandle<()>,
}

impl ScriptedEndpoint {
    /// Serves `responses` in order; a request past the end of the script is answered `500`, so a
    /// case that sent one more request than it scripted fails rather than hangs.
    pub async fn start(responses: Vec<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a loopback listener must bind");
        let endpoint = format!(
            "http://{}",
            listener.local_addr().expect("a bound listener has an address")
        );
        let requested_targets = Arc::new(Mutex::new(Vec::new()));
        let responses = Arc::new(Mutex::new(responses));

        let served_targets = Arc::clone(&requested_targets);
        let served_responses = Arc::clone(&responses);
        let accepting = tokio::spawn(async move {
            let mut served = 0;
            while let Ok((connection, _)) = listener.accept().await {
                let response = served_responses
                    .lock()
                    .get(served)
                    .cloned()
                    .unwrap_or_else(|| build_http_response(500, &[], "unscripted request"));
                served += 1;
                serve_one_request(connection, &response, &served_targets).await;
            }
        });

        Self {
            endpoint,
            #[cfg(feature = "storage-gcs")]
            responses,
            requested_targets,
            accepting,
        }
    }

    /// Address the scripted store answers on, in the shape each configuration's `endpoint` takes.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Targets the endpoint was asked for, in the order it served them: each one the whole target of
    /// a request line, from the path to the end of the query, and each recorded before the answer to
    /// it was written - so a call that has come back has its target here already.
    pub fn requested_targets(&self) -> Vec<String> {
        self.requested_targets.lock().clone()
    }

    /// Replaces the first scripted response, for the one case whose answer has to name the address
    /// the endpoint was given - which is only known once it is bound.
    // Only the Google Cloud Storage redirect case answers with the endpoint's own address; the other
    // two providers' scripts are known before the listener binds.
    #[cfg(feature = "storage-gcs")]
    pub fn rewrite_first_response(&self, response: String) {
        self.responses.lock()[0] = response;
    }
}

impl Drop for ScriptedEndpoint {
    fn drop(&mut self) {
        self.accepting.abort();
    }
}

/// Reads one HTTP request whole, records the target it asked for in `served_targets` and answers it
/// with `response`.
///
/// The body is read before the answer is written because a client whose request was refused
/// mid-send reports a transport failure, which is a different case from the status under test.
///
/// The target is recorded before the answer rather than after it: a case reads the targets the
/// moment the call under test comes back, and everything from the first byte of the answer onwards
/// is a point at which the client's task may run to that read while this one has not resumed.
async fn serve_one_request(mut connection: TcpStream, response: &str, served_targets: &Mutex<Vec<String>>) {
    let mut received = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = connection.read(&mut chunk).await.unwrap_or(0);
        if read == 0 {
            break;
        }
        received.extend_from_slice(&chunk[..read]);

        let text = String::from_utf8_lossy(&received).to_string();
        let Some(header_end) = text.find("\r\n\r\n") else {
            continue;
        };
        let declared_body_len = text
            .to_lowercase()
            .split("content-length:")
            .nth(1)
            .and_then(|rest| rest.split("\r\n").next())
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(0);
        if received.len() >= header_end + 4 + declared_body_len {
            break;
        }
    }

    let request = String::from_utf8_lossy(&received).to_string();
    let target = request.split_whitespace().nth(1).unwrap_or("<no target>").to_string();
    served_targets.lock().push(target);

    let _ = connection.write_all(response.as_bytes()).await;
    let _ = connection.shutdown().await;
}

/// One HTTP/1.1 response, closed rather than kept alive so each request opens its own connection and
/// the script stays in step with the requests.
pub fn build_http_response(status: u16, headers: &[(&str, &str)], body: &str) -> String {
    let mut named_headers = String::new();
    for (name, value) in headers {
        named_headers.push_str(name);
        named_headers.push_str(": ");
        named_headers.push_str(value);
        named_headers.push_str("\r\n");
    }

    format!(
        "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n{named_headers}\r\n{body}",
        body.len()
    )
}
