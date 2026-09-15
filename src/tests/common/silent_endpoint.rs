// A local endpoint that accepts a connection and answers nothing, shared by the backend cases that
// measure what a request does when no answer comes back.

use std::time::Duration;

use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

/// How long a case lets a request stand before cancelling it - far below the request timeout those
/// cases configure, so which of the two bounds ended the wait is never in doubt.
pub const CANCELLATION_DELAY: Duration = Duration::from_millis(100);

/// Starts a listener that accepts a connection and answers nothing, answering with its address and
/// the task accepting on it.
///
/// This is the failure the timeout exists for and the one a cancellation has to cut through: the
/// connection is established, so neither the transport nor the operating system reports anything,
/// and a request waits for as long as it is allowed to.
///
/// The address comes back as a bare `http://host:port`, which each backend shapes into the url its
/// own provider is addressed by.
pub async fn start_silent_endpoint() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a loopback listener must bind");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("a bound listener has an address")
    );
    let accepting = tokio::spawn(async move {
        while let Ok((connection, _)) = listener.accept().await {
            // Each connection is parked rather than dropped: closing one would answer the request
            // with a transport failure, which is the opposite of the case under test.
            tokio::spawn(async move {
                let _held = connection;
                std::future::pending::<()>().await;
            });
        }
    });

    (endpoint, accepting)
}

/// Cancels `cancel_token` once [`CANCELLATION_DELAY`] has passed, so the request under test is
/// cancelled while it is in flight rather than before it starts.
pub fn cancel_after_delay(cancel_token: &CancellationToken) {
    let cancel_token = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(CANCELLATION_DELAY).await;
        cancel_token.cancel();
    });
}
