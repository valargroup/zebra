//! The peer message sender channel.

#[cfg(feature = "p2p-tracing")]
use std::sync::Arc;

use futures::{FutureExt, Sink, SinkExt};

use zebra_chain::serialization::SerializationError;

use crate::{constants::REQUEST_TIMEOUT, protocol::external::Message, PeerError};

/// A wrapper type for a peer connection message sender.
///
/// Used to apply a timeout to send messages.
#[derive(Clone, Debug)]
pub struct PeerTx<Tx>
where
    Tx: Sink<Message, Error = SerializationError> + Unpin,
{
    /// A channel for sending Zcash messages to the connected peer.
    ///
    /// This channel accepts [`Message`]s.
    inner: Tx,

    /// Send-path timing tracer.
    #[cfg(feature = "p2p-tracing")]
    send_timing_tracer: crate::send_timing::SendTimingTracer,

    /// Peer address label for tracing.
    #[cfg(feature = "p2p-tracing")]
    peer_label: Arc<str>,

    /// Connection ID for tracing.
    #[cfg(feature = "p2p-tracing")]
    conn_id: u64,
}

impl<Tx> PeerTx<Tx>
where
    Tx: Sink<Message, Error = SerializationError> + Unpin,
{
    /// Create a new `PeerTx` with send-timing tracing metadata.
    #[cfg(feature = "p2p-tracing")]
    pub fn new(
        tx: Tx,
        send_timing_tracer: crate::send_timing::SendTimingTracer,
        peer_label: Arc<str>,
        conn_id: u64,
    ) -> Self {
        PeerTx {
            inner: tx,
            send_timing_tracer,
            peer_label,
            conn_id,
        }
    }

    /// Sends `msg` on `self.inner`, returning a timeout error if it takes too long.
    pub async fn send(&mut self, msg: Message) -> Result<(), PeerError> {
        #[cfg(feature = "p2p-tracing")]
        let command = msg.command();
        #[cfg(feature = "p2p-tracing")]
        let start = std::time::Instant::now();

        let result = tokio::time::timeout(REQUEST_TIMEOUT, self.inner.send(msg))
            .await
            .map_err(|_| PeerError::ConnectionSendTimeout)?
            .map_err(Into::into);

        #[cfg(feature = "p2p-tracing")]
        self.send_timing_tracer.record(
            "sink_send",
            command,
            &self.peer_label,
            self.conn_id,
            start.elapsed(),
            None,
        );

        result
    }

    /// Flush any remaining output and close this [`PeerTx`], if necessary.
    pub async fn close(&mut self) -> Result<(), SerializationError> {
        self.inner.close().await
    }
}

impl<Tx> From<Tx> for PeerTx<Tx>
where
    Tx: Sink<Message, Error = SerializationError> + Unpin,
{
    fn from(tx: Tx) -> Self {
        PeerTx {
            inner: tx,
            #[cfg(feature = "p2p-tracing")]
            send_timing_tracer: crate::send_timing::SendTimingTracer::noop(),
            #[cfg(feature = "p2p-tracing")]
            peer_label: Arc::from("unknown"),
            #[cfg(feature = "p2p-tracing")]
            conn_id: 0,
        }
    }
}

impl<Tx> Drop for PeerTx<Tx>
where
    Tx: Sink<Message, Error = SerializationError> + Unpin,
{
    fn drop(&mut self) {
        // Do a last-ditch close attempt on the sink
        self.close().now_or_never();
    }
}
