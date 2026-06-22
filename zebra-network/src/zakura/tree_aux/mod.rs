//! Native Zakura `tree_aux` stream: verified per-block commitment roots for the fast
//! checkpoint-sync path (`docs/design/verified-commitment-trees.md` §5.4).
//!
//! A **roots-only request/response stream**: a client sends `GetRoots` and the server
//! answers with the per-block commitment roots from its local state. The checkpoint
//! final frontier is embedded in the binary, not carried here. The stream carries no
//! trust — every recipient re-verifies served roots against its own checkpoint-committed
//! headers before the fast path folds them in.
//!
//! Templated on `header_sync` / `legacy_gossip`, but much smaller: it is a one-shot
//! `RequestResponseService`, so it needs no ordered-stream reactor/scheduler.

use super::{
    BoxRunFuture, Frame, Peer, RequestResponseService, Service as ZakuraService, SinkReject,
    Stream, StreamMode, ZakuraPeerId, FRAME_HEADER_BYTES,
};

mod driver;
mod service;
#[cfg(test)]
mod tests;
mod wire;

pub use driver::fetch_roots;
pub use service::{TreeAuxService, TreeAuxStatePort};
pub use wire::*;
