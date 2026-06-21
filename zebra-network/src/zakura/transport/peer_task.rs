//! Supervised per-peer routine/task launchers shared by every Zakura service.
//!
//! A *peer routine* is the single async function that drives one peer's ordered
//! stream from connect to disconnect. Each service owns its own concrete routine
//! (`HeaderSyncPeerRoutine`, block-sync `PeerRoutine`, the discovery routine);
//! this module owns only the cross-service supervision vocabulary those concrete
//! routines launch through — the panic-containment launcher
//! ([`spawn_supervised_routine`]), its non-routine sibling
//! ([`spawn_supervised_peer_task`]), and the single connection-teardown decision
//! ([`handle_routine_exit`]). Services depend on this supervision but never
//! re-implement it.
//!
//! The generic pipe/DAG data-plane vocabulary that once lived here (the per-stage
//! `Flow`, the per-peer context, the checked-documentation DAG, the generic
//! inbound runner) was retired once every service owned a concrete routine that
//! decodes and dispatches its own frames inline. What remains is the supervision
//! concept, which is not service-specific and is not removable.

use std::future::Future;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::SinkReject;
use crate::zakura::ZakuraPeerId;

/// Cleanup that runs when a supervised peer routine ends, on every exit path.
///
/// Living in `Drop`, the teardown runs whether the routine future returns
/// normally, returns after a reject, or unwinds on a panic — all from within the
/// single spawned routine task, with no second `tokio::spawn`. This depends on
/// the build unwinding rather than aborting: under `panic = "unwind"` the `Drop`
/// runs as the panic unwinds the task and tokio catches the panic at the task
/// boundary, so the blast radius is exactly one peer (security_requirements.md
/// SR-1). The `#[cfg(panic = "abort")] compile_error!` above
/// [`spawn_supervised_routine`] refuses to build the node with abort, where this
/// `Drop` could not run and a single peer panic would kill the whole node.
struct PeerRoutineTeardown<F: FnOnce(), P: FnOnce()> {
    peer_id: ZakuraPeerId,
    cancel: CancellationToken,
    on_teardown: Option<F>,
    on_panic: Option<P>,
}

impl<F: FnOnce(), P: FnOnce()> Drop for PeerRoutineTeardown<F, P> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            metrics::counter!("zakura.pipe.panic").increment(1);
            tracing::error!(
                peer_id = ?self.peer_id,
                "Zakura peer routine panicked; disconnecting peer only"
            );
            if let Some(on_panic) = self.on_panic.take() {
                on_panic();
            }
        }
        self.cancel.cancel();
        if let Some(on_teardown) = self.on_teardown.take() {
            on_teardown();
        }
    }
}

// Peer-routine panic containment (security_requirements.md SR-1) depends on
// unwinding: `PeerRoutineTeardown`'s `Drop` runs during the unwind to disconnect
// and clean up the panicked peer, and tokio catches the task panic so the blast
// radius is one peer. Under `panic = "abort"` none of that can happen — a single
// peer panic aborts the whole node — so refuse to build that way. The workspace
// [profile.*] tables set `panic = "unwind"`; this guard catches a silent
// regression back to abort.
#[cfg(panic = "abort")]
compile_error!(
    "Zakura peer-routine panic containment requires `panic = \"unwind\"` \
     (security_requirements.md SR-1); this build sets `panic = \"abort\"`. \
     Set panic = \"unwind\" in the workspace [profile.dev] and [profile.release]."
);

/// Launch a per-peer routine in its own supervised task.
///
/// This is the single way concrete peer routines are launched. The routine runs
/// inside one spawned task guarded by a [`PeerRoutineTeardown`], which cancels the
/// caller-supplied `cancel` token and runs `on_teardown` (which must be
/// idempotent) on every exit path — normal return, reject, or panic. On panic
/// only, it also runs `on_panic`. Callers pass the token whose cancellation is
/// safe on *every* exit (e.g. a per-service token, not a shared connection token
/// that other services ride on); connection-level teardown that must only happen
/// on a fatal reject belongs inside the `routine` future (see
/// [`handle_routine_exit`]), and connection-level teardown for panic belongs in
/// `on_panic`. A panicking peer is contained to its own task without a nested
/// `tokio::spawn`.
///
/// Returns the task's [`JoinHandle`]: services let it drop to detach the task (it
/// self-reaps; the `PeerRoutineTeardown` still runs on every exit), while the
/// panic-containment test awaits it to observe the contained panic.
pub(crate) fn spawn_supervised_routine(
    peer_id: ZakuraPeerId,
    cancel: CancellationToken,
    on_teardown: impl FnOnce() + Send + 'static,
    on_panic: impl FnOnce() + Send + 'static,
    routine: impl Future<Output = ()> + Send + 'static,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _teardown = PeerRoutineTeardown {
            peer_id,
            cancel,
            on_teardown: Some(on_teardown),
            on_panic: Some(on_panic),
        };
        routine.await;
    })
}

/// Cleanup that runs when a supervised non-routine peer task ends, on every exit
/// path.
///
/// This is the task-level sibling of [`PeerRoutineTeardown`] for the
/// peer-influenced service tasks that are not a per-peer routine — e.g. the
/// discovery source/admission helpers and the legacy gossip replay/receive loops.
/// Unlike [`PeerRoutineTeardown`] it owns no [`CancellationToken`] of its own:
/// those tasks decide which token(s) a panic must cancel from inside their
/// `on_panic` hook, because some of them ride directly on the shared connection
/// token, which must *not* be cancelled on a normal exit (a clean stream-end of
/// one service must not tear the whole connection down). On panic it runs
/// `on_panic` during the unwind; on every exit it runs `on_teardown`.
struct PeerTaskTeardown<F: FnOnce(), P: FnOnce()> {
    peer_id: ZakuraPeerId,
    on_teardown: Option<F>,
    on_panic: Option<P>,
}

impl<F: FnOnce(), P: FnOnce()> Drop for PeerTaskTeardown<F, P> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            metrics::counter!("zakura.pipe.panic").increment(1);
            tracing::error!(
                peer_id = ?self.peer_id,
                "Zakura peer task panicked; disconnecting peer only"
            );
            if let Some(on_panic) = self.on_panic.take() {
                on_panic();
            }
        }
        if let Some(on_teardown) = self.on_teardown.take() {
            on_teardown();
        }
    }
}

/// Launch a peer-influenced, non-routine service task in its own supervised task.
///
/// This is the task-level counterpart to [`spawn_supervised_routine`] for the
/// peer-driven helper tasks that are *not* a per-peer routine (discovery
/// source/admission, legacy gossip replay/receive). The task runs inside one
/// spawned task guarded by a [`PeerTaskTeardown`], which runs `on_teardown` (which
/// must be idempotent) on every exit path — normal return or panic — and
/// `on_panic` on panic only. Callers put whatever connection / service
/// cancellation a panic requires inside `on_panic`, so a buggy or hostile peer
/// that panics one of these tasks still disconnects *that one peer* and runs its
/// cleanup instead of leaving stale service state behind a half-live connection
/// (security_requirements.md SR-1). Like [`spawn_supervised_routine`] this depends
/// on the build unwinding rather than aborting; the `#[cfg(panic = "abort")]
/// compile_error!` above guards that for both wrappers.
///
/// Returns the task's [`JoinHandle`]: services let it drop to detach the task (it
/// self-reaps; the `PeerTaskTeardown` still runs on every exit), while the
/// panic-containment test awaits it to observe the contained panic.
pub(crate) fn spawn_supervised_peer_task(
    peer_id: ZakuraPeerId,
    on_teardown: impl FnOnce() + Send + 'static,
    on_panic: impl FnOnce() + Send + 'static,
    task: impl Future<Output = ()> + Send + 'static,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _teardown = PeerTaskTeardown {
            peer_id,
            on_teardown: Some(on_teardown),
            on_panic: Some(on_panic),
        };
        task.await;
    })
}

/// Map a finished routine run to its connection-teardown effect — the single place
/// the "is this exit fatal to the whole connection?" decision lives.
///
/// A protocol reject is fatal: it cancels the shared `connection_cancel` token so
/// the whole connection tears down. A local reject (e.g. a closed service queue)
/// tears down only this stream — the per-service token is already cancelled by
/// the [`PeerRoutineTeardown`] — so it is logged and the connection is left for
/// other services. `Ok` is a normal/parked exit and does nothing here. Panic-path
/// connection teardown is separate (`on_panic`), because a panic never returns a
/// `Result` to inspect.
pub(crate) fn handle_routine_exit(
    service: &'static str,
    connection_cancel: &CancellationToken,
    result: Result<(), SinkReject>,
) {
    match result {
        Ok(()) => {}
        Err(SinkReject::Protocol(error)) => {
            tracing::debug!(
                ?error,
                service,
                "Zakura stream rejected protocol-invalid frame"
            );
            connection_cancel.cancel();
        }
        Err(SinkReject::Local(error)) => {
            tracing::debug!(?error, service, "Zakura stream stopped on local error");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use super::*;

    fn peer_id() -> ZakuraPeerId {
        ZakuraPeerId::new(vec![1, 2, 3]).expect("3-byte id is within the node-id bound")
    }

    #[tokio::test]
    async fn supervised_routine_runs_teardown_on_panic() {
        let cancel = CancellationToken::new();
        let torn_down = Arc::new(AtomicBool::new(false));
        let flag = torn_down.clone();
        let panic_disconnected = Arc::new(AtomicBool::new(false));
        let panic_flag = panic_disconnected.clone();

        // Production drops this handle to detach the task; here we await it to
        // observe the contained panic.
        let handle = spawn_supervised_routine(
            peer_id(),
            cancel.clone(),
            move || flag.store(true, Ordering::SeqCst),
            move || panic_flag.store(true, Ordering::SeqCst),
            async {
                panic!("peer routine panics");
            },
        );

        // The routine runs in a single task with no nested unwind-isolation spawn,
        // so the task itself surfaces the panic to its `JoinHandle`. The
        // `Drop`-based teardown still runs during the unwind, so cleanup is
        // guaranteed and the panic is contained to this one task.
        let join_error = handle
            .await
            .expect_err("a panicking routine surfaces a join error");
        assert!(
            join_error.is_panic(),
            "the routine panic is reported as a panic, not a cancellation"
        );

        assert!(
            torn_down.load(Ordering::SeqCst),
            "teardown runs even when the routine panics"
        );
        assert!(
            panic_disconnected.load(Ordering::SeqCst),
            "panic-only disconnect hook runs when the routine panics"
        );
        assert!(cancel.is_cancelled(), "the peer connection is cancelled");
    }

    #[tokio::test]
    async fn supervised_peer_task_runs_teardown_and_disconnect_on_panic() {
        // The surprising input: a peer-influenced helper task (e.g. discovery
        // source/admission, legacy gossip recv loop) panics *after* its per-peer
        // service state is registered, before its normal-path cleanup runs.
        let torn_down = Arc::new(AtomicBool::new(false));
        let teardown_flag = torn_down.clone();
        let disconnected = Arc::new(AtomicBool::new(false));
        let disconnect_flag = disconnected.clone();

        // Production drops this handle to detach the task; here we await it to
        // observe the contained panic.
        let handle = spawn_supervised_peer_task(
            peer_id(),
            move || teardown_flag.store(true, Ordering::SeqCst),
            move || disconnect_flag.store(true, Ordering::SeqCst),
            async {
                panic!("peer task panics after state registration");
            },
        );

        let join_error = handle
            .await
            .expect_err("a panicking peer task surfaces a join error");
        assert!(
            join_error.is_panic(),
            "the task panic is reported as a panic, not a cancellation"
        );
        // The safe expectation (SR-1): the panic still ran cleanup and the
        // peer-disconnect hook, so no stale service state survives behind a
        // half-live connection.
        assert!(
            torn_down.load(Ordering::SeqCst),
            "teardown runs even when the peer task panics"
        );
        assert!(
            disconnected.load(Ordering::SeqCst),
            "panic-only disconnect hook runs when the peer task panics"
        );
    }

    #[tokio::test]
    async fn supervised_peer_task_skips_disconnect_on_normal_exit() {
        let torn_down = Arc::new(AtomicBool::new(false));
        let teardown_flag = torn_down.clone();
        let disconnected = Arc::new(AtomicBool::new(false));
        let disconnect_flag = disconnected.clone();

        let handle = spawn_supervised_peer_task(
            peer_id(),
            move || teardown_flag.store(true, Ordering::SeqCst),
            move || disconnect_flag.store(true, Ordering::SeqCst),
            async {},
        );

        handle
            .await
            .expect("a normal peer task exit does not panic");
        assert!(
            torn_down.load(Ordering::SeqCst),
            "teardown runs on a normal exit"
        );
        // A clean exit (e.g. one service's stream ends) must NOT trip the
        // panic-only disconnect — that would tear down peers that rode on the
        // same connection.
        assert!(
            !disconnected.load(Ordering::SeqCst),
            "the panic-only disconnect hook must not fire on a normal exit"
        );
    }

    #[tokio::test]
    async fn supervised_routine_runs_teardown_and_cancels_on_normal_return() {
        // The non-panic exit path: the routine future returns normally (e.g. its
        // stream closed). The `PeerRoutineTeardown` must still run `on_teardown`
        // and cancel the service token it owns, while leaving the panic-only hook
        // untouched.
        let cancel = CancellationToken::new();
        let torn_down = Arc::new(AtomicBool::new(false));
        let teardown_flag = torn_down.clone();
        let panicked = Arc::new(AtomicBool::new(false));
        let panic_flag = panicked.clone();

        let handle = spawn_supervised_routine(
            peer_id(),
            cancel.clone(),
            move || teardown_flag.store(true, Ordering::SeqCst),
            move || panic_flag.store(true, Ordering::SeqCst),
            async {},
        );

        handle.await.expect("a normal routine exit does not panic");
        assert!(
            torn_down.load(Ordering::SeqCst),
            "teardown runs on a normal routine return"
        );
        assert!(
            cancel.is_cancelled(),
            "the service cancellation token is cancelled on a normal return"
        );
        // The panic-only hook is for connection-level teardown a panic requires;
        // a clean return must not fire it.
        assert!(
            !panicked.load(Ordering::SeqCst),
            "the panic-only hook must not run on a normal return"
        );
    }

    #[tokio::test]
    async fn supervised_routine_cancels_service_token_on_protocol_reject() {
        // Compose a routine future that protocol-rejects with `handle_routine_exit`
        // exactly as the services do. A protocol reject returns
        // `Err(SinkReject::Protocol)`, but `spawn_supervised_routine`'s own
        // `PeerRoutineTeardown` always cancels the *service* token it was handed,
        // regardless of the reject variant, while `handle_routine_exit` cancels the
        // shared connection token only on the protocol reject.
        let service_cancel = CancellationToken::new();
        let connection_cancel = CancellationToken::new();
        let torn_down = Arc::new(AtomicBool::new(false));
        let teardown_flag = torn_down.clone();

        let connection_cancel_in_future = connection_cancel.clone();
        let routine_future = async move {
            handle_routine_exit(
                "test",
                &connection_cancel_in_future,
                Err(SinkReject::protocol("bad frame")),
            );
        };

        let handle = spawn_supervised_routine(
            peer_id(),
            service_cancel.clone(),
            move || teardown_flag.store(true, Ordering::SeqCst),
            || {},
            routine_future,
        );

        handle.await.expect("a protocol reject is not a panic");
        assert!(
            torn_down.load(Ordering::SeqCst),
            "teardown runs on a protocol reject"
        );
        assert!(
            service_cancel.is_cancelled(),
            "the service token is cancelled on a protocol reject"
        );
        assert!(
            connection_cancel.is_cancelled(),
            "a protocol reject cancels the shared connection token"
        );
    }

    #[tokio::test]
    async fn supervised_routine_local_reject_leaves_connection_token_alive() {
        // A local reject (the peer is not at fault) still cancels this peer's own
        // service token via the `PeerRoutineTeardown`, but must NOT cancel the
        // shared connection token: other services riding the same connection keep
        // going.
        let service_cancel = CancellationToken::new();
        let connection_cancel = CancellationToken::new();
        let torn_down = Arc::new(AtomicBool::new(false));
        let teardown_flag = torn_down.clone();

        let connection_cancel_in_future = connection_cancel.clone();
        let routine_future = async move {
            handle_routine_exit(
                "test",
                &connection_cancel_in_future,
                Err(SinkReject::local("closed queue")),
            );
        };

        let handle = spawn_supervised_routine(
            peer_id(),
            service_cancel.clone(),
            move || teardown_flag.store(true, Ordering::SeqCst),
            || {},
            routine_future,
        );

        handle.await.expect("a local reject is not a panic");
        assert!(
            torn_down.load(Ordering::SeqCst),
            "teardown runs on a local reject"
        );
        assert!(
            service_cancel.is_cancelled(),
            "the service token is still cancelled on a local reject"
        );
        assert!(
            !connection_cancel.is_cancelled(),
            "a local reject must not cancel the shared connection token"
        );
    }

    #[tokio::test]
    async fn supervised_routine_normal_stream_close_leaves_shared_connection_alive() {
        // The clean-exit case for a multi-service connection: one service's stream
        // ends, its routine returns `Ok(())`. The service token is cancelled (only
        // this service parks), but the shared connection token other services ride
        // on must stay alive.
        let service_cancel = CancellationToken::new();
        let connection_cancel = CancellationToken::new();

        let connection_cancel_in_future = connection_cancel.clone();
        let routine_future = async move {
            handle_routine_exit("test", &connection_cancel_in_future, Ok(()));
        };

        let handle = spawn_supervised_routine(
            peer_id(),
            service_cancel.clone(),
            || {},
            || {},
            routine_future,
        );

        handle.await.expect("a normal stream close is not a panic");
        assert!(
            service_cancel.is_cancelled(),
            "the service token is cancelled when this service's stream closes"
        );
        assert!(
            !connection_cancel.is_cancelled(),
            "a clean stream close must not tear down the shared connection token"
        );
    }

    #[test]
    fn handle_routine_exit_cancels_connection_on_protocol_reject() {
        // The single decision point: a protocol reject is fatal to the whole
        // connection, so it cancels the shared connection token.
        let connection_cancel = CancellationToken::new();
        handle_routine_exit(
            "test",
            &connection_cancel,
            Err(SinkReject::protocol("bad frame")),
        );
        assert!(
            connection_cancel.is_cancelled(),
            "a protocol reject cancels the connection token"
        );
    }

    #[test]
    fn handle_routine_exit_leaves_connection_on_local_reject() {
        // A local reject tears down only this stream (already handled by the
        // per-service token), never the shared connection.
        let connection_cancel = CancellationToken::new();
        handle_routine_exit(
            "test",
            &connection_cancel,
            Err(SinkReject::local("closed queue")),
        );
        assert!(
            !connection_cancel.is_cancelled(),
            "a local reject must not cancel the connection token"
        );
    }

    #[test]
    fn handle_routine_exit_leaves_connection_on_normal_exit() {
        // A normal/parked exit does nothing to the connection token.
        let connection_cancel = CancellationToken::new();
        handle_routine_exit("test", &connection_cancel, Ok(()));
        assert!(
            !connection_cancel.is_cancelled(),
            "a normal exit must not cancel the connection token"
        );
    }
}
