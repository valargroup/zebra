//! Zakura P2P dependency, identity, handshake, and protocol-handler scaffolding.
//!
//! This module reserves the iroh dependency, privacy-preserving endpoint posture,
//! persistent identity storage surface, and bounded Zakura handshake wire types.

use iroh::{endpoint, Endpoint, RelayMode, SecretKey};
use thiserror::Error;

mod handler;
mod handshake;
#[cfg(any(test, feature = "zakura-testkit"))]
pub mod testkit;
mod trace;

pub use handler::*;
pub use handshake::*;
pub use trace::{
    peer_label as zakura_trace_peer_label, reject_reason_label as zakura_trace_reject_reason_label,
    ZakuraTrace, ZakuraTraceEvent, CONN_TABLE, HANDSHAKE_TABLE, RATELIMIT_TABLE, STREAM_TABLE,
};

#[cfg(any(test, feature = "zakura-testkit"))]
pub(crate) use handler::run_native_initiator_handshake_without_trace as run_native_initiator_handshake;

#[cfg(test)]
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// The pinned iroh version the Zakura P2P plan was verified against.
pub const IROH_VERSION: &str = "0.92.0";

/// Returns an iroh endpoint builder with relays and external address lookup disabled.
///
/// Callers must add direct bind addresses before binding if they do not want the
/// endpoint to listen on iroh's default unspecified sockets.
pub fn direct_endpoint_builder(secret_key: SecretKey) -> endpoint::Builder {
    Endpoint::builder()
        .relay_mode(RelayMode::Disabled)
        .clear_discovery()
        .secret_key(secret_key)
}

/// The result of routing a mutually P2P-v2-capable legacy handshake.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ZakuraUpgradeOutcome {
    /// The peer was authenticated and registered with the Zakura supervisor.
    Upgraded {
        /// The authenticated Zakura/Iroh peer identity.
        peer_id: ZakuraPeerId,
    },

    /// The peer was authenticated, but a better duplicate connection already exists.
    Duplicate {
        /// The authenticated Zakura/Iroh peer identity.
        peer_id: ZakuraPeerId,
    },

    /// The upgrade was rejected neutrally.
    Rejected {
        /// The neutral rejection reason.
        reason: ZakuraRejectReason,
    },
}

/// An error from the Zakura handshake upgrade hook.
#[derive(Error, Debug)]
pub enum ZakuraUpgradeError {
    /// Local config and the remote service bit selected Zakura, but no supervisor exists yet.
    #[error("Zakura P2P v2 upgrade selected but no Zakura handshake connector is available")]
    Unavailable,
}

/// Legacy handshake context passed to the Zakura upgrade connector.
#[derive(Clone, Debug)]
pub struct ZakuraUpgradeRequest {
    /// The local network and resource policy.
    pub local_config: ZakuraHandshakeConfig,
    /// The local legacy Zebra `version` message.
    pub local_version: crate::VersionMessage,
    /// The remote legacy Zebra `version` message.
    pub remote_version: crate::VersionMessage,
    /// The local/remote legacy nonce pair as observed locally.
    pub nonces: ZakuraLegacyNonces,
    /// Whether this side initiated or responded to the legacy TCP connection.
    pub role: ZakuraControlRole,
}

/// Handle used by the legacy handshake to enter the Zakura P2P upgrade path.
#[derive(Clone, Debug, Default)]
pub struct ZakuraHandshakeConnector {
    supervisor: Option<ZakuraSupervisorHandle>,
    trace: ZakuraTrace,
    #[cfg(test)]
    test_outcome: Option<(Arc<AtomicUsize>, ZakuraUpgradeOutcome)>,
}

impl ZakuraHandshakeConnector {
    /// Create a placeholder connector for a node that can advertise P2P v2, but does not yet have
    /// a Zakura supervisor capable of accepting a handoff.
    pub fn unavailable() -> Self {
        Self {
            supervisor: None,
            trace: ZakuraTrace::noop(),
            #[cfg(test)]
            test_outcome: None,
        }
    }

    /// Create a connector backed by the active Zakura supervisor.
    pub fn new(supervisor: ZakuraSupervisorHandle) -> Self {
        Self {
            supervisor: Some(supervisor),
            trace: ZakuraTrace::noop(),
            #[cfg(test)]
            test_outcome: None,
        }
    }

    /// Create a connector backed by the active Zakura supervisor and trace emitter.
    pub(crate) fn new_with_trace(supervisor: ZakuraSupervisorHandle, trace: ZakuraTrace) -> Self {
        Self {
            supervisor: Some(supervisor),
            trace,
            #[cfg(test)]
            test_outcome: None,
        }
    }

    /// Resolve the upgrade outcome for the current legacy handshake.
    pub(crate) async fn upgrade_outcome(
        &self,
        request: ZakuraUpgradeRequest,
    ) -> Result<ZakuraUpgradeOutcome, ZakuraUpgradeError> {
        self.trace.emit(
            HANDSHAKE_TABLE,
            ZakuraTraceEvent::new("prelude.accepted")
                .role(control_role_label(request.role))
                .phase("legacy_upgrade")
                .network(request.local_config.network_label()),
        );

        #[cfg(test)]
        if let Some((calls, outcome)) = &self.test_outcome {
            calls.fetch_add(1, Ordering::SeqCst);
            self.emit_upgrade_outcome(&request, outcome);
            return Ok(outcome.clone());
        }

        let Some(supervisor) = &self.supervisor else {
            self.trace.emit(
                HANDSHAKE_TABLE,
                ZakuraTraceEvent::new("rejected")
                    .role(control_role_label(request.role))
                    .phase("legacy_upgrade")
                    .reason(zakura_trace_reject_reason_label(
                        ZakuraRejectReason::TemporaryUnavailable,
                    ))
                    .network(request.local_config.network_label()),
            );
            return Err(ZakuraUpgradeError::Unavailable);
        };

        let outcome = supervisor
            .register_legacy_upgrade_placeholder(request.clone())
            .await;
        self.emit_upgrade_outcome(&request, &outcome);
        Ok(outcome)
    }

    fn emit_upgrade_outcome(&self, request: &ZakuraUpgradeRequest, outcome: &ZakuraUpgradeOutcome) {
        let event = match outcome {
            ZakuraUpgradeOutcome::Upgraded { .. } => {
                ZakuraTraceEvent::new("upgraded").selected_protocol(ZAKURA_PROTOCOL_VERSION_1)
            }
            ZakuraUpgradeOutcome::Duplicate { .. } => ZakuraTraceEvent::new("duplicate"),
            ZakuraUpgradeOutcome::Rejected { reason } => {
                ZakuraTraceEvent::new("rejected").reason(zakura_trace_reject_reason_label(*reason))
            }
        };

        self.trace.emit(
            HANDSHAKE_TABLE,
            event
                .role(control_role_label(request.role))
                .phase("legacy_upgrade")
                .network(request.local_config.network_label()),
        );
    }

    /// Create a deterministic connector for handshake routing tests.
    #[cfg(test)]
    pub(crate) fn for_test(calls: Arc<AtomicUsize>, outcome: ZakuraUpgradeOutcome) -> Self {
        Self {
            supervisor: None,
            trace: ZakuraTrace::noop(),
            test_outcome: Some((calls, outcome)),
        }
    }
}

fn control_role_label(role: ZakuraControlRole) -> &'static str {
    match role {
        ZakuraControlRole::Initiator => "initiator",
        ZakuraControlRole::Responder => "responder",
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        net::{Ipv4Addr, SocketAddrV4},
        path::PathBuf,
    };

    use iroh::{
        endpoint::Connection,
        protocol::{AcceptError, ProtocolHandler, Router},
        SecretKey, Watcher as _,
    };
    use zebra_chain::parameters::Network;

    use super::*;
    use crate::CacheDir;

    #[derive(Debug, Clone)]
    struct SmokeProtocolHandler;

    impl ProtocolHandler for SmokeProtocolHandler {
        async fn accept(&self, _connection: Connection) -> Result<(), AcceptError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn iroh_endpoint_starts_without_relay_or_discovery() -> Result<(), Box<dyn Error>> {
        let secret_key = SecretKey::from_bytes(&[7; 32]);

        let endpoint = direct_endpoint_builder(secret_key)
            .bind_addr_v4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .bind()
            .await?;

        let router = Router::builder(endpoint)
            .accept(b"/zakura/smoke/0", SmokeProtocolHandler)
            .spawn();

        let addr = router.endpoint().node_addr().initialized().await;

        assert_eq!(addr.node_id, router.endpoint().node_id());
        assert!(addr.direct_addresses().next().is_some());
        assert!(addr.relay_url().is_none());
        assert!(router.endpoint().discovery().is_none());

        router.shutdown().await?;

        Ok(())
    }

    #[test]
    fn zakura_secret_key_path_uses_network_cache_dir() {
        let cache_dir = CacheDir::custom_path("/tmp/zebra-cache");

        assert_eq!(
            cache_dir
                .zakura_node_secret_key_file_path(&Network::Mainnet)
                .expect("custom cache path should be enabled"),
            PathBuf::from("/tmp/zebra-cache/network/mainnet.zakura-iroh-secret-key")
        );
    }
}
