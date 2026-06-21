use super::{
    config::*,
    error::*,
    events::*,
    scheduler::*,
    service::{HeaderSyncCommandOutcome, HeaderSyncCommandSink},
    state::*,
    validation::*,
    wire::*,
    *,
};
use crate::zakura::{
    FrontierChange, FrontierUpdate, HeaderSyncServiceSummary, ServiceAdmissionDecision,
    ServicePeerDirection, ServicePeerSnapshot, ZakuraHeaderSyncCandidateState,
};

/// Upper bound on how long the reactor will wait to enqueue a data-plane action
/// before abandoning it. The bounded `actions` channel is normally drained by
/// the action driver almost immediately; this deadline only trips when that
/// driver is genuinely stalled on backend/verifier work, and it keeps a stalled
/// driver from wedging the reactor's control plane — peer-lifecycle draining,
/// request timeouts, and above all misbehavior disconnects.
const ACTION_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Spawn a header-sync reactor and return its handle plus action stream.
pub fn spawn_header_sync_reactor(
    startup: HeaderSyncStartup,
) -> Result<
    (
        HeaderSyncHandle,
        mpsc::Receiver<HeaderSyncAction>,
        JoinHandle<()>,
    ),
    HeaderSyncStartError,
> {
    let state = HeaderSyncCore::new(&startup)?;
    let (events_tx, events_rx) = mpsc::channel(128);
    let (lifecycle_tx, lifecycle_rx) = mpsc::unbounded_channel();
    let (actions_tx, actions_rx) = mpsc::channel(128);
    let (tip_tx, tip_rx) = watch::channel((state.best_header_tip, state.best_header_hash));
    let (peers_tx, peers_rx) =
        watch::channel(ServicePeerSnapshot::new(0, 0, startup.config.peer_limits));
    let (candidates_tx, candidates_rx) = watch::channel(ZakuraHeaderSyncCandidateState {
        target_height: header_sync_candidate_target(state.best_header_tip),
        admitted_node_ids: Vec::new(),
        backed_off_node_ids: Vec::new(),
    });
    let handle = HeaderSyncHandle {
        events: events_tx,
        lifecycle: lifecycle_tx,
        tip: tip_rx,
        peers: peers_rx,
        candidates: candidates_rx,
        // Hand routines a clone of the shared range queue so they can pull
        // `GetHeaders` work directly; the reactor produces work into the same
        // queue via `schedule()`/`mark_covered`/reset.
        schedule: state.schedule.clone(),
    };
    let reactor = HeaderSyncReactor {
        startup,
        state,
        events: events_rx,
        lifecycle: lifecycle_rx,
        actions: actions_tx,
        tip: tip_tx,
        peers: peers_tx,
        candidates: candidates_tx,
    };
    let task = tokio::spawn(reactor.run());

    Ok((handle, actions_rx, task))
}

#[derive(Debug)]
pub(super) struct HeaderSyncReactor {
    startup: HeaderSyncStartup,
    state: HeaderSyncCore,
    events: mpsc::Receiver<HeaderSyncEvent>,
    lifecycle: mpsc::UnboundedReceiver<HeaderSyncEvent>,
    actions: mpsc::Sender<HeaderSyncAction>,
    tip: watch::Sender<(block::Height, block::Hash)>,
    peers: watch::Sender<ServicePeerSnapshot>,
    candidates: watch::Sender<ZakuraHeaderSyncCandidateState>,
}

impl HeaderSyncReactor {
    async fn run(mut self) {
        let mut frontier_updates = self.startup.frontier_updates.clone();
        let mut frontier_updates_open = frontier_updates.is_some();
        if self.startup.range_state_actions_enabled {
            let _ = self
                .dispatch_action(HeaderSyncAction::QueryBestHeaderTip)
                .await;
            let _ = self
                .dispatch_action(HeaderSyncAction::QueryMissingBlockBodies {
                    from: next_height(self.state.verified_block_tip)
                        .unwrap_or(self.state.verified_block_tip),
                    limit: DEFAULT_HS_RANGE,
                })
                .await;
        }

        let mut ticks = time::interval(self.empty_headers_retry_delay());
        loop {
            tokio::select! {
                biased;
                _ = self.startup.shutdown.cancelled() => {
                    break;
                }
                event = self.lifecycle.recv() => {
                    let Some(event) = event else {
                        break;
                    };
                    self.handle_event(event).await;
                }
                event = self.events.recv() => {
                    let Some(event) = event else {
                        break;
                    };
                    self.handle_event(event).await;
                }
                changed = async {
                    match frontier_updates.as_mut() {
                        Some(frontier_updates) => frontier_updates.changed().await,
                        None => std::future::pending().await,
                    }
                }, if frontier_updates_open => {
                    match changed {
                        Ok(()) => {
                            let frontier_updates = frontier_updates
                                .as_mut()
                                .expect("frontier update receiver exists while frontier_updates_open is true");
                            let update = *frontier_updates.borrow_and_update();
                            self.handle_frontier_update(update).await;
                        }
                        Err(_) => frontier_updates_open = false,
                    }
                }
                _ = ticks.tick() => {
                    self.handle_timeouts().await;
                }
            }
        }
    }

    async fn handle_event(&mut self, event: HeaderSyncEvent) {
        self.trace_event_received(&event);
        match event {
            HeaderSyncEvent::PeerConnected(session) => self.handle_peer_connected(session).await,
            HeaderSyncEvent::PeerDisconnected(peer) => self.handle_peer_disconnected(peer),
            HeaderSyncEvent::AdvisoryHeaderSummary { peer, summary } => {
                self.handle_advisory_header_summary(peer, summary)
            }
            HeaderSyncEvent::FullBlockCommitted {
                height,
                hash,
                header: _,
            } => self.handle_full_block_committed(height, hash).await,
            HeaderSyncEvent::NewBlockAccepted {
                peer,
                height,
                hash,
                block,
            } => {
                self.handle_new_block_accepted(peer, height, hash, block)
                    .await
            }
            HeaderSyncEvent::NewBlockDuplicate { peer, height, hash } => {
                self.handle_new_block_duplicate(peer, height, hash)
            }
            HeaderSyncEvent::NewBlockRejected { peer, hash } => {
                self.handle_new_block_rejected(peer, hash).await
            }
            HeaderSyncEvent::PeerStatusUpdated { peer, status } => {
                self.handle_peer_status_updated(peer, status).await;
            }
            HeaderSyncEvent::PeerWorkAssigned {
                peer,
                generation,
                start_height,
                count,
                anchor_hash,
                finalized,
                forward,
            } => {
                self.handle_peer_work_assigned(
                    peer,
                    generation,
                    start_height,
                    count,
                    anchor_hash,
                    finalized,
                    forward,
                )
                .await;
            }
            HeaderSyncEvent::PeerHeadersReceived {
                peer,
                headers,
                body_sizes,
            } => {
                self.handle_headers(peer, headers, body_sizes).await;
            }
            HeaderSyncEvent::InboundGetHeadersRequested {
                peer,
                start_height,
                count,
            } => {
                self.handle_get_headers(peer, start_height, count).await;
            }
            HeaderSyncEvent::NewBlockCandidate { peer, block } => {
                self.handle_new_block(peer, block).await;
            }
            HeaderSyncEvent::PeerMisbehavior { peer, reason } => {
                self.handle_peer_misbehavior(peer, reason).await;
            }
            HeaderSyncEvent::StateFrontiersChanged(frontiers) => {
                self.handle_state_frontiers_changed(frontiers).await;
            }
            HeaderSyncEvent::HeaderRangeCommitted {
                start_height,
                tip_height,
                tip_hash,
            } => {
                self.handle_header_range_committed(start_height, tip_height, tip_hash)
                    .await
            }
            HeaderSyncEvent::HeaderRangeCommitFailed {
                peer,
                start_height,
                count,
                kind,
            } => {
                self.handle_header_range_commit_failed(peer, start_height, count, kind)
                    .await
            }
            HeaderSyncEvent::HeaderRangeResponseFinished {
                peer,
                start_height,
                requested_count,
                returned_count,
            } => self.handle_header_range_response_finished(
                peer,
                start_height,
                requested_count,
                returned_count,
            ),
            HeaderSyncEvent::HeaderRangeResponseReady {
                peer,
                start_height,
                requested_count,
                headers,
                body_sizes,
            } => self.handle_header_range_response_ready(
                peer,
                start_height,
                requested_count,
                headers,
                body_sizes,
            ),
        }
    }

    fn admission_decision_for(
        &self,
        peer: &ZakuraPeerId,
        direction: ServicePeerDirection,
    ) -> ServiceAdmissionDecision {
        if self.state.peers.contains_key(peer) {
            return ServiceAdmissionDecision::Admit;
        }

        let limits = self.startup.config.peer_limits;
        let admitted = self.admitted_count(direction);
        let cap = match direction {
            ServicePeerDirection::Inbound => limits.max_inbound_peers,
            ServicePeerDirection::Outbound => limits.max_outbound_peers,
        };

        if admitted >= cap {
            ServiceAdmissionDecision::RejectFull
        } else {
            ServiceAdmissionDecision::Admit
        }
    }

    async fn handle_frontier_update(&mut self, update: FrontierUpdate) {
        match update.change {
            FrontierChange::Snapshot
            | FrontierChange::VerifiedGrow
            | FrontierChange::VerifiedReset => {
                let frontier = update.frontier;
                self.handle_state_frontiers_changed(HeaderSyncFrontiers {
                    finalized_height: frontier.finalized.height,
                    verified_block_tip: frontier.verified_body.height,
                    verified_block_hash: frontier.verified_body.hash,
                })
                .await;
            }
            FrontierChange::HeaderAdvanced | FrontierChange::HeaderReanchored => {}
        }
    }

    fn admitted_count(&self, direction: ServicePeerDirection) -> usize {
        self.state
            .peers
            .values()
            .filter(|peer| peer.direction == direction)
            .count()
    }

    fn publish_peer_snapshot(&self) {
        let snapshot = ServicePeerSnapshot::new(
            self.admitted_count(ServicePeerDirection::Inbound),
            self.admitted_count(ServicePeerDirection::Outbound),
            self.startup.config.peer_limits,
        );
        let _ = self.peers.send(snapshot);
    }

    fn publish_candidate_state(&mut self) {
        let now = Instant::now();
        self.state
            .advisory
            .retain(|_, advisory| !advisory.is_expired(now));
        for advisory in self.state.advisory.values_mut() {
            if advisory.backoff_until.is_some_and(|until| until <= now) {
                advisory.record_confirmed();
            }
        }

        let mut admitted_node_ids: Vec<_> = self
            .state
            .peers
            .keys()
            .filter_map(node_id_from_header_peer_id)
            .collect();
        admitted_node_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        admitted_node_ids.dedup();

        let mut backed_off_node_ids: Vec<_> = self
            .state
            .advisory
            .iter()
            .filter_map(|(peer, advisory)| {
                advisory
                    .is_backed_off(now)
                    .then(|| node_id_from_header_peer_id(peer))
                    .flatten()
            })
            .collect();
        backed_off_node_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        backed_off_node_ids.dedup();

        let _ = self.candidates.send(ZakuraHeaderSyncCandidateState {
            target_height: header_sync_candidate_target(self.state.best_header_tip),
            admitted_node_ids,
            backed_off_node_ids,
        });
    }

    fn handle_advisory_header_summary(
        &mut self,
        peer: ZakuraPeerId,
        summary: HeaderSyncServiceSummary,
    ) {
        if self.state.peers.contains_key(&peer) {
            return;
        }
        if !header_summary_is_useful(
            summary,
            header_sync_candidate_target(self.state.best_header_tip),
        ) {
            self.state.advisory.remove(&peer);
            self.publish_candidate_state();
            return;
        }

        self.state
            .advisory
            .entry(peer)
            .and_modify(|advisory| advisory.refresh_summary(summary, Instant::now()))
            .or_insert_with(|| HeaderSyncAdvisoryPeerState::new(summary, Instant::now()));
        self.publish_candidate_state();
    }

    fn confirm_advisory_status(&mut self, peer: &ZakuraPeerId, status: HeaderSyncStatus) {
        let Some(summary) = self
            .state
            .advisory
            .get(peer)
            .map(|advisory| advisory.summary)
        else {
            return;
        };

        if status.tip_height >= summary.best_height {
            self.state.advisory.remove(peer);
        } else if let Some(advisory) = self.state.advisory.get_mut(peer) {
            advisory.record_unconfirmed(Instant::now());
        }
        self.publish_candidate_state();
    }

    fn record_advisory_unconfirmed(&mut self, peer: &ZakuraPeerId) {
        let Some(advisory) = self.state.advisory.get_mut(peer) else {
            return;
        };
        advisory.record_unconfirmed(Instant::now());
        self.publish_candidate_state();
    }

    async fn handle_peer_connected(&mut self, session: HeaderSyncPeerSession) {
        let peer = session.peer_id().clone();
        let direction = session.direction();
        let decision = self.admission_decision_for(&peer, direction);
        if decision != ServiceAdmissionDecision::Admit {
            tracing::debug!(
                ?peer,
                ?direction,
                ?decision,
                "locally parking Zakura header-sync service session"
            );
            self.state.parked_peers.insert(peer);
            session.cancel_token().cancel();
            self.publish_peer_snapshot();
            self.publish_candidate_state();
            return;
        }

        self.state.parked_peers.remove(&peer);
        self.state.schedule.forget_peer(&peer);
        let generation = session.generation();
        let status_refresh_interval = self.startup.status_refresh_interval;
        self.state
            .peers
            .entry(peer.clone())
            .and_modify(|peer_state| {
                peer_state.session = session.clone();
                peer_state.direction = direction;
                // A new transport replaces the old one; mint its generation so a
                // late timeout/teardown of the previous session cannot disturb this
                // one. Its remote has received no status yet, so the initial status
                // below must always be sent. Outstanding requests and inbound
                // serving counts are also session-local: responses for the old
                // stream cannot satisfy work sent on this fresh stream.
                peer_state.generation = generation;
                peer_state.received_status = false;
                peer_state.reset_sent_status();
                peer_state.outstanding.clear();
                peer_state.late_covered_responses = 0;
                peer_state.served_headers_inflight = 0;
                peer_state.meters = HeaderSyncPeerMeters::new(
                    status_refresh_interval,
                    DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
                );
            })
            .or_insert_with(|| {
                PeerHeaderState::new(
                    session,
                    generation,
                    self.state.anchor,
                    self.startup.config.advertised_max_headers_per_response(),
                    self.startup.config.advertised_max_inflight_requests(),
                    self.startup.status_refresh_interval,
                    DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
                )
            });
        self.publish_peer_snapshot();
        self.publish_candidate_state();
        self.send_status(&peer);
        self.schedule().await;
    }

    fn handle_peer_disconnected(&mut self, peer: ZakuraPeerId) {
        self.state.peers.remove(&peer);
        self.state.parked_peers.remove(&peer);
        self.state.advisory.remove(&peer);
        self.state.schedule.forget_peer(&peer);
        self.publish_peer_snapshot();
        self.publish_candidate_state();
    }

    async fn handle_full_block_committed(&mut self, height: block::Height, hash: block::Hash) {
        self.state.pending_new_blocks.remove(&hash);
        let _ = self.state.seen.insert(hash);
        self.update_verified_block_tip(height, hash);
        self.state.schedule.mark_height_covered(height);
        self.cancel_covered_outstanding();
        if height > self.state.best_header_tip {
            self.publish_best_tip(height, hash).await;
        }
        self.schedule().await;
    }

    async fn handle_new_block_accepted(
        &mut self,
        peer: ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
        block: Arc<block::Block>,
    ) {
        self.state.pending_new_blocks.remove(&hash);
        let inserted = self.state.seen.insert(hash);
        if !inserted {
            metrics::counter!("sync.header.tip.new_block.deduped").increment(1);
            self.trace_new_block_deduped(&peer, height, hash, "seen_cache");
            return;
        }

        self.update_verified_block_tip(height, hash);
        self.state.schedule.mark_height_covered(height);
        self.cancel_covered_outstanding();
        if height > self.state.best_header_tip {
            self.publish_best_tip(height, hash).await;
        }

        // The reactor selects the forwarding destinations (who); each destination's
        // routine writes the `NewBlock` frame on its own stream (what). Forwarding is
        // advisory tip gossip: a full command queue drops it for that peer (the peer
        // re-learns the tip via `Status`/sync), matching the previous best-effort
        // `try_send_new_block`.
        let destinations = self.eligible_tip_destinations(&peer, height);
        let destination_count = destinations.len();
        self.trace_broadcast_started("new_block", destination_count);
        for destination in destinations {
            let outcome = match self.command_sink(&destination) {
                Some(sink) => sink.enqueue_new_block(block.clone()),
                None => HeaderSyncCommandOutcome::NoRoutine,
            };
            self.trace_command_enqueued(&destination, "new_block", outcome);
            if outcome != HeaderSyncCommandOutcome::Queued {
                tracing::debug!(
                    ?peer,
                    ?destination,
                    ?height,
                    ?hash,
                    ?outcome,
                    "did not forward Zakura header-sync NewBlock to peer routine"
                );
                continue;
            }
            metrics::counter!("sync.header.tip.new_block.forwarded").increment(1);
            self.trace_new_block_forwarded(&peer, &destination, height, hash, destination_count);
            #[cfg(test)]
            let _ = self
                .actions
                .send(HeaderSyncAction::ForwardNewBlock {
                    source: Some(peer.clone()),
                    peer: destination,
                    height,
                    hash,
                    block: block.clone(),
                })
                .await;
        }
        self.schedule().await;
    }

    fn handle_new_block_duplicate(
        &mut self,
        peer: ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
    ) {
        self.state.pending_new_blocks.remove(&hash);
        let _ = self.state.seen.insert(hash);
        metrics::counter!("sync.header.tip.new_block.deduped").increment(1);
        self.trace_new_block_deduped(&peer, height, hash, "already_in_chain");
    }

    async fn handle_new_block_rejected(&mut self, peer: ZakuraPeerId, hash: block::Hash) {
        self.state.pending_new_blocks.remove(&hash);
        metrics::counter!("sync.header.tip.new_block.rejected").increment(1);
        debug!(
            ?peer,
            ?hash,
            "Zakura header-sync NewBlock rejected by block pipeline"
        );
        self.report_misbehavior(peer, HeaderSyncMisbehavior::InvalidNewBlock)
            .await;
    }

    /// Aggregate a routine-classified peer-local protocol violation.
    ///
    /// The peer routine already classified the violation (invalid/spammy `Status`,
    /// unsolicited/malformed `Headers`, over-cap/unsolicited `GetHeaders`, malformed
    /// frame) before emitting this event. The reactor only owns aggregation and the
    /// connection-level disconnect decision.
    async fn handle_peer_misbehavior(&mut self, peer: ZakuraPeerId, reason: HeaderSyncMisbehavior) {
        if self.state.parked_peers.contains(&peer) {
            return;
        }
        self.report_misbehavior(peer, reason).await;
    }

    async fn handle_state_frontiers_changed(&mut self, frontiers: HeaderSyncFrontiers) {
        self.state.finalized_height = frontiers.finalized_height;
        self.state.verified_block_tip = frontiers.verified_block_tip;
        self.state.verified_block_hash = frontiers.verified_block_hash;
        if self.state.best_header_tip <= self.state.verified_block_tip {
            self.state.stale_anchor.reset();
        }
        self.schedule().await;
    }

    async fn handle_header_range_committed(
        &mut self,
        start_height: block::Height,
        tip_height: block::Height,
        tip_hash: block::Hash,
    ) {
        metrics::counter!("sync.header.range.committed").increment(1);
        self.trace_range_event(
            hs_trace::HEADER_RANGE_COMMITTED,
            start_height,
            count_between(start_height, tip_height),
            None,
            None,
        );
        self.state
            .pending_commits
            .retain(|_, range| !range.is_within(start_height, tip_height));
        self.state
            .schedule
            .mark_range_covered(start_height, tip_height);
        // The zebrad driver also uses this event to reload the durable best header tip at
        // startup. In that path start==tip, so covered-range side effects are bounded.
        self.cancel_covered_outstanding();
        if tip_height > self.state.best_header_tip {
            self.publish_best_tip(tip_height, tip_hash).await;
        }
        self.notify_body_gaps().await;
        self.schedule().await;
    }

    async fn handle_header_range_commit_failed(
        &mut self,
        peer: ZakuraPeerId,
        start_height: block::Height,
        count: u32,
        kind: HeaderSyncCommitFailureKind,
    ) {
        metrics::counter!("sync.header.range.rejected").increment(1);
        self.trace_range_event(
            hs_trace::HEADER_RANGE_REJECTED,
            start_height,
            count,
            Some(&peer),
            Some(commit_failure_reason_label(kind)),
        );
        if kind == HeaderSyncCommitFailureKind::InvalidPeerRange {
            self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::InvalidRange)
                .await;
        }
        let key = PendingCommitKey {
            peer,
            start_height,
            count,
        };
        if let Some(range) = self.state.pending_commits.remove(&key) {
            if kind == HeaderSyncCommitFailureKind::Local {
                self.state.schedule.clear_assignment(range);
            }
            self.state.schedule.retry(range);
        }
        self.schedule().await;
    }

    fn handle_header_range_response_finished(
        &mut self,
        peer: ZakuraPeerId,
        start_height: block::Height,
        requested_count: u32,
        returned_count: u32,
    ) {
        self.trace_headers_served(&peer, start_height, requested_count, returned_count);
        if let Some(peer_state) = self.state.peers.get_mut(&peer) {
            peer_state.finish_serving_headers();
        }
    }

    fn handle_header_range_response_ready(
        &mut self,
        peer: ZakuraPeerId,
        start_height: block::Height,
        requested_count: u32,
        headers: Vec<Arc<block::Header>>,
        body_sizes: Vec<u32>,
    ) {
        if !self.state.peers.contains_key(&peer) {
            return;
        }
        if validate_body_sizes_len(headers.len(), body_sizes.len()).is_err() {
            if let Some(peer_state) = self.state.peers.get_mut(&peer) {
                peer_state.finish_serving_headers();
            }
            return;
        }
        let returned_count = u32::try_from(headers.len()).unwrap_or(u32::MAX);
        // The reactor served this inbound `GetHeaders` from state and chose the
        // destination; the peer's routine writes the `Headers` frame on its own
        // stream. A `Headers` response is the correlated reply the peer is waiting
        // for, so it is never silently dropped: a full command queue or a routine
        // that already exited parks the peer (the explicit service-policy mechanism)
        // rather than losing the reply.
        let outcome = match self.command_sink(&peer) {
            Some(sink) => sink.enqueue_headers(headers, body_sizes),
            None => HeaderSyncCommandOutcome::NoRoutine,
        };
        self.trace_command_enqueued(&peer, "headers_response", outcome);
        if let Some(peer_state) = self.state.peers.get_mut(&peer) {
            peer_state.finish_serving_headers();
        }
        match outcome {
            HeaderSyncCommandOutcome::Queued => {
                self.trace_headers_served(&peer, start_height, requested_count, returned_count);
            }
            HeaderSyncCommandOutcome::ParkRequired => {
                tracing::debug!(
                    ?peer,
                    ?start_height,
                    ?requested_count,
                    "parking peer: could not deliver Zakura header-sync Headers response"
                );
                self.park_peer_for_undeliverable_command(&peer, "headers_response");
            }
            // NoRoutine (test/recorder session) and the coalesce/drop outcomes do not
            // apply to the never-drop `Headers` command, so nothing else to do.
            _ => {}
        }
    }

    /// Apply a peer routine's accepted, cap-clamped `Status` to the reactor's peer
    /// snapshot.
    ///
    /// The routine already ran the peer-local validation that was here: anchor/tip
    /// ordering, the status-spam rate gate, and advertised-cap clamping. The reactor
    /// trusts the narrowed event and only mirrors the status into `PeerHeaderState`
    /// (read by scheduling), confirms advisory usefulness, traces, and schedules.
    async fn handle_peer_status_updated(&mut self, peer: ZakuraPeerId, status: HeaderSyncStatus) {
        if self.state.parked_peers.contains(&peer) {
            return;
        }
        let Some(peer_state) = self.state.peers.get_mut(&peer) else {
            return;
        };
        peer_state.advertised_tip = status.tip_height;
        peer_state.advertised_hash = status.tip_hash;
        peer_state.anchor = status.anchor_height;
        // The routine already clamped these; re-clamp defensively so a stray event
        // cannot widen the reactor's serving/scheduling bounds.
        peer_state.max_headers_per_response =
            clamp_advertised_range(status.max_headers_per_response);
        peer_state.max_inflight_requests = status
            .max_inflight_requests
            .clamp(1, LOCAL_MAX_HS_INFLIGHT_PER_PEER);
        peer_state.received_status = true;
        self.confirm_advisory_status(&peer, status);
        self.trace_status_received(&peer, status);
        // Refresh global ranges, then nudge THIS peer's routine to re-attempt a pull
        // now that its advertised tip/caps are known and any forward range it just
        // enabled has been produced. `schedule()` also broadcasts a queue wake, but
        // the targeted wake is the per-peer command path: the reactor decides which
        // routine to wake, the routine re-pulls. Idempotent and coalescible, so it is
        // never parked.
        self.schedule().await;
        let outcome = match self.command_sink(&peer) {
            Some(sink) => sink.enqueue_wake(),
            None => HeaderSyncCommandOutcome::NoRoutine,
        };
        self.trace_command_enqueued(&peer, "wake", outcome);
    }

    fn restore_outstanding_after_late_covered_response(
        &mut self,
        peer: &ZakuraPeerId,
        outstanding: OutstandingRange,
    ) -> bool {
        let Some(peer_state) = self.state.peers.get_mut(peer) else {
            return false;
        };
        if !peer_state.take_late_covered_response() {
            return false;
        }
        peer_state.restore_oldest_outstanding(outstanding);
        metrics::counter!("sync.header.response.late_covered_dropped").increment(1);
        true
    }

    async fn handle_get_headers(
        &mut self,
        peer: ZakuraPeerId,
        start_height: block::Height,
        count: u32,
    ) {
        let local_inflight_cap = self.startup.config.advertised_max_inflight_requests();
        let Some(peer_state) = self.state.peers.get_mut(&peer) else {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::GetHeadersSpam)
                .await;
            return;
        };

        if !peer_state.received_status {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::GetHeadersSpam)
                .await;
            return;
        }

        let allowed_count = inbound_get_headers_count_limit(
            &self.startup.config,
            &self.startup.network,
            self.startup.max_frame_bytes,
        );
        if count == 0 || count > allowed_count {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::GetHeadersTooLong)
                .await;
            return;
        }

        if !peer_state.try_start_serving_headers(local_inflight_cap) {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::GetHeadersSpam)
                .await;
            return;
        }

        if !self
            .dispatch_action(HeaderSyncAction::QueryHeadersByHeightRange {
                peer: peer.clone(),
                start: start_height,
                count,
            })
            .await
        {
            if let Some(peer_state) = self.state.peers.get_mut(&peer) {
                peer_state.finish_serving_headers();
            }
        }
    }

    #[tracing::instrument(skip(self, block))]
    async fn handle_new_block(&mut self, peer: ZakuraPeerId, block: Arc<block::Block>) {
        metrics::counter!("sync.header.tip.new_block.received").increment(1);

        if !self.state.peers.contains_key(&peer) {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::UnknownPeer)
                .await;
            return;
        }

        let hash = block.hash();
        let Some(height) = block.coinbase_height() else {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage)
                .await;
            return;
        };
        self.trace_new_block_received(&peer, height, hash);

        if self.state.seen.contains(&hash) {
            metrics::counter!("sync.header.tip.new_block.deduped").increment(1);
            self.trace_new_block_deduped(&peer, height, hash, "seen_cache");
            return;
        }
        if self.state.pending_new_blocks.contains(&hash) {
            metrics::counter!("sync.header.tip.new_block.deduped").increment(1);
            self.trace_new_block_deduped(&peer, height, hash, "pending_acceptance");
            return;
        }

        if !self
            .state
            .peers
            .get_mut(&peer)
            .expect("peer exists because it was checked before validation")
            .meters
            .inbound_new_block
            .try_take(Instant::now())
        {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::NewBlockSpam)
                .await;
            return;
        }

        if validate_new_block_stateless(block.clone(), &self.startup.network, Utc::now(), height)
            .await
            .is_err()
        {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::InvalidNewBlock)
                .await;
            return;
        }

        if !self.startup.inbound_new_block_acceptance_enabled {
            metrics::counter!("sync.header.tip.new_block.acceptance_unavailable").increment(1);
            debug!(
                ?peer,
                ?hash,
                "Zakura header-sync NewBlock body suppressed until block acceptance is wired"
            );
            return;
        }

        let inserted = self.state.pending_new_blocks.insert(hash);
        debug_assert!(inserted, "pending acceptance was checked before insert");

        if !self
            .dispatch_action(HeaderSyncAction::NewBlockReceived {
                peer,
                height,
                hash,
                block,
            })
            .await
        {
            self.state.pending_new_blocks.remove(&hash);
        }
    }

    fn eligible_tip_destinations(
        &self,
        source: &ZakuraPeerId,
        height: block::Height,
    ) -> Vec<ZakuraPeerId> {
        let mut peers: Vec<_> = self
            .state
            .peers
            .iter()
            .filter(|(peer_id, peer)| {
                *peer_id != source && (!peer.received_status || peer.advertised_tip < height)
            })
            .map(|(peer_id, _)| peer_id.clone())
            .collect();
        peers.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        peers
    }

    #[tracing::instrument(skip(self, headers))]
    async fn handle_headers(
        &mut self,
        peer: ZakuraPeerId,
        headers: Vec<Arc<block::Header>>,
        body_sizes: Vec<u32>,
    ) {
        metrics::counter!("sync.header.response.received").increment(1);
        let Some(peer_state) = self.state.peers.get_mut(&peer) else {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::UnsolicitedHeaders)
                .await;
            return;
        };
        let Some(outstanding) = peer_state.pop_oldest_outstanding() else {
            if peer_state.take_late_covered_response() {
                return;
            }
            self.report_misbehavior(peer, HeaderSyncMisbehavior::UnsolicitedHeaders)
                .await;
            return;
        };
        let peer_max_headers_per_response = peer_state.max_headers_per_response;
        let in_flight_count = peer_state.outstanding.len();

        self.handle_headers_for_outstanding(
            peer,
            headers,
            body_sizes,
            outstanding,
            peer_max_headers_per_response,
            in_flight_count,
        )
        .await;
    }

    async fn handle_headers_for_outstanding(
        &mut self,
        peer: ZakuraPeerId,
        headers: Vec<Arc<block::Header>>,
        body_sizes: Vec<u32>,
        outstanding: OutstandingRange,
        peer_max_headers_per_response: u32,
        in_flight_count: usize,
    ) {
        if validate_body_sizes_len(headers.len(), body_sizes.len()).is_err() {
            self.report_misbehavior(peer, HeaderSyncMisbehavior::MalformedMessage)
                .await;
            self.state.schedule.retry(outstanding.range);
            self.schedule().await;
            return;
        }

        if headers.is_empty() {
            self.record_advisory_unconfirmed(&peer);
            self.trace_headers_received(
                &peer,
                outstanding.range.start_height,
                0,
                outstanding.expected_max_count,
                peer_max_headers_per_response,
                in_flight_count,
            );
            // The reactor already popped this outstanding range. The empty-response
            // re-fan is owned by the routine now: it returned the range to the
            // shared queue (clearing this peer's assignment) and armed its own
            // short retry window before re-pulling. The reactor only drops its
            // outstanding bookkeeping here.
            return;
        }

        let header_count =
            u32::try_from(headers.len()).expect("decoded Headers length is capped by u32");
        self.trace_headers_received(
            &peer,
            outstanding.range.start_height,
            header_count,
            outstanding.expected_max_count,
            peer_max_headers_per_response,
            in_flight_count,
        );
        if header_count > outstanding.expected_max_count || header_count > outstanding.range.count {
            self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::ResponseTooLong)
                .await;
            self.state.schedule.retry(outstanding.range);
            self.schedule().await;
            return;
        }

        let validation_context = HeaderSyncValidationContext {
            network: &self.startup.network,
            now: Utc::now(),
            start_height: outstanding.range.start_height,
            decode_context: HeaderSyncDecodeContext::for_headers_response(
                ExpectedHeadersResponse::new(
                    outstanding.range.start_height,
                    outstanding.expected_max_count,
                )
                .expect("outstanding range uses a non-zero bounded count"),
                outstanding.expected_max_count,
            ),
        };
        if let Err(error) = validate_header_range_links(outstanding.range.anchor_hash, &headers) {
            debug!(
                ?peer,
                ?error,
                anchor_hash = ?outstanding.range.anchor_hash,
                start_height = ?outstanding.range.start_height,
                count = ?header_count,
                "Zakura header-sync rejected header range links"
            );
            self.trace_range_validation_rejected(
                &peer,
                outstanding.range,
                header_count,
                "link",
                header_sync_wire_error_kind(&error),
            );
            if matches!(error, HeaderSyncWireError::FirstHeaderDoesNotLink)
                && self.restore_outstanding_after_late_covered_response(&peer, outstanding)
            {
                return;
            }
            if self
                .handle_possible_stale_anchor_link_failure(&peer, outstanding.range, &error)
                .await
            {
                self.schedule().await;
                return;
            }
            self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::InvalidRange)
                .await;
            self.state.schedule.retry(outstanding.range);
            self.schedule().await;
            return;
        }
        if let Err(error) = validate_headers_stateless(headers.clone(), validation_context).await {
            debug!(
                ?peer,
                ?error,
                start_height = ?outstanding.range.start_height,
                count = ?header_count,
                "Zakura header-sync rejected stateless header range"
            );
            self.trace_range_validation_rejected(
                &peer,
                outstanding.range,
                header_count,
                "stateless",
                header_sync_wire_error_kind(&error),
            );
            self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::InvalidRange)
                .await;
            self.state.schedule.retry(outstanding.range);
            self.schedule().await;
            return;
        }

        let end_height = height_after_count(outstanding.range.start_height, header_count)
            .and_then(previous_height)
            .expect("non-empty bounded range has an end height");
        if outstanding.range.finalized {
            let last_hash = headers
                .last()
                .map(|header| block::Hash::from(header.as_ref()))
                .expect("headers is non-empty");
            if end_height != outstanding.range.end_height()
                || self.startup.network.checkpoint_list().hash(end_height) != Some(last_hash)
            {
                self.trace_range_validation_rejected(
                    &peer,
                    outstanding.range,
                    header_count,
                    "checkpoint",
                    "checkpoint_hash_mismatch",
                );
                self.report_misbehavior(peer.clone(), HeaderSyncMisbehavior::InvalidRange)
                    .await;
                self.state.schedule.retry(outstanding.range);
                self.schedule().await;
                return;
            }
        }

        self.state.pending_commits.insert(
            PendingCommitKey {
                peer: peer.clone(),
                start_height: outstanding.range.start_height,
                count: header_count,
            },
            outstanding.range,
        );
        let _ = self
            .dispatch_action(HeaderSyncAction::CommitHeaderRange {
                peer,
                anchor: outstanding.range.anchor_hash,
                start_height: outstanding.range.start_height,
                headers,
                body_sizes,
                finalized: outstanding.range.finalized,
            })
            .await;
    }

    async fn handle_possible_stale_anchor_link_failure(
        &mut self,
        peer: &ZakuraPeerId,
        range: RangeRequest,
        error: &HeaderSyncWireError,
    ) -> bool {
        if !matches!(error, HeaderSyncWireError::FirstHeaderDoesNotLink)
            || range.priority != RangePriority::Forward
            || range.finalized
            || self.state.best_header_tip <= self.state.verified_block_tip
        {
            self.state.stale_anchor.reset();
            return false;
        }

        self.state.stale_anchor.record(peer.clone());
        metrics::counter!("sync.header.stale_anchor.link_failure").increment(1);

        if !self.state.stale_anchor.should_reanchor() {
            self.state.schedule.clear_assignment(range);
            self.state.schedule.retry(range);
            return true;
        }

        self.reanchor_to_verified_block_tip().await;
        true
    }

    async fn reanchor_to_verified_block_tip(&mut self) {
        let height = self.state.verified_block_tip;
        let hash = self.state.verified_block_hash;
        metrics::counter!("sync.header.stale_anchor.reanchored").increment(1);

        self.state.stale_anchor.reset();
        // Reanchor re-bases the best-header target down to the verified tip; drop
        // every forward range above the new target so it is re-derived from the
        // verified tip on the next `schedule()`. Reanchor only fires while
        // `best_header_tip > verified_block_tip`, so all forward work is above the
        // new target — this clears the same set the old `clear_forward()` did.
        self.state.schedule.reset_above(height);
        self.state
            .pending_commits
            .retain(|_, range| range.priority != RangePriority::Forward);
        self.cancel_forward_outstanding();
        self.publish_best_tip_reanchored(height, hash).await;
    }

    async fn handle_timeouts(&mut self) {
        let now = Instant::now();
        // Collect (peer, generation, range) for every expired outstanding request,
        // remembering the session so the range is returned under its generation and
        // its routine is told to drop the matching expectation.
        let mut timed_out = Vec::new();
        for (peer_id, peer) in self.state.peers.iter_mut() {
            let mut index = 0;
            while index < peer.outstanding.len() {
                if peer.outstanding[index].deadline <= now {
                    let outstanding = peer.outstanding.remove(index);
                    timed_out.push((peer_id.clone(), outstanding));
                } else {
                    index += 1;
                }
            }
        }
        for (peer_id, outstanding) in timed_out {
            metrics::counter!("sync.header.request.timed_out").increment(1);
            // Return the range to the shared queue under the requesting session's
            // generation so it re-fans to another peer; the generation guard means a
            // stale return cannot disturb a reconnected session's live work.
            self.state.schedule.return_work(
                &peer_id,
                outstanding.generation,
                outstanding.range,
                ReturnReason::Timeout,
            );
            // Tell the routine to drop the stale expectation and free its slot so it
            // can pull fresh work. The reactor stays the single timeout authority; the
            // routine only owns its local FIFO. This reset controls request
            // correlation and the outbound slot, so it is never silently dropped: a
            // full command queue parks the peer (its remaining work is returned) so
            // the slot cannot stay wedged on a covered range.
            let matches_generation = self
                .state
                .peers
                .get(&peer_id)
                .is_some_and(|peer_state| peer_state.generation == outstanding.generation);
            if matches_generation {
                let outcome = match self.command_sink(&peer_id) {
                    Some(sink) => sink.enqueue_drop_expectation(
                        outstanding.generation,
                        outstanding.range.start_height,
                        outstanding.expected_max_count,
                    ),
                    None => HeaderSyncCommandOutcome::NoRoutine,
                };
                self.trace_command_enqueued(&peer_id, "drop_expectation", outcome);
                if outcome.requires_park() {
                    self.park_peer_for_undeliverable_command(&peer_id, "drop_expectation");
                }
            }
        }
        self.schedule().await;
    }

    fn empty_headers_retry_delay(&self) -> Duration {
        self.startup.request_timeout.min(EMPTY_HEADERS_RETRY_DELAY)
    }

    /// Produce outbound header work into the shared range queue and wake parked
    /// routines.
    ///
    /// As of the routine-pulled-work chunk, the reactor no longer chooses a peer or
    /// sends `GetHeaders` itself: it refreshes the forward/backward target ranges
    /// from the current frontier and lets each per-peer routine pull eligible work
    /// from the shared queue, send the request on its own stream, and record the
    /// expected response locally. `SharedHeaderRangeQueue::ensure_*` already wakes
    /// routines; the explicit `wake()` covers the no-new-range case where only a
    /// peer's status/slot changed.
    async fn schedule(&mut self) {
        if !self.startup.range_state_actions_enabled {
            return;
        }

        self.state.refresh_forward_range(&self.startup);
        self.state.refresh_backward_range(&self.startup);
        self.state.schedule.wake();
    }

    /// Record the outstanding range a peer routine just requested.
    ///
    /// The routine performed the assignment (narrow/clamp/`mark_assigned`) under
    /// the shared-queue lock and recorded its own expected `Headers` before sending
    /// `GetHeaders`; this only records the matching [`OutstandingRange`] so the
    /// reactor's timeout, covered-range, and commit machinery stay reactor-owned.
    /// The deadline is computed reactor-side (the reactor remains the single
    /// timeout authority). A `PeerWorkAssigned` for an unknown or generation-stale
    /// session is dropped.
    #[allow(clippy::too_many_arguments)]
    async fn handle_peer_work_assigned(
        &mut self,
        peer: ZakuraPeerId,
        generation: u64,
        start_height: block::Height,
        count: u32,
        anchor_hash: block::Hash,
        finalized: bool,
        forward: bool,
    ) {
        let priority = if forward {
            RangePriority::Forward
        } else {
            RangePriority::Backward
        };
        let range = RangeRequest {
            start_height,
            count,
            anchor_hash,
            finalized,
            priority,
        };
        let peer_cap = {
            let Some(peer_state) = self.state.peers.get_mut(&peer) else {
                return;
            };
            if peer_state.generation != generation {
                // A request from a superseded session; its work belongs to the old
                // generation in the shared queue and will be cleaned up there.
                return;
            }
            let deadline = Instant::now() + self.startup.request_timeout;
            peer_state.outstanding.push(OutstandingRange {
                range,
                deadline,
                expected_max_count: count,
                generation,
            });
            peer_state.max_headers_per_response
        };
        metrics::counter!("sync.header.request.sent").increment(1);
        self.trace_get_headers_sent(&peer, start_height, count, peer_cap);
        #[cfg(test)]
        let _ = self
            .actions
            .send(HeaderSyncAction::SendMessage {
                peer,
                msg: HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                },
            })
            .await;
    }

    fn send_status(&mut self, peer: &ZakuraPeerId) {
        let status = self.local_status();
        // Suppress a status identical to the last one we sent this peer over its
        // current session: it advances nothing and the peer's inbound status
        // rate limiter would treat the redundant message as spam.
        match self.state.peers.get_mut(peer) {
            Some(peer_state) if peer_state.status_differs_from_last_sent(status) => {
                peer_state.record_sent_status(status);
            }
            Some(_) => {
                metrics::counter!("sync.header.peer.status.suppressed_redundant").increment(1);
                return;
            }
            None => return,
        }
        metrics::counter!("sync.header.peer.status.sent").increment(1);
        self.trace_status_sent(peer, status);
        // The reactor chose the status and the destination; the peer's routine writes
        // the frame on its own stream. Status is coalesced through the routine's
        // single-slot watch (a newer status fully replaces an unread one) and is
        // advisory, so it is never parked.
        self.enqueue_status(peer, status);
        #[cfg(test)]
        let _ = self.actions.try_send(HeaderSyncAction::SendMessage {
            peer: peer.clone(),
            msg: HeaderSyncMessage::Status(status),
        });
    }

    async fn publish_best_tip(&mut self, height: block::Height, hash: block::Hash) {
        self.state.best_header_tip = height;
        self.state.best_header_hash = hash;
        metrics::gauge!("sync.header.best_tip.height").set(height.0 as f64);
        self.trace_frontier_advanced(height, hash);
        let _ = self.tip.send((height, hash));
        let _ = self
            .dispatch_action(HeaderSyncAction::HeaderAdvanced { height, hash })
            .await;
        self.publish_candidate_state();
        self.broadcast_status_refresh().await;
    }

    async fn publish_best_tip_reanchored(&mut self, height: block::Height, hash: block::Hash) {
        let old = (self.state.best_header_tip, self.state.best_header_hash);
        self.state.best_header_tip = height;
        self.state.best_header_hash = hash;
        metrics::gauge!("sync.header.best_tip.height").set(height.0 as f64);
        self.trace_frontier_reanchored(height, hash);
        let _ = self.tip.send((height, hash));
        let _ = self
            .dispatch_action(HeaderSyncAction::HeaderReanchored {
                old,
                new: (height, hash),
            })
            .await;
        self.publish_candidate_state();
        self.broadcast_status_refresh().await;
    }

    fn update_verified_block_tip(&mut self, height: block::Height, hash: block::Hash) {
        if height > self.state.verified_block_tip {
            self.state.verified_block_tip = height;
            self.state.verified_block_hash = hash;
        }
        if self.state.best_header_tip <= self.state.verified_block_tip {
            self.state.stale_anchor.reset();
        }
    }

    async fn broadcast_status_refresh(&mut self) {
        let now = Instant::now();
        let status = self.local_status();
        let peer_ids: Vec<_> = self
            .state
            .peers
            .iter_mut()
            .filter_map(|(peer_id, peer)| {
                // Never re-send a peer a status identical to its last one: the
                // peer's inbound rate limiter would treat it as spam. A redundant
                // refresh is dropped without spending the peer's status budget.
                if !peer.status_differs_from_last_sent(status) {
                    metrics::counter!("sync.header.peer.status.suppressed_redundant").increment(1);
                    return None;
                }
                if !peer.meters.unsolicited.try_take(now) {
                    return None;
                }
                peer.record_sent_status(status);
                Some(peer_id.clone())
            })
            .collect();

        // The reactor selects the broadcast destinations (who); each peer's routine
        // writes the `Status` frame on its own stream (what). The broadcast loop
        // iterates the reactor's admitted sessions and enqueues a per-routine command
        // — routines never scan all peers themselves.
        self.trace_broadcast_started("status", peer_ids.len());
        for peer in peer_ids {
            self.enqueue_status(&peer, status);
            #[cfg(test)]
            let _ = self.actions.try_send(HeaderSyncAction::SendMessage {
                peer,
                msg: HeaderSyncMessage::Status(status),
            });
        }
    }

    async fn notify_body_gaps(&self) {
        if !self.startup.range_state_actions_enabled {
            return;
        }

        if self.state.best_header_tip > self.state.verified_block_tip {
            let from =
                next_height(self.state.verified_block_tip).unwrap_or(self.state.verified_block_tip);
            metrics::gauge!("sync.header.missing_bodies")
                .set(count_between(from, self.state.best_header_tip) as f64);
            self.trace_missing_bodies(from, self.state.best_header_tip);
            let _ = self
                .dispatch_action(HeaderSyncAction::BodyGaps {
                    from,
                    to: self.state.best_header_tip,
                })
                .await;
        }
    }

    /// Hand a data-plane action to the action driver without letting a slow or
    /// stalled driver wedge the reactor. A full channel is awaited only up to
    /// [`ACTION_SEND_TIMEOUT`]; past that the action is dropped so the reactor
    /// keeps draining peer-lifecycle events, request timeouts, and misbehavior
    /// disconnects. Returns `true` only if the action was accepted.
    async fn dispatch_action(&self, action: HeaderSyncAction) -> bool {
        self.trace_action_dispatched(&action);
        match time::timeout(ACTION_SEND_TIMEOUT, self.actions.send(action)).await {
            Ok(Ok(())) => true,
            // Receiver dropped: the driver is gone, treat like a send failure.
            Ok(Err(_)) => false,
            // Driver stalled past the deadline: drop the action and stay live.
            Err(_) => {
                metrics::counter!("sync.header.action.send_timeout").increment(1);
                false
            }
        }
    }

    async fn report_misbehavior(&mut self, peer: ZakuraPeerId, reason: HeaderSyncMisbehavior) {
        // Misbehavior is record-only: trace and forward it, but never cancel the
        // session. Peer scoring no longer drives disconnects.
        metrics::counter!("sync.header.peer.violation").increment(1);
        self.trace_peer_violation(&peer, reason);
        self.trace_peer_disconnect_requested(&peer, reason);
        // Best-effort record of the violation for the driver. Never block the
        // reactor waiting for channel capacity.
        let action = HeaderSyncAction::Misbehavior { peer, reason };
        self.trace_action_dispatched(&action);
        if self.actions.try_send(action).is_err() {
            metrics::counter!("sync.header.peer.violation.action_dropped").increment(1);
        }
    }

    /// Enqueue this node's current local `Status` to one peer's routine through its
    /// coalescing command slot, returning the per-peer enqueue outcome. The routine
    /// writes the frame on its own stream; the reactor only chose the destination and
    /// the status. A redundant status is suppressed by the caller (via
    /// `status_differs_from_last_sent`) before this is reached, and the watch slot
    /// further coalesces a burst to the newest value — both safe, since a `Status` is
    /// a full snapshot that fully replaces any older one.
    fn enqueue_status(
        &self,
        peer: &ZakuraPeerId,
        status: HeaderSyncStatus,
    ) -> HeaderSyncCommandOutcome {
        let outcome = match self.command_sink(peer) {
            Some(sink) => sink.enqueue_status(status),
            None => HeaderSyncCommandOutcome::NoRoutine,
        };
        self.trace_command_enqueued(peer, "status", outcome);
        outcome
    }

    /// The per-peer routine command sink, if the peer has live state with a routine.
    fn command_sink(&self, peer: &ZakuraPeerId) -> Option<&HeaderSyncCommandSink> {
        self.state.peers.get(peer)?.session.commands()
    }

    /// Park a peer whose routine could not be handed a correctness-critical command
    /// (a `Headers` response or a reset/timeout `DropExpectation`) because its bounded
    /// command queue was full or its routine had already exited. This is the explicit
    /// service-policy mechanism the outbound-commands chunk introduces: rather than
    /// silently lose a correlated reply or strand the outbound slot, the reactor drops
    /// this peer's header-sync state, marks it parked (so re-admission must mint a
    /// fresh session), and cancels its *service* token. It is NOT wired to ordinary
    /// misbehavior — record-only misbehavior still never disconnects — and cancels
    /// only the service token, leaving the shared connection (discovery, block-sync)
    /// untouched. Any outstanding range this peer held is returned to the shared queue
    /// so it re-fans to another peer and is never stranded.
    fn park_peer_for_undeliverable_command(
        &mut self,
        peer: &ZakuraPeerId,
        command_kind: &'static str,
    ) {
        metrics::counter!("sync.header.peer.command.park").increment(1);
        self.trace_command_enqueued(peer, command_kind, HeaderSyncCommandOutcome::ParkRequired);
        let Some(peer_state) = self.state.peers.remove(peer) else {
            return;
        };
        // Return every outstanding range this peer held to the shared queue under its
        // own generation so it re-fans to another peer (the routine's own `Drop` also
        // returns its single in-flight range; both are generation-scoped and
        // idempotent, so a double-return cannot disturb a reconnected session).
        for outstanding in &peer_state.outstanding {
            self.state.schedule.return_work(
                peer,
                outstanding.generation,
                outstanding.range,
                ReturnReason::Teardown,
            );
        }
        self.state.schedule.forget_peer(peer);
        self.state.parked_peers.insert(peer.clone());
        // Cancel only the service token: parking one service must not tear down a
        // multi-service connection.
        peer_state.session.cancel_token().cancel();
        self.publish_peer_snapshot();
        self.publish_candidate_state();
    }

    fn trace_command_enqueued(
        &self,
        peer: &ZakuraPeerId,
        command_kind: &'static str,
        outcome: HeaderSyncCommandOutcome,
    ) {
        self.emit_trace(hs_trace::HEADER_COMMAND_ENQUEUED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_optional_str(row, hs_trace::KIND, Some(command_kind));
            insert_optional_str(row, hs_trace::RESULT, Some(outcome.label()));
        });
    }

    fn trace_broadcast_started(&self, command_kind: &'static str, destination_count: usize) {
        self.emit_trace(hs_trace::HEADER_BROADCAST_STARTED, |row| {
            insert_optional_str(row, hs_trace::KIND, Some(command_kind));
            insert_u64(
                row,
                hs_trace::DESTINATION_PEER_COUNT,
                // Lossless usize -> u64 widening for the trace row.
                destination_count as u64,
            );
        });
    }

    fn trace_event_received(&self, event: &HeaderSyncEvent) {
        self.emit_trace(hs_trace::HEADER_EVENT_RECEIVED, |row| match event {
            HeaderSyncEvent::PeerConnected(session) => {
                insert_optional_str(row, hs_trace::KIND, Some("peer_connected"));
                insert_peer(row, hs_trace::PEER, session.peer_id());
            }
            HeaderSyncEvent::PeerDisconnected(peer) => {
                insert_optional_str(row, hs_trace::KIND, Some("peer_disconnected"));
                insert_peer(row, hs_trace::PEER, peer);
            }
            HeaderSyncEvent::AdvisoryHeaderSummary { peer, summary } => {
                insert_optional_str(row, hs_trace::KIND, Some("advisory_header_summary"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::HEIGHT, summary.best_height);
            }
            HeaderSyncEvent::FullBlockCommitted { height, hash, .. } => {
                insert_optional_str(row, hs_trace::KIND, Some("full_block_committed"));
                insert_height(row, hs_trace::HEIGHT, *height);
                insert_hash(row, hs_trace::HASH, *hash);
            }
            HeaderSyncEvent::NewBlockAccepted {
                peer, height, hash, ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("new_block_accepted"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::HEIGHT, *height);
                insert_hash(row, hs_trace::HASH, *hash);
            }
            HeaderSyncEvent::NewBlockDuplicate { peer, height, hash } => {
                insert_optional_str(row, hs_trace::KIND, Some("new_block_duplicate"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::HEIGHT, *height);
                insert_hash(row, hs_trace::HASH, *hash);
            }
            HeaderSyncEvent::NewBlockRejected { peer, hash } => {
                insert_optional_str(row, hs_trace::KIND, Some("new_block_rejected"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_hash(row, hs_trace::HASH, *hash);
            }
            HeaderSyncEvent::PeerStatusUpdated { peer, status } => {
                insert_optional_str(row, hs_trace::KIND, Some("peer_status_updated"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::HEIGHT, status.tip_height);
                insert_hash(row, hs_trace::HASH, status.tip_hash);
                insert_height(row, hs_trace::RANGE_START, status.anchor_height);
            }
            HeaderSyncEvent::PeerWorkAssigned {
                peer,
                start_height,
                count,
                ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("peer_work_assigned"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::RANGE_START, *start_height);
                insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*count));
            }
            HeaderSyncEvent::PeerHeadersReceived { peer, headers, .. } => {
                insert_optional_str(row, hs_trace::KIND, Some("peer_headers_received"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_u64(row, hs_trace::RANGE_COUNT, headers.len() as u64);
            }
            HeaderSyncEvent::InboundGetHeadersRequested {
                peer,
                start_height,
                count,
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("inbound_get_headers_requested"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::RANGE_START, *start_height);
                insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*count));
            }
            HeaderSyncEvent::NewBlockCandidate { peer, block } => {
                insert_optional_str(row, hs_trace::KIND, Some("new_block_candidate"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_hash(row, hs_trace::HASH, block.hash());
                if let Some(height) = block.coinbase_height() {
                    insert_height(row, hs_trace::HEIGHT, height);
                }
            }
            HeaderSyncEvent::PeerMisbehavior { peer, reason } => {
                insert_optional_str(row, hs_trace::KIND, Some("peer_misbehavior"));
                insert_optional_str(
                    row,
                    hs_trace::REASON,
                    Some(misbehavior_reason_label(*reason)),
                );
                insert_peer(row, hs_trace::PEER, peer);
            }
            HeaderSyncEvent::StateFrontiersChanged(frontiers) => {
                insert_optional_str(row, hs_trace::KIND, Some("state_frontiers_changed"));
                insert_height(row, "finalized_height", frontiers.finalized_height);
                insert_height(row, "verified_block_tip", frontiers.verified_block_tip);
            }
            HeaderSyncEvent::HeaderRangeCommitted {
                start_height,
                tip_height,
                tip_hash,
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("header_range_committed"));
                insert_height(row, hs_trace::RANGE_START, *start_height);
                insert_u64(
                    row,
                    hs_trace::RANGE_COUNT,
                    u64::from(count_between(*start_height, *tip_height)),
                );
                insert_height(row, hs_trace::HEIGHT, *tip_height);
                insert_hash(row, hs_trace::HASH, *tip_hash);
            }
            HeaderSyncEvent::HeaderRangeCommitFailed {
                peer,
                start_height,
                count,
                kind,
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("header_range_commit_failed"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::RANGE_START, *start_height);
                insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*count));
                insert_optional_str(
                    row,
                    hs_trace::REASON,
                    Some(commit_failure_reason_label(*kind)),
                );
            }
            HeaderSyncEvent::HeaderRangeResponseFinished {
                peer,
                start_height,
                requested_count,
                returned_count,
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("header_range_response_finished"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::RANGE_START, *start_height);
                insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*returned_count));
                insert_u64(row, hs_trace::EXPECTED_COUNT, u64::from(*requested_count));
            }
            HeaderSyncEvent::HeaderRangeResponseReady {
                peer,
                start_height,
                requested_count,
                headers,
                ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("header_range_response_ready"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::RANGE_START, *start_height);
                insert_u64(row, hs_trace::RANGE_COUNT, headers.len() as u64);
                insert_u64(row, hs_trace::EXPECTED_COUNT, u64::from(*requested_count));
            }
        });
    }

    fn trace_action_dispatched(&self, action: &HeaderSyncAction) {
        self.emit_trace(hs_trace::HEADER_ACTION_DISPATCHED, |row| match action {
            #[cfg(test)]
            HeaderSyncAction::SendMessage { peer, msg } => {
                insert_optional_str(row, hs_trace::KIND, Some("send_message"));
                insert_optional_str(row, hs_trace::REASON, Some(header_sync_message_label(msg)));
                insert_peer(row, hs_trace::PEER, peer);
                trace_header_sync_message_fields(row, msg);
            }
            #[cfg(test)]
            HeaderSyncAction::ForwardNewBlock {
                source,
                peer,
                height,
                hash,
                ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("forward_new_block"));
                if let Some(source) = source {
                    insert_peer(row, hs_trace::SOURCE_PEER, source);
                }
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::HEIGHT, *height);
                insert_hash(row, hs_trace::HASH, *hash);
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                insert_optional_str(row, hs_trace::KIND, Some("misbehavior"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_optional_str(
                    row,
                    hs_trace::REASON,
                    Some(misbehavior_reason_label(*reason)),
                );
            }
            HeaderSyncAction::NewBlockReceived {
                peer, height, hash, ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("new_block_received"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::HEIGHT, *height);
                insert_hash(row, hs_trace::HASH, *hash);
            }
            HeaderSyncAction::QueryHeadersByHeightRange { peer, start, count } => {
                insert_optional_str(row, hs_trace::KIND, Some("query_headers_by_height_range"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::RANGE_START, *start);
                insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*count));
            }
            HeaderSyncAction::CommitHeaderRange {
                peer,
                start_height,
                headers,
                ..
            } => {
                insert_optional_str(row, hs_trace::KIND, Some("commit_header_range"));
                insert_peer(row, hs_trace::PEER, peer);
                insert_height(row, hs_trace::RANGE_START, *start_height);
                insert_u64(row, hs_trace::RANGE_COUNT, headers.len() as u64);
            }
            HeaderSyncAction::QueryBestHeaderTip => {
                insert_optional_str(row, hs_trace::KIND, Some("query_best_header_tip"));
            }
            HeaderSyncAction::QueryMissingBlockBodies { from, limit } => {
                insert_optional_str(row, hs_trace::KIND, Some("query_missing_block_bodies"));
                insert_height(row, hs_trace::RANGE_START, *from);
                insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*limit));
            }
            HeaderSyncAction::BodyGaps { from, to } => {
                insert_optional_str(row, hs_trace::KIND, Some("body_gaps"));
                insert_height(row, hs_trace::RANGE_START, *from);
                insert_u64(
                    row,
                    hs_trace::RANGE_COUNT,
                    u64::from(count_between(*from, *to)),
                );
            }
            HeaderSyncAction::HeaderAdvanced { height, hash } => {
                insert_optional_str(row, hs_trace::KIND, Some("header_advanced"));
                insert_height(row, hs_trace::HEIGHT, *height);
                insert_hash(row, hs_trace::HASH, *hash);
            }
            HeaderSyncAction::HeaderReanchored { old, new } => {
                insert_optional_str(row, hs_trace::KIND, Some("header_reanchored"));
                insert_height(row, hs_trace::HEIGHT, new.0);
                insert_hash(row, hs_trace::HASH, new.1);
                insert_height(row, hs_trace::RANGE_START, old.0);
            }
        });
    }

    fn trace_status_sent(&self, peer: &ZakuraPeerId, status: HeaderSyncStatus) {
        self.emit_trace(hs_trace::HEADER_STATUS_SENT, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, status.tip_height);
            insert_hash(row, hs_trace::HASH, status.tip_hash);
            insert_height(row, hs_trace::RANGE_START, status.anchor_height);
            insert_u64(
                row,
                hs_trace::ADVERTISED_CAP,
                u64::from(status.max_headers_per_response),
            );
            insert_u64(
                row,
                hs_trace::IN_FLIGHT_COUNT,
                u64::from(status.max_inflight_requests),
            );
        });
    }

    fn trace_status_received(&self, peer: &ZakuraPeerId, status: HeaderSyncStatus) {
        self.emit_trace(hs_trace::HEADER_STATUS_RECEIVED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, status.tip_height);
            insert_hash(row, hs_trace::HASH, status.tip_hash);
            insert_height(row, hs_trace::RANGE_START, status.anchor_height);
            insert_u64(
                row,
                hs_trace::ADVERTISED_CAP,
                u64::from(status.max_headers_per_response),
            );
            insert_u64(
                row,
                hs_trace::IN_FLIGHT_COUNT,
                u64::from(status.max_inflight_requests),
            );
        });
    }

    fn trace_get_headers_sent(
        &self,
        peer: &ZakuraPeerId,
        start_height: block::Height,
        count: u32,
        advertised_cap: u32,
    ) {
        self.emit_trace(hs_trace::HEADER_GET_HEADERS_SENT, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::RANGE_START, start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(count));
            insert_u64(row, hs_trace::ADVERTISED_CAP, u64::from(advertised_cap));
        });
    }

    fn trace_headers_received(
        &self,
        peer: &ZakuraPeerId,
        start_height: block::Height,
        count: u32,
        expected_max_count: u32,
        advertised_cap: u32,
        in_flight_count: usize,
    ) {
        self.emit_trace(hs_trace::HEADER_HEADERS_RECEIVED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::RANGE_START, start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(count));
            insert_u64(row, hs_trace::ADVERTISED_CAP, u64::from(advertised_cap));
            insert_u64(row, hs_trace::EXPECTED_COUNT, u64::from(expected_max_count));
            insert_u64(row, hs_trace::IN_FLIGHT_COUNT, in_flight_count as u64);
        });
    }

    fn trace_headers_served(
        &self,
        peer: &ZakuraPeerId,
        start_height: block::Height,
        requested_count: u32,
        returned_count: u32,
    ) {
        self.emit_trace(hs_trace::HEADER_HEADERS_SERVED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::RANGE_START, start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(returned_count));
            insert_u64(row, hs_trace::EXPECTED_COUNT, u64::from(requested_count));
        });
    }

    fn trace_range_event(
        &self,
        event: &'static str,
        start_height: block::Height,
        count: u32,
        peer: Option<&ZakuraPeerId>,
        reason: Option<&'static str>,
    ) {
        self.emit_trace(event, |row| {
            if let Some(peer) = peer {
                insert_peer(row, hs_trace::PEER, peer);
            }
            insert_height(row, hs_trace::RANGE_START, start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(count));
            insert_optional_str(row, hs_trace::REASON, reason);
        });
    }

    fn trace_range_validation_rejected(
        &self,
        peer: &ZakuraPeerId,
        range: RangeRequest,
        count: u32,
        validation_stage: &'static str,
        error_kind: &'static str,
    ) {
        self.emit_trace(hs_trace::HEADER_RANGE_REJECTED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::RANGE_START, range.start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(count));
            insert_hash(row, hs_trace::ANCHOR_HASH, range.anchor_hash);
            insert_optional_str(row, hs_trace::VALIDATION_STAGE, Some(validation_stage));
            insert_optional_str(row, hs_trace::ERROR_KIND, Some(error_kind));
            insert_optional_str(
                row,
                hs_trace::REASON,
                Some(misbehavior_reason_label(
                    HeaderSyncMisbehavior::InvalidRange,
                )),
            );
        });
    }

    fn trace_new_block_received(
        &self,
        peer: &ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
    ) {
        self.emit_trace(hs_trace::HEADER_NEW_BLOCK_RECEIVED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, height);
            insert_hash(row, hs_trace::HASH, hash);
        });
    }

    fn trace_new_block_forwarded(
        &self,
        source: &ZakuraPeerId,
        destination: &ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
        destination_count: usize,
    ) {
        self.emit_trace(hs_trace::HEADER_NEW_BLOCK_FORWARDED, |row| {
            insert_peer(row, hs_trace::SOURCE_PEER, source);
            insert_peer(row, hs_trace::PEER, destination);
            insert_height(row, hs_trace::HEIGHT, height);
            insert_hash(row, hs_trace::HASH, hash);
            insert_u64(
                row,
                hs_trace::DESTINATION_PEER_COUNT,
                destination_count as u64,
            );
        });
    }

    fn trace_new_block_deduped(
        &self,
        peer: &ZakuraPeerId,
        height: block::Height,
        hash: block::Hash,
        reason: &'static str,
    ) {
        self.emit_trace(hs_trace::HEADER_NEW_BLOCK_DEDUPED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_height(row, hs_trace::HEIGHT, height);
            insert_hash(row, hs_trace::HASH, hash);
            insert_optional_str(row, hs_trace::REASON, Some(reason));
        });
    }

    fn trace_peer_violation(&self, peer: &ZakuraPeerId, reason: HeaderSyncMisbehavior) {
        self.emit_trace(hs_trace::HEADER_PEER_VIOLATION, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_optional_str(
                row,
                hs_trace::REASON,
                Some(misbehavior_reason_label(reason)),
            );
        });
    }

    fn trace_peer_disconnect_requested(&self, peer: &ZakuraPeerId, reason: HeaderSyncMisbehavior) {
        self.emit_trace(hs_trace::HEADER_PEER_DISCONNECT_REQUESTED, |row| {
            insert_peer(row, hs_trace::PEER, peer);
            insert_optional_str(
                row,
                hs_trace::REASON,
                Some(misbehavior_reason_label(reason)),
            );
        });
    }

    fn trace_frontier_advanced(&self, height: block::Height, hash: block::Hash) {
        self.emit_trace(hs_trace::HEADER_FRONTIER_ADVANCED, |row| {
            insert_height(row, hs_trace::HEIGHT, height);
            insert_hash(row, hs_trace::HASH, hash);
        });
    }

    fn trace_frontier_reanchored(&self, height: block::Height, hash: block::Hash) {
        self.emit_trace(hs_trace::HEADER_FRONTIER_REANCHORED, |row| {
            insert_height(row, hs_trace::HEIGHT, height);
            insert_hash(row, hs_trace::HASH, hash);
        });
    }

    fn trace_missing_bodies(&self, from: block::Height, to: block::Height) {
        self.emit_trace(hs_trace::HEADER_MISSING_BODIES_REPORTED, |row| {
            insert_height(row, hs_trace::RANGE_START, from);
            insert_u64(
                row,
                hs_trace::RANGE_COUNT,
                u64::from(count_between(from, to)),
            );
        });
    }

    fn emit_trace(
        &self,
        event: &'static str,
        build: impl FnOnce(&mut serde_json::Map<String, Value>),
    ) {
        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(
                hs_trace::EVENT.to_string(),
                Value::String(event.to_string()),
            );
            build(row);
        });
    }

    fn local_status(&self) -> HeaderSyncStatus {
        HeaderSyncStatus {
            tip_height: self.state.best_header_tip,
            tip_hash: self.state.best_header_hash,
            anchor_height: self.state.anchor.0,
            max_headers_per_response: self.startup.config.advertised_max_headers_per_response(),
            max_inflight_requests: self.startup.config.advertised_max_inflight_requests(),
        }
    }

    fn cancel_covered_outstanding(&mut self) {
        let covered = self.state.schedule.clone();
        let park = Self::cancel_outstanding_matching(&mut self.state.peers, |range| {
            covered.is_covered(range)
        });
        self.park_undeliverable_drop_peers(park);
    }

    fn cancel_forward_outstanding(&mut self) {
        let park = Self::cancel_outstanding_matching(&mut self.state.peers, |range| {
            range.priority == RangePriority::Forward
        });
        self.park_undeliverable_drop_peers(park);
    }

    /// Park every peer whose covered/forward-cancel `DropExpectation` could not be
    /// delivered (full command queue or exited routine). Collected by
    /// [`Self::cancel_outstanding_matching`] and parked here, after the `&mut peers`
    /// borrow ends, since parking removes peer state.
    fn park_undeliverable_drop_peers(&mut self, park: Vec<ZakuraPeerId>) {
        for peer in park {
            // A peer may appear more than once when several of its ranges were
            // covered; the first park removes it, so later parks are no-ops.
            if self.state.peers.contains_key(&peer) {
                self.park_peer_for_undeliverable_command(&peer, "drop_expectation");
            }
        }
    }

    /// Remove every outstanding range matching `predicate` from each peer, bump its
    /// late-covered credit so a late in-flight response is absorbed rather than
    /// treated as unsolicited, and tell that peer's routine to drop the matching
    /// expectation and free its outbound slot (generation-scoped) so it can pull
    /// fresh work. Without the `DropExpectation` the routine's slot would stay
    /// occupied by a range the reactor already covered, stalling its next pull.
    ///
    /// Returns the peers whose `DropExpectation` could not be delivered (full command
    /// queue or exited routine); the caller parks them, since that reset controls
    /// request correlation and the outbound slot and must never be silently lost. The
    /// caller parks after this returns, because parking removes peer state and this
    /// holds `&mut peers`.
    fn cancel_outstanding_matching(
        peers: &mut HashMap<ZakuraPeerId, PeerHeaderState>,
        predicate: impl Fn(RangeRequest) -> bool,
    ) -> Vec<ZakuraPeerId> {
        let mut park = Vec::new();
        for (peer_id, peer) in peers.iter_mut() {
            let mut index = 0;
            while index < peer.outstanding.len() {
                if predicate(peer.outstanding[index].range) {
                    let outstanding = peer.outstanding.remove(index);
                    peer.late_covered_responses = peer.late_covered_responses.saturating_add(1);
                    if peer.generation == outstanding.generation {
                        let outcome = match peer.session.commands() {
                            Some(sink) => sink.enqueue_drop_expectation(
                                outstanding.generation,
                                outstanding.range.start_height,
                                outstanding.expected_max_count,
                            ),
                            None => HeaderSyncCommandOutcome::NoRoutine,
                        };
                        if outcome.requires_park() {
                            park.push(peer_id.clone());
                        }
                    }
                } else {
                    index += 1;
                }
            }
        }
        park
    }
}

fn header_sync_wire_error_kind(error: &HeaderSyncWireError) -> &'static str {
    match error {
        HeaderSyncWireError::OversizedPayload { .. } => "oversized_payload",
        HeaderSyncWireError::HeaderCountLimit { .. } => "header_count_limit",
        HeaderSyncWireError::BodySizeCountMismatch { .. } => "body_size_count_mismatch",
        HeaderSyncWireError::UnsolicitedHeaders => "unsolicited_headers",
        HeaderSyncWireError::ZeroHeaderRequestCount => "zero_header_request_count",
        HeaderSyncWireError::HeightOutOfRange(_) => "height_out_of_range",
        HeaderSyncWireError::UnknownMessageType(_) => "unknown_message_type",
        HeaderSyncWireError::UnknownFrameMessageType(_) => "unknown_frame_message_type",
        HeaderSyncWireError::UnsupportedFlags(_) => "unsupported_flags",
        HeaderSyncWireError::MismatchedFrameMessageType { .. } => "mismatched_frame_message_type",
        HeaderSyncWireError::TrailingBytes => "trailing_bytes",
        HeaderSyncWireError::NonContiguousHeaders => "non_contiguous_headers",
        HeaderSyncWireError::FirstHeaderDoesNotLink => "first_header_does_not_link",
        HeaderSyncWireError::WrongEquihashSolutionSize => "wrong_equihash_solution_size",
        HeaderSyncWireError::InvalidDifficultyThreshold => "invalid_difficulty_threshold",
        HeaderSyncWireError::DifficultyFilter { .. } => "difficulty_filter",
        HeaderSyncWireError::NumericOverflow(_) => "numeric_overflow",
        HeaderSyncWireError::Io(_) => "io",
        HeaderSyncWireError::Serialization(_) => "serialization",
        HeaderSyncWireError::Time(_) => "time",
        HeaderSyncWireError::Equihash(_) => "equihash",
        HeaderSyncWireError::BlockingTask(_) => "blocking_task",
    }
}

fn header_sync_candidate_target(best_header_tip: block::Height) -> block::Height {
    next_height(best_header_tip).unwrap_or(best_header_tip)
}

fn header_summary_is_useful(
    summary: HeaderSyncServiceSummary,
    target_height: block::Height,
) -> bool {
    summary.serving_headers
        && summary.inbound_slots_free > 0
        && summary.best_height >= target_height
}

fn node_id_from_header_peer_id(peer: &ZakuraPeerId) -> Option<NodeId> {
    let bytes: [u8; 32] = peer.as_bytes().try_into().ok()?;
    NodeId::from_bytes(&bytes).ok()
}

#[cfg(test)]
fn trace_header_sync_message_fields(
    row: &mut serde_json::Map<String, Value>,
    msg: &HeaderSyncMessage,
) {
    match msg {
        HeaderSyncMessage::Status(status) => {
            insert_height(row, hs_trace::HEIGHT, status.tip_height);
            insert_hash(row, hs_trace::HASH, status.tip_hash);
            insert_height(row, hs_trace::RANGE_START, status.anchor_height);
            insert_u64(
                row,
                hs_trace::ADVERTISED_CAP,
                u64::from(status.max_headers_per_response),
            );
            insert_u64(
                row,
                hs_trace::IN_FLIGHT_COUNT,
                u64::from(status.max_inflight_requests),
            );
        }
        HeaderSyncMessage::Headers { headers, .. } => {
            insert_u64(row, hs_trace::RANGE_COUNT, headers.len() as u64);
        }
        HeaderSyncMessage::GetHeaders {
            start_height,
            count,
        } => {
            insert_height(row, hs_trace::RANGE_START, *start_height);
            insert_u64(row, hs_trace::RANGE_COUNT, u64::from(*count));
        }
        HeaderSyncMessage::NewBlock(block) => {
            insert_hash(row, hs_trace::HASH, block.hash());
            if let Some(height) = block.coinbase_height() {
                insert_height(row, hs_trace::HEIGHT, height);
            }
        }
    }
}

#[cfg(test)]
fn header_sync_message_label(msg: &HeaderSyncMessage) -> &'static str {
    match msg {
        HeaderSyncMessage::Status(_) => "status",
        HeaderSyncMessage::Headers { .. } => "headers",
        HeaderSyncMessage::GetHeaders { .. } => "get_headers",
        HeaderSyncMessage::NewBlock(_) => "new_block",
    }
}
