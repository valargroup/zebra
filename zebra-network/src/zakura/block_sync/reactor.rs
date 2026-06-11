use super::{config::*, events::*, reorder::*, scheduler::*, state::*, wire::*, *};
use crate::zakura::{ServiceAdmissionDecision, ServicePeerDirection, ServicePeerSnapshot};

/// Spawn a block-sync reactor and return its handle plus action stream.
pub fn spawn_block_sync_reactor(
    startup: BlockSyncStartup,
) -> (
    BlockSyncHandle,
    mpsc::Receiver<BlockSyncAction>,
    JoinHandle<()>,
) {
    let state = BlockSyncState::new(&startup);
    let (events_tx, events_rx) =
        mpsc::channel(startup.config.peer_limits.inbound_queue_depth.max(1));
    let (lifecycle_tx, lifecycle_rx) = mpsc::unbounded_channel();
    let (actions_tx, actions_rx) = mpsc::channel(128);
    let (peers_tx, peers_rx) = watch::channel(state.peer_snapshot(startup.config.peer_limits));

    let handle = BlockSyncHandle {
        events: events_tx,
        lifecycle: lifecycle_tx,
        peers: peers_rx,
    };
    let reactor = BlockSyncReactor {
        startup,
        state,
        events: events_rx,
        lifecycle: lifecycle_rx,
        actions: actions_tx,
        peers: peers_tx,
    };
    let task = tokio::spawn(reactor.run());

    (handle, actions_rx, task)
}

#[derive(Debug)]
pub(super) struct BlockSyncReactor {
    startup: BlockSyncStartup,
    state: BlockSyncState,
    events: mpsc::Receiver<BlockSyncEvent>,
    lifecycle: mpsc::UnboundedReceiver<BlockSyncEvent>,
    actions: mpsc::Sender<BlockSyncAction>,
    peers: watch::Sender<ServicePeerSnapshot>,
}

impl BlockSyncReactor {
    async fn run(mut self) {
        let mut header_tip = self.startup.header_tip.clone();
        let mut header_tip_open = true;
        let mut ticks = time::interval(self.startup.config.request_timeout);

        loop {
            tokio::select! {
                biased;
                _ = self.startup.shutdown.cancelled() => break,
                event = self.lifecycle.recv() => {
                    let Some(event) = event else { break };
                    self.handle_event(event).await;
                }
                event = self.events.recv() => {
                    let Some(event) = event else { break };
                    self.handle_event(event).await;
                }
                changed = header_tip.changed(), if header_tip_open => {
                    match changed {
                        Ok(()) => {
                            let (height, hash) = *header_tip.borrow_and_update();
                            self.handle_header_tip_changed(height, hash).await;
                        }
                        Err(_) => header_tip_open = false,
                    }
                }
                _ = ticks.tick() => self.handle_timeouts().await,
            }
        }
    }

    async fn handle_event(&mut self, event: BlockSyncEvent) {
        match event {
            BlockSyncEvent::PeerConnected(session) => self.handle_peer_connected(session).await,
            BlockSyncEvent::PeerDisconnected(peer) => self.handle_peer_disconnected(peer),
            BlockSyncEvent::WireMessage { peer, msg } => self.handle_wire_message(peer, msg).await,
            BlockSyncEvent::WireDecodeFailed { peer, error } => {
                self.handle_wire_decode_failed(peer, error).await
            }
            BlockSyncEvent::HeaderTipChanged { height, hash } => {
                self.handle_header_tip_changed(height, hash).await
            }
            BlockSyncEvent::StateFrontiersChanged(frontiers) => {
                self.handle_state_frontiers_changed(frontiers).await
            }
            BlockSyncEvent::NeededBlocks(blocks) => {
                self.handle_needed_blocks(blocks).await;
            }
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

    fn admitted_count(&self, direction: ServicePeerDirection) -> usize {
        self.state
            .peers
            .values()
            .filter(|peer| peer.direction == direction)
            .count()
    }

    fn publish_peer_snapshot(&self) {
        let _ = self
            .peers
            .send(self.state.peer_snapshot(self.startup.config.peer_limits));
    }

    async fn handle_peer_connected(&mut self, session: BlockSyncPeerSession) {
        let peer = session.peer_id().clone();
        let direction = session.direction();
        let decision = self.admission_decision_for(&peer, direction);
        if decision != ServiceAdmissionDecision::Admit {
            self.state.parked_peers.insert(peer);
            session.cancel_token().cancel();
            self.publish_peer_snapshot();
            return;
        }

        self.state.parked_peers.remove(&peer);
        self.state
            .peers
            .entry(peer.clone())
            .and_modify(|peer_state| {
                peer_state.session = session.clone();
                peer_state.direction = direction;
            })
            .or_insert_with(|| PeerBlockState::new(session, &self.startup.config));
        self.publish_peer_snapshot();
        self.send_status(&peer).await;
        self.schedule().await;
    }

    fn handle_peer_disconnected(&mut self, peer: ZakuraPeerId) {
        if let Some(peer_state) = self.state.peers.remove(&peer) {
            for outstanding in peer_state.outstanding {
                self.state.budget.release(outstanding.reserved_bytes());
                self.state.schedule.retry(outstanding.request);
            }
        }
        self.state.parked_peers.remove(&peer);
        self.state.schedule.forget_peer(&peer);
        self.publish_peer_snapshot();
    }

    async fn handle_header_tip_changed(&mut self, height: block::Height, hash: block::Hash) {
        self.state.best_header_tip = height;
        self.state.best_header_hash = hash;
        self.query_needed_blocks().await;
    }

    async fn handle_state_frontiers_changed(&mut self, frontiers: BlockSyncFrontiers) {
        self.state.finalized_height = frontiers.finalized_height;
        self.state.verified_block_hash = frontiers.verified_block_hash;
        if frontiers.verified_block_tip != self.state.verified_block_tip {
            self.state.verified_block_tip = frontiers.verified_block_tip;
            self.release_contiguous_blocks().await;
        }
        self.query_needed_blocks().await;
    }

    async fn handle_needed_blocks(&mut self, blocks: Vec<BlockSyncBlockMeta>) {
        let needed = blocks
            .into_iter()
            .map(|block| NeededBlock {
                height: block.height,
                hash: block.hash,
                size: block.size,
            })
            .collect();
        self.state.schedule.refresh_needed(needed);
        self.schedule().await;
    }

    async fn handle_wire_decode_failed(
        &mut self,
        peer: ZakuraPeerId,
        error: Arc<BlockSyncWireError>,
    ) {
        tracing::debug!(?peer, ?error, "malformed Zakura block-sync frame");
        self.report_misbehavior(peer, BlockSyncMisbehavior::MalformedMessage)
            .await;
    }

    async fn handle_wire_message(&mut self, peer: ZakuraPeerId, msg: BlockSyncMessage) {
        if self.state.parked_peers.contains(&peer) {
            return;
        }

        match msg {
            BlockSyncMessage::Status(status) => self.handle_status(peer, status).await,
            BlockSyncMessage::Block(block) => self.handle_block(peer, block).await,
            BlockSyncMessage::BlocksDone {
                start_height,
                returned: _,
            } => self.handle_blocks_done(peer, start_height).await,
            BlockSyncMessage::RangeUnavailable { .. } => {
                self.report_misbehavior(peer, BlockSyncMisbehavior::RangeUnavailable)
                    .await;
            }
            BlockSyncMessage::GetBlocks { .. } => {
                self.report_misbehavior(peer, BlockSyncMisbehavior::GetBlocksSpam)
                    .await;
            }
        }
    }

    async fn handle_status(&mut self, peer: ZakuraPeerId, status: BlockSyncStatus) {
        if status.servable_low > status.servable_high {
            self.report_misbehavior(peer, BlockSyncMisbehavior::InvalidStatus)
                .await;
            return;
        }
        let Some(peer_state) = self.state.peers.get_mut(&peer) else {
            return;
        };
        let servable_range_grew = status.servable_high > peer_state.servable_high
            || status.servable_low < peer_state.servable_low;
        if !peer_state.inbound_status.try_take(Instant::now()) && !servable_range_grew {
            self.report_misbehavior(peer, BlockSyncMisbehavior::StatusSpam)
                .await;
            return;
        }
        peer_state.servable_low = status.servable_low;
        peer_state.servable_high = status.servable_high;
        peer_state.max_blocks_per_response =
            clamp_advertised_blocks(status.max_blocks_per_response);
        peer_state.max_inflight_requests = clamp_advertised_inflight(status.max_inflight_requests);
        peer_state.max_response_bytes = clamp_advertised_response_bytes(status.max_response_bytes);
        peer_state.received_status = true;
        self.schedule().await;
    }

    async fn handle_block(&mut self, peer: ZakuraPeerId, block: Arc<block::Block>) {
        let hash = block.hash();
        let Some(height) = block.coinbase_height() else {
            self.report_misbehavior(peer, BlockSyncMisbehavior::InvalidBlock)
                .await;
            return;
        };

        let Some(peer_state) = self.state.peers.get_mut(&peer) else {
            self.report_misbehavior(peer, BlockSyncMisbehavior::UnsolicitedBlock)
                .await;
            return;
        };
        let Some(index) = peer_state.outstanding_index_for_height(height) else {
            self.report_misbehavior(peer, BlockSyncMisbehavior::UnsolicitedBlock)
                .await;
            return;
        };
        let outstanding = &peer_state.outstanding[index];
        if outstanding.has_received(height) {
            tracing::debug!(?peer, ?height, "ignoring duplicate block-sync body frame");
            return;
        }

        if outstanding.request.expected_hash(height) != Some(hash) {
            self.report_misbehavior(peer, BlockSyncMisbehavior::InvalidBlock)
                .await;
            return;
        }
        let estimated_bytes = outstanding.estimated_bytes_for_height(height).unwrap_or(0);
        let retry_request = outstanding.request.single_height_retry(height);

        let serialized_bytes = match block.zcash_serialize_to_vec() {
            Ok(bytes) => bytes.len() as u64,
            Err(error) => {
                tracing::debug!(?error, "failed to serialize decoded block-sync body");
                self.report_misbehavior(peer, BlockSyncMisbehavior::InvalidBlock)
                    .await;
                return;
            }
        };
        if serialized_bytes
            > tolerated_bytes(
                estimated_bytes,
                self.startup.config.size_deviation_tolerance,
            )
        {
            self.report_misbehavior(peer.clone(), BlockSyncMisbehavior::SizeMismatch)
                .await;
        }

        self.state.budget.release(estimated_bytes);
        let mut completed = None;
        if let Some(peer_state) = self.state.peers.get_mut(&peer) {
            if let Some(outstanding) = peer_state.outstanding.get_mut(index) {
                outstanding.mark_received(height);
                if outstanding.is_complete() {
                    completed = Some(peer_state.outstanding.remove(index).request);
                }
            }
        }
        if let Some(request) = completed {
            self.state.schedule.clear_assignment(&request);
        }

        if height <= self.state.verified_block_tip || self.state.reorder.contains(height) {
            self.release_contiguous_blocks().await;
            self.schedule().await;
            return;
        }

        match self
            .state
            .reorder
            .insert(height, block, serialized_bytes, &mut self.state.budget)
        {
            ReorderInsertResult::Inserted | ReorderInsertResult::Duplicate => {}
            ReorderInsertResult::BudgetFull => {
                if let Some(request) = retry_request {
                    self.state.schedule.retry(request);
                }
                tracing::debug!(
                    ?peer,
                    ?height,
                    serialized_bytes,
                    "dropping block-sync body because local byte budget is full"
                );
                self.schedule().await;
                return;
            }
        }
        self.release_contiguous_blocks().await;
        self.schedule().await;
    }

    async fn handle_blocks_done(&mut self, peer: ZakuraPeerId, start_height: block::Height) {
        let Some(peer_state) = self.state.peers.get_mut(&peer) else {
            self.report_misbehavior(peer, BlockSyncMisbehavior::UnsolicitedDone)
                .await;
            return;
        };
        if let Some(index) = peer_state
            .outstanding
            .iter()
            .position(|outstanding| outstanding.request.start_height == start_height)
        {
            let outstanding = peer_state.outstanding.remove(index);
            self.state.budget.release(outstanding.reserved_bytes());
            self.state.schedule.clear_assignment(&outstanding.request);
        }
        self.schedule().await;
    }

    async fn handle_timeouts(&mut self) {
        let now = Instant::now();
        let mut timed_out = Vec::new();
        for peer in self.state.peers.values_mut() {
            let mut index = 0;
            while index < peer.outstanding.len() {
                if peer.outstanding[index].deadline <= now {
                    timed_out.push(peer.outstanding.remove(index));
                } else {
                    index += 1;
                }
            }
        }

        for outstanding in timed_out {
            self.state.budget.release(outstanding.reserved_bytes());
            self.state.schedule.retry(outstanding.request);
        }
        self.schedule().await;
    }

    async fn query_needed_blocks(&mut self) {
        if !self.startup.state_queries_enabled
            || self.state.best_header_tip <= self.state.verified_block_tip
        {
            return;
        }
        let _ = self
            .actions
            .send(BlockSyncAction::QueryNeededBlocks {
                verified_block_tip: self.state.verified_block_tip,
                best_header_tip: self.state.best_header_tip,
            })
            .await;
    }

    async fn schedule(&mut self) {
        let mut peer_ids: Vec<_> = self.state.peers.keys().cloned().collect();
        peer_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));

        for peer_id in peer_ids {
            let Some(peer) = self.state.peers.get(&peer_id) else {
                continue;
            };
            if !peer.received_status || peer.available_slots() == 0 {
                continue;
            }
            let Some(request) =
                self.state
                    .schedule
                    .next_for_peer(&peer_id, peer, &mut self.state.budget)
            else {
                continue;
            };

            let Some(peer) = self.state.peers.get(&peer_id) else {
                continue;
            };
            if let Err(error) = peer
                .session
                .try_send_get_blocks(request.start_height, request.count)
            {
                tracing::debug!(
                    peer = ?peer_id,
                    start_height = ?request.start_height,
                    count = request.count,
                    ?error,
                    "failed to queue Zakura block-sync GetBlocks"
                );
                self.state.budget.release(request.estimated_bytes);
                self.state.schedule.retry(request);
                continue;
            }

            let deadline = Instant::now() + self.startup.config.request_timeout;
            if let Some(peer) = self.state.peers.get_mut(&peer_id) {
                peer.outstanding.push(OutstandingBlockRange {
                    request: request.clone(),
                    deadline,
                    received: HashSet::new(),
                });
            }
            let _ = self
                .actions
                .send(BlockSyncAction::SendMessage {
                    peer: peer_id,
                    msg: BlockSyncMessage::GetBlocks {
                        start_height: request.start_height,
                        count: request.count,
                    },
                })
                .await;
        }
    }

    async fn release_contiguous_blocks(&mut self) {
        let released = self
            .state
            .reorder
            .drain_contiguous_prefix(self.state.verified_block_tip, &mut self.state.budget);
        for (height, block) in released {
            self.state.verified_block_tip = height;
            self.state.verified_block_hash = block.hash();
            self.state.schedule.mark_height_covered(height);
            let _ = self
                .actions
                .send(BlockSyncAction::SubmitBlock { block })
                .await;
        }
    }

    async fn send_status(&self, peer: &ZakuraPeerId) {
        let Some(peer_state) = self.state.peers.get(peer) else {
            return;
        };
        let status = self.local_status();
        let _ = peer_state.session.try_send_status(status);
        let _ = self
            .actions
            .send(BlockSyncAction::SendMessage {
                peer: peer.clone(),
                msg: BlockSyncMessage::Status(status),
            })
            .await;
    }

    fn local_status(&self) -> BlockSyncStatus {
        BlockSyncStatus {
            servable_low: block::Height::MIN,
            servable_high: self.state.verified_block_tip,
            tip_hash: self.state.verified_block_hash,
            max_blocks_per_response: self.startup.config.advertised_max_blocks_per_response(),
            max_inflight_requests: self.startup.config.advertised_max_inflight_requests(),
            max_response_bytes: self.startup.config.advertised_max_response_bytes(),
        }
    }

    async fn report_misbehavior(&mut self, peer: ZakuraPeerId, reason: BlockSyncMisbehavior) {
        if let Some(peer_state) = self.state.peers.get_mut(&peer) {
            peer_state.misbehavior = peer_state.misbehavior.saturating_add(1);
        }
        let _ = self
            .actions
            .send(BlockSyncAction::Misbehavior { peer, reason })
            .await;
    }
}

fn tolerated_bytes(reserved_bytes: u64, tolerance_percent: u32) -> u64 {
    reserved_bytes.saturating_mul(u64::from(tolerance_percent.max(100))) / 100
}
