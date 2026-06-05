//! A download stream for Zebra's block syncer.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    convert,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{
    future::{FutureExt, TryFutureExt},
    stream::{FuturesUnordered, Stream, StreamExt},
};
use pin_project::pin_project;
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle, time::timeout};
use tower::{hedge, Service, ServiceExt};
use tracing_futures::Instrument;

use zebra_chain::{
    block::{self, Height, HeightDiff},
    chain_tip::ChainTip,
};
use zebra_network::{self as zn, PeerSocketAddr};
use zebra_state as zs;

use crate::components::sync::{
    FINAL_CHECKPOINT_BLOCK_VERIFY_TIMEOUT, FINAL_CHECKPOINT_BLOCK_VERIFY_TIMEOUT_LIMIT,
};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
type DownloadResult = Result<(Height, block::Hash), (BlockDownloadVerifyError, block::Hash)>;
type BatchDownloadResult = DownloadResult;

struct BlockVerifyContext<ZV, ZSTip> {
    verifier: ZV,
    latest_chain_tip: ZSTip,
    lookahead_limit: usize,
    max_checkpoint_height: Height,
    past_lookahead_limit_sender: Arc<std::sync::Mutex<watch::Sender<bool>>>,
    past_lookahead_limit_receiver: zs::WatchReceiver<bool>,
}

/// A multiplier used to calculate the extra number of blocks we allow in the
/// verifier, state, and block commit pipelines, on top of the lookahead limit.
///
/// The extra number of blocks is calculated using
/// `lookahead_limit * VERIFICATION_PIPELINE_SCALING_MULTIPLIER`.
///
/// This allows the verifier and state queues, and the block commit channel,
/// to hold a few extra tips responses worth of blocks,
/// even if the syncer queue is full. Any unused capacity is shared between both queues.
///
/// If this capacity is exceeded, the downloader will tell the syncer to pause new downloads.
///
/// Since the syncer queue is limited to the `lookahead_limit`,
/// the rest of the capacity is reserved for the other queues.
/// There is no reserved capacity for the syncer queue:
/// if the other queues stay full, the syncer will eventually time out and reset.
pub const VERIFICATION_PIPELINE_SCALING_MULTIPLIER: usize = 2;

/// The maximum height difference between Zebra's state tip and a downloaded block.
/// Blocks higher than this will get dropped and return an error.
pub const VERIFICATION_PIPELINE_DROP_LIMIT: HeightDiff = 50_000;

#[derive(Copy, Clone, Debug)]
pub(super) struct AlwaysHedge;

impl<Request: Clone> hedge::Policy<Request> for AlwaysHedge {
    fn can_retry(&self, _req: &Request) -> bool {
        true
    }
    fn clone_request(&self, req: &Request) -> Option<Request> {
        Some(req.clone())
    }
}

/// Errors that can occur while downloading and verifying a block.
#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum BlockDownloadVerifyError {
    #[error("permanent readiness error from the network service: {error:?}")]
    NetworkServiceError {
        #[source]
        error: BoxError,
    },

    #[error("permanent readiness error from the verifier service: {error:?}")]
    VerifierServiceError {
        #[source]
        error: BoxError,
    },

    #[error("duplicate block hash queued for download: {hash:?}")]
    DuplicateBlockQueuedForDownload { hash: block::Hash },

    #[error("error downloading block: {error:?} {hash:?}")]
    DownloadFailed {
        #[source]
        error: BoxError,
        hash: block::Hash,
    },

    /// A downloaded block was a long way ahead of the state chain tip.
    /// This error should be very rare during normal operation.
    ///
    /// We need to reset the syncer on this error, to allow the verifier and state to catch up,
    /// or prevent it following a bad chain.
    ///
    /// If we don't reset the syncer on this error, it will continue downloading blocks from a bad
    /// chain, or blocks far ahead of the current state tip.
    #[error("downloaded block was too far ahead of the chain tip: {height:?} {hash:?}")]
    AboveLookaheadHeightLimit {
        height: block::Height,
        hash: block::Hash,
        advertiser_addr: Option<PeerSocketAddr>,
    },

    #[error("downloaded block was too far behind the chain tip: {height:?} {hash:?}")]
    BehindTipHeightLimit {
        height: block::Height,
        hash: block::Hash,
    },

    #[error("downloaded block had an invalid height: {hash:?}")]
    InvalidHeight {
        hash: block::Hash,
        advertiser_addr: Option<PeerSocketAddr>,
    },

    #[error("block failed consensus validation: {error:?} {height:?} {hash:?}")]
    Invalid {
        #[source]
        error: zebra_consensus::router::RouterError,
        height: block::Height,
        hash: block::Hash,
        advertiser_addr: Option<PeerSocketAddr>,
    },

    #[error("block validation request failed: {error:?} {height:?} {hash:?}")]
    ValidationRequestError {
        #[source]
        error: BoxError,
        height: block::Height,
        hash: block::Hash,
    },

    #[error("block download & verification was cancelled during download: {hash:?}")]
    CancelledDuringDownload { hash: block::Hash },

    #[error(
        "block download & verification was cancelled while waiting for the verifier service: \
         to become ready: {height:?} {hash:?}"
    )]
    CancelledAwaitingVerifierReadiness {
        height: block::Height,
        hash: block::Hash,
    },

    #[error(
        "block download & verification was cancelled during verification: {height:?} {hash:?}"
    )]
    CancelledDuringVerification {
        height: block::Height,
        hash: block::Hash,
    },

    #[error(
        "timeout during service readiness, download, verification, or internal downloader operation"
    )]
    Timeout,
}

impl BlockDownloadVerifyError {
    /// Returns the missing block hash for network `notfound` download failures.
    pub(super) fn not_found_download_hash(&self) -> Option<block::Hash> {
        match self {
            BlockDownloadVerifyError::DownloadFailed { error, hash }
                if error
                    .downcast_ref::<zn::SharedPeerError>()
                    .is_some_and(shared_peer_error_is_not_found) =>
            {
                Some(*hash)
            }
            _ => None,
        }
    }

    /// Returns the block hash for download failures caused by no ready peers.
    pub(super) fn no_ready_peers_download_hash(&self) -> Option<block::Hash> {
        match self {
            BlockDownloadVerifyError::DownloadFailed { error, hash }
                if error
                    .downcast_ref::<zn::SharedPeerError>()
                    .is_some_and(shared_peer_error_is_no_ready_peers) =>
            {
                Some(*hash)
            }
            _ => None,
        }
    }

    /// Returns the block hash for download failures caused by busy source peers.
    pub(super) fn preferred_peers_busy_download_hash(&self) -> Option<block::Hash> {
        match self {
            BlockDownloadVerifyError::DownloadFailed { error, hash }
                if error
                    .downcast_ref::<zn::SharedPeerError>()
                    .is_some_and(shared_peer_error_is_preferred_peers_busy) =>
            {
                Some(*hash)
            }
            _ => None,
        }
    }
}

fn shared_peer_error_is_not_found(error: &zn::SharedPeerError) -> bool {
    let inner = error.inner_debug();

    inner.contains("NotFoundResponse") || inner.contains("NotFoundRegistry")
}

fn shared_peer_error_is_no_ready_peers(error: &zn::SharedPeerError) -> bool {
    error.inner_debug().contains("NoReadyPeers")
}

fn shared_peer_error_is_preferred_peers_busy(error: &zn::SharedPeerError) -> bool {
    error.inner_debug().contains("PreferredPeersBusy")
}

fn classify_download_error(error: &BoxError) -> &'static str {
    if let Some(error) = error.downcast_ref::<zn::SharedPeerError>() {
        let inner = error.inner_debug();

        if inner.contains("NotFoundResponse") {
            "not_found_response"
        } else if inner.contains("NotFoundRegistry") {
            "not_found_registry"
        } else if inner.contains("NoReadyPeers") {
            "no_ready_peers"
        } else if inner.contains("PreferredPeersBusy") {
            "preferred_peers_busy"
        } else if inner.contains("Timeout") {
            "timeout"
        } else {
            "shared_peer_error"
        }
    } else {
        "error"
    }
}

fn clone_download_error(error: &BoxError) -> BoxError {
    if let Some(error) = error.downcast_ref::<zn::SharedPeerError>() {
        return Box::new(error.clone());
    }

    std::io::Error::other(format!("{error:?}")).into()
}

impl From<tokio::time::error::Elapsed> for BlockDownloadVerifyError {
    fn from(_value: tokio::time::error::Elapsed) -> Self {
        BlockDownloadVerifyError::Timeout
    }
}

/// Represents a [`Stream`] of download and verification tasks during chain sync.
#[pin_project]
#[derive(Debug)]
pub struct Downloads<ZN, ZV, ZSTip>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Sync + 'static,
    ZN::Future: Send,
    ZV: Service<zebra_consensus::Request, Response = block::Hash, Error = BoxError>
        + Send
        + Sync
        + Clone
        + 'static,
    ZV::Future: Send,
    ZSTip: ChainTip + Clone + Send + 'static,
{
    // Services
    //
    /// A service that forwards requests to connected peers, and returns their
    /// responses.
    network: ZN,

    /// A service that verifies downloaded blocks.
    verifier: ZV,

    /// Allows efficient access to the best tip of the blockchain.
    latest_chain_tip: ZSTip,

    // Configuration
    //
    /// The configured lookahead limit, after applying the minimum limit.
    lookahead_limit: usize,

    /// The largest block height for the checkpoint verifier, based on the current config.
    max_checkpoint_height: Height,

    // Shared syncer state
    //
    /// Sender that is set to `true` when the downloader is past the lookahead limit.
    /// This is based on the downloaded block height and the state tip height.
    past_lookahead_limit_sender: Arc<std::sync::Mutex<watch::Sender<bool>>>,

    /// Receiver for `past_lookahead_limit_sender`, which is used to avoid accessing the mutex.
    past_lookahead_limit_receiver: zs::WatchReceiver<bool>,

    // Internal downloads state
    //
    /// A list of pending block download and verify tasks.
    #[pin]
    pending: FuturesUnordered<
        JoinHandle<Result<(Height, block::Hash), (BlockDownloadVerifyError, block::Hash)>>,
    >,

    /// A list of pending batch download tasks.
    #[pin]
    batch_pending: FuturesUnordered<JoinHandle<Vec<BatchDownloadResult>>>,

    /// Per-block results from completed batch tasks.
    ready_results: VecDeque<DownloadResult>,

    /// A list of channels that can be used to cancel pending block download and
    /// verify tasks.
    cancel_handles: HashMap<block::Hash, watch::Sender<bool>>,
}

impl<ZN, ZV, ZSTip> Stream for Downloads<ZN, ZV, ZSTip>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Sync + 'static,
    ZN::Future: Send,
    ZV: Service<zebra_consensus::Request, Response = block::Hash, Error = BoxError>
        + Send
        + Sync
        + Clone
        + 'static,
    ZV::Future: Send,
    ZSTip: ChainTip + Clone + Send + 'static,
{
    type Item = Result<(Height, block::Hash), BlockDownloadVerifyError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            if let Some(result) = this.ready_results.pop_front() {
                return match result {
                    Ok((height, hash)) => {
                        this.cancel_handles.remove(&hash);

                        Poll::Ready(Some(Ok((height, hash))))
                    }
                    Err((e, hash)) => {
                        this.cancel_handles.remove(&hash);
                        Poll::Ready(Some(Err(e)))
                    }
                };
            }

            if let Poll::Ready(Some(join_result)) = this.batch_pending.as_mut().poll_next(cx) {
                let batch_results = join_result.expect("batch download tasks must not panic");

                for batch_result in batch_results {
                    let hash = match &batch_result {
                        Ok((_, hash)) | Err((_, hash)) => *hash,
                    };

                    if this.cancel_handles.contains_key(&hash) {
                        this.ready_results.push_back(batch_result);
                    } else {
                        warn!(
                            %hash,
                            "dropping stale batch result without cancellation handle"
                        );
                        metrics::counter!("sync.block.download.batch.stale.result.count")
                            .increment(1);
                    }
                }

                continue;
            }

            if let Poll::Ready(Some(join_result)) = this.pending.as_mut().poll_next(cx) {
                match join_result.expect("block download and verify tasks must not panic") {
                    Ok((height, hash)) => {
                        this.cancel_handles.remove(&hash);

                        return Poll::Ready(Some(Ok((height, hash))));
                    }
                    Err((e, hash)) => {
                        this.cancel_handles.remove(&hash);
                        return Poll::Ready(Some(Err(e)));
                    }
                }
            } else if this.pending.is_empty() && this.batch_pending.is_empty() {
                return Poll::Ready(None);
            } else {
                return Poll::Pending;
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let pending = self.cancel_handles.len();
        (pending, Some(pending))
    }
}

impl<ZN, ZV, ZSTip> Downloads<ZN, ZV, ZSTip>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Sync + 'static,
    ZN::Future: Send,
    ZV: Service<zebra_consensus::Request, Response = block::Hash, Error = BoxError>
        + Send
        + Sync
        + Clone
        + 'static,
    ZV::Future: Send,
    ZSTip: ChainTip + Clone + Send + 'static,
{
    /// Initialize a new download stream with the provided `network` and
    /// `verifier` services.
    ///
    /// Uses the `latest_chain_tip` and `lookahead_limit` to drop blocks
    /// that are too far ahead of the current state tip.
    /// Uses `max_checkpoint_height` to work around a known block timeout (#5125).
    ///
    /// The [`Downloads`] stream is agnostic to the network policy, so retry and
    /// timeout limits should be applied to the `network` service passed into
    /// this constructor.
    pub fn new(
        network: ZN,
        verifier: ZV,
        latest_chain_tip: ZSTip,
        past_lookahead_limit_sender: watch::Sender<bool>,
        lookahead_limit: usize,
        max_checkpoint_height: Height,
    ) -> Self {
        let past_lookahead_limit_receiver =
            zs::WatchReceiver::new(past_lookahead_limit_sender.subscribe());

        Self {
            network,
            verifier,
            latest_chain_tip,
            lookahead_limit,
            max_checkpoint_height,
            past_lookahead_limit_sender: Arc::new(std::sync::Mutex::new(
                past_lookahead_limit_sender,
            )),
            past_lookahead_limit_receiver,
            pending: FuturesUnordered::new(),
            batch_pending: FuturesUnordered::new(),
            ready_results: VecDeque::new(),
            cancel_handles: HashMap::new(),
        }
    }

    async fn verify_downloaded_block(
        context: BlockVerifyContext<ZV, ZSTip>,
        mut cancel_rx: watch::Receiver<bool>,
        hash: block::Hash,
        block: Arc<block::Block>,
        advertiser_addr: Option<PeerSocketAddr>,
    ) -> Result<(Height, block::Hash), BlockDownloadVerifyError> {
        let BlockVerifyContext {
            mut verifier,
            latest_chain_tip,
            lookahead_limit,
            max_checkpoint_height,
            past_lookahead_limit_sender,
            past_lookahead_limit_receiver,
        } = context;

        let tip_height = latest_chain_tip.best_tip_height();

        let (lookahead_drop_height, lookahead_pause_height, lookahead_reset_height) =
            if let Some(tip_height) = tip_height {
                let lookahead_pause = HeightDiff::try_from(
                    lookahead_limit + lookahead_limit * VERIFICATION_PIPELINE_SCALING_MULTIPLIER,
                )
                .expect("fits in HeightDiff");

                (
                    (tip_height + VERIFICATION_PIPELINE_DROP_LIMIT)
                        .expect("tip is much lower than Height::MAX"),
                    (tip_height + lookahead_pause).expect("tip is much lower than Height::MAX"),
                    (tip_height + lookahead_pause / 2).expect("tip is much lower than Height::MAX"),
                )
            } else {
                let genesis_drop = VERIFICATION_PIPELINE_DROP_LIMIT
                    .try_into()
                    .expect("fits in u32");
                let genesis_lookahead = u32::try_from(lookahead_limit - 1).expect("fits in u32");

                (
                    block::Height(genesis_drop),
                    block::Height(genesis_lookahead),
                    block::Height(genesis_lookahead / 2),
                )
            };

        let min_accepted_height = tip_height
            .map(|tip_height| {
                block::Height(tip_height.0.saturating_sub(zs::MAX_BLOCK_REORG_HEIGHT))
            })
            .unwrap_or(block::Height(0));

        let block_height = if let Some(block_height) = block.coinbase_height() {
            block_height
        } else {
            debug!(
                ?hash,
                "synced block with no height: dropped downloaded block"
            );
            metrics::counter!("sync.no.height.dropped.block.count").increment(1);

            return Err(BlockDownloadVerifyError::InvalidHeight {
                hash,
                advertiser_addr,
            });
        };

        if block_height > lookahead_drop_height {
            Err(BlockDownloadVerifyError::AboveLookaheadHeightLimit {
                height: block_height,
                hash,
                advertiser_addr,
            })?;
        } else if block_height > lookahead_pause_height {
            if !past_lookahead_limit_receiver.cloned_watch_data() {
                info!(
                    ?hash,
                    ?block_height,
                    ?tip_height,
                    ?lookahead_pause_height,
                    ?lookahead_reset_height,
                    lookahead_limit = ?lookahead_limit,
                    "synced block height too far ahead of the tip: \
                     waiting for downloaded blocks to commit to the state",
                );

                let _ = past_lookahead_limit_sender
                    .lock()
                    .expect(
                        "thread panicked while holding the past_lookahead_limit_sender mutex guard",
                    )
                    .send(true);
            } else {
                debug!(
                    ?hash,
                    ?block_height,
                    ?tip_height,
                    ?lookahead_pause_height,
                    ?lookahead_reset_height,
                    lookahead_limit = ?lookahead_limit,
                    "synced block height too far ahead of the tip: \
                     waiting for downloaded blocks to commit to the state",
                );
            }

            metrics::counter!("sync.max.height.limit.paused.count").increment(1);
        } else if block_height <= lookahead_reset_height
            && past_lookahead_limit_receiver.cloned_watch_data()
        {
            let _ = past_lookahead_limit_sender
                .lock()
                .expect("thread panicked while holding the past_lookahead_limit_sender mutex guard")
                .send(false);
            metrics::counter!("sync.max.height.limit.reset.count").increment(1);

            metrics::counter!("sync.max.height.limit.reset.attempt.count").increment(1);
        }

        if block_height < min_accepted_height {
            debug!(
                ?hash,
                ?block_height,
                ?tip_height,
                ?min_accepted_height,
                behind_tip_limit = ?zs::MAX_BLOCK_REORG_HEIGHT,
                "synced block height behind the finalized tip: dropped downloaded block"
            );
            metrics::counter!("gossip.min.height.limit.dropped.block.count").increment(1);

            Err(BlockDownloadVerifyError::BehindTipHeightLimit {
                height: block_height,
                hash,
            })?;
        }

        let readiness = verifier.ready();
        let verifier = tokio::select! {
            biased;
            _ = cancel_rx.changed() => {
                trace!("task cancelled waiting for verifier service readiness");
                metrics::counter!("sync.cancelled.verify.ready.count").increment(1);
                return Err(BlockDownloadVerifyError::CancelledAwaitingVerifierReadiness { height: block_height, hash })
            }
            verifier = readiness => verifier,
        };

        let verify_start = std::time::Instant::now();
        let mut rsp = verifier
            .map_err(|error| BlockDownloadVerifyError::VerifierServiceError { error })?
            .call(zebra_consensus::Request::Commit(block))
            .boxed();

        let short_timeout_max = (max_checkpoint_height
            + FINAL_CHECKPOINT_BLOCK_VERIFY_TIMEOUT_LIMIT)
            .expect("checkpoint block height is in valid range");
        if block_height >= max_checkpoint_height && block_height <= short_timeout_max {
            rsp = timeout(FINAL_CHECKPOINT_BLOCK_VERIFY_TIMEOUT, rsp)
                .map_err(|timeout| {
                    format!("initial fully verified block timed out: retrying: {timeout:?}").into()
                })
                .map(|nested_result| nested_result.and_then(convert::identity))
                .boxed();
        }

        let verification = tokio::select! {
            biased;
            _ = cancel_rx.changed() => {
                trace!("task cancelled prior to verification");
                metrics::counter!("sync.cancelled.verify.count").increment(1);
                metrics::histogram!("sync.block.verify.duration_seconds", "result" => "cancelled")
                    .record(verify_start.elapsed().as_secs_f64());
                return Err(BlockDownloadVerifyError::CancelledDuringVerification { height: block_height, hash })
            }
            verification = rsp => verification,
        };

        let verify_result = if verification.is_ok() {
            "success"
        } else {
            "failure"
        };
        metrics::histogram!("sync.block.verify.duration_seconds", "result" => verify_result)
            .record(verify_start.elapsed().as_secs_f64());

        if verification.is_ok() {
            metrics::counter!("sync.verified.block.count").increment(1);
        }

        verification
            .map(|hash| (block_height, hash))
            .map_err(
                |err| match err.downcast::<zebra_consensus::router::RouterError>() {
                    Ok(error) => BlockDownloadVerifyError::Invalid {
                        error: *error,
                        height: block_height,
                        hash,
                        advertiser_addr,
                    },
                    Err(error) => BlockDownloadVerifyError::ValidationRequestError {
                        error,
                        height: block_height,
                        hash,
                    },
                },
            )
    }

    /// Queue a block for download and verification.
    ///
    /// This method waits for the network to become ready, and returns an error
    /// only if the network service fails. It returns immediately after queuing
    /// the request.
    #[instrument(level = "debug", skip(self), fields(%hash))]
    pub async fn download_and_verify(
        &mut self,
        hash: block::Hash,
    ) -> Result<(), BlockDownloadVerifyError> {
        self.download_and_verify_from_peers(hash, HashSet::new())
            .await
    }

    /// Queue a block for download and verification, preferring known source
    /// peers before falling back to normal inventory routing.
    #[instrument(level = "debug", skip(self, preferred_peers), fields(%hash, preferred_peers = preferred_peers.len()))]
    pub async fn download_and_verify_from_peers(
        &mut self,
        hash: block::Hash,
        preferred_peers: HashSet<PeerSocketAddr>,
    ) -> Result<(), BlockDownloadVerifyError> {
        if self.cancel_handles.contains_key(&hash) {
            metrics::counter!("sync.already.queued.dropped.block.hash.count").increment(1);
            return Err(BlockDownloadVerifyError::DuplicateBlockQueuedForDownload { hash });
        }

        let preferred_peer_count = preferred_peers.len();
        let hashes = std::iter::once(hash).collect();
        let request = if preferred_peers.is_empty() {
            zn::Request::BlocksByHash(hashes)
        } else {
            zn::Request::BlocksByHashFromPeers {
                hashes,
                preferred_peers,
            }
        };

        // We construct the block requests sequentially, waiting for the peer
        // set to be ready to process each request. This ensures that we start
        // block downloads in the order we want them (though they may resolve
        // out of order), and it means that we respect backpressure. Otherwise,
        // if we waited for readiness and did the service call in the spawned
        // tasks, all of the spawned tasks would race each other waiting for the
        // network to become ready.
        let block_req = self
            .network
            .ready()
            .await
            .map_err(|error| BlockDownloadVerifyError::NetworkServiceError { error })?
            .call(request);

        // This watch channel is used to signal cancellation to the download task.
        let (cancel_tx, mut cancel_rx) = watch::channel(false);

        let verifier = self.verifier.clone();
        let latest_chain_tip = self.latest_chain_tip.clone();

        let lookahead_limit = self.lookahead_limit;
        let max_checkpoint_height = self.max_checkpoint_height;

        let past_lookahead_limit_sender = self.past_lookahead_limit_sender.clone();
        let past_lookahead_limit_receiver = self.past_lookahead_limit_receiver.clone();

        let task = tokio::spawn(
            async move {
                // Download the block.
                // Prefer the cancel handle if both are ready.
                let download_start = std::time::Instant::now();
                let rsp = tokio::select! {
                    biased;
                    _ = cancel_rx.changed() => {
                        trace!("task cancelled prior to download completion");
                        metrics::counter!("sync.cancelled.download.count").increment(1);
                        metrics::histogram!("sync.block.download.duration_seconds", "result" => "cancelled")
                            .record(download_start.elapsed().as_secs_f64());
                        return Err(BlockDownloadVerifyError::CancelledDuringDownload { hash })
                    }
                    rsp = block_req => match rsp {
                        Ok(rsp) => rsp,
                        Err(error) => {
                            let reason = classify_download_error(&error);
                            info!(
                                %hash,
                                preferred_peer_count,
                                reason,
                                error = ?error,
                                "single block download request failed"
                            );
                            metrics::counter!(
                                "sync.block.download.single.request.error.count",
                                "reason" => reason
                            )
                            .increment(1);
                            metrics::histogram!(
                                "sync.block.download.single.request.error.preferred_peer_count",
                                "reason" => reason
                            )
                            .record(preferred_peer_count as f64);

                            return Err(BlockDownloadVerifyError::DownloadFailed { error, hash });
                        }
                    },
                };

                let (block, advertiser_addr) = if let zn::Response::Blocks(blocks) = rsp {
                    assert_eq!(
                        blocks.len(),
                        1,
                        "wrong number of blocks in response to a single hash"
                    );

                    blocks
                        .first()
                        .expect("just checked length")
                        .available()
                        .expect("unexpected missing block status: single block failures should be errors")
                } else {
                    unreachable!("wrong response to block request");
                };
                metrics::counter!("sync.downloaded.block.count").increment(1);
                metrics::histogram!("sync.block.download.duration_seconds", "result" => "success")
                    .record(download_start.elapsed().as_secs_f64());

                let verify_context = BlockVerifyContext {
                    verifier,
                    latest_chain_tip,
                    lookahead_limit,
                    max_checkpoint_height,
                    past_lookahead_limit_sender,
                    past_lookahead_limit_receiver,
                };

                Self::verify_downloaded_block(verify_context, cancel_rx, hash, block, advertiser_addr)
                    .await
            }
            .in_current_span()
            // Tack the hash onto the error so we can remove the cancel handle
            // on failure as well as on success.
            .map_err(move |e| (e, hash)),
        );

        // Try to start the spawned task before queueing the next block request
        tokio::task::yield_now().await;

        self.pending.push(task);
        assert!(
            self.cancel_handles.insert(hash, cancel_tx).is_none(),
            "blocks are only queued once"
        );

        Ok(())
    }

    /// Queue a batch of blocks for download from the same preferred peer set.
    #[instrument(level = "debug", skip(self, hashes, preferred_peers), fields(batch_len = hashes.len(), preferred_peers = preferred_peers.len()))]
    pub async fn download_and_verify_batch_from_peers(
        &mut self,
        hashes: Vec<block::Hash>,
        preferred_peers: HashSet<PeerSocketAddr>,
    ) -> Result<(), BlockDownloadVerifyError> {
        assert!(!hashes.is_empty(), "batch downloads need at least one hash");

        if hashes.len() == 1 {
            return self
                .download_and_verify_from_peers(hashes[0], preferred_peers)
                .await;
        }

        for hash in &hashes {
            if self.cancel_handles.contains_key(hash) {
                metrics::counter!("sync.already.queued.dropped.block.hash.count").increment(1);
                return Err(BlockDownloadVerifyError::DuplicateBlockQueuedForDownload {
                    hash: *hash,
                });
            }
        }

        let batch_len = hashes.len();
        let preferred_peer_count = preferred_peers.len();

        metrics::histogram!("sync.block.download.batch.size").record(hashes.len() as f64);

        let request_hashes: HashSet<_> = hashes.iter().copied().collect();
        let request = if preferred_peers.is_empty() {
            zn::Request::BlocksByHash(request_hashes.clone())
        } else {
            zn::Request::BlocksByHashFromPeers {
                hashes: request_hashes.clone(),
                preferred_peers,
            }
        };

        let block_req = self
            .network
            .ready()
            .await
            .map_err(|error| BlockDownloadVerifyError::NetworkServiceError { error })?
            .call(request);

        let (cancel_tx, mut cancel_rx) = watch::channel(false);

        let verifier = self.verifier.clone();
        let latest_chain_tip = self.latest_chain_tip.clone();

        let lookahead_limit = self.lookahead_limit;
        let max_checkpoint_height = self.max_checkpoint_height;

        let past_lookahead_limit_sender = self.past_lookahead_limit_sender.clone();
        let past_lookahead_limit_receiver = self.past_lookahead_limit_receiver.clone();

        let task = tokio::spawn(
            async move {
                let mut results = Vec::with_capacity(hashes.len());
                let mut verification_tasks = FuturesUnordered::new();

                let download_start = std::time::Instant::now();
                let rsp = tokio::select! {
                    biased;
                    _ = cancel_rx.changed() => {
                        trace!("batch task cancelled prior to download completion");
                        metrics::counter!("sync.cancelled.download.count").increment(hashes.len() as u64);
                        metrics::histogram!("sync.block.download.duration_seconds", "result" => "cancelled")
                            .record(download_start.elapsed().as_secs_f64());

                        return hashes
                            .into_iter()
                            .map(|hash| {
                                Err((BlockDownloadVerifyError::CancelledDuringDownload { hash }, hash))
                            })
                            .collect();
                    }
                    rsp = block_req => rsp,
                };

                let blocks = match rsp {
                    Ok(zn::Response::Blocks(blocks)) => blocks,
                    Ok(_) => unreachable!("wrong response to block request"),
                    Err(error) => {
                        let reason = classify_download_error(&error);
                        info!(
                            batch_len,
                            preferred_peer_count,
                            reason,
                            error = ?error,
                            "source-routed batch download request failed"
                        );
                        metrics::counter!(
                            "sync.block.download.batch.request.error.count",
                            "reason" => reason
                        )
                        .increment(1);
                        metrics::histogram!(
                            "sync.block.download.batch.request.error.preferred_peer_count",
                            "reason" => reason
                        )
                        .record(preferred_peer_count as f64);
                        metrics::histogram!(
                            "sync.block.download.batch.request.error.batch_size",
                            "reason" => reason
                        )
                        .record(batch_len as f64);

                        return hashes
                            .into_iter()
                            .map(|hash| {
                                Err((
                                    BlockDownloadVerifyError::DownloadFailed {
                                        error: clone_download_error(&error),
                                        hash,
                                    },
                                    hash,
                                ))
                        })
                        .collect();
                    }
                };

                metrics::histogram!("sync.block.download.batch.response.size")
                    .record(blocks.len() as f64);

                let mut returned_hashes = HashSet::new();

                for block_status in blocks {
                    match block_status {
                        zn::InventoryResponse::Available((block, advertiser_addr)) => {
                            let hash = block.hash();

                            returned_hashes.insert(hash);
                            metrics::counter!("sync.downloaded.block.count").increment(1);
                            metrics::histogram!(
                                "sync.block.download.duration_seconds",
                                "result" => "success"
                            )
                            .record(download_start.elapsed().as_secs_f64());

                            let verify_context = BlockVerifyContext {
                                verifier: verifier.clone(),
                                latest_chain_tip: latest_chain_tip.clone(),
                                lookahead_limit,
                                max_checkpoint_height,
                                past_lookahead_limit_sender: past_lookahead_limit_sender.clone(),
                                past_lookahead_limit_receiver: past_lookahead_limit_receiver
                                    .clone(),
                            };
                            let cancel_rx = cancel_rx.clone();

                            verification_tasks.push(tokio::spawn(
                                async move {
                                    Self::verify_downloaded_block(
                                        verify_context,
                                        cancel_rx,
                                        hash,
                                        block,
                                        advertiser_addr,
                                    )
                                    .await
                                    .map_err(|error| (error, hash))
                                }
                                .in_current_span(),
                            ));
                        }
                        zn::InventoryResponse::Missing(hash) => {
                            returned_hashes.insert(hash);
                            info!(
                                %hash,
                                batch_len,
                                preferred_peer_count,
                                "source-routed batch download reported missing block"
                            );
                            metrics::histogram!(
                                "sync.block.download.batch.missing.preferred_peer_count",
                                "reason" => "not_found_response"
                            )
                            .record(preferred_peer_count as f64);
                            metrics::histogram!(
                                "sync.block.download.batch.missing.batch_size",
                                "reason" => "not_found_response"
                            )
                            .record(batch_len as f64);
                            let error = zn::SharedPeerError::from(zn::PeerError::NotFoundResponse(
                                vec![hash.into()],
                            ));
                            results.push(Err((
                                BlockDownloadVerifyError::DownloadFailed {
                                    error: error.into(),
                                    hash,
                                },
                                hash,
                            )));
                        }
                    }
                }

                let mut synthetic_missing = 0;
                for hash in hashes {
                    if !returned_hashes.contains(&hash) {
                        synthetic_missing += 1;
                        info!(
                            %hash,
                            batch_len,
                            preferred_peer_count,
                            "source-routed batch download omitted requested block"
                        );
                        metrics::histogram!(
                            "sync.block.download.batch.missing.preferred_peer_count",
                            "reason" => "omitted"
                        )
                        .record(preferred_peer_count as f64);
                        metrics::histogram!(
                            "sync.block.download.batch.missing.batch_size",
                            "reason" => "omitted"
                        )
                        .record(batch_len as f64);
                        let error = zn::SharedPeerError::from(zn::PeerError::NotFoundResponse(
                            vec![hash.into()],
                        ));
                        results.push(Err((
                            BlockDownloadVerifyError::DownloadFailed {
                                error: error.into(),
                                hash,
                            },
                            hash,
                        )));
                    }
                }

                metrics::counter!("sync.block.download.batch.synthetic.missing.count")
                    .increment(synthetic_missing);

                while let Some(verification) = verification_tasks.next().await {
                    results.push(verification.expect("batch block verify tasks must not panic"));
                }

                results
            }
            .in_current_span(),
        );

        tokio::task::yield_now().await;

        self.batch_pending.push(task);
        for hash in request_hashes {
            assert!(
                self.cancel_handles
                    .insert(hash, cancel_tx.clone())
                    .is_none(),
                "blocks are only queued once"
            );
        }

        Ok(())
    }

    /// Cancel all running tasks and reset the downloader state.
    pub fn cancel_all(&mut self) {
        // Replace the pending task list with an empty one and drop it.
        let _ = std::mem::take(&mut self.pending);
        let _ = std::mem::take(&mut self.batch_pending);
        self.ready_results.clear();

        // Signal cancellation to all running tasks.
        // Since we already dropped the JoinHandles above, they should
        // fail silently.
        for (_hash, cancel) in self.cancel_handles.drain() {
            let _ = cancel.send(true);
        }

        assert!(self.pending.is_empty());
        assert!(self.cancel_handles.is_empty());

        // Set the lookahead limit to false, since we're empty (so we're under the limit).
        //
        // It is ok to block here, because we're doing a reset and sleep anyway.
        // But if Zebra is shutting down, ignore the send error.
        let _ = self
            .past_lookahead_limit_sender
            .lock()
            .expect("thread panicked while holding the past_lookahead_limit_sender mutex guard")
            .send(false);
    }

    /// Get the number of currently in-flight download and verify tasks.
    pub fn in_flight(&mut self) -> usize {
        self.cancel_handles.len() + self.ready_results.len()
    }

    /// Returns true if there are no in-flight download and verify tasks.
    #[allow(dead_code)]
    pub fn is_empty(&mut self) -> bool {
        self.cancel_handles.is_empty() && self.ready_results.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use futures::StreamExt;
    use tokio::sync::watch;
    use zebra_chain::{
        block::{self, Block, Height},
        chain_tip::mock::MockChainTip,
        serialization::ZcashDeserializeInto,
    };
    use zebra_consensus::Request as VerifyRequest;
    use zebra_network::{
        InventoryResponse::Available, PeerError, Request as NetworkRequest, Response,
        SharedPeerError,
    };
    use zebra_test::mock_service::{MockService, PanicAssertion};

    use super::*;

    #[tokio::test]
    async fn batch_downloads_queue_all_returned_blocks_for_verification() -> Result<(), BoxError> {
        let block1: Arc<Block> =
            zebra_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?;
        let block1_hash = block1.hash();
        let block2: Arc<Block> =
            zebra_test::vectors::BLOCK_MAINNET_2_BYTES.zcash_deserialize_into()?;
        let block2_hash = block2.hash();

        let mut peer_set: MockService<NetworkRequest, Response, PanicAssertion> =
            MockService::build().for_unit_tests();
        let mut verifier: MockService<VerifyRequest, block::Hash, PanicAssertion> =
            MockService::build().for_unit_tests();
        let (latest_chain_tip, _latest_chain_tip_sender) = MockChainTip::new();
        let (past_lookahead_limit_sender, _past_lookahead_limit_receiver) = watch::channel(false);

        let mut downloads = Downloads::new(
            peer_set.clone(),
            verifier.clone(),
            latest_chain_tip,
            past_lookahead_limit_sender,
            100,
            Height(0),
        );

        downloads
            .download_and_verify_batch_from_peers(vec![block1_hash, block2_hash], HashSet::new())
            .await?;

        peer_set
            .expect_request(NetworkRequest::BlocksByHash(
                [block1_hash, block2_hash].into_iter().collect(),
            ))
            .await
            .respond(Response::Blocks(vec![
                Available((block1, None)),
                Available((block2, None)),
            ]));

        let mut downloads = Box::pin(downloads);
        let next_result = tokio::spawn(async move { downloads.next().await });

        let first_verify = verifier
            .expect_request_that(|request| {
                let hash = request.block().hash();
                hash == block1_hash || hash == block2_hash
            })
            .await;
        let first_hash = first_verify.request().block().hash();

        let second_verify = verifier
            .expect_request_that(|request| {
                let hash = request.block().hash();
                hash != first_hash && (hash == block1_hash || hash == block2_hash)
            })
            .await;

        first_verify.respond_with(|request| request.block().hash());
        second_verify.respond_with(|request| request.block().hash());

        let result = next_result
            .await
            .expect("download stream task should not panic")
            .expect("download stream should produce a verification result")?;

        assert!(
            result.1 == block1_hash || result.1 == block2_hash,
            "unexpected verified block hash: {:?}",
            result.1,
        );

        Ok(())
    }

    #[tokio::test]
    async fn batch_download_error_preserves_shared_peer_error() -> Result<(), BoxError> {
        let block1: Arc<Block> =
            zebra_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?;
        let block1_hash = block1.hash();
        let block2: Arc<Block> =
            zebra_test::vectors::BLOCK_MAINNET_2_BYTES.zcash_deserialize_into()?;
        let block2_hash = block2.hash();

        let mut peer_set: MockService<NetworkRequest, Response, PanicAssertion> =
            MockService::build().for_unit_tests();
        let verifier: MockService<VerifyRequest, block::Hash, PanicAssertion> =
            MockService::build().for_unit_tests();
        let (latest_chain_tip, _latest_chain_tip_sender) = MockChainTip::new();
        let (past_lookahead_limit_sender, _past_lookahead_limit_receiver) = watch::channel(false);

        let mut downloads = Downloads::new(
            peer_set.clone(),
            verifier,
            latest_chain_tip,
            past_lookahead_limit_sender,
            100,
            Height(0),
        );

        downloads
            .download_and_verify_batch_from_peers(vec![block1_hash, block2_hash], HashSet::new())
            .await?;

        peer_set
            .expect_request(NetworkRequest::BlocksByHash(
                [block1_hash, block2_hash].into_iter().collect(),
            ))
            .await
            .respond_error(
                Box::new(SharedPeerError::from(PeerError::NotFoundResponse(vec![
                    block1_hash.into(),
                    block2_hash.into(),
                ]))) as BoxError,
            );

        let mut downloads = Box::pin(downloads);
        let error = downloads
            .next()
            .await
            .expect("download stream should produce a batch error")
            .expect_err("batch request should fail");

        assert!(
            error.not_found_download_hash().is_some(),
            "batched notfound errors should still be recognized for sync retry: {error:?}"
        );

        let BlockDownloadVerifyError::DownloadFailed { error, .. } = error else {
            panic!("unexpected batch download error: {error:?}");
        };

        assert!(
            error.downcast_ref::<SharedPeerError>().is_some(),
            "batched peer errors should not be converted to strings"
        );

        Ok(())
    }
}
