use super::{error::*, wire::*, *};

/// Default number of blocks advertised per response.
///
/// Keep block-body ranges narrow so a missing response only holds one height at
/// the body-download floor.
pub const DEFAULT_BS_BLOCKS_PER_RESPONSE: u32 = 1;
/// Default advertised hard cap on concurrent in-flight block requests per peer.
///
/// This is the ceiling the adaptive per-peer window grows *toward*, **not** the
/// opening window. Scheduling starts at [`DEFAULT_BS_INITIAL_INFLIGHT`] and ramps
/// up to the peer-advertised value (this default, clamped to
/// [`MAX_BS_INFLIGHT_REQUESTS`]) only after sustained error-free responses (see
/// the streak-gated cubic ramp on `DownloadWindow`). In a homogeneous fleet this
/// is the per-peer concurrency ceiling every peer offers.
pub const DEFAULT_BS_MAX_INFLIGHT: u32 = 32000;
/// Initial per-peer outbound request window (slow-start point).
///
/// The adaptive window starts here and grows toward the peer-advertised hard cap
/// on successful responses, rather than opening at the full `max_inflight`. This
/// keeps the opening burst modest so a peer is not flooded before its latency is
/// known.
pub const DEFAULT_BS_INITIAL_INFLIGHT: u32 = 64;
/// Maximum peer-advertised in-flight request count accepted by this node.
///
/// This is the hard ceiling the default advertisement ([`DEFAULT_BS_MAX_INFLIGHT`]
/// = 32,000) is clamped to, and also the per-peer outstanding-request safety bound
/// (`EFFECTIVE_BS_OUTBOUND_INFLIGHT_PER_PEER`). It bounds how many concurrent
/// requests a remote peer can make us hold against it, so it doubles as a DoS bound.
pub const MAX_BS_INFLIGHT_REQUESTS: u32 = 32_768;
/// Default total response byte target advertised per range response.
pub const DEFAULT_BS_MAX_RESPONSE_BYTES: u32 = 32 * 1024 * 1024;
/// Default global byte budget reserved for later block-download scheduling.
pub const DEFAULT_BS_MAX_INFLIGHT_BLOCK_BYTES: u64 = 6 * 1024 * 1024 * 1024;
/// Worst-case serialized bytes reserved per requested block body.
///
/// Block-sync reserves this much per requested block at send time and only ever
/// shrinks the reservation toward the actual serialized size on receipt, so a
/// valid, already-downloaded body is never discarded for a full budget. Each
/// body arrives in its own `Block` frame bounded by [`block::MAX_BLOCK_BYTES`]
/// at decode (`MAX_BS_MESSAGE_BYTES > MAX_BLOCK_BYTES`), so the actual size can
/// never exceed this worst case and the shrink is always non-negative.
pub const BS_PER_BLOCK_WORST_CASE_BYTES: u64 = block::MAX_BLOCK_BYTES;
/// Default byte cap for speculative reorder look-ahead above the download floor.
///
/// The default leaves one advertised response worth of headroom below the global
/// byte budget. The synchronous floor-pop path is the funding guarantee when
/// that headroom has been consumed by races or changed configuration.
pub const DEFAULT_BS_MAX_REORDER_LOOKAHEAD_BYTES: u64 =
    // `DEFAULT_BS_MAX_RESPONSE_BYTES` is a `u32`, so widening to `u64` is lossless.
    DEFAULT_BS_MAX_INFLIGHT_BLOCK_BYTES - DEFAULT_BS_MAX_RESPONSE_BYTES as u64;
/// Default block-count cap for speculative reorder look-ahead bookkeeping.
pub const DEFAULT_BS_MAX_REORDER_LOOKAHEAD_BLOCKS: u32 = 4096;
/// Minimum submitted block applies required to resolve one checkpoint range.
///
/// The checkpoint verifier resolves a checkpoint window only after the whole
/// window, including the resolving checkpoint block, is queued. A node that
/// starts one height before a checkpoint-gap boundary can therefore need one
/// maximum checkpoint gap plus the boundary block in flight.
pub const MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES: usize =
    zebra_chain::parameters::checkpoint::constants::MAX_CHECKPOINT_HEIGHT_GAP + 1;
/// Default maximum submitted block applies awaiting verifier completion.
pub const DEFAULT_BS_MAX_SUBMITTED_BLOCK_APPLIES: usize =
    MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES;
/// The byte budget required to hold one full worst-case checkpoint range in
/// flight.
///
/// The checkpoint verifier resolves a block's commit only once the entire
/// contiguous range to the next checkpoint has been submitted, and every
/// submitted body stays reserved against `max_inflight_block_bytes` until it is
/// durable. A budget that cannot hold a whole worst-case range can never
/// complete one: the verifier never commits, nothing becomes durable, and no
/// bytes are ever released.
pub const BS_CHECKPOINT_RANGE_BYTE_FLOOR: u64 =
    // `MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES` is `MAX_CHECKPOINT_HEIGHT_GAP + 1`
    // (= 401), which fits `u64` losslessly; the product (~802 MB) cannot overflow.
    MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES as u64 * BS_PER_BLOCK_WORST_CASE_BYTES;
/// Default block-sync request timeout.
pub const DEFAULT_BS_REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
/// Default central floor-watchdog cadence.
pub const DEFAULT_BS_FLOOR_WATCHDOG_TICK: Duration = Duration::from_secs(1);
/// Default hard floor-peer avoid cooldown after a watchdog cancellation.
pub const DEFAULT_BS_FLOOR_PEER_AVOID_COOLDOWN: Duration = DEFAULT_BS_REQUEST_TIMEOUT;
/// Default block-sync status refresh interval reserved for later advertisement.
pub const DEFAULT_BS_STATUS_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
/// Default tolerated size-hint deviation percentage reserved for later soft scoring.
pub const DEFAULT_BS_SIZE_DEVIATION_TOLERANCE: u32 = 200;
/// Default block-sync peer fanout for the same requested range.
pub const DEFAULT_BS_FANOUT: usize = 1;
/// Maximum peer-advertised aggregate byte target accepted per requested range.
///
/// A range response is sent as one `Block` frame per body, and each body frame
/// remains independently bounded by `MAX_BS_MESSAGE_BYTES`. This aggregate cap
/// only controls how many bounded body frames a server sends before `BlocksDone`.
pub const MAX_BS_RESPONSE_BYTES: u32 = DEFAULT_BS_MAX_RESPONSE_BYTES;

/// Block-sync peer status advertisement.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BlockSyncStatus {
    /// Earliest block body this peer can serve.
    pub servable_low: block::Height,
    /// Highest contiguous verified block body this peer can serve.
    pub servable_high: block::Height,
    /// Hash of `servable_high`.
    pub tip_hash: block::Hash,
    /// Maximum blocks the sender will serve per requested range.
    pub max_blocks_per_response: u32,
    /// Maximum concurrent `GetBlocks` requests the sender will service.
    pub max_inflight_requests: u32,
    /// Maximum total response bytes the sender targets per requested range.
    pub max_response_bytes: u32,
}

impl BlockSyncStatus {
    pub(super) fn encode_to<W: Write>(&self, writer: &mut W) -> Result<(), BlockSyncWireError> {
        write_height(writer, self.servable_low)?;
        write_height(writer, self.servable_high)?;
        self.tip_hash.zcash_serialize(&mut *writer)?;
        writer.write_u32::<LittleEndian>(clamp_advertised_blocks(self.max_blocks_per_response))?;
        writer.write_u32::<LittleEndian>(self.max_inflight_requests)?;
        writer.write_u32::<LittleEndian>(self.max_response_bytes.max(1))?;
        Ok(())
    }

    pub(super) fn decode_from<R: Read>(reader: &mut R) -> Result<Self, BlockSyncWireError> {
        Ok(Self {
            servable_low: read_height(reader)?,
            servable_high: read_height(reader)?,
            tip_hash: block::Hash::zcash_deserialize(&mut *reader)?,
            max_blocks_per_response: clamp_advertised_blocks(reader.read_u32::<LittleEndian>()?),
            max_inflight_requests: clamp_advertised_inflight(reader.read_u32::<LittleEndian>()?),
            max_response_bytes: clamp_advertised_response_bytes(reader.read_u32::<LittleEndian>()?),
        })
    }
}

impl Default for BlockSyncStatus {
    fn default() -> Self {
        Self {
            servable_low: block::Height::MIN,
            servable_high: block::Height::MIN,
            tip_hash: block::Hash([0; 32]),
            max_blocks_per_response: DEFAULT_BS_BLOCKS_PER_RESPONSE,
            max_inflight_requests: DEFAULT_BS_MAX_INFLIGHT,
            max_response_bytes: DEFAULT_BS_MAX_RESPONSE_BYTES,
        }
    }
}

/// Block-sync configuration nested under the Zakura P2P-v2 config.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ZakuraBlockSyncConfig {
    /// Deprecated compatibility key for older rollout configs.
    ///
    /// Zakura block sync is now selected by the top-level `v2_p2p` flag. This
    /// field is accepted but ignored so older configs keep parsing.
    #[doc(hidden)]
    #[serde(
        default,
        skip_serializing,
        deserialize_with = "deserialize_ignored_replace_legacy_syncer"
    )]
    pub replace_legacy_syncer: bool,
    /// Maximum blocks this node advertises per `GetBlocks` response.
    pub max_blocks_per_response: u32,
    /// Maximum concurrent `GetBlocks` requests this node advertises per peer.
    pub max_inflight_requests: u32,
    /// Initial per-peer outbound request window (slow-start point); grows toward
    /// the advertised hard cap on success. Clamped to `[1, max_inflight_requests]`.
    pub initial_inflight_requests: u32,
    /// Maximum total response bytes this node advertises per `GetBlocks` response.
    pub max_response_bytes: u32,
    /// Maximum estimated bytes reserved for in-flight and buffered block bodies.
    pub max_inflight_block_bytes: u64,
    /// Maximum speculative body bytes held above the download floor.
    pub max_reorder_lookahead_bytes: u64,
    /// Maximum speculative body heights tracked above the download floor.
    pub max_reorder_lookahead_blocks: u32,
    /// Cadence for the central floor watchdog that rescues expired floor claims.
    #[serde(with = "humantime_serde")]
    pub floor_watchdog_tick: Duration,
    /// How long to avoid reassigning an expired floor height to the same peer.
    #[serde(with = "humantime_serde")]
    pub floor_peer_avoid_cooldown: Duration,
    /// Maximum block bodies submitted to the verifier before completed applies
    /// release more submission slots.
    pub max_submitted_block_applies: usize,
    /// Timeout for an outstanding block-body range request.
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
    /// How often this node sends unsolicited status refreshes after local frontier changes.
    #[serde(with = "humantime_serde")]
    pub status_refresh_interval: Duration,
    /// Percentage deviation from advertised body-size hints tolerated before soft scoring.
    pub size_deviation_tolerance: u32,
    /// Number of peers later range scheduling may fan out to for the same body gap.
    pub fanout: usize,
    /// Block-sync peer caps and queue limits owned by this service.
    pub peer_limits: ServicePeerLimits,
}

fn deserialize_ignored_replace_legacy_syncer<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let _ = bool::deserialize(deserializer)?;
    Ok(false)
}

impl Default for ZakuraBlockSyncConfig {
    fn default() -> Self {
        Self {
            replace_legacy_syncer: false,
            max_blocks_per_response: DEFAULT_BS_BLOCKS_PER_RESPONSE,
            max_inflight_requests: DEFAULT_BS_MAX_INFLIGHT,
            initial_inflight_requests: DEFAULT_BS_INITIAL_INFLIGHT,
            max_response_bytes: DEFAULT_BS_MAX_RESPONSE_BYTES,
            max_inflight_block_bytes: DEFAULT_BS_MAX_INFLIGHT_BLOCK_BYTES,
            max_reorder_lookahead_bytes: DEFAULT_BS_MAX_REORDER_LOOKAHEAD_BYTES,
            max_reorder_lookahead_blocks: DEFAULT_BS_MAX_REORDER_LOOKAHEAD_BLOCKS,
            floor_watchdog_tick: DEFAULT_BS_FLOOR_WATCHDOG_TICK,
            floor_peer_avoid_cooldown: DEFAULT_BS_FLOOR_PEER_AVOID_COOLDOWN,
            max_submitted_block_applies: DEFAULT_BS_MAX_SUBMITTED_BLOCK_APPLIES,
            request_timeout: DEFAULT_BS_REQUEST_TIMEOUT,
            status_refresh_interval: DEFAULT_BS_STATUS_REFRESH_INTERVAL,
            size_deviation_tolerance: DEFAULT_BS_SIZE_DEVIATION_TOLERANCE,
            fanout: DEFAULT_BS_FANOUT,
            peer_limits: ServicePeerLimits::default(),
        }
    }
}

impl ZakuraBlockSyncConfig {
    /// Return the clamped block-count advertisement for wire status messages.
    pub fn advertised_max_blocks_per_response(&self) -> u32 {
        clamp_advertised_blocks(self.max_blocks_per_response)
    }

    /// Return the locally capped in-flight advertisement for status messages.
    pub fn advertised_max_inflight_requests(&self) -> u32 {
        clamp_advertised_inflight(self.max_inflight_requests)
    }

    /// Return the non-zero response byte advertisement for status messages.
    pub fn advertised_max_response_bytes(&self) -> u32 {
        clamp_advertised_response_bytes(self.max_response_bytes)
    }

    /// Return the non-zero verifier submission cap.
    pub fn submitted_apply_limit(&self) -> usize {
        self.max_submitted_block_applies
            .max(DEFAULT_BS_MAX_SUBMITTED_BLOCK_APPLIES)
    }

    /// Return the speculative look-ahead byte cap clamped to the global budget.
    pub fn effective_max_reorder_lookahead_bytes(&self) -> u64 {
        self.max_reorder_lookahead_bytes
            .min(self.max_inflight_block_bytes)
    }

    /// Return the watchdog tick clamped to a positive duration no larger than the request timeout.
    pub fn effective_floor_watchdog_tick(&self) -> Duration {
        self.floor_watchdog_tick
            .min(self.request_timeout)
            .max(Duration::from_millis(1))
    }

    /// Return the floor avoid cooldown clamped to a positive duration.
    pub fn effective_floor_peer_avoid_cooldown(&self) -> Duration {
        self.floor_peer_avoid_cooldown.max(Duration::from_millis(1))
    }

    /// Return the largest byte reservation a single floor request can need.
    pub fn floor_request_byte_reservation(&self) -> u64 {
        let fanout = u64::try_from(self.fanout.max(1)).unwrap_or(u64::MAX);
        let worst_case_blocks = u64::from(self.advertised_max_blocks_per_response())
            .saturating_mul(BS_PER_BLOCK_WORST_CASE_BYTES)
            .saturating_mul(fanout);
        u64::from(self.advertised_max_response_bytes()).max(worst_case_blocks)
    }

    /// Validate production-safety bounds after deserialization.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_inflight_block_bytes == 0 {
            return Err("max_inflight_block_bytes must be greater than zero");
        }
        if self.max_reorder_lookahead_bytes == 0 {
            return Err("max_reorder_lookahead_bytes must be greater than zero");
        }
        if self.max_reorder_lookahead_blocks == 0 {
            return Err("max_reorder_lookahead_blocks must be greater than zero");
        }
        if self.max_inflight_block_bytes <= self.floor_request_byte_reservation() {
            return Err("max_inflight_block_bytes must exceed one floor request");
        }
        if self.max_inflight_block_bytes < BS_CHECKPOINT_RANGE_BYTE_FLOOR {
            return Err(
                "max_inflight_block_bytes must hold one full checkpoint range \
<<<<<<< HEAD
<<<<<<< HEAD
                 (MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES * BS_PER_BLOCK_WORST_CASE_BYTES) \
=======
                 (DEFAULT_BS_MAX_SUBMITTED_BLOCK_APPLIES * BS_PER_BLOCK_WORST_CASE_BYTES) \
>>>>>>> 1f45a4d70 (fix(network): enforce checkpoint-safe block apply budget)
=======
                 (DEFAULT_BS_MAX_SUBMITTED_BLOCK_APPLIES * BS_PER_BLOCK_WORST_CASE_BYTES) \
>>>>>>> 2bd2f9267 (Revert "fix(network): align block sync submit window with checkpoints")
                 or checkpoint sync can deadlock",
            );
        }
        Ok(())
    }

    /// Build the inert local status used before the block-sync reactor is wired.
    pub fn initial_status(&self) -> BlockSyncStatus {
        BlockSyncStatus {
            max_blocks_per_response: self.advertised_max_blocks_per_response(),
            max_inflight_requests: self.advertised_max_inflight_requests(),
            max_response_bytes: self.advertised_max_response_bytes(),
            ..BlockSyncStatus::default()
        }
    }
}

/// Clamp an advertised block count to the hard stream-6 request cap.
pub fn clamp_advertised_blocks(count: u32) -> u32 {
    count.clamp(1, MAX_BS_BLOCKS_PER_REQUEST)
}

/// Clamp an advertised in-flight request count to the local status ceiling.
pub fn clamp_advertised_inflight(count: u32) -> u32 {
    count.clamp(1, MAX_BS_INFLIGHT_REQUESTS)
}

/// Clamp an advertised response byte target to the largest stream-6 message.
pub fn clamp_advertised_response_bytes(bytes: u32) -> u32 {
    bytes.clamp(1, MAX_BS_RESPONSE_BYTES)
}

/// Maximum inbound `GetBlocks.count` this node will serve before looking at body sizes.
pub fn inbound_get_blocks_count_limit(config: &ZakuraBlockSyncConfig) -> u32 {
    config
        .advertised_max_blocks_per_response()
        .clamp(1, MAX_BS_BLOCKS_PER_REQUEST)
}
