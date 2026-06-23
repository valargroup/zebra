use super::{error::*, wire::*, *};

/// Default number of blocks advertised per response.
///
/// Keep block-body ranges narrow so a missing response only holds one height at
/// the body-download floor.
pub const DEFAULT_BS_BLOCKS_PER_RESPONSE: u32 = 1;
/// Initial number of in-flight block requests advertised per peer.
///
/// Outbound scheduling starts at this window and adjusts per peer based on
/// request timeouts, while peer advertisements can still allow growth up to
/// [`MAX_BS_INFLIGHT_REQUESTS`].
pub const DEFAULT_BS_MAX_INFLIGHT: u16 = 2048;
/// Maximum peer-advertised in-flight request count accepted by this node.
pub const MAX_BS_INFLIGHT_REQUESTS: u16 = 10_000;
/// Default total response byte target advertised per range response.
pub const DEFAULT_BS_MAX_RESPONSE_BYTES: u32 = 32 * 1024 * 1024;
/// Default global byte budget reserved for later block-download scheduling.
pub const DEFAULT_BS_MAX_INFLIGHT_BLOCK_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// Worst-case serialized bytes reserved per requested block body.
///
/// Block-sync reserves this much per requested block at send time and only ever
/// shrinks the reservation toward the actual serialized size on receipt, so a
/// valid, already-downloaded body is never discarded for a full budget. Each
/// body arrives in its own `Block` frame bounded by [`block::MAX_BLOCK_BYTES`]
/// at decode (`MAX_BS_MESSAGE_BYTES > MAX_BLOCK_BYTES`), so the actual size can
/// never exceed this worst case and the shrink is always non-negative.
pub const BS_PER_BLOCK_WORST_CASE_BYTES: u64 = block::MAX_BLOCK_BYTES;
/// Default maximum submitted block applies awaiting verifier completion.
///
/// The checkpoint verifier resolves a checkpoint window only after the whole
/// window is queued, so this defaults to one maximum checkpoint gap.
pub const DEFAULT_BS_MAX_SUBMITTED_BLOCK_APPLIES: usize =
    zebra_chain::parameters::checkpoint::constants::MAX_CHECKPOINT_HEIGHT_GAP;
/// Default block-sync request timeout.
pub const DEFAULT_BS_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
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

/// Fraction of *available* RAM the `auto` in-flight ceiling targets.
///
/// Chosen at one quarter so the auto ceiling leaves ample headroom for the rest
/// of the process (state cache, network buffers, the OS page cache) on top of
/// the [`SAFE_FRACTION`] guard applied to *total* RAM.
pub const AUTO_FRACTION_NUM: u64 = 1;
/// Denominator of [`AUTO_FRACTION_NUM`] (`AUTO_FRACTION = 1/4`).
pub const AUTO_FRACTION_DEN: u64 = 4;
/// Fraction of *total* RAM the effective worst-case in-flight bound may never
/// exceed, for any configured ceiling and oversubscription factor.
pub const SAFE_FRACTION_NUM: u64 = 1;
/// Denominator of [`SAFE_FRACTION_NUM`] (`SAFE_FRACTION = 1/2`).
pub const SAFE_FRACTION_DEN: u64 = 2;
/// Lower bound on the resolved in-flight ceiling.
///
/// Anchored to the **protocol** worst case — one maximally packed block-range
/// request: [`MAX_BS_BLOCKS_PER_REQUEST`] blocks each bounded by
/// [`block::MAX_BLOCK_BYTES`]. Below this no full protocol-max request could
/// ever be admitted, so `auto` never resolves under it (a host that cannot
/// afford even this much under [`SAFE_FRACTION`] is warned as under-provisioned).
// `MAX_BS_BLOCKS_PER_REQUEST` is a `u32` protocol constant (128) and
// `MAX_BLOCK_BYTES` is 2_000_000; their product (256_000_000) fits in `u64`.
pub const MIN_CEILING: u64 = MAX_BS_BLOCKS_PER_REQUEST as u64 * block::MAX_BLOCK_BYTES;
/// Upper bound on the resolved in-flight ceiling (today's flat default, 8 GiB).
///
/// A large host gains nothing from an even larger in-flight body backlog, so the
/// `auto` ceiling never climbs above what the flat default already used.
pub const MAX_CEILING: u64 = DEFAULT_BS_MAX_INFLIGHT_BLOCK_BYTES;

/// Configured ceiling on estimated bytes reserved for in-flight and buffered
/// block bodies.
///
/// Deserializes from **either** a bare TOML integer (=> [`MemoryLimit::Bytes`])
/// **or** the literal string `"auto"` (=> [`MemoryLimit::Auto`]). This is a pure
/// serde/config type: it carries **no** system-memory probing (that lives in
/// `zebrad` behind [`MemoryProbe`], keeping `zebra-network` free of a `sysinfo`
/// dependency). Resolution into a concrete byte count happens in
/// [`resolve_ceiling`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum MemoryLimit {
    /// An explicit byte ceiling. Clamped down (with a warning) at resolution if
    /// it is outside the safe runtime envelope.
    Bytes(u64),
    /// Derive a safe ceiling from system RAM at startup (the default).
    #[default]
    Auto,
}

impl Serialize for MemoryLimit {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            // Round-trips back to the same bare integer / `"auto"` it parsed from.
            MemoryLimit::Bytes(bytes) => serializer.serialize_u64(*bytes),
            MemoryLimit::Auto => serializer.serialize_str("auto"),
        }
    }
}

impl<'de> Deserialize<'de> for MemoryLimit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // A bare `untagged` enum cannot parse a *unit* variant from a string, and
        // `untagged` also fights `deny_unknown_fields` on the embedding struct, so
        // the accept-either logic is written by hand against a single visitor.
        struct MemoryLimitVisitor;

        impl serde::de::Visitor<'_> for MemoryLimitVisitor {
            type Value = MemoryLimit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a byte count or the string \"auto\"")
            }

            fn visit_u64<E>(self, value: u64) -> Result<MemoryLimit, E>
            where
                E: serde::de::Error,
            {
                Ok(MemoryLimit::Bytes(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<MemoryLimit, E>
            where
                E: serde::de::Error,
            {
                // TOML integers are deserialized as `i64`; a memory ceiling is a
                // non-negative byte count, so reject negatives loudly.
                let bytes = u64::try_from(value).map_err(|_| {
                    E::invalid_value(
                        serde::de::Unexpected::Signed(value),
                        &"a non-negative byte count",
                    )
                })?;
                Ok(MemoryLimit::Bytes(bytes))
            }

            fn visit_str<E>(self, value: &str) -> Result<MemoryLimit, E>
            where
                E: serde::de::Error,
            {
                if value.eq_ignore_ascii_case("auto") {
                    Ok(MemoryLimit::Auto)
                } else {
                    Err(E::invalid_value(
                        serde::de::Unexpected::Str(value),
                        &"the string \"auto\" or a byte count",
                    ))
                }
            }
        }

        deserializer.deserialize_any(MemoryLimitVisitor)
    }
}

/// System-memory probe injected at resolution time.
///
/// Lives in `zebra-network` (next to the config type and [`resolve_ceiling`]) so
/// no `sysinfo` dependency crosses the dependency-flow boundary. The production
/// implementation (cgroup-aware, `sysinfo`-backed) lives in `zebrad`; tests
/// inject deterministic figures. Implementations MUST return **cgroup-clamped**
/// values (the min of host and container limit) so every line of
/// [`resolve_ceiling`] is container-correct by construction.
pub trait MemoryProbe {
    /// Currently available RAM in bytes (`MemAvailable`-style), cgroup-clamped.
    fn available(&self) -> u64;
    /// Total RAM in bytes, cgroup-clamped.
    fn total(&self) -> u64;
}

/// Why the in-flight ceiling ended up at its resolved value (for observability).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CeilingSource {
    /// `auto`: derived from a fraction of available RAM, clamped to range.
    Auto,
    /// An explicit configured value used unchanged.
    Explicit,
    /// A configured value clamped for safety (warned).
    Clamped,
}

/// Outcome of resolving a [`MemoryLimit`] against a [`MemoryProbe`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedCeiling {
    /// The concrete in-flight byte ceiling handed to the accounting layer.
    pub ceiling: u64,
    /// Where [`ceiling`](Self::ceiling) came from.
    pub source: CeilingSource,
    /// Probe's reported total RAM (cgroup-clamped).
    pub total: u64,
    /// Probe's reported available RAM (cgroup-clamped).
    pub available: u64,
    /// Effective worst-case in-flight bound `factor × ceiling`.
    pub effective_bound: u64,
    /// True when the host cannot afford [`MIN_CEILING`] under [`SAFE_FRACTION`].
    pub under_provisioned: bool,
    /// True when `auto` could not afford [`MIN_CEILING`] under current available RAM.
    pub available_starved: bool,
}

/// Multiply `value` by `num/den` without intermediate overflow, saturating.
///
/// Used only with the small constant fractions [`AUTO_FRACTION_NUM`] /
/// [`SAFE_FRACTION_NUM`] (numerator 1), so this is integer-exact and avoids the
/// precision loss an `f64` round-trip on large byte counts would introduce.
fn scale_fraction(value: u64, num: u64, den: u64) -> u64 {
    debug_assert!(den != 0, "fraction denominator is a non-zero constant");
    // `value / den * num + (value % den) * num / den`, computed so neither term
    // overflows for the numerators we use (1); saturating as a belt-and-braces
    // guard for any future larger numerator.
    let whole = (value / den).saturating_mul(num);
    let rem = (value % den).saturating_mul(num) / den;
    whole.saturating_add(rem)
}

/// Floor `ceiling * factor` into a byte count, saturating on absurd inputs.
fn effective_bound_for(ceiling: u64, factor: f64) -> u64 {
    // safe: `ceiling` is a byte count and `factor` has already been sanitized by
    // the caller. The result is only used as a conservative runtime bound.
    let raw = (ceiling as f64) * factor;
    if raw >= u64::MAX as f64 {
        u64::MAX
    } else {
        raw as u64
    }
}

/// Resolve a configured [`MemoryLimit`] into a concrete, RAM-safe, cgroup-clamped
/// byte ceiling, validated against the probe's figures.
///
/// `factor` is the sibling plan's `oversubscription_factor`, passed in as a
/// parameter (this standalone plan passes the constant `1.0`). The returned
/// ceiling always satisfies `floor(factor × ceiling) ≤ SAFE_FRACTION × total`,
/// and under `auto` additionally `ceiling ≤ AUTO_FRACTION × available`.
///
/// This is the single seam through which any future RAM-derived p2p limit should
/// be resolved; today it has exactly one consumer (the block-sync in-flight
/// ceiling).
pub fn resolve_ceiling(
    limit: MemoryLimit,
    factor: f64,
    probe: &dyn MemoryProbe,
) -> ResolvedCeiling {
    let total = probe.total();
    let available = probe.available();

    // A non-positive, NaN, or sub-unit factor cannot relax the bound; treat it as
    // the neutral `1.0` so the safety arithmetic below is always well-defined.
    let factor = if factor.is_finite() && factor >= 1.0 {
        factor
    } else {
        1.0
    };

    // `SAFE_FRACTION × total`, computed in integer space (exact for den = 2).
    let safe_total = scale_fraction(total, SAFE_FRACTION_NUM, SAFE_FRACTION_DEN);
    // The largest ceiling whose floored effective bound `factor × ceiling` still
    // fits under `safe_total`: `safe_total / factor`. Clamp it to MAX_CEILING so
    // absurd totals or explicit values cannot open an unbounded backlog.
    let safe_ceiling = {
        // safe: `safe_total` is a byte count; `factor >= 1.0`, so the quotient is
        // `<= safe_total`.
        let raw = (safe_total as f64) / factor;
        let mut ceiling = if raw >= MAX_CEILING as f64 {
            MAX_CEILING
        } else {
            // truncation toward zero is the desired floor for a safety bound.
            raw as u64
        };
        while ceiling > 0 && effective_bound_for(ceiling, factor) > safe_total {
            ceiling -= 1;
        }
        ceiling
    };

    let under_provisioned = MIN_CEILING > safe_total;
    let auto_target = scale_fraction(available, AUTO_FRACTION_NUM, AUTO_FRACTION_DEN);
    let available_starved = matches!(limit, MemoryLimit::Auto) && auto_target < MIN_CEILING;

    let (mut ceiling, source) = match limit {
        MemoryLimit::Auto => {
            let ceiling = if available_starved {
                auto_target
            } else {
                auto_target.clamp(MIN_CEILING, MAX_CEILING)
            };
            (ceiling, CeilingSource::Auto)
        }
        MemoryLimit::Bytes(n) => {
            let configured_ceiling = n.clamp(MIN_CEILING, MAX_CEILING);
            let ceiling = configured_ceiling.min(safe_ceiling);
            let source = if ceiling == n {
                CeilingSource::Explicit
            } else {
                CeilingSource::Clamped
            };
            (ceiling, source)
        }
    };

    // Final guard: regardless of source, the effective worst-case bound must hold.
    // `auto` clamps to `MIN_CEILING` which can exceed `safe_ceiling` on an
    // under-provisioned host; tighten to `safe_ceiling` so the invariant never
    // breaks (the under-provisioned warning is surfaced separately).
    if ceiling > safe_ceiling {
        ceiling = safe_ceiling;
    }

    // `factor × ceiling`, floored, for logging the worst-case in-flight bound.
    let effective_bound = effective_bound_for(ceiling, factor);

    ResolvedCeiling {
        ceiling,
        source,
        total,
        available,
        effective_bound,
        under_provisioned,
        available_starved,
    }
}

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
    pub max_inflight_requests: u16,
    /// Maximum total response bytes the sender targets per requested range.
    pub max_response_bytes: u32,
}

impl BlockSyncStatus {
    pub(super) fn encode_to<W: Write>(&self, writer: &mut W) -> Result<(), BlockSyncWireError> {
        write_height(writer, self.servable_low)?;
        write_height(writer, self.servable_high)?;
        self.tip_hash.zcash_serialize(&mut *writer)?;
        writer.write_u32::<LittleEndian>(clamp_advertised_blocks(self.max_blocks_per_response))?;
        writer.write_u16::<LittleEndian>(self.max_inflight_requests)?;
        writer.write_u32::<LittleEndian>(self.max_response_bytes.max(1))?;
        Ok(())
    }

    pub(super) fn decode_from<R: Read>(reader: &mut R) -> Result<Self, BlockSyncWireError> {
        Ok(Self {
            servable_low: read_height(reader)?,
            servable_high: read_height(reader)?,
            tip_hash: block::Hash::zcash_deserialize(&mut *reader)?,
            max_blocks_per_response: clamp_advertised_blocks(reader.read_u32::<LittleEndian>()?),
            max_inflight_requests: clamp_advertised_inflight(reader.read_u16::<LittleEndian>()?),
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
    pub max_inflight_requests: u16,
    /// Maximum total response bytes this node advertises per `GetBlocks` response.
    pub max_response_bytes: u32,
    /// Maximum estimated bytes reserved for in-flight and buffered block bodies.
    ///
    /// Accepts a bare byte count or `"auto"` (the default), which derives a safe,
    /// cgroup-aware ceiling from system RAM at startup. Resolved into a concrete
    /// `u64` by [`ZakuraBlockSyncConfig::resolve`] before it reaches the runtime.
    pub max_inflight_block_bytes: MemoryLimit,
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
            max_response_bytes: DEFAULT_BS_MAX_RESPONSE_BYTES,
            max_inflight_block_bytes: MemoryLimit::Auto,
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
    pub fn advertised_max_inflight_requests(&self) -> u16 {
        clamp_advertised_inflight(self.max_inflight_requests)
    }

    /// Return the non-zero response byte advertisement for status messages.
    pub fn advertised_max_response_bytes(&self) -> u32 {
        clamp_advertised_response_bytes(self.max_response_bytes)
    }

    /// Return the non-zero verifier submission cap.
    pub fn submitted_apply_limit(&self) -> usize {
        self.max_submitted_block_applies.max(1)
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

    /// Resolve the configured [`MemoryLimit`] against system memory, logging the
    /// outcome, and return a [`ResolvedZakuraBlockSyncConfig`] whose
    /// `max_inflight_block_bytes` is a concrete, RAM-safe, cgroup-clamped `u64`.
    ///
    /// `factor` is the sibling plan's `oversubscription_factor`; standalone this
    /// plan passes `1.0`. After this call the runtime never sees a
    /// [`MemoryLimit`], so [`MemoryLimit::Auto`] can never reach `ByteBudget`.
    pub fn resolve(self, factor: f64, probe: &dyn MemoryProbe) -> ResolvedZakuraBlockSyncConfig {
        let resolved = resolve_ceiling(self.max_inflight_block_bytes, factor, probe);

        let source = match resolved.source {
            CeilingSource::Auto => "auto",
            CeilingSource::Explicit => "explicit",
            CeilingSource::Clamped => "clamped",
        };
        tracing::info!(
            source,
            total_bytes = resolved.total,
            available_bytes = resolved.available,
            resolved_ceiling_bytes = resolved.ceiling,
            effective_bound_bytes = resolved.effective_bound,
            factor,
            "resolved block-sync in-flight memory ceiling from system RAM"
        );
        if matches!(resolved.source, CeilingSource::Clamped) {
            tracing::warn!(
                configured = ?self.max_inflight_block_bytes,
                clamped_to_bytes = resolved.ceiling,
                safe_fraction_of_total_bytes =
                    scale_fraction(resolved.total, SAFE_FRACTION_NUM, SAFE_FRACTION_DEN),
                "configured block-sync in-flight ceiling exceeded a safe fraction of \
                 total RAM; clamped down"
            );
        }
        if resolved.under_provisioned {
            tracing::warn!(
                total_bytes = resolved.total,
                min_ceiling_bytes = MIN_CEILING,
                resolved_ceiling_bytes = resolved.ceiling,
                "host is under-provisioned for block sync: even one protocol-max \
                 request exceeds a safe fraction of total RAM"
            );
        }
        if resolved.available_starved {
            tracing::warn!(
                available_bytes = resolved.available,
                min_ceiling_bytes = MIN_CEILING,
                resolved_ceiling_bytes = resolved.ceiling,
                "host is currently available-memory-starved for block sync: auto \
                 resolved below the protocol-max request floor to avoid overcommitting RAM"
            );
        }

        ResolvedZakuraBlockSyncConfig {
            max_inflight_block_bytes: resolved.ceiling,
            max_blocks_per_response: self.max_blocks_per_response,
            max_inflight_requests: self.max_inflight_requests,
            max_response_bytes: self.max_response_bytes,
            max_submitted_block_applies: self.max_submitted_block_applies,
            request_timeout: self.request_timeout,
            status_refresh_interval: self.status_refresh_interval,
            size_deviation_tolerance: self.size_deviation_tolerance,
            fanout: self.fanout,
            peer_limits: self.peer_limits,
        }
    }
}

/// A [`ZakuraBlockSyncConfig`] whose in-flight memory ceiling has been resolved
/// to a concrete byte count.
///
/// This is the type the block-sync runtime consumes: the deserialized
/// [`MemoryLimit`] (and therefore [`MemoryLimit::Auto`]) never appears past
/// [`ZakuraBlockSyncConfig::resolve`].
#[derive(Clone, Debug)]
pub struct ResolvedZakuraBlockSyncConfig {
    /// Concrete in-flight block-body byte ceiling handed to `ByteBudget::new`.
    pub max_inflight_block_bytes: u64,
    /// Maximum blocks this node advertises per `GetBlocks` response.
    pub max_blocks_per_response: u32,
    /// Maximum concurrent `GetBlocks` requests this node advertises per peer.
    pub max_inflight_requests: u16,
    /// Maximum total response bytes this node advertises per `GetBlocks` response.
    pub max_response_bytes: u32,
    /// Maximum block bodies submitted to the verifier before completed applies
    /// release more submission slots.
    pub max_submitted_block_applies: usize,
    /// Timeout for an outstanding block-body range request.
    pub request_timeout: Duration,
    /// How often this node sends unsolicited status refreshes after local frontier changes.
    pub status_refresh_interval: Duration,
    /// Percentage deviation from advertised body-size hints tolerated before soft scoring.
    pub size_deviation_tolerance: u32,
    /// Number of peers later range scheduling may fan out to for the same body gap.
    pub fanout: usize,
    /// Block-sync peer caps and queue limits owned by this service.
    pub peer_limits: ServicePeerLimits,
}

impl ResolvedZakuraBlockSyncConfig {
    /// Build a runtime config from a concrete, already-resolved byte ceiling.
    ///
    /// Prefer [`ZakuraBlockSyncConfig::resolve`] for production auto-provisioning.
    /// This constructor is for callers that have already resolved the memory
    /// limit at a higher layer and need to carry the remaining block-sync config
    /// fields across the type boundary.
    pub fn from_resolved_ceiling(
        config: ZakuraBlockSyncConfig,
        max_inflight_block_bytes: u64,
    ) -> Self {
        Self {
            max_inflight_block_bytes,
            max_blocks_per_response: config.max_blocks_per_response,
            max_inflight_requests: config.max_inflight_requests,
            max_response_bytes: config.max_response_bytes,
            max_submitted_block_applies: config.max_submitted_block_applies,
            request_timeout: config.request_timeout,
            status_refresh_interval: config.status_refresh_interval,
            size_deviation_tolerance: config.size_deviation_tolerance,
            fanout: config.fanout,
            peer_limits: config.peer_limits,
        }
    }

    /// Build a disabled resolved config for legacy-P2P initialization paths where
    /// no Zakura block-sync runtime can be started.
    pub(crate) fn disabled(config: ZakuraBlockSyncConfig) -> Self {
        Self::from_resolved_ceiling(config, 0)
    }

    /// Build a resolved config that maps the configured limit straight through
    /// for deterministic tests.
    #[cfg(any(test, feature = "zakura-testkit"))]
    pub fn for_test(config: ZakuraBlockSyncConfig) -> Self {
        let max_inflight_block_bytes = match config.max_inflight_block_bytes {
            MemoryLimit::Bytes(n) => n,
            MemoryLimit::Auto => MAX_CEILING,
        };
        Self::from_resolved_ceiling(config, max_inflight_block_bytes)
    }

    /// Return the clamped block-count advertisement for wire status messages.
    pub fn advertised_max_blocks_per_response(&self) -> u32 {
        clamp_advertised_blocks(self.max_blocks_per_response)
    }

    /// Return the locally capped in-flight advertisement for status messages.
    pub fn advertised_max_inflight_requests(&self) -> u16 {
        clamp_advertised_inflight(self.max_inflight_requests)
    }

    /// Return the non-zero response byte advertisement for status messages.
    pub fn advertised_max_response_bytes(&self) -> u32 {
        clamp_advertised_response_bytes(self.max_response_bytes)
    }

    /// Return the non-zero verifier submission cap.
    pub fn submitted_apply_limit(&self) -> usize {
        self.max_submitted_block_applies.max(1)
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

pub(crate) trait BlockSyncConfigAccess {
    fn advertised_max_blocks_per_response(&self) -> u32;
    fn advertised_max_inflight_requests(&self) -> u16;
    fn advertised_max_response_bytes(&self) -> u32;
    fn status_refresh_interval(&self) -> Duration;
}

impl BlockSyncConfigAccess for ZakuraBlockSyncConfig {
    fn advertised_max_blocks_per_response(&self) -> u32 {
        ZakuraBlockSyncConfig::advertised_max_blocks_per_response(self)
    }

    fn advertised_max_inflight_requests(&self) -> u16 {
        ZakuraBlockSyncConfig::advertised_max_inflight_requests(self)
    }

    fn advertised_max_response_bytes(&self) -> u32 {
        ZakuraBlockSyncConfig::advertised_max_response_bytes(self)
    }

    fn status_refresh_interval(&self) -> Duration {
        self.status_refresh_interval
    }
}

impl BlockSyncConfigAccess for ResolvedZakuraBlockSyncConfig {
    fn advertised_max_blocks_per_response(&self) -> u32 {
        ResolvedZakuraBlockSyncConfig::advertised_max_blocks_per_response(self)
    }

    fn advertised_max_inflight_requests(&self) -> u16 {
        ResolvedZakuraBlockSyncConfig::advertised_max_inflight_requests(self)
    }

    fn advertised_max_response_bytes(&self) -> u32 {
        ResolvedZakuraBlockSyncConfig::advertised_max_response_bytes(self)
    }

    fn status_refresh_interval(&self) -> Duration {
        self.status_refresh_interval
    }
}

pub trait IntoResolvedZakuraBlockSyncConfig {
    fn into_resolved(self) -> ResolvedZakuraBlockSyncConfig;
}

impl IntoResolvedZakuraBlockSyncConfig for ResolvedZakuraBlockSyncConfig {
    fn into_resolved(self) -> ResolvedZakuraBlockSyncConfig {
        self
    }
}

#[cfg(test)]
impl IntoResolvedZakuraBlockSyncConfig for ZakuraBlockSyncConfig {
    fn into_resolved(self) -> ResolvedZakuraBlockSyncConfig {
        ResolvedZakuraBlockSyncConfig::for_test(self)
    }
}

/// Clamp an advertised block count to the hard stream-6 request cap.
pub fn clamp_advertised_blocks(count: u32) -> u32 {
    count.clamp(1, MAX_BS_BLOCKS_PER_REQUEST)
}

/// Clamp an advertised in-flight request count to the local status ceiling.
pub fn clamp_advertised_inflight(count: u16) -> u16 {
    count.clamp(1, MAX_BS_INFLIGHT_REQUESTS)
}

/// Clamp an advertised response byte target to the largest stream-6 message.
pub fn clamp_advertised_response_bytes(bytes: u32) -> u32 {
    bytes.clamp(1, MAX_BS_RESPONSE_BYTES)
}

/// Maximum inbound `GetBlocks.count` this node will serve before looking at body sizes.
pub(crate) fn inbound_get_blocks_count_limit(config: &impl BlockSyncConfigAccess) -> u32 {
    config
        .advertised_max_blocks_per_response()
        .clamp(1, MAX_BS_BLOCKS_PER_REQUEST)
}

#[cfg(test)]
mod provisioning_tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::{Arc, Mutex};
    use tracing::subscriber;
    use tracing::Level;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Layer;

    /// One MiB.
    const MIB: u64 = 1024 * 1024;
    /// One GiB.
    const GIB: u64 = 1024 * MIB;

    /// Deterministic injected probe: reports exactly the figures it is given (the
    /// figures are presumed already cgroup-clamped, as the prod probe guarantees).
    struct FixedProbe {
        available: u64,
        total: u64,
    }

    impl MemoryProbe for FixedProbe {
        fn available(&self) -> u64 {
            self.available
        }
        fn total(&self) -> u64 {
            self.total
        }
    }

    // ---- GA.1: resolution unit tests (injected probe, no real sysinfo) ----

    #[test]
    fn ga1_auto_on_large_box_is_quarter_of_available() {
        // 64 GiB available/total: AUTO_FRACTION (1/4) = 16 GiB, clamped to
        // MAX_CEILING = 8 GiB.
        let probe = FixedProbe {
            available: 64 * GIB,
            total: 64 * GIB,
        };
        let r = resolve_ceiling(MemoryLimit::Auto, 1.0, &probe);
        assert_eq!(r.ceiling, MAX_CEILING);
        assert_eq!(r.source, CeilingSource::Auto);
        assert!(!r.under_provisioned);

        // 16 GiB box: 1/4 = 4 GiB, under MAX_CEILING and over MIN_CEILING.
        let probe = FixedProbe {
            available: 16 * GIB,
            total: 16 * GIB,
        };
        let r = resolve_ceiling(MemoryLimit::Auto, 1.0, &probe);
        assert_eq!(r.ceiling, 4 * GIB);
        assert!(r.ceiling <= scale_fraction(probe.available, AUTO_FRACTION_NUM, AUTO_FRACTION_DEN));
    }

    #[test]
    fn ga1_auto_on_small_box_uses_available_pressure_floor() {
        // 4 GiB box: 1/4 = 1 GiB > MIN_CEILING(256 MiB) and 1 GiB <= 1/2*total(2 GiB).
        let probe = FixedProbe {
            available: 4 * GIB,
            total: 4 * GIB,
        };
        let r = resolve_ceiling(MemoryLimit::Auto, 1.0, &probe);
        assert_eq!(r.ceiling, GIB);
        assert!(!r.under_provisioned);

        // 512 MiB box: safe_total is above MIN_CEILING, but current available
        // memory only supports 128 MiB under AUTO_FRACTION. Auto stays under the
        // available-memory policy and warns that startup is memory-starved.
        let probe = FixedProbe {
            available: 512 * MIB,
            total: 512 * MIB,
        };
        let r = resolve_ceiling(MemoryLimit::Auto, 1.0, &probe);
        assert_eq!(r.ceiling, 128 * MIB);
        assert!(!r.under_provisioned);
        assert!(r.available_starved);

        // 256 MiB box: 1/2*total = 128 MiB < MIN_CEILING and AUTO_FRACTION of
        // available is 64 MiB, so auto resolves to the tighter available bound.
        let probe = FixedProbe {
            available: 256 * MIB,
            total: 256 * MIB,
        };
        let r = resolve_ceiling(MemoryLimit::Auto, 1.0, &probe);
        assert!(r.under_provisioned);
        assert!(r.available_starved);
        assert_eq!(r.ceiling, 64 * MIB);
        assert!(u128::from(r.ceiling) <= u128::from(r.total) / 2);
    }

    #[test]
    fn ga1_explicit_over_config_is_clamped() {
        // total 8 GiB -> safe_total 4 GiB; configure 7 GiB -> clamp to 4 GiB.
        let probe = FixedProbe {
            available: 8 * GIB,
            total: 8 * GIB,
        };
        let r = resolve_ceiling(MemoryLimit::Bytes(7 * GIB), 1.0, &probe);
        assert_eq!(r.source, CeilingSource::Clamped);
        assert_eq!(r.ceiling, 4 * GIB);
        assert!(u128::from(r.effective_bound) <= u128::from(r.total) / 2);
    }

    #[test]
    fn ga1_explicit_sane_value_is_unchanged() {
        let probe = FixedProbe {
            available: 8 * GIB,
            total: 8 * GIB,
        };
        let r = resolve_ceiling(MemoryLimit::Bytes(2 * GIB), 1.0, &probe);
        assert_eq!(r.source, CeilingSource::Explicit);
        assert_eq!(r.ceiling, 2 * GIB);
    }

    #[test]
    fn ga1_explicit_low_value_is_clamped_to_min_when_safe() {
        let probe = FixedProbe {
            available: 8 * GIB,
            total: 8 * GIB,
        };
        let r = resolve_ceiling(MemoryLimit::Bytes(0), 1.0, &probe);
        assert_eq!(r.source, CeilingSource::Clamped);
        assert_eq!(r.ceiling, MIN_CEILING);
    }

    #[test]
    fn ga1_explicit_huge_value_is_clamped_to_max_even_on_large_hosts() {
        let probe = FixedProbe {
            available: u64::MAX,
            total: u64::MAX,
        };
        let r = resolve_ceiling(MemoryLimit::Bytes(u64::MAX), 1.0, &probe);
        assert_eq!(r.source, CeilingSource::Clamped);
        assert_eq!(r.ceiling, MAX_CEILING);
        assert!(r.effective_bound <= MAX_CEILING);
    }

    #[test]
    fn ga1_containerized_resolves_from_cgroup_not_host() {
        // The probe already reports the cgroup-clamped figures (256 MiB) even on a
        // 64 GiB host: auto must resolve from the cgroup, never the host.
        let probe = FixedProbe {
            available: 256 * MIB,
            total: 256 * MIB,
        };
        let r = resolve_ceiling(MemoryLimit::Auto, 1.0, &probe);
        // Never the host's 16 GiB (1/4 of 64 GiB) or 8 GiB MAX_CEILING.
        assert!(r.ceiling <= 256 * MIB);
        assert!(u128::from(r.ceiling) <= u128::from(r.total) / 2);
    }

    #[test]
    fn ga1_factor_tightens_explicit_clamp() {
        // total 8 GiB -> safe_total 4 GiB; factor 2.0 -> safe_ceiling 2 GiB.
        let probe = FixedProbe {
            available: 8 * GIB,
            total: 8 * GIB,
        };
        let r = resolve_ceiling(MemoryLimit::Bytes(4 * GIB), 2.0, &probe);
        assert_eq!(r.source, CeilingSource::Clamped);
        assert_eq!(r.ceiling, 2 * GIB);
        assert_eq!(r.effective_bound, 4 * GIB);
        assert!(u128::from(r.effective_bound) <= u128::from(r.total) / 2);
    }

    #[test]
    fn ga1_non_positive_factor_is_treated_as_one() {
        let probe = FixedProbe {
            available: 8 * GIB,
            total: 8 * GIB,
        };
        for bad in [0.0_f64, -1.0, f64::NAN, 0.5] {
            let r = resolve_ceiling(MemoryLimit::Bytes(2 * GIB), bad, &probe);
            assert_eq!(r.ceiling, 2 * GIB, "factor {bad} must not relax the bound");
            assert_eq!(r.effective_bound, 2 * GIB);
        }
    }

    // ---- GA.3: serde + back-compat + deny_unknown_fields interaction ----

    #[test]
    fn ga3_default_memory_limit_is_auto() {
        assert_eq!(MemoryLimit::default(), MemoryLimit::Auto);
        assert_eq!(
            ZakuraBlockSyncConfig::default().max_inflight_block_bytes,
            MemoryLimit::Auto
        );
    }

    #[test]
    fn ga3_auto_string_parses_inside_real_config() {
        let toml = r#"
            replace_legacy_syncer = false
            max_blocks_per_response = 1
            max_inflight_requests = 2048
            max_response_bytes = 33554432
            max_inflight_block_bytes = "auto"
            max_submitted_block_applies = 100
            request_timeout = "10s"
            status_refresh_interval = "30s"
            size_deviation_tolerance = 200
            fanout = 1

            [peer_limits]
        "#;
        let cfg: ZakuraBlockSyncConfig = toml::from_str(toml).expect("auto parses");
        assert_eq!(cfg.max_inflight_block_bytes, MemoryLimit::Auto);
    }

    #[test]
    fn ga3_bare_integer_parses_inside_real_config() {
        let toml = r#"
            max_inflight_block_bytes = 4294967296
            [peer_limits]
        "#;
        let cfg: ZakuraBlockSyncConfig = toml::from_str(toml).expect("integer parses");
        assert_eq!(cfg.max_inflight_block_bytes, MemoryLimit::Bytes(4 * GIB));
    }

    #[test]
    fn ga3_round_trips_both_forms() {
        for limit in [MemoryLimit::Auto, MemoryLimit::Bytes(2 * GIB)] {
            let cfg = ZakuraBlockSyncConfig {
                max_inflight_block_bytes: limit,
                ..ZakuraBlockSyncConfig::default()
            };
            let serialized = toml::to_string(&cfg).expect("serializes");
            let parsed: ZakuraBlockSyncConfig = toml::from_str(&serialized).expect("re-parses");
            assert_eq!(
                parsed.max_inflight_block_bytes, limit,
                "round-trip {limit:?}"
            );
        }
        // The auto form serializes to the literal string, not a number.
        let cfg = ZakuraBlockSyncConfig::default();
        let serialized = toml::to_string(&cfg).expect("serializes");
        assert!(
            serialized.contains("max_inflight_block_bytes = \"auto\""),
            "auto must round-trip as the string: {serialized}"
        );
    }

    #[test]
    fn ga3_deny_unknown_fields_still_rejects_typos() {
        let toml = r#"
            max_inflight_block_bytes = "auto"
            definitely_not_a_field = 1
            [peer_limits]
        "#;
        assert!(
            toml::from_str::<ZakuraBlockSyncConfig>(toml).is_err(),
            "deny_unknown_fields must still reject unknown keys alongside the custom enum"
        );
    }

    #[test]
    fn ga3_negative_integer_is_rejected() {
        let toml = r#"
            max_inflight_block_bytes = -1
            [peer_limits]
        "#;
        assert!(toml::from_str::<ZakuraBlockSyncConfig>(toml).is_err());
    }

    // ---- GA.4: standalone composition invariant (factor = 1.0) ----

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]

        #[test]
        fn ga4_resolved_never_exceeds_safe_fraction_of_total(
            is_auto in any::<bool>(),
            bytes in any::<u64>(),
            total in 1u64..=(256 * GIB),
            available in 0u64..=(256 * GIB),
        ) {
            let limit = if is_auto { MemoryLimit::Auto } else { MemoryLimit::Bytes(bytes) };
            let probe = FixedProbe { available, total };
            let r = resolve_ceiling(limit, 1.0, &probe);

            // GA.4 core: resolved <= SAFE_FRACTION * total (u128 to avoid any
            // intermediate rounding in the assertion itself).
            prop_assert!(
                u128::from(r.ceiling) <= u128::from(total) / 2,
                "ceiling {} > 1/2 total {}", r.ceiling, total,
            );
            // At factor 1.0, effective_bound == ceiling.
            prop_assert_eq!(r.effective_bound, r.ceiling);

            // Under auto, additionally <= AUTO_FRACTION * available, even when
            // current available memory is below MIN_CEILING.
            if is_auto {
                prop_assert!(
                    u128::from(r.ceiling) <= u128::from(available) / 4,
                    "auto ceiling {} > 1/4 available {}", r.ceiling, available,
                );
            }
        }

        #[test]
        fn ga4_factor_aware_bound_holds(
            is_auto in any::<bool>(),
            bytes in any::<u64>(),
            total in 1u64..=(256 * GIB),
            available in 0u64..=(256 * GIB),
            factor in 1.0f64..=16.0,
        ) {
            let limit = if is_auto { MemoryLimit::Auto } else { MemoryLimit::Bytes(bytes) };
            let probe = FixedProbe { available, total };
            let r = resolve_ceiling(limit, factor, &probe);
            // The integer-exact bound the code actually enforces: the *floored*
            // effective bound `factor × ceiling` is `<= SAFE_FRACTION × total`.
            // Asserted in u128 with no factor floor and no slack so it catches any
            // f64-rounding violation where `ceiling × factor` marginally overshoots
            // `safe_total`.
            prop_assert!(
                u128::from(r.effective_bound) <= u128::from(total) / 2,
                "effective_bound {} > 1/2 total {}", r.effective_bound, total,
            );
        }
    }

    // ---- GA.5: resolved-config boundary ----

    #[test]
    fn ga5_test_only_passthrough_unwraps_bytes_and_defaults_auto() {
        let cfg = ZakuraBlockSyncConfig {
            max_inflight_block_bytes: MemoryLimit::Bytes(3 * GIB),
            ..ZakuraBlockSyncConfig::default()
        };
        let resolved = ResolvedZakuraBlockSyncConfig::for_test(cfg);
        // The seam value is a concrete u64 (this is exactly what state.rs reads).
        let _: u64 = resolved.max_inflight_block_bytes;
        assert_eq!(resolved.max_inflight_block_bytes, 3 * GIB);

        let auto = ResolvedZakuraBlockSyncConfig::for_test(ZakuraBlockSyncConfig::default());
        assert_eq!(auto.max_inflight_block_bytes, MAX_CEILING);
    }

    #[test]
    fn ga5_resolve_yields_concrete_u64_seam() {
        let probe = FixedProbe {
            available: 16 * GIB,
            total: 16 * GIB,
        };
        let resolved = ZakuraBlockSyncConfig::default().resolve(1.0, &probe);
        // Auto resolved to a concrete byte count and the resolved runtime config
        // has no MemoryLimit field, so ByteBudget::new never sees Auto.
        let seam: u64 = resolved.max_inflight_block_bytes;
        assert_eq!(seam, 4 * GIB);
        assert_eq!(
            resolved.advertised_max_blocks_per_response(),
            ZakuraBlockSyncConfig::default().advertised_max_blocks_per_response()
        );
    }

    // ---- GA.6: observability ----

    #[derive(Clone, Default)]
    struct CapturedLogs {
        records: Arc<Mutex<Vec<(Level, String, Vec<String>)>>>,
    }

    struct CaptureLayer {
        sink: CapturedLogs,
    }

    struct FieldGrab(Vec<String>);

    impl tracing::field::Visit for FieldGrab {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.push(format!("{}={:?}", field.name(), value));
        }
    }

    impl<S: tracing::Subscriber> Layer<S> for CaptureLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let meta = event.metadata();
            let mut grab = FieldGrab(Vec::new());
            event.record(&mut grab);
            let message = grab
                .0
                .iter()
                .find(|f| f.starts_with("message="))
                .cloned()
                .unwrap_or_default();
            self.sink
                .records
                .lock()
                .expect("log sink mutex healthy")
                .push((*meta.level(), message, grab.0));
        }
    }

    fn capture<F: FnOnce()>(f: F) -> Vec<(Level, String, Vec<String>)> {
        let sink = CapturedLogs::default();
        let layer = CaptureLayer { sink: sink.clone() };
        let subscriber = tracing_subscriber::registry().with(layer);
        subscriber::with_default(subscriber, f);
        let out = sink.records.lock().expect("log sink mutex healthy").clone();
        out
    }

    #[test]
    fn ga6_startup_logs_source_figures_and_bound() {
        let logs = capture(|| {
            let probe = FixedProbe {
                available: 16 * GIB,
                total: 16 * GIB,
            };
            let _ = ZakuraBlockSyncConfig::default().resolve(1.0, &probe);
        });
        let info = logs
            .iter()
            .find(|(lvl, _, _)| *lvl == Level::INFO)
            .expect("startup info log emitted");
        let fields = info.2.join(" ");
        assert!(fields.contains("source="), "missing source: {fields}");
        assert!(fields.contains("total_bytes="), "missing total: {fields}");
        assert!(
            fields.contains("available_bytes="),
            "missing available: {fields}"
        );
        assert!(
            fields.contains("resolved_ceiling_bytes="),
            "missing resolved ceiling: {fields}"
        );
        assert!(
            fields.contains("effective_bound_bytes="),
            "missing effective bound: {fields}"
        );
    }

    #[test]
    fn ga6_over_config_logs_warn() {
        let logs = capture(|| {
            let probe = FixedProbe {
                available: 8 * GIB,
                total: 8 * GIB,
            };
            let cfg = ZakuraBlockSyncConfig {
                max_inflight_block_bytes: MemoryLimit::Bytes(7 * GIB),
                ..ZakuraBlockSyncConfig::default()
            };
            let _ = cfg.resolve(1.0, &probe);
        });
        assert!(
            logs.iter().any(|(lvl, _, _)| *lvl == Level::WARN),
            "over-config must warn"
        );
    }

    #[test]
    fn ga6_under_provisioned_logs_warn() {
        let logs = capture(|| {
            let probe = FixedProbe {
                available: 256 * MIB,
                total: 256 * MIB,
            };
            let _ = ZakuraBlockSyncConfig::default().resolve(1.0, &probe);
        });
        assert!(
            logs.iter().any(|(lvl, _, _)| *lvl == Level::WARN),
            "under-provisioned host must warn"
        );
    }
}
