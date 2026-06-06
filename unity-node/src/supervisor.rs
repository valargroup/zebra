use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Result};
use reqwest::blocking::Client;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::config::{
    default_miner_address, load_manifest, load_or_create_credentials, validate_hex_sha256,
    write_configs, Layout, Manifest, Network, RpcCredentials,
};

const READINESS_POLL_INTERVAL: Duration = Duration::from_secs(2);
const READINESS_LOG_EVERY_POLLS: u32 = 5;

#[derive(Clone, Debug)]
pub struct StartOptions {
    pub canonical_regtest: bool,
    pub follower_lag_tolerance: i64,
    pub regtest_producer_external: bool,
    pub external_producer_cmd: Option<String>,
    pub producer_interval_secs: u64,
}

pub fn start_stack(
    network: Network,
    state_dir: &Path,
    manifest_path: &Path,
    timeout_secs: u64,
    options: StartOptions,
) -> Result<()> {
    println!(
        "starting unity-node stack: network={}, state_dir={}, manifest={}, canonical_regtest={}, follower_lag_tolerance={}, regtest_producer_external={}",
        network.as_str(),
        state_dir.display(),
        manifest_path.display(),
        options.canonical_regtest,
        options.follower_lag_tolerance,
        options.regtest_producer_external
    );

    let layout = Layout::new(state_dir, network);
    layout.ensure_dirs()?;

    let manifest = load_manifest(manifest_path)?;
    verify_binary_specs(&manifest)?;

    if process_exists_from_pid_file(&layout.zebra_pid)? {
        bail!("zebrad already running (pid file exists and process is alive)");
    }
    if process_exists_from_pid_file(&layout.zcashd_pid)? {
        bail!("zcashd already running (pid file exists and process is alive)");
    }

    let creds = load_or_create_credentials(&layout.creds_file)?;
    let enable_internal_miner =
        network.is_private_regtest_like() && !options.regtest_producer_external;
    write_configs(
        &layout,
        network,
        default_miner_address(network),
        &creds,
        enable_internal_miner,
    )?;

    println!("starting zebrad, then waiting for readiness");
    start_zebrad_with_backoff(&layout, &manifest, network, timeout_secs)?;
    println!("starting zcashd, then waiting for single peer");
    start_zcashd_with_backoff(&layout, &manifest, network, timeout_secs, &options)?;

    start_regtest_producer_if_needed(network, &layout, &options)?;

    if options.canonical_regtest && network.is_private_regtest_like() {
        verify_canonical_private_behavior(
            network,
            &layout,
            &creds,
            options.follower_lag_tolerance,
        )?;
    }

    println!(
        "unity-node started: network={}, zebrad_pid_file={}, zcashd_pid_file={}",
        network.as_str(),
        layout.zebra_pid.display(),
        layout.zcashd_pid.display()
    );
    Ok(())
}

pub fn status_stack(network: Network, state_dir: &Path, manifest_path: &Path) -> Result<()> {
    let layout = Layout::new(state_dir, network);
    let manifest = load_manifest(manifest_path)?;
    verify_binary_specs(&manifest)?;

    let zebra_pid = read_pid(&layout.zebra_pid).ok();
    let zcashd_pid = read_pid(&layout.zcashd_pid).ok();
    let producer_pid = read_pid(&layout.producer_pid).ok();
    let zebra_alive = zebra_pid.map(process_exists).unwrap_or(false);
    let zcashd_alive = zcashd_pid.map(process_exists).unwrap_or(false);
    let producer_alive = producer_pid.map(process_exists).unwrap_or(false);

    println!(
        "processes: zebrad={} (pid={:?}), zcashd={} (pid={:?}), producer={} (pid={:?})",
        zebra_alive, zebra_pid, zcashd_alive, zcashd_pid, producer_alive, producer_pid
    );

    if zebra_alive {
        match zebra_get_blockchaininfo(network, &layout) {
            Ok(v) => {
                let height = v.get("blocks").and_then(Value::as_i64).unwrap_or_default();
                println!("zebrad rpc: ok, blocks={height}");
            }
            Err(e) => println!("zebrad rpc: error: {e}"),
        }
    } else {
        println!("zebrad rpc: skipped (process down)");
    }

    let creds = load_or_create_credentials(&layout.creds_file)?;
    if zcashd_alive {
        match zcashd_get_blockchaininfo(network, &creds) {
            Ok(v) => {
                let height = v.get("blocks").and_then(Value::as_i64).unwrap_or_default();
                println!("zcashd rpc: ok, blocks={height}");
            }
            Err(e) => println!("zcashd rpc: error: {e}"),
        }
        match zcashd_get_peer_count(network, &creds) {
            Ok(count) => println!("zcashd peers: {count}"),
            Err(e) => println!("zcashd peers: error: {e}"),
        }
    } else {
        println!("zcashd rpc: skipped (process down)");
    }

    Ok(())
}

pub fn stop_stack(network: Network, state_dir: &Path) -> Result<()> {
    println!(
        "stopping unity-node stack: network={}, state_dir={}",
        network.as_str(),
        state_dir.display()
    );
    let layout = Layout::new(state_dir, network);
    stop_from_pid_file(&layout.producer_pid, "producer")?;
    stop_from_pid_file(&layout.zcashd_pid, "zcashd")?;
    stop_from_pid_file(&layout.zebra_pid, "zebrad")?;
    Ok(())
}

fn verify_binary_specs(manifest: &Manifest) -> Result<()> {
    verify_binary(&manifest.zebra.path, &manifest.zebra.sha256).with_context(|| {
        format!(
            "zebrad binary verification failed ({})",
            manifest.zebra.path.display()
        )
    })?;
    verify_binary(&manifest.zcashd.path, &manifest.zcashd.sha256).with_context(|| {
        format!(
            "zcashd binary verification failed ({})",
            manifest.zcashd.path.display()
        )
    })?;
    println!(
        "binary pins: zebrad={} zcashd={}",
        manifest.zebra.version, manifest.zcashd.version
    );
    Ok(())
}

fn start_regtest_producer_if_needed(
    network: Network,
    layout: &Layout,
    options: &StartOptions,
) -> Result<()> {
    if !network.is_private_regtest_like() || !options.regtest_producer_external {
        return Ok(());
    }

    if process_exists_from_pid_file(&layout.producer_pid)? {
        bail!("producer already running (pid file exists and process is alive)");
    }

    let log_path = layout.logs_dir.join("producer.log");
    println!(
        "starting external producer side process (log={})",
        log_path.display()
    );

    let child = if let Some(cmd) = options.external_producer_cmd.as_ref() {
        Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .stdout(open_append(&log_path)?)
            .stderr(open_append(&log_path)?)
            .spawn()
            .context("starting configured external producer command")?
    } else {
        let rpc_addr = format!("http://127.0.0.1:{}", network.zebra_rpc_port());
        let cookie_path = layout.cookie_dir.join(".cookie");
        Command::new(std::env::current_exe().context("getting unity-node executable path")?)
            .env(
                "UNITY_NODE_ZCASHD_CONF",
                layout.zcashd_conf.display().to_string(),
            )
            .arg("producer-harness")
            .arg("--rpc-addr")
            .arg(rpc_addr)
            .arg("--cookie-path")
            .arg(cookie_path)
            .arg("--interval-secs")
            .arg(options.producer_interval_secs.to_string())
            .stdout(open_append(&log_path)?)
            .stderr(open_append(&log_path)?)
            .spawn()
            .context("starting built-in producer harness")?
    };

    write_pid(&layout.producer_pid, child.id())?;
    println!("producer started with pid={}", child.id());
    Ok(())
}

fn verify_binary(path: &Path, expected_sha256_hex: &str) -> Result<()> {
    validate_hex_sha256(expected_sha256_hex)?;
    let data = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(data);
    let actual = hex::encode(hasher.finalize());
    if !sha256_matches(&actual, expected_sha256_hex) {
        bail!(
            "sha256 mismatch for {}: expected {}, got {}",
            path.display(),
            expected_sha256_hex,
            actual
        );
    }
    Ok(())
}

fn start_zebrad_with_backoff(
    layout: &Layout,
    manifest: &Manifest,
    network: Network,
    timeout_secs: u64,
) -> Result<()> {
    let log_path = layout.logs_dir.join("zebrad.log");
    let mut delay_secs = 1u64;

    for attempt in 1..=4 {
        println!(
            "zebrad start attempt {attempt}/4 (log={})",
            log_path.display()
        );
        let child = Command::new(&manifest.zebra.path)
            .arg("-c")
            .arg(&layout.zebra_conf)
            .arg("start")
            .stdout(open_append(&log_path)?)
            .stderr(open_append(&log_path)?)
            .spawn()
            .with_context(|| format!("starting zebrad attempt {attempt}"))?;

        write_pid(&layout.zebra_pid, child.id())?;

        match wait_for_zebra_ready(network, layout, timeout_secs) {
            Ok(()) => return Ok(()),
            Err(err) => {
                println!("zebrad not ready on attempt {attempt}: {err}");
            }
        }

        let _ = stop_from_pid_file(&layout.zebra_pid, "zebrad");
        if attempt < 4 {
            println!("retrying zebrad start in {delay_secs}s");
        }
        thread::sleep(Duration::from_secs(delay_secs));
        delay_secs = delay_secs.saturating_mul(2);
    }

    bail!("zebrad did not become ready after retries")
}

fn start_zcashd_with_backoff(
    layout: &Layout,
    manifest: &Manifest,
    network: Network,
    timeout_secs: u64,
    options: &StartOptions,
) -> Result<()> {
    let log_path = layout.logs_dir.join("zcashd.log");
    let mut delay_secs = 1u64;

    for attempt in 1..=4 {
        println!(
            "zcashd start attempt {attempt}/4 (log={})",
            log_path.display()
        );
        let child = Command::new(&manifest.zcashd.path)
            .arg(format!("-conf={}", layout.zcashd_conf.display()))
            .arg(format!("-datadir={}", layout.zcashd_dir.display()))
            .arg("-printtoconsole=1")
            .stdout(open_append(&log_path)?)
            .stderr(open_append(&log_path)?)
            .spawn()
            .with_context(|| format!("starting zcashd attempt {attempt}"))?;

        write_pid(&layout.zcashd_pid, child.id())?;

        let creds = load_or_create_credentials(&layout.creds_file)?;
        match wait_for_zcashd_peer(network, &creds, timeout_secs, options) {
            Ok(()) => return Ok(()),
            Err(err) => {
                println!("zcashd not ready on attempt {attempt}: {err}");
            }
        }

        let _ = stop_from_pid_file(&layout.zcashd_pid, "zcashd");
        if attempt < 4 {
            println!("retrying zcashd start in {delay_secs}s");
        }
        thread::sleep(Duration::from_secs(delay_secs));
        delay_secs = delay_secs.saturating_mul(2);
    }

    bail!("zcashd did not become ready after retries")
}

fn wait_for_zebra_ready(network: Network, layout: &Layout, timeout_secs: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    let start = Instant::now();
    let mut polls = 0u32;
    let mut last_error = None;
    println!("waiting for zebrad RPC readiness (timeout={timeout_secs}s)");

    while Instant::now() < deadline {
        polls = polls.saturating_add(1);
        match zebra_get_blockchaininfo(network, layout) {
            Ok(_) => {
                println!(
                    "zebrad ready after {}s ({} polls)",
                    start.elapsed().as_secs(),
                    polls
                );
                return Ok(());
            }
            Err(err) => {
                if should_log_readiness_progress(polls) {
                    println!(
                        "zebrad not ready yet after {}s (poll {}): {}",
                        start.elapsed().as_secs(),
                        polls,
                        err
                    );
                }
                last_error = Some(err);
            }
        }
        thread::sleep(READINESS_POLL_INTERVAL);
    }

    if let Some(err) = last_error {
        bail!("timed out waiting for zebrad RPC: {err}");
    }
    bail!("timed out waiting for zebrad RPC")
}

fn wait_for_zcashd_peer(
    network: Network,
    creds: &RpcCredentials,
    timeout_secs: u64,
    options: &StartOptions,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    let start = Instant::now();
    let mut polls = 0u32;
    let mut last_error = None;
    println!("waiting for zcashd peer readiness (timeout={timeout_secs}s)");

    while Instant::now() < deadline {
        polls = polls.saturating_add(1);
        match zcashd_get_peer_addrs(network, creds) {
            Ok(peer_addrs) => {
                let count = peer_addrs.len();
                if peers_are_ready(network, &peer_addrs, options.canonical_regtest) {
                    println!(
                        "zcashd ready after {}s ({} polls)",
                        start.elapsed().as_secs(),
                        polls
                    );
                    return Ok(());
                }
                if should_log_readiness_progress(polls) {
                    println!(
                        "zcashd waiting for expected follower peers after {}s (poll {}): peers={} addrs={:?}",
                        start.elapsed().as_secs(),
                        polls,
                        count,
                        peer_addrs
                    );
                }
            }
            Err(err) => {
                if should_log_readiness_progress(polls) {
                    println!(
                        "zcashd peer check failed after {}s (poll {}): {}",
                        start.elapsed().as_secs(),
                        polls,
                        err
                    );
                }
                last_error = Some(err);
            }
        }
        thread::sleep(READINESS_POLL_INTERVAL);
    }

    if let Some(err) = last_error {
        bail!("timed out waiting for zcashd single peer: {err}");
    }
    bail!("timed out waiting for zcashd single peer")
}

fn zebra_get_blockchaininfo(network: Network, layout: &Layout) -> Result<Value> {
    let cookie = fs::read_to_string(layout.cookie_dir.join(".cookie"))
        .context("reading zebra cookie file (.cookie)")?;
    let (user, pass) = parse_cookie_auth(&cookie)?;

    let url = format!("http://127.0.0.1:{}", network.zebra_rpc_port());
    rpc_call_basic_auth(&url, "getblockchaininfo", json!([]), user, pass)
}

fn zcashd_get_blockchaininfo(network: Network, creds: &RpcCredentials) -> Result<Value> {
    let url = format!("http://127.0.0.1:{}", network.zcashd_rpc_port());
    rpc_call_basic_auth(
        &url,
        "getblockchaininfo",
        json!([]),
        &creds.user,
        &creds.password,
    )
}

fn zcashd_get_peer_count(network: Network, creds: &RpcCredentials) -> Result<usize> {
    let peers = zcashd_get_peer_addrs(network, creds)?;
    Ok(peers.len())
}

fn zcashd_get_peer_addrs(network: Network, creds: &RpcCredentials) -> Result<Vec<String>> {
    let url = format!("http://127.0.0.1:{}", network.zcashd_rpc_port());
    let peers = rpc_call_basic_auth(&url, "getpeerinfo", json!([]), &creds.user, &creds.password)?;
    let entries = peers
        .as_array()
        .ok_or_else(|| anyhow!("getpeerinfo did not return an array"))?
        .iter()
        .map(|peer| {
            peer.get("addr")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("peer entry missing addr"))
                .map(|s| s.to_string())
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(entries)
}

fn rpc_call_basic_auth(
    url: &str,
    method: &str,
    params: Value,
    user: &str,
    pass: &str,
) -> Result<Value> {
    let client = Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .context("building RPC client")?;
    let req = json!({
        "jsonrpc": "2.0",
        "id": "unity-node",
        "method": method,
        "params": params,
    });
    let resp = client
        .post(url)
        .basic_auth(user, Some(pass))
        .json(&req)
        .send()
        .with_context(|| format!("calling {method} at {url}"))?;

    let v: Value = resp.json().context("parsing RPC JSON response")?;
    if let Some(err) = v.get("error") {
        if !err.is_null() {
            bail!("rpc error for {method}: {err}");
        }
    }
    v.get("result")
        .cloned()
        .ok_or_else(|| anyhow!("missing result field in RPC response"))
}

fn open_append(path: &Path) -> Result<Stdio> {
    let file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    Ok(Stdio::from(file))
}

fn write_pid(path: &Path, pid: u32) -> Result<()> {
    fs::write(path, pid.to_string()).with_context(|| format!("writing pid {}", path.display()))?;
    Ok(())
}

fn read_pid(path: &Path) -> Result<u32> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let pid = raw
        .trim()
        .parse::<u32>()
        .with_context(|| format!("parsing pid from {}", path.display()))?;
    Ok(pid)
}

fn process_exists_from_pid_file(path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let pid = read_pid(path)?;
    Ok(process_exists(pid))
}

fn process_exists(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn sha256_matches(actual_sha256_hex: &str, expected_sha256_hex: &str) -> bool {
    actual_sha256_hex.eq_ignore_ascii_case(expected_sha256_hex)
}

fn should_log_readiness_progress(polls: u32) -> bool {
    polls == 1 || polls.is_multiple_of(READINESS_LOG_EVERY_POLLS)
}

fn peer_count_is_ready(count: usize) -> bool {
    count == 1
}

fn peers_are_ready(network: Network, peer_addrs: &[String], canonical_regtest: bool) -> bool {
    if !peer_count_is_ready(peer_addrs.len()) {
        return false;
    }

    if canonical_regtest && network.is_private_regtest_like() {
        return peer_addr_matches_expected_zebra(peer_addrs[0].as_str(), network.zebra_p2p_port());
    }

    true
}

fn peer_addr_matches_expected_zebra(addr: &str, expected_port: u16) -> bool {
    let expected_suffix = format!(":{expected_port}");
    if !addr.ends_with(&expected_suffix) {
        return false;
    }

    addr.starts_with("127.0.0.1:")
        || addr.starts_with("::ffff:127.0.0.1:")
        || addr.starts_with("[::1]:")
}

fn parse_cookie_auth(cookie_contents: &str) -> Result<(&str, &str)> {
    cookie_contents
        .trim()
        .split_once(':')
        .ok_or_else(|| anyhow!("invalid zebra cookie format"))
}

fn verify_canonical_private_behavior(
    network: Network,
    layout: &Layout,
    creds: &RpcCredentials,
    follower_lag_tolerance: i64,
) -> Result<()> {
    println!(
        "verifying canonical private producer/follower behavior (network={})",
        network.as_str()
    );
    let mut zebra_prev = zebra_blocks(network, layout)?;
    let mut zcashd_prev = zcashd_blocks(network, creds)?;
    let mut zebra_increased = false;
    let mut zcashd_ahead_polls = 0u32;

    for poll in 1..=60 {
        thread::sleep(Duration::from_secs(2));
        let zebra_now = zebra_blocks(network, layout)?;
        let zcashd_now = zcashd_blocks(network, creds)?;
        let peer_addrs = zcashd_get_peer_addrs(network, creds)?;

        if zcashd_now > zebra_now {
            zcashd_ahead_polls = zcashd_ahead_polls.saturating_add(1);
            if zcashd_ahead_polls >= 5 {
                bail!(
                    "divergence detected: zcashd stayed ahead of zebra for {} polls (zebra_blocks={}, zcashd_blocks={})",
                    zcashd_ahead_polls, zebra_now, zcashd_now
                );
            }
        } else {
            zcashd_ahead_polls = 0;
        }

        if zebra_now > zebra_prev {
            zebra_increased = true;
        }

        if !peers_are_ready(network, &peer_addrs, true) {
            bail!(
                "follower isolation violated in canonical mode: expected one zebra loopback peer, got addrs={peer_addrs:?}"
            );
        }

        if should_log_readiness_progress(poll) {
            println!(
                "canonical private check poll {}: zebra_blocks={} zcashd_blocks={} lag={} peers={:?}",
                poll,
                zebra_now,
                zcashd_now,
                zebra_now.saturating_sub(zcashd_now),
                peer_addrs
            );
        }

        zebra_prev = zebra_now;
        zcashd_prev = zcashd_now;
    }

    if !zebra_increased {
        bail!(
            "zebra chain height did not increase during canonical verification window for network={}",
            network.as_str()
        );
    }

    let lag = zebra_prev.saturating_sub(zcashd_prev);
    if lag > follower_lag_tolerance {
        bail!(
            "zcashd follower lag too high: zebra_blocks={zebra_prev}, zcashd_blocks={zcashd_prev}, lag={lag}, tolerance={follower_lag_tolerance}"
        );
    }

    if zcashd_prev > zebra_prev {
        bail!(
            "zcashd ahead of zebra in canonical mode: zebra_blocks={zebra_prev}, zcashd_blocks={zcashd_prev}"
        );
    }

    println!(
        "canonical private behavior verified: network={}, zebra_blocks={zebra_prev}, zcashd_blocks={zcashd_prev}, lag={lag}",
        network.as_str()
    );
    Ok(())
}

fn zebra_blocks(network: Network, layout: &Layout) -> Result<i64> {
    let info = zebra_get_blockchaininfo(network, layout)?;
    info.get("blocks")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("zebra getblockchaininfo missing blocks field"))
}

fn zcashd_blocks(network: Network, creds: &RpcCredentials) -> Result<i64> {
    let info = zcashd_get_blockchaininfo(network, creds)?;
    info.get("blocks")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("zcashd getblockchaininfo missing blocks field"))
}

fn stop_from_pid_file(path: &Path, name: &str) -> Result<()> {
    if !path.exists() {
        println!("{name}: not running (pid file missing)");
        return Ok(());
    }
    let pid = read_pid(path)?;
    if !process_exists(pid) {
        fs::remove_file(path).ok();
        println!("{name}: stale pid file removed");
        return Ok(());
    }

    let term_status = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .with_context(|| format!("sending SIGTERM to {name} pid={pid}"))?;
    if !term_status.success() {
        bail!("failed to SIGTERM {name} pid={pid}");
    }
    println!("{name}: sent SIGTERM to pid={pid}, waiting for exit");

    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if !process_exists(pid) {
            fs::remove_file(path).ok();
            println!("{name}: stopped");
            return Ok(());
        }
        thread::sleep(Duration::from_millis(500));
    }

    let kill_status = Command::new("kill")
        .arg("-KILL")
        .arg(pid.to_string())
        .status()
        .with_context(|| format!("sending SIGKILL to {name} pid={pid}"))?;
    if !kill_status.success() {
        bail!("failed to SIGKILL {name} pid={pid}");
    }
    fs::remove_file(path).ok();
    println!("{name}: sent SIGKILL to pid={pid}, process killed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_progress_logs_on_first_and_periodic_polls() {
        assert!(should_log_readiness_progress(1));
        assert!(!should_log_readiness_progress(2));
        assert!(!should_log_readiness_progress(4));
        assert!(should_log_readiness_progress(5));
        assert!(should_log_readiness_progress(10));
    }

    #[test]
    fn peer_ready_requires_exactly_one_peer() {
        assert!(!peer_count_is_ready(0));
        assert!(peer_count_is_ready(1));
        assert!(!peer_count_is_ready(2));
    }

    #[test]
    fn peer_addr_match_requires_loopback_and_expected_port() {
        assert!(peer_addr_matches_expected_zebra("127.0.0.1:18235", 18235));
        assert!(peer_addr_matches_expected_zebra(
            "::ffff:127.0.0.1:18235",
            18235
        ));
        assert!(peer_addr_matches_expected_zebra("[::1]:18235", 18235));
        assert!(!peer_addr_matches_expected_zebra("127.0.0.1:18234", 18235));
        assert!(!peer_addr_matches_expected_zebra("10.0.0.2:18235", 18235));
    }

    #[test]
    fn peers_are_ready_enforces_canonical_regtest_target() {
        let ok = vec!["127.0.0.1:18235".to_string()];
        let wrong_target = vec!["127.0.0.1:18234".to_string()];
        let too_many = vec!["127.0.0.1:18235".to_string(), "127.0.0.1:18235".to_string()];

        assert!(peers_are_ready(Network::Regtest, &ok, true));
        assert!(!peers_are_ready(Network::Regtest, &wrong_target, true));
        assert!(!peers_are_ready(Network::Regtest, &too_many, true));
        let mainnet_like_ok = vec!["127.0.0.1:19235".to_string()];
        assert!(peers_are_ready(
            Network::MainnetLike,
            &mainnet_like_ok,
            true
        ));
        assert!(peers_are_ready(Network::Testnet, &ok, false));
    }

    #[test]
    fn parse_cookie_auth_handles_valid_and_invalid_values() {
        let parsed = parse_cookie_auth("user:pass\n").expect("cookie with colon should parse");
        assert_eq!(parsed.0, "user");
        assert_eq!(parsed.1, "pass");

        assert!(parse_cookie_auth("missing-delimiter").is_err());
    }

    #[test]
    fn sha256_matching_is_case_insensitive() {
        assert!(sha256_matches("0123abcdef", "0123ABCDEF"));
        assert!(!sha256_matches("0123abcdef", "ffffabcdef"));
    }
}
