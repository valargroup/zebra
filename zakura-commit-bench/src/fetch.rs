//! Download real blocks from a node's JSON-RPC (`getblock <height> 0`) into a
//! local on-disk cache, so `run` can replay them offline and repeatably.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::Args;
use color_eyre::eyre::{bail, eyre, Result, WrapErr};
use futures::{stream::FuturesUnordered, StreamExt};
use serde_json::json;

use zebra_chain::{block, serialization::ZcashDeserializeInto};

#[derive(Args, Debug)]
pub struct FetchArgs {
    /// JSON-RPC endpoint of a synced node (zebrad/zcashd), e.g. http://127.0.0.1:8232
    #[arg(long)]
    pub rpc_url: String,

    /// Optional HTTP basic-auth user (zcashd rpcuser).
    #[arg(long)]
    pub rpc_user: Option<String>,

    /// Optional HTTP basic-auth password (zcashd rpcpassword).
    #[arg(long)]
    pub rpc_password: Option<String>,

    /// First height to fetch (genesis is 0).
    #[arg(long, default_value_t = 0)]
    pub start: u32,

    /// Last height to fetch (inclusive).
    #[arg(long, default_value_t = 19_999)]
    pub end: u32,

    /// Directory the raw block bytes are cached under.
    #[arg(long, default_value = "target/zakura-commit-bench/blocks")]
    pub cache_dir: PathBuf,

    /// Concurrent in-flight RPC requests.
    #[arg(long, default_value_t = 16)]
    pub concurrency: usize,

    /// Also fetch per-height note-commitment tree roots (z_gettreestate) for the
    /// VCT fast path — needed for production-faithful post-Sapling commit cost.
    #[arg(long, default_value_t = false)]
    pub with_roots: bool,
}

/// Path a height's raw bytes are cached at.
pub fn block_path(cache_dir: &Path, height: u32) -> PathBuf {
    cache_dir.join(format!("{height:08}.bin"))
}

/// Path a height's cached tree roots are stored at.
pub fn roots_path(cache_dir: &Path, height: u32) -> PathBuf {
    cache_dir.join(format!("{height:08}.roots.json"))
}

/// Cached `z_gettreestate` final roots (display-order hex, as the RPC returns).
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CachedRoots {
    pub sapling: Option<String>,
    pub orchard: Option<String>,
}

pub async fn run(args: FetchArgs) -> Result<()> {
    if args.end < args.start {
        bail!("--end ({}) must be >= --start ({})", args.end, args.start);
    }
    std::fs::create_dir_all(&args.cache_dir)
        .wrap_err_with(|| format!("creating cache dir {}", args.cache_dir.display()))?;

    let client = Arc::new(reqwest::Client::builder().build()?);
    let auth = Arc::new(match (&args.rpc_user, &args.rpc_password) {
        (Some(u), p) => Some((u.clone(), p.clone())),
        _ => None,
    });
    let url = Arc::new(args.rpc_url.clone());
    let cache_dir = Arc::new(args.cache_dir.clone());

    let total = (args.end - args.start + 1) as usize;
    let mut fetched = 0usize;
    let mut cached = 0usize;
    let mut in_flight = FuturesUnordered::new();
    let mut next = args.start;

    let drain_one =
        |result: Result<FetchOutcome>, fetched: &mut usize, cached: &mut usize| -> Result<()> {
            match result? {
                FetchOutcome::Downloaded => *fetched += 1,
                FetchOutcome::AlreadyCached => *cached += 1,
            }
            let done = *fetched + *cached;
            if done.is_multiple_of(500) || done == total {
                tracing::info!(
                    done,
                    total,
                    downloaded = *fetched,
                    cached = *cached,
                    "fetch progress"
                );
            }
            Ok(())
        };

    loop {
        while in_flight.len() < args.concurrency && next <= args.end {
            let height = next;
            next += 1;
            let client = client.clone();
            let auth = auth.clone();
            let url = url.clone();
            let cache_dir = cache_dir.clone();
            let with_roots = args.with_roots;
            in_flight.push(async move {
                fetch_one(&client, &url, &auth, &cache_dir, height, with_roots).await
            });
        }

        match in_flight.next().await {
            Some(result) => drain_one(result, &mut fetched, &mut cached)?,
            None => break,
        }
    }

    tracing::info!(
        downloaded = fetched,
        cached,
        total,
        cache_dir = %args.cache_dir.display(),
        "fetch complete"
    );
    Ok(())
}

enum FetchOutcome {
    Downloaded,
    AlreadyCached,
}

async fn rpc_call(
    client: &reqwest::Client,
    url: &str,
    auth: &Option<(String, Option<String>)>,
    method: &str,
    params: serde_json::Value,
    label: &str,
) -> Result<serde_json::Value> {
    let body = json!({
        "jsonrpc": "1.0",
        "id": "zakura-commit-bench",
        "method": method,
        "params": params,
    });
    let mut request = client.post(url).json(&body);
    if let Some((user, password)) = auth {
        request = request.basic_auth(user, password.clone());
    }
    let response = request
        .send()
        .await
        .wrap_err_with(|| format!("{label} request failed"))?;
    let status = response.status();
    let value: serde_json::Value = response
        .json()
        .await
        .wrap_err_with(|| format!("{label} returned non-JSON (status {status})"))?;
    if let Some(error) = value.get("error") {
        if !error.is_null() {
            bail!("{label} RPC error: {error}");
        }
    }
    Ok(value)
}

async fn fetch_one(
    client: &reqwest::Client,
    url: &str,
    auth: &Option<(String, Option<String>)>,
    cache_dir: &Path,
    height: u32,
    with_roots: bool,
) -> Result<FetchOutcome> {
    let path = block_path(cache_dir, height);
    let roots_cached = !with_roots || roots_path(cache_dir, height).is_file();
    if path.is_file() && roots_cached {
        return Ok(FetchOutcome::AlreadyCached);
    }

    if !path.is_file() {
        let value = rpc_call(
            client,
            url,
            auth,
            "getblock",
            // verbosity 0 => raw serialized block hex
            json!([height.to_string(), 0]),
            &format!("getblock {height}"),
        )
        .await?;
        let hex_str = value
            .get("result")
            .and_then(|r| r.as_str())
            .ok_or_else(|| eyre!("getblock {height} result was not a hex string: {value}"))?;
        let bytes = hex::decode(hex_str.trim())
            .wrap_err_with(|| format!("getblock {height} hex decode"))?;

        // Validate it parses and has the expected height before caching.
        let block: block::Block = bytes
            .zcash_deserialize_into()
            .wrap_err_with(|| format!("getblock {height} did not deserialize as a block"))?;
        match block.coinbase_height() {
            Some(block::Height(h)) if h == height => {}
            other => bail!("getblock {height} returned a block at height {other:?}"),
        }

        let tmp = path.with_extension("bin.tmp");
        std::fs::write(&tmp, &bytes).wrap_err_with(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, &path)
            .wrap_err_with(|| format!("renaming into {}", path.display()))?;
    }

    if with_roots && !roots_path(cache_dir, height).is_file() {
        fetch_roots(client, url, auth, cache_dir, height).await?;
    }

    Ok(FetchOutcome::Downloaded)
}

/// Fetch and cache a height's note-commitment tree final roots via z_gettreestate.
async fn fetch_roots(
    client: &reqwest::Client,
    url: &str,
    auth: &Option<(String, Option<String>)>,
    cache_dir: &Path,
    height: u32,
) -> Result<()> {
    let value = rpc_call(
        client,
        url,
        auth,
        "z_gettreestate",
        json!([height.to_string()]),
        &format!("z_gettreestate {height}"),
    )
    .await?;
    let result = value.get("result");
    let final_root = |pool: &str| -> Option<String> {
        result
            .and_then(|r| r.get(pool))
            .and_then(|p| p.get("commitments"))
            .and_then(|c| c.get("finalRoot"))
            .and_then(|r| r.as_str())
            .map(|s| s.to_string())
    };
    let roots = CachedRoots {
        sapling: final_root("sapling"),
        orchard: final_root("orchard"),
    };
    let path = roots_path(cache_dir, height);
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec(&roots)?)
        .wrap_err_with(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).wrap_err_with(|| format!("renaming into {}", path.display()))?;
    Ok(())
}
