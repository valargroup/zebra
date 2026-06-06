use std::{fs, thread, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use reqwest::blocking::Client;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

pub fn run_harness(rpc_addr: &str, cookie_path: &str, interval_secs: u64) -> Result<()> {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("building producer RPC client")?;
    println!("starting regtest producer harness: zebra_rpc={rpc_addr}, cookie_path={cookie_path}, interval_secs={interval_secs}");

    let zcashd_conf_path = std::env::var("UNITY_NODE_ZCASHD_CONF")
        .unwrap_or_else(|_| "/var/lib/unity-node/regtest/zcash.conf".to_string());
    let zcashd_rpc = load_zcashd_rpc(&zcashd_conf_path)?;
    println!(
        "producer will mine via zcashd rpc={} and submit to zebra",
        zcashd_rpc.url
    );

    loop {
        let cookie = fs::read_to_string(cookie_path)
            .with_context(|| format!("reading zebra cookie at {cookie_path}"))?;
        let (user, pass) = cookie
            .trim()
            .split_once(':')
            .ok_or_else(|| anyhow!("invalid zebra cookie format"))?;

        let generated_hashes: Vec<String> = rpc_call(
            &client,
            &zcashd_rpc.url,
            &zcashd_rpc.user,
            &zcashd_rpc.pass,
            "generate",
            json!([1]),
        )?;
        if generated_hashes.len() != 1 {
            bail!(
                "generate returned unexpected block hash count: {}",
                generated_hashes.len()
            );
        }

        let block_hex: String = rpc_call(
            &client,
            &zcashd_rpc.url,
            &zcashd_rpc.user,
            &zcashd_rpc.pass,
            "getblock",
            json!([generated_hashes[0], 0]),
        )?;
        let submit_result: Value = rpc_call(
            &client,
            rpc_addr,
            user,
            pass,
            "submitblock",
            json!([block_hex]),
        )?;
        if !is_submit_ok(&submit_result) {
            bail!("submitblock rejected block from zcashd producer: {submit_result}");
        }

        println!("producer mined+submitted block {}", generated_hashes[0]);
        thread::sleep(Duration::from_secs(interval_secs));
    }
}

fn is_submit_ok(result: &Value) -> bool {
    if result.is_null() {
        return true;
    }
    result
        .as_str()
        .map(|s| s.contains("duplicate"))
        .unwrap_or(false)
}

struct ZcashdRpcAuth {
    url: String,
    user: String,
    pass: String,
}

fn load_zcashd_rpc(path: &str) -> Result<ZcashdRpcAuth> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading zcashd conf at {path}"))?;
    let mut user = None::<String>;
    let mut pass = None::<String>;
    let mut port = None::<String>;

    for line in raw.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if let Some(v) = line.strip_prefix("rpcuser=") {
            user = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("rpcpassword=") {
            pass = Some(v.trim().to_string());
        } else if let Some(v) = line.strip_prefix("rpcport=") {
            port = Some(v.trim().to_string());
        }
    }

    let user = user.ok_or_else(|| anyhow!("missing rpcuser in {path}"))?;
    let pass = pass.ok_or_else(|| anyhow!("missing rpcpassword in {path}"))?;
    let port = port.unwrap_or_else(|| "18233".to_string());

    Ok(ZcashdRpcAuth {
        url: format!("http://127.0.0.1:{port}"),
        user,
        pass,
    })
}

fn rpc_call<T: DeserializeOwned>(
    client: &Client,
    rpc_addr: &str,
    user: &str,
    pass: &str,
    method: &str,
    params: Value,
) -> Result<T> {
    let req = json!({
        "jsonrpc": "2.0",
        "id": "unity-node-producer",
        "method": method,
        "params": params,
    });
    let resp = client
        .post(rpc_addr)
        .basic_auth(user, Some(pass))
        .json(&req)
        .send()
        .with_context(|| format!("calling {method} at {rpc_addr}"))?;
    let v: Value = resp.json().context("parsing producer RPC response")?;

    if let Some(err) = v.get("error") {
        if !err.is_null() {
            bail!("rpc error for {method}: {err}");
        }
    }

    let result = v
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow!("missing result field in producer RPC response"))?;
    serde_json::from_value(result).with_context(|| format!("deserializing result for {method}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_ok_accepts_null_and_duplicate_variants() {
        assert!(is_submit_ok(&Value::Null));
        assert!(is_submit_ok(&Value::String("duplicate".to_string())));
        assert!(is_submit_ok(&Value::String(
            "duplicate-inconclusive".to_string()
        )));
        assert!(!is_submit_ok(&Value::String("rejected".to_string())));
        assert!(!is_submit_ok(&json!({"unexpected": true})));
    }

    #[test]
    fn load_zcashd_rpc_reads_credentials_and_default_port() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "unity-node-zcashd-conf-{}.conf",
            std::process::id()
        ));

        fs::write(
            &path,
            "rpcuser=test-user\nrpcpassword=test-pass\n# rpcport omitted on purpose\n",
        )
        .expect("test config write should succeed");

        let parsed = load_zcashd_rpc(
            path.to_str()
                .expect("temporary path should be valid UTF-8 on test platform"),
        )
        .expect("config with credentials should parse");

        fs::remove_file(&path).expect("test config cleanup should succeed");

        assert_eq!(parsed.user, "test-user");
        assert_eq!(parsed.pass, "test-pass");
        assert_eq!(parsed.url, "http://127.0.0.1:18233");
    }
}
