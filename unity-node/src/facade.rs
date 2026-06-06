use std::{fs, net::SocketAddr, path::PathBuf, str::FromStr, time::Duration};

use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use reqwest::blocking::Client;
use serde_json::{json, Value};
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};

use crate::{
    config::{Layout, Network, RpcCredentials},
    wallet_rpc::{route_for, zcashd_fallback_methods, WalletRpcProvider},
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);

pub fn run_server(network: Network, state_dir: PathBuf) -> Result<()> {
    let layout = Layout::new(&state_dir, network);
    let creds = crate::config::load_or_create_credentials(&layout.creds_file)?;
    let listener = SocketAddr::from(([127, 0, 0, 1], network.wallet_facade_rpc_port()));
    let server = Server::http(listener)
        .map_err(|err| anyhow!("starting facade HTTP server failed: {err}"))?;
    println!("wallet facade listening at http://{listener}");

    let backends = BackendEndpoints::new(network, &layout, &creds)?;
    let client = Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .context("building facade backend client")?;

    loop {
        let request = match server.recv() {
            Ok(request) => request,
            Err(err) => {
                eprintln!("wallet facade receive error: {err}");
                continue;
            }
        };
        if let Err(err) = handle_request(request, &client, &backends, &creds) {
            eprintln!("wallet facade request handling error: {err}");
        }
    }
}

struct BackendEndpoints {
    zallet_url: String,
    zebra_url: String,
    zcashd_url: String,
    zebra_cookie: (String, String),
    zcashd_auth: (String, String),
}

impl BackendEndpoints {
    fn new(network: Network, layout: &Layout, creds: &RpcCredentials) -> Result<Self> {
        let zallet_url = format!("http://127.0.0.1:{}", network.zallet_rpc_port());
        let zebra_url = format!("http://127.0.0.1:{}", network.zebra_rpc_port());
        let zcashd_url = format!("http://127.0.0.1:{}", network.zcashd_rpc_port());
        let zebra_cookie_raw = fs::read_to_string(layout.cookie_dir.join(".cookie"))
            .context("reading zebra cookie file (.cookie)")?;
        let zebra_cookie =
            parse_user_pass(zebra_cookie_raw.trim()).context("parsing zebra cookie credentials")?;
        let zcashd_auth = (creds.user.clone(), creds.password.clone());

        Ok(Self {
            zallet_url,
            zebra_url,
            zcashd_url,
            zebra_cookie,
            zcashd_auth,
        })
    }
}

fn handle_request(
    mut request: Request,
    client: &Client,
    backends: &BackendEndpoints,
    creds: &RpcCredentials,
) -> Result<()> {
    if request.method() != &Method::Post {
        respond_json(
            request,
            json!({"jsonrpc":"2.0","error":{"code":-32600,"message":"Only POST is supported"},"id":null}),
            405,
        )?;
        return Ok(());
    }

    if !is_authorized(&request, creds)? {
        respond_json(
            request,
            json!({"jsonrpc":"2.0","error":{"code":-32600,"message":"Unauthorized"},"id":null}),
            401,
        )?;
        return Ok(());
    }

    let mut body = String::new();
    request
        .as_reader()
        .read_to_string(&mut body)
        .context("reading facade request body")?;

    let rpc_request: Value = match serde_json::from_str(&body) {
        Ok(req) => req,
        Err(_) => {
            respond_json(
                request,
                json!({"jsonrpc":"2.0","error":{"code":-32700,"message":"Parse error"},"id":null}),
                400,
            )?;
            return Ok(());
        }
    };

    let response = dispatch_rpc(&rpc_request, client, backends);
    respond_json(request, response, 200)?;
    Ok(())
}

fn dispatch_rpc(rpc_request: &Value, client: &Client, backends: &BackendEndpoints) -> Value {
    let method = match rpc_request.get("method").and_then(Value::as_str) {
        Some(method) => method,
        None => {
            return json!({"jsonrpc":"2.0","error":{"code":-32600,"message":"Missing method"},"id":rpc_request.get("id").cloned().unwrap_or(Value::Null)});
        }
    };

    if let Some(provider) = resolve_provider(method) {
        return match provider {
            WalletRpcProvider::ZalletNative => {
                proxy_rpc(client, &backends.zallet_url, rpc_request, None).unwrap_or_else(|err| {
                    rpc_internal_error(
                        rpc_request,
                        format!("zallet backend call failed for {method}: {err}"),
                    )
                })
            }
            WalletRpcProvider::ZcashdFallback => proxy_fallback(rpc_request, client, backends),
            WalletRpcProvider::FacadeCompose => compose_rpc(method, rpc_request, client, backends),
        };
    }

    json!({
        "jsonrpc":"2.0",
        "error":{"code":-32601,"message":format!("Method not found: {method}")},
        "id":rpc_request.get("id").cloned().unwrap_or(Value::Null),
    })
}

fn resolve_provider(method: &str) -> Option<WalletRpcProvider> {
    if let Some(route) = route_for(method) {
        return Some(route.provider);
    }
    if zcashd_fallback_methods().contains(&method) {
        return Some(WalletRpcProvider::ZcashdFallback);
    }
    None
}

fn proxy_fallback(rpc_request: &Value, client: &Client, backends: &BackendEndpoints) -> Value {
    proxy_rpc(
        client,
        &backends.zcashd_url,
        rpc_request,
        Some((&backends.zcashd_auth.0, &backends.zcashd_auth.1)),
    )
    .unwrap_or_else(|err| rpc_internal_error(rpc_request, format!("zcashd fallback failed: {err}")))
}

fn compose_rpc(
    method: &str,
    rpc_request: &Value,
    client: &Client,
    backends: &BackendEndpoints,
) -> Value {
    let mut zallet_response = match proxy_rpc(client, &backends.zallet_url, rpc_request, None) {
        Ok(response) => response,
        Err(err) => return proxy_fallback_on_compose_error(rpc_request, client, backends, err),
    };

    let zebra_tip = match zebra_chain_tip(client, backends) {
        Ok(tip) => Some(tip),
        Err(err) => {
            eprintln!("compose {method}: zebra enrichment unavailable: {err}");
            None
        }
    };

    if let Some(result) = zallet_response.get_mut("result") {
        apply_compose_normalization(method, result, zebra_tip);
    }

    if zallet_response
        .get("error")
        .is_some_and(|err| !err.is_null())
    {
        return proxy_fallback(rpc_request, client, backends);
    }

    zallet_response
}

fn proxy_fallback_on_compose_error(
    rpc_request: &Value,
    client: &Client,
    backends: &BackendEndpoints,
    err: anyhow::Error,
) -> Value {
    eprintln!("compose primary failed, attempting zcashd fallback: {err}");
    proxy_fallback(rpc_request, client, backends)
}

fn apply_compose_normalization(method: &str, result: &mut Value, zebra_tip: Option<i64>) {
    match method {
        "gettransaction" => {
            if let Some(obj) = result.as_object_mut() {
                enrich_confirmations(obj, zebra_tip);
            }
        }
        "listtransactions" | "listunspent" | "z_listreceivedbyaddress" => {
            if let Some(items) = result.as_array_mut() {
                for item in items {
                    if let Some(obj) = item.as_object_mut() {
                        enrich_confirmations(obj, zebra_tip);
                    }
                }
            }
        }
        "listsinceblock" => {
            if let Some(obj) = result.as_object_mut() {
                if let Some(items) = obj.get_mut("transactions").and_then(Value::as_array_mut) {
                    for item in items {
                        if let Some(item_obj) = item.as_object_mut() {
                            enrich_confirmations(item_obj, zebra_tip);
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

fn enrich_confirmations(entry: &mut serde_json::Map<String, Value>, zebra_tip: Option<i64>) {
    if entry.contains_key("confirmations") {
        return;
    }
    let Some(tip) = zebra_tip else {
        return;
    };
    let height = entry
        .get("height")
        .and_then(Value::as_i64)
        .or_else(|| entry.get("blockheight").and_then(Value::as_i64));
    let Some(height) = height else {
        return;
    };
    let conf = tip.saturating_sub(height).saturating_add(1);
    if conf > 0 {
        entry.insert("confirmations".to_string(), Value::from(conf));
    }
}

fn zebra_chain_tip(client: &Client, backends: &BackendEndpoints) -> Result<i64> {
    let req = json!({
        "jsonrpc": "2.0",
        "id": "unity-facade-zebra-tip",
        "method": "getblockchaininfo",
        "params": []
    });
    let response = proxy_rpc(
        client,
        &backends.zebra_url,
        &req,
        Some((&backends.zebra_cookie.0, &backends.zebra_cookie.1)),
    )?;
    response
        .get("result")
        .and_then(|r| r.get("blocks"))
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("zebra getblockchaininfo result missing blocks"))
}

fn proxy_rpc(
    client: &Client,
    url: &str,
    rpc_request: &Value,
    auth: Option<(&str, &str)>,
) -> Result<Value> {
    let mut req = client.post(url).json(rpc_request);
    if let Some((user, pass)) = auth {
        req = req.basic_auth(user, Some(pass));
    }
    let response = req
        .send()
        .with_context(|| format!("calling backend {url} for method {}", rpc_request["method"]))?;
    response
        .json::<Value>()
        .context("parsing backend JSON-RPC response")
}

fn respond_json(request: Request, body: Value, status_code: u16) -> Result<()> {
    let payload = serde_json::to_string(&body).context("serializing facade response body")?;
    let content_type = Header::from_str("Content-Type: application/json")
        .map_err(|_| anyhow!("invalid content-type header literal"))?;
    request
        .respond(
            Response::from_string(payload)
                .with_status_code(StatusCode(status_code))
                .with_header(content_type),
        )
        .context("writing HTTP response")?;
    Ok(())
}

fn is_authorized(request: &Request, creds: &RpcCredentials) -> Result<bool> {
    let Some(header) = request
        .headers()
        .iter()
        .find(|header| header.field.equiv("Authorization"))
    else {
        return Ok(false);
    };
    let value = header.value.as_str();
    let encoded = value
        .strip_prefix("Basic ")
        .ok_or_else(|| anyhow!("unsupported authorization scheme"))?;
    let decoded_bytes = BASE64_STANDARD
        .decode(encoded)
        .context("decoding basic auth payload")?;
    let decoded =
        String::from_utf8(decoded_bytes).context("basic auth payload is not valid UTF-8")?;
    let (user, pass) = parse_user_pass(&decoded)?;
    Ok(user == creds.user && pass == creds.password)
}

fn parse_user_pass(value: &str) -> Result<(String, String)> {
    let (user, pass) = value
        .split_once(':')
        .ok_or_else(|| anyhow!("missing ':' in user:pass pair"))?;
    Ok((user.to_string(), pass.to_string()))
}

fn rpc_internal_error(request: &Value, message: String) -> Value {
    json!({
        "jsonrpc":"2.0",
        "error":{"code":-32603,"message":message},
        "id":request.get("id").cloned().unwrap_or(Value::Null),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enrich_confirmations_uses_height_when_confirmation_missing() {
        let mut entry = serde_json::Map::new();
        entry.insert("height".to_string(), Value::from(15));
        enrich_confirmations(&mut entry, Some(20));
        assert_eq!(
            entry.get("confirmations").and_then(Value::as_i64),
            Some(6),
            "tip 20 and height 15 should produce 6 confirmations"
        );
    }

    #[test]
    fn enrich_confirmations_preserves_existing_value() {
        let mut entry = serde_json::Map::new();
        entry.insert("height".to_string(), Value::from(15));
        entry.insert("confirmations".to_string(), Value::from(42));
        enrich_confirmations(&mut entry, Some(20));
        assert_eq!(entry.get("confirmations").and_then(Value::as_i64), Some(42));
    }

    #[test]
    fn parse_user_pass_requires_delimiter() {
        assert!(parse_user_pass("user:pass").is_ok());
        assert!(parse_user_pass("user-pass").is_err());
    }

    #[test]
    fn resolve_provider_prefers_explicit_routing() {
        assert_eq!(
            resolve_provider("listunspent"),
            Some(WalletRpcProvider::FacadeCompose)
        );
        assert_eq!(
            resolve_provider("z_getbalance"),
            Some(WalletRpcProvider::ZcashdFallback)
        );
        assert_eq!(
            resolve_provider("sendtoaddress"),
            Some(WalletRpcProvider::ZalletNative)
        );
    }

    #[test]
    fn resolve_provider_returns_none_for_unknown_method() {
        assert_eq!(resolve_provider("rpc_method_that_does_not_exist"), None);
    }
}
