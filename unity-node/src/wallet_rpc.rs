use std::{fs, path::Path};

use anyhow::{Context, Result};
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum WalletRpcProvider {
    ZalletNative,
    FacadeCompose,
    ZcashdFallback,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct WalletRpcRoute {
    pub method: &'static str,
    pub provider: WalletRpcProvider,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum WalletRpcPriority {
    P0,
    P1,
    P2,
}

const P0_ROUTES: [WalletRpcRoute; 26] = [
    WalletRpcRoute {
        method: "getnewaddress",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "z_getnewaddress",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "z_getnewaccount",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "z_getaddressforaccount",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "sendtoaddress",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "sendmany",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "z_sendmany",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "z_shieldcoinbase",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "z_getoperationstatus",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "z_getoperationresult",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "z_listoperationids",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "dumpprivkey",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "importprivkey",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "z_exportkey",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "z_importkey",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "getwalletinfo",
        provider: WalletRpcProvider::ZalletNative,
    },
    WalletRpcRoute {
        method: "listunspent",
        provider: WalletRpcProvider::FacadeCompose,
    },
    WalletRpcRoute {
        method: "gettransaction",
        provider: WalletRpcProvider::FacadeCompose,
    },
    WalletRpcRoute {
        method: "listtransactions",
        provider: WalletRpcProvider::FacadeCompose,
    },
    WalletRpcRoute {
        method: "listsinceblock",
        provider: WalletRpcProvider::FacadeCompose,
    },
    WalletRpcRoute {
        method: "z_listreceivedbyaddress",
        provider: WalletRpcProvider::FacadeCompose,
    },
    WalletRpcRoute {
        method: "z_getbalance",
        provider: WalletRpcProvider::ZcashdFallback,
    },
    WalletRpcRoute {
        method: "getbalance",
        provider: WalletRpcProvider::ZcashdFallback,
    },
    WalletRpcRoute {
        method: "z_gettotalbalance",
        provider: WalletRpcProvider::ZcashdFallback,
    },
    WalletRpcRoute {
        method: "z_getbalanceforaccount",
        provider: WalletRpcProvider::ZcashdFallback,
    },
    WalletRpcRoute {
        method: "z_listunspent",
        provider: WalletRpcProvider::ZcashdFallback,
    },
];

const P0_FALLBACK_METHODS: [&str; 27] = [
    "getnewaddress",
    "z_getnewaddress",
    "z_getnewaccount",
    "z_getaddressforaccount",
    "z_listunifiedreceivers",
    "getbalance",
    "z_gettotalbalance",
    "z_getbalanceforaccount",
    "z_getbalance",
    "z_listreceivedbyaddress",
    "gettransaction",
    "listtransactions",
    "listsinceblock",
    "listunspent",
    "z_listunspent",
    "sendtoaddress",
    "sendmany",
    "z_sendmany",
    "z_shieldcoinbase",
    "z_getoperationstatus",
    "z_getoperationresult",
    "z_listoperationids",
    "dumpprivkey",
    "importprivkey",
    "z_exportkey",
    "z_importkey",
    "getwalletinfo",
];

const P1_FALLBACK_METHODS: [&str; 22] = [
    "getrawchangeaddress",
    "z_listaccounts",
    "z_listaddresses",
    "listaddresses",
    "getunconfirmedbalance",
    "z_getbalanceforviewingkey",
    "getreceivedbyaddress",
    "z_viewtransaction",
    "z_mergetoaddress",
    "z_exportviewingkey",
    "z_importviewingkey",
    "importaddress",
    "dumpwallet",
    "importwallet",
    "z_exportwallet",
    "z_importwallet",
    "backupwallet",
    "encryptwallet",
    "walletpassphrase",
    "walletlock",
    "walletpassphrasechange",
    "signmessage",
];

const P2_FALLBACK_METHODS: [&str; 18] = [
    "listaddressgroupings",
    "addmultisigaddress",
    "z_getnotescount",
    "listlockunspent",
    "lockunspent",
    "settxfee",
    "fundrawtransaction",
    "resendwallettransactions",
    "importpubkey",
    "walletconfirmbackup",
    "keypoolrefill",
    "z_setmigration",
    "z_getmigrationstatus",
    "z_converttex",
    "zcbenchmark",
    "zcsamplejoinsplit",
    "z_getpaymentdisclosure",
    "z_validatepaymentdisclosure",
];

pub fn p0_routes() -> &'static [WalletRpcRoute] {
    &P0_ROUTES
}

pub fn p0_method_names() -> Vec<&'static str> {
    P0_ROUTES.iter().map(|route| route.method).collect()
}

pub fn route_for(method: &str) -> Option<WalletRpcRoute> {
    P0_ROUTES
        .iter()
        .copied()
        .find(|route| route.method == method)
}

pub fn zcashd_fallback_methods() -> Vec<&'static str> {
    let mut methods = Vec::new();
    methods.extend_from_slice(&P0_FALLBACK_METHODS);
    methods.extend_from_slice(&P1_FALLBACK_METHODS);
    methods.extend_from_slice(&P2_FALLBACK_METHODS);
    methods
}

pub fn zcashd_fallback_methods_by_priority() -> Vec<(WalletRpcPriority, Vec<&'static str>)> {
    vec![
        (WalletRpcPriority::P0, P0_FALLBACK_METHODS.to_vec()),
        (WalletRpcPriority::P1, P1_FALLBACK_METHODS.to_vec()),
        (WalletRpcPriority::P2, P2_FALLBACK_METHODS.to_vec()),
    ]
}

pub fn requires_zcashd_fallback() -> bool {
    !zcashd_fallback_methods().is_empty()
}

pub fn write_routing_metadata(path: &Path) -> Result<()> {
    let fallback_by_priority = zcashd_fallback_methods_by_priority()
        .into_iter()
        .map(|(priority, methods)| (priority, methods));
    let payload = serde_json::json!({
        "p0_routes": p0_routes(),
        "zcashd_fallback_methods": zcashd_fallback_methods(),
        "zcashd_fallback_methods_by_priority": fallback_by_priority.collect::<Vec<_>>(),
    });
    fs::write(
        path,
        serde_json::to_string_pretty(&payload).context("serializing routing metadata")?,
    )
    .with_context(|| format!("writing routing metadata {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_table_covers_all_expected_p0_methods() {
        assert_eq!(p0_routes().len(), 26);
        assert_eq!(
            route_for("z_listreceivedbyaddress"),
            Some(WalletRpcRoute {
                method: "z_listreceivedbyaddress",
                provider: WalletRpcProvider::FacadeCompose,
            })
        );
    }

    #[test]
    fn compose_methods_are_marked_for_facade_provider() {
        let compose_methods = [
            "listunspent",
            "gettransaction",
            "listtransactions",
            "listsinceblock",
            "z_listreceivedbyaddress",
        ];
        for method in compose_methods {
            assert_eq!(
                route_for(method),
                Some(WalletRpcRoute {
                    method,
                    provider: WalletRpcProvider::FacadeCompose,
                }),
                "expected {method} to remain routed to composed facade behavior"
            );
        }
    }

    #[test]
    fn fallback_list_matches_plan_assignment() {
        let methods = zcashd_fallback_methods();
        assert_eq!(methods.len(), 67);
        assert!(methods.contains(&"getwalletinfo"));
        assert!(methods.contains(&"z_getbalance"));
        assert!(methods.contains(&"walletpassphrasechange"));
        assert!(methods.contains(&"zcbenchmark"));
        assert!(methods.contains(&"z_listunspent"));
        assert!(requires_zcashd_fallback());
    }

    #[test]
    fn fallback_groups_match_expected_sizes() {
        let groups = zcashd_fallback_methods_by_priority();
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].0, WalletRpcPriority::P0);
        assert_eq!(groups[0].1.len(), 27);
        assert_eq!(groups[1].0, WalletRpcPriority::P1);
        assert_eq!(groups[1].1.len(), 22);
        assert_eq!(groups[2].0, WalletRpcPriority::P2);
        assert_eq!(groups[2].1.len(), 18);
    }

    #[test]
    fn fixture_file_contains_all_p0_methods() {
        let fixture_raw = include_str!("../fixtures/wallet-rpc-p0/contracts.json");
        let fixture_value: serde_json::Value =
            serde_json::from_str(fixture_raw).expect("fixture json should parse");
        let methods = fixture_value
            .get("methods")
            .and_then(serde_json::Value::as_object)
            .expect("fixture should contain methods map");

        for method in p0_method_names() {
            assert!(
                methods.contains_key(method),
                "fixture is missing P0 method {method}"
            );
        }
    }
}
