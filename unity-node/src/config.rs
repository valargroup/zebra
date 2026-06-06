use std::{
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Network {
    Regtest,
    Testnet,
    MainnetLike,
}

impl Network {
    pub fn zebra_network_name(self) -> &'static str {
        match self {
            Self::Regtest | Self::MainnetLike => "Regtest",
            Self::Testnet => "Testnet",
        }
    }

    pub fn zebra_p2p_port(self) -> u16 {
        match self {
            Self::Regtest => 18235,
            Self::Testnet => 18234,
            Self::MainnetLike => 19235,
        }
    }

    pub fn zebra_rpc_port(self) -> u16 {
        match self {
            Self::Regtest => 8232,
            Self::Testnet => 18232,
            Self::MainnetLike => 9232,
        }
    }

    pub fn zcashd_rpc_port(self) -> u16 {
        match self {
            Self::Regtest => 18233,
            Self::Testnet => 18236,
            Self::MainnetLike => 19233,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Regtest => "regtest",
            Self::Testnet => "testnet",
            Self::MainnetLike => "mainnet-like",
        }
    }

    pub fn is_private_regtest_like(self) -> bool {
        matches!(self, Self::Regtest | Self::MainnetLike)
    }
}

#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub zebra: BinarySpec,
    pub zcashd: BinarySpec,
}

#[derive(Debug, Deserialize)]
pub struct BinarySpec {
    pub path: PathBuf,
    pub version: String,
    pub sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcCredentials {
    pub user: String,
    pub password: String,
}

pub struct Layout {
    pub network_dir: PathBuf,
    pub run_dir: PathBuf,
    pub logs_dir: PathBuf,
    pub zebra_dir: PathBuf,
    pub zcashd_dir: PathBuf,
    pub cookie_dir: PathBuf,
    pub zebra_conf: PathBuf,
    pub zcashd_conf: PathBuf,
    pub creds_file: PathBuf,
    pub zebra_pid: PathBuf,
    pub zcashd_pid: PathBuf,
    pub producer_pid: PathBuf,
}

impl Layout {
    pub fn new(state_dir: &Path, network: Network) -> Self {
        let network_dir = state_dir.join(network.as_str());
        let run_dir = network_dir.join("run");
        let logs_dir = network_dir.join("logs");
        let zebra_dir = network_dir.join("zebra");
        let zcashd_dir = network_dir.join("zcashd");
        let cookie_dir = network_dir.join("cookies").join("zebra");
        let zebra_conf = network_dir.join("zebra.toml");
        let zcashd_conf = network_dir.join("zcash.conf");
        let creds_file = network_dir.join("rpc-creds.toml");
        let zebra_pid = run_dir.join("zebrad.pid");
        let zcashd_pid = run_dir.join("zcashd.pid");
        let producer_pid = run_dir.join("producer.pid");

        Self {
            network_dir,
            run_dir,
            logs_dir,
            zebra_dir,
            zcashd_dir,
            cookie_dir,
            zebra_conf,
            zcashd_conf,
            creds_file,
            zebra_pid,
            zcashd_pid,
            producer_pid,
        }
    }

    pub fn ensure_dirs(&self) -> Result<()> {
        for dir in [
            &self.network_dir,
            &self.run_dir,
            &self.logs_dir,
            &self.zebra_dir,
            &self.zcashd_dir,
            &self.cookie_dir,
        ] {
            fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        Ok(())
    }
}

pub fn load_manifest(path: &Path) -> Result<Manifest> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("reading manifest {}", path.display()))?;
    let manifest: Manifest = toml::from_str(&raw).context("parsing manifest TOML")?;
    Ok(manifest)
}

pub fn load_or_create_credentials(path: &Path) -> Result<RpcCredentials> {
    if path.exists() {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("reading credentials {}", path.display()))?;
        let creds: RpcCredentials = toml::from_str(&raw).context("parsing credentials TOML")?;
        return Ok(creds);
    }

    let creds = RpcCredentials {
        user: "unity".to_string(),
        password: random_hex(24),
    };
    let rendered = toml::to_string(&creds).context("serializing credentials")?;
    fs::write(path, rendered).with_context(|| format!("writing {}", path.display()))?;
    Ok(creds)
}

pub fn render_zebra_config(
    layout: &Layout,
    network: Network,
    miner_address: &str,
    enable_internal_miner: bool,
) -> String {
    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), network.zebra_p2p_port());
    let rpc_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), network.zebra_rpc_port());

    let internal_miner = if network.is_private_regtest_like() && enable_internal_miner {
        "\ninternal_miner = true"
    } else {
        ""
    };
    let debug_force_finished_sync = if network.is_private_regtest_like() {
        "\ndebug_force_finished_sync = true"
    } else {
        ""
    };
    let mainnet_like_parameters = if matches!(network, Network::MainnetLike) {
        r#"
[network.testnet_parameters]
disable_pow = true
checkpoints = false
extend_funding_stream_addresses_as_required = true

[network.testnet_parameters.activation_heights]
BeforeOverwinter = 1
Overwinter = 1
Sapling = 1
Blossom = 1
Heartwood = 1
Canopy = 1
NU5 = 7
NU6 = 8
"NU6.1" = 100000000
"NU6.2" = 100000000
"#
    } else {
        ""
    };

    format!(
        r#"[network]
network = "{network_name}"
listen_addr = "{listen_addr}"
{mainnet_like_parameters}

[state]
cache_dir = "{state_cache}"

[rpc]
listen_addr = "{rpc_addr}"
enable_cookie_auth = true
cookie_dir = "{cookie_dir}"
{debug_force_finished_sync}

[mining]
miner_address = "{miner_address}"
{internal_miner}
"#,
        network_name = network.zebra_network_name(),
        listen_addr = listen_addr,
        mainnet_like_parameters = mainnet_like_parameters,
        state_cache = layout.zebra_dir.display(),
        rpc_addr = rpc_addr,
        cookie_dir = layout.cookie_dir.display(),
        debug_force_finished_sync = debug_force_finished_sync,
        miner_address = miner_address,
        internal_miner = internal_miner,
    )
}

pub fn render_zcashd_config(layout: &Layout, network: Network, creds: &RpcCredentials) -> String {
    let connect = format!("127.0.0.1:{}", network.zebra_p2p_port());
    let rpc_bind = "127.0.0.1";
    let canonical_follower_settings = if matches!(network, Network::Regtest) {
        "maxconnections=1\ndiscover=0\ndnsseed=0\nnuparams=5ba81b19:1\nnuparams=76b809bb:1\nnuparams=2bb40e60:1\nnuparams=f5b9230b:1\nnuparams=e9ff75a6:1\nnuparams=c2d6d0b4:100000000\nnuparams=c8e71055:100000000\nnuparams=4dec4df0:100000000\nnuparams=5437f330:100000000\n"
    } else if matches!(network, Network::MainnetLike) {
        "maxconnections=1\ndiscover=0\ndnsseed=0\nnuparams=5ba81b19:1\nnuparams=76b809bb:1\nnuparams=2bb40e60:1\nnuparams=f5b9230b:1\nnuparams=e9ff75a6:1\nnuparams=c2d6d0b4:7\nnuparams=c8e71055:8\nnuparams=4dec4df0:100000000\nnuparams=5437f330:100000000\n"
    } else {
        "maxconnections=8\n"
    };

    format!(
        r#"regtest={regtest}
testnet={testnet}
datadir={datadir}
connect={connect}
listen=0
{canonical_follower_settings}rpcbind={rpc_bind}
rpcallowip=127.0.0.1
rpcport={rpc_port}
rpcuser={rpc_user}
rpcpassword={rpc_password}
wallet=1
allowdeprecated=getnewaddress
i-am-aware-zcashd-will-be-replaced-by-zebrad-and-zallet-in-2025=1
"#,
        regtest = if network.is_private_regtest_like() {
            1
        } else {
            0
        },
        testnet = if matches!(network, Network::Testnet) {
            1
        } else {
            0
        },
        datadir = layout.zcashd_dir.display(),
        connect = connect,
        canonical_follower_settings = canonical_follower_settings,
        rpc_bind = rpc_bind,
        rpc_port = network.zcashd_rpc_port(),
        rpc_user = creds.user,
        rpc_password = creds.password,
    )
}

pub fn write_configs(
    layout: &Layout,
    network: Network,
    miner_address: &str,
    creds: &RpcCredentials,
    enable_internal_miner: bool,
) -> Result<()> {
    let zebra_conf = render_zebra_config(layout, network, miner_address, enable_internal_miner);
    fs::write(&layout.zebra_conf, zebra_conf)
        .with_context(|| format!("writing {}", layout.zebra_conf.display()))?;

    let zcashd_conf = render_zcashd_config(layout, network, creds);
    fs::write(&layout.zcashd_conf, zcashd_conf)
        .with_context(|| format!("writing {}", layout.zcashd_conf.display()))?;

    Ok(())
}

pub fn default_miner_address(network: Network) -> &'static str {
    match network {
        Network::Regtest => "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV",
        Network::Testnet => "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV",
        Network::MainnetLike => "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV",
    }
}

pub fn validate_hex_sha256(value: &str) -> Result<()> {
    let decoded = hex::decode(value).context("sha256 must be a hex string")?;
    if decoded.len() != 32 {
        bail!("sha256 must be 32 bytes");
    }
    Ok(())
}

fn random_hex(bytes: usize) -> String {
    let mut data = vec![0u8; bytes];
    OsRng.fill_bytes(&mut data);
    hex::encode(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_accessors_match_expected_values() {
        assert_eq!(Network::Regtest.zebra_network_name(), "Regtest");
        assert_eq!(Network::Testnet.zebra_network_name(), "Testnet");
        assert_eq!(Network::Regtest.zebra_p2p_port(), 18235);
        assert_eq!(Network::Testnet.zebra_p2p_port(), 18234);
        assert_eq!(Network::Regtest.zebra_rpc_port(), 8232);
        assert_eq!(Network::Testnet.zebra_rpc_port(), 18232);
        assert_eq!(Network::Regtest.zcashd_rpc_port(), 18233);
        assert_eq!(Network::Testnet.zcashd_rpc_port(), 18236);
        assert_eq!(Network::MainnetLike.zebra_network_name(), "Regtest");
        assert_eq!(Network::MainnetLike.zebra_p2p_port(), 19235);
        assert_eq!(Network::MainnetLike.zebra_rpc_port(), 9232);
        assert_eq!(Network::MainnetLike.zcashd_rpc_port(), 19233);
        assert!(Network::MainnetLike.is_private_regtest_like());
        assert_eq!(Network::Regtest.as_str(), "regtest");
        assert_eq!(Network::Testnet.as_str(), "testnet");
        assert_eq!(Network::MainnetLike.as_str(), "mainnet-like");
    }

    #[test]
    fn layout_is_network_scoped() {
        let root = Path::new("/tmp/unity-node");
        let regtest = Layout::new(root, Network::Regtest);
        let testnet = Layout::new(root, Network::Testnet);

        assert_eq!(regtest.network_dir, root.join("regtest"));
        assert_eq!(testnet.network_dir, root.join("testnet"));
        assert_eq!(
            regtest.zebra_pid,
            root.join("regtest").join("run/zebrad.pid")
        );
        assert_eq!(
            testnet.zcashd_pid,
            root.join("testnet").join("run/zcashd.pid")
        );
    }

    #[test]
    fn zebra_config_uses_expected_ports_and_miner_address() {
        let layout = Layout::new(Path::new("/tmp/unity-node"), Network::Regtest);
        let rendered = render_zebra_config(
            &layout,
            Network::Regtest,
            "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV",
            true,
        );

        assert!(rendered.contains("network = \"Regtest\""));
        assert!(rendered.contains("listen_addr = \"127.0.0.1:18235\""));
        assert!(rendered.contains("listen_addr = \"127.0.0.1:8232\""));
        assert!(rendered.contains("miner_address = \"tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV\""));
        assert!(rendered.contains("internal_miner = true"));
        assert!(rendered.contains("debug_force_finished_sync = true"));
    }

    #[test]
    fn zebra_config_disables_internal_miner_when_not_requested() {
        let layout = Layout::new(Path::new("/tmp/unity-node"), Network::Regtest);
        let rendered = render_zebra_config(
            &layout,
            Network::Regtest,
            "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV",
            false,
        );

        assert!(!rendered.contains("internal_miner = true"));
        assert!(rendered.contains("debug_force_finished_sync = true"));
    }

    #[test]
    fn zcashd_config_switches_between_regtest_and_testnet() {
        let layout = Layout::new(Path::new("/tmp/unity-node"), Network::Testnet);
        let creds = RpcCredentials {
            user: "u".to_string(),
            password: "p".to_string(),
        };
        let rendered = render_zcashd_config(&layout, Network::Testnet, &creds);

        assert!(rendered.contains("regtest=0"));
        assert!(rendered.contains("testnet=1"));
        assert!(rendered.contains("connect=127.0.0.1:18234"));
        assert!(rendered.contains("rpcport=18236"));
        assert!(rendered.contains("rpcuser=u"));
        assert!(rendered.contains("rpcpassword=p"));
        assert!(rendered.contains("allowdeprecated=getnewaddress"));
        assert!(rendered.contains("maxconnections=8"));
    }

    #[test]
    fn zcashd_regtest_config_enforces_follower_only_settings() {
        let layout = Layout::new(Path::new("/tmp/unity-node"), Network::Regtest);
        let creds = RpcCredentials {
            user: "u".to_string(),
            password: "p".to_string(),
        };
        let rendered = render_zcashd_config(&layout, Network::Regtest, &creds);

        assert!(rendered.contains("connect=127.0.0.1:18235"));
        assert!(rendered.contains("listen=0"));
        assert!(rendered.contains("maxconnections=1"));
        assert!(rendered.contains("discover=0"));
        assert!(rendered.contains("dnsseed=0"));
        assert!(rendered.contains("nuparams=c2d6d0b4:100000000"));
    }

    #[test]
    fn mainnet_like_configs_use_private_regtest_profile() {
        let layout = Layout::new(Path::new("/tmp/unity-node"), Network::MainnetLike);
        let creds = RpcCredentials {
            user: "u".to_string(),
            password: "p".to_string(),
        };

        let zebra = render_zebra_config(
            &layout,
            Network::MainnetLike,
            "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV",
            false,
        );
        assert!(zebra.contains("network = \"Regtest\""));
        assert!(zebra.contains("[network.testnet_parameters]"));
        assert!(zebra.contains("disable_pow = true"));
        assert!(zebra.contains("\"NU6.2\" = 100000000"));

        let zcashd = render_zcashd_config(&layout, Network::MainnetLike, &creds);
        assert!(zcashd.contains("regtest=1"));
        assert!(zcashd.contains("testnet=0"));
        assert!(zcashd.contains("connect=127.0.0.1:19235"));
        assert!(zcashd.contains("rpcport=19233"));
        assert!(zcashd.contains("nuparams=e9ff75a6:1"));
        assert!(zcashd.contains("nuparams=5437f330:100000000"));
    }

    #[test]
    fn validate_hex_sha256_accepts_and_rejects_lengths() {
        assert!(validate_hex_sha256(
            "0000000000000000000000000000000000000000000000000000000000000000"
        )
        .is_ok());
        assert!(validate_hex_sha256("deadbeef").is_err());
        assert!(validate_hex_sha256("zzzz").is_err());
    }
}
