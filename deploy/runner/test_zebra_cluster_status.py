#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("zebra-cluster-status.py")


def load_module():
    spec = importlib.util.spec_from_file_location("zebra_cluster_status", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


status = load_module()


class DashboardConfigTests(unittest.TestCase):
    def test_load_dashboard_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "mainnet.nodes.toml").write_text(
                """
[[nodes]]
name = "mainnet-0"
ssh_string = "root@127.0.0.1"
""",
                encoding="utf-8",
            )
            config = root / "dashboard.toml"
            config.write_text(
                """
default_profile = "mainnet"

[profiles.mainnet]
label = "Zebra mainnet"
nodes_config = "mainnet.nodes.toml"
target_spacing = 75.0

[profiles.testnet]
label = "Zakura Ironwood testnet"
nodes_config = "/opt/zebra-dashboard/testnet.nodes.toml"
target_spacing = 7.5
upgrade_height = 4134000
""",
                encoding="utf-8",
            )

            dashboard = status.load_dashboard_config(config)

        self.assertEqual(dashboard.default_profile, "mainnet")
        self.assertEqual(dashboard.profiles["mainnet"].label, "Zebra mainnet")
        self.assertEqual(dashboard.profiles["mainnet"].nodes_config, root / "mainnet.nodes.toml")
        self.assertIsNone(dashboard.profiles["mainnet"].upgrade_height)
        self.assertEqual(dashboard.profiles["testnet"].upgrade_height, 4_134_000)

    def test_legacy_config_creates_default_testnet_profile(self):
        dashboard = status.legacy_dashboard_config(
            Path("/opt/zakura-testnet-dashboard/nodes.toml"),
            target_spacing=7.5,
            upgrade_height=4_134_000,
        )

        self.assertEqual(dashboard.default_profile, "default")
        self.assertEqual(dashboard.profiles["default"].label, "Zakura Ironwood testnet")
        self.assertEqual(dashboard.profiles["default"].target_spacing, 7.5)
        self.assertEqual(dashboard.profiles["default"].upgrade_height, 4_134_000)


class DashboardStateTests(unittest.TestCase):
    def make_collector(self, *, upgrade_height=None):
        node = status.Node(
            name="node-0",
            ssh_string="root@127.0.0.1",
            service_name="zebrad",
            bin_path="/usr/local/bin/zebrad",
            log_file="/root/logs/zebrad.log",
            rpc_listen_addr="127.0.0.1:8232",
            node_id="",
        )
        return status.ClusterCollector(
            [node],
            interval=10.0,
            stale_after=300.0,
            upgrade_height=upgrade_height,
            target_spacing=75.0,
        )

    def make_dashboard_state(self):
        mainnet = status.DashboardProfile(
            id="mainnet",
            label="Zebra mainnet",
            nodes_config=Path("/tmp/mainnet.nodes.toml"),
            target_spacing=75.0,
        )
        testnet = status.DashboardProfile(
            id="testnet",
            label="Zakura Ironwood testnet",
            nodes_config=Path("/tmp/testnet.nodes.toml"),
            target_spacing=7.5,
            upgrade_height=4_134_000,
        )
        config = status.DashboardConfig(
            default_profile="mainnet",
            profiles={"mainnet": mainnet, "testnet": testnet},
        )
        return status.DashboardState(
            config,
            {
                "mainnet": self.make_collector(upgrade_height=None),
                "testnet": self.make_collector(upgrade_height=4_134_000),
            },
        )

    def test_profile_without_upgrade_height_omits_upgrade_estimate(self):
        dashboard = self.make_dashboard_state()

        snapshot = dashboard.snapshot("mainnet")

        self.assertIsNone(snapshot["upgrade"])
        self.assertEqual(snapshot["active_profile"]["id"], "mainnet")

    def test_unknown_profile_data_request_returns_clear_error(self):
        previous = status.DASHBOARD
        status.DASHBOARD = self.make_dashboard_state()
        server = ThreadingHTTPServer(("127.0.0.1", 0), status.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = f"http://127.0.0.1:{server.server_address[1]}/data?profile=missing"
            with self.assertRaises(urllib.error.HTTPError) as raised:
                urllib.request.urlopen(url, timeout=5)
            self.assertEqual(raised.exception.code, 404)
            body = json.loads(raised.exception.read().decode())
            raised.exception.close()
            self.assertEqual(body["error"], "unknown profile")
            self.assertEqual(body["profile"], "missing")
            self.assertEqual(body["default_profile"], "mainnet")
        finally:
            server.shutdown()
            server.server_close()
            status.DASHBOARD = previous


if __name__ == "__main__":
    unittest.main()
