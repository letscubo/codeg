"""Pure fake-module coverage. No Hermes SDK, Agent process, network or database."""
import asyncio
import importlib.util
import json
import os
from pathlib import Path
from types import ModuleType, SimpleNamespace
import sys
import subprocess
import tempfile
import textwrap
import unittest
from unittest.mock import Mock, patch

BRIDGE = Path(__file__).with_name("reasoning_bridge.py")


class BridgeTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("bridge_under_test", BRIDGE)
        self.bridge = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.bridge)
        self.config = {"agent": {"reasoning_effort": "high", "reasoning_overrides": {"model": "medium"}}}
        self.parse = Mock(side_effect=lambda value: {
            "low": {"enabled": True, "effort": "low"},
            "high": {"enabled": True, "effort": "high"},
            "none": {"enabled": False},
        }.get(value))
        self.resolve = Mock(return_value={"enabled": True, "effort": "medium"})

        class Agent:
            def __init__(self, *, platform="acp", reasoning_config=None, **kwargs):
                self.reasoning_config = reasoning_config

        class Sessions:
            _agent_factory = None
            def _make_agent(self, *, session_id, cwd, model=None):
                if getattr(self, "fail", False):
                    raise RuntimeError("native construction failed")
                return Agent(platform="acp", model=model)

        class Service:
            async def initialize(self):
                return SimpleNamespace(field_meta={"existing": True})
            async def prompt(self, **kwargs):
                pass
            def _run_agent_turn(self, **kwargs):
                pass
            async def cancel(self, **kwargs):
                pass

        self.Agent, self.Sessions, self.Service = Agent, Sessions, Service
        modules = {}
        for name, attrs in {
            "acp_adapter.session": {"SessionManager": Sessions},
            "acp_adapter.server": {"HermesACPAgent": Service},
            "hermes_cli.config": {"load_config": lambda: self.config},
            "hermes_constants": {"parse_reasoning_effort": self.parse, "resolve_reasoning_config": self.resolve},
            "run_agent": {"AIAgent": Agent},
        }.items():
            module = ModuleType(name)
            module.__dict__.update(attrs)
            modules[name] = module
        self.modules = patch.dict(sys.modules, modules)
        self.modules.start()
        self.env = patch.dict(os.environ, {}, clear=False)
        self.env.start()
        os.environ.pop(self.bridge.SNAPSHOT_ENV, None)

    def tearDown(self):
        self.env.stop()
        self.modules.stop()

    def install(self, level):
        os.environ[self.bridge.SNAPSHOT_ENV] = level
        self.bridge.install_snapshot_bridge()

    def make(self):
        return self.Sessions()._make_agent(session_id="isolated", cwd="/workspace", model="model")

    def test_low_reaches_native_constructor(self):
        self.install("low")
        self.assertEqual(self.make().reasoning_config, {"enabled": True, "effort": "low"})
        self.parse.assert_called_with("low")
        self.resolve.assert_not_called()

    def test_high_snapshot_wins_over_stale_shared_low(self):
        self.config["agent"]["reasoning_effort"] = "low"
        self.install("high")
        self.assertEqual(self.make().reasoning_config["effort"], "high")

    def test_snapshot_is_immutable_after_launch(self):
        self.install("low")
        os.environ[self.bridge.SNAPSHOT_ENV] = "high"
        self.config["agent"]["reasoning_effort"] = "high"
        self.assertEqual(self.make().reasoning_config["effort"], "low")
        with self.assertRaises(RuntimeError):
            self.bridge.install_snapshot_bridge()

    def test_inherit_excludes_another_sessions_scratch_value(self):
        self.install("_inherit")
        self.assertEqual(self.make().reasoning_config["effort"], "medium")
        passed, model = self.resolve.call_args.args
        self.assertNotIn("reasoning_effort", passed["agent"])
        self.assertEqual(passed["agent"]["reasoning_overrides"], {"model": "medium"})
        self.assertEqual(model, "model")
        self.assertEqual(self.config["agent"]["reasoning_effort"], "high")

    def test_inherit_can_restore_native_default_none(self):
        self.resolve.return_value = None
        self.install("_inherit")
        self.assertIsNone(self.make().reasoning_config)

    def test_none_uses_native_disable_semantics(self):
        self.install("none")
        self.assertEqual(self.make().reasoning_config, {"enabled": False})

    def test_unrelated_agents_keep_their_native_configuration(self):
        self.install("low")
        self.assertEqual(self.Agent(platform="cli", reasoning_config={"effort": "high"}).reasoning_config, {"effort": "high"})
        self.assertIsNone(self.Agent(platform="acp").reasoning_config)
        self.assertEqual(self.make().reasoning_config["effort"], "low")

    def test_failed_factory_does_not_leak_its_context(self):
        self.install("low")
        self.Sessions._agent_factory = object()
        with self.assertRaises(RuntimeError):
            self.make()
        self.assertIsNone(self.Agent(platform="acp").reasoning_config)

    def test_native_constructor_exception_clears_the_snapshot_context(self):
        self.install("low")
        sessions = self.Sessions()
        sessions.fail = True
        with self.assertRaises(RuntimeError):
            sessions._make_agent(session_id="failed", cwd="/workspace")
        self.assertIsNone(self.Agent(platform="acp").reasoning_config)

    def test_recreated_agents_keep_the_connection_snapshot(self):
        self.install("high")
        sessions = self.Sessions()
        first = sessions._make_agent(session_id="same", cwd="/workspace", model="one")
        second = sessions._make_agent(session_id="same", cwd="/workspace", model="two")
        self.assertIsNot(first, second)
        self.assertEqual(first.reasoning_config, second.reasoning_config)

    def test_native_agent_ignoring_the_parameter_is_rejected(self):
        def ignores_depth(agent, *, platform="acp", reasoning_config=None, **kwargs):
            agent.reasoning_config = None
        self.Agent.__init__ = ignores_depth
        self.install("low")
        with self.assertRaises(RuntimeError):
            self.make()

    def test_an_unknown_acp_metadata_schema_is_rejected(self):
        async def unknown_schema(service):
            return SimpleNamespace()
        self.Service.initialize = unknown_schema
        self.install("low")
        with self.assertRaises(RuntimeError):
            asyncio.run(self.Service().initialize())

    def test_missing_or_invalid_snapshot_fails(self):
        with self.assertRaises(RuntimeError):
            self.bridge.install_snapshot_bridge()
        with self.assertRaises(RuntimeError):
            self.install("not-a-tier")

    def test_initialize_preserves_metadata_and_confirms_the_bridge(self):
        self.install("high")
        response = asyncio.run(self.Service().initialize())
        self.assertEqual(response.field_meta, {"existing": True, self.bridge.HANDSHAKE_KEY: True})

    def test_install_is_idempotent_for_the_same_snapshot(self):
        self.install("low")
        original = self.Agent.__init__
        self.install("low")
        self.assertIs(self.Agent.__init__, original)

    def test_real_native_and_reasoning_wrappers_preserve_both_initialize_metadata(self):
        # Load only the production shim definitions, never an installed Hermes.
        spec = importlib.util.spec_from_file_location(
            "codeg_reasoning_test_native", BRIDGE.with_name("codeg_native_steering.py"))
        native = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {spec.name: native}):
            spec.loader.exec_module(native)
            native.patch_server(sys.modules["acp_adapter.server"])
            self.install("low")
            response = asyncio.run(self.Service().initialize())
        self.assertTrue(response.field_meta["existing"])
        self.assertEqual(response.field_meta[native.META], {
            "version": 1, "activeOnly": True, "receipts": "consumed"})
        self.assertTrue(response.field_meta[self.bridge.HANDSHAKE_KEY])
        self.assertEqual(self.make().reasoning_config["effort"], "low")


class OwnedBootstrapTests(unittest.TestCase):
    """Production bootstrap + reasoning adapter, with a file-only fake runtime.

    Child Python processes have an isolated HOME and only fake Hermes modules.
    They never import the actual SDK, sqlite3, providers, or installed runtime.
    """
    def run_bootstrap(self, *, native="1", reasoning="1", snapshot="low", command="acp", descendant=False):
        with tempfile.TemporaryDirectory(prefix="codeg-reasoning-bootstrap-") as temporary:
            root = Path(temporary)
            owned, runtime = root / "owned", root / "fake-runtime"
            owned.mkdir()
            runtime.mkdir()
            for name in ["sitecustomize.py", "reasoning_bridge.py"]:
                (owned / name).write_text(BRIDGE.with_name(name).read_text())
            (owned / "codeg_native_steering.py").write_text(textwrap.dedent('''
                import builtins
                def install():
                    builtins._codeg_boot_events = ["native_finder"]
                    return True
            '''))
            sources = {
                "sitecustomize.py": "# Stop chaining before unrelated system customization.\n",
                "acp_adapter/__init__.py": "",
                "acp_adapter/session.py": '''
                    import builtins
                    from run_agent import AIAgent
                    events = getattr(builtins, "_codeg_boot_events", [])
                    events.append("session_import")
                    builtins._codeg_boot_events = events
                    class SessionManager:
                        _agent_factory = None
                        def _make_agent(self, *, session_id, cwd, model=None):
                            return AIAgent(platform="acp")
                ''',
                "acp_adapter/server.py": '''
                    from types import SimpleNamespace
                    class HermesACPAgent:
                        async def initialize(self):
                            return SimpleNamespace(field_meta={"existing": True})
                ''',
                "hermes_cli/__init__.py": "",
                "hermes_cli/config.py": "def load_config(): return {'agent': {}}\n",
                "hermes_constants.py": '''
                    def parse_reasoning_effort(value):
                        return {"enabled": True, "effort": value} if value in ("low", "high") else None
                    def resolve_reasoning_config(config, model):
                        return None
                ''',
                "run_agent.py": '''
                    class AIAgent:
                        def __init__(self, *, platform="acp", reasoning_config=None):
                            self.reasoning_config = reasoning_config
                ''',
            }
            for name, source in sources.items():
                path = runtime / name
                path.parent.mkdir(exist_ok=True)
                path.write_text(textwrap.dedent(source))
            console = root / "fake-console.py"
            console.write_text(textwrap.dedent('''
                import asyncio, builtins, json, os, subprocess, sys
                from acp_adapter.session import SessionManager
                from acp_adapter.server import HermesACPAgent
                agent = SessionManager()._make_agent(session_id="fake", cwd="/fake")
                response = asyncio.run(HermesACPAgent().initialize())
                assert "sqlite3" not in sys.modules
                result = {"depth": agent.reasoning_config, "meta": response.field_meta,
                    "events": getattr(builtins, "_codeg_boot_events", []),
                    "nativeMarker": os.environ.get("CODEG_HERMES_NATIVE_BOOTSTRAP"),
                    "reasoningMarker": os.environ.get("CODEG_HERMES_REASONING_BOOTSTRAP")}
                if "descendant" in sys.argv:
                    child = subprocess.run([sys.executable, "-B", __file__, "acp"],
                        env=dict(os.environ), capture_output=True, text=True, check=True)
                    result["descendant"] = json.loads(child.stdout)
                print(json.dumps(result))
            '''))
            env = {"PATH": os.defpath, "HOME": str(root), "PYTHONNOUSERSITE": "1",
                   "PYTHONDONTWRITEBYTECODE": "1", "PYTHONPATH": os.pathsep.join([str(owned), str(runtime)])}
            for name, value in [("CODEG_HERMES_NATIVE_BOOTSTRAP", native),
                                ("CODEG_HERMES_REASONING_BOOTSTRAP", reasoning),
                                ("CODEG_HERMES_REASONING_EFFORT", snapshot)]:
                if value is not None:
                    env[name] = value
            args = [sys.executable, "-B", str(console), command]
            if descendant:
                args.append("descendant")
            process = subprocess.run(args, cwd=root, env=env, capture_output=True, text=True, timeout=10)
            if process.returncode:
                return process, None
            return process, json.loads(process.stdout)

    def test_owned_bootstrap_installs_native_before_reasoning_imports_for_low_and_high(self):
        for effort in ["low", "high"]:
            process, result = self.run_bootstrap(snapshot=effort)
            self.assertEqual(process.returncode, 0, process.stderr)
            self.assertEqual(result["depth"], {"enabled": True, "effort": effort})
            self.assertEqual(result["events"], ["native_finder", "session_import"])
            self.assertTrue(result["meta"]["codegHermesReasoningSnapshotV1"])
            self.assertIsNone(result["nativeMarker"])
            self.assertIsNone(result["reasoningMarker"])

    def test_native_optout_zero_keeps_reasoning_snapshot_enabled(self):
        process, result = self.run_bootstrap(native="0", snapshot="high")
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(result["depth"]["effort"], "high")
        self.assertEqual(result["events"], ["session_import"])
        self.assertTrue(result["meta"]["codegHermesReasoningSnapshotV1"])

    def test_native_only_and_non_acp_keep_their_original_agent_configuration(self):
        for arguments, events in [({"reasoning": None}, ["native_finder", "session_import"]),
                                  ({"command": "chat"}, ["session_import"])]:
            process, result = self.run_bootstrap(**arguments)
            self.assertEqual(process.returncode, 0, process.stderr)
            self.assertIsNone(result["depth"])
            self.assertEqual(result["meta"], {"existing": True})
            self.assertEqual(result["events"], events)

    def test_invalid_or_missing_snapshot_exits_before_the_console_without_secret_details(self):
        for snapshot in [None, "secret-invalid-value"]:
            process, result = self.run_bootstrap(snapshot=snapshot)
            self.assertEqual(process.returncode, 78)
            self.assertEqual(process.stdout, "")
            self.assertEqual(process.stderr, "Codeg could not initialize Hermes reasoning support\n")
            self.assertIsNone(result)

    def test_consumed_markers_do_not_patch_descendant_acp_processes(self):
        process, result = self.run_bootstrap(descendant=True)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(result["depth"]["effort"], "low")
        self.assertIsNone(result["descendant"]["depth"])
        self.assertEqual(result["descendant"]["events"], ["session_import"])
        self.assertEqual(result["descendant"]["meta"], {"existing": True})


if __name__ == "__main__":
    unittest.main()
