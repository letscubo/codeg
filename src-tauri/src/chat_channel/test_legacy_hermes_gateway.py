"""Offline fixtures for codeg's installed-official-client bridge. No Hermes process/network."""
import asyncio
from contextlib import asynccontextmanager
import importlib.util
import io
import json
import hashlib
from pathlib import Path
import sys
import sqlite3
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("bridge", Path(__file__).with_name("legacy_hermes_gateway.py"))
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)


class Frames(unittest.TestCase):
    def test_other_turns_reasoning_and_duplicate_frames_cannot_be_body(self):
        wire = io.StringIO()
        def frame(kind, payload, seq=1, admission="mine", epoch=3):
            return {"method":"event", "params":{"session_id":"original", "admission_id":admission,
                "authority_epoch":epoch, "seq":seq, "type":kind, "payload":payload}}
        with patch.object(bridge, "WIRE", wire):
            seen = bridge.relay_frame(frame("message.delta", {"text":"other"}, admission="other"), "original", "mine", 3, 0)
            seen = bridge.relay_frame(frame("reasoning.delta", {"text":"private"}), "original", "mine", 3, seen)
            seen = bridge.relay_frame(frame("message.delta", {"text":"answer"}, seq=2), "original", "mine", 3, seen)
            bridge.relay_frame(frame("message.delta", {"text":"answer"}, seq=2), "original", "mine", 3, seen)
            with self.assertRaisesRegex(ValueError, "hermes_authority_changed"):
                bridge.relay_frame(frame("message.delta", {"text":"bad"}, seq=3, epoch=4), "original", "mine", 3, seen)
        self.assertEqual([json.loads(line) for line in wire.getvalue().splitlines()],
                         [{"event":"message.delta", "delta":"answer"}])

    def test_tool_lifecycle_keeps_identity_and_excludes_tool_payloads(self):
        wire = io.StringIO()
        with patch.object(bridge, "WIRE", wire):
            for seq, kind in enumerate(("tool.start", "tool.complete"), 1):
                bridge.relay_frame({"method":"event", "params":{"session_id":"s", "admission_id":"a",
                    "authority_epoch":1, "seq":seq, "type":kind,
                    "payload":{"tool_id":"id", "name":"shell", "args":"secret", "result":"secret"}}}, "s", "a", 1, seq-1)
        frames = [json.loads(line) for line in wire.getvalue().splitlines()]
        self.assertEqual([f["event"] for f in frames], ["tool.started", "tool.completed"])
        self.assertTrue(all(f["tool_id"] == "id" for f in frames))
        self.assertNotIn("secret", wire.getvalue())


class Inspection(unittest.TestCase):
    def test_exact_routing_index_and_custom_scope_are_read_without_writes(self):
        module = ModuleType("gateway.session")
        def parse(raw):
            return SimpleNamespace(session_key=raw["session_key"], session_id=raw["session_id"],
                model_override={"model":"original-model", "provider":"original-provider"},
                origin=SimpleNamespace(platform=SimpleNamespace(value="telegram"), **raw["origin"]))
        module.SessionEntry = SimpleNamespace(from_dict=parse)
        with tempfile.TemporaryDirectory() as directory, patch.dict(sys.modules,{"gateway.session":module}):
            home = Path(directory)
            sessions_dir = home / "custom-routes"
            database = home / "state.db"
            with sqlite3.connect(database) as db:
                db.execute("CREATE TABLE gateway_routing(scope TEXT, session_key TEXT, entry_json TEXT)")
                db.execute("CREATE TABLE sessions(id TEXT)")
                db.execute("INSERT INTO sessions VALUES('native-id')")
                raw = {"session_key":"opaque/original/key", "session_id":"native-id",
                       "origin":{"chat_id":"123", "thread_id":None, "chat_type":"dm", "user_id":"456"}}
                db.execute("INSERT INTO gateway_routing VALUES(?,?,?)",
                           (str(sessions_dir.resolve()),raw["session_key"],json.dumps(raw)))
                db.execute("INSERT INTO gateway_routing VALUES('other-scope','foreign','{}')")
            before = hashlib.sha256(database.read_bytes()).digest()
            self.assertEqual(bridge.native_sessions(home,sessions_dir), [{"sessionKey":"opaque/original/key",
                "nativeSessionId":"native-id", "chatId":"123", "threadId":None, "chatType":"private",
                "nativeOriginChatType":"dm", "senderId":"456", "model":"original-model", "provider":"original-provider"}])
            self.assertEqual(before,hashlib.sha256(database.read_bytes()).digest())

    def test_stale_or_foreign_owner_status_never_proves_adapter_absence(self):
        modules = {name:ModuleType(name) for name in
                   ("hermes_cli.gateway_runtime","hermes_cli.gateway_runtime_discovery","gateway.status")}
        modules["hermes_cli.gateway_runtime"].control_home_for = lambda home,endpoint:home
        modules["hermes_cli.gateway_runtime_discovery"].query_identify = lambda home,timeout: {
            "instance_id":"i", "pid":10, "start_time":20}
        modules["gateway.status"].runtime_status_is_stale = lambda status:status.get("stale",False)
        status = {"pid":10,"answering_pid":10,"start_time":20,"platforms":{}}
        with patch.dict(sys.modules,modules), patch.object(bridge,"read_control_status",lambda home:status):
            self.assertTrue(bridge.native_adapter_absent(Path("/original"),SimpleNamespace(instance_id="i")))
            status["platforms"] = {"telegram":{"state":"disconnected"}}
            self.assertFalse(bridge.native_adapter_absent(Path("/original"),SimpleNamespace(instance_id="i")))
            status["platforms"] = {}
            status["stale"] = True
            with self.assertRaisesRegex(ValueError,"hermes_status_unverified"):
                bridge.native_adapter_absent(Path("/original"),SimpleNamespace(instance_id="i"))

    def test_simple_allowlist_and_pairing_union_preserve_ids(self):
        platform = SimpleNamespace(channel_overrides={})
        key = type("Platform",(),{"value":"telegram"})()
        config = SimpleNamespace(platforms={key:platform})
        with tempfile.TemporaryDirectory() as directory, patch.dict(bridge.os.environ,
                {"TELEGRAM_ALLOWED_USERS":"123","GATEWAY_ALLOWED_USERS":"456","TELEGRAM_REQUIRE_MENTION":"true"},clear=True):
            home = Path(directory)
            (home/"pairing").mkdir()
            (home/"pairing"/"telegram-approved.json").write_text('{"789":{}}')
            policy,reasons = bridge.simple_policy(config,{},home)
            self.assertEqual(policy,{"allowAllUsers":False,"allowedUserIds":["123","456","789"],"requireMention":True})
            self.assertFalse(reasons)
            policy,reasons = bridge.simple_policy(config,{"allow_from":["123"]},home)
            self.assertEqual(policy["allowedUserIds"],["123"], "global/pairing grants cannot widen adapter intake")
            self.assertFalse(reasons)
            policy,reasons = bridge.simple_policy(config,{"allow_from":[]},home)
            self.assertEqual(policy["allowedUserIds"],[])
            self.assertFalse(policy["allowAllUsers"])
            config.unauthorized_dm_behavior = "decline"
            _,reasons = bridge.simple_policy(config,{},home)
            self.assertIn("hermes_pairing_flow_unsupported",reasons)
            _,reasons = bridge.simple_policy(config,{"groups":{"some":"special"}},home)
            self.assertIn("hermes_complex_telegram_policy_unsupported",reasons)


class Runtime(unittest.IsolatedAsyncioTestCase):
    async def test_other_sessions_cannot_starve_terminal_receipt_observation(self):
        class OtherSessionEvents:
            async def get(self):
                return {"method":"event", "params":{"session_id":"other"}}
            def empty(self):
                return True
        class Client:
            events = OtherSessionEvents()
            async def rpc(self, method, **params):
                self.asserted = (method, params)
                return {"status":"terminal", "outcome":"completed", "result":{"final_response":"done"}}
        async def command():
            await asyncio.sleep(10)
        client = Client()
        wire = io.StringIO()
        with patch.object(bridge, "command", command), patch.object(bridge, "WIRE", wire):
            await asyncio.wait_for(bridge.observe(client,"mine","admission",1,0,poll_interval=0.01),1)
        self.assertEqual(client.asserted, ("prompt.receipt",{"session_id":"mine","admission_id":"admission","include_result":True}))
        self.assertEqual(json.loads(wire.getvalue()), {"event":"run.completed", "output":"done"})

    async def test_original_session_model_and_stable_submission_are_preserved(self):
        calls = []
        endpoint = SimpleNamespace(runtime_protocol=1, capabilities={"session-authority-v1"},
            instance_id="owner", profile_id="profile", authority_epoch=3)
        class Client:
            async def rpc(self, method, **params):
                calls.append((method, params))
                if method == "runtime.describe":
                    return dict(instance_id="owner", profile_id="profile", authority_epoch=3,
                                capabilities=["durable-admission-v1", "event-replay-v1"])
                if method == "session.resume":
                    return {"session_id":"original", "last_sequence":9}
                if method == "prompt.submit":
                    return {"admission_id":"accepted"}
                raise AssertionError(method)
        @asynccontextmanager
        async def connect_gateway(**params):
            self.assertIs(params["endpoint"], endpoint)
            yield Client()
        async def observe(client, session_id, admission_id, epoch, sequence):
            self.assertEqual((session_id, admission_id, epoch, sequence), ("original", "accepted", 3, 9))
        async def command():
            return {"action":"submit"}
        modules = {name:ModuleType(name) for name in
            ("hermes_cli", "hermes_cli.gateway_runtime", "hermes_cli.gateway_client", "gateway", "gateway.session_cli_turn")}
        modules["hermes_cli.gateway_runtime"].discover_gateway_endpoint = lambda home,timeout: SimpleNamespace(state="ready", endpoint=endpoint)
        modules["hermes_cli.gateway_client"].connect_gateway = connect_gateway
        modules["gateway.session_cli_turn"].admit_cli_turn = lambda params: params
        with tempfile.TemporaryDirectory() as home, patch.dict(sys.modules, modules), \
             patch.object(bridge,"WIRE",io.StringIO()), patch.object(bridge,"observe",observe), \
             patch.object(bridge,"command",command), patch.object(bridge,"native_adapter_absent",lambda *args:True), \
             patch.object(bridge,"discover_existing", lambda home: SimpleNamespace(state="ready",endpoint=endpoint)), \
             patch.dict(bridge.os.environ, {}, clear=False):
            await bridge.run({"nativeDeliveryDisabled":True, "hermesHome":home, "sessionId":"original",
                              "message":"hello", "model":"target", "provider":"provider", "turnId":"stable"})
            await bridge.run({"nativeDeliveryDisabled":True, "hermesHome":home, "sessionId":"original",
                              "message":"keep original model", "turnId":"stable-2"})
        self.assertEqual(calls[1], ("session.resume", {"session_id":"original"}))
        self.assertEqual(calls[2], ("prompt.submit", {"session_id":"original", "text":"hello",
            "submission_id":"stable", "model_override":{"model":"target", "provider":"provider"}}))
        self.assertFalse(any(method in ("session.create", "session.reset") for method,_ in calls))
        self.assertEqual(calls[5], ("prompt.submit", {"session_id":"original", "text":"keep original model",
                                                     "submission_id":"stable-2"}))

    async def test_native_delivery_must_be_disabled_before_any_import_or_submission(self):
        with self.assertRaisesRegex(ValueError, "hermes_native_delivery_not_disabled"):
            await bridge.run({"nativeDeliveryDisabled":False})

    async def test_cancel_interrupts_only_the_admitted_generation(self):
        calls = []
        class Client:
            async def rpc(self, method, **params):
                calls.append((method,params))
                return {"status":"started", "execution_generation":7}
        await bridge.stop(Client(), "original", "mine")
        self.assertEqual(calls, [("prompt.receipt",{"session_id":"original","admission_id":"mine"}),
            ("session.interrupt",{"session_id":"original","execution_generation":7})])

    async def test_queued_cancel_does_not_interrupt_an_unrelated_running_turn(self):
        calls = []
        class Client:
            async def rpc(self, method, **params):
                calls.append(method)
                return {"status":"queued"}
        await bridge.stop(Client(), "original", "mine")
        self.assertEqual(calls, ["prompt.receipt", "prompt.cancel"])


if __name__ == "__main__":
    unittest.main()
