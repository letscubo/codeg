"""Offline HTTP contract tests; never load a real Hermes home or call a model."""
import asyncio
from contextlib import ExitStack, nullcontext
import io
import json
from pathlib import Path
from types import ModuleType, SimpleNamespace
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).parent
bridge = ModuleType("http_bridge_fixture")
bridge.__name__ = "http_bridge_fixture"
exec(compile((HERE / "legacy_hermes_http.py").read_text() + "\n" + (HERE / "legacy_hermes_gateway.py").read_text(), "http_bridge_fixture", "exec"), bridge.__dict__)


class HttpFrames(unittest.TestCase):
    def test_only_answer_and_tool_names_are_forwarded_with_one_run_identity(self):
        wire = io.StringIO()
        events = bridge.HttpSessionEvents("native")
        def frame(name, seq, **data):
            return events.accept(name, {"run_id":"run_one", "session_id":"native", "seq":seq, **data})
        with patch.object(bridge, "WIRE", wire):
            frame("run.started", 1, user_message={"content":"private input"})
            frame("tool.progress", 2, tool_name="_thinking", delta="private reasoning")
            frame("assistant.delta", 3, delta="Visible")
            frame("assistant.delta", 3, delta="duplicate")
            frame("tool.started", 4, tool_name="web_search", args={"secret":"private args"})
            frame("tool.completed", 5, tool_name="web_search", preview="private output")
            frame("assistant.completed", 6, content="Visible answer")
            self.assertTrue(frame("run.completed", 7, messages=[{"content":"private transcript"}]))
        text = wire.getvalue()
        self.assertNotIn("private", text)
        self.assertNotIn("duplicate", text)
        self.assertEqual([json.loads(line)["event"] for line in text.splitlines()],
                         ["run.accepted", "message.delta", "tool.started", "tool.completed", "run.completed"])

    def test_foreign_run_and_missing_final_are_rejected(self):
        events = bridge.HttpSessionEvents("native")
        with patch.object(bridge, "WIRE", io.StringIO()):
            events.accept("run.started", {"run_id":"one", "session_id":"native", "seq":1})
            with self.assertRaisesRegex(ValueError, "hermes_session_identity_changed"):
                events.accept("assistant.delta", {"run_id":"other", "session_id":"native", "seq":2, "delta":"foreign"})
            with self.assertRaisesRegex(ValueError, "hermes_final_snapshot_missing"):
                events.accept("run.completed", {"run_id":"one", "session_id":"native", "seq":2})

    def test_completion_must_stay_in_verified_compression_lineage(self):
        events = bridge.HttpSessionEvents("original",lambda sid:sid in ("original","compressed"))
        with patch.object(bridge,"WIRE",io.StringIO()):
            events.accept("run.started",{"run_id":"one","session_id":"original","seq":1})
            with self.assertRaisesRegex(ValueError,"hermes_session_identity_changed"):
                events.accept("assistant.completed",{"run_id":"one","session_id":"reset","seq":2,"content":"foreign"})
            events.accept("assistant.completed",{"run_id":"one","session_id":"compressed","seq":3,"content":"answer"})
            self.assertTrue(events.accept("run.completed",{"run_id":"one","session_id":"compressed","seq":4}))

    def test_private_sender_identity_is_not_forgeable(self):
        for request in [{"chatId":"42","senderId":"7"}, {"chatId":"-42","senderId":"42"},
                        {"chatId":"42","senderId":"42","threadId":"99"}]:
            with self.assertRaises(ValueError): bridge.http_identity(request)
        self.assertEqual(bridge.http_identity({"chatId":"42","senderId":"42"}), ("42","42"))


class Resolve(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        fixture_lock=patch.object(bridge,"http_mutation_lock",lambda _:nullcontext())
        fixture_lock.start()
        self.addCleanup(fixture_lock.stop)

    async def test_pairing_uses_official_store_and_does_not_create_session(self):
        calls = []
        pairing = ModuleType("gateway.pairing")
        pairing.PairingStore = lambda:SimpleNamespace(_is_rate_limited=lambda *args:False,generate_code=lambda *args:(calls.append(args) or "ABCDEFGH"))
        policy = {"allowAllUsers":False,"allowedUserIds":[],"pairingEnabled":True}
        async def inspect(_): return {"ready":True,"sessions":[]}
        with patch.object(bridge,"http_inspect",inspect), patch.object(bridge,"http_context",lambda _:(Path("/fixture"),None,None,None,None,None,policy,[])), patch.dict(sys.modules,{"gateway.pairing":pairing}):
            result = await bridge.http_resolve({"chatId":"42","senderId":"42","senderName":"User","createSession":True})
        self.assertFalse(result["authorized"])
        self.assertIn("`ABCDEFGH`",result["pairingText"])
        self.assertEqual(calls,[("telegram","42","User")])

    async def test_pairing_rate_limit_and_capacity_reply_match_native_behavior(self):
        calls=[]
        pairing=ModuleType("gateway.pairing")
        store=SimpleNamespace(_is_rate_limited=lambda *args:True,generate_code=lambda *args:(calls.append("generated") or None),_record_rate_limit=lambda *args:calls.append("limited"))
        pairing.PairingStore=lambda:store
        policy={"allowAllUsers":False,"allowedUserIds":[],"pairingEnabled":True}
        async def inspect(_):return {"ready":True,"sessions":[]}
        with patch.object(bridge,"http_inspect",inspect), patch.object(bridge,"http_context",lambda _:(Path("/fixture"),None,None,None,None,None,policy,[])),patch.dict(sys.modules,{"gateway.pairing":pairing}):
            result=await bridge.http_resolve({"chatId":"42","senderId":"42","createSession":True})
            self.assertNotIn("pairingText",result)
            self.assertEqual(calls,[])
            store._is_rate_limited=lambda *args:False
            result=await bridge.http_resolve({"chatId":"42","senderId":"42","createSession":True})
        self.assertEqual(result["pairingText"],"Too many pairing requests right now~ Please try again later!")
        self.assertEqual(calls,["generated","limited"])

    async def test_allow_from_cannot_be_widened_by_pairing(self):
        policy = {"allowAllUsers":True,"allowedUserIds":["42"],"pairingEnabled":True,"intakeAllowedUserIds":["7"]}
        async def inspect(_): return {"ready":True,"sessions":[]}
        with patch.object(bridge,"http_inspect",inspect), patch.object(bridge,"http_context",lambda _:(Path("/fixture"),None,None,None,None,None,policy,[])), patch.object(bridge,"http_compression_ids",lambda *args:["old"]):
            result = await bridge.http_resolve({"chatId":"42","senderId":"42","createSession":True})
        self.assertEqual(result,{"authorized":False,"transport":"hermes_http"})

    async def test_first_authorized_chat_uses_official_native_session_store(self):
        created=[]
        native={"chatId":"42","senderId":"42","chatType":"private","threadId":None,"nativeSessionId":"created-id","sessionKey":"native-created-key"}
        config=SimpleNamespace(sessions_dir="/fixture/sessions")
        session=ModuleType("gateway.session")
        session.SessionSource=lambda **kwargs:SimpleNamespace(**kwargs)
        def create(source):
            created.append(source)
            return SimpleNamespace(session_key=native["sessionKey"])
        session.SessionStore=lambda directory,conf:SimpleNamespace(get_or_create_session=create)
        config_module=ModuleType("gateway.config")
        config_module.Platform=SimpleNamespace(TELEGRAM="telegram")
        policy={"allowAllUsers":False,"allowedUserIds":["42"],"pairingEnabled":True}
        async def inspect(_):return {"ready":True,"sessions":[]}
        with patch.object(bridge,"http_inspect",inspect),patch.object(bridge,"http_context",lambda _:(Path("/fixture"),config,None,None,None,None,policy,[])),patch.object(bridge,"http_channel_sessions",lambda *args:[native] if created else []),patch.object(bridge,"http_compression_ids",lambda *args:["created-id"]),patch.dict(sys.modules,{"gateway.session":session,"gateway.config":config_module}):
            result=await bridge.http_resolve({"chatId":"42","senderId":"42","senderName":"Name","createSession":True})
        self.assertEqual(len(created),1)
        self.assertEqual(vars(created[0]),{"platform":"telegram","chat_id":"42","chat_type":"dm","user_id":"42","user_name":"Name"})
        self.assertEqual(result["session"],{**native,"compressionSessionIds":["created-id"]})

    async def test_existing_native_mapping_is_returned_without_replacement(self):
        native = {"chatId":"42","senderId":"42","chatType":"private","threadId":None,"nativeSessionId":"old","sessionKey":"original"}
        policy = {"allowAllUsers":False,"allowedUserIds":["42"],"pairingEnabled":True}
        async def inspect(_): return {"ready":True,"sessions":[native]}
        with patch.object(bridge,"http_inspect",inspect), patch.object(bridge,"http_context",lambda _:(Path("/fixture"),None,None,None,None,None,policy,[])), patch.object(bridge,"http_compression_ids",lambda *args:["old"]):
            result = await bridge.http_resolve({"chatId":"42","senderId":"42","createSession":True})
        self.assertEqual(result["session"],{**native,"compressionSessionIds":["old"]})


class Inspect(unittest.IsolatedAsyncioTestCase):
    async def test_native_active_and_no_token_or_sessions_is_compatible_before_apply(self):
        health={"gateway_busy":False,"gateway_state":"running","active_agents":0,
            "readiness":{"checks":{"background_queues":{"active_api_runs":0,"process_completions":0,"active_delegations":0}}}}
        async def http(client,method,url):
            return {"features":{name:True for name in bridge.HTTP_FEATURES}} if url.endswith("capabilities") else health
        with tempfile.TemporaryDirectory() as directory, patch.object(bridge,"http_json",http), patch.object(bridge,"http_native_absent",lambda *args:False), patch.object(bridge,"http_context",lambda _:(Path(directory),SimpleNamespace(sessions_dir=directory),SimpleNamespace(enabled=True),"http://fixture","fixture-key","",{},[])):
            result=await bridge.http_inspect({"pythonPath":"/fixture/python"})
        self.assertTrue(result["compatible"])
        self.assertFalse(result["ready"])
        self.assertFalse(result["busy"])
        self.assertEqual(result["sessions"],[])
        self.assertIn("hermes_native_adapter_still_present",result["reasonCodes"])
        self.assertIn("hermes_telegram_token_unavailable",result["reasonCodes"])

    async def test_background_api_work_is_busy_and_unknown_is_not_idle(self):
        health={"gateway_busy":False,"gateway_state":"running","active_agents":0,
            "readiness":{"checks":{"background_queues":{"active_api_runs":1,"process_completions":0,"active_delegations":0}}}}
        async def http(client,method,url):
            return {"features":{name:True for name in bridge.HTTP_FEATURES}} if url.endswith("capabilities") else health
        with tempfile.TemporaryDirectory() as directory, patch.object(bridge,"http_json",http), patch.object(bridge,"http_native_absent",lambda *args:True), patch.object(bridge,"http_context",lambda _:(Path(directory),SimpleNamespace(sessions_dir=directory),SimpleNamespace(enabled=False),"http://fixture","fixture-key","token",{},[])):
            result=await bridge.http_inspect({"pythonPath":"/fixture/python"})
            self.assertTrue(result["ready"])
            self.assertTrue(result["busy"])
            health["readiness"]={}
            with self.assertRaisesRegex(ValueError,"hermes_status_unverified"):
                await bridge.http_inspect({"pythonPath":"/fixture/python"})

    def test_status_must_belong_to_the_live_api_process(self):
        status=ModuleType("gateway.status")
        status.read_runtime_status=lambda path:{"pid":42,"start_time":7,"platforms":{}}
        status.runtime_status_pid_is_live=lambda state:True
        status.get_process_start_time=lambda pid:7
        with patch.dict(sys.modules,{"gateway.status":status}):
            self.assertTrue(bridge.http_native_absent(Path("/fixture"),{"pid":42,"platforms":{}}))
            with self.assertRaisesRegex(ValueError,"hermes_status_unverified"):
                bridge.http_native_absent(Path("/fixture"),{"pid":43,"platforms":{}})
            self.assertFalse(bridge.http_native_absent(Path("/fixture"),{"pid":42,"platforms":{"telegram":{}}}))


class IsolatedRoutingScope(unittest.TestCase):
    def test_concurrent_first_chats_preserve_native_scope_and_create_once(self):
        from concurrent.futures import ThreadPoolExecutor
        import time
        with tempfile.TemporaryDirectory() as directory:
            home=Path(directory);(home/"state.db").touch()
            original=home/"sessions";managed=original/".codeg-telegram"
            existing={"sessionKey":"native-41","nativeSessionId":"id-41","chatId":"41","senderId":"41","chatType":"private"}
            records={str(original):[existing],str(managed):[]}
            creates=[]
            class Store:
                def __init__(self,path,config):
                    self.path=str(path)
                    self.snapshot=list(records[self.path])
                def get_or_create_session(self,source):
                    time.sleep(0.02)
                    row={"sessionKey":"native-"+source.chat_id,"nativeSessionId":"id-"+source.chat_id,
                         "chatId":source.chat_id,"senderId":source.user_id,"chatType":"private"}
                    self.snapshot.append(row)
                    records[self.path]=self.snapshot
                    creates.append(source.chat_id)
                    return SimpleNamespace(session_key=row["sessionKey"])
            session=ModuleType("gateway.session");session.SessionStore=Store
            config=SimpleNamespace(sessions_dir=original)
            def create(chat):
                return bridge.http_create_native_session(home,config,SimpleNamespace(chat_id=chat,user_id=chat))
            with patch.dict(sys.modules,{"gateway.session":session}),patch.object(bridge,"native_sessions",lambda _,scope:list(records[str(scope)])):
                with ThreadPoolExecutor(max_workers=3) as workers:
                    results=list(workers.map(create,["42","43","42"]))
            self.assertEqual(records[str(original)],[existing])
            self.assertEqual(sorted(creates),["42","43"])
            self.assertEqual({row["chatId"] for row in records[str(managed)]},{"42","43"})
            self.assertEqual(results[0],results[2])

    def test_scope_merge_accepts_only_same_session_or_verified_compression(self):
        with tempfile.TemporaryDirectory() as directory:
            home=Path(directory);(home/"state.db").touch()
            original=home/"sessions"
            root={"sessionKey":"native","nativeSessionId":"root","chatId":"42","senderId":"42","chatType":"private"}
            child={**root,"nativeSessionId":"tip"}
            def rows(_,scope):return [root] if scope==original else [child]
            with patch.object(bridge,"native_sessions",rows),patch.object(bridge,"http_compression_ids",lambda *args:["root","tip"]):
                self.assertEqual(bridge.http_channel_sessions(home,original),[root])
                child["nativeSessionId"]="reset"
                with self.assertRaisesRegex(ValueError,"hermes_session_mapping_ambiguous"):
                    bridge.http_channel_sessions(home,original)


class PairingPolicy(unittest.TestCase):
    def test_boolean_and_pairing_defaults_never_widen_native_authorization(self):
        platform=SimpleNamespace(extra={},channel_overrides={})
        key=SimpleNamespace(value="telegram")
        class PlatformKey:
            value="telegram"
        key=PlatformKey()
        config=SimpleNamespace(platforms={key:platform},unauthorized_dm_behavior="pair",
            get_unauthorized_dm_behavior=lambda _:"pair")
        with tempfile.TemporaryDirectory() as directory,patch.dict(bridge.os.environ,{"TELEGRAM_ALLOW_ALL_USERS":"on"},clear=True):
            policy,_=bridge.simple_policy(config,{},Path(directory))
            self.assertFalse(policy["allowAllUsers"])
            self.assertEqual(bridge.http_pairing_behavior(config,key,{}),"pair")
            bridge.os.environ["TELEGRAM_ALLOWED_USERS"]="42"
            self.assertEqual(bridge.http_pairing_behavior(config,key,{}),"ignore")
            self.assertEqual(bridge.http_pairing_behavior(config,key,{"unauthorized_dm_behavior":"pair"}),"pair")
            bridge.os.environ.clear()
            self.assertEqual(bridge.http_pairing_behavior(config,key,{"dm_policy":"allowlist"}),"ignore")
            self.assertEqual(bridge.http_pairing_behavior(config,key,{"dm_policy":"disabled"}),"ignore")
            config.unauthorized_dm_behavior="ignore"
            self.assertEqual(bridge.http_pairing_behavior(config,key,{"dm_policy":"pairing"}),"ignore")


class ProcessCredentialDiscovery(unittest.TestCase):
    def test_reads_only_verified_gateway_pid_and_exposes_only_api_keys(self):
        status=ModuleType("gateway.status")
        status.read_runtime_status=lambda path:{"pid":42,"start_time":7}
        status.runtime_status_pid_is_live=lambda state:True
        status.get_process_start_time=lambda pid:7
        status.looks_like_gateway_runtime_command_line=lambda command:command.strip()=="hermes gateway run"
        reads=[]
        files={"/proc/42/cmdline":b"hermes\0gateway\0run\0",
            "/proc/42/environ":b"HERMES_HOME=/fixture/home\0API_SERVER_KEY=fixture-secret-only\0API_SERVER_PORT=8642\0UNRELATED_SECRET=must-not-escape\0"}
        def read(path):
            reads.append(str(path))
            return files[str(path)]
        with patch.dict(sys.modules,{"gateway.status":status}),patch.object(Path,"read_bytes",read):
            result=bridge.http_process_api_environment(Path("/fixture/home"))
            self.assertEqual(result,{"API_SERVER_KEY":"fixture-secret-only","API_SERVER_PORT":"8642"})
            self.assertEqual(reads,["/proc/42/cmdline","/proc/42/environ"])
            with self.assertRaisesRegex(ValueError,"hermes_authority_changed"):
                bridge.http_process_api_environment(Path("/other/home"))
            status.runtime_status_pid_is_live=lambda state:False
            reads.clear()
            with self.assertRaisesRegex(ValueError,"hermes_status_unverified"):
                bridge.http_process_api_environment(Path("/fixture/home"))
            self.assertEqual(reads,[])

    def test_pid_reuse_after_read_is_rejected(self):
        status=ModuleType("gateway.status")
        status.read_runtime_status=lambda path:{"pid":42,"start_time":7}
        status.runtime_status_pid_is_live=lambda state:True
        values=iter([7,8])
        status.get_process_start_time=lambda pid:next(values)
        status.looks_like_gateway_runtime_command_line=lambda command:True
        def read(path):
            return b"hermes\0gateway\0run" if path.name=="cmdline" else b"HERMES_HOME=/fixture/home\0API_SERVER_KEY=fixture-secret-only"
        with patch.dict(sys.modules,{"gateway.status":status}),patch.object(Path,"read_bytes",read):
            with self.assertRaisesRegex(ValueError,"hermes_authority_changed"):
                bridge.http_process_api_environment(Path("/fixture/home"))


class NativeSessionIdentity(unittest.TestCase):
    def test_compressed_http_child_retains_telegram_peer_without_touching_routing(self):
        calls=[]
        rows={"root":{"id":"root","source":"telegram","session_key":"original-key","origin_json":"{}"},
              "tip":{"id":"tip","source":"api_server","session_key":"original-key"}}
        state=ModuleType("hermes_state")
        def db(path,read_only=False):
            def record(sid,**kwargs):
                self.assertFalse(read_only)
                calls.append((sid,kwargs))
                rows[sid]["source"]="telegram"
            return SimpleNamespace(get_session=lambda sid:rows.get(sid),get_compression_chain=lambda sid:["root","tip"],get_compression_tip=lambda sid:"tip",
                record_gateway_session_peer=record,close=lambda:None)
        state.SessionDB=db
        native={"nativeSessionId":"root","sessionKey":"original-key","chatId":"42","senderId":"42","nativeOriginChatType":"dm"}
        with patch.dict(sys.modules,{"hermes_state":state}):
            bridge.http_restore_native_peer(Path("/fixture"),native,"root")
            self.assertEqual(calls,[])
            bridge.http_restore_native_peer(Path("/fixture"),native,"tip")
            self.assertEqual(len(calls),1)
            self.assertEqual(calls[0][1]["source"],"telegram")
            self.assertEqual(calls[0][1]["session_key"],"original-key")
            self.assertEqual(calls[0][1]["chat_id"],"42")
            self.assertNotIn("include_compression_ancestors",calls[0][1])
            self.assertEqual(calls[0][1]["user_id"],"42")
            self.assertEqual(calls[0][1]["chat_type"],"dm")
            bridge.http_restore_native_peer(Path("/fixture"),native,"tip")
            self.assertEqual(len(calls),1)
            rows["tip"]["source"]="api_server"
            rows["tip"]["session_key"]="foreign-key"
            with self.assertRaisesRegex(ValueError,"hermes_session_identity_changed"):
                bridge.http_restore_native_peer(Path("/fixture"),native,"tip")
            self.assertEqual(len(calls),1)

    def test_read_only_official_compression_apis_filter_other_branches(self):
        closes=[]
        state=ModuleType("hermes_state")
        def db(path,read_only=False):
            self.assertTrue(read_only)
            return SimpleNamespace(get_session=lambda sid:{"id":sid},
                get_compression_tip=lambda sid:"other-tip" if sid=="branch" else "tip",
                get_compression_lineage=lambda sid:["root","branch","tip"],close=lambda:closes.append(True))
        state.SessionDB=db
        with patch.dict(sys.modules,{"hermes_state":state}):
            self.assertEqual(bridge.http_live_session(Path("/fixture"),"root"),"tip")
            self.assertEqual(bridge.http_compression_ids(Path("/fixture"),"root"),["root","tip"])
        self.assertEqual(len(closes),2)


class TerminalPeerRecovery(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.writes=[]
        self.rows={"root":{"id":"root","source":"telegram","session_key":"original","chat_id":"42","user_id":"42","chat_type":"dm"},
                   "compressed":{"id":"compressed","source":"api_server","session_key":"original"}}
        state=ModuleType("hermes_state")
        def db(path,read_only=False):
            def record(sid,**identity):
                self.assertFalse(read_only)
                self.writes.append((sid,identity))
                self.rows[sid].update(identity)
            return SimpleNamespace(get_session=lambda sid:self.rows.get(sid),
                get_compression_chain=lambda sid:["root","compressed"],get_compression_tip=lambda sid:"compressed",
                record_gateway_session_peer=record,close=lambda:None)
        state.SessionDB=db
        self.stack=ExitStack()
        self.stack.enter_context(patch.dict(sys.modules,{"hermes_state":state}))
        self.wire=io.StringIO();self.stack.enter_context(patch.object(bridge,"WIRE",self.wire))
        native={"nativeSessionId":"root","sessionKey":"original","chatId":"42","senderId":"42","nativeOriginChatType":"dm"}
        def recover():
            current=bridge.http_live_session(Path("/fixture"),"root")
            bridge.http_restore_native_peer(Path("/fixture"),native,current)
        self.events=bridge.HttpSessionEvents("root",terminal_session=recover)
        self.events.accept("run.started",{"run_id":"run_fixture","session_id":"root","seq":1})

    async def asyncTearDown(self):
        self.stack.close()

    def assert_recovered(self):
        self.assertEqual(len(self.writes),1)
        sid,identity=self.writes[0]
        self.assertEqual(sid,"compressed")
        self.assertEqual(identity["source"],"telegram")
        self.assertEqual(identity["session_key"],"original")
        self.assertEqual(identity["chat_id"],"42")
        self.assertEqual(identity["user_id"],"42")
        self.assertNotIn("include_compression_ancestors",identity)

    async def test_sse_error_after_compression_repairs_only_current_tip_before_failure(self):
        self.assertTrue(self.events.accept("error",{"run_id":"run_fixture","session_id":"root","seq":2,"message":"private failure details"}))
        self.assert_recovered()
        self.assertEqual(json.loads(self.wire.getvalue().splitlines()[-1])["event"],"run.failed")
        self.assertNotIn("private failure",self.wire.getvalue())

    async def test_observed_failed_and_cancelled_runs_repair_compressed_peer(self):
        for status,outcome in [("failed","run.failed"),("cancelled","run.cancelled"),("interrupted","run.cancelled")]:
            with self.subTest(status=status):
                self.rows["compressed"]["source"]="api_server";self.writes.clear()
                async def http(*args,**kwargs):return {"run_id":"run_fixture","status":status,"session_id":"root"}
                with patch.object(bridge,"http_json",http):
                    await bridge.http_observe_after_disconnect(None,"http://fixture",self.events)
                self.assert_recovered()
                self.assertEqual(json.loads(self.wire.getvalue().splitlines()[-1])["event"],outcome)

    async def test_confirmed_completion_without_final_text_repairs_peer_but_not_result(self):
        async def http(*args,**kwargs):return {"run_id":"run_fixture","status":"completed","session_id":"compressed"}
        with patch.object(bridge,"http_json",http),self.assertRaisesRegex(ValueError,"hermes_final_snapshot_missing"):
            await bridge.http_observe_after_disconnect(None,"http://fixture",self.events)
        self.assert_recovered()
        self.assertNotIn('"event": "run.completed"',self.wire.getvalue())

    async def test_unknown_or_foreign_run_does_not_repair_peer(self):
        for run_id,status,error in [("run_fixture","unknown","hermes_execution_unknown"),("other","failed","hermes_session_identity_changed")]:
            with self.subTest(run_id=run_id,status=status):
                async def http(*args,**kwargs):return {"run_id":run_id,"status":status}
                with patch.object(bridge,"http_json",http),self.assertRaisesRegex(ValueError,error):
                    await bridge.http_observe_after_disconnect(None,"http://fixture",self.events)
                self.assertEqual(self.writes,[])
                self.assertEqual(self.rows["compressed"]["source"],"api_server")


try:
    from aiohttp import web
except ImportError:
    web = None


@unittest.skipIf(web is None, "install aiohttp to run local HTTP contract tests")
class HttpTransport(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.posts = []
        self.stops = 0
        self.stop_conflicts = 0
        self.mode = "complete"
        self.started = asyncio.Event()
        self.stop_event = asyncio.Event()
        async def session(request):
            return web.json_response({"session":{"id":request.match_info["id"]}})
        async def stream(request):
            self.posts.append((request.match_info["id"],dict(request.headers),await request.json()))
            response = web.StreamResponse(headers={"Content-Type":"text/event-stream","X-Hermes-Session-Id":"tip"})
            await response.prepare(request)
            async def event(name, seq, **data):
                payload = {"run_id":"run_fixture","session_id":"tip","seq":seq,**data}
                await response.write(("event: " + name + "\ndata: " + json.dumps(payload) + "\n\n").encode())
            await event("run.started",1)
            self.started.set()
            await event("tool.progress",2,tool_name="_thinking",delta="private reasoning")
            await event("assistant.delta",3,delta="Hello")
            if self.mode == "cancel":
                await asyncio.wait_for(self.stop_event.wait(),2)
            if self.mode != "eof":
                await event("assistant.completed",4,content="Hello world")
                await event("run.completed",5)
            await response.write_eof()
            return response
        async def stop(request):
            self.stops += 1
            if self.stops <= self.stop_conflicts:
                return web.json_response({"error":{"code":"run_not_active"}},status=409)
            self.stop_event.set()
            return web.json_response({"status":"stopping"})
        async def status(request):
            return web.json_response({"run_id":"run_fixture","status":"completed","session_id":"tip"})
        app=web.Application()
        app.router.add_get("/api/sessions/{id}",session)
        app.router.add_post("/api/sessions/{id}/chat/stream",stream)
        app.router.add_post("/v1/runs/{id}/stop",stop)
        app.router.add_get("/v1/runs/{id}",status)
        self.runner=web.AppRunner(app);await self.runner.setup()
        self.site=web.TCPSite(self.runner,"127.0.0.1",0);await self.site.start()
        self.base="http://127.0.0.1:" + str(self.site._server.sockets[0].getsockname()[1])
        self.stack=ExitStack();self.wire=io.StringIO()
        self.stack.enter_context(patch.object(bridge,"WIRE",self.wire))
        self.stack.enter_context(patch.object(bridge,"http_context",lambda _:(Path("/fixture"),None,None,self.base,"fixture-api-key-only",None,{},[])))
        async def inspect(_):return {"ready":True,"sessions":[{"sessionKey":"original-key","nativeSessionId":"original-id"}]}
        self.stack.enter_context(patch.object(bridge,"http_inspect",inspect))
        self.stack.enter_context(patch.object(bridge,"http_live_session",lambda home,sid:"tip"))
        self.stack.enter_context(patch.object(bridge,"http_restore_native_peer",lambda *args:None))
        self.stack.enter_context(patch.object(bridge,"http_compression_ids",lambda home,sid:["original-id","tip"]))
        self.commands=0
        async def command():
            self.commands+=1
            if self.commands == 1:return {"action":"submit"}
            if self.mode == "cancel":
                await self.started.wait()
                return {"action":"cancel"}
            await asyncio.sleep(60)
        self.stack.enter_context(patch.object(bridge,"command",command))
        self.request={"transport":"hermes_http","nativeDeliveryDisabled":True,"sessionKey":"original-key","sessionId":"original-id","message":"hello","model":"beta","provider":"provider"}

    async def asyncTearDown(self):
        self.stack.close()
        await self.runner.cleanup()

    async def test_native_lineage_model_lock_and_incremental_output(self):
        await bridge.http_run(self.request)
        self.assertEqual(len(self.posts),1)
        sid, headers, body=self.posts[0]
        self.assertEqual(sid,"tip")
        self.assertEqual(headers["X-Hermes-Session-Key"],"original-key")
        self.assertEqual(body,{"message":"hello","model":"beta","provider":"provider","require_model_lock":True})
        events=[json.loads(line) for line in self.wire.getvalue().splitlines()]
        self.assertEqual([e["event"] for e in events],["ready","run.accepted","message.delta","run.completed"])
        self.assertNotIn("private reasoning", self.wire.getvalue())
        self.assertEqual(events[-1]["output"],"Hello world")

    async def test_incomplete_model_pair_keeps_native_configuration(self):
        self.request.pop("provider")
        await bridge.http_run(self.request)
        self.assertEqual(self.posts[0][2], {"message":"hello"})

    async def test_reset_session_is_not_accepted_as_original_binding(self):
        self.request["sessionId"]="reset-session"
        with self.assertRaisesRegex(ValueError,"hermes_session_identity_changed"):
            await bridge.http_run(self.request)
        self.assertEqual(self.posts,[])

    async def test_cancel_uses_actual_run_once_and_terminal_is_cancelled(self):
        self.mode="cancel"
        await bridge.http_run(self.request)
        self.assertEqual(self.stops,1)
        self.assertEqual(len(self.posts),1)
        self.assertEqual(json.loads(self.wire.getvalue().splitlines()[-1])["event"],"run.cancelled")

    async def test_stop_retries_only_known_agent_registration_window(self):
        self.mode="cancel"
        self.stop_conflicts=1
        await bridge.http_run(self.request)
        self.assertEqual(self.stops,2)
        self.assertEqual(len(self.posts),1)
        self.assertEqual(json.loads(self.wire.getvalue().splitlines()[-1])["event"],"run.cancelled")

    async def test_eof_does_not_retry_or_treat_partial_text_as_final(self):
        self.mode="eof"
        with self.assertRaisesRegex(ValueError,"hermes_final_snapshot_missing"):
            await bridge.http_run(self.request)
        self.assertEqual(len(self.posts),1)
        self.assertNotIn('"event": "run.completed"',self.wire.getvalue())

if __name__ == "__main__":unittest.main()
