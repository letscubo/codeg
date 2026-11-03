"""No DB/model/network: real audited source functions + owned production shim.

Fixtures are AST-isolated official definitions with source/SHA/license records.
Only unrelated provider/repair services are fakes. Never import Hermes or open
its default state.db, even in memory. Run with Python's standard unittest.
"""
import asyncio
import contextlib
import contextvars
import dataclasses
import hashlib
import importlib.util
import json
import logging
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import types
import typing
import unittest

ROOT = pathlib.Path(__file__).parent
spec = importlib.util.spec_from_file_location("codeg_native_steering", ROOT / "codeg_native_steering.py")
native = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = native
spec.loader.exec_module(native)
FIXTURES = json.loads((ROOT / "native-test-fixtures.json").read_text())


def official_definitions():
    module = types.ModuleType("codeg_hermes_official_fixture")
    sys.modules[module.__name__] = module
    ns = module.__dict__
    ns.update(Any=typing.Any, Dict=typing.Dict, List=typing.List, Optional=typing.Optional,
              dataclass=dataclasses.dataclass, field=dataclasses.field, threading=threading,
              contextlib=contextlib, suppress=contextlib.suppress, logger=logging.getLogger("fixture"))
    ns.update(FIXTURES["constants"])
    for entry in FIXTURES["entries"]:
        assert hashlib.sha256(entry["snippet"].encode()).hexdigest() == entry["snippetSha256"]
        manifest = native.SDK_SOURCES if entry["path"].startswith("acp/") else native.SOURCES
        path = entry["path"][4:] if entry["path"].startswith("acp/") else entry["path"]
        assert manifest[path] == entry["sourceSha256"], entry["path"]
        exec(compile("from __future__ import annotations\n" + entry["snippet"], entry["path"], "exec"), ns)
    ns["_ra"] = lambda: types.SimpleNamespace(logger=ns["logger"])
    ns["_maybe_inject_iteration_budget_warning"] = lambda *_: False
    return ns


@contextlib.contextmanager
def runtime():
    ns = official_definitions()
    old = {}
    modules = {name: types.ModuleType(name) for name in ["agent", "agent.prompt_builder", "agent.conversation_loop", "agent.agent_runtime_helpers"]}
    modules["agent"].__path__ = []
    for name, module in modules.items():
        old[name] = sys.modules.get(name)
        sys.modules[name] = module
    for name in ["format_steer_marker", "steer_user_row"]:
        setattr(modules["agent.prompt_builder"], name, ns[name])
    modules["agent.conversation_loop"]._INTERRUPT_SCAFFOLD_MARKER = "fixture-hidden-scaffold"
    modules["agent.conversation_loop"]._maybe_inject_run_budget_wrapup = lambda *_: None
    helper = modules["agent.agent_runtime_helpers"]
    helper.repair_message_sequence_with_cursor = lambda *_: 0
    helper._requeue_pending_steer = ns["_requeue_pending_steer"]
    helper.apply_pending_steer_to_tool_results = ns["apply_pending_steer_to_tool_results"]
    prep = types.SimpleNamespace(prepare_iteration=ns["prepare_iteration"])
    agent = types.SimpleNamespace(_current_turn_id="native-A", _interrupt_requested=False,
        _pending_steer=None, _pending_steer_lock=threading.Lock(), step_callback=None,
        _skill_nudge_interval=0, valid_tool_names=set(), run_budget_seconds=None,
        _adopt_nous_key_before_expiry=lambda: None, _sanitize_tool_call_arguments=lambda *a, **k: 0,
        session_id="native-session")
    agent._drain_pending_steer = types.MethodType(ns["_drain_pending_steer"], agent)
    state = ns["SessionState"](session_id="acp-session", agent=agent, is_running=True, cancel_event=threading.Event())
    states = []
    turn = native.Turn(state, "host-A", states.append)
    setattr(agent, native.ATTR, turn)
    setattr(state, native.ATTR, turn)
    native.patch_boundary(helper, "tools")
    native.patch_boundary(prep, "prepare")
    try:
        yield types.SimpleNamespace(ns=ns, agent=agent, state=state, turn=turn, states=states, helper=helper, prep=prep)
    finally:
        turn.close()
        for name, original in old.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


def request(identifier="b", **updates):
    value = dict(version=1, sessionId="acp-session", hostTurnId="host-A",
                 expectedNativeTurnId="native-A", submissionId=identifier,
                 prompt=[dict(type="text", text="B 原生调整")])
    value.update(updates)
    return value


def messages():
    return [{"role": "user", "content": "A"}, {"role": "tool", "content": "tool completed"}]


class NativeTests(unittest.TestCase):
    def test_official_claim_and_same_agent_are_preserved(self):
        with runtime() as h:
            h.state.is_running = False
            claimed = h.ns["_claim_turn_or_queue"](None, h.state, "acp-session", "A", "A", True)
            self.assertIsNone(claimed)
            self.assertTrue(h.state.is_running)
            self.assertIs(h.state.agent, h.agent)
            self.assertEqual(h.state.queued_prompts, [])

    def test_initial_preparation_opens_only_actual_native_generation(self):
        with runtime() as h:
            self.assertEqual(h.turn.submit(request()).result()["outcome"], "not_injected")
            result = h.prep.prepare_iteration(h.agent, messages=[dict(role="user", content="A")], api_call_count=0, current_turn_user_idx=0)
            self.assertEqual(result.action, "fallthrough")
            self.assertEqual(h.states[-1]["nativeTurnId"], "native-A")
            self.assertTrue(h.states[-1]["ready"])
            self.assertIs(h.state.agent, h.agent)

    def test_real_native_tool_helper_consumes_distinct_b_rows(self):
        with runtime() as h:
            h.turn.bind_native()
            one, two = h.turn.submit(request("one")), h.turn.submit(request("two"))
            self.assertFalse(one.done())
            rows = messages()
            h.helper.apply_pending_steer_to_tool_results(h.agent, rows, 1)
            self.assertEqual([one.result()["outcome"], two.result()["outcome"]], ["injected", "injected"])
            self.assertEqual(len(rows), 4)
            self.assertEqual(rows[2]["display_kind"], "steer")
            self.assertIn("B 原生调整", rows[3]["content"])
            self.assertEqual(h.agent._pending_steer, None)
            self.assertEqual(h.state.queued_prompts, [])

    def test_real_prepare_hook_consumes_only_after_current_a_tool(self):
        with runtime() as h:
            h.turn.bind_native()
            outcome = h.turn.submit(request())
            rows = [dict(role="user", content="old"), dict(role="tool", content="old tool"), dict(role="user", content="A")]
            prepared = h.prep.prepare_iteration(h.agent, messages=rows, api_call_count=1, current_turn_user_idx=2)
            self.assertEqual(len(prepared.messages), 3)
            self.assertFalse(outcome.done(), "historical tool rows cannot consume current B")
            rows.append(dict(role="tool", content="A tool"))
            prepared = h.prep.prepare_iteration(h.agent, messages=rows, api_call_count=2, current_turn_user_idx=2)
            self.assertEqual(outcome.result()["outcome"], "injected")
            self.assertEqual(prepared.messages[-1]["display_kind"], "steer")

    def test_legacy_steer_and_v1_remain_separate_and_both_reach_tool_boundary(self):
        with runtime() as h:
            h.turn.bind_native()
            outcome = h.turn.submit(request())
            h.agent._pending_steer = "legacy"
            rows = messages()
            h.helper.apply_pending_steer_to_tool_results(h.agent, rows, 1)
            self.assertEqual(outcome.result()["outcome"], "injected")
            self.assertIn("legacy", rows[-2]["content"])
            self.assertIn("B 原生调整", rows[-1]["content"])

    def test_finalizer_withdraws_before_legacy_next_turn_fallback(self):
        with runtime() as h:
            h.turn.bind_native()
            outcome = h.turn.submit(request())
            finalizer = types.SimpleNamespace(finalize_turn=lambda agent: {"pending_steer": agent._drain_pending_steer(), "finish": "A"})
            native.patch_boundary(finalizer, "finalizer")
            result = finalizer.finalize_turn(h.agent)
            self.assertIsNone(result["pending_steer"])
            self.assertEqual(result["finish"], "A")
            self.assertEqual(outcome.result()["outcome"], "not_injected")
            self.assertFalse(h.states[-1]["ready"])

    def test_generation_change_never_delivers_old_b(self):
        with runtime() as h:
            h.turn.bind_native()
            old = h.turn.submit(request())
            h.agent._current_turn_id = "native-replacement"
            h.turn.bind_native()
            self.assertEqual(old.result()["outcome"], "not_injected")
            self.assertEqual(h.turn.submit(request("stale")).result()["outcome"], "not_injected")
            self.assertEqual(h.states[-1]["nativeTurnId"], "native-replacement")
            self.assertEqual([s["ready"] for s in h.states], [True, False, True])

    def test_duplicate_conflict_scope_attachments_and_cancel_fail_closed(self):
        with runtime() as h:
            h.turn.bind_native()
            original = h.turn.submit(request())
            self.assertIs(h.turn.submit(request()), original)
            self.assertEqual(h.turn.submit(request(prompt=[dict(type="text", text="changed")])).result()["outcome"], "unconfirmed")
            for changes in [dict(sessionId="other"), dict(hostTurnId="other"), dict(expectedNativeTurnId="other"), dict(prompt=[dict(type="image", data="abc")])]:
                self.assertEqual(h.turn.submit(request("rejected", **changes)).result()["outcome"], "not_injected")
            h.state.cancel_event.set()
            rows = messages()
            h.helper.apply_pending_steer_to_tool_results(h.agent, rows, 1)
            h.turn.close()
            self.assertEqual(len(rows), 2)
            self.assertEqual(original.result()["outcome"], "not_injected")

    def test_consume_close_race_is_atomic_with_real_session_state_lock(self):
        for _ in range(50):
            with runtime() as h:
                h.turn.bind_native()
                result = h.turn.submit(request())
                rows = messages()
                barrier = threading.Barrier(3)
                def consume():
                    barrier.wait()
                    h.turn.consume(rows, h.ns["steer_user_row"])
                def close():
                    barrier.wait()
                    h.turn.close()
                workers = [threading.Thread(target=consume), threading.Thread(target=close)]
                for worker in workers: worker.start()
                barrier.wait()
                for worker in workers: worker.join()
                self.assertEqual(result.result()["outcome"], "injected" if len(rows) == 3 else "not_injected")
                self.assertEqual(h.turn.pending, [])

    def test_provider_error_and_no_tool_boundary_cannot_consume_b(self):
        with runtime() as h:
            h.turn.bind_native()
            result = h.turn.submit(request())
            rows = [dict(role="user", content="A")]
            h.helper.apply_pending_steer_to_tool_results(h.agent, rows, 0)
            finalizer = types.SimpleNamespace(finalize_turn=lambda agent: (_ for _ in ()).throw(RuntimeError("provider error")))
            native.patch_boundary(finalizer, "finalizer")
            with self.assertRaisesRegex(RuntimeError, "provider error"): finalizer.finalize_turn(h.agent)
            self.assertEqual(result.result()["outcome"], "not_injected")

    def test_unknown_source_is_read_only_and_rejected_before_target_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp)
            (path / "source.py").write_text("raise AssertionError('must not execute')")
            self.assertFalse(native.source_matches(path, {"source.py": "0" * 64}))
            self.assertEqual(list(path.iterdir()), [path / "source.py"])

    def test_sitecustomize_chains_two_owned_caches_and_original_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = pathlib.Path(tmp)
            for name in ["new", "old", "user"]: (base / name).mkdir()
            for name in ["new", "old"]:
                (base / name / "sitecustomize.py").write_text((ROOT / "sitecustomize.py").read_text())
                (base / name / "codeg_native_steering.py").write_text("def install():\n import sys\n sys.native_installs=getattr(sys,'native_installs',0)+1\n")
            (base / "user" / "sitecustomize.py").write_text("import sys\nsys.user_site_runs=getattr(sys,'user_site_runs',0)+1\n")
            environment = {"PATH": os.defpath, "PYTHONPATH": os.pathsep.join(str(base / name) for name in ["new", "old", "user"]), "CODEG_HERMES_NATIVE_BOOTSTRAP": "1"}
            code = "import sys,os,json;print(json.dumps([sys.user_site_runs,getattr(sys,'native_installs',0),os.getenv('CODEG_HERMES_NATIVE_BOOTSTRAP')]))"
            out = subprocess.run([sys.executable, "-c", code, "acp"], env=environment, cwd=base, capture_output=True, text=True, check=True)
            self.assertEqual(json.loads(out.stdout), [1, 1, None])
            out = subprocess.run([sys.executable, "-c", code], env=environment, cwd=base, capture_output=True, text=True, check=True)
            self.assertEqual(json.loads(out.stdout), [1, 0, None])

    def test_official_sdk_ext_notification_and_prompt_meta_contract(self):
        ns = official_definitions()
        sent = []
        async def send_notification(method, params): sent.append((method, params))
        conn = types.SimpleNamespace(_conn=types.SimpleNamespace(send_notification=send_notification))
        asyncio.run(ns["ext_notification"](conn, native.STATE_METHOD, {"ready": True}))
        self.assertEqual(sent, [("_ai.myclaw/nativeSteeringState", {"ready": True})])
        observed = []
        async def prompt(**params): observed.append(params)
        class Model:
            model_fields = ["session_id", "prompt", "field_meta"]
            @staticmethod
            def model_validate(_):
                return types.SimpleNamespace(session_id="acp-session", prompt=[], field_meta={native.TURN_META: {"version": 1, "hostTurnId": "A"}})
        ns["_resolve_handler"] = lambda obj, attr: (getattr(obj, attr), attr, False)
        route = ns["_make_func"](None, Model, types.SimpleNamespace(prompt=prompt), "prompt")
        asyncio.run(route({}))
        self.assertEqual(observed[0][native.TURN_META]["hostTurnId"], "A")


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, *, cancel=False, detach=False, failure=False):
        with runtime() as h:
            h.state.is_running = False
            entered, release = threading.Event(), threading.Event()
            emitted = []

            class Connection:
                async def ext_notification(self, name, params): emitted.append((name, params))

            class Server:
                def __init__(self):
                    self.session_manager = types.SimpleNamespace(get_session=lambda sid: h.state if sid == h.state.session_id else None)
                    self._conn = Connection()

                async def initialize(self): return types.SimpleNamespace(field_meta={})

                async def prompt(self, prompt, session_id, **kwargs):
                    # Official ACP claim, copy_context and same worker invocation;
                    # fake persistence/provider only. No SessionManager/DB import.
                    self.outer_scope = native._HOST.get()
                    self.claimed = h.ns["_claim_turn_or_queue"](self, h.state, session_id, "A", "A", True)
                    ctx = contextvars.copy_context()
                    loop = asyncio.get_running_loop()
                    try:
                        return await loop.run_in_executor(None, lambda: ctx.run(self._run_agent_turn,
                            state=h.state, session_id=session_id, loop=loop, conn=self._conn))
                    finally:
                        h.state.is_running = False

                def _run_agent_turn(self, *, state, session_id, loop, conn):
                    self.worker_agent = state.agent
                    prepared = h.prep.prepare_iteration(state.agent, messages=[dict(role="user", content="A")], api_call_count=0, current_turn_user_idx=0)
                    entered.set()
                    if not release.wait(3): raise RuntimeError("fixture timed out")
                    if failure: raise RuntimeError("provider failed")
                    prepared.messages.append(dict(role="tool", content="A tool"))
                    h.helper.apply_pending_steer_to_tool_results(state.agent, prepared.messages, 1)
                    return dict(messages=prepared.messages, finish="original A terminal", usage={"input": 7})

                async def cancel(self, session_id, **kwargs):
                    h.state.cancel_event.set()
                    release.set()

            native.patch_server(types.SimpleNamespace(HermesACPAgent=Server))
            server = Server()
            initialized = await server.initialize()
            self.assertEqual(initialized.field_meta[native.META], {"version": 1, "activeOnly": True, "receipts": "consumed"})
            task = asyncio.create_task(server.prompt([], h.state.session_id, **{native.TURN_META: {"version": 1, "hostTurnId": "host-A"}}))
            async def wait_ready():
                while not entered.is_set(): await asyncio.sleep(.001)
                while not emitted: await asyncio.sleep(.001)
            await asyncio.wait_for(wait_ready(), 3)
            self.assertEqual(server.outer_scope, (server, h.state.session_id, "host-A"))
            self.assertIs(server.worker_agent, h.agent)
            response = asyncio.create_task(server.ext_method(native.STEER_METHOD, request()))
            await asyncio.sleep(.005)
            current = getattr(h.state, native.ATTR)
            self.assertEqual(len(current.pending), 1)
            self.assertFalse(response.done(), "admission is not a consumption ACK")
            rejected = await server.ext_method(native.STEER_METHOD, request("wrong", sessionId="other"))
            self.assertEqual(rejected["outcome"], "not_injected")
            if detach:
                response.cancel()
                with self.assertRaises(asyncio.CancelledError): await response
                self.assertEqual(len(current.pending), 1, "request loss must not retract admitted B")
                response = asyncio.create_task(server.ext_method(native.STEER_METHOD, request()))
            if cancel: await server.cancel(h.state.session_id)
            else: release.set()
            result = await response
            self.assertEqual(result["outcome"], "not_injected" if cancel or failure else "injected")
            self.assertEqual(result["nativeTurnId"], "native-A")
            if failure:
                with self.assertRaisesRegex(RuntimeError, "provider failed"): await task
            else:
                final = await task
                self.assertEqual(final["finish"], "original A terminal")
                self.assertEqual(final["usage"], {"input": 7})
                self.assertEqual(len(final["messages"]), 2 if cancel else 3)
            await asyncio.sleep(.005)
            self.assertEqual(emitted[0][0], "ai.myclaw/nativeSteeringState")
            self.assertEqual(emitted[0][1]["sessionId"], h.state.session_id)
            self.assertFalse(emitted[-1][1]["ready"])
            self.assertEqual(h.state.queued_prompts, [])

    async def test_production_server_wrapper_tracks_original_executor_and_finish(self):
        await self.exercise()

    async def test_caller_loss_preserves_same_native_submission(self):
        await self.exercise(detach=True)

    async def test_cancel_and_provider_error_withdraw_only_unconsumed_v1(self):
        await self.exercise(cancel=True)
        await self.exercise(failure=True)


if __name__ == "__main__":
    unittest.main()
