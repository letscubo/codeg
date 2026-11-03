"""Strict, same-process Hermes ACP steering for official v2026.9.14 only.

No runtime imports at module load, no DB/model/network operations. Original
source is checked BEFORE executing a target module. The owned queue uses the
same native tool/pre-API boundaries and steer_user_row as Hermes's own steer;
it never enters the legacy string slot or its next-turn fallback.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import functools
import hashlib
import importlib.abc
import importlib.machinery
import pathlib
import sys
import threading
from dataclasses import dataclass, field

META = "ai.myclaw/nativeSteering"
TURN_META = "ai.myclaw/nativeTurn"
STATE_METHOD = "ai.myclaw/nativeSteeringState"  # SDK adds the leading underscore.
STEER_METHOD = "ai.myclaw/steer"  # SDK removes the leading underscore on dispatch.
ATTR = "_codeg_native_steering_v1"
_HOST = contextvars.ContextVar("codeg_hermes_native_host", default=None)

# git tag v2026.9.14, peeled commit 345cd2b057a452236de401d3534b8502a7465e8d.
SOURCES = {
    "hermes_cli/__init__.py": "0d78a58a9f27f32adfdac959e89767cecde93a424bcfbd39d64fb95f6cf13e6c",
    "acp_adapter/server.py": "6034d98773ebc05a172108ba886f0413a13a189c2e922c3996c39029275bf076",
    "acp_adapter/session.py": "73740c90cfdc603b4c839398e62592c4f47b02d42fc6bbe1f45148a626ec78b2",
    "acp_adapter/entry.py": "70218ec9e9f5d7590fa927b86fdcda2cf33c45457247e291382e409995e8ac54",
    "agent/agent_runtime_helpers.py": "d9e0f31bc5f192e48f4c84c91a4e375e77d04a6304bb660978fac9ca51d6207e",
    "agent/turn_iteration_prep.py": "75b6495c51ce75e9e2d073212e0eef7899af31d8b5bcaebcbf756e851344294f",
    "agent/turn_finalizer.py": "3e9658908b7421d3ee2da6516f2ac511541f7e134fcacaace8c807c996747fb1",
    "agent/prompt_builder.py": "586ea363fa1e70bb0fdd5426af40758976a16c54f07efeb7a1b4f3fe0ad99309",
    "agent/turn_context.py": "46ed030ee35a13f75994bf8e28cde3633db14969d1fb5c859fc0ff6964cbb0bd",
    "agent/tool_executor.py": "9634ebdc01b12591753fa9a1653f4aa8937edde7130175bbd69be8a0e5895ab7",
    "agent/interrupt_control.py": "b5eee15d9f8105afe55c67f918d2f80342cf37acab8683d52afd81beeb23e9de",
    "agent/conversation_loop.py": "2086d4d084a7cba728a5862f2f27d172860908af967e2b83a43d22fe757548c2",
    "agent/turn_facade.py": "10df0ee532dc0aca989ba4893dfa630fba521166586d72c6703fc691e9e83d55",
    "run_agent.py": "dc125031d13e2a341eec473bcfcafce16c7356505dcbc560bec562b1ecaddc04",
}
# agent-client-protocol==0.9.0, official wheel SHA256
# 06911500b51d8cb69112544e2be01fc5e7db39ef88fecbc3848c5c6f194798ee.
SDK_SOURCES = {
    "router.py": "9cf0700c009a6bd6918317a238558859e2f2c190198b36c66142827a6245c16a",
    "agent/router.py": "4f1fbc1c702884acae6227f678f5bd3d778fcb0ef3926defcf8ca593d3a34c12",
    "agent/connection.py": "42a1af6523074d088224192e23ffed65145884fbc3f996803322c002d3075035",
}


def source_matches(root, manifest):
    try:
        return all(hashlib.sha256((pathlib.Path(root) / name).read_bytes()).hexdigest() == digest
                   for name, digest in manifest.items())
    except (OSError, ValueError):
        return False


@dataclass
class Submission:
    submission_id: str
    text: str
    native: str
    future: concurrent.futures.Future = field(default_factory=concurrent.futures.Future)


class Turn:
    """One exact ACP worker invocation; all custody transitions share this lock."""
    def __init__(self, state, host, notify):
        self.state, self.agent, self.host = state, state.agent, host
        self.notify = notify
        self.lock = threading.RLock()
        self.native = None
        self.open = True
        self.accepting = False
        self.items = {}
        self.pending = []

    def _state(self, ready):
        self.accepting = ready
        self.notify({"version": 1, "sessionId": self.state.session_id,
                     "hostTurnId": self.host, "nativeTurnId": self.native or "",
                     "ready": ready})

    def _result(self, item, outcome):
        if not item.future.done():
            item.future.set_result({"version": 1, "submissionId": item.submission_id,
                                    "hostTurnId": self.host, "nativeTurnId": item.native,
                                    "outcome": outcome})

    def _live(self):
        cancel = getattr(self.state, "cancel_event", None)
        return (self.open and self.state.agent is self.agent and self.state.is_running
                and not (cancel and cancel.is_set())
                and getattr(self.agent, "_interrupt_requested", False) is not True)

    def bind_native(self):
        with self.lock:
            native = getattr(self.agent, "_current_turn_id", None)
            if not self._live() or not isinstance(native, str) or not native:
                return
            if native != self.native:
                if self.native is not None:
                    self._state(False)
                    self._withdraw()
                self.native = native
            if not self.accepting:
                self._state(True)

    def submit(self, params):
        identity = params.get("submissionId")
        prompt = params.get("prompt")
        text = "\n".join(block["text"] for block in prompt) if (
            isinstance(prompt, list) and prompt and all(isinstance(block, dict)
            and block.get("type") == "text" and isinstance(block.get("text"), str)
            for block in prompt)) else None
        with self.lock:
            # A duplicate is resolved before liveness, but scope/content must match.
            previous = self.items.get(identity) if isinstance(identity, str) else None
            scoped = (params.get("version") == 1 and params.get("sessionId") == self.state.session_id
                      and params.get("hostTurnId") == self.host)
            if previous is not None:
                if scoped and text == previous.text and params.get("expectedNativeTurnId") == previous.native:
                    return previous.future
                return self._immediate(params, "unconfirmed")
            if (not scoped or not isinstance(identity, str) or not 1 <= len(identity) <= 128
                    or text is None or not text.strip() or len(text) > 16000
                    or not self._live() or not self.accepting or len(self.items) >= 8192
                    or params.get("expectedNativeTurnId") != self.native
                    or getattr(self.agent, "_current_turn_id", None) != self.native):
                return self._immediate(params, "not_injected")
            item = Submission(identity, text, self.native)
            self.items[identity] = item
            self.pending.append(item)
            return item.future

    def _immediate(self, params, outcome):
        future = concurrent.futures.Future()
        future.set_result({"version": 1, "submissionId": params.get("submissionId", ""),
                           "hostTurnId": params.get("hostTurnId", ""),
                           "nativeTurnId": params.get("expectedNativeTurnId", ""), "outcome": outcome})
        return future

    def consume(self, messages, row_factory, *, after=None):
        """Append on Hermes's execution thread at an audited model/tool boundary."""
        with self.lock:
            if (not self._live() or not self.accepting or type(messages) is not list
                    or getattr(self.agent, "_current_turn_id", None) != self.native):
                return
            # A prior turn's tool history must not admit B ahead of the current A.
            if after is not None and (type(after) is not int or after < 0 or after >= len(messages)):
                return
            start = after if after is not None else 0
            if not any(isinstance(row, dict) and row.get("role") == "tool" for row in messages[start:]):
                return
            while self.pending:
                item = self.pending.pop(0)
                try:
                    messages.append(row_factory(item.text))
                except BaseException:
                    # An unexpected custom formatter/append failure is ambiguous.
                    self._result(item, "unconfirmed")
                    raise
                self._result(item, "injected")

    def _withdraw(self):
        for item in self.pending:
            self._result(item, "not_injected")
        self.pending.clear()

    def close(self):
        with self.lock:
            if self.open:
                self.open = False
                self._state(False)
                self._withdraw()


def _controller(agent):
    turn = getattr(agent, ATTR, None)
    return turn if isinstance(turn, Turn) else None


def patch_boundary(module, kind):
    if getattr(module, ATTR, False):
        return
    setattr(module, ATTR, True)
    if kind == "finalizer":
        original = module.finalize_turn

        @functools.wraps(original)
        def finalize(agent, *args, **kwargs):
            turn = _controller(agent)
            if turn is not None:
                turn.close()  # BEFORE native leftover steer can become a next-turn input.
            return original(agent, *args, **kwargs)
        module.finalize_turn = finalize
        return
    name = "prepare_iteration" if kind == "prepare" else "apply_pending_steer_to_tool_results"
    original = getattr(module, name)

    @functools.wraps(original)
    def boundary(agent, *args, **kwargs):
        messages = args[0] if args else kwargs.get("messages")
        count = args[1] if len(args) > 1 else kwargs.get("num_tool_msgs", 0)
        eligible = kind == "tools" and type(count) is int and count > 0 and type(messages) is list and any(
            isinstance(row, dict) and row.get("role") == "tool" for row in messages[max(0, len(messages) - count):])
        result = original(agent, *args, **kwargs)
        turn = _controller(agent)
        if turn is None:
            return result
        from agent.prompt_builder import steer_user_row
        turn.bind_native()
        if kind == "prepare":
            if getattr(result, "action", None) == "fallthrough":
                turn.consume(result.messages, steer_user_row, after=result.current_turn_user_idx)
        elif eligible:
            # Measure before the legacy helper adds its own steer user row.
            turn.consume(messages, steer_user_row)
        return result
    setattr(module, name, boundary)


def patch_server(module):
    cls = module.HermesACPAgent
    if getattr(cls, ATTR, False):
        return
    setattr(cls, ATTR, True)
    initialize, prompt, run, cancel = cls.initialize, cls.prompt, cls._run_agent_turn, cls.cancel
    old_extension = getattr(cls, "ext_method", None)

    @functools.wraps(initialize)
    async def init(self, *args, **kwargs):
        response = await initialize(self, *args, **kwargs)
        response.field_meta = dict(response.field_meta or {})
        response.field_meta[META] = {"version": 1, "activeOnly": True, "receipts": "consumed"}
        return response

    @functools.wraps(prompt)
    async def scoped_prompt(self, prompt, session_id, **kwargs):
        meta = kwargs.get(TURN_META)
        host = meta.get("hostTurnId") if isinstance(meta, dict) and meta.get("version") == 1 else None
        token = _HOST.set((self, session_id, host) if isinstance(host, str) and 0 < len(host) <= 256 else None)
        try:
            return await prompt_original(self, prompt=prompt, session_id=session_id, **kwargs)
        finally:
            _HOST.reset(token)
    prompt_original = prompt

    @functools.wraps(run)
    def worker(self, *, state, session_id, loop, conn, **kwargs):
        scope = _HOST.get()
        if scope is None or scope[:2] != (self, session_id):
            return run(self, state=state, session_id=session_id, loop=loop, conn=conn, **kwargs)

        def notify(params):
            if conn is not None and not loop.is_closed():
                async def send():
                    try:
                        await conn.ext_notification(STATE_METHOD, params)
                    except Exception:
                        pass  # Lost readiness never proves delivery or permits a retry.
                try:
                    loop.call_soon_threadsafe(lambda: loop.create_task(send()))
                except RuntimeError:
                    pass  # Shutdown between the is_closed check and scheduling.
        turn = Turn(state, scope[2], notify)
        previous = _controller(state.agent)
        if previous is not None:
            previous.close()
        setattr(state, ATTR, turn)
        setattr(state.agent, ATTR, turn)
        try:
            return run(self, state=state, session_id=session_id, loop=loop, conn=conn, **kwargs)
        finally:
            turn.close()

    @functools.wraps(cancel)
    async def stop(self, session_id, **kwargs):
        state = self.session_manager.get_session(session_id)
        turn = getattr(state, ATTR, None)
        if isinstance(turn, Turn):
            turn.close()
        return await cancel(self, session_id=session_id, **kwargs)

    async def extension(self, method, params):
        if method != STEER_METHOD:
            if old_extension is not None:
                return await old_extension(self, method, params)
            from acp.exceptions import RequestError
            raise RequestError.method_not_found("_" + method)
        state = self.session_manager.get_session(params.get("sessionId"))
        turn = getattr(state, ATTR, None)
        if not isinstance(turn, Turn):
            return {"version": 1, "submissionId": params.get("submissionId", ""),
                    "hostTurnId": params.get("hostTurnId", ""),
                    "nativeTurnId": params.get("expectedNativeTurnId", ""), "outcome": "not_injected"}
        # Request cancellation/transport loss must not cancel custody of an admitted B.
        return await asyncio.shield(asyncio.wrap_future(turn.submit(params)))

    cls.initialize, cls.prompt, cls._run_agent_turn, cls.cancel, cls.ext_method = init, scoped_prompt, worker, stop, extension


TARGETS = {"acp_adapter.server": "server", "agent.agent_runtime_helpers": "tools",
           "agent.turn_iteration_prep": "prepare", "agent.turn_finalizer": "finalizer"}


class _Loader(importlib.abc.Loader):
    def __init__(self, loader, finder, name, root):
        self.loader, self.finder, self.name, self.root = loader, finder, name, root

    def create_module(self, spec):
        create = getattr(self.loader, "create_module", None)
        return create(spec) if create is not None else None

    def exec_module(self, module):
        # get_source and execution both use the hash-audited source, not a stale pyc.
        source = self.loader.get_source(self.name)
        expected = SOURCES[self.name.replace(".", "/") + ".py"]
        if source is None or hashlib.sha256(source.encode()).hexdigest() != expected:
            self.finder.disabled = True
            return self.loader.exec_module(module)
        exec(compile(source, module.__spec__.origin, "exec"), module.__dict__)
        if self.finder.disabled:
            return
        if TARGETS[self.name] == "server":
            patch_server(module)
        else:
            patch_boundary(module, TARGETS[self.name])


class _Finder(importlib.abc.MetaPathFinder):
    def __init__(self):
        self.checked = {}
        self.disabled = False

    def find_spec(self, fullname, path=None, target=None):
        if self.disabled or fullname not in TARGETS:
            return None
        spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        if spec is None or spec.origin is None or not hasattr(spec.loader, "get_source"):
            self.disabled = True
            return spec
        root = pathlib.Path(spec.origin).parent.parent
        if root not in self.checked:
            sdk = importlib.machinery.PathFinder.find_spec("acp")
            sdk_root = pathlib.Path(sdk.origin).parent if sdk is not None and sdk.origin else None
            self.checked[root] = source_matches(root, SOURCES) and sdk_root is not None and source_matches(sdk_root, SDK_SOURCES)
        if not self.checked[root]:
            self.disabled = True
            return spec
        spec.loader = _Loader(spec.loader, self, fullname, root)
        return spec


def install():
    # A preexisting customization that already imported runtime modules retains
    # its normal behavior; we cannot safely retrofit aliases after module import.
    if any(name in sys.modules for name in TARGETS):
        return False
    sys.meta_path.insert(0, _Finder())
    return True
