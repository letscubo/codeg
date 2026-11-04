"""Reasoning adapter installed by the shared owned Hermes ACP bootstrap."""
from __future__ import annotations

import contextvars
import copy
import functools
import inspect
import os

SNAPSHOT_ENV = "CODEG_HERMES_REASONING_EFFORT"
HANDSHAKE_KEY = "codegHermesReasoningSnapshotV1"
_UNSET = object()
_active_depth = contextvars.ContextVar("codeg_hermes_acp_depth", default=_UNSET)


def install_snapshot_bridge():
    """Bind one immutable connection snapshot only to ACP Agent construction."""
    from acp_adapter.session import SessionManager
    from acp_adapter.server import HermesACPAgent
    from hermes_cli.config import load_config
    from hermes_constants import parse_reasoning_effort, resolve_reasoning_config
    from run_agent import AIAgent

    snapshot = os.environ.get(SNAPSHOT_ENV)
    if snapshot is None or not snapshot.strip():
        raise RuntimeError("Hermes reasoning snapshot is missing")
    inherited = snapshot == "_inherit"
    explicit = None if inherited else parse_reasoning_effort(snapshot)
    if not inherited and explicit is None:
        raise RuntimeError("Hermes rejected the reasoning snapshot")

    installed = getattr(SessionManager, "_codeg_reasoning_snapshot", _UNSET)
    if installed is not _UNSET:
        if installed != snapshot:
            raise RuntimeError("A Hermes connection cannot change its launch snapshot")
        return

    original_init = AIAgent.__init__
    if "reasoning_config" not in inspect.signature(original_init).parameters:
        raise RuntimeError("This Hermes Agent has no reasoning_config constructor parameter")
    original_make = SessionManager._make_agent
    make_signature = inspect.signature(original_make)
    original_initialize = HermesACPAgent.initialize

    @functools.wraps(original_init)
    def initialize_agent(self, *args, **kwargs):
        selected = _active_depth.get()
        if selected is not _UNSET and kwargs.get("platform") == "acp":
            # Applied before native initialization, including its session restore snapshot.
            kwargs["reasoning_config"] = copy.deepcopy(selected)
        return original_init(self, *args, **kwargs)

    @functools.wraps(original_make)
    def make_agent(self, *args, **kwargs):
        bound = make_signature.bind(self, *args, **kwargs)
        if getattr(self, "_agent_factory", None) is not None:
            raise RuntimeError("Hermes reasoning requires the native ACP Agent factory")
        if inherited:
            config = copy.deepcopy(load_config())
            if not isinstance(config, dict):
                raise RuntimeError("Hermes configuration is not an object")
            agent_config = config.get("agent")
            if isinstance(agent_config, dict):
                # This field is the legacy per-session scratch value. Another
                # conversation may have rewritten it before this process starts.
                agent_config.pop("reasoning_effort", None)
            selected = resolve_reasoning_config(config, bound.arguments.get("model") or "")
        else:
            selected = copy.deepcopy(explicit)
        token = _active_depth.set(selected)
        try:
            agent = original_make(self, *args, **kwargs)
        finally:
            _active_depth.reset(token)
        if getattr(agent, "reasoning_config", _UNSET) != selected:
            raise RuntimeError("Hermes did not apply the selected reasoning snapshot")
        return agent

    @functools.wraps(original_initialize)
    async def initialize_acp(self, *args, **kwargs):
        response = await original_initialize(self, *args, **kwargs)
        # agent-client-protocol 0.9 uses field_meta with the wire alias _meta.
        # A different schema must fail, rather than advertise an unverified bridge.
        if not hasattr(response, "field_meta"):
            raise RuntimeError("This Hermes ACP schema cannot confirm reasoning support")
        response.field_meta = {**(response.field_meta or {}), HANDSHAKE_KEY: True}
        return response

    AIAgent.__init__ = initialize_agent
    SessionManager._make_agent = make_agent
    HermesACPAgent.initialize = initialize_acp
    SessionManager._codeg_reasoning_snapshot = snapshot
