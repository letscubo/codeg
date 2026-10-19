"""Attach to an installed Hermes owner's existing session; never start or replace it.

The installed official client owns private-socket peer authentication and WS tickets.
Only bounded, credential-free relay frames are written to stdout. Source contracts:
NousResearch/hermes-agent: hermes_cli/gateway_client.py, gateway/session_controls.py,
gateway/session_authority.py, gateway/run_turn_runner.py (canonical protocol v1).
"""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys
import sqlite3
import time

WIRE = sys.stdout
CONTROL_READER = None


def emit(event, **fields):
    WIRE.write(json.dumps({"event": event, **fields}, ensure_ascii=False) + "\n")
    WIRE.flush()


def require(value, reason):
    if not value:
        raise ValueError(reason)


def read_control_status(home):
    """The official private connector authenticates the owner UID before this read."""
    from hermes_cli.gateway_runtime_discovery import connect_private, _identify_response
    deadline = time.monotonic() + 5
    with connect_private(home, 5) as peer:
        peer.sendall(b'{"protocol":1,"verb":"status","id":1}\n')
        data = bytearray()
        while b"\n" not in data:
            remaining = deadline - time.monotonic()
            require(remaining > 0, "hermes_status_timeout")
            peer.settimeout(remaining)
            chunk = peer.recv(65536)
            require(chunk and len(data) + len(chunk) <= 524288, "hermes_invalid_status")
            data.extend(chunk)
    return _identify_response(bytes(data))


def discover_existing(home):
    from hermes_cli.gateway_runtime import _endpoint
    from hermes_cli.gateway_runtime_discovery import query_identify
    # discover_gateway_endpoint's absent-owner fallback may chmod an old lock.
    # These official read/validation primitives do not repair or start anything.
    return _endpoint(query_identify(home, timeout=5), home)


def native_adapter_absent(home, endpoint):
    from hermes_cli.gateway_runtime import control_home_for
    from hermes_cli.gateway_runtime_discovery import query_identify
    from gateway.status import runtime_status_is_stale
    control_home = control_home_for(home, endpoint)
    identity = query_identify(control_home, timeout=5)
    status = read_control_status(control_home)
    require(identity.get("instance_id") == endpoint.instance_id, "hermes_authority_changed")
    require(identity.get("pid") == status.get("answering_pid") == status.get("pid")
            and identity.get("start_time") == status.get("start_time")
            and not runtime_status_is_stale(status), "hermes_status_unverified")
    # A stale disconnected/error/disabled record is insufficient proof that its
    # adapter object no longer exists. Require it absent from the current boot.
    require(isinstance(status.get("platforms"), dict), "hermes_status_unverified")
    return not any(key == "telegram" or key.endswith(":telegram") for key in status["platforms"])


def simple_policy(config, extra, home):
    """Preserve the supported policy exactly; refuse rules this bridge cannot express."""
    truthy = lambda value: str(value).strip().lower() in ("true", "1", "yes", "on")
    reasons = []
    complex_keys = ("groups", "group_allow_from", "group_policy", "allowed_chats", "group_allowed_chats",
        "allowed_topics", "ignored_threads", "mention_patterns", "guest_mode", "allow_admin_from",
        "observe_unmentioned_group_messages", "ingest_unmentioned_group_messages", "free_response",
        "roles", "allow_bots", "channel_overrides", "channel_prompts", "channel_skill_bindings")
    if any(extra.get(key) for key in complex_keys) or config.platforms[next(p for p in config.platforms if p.value == "telegram")].channel_overrides:
        reasons.append("hermes_complex_telegram_policy_unsupported")
    env_complex = ("TELEGRAM_GROUP_ALLOWED_USERS", "TELEGRAM_GROUP_ALLOWED_CHATS", "TELEGRAM_ALLOWED_CHATS",
        "TELEGRAM_ALLOWED_TOPICS", "TELEGRAM_IGNORED_THREADS", "TELEGRAM_MENTION_PATTERNS",
        "TELEGRAM_ALLOW_BOTS", "TELEGRAM_GUEST_MODE", "TELEGRAM_OBSERVE_UNMENTIONED_GROUP_MESSAGES")
    if any(os.environ.get(key) for key in env_complex):
        reasons.append("hermes_complex_telegram_policy_unsupported")
    def ids(raw):
        if isinstance(raw, list):
            return {str(value).strip() for value in raw if str(value).strip()}
        if not isinstance(raw, str):
            return set()
        raw = raw.strip()
        if raw.startswith("["):
            return ids(json.loads(raw))
        return {value.strip() for value in raw.split(",") if value.strip()}
    platform_ids = ids(os.environ.get("TELEGRAM_ALLOWED_USERS", ""))
    global_ids = ids(os.environ.get("GATEWAY_ALLOWED_USERS", ""))
    allowed = platform_ids | global_ids
    adapter_ids = ids(extra.get("allow_from", extra.get("allowed_users", "")))
    if not platform_ids and not global_ids:
        allowed |= adapter_ids
    # PairingStore.__init__ migrates files. Read its two documented layouts
    # without instantiating it; the runtime unions both during startup.
    for directory in (home / "pairing", home / "platforms" / "pairing"):
        approved = directory / "telegram-approved.json"
        if approved.exists():
            grants = json.loads(approved.read_text(encoding="utf-8-sig"))
            require(isinstance(grants, dict), "hermes_pairing_unreadable")
            allowed |= set(grants)
    allow_all = truthy(os.environ.get("TELEGRAM_ALLOW_ALL_USERS", "")) or "*" in allowed
    if not platform_ids and not global_ids:
        allow_all = allow_all or truthy(os.environ.get("GATEWAY_ALLOW_ALL_USERS", ""))
    # Telegram's explicit allow_from is an intake prefilter before the gateway
    # union above. A global or pairing grant cannot widen that earlier filter.
    if extra.get("allow_from") is not None and "*" not in adapter_ids:
        allowed = adapter_ids if allow_all else allowed & adapter_ids
        allow_all = False
    if any(not value.isdigit() for value in allowed if value != "*"):
        reasons.append("hermes_username_allowlist_unsupported")
    if extra.get("dm_policy") not in (None, "", "allowlist", "open"):
        reasons.append("hermes_dm_policy_unsupported")
    behavior = extra.get("unauthorized_dm_behavior")
    if behavior is None and getattr(config, "unauthorized_dm_behavior", "pair") not in ("pair", "ignore"):
        reasons.append("hermes_pairing_flow_unsupported")
    if behavior not in (None, "ignore") or (not allow_all and not platform_ids and not global_ids and not adapter_ids):
        reasons.append("hermes_pairing_flow_unsupported")
    return {"allowAllUsers": allow_all, "allowedUserIds": sorted(allowed - {"*"}),
            "requireMention": truthy(os.environ.get("TELEGRAM_REQUIRE_MENTION", extra.get("require_mention", False)))}, reasons


def native_sessions(home, sessions_dir):
    """Read SessionStore's primary index using its official entry parser, no DB migration."""
    from gateway.session import SessionEntry
    database = home / "state.db"
    require(database.is_file(), "hermes_session_store_unavailable")
    result = []
    with sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True) as db:
        db.execute("PRAGMA query_only=ON")
        # Exact query and scope from SessionGatewayMixin.load_gateway_routing_entries
        # and SessionPersistenceMixin._routing_scope; never synthesize session keys.
        rows = db.execute("SELECT session_key, entry_json FROM gateway_routing WHERE scope = ? LIMIT 5001",
                          (str(Path(sessions_dir).resolve()),)).fetchall()
        require(len(rows) <= 5000, "hermes_session_inventory_too_large")
        for key, raw in rows:
            entry = SessionEntry.from_dict(json.loads(raw))
            source = entry.origin
            if source is None:
                require(getattr(entry.platform, "value", None) != "telegram", "hermes_original_source_missing")
                continue
            if source.platform.value != "telegram":
                continue
            require(entry.session_key == key, "hermes_invalid_session_mapping")
            require(source.chat_id and entry.session_id, "hermes_invalid_session_mapping")
            # Only retained routes whose original transcript row still exists.
            require(db.execute("SELECT 1 FROM sessions WHERE id = ?", (entry.session_id,)).fetchone(),
                    "hermes_session_mapping_stale")
            override = entry.model_override or {}
            result.append({"sessionKey":key, "nativeSessionId":entry.session_id, "chatId":str(source.chat_id),
                "threadId":str(source.thread_id) if source.thread_id is not None else None,
                "chatType":"private" if source.chat_type == "dm" else source.chat_type,
                "nativeOriginChatType":source.chat_type,
                "senderId":str(source.user_id) if source.user_id is not None else None,
                **({"model":override["model"]} if override.get("model") else {}),
                **({"provider":override["provider"]} if override.get("provider") else {})})
    return result


async def inspect_native(request):
    home = Path(request["hermesHome"])
    require(home.is_absolute() and home.is_dir(), "hermes_home_unavailable")
    os.environ["HERMES_HOME"] = str(home)
    from dotenv import dotenv_values
    # Read-only parser, unlike load_hermes_dotenv's BOM repair/external secret hooks.
    os.environ.update({key:value for key,value in dotenv_values(home / ".env").items() if value is not None})
    # An old dotenv must not redirect inspection to a different runtime home.
    os.environ["HERMES_HOME"] = str(home)
    import yaml
    raw = yaml.safe_load((home / "config.yaml").read_text(encoding="utf-8-sig")) or {}
    require(isinstance(raw, dict), "hermes_config_unreadable")
    from gateway.config import load_gateway_config, Platform
    from hermes_cli.gateway_client import connect_gateway
    from gateway.session_cli_turn import admit_cli_turn  # capability gate, without executing a turn
    config = load_gateway_config()
    require(not config.multiplex_profiles, "hermes_multiplex_takeover_unsupported")
    platform = config.platforms.get(Platform.TELEGRAM)
    require(platform is not None, "hermes_telegram_not_configured")
    policy, reasons = simple_policy(config, platform.extra, home)
    if raw.get("secrets") or os.environ.get("HERMES_MANAGED_DIR"):
        reasons.append("hermes_external_policy_source_unsupported")
    if platform.enabled:
        reasons.append("hermes_native_delivery_not_disabled")
    token = platform.token or os.environ.get("TELEGRAM_BOT_TOKEN", "")
    require(isinstance(token,str) and token, "hermes_telegram_token_unavailable")
    sessions = native_sessions(home, config.sessions_dir)
    if not sessions:
        reasons.append("hermes_existing_native_session_required")
    # Group intake also has reply-to-bot and exclusive-mention semantics. Until
    # the platform envelope carries those facts, refuse to weaken that policy.
    if any(session["nativeOriginChatType"] != "dm" for session in sessions):
        reasons.append("hermes_group_intake_policy_unsupported")
    discovered = await asyncio.to_thread(discover_existing, home)
    require(discovered.state == "ready" and discovered.endpoint is not None, "hermes_gateway_not_ready")
    endpoint = discovered.endpoint
    absent = await asyncio.to_thread(native_adapter_absent, home, endpoint)
    disabled = not platform.enabled and absent
    if not absent:
        reasons.append("hermes_native_adapter_still_present")
    async with connect_gateway(endpoint=endpoint) as client:
        info = await client.rpc("runtime.describe")
        require(info.get("instance_id") == endpoint.instance_id and
                {"durable-admission-v1","event-replay-v1"} <= set(info.get("capabilities") or []),
                "hermes_canonical_gateway_unsupported")
    return {"kind":"hermes", "ready":not reasons, "reasonCodes":sorted(set(reasons)),
        "hermesHome":str(home), "pythonPath":request["pythonPath"], "nativeDeliveryDisabled":disabled,
        "botTokenSha256":hashlib.sha256(token.encode()).hexdigest(), "policy":policy,
        "sessions":sessions, "existingSessionsOnly":True, "gatewayInstanceId":endpoint.instance_id,
        "gatewayEpoch":endpoint.authority_epoch}


async def command():
    global CONTROL_READER
    if CONTROL_READER is None:
        reader = asyncio.StreamReader(limit=65536)
        await asyncio.get_running_loop().connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
        CONTROL_READER = reader
    # A cancellable pipe reader: a background executor thread blocked in
    # readline would keep asyncio.run alive after an execution error forever.
    line = await CONTROL_READER.readline()
    return json.loads(line) if line else {"action": "cancel"}


def relay_frame(frame, session_id, admission_id, epoch, seen):
    """Only this admission's ordered output can leave the runtime boundary."""
    if frame.get("method") != "event":
        return seen
    event = frame.get("params") or {}
    if event.get("session_id") != session_id:
        return seen
    if event.get("type") == "session.replay_gap":
        raise ValueError("hermes_event_replay_gap")
    if event.get("admission_id") != admission_id:
        return seen
    require(event.get("authority_epoch") == epoch, "hermes_authority_changed")
    sequence = event.get("seq")
    require(type(sequence) is int, "hermes_invalid_event")
    if sequence <= seen:
        return seen
    payload = event.get("payload") or {}
    kind = event.get("type")
    if kind == "message.delta" and isinstance(payload.get("text"), str):
        emit("message.delta", delta=payload["text"])
    elif kind == "message.interim" and not payload.get("already_streamed"):
        if isinstance(payload.get("text"), str):
            emit("message.delta", delta=payload["text"])
    elif kind in ("tool.start", "tool.complete"):
        emit("tool.started" if kind == "tool.start" else "tool.completed",
             tool=str(payload.get("name") or payload.get("tool_name") or "tool"),
             tool_id=str(payload.get("tool_id") or payload.get("tool_call_id") or ""),
             failed=payload.get("is_error") is True)
    # Reasoning text, raw tool input/output, history and credentials are never forwarded.
    return sequence


async def stop(client, session_id, admission_id):
    receipt = await client.rpc("prompt.receipt", session_id=session_id, admission_id=admission_id)
    if receipt.get("status") == "queued":
        return await client.rpc("prompt.cancel", session_id=session_id, admission_id=admission_id)
    if receipt.get("status") == "started":
        generation = receipt.get("execution_generation")
        require(type(generation) is int, "hermes_invalid_generation")
        await client.rpc("session.interrupt", session_id=session_id, execution_generation=generation)
    return receipt


async def observe(client, session_id, admission_id, epoch, seen, poll_interval=5):
    control = asyncio.create_task(command())
    incoming = asyncio.create_task(client.events.get())
    stopped = False
    next_receipt = time.monotonic() + poll_interval
    try:
        while True:
            ready, _ = await asyncio.wait((control, incoming), timeout=max(0, next_receipt - time.monotonic()),
                                          return_when=asyncio.FIRST_COMPLETED)
            if control in ready:
                action = control.result()
                if action.get("action") == "cancel" and not stopped:
                    await stop(client, session_id, admission_id)
                    stopped = True
                # EOF is cancellation once; do not spin on the closed pipe.
                control = asyncio.create_task(asyncio.sleep(86400)) if stopped else asyncio.create_task(command())
            if incoming in ready:
                frame = incoming.result()
                if isinstance(frame, Exception):
                    raise frame
                seen = relay_frame(frame, session_id, admission_id, epoch, seen)
                incoming = asyncio.create_task(client.events.get())
            # A completion notification is not the durable terminal verdict. The receipt
            # is also the recovery path when an event stream ends between final and ACK.
            if time.monotonic() >= next_receipt or (incoming not in ready and stopped):
                receipt = await client.rpc("prompt.receipt", session_id=session_id,
                                           admission_id=admission_id, include_result=True)
                next_receipt = time.monotonic() + poll_interval
                status = receipt.get("status")
                if status == "unknown":
                    raise ValueError("hermes_execution_unknown")
                if status == "terminal":
                    # The official owner publishes completion before exposing terminal.
                    # Drain already-buffered frames before considering the final snapshot.
                    if incoming.done() and not incoming.cancelled():
                        seen = relay_frame(incoming.result(), session_id, admission_id, epoch, seen)
                    while not client.events.empty():
                        seen = relay_frame(client.events.get_nowait(), session_id, admission_id, epoch, seen)
                    result = receipt.get("result") or {}
                    outcome = receipt.get("outcome")
                    if outcome == "completed":
                        emit("run.completed", output=result.get("final_response") or "")
                    elif outcome in ("cancelled", "interrupted"):
                        emit("run.cancelled")
                    else:
                        emit("run.failed")
                    return
    finally:
        control.cancel()
        incoming.cancel()


async def run(request):
    require(request.get("nativeDeliveryDisabled") is True, "hermes_native_delivery_not_disabled")
    home = Path(request["hermesHome"])
    require(home.is_absolute() and home.is_dir(), "hermes_home_unavailable")
    os.environ["HERMES_HOME"] = str(home)
    # Importing an old runtime fails before session.resume or prompt.submit. Never
    # install/upgrade modules, start an owner, create a session, or copy its history.
    from hermes_cli.gateway_client import connect_gateway
    from gateway.session_cli_turn import admit_cli_turn
    override = None
    if request.get("model"):
        override = {"model": request["model"], **({"provider":request["provider"]} if request.get("provider") else {})}
        admit_cli_turn({"model_override": override})
    discovered = await asyncio.to_thread(discover_existing, home)
    require(discovered.state == "ready" and discovered.endpoint is not None, "hermes_gateway_not_ready")
    endpoint = discovered.endpoint
    require(endpoint.runtime_protocol == 1 and "session-authority-v1" in endpoint.capabilities,
            "hermes_canonical_gateway_unsupported")
    require(await asyncio.to_thread(native_adapter_absent, home, endpoint), "hermes_native_adapter_still_present")
    async with connect_gateway(endpoint=endpoint) as client:
        info = await client.rpc("runtime.describe")
        require(all(info.get(key) == getattr(endpoint, key)
                    for key in ("instance_id", "profile_id", "authority_epoch")), "hermes_authority_changed")
        require({"durable-admission-v1", "event-replay-v1"} <= set(info.get("capabilities") or []),
                "hermes_canonical_gateway_unsupported")
        snapshot = await client.rpc("session.resume", session_id=request["sessionId"])
        # The owner may resolve a compressed physical row to its original lineage root.
        # This is the official resume mapping, never a client-created replacement.
        session_id = snapshot.get("session_id")
        require(isinstance(session_id, str) and session_id, "hermes_session_not_found")
        emit("ready", session_id=session_id)
        if (await command()).get("action") != "submit":
            emit("run.cancelled")
            return
        receipt = await client.rpc("prompt.submit", session_id=session_id, text=request["message"],
                                   submission_id=request["turnId"], **({"model_override":override} if override else {}))
        admission_id = receipt.get("admission_id")
        require(isinstance(admission_id, str) and admission_id, "hermes_admission_unknown")
        await observe(client, session_id, admission_id, endpoint.authority_epoch,
                      int(snapshot.get("last_sequence") or 0))


if __name__ == "__main__":
    sys.stdout = sys.stderr
    try:
        request = json.loads(sys.stdin.readline())
        if request.get("action") == "inspect":
            emit("inspect", result=asyncio.run(inspect_native(request)))
        else:
            asyncio.run(run(request))
    except Exception as error:
        # RPC errors and imports can contain paths/tokens. Only our fixed codes leave.
        reason = str(error) if isinstance(error, ValueError) else ""
        if isinstance(error, ImportError):
            reason = "hermes_canonical_gateway_unsupported"
        if not reason.startswith("hermes_") or not reason.replace("_", "").isalnum() or len(reason) > 80:
            reason = "hermes_gateway_failed"
        emit("bridge.error", code=reason)
        sys.exit(1)
