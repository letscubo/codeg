"""Compatibility with the installed Hermes HTTP/session APIs.

Contract verified against NousResearch/hermes-agent commit 345cd2b057a452236de401d3534b8502a7465e8d.
The five primary source hashes match the inspected runtime. No provider credentials,
transcript bodies, or bot credentials are returned by inspect/resolve.
This file is prepended to legacy_hermes_gateway.py by the Rust launcher.
"""
import asyncio
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import time
from urllib.parse import quote

HTTP_TRANSPORT = "hermes_http"
HTTP_FEATURES = ("run_status", "run_stop", "session_resources", "session_chat_streaming", "session_model_lock")


@contextmanager
def http_mutation_lock(home):
    import fcntl
    # PairingStore and SessionStore only have threading locks. Serialize codeg's
    # independent bridge processes; no credentials or message bodies live here.
    descriptor = os.open(home / ".codeg-channel-mutation.lock",
        os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def http_channel_sessions(home, sessions_dir):
    if not (home / "state.db").is_file():
        return []
    by_key = {}
    for directory in (Path(sessions_dir), Path(sessions_dir) / ".codeg-telegram"):
        for session in native_sessions(home, directory):
            previous = by_key.get(session["sessionKey"])
            if previous:
                if previous["nativeSessionId"] != session["nativeSessionId"]:
                    require(session["nativeSessionId"] in http_compression_ids(home, previous["nativeSessionId"]),
                            "hermes_session_mapping_ambiguous")
                require(all(previous.get(key) == session.get(key) for key in ("chatId","threadId","chatType","senderId")),
                        "hermes_session_mapping_ambiguous")
            else:
                by_key[session["sessionKey"]] = session
    require(len(by_key) <= 5000, "hermes_session_inventory_too_large")
    return list(by_key.values())


def http_private_sessions(sessions, chat_id, sender_id):
    matches = [s for s in sessions if s["chatId"] == chat_id and not s.get("threadId")
        and s.get("senderId") in (None, sender_id) and s.get("chatType") == "private"]
    require(len(matches) <= 1, "hermes_session_mapping_ambiguous")
    return matches


def http_create_native_session(home, config, source):
    from gateway.session import SessionStore
    with http_mutation_lock(home):
        # Re-read after admission to the process lock: another bridge may have
        # created this route. Existing native routes are always left untouched.
        matches = http_private_sessions(http_channel_sessions(home, config.sessions_dir), source.chat_id, source.user_id)
        if matches:
            return matches
        # Same state.db, original Telegram key grammar/source; isolate only the
        # routing index so the gateway's cached whole-index saves cannot erase it.
        store = SessionStore(Path(config.sessions_dir) / ".codeg-telegram", config)
        entry = store.get_or_create_session(source)
        return [s for s in http_channel_sessions(home, config.sessions_dir) if s["sessionKey"] == entry.session_key]


def http_pairing_reply(home, sender_id, sender_name):
    from gateway.pairing import PairingStore
    with http_mutation_lock(home):
        store = PairingStore()
        # Exact gateway/run_inbound.py response and rate limit.
        if store._is_rate_limited("telegram", sender_id):
            return None
        code = store.generate_code("telegram", sender_id, sender_name)
        if code:
            return "Hi~ I don't recognize you yet!\n\nHere's your pairing code: `" + code + "`\n\nAsk the bot owner to run:\n`hermes pairing approve telegram " + code + "`"
        store._record_rate_limit("telegram", sender_id)
        return "Too many pairing requests right now~ Please try again later!"


def http_process_api_environment(home):
    """Read only the verified gateway PID's API settings, never scan other processes."""
    from gateway.status import (read_runtime_status, runtime_status_pid_is_live,
        get_process_start_time, looks_like_gateway_runtime_command_line)
    state = read_runtime_status(home / "gateway_state.json")
    require(isinstance(state, dict) and runtime_status_pid_is_live(state), "hermes_status_unverified")
    pid, started = state.get("pid"), state.get("start_time")
    require(type(pid) is int and pid > 0 and started is not None
            and get_process_start_time(pid) == started, "hermes_status_unverified")
    process = Path("/proc") / str(pid)
    try:
        args = (process / "cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8")
        require(looks_like_gateway_runtime_command_line(args), "hermes_status_unverified")
        # Only this known process is read; unrelated env is discarded immediately.
        needed = {"API_SERVER_KEY", "API_SERVER_ENABLED", "API_SERVER_PORT", "API_SERVER_HOST", "HERMES_HOME", "HOME"}
        selected = {}
        for item in (process / "environ").read_bytes().split(b"\0"):
            key, separator, value = item.partition(b"=")
            name = key.decode("utf-8", errors="ignore")
            if separator and name in needed:
                selected[name] = value.decode("utf-8")
        process_home = selected.get("HERMES_HOME") or str(Path(selected.get("HOME", "/")) / ".hermes")
        require(Path(process_home).is_absolute() and Path(process_home).resolve() == home.resolve()
                and get_process_start_time(pid) == started, "hermes_authority_changed")
        return {name:value for name,value in selected.items() if name.startswith("API_SERVER_")}
    except (OSError, UnicodeError):
        raise ValueError("hermes_api_credential_unavailable") from None


def http_pairing_behavior(config, platform_id, extra):
    """Private Telegram subset of GatewayAuthorizationMixin's exact precedence."""
    if "unauthorized_dm_behavior" in extra:
        return config.get_unauthorized_dm_behavior(platform_id)
    if config.unauthorized_dm_behavior != "pair":
        return config.unauthorized_dm_behavior
    dm_policy = str(extra.get("dm_policy") or "").strip().lower()
    if dm_policy == "pairing":
        return "pair"
    if dm_policy in ("allowlist", "disabled"):
        return "ignore"
    if any(os.environ.get(name, "").strip() for name in ("TELEGRAM_ALLOWED_USERS",
            "TELEGRAM_GROUP_ALLOWED_USERS", "TELEGRAM_GROUP_ALLOWED_CHATS", "GATEWAY_ALLOWED_USERS")):
        return "ignore"
    return "pair"


def http_context(request):
    home = Path(request["hermesHome"])
    require(home.is_absolute() and home.is_dir(), "hermes_home_unavailable")
    os.environ["HERMES_HOME"] = str(home)
    from dotenv import dotenv_values
    # Hermes's own dotenv values overlay deployment env; never print either.
    os.environ.update({k:v for k,v in dotenv_values(home / ".env").items() if v is not None})
    os.environ["HERMES_HOME"] = str(home)
    import yaml
    raw = yaml.safe_load((home / "config.yaml").read_text(encoding="utf-8-sig")) or {}
    require(isinstance(raw, dict), "hermes_config_unreadable")
    raw_api = (raw.get("platforms") or {}).get("api_server") or {}
    raw_key = (raw_api.get("extra") or {}).get("key")
    if not os.environ.get("API_SERVER_KEY") and not raw_key:
        # Container exec may not inherit the gateway's original API credential.
        # Keep the verified value only in this short-lived bridge's memory.
        os.environ.update(http_process_api_environment(home))
    from gateway.config import load_gateway_config, Platform, PlatformConfig
    config = load_gateway_config()
    require(not config.multiplex_profiles, "hermes_multiplex_takeover_unsupported")
    platform = config.platforms.setdefault(Platform.TELEGRAM, PlatformConfig())
    api = config.platforms.get(Platform.API_SERVER)
    require(api is not None and api.enabled, "hermes_api_not_configured")
    key = api.extra.get("key") or os.environ.get("API_SERVER_KEY", "")
    require(isinstance(key, str) and len(key.strip()) >= 16, "hermes_api_credential_unavailable")
    port = api.extra.get("port", os.environ.get("API_SERVER_PORT", 8642))
    require(str(port).isdigit() and 0 < int(port) < 65536, "hermes_api_not_configured")
    # Local runtime only: a platform request cannot redirect the API credential.
    base = "http://127.0.0.1:" + str(int(port))
    token = platform.token or os.environ.get("TELEGRAM_BOT_TOKEN", "")
    policy, reasons = simple_policy(config, platform.extra, home)
    # The HTTP compatibility bridge retains the official PairingStore flow.
    reasons = [reason for reason in reasons if reason != "hermes_pairing_flow_unsupported"]
    behavior = http_pairing_behavior(config, Platform.TELEGRAM, platform.extra)
    if behavior not in ("pair", "ignore"):
        reasons.append("hermes_pairing_flow_unsupported")
    if raw.get("secrets") or os.environ.get("HERMES_MANAGED_DIR"):
        reasons.append("hermes_external_policy_source_unsupported")
    policy["pairingEnabled"] = behavior == "pair"
    # Adapter allow_from is checked before gateway pairing and cannot be widened.
    allowed_from = platform.extra.get("allow_from")
    if allowed_from is not None:
        values = allowed_from if isinstance(allowed_from,list) else str(allowed_from).split(",")
        policy["intakeAllowedUserIds"] = [str(v).strip() for v in values if str(v).strip()]
    return home, config, platform, base, key, token, policy, sorted(set(reasons))


async def http_json(client, method, path, *, body=None, headers=None, missing=False):
    import aiohttp
    try:
        async with client.request(method, path, json=body, headers=headers, allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=15)) as response:
            if missing and response.status == 404:
                return None
            require(response.status < 300, "hermes_http_request_rejected")
            data = await response.json()
            require(isinstance(data, dict), "hermes_invalid_http_response")
            return data
    except (asyncio.TimeoutError, aiohttp.ClientError):
        raise ValueError("hermes_http_unavailable") from None


def http_native_absent(home, health):
    from gateway.status import read_runtime_status, runtime_status_pid_is_live, get_process_start_time
    state = read_runtime_status(home / "gateway_state.json")
    require(isinstance(state, dict) and runtime_status_pid_is_live(state), "hermes_status_unverified")
    pid, started = state.get("pid"), state.get("start_time")
    require(type(pid) is int and pid > 0 and type(started) is int and started > 0
            and health.get("pid") == pid and started == get_process_start_time(pid)
            and health.get("gateway_state") == state.get("gateway_state") == "running",
            "hermes_status_unverified")
    require(isinstance(health.get("platforms"), dict) and isinstance(state.get("platforms"), dict), "hermes_status_unverified")
    # Hermes status.py preserves primary-platform entries across restarts and
    # /health/detailed returns that same file, not the live adapter inventory.
    # Only an explicitly disconnected record from a verified previous process
    # can be ignored. Current/unknown writers may still own a reconnect loop.
    # Official contract: 345cd2b status.py:815-862, run_startup.py:984-988.
    for platforms in (health["platforms"], state["platforms"]):
        for name, entry in platforms.items():
            if name != "telegram" and not name.endswith(":telegram"):
                continue
            if name != "telegram" or not isinstance(entry, dict) or entry.get("state") != "disconnected":
                return False
            writer_pid, writer_started = entry.get("writer_pid"), entry.get("writer_start_time")
            if (type(writer_pid) is not int or writer_pid <= 0
                    or type(writer_started) is not int or not 0 < writer_started < started):
                return False
            try:
                observed_start = get_process_start_time(writer_pid)
                if (observed_start is not None and (type(observed_start) is not int or observed_start <= 0)):
                    return False
                if observed_start == writer_started or runtime_status_pid_is_live(
                        {"pid":writer_pid, "start_time":writer_started}) is not False:
                    return False
            except Exception:
                return False
    return True


async def http_inspect(request):
    import aiohttp
    home, config, platform, base, key, token, policy, reasons = http_context(request)
    async with aiohttp.ClientSession(headers={"Authorization":"Bearer " + key}) as client:
        capabilities = await http_json(client, "GET", base + "/v1/capabilities")
        features = capabilities.get("features") or {}
        supported = all(features.get(name) is True for name in HTTP_FEATURES)
        if not supported:
            reasons.append("hermes_runs_unsupported")
        health = await http_json(client, "GET", base + "/health/detailed")
    absent = http_native_absent(home, health)
    queues = ((health.get("readiness") or {}).get("checks") or {}).get("background_queues") or {}
    counts = [health.get("active_agents"), *(queues.get(name) for name in
        ("active_api_runs", "process_completions", "active_delegations"))]
    require(type(health.get("gateway_busy")) is bool and health.get("gateway_state") == "running"
            and all(type(value) is int and value >= 0 for value in counts), "hermes_status_unverified")
    busy = health["gateway_busy"] or any(value > 0 for value in counts)
    disabled = not platform.enabled and absent
    sessions = http_channel_sessions(home, config.sessions_dir)
    # The platform intake currently proves only private-chat identity. Stopping
    # the native adapter must not silently strand an existing group/thread route.
    if any(session.get("chatType") != "private" or session.get("threadId") for session in sessions):
        reasons.append("hermes_group_intake_policy_unsupported")
    compatible = not reasons
    if platform.enabled:
        reasons.append("hermes_native_delivery_not_disabled")
    if not absent:
        reasons.append("hermes_native_adapter_still_present")
    if not token:
        reasons.append("hermes_telegram_token_unavailable")
    return {"kind":"hermes", "transport":HTTP_TRANSPORT, "compatible":compatible,
        "ready":not reasons, "reasonCodes":sorted(set(reasons)), "nativeDeliveryDisabled":disabled, "busy":busy,
        "hermesHome":str(home), "pythonPath":request["pythonPath"], "apiUrl":base,
        "botTokenSha256":hashlib.sha256(token.encode()).hexdigest() if token else None,
        "policy":policy, "sessions":sessions, "existingSessionsOnly":False}


def http_identity(request):
    chat_id, sender_id = str(request.get("chatId", "")), str(request.get("senderId", ""))
    require(chat_id.isdigit() and sender_id.isdigit() and chat_id == sender_id,
            "hermes_telegram_private_identity_invalid")
    require(not request.get("threadId") and request.get("chatType", "private") == "private",
            "hermes_group_intake_policy_unsupported")
    return chat_id, sender_id


async def http_resolve(request):
    chat_id, sender_id = http_identity(request)
    inspection = await http_inspect(request)
    require(inspection["ready"], "hermes_channel_not_ready")
    home, config, platform, base, key, token, policy, reasons = http_context(request)
    require(not reasons, "hermes_channel_policy_unsupported")
    intake = policy.get("intakeAllowedUserIds")
    allowed_intake = intake is None or "*" in intake or sender_id in intake
    authorized = allowed_intake and (policy["allowAllUsers"] or sender_id in policy["allowedUserIds"])
    if not authorized:
        reply = {"authorized":False, "transport":HTTP_TRANSPORT}
        if allowed_intake and policy["pairingEnabled"]:
            text = await asyncio.to_thread(http_pairing_reply, home, sender_id,
                str(request.get("senderName") or "")[:128])
            if text:
                reply["pairingText"] = text
        return reply
    matches = http_private_sessions(inspection["sessions"], chat_id, sender_id)
    if not matches and request.get("createSession") is True:
        from gateway.session import SessionSource
        from gateway.config import Platform
        source = SessionSource(platform=Platform.TELEGRAM, chat_id=chat_id, chat_type="dm",
            user_id=sender_id, user_name=str(request.get("senderName") or "")[:128] or None)
        matches = await asyncio.to_thread(http_create_native_session, home, config, source)
    if matches:
        matches[0] = {**matches[0], "compressionSessionIds":await asyncio.to_thread(
            http_compression_ids, home, matches[0]["nativeSessionId"])}
    return {"authorized":True, "transport":HTTP_TRANSPORT, "session":matches[0] if matches else None}


def http_live_session(home, session_id):
    from hermes_state import SessionDB
    db = SessionDB(home / "state.db", read_only=True)
    try:
        require(db.get_session(session_id) is not None, "hermes_session_not_found")
        resolved = db.get_compression_tip(session_id)
        require(isinstance(resolved, str) and bool(resolved) and db.get_session(resolved) is not None,
                "hermes_session_not_found")
        return resolved
    finally:
        db.close()


def http_compression_ids(home, session_id):
    from hermes_state import SessionDB
    db = SessionDB(home / "state.db", read_only=True)
    try:
        require(db.get_session(session_id) is not None, "hermes_session_not_found")
        # Official lineage excludes reset/branch/delegate boundaries. Require the
        # selected forward tip as well, so a competing branch cannot authorize an ID.
        tip = db.get_compression_tip(session_id)
        return [sid for sid in db.get_compression_lineage(session_id)
                if db.get_compression_tip(sid) == tip]
    finally:
        db.close()


def http_restore_native_peer(home, native, session_id):
    """Keep a verified compression continuation tagged as the original Telegram peer."""
    from hermes_state import SessionDB
    db = SessionDB(home / "state.db", read_only=True)
    try:
        require(session_id in db.get_compression_chain(native["nativeSessionId"]), "hermes_session_identity_changed")
        row = db.get_session(session_id)
        require(row is not None and row.get("session_key") in (None, native["sessionKey"]), "hermes_session_identity_changed")
        if row.get("source") == "telegram":
            return
        original = db.get_session(native["nativeSessionId"])
        require(original is not None and original.get("source") == "telegram", "hermes_session_identity_changed")
    finally:
        db.close()
    # API agents create compression children with source=api_server. The official
    # peer writer updates metadata only; it never rewrites messages or routing scopes.
    db = SessionDB(home / "state.db")
    try:
        require(session_id in db.get_compression_chain(native["nativeSessionId"]), "hermes_session_identity_changed")
        # Only the currently selected tip is repaired; a concurrent later turn
        # owns any newly advanced continuation and will validate it on its next admission.
        require(db.get_compression_tip(native["nativeSessionId"]) == session_id, "hermes_session_identity_changed")
        current = db.get_session(session_id)
        require(current is not None and current.get("session_key") in (None, native["sessionKey"]), "hermes_session_identity_changed")
        db.record_gateway_session_peer(session_id, source="telegram", session_key=native["sessionKey"],
            user_id=original.get("user_id") or native.get("senderId") or native["chatId"],
            chat_id=original.get("chat_id") or native["chatId"],
            chat_type=original.get("chat_type") or native.get("nativeOriginChatType") or "dm",
            thread_id=original.get("thread_id") or native.get("threadId"),
            display_name=original.get("display_name"), origin_json=original.get("origin_json"))
    finally:
        db.close()


class HttpSessionEvents:
    """The official session SSE uses named events, not the Runs JSON event field."""
    def __init__(self, session_id, validate_session=None, complete_session=None, terminal_session=None):
        self.session_id = session_id
        self.validate_session = validate_session or (lambda sid:sid == session_id)
        self.complete_session = complete_session or (lambda sid:None)
        self.terminal_session = terminal_session or (lambda:None)
        self.run_id = None
        self.sequence = 0
        self.final = None
        self.completed = False
        self.cancelled = False

    def accept(self, name, payload):
        require(isinstance(payload, dict), "hermes_invalid_sse_event")
        run_id = payload.get("run_id")
        require(isinstance(run_id, str) and 0 < len(run_id) <= 200, "hermes_invalid_sse_event")
        if self.run_id is None:
            require(name == "run.started" and payload.get("session_id") == self.session_id,
                    "hermes_session_identity_changed")
            self.run_id = run_id
            emit("run.accepted", run_id=run_id, session_id=self.session_id)
        require(run_id == self.run_id, "hermes_session_identity_changed")
        sequence = payload.get("seq")
        require(type(sequence) is int and sequence > 0, "hermes_invalid_sse_event")
        if sequence <= self.sequence:
            return False
        self.sequence = sequence
        native_id = payload.get("session_id")
        require(native_id == self.session_id or (name in ("assistant.completed", "run.completed")
            and self.validate_session(native_id)), "hermes_session_identity_changed")
        if name == "assistant.delta":
            require(payload.get("session_id") == self.session_id, "hermes_session_identity_changed")
            if isinstance(payload.get("delta"), str):
                emit("message.delta", delta=payload["delta"])
        elif name == "assistant.completed":
            self.complete_session(native_id)
            self.final = payload.get("content") if isinstance(payload.get("content"), str) else ""
        elif name in ("tool.started", "tool.completed", "tool.failed"):
            emit("tool.started" if name == "tool.started" else "tool.completed",
                 tool=str(payload.get("tool_name") or "tool"), failed=name == "tool.failed")
        elif name == "run.completed":
            require(self.final is not None, "hermes_final_snapshot_missing")
            self.completed = True
            emit("run.cancelled" if self.cancelled else "run.completed", **({} if self.cancelled else {"output":self.final}))
            return True
        elif name == "error":
            # The session API sets failed before emitting error. A failed turn
            # may already have compressed, without an assistant.completed frame.
            self.terminal_session()
            emit("run.failed")
            return True
        # tool.progress contains reasoning text; deliberately never relay it.
        return False


async def http_try_stop(client, base, run_id):
    import aiohttp
    async with client.post(base + "/v1/runs/" + quote(run_id, safe="") + "/stop",
            allow_redirects=False, timeout=aiohttp.ClientTimeout(total=15)) as response:
        data = await response.json()
        # Session SSE queues run.started before _run_agent registers its agent.
        # This exact 409 is a retryable stop window, never permission to re-prompt.
        if response.status == 409 and isinstance(data,dict) and (data.get("error") or {}).get("code") == "run_not_active":
            return False
        require(response.status < 300 and isinstance(data,dict), "hermes_stop_unknown")
        return True


async def http_run(request):
    import aiohttp
    require(request.get("nativeDeliveryDisabled") is True, "hermes_native_delivery_not_disabled")
    inspection = await http_inspect(request)
    require(inspection["ready"], "hermes_channel_not_ready")
    home, config, platform, base, key, token, policy, reasons = http_context(request)
    session_id = request.get("sessionId")
    session_key = request.get("sessionKey")
    require(isinstance(session_id, str) and isinstance(session_key, str), "invalid_hermes_native_request")
    native = next((row for row in inspection["sessions"] if row["sessionKey"] == session_key), None)
    require(native is not None, "hermes_session_identity_changed")
    lineage = await asyncio.to_thread(http_compression_ids, home, native["nativeSessionId"])
    require(session_id in lineage, "hermes_session_identity_changed")
    current = await asyncio.to_thread(http_live_session, home, session_id)
    await asyncio.to_thread(http_restore_native_peer, home, native, current)
    body = {"message":request["message"]}
    if request.get("model") and request.get("provider"):
        # Confirmed model lock wins over an old native /model cache and global defaults.
        body.update(model=request["model"], provider=request["provider"], require_model_lock=True)
    headers = {"Authorization":"Bearer " + key, "X-Hermes-Session-Key":session_key}
    events = HttpSessionEvents(current, lambda sid:sid in http_compression_ids(home, current),
        lambda sid:http_restore_native_peer(home, native, sid),
        lambda:http_restore_native_peer(home, native, http_live_session(home, current)))
    async with aiohttp.ClientSession(headers=headers, read_bufsize=4 * 1024 * 1024) as client:
        existing = await http_json(client, "GET", base + "/api/sessions/" + quote(current, safe=""))
        require((existing.get("session") or {}).get("id") == current, "hermes_session_not_found")
        emit("ready", session_id=current)
        if (await command()).get("action") != "submit":
            emit("run.cancelled")
            return
        control = asyncio.create_task(command())
        stop_sent = False
        next_stop_attempt = 0
        try:
            # This POST has no idempotency contract. The durable codeg turn claim
            # owns exactly-one submission; a timeout/EOF is never retried.
            async with client.post(base + "/api/sessions/" + quote(current, safe="") + "/chat/stream",
                    json=body, allow_redirects=False, timeout=aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=45)) as response:
                require(response.status < 300, "hermes_admission_rejected")
                require(response.headers.get("X-Hermes-Session-Id") == current, "hermes_session_identity_changed")
                name, data = "", []
                line_task = asyncio.create_task(response.content.readline())
                try:
                    while True:
                        ready, _ = await asyncio.wait((line_task, control), timeout=1,
                                                      return_when=asyncio.FIRST_COMPLETED)
                        if control in ready:
                            control.result()
                            events.cancelled = True
                            # Do not close SSE here: observe the actual terminal run.
                            control = asyncio.create_task(asyncio.Event().wait())
                        if events.cancelled and events.run_id and not stop_sent and time.monotonic() >= next_stop_attempt:
                            stop_sent = await http_try_stop(client, base, events.run_id)
                            next_stop_attempt = time.monotonic() + 0.25
                        if line_task not in ready:
                            continue
                        line = line_task.result()
                        if not line:
                            break
                        require(len(line) <= 8 * 1024 * 1024, "hermes_invalid_sse_event")
                        line = line.decode("utf-8").rstrip("\r\n")
                        if not line:
                            if data:
                                payload = json.loads("\n".join(data))
                                if events.accept(name, payload):
                                    return
                                if events.cancelled and events.run_id and not stop_sent and time.monotonic() >= next_stop_attempt:
                                    stop_sent = await http_try_stop(client, base, events.run_id)
                                    next_stop_attempt = time.monotonic() + 0.25
                            name, data = "", []
                        elif line.startswith("event:"):
                            name = line[6:].strip()
                        elif line.startswith("data:"):
                            data.append(line[5:].lstrip(" "))
                        line_task = asyncio.create_task(response.content.readline())
                finally:
                    line_task.cancel()
            await http_observe_after_disconnect(client, base, events)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await http_observe_after_disconnect(client, base, events)
        finally:
            control.cancel()


async def http_observe_after_disconnect(client, base, events):
    # Session-chat interrupts on disconnect. A known terminal state is still
    # observed by run ID, but we never start a replacement or invent final text.
    require(events.run_id is not None, "hermes_admission_unknown")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            status = await http_json(client, "GET", base + "/v1/runs/" + quote(events.run_id, safe=""), missing=True)
        except ValueError:
            status = None
        if status:
            require(status.get("run_id") == events.run_id, "hermes_session_identity_changed")
            if status.get("status") in ("completed", "failed", "cancelled", "interrupted"):
                # Only a confirmed terminal run authorizes this recovery. Unknown
                # status never rewrites peer metadata or retries the prompt.
                events.terminal_session()
            if status.get("status") in ("cancelled", "interrupted"):
                emit("run.cancelled")
                return
            if status.get("status") == "failed":
                emit("run.failed")
                return
            if status.get("status") == "completed":
                require(events.final is not None, "hermes_final_snapshot_missing")
                emit("run.cancelled" if events.cancelled else "run.completed", **({} if events.cancelled else {"output":events.final}))
                return
            if status.get("status") == "unknown":
                break
        await asyncio.sleep(2)
    raise ValueError("hermes_execution_unknown")
