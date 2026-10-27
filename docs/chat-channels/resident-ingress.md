# Resident channel coordinator

MyClaw sends authenticated, normalized inputs to the resident `codeg-server`.
Deployment starts the service; receiving a message never installs or starts another agent runtime.

## HTTP contract

All endpoints use the existing codeg authentication middleware.

```json
POST /api/channel_submit
{
  "requestId": "durable-platform-job-id",
  "sessionRef": "stable-platform-conversation-scope",
  "source": { "channelType": "telegram", "accountId": "account-id" },
  "input": { "kind": "text", "text": "\\model provider/model" },
  "context": {
    "entry": "managed",
    "jobId": "durable-platform-job-id",
    "expectedSessionKey": "codeg:123",
    "sessionKey": "codeg:123"
  }
}
```

`\model` is the portable ordinary-text command: `\model`, `\model list 2`, or
`\model provider/model`. It does not require platform-native slash-command registration.
The former `!model` syntax is ordinary conversation. Existing `/model`, Telegram `/model@Bot`, and normalized native commands remain compatible.
Native commands use `input: { "kind": "command", "name": "model", "arguments": "provider/model" }`.
Text commands must occupy the whole command input. A normal sentence mentioning
`\model` or `/model` stays a prompt; codeg does not ask an agent to infer intent.
Only the `model` command is implemented; `\new` and `/new` are not intercepted.

Channel adapters authorize the sender, target App/Agent, and exact conversation
before submission. They remove only a verified mention of the target bot before
passing the command text. Delivery rules remain platform-specific: in a shared
channel the bot must actually receive the message and be the intended recipient.
The portable text syntax cannot grant a bot access to unreadable group messages.

`sessionRef` must be the same stable scope used for ordinary messages to that
Agent/session, including existing thread/topic mappings. Do not derive a new
session from the command spelling or from the sender of a shared conversation.
If a new conversation has several possible Agents, the platform requests a
selection before changing a model. Ambiguous targets never fall back to the
latest session. Retained sessions keep their original mapping; a visible reply
thread does not itself prove that the runtime has a separate session.

`POST /api/channel_operation` and `POST /api/channel_cancel` take `{ "requestId": "..." }`.
Repeated IDs with matching input return the original operation; a conflicting payload is rejected.
The stable session scope does not change when the first runtime conversation is created.

## Signed platform callback

The existing configured MyClaw webhook identifies the fixed `/api/codeg/channel-input` origin.
Calls use the existing HMAC protocol; request data cannot choose an arbitrary callback URL.

- `resolve`: read-only lookup of the latest authorized session before a queued managed model command.
- `dispatch`: platform verifies the original job, sender/account/VM/session and parsed command; returns a turn, retained-runtime request, or command reply.
- `complete`: carries compact receipt and safe job references. The platform owns existing history and delivery state.
- `status`: read-only recovery check; never reapplies a model or prompts an agent.

Ordinary messages and model commands share the same per-`sessionRef` FIFO. A
queued model command runs after the current operation and before the next prompt;
it does not change a running turn mid-flight. This applies to both managed
`kind=all` instances and retained Hermes/OpenClaw channel inputs.

Codeg's execution guard orders connected and reconnecting ACP prompts against a model callback.
The guard remains logically active after an unknown model result until a read-only receipt proves the outcome.
The group-task `accepted` response acknowledges fan-out dispatch; it does not serialize the entire group until all members finish.
Retained runtimes can still have their original Web/native clients. Those clients do not acquire codeg's channel queue; this contract does not claim global ordering of every external runtime operation.

## Retained runtimes

| Runtime | Execution | Required deployment configuration |
| --- | --- | --- |
| Existing OpenClaw | Original CLI ACP with exact session key and no reset | `CODEG_LEGACY_OPENCLAW_CLI` = absolute path to the original installed CLI; preserve original HOME/config |
| Existing Hermes Slack | Original Runs API with saved Slack history and explicit model/provider | Existing local API endpoint and credentials supplied by signed platform callback |
| Existing Hermes native Telegram | Original HTTP session streaming API with retained native session ID | `CODEG_LEGACY_HERMES_HOME`, `CODEG_LEGACY_HERMES_PYTHON` = absolute original paths |

`POST /api/legacy_channel_inspect` takes `{ "kind": "hermes", "platform": "telegram" }` (or `openclaw`).
It reads the original runtime's policy, session inventory and live adapter state. It does not stop, reconfigure or upgrade that runtime.
Hermes inspection reports `compatible` before handoff and `ready` after the original adapter is stopped. The `legacyHermesTelegramVersion: 1` capability requires the HTTP session stream, request-scoped model lock and run stop/status APIs. Compatibility is verified against the running APIs and policy, not a version label.
Hermes supports existing and new private text conversations. `POST /api/legacy_channel_resolve` receives `{ "kind": "hermes", "platform": "telegram", "chatId": "...", "senderId": "...", "createSession": true }`; the runtime checks its current allowlist and PairingStore before returning or creating the native session. An unapproved sender receives the original pairing response. Group/topic and unsupported policy configurations fail closed. OpenClaw adoption still requires an existing private session.
New Telegram routing entries use a separate `.codeg-telegram` routing scope in the same Hermes database; the native gateway's original routing scope stays read-only. The bound native ID survives official compression lineage changes. A reset or unrelated session cannot replace it. Runtime API credentials are read locally from the validated Gateway process and never returned to the platform.
The original Telegram adapter must be fully stopped before adoption; otherwise both the runtime and codeg could send replies.

Hermes uses `POST /api/sessions/{id}/chat/stream` exactly once per admitted codeg operation. An explicit model/provider pair uses `require_model_lock`; tool events drive loading, assistant events drive text, and reasoning progress is filtered. The original HTTP API does not offer prompt idempotency, so interrupted or uncertain submissions are not retried. Cancellation targets the same observed run ID and waits for its terminal event.

Telegram outbound accepts optional `messageThreadId`, taken from the authorized
incoming message's `message_thread_id`. New messages, typing, supported drafts,
and durable receipt targets retain it; omission preserves existing private-chat
requests. Invalid non-positive IDs are rejected before delivery admission.
`get_relay_capabilities` advertises `telegramMessageThreadVersion: 1`. Before
submitting a topic target, the platform requires this capability so an older
codeg cannot ignore the field and send to the main group instead.
Telegram's [`sendMessageDraft`](https://core.telegram.org/bots/api#sendmessagedraft)
only supports private chats. The relay skips drafts for negative group/channel
[Bot API IDs](https://core.telegram.org/api/bots/ids); group loading uses native
typing, then the existing streamed message edits and tool status on that message.
It does not create a separate status-only message. Private chats, including their
topics, retain draft loading. A channel's unsupported typing action remains
best effort and does not fail message delivery.

The relay uses the existing Telegram typing/draft and Slack loading/streaming implementation. Thinking records remain excluded from visible response text.
Legacy Slack `thread_message` may suppress `[[MYCLAW_NO_REPLY]]` without exposing a partial sentinel.

## Persistence and recovery

- Operation identity, digest, compact state and safe callback references live in existing SQLite metadata. Inputs and credentials are not saved there.
- The existing relay outbox owns the final result body; operation records do not duplicate it.
- Completion retries scan the operation history only at startup, then work from pending receipt references.
- Process restart or uncertain execution/delivery becomes `unknown`; neither the agent nor the output is replayed automatically.
- A later verified receipt can resolve the operation. A platform-confirmed execution with uncertain output retains `deliveryUnknown`.
- The activity API includes queued/unknown operations. MyClaw applies instance sleep/wake policy only to `kind=all`; retained Hermes/OpenClaw are not enrolled in that lifecycle.

See MyClaw `docs/channels-resident-codeg.md` for authenticated prepare/activate/adopt endpoints and deployment acceptance steps.

## WhatsApp Agent Platform (new managed instances)

`get_relay_capabilities` advertises `whatsappAgentPlatformVersion: 1`,
`whatsappIncrementalMessages: true`, and the `whatsapp_agent` kind. This is
incremental delivery as **separate messages**, not an editable native stream:
the official Agent Platform v1 has no message-edit, draft, or streaming endpoint.

The platform binds an individual MyClaw Agent to the numeric `entry.id` verified
by the official updates API. It retains the exact `user:...` sender and inbound
`wamid...` message ID. After durable enqueue and instance wake-up, ordinary text
and `\model` use the existing `channel_submit` queue and exact session binding.
`source.channelType` and `register_channel_callback.channelType` are both
`whatsapp_agent`; its private callback origin does not replace Telegram or Slack.

The prompt/command reply outbound contract is:

```json
{
  "kind": "whatsapp_agent",
  "turnId": "durable-platform-turn-id",
  "deliveryVersion": 2,
  "resultFormat": "compact-v1",
  "resultCallbackTargetId": "registered-channel-target-id",
  "apiToken": "opaque-agent-api-key",
  "agentId": "123456789",
  "recipient": "user:987654321",
  "inboundMessageId": "wamid.inbound"
}
```

Tokens exist only in the HTTP worker's memory. Receipts use `relay_kind` =
`whatsapp_agent`, target `{ "agent_id": "123456789", "recipient": "user:987654321" }`,
and opaque `segments[].message_key` IDs. Compact receipts retain one final body.

Native typing refreshes about every 20 seconds while processing. Each message
clears typing; it is refreshed while generation or normal final delivery continues.
Normal completion stops refreshing after the last queued chunk. Failed,
cancel-pending, cancelled and uncertain turns stop refreshing immediately. There is no stop-typing
API: a final reply clears it, or it expires within the documented 25 seconds.
Only main assistant text is delivered; reasoning and tool output are excluded.
Completed paragraphs/sentences are coalesced, with a conservative 3800 UTF-16
unit ceiling per message. Partial unpunctuated text waits for the size bound or
terminal flush. Both API endpoint quotas are independently limited per Agent;
sends are serialized with at least six seconds between attempts.

Only explicit 429 and 503/error.code 131016 non-acceptance can retry, with bounded
backoff. Network loss, unreadable success bodies, and other server failures are
unknown delivery outcomes and never replay automatically. All WhatsApp relay
receipts set `fallback_allowed: false`; platform reconciliation must not send the
whole answer again after partial delivery. Existing durable turn-ID admission
and restart-unknown behavior apply unchanged.

API reference: https://www.whatsapp.com/developer/WhatsApp-Agent-Platform-Developer-Manual.pdf
(version 1, 25 August 2026; request/response, retry, typing and rate-limit sections).
