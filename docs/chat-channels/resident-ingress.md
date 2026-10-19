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
  "input": { "kind": "text", "text": "/model provider/model" },
  "context": {
    "entry": "managed",
    "jobId": "durable-platform-job-id",
    "expectedSessionKey": "codeg:123",
    "sessionKey": "codeg:123"
  }
}
```

Native commands use `input: { "kind": "command", "name": "/model", "arguments": "provider/model" }`.
Text commands must occupy the whole command input. A normal sentence mentioning `/model` stays a prompt.
Only the `model` command is implemented; `/new` is not intercepted.

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

Codeg's execution guard orders connected and reconnecting ACP prompts against a model callback.
The guard remains logically active after an unknown model result until a read-only receipt proves the outcome.
The group-task `accepted` response acknowledges fan-out dispatch; it does not serialize the entire group until all members finish.
Retained runtimes can still have their original Web/native clients. Those clients do not acquire codeg's channel queue; this contract does not claim global ordering of every external runtime operation.

## Retained runtimes

| Runtime | Execution | Required deployment configuration |
| --- | --- | --- |
| Existing OpenClaw | Original CLI ACP with exact session key and no reset | `CODEG_LEGACY_OPENCLAW_CLI` = absolute path to the original installed CLI; preserve original HOME/config |
| Existing Hermes Slack | Original Runs API with saved Slack history and explicit model/provider | Existing local API endpoint and credentials supplied by signed platform callback |
| Existing Hermes native Telegram | Original canonical Gateway, existing native session ID | `CODEG_LEGACY_HERMES_HOME`, `CODEG_LEGACY_HERMES_PYTHON` = absolute original paths |

`POST /api/legacy_channel_inspect` takes `{ "kind": "hermes", "platform": "telegram" }` (or `openclaw`).
It reads the original runtime's policy, session inventory and live adapter state. It does not stop, reconfigure or upgrade that runtime.
Native Telegram adoption currently covers verified existing private text sessions only. Complex/ambiguous routes, topics and new native chats fail closed.
The original Telegram adapter must be fully stopped before adoption; otherwise both the runtime and codeg could send replies.

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
