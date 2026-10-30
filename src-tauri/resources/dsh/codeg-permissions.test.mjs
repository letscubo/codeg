import assert from "node:assert/strict";
import test from "node:test";
import { apply, pendingInput } from "./codeg-permissions.mjs";

test("native approval requires an exact boolean response and never grants a persistent permission", async () => {
  const handlers = new Map();
  const output = [];
  const original = process.stdout.write;
  process.stdout.write = line => { output.push(JSON.parse(line)); return true; };
  try {
    apply({ on: (name, handler) => handlers.set(name, handler) });
    const controller = new AbortController();
    const result = handlers.get("approval/request")({ toolName: "shell", callId: "call-1", signal: controller.signal });
    const [{ requestId }] = output;
    let settled = false;
    result.then(() => { settled = true; });
    process.stdin.emit("data", Buffer.from(JSON.stringify({ type: "codeg_permission_response", requestId: "other-session", approved: true }) + "\n"));
    process.stdin.emit("data", Buffer.from(JSON.stringify({ type: "codeg_permission_response", requestId, approved: "true" }) + "\n"));
    await Promise.resolve();
    assert.equal(settled, false);
    const line = JSON.stringify({ type: "codeg_permission_response", requestId, approved: true }) + "\n";
    // Real pipe reads may split one response into multiple chunks.
    process.stdin.emit("data", Buffer.from(line.slice(0, 20)));
    process.stdin.emit("data", Buffer.from(line.slice(20)));
    assert.equal(await result, "allowed-once");
    const second = handlers.get("approval/request")({ toolName: "shell", signal: controller.signal });
    process.stdin.emit("data", Buffer.from(line)); // stale grant cannot approve a new action
    controller.abort();
    assert.equal(await second, "cancelled");
    handlers.get("dispose")();
  } finally { process.stdout.write = original; handlers.get("dispose")?.(); }
});

test("native denial and disconnect both fail closed", async () => {
  const handlers = new Map(), output = [];
  const original = process.stdout.write;
  process.stdout.write = line => { output.push(JSON.parse(line)); return true; };
  try {
    apply({ on: (name, handler) => handlers.set(name, handler) });
    const denied = handlers.get("approval/request")({ toolName: "shell" });
    process.stdin.emit("data", Buffer.from(JSON.stringify({ type: "codeg_permission_response", requestId: output[0].requestId, approved: false }) + "\n"));
    assert.equal(await denied, "rejected");
    const abandoned = handlers.get("approval/request")({ toolName: "edit" });
    handlers.get("dispose")();
    assert.equal(await abandoned, "cancelled");
  } finally { process.stdout.write = original; handlers.get("dispose")?.(); }
});

test("approval details come from the exact call in this turn", () => {
  const events = [{ type: "tool/call", data: { name: "shell", callId: "old", arguments: '{"command":"old"}' } },
    { type: "turn/start" }, { type: "tool/call", data: { name: "shell", callId: "now", arguments: '{"command":"pwd"}' } }];
  const agent = { session: { snapshotEvents: () => events } };
  assert.deepEqual(pendingInput({ agent, toolName: "shell", callId: "now" }), { command: "pwd" });
  assert.equal(pendingInput({ agent, toolName: "shell", callId: "old" }), undefined);
  assert.equal(pendingInput({ agent, toolName: "edit", callId: "now" }), undefined);
});
