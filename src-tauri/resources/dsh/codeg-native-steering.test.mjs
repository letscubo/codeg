// Pure fake event/inbox tests: no dsh app, model, session DB or network.
import assert from "node:assert/strict";
import test from "node:test";
import { PassThrough } from "node:stream";
import { nativeSteeringBridge, pinnedSteeringVersions } from "./codeg-headless-runner.mjs";

function harness(supported = true) {
  const handlers = new Map();
  const ctx = {
    on(name, fn) {
      const list = handlers.get(name) ?? new Set(); list.add(fn); handlers.set(name, list);
      return () => list.delete(fn);
    },
    emit(name, ...args) { for (const fn of handlers.get(name) ?? []) fn(...args); },
  };
  const input = new PassThrough(); const output = new PassThrough(); const records = [];
  output.on("data", (data) => records.push(...String(data).trim().split("\n").map(JSON.parse)));
  const queued = new Map(); const steered = []; let sequence = 0; let publishing = false;
  const agent = {
    session: { id: "session-exact" },
    phase: { kind: "running", turn: 1, abort: new AbortController() },
    inject(message) { queued.set(message.id, message); steered.push(message); },
    inbox: {
      remove(id) {
        assert.equal(publishing, false, "Session.append forbids reentrant inbox writes");
        const message = queued.get(id); if (message === undefined) return false;
        queued.delete(id); ctx.emit("agent/inbox/discarded", { agent, message }); return true;
      },
    },
  };
  const bridge = nativeSteeringBridge(ctx, agent, (value) => ({ ...value, id: `message-${++sequence}` }), "initial", { supported, input, output });
  const event = (type, data, session = agent.session) => {
    publishing = true;
    try { ctx.emit("session/event", session, { type, data }); } finally { publishing = false; }
  };
  const start = (turn = 1) => {
    agent.phase = { kind: "running", turn, abort: new AbortController() };
    event("turn/start", { turn }); event("step/start", { turn });
    event("user/message", { id: turn === 1 ? "initial" : `background-${turn}` });
  };
  const submit = (id = "request-1", turn = 1, rest = {}) => input.write(`${JSON.stringify({ type: "codeg_native_steer", protocol: 1, sessionId: agent.session.id, id, turn, text: "same text", ...rest })}\n`);
  const results = () => records.filter((value) => value.type === "codeg_native_result");
  const consume = (message, turn = 1) => {
    queued.delete(message.id); ctx.emit("agent/inbox/claimed", { agent, message, turn });
    event("step/start", { turn }); event("user/message", message);
  };
  const idle = () => { agent.phase = { kind: "idle" }; ctx.emit("agent/status", { agent, status: "idle" }); };
  const preStep = (messages, turn) => {
    let next = () => Promise.resolve({ kind: "enter", messages });
    for (const handler of [...handlers.get("agent/pre-step") ?? []].reverse()) {
      const inner = next; next = () => handler({ agent, turn }, inner);
    }
    return next();
  };
  return { agent, bridge, event, start, submit, records, steered, queued, results, consume, ctx, input, idle, preStep };
}

test("native bridge enables only the exact reviewed package versions", () => {
  assert.equal(pinnedSteeringVersions(Array(5).fill("0.1.6-alpha.2")), true);
  for (const versions of [[], Array(4).fill("0.1.6-alpha.2"), ["0.1.6-alpha.2", "0.1.6-alpha.2", "0.1.6-alpha.2", "0.1.6-alpha.2", "0.1.7"], Array(5).fill(undefined)]) assert.equal(pinnedSteeringVersions(versions), false);
  const h = harness(false); h.start(); h.submit();
  assert.deepEqual(h.records, []); assert.equal(h.steered.length, 0); h.bridge.dispose();
});

test("queued and claimed are not receipts; only same-native-turn user/message append confirms", () => {
  const h = harness(); h.start(); h.submit();
  assert.equal(h.steered.length, 1); assert.deepEqual(h.results(), []);
  const message = h.steered[0]; h.queued.delete(message.id);
  h.ctx.emit("agent/inbox/claimed", { agent: h.agent, message, turn: 1 });
  assert.deepEqual(h.results(), []);
  h.event("user/message", message, { id: "different-session" });
  assert.deepEqual(h.results(), []);
  h.event("user/message", message);
  assert.deepEqual(h.results().map((r) => [r.id, r.turn, r.status]), [["request-1", 1, "injected"]]);
  h.bridge.dispose();
});

test("turn end removes only the pending submission and a later native turn cannot confirm it", () => {
  const h = harness(); h.start(); h.submit();
  h.queued.set("subagent-system", { id: "subagent-system" });
  h.event("turn/end", { turn: 1 });
  assert.equal(h.results().length, 0, "turn/end observer must not mutate Session/inbox");
  h.idle();
  assert.deepEqual([...h.queued.keys()], ["subagent-system"]);
  assert.equal(h.results()[0].status, "not_injected");
  h.start(2); h.event("user/message", h.steered[0]);
  assert.equal(h.results().length, 1);
  h.submit("old-token", 1);
  assert.equal(h.results().at(-1).status, "not_injected");
  assert.equal(h.steered.length, 1); h.bridge.dispose();
});

test("claimed but rejected step remains unconfirmed and is never replayed", () => {
  const h = harness(); h.start(); h.submit();
  const message = h.steered[0]; h.queued.delete(message.id);
  h.ctx.emit("agent/inbox/claimed", { agent: h.agent, message, turn: 1 });
  h.event("turn/end", { turn: 1 });
  h.idle();
  assert.equal(h.results()[0].status, "unconfirmed");
  h.start(2); h.event("user/message", message); h.submit();
  assert.equal(h.steered.length, 1); assert.equal(h.results().at(-1).status, "unconfirmed");
  h.bridge.dispose();
});

test("identical text uses distinct message ids, repeated request id never steers twice", () => {
  const h = harness(); h.start(); h.submit("one"); h.submit("two"); h.submit("one");
  assert.equal(h.steered.length, 2); assert.notEqual(h.steered[0].id, h.steered[1].id);
  h.consume(h.steered[1]); h.consume(h.steered[0]);
  assert.deepEqual(h.results().map((r) => [r.id, r.status]), [["two", "injected"], ["one", "injected"]]);
  h.bridge.dispose();
});

test("idle/aborting/not-started/wrong-session/oversize requests never call steer", () => {
  const h = harness(); h.submit("before"); h.start();
  h.submit("foreign", 1, { sessionId: "other" });
  h.submit("oversize", 1, { text: "😀".repeat(16001) });
  h.agent.phase.abort.abort(); h.submit("aborted");
  h.agent.phase = { kind: "idle" }; h.submit("idle");
  assert.equal(h.steered.length, 0);
  assert.ok(h.results().every((r) => r.status === "not_injected")); h.bridge.dispose();
});

test("wrong native turn consumption is unconfirmed, never same-turn injected", () => {
  const h = harness(); h.start(); h.submit();
  h.consume(h.steered[0], 2);
  assert.equal(h.results()[0].status, "unconfirmed"); h.bridge.dispose();
});

test("disposal before consume removes pending user input and stops control processing", () => {
  const h = harness(); h.start(); h.submit(); h.bridge.dispose(); h.submit("later");
  assert.equal(h.steered.length, 1); assert.equal(h.queued.size, 0);
  assert.equal(h.results()[0].status, "not_injected");
  assert.equal(h.records.at(-2).supported, false);
});

test("awaited turn-stopping owner hook removes only owned pending input outside Session.append", () => {
  const h = harness(); h.start(); h.submit();
  h.queued.set("system", { id: "system" });
  h.ctx.emit("agent/turn-stopping", { agent: h.agent, turn: 1, signal: h.agent.phase.abort.signal });
  assert.deepEqual([...h.queued.keys()], ["system"]);
  assert.equal(h.results()[0].status, "not_injected");
  h.submit("too-late"); assert.equal(h.steered.length, 1);
  h.event("turn/end", { turn: 1 }); h.bridge.dispose();
});

test("next native turn admission filters stale exact ids while preserving background/system input", async () => {
  const h = harness(); h.start(); h.submit();
  const stale = h.steered[0]; h.queued.delete(stale.id);
  h.event("turn/end", { turn: 1 });
  h.start(2); // Normal background continuation does not need an idle transition.
  h.ctx.emit("agent/inbox/claimed", { agent: h.agent, message: stale, turn: 2 });
  const background = { id: "background-report" }; const system = { id: "system-context" };
  const decision = await h.preStep([stale, background, system], 2);
  assert.deepEqual(decision.messages, [background, system]);
  assert.equal(h.results()[0].status, "unconfirmed");
  assert.ok(h.results().every((result) => result.status !== "injected"));
  h.bridge.dispose();
});
