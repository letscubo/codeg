// Run: node --test src-tauri/resources/dsh/codeg-headless-runner.test.mjs
import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, mkdirSync, writeFileSync, symlinkSync, realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { descendantsOf, entryOf, isBusy, launcherRoot, packageDir, settle, summarize } from "./codeg-headless-runner.mjs";

/** A fake Agent: `run(ms)` makes it busy for `ms`, then idle. */
function fakeAgent(id) {
  const agent = {
    id,
    phase: { kind: "idle" },
    inbox: { hasPending: false },
    idle: Promise.resolve(),
    run(ms, then) {
      agent.phase = { kind: "running" };
      agent.idle = new Promise((resolve) =>
        setTimeout(() => {
          agent.phase = { kind: "idle" };
          resolve();
          then?.();
        }, ms),
      );
    },
    whenIdle() {
      return agent.idle;
    },
  };
  return agent;
}

function registry(owners) {
  const agents = [...owners.keys()];
  return { list: () => agents, isOwnedBy: (id, owner) => agents.find((a) => a.id === id) && owners.get(agents.find((a) => a.id === id)) === owner };
}

test("isBusy: running or queued input", () => {
  assert.equal(isBusy({ phase: { kind: "idle" }, inbox: { hasPending: false } }), false);
  assert.equal(isBusy({ phase: { kind: "running" } }), true);
  assert.equal(isBusy({ phase: { kind: "idle" }, inbox: { hasPending: true } }), true);
  assert.equal(isBusy(undefined), false);
});

test("descendantsOf walks owner links transitively and stops at the root", () => {
  const parent = fakeAgent("p");
  const child = fakeAgent("c");
  const grandchild = fakeAgent("g");
  const stranger = fakeAgent("s");
  const owners = new Map([[parent, undefined], [child, parent], [grandchild, child], [stranger, undefined]]);
  assert.deepEqual(descendantsOf(registry(owners), parent), [child, grandchild]);
});

test("settle waits for a busy child and the parent's follow-up it triggers", async () => {
  const parent = fakeAgent("p");
  const child = fakeAgent("c");
  const owners = new Map([[parent, undefined], [child, parent]]);
  const order = [];
  // Parent's first turn ends quickly; the child works longer, then its report
  // wakes the parent for a follow-up turn.
  parent.run(10, () => order.push("parent turn 1"));
  child.run(60, () => {
    order.push("child done");
    parent.inbox.hasPending = true;
    setTimeout(() => {
      parent.inbox.hasPending = false;
      parent.run(30, () => order.push("parent follow-up"));
    }, 5);
  });
  const ok = await settle(registry(owners), parent, 5000, 50);
  assert.equal(ok, true);
  assert.deepEqual(order, ["parent turn 1", "child done", "parent follow-up"]);
});

test("settle gives up at the budget when a child never finishes", async () => {
  const parent = fakeAgent("p");
  const child = fakeAgent("c");
  child.run(1_000);
  const started = Date.now();
  const ok = await settle(registry(new Map([[parent, undefined], [child, parent]])), parent, 150, 20);
  assert.equal(ok, false);
  assert.ok(Date.now() - started < 1500);
});

test("settle returns at once when nothing is running", async () => {
  const parent = fakeAgent("p");
  assert.equal(await settle(registry(new Map([[parent, undefined]])), parent, 1000, 10), true);
});

test("summarize keeps the last assistant text and the last turn reason", () => {
  const events = [
    { type: "turn/start" },
    { type: "assistant/message", data: { message: { content: [{ type: "text", text: "started" }] } } },
    { type: "turn/end", data: { reason: { kind: "completed" } } },
    { type: "turn/start" },
    { type: "assistant/message", data: { message: { content: [{ type: "text", text: "BG_OK delivered" }] } } },
    { type: "turn/end", data: { reason: { kind: "completed" } } },
  ];
  const session = { seq: events.length, eventAt: (i) => events[i] };
  assert.deepEqual(summarize(session, 0), { text: "BG_OK delivered", reason: { kind: "completed" } });
});

test("launcherRoot / packageDir / entryOf follow a real npm global layout", () => {
  const root = realpathSync(mkdtempSync(join(tmpdir(), "codeg-runner-")));
  const dsh = join(root, "lib", "node_modules", "@deepseek-ai", "dsh");
  mkdirSync(join(dsh, "lib"), { recursive: true });
  writeFileSync(join(dsh, "package.json"), JSON.stringify({ name: "@deepseek-ai/dsh" }));
  writeFileSync(join(dsh, "lib", "bin.js"), "");
  mkdirSync(join(root, "bin"));
  symlinkSync(join(dsh, "lib", "bin.js"), join(root, "bin", "dsh"));
  // nested dependency with an exports map, and a hoisted one with only `main`
  const nested = join(dsh, "node_modules", "@deepseek-ai", "dsh-llm");
  mkdirSync(join(nested, "lib"), { recursive: true });
  writeFileSync(join(nested, "package.json"), JSON.stringify({ exports: { ".": { import: "./lib/index.js", types: "./x.d.ts" } } }));
  const hoisted = join(root, "lib", "node_modules", "@deepseek-ai", "dsh-agent");
  mkdirSync(hoisted, { recursive: true });
  writeFileSync(join(hoisted, "package.json"), JSON.stringify({ main: "lib/main.js" }));

  assert.equal(launcherRoot(join(root, "bin", "dsh")), dsh);
  assert.equal(packageDir(dsh, "dsh-llm"), nested);
  assert.equal(packageDir(dsh, "dsh-agent"), hoisted);
  assert.equal(entryOf(nested), join(nested, "lib", "index.js"));
  assert.equal(entryOf(hoisted), join(hoisted, "lib", "main.js"));
  assert.throws(() => packageDir(dsh, "dsh-missing"));
});
