// Reviewed no-database tests: pure metadata/selection and private temp JSON only.
import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { verifiedWireLevels } from "./codeg-effort-wire.mjs";
import { readonlySettingsStorage } from "./pi/effort-probe.mjs";
import { effortCapabilities, turnModelSelection } from "./dsh/codeg-headless-runner.mjs";

const native = ["off", "low", "medium", "high", "max"];
const map = { off: "none", low: "low", medium: null, high: "high", max: "max" };
const openai = { supportsReasoningEffort: true, thinkingFormat: "openai" };
function context(overrides = {}) {
  let requests = 0;
  const section = { providers: { myclaw: {
    api: "openai-completions", models: [{ id: "exact", reasoningEfforts: map, compat: openai }],
  } } };
  const llm = {
    async resolveModelInfo(provider, model) {
      if (provider !== "myclaw" || model !== "exact") throw new Error("unknown exact tuple");
      return { reasoning: { efforts: native.map((id) => ({ id })) } };
    },
    async resolveCallConfig(selection) { requests++; return selection; },
  };
  const services = { llm, settings: { get: () => section }, ...overrides };
  return { get: (name) => services[name], section, requests: () => requests };
}

test("wire identity rejects aliases, absent off mapping and missing protocol knobs", () => {
  const input = { runtime: "dsh", provider: "myclaw", api: "openai-completions", map, compat: openai, nativeLevels: native };
  assert.deepEqual(verifiedWireLevels(input), ["off", "low", "high", "max"]);
  assert.deepEqual(verifiedWireLevels({ ...input, map: { ...map, low: "high", off: undefined } }), ["high", "max"]);
  assert.deepEqual(verifiedWireLevels({ ...input, compat: {} }), []);
  assert.deepEqual(verifiedWireLevels({ ...input, map: undefined }), []);
});

test("Pi projected OpenAI schema omits thinkingFormat while DSH requires it", () => {
  const input = { provider: "myclaw", api: "openai-completions", map, compat: { supportsReasoningEffort: true }, nativeLevels: native };
  assert.deepEqual(verifiedWireLevels({ ...input, runtime: "pi" }), ["off", "low", "high", "max"]);
  assert.deepEqual(verifiedWireLevels({ ...input, runtime: "dsh" }), []);
  assert.deepEqual(verifiedWireLevels({ ...input, runtime: "pi", compat: { supportsReasoningEffort: true, thinkingFormat: "deepseek" } }), []);
});

test("Anthropic platform effort requires adaptive dispatch and rejects forced high", () => {
  const input = { runtime: "pi", provider: "myclaw-anthropic", api: "anthropic-messages", map, nativeLevels: native };
  assert.deepEqual(verifiedWireLevels({ ...input, compat: { forceAdaptiveThinking: true } }), ["off", "low", "high", "max"]);
  assert.deepEqual(verifiedWireLevels({ ...input, compat: {} }), []);
  assert.deepEqual(verifiedWireLevels({ ...input, compat: { forceAdaptiveThinking: true, supportsMidConvoEffort: true } }), []);
});

test("DSH capabilities inspect exact models without creating an Agent or submitting a request", async () => {
  const ctx = context({ agents: new Proxy({}, { get() { throw new Error("must not use Agents"); } }) });
  const result = await effortCapabilities(ctx, [{ model: "exact", provider: "myclaw" }, { model: "absent", provider: "myclaw" }], "0.1.6-alpha.2");
  assert.deepEqual(result.models[0].levels, ["none", "low", "high", "max"]);
  assert.deepEqual(result.models[1], { model: "absent", provider: "myclaw", known: false, levels: [] });
  assert.equal(ctx.requests(), 0);
});

test("DSH strict selection overrides saved route, rejects alias and clear removes inherited effort", async () => {
  const ctx = context();
  const saved = { provider: "old", model: "old", reasoningEffort: "max" };
  const defaults = { currentSelection: () => saved };
  const selected = await turnModelSelection(defaults, { provider: "myclaw", model: "exact", reasoningEffort: "off" }, ctx.get("llm"), ctx);
  assert.deepEqual(selected, { provider: "myclaw", model: "exact", reasoningEffort: "off" });
  assert.equal(saved.reasoningEffort, "max");
  const cleared = await turnModelSelection(defaults, { provider: "myclaw", model: "exact", reasoningEffort: null }, ctx.get("llm"), ctx);
  assert.deepEqual(cleared, { provider: "myclaw", model: "exact" });
  assert.equal(Object.hasOwn(cleared, "reasoningEffort"), false);
  ctx.section.providers.myclaw.models[0].reasoningEfforts = { ...map, low: "high" };
  await assert.rejects(turnModelSelection(defaults, { provider: "myclaw", model: "exact", reasoningEffort: "low" }, ctx.get("llm"), ctx));
  assert.equal(await turnModelSelection(defaults, {}, ctx.get("llm"), ctx), saved);
});

test("Pi settings backend reads defaults without writing files or lock directories", () => {
  const root = mkdtempSync(join(tmpdir(), "codeg-pi-readonly-test-"));
  const agentDir = join(root, "agent");
  mkdirSync(agentDir);
  const file = join(agentDir, "settings.json");
  writeFileSync(file, '{"defaultThinkingLevel":"high"}');
  const storage = readonlySettingsStorage(root, agentDir, ".pi");
  storage.withLock("global", (text) => { assert.equal(JSON.parse(text).defaultThinkingLevel, "high"); });
  assert.throws(() => storage.withLock("global", () => '{"defaultThinkingLevel":"low"}'));
  assert.equal(readFileSync(file, "utf8"), '{"defaultThinkingLevel":"high"}');
  assert.equal(existsSync(`${file}.lock`), false);
});
