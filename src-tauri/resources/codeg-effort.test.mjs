// Reviewed no-database tests: pure metadata/selection and private temp JSON only.
import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { samplingPreservesSelection, verifiedWireLevels } from "./codeg-effort-wire.mjs";
import { nativeDefaultThinkingLevel, readonlySettingsStorage } from "./pi/effort-probe.mjs";
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

test("Pi 1.3 per-model defaults precede global settings while 0.84 remains compatible", () => {
  const settings = {
    getModelThinkingLevel(provider, model) {
      assert.equal(provider, "myclaw");
      return { first: "high", second: "off" }[model];
    },
    getDefaultThinkingLevel: () => "low",
  };
  assert.equal(nativeDefaultThinkingLevel(settings, "myclaw", "first", "medium"), "high");
  assert.equal(nativeDefaultThinkingLevel(settings, "myclaw", "second", "medium"), "off");
  assert.equal(nativeDefaultThinkingLevel(settings, "myclaw", "other", "medium"), "low");
  assert.equal(nativeDefaultThinkingLevel({ getDefaultThinkingLevel: () => "high" }, "myclaw", "first", "medium"), "high");
  assert.equal(nativeDefaultThinkingLevel({ getDefaultThinkingLevel: () => undefined }, "myclaw", "first", "medium"), "medium");
});

test("Pi effective sampling preserves exact model and effort without blocking unrelated options", () => {
  const input = { runtime: "pi", provider: "myclaw", api: "openai-completions", map,
    compat: { supportsReasoningEffort: true }, nativeLevels: native, modelId: "exact" };
  assert.deepEqual(verifiedWireLevels({ ...input, samplingParams: { temperature: 0.7, top_p: 0.9, model: "exact" } }), ["off", "low", "high", "max"]);
  assert.deepEqual(verifiedWireLevels({ ...input, samplingParams: { reasoning_effort: "high" } }), ["high"]);
  assert.deepEqual(verifiedWireLevels({ ...input, samplingParams: { reasoning_effort: "high" }, samplingParamsByThinkingLevel: { low: { reasoning_effort: "low" } } }), ["low", "high"]);
  assert.deepEqual(verifiedWireLevels({ ...input, samplingParamsByThinkingLevel: { low: { reasoning_effort: "high" }, off: { reasoning_effort: null } } }), ["high", "max"]);
  assert.deepEqual(verifiedWireLevels({ ...input, samplingParams: { model: "wrong-model" } }), []);
  assert.deepEqual(verifiedWireLevels({ ...input, samplingParams: { model: "wrong-model" }, samplingParamsByThinkingLevel: { high: { model: "exact" } } }), ["high"]);
  assert.deepEqual(verifiedWireLevels({ ...input, samplingParamsByThinkingLevel: { high: { reasoning_effort: undefined } } }), ["off", "low", "max"]);
  assert.equal(samplingPreservesSelection({ modelId: "plain", level: "off", samplingParams: { model: "wrong-model" } }), false);
});
