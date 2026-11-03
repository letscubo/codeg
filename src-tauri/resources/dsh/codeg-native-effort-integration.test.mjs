// No runtime SDK/Agent imports: materialized owned resources, a stub launcher
// manifest and metadata-only fake services. Stdin access is an explicit error.
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { pathToFileURL } from "node:url";
import test from "node:test";

function profile(withWire = true) {
  const root = mkdtempSync(join(tmpdir(), "codeg-dsh-native-effort-"));
  const home = join(root, "dsh-home");
  mkdirSync(join(home, "plugins"), { recursive: true });
  const runner = join(home, "plugins", "codeg-headless-runner.mjs");
  writeFileSync(runner, readFileSync(new URL("./codeg-headless-runner.mjs", import.meta.url)));
  if (withWire) writeFileSync(join(home, "codeg-effort-wire.mjs"), readFileSync(new URL("../codeg-effort-wire.mjs", import.meta.url)));
  const launcher = join(root, "launcher", "bin", "dsh.mjs");
  mkdirSync(join(root, "launcher", "bin"), { recursive: true });
  writeFileSync(launcher, "// Stub identity; never executed.\n");
  writeFileSync(join(root, "launcher", "package.json"), JSON.stringify({ name: "@deepseek-ai/dsh", version: "0.1.6-alpha.2" }));
  return { home, runner, launcher };
}

test("merged DSH metadata mode returns before stdin, runtime internals, Agent or steering setup", async () => {
  const layout = profile();
  // This is the exact deployed relative layout from dsh_profile, including the
  // new sibling wire validator. Importing these owned modules runs no SDK.
  const runner = await import(pathToFileURL(layout.runner).href);
  assert.equal(typeof runner.nativeSteeringBridge, "function");
  assert.equal(typeof runner.turnModelSelection, "function");
  const originalArg = process.argv[1];
  const originalWrite = process.stdout.write;
  const originalError = process.stderr.write;
  const input = process.stdin;
  const iterator = Object.getOwnPropertyDescriptor(input, Symbol.asyncIterator);
  const output = [], errors = [], lookups = [];
  let complete;
  const finished = new Promise(resolve => { complete = resolve; });
  let loaderCalls = 0;
  const ctx = {
    get(service) {
      if (service === "appExit") return complete;
      if (service === "loader") return { async await() { loaderCalls++; } };
      if (service === "llm") return {
        async resolveModelInfo(provider, model) {
          lookups.push([provider, model]);
          return { reasoning: { efforts: [{ id: "off" }, { id: "high" }] } };
        },
        async resolveCallConfig() { throw new Error("metadata mode must not prepare a call"); },
      };
      throw new Error(`metadata mode accessed runtime service: ${service}`);
    },
    on() { throw new Error("metadata mode installed a native steering listener"); },
  };
  let timeout;
  try {
    process.argv[1] = layout.launcher;
    process.stdout.write = chunk => { output.push(String(chunk)); return true; };
    process.stderr.write = chunk => { errors.push(String(chunk)); return true; };
    Object.defineProperty(input, Symbol.asyncIterator, { configurable: true, value() { throw new Error("metadata mode read stdin"); } });
    runner.apply(ctx, {
      task: "-", json: true,
      capabilityModels: [{ provider: "direct", model: "exact" }],
      // Keep the production unref exit fallback beyond this test process.
      exitGraceMs: 3600000,
    });
    const code = await Promise.race([finished, new Promise((_, reject) => { timeout = setTimeout(() => reject(new Error("metadata mode did not exit")), 2000); })]);
    assert.equal(code, 0);
  } finally {
    clearTimeout(timeout);
    process.argv[1] = originalArg;
    process.stdout.write = originalWrite;
    process.stderr.write = originalError;
    if (iterator === undefined) delete input[Symbol.asyncIterator];
    else Object.defineProperty(input, Symbol.asyncIterator, iterator);
  }
  assert.deepEqual(errors, []);
  assert.equal(loaderCalls, 1);
  assert.deepEqual(lookups, [["direct", "exact"]]);
  assert.deepEqual(JSON.parse(output.join("")), {
    type: "codeg_effort_capabilities", version: "0.1.6-alpha.2",
    models: [{ model: "exact", provider: "direct", known: true, levels: ["none", "high"] }],
  });
  // No dsh dependency packages exist in this stub: loadInternals would fail.
  assert.equal(runner.pinnedSteeringVersions(Array(5).fill("0.1.6-alpha.2")), true);
});

test("materialized DSH runner requires its delivered sibling effort wire module", async () => {
  const missingWire = profile(false);
  await assert.rejects(import(pathToFileURL(missingWire.runner).href), error => error.code === "ERR_MODULE_NOT_FOUND" && error.message.includes("codeg-effort-wire.mjs"));
});
