// Actual fixed npm launcher -> fake Python console -> production sitecustomize.
// No installer, Hermes runtime, provider/model, network or database is invoked.
import assert from "node:assert/strict";
import { createHash, createPublicKey, verify } from "node:crypto";
import { spawn } from "node:child_process";
import { chmod, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const resources = dirname(fileURLToPath(import.meta.url));
const fixture = JSON.parse(await readFile(join(resources, "node-wrapper-test-fixture.json"), "utf8"));
for (const [name, content] of Object.entries(fixture.files)) {
  assert.equal(createHash("sha256").update(content).digest("hex"), fixture.sha256[name]);
}
assert.equal(fixture.metadata.version, "0.21.3");
assert.equal(fixture.sha256["lib/python-launcher.js"], "d2bd674fc9ddeb274be0ad56afacdd2385b0f5069ee1564937622cd50ae3c23a");

const fakeConsole = `#!/usr/bin/env python3
import json, os, sys
assert not any(name in sys.modules for name in ['agent','acp_adapter','run_agent','sqlite3']), 'runtime or database module was imported'
probe=json.loads(sys.stdin.readline())
installed=any(type(f).__module__=='codeg_native_steering' and type(f).__name__=='_Finder' for f in sys.meta_path)
print(json.dumps({'probe':probe,'installed':installed,'argv':sys.argv[1:],'cwd':os.getcwd(),
 'pythonPath':os.environ.get('PYTHONPATH'),'marker':os.environ.get('CODEG_HERMES_NATIVE_BOOTSTRAP'),
 'venv':os.environ.get('VIRTUAL_ENV'),'bridgeVersion':os.environ.get('HERMES_NPM_PACKAGE_VERSION')},ensure_ascii=False),flush=True)
`;

async function launch(mode) {
  const root = await mkdtemp(join(tmpdir(), "codeg-hermes-stdio-"));
  try {
    const owned = join(root, "owned cache");
    const packageRoot = join(root, "package");
    const work = join(root, "original-cwd");
    await Promise.all([mkdir(owned, { mode: 0o700 }), mkdir(work)]);
    for (const [name, source] of Object.entries(fixture.files)) {
      const path = join(packageRoot, name);
      await mkdir(dirname(path), { recursive: true });
      await writeFile(path, source);
    }
    if (mode === "unknown-version") {
      const metadata = JSON.parse(fixture.files["package.json"]);
      metadata.version = "0.21.4";
      await writeFile(join(packageRoot, "package.json"), JSON.stringify(metadata));
    }
    if (mode === "changed-launcher") {
      await writeFile(join(packageRoot, "lib/python-launcher.js"), fixture.files["lib/python-launcher.js"] + "\n// unaudited revision\n");
    }
    for (const name of ["node-bootstrap.mjs", "sitecustomize.py", "codeg_native_steering.py"]) {
      await writeFile(join(owned, name), await readFile(join(resources, name)));
    }
    const consolePath = join(packageRoot, "runtime/hermes-agent/venv/bin/hermes");
    await mkdir(dirname(consolePath), { recursive: true });
    await writeFile(consolePath, fakeConsole);
    await chmod(consolePath, 0o700);
    const canonicalOwned = await realpath(owned);
    let preload = join(canonicalOwned, "node-bootstrap.mjs");
    if (mode === "async-supported") {
      preload = join(canonicalOwned, "force-async.mjs");
      await writeFile(preload, `import { isMainThread } from 'node:worker_threads';
if (isMainThread) {
 const marker=process.env.CODEG_HERMES_NATIVE_BOOTSTRAP;
 delete process.env.CODEG_HERMES_NATIVE_BOOTSTRAP;
 const {registerHermesHook}=await import('./node-bootstrap.mjs');
 process.env.CODEG_HERMES_NATIVE_BOOTSTRAP=marker;
 if (!registerHermesHook({registerHooks:null})) throw new Error('async fallback was not registered');
}
`);
    }
    const env = {
      PATH: process.env.PATH,
      HOME: root,
      PYTHONNOUSERSITE: "1",
      // The real official wrapper MUST remove this, even in the successful case.
      PYTHONPATH: join(root, "ambient-path-must-not-return"),
      CODEG_HERMES_NATIVE_BOOTSTRAP: "1",
      CODEG_HERMES_OWNED_BOOTSTRAP_DIR: canonicalOwned,
      NODE_OPTIONS: `--import=${JSON.stringify(preload)}`,
    };
    const args = mode === "non-acp" ? ["--check"] : ["acp"];
    const input = { id: "stdio-probe", text: "B 原样输入" };
    const result = await new Promise((resolve, reject) => {
      const child = spawn(process.execPath, [join(packageRoot, "bin/hermes.js"), ...args], { cwd: work, env, stdio: ["pipe", "pipe", "pipe"] });
      let stdout = "", stderr = "";
      child.stdout.setEncoding("utf8").on("data", (data) => { stdout += data; });
      child.stderr.setEncoding("utf8").on("data", (data) => { stderr += data; });
      const deadline = setTimeout(() => { child.kill("SIGKILL"); reject(new Error(`wrapper timed out: ${stderr}`)); }, 10000);
      child.on("error", (error) => { clearTimeout(deadline); reject(error); });
      child.on("close", (code) => {
        clearTimeout(deadline);
        if (code !== 0) reject(new Error(`wrapper exit ${code}: ${stderr}`));
        else resolve(JSON.parse(stdout));
      });
      child.stdin.end(`${JSON.stringify(input)}\n`);
    });
    assert.deepEqual(result.probe, input);
    assert.deepEqual(result.argv, args);
    assert.equal(result.cwd, await realpath(work));
    assert.equal(result.venv, await realpath(join(packageRoot, "runtime/hermes-agent/venv")));
    assert.equal(result.bridgeVersion, mode === "unknown-version" ? "0.21.4" : "0.21.3");
    if (mode === "supported" || mode === "async-supported") {
      assert.equal(result.installed, true, "the production sitecustomize must actually install its import hook");
      assert.equal(result.pythonPath, canonicalOwned, "only Codeg's owned directory returns; ambient stays stripped");
      assert.equal(result.marker, null, "Python consumed the one-launch marker");
    } else {
      assert.equal(result.installed, false, "unknown/other commands must keep ordinary Python startup");
      assert.equal(result.pythonPath, null);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

test("fixed npm registry payload verifies offline with its public ECDSA key", () => {
  const metadata = fixture.metadata;
  const message = Buffer.from(`${metadata.name}@${metadata.version}:${metadata.dist.integrity}`);
  assert.equal(metadata.dist.signatures.length, 2);
  for (const signature of metadata.dist.signatures) {
    const key = fixture.registryKeys.keys.find((key) => key.keyid === signature.keyid);
    assert.equal(key.scheme, "ecdsa-sha2-nistp256");
    assert.equal(verify("sha256", message, createPublicKey({ key: Buffer.from(key.key, "base64"), format: "der", type: "spki" }), Buffer.from(signature.sig, "base64")), true);
  }
});
test("official 0.21.3 wrapper reaches production Python bootstrap with original stdio/venv/cwd", async () => launch("supported"));
test("official asynchronous loader fallback also reaches the production Python bootstrap", async () => launch("async-supported"));
test("unknown npm version retains ordinary stdio without enabling the Python hook", async () => launch("unknown-version"));
test("changed launcher source retains ordinary stdio without enabling the Python hook", async () => launch("changed-launcher"));
test("non-ACP npm commands retain ordinary stdio without enabling the Python hook", async () => launch("non-acp"));
