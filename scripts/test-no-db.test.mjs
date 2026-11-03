import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {mkdtempSync, mkdirSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname, join} from 'node:path';
import test from 'node:test';
import {assertAllowedTests, runNoDbTests, sourceFingerprint} from './test-no-db.mjs';

const name = 'acp::myclaw_prompt::tests::myclaw_prompt_example';
const manifest = {schemaVersion: 1, sourceSha256: 'reviewed-source', tests: [name]};

test('all bundled resource types and main effort sources are fingerprinted once before cargo', () => {
  const repo = mkdtempSync(join(tmpdir(), 'codeg-steering-review-'));
  try {
    for (const dir of ['src', 'tests', 'vendor']) {
      mkdirSync(join(repo, 'src-tauri', dir), {recursive: true});
    }
    const resources = ['codeg-effort-wire.mjs', 'pi/effort-probe.mjs', 'dsh/codeg-headless-runner.mjs',
      'hermes/steering.py', 'openclaw/openclaw.plugin.json', 'nested/deep/bootstrap.cjs', 'nested/deep/plugin.js'];
    const files = ['Cargo.lock', 'Cargo.toml', 'build.rs'].map(file => `src-tauri/${file}`)
      .concat(['src-tauri/src/fixture.rs'], resources.map(file => `src-tauri/resources/${file}`));
    const contents = 'reviewed fixture\r\n';
    for (const file of files) {
      const source = join(repo, file);
      mkdirSync(dirname(source), {recursive: true});
      writeFileSync(source, contents);
    }
    // Independent, explicit file set catches both a skipped resource and a
    // duplicate main resource appended after the recursive directory walk.
    const expected = createHash('sha256');
    for (const file of [...files].sort()) expected.update(`${file}\0${contents.replaceAll('\r\n', '\n')}\0`);
    assert.equal(sourceFingerprint(repo), expected.digest('hex'));
    for (const file of resources) {
      const source = join(repo, 'src-tauri/resources', file);
      writeFileSync(source, 'reviewed fixture');
      const reviewed = {...manifest, sourceSha256: sourceFingerprint(repo)};
      writeFileSync(source, 'changed fixture');
      assert.throws(() => runNoDbTests({repo, mode: 'server', manifest: reviewed,
        invoke: () => { throw new Error('must not invoke cargo'); },
      }), /source\/dependencies changed/);
    }
  } finally {
    rmSync(repo, {recursive: true});
  }
});

test('unknown, missing and duplicate test names cannot enter the executable suite', () => {
  assertAllowedTests(`${name}: test\r\n1 test, 0 benchmarks\r\n`, [name]);
  for (const listing of ['', `${name}: test\nacp::myclaw_prompt_database_test: test`, `${name}: test\n${name}: test`]) {
    assert.throws(() => assertAllowedTests(listing, [name]));
  }
  assert.throws(() => assertAllowedTests('', []));
});

test('source changes and invalid modes stop before any cargo invocation', () => {
  const invoke = () => { throw new Error('must not invoke cargo'); };
  assert.throws(() => runNoDbTests({mode: 'server', manifest, fingerprint: () => 'changed', invoke}), /source\/dependencies changed/);
  assert.throws(() => runNoDbTests({mode: 'all', manifest, fingerprint: () => 'reviewed-source', invoke}), /Usage/);
  assert.throws(() => runNoDbTests({mode: 'server', release: 'all', manifest, fingerprint: () => 'reviewed-source', invoke}), /Invalid release/);
});

test('discovery failure or an added test stops before test execution', () => {
  for (const result of [{status: 1, stderr: 'compile error'}, {status: 0, stdout: `${name}: test\nacp::myclaw_prompt_sqlite: test`}]) {
    let calls = 0;
    assert.throws(() => runNoDbTests({mode: 'server', manifest, fingerprint: () => 'reviewed-source', invoke: () => { calls++; return result; }}));
    assert.equal(calls, 1);
  }
});

test('source changes during discovery stop before execution', () => {
  let checks = 0;
  let calls = 0;
  assert.throws(() => runNoDbTests({mode: 'server', manifest,
    fingerprint: () => ++checks === 1 ? 'reviewed-source' : 'changed',
    invoke: () => { calls++; return {status: 0, stdout: `${name}: test`}; },
  }), /Source changed during/);
  assert.equal(calls, 1);
});

test('only the reviewed library filter executes in either supported mode', () => {
  for (const mode of ['desktop', 'server']) {
    for (const release of [false, true]) {
      const calls = [];
      runNoDbTests({mode, release, manifest, fingerprint: () => 'reviewed-source', invoke: (command, args) => {
        calls.push({command, args});
        return {status: 0, stdout: `${name}: test`};
      }});
      assert.equal(calls.length, 2);
      assert.ok(calls.every(call => call.command === 'cargo' && call.args.includes('--lib') && call.args.includes('myclaw_prompt')));
      assert.deepEqual(calls[0].args.slice(-2), ['--', '--list']);
      assert.deepEqual(calls[1].args.slice(-2), ['--', '--test-threads=1']);
      assert.ok(calls[1].args.includes(mode === 'server' ? '--no-default-features' : 'test-utils'));
      assert.equal(calls[1].args.includes('--release'), release);
    }
  }
});

test('a failing approved suite is not reported as success', () => {
  let calls = 0;
  assert.throws(() => runNoDbTests({mode: 'server', manifest, fingerprint: () => 'reviewed-source',
    invoke: () => ++calls === 1 ? {status: 0, stdout: `${name}: test`} : {status: 1},
  }), /tests failed/);
});
