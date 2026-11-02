#!/usr/bin/env node
// Execute only reviewed tests; never discover safety by running the full suite.
import {createHash} from 'node:crypto';
import {readFileSync, readdirSync} from 'node:fs';
import {dirname, join, relative, resolve} from 'node:path';
import {spawnSync} from 'node:child_process';
import {fileURLToPath, pathToFileURL} from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const filter = 'myclaw_prompt';

// Pin the source revision and Rust dependency lockfile. A source/dependency
// change requires another review before even the allowlisted tests may run.
export function sourceFingerprint(repo) {
  const files = [];
  function walk(dir) {
    for (const entry of readdirSync(dir, {withFileTypes: true})) {
      const path = join(dir, entry.name);
      if (entry.isSymbolicLink()) throw new Error(`Unexpected source symlink: ${path}`);
      if (entry.isDirectory() && entry.name !== 'target' && entry.name !== '.git') walk(path);
      else if (entry.isFile() && (entry.name.endsWith('.rs') || entry.name === 'Cargo.toml')) files.push(path);
    }
  }
  for (const dir of ['src-tauri/src', 'src-tauri/tests', 'src-tauri/vendor']) walk(join(repo, dir));
  for (const name of ['Cargo.lock', 'Cargo.toml', 'build.rs']) files.push(join(repo, 'src-tauri', name));
  const digest = createHash('sha256');
  const ordered = files.map(path => ({path, name: relative(repo, path).replaceAll('\\', '/')}))
    .sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
  for (const {path, name} of ordered) {
    digest.update(name);
    digest.update('\0');
    // Git may check out CRLF on Windows; normalize text, not its semantics.
    digest.update(readFileSync(path, 'utf8').replaceAll('\r\n', '\n'));
    digest.update('\0');
  }
  return digest.digest('hex');
}

export function assertAllowedTests(listing, expected) {
  if (!Array.isArray(expected) || !expected.length || new Set(expected).size !== expected.length
    || expected.some(name => typeof name !== 'string' || !name.startsWith('acp::') || !name.includes(filter))) {
    throw new Error('Invalid or empty database-free test allowlist');
  }
  const actual = listing.split(/\r?\n/).filter(line => line.endsWith(': test')).map(line => line.slice(0, -6));
  if (JSON.stringify(actual.sort()) !== JSON.stringify([...expected].sort())) {
    throw new Error('Test discovery differs from the reviewed allowlist; no tests were executed');
  }
}

export function runNoDbTests({mode, release = false, repo = root, invoke = spawnSync, fingerprint = sourceFingerprint, manifest}) {
  if (!['desktop', 'server'].includes(mode)) throw new Error('Usage: node scripts/test-no-db.mjs desktop|server');
  if (typeof release !== 'boolean') throw new Error('Invalid release profile selection');
  if (manifest.schemaVersion !== 1 || fingerprint(repo) !== manifest.sourceSha256) {
    throw new Error('Rust source/dependencies changed; review database-free test safety and update the manifest first');
  }
  const args = ['test', '--locked', ...(release ? ['--release'] : []), '--lib', ...(mode === 'server' ? ['--no-default-features'] : ['--features', 'test-utils']), filter];
  const options = {cwd: join(repo, 'src-tauri'), encoding: 'utf8', maxBuffer: 32 * 1024 * 1024};
  // --list only compiles/enumerates tests. It does not call the test functions.
  const listed = invoke('cargo', [...args, '--', '--list'], options);
  if (listed.status !== 0) throw new Error(`Test discovery failed: ${listed.error?.message ?? listed.stderr ?? listed.signal}`);
  assertAllowedTests(listed.stdout, manifest.tests);
  if (fingerprint(repo) !== manifest.sourceSha256) throw new Error('Source changed during test discovery');
  const result = invoke('cargo', [...args, '--', '--test-threads=1'], {...options, stdio: 'inherit'});
  if (result.status !== 0) throw new Error(`Database-free tests failed: ${result.error?.message ?? result.signal ?? result.status}`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    if (process.argv.length < 3 || process.argv.length > 4 || (process.argv[3] && process.argv[3] !== '--release')) {
      throw new Error('Usage: node scripts/test-no-db.mjs desktop|server [--release]');
    }
    runNoDbTests({mode: process.argv[2], release: process.argv[3] === '--release', manifest: JSON.parse(readFileSync(join(root, 'scripts/no-db-tests.json'), 'utf8'))});
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
