// Narrow, source-pinned bridge for the community hermes-agent npm wrapper.
// No wrapper/runtime imports, installation, configuration writes or new child.
import * as moduleApi from 'node:module'
import { createHash } from 'node:crypto'
import { readFileSync, realpathSync, statSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { isMainThread } from 'node:worker_threads'

export const PINS = Object.freeze({
  'package.json': 'ad1c65cc1c6d56a52a1e6ad0b563b2e30fd240473b3c99f7730bc964532a9d9f',
  'bin/hermes.js': 'e0f9d55a7345c1a12a8871c6c622fc88799403a8b3c839e0d63c56c603b3e1e1',
  'lib/python-launcher.js': 'd2bd674fc9ddeb274be0ad56afacdd2385b0f5069ee1564937622cd50ae3c23a',
})
const hash = source => createHash('sha256').update(source).digest('hex')
const ownFile = fileURLToPath(import.meta.url)

export function inspectWrapper({ entry = process.argv[1], args = process.argv.slice(2), env = process.env, bootstrapFile = ownFile, platform = process.platform } = {}) {
  try {
    const nativeEnabled = env.CODEG_HERMES_NATIVE_BOOTSTRAP === '1'
    const reasoningEnabled = env.CODEG_HERMES_REASONING_BOOTSTRAP === '1'
    if ((!nativeEnabled && !reasoningEnabled) || args.length !== 1 || args[0] !== 'acp') return null
    const ownedDir = realpathSync(dirname(bootstrapFile))
    if (!env.CODEG_HERMES_OWNED_BOOTSTRAP_DIR || realpathSync(env.CODEG_HERMES_OWNED_BOOTSTRAP_DIR) !== ownedDir) return null
    const required = ['sitecustomize.py', ...(nativeEnabled ? ['codeg_native_steering.py'] : []), ...(reasoningEnabled ? ['reasoning_bridge.py'] : [])]
    if (!required.every(name => statSync(join(ownedDir, name)).isFile())) return null
    const resolved = realpathSync(entry)
    const root = dirname(dirname(resolved))
    if (resolved !== join(root, 'bin', 'hermes.js')) return null
    const sources = Object.fromEntries(Object.keys(PINS).map(name => [name, readFileSync(join(root, name), 'utf8')]))
    if (!Object.entries(PINS).every(([name, pin]) => hash(sources[name]) === pin)) return null
    const pkg = JSON.parse(sources['package.json'])
    if (pkg.name !== 'hermes-agent' || pkg.version !== '0.21.3' || pkg.hermesAgent?.upstreamCommit !== '345cd2b057a452236de401d3534b8502a7465e8d') return null
    const venv = join(root, 'runtime', 'hermes-agent', 'venv')
    return {
      root, ownedDir, ownedSelector: env.CODEG_HERMES_OWNED_BOOTSTRAP_DIR, venv,
      nativeMarker: env.CODEG_HERMES_NATIVE_BOOTSTRAP,
      reasoningMarker: env.CODEG_HERMES_REASONING_BOOTSTRAP,
      console: platform === 'win32' ? join(venv, 'Scripts', 'hermes.exe') : join(venv, 'bin', 'hermes'),
      entryUrl: pathToFileURL(resolved).href,
      launcherUrl: pathToFileURL(join(root, 'lib', 'python-launcher.js')).href,
    }
  } catch { return null }
}

// Serialized into the audited wrapper's module scope. Only the final env of
// its original exact venv ACP spawn is changed. Ambient PYTHONPATH stays gone.
export function restoreOwnedEnvironment(env, invocation, userArgs, plan) {
  if (!env || !plan || invocation?.command !== plan.console ||
      !Array.isArray(userArgs) || userArgs.length !== 1 || userArgs[0] !== 'acp' ||
      !Array.isArray(invocation.args) || invocation.args.length !== 1 || invocation.args[0] !== 'acp' ||
      env.HERMES_NPM_BRIDGE !== '1' || env.HERMES_NPM_PACKAGE_ROOT !== plan.root ||
      env.HERMES_NPM_PACKAGE_VERSION !== '0.21.3' || env.HERMES_NPM_RELEASE_TAG !== 'v2026.9.14' ||
      env.VIRTUAL_ENV !== plan.venv ||
      (env.CODEG_HERMES_NATIVE_BOOTSTRAP !== '1' && env.CODEG_HERMES_REASONING_BOOTSTRAP !== '1') ||
      env.CODEG_HERMES_NATIVE_BOOTSTRAP !== plan.nativeMarker ||
      env.CODEG_HERMES_REASONING_BOOTSTRAP !== plan.reasoningMarker ||
      env.CODEG_HERMES_OWNED_BOOTSTRAP_DIR !== plan.ownedSelector ||
      Object.hasOwn(env, 'PYTHONHOME') || Object.hasOwn(env, 'PYTHONPATH')) return env
  const result = { ...env, PYTHONPATH: plan.ownedDir }
  // Python consumes its own one-shot marker; Node descendants must not use
  // this wrapper-only selector to install another launch shim.
  delete result.CODEG_HERMES_OWNED_BOOTSTRAP_DIR
  return result
}

export function patchLauncher(source, plan) {
  if (!plan || hash(source) !== PINS['lib/python-launcher.js']) return null
  const anchor = 'env: getRuntimeEnvironment(),'
  if (source.split(anchor).length !== 2) return null
  const selector = JSON.stringify({ root: plan.root, ownedDir: plan.ownedDir, ownedSelector: plan.ownedSelector, venv: plan.venv, console: plan.console, nativeMarker: plan.nativeMarker, reasoningMarker: plan.reasoningMarker })
  return `const __codegHermesOwnedEnvironment = (${restoreOwnedEnvironment.toString()});\nconst __codegHermesOwnedPlan = ${selector};\n` +
    source.replace(anchor, 'env: __codegHermesOwnedEnvironment(getRuntimeEnvironment(), invocation, userArgs, __codegHermesOwnedPlan),')
}

function rewrite(url, result, plan, populateEntry = false) {
  if (!plan || plan.disabled || result?.format !== 'commonjs' || ![plan.launcherUrl, plan.entryUrl].includes(url)) return result
  // Recheck at actual load, not only registration: a concurrent package
  // replacement must not retain a hook admitted for older on-disk files.
  try {
    if (!Object.entries(PINS).every(([name, pin]) => hash(readFileSync(join(plan.root, name), 'utf8')) === pin)) {
      plan.disabled = true
      return result
    }
  } catch { plan.disabled = true; return result }
  const source = result.source == null ? readFileSync(fileURLToPath(url), 'utf8') :
    typeof result.source === 'string' ? result.source : Buffer.from(result.source).toString('utf8')
  if (url === plan.entryUrl) {
    // Async hooks must provide the original known CJS entry source so its
    // require('./python-launcher') also traverses that official loader.
    if (hash(source) !== PINS['bin/hermes.js']) { plan.disabled = true; return result }
    return populateEntry ? { ...result, source } : result
  }
  const patched = patchLauncher(source, plan)
  return patched ? { ...result, source: patched } : result
}

let asynchronousPlan
export function initialize(plan) { asynchronousPlan = plan }
export async function load(url, context, nextLoad) {
  return rewrite(url, await nextLoad(url, context), asynchronousPlan, true)
}

export function registerHermesHook({ registerHooks = moduleApi.registerHooks, register = moduleApi.register, ...options } = {}) {
  const plan = inspectWrapper(options)
  if (!plan) return false
  if (typeof registerHooks === 'function') {
    registerHooks({ load(url, context, nextLoad) { return rewrite(url, nextLoad(url, context), plan) } })
    return true
  }
  if (typeof register === 'function') {
    // Node >=20.6 supports source-providing CommonJS async load hooks. No
    // Module._load / require.extensions private API or global spawn patch.
    register(import.meta.url, import.meta.url, { data: plan })
    return true
  }
  return false
}

if (isMainThread) {
  try { registerHermesHook() } catch {}
}
