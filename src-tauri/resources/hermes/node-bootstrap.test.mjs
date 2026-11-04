// Source/policy tests only: never imports the npm wrapper or Hermes runtime.
import assert from 'node:assert/strict'
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, realpathSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import vm from 'node:vm'
import { inspectWrapper, restoreOwnedEnvironment, patchLauncher, registerHermesHook, initialize, load, PINS } from './node-bootstrap.mjs'

const fixture = JSON.parse(readFileSync(new URL('./node-wrapper-test-fixture.json', import.meta.url)))

function setup(t) {
  const temp = realpathSync(mkdtempSync(join(tmpdir(), 'codeg-hermes-node-policy-')))
  t.after(() => rmSync(temp, { recursive: true, force: true }))
  const root = join(temp, 'wrapper'), owned = join(temp, 'owned dir')
  mkdirSync(owned); mkdirSync(join(root, 'bin'), { recursive: true }); mkdirSync(join(root, 'lib'))
  for (const name of Object.keys(PINS)) writeFileSync(join(root, name), fixture.files[name])
  for (const name of ['node-bootstrap.mjs', 'sitecustomize.py', 'codeg_native_steering.py']) writeFileSync(join(owned, name), '# never executed')
  const options = { entry: join(root, 'bin/hermes.js'), args: ['acp'], bootstrapFile: join(owned, 'node-bootstrap.mjs'),
    env: { CODEG_HERMES_NATIVE_BOOTSTRAP: '1', CODEG_HERMES_OWNED_BOOTSTRAP_DIR: owned } }
  const plan = inspectWrapper(options)
  assert.ok(plan)
  const env = { ...options.env, HERMES_NPM_BRIDGE: '1', HERMES_NPM_PACKAGE_ROOT: root, HERMES_NPM_PACKAGE_VERSION: '0.21.3',
    HERMES_NPM_RELEASE_TAG: 'v2026.9.14', VIRTUAL_ENV: plan.venv, KEEP: 'existing' }
  const invocation = { command: plan.console, args: ['acp'] }
  return { root, owned, options, plan, env, invocation }
}

test('Hermes restores only owned directory after actual published environment cleanup', t => {
  const h = setup(t)
  const source = fixture.files['lib/python-launcher.js']
  const start = source.indexOf('function getRuntimeEnvironment()')
  const end = source.indexOf('function runHermes', start)
  const context = { process: { env: { ...h.env, PYTHONPATH: '/ambient/user-path', PYTHONHOME: '/ambient/python' } },
    getVenvDirectory: () => h.plan.venv, packageRoot: h.root, packageJson: JSON.parse(fixture.files['package.json']) }
  const cleaned = vm.runInNewContext(source.slice(start, end) + '\ngetRuntimeEnvironment()', context)
  assert.equal(Object.hasOwn(cleaned, 'PYTHONPATH'), false)
  const result = restoreOwnedEnvironment(cleaned, h.invocation, ['acp'], h.plan)
  assert.equal(result.PYTHONPATH, h.owned)
  assert.equal(result.PYTHONHOME, undefined)
  assert.equal(result.CODEG_HERMES_OWNED_BOOTSTRAP_DIR, undefined)
  assert.equal(result.CODEG_HERMES_NATIVE_BOOTSTRAP, '1')
  assert.equal(result.KEEP, 'existing')
  assert.deepEqual(h.invocation, { command: h.plan.console, args: ['acp'] })
})

test('Hermes other command, args, venv and unknown final env remain the original object', t => {
  const h = setup(t)
  const cases = [
    [h.env, { ...h.invocation, command: '/other/python' }, ['acp']],
    [h.env, { ...h.invocation, args: ['chat'] }, ['chat']],
    [h.env, h.invocation, ['acp', '--unknown']],
    [{ ...h.env, VIRTUAL_ENV: '/other/venv' }, h.invocation, ['acp']],
    [{ ...h.env, HERMES_NPM_PACKAGE_VERSION: '0.21.4' }, h.invocation, ['acp']],
    [{ ...h.env, HERMES_NPM_PACKAGE_ROOT: '/other/package' }, h.invocation, ['acp']],
    [{ ...h.env, HERMES_NPM_RELEASE_TAG: 'v-next' }, h.invocation, ['acp']],
    [{ ...h.env, PYTHONPATH: '/still-ambient' }, h.invocation, ['acp']],
    [{ ...h.env, PYTHONHOME: '/still-ambient' }, h.invocation, ['acp']],
    [{ ...h.env, CODEG_HERMES_OWNED_BOOTSTRAP_DIR: '/untrusted/dir' }, h.invocation, ['acp']],
    [{ ...h.env, CODEG_HERMES_NATIVE_BOOTSTRAP: '0' }, h.invocation, ['acp']],
  ]
  for (const [env, invocation, args] of cases) assert.equal(restoreOwnedEnvironment(env, invocation, args, h.plan), env)
})

test('Hermes wrapper version, entry, source and owned selector are all verified before hooking', t => {
  const h = setup(t)
  for (const options of [
    { ...h.options, args: ['chat'] },
    { ...h.options, entry: join(h.root, 'bin/hermes-agent.js') },
    { ...h.options, env: { ...h.options.env, CODEG_HERMES_OWNED_BOOTSTRAP_DIR: h.root } },
    { ...h.options, env: { ...h.options.env, CODEG_HERMES_NATIVE_BOOTSTRAP: '0' } },
  ]) assert.equal(inspectWrapper(options), null)
  writeFileSync(join(h.root, 'package.json'), fixture.files['package.json'].replace('0.21.3', '0.21.4'))
  assert.equal(inspectWrapper(h.options), null)
  writeFileSync(join(h.root, 'package.json'), fixture.files['package.json'])
  writeFileSync(join(h.root, 'lib/python-launcher.js'), fixture.files['lib/python-launcher.js'] + '\n')
  assert.equal(inspectWrapper(h.options), null)
})

test('Hermes source hook patches only exact original environment expression and rechecks at load', async t => {
  const h = setup(t), source = fixture.files['lib/python-launcher.js']
  const patched = patchLauncher(source, h.plan)
  assert.ok(patched.includes('env: __codegHermesOwnedEnvironment(getRuntimeEnvironment(), invocation, userArgs, __codegHermesOwnedPlan),'))
  assert.ok(patched.includes('spawn(invocation.command, invocation.args,'))
  new vm.Script(patched) // syntax only; never executes require/runHermes
  assert.equal(patchLauncher(source + '\n', h.plan), null)
  let hooks
  assert.equal(registerHermesHook({ ...h.options, registerHooks: value => { hooks = value } }), true)
  const original = { format: 'commonjs', source }
  assert.equal(hooks.load('file:///other.js', {}, () => original), original)
  assert.notEqual(hooks.load(h.plan.launcherUrl, {}, () => original).source, source)
  writeFileSync(join(h.root, 'bin/hermes.js'), fixture.files['bin/hermes.js'] + '\n')
  assert.equal(hooks.load(h.plan.launcherUrl, {}, () => original), original)
  writeFileSync(join(h.root, 'bin/hermes.js'), fixture.files['bin/hermes.js'])
  assert.equal(hooks.load(h.plan.launcherUrl, {}, () => original), original, 'once revoked, a hook cannot reopen')
})

test('Hermes Node async CommonJS hook fallback supplies known entry source without a global spawn patch', async t => {
  const h = setup(t)
  let registered
  assert.equal(registerHermesHook({ ...h.options, registerHooks: null, register: (...args) => { registered = args } }), true)
  assert.equal(registered[0], registered[1])
  assert.deepEqual(registered[2].data, h.plan)
  initialize(h.plan)
  const entry = await load(h.plan.entryUrl, {}, async () => ({ format: 'commonjs', source: null }))
  assert.equal(entry.source, fixture.files['bin/hermes.js'])
  const launcher = await load(h.plan.launcherUrl, {}, async () => ({ format: 'commonjs', source: null }))
  assert.ok(launcher.source.includes('__codegHermesOwnedEnvironment'))
  assert.equal(registerHermesHook({ ...h.options, registerHooks: null, register: null }), false)
})

test('Hermes Windows console remains the exact wrapper-selected executable', t => {
  const h = setup(t)
  const plan = inspectWrapper({ ...h.options, platform: 'win32' })
  assert.equal(plan.console, join(h.plan.venv, 'Scripts', 'hermes.exe'))
  assert.equal(restoreOwnedEnvironment(h.env, h.invocation, ['acp'], plan), h.env)
})

test('Hermes verified selector spelling can differ from canonical owned directory', t => {
  const h = setup(t)
  const selector = h.owned + '/.'
  const plan = inspectWrapper({ ...h.options, env: { ...h.options.env, CODEG_HERMES_OWNED_BOOTSTRAP_DIR: selector } })
  assert.equal(plan.ownedDir, h.owned)
  assert.equal(plan.ownedSelector, selector)
  const result = restoreOwnedEnvironment({ ...h.env, CODEG_HERMES_OWNED_BOOTSTRAP_DIR: selector }, h.invocation, ['acp'], plan)
  assert.equal(result.PYTHONPATH, h.owned)
})

test('Hermes native optout zero permits only the requested reasoning bootstrap', t => {
  const h = setup(t)
  writeFileSync(join(h.owned, 'reasoning_bridge.py'), '# fake resource, never executed')
  const options = { ...h.options, env: { ...h.options.env, CODEG_HERMES_NATIVE_BOOTSTRAP: '0',
    CODEG_HERMES_REASONING_BOOTSTRAP: '1', CODEG_HERMES_REASONING_EFFORT: 'high' } }
  const plan = inspectWrapper(options)
  assert.ok(plan)
  const env = { ...h.env, ...options.env }
  const restored = restoreOwnedEnvironment(env, h.invocation, ['acp'], plan)
  assert.equal(restored.PYTHONPATH, h.owned)
  assert.equal(restored.CODEG_HERMES_NATIVE_BOOTSTRAP, '0')
  assert.equal(restored.CODEG_HERMES_REASONING_BOOTSTRAP, '1')
  assert.equal(restored.CODEG_HERMES_REASONING_EFFORT, 'high')
  assert.equal(restoreOwnedEnvironment({ ...env, CODEG_HERMES_NATIVE_BOOTSTRAP: '1' }, h.invocation, ['acp'], plan).PYTHONPATH, undefined)
})

test('Hermes joint bootstrap keeps both markers and requires its reasoning resource', t => {
  const h = setup(t)
  const options = { ...h.options, env: { ...h.options.env,
    CODEG_HERMES_REASONING_BOOTSTRAP: '1', CODEG_HERMES_REASONING_EFFORT: 'low' } }
  assert.equal(inspectWrapper(options), null)
  writeFileSync(join(h.owned, 'reasoning_bridge.py'), '# fake resource, never executed')
  const plan = inspectWrapper(options)
  assert.ok(plan)
  const env = { ...h.env, ...options.env }
  const restored = restoreOwnedEnvironment(env, h.invocation, ['acp'], plan)
  assert.equal(restored.PYTHONPATH, h.owned)
  assert.equal(restored.CODEG_HERMES_NATIVE_BOOTSTRAP, '1')
  assert.equal(restored.CODEG_HERMES_REASONING_BOOTSTRAP, '1')
  assert.equal(restored.CODEG_HERMES_REASONING_EFFORT, 'low')
  for (const value of ['0', undefined]) {
    const changed = { ...env, CODEG_HERMES_REASONING_BOOTSTRAP: value }
    assert.equal(restoreOwnedEnvironment(changed, h.invocation, ['acp'], plan), changed)
  }
})
