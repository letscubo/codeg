import assert from 'node:assert/strict'
import { test } from 'node:test'
import { readFileSync } from 'node:fs'
import { pathToFileURL } from 'node:url'
import vm from 'node:vm'
import { createGatewayController, CAPABILITY, RPC } from './native-gateway.mjs'
import { installAcpBridge } from './acp-bridge.mjs'

const tick = () => new Promise(resolve => setImmediate(resolve))
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r }); return { promise, resolve } }
const PRIVATE = 'PRIVATE_PROMPT_TOKEN_SESSION_KEY'
async function logsDuring(run) {
  const original = console.error, lines = []
  console.error = line => lines.push(line)
  try { await run(lines) } finally { console.error = original }
  assert.ok(lines.every(line => line.startsWith('[codeg-openclaw-native] ')))
  assert.equal(lines.join('\n').includes(PRIVATE), false)
  return lines.map(line => JSON.parse(line.slice('[codeg-openclaw-native] '.length)))
}
function gatewayFixture() {
  const controller = new AbortController()
  const entry = { ownerConnId: 'owner', sessionKey: PRIVATE, sessionId: 'native-session', controller }
  const operation = { key: PRIVATE, sessionId: entry.sessionId, phase: 'running', toolAuthorityFingerprint: 'authority' }
  const backend = { kind: 'embedded', runId: 'native-run', supportsTranscriptCommitWait: true,
    messageInjectionV2: { version: 2, isAvailable: () => true, queueMessage() { assert.fail('diagnostics must never submit input') } } }
  const deps = { operations: new WeakMap([[controller.signal, operation]]), registry: new Map([[PRIVATE, operation]]), backend: () => backend }
  const context = { chatAbortControllers: new Map([['gateway-run', entry]]) }
  const scope = { version: 1, sessionKey: PRIVATE, gatewayRunId: 'gateway-run', hostTurnId: PRIVATE }
  return { entry, operation, backend, deps, context, scope, client: { connId: 'owner' }, gateway: createGatewayController(async () => deps) }
}
function bridgeFixture(capability = () => CAPABILITY) {
  const calls = [], notifications = [], wakes = []
  const session = { sessionKey: PRIVATE }
  let finish, prompts = 0, state = { version: 1, ready: false, reason: 'backend_missing' }
  const store = { getSession: () => session, setActiveRun(_sid, runId, controller) { Object.assign(session, { activeRunId: runId, abortController: controller }) } }
  const agent = { promptStream: { sessionStore: store, pendingPrompts: new Map() },
    async initialize(params) { assert.deepEqual(params, { caller: 'original' }); return { protocolVersion: 1, original: true } },
    async prompt(params) { assert.equal(params.sessionId, PRIVATE); prompts++; store.setActiveRun(PRIVATE, 'gateway-run', new AbortController()); finish = deferred(); return finish.promise },
    handleGatewayReconnect() {},
    handleGatewayDisconnect(reason) { return reason }, shutdown(reason) { return reason },
  }
  installAcpBridge(agent, { extNotification(method, params) { notifications.push({ method, params }) } }, {
    async request(method, params, options) {
      calls.push({ method, params, options })
      if (method === RPC.capability) return capability()
      assert.equal(method, RPC.state, 'no new RPC or ordinary/native submit')
      if (state instanceof Error) throw state
      return state
    },
  }, { sleep: () => new Promise(resolve => wakes.push(resolve)) })
  return {
    agent, calls, notifications, get prompts() { return prompts },
    async initialize() { return agent.initialize({ caller: 'original' }) },
    changeBinding(change) { change(session) },
    start(hostTurnId = PRIVATE) { return agent.prompt({ sessionId: PRIVATE, prompt: [{ type: 'text', text: PRIVATE }], _meta: { 'ai.myclaw/nativeTurn': { version: 1, hostTurnId } } }) },
    async advance(next) { state = next; const wake = wakes.shift(); assert.ok(wake); wake(); await tick() },
    async finish(result = { stopReason: 'end_turn', original: true }) { finish.resolve(result); for (const wake of wakes.splice(0)) wake(); await tick(); return result },
  }
}

test('OpenClaw diagnostics never reveal run existence across an unowned or mismatched scope', async () => {
  for (const change of [
    h => h.context.chatAbortControllers.clear(),
    h => { h.entry.ownerConnId = 'different' },
    h => { h.entry.sessionKey = 'different' },
    h => { h.operation.phase = 'completed' },
    h => { h.deps.backend = () => undefined },
    h => { h.backend.kind = 'unknown' },
    h => { h.backend.messageInjectionV2.isAvailable = () => false },
    h => { h.operation.toolAuthorityFingerprint = undefined },
  ]) {
    const h = gatewayFixture(); change(h)
    assert.deepEqual(await h.gateway.state(h.scope, { connId: 'outsider' }, h.context), { version: 1, ready: false, reason: 'inactive_or_unowned' })
  }
  const h = gatewayFixture()
  h.deps.backend = () => { assert.fail('must not inspect an unowned backend') }
  assert.deepEqual(await h.gateway.state(h.scope, { connId: 'outsider' }, h.context), { version: 1, ready: false, reason: 'inactive_or_unowned' })
})

test('OpenClaw diagnostics add coarse backend reasons only after exact ownership and preserve ready shape', async () => {
  const h = gatewayFixture()
  assert.strictEqual(await h.gateway.capability(), CAPABILITY)
  const ready = await h.gateway.state(h.scope, h.client, h.context)
  assert.deepEqual(Object.keys(ready).sort(), ['nativeTurnId', 'ready', 'version'])
  assert.equal(ready.ready, true); assert.match(ready.nativeTurnId, /^[0-9a-f]{64}$/)
  for (const [change, reason] of [
    [h => { h.deps.backend = () => undefined }, 'backend_missing'],
    [h => { h.backend.kind = 'other' }, 'backend_unsupported'],
    [h => { h.backend.messageInjectionV2.isAvailable = () => false }, 'backend_not_available'],
    [h => { h.operation.toolAuthorityFingerprint = undefined }, 'authority_missing'],
  ]) {
    const item = gatewayFixture(); change(item)
    assert.deepEqual(await item.gateway.state(item.scope, item.client, item.context), { version: 1, ready: false, reason })
  }
})

test('OpenClaw capability failure reasons are a closed whitelist without raw errors', async () => {
  for (const [message, reason] of [
    ['unsupported_openclaw_version', 'unsupported_runtime'], ['unsupported_openclaw_bundle', 'unsupported_bundle'],
    ['unsupported_openclaw_exports', 'unsupported_exports'], [PRIVATE, 'load_failed'],
  ]) {
    const gateway = createGatewayController(async () => { throw new Error(message) })
    assert.deepEqual(await gateway.capability(), { version: 0, reason })
  }
  let reads = 0
  const error = new Error()
  Object.defineProperty(error, 'message', { get() { reads++; throw new Error(PRIVATE) } })
  const gateway = createGatewayController(async () => { throw error })
  assert.deepEqual(await gateway.capability(), { version: 0, reason: 'load_failed' })
  assert.equal(reads, 0)
})

test('OpenClaw initialize diagnostics classify once without retry or changing ordinary ACP', async () => {
  const timeout = Object.assign(new Error(PRIVATE), { name: 'GatewayClientRequestTimeoutError' })
  for (const [probe, result, advertised] of [
    [() => CAPABILITY, 'ready', true], [() => { throw timeout }, 'timeout', false],
    [() => { throw new Error('gateway not connected') }, 'unavailable', false],
    [() => ({ version: 1, activeOnly: false, receipts: PRIVATE }), 'unsupported_contract', false],
    [() => { throw new Error(PRIVATE) }, 'failed', false],
    [() => ({ version: 0, reason: PRIVATE }), 'unavailable', false],
  ]) {
    const logs = await logsDuring(async () => {
      const h = bridgeFixture(probe), response = await h.initialize()
      assert.equal(Boolean(response._meta), advertised)
      assert.equal(response.protocolVersion, 1); assert.equal(response.original, true)
      assert.equal(h.calls.length, 1); assert.deepEqual(h.calls[0].options, { timeoutMs: 2000 })
      h.agent.handleGatewayReconnect()
      const active = h.start(); await tick(); const terminal = await h.finish(); assert.deepEqual(await active, terminal)
      assert.equal(h.prompts, 1)
      assert.equal(h.calls.filter(call => call.method === RPC.capability).length, 1, 'diagnostics cannot add retries')
      if (!advertised) assert.equal(h.calls.length, 1)
    })
    const init = logs.filter(entry => entry.phase === 'initialize')
    assert.equal(init.length, 1); assert.equal(init[0].result, result)
    assert.ok(Number.isFinite(init[0].elapsedMs) && init[0].elapsedMs >= 0 && init[0].elapsedMs <= 86400000)
  }
})

test('OpenClaw readiness diagnostics deduplicate repeats and redact peer reasons and errors', async () => {
  const logs = await logsDuring(async () => {
    const h = bridgeFixture(); await h.initialize(); const active = h.start(); await tick()
    for (let i = 0; i < 12; i++) await h.advance({ version: 1, ready: false, reason: 'backend_missing' })
    await h.advance({ version: 1, ready: false, reason: 'backend_not_available' })
    for (let i = 0; i < 8; i++) await h.advance(Object.assign(new Error(PRIVATE), { name: 'GatewayProtocolRequestTimeoutError' }))
    await h.advance({ version: 1, ready: false, reason: PRIVATE })
    await h.advance({ version: 1, ready: true, nativeTurnId: PRIVATE })
    await h.advance({ version: 1, ready: true, nativeTurnId: PRIVATE })
    assert.equal(h.notifications.filter(item => item.params.ready).length, 1, 'notification deduplication is unchanged')
    assert.equal(h.calls.filter(call => call.method === RPC.capability).length, 1)
    const terminal = await h.finish(); assert.deepEqual(await active, terminal)
  })
  const readiness = logs.filter(entry => entry.phase === 'readiness')
  assert.deepEqual(readiness.slice(0, 5).map(entry => entry.reason), ['backend_missing', 'backend_not_available', 'rpc_timeout', 'inactive_or_unowned', 'ready'])
  assert.equal(readiness.filter(entry => entry.reason === 'backend_missing').length, 1)
  assert.ok(readiness.every(entry => entry.turn === 1 && !('sessionId' in entry) && !('hostTurnId' in entry) && !('nativeTurnId' in entry)))
})

test('OpenClaw readiness logs stay bounded during alternating reasons without stopping polling', async () => {
  const logs = await logsDuring(async () => {
    const h = bridgeFixture(); await h.initialize(); const active = h.start(); await tick()
    for (let i = 0; i < 100; i++) await h.advance({ version: 1, ready: false, reason: i % 2 ? 'backend_missing' : 'backend_not_available' })
    assert.equal(h.calls.filter(call => call.method === RPC.state).length, 101)
    const terminal = await h.finish(); assert.deepEqual(await active, terminal)
  })
  assert.equal(logs.filter(entry => entry.phase === 'readiness').length, 64)
})

test('OpenClaw diagnostic sink failure cannot affect initialization or the original prompt terminal', async () => {
  const original = console.error
  console.error = () => { throw new Error(PRIVATE) }
  try {
    const h = bridgeFixture(); assert.equal((await h.initialize())._meta['ai.myclaw/nativeSteering'].version, 1)
    const active = h.start(); await tick(); const terminal = await h.finish(); assert.deepEqual(await active, terminal)
  } finally { console.error = original }
})

test('OpenClaw identical readiness reasons are recorded independently for successive host turns', async () => {
  const logs = await logsDuring(async () => {
    const h = bridgeFixture(); await h.initialize()
    for (const host of [PRIVATE + '-A', PRIVATE + '-B']) {
      const active = h.start(host); await tick()
      const terminal = await h.finish(); assert.deepEqual(await active, terminal)
    }
    assert.equal(h.prompts, 2)
    assert.equal(h.calls.filter(call => call.method === RPC.capability).length, 1)
  })
  assert.deepEqual(logs.filter(entry => entry.phase === 'readiness' && entry.reason === 'backend_missing').map(entry => entry.turn), [1, 2])
})

test('OpenClaw request diagnostics retain only official code and detailCode whitelists', async () => {
  for (const [code, detailCode, include] of [
    ['FORBIDDEN', 'MISSING_SCOPE', true], ['UNAVAILABLE', 'AUTHENTICATED_PROFILE_UNAVAILABLE', true],
    ['INVALID_REQUEST', 'MISSING_SCOPE', true], ['CLIENT_TIMEOUT', 'AUTHENTICATED_PROFILE_UNAVAILABLE', true],
    [PRIVATE, PRIVATE, false],
  ]) {
    const logs = await logsDuring(async () => {
      const error = Object.assign(new Error(PRIVATE), { code, gatewayCode: code, details: { code: detailCode, prompt: PRIVATE, scopes: [PRIVATE] } })
      const h = bridgeFixture(() => { throw error }); await h.initialize()
      const active = h.start(); await tick(); await h.finish(); await active
      assert.equal(h.calls.length, 1)
    })
    const init = logs.find(entry => entry.phase === 'initialize')
    assert.equal(init.code, include ? code : undefined)
    assert.equal(init.detailCode, include ? detailCode : undefined)
    assert.equal(logs.filter(entry => entry.phase === 'prompt').length, 1)
    assert.deepEqual(logs.find(entry => entry.phase === 'prompt'), { phase: 'prompt', supported: false, tracking: false })
  }
})

test('OpenClaw binding-only diagnostics observe canonical changes without changing the captured request', async () => {
  const logs = await logsDuring(async () => {
    const h = bridgeFixture(); await h.initialize(); const active = h.start(); await tick()
    const state = { version: 1, ready: false, reason: 'backend_missing' }
    h.changeBinding(session => { session.sessionKey = PRIVATE + '-canonical' })
    await h.advance(state)
    h.changeBinding(session => { session.activeRunId = PRIVATE; session.abortController = new AbortController() })
    await h.advance(state)
    await h.advance(state)
    assert.ok(h.calls.filter(call => call.method === RPC.state).every(call => call.params.sessionKey === PRIVATE), 'diagnostics must not repair canonicalization')
    await h.finish(); await active
  })
  const reads = logs.filter(entry => entry.phase === 'readiness' && entry.reason === 'backend_missing')
  assert.deepEqual(reads.map(entry => [entry.sessionKeyChanged, entry.activeRunMatches, entry.controllerMatches]), [[false, true, true], [true, true, true], [true, false, false]])
})

test('OpenClaw prompt and disconnect diagnostics preserve original calls and omit raw arguments', async () => {
  const logs = await logsDuring(async () => {
    const h = bridgeFixture(); await h.initialize(); const active = h.start(); await tick()
    assert.equal(h.agent.handleGatewayDisconnect(PRIVATE), PRIVATE)
    await h.finish(); await active
    assert.equal(h.agent.shutdown(PRIVATE), PRIVATE)
  })
  assert.deepEqual(logs.find(entry => entry.phase === 'prompt'), { phase: 'prompt', supported: true, validHostMeta: true, tracking: true })
  assert.deepEqual(logs.filter(entry => entry.phase === 'connection'), [
    { phase: 'connection', event: 'gateway_disconnect', activeTurns: 1 }, { phase: 'connection', event: 'shutdown', activeTurns: 0 },
  ])
})

function bootFixture({ args = ['acp'], supported = true, root = '/fixed', checkedFails = false, badSource = false, sinkFails = false } = {}) {
  const source = readFileSync(new URL('./acp-bootstrap.mjs', import.meta.url), 'utf8')
    .replace(/^import .*$/gm, '').replace(/^export /gm, '').replaceAll('import.meta.url', JSON.stringify('file:///owned/acp-bootstrap.mjs'))
  const original = '\t\tagent.start();\n\t\treturn agent;'
  const lines = []; let hook
  const context = { moduleApi: supported ? { registerHooks(value) { hook = value } } : {}, pathToFileURL,
    checkedFile() { if (checkedFails) throw new Error(PRIVATE); return { path: '/fixed/server.mjs' } },
    packageRoot: () => root, digest: value => value === original ? 'audited' : 'not-audited', FILES: { server: ['server.mjs', 'audited'] },
    process: { argv: ['node', PRIVATE, ...args] }, URL, Buffer,
    console: { error(line) { if (sinkFails) throw new Error(PRIVATE); lines.push(line) } },
  }
  vm.runInNewContext(source, context)
  return { lines, get hook() { return hook }, original, load() { return hook.load('file:///fixed/server.mjs', {}, () => ({ source: badSource ? PRIVATE : original })) } }
}

test('OpenClaw boot diagnostics expose only fixed skip reasons with no paths or arguments', () => {
  for (const [options, reason] of [
    [{ args: [PRIVATE] }, 'not_acp'], [{ supported: false }, 'node_unsupported'], [{ root: null }, 'version_guard'], [{ checkedFails: true }, 'bundle_guard'],
  ]) {
    const h = bootFixture(options)
    assert.equal(h.hook, undefined); assert.equal(h.lines.length, 1)
    assert.equal(h.lines[0].includes(PRIVATE), false)
    assert.deepEqual(JSON.parse(h.lines[0].slice('[codeg-openclaw-native] '.length)), { phase: 'boot', event: 'skipped', reason })
  }
})

test('OpenClaw real boot hook records its first patch result once and logging failure is harmless', () => {
  for (const badSource of [false, true]) {
    const h = bootFixture({ badSource })
    for (let i = 0; i < 100; i++) {
      const result = h.load()
      assert.equal(result.source.includes('__codegInstallNativeSteering(agent, conn, gateway);'), !badSource)
    }
    assert.equal(h.lines.length, 2)
    assert.equal(h.lines.join('\n').includes(PRIVATE), false)
    const events = h.lines.map(line => JSON.parse(line.slice('[codeg-openclaw-native] '.length)))
    assert.equal(events[0].event, 'registered')
    assert.equal(events[1].event, badSource ? 'guard_rejected' : 'patch_applied')
  }
  const h = bootFixture({ sinkFails: true }); assert.ok(h.hook); assert.ok(h.load().source.includes('__codegInstallNativeSteering'))
})

test('OpenClaw diagnostic observation does not add metadata, error or binding getter evaluations', async () => {
  await logsDuring(async () => {
    let unsupportedReads = 0
    const unsupported = bridgeFixture(() => ({ version: 0 })); await unsupported.initialize()
    const meta = { get version() { unsupportedReads++; throw new Error(PRIVATE) } }
    const plain = unsupported.agent.prompt({ sessionId: PRIVATE, _meta: { 'ai.myclaw/nativeTurn': meta } })
    await tick(); await unsupported.finish(); await plain; assert.equal(unsupportedReads, 0)
    let versionReads = 0, hostReads = 0, bindingReads = 0
    const h = bridgeFixture(); await h.initialize()
    const active = h.agent.prompt({ sessionId: PRIVATE, _meta: { 'ai.myclaw/nativeTurn': {
      get version() { versionReads++; return 1 }, get hostTurnId() { hostReads++; return PRIVATE },
    } } })
    await tick(); assert.equal(versionReads, 1); assert.equal(hostReads, 3, 'same two guard reads and one original turn capture as 69')
    h.changeBinding(session => Object.defineProperty(session, 'sessionKey', { configurable: true, get() { bindingReads++; throw new Error(PRIVATE) } }))
    await h.advance({ version: 1, ready: false, reason: 'backend_missing' })
    assert.equal(bindingReads, 0); await h.finish(); await active
    let errorReads = 0
    const error = new Error(PRIVATE)
    for (const key of ['code', 'details', 'name']) Object.defineProperty(error, key, { get() { errorReads++; throw new Error(PRIVATE) } })
    const failed = bridgeFixture(() => { throw error }); await failed.initialize()
    assert.equal(errorReads, 0)
  })
})
