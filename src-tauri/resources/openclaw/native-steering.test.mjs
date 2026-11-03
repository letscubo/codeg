import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import vm from 'node:vm'
import { createGatewayController, RPC } from './native-gateway.mjs'
import { installAcpBridge } from './acp-bridge.mjs'

const fixture = JSON.parse(readFileSync(new URL('./audited-consumer-fixture.json', import.meta.url)))
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r }); return { promise, resolve } }
const tick = () => new Promise(resolve => setImmediate(resolve))

// Execute the published OpenClaw V2 queueMessage and consumer, rather than a
// fake queue implementation that might accidentally ignore assertCurrent.
function nativeRuntime() {
  const subscribers = new Set()
  const queued = []
  const transcript = []
  const activeSession = {
    agent: { cancelSteeringMessage(predicate) { const i = queued.findIndex(predicate); return i < 0 ? null : queued.splice(i, 1)[0] } },
    subscribe(listener) { subscribers.add(listener); return () => subscribers.delete(listener) },
    async steer(text, _images, _recorder, _media, _imageOrder, queueIdentity, canInject) {
      if (!canInject()) throw new Error('stale actual consumer guard')
      queued.push({ text, queueIdentity })
    },
  }
  const scope = {
    setTimeout, clearTimeout, crypto: { randomUUID }, input: { activeSession }, attempt: { sessionKey: 'agent:main:test' },
    activeQueueAdmissions: 0, canInject: () => true, log$6: { warn() {} },
    getSteeringMessageIdentity: message => message?.queueIdentity,
    retireQueuedUserMessage: () => true, toErrorObject: error => error,
    subscribeSteeringMessagePersistenceFailure: () => () => {}, hasPromptImageInput: () => false,
    claimEmbeddedPendingUserInputAnswer() { throw new Error('ask_user shortcut is forbidden') },
    EmbeddedSteeringAcceptedUnconfirmedError: class extends Error {},
  }
  vm.createContext(scope)
  vm.runInContext(fixture.fragments.consumer + '\n' + fixture.fragments.queue + '\nglobalThis.queue = queueMessage;', scope)
  return {
    queueMessage: scope.queue, queued, transcript,
    consume() { const message = queued.shift(); assert.ok(message); transcript.push({ role: 'user', ...message }); for (const listener of [...subscribers]) listener({ type: 'message_end', message }) },
    terminal(type = 'agent_settled') { for (const listener of [...subscribers]) listener({ type }) },
  }
}
function setup({ timeout = 1000, limit = 8192 } = {}) {
  const native = nativeRuntime()
  const controller = new AbortController()
  const entry = { ownerConnId: 'original-acp', sessionKey: 'agent:main:test', sessionId: 'native-session', controller }
  const operation = { key: entry.sessionKey, sessionId: entry.sessionId, phase: 'running', toolAuthorityFingerprint: 'original-authority' }
  let backend = { kind: 'embedded', runId: 'native-A', supportsTranscriptCommitWait: true, toolAuthorityFingerprint: 'original-authority', messageInjectionV2: { version: 2, isAvailable: () => true, queueMessage: native.queueMessage } }
  const operations = new WeakMap([[controller.signal, operation]])
  const registry = new Map([[entry.sessionKey, operation]])
  const deps = { operations, registry, backend: () => backend }
  const client = { connId: 'original-acp' }
  const context = { chatAbortControllers: new Map([['gateway-A', entry]]) }
  const scope = { version: 1, sessionKey: entry.sessionKey, gatewayRunId: 'gateway-A', hostTurnId: 'host-A' }
  const gateway = createGatewayController(async () => deps, { deliveryTimeoutMs: timeout, limit })
  return { native, gateway, client, context, scope, operation, entry, deps, changeBackend(value) { backend = value }, get backend() { return backend } }
}
async function request(h, id = 'B', text = 'same text') {
  const state = await h.gateway.state(h.scope, h.client, h.context)
  assert.equal(state.ready, true)
  return { ...h.scope, expectedNativeTurnId: state.nativeTurnId, submissionId: id, text }
}

test('OpenClaw actual V2 consumer requires exact UUID message_end and appends B once', async () => {
  const h = setup()
  const p = await request(h)
  let resolved = false
  const result = h.gateway.submit(p, h.client, h.context).then(value => { resolved = true; return value })
  await tick()
  assert.equal(h.native.queued.length, 1)
  assert.equal(resolved, false, 'enqueue alone is not consumed')
  h.native.consume()
  assert.equal((await result).outcome, 'injected')
  assert.deepEqual(h.native.transcript.map(m => m.text), ['same text'])
  assert.equal((await h.gateway.submit(p, h.client, h.context)).outcome, 'injected')
  assert.equal(h.native.queued.length, 0)
})
test('OpenClaw exact owner, run, session and authority refuse before native write', async () => {
  for (const change of [
    h => { h.client.connId = 'another-client' },
    h => { h.entry.sessionKey = 'another-session' },
    h => { h.context.chatAbortControllers.delete('gateway-A') },
    h => { h.operation.phase = 'completed' },
    h => { h.changeBackend({ ...h.backend, runId: 'native-C' }) },
    h => { h.backend.toolAuthorityFingerprint = 'changed-authority' },
  ]) {
    const h = setup(); const p = await request(h); change(h)
    assert.equal((await h.gateway.submit(p, h.client, h.context)).outcome, 'not_injected')
    assert.equal(h.native.queued.length, 0)
  }
})
test('OpenClaw real composed assertCurrent rejects a runtime race without fallback', async () => {
  const h = setup()
  const p = await request(h)
  const original = h.backend.messageInjectionV2.queueMessage
  h.backend.messageInjectionV2.queueMessage = (...args) => { h.operation.phase = 'completed'; return original(...args) }
  assert.equal((await h.gateway.submit(p, h.client, h.context)).outcome, 'unconfirmed')
  assert.equal(h.native.queued.length, 0)
})
test('OpenClaw terminal, handoff and timeout remove exact unconsumed input, stay unknown', async () => {
  for (const kind of ['agent_settled', 'agent_handoff', 'timeout']) {
    const h = setup({ timeout: 15 }); const p = await request(h)
    const result = h.gateway.submit(p, h.client, h.context)
    await tick()
    if (kind === 'timeout') await new Promise(resolve => setTimeout(resolve, 25))
    else h.native.terminal(kind)
    assert.equal((await result).outcome, 'unconfirmed')
    assert.equal(h.native.queued.length, 0)
    assert.equal(h.native.transcript.length, 0)
  }
})
test('OpenClaw same-text inputs have distinct receipts and conflicts never resend', async () => {
  const h = setup(); const a = await request(h, 'B1'); const b = await request(h, 'B2')
  const first = h.gateway.submit(a, h.client, h.context)
  const second = h.gateway.submit(b, h.client, h.context)
  const duplicate = h.gateway.submit(a, h.client, h.context)
  await tick()
  assert.equal(h.native.queued.length, 2)
  assert.notEqual(h.native.queued[0].queueIdentity, h.native.queued[1].queueIdentity)
  assert.equal((await h.gateway.submit({ ...a, text: 'conflict' }, h.client, h.context)).outcome, 'unconfirmed')
  h.native.consume(); h.native.consume()
  assert.deepEqual((await Promise.all([first, second, duplicate])).map(r => r.outcome), ['injected', 'injected', 'injected'])
  assert.equal(h.native.transcript.length, 2)
})
test('OpenClaw queue ACK-shaped results and unknown dependencies never advertise consumption', async () => {
  const h = setup(); const p = await request(h)
  h.backend.messageInjectionV2.queueMessage = async () => ({ queued: true })
  assert.equal((await h.gateway.submit(p, h.client, h.context)).outcome, 'unconfirmed')
  const unknown = createGatewayController(async () => { throw new Error('unsupported bundle') })
  assert.deepEqual(await unknown.capability(), { version: 0, reason: 'load_failed' })
  assert.equal((await unknown.state(h.scope, h.client, h.context)).ready, false)
})
test('OpenClaw ended A, caller disconnect, provider failure and full ledger never resend', async () => {
  for (const change of [
    h => { h.entry.controller.abort() },
    h => { h.context.chatAbortControllers.set('gateway-A', { ...h.entry, ownerConnId: 'new-connection' }) },
  ]) {
    const h = setup(); const p = await request(h); change(h)
    assert.equal((await h.gateway.submit(p, h.client, h.context)).outcome, 'not_injected')
    assert.equal(h.native.queued.length, 0)
  }
  const h = setup({ limit: 1 }); const p = await request(h)
  h.backend.messageInjectionV2.queueMessage = async () => { throw new Error('provider failure after possible consumption') }
  assert.equal((await h.gateway.submit(p, h.client, h.context)).outcome, 'unconfirmed')
  assert.equal((await h.gateway.submit({ ...p, submissionId: 'B2' }, h.client, h.context)).outcome, 'not_injected')
  assert.equal((await h.gateway.submit({ submissionId: 'B' }, h.client, h.context)).outcome, 'unconfirmed')
})

test('OpenClaw missing Gateway plugin preserves original ACP prompt without capability', async () => {
  let ordinaryPrompts = 0
  const agent = { promptStream: { sessionStore: { setActiveRun() {} }, pendingPrompts: new Map() },
    async initialize() { return { protocolVersion: 1 } }, async prompt() { ordinaryPrompts++; return { stopReason: 'end_turn' } } }
  installAcpBridge(agent, { extNotification() { assert.fail('no native state without capability') } }, { async request() { throw new Error('method not found') } })
  assert.equal((await agent.initialize({}))._meta, undefined)
  assert.deepEqual(await agent.prompt({ sessionId: 's', _meta: { 'ai.myclaw/nativeTurn': { version: 1, hostTurnId: 'h' } } }), { stopReason: 'end_turn' })
  assert.equal(ordinaryPrompts, 1)
})

test('OpenClaw ACP keeps original Gateway object and A promise, retires exact native id', async () => {
  const h = setup()
  const completed = deferred()
  const notices = []
  const session = { sessionKey: h.scope.sessionKey }
  const store = { getSession: () => session, setActiveRun(_sid, id, controller) { Object.assign(session, { activeRunId: id, abortController: controller }) } }
  let ordinaryPrompts = 0
  const agent = {
    promptStream: { sessionStore: store, pendingPrompts: new Map() },
    async initialize() { return { protocolVersion: 1 } },
    async prompt() { ordinaryPrompts++; store.setActiveRun('acp-session', 'gateway-A', h.entry.controller); return completed.promise },
  }
  const calls = []
  const gateway = { async request(method, params) {
    calls.push(method)
    if (method === RPC.capability) return h.gateway.capability()
    if (method === RPC.state) return h.gateway.state(params, h.client, h.context)
    if (method === RPC.submit) return h.gateway.submit(params, h.client, h.context)
    throw new Error('unexpected ordinary RPC')
  } }
  installAcpBridge(agent, { extNotification: async (_method, params) => { notices.push(params) } }, gateway)
  assert.equal((await agent.initialize({}))._meta['ai.myclaw/nativeSteering'].version, 1)
  const original = agent.prompt({ sessionId: 'acp-session', _meta: { 'ai.myclaw/nativeTurn': { version: 1, hostTurnId: 'host-A' } } })
  await tick()
  const ready = notices.find(n => n.ready)
  assert.ok(ready?.nativeTurnId)
  const result = agent.extMethod('_ai.myclaw/steer', { version: 1, sessionId: 'acp-session', hostTurnId: 'host-A', expectedNativeTurnId: ready.nativeTurnId, submissionId: 'B', prompt: [{ type: 'text', text: 'new direction' }] })
  await tick(); h.native.consume()
  assert.equal((await result).outcome, 'injected')
  completed.resolve({ stopReason: 'end_turn', usage: { output: 7 }, tools: ['original-tool'] })
  assert.deepEqual(await original, { stopReason: 'end_turn', usage: { output: 7 }, tools: ['original-tool'] })
  const retired = notices.find(n => !n.ready)
  assert.equal(retired.nativeTurnId, ready.nativeTurnId)
  assert.equal(ordinaryPrompts, 1)
  assert.ok(calls.every(m => Object.values(RPC).includes(m)))
})

test('OpenClaw ACP stale input never writes, lost reply stays unknown and notify failure cannot replace A', async () => {
  const completed = deferred(), nextState = deferred(), notices = []
  const session = { sessionKey: 'exact-key' }, controller = new AbortController()
  const store = { getSession: () => session, setActiveRun(_sid, id, owner) { Object.assign(session, { activeRunId: id, abortController: owner }) } }
  let submits = 0, notificationsFail = false, reads = 0
  const agent = { promptStream: { sessionStore: store, pendingPrompts: new Map() },
    async initialize() { return {} }, async prompt() { store.setActiveRun('s', 'original-A', controller); return completed.promise } }
  installAcpBridge(agent, { extNotification(_method, params) { if (notificationsFail) throw new Error('closed transport'); notices.push(params) } }, {
    async request(method) {
      if (method === RPC.capability) return { version: 1, activeOnly: true, receipts: 'consumed' }
      if (method === RPC.state) return reads++ === 0 ? { version: 1, ready: true, nativeTurnId: 'native-A' } : nextState.promise
      if (method === RPC.submit) { submits++; throw new Error('reply lost after possible consumption') }
      throw new Error('ordinary dispatch is forbidden')
    },
  })
  await agent.initialize({})
  const original = agent.prompt({ sessionId: 's', _meta: { 'ai.myclaw/nativeTurn': { version: 1, hostTurnId: 'host-A' } } })
  await tick()
  const p = { version: 1, sessionId: 's', hostTurnId: 'host-A', expectedNativeTurnId: 'native-A', submissionId: 'B', prompt: [{ type: 'text', text: 'B' }] }
  assert.equal((await agent.extMethod('_ai.myclaw/steer', { ...p, expectedNativeTurnId: 'native-stale' })).outcome, 'not_injected')
  assert.equal(submits, 0)
  assert.equal((await agent.extMethod('_ai.myclaw/steer', p)).outcome, 'unconfirmed')
  assert.equal(submits, 1)
  notificationsFail = true
  completed.resolve({ stopReason: 'end_turn', originalA: true })
  assert.deepEqual(await original, { stopReason: 'end_turn', originalA: true })
  assert.equal((await agent.extMethod('_ai.myclaw/steer', p)).outcome, 'not_injected')
  assert.equal(submits, 1)
  assert.equal(notices.filter(n => n.ready).length, 1)
  nextState.resolve({ version: 1, ready: false })
})
