// Production owned controller/ACP bridge/lifecycle observer, fake in-memory
// registry and transport only. Never imports OpenClaw, SDKs or database owners.
import assert from 'node:assert/strict'
import { test } from 'node:test'
import { createGatewayController, CAPABILITY, RPC } from './native-gateway.mjs'
import { installAcpBridge } from './acp-bridge.mjs'
import { installBackendLifecycleObserver } from './backend-lifecycle.mjs'

const tick = () => new Promise(resolve => setImmediate(resolve))
function setup({ legacy = false, oldOperation = false } = {}) {
  const attached = new WeakMap(), operations = new WeakMap(), registry = new Map()
  const controller = new AbortController()
  const entry = { ownerConnId: 'original-client', sessionKey: 'agent:fixture:codeg-209', sessionId: 'gateway-session', controller }
  const operation = {
    key: entry.sessionKey, sessionId: entry.sessionId, phase: 'running', toolAuthorityFingerprint: 'authority',
    attachBackend(handle) { attached.set(this, handle) }, detachBackend() { attached.delete(this) },
  }
  if (oldOperation) operations.set(controller.signal, operation)
  const backend = operation => attached.get(operation)
  const firstBackend = installBackendLifecycleObserver(operations, backend)
  if (!oldOperation) operations.set(controller.signal, operation)
  registry.set(operation.key, operation)
  let consumed = 0
  const handle = runId => ({ kind: 'embedded', runId, toolAuthorityFingerprint: 'authority', supportsTranscriptCommitWait: true,
    messageInjectionV2: { version: 2, isAvailable: () => true, queueMessage: async (_text, options, assertCurrent) => {
      assertCurrent(); assert.equal(options.waitForTranscriptCommit, true); consumed++
    } },
  })
  const deps = { operations, registry, backend, ...(legacy ? {} : { firstBackend }) }
  const gateway = createGatewayController(async () => deps)
  const scope = { version: 1, sessionKey: operation.key, gatewayRunId: 'gateway-A', hostTurnId: 'host-A' }
  const context = { chatAbortControllers: new Map([[scope.gatewayRunId, entry]]) }, client = { connId: entry.ownerConnId }
  return { attached, operation, handle, deps, gateway, scope, context, client, entry, get consumed() { return consumed } }
}

function bridge(h, capability) {
  const session = { sessionKey: h.scope.sessionKey }, notifications = [], sleeps = []
  let finish, prompts = 0
  const sessionStore = { getSession: () => session, setActiveRun(_session, runId, controller) {
    Object.assign(session, { activeRunId: runId, abortController: controller })
  } }
  const agent = { promptStream: { sessionStore, pendingPrompts: new Map() }, initialize: async () => ({ protocolVersion: 1 }),
    async prompt() {
      prompts++; sessionStore.setActiveRun('acp-A', h.scope.gatewayRunId, h.entry.controller)
      return new Promise(resolve => { finish = resolve })
    },
  }
  installAcpBridge(agent, { extNotification(method, state) { notifications.push({ method, state }) } }, {
    async request(method, params) {
      if (method === RPC.capability) return capability ?? h.gateway.capability()
      if (method === RPC.state) return h.gateway.state(params, h.client, h.context)
      if (method === RPC.submit) return h.gateway.submit(params, h.client, h.context)
      assert.fail('ordinary prompt, cancellation or reconnection is forbidden')
    },
  }, { sleep: () => new Promise(resolve => sleeps.push(resolve)) })
  return { agent, notifications,
    start: () => agent.prompt({ sessionId: 'acp-A', prompt: [{ type: 'text', text: 'A' }],
      _meta: { 'ai.myclaw/nativeTurn': { version: 1, hostTurnId: h.scope.hostTurnId } } }),
    async advance() { const wake = sleeps.shift(); assert.ok(wake); wake(); await tick() },
    async finish() { finish({ stopReason: 'end_turn' }); sleeps.splice(0).forEach(wake => wake()); await tick() },
    get prompts() { return prompts },
  }
}

test('OpenClaw preparation contract is advertised only with an installed lifecycle observer', async () => {
  const legacy = setup({ legacy: true }), current = setup()
  assert.strictEqual(await legacy.gateway.capability(), CAPABILITY)
  assert.deepEqual(await current.gateway.capability(), { ...CAPABILITY, preparationWait: 1 })
  for (const capability of [CAPABILITY, { ...CAPABILITY, preparationWait: true }, { ...CAPABILITY, preparationWait: 1 }]) {
    const b = bridge(setup(), capability), init = await b.agent.initialize({})
    assert.deepEqual(init._meta['ai.myclaw/nativeSteering'], capability.preparationWait === 1
      ? { ...CAPABILITY, preparationWait: 1 } : CAPABILITY)
  }
})

test('OpenClaw real first backend proof survives an attach and retirement missed by all polling', async () => {
  const h = setup(), first = h.handle('native-1'), second = h.handle('native-2')
  h.operation.attachBackend(first); h.operation.detachBackend(); h.operation.attachBackend(second)
  const state = await h.gateway.state(h.scope, h.client, h.context)
  assert.equal(state.ready, true)
  assert.equal(state.initialBackend, false, 'first observed by a poll must not impersonate the actual first backend')
  assert.strictEqual(h.deps.firstBackend(h.operation), first)
})

test('OpenClaw old operations and foreign owners never acquire preparation proof retroactively', async () => {
  const h = setup({ oldOperation: true })
  h.operation.attachBackend(h.handle('old-native'))
  assert.equal((await h.gateway.state(h.scope, h.client, h.context)).initialBackend, false)
  h.deps.firstBackend = () => assert.fail('foreign owners must not inspect lifecycle proof')
  assert.deepEqual(await h.gateway.state(h.scope, { connId: 'other-client' }, h.context),
    { version: 1, ready: false, reason: 'inactive_or_unowned' })
})

test('OpenClaw ACP forwards negotiated first-backend proof while keeping one original host prompt', async () => {
  const h = setup(), b = bridge(h), init = await b.agent.initialize({})
  assert.equal(init._meta['ai.myclaw/nativeSteering'].preparationWait, 1)
  const running = b.start(); await tick()
  assert.deepEqual(b.notifications, [])
  h.operation.attachBackend(h.handle('native-1')); await b.advance()
  const first = b.notifications.at(-1).state
  assert.equal(first.ready, true); assert.equal(first.initialBackend, true)
  assert.equal(first.sessionId, 'acp-A'); assert.equal(first.hostTurnId, 'host-A')
  h.operation.detachBackend(); await b.advance()
  assert.equal(b.notifications.at(-1).state.ready, false)
  h.operation.attachBackend(h.handle('native-2')); await b.advance()
  const second = b.notifications.at(-1).state
  assert.equal(second.ready, true); assert.equal(second.initialBackend, false)
  assert.notEqual(second.nativeTurnId, first.nativeTurnId)
  // A new explicit click against native-2 is still allowed. Only a submission
  // waiting for A's first native is forbidden from silently following it.
  const receipt = await b.agent.extMethod('_ai.myclaw/steer', {
    version: 1, sessionId: 'acp-A', hostTurnId: 'host-A', expectedNativeTurnId: second.nativeTurnId,
    submissionId: 'explicit-native-2', prompt: [{ type: 'text', text: 'explicit B' }],
  })
  assert.equal(receipt.outcome, 'injected'); assert.equal(h.consumed, 1)
  await b.finish(); assert.equal((await running).stopReason, 'end_turn')
  assert.equal(b.prompts, 1); assert.equal(h.entry.controller.signal.aborted, false)
})

test('OpenClaw legacy capability does not gain first-backend authority from state payloads alone', async () => {
  const h = setup(), b = bridge(h, CAPABILITY)
  assert.equal((await b.agent.initialize({}))._meta['ai.myclaw/nativeSteering'].preparationWait, undefined)
  const running = b.start(); await tick()
  h.operation.attachBackend(h.handle('native-1')); await b.advance()
  assert.equal(b.notifications.at(-1).state.ready, true)
  assert.equal(Object.hasOwn(b.notifications.at(-1).state, 'initialBackend'), false)
  await b.finish(); await running
})
