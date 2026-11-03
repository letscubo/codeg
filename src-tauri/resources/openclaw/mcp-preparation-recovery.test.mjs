// Fixed published method bodies with in-memory process/SDK request boundaries.
// No runtime, SDK, Agent, database, real child process or model is initialized.
import assert from 'node:assert/strict'
import { AsyncLocalStorage } from 'node:async_hooks'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { PassThrough } from 'node:stream'
import { test } from 'node:test'
import vm from 'node:vm'
import { digest, installMcpCloseObserver, resolveMcpSdkFiles } from './compat.mjs'
import { createGatewayController, RPC } from './native-gateway.mjs'
import { installAcpBridge } from './acp-bridge.mjs'

const saved = JSON.parse(readFileSync(new URL('./audited-mcp-preparation-fixture.json', import.meta.url)))
const sdk = JSON.parse(readFileSync(new URL('./audited-mcp-cleanup-fixture.json', import.meta.url)))
const deferred = () => {
  let resolve, reject
  const promise = new Promise((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
const tick = () => new Promise(resolve => setImmediate(resolve))

function failedMcpPreparation() {
  const initError = new Error('synthetic MCP initialize timed out after 30000ms')
  const identityError = new Error('synthetic service child cleanup identity lost')
  const root = deferred(), extinction = deferred()
  root.promise.catch(() => {}); extinction.promise.catch(() => {})
  let closed = false, observerReports = 0
  const logs = [], timeouts = []
  const process = {
    stdin: { end() {
      if (closed) return
      closed = true
      root.resolve({ code: 0, signal: null })
      extinction.reject(identityError)
    } },
    wait: () => root.promise,
    waitForExtinction: () => extinction.promise,
    onError() {}, onStdout() {}, onStderr() {}, dispose() {},
    kill() { throw new Error('immediate synthetic cleanup must not require a signal') },
  }
  const context = vm.createContext({
    Promise, Error, Map, Set, WeakMap, Object, Date, AbortController, AbortSignal,
    setTimeout, clearTimeout, AsyncLocalStorage, PassThrough,
    resolveGlobalSingleton: (_key, create) => create(),
    initError, identityError, logs, timeouts,
    fs: { mkdir() { throw new Error('filesystem preparation is forbidden in this fixture') } },
    createOwnedStdioProcess: async () => process,
    ReadBuffer: class { clear() {} },
    serializeMessage: message => JSON.stringify(message) + '\n',
    LATEST_PROTOCOL_VERSION: 'test', SUPPORTED_PROTOCOL_VERSIONS: ['test'], InitializeResultSchema: {},
    isMcpRequestTimeoutError: error => error === initError,
    createMcpJsonSchemaValidator: () => ({}),
    redactMcpDiagnosticError: error => error.message,
    logWarn: message => logs.push(message),
    getBundleMcpTestState: () => ({}),
    buildMcpClientCapabilities: () => ({}),
    createDisposedError: () => new Error('synthetic runtime retired'),
    getSessionMcpRequestSignal: () => undefined,
    mergeProcessEnv: sources => Object.assign({}, ...sources),
    getDefaultEnvironment: () => ({}),
    ErrorCode: { ConnectionClosed: 'closed' },
    McpError: { fromError: () => new Error('synthetic connection closed') },
  })
  vm.runInContext(`
    const BUNDLE_MCP_FAILURE_THRESHOLD=3, BUNDLE_MCP_FAILURE_COOLDOWN_MS=60000,
      BUNDLE_MCP_CATALOG_FAILURE_RETRY_MS=5000, BUNDLE_MCP_DISPOSE_TIMEOUT_MS=5000;
    class OwnedStdioCleanupError extends Error {}
    class Protocol {
      constructor(){
        this._responseHandlers=new Map(); this._progressHandlers=new Map();
        this._taskProgressTokens=new Map(); this._pendingDebouncedNotifications=new Map();
        this._timeoutInfo=new Map(); this._requestHandlerAbortControllers=new Map();
      }
      ${sdk.fragments.protocolConnect}
      ${sdk.fragments.protocolClose}
      ${saved.fragments.protocolCloseCallbacks}
    }
    class Client extends Protocol {
      constructor(){super(); this._capabilities={}; this._clientInfo={name:'fixture',version:'0'}}
      request(_request, _schema, options){timeouts.push(options.timeout);return Promise.reject(initError)}
      getServerCapabilities(){throw new Error('failed initialization must not acquire capabilities')}
      ${sdk.fragments.clientConnect}
    }
    ${saved.fragments.cleanupScope}
    ${saved.fragments.settlesWithin}
    ${saved.fragments.closeOwned}
    ${saved.fragments.stdioTransport}
    ${saved.fragments.mcpLifecycle}
    function disposeBundleMcpSession(session){return disposeMcpClient(session,getBundleMcpTestState().disposeTimeoutMs??BUNDLE_MCP_DISPOSE_TIMEOUT_MS)}
    ${saved.fragments.serverRuntime}
    const transport=new OpenClawStdioClientTransport({command:'memory-fake',env:{},stderr:'pipe',decoder:{clear(){}}});
    function resolveMcpTransport(){return {transport,transportType:'stdio',description:'memory-fake',connectionTimeoutMs:30000,requestTimeoutMs:60000}}
    globalThis.fixture={Client,Protocol,StdioTransport:OpenClawStdioClientTransport,
      createServerMcpRuntime,createAgentCleanupScope,transport};
  `, context)
  installMcpCloseObserver(context.fixture, () => { observerReports++ })
  const runtime = context.fixture.createServerMcpRuntime({
    serverName: 'failed-mcp', sessionId: 'same-session', sessionKey: 'same-key', cfg: {},
    serverConfig: { loaded: { mcpServers: { 'failed-mcp': {} } }, fingerprint: 'memory-only' },
  })
  return { runtime, scope: context.fixture.createAgentCleanupScope(), logs, timeouts,
    get observerReports() { return observerReports } }
}

test('failed MCP preparation preserves uncertainty and lets the same owned turn become steerable', async () => {
  const mcp = failedMcpPreparation(), signal = new AbortController()
  const p = { version: 1, sessionKey: 'same-key', gatewayRunId: 'same-run', hostTurnId: 'same-host' }
  const client = { connId: 'same-client' }
  const operation = { key: p.sessionKey, sessionId: 'same-session', phase: 'running' }
  const entry = { ownerConnId: client.connId, sessionKey: p.sessionKey,
    sessionId: operation.sessionId, controller: signal }
  const context = { chatAbortControllers: new Map([[p.gatewayRunId, entry]]) }
  const operations = new WeakMap([[signal.signal, operation]])
  const registry = new Map([[operation.key, operation]])
  let backend, queueCalls = 0, activeRegistrations = 0
  const native = createGatewayController(async () => ({ operations, registry, backend: () => backend }))
  const prepared = deferred(), finishA = deferred(), sleeps = [], notifications = []
  const sessionId = 'same-acp-session'
  const session = { sessionKey: p.sessionKey }
  const sessionStore = {
    setActiveRun(id, run, controller) {
      assert.equal(id, sessionId); activeRegistrations++
      Object.assign(session, { activeRunId: run, abortController: controller })
    },
    getSession(id) { assert.equal(id, sessionId); return session },
  }
  const agent = {
    promptStream: { sessionStore, pendingPrompts: new Map() },
    initialize: async () => ({}),
    prompt: async () => {
      sessionStore.setActiveRun(sessionId, p.gatewayRunId, signal)
      try {
        const catalog = await mcp.runtime.getCatalog()
        // Model only the later original backend registration. The controller,
        // entry, signal, operation, run and ACP host turn all stay the same.
        backend = { kind: 'embedded', runId: 'same-native-run', supportsTranscriptCommitWait: true,
          toolAuthorityFingerprint: 'same-authority', messageInjectionV2: {
            version: 2, isAvailable: () => true,
            queueMessage: async (_text, options, assertCurrent) => {
              assertCurrent(); queueCalls++
              assert.equal(options.waitForTranscriptCommit, true)
            },
          } }
        prepared.resolve(catalog)
        await finishA.promise
        return { stopReason: 'end_turn' }
      } catch (error) { prepared.reject(error); throw error }
    },
  }
  const gateway = { request: async (method, params) => {
    if (method === RPC.capability) return native.capability()
    if (method === RPC.state) return native.state(params, client, context)
    if (method === RPC.submit) return native.submit(params, client, context)
    throw new Error('ordinary prompts, cancel and reconnect are forbidden in this fixture')
  } }
  assert.equal(installAcpBridge(agent, { extNotification: (_method, state) => { notifications.push(state) } }, gateway,
    { sleep: () => new Promise(resolve => sleeps.push(resolve)) }), true)
  const unhandled = [], capture = reason => unhandled.push(reason)
  process.on('unhandledRejection', capture)
  let running
  try {
    const before = await native.state(p, client, context)
    assert.equal(before.ready, false)
    assert.equal(before.reason, 'backend_missing')
    await agent.initialize({})
    running = mcp.scope.run(() => agent.prompt({ sessionId,
      _meta: { 'ai.myclaw/nativeTurn': { version: 1, hostTurnId: p.hostTurnId } } }))
    const catalog = await prepared.promise
    {
      assert.equal(Object.keys(catalog.servers).length, 0)
      assert.equal(catalog.tools.length, 0)
      assert.equal(catalog.diagnostics.length, 1)
      assert.match(catalog.diagnostics[0].message, /timed out/)
      assert.equal(mcp.scope.outcome, 'uncertain')
      for (let i = 0; i < 8 && !notifications.some(state => state.ready); i++) {
        sleeps.splice(0).forEach(resolve => resolve())
        await tick()
      }
      const ready = await native.state(p, client, context)
      assert.equal(ready.ready, true)
      assert.match(ready.nativeTurnId, /^[a-f0-9]{64}$/)
      const published = notifications.find(state => state.ready)
      assert.equal(published?.nativeTurnId, ready.nativeTurnId)
      assert.equal(published.hostTurnId, p.hostTurnId)
      assert.strictEqual(context.chatAbortControllers.get(p.gatewayRunId), entry)
      assert.strictEqual(operations.get(signal.signal), operation)
      assert.strictEqual(registry.get(p.sessionKey), operation)
      assert.equal(mcp.scope.outcome, 'uncertain', 'native readiness never clears cleanup uncertainty')
      assert.equal((await native.state(p, { connId: 'other-owner' }, context)).ready, false)
      assert.equal((await native.state({ ...p, sessionKey: 'other-session' }, client, context)).ready, false)
      const input = { version: 1, sessionId, hostTurnId: p.hostTurnId,
        expectedNativeTurnId: published.nativeTurnId, prompt: [{ type: 'text', text: 'synthetic follow-up' }] }
      assert.equal((await agent.extMethod('_ai.myclaw/steer', { ...input,
        submissionId: 'wrong-host', hostTurnId: 'other-host' })).outcome, 'not_injected')
      assert.equal((await agent.extMethod('_ai.myclaw/steer', { ...input,
        submissionId: 'wrong-token', expectedNativeTurnId: 'other-token' })).outcome, 'not_injected')
      assert.equal(queueCalls, 0)
      const receipt = await agent.extMethod('_ai.myclaw/steer', { ...input, submissionId: 'same-turn-followup' })
      assert.equal(receipt.outcome, 'injected')
      assert.equal(queueCalls, 1)
      registry.set(p.sessionKey, { ...operation })
      assert.equal((await native.state(p, client, context)).ready, false, 'replacement operation cannot inherit A readiness')
      assert.equal((await agent.extMethod('_ai.myclaw/steer', { ...input,
        submissionId: 'replaced-operation' })).outcome, 'not_injected')
      assert.equal(queueCalls, 1)
      await assert.rejects(mcp.runtime.joinCleanup(), /could not confirm closure/)
      assert.equal(mcp.scope.outcome, 'uncertain')
    }
    finishA.resolve()
    assert.equal((await running).stopReason, 'end_turn')
    assert.equal(activeRegistrations, 1)
    assert.equal(session.activeRunId, p.gatewayRunId)
    assert.equal(signal.signal.aborted, false)
    await tick(); await tick()
    assert.equal(mcp.scope.outcome, 'uncertain')
    assert.equal(mcp.observerReports, 1)
    assert.deepEqual(mcp.timeouts, [30000], 'the original timeout contract is retained without waiting 30 seconds')
    assert.equal(unhandled.length, 0)
  } finally {
    finishA.resolve()
    await running?.catch(() => {})
    sleeps.splice(0).forEach(resolve => resolve())
    process.off('unhandledRejection', capture)
  }
})

const auditRoot = process.env.CODEG_OPENCLAW_AUDIT_PACKAGE
test('preparation fragments remain exact published source without importing runtime modules', { skip: !auditRoot }, () => {
  const contents = {}
  for (const [name, source] of Object.entries(saved.sourceFiles)) {
    const text = readFileSync(join(auditRoot, 'dist', source.path), 'utf8')
    assert.equal(digest(text), source.sha256)
    contents[name] = text
  }
  for (const [key, owner] of Object.entries({ serverRuntime: 'runtime', stdioTransport: 'lifecycle',
    mcpLifecycle: 'lifecycle', settlesWithin: 'owned', closeOwned: 'owned', cleanupScope: 'cleanup' })) {
    assert.ok(contents[owner].includes(saved.fragments[key]), key)
  }
  assert.equal(saved.sdkProtocolSha256, sdk.files.protocol)
  const files = resolveMcpSdkFiles(auditRoot)
  assert.ok(files.protocol.source.includes(saved.fragments.protocolCloseCallbacks))
})
