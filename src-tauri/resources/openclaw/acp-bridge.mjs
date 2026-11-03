import { RPC, CAPABILITY } from './native-gateway.mjs'

const META = 'ai.myclaw/nativeSteering'
const TURN = 'ai.myclaw/nativeTurn'
const STATE = '_ai.myclaw/nativeSteeringState'
const STEER = '_ai.myclaw/steer'
const delay = ms => new Promise(resolve => { const timer = setTimeout(resolve, ms); timer.unref?.() })
const response = (p, outcome) => ({ version: 1, submissionId: p?.submissionId ?? '', hostTurnId: p?.hostTurnId ?? '', nativeTurnId: p?.expectedNativeTurnId ?? '', outcome })
const stateReasons = new Set(['inactive_or_unowned', 'backend_missing', 'backend_unsupported', 'backend_not_available', 'authority_missing'])
const loadReasons = new Set(['unsupported_runtime', 'unsupported_bundle', 'unsupported_exports', 'load_failed'])
const errorCodes = new Set(['FORBIDDEN', 'UNAVAILABLE', 'INVALID_REQUEST', 'CLIENT_TIMEOUT'])
const detailCodes = new Set(['MISSING_SCOPE', 'AUTHENTICATED_PROFILE_UNAVAILABLE'])
const elapsed = started => Math.max(0, Math.min(86400000, Date.now() - started))
const diagnostic = fields => { try { console.error('[codeg-openclaw-native] ' + JSON.stringify(fields)) } catch {} }
const dataDescriptor = (object, key) => {
  try {
    const descriptor = object && Object.getOwnPropertyDescriptor(object, key)
    return descriptor && Object.hasOwn(descriptor, 'value') ? descriptor : undefined
  } catch { return undefined }
}
const dataValue = (object, key) => dataDescriptor(object, key)?.value
const requestFailure = error => {
  try {
    if (['GatewayClientRequestTimeoutError', 'GatewayProtocolRequestTimeoutError'].includes(dataValue(error, 'name')) || ['TIMEOUT', 'CLIENT_TIMEOUT'].includes(dataValue(error, 'code'))) return 'timeout'
    if (dataValue(error, 'message') === 'gateway not connected' || ['UNAVAILABLE', 'METHOD_NOT_FOUND'].includes(dataValue(error, 'code'))) return 'unavailable'
  } catch {}
  return 'failed'
}
const requestCodes = error => {
  try {
    const code = dataValue(error, 'code') ?? dataValue(error, 'gatewayCode'), detailCode = dataValue(dataValue(error, 'details'), 'code')
    return { ...(errorCodes.has(code) ? { code } : {}), ...(detailCodes.has(detailCode) ? { detailCode } : {}) }
  } catch { return {} }
}
const probeDiagnostic = (supported, cap, failed, error) => {
  try {
    const result = failed ? requestFailure(error) : supported ? 'ready' : dataValue(cap, 'version') === 0 ? 'unavailable' : 'unsupported_contract'
    return { result, ...(failed ? requestCodes(error) : {}), ...(!failed && dataValue(cap, 'version') === 0 ? { reason: loadReasons.has(dataValue(cap, 'reason')) ? dataValue(cap, 'reason') : 'load_failed' } : {}) }
  } catch { return { result: 'failed' } }
}
const readinessReason = (ready, state) => {
  try {
    return ready ? 'ready' : dataValue(state, 'version') !== 1 || dataValue(state, 'ready') === true ? 'unsupported_contract'
      : stateReasons.has(dataValue(state, 'reason')) ? dataValue(state, 'reason') : 'inactive_or_unowned'
  } catch { return 'rpc_failed' }
}

// Invoked inside the original serveAcpGateway callback, before the ACP SDK
// registers handlers. No second ACP agent or Gateway client is created.
export function installAcpBridge(agent, connection, gateway, { sleep = delay } = {}) {
  const stream = agent.promptStream
  if (!stream?.sessionStore?.setActiveRun || !stream.pendingPrompts || !gateway?.request) return false
  const turns = new Map()
  let supported = false
  let diagnosticTurn = 0
  const initialize = agent.initialize.bind(agent)
  agent.initialize = async params => {
    const result = await initialize(params)
    const started = Date.now()
    let cap, failed = false, failure
    try {
      cap = await gateway.request(RPC.capability, { version: 1 }, { timeoutMs: 2000 })
      supported = cap?.version === 1 && cap.activeOnly === true && cap.receipts === 'consumed'
    } catch (error) { supported = false; failed = true; failure = error }
    diagnostic({ phase: 'initialize', ...probeDiagnostic(supported, cap, failed, failure), elapsedMs: elapsed(started) })
    return supported ? { ...result, _meta: { ...result._meta, [META]: CAPABILITY } } : result
  }
  const noteReadiness = (turn, reason, error) => {
    try {
      // Observe the SAME mutable session captured by the original getSession;
      // calling that getter again would change upstream lastTouchedAt.
      const session = turn.diagnosticSession
      const descriptors = ['sessionKey', 'activeRunId', 'abortController'].map(key => dataDescriptor(session, key))
      const binding = descriptors.every(Boolean) ? { sessionKeyChanged: descriptors[0].value !== turn.sessionKey,
        activeRunMatches: descriptors[1].value === turn.gatewayRunId,
        controllerMatches: descriptors[2].value === turn.diagnosticController } : {}
      const fields = { reason, ...requestCodes(error), ...binding }
      const key = JSON.stringify(fields)
      if (turn.lastDiagnosticState === key) return
      turn.lastDiagnosticState = key
      // A pathological alternating peer cannot create an unbounded stderr stream.
      if (turn.diagnosticCount >= 64) return
      turn.diagnosticCount++
      diagnostic({ phase: 'readiness', turn: turn.diagnosticTurn, ready: reason === 'ready', ...fields, elapsedMs: elapsed(turn.diagnosticStarted) })
    } catch {}
  }
  const notify = (turn, ready, nativeTurnId = turn.nativeTurnId ?? '') => {
    // Retire the last published identity before clearing it. Never send an
    // empty/undefined native id: Rust cannot attribute that to a live turn.
    if (!ready && !turn.nativeTurnId) return
    if (ready && (typeof nativeTurnId !== 'string' || !nativeTurnId)) return
    const state = { version: 1, sessionId: turn.sessionId, hostTurnId: turn.hostTurnId, nativeTurnId, ready }
    const key = JSON.stringify(state)
    if (turn.lastState === key) return
    turn.lastState = key
    turn.nativeTurnId = ready ? nativeTurnId : null
    try { Promise.resolve(connection.extNotification(STATE, state)).catch(() => {}) } catch {}
  }
  const active = turn => !turn.closed && turns.get(turn.sessionId) === turn && !turn.signal?.aborted
  async function monitor(turn) {
    while (active(turn)) {
      try {
        const state = await gateway.request(RPC.state, {
          version: 1, sessionKey: turn.sessionKey, gatewayRunId: turn.gatewayRunId, hostTurnId: turn.hostTurnId,
        }, { timeoutMs: 2000 })
        if (!active(turn)) break
        const ready = state?.version === 1 && state.ready === true && typeof state.nativeTurnId === 'string' && state.nativeTurnId.length > 0
        noteReadiness(turn, readinessReason(ready, state))
        notify(turn, ready, state?.nativeTurnId)
      } catch (error) {
        if (!active(turn)) break
        noteReadiness(turn, 'rpc_' + requestFailure(error), error)
        notify(turn, false)
      }
      await sleep(500)
    }
    noteReadiness(turn, 'inactive_or_unowned')
    notify(turn, false)
  }
  const store = stream.sessionStore
  const setActiveRun = store.setActiveRun.bind(store)
  store.setActiveRun = (sessionId, runId, controller) => {
    const result = setActiveRun(sessionId, runId, controller)
    const turn = turns.get(sessionId)
    if (supported && turn && !turn.gatewayRunId) {
      const session = store.getSession(sessionId)
      if (session?.activeRunId === runId && session.abortController === controller) {
        Object.assign(turn, { gatewayRunId: runId, sessionKey: session.sessionKey, signal: controller.signal })
        Object.assign(turn, { diagnosticSession: session, diagnosticController: controller })
        monitor(turn).catch(() => notify(turn, false))
      }
    }
    return result
  }
  const prompt = agent.prompt.bind(agent)
  agent.prompt = async params => {
    const meta = params?._meta?.[TURN]
    if (!supported) { diagnostic({ phase: 'prompt', supported, tracking: false }); return prompt(params) }
    const validHostMeta = meta?.version === 1 && typeof meta.hostTurnId === 'string' && Boolean(meta.hostTurnId)
    if (!validHostMeta) { diagnostic({ phase: 'prompt', supported, validHostMeta, tracking: false }); return prompt(params) }
    // Codeg's admission gate serializes prompts; this also refuses accidental
    // duplicate ownership without invoking OpenClaw's cancel/replace behavior.
    const alreadyActive = turns.has(params.sessionId)
    diagnostic({ phase: 'prompt', supported, validHostMeta, tracking: !alreadyActive })
    if (alreadyActive) throw new Error('codeg_native_prompt_already_active')
    const turn = { sessionId: params.sessionId, hostTurnId: meta.hostTurnId, closed: false,
      diagnosticTurn: ++diagnosticTurn, diagnosticCount: 0, diagnosticStarted: Date.now() }
    turns.set(params.sessionId, turn)
    try { return await prompt(params) } finally {
      turn.closed = true
      notify(turn, false)
      if (turns.get(params.sessionId) === turn) turns.delete(params.sessionId)
    }
  }
  const previousExtension = agent.extMethod?.bind(agent)
  agent.extMethod = async (method, params) => {
    if (method !== STEER) {
      if (previousExtension) return previousExtension(method, params)
      throw new Error('unsupported_extension')
    }
    const turn = turns.get(params?.sessionId)
    if (!supported || params?.version !== 1 || !turn || !active(turn) ||
        turn.hostTurnId !== params.hostTurnId || !turn.nativeTurnId || turn.nativeTurnId !== params.expectedNativeTurnId ||
        typeof params.submissionId !== 'string' || !params.submissionId || !Array.isArray(params.prompt) ||
        !params.prompt.length || params.prompt.some(block => block?.type !== 'text' || typeof block.text !== 'string')) {
      return response(params, 'not_injected')
    }
    try {
      const result = await gateway.request(RPC.submit, {
        version: 1, sessionKey: turn.sessionKey, gatewayRunId: turn.gatewayRunId,
        hostTurnId: turn.hostTurnId, expectedNativeTurnId: params.expectedNativeTurnId,
        submissionId: params.submissionId, text: params.prompt.map(block => block.text).join('\n'),
      }, { timeoutMs: null })
      if (result?.version === 1 && result.submissionId === params.submissionId && result.hostTurnId === params.hostTurnId &&
          result.nativeTurnId === params.expectedNativeTurnId && ['injected', 'not_injected', 'unconfirmed'].includes(result.outcome)) return result
    } catch {}
    return response(params, 'unconfirmed')
  }
  for (const name of ['handleGatewayDisconnect', 'shutdown']) {
    const original = agent[name]?.bind(agent)
    if (original) agent[name] = (...args) => {
      diagnostic({ phase: 'connection', event: name === 'shutdown' ? 'shutdown' : 'gateway_disconnect', activeTurns: turns.size })
      for (const turn of turns.values()) { turn.closed = true; notify(turn, false) }
      return original(...args)
    }
  }
  return true
}
