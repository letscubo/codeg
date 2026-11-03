import { RPC, CAPABILITY } from './native-gateway.mjs'

const META = 'ai.myclaw/nativeSteering'
const TURN = 'ai.myclaw/nativeTurn'
const STATE = '_ai.myclaw/nativeSteeringState'
const STEER = '_ai.myclaw/steer'
const delay = ms => new Promise(resolve => { const timer = setTimeout(resolve, ms); timer.unref?.() })
const response = (p, outcome) => ({ version: 1, submissionId: p?.submissionId ?? '', hostTurnId: p?.hostTurnId ?? '', nativeTurnId: p?.expectedNativeTurnId ?? '', outcome })

// Invoked inside the original serveAcpGateway callback, before the ACP SDK
// registers handlers. No second ACP agent or Gateway client is created.
export function installAcpBridge(agent, connection, gateway, { sleep = delay } = {}) {
  const stream = agent.promptStream
  if (!stream?.sessionStore?.setActiveRun || !stream.pendingPrompts || !gateway?.request) return false
  const turns = new Map()
  let supported = false
  const initialize = agent.initialize.bind(agent)
  agent.initialize = async params => {
    const result = await initialize(params)
    try {
      const cap = await gateway.request(RPC.capability, { version: 1 }, { timeoutMs: 2000 })
      supported = cap?.version === 1 && cap.activeOnly === true && cap.receipts === 'consumed'
    } catch { supported = false }
    return supported ? { ...result, _meta: { ...result._meta, [META]: CAPABILITY } } : result
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
        notify(turn, state?.version === 1 && state.ready === true && typeof state.nativeTurnId === 'string' && state.nativeTurnId.length > 0, state?.nativeTurnId)
      } catch {
        if (!active(turn)) break
        notify(turn, false)
      }
      await sleep(500)
    }
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
        monitor(turn).catch(() => notify(turn, false))
      }
    }
    return result
  }
  const prompt = agent.prompt.bind(agent)
  agent.prompt = async params => {
    const meta = params?._meta?.[TURN]
    if (!supported || meta?.version !== 1 || typeof meta.hostTurnId !== 'string' || !meta.hostTurnId) return prompt(params)
    // Codeg's admission gate serializes prompts; this also refuses accidental
    // duplicate ownership without invoking OpenClaw's cancel/replace behavior.
    if (turns.has(params.sessionId)) throw new Error('codeg_native_prompt_already_active')
    const turn = { sessionId: params.sessionId, hostTurnId: meta.hostTurnId, closed: false }
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
      for (const turn of turns.values()) { turn.closed = true; notify(turn, false) }
      return original(...args)
    }
  }
  return true
}
