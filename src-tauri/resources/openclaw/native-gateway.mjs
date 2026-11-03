// Pure owner: no config, database, model or networking imports.
import { createHash, randomUUID } from 'node:crypto'

export const RPC = {
  capability: 'ai.myclaw.nativeSteering.capabilities',
  state: 'ai.myclaw.nativeSteering.state',
  submit: 'ai.myclaw.nativeSteering.submit',
}
export const CAPABILITY = { version: 1, activeOnly: true, receipts: 'consumed' }
const text = value => typeof value === 'string' && value.length > 0 && value.length <= 512
const validScope = p => p?.version === 1 && ['sessionKey', 'gatewayRunId', 'hostTurnId'].every(k => text(p[k]))
const reply = (p, outcome) => ({ version: 1, submissionId: p?.submissionId ?? '', hostTurnId: p?.hostTurnId ?? '', nativeTurnId: p?.expectedNativeTurnId ?? '', outcome })
const loadFailureReason = error => {
  try {
    const descriptor = error && Object.getOwnPropertyDescriptor(error, 'message')
    const message = descriptor && Object.hasOwn(descriptor, 'value') ? descriptor.value : undefined
    if (message === 'unsupported_openclaw_version') return 'unsupported_runtime'
    if (message === 'unsupported_openclaw_bundle') return 'unsupported_bundle'
    if (message === 'unsupported_openclaw_exports') return 'unsupported_exports'
  } catch {}
  return 'load_failed'
}

export function createGatewayController(loadInternals, { limit = 8192, deliveryTimeoutMs = 120000 } = {}) {
  let internals
  const ids = new WeakMap()
  const submissions = new Map()
  const identity = object => {
    if (!ids.has(object)) ids.set(object, randomUUID())
    return ids.get(object)
  }
  const load = async () => internals ??= await loadInternals()

  // Every check uses the exact original request owner and original runtime
  // objects. Looking up a newer run by session is deliberately forbidden.
  function capture(p, client, context, deps, diagnostic) {
    const reject = reason => { if (diagnostic) diagnostic.reason = reason; return null }
    if (!validScope(p) || !text(client?.connId)) return reject('inactive_or_unowned')
    const entry = context?.chatAbortControllers?.get(p.gatewayRunId)
    if (!entry || entry.ownerConnId !== client.connId || entry.sessionKey !== p.sessionKey ||
        entry.registrationCleanupRequested || entry.controller?.signal?.aborted) return reject('inactive_or_unowned')
    const operation = deps.operations.get(entry.controller.signal)
    if (!operation || operation.key !== p.sessionKey || operation.sessionId !== entry.sessionId ||
        operation.phase !== 'running' || operation.result || deps.registry.get(operation.key) !== operation) return reject('inactive_or_unowned')
    const backend = deps.backend(operation)
    // This first release audits the embedded OpenClaw implementation only.
    // Its success waits for the matching queueIdentity's user message_end.
    if (!backend) return reject('backend_missing')
    if (backend.kind !== 'embedded' || !text(backend.runId) ||
        backend.supportsTranscriptCommitWait !== true || backend.messageInjectionV2?.version !== 2 ||
        typeof backend.messageInjectionV2.queueMessage !== 'function') return reject('backend_unsupported')
    if (backend.messageInjectionV2.isAvailable?.() !== true) return reject('backend_not_available')
    const fingerprint = backend.toolAuthorityFingerprint ?? operation.toolAuthorityFingerprint
    if (!text(fingerprint)) return reject('authority_missing')
    const nativeTurnId = createHash('sha256').update(JSON.stringify([
      p.gatewayRunId, entry.sessionId, backend.runId, fingerprint, identity(entry), identity(operation), identity(backend),
    ])).digest('hex')
    const current = () => {
      try {
        const next = capture(p, client, context, deps)
        return next?.entry === entry && next.operation === operation && next.backend === backend &&
          next.fingerprint === fingerprint && next.nativeTurnId === nativeTurnId
      } catch { return false }
    }
    return { entry, operation, backend, fingerprint, nativeTurnId, current }
  }
  async function capability() {
    try { await load(); return CAPABILITY } catch (error) { return { version: 0, reason: loadFailureReason(error) } }
  }
  async function state(p, client, context) {
    try {
      const diagnostic = { reason: 'inactive_or_unowned' }
      const owner = capture(p, client, context, await load(), diagnostic)
      return { version: 1, ready: Boolean(owner), ...(owner ? { nativeTurnId: owner.nativeTurnId } : { reason: diagnostic.reason }) }
    } catch { return { version: 1, ready: false, reason: 'inactive_or_unowned' } }
  }
  async function submit(p, client, context) {
    const fingerprint = JSON.stringify([client?.connId, p?.sessionKey, p?.gatewayRunId, p?.hostTurnId, p?.expectedNativeTurnId, p?.text])
    const old = submissions.get(p?.submissionId)
    if (old) return old.fingerprint === fingerprint ? old.promise : reply(p, 'unconfirmed')
    if (!validScope(p) || !text(p?.submissionId) || !text(p?.expectedNativeTurnId) ||
        typeof p?.text !== 'string' || !p.text.trim() || Buffer.byteLength(p.text, 'utf8') > 65536) {
      return reply(p, 'not_injected')
    }
    if (submissions.size >= limit) return reply(p, 'not_injected')
    // Claim the submission before any await. Never evict and accidentally send
    // an old input twice; process restart leaves the caller unconfirmed.
    let settle
    const promise = new Promise(resolve => { settle = resolve })
    submissions.set(p.submissionId, { fingerprint, promise })
    let owner
    try { owner = capture(p, client, context, await load()) } catch {}
    if (!owner || owner.nativeTurnId !== p.expectedNativeTurnId || !owner.current()) {
      settle(reply(p, 'not_injected')); return promise
    }
    let called = false
    try {
      const assertCurrent = () => { if (!owner.current()) throw new Error('stale_native_turn') }
      assertCurrent()
      called = true
      const result = await owner.backend.messageInjectionV2.queueMessage(p.text, {
        steeringMode: 'all',
        // Avoid the separate ask_user-answer shortcut: success must be the
        // matching steering user message_end, not a question-claim ACK.
        isInboundUserMessage: false,
        toolAuthorityFingerprint: owner.fingerprint,
        waitForTranscriptCommit: true,
        queueIdentity: randomUUID(),
        deliveryTimeoutMs,
      }, assertCurrent, 'run')
      settle(reply(p, result === undefined ? 'injected' : 'unconfirmed'))
    } catch {
      // Runtime errors can follow consumption/persistence. Even cancellation
      // success is conservatively unknown unless refusal preceded the call.
      settle(reply(p, called ? 'unconfirmed' : 'not_injected'))
    }
    return promise
  }
  return { capability, state, submit }
}

export function registerGatewayMethods(api, loadInternals) {
  const controller = createGatewayController(loadInternals)
  for (const [name, method] of Object.entries(RPC)) {
    api.registerGatewayMethod(method, async ({ params, client, context, respond }) => {
      respond(true, await controller[name](params, client, context))
    }, { scope: 'operator.write' })
  }
  return controller
}
