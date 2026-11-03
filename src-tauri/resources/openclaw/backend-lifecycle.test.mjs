// Pure objects and a pinned source fragment only; no runtime/SDK/Agent/DB import.
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { test } from 'node:test'
import vm from 'node:vm'
import { installBackendLifecycleObserver } from './backend-lifecycle.mjs'

function fixture() {
  const operations = new WeakMap(), attached = new WeakMap()
  const backend = operation => attached.get(operation)
  const make = (attach = function (handle) { attached.set(this, handle) }) => ({ attachBackend: attach })
  return { operations, attached, backend, make }
}

test('backend proof retains attach1 between polls after detach1 and attach2', () => {
  const f = fixture(), nativeSet = WeakMap.prototype.set
  const firstBackend = installBackendLifecycleObserver(f.operations, f.backend)
  const operation = f.make(), first = {}, second = {}
  assert.strictEqual(f.operations.set({}, operation), f.operations)
  operation.attachBackend(first)
  f.attached.delete(operation)
  operation.attachBackend(second)
  // No state sampling took place before native2 became current.
  assert.strictEqual(firstBackend(operation), first)
  assert.strictEqual(f.backend(operation), second)
  assert.notStrictEqual(firstBackend(operation), f.backend(operation))
  assert.strictEqual(WeakMap.prototype.set, nativeSet)
})

test('old operations never acquire proof from attachment or repeated registration', () => {
  const f = fixture(), signal = {}, old = f.make()
  f.operations.set(signal, old)
  const firstBackend = installBackendLifecycleObserver(f.operations, f.backend)
  old.attachBackend({})
  f.attached.delete(old)
  f.operations.set(signal, old)
  old.attachBackend({})
  assert.equal(firstBackend(old), undefined)
  const alreadyAttached = f.make()
  alreadyAttached.attachBackend({})
  f.operations.set({}, alreadyAttached)
  assert.equal(firstBackend(alreadyAttached), undefined)
})

test('map observer preserves native return, receiver and exceptions', () => {
  const f = fixture(), firstBackend = installBackendLifecycleObserver(f.operations, f.backend)
  const other = new WeakMap(), operation = f.make(), key = {}
  assert.strictEqual(f.operations.set.call(other, key, operation), other)
  assert.strictEqual(other.get(key), operation)
  operation.attachBackend({})
  assert.equal(firstBackend(operation), undefined)
  assert.throws(() => f.operations.set.call({}, key, operation), TypeError)
  assert.throws(() => f.operations.set(1, operation), TypeError)
  assert.equal(firstBackend(null), undefined)
})

test('attach observer preserves this, arguments, return and original thrown error', () => {
  const f = fixture(), firstBackend = installBackendLifecycleObserver(f.operations, f.backend)
  const returned = Promise.resolve('same promise'), handle = {}, extra = {}, calls = []
  const operation = f.make(function (...args) {
    calls.push({ receiver: this, args })
    f.attached.set(operation, args[0])
    return returned
  })
  f.operations.set({}, operation)
  const receiver = {}
  assert.strictEqual(operation.attachBackend.call(receiver, handle, extra), returned)
  assert.deepEqual(calls, [{ receiver, args: [handle, extra] }])
  assert.strictEqual(firstBackend(operation), handle)
  const failure = new Error('synthetic original failure')
  const partial = f.make(function (next) {
    f.attached.set(this, next)
    if (next.fail) throw failure
    return next
  })
  f.operations.set({}, partial)
  assert.throws(() => partial.attachBackend({ fail: true }), error => error === failure)
  const second = {}
  assert.strictEqual(partial.attachBackend(second), second)
  assert.equal(firstBackend(partial), undefined, 'a partially attached failing native1 cannot certify native2')
})

test('unknown operation descriptors and observer failures only disable proof', () => {
  const f = fixture(), firstBackend = installBackendLifecycleObserver(f.operations, f.backend)
  let getterReads = 0
  const accessor = {}
  Object.defineProperty(accessor, 'attachBackend', { get() { getterReads++; throw Error('unreachable') } })
  const frozen = Object.freeze(f.make())
  for (const operation of [accessor, frozen, {}, null]) {
    assert.strictEqual(f.operations.set({}, operation), f.operations)
    assert.equal(firstBackend(operation), undefined)
  }
  assert.equal(getterReads, 0)
  let fail = false
  const other = fixture(), observe = installBackendLifecycleObserver(other.operations, operation => {
    if (fail) throw new Error('synthetic observation failure')
    return other.backend(operation)
  })
  const operation = other.make(function (handle) { other.attached.set(this, handle); return handle })
  other.operations.set({}, operation)
  fail = true
  const handle = {}
  assert.strictEqual(operation.attachBackend(handle), handle)
  assert.equal(observe(operation), undefined)
  fail = false
  operation.attachBackend({})
  assert.equal(observe(operation), undefined)
})

test('a reentrant attachment cannot certify native2 while native1 is still returning', () => {
  const f = fixture(), firstBackend = installBackendLifecycleObserver(f.operations, f.backend)
  const first = {}, second = {}, calls = []
  const operation = f.make(function (handle) {
    f.attached.set(this, handle)
    calls.push(handle)
    if (handle === first) this.attachBackend(second)
    return handle
  })
  f.operations.set({}, operation)
  assert.strictEqual(operation.attachBackend(first), first)
  assert.deepEqual(calls, [first, second])
  assert.strictEqual(f.backend(operation), second)
  assert.equal(firstBackend(operation), undefined)
})

test('install is idempotent across owned module instances and isolated by exact map', async () => {
  const f = fixture(), firstBackend = installBackendLifecycleObserver(f.operations, f.backend)
  const set = f.operations.set
  const duplicate = await import('./backend-lifecycle.mjs?duplicate-owner')
  assert.strictEqual(installBackendLifecycleObserver(f.operations, f.backend), firstBackend)
  assert.strictEqual(duplicate.installBackendLifecycleObserver(f.operations, f.backend), firstBackend)
  assert.strictEqual(f.operations.set, set)
  assert.throws(() => installBackendLifecycleObserver(f.operations, () => undefined), /unsupported_openclaw_exports/)
  const other = new WeakMap(), otherFirst = installBackendLifecycleObserver(other, f.backend)
  const operation = f.make(), handle = {}
  f.operations.set({}, operation)
  operation.attachBackend(handle)
  assert.strictEqual(firstBackend(operation), handle)
  assert.equal(otherFirst(operation), undefined)
  const copied = new WeakMap()
  Object.defineProperty(copied, 'set', { value: set, configurable: true, writable: true })
  assert.throws(() => installBackendLifecycleObserver(copied, f.backend), /unsupported_openclaw_exports/)
})

test('unknown map shape fails closed without evaluating accessor methods', () => {
  let reads = 0
  for (const key of ['get', 'set']) {
    const operations = new WeakMap()
    Object.defineProperty(operations, key, { get() { reads++; throw Error('unreachable') } })
    assert.throws(() => installBackendLifecycleObserver(operations, () => undefined), /unsupported_openclaw_exports/)
  }
  assert.equal(reads, 0)
  assert.throws(() => installBackendLifecycleObserver(Object.create(WeakMap.prototype), () => undefined), /unsupported_openclaw_exports/)
  const frozen = Object.freeze(new WeakMap())
  assert.throws(() => installBackendLifecycleObserver(frozen, () => undefined), TypeError)
  assert.strictEqual(frozen.set({}, {}), frozen, 'ordinary frozen-map storage is unchanged')
})

const sourcePath = process.env.CODEG_OPENCLAW_REGISTRY_AUDIT
test('published createReplyOperation enrolls before return and actual attach tracks first backend', { skip: !sourcePath }, () => {
  const source = readFileSync(sourcePath, 'utf8')
  assert.equal(createHash('sha256').update(source).digest('hex'), '3d53a6271de56728e8b70c7d7cc1989adc52d6fb2033a7a838c2633e09615f09')
  const start = source.indexOf('function createReplyOperation(params) {')
  const end = source.indexOf('\n\treturn operation;\n}', start) + '\n\treturn operation;\n}'.length
  assert.ok(start >= 0 && end > start)
  const fragment = source.slice(start, end)
  const operations = new WeakMap(), attached = new WeakMap(), backend = operation => attached.get(operation)
  const noop = () => {}
  const context = {
    WeakMap, WeakSet, Map, Set, Promise, AbortController, Date,
    normalizeOptionalString: value => typeof value === 'string' ? value.trim() || undefined : undefined,
    resolveReplyOperationAgentId: () => 'synthetic-agent',
    createDeferredCore: () => { let resolve; const promise = new Promise(yes => { resolve = yes }); return { promise, resolve } },
    getAgentEventLifecycleGeneration: () => 1,
    replyRunState: { followupAdmissionBarriersByKey: new Map(), activeRunsByKey: new Map(),
      successorAdmissionBarriersByKey: new Map(), activeSessionIdsByKey: new Map(), activeKeysBySessionId: new Map() },
    operationsByUpstreamAbortSignal: operations, attachedBackendByOperation: attached,
    getAttachedBackend: backend, clearReplyOperationByOperation: new WeakMap(),
    expireReplyOperationByOperation: new WeakMap(), evictReplyOperationByOperation: new WeakMap(),
    createReplyRunFinalizationLease: () => ({ clear: noop, recordActivity: noop }),
    createReplyRunSettleTimer: () => ({ clear: noop, scheduleOnce: noop }),
    registerWaitSessionId: noop, markReplyRunDiagnosticProgress: noop,
  }
  // Only the published pure factory runs. Lifecycle timers, diagnostics and
  // external infrastructure are explicit memory-only stubs; no module imports.
  const create = vm.runInNewContext(`(${fragment})`, context)
  const oldSignal = new AbortController().signal
  const old = create({ sessionKey: 'old-key', sessionId: 'old-session', upstreamAbortSignal: oldSignal })
  const original = old.attachBackend
  const firstBackend = installBackendLifecycleObserver(operations, backend)
  const signal = new AbortController().signal
  const operation = create({ sessionKey: 'new-key', sessionId: 'new-session', upstreamAbortSignal: signal })
  assert.strictEqual(operations.get(signal), operation)
  assert.notEqual(Function.prototype.toString.call(operation.attachBackend), Function.prototype.toString.call(original),
    'observer is installed inside factory before return')
  assert.equal(firstBackend(operation), undefined)
  const first = { toolAuthorityFingerprint: 'a', cancel: noop }, second = { toolAuthorityFingerprint: 'b', cancel: noop }
  assert.equal(operation.attachBackend(first), undefined)
  operation.detachBackend(first)
  operation.attachBackend(second)
  assert.strictEqual(firstBackend(operation), first)
  assert.strictEqual(backend(operation), second)
  old.attachBackend(first)
  assert.equal(firstBackend(old), undefined)
})
