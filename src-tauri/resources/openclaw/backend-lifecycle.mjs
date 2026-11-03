// Observe only the audited registry's own WeakMap. No runtime/module imports.
const MARKER = Symbol.for('codeg.openclaw.backendLifecycle.v1')
const weakGet = WeakMap.prototype.get
const weakSet = WeakMap.prototype.set
const object = value => (typeof value === 'object' && value !== null) || typeof value === 'function'
const data = (owner, key) => {
  const descriptor = Object.getOwnPropertyDescriptor(owner, key)
  return descriptor && Object.hasOwn(descriptor, 'value') ? descriptor.value : undefined
}
const unsupported = () => { throw new Error('unsupported_openclaw_exports') }

/** Installed after all bundle guards, before capability succeeds. In the
 * audited registry, set(signal, operation) occurs once during construction,
 * before createReplyOperation returns and any backend can attach. Existing
 * operations are deliberately not enumerated or retroactively certified. */
export function installBackendLifecycleObserver(operations, backend) {
  if (!object(operations) || Object.getPrototypeOf(operations) !== WeakMap.prototype
    || typeof backend !== 'function' || WeakMap.prototype.get !== weakGet || WeakMap.prototype.set !== weakSet
    || Object.getOwnPropertyDescriptor(operations, 'get')) unsupported()
  try { Reflect.apply(weakGet, operations, [operations]) } catch { unsupported() }
  const prior = Object.getOwnPropertyDescriptor(operations, 'set')
  if (prior) {
    const installed = typeof prior.value === 'function' && data(prior.value, MARKER)
    if (installed && prior.writable === true && prior.configurable === true && prior.enumerable === false
      && data(installed, 'operations') === operations && data(installed, 'backend') === backend
      && data(installed, 'original') === weakSet && data(installed, 'wrapper') === prior.value
      && typeof data(installed, 'firstBackend') === 'function') return data(installed, 'firstBackend')
    unsupported()
  }

  const first = new WeakMap(), enrolled = new WeakSet(), uncertain = new WeakSet()
  const firstBackend = operation => object(operation) && !uncertain.has(operation)
    ? Reflect.apply(weakGet, first, [operation]) : undefined
  const observe = (operation, previous) => {
    if (!object(operation) || enrolled.has(operation)) return
    enrolled.add(operation)
    // A repeated registration cannot turn an old operation into a new proof.
    if (previous === operation || backend(operation) !== undefined) {
      uncertain.add(operation)
      return
    }
    const descriptor = Object.getOwnPropertyDescriptor(operation, 'attachBackend')
    if (!descriptor || !Object.hasOwn(descriptor, 'value') || typeof descriptor.value !== 'function'
      || descriptor.writable !== true || descriptor.configurable !== true || descriptor.enumerable !== true) {
      uncertain.add(operation)
      return
    }
    const original = descriptor.value
    let attaching = 0
    function attachBackend(...args) {
      attaching++
      if (attaching > 1 && !Reflect.apply(weakGet, first, [operation])) uncertain.add(operation)
      try {
        let result
        try { result = Reflect.apply(original, this, args) } catch (error) {
          // An attachment may precede a throw. A later success is not first.
          if (!Reflect.apply(weakGet, first, [operation])) uncertain.add(operation)
          throw error
        }
        if (!uncertain.has(operation) && !Reflect.apply(weakGet, first, [operation])) {
          try {
            const handle = args[0], current = backend(operation)
            if (!object(handle) || current !== handle || uncertain.has(operation)) uncertain.add(operation)
            else Reflect.apply(weakSet, first, [operation, handle])
          } catch { uncertain.add(operation) }
        }
        return result
      } finally { attaching-- }
    }
    Object.defineProperty(operation, 'attachBackend', { ...descriptor, value: attachBackend })
  }
  function set(key, operation) {
    // Preserve the native receiver, arguments, return value and exception.
    const previous = this === operations ? Reflect.apply(weakGet, operations, [key]) : undefined
    const result = Reflect.apply(weakSet, this, arguments)
    if (this === operations) {
      try { observe(operation, previous) } catch {
        if (object(operation)) uncertain.add(operation)
      }
    }
    return result
  }
  Object.defineProperty(set, MARKER, { value: Object.freeze({ operations, backend, original: weakSet, wrapper: set, firstBackend }) })
  Object.defineProperty(operations, 'set', { value: set, configurable: true, writable: true })
  return firstBackend
}
