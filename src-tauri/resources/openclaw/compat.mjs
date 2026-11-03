// Private OpenClaw seams are accepted only for this audited published bundle.
import { createHash } from 'node:crypto'
import { readFileSync, realpathSync } from 'node:fs'
import { createRequire } from 'node:module'
import { dirname, join, parse } from 'node:path'
import { pathToFileURL } from 'node:url'
import { installBackendLifecycleObserver } from './backend-lifecycle.mjs'

export const VERSION = '2026.9.4'
export const FILES = {
  server: ['server-DWybsgYT.mjs', '96113ca44674714f6157b040ecd956f94c226bd983f62ff342ad9ef78963d020'],
  registry: ['reply-run-registry.registry-DDgx8cNZ.mjs', '3d53a6271de56728e8b70c7d7cc1989adc52d6fb2033a7a838c2633e09615f09'],
  embedded: ['builtin-openclaw-B-H-7lKk.mjs', '0a8c813e535c92d03f69bc58381518ba0e6ac6e46f3adda54138c5f668340ea8'],
  mcpLifecycle: ['mcp-client-lifecycle-CbhQDS1-.mjs', 'a72d5a4078d85c5eb5b56d1cb03bdc44257fa53201f75c7f1bc7a0f2a6f56cc6'],
}
export const MCP_SDK_VERSION = '1.30.0'
export const MCP_SDK_FILES = {
  client: ['client/index.js', 'db6cc643f5cc095b2fd0b70bf691d1775f07a317e97555e3928559e815f5e32a'],
  protocol: ['shared/protocol.js', '8de524602541771e084f7185e7d0fc418b8142a9cae1bd80a3ee7c70e9a9e132'],
}
const CLOSE_SOURCE = 'async close() {\n        await this._transport?.close();\n    }'
const CLOSE_OBSERVER = Symbol.for('codeg.openclaw.mcpCloseObserver.v1')
// The complete published export map, not just the wildcard's two targets.
const MCP_EXPORTS_SHA = 'f036c4d9d8d2b04d1b5b254716930a97b75ab00fa46c3f017f04ab3784fae630'
export const digest = source => createHash('sha256').update(source).digest('hex')
const canonical = value => value && typeof value === 'object'
  ? Array.isArray(value) ? value.map(canonical) : Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]))
  : value

export function packageRoot(entry = process.argv[1]) {
  let dir = dirname(realpathSync(entry))
  while (true) {
    try {
      const pkg = JSON.parse(readFileSync(join(dir, 'package.json'), 'utf8'))
      if (pkg.name === 'openclaw') return pkg.version === VERSION ? dir : null
    } catch {}
    if (dir === parse(dir).root) return null
    dir = dirname(dir)
  }
}

export function checkedFile(root, kind) {
  const [name, hash] = FILES[kind]
  const path = join(root, 'dist', name)
  const source = readFileSync(path, 'utf8')
  if (digest(source) !== hash) throw new Error('unsupported_openclaw_bundle')
  return { path, source, url: pathToFileURL(path).href }
}

/** CJS resolution locates the package only. The actual imports below use its
 * verified ESM export targets, matching the audited lifecycle's bare imports. */
export function resolveMcpSdkFiles(root) {
  const runtime = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'))
  if (runtime.name !== 'openclaw' || runtime.version !== VERSION) throw new Error('unsupported_openclaw_version')
  const lifecycle = checkedFile(root, 'mcpLifecycle')
  const cjsClient = realpathSync(createRequire(lifecycle.path).resolve('@modelcontextprotocol/sdk/client/index.js'))
  const sdkRoot = dirname(dirname(dirname(dirname(cjsClient))))
  const sdk = JSON.parse(readFileSync(join(sdkRoot, 'package.json'), 'utf8'))
  if (sdk.name !== '@modelcontextprotocol/sdk' || sdk.version !== MCP_SDK_VERSION) throw new Error('unsupported_openclaw_version')
  if (sdk.type !== 'module' || !sdk.exports || typeof sdk.exports !== 'object' || Array.isArray(sdk.exports)
    || digest(JSON.stringify(canonical(sdk.exports))) !== MCP_EXPORTS_SHA
    || cjsClient !== join(sdkRoot, 'dist', 'cjs', 'client', 'index.js')) throw new Error('unsupported_openclaw_exports')
  const files = { lifecycle }
  for (const [kind, [relative, hash]] of Object.entries(MCP_SDK_FILES)) {
    const path = join(sdkRoot, 'dist', 'esm', relative)
    if (realpathSync(path) !== path) throw new Error('unsupported_openclaw_exports')
    const source = readFileSync(path, 'utf8')
    if (digest(source) !== hash) throw new Error('unsupported_openclaw_bundle')
    files[kind] = { path, source, url: pathToFileURL(path).href }
  }
  return files
}

/** Observe the SDK's fire-and-forget close without converting failed cleanup
 * into success. In particular, every explicit await still rejects unchanged. */
export function installMcpCloseObserver({ Client, Protocol, StdioTransport }, report = () => {}) {
  if (typeof Client !== 'function' || typeof Protocol !== 'function' || typeof StdioTransport !== 'function'
    || Object.getPrototypeOf(Client.prototype) !== Protocol.prototype) throw new Error('unsupported_openclaw_exports')
  const original = Object.getOwnPropertyDescriptor(Protocol.prototype, 'close')?.value
  if (typeof original !== 'function' || Function.prototype.toString.call(original) !== CLOSE_SOURCE) throw new Error('unsupported_openclaw_exports')
  const prior = Object.getOwnPropertyDescriptor(Client.prototype, 'close')
  if (prior) {
    const installed = typeof prior.value === 'function' && Object.getOwnPropertyDescriptor(prior.value, CLOSE_OBSERVER)?.value
    if (installed?.Client === Client && installed.Protocol === Protocol && installed.StdioTransport === StdioTransport
      && installed.original === original && installed.wrapper === prior.value) return
    throw new Error('unsupported_openclaw_exports')
  }
  function close(...args) {
    // Protocol.connect stores this as an own data property. Observing it must
    // not add getter evaluations on an unfamiliar receiver.
    let owned = false
    try {
      const descriptor = Object.getOwnPropertyDescriptor(this, '_transport')
      owned = Boolean(descriptor && Object.hasOwn(descriptor, 'value') && descriptor.value instanceof StdioTransport)
    } catch {}
    const result = Reflect.apply(original, this, args)
    if (owned) Promise.prototype.then.call(result, undefined, () => {
      // Only fixed diagnostics leave this observer, never the cleanup error.
      try { Promise.resolve(report()).catch(() => {}) } catch {}
    })
    return result
  }
  Object.defineProperty(close, CLOSE_OBSERVER, { value: Object.freeze({ Client, Protocol, StdioTransport, original, wrapper: close }) })
  Object.defineProperty(Client.prototype, 'close', { value: close, writable: true, configurable: true })
}

export async function loadGatewayInternals(root = packageRoot(), reportCleanupFailure) {
  if (!root) throw new Error('unsupported_openclaw_version')
  const registry = checkedFile(root, 'registry')
  checkedFile(root, 'server')
  checkedFile(root, 'embedded')
  // Complete every version/hash guard before importing or patching anything.
  const sdk = resolveMcpSdkFiles(root)
  const [module, client, protocol, lifecycle] = await Promise.all([
    import(registry.url), import(sdk.client.url), import(sdk.protocol.url), import(sdk.lifecycle.url),
  ])
  if (!module.h?.get || !module.V?.get || typeof module.A !== 'function') {
    throw new Error('unsupported_openclaw_exports')
  }
  installMcpCloseObserver({ Client: client.Client, Protocol: protocol.Protocol, StdioTransport: lifecycle.a }, reportCleanupFailure)
  const firstBackend = installBackendLifecycleObserver(module.V, module.A)
  return { registry: module.h, operations: module.V, backend: module.A, firstBackend }
}
