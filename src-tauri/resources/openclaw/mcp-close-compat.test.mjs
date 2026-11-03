// Pure audited method bodies and fake transports only. No SDK/Agent imports,
// model calls, runtime startup or database initialization.
import assert from 'node:assert/strict'
import { createHook } from 'node:async_hooks'
import { spawnSync } from 'node:child_process'
import { readFileSync, realpathSync, mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import vm from 'node:vm'
import { FILES, MCP_SDK_FILES, digest, installMcpCloseObserver, resolveMcpSdkFiles } from './compat.mjs'

const fixture = JSON.parse(readFileSync(new URL('./audited-mcp-cleanup-fixture.json', import.meta.url)))
const tick = () => new Promise(resolve => setImmediate(resolve))
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no }); return { promise, resolve, reject } }

function classes() {
  const context = vm.createContext({ Promise, Error, LATEST_PROTOCOL_VERSION: 'test', InitializeResultSchema: {}, SUPPORTED_PROTOCOL_VERSIONS: ['test'] })
  vm.runInContext(`
    class Protocol { ${fixture.fragments.protocolConnect}\n${fixture.fragments.protocolClose} }
    class Client extends Protocol {
      constructor(){super();this._capabilities={};this._clientInfo={name:'fake',version:'0'}}
      request(){return Promise.reject(this.initializeError)}
      ${fixture.fragments.clientConnect}
    }
    class StdioTransport {}
    globalThis.types={Protocol,Client,StdioTransport};
  `, context)
  return context.types
}
function transportFor(types, settle) {
  const transport = new types.StdioTransport()
  transport.calls = 0
  transport.start = async () => {}
  transport.close = () => { transport.calls++; return settle.promise }
  return transport
}

test('MCP cleanup wrapper returns the original Promise, rejection and cleanup uncertainty', async () => {
  const types = classes(), settle = deferred(), transport = transportFor(types, settle)
  let reports = 0, uncertain = false
  settle.promise.catch(() => { uncertain = true })
  installMcpCloseObserver(types, () => { reports++ })
  const client = new types.Client(); client._transport = transport
  const promises = [], hook = createHook({ init(_id, type, _trigger, resource) { if (type === 'PROMISE') promises.push(resource) } })
  let result
  hook.enable()
  try { result = client.close() } finally { hook.disable() }
  assert.strictEqual(result, promises[0], 'the first Promise belongs to the original async Protocol.close')
  const failure = new Error('synthetic identity loss')
  settle.reject(failure)
  await assert.rejects(result, error => error === failure)
  await tick()
  assert.equal(transport.calls, 1)
  assert.equal(reports, 1)
  assert.equal(uncertain, true)
  assert.equal(Object.getPrototypeOf(types.Client.prototype.close).constructor.name, 'Function', 'wrapper is not async')
})

test('MCP close success, unrelated transports and Protocol users retain original behavior', async () => {
  const types = classes(), ownClose = types.Protocol.prototype.close
  let reports = 0
  installMcpCloseObserver(types, () => { reports++ })
  const done = deferred(), owned = transportFor(types, done), client = new types.Client()
  client._transport = owned; const success = client.close(); done.resolve(); await success
  const failure = new Error('unrelated close failure'), unrelated = { close() { return Promise.reject(failure) } }
  client._transport = unrelated
  await assert.rejects(client.close(), error => error === failure)
  const other = new types.Protocol(); other._transport = transportFor(types, { promise: Promise.reject(failure) })
  await assert.rejects(other.close(), error => error === failure)
  await tick()
  assert.strictEqual(types.Protocol.prototype.close, ownClose)
  assert.equal(reports, 0)
})

test('MCP close observer is idempotent and refuses foreign prototype changes', async () => {
  const types = classes()
  installMcpCloseObserver(types)
  const close = types.Client.prototype.close
  installMcpCloseObserver(types)
  assert.strictEqual(types.Client.prototype.close, close)
  const duplicate = await import('./compat.mjs?duplicate-owner')
  duplicate.installMcpCloseObserver(types)
  assert.strictEqual(types.Client.prototype.close, close, 'another owned module instance must not wrap twice')
  const unknown = classes(), foreign = function close() {}
  Object.defineProperty(unknown.Client.prototype, 'close', { value: foreign, configurable: true })
  assert.throws(() => installMcpCloseObserver(unknown), /unsupported_openclaw_exports/)
  assert.strictEqual(unknown.Client.prototype.close, foreign)
  const changed = classes()
  changed.Protocol.prototype.close = async function close() {}
  assert.throws(() => installMcpCloseObserver(changed), /unsupported_openclaw_exports/)
  assert.equal(Object.hasOwn(changed.Client.prototype, 'close'), false)
})

test('MCP observer failures, transport clearing and accessors do not alter close', async () => {
  for (const report of [() => { throw new Error('sink failure') }, async () => { throw new Error('async sink failure') }]) {
    const types = classes(), settle = deferred(), client = new types.Client(), transport = transportFor(types, settle)
    installMcpCloseObserver(types, report)
    client._transport = transport
    transport.close = () => { client._transport = undefined; return settle.promise }
    const failure = new Error('identity loss'), closing = client.close(); settle.reject(failure)
    await assert.rejects(closing, error => error === failure)
    await tick()
  }
  const types = classes(), client = new types.Client()
  let reads = 0, reports = 0
  installMcpCloseObserver(types, () => { reports++ })
  Object.defineProperty(client, '_transport', { get() { reads++; throw new Error('getter failure') } })
  await assert.rejects(client.close(), /getter failure/)
  assert.equal(reads, 1, 'only the original SDK close evaluates an accessor')
  assert.equal(reports, 0)
  await assert.rejects(types.Client.prototype.close.call(null), error => error?.name === 'TypeError')
})

test('actual SDK initialize catch reproduces the escaped close rejection and the targeted wrapper isolates it', () => {
  // A standalone local Node process observes the intentional unhandled control
  // case; the node:test runner itself never receives an unhandled rejection.
  const script = `
    import vm from 'node:vm';import assert from 'node:assert/strict';
    import {installMcpCloseObserver} from ${JSON.stringify(new URL('./compat.mjs', import.meta.url).href)};
    const fragments=${JSON.stringify(fixture.fragments)};
    const results=[];
    for(const wrapped of [false,true]){
      const unhandled=[];const handler=reason=>unhandled.push(reason);process.on('unhandledRejection',handler);
      try{
        const c=vm.createContext({Promise,Error,LATEST_PROTOCOL_VERSION:'test',InitializeResultSchema:{},SUPPORTED_PROTOCOL_VERSIONS:['test']});
        vm.runInContext('class Protocol {'+fragments.protocolConnect+fragments.protocolClose+'};'+
          'class Client extends Protocol {constructor(){super();this._capabilities={};this._clientInfo={}}request(){return Promise.reject(this.initializeError)}'+fragments.clientConnect+'};'+
          'class StdioTransport {};globalThis.types={Protocol,Client,StdioTransport}',c);
        let reject,uncertain=false,reports=0;const cleanup=new Promise((_resolve,no)=>reject=no);cleanup.catch(()=>{uncertain=true});
        const client=new c.types.Client(),transport=new c.types.StdioTransport(),initError=new Error('initialize timeout'),identityError=new Error('identity loss');
        transport.start=async()=>{};transport.close=()=>cleanup;client.initializeError=initError;
        if(wrapped)installMcpCloseObserver(c.types,()=>{reports++});
        await assert.rejects(client.connect(transport),error=>error===initError);
        reject(identityError);await new Promise(done=>setImmediate(done));await new Promise(done=>setImmediate(done));
        assert.equal(unhandled.length,wrapped?0:1);assert.equal(uncertain,true);
        results.push({wrapped,unhandled:unhandled.length,reports,uncertain});
      }finally{process.off('unhandledRejection',handler)}
    }
    console.log(JSON.stringify(results));
  `
  const run = spawnSync(process.execPath, ['--input-type=module', '-'], { input: script, encoding: 'utf8', env: {}, timeout: 10_000 })
  assert.equal(run.status, 0, run.stderr)
  assert.deepEqual(JSON.parse(run.stdout), [
    { wrapped: false, unhandled: 1, reports: 0, uncertain: true },
    { wrapped: true, unhandled: 0, reports: 1, uncertain: true },
  ])
})

const auditRoot = process.env.CODEG_OPENCLAW_AUDIT_PACKAGE
test('published MCP resolution selects pinned ESM targets and audits saved method bytes without importing SDK', { skip: !auditRoot }, () => {
  const files = resolveMcpSdkFiles(auditRoot)
  for (const [kind, [relative, hash]] of Object.entries(MCP_SDK_FILES)) {
    assert.ok(files[kind].path.endsWith(join('dist', 'esm', relative)))
    assert.equal(digest(files[kind].source), hash)
    assert.equal(fixture.files[kind], hash)
  }
  for (const [kind, names] of [['client', ['clientConnect']], ['protocol', ['protocolConnect', 'protocolClose']]]) {
    for (const name of names) assert.ok(files[kind].source.includes(fixture.fragments[name]))
  }
})

test('unknown SDK version, exports, files and runtime fail before any prototype patch or SDK import', { skip: !auditRoot }, () => {
  const originals = resolveMcpSdkFiles(auditRoot), temp = realpathSync(mkdtempSync(join(tmpdir(), 'codeg-mcp-guard-')))
  try {
    const root = join(temp, 'openclaw'), sdk = join(root, 'node_modules', '@modelcontextprotocol', 'sdk')
    for (const directory of [join(root, 'dist'), join(sdk, 'dist', 'cjs', 'client'), join(sdk, 'dist', 'esm', 'client'), join(sdk, 'dist', 'esm', 'shared')]) mkdirSync(directory, { recursive: true })
    const publishedExports = JSON.parse(readFileSync(new URL('../../../package.json', originals.client.url))).exports
    const packageJson = { name: '@modelcontextprotocol/sdk', version: '1.30.0', type: 'module', exports: publishedExports }
    const runtimePath = join(root, 'package.json'), sdkPath = join(sdk, 'package.json'), clientPath = join(sdk, 'dist', 'esm', 'client', 'index.js')
    writeFileSync(runtimePath, JSON.stringify({ name: 'openclaw', version: '2026.9.4' }))
    writeFileSync(join(root, 'dist', FILES.mcpLifecycle[0]), originals.lifecycle.source)
    writeFileSync(sdkPath, JSON.stringify(packageJson))
    writeFileSync(join(sdk, 'dist', 'cjs', 'client', 'index.js'), 'throw new Error("CJS must never execute")')
    for (const kind of ['client', 'protocol']) writeFileSync(join(sdk, 'dist', 'esm', MCP_SDK_FILES[kind][0]), originals[kind].source)
    assert.equal(resolveMcpSdkFiles(root).client.path, clientPath)
    writeFileSync(sdkPath, JSON.stringify({ ...packageJson, version: '1.30.1' }))
    assert.throws(() => resolveMcpSdkFiles(root), /unsupported_openclaw_version/)
    writeFileSync(sdkPath, JSON.stringify({ ...packageJson, exports: { './*': { import: './dist/cjs/*', require: './dist/cjs/*' } } }))
    assert.throws(() => resolveMcpSdkFiles(root), /unsupported_openclaw_exports/)
    for (const exports of [
      { ...publishedExports, './*': { node: './dist/cjs/*', ...publishedExports['./*'] } },
      { ...publishedExports, './client/index.js': { import: './dist/cjs/client/index.js', require: './dist/cjs/client/index.js' } },
      { ...publishedExports, './shared/protocol.js': { import: './dist/cjs/shared/protocol.js', require: './dist/cjs/shared/protocol.js' } },
    ]) {
      writeFileSync(sdkPath, JSON.stringify({ ...packageJson, exports }))
      assert.throws(() => resolveMcpSdkFiles(root), /unsupported_openclaw_exports/)
    }
    writeFileSync(sdkPath, JSON.stringify(packageJson)); writeFileSync(clientPath, originals.client.source + '\n')
    assert.throws(() => resolveMcpSdkFiles(root), /unsupported_openclaw_bundle/)
    writeFileSync(clientPath, originals.client.source)
    writeFileSync(runtimePath, JSON.stringify({ name: 'openclaw', version: 'unknown' }))
    assert.throws(() => resolveMcpSdkFiles(root), /unsupported_openclaw_version/)
    writeFileSync(runtimePath, JSON.stringify({ name: 'openclaw', version: '2026.9.4' }))
    writeFileSync(join(root, 'dist', FILES.mcpLifecycle[0]), originals.lifecycle.source + '\n')
    assert.throws(() => resolveMcpSdkFiles(root), /unsupported_openclaw_bundle/)
  } finally { rmSync(temp, { recursive: true, force: true }) }
})
