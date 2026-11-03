// Explicit local source audit only. Imports the pure ACP SDK, never OpenClaw's
// server (which creates a SQLite replay ledger) or any runtime/provider.
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { pathToFileURL } from 'node:url'
import { test } from 'node:test'
import vm from 'node:vm'
import { checkedFile, digest, packageRoot } from './compat.mjs'
import { patchAcpSource } from './acp-bootstrap.mjs'

const root = process.env.CODEG_OPENCLAW_AUDIT_PACKAGE
assert.ok(root, 'Set CODEG_OPENCLAW_AUDIT_PACKAGE to the already-published 2026.9.4 package; do not install or start it')
assert.equal(packageRoot(join(root, 'openclaw.mjs')), root)

test('OpenClaw published startup planner selects the native steering RPC plugin', () => {
  const source = readFileSync(join(root, 'dist/installed-plugin-index-scope-lookup-hT11eO5V.mjs'), 'utf8')
  const fragment = source.match(/function shouldConsiderForGatewayStartup\(params\) \{[\s\S]*?\n\}/)?.[0]
  assert.ok(fragment, 'the audited startup planner must still exist')
  const consider = vm.runInNewContext(`(${fragment})`, { isGatewayStartupMemoryPlugin: () => false })
  const manifest = JSON.parse(readFileSync(new URL('./openclaw.plugin.json', import.meta.url)))
  assert.equal(consider({ manifest, plugin: { pluginId: manifest.id } }), true)
})

test('OpenClaw published bundle anchors and copied native consumer match audited source exactly', () => {
  const server = checkedFile(root, 'server')
  checkedFile(root, 'registry')
  const embedded = checkedFile(root, 'embedded')
  const fixture = JSON.parse(readFileSync(new URL('./audited-consumer-fixture.json', import.meta.url)))
  assert.equal(digest(embedded.source), fixture.bundleSha256)
  for (const fragment of Object.values(fixture.fragments)) assert.ok(embedded.source.includes(fragment))
  const patched = patchAcpSource(server.source, new URL('./acp-bridge.mjs', import.meta.url).href)
  assert.ok(patched.includes('__codegInstallNativeSteering(agent, conn, gateway);\n\t\tagent.start();'))
  assert.equal(patchAcpSource(server.source + '\n', 'file:///unused.mjs'), null)
})

test('OpenClaw published ACP SDK 1.4 router preserves underscore extension wire names', async () => {
  const sdkDir = join(dirname(root), '@agentclientprotocol', 'sdk')
  assert.equal(JSON.parse(readFileSync(join(sdkDir, 'package.json'))).version, '1.4.0')
  const sdk = await import(pathToFileURL(join(sdkDir, 'dist/acp.js')).href)
  const requests = [], notifications = [], agentCalls = [], clientCalls = []
  const c2s = new TransformStream({ transform(frame, controller) { requests.push(frame); controller.enqueue(frame) } })
  const s2c = new TransformStream({ transform(frame, controller) { notifications.push(frame); controller.enqueue(frame) } })
  let agentConnection
  const server = new sdk.AgentSideConnection(connection => {
    agentConnection = connection
    return { extMethod(method, params) { agentCalls.push(method); return { echo: params } } }
  }, { readable: c2s.readable, writable: s2c.writable })
  const client = new sdk.ClientSideConnection(() => ({ extNotification(method) { clientCalls.push(method) } }), { readable: s2c.readable, writable: c2s.writable })
  assert.deepEqual(await client.extMethod('_ai.myclaw/steer', { version: 1 }), { echo: { version: 1 } })
  await agentConnection.extNotification('_ai.myclaw/nativeSteeringState', { version: 1 })
  await new Promise(resolve => setImmediate(resolve))
  assert.deepEqual(agentCalls, ['_ai.myclaw/steer'])
  assert.deepEqual(clientCalls, ['_ai.myclaw/nativeSteeringState'])
  assert.equal(requests[0].method, '_ai.myclaw/steer')
  assert.equal(notifications.find(frame => !('id' in frame)).method, '_ai.myclaw/nativeSteeringState')
  client.connection.close()
  server.connection.close()
})
