// Run: node --test src-tauri/resources/dsh/
import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  AgentToolSearch, apply, bm25Search, buildIndex, describeHidden, selectCandidates,
  serverOf, shortNameOf, shouldGate, tokenize,
} from './codeg-tool-search.mjs'

const schema = (name, description = '', props = []) => ({
  name, description,
  parameters: { type: 'object', properties: Object.fromEntries(props.map(p => [p, { type: 'string' }])) },
})

const CATALOG = [
  schema('bash', 'Run a shell command', ['command']),
  schema('mcp__a1_myclaw__ask_user_question', 'Ask the user a question'),
  schema('mcp__app-notion__notion-search', 'Search pages and databases in the Notion workspace', ['query']),
  schema('mcp__app-notion__notion-list-recent-pages', 'List pages the user viewed recently', ['limit']),
  schema('mcp__app-higgsfield__generate_video', 'Generate a video from a text prompt', ['prompt', 'duration']),
  schema('mcp__app-higgsfield__generate_image', 'Generate an image from a text prompt', ['prompt']),
]

test('serverOf / shortNameOf parse the dsh public name', () => {
  assert.equal(serverOf('mcp__app-notion__notion-search'), 'app-notion')
  assert.equal(shortNameOf('mcp__app-notion__notion-search'), 'notion-search')
  assert.equal(serverOf('bash'), null)
  assert.equal(shortNameOf('bash'), 'bash')
})

test('selectCandidates keeps mcp tools, exempts myclaw under a session prefix, drops builtins', () => {
  const names = selectCandidates(CATALOG, { hidePrefixes: ['mcp__'], exemptServers: ['myclaw'] }).map(s => s.name)
  assert.deepEqual(names, [
    'mcp__app-notion__notion-search',
    'mcp__app-notion__notion-list-recent-pages',
    'mcp__app-higgsfield__generate_video',
    'mcp__app-higgsfield__generate_image',
  ])
})

test('tokenize splits snake/kebab/camel and makes CJK bigrams', () => {
  assert.deepEqual(tokenize('listRecent_pages-v2'), ['list', 'recent', 'pages', 'v2'])
  assert.deepEqual(tokenize('生成视频'), ['生成', '成视', '视频'])
  assert.deepEqual(tokenize(''), [])
})

test('bm25Search ranks the notion page lister first for a notion recent-pages query', () => {
  const index = buildIndex(selectCandidates(CATALOG, {}))
  const hits = bm25Search(index, 'notion recent pages', 3).map(s => s.name)
  assert.equal(hits[0], 'mcp__app-notion__notion-list-recent-pages')
  assert.ok(hits.includes('mcp__app-notion__notion-search'))
  assert.ok(!hits.includes('mcp__app-higgsfield__generate_video'))
})

test('bm25Search returns nothing for an unrelated query and honours the limit', () => {
  const index = buildIndex(selectCandidates(CATALOG, {}))
  assert.deepEqual(bm25Search(index, 'kubernetes', 5), [])
  assert.equal(bm25Search(index, 'generate prompt', 1).length, 1)
})

test('shouldGate follows mode and the auto threshold', () => {
  assert.equal(shouldGate('off', 100, 10), false)
  assert.equal(shouldGate('on', 1, 10), true)
  assert.equal(shouldGate('on', 0, 10), false)
  assert.equal(shouldGate('auto', 9, 10), false)
  assert.equal(shouldGate('auto', 10, 10), true)
})

test('describeHidden groups by server', () => {
  assert.equal(describeHidden(selectCandidates(CATALOG, {})), 'app-higgsfield (2 tools), app-notion (2 tools)')
})

// ─── fake dsh runtime ───────────────────────────────────────────────────────

function fakeAgent(id, log) {
  const restrictions = []
  const registered = []
  let uid = 1
  const agent = {
    id,
    ctx: {
      fiber: { get uid() { return uid } },
      effect: (factory) => { const dispose = factory(); return () => dispose() },
      tools: {
        restrict: (filter) => {
          const entry = { deny: [...filter.deny] }
          restrictions.push(entry); log.push(`restrict:${entry.deny.length}`)
          return () => { restrictions.splice(restrictions.indexOf(entry), 1); log.push('unrestrict') }
        },
        register: (def) => { registered.push(def); log.push(`register:${def.name}`); return () => registered.splice(registered.indexOf(def), 1) },
      },
    },
    restrictions, registered,
    kill() { uid = null },
  }
  return agent
}

function fakeRoot(catalog) {
  const listeners = new Map()
  const agents = []
  return {
    logger: { info() {}, warn() {}, error() {} },
    tools: { schemas: () => catalog },
    agents: { list: () => agents, _push: a => agents.push(a) },
    on: (event, fn) => { listeners.set(event, fn); return () => listeners.delete(event) },
    effect: (factory) => { const dispose = factory(); return () => dispose() },
    emit: (event, payload) => listeners.get(event)?.(payload),
  }
}

test('controller hides all candidates, search lifts hits with new-mask-before-old ordering', () => {
  const log = []
  const root = fakeRoot(CATALOG)
  const agent = fakeAgent('a1', log)
  const c = new AgentToolSearch(root, agent, { mode: 'on', autoMinTools: 10, hidePrefixes: ['mcp__'], exemptServers: ['myclaw'], searchLimit: 8, searchToolName: 'search_tools', debug: false }, op => op())
  c.install()
  assert.equal(agent.restrictions.length, 1)
  assert.equal(agent.restrictions[0].deny.length, 4)
  assert.equal(agent.registered[0].name, 'search_tools')
  assert.match(agent.registered[0].description, /app-notion \(2 tools\)/)

  const result = c.search('notion recent pages', 2)
  assert.equal(result.tools.length, 2)
  assert.equal(result.remainingHidden, 2)
  assert.equal(agent.restrictions.length, 1)
  assert.equal(agent.restrictions[0].deny.length, 2)
  // ordering: restrict(new) happens before unrestrict(old)
  const i = log.lastIndexOf('restrict:2')
  assert.ok(i >= 0 && log[i + 1] === 'unrestrict')
  // search tool re-registered with the smaller hidden set
  assert.match(agent.registered.at(-1).description, /app-higgsfield \(2 tools\)/)
  assert.ok(!/app-notion/.test(agent.registered.at(-1).description))

  c.dispose()
  assert.equal(agent.restrictions.length, 0)
  assert.equal(agent.registered.length, 0)
})

test('auto mode below threshold installs nothing', () => {
  const root = fakeRoot(CATALOG)
  const agent = fakeAgent('a2', [])
  const c = new AgentToolSearch(root, agent, { mode: 'auto', autoMinTools: 10, hidePrefixes: ['mcp__'], exemptServers: ['myclaw'], searchLimit: 8, searchToolName: 'search_tools' }, op => op())
  c.install()
  assert.equal(agent.restrictions.length, 0)
  assert.equal(agent.registered.length, 0)
})

test('apply installs on agent/created and cleans up on agent/disposed', () => {
  const root = fakeRoot(CATALOG)
  apply(root, { mode: 'on' })
  const agent = fakeAgent('a3', [])
  root.emit('agent/created', { agent, source: 'startup' })
  assert.equal(agent.restrictions.length, 1)
  root.emit('agent/disposed', { agent })
  assert.equal(agent.restrictions.length, 0)
})

test('apply with mode off or an incompatible runtime is a no-op', () => {
  const root = fakeRoot(CATALOG)
  apply(root, { mode: 'off' })
  const agent = fakeAgent('a4', [])
  root.emit('agent/created', { agent })
  assert.equal(agent.restrictions.length, 0)

  const broken = fakeRoot(CATALOG)
  delete broken.tools.schemas
  apply(broken, { mode: 'on' })
  const agent2 = fakeAgent('a5', [])
  broken.emit('agent/created', { agent: agent2 })
  assert.equal(agent2.restrictions.length, 0)
})

test('a restrict failure fails open instead of throwing', () => {
  const root = fakeRoot(CATALOG)
  const agent = fakeAgent('a6', [])
  agent.ctx.tools.restrict = () => { throw new Error('unknown global tool') }
  const c = new AgentToolSearch(root, agent, { mode: 'on', hidePrefixes: ['mcp__'], exemptServers: ['myclaw'], searchLimit: 8, searchToolName: 'search_tools' }, op => op())
  c.install()
  assert.equal(agent.restrictions.length, 0)
  assert.equal(agent.registered.length, 1)
})
