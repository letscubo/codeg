/**
 * codeg-tool-search — tool-level progressive disclosure for DeepSeek Harness (dsh).
 *
 * Shipped inside codeg (`include_str!`) and materialized into `$DSH_HOME/plugins/`
 * at launch; referenced from codeg's per-connection `--patch` overlay by absolute
 * path. Zero imports on purpose: the profile has no package manager and a second
 * copy of any `@deepseek-ai/*` package in the profile breaks the loader.
 *
 * Mechanism: at `agent/created` every MCP tool (`mcp__<server>__<tool>`) except the
 * exempt servers (myclaw) is hidden from that agent with `agent.ctx.tools.restrict`
 * ({deny}) and a `search_tools` tool is registered in the agent scope. `search_tools`
 * runs BM25 over the hidden catalog and lifts the mask for the hits; from the next
 * request on the model sees their full schemas and calls them directly. Execution,
 * approval and presentation stay native — this file never proxies a call.
 *
 * Pinned to dsh 0.1.6-alpha.x: `agent/created` (serial dispatch), `tools/change`,
 * `ctx.tools.schemas(scope)`, `agent.ctx.tools.restrict/register`. Any missing piece
 * fails open (no mask, full native surface).
 */

export const name = 'codeg-tool-search'
export const inject = ['tools', 'agents']

export const DEFAULT_CONFIG = Object.freeze({
  /** off = never mask; on = always mask; auto = mask when candidates >= autoMinTools */
  mode: 'auto',
  autoMinTools: 10,
  hidePrefixes: ['mcp__'],
  exemptServers: ['myclaw'],
  searchLimit: 8,
  searchToolName: 'search_tools',
  debug: false,
})

// ─── pure helpers (unit-tested) ────────────────────────────────────────────

/** `mcp__<server>__<tool>` → `<server>`, else null. */
export function serverOf(toolName) {
  if (typeof toolName !== 'string' || !toolName.startsWith('mcp__')) return null
  const rest = toolName.slice('mcp__'.length)
  const idx = rest.indexOf('__')
  return idx > 0 ? rest.slice(0, idx) : null
}

/** Bare tool name without the `mcp__<server>__` prefix. */
export function shortNameOf(toolName) {
  const server = serverOf(toolName)
  return server === null ? toolName : toolName.slice('mcp__'.length + server.length + 2)
}

function isExemptServer(server, exemptServers) {
  if (server === null) return false
  return exemptServers.some(s => server === s || server.endsWith(`_${s}`) || server.endsWith(`-${s}`))
}

/** Which schemas are eligible for masking. */
export function selectCandidates(schemas, config) {
  const prefixes = config.hidePrefixes ?? DEFAULT_CONFIG.hidePrefixes
  const exempt = config.exemptServers ?? DEFAULT_CONFIG.exemptServers
  return schemas.filter(s => typeof s?.name === 'string'
    && prefixes.some(p => s.name.startsWith(p))
    && !isExemptServer(serverOf(s.name), exempt))
}

const CJK = /[぀-ヿ㐀-䶿一-鿿가-힯]/

/** Lowercased word tokens; camelCase split; CJK runs become character bigrams. */
export function tokenize(text) {
  if (typeof text !== 'string' || text === '') return []
  const out = []
  const spaced = text.replace(/([a-z0-9])([A-Z])/g, '$1 $2')
  for (const raw of spaced.split(/[^\p{L}\p{N}]+/u)) {
    if (raw === '') continue
    const word = raw.toLowerCase()
    if (CJK.test(word)) {
      const chars = [...word]
      if (chars.length === 1) out.push(chars[0])
      for (let i = 0; i + 1 < chars.length; i += 1) out.push(chars[i] + chars[i + 1])
      continue
    }
    out.push(word)
  }
  return out
}

function paramNames(parameters) {
  const props = parameters && typeof parameters === 'object' ? parameters.properties : undefined
  return props && typeof props === 'object' ? Object.keys(props) : []
}

/** Build a BM25 index over tool schemas. Name tokens are weighted by repetition. */
export function buildIndex(schemas) {
  const docs = schemas.map(schema => {
    const short = shortNameOf(schema.name)
    const server = serverOf(schema.name) ?? ''
    const tokens = [
      ...tokenize(short), ...tokenize(short), ...tokenize(short),
      ...tokenize(server),
      ...tokenize(schema.description ?? ''),
      ...paramNames(schema.parameters).flatMap(tokenize),
    ]
    const tf = new Map()
    for (const t of tokens) tf.set(t, (tf.get(t) ?? 0) + 1)
    return { schema, tf, length: tokens.length }
  })
  const df = new Map()
  for (const doc of docs) for (const term of doc.tf.keys()) df.set(term, (df.get(term) ?? 0) + 1)
  const avgLength = docs.length === 0 ? 0 : docs.reduce((sum, d) => sum + d.length, 0) / docs.length
  return { docs, df, avgLength }
}

/** BM25 ranking; returns schemas with score > 0, best first. Ties keep catalog order. */
export function bm25Search(index, query, limit) {
  const terms = tokenize(query)
  if (terms.length === 0 || index.docs.length === 0) return []
  const k1 = 1.2
  const b = 0.75
  const n = index.docs.length
  const scored = index.docs.map((doc, position) => {
    let score = 0
    for (const term of new Set(terms)) {
      const f = doc.tf.get(term)
      if (f === undefined) continue
      const dfv = index.df.get(term) ?? 0
      const idf = Math.log(1 + (n - dfv + 0.5) / (dfv + 0.5))
      const norm = f * (k1 + 1) / (f + k1 * (1 - b + b * (doc.length / (index.avgLength || 1))))
      score += idf * norm
    }
    return { schema: doc.schema, score, position }
  })
  return scored
    .filter(item => item.score > 0)
    .sort((a, c) => c.score - a.score || a.position - c.position)
    .slice(0, limit)
    .map(item => item.schema)
}

/** Whether masking applies for this candidate count under the configured mode. */
export function shouldGate(mode, candidateCount, autoMinTools) {
  if (mode === 'off') return false
  if (mode === 'on') return candidateCount > 0
  return candidateCount >= (autoMinTools ?? DEFAULT_CONFIG.autoMinTools)
}

/** Group hidden tools by server for the search tool's description. */
export function describeHidden(hiddenSchemas) {
  const byServer = new Map()
  for (const s of hiddenSchemas) {
    const server = serverOf(s.name) ?? '(other)'
    byServer.set(server, (byServer.get(server) ?? 0) + 1)
  }
  return [...byServer.entries()]
    .sort((a, c) => a[0].localeCompare(c[0]))
    .map(([server, count]) => `${server} (${count} tools)`)
    .join(', ')
}

export function searchToolDescription(hiddenSchemas) {
  const summary = describeHidden(hiddenSchemas)
  return [
    'Find application tools before using them. The application tools listed below are not loaded yet;',
    'call this with a few keywords describing what you need (in any language), then call the returned',
    'tools directly by name. Tools stay loaded for the rest of the session.',
    summary === '' ? 'Hidden application tools: none.' : `Hidden application tools: ${summary}.`,
  ].join(' ')
}

// ─── runtime ───────────────────────────────────────────────────────────────

function hasFn(obj, key) {
  return obj !== null && typeof obj === 'object' && typeof obj[key] === 'function'
}

function rootCompatible(ctx) {
  return hasFn(ctx.tools, 'schemas') && hasFn(ctx.agents, 'list') && hasFn(ctx, 'on') && hasFn(ctx, 'effect')
}

function agentCompatible(agent) {
  const actx = agent?.ctx
  return actx !== undefined && actx !== null
    && hasFn(actx.tools, 'restrict') && hasFn(actx.tools, 'register') && hasFn(actx, 'effect')
    && actx.fiber !== undefined && actx.fiber !== null && 'uid' in actx.fiber
}

function resolveConfig(raw) {
  const cfg = { ...DEFAULT_CONFIG, ...(raw && typeof raw === 'object' ? raw : {}) }
  if (!['auto', 'on', 'off'].includes(cfg.mode)) cfg.mode = 'auto'
  if (!Array.isArray(cfg.hidePrefixes) || cfg.hidePrefixes.length === 0) cfg.hidePrefixes = [...DEFAULT_CONFIG.hidePrefixes]
  if (!Array.isArray(cfg.exemptServers)) cfg.exemptServers = [...DEFAULT_CONFIG.exemptServers]
  cfg.autoMinTools = Number.isFinite(cfg.autoMinTools) && cfg.autoMinTools > 0 ? cfg.autoMinTools : DEFAULT_CONFIG.autoMinTools
  cfg.searchLimit = Number.isFinite(cfg.searchLimit) && cfg.searchLimit > 0 ? cfg.searchLimit : DEFAULT_CONFIG.searchLimit
  return cfg
}

/** One agent's mask + search tool. Never proxies execution. */
export class AgentToolSearch {
  constructor(rootCtx, agent, config, internalMutation) {
    this.rootCtx = rootCtx
    this.agent = agent
    this.config = config
    this.internalMutation = internalMutation
    this.loaded = new Set()
    this.candidates = []
    this.index = null
    this.restrictionDispose = undefined
    this.searchDispose = undefined
    this.lifecycleDispose = undefined
    this.disposed = false
    this.gated = false
  }

  get active() {
    return !this.disposed && this.agent.ctx.fiber.uid !== null
  }

  snapshot() {
    const schemas = this.rootCtx.tools.schemas(this.agent).map(s => ({
      name: s.name, description: s.description, parameters: s.parameters,
    }))
    this.candidates = selectCandidates(schemas, this.config)
    this.index = buildIndex(this.candidates)
    const known = new Set(this.candidates.map(s => s.name))
    for (const n of [...this.loaded]) if (!known.has(n)) this.loaded.delete(n)
  }

  hiddenSchemas() {
    return this.candidates.filter(s => !this.loaded.has(s.name))
  }

  install() {
    if (!this.active) throw new Error('cannot install on an inactive agent context')
    this.lifecycleDispose = this.agent.ctx.effect(() => () => {
      this.lifecycleDispose = undefined
      this.dispose()
    }, `${name}(${this.agent.id}).agent-lifecycle`)
    this.snapshot()
    this.gated = shouldGate(this.config.mode, this.candidates.length, this.config.autoMinTools)
    if (!this.gated) {
      this.log(`not gating (${this.candidates.length} candidates, mode=${this.config.mode})`)
      return
    }
    this.internalMutation(() => {
      this.replaceRestriction(false)
      this.replaceSearchTool()
    })
    this.log(`installed; hiding ${this.hiddenSchemas().length} tools`)
  }

  /** Real registry change: lift our own effects, re-snapshot, rebuild. */
  refresh() {
    if (!this.active) { this.dispose(); return }
    this.internalMutation(() => {
      this.searchDispose?.(); this.searchDispose = undefined
      this.restrictionDispose?.(); this.restrictionDispose = undefined
      this.snapshot()
      this.gated = shouldGate(this.config.mode, this.candidates.length, this.config.autoMinTools)
      if (!this.active || !this.gated) return
      this.replaceRestriction(false)
      this.replaceSearchTool()
    })
    if (!this.active) { this.dispose(); return }
    this.log(`refreshed; hiding ${this.hiddenSchemas().length} tools`)
  }

  search(query, limit) {
    if (!this.active) throw new Error('tool search is inactive')
    const cap = Number.isFinite(limit) && limit > 0 ? Math.min(limit, 20) : this.config.searchLimit
    const hits = bm25Search(this.index, query, cap)
    const added = []
    for (const hit of hits) {
      if (!this.loaded.has(hit.name)) { this.loaded.add(hit.name); added.push(hit.name) }
    }
    if (added.length > 0) {
      this.internalMutation(() => {
        this.replaceRestriction(true)
        this.replaceSearchTool()
      })
      this.log(`loaded ${added.length} tools for "${query}"`)
    }
    return {
      tools: hits.map(h => ({ name: h.name, description: h.description ?? '' })),
      remainingHidden: this.hiddenSchemas().length,
    }
  }

  dispose() {
    if (this.disposed) return
    this.disposed = true
    const lifecycle = this.lifecycleDispose
    this.lifecycleDispose = undefined
    lifecycle?.()
    this.internalMutation(() => {
      this.searchDispose?.(); this.searchDispose = undefined
      this.restrictionDispose?.(); this.restrictionDispose = undefined
    })
  }

  /** Register the new mask before lifting the old one so nothing is over-exposed in between. */
  replaceRestriction(registerBeforeDispose) {
    if (!this.active) return
    const denied = this.hiddenSchemas().map(s => s.name).sort()
    const old = this.restrictionDispose
    if (denied.length === 0) {
      old?.()
      this.restrictionDispose = undefined
      return
    }
    let next
    try {
      next = this.agent.ctx.tools.restrict({ deny: denied })
    } catch (error) {
      // Fail open: leave the previous mask in place if it exists, else nothing is hidden.
      this.log(`restrict failed, failing open: ${String(error)}`, 'warn')
      if (!registerBeforeDispose) { old?.(); this.restrictionDispose = undefined }
      return
    }
    old?.()
    this.restrictionDispose = next
  }

  replaceSearchTool() {
    if (!this.active) return
    this.searchDispose?.()
    this.searchDispose = this.agent.ctx.tools.register(createSearchToolDefinition(this))
  }

  log(message, level = 'info') {
    if (!this.config.debug && level === 'info') return
    const logger = this.rootCtx.logger
    if (logger && typeof logger[level] === 'function') logger[level](`${name}(${this.agent.id}): ${message}`)
  }
}

function createSearchToolDefinition(controller) {
  return {
    name: controller.config.searchToolName,
    description: searchToolDescription(controller.hiddenSchemas()),
    parameters: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Keywords describing the capability you need, e.g. "notion search pages" or "generate video".' },
        limit: { type: 'integer', minimum: 1, maximum: 20, description: 'Maximum number of tools to load (default 8).' },
      },
      required: ['query'],
      additionalProperties: false,
    },
    output: {
      schema: {
        type: 'object',
        properties: {
          tools: {
            type: 'array',
            items: {
              type: 'object',
              properties: { name: { type: 'string' }, description: { type: 'string' } },
              required: ['name', 'description'],
              additionalProperties: false,
            },
          },
          remainingHidden: { type: 'integer' },
        },
        required: ['tools', 'remainingHidden'],
        additionalProperties: false,
      },
      render: (_args, value) => {
        const result = value
        if (result.tools.length === 0) {
          return [{ type: 'text', text: `No matching tools. ${result.remainingHidden} application tools remain hidden; try different keywords.` }]
        }
        const lines = result.tools.map(t => `- ${t.name}: ${t.description.split('\n')[0].slice(0, 200)}`)
        return [{ type: 'text', text: `Loaded ${result.tools.length} tools; call them directly by name now:\n${lines.join('\n')}\n(${result.remainingHidden} application tools remain hidden.)` }]
      },
    },
    async execute(args, exec) {
      if (exec?.agent !== undefined && exec.agent !== controller.agent) {
        throw new Error(`${controller.config.searchToolName} was invoked from a different agent scope`)
      }
      const query = typeof args?.query === 'string' ? args.query.trim() : ''
      if (query === '') throw new Error('query must be a non-empty string')
      const limit = typeof args?.limit === 'number' ? args.limit : undefined
      return controller.search(query, limit)
    },
  }
}

function isInactiveEffectError(error) {
  return typeof error === 'object' && error !== null && 'code' in error && error.code === 'INACTIVE_EFFECT'
}

export function apply(ctx, rawConfig) {
  const config = resolveConfig(rawConfig)
  if (config.mode === 'off') return
  if (!rootCompatible(ctx)) {
    ctx.logger?.warn?.(`${name}: incompatible dsh runtime; disabled without changing tool visibility`)
    return
  }

  const controllers = new Map()
  let internalMutationDepth = 0
  let refreshScheduled = false
  let active = true

  const internalMutation = (operation) => {
    internalMutationDepth += 1
    try { return operation() } finally { internalMutationDepth -= 1 }
  }

  const install = (agent) => {
    if (!active || controllers.has(agent)) return
    if (!agentCompatible(agent)) {
      ctx.logger?.warn?.(`${name}(${agent?.id}): incompatible agent runtime; leaving native tool visibility unchanged`)
      return
    }
    if (agent.ctx.fiber.uid === null) return
    const controller = new AgentToolSearch(ctx, agent, config, internalMutation)
    controller.install()
    if (!controller.active) return
    controllers.set(agent, controller)
  }

  const disposeAgent = (agent) => {
    const controller = controllers.get(agent)
    if (controller === undefined) return
    controllers.delete(agent)
    controller.dispose()
  }

  const refreshAll = () => {
    refreshScheduled = false
    if (!active) return
    for (const [agent, controller] of [...controllers]) {
      if (!controller.active) { disposeAgent(agent); continue }
      try {
        controller.refresh()
      } catch (error) {
        if (!controller.active || isInactiveEffectError(error)) { disposeAgent(agent); continue }
        ctx.logger?.error?.(`${name}(${agent.id}): failed to refresh: ${String(error)}`)
      }
    }
  }

  for (const agent of ctx.agents.list()) install(agent)

  // dsh 0.1.6: `agent/created` is the serial creation dispatch; `agent/session-start` no longer exists.
  ctx.on('agent/created', ({ agent }) => {
    try {
      install(agent)
    } catch (error) {
      if (isInactiveEffectError(error) || agent?.ctx?.fiber?.uid === null) return
      ctx.logger?.error?.(`${name}(${agent?.id}): failed to install: ${String(error)}`)
    }
  })
  ctx.on('agent/disposed', ({ agent }) => { disposeAgent(agent) })
  ctx.on('tools/change', () => {
    if (!active || internalMutationDepth > 0 || refreshScheduled) return
    refreshScheduled = true
    queueMicrotask(refreshAll)
  })

  ctx.effect(() => () => {
    active = false
    for (const controller of controllers.values()) controller.dispose()
    controllers.clear()
  }, `${name}.lifecycle`)
}
