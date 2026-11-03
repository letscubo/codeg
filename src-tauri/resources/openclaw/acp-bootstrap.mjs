// This import hook only transforms the audited ACP module in memory. It never
// edits OpenClaw's installation, configuration, provider or Gateway process.
import * as moduleApi from 'node:module'
import { pathToFileURL } from 'node:url'
import { checkedFile, packageRoot, digest, FILES } from './compat.mjs'

export function patchAcpSource(source, bridgeUrl) {
  if (digest(source) !== FILES.server[1]) return null
  const anchor = '\t\tagent.start();\n\t\treturn agent;'
  if (source.split(anchor).length !== 2) return null
  return `import { installAcpBridge as __codegInstallNativeSteering } from ${JSON.stringify(bridgeUrl)};\n` +
    source.replace(anchor, '\t\t__codegInstallNativeSteering(agent, conn, gateway);\n' + anchor)
}

export function registerAcpHook({ entry = process.argv[1], args = process.argv.slice(2) } = {}) {
  if (args[0] !== 'acp' || typeof moduleApi.registerHooks !== 'function') return false
  try {
    const root = packageRoot(entry)
    if (!root) return false
    const server = checkedFile(root, 'server')
    const bridgeUrl = new URL('./acp-bridge.mjs', import.meta.url).href
    moduleApi.registerHooks({
      load(url, context, nextLoad) {
        const result = nextLoad(url, context)
        if (url !== pathToFileURL(server.path).href) return result
        const source = typeof result.source === 'string' ? result.source : Buffer.from(result.source).toString('utf8')
        const patched = patchAcpSource(source, bridgeUrl)
        return patched ? { ...result, source: patched } : result
      },
    })
    return true
  } catch { return false }
}

registerAcpHook()
