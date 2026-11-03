// Private OpenClaw seams are accepted only for this audited published bundle.
import { createHash } from 'node:crypto'
import { readFileSync, realpathSync } from 'node:fs'
import { dirname, join, parse } from 'node:path'
import { pathToFileURL } from 'node:url'

export const VERSION = '2026.9.4'
export const FILES = {
  server: ['server-DWybsgYT.mjs', '96113ca44674714f6157b040ecd956f94c226bd983f62ff342ad9ef78963d020'],
  registry: ['reply-run-registry.registry-DDgx8cNZ.mjs', '3d53a6271de56728e8b70c7d7cc1989adc52d6fb2033a7a838c2633e09615f09'],
  embedded: ['builtin-openclaw-B-H-7lKk.mjs', '0a8c813e535c92d03f69bc58381518ba0e6ac6e46f3adda54138c5f668340ea8'],
}
export const digest = source => createHash('sha256').update(source).digest('hex')

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

export async function loadGatewayInternals(root = packageRoot()) {
  if (!root) throw new Error('unsupported_openclaw_version')
  const registry = checkedFile(root, 'registry')
  checkedFile(root, 'embedded')
  const module = await import(registry.url)
  if (!module.h?.get || !module.V?.get || typeof module.A !== 'function') {
    throw new Error('unsupported_openclaw_exports')
  }
  return { registry: module.h, operations: module.V, backend: module.A }
}
