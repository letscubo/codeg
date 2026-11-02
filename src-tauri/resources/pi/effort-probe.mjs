// Official @earendil-works/pi-coding-agent 0.84.2 SDK, metadata only.
// Never load extensions, refresh catalogs, resolve auth or create a session.
import { existsSync, readFileSync, realpathSync } from "node:fs";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { verifiedWireLevels } from "../codeg-effort-wire.mjs";

export function packageRoot(executable, name) {
  let dir = dirname(realpathSync(executable));
  for (let i = 0; i < 12; i++) {
    const file = join(dir, "package.json");
    if (existsSync(file) && JSON.parse(readFileSync(file, "utf8")).name === name) return dir;
    const parent = dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  throw new Error("Cannot verify the installed Pi package");
}

function dependencyRoot(root, name) {
  for (let dir = root; ; dir = dirname(dir)) {
    const candidate = join(dir, "node_modules", name);
    if (existsSync(join(candidate, "package.json"))) return candidate;
    if (dirname(dir) === dir) throw new Error("Cannot verify the installed Pi model library");
  }
}

function exportedEntry(root, key) {
  const pkg = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
  const entry = pkg.exports?.[key];
  const file = typeof entry === "string" ? entry : entry?.import?.default ?? entry?.import ?? entry?.default;
  if (typeof file !== "string") throw new Error("The installed Pi model API is unavailable");
  return join(root, file);
}

export function readonlySettingsStorage(cwd, agentDir, configDirName) {
  return {
    withLock(scope, read) {
      const path = scope === "global" ? join(agentDir, "settings.json") : join(cwd, configDirName, "settings.json");
      const next = read(existsSync(path) ? readFileSync(path, "utf8") : undefined);
      if (next !== undefined) throw new Error("A metadata probe cannot write settings");
    },
  };
}

export async function probeModels(executable, queries, cwd = process.cwd()) {
  const root = packageRoot(executable, "@earendil-works/pi-coding-agent");
  const manifest = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
  const sdk = await import(pathToFileURL(join(root, "dist/index.js")).href);
  const compat = await import(pathToFileURL(exportedEntry(dependencyRoot(root, "@earendil-works/pi-ai"), "./compat")).href);
  const defaults = await import(pathToFileURL(join(root, "dist/core/defaults.js")).href);
  const agentDir = sdk.getAgentDir();
  const deny = () => { throw new Error("A metadata probe cannot write runtime storage"); };
  const runtime = await sdk.ModelRuntime.create({
    modelsPath: join(agentDir, "models.json"),
    credentials: { read: async () => undefined, list: async () => [], modify: deny, delete: deny },
    modelsStore: { read: async () => undefined, write: deny, delete: deny },
    allowModelNetwork: false,
    refreshOnCreate: false,
  });
  const projectSettings = join(cwd, sdk.CONFIG_DIR_NAME, "settings.json");
  // Trust resolution itself writes trust files. Refuse clear in this ambiguous
  // case instead of reading an untrusted project default or restoring history.
  let clearError = existsSync(projectSettings)
    ? "Pi default thinking cannot be verified without the project's trust decision"
    : undefined;
  const settings = sdk.SettingsManager.fromStorage(readonlySettingsStorage(cwd, agentDir, sdk.CONFIG_DIR_NAME), { projectTrusted: false });
  if (settings.drainErrors().length > 0) clearError = "Pi settings could not be read to verify the default thinking level";
  const defaultLevel = settings.getDefaultThinkingLevel() ?? defaults.DEFAULT_THINKING_LEVEL;
  const models = queries.map(({ model, provider }) => {
    const descriptor = provider ? runtime.getModel(provider, model) : undefined;
    if (!descriptor || runtime.getError()) return { model, provider: provider ?? null, known: false, levels: [] };
    const native = verifiedWireLevels({
      runtime: "pi", provider, api: descriptor.api, map: descriptor.thinkingLevelMap,
      compat: descriptor.compat, nativeLevels: compat.getSupportedThinkingLevels(descriptor),
    });
    const levels = descriptor.reasoning === true ? native.map((level) => level === "off" ? "none" : level) : [];
    const effectiveDefault = typeof defaultLevel === "string" ? compat.clampThinkingLevel(descriptor, defaultLevel) : undefined;
    return { model, provider: provider ?? null, known: true, levels, defaultLevel: effectiveDefault,
      clearError: descriptor.reasoning === true ? clearError ?? (native.includes(effectiveDefault) ? undefined : "Pi default effort has no verified canonical wire mapping") : undefined };
  });
  return { type: "codeg_effort_capabilities", version: manifest.version, models };
}

if (process.env.CODEG_PI_EFFORT_PROBE === "1") {
  try {
    console.log(JSON.stringify(await probeModels(process.argv[2], JSON.parse(process.argv[3]))));
  } catch {
    // Do not echo provider configuration, credentials or package error payloads.
    process.stderr.write("Installed Pi model metadata could not be verified\n");
    process.exitCode = 1;
  }
}
