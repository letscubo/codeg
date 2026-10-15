// fork(letscubo): codeg's replacement for `@deepseek-ai/dsh-headless`'s runner.
//
// The stock one-shot runner does: followup(task) → agent.whenIdle() → flush →
// print the final text → appExit. It never waits for BACKGROUND subagents
// (dsh-base mounts `tool-subagent` with `backgroundMode: continuable`, so the
// model's default `subagent` call returns "started subagent <id>" at once), and
// exiting kills them mid-task — the parent's "I'll deliver when it is done" is
// never honoured.
//
// dsh itself already delivers the child's outcome back to the parent as queued
// user messages (the child's own `send` report and the runtime's "Background
// subagent … finished" settlement notice), and an idle Agent with a pending
// inbox runs a follow-up turn — exactly what `claude -p` does in-process. So
// this runner is the stock one plus one step: after the parent goes idle, keep
// waiting while any descendant Agent is busy or the parent has queued input,
// then drain, flush and print. Output is the stock JSON projection, so codeg's
// `dsh_stream` mapper reads it unchanged (follow-ups are just more
// turn_start/turn_end pairs before the single `final`).
//
// Zero static imports: dsh's internal packages are resolved at runtime from the
// launcher that loaded us (`process.argv[1]` → the `@deepseek-ai/dsh` package),
// so this file can live anywhere (codeg writes it to `$DSH_HOME/plugins/`).
// Pinned to dsh 0.1.6-alpha.2 internals — re-verify on every dsh bump:
// `agents.create/resume/list/isOwnedBy`, `agent.phase.kind`, `agent.inbox
// .hasPending`, `subagents.drainContinuableDescendants`, and the hashed
// `dsh-headless/lib/json-stream-*.js` chunk exporting `projectJsonRun`.
//
// Verified on A3 (2026-09-18): background subagent → parent idle at 12s →
// follow-up turns at 39–50s → final at 51s; resume after it; SIGTERM cancel.

import {
  realpathSync,
  readdirSync,
  readFileSync,
  existsSync,
  mkdirSync,
  writeFileSync,
  renameSync,
  rmSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { randomUUID, createHash } from "node:crypto";

export const name = "codeg-headless-runner";
export const inject = ["agentDefaultModel", "agents", "sessions"];

const DEFAULT_BACKGROUND_WAIT_SECS = 20 * 60;
const DEFAULT_EXIT_GRACE_MS = 3000;
/** Let a just-settled child's report land in the parent inbox before judging. */
const SETTLE_QUIET_MS = 500;
/**
 * Where a turn's tool images land, under the session's own working directory.
 *
 * MUST stay in sync with `SESSION_TOOL_MEDIA_DIR` in codeg's `acp::connection`:
 * codeg computes the very same path when it replays history, and the two never
 * talk to each other.
 */
const SESSION_TOOL_MEDIA_DIR = ".codeg-media";

const log = (message) => process.stderr.write(`codeg-runner: ${message}\n`);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ── dsh internals ──────────────────────────────────────────────────────────

/** The `@deepseek-ai/dsh` package root of the launcher running us. */
export function launcherRoot(argv1 = process.argv[1]) {
  let dir = dirname(realpathSync(argv1));
  for (let i = 0; i < 8; i++) {
    const pkg = join(dir, "package.json");
    if (existsSync(pkg)) {
      try {
        if (JSON.parse(readFileSync(pkg, "utf8")).name === "@deepseek-ai/dsh") return dir;
      } catch {}
    }
    const parent = dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  throw new Error(`codeg-runner: cannot locate @deepseek-ai/dsh from ${argv1}`);
}

/** Directory of an `@deepseek-ai/<pkg>` dependency: nested first, then hoisted. */
export function packageDir(root, pkg) {
  let dir = root;
  for (let i = 0; i < 8; i++) {
    const candidate = join(dir, "node_modules", "@deepseek-ai", pkg);
    if (existsSync(join(candidate, "package.json"))) return candidate;
    const parent = dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  throw new Error(`codeg-runner: cannot locate @deepseek-ai/${pkg} from ${root}`);
}

/** The ESM entry of a package (its `exports["."]`, falling back to `main`). */
export function entryOf(dir) {
  const manifest = JSON.parse(readFileSync(join(dir, "package.json"), "utf8"));
  const dot = manifest.exports?.["."] ?? manifest.exports;
  const rel =
    typeof dot === "string"
      ? dot
      : dot?.import?.default ?? dot?.import ?? dot?.default ?? manifest.module ?? manifest.main ?? "lib/index.js";
  return join(dir, typeof rel === "string" ? rel : "lib/index.js");
}

async function loadInternals() {
  const root = launcherRoot();
  const imp = (file) => import(pathToFileURL(file).href);
  const llm = await imp(entryOf(packageDir(root, "dsh-llm")));
  const agent = await imp(entryOf(packageDir(root, "dsh-agent")));
  const session = await imp(entryOf(packageDir(root, "dsh-session")));
  const headlessLib = join(packageDir(root, "dsh-headless"), "lib");
  const chunk = readdirSync(headlessLib).find((f) => /^json-stream-.*\.js$/.test(f));
  if (chunk === undefined) throw new Error("codeg-runner: dsh-headless JSON projection chunk not found");
  const stream = await imp(join(headlessLib, chunk));
  const byName = (fnName) => Object.values(stream).find((v) => typeof v === "function" && v.name === fnName);
  const internals = {
    createUserMessage: llm.createUserMessage,
    installModelSelection: agent.installModelSelection,
    SessionSeq: session.SessionSeq ?? ((n) => n),
    projectJsonRun: byName("projectJsonRun"),
    boundJsonLine: byName("boundJsonLine") ?? ((value) => JSON.stringify(value)),
  };
  for (const [key, value] of Object.entries(internals)) {
    if (typeof value !== "function") throw new Error(`codeg-runner: dsh internal ${key} is unavailable`);
  }
  return internals;
}

// ── pure helpers (unit-tested) ─────────────────────────────────────────────

/** Whether an Agent is still working or has queued input it will run. */
export function isBusy(agent) {
  return agent?.phase?.kind === "running" || agent?.inbox?.hasPending === true;
}

/** Live Agents created (transitively) under `root`, via the registry's owner links. */
export function descendantsOf(agents, root) {
  const out = [];
  const frontier = [root];
  const all = agents.list();
  while (frontier.length > 0) {
    const owner = frontier.pop();
    for (const candidate of all) {
      if (candidate === root || out.includes(candidate)) continue;
      if (agents.isOwnedBy(candidate.id, owner)) {
        out.push(candidate);
        frontier.push(candidate);
      }
    }
  }
  return out;
}

/**
 * Wait until the parent and every descendant are idle with nothing queued.
 * @returns true when settled, false when the budget ran out first.
 */
export async function settle(agents, parent, budgetMs, quietMs = SETTLE_QUIET_MS) {
  const deadline = Date.now() + budgetMs;
  for (;;) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) return false;
    const idle = await Promise.race([parent.whenIdle().then(() => true), sleep(remaining).then(() => false)]);
    if (!idle) return false;
    const busy = descendantsOf(agents, parent).filter(isBusy);
    if (busy.length === 0) {
      await sleep(quietMs);
      if (isBusy(parent)) continue; // a report / settlement notice just woke it
      if (descendantsOf(agents, parent).some(isBusy)) continue;
      return true;
    }
    const left = deadline - Date.now();
    if (left <= 0) return false;
    await Promise.race([Promise.all(busy.map((child) => child.whenIdle())), sleep(left)]);
    await sleep(quietMs);
  }
}

/** Last assistant text and turn outcome after `firstSeq` (stock `summarize`). */
export function summarize(session, firstSeq, SessionSeq = (n) => n) {
  let started = false;
  let text = "";
  let reason;
  for (let seq = firstSeq; seq < session.seq; seq++) {
    const event = session.eventAt(SessionSeq(seq));
    if (event === undefined) continue;
    if (event.type === "turn/start") {
      started = true;
      continue;
    }
    if (!started) continue;
    if (event.type === "assistant/message") {
      const joined = event.data.message.content
        .filter((block) => block.type === "text")
        .map((block) => block.text)
        .join("");
      if (joined !== "") text = joined;
    }
    if (event.type === "turn/end") reason = event.data.reason;
  }
  return { text, reason };
}

// ── tool-result images ─────────────────────────────────────────────────────
//
// dsh's JSON projection drops them: `resultText` keeps `type: "text"` blocks
// only, so a screenshot reaches codeg as `{"type":"tool_result","result":""}` —
// the model saw the image, the card and the history never did (2026-10-13).
//
// We put them back, but NOT as bytes on the line. That stream is deliberately
// bounded (8 KiB per string, 32 KiB per event, see `boundJsonLine`): it is an
// event stream, not a data channel, and one screenshot is 100 KiB+. So the
// bytes go to disk here and the line carries only the path.
//
// The path is the one codeg would have computed itself (same digest, same
// directory), because codeg's history replay is a pure function that must name the
// file without ever reading the disk. Keep `toolImageFileName` and
// `SESSION_TOOL_MEDIA_DIR` in lockstep with `acp::connection::tool_image_path`.

/** The `<path>…</path>` a dsh file result puts in its text block. */
function pathFromTextBlock(block) {
  if (block === null || typeof block !== "object" || block.type !== "text") return undefined;
  if (typeof block.text !== "string") return undefined;
  const found = /<path>([^<]+)<\/path>/.exec(block.text);
  const path = found?.[1]?.trim();
  return path !== undefined && path.startsWith("/") ? path : undefined;
}

/**
 * Images of one tool result, each as either `{path}` (already a file) or
 * `{data}` (base64 we still have to write).
 *
 * dsh does NOT put bytes in the event. Its image block is an attachment
 * reference — `{type:"image", attachment:{attachmentId, mediaType, …}}` — and
 * the tool has already written the file into the session directory. The path
 * rides along in the sibling text block (`<path>…</path>`), with the event's
 * `meta.path` as a fallback for a single-file result (2026-10-15, verified
 * against a real screenshot turn).
 *
 * The base64 shapes (MCP's `{data, mimeType}` and the Messages API's
 * `source.base64`) stay supported: a different MCP server reached through dsh
 * may still answer that way, and then we are the ones who must write the file.
 */
export function toolResultImages(blocks, metaPath) {
  if (!Array.isArray(blocks)) return [];
  const filePaths = blocks.map(pathFromTextBlock).filter((p) => p !== undefined);
  let nextPath = 0;
  const out = [];
  for (const block of blocks) {
    if (block === null || typeof block !== "object" || block.type !== "image") continue;
    // dsh: the bytes are already a file; carry its path, do not copy it.
    if (block.attachment !== null && typeof block.attachment === "object") {
      const path = filePaths[nextPath++] ?? (typeof metaPath === "string" ? metaPath : undefined);
      if (typeof path !== "string" || !path.startsWith("/")) continue;
      const mimeType = block.attachment.mediaType;
      out.push({ path, mimeType: typeof mimeType === "string" && mimeType ? mimeType : "image/png" });
      continue;
    }
    // MCP / Messages API: the bytes are inline, so we write the file ourselves.
    const base64 = block.source?.type === "base64" ? block.source : undefined;
    const data = typeof block.data === "string" ? block.data : base64?.data;
    if (typeof data !== "string" || data === "") continue;
    const mimeType = block.mimeType ?? block.mime_type ?? base64?.media_type ?? "image/png";
    out.push({ data, mimeType: typeof mimeType === "string" ? mimeType : "image/png" });
  }
  return out;
}

/**
 * File name codeg will look for: SHA-256 of the base64 TEXT, first 32 hex
 * digits, extension from the mime type. Mirrors `tool_image_path` exactly — a
 * drift here means the card points at a file that is not there.
 */
export function toolImageFileName(data, mimeType) {
  const ext =
    mimeType === "image/jpeg" || mimeType === "image/jpg"
      ? "jpg"
      : mimeType === "image/webp"
        ? "webp"
        : mimeType === "image/gif"
          ? "gif"
          : "png";
  const digest = createHash("sha256").update(data, "utf8").digest("hex");
  return `${digest.slice(0, 32)}.${ext}`;
}

/**
 * The tool result a `tool/result` event carries, across dsh versions.
 *
 * 0.1.6 wraps it: `message.content[0]` is `{toolCallId, content}`. 0.2.x drops
 * that level and puts both on `message`. Reading both keeps a dsh bump from
 * silently losing images again (it would not throw — just stop finding any).
 */
export function toolResultOf(event) {
  const message = event?.data?.message;
  if (message === null || typeof message !== "object") return undefined;
  if (typeof message.toolCallId === "string") return message;
  const first = Array.isArray(message.content) ? message.content[0] : undefined;
  return first !== null && typeof first === "object" && typeof first.toolCallId === "string"
    ? first
    : undefined;
}

/** Write one image under the session directory; returns the path it lives at. */
function writeToolImage(sessionDir, image) {
  const dir = join(sessionDir, SESSION_TOOL_MEDIA_DIR);
  const path = join(dir, toolImageFileName(image.data, image.mimeType));
  // The same screenshot in two turns is one file — content-addressed.
  if (existsSync(path)) return path;
  mkdirSync(dir, { recursive: true });
  // Write-then-rename: a reader must never see a half-written PNG under a name
  // that says "this is the whole picture".
  const tmp = `${path}.part`;
  try {
    writeFileSync(tmp, Buffer.from(image.data, "base64"));
    renameSync(tmp, path);
  } catch (err) {
    try {
      rmSync(tmp, { force: true });
    } catch {}
    throw err;
  }
  return path;
}

/**
 * Scan forward from `from`, writing every tool result's images and recording
 * their paths by call id. Returns the new cursor, so each line only costs the
 * events since the last one.
 *
 * Best effort by contract: a failed write only means that card has no picture,
 * and a turn must never fail over it.
 */
export function indexToolImages(session, SessionSeq, from, into, sessionDir) {
  let seq = from;
  for (; seq < session.seq; seq++) {
    const event = session.eventAt(SessionSeq(seq));
    if (event === undefined || event.type !== "tool/result") continue;
    const result = toolResultOf(event);
    if (result === undefined) continue;
    const written = [];
    for (const image of toolResultImages(result.content, event.data?.meta?.path)) {
      // Already a file (dsh's own attachments): carry the path as is.
      if (image.path !== undefined) {
        written.push({ path: image.path, mimeType: image.mimeType });
        continue;
      }
      try {
        written.push({ path: writeToolImage(sessionDir, image), mimeType: image.mimeType });
      } catch (err) {
        log(`tool image not saved: ${String(err)}`);
      }
    }
    if (written.length > 0) into.set(result.toolCallId, written);
  }
  return seq;
}

/**
 * Add `images` to a `tool_result` line. Anything else passes through byte for
 * byte — an unparseable or unrelated line is never rewritten.
 */
export function attachImagesToLine(line, lookup) {
  if (typeof line !== "string" || !line.includes('"tool_result"')) return line;
  let value;
  try {
    value = JSON.parse(line);
  } catch {
    return line;
  }
  if (value === null || typeof value !== "object") return line;
  if (value.type !== "tool_result" || typeof value.callId !== "string") return line;
  const images = lookup(value.callId);
  if (images === undefined || images.length === 0) return line;
  return `${JSON.stringify({ ...value, images })}\n`;
}

/**
 * The projection's sink, with tool-result images saved and their paths put back
 * on the way out.
 *
 * Reading the session (rather than listening for the event) is what makes this
 * independent of listener order: by the time the projection has written a line,
 * the event it describes is already in the session.
 */
export function imageAttachingSink(out, session, SessionSeq, sessionDir) {
  const byCallId = new Map();
  let cursor = 0;
  return {
    write(chunk) {
      return out.write(
        attachImagesToLine(chunk, (callId) => {
          cursor = indexToolImages(session, SessionSeq, cursor, byCallId, sessionDir);
          return byCallId.get(callId);
        }),
      );
    },
  };
}

// ── the run ────────────────────────────────────────────────────────────────

function* liveEvents(session, SessionSeq) {
  for (let seq = 0; seq < session.seq; seq++) {
    const event = session.eventAt(SessionSeq(seq));
    if (event !== undefined) yield event;
  }
}

function assertAdoptable(header, sessionId, cwd) {
  if (header.origin === "subagent" || header.parentSession !== undefined) {
    throw new Error(`session "${sessionId}" is a subagent or forked session and cannot be driven directly`);
  }
  if (header.cwd === undefined) throw new Error(`session "${sessionId}" recorded no working directory, so it cannot be adopted`);
  if (header.cwd !== cwd) throw new Error(`session "${sessionId}" was recorded in "${header.cwd}", not "${cwd}"`);
}

async function resumeHandle(ctx, agents, sessionId, agentOptions, setup, cwd) {
  if (ctx.get("sessionPersistence") === undefined) {
    throw new Error("headless --session-id requires the sessionPersistence service; the Session would not survive this process");
  }
  const query = ctx.get("sessionQuery");
  if (query === undefined) throw new Error("headless --session-id requires the sessionQuery service");
  const live = agents.get(sessionId);
  if (live !== undefined) {
    throw new Error(`session "${sessionId}" is live in this process, so the runner cannot own an exclusive run interval`);
  }
  let observation;
  try {
    observation = await query.observeSession(sessionId);
  } catch (error) {
    if (error?.code === "SESSION_QUERY_SESSION_NOT_FOUND") {
      throw new Error(`session "${sessionId}" not found: no stored Session; omit --session-id to start a new Session`);
    }
    throw error;
  }
  try {
    assertAdoptable(observation.header, sessionId, cwd);
  } finally {
    await observation[Symbol.asyncDispose]?.();
    observation[Symbol.dispose]?.();
  }
  return agents.resume({ resumeSessionId: sessionId, agentOptions, setup });
}

async function readStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}

async function run(ctx, config, internals) {
  await ctx.get("loader")?.await();
  const agents = ctx.get("agents");
  const defaultModel = ctx.get("agentDefaultModel");
  const sessions = ctx.get("sessions");
  if (agents === undefined || defaultModel === undefined || sessions === undefined) {
    throw new Error("codeg-runner: the agents / agentDefaultModel / sessions services are not mounted");
  }
  const wantSession = typeof config.sessionId === "string" && config.sessionId.trim() !== "" ? config.sessionId : undefined;
  const task = config.task === undefined || config.task === "-" ? await readStdin() : config.task;
  if (typeof task !== "string" || task.trim() === "") throw new Error("a task is required");

  const selection = defaultModel.currentSelection();
  const agentOptions = { provider: selection.provider, model: selection.model };
  // Must return nothing: the registry calls `.commit()` on a returned value.
  const setup = (agentCtx) => {
    internals.installModelSelection(agentCtx, { current: selection, assembled: undefined });
  };
  const fs = ctx.get("fs");
  const cwd = fs === undefined ? process.cwd() : fs.processPath(await fs.resolve("."));
  const handle =
    wantSession === undefined
      ? await agents.create({ sessionId: `session-${randomUUID()}`, meta: { cwd }, agentOptions, setup })
      : await resumeHandle(ctx, agents, wantSession, agentOptions, setup, cwd);
  const agent = handle.agent;
  await agent.whenIdle();
  if (wantSession !== undefined) assertAdoptable(agent.session.header, wantSession, cwd);
  const firstSeq = agent.session.seq;
  // Same projection as stock, through a sink that saves tool-result images under
  // the session directory and puts their paths back on the line (dsh's own
  // `resultText` keeps text blocks only, and its stream is capped at 32 KiB).
  const projection =
    config.json === true
      ? internals.projectJsonRun(
          ctx,
          agent,
          imageAttachingSink(process.stdout, agent.session, internals.SessionSeq, cwd),
          { cwd },
        )
      : undefined;
  try {
    agent.followup(
      internals.createUserMessage({ content: [{ type: "text", text: task }], source: { kind: "user" } }),
    );
    const budgetSecs = Number(config.backgroundWaitSecs) > 0 ? Number(config.backgroundWaitSecs) : DEFAULT_BACKGROUND_WAIT_SECS;
    const settled = await settle(agents, agent, budgetSecs * 1000);
    if (!settled) {
      log(`background subagents still running after ${Math.round(budgetSecs / 60)} min; stopping them`);
    }
    // Stops whatever is still resident (all of it idle when settled) and
    // releases the child forest before the parent is flushed.
    await ctx.get("subagents")?.drainContinuableDescendants?.([agent]);
    if (!settled) agent.cancel?.({ kind: "user" });
    await agent.whenIdle();
    await sessions.flush(agent.session);
    const outcome = summarize(agent.session, firstSeq, internals.SessionSeq);
    await handle.dispose?.();
    if (!settled && projection !== undefined) {
      process.stdout.write(
        `${internals.boundJsonLine({
          type: "error",
          message: `Background subagents were still running after ${Math.round(budgetSecs / 60)} minutes and were stopped`,
        })}\n`,
      );
    }
    if (projection === undefined) process.stdout.write(`${outcome.text}\n`);
    else projection.finish(outcome.text);
    if (outcome.reason?.kind === "error") {
      process.stderr.write(`dsh: ${outcome.reason.error.code}: ${outcome.reason.error.message}\n`);
    }
    return outcome.reason?.kind === "completed" ? 0 : 1;
  } finally {
    projection?.dispose?.();
  }
}

export function apply(ctx, config) {
  const exit = ctx.get("appExit");
  if (exit === undefined) throw new Error("codeg-runner: the launcher must provide ctx.appExit before the tree mounts");
  const graceMs = Number(config.exitGraceMs) > 0 ? Number(config.exitGraceMs) : DEFAULT_EXIT_GRACE_MS;
  const finish = (code) => {
    exit(code);
    // After a subagent has run, appExit alone can leave the process alive on a
    // lingering handle (A3: one Socket). Everything is flushed and printed by
    // now, so a forced exit is safe; unref'd so it never delays a clean exit.
    setTimeout(() => process.exit(code), graceMs).unref();
  };
  loadInternals()
    .then((internals) =>
      run(ctx, config, internals).catch((error) => {
        const message = error instanceof Error ? error.message : String(error);
        if (config.json === true) {
          process.stdout.write(`${internals.boundJsonLine({ type: "error", message })}\n`);
        }
        process.stderr.write(`dsh: ${message}\n`);
        return 1;
      }),
    )
    .catch((error) => {
      const message = error instanceof Error ? error.message : String(error);
      if (config.json === true) process.stdout.write(`${JSON.stringify({ type: "error", message })}\n`);
      process.stderr.write(`dsh: ${message}\n`);
      return 1;
    })
    .then(finish);
}
