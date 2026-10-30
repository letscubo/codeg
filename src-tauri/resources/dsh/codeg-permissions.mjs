// Native approval answerer for a single CLI process. No policy or persistent grants.
import { randomUUID } from "node:crypto"
import { StringDecoder } from "node:string_decoder"

export const name = "codeg-permissions"
export const inject = ["approval"]

// Read only the exact pending tool call from this turn for the approval card.
export function pendingInput(req) {
  if (!req.callId || !req.agent?.session?.snapshotEvents) return undefined
  const events = req.agent.session.snapshotEvents()
  for (let i = events.length - 1; i >= 0; i--) {
    const event = events[i]
    if (event.type === "turn/start") break
    if (!["tool/call", "tool/ptc-dispatch-start"].includes(event.type)) continue
    if (event.data.callId !== req.callId || event.data.name !== req.toolName) continue
    const args = event.data.arguments
    if (typeof args !== "string") return args
    try {
      return JSON.parse(args)
    } catch {
      return { arguments: args }
    }
  }
  return undefined
}

export function apply(ctx) {
  const pending = new Map()
  let buffer = ""
  const decoder = new StringDecoder("utf8")
  const cancelPending = () => {
    for (const finish of pending.values()) finish("cancelled")
  }
  const onData = (chunk) => {
    buffer += decoder.write(chunk)
    if (buffer.length > 65536) {
      cancelPending()
      buffer = ""
      return
    }
    let end
    while ((end = buffer.indexOf("\n")) !== -1) {
      const line = buffer.slice(0, end)
      buffer = buffer.slice(end + 1)
      try {
        const value = JSON.parse(line)
        if (value.type === "codeg_permission_response" && typeof value.approved === "boolean") {
          pending.get(value.requestId)?.(value.approved ? "allowed-once" : "rejected")
        }
      } catch {
        // Malformed responses cannot grant access.
      }
    }
  }
  process.stdin.on("data", onData)
  process.stdin.on("end", cancelPending)
  process.stdin.on("error", cancelPending)
  ctx.on("dispose", () => {
    process.stdin.off("data", onData)
    process.stdin.off("end", cancelPending)
    process.stdin.off("error", cancelPending)
    if (process.stdin.listenerCount("data") === 0) process.stdin.pause()
    cancelPending()
  })
  ctx.on("approval/request", (req) => new Promise((resolve) => {
    const requestId = randomUUID()
    const done = (outcome) => {
      if (!pending.delete(requestId)) return
      clearTimeout(timer)
      req.signal?.removeEventListener("abort", abort)
      resolve(outcome)
    }
    const abort = () => done("cancelled")
    const timer = setTimeout(() => done("unavailable"), 180000)
    pending.set(requestId, done)
    if (req.signal?.aborted) {
      done("cancelled")
      return
    }
    req.signal?.addEventListener("abort", abort, { once: true })
    process.stdout.write(JSON.stringify({
      type: "codeg_permission_request",
      requestId,
      toolName: req.toolName,
      callId: req.callId,
      input: pendingInput(req),
      description: req.reason ?? req.toolName,
    }) + "\n")
  }))
}
