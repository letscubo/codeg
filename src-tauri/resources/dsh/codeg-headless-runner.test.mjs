// Run: node --test src-tauri/resources/dsh/codeg-headless-runner.test.mjs
import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, mkdirSync, writeFileSync, symlinkSync, realpathSync, readFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  attachImagesToLine,
  attachmentPath,
  descendantsOf,
  entryOf,
  imageAttachingSink,
  indexToolImages,
  isBusy,
  launcherRoot,
  packageDir,
  settle,
  summarize,
  toolImageFileName,
  toolResultImages,
  toolResultOf,
} from "./codeg-headless-runner.mjs";

/** A fake Agent: `run(ms)` makes it busy for `ms`, then idle. */
function fakeAgent(id) {
  const agent = {
    id,
    phase: { kind: "idle" },
    inbox: { hasPending: false },
    idle: Promise.resolve(),
    run(ms, then) {
      agent.phase = { kind: "running" };
      agent.idle = new Promise((resolve) =>
        setTimeout(() => {
          agent.phase = { kind: "idle" };
          resolve();
          then?.();
        }, ms),
      );
    },
    whenIdle() {
      return agent.idle;
    },
  };
  return agent;
}

function registry(owners) {
  const agents = [...owners.keys()];
  return { list: () => agents, isOwnedBy: (id, owner) => agents.find((a) => a.id === id) && owners.get(agents.find((a) => a.id === id)) === owner };
}

test("isBusy: running or queued input", () => {
  assert.equal(isBusy({ phase: { kind: "idle" }, inbox: { hasPending: false } }), false);
  assert.equal(isBusy({ phase: { kind: "running" } }), true);
  assert.equal(isBusy({ phase: { kind: "idle" }, inbox: { hasPending: true } }), true);
  assert.equal(isBusy(undefined), false);
});

test("descendantsOf walks owner links transitively and stops at the root", () => {
  const parent = fakeAgent("p");
  const child = fakeAgent("c");
  const grandchild = fakeAgent("g");
  const stranger = fakeAgent("s");
  const owners = new Map([[parent, undefined], [child, parent], [grandchild, child], [stranger, undefined]]);
  assert.deepEqual(descendantsOf(registry(owners), parent), [child, grandchild]);
});

test("settle waits for a busy child and the parent's follow-up it triggers", async () => {
  const parent = fakeAgent("p");
  const child = fakeAgent("c");
  const owners = new Map([[parent, undefined], [child, parent]]);
  const order = [];
  // Parent's first turn ends quickly; the child works longer, then its report
  // wakes the parent for a follow-up turn.
  parent.run(10, () => order.push("parent turn 1"));
  child.run(60, () => {
    order.push("child done");
    parent.inbox.hasPending = true;
    setTimeout(() => {
      parent.inbox.hasPending = false;
      parent.run(30, () => order.push("parent follow-up"));
    }, 5);
  });
  const ok = await settle(registry(owners), parent, 5000, 50);
  assert.equal(ok, true);
  assert.deepEqual(order, ["parent turn 1", "child done", "parent follow-up"]);
});

test("settle gives up at the budget when a child never finishes", async () => {
  const parent = fakeAgent("p");
  const child = fakeAgent("c");
  child.run(1_000);
  const started = Date.now();
  const ok = await settle(registry(new Map([[parent, undefined], [child, parent]])), parent, 150, 20);
  assert.equal(ok, false);
  assert.ok(Date.now() - started < 1500);
});

test("settle returns at once when nothing is running", async () => {
  const parent = fakeAgent("p");
  assert.equal(await settle(registry(new Map([[parent, undefined]])), parent, 1000, 10), true);
});

test("summarize keeps the last assistant text and the last turn reason", () => {
  const events = [
    { type: "turn/start" },
    { type: "assistant/message", data: { message: { content: [{ type: "text", text: "started" }] } } },
    { type: "turn/end", data: { reason: { kind: "completed" } } },
    { type: "turn/start" },
    { type: "assistant/message", data: { message: { content: [{ type: "text", text: "BG_OK delivered" }] } } },
    { type: "turn/end", data: { reason: { kind: "completed" } } },
  ];
  const session = { seq: events.length, eventAt: (i) => events[i] };
  assert.deepEqual(summarize(session, 0), { text: "BG_OK delivered", reason: { kind: "completed" } });
});

test("launcherRoot / packageDir / entryOf follow a real npm global layout", () => {
  const root = realpathSync(mkdtempSync(join(tmpdir(), "codeg-runner-")));
  const dsh = join(root, "lib", "node_modules", "@deepseek-ai", "dsh");
  mkdirSync(join(dsh, "lib"), { recursive: true });
  writeFileSync(join(dsh, "package.json"), JSON.stringify({ name: "@deepseek-ai/dsh" }));
  writeFileSync(join(dsh, "lib", "bin.js"), "");
  mkdirSync(join(root, "bin"));
  symlinkSync(join(dsh, "lib", "bin.js"), join(root, "bin", "dsh"));
  // nested dependency with an exports map, and a hoisted one with only `main`
  const nested = join(dsh, "node_modules", "@deepseek-ai", "dsh-llm");
  mkdirSync(join(nested, "lib"), { recursive: true });
  writeFileSync(join(nested, "package.json"), JSON.stringify({ exports: { ".": { import: "./lib/index.js", types: "./x.d.ts" } } }));
  const hoisted = join(root, "lib", "node_modules", "@deepseek-ai", "dsh-agent");
  mkdirSync(hoisted, { recursive: true });
  writeFileSync(join(hoisted, "package.json"), JSON.stringify({ main: "lib/main.js" }));

  assert.equal(launcherRoot(join(root, "bin", "dsh")), dsh);
  assert.equal(packageDir(dsh, "dsh-llm"), nested);
  assert.equal(packageDir(dsh, "dsh-agent"), hoisted);
  assert.equal(entryOf(nested), join(nested, "lib", "index.js"));
  assert.equal(entryOf(hoisted), join(hoisted, "lib", "main.js"));
  assert.throws(() => packageDir(dsh, "dsh-missing"));
});

// ── tool-result images ─────────────────────────────────────────────────────
// dsh's projection keeps text blocks only, so a screenshot reached codeg as an
// empty result: the model saw it, the card and the history never did.

/** A fake Session: events by seq, like dsh's own. */
function fakeSession(events) {
  return { seq: events.length, eventAt: (seq) => events[seq] };
}

/** 0.1.6 wraps the result in `content[0]`; 0.2.x puts it straight on `message`. */
function toolResultEvent(callId, content, { nested = true } = {}) {
  const result = { toolCallId: callId, content };
  return {
    type: "tool/result",
    data: { message: nested ? { content: [result] } : result },
  };
}

test("attachmentId → dsh 附件库的路径(按摘要前两位分片)", () => {
  // 规则在一个真实的附件库上核过 5 个对象(2026-10-16)
  const id = "sha256:197a9257b60c772898a64f3640f69829fa6e0a33fd9f1f6406caa95d78c2dbf3";
  assert.equal(
    attachmentPath("/dsh", id),
    "/dsh/attachments/v1/objects/19/197a9257b60c772898a64f3640f69829fa6e0a33fd9f1f6406caa95d78c2dbf3",
  );
  // 不是规规矩矩的小写 sha256 就不认 —— 绝不让一个怪 id 把读操作引到别处
  for (const bad of [
    undefined, null, 42, "", "197a9257", "sha256:", "sha256:XYZ",
    "sha256:" + "A".repeat(64), "sha256:" + "0".repeat(63), "sha256:../../etc/passwd",
    "md5:" + "0".repeat(64),
  ]) {
    assert.equal(attachmentPath("/dsh", bad), undefined, String(bad));
  }
  // 没有 DSH_HOME 就算不出位置
  for (const home of [undefined, null, "", 7]) assert.equal(attachmentPath(home, id), undefined, String(home));
});

test("只有 attachmentId、没有任何路径时也能定位(2026-10-16 实测的那一轮)", () => {
  const blocks = [
    {
      type: "image",
      attachment: {
        attachmentId: "sha256:197a9257b60c772898a64f3640f69829fa6e0a33fd9f1f6406caa95d78c2dbf3",
        mediaType: "image/png",
        width: 780,
        height: 493,
        bytes: 50898,
      },
    },
  ];
  // meta 为 null、没有 <path> 文本块 —— 只剩附件库这一条路
  assert.deepEqual(toolResultImages(blocks, undefined, "/dsh"), [
    {
      path: "/dsh/attachments/v1/objects/19/197a9257b60c772898a64f3640f69829fa6e0a33fd9f1f6406caa95d78c2dbf3",
      mimeType: "image/png",
    },
  ]);
  // 连 DSH_HOME 都没有(stock 环境)时跳过,不瞎猜
  assert.deepEqual(toolResultImages(blocks, undefined, undefined), []);
});

test("dsh 的 image 块是附件引用,字节早就是磁盘上的文件了 —— 带上它的路径,不复制", () => {
  // 这是 2026-10-15 从 C10 一轮真实截图里抄下来的形状
  const blocks = [
    {
      type: "text",
      text: "<path>/home/ubuntu/.myclaw/sessions/s1/example_com.png</path>\n<type>image</type>\n<content>\nimage/png image, 780x493 px, 51727 bytes\n</content>",
    },
    {
      type: "image",
      attachment: {
        attachmentId: "sha256:4aa3df4405ba6fa5eead5ef5dfbd0f14a4f0fdab3781250a35d08150badeefa2",
        mediaType: "image/png",
        bytes: 51727,
        width: 780,
        height: 493,
        name: "example_com.png",
      },
    },
  ];
  // 没有 DSH_HOME 时退到文本块给的文件名
  assert.deepEqual(toolResultImages(blocks), [
    { path: "/home/ubuntu/.myclaw/sessions/s1/example_com.png", mimeType: "image/png" },
  ]);
  // 两者都有时,附件库优先 —— 那个地址一定在,文件名未必
  assert.deepEqual(toolResultImages(blocks, undefined, "/dsh"), [
    {
      path: "/dsh/attachments/v1/objects/4a/4aa3df4405ba6fa5eead5ef5dfbd0f14a4f0fdab3781250a35d08150badeefa2",
      mimeType: "image/png",
    },
  ]);
  // 文本块没给路径时,回落到事件的 meta.path
  assert.deepEqual(toolResultImages([blocks[1]], "/home/ubuntu/.myclaw/sessions/s1/shot.png"), [
    { path: "/home/ubuntu/.myclaw/sessions/s1/shot.png", mimeType: "image/png" },
  ]);
  // 两者都没有、或路径不是绝对路径 → 跳过,绝不瞎编一个位置
  assert.deepEqual(toolResultImages([blocks[1]]), []);
  assert.deepEqual(toolResultImages([blocks[1]], "relative/x.png"), []);
});

test("带 base64 的两种形状仍然认(别的 MCP server 可能这么答),这时字节由我们落盘", () => {
  assert.deepEqual(
    toolResultImages([
      { type: "text", text: "ok" },
      { type: "image", data: "AAA", mimeType: "image/jpeg" },
      { type: "image", source: { type: "base64", data: "BBB", media_type: "image/webp" } },
    ]),
    [
      { data: "AAA", mimeType: "image/jpeg" },
      { data: "BBB", mimeType: "image/webp" },
    ],
  );
  assert.deepEqual(toolResultImages([{ type: "image", data: "CCC" }]), [{ data: "CCC", mimeType: "image/png" }]);
  // 没有字节也没有附件的(url 来源、空串)一律跳过
  assert.deepEqual(toolResultImages([{ type: "image", data: "" }, { type: "image", source: { type: "url", url: "x" } }]), []);
  for (const junk of [undefined, null, "", 42, [null, "x", { type: "text" }]]) {
    assert.deepEqual(toolResultImages(junk), [], String(junk));
  }
});

test("两代 dsh 的 tool/result 结构都认 —— 升级时图不会悄悄消失", () => {
  const blocks = [{ type: "image", data: "AAA" }];
  assert.equal(toolResultOf(toolResultEvent("c1", blocks))?.toolCallId, "c1", "0.1.6 的 content[0]");
  assert.equal(toolResultOf(toolResultEvent("c1", blocks, { nested: false }))?.toolCallId, "c1", "0.2.x 的 message");
  for (const junk of [undefined, null, {}, { data: {} }, { data: { message: { content: [] } } }]) {
    assert.equal(toolResultOf(junk), undefined, JSON.stringify(junk));
  }
});

test("文件名必须和 codeg 的 tool_image_path 逐字一致(sha256 of base64 文本, 前 32 位)", () => {
  // 写死的值:codeg 的 Rust 侧对同一份输入算出同一个名字(connection.rs 有对应断言)。
  // 两边任何一侧改了摘要算法,这里立刻红 —— 否则卡片会指向一个不存在的文件。
  assert.equal(
    toolImageFileName("iVBORw0KGgo=", "image/png"),
    "929e08d597feae564ce98003c4a47ff5.png",
    "摘要 = sha256(base64 文本) 的前 32 位十六进制",
  );
  // 扩展名跟 mime 走,`/file?path=` 那头据它决定 Content-Type
  assert.ok(toolImageFileName("AAA", "image/jpeg").endsWith(".jpg"));
  assert.ok(toolImageFileName("AAA", "image/webp").endsWith(".webp"));
  assert.ok(toolImageFileName("AAA", "image/gif").endsWith(".gif"));
  assert.ok(toolImageFileName("AAA", "application/octet-stream").endsWith(".png"), "认不出的 mime 回落 png");
  // 同样的字节 → 同一个文件;不同的图不能互相覆盖
  assert.equal(toolImageFileName("AAA", "image/png"), toolImageFileName("AAA", "image/png"));
  assert.notEqual(toolImageFileName("AAA", "image/png"), toolImageFileName("BBB", "image/png"));
});

test("图落在会话目录的隐藏子目录里,行上只留路径(不把 100KB base64 塞进 32KiB 的流)", () => {
  const dir = mkdtempSync(join(tmpdir(), "codeg-media-"));
  const data = Buffer.from("hello-png").toString("base64");
  const session = fakeSession([toolResultEvent("c1", [{ type: "image", data, mimeType: "image/png" }])]);
  const written = [];
  const sink = imageAttachingSink({ write: (chunk) => written.push(chunk) }, session, (n) => n, dir);

  sink.write(`${JSON.stringify({ type: "tool_call", callId: "c1", tool: "screenshot" })}\n`);
  sink.write(`${JSON.stringify({ type: "tool_result", callId: "c1", status: "completed", result: "" })}\n`);

  assert.equal(JSON.parse(written[0]).images, undefined, "tool_call 行不该被改");
  const out = JSON.parse(written[1]);
  assert.equal(out.images.length, 1);
  assert.equal(out.images[0].mimeType, "image/png");
  assert.equal(out.images[0].path, join(dir, ".codeg-media", toolImageFileName(data, "image/png")));
  assert.equal(readFileSync(out.images[0].path, "utf8"), "hello-png", "字节真的落盘了");
  assert.ok(!written[1].includes(data), "行上不该出现 base64");
});

test("dsh 附件形状: 路径原样上行,一个字节都不复制", () => {
  const dir = mkdtempSync(join(tmpdir(), "codeg-media-"));
  const shot = join(dir, "example_com.png");
  writeFileSync(shot, "png-bytes");
  const event = {
    type: "tool/result",
    data: {
      meta: { path: shot },
      message: {
        content: [
          {
            toolCallId: "c9",
            content: [
              { type: "text", text: `<path>${shot}</path>\n<type>image</type>` },
              { type: "image", attachment: { mediaType: "image/png", name: "example_com.png" } },
            ],
          },
        ],
      },
    },
  };
  const into = new Map();
  indexToolImages(fakeSession([event]), (n) => n, 0, into, dir);
  assert.deepEqual(into.get("c9"), [{ path: shot, mimeType: "image/png" }]);
  assert.ok(!existsSync(join(dir, ".codeg-media")), "附件已经是文件,不该再抄一份进 .codeg-media");
});

test("只改写带图的 tool_result 行,其余逐字节原样透传", () => {
  const lookup = (callId) => (callId === "c1" ? [{ path: "/x/a.png", mimeType: "image/png" }] : undefined);
  const noImages = `${JSON.stringify({ type: "tool_result", callId: "c2", result: "ok" })}\n`;
  assert.equal(attachImagesToLine(noImages, lookup), noImages);
  const other = `${JSON.stringify({ type: "tool_call", callId: "c1" })}\n`;
  assert.equal(attachImagesToLine(other, lookup), other);
  assert.equal(attachImagesToLine('{"type":"tool_result" oops\n', lookup), '{"type":"tool_result" oops\n');
  assert.equal(attachImagesToLine("", lookup), "");
  const ok = JSON.parse(attachImagesToLine(`${JSON.stringify({ type: "tool_result", callId: "c1", status: "completed" })}\n`, lookup));
  assert.equal(ok.status, "completed", "原有字段必须保留");
});

test("写盘失败不打断一轮,只是那张图没有", () => {
  const session = fakeSession([toolResultEvent("c1", [{ type: "image", data: "AAA" }])]);
  const into = new Map();
  // 指向一个普通文件,mkdir 必失败
  const file = join(mkdtempSync(join(tmpdir(), "codeg-media-")), "not-a-dir");
  writeFileSync(file, "x");
  assert.doesNotThrow(() => indexToolImages(session, (n) => n, 0, into, file));
  assert.equal(into.size, 0, "失败的图不留半条记录");
});
