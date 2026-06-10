I have everything verified against live source. Producing the deliverable doc.

# Hermes Subscription Broker + Per-Session Channel Shim — Design & Phase-0 POC

**Status:** design + POC spec. Decision = **Approach B (Minimal Hermes Rework)** with grafted durable-inbox / decoupled-egress / heartbeat ideas from A & C.
**Verified on this machine (2026-06-06):** `claude 2.1.166`, `bun 1.3.9`. **Two hard blockers found:** (1) `--dangerously-load-development-channels` is **NOT** in `claude --help` (only `--dangerously-skip-permissions` is); (2) **tmux is NOT installed**. Both must be resolved at POC-0 step 0 before any code — see §7.

> **Update (2026-06-10):** both blockers resolved (tmux installed; the channel
> path works via shim masquerade). **Phase 0 + Phase 1 are built and running
> live** under launchd on this machine, with `useBrokerSessions: true`. For how
> the deployed system runs (process management, logs, inbox recovery, the egress
> security model) and the open follow-ups, see **[`broker-operations.md`](broker-operations.md)**.
> This document remains the design rationale; several §8 open decisions are now
> settled (session granularity = `workspaceKey` per channel; idle-reap
> implemented but currently disabled pending fixes).

---

## 1. Architecture

One supervised **broker** = the existing Hermes daemon, unchanged at the gateway level. It owns the single Discord gateway + `channelDirectories` routing + allowlist + a durable inbox. It fans messages out over a Unix socket to **N thin per-session stdio MCP shims**, each glued to **one** long-running interactive `claude` REPL in its own tmux session. A crash in one project's session never deafens the others; every inbound is persisted, ack'd, deduped and replayable. The metered `claude -p` path stays behind a flag (`useBrokerSessions`, default off) as instant rollback.

```
                         ┌──────────────────────────────────────────────────────────┐
   Discord Gateway       │  HERMES BROKER  (existing daemon = src/commands/start.ts)  │
   (single WS conn) ────▶│                                                            │
        ▲   │            │  raw-WS gateway  (discord.ts: connectGateway/handleDispatch│
        │   │ MESSAGE_   │                   /startHeartbeat/resume — UNCHANGED)      │
        │   │ CREATE     │  checkAuth (router/auth.ts, fail-closed)                   │
        │   ▼            │  resolveChannelCwd(channelId)  (discord.ts:136)            │
   sendMessage          │  sessionKey = threadKey/workspaceKey (session-key.ts:40/44)│
   (egress, discord.ts  │  ┌──────────────┐    ┌─────────────────────────────────┐   │
    :183 chunk+[react:])│  │ inbox.db     │    │ session registry + supervisor   │   │
        ▲               │  │ (seq, state, │    │ Map<sessionKey,{tmux,sock,...}> │   │
        │ reply RPC     │  │  msgId UNIQUE│    │ ensureSession() / heartbeat /   │   │
        │               │  │  pending|    │    │ backoff+breaker / reap          │   │
        │               │  │  delivered|  │    └─────────────────────────────────┘   │
        │               │  │  answered)   │              │ spawn (lazy)               │
        │               │  └──────────────┘              ▼                            │
        │               │  broker.sock  (AF_UNIX, length-prefixed JSON-RPC)           │
        └───────────────┼──────────┬──────────────────────────┬─────────────────────┘
                        └──────────┼──────────────────────────┼──── inbound push / reply RPC / heartbeat
                                   │                          │
              ┌────────────────────▼─────┐       ┌────────────▼──────────────┐
              │ tmux: hermes-<keyA>      │       │ tmux: hermes-<keyB>       │   ← user can `tmux attach`
              │ ┌──────────────────────┐ │       │ ┌───────────────────────┐ │
              │ │ claude (interactive, │ │       │ │ claude (interactive)  │ │   ← SUBSCRIPTION billed
              │ │  --channels SHIM,    │ │       │ │  --channels SHIM      │ │
              │ │  cwd = projectA)     │ │       │ │  cwd = projectB)      │ │
              │ │   └─ stdio MCP SHIM  │ │       │ │   └─ stdio MCP SHIM   │ │   ← 1 shim : 1 session
              │ │      claude/channel  │ │       │ │      claude/channel   │ │      (no Discord conn)
              │ │      reply tool      │ │       │ │      reply tool       │ │
              │ └──────────────────────┘ │       │ └───────────────────────┘ │
              └──────────────────────────┘       └───────────────────────────┘
```

Key inversion vs. official plugins / telegram-supercharged: the gateway lives **only** in the broker (one consumer, so no 409 / no double-deliver). The shim holds **no** Discord connection — it is a dumb relay carrying the `claude/channel` capability + `reply` tool + an IPC link.

---

## 2. Component list

### Broker responsibilities (the existing Hermes daemon, near-zero gateway change)
- **Ingress (REUSE verbatim):** raw-WS gateway in `src/commands/discord.ts` — `connectGateway`, `handleDispatch`, `startHeartbeat`, resume-vs-reconnect with jitter, `FATAL_CLOSE_CODES`; `handleMessageCreate` (`discord.ts:368`) up to the seam.
- **Auth (REUSE):** `checkAuth` (`src/router/auth.ts:21`, fail-closed on empty allowlist) + snowflake-as-string parsing in `config.ts`.
- **Routing (REUSE):** `resolveChannelCwd(channelId)` (`discord.ts:136`) + `discord.channelDirectories` (`config.ts`). Resolved cwd becomes the **registry key**, not a one-shot spawn cwd.
- **Identity (REUSE):** `threadKey(source, thread)` / `workspaceKey(cwd)` (`session-key.ts:40/44`) → `sessionKey`, carried in `meta`, used as registry key + tmux name + shim env.
- **Egress (REUSE):** `sendMessage` (`discord.ts:183`, 2000-char chunk + `[react:]` strip), `sendReaction`, `discordApi` (`discord-api.ts`, 429/5xx backoff).
- **Serial lane (REUSE):** `enqueue` / `threadQueues` / `queueKey` (`runner.ts:92/142`) — one prompt at a time per session.
- **Durable inbox (NEW, single SQLite writer):** `inbox(seq, sessionKey, discord_msg_id UNIQUE, content, metaJson, state[pending|delivered|answered], createdAt)`. De-dup by `discord_msg_id`; replay pending+delivered-not-answered on shim reconnect.
- **Session supervisor (NEW):** lazy spawn tmux+claude+shim, app-level heartbeat, backoff+circuit-breaker, reap-by-tmux-kill.
- **IPC server (NEW):** `broker.sock` JSON-RPC.

### Shim responsibilities (NEW, ~150–200 LOC, holds NO Discord code)
- stdio MCP `Server` with `capabilities.experimental['claude/channel'] = {}`.
- One `reply` tool `{chat_id, text, reply_to?, files?}`. (`react` / `download_attachment` / permission = later phases.)
- Emits `notifications/claude/channel` when the broker pushes an inbound for its `sessionKey`.
- All tool bodies = RPC to broker over `broker.sock`. Authenticates with `HERMES_SESSION_KEY` (env, set at spawn) + one-time token.
- Self-defense watchdog (telegram pattern): exit if socket destroyed or `ppid` changed.

### Session / tmux layout
- One tmux session per `sessionKey`: `tmux new-session -d -s hermes-<sanitizedKey> -c <cwd> 'claude --dangerously-load-development-channels --channels <shim> [--dangerously-skip-permissions]'`.
- `:` in keys is illegal in tmux `-s`; sanitize `:`→`-` and store the reverse map in the registry (sha256 workspace keys are injective; raw thread ids get `projectSlug+shortHash`).
- Attach: `tmux attach -t hermes-<sanitizedKey>`. List: `tmux ls | grep hermes-`.

### Reuse vs. new (file refs)
| Concern | Source | Disposition |
|---|---|---|
| Gateway / reconnect / heartbeat | `src/commands/discord.ts` (`connectGateway`, `handleDispatch`, resume @ `:1147`, `stopGateway` @ `:1183`) | REUSE verbatim |
| Routing | `discord.ts:136` `resolveChannelCwd` + `config.ts` `channelDirectories` | REUSE verbatim |
| Session-key | `session-key.ts:40/44` | REUSE verbatim |
| Auth | `auth.ts:21` `checkAuth` | REUSE verbatim |
| Egress | `discord.ts:183` `sendMessage`, `:237` `extractReactionDirective`, `discord-api.ts` | REUSE verbatim |
| Serial queue | `runner.ts:92/142` | REUSE verbatim |
| Daemon skeleton | `start.ts` (`initConfig`, SIGTERM/SIGINT, 30s hot-reload, `initDiscord` token wiring) | REUSE, add broker bring-up |
| Atomic state writes | `runtime/daemon-registry.ts` | REUSE pattern for registry |
| **The metered core** | `runner.ts:730` `execClaude` (Bun.spawn `claude -p --resume`, `CLAUDE_TIMEOUT_MS`, auto-compact-on-124) | **REPLACE body** behind flag |
| Shim | — | **NEW** `src/shim/channel-shim.ts` |
| IPC | — | **NEW** `src/broker/ipc.ts` |
| Supervisor + registry + inbox | — | **NEW** `src/broker/sessions.ts`, `src/broker/inbox.ts` |

The single seam to re-point: `runner.ts` `run()` (`:1091`) → `enqueue(() => execClaude(...))`. Only `execClaude`'s **body** changes; `run` / `runUserMessage` / the `discord.ts:599` call site signatures stay byte-for-byte.

---

## 3. Message flow

### Inbound (Discord → broker → shim → session)
1. `MESSAGE_CREATE` (`discord.ts:1003`) → `handleMessageCreate` (`:368`): bot-filter, DM/guild detect, `knownThreads` thread recovery, trigger gate, `checkAuth`, attachment classify/download, prompt assembly — **all unchanged**.
2. At `discord.ts:599` the existing `runUserMessage("discord", prefixedPrompt, threadId, statusSink, "discord", resolveChannelCwd(channelId))` is **unchanged**. The behaviour change is **inside** `execClaude`.
3. `execClaude` computes `sessionKey = threadId ? threadKey("discord", threadId) : workspaceKey(cwd)`.
4. Broker `INSERT inbox(state='pending', discord_msg_id=…)` — UNIQUE makes gateway-resume re-delivery a no-op (exactly-once).
5. `ensureSession(sessionKey)` finds-or-spawns tmux+claude+shim.
6. Broker pushes over `broker.sock`:
   `{ type:"inbound", sessionKey, content, meta:{ chat_id: channelId, message_id, user, user_id, ts, thread_id, cwd } }` → mark row `delivered` on shim ack.
   **Meta is the routing carrier**: `chat_id`=Discord channel/thread id the reply must go back to; `thread_id`/`sessionKey` pick the lane. **No hyphen keys** (Claude Code drops hyphenated meta keys → use `chat_id`, not `chat-id`).
7. Shim emits `mcp.notification("notifications/claude/channel", {content, meta})` → Claude renders `<channel source="discord" chat_id="…" thread_id="…" user="…">` into its one long-running session.

### Outbound (reply tool → shim → broker → Discord)
1. Session's Claude calls shim `reply {chat_id, text, reply_to?, files?}`.
2. Shim `CallTool` handler sends `{type:"reply", sessionKey, chat_id, text, reply_to, files}` over `broker.sock` (no Discord call here).
3. Broker validates `chat_id` against the **same** auth/`channelDirectories` gate (a session can only reply to its own allowlisted channels), then `sendMessage(token, chat_id, text)` (2000-char chunk + `[react:]` honored for free).
4. **Decoupled egress (graft from A & C):** the shim→broker→`sendMessage` path is the **single** egress owner. `execClaude` returns an **ack-only** sentinel `RunResult{ stdout:"", exitCode:0 }` so `discord.ts:611` does **not** double-send. A long session may call `reply` many times (progress + final); each flushes immediately. The serial lane releases on inbound **delivered+acked**, not on awaiting one reply.
5. Broker marks the row `answered` when the `reply` tool returns; returns `"sent (id: X)"` / `"sent N parts"` / `"reply failed after N of M"` to the shim.
6. `edit_message` (later phase) deliberately does **not** push-notify — model sends a fresh `reply` on completion so the device pings.

### Files / reactions / optional permission relay
- **Files inbound:** broker downloads attachments to `~/.claude/channels/discord/inbox/`, passes `image_path`/`attachments` in `meta`; the session Reads the file. (Phase 2.)
- **Files outbound:** `reply.files` = absolute paths; broker runs `assertSendable()` (refuse anything inside state dir except `inbox/`, blocks `.env`/`access.json` exfil) before attaching to the first chunk. (Phase 2.)
- **Reactions:** `react` tool → broker `sendReaction`; `[react:emoji]` in reply text already supported by `sendMessage`. (Phase 2.)
- **Permission relay (Phase 3, optional):** shim declares `experimental['claude/channel/permission']={}` **only if** the broker actually gates the sender. `notifications/claude/channel/permission_request {request_id, tool_name, input_preview}` → broker DMs the operator allow/deny buttons (custom-id `perm:(allow|deny):<id>`) + text fallback `/^(y|yes|n|no) ([a-km-z]{5})$/i`, correlates `request_id`→`sessionKey`, replies `notifications/claude/channel/permission {request_id, behavior}` to the right shim. Until wired, launch with scoped `--dangerously-skip-permissions` **per cwd** (never global) so the lane doesn't hang on a tool prompt.

---

## 4. Stability plan (fixes the telegram-supercharged failures)

| telegram-supercharged failure | Hermes broker fix |
|---|---|
| **Restart-everything-as-control-flow** (context reset, >50%, 2h cap all kill the whole `claude`) | **Never kill a session to manage context.** Context handled in-session via `/compact`. Restarts reserved for genuine crashes only. |
| **No global error boundary** (zero `bot.catch`, `void bot.start()` → unhandledRejection crash-loops) | `process.on('unhandledRejection')` + `('uncaughtException')` **log-and-keep-serving** on both broker and shim. Reuse Hermes's already-hardened gateway resume/`FATAL_CLOSE_CODES`/heartbeat self-heal (`discord.ts:1147`, `:905`). |
| **Single-session SPOF** (any restart deafens every chat) | **Per-session failure isolation.** Recycling lane A never touches lane B. Broker (gateway+routing+inbox) is the only always-up component; each session independently restartable. |
| **Message loss across 3s restart race / lossy 5-min SQLite scan** | **Durable inbox + explicit `answered` state + ordered replay.** Persist before dispatch; mark delivered on shim ack, answered on reply return; replay pending+delivered-not-answered on reconnect. No timer race. |
| **Duplicate delivery on resume/replay** | **De-dup by `discord_msg_id UNIQUE`** — exactly-once per session. |
| **PPID=1 orphan holding SQLite WAL lock ("reacts but never replies")** | **Single broker-owned writer**; shims never open the DB (IPC only). Shims reaped by `tmux kill-session` (kills the process group). |
| **PTY/ANSI status-bar scraping to decide restarts (missed a 7-day-dormant session)** | **Never scrape terminal output.** App-level heartbeat: broker pings each shim every ~10s, recycle after 3 misses → catches wedged-but-alive sessions. |
| **getUpdates 409 (one consumer per token)** | Structurally impossible: **one broker owns the single Discord gateway**. New risk (broker death = all deaf) mitigated by broker-under-supervisor + durable inbox buffering. |
| **Restart storm** | Capped exponential backoff (1s→30s, reset after 60s stable) **+ circuit breaker** (hold lane "breaker-open" + alert operator after N rapid fails). |
| **Fragile launchd-only bootstrap** | Broker runs under a supervisor with a **health check** (and, once installed, under tmux), not KeepAlive alone. |

---

## 5. Phase-0 POC (single channel → single session round-trip, subscription-billed, tmux-attachable)

**Goal:** one Discord channel → one persistent interactive `claude` (subscription, not `claude -p`) → reply back to Discord, with `tmux attach` working. Smallest end-to-end slice.

**Step 0 — Prerequisite gate (BLOCKING; do this first):**
```bash
brew install tmux && tmux -V                       # tmux is NOT installed
claude --help 2>&1 | grep -i 'load-development-channels'   # currently EMPTY on 2.1.166
```
If the flag prints nothing, **stop** and surface to the user (see §7) — none of the approaches proceed without it. Pin the Claude version that exposes it. Also kill stray pollers: `pkill -f 'claude --channels'`, `pkill -f telegram-supercharged`.

**Step 1 — Smallest shim, no broker.** Write `src/shim/channel-shim.ts` (code in §6). `reply` tool appends args to `/tmp/hermes-replies.ndjson`; on boot it self-injects one hardcoded inbound notification. ~120 LOC.

**Step 2 — Manual single-session smoke test (zero Hermes change):**
```bash
tmux new-session -d -s hermes-poc -c /Users/benjaminwong/Projects/claude-hermes \
  'claude --dangerously-load-development-channels --channels /Users/benjaminwong/Projects/claude-hermes/src/shim/channel-shim.ts'
tmux attach -t hermes-poc
```
Confirm: (a) Claude renders the `<channel source="discord" …>` block; (b) when Claude calls `reply`, args land in `/tmp/hermes-replies.ndjson`. **This proves the 1-shim:1-session protocol + tmux PTY + dev-channels flag end to end with no Discord.**

**Step 3 — Broker IPC.** Add `src/broker/ipc.ts` (AF_UNIX at `$XDG_RUNTIME_DIR/hermes/broker.sock`, length-prefixed JSON, per-`sessionKey` pending-reply Promise map) and `src/broker/sessions.ts` (`ensureSession` spawns tmux as above, sets `HERMES_SESSION_KEY`). Move shim's inbound source from self-inject to a broker push frame; make `reply` an RPC. Add 10s ping / 5s pong; prove lane-drop on shim kill.

**Step 4 — Re-point `execClaude` behind a flag.** In `runner.ts:730`, guard on `getSettings().discord.useBrokerSessions` (default **false** = metered fallback). When true: `ensureSession(sessionKey)` → `ipc.sendInbound(...)` → `await ipc.awaitReply(sessionKey)` → return ack-only `RunResult`. Leave `run`/`runUserMessage`/`discord.ts:599` untouched.

**Step 5 — One real channel end-to-end.** Set `channelDirectories` for one test channel → a test cwd; flip `useBrokerSessions=true`; DM/mention the bot. Verify it routes to the tmux session, Claude replies via shim→broker→`sendMessage` back to Discord, and `tmux attach -t hermes-<key>` shows the live REPL. **Metered `claude -p` fully bypassed = subscription-billed.**

**Step 6 — Prove isolation + replay.** Map a 2nd channel→2nd cwd (2nd tmux session). `tmux kill-session` the first mid-task; confirm (a) the 2nd keeps replying, (b) the 1st auto-restarts with backoff, (c) its unanswered message replays exactly once.

---

## 6. Draft code

### 6a. Channel-server shim — `src/shim/channel-shim.ts`
Minimal, runnable, NO Discord code. POC variant writes to a file; the broker-socket path is marked TODO.

```ts
// src/shim/channel-shim.ts — thin per-session channel shim (1 shim : 1 Claude session).
// Run via: claude --dangerously-load-development-channels --channels <abs path to this file>
// Holds NO Discord connection. Only: claude/channel capability + reply tool + broker IPC.
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema, ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { connect, type Socket } from "node:net";
import { appendFileSync } from "node:fs";

const SESSION_KEY = process.env.HERMES_SESSION_KEY ?? "poc";
const SOCK = process.env.HERMES_BROKER_SOCK ?? ""; // empty in POC step 1-2
const REPLY_LOG = "/tmp/hermes-replies.ndjson";    // POC only; remove once broker wired

const server = new Server(
  { name: "hermes-channel-shim", version: "0.0.1" },
  { capabilities: {
      tools: {},
      // CRITICAL: registers the inbound channel listener. Add 'claude/channel/permission'
      // ONLY once the broker actually gates the sender (asserts authenticated replier).
      experimental: { "claude/channel": {} },
  } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [{
    name: "reply",
    description:
      "Send a message back to the originating Discord chat. Pass back the chat_id you " +
      "received in the <channel> frame. Call this when you have something to say — it is " +
      "the user-facing reply and the completion ping. You may call it multiple times.",
    inputSchema: {
      type: "object",
      required: ["chat_id", "text"],
      properties: {
        chat_id:  { type: "string", description: "Discord channel/thread id from meta.chat_id" },
        text:     { type: "string" },
        reply_to: { type: "string", description: "message_id to thread-reply to (optional)" },
        files:    { type: "array", items: { type: "string" }, description: "absolute paths (optional)" },
      },
    },
  }],
}));

server.setRequestHandler(CallToolRequestSchema, async (req) => {
  if (req.params.name !== "reply") throw new Error(`unknown tool ${req.params.name}`);
  const a = req.params.arguments as { chat_id: string; text: string; reply_to?: string; files?: string[] };
  const frame = { type: "reply", sessionKey: SESSION_KEY, ...a };
  if (sock) {
    const id = await rpc(frame);                 // broker performs the actual sendMessage
    return { content: [{ type: "text", text: `sent (id: ${id})` }] };
  }
  appendFileSync(REPLY_LOG, JSON.stringify(frame) + "\n"); // POC fallback
  return { content: [{ type: "text", text: "sent (poc)" }] };
});

// ---- broker IPC (length-prefixed JSON over AF_UNIX) ----
let sock: Socket | null = null;
const pending = new Map<string, (v: string) => void>();
function emitInbound(content: string, meta: Record<string, unknown>) {
  // Inbound -> the ONE parent Claude session. Fire-and-forget per channels-reference.
  server.notification({ method: "notifications/claude/channel", params: { content, meta } })
    .catch((e) => process.stderr.write(`inbound emit failed: ${e}\n`));
}
function connectBroker() {
  if (!SOCK) {                                   // POC step 1-2: self-inject one inbound
    setTimeout(() => emitInbound("hello from poc inbound",
      { chat_id: "POC_CHAT", message_id: "1", user: "tester", ts: new Date().toISOString() }), 1500);
    return;
  }
  sock = connect(SOCK, () => sock!.write(framed({ type: "hello", sessionKey: SESSION_KEY,
    token: process.env.HERMES_TOKEN ?? "" })));
  let buf = Buffer.alloc(0);
  sock.on("data", (d) => {
    buf = Buffer.concat([buf, d]);
    for (;;) {
      if (buf.length < 4) break;
      const len = buf.readUInt32BE(0);
      if (buf.length < 4 + len) break;
      const msg = JSON.parse(buf.subarray(4, 4 + len).toString());
      buf = buf.subarray(4 + len);
      if (msg.type === "inbound") emitInbound(msg.content, msg.meta);
      else if (msg.type === "reply_ack" && pending.has(msg.rpcId)) {
        pending.get(msg.rpcId)!(msg.id); pending.delete(msg.rpcId);
      } else if (msg.type === "ping") sock!.write(framed({ type: "pong", sessionKey: SESSION_KEY }));
    }
  });
  // Self-defense watchdog (telegram pattern): exit if broker link dies or we get reparented.
  const bootPpid = process.ppid;
  setInterval(() => {
    if (sock!.destroyed || process.ppid !== bootPpid) process.exit(0); // TODO: backoff-reconnect instead of hard exit
  }, 5000).unref();
}
function rpc(frame: object): Promise<string> {
  const rpcId = Math.random().toString(36).slice(2);
  return new Promise((res) => { pending.set(rpcId, res); sock!.write(framed({ ...frame, rpcId })); });
}
function framed(o: object): Buffer {
  const b = Buffer.from(JSON.stringify(o));
  const h = Buffer.alloc(4); h.writeUInt32BE(b.length, 0); return Buffer.concat([h, b]);
}

process.on("unhandledRejection", (e) => process.stderr.write(`unhandledRejection ${e}\n`)); // keep serving
process.on("uncaughtException",  (e) => process.stderr.write(`uncaughtException ${e}\n`));

await server.connect(new StdioServerTransport());
connectBroker();
// TODO: react/edit_message/download_attachment tools (Phase 2); permission relay (Phase 3).
```

### 6b. Broker hook — replace `execClaude`'s body in `src/runner.ts:730`
The exact seam. `run()` (`:1091`) and `runUserMessage()` (`:1113`) and the `discord.ts:599` call site stay unchanged; only `execClaude`'s body is gated.

```ts
// src/runner.ts — inside execClaude(name, prompt, threadId, sink, source, cwd), at the TOP,
// BEFORE the existing claudeArgv()/Bun.spawn `claude -p --resume` block (~:780-897).
import { threadKey, workspaceKey } from "./router/session-key";
import { ensureSession, sendInbound, awaitDelivered } from "./broker/ipc"; // NEW module

async function execClaude(
  name: string, prompt: string, threadId?: string,
  sink?: StatusSink, source: ThreadSource = "cli", cwd: string = process.cwd(),
): Promise<RunResult> {
  const settings = getSettings();
  if (settings.discord?.useBrokerSessions && source === "discord") {        // FLAG: default false = metered fallback
    const sessionKey = threadId ? threadKey(source, threadId) : workspaceKey(cwd);
    await ensureSession(sessionKey, cwd);                                   // lazy tmux+claude+shim spawn
    const meta = {
      chat_id: threadId ?? cwd,        // TODO: pass the real Discord channelId via run(); cwd is a placeholder
      message_id: "TODO",              // TODO: thread Discord message id through runUserMessage signature
      user: "TODO", ts: new Date().toISOString(), cwd,
    };
    // Persist BEFORE dispatch (de-dup by message_id UNIQUE) then push to the shim.
    await sendInbound(sessionKey, prompt, meta);
    // Lane releases on DELIVERED+ACKED, not on awaiting a reply (replies flush async via shim->broker->sendMessage).
    await awaitDelivered(sessionKey, meta.message_id);
    return { stdout: "", stderr: "", exitCode: 0 };   // ack-only: discord.ts:611 must send nothing on empty stdout
  }
  // ---- existing metered path UNCHANGED below (Bun.spawn `claude -p --resume`, :780-897) ----
  // ... leave intact as the rollback lever ...
}
```

> Note: to populate `meta.chat_id`/`message_id` properly, thread the Discord `channelId`+`messageId` from `handleMessageCreate` (`discord.ts:599`) into `run`/`runUserMessage`/`execClaude`. Cleanest is a small struct param — marked TODO; in POC step 5 you can pass `channelId` as `threadId` since `resolveChannelCwd` already keys on it. Also patch `discord.ts:611` to skip `sendMessage` when `result.stdout` is empty (ack sentinel) so egress isn't doubled.

`src/broker/ipc.ts` (NEW) exposes `ensureSession`, `sendInbound`, `awaitDelivered`, `onReply` (calls existing `sendMessage`), and the `inbox.db` writer + heartbeat — sketched in §2/§5; ~200 LOC, single SQLite writer, atomic write-then-rename for the registry (reuse `daemon-registry.ts` pattern).

---

## 7. Risks & caveats

- **BLOCKER — dev-channels flag absent on this machine.** `claude 2.1.166 --help` shows **no** `--dangerously-load-development-channels`. The whole custom-channel approach depends on it. **Action:** confirm which Claude Code version exposes it (research says v2.1.80+ research-preview; it is not surfaced in `--help` on 2.1.166), pin that version, re-run the step-0 grep. If it cannot be found, **none of A/B/C proceed** — surface to the user before building.
- **BLOCKER — tmux not installed.** `brew install tmux` first; the attachable-session design needs it for the PTY.
- **Research-preview protocol risk.** The `notifications/claude/channel` shape, the `experimental` capability keys, and the `reply` inputSchema can change between Claude Code releases and silently break the shim. Mitigation: `useBrokerSessions` flag keeps the metered `claude -p` path live as one-flip rollback; pin Claude version; re-validate the protocol on every upgrade.
- **Billing assumption (UNVERIFIED).** The premise is that a long-running **interactive** `claude` session bills against the Max/Pro subscription while `claude -p` per-message is metered/API-billed. This must be **confirmed on the Anthropic usage dashboard** with a real session before committing — it is the entire economic rationale.
- **1 server : 1 session is hard-coded.** The channel server is a stdio subprocess of one session and (in the official plugins) holds the gateway itself. You cannot point N sessions at one server. The broker+shim split is the only way; if any shim ever opens a Discord connection you get double-delivery.
- **Broker is the new SPOF.** Broker death = all lanes deaf at once (inverse of telegram's per-token 409). Mitigate with an external supervisor + health check + durable inbox buffering; gateway resume only covers Discord's short window, so long broker downtime still loses in-flight inbound.
- **`--dangerously-skip-permissions` is a security surface.** Scope it **per project cwd**, never global; prefer wiring the permission relay (Phase 3) over blanket-skip for Bash/Write in untrusted dirs.
- **Discord-specific re-implementation.** Hermes uses raw-WS + REST, not discord.js. Reactions / attachment download / permission buttons must be re-implemented against `discord-api.ts`, not copied from the official plugin.
- **Meta key hygiene.** Hyphenated meta keys are dropped by Claude Code — use `chat_id`, `message_id`, `thread_id` (underscores). Snowflakes stay **strings** end-to-end (exceed 2^53).
- **tmux name sanitization.** `:` is illegal in `tmux -s`; sanitize and keep an injective reverse map (sha256 workspace keys safe; raw thread ids need `projectSlug+hash`).
- **Cold-start latency.** First message to a new lane spawns tmux + boots `claude` + MCP handshake (seconds). Pre-warm hot channels or fire the existing typing indicator during spawn.

---

## 8. Open decisions for the user

1. **Dev-channels flag / Claude version** — accept pinning to a specific Claude Code build that exposes `--dangerously-load-development-channels`? (POC cannot start otherwise.) Or do you want me to first dig up exactly which version surfaces it?
2. **Billing verification** — OK to run a short live interactive session and check the usage dashboard to confirm it's subscription-billed before any build work?
3. **Install tmux** — confirm `brew install tmux` on this machine, or do you prefer a different process supervisor (and forgo live attach)?
4. **Session granularity** — one persistent session **per Discord thread** (`threadKey`), or one **per project cwd / channel** (`workspaceKey`, shared across that channel)? Affects how many concurrent `claude` sessions run (and subscription concurrency limits).
5. **Permission model for POC** — scoped `--dangerously-skip-permissions` per cwd to start (fastest), or block POC on the Phase-3 permission relay (safer, slower)?
6. **Rollout** — ship Approach B with `useBrokerSessions=false` default and flip per-channel, or cut over all mapped channels at once?
7. **Telegram** — port the same broker+shim split to Telegram in parallel (your unstable setup), or get Discord stable first then reuse the broker for Telegram?
8. **Concurrency cap** — set a max number of live sessions (and an idle-reap timeout) to bound subscription usage? If so, what limit?