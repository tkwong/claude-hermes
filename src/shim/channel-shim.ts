#!/usr/bin/env bun
/**
 * Hermes per-session channel shim.
 *
 * One shim : one interactive Claude Code session (the channel protocol is a
 * stdio subprocess of exactly one session). Holds NO Discord connection — it
 * only carries the `claude/channel` capability + a `reply` tool.
 *
 * Two modes, selected by the HERMES_BROKER_SOCK env var:
 *
 *  - BROKER MODE (HERMES_BROKER_SOCK set): connect to the broker's AF_UNIX
 *    socket, send {type:'hello',sessionKey,token,shimPid}; on {type:'inbound'}
 *    emit notifications/claude/channel and ack {type:'inbound_ack',seq}; the
 *    reply tool RPCs {type:'reply',...,rpcId} and awaits {type:'reply_ack'};
 *    answer {type:'ping'} with {type:'pong'}. Reconnects with capped backoff on
 *    socket drop — it NEVER hard-exits (per-session isolation + log-and-keep-
 *    serving). The shim NEVER opens the inbox DB; the broker is the sole writer.
 *
 *  - POC MODE (HERMES_BROKER_SOCK unset): self-inject one inbound on boot and
 *    append replies to a log file, to prove the inbound→session→reply round-trip
 *    end to end with no broker.
 *
 * Wire codec (BROKER MODE) is duplicated inline and MUST stay byte-identical to
 * src/broker/ipc.ts frame()/createFrameDecoder(): a 4-byte big-endian uint32
 * length prefix + UTF-8 JSON body. The shim deliberately does NOT import from
 * src/broker/* — it runs in the claude child process with a minimal surface.
 *
 * Launch (POC, via .mcp.json entry "hermes-shim"):
 *   claude --dangerously-load-development-channels server:hermes-shim
 * or via the masquerade (overwrite the official discord plugin's server.ts):
 *   claude --channels plugin:discord@claude-plugins-official
 */
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import { appendFileSync } from "node:fs";
import { connect, type Socket } from "node:net";

const SESSION_KEY = process.env.HERMES_SESSION_KEY ?? "poc";
const TOKEN = process.env.HERMES_TOKEN ?? "";
const SOCK = process.env.HERMES_BROKER_SOCK ?? ""; // empty => POC self-inject mode
const BROKER_MODE = SOCK !== "";
const REPLY_LOG = process.env.HERMES_REPLY_LOG ?? "/tmp/hermes-replies.ndjson"; // POC only

const server = new Server(
  { name: "hermes-shim", version: "0.0.1" },
  {
    capabilities: {
      tools: {},
      // CRITICAL: registers the inbound channel listener.
      experimental: { "claude/channel": {} },
    },
    instructions:
      'Discord messages arrive as <channel source="discord" chat_id="..."> tags. ' +
      "When one arrives, call the `reply` tool with the chat_id from the tag and your reply " +
      "text — that is the user-facing message sent back to Discord.",
  },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: "reply",
      description:
        "Send a message back to the originating Discord chat. Pass back the chat_id you " +
        "received in the <channel> frame. You may call it multiple times (progress + final). " +
        "To REACT to a message with an emoji, include `[react:👍]` anywhere in `text` AND set " +
        "`reply_to` to that message's id (the message_id attribute in the <channel> frame) — the " +
        "reaction lands on that message; remaining text is sent as a normal message (omit text " +
        "to react only). To QUOTE-REPLY a specific message, set `reply_to` to its message_id. " +
        "To send file(s)/image(s) back, pass absolute paths in `files`.",
      inputSchema: {
        type: "object",
        required: ["chat_id", "text"],
        properties: {
          chat_id: { type: "string", description: "Discord channel/thread id from meta.chat_id" },
          text: { type: "string" },
          reply_to: {
            type: "string",
            description:
              "message_id to quote-reply to / react on (use the message_id from the <channel> frame)",
          },
          files: {
            type: "array",
            items: { type: "string" },
            description: "absolute file paths to upload as attachments (optional)",
          },
        },
      },
    },
    {
      name: "create_thread",
      description:
        "Open a NEW Discord thread under the current channel and get its id back. Use this " +
        "to spin up a separate session for a sub-task: the new thread becomes its own " +
        "Claude session (threads => session), so the user can continue that sub-task there " +
        "in isolation. Pass the parent channel's chat_id from the <channel> frame (must be a " +
        "top-level channel — Discord has no threads-in-threads). After it returns thread_id, " +
        "call `reply` with chat_id=<thread_id> to post into the new thread.",
      inputSchema: {
        type: "object",
        required: ["chat_id", "name"],
        properties: {
          chat_id: {
            type: "string",
            description: "Parent channel id from meta.chat_id (a channel, not a thread)",
          },
          name: { type: "string", description: "Thread name (<= 100 chars)" },
          seed_text: {
            type: "string",
            description: "Optional first message to post into the new thread",
          },
        },
      },
    },
    {
      name: "progress",
      description:
        "Show a LIVE status line for a slow turn so the user knows what you're doing " +
        "(there is otherwise only a 'typing…' dot). The first call posts a status message; " +
        "each later call EDITS it in place (no spam). Call it at the START of any turn that " +
        "will take more than a few seconds — e.g. progress('🔍 睇緊 memory…') — and update it " +
        "as you move to a new step. The status message is auto-removed when you send your final " +
        "`reply`. This is for short status notes, NOT your actual answer (use `reply` for that).",
      inputSchema: {
        type: "object",
        required: ["chat_id", "text"],
        properties: {
          chat_id: { type: "string", description: "Discord channel/thread id from meta.chat_id" },
          text: { type: "string", description: "Short status line (e.g. '⚙️ 揾緊 X…')" },
        },
      },
    },
  ],
}));

server.setRequestHandler(CallToolRequestSchema, async (req) => {
  if (req.params.name === "progress") {
    const a = req.params.arguments as { chat_id: string; text: string };
    if (!BROKER_MODE) {
      return { content: [{ type: "text", text: "progress noted (poc)" }] };
    }
    const ack = await rpcProgress({ chat_id: a.chat_id, text: a.text });
    if (!ack.ok) {
      return { isError: true, content: [{ type: "text", text: ack.error ?? "progress failed" }] };
    }
    return { content: [{ type: "text", text: "status updated" }] };
  }
  if (req.params.name === "create_thread") {
    const a = req.params.arguments as { chat_id: string; name: string; seed_text?: string };
    if (!BROKER_MODE) {
      return { isError: true, content: [{ type: "text", text: "create_thread requires broker mode" }] };
    }
    const ack = await rpcCreateThread({
      parent_chat_id: a.chat_id,
      name: a.name,
      seed_text: a.seed_text,
    });
    if (!ack.ok || !ack.threadId) {
      return { isError: true, content: [{ type: "text", text: ack.error ?? "create_thread failed" }] };
    }
    return {
      content: [
        {
          type: "text",
          text: `thread created: ${ack.threadId} — reply with chat_id=${ack.threadId} to post in it`,
        },
      ],
    };
  }
  if (req.params.name !== "reply") throw new Error(`unknown tool ${req.params.name}`);
  const a = req.params.arguments as {
    chat_id: string;
    text: string;
    reply_to?: string;
    files?: string[];
  };
  if (BROKER_MODE) {
    // Broker performs the real Discord sendMessage. Attach the inbound seq this
    // reply answers — but ONLY on the first reply after an inbound, and mark it
    // `final` so the broker closes (marks answered) the row exactly once. We
    // CONSUME the tracked seq here so any subsequent reply for the same chat
    // (a follow-up / progress message with no new inbound) carries NO seq and
    // NOT final — it must never re-mark or prematurely-answer the row. Keeping
    // the row open until this point means a mid-turn shim reconnect still
    // replays the prompt (durability §4); answering only on the seq-bearing
    // reply means a progress-only reply can't strand the final answer.
    const seq = consumeInboundSeqFor(a.chat_id);
    const ack = await rpcReply({
      chat_id: a.chat_id,
      text: a.text,
      reply_to: a.reply_to,
      files: a.files,
      seq,
      final: seq !== undefined,
    });
    if (!ack.ok) {
      return {
        isError: true,
        content: [{ type: "text", text: ack.error ?? "reply failed" }],
      };
    }
    return { content: [{ type: "text", text: ack.id }] };
  }
  // POC sink: append to a log file.
  appendFileSync(
    REPLY_LOG,
    `${JSON.stringify({
      type: "reply",
      sessionKey: SESSION_KEY,
      chat_id: a.chat_id,
      text: a.text,
      reply_to: a.reply_to,
      files: a.files,
      at: new Date().toISOString(),
    })}\n`,
  );
  return { content: [{ type: "text", text: "sent (poc)" }] };
});

// Boot-race guard. Claude silently DROPS channel notifications that arrive before
// it has finished loading the channel ("if the session hasn't loaded your server
// as a channel, events are dropped silently" — channels-reference). A broker that
// replays a PENDING inbound the instant the shim sends hello races claude's boot
// (it is still spawning its other MCP servers), so that first message vanishes.
// The official chat channels never hit this: their messages only arrive after the
// session is already warm. We bridge the gap here — buffer emissions until Claude
// completes the MCP `initialize` handshake (server.oninitialized) plus a short
// grace for its turn loop to be ready, then flush in order. A fallback timer flips
// us ready even if oninitialized never fires, so an inbound can never strand.
const CHANNEL_READY_GRACE_MS = Number(process.env.HERMES_CHANNEL_READY_GRACE_MS ?? "4000");
const CHANNEL_READY_FALLBACK_MS = Number(process.env.HERMES_CHANNEL_READY_FALLBACK_MS ?? "20000");
let channelReady = false;
const pendingEmissions: Array<{ content: string; meta: Record<string, unknown> }> = [];

function doEmit(content: string, meta: Record<string, unknown>): void {
  // Inbound -> the ONE parent Claude session. Fire-and-forget per channels-reference.
  server
    .notification({ method: "notifications/claude/channel", params: { content, meta } })
    .catch((e) => process.stderr.write(`inbound emit failed: ${e}\n`));
}

function markChannelReady(why: string): void {
  if (channelReady) return;
  channelReady = true;
  if (pendingEmissions.length > 0) {
    process.stderr.write(
      `[shim ${SESSION_KEY}] channel ready (${why}); flushing ${pendingEmissions.length} buffered inbound(s)\n`,
    );
  }
  const drained = pendingEmissions.splice(0, pendingEmissions.length);
  for (const e of drained) doEmit(e.content, e.meta);
}

// Claude finished the MCP initialize handshake → it has registered the channel
// listener. Give its turn loop a short grace, then start delivering.
server.oninitialized = () => {
  setTimeout(() => markChannelReady("initialized+grace"), CHANNEL_READY_GRACE_MS);
};
// Defensive: never strand a buffered inbound if oninitialized never fires.
setTimeout(() => markChannelReady("fallback"), CHANNEL_READY_FALLBACK_MS).unref?.();

function emitInbound(content: string, meta: Record<string, unknown>): void {
  if (!channelReady) {
    pendingEmissions.push({ content, meta });
    return;
  }
  doEmit(content, meta);
}

// ---- broker IPC (length-prefixed JSON over AF_UNIX) ----
// Codec MUST stay byte-identical to src/broker/ipc.ts: 4-byte big-endian uint32
// length prefix + UTF-8 JSON body. Duplicated inline (no src/broker/* import).
// MAX_FRAME_BYTES must match ipc.ts so the two stay wire-compatible.
const MAX_FRAME_BYTES = 16 * 1024 * 1024;

function frame(obj: object): Buffer {
  const body = Buffer.from(JSON.stringify(obj), "utf8");
  const head = Buffer.alloc(4);
  head.writeUInt32BE(body.length, 0);
  return Buffer.concat([head, body]);
}

function createFrameDecoder(
  onFrame: (msg: Record<string, unknown>) => void,
  onOverflow?: (len: number) => void,
): (chunk: Buffer) => void {
  let buf = Buffer.alloc(0);
  let poisoned = false;
  return (chunk: Buffer) => {
    if (poisoned) return;
    buf = Buffer.concat([buf, chunk]);
    for (;;) {
      if (buf.length < 4) break;
      const len = buf.readUInt32BE(0);
      if (len > MAX_FRAME_BYTES) {
        poisoned = true;
        buf = Buffer.alloc(0);
        process.stderr.write(`frame length ${len} exceeds MAX_FRAME_BYTES; tearing down\n`);
        onOverflow?.(len);
        return;
      }
      if (buf.length < 4 + len) break;
      const body = buf.subarray(4, 4 + len);
      buf = buf.subarray(4 + len);
      try {
        onFrame(JSON.parse(body.toString("utf8")) as Record<string, unknown>);
      } catch (e) {
        process.stderr.write(`frame decode failed: ${e}\n`);
      }
    }
  };
}

let sock: Socket | null = null;
const pending = new Map<string, (ack: ReplyAck) => void>();
// Track the most recent inbound seq per chat_id so the reply tool can correlate.
const lastSeqByChat = new Map<string, number>();
// Highest inbound seq already EMITTED to the parent Claude session. Seqs are
// monotonic per broker and replay is ordered, so a simple high-water mark
// dedups broker->session delivery: a row pushed live then replayed (or replayed
// twice across reconnects) is emitted to the session exactly once. The DB-layer
// UNIQUE(discord_msg_id) only dedups the Discord->broker leg — this closes the
// session-emission leg (the §4 "duplicate delivery on resume/replay" failure).
let maxEmittedSeq = 0;

interface ReplyAck {
  ok: boolean;
  id: string;
  error?: string;
}

interface CreateThreadAck {
  ok: boolean;
  threadId?: string;
  parentId?: string;
  error?: string;
}
const pendingCreateThread = new Map<string, (ack: CreateThreadAck) => void>();

interface ProgressAck {
  ok: boolean;
  error?: string;
}
const pendingProgress = new Map<string, (ack: ProgressAck) => void>();

/**
 * Return AND clear the tracked inbound seq for a chat. The first reply after an
 * inbound consumes it (becomes the final, answered reply); later replies with no
 * intervening inbound get undefined → no seq → broker leaves the row as-is.
 */
function consumeInboundSeqFor(chatId: string): number | undefined {
  const seq = lastSeqByChat.get(chatId);
  if (seq !== undefined) lastSeqByChat.delete(chatId);
  return seq;
}

function writeFrame(obj: object): void {
  const s = sock;
  if (!s || s.destroyed) return;
  s.write(frame(obj));
}

function rpcReply(args: {
  chat_id: string;
  text: string;
  reply_to?: string;
  files?: string[];
  seq?: number;
  final?: boolean;
}): Promise<ReplyAck> {
  const rpcId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  return new Promise<ReplyAck>((resolve) => {
    pending.set(rpcId, resolve);
    if (!sock || sock.destroyed) {
      pending.delete(rpcId);
      resolve({ ok: false, id: "", error: "broker socket not connected" });
      return;
    }
    writeFrame({
      type: "reply",
      sessionKey: SESSION_KEY,
      rpcId,
      chat_id: args.chat_id,
      text: args.text,
      reply_to: args.reply_to,
      files: args.files,
      seq: args.seq,
      final: args.final,
    });
  });
}

function rpcCreateThread(args: {
  parent_chat_id: string;
  name: string;
  seed_text?: string;
}): Promise<CreateThreadAck> {
  const rpcId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  return new Promise<CreateThreadAck>((resolve) => {
    pendingCreateThread.set(rpcId, resolve);
    if (!sock || sock.destroyed) {
      pendingCreateThread.delete(rpcId);
      resolve({ ok: false, error: "broker socket not connected" });
      return;
    }
    writeFrame({
      type: "create_thread",
      sessionKey: SESSION_KEY,
      rpcId,
      parent_chat_id: args.parent_chat_id,
      name: args.name,
      seed_text: args.seed_text,
    });
  });
}

function rpcProgress(args: { chat_id: string; text: string }): Promise<ProgressAck> {
  const rpcId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  return new Promise<ProgressAck>((resolve) => {
    pendingProgress.set(rpcId, resolve);
    if (!sock || sock.destroyed) {
      pendingProgress.delete(rpcId);
      resolve({ ok: false, error: "broker socket not connected" });
      return;
    }
    writeFrame({ type: "progress", sessionKey: SESSION_KEY, rpcId, chat_id: args.chat_id, text: args.text });
  });
}

function handleFrame(msg: Record<string, unknown>): void {
  switch (msg.type) {
    case "hello_ack": {
      const replayCount = typeof msg.replayCount === "number" ? msg.replayCount : 0;
      process.stderr.write(`[shim ${SESSION_KEY}] hello_ack (replayCount=${replayCount})\n`);
      break;
    }
    case "inbound": {
      const seq = typeof msg.seq === "number" ? msg.seq : undefined;
      const content = typeof msg.content === "string" ? msg.content : "";
      const meta = (msg.meta && typeof msg.meta === "object" ? msg.meta : {}) as Record<
        string,
        unknown
      >;
      const chatId = typeof meta.chat_id === "string" ? meta.chat_id : undefined;
      // Dedup broker->session emission by high-water mark: a row delivered live
      // and then replayed (e.g. shim dropped before acking, reconnected) must
      // NOT be shown to Claude twice. We still ack so the broker can mark it
      // delivered + resolve awaitDelivered (the ack is idempotent).
      const alreadyEmitted = seq !== undefined && seq <= maxEmittedSeq;
      if (!alreadyEmitted) {
        if (chatId !== undefined && seq !== undefined) lastSeqByChat.set(chatId, seq);
        if (seq !== undefined) maxEmittedSeq = seq;
        emitInbound(content, meta);
      }
      // Ack delivery so the broker marks the row delivered and resolves awaitDelivered.
      if (seq !== undefined) {
        writeFrame({ type: "inbound_ack", sessionKey: SESSION_KEY, seq });
      }
      break;
    }
    case "reply_ack": {
      const rpcId = typeof msg.rpcId === "string" ? msg.rpcId : "";
      const resolve = pending.get(rpcId);
      if (resolve) {
        pending.delete(rpcId);
        resolve({
          ok: msg.ok === true,
          id: typeof msg.id === "string" ? msg.id : "",
          error: typeof msg.error === "string" ? msg.error : undefined,
        });
      }
      break;
    }
    case "create_thread_ack": {
      const rpcId = typeof msg.rpcId === "string" ? msg.rpcId : "";
      const resolve = pendingCreateThread.get(rpcId);
      if (resolve) {
        pendingCreateThread.delete(rpcId);
        resolve({
          ok: msg.ok === true,
          threadId: typeof msg.threadId === "string" ? msg.threadId : undefined,
          parentId: typeof msg.parentId === "string" ? msg.parentId : undefined,
          error: typeof msg.error === "string" ? msg.error : undefined,
        });
      }
      break;
    }
    case "progress_ack": {
      const rpcId = typeof msg.rpcId === "string" ? msg.rpcId : "";
      const resolve = pendingProgress.get(rpcId);
      if (resolve) {
        pendingProgress.delete(rpcId);
        resolve({
          ok: msg.ok === true,
          error: typeof msg.error === "string" ? msg.error : undefined,
        });
      }
      break;
    }
    case "ping": {
      const ts = typeof msg.ts === "number" ? msg.ts : Date.now();
      writeFrame({ type: "pong", sessionKey: SESSION_KEY, ts });
      break;
    }
    default:
      process.stderr.write(`[shim ${SESSION_KEY}] unknown frame type: ${String(msg.type)}\n`);
  }
}

// Capped exponential backoff reconnect — NEVER hard-exit (per-session isolation).
const BACKOFF_BASE_MS = 500;
const BACKOFF_MAX_MS = 10_000;
let backoffMs = BACKOFF_BASE_MS;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

function scheduleReconnect(): void {
  if (reconnectTimer) return;
  const delay = backoffMs;
  backoffMs = Math.min(backoffMs * 2, BACKOFF_MAX_MS);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectBroker();
  }, delay);
  reconnectTimer.unref?.();
}

function failAllPending(reason: string): void {
  for (const [rpcId, resolve] of pending) {
    pending.delete(rpcId);
    resolve({ ok: false, id: "", error: reason });
  }
  for (const [rpcId, resolve] of pendingCreateThread) {
    pendingCreateThread.delete(rpcId);
    resolve({ ok: false, error: reason });
  }
  for (const [rpcId, resolve] of pendingProgress) {
    pendingProgress.delete(rpcId);
    resolve({ ok: false, error: reason });
  }
}

function connectBroker(): void {
  const decode = createFrameDecoder(handleFrame, () => {
    // Oversized frame from the broker: destroy + let 'close' schedule reconnect.
    try {
      sock?.destroy();
    } catch {
      // already gone
    }
  });
  const s = connect(SOCK);
  sock = s;

  s.on("connect", () => {
    backoffMs = BACKOFF_BASE_MS; // reset on a successful connect
    process.stderr.write(`[shim ${SESSION_KEY}] connected to broker ${SOCK}\n`);
    writeFrame({ type: "hello", sessionKey: SESSION_KEY, token: TOKEN, shimPid: process.pid });
  });

  s.on("data", (d: Buffer) => decode(d));

  s.on("error", (e: Error) => {
    process.stderr.write(`[shim ${SESSION_KEY}] socket error: ${e.message}\n`);
    // 'close' fires after 'error'; reconnect is scheduled there.
  });

  s.on("close", () => {
    process.stderr.write(`[shim ${SESSION_KEY}] socket closed; scheduling reconnect\n`);
    sock = null;
    failAllPending("broker socket closed");
    scheduleReconnect();
  });
}

process.on("unhandledRejection", (e) => process.stderr.write(`unhandledRejection ${e}\n`));
process.on("uncaughtException", (e) => process.stderr.write(`uncaughtException ${e}\n`));

await server.connect(new StdioServerTransport());

if (BROKER_MODE) {
  connectBroker();
} else {
  // POC: prove the round-trip by pushing one inbound a moment after boot.
  setTimeout(() => {
    emitInbound(
      "PING from Hermes POC smoke test. Reply with the reply tool now: chat_id=POC_CHAT, text=pong.",
      { chat_id: "POC_CHAT", message_id: "1", user: "smoke-test" },
    );
  }, 2500);
}
