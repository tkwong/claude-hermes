/**
 * Broker-side AF_UNIX JSON-RPC server + the length-prefixed framing codec that
 * the per-session channel-shim speaks (4-byte big-endian uint32 length prefix +
 * UTF-8 JSON body — byte-identical to `src/shim/channel-shim.ts`'s inline copy).
 *
 * Responsibilities (broker side of the wire contract in docs/broker-design.md):
 *   - Listen on `brokerSockPath()`; accept shim connections.
 *   - On `{type:'hello'}`: verify the one-time per-session token, bind the
 *     socket to its `sessionKey`, then replay pending + delivered-not-answered
 *     inbox rows in seq order (durable, ordered redelivery on reconnect).
 *   - Route `{type:'reply'}` to `onReply` (which performs the real Discord
 *     `sendMessage`) and answer `{type:'reply_ack'}`; mark the answered row.
 *   - On `{type:'inbound_ack'}`: `markDelivered(seq)` and resolve the matching
 *     `awaitDelivered` promise.
 *   - App-level heartbeat: ping every PING_INTERVAL_MS; recycle the lane after
 *     MAX_MISSED_PONGS consecutive misses (catches wedged-but-alive sessions
 *     without ever scraping PTY output — stability §4).
 *
 * This module owns the inbox DB handle (the single broker writer — stability
 * §4). Shims NEVER open the DB; they only see frames over this socket.
 *
 * Bring-up order in start.ts (breaks the runner<->broker import cycle):
 *   openInbox -> createSessionSupervisor -> startBrokerIpc({inboxDb,
 *   ensureSession, recycleSession, verifyToken, onReply}) -> setBrokerIpc(ipc).
 * This module must NOT import sessions.ts at module scope beyond types, and must
 * NOT import discord.ts (egress is injected via the onReply callback).
 */

import { createServer, type Server, type Socket } from "node:net";
import { createHash } from "node:crypto";
import { mkdirSync } from "node:fs";
import { unlink } from "node:fs/promises";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import type { Database } from "bun:sqlite";
import type { InboxMeta, InboxRow } from "./inbox";
import { getRow, getRowByMsgId, markAnswered, markDelivered, recordInbound, replayFor } from "./inbox";

/** Broker pings each shim on this cadence (epoch-ms `ts` echoed by `pong`). */
export const PING_INTERVAL_MS = 10_000;
/** Consecutive missed pongs (>= ~30s wedged) before the lane is recycled. */
export const MAX_MISSED_PONGS = 3;
/**
 * Upper bound on a single frame body. A buggy/hostile shim that writes a huge
 * length prefix must NOT make the broker buffer unboundedly — past this we
 * destroy the socket. MUST match the shim's inline copy to stay wire-compatible.
 */
export const MAX_FRAME_BYTES = 16 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Wire codec — MUST stay byte-identical to the shim's inline copy.
// ---------------------------------------------------------------------------

/** Encode an object as a length-prefixed frame: uint32be length + UTF-8 JSON. */
export function frame(obj: object): Buffer {
  const body = Buffer.from(JSON.stringify(obj), "utf8");
  const header = Buffer.alloc(4);
  header.writeUInt32BE(body.length, 0);
  return Buffer.concat([header, body]);
}

/**
 * Build a stateful decoder that buffers partial reads and invokes `onFrame`
 * once per complete frame. The returned function is fed raw socket chunks.
 * A malformed JSON body is skipped (logged) rather than wedging the stream.
 *
 * `onOverflow` (optional) fires when a length prefix exceeds MAX_FRAME_BYTES —
 * the caller must tear down the socket (a frame that large is a bug or attack;
 * continuing would buffer unboundedly). When it fires the decoder stops
 * processing the poisoned buffer.
 */
export function createFrameDecoder(
  onFrame: (msg: Record<string, unknown>) => void,
  onOverflow?: (len: number) => void,
): (chunk: Buffer) => void {
  let buf: Buffer = Buffer.alloc(0);
  let poisoned = false;
  return (chunk: Buffer): void => {
    if (poisoned) return;
    buf = buf.length === 0 ? chunk : Buffer.concat([buf, chunk]);
    for (;;) {
      if (buf.length < 4) break;
      const len = buf.readUInt32BE(0);
      if (len > MAX_FRAME_BYTES) {
        poisoned = true;
        buf = Buffer.alloc(0);
        logErr(`frame length ${len} exceeds MAX_FRAME_BYTES (${MAX_FRAME_BYTES}); tearing down`);
        onOverflow?.(len);
        return;
      }
      if (buf.length < 4 + len) break;
      const body = buf.subarray(4, 4 + len);
      buf = buf.subarray(4 + len);
      let msg: unknown;
      try {
        msg = JSON.parse(body.toString("utf8"));
      } catch (e) {
        logErr(`frame decode failed: ${String(e)}`);
        continue;
      }
      if (msg && typeof msg === "object") {
        onFrame(msg as Record<string, unknown>);
      }
    }
  };
}

// ---------------------------------------------------------------------------
// Socket path resolution (see contract `socketPath`).
// ---------------------------------------------------------------------------

/**
 * `$XDG_RUNTIME_DIR/hermes/broker.sock` when set (Linux); otherwise
 * `os.tmpdir()/hermes-<sha256(daemonCwd).slice(0,12)>/broker.sock` (macOS/dev).
 * The hashed dir keeps the path under the 104-byte AF_UNIX `sun_path` limit on
 * darwin and avoids collisions between daemons in different cwds. The shim reads
 * the resolved value verbatim from `HERMES_BROKER_SOCK` — it never recomputes.
 */
export function brokerSockPath(): string {
  const xdg = process.env.XDG_RUNTIME_DIR;
  if (xdg && xdg.length > 0) {
    return join(xdg, "hermes", "broker.sock");
  }
  const tag = createHash("sha256").update(process.cwd()).digest("hex").slice(0, 12);
  return join(tmpdir(), `hermes-${tag}`, "broker.sock");
}

// ---------------------------------------------------------------------------
// Public types.
// ---------------------------------------------------------------------------

export interface BrokerIpcOptions {
  sockPath?: string;
  /** The single broker-owned inbox DB handle (this module is the sole writer). */
  inboxDb: Database;
  /** Performs the real Discord egress (broker passes Hermes's sendMessage). */
  onReply: (r: {
    sessionKey: string;
    chatId: string;
    text: string;
    replyTo?: string;
    files?: string[];
    /** True on the turn-final reply (the one that answers the inbox row). Lets the
     *  daemon stop the "typing…" indicator only when the turn is actually done. */
    final?: boolean;
  }) => Promise<void>;
  /**
   * Fired when an inbound is delivered to a live lane (the lane is now WORKING on
   * it). The daemon uses this to start a platform "typing…"/activity indicator so
   * a slow (high-effort) turn doesn't look dead. Stop signal = the matching final
   * `onReply`. Optional + best-effort (never blocks delivery).
   */
  onInbound?: (r: { sessionKey: string; chatId: string }) => void;
  /**
   * Live progress/status update for the shim's `progress` tool — the daemon posts
   * or edits a single status message for the chat (cleared by the final reply).
   * Optional; when absent the shim's tool acks an error.
   */
  onProgress?: (r: { sessionKey: string; chatId: string; text: string }) => Promise<void>;
  /**
   * Creates a real platform thread under a parent chat for the shim's
   * `create_thread` tool, returning the new thread id (Claude then replies into
   * it). Optional — when absent the shim's tool acks an error. The implementation
   * enforces the allowlist + per-session ownership (broker never re-checks).
   */
  onCreateThread?: (r: {
    sessionKey: string;
    parentChatId: string;
    name: string;
    seedText?: string;
  }) => Promise<{ threadId: string; parentId: string }>;
  /** Lazily spawns the tmux+claude+shim lane for a sessionKey (supervisor). */
  ensureSession: (sessionKey: string, cwd: string) => Promise<void>;
  /** Kills + respawns ONE lane (per-session isolation; never touches others). */
  recycleSession: (sessionKey: string, reason: string) => Promise<void>;
  /** Verifies the one-time per-session token minted by the supervisor. */
  verifyToken: (sessionKey: string, token: string) => boolean;
  /**
   * Notifies the supervisor of a live pong so its durable registry's liveness
   * view (and the `spawning`->`live` promotion) stays in sync with the IPC's
   * per-connection view. Optional so unit tests can omit it.
   */
  markPong?: (sessionKey: string) => void;
  /**
   * Reports the supervisor's current lane state for a sessionKey, so sendInbound
   * can short-circuit a known-down lane (breaker-open / dead) instead of letting
   * awaitDelivered hang the full timeout on a black-holed message. Optional.
   */
  laneState?: (sessionKey: string) => string | undefined;
  pingIntervalMs?: number;
  maxMissedPongs?: number;
}

export interface BrokerIpc {
  /** Persist (de-dup by discord_msg_id), ensure the lane, push inbound. */
  sendInbound(
    sessionKey: string,
    content: string,
    meta: InboxMeta
  ): Promise<{ seq: number; inserted: boolean }>;
  /** Resolve once the shim acks delivery of `discordMsgId` (or times out). */
  awaitDelivered(sessionKey: string, discordMsgId: string, timeoutMs?: number): Promise<void>;
  close(): Promise<void>;
  readonly sockPath: string;
}

// ---------------------------------------------------------------------------
// Internal per-connection / per-session bookkeeping.
// ---------------------------------------------------------------------------

interface Conn {
  socket: Socket;
  sessionKey: string | null; // null until hello (token-verified) binds it
  shimPid: number | null;
  missedPongs: number;
  lastPongAt: number;
  alive: boolean;
  /** True while the hello-driven ordered replay loop is still streaming. */
  replaying: boolean;
  /** Seqs already written to THIS conn — idempotent per-seq delivery guard. */
  sentSeqs: Set<number>;
  /** Rows freshly inserted while `replaying` — drained in seq order after. */
  queuedDuringReplay: InboxRow[];
}

interface DeliveryWaiter {
  resolve: () => void;
  reject: (e: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

const DEFAULT_DELIVER_TIMEOUT_MS = 30_000;

function logErr(msg: string): void {
  process.stderr.write(`[${new Date().toLocaleTimeString()}] [broker-ipc] ${msg}\n`);
}

function asString(v: unknown): string | null {
  return typeof v === "string" ? v : null;
}

function asNumber(v: unknown): number | null {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

// ---------------------------------------------------------------------------
// startBrokerIpc — the server.
// ---------------------------------------------------------------------------

export async function startBrokerIpc(opts: BrokerIpcOptions): Promise<BrokerIpc> {
  const sockPath = opts.sockPath ?? brokerSockPath();
  const pingIntervalMs = opts.pingIntervalMs ?? PING_INTERVAL_MS;
  const maxMissedPongs = opts.maxMissedPongs ?? MAX_MISSED_PONGS;
  const db = opts.inboxDb;

  // Global error boundary — log-and-keep-serving (stability §4). Installed once
  // per process; cheap to re-add (Node dedups identical listener fns, and these
  // are module-private so a second startBrokerIpc in a test won't stack noise).
  installGlobalErrorBoundary();

  // sessionKey -> the currently-bound live connection (latest wins; a reconnect
  // supersedes a stale socket so replay always targets the fresh shim).
  const conns = new Map<string, Conn>();
  // sessionKey -> (discordMsgId -> waiters[]) for awaitDelivered. Multiple
  // concurrent waiters on the same msgId are all resolved (no orphaning).
  const deliveryWaiters = new Map<string, Map<string, DeliveryWaiter[]>>();
  // Per-session egress serialization chain so progress + final replies for one
  // lane reach Discord in strict order (distinct lanes still run in parallel).
  const replyChains = new Map<string, Promise<void>>();
  // seq -> discordMsgId, populated at record/replay time so inbound_ack can
  // resolve the awaitDelivered waiter WITHOUT a DB read (a transient DB fault
  // then never costs a 30s lane stall).
  const seqToMsgId = new Map<number, string>();

  // Ensure the parent dir exists (mode 0700) and unlink any stale socket so the
  // bind doesn't EADDRINUSE on a crashed-daemon leftover.
  mkdirSync(dirname(sockPath), { recursive: true, mode: 0o700 });
  await unlink(sockPath).catch(() => {});

  function waitersFor(sessionKey: string): Map<string, DeliveryWaiter[]> {
    let m = deliveryWaiters.get(sessionKey);
    if (!m) {
      m = new Map();
      deliveryWaiters.set(sessionKey, m);
    }
    return m;
  }

  /** Resolve ALL waiters for a (sessionKey, discordMsgId) — never orphans one. */
  function resolveDelivery(sessionKey: string, discordMsgId: string): void {
    const m = deliveryWaiters.get(sessionKey);
    const list = m?.get(discordMsgId);
    if (!list || !m) return;
    m.delete(discordMsgId);
    if (m.size === 0) deliveryWaiters.delete(sessionKey);
    for (const w of list) {
      clearTimeout(w.timer);
      w.resolve();
    }
  }

  /** Reject ALL in-flight delivery waiters for a lane (disconnect/recycle). */
  function rejectAllWaiters(sessionKey: string, reason: string): void {
    const m = deliveryWaiters.get(sessionKey);
    if (!m) return;
    deliveryWaiters.delete(sessionKey);
    for (const [, list] of m) {
      for (const w of list) {
        clearTimeout(w.timer);
        w.reject(new Error(reason));
      }
    }
  }

  /**
   * Push an inbound frame to the bound shim for `sessionKey`, if connected.
   * Idempotent per seq (a row already sent to this conn — live or replayed — is
   * skipped) so a row never double-delivers. Rows inserted while the conn is
   * still replaying are buffered and drained in seq order afterwards, so a fresh
   * (higher-seq) inbound never jumps ahead of the still-streaming replay.
   */
  function pushInbound(sessionKey: string, row: InboxRow): void {
    const conn = conns.get(sessionKey);
    if (!conn || !conn.alive) return; // no live shim — replayed on reconnect
    if (conn.replaying) {
      conn.queuedDuringReplay.push(row);
      return;
    }
    deliverRow(conn, sessionKey, row);
  }

  /** Write one inbound row to a conn exactly once (per-seq idempotent). */
  function deliverRow(conn: Conn, sessionKey: string, row: InboxRow): void {
    seqToMsgId.set(row.seq, row.discordMsgId);
    if (conn.sentSeqs.has(row.seq)) return;
    conn.sentSeqs.add(row.seq);
    safeWrite(conn, {
      type: "inbound",
      sessionKey,
      seq: row.seq,
      content: row.content,
      meta: parseMeta(row),
    });
    // The lane is now working on this inbound — kick the "typing…" indicator.
    try {
      opts.onInbound?.({ sessionKey, chatId: row.chatId });
    } catch (e) {
      logErr(`onInbound(${sessionKey}) failed: ${String(e)}`);
    }
  }

  function parseMeta(row: InboxRow): InboxMeta {
    try {
      return JSON.parse(row.metaJson) as InboxMeta;
    } catch {
      // Fall back to reconstructing the minimal routing carrier from columns.
      return {
        chat_id: row.chatId,
        message_id: row.discordMsgId,
        user: "",
        user_id: "",
        ts: new Date(row.createdAt).toISOString(),
        thread_id: row.threadId ?? undefined,
        cwd: "",
      };
    }
  }

  function safeWrite(conn: Conn, obj: object): void {
    if (!conn.alive || conn.socket.destroyed) return;
    try {
      conn.socket.write(frame(obj));
    } catch (e) {
      logErr(`write to ${conn.sessionKey ?? "<unbound>"} failed: ${String(e)}`);
    }
  }

  // ---- hello: token check, bind, ordered replay ----
  function handleHello(conn: Conn, msg: Record<string, unknown>): void {
    const sessionKey = asString(msg.sessionKey);
    const token = asString(msg.token);
    conn.shimPid = asNumber(msg.shimPid);
    if (!sessionKey || token === null) {
      logErr(`hello missing sessionKey/token; closing socket (pid=${conn.shimPid ?? "?"})`);
      conn.socket.destroy();
      return;
    }
    if (!opts.verifyToken(sessionKey, token)) {
      logErr(`hello token mismatch for ${sessionKey}; closing socket`);
      conn.socket.destroy();
      return;
    }
    // Supersede any stale socket bound to this sessionKey (reconnect).
    const prev = conns.get(sessionKey);
    if (prev && prev.socket !== conn.socket) {
      prev.alive = false;
      try {
        prev.socket.destroy();
      } catch {
        // already gone
      }
    }
    conn.sessionKey = sessionKey;
    conn.alive = true;
    conn.lastPongAt = Date.now();
    conn.missedPongs = 0;
    conn.replaying = true; // gate live pushes until the replay loop drains
    conn.sentSeqs = new Set();
    conn.queuedDuringReplay = [];
    conns.set(sessionKey, conn);
    // A hello is a liveness signal; keep the supervisor registry in sync (also
    // promotes the lane spawning->live without scraping any PTY output).
    try {
      opts.markPong?.(sessionKey);
    } catch (e) {
      logErr(`markPong(${sessionKey}) on hello failed: ${String(e)}`);
    }

    // Ordered replay of pending + delivered-not-answered rows, by seq. While
    // `replaying` is set, any concurrently-inserted row is buffered (not pushed
    // live) so it cannot land ahead of a still-streaming replayed frame.
    let rows: InboxRow[] = [];
    try {
      rows = replayFor(db, sessionKey);
    } catch (e) {
      logErr(`replay query failed for ${sessionKey}: ${String(e)}`);
    }
    safeWrite(conn, { type: "hello_ack", sessionKey, replayCount: rows.length });
    for (const row of rows) {
      deliverRow(conn, sessionKey, row);
    }
    // Drain anything that arrived during replay, in seq order, deduped.
    conn.replaying = false;
    if (conn.queuedDuringReplay.length > 0) {
      const queued = conn.queuedDuringReplay
        .slice()
        .sort((a, b) => a.seq - b.seq);
      conn.queuedDuringReplay = [];
      for (const row of queued) deliverRow(conn, sessionKey, row);
    }
  }

  // ---- inbound_ack: markDelivered + resolve awaitDelivered ----
  // Only an authenticated (hello-bound) connection may ack — never trust a
  // sessionKey carried in the frame body (that would let an unbound socket forge
  // another lane's delivery). The hello token check is the single gate.
  function handleInboundAck(c: Conn, msg: Record<string, unknown>): void {
    if (c.sessionKey === null) {
      logErr(`inbound_ack from unauthenticated socket (no hello); dropping`);
      return;
    }
    const sessionKey = c.sessionKey;
    const seq = asNumber(msg.seq);
    if (seq === null) return;
    // Resolve the waiter from the in-memory seq->msgId cache FIRST, so a DB
    // hiccup in markDelivered can never cost a 30s lane stall.
    const cachedMsgId = seqToMsgId.get(seq);
    if (cachedMsgId) resolveDelivery(sessionKey, cachedMsgId);
    try {
      markDelivered(db, seq);
      if (!cachedMsgId) {
        const row = getRow(db, seq);
        if (row?.discordMsgId) resolveDelivery(sessionKey, row.discordMsgId);
      }
    } catch (e) {
      logErr(`markDelivered(${seq}) failed: ${String(e)}`);
    }
  }

  // ---- reply: route to egress (per-session ordered), ack, mark answered ----
  // Only an authenticated (hello-bound) connection may reply — never trust a
  // sessionKey from the frame body (would let an unbound socket exfiltrate to
  // any allowlisted channel as another lane). The hello token check is the gate.
  async function handleReply(c: Conn, msg: Record<string, unknown>): Promise<void> {
    if (c.sessionKey === null) {
      logErr(`reply from unauthenticated socket (no hello); dropping`);
      return;
    }
    const sessionKey = c.sessionKey;
    const rpcId = asString(msg.rpcId);
    const chatId = asString(msg.chat_id);
    const text = asString(msg.text) ?? "";
    const replyTo = asString(msg.reply_to) ?? undefined;
    const files = Array.isArray(msg.files)
      ? msg.files.filter((f): f is string => typeof f === "string")
      : undefined;
    const seq = asNumber(msg.seq);
    // The FINAL reply of a turn closes the inbox row. Progress replies set
    // final=false (or omit it) so a long turn's intermediate replies never mark
    // the row answered prematurely — the row stays replayable until the final
    // reply lands, so a mid-turn shim reconnect still re-delivers the prompt.
    const isFinal = msg.final === true;

    if (rpcId === null) {
      logErr(`reply from ${sessionKey} missing rpcId; dropping`);
      return;
    }
    if (chatId === null) {
      safeWrite(c, {
        type: "reply_ack",
        sessionKey,
        rpcId,
        ok: false,
        id: "",
        error: "chat_id missing",
      });
      return;
    }
    // Serialize egress per sessionKey: chain onto the lane's reply promise so
    // two reply frames read back-to-back reach Discord in order (a 429 retry on
    // the first can't let the second overtake it). Distinct lanes are parallel.
    const prev = replyChains.get(sessionKey) ?? Promise.resolve();
    const run = prev.then(async () => {
      try {
        await opts.onReply({ sessionKey, chatId, text, replyTo, files, final: isFinal });
        // Mark answered ONLY on the final reply (idempotent on an already-
        // answered row). Progress replies leave the row open + replayable.
        if (isFinal && seq !== null) {
          try {
            markAnswered(db, seq);
          } catch (e) {
            logErr(`markAnswered(${seq}) failed: ${String(e)}`);
          }
        }
        safeWrite(c, { type: "reply_ack", sessionKey, rpcId, ok: true, id: "sent" });
      } catch (e) {
        const error =
          e instanceof Error && /allowlist/i.test(e.message)
            ? "chat_id not allowlisted"
            : `reply failed: ${String(e)}`;
        logErr(`onReply for ${sessionKey} failed: ${String(e)}`);
        safeWrite(c, { type: "reply_ack", sessionKey, rpcId, ok: false, id: "", error });
      }
    });
    // Keep the chain alive but swallow rejections so one failed egress doesn't
    // poison the lane's future replies (log-and-keep-serving).
    replyChains.set(
      sessionKey,
      run.catch(() => undefined),
    );
    await run.catch(() => undefined);
  }

  // ---- create_thread: route to platform thread-create egress, ack with id ----
  // Same auth gate as handleReply (hello-bound socket only) and same per-session
  // egress serialization, so a create_thread + the reply that follows it reach
  // the platform in order.
  async function handleCreateThread(c: Conn, msg: Record<string, unknown>): Promise<void> {
    if (c.sessionKey === null) {
      logErr(`create_thread from unauthenticated socket (no hello); dropping`);
      return;
    }
    const sessionKey = c.sessionKey;
    const rpcId = asString(msg.rpcId);
    const parentChatId = asString(msg.parent_chat_id);
    const name = asString(msg.name) ?? "";
    const seedText = asString(msg.seed_text) ?? undefined;
    if (rpcId === null) {
      logErr(`create_thread from ${sessionKey} missing rpcId; dropping`);
      return;
    }
    if (!opts.onCreateThread) {
      safeWrite(c, { type: "create_thread_ack", sessionKey, rpcId, ok: false, error: "create_thread not supported" });
      return;
    }
    if (parentChatId === null || !name.trim()) {
      safeWrite(c, {
        type: "create_thread_ack",
        sessionKey,
        rpcId,
        ok: false,
        error: "parent_chat_id and name required",
      });
      return;
    }
    const prev = replyChains.get(sessionKey) ?? Promise.resolve();
    const run = prev.then(async () => {
      try {
        const { threadId, parentId } = await opts.onCreateThread!({
          sessionKey,
          parentChatId,
          name,
          seedText,
        });
        safeWrite(c, { type: "create_thread_ack", sessionKey, rpcId, ok: true, threadId, parentId });
      } catch (e) {
        const error =
          e instanceof Error && /allowlist|owned|not configured/i.test(e.message)
            ? e.message
            : `create_thread failed: ${String(e)}`;
        logErr(`onCreateThread for ${sessionKey} failed: ${String(e)}`);
        safeWrite(c, { type: "create_thread_ack", sessionKey, rpcId, ok: false, error });
      }
    });
    replyChains.set(
      sessionKey,
      run.catch(() => undefined),
    );
    await run.catch(() => undefined);
  }

  // ---- progress: live status egress (post/edit a status message), ack ----
  // Same auth gate + per-session serialization as handleReply, so a progress
  // update and the reply that follows reach the platform in order.
  async function handleProgress(c: Conn, msg: Record<string, unknown>): Promise<void> {
    if (c.sessionKey === null) {
      logErr(`progress from unauthenticated socket (no hello); dropping`);
      return;
    }
    const sessionKey = c.sessionKey;
    const rpcId = asString(msg.rpcId);
    const chatId = asString(msg.chat_id);
    const text = asString(msg.text) ?? "";
    if (rpcId === null) {
      logErr(`progress from ${sessionKey} missing rpcId; dropping`);
      return;
    }
    if (!opts.onProgress) {
      safeWrite(c, { type: "progress_ack", sessionKey, rpcId, ok: false, error: "progress not supported" });
      return;
    }
    if (chatId === null) {
      safeWrite(c, { type: "progress_ack", sessionKey, rpcId, ok: false, error: "chat_id missing" });
      return;
    }
    const prev = replyChains.get(sessionKey) ?? Promise.resolve();
    const run = prev.then(async () => {
      try {
        await opts.onProgress!({ sessionKey, chatId, text });
        safeWrite(c, { type: "progress_ack", sessionKey, rpcId, ok: true });
      } catch (e) {
        logErr(`onProgress for ${sessionKey} failed: ${String(e)}`);
        safeWrite(c, {
          type: "progress_ack",
          sessionKey,
          rpcId,
          ok: false,
          error: `progress failed: ${String(e)}`,
        });
      }
    });
    replyChains.set(
      sessionKey,
      run.catch(() => undefined),
    );
    await run.catch(() => undefined);
  }

  // ---- pong: heartbeat liveness ----
  function handlePong(c: Conn, msg: Record<string, unknown>): void {
    void msg;
    if (c.sessionKey === null) return; // unauthenticated socket: ignore
    c.lastPongAt = Date.now();
    c.missedPongs = 0;
    // Keep the supervisor's durable registry liveness view in sync.
    try {
      opts.markPong?.(c.sessionKey);
    } catch (e) {
      logErr(`markPong(${c.sessionKey}) failed: ${String(e)}`);
    }
  }

  // ---- per-connection wiring ----
  const server: Server = createServer((socket: Socket) => {
    const conn: Conn = {
      socket,
      sessionKey: null,
      shimPid: null,
      missedPongs: 0,
      lastPongAt: Date.now(),
      alive: true,
      replaying: false,
      sentSeqs: new Set(),
      queuedDuringReplay: [],
    };

    const decode = createFrameDecoder(
      (msg) => {
        const type = asString(msg.type);
        try {
          switch (type) {
            case "hello":
              handleHello(conn, msg);
              break;
            case "inbound_ack":
              handleInboundAck(conn, msg);
              break;
            case "reply":
              // fire-and-forget: a slow egress never blocks the read loop, and a
              // rejected promise is swallowed inside handleReply (acks ok:false).
              void handleReply(conn, msg);
              break;
            case "create_thread":
              void handleCreateThread(conn, msg);
              break;
            case "progress":
              void handleProgress(conn, msg);
              break;
            case "pong":
              handlePong(conn, msg);
              break;
            default:
              logErr(`unknown frame type ${type ?? "<none>"} from ${conn.sessionKey ?? "<unbound>"}`);
          }
        } catch (e) {
          // Per-connection isolation: a handler throw never tears down the server.
          logErr(`handler error for ${type ?? "?"}: ${String(e)}`);
        }
      },
      (len) => {
        // Oversized frame: tear down this socket (its read buffer is poisoned).
        conn.alive = false;
        try {
          conn.socket.destroy();
        } catch {
          // already gone
        }
        logErr(`oversized frame (${len}B) from ${conn.sessionKey ?? "<unbound>"}; socket destroyed`);
      },
    );

    socket.on("data", (chunk: Buffer) => {
      try {
        decode(chunk);
      } catch (e) {
        logErr(`decode error: ${String(e)}`);
      }
    });
    socket.on("error", (e) => {
      logErr(`socket error (${conn.sessionKey ?? "<unbound>"}): ${String(e)}`);
    });
    socket.on("close", () => {
      conn.alive = false;
      // Only drop the registry entry if THIS socket is still the bound one — a
      // reconnect may have already superseded it (don't evict the fresh shim).
      if (conn.sessionKey) {
        const cur = conns.get(conn.sessionKey);
        if (cur === conn) {
          conns.delete(conn.sessionKey);
          // Fail any in-flight delivery waiters fast: holding the serial lane
          // the full 30s after the shim is known dead just delays error
          // surfacing + lane release. The durable row replays on reconnect, so
          // failing now is safe.
          rejectAllWaiters(conn.sessionKey, `lane ${conn.sessionKey} disconnected`);
        }
      }
    });
  });

  server.on("error", (e) => {
    logErr(`server error: ${String(e)}`);
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(sockPath, () => {
      server.off("error", reject);
      resolve();
    });
  });

  // ---- heartbeat loop: ping all bound lanes; recycle wedged ones ----
  // Liveness is judged by ELAPSED TIME since the last pong, not a counter that
  // is incremented before any ping is sent. Time-based liveness is immune to
  // increment/reset ordering (no spurious recycle of a live lane on the bind
  // tick — the §4 restart-as-control-flow failure). We ping first, then recycle
  // any lane that has been silent longer than maxMissedPongs * pingIntervalMs.
  const wedgeThresholdMs = maxMissedPongs * pingIntervalMs;
  const heartbeat = setInterval(() => {
    const now = Date.now();
    for (const [sessionKey, conn] of conns) {
      if (!conn.alive || conn.socket.destroyed) continue;
      const silentMs = now - conn.lastPongAt;
      if (silentMs > wedgeThresholdMs) {
        logErr(
          `lane ${sessionKey} silent ${Math.round(silentMs / 1000)}s (> ~${Math.round(
            wedgeThresholdMs / 1000,
          )}s wedged); recycling`,
        );
        conn.alive = false;
        const dead = conns.get(sessionKey);
        if (dead === conn) conns.delete(sessionKey);
        // Fail in-flight waiters now so execClaude's catch fires immediately
        // instead of after its own 30s timeout (lane is durable + replayable).
        rejectAllWaiters(sessionKey, `lane ${sessionKey} recycled (wedged)`);
        try {
          conn.socket.destroy();
        } catch {
          // already gone
        }
        void opts
          .recycleSession(sessionKey, `silent ${Math.round(silentMs / 1000)}s (>= ${maxMissedPongs} missed pongs)`)
          .catch((e) => logErr(`recycleSession(${sessionKey}) failed: ${String(e)}`));
        continue;
      }
      safeWrite(conn, { type: "ping", sessionKey, ts: now });
    }
  }, pingIntervalMs);
  heartbeat.unref();

  // ---- broker -> runner API ----
  const ipc: BrokerIpc = {
    async sendInbound(sessionKey, content, meta) {
      // 1. Persist BEFORE dispatch (UNIQUE de-dup makes gateway-resume
      //    re-delivery a no-op). recordInbound returns inserted:false on a dup.
      const rec = recordInbound(db, {
        sessionKey,
        discordMsgId: meta.message_id,
        chatId: meta.chat_id,
        threadId: meta.thread_id ?? null,
        content,
        meta,
      });
      if (rec.seq >= 0) seqToMsgId.set(rec.seq, meta.message_id);
      // A dup (gateway-resume resend) is either already in flight or already
      // answered — do NOT re-ensure/re-push. The runner skips awaitDelivered
      // for inserted:false too, so there's no waiter to satisfy.
      if (!rec.inserted) return rec;
      // 2. Ensure the lane exists (lazy cold-start). A failed spawn is logged
      //    but never thrown out of the lane (per-session isolation): the row is
      //    durable and replays once the shim eventually connects.
      try {
        await opts.ensureSession(sessionKey, meta.cwd);
      } catch (e) {
        logErr(`ensureSession(${sessionKey}) failed: ${String(e)}`);
      }
      // 3. Push if a shim is already connected; otherwise hello-driven replay
      //    will (re)deliver in seq order.
      const row = getRow(db, rec.seq);
      if (row) pushInbound(sessionKey, row);
      return rec;
    },

    awaitDelivered(sessionKey, discordMsgId, timeoutMs = DEFAULT_DELIVER_TIMEOUT_MS) {
      return new Promise<void>((resolve, reject) => {
        // Fast path: if the row is already delivered/answered (a warm shim acked
        // before the waiter was registered, or this is a dedup resend of an
        // already-handled message), resolve immediately. Without this, a
        // warm-fast turn hangs the full timeout because the ack already fired.
        try {
          const row = getRowByMsgId(db, discordMsgId);
          if (row && (row.state === "delivered" || row.state === "answered")) {
            resolve();
            return;
          }
        } catch (e) {
          logErr(`awaitDelivered fast-path query failed: ${String(e)}`);
        }
        // Known-down lane: short-circuit instead of silently black-holing for
        // the full timeout. The row is durable + replays when the lane recovers.
        const state = opts.laneState?.(sessionKey);
        if (state === "breaker-open" || state === "dead") {
          reject(new Error(`lane ${sessionKey} is ${state}; message persisted, will replay`));
          return;
        }
        const m = waitersFor(sessionKey);
        const timer = setTimeout(() => {
          const list = deliveryWaiters.get(sessionKey)?.get(discordMsgId);
          if (list) {
            const idx = list.indexOf(waiter);
            if (idx >= 0) list.splice(idx, 1);
            if (list.length === 0) {
              deliveryWaiters.get(sessionKey)?.delete(discordMsgId);
            }
          }
          reject(new Error(`awaitDelivered timeout for ${sessionKey}:${discordMsgId}`));
        }, timeoutMs);
        if (typeof timer.unref === "function") timer.unref();
        const waiter: DeliveryWaiter = { resolve, reject, timer };
        // Append (never overwrite): concurrent waiters on the same msgId are all
        // resolved together by resolveDelivery — no orphaned never-settling one.
        const list = m.get(discordMsgId);
        if (list) list.push(waiter);
        else m.set(discordMsgId, [waiter]);
      });
    },

    async close() {
      clearInterval(heartbeat);
      // Reject any in-flight delivery waiters so callers don't hang on shutdown.
      for (const [, m] of deliveryWaiters) {
        for (const [, list] of m) {
          for (const w of list) {
            clearTimeout(w.timer);
            w.reject(new Error("broker ipc closing"));
          }
        }
      }
      deliveryWaiters.clear();
      for (const [, conn] of conns) {
        conn.alive = false;
        try {
          conn.socket.destroy();
        } catch {
          // already gone
        }
      }
      conns.clear();
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
      await unlink(sockPath).catch(() => {});
    },

    sockPath,
  };

  return ipc;
}

// ---------------------------------------------------------------------------
// Global error boundary (installed once).
// ---------------------------------------------------------------------------

let boundaryInstalled = false;

function installGlobalErrorBoundary(): void {
  if (boundaryInstalled) return;
  boundaryInstalled = true;
  process.on("unhandledRejection", (reason) => {
    logErr(`unhandledRejection: ${String(reason)}`);
  });
  process.on("uncaughtException", (err) => {
    logErr(`uncaughtException: ${String(err)}`);
  });
}
