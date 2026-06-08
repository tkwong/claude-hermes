/**
 * Single broker-owned SQLite writer for the durable inbox.
 *
 * Opens its OWN dedicated bun:sqlite handle at `inboxDbFile()` (NOT `state.db`,
 * NOT `getSharedDb` — keeping the broker the sole writer is the stability-§4
 * fix for the telegram "PPID=1 orphan holds the WAL lock" failure). Shims NEVER
 * import this module: they only ever see frames over the AF_UNIX socket.
 *
 * Durability contract (broker-design.md §4):
 *   - De-dup by `discord_msg_id UNIQUE` → gateway-resume re-delivery is a no-op
 *     (exactly-once per session).
 *   - `pending | delivered | answered` state machine: persist BEFORE dispatch,
 *     `markDelivered` on the shim's `inbound_ack`, `markAnswered` on the reply
 *     that carries the seq.
 *   - Ordered replay on shim reconnect: `replayFor` returns pending +
 *     delivered-not-answered rows, by seq.
 *
 * Reuses the openDb() PRAGMA pattern (WAL + synchronous=NORMAL + foreign_keys).
 */

import { Database } from "bun:sqlite";
import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { hermesDir } from "../paths";

export interface InboxRow {
  seq: number;
  sessionKey: string;
  discordMsgId: string;
  chatId: string;
  threadId: string | null;
  content: string;
  metaJson: string;
  state: "pending" | "delivered" | "answered";
  createdAt: number;
  deliveredAt: number | null;
  answeredAt: number | null;
}

export interface InboxMeta {
  chat_id: string;
  message_id: string;
  user: string;
  user_id: string;
  ts: string;
  thread_id?: string;
  cwd: string;
}

/**
 * On-disk path for the broker inbox DB. Deliberately distinct from `state.db`
 * (the shared session/messages store) so the broker keeps an exclusive writer
 * handle on the inbox without contending with `getSharedDb`.
 */
export function inboxDbFile(cwd?: string): string {
  return join(hermesDir(cwd ?? process.cwd()), "inbox.db");
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS inbox (
  seq            INTEGER PRIMARY KEY AUTOINCREMENT,
  session_key    TEXT    NOT NULL,
  discord_msg_id TEXT    NOT NULL UNIQUE,
  chat_id        TEXT    NOT NULL,
  thread_id      TEXT,
  content        TEXT    NOT NULL,
  meta_json      TEXT    NOT NULL,
  state          TEXT    NOT NULL DEFAULT 'pending'
                 CHECK (state IN ('pending','delivered','answered')),
  created_at     INTEGER NOT NULL,
  delivered_at   INTEGER,
  answered_at    INTEGER
);
CREATE INDEX IF NOT EXISTS idx_inbox_replay ON inbox (session_key, state, seq);
`;

/**
 * Open (or create) the broker inbox DB. Runs the standard openDb() PRAGMAs
 * (WAL, synchronous=NORMAL, foreign_keys=ON) then applies the inbox schema.
 */
export function openInbox(path?: string): Database {
  const dbPath = path ?? inboxDbFile();
  if (dbPath !== ":memory:") {
    mkdirSync(dirname(dbPath), { recursive: true });
  }
  const db = new Database(dbPath);
  db.exec("PRAGMA journal_mode = WAL;");
  db.exec("PRAGMA synchronous = NORMAL;");
  db.exec("PRAGMA foreign_keys = ON;");
  db.exec(SCHEMA);
  return db;
}

interface RawInboxRow {
  seq: number;
  session_key: string;
  discord_msg_id: string;
  chat_id: string;
  thread_id: string | null;
  content: string;
  meta_json: string;
  state: string;
  created_at: number;
  delivered_at: number | null;
  answered_at: number | null;
}

function toRow(r: RawInboxRow): InboxRow {
  return {
    seq: r.seq,
    sessionKey: r.session_key,
    discordMsgId: r.discord_msg_id,
    chatId: r.chat_id,
    threadId: r.thread_id,
    content: r.content,
    metaJson: r.meta_json,
    state: r.state as InboxRow["state"],
    createdAt: r.created_at,
    deliveredAt: r.delivered_at,
    answeredAt: r.answered_at,
  };
}

/**
 * Persist an inbound message. De-dup by `discord_msg_id` (INSERT ... ON CONFLICT
 * DO NOTHING): a gateway-resume re-delivery returns the existing row's seq with
 * `inserted:false` so the caller skips re-dispatch.
 */
export function recordInbound(
  db: Database,
  row: {
    sessionKey: string;
    discordMsgId: string;
    chatId: string;
    threadId: string | null;
    content: string;
    meta: InboxMeta;
  },
): { seq: number; inserted: boolean } {
  const createdAt = Date.now();
  const metaJson = JSON.stringify(row.meta);
  const insert = db.query(
    `INSERT INTO inbox (session_key, discord_msg_id, chat_id, thread_id, content, meta_json, state, created_at)
     VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
     ON CONFLICT(discord_msg_id) DO NOTHING
     RETURNING seq`,
  );
  const inserted = insert.get(
    row.sessionKey,
    row.discordMsgId,
    row.chatId,
    row.threadId,
    row.content,
    metaJson,
    createdAt,
  ) as { seq: number } | null;

  if (inserted) {
    return { seq: inserted.seq, inserted: true };
  }

  // Conflict: the row already exists. Look up its seq.
  const existing = db
    .query("SELECT seq FROM inbox WHERE discord_msg_id = ?")
    .get(row.discordMsgId) as { seq: number } | null;
  // Should always be present after a conflict; -1 is a defensive sentinel.
  return { seq: existing?.seq ?? -1, inserted: false };
}

/** Transition pending → delivered (idempotent; never downgrades answered). */
export function markDelivered(db: Database, seq: number): void {
  db.run(
    `UPDATE inbox
     SET state = 'delivered', delivered_at = ?
     WHERE seq = ? AND state = 'pending'`,
    [Date.now(), seq],
  );
}

/** Transition pending/delivered → answered (idempotent). */
export function markAnswered(db: Database, seq: number): void {
  db.run(
    `UPDATE inbox
     SET state = 'answered', answered_at = ?
     WHERE seq = ? AND state IN ('pending','delivered')`,
    [Date.now(), seq],
  );
}

/**
 * Ordered replay set for a reconnecting shim: every pending OR
 * delivered-not-answered row for the session, by seq ascending.
 */
export function replayFor(db: Database, sessionKey: string): InboxRow[] {
  const rows = db
    .query(
      `SELECT * FROM inbox
       WHERE session_key = ? AND state IN ('pending','delivered')
       ORDER BY seq ASC`,
    )
    .all(sessionKey) as RawInboxRow[];
  return rows.map(toRow);
}

export function getRow(db: Database, seq: number): InboxRow | null {
  const r = db.query("SELECT * FROM inbox WHERE seq = ?").get(seq) as RawInboxRow | null;
  return r ? toRow(r) : null;
}

/**
 * Look up a row by its `discord_msg_id` (the UNIQUE de-dup key). Used by
 * `awaitDelivered` to fast-resolve when a warm shim acked before the waiter was
 * registered (the row is already `delivered`/`answered`), avoiding a spurious
 * 30s timeout on every warm-fast turn.
 */
export function getRowByMsgId(db: Database, discordMsgId: string): InboxRow | null {
  const r = db
    .query("SELECT * FROM inbox WHERE discord_msg_id = ?")
    .get(discordMsgId) as RawInboxRow | null;
  return r ? toRow(r) : null;
}

export function closeInbox(db: Database): void {
  db.close();
}
