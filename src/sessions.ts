/**
 * Workspace-scoped Claude session state. Single source of truth is the
 * SQLite `sessions` table keyed by `workspace:<hash>`; legacy JSON files
 * under `.claude/hermes/` are read-only migration input, produced once by
 * `importLegacyJson` and never written again.
 *
 * Every function is keyed by a working directory (default `process.cwd()`,
 * the daemon home). Callers that run a channel in its own project directory
 * (see `discord.channelDirectories`) pass that cwd so each project gets its
 * own workspace session — and so a session created under one project's
 * `~/.claude/projects/<slug>` is only ever `--resume`d from that same cwd.
 *
 * `backupSession` still writes a `session_N.backup` JSON file under the
 * daemon's `.claude/hermes/` because it is a user-facing artifact (people
 * restore from it by hand); the row is deleted from the DB afterwards.
 */

import { readdir, unlink, writeFile } from "fs/promises";
import { join } from "path";
import { workspaceKey } from "./router/session-key";
import { hermesDir, sessionFile } from "./paths";
import { getSharedDb } from "./state/shared-db";
import {
  bumpTurn,
  deleteByKey,
  getByKey,
  markCompactWarned as repoMarkCompactWarned,
  replaceSession,
  touchLastUsed,
} from "./state/repos/sessions";

export interface GlobalSession {
  sessionId: string;
  createdAt: string;
  lastUsedAt: string;
  turnCount: number;
  compactWarned: boolean;
}

function currentKey(cwd: string = process.cwd()): string {
  return workspaceKey(cwd);
}

async function findCurrent(cwd: string = process.cwd()): Promise<GlobalSession | null> {
  const db = await getSharedDb(cwd);
  const row = getByKey(db, currentKey(cwd));
  if (!row || !row.claude_session_id) return null;
  return {
    sessionId: row.claude_session_id,
    createdAt: row.created_at,
    lastUsedAt: row.last_used_at,
    turnCount: row.turn_count,
    compactWarned: row.compact_warned !== 0,
  };
}

/** Returns the existing session or null. Bumps `lastUsedAt` as a side effect. */
export async function getSession(cwd: string = process.cwd()): Promise<{ sessionId: string; turnCount: number; compactWarned: boolean } | null> {
  const db = await getSharedDb(cwd);
  const row = getByKey(db, currentKey(cwd));
  if (!row || !row.claude_session_id) return null;
  touchLastUsed(db, row.id);
  return {
    sessionId: row.claude_session_id,
    turnCount: row.turn_count,
    compactWarned: row.compact_warned !== 0,
  };
}

/** Save a session ID obtained from Claude Code's output. Resets turn/compact counters. */
export async function createSession(sessionId: string, cwd: string = process.cwd()): Promise<void> {
  const db = await getSharedDb(cwd);
  replaceSession(db, {
    key: workspaceKey(cwd),
    scope: "workspace",
    source: "cli",
    workspace: cwd,
    claudeSessionId: sessionId,
  });
}

/** Returns session metadata without mutating lastUsedAt. */
export async function peekSession(cwd: string = process.cwd()): Promise<GlobalSession | null> {
  return await findCurrent(cwd);
}

/** Increment the turn counter after a successful Claude invocation. */
export async function incrementTurn(cwd: string = process.cwd()): Promise<number> {
  const db = await getSharedDb(cwd);
  const row = getByKey(db, currentKey(cwd));
  if (!row) return 0;
  bumpTurn(db, row.id);
  return row.turn_count + 1;
}

/** Mark that the compact warning has been sent for the current session. */
export async function markCompactWarned(cwd: string = process.cwd()): Promise<void> {
  const db = await getSharedDb(cwd);
  const row = getByKey(db, currentKey(cwd));
  if (!row) return;
  repoMarkCompactWarned(db, row.id);
}

export async function resetSession(cwd: string = process.cwd()): Promise<void> {
  const db = await getSharedDb(cwd);
  deleteByKey(db, currentKey(cwd));
  // Legacy session.json is imported into SQLite on first shared-db open.
  // After a reset the user expects an empty slate, so drop the legacy
  // file too — otherwise the next boot re-imports it and "reset" looks
  // like it did nothing.
  await unlink(sessionFile()).catch(() => {});
}

export async function backupSession(cwd: string = process.cwd()): Promise<string | null> {
  const existing = await findCurrent(cwd);
  if (!existing) return null;

  let files: string[];
  try {
    files = await readdir(hermesDir());
  } catch {
    files = [];
  }
  const indices = files
    .filter((f) => /^session_\d+\.backup$/.test(f))
    .map((f) => Number(f.match(/^session_(\d+)\.backup$/)![1]));
  const nextIndex = indices.length > 0 ? Math.max(...indices) + 1 : 1;

  const backupName = `session_${nextIndex}.backup`;
  const backupPath = join(hermesDir(), backupName);
  await writeFile(backupPath, JSON.stringify(existing, null, 2) + "\n", "utf8");

  const db = await getSharedDb(cwd);
  deleteByKey(db, currentKey(cwd));
  // Legacy session.json, if present, has already been imported into
  // SQLite on shared-db open. After backup the row is gone and we also
  // remove the legacy file — otherwise the next boot re-imports stale
  // state and the backup appears to have been ignored.
  await unlink(sessionFile()).catch(() => {});

  return backupName;
}
