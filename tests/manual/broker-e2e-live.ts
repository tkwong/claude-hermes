#!/usr/bin/env bun
/**
 * LIVE end-to-end broker round-trip — proves the FULL real path:
 *
 *   sendInbound (fake Discord msg)
 *     -> broker IPC (real ipc.ts)
 *     -> supervisor spawns a REAL `claude --channels` lane in tmux (real sessions.ts)
 *     -> masqueraded channel-shim connects to the broker, gets the inbound
 *     -> the interactive (subscription-billed) claude session calls the reply tool
 *     -> broker onReply fires with the reply text
 *
 * It uses the SAME wiring as src/commands/start.ts startBroker(), with two
 * deliberate substitutions so it touches nothing live:
 *   - a TEMP inbox DB + TEMP broker sock (not the daemon's)
 *   - onReply CAPTURES the reply instead of posting to Discord
 *
 * Run: bun run tests/manual/broker-e2e-live.ts
 * Requires: the discord plugin cached, ~/Projects trusted, a logged-in claude.
 */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir, homedir } from "node:os";
import { join } from "node:path";
import { openInbox, closeInbox, type InboxMeta } from "../../src/broker/inbox";
import { createSessionSupervisor, installChannelShim } from "../../src/broker/sessions";
import { startBrokerIpc } from "../../src/broker/ipc";

const CWD = join(homedir(), "Projects"); // trusted dir → no trust prompt
const SESSION_KEY = "workspace:e2e-smoke";
const CHAT_ID = "smoke-chat-1";
const MSG_ID = "smoke-msg-1";
const BOOT_TIMEOUT_MS = 150_000;

function log(m: string) {
  process.stdout.write(`[e2e] ${m}\n`);
}

const dir = mkdtempSync(join(tmpdir(), "hermes-e2e-"));
const inboxFile = join(dir, "inbox.db");
const sockPath = join(dir, "broker.sock");

let captured: { sessionKey: string; chatId: string; text: string; replyTo?: string } | null = null;
let resolveReply: (() => void) | null = null;
const gotReply = new Promise<void>((res) => (resolveReply = res));

const inboxDb = openInbox(inboxFile);

const installed = installChannelShim();
log(`channel shim install: ${installed ?? "FAILED (discord plugin not cached)"}`);
if (!installed) process.exit(2);

const supervisor = createSessionSupervisor({
  sockPath,
  onAlert: (k, m) => log(`ALERT ${k}: ${m}`),
});

const ipc = await startBrokerIpc({
  sockPath, // MUST match the supervisor's sockPath (lanes connect here)
  inboxDb,
  onReply: ({ sessionKey, chatId, text, replyTo }) => {
    log(`*** onReply fired: chat=${chatId} text=${JSON.stringify(text)}`);
    captured = { sessionKey, chatId, text, replyTo };
    resolveReply?.();
    return Promise.resolve();
  },
  ensureSession: (k, cwd) => supervisor.ensureSession(k, cwd),
  recycleSession: (k, reason) => supervisor.recycleSession(k, reason),
  verifyToken: (k, t) => supervisor.verifyToken(k, t),
  markPong: (k) => supervisor.markPong(k),
  laneState: (k) => supervisor.get(k)?.state,
});
log(`broker listening at ${sockPath}`);

const meta: InboxMeta = {
  chat_id: CHAT_ID,
  message_id: MSG_ID,
  user: "smoke-tester",
  user_id: "0",
  ts: "0",
  cwd: CWD,
};
const content =
  "This is an automated Hermes broker smoke test. Reply with the single word PONG " +
  "using the reply tool (pass the chat_id from the channel tag). Do NOT run any " +
  "commands, read any files, or use any other tool.";

// NATURAL flow (no artificial warmup): inject immediately, exactly as the real
// daemon does when a Discord message arrives while the lane is cold. This relies
// on the shim's boot-race guard (buffer-until-channel-ready) to hold the inbound
// until claude has loaded the channel — otherwise it would be dropped during boot.
log(`sendInbound → spawning real claude --channels lane (cwd=${CWD}) + injecting immediately…`);
const { seq, inserted } = await ipc.sendInbound(SESSION_KEY, content, meta);
log(
  `inbound persisted seq=${seq} inserted=${inserted}; waiting up to ${BOOT_TIMEOUT_MS / 1000}s for the lane to boot + reply…`
);

const timeout = new Promise<void>((_, rej) =>
  setTimeout(() => rej(new Error("timeout: no reply within budget")), BOOT_TIMEOUT_MS)
);

let exitCode = 0;
try {
  await Promise.race([gotReply, timeout]);
  // `captured` is only ever assigned inside the onReply closure, so TS's
  // control-flow analysis narrows it to null here — read it through a typed alias.
  const cap = captured as { sessionKey: string; chatId: string; text: string } | null;
  const ok = !!cap && /pong/i.test(cap.text) && cap.chatId === CHAT_ID;
  log("");
  log("=================== VERDICT ===================");
  log(`reply captured: ${cap ? "YES" : "NO"}`);
  log(`chat_id routed correctly: ${cap?.chatId === CHAT_ID}`);
  log(`text contains PONG: ${cap ? /pong/i.test(cap.text) : false}`);
  log(`RESULT: ${ok ? "✅ PASS — full live round-trip works" : "❌ FAIL"}`);
  exitCode = ok ? 0 : 1;
} catch (e) {
  log("");
  log("=================== VERDICT ===================");
  log(`RESULT: ❌ FAIL — ${String(e)}`);
  log(`lane state at timeout: ${supervisor.get(SESSION_KEY)?.state ?? "unknown"}`);
  exitCode = 1;
} finally {
  log("tearing down (killing lane, closing broker)…");
  await ipc.close().catch(() => {});
  await supervisor.shutdown().catch(() => {});
  closeInbox(inboxDb);
  rmSync(dir, { recursive: true, force: true });
  process.exit(exitCode);
}
