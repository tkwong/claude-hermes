#!/usr/bin/env bun
/**
 * Phase-1 broker INTEGRATION smoke test — proves the broker IPC + inbox + routing
 * at the SOCKET level, end-to-end, WITHOUT a real interactive `claude`.
 *
 * What this exercises that the in-process unit tests (src/broker/ipc.test.ts,
 * src/broker/sessions.test.ts) do NOT:
 *   - The REAL `createSessionSupervisor` wired to the REAL `startBrokerIpc`,
 *     exactly as start.ts wires them (openInbox -> supervisor -> ipc, with
 *     verifyToken/ensureSession/recycleSession/markPong/onReply as the seam).
 *   - A SEPARATE fake-shim PROCESS (not an in-process socket client) that the
 *     supervisor's tmux-spawn hook launches — connecting over the real AF_UNIX
 *     socket with the supervisor-minted one-time token, speaking the real
 *     length-prefixed uint32be+JSON codec. This proves the cross-process wire
 *     contract and the token mint/verify handshake, which an in-process test
 *     can't (it never goes through ensureSession -> token env -> hello).
 *
 * The tmux spawn is STUBBED: instead of `tmux new-session ... claude
 * --dangerously-load-development-channels ...` (interactive, would block), the
 * stub launches `bun <this-file> --fake-shim` as a detached child, passing the
 * broker sock + sessionKey + token via env exactly as the supervisor would inject
 * them into the real lane. So the supervisor code path (token mint, env injection,
 * registry, has-session/kill-session bookkeeping) all runs for real.
 *
 * Run: bun tests/manual/broker-fakeshim.ts
 * Exit code 0 = all assertions passed; 1 = a failure (printed).
 *
 * NOTE: kept under tests/manual/ (NOT picked up by `bun test src`). It is a
 * driver, not a bun:test file — it spawns child processes and self-execs.
 */

import { spawn as nodeSpawn } from "node:child_process";
import { connect, type Socket } from "node:net";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// ===========================================================================
// FAKE SHIM CHILD MODE
// ---------------------------------------------------------------------------
// When invoked with --fake-shim this process acts as a thin per-session shim:
// connect to HERMES_BROKER_SOCK, hello with HERMES_TOKEN, on inbound immediately
// ack + RPC a reply, answer ping with pong. It speaks the SAME wire codec as
// src/shim/channel-shim.ts (duplicated inline, byte-identical). It logs each
// frame it sees to stderr as `SHIM <json>` so the parent can observe behavior.
// ===========================================================================

const MAX_FRAME_BYTES = 16 * 1024 * 1024;

function frame(obj: object): Buffer {
  const body = Buffer.from(JSON.stringify(obj), "utf8");
  const head = Buffer.alloc(4);
  head.writeUInt32BE(body.length, 0);
  return Buffer.concat([head, body]);
}

function makeDecoder(onFrame: (m: Record<string, unknown>) => void): (chunk: Buffer) => void {
  let buf: Buffer = Buffer.alloc(0);
  return (chunk: Buffer) => {
    buf = buf.length === 0 ? Buffer.from(chunk) : Buffer.concat([buf, chunk]);
    for (;;) {
      if (buf.length < 4) break;
      const len = buf.readUInt32BE(0);
      if (len > MAX_FRAME_BYTES) {
        buf = Buffer.alloc(0);
        return;
      }
      if (buf.length < 4 + len) break;
      const body = buf.subarray(4, 4 + len);
      buf = buf.subarray(4 + len);
      try {
        onFrame(JSON.parse(body.toString("utf8")) as Record<string, unknown>);
      } catch {
        // skip malformed
      }
    }
  };
}

function runFakeShim(): void {
  const sock = process.env.HERMES_BROKER_SOCK ?? "";
  const sessionKey = process.env.HERMES_SESSION_KEY ?? "";
  const token = process.env.HERMES_TOKEN ?? "";
  // Optional behavior knobs for negative tests.
  const badToken = process.env.FAKE_SHIM_BAD_TOKEN === "1";
  const silent = process.env.FAKE_SHIM_SILENT === "1"; // never pong (heartbeat test)

  const s: Socket = connect(sock);
  const send = (o: object) => {
    if (!s.destroyed) s.write(frame(o));
  };
  // Emit a structured line so the parent can correlate this child's lifecycle.
  const emit = (o: object) => process.stderr.write(`SHIM ${JSON.stringify({ sessionKey, ...o })}\n`);

  const decode = makeDecoder((m) => {
    emit({ recv: m.type, frame: m });
    switch (m.type) {
      case "hello_ack":
        break;
      case "inbound": {
        const seq = typeof m.seq === "number" ? m.seq : undefined;
        const meta = (m.meta && typeof m.meta === "object" ? m.meta : {}) as Record<string, unknown>;
        const chatId = typeof meta.chat_id === "string" ? meta.chat_id : "";
        if (seq !== undefined) send({ type: "inbound_ack", sessionKey, seq });
        // Immediately reply (final) — broker should route to onReply + mark answered.
        send({
          type: "reply",
          sessionKey,
          rpcId: `r-${seq}`,
          chat_id: chatId,
          text: `echo: ${typeof m.content === "string" ? m.content : ""}`,
          seq,
          final: true,
        });
        break;
      }
      case "ping": {
        if (!silent) send({ type: "pong", sessionKey, ts: typeof m.ts === "number" ? m.ts : Date.now() });
        break;
      }
      case "reply_ack":
        break;
      default:
        break;
    }
  });

  s.on("connect", () => {
    emit({ event: "connected" });
    send({ type: "hello", sessionKey, token: badToken ? "WRONG-TOKEN" : token, shimPid: process.pid });
  });
  s.on("data", (d: Buffer) => decode(d));
  s.on("error", (e) => emit({ event: "error", msg: String(e) }));
  s.on("close", () => emit({ event: "closed" }));

  process.on("unhandledRejection", () => {});
  process.on("uncaughtException", () => {});
  // Keep alive until killed by the supervisor (tmux kill-session => SIGTERM).
  setInterval(() => {}, 1 << 30).unref?.();
}

// ===========================================================================
// DRIVER MODE (default)
// ===========================================================================

let failures = 0;
function assert(cond: boolean, label: string): void {
  if (cond) {
    process.stdout.write(`  PASS  ${label}\n`);
  } else {
    failures += 1;
    process.stdout.write(`  FAIL  ${label}\n`);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitUntil(pred: () => boolean, timeoutMs = 4000, stepMs = 25): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (pred()) return true;
    await sleep(stepMs);
  }
  return pred();
}

async function main(): Promise<void> {
  const { openInbox, closeInbox, getRowByMsgId, recordInbound } = await import("../../src/broker/inbox");
  const { startBrokerIpc } = await import("../../src/broker/ipc");
  const { createSessionSupervisor } = await import("../../src/broker/sessions");

  const dir = mkdtempSync(join(tmpdir(), "hermes-broker-fakeshim-"));
  const sockPath = join(dir, "broker.sock");
  const registryPath = join(dir, "broker-sessions.json");
  const db = openInbox(":memory:");

  // Egress sink: records every reply the broker routed out (the real onReply
  // would call Discord sendMessage; here we just capture {sessionKey,chatId,text}).
  const egress: Array<{ sessionKey: string; chatId: string; text: string }> = [];

  // Track which fake-shim child processes we launched so we can reap them.
  const children: ReturnType<typeof nodeSpawn>[] = [];
  // Pretend tmux registry: tmuxName -> child handle, so has-session / kill-session
  // reflect reality (the supervisor uses these for warm-detect + recycle).
  const liveTmux = new Map<string, ReturnType<typeof nodeSpawn>>();

  const selfPath = import.meta.path ?? new URL(import.meta.url).pathname;

  // STUB tmux spawn: translate the supervisor's tmux argv into a real fake-shim
  // child process. new-session => launch `bun <self> --fake-shim` with the broker
  // env injected exactly as the supervisor builds it. has-session / kill-session /
  // display-message mimic tmux bookkeeping against `liveTmux`.
  const supervisor = createSessionSupervisor({
    sockPath,
    registryPath,
    backoffBaseMs: 50,
    backoffMaxMs: 200,
    breakerThreshold: 3,
    connectTimeoutMs: 3000,
    sweepIntervalMs: 0, // disable the periodic sweep for a deterministic run
    onAlert: (sk, msg) => process.stdout.write(`  ALERT ${sk}: ${msg}\n`),
    spawn: (_bin, args, o) => {
      const sub = args[0];
      if (sub === "has-session") {
        const name = args[args.indexOf("-t") + 1];
        return { status: liveTmux.has(name) ? 0 : 1, stdout: "", stderr: "" };
      }
      if (sub === "kill-session") {
        const name = args[args.indexOf("-t") + 1];
        const child = liveTmux.get(name);
        if (child) {
          try {
            child.kill("SIGTERM");
          } catch {
            // already gone
          }
          liveTmux.delete(name);
        }
        return { status: 0, stdout: "", stderr: "" };
      }
      if (sub === "display-message") {
        return { status: 0, stdout: "4242\n", stderr: "" };
      }
      if (sub === "new-session") {
        const name = args[args.indexOf("-s") + 1];
        // Launch the fake shim as a detached child with the broker env the
        // supervisor injected (HERMES_BROKER_SOCK / _SESSION_KEY / _TOKEN).
        const env = { ...(o.env ?? {}) } as Record<string, string>;
        const child = nodeSpawn(process.execPath, [selfPath, "--fake-shim"], {
          env,
          stdio: ["ignore", "ignore", "inherit"], // SHIM <...> lines surface for debugging
        });
        children.push(child);
        liveTmux.set(name, child);
        return { status: 0, stdout: "", stderr: "" };
      }
      return { status: 0, stdout: "", stderr: "" };
    },
  });

  const ipc = await startBrokerIpc({
    sockPath,
    inboxDb: db,
    onReply: async (r) => {
      egress.push({ sessionKey: r.sessionKey, chatId: r.chatId, text: r.text });
    },
    ensureSession: supervisor.ensureSession,
    recycleSession: supervisor.recycleSession,
    verifyToken: supervisor.verifyToken,
    markPong: supervisor.markPong,
    laneState: (sk) => supervisor.get(sk)?.state,
    pingIntervalMs: 300,
    maxMissedPongs: 2,
  });

  process.stdout.write(`broker listening on ${sockPath}\n\n`);

  try {
    // -------------------------------------------------------------------
    // TEST 1: cold-start lane via ensureSession -> real shim child connects,
    // inbound is delivered, shim acks + replies, egress fires, inbox row goes
    // pending -> delivered -> answered.
    // -------------------------------------------------------------------
    process.stdout.write("TEST 1: cold-start round-trip (ensureSession -> shim -> reply -> egress)\n");
    const sk1 = "workspace:aaaa1111bbbb";
    const meta1 = {
      chat_id: "chan-1",
      message_id: "msg-1",
      user: "tester",
      user_id: "u1",
      ts: new Date().toISOString(),
      cwd: dir,
    };

    // sendInbound persists pending, calls ensureSession (spawns the shim child),
    // then pushes once the shim's hello-driven replay (or live push) fires.
    const rec1 = await ipc.sendInbound(sk1, "ping one", meta1);
    assert(rec1.inserted === true, "inbound persisted (inserted=true)");

    // Wait for the lane to come up + the round-trip to complete.
    const got1 = await waitUntil(() => egress.some((e) => e.sessionKey === sk1), 6000);
    assert(got1, "egress callback received the shim's reply");
    const e1 = egress.find((e) => e.sessionKey === sk1);
    assert(e1?.chatId === "chan-1", "reply routed to the right chat_id (chan-1)");
    assert(e1?.text === "echo: ping one", "reply text round-tripped through the shim");

    // Inbox state machine: row must be answered (final reply carried the seq).
    const answered = await waitUntil(() => getRowByMsgId(db, "msg-1")?.state === "answered", 3000);
    const row1 = getRowByMsgId(db, "msg-1");
    assert(answered, "inbox row reached state=answered");
    assert(
      typeof row1?.deliveredAt === "number",
      "inbox row has deliveredAt set (passed through pending->delivered)"
    );
    assert(typeof row1?.answeredAt === "number", "inbox row has answeredAt set");

    // The supervisor promoted the lane to live on the shim's hello/pong.
    const lanePromoted = await waitUntil(() => supervisor.get(sk1)?.state === "live", 3000);
    assert(lanePromoted, "supervisor promoted lane to state=live (via markPong on hello/pong)");

    // -------------------------------------------------------------------
    // TEST 2: duplicate discord_msg_id is de-duped (no second push, no second
    // egress, inserted=false). Proves exactly-once at the DB layer.
    // -------------------------------------------------------------------
    process.stdout.write("\nTEST 2: duplicate discord_msg_id de-dup\n");
    const egressCountBefore = egress.filter((e) => e.sessionKey === sk1).length;
    const rec1dup = await ipc.sendInbound(sk1, "ping one (resend)", meta1);
    assert(rec1dup.inserted === false, "duplicate sendInbound returns inserted=false");
    assert(rec1dup.seq === rec1.seq, "duplicate maps to the SAME seq as the original");
    await sleep(400);
    const egressCountAfter = egress.filter((e) => e.sessionKey === sk1).length;
    assert(egressCountAfter === egressCountBefore, "duplicate produced NO additional egress");

    // -------------------------------------------------------------------
    // TEST 3: a second concurrent lane is fully isolated (its own shim child,
    // own token, own egress) — proves per-session isolation + parallel lanes.
    // -------------------------------------------------------------------
    process.stdout.write("\nTEST 3: second lane isolation (parallel session)\n");
    const sk2 = "workspace:cccc2222dddd";
    const meta2 = { ...meta1, chat_id: "chan-2", message_id: "msg-2", cwd: dir };
    const rec2 = await ipc.sendInbound(sk2, "ping two", meta2);
    assert(rec2.inserted === true, "second lane inbound persisted");
    const got2 = await waitUntil(() => egress.some((e) => e.sessionKey === sk2), 6000);
    assert(got2, "second lane produced its own egress");
    const e2 = egress.find((e) => e.sessionKey === sk2);
    assert(e2?.chatId === "chan-2", "second lane reply routed to chan-2 (not cross-wired)");
    assert(supervisor.list().length >= 2, "supervisor registry holds >=2 distinct lanes");
    assert(
      supervisor.get(sk1)?.token !== supervisor.get(sk2)?.token,
      "each lane has a distinct one-time token"
    );

    // -------------------------------------------------------------------
    // TEST 4: token security — a shim presenting the WRONG token is rejected
    // (socket closed by broker; verifyToken gate). We launch ONLY a rogue
    // bad-token child (we deliberately do NOT call ensureSession/sendInbound,
    // which would spawn a *legitimate* shim and answer the row — that would test
    // the happy path, not the forgery gate). A row is staged directly via
    // recordInbound (pending, no dispatch) so we can assert the forged shim never
    // gets it delivered.
    // -------------------------------------------------------------------
    process.stdout.write("\nTEST 4: bad-token shim is rejected (verifyToken gate)\n");
    const sk3 = "workspace:eeee3333ffff";
    // Pre-mint a token so the registry has a row for verifyToken to compare
    // against (the rogue will present "WRONG-TOKEN", which must NOT match it).
    const realToken = supervisor.mintToken(sk3);
    assert(typeof realToken === "string" && realToken.length > 0, "supervisor minted a token for sk3");
    // Stage a pending row WITHOUT dispatch (no ensureSession => no legit shim).
    recordInbound(db, {
      sessionKey: sk3,
      discordMsgId: "msg-3",
      chatId: "chan-3",
      threadId: null,
      content: "should not be delivered to a forged shim",
      meta: { ...meta1, chat_id: "chan-3", message_id: "msg-3" },
    });
    const rogue = nodeSpawn(process.execPath, [selfPath, "--fake-shim"], {
      env: {
        ...process.env,
        HERMES_BROKER_SOCK: sockPath,
        HERMES_SESSION_KEY: sk3,
        HERMES_TOKEN: realToken, // ignored by the child: FAKE_SHIM_BAD_TOKEN forces "WRONG-TOKEN" on the wire
        FAKE_SHIM_BAD_TOKEN: "1",
      } as Record<string, string>,
      stdio: ["ignore", "ignore", "pipe"],
    });
    children.push(rogue);
    // Observe the rogue child's lifecycle: the broker must close its socket after
    // the token mismatch, so the child should report a `closed` event.
    let rogueClosedByBroker = false;
    rogue.stderr?.on("data", (d: Buffer) => {
      if (d.toString().includes('"event":"closed"')) rogueClosedByBroker = true;
    });
    await sleep(1000);
    const row3 = getRowByMsgId(db, "msg-3");
    assert(
      row3?.state === "pending",
      "bad-token lane: staged row stays pending (forged shim never received it)"
    );
    assert(
      !egress.some((e) => e.chatId === "chan-3"),
      "bad-token lane produced NO egress (forged shim cannot reply)"
    );
    assert(rogueClosedByBroker, "broker closed the forged-token socket (verifyToken rejection)");

    // -------------------------------------------------------------------
    // TEST 5: ordered replay on reconnect. Kill TEST-3's lane shim (tmux
    // kill-session via recycle), stage a NEW inbound while it's down, then let
    // the supervisor respawn it — the pending row must replay + round-trip.
    // -------------------------------------------------------------------
    process.stdout.write("\nTEST 5: replay on reconnect after recycle\n");
    // Stage an inbound for sk2 first, but kill its shim before it can reply by
    // recycling the lane right after persisting. Use a fresh msg id.
    const meta2b = { ...meta1, chat_id: "chan-2", message_id: "msg-2b", cwd: dir };
    // Manually persist as pending without dispatch race: use recordInbound then
    // recycle, then ensureSession to bring it back and trigger replay.
    const recB = recordInbound(db, {
      sessionKey: sk2,
      discordMsgId: meta2b.message_id,
      chatId: meta2b.chat_id,
      threadId: null,
      content: "ping two-b (staged while down)",
      meta: meta2b,
    });
    assert(recB.inserted === true, "staged replay row persisted as pending");
    // Manual recycle = kill + immediate respawn of ONLY this lane.
    await supervisor.recycleSession(sk2, "test replay", true);
    // After respawn the shim reconnects, hello triggers ordered replay of the
    // pending row, shim acks + replies, egress fires for chan-2 / the staged msg.
    const replayed = await waitUntil(() => getRowByMsgId(db, "msg-2b")?.state === "answered", 8000);
    assert(replayed, "staged-while-down row replayed on reconnect and reached answered");
    // sk1 (the other lane) must be untouched by sk2's recycle (isolation).
    assert(
      supervisor.get(sk1)?.state === "live",
      "recycling sk2 did NOT disturb sk1 (per-session isolation)"
    );

    // -------------------------------------------------------------------
    // Summary
    // -------------------------------------------------------------------
    process.stdout.write(
      `\nfake-shim children launched: ${children.length}; lanes in registry: ${supervisor.list().length}\n`
    );
  } finally {
    // Teardown: kill every shim child, shut down supervisor + ipc, close db.
    for (const c of children) {
      try {
        c.kill("SIGKILL");
      } catch {
        // already gone
      }
    }
    await supervisor.shutdown().catch(() => {});
    await ipc.close().catch(() => {});
    closeInbox(db);
    rmSync(dir, { recursive: true, force: true });
  }

  process.stdout.write(`\n${failures === 0 ? "ALL PASS" : `${failures} FAILURE(S)`}\n`);
  process.exit(failures === 0 ? 0 : 1);
}

if (process.argv.includes("--fake-shim")) {
  runFakeShim();
} else {
  main().catch((e) => {
    process.stderr.write(`driver crashed: ${e instanceof Error ? e.stack : String(e)}\n`);
    process.exit(1);
  });
}
