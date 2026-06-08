/**
 * Broker IPC verification via a FAKE shim (no real interactive `claude`).
 *
 * The fake shim is a tiny AF_UNIX client that speaks the same length-prefixed
 * uint32be + JSON codec as src/shim/channel-shim.ts. It lets us assert the
 * broker's hello/replay/ack/reply/heartbeat contract — and the security +
 * concurrency fixes from the review — without launching a real claude (whose
 * --dangerously-load-development-channels confirmation prompt is interactive).
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { connect, type Socket } from "node:net";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openInbox, type InboxMeta } from "./inbox";
import { startBrokerIpc, frame, createFrameDecoder, type BrokerIpc } from "./ipc";
import type { Database } from "bun:sqlite";

interface Recorded {
  sessionKey: string;
  chatId: string;
  text: string;
}

function metaFor(sessionKey: string, msgId: string): InboxMeta {
  return {
    chat_id: "chan-A",
    message_id: msgId,
    user: "tester",
    user_id: "u1",
    ts: new Date().toISOString(),
    cwd: "/tmp/projA",
  };
}

/** Minimal fake shim: connects, frames JSON, exposes received frames + helpers. */
class FakeShim {
  socket: Socket;
  frames: Record<string, unknown>[] = [];
  private decode: (chunk: Buffer) => void;
  private waiters: Array<{
    pred: (m: Record<string, unknown>) => boolean;
    resolve: (m: Record<string, unknown>) => void;
  }> = [];

  connected: Promise<void>;

  constructor(sockPath: string) {
    this.decode = createFrameDecoder((m) => {
      this.frames.push(m);
      this.waiters = this.waiters.filter((w) => {
        if (w.pred(m)) {
          w.resolve(m);
          return false;
        }
        return true;
      });
    });
    this.socket = connect(sockPath);
    this.connected = new Promise<void>((resolve) => {
      this.socket.once("connect", () => resolve());
    });
    this.socket.on("data", (d: Buffer) => this.decode(d));
    this.socket.on("error", () => {});
  }

  async hello(sessionKey: string, tok: string, pid: number): Promise<void> {
    await this.connected;
    this.send({ type: "hello", sessionKey, token: tok, shimPid: pid });
  }

  send(obj: object): void {
    this.socket.write(frame(obj));
  }

  waitFor(pred: (m: Record<string, unknown>) => boolean, timeoutMs = 2000): Promise<Record<string, unknown>> {
    const existing = this.frames.find(pred);
    if (existing) return Promise.resolve(existing);
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("waitFor timeout")), timeoutMs);
      this.waiters.push({
        pred,
        resolve: (m) => {
          clearTimeout(t);
          resolve(m);
        },
      });
    });
  }

  close(): void {
    try {
      this.socket.destroy();
    } catch {
      // ignore
    }
  }
}

describe("broker ipc (fake shim)", () => {
  let dir: string;
  let db: Database;
  let ipc: BrokerIpc;
  let sockPath: string;
  let replies: Recorded[];
  let token: string;
  const ensured: string[] = [];
  const recycled: string[] = [];

  beforeEach(async () => {
    dir = mkdtempSync(join(tmpdir(), "hermes-ipc-test-"));
    db = openInbox(":memory:");
    sockPath = join(dir, "broker.sock");
    replies = [];
    ensured.length = 0;
    recycled.length = 0;
    token = "valid-token-123";
    ipc = await startBrokerIpc({
      sockPath,
      inboxDb: db,
      onReply: async (r) => {
        replies.push({ sessionKey: r.sessionKey, chatId: r.chatId, text: r.text });
      },
      ensureSession: async (sk) => {
        ensured.push(sk);
      },
      recycleSession: async (sk) => {
        recycled.push(sk);
      },
      verifyToken: (_sk, t) => t === token,
      pingIntervalMs: 300,
      maxMissedPongs: 2,
    });
  });

  afterEach(async () => {
    await ipc.close().catch(() => {});
    db.close();
    rmSync(dir, { recursive: true, force: true });
  });

  test("hello → hello_ack, inbound push, inbound_ack resolves awaitDelivered, markDelivered", async () => {
    const shim = new FakeShim(sockPath);
    await shim.hello("s1", token, 1);
    await shim.waitFor((m) => m.type === "hello_ack");

    const delivered = ipc.awaitDelivered("s1", "m1", 2000);
    const rec = await ipc.sendInbound("s1", "hello prompt", metaFor("s1", "m1"));
    expect(rec.inserted).toBe(true);

    const inbound = await shim.waitFor((m) => m.type === "inbound");
    expect(inbound.content).toBe("hello prompt");
    expect(inbound.seq).toBe(rec.seq);

    shim.send({ type: "inbound_ack", sessionKey: "s1", seq: inbound.seq });
    await delivered; // must resolve (no 30s hang)
    shim.close();
  });

  test("reply with final:true routes to onReply, acks, marks answered (drops from replay)", async () => {
    const shim = new FakeShim(sockPath);
    await shim.hello("s2", token, 2);
    await shim.waitFor((m) => m.type === "hello_ack");
    const rec = await ipc.sendInbound("s2", "p", metaFor("s2", "m2"));
    const inbound = await shim.waitFor((m) => m.type === "inbound");
    shim.send({ type: "inbound_ack", sessionKey: "s2", seq: inbound.seq });

    shim.send({
      type: "reply",
      sessionKey: "s2",
      rpcId: "r1",
      chat_id: "chan-A",
      text: "the answer",
      seq: rec.seq,
      final: true,
    });
    const ack = await shim.waitFor((m) => m.type === "reply_ack" && m.rpcId === "r1");
    expect(ack.ok).toBe(true);
    expect(replies).toHaveLength(1);
    expect(replies[0].text).toBe("the answer");

    // Answered row must NOT replay on reconnect.
    const shim2 = new FakeShim(sockPath);
    await shim2.hello("s2", token, 22);
    const ack2 = await shim2.waitFor((m) => m.type === "hello_ack");
    expect(ack2.replayCount).toBe(0);
    shim.close();
    shim2.close();
  });

  test("progress reply (no seq / final:false) does NOT mark answered → row still replays", async () => {
    const shim = new FakeShim(sockPath);
    await shim.hello("s3", token, 3);
    await shim.waitFor((m) => m.type === "hello_ack");
    await ipc.sendInbound("s3", "p", metaFor("s3", "m3"));
    const inbound = await shim.waitFor((m) => m.type === "inbound");
    shim.send({ type: "inbound_ack", sessionKey: "s3", seq: inbound.seq });

    // A progress reply carrying NO seq must leave the row open (delivered).
    shim.send({ type: "reply", sessionKey: "s3", rpcId: "rp", chat_id: "chan-A", text: "working..." });
    await shim.waitFor((m) => m.type === "reply_ack" && m.rpcId === "rp");
    shim.close();

    // Reconnect: delivered-not-answered row must replay.
    const shim2 = new FakeShim(sockPath);
    await shim2.hello("s3", token, 33);
    const ack2 = await shim2.waitFor((m) => m.type === "hello_ack");
    expect(ack2.replayCount).toBe(1);
    shim2.close();
  });

  test("SECURITY: unauthenticated socket (no hello) cannot reply or ack", async () => {
    // Pre-stage a row + a waiter for a victim lane.
    const delivered = ipc.awaitDelivered("victim", "mv", 800);
    await ipc.sendInbound("victim", "p", metaFor("victim", "mv"));

    const attacker = new FakeShim(sockPath);
    await new Promise((r) => setTimeout(r, 50));
    // Forge a reply for the victim WITHOUT ever sending hello.
    attacker.send({ type: "reply", sessionKey: "victim", rpcId: "x", chat_id: "chan-A", text: "exfil" });
    // Forge an inbound_ack for the victim WITHOUT hello.
    attacker.send({ type: "inbound_ack", sessionKey: "victim", seq: 1 });
    await new Promise((r) => setTimeout(r, 150));

    expect(replies).toHaveLength(0); // egress NOT fired for the forged reply
    // The victim's awaitDelivered must NOT have been resolved by the forged ack.
    let resolved = false;
    void delivered
      .then(() => {
        resolved = true;
      })
      .catch(() => {});
    await new Promise((r) => setTimeout(r, 50));
    expect(resolved).toBe(false);
    attacker.close();
    await delivered.catch(() => {}); // let it time out cleanly
  });

  test("SECURITY: hello with bad token closes the socket", async () => {
    const shim = new FakeShim(sockPath);
    let closed = false;
    shim.socket.on("close", () => {
      closed = true;
    });
    shim.send({ type: "hello", sessionKey: "s4", token: "WRONG", shimPid: 4 });
    await new Promise((r) => setTimeout(r, 150));
    expect(closed).toBe(true);
  });

  test("dedup: same discordMsgId is a no-op (inserted:false, no second push)", async () => {
    const shim = new FakeShim(sockPath);
    await shim.hello("s5", token, 5);
    await shim.waitFor((m) => m.type === "hello_ack");
    const r1 = await ipc.sendInbound("s5", "p", metaFor("s5", "dup"));
    expect(r1.inserted).toBe(true);
    await shim.waitFor((m) => m.type === "inbound");
    const before = shim.frames.filter((m) => m.type === "inbound").length;
    const r2 = await ipc.sendInbound("s5", "p", metaFor("s5", "dup"));
    expect(r2.inserted).toBe(false);
    await new Promise((r) => setTimeout(r, 100));
    const after = shim.frames.filter((m) => m.type === "inbound").length;
    expect(after).toBe(before); // no second push for the dup
    shim.close();
  });

  test("awaitDelivered fast-resolves when row already delivered (warm-fast race)", async () => {
    const shim = new FakeShim(sockPath);
    await shim.hello("s6", token, 6);
    await shim.waitFor((m) => m.type === "hello_ack");
    const rec = await ipc.sendInbound("s6", "p", metaFor("s6", "m6"));
    const inbound = await shim.waitFor((m) => m.type === "inbound");
    // Ack BEFORE registering the waiter (warm shim acks first).
    shim.send({ type: "inbound_ack", sessionKey: "s6", seq: inbound.seq });
    await new Promise((r) => setTimeout(r, 80));
    void rec;
    // Now await — must resolve immediately from the DB state, not hang.
    await ipc.awaitDelivered("s6", "m6", 500);
    shim.close();
  });

  test("heartbeat: ping is sent; pong keeps lane alive; silence recycles", async () => {
    // threshold = maxMissedPongs(2) * pingIntervalMs(300) = 600ms of silence.
    const shim = new FakeShim(sockPath);
    await shim.hello("s7", token, 7);
    await shim.waitFor((m) => m.type === "hello_ack");
    // Receive a ping and answer it → lastPongAt refreshed, no recycle yet.
    const ping = await shim.waitFor((m) => m.type === "ping", 2000);
    shim.send({ type: "pong", sessionKey: "s7", ts: ping.ts });
    await new Promise((r) => setTimeout(r, 200));
    expect(recycled).not.toContain("s7");

    // Stop answering → after > threshold of silence, the lane is recycled.
    await new Promise((r) => setTimeout(r, 1100));
    expect(recycled).toContain("s7");
    shim.close();
  });
});
