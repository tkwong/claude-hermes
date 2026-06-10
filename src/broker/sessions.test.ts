/**
 * Session supervisor tests. The tmux spawn is STUBBED (injected via
 * SessionSupervisorOptions.spawn) so no real interactive `claude` is launched —
 * its --dangerously-load-development-channels confirmation prompt is interactive
 * and would block. We assert the spawn argv, the spawning->live promotion via
 * markPong, automatic-recycle backoff/breaker accounting, manual-recycle breaker
 * clearing, read-only mintToken for a live lane, and injective tmux naming.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  createSessionSupervisor,
  sanitizeTmuxName,
  sessionUuidFor,
  type SessionSupervisor,
  type TmuxResult,
} from "./sessions";

interface SpawnCall {
  bin: string;
  args: string[];
  env?: Record<string, string>;
}

function makeSpawn(opts?: {
  failNewSession?: () => boolean;
  liveSessions?: Set<string>;
}): {
  spawn: (bin: string, args: string[], o: { env?: Record<string, string> }) => TmuxResult;
  calls: SpawnCall[];
} {
  const calls: SpawnCall[] = [];
  const live = opts?.liveSessions ?? new Set<string>();
  const spawn = (bin: string, args: string[], o: { env?: Record<string, string> }): TmuxResult => {
    calls.push({ bin, args, env: o.env });
    const sub = args[0];
    if (sub === "has-session") {
      const name = args[args.indexOf("-t") + 1];
      return { status: live.has(name) ? 0 : 1, stdout: "", stderr: "" };
    }
    if (sub === "kill-session") {
      const name = args[args.indexOf("-t") + 1];
      live.delete(name);
      return { status: 0, stdout: "", stderr: "" };
    }
    if (sub === "new-session") {
      if (opts?.failNewSession?.()) {
        return { status: 1, stdout: "", stderr: "boom" };
      }
      const name = args[args.indexOf("-s") + 1];
      live.add(name);
      return { status: 0, stdout: "", stderr: "" };
    }
    if (sub === "display-message") {
      return { status: 0, stdout: "4242\n", stderr: "" };
    }
    return { status: 0, stdout: "", stderr: "" };
  };
  return { spawn, calls };
}

describe("session supervisor (stubbed spawn)", () => {
  let dir: string;
  let registryPath: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "hermes-sessions-test-"));
    registryPath = join(dir, "broker-sessions.json");
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  function makeSup(extra?: Partial<Parameters<typeof createSessionSupervisor>[0]>): {
    sup: SessionSupervisor;
    calls: SpawnCall[];
    live: Set<string>;
  } {
    const live = new Set<string>();
    const { spawn, calls } = makeSpawn({
      liveSessions: live,
      ...(extra as { failNewSession?: () => boolean }),
    });
    const sup = createSessionSupervisor({
      sockPath: join(dir, "broker.sock"),
      registryPath,
      spawn,
      sweepIntervalMs: 0, // no background sweep during tests
      backoffBaseMs: 5,
      backoffMaxMs: 20,
      breakerThreshold: 3,
      connectTimeoutMs: 10_000,
      ...extra,
    });
    return { sup, calls, live };
  }

  test("ensureSession spawns the interactive claude --channels argv with broker env", async () => {
    const { sup, calls } = makeSup();
    await sup.ensureSession("workspace:abc123", "/tmp/projA");
    const newSession = calls.find((c) => c.args[0] === "new-session");
    expect(newSession).toBeDefined();
    const args = newSession?.args ?? [];
    const joined = args.join(" ");
    expect(joined).toContain("--channels");
    expect(joined).toContain("plugin:discord@claude-plugins-official");
    expect(joined).toContain("--dangerously-skip-permissions");
    // The channel only spawns if the plugin is enabled in the lane scope, so the
    // launch must enable it per-lane via --settings (not user/project settings).
    expect(joined).toContain("--settings");
    expect(joined).toContain("enabledPlugins");
    expect(joined).toContain('"discord@claude-plugins-official":true');
    // HERMES_* MUST be injected via tmux `-e` so the pane (not just the tmux
    // client) inherits them — otherwise the shim falls back to POC mode.
    expect(args).toContain("-e");
    expect(args.some((a) => a.startsWith("HERMES_BROKER_SOCK=") && a.includes("broker.sock"))).toBe(true);
    expect(args).toContain("HERMES_SESSION_KEY=workspace:abc123");
    expect(args.some((a) => a.startsWith("HERMES_TOKEN="))).toBe(true);
    // First spawn (no transcript on disk for /tmp/projA) → --session-id with the
    // lane's deterministic uuid, so a later respawn can --resume it.
    expect(joined).toContain("--session-id");
    expect(joined).toContain(sessionUuidFor("workspace:abc123"));
    // The spawn env still carries them too (belt-and-suspenders for a cold server).
    expect(newSession?.env?.HERMES_SESSION_KEY).toBe("workspace:abc123");
    expect(newSession?.env?.HERMES_BROKER_SOCK).toContain("broker.sock");
    expect(typeof newSession?.env?.HERMES_TOKEN).toBe("string");
    await sup.shutdown();
  });

  test("lane stays 'spawning' until markPong promotes it to 'live'", async () => {
    const { sup } = makeSup();
    await sup.ensureSession("workspace:k1", "/tmp/p");
    expect(sup.get("workspace:k1")?.state).toBe("spawning");
    sup.markPong("workspace:k1");
    expect(sup.get("workspace:k1")?.state).toBe("live");
    await sup.shutdown();
  });

  test("ensureSession is warm: a still-spawning lane with live tmux is not respawned", async () => {
    const { sup, calls } = makeSup();
    await sup.ensureSession("workspace:k2", "/tmp/p");
    const firstNew = calls.filter((c) => c.args[0] === "new-session").length;
    await sup.ensureSession("workspace:k2", "/tmp/p");
    const secondNew = calls.filter((c) => c.args[0] === "new-session").length;
    expect(secondNew).toBe(firstNew); // no double-spawn
    await sup.shutdown();
  });

  test("automatic recycle accounts as failure; repeated wedge trips the breaker", async () => {
    const { sup } = makeSup({ breakerThreshold: 3 });
    await sup.ensureSession("workspace:k3", "/tmp/p");
    sup.markPong("workspace:k3");
    expect(sup.get("workspace:k3")?.state).toBe("live");

    // Three automatic recycles → failures hit breakerThreshold → breaker-open.
    await sup.recycleSession("workspace:k3", "wedged 1");
    await sup.recycleSession("workspace:k3", "wedged 2");
    await sup.recycleSession("workspace:k3", "wedged 3");
    const entry = sup.get("workspace:k3");
    expect(entry?.failures).toBeGreaterThanOrEqual(3);
    expect(entry?.state).toBe("breaker-open");
    await sup.shutdown();
  });

  test("breaker-open lane is NOT auto-respawned by ensureSession", async () => {
    const { sup, calls } = makeSup({ breakerThreshold: 1 });
    await sup.ensureSession("workspace:k4", "/tmp/p");
    sup.markPong("workspace:k4");
    await sup.recycleSession("workspace:k4", "wedged"); // breakerThreshold=1 → opens
    expect(sup.get("workspace:k4")?.state).toBe("breaker-open");
    const before = calls.filter((c) => c.args[0] === "new-session").length;
    await sup.ensureSession("workspace:k4", "/tmp/p");
    const after = calls.filter((c) => c.args[0] === "new-session").length;
    expect(after).toBe(before); // held lane, no respawn
    await sup.shutdown();
  });

  test("manual recycle clears the breaker and respawns immediately", async () => {
    const { sup, calls } = makeSup({ breakerThreshold: 1 });
    await sup.ensureSession("workspace:k5", "/tmp/p");
    sup.markPong("workspace:k5");
    await sup.recycleSession("workspace:k5", "wedged"); // opens breaker
    expect(sup.get("workspace:k5")?.state).toBe("breaker-open");
    const before = calls.filter((c) => c.args[0] === "new-session").length;
    await sup.recycleSession("workspace:k5", "operator", true); // manual
    const after = calls.filter((c) => c.args[0] === "new-session").length;
    expect(after).toBeGreaterThan(before); // respawned
    expect(sup.get("workspace:k5")?.failures).toBe(0);
    await sup.shutdown();
  });

  test("mintToken is read-only for a live lane (does not rotate the running shim's token)", async () => {
    const { sup } = makeSup();
    await sup.ensureSession("workspace:k6", "/tmp/p");
    sup.markPong("workspace:k6");
    const t1 = sup.get("workspace:k6")?.token;
    const t2 = sup.mintToken("workspace:k6");
    expect(t2).toBe(t1 ?? "");
    expect(sup.verifyToken("workspace:k6", t1 ?? "")).toBe(true);
    await sup.shutdown();
  });

  test("sanitizeTmuxName is injective and tmux-legal (no ':')", () => {
    const a = sanitizeTmuxName("workspace:abc123abc123");
    const b = sanitizeTmuxName("thread:discord:123456789");
    const c = sanitizeTmuxName("thread:discord:123456789"); // same input → same name
    expect(a).not.toContain(":");
    expect(b).not.toContain(":");
    expect(a).not.toBe(b);
    expect(b).toBe(c);
    expect(a.startsWith("hermes-")).toBe(true);
  });

  test("verifyToken rejects an unknown lane and a wrong token", async () => {
    const { sup } = makeSup();
    expect(sup.verifyToken("nope", "x")).toBe(false);
    await sup.ensureSession("workspace:k7", "/tmp/p");
    const t = sup.get("workspace:k7")?.token ?? "";
    expect(sup.verifyToken("workspace:k7", t)).toBe(true);
    expect(sup.verifyToken("workspace:k7", `${t}x`)).toBe(false);
    await sup.shutdown();
  });

  test("sessionUuidFor is deterministic, distinct per key, and v4-shaped", () => {
    const a = sessionUuidFor("workspace:abc");
    const b = sessionUuidFor("workspace:abc");
    const c = sessionUuidFor("thread:discord:999");
    expect(a).toBe(b); // stable across calls → same lane resumes the same session
    expect(a).not.toBe(c);
    expect(a).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  });

  test("idle-reap kills an inactive lane (tmux + registry); next inbound cold-starts it", async () => {
    let clock = 1_000_000;
    const { sup, live } = makeSup({
      now: () => clock,
      idleReapMs: 1000,
      sweepIntervalMs: 15, // enable the sweep (default in makeSup is 0)
    });
    await sup.ensureSession("workspace:idle", "/tmp/p");
    sup.markPong("workspace:idle");
    const tmuxName = sup.get("workspace:idle")?.tmuxName ?? "";
    expect(sup.get("workspace:idle")?.state).toBe("live");
    expect(live.has(tmuxName)).toBe(true);

    // A heartbeat pong must NOT count as activity (else idle never trips).
    clock += 2000;
    sup.markPong("workspace:idle");
    // Let a sweep tick run with the clock past idleReapMs.
    await new Promise((r) => setTimeout(r, 60));
    expect(sup.get("workspace:idle")).toBeUndefined(); // reaped from registry
    expect(live.has(tmuxName)).toBe(false); // tmux killed

    // Next inbound cold-starts a fresh lane (reap is invisible to the user).
    await sup.ensureSession("workspace:idle", "/tmp/p");
    expect(sup.get("workspace:idle")?.state).toBe("spawning");
    await sup.shutdown();
  });
});
