/**
 * Per-session supervisor + in-memory session registry.
 *
 * One tmux session per `sessionKey` runs a long-running INTERACTIVE
 * `claude --dangerously-load-development-channels --channels <shim abs path>`
 * in the resolved project cwd, with HERMES_BROKER_SOCK / HERMES_SESSION_KEY /
 * HERMES_TOKEN injected so the per-session channel-shim (running inside that
 * claude child) can dial back to the broker over AF_UNIX.
 *
 * Stability invariants (broker-design.md §4):
 *   - Per-session failure isolation: recycleSession kills + respawns exactly ONE
 *     lane (`tmux kill-session`). It never touches any other lane.
 *   - Capped exponential backoff (1s -> 30s), failure counter reset after the
 *     lane has been `live` for `stableResetMs`.
 *   - Circuit breaker: after `breakerThreshold` rapid failures the lane goes
 *     `breaker-open` and stops auto-respawning; the operator is alerted and must
 *     manually recycle. The lane is held open (registry entry kept), not dropped.
 *   - NEVER kill a session to manage context. Recycles are for genuine crashes /
 *     wedged-heartbeat lanes only — driven by markPong + the IPC heartbeat, never
 *     by scraping tmux/PTY output.
 *   - Registry persisted via atomic write-then-rename (same pattern as
 *     runtime/daemon-registry.ts) so a crash mid-write leaves the file intact.
 *
 * This module MUST NOT import src/commands/discord.ts or src/broker/ipc.ts
 * (beyond types) — bring-up order in start.ts wires the supervisor's methods
 * into startBrokerIpc() as plain callbacks, breaking any cycle.
 */

import { spawnSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { copyFileSync, existsSync, readdirSync, readFileSync } from "node:fs";
import { mkdir, rename, unlink, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { hermesDir } from "../paths";

export type SessionState = "spawning" | "live" | "backoff" | "breaker-open" | "dead";

export interface SessionEntry {
  sessionKey: string;
  cwd: string;
  tmuxName: string;
  token: string;
  state: SessionState;
  pid?: number;
  bootAt: number;
  lastPongAt: number;
  failures: number;
  nextBackoffMs: number;
  /**
   * Last USER activity (an inbound for this lane via ensureSession). NOT bumped by
   * heartbeat pongs — those prove the lane is alive, not that it's still wanted.
   * The idle-reap sweep kills a lane whose lastActivityAt is older than idleReapMs
   * so abandoned conversations don't leak a claude process forever. A reaped lane
   * cold-starts again on the next inbound (ensureSession), so this is invisible.
   */
  lastActivityAt: number;
}

/**
 * Result of a tmux invocation. Mirrors the subset of
 * `child_process.SpawnSyncReturns` the supervisor inspects, so the spawn hook
 * can be stubbed in tests without pulling in node:child_process.
 */
export interface TmuxResult {
  status: number | null;
  stdout: string;
  stderr: string;
}

export interface SessionSupervisorOptions {
  /** Absolute path to the channel-shim entry. Defaults to src/shim/channel-shim.ts under this repo. */
  shimPath?: string;
  /** Broker AF_UNIX socket path (from brokerSockPath()); injected into the lane env as HERMES_BROKER_SOCK. */
  sockPath: string;
  backoffBaseMs?: number;
  backoffMaxMs?: number;
  stableResetMs?: number;
  breakerThreshold?: number;
  /**
   * Grace period after a successful tmux spawn for the shim to say hello before
   * the lane is treated as a boot failure (claude crashed / wedged at a prompt).
   * Default 30s — long enough for a cold claude + MCP handshake.
   */
  connectTimeoutMs?: number;
  /**
   * Cadence for the supervisor's own liveness sweep: recycles a lane that is
   * 'live' (or 'spawning') but whose tmux session has died with no traffic to
   * notice it (idle-lane-died hole the IPC heartbeat can't see — it only pings
   * CONNECTED shims). Default = 4x backoffBaseMs floored at 15s. 0 disables.
   */
  sweepIntervalMs?: number;
  /**
   * Reap (kill tmux + drop the registry entry) a lane with no USER activity (no
   * inbound) for this long. Reclaims abandoned-conversation lanes so a long-lived
   * daemon doesn't accumulate idle claude processes. A reaped lane cold-starts on
   * the next inbound, so reaping is invisible to the user. Checked on the sweep
   * cadence. Default 1 hour; 0 disables. NOT bumped by heartbeat pongs.
   */
  idleReapMs?: number;
  onAlert?: (sessionKey: string, msg: string) => void;
  /** Absolute path to the tmux binary. Defaults to /opt/homebrew/bin/tmux. */
  tmuxBin?: string;
  /**
   * Path to the `claude` binary the lane runs. Defaults to "claude" (resolved
   * via PATH inside tmux). Exposed mainly so tests can point at a fake.
   */
  claudeBin?: string;
  /**
   * Allowlisted channel plugin id the lane launches as `--channels plugin:<id>`.
   * Its server.ts is masqueraded to our channel-shim by installChannelShim().
   * Default "discord@claude-plugins-official". Using an allowlisted name avoids
   * the interactive --dangerously-load-development-channels confirmation prompt.
   */
  channelPlugin?: string;
  /**
   * Override the on-disk registry path (tests inject a tmpdir). Falls back to
   * $HERMES_SESSION_REGISTRY, then sessionRegistryFile().
   */
  registryPath?: string;
  /**
   * Injectable tmux runner — defaults to a real spawnSync of the tmux binary.
   * Tests stub this to assert the spawn argv without launching a real claude
   * (whose --dangerously-load-development-channels confirmation prompt is
   * interactive and would block).
   */
  spawn?: (bin: string, args: string[], opts: { cwd?: string; env?: Record<string, string> }) => TmuxResult;
  /** Clock injection for deterministic backoff/stability tests. Defaults to Date.now. */
  now?: () => number;
}

export interface SessionSupervisor {
  ensureSession(sessionKey: string, cwd: string): Promise<void>;
  /**
   * Recycle a lane. `manual=true` (operator `/discord recycle`) clears the
   * breaker + backoff and respawns immediately; the default (automatic,
   * heartbeat-driven) accounts the recycle as a failure (backoff + breaker).
   */
  recycleSession(sessionKey: string, reason: string, manual?: boolean): Promise<void>;
  mintToken(sessionKey: string): string;
  verifyToken(sessionKey: string, token: string): boolean;
  markPong(sessionKey: string): void;
  get(sessionKey: string): SessionEntry | undefined;
  list(): SessionEntry[];
  shutdown(): Promise<void>;
}

const DEFAULT_BACKOFF_BASE_MS = 1000;
const DEFAULT_BACKOFF_MAX_MS = 30_000;
const DEFAULT_STABLE_RESET_MS = 60_000;
const DEFAULT_BREAKER_THRESHOLD = 5;
const DEFAULT_CONNECT_TIMEOUT_MS = 30_000;
const DEFAULT_SWEEP_INTERVAL_MS = 30_000;
const DEFAULT_IDLE_REAP_MS = 60 * 60_000; // 1 hour of no user activity → reap
const DEFAULT_TMUX_BIN = "/opt/homebrew/bin/tmux";

/**
 * On-disk path for the persisted session registry. Deliberately distinct from
 * `daemons.json` (daemon-registry.ts) and `sessions.json` (thread sessions) to
 * avoid clobbering either.
 */
export function sessionRegistryFile(cwd?: string): string {
  return join(hermesDir(cwd ?? process.cwd()), "broker-sessions.json");
}

/**
 * tmux session names cannot contain ':' (it is the window/pane addressing
 * separator in `tmux -s`). We map ':' -> '-', prefix `hermes-`, and append a
 * short hash of the RAW sessionKey. The readable part stays human-friendly for
 * `tmux attach`, while the hash suffix makes the mapping provably injective for
 * ANY key shape — it no longer relies on the source alphabet never containing a
 * '-' (e.g. a future source label like "x-y" would otherwise collide with a
 * ':'->'-' substitution). The reverse map is kept in SessionEntry.tmuxName.
 */
export function sanitizeTmuxName(sessionKey: string): string {
  const safe = sessionKey.replace(/[^A-Za-z0-9_-]/g, "-");
  const tag = createHash("sha256").update(sessionKey).digest("hex").slice(0, 8);
  return `hermes-${safe}-${tag}`;
}

function defaultShimPath(): string {
  // sessions.ts lives at src/broker/sessions.ts; the shim at src/shim/channel-shim.ts.
  return join(dirname(import.meta.dir), "shim", "channel-shim.ts");
}

/**
 * Masquerade install: overwrite the allowlisted channel plugin's `server.ts`
 * with our channel-shim so `claude --channels plugin:<pluginId>` runs the shim
 * under an allowlisted name — no interactive --dangerously-load-development-
 * channels confirmation prompt (which would wedge a headless lane). Idempotent;
 * call at broker bring-up so the masqueraded shim tracks the repo copy. Backs up
 * the vendor original ONCE to `server.ts.orig-backup`. Returns the installed
 * path, or null if the plugin isn't cached on this machine.
 */
export function installChannelShim(
  shimPath: string = defaultShimPath(),
  pluginId = "discord@claude-plugins-official",
  home: string = homedir(),
): string | null {
  const [name, marketplace] = pluginId.split("@");
  if (!name || !marketplace) return null;
  const base = join(home, ".claude", "plugins", "cache", marketplace, name);
  if (!existsSync(base)) return null;
  // Highest version dir that actually ships a server.ts.
  const versions = readdirSync(base)
    .filter((v) => existsSync(join(base, v, "server.ts")))
    .sort();
  const ver = versions[versions.length - 1];
  if (!ver) return null;
  const target = join(base, ver, "server.ts");
  const backup = `${target}.orig-backup`;
  if (!existsSync(backup)) copyFileSync(target, backup);
  copyFileSync(shimPath, target);
  return target;
}

function realSpawn(
  bin: string,
  args: string[],
  opts: { cwd?: string; env?: Record<string, string> },
): TmuxResult {
  const r = spawnSync(bin, args, {
    cwd: opts.cwd,
    env: opts.env,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
  return {
    status: r.status,
    stdout: r.stdout ?? "",
    stderr: r.stderr ?? "",
  };
}

export function createSessionSupervisor(opts: SessionSupervisorOptions): SessionSupervisor {
  const shimPath = opts.shimPath ?? defaultShimPath();
  const sockPath = opts.sockPath;
  const backoffBaseMs = opts.backoffBaseMs ?? DEFAULT_BACKOFF_BASE_MS;
  const backoffMaxMs = opts.backoffMaxMs ?? DEFAULT_BACKOFF_MAX_MS;
  const stableResetMs = opts.stableResetMs ?? DEFAULT_STABLE_RESET_MS;
  const breakerThreshold = opts.breakerThreshold ?? DEFAULT_BREAKER_THRESHOLD;
  const connectTimeoutMs = opts.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
  const sweepIntervalMs = opts.sweepIntervalMs ?? DEFAULT_SWEEP_INTERVAL_MS;
  // Idle-reap: kill a lane with no user activity for this long (0 = disabled).
  const idleReapMs = opts.idleReapMs ?? DEFAULT_IDLE_REAP_MS;
  const tmuxBin = opts.tmuxBin ?? DEFAULT_TMUX_BIN;
  const claudeBin = opts.claudeBin ?? "claude";
  const channelPlugin = opts.channelPlugin ?? "discord@claude-plugins-official";
  const spawn = opts.spawn ?? realSpawn;
  const now = opts.now ?? (() => Date.now());
  const onAlert = opts.onAlert;
  const registryPath = opts.registryPath ?? process.env.HERMES_SESSION_REGISTRY ?? sessionRegistryFile();

  const registry = new Map<string, SessionEntry>();
  // Serialize ensure/recycle per sessionKey so concurrent inbound for the same
  // lane can't double-spawn or interleave a kill with a respawn. Distinct keys
  // run fully in parallel (per-session isolation).
  const laneChains = new Map<string, Promise<void>>();
  // Serialize on-disk registry writes (in-flight promise chain), same idea as
  // daemon-registry.ts. Cross-process races are out of scope (one broker owns it).
  let writeChain: Promise<void> = Promise.resolve();
  let shuttingDown = false;

  function log(msg: string): void {
    console.error(`[${new Date().toLocaleTimeString()}] [broker/sessions] ${msg}`);
  }

  /**
   * Crash-recovery reconcile (runs once at construction). The persisted registry
   * is otherwise WRITE-ONLY: after a broker crash/restart, previously-spawned
   * tmux sessions keep running with tokens this fresh supervisor doesn't know
   * (the sock was unlinked + re-bound, so those orphan shims reconnect-loop on
   * verifyToken forever). We can't adopt them (token unrecoverable), so we KILL
   * the orphans here and start clean. This also prevents the next inbound from
   * cold-starting a SECOND claude for the same key (double-billing). We do NOT
   * repopulate the in-memory registry from disk — a clean cold-start per key is
   * the safe path; the file is then overwritten on the next persist().
   */
  function reconcileOnBoot(): void {
    let parsed: { sessions?: Array<{ tmuxName?: string; sessionKey?: string }> } | null = null;
    try {
      const text = readFileSync(registryPath, "utf8");
      parsed = JSON.parse(text);
    } catch {
      return; // no prior registry (fresh boot) — nothing to reconcile
    }
    const sessions = Array.isArray(parsed?.sessions) ? parsed.sessions : [];
    let killed = 0;
    for (const s of sessions) {
      const tmuxName = typeof s?.tmuxName === "string" ? s.tmuxName : undefined;
      if (!tmuxName) continue;
      if (tmuxHasSession(tmuxName)) {
        log(`reconcile: killing orphan tmux ${tmuxName} (stale token from prior broker)`);
        tmuxKillSession(tmuxName);
        killed += 1;
      }
    }
    if (sessions.length > 0) {
      log(`reconcile: ${sessions.length} persisted lane(s), ${killed} orphan tmux killed`);
    }
  }

  function enqueueLane(sessionKey: string, fn: () => Promise<void>): Promise<void> {
    const prev = laneChains.get(sessionKey) ?? Promise.resolve();
    const next = prev.then(fn, fn); // run regardless of a prior lane op's outcome
    // Keep the chain alive but swallow rejections so one failure doesn't poison
    // the lane's future ops (log-and-keep-serving).
    laneChains.set(
      sessionKey,
      next.catch(() => undefined),
    );
    return next;
  }

  function persist(): Promise<void> {
    const snapshot = Array.from(registry.values()).map((e) => ({ ...e }));
    const next = writeChain.then(() => writeRegistry(registryPath, snapshot));
    writeChain = next.catch((err) => {
      log(`registry persist failed: ${err instanceof Error ? err.message : String(err)}`);
    });
    return next;
  }

  function tmux(args: string[], cwd?: string, env?: Record<string, string>): TmuxResult {
    try {
      return spawn(tmuxBin, args, { cwd, env });
    } catch (err) {
      // log-and-keep-serving: a spawn-layer throw becomes a non-zero status so
      // callers treat it as a failed tmux op rather than crashing the broker.
      return { status: -1, stdout: "", stderr: err instanceof Error ? err.message : String(err) };
    }
  }

  function tmuxHasSession(tmuxName: string): boolean {
    const r = tmux(["has-session", "-t", tmuxName]);
    return r.status === 0;
  }

  function tmuxKillSession(tmuxName: string): void {
    // Best-effort: a missing session returns non-zero, which is fine for reap.
    tmux(["kill-session", "-t", tmuxName]);
  }

  /**
   * Launch one detached tmux session running the interactive claude REPL with
   * the channel-shim and the broker env injected. Returns the tmux pane pid on
   * success (best-effort), or undefined.
   */
  function tmuxSpawnLane(entry: SessionEntry): { ok: boolean; pid?: number; stderr: string } {
    const childEnv: Record<string, string> = {
      ...process.env,
      HERMES_BROKER_SOCK: sockPath,
      HERMES_SESSION_KEY: entry.sessionKey,
      HERMES_TOKEN: entry.token,
    } as Record<string, string>;

    // `claude --channels plugin:<channelPlugin> --dangerously-skip-permissions`
    // run inside tmux in the lane's cwd. We launch via the ALLOWLISTED official
    // plugin name (whose server.ts is masqueraded to our shim by
    // installChannelShim) so there is NO interactive
    // `--dangerously-load-development-channels` confirmation prompt to wedge a
    // headless lane (verified: the raw `--channels <path>` dev form makes claude
    // exit immediately). tmux keeps the PTY so the user can `tmux attach -t
    // <tmuxName>`. We do NOT redirect/scrape its output. shimPath is consumed by
    // installChannelShim (the masquerade install), not the launch argv.
    void shimPath;
    // `--channels plugin:X` only ACTIVATES the channel of an ALREADY-ENABLED
    // plugin — it does NOT enable the plugin. If the masqueraded discord plugin
    // isn't enabled in this lane's session scope, claude never spawns its MCP
    // server (verified: debug log shows "Loaded plugins - Enabled: N" with no
    // discord entry, yet the cosmetic "Channels (experimental) … inject directly"
    // banner still prints). So we enable it PER-LANE via --settings (a JSON
    // override that does NOT touch the user's or project settings.json), which is
    // what makes claude actually spawn our masqueraded server.ts → broker hello.
    const enableSettings = JSON.stringify({ enabledPlugins: { [channelPlugin]: true } });
    // A lane is a HEADLESS interactive session — nobody is at the tmux keyboard.
    // AskUserQuestion would render a selection menu and block the turn forever (no
    // one can answer), so the lane wedges and the user's message never gets a
    // reply. Two guards: (1) hard-remove the tool so it can't be called, and (2) a
    // behavioural nudge telling Claude to ask via the channel `reply` tool (or take
    // the safe default) instead of stopping at an interactive prompt.
    const noTtyPrompt =
      "You are running as a headless, no-TTY daemon session driven over a chat channel — " +
      "there is NO human at this terminal. NEVER call the AskUserQuestion tool: it renders an " +
      "interactive selector that will hang this session forever because nobody can answer it. " +
      "When you need a decision, ASK the user via the `reply` tool (send your question to the " +
      "chat and wait for their next message) or proceed with the safest / recommended default. " +
      "Never block on an interactive prompt. " +
      "FEEDBACK: the user only sees a 'typing…' dot while you work. If a turn will take more than " +
      "a few seconds (reading files, running commands, investigating), call the `progress` tool " +
      "FIRST with a short status line (e.g. progress('🔍 睇緊 memory…')) and update it as you move " +
      "to each new step, so the user can see what you're doing. It edits one status message (no " +
      "spam) and is auto-removed when you send your final `reply`.";
    // Persistent context across reap/restart: each lane maps to a DETERMINISTIC
    // claude session id (derived from its sessionKey). First spawn creates it with
    // --session-id; a respawn (after idle-reap, crash, or daemon restart) finds the
    // transcript on disk and --resume's it, so the conversation survives — a reaped
    // lane cold-starts WITH its history instead of as a blank slate. (--session-id
    // on an existing id errors "already in use", hence the file-existence switch.)
    const sessionId = sessionUuidFor(entry.sessionKey);
    const resumeArgs = claudeSessionExists(entry.cwd, sessionId)
      ? ["--resume", sessionId]
      : ["--session-id", sessionId];
    const claudeCmd = [
      claudeBin,
      "--channels",
      `plugin:${channelPlugin}`,
      ...resumeArgs,
      "--settings",
      enableSettings,
      "--disallowed-tools",
      "AskUserQuestion",
      "--append-system-prompt",
      noTtyPrompt,
      "--dangerously-skip-permissions",
    ]
      .map(shellQuote)
      .join(" ");

    // HERMES_* MUST be injected via tmux `-e` (per-session environment). Passing
    // them only through the spawn env reaches the tmux CLIENT process, not the
    // pane: when a tmux server is already running, it hands new panes the SERVER's
    // environment (refreshed only for update-environment vars like DISPLAY/SSH_*),
    // so arbitrary HERMES_* would be dropped — the shim then sees no SOCK, falls
    // back to POC self-inject mode, and never reaches the broker (the connect
    // timeout we observed). `-e` sets them on the session so the pane inherits
    // them (verified live: env_has_SOCK=true inside the spawned plugin subprocess).
    const r = tmux(
      [
        "new-session",
        "-d",
        "-s",
        entry.tmuxName,
        "-c",
        entry.cwd,
        "-e",
        `HERMES_BROKER_SOCK=${sockPath}`,
        "-e",
        `HERMES_SESSION_KEY=${entry.sessionKey}`,
        "-e",
        `HERMES_TOKEN=${entry.token}`,
        claudeCmd,
      ],
      entry.cwd,
      childEnv,
    );
    if (r.status !== 0) {
      return { ok: false, stderr: r.stderr || `tmux new-session exited ${r.status}` };
    }
    // Best-effort pane pid for orphan diagnostics; failure is non-fatal.
    const pidRes = tmux(["display-message", "-p", "-t", entry.tmuxName, "#{pane_pid}"]);
    const pid = pidRes.status === 0 ? Number.parseInt(pidRes.stdout.trim(), 10) : Number.NaN;
    return { ok: true, pid: Number.isFinite(pid) ? pid : undefined, stderr: "" };
  }

  function newEntry(sessionKey: string, cwd: string): SessionEntry {
    return {
      sessionKey,
      cwd,
      tmuxName: sanitizeTmuxName(sessionKey),
      token: mintTokenValue(),
      state: "spawning",
      pid: undefined,
      bootAt: now(),
      lastPongAt: now(),
      failures: 0,
      nextBackoffMs: backoffBaseMs,
      lastActivityAt: now(),
    };
  }

  /**
   * Attempt one spawn for an existing registry entry, transitioning its state.
   * A successful `tmux new-session` only means tmux LAUNCHED claude — claude may
   * still crash on boot or wedge at the dev-channels confirmation prompt. So we
   * leave the lane in 'spawning' (NOT 'live') until the shim's hello/pong proves
   * the MCP handshake completed (markPong promotes spawning->live). A connect
   * watchdog recycles the lane if no hello arrives within connectTimeoutMs, so a
   * boot-hang is retried/breaker'd instead of masquerading as live forever.
   * On a tmux-layer failure -> backoff bookkeeping + breaker check.
   */
  function trySpawn(entry: SessionEntry): boolean {
    entry.state = "spawning";
    entry.bootAt = now();
    const res = tmuxSpawnLane(entry);
    if (res.ok) {
      entry.state = "spawning"; // promoted to 'live' on first hello/pong
      entry.pid = res.pid;
      entry.lastPongAt = now();
      log(`lane spawning: ${entry.sessionKey} (tmux ${entry.tmuxName}, pid ${entry.pid ?? "?"})`);
      armConnectWatchdog(entry);
      return true;
    }
    recordFailure(entry, res.stderr);
    return false;
  }

  /**
   * If a freshly-spawned lane hasn't said hello (state still 'spawning') after
   * connectTimeoutMs, treat it as a boot failure: recycle it (automatic →
   * backoff/breaker accounting) so a wedged boot doesn't sit "spawning" forever
   * black-holing inbound. Re-armed on every spawn; cancelled once promoted live.
   */
  function armConnectWatchdog(entry: SessionEntry): void {
    const bootedAt = entry.bootAt;
    const t = setTimeout(() => {
      const cur = registry.get(entry.sessionKey);
      // Only fire if THIS boot is still un-promoted (a newer spawn re-arms its
      // own watchdog; a promotion to live clears the concern).
      if (!cur || cur.bootAt !== bootedAt || cur.state !== "spawning") return;
      log(`lane ${entry.sessionKey} never connected within ${connectTimeoutMs}ms; recycling`);
      void recycleSession(entry.sessionKey, "connect timeout (no hello)").catch(() => undefined);
    }, connectTimeoutMs);
    if (typeof t.unref === "function") t.unref();
  }

  function recordFailure(entry: SessionEntry, reason: string): void {
    entry.failures += 1;
    log(`lane spawn failed (#${entry.failures}) ${entry.sessionKey}: ${reason}`);
    if (entry.failures >= breakerThreshold) {
      entry.state = "breaker-open";
      const msg = `circuit breaker OPEN after ${entry.failures} rapid failures: ${reason}`;
      log(`${entry.sessionKey}: ${msg} — lane held; manual /discord recycle required`);
      try {
        onAlert?.(entry.sessionKey, msg);
      } catch {
        // never let the alert callback take down the supervisor
      }
      return;
    }
    entry.state = "backoff";
    // Capped exponential backoff. nextBackoffMs holds the delay to apply BEFORE
    // the next spawn attempt; double it (capped) for the attempt after that.
    entry.nextBackoffMs = Math.min(entry.nextBackoffMs * 2, backoffMaxMs);
  }

  function delay(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const t = setTimeout(resolve, ms);
      if (typeof t.unref === "function") t.unref();
    });
  }

  async function ensureSession(sessionKey: string, cwd: string): Promise<void> {
    return enqueueLane(sessionKey, async () => {
      if (shuttingDown) return;
      let entry = registry.get(sessionKey);
      // An inbound for this lane = user activity; push back the idle-reap clock.
      if (entry) entry.lastActivityAt = now();

      // Cold start: no entry yet. Kill any pre-existing orphan tmux session for
      // this key first (e.g. survivor of a broker crash holding a stale token)
      // so we never run TWO claude sessions for one lane (double-billing + a
      // reconnect-looping orphan that fails verifyToken).
      if (!entry || entry.state === "dead") {
        // Preserve a pre-minted token (a `dead` placeholder from mintToken that a
        // pending shim may already hold) before overwriting the entry.
        const preMintedToken = entry?.state === "dead" ? entry.token : undefined;
        entry = newEntry(sessionKey, cwd);
        if (preMintedToken) entry.token = preMintedToken;
        registry.set(sessionKey, entry);
        if (tmuxHasSession(entry.tmuxName)) {
          log(`cold-start: killing orphan tmux ${entry.tmuxName} before spawn`);
          tmuxKillSession(entry.tmuxName);
        }
        trySpawn(entry);
        await persist();
        return;
      }

      // Breaker is open — held lane, do NOT auto-respawn. Operator must recycle.
      if (entry.state === "breaker-open") {
        return;
      }

      // Already live/spawning AND the tmux session is actually present -> warm,
      // nothing to do (don't double-spawn a still-booting lane). A 'spawning'
      // lane is promoted to 'live' by markPong on the shim's first hello/pong.
      // Stable-uptime resets the failure counter (per §4 backoff reset).
      if ((entry.state === "live" || entry.state === "spawning") && tmuxHasSession(entry.tmuxName)) {
        if (entry.state === "live" && now() - entry.bootAt >= stableResetMs && entry.failures !== 0) {
          entry.failures = 0;
          entry.nextBackoffMs = backoffBaseMs;
          await persist();
        }
        return;
      }

      // Entry exists but the lane is down (crashed, never-came-up, or in backoff).
      // Apply the backoff delay if we're in a backoff window, then (re)spawn.
      if (entry.state === "backoff") {
        await delay(entry.nextBackoffMs);
        if (shuttingDown) return;
      }
      // Reap any stale tmux session before respawning (defensive).
      if (tmuxHasSession(entry.tmuxName)) tmuxKillSession(entry.tmuxName);
      // Mint a fresh one-time token for the new boot so a stale shim can't auth.
      entry.token = mintTokenValue();
      trySpawn(entry);
      await persist();
    });
  }

  /**
   * Recycle a lane. `manual` (operator `/discord recycle`) clears the breaker
   * and respawns immediately. The default (automatic, heartbeat-driven wedge
   * recycle) is treated as a FAILURE for backoff/breaker accounting — otherwise
   * a lane that wedges every ~40s would loop live->recycle->respawn->wedge with
   * zero backoff and never trip the breaker (the §4 "restart storm" vector).
   */
  async function recycleSession(sessionKey: string, reason: string, manual = false): Promise<void> {
    return enqueueLane(sessionKey, async () => {
      const entry = registry.get(sessionKey);
      if (!entry) {
        log(`recycle ignored, unknown lane: ${sessionKey}`);
        return;
      }
      log(`recycling lane ${sessionKey} (${manual ? "manual" : "auto"}): ${reason}`);
      // Per-session isolation: kill ONLY this lane's tmux session. Rotate the
      // token FIRST (before any await) so a racing reconnect from the old/wedged
      // shim with the stale token is rejected — exactly one teardown owner.
      const oldToken = entry.token;
      entry.token = mintTokenValue();
      tmuxKillSession(entry.tmuxName);
      void oldToken;
      if (shuttingDown) {
        entry.state = "dead";
        await persist();
        return;
      }
      if (manual) {
        // Operator override: clear the breaker + backoff and respawn now.
        entry.failures = 0;
        entry.nextBackoffMs = backoffBaseMs;
        entry.state = "spawning";
        trySpawn(entry);
        await persist();
        return;
      }
      // Automatic (wedged) recycle: account it as a failure so a flapping lane
      // backs off and ultimately trips the breaker.
      recordFailure(entry, reason);
      if (entry.state === "breaker-open") {
        // Breaker just opened (or was already open): hold the lane, do not
        // respawn. Operator must /discord recycle (manual) to clear it.
        await persist();
        return;
      }
      // In backoff: wait the capped delay before respawning.
      await delay(entry.nextBackoffMs);
      if (shuttingDown) return;
      entry.token = mintTokenValue();
      trySpawn(entry);
      await persist();
    });
  }

  function mintTokenValue(): string {
    return randomBytes(24).toString("hex");
  }

  function mintToken(sessionKey: string): string {
    const entry = registry.get(sessionKey);
    if (!entry) {
      // Pre-mint for a lane we haven't spawned yet (ipc may verify before
      // ensureSession lands). Create a placeholder dead entry holding the token.
      const placeholder = newEntry(sessionKey, process.cwd());
      placeholder.state = "dead";
      registry.set(sessionKey, placeholder);
      return placeholder.token;
    }
    // Read-only for a running lane: rotating the token here would invalidate the
    // live shim's env token, so its next reconnect-hello would fail verifyToken
    // and it would loop in backoff the broker never accepts. Token rotation
    // happens ONLY at (re)spawn (ensureSession/recycleSession), which is the only
    // place a fresh shim picks up the new token via its env.
    if (entry.state === "live" || entry.state === "spawning") {
      return entry.token;
    }
    entry.token = mintTokenValue();
    return entry.token;
  }

  function verifyToken(sessionKey: string, token: string): boolean {
    const entry = registry.get(sessionKey);
    if (!entry || !entry.token || !token) return false;
    return timingSafeEqualStr(entry.token, token);
  }

  function markPong(sessionKey: string): void {
    const entry = registry.get(sessionKey);
    if (!entry) return;
    entry.lastPongAt = now();
    // A healthy pong is the strongest liveness signal; if we'd marked the lane
    // down-but-present, promote it back to live without scraping any output.
    if (entry.state === "spawning") entry.state = "live";
  }

  function get(sessionKey: string): SessionEntry | undefined {
    return registry.get(sessionKey);
  }

  function list(): SessionEntry[] {
    return Array.from(registry.values()).map((e) => ({ ...e }));
  }

  /**
   * Low-frequency liveness sweep. The IPC heartbeat can only ping CONNECTED
   * shims, so a lane whose claude/tmux dies while IDLE (no traffic) is invisible
   * to it — it would only self-heal on the next inbound. This sweep closes that
   * hole: any 'live' lane whose tmux session is gone (dead), and any 'spawning'
   * lane that has overshot its connect window, is recycled (automatic → backoff
   * + breaker). Per-session isolation: each entry handled independently.
   */
  function sweepOnce(): void {
    if (shuttingDown) return;
    for (const entry of registry.values()) {
      // Idle-reap first: a lane with no user activity for idleReapMs is killed
      // outright (no respawn) and dropped from the registry; the next inbound
      // cold-starts it via ensureSession. Takes priority over the liveness checks
      // below (a long-idle lane shouldn't be recycled-then-reaped).
      if (idleReapMs > 0 && now() - entry.lastActivityAt > idleReapMs) {
        const idleMin = Math.round((now() - entry.lastActivityAt) / 60_000);
        log(`sweep: reaping idle lane ${entry.sessionKey} (idle ${idleMin}m) — tmux killed, will cold-start on next message`);
        if (tmuxHasSession(entry.tmuxName)) tmuxKillSession(entry.tmuxName);
        registry.delete(entry.sessionKey);
        void persist().catch(() => undefined);
        continue;
      }
      if (entry.state === "live" && !tmuxHasSession(entry.tmuxName)) {
        log(`sweep: lane ${entry.sessionKey} tmux gone while live; recycling`);
        void recycleSession(entry.sessionKey, "tmux session died (sweep)").catch(() => undefined);
      } else if (
        entry.state === "spawning" &&
        now() - entry.bootAt > connectTimeoutMs &&
        !tmuxHasSession(entry.tmuxName)
      ) {
        log(`sweep: lane ${entry.sessionKey} spawning but tmux gone; recycling`);
        void recycleSession(entry.sessionKey, "spawn died (sweep)").catch(() => undefined);
      }
    }
  }

  // Crash-recovery: kill orphan tmux sessions from a prior broker before serving.
  try {
    reconcileOnBoot();
  } catch (e) {
    log(`reconcileOnBoot error: ${e instanceof Error ? e.message : String(e)}`);
  }

  const sweepTimer =
    sweepIntervalMs > 0
      ? setInterval(() => {
          try {
            sweepOnce();
          } catch (e) {
            log(`sweep error: ${e instanceof Error ? e.message : String(e)}`);
          }
        }, sweepIntervalMs)
      : null;
  if (sweepTimer && typeof sweepTimer.unref === "function") sweepTimer.unref();

  async function shutdown(): Promise<void> {
    shuttingDown = true;
    if (sweepTimer) clearInterval(sweepTimer);
    // Kill every lane's tmux session (each isolated; failures swallowed).
    for (const entry of registry.values()) {
      tmuxKillSession(entry.tmuxName);
      entry.state = "dead";
    }
    // Flush any in-flight lane chains and the final persisted snapshot.
    await Promise.allSettled(Array.from(laneChains.values()));
    await persist();
    await writeChain.catch(() => undefined);
  }

  return {
    ensureSession,
    recycleSession,
    mintToken,
    verifyToken,
    markPong,
    get,
    list,
    shutdown,
  };
}

/**
 * Atomic write-then-rename of the registry snapshot (mirrors
 * runtime/daemon-registry.ts::writeEntries). A crash mid-write leaves the
 * previous file intact.
 */
async function writeRegistry(path: string, entries: SessionEntry[]): Promise<void> {
  await mkdir(dirname(path), { recursive: true }).catch(() => {});
  const body = `${JSON.stringify({ sessions: entries }, null, 2)}\n`;
  const tmpPath = `${path}.tmp.${process.pid}-${Math.random().toString(36).slice(2, 10)}`;
  try {
    await writeFile(tmpPath, body, "utf8");
    await rename(tmpPath, path);
  } catch (err) {
    await unlink(tmpPath).catch(() => {});
    throw err;
  }
}

/** Constant-time-ish string compare so token verification doesn't leak via timing. */
function timingSafeEqualStr(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let mismatch = 0;
  for (let i = 0; i < a.length; i++) {
    mismatch |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }
  return mismatch === 0;
}

/**
 * Minimal POSIX single-quote shell-quoting for the claude argv we hand to tmux
 * as one command string. The shim path / binary names are operator-controlled,
 * but quoting keeps spaces in paths from splitting the command.
 */
function shellQuote(s: string): string {
  if (s.length > 0 && /^[A-Za-z0-9_/.:=@%+-]+$/.test(s)) return s;
  return `'${s.replace(/'/g, `'\\''`)}'`;
}

/**
 * Deterministic v4-shaped UUID for a sessionKey. A lane always maps to the SAME
 * claude session id across reaps/restarts, so the same on-disk transcript can be
 * --resume'd and the conversation context persists.
 */
export function sessionUuidFor(sessionKey: string): string {
  const h = createHash("sha256").update(`hermes-lane:${sessionKey}`).digest("hex");
  const variant = ((Number.parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${variant}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

/**
 * True if claude already has a transcript for this (cwd, session-id) — i.e. the
 * lane ran before and must be --resume'd (claude errors if --session-id reuses an
 * existing id). claude stores it at ~/.claude/projects/<cwd-with-nonalnum→->/<id>.jsonl.
 */
export function claudeSessionExists(cwd: string, uuid: string): boolean {
  const home = process.env.HOME ?? homedir();
  const enc = cwd.replace(/[^a-zA-Z0-9]/g, "-");
  return existsSync(join(home, ".claude", "projects", enc, `${uuid}.jsonl`));
}
