# Hermes Broker — Operations & As-Built (Phase 1)

Companion to `broker-design.md` (the design rationale). This doc describes how
the broker **actually runs today** on the author's Mac and how to operate it.
If you only want the architecture, read the design doc; if you need to start,
stop, debug, or recover the live bot, read this.

---

## 1. What's deployed

The Hermes daemon routes Discord messages to long-running, **subscription-billed**
interactive `claude --channels` sessions ("lanes") in tmux, via an AF_UNIX
broker + a per-lane MCP channel-shim. This is Approach B from the design doc,
gated behind `discord.useBrokerSessions`.

| | |
|---|---|
| **Code** | `~/Projects/claude-hermes` (this repo) |
| **Run dir (cwd)** | `~/Projects/claudehermes` — a plain workspace dir, **not** a checkout |
| **Runtime state** | `~/Projects/claudehermes/.claude/hermes/` |
| **Process manager** | launchd user agent `com.benjaminwong.claude-hermes` |
| **Discord app** | `claude_hermes#2653` (single allowlisted user) |

> Why two dirs: the daemon reads its settings/state from `<cwd>/.claude/hermes`.
> Running it from a dedicated workspace dir keeps the daemon's own state out of
> the source tree (the repo's `.claude/hermes/` is used only by e2e tests).

### Runtime state files (`~/Projects/claudehermes/.claude/hermes/`)

| file | purpose |
|---|---|
| `settings.json` | daemon config (token, allowlist, channel→cwd map, `useBrokerSessions`, `idleReapMinutes`) |
| `logs/daemon.log` | combined stdout/stderr (launchd appends here) |
| `inbox.db` | durable SQLite inbox — every inbound row, `pending`→`delivered`→`answered` |
| `broker-sessions.json` | persisted lane registry (sessionKey, tmux name, token, state) |
| `state.db`, `state.json` | heartbeat schedule, message history |

---

## 2. Process management (launchd)

The daemon runs under `~/Library/LaunchAgents/com.benjaminwong.claude-hermes.plist`:

- **`RunAtLoad`** — starts at user login.
- **`KeepAlive { SuccessfulExit: false }`** — respawns on crash / signal death,
  stays down after a clean `exit(0)` (so `bootout` actually stops it).
- **`ThrottleInterval 15`** — a boot-time crash can't hot-loop.
- **Hardcoded `PATH`** — launchd agents get a bare environment; the daemon
  spawns `claude` (`~/.local/bin`) and `tmux` (`/opt/homebrew/bin`) for lanes,
  so both dirs are in the plist's `EnvironmentVariables.PATH`.

> ⚠️ A GUI launchd agent starts at **login**, not at boot. If the Mac reboots to
> the login window and nobody logs in, the bot stays down. (Acceptable on an
> always-logged-in personal machine; the 2026-06-10 reboot outage — daemon dead
> from 08:42 until a manual restart — is what motivated moving off `nohup`.)

### Operate it

```bash
# Restart (use this after pulling code changes — picks up the new src/)
launchctl kickstart -k gui/$(id -u)/com.benjaminwong.claude-hermes

# Stop (won't respawn until next login or bootstrap)
launchctl bootout gui/$(id -u)/com.benjaminwong.claude-hermes

# Start / re-register after editing the plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.benjaminwong.claude-hermes.plist

# Is it alive? (column 1 = pid, or '-' if not running)
launchctl list | grep claude-hermes
```

Do **not** start it by hand with `nohup bun … start` anymore — two managers
fighting over one daemon (and one pidfile) is avoidable confusion.

> **Restart hygiene:** a restart runs `reconcileOnBoot`, which **kills surviving
> tmux lanes** (their broker tokens are unrecoverable across a restart). A lane
> that was mid-turn loses its in-flight work; the durable inbox replays the
> unanswered row when the user next messages that channel. So prefer to restart
> when lanes are idle, and never restart in the middle of a lane doing real work.

---

## 3. Observability & recovery

```bash
RUN=~/Projects/claudehermes/.claude/hermes

# Live daemon log
tail -f $RUN/logs/daemon.log

# Running lanes (tmux sessions named hermes-workspace-<hash>-<rand>)
tmux ls

# Attach to a lane to watch the interactive claude session
tmux attach -t hermes-workspace-<hash>-<rand>     # detach with ctrl-b d

# Inbox: anything stuck unanswered?
sqlite3 $RUN/inbox.db \
  "SELECT seq, session_key, chat_id, state,
          datetime(created_at/1000,'unixepoch','+8 hours')
   FROM inbox WHERE state != 'answered' ORDER BY seq"
```

**Stranded inbox rows.** A row stays `delivered` (never `answered`) when a lane
read the message but the turn didn't produce a final reply to that chat — e.g.
the DM-egress bug, or a create_thread turn (see follow-up #1 below). On the next
restart/cold-start these replay into a fresh lane. To clear them deliberately
(back up first):

```bash
cd $RUN && cp inbox.db inbox.db.bak-$(date +%F)
sqlite3 inbox.db \
  "UPDATE inbox SET state='answered', answered_at=CAST(strftime('%s','now') AS INTEGER)*1000
   WHERE state != 'answered'"
```

---

## 4. Egress security model

A lane is an interactive Claude session fed untrusted Discord/web content, so
**every broker→Discord text path is gated** by `assertBrokerEgressAllowed`
(`src/commands/discord.ts`) before any API call. Both `brokerReply` and
`brokerProgress` run it; the `chat_id` is lane-supplied, so an ungated path is
an exfiltration primitive.

The gate is two checks:

1. **Allowlist** — `chat_id` must be in the union of `channelDirectories` keys ∪
   `listenChannels` ∪ known thread channels ∪ **verified DM channels**. A DM
   channel id never appears in the config maps, so `verifyDmChannel` does a
   `GET /channels/{id}` and admits only a **type-1** (1:1) DM whose recipient is
   an allowlisted user — group DMs (type 3) and guild channels are barred. The
   result is cached per boot (pre-warmed on authorized inbound DMs).
2. **Ownership** — the replying `sessionKey` must own that `chat_id` (a thread
   accepts its own lane or its parent channel's lane; a plain channel/DM is owned
   only by its workspace lane). Stops lane A replying into lane B's channel.

The worst a gated lane can reach is a DM with an already-allowlisted user.

---

## 5. Key settings (`discord` block)

| key | current | meaning |
|---|---|---|
| `useBrokerSessions` | `true` | route Discord runs through the broker (vs. metered `claude -p`). One-flip rollback to `false`. |
| `idleReapMinutes` | `0` (disabled) | reap a lane after N idle minutes. **Held at 0** — the POC idle-reap has confirmed bugs (see follow-ups); re-enable only after they're fixed. |
| `channelDirectories` | 4 entries | channel id → project cwd for that channel's lane |
| `listenChannels` | 4 ids | channels the bot answers without an @-mention |
| `allowedUserIds` | 1 id | fail-closed allowlist (snowflakes kept as strings) |

---

## 6. Known follow-ups (deferred, tracked)

From the 2026-06-10 adversarial review of the Phase-1 branch. Not blockers for
the current single-user deployment, but fix before scaling:

1. **create_thread leaves the parent inbox row unanswered** — a lane that
   answers by creating a thread + replying into it never marks the triggering
   row answered, so the stale prompt replays on every restart/cold-start.
2. **Idle-reap bugs (3)** — reaps `breaker-open` lanes (voids the manual-recycle
   invariant), kills lanes mid-turn (`lastActivityAt` is only bumped on inbound,
   never on egress), and races the per-lane serialization chain. Mitigated by
   `idleReapMinutes: 0`.
3. **No per-user DM isolation** — all DM lanes collapse to
   `workspaceKey(daemon cwd)`; harden before adding a second allowlisted user.
4. **`knownDMs` survives allowlist revocation** until restart (egress-only).
5. **Shim RPC acks have no deadline** — `discordApi` now has a 30s per-attempt
   timeout, but the shim still waits for an ack forever, and `replyChains`
   entries are never reset on recycle.
6. **Two-daemon / stale-registry wedge** (observed live 2026-06-10) — if two
   daemons overlap (e.g. a manual start racing the launchd copy, or a crash +
   respawn before the old pid is reaped), the second one `unlink`s + rebinds the
   broker socket out from under the first, orphaning its shims. The orphaned
   lane keeps `state: "live"` in `broker-sessions.json` with a stale
   `lastPongAt` and is never recycled, so its `reply`/`progress` tool calls loop
   on `broker socket not connected`. Recovery: cold-start the lane (next message
   after a clean single-daemon restart). Root fixes to consider: a single-flight
   daemon lock, don't `unlink` a socket whose owner is still alive, and reconcile
   `broker-sessions.json` on boot (recycle lanes whose token the new broker can't
   verify). Operational rule: only ever run one daemon — see §2.
