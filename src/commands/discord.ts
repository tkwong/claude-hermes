import { ensureProjectClaudeMd, run, runUserMessage, compactCurrentSession } from "../runner";
import { getSettings, loadSettings } from "../config";
import { resetSession, peekSession } from "../sessions";
import { listThreadSessions, removeThreadSession, peekThreadSession } from "../sessionManager";
import { readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { transcribeAudioToText } from "../whisper";
import { resolveSkillPrompt } from "../skills";
import { discoverSkills } from "../skills/discovery";
import { mkdir } from "node:fs/promises";
import { basename, extname, join } from "node:path";
import { discordInboxDir } from "../paths";
import { projectSlugFromCwd } from "../runtime/claude-paths";
import { extractSessionAndResultFromText } from "../runtime/claude-output";
import { createDiscordStatusSink, type DiscordTransport } from "../status/sinks/discord";
import { DISCORD_API, discordApi } from "./discord-api";
import { buildSlashCommandList } from "./slash-commands";
import { classifyThreadIntent } from "./discord-intent";
import { threadKey, workspaceKey } from "../router/session-key";

// --- Discord API constants ---

const GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json";

const GatewayOp = {
  DISPATCH: 0,
  HEARTBEAT: 1,
  IDENTIFY: 2,
  RESUME: 6,
  RECONNECT: 7,
  INVALID_SESSION: 9,
  HELLO: 10,
  HEARTBEAT_ACK: 11,
} as const;

// Intents bitfield
const INTENTS =
  (1 << 0) |   // GUILDS
  (1 << 9) |   // GUILD_MESSAGES
  (1 << 10) |  // GUILD_MESSAGE_REACTIONS
  (1 << 12) |  // DIRECT_MESSAGES
  (1 << 15);   // MESSAGE_CONTENT (privileged)

// --- Type interfaces ---

interface DiscordUser {
  id: string;
  username: string;
  discriminator: string;
  bot?: boolean;
}

interface DiscordAttachment {
  id: string;
  filename: string;
  content_type?: string;
  url: string;
  proxy_url: string;
  size: number;
  flags?: number;
}

interface DiscordPoll {
  question?: { text?: string };
  answers?: Array<{ answer_id?: number; poll_media?: { text?: string } }>;
}

interface DiscordMessageSnapshot {
  message?: {
    content?: string;
    attachments?: DiscordAttachment[];
    author?: DiscordUser;
    poll?: DiscordPoll;
  };
}

interface DiscordMessage {
  id: string;
  channel_id: string;
  guild_id?: string;
  author: DiscordUser;
  content: string;
  attachments: DiscordAttachment[];
  mentions: DiscordUser[];
  referenced_message?: DiscordMessage | null;
  // Forwarded messages: message_reference.type === 1 (FORWARD) + a snapshot of
  // the original (the visible `content` is empty for a pure forward).
  message_reference?: { type?: number; message_id?: string; channel_id?: string };
  message_snapshots?: DiscordMessageSnapshot[];
  poll?: DiscordPoll;
  sticker_items?: Array<{ id: string; name: string }>;
  flags?: number;
  type: number;
}

interface DiscordInteraction {
  id: string;
  type: number; // 2=APPLICATION_COMMAND, 3=MESSAGE_COMPONENT
  data?: {
    name?: string;
    custom_id?: string;
  };
  channel_id?: string;
  guild_id?: string;
  member?: { user: DiscordUser };
  user?: DiscordUser;
  token: string;
  message?: DiscordMessage;
}

interface DiscordGuild {
  id: string;
  name: string;
  system_channel_id?: string | null;
  joined_at?: string;
}

interface GatewayPayload {
  op: number;
  d: any;
  s: number | null;
  t: string | null;
}

// --- Gateway state ---

let ws: WebSocket | null = null;
let heartbeatIntervalMs = 0;
let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
let heartbeatJitterTimer: ReturnType<typeof setTimeout> | null = null;
let lastSequence: number | null = null;
let gatewaySessionId: string | null = null;
let resumeGatewayUrl: string | null = null;
let heartbeatAcked = true;
let running = true;
let discordDebug = false;

// Bot identity (populated from READY)
let botUserId: string | null = null;
let botUsername: string | null = null;
let applicationId: string | null = null;

// Track guilds we were already in before this session to avoid duplicate welcome messages
let readyGuildIds: Set<string> | null = null;

// Track known thread channel IDs and their parent channel IDs for multi-session support
const knownThreads = new Map<string, { parentId: string }>();

// DM channels VERIFIED this boot. A DM channel id never appears in
// channelDirectories / listenChannels, so broker egress needs its own record of
// which chat_ids are DMs we may reply to. Populated only by verifyDmChannel
// (pre-warmed on authorized inbound DMs, backfilled on egress when a durable
// inbox row outlives a daemon restart) — never by a blind insert, because a
// MESSAGE_CREATE payload carries no channel type and a group DM (type 3, other
// humans present) must not become an egress destination.
const knownDMs = new Set<string>();

// Broker live-progress: chatId -> the message id of the current turn's editable
// "status" message (e.g. "🔍 睇緊 memory…"). brokerProgress posts it once then
// edits it in place; the turn-final reply deletes it via clearBrokerStatus.
const brokerStatusMsg = new Map<string, string>();

/**
 * Resolve the project working directory (cwd) for a Discord channel.
 * Maps the channel — or, for a thread, its parent channel — via
 * `discord.channelDirectories`. Falls back to the daemon's own cwd when the
 * channel is unmapped or the mapped directory does not exist, so unconfigured
 * channels keep their current behaviour.
 */
function resolveChannelCwd(channelId: string): string {
  const map = getSettings().discord.channelDirectories ?? {};
  const lookupId = knownThreads.get(channelId)?.parentId ?? channelId;
  const dir = map[lookupId];
  if (dir && existsSync(dir)) return dir;
  if (dir) {
    console.warn(`[Discord] channelDirectories maps ${lookupId} → ${dir} but it does not exist; falling back to daemon cwd`);
  }
  return process.cwd();
}

// --- Debug ---

function debugLog(message: string): void {
  if (!discordDebug) return;
  console.log(`[Discord][debug] ${message}`);
}

// --- Message sending ---

export function discordStatusTransport(token: string): DiscordTransport {
  return {
    async postMessage(channelId, content) {
      const trimmed = content.slice(0, 2000);
      const res = await discordApi<{ id: string }>(
        token,
        "POST",
        `/channels/${channelId}/messages`,
        { content: trimmed },
      );
      return { id: res.id };
    },
    async patchMessage(channelId, messageId, content) {
      const trimmed = content.slice(0, 2000);
      await discordApi(
        token,
        "PATCH",
        `/channels/${channelId}/messages/${messageId}`,
        { content: trimmed },
      );
    },
    async deleteMessage(channelId, messageId) {
      await discordApi(token, "DELETE", `/channels/${channelId}/messages/${messageId}`);
    },
  };
}

async function sendMessage(
  token: string,
  channelId: string,
  text: string,
  components?: unknown[],
  replyToMessageId?: string,
): Promise<void> {
  const normalized = text.replace(/\[react:[^\]\r\n]+\]/gi, "").trim();
  if (!normalized) return;
  const MAX_LEN = 2000;
  for (let i = 0; i < normalized.length; i += MAX_LEN) {
    const chunk = normalized.slice(i, i + MAX_LEN);
    const body: Record<string, unknown> = { content: chunk };
    // Attach components only to the last chunk
    if (components && i + MAX_LEN >= normalized.length) {
      body.components = components;
    }
    // A native reply reference belongs only on the FIRST chunk. fail_if_not_exists
    // false → if the referenced message was deleted, send a normal message rather
    // than erroring.
    if (replyToMessageId && i === 0) {
      body.message_reference = { message_id: replyToMessageId, fail_if_not_exists: false };
    }
    await discordApi(token, "POST", `/channels/${channelId}/messages`, body);
  }
}

/**
 * Outbound file attachments (multipart/form-data — discordApi is JSON-only). Used
 * by brokerReply when Claude's `reply` carries `files`. Skips any path that is
 * missing or over Discord's 25 MiB non-Nitro cap, returning the skipped names so
 * the caller can tell the user instead of silently dropping them.
 */
async function sendFilesToChannel(
  token: string,
  channelId: string,
  text: string,
  filePaths: string[],
  replyToMessageId?: string,
): Promise<{ skipped: string[] }> {
  const form = new FormData();
  const skipped: string[] = [];
  let n = 0;
  for (const p of filePaths) {
    const file = Bun.file(p);
    if (!(await file.exists())) {
      skipped.push(`${basename(p)} (not found)`);
      continue;
    }
    if (file.size > MAX_INBOUND_FILE_BYTES) {
      skipped.push(`${basename(p)} (>25MB)`);
      continue;
    }
    form.append(`files[${n}]`, new Blob([await file.arrayBuffer()]), basename(p));
    n++;
  }
  const payload: Record<string, unknown> = {};
  const content = text.replace(/\[react:[^\]\r\n]+\]/gi, "").trim();
  if (content) payload.content = content.slice(0, 2000);
  if (replyToMessageId) {
    payload.message_reference = { message_id: replyToMessageId, fail_if_not_exists: false };
  }
  // Nothing attachable and no text → nothing to send.
  if (n === 0 && !content) return { skipped };
  if (n === 0) {
    // All files skipped but there's text — fall back to a plain message.
    await sendMessage(token, channelId, content, undefined, replyToMessageId);
    return { skipped };
  }
  form.append("payload_json", JSON.stringify(payload));
  const res = await fetch(`${DISCORD_API}/channels/${channelId}/messages`, {
    method: "POST",
    headers: { Authorization: `Bot ${token}` },
    body: form,
  });
  if (!res.ok) {
    const errText = await res.text().catch(() => "");
    throw new Error(`Discord file upload failed: ${res.status} ${errText}`);
  }
  return { skipped };
}

async function sendMessageToUser(
  token: string,
  userId: string,
  text: string,
): Promise<void> {
  // Discord requires creating a DM channel before sending
  const channel = await discordApi<{ id: string }>(
    token,
    "POST",
    "/users/@me/channels",
    { recipient_id: userId },
  );
  await sendMessage(token, channel.id, text);
}

async function sendTyping(token: string, channelId: string): Promise<void> {
  await discordApi(token, "POST", `/channels/${channelId}/typing`).catch(() => {});
}

/**
 * Broker typing indicator. The broker calls this (via onInbound) while a lane
 * works a turn, so a slow high-effort reply shows "claude_hermes is typing…"
 * instead of dead silence. Reads the token from settings (like brokerReply) and
 * is fully best-effort (swallows errors). Discord typing auto-expires after ~10s,
 * so the daemon refreshes it on an interval until the final reply.
 */
export async function brokerSendTyping(chatId: string): Promise<void> {
  const token = getSettings().discord.token;
  if (!token) return;
  await sendTyping(token, chatId);
}

export async function sendReaction(
  token: string,
  channelId: string,
  messageId: string,
  emoji: string,
): Promise<void> {
  await discordApi(
    token,
    "PUT",
    `/channels/${channelId}/messages/${messageId}/reactions/${encodeURIComponent(emoji)}/@me`,
  );
}

/**
 * Broker egress callback (Phase-1). The broker's IPC server hands every shim
 * `reply` frame here; this is the SAME egress as the metered path
 * (`sendMessage` with chunking + `[react:]` stripping), so the broker never
 * re-implements that logic.
 *
 * Re-validates `chatId` against the bot's own routing allowlist before sending
 * via `assertBrokerEgressAllowed` (see its doc for the allowlist union and the
 * ownership rule): a session may only reply to a channel it is actually wired
 * to. An unrecognised `chatId` throws an `allowlist`-tagged error so ipc.ts
 * maps it to the `chat_id not allowlisted` reply_ack — the session never
 * exfiltrates to an arbitrary channel by minting a foreign chat_id.
 *
 * ALSO enforces per-session ownership: the replying `sessionKey` must be the one
 * that owns `chatId` (the same sessionKey execClaude derives for that channel /
 * thread). This stops lane A from replying into lane B's channel even when both
 * are globally allowlisted (the contract's "a session may only reply to its own
 * allowlisted channels"). For a thread, ownership = `threadKey('discord',chatId)`;
 * for a plain channel, ownership = `workspaceKey(resolveChannelCwd(chatId))`.
 *
 * `reply_to` is accepted for the schema but threaded replies are Phase-2; for
 * now it is logged and the message goes to the channel directly.
 */
/**
 * Last-resort DM check for broker egress: a tmux lane can outlive a daemon
 * restart, so its reply may arrive before the user DMs again (knownDMs is
 * empty after boot). Look the channel up and allow only a real DM (type 1)
 * whose recipient is an allowlisted user, caching a hit so the lookup happens
 * at most once per channel per boot. Fails closed on any API error.
 */
async function verifyDmChannel(
  token: string,
  chatId: string,
  allowedUserIds: string[],
): Promise<boolean> {
  if (knownDMs.has(chatId)) return true;
  try {
    const ch = await discordApi<{ type?: number; recipients?: Array<{ id?: string }> }>(
      token,
      "GET",
      `/channels/${chatId}`,
    );
    const ok =
      ch.type === 1 &&
      (ch.recipients ?? []).some((r) => r.id !== undefined && allowedUserIds.includes(r.id));
    if (ok) knownDMs.add(chatId);
    return ok;
  } catch {
    return false;
  }
}

/**
 * Shared egress gate for every broker→Discord text path (`reply`, `progress`).
 * Throws an `allowlist`-tagged error (ipc.ts regex-matches /allowlist/i for the
 * ack) unless `chatId` is a destination this lane may write to:
 *
 *   1. Allowlist: union of `channelDirectories` keys + `listenChannels` + any
 *      known thread channel + any verified DM channel (see knownDMs above).
 *   2. Ownership: the replying `sessionKey` must own `chatId` — a thread accepts
 *      its own lane (threadKey) OR its parent channel's workspace lane (so a
 *      channel session that just created a thread can post into it); a plain
 *      channel or DM is owned only by its workspace lane.
 *
 * Every broker egress callback MUST run this before touching the Discord API —
 * a lane is an interactive Claude session fed untrusted content, so an ungated
 * path is an exfiltration primitive (the worst a gated lane can reach is a DM
 * with an already-allowlisted user).
 */
async function assertBrokerEgressAllowed(
  caller: string,
  sessionKey: string,
  chatId: string,
  config: { token: string; channelDirectories?: Record<string, string>; listenChannels: string[]; allowedUserIds: string[] },
): Promise<void> {
  const mapped = new Set<string>(Object.keys(config.channelDirectories ?? {}));
  for (const ch of config.listenChannels) mapped.add(ch);
  const parentId = knownThreads.get(chatId)?.parentId;
  const allowed =
    mapped.has(chatId) ||
    (parentId !== undefined && mapped.has(parentId)) ||
    knownThreads.has(chatId) ||
    (await verifyDmChannel(config.token, chatId, config.allowedUserIds));
  if (!allowed) {
    throw new Error(`${caller}: chat_id ${chatId} not allowlisted (no channelDirectories/listenChannels/thread/DM match)`);
  }
  const isThread = knownThreads.has(chatId);
  const channelOwner = workspaceKey(resolveChannelCwd(chatId));
  const ownerKeys = isThread ? [threadKey("discord", chatId), channelOwner] : [channelOwner];
  if (!ownerKeys.includes(sessionKey)) {
    throw new Error(
      `${caller}: chat_id ${chatId} not allowlisted for session ${sessionKey} (owners ${ownerKeys.join("/")})`,
    );
  }
}

export async function brokerReply(
  sessionKey: string,
  chatId: string,
  text: string,
  replyTo?: string,
  files?: string[],
): Promise<void> {
  const config = getSettings().discord;
  if (!config.token) {
    throw new Error("brokerReply: Discord token not configured");
  }
  await assertBrokerEgressAllowed("brokerReply", sessionKey, chatId, config);
  // A `[react:emoji]` directive in the reply text → add a reaction. It targets the
  // message Claude is replying to (reply_to). Without a target we can't react, so
  // we skip it (the text/files still go out). This mirrors the metered path, which
  // reacts to the triggering message.
  const { cleanedText, reactionEmoji } = extractReactionDirective(text);
  if (reactionEmoji && replyTo) {
    await sendReaction(config.token, chatId, replyTo, reactionEmoji).catch((err) =>
      console.error(`[Discord] broker reaction failed: ${err instanceof Error ? err.message : err}`),
    );
  }
  // Outbound files (multipart). On partial failure, tell the user which were dropped.
  if (files && files.length > 0) {
    const { skipped } = await sendFilesToChannel(config.token, chatId, cleanedText, files, replyTo);
    if (skipped.length > 0) {
      await sendMessage(
        config.token,
        chatId,
        `⚠️ Couldn't attach: ${skipped.join(", ")}`,
        undefined,
        replyTo,
      );
    }
    return;
  }
  // `reply_to` → native Discord reply reference (a quote of that message).
  await sendMessage(config.token, chatId, cleanedText, undefined, replyTo);
}

/**
 * Broker egress for the shim's `progress` tool: a LIVE status line for a slow
 * turn. The first call posts a message ("🔍 睇緊 memory…"); each subsequent call
 * EDITS that same message in place, so the user watches the agent's intent update
 * without message spam. The turn-final reply removes it via clearBrokerStatus.
 *
 * Runs the SAME `assertBrokerEgressAllowed` gate as brokerReply — `chat_id` is
 * lane-supplied free text, so an ungated progress post would let a lane write
 * arbitrary text into any channel the bot can see, trivially bypassing the
 * reply gate. Gate failures propagate (ipc.ts acks them as allowlist errors);
 * only the post/edit itself is best-effort (a failed edit — e.g. message
 * deleted — just forgets the tracked id).
 */
export async function brokerProgress(sessionKey: string, chatId: string, text: string): Promise<void> {
  const config = getSettings().discord;
  const token = config.token;
  if (!token) return;
  await assertBrokerEgressAllowed("brokerProgress", sessionKey, chatId, config);
  const clean = text.replace(/\[react:[^\]\r\n]+\]/gi, "").trim().slice(0, 2000) || "⏳ …";
  const existing = brokerStatusMsg.get(chatId);
  try {
    if (existing) {
      await discordApi(token, "PATCH", `/channels/${chatId}/messages/${existing}`, {
        content: clean,
      });
    } else {
      const res = await discordApi<{ id: string }>(token, "POST", `/channels/${chatId}/messages`, {
        content: clean,
      });
      brokerStatusMsg.set(chatId, res.id);
    }
  } catch (err) {
    // Lost the status message (deleted / perms) — forget it so the next progress
    // call posts a fresh one instead of looping on a dead edit.
    brokerStatusMsg.delete(chatId);
    debugLog(`brokerProgress edit failed for ${chatId}: ${err instanceof Error ? err.message : err}`);
  }
}

/**
 * Delete + forget the live status message for a chat (called on the turn-final
 * reply, and at the start of a new turn so a stranded status from an
 * un-finalized prior turn can't linger). Best-effort.
 */
export function clearBrokerStatus(chatId: string): void {
  const id = brokerStatusMsg.get(chatId);
  brokerStatusMsg.delete(chatId);
  if (!id) return;
  const token = getSettings().discord.token;
  if (token) {
    void discordApi(token, "DELETE", `/channels/${chatId}/messages/${id}`).catch(() => {});
  }
}

/**
 * Delete + forget ALL live status messages. Called on broker shutdown so a daemon
 * restart that kills lanes mid-turn doesn't strand orphaned "⏳ …" messages (the
 * killed lane never sends its final reply to clear them). Awaits the deletes
 * best-effort so they actually go out before the process exits.
 */
export async function clearAllBrokerStatus(): Promise<void> {
  const entries = [...brokerStatusMsg.entries()];
  brokerStatusMsg.clear();
  const token = getSettings().discord.token;
  if (!token || entries.length === 0) return;
  await Promise.all(
    entries.map(([chatId, id]) =>
      discordApi(token, "DELETE", `/channels/${chatId}/messages/${id}`).catch(() => {}),
    ),
  );
}

/**
 * Broker egress for the shim's `create_thread` tool. Creates a real Discord
 * thread under an allowlisted parent channel and registers it in knownThreads so
 * the user's subsequent messages in it route to (and the bot can reply into) that
 * thread's OWN lane — the "threads => session" model. Mirrors brokerReply's
 * fail-closed allowlist + per-session ownership guard: only the session that owns
 * the parent CHANNEL lane may open threads under it. Returns the new thread id so
 * Claude can `reply` into it (chat_id = threadId).
 */
export async function brokerCreateThread(
  sessionKey: string,
  parentChatId: string,
  name: string,
  seedText?: string,
): Promise<{ threadId: string; parentId: string }> {
  const config = getSettings().discord;
  if (!config.token) {
    throw new Error("brokerCreateThread: Discord token not configured");
  }
  const mapped = new Set<string>(Object.keys(config.channelDirectories ?? {}));
  for (const ch of config.listenChannels) mapped.add(ch);
  if (!mapped.has(parentChatId)) {
    throw new Error(`brokerCreateThread: parent ${parentChatId} not allowlisted (no channelDirectories/listenChannels match)`);
  }
  // A thread is a child of a CHANNEL lane (Discord forbids threads-in-threads),
  // so the creating session must own the parent channel's workspace lane.
  const ownerKey = workspaceKey(resolveChannelCwd(parentChatId));
  if (ownerKey !== sessionKey) {
    throw new Error(
      `brokerCreateThread: parent ${parentChatId} not owned by session ${sessionKey} (owner ${ownerKey})`,
    );
  }
  const cleanName = name.replace(/\s+/g, " ").trim().slice(0, 100) || "thread";
  const thread = await discordApi<{ id: string; name: string }>(
    config.token,
    "POST",
    `/channels/${parentChatId}/threads`,
    { name: cleanName, type: 11 /* PUBLIC_THREAD */, auto_archive_duration: 4320 },
  );
  knownThreads.set(thread.id, { parentId: parentChatId });
  if (seedText && seedText.trim()) {
    await sendMessage(config.token, thread.id, seedText);
  }
  console.log(
    `[Discord] broker thread created: ${thread.id} name="${cleanName}" parent=${parentChatId} (session ${sessionKey})`,
  );
  return { threadId: thread.id, parentId: parentChatId };
}

// --- Reaction directive extraction (same as telegram.ts) ---

function extractReactionDirective(text: string): { cleanedText: string; reactionEmoji: string | null } {
  let reactionEmoji: string | null = null;
  const cleanedText = text
    .replace(/\[react:([^\]\r\n]+)\]/gi, (_match, raw) => {
      const candidate = String(raw).trim();
      if (!reactionEmoji && candidate) reactionEmoji = candidate;
      return "";
    })
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
  return { cleanedText, reactionEmoji };
}

// --- Thread rejoin helper ---
async function rejoinThreads(token: string): Promise<void> {
  // Union of persisted (metered sessionManager) threads + in-memory knownThreads.
  // CRITICAL: broker-created threads live ONLY in knownThreads (not sessions.json),
  // so a resume that rejoined only the persisted set would silently stop the bot
  // receiving their MESSAGE_CREATE events — the "no response in a new thread" bug.
  const persisted = await listThreadSessions();
  const ids = new Set<string>();
  for (const ts of persisted) ids.add(ts.threadId);
  for (const id of knownThreads.keys()) ids.add(id);
  let n = 0;
  for (const threadId of ids) {
    try {
      await discordApi(token, "PUT", `/channels/${threadId}/thread-members/@me`);
      if (!knownThreads.has(threadId)) {
        const ch = await discordApi<{ parent_id?: string }>(token, "GET", `/channels/${threadId}`);
        if (ch.parent_id) knownThreads.set(threadId, { parentId: ch.parent_id });
      }
      n++;
    } catch (err) {
      console.error(`[Discord] Failed to rejoin thread ${threadId}: ${err}`);
    }
  }
  if (n > 0) console.log(`[Discord] Rejoined ${n} thread(s) (persisted + knownThreads)`);
}

// --- Guild trigger logic ---

function guildTriggerReason(message: DiscordMessage): string | null {
  // Reply to bot
  if (botUserId && message.referenced_message?.author?.id === botUserId) return "reply_to_bot";

  // Mention via mentions array
  if (botUserId && message.mentions.some((m) => m.id === botUserId)) return "mention";

  // Mention in content (fallback)
  if (botUserId && message.content.includes(`<@${botUserId}>`)) return "mention_in_content";

  // Listen channel (respond to all messages, no mention needed)
  const config = getSettings().discord;
  if (config.listenChannels.includes(message.channel_id)) return "listen_channel";

  // Thread whose parent channel is a listen channel
  const threadInfo = knownThreads.get(message.channel_id);
  if (threadInfo && config.listenChannels.includes(threadInfo.parentId)) return "listen_channel_thread";

  return null;
}

// --- Attachment handling ---

function isImageAttachment(a: DiscordAttachment): boolean {
  return Boolean(a.content_type?.startsWith("image/"));
}

function isVoiceAttachment(a: DiscordAttachment): boolean {
  // IS_VOICE_MESSAGE flag
  if ((a.flags ?? 0) & (1 << 13)) return true;
  return Boolean(a.content_type?.startsWith("audio/"));
}

// Don't pull huge files into the box; Claude can't usefully read them and it
// risks filling disk. Discord's own non-Nitro upload cap is 25 MiB anyway.
const MAX_INBOUND_FILE_BYTES = 25 * 1024 * 1024;

async function downloadDiscordAttachment(
  attachment: DiscordAttachment,
  type: "image" | "voice" | "file",
): Promise<string | null> {
  if (attachment.size > MAX_INBOUND_FILE_BYTES) {
    debugLog(`Attachment ${attachment.filename} too large (${attachment.size}B); skipping download`);
    return null;
  }
  const dir = discordInboxDir();
  await mkdir(dir, { recursive: true });

  const response = await fetch(attachment.url);
  if (!response.ok) throw new Error(`Discord attachment download failed: ${response.status}`);

  const ext = extname(attachment.filename) || (type === "voice" ? ".ogg" : type === "image" ? ".jpg" : "");
  const filename = `${attachment.id}-${Date.now()}${ext}`;
  const localPath = join(dir, filename);

  const bytes = new Uint8Array(await response.arrayBuffer());
  await Bun.write(localPath, bytes);
  debugLog(`Attachment downloaded: ${localPath} (${bytes.length} bytes)`);
  return localPath;
}

// --- Slash command registration ---

async function registerSlashCommands(token: string): Promise<void> {
  if (!applicationId) return;

  // Discovery failures must never block boot — fall back to the hardcoded
  // baseline by passing an empty skill list.
  const skills = await discoverSkills().catch(() => [] as never[]);
  const commands = buildSlashCommandList(skills);

  await discordApi(
    token,
    "PUT",
    `/applications/${applicationId}/commands`,
    commands,
  );
  debugLog(`Slash commands registered (${commands.length} total)`);
}

// --- Interaction response helper ---

async function respondToInteraction(
  interaction: DiscordInteraction,
  data: { content: string; flags?: number; components?: unknown[] },
): Promise<void> {
  await fetch(
    `${DISCORD_API}/interactions/${interaction.id}/${interaction.token}/callback`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        type: 4, // CHANNEL_MESSAGE_WITH_SOURCE
        data,
      }),
    },
  );
}

// --- Message handler ---

async function handleMessageCreate(token: string, message: DiscordMessage): Promise<void> {
  const config = getSettings().discord;

  // Ignore bot messages
  if (message.author.bot) return;

  const userId = message.author.id;
  const channelId = message.channel_id;
  const isDM = !message.guild_id;
  const isGuild = !!message.guild_id;
  const content = message.content;

  // Recover lost thread from sessions.json (fallback for knownThreads volatility)
  if (isGuild && !knownThreads.has(channelId)) {
    const persisted = await peekThreadSession("discord", channelId);
    if (persisted) {
      try {
        const ch = await discordApi<{ parent_id?: string }>(config.token, "GET", `/channels/${channelId}`);
        if (ch.parent_id) {
          knownThreads.set(channelId, { parentId: ch.parent_id });
          debugLog(`Thread recovered from sessions.json: ${channelId} (parent: ${ch.parent_id})`);
        }
      } catch (err) {
        debugLog(`Thread recovery failed for ${channelId}: ${err}`);
      }
    }
  }

  // Guild trigger check
  const triggerReason = isGuild ? guildTriggerReason(message) : "direct_message";
  if (isGuild && !triggerReason) {
    const threadInfo = knownThreads.get(channelId);
    console.log(`[Discord][DIAG] SKIP channel=${channelId} guild=${message.guild_id} inKnown=${knownThreads.has(channelId)} threadInfo=${JSON.stringify(threadInfo)} knownSize=${knownThreads.size} listenCh=${JSON.stringify(config.listenChannels)} text="${content.slice(0, 40)}"`);
    return;
  }
  debugLog(
    `Handle message channel=${channelId} from=${userId} reason=${triggerReason} text="${content.slice(0, 80)}"`,
  );

  // Authorization check (fail-closed — empty allowlist rejects everyone).
  if (config.allowedUserIds.length === 0) {
    if (isDM) {
      await sendMessage(config.token, channelId, "Unauthorized: no allowlist configured.");
    } else {
      debugLog(`Skip guild message channel=${channelId} reason=no_allowlist_configured`);
    }
    return;
  }
  if (!config.allowedUserIds.includes(userId)) {
    if (isDM) {
      await sendMessage(config.token, channelId, "Unauthorized.");
    } else {
      debugLog(`Skip guild message channel=${channelId} from=${userId} reason=unauthorized_user`);
    }
    return;
  }

  // Authorized inbound DM — pre-warm the verified-DM cache so broker egress
  // (brokerReply/brokerProgress) can pass its allowlist gate without paying the
  // channel lookup on the reply path; DM channel ids never appear in
  // channelDirectories or listenChannels. Verification (not a blind insert):
  // `!guild_id` is also true for group DMs (type 3), which must stay barred.
  if (isDM) void verifyDmChannel(config.token, channelId, config.allowedUserIds);

  // Detect attachments — image / voice get special handling (vision /
  // transcribe); everything else is a generic file we download + hand Claude the
  // path. A forward / poll / sticker carries no `content` but is still a real
  // inbound, so each keeps the message alive past the empty-content guard.
  const imageAttachments = message.attachments.filter(isImageAttachment);
  const voiceAttachments = message.attachments.filter(isVoiceAttachment);
  const fileAttachments = message.attachments.filter(
    (a) => !isImageAttachment(a) && !isVoiceAttachment(a),
  );
  const hasImage = imageAttachments.length > 0;
  const hasVoice = voiceAttachments.length > 0;
  const hasFile = fileAttachments.length > 0;
  const isForward =
    message.message_reference?.type === 1 || (message.message_snapshots?.length ?? 0) > 0;
  const hasPoll = Boolean(message.poll);
  const hasSticker = (message.sticker_items?.length ?? 0) > 0;

  if (
    !content.trim() &&
    !hasImage &&
    !hasVoice &&
    !hasFile &&
    !isForward &&
    !hasPoll &&
    !hasSticker
  ) {
    return;
  }

  // Strip bot mention from content for cleaner prompt
  let cleanContent = content;
  if (botUserId) {
    cleanContent = cleanContent.replace(new RegExp(`<@!?${botUserId}>`, "g"), "").trim();
  }

  const label = message.author.username;
  const mediaParts = [
    hasImage ? "image" : "",
    hasVoice ? "voice" : "",
    hasFile ? "file" : "",
    isForward ? "forward" : "",
    hasPoll ? "poll" : "",
    hasSticker ? "sticker" : "",
  ].filter(Boolean);
  const mediaSuffix = mediaParts.length > 0 ? ` [${mediaParts.join("+")}]` : "";
  console.log(
    `[${new Date().toLocaleTimeString()}] Discord ${label}${mediaSuffix}: "${cleanContent.slice(0, 60)}${cleanContent.length > 60 ? "..." : ""}"`,
  );

  // Typing indicator loop (Discord typing lasts 10s, fire every 8s)
  const typingInterval = setInterval(() => sendTyping(config.token, channelId), 8000);

  try {
    await sendTyping(config.token, channelId);

    let imagePath: string | null = null;
    let voicePath: string | null = null;
    let voiceTranscript: string | null = null;
    const filePaths: string[] = [];
    // Things we saw but can't fully process — surfaced to Claude so it can tell
    // the user instead of silently dropping (no "why did nothing happen?").
    const unsupportedNotes: string[] = [];

    if (hasFile) {
      for (const att of fileAttachments) {
        try {
          const p = await downloadDiscordAttachment(att, "file");
          if (p) filePaths.push(p);
          else
            unsupportedNotes.push(
              `a file "${att.filename}" (${Math.round(att.size / 1024)}KB) too large to download (>25MB)`,
            );
        } catch (err) {
          unsupportedNotes.push(`a file "${att.filename}" that failed to download`);
          console.error(
            `[Discord] Failed to download file ${att.filename} for ${label}: ${err instanceof Error ? err.message : err}`,
          );
        }
      }
    }

    if (hasImage) {
      try {
        imagePath = await downloadDiscordAttachment(imageAttachments[0], "image");
      } catch (err) {
        console.error(`[Discord] Failed to download image for ${label}: ${err instanceof Error ? err.message : err}`);
      }
    }

    if (hasVoice) {
      try {
        voicePath = await downloadDiscordAttachment(voiceAttachments[0], "voice");
      } catch (err) {
        console.error(`[Discord] Failed to download voice for ${label}: ${err instanceof Error ? err.message : err}`);
      }

      if (voicePath) {
        try {
          debugLog(`Voice file saved: path=${voicePath}`);
          voiceTranscript = await transcribeAudioToText(voicePath, {
            debug: discordDebug,
            log: (msg) => debugLog(msg),
          });
        } catch (err) {
          console.error(`[Discord] Failed to transcribe voice for ${label}: ${err instanceof Error ? err.message : err}`);
        }
      }
    }

    // --- Thread management: pattern-based intent classification ---
    // DISABLED by user (tszkan): keyword classifier (spawn/hire/deploy/開/派...)
    // false-fires on normal sentences and splits them into garbage thread names.
    // Flip ENABLE_THREAD_INTENT back to true to restore the feature.
    const ENABLE_THREAD_INTENT = false;
    if (ENABLE_THREAD_INTENT && isGuild && cleanContent.length < 200) {
      const intent = classifyThreadIntent(cleanContent);
      if (intent && intent.action === "hire" && intent.names.length > 0) {
        const results: string[] = [];
        for (const threadName of intent.names) {
          try {
            const thread = await discordApi<{ id: string; name: string }>(
              config.token,
              "POST",
              `/channels/${channelId}/threads`,
              {
                name: threadName,
                type: 11, // PUBLIC_THREAD
                auto_archive_duration: 4320, // 3 days
              },
            );
            knownThreads.set(thread.id, { parentId: channelId });
            // Don't pre-create session — let Claude CLI create it on first message
            // The real UUID will be captured and saved by runner.ts
            await sendMessage(config.token, thread.id, `🧵 Thread **${threadName}** created with independent session. Start chatting!`);
            results.push(`✅ **${threadName}** → <#${thread.id}>`);
            console.log(`[Discord] Thread created: ${thread.id} name="${threadName}" parent=${channelId} knownSize=${knownThreads.size}`);
          } catch (err) {
            results.push(`❌ **${threadName}** — ${err instanceof Error ? err.message : err}`);
          }
        }
        await sendMessage(config.token, channelId, results.join("\n"));
        return;
      }

      if (intent && intent.action === "fire" && intent.names.length > 0) {
        const results: string[] = [];
        for (const targetName of intent.names) {
          const targetLower = targetName.toLowerCase();
          let foundId: string | null = null;
          for (const [tid, info] of knownThreads.entries()) {
            if (info.parentId === channelId) {
              try {
                const ch = await discordApi<{ id: string; name: string }>(config.token, "GET", `/channels/${tid}`);
                if (ch.name.toLowerCase() === targetLower) {
                  foundId = tid;
                  break;
                }
              } catch { /* thread might be gone */ }
            }
          }
          if (foundId) {
            try {
              await removeThreadSession("discord", foundId);
              await discordApi(config.token, "DELETE", `/channels/${foundId}`);
              knownThreads.delete(foundId);
              results.push(`🗑️ **${targetName}** — deleted`);
            } catch (err) {
              results.push(`❌ **${targetName}** — ${err instanceof Error ? err.message : err}`);
            }
          } else {
            results.push(`❌ **${targetName}** — not found`);
          }
        }
        await sendMessage(config.token, channelId, results.join("\n"));
        return;
      }
    }

    // Skill routing: detect slash commands and resolve to SKILL.md prompts
    const command = cleanContent.startsWith("/") ? cleanContent.trim().split(/\s+/, 1)[0].toLowerCase() : null;
    let skillContext: string | null = null;
    if (command) {
      try {
        skillContext = await resolveSkillPrompt(command);
        if (skillContext) {
          debugLog(`Skill resolved for ${command}: ${skillContext.length} chars`);
        }
      } catch (err) {
        debugLog(`Skill resolution failed for ${command}: ${err instanceof Error ? err.message : err}`);
      }
    }

    // Build prompt (same pattern as Telegram)
    const promptParts = [`[Discord from ${label}]`];
    // If this message is a native reply to ANOTHER message, quote what it replied
    // to — otherwise a bare "呢個" / "this one" pointing at an earlier message is
    // unresolvable to Claude (it only sees the new message, not the reply target).
    const ref = message.referenced_message;
    if (ref && (ref.content ?? "").trim()) {
      const refAuthor = ref.author?.username ?? "someone";
      promptParts.push(`(In reply to ${refAuthor}: "${ref.content.trim().slice(0, 1500)}")`);
    }
    if (skillContext) {
      const args = cleanContent.trim().slice(command!.length).trim();
      promptParts.push(`<command-name>${command}</command-name>`);
      promptParts.push(skillContext);
      if (args) promptParts.push(`User arguments: ${args}`);
    } else if (cleanContent.trim()) {
      promptParts.push(`Message: ${cleanContent}`);
    }
    if (imagePath) {
      promptParts.push(`Image path: ${imagePath}`);
      promptParts.push("The user attached an image. Inspect this image file directly before answering.");
    } else if (hasImage) {
      promptParts.push("The user attached an image, but downloading it failed. Respond and ask them to resend.");
    }
    if (voiceTranscript) {
      promptParts.push(`Voice transcript: ${voiceTranscript}`);
      promptParts.push("The user attached voice audio. Use the transcript as their spoken message.");
    } else if (hasVoice) {
      promptParts.push(
        "The user attached voice audio, but it could not be transcribed. Respond and ask them to resend a clearer clip.",
      );
    }
    if (filePaths.length > 0) {
      for (const fp of filePaths) promptParts.push(`Attached file path: ${fp}`);
      promptParts.push(
        "The user attached the file(s) above (saved locally). Read them if relevant before answering.",
      );
    }
    if (isForward) {
      const snap = message.message_snapshots?.[0]?.message;
      const fwd: string[] = [];
      if (snap?.content?.trim()) fwd.push(snap.content.trim());
      if (snap?.attachments?.length) {
        fwd.push(`[${snap.attachments.length} attachment(s): ${snap.attachments.map((a) => a.filename).join(", ")}]`);
      }
      if (snap?.poll?.question?.text) fwd.push(`[forwarded poll: ${snap.poll.question.text}]`);
      promptParts.push(
        fwd.length > 0
          ? `Forwarded message:\n${fwd.join("\n")}`
          : "The user forwarded a message, but Hermes couldn't read its contents — tell them you see a forward but can't read it.",
      );
    }
    if (hasPoll) {
      const q = message.poll?.question?.text ?? "(no question text)";
      const opts = (message.poll?.answers ?? [])
        .map((a) => a.poll_media?.text)
        .filter((t): t is string => Boolean(t))
        .join(" | ");
      promptParts.push(
        `The user sent a POLL — question: "${q}"; options: ${opts || "(none)"}. ` +
          "You can see the question and options but NOT live vote counts. Acknowledge it and, if relevant, " +
          "tell the user you can't tally poll votes yet.",
      );
    }
    if (hasSticker) {
      const names = (message.sticker_items ?? []).map((s) => s.name).join(", ");
      promptParts.push(
        `The user sent sticker(s): ${names}. You see the sticker name(s) but not the image — respond to the intent.`,
      );
    }
    for (const note of unsupportedNotes) {
      promptParts.push(
        `NOTE: the user's message included ${note}. Tell the user you received it but can't process that part yet, so they're not left wondering why nothing happened.`,
      );
    }

    const prefixedPrompt = promptParts.join("\n");
    // Use thread-specific session if message is in a known thread
    const threadId = knownThreads.has(channelId) ? channelId : undefined;
    const statusSink = createDiscordStatusSink({
      transport: discordStatusTransport(config.token),
      channelId,
    });
    const result = await runUserMessage("discord", prefixedPrompt, threadId, statusSink, "discord", resolveChannelCwd(channelId), {
      channelId,
      messageId: message.id,
      userId,
      user: message.author.username,
    });

    if (result.exitCode !== 0) {
      await sendMessage(config.token, channelId, `Error (exit ${result.exitCode}): ${result.stderr || result.stdout || "Unknown error"}`);
    } else if (result.stdout === "" && config.useBrokerSessions) {
      // Broker lane ack-only sentinel: egress already happened via
      // shim->broker->brokerReply. Sending here would double-deliver. The flag
      // guard keeps this from swallowing a genuinely empty metered response.
    } else {
      const visibleText = extractSessionAndResultFromText(result.stdout || "").result ?? result.stdout ?? "";
      const { cleanedText, reactionEmoji } = extractReactionDirective(visibleText);
      if (reactionEmoji) {
        await sendReaction(config.token, channelId, message.id, reactionEmoji).catch((err) => {
          console.error(`[Discord] Failed to send reaction for ${label}: ${err instanceof Error ? err.message : err}`);
        });
      }
      await sendMessage(config.token, channelId, cleanedText || "(empty response)");
    }
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : String(err);
    console.error(`[Discord] Error for ${label}: ${errMsg}`);
    await sendMessage(config.token, channelId, `Error: ${errMsg}`);
  } finally {
    clearInterval(typingInterval);
  }
}

// --- Interaction handler (slash commands + button acks) ---

async function handleInteractionCreate(token: string, interaction: DiscordInteraction): Promise<void> {
  const config = getSettings().discord;
  const actorId = interaction.member?.user?.id ?? interaction.user?.id;

  // Fail-closed: empty allowlist rejects every slash-command interaction.
  if (config.allowedUserIds.length === 0 || !actorId || !config.allowedUserIds.includes(actorId)) {
    await respondToInteraction(interaction, { content: "Unauthorized.", flags: 64 });
    return;
  }

  // Slash commands (type 2)
  if (interaction.type === 2 && interaction.data?.name) {
    if (interaction.data.name === "start") {
      await respondToInteraction(interaction, {
        content: "Hello! Send me a message and I'll respond using Claude.\nUse `/reset` to start a fresh session.",
      });
      return;
    }

    if (interaction.data.name === "reset") {
      await resetSession(interaction.channel_id ? resolveChannelCwd(interaction.channel_id) : process.cwd());
      await respondToInteraction(interaction, {
        content: "Global session reset. Next message starts fresh.",
      });
      return;
    }

    if (interaction.data.name === "compact") {
      await respondToInteraction(interaction, { content: "⏳ Compacting session..." });
      const channelId = interaction.channel_id;
      const sink = channelId
        ? createDiscordStatusSink({
            transport: discordStatusTransport(config.token),
            channelId,
          })
        : undefined;
      const result = await compactCurrentSession(sink ? { sink } : undefined);
      await fetch(
        `${DISCORD_API}/webhooks/${applicationId}/${interaction.token}/messages/@original`,
        {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ content: result.message }),
        },
      );
      return;
    }

    if (interaction.data.name === "status") {
      const session = await peekSession(interaction.channel_id ? resolveChannelCwd(interaction.channel_id) : process.cwd());
      const settings = getSettings();
      if (!session) {
        await respondToInteraction(interaction, { content: "📊 No active session." });
        return;
      }
      const threadSessions = await listThreadSessions();
      const lines = [
        "📊 **Session Status**",
        `Session: \`${session.sessionId.slice(0, 8)}\``,
        `Turns: ${(session as any).turnCount ?? 0}`,
        `Model: ${settings.model || "default"}`,
        `Security: ${settings.security.level}`,
        `Created: ${session.createdAt}`,
        `Last used: ${session.lastUsedAt}`,
        `Compact warned: ${(session as any).compactWarned ? "yes" : "no"}`,
      ];
      if (threadSessions.length > 0) {
        lines.push("", `**Thread Sessions:** ${threadSessions.length}`);
        for (const ts of threadSessions.slice(0, 5)) {
          lines.push(`  Thread \`${ts.threadId.slice(0, 8)}\` → Session \`${ts.sessionId.slice(0, 8)}\` (${ts.turnCount} turns)`);
        }
        if (threadSessions.length > 5) {
          lines.push(`  ... and ${threadSessions.length - 5} more`);
        }
      }
      await respondToInteraction(interaction, { content: lines.join("\n") });
      return;
    }

    if (interaction.data.name === "context") {
      const contextCwd = interaction.channel_id ? resolveChannelCwd(interaction.channel_id) : process.cwd();
      const session = await peekSession(contextCwd);
      if (!session) {
        await respondToInteraction(interaction, { content: "No active session." });
        return;
      }
      const home = homedir();
      const projectSlug = projectSlugFromCwd(contextCwd);
      const jsonlPath = `${home}/.claude/projects/${projectSlug}/${session.sessionId}.jsonl`;
      if (!existsSync(jsonlPath)) {
        await respondToInteraction(interaction, { content: "Conversation file not found." });
        return;
      }
      try {
        const raw = await readFile(jsonlPath, "utf8");
        const fileLines = raw.trim().split("\n");
        let lastUsage: any = null;
        let totalOutput = 0;
        for (const line of fileLines) {
          try {
            const obj = JSON.parse(line);
            if (obj.message?.usage) lastUsage = obj.message.usage;
            if (obj.message?.usage?.output_tokens) totalOutput += obj.message.usage.output_tokens;
          } catch {}
        }
        if (!lastUsage) {
          await respondToInteraction(interaction, { content: "No usage data found." });
          return;
        }
        const input = lastUsage.input_tokens ?? 0;
        const cacheCreation = lastUsage.cache_creation_input_tokens ?? 0;
        const cacheRead = lastUsage.cache_read_input_tokens ?? 0;
        const totalContext = input + cacheCreation + cacheRead;
        const maxContext = 200000;
        const pct = ((totalContext / maxContext) * 100).toFixed(1);
        const filled = Math.round((Math.min(totalContext / maxContext, 1)) * 20);
        const bar = "█".repeat(filled) + "░".repeat(20 - filled);
        const msg = [
          `📐 **Context Window**`,
          `${bar} ${pct}%`,
          ``,
          `Total: \`${totalContext.toLocaleString()}\` / \`${maxContext.toLocaleString()}\` tokens`,
          `├ Input: \`${input.toLocaleString()}\``,
          `├ Cache creation: \`${cacheCreation.toLocaleString()}\``,
          `├ Cache read: \`${cacheRead.toLocaleString()}\``,
          `└ Output (cumulative): \`${totalOutput.toLocaleString()}\``,
          ``,
          `Turns: ${(session as any).turnCount ?? 0}`,
        ];
        await respondToInteraction(interaction, { content: msg.join("\n") });
      } catch (err) {
        await respondToInteraction(interaction, {
          content: `Failed to read context: ${err instanceof Error ? err.message : err}`,
        });
      }
      return;
    }

    // Skill fallthrough: names registered from discovered SKILL.md files are
    // resolved here. Anything that neither matches a hardcoded handler nor
    // resolves to a skill body gets the legacy "Unknown command" reply so
    // autocomplete-surfaced but now-missing names still get a response.
    const commandName = interaction.data.name;
    try {
      // Plugin skills are registered with discovery's `${plugin}_${skill}`
      // name but resolveSkillPrompt expects `${plugin}:${skill}`. If the
      // literal slug misses, retry with the first underscore rewritten.
      let skillContext = await resolveSkillPrompt(`/${commandName}`).catch(
        () => null,
      );
      if (!skillContext) {
        const firstUnderscore = commandName.indexOf("_");
        if (firstUnderscore > 0) {
          const pluginForm = `${commandName.slice(0, firstUnderscore)}:${commandName.slice(firstUnderscore + 1)}`;
          skillContext = await resolveSkillPrompt(`/${pluginForm}`).catch(
            () => null,
          );
        }
      }
      if (skillContext) {
        await respondToInteraction(interaction, {
          content: `⏳ Running /${commandName}…`,
        });

        const channelId = interaction.channel_id;
        const threadId =
          channelId && knownThreads.has(channelId) ? channelId : undefined;

        const promptParts = [
          `[Discord slash command /${commandName}]`,
          `<command-name>${commandName}</command-name>`,
          skillContext,
        ];
        const prefixedPrompt = promptParts.join("\n");

        const statusSink = channelId
          ? createDiscordStatusSink({
              transport: discordStatusTransport(config.token),
              channelId,
            })
          : undefined;

        const result = await runUserMessage(
          "discord-slash",
          prefixedPrompt,
          threadId,
          statusSink,
          "discord",
          channelId ? resolveChannelCwd(channelId) : process.cwd(),
        );

        const body =
          result.exitCode === 0
            ? extractReactionDirective(result.stdout || "").cleanedText ||
              "(empty response)"
            : `Error (exit ${result.exitCode}): ${result.stderr || result.stdout || "Unknown error"}`;

        await fetch(
          `${DISCORD_API}/webhooks/${applicationId}/${interaction.token}/messages/@original`,
          {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ content: body.slice(0, 2000) }),
          },
        ).catch((err) => {
          console.error(
            `[Discord] Failed to patch slash-command response: ${err}`,
          );
        });
        return;
      }
    } catch (err) {
      console.error(
        `[Discord] Slash-command /${commandName} failed: ${err instanceof Error ? err.message : err}`,
      );
      await respondToInteraction(interaction, {
        content: `Error running /${commandName}: ${err instanceof Error ? err.message : String(err)}`,
      }).catch(() => {});
      return;
    }

    // Unknown command
    await respondToInteraction(interaction, { content: "Unknown command." });
    return;
  }

  // Button interactions (type 3): no patterns are handled today, just ack
  // ephemerally so Discord stops the spinner.
  if (interaction.type === 3 && interaction.data?.custom_id) {
    await respondToInteraction(interaction, { content: "OK", flags: 64 });
    return;
  }

  // Default ack for any other interaction type
  await respondToInteraction(interaction, { content: "OK", flags: 64 });
}

// --- Guild join handler ---

async function handleGuildCreate(token: string, guild: DiscordGuild): Promise<void> {
  const config = getSettings().discord;

  // Skip guilds we were already in at READY time
  if (readyGuildIds?.has(guild.id)) return;

  const channelId = guild.system_channel_id;
  if (!channelId) return;

  console.log(`[Discord] Joined guild: ${guild.name} (${guild.id})`);

  const eventPrompt =
    `[Discord system event] I was added to a guild.\n` +
    `Guild name: ${guild.name}\n` +
    `Guild id: ${guild.id}\n` +
    "Write a short first message for the server. Confirm I was added and explain how to trigger me (mention or reply).";

  try {
    const result = await run("discord", eventPrompt, undefined, undefined, "cli", resolveChannelCwd(channelId));
    if (result.exitCode !== 0) {
      await sendMessage(config.token, channelId, "I was added to this server. Mention me to start.");
      return;
    }
    await sendMessage(config.token, channelId, result.stdout || "I was added to this server.");
  } catch {
    await sendMessage(config.token, channelId, "I was added to this server. Mention me to start.");
  }
}

// --- Gateway WebSocket ---

function sendWs(data: unknown): void {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function sendHeartbeat(): void {
  sendWs({ op: GatewayOp.HEARTBEAT, d: lastSequence });
  heartbeatAcked = false;
}

function startHeartbeat(): void {
  stopHeartbeat();
  // Reset the ack flag for the new connection. Without this, a heartbeat-
  // timeout-triggered close leaves `heartbeatAcked = false` from the dead
  // session, so the very first interval tick on the new socket sees a
  // missing ack and immediately self-closes — flap loop. `resetGatewayState`
  // does this too but only fires from `stopGateway`, not the auto-reconnect
  // path that goes through `ws.onclose → connectGateway → HELLO`.
  heartbeatAcked = true;
  // First heartbeat with jitter per Discord spec
  heartbeatJitterTimer = setTimeout(() => {
    heartbeatJitterTimer = null;
    sendHeartbeat();
  }, Math.random() * heartbeatIntervalMs);
  heartbeatTimer = setInterval(() => {
    if (!heartbeatAcked) {
      debugLog("Heartbeat not acked, reconnecting");
      ws?.close(4000, "Heartbeat timeout");
      return;
    }
    sendHeartbeat();
  }, heartbeatIntervalMs);
}

function stopHeartbeat(): void {
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  heartbeatTimer = null;
  if (heartbeatJitterTimer) clearTimeout(heartbeatJitterTimer);
  heartbeatJitterTimer = null;
}

function resetGatewayState(): void {
  heartbeatIntervalMs = 0;
  heartbeatAcked = true;
  lastSequence = null;
  gatewaySessionId = null;
  resumeGatewayUrl = null;
  readyGuildIds = null;
  botUserId = null;
  botUsername = null;
  applicationId = null;
  knownThreads.clear();
}

function sendIdentify(token: string): void {
  sendWs({
    op: GatewayOp.IDENTIFY,
    d: {
      token,
      intents: INTENTS,
      properties: {
        os: process.platform,
        browser: "claude-hermes",
        device: "claude-hermes",
      },
    },
  });
}

function sendResume(token: string): void {
  sendWs({
    op: GatewayOp.RESUME,
    d: {
      token,
      session_id: gatewaySessionId,
      seq: lastSequence,
    },
  });
}

// Non-recoverable close codes that should not trigger reconnection
const FATAL_CLOSE_CODES = new Set([4004, 4010, 4011, 4012, 4013, 4014]);

function handleDispatch(token: string, eventName: string, data: any): void {
  debugLog(`Dispatch: ${eventName}`);

  switch (eventName) {
    case "READY":
      gatewaySessionId = data.session_id;
      resumeGatewayUrl = data.resume_gateway_url;
      botUserId = data.user.id;
      botUsername = data.user.username;
      applicationId = data.application.id;
      // Track existing guilds so we don't send welcome messages on reconnect
      readyGuildIds = new Set((data.guilds ?? []).map((g: { id: string }) => g.id));
      console.log(`[Discord] Ready as ${data.user.username} (${data.user.id})`);
      registerSlashCommands(token).catch((err) =>
        console.error(`[Discord] Failed to register slash commands: ${err}`),
      );
      break;

    case "RESUMED":
      console.log("[Discord] Session resumed — rejoining threads");
      rejoinThreads(token).catch((err) =>
        console.error(`[Discord] Failed to rejoin threads on RESUMED: ${err}`),
      );
      break;

    case "MESSAGE_CREATE":
      console.log(`[Discord][GW] MESSAGE_CREATE ch=${data.channel_id} author=${data.author?.username} guild=${data.guild_id || 'DM'}`);
      handleMessageCreate(token, data).catch((err) =>
        console.error(`[Discord] MESSAGE_CREATE unhandled:`, err),
      );
      break;

    case "INTERACTION_CREATE":
      handleInteractionCreate(token, data).catch((err) =>
        console.error(`[Discord] INTERACTION_CREATE unhandled: ${err}`),
      );
      break;

    case "GUILD_CREATE":
      // Cache active threads for multi-session support
      if (data.threads) {
        console.log(`[Discord] GUILD_CREATE: ${data.threads.length} active threads in guild ${data.id}`);
        for (const thread of data.threads) {
          knownThreads.set(thread.id, { parentId: thread.parent_id });
          console.log(`[Discord]   thread: ${thread.id} name="${thread.name}" parent=${thread.parent_id}`);
        }
      } else {
        console.log(`[Discord] GUILD_CREATE: no active threads in guild ${data.id}`);
      }
      // Rejoin all known threads from sessions.json so gateway sends MESSAGE_CREATE
      rejoinThreads(token).catch((err) =>
        console.error(`[Discord] Failed to rejoin threads: ${err}`),
      );
      handleGuildCreate(token, data).catch((err) =>
        console.error(`[Discord] GUILD_CREATE unhandled: ${err}`),
      );
      break;

    case "THREAD_CREATE":
      if (data.id && data.parent_id) {
        knownThreads.set(data.id, { parentId: data.parent_id });
        debugLog(`Thread tracked: ${data.id} (parent: ${data.parent_id})`);
        // A bot only receives a thread's MESSAGE_CREATE once it's a MEMBER. Join
        // any thread under a channel we watch (listenChannels / channelDirectories)
        // so user-opened threads work too — not just bot-created ones (which
        // auto-join). Gated on watched parents so we don't join every guild thread.
        const cfg = getSettings().discord;
        const watched =
          cfg.listenChannels.includes(data.parent_id) ||
          Boolean(cfg.channelDirectories?.[data.parent_id]);
        if (watched) {
          void discordApi(token, "PUT", `/channels/${data.id}/thread-members/@me`).catch((err) =>
            console.error(`[Discord] Failed to join thread ${data.id}: ${err}`),
          );
        }
      }
      break;

    case "THREAD_DELETE":
      if (data.id) {
        knownThreads.delete(data.id);
        removeThreadSession("discord", data.id).catch((err) =>
          console.error(`[Discord] Failed to cleanup thread session: ${err}`),
        );
        debugLog(`Thread removed: ${data.id}`);
      }
      break;

    case "THREAD_UPDATE":
      if (data.id && data.parent_id) {
        if (data.thread_metadata?.archived) {
          knownThreads.delete(data.id);
          removeThreadSession("discord", data.id).catch((err) =>
            console.error(`[Discord] Failed to cleanup archived thread session: ${err}`),
          );
          debugLog(`Thread archived and cleaned up: ${data.id}`);
        } else {
          knownThreads.set(data.id, { parentId: data.parent_id });
        }
      }
      break;

    case "THREAD_LIST_SYNC":
      if (data.threads) {
        for (const thread of data.threads) {
          knownThreads.set(thread.id, { parentId: thread.parent_id });
        }
      }
      break;
  }
}

function handleGatewayPayload(token: string, payload: GatewayPayload): void {
  if (payload.s !== null) lastSequence = payload.s;

  switch (payload.op) {
    case GatewayOp.HELLO:
      heartbeatIntervalMs = payload.d.heartbeat_interval;
      startHeartbeat();
      if (gatewaySessionId && lastSequence !== null) {
        sendResume(token);
      } else {
        sendIdentify(token);
      }
      break;

    case GatewayOp.HEARTBEAT_ACK:
      heartbeatAcked = true;
      break;

    case GatewayOp.HEARTBEAT:
      // Server-requested heartbeat
      sendHeartbeat();
      break;

    case GatewayOp.RECONNECT:
      debugLog("Gateway requested reconnect");
      ws?.close(4000, "Reconnect requested");
      break;

    case GatewayOp.INVALID_SESSION: {
      const resumable = payload.d;
      debugLog(`Invalid session, resumable=${resumable}`);
      if (!resumable) {
        gatewaySessionId = null;
        lastSequence = null;
      }
      setTimeout(() => {
        if (resumable && gatewaySessionId) {
          sendResume(token);
        } else {
          sendIdentify(token);
        }
      }, 1000 + Math.random() * 4000);
      break;
    }

    case GatewayOp.DISPATCH:
      handleDispatch(token, payload.t!, payload.d);
      break;
  }
}

function connectGateway(token: string, url?: string): void {
  const gatewayUrl = url || GATEWAY_URL;
  debugLog(`Connecting to gateway: ${gatewayUrl}`);

  ws = new WebSocket(gatewayUrl);

  ws.onopen = () => {
    debugLog("Gateway WebSocket opened");
  };

  ws.onmessage = (event) => {
    try {
      const payload = JSON.parse(String(event.data)) as GatewayPayload;
      handleGatewayPayload(token, payload);
    } catch (err) {
      console.error(`[Discord] Failed to parse gateway payload: ${err}`);
    }
  };

  ws.onclose = (event) => {
    debugLog(`Gateway closed: code=${event.code} reason=${event.reason}`);
    stopHeartbeat();
    if (!running) return;

    // Fatal close codes — do not reconnect
    if (FATAL_CLOSE_CODES.has(event.code)) {
      console.error(`[Discord] Fatal close code ${event.code}: ${event.reason}. Not reconnecting.`);
      return;
    }

    // Attempt resume if we have session state
    const canResume = gatewaySessionId && lastSequence !== null;
    if (canResume) {
      debugLog("Attempting resume...");
      setTimeout(() => connectGateway(token, resumeGatewayUrl || undefined), 1000 + Math.random() * 2000);
    } else {
      // Full reconnect
      gatewaySessionId = null;
      lastSequence = null;
      resumeGatewayUrl = null;
      setTimeout(() => connectGateway(token), 3000 + Math.random() * 4000);
    }
  };

  ws.onerror = () => {
    // onclose will fire after onerror, reconnection handled there
  };
}

// --- Exports ---

/** Send a message to a specific channel (used by heartbeat forwarding) */
export { sendMessage, sendMessageToUser };

/** Stop gateway connection and clear runtime state (used for token rotation/hot reload). */
export function stopGateway(): void {
  running = false;
  stopHeartbeat();
  if (ws) {
    try {
      ws.close(1000, "Gateway stop requested");
    } catch {
      // best-effort
    }
    ws = null;
  }
  resetGatewayState();
}

process.on("SIGTERM", () => {
  stopGateway();
});
process.on("SIGINT", () => {
  stopGateway();
});

/** Start gateway connection in-process (called by start.ts when token is configured) */
export function startGateway(debug = false): void {
  discordDebug = debug;
  const config = getSettings().discord;
  if (ws) stopGateway();
  running = true;
  console.log("Discord bot started (gateway)");
  console.log(`  Allowed users: ${config.allowedUserIds.length === 0 ? "none (fail-closed)" : config.allowedUserIds.join(", ")}`);
  if (config.listenChannels.length > 0) {
    console.log(`  Listen channels: ${config.listenChannels.join(", ")}`);
  }
  if (discordDebug) console.log("  Debug: enabled");

  (async () => {
    await ensureProjectClaudeMd();
    connectGateway(config.token);
  })().catch((err) => {
    console.error(`[Discord] Fatal: ${err}`);
  });
}

/** Standalone entry point (bun run src/index.ts discord) */
export async function discord() {
  await loadSettings();
  await ensureProjectClaudeMd();
  const config = getSettings().discord;

  if (!config.token) {
    console.error("Discord token not configured. Set discord.token in .claude/hermes/settings.json");
    process.exit(1);
  }

  console.log("Discord bot started (gateway, standalone)");
  console.log(`  Allowed users: ${config.allowedUserIds.length === 0 ? "none (fail-closed)" : config.allowedUserIds.join(", ")}`);
  if (discordDebug) console.log("  Debug: enabled");

  connectGateway(config.token);
  // Keep process alive
  await new Promise(() => {});
}
