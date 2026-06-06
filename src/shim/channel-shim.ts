#!/usr/bin/env bun
/**
 * Hermes per-session channel shim (Phase 0 POC).
 *
 * One shim : one interactive Claude Code session (the channel protocol is a
 * stdio subprocess of exactly one session). Holds NO Discord connection — it
 * only carries the `claude/channel` capability + a `reply` tool. In the full
 * design it relays to the Hermes broker over a Unix socket; this POC variant
 * instead self-injects one inbound on boot and appends replies to a log file,
 * to prove the inbound→session→reply round-trip end to end with no broker.
 *
 * Launch (POC, via .mcp.json entry "hermes-shim"):
 *   claude --dangerously-load-development-channels server:hermes-shim
 * or via the masquerade (overwrite the official discord plugin's server.ts):
 *   claude --channels plugin:discord@claude-plugins-official
 */
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import { appendFileSync } from "node:fs";

const SESSION_KEY = process.env.HERMES_SESSION_KEY ?? "poc";
const REPLY_LOG = process.env.HERMES_REPLY_LOG ?? "/tmp/hermes-replies.ndjson";
const SELF_INJECT = process.env.HERMES_SHIM_SELF_INJECT !== "0"; // POC: emit one inbound on boot

const server = new Server(
  { name: "hermes-shim", version: "0.0.1" },
  {
    capabilities: {
      tools: {},
      // CRITICAL: registers the inbound channel listener.
      experimental: { "claude/channel": {} },
    },
    instructions:
      'Discord messages arrive as <channel source="discord" chat_id="..."> tags. ' +
      "When one arrives, call the `reply` tool with the chat_id from the tag and your reply " +
      "text — that is the user-facing message sent back to Discord.",
  },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: "reply",
      description:
        "Send a message back to the originating Discord chat. Pass back the chat_id you " +
        "received in the <channel> frame. You may call it multiple times (progress + final).",
      inputSchema: {
        type: "object",
        required: ["chat_id", "text"],
        properties: {
          chat_id: { type: "string", description: "Discord channel/thread id from meta.chat_id" },
          text: { type: "string" },
        },
      },
    },
  ],
}));

server.setRequestHandler(CallToolRequestSchema, async (req) => {
  if (req.params.name !== "reply") throw new Error(`unknown tool ${req.params.name}`);
  const a = req.params.arguments as { chat_id: string; text: string };
  // POC sink: append to a log file. TODO(broker): RPC the reply over broker.sock so the
  // broker performs the real Discord sendMessage(chat_id, text).
  appendFileSync(
    REPLY_LOG,
    JSON.stringify({ type: "reply", sessionKey: SESSION_KEY, chat_id: a.chat_id, text: a.text, at: new Date().toISOString() }) + "\n",
  );
  return { content: [{ type: "text", text: "sent (poc)" }] };
});

process.on("unhandledRejection", (e) => process.stderr.write(`unhandledRejection ${e}\n`));
process.on("uncaughtException", (e) => process.stderr.write(`uncaughtException ${e}\n`));

await server.connect(new StdioServerTransport());

if (SELF_INJECT) {
  // Prove the round-trip: push one inbound a moment after the session boots.
  setTimeout(() => {
    server
      .notification({
        method: "notifications/claude/channel",
        params: {
          content: "PING from Hermes POC smoke test. Reply with the reply tool now: chat_id=POC_CHAT, text=pong.",
          meta: { chat_id: "POC_CHAT", message_id: "1", user: "smoke-test" },
        },
      })
      .catch((e) => process.stderr.write(`inbound emit failed: ${e}\n`));
  }, 2500);
}
