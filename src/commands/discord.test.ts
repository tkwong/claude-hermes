import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { reloadSettings, resetSettingsCacheForTest } from "../config";
import { workspaceKey } from "../router/session-key";
import { brokerProgress, brokerReply, sendReaction } from "./discord";

// PL-2: sendReaction should route through discordApi() so that a Discord 429
// rate-limit is retried (honoring `retry_after`) instead of silently dropping
// the reaction. Today the function calls raw fetch(...).catch(() => {}), so a
// 429 is swallowed on the first hit and `fetch` is invoked exactly once. Once
// the body is swapped to `await discordApi(...)`, the helper will re-fetch and
// the call count becomes 2.

describe("discord sendReaction — PL-2", () => {
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  test("retries on 429 rate-limit via discordApi", async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];

    globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      calls.push({ url, init });
      if (calls.length === 1) {
        return Promise.resolve(
          new Response(JSON.stringify({ retry_after: 0.01 }), {
            status: 429,
            headers: { "Content-Type": "application/json" },
          })
        );
      }
      return Promise.resolve(new Response(null, { status: 204 }));
    }) as unknown as typeof globalThis.fetch;

    await sendReaction("fake-token", "fake-channel", "fake-message", "\u{1F44D}");

    // Two fetches: first 429, then the retry issued by discordApi().
    expect(calls.length).toBe(2);

    const finalUrl = calls[calls.length - 1]?.url ?? "";
    expect(finalUrl).toContain("/channels/fake-channel/messages/fake-message/reactions/");
    expect(finalUrl.endsWith("/@me")).toBe(true);
    // Thumbs-up (U+1F44D) URL-encodes to %F0%9F%91%8D.
    expect(finalUrl).toContain("/reactions/%F0%9F%91%8D/");
  });
});

// Regression (2026-06-10): broker egress dropped every DM reply. The lane
// answered ("而家上番綫未？" → lane workspace:cd582b66cbda replied in 20s) but
// brokerReply threw `chat_id not allowlisted` because the allowlist union was
// only channelDirectories ∪ listenChannels ∪ knownThreads — a DM channel id is
// never in any of those. Fix: verifyDmChannel — GET /channels/{id}, allow only
// type-1 channels whose recipient is an allowlisted user, cached per boot
// (pre-warmed on authorized inbound DMs). Group DMs (type 3) stay barred, and
// brokerProgress runs the same gate (an ungated lane-supplied chat_id would
// bypass the reply gate entirely).
describe("brokerReply — DM egress", () => {
  const originalFetch = globalThis.fetch;
  const originalCwd = process.cwd();
  let tempDir: string;
  let sessionKey: string;

  beforeAll(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "hermes-broker-dm-"));
    await mkdir(join(tempDir, ".claude", "hermes"), { recursive: true });
    await Bun.write(
      join(tempDir, ".claude", "hermes", "settings.json"),
      JSON.stringify({
        discord: {
          token: "fake-token",
          allowedUserIds: [111222333],
          listenChannels: ["555"],
          channelDirectories: {},
        },
      })
    );
    process.chdir(tempDir);
    await reloadSettings();
    // Ownership key for a DM lane = workspace key of the daemon cwd fallback.
    // Must be computed AFTER chdir (macOS tmpdir is a /var → /private/var symlink
    // and process.cwd() returns the resolved form).
    sessionKey = workspaceKey(process.cwd());
  });

  afterAll(async () => {
    process.chdir(originalCwd);
    resetSettingsCacheForTest();
    await rm(tempDir, { recursive: true, force: true });
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  function mockDiscord(channelInfo: Record<string, unknown> | null) {
    const calls: Array<{ method: string; url: string; body?: string }> = [];
    globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      const method = init?.method ?? "GET";
      calls.push({ method, url, body: typeof init?.body === "string" ? init.body : undefined });
      if (method === "GET" && /\/channels\/\d+$/.test(url)) {
        if (channelInfo === null) return new Response("{}", { status: 404 });
        return new Response(JSON.stringify(channelInfo), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (method === "POST" && url.includes("/messages")) {
        return new Response(JSON.stringify({ id: "m1" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      return new Response("{}", { status: 200, headers: { "Content-Type": "application/json" } });
    }) as unknown as typeof globalThis.fetch;
    return calls;
  }

  test("unverified DM channel is allowed via channel-lookup fallback", async () => {
    const calls = mockDiscord({ type: 1, recipients: [{ id: "111222333" }] });

    await brokerReply(sessionKey, "900001", "pong");

    const lookup = calls.find((c) => c.method === "GET" && c.url.endsWith("/channels/900001"));
    expect(lookup).toBeDefined();
    const post = calls.find((c) => c.method === "POST" && c.url.includes("/channels/900001/messages"));
    expect(post).toBeDefined();
    expect(post!.body).toContain("pong");
  });

  test("verified DM channel is cached — second reply skips the lookup", async () => {
    // channelInfo=null → a GET would 404 and fail closed, so a successful POST
    // proves the verdict came from the cache populated by the previous test.
    const calls = mockDiscord(null);

    await brokerReply(sessionKey, "900001", "pong2");

    expect(calls.find((c) => c.method === "GET" && c.url.endsWith("/channels/900001"))).toBeUndefined();
    const post = calls.find((c) => c.method === "POST" && c.url.includes("/channels/900001/messages"));
    expect(post).toBeDefined();
  });

  test("group DM (type 3) is rejected even with an allowlisted recipient", async () => {
    const calls = mockDiscord({ type: 3, recipients: [{ id: "111222333" }, { id: "424242" }] });

    await expect(brokerReply(sessionKey, "900003", "nope")).rejects.toThrow(/not allowlisted/);
    expect(calls.find((c) => c.method === "POST")).toBeUndefined();
  });

  test("unknown guild channel is still rejected", async () => {
    const calls = mockDiscord({ type: 0 });

    await expect(brokerReply(sessionKey, "900004", "nope")).rejects.toThrow(/not allowlisted/);
    expect(calls.find((c) => c.method === "POST")).toBeUndefined();
  });

  test("DM whose recipient is not allowlisted is rejected", async () => {
    const calls = mockDiscord({ type: 1, recipients: [{ id: "999999" }] });

    await expect(brokerReply(sessionKey, "900005", "nope")).rejects.toThrow(/not allowlisted/);
    expect(calls.find((c) => c.method === "POST")).toBeUndefined();
  });

  test("brokerProgress runs the same egress gate", async () => {
    // Foreign guild channel: rejected before any message POST.
    const denied = mockDiscord({ type: 0 });
    await expect(brokerProgress(sessionKey, "900006", "🔍 …")).rejects.toThrow(/not allowlisted/);
    expect(denied.find((c) => c.method === "POST")).toBeUndefined();

    // Verified DM (cached by the fallback test above): status message posts.
    const ok = mockDiscord(null);
    await brokerProgress(sessionKey, "900001", "🔍 reading memory");
    const post = ok.find((c) => c.method === "POST" && c.url.includes("/channels/900001/messages"));
    expect(post).toBeDefined();
    expect(post!.body).toContain("reading memory");
  });
});
