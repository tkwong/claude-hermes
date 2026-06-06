import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import * as fs from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { claudeProjectMemoryDir } from "../runtime/claude-paths";

const ORIG_CWD = process.cwd();
const ORIG_HOME = process.env.HOME;
let tempRoot: string;
let tempHome: string;
let memoryDir: string;
let mem: typeof import("./index");

beforeAll(async () => {
  tempRoot = await fs.mkdtemp(join(tmpdir(), "hermes-memory-"));
  // Memory now lives under Claude Code's auto-memory dir (derived from $HOME).
  // Isolate $HOME to a temp dir so the layer files land where the reader looks.
  tempHome = await fs.mkdtemp(join(tmpdir(), "hermes-memory-home-"));
  process.env.HOME = tempHome;
  process.chdir(tempRoot);
  // Derive the memory dir from process.cwd() (which may differ from tempRoot
  // when tmpdir is a symlink, e.g. /var -> /private/var on macOS) so it matches
  // exactly what the memory readers/writers resolve from process.cwd().
  memoryDir = claudeProjectMemoryDir(tempHome, process.cwd());
  await fs.mkdir(memoryDir, { recursive: true });
  mem = await import("./index");
});

afterAll(async () => {
  process.chdir(ORIG_CWD);
  if (ORIG_HOME === undefined) delete process.env.HOME;
  else process.env.HOME = ORIG_HOME;
  await fs.rm(tempRoot, { recursive: true, force: true });
  await fs.rm(tempHome, { recursive: true, force: true });
});

describe("memory files", () => {
  test("missing layer returns empty string", async () => {
    const soul = await mem.readSoul();
    expect(soul).toBe("");
  });

  test("write+read USER.md round-trips", async () => {
    await mem.writeUserMemory("owner: alice\nlikes: indigo");
    const content = await mem.readUserMemory();
    expect(content).toContain("alice");
    expect(content).toContain("indigo");
  });

  test("appendCrossSessionMemory stacks entries with timestamps", async () => {
    await mem.appendCrossSessionMemory("postgres port is 6543");
    await mem.appendCrossSessionMemory("ci uses bun test");
    const body = await mem.readCrossSessionMemory();
    expect(body).toContain("postgres port is 6543");
    expect(body).toContain("ci uses bun test");
    expect(body.split("<!-- ").length).toBeGreaterThanOrEqual(3);
  });

  test("channel memory is stored per channel id", async () => {
    await mem.writeChannelMemory("C1", "greet with claws");
    await mem.writeChannelMemory("C2", "announcements only");
    const one = await mem.readChannelMemory("C1");
    const two = await mem.readChannelMemory("C2");
    expect(one).toContain("claws");
    expect(two).toContain("announcements");
  });
});

describe("system prompt composition", () => {
  test("concatenates present layers in SOUL→IDENTITY→USER→MEMORY→CHANNEL order", async () => {
    await fs.writeFile(join(memoryDir, "SOUL.md"), "SOUL-text");
    await fs.writeFile(join(memoryDir, "IDENTITY.md"), "ID-text");
    await mem.writeUserMemory("USER-text");
    await mem.appendCrossSessionMemory("MEM-fact");
    await mem.writeChannelMemory("CX", "CHANNEL-play");

    const prompt = await mem.composeSystemPrompt({
      channelId: "CX",
      memoryScope: "channel",
    });

    const soulIdx = prompt.indexOf("SOUL-text");
    const idIdx = prompt.indexOf("ID-text");
    const memIdx = prompt.indexOf("MEM-fact");
    const chanIdx = prompt.indexOf("CHANNEL-play");
    expect(soulIdx).toBeGreaterThanOrEqual(0);
    expect(idIdx).toBeGreaterThan(soulIdx);
    expect(memIdx).toBeGreaterThan(idIdx);
    expect(chanIdx).toBeGreaterThan(memIdx);
  });

  test("memoryScope=none omits MEMORY + USER + CHANNEL layers", async () => {
    const prompt = await mem.composeSystemPrompt({ memoryScope: "none" });
    expect(prompt).not.toContain("USER-text");
    expect(prompt).not.toContain("MEM-fact");
    expect(prompt).not.toContain("CHANNEL-play");
  });

  test("maxBytes trims the tail", async () => {
    const prompt = await mem.composeSystemPrompt({
      memoryScope: "user",
      maxBytes: 20,
    });
    expect(prompt.length).toBeLessThanOrEqual(20);
  });
});

describe("nudge extractor", () => {
  test("heuristic extractor finds 'my X is Y' facts", async () => {
    const facts = await mem.extractFacts([
      { role: "user", content: "my postgres port is 6543" },
      { role: "assistant", content: "noted" },
      { role: "user", content: "remember that deploys need the gateway restarted" },
    ]);
    expect(facts.length).toBe(2);
    expect(facts[0]).toEqual({ scope: "user", key: "postgres port", value: "6543" });
    expect(facts[1]?.scope).toBe("workspace");
  });

  test("setExtractor overrides the default", async () => {
    mem.setExtractor(async () => [{ scope: "user", key: "synthetic", value: "ok" }]);
    const facts = await mem.extractFacts([]);
    expect(facts).toEqual([{ scope: "user", key: "synthetic", value: "ok" }]);
    mem.resetExtractor();
  });
});
