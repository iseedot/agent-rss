/**
 * pi-agent-rss - RSS aggregation plugin (official pi package form)
 *
 * Layout:
 *   index.ts    Extension entry: tools, environment gate, optional scheduled fetch
 *   rss.py      Single-file Python backend (stdlib + feedparser only)
 *   INSTALL.md  AI-readable setup guide (referenced by the environment gate)
 *   data/       Runtime data directory (rss.db; override with RSS_DB_PATH)
 *
 * Tools: add / fetch / unread / search / markread / list / remove
 * Optional scheduling: set RSS_AUTO_FETCH_MINUTES (e.g. 60) to fetch
 * periodically and push new items to the agent.
 */
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Type } from "@earendil-works/pi-ai";
import { defineTool, type ExtensionAPI } from "@earendil-works/pi-coding-agent";

const execFileAsync = promisify(execFile);
const PYTHON = process.env.RSS_PY_PYTHON || "python3";
const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const PY = path.join(MODULE_DIR, "rss.py");
const INSTALL_MD = path.join(MODULE_DIR, "INSTALL.md");
const TIMEOUT_MS = 180_000;

/** Environment gate: python3 present + feedparser importable */
async function checkEnv(): Promise<string | null> {
  try {
    await execFileAsync(PYTHON, ["-c", "import feedparser"], { timeout: 15_000 });
    return null;
  } catch {
    let pythonOk = false;
    try {
      await execFileAsync(PYTHON, ["--version"], { timeout: 10_000 });
      pythonOk = true;
    } catch {
      pythonOk = false;
    }
    if (!pythonOk) {
      return (
        "❌ RSS plugin environment not ready: python3 not found.\n" +
        `Please read the setup guide and follow it: ${INSTALL_MD}`
      );
    }
    return (
      "❌ RSS plugin environment not ready: Python package 'feedparser' is missing.\n" +
      "Run: python3 -m pip install --user feedparser\n" +
      "(Ubuntu/Debian: sudo apt install -y python3-feedparser)\n" +
      "(If you see 'externally-managed-environment', add --break-system-packages)\n" +
      `See the setup guide: ${INSTALL_MD}`
    );
  }
}

/** Hint for git-installed packages: pi update resets the plugin directory */
function gitModeHint(): string {
  const isGitInstall = MODULE_DIR.includes(`${path.sep}git${path.sep}`);
  const hasDbOverride = !!process.env.RSS_DB_PATH;
  if (!isGitInstall || hasDbOverride) return "";
  return (
    "\nℹ️ Note: installed via git — `pi update` may reset the plugin directory and " +
    "lose data.\nSet the environment variable RSS_DB_PATH (e.g. ~/.pi/rss-data/rss.db) " +
    "to persist subscriptions and read state."
  );
}

/** Run rss.py and return its output text */
async function runPy(...args: string[]): Promise<string> {
  try {
    const { stdout } = await execFileAsync(PYTHON, [PY, ...args], {
      timeout: TIMEOUT_MS,
      maxBuffer: 10 * 1024 * 1024,
    });
    return stdout || "(no output)";
  } catch (e: any) {
    const detail = `${e?.stdout ?? ""}${e?.stderr ?? ""}`.trim();
    return `❌ RSS operation failed: ${e?.message ?? e}${detail ? `\n${detail}` : ""}`;
  }
}

const rssTool = defineTool({
  name: "rss",
  label: "RSS News",
  description:
    "RSS tool: add a subscription (add), fetch updates (fetch), list unread items (unread), " +
    "full-text search (search), mark items as read (markread), manage subscriptions (list/remove). " +
    "Data is stored in the plugin's data/rss.db by default (override with RSS_DB_PATH).",
  parameters: Type.Object({
    action: Type.Union([
      Type.Literal("add", { description: "Add a subscription; requires feed_url" }),
      Type.Literal("fetch", { description: "Fetch all enabled subscriptions" }),
      Type.Literal("unread", { description: "List unread items" }),
      Type.Literal("search", { description: "Full-text search; requires query" }),
      Type.Literal("markread", { description: "Mark items as read (all unread, or one by item_id)" }),
      Type.Literal("list", { description: "List subscriptions" }),
      Type.Literal("remove", { description: "Remove a subscription; requires feed_id" }),
    ]),
    feed_url: Type.Optional(
      Type.String({ description: "Feed URL (required for action=add)" }),
    ),
    query: Type.Optional(
      Type.String({ description: "Search keyword (required for action=search; supports FTS5 syntax)" }),
    ),
    limit: Type.Optional(
      Type.Integer({ description: "Max result count (unread default 20, search default 50)" }),
    ),
    item_id: Type.Optional(
      Type.Integer({ description: "Item ID (optional for markread; all unread when omitted)" }),
    ),
    feed_id: Type.Optional(
      Type.Integer({ description: "Feed ID (required for action=remove)" }),
    ),
  }),

  async execute(_toolCallId, params) {
    const envError = await checkEnv();
    if (envError) return { content: [{ type: "text", text: envError }] };

    const args = [params.action];

    if (params.action === "add") {
      if (!params.feed_url) return { content: [{ type: "text", text: "❌ action=add requires feed_url" }] };
      args.push(params.feed_url);
    } else if (params.action === "search") {
      if (!params.query) return { content: [{ type: "text", text: "❌ action=search requires query" }] };
      args.push(params.query);
    } else if (params.action === "markread" && params.item_id != null) {
      args.push(String(params.item_id));
    } else if (params.action === "remove") {
      if (params.feed_id == null) return { content: [{ type: "text", text: "❌ action=remove requires feed_id" }] };
      args.push(String(params.feed_id));
    }

    if (params.limit != null && (params.action === "unread" || params.action === "search")) {
      args.push("-l", String(params.limit));
    }

    let text = await runPy(...args);
    if (params.action === "fetch") text += gitModeHint();
    return { content: [{ type: "text", text }] };
  },
});

export default function (pi: ExtensionAPI) {
  pi.registerTool(rssTool);

  // Optional scheduled fetch (disabled by default)
  const autoMinutes = Number(process.env.RSS_AUTO_FETCH_MINUTES || "0");
  if (autoMinutes > 0) {
    const intervalMs = autoMinutes * 60 * 1000;
    setInterval(async () => {
      try {
        const text = await runPy("fetch");
        const match = text.match(/New items: (\d+)/);
        const newCount = match ? Number(match[1]) : 0;
        if (newCount > 0) {
          pi.sendUserMessage(
            `📡 RSS scheduled fetch complete: ${newCount} new item(s).\n${text}`,
            { triggerTurn: false },
          );
        }
      } catch (e) {
        console.error("[rss] scheduled fetch failed:", e);
      }
    }, intervalMs);
    console.log(`[rss] scheduled fetch enabled: every ${autoMinutes} minutes`);
  } else {
    console.log("[rss] scheduled fetch disabled (set RSS_AUTO_FETCH_MINUTES to enable)");
  }
}