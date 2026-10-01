/**
 * pi-agent-rss - RSS report engine for pi (official pi package form)
 *
 * Layout:
 *   index.ts    Extension entry: tools, jev triage/lint/verification, credentials
 *   rss.py      Single-file Python backend (stdlib + feedparser only)
 *   INSTALL.md  AI-readable setup guide (referenced by the environment gate)
 *   data/       Legacy data directory (auto-migrated on first run)
 *
 * Report pipeline (3 calls per scheduled run):
 *   brief   fetch + select unread + pre-filter + jev triage + ledger diff + last report
 *   qa      deterministic checks + jev line lint + web verification (hard budget)
 *   commit  final gate: header, save MM-DD_hh.md (Beijing), ledger, mark batch read
 * Basic: fetch / unread / markread;  Admin: manage;  Calibration: calibrate
 *
 * Credentials: TYPESAFE_API_KEY env -> <agent-dir>/rss-plugin/auth.json (0600) ->
 * TYPESAFE_KEY_FILE or ~/.config/typesafe/key. The resolved key is injected into the
 * environment so pi's typesafe provider can authenticate classifier requests.
 */
import { execFile } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Type } from "@earendil-works/pi-ai";
import { defineTool, type ExtensionAPI } from "@earendil-works/pi-coding-agent";

const PYTHON = process.env.RSS_PY_PYTHON || "python3";
const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const PY = path.join(MODULE_DIR, "rss.py");
const INSTALL_MD = path.join(MODULE_DIR, "INSTALL.md");

/** Environment gate: python3 present + feedparser importable */
async function checkEnv(): Promise<string | null> {
  try {
    await new Promise<void>((resolve, reject) => {
      execFile(PYTHON, ["-c", "import feedparser"], { timeout: 15_000 }, (err) =>
        err ? reject(err) : resolve(),
      );
    });
    return null;
  } catch {
    let pythonOk = false;
    try {
      await new Promise<void>((resolve, reject) => {
        execFile(PYTHON, ["--version"], { timeout: 10_000 }, (err) =>
          err ? reject(err) : resolve(),
        );
      });
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

/** Run rss.py, optionally feeding stdin; returns stdout/stderr/exit code */
function runPy(
  args: string[],
  stdin?: string,
  timeoutMs = 180_000,
): Promise<{ stdout: string; stderr: string; code: number }> {
  return new Promise((resolve) => {
    const child = execFile(
      PYTHON,
      [PY, ...args],
      { timeout: timeoutMs, maxBuffer: 64 * 1024 * 1024 },
      (err: any, stdout: string, stderr: string) => {
        resolve({
          stdout: stdout ?? "",
          stderr: stderr ?? "",
          code: err ? (typeof err.code === "number" ? err.code : 1) : 0,
        });
      },
    );
    if (stdin !== undefined) {
      child.stdin?.on("error", () => {});
      child.stdin?.end(stdin, "utf-8");
    }
  });
}

type Envelope = { ok: boolean; action: string; data: any; error?: string | null; text?: string | null };

/** Run rss.py with --json and parse the envelope */
async function runPyJson(args: string[], stdin?: string, timeoutMs?: number): Promise<Envelope> {
  const { stdout, stderr } = await runPy(args, stdin, timeoutMs);
  try {
    const parsed = JSON.parse(stdout.trim());
    if (parsed && typeof parsed === "object") return parsed as Envelope;
    throw new Error("not an object");
  } catch {
    const detail = (stderr || stdout || "invalid JSON from rss.py").trim().slice(0, 2000);
    return { ok: false, action: "", data: null, error: detail };
  }
}

// ---------------------------------------------------------------------------
// Credentials (TYPESAFE_API_KEY): env -> plugin store -> shared file
// ---------------------------------------------------------------------------

function agentDir(): string {
  const raw = process.env.PI_CODING_AGENT_DIR ?? path.join(homedir(), ".pi", "agent");
  return raw.startsWith("~") ? path.join(homedir(), raw.slice(1)) : raw;
}

function pluginDir(): string {
  const dir = path.join(agentDir(), "rss-plugin");
  try {
    mkdirSync(dir, { recursive: true, mode: 0o700 });
  } catch {
    /* ignore */
  }
  return dir;
}

function readKeyFile(filePath: string, requirePrivate: boolean): string | null {
  try {
    if (!existsSync(filePath)) return null;
    const st = statSync(filePath);
    if (requirePrivate && (st.mode & 0o077) !== 0) {
      console.error(
        `[rss] refusing to read ${filePath}: mode ${(st.mode & 0o777).toString(8)} (expected 600)`,
      );
      return null;
    }
    const raw = readFileSync(filePath, "utf-8").trim();
    if (!raw) return null;
    if (raw.startsWith("{")) {
      try {
        const parsed = JSON.parse(raw);
        const value =
          parsed.typesafeApiKey ?? parsed.apiKey ?? parsed.TYPESAFE_API_KEY ?? parsed.key ?? "";
        return String(value).trim() || null;
      } catch {
        return null;
      }
    }
    return raw;
  } catch {
    return null;
  }
}

function resolveTypesafeKey(): { key: string | null; source: string | null } {
  const fromEnv = process.env.TYPESAFE_API_KEY?.trim();
  if (fromEnv) return { key: fromEnv, source: "env" };

  const store = path.join(pluginDir(), "auth.json");
  const fromStore = readKeyFile(store, true);
  if (fromStore) return { key: fromStore, source: store };

  const shared =
    process.env.TYPESAFE_KEY_FILE?.trim() || path.join(homedir(), ".config", "typesafe", "key");
  const fromShared = readKeyFile(shared, false);
  if (fromShared) return { key: fromShared, source: shared };

  return { key: null, source: null };
}

// ---------------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------------

function clampInt(raw: string | undefined, fallback: number, min: number, max: number): number {
  if (raw === undefined || raw.trim() === "") return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(value)));
}

function clampFloat(raw: string | undefined, fallback: number): number {
  if (raw === undefined || raw.trim() === "") return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value)) return fallback;
  return Math.min(1, Math.max(0, value));
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

async function mapLimit<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let cursor = 0;
  const workers = Array.from({ length: Math.max(1, Math.min(limit, items.length || 1)) }, async () => {
    while (true) {
      const idx = cursor++;
      if (idx >= items.length) return;
      out[idx] = await fn(items[idx]);
    }
  });
  await Promise.all(workers);
  return out;
}

function toolResultToText(result: { content?: any[]; structuredContent?: any } | undefined): string {
  const parts: string[] = [];
  if (result?.structuredContent !== undefined) {
    try {
      parts.push(JSON.stringify(result.structuredContent));
    } catch {
      /* ignore */
    }
  }
  for (const block of result?.content ?? []) {
    if (block?.type === "text" && typeof block.text === "string") parts.push(block.text);
  }
  return parts.join("\n");
}

// ---------------------------------------------------------------------------
// jev (classifier) plumbing
// ---------------------------------------------------------------------------

/** Structural subset of ExtensionToolContext that this extension needs. */
interface ClassifierCtx {
  modelRegistry: {
    findOfType(type: "classifier", provider: string, modelId: string): any;
    hasConfiguredAuth(model: any): boolean;
    classify(model: any, context: any, options?: any): Promise<any>;
  };
  executeTool(
    name: string,
    args: unknown,
    options?: { signal?: AbortSignal },
  ): Promise<{ result: { content?: any[]; structuredContent?: any }; isError: boolean }>;
}

function resolveJevModel(ctx: ClassifierCtx): { model: any | null; spec: string; reason?: string } {
  const spec = process.env.RSS_JE_V_MODEL?.trim() || "typesafe/jev-latest";
  const slash = spec.indexOf("/");
  const provider = slash > 0 ? spec.slice(0, slash) : "typesafe";
  const modelId = slash > 0 ? spec.slice(slash + 1) : spec;
  let model = ctx.modelRegistry.findOfType("classifier", provider, modelId);
  if (!model && spec !== "typesafe/jev-latest") {
    model = ctx.modelRegistry.findOfType("classifier", "typesafe", "jev-latest");
  }
  if (!model) return { model: null, spec, reason: `classifier ${spec} not found` };
  return { model, spec };
}

async function classifyWithRetry(
  ctx: ClassifierCtx,
  model: any,
  context: any,
  deadline: number,
): Promise<any | null> {
  const delays = [800, 2000, 5000];
  for (let attempt = 0; attempt <= delays.length; attempt += 1) {
    if (Date.now() > deadline) return null;
    const res = await ctx.modelRegistry.classify(model, context);
    if (res?.stopReason !== "error") return res;
    const message = String(res?.errorMessage ?? "");
    if (!/429|rate.?limit|too many/i.test(message)) return null;
    if (attempt === delays.length) return null;
    await sleep(delays[attempt]);
  }
  return null;
}

// ---- triage (brief stage) ----

type TriageInput = {
  id: number;
  title?: string | null;
  summary?: string | null;
  feed?: string | null;
  tags?: string | null;
  numbers?: string[];
};

type LedgerInput = {
  prev_facts?: { id: number; text: string }[];
  unconfirmed?: { id: number; text: string }[];
  active_opinions?: { id: number; source?: string; claim?: string; conclusion?: string }[];
};

type TriageAnnotation = {
  id: number;
  kind?: string;
  topic?: string;
  has_number?: boolean;
  value?: number;
  kind_confidence?: number;
  relation?: string;
  relation_confidence?: number;
  change_hint?: string;
  verify_needed?: boolean;
  repeat_of?: number;
  new_number?: boolean;
  is_skipped?: boolean;
  flags?: string[];
  numbers?: string[];
  source?: string;
};

const RELATION_HINTS: Record<string, string> = {
  new: "新出现",
  reversal: "结论反转",
  reinforce: "被强化",
  weaken: "被削弱",
  none: "无变化",
  na: "",
};

type TriageResult = {
  annotations: TriageAnnotation[];
  mode: "jev" | "partial" | "heuristic";
  model?: string;
  reason?: string;
  batches: number;
  failures: number;
};

async function runJevTriage(
  ctx: ClassifierCtx,
  items: TriageInput[],
  ledger: LedgerInput,
  deadlineMs: number,
): Promise<TriageResult> {
  const resolved = resolveJevModel(ctx);
  if (!resolved.model) {
    return { annotations: [], mode: "heuristic", reason: resolved.reason, batches: 0, failures: 0 };
  }
  if (!ctx.modelRegistry.hasConfiguredAuth(resolved.model)) {
    return {
      annotations: [],
      mode: "heuristic",
      model: resolved.spec,
      reason: "no credentials for classifier",
      batches: 0,
      failures: 0,
    };
  }
  const batchSize = clampInt(process.env.RSS_JE_V_BATCH, 10, 1, 20);
  const concurrency = clampInt(process.env.RSS_JE_V_CONCURRENCY, 4, 1, 8);
  const skipConf = clampFloat(process.env.RSS_JE_V_SKIP_CONF, 0.7);
  const noneConf = clampFloat(process.env.RSS_JE_V_NONE_CONF, 0.7);
  const deadline = Date.now() + Math.max(20_000, deadlineMs);
  const info = new Map<number, TriageAnnotation>();
  let batches = 0;
  let failures = 0;
  const timedOut = () => Date.now() > deadline;

  const boolQ = (instructions: string) => ({
    type: "bool" as const,
    instructions,
    criteria: { true: "yes", false: "no" },
  });

  // ---- pass 1: kind / topic / number / value (all items) ----
  await mapLimit(chunk(items, batchSize), concurrency, async (batch) => {
    if (timedOut()) return;
    batches += 1;
    const state: any = { items: {} };
    const questions: any = {};
    batch.forEach((it, k) => {
      const p = `i${k}`;
      state.items[p] = {
        feed: it.feed ?? "",
        tags: it.tags ?? "",
        title: it.title ?? "",
        summary: (it.summary ?? "").slice(0, 500),
      };
      questions[`${p}_kind`] = {
        type: "choice",
        instructions:
          `Item ${p}: does its main content state facts/events/data, or does it contain judgments ` +
          `(predictions, recommendations, evaluations, causal claims)? Sports, entertainment, ads ` +
          `and empty content are skip. (中文：主要陈述事实/数据，还是含预测/建议/评价等判断？体育娱乐广告为 skip)`,
        criteria: {
          fact: "facts / events / data only, no judgment (只陈述事实数据)",
          opinion: "contains a judgment (含判断/预测/评价)",
          mixed: "both facts and judgments (事实与判断都有)",
          skip: "sports / entertainment / ads / no substance (体育娱乐广告无内容)",
        },
      };
      questions[`${p}_topic`] = {
        type: "choice",
        instructions: `Item ${p}: pick the single best topic. (主题归类)`,
        criteria: {
          国际经济: "international economy / markets",
          国内经济: "China economy / markets",
          国际形势: "international politics / geopolitics",
          国内形势: "China domestic affairs",
          政策: "policy / regulation",
          科技: "technology / AI / software",
          民生: "livelihood / society",
          趣闻: "quirky / interesting",
          其他: "other",
        },
      };
      questions[`${p}_has_number`] = boolQ(
        `Item ${p}: does it contain a concrete number (percent / amount / date) usable in a headline? (是否有可入标题的具体数字)`,
      );
      questions[`${p}_value`] = {
        type: "score",
        instructions:
          `Item ${p}: how valuable is it for a Chinese reader following global economy, policy, ` +
          `geopolitics, technology and markets? (对关注宏观/政策/科技/市场的读者有多大价值)`,
        criteria: ["0 = noise", "3 = low", "5 = useful", "8 = important", "10 = must-know today"],
      };
    });
    const res = await classifyWithRetry(ctx, resolved.model, { state, questions }, deadline);
    if (!res) {
      failures += 1;
      return;
    }
    batch.forEach((it, k) => {
      const p = `i${k}`;
      const a = res.answers ?? {};
      info.set(it.id, {
        id: it.id,
        kind: a[`${p}_kind`]?.choice,
        kind_confidence: a[`${p}_kind`]?.confidence,
        topic: a[`${p}_topic`]?.choice,
        has_number: (a[`${p}_has_number`]?.probability ?? 0) > 0.5,
        value: typeof a[`${p}_value`]?.score === "number" ? a[`${p}_value`].score : undefined,
        numbers: it.numbers,
        source: "jev",
      });
    });
  });

  // ---- pass 2: opinion relation vs active ledger ----
  const opinionItems = items.filter((it) => ["opinion", "mixed"].includes(info.get(it.id)?.kind ?? ""));
  const active = (ledger.active_opinions ?? []).slice(0, 12);
  if (opinionItems.length) {
    const targetCriteria: Record<string, string> = { none: "none of them / no comparable claim" };
    active.forEach((o) => {
      targetCriteria[`o${o.id}`] = `${o.source ?? "?"}: ${String(o.claim ?? "").slice(0, 60)}`;
    });
    await mapLimit(chunk(opinionItems, Math.min(batchSize, 8)), concurrency, async (batch) => {
      if (timedOut()) return;
      batches += 1;
      const state: any = {
        active_opinions: active.map((o) => ({
          id: `o${o.id}`,
          source: o.source ?? "",
          claim: o.claim ?? "",
          conclusion: o.conclusion ?? "",
        })),
        items: {},
      };
      const questions: any = {};
      batch.forEach((it, k) => {
        const p = `i${k}`;
        state.items[p] = {
          feed: it.feed ?? "",
          title: it.title ?? "",
          summary: (it.summary ?? "").slice(0, 600),
        };
        if (active.length) {
          questions[`${p}_relation`] = {
            type: "choice",
            instructions:
              `Item ${p}: compare its judgment with active_opinions. Is it a new claim, does it reverse / ` +
              `reinforce / weaken one of them, or is it substantively the same with no new information? ` +
              `(与上期台账相比：新观点/反转/强化/削弱/无变化)`,
            criteria: {
              new: "ledger has no comparable claim (台账没有) ",
              reversal: "contradicts an existing conclusion (与已有结论相反)",
              reinforce: "new evidence strengthening an existing claim (强化)",
              weaken: "new evidence weakening an existing claim (削弱)",
              none: "same as an existing claim, no new information (实质相同无新信息)",
              na: "not a judgment / cannot tell (无法判断或不是观点)",
            },
          };
          questions[`${p}_target`] = {
            type: "choice",
            instructions: `Item ${p}: which active_opinions entry does it relate to? Pick its o-id, or none.`,
            criteria: targetCriteria,
          };
        }
        questions[`${p}_verify`] = boolQ(
          `Item ${p}: does its key claim need external verification (specific numbers or testable assertions)? (关键断言是否需要外部核验)`,
        );
      });
      const res = await classifyWithRetry(ctx, resolved.model, { state, questions }, deadline);
      if (!res) {
        failures += 1;
        return;
      }
      batch.forEach((it, k) => {
        const entry = info.get(it.id);
        if (!entry) return;
        const p = `i${k}`;
        const a = res.answers ?? {};
        entry.relation = a[`${p}_relation`]?.choice;
        entry.relation_confidence = a[`${p}_relation`]?.confidence;
        const target = a[`${p}_target`]?.choice;
        if (typeof target === "string" && target.startsWith("o")) {
          entry.flags = [...(entry.flags ?? []), `related_to_${target}`];
        }
        entry.verify_needed = (a[`${p}_verify`]?.probability ?? 0) > 0.5;
      });
    });
  }

  // ---- pass 3: fact repeat vs previous facts ----
  const factItems = items.filter((it) => ["fact", "mixed"].includes(info.get(it.id)?.kind ?? ""));
  const prevFacts = (ledger.prev_facts ?? []).slice(0, 60);
  if (factItems.length && prevFacts.length) {
    const repeatCriteria: Record<string, string> = { none: "no repeat" };
    prevFacts.forEach((f) => {
      repeatCriteria[`f${f.id}`] = String(f.text ?? "").slice(0, 60);
    });
    await mapLimit(chunk(factItems, Math.min(batchSize, 8)), concurrency, async (batch) => {
      if (timedOut()) return;
      batches += 1;
      const state: any = {
        prev_facts: prevFacts.map((f) => ({ id: `f${f.id}`, text: f.text })),
        items: {},
      };
      const questions: any = {};
      batch.forEach((it, k) => {
        const p = `i${k}`;
        state.items[p] = { title: it.title ?? "", summary: (it.summary ?? "").slice(0, 400) };
        questions[`${p}_repeat`] = boolQ(
          `Item ${p}: is it the same event as one of prev_facts, reporting it again? (是否与上期事实是同一事件)`,
        );
        questions[`${p}_repeat_of`] = {
          type: "choice",
          instructions: `Item ${p}: which prev_facts entry is the same event? Pick its f-id, or none.`,
          criteria: repeatCriteria,
        };
        questions[`${p}_new_number`] = boolQ(
          `Item ${p}: does it bring a new number or a major new development? (是否带来新数字或重大新进展)`,
        );
      });
      const res = await classifyWithRetry(ctx, resolved.model, { state, questions }, deadline);
      if (!res) {
        failures += 1;
        return;
      }
      batch.forEach((it, k) => {
        const entry = info.get(it.id);
        if (!entry) return;
        const p = `i${k}`;
        const a = res.answers ?? {};
        const repeat = (a[`${p}_repeat`]?.probability ?? 0) > 0.5;
        const newNumber = (a[`${p}_new_number`]?.probability ?? 0) > 0.5;
        if (repeat) {
          const of = a[`${p}_repeat_of`]?.choice;
          if (typeof of === "string" && of.startsWith("f")) {
            entry.repeat_of = Number(of.slice(1)) || undefined;
          }
          entry.flags = [...(entry.flags ?? []), newNumber ? "repeat_with_development" : "repeat_no_new_number"];
        }
        entry.new_number = newNumber;
      });
    });
  }

  // ---- confidence gating: never drop on uncertain judgments ----
  const annotations: TriageAnnotation[] = [];
  for (const it of items) {
    const entry = info.get(it.id);
    if (!entry) continue;
    if (entry.kind === "skip") {
      if ((entry.kind_confidence ?? 0) >= skipConf) entry.is_skipped = true;
      else {
        entry.kind = "fact";
        entry.flags = [...(entry.flags ?? []), "possible_skip"];
      }
    }
    if (entry.relation === "none" && (entry.relation_confidence ?? 0) < noneConf) {
      entry.relation = "new";
      entry.flags = [...(entry.flags ?? []), "relation_uncertain"];
    }
    if (entry.relation) entry.change_hint = RELATION_HINTS[entry.relation] ?? "";
    if (entry.kind === "na" || entry.kind === undefined) entry.kind = "fact";
    annotations.push(entry);
  }

  const mode: "jev" | "partial" | "heuristic" = failures === 0 ? "jev" : annotations.length ? "partial" : "heuristic";
  return {
    annotations,
    mode,
    model: resolved.spec,
    batches,
    failures,
    reason: failures ? `${failures} classifier batch(es) failed` : undefined,
  };
}

// ---- semantic lint (qa stage) ----

async function runJevLint(
  ctx: ClassifierCtx,
  draft: string,
  deadlineMs: number,
): Promise<{ violations: { line: number; kind: string; why: string }[]; used: boolean; reason?: string }> {
  const resolved = resolveJevModel(ctx);
  if (!resolved.model || !ctx.modelRegistry.hasConfiguredAuth(resolved.model)) {
    return { violations: [], used: false, reason: resolved.reason ?? "classifier unavailable" };
  }
  const concurrency = clampInt(process.env.RSS_JE_V_CONCURRENCY, 4, 1, 8);
  const deadline = Date.now() + Math.max(15_000, deadlineMs);
  const lines = draft.split("\n");
  let section: "none" | "facts" | "opinions" = "none";
  const targets: { idx: number; text: string; kind: "fact" | "claim" | "check" | "trend" }[] = [];
  lines.forEach((raw, idx) => {
    const text = raw.trim();
    if (/^#{1,6}\s*一、\s*资讯速览/.test(text)) {
      section = "facts";
      return;
    }
    if (/^#{1,6}\s*二、\s*观点与趋势/.test(text)) {
      section = "opinions";
      return;
    }
    if (!text) return;
    if (section === "facts" && /^[-*·]\s+/.test(text)) {
      targets.push({ idx, text: text.replace(/^[-*·]\s+/, ""), kind: "fact" });
    } else if (section === "opinions") {
      if (/^[-*·]?\s*观点：/.test(text)) targets.push({ idx, text, kind: "claim" });
      else if (/^[-*·]?\s*检验：/.test(text)) targets.push({ idx, text, kind: "check" });
      else if (/^[-*·]?\s*趋势：/.test(text)) targets.push({ idx, text, kind: "trend" });
    }
  });

  const violations: { line: number; kind: string; why: string }[] = [];
  const boolQ = (instructions: string) => ({
    type: "bool" as const,
    instructions,
    criteria: { true: "yes", false: "no" },
  });

  await mapLimit(chunk(targets, 10), concurrency, async (batch) => {
    if (Date.now() > deadline) return;
    const state: any = {};
    const questions: any = {};
    batch.forEach((m, k) => {
      const p = `l${k}`;
      state[p] = m.text.slice(0, 300);
      if (m.kind === "fact") {
        questions[`${p}_mixed`] = boolQ(
          `Line ${p} is in the facts section. Does it contain an opinion, prediction, evaluation, or an institutional judgment (认为/预计/建议/称将/研报显示/分析师表示)?`,
        );
        questions[`${p}_emotion`] = boolQ(
          `Does line ${p} contain emotional or biased wording (e.g. 惨烈/崩了/利好/值得警惕/显然/大概率)?`,
        );
        questions[`${p}_sports`] = boolQ(
          `Is line ${p} about sports or entertainment (match results, box office, celebrities, awards, TV shows)?`,
        );
      } else if (m.kind === "claim") {
        questions[`${p}_source`] = boolQ(
          `Does line ${p} state an opinion without naming a specific source (@account, 财新《栏目》, or similar)? Answer true when the source is missing or vague.`,
        );
        questions[`${p}_emotion`] = boolQ(`Does line ${p} contain emotional or biased wording?`);
      } else if (m.kind === "check") {
        questions[`${p}_nocounter`] = boolQ(
          `Line ${p} should include counter-evidence or an explicit 未证实 / not-verified status. Does it lack both?`,
        );
      } else if (m.kind === "trend") {
        questions[`${p}_nosignal`] = boolQ(
          `Line ${p} should contain a verification signal and a time window. Is either missing?`,
        );
      }
    });
    const res = await classifyWithRetry(ctx, resolved.model, { state, questions }, deadline);
    if (!res) return;
    batch.forEach((m, k) => {
      const p = `l${k}`;
      const a = res.answers ?? {};
      const hit = (key: string) => (a[`${p}_${key}`]?.probability ?? 0) > 0.6;
      const push = (why: string) => violations.push({ line: m.idx + 1, kind: "semantic", why });
      if (hit("mixed")) push("叙事夹带观点");
      if (hit("emotion")) push("情绪化措辞");
      if (hit("sports")) push("体育娱乐内容");
      if (hit("source")) push("观点缺具体署名");
      if (hit("nocounter")) push("检验行缺反例或未证实标注");
      if (hit("nosignal")) push("趋势行缺验证信号或时间窗");
    });
  });

  return { violations, used: true };
}

// ---- web verification (qa stage, hard budget) ----

function extractVerificationTargets(draft: string): { claim: string }[] {
  const lines = draft.split("\n");
  const out: { claim: string }[] = [];
  let claim = "";
  let check = "";
  let conclusion = "";
  const flush = () => {
    if (claim && /未证实|存疑|无法判断|待确认|未核实|未验证|insufficient|unverified/i.test(check + conclusion)) {
      out.push({ claim });
    }
    claim = "";
    check = "";
    conclusion = "";
  };
  for (const raw of lines) {
    const line = raw.trim().replace(/^[-*·]\s*/, "");
    if (line.startsWith("观点：")) {
      flush();
      claim = line.slice(3).trim();
    } else if (line.startsWith("检验：")) check = line.slice(3);
    else if (line.startsWith("结论：")) conclusion = line.slice(3);
    else if (line.startsWith("与上期相比：")) flush();
  }
  flush();
  return out.slice(0, 8);
}

async function runVerification(
  ctx: ClassifierCtx,
  draft: string,
): Promise<{
  entries: any[];
  used: number;
  budget: number;
  unavailable?: boolean;
}> {
  const budget = clampInt(process.env.RSS_VERIFY_BUDGET, 4, 0, 8);
  const entries: any[] = [];
  if (budget === 0) return { entries, used: 0, budget };
  const targets = extractVerificationTargets(draft);
  if (!targets.length) return { entries, used: 0, budget };

  const resolved = resolveJevModel(ctx);
  const canClassify = !!resolved.model && ctx.modelRegistry.hasConfiguredAuth(resolved.model);
  const deadline = Date.now() + clampInt(process.env.RSS_VERIFY_DEADLINE_MS, 90_000, 10_000, 300_000);

  for (const target of targets) {
    if (entries.length >= budget || Date.now() > deadline) break;
    let outcome: { result: { content?: any[]; structuredContent?: any }; isError: boolean };
    try {
      outcome = await ctx.executeTool("source_check", {
        claim: target.claim,
        fetchContent: true,
        numResults: 5,
      });
    } catch {
      return { entries, used: entries.length, budget, unavailable: true };
    }
    if (outcome?.isError) {
      return { entries, used: entries.length, budget, unavailable: true };
    }
    const evidence = toolResultToText(outcome.result);
    const entry: any = {
      claim: target.claim,
      verdict: "insufficient",
      counter: null,
      evidence_chars: evidence.length,
      evidence_excerpt: evidence.slice(0, 600),
    };
    if (canClassify) {
      const res = await classifyWithRetry(
        ctx,
        resolved.model,
        {
          state: { claim: target.claim, sources: evidence.slice(0, 6000) },
          questions: {
            verdict: {
              type: "choice",
              instructions:
                "Do these sources support or refute the claim? (这些来源支持还是反驳该断言)",
              criteria: {
                supports: "direct supporting evidence (有直接支持证据)",
                refutes: "direct counter-evidence (有直接反证)",
                mixed: "both support and counter-evidence (支持与反证并存)",
                insufficient: "not enough evidence to judge (证据不足)",
              },
            },
            counter: {
              type: "choice",
              instructions:
                "Which passage index contains counter-evidence? Pick none when there is none. (哪段是反证)",
              criteria: { s0: "passage 0", s1: "passage 1", s2: "passage 2", none: "no counter-evidence" },
            },
          },
        },
        deadline,
      );
      if (res) {
        entry.verdict = res.answers?.verdict?.choice ?? "insufficient";
        entry.verdict_confidence = res.answers?.verdict?.confidence;
        const counter = res.answers?.counter?.choice;
        if (typeof counter === "string" && counter !== "none") entry.counter = counter;
      }
    }
    entries.push(entry);
  }
  return { entries, used: entries.length, budget };
}

// ---- calibration (Chinese quality gate) ----

const CALIBRATION_LABELS = ["fact", "opinion", "mixed", "skip"];

function calibrationFile(): string {
  return process.env.RSS_CALIBRATION_FILE?.trim() || path.join(pluginDir(), "calibration.json");
}

function calibrationMetrics(rows: any[]): any {
  const classes = CALIBRATION_LABELS;
  const confusion: Record<string, Record<string, number>> = {};
  const perClass: Record<string, { precision: number | null; recall: number | null; support: number }> = {};
  const buckets: Record<string, { n: number; correct: number }> = {};
  let correct = 0;
  const labeled = rows.filter((r) => classes.includes(r.label) && classes.includes(r.jev_kind));
  for (const row of labeled) {
    confusion[row.label] ??= {};
    confusion[row.label][row.jev_kind] = (confusion[row.label][row.jev_kind] ?? 0) + 1;
    if (row.label === row.jev_kind) correct += 1;
    if (typeof row.jev_confidence === "number") {
      const bucket = Math.min(0.9, Math.max(0.5, Math.floor(row.jev_confidence * 10) / 10)).toFixed(1);
      buckets[bucket] ??= { n: 0, correct: 0 };
      buckets[bucket].n += 1;
      if (row.label === row.jev_kind) buckets[bucket].correct += 1;
    }
  }
  for (const c of classes) {
    const tp = labeled.filter((r) => r.label === c && r.jev_kind === c).length;
    const predicted = labeled.filter((r) => r.jev_kind === c).length;
    const actual = labeled.filter((r) => r.label === c).length;
    perClass[c] = {
      precision: predicted ? Number((tp / predicted).toFixed(3)) : null,
      recall: actual ? Number((tp / actual).toFixed(3)) : null,
      support: actual,
    };
  }
  const high = Object.entries(buckets)
    .filter(([b]) => Number(b) >= 0.8)
    .reduce((acc, [, v]) => ({ n: acc.n + v.n, correct: acc.correct + v.correct }), { n: 0, correct: 0 });
  const low = Object.entries(buckets)
    .filter(([b]) => Number(b) < 0.7)
    .reduce((acc, [, v]) => ({ n: acc.n + v.n, correct: acc.correct + v.correct }), { n: 0, correct: 0 });
  const highAccuracy = high.n ? high.correct / high.n : null;
  const lowAccuracy = low.n ? low.correct / low.n : null;
  const reasons: string[] = [];
  if (labeled.length < 30) reasons.push(`labeled sample too small (${labeled.length} < 30)`);
  if (high.n < 10) reasons.push(`high-confidence bucket too small (${high.n} < 10)`);
  if (highAccuracy !== null && highAccuracy < 0.9) reasons.push(`high-confidence accuracy ${highAccuracy.toFixed(2)} < 0.90`);
  if (highAccuracy !== null && lowAccuracy !== null && highAccuracy <= lowAccuracy) {
    reasons.push("confidence does not separate correct from wrong answers");
  }
  return {
    labeled: labeled.length,
    total: rows.length,
    accuracy: labeled.length ? Number((correct / labeled.length).toFixed(3)) : null,
    per_class: perClass,
    confusion,
    confidence_buckets: Object.fromEntries(
      Object.entries(buckets).map(([b, v]) => [b, { n: v.n, accuracy: Number((v.correct / v.n).toFixed(3)) }]),
    ),
    trustworthy_for_discard_decisions: reasons.length === 0,
    reasons,
  };
}

// ---------------------------------------------------------------------------
// Tool
// ---------------------------------------------------------------------------

const RssOutput = Type.Object({
  ok: Type.Boolean(),
  action: Type.String(),
  error: Type.Optional(Type.String()),
  data: Type.Optional(Type.Any()),
});

function okResult(action: string, text: string, data?: any) {
  return {
    content: [{ type: "text" as const, text }],
    details: undefined as unknown,
    structuredContent: { ok: true, action, ...(data !== undefined ? { data } : {}) },
  };
}

function errResult(action: string, text: string, data?: any) {
  return {
    content: [{ type: "text" as const, text }],
    details: undefined as unknown,
    structuredContent: { ok: false, action, error: text, ...(data !== undefined ? { data } : {}) },
    isError: true,
  };
}

function formatViolations(violations: any[]): string {
  return [...violations]
    .sort((a, b) => (a?.line ?? 0) - (b?.line ?? 0))
    .map((v) => `  line ${v?.line ?? "?"}: [${v?.kind ?? "?"}] ${v?.why ?? ""}`)
    .join("\n");
}

/** Web searches used by the last `qa` call, recorded by the following `commit`. */
let pendingVerifyUsed = 0;

const rssTool = defineTool({
  name: "rss",
  label: "RSS Report",
  description:
    "RSS report engine for the scheduled short-report workflow (facts + opinions digest). " +
    "MAIN PIPELINE — exactly these three calls, one per stage:\n" +
    "1) brief: fetch all feeds, select every unread item, drop sports/entertainment, dedupe, " +
    "run jev triage (fact/opinion/skip, topic, value, opinion change, repeats), reconcile the " +
    "previous-period ledger, and return a structured payload (facts pool, opinion pool, ledger, " +
    "last report). Call this FIRST and write the draft from its payload only; do not call " +
    "unread/search/recent.\n" +
    "2) qa: validate a draft: character budget, per-line limits, section separation, opinion " +
    "fields, tone words, links (deterministic) + jev line lint + web verification under a hard " +
    "budget (default 4). Pass the draft text; fix everything it reports.\n" +
    "3) commit: final gate. Validates, builds the header line, saves RSS_REPORT_DIR/MM-DD_hh.md " +
    "(Beijing time), updates the fact/opinion ledger, marks this batch as read, and returns the " +
    "final report text. Reply with exactly that text.\n" +
    "BASIC: fetch, unread, markread. ADMIN: manage op=add|remove|list|tag|tags|stats. " +
    "CALIBRATION: calibrate op=sample|score (checks whether jev handles Chinese well enough " +
    "before trusting its discard decisions).",
  parameters: Type.Object({
    action: Type.Union(
      [
        Type.Literal("brief", { description: "Stage 1: fetch + prepare the report payload" }),
        Type.Literal("qa", { description: "Stage 2: validate the draft; requires draft" }),
        Type.Literal("commit", { description: "Stage 3: save + ledger + mark read; requires body" }),
        Type.Literal("fetch", { description: "Fetch all subscriptions now" }),
        Type.Literal("unread", { description: "List unread items (human/debug)" }),
        Type.Literal("markread", { description: "Mark items read: ids, item_id, tag, older_than, before" }),
        Type.Literal("manage", { description: "Admin: op=add|remove|list|tag|tags|stats" }),
        Type.Literal("calibrate", { description: "jev Chinese calibration: op=sample|score" }),
      ],
      { description: "Pipeline stage or maintenance action" },
    ),
    fetch: Type.Optional(
      Type.Boolean({ description: "brief only: set false to skip fetching (default true)" }),
    ),
    max: Type.Optional(
      Type.Integer({ description: "brief: max items (default 250); calibrate sample: sample size" }),
    ),
    draft: Type.Optional(Type.String({ description: "qa only: the draft report text" })),
    body: Type.Optional(Type.String({ description: "commit only: the revised report body" })),
    strict: Type.Optional(
      Type.Boolean({ description: "commit only: reject on violations (default true)" }),
    ),
    op: Type.Optional(
      Type.Union(
        [
          Type.Literal("add"),
          Type.Literal("remove"),
          Type.Literal("list"),
          Type.Literal("tag"),
          Type.Literal("tags"),
          Type.Literal("stats"),
          Type.Literal("sample"),
          Type.Literal("score"),
        ],
        { description: "manage/calibrate sub-operation" },
      ),
    ),
    feed_url: Type.Optional(Type.String({ description: "manage add: feed URL" })),
    feed_id: Type.Optional(Type.Integer({ description: "manage remove/tag: feed ID" })),
    tags: Type.Optional(Type.String({ description: "manage add/tag: comma-separated tags" })),
    item_id: Type.Optional(Type.Integer({ description: "markread: single item ID" })),
    ids: Type.Optional(Type.String({ description: "markread: comma-separated item IDs" })),
    tag: Type.Optional(Type.String({ description: "unread/markread/manage list: tag filter" })),
    older_than: Type.Optional(Type.Integer({ description: "markread: hours" })),
    before: Type.Optional(Type.String({ description: "markread: unix ts or ISO date" })),
    limit: Type.Optional(Type.Integer({ description: "unread: max items (default 20)" })),
  }),
  outputSchema: RssOutput,

  async execute(_toolCallId, params, signal, _onUpdate, ctxRaw: any) {
    const ctx = ctxRaw as ClassifierCtx;
    const envError = await checkEnv();
    if (envError) return errResult(params.action, envError);

    switch (params.action) {
      // ---------------------------------------------------------------- Stage 1
      case "brief": {
        const args = ["brief", "--json"];
        if (params.max != null) args.push("--max", String(params.max));
        if (params.fetch === false) args.push("--no-fetch");
        const brief = await runPyJson(args, undefined, 420_000);
        if (!brief.ok) return errResult("brief", `❌ brief failed: ${brief.error ?? "unknown"}`);
        const runId = brief.data?.run_id;
        if (runId == null) return errResult("brief", "❌ brief returned no run id", brief.data);

        const data = brief.data as any;
        const deadlineMs = clampInt(process.env.RSS_BRIEF_DEADLINE_MS, 150_000, 10_000, 600_000);
        const triageTargets = (data?.items ?? []).filter((it: any) => !it.is_skipped);
        let triage: TriageResult;
        try {
          triage = await runJevTriage(ctx, triageTargets, data?.ledger ?? {}, deadlineMs);
        } catch (err: any) {
          triage = {
            annotations: [],
            mode: "heuristic",
            reason: `classifier error: ${err?.message ?? err}`,
            batches: 0,
            failures: 0,
          };
        }
        if (triage.annotations.length) {
          const ann = await runPyJson(["annotate", "--json"], JSON.stringify(triage.annotations));
          if (!ann.ok) console.error(`[rss] annotate failed: ${ann.error}`);
        }

        const payload = await runPyJson(["payload", "--json", "--run", String(runId)]);
        if (!payload.ok) return errResult("brief", `❌ payload failed: ${payload.error ?? "unknown"}`);
        const pd = payload.data as any;
        pd.triage = {
          mode: triage.mode,
          model: triage.model ?? null,
          classified: triage.annotations.length,
          batches: triage.batches,
          failures: triage.failures,
          ...(triage.reason ? { reason: triage.reason } : {}),
        };
        const text = [
          `✅ Brief ready (run ${runId}) — triage: ${triage.mode}${
            triage.model ? ` (${triage.model})` : ""
          }`,
          `counts: ${JSON.stringify(pd.counts ?? {})}`,
          "Write the draft from this payload only, then call rss {action:\"qa\", draft:\"...\"}.",
          "",
          JSON.stringify(pd, null, 1),
        ].join("\n");
        return okResult("brief", text, pd);
      }

      // ---------------------------------------------------------------- Stage 2
      case "qa": {
        if (!params.draft) return errResult("qa", "❌ action=qa requires draft");
        const check = await runPyJson(["check", "--json"], JSON.stringify({ body: params.draft }));
        if (!check.ok) return errResult("qa", `❌ qa failed: ${check.error ?? "unknown"}`);
        const data = check.data as any;
        const violations: any[] = [...(data?.violations ?? [])];
        const deadlineMs = clampInt(process.env.RSS_QA_DEADLINE_MS, 120_000, 10_000, 600_000);
        let lint: Awaited<ReturnType<typeof runJevLint>>;
        try {
          lint = await runJevLint(ctx, params.draft, deadlineMs);
        } catch (err: any) {
          lint = { violations: [], used: false, reason: `classifier error: ${err?.message ?? err}` };
        }
        violations.push(...lint.violations);
        let verification: Awaited<ReturnType<typeof runVerification>>;
        try {
          verification = await runVerification(ctx, params.draft);
        } catch (err: any) {
          verification = {
            entries: [],
            used: 0,
            budget: clampInt(process.env.RSS_VERIFY_BUDGET, 4, 0, 8),
            unavailable: true,
          };
        }
        pendingVerifyUsed = verification.used;

        data.violations = violations;
        data.verification = verification.entries;
        data.budget_used = verification.used;
        data.budget = verification.budget;
        data.jev_lint = lint.used ? "on" : "off";
        if (lint.reason) data.lint_reason = lint.reason;
        if (verification.unavailable) data.verification_unavailable = true;

        const parts: string[] = [];
        if (violations.length) {
          parts.push(`❌ ${violations.length} violation(s) — fix them and call commit:`);
          parts.push(formatViolations(violations));
        } else {
          parts.push(`✅ No violations (${JSON.stringify(data?.stats ?? {})}). Call rss {action:"commit", body:"..."}.`);
        }
        if (verification.entries.length) {
          parts.push(`\nverification ${verification.used}/${verification.budget}:`);
          for (const entry of verification.entries) {
            parts.push(
              `  - claim: ${String(entry.claim).slice(0, 60)}\n` +
                `    verdict: ${entry.verdict}${entry.counter ? ` | counter: ${entry.counter}` : ""}\n` +
                `    excerpt: ${String(entry.evidence_excerpt ?? "").slice(0, 200)}`,
            );
          }
        } else if (verification.unavailable) {
          parts.push("\nverification unavailable (source_check/web tools not loaded) — mark unverified facts as 未证实.");
        } else {
          parts.push(`\nverification: none needed (budget ${verification.budget}).`);
        }
        return okResult("qa", parts.join("\n"), data);
      }

      // ---------------------------------------------------------------- Stage 3
      case "commit": {
        if (!params.body) return errResult("commit", "❌ action=commit requires body");
        const res = await runPyJson(
          ["commit", "--json"],
          JSON.stringify({
            body: params.body,
            strict: params.strict !== false,
            websearch_used: pendingVerifyUsed,
          }),
        );
        const data = res.data as any;
        if (!res.ok || data?.ok === false) {
          const violations: any[] = data?.violations ?? [];
          const text = violations.length
            ? `❌ commit blocked — fix and retry:\n${formatViolations(violations)}`
            : `❌ commit failed: ${res.error ?? "unknown"}`;
          return errResult("commit", text, data);
        }
        pendingVerifyUsed = 0;
        return okResult("commit", data.reportText, {
          report_path: data.report_path,
          header: data.header,
          counts: data.counts,
          ledger: data.ledger,
        });
      }

      // ---------------------------------------------------------------- Calibration
      case "calibrate": {
        const op = params.op ?? "sample";
        const file = calibrationFile();
        if (op === "sample") {
          const n = params.max ?? 50;
          let sample = await runPyJson(["sample", "--json", "--n", String(n), "--unread"]);
          if (!sample.ok) return errResult("calibrate", `❌ sample failed: ${sample.error ?? "unknown"}`);
          let items = sample.data?.items ?? [];
          if (!items.length) {
            // After a commit everything is read; fall back to a random sample of the whole DB.
            sample = await runPyJson(["sample", "--json", "--n", String(n)]);
            if (!sample.ok) return errResult("calibrate", `❌ sample failed: ${sample.error ?? "unknown"}`);
            items = sample.data?.items ?? [];
          }
          let triage: TriageResult;
          try {
            triage = await runJevTriage(
              ctx,
              items,
              { prev_facts: [], unconfirmed: [], active_opinions: [] },
              clampInt(process.env.RSS_BRIEF_DEADLINE_MS, 150_000, 10_000, 600_000),
            );
          } catch (err: any) {
            return errResult("calibrate", `❌ classifier error: ${err?.message ?? err}`);
          }
          if (triage.mode === "heuristic") {
            return errResult(
              "calibrate",
              `❌ classifier unavailable: ${triage.reason ?? "unknown"}\n` +
                "Configure TYPESAFE_API_KEY (env, rss-plugin/auth.json, or TYPESAFE_KEY_FILE).",
            );
          }
          const byId = new Map(triage.annotations.map((a) => [a.id, a]));
          const rows = items.map((it: any) => {
            const a = byId.get(it.id);
            return {
              id: it.id,
              feed: it.feed,
              tags: it.tags,
              title: it.title,
              summary: it.summary,
              jev_kind: a?.kind ?? null,
              jev_confidence: a?.kind_confidence ?? null,
              jev_topic: a?.topic ?? null,
              label: "",
            };
          });
          writeFileSync(
            file,
            JSON.stringify({ createdAt: new Date().toISOString(), model: triage.model, rows }, null, 1),
            { mode: 0o600 },
          );
          const preview = rows
            .slice(0, 5)
            .map((r: any) => `  #${r.id} [${r.jev_kind} ${r.jev_confidence ?? "-"}] ${String(r.title).slice(0, 40)}`)
            .join("\n");
          return okResult(
            "calibrate",
            `✅ Wrote ${rows.length} sample(s) to ${file} (model ${triage.model}).\n` +
              `Fill the "label" field of every row with one of: ${CALIBRATION_LABELS.join(" / ")}.\n` +
              "Then call rss {action:\"calibrate\", op:\"score\"}.\n" +
              `Preview:\n${preview}`,
            { file, rows: rows.length, model: triage.model },
          );
        }
        if (op === "score") {
          if (!existsSync(file)) return errResult("calibrate", `❌ no calibration file at ${file}; run op="sample" first`);
          let parsed: any;
          try {
            parsed = JSON.parse(readFileSync(file, "utf-8"));
          } catch (err: any) {
            return errResult("calibrate", `❌ cannot parse ${file}: ${err?.message ?? err}`);
          }
          const rows: any[] = parsed?.rows ?? [];
          const unlabeled = rows.filter((r) => !CALIBRATION_LABELS.includes(r.label)).length;
          if (!rows.length || unlabeled === rows.length) {
            return errResult("calibrate", `❌ no labeled rows in ${file}; fill the "label" field first`);
          }
          const metrics = calibrationMetrics(rows);
          const lines = [
            `📊 Calibration: ${metrics.labeled}/${metrics.total} labeled (model ${parsed?.model ?? "?"})`,
            `accuracy: ${metrics.accuracy}`,
            ...CALIBRATION_LABELS.map(
              (c) =>
                `  ${c}: precision ${metrics.per_class[c]?.precision ?? "-"} recall ${
                  metrics.per_class[c]?.recall ?? "-"
                } (support ${metrics.per_class[c]?.support ?? 0})`,
            ),
            `confidence buckets: ${JSON.stringify(metrics.confidence_buckets)}`,
            metrics.trustworthy_for_discard_decisions
              ? "✅ trustworthy for discard decisions (skip / no-change)"
              : `⚠️ NOT trustworthy for discard decisions: ${metrics.reasons.join("; ")}`,
          ];
          return okResult("calibrate", lines.join("\n"), metrics);
        }
        return errResult("calibrate", `❌ unknown calibrate op: ${op}`);
      }

      // ---------------------------------------------------------------- Basic
      case "fetch": {
        const res = await runPyJson(["fetch", "--json"], undefined, 420_000);
        if (!res.ok) return errResult("fetch", `❌ fetch failed: ${res.error ?? "unknown"}`);
        const data = res.data as any;
        return okResult(
          "fetch",
          `✅ Fetched ${data?.total_feeds ?? 0} feed(s): ${data?.total_new_items ?? 0} new item(s), ` +
            `${data?.failed ?? 0} failed`,
          data,
        );
      }

      case "unread": {
        const args = ["unread"];
        if (params.limit != null) args.push("-l", String(params.limit));
        if (params.tag) args.push("-t", params.tag);
        if (params.feed_id != null) args.push("-f", String(params.feed_id));
        const { stdout } = await runPy(args);
        return okResult("unread", stdout.trim() || "(no output)");
      }

      case "markread": {
        const args = ["markread"];
        if (params.ids) args.push("--ids", params.ids);
        else if (params.item_id != null) args.push(String(params.item_id));
        else {
          if (params.tag) args.push("-t", params.tag);
          if (params.older_than != null) args.push("--older-than", String(params.older_than));
          if (params.before) args.push("--before", params.before);
        }
        const res = await runPyJson([...args, "--json"]);
        if (!res.ok) return errResult("markread", `❌ markread failed: ${res.error ?? "unknown"}`);
        const data = res.data as any;
        const text = typeof res.text === "string" && res.text.trim()
          ? res.text.trim()
          : `✅ Marked ${data?.marked ?? 0} item(s) as read`;
        return okResult("markread", text, data);
      }

      // ---------------------------------------------------------------- Admin
      case "manage": {
        const op = params.op;
        if (!op || !["add", "remove", "list", "tag", "tags", "stats"].includes(op)) {
          return errResult("manage", "❌ action=manage requires op=add|remove|list|tag|tags|stats");
        }
        let args: string[] = [];
        if (op === "add") {
          if (!params.feed_url) return errResult("manage", "❌ op=add requires feed_url");
          args = ["add", params.feed_url];
          if (params.tags) args.push("-t", params.tags);
        } else if (op === "remove") {
          if (params.feed_id == null) return errResult("manage", "❌ op=remove requires feed_id");
          args = ["remove", String(params.feed_id)];
        } else if (op === "tag") {
          if (params.feed_id == null) return errResult("manage", "❌ op=tag requires feed_id");
          args = ["tag", String(params.feed_id)];
          if (params.tags) args.push("-t", params.tags);
        } else if (op === "list") {
          args = ["list"];
          if (params.tag) args.push("-t", params.tag);
        } else if (op === "tags") {
          args = ["tags"];
        } else if (op === "stats") {
          args = ["stats"];
        }
        const { stdout, stderr } = await runPy(args);
        return okResult("manage", (stdout || stderr).trim() || "(no output)");
      }

      default:
        return errResult(String(params.action), `❌ Unknown action: ${params.action}`);
    }
  },
});

export default function (pi: ExtensionAPI) {
  pi.registerTool(rssTool);

  const { key, source } = resolveTypesafeKey();
  if (key && !process.env.TYPESAFE_API_KEY) {
    process.env.TYPESAFE_API_KEY = key;
    console.log(`[rss] TYPESAFE_API_KEY loaded from ${source}`);
  } else if (!key) {
    console.log("[rss] no TYPESAFE_API_KEY found (jev triage will be degraded)");
  }
}
