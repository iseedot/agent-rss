# 方案：用 pi + jev + codemode 重构 RSS 定时任务

> 目标：把「paseo cron 定时把一堆原始 RSS 文本塞给主模型」改造成
> **确定性抓取 → codemode 编排 → jev 便宜分类/排序 → 主模型只处理 Top-N** 的流水线。
> 主模型的 token 只花在真正重要的条目上，定时任务的状态可跨 run 持久化。

---

## 1. 现状与问题

### 1.1 现有链路

```
paseo schedule (cron) ──► 新建 pi agent (--mode rpc, cwd)
                              │
                              ├─ 插件 rss 工具（文本输出）
                              ├─ codemode（settings 已开 +codemode）
                              └─ 主模型（deepseek-v4.1-flash）

插件内部（可选）：setInterval(RSS_AUTO_FETCH_MINUTES)
  └─ runPy("fetch") → 正则 /New items: (\d+)/ → pi.sendUserMessage(...)
```

已经就绪的有利条件：
- `~/.pi/agent/settings.json` 里已有 `"defaultTools": ["+codemode"]`，且插件以 pi package 形式安装
  （`git:github.com/iseedot/agent-rss`），所以 **paseo 新建的 pi agent 天生同时具备 rss 工具和 codemode**。
- 已有 `jev-1.13(-free)`（opencode provider）可用，`models.classify()` 在 codemode 和
  extension 两条路径都能调用（`ctx.modelRegistry.classify()`）。
- paseo schedule 自带 run 历史、output 存档、pause/resume、max-runs、expiry、通知，
  不需要自己造调度和审计。

### 1.2 具体问题

| # | 问题 | 代码位置 | 影响 |
|---|---|---|---|
| 1 | 抓取与会话绑定：`setInterval` 只在 pi 进程存活时跑 | `index.ts` 尾部 | pi 不在 = 定时任务停摆；paseo agent 每次都新起，又变成"每次都全量处理" |
| 2 | 正则解析输出（`/New items: (\d+)/`） | `index.ts` `RSS_AUTO_FETCH_MINUTES` 分支 | 文案一改就坏；拿不到每个 feed 的详情 |
| 3 | 无筛选：全部 unread 原样进入主模型上下文 | `rss unread` → `_pretty_items` | 噪声/token 浪费；重要条目被淹没 |
| 4 | 无去重/聚类：同一新闻多源重复 | `rss_items.guid` 只做单 feed 内去重 | 摘要重复 |
| 5 | 无跨 run 状态：paseo 每次都新建 agent | — | 已分类/已推送无法记忆，只能靠 `is_read` 粗粒度表达 |
| 6 | 工具只返回文本，codemode 拿不到结构 | `index.ts` 未声明 `outputSchema` | 脚本只能字符串解析，脆弱 |
| 7 | 抓取串行（每 feed 30s 超时） | `fetch_all_feeds()` 顺序循环 | 20 个 feed 最坏可阻塞数分钟 |
| 8 | 无并发锁/退避/抖动 | `setInterval` | 多 session 同时 fetch 同一 DB，重复推送 |

---

## 2. 目标架构

```
┌─────────────────────────────────────────────────────────────┐
│ L0 触发层   paseo schedule (cron)  ← 唯一时钟源，带 run 审计 │
├─────────────────────────────────────────────────────────────┤
│ L1 采集层   rss.py fetch        ← ETag/304、并发、失败记录   │
├─────────────────────────────────────────────────────────────┤
│ L2 编排层   codemode 脚本       ← 去重/聚类/批量分类/聚合    │
│                                        └─ jev classify       │
├─────────────────────────────────────────────────────────────┤
│ L3 决策层   主模型（一个 turn） ← 只读 shortlist(≤12 条)     │
├─────────────────────────────────────────────────────────────┤
│ L4 状态层   SQLite (rss.db)     ← watermark / score / digest │
└─────────────────────────────────────────────────────────────┘
```

### 职责划分（这是本方案的核心）

| 角色 | 负责 | 不负责 |
|---|---|---|
| **Python (rss.py)** | 网络抓取、ETag 缓存、SQLite 存取、watermark、原子性、批量回写 | 语义判断 |
| **codemode** | 控制流、并行分类、确定性去重、聚合排序、**只把 shortlist 返回给主模型** | 长时间存储（用 SQLite 而非 `store()`） |
| **jev 分类器** | 逐条打分（0-10）、主题归类、是否值得打断；结果缓存进 DB | 写摘要、联网取正文 |
| **主模型** | 写 digest、执行动作、决定 markread | 读原始条目全量文本 |
| **paseo** | cron、并发控制、重试、run 日志、通知、暂停/过期 | 业务逻辑 |

> 关键收益：codemode 只有脚本的 `return` 会进入主模型上下文，
> 原始 N 条 item 完全不进主模型。这是 token 与噪声的最大削减点。

---

## 3. 插件改造清单（v0.5）

### 3.1 `rss.py`

1. **全局 `--json`**：所有子命令支持 `--json`，统一输出
   `{"ok": true, "action": "...", "data": {...}, "error": null}`。
   人类可读文本保持不变（不破坏现有调用）。
2. **新命令 `digest`**（跨 run 状态机）：
   ```
   rss.py digest --json [-l 120] [-t tag] [--max-age-hours 72] [--since <ts|last>] [--redigest]
   ```
   - 返回 `digest_at IS NULL` 的条目（含 feed 标题、tags、summary、link、age）；
   - 取出时在同一事务里写 `digest_at = now`，天然防重复 digest；
   - `annotate` 回来的 score 会一并带出（便于二次排序）。
3. **新命令 `annotate`**：`rss.py annotate --json`，stdin 收 JSON 数组
   `[{id, score, label, interrupt, dup_of}]`，批量写入（分批 500）。
4. **`markread` 支持 `--ids 1,2,3`**：替代逐条 item_id 调用。
5. **`fetch` 并发化 + 结构化 stats**：
   - `ThreadPoolExecutor(max_workers=RSS_FETCH_WORKERS, default 6)`；
   - 单 feed 超时 30s，整轮总预算可配（默认 300s）；
   - `--json` 返回 `{total, success, failed, newItems, feeds:[{id,url,new,status,error}]}`。
6. **并发锁**：`rss.py fetch` 用 `fcntl.flock`（或 `rss_fetch.lock`）防多 session 同时抓取。
7. **Schema 增量迁移**（`migrate_schema` 里用 `PRAGMA table_info` 逐列判断）：
   ```
   rss_items:     + digest_at INTEGER, + score INTEGER, + label TEXT,
                  + interrupt INTEGER DEFAULT 0, + dup_of INTEGER, + triaged_at INTEGER
   rss_state:     key TEXT PRIMARY KEY, value TEXT, updated_at INTEGER
                  （存 last_digest_at、last_push_fingerprint 等）
   ```
   全部加 `CREATE IF NOT EXISTS` / `ALTER TABLE ... ADD COLUMN`，向后兼容。
8. （可选，P3）`cluster` 只在 codemode 里做，Python 不引入相似度依赖。

### 3.2 `index.ts`

1. **`rss` 工具声明 `outputSchema` + 返回 `structuredContent`**：
   ```ts
   const RssResult = Type.Object({
     ok: Type.Boolean(),
     action: Type.String(),
     data: Type.Unknown(),      // 各 action 的载荷（与 rss.py --json 对齐）
     error: Type.Optional(Type.String()),
   });
   // defineTool({ ..., outputSchema: RssResult, execute: ... })
   // execute 返回 { content:[{type:"text",text}], structuredContent: parsed }
   ```
   这样 codemode 里 `const r = await tools.rss({...}); r.data.items` 直接用，不再解析文本。
2. **新增 action 映射**：`digest` / `annotate`（参数带 `--json`）。
3. **重写定时逻辑**，新增三个模式（环境变量）：
   | 变量 | 行为 |
   |---|---|
   | `RSS_AUTO_FETCH_MINUTES` | **只抓取**，不注入消息（或注入 `sendMessage(..., {triggerTurn:false})` 一行摘要） |
   | `RSS_PUSH_MIN_SCORE` | 扩展内用 `ctx.modelRegistry.classify()` 拿 jev 预筛；仅在 `score≥阈值` 时 `pi.sendMessage({customType:"rss-alert",...},{triggerTurn:true})` 真正唤醒主模型 |
   | `RSS_AUTO_FETCH_JITTER` | ±抖动 + 指数退避，失败不叠加 |
   > 注意：现有代码 `pi.sendUserMessage(text, { triggerTurn: false })` 的选项在当前
   > ExtensionAPI 上是非法的（`sendUserMessage` 恒触发 turn，`triggerTurn` 只属于 `sendMessage`）。
   > 必须改掉，否则每个时段都会无谓唤醒一次主模型。
4. **并发保护**：进程内 `inflight` 标志 + 失败退避；跨进程交给 rss.py 的 flock。
5. **session 钩子**（可选）：`session_start` 读 `rss_state.last_digest_at`，避免新 agent 重复处理。

### 3.3 打包 skill / prompt 模板（让 paseo prompt 缩到一行）

`package.json` 的 `pi` 字段扩展：

```json
"pi": {
  "extensions": ["./index.ts"],
  "skills": ["./skills"],
  "prompts": ["./prompts"]
}
```

- `skills/rss-digest/SKILL.md`：固化整套流程（步骤、阈值、输出格式、markread 策略、
  "禁止直接 `rss unread`，必须走 digest"），并把 §4 的脚本作为 `references/digest.js` 附带；
- `prompts/rss-digest.md`：`/rss-digest [tag] [topN]` 一行触发；
- 这样 paseo schedule 的 prompt 只需要：
  `Run /skill:rss-digest and output the digest.`（或直接 `/rss-digest`）

---

## 4. 单次运行流程（新）

```
paseo cron 触发
  └─ 新建 pi agent（便宜模型即可）→ 加载 rss-digest skill
       │
       ├─ ① rss {action:"fetch"}                     确定性、幂等、带 304 缓存
       │
       ├─ ② codemode 一次调用：
       │     tools.rss({action:"digest", limit:120})   取回未 digest 的条目
       │     确定性去重（canonical URL / 标题指纹）
       │     jev 逐条 classify（4 并发）：score / topic / interrupt
       │     tools.rss({action:"annotate", items})     分数写回 SQLite（跨 run 缓存）
       │     return 只返回 shortlist + 统计
       │
       ├─ ③ 主模型读 shortlist（≤12 条）→ 写 digest（Markdown/通知/待办）
       │
       └─ ④ rss {action:"markread", ids:[...]}         消费掉的标记
          paseo 自动记录 run output / 时长 / 失败
```

---

## 5. 核心 codemode 脚本（可直接放进 skill）

```js
// ① 抓取
const fetched = await tools.rss({ action: "fetch" });

// ② 取本轮待处理条目（digest_at 在 DB 侧原子写入，重复 run 不会重复处理）
const { data: { items } } = await tools.rss({
  action: "digest", limit: 120, tag: null, maxAgeHours: 72,
});

// ③ 确定性去重（零成本，先砍掉跨源重复）
const canon = (u) => String(u || "")
  .replace(/[#?].*$/, "").replace(/^https?:\/\/(www\.)?/, "").toLowerCase();
const seen = new Map(), unique = [];
for (const it of items) {
  const key = canon(it.link) || `${it.feedId}:${it.title}`;
  if (seen.has(key)) continue;
  seen.set(key, it.id);
  unique.push(it);
}

// ④ jev 分类：score + topic + interrupt，4 并发（codemode 每脚本上限 4）
const jev = await models.getModelOfType("classifier", "opencode", "jev-1.13-free");
const classifyOne = async (it) => {
  const r = await models.classify(jev, {
    state: {
      feed: it.feedTitle, tags: it.tags, title: it.title,
      summary: (it.summary || it.content || "").slice(0, 500),
      ageHours: it.ageHours,
    },
    questions: {
      importance: {
        type: "score",
        instructions: "对关注 AI/软件工程/基础设施/安全/创业的人有多重要？",
        criteria: ["0=广告/离题噪声", "5=有用但可等", "10=今天必须知道，可行动或重大"],
      },
      topic: {
        type: "choice",
        instructions: "选唯一最合适的主题。",
        criteria: { models: "模型发布", infra: "系统/DB/性能", security: "漏洞/事故",
                    product: "工具产品", funding: "融资并购", other: "其他" },
      },
      interrupt: {
        type: "bool",
        instructions: "值得现在打断用户吗？",
        criteria: { true: "重大变化或今天可行动", false: "信息性内容" },
      },
    },
  });
  return { id: it.id, a: r.answers, stopReason: r.stopReason };
};

const triaged = [];
for (let i = 0; i < unique.length; i += 4) {
  triaged.push(...await Promise.all(unique.slice(i, i + 4).map(classifyOne)));
}

// ⑤ 回写，下次 run 直接命中缓存（不会重复花分类器额度）
await tools.rss({
  action: "annotate",
  items: triaged.map(t => ({
    id: t.id,
    score: t.a.importance?.score ?? 0,
    label: t.a.topic?.choice ?? "other",
    interrupt: (t.a.interrupt?.probability ?? 0) > 0.6,
  })),
});

// ⑥ 只把 shortlist 返回给主模型（其余全部留在脚本里）
const byId = new Map(unique.map(i => [i.id, i]));
const shortlist = triaged
  .filter(t => (t.a.importance?.score ?? 0) >= 6 || (t.a.interrupt?.probability ?? 0) > 0.6)
  .sort((a, b) => (b.a.importance?.score ?? 0) - (a.a.importance?.score ?? 0))
  .slice(0, 12)
  .map(t => ({
    id: t.id, score: t.a.importance?.score, topic: t.a.topic?.choice,
    interrupt: (t.a.interrupt?.probability ?? 0) > 0.6,
    title: byId.get(t.id)?.title, link: byId.get(t.id)?.link, feed: byId.get(t.id)?.feedTitle,
  }));

return {
  fetched: fetched.data?.newItems ?? null,
  scanned: items.length, deduped: items.length - unique.length,
  shortlist, dropped: triaged.length - shortlist.length,
};
```

主模型拿到的只有最后那个对象（十几条），然后写 digest、对 `interrupt=true` 的条目做事、
最后 `rss markread --ids ...`。

---

## 6. 调度方案选型

### 方案 A（推荐）：paseo cron 作唯一时钟源

```bash
paseo schedule create \
  --cron "0 */2 * * *" --timezone Asia/Shanghai \
  --name rss-digest \
  --provider pi/opencode-go/deepseek-v4.1-flash \
  --cwd /home/ericfu \
  --max-runs 720 --expires-in 60d \
  --run-now \
  "Run /skill:rss-digest. Fetch, triage with codemode+jev, and output a digest of at most 12 items. Mark consumed items as read."
```

配套：把插件环境变量 `RSS_AUTO_FETCH_MINUTES` 设为 `0`（避免双时钟）。

优点：run 历史/失败可见、可暂停、可 `run-once`、可 `schedule logs` 审计、
新 agent 天然干净、prompt 可版本化。

### 方案 B（离线兜底）：插件内条件唤醒

无 paseo / pi 常驻时使用：扩展 `setInterval` → fetch → `ctx.modelRegistry.classify()`
逐条 jev 预筛 → 只有高分才 `pi.sendMessage(..., {triggerTurn: true})`。
好处是**平时零主模型调用**；坏处是依赖 pi 进程存活、无 run 审计。

### 方案 C（积压消化）：paseo heartbeat

首次接入或积压很多时，用 heartbeat 每 10 分钟消化一批（每次 50 条），
`rss stats` 显示清空后 `delete_heartbeat`。适合有界循环，别把它当常驻 cron。

> 建议：A 为主；B 作为 A 不可用时的降级；C 只在一次性清积压时开。
> 三者共用同一套 digest/annotate 状态，不会互相污染。

---

## 7. 跨 run 状态设计（关键）

paseo 每次 run 都是新 agent，所以**状态必须落在 SQLite，而不是 codemode `store()`**：

| 字段 | 作用 |
|---|---|
| `rss_items.digest_at` | 本条已被某次 digest 取走；防止失败重跑重复处理 |
| `rss_items.score/label/interrupt/triaged_at` | jev 结果缓存；重跑/多调度共享，不重复调分类器 |
| `rss_items.dup_of` | 跨源重复指向主条目 |
| `rss_state.last_digest_at` | 全局 watermark（`stats`/skill 展示用） |
| `rss_state.last_push_fingerprint` | 扩展推送去重（避免同一批反复唤醒） |

---

## 8. 收益与成本

以一轮 **100 条新条目** 估算：

| 项 | 现状 | 新方案 |
|---|---|---|
| 进主模型的内容 | ~100 条 × ~200 token ≈ 20k+ token，且噪声大 | shortlist ≤12 条，约 1.5–2k token |
| 主模型 turns/run | 至少 1 次"看到 N 条新消息"再自行 `rss unread`（可能多轮） | 1 次，输入即结论 |
| 分类成本 | 0（全部靠主模型硬扛） | jev 100 次调用；`jev-1.13-free` 免费，付费模型也远低于主模型 |
| 延迟 | 抓取串行（最坏数分钟）+ 主模型读全量 | 抓取并发（~10–30s）+ 分类 25 批 × 4 并发（~10–25s） |
| 跨 run | 无记忆 | score/digest 缓存，重跑近零成本 |

> 若不想逐条调用分类器，可先用标签/关键词硬过滤 + 只对 `important` feed 分类，
> 或把每条 3 问合并成"只问 interrupt"（1 问/条）进一步降本。

---

## 9. 分阶段落地

| 阶段 | 内容 | 预估 | 验收 |
|---|---|---|---|
| P0 | rss.py `--json` + `digest` + `annotate` + `markread --ids` + 并发 fetch + flock；index.ts `outputSchema`/`structuredContent`；去掉正则与非法 `sendUserMessage` 选项 | 0.5–1 天 | `python3 rss.py digest --json` 返回结构化；`pi --print "/rss-digest"` 能跑通 |
| P1 | skill + prompt 模板 + `scripts/paseo-schedule.sh`；paseo `--run-now` 验证一次 | 0.5 天 | `paseo schedule logs` 里能看到 shortlist digest |
| P2 | 扩展内 jev 条件唤醒（方案 B）、抖动/退避/跨进程锁、`session_start` watermark | 1 天 | 无高分时不触发主模型 turn |
| P3 | 跨源聚类、正文抓取（web 工具）补充摘要、按 tag 分频（tech 每 1h / 其他每 6h）、周报 rollup、积压 heartbeat | 1–2 天 | 重复条目合并率、周报可读 |

---

## 10. 风险与对策

| 风险 | 对策 |
|---|---|
| jev 调用失败/限流 | `classify` 不抛异常；读 `stopReason==="error"` → 该条按 "未评分" 放行并标 `score=null`，不阻塞整轮 |
| 阈值漂移、漏掉重要条目 | 阈值写进 skill 参数；保留 `rss unread` 人工兜底；每周抽样复核 shortlist |
| 重复推送 | `digest_at` + `last_push_fingerprint` + flock 三重保护 |
| 隐私（正文送第三方分类器） | 可切 llama.cpp 本地分类模型；或在 skill 里配置"只送标题+摘要" |
| paseo 新 agent 权限/工具差异 | 插件走全局 pi package 安装即可全 session 生效；P1 用 `--run-now` 先验证工具在 schedule agent 里可用 |
| 僵尸 cron | `--max-runs` + `--expires-in`，失败会体现在 `schedule logs` |

---

## 11. 验收指标（建议写进 `rss stats --json`）

- 每轮：`feeds.new / 304 / error`、`itemsFetched`、`dedupRatio`、`triaged`、
  `shortlistSize`、`runDurationMs`、`mainModelTokens`（session 统计）、`backlog`（unread 总数）。
- 观测点：`paseo schedule inspect/logs <id>`（run 状态与输出）+ `rss stats`（订阅健康）。

---

## 附：方案一句话总结

**paseo 负责"准时"，rss.py 负责"抓得准、记得住"，codemode 负责"编排与并行"，
jev 负责"便宜地判断值不值得看"，主模型只负责"把值得看的写成结论"。**
