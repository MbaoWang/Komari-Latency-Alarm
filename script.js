// Komari 延遲波動告警插件
//
// 每 30 秒讀取受監控的延遲監測任務紀錄，對每一組「任務 × 節點」偵測：
//   1. 突發延遲波動：對比短期基準（前 baseline_minutes 分鐘中位數，排除已恢復異常區間）
//   2. 緩慢爬升波動：對比長期基準（前 long_baseline_hours 小時中位數，每 30 分鐘背景更新）
//   3. 持續高丟包（疑似被牆／阻斷）：節點仍在線回報，但連續 loss_minutes 分鐘丟包率達門檻

const server = require("server");
const fs = require("fs");
const path = require("path");

const CHECK_SPEC = "@every 30s";
const MIN_BASELINE_SAMPLES = 5;
const RECOVERY_HYSTERESIS = 0.7; // 恢復時須回到門檻的 70% 以內，避免在臨界值反覆抖動
const LONG_BASELINE_REFRESH_MS = 30 * 60000; // 每 30 分鐘更新一次長期基準，避免對資料庫造成負擔
const FETCH_TIMEOUT_MS = 10000;
const FETCH_CONCURRENCY = 4;
const BATCH_NOTIFY_THRESHOLD = 4; // 單輪同類型通知達 4 筆以上改發彙整摘要，避免觸發 Telegram 429 限流
const BATCH_CHUNK_SIZE = 15; // 每則彙整摘要最多列 15 筆，確保遠低於 Telegram 4096 字元上限
const STATE_FILE = path.join(__storageDir__, "state.json");
const STATE_TMP = STATE_FILE + ".tmp";

// alerting:     "<taskId>:<clientUuid>" -> { base, after, dir, since, start, source }
// lossAlerting: "<taskId>:<clientUuid>" -> { since, start, lossPct }
// excluded:     "<taskId>:<clientUuid>" -> [{ start, end }, ...]（已恢復的異常區間，避免污染基準視窗）
// cooldowns:    "<taskId>:<clientUuid>" -> 毫秒時間戳（延遲波動冷卻結束時間）
// lossCooldowns:"<taskId>:<clientUuid>" -> 毫秒時間戳（高丟包冷卻結束時間）
// longBaseline: "<taskId>:<clientUuid>" -> { value, count, updatedAt }
// rebasedAt:    "<taskId>:<clientUuid>" -> 毫秒時間戳（上次重定基準時間，防止長期基準被舊資料拉回）
let alerting = {};
let lossAlerting = {};
let excluded = {};
let cooldowns = {};
let lossCooldowns = {};
let longBaseline = {};
let rebasedAt = {};
let longRefreshedAt = 0;
let longBaselineHoursCached = 0;
let longTaskRefreshed = {};

let running = false;
let lastResult = null;
let checks = 0;

function isPlainObject(v) {
  return Boolean(v) && typeof v === "object" && !Array.isArray(v);
}

function loadState() {
  alerting = {};
  lossAlerting = {};
  excluded = {};
  cooldowns = {};
  lossCooldowns = {};
  longBaseline = {};
  rebasedAt = {};
  longRefreshedAt = 0;
  longBaselineHoursCached = 0;
  longTaskRefreshed = {};
  try {
    const parsed = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
    if (!isPlainObject(parsed)) return;
    if (parsed.version === 2) {
      if (isPlainObject(parsed.alerting)) alerting = parsed.alerting;
      if (isPlainObject(parsed.lossAlerting)) lossAlerting = parsed.lossAlerting;
      if (isPlainObject(parsed.excluded)) excluded = parsed.excluded;
      if (isPlainObject(parsed.cooldowns)) cooldowns = parsed.cooldowns;
      if (isPlainObject(parsed.lossCooldowns)) lossCooldowns = parsed.lossCooldowns;
      if (isPlainObject(parsed.longBaseline)) longBaseline = parsed.longBaseline;
      if (isPlainObject(parsed.rebasedAt)) rebasedAt = parsed.rebasedAt;
      if (Number.isFinite(parsed.longRefreshedAt)) longRefreshedAt = parsed.longRefreshedAt;
      if (Number.isFinite(parsed.longBaselineHoursCached)) longBaselineHoursCached = parsed.longBaselineHoursCached;
    } else {
      // 相容 v1.3.0 舊版狀態檔格式
      alerting = parsed;
    }
  } catch (e) {}
}

function saveState() {
  try {
    const payload = JSON.stringify({
      version: 2,
      alerting: alerting,
      lossAlerting: lossAlerting,
      excluded: excluded,
      cooldowns: cooldowns,
      lossCooldowns: lossCooldowns,
      longBaseline: longBaseline,
      rebasedAt: rebasedAt,
      longRefreshedAt: longRefreshedAt,
      longBaselineHoursCached: longBaselineHoursCached,
    });
    fs.writeFileSync(STATE_TMP, payload);
    fs.renameSync(STATE_TMP, STATE_FILE);
  } catch (e) {
    console.error("[latency-alert] 無法寫入狀態檔：" + errText(e));
  }
}

function load() {
  loadState();
  server.cron(CHECK_SPEC, tick);
  // 插件 RPC 是同步回傳的（不會等 Promise），所以這裡只負責觸發，結果用 status 查。
  //   latencyAlert:check  立即檢查一次
  //   latencyAlert:test   發送測試訊息
  //   latencyAlert:status 上次檢查結果與目前告警中的序列
  server.registerRPC("latencyAlert:check", () => {
    if (running) return { started: false, reason: "already running" };
    tick();
    return { started: true };
  });
  server.registerRPC("latencyAlert:test", () => {
    announce(true);
    return { started: true };
  });
  server.registerRPC("latencyAlert:status", () => ({
    running: running,
    checks: checks,
    last: lastResult,
    alertingCount: Object.keys(alerting).length,
    lossAlertingCount: Object.keys(lossAlerting).length,
    longBaselineCount: Object.keys(longBaseline).length,
    cooldownCount: Object.keys(cooldowns).length,
    alerting: alerting,
    lossAlerting: lossAlerting,
  }));
  announce(false);
}

async function announce(force) {
  try {
    const cfg = readSettings(await server.getConfig());
    if (!force && !cfg.testOnLoad) return;
    if (cfg.partialTelegram) {
      console.warn("[latency-alert] 只填了 Bot Token 或 Chat ID 其中一項，已改用 Komari 內建通知。");
    }
    const lines = [
      "延遲門檻：" + cfg.pct + "% 且 " + cfg.ms + " ms，連續 " + cfg.confirm + " 筆",
      "基準視窗：短期 " + cfg.baselineMinutes + " 分鐘" +
        (cfg.longBaselineHours > 0 ? " ／ 長期 " + cfg.longBaselineHours + " 小時（中位數）" : "（中位數）"),
      "高丟包偵測：" + (cfg.lossMinutes > 0 ? "連續 " + cfg.lossMinutes + " 分鐘且丟包 ≥ " + cfg.lossPercent + "%" : "關閉"),
      "冷卻時間：" + (cfg.cooldownMinutes > 0 ? cfg.cooldownMinutes + " 分鐘" : "關閉") +
        "｜重定基準通知：" + (cfg.notifyRebase ? "開啟" : "關閉"),
      "推播方式：" + (cfg.builtin ? "Komari 內建通知" : "插件自帶的 Telegram 機器人"),
    ];
    await notify(cfg, {
      emoji: "✅",
      title: "Komari 延遲波動告警已啟動",
      body: lines.join("\n"),
    });
  } catch (e) {
    console.error("[latency-alert] 測試訊息發送失敗：" + errText(e));
  }
}

async function tick() {
  if (running) return; // 上一輪還沒跑完
  running = true;
  try {
    lastResult = await check();
  } catch (e) {
    lastResult = { error: errText(e) };
    console.error("[latency-alert] 檢查失敗：" + lastResult.error);
  } finally {
    if (!isPlainObject(lastResult)) lastResult = {};
    lastResult.at = fmtTime(Date.now());
    checks++;
    running = false;
  }
}

// ---------------------------------------------------------------- 設定

function num(value, fallback, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < min) return fallback;
  return max !== undefined && n > max ? max : n;
}

function readSettings(raw) {
  raw = raw || {};
  const token = String(raw.bot_token || "").trim();
  const chatId = String(raw.chat_id || "").trim();
  const rawThread = Number(String(raw.thread_id || "").trim());
  const threadId = Number.isInteger(rawThread) && rawThread > 0 ? rawThread : 0;
  const apiBase = (String(raw.api_base || "").trim() || "https://api.telegram.org").replace(/\/+$/, "");
  const baselineMinutes = num(raw.baseline_minutes, 30, 1);
  return {
    token: token,
    chatId: chatId,
    threadId: threadId,
    apiBase: apiBase,
    testOnLoad: raw.test_on_load !== false,
    // 沒填完整 Bot Token + Chat ID 時，改走 Komari 後台已設定好的通知管道
    builtin: !token || !chatId,
    partialTelegram: Boolean(token) !== Boolean(chatId),
    pct: num(raw.threshold_percent, 50, 0),
    ms: num(raw.threshold_ms, 30, 0),
    confirm: Math.floor(num(raw.confirm_samples, 3, 1)),
    baselineMinutes: baselineMinutes,
    longBaselineHours: Math.floor(num(raw.long_baseline_hours, 24, 0, 168)),
    lossMinutes: num(raw.loss_minutes, 15, 0),
    lossPercent: num(raw.loss_percent, 80, 1, 100),
    upOnly: raw.direction === "up",
    notifyRecovery: raw.notify_recovery !== false,
    // 基準視窗還沒被新延遲填滿前就重定基準，會拿到新舊混雜的基準
    rebaseMinutes: Math.max(num(raw.rebase_minutes, 60, 1), baselineMinutes),
    notifyRebase: raw.notify_rebase !== false,
    cooldownMinutes: num(raw.cooldown_minutes, 15, 0),
    groups: readGroups(raw),
  };
}

// 監控範圍由 V4、V6 兩個組合構成，每個組合 = 選定的節點 × 選定的任務，兩組取聯集。
// 有選節點但沒選任務時，自動套用名稱含 V4（或 V6）的監測任務。
// 節點留空但有選任務 = 所有節點；兩邊都留空的組合視為未使用。兩組都未使用 = 監控全部。
const GROUPS = [
  { label: "V4", suffix: "", auto: /v4/i },
  { label: "V6", suffix: "2", auto: /v6/i },
];

function readGroups(raw) {
  const groups = [];
  for (const def of GROUPS) {
    const nodes = Array.isArray(raw["nodes" + def.suffix])
      ? raw["nodes" + def.suffix].map(String).filter(Boolean)
      : [];
    const tasks = Array.isArray(raw["tasks" + def.suffix])
      ? raw["tasks" + def.suffix].map(Number).filter(Number.isFinite)
      : [];
    if (nodes.length || tasks.length) {
      groups.push({ label: def.label, nodes: nodes, tasks: tasks, auto: tasks.length ? null : def.auto });
    }
  }
  return groups;
}

// 把「沒選任務」的組合展開成名稱符合的任務 Set，供 O(1) 查詢
function resolveGroups(groups, tasks) {
  return groups.map((g) => {
    const nodeSet = new Set(g.nodes);
    if (!g.auto) return { label: g.label, nodes: nodeSet, tasks: new Set(g.tasks), auto: null };
    const matched = tasks.filter((t) => g.auto.test(String(t.name || ""))).map((t) => Number(t.id));
    return { label: g.label, nodes: nodeSet, tasks: new Set(matched), auto: g.auto };
  });
}

function taskWatched(groups, taskId) {
  return !groups.length || groups.some((g) => g.tasks.has(taskId));
}

function pairWatched(groups, taskId, uuid) {
  return !groups.length || groups.some((g) =>
    g.tasks.has(taskId) && (!g.nodes.size || g.nodes.has(uuid)));
}

// ---------------------------------------------------------------- 判定

function mean(values) {
  let sum = 0;
  for (const v of values) sum += v;
  return sum / values.length;
}

function median(values) {
  const sorted = values.slice().sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function limitOf(base, cfg) {
  return Math.max(cfg.ms, (base * cfg.pct) / 100);
}

// 依時間排序並去除重複時間戳（保留最後一筆）
function normalizeRecords(rawList) {
  rawList.sort((a, b) => a.t - b.t);
  const deduped = [];
  for (const r of rawList) {
    if (deduped.length && deduped[deduped.length - 1].t === r.t) {
      deduped[deduped.length - 1] = r;
    } else {
      deduped.push(r);
    }
  }
  return deduped;
}

function inExcluded(ts, ranges) {
  if (!ranges || !ranges.length) return false;
  for (const ep of ranges) {
    if (ts >= ep.start && ts < ep.end) return true;
  }
  return false;
}

function matchDirection(valid, base, cfg) {
  const limit = limitOf(base, cfg);
  if (limit <= 0) return "";
  if (valid.every((v) => v - base >= limit)) return "up";
  if (!cfg.upOnly && valid.every((v) => base - v >= limit)) return "down";
  return "";
}

// 延遲波動判定（短期突發 + 長期緩慢爬升 + 遲滯恢復 + 雙向反轉 + 重定基準）
// records：同一任務同一節點的紀錄，已依時間嚴格遞增，{ t: 毫秒時間戳, v: 延遲 ms（<0 為丟包）}
// 回傳 null（不動作）或 { type: "alert" | "recover" | "rebase", ... }
function evaluate(records, intervalSec, cfg, prev, excludedRanges, longBaseInfo, cooldownUntil, now) {
  const n = cfg.confirm;
  if (records.length < n) return null;
  const recent = records.slice(-n);

  // 節點離線或任務停用時紀錄會中斷，不拿過期或跨斷線的資料判斷
  const staleMs = Math.max(intervalSec * 3000, 5 * 60000);
  if (now - recent[n - 1].t > staleMs) return null;
  const maxSpanMs = Math.max((n - 1) * intervalSec * 3000, staleMs);
  if (recent[n - 1].t - recent[0].t > maxSpanMs) return null;

  const valid = recent.filter((r) => r.v >= 0).map((r) => r.v);
  const lost = n - valid.length;
  // 告警須嚴格過半有效（n=1 為 1、n=2 為 2、n=3 為 2、n=4 為 3...），避免 n=2 時單筆尖峰+丟包誤報
  const minValidAlert = n === 1 ? 1 : Math.floor(n / 2) + 1;
  if (valid.length < minValidAlert) return null;
  const after = mean(valid);

  if (prev) {
    const limit = limitOf(prev.base, cfg);
    if (limit <= 0) return null;

    // 1. 先檢查恢復（優先於 rebase，避免剛好在 rebase 時間點恢復卻被靜默重定基準）
    //    恢復要求較高有效比例（避免高丟包期間誤判恢復）與遲滯區間（避免臨界值反覆抖動）
    const minValidRecover = n === 1 ? 1 : Math.max(2, Math.ceil(n * 0.8));
    const recoverLimit = limit * RECOVERY_HYSTERESIS;
    if (valid.length >= minValidRecover) {
      const recovered = cfg.upOnly && prev.dir === "up"
        ? valid.every((v) => v - prev.base < recoverLimit)
        : valid.every((v) => Math.abs(v - prev.base) < recoverLimit);
      if (recovered) {
        return {
          type: "recover",
          base: prev.base,
          after: after,
          samples: valid,
          lost: lost,
          since: prev.since,
          episodeStart: prev.start || Math.max(0, prev.since - n * intervalSec * 1000),
          recoverStart: recent[0].t,
        };
      }
    }

    // 2. 雙向監控下若延遲直接反轉到基準另一側且達門檻，切換告警方向而非卡在舊方向
    if (!cfg.upOnly) {
      let flipDir = "";
      if (prev.dir === "up" && valid.every((v) => prev.base - v >= limit)) flipDir = "down";
      else if (prev.dir === "down" && valid.every((v) => v - prev.base >= limit)) flipDir = "up";
      if (flipDir) {
        return {
          type: "alert",
          dir: flipDir,
          flipped: true,
          source: prev.source || "short",
          base: prev.base,
          after: after,
          samples: valid,
          lost: lost,
          baselineCount: 0,
          episodeStart: prev.start || Math.max(0, prev.since - n * intervalSec * 1000),
        };
      }
    }

    // 3. 超過重定基準時間仍未恢復，視為線路已穩定在新延遲
    if (now - prev.since >= cfg.rebaseMinutes * 60000) {
      const recentStart = recent[0].t;
      const winStart = recentStart - cfg.baselineMinutes * 60000;
      const steady = [];
      for (const r of records) {
        if (r.t >= winStart && r.v >= 0) steady.push(r.v);
      }
      const newBase = steady.length >= 3 ? median(steady) : after;
      return {
        type: "rebase",
        base: prev.base,
        newBase: newBase,
        after: after,
        samples: valid,
        lost: lost,
        since: prev.since,
        sampleCount: steady.length || valid.length,
      };
    }
    return null;
  }

  // 冷卻期間內不觸發新的延遲波動告警
  if (cooldownUntil && now < cooldownUntil) return null;

  // 計算短期基準延遲：排除先前已恢復的異常區間，並以中位數抵抗單次尖峰污染
  const recentStart = recent[0].t;
  const baseStart = recentStart - cfg.baselineMinutes * 60000;
  const baseline = [];
  for (const r of records) {
    if (r.t >= recentStart) break;
    if (r.t < baseStart || r.v < 0) continue;
    if (inExcluded(r.t, excludedRanges)) continue;
    baseline.push(r.v);
  }
  const expectedSamples = Math.floor((cfg.baselineMinutes * 60) / Math.max(1, intervalSec));
  const minBaseline = Math.min(MIN_BASELINE_SAMPLES, Math.max(3, Math.floor(expectedSamples * 0.5)));

  if (baseline.length >= minBaseline) {
    const base = median(baseline);
    const dir = matchDirection(valid, base, cfg);
    if (dir) {
      return {
        type: "alert",
        dir: dir,
        source: "short",
        base: base,
        after: after,
        samples: valid,
        lost: lost,
        baselineCount: baseline.length,
        episodeStart: recentStart,
      };
    }
  }

  // 第二道：長期基準偵測（抓數小時內緩慢爬升的「溫水煮青蛙」型延遲變化）
  if (cfg.longBaselineHours > 0 && longBaseInfo && Number.isFinite(longBaseInfo.value)) {
    const longBase = longBaseInfo.value;
    const dir = matchDirection(valid, longBase, cfg);
    if (dir) {
      return {
        type: "alert",
        dir: dir,
        source: "long",
        base: longBase,
        after: after,
        samples: valid,
        lost: lost,
        baselineCount: longBaseInfo.count || 0,
        // 緩慢爬升期間的前一個短期視窗本身也已偏離，一併納入異常區間以免恢復後誤報反向波動
        episodeStart: Math.max(0, recentStart - cfg.baselineMinutes * 60000),
      };
    }
  }

  return null;
}

// 持續高丟包偵測（適合抓節點仍在線但線路被牆／阻斷的情況）
function evaluateLoss(records, intervalSec, cfg, prevLoss, lossCooldownUntil, now) {
  if (cfg.lossMinutes <= 0 || !records.length) return null;
  const n = cfg.confirm;
  const latest = records[records.length - 1];
  const staleMs = Math.max(intervalSec * 3000, 5 * 60000);

  // 節點離線時不會有新紀錄，不視為線路被牆／高丟包
  if (now - latest.t > staleMs) return null;

  const recent = records.slice(-n);
  const tailValid = recent.filter((r) => r.v >= 0).map((r) => r.v);
  const tailLost = recent.length - tailValid.length;

  if (prevLoss) {
    const maxSpanMs = Math.max((n - 1) * intervalSec * 3000, staleMs);
    if (recent.length >= n && latest.t - recent[0].t <= maxSpanMs) {
      const minValidRecover = n === 1 ? 1 : Math.max(2, Math.ceil(n * 0.8));
      if (tailValid.length >= minValidRecover) {
        return {
          type: "loss_recover",
          after: mean(tailValid),
          samples: tailValid,
          lost: tailLost,
          since: prevLoss.since,
          episodeStart: prevLoss.start || prevLoss.since,
          recoverStart: recent[0].t,
        };
      }
    }
    return null;
  }

  if (lossCooldownUntil && now < lossCooldownUntil) return null;

  const windowMs = cfg.lossMinutes * 60000;
  const winStart = latest.t - windowMs;
  const win = [];
  for (let i = records.length - 1; i >= 0; i--) {
    if (records[i].t < winStart) break;
    win.push(records[i]);
  }
  win.reverse();

  // 視窗跨度須接近 lossMinutes，避免剛上線或剛新增任務時因樣本少而誤觸
  const minSpanMs = Math.max(0, windowMs - Math.max(intervalSec * 2000, 60000));
  if (win.length < 2 || latest.t - win[0].t < minSpanMs) return null;

  const expected = Math.floor((cfg.lossMinutes * 60) / Math.max(1, intervalSec));
  const minSamples = Math.max(2, Math.min(3, expected), Math.floor(expected * 0.5));
  if (win.length < minSamples) return null;

  let winLost = 0;
  let firstLostTs = 0;
  for (const r of win) {
    if (r.v < 0) {
      winLost++;
      if (!firstLostTs) firstLostTs = r.t;
    }
  }
  const lossPct = (winLost / win.length) * 100;
  if (lossPct < cfg.lossPercent) return null;

  // 最近 N 筆也必須仍在丟包，避免尾端已經恢復正常卻被前段歷史觸發告警
  const minTailLost = n === 1 ? 1 : Math.ceil(n / 2);
  if (recent.length < n || tailLost < minTailLost) return null;

  // 第一筆丟包至今的跨度也須達標，確保確實已持續足夠時間
  const requiredLossSpan = Math.max(0, windowMs * (cfg.lossPercent / 100) - Math.max(intervalSec * 1500, 60000));
  if (!firstLostTs || latest.t - firstLostTs < requiredLossSpan) return null;

  return {
    type: "loss_alert",
    lossPct: lossPct,
    lostCount: winLost,
    totalCount: win.length,
    windowMinutes: cfg.lossMinutes,
    episodeStart: firstLostTs,
  };
}

// ---------------------------------------------------------------- 主流程

async function mapLimit(items, limit, fn) {
  const out = new Array(items.length);
  let next = 0;
  const count = Math.min(limit, items.length);
  const workers = [];
  for (let i = 0; i < count; i++) {
    workers.push((async () => {
      while (next < items.length) {
        const idx = next++;
        out[idx] = await fn(items[idx], idx);
      }
    })());
  }
  await Promise.all(workers);
  return out;
}

function groupRecordsByClient(rawRecords, taskId, groups) {
  const byClient = {};
  const watchedClientCache = {};
  for (const r of rawRecords || []) {
    if (!r || !r.client) continue;
    const uuid = String(r.client);
    let ok = watchedClientCache[uuid];
    if (ok === undefined) {
      ok = pairWatched(groups, taskId, uuid);
      watchedClientCache[uuid] = ok;
    }
    if (!ok) continue;
    if (r.value === null || r.value === undefined || r.value === "") continue;
    const t = Date.parse(r.time);
    const v = Number(r.value);
    if (!Number.isFinite(t) || !Number.isFinite(v)) continue;
    (byClient[uuid] = byClient[uuid] || []).push({ t: t, v: v });
  }
  return byClient;
}

// 每 30 分鐘背景刷新一次長期基準（例如前 24 小時中位數）
async function refreshLongBaselines(cfg, watchedTasks, groups, now) {
  if (cfg.longBaselineHours <= 0) {
    if (Object.keys(longBaseline).length) {
      longBaseline = {};
      longBaselineHoursCached = 0;
      longRefreshedAt = 0;
      longTaskRefreshed = {};
      return true;
    }
    return false;
  }

  const hoursChanged = longBaselineHoursCached !== cfg.longBaselineHours;
  const expired = now - longRefreshedAt >= LONG_BASELINE_REFRESH_MS;
  const tasksToFetch = (hoursChanged || expired)
    ? watchedTasks
    : watchedTasks.filter((t) => !longTaskRefreshed[Number(t.id)]);
  if (!tasksToFetch.length) return false;

  const hoursStr = String(cfg.longBaselineHours);
  const minSpanMs = Math.min(cfg.longBaselineHours * 0.25, 2) * 3600000;
  let dirty = false;

  await mapLimit(tasksToFetch, FETCH_CONCURRENCY, async (task) => {
    const taskId = Number(task.id);
    const intervalSec = Math.max(1, Number(task.interval) || 60);
    let resp;
    try {
      resp = await server.call("public:getPingRecords", { task_id: String(task.id), hours: hoursStr });
    } catch (e) {
      console.error("[latency-alert] 更新長期基準失敗（" + task.name + "）：" + errText(e));
      return;
    }
    longTaskRefreshed[taskId] = true;
    const byClient = groupRecordsByClient(resp && resp.records, taskId, groups);
    const minSamples = Math.min(30, Math.max(10, Math.floor((minSpanMs / 1000) / intervalSec * 0.4)));

    for (const uuid of Object.keys(byClient)) {
      const key = taskId + ":" + uuid;
      // 正在告警或高丟包中的序列暫不覆蓋長期基準，避免異常資料污染
      if (alerting[key] || lossAlerting[key]) continue;

      const records = normalizeRecords(byClient[uuid]);
      const cutAt = rebasedAt[key] ? rebasedAt[key] - cfg.baselineMinutes * 60000 : 0;
      const valid = [];
      let firstTs = 0;
      let lastTs = 0;
      for (const r of records) {
        if (r.t < cutAt || r.v < 0) continue;
        if (inExcluded(r.t, excluded[key])) continue;
        if (!firstTs) firstTs = r.t;
        lastTs = r.t;
        valid.push(r.v);
      }
      if (valid.length >= minSamples && lastTs - firstTs >= minSpanMs) {
        longBaseline[key] = { value: median(valid), count: valid.length, updatedAt: now };
        dirty = true;
      }
    }
  });

  if (hoursChanged || expired) {
    longRefreshedAt = now;
    longBaselineHoursCached = cfg.longBaselineHours;
    dirty = true;
  }
  return dirty;
}

function applyAlertState(key, action, now) {
  alerting[key] = {
    base: action.base,
    after: action.after,
    dir: action.dir,
    since: now,
    start: action.episodeStart,
    source: action.source || "short",
  };
}

function applyRecoveryState(key, action, cfg, now) {
  if (action.recoverStart > action.episodeStart) {
    (excluded[key] = excluded[key] || []).push({ start: action.episodeStart, end: action.recoverStart });
  }
  delete alerting[key];
  if (cfg.cooldownMinutes > 0) cooldowns[key] = now + cfg.cooldownMinutes * 60000;
}

function applyRebaseState(key, action, cfg, now) {
  delete alerting[key];
  rebasedAt[key] = now;
  if (cfg.longBaselineHours > 0) {
    longBaseline[key] = { value: action.newBase, count: action.sampleCount, updatedAt: now };
  }
  if (cfg.cooldownMinutes > 0) cooldowns[key] = now + cfg.cooldownMinutes * 60000;
}

function applyLossAlertState(key, action, now) {
  lossAlerting[key] = {
    since: now,
    start: action.episodeStart,
    lossPct: Math.round(action.lossPct),
  };
}

function applyLossRecoveryState(key, action, cfg, now) {
  if (action.recoverStart > action.episodeStart) {
    (excluded[key] = excluded[key] || []).push({ start: action.episodeStart, end: action.recoverStart });
  }
  delete lossAlerting[key];
  if (cfg.cooldownMinutes > 0) lossCooldowns[key] = now + cfg.cooldownMinutes * 60000;
}

async function check() {
  const cfg = readSettings(await server.getConfig());
  const [rawTasks, rawClients] = await Promise.all([
    server.call("admin:getAllPingTasks"),
    server.call("admin:listClients"),
  ]);
  const tasks = Array.isArray(rawTasks) ? rawTasks : [];
  const clients = Array.isArray(rawClients) ? rawClients : [];
  const groups = resolveGroups(cfg.groups, tasks);

  const warnings = [];
  if (cfg.partialTelegram) warnings.push("partial_telegram_config");
  for (const g of groups) {
    if (g.auto && g.tasks.size === 0) {
      warnings.push(g.label + "_auto_no_matched_tasks");
      console.warn("[latency-alert] 「" + g.label + "」未選任務且找不到名稱含 " + g.label + " 的監測任務。");
    }
  }

  const taskIds = new Set();
  for (const t of tasks) taskIds.add(Number(t.id));
  const clientIds = new Set();
  const nodeNames = {};
  for (const c of clients) {
    const id = String(c.uuid);
    clientIds.add(id);
    nodeNames[id] = c.name || id;
  }

  const watchedTasks = tasks.filter((t) => taskWatched(groups, Number(t.id)));
  const now = Date.now();
  const result = {
    series: 0,
    alerts: 0,
    recoveries: 0,
    rebased: 0,
    lossAlerts: 0,
    lossRecoveries: 0,
    taskErrors: 0,
  };
  if (warnings.length) result.warnings = warnings;
  let dirty = false;
  let maxIntervalSec = 60;

  // 若啟用長期基準，定期（每 30 分鐘）刷新一次 24h 中位數
  if (await refreshLongBaselines(cfg, watchedTasks, groups, now)) dirty = true;

  // 有限併發拉取各任務紀錄，單一任務失敗不影響其他任務
  const lookbackMinutes = Math.max(cfg.baselineMinutes, cfg.lossMinutes);
  const fetched = await mapLimit(watchedTasks, FETCH_CONCURRENCY, async (task) => {
    const intervalSec = Math.max(1, Number(task.interval) || 60);
    if (intervalSec > maxIntervalSec) maxIntervalSec = intervalSec;
    const confirmMinutes = Math.ceil((intervalSec * cfg.confirm) / 60);
    const hours = String(Math.ceil((lookbackMinutes + confirmMinutes + 10) / 60) + 1);
    try {
      const resp = await server.call("public:getPingRecords", { task_id: String(task.id), hours: hours });
      return { task: task, intervalSec: intervalSec, records: (resp && resp.records) || [] };
    } catch (e) {
      result.taskErrors++;
      console.error("[latency-alert] 讀取任務紀錄失敗（" + task.name + "）：" + errText(e));
      return null;
    }
  });

  const pendingAlerts = [];
  const pendingRecoveries = [];
  const pendingRebases = [];
  const pendingLossAlerts = [];
  const pendingLossRecoveries = [];

  for (const item of fetched) {
    if (!item) continue;
    const task = item.task;
    const taskId = Number(task.id);
    const byClient = groupRecordsByClient(item.records, taskId, groups);

    for (const uuid of Object.keys(byClient)) {
      const key = taskId + ":" + uuid;
      result.series++;
      const records = normalizeRecords(byClient[uuid]);
      const who = { uuid: uuid, node: nodeNames[uuid] || uuid, task: task.name, type: task.type };

      // 1. 延遲波動檢查
      const action = evaluate(
        records,
        item.intervalSec,
        cfg,
        alerting[key],
        excluded[key],
        longBaseline[key],
        cooldowns[key],
        now
      );
      if (action) {
        if (action.type === "alert") {
          pendingAlerts.push({ key: key, who: who, action: action });
        } else if (action.type === "recover") {
          if (cfg.notifyRecovery) {
            pendingRecoveries.push({ key: key, who: who, action: action });
          } else {
            applyRecoveryState(key, action, cfg, now);
            result.recoveries++;
            dirty = true;
          }
        } else if (action.type === "rebase") {
          if (cfg.notifyRebase) {
            pendingRebases.push({ key: key, who: who, action: action });
          } else {
            applyRebaseState(key, action, cfg, now);
            result.rebased++;
            dirty = true;
          }
        }
      }

      // 2. 持續高丟包（疑似被牆／阻斷）檢查
      const lossAction = evaluateLoss(records, item.intervalSec, cfg, lossAlerting[key], lossCooldowns[key], now);
      if (lossAction) {
        if (lossAction.type === "loss_alert") {
          pendingLossAlerts.push({ key: key, who: who, action: lossAction });
        } else if (lossAction.type === "loss_recover") {
          if (cfg.notifyRecovery) {
            pendingLossRecoveries.push({ key: key, who: who, action: lossAction });
          } else {
            applyLossRecoveryState(key, lossAction, cfg, now);
            result.lossRecoveries++;
            dirty = true;
          }
        }
      }
    }
  }

  // 發送各類通知：少量逐筆詳細、大量突發自動分批彙整避免 429 限流
  if (await dispatchAlerts(cfg, pendingAlerts, result, now)) dirty = true;
  if (await dispatchRecoveries(cfg, pendingRecoveries, result, now)) dirty = true;
  if (await dispatchRebases(cfg, pendingRebases, result, now)) dirty = true;
  if (await dispatchLossAlerts(cfg, pendingLossAlerts, result, now)) dirty = true;
  if (await dispatchLossRecoveries(cfg, pendingLossRecoveries, result, now)) dirty = true;

  // 若使用者關閉高丟包偵測，清掉殘留的高丟包告警狀態
  if (cfg.lossMinutes <= 0 && Object.keys(lossAlerting).length) {
    lossAlerting = {};
    dirty = true;
  }

  // 僅清理「任務或節點已刪除／移出監控範圍」的殘留狀態，不因暫時無紀錄而誤刪
  for (const map of [alerting, lossAlerting, longBaseline, rebasedAt]) {
    for (const key of Object.keys(map)) {
      if (!keyStillValid(key, taskIds, clientIds, groups)) {
        delete map[key];
        dirty = true;
      }
    }
  }
  for (const map of [cooldowns, lossCooldowns]) {
    for (const key of Object.keys(map)) {
      if (map[key] <= now || !keyStillValid(key, taskIds, clientIds, groups)) {
        delete map[key];
        dirty = true;
      }
    }
  }

  // 清理過期或無效的已排除異常區間（保留至少涵蓋長期基準視窗）
  const excludeKeepHours = Math.max(
    (lookbackMinutes + Math.ceil((maxIntervalSec * cfg.confirm) / 60) + 15) / 60,
    cfg.longBaselineHours + 1
  );
  const excludeCutoff = now - excludeKeepHours * 3600000;
  for (const key of Object.keys(excluded)) {
    if (!keyStillValid(key, taskIds, clientIds, groups)) {
      delete excluded[key];
      dirty = true;
      continue;
    }
    const prevList = excluded[key];
    const kept = Array.isArray(prevList) ? prevList.filter((ep) => ep && ep.end >= excludeCutoff) : [];
    if (!kept.length) {
      delete excluded[key];
      dirty = true;
    } else if (kept.length !== prevList.length) {
      excluded[key] = kept;
      dirty = true;
    }
  }

  if (dirty) saveState();
  return result;
}

function keyStillValid(key, taskIds, clientIds, groups) {
  const sep = key.indexOf(":");
  if (sep <= 0) return false;
  const taskId = Number(key.slice(0, sep));
  const uuid = key.slice(sep + 1);
  return taskIds.has(taskId) && clientIds.has(uuid) && pairWatched(groups, taskId, uuid);
}

async function dispatchQueue(cfg, items, opts) {
  if (!items.length) return false;
  let dirty = false;
  if (items.length < BATCH_NOTIFY_THRESHOLD) {
    for (const it of items) {
      try {
        await notify(cfg, opts.singleMsg(it));
        opts.onSuccess(it);
        dirty = true;
      } catch (e) {
        console.error("[latency-alert] " + opts.label + "發送失敗（" + it.who.node + " / " + it.who.task + "）：" + errText(e));
      }
    }
    return dirty;
  }

  const totalChunks = Math.ceil(items.length / BATCH_CHUNK_SIZE);
  for (let i = 0; i < items.length; i += BATCH_CHUNK_SIZE) {
    const chunk = items.slice(i, i + BATCH_CHUNK_SIZE);
    const page = Math.floor(i / BATCH_CHUNK_SIZE) + 1;
    try {
      await notify(cfg, opts.batchMsg(chunk, page, totalChunks, items.length));
      for (const it of chunk) opts.onSuccess(it);
      dirty = true;
    } catch (e) {
      console.error("[latency-alert] 彙整" + opts.label + "發送失敗（第 " + page + "/" + totalChunks + " 批）：" + errText(e));
    }
  }
  return dirty;
}

function dispatchAlerts(cfg, items, result, now) {
  return dispatchQueue(cfg, items, {
    label: "告警通知",
    singleMsg: (it) => alertMessage(it.who, it.action, cfg, now),
    batchMsg: (chunk, page, total, count) => batchAlertMessage(chunk, page, total, count, cfg, now),
    onSuccess: (it) => {
      applyAlertState(it.key, it.action, now);
      result.alerts++;
    },
  });
}

function dispatchRecoveries(cfg, items, result, now) {
  return dispatchQueue(cfg, items, {
    label: "恢復通知",
    singleMsg: (it) => recoveryMessage(it.who, it.action, now),
    batchMsg: (chunk, page, total, count) => batchRecoveryMessage(chunk, page, total, count, now),
    onSuccess: (it) => {
      applyRecoveryState(it.key, it.action, cfg, now);
      result.recoveries++;
    },
  });
}

function dispatchRebases(cfg, items, result, now) {
  return dispatchQueue(cfg, items, {
    label: "基準更新通知",
    singleMsg: (it) => rebaseMessage(it.who, it.action, now),
    batchMsg: (chunk, page, total, count) => batchRebaseMessage(chunk, page, total, count, now),
    onSuccess: (it) => {
      applyRebaseState(it.key, it.action, cfg, now);
      result.rebased++;
    },
  });
}

function dispatchLossAlerts(cfg, items, result, now) {
  return dispatchQueue(cfg, items, {
    label: "高丟包告警",
    singleMsg: (it) => lossAlertMessage(it.who, it.action, now),
    batchMsg: (chunk, page, total, count) => batchLossAlertMessage(chunk, page, total, count, now),
    onSuccess: (it) => {
      applyLossAlertState(it.key, it.action, now);
      result.lossAlerts++;
    },
  });
}

function dispatchLossRecoveries(cfg, items, result, now) {
  return dispatchQueue(cfg, items, {
    label: "丟包恢復通知",
    singleMsg: (it) => lossRecoveryMessage(it.who, it.action, now),
    batchMsg: (chunk, page, total, count) => batchLossRecoveryMessage(chunk, page, total, count, now),
    onSuccess: (it) => {
      applyLossRecoveryState(it.key, it.action, cfg, now);
      result.lossRecoveries++;
    },
  });
}

// ---------------------------------------------------------------- 訊息

function esc(text) {
  return String(text).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function fmtMs(v) {
  return (Math.round(v * 10) / 10).toFixed(1) + " ms";
}

function pad(n) {
  return n < 10 ? "0" + n : "" + n;
}

function fmtTime(ts) {
  const d = new Date(ts);
  return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) + " " +
    pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
}

function fmtDuration(ms) {
  const mins = Math.max(1, Math.round(ms / 60000));
  if (mins < 60) return "約 " + mins + " 分鐘";
  const h = Math.floor(mins / 60);
  const m = mins % 60;
  return m ? "約 " + h + " 小時 " + m + " 分鐘" : "約 " + h + " 小時";
}

function fmtChange(base, after) {
  const diff = after - base;
  const sign = diff >= 0 ? "+" : "−";
  const pct = base > 0 ? "（" + sign + Math.round((Math.abs(diff) / base) * 100) + "%）" : "";
  return sign + fmtMs(Math.abs(diff)) + pct;
}

function header(who) {
  return "節點：<b>" + esc(who.node) + "</b>\n監測：" + esc(who.task) + "（" + esc(who.type) + "）\n";
}

function uniqueUuids(items) {
  const set = new Set();
  for (const it of items) {
    if (it.who && it.who.uuid) set.add(it.who.uuid);
  }
  return Array.from(set);
}

// 訊息一律回傳 { emoji, title, body, uuid?, uuids? }，body 是 Telegram HTML
function alertMessage(who, a, cfg, now) {
  let baseInfo;
  if (a.flipped) {
    baseInfo = "（延續原基準）";
  } else if (a.source === "long") {
    baseInfo = "（長期：前 " + cfg.longBaselineHours + " 小時中位數，" + a.baselineCount + " 筆）";
  } else {
    baseInfo = "（前 " + cfg.baselineMinutes + " 分鐘中位數，" + a.baselineCount + " 筆）";
  }
  const dirLabel = a.dir === "up"
    ? (a.source === "long" ? "延遲緩慢上升" : "延遲上升")
    : (a.source === "long" ? "延遲緩慢下降" : "延遲下降");
  let body = header(who) +
    "波動前基準延遲：<b>" + fmtMs(a.base) + "</b>" + baseInfo + "\n" +
    "波動後延遲：<b>" + fmtMs(a.after) + "</b>（最近 " + a.samples.length + " 筆：" + a.samples.map(Math.round).join(" / ") + " ms）\n" +
    "變化：" + fmtChange(a.base, a.after) + "\n";
  if (a.lost > 0) body += "丟包：最近 " + (a.samples.length + a.lost) + " 筆中 " + a.lost + " 筆\n";
  return {
    emoji: "⚠️",
    title: "延遲波動告警｜" + dirLabel,
    body: body + "時間：" + fmtTime(now),
    uuid: who.uuid,
  };
}

function recoveryMessage(who, a, now) {
  return {
    emoji: "✅",
    title: "延遲已恢復",
    body: header(who) +
      "波動前基準延遲：<b>" + fmtMs(a.base) + "</b>\n" +
      "目前延遲：<b>" + fmtMs(a.after) + "</b>\n" +
      "波動持續：" + fmtDuration(now - a.since) + "\n" +
      "時間：" + fmtTime(now),
    uuid: who.uuid,
  };
}

function rebaseMessage(who, a, now) {
  return {
    emoji: "ℹ️",
    title: "延遲基準已更新",
    body: header(who) +
      "原基準延遲：" + fmtMs(a.base) + "\n" +
      "新基準延遲：<b>" + fmtMs(a.newBase) + "</b>（最近 " + a.samples.length + " 筆：" + a.samples.map(Math.round).join(" / ") + " ms）\n" +
      "變化：" + fmtChange(a.base, a.newBase) + "\n" +
      "持續時間：" + fmtDuration(now - a.since) + "（逾時未恢復，已改用新延遲作為基準）\n" +
      "時間：" + fmtTime(now),
    uuid: who.uuid,
  };
}

function lossAlertMessage(who, a, now) {
  const pct = Math.round(a.lossPct);
  return {
    emoji: "🚫",
    title: "持續高丟包告警｜疑似線路阻斷",
    body: header(who) +
      "丟包率：<b>" + pct + "%</b>（最近 " + a.windowMinutes + " 分鐘內 " + a.lostCount + "/" + a.totalCount + " 筆丟包）\n" +
      "說明：節點仍在線回報但持續高丟包，請確認線路是否異常或受阻\n" +
      "時間：" + fmtTime(now),
    uuid: who.uuid,
  };
}

function lossRecoveryMessage(who, a, now) {
  return {
    emoji: "✅",
    title: "高丟包已恢復",
    body: header(who) +
      "目前延遲：<b>" + fmtMs(a.after) + "</b>（最近 " + a.samples.length + " 筆：" + a.samples.map(Math.round).join(" / ") + " ms）\n" +
      "阻斷持續：" + fmtDuration(now - a.since) + "\n" +
      "時間：" + fmtTime(now),
    uuid: who.uuid,
  };
}

function batchAlertMessage(chunk, page, totalChunks, totalCount, cfg, now) {
  const suffix = totalChunks > 1 ? "（" + page + "/" + totalChunks + "）" : "";
  const lines = chunk.map((it) => {
    const arrow = it.action.dir === "up" ? "↑" : "↓";
    const tag = it.action.source === "long" ? "[長期]" : "";
    return "• " + tag + "<b>" + esc(it.who.node) + "</b> / " + esc(it.who.task) + "：" +
      fmtMs(it.action.base) + " → <b>" + fmtMs(it.action.after) + "</b> " + arrow + " " +
      fmtChange(it.action.base, it.action.after);
  });
  return {
    emoji: "⚠️",
    title: "延遲波動告警｜共 " + totalCount + " 筆" + suffix,
    body: lines.join("\n") + "\n基準視窗：短期 " + cfg.baselineMinutes + " 分鐘" +
      (cfg.longBaselineHours > 0 ? "／長期 " + cfg.longBaselineHours + " 小時" : "") + "中位數\n時間：" + fmtTime(now),
    uuids: uniqueUuids(chunk),
  };
}

function batchRecoveryMessage(chunk, page, totalChunks, totalCount, now) {
  const suffix = totalChunks > 1 ? "（" + page + "/" + totalChunks + "）" : "";
  const lines = chunk.map((it) =>
    "• <b>" + esc(it.who.node) + "</b> / " + esc(it.who.task) + "：" +
    fmtMs(it.action.base) + "（目前 <b>" + fmtMs(it.action.after) + "</b>，持續" + fmtDuration(now - it.action.since) + "）"
  );
  return {
    emoji: "✅",
    title: "延遲已恢復｜共 " + totalCount + " 筆" + suffix,
    body: lines.join("\n") + "\n時間：" + fmtTime(now),
    uuids: uniqueUuids(chunk),
  };
}

function batchRebaseMessage(chunk, page, totalChunks, totalCount, now) {
  const suffix = totalChunks > 1 ? "（" + page + "/" + totalChunks + "）" : "";
  const lines = chunk.map((it) =>
    "• <b>" + esc(it.who.node) + "</b> / " + esc(it.who.task) + "：" +
    fmtMs(it.action.base) + " → <b>" + fmtMs(it.action.newBase) + "</b> " +
    fmtChange(it.action.base, it.action.newBase)
  );
  return {
    emoji: "ℹ️",
    title: "延遲基準已更新｜共 " + totalCount + " 筆" + suffix,
    body: lines.join("\n") + "\n說明：逾時未恢復，已改用新延遲作為基準\n時間：" + fmtTime(now),
    uuids: uniqueUuids(chunk),
  };
}

function batchLossAlertMessage(chunk, page, totalChunks, totalCount, now) {
  const suffix = totalChunks > 1 ? "（" + page + "/" + totalChunks + "）" : "";
  const lines = chunk.map((it) =>
    "• <b>" + esc(it.who.node) + "</b> / " + esc(it.who.task) + "：丟包 <b>" +
    Math.round(it.action.lossPct) + "%</b>（" + it.action.lostCount + "/" + it.action.totalCount + " 筆，" +
    it.action.windowMinutes + " 分鐘）"
  );
  return {
    emoji: "🚫",
    title: "持續高丟包告警｜共 " + totalCount + " 筆" + suffix,
    body: lines.join("\n") + "\n時間：" + fmtTime(now),
    uuids: uniqueUuids(chunk),
  };
}

function batchLossRecoveryMessage(chunk, page, totalChunks, totalCount, now) {
  const suffix = totalChunks > 1 ? "（" + page + "/" + totalChunks + "）" : "";
  const lines = chunk.map((it) =>
    "• <b>" + esc(it.who.node) + "</b> / " + esc(it.who.task) + "：目前 <b>" +
    fmtMs(it.action.after) + "</b>（阻斷持續" + fmtDuration(now - it.action.since) + "）"
  );
  return {
    emoji: "✅",
    title: "高丟包已恢復｜共 " + totalCount + " 筆" + suffix,
    body: lines.join("\n") + "\n時間：" + fmtTime(now),
    uuids: uniqueUuids(chunk),
  };
}

function plain(html) {
  return html.replace(/<[^>]+>/g, "").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");
}

async function notify(cfg, msg) {
  if (!cfg.builtin) {
    return sendTelegram(cfg, msg.emoji + " <b>" + msg.title + "</b>\n" + msg.body);
  }
  const clientList = msg.uuids
    ? msg.uuids.map((id) => ({ uuid: id }))
    : (msg.uuid ? [{ uuid: msg.uuid }] : []);
  // 交給 Komari 內建通知：用後台設定的通知管道（例如 Telegram 機器人）與訊息範本發送
  await server.call("admin:sendNotification", {
    event: {
      event: msg.title,
      emoji: msg.emoji,
      message: plain(msg.body),
      clients: clientList,
    },
  });
}

async function sendTelegram(cfg, text) {
  const body = { chat_id: cfg.chatId, text: text, parse_mode: "HTML", disable_web_page_preview: true };
  if (cfg.threadId > 0) body.message_thread_id = cfg.threadId;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  let res;
  try {
    res = await fetch(cfg.apiBase + "/bot" + cfg.token + "/sendMessage", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
  } catch (e) {
    // 連線錯誤訊息會帶完整網址，別讓 Token 進到插件日誌
    const msg = e && e.name === "AbortError"
      ? "請求逾時（>" + (FETCH_TIMEOUT_MS / 1000) + "s）"
      : errText(e).split(cfg.token).join("<token>");
    throw new Error(msg);
  } finally {
    clearTimeout(timer);
  }
  const raw = await res.text();
  let data = null;
  try {
    data = JSON.parse(raw);
  } catch (e) {}
  if (!res.ok || !data || data.ok !== true) {
    throw new Error("HTTP " + res.status + " " + ((data && data.description) || raw.slice(0, 200)));
  }
}

function errText(e) {
  return (e && e.message) || String(e);
}
