const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const vm = require("vm");

const SCRIPT_PATH = path.join(__dirname, "..", "script.js");
const MANIFEST_PATH = path.join(__dirname, "..", "komari-plugin.json");

function createHarness(opts = {}) {
  const storageDir = fs.mkdtempSync(path.join(os.tmpdir(), "komari-test-"));
  if (opts.initialState !== undefined) {
    fs.writeFileSync(path.join(storageDir, "state.json"), JSON.stringify(opts.initialState));
  }

  const rpcs = {};
  const crons = [];
  const notifications = [];
  const telegramCalls = [];
  const pingQueries = [];
  const logs = { warn: [], error: [] };

  let currentConfig = opts.config || { test_on_load: false };
  let tasks = opts.tasks || [];
  let clients = opts.clients || [];
  let pingRecordsByTask = opts.pingRecordsByTask || {};
  let pingRecordsHandler = opts.pingRecordsHandler || null;
  let failingTasks = new Set(opts.failingTasks || []);

  const mockServer = {
    cron(spec, fn) {
      crons.push({ spec, fn });
    },
    registerRPC(name, fn) {
      rpcs[name] = fn;
    },
    async getConfig() {
      return currentConfig;
    },
    async call(method, params) {
      if (method === "admin:getAllPingTasks") return tasks;
      if (method === "admin:listClients") return clients;
      if (method === "public:getPingRecords") {
        pingQueries.push(params);
        if (failingTasks.has(String(params.task_id))) {
          throw new Error("simulated task failure");
        }
        if (pingRecordsHandler) return pingRecordsHandler(params);
        const recs = pingRecordsByTask[String(params.task_id)] || [];
        return { records: recs };
      }
      if (method === "admin:sendNotification") {
        if (opts.failNotify && opts.failNotify(params)) {
          throw new Error("simulated notify error");
        }
        notifications.push(params);
        return { ok: true };
      }
      throw new Error("unexpected server.call: " + method);
    },
  };

  const mockFetch = async (url, init) => {
    if (opts.fetchHandler) return opts.fetchHandler(url, init);
    telegramCalls.push({ url, body: JSON.parse(init.body) });
    return {
      ok: true,
      status: 200,
      async text() {
        return JSON.stringify({ ok: true, result: { message_id: 1 } });
      },
    };
  };

  const sandbox = {
    require(mod) {
      if (mod === "server") return mockServer;
      return require(mod);
    },
    __storageDir__: storageDir,
    fetch: mockFetch,
    AbortController,
    setTimeout,
    clearTimeout,
    console: {
      log: () => {},
      warn: (m) => logs.warn.push(String(m)),
      error: (m) => logs.error.push(String(m)),
    },
    Date,
    Math,
    Number,
    String,
    Boolean,
    Array,
    Object,
    Set,
    Map,
    JSON,
    Promise,
    Error,
  };

  const code = fs.readFileSync(SCRIPT_PATH, "utf8");
  const ctx = vm.createContext(sandbox);
  vm.runInContext(code, ctx, { filename: SCRIPT_PATH });
  ctx.load();

  return {
    ctx,
    storageDir,
    rpcs,
    crons,
    notifications,
    telegramCalls,
    pingQueries,
    logs,
    setConfig(c) { currentConfig = c; },
    setTasks(t) { tasks = t; },
    setClients(c) { clients = c; },
    setPingRecords(m) { pingRecordsByTask = m; },
    setPingRecordsHandler(fn) { pingRecordsHandler = fn; },
    setFailingTasks(arr) { failingTasks = new Set(arr.map(String)); },
    readState() {
      const p = path.join(storageDir, "state.json");
      return fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, "utf8")) : null;
    },
    cleanup() {
      fs.rmSync(storageDir, { recursive: true, force: true });
    },
  };
}

function makeSeries(startTs, stepSec, values) {
  return values.map((v, i) => ({ t: startTs + i * stepSec * 1000, v }));
}

async function runTests() {
  let passed = 0;
  async function test(name, fn) {
    try {
      await fn();
      console.log("PASS:", name);
      passed++;
    } catch (e) {
      console.error("FAIL:", name);
      console.error(e);
      process.exitCode = 1;
    }
  }

  await test("manifest JSON is valid and keeps all original + new v1.5.0 config keys", async () => {
    const manifest = JSON.parse(fs.readFileSync(MANIFEST_PATH, "utf8"));
    const keys = manifest.configuration.data.map((d) => d.key).filter(Boolean);
    const expected = [
      "bot_token", "chat_id", "thread_id", "api_base", "test_on_load",
      "threshold_percent", "threshold_ms", "confirm_samples", "baseline_minutes", "long_baseline_hours", "direction",
      "loss_minutes", "loss_percent",
      "notify_recovery", "rebase_minutes", "notify_rebase", "cooldown_minutes",
      "nodes", "tasks", "nodes2", "tasks2",
    ];
    for (const k of expected) {
      assert(keys.includes(k), "missing config key: " + k);
    }
  });

  await test("median baseline resists extreme spikes in baseline window", async () => {
    const h = createHarness();
    try {
      const cfg = h.ctx.readSettings({ long_baseline_hours: 0 });
      const baseVals = Array(20).fill(50);
      baseVals[5] = 3000;
      baseVals[10] = 2500;
      const vals = [...baseVals, 50, 51, 49];
      const now = 1_700_000_000_000;
      const recs = makeSeries(now - (vals.length - 1) * 60_000, 60, vals);
      const action = h.ctx.evaluate(recs, 60, cfg, undefined, [], undefined, 0, now);
      assert.strictEqual(action, null);
    } finally {
      h.cleanup();
    }
  });

  await test("post-recovery excluded window prevents immediate false opposite-direction alert", async () => {
    const h = createHarness();
    try {
      const cfg = h.ctx.readSettings({ baseline_minutes: 30, rebase_minutes: 60, confirm_samples: 3, cooldown_minutes: 0, long_baseline_hours: 0 });
      const t0 = 1_700_000_000_000;
      const vals = [...Array(10).fill(50), ...Array(25).fill(150), 50, 50, 50];
      const recs = makeSeries(t0, 60, vals);

      const alertRecs = recs.slice(0, 13);
      const alertNow = alertRecs[alertRecs.length - 1].t + 10_000;
      const a1 = h.ctx.evaluate(alertRecs, 60, cfg, undefined, [], undefined, 0, alertNow);
      assert(a1 && a1.type === "alert" && a1.dir === "up");
      const prev = { base: a1.base, after: a1.after, dir: a1.dir, since: alertNow, start: a1.episodeStart };

      const recNow = recs[recs.length - 1].t + 10_000;
      const a2 = h.ctx.evaluate(recs, 60, cfg, prev, [], undefined, 0, recNow);
      assert(a2 && a2.type === "recover");
      const excluded = [{ start: a2.episodeStart, end: a2.recoverStart }];

      const a3 = h.ctx.evaluate(recs, 60, cfg, undefined, excluded, undefined, 0, recNow + 30_000);
      assert.strictEqual(a3, null);
    } finally {
      h.cleanup();
    }
  });

  await test("recovery hysteresis prevents flapping around threshold", async () => {
    const h = createHarness();
    try {
      const cfg = h.ctx.readSettings({ threshold_percent: 50, threshold_ms: 30, confirm_samples: 3 });
      const now = 1_700_000_000_000;
      const prev = { base: 50, after: 85, dir: "up", since: now - 5 * 60_000, start: now - 7 * 60_000 };
      const jitterRecs = makeSeries(now - 2 * 60_000, 60, [78, 77, 79]);
      assert.strictEqual(h.ctx.evaluate(jitterRecs, 60, cfg, prev, [], undefined, 0, now), null);
      const okRecs = makeSeries(now - 2 * 60_000, 60, [65, 64, 66]);
      const res = h.ctx.evaluate(okRecs, 60, cfg, prev, [], undefined, 0, now);
      assert(res && res.type === "recover");
    } finally {
      h.cleanup();
    }
  });

  await test("upOnly mode recovers even if latency drops well below original baseline", async () => {
    const h = createHarness();
    try {
      const cfg = h.ctx.readSettings({ direction: "up", threshold_percent: 50, threshold_ms: 30, confirm_samples: 3 });
      const now = 1_700_000_000_000;
      const prev = { base: 100, after: 200, dir: "up", since: now - 10 * 60_000, start: now - 12 * 60_000 };
      const recs = makeSeries(now - 2 * 60_000, 60, [30, 32, 28]);
      const res = h.ctx.evaluate(recs, 60, cfg, prev, [], undefined, 0, now);
      assert(res && res.type === "recover");
    } finally {
      h.cleanup();
    }
  });

  await test("both-direction mode handles direct flip from up to down", async () => {
    const h = createHarness();
    try {
      const cfg = h.ctx.readSettings({ direction: "both", threshold_percent: 50, threshold_ms: 30, confirm_samples: 3 });
      const now = 1_700_000_000_000;
      const prev = { base: 100, after: 200, dir: "up", since: now - 10 * 60_000, start: now - 12 * 60_000 };
      const recs = makeSeries(now - 2 * 60_000, 60, [30, 32, 28]);
      const res = h.ctx.evaluate(recs, 60, cfg, prev, [], undefined, 0, now);
      assert(res && res.type === "alert" && res.dir === "down" && res.flipped === true);
    } finally {
      h.cleanup();
    }
  });

  await test("recover takes precedence over rebase right at rebaseMinutes", async () => {
    const h = createHarness();
    try {
      const cfg = h.ctx.readSettings({ rebase_minutes: 60, baseline_minutes: 30, confirm_samples: 3 });
      const now = 1_700_000_000_000;
      const prev = { base: 50, after: 150, dir: "up", since: now - 61 * 60_000, start: now - 63 * 60_000 };
      const recs = makeSeries(now - 2 * 60_000, 60, [50, 51, 49]);
      const res = h.ctx.evaluate(recs, 60, cfg, prev, [], undefined, 0, now);
      assert(res && res.type === "recover");
    } finally {
      h.cleanup();
    }
  });

  await test("slow drift detected by long-term baseline when short-term sliding window misses it", async () => {
    const h = createHarness();
    try {
      const cfg = h.ctx.readSettings({ baseline_minutes: 30, long_baseline_hours: 24, threshold_percent: 50, threshold_ms: 30, confirm_samples: 3, cooldown_minutes: 0 });
      const now = 1_700_000_000_000;
      // Short-term 30 min has slowly drifted from 100ms to 135ms (median ~118ms, recent ~136ms -> diff 18ms < 30ms!)
      const vals = [];
      for (let i = 0; i < 33; i++) vals.push(100 + i * 1.15);
      const recs = makeSeries(now - (vals.length - 1) * 60_000, 60, vals);

      // Without long baseline -> misses slow drift
      assert.strictEqual(h.ctx.evaluate(recs, 60, cfg, undefined, [], undefined, 0, now), null);

      // With 24h long baseline = 50ms -> catches slow drift!
      const res = h.ctx.evaluate(recs, 60, cfg, undefined, [], { value: 50, count: 1200, updatedAt: now }, 0, now);
      assert(res && res.type === "alert" && res.source === "long" && res.dir === "up" && res.base === 50);

      // Verify episodeStart covers the drifted short-term window so recovery won't false-alert "down"
      const prev = { base: 50, after: res.after, dir: "up", since: now, start: res.episodeStart, source: "long" };
      const recVals = [...vals, 50, 51, 49];
      const recRecs = makeSeries(now - (vals.length - 1) * 60_000, 60, recVals);
      const recNow = recRecs[recRecs.length - 1].t + 10_000;
      const r1 = h.ctx.evaluate(recRecs, 60, cfg, prev, [], { value: 50, count: 1200, updatedAt: now }, 0, recNow);
      assert(r1 && r1.type === "recover");
      const excl = [{ start: r1.episodeStart, end: r1.recoverStart }];
      const r2 = h.ctx.evaluate(recRecs, 60, cfg, undefined, excl, { value: 50, count: 1200, updatedAt: now }, 0, recNow + 30_000);
      assert.strictEqual(r2, null, "no false down alert after slow-drift recovery");
    } finally {
      h.cleanup();
    }
  });

  await test("cooldown suppresses repeat alerts after recovery until expired", async () => {
    const h = createHarness();
    try {
      const cfg = h.ctx.readSettings({ cooldown_minutes: 15, long_baseline_hours: 0 });
      const now = 1_700_000_000_000;
      const vals = [...Array(15).fill(50), 150, 155, 160];
      const recs = makeSeries(now - (vals.length - 1) * 60_000, 60, vals);

      // Inside cooldown -> suppressed
      assert.strictEqual(h.ctx.evaluate(recs, 60, cfg, undefined, [], undefined, now + 5 * 60_000, now), null);
      // Cooldown expired -> alerts
      const res = h.ctx.evaluate(recs, 60, cfg, undefined, [], undefined, now - 1000, now);
      assert(res && res.type === "alert");
    } finally {
      h.cleanup();
    }
  });

  await test("sustained high packet loss detects 15-min block, ignores brief loss / offline / recovered tail, and recovers cleanly", async () => {
    const h = createHarness();
    try {
      const cfg = h.ctx.readSettings({ loss_minutes: 15, loss_percent: 80, confirm_samples: 3 });
      const now = 1_700_000_000_000;

      // 1. 15 min continuous 100% loss (16 samples at 60s) -> triggers loss_alert
      const blocked = makeSeries(now - 15 * 60_000, 60, Array(16).fill(-1));
      const a1 = h.ctx.evaluateLoss(blocked, 60, cfg, undefined, 0, now);
      assert(a1 && a1.type === "loss_alert" && a1.lossPct === 100);

      // 2. 87.5% loss (14/16 lost, tail still losing) -> triggers loss_alert
      const partialVals = Array(16).fill(-1);
      partialVals[4] = 55;
      partialVals[14] = 60; // 1 lucky packet in recent 3 (2/3 lost)
      const partial = makeSeries(now - 15 * 60_000, 60, partialVals);
      const a2 = h.ctx.evaluateLoss(partial, 60, cfg, undefined, 0, now);
      assert(a2 && a2.type === "loss_alert" && a2.lossPct >= 80);

      // 3. 13 min lost THEN last 3 min ALREADY recovered -> must NOT alert!
      const alreadyOk = makeSeries(now - 15 * 60_000, 60, [...Array(13).fill(-1), 50, 51, 49]);
      assert.strictEqual(h.ctx.evaluateLoss(alreadyOk, 60, cfg, undefined, 0, now), null);

      // 4. Short 5-min outage (only 5 lost out of 16) -> must NOT alert!
      const brief = makeSeries(now - 15 * 60_000, 60, [...Array(11).fill(50), ...Array(5).fill(-1)]);
      assert.strictEqual(h.ctx.evaluateLoss(brief, 60, cfg, undefined, 0, now), null);

      // 5. Brand-new node with only 5 min of records (all -1) -> span too short, must NOT alert!
      const tooShort = makeSeries(now - 5 * 60_000, 60, Array(6).fill(-1));
      assert.strictEqual(h.ctx.evaluateLoss(tooShort, 60, cfg, undefined, 0, now), null);

      // 6. Node completely offline (last record 10 min ago > staleMs) -> must NOT alert!
      const offline = makeSeries(now - 25 * 60_000, 60, Array(16).fill(-1));
      assert.strictEqual(h.ctx.evaluateLoss(offline, 60, cfg, undefined, 0, now), null);

      // 7. Recovery once 3 consecutive valid samples arrive
      const prevLoss = { since: now - 20 * 60_000, start: now - 35 * 60_000, lossPct: 100 };
      const stillBlocked = makeSeries(now - 2 * 60_000, 60, [50, -1, 50]);
      assert.strictEqual(h.ctx.evaluateLoss(stillBlocked, 60, cfg, prevLoss, 0, now), null);
      const recovered = makeSeries(now - 2 * 60_000, 60, [50, 52, 48]);
      const r = h.ctx.evaluateLoss(recovered, 60, cfg, prevLoss, 0, now);
      assert(r && r.type === "loss_recover" && Math.round(r.after) === 50);
    } finally {
      h.cleanup();
    }
  });

  await test("end-to-end rebase sends notification, updates longBaseline, and prevents re-alert loop", async () => {
    const now = Date.now();
    const mkIso = (minsAgo, val, client) => ({ time: new Date(now - minsAgo * 60_000).toISOString(), value: val, client });

    // 24h of data: first 22h at 50ms, last 70 min at 150ms
    const longRecs = [];
    for (let m = 24 * 60; m > 70; m -= 5) longRecs.push(mkIso(m, 50, "c1"));
    for (let m = 70; m >= 0; m--) longRecs.push(mkIso(m, 150, "c1"));
    const shortRecs = longRecs.filter((r) => Date.parse(r.time) >= now - 120 * 60_000);

    const h = createHarness({
      initialState: {
        version: 2,
        alerting: {
          "1:c1": { base: 50, after: 150, dir: "up", since: now - 65 * 60_000, start: now - 68 * 60_000, source: "short" },
        },
        longBaseline: {
          "1:c1": { value: 50, count: 250, updatedAt: now - 10 * 60_000 },
        },
        longRefreshedAt: now - 10 * 60_000,
        longBaselineHoursCached: 24,
      },
      tasks: [{ id: 1, name: "V4 Ping", type: "icmp", interval: 60 }],
      clients: [{ uuid: "c1", name: "HK-1" }],
      config: { test_on_load: false, bot_token: "123:TOK", chat_id: "999", notify_rebase: true, cooldown_minutes: 0, long_baseline_hours: 24 },
      pingRecordsHandler: (params) => ({
        records: Number(params.hours) >= 24 ? longRecs : shortRecs,
      }),
    });

    try {
      // Tick 1: should trigger rebase + send rebase notification + update longBaseline to 150
      const res1 = await h.ctx.check();
      assert.strictEqual(res1.rebased, 1);
      assert.strictEqual(h.telegramCalls.length, 1);
      assert(h.telegramCalls[0].body.text.includes("延遲基準已更新"));
      const st = h.readState();
      assert.strictEqual(Object.keys(st.alerting).length, 0);
      assert(st.longBaseline["1:c1"].value >= 145, "longBaseline must update to new rebased latency");
      assert(st.rebasedAt["1:c1"] > 0);

      // Tick 2 (even with cooldown_minutes=0): must NOT re-alert against old 50ms long baseline!
      const res2 = await h.ctx.check();
      assert.strictEqual(res2.alerts, 0, "must not re-alert after rebase");
      assert.strictEqual(h.telegramCalls.length, 1);
    } finally {
      h.cleanup();
    }
  });

  await test("end-to-end check: burst aggregation (>3 alerts), per-task error isolation, and preserving offline alerting state", async () => {
    const now = Date.now();
    const mkIso = (minsAgo, val, client) => ({ time: new Date(now - minsAgo * 60_000).toISOString(), value: val, client });

    const clients = [
      { uuid: "c1", name: "Node-1" },
      { uuid: "c2", name: "Node-2" },
      { uuid: "c3", name: "Node-3" },
      { uuid: "c4", name: "Node-4" },
      { uuid: "c5", name: "Node-5" },
      { uuid: "c-offline", name: "Node-Offline" },
    ];
    const tasks = [
      { id: 1, name: "V4 Ping", type: "icmp", interval: 60 },
      { id: 2, name: "V4 Broken", type: "icmp", interval: 60 },
    ];
    const records1 = [];
    for (const c of clients.slice(0, 5)) {
      for (let m = 20; m >= 4; m--) records1.push(mkIso(m, 40, c.uuid));
      records1.push(mkIso(3, 140, c.uuid), mkIso(2, 145, c.uuid), mkIso(1, 150, c.uuid));
    }
    records1.push({ time: new Date(now - 30_000).toISOString(), value: null, client: "c1" });

    const initialState = {
      "1:c-offline": { base: 30, after: 120, dir: "up", since: now - 10 * 60_000 },
      "1:c-deleted": { base: 30, after: 120, dir: "up", since: now - 10 * 60_000 },
    };

    const h = createHarness({
      initialState,
      tasks,
      clients,
      pingRecordsByTask: { "1": records1 },
      failingTasks: ["2"],
      config: { test_on_load: false, bot_token: "123:SECRET", chat_id: "999", long_baseline_hours: 0 },
    });

    try {
      const res = await h.ctx.check();
      assert.strictEqual(res.alerts, 5);
      assert.strictEqual(res.taskErrors, 1);
      assert.strictEqual(h.telegramCalls.length, 1);
      assert(h.telegramCalls[0].body.text.includes("共 5 筆"));

      const st = h.readState();
      assert.strictEqual(st.version, 2);
      assert(st.alerting["1:c-offline"]);
      assert(!st.alerting["1:c-deleted"]);
    } finally {
      h.cleanup();
    }
  });

  await test("sendTelegram redacts bot token on fetch error", async () => {
    const h = createHarness({
      fetchHandler: async (url) => {
        throw new Error("connect ECONNREFUSED " + url);
      },
    });
    try {
      const cfg = h.ctx.readSettings({ bot_token: "999:SUPER_SECRET_TOKEN", chat_id: "123" });
      let err = null;
      try {
        await h.ctx.sendTelegram(cfg, "hi");
      } catch (e) {
        err = e;
      }
      assert(err && !err.message.includes("SUPER_SECRET_TOKEN") && err.message.includes("<token>"));
    } finally {
      h.cleanup();
    }
  });

  console.log(`\nAll ${passed} tests completed.`);
}

runTests();
