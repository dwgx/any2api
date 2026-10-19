/* admin.js — Any2API 后台管理 dashboard 渲染层（零依赖、无框架、零 fetch）
 *
 * 结构（L19-W75 重排版；W77-2 加启动层/切页动效/统一控件）：
 *   helpers（转义/格式化/内联 SVG 趋势图/缺失值哨兵/数据源图例）
 *   8 个页面的 render 函数（数据全部来自 window.A2A_LIVE —— admin-data.js 装载）
 *   路由（#/page）+ 抽屉 + 二次确认对话框 + toast + 主题 + 10s 自动刷新 + 门控 + 启动层
 *
 * 纪律：所有插值走 esc()；本文件**不发起任何网络请求**（唯一网络面 = admin-data.js）；
 * 破坏性动作 = 二次确认 → A2AAdmin.revoke/resetCooldown 真发请求 → 刷新 + toast；
 * 未实现的动作（kill switch / 任务取消 / 探针 / 告警静音）按钮一律 disabled 且写「未实现」，
 * 绝不假装成功、绝不造审计行。
 * 启动层：首帧盖 #boot，首次装载/落锁后收掉（最短 300ms；prefers-reduced-motion 关动效）；
 * 登录成功与切页都必须走 render —— 只补数据不渲染 = 空壳（W77-2 修的就是这条）。
 * 控件：统一 .ad-ctl* / .ad-seg（32/28px、8/10px 圆角、1px 边框、四态齐全）。
 * 来源标注：不在每张卡片挂「已有」/ API path chip（会撑破卡片）；改为每页底部一个
 * <details>「数据源」图例，从 A2A_LIVE.meta.sources 取状态与端点。
 */
(function () {
  "use strict";

  var M = window.A2A_LIVE;
  var state = {
    page: "overview",
    traffic: { window: "", entry: "", model: "", status: "", upstream: "", stream: "", failed: "", q: "" },
    sessions: { q: "", status: "" },
    models: { q: "", upstream: "", usage: "" },
    audit: { q: "", kind: "" },
    usage: { scope: "session", day: "" },
    probes: {},   // 本会话内最近一次探针结果（键 = 上游名）——进程内 UI 态，非持久、非伪造数据
    lastRefresh: ""
  };

  /* ---------------------------------------------------------------- helpers */
  function $(sel, root) { return (root || document).querySelector(sel); }
  function $$(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }
  function esc(s) {
    return String(s === null || s === undefined ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }
  function mono(s) { return '<span class="mono">' + esc(s) + "</span>"; }
  function num(n) { return (n === null || n === undefined) ? "—" : Number(n).toLocaleString("en-US"); }
  function num1(n) { return (n === null || n === undefined) ? "—" : (Math.round(Number(n) * 10) / 10).toFixed(1); }
  function pct(a, b) { return (a === null || b === null || a === undefined || b === undefined || b <= 0) ? "—" : (100 * a / b).toFixed(1) + "%"; }
  function ms2s(v) { return v === null || v === undefined ? "—" : (v / 1000).toFixed(2) + "s"; }
  function fmtBytes(n) {
    if (n === null || n === undefined) return "—";
    if (!n) return "0 B";
    var u = ["B", "KiB", "MiB", "GiB"], i = 0;
    while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
    return (i === 0 ? n : n.toFixed(1)) + " " + u[i];
  }
  function ts(t) { return new Date(t).toISOString().replace("T", " ").replace("Z", "Z"); }
  function hhmm(t) { return t ? new Date(t).toISOString().slice(11, 19) + "Z" : "—"; }
  function rel(t) {
    var d = Date.now() - t, s = Math.round(d / 1000);
    if (Math.abs(s) < 60) return s + "s 前";
    if (Math.abs(s) < 3600) return Math.round(s / 60) + "m 前";
    return Math.round(s / 3600) + "h 前";
  }
  function dur(s) {
    if (s === null || s === undefined) return "—";
    s = Math.max(0, Math.round(s));
    var d = Math.floor(s / 86400), h = Math.floor((s % 86400) / 3600), m = Math.floor((s % 3600) / 60);
    return (d ? d + "d " : "") + (h ? h + "h " : "") + m + "m";
  }
  /* 缺失值哨兵：端点不存在 / 字段为 null（= 无计数器）时一律「未提供」，绝不显示 0/空表。
   * 注意：不把「—」与「未提供」拼在一起（重复表达缺失）；要么「—」（不适用），要么本标记。 */
  function none(note) { return '<span class="none" title="' + esc(note || "端点或字段未提供") + '">未提供</span>'; }
  /* 动态尺寸走 CSSOM：CSP `style-src 'self'` 会屏蔽内联 style 属性（实测
   * style-src-attr 违规），但 `el.style.x = …`（CSSOM 赋值）不在该指令管辖内。
   * 直方图柱高 = data-h，gauge 宽 = data-pct。 */
  function applyDynSizes(root) {
    if (!root) return;
    $$(".bars i[data-h]", root).forEach(function (el) { el.style.height = el.getAttribute("data-h") + "px"; });
    $$(".gauge[data-pct]", root).forEach(function (el) {
      var bar = el.querySelector("i");
      if (bar) bar.style.width = el.getAttribute("data-pct") + "%";
    });
  }
  function orNone(v, fmt) {
    if (v === null || v === undefined || v === "") return none("");
    return fmt ? fmt(v) : esc(v);
  }
  function unavail(section, title) {
    var reason = (section && section.reason) || "端点未返回数据";
    return '<div class="unavail"><b>' + esc(title || "未提供") + "</b><span>" + esc(reason) + "</span></div>";
  }

  /* 数据源图例：每页底部一个 <details>，替代原先挂在每张卡片头的 [状态] + API chip。
   * 状态来自 A2A_LIVE.meta.sources；端点路径可换行，不产生 nowrap 宽条。 */
  function sourceLegend(keys) {
    var sources = (M.meta || {}).sources || {};
    var rows = (keys || []).map(function (k) {
      var s = sources[k];
      if (!s) return "";
      var ok = s.state === "已有";
      return '<div class="source-row"><span class="source-state ' + (ok ? "ok" : "missing") + '">' + esc(s.state) + '</span><span><span class="source-api">' + esc(s.api) + "</span>" + (s.ref ? ' <span class="faint">· ' + esc(s.ref) + "</span>" : "") + "</span></div>";
    }).join("");
    if (!rows) return "";
    return '<details class="source-legend"><summary>数据源 · 端点与字段核对</summary><div class="source-list">' + rows + "</div></details>";
  }
  function defRow(k, v) { return '<div class="def-row"><span class="def-k">' + esc(k) + '</span><span class="def-v">' + v + "</span></div>"; }
  function healthItem(tone, label, value) {
    return '<div class="health-item"><span class="health-dot ' + (tone || "") + '" aria-hidden="true"></span><div class="health-body"><div class="health-label">' + esc(label) + '</div><div class="health-value">' + esc(value) + "</div></div></div>";
  }
  function badge(kind, text) { return '<span class="badge ' + kind + '">' + esc(text) + "</span>"; }
  function statusBadge(code) {
    if (code >= 500) return badge("err", String(code));
    if (code >= 400) return badge("warn", String(code));
    if (code >= 200) return badge("ok", String(code));
    return badge("plain", "—");
  }
  function outcomeBadge(o) {
    var map = { completed: "ok", precommit_error: "warn", incomplete_stream: "err", quota_exhausted: "err", rate_limited: "warn", capacity_exhausted: "err" };
    return badge(map[o] || "plain", o || "—");
  }

  /* 内联 SVG sparkline（零外部依赖；空序列 = 显式「未提供」占位，不画假的平线） */
  function spark(values, opt) {
    opt = opt || {};
    if (!values || !values.length) return '<span class="faint foot">未提供</span>';
    var w = opt.w || 260, h = opt.h || 40, pad = 2, color = opt.color || "var(--ad-accent)";
    var min = Math.min.apply(null, values), max = Math.max.apply(null, values);
    var span = (max - min) || 1;
    var step = (w - pad * 2) / Math.max(1, values.length - 1);
    var pts = values.map(function (v, i) {
      var x = pad + i * step, y = h - pad - ((v - min) / span) * (h - pad * 2);
      return (Math.round(x * 10) / 10) + "," + (Math.round(y * 10) / 10);
    });
    var area = "0," + (h - pad) + " " + pts.join(" ") + " " + (w - pad) + "," + (h - pad);
    return '<svg width="' + w + '" height="' + h + '" viewBox="0 0 ' + w + " " + h + '" role="img" aria-label="' + esc(opt.alt || "sparkline 趋势") + '">' +
      '<polygon points="' + area + '" fill="' + color + '" opacity="0.10"></polygon>' +
      '<polyline points="' + pts.join(" ") + '" fill="none" stroke="' + color + '" stroke-width="1.5" stroke-linejoin="round"></polyline>' +
      '<line x1="' + pad + '" y1="' + (h - pad) + '" x2="' + (w - pad) + '" y2="' + (h - pad) + '" stroke="var(--ad-border)"></line>' +
      "</svg>";
  }
  /* 延迟直方图：`buckets` = 各段计数，`edges` = /admin/metrics 的边界表
   * （14 个有限上界 + "+Inf" 收尾 = 15 段）；无 edges 时退化为等距标签。 */
  function histogram(h) {
    if (!h || !h.buckets || !h.buckets.length || !h.buckets.some(function (v) { return v > 0; })) {
      return '<div class="faint foot">窗口内无观测（未提供）</div>';
    }
    var max = Math.max.apply(null, h.buckets);
    var edges = h.edges || [];
    return '<div class="bars">' + h.buckets.map(function (v, i) {
      var px = Math.max(2, Math.round(64 * v / max));
      var lo = i === 0 ? 0 : edges[i - 1];
      var hi = (i < edges.length) ? edges[i] : "+Inf";
      var label = (lo === undefined ? "?" : lo) + "-" + hi + "ms: " + v + " req";
      return '<i class="bar' + (i >= 13 ? " warn" : "") + '" data-h="' + px + '" title="' + esc(label) + '"></i>';
    }).join("") + "</div>";
  }
  function kpi(label, value, hint) {
    return '<div class="kpi"><div class="kpi-label">' + esc(label) + '</div><div class="kpi-value">' + value + '</div><div class="kpi-hint">' + (hint || "") + "</div></div>";
  }

  /* ---------------------------------------------------------------- 总览 */
  function pctPoolAvail() {
    var total = 0, ready = 0;
    ((M.pools && M.pools.upstreams) || []).forEach(function (u) {
      (u.credentials || []).forEach(function (c) { total++; if (!c.cooling && !c.busy) ready++; });
    });
    return { ready: ready, total: total };
  }
  function lastOf(xs) { return (xs && xs.length) ? xs[xs.length - 1] : null; }

  /* 趋势图：requests 面积 + errors / failovers 折线。viewBox 固定 1000×220，
   * preserveAspectRatio=none 让图随卡片宽度缩放；vector-effect 保持线宽。
   * 不在 SVG 里放文字（非等比缩放会压扁），轴标签走 HTML。 */
  function trendSvg(req, err, fail) {
    var n = req.length;
    if (!n) return '<div class="chart-frame chart-empty"><span>窗口内无观测（未提供）</span></div>';
    var W = 1000, H = 220, top = 10, bottom = 16;
    var max = 1;
    [req, err, fail].forEach(function (arr) { (arr || []).forEach(function (v) { if (v > max) max = v; }); });
    function points(arr) {
      var step = n > 1 ? W / (n - 1) : W;
      return (arr || []).map(function (v, i) {
        var x = Math.round((i * step) * 10) / 10;
        var y = Math.round((H - bottom - (v / max) * (H - top - bottom)) * 10) / 10;
        return x + "," + y;
      });
    }
    var rp = points(req), ep = points(err), fp = points(fail);
    var area = "0," + (H - bottom) + " " + rp.join(" ") + " " + W + "," + (H - bottom);
    var grid = [0, 1, 2, 3, 4].map(function (i) {
      var y = Math.round((top + (H - top - bottom) * i / 4) * 10) / 10;
      return '<line x1="0" y1="' + y + '" x2="' + W + '" y2="' + y + '" stroke="var(--ad-border)" stroke-width="1" vector-effect="non-scaling-stroke"></line>';
    }).join("");
    return '<div class="chart-frame"><svg class="trend-svg" viewBox="0 0 ' + W + " " + H + '" preserveAspectRatio="none" role="img" aria-label="请求 / 错误 / failover 趋势">' +
      grid +
      '<polygon points="' + area + '" fill="var(--ad-accent)" opacity="0.12"></polygon>' +
      '<polyline points="' + rp.join(" ") + '" fill="none" stroke="var(--ad-accent)" stroke-width="2" stroke-linejoin="round" vector-effect="non-scaling-stroke"></polyline>' +
      '<polyline points="' + ep.join(" ") + '" fill="none" stroke="var(--ad-err)" stroke-width="1.6" stroke-linejoin="round" vector-effect="non-scaling-stroke"></polyline>' +
      '<polyline points="' + fp.join(" ") + '" fill="none" stroke="var(--ad-warn)" stroke-width="1.4" stroke-dasharray="4 3" stroke-linejoin="round" vector-effect="non-scaling-stroke"></polyline>' +
      "</svg></div>";
  }
  function chartAxis(labels) {
    if (!labels || !labels.length) return "";
    var mid = labels[Math.floor(labels.length / 2)];
    return '<div class="chart-axis"><span>' + esc(labels[0]) + "</span><span>" + esc(mid) + "</span><span>" + esc(labels[labels.length - 1]) + "</span></div>";
  }
  function trendCard(mt, s) {
    if (mt.available === false) {
      return '<div class="card trend-card"><header><h2>流量趋势</h2></header><div class="body">' + unavail(mt, "指标面不可用（/admin/metrics）") + "</div></div>";
    }
    var req = s.requests || [], err = s.errors || [], fail = s.failovers || [];
    var last = lastOf;
    var hb = (mt.histogram && mt.histogram.buckets) || [];
    var hasHist = hb.length > 0 && hb.some(function (v) { return v > 0; });
    var histTotal = hb.reduce(function (a, b) { return a + b; }, 0);
    return '<div class="card trend-card">' +
      '<header><h2>流量趋势</h2><span class="dim foot-sm">' + num((mt.window_seconds || 0) / 60) + " 分钟 · " + num(mt.step_seconds || 60) + 's 桶</span><div class="spacer"></div>' +
        '<div class="chart-legend"><span class="legend-item"><span class="legend-swatch"></span>requests</span><span class="legend-item"><span class="legend-swatch err"></span>errors</span><span class="legend-item"><span class="legend-swatch warn"></span>failovers</span></div></header>' +
      '<div class="body">' + trendSvg(req, err, fail) + chartAxis(mt.labels) +
        '<div class="chart-foot">' +
          '<div class="chart-stat"><div class="k">requests / 桶</div><div class="v">' + num(last(req)) + "</div></div>" +
          '<div class="chart-stat"><div class="k">errors / 桶</div><div class="v">' + num(last(err)) + "</div></div>" +
          '<div class="chart-stat"><div class="k">failovers / 桶</div><div class="v">' + num(last(fail)) + "</div></div>" +
          '<div class="chart-stat"><div class="k">TTFT p50 / p95</div><div class="v">' + ms2s(last(s.ttft_p50_ms)) + " / " + ms2s(last(s.ttft_p95_ms)) + "</div></div>" +
          '<div class="chart-stat"><div class="k">total p50 / p95</div><div class="v">' + ms2s(last(s.total_p50_ms)) + " / " + ms2s(last(s.total_p95_ms)) + "</div></div>" +
        "</div>" +
        (hasHist ? '<div class="chart-hist"><div class="chart-hist-head"><span>延迟分布 · total</span><span>' + num(histTotal) + " 次观测</span></div>" + histogram(mt.histogram || {}) + "</div>" : "") +
      "</div></div>";
  }
  function alertsCard() {
    var active = (M.alerts && M.alerts.active) || [];
    var items = active.slice(0, 6).map(function (a) {
      var tone = a.severity === "error" ? "err" : a.severity === "warning" ? "warn" : "info";
      return '<div class="alert-item"><span class="alert-sev ' + tone + '"></span><div class="alert-main"><div class="alert-name">' + esc(a.name) + '</div><div class="alert-sub">' + esc(a.subject) + " · " + esc(a.detail) + '</div></div><span class="alert-time">' + esc(hhmm(a.since)) + "</span></div>";
    }).join("");
    if (!items) items = '<div class="alert-empty">当前无 active 告警<br><span class="faint">派生自 admin ring + 进程内状态</span></div>';
    return '<div class="card alerts-card"><header><h2>活跃告警</h2><div class="spacer"></div>' + badge(active.length ? "warn" : "ok", active.length + " active") + '</header><div class="body">' + items +
      (active.length > 6 ? '<a class="alert-more" href="#/audit">还有 ' + (active.length - 6) + " 条 →</a>" : "") + "</div></div>";
  }
  function eventsCard() {
    var logs = M.logs || {};
    var events = (logs.entries || []).slice(0, 10).map(function (e) {
      return "<tr><td class='mono'>" + num(e.seq) + "</td><td class='mono'>" + esc(hhmm(e.time)) + "</td><td>" + mono(e.kind) + "</td>" +
        "<td>" + esc(e.upstream || "—") + "</td><td>" + (e.status ? statusBadge(e.status) : "—") + "</td><td class='dim'>" + esc(e.message) + "</td></tr>";
    }).join("");
    if (!events) events = "<tr><td colspan='6' class='empty'>" + esc(logs.available === false ? (logs.reason || "未提供") : "ring 为空") + "</td></tr>";
    return '<div class="card table-card events-card"><header><h2>最近事件</h2><div class="spacer"></div><span class="card-note">ring ' + num(logs.count) + " / " + num(logs.capacity) + " · latest_seq " + num(logs.latest_seq) + '</span></header>' +
      '<div class="body flush"><div class="table-wrap"><table class="admin"><thead><tr><th>seq</th><th>时间</th><th>kind</th><th>upstream</th><th>status</th><th>message</th></tr></thead><tbody>' + events + "</tbody></table></div></div></div>";
  }
  function statusCard(dg, sys, cfg) {
    var rt = sys.runtime || {}, bd = sys.build || {}, st = sys.store || {}, pub = cfg.public || {}, ui = cfg.ui || {}, cap = (M.pools && M.pools.capacity) || {};
    var rows =
      defRow("状态", badge("ok", "up")) +
      defRow("commit", bd.commit ? mono(bd.commit) + (bd.commit_subject ? " " + esc(bd.commit_subject) : "") : none("构建期未注入 commit（/admin/system.build.commit 为空）")) +
      defRow("binary sha256[:8]", bd.binary_sha256_12 ? mono(bd.binary_sha256_12) : none("无 buildinfo sha 注入面（设计 §5.8 登记）")) +
      defRow("built at", orNone(bd.built_at, mono)) +
      defRow("uptime", esc(dur(rt.uptime_seconds))) +
      defRow("listen", cfg.listen ? mono(cfg.listen) : none("配置面未返回 listen")) +
      defRow("upstreams", '<span class="num">' + num((dg.upstreams || []).length) + "</span>（链序见「上游与池」）") +
      defRow("inflight / queue", '<span class="num">' + num(cap.inflight) + " / " + num(cap.queue_len) + "</span>（上限 " + num(cap.max_inflight) + "）") +
      defRow("public 模式", badge("plain", pub.enabled ? "on" : "off") + " access_mode=" + orNone(pub.access_mode)) +
      defRow("store 模式", badge("plain", st.mode || "—")) +
      defRow("schema 版本", none("无 schema_meta 只读面（/admin/system 不回该位）")) +
      defRow("ui", (ui.enabled ? badge("ok", "enabled") : badge("plain", "off")) + " rate_limit_per_min=" + num(ui.rate_limit_per_min));
    return '<div class="card status-card"><header><h2>服务状态</h2><div class="spacer"></div><span class="card-note">/healthz · /admin/system · /admin/config</span></header>' +
      '<div class="body"><div class="status-grid">' + rows + "</div></div></div>";
  }
  function renderOverview() {
    var dg = M.diagnostics || {};
    if (!dg.available) {
      $("#page-overview").innerHTML = '<div class="page-head"><div><h1 id="h-overview">总览</h1></div></div>' +
        '<div class="card"><div class="body">' + unavail(dg, "总览数据不可用") + "</div></div>";
      return;
    }
    var d = dg.diagnostics || {}, cap = (M.pools && M.pools.capacity) || {}, sys = M.system || {}, mt = M.metrics || {}, cfg = M.config || {};
    var pool = pctPoolAvail();
    var errCount = d.attempt_usage_failed_records;
    var successRate = pct((typeof d.requests === "number" && typeof errCount === "number") ? d.requests - errCount : null, d.requests);
    var s = mt.series || {};
    var ups = dg.upstreams || [];
    var upsReady = ups.filter(function (u) { return !u.cooling; }).length;
    var upsCooling = ups.filter(function (u) { return u.cooling; }).length;
    var tasksRun = (typeof d.tasks_created === "number")
      ? d.tasks_created - ((d.tasks_terminal_completed || 0) + (d.tasks_terminal_failed || 0) + (d.tasks_terminal_incomplete || 0) + (d.tasks_terminal_cancelled || 0))
      : null;
    var budget = cap.daily_units_budget;
    var usedPct = (budget && typeof cap.used_today === "number") ? Math.min(100, Math.round(100 * cap.used_today / budget)) : 0;
    var budgetTone = !budget ? "" : usedPct > 80 ? "warn" : "ok";
    var health =
      healthItem(ups.length ? (upsCooling ? "warn" : "ok") : "", "上游", ups.length ? upsReady + " 台就绪 · " + upsCooling + " 冷却" : "未配置") +
      healthItem(pool.total ? (pool.ready === pool.total ? "ok" : "warn") : "", "凭据池", pool.total ? pool.ready + " / " + pool.total + " ready" : "无凭据池") +
      healthItem(budgetTone, "日预算", budget ? num(cap.used_today) + " / " + num(budget) + " units · 剩 " + num(cap.remaining) : "未启用") +
      healthItem(tasksRun ? "info" : "ok", "任务", num(tasksRun) + " running · inflight " + num(cap.inflight) + " / queue " + num(cap.queue_len));
    var kpis =
      kpi("requests", num(d.requests), "下游请求 · 进程内累计") +
      kpi("success rate", successRate, "分母 = requests · 失败 = usage 记录失败") +
      kpi("failovers", num(d.failovers), "换上游次数 · cooldown skips " + num(d.cooldown_skips)) +
      kpi("TTFT p95", ms2s(lastOf(s.ttft_p95_ms)), "p50 " + ms2s(lastOf(s.ttft_p50_ms)) + " · 最近 60s 桶") +
      kpi("total p95", ms2s(lastOf(s.total_p95_ms)), "p50 " + ms2s(lastOf(s.total_p50_ms)) + " · 最近 60s 桶");

    $("#page-overview").innerHTML =
      '<div class="page-head"><div><h1 id="h-overview">总览</h1><p class="sub">服务健康、KPI、趋势与最近事件 · 快照 ' + mono((M.meta || {}).generated_at || "—") + "</p></div></div>" +
      '<div class="health-strip">' + health + "</div>" +
      '<div class="kpi-grid">' + kpis + "</div>" +
      '<div class="overview-grid">' + trendCard(mt, s) + alertsCard() + "</div>" +
      eventsCard() +
      statusCard(dg, sys, cfg) +
      sourceLegend(["diagnostics", "pools", "metrics", "alerts", "logs", "sessions", "system", "config", "keys", "usageTrend", "events", "probe"]);
  }

  /* ---------------------------------------------------------------- 上游与池 */
  function renderUpstreams() {
    var dg = M.diagnostics || {};
    if (!dg.available) {
      $("#page-upstreams").innerHTML = '<div class="page-head"><h1 id="h-upstreams">上游与池</h1></div>' +
        '<div class="card"><div class="body">' + unavail(dg, "上游面不可用（/admin/diagnostics）") + "</div></div>";
      return;
    }
    var ups = dg.upstreams || [], pools = (M.pools && M.pools.upstreams) || [];
    var chainNodes = ups.map(function (u, i) {
      var cls = u.cooling ? "cooling" : (u.consecutive_failures >= 3 ? "dead" : "");
      return '<div class="node ' + cls + '"><div class="n">' + (i + 1) + ". " + esc(u.name) + "</div><div class='p'>" + esc(u.protocol) + "</div>" +
        '<div class="foot-sm">' + (u.cooling ? badge("cooling", "cooling " + Math.round(u.cooldown_remaining_seconds || 0) + "s") : badge("ok", "ready")) + "</div></div>";
    }).join('<span class="arrow" aria-hidden="true">→</span>');

    var healthRows = ups.map(function (u) {
      return "<tr><td class='mono'>" + u.index + "</td><td>" + esc(u.name) + "</td><td>" + mono(u.protocol) + "</td>" +
        "<td>" + (u.cooling ? badge("cooling", "cooling") : badge("ok", "ready")) + "</td>" +
        "<td class='mono'>" + num1(u.cooldown_remaining_seconds) + "s</td>" +
        "<td class='mono'>" + u.consecutive_failures + "</td><td class='mono'>" + u.ladder_step + "</td>" +
        "<td class='mono'>" + num(u.window_requests) + "</td><td class='mono'>" + num(u.window_failures) + "</td>" +
        "<td class='mono'>" + pct(u.window_failures, u.window_requests) + "</td>" +
        '<td><button class="ad-ctl sm" data-act="reset" data-up="' + esc(u.name) + '">复位冷却</button> ' +
        '<button class="ad-ctl sm unimpl" disabled title="未实现：无 POST /admin/upstreams/{name}/probe（设计 §6.1 #16）">测试</button></td></tr>';
    }).join("");
    if (!healthRows) healthRows = "<tr><td colspan='11' class='empty'>无上游行（/admin/diagnostics.upstreams 为空：未配置 upstreams 表）——failover 链与健康面无数据，不是渲染失败</td></tr>";

    var credRows = pools.map(function (u) {
      var head = "<tr><td colspan='8' class='group-head'>#" + u.index + " " + esc(u.name) + " · " + mono(u.protocol) + " · pool_size=" + u.pool_size + "</td></tr>";
      if (!u.credentials.length) {
        return head + "<tr><td class='dim' colspan='8'>无凭据池（该上游未配 credentials/api_key 池——非 claude_web 或无凭据，池面为空）</td></tr>";
      }
      return head + u.credentials.map(function (c) {
        return "<tr><td class='mono'>" + c.index + "</td>" +
          "<td>" + (!c.cooling && !c.busy ? badge("ok", "ready") : c.busy ? badge("plain", "busy") : badge("cooling", "cooling")) + "</td>" +
          "<td>" + (c.cooling ? badge("cooling", "cooling") : badge("plain", "no")) + "</td>" +
          "<td>" + (c.busy ? badge("warn", "busy") : badge("plain", "idle")) + "</td>" +
          "<td class='mono'>" + (c.cooldown_remaining_seconds ? c.cooldown_remaining_seconds + "s" : "—") + "</td>" +
          "<td class='mono'>" + (c.last_used ? hhmm(c.last_used) : "从未") + "</td>" +
          "<td class='mono dim'>— " + none("无逐凭据计数器（admin_api.go 已登记差异）") + "</td>" +
          "<td class='mono dim'>— " + none("无逐凭据计数器（admin_api.go 已登记差异）") + "</td></tr>";
      }).join("");
    }).join("");
    if (!credRows) credRows = "<tr><td colspan='8' class='empty'>池面为空（/admin/pool.upstreams 为空：没有带凭据池的上游）——非渲染失败</td></tr>";

    var cap = (M.pools && M.pools.capacity) || {};
    var usedPct = (cap.daily_units_budget && typeof cap.used_today === "number") ? Math.min(100, Math.round(100 * cap.used_today / cap.daily_units_budget)) : 0;

    $("#page-upstreams").innerHTML =
      '<div class="page-head"><h1 id="h-upstreams">上游与池</h1><span class="sub">有序 failover 链 · 逐上游健康 · 逐凭据池 · 复位冷却</span></div>' +

      '<div class="card"><header><h2>failover 链（配置顺序 = 尝试顺序）</h2>' + '<div class="spacer"></div><span class="card-note">冷却中的上游被跳过；全部冷却时退化为全列表重试</span></header>' +
        '<div class="body"><div class="chain">' + (chainNodes || "<span class='dim'>未配置上游</span>") + "</div></div></div>" +

      '<div class="card"><header><h2>逐上游健康</h2>' + '<div class="spacer"></div><span class="card-note">window_requests/failures = 失败率窗口</span></header>' +
        '<div class="body flush"><div class="table-wrap"><table class="admin"><thead><tr><th>#</th><th>name</th><th>protocol</th><th>状态</th><th>cooldown 剩余</th><th>连续失败</th><th>阶梯档</th><th>窗口请求</th><th>窗口失败</th><th>失败率</th><th>动作</th></tr></thead><tbody>' + healthRows + "</tbody></table></div></div></div>" +

      '<div class="grid c2-1">' +
        '<div class="card"><header><h2>逐凭据池</h2>' + '<div class="spacer"></div><span class="card-note">只回索引与状态（零凭据字节）</span></header>' +
          '<div class="body flush"><div class="table-wrap"><table class="admin"><thead><tr><th>idx</th><th>就绪</th><th>冷却</th><th>busy</th><th>冷却剩余</th><th>最近使用</th><th>今日请求</th><th>今日 units</th></tr></thead><tbody>' + credRows + "</tbody></table></div></div>" +
          '<div class="body faint foot">' + esc((M.degradeNote || {}).poolByCredential || "") + "</div></div>" +

        '<div class="card"><header><h2>生成面容量</h2>' + "</header>" +
          '<div class="body">' + ((M.pools && M.pools.available === false) ? unavail(M.pools, "池面不可用（/admin/pool）") :
            '<div class="kv-list">' +
              "<li><span>日预算</span><b>" + num(cap.daily_units_budget) + " units</b></li>" +
              "<li><span>今日已用</span><b>" + num(cap.used_today) + " units（" + usedPct + "%）</b></li>" +
              "<li><span>剩余</span><b>" + num(cap.remaining) + " units</b></li>" +
              "<li><span>inflight / 上限</span><b>" + num(cap.inflight) + " / " + num(cap.max_inflight) + "</b></li>" +
              "<li><span>队列深度</span><b>" + num(cap.queue_len) + "</b></li>" +
            "</ul></div>" +
            '<div class="gauge mt10' + (usedPct > 80 ? " warn" : "") + '" data-pct="' + usedPct + '"><i></i></div>' +
            '<div class="faint foot mt8">public.enabled=' + String(((M.config || {}).public || {}).enabled) + "：闸与预算在 public 关时为零值（status 面照常）。</div>") +
          "</div></div>" +
      "</div>" +
      sourceLegend(["upstreams", "pools", "diagnostics"]);
  }

  /* ---------------------------------------------------------------- 账号与代理 */
  /* 上游 × 凭据池 × 代理（GET /admin/accounts · /admin/proxies）。
   * 零凭据字节：池面只有 #index/状态/last_used，代理面只有 scheme/host/port/auth/display。
   * 动作两条都真发：安全下线 = POST …/cooldown（进程内冷却，零配置写）；探针 = POST …/probe。 */

  function localTs(ms) {
    if (!ms) return "从未";
    try { return new Date(ms).toLocaleString(); } catch (e) { return hhmm(ms); }
  }
  function clip(s, n) {
    s = String(s === null || s === undefined ? "" : s);
    return s.length > n ? s.slice(0, n - 1) + "…" : s;
  }
  function credChip(c) {
    var cls = c.cooling ? "cooling" : (c.busy ? "busy" : "ready");
    var label = c.cooling ? ("冷却 " + dur(c.cooldown_remaining_seconds)) : (c.busy ? "使用中" : "就绪");
    var title = "凭据 #" + c.index + " · " + label + " · 最近使用 " + localTs(c.last_used);
    return '<span class="cred-chip ' + cls + '" data-cred="' + esc(c.index) + '" title="' + esc(title) + '">#' + esc(c.index) + " " + esc(label) + "</span>";
  }
  function unconfigured() { return '<span class="dim">未配置</span>'; }

  // 探针结果文案：ok = `200 · 123ms · auth verified`；失败按 error_class 词表落人话（绝不回响应体）。
  function probeText(p) {
    if (!p) return "";
    if (p.ok) return num(p.status) + " · " + (p.latency_ms === null || p.latency_ms === undefined ? "—" : p.latency_ms + "ms") + " · auth " + (p.auth || "unknown");
    var cls = p.error_class || "";
    if (cls === "timeout" || cls === "network" || cls === "tls") return cls + " · 网络不可达";
    if (cls === "http_4xx" || cls === "http_5xx") return cls + " · 上游 HTTP " + num(p.status);
    return (cls || "未达") + " · 未收到响应";
  }
  function probeInline(name) {
    var p = state.probes[name];
    if (!p) return "";
    return '<span class="probe-result" data-probe="' + esc(name) + '"><span class="badge ' + (p.ok ? "ok" : (p.error_class ? "err" : "plain")) + '">' + esc(probeText(p)) + "</span></span>";
  }
  function cdSelect(uid) {
    return '<select class="ad-ctl sm cd-sel" id="' + esc(uid) + '" aria-label="冷却时长">' +
      '<option value="3600">1h</option><option value="21600">6h</option><option value="86400">24h</option></select>';
  }
  // exportBar 导出工具条：每类一对（CSV / JSONL）；按钮带 type/format/scope/day，动作 = handleAction("export")。
  function exportBar(groups) {
    return '<div class="export-bar">' + (groups || []).map(function (g) {
      return '<span class="export-group"><span class="export-label">' + esc(g.label) + "</span>" + ["csv", "jsonl"].map(function (f) {
        var attrs = 'data-act="export" data-type="' + esc(g.type) + '" data-format="' + f + '"';
        if (g.scope) attrs += ' data-scope="' + esc(g.scope) + '"';
        if (g.day) attrs += ' data-day="' + esc(g.day) + '"';
        return '<button class="ad-ctl sm" ' + attrs + ">导出 " + f.toUpperCase() + "</button>";
      }).join("") + "</span>";
    }).join("") + "</div>";
  }

  function renderAccounts() {
    var acct = M.accounts || {}, prox = M.proxies || {};
    var head = '<div class="page-head"><div><h1 id="h-accounts">账号与代理</h1><p class="sub">上游 × 凭据池 × 代理；只回索引与状态，零凭据字节。</p></div>' +
      exportBar([{ label: "账号", type: "accounts" }, { label: "代理", type: "proxies" }]) + "</div>";
    if (acct.available === false) {
      $("#page-accounts").innerHTML = head + '<div class="card"><div class="body">' + unavail(acct, "账号面不可用（/admin/accounts）") + "</div></div>" +
        sourceLegend(["accounts", "proxies", "pools", "cooldown"]);
      return;
    }
    var ups = acct.upstreams || [];
    var creds = 0, cooling = 0, proxyN = 0;
    ups.forEach(function (u) {
      if (u.proxy && u.proxy.configured) proxyN++;
      (u.credentials || []).forEach(function (c) { creds++; if (c.cooling) cooling++; });
    });
    var cards = kpi("上游", num(ups.length), "配置序 = failover 链序（与 /admin/pool 同源）") +
      kpi("凭据", num(creds), "pool_size 合计（只回条目数与状态，零凭据字节）") +
      kpi("已配代理", num(proxyN), "proxy.configured = true 的上游数") +
      kpi("冷却中", num(cooling), "冷却窗内的凭据数（可用「冷却」动作下线）");

    var rows = ups.map(function (u) {
      var chips = (u.credentials || []).map(credChip).join("");
      if (!chips) chips = '<span class="cred-chip none-chip">无凭据</span>';
      var acts = (u.credentials || []).map(function (c) {
        var uid = "cd-" + u.index + "-" + c.index;
        return '<span class="cd-group" data-cred="' + esc(c.index) + '">' + mono("#" + c.index) + cdSelect(uid) +
          '<button class="ad-ctl sm" data-act="cooldown" data-up="' + esc(u.name) + '" data-idx="' + esc(c.index) + '" data-cd="' + esc(uid) + '">冷却</button></span>';
      }).join("");
      if (!acts) acts = none("该上游池为空：无可冷却凭据");
      var base = u.base_url ? '<span title="' + esc(u.base_url) + '">' + esc(clip(u.base_url, 46)) + "</span>" : unconfigured();
      var proxy = (u.proxy && u.proxy.configured)
        ? mono(u.proxy.display) + (u.proxy.auth ? ' <span class="badge plain">认证</span>' : "")
        : unconfigured();
      return '<tr class="acct-row" data-up="' + esc(u.name) + '">' +
        "<td class='mono'>" + num(u.index) + "</td><td>" + esc(u.name) + "</td><td>" + mono(u.protocol) + "</td>" +
        "<td class='mono base-url'>" + base + "</td><td>" + proxy + "</td>" +
        "<td class='mono'>" + (u.tls_fingerprint ? esc(u.tls_fingerprint) : unconfigured()) + "</td>" +
        "<td class='mono'>" + num(u.pool_size) + "</td>" +
        '<td><div class="cred-chips">' + chips + "</div></td>" +
        '<td class="act-cell">' + acts +
          '<button class="ad-ctl sm" data-act="probe" data-up="' + esc(u.name) + '">探针</button>' + probeInline(u.name) + "</td></tr>";
    }).join("");
    if (!rows) rows = "<tr><td colspan='9' class='empty'>无上游（/admin/accounts.upstreams 为空：未配置 upstreams 表）——上游 × 凭据 × 代理三面都无数据，不是渲染失败</td></tr>";

    var prows = (prox.proxies || []).map(function (p) {
      return '<tr class="proxy-row" data-up="' + esc(p.upstream) + '">' +
        "<td class='mono'>" + num(p.index) + "</td><td>" + esc(p.upstream) + "</td>" +
        "<td>" + (p.configured ? badge("ok", "on") : badge("plain", "未配置")) + "</td>" +
        "<td class='mono'>" + (p.display ? esc(p.display) : unconfigured()) + "</td>" +
        "<td class='mono'>" + orNone(p.scheme) + "</td><td class='mono'>" + orNone(p.host) + "</td><td class='mono'>" + orNone(p.port) + "</td>" +
        "<td>" + (p.auth ? badge("warn", "认证") : badge("plain", "无")) + "</td>" +
        "<td class='mono'>" + (p.tls_fingerprint ? esc(p.tls_fingerprint) : unconfigured()) + "</td></tr>";
    }).join("");
    var proxyCard = (prox.available === false)
      ? '<div class="card"><header><h2>代理（逐上游，含未配置）</h2></header><div class="body">' + unavail(prox, "代理面不可用（/admin/proxies）") + "</div></div>"
      : '<div class="card table-card"><header><h2>代理（逐上游，含未配置）</h2><div class="spacer"></div><span class="card-note">GET /admin/proxies · display = scheme://host[:port]（userinfo 只化为 auth 布尔）</span></header>' +
        '<div class="body flush"><div class="table-wrap"><table class="admin proxies-table"><thead><tr><th>#</th><th>上游</th><th>配置</th><th>display</th><th>scheme</th><th>host</th><th>port</th><th>认证</th><th>tls_fingerprint</th></tr></thead>' +
        "<tbody>" + (prows || "<tr><td colspan='9' class='empty'>代理面为空（该服务没有逐上游代理行）——不是渲染失败</td></tr>") + "</tbody></table></div></div></div>";

    $("#page-accounts").innerHTML = head +
      '<div class="kpi-grid">' + cards + "</div>" +
      '<div class="card table-card"><header><h2>账号与凭据池</h2><div class="spacer"></div><span class="card-note">GET /admin/accounts · 冷却动作 = POST /admin/accounts/{upstream}/{index}/cooldown（零配置写）</span></header>' +
        '<div class="body flush"><div class="table-wrap"><table class="admin accounts-table"><thead><tr><th>#</th><th>上游</th><th>协议</th><th>Base URL</th><th>代理</th><th>TLS</th><th>池</th><th>凭据状态</th><th>操作</th></tr></thead>' +
        "<tbody>" + rows + "</tbody></table></div></div>" +
        '<div class="body faint foot">Base URL 已去 userinfo / query / fragment；凭据只回 #下标与状态（就绪 / 冷却 / 使用中），零 api_key / cookie / 认证头字节。探针 = POST /admin/upstreams/{name}/probe（1 发只读 GET，不占号池）。</div></div>' +
      proxyCard +
      (!ups.length ? '<div class="card"><div class="body">' + unavail({ reason: "无上游：/admin/accounts.upstreams 为空（未配置 upstreams 表）——账号面没有可下线的凭据" }, "无上游") + "</div></div>" : "") +
      sourceLegend(["accounts", "proxies", "pools", "cooldown", "probe", "exportFile"]);
  }

  /* ---------------------------------------------------------------- 服务商 */
  /* 协议族 × 配置 × 最近探针（GET /admin/providers）：readiness 由配置命中 + 探针结果派生，
   * 不是承诺。探针按钮真发 POST；成功后重取 providers 让 readiness 跟着变。 */
  var READINESS = {
    not_configured: ["plain", "未配置"],
    configured: ["cool", "已配置"],
    ready: ["ok", "就绪"],
    degraded: ["warn", "降级"]
  };
  function readinessBadge(v) {
    var m = READINESS[v];
    if (!m) return '<span class="badge plain readiness" data-ready="' + esc(v) + '">' + esc(v || "未提供") + "</span>";
    return '<span class="badge ' + m[0] + ' readiness" data-ready="' + esc(v) + '">' + esc(m[1]) + "</span>";
  }
  function providerRow(k, v) { return '<div class="provider-row"><span class="k">' + esc(k) + "</span><span>" + v + "</span></div>"; }

  function renderProviders() {
    var pv = M.providers || {};
    if (pv.available === false) {
      $("#page-providers").innerHTML = '<div class="page-head"><h1 id="h-providers">服务商</h1></div>' +
        '<div class="card"><div class="body">' + unavail(pv, "供应商目录不可用（/admin/providers）") + "</div></div>" +
        sourceLegend(["providers", "probe"]);
      return;
    }
    var list = pv.providers || [];
    var configuredN = list.filter(function (p) { return p.configured; }).length;
    var cards = list.map(function (p) {
      var protos = (p.protocols || []).map(function (x) { return '<span class="cap-chip proto">' + esc(x) + "</span>"; }).join("");
      var caps = (p.capabilities || []).map(function (x) { return '<span class="cap-chip">' + esc(x) + "</span>"; }).join("");
      var ups = (p.upstreams || []).map(function (n) {
        return '<span class="up-chip">' + mono(n) + '<button class="ad-ctl sm" data-act="probe" data-up="' + esc(n) + '">探针</button>' + probeInline(n) + "</span>";
      }).join("");
      return '<div class="provider-card" data-provider="' + esc(p.id) + '">' +
        "<header><h3>" + esc(p.label) + "</h3>" + mono(p.id) + '<div class="spacer"></div>' + readinessBadge(p.readiness) + "</header>" +
        '<div class="body">' +
          providerRow("协议", protos || '<span class="dim">—</span>') +
          providerRow("上游", ups || '<span class="dim">未配置</span>') +
          providerRow("能力", caps || none("该条目未声明能力")) +
          providerRow("文档", p.docs ? '<a class="docs-link" href="' + esc(p.docs) + '" target="_blank" rel="noopener">' + esc(p.docs) + "</a>" : none("目录条目未带文档指针")) +
        "</div></div>";
    }).join("");

    var emptyCards = "";
    if (!list.length) emptyCards = '<div class="card"><div class="body">' + unavail({ reason: "GET /admin/providers 回 providers: []（静态目录为空）" }, "服务商目录为空") + "</div></div>";
    else if (!configuredN) emptyCards = '<div class="card"><div class="body">' + unavail({ reason: "目录有 " + list.length + " 条，但没有一条命中配置的上游（upstreams 协议族全未配置）——探针无目标" }, "未配置任何上游") + "</div></div>";

    $("#page-providers").innerHTML =
      '<div class="page-head"><div><h1 id="h-providers">服务商</h1><p class="sub">协议族 × 配置 × 最近探针；readiness 不是承诺，是当前证据。</p></div>' +
      '<span class="dim foot-sm">' + num(configuredN) + " / " + num(list.length) + " 条已配置</span></div>" +
      emptyCards +
      (cards ? '<div class="provider-grid">' + cards + "</div>" : "") +
      '<div class="card"><div class="body faint foot">readiness 四值：未配置（无命中上游）· 已配置（有命中但从未探针）· 就绪（该组上游最近一次探针 ok）· 降级（可达但不可用 / 认证被拒）。探针结果只存进程内内存，重启清零；能力词表是静态声明（协议常量 + 转换表覆盖），不是运行时探测结果。</div></div>' +
      sourceLegend(["providers", "probe", "upstreams"]);
  }

  /* ---------------------------------------------------------------- 请求 */
  function trafficRows() {
    var f = state.traffic;
    var rows = (M.requests && M.requests.rows) || [];
    var now = (M.meta && M.meta.generated_at) ? Date.parse(M.meta.generated_at) : Date.now();
    var windowMs = { "5m": 5 * 60000, "1h": 3600000, "24h": 86400000 }[f.window] || 0;
    return rows.filter(function (r) {
      if (windowMs && r.time && now - Date.parse(r.time) > windowMs) return false;
      if (f.entry && r.entry !== f.entry) return false;
      if (f.model && r.model !== f.model) return false;
      if (f.status && String(r.status) !== f.status) return false;
      if (f.upstream && r.upstream !== f.upstream && (r.upstreams || []).indexOf(f.upstream) === -1) return false;
      if (f.stream && String(r.stream) !== f.stream) return false;
      if (f.failed === "yes" && !r.failed) return false;
      if (f.failed === "no" && r.failed) return false;
      if (f.q && r.request_id.indexOf(f.q) === -1) return false;
      return true;
    });
  }
  function renderTraffic() {
    var reqs = M.requests || {};
    if (!reqs.available) {
      $("#page-traffic").innerHTML = '<div class="page-head"><h1 id="h-traffic">请求</h1></div>' +
        '<div class="card"><div class="body">' + unavail(reqs, "请求面不可用（/admin/requests）") + "</div></div>";
      return;
    }
    var rows = trafficRows();
    var total = (reqs.rows || []).length;
    var body = rows.map(function (r) {
      return '<tr class="clickable" data-act="req" data-id="' + esc(r.request_id) + '">' +
        "<td class='mono'>" + esc(hhmm(r.time)) + "</td><td class='mono'>" + esc(r.request_id) + "</td><td>" + esc(r.entry) + "</td>" +
        "<td>" + mono(r.model) + "</td><td>" + (r.stream ? badge("plain", "stream") : badge("plain", "json")) + "</td>" +
        "<td>" + statusBadge(r.status) + "</td><td class='mono'>" + num(r.attempts) + "</td><td class='mono'>" + num(r.failovers) + "</td>" +
        "<td>" + esc(r.upstream || "—") + "</td><td class='mono'>" + (r.credential_index === null || r.credential_index === undefined ? "—" : r.credential_index) + "</td>" +
        "<td class='mono'>" + (r.ttft_ms ? r.ttft_ms + "ms" : "—") + "</td><td class='mono'>" + (r.total_ms ? r.total_ms + "ms" : "—") + "</td>" +
        "<td class='mono'>" + num(r.in_tokens) + "</td><td class='mono'>" + num(r.out_tokens) + "</td><td>" + outcomeBadge(r.outcome) + "</td>" +
        "<td class='mono'>" + esc(r.sid_prefix || "—") + "</td><td class='mono'>" + esc(r.key_id || "—") + "</td><td class='mono'>" + esc(r.task || "—") + "</td></tr>";
    }).join("");
    if (!rows.length) body = "<tr><td class='empty' colspan='18'>本窗口无请求行（ring 是进程内有界内存：重启清零；服务端筛选与翻页见 GET /admin/requests）</td></tr>";

    var entries = ["chat", "messages", "responses", "gemini"];
    var ups = ((M.diagnostics || {}).upstreams || []).map(function (u) { return u.name; });
    var models = [];
    (reqs.rows || []).forEach(function (r) { if (models.indexOf(r.model) === -1) models.push(r.model); });

    $("#page-traffic").innerHTML =
      '<div class="page-head"><h1 id="h-traffic">请求</h1><span class="sub">实时请求表 + 筛选 + 详情抽屉（attempt 时间线）· 本页 ' + num(total) + " 行（ring 窗口，limit=200）</span></div>" +
      '<div class="card"><header><h2>请求表</h2>' + '<div class="spacer"></div>' + exportBar([{ label: "请求台账", type: "requests" }]) + '<span class="card-note">GET /admin/requests?limit=&since=&entry=&model=&upstream=&status=&failed=&stream=&request_id=</span></header>' +
        '<div class="filters">' +
          '<label>时间窗 <select id="f-window"><option value="">全部</option><option value="5m">近 5 分钟</option><option value="1h">近 1 小时</option><option value="24h">近 24 小时</option></select></label>' +
          '<label>入口 <select id="f-entry"><option value="">全部</option>' + entries.map(function (e) { return '<option value="' + e + '">' + e + "</option>"; }).join("") + "</select></label>" +
          '<label>模型 <select id="f-model"><option value="">全部</option>' + models.map(function (m) { return '<option value="' + esc(m) + '">' + esc(m) + "</option>"; }).join("") + "</select></label>" +
          '<label>上游 <select id="f-up"><option value="">全部</option>' + ups.map(function (e) { return '<option value="' + esc(e) + '">' + esc(e) + "</option>"; }).join("") + "</select></label>" +
          '<label>状态 <select id="f-status"><option value="">全部</option><option>200</option><option>400</option><option>403</option><option>429</option><option>500</option></select></label>' +
          '<label>failed <select id="f-failed"><option value="">全部</option><option value="yes">仅失败</option><option value="no">仅成功</option></select></label>' +
          '<label>流 <select id="f-stream"><option value="">全部</option><option value="true">stream</option><option value="false">非流式</option></select></label>' +
          '<span class="sep"></span><label>request_id <input type="search" id="f-q" placeholder="req_…" size="12"></label>' +
          '<button class="ad-ctl sm" id="f-reset">重置</button>' +
          '<div class="spacer" ></div><span class="dim" id="f-count">' + rows.length + " / " + total + " 行</span>" +
        "</div>" +
        '<div class="body flush"><div class="table-wrap"><table class="admin dense"><thead><tr><th>time</th><th>request_id</th><th>entry</th><th>model</th><th>stream</th><th>status</th><th>attempts</th><th>failovers</th><th>upstream</th><th>cred</th><th>ttft</th><th>total</th><th>in</th><th>out</th><th>outcome</th><th>sid</th><th>key</th><th>task</th></tr></thead><tbody>' + body + "</tbody></table></div></div>" +
        '<div class="body faint foot">点击任意行 → 右侧详情抽屉（GET /admin/requests/{id}）：attempt 时间线 + 脱敏错误摘要。筛选项随下一次刷新（≤10s）带给服务端；改筛选立即重取。</div>' +
      "</div>" +
      sourceLegend(["requests", "requestDetail", "upstreams", "diagnostics"]);
    var q = function (id) { return document.getElementById(id); };
    q("f-window").value = state.traffic.window;
    q("f-entry").value = state.traffic.entry; q("f-up").value = state.traffic.upstream;
    q("f-model").value = state.traffic.model; q("f-failed").value = state.traffic.failed;
    q("f-status").value = state.traffic.status; q("f-stream").value = state.traffic.stream; q("f-q").value = state.traffic.q;
    var refetch = function () { refreshAll({ local: true }); };
    q("f-window").onchange = function () { state.traffic.window = this.value; refetch(); };
    q("f-entry").onchange = function () { state.traffic.entry = this.value; refetch(); };
    q("f-model").onchange = function () { state.traffic.model = this.value; refetch(); };
    q("f-up").onchange = function () { state.traffic.upstream = this.value; refetch(); };
    q("f-failed").onchange = function () { state.traffic.failed = this.value; refetch(); };
    q("f-status").onchange = function () { state.traffic.status = this.value; refetch(); };
    q("f-stream").onchange = function () { state.traffic.stream = this.value; refetch(); };
    q("f-q").oninput = function () { state.traffic.q = this.value; renderTraffic(); debounceRefetch(); };
    q("f-reset").onclick = function () { state.traffic = { window: "", entry: "", model: "", status: "", upstream: "", stream: "", failed: "", q: "" }; refetch(); };
  }

  /* 请求详情抽屉：按需 GET /admin/requests/{id}（A2AAdmin.detail 写 LIVE.requestDetail）。 */
  function openRequestDrawer(id) {
    $("#drawer-title").textContent = "请求详情 " + id;
    $("#drawer-body").innerHTML = '<div class="faint pad-sm">加载中…（GET /admin/requests/' + esc(id) + "）</div>";
    openDrawer();
    if (!window.A2AAdmin) return;
    window.A2AAdmin.detail(id).then(function (res) {
      if (!res.ok) {
        if (res.reason === "unauthorized") { lockUI("密钥无效或已失效"); return; }
        $("#drawer-body").innerHTML = '<div class="card"><div class="body">' +
          unavail({ reason: (res.error || {}).message || res.reason || "未知错误" }, "请求详情不可用") + "</div></div>";
        return;
      }
      drawRequestDrawer(id);
    });
  }
  function drawRequestDrawer(id) {
    var r = M.requestDetail || {};
    var tl = (r.attempts || []).map(function (a, i) {
      var cls = a.failed ? "fail" : "ok";
      var err = a.error ? '<div class="dim xs mt4">错误体（脱敏分类）：' + mono(a.error.type || "—") + " · " + esc(a.error.class || "—") + "</div>" : "";
      var span = (a.start || a.end) ? mono(hhmm(a.start)) + " → " + mono(hhmm(a.end))
        : none("attempt 记录只有耗时（ttft_ms/total_ms），无起止时刻");
      return '<li class="' + cls + '"><div class="n">' + (i + 1) + "</div><div>" +
        "<div><b>" + esc(a.upstream) + "</b> " + mono(a.protocol) + " · cred " + (a.credential_index === null || a.credential_index === undefined ? "—" : "#" + a.credential_index) + " · " + statusBadge(a.status) + " · " + badge(a.failed ? "err" : "ok", a.end_reason || "—") + "</div>" +
        '<div class="dim xs">' + span + " · ttft " + (a.ttft_ms ? a.ttft_ms + "ms" : "—") + " · total " + (a.total_ms ? a.total_ms + "ms" : "—") + " · in/out " + num(a.in_tokens) + "/" + num(a.out_tokens) + " · delivered=" + String(a.delivered) + (a.disconnected ? " · disconnected" : "") + "</div>" + err +
        "</div></li>";
    }).join("");
    if (!tl) tl = "<li class='dim'>无 attempt 记录</li>";
    $("#drawer-title").textContent = "请求详情 " + id;
    $("#drawer-body").innerHTML =
      '<div class="card"><header><h2>请求面</h2>' + "</header><div class='body'><div class='meta-grid'>" +
        "<div class='row'><span class='k'>request_id</span><span>" + mono(r.request_id) + "</span></div>" +
        "<div class='row'><span class='k'>entry / stream</span><span>" + esc(r.entry) + " / " + String(r.stream) + "</span></div>" +
        "<div class='row'><span class='k'>model（请求 → 上游）</span><span>" + mono(r.model_requested) + " → " + (r.model_upstream ? mono(r.model_upstream) : none("ring 不存别名映射（见「配置与系统」的模型映射）")) + "</span></div>" +
        "<div class='row'><span class='k'>下游状态</span><span>" + statusBadge(r.downstream_status) + " delivered=" + String(r.delivered) + "</span></div>" +
        "<div class='row'><span class='k'>提交 → 收尾</span><span>" + mono(hhmm(r.submitted_at)) + " → " + orNone(r.finished_at, mono) + "</span></div>" +
        "<div class='row'><span class='k'>sid 前缀 / key_id</span><span>" + orNone(r.sid_prefix, mono) + " / " + orNone(r.key_id, mono) + "</span></div>" +
        "<div class='row'><span class='k'>ip hash / ua hash</span><span>" + orNone(r.client_ip_hash, mono) + " / " + orNone(r.ua_hash, mono) + "</span></div>" +
        "<div class='row'><span class='k'>turn</span><span>" + (r.turn ? mono(r.turn.id) : none("ring 行不关联 turn id（store.TurnsByRequest 才有）")) + "</span></div>" +
      "</div></div></div>" +
      '<div class="card"><header><h2>attempt 时间线</h2>' + "</header><div class=\"body\"><ul class=\"timeline\">" + tl + "</ul></div></div>" +
      '<div class="card"><header><h2>关联面</h2></header><div class="body"><div class="kv-list">' +
        "<li><span>audit_log（kind=request）</span><span>" + mono("见「审计与告警」页（GET /admin/audit?kind=request）") + "</span></li>" +
        "<li><span>turns 表（Store.TurnsByRequest）</span><span>— " + none("无 HTTP 面（store 方法已有，缺只读投影）") + "</span></li>" +
        "<li><span>admin ring seq</span><span>— " + none("ring 行不经 request_id 关联（按时间窗读）") + "</span></li>" +
      "</ul></div><div class='faint foot mt8'>错误面只回枚举：end_reason + 归因分类（type/class），上游错误原文/头面/URL 查询串一律不回（服务端脱敏投影）。</div></div></div>";
    openDrawer();
  }

  /* ---------------------------------------------------------------- 会话 */
  function renderSessions() {
    var ss = M.sessions || {};
    if (!ss.available) {
      $("#page-sessions").innerHTML = '<div class="page-head"><h1 id="h-sessions">会话</h1></div>' +
        '<div class="card"><div class="body">' + unavail(ss, "会话面不可用（/admin/sessions）") + "</div></div>";
      return;
    }
    var f = state.sessions;
    var all = ss.sessions || [];
    var filtered = all.filter(function (s) {
      if (f.q && ((s.sid_prefix || "") + " " + (s.key_id || "")).indexOf(f.q) === -1) return false;
      var expired = s.expires_at < Date.now();
      if (f.status === "active" && (s.revoked || expired)) return false;
      if (f.status === "revoked" && !s.revoked) return false;
      if (f.status === "expired" && !expired) return false;
      return true;
    });
    var rows = filtered.map(function (s) {
      var st = s.revoked ? badge("err", "revoked") : (s.expires_at < Date.now() ? badge("warn", "expired") : badge("ok", "active"));
      return "<tr><td class='mono'>" + esc(s.sid_prefix) + "</td><td class='mono'>" + esc(s.key_id) + "</td>" +
        "<td class='mono'>" + esc(hhmm(s.created_at)) + "</td><td class='mono'>" + esc(hhmm(s.last_seen_at)) + "</td>" +
        "<td class='mono'>" + esc(hhmm(s.expires_at)) + "</td><td class='mono'>" + esc(hhmm(s.abs_expires_at)) + "</td>" +
        "<td>" + st + "</td><td class='mono'>" + num(s.requests_today) + "</td><td class='mono'>" + num(s.cost_units_today) + "</td>" +
        '<td><button class="ad-ctl sm" data-act="sess" data-sid="' + esc(s.sid_prefix) + '">详情</button> ' +
        '<button class="ad-ctl sm danger" data-act="revoke" data-sid="' + esc(s.sid_prefix) + '"' + (s.revoked ? " disabled" : "") + ">吊销</button></td></tr>";
    }).join("");
    if (!rows) rows = "<tr><td colspan='10' class='empty'>" + esc(all.length ? "无匹配会话行（放宽筛选）" : "无会话行（store 未启用时无持久面；已吊销/过期行由服务端过滤）") + "</td></tr>";

    var ipScope = (M.usageScopes || {}).ip || {};
    var ips = (ipScope.rows || []).map(function (r) {
      return "<tr><td class='mono'>" + esc(r.scope) + "</td><td class='mono'>" + num(r.requests) + "</td><td class='mono'>" + num(r.cost_units) + "</td></tr>";
    }).join("");
    if (!ips) ips = "<tr><td colspan='3' class='empty'>" + esc(ipScope.available === false ? (ipScope.reason || "未提供") : "今日 ip 作用域无台账行") + "</td></tr>";

    $("#page-sessions").innerHTML =
      '<div class="page-head"><h1 id="h-sessions">会话</h1><span class="sub">会话清单 / 详情 / 吊销 · access key · per-IP 用量</span></div>' +
      '<div class="card table-card"><header><h2>会话清单</h2><div class="spacer"></div><span class="card-note">sid 只回 8 字符前缀；排序 last_seen DESC</span></header>' +
        '<div class="filters">' +
          '<label>搜索 <input type="search" id="s-q" placeholder="sid / key_id" size="16"></label>' +
          '<label>状态 <select id="s-status"><option value="">全部</option><option value="active">active</option><option value="revoked">revoked</option><option value="expired">expired</option></select></label>' +
          '<button class="ad-ctl sm" id="s-clear">重置</button>' +
          '<div class="spacer"></div><span class="filter-count" id="s-count">' + filtered.length + " / " + all.length + " 行</span>" +
        "</div>" +
        '<div class="body flush"><div class="table-wrap"><table class="admin"><thead><tr><th>sid_prefix</th><th>key_id</th><th>created</th><th>last_seen</th><th>expires(滑动)</th><th>绝对到期</th><th>状态</th><th>今日请求</th><th>今日 units</th><th>动作</th></tr></thead><tbody>' + rows + "</tbody></table></div></div>" +
        '<div class="body row-flex"><span class="dim">游标翻页：</span>' + mono(ss.next_cursor ? "next_cursor=" + ss.next_cursor : "next_cursor=（末页）") +
        '<button class="ad-ctl sm" data-act="cursor"' + (ss.next_cursor ? "" : " disabled") + ">加载下一页</button></div></div>" +

      '<div class="grid c2">' +
        '<div class="card"><header><h2>access keys</h2></header>' +
          '<div class="body">' + unavail({ reason: "无 GET /admin/keys 端点（access key 视图未实现；key_id 记账面已在 usage scope=key）" }, "未提供") + "</div>" +
          '<div class="body faint foot">access key 原文只在 0600 文件/进程环境；即便实现本面也只回 sha8 与计数（零凭据字节）。</div></div>' +
        '<div class="card table-card"><header><h2>per-IP 用量（今日）</h2></header>' +
          '<div class="body flush"><div class="table-wrap"><table class="admin"><thead><tr><th>ip scope（已哈希）</th><th>requests</th><th>units</th></tr></thead><tbody>' + ips + "</tbody></table></div></div>" +
          '<div class="body faint foot">IP 只以哈希形态出现（数据源 = usage_counters 的 ip: scope）。</div></div>' +
      "</div>" +
      sourceLegend(["sessions", "keys", "usage"]);

    var q = function (id) { return document.getElementById(id); };
    q("s-q").value = f.q;
    q("s-status").value = f.status;
    q("s-q").oninput = function () {
      state.sessions.q = this.value;
      renderSessions();
      var el = document.getElementById("s-q");
      el.focus();
      el.setSelectionRange(el.value.length, el.value.length);
    };
    q("s-status").onchange = function () { state.sessions.status = this.value; renderSessions(); };
    q("s-clear").onclick = function () { state.sessions = { q: "", status: "" }; renderSessions(); };
  }

  function openSessionDrawer(sid) {
    var s = ((M.sessions || {}).sessions || []).filter(function (x) { return x.sid_prefix === sid; })[0];
    if (!s) return;
    $("#drawer-title").textContent = "会话详情 " + sid;
    $("#drawer-body").innerHTML =
      '<div class="card"><header><h2>会话面</h2>' + "</header><div class='body'><div class='meta-grid'>" +
        "<div class='row'><span class='k'>sid_prefix</span><span>" + mono(s.sid_prefix) + "</span></div>" +
        "<div class='row'><span class='k'>key_id</span><span>" + mono(s.key_id) + "</span></div>" +
        "<div class='row'><span class='k'>created_at</span><span>" + mono(hhmm(s.created_at)) + "</span></div>" +
        "<div class='row'><span class='k'>last_seen_at</span><span>" + mono(hhmm(s.last_seen_at)) + "</span></div>" +
        "<div class='row'><span class='k'>expires_at（滑动）</span><span>" + mono(hhmm(s.expires_at)) + "</span></div>" +
        "<div class='row'><span class='k'>abs_expires_at</span><span>" + mono(hhmm(s.abs_expires_at)) + "</span></div>" +
        "<div class='row'><span class='k'>revoked</span><span>" + String(s.revoked) + "</span></div>" +
        "<div class='row'><span class='k'>今日 requests / units</span><span>" + num(s.requests_today) + " / " + num(s.cost_units_today) + "</span></div>" +
      "</div></div></div>" +
      '<div class="card"><header><h2>会话内容面（conversations / messages / turns）</h2></header>' +
        '<div class="body">' + unavail({ reason: "无 /admin/sessions/{sid}/content 端点（store 侧方法 ConversationsBySession / MessageGet / TurnsByRequest 已在，缺只读 HTTP 面与脱敏投影）" }, "未提供") + "</div></div>" +
      '<div class="card"><header><h2>吊销</h2>' + "</header><div class=\"body\">" +
        '<button class="ad-ctl danger" data-act="revoke" data-sid="' + esc(s.sid_prefix) + '">吊销该会话</button>' +
        "<div class='faint foot mt8'>破坏性动作：二次确认 → POST /admin/sessions/revoke（selector = sid_prefix）→ 服务端级联删 conversations/messages/turns；审计 kind=session_revoke（detail 只含 selector 与计数）。</div></div></div>";
    openDrawer();
  }

  /* ---------------------------------------------------------------- 用量 */
  function renderUsage() {
    var sc = state.usage.scope;
    var scopes = M.usageScopes || {};
    var data = scopes[sc] || scopes.global || { rows: [] };
    var cap = (M.pools && M.pools.capacity) || {};
    var usedPct = (cap.daily_units_budget && typeof cap.used_today === "number") ? Math.min(100, Math.round(100 * cap.used_today / cap.daily_units_budget)) : 0;
    var rows = (data.rows || []).map(function (r) {
      return "<tr><td class='mono'>" + esc(r.scope) + "</td><td class='mono'>" + num(r.requests) + "</td><td class='mono'>" + num(r.cost_units) + "</td></tr>";
    }).join("");
    if (!rows) rows = "<tr><td colspan='3' class='dim'>" + esc(data.available === false ? (data.reason || "未提供") : "该 scope 今日无台账行") + "</td></tr>";
    var quotaHit = ((M.audit || {}).by_kind || []).filter(function (k) { return k.kind === "quota_exhausted"; })[0];
    // 逐模型用量（W78-2）：真来源 = GET /admin/usage?scope=model（usage_counters 的 model: 行）。
    // 旧的「按模型 / 上游用量」卡读的是 /admin/models.usage —— 该面恒 available:false（无每模型计数器），
    // 故整卡改成真数据面；/admin/models 的模型目录仍留在「模型与任务」页。
    var mu = M.modelUsage || { available: false, rows: [], totals: {}, reason: "未取到 /admin/usage?scope=model" };
    var muDay = state.usage.day || mu.day || data.day || "";
    var modelRows = (mu.rows || []).map(function (r) {
      return '<tr class="model-usage-row"><td>' + mono(r.scope) + "</td><td class='mono'>" + num(r.requests) + "</td><td class='mono'>" + num(r.cost_units) + "</td></tr>";
    }).join("");
    if (!modelRows) modelRows = "<tr><td colspan='3' class='empty'>" + esc(mu.available === false ? (mu.reason || "未提供") : "本日无模型用量") + "</td></tr>";

    $("#page-usage").innerHTML =
      '<div class="page-head"><h1 id="h-usage">用量与成本</h1><span class="sub">UTC 日 · scope 四类 · 预算 gauge · 逐模型台账 · 多日趋势 · CSV/JSONL 导出</span></div>' +
      '<div class="card"><header><h2>台账</h2>' + '<div class="spacer"></div>' +
        '<label class="dim xs">day <input type="date" id="u-day" value="' + esc(data.day || state.usage.day) + '"></label></header>' +
        '<div class="ad-seg" id="u-tabs">' + ["global", "session", "key", "ip"].map(function (s) {
          return '<button data-scope="' + s + '" aria-selected="' + String(s === sc) + '">' + s + (scopes[s] && scopes[s].available === false ? "（未提供）" : "") + "</button>";
        }).join("") + "</div>" +
        (data.available === false ? '<div class="body">' + unavail(data, "台账不可用") + "</div>" :
          '<div class="body flush"><div class="table-wrap"><table class="admin"><thead><tr><th>scope</th><th>requests</th><th>cost_units</th></tr></thead>' +
          "<tbody>" + rows + "</tbody>" +
          "<tfoot><tr><td>合计（limit 截断前）</td><td class='mono'>" + num((data.totals || {}).requests) + "</td><td class='mono'>" + num((data.totals || {}).cost_units) + "</td></tr></tfoot></table></div></div>") +
        '<div class="body faint foot">session scope 只显示 sid 前缀；key/ip scope 后缀本身已是 sha8 摘要。日界 = UTC（' + esc(data.day || state.usage.day) + "）。</div></div>" +

      '<div class="grid c2">' +
        '<div class="card"><header><h2>日预算</h2>' + "</header><div class='body'>" +
          "<div class='kv-list'><li><span>budget</span><b>" + num(cap.daily_units_budget) + " units</b></li>" +
          "<li><span>used</span><b>" + num(cap.used_today) + " units</b></li><li><span>remaining</span><b>" + num(cap.remaining) + " units</b></li>" +
          "<li><span>拒绝事件（本页 audit 行）</span><b>" + (quotaHit ? num(quotaHit.count) + " 次 quota_exhausted" : none("本页无 quota_exhausted 行")) + "</b></li></div>" +
          '<div class="gauge mt10' + (usedPct > 80 ? " warn" : "") + '" data-pct="' + usedPct + '"><i></i></div>' +
        "</div></div>" +
        '<div class="card"><header><h2>7 日趋势（usage_counters 多日）</h2></header><div class="body">' +
          unavail({ reason: "无 GET /admin/usage/series 端点（多日 usage_counters 行在 store 里，缺查询口）——本卡不回数据" }, "未提供") +
        "</div></div>" +
      "</div>" +

      '<div class="card table-card"><header><h2>逐模型用量</h2><div class="spacer"></div>' +
        exportBar([{ label: "逐模型台账", type: "usage", scope: "model", day: muDay }]) +
        '<span class="card-note">来源 GET /admin/usage?scope=model</span></header>' +
        (mu.available === false ? '<div class="body">' + unavail(mu, "逐模型用量不可用（/admin/usage?scope=model）") + "</div>" :
          '<div class="body flush"><div class="table-wrap"><table class="admin model-usage-table"><thead><tr><th>模型</th><th>请求</th><th>成本 units</th></tr></thead>' +
          "<tbody>" + modelRows + "</tbody>" +
          "<tfoot><tr><td>合计（limit 截断前）</td><td class='mono'>" + num((mu.totals || {}).requests) + "</td><td class='mono'>" + num((mu.totals || {}).cost_units) + "</td></tr></tfoot></table></div></div>") +
        '<div class="body faint foot">' + esc((M.degradeNote || {}).perModelUsage || "") + "（day " + esc(muDay || "（服务端缺省 = 今日 UTC）") + "）</div></div>" +
      sourceLegend(["usage", "modelUsage", "pools", "audit", "exportFile"]);

    $("#u-tabs").onclick = function (ev) {
      var b = ev.target.closest("button[data-scope]");
      if (!b) return;
      state.usage.scope = b.getAttribute("data-scope");
      renderUsage();
    };
    $("#u-day").onchange = function () { state.usage.day = this.value; refreshAll({ day: this.value }); };
  }

  /* ---------------------------------------------------------------- 审计与告警 */
  function renderAudit() {
    var audit = M.audit || {};
    var alerts = M.alerts || {};
    var dg = M.diagnostics || {};
    var f = state.audit;
    var allRows = audit.rows || [];
    var filtered = allRows.filter(function (a) {
      if (f.kind && a.kind !== f.kind) return false;
      var hay = (a.kind || "") + " " + (a.detail || "") + " " + (a.sid_prefix || "") + " " + (a.key_id || "") + " " + (a.request_id || "");
      return !f.q || hay.indexOf(f.q) !== -1;
    });
    var rows = filtered.map(function (a) {
      return "<tr><td class='mono'>" + esc(hhmm(a.ts)) + "</td><td>" + mono(a.kind) + "</td>" +
        "<td class='mono'>" + esc(a.sid_prefix || "—") + "</td><td class='mono'>" + esc(a.key_id || "—") + "</td>" +
        "<td class='mono'>" + esc(a.request_id || "—") + "</td><td class='dim mono wrap'>" + esc(a.detail || "—") + "</td></tr>";
    }).join("");
    var byKind = (audit.by_kind || []).map(function (k) {
      return "<tr><td>" + mono(k.kind) + "</td><td class='mono'>" + num(k.count) + "</td><td class='right'>" + pct(k.count, audit.total) + "</td></tr>";
    }).join("");
    var kindOptions = (audit.by_kind || []).slice(0, 10).map(function (k) {
      return '<option value="' + esc(k.kind) + '">' + esc(k.kind) + "（" + num(k.count) + "）</option>";
    }).join("");
    var active = (alerts.active || []).map(function (a) {
      return "<tr><td>" + badge(a.severity === "error" ? "err" : a.severity === "warning" ? "warn" : "plain", a.severity) + "</td><td>" + mono(a.name) + "</td>" +
        "<td>" + esc(a.source || "—") + "</td><td>" + esc(a.subject) + "</td><td class='dim wrap'>" + esc(a.detail) + "</td><td class='mono'>" + esc(hhmm(a.since)) + "</td><td>" +
        '<button class="ad-ctl sm unimpl" disabled title="未实现：无 POST /admin/alerts/{id}/silence 端点">静音</button></td></tr>';
    }).join("");
    if (!active) active = "<tr><td colspan='7' class='empty'>当前无 active 告警</td></tr>";
    var history = (alerts.history || []).map(function (a) {
      return "<tr><td>" + badge("plain", a.severity) + "</td><td>" + mono(a.name) + "</td><td>" + esc(a.subject) + "</td>" +
        "<td class='mono'>" + esc(hhmm(a.since)) + "</td><td class='mono'>" + esc(a.until ? hhmm(a.until) : "—") + "</td><td class='dim wrap'>" + esc(a.detail) + "</td></tr>";
    }).join("");
    if (!history) history = "<tr><td colspan='6' class='empty'>无历史告警行（active 窗外的 ring 派生行）</td></tr>";
    var adm = dg.admin || {};
    var bans = ((adm.banned_ips) || []).map(function (b) { return "<tr><td class='mono'>" + esc(b.ip) + "</td><td class='mono'>" + esc(b.banned_until) + "</td><td class='mono'>" + num1(b.remaining_seconds) + "s</td></tr>"; }).join("");
    if (!bans) bans = "<tr><td colspan='3' class='empty'>当前无封禁 IP</td></tr>";

    var auditFilters = audit.available ?
      '<div class="filters">' +
        '<label>搜索 <input type="search" id="a-q" placeholder="kind / detail / sid / req" size="20"></label>' +
        '<label>kind <select id="a-kind"><option value="">全部</option>' + kindOptions + "</select></label>" +
        '<button class="ad-ctl sm" id="a-clear">重置</button>' +
        '<div class="spacer"></div><span class="filter-count" id="a-count">' + filtered.length + " / " + allRows.length + " 行</span>" +
      "</div>" : "";
    var auditCard = audit.available ?
      (auditFilters + '<div class="body flush"><div class="table-wrap"><table class="admin"><thead><tr><th>ts</th><th>kind</th><th>sid_prefix</th><th>key_id</th><th>request_id</th><th>detail（脱敏 JSON）</th></tr></thead><tbody>' +
        (rows || "<tr><td colspan='6' class='empty'>无匹配行（本页 " + num(allRows.length) + " 行，limit=200）</td></tr>") + "</tbody></table></div></div>") :
      ('<div class="body">' + unavail(audit, audit.reason && audit.reason.indexOf("503") === 0 ? "store 未启用（503 store_unavailable）" : "审计面不可用（/admin/audit）") + "</div>");

    $("#page-audit").innerHTML =
      '<div class="page-head"><h1 id="h-audit">审计与告警</h1><span class="sub">audit_log · admin 鉴权面 · active/history 告警 · webhook 状态</span></div>' +
      '<div class="card table-card"><header><h2>audit_log</h2>' +
        '<div class="spacer"></div>' + exportBar([{ label: "审计台账", type: "audit" }]) + '<span class="card-note">GET /admin/audit?kind=&since=&limit=（kind 空 = 全量）</span></header>' +
        auditCard +
        '<div class="body faint foot">detail 只含 op/conv/rev/messages 或请求摘要；绝不含 title/content/cookie/key/IP 原文（服务端出口过 redactAdminDetail）。</div></div>' +

      '<div class="grid c2-1">' +
        '<div class="card table-card"><header><h2>active 告警</h2></header>' +
          '<div class="body flush"><div class="table-wrap"><table class="admin"><thead><tr><th>severity</th><th>name</th><th>source</th><th>subject</th><th>说明</th><th>起始</th><th></th></tr></thead><tbody>' + active + "</tbody></table></div></div></div>" +
        '<div class="card"><header><h2>admin 鉴权面</h2></header><div class="body">' +
          "<div class='kv-list'><li><span>累计鉴权失败</span><b>" + num(adm.auth_failures) + "</b></li>" +
          "<li><span>跟踪 IP 数</span><b>" + num(adm.tracked_ips) + "</b></li>" +
          "<li><span>当前封禁</span><b>" + num((adm.banned_ips || []).length) + " 个</b></li></div>" +
          '<div class="table-wrap mt8"><table class="admin"><thead><tr><th>ip</th><th>banned_until</th><th>remaining</th></tr></thead><tbody>' + bans + "</tbody></table></div>" +
          "<div class='faint foot mt8'>连续 5 次失败封禁该 IP 30 分钟；封禁期响应与普通失败同形（401）。</div>" +
        "</div></div>" +
      "</div>" +

      '<div class="grid c2">' +
        '<div class="card table-card"><header><h2>history 告警</h2></header><div class="body flush"><div class="table-wrap"><table class="admin"><thead><tr><th>sev</th><th>name</th><th>subject</th><th>起始</th><th>结束</th><th>说明</th></tr></thead><tbody>' + history + "</tbody></table></div></div></div>" +
        '<div class="card"><header><h2>alert bridge（webhook）</h2></header><div class="body">' +
          unavail((alerts.webhook || {}), "未提供：服务无出网 webhook 面") +
        "</div></div>" +
      "</div>" +

      '<div class="card table-card"><header><h2>kind 分布（本页行）</h2></header>' +
        '<div class="body flush"><div class="table-wrap"><table class="admin"><thead><tr><th>kind</th><th>count</th><th>占比</th></tr></thead><tbody>' +
        (byKind || "<tr><td colspan='3' class='empty'>" + esc(audit.available === false ? (audit.reason || "未提供") : "本页无行") + "</td></tr>") +
        "</tbody></table></div></div>" +
        '<div class="body faint foot">本页 ' + num(audit.total) + " 行（分母 = 本页行数，limit=200；响应无聚合面，全量分布需 /admin/export 类端点）。</div></div>" +
      sourceLegend(["audit", "alerts", "diagnostics"]);

    if (audit.available) {
      var q = function (id) { return document.getElementById(id); };
      q("a-q").value = f.q;
      q("a-kind").value = f.kind;
      q("a-q").oninput = function () {
        state.audit.q = this.value;
        renderAudit();
        var el = document.getElementById("a-q");
        el.focus();
        el.setSelectionRange(el.value.length, el.value.length);
      };
      q("a-kind").onchange = function () { state.audit.kind = this.value; renderAudit(); };
      q("a-clear").onclick = function () { state.audit = { q: "", kind: "" }; renderAudit(); };
    }
  }

  /* ---------------------------------------------------------------- 模型与任务 */
  function renderModels() {
    var md = M.models || {}, tk = M.tasks || {}, dd = (M.diagnostics || {}).diagnostics || {};
    var f = state.models;
    var allModels = md.rows || [];
    var upstreams = [];
    allModels.forEach(function (m) { if (m.upstream && upstreams.indexOf(m.upstream) === -1) upstreams.push(m.upstream); });
    var filtered = allModels.filter(function (m) {
      var hay = (m.id || "") + " " + (m.upstream_model || "") + " " + (m.upstream || "");
      if (f.q && hay.indexOf(f.q) === -1) return false;
      if (f.upstream && m.upstream !== f.upstream) return false;
      if (f.usage === "yes" && !m.usage_available) return false;
      if (f.usage === "no" && m.usage_available) return false;
      return true;
    });
    var rows = filtered.map(function (m) {
      var st = m.state ? (m.state === "ok" ? badge("ok", "ok") : badge("cooling", m.state)) : none("无逐模型状态面");
      return "<tr><td>" + mono(m.id) + "</td><td>" + orNone(m.upstream_model, mono) + "</td><td>" + orNone(m.upstream, esc) + "</td>" +
        "<td class='mono'>" + num(m.max_input_tokens) + "</td><td class='mono'>" + num(m.max_tokens) + "</td>" +
        "<td class='mono'>" + (m.usage_available ? num(m.requests_today) : none("无每模型计数器")) + "</td>" +
        "<td class='mono'>" + (m.usage_available ? num(m.cost_units_today) : none("无每模型计数器")) + "</td><td>" + st + "</td></tr>";
    }).join("");
    if (!rows) rows = "<tr><td colspan='8' class='empty'>" + esc(md.available === false ? (md.reason || "未提供") : (allModels.length ? "无匹配模型（放宽筛选）" : (md.configured ? "models 表为空" : "未配置 models: 表（/admin/models 的 models 恒 []）"))) + "</td></tr>";
    var tasks = (tk.rows || []).map(function (t) {
      var st = t.status === "queued" || t.status === "in_progress" ? badge("plain", t.status) : t.status === "completed" ? badge("ok", "completed") : t.status === "cancelled" ? badge("plain", "cancelled") : badge("err", t.status);
      return "<tr><td class='mono'>" + esc(t.id) + "</td><td>" + mono(t.model) + "</td><td>" + st + "</td>" +
        "<td class='mono'>" + esc(hhmm(t.created)) + "</td><td class='mono'>" + orNone(t.terminal, hhmm) + "</td>" +
        "<td>" + (t.delivered ? badge("ok", "delivered") : badge("plain", "no")) + "</td>" +
        "<td class='mono'>" + (t.attempts === null ? none("任务未收尾（在飞记录不可读）") : num(t.attempts)) + "</td>" +
        "<td class='mono'>" + orNone(t.owner_sid_prefix, esc) + "</td>" +
        "<td>" + (t.face === "unknown" || !t.face ? none("taskEntry 不存该位（服务端恒回 unknown）") : mono(t.face)) + "</td>" +
        "<td>" + orNone(t.upstream, esc) + "</td>" +
        '<td><button class="ad-ctl sm danger unimpl" disabled title="未实现：无取消面（POST /v1/responses/{id}/cancel 属业务面，管理面不代理）">取消</button></td></tr>';
    }).join("");
    if (!tasks) tasks = "<tr><td colspan='11' class='empty'>" + esc(tk.available === false ? (tk.reason || "未提供") : "无任务行（快照为空）") + "</td></tr>";
    var counts = [
      ["tasks created", dd.tasks_created],
      ["terminal completed", dd.tasks_terminal_completed],
      ["terminal failed", dd.tasks_terminal_failed],
      ["terminal incomplete", dd.tasks_terminal_incomplete],
      ["terminal cancelled", dd.tasks_terminal_cancelled],
      ["idem replays", dd.tasks_idem_replays],
      ["passthrough usage records", dd.passthrough_usage_records],
      ["passthrough face degrades", dd.passthrough_face_degrades]
    ].map(function (kv) { return "<li><span>" + esc(kv[0]) + "</span><b>" + num(kv[1]) + "</b></li>"; }).join("");

    $("#page-models").innerHTML =
      '<div class="page-head"><h1 id="h-models">模型与任务</h1><span class="sub">模型目录（别名 → 上游模型）· 按模型用量 · 后台任务</span></div>' +
      '<div class="card table-card"><header><h2>模型目录</h2>' +
        '<div class="spacer"></div><span class="card-note">GET /admin/models（configured=' + String(!!md.configured) + "）</span></header>" +
        (md.available === false ? '<div class="body">' + unavail(md, "模型目录不可用") + "</div>" :
        '<div class="filters">' +
          '<label>搜索 <input type="search" id="m-q" placeholder="model / upstream" size="18"></label>' +
          '<label>上游 <select id="m-up"><option value="">全部</option>' + upstreams.map(function (u) { return '<option value="' + esc(u) + '">' + esc(u) + "</option>"; }).join("") + "</select></label>" +
          '<label>用量 <select id="m-usage"><option value="">全部</option><option value="yes">有计数器</option><option value="no">未提供</option></select></label>' +
          '<button class="ad-ctl sm" id="m-clear">重置</button>' +
          '<div class="spacer"></div><span class="filter-count" id="m-count">' + filtered.length + " / " + allModels.length + " 行</span>" +
        "</div>" +
        '<div class="body flush"><div class="table-wrap"><table class="admin"><thead><tr><th>id</th><th>upstream_model</th><th>upstream</th><th>max_input_tokens</th><th>max_tokens</th><th>今日请求</th><th>今日 units</th><th>状态</th></tr></thead><tbody>' + rows + "</tbody></table></div></div>") +
        '<div class="body faint foot">发现面（默认 ' + esc((md.discover || {}).default_model || "—") + " / " + num(((md.discover || {}).models || []).length) + " 个模型）与 /ui/api/config、/v1/models 同源（Config.Models 的 id 表）；upstream 列与管理面缺的逐模型用量见下方说明。</div></div>" +

      '<div class="card table-card"><header><h2>后台任务</h2>' +
        '<div class="spacer"></div><span class="card-note">GET /admin/tasks（有界只读快照，最新优先）</span></header>' +
        (tk.available === false ? '<div class="body">' + unavail(tk, "任务面不可用") + "</div>" :
        '<div class="body flush"><div class="table-wrap"><table class="admin"><thead><tr><th>id</th><th>model</th><th>status</th><th>created</th><th>terminal</th><th>delivered</th><th>attempts</th><th>owner sid</th><th>face</th><th>upstream</th><th>动作</th></tr></thead><tbody>' + tasks + "</tbody></table></div></div>") +
        '<div class="body faint foot">取消未实现（按钮 disabled）：管理面不代理 /v1/responses/{id}/cancel；face 位服务端恒 unknown（taskEntry 不存该位）。</div></div>' +

      '<div class="card"><header><h2>任务面计数（diagnostics 累计）</h2></header><div class="body">' +
        (dd.tasks_created === undefined ? unavail({ reason: "diagnostics 未返回任务面计数" }, "未提供") :
          "<div class='kv-list'>" + counts + "</div>") +
        '<div class="faint foot mt8">进程内累计、重启清零；逐任务明细见上表（/admin/tasks 快照）。</div></div></div>' +
      sourceLegend(["models", "tasks", "diagnostics"]);

    if (md.available !== false) {
      var q = function (id) { return document.getElementById(id); };
      q("m-q").value = f.q;
      q("m-up").value = f.upstream;
      q("m-usage").value = f.usage;
      q("m-q").oninput = function () {
        state.models.q = this.value;
        renderModels();
        var el = document.getElementById("m-q");
        el.focus();
        el.setSelectionRange(el.value.length, el.value.length);
      };
      q("m-up").onchange = function () { state.models.upstream = this.value; renderModels(); };
      q("m-usage").onchange = function () { state.models.usage = this.value; renderModels(); };
      q("m-clear").onclick = function () { state.models = { q: "", upstream: "", usage: "" }; renderModels(); };
    }
  }

  /* ---------------------------------------------------------------- 配置与系统 */
  function renderConfig() {
    var c = M.config || {}, sys = M.system || {}, rt = sys.runtime || {}, bd = sys.build || {}, st = sys.store || {}, pub = c.public || {}, ui = c.ui || {}, cd = c.cooldowns || {}, logs = M.logs || {};
    if (c.available === false) {
      $("#page-config").innerHTML = '<div class="page-head"><h1 id="h-config">配置与系统</h1></div>' +
        '<div class="card"><div class="body">' + unavail(c, "配置面不可用（/admin/config）") + "</div></div>";
      return;
    }
    var ups = (c.upstreams || []).map(function (u) {
      return "<tr><td class='mono'>" + num(u.index) + "</td><td>" + esc(u.name) + "</td><td>" + mono(u.protocol) + "</td>" +
        "<td class='mono'>" + esc(u.base_url) + " <span class='faint foot-sm'>（已去 userinfo）</span></td>" +
        "<td class='mono'>" + num(u.credentials) + "</td><td>" + (u.proxy ? badge("plain", "proxy") : badge("plain", "direct")) + "</td>" +
        "<td class='mono'>" + orNone(u.tls_fingerprint) + "</td><td>" + mono(u.task_face || "—") + "</td>" +
        "<td>" + (u.tool_sim === null || u.tool_sim === undefined ? none("配置面不回该位（不在脱敏投影里）") : badge("warn", "on")) + "</td></tr>";
    }).join("");
    if (!ups) ups = "<tr><td colspan='9' class='dim'>未配置上游</td></tr>";
    var models = (c.models || []).map(function (m) { return "<li><span>" + mono(m.id) + "</span><span>→ " + orNone(m.upstream_model, mono) + "</span></li>"; }).join("");
    if (!models) models = "<li><span class='dim'>未配置 models 表</span></li>";
    var logTail = (logs.entries || []).slice(0, 6).map(function (e) {
      return "<li><span class='mono'>" + esc(hhmm(e.time)) + " " + esc(e.kind) + "</span><span class='dim'>" + esc(e.message) + "</span></li>";
    }).join("");
    if (!logTail) logTail = "<li><span class='dim'>" + esc(logs.available === false ? (logs.reason || "未提供") : "ring 为空") + "</span></li>";

    $("#page-config").innerHTML =
      '<div class="page-head"><h1 id="h-config">配置与系统</h1><span class="sub">脱敏配置 · 运行时 · store · 部署件 · kill switch runbook · 日志 tail</span></div>' +

      '<div class="grid c2">' +
        '<div class="card"><header><h2>服务面（脱敏）</h2>' + "</header><div class=\"body\"><ul class=\"kv-list def-list\">" +
          "<li><span>listen</span><b>" + orNone(c.listen, mono) + "</b></li>" +
          "<li><span>admin key</span><b>" + (c.admin_configured ? badge("ok", "configured") : badge("err", "missing")) + "（只回是否配置，不回值；log_capacity=" + num(c.admin_log_capacity) + "）</b></li>" +
          "<li><span>ui</span><b>enabled=" + String(!!ui.enabled) + " rate_limit_per_min=" + num(ui.rate_limit_per_min) + "</b></li>" +
          "<li><span>store</span><b>enabled=" + String(!!st.enabled) + " path=" + orNone(st.path_masked) + "</b></li>" +
          "<li><span>public</span><b>enabled=" + String(!!pub.enabled) + " mode=" + orNone(pub.access_mode) + " trusted_proxies=" + num(pub.trusted_proxies) + " access_keys=" + num(pub.access_keys) + "</b></li>" +
          "<li><span>limits</span><b>max_body=" + fmtBytes(pub.max_body_bytes) + " max_messages=" + num(pub.max_messages) + " max_chars=" + num(pub.max_message_chars) + "</b></li>" +
          "<li><span>闸 / 队列</span><b>inflight " + num(pub.max_inflight) + " / queue " + num(pub.queue_size) + " (" + num(pub.queue_timeout_seconds) + "s)</b></li>" +
          "<li><span>凭据最小间隔</span><b>" + num(pub.credential_min_gap_seconds) + "s</b></li>" +
          "<li><span>成本档 / 预算</span><b>max_cost_class=" + orNone(pub.max_cost_class) + " daily_units=" + num(pub.daily_budget_units) + " enabled=" + String(!!pub.daily_budget_enabled) + "</b></li>" +
          "<li><span>heartbeat</span><b>" + num(c.heartbeat_seconds) + "s</b></li>" +
          "<li><span>cooldowns</span><b>failures=" + num(cd.failures) + " seconds=" + num(cd.seconds) + " ladder=" + esc((cd.ladder || []).join("/")) + "</b></li>" +
          "<li><span>failure rate cooldown</span><b>enabled=" + String(!!cd.failure_rate_enabled) + " thr=" + num(cd.failure_rate_threshold) + " min_req=" + num(cd.failure_rate_min_requests) + "</b></li>" +
          "<li><span>task_face</span><b>degrade=" + orNone(c.task_face_degrade) + " · 逐上游 face 见下表（无全局 default 位）</b></li>" +
          "<li><span>debug capture</span><b>" + orNone(c.debug_capture_dir) + "</b></li>" +
        "</ul><div class='faint foot mt8'>脱敏规则：" + esc((c.redaction || []).join("；")) + "。</div></div></div>" +

        '<div class="card"><header><h2>运行时</h2>' + "</header><div class=\"body\"><ul class=\"kv-list def-list\">" +
          "<li><span>go version</span><b>" + orNone(rt.go_version, mono) + "</b></li>" +
          "<li><span>commit</span><b>" + orNone(bd.commit, mono) + "</b></li>" +
          "<li><span>built at</span><b>" + orNone(bd.built_at, mono) + "</b></li>" +
          "<li><span>uptime</span><b>" + esc(dur(rt.uptime_seconds)) + "</b></li>" +
          "<li><span>pid</span><b>" + num(rt.pid) + "</b></li>" +
          "<li><span>goroutines</span><b>" + num(rt.goroutines) + "</b></li>" +
          "<li><span>mem alloc / sys</span><b>" + fmtBytes(rt.mem_alloc_bytes) + " / " + fmtBytes(rt.mem_sys_bytes) + "</b></li>" +
          "<li><span>GC cycles / pause</span><b>" + num(rt.gc_cycles) + " / " + num1(rt.gc_pause_total_ms) + "ms</b></li>" +
          "<li><span>healthz</span><b>" + badge("ok", "ok") + "（管理面已注册：未配 key 时本页整页 404）</b></li>" +
        "</ul></div></div>" +
      "</div>" +

      '<div class="card table-card"><header><h2>上游配置（脱敏）</h2>' + '<div class="spacer"></div><span class="card-note">base_url 去 userinfo；api_key/workspace/device 只回是否配置；pool_size 只回条目数</span></header>' +
        '<div class="body flush"><div class="table-wrap"><table class="admin"><thead><tr><th>#</th><th>name</th><th>protocol</th><th>base_url</th><th>池条目</th><th>出站</th><th>tls_fingerprint</th><th>task_face</th><th>tool_sim</th></tr></thead><tbody>' + ups + "</tbody></table></div></div></div>" +

      '<div class="grid c2">' +
        '<div class="card"><header><h2>store / 部署件</h2>' + "</header><div class=\"body\"><ul class=\"kv-list def-list\">" +
          "<li><span>store mode</span><b>" + esc(st.mode || "—") + "（size " + fmtBytes(st.size_bytes) + " / path " + orNone(st.path_masked) + "）</b></li>" +
          "<li><span>sessions / audit 行</span><b>" + num(st.sessions) + " / " + num(st.audit_rows) + "</b></li>" +
          "<li><span>schema 版本</span><b>" + none("无 schema_meta 只读面（/admin/system 不回该位）") + "</b></li>" +
          "<li><span>binary sha256[:8]</span><b>" + none("无 buildinfo sha 注入面（设计 §5.8）") + "</b></li>" +
          "<li><span>deploy dir</span><b>" + none("服务不感知部署目录（需启动参数/外部桥）") + "</b></li>" +
          "<li><span>backup 引用</span><b>" + none("备份装置在 server，服务不感知（需外部桥）") + "</b></li>" +
        "</ul><div class='faint foot mt8'>" + esc(st.note || "") + "</div></div></div>" +

        '<div class="card"><header><h2>kill switch runbook</h2></header><div class="body">' +
          "<ol class='runbook'>" +
            "<li>确认影响面：public.enabled=false 只停公开面（会话/闸/限额）；<code>/healthz</code> 保留。</li>" +
            "<li>改配置 → 重启进程（配置启动期静态加载，无热改）。</li>" +
            "<li>回滚：恢复上一份 config.yaml → 重启。</li>" +
            "<li>全程审计：<code>kill_switch</code> 事件 + 操作者 key_id + 时间。</li>" +
          "</ol>" +
          '<button class="ad-ctl danger unimpl" disabled title="未实现：kill switch = 改配置 public.enabled=false + 重启，无 HTTP 面；管理面不提供该动作">执行 kill switch（未实现）</button>' +
          '<div class="faint foot mt8">按钮 disabled 是刻意的：管理面没有任何端点能停公开面/改配置；在此假装成功就是撒谎。部署件回滚引用（备份文件名/sha）服务不感知 → 见 server 侧 runbook。</div>' +
        "</div></div>" +
      "</div>" +

      '<div class="grid c2">' +
        '<div class="card"><header><h2>模型映射</h2>' + "</header><div class=\"body\"><ul class=\"kv-list def-list\">" + models + "</ul></div></div>" +
        '<div class="card"><header><h2>日志 tail（脱敏）</h2>' + "</header><div class=\"body\"><ul class='kv-list def-list'>" + logTail + "</ul>" +
          "<div class='faint foot mt8'>只读 ring（不落盘）；游标增量读见 /admin/logs?since_id=。凭据头一律掩码。</div></div></div>" +
      "</div>" +

      '<div class="card"><header><h2>运维装置状态（cron / relogin / 备份）</h2></header>' +
        '<div class="body">' + unavail({ reason: "服务不感知这些装置（在 server 侧 cron 上）；要进 dashboard 必须有外部状态桥（装置写 JSON 状态文件 → 管理面只读读取，或运维侧 Prometheus textfile 采集）" }, "未提供") + "</div></div>" +
      sourceLegend(["config", "system", "logs"]);
  }

  /* ---------------------------------------------------------------- 抽屉 / 对话框 / toast */
  function openDrawer() { $("#drawer").classList.add("open"); $("#drawer").setAttribute("aria-hidden", "false"); $("#scrim").classList.add("on"); applyDynSizes($("#drawer-body")); }
  function closeDrawer() { $("#drawer").classList.remove("open"); $("#drawer").setAttribute("aria-hidden", "true"); $("#scrim").classList.remove("on"); }

  var pending = null;
  function confirmDialog(title, bodyHtml, onOk) {
    $("#dialog-title").textContent = title;
    $("#dialog-body").innerHTML = bodyHtml;
    $("#dialog").classList.add("open");
    $("#scrim").classList.add("on");
    pending = onOk;
  }
  function closeDialog() { $("#dialog").classList.remove("open"); pending = null; if (!$("#drawer").classList.contains("open")) $("#scrim").classList.remove("on"); }

  function toast(msg, kind) {
    var el = document.createElement("div");
    el.className = "toast" + (kind ? " " + kind : "");
    el.textContent = msg;
    $("#toasts").appendChild(el);
    setTimeout(function () { el.remove(); }, 4200);
  }

  /* 破坏性动作：二次确认 → 真发请求（A2AAdmin.revoke / resetCooldown）→ 刷新 + toast。
     未实现的动作（kill switch / 任务取消 / 探针 / 告警静音）按钮在 DOM 里就 disabled。 */
  function authFail(res) { return res && res.status === 401; }

  function handleAction(act, el) {
    if (act === "req") { openRequestDrawer(el.getAttribute("data-id")); return; }
    if (act === "sess") { openSessionDrawer(el.getAttribute("data-sid")); return; }
    if (act === "revoke") {
      var sid = el.getAttribute("data-sid");
      confirmDialog("吊销会话 " + sid + "？",
        "将删除该会话及其级联内容（conversations / messages / turns），旧 cookie 立即失效。<div class='danger-note'>破坏性动作：二次确认后真发 POST /admin/sessions/revoke；服务端写审计行 kind=session_revoke（detail 只含 selector 与计数）。</div>",
        function () {
          if (!window.A2AAdmin) return;
          window.A2AAdmin.revoke({ sid_prefix: sid }).then(function (res) {
            if (res.ok) {
              toast("已吊销 " + num((res.body || {}).revoked) + " 个会话（matched " + num((res.body || {}).matched) + "）");
              refreshAll({});
              return;
            }
            if (authFail(res)) { lockUI("密钥无效或已失效"); return; }
            if (res.status === 404) { toast("未匹配到会话（服务端 404：selector 无命中）", "warn"); return; }
            if (res.status === 409) { toast("selector 歧义（服务端 409，matched " + num((res.body || {}).matched) + "）", "err"); return; }
            toast("吊销失败：" + ((res && res.message) || res.status), "err");
          });
        });
      return;
    }
    if (act === "reset") {
      var up = el.getAttribute("data-up");
      confirmDialog("复位冷却 " + up + "？",
        "清冷却截止 + 连续失败计数 + 阶梯档位（不动失败率窗口）。幂等：重复复位第二次起 was_cooling=false。<div class='danger-note'>二次确认后真发 POST /admin/cooldowns/reset（selector = upstream）。</div>",
        function () {
          if (!window.A2AAdmin) return;
          window.A2AAdmin.resetCooldown({ upstream: up }).then(function (res) {
            if (res.ok) {
              var b = res.body || {};
              var first = (b.reset || [])[0] || {};
              toast("已复位 " + num(b.count) + " 个上游（" + esc(first.name || up) + " was_cooling=" + String(!!first.was_cooling) + "）");
              refreshAll({});
              return;
            }
            if (authFail(res)) { lockUI("密钥无效或已失效"); return; }
            if (res.status === 404) { toast("上游不存在（服务端 404）", "err"); return; }
            toast("复位失败：" + ((res && res.message) || res.status), "err");
          });
        });
      return;
    }
    if (act === "cooldown") {
      var cdUp = el.getAttribute("data-up") || "", cdIdx = Number(el.getAttribute("data-idx"));
      var sel = document.getElementById(el.getAttribute("data-cd") || "");
      var cdSec = sel ? Number(sel.value) : 3600;
      if (!window.A2AAdmin || !window.A2AAdmin.cooldown) { toast("冷却动作不可用：适配层未提供该面", "err"); return; }
      var cdLabel = el.textContent;
      el.disabled = true; el.textContent = "冷却中…";
      window.A2AAdmin.cooldown(cdUp, cdIdx, cdSec, "manual-ui").then(function (res) {
        el.disabled = false; el.textContent = cdLabel;
        if (res && res.ok) {
          var cb = res.body || {};
          toast("已冷却 #" + num(cb.index) + " " + dur(cb.cooldown_remaining_seconds));
          refreshAll({});
          return;
        }
        if (authFail(res)) { lockUI("密钥无效或已失效"); return; }
        if (res && res.status === 404) { toast("未找到该凭据（服务端 404：上游名或池下标无命中）", "err"); return; }
        if (res && res.status === 400) { toast("冷却被拒（服务端 400：" + ((res.body || {}).error ? (res.body.error.message || "") : (res.message || "")) + "）", "err"); return; }
        toast("冷却失败：" + ((res && res.message) || (res && res.status) || "未知错误"), "err");
      }, function (e) { el.disabled = false; el.textContent = cdLabel; toast("冷却异常：" + e, "err"); });
      return;
    }
    if (act === "probe") {
      var pUp = el.getAttribute("data-up") || "";
      if (!window.A2AAdmin || !window.A2AAdmin.probe) { toast("探针动作不可用：适配层未提供该面", "err"); return; }
      var pLabel = el.textContent;
      el.disabled = true; el.textContent = "探测中…";
      window.A2AAdmin.probe(pUp).then(function (res) {
        el.disabled = false; el.textContent = pLabel;
        if (res && res.ok) {
          state.probes[pUp] = res.body || {};
          var pb = state.probes[pUp];
          toast("探针 " + pUp + "：" + probeText(pb), pb.ok ? "" : "warn");
          refreshAll({});  // providers.readiness 由探针结果派生 → 重新拉，让徽标跟上
          return;
        }
        if (authFail(res)) { lockUI("密钥无效或已失效"); return; }
        if (res && res.status === 404) { toast("上游不存在（服务端 404：" + pUp + "）", "err"); return; }
        toast("探针失败：" + ((res && res.message) || (res && res.status) || "未知错误"), "err");
      }, function (e) { el.disabled = false; el.textContent = pLabel; toast("探针异常：" + e, "err"); });
      return;
    }
    if (act === "export") {
      var exType = el.getAttribute("data-type") || "";
      var exFmt = el.getAttribute("data-format") || "jsonl";
      if (!window.A2AAdmin || !window.A2AAdmin.exportFile) { toast("导出不可用：适配层未提供导出面", "err"); return; }
      var exLabel = el.textContent;
      el.disabled = true; el.textContent = "导出中…";
      window.A2AAdmin.exportFile({
        type: exType, format: exFmt,
        scope: el.getAttribute("data-scope") || "",
        day: el.getAttribute("data-day") || ""
      }).then(function (res) {
        el.disabled = false; el.textContent = exLabel;
        if (res && res.ok) { toast("已导出 " + res.filename); return; }
        if (authFail(res)) { lockUI("密钥无效或已失效"); return; }
        toast("导出失败：" + ((res && res.reason) || (res && res.status) || "未知错误"), "err");
      }, function (e) { el.disabled = false; el.textContent = exLabel; toast("导出异常：" + e, "err"); });
      return;
    }
    if (act === "cursor") {
      if (!window.A2AAdmin) return;
      window.A2AAdmin.loadMoreSessions().then(function (res) {
        if (res.ok) { toast("已加载下一页（新增 " + res.added + " 行）"); renderSessions(); return; }
        if (res.reason === "unauthorized") { lockUI("密钥无效或已失效"); return; }
        if (res.reason === "no_cursor") { toast("已到末页（无 next_cursor）", "warn"); return; }
        toast("翻页失败：" + (((res.error || {}).message) || "未知错误"), "err");
      });
      return;
    }
    toast("未实现：管理面没有该动作的端点（不假装成功）", "warn");
  }

  /* ---------------------------------------------------------------- 刷新 / 门控 */
  var REFRESH_MS = 10000; // overview / traffic / audit 的自动刷新间隔
  var AUTO_PAGES = { overview: true, traffic: true, audit: true };
  var bootDone = false, refetchTimer = null;

  /* 启动层：首帧由 #boot 盖住，首次装载 / 落锁后收掉（最短 BOOT_MIN_MS 防闪一下就没）。
   * prefers-reduced-motion：不做收尾过渡（admin.css 同步关掉），JS 侧也不等最短时长。
   * 观测面 window.__a2aAdminBoot = {ms, min, reduced}（smoke 量启动时长用）。
   * 看门狗：装载久拖不决（服务端卡住）也要露出界面，绝不留一层永远不走的启动层。 */
  var BOOT_MIN_MS = 300;
  var BOOT_WATCHDOG_MS = 6000;
  var bootShownAt = Date.now(), bootTimer = null, bootGone = false;

  function bootReduced() {
    try { return !!(window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches); } catch (e) { return false; }
  }
  function bootShow(msg) {
    var el = $("#boot");
    if (!el) return;
    el.classList.remove("boot-done");
    el.setAttribute("aria-hidden", "false");
    var t = $("#boot-text");
    if (t && msg) t.textContent = msg;
    bootShownAt = Date.now();
    bootGone = false;
  }
  function bootHide() {
    var el = $("#boot");
    if (!el || bootGone) return;
    bootGone = true;
    var wait = bootReduced() ? 0 : Math.max(0, BOOT_MIN_MS - (Date.now() - bootShownAt));
    clearTimeout(bootTimer);
    bootTimer = setTimeout(function () {
      var shown = Date.now() - bootShownAt;
      el.classList.add("boot-done");
      el.setAttribute("aria-hidden", "true");
      window.__a2aAdminBoot = { ms: shown, min: BOOT_MIN_MS, reduced: bootReduced() };
    }, wait);
  }
  setTimeout(bootHide, BOOT_WATCHDOG_MS); // 看门狗：服务端卡住也不留永不收的启动层

  function overlayBusy() { return $("#dialog").classList.contains("open") || $("#drawer").classList.contains("open"); }

  // filterParams 把 traffic 页的筛选映射成 /admin/requests 的服务端参数（空值不发）。
  function filterParams() {
    var f = state.traffic, out = {};
    ["entry", "model", "upstream", "status", "failed", "stream"].forEach(function (k) { if (f[k] !== "") out[k] = f[k]; });
    if (f.q) out.request_id = f.q;
    if (f.window) out.since = new Date(Date.now() - { "5m": 300000, "1h": 3600000, "24h": 86400000 }[f.window]).toISOString();
    return out;
  }

  function refreshAll(opts) {
    opts = opts || {};
    if (!window.A2AAdmin) return Promise.resolve({ ok: false, reason: "no_adapter" });
    if (!window.A2AAdmin.key.has()) { lockUI(""); return Promise.resolve({ ok: false, reason: "no_key" }); }
    return window.A2AAdmin.load({
      filter: filterParams(),
      day: opts.day || state.usage.day,
      usageScope: state.usage.scope
    }).then(function (res) {
      if (!res.ok) {
        if (res.reason === "unauthorized") { lockUI("密钥无效或已失效"); return res; }
        if (res.reason === "disabled") { adminOff(); return res; }
        if (res.reason === "no_key") { lockUI(""); return res; }
        toast("刷新失败：" + (res.reason || "未知错误"), "err");
        // 失败也要给出界面：空壳 = 又一次"登录后白页"。渲染本页的显式空态/未提供卡片。
        if (!$("#page-" + state.page) || !$("#page-" + state.page).innerHTML) render(state.page);
        bootHide();
        return res;
      }
      var meta = M.meta || {};
      state.usage.day = state.usage.day || meta.day || "";
      $("#top-status").textContent = "live";
      $("#top-status").className = "badge ok";
      hideLock();
      // 渲染条件：默认渲染；opts.local（后台重取，如筛选输入抖动）只换数据不重绘，
      // 但 opts.initial（首帧 / 登录首装）必须渲染——否则锁层一藏就是一片空白。
      if (!opts.local || opts.initial) {
        navCounts();
        if (!overlayBusy()) render(state.page);
        bootHide();
      }
      return res;
    }, function (e) { toast("刷新异常：" + e, "err"); bootHide(); });
  }

  function debounceRefetch() {
    clearTimeout(refetchTimer); // null/已触发句柄本就 no-op
    refetchTimer = setTimeout(function () { refetchTimer = null; refreshAll({ local: true }); }, 300);
  }

  // autoRefresh：overview / traffic / audit 每 10s；抽屉或对话框打开时暂停（不动用户正在读的界面）。
  function autoRefresh() {
    if (overlayBusy() || !AUTO_PAGES[state.page]) return;
    if (!window.A2AAdmin || !window.A2AAdmin.key.has()) return;
    refreshAll({});
  }

  // lockUI 落锁：清 sessionStorage 密钥 + 清空页面/抽屉 DOM（不留密钥，也不留需要密钥才该看的数据）。
  function lockUI(msg) {
    if (window.A2AAdmin) window.A2AAdmin.key.clear();
    $("#lock-key").value = "";
    $$("section.page").forEach(function (s) { s.innerHTML = ""; });
    $("#drawer-body").innerHTML = "";
    closeDrawer(); closeDialog();
    $("#lock").hidden = false;
    $("#lock-fields").style.display = "";
    $("#lock-connect").disabled = false;
    $("#lock-msg").textContent = msg || "";
    $("#lock-hint").textContent = "未配置 admin_key 时管理面整体不注册：/admin/* 回 404。";
    $("#top-status").textContent = "locked";
    $("#top-status").className = "badge err";
    bootHide(); // 落锁 = 有界面了，启动层让位（否则永远盖着锁层）
  }

  // adminOff：/admin/* 404 = 服务端没配 admin_key（路由不注册）——无密钥可输，给明确处置说明。
  function adminOff() {
    if (window.A2AAdmin) window.A2AAdmin.key.clear();
    $("#lock-key").value = "";
    $$("section.page").forEach(function (s) { s.innerHTML = ""; });
    closeDrawer(); closeDialog();
    $("#lock").hidden = false;
    $("#lock-fields").style.display = "none";
    $("#lock-connect").disabled = true;
    $("#lock-msg").textContent = "管理面未启用（未配置 admin_key）";
    $("#lock-hint").textContent = "服务端未配置 admin_key ⇒ /admin/* 全部 404（不是 401）。配置 admin_key 并重启服务后刷新本页。";
    $("#top-status").textContent = "admin off";
    $("#top-status").className = "badge err";
    bootHide();
  }

  function hideLock() {
    $("#lock").hidden = true;
    $("#lock-key").value = ""; // 密钥只留 sessionStorage，DOM 不留
  }

  function submitKey(ev) {
    ev.preventDefault();
    var k = $("#lock-key").value.trim();
    if (!k) { $("#lock-msg").textContent = "请输入管理密钥"; $("#lock-key").focus(); return; }
    window.A2AAdmin.key.set(k);
    $("#lock-key").value = "";
    $("#lock-msg").textContent = "连接中…";
    bootShow("正在校验管理密钥并装载管理面…");
    // initial:true —— 登录首装必须渲染（含 #/upstreams、#/traffic 这类深链），
    // 不能只把数据装进 A2A_LIVE 就完事（W77-2 修的白页根因）。
    refreshAll({ initial: true }).then(function (res) {
      if (res.ok || res.reason === "disabled") { $("#lock-msg").textContent = ""; return; }
      // 401 / no_key 的原因已由 lockUI 写在锁层（"密钥无效或已失效"）；这里只兜其余错误，
      // 不拿 reason 码糊脸、不覆盖更具体的原因。
      if (res.reason === "unauthorized") return;
      $("#lock-msg").textContent = res.reason === "no_key" ? "请输入管理密钥" : "连接失败：" + (res.reason || "未知错误");
    });
  }

  /* ---------------------------------------------------------------- 路由 / 主题 */
  var PAGES = { overview: ["总览", renderOverview], upstreams: ["上游与池", renderUpstreams], accounts: ["账号与代理", renderAccounts], traffic: ["请求", renderTraffic], sessions: ["会话", renderSessions], usage: ["用量与成本", renderUsage], audit: ["审计与告警", renderAudit], models: ["模型与任务", renderModels], providers: ["服务商", renderProviders], config: ["配置与系统", renderConfig] };
  var PAGE_GROUP = { overview: "运行", upstreams: "运行", accounts: "运行", traffic: "运行", sessions: "运行", usage: "账与合规", audit: "账与合规", models: "配置", providers: "配置", config: "配置" };

  function render(page) {
    if (!PAGES[page]) page = "overview";
    state.page = page;
    document.documentElement.setAttribute("data-page", page);
    var target = $("#page-" + page);
    $$("section.page").forEach(function (s) { s.classList.toggle("hidden", s.id !== "page-" + page); });
    $$(".nav a.item").forEach(function (a) { a.setAttribute("aria-current", a.getAttribute("data-page") === page ? "page" : "false"); });
    $("#where-crumb").textContent = PAGE_GROUP[page] || "Any2API";
    $("#where-page").textContent = PAGES[page][0];
    PAGES[page][1]();
    applyDynSizes(target);
    pageIn(target);
    window.__a2aAdminPage = page;
    try { window.__a2aAdminReady = (window.__a2aAdminReady || 0) + 1; } catch (e) { /* noop */ }
  }
  /* 切页动效：只动 opacity/transform（无 layout shift）；动画一结束就摘类，
   * 免得 transform 长期挂着改变 fixed/sticky 的包含块。reduced-motion 下不加类。 */
  function pageIn(el) {
    if (!el || bootReduced() || !el.classList) return;
    el.classList.remove("page-in");
    void el.offsetWidth; // 强制重排：同一页复渲染也能重放
    el.classList.add("page-in");
    var clear = function () {
      el.classList.remove("page-in");
      if (el.removeEventListener) el.removeEventListener("animationend", clear);
    };
    if (el.addEventListener) el.addEventListener("animationend", clear);
    setTimeout(clear, 400);
  }
  function hashPage() {
    var h = (location.hash || "").replace(/^#\/?/, "");
    return PAGES[h] ? h : "overview";
  }
  function route() {
    $("#nav").classList.remove("open");
    render(hashPage());
    if (bootDone && window.A2AAdmin && window.A2AAdmin.key.has()) refreshAll({}); // 切页即刷新
  }
  function navCounts() {
    var al = M.alerts || {}, dg = M.diagnostics || {}, rq = M.requests || {}, ss = M.sessions || {}, meta = M.meta || {};
    var ac = M.accounts || {}, pv = M.providers || {};
    $("#nav-cnt-alerts").textContent = (al.active || []).length ? String((al.active || []).length) : "";
    $("#nav-cnt-upstreams").textContent = (dg.upstreams || []).filter(function (u) { return u.cooling; }).length + " 冷却";
    // 账号计数 = 上游条数（/admin/accounts 的 upstreams 与配置序同源）。
    $("#nav-cnt-accounts").textContent = ac.available ? String((ac.upstreams || []).length) : "";
    // 服务商计数 = 已配置（readiness ≠ not_configured）的条目数。
    $("#nav-cnt-providers").textContent = pv.available ? String((pv.providers || []).filter(function (p) { return p.configured; }).length) : "";
    $("#nav-cnt-requests").textContent = (rq.rows || []).length + " 行";
    $("#nav-cnt-sessions").textContent = String((ss.sessions || []).length);
    $("#nav-cnt-audit").textContent = (al.active || []).length + " 告警";
    $("#top-build").textContent = "build " + (meta.build || "未注入") + (meta.build_go_version ? " · " + meta.build_go_version : "");
    $("#nav-build").textContent = "build " + (meta.build || "未注入") + " · live";
    $("#top-refresh").textContent = meta.generated_at ? "刷新 " + meta.generated_at.slice(11, 19) + "Z" : "未刷新";
    $("#live-via").textContent = num(meta.upstream_count) + " 台上游 · 自动刷新 " + (REFRESH_MS / 1000) + "s（overview/traffic/audit）";
  }
  function theme() {
    var saved = null;
    try { saved = localStorage.getItem("a2a.admin.theme"); } catch (e) { /* 隐私模式：忽略 */ }
    if (saved) document.documentElement.setAttribute("data-theme", saved);
    $("#theme-btn").onclick = function () {
      var cur = document.documentElement.getAttribute("data-theme");
      var next = cur === "dark" ? "light" : cur === "light" ? "" : "dark";
      if (next) document.documentElement.setAttribute("data-theme", next); else document.documentElement.removeAttribute("data-theme");
      try { if (next) localStorage.setItem("a2a.admin.theme", next); else localStorage.removeItem("a2a.admin.theme"); } catch (e) { /* noop */ }
    };
  }

  /* ---------------------------------------------------------------- 启动 */
  document.addEventListener("click", function (ev) {
    var el = ev.target.closest("[data-act]");
    if (el) { handleAction(el.getAttribute("data-act"), el); return; }
  });
  $("#drawer-close").onclick = closeDrawer;
  $("#scrim").onclick = function () { closeDrawer(); closeDialog(); };
  $("#dialog-cancel").onclick = closeDialog;
  $("#dialog-ok").onclick = function () { var f = pending; closeDialog(); if (f) f(); };
  $("#hamb").onclick = function () { $("#nav").classList.toggle("open"); };
  $("#refresh-btn").onclick = function () { refreshAll({}); };
  $("#lock-form").addEventListener("submit", submitKey);
  document.addEventListener("keydown", function (ev) { if (ev.key === "Escape") { closeDrawer(); closeDialog(); } });
  window.addEventListener("hashchange", route);

  theme();
  state.page = hashPage(); // 深链 #/audit 等在首帧就生效（refreshAll 渲染 state.page）
  if (window.A2AAdmin && window.A2AAdmin.key.has()) {
    // 已带密钥（本会话内刷新过）：直接首装，启动层在首次 render 后收掉。
    refreshAll({ initial: true });
  } else {
    // 无密钥：锁层就是首屏，启动层让位（不遮输入框）。
    lockUI("");
    $("#lock-key").focus();
  }
  bootDone = true;
  setInterval(autoRefresh, REFRESH_MS);
})();
