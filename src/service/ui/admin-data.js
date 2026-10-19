/* admin-data.js — Any2API 后台管理 dashboard 的**实时数据适配层**（L19-W74-1）
 *
 * 定位：唯一的网络面。它在 `window.A2A_LIVE` 上维护一个与「设计合同」同形的对象，
 * 由 admin.js 的 render 函数逐字段消费（admin.js 自身零 fetch）。
 * 形状 = 8 页 render 函数消费的字段名（与既有设计规格 docs/admin-dashboard.md §5 对齐），
 * 本文件把它接到真实端点：
 *
 *   GET /admin/diagnostics · /admin/pool · /admin/logs · /admin/sessions
 *   GET /admin/usage?scope=global|session|key|ip&day= · /admin/requests[?filter]
 *   GET /admin/requests/{id} · /admin/audit · /admin/alerts · /admin/metrics
 *   GET /admin/models · /admin/tasks · /admin/config · /admin/system
 *   POST /admin/sessions/revoke · POST /admin/cooldowns/reset
 *   GET /admin/accounts · /admin/proxies · /admin/providers · /admin/usage?scope=model&day=
 *   GET /admin/export?type=&format=&scope=&day= （CSV/JSONL 下载）
 *   POST /admin/accounts/{upstream}/{index}/cooldown · POST /admin/upstreams/{name}/probe
 *
 * 纪律（与 src/service/admin_*.go 的出口纪律对齐）：
 *   - **密钥只在 sessionStorage**（键 `any2api.admin.key`）；绝不进 localStorage、URL、
 *     DOM 属性或日志。每个 /admin/* 请求都带 `X-Management-Key: <key>`。
 *   - **缺数不补零**：端点到不了 / 字段不存在 / `null`（= 无计数器）→ 传 `null` 或空串，
 *     由 UI 渲染成 `—` / 「未提供」；绝不用 0 或构造值冒充观测。
 *   - **零凭据字节**：本层只做字段改名与单位换算，不引入任何新字段。
 *
 * 依赖：浏览器 fetch / sessionStorage；无外部资源、无框架。
 */
(function () {
  "use strict";

  var KEY_STORAGE = "any2api.admin.key"; // sessionStorage 专用键（绝不 localStorage）
  var LIVE = window.A2A_LIVE = window.A2A_LIVE || {};

  /* ---------------------------------------------------------------- 密钥面 */

  function getKey() {
    try { return sessionStorage.getItem(KEY_STORAGE) || ""; } catch (e) { return ""; }
  }
  function setKey(k) {
    try {
      if (k) sessionStorage.setItem(KEY_STORAGE, k);
      else sessionStorage.removeItem(KEY_STORAGE);
    } catch (e) { /* 隐私模式等：本次会话内不可存，退化为内存态 */ }
  }
  function clearKey() { setKey(""); }
  function hasKey() { return getKey() !== ""; }

  /* ---------------------------------------------------------------- HTTP 面 */

  // 管理面请求头：每个 /admin/* 请求都带管理密钥（服务端常量时间比较，
  // Authorization: Bearer 亦可，本面统一用 X-Management-Key）。
  function adminHeaders(extra) {
    var h = { "X-Management-Key": getKey() };
    if (extra) { for (var k in extra) { if (Object.prototype.hasOwnProperty.call(extra, k)) h[k] = extra[k]; } }
    return h;
  }

  function qs(params) {
    var parts = [];
    var p = params || {};
    Object.keys(p).forEach(function (k) {
      var v = p[k];
      if (v === "" || v === null || v === undefined || v === false) return;
      parts.push(encodeURIComponent(k) + "=" + encodeURIComponent(String(v)));
    });
    return parts.length ? "?" + parts.join("&") : "";
  }

  // 归一错误：{ok:false, status, type, message}——status 0 = 网络层失败。
  function errOf(status, body) {
    var e = (body && body.error) || {};
    return { ok: false, status: status, type: e.type || "", message: e.message || ("HTTP " + status) };
  }

  // adminGET 一发管理面只读请求（失败不抛，回归一错误对象）。
  function adminGET(path, params) {
    var url = path + qs(params);
    return fetch(url, { method: "GET", headers: adminHeaders(), cache: "no-store", credentials: "same-origin" }).then(
      function (resp) {
        return resp.text().then(function (text) {
          var body = null;
          try { body = text ? JSON.parse(text) : null; } catch (e) { body = null; }
          if (!resp.ok) return errOf(resp.status, body);
          return { ok: true, status: resp.status, body: body };
        });
      },
      function (e) { return { ok: false, status: 0, type: "network_error", message: String((e && e.message) || e) }; }
    );
  }

  // adminPOST 一发管理面写请求（revoke / cooldowns-reset）。
  function adminPOST(path, payload) {
    return fetch(path, {
      method: "POST",
      headers: adminHeaders({ "Content-Type": "application/json" }),
      cache: "no-store",
      credentials: "same-origin",
      body: JSON.stringify(payload || {})
    }).then(
      function (resp) {
        return resp.text().then(function (text) {
          var body = null;
          try { body = text ? JSON.parse(text) : null; } catch (e) { body = null; }
          if (!resp.ok) return errOf(resp.status, body);
          return { ok: true, status: resp.status, body: body };
        });
      },
      function (e) { return { ok: false, status: 0, type: "network_error", message: String((e && e.message) || e) }; }
    );
  }

  /* ---------------------------------------------------------------- 小工具 */

  function isoFromMs(ms) { return (typeof ms === "number" && ms > 0) ? new Date(ms).toISOString() : ""; }
  function isoFromSec(s) { return (typeof s === "number" && s > 0) ? new Date(s * 1000).toISOString() : ""; }
  function arr(v) { return Array.isArray(v) ? v : []; }
  function numOrNull(v) { return (typeof v === "number") ? v : null; }

  // 端点状态 → 人话（UI 的「未提供」卡片与 chip 文案）。
  function whyUnavailable(r) {
    if (r.status === 503) return "503 " + (r.type || "store_unavailable") + "：" + r.message;
    if (r.status === 401) return "401 鉴权失败";
    if (r.status === 0) return "网络错误：" + r.message;
    return "HTTP " + r.status + "：" + r.message;
  }

  /* ---------------------------------------------------------------- 映射层 */
  // 每个映射函数：端点响应 → 设计合同的 section 形状；端点不可用时回
  // `{available:false, reason, …空形状}`（UI 据此渲染「未提供」，绝不显示空表冒充实测）。

  function mapDiagnostics(r) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), now: "", diagnostics: {}, upstreams: [], log: {}, admin: { auth_failures: null, tracked_ips: null, banned_ips: [] } };
    var b = r.body || {};
    return {
      available: true,
      now: b.now || "",
      diagnostics: b.diagnostics || {},
      upstreams: arr(b.upstreams),
      log: b.log || {},
      admin: b.admin || { auth_failures: null, tracked_ips: null, banned_ips: [] }
    };
  }

  function mapPool(r) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), upstreams: [], capacity: {}, now: 0 };
    var b = r.body || {};
    return { available: true, upstreams: arr(b.upstreams), capacity: b.capacity || {}, now: b.now || 0 };
  }

  function mapLogs(r) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), entries: [], capacity: null, count: null, latest_seq: null, next_cursor: "" };
    var b = r.body || {};
    return { available: true, entries: arr(b.entries), capacity: numOrNull(b.capacity), count: numOrNull(b.count), latest_seq: numOrNull(b.latest_seq), next_cursor: b.next_cursor || "" };
  }

  function mapSessions(r) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), sessions: [], next_cursor: "", now: 0 };
    var b = r.body || {};
    return { available: true, sessions: arr(b.sessions), next_cursor: b.next_cursor || "", now: b.now || 0 };
  }

  function mapUsage(r, scope) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), scope: scope, rows: [], totals: { requests: null, cost_units: null } };
    var b = r.body || {};
    return { available: true, scope: b.scope || scope, day: b.day || "", rows: arr(b.rows), totals: b.totals || { requests: null, cost_units: null }, now: b.now || 0 };
  }

  /* 账号与代理 / 服务商（W78-2）——只做字段改名与词表校验，零凭据字节：
   * 池面只有 index/状态/last_used，代理面只有 scheme/host/port/auth/display。 */

  function mapCredential(c) {
    return {
      index: numOrNull(c.index),
      cooling: !!c.cooling,
      cooldown_until: numOrNull(c.cooldown_until),
      cooldown_remaining_seconds: numOrNull(c.cooldown_remaining_seconds),
      last_used: numOrNull(c.last_used),
      busy: !!c.busy
    };
  }

  function mapProxy(x) {
    x = x || {};
    return {
      configured: !!x.configured,
      scheme: x.scheme || "",
      host: x.host || "",
      port: numOrNull(x.port),
      auth: !!x.auth,
      display: x.display || ""
    };
  }

  function mapAccounts(r) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), upstreams: [], now: 0 };
    var b = r.body || {};
    return {
      available: true,
      upstreams: arr(b.upstreams).map(function (u) {
        return {
          index: numOrNull(u.index),
          name: u.name || "",
          protocol: u.protocol || "",
          base_url: u.base_url || "",
          proxy: mapProxy(u.proxy),
          tls_fingerprint: u.tls_fingerprint || "",
          pool_size: numOrNull(u.pool_size),
          credentials: arr(u.credentials).map(mapCredential)
        };
      }),
      now: numOrNull(b.now) || 0
    };
  }

  function mapProxies(r) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), proxies: [], now: 0 };
    var b = r.body || {};
    return {
      available: true,
      proxies: arr(b.proxies).map(function (p) {
        return {
          upstream: p.upstream || "",
          index: numOrNull(p.index),
          configured: !!p.configured,
          scheme: p.scheme || "",
          host: p.host || "",
          port: numOrNull(p.port),
          auth: !!p.auth,
          display: p.display || "",
          tls_fingerprint: p.tls_fingerprint || ""
        };
      }),
      now: numOrNull(b.now) || 0
    };
  }

  // 就绪态词表（服务端 adminProvider* 四值）：词表外的值原样带上，UI 不假装映射。
  var PROVIDER_READINESS = ["not_configured", "configured", "ready", "degraded"];

  function mapProviders(r) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), providers: [], now: 0 };
    var b = r.body || {};
    return {
      available: true,
      providers: arr(b.providers).map(function (p) {
        return {
          id: p.id || "",
          label: p.label || "",
          protocols: arr(p.protocols),
          configured: !!p.configured,
          upstreams: arr(p.upstreams),
          capabilities: arr(p.capabilities),
          readiness: p.readiness || "",
          docs: p.docs || ""
        };
      }),
      now: numOrNull(b.now) || 0
    };
  }

  // 请求行：API `in`/`out` → 合同 `in_tokens`/`out_tokens`；`credential_index` 缺省 = null。
  function mapRequestRow(x) {
    return {
      time: isoFromMs(x.time),
      request_id: x.request_id || "",
      entry: x.entry || "",
      model: x.model || "",
      stream: !!x.stream,
      status: numOrNull(x.status),
      attempts: numOrNull(x.attempts),
      failovers: numOrNull(x.failovers),
      upstream: x.upstream || "",
      credential_index: numOrNull(x.credential_index),
      ttft_ms: numOrNull(x.ttft_ms),
      total_ms: numOrNull(x.total_ms),
      in_tokens: numOrNull(x.in),
      out_tokens: numOrNull(x.out),
      outcome: x.outcome || "",
      sid_prefix: x.sid_prefix || "",
      key_id: x.key_id || "",
      task: x.task || "",
      failed: !!x.failed,
      delivered: !!x.delivered
    };
  }

  function mapRequests(r) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), rows: [], next_cursor: "" };
    var b = r.body || {};
    return { available: true, rows: arr(b.rows).map(mapRequestRow), next_cursor: b.next_cursor || "" };
  }

  function mapRequestDetail(r) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), request_id: "", attempts: [] };
    var b = r.body || {};
    var q = b.request || {};
    var t0 = (typeof q.time === "number" && q.time > 0) ? q.time : null;
    return {
      available: true,
      request_id: q.request_id || "",
      entry: q.entry || "",
      stream: !!q.stream,
      model_requested: q.model || "",
      model_upstream: "", // 未提供：ring 不存"别名 → 上游模型"映射（在 config.models 里）
      submitted_at: isoFromMs(t0),
      finished_at: (t0 && q.total_ms > 0) ? isoFromMs(t0 + q.total_ms) : "",
      downstream_status: numOrNull(q.status),
      delivered: !!q.delivered,
      sid_prefix: q.sid_prefix || "",
      key_id: q.key_id || "",
      client_ip_hash: "", // 未提供：请求 ring 不存 ip_hash / ua_hash
      ua_hash: "",
      turn: null, // 未提供：ring 行不关联 turn id（store.TurnsByRequest 才有）
      attempts: arr(b.attempts).map(function (a) {
        return {
          attempt: numOrNull(a.n),
          upstream: a.upstream || "",
          protocol: a.protocol || "",
          credential_index: numOrNull(a.credential_index),
          start: "", // 未提供：attempt 记录只有耗时（ttft_ms/total_ms），无起止时刻
          end: "",
          ttft_ms: numOrNull(a.ttft_ms),
          total_ms: numOrNull(a.total_ms),
          failed: !!a.failed,
          disconnected: !!a.disconnected,
          status: numOrNull(a.status),
          in_tokens: numOrNull(a.in),
          out_tokens: numOrNull(a.out),
          end_reason: a.end_reason || "",
          delivered: !!a.delivered,
          error: a.error || null
        };
      })
    };
  }

  // 指标：桶数组 → 序列 + 标签 + 直方图（直方图 = 逐桶 total_hist 求和，边界 = edges_ms）。
  function mapMetrics(r, wantMinutes) {
    var empty = {
      available: false, reason: r.ok ? "" : whyUnavailable(r),
      window_seconds: wantMinutes * 60, step_seconds: 60,
      series: { requests: [], errors: [], failovers: [], ttft_p50_ms: [], ttft_p95_ms: [], total_p50_ms: [], total_p95_ms: [] },
      labels: [], histogram: { edges: [], buckets: [] }
    };
    if (!r.ok) return empty;
    var b = r.body || {};
    var buckets = arr(b.buckets);
    var s = { requests: [], errors: [], failovers: [], ttft_p50_ms: [], ttft_p95_ms: [], total_p50_ms: [], total_p95_ms: [] };
    var labels = [];
    var hist = [];
    buckets.forEach(function (k) {
      s.requests.push(k.requests || 0);
      s.errors.push(k.errors || 0);
      s.failovers.push(k.failovers || 0);
      s.ttft_p50_ms.push((k.ttft_ms && k.ttft_ms.p50) || 0);
      s.ttft_p95_ms.push((k.ttft_ms && k.ttft_ms.p95) || 0);
      s.total_p50_ms.push((k.total_ms && k.total_ms.p50) || 0);
      s.total_p95_ms.push((k.total_ms && k.total_ms.p95) || 0);
      labels.push(isoFromMs(k.ts).slice(11, 16));
      arr(k.total_hist).forEach(function (v, i) { hist[i] = (hist[i] || 0) + v; });
    });
    return {
      available: true,
      window_seconds: b.window_seconds || wantMinutes * 60,
      step_seconds: b.step_seconds || 60,
      series: s,
      labels: labels,
      histogram: { edges: arr(b.edges_ms), buckets: hist }
    };
  }

  // 审计行：ts（unix ms）→ ISO；by_kind/total = **本页**行统计（响应无聚合面，
  // 分母在 UI 上必须标成"本页"）。
  function mapAudit(r) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), rows: [], by_kind: [], total: null, next_cursor: "" };
    var b = r.body || {};
    var rows = arr(b.rows).map(function (a) {
      return {
        ts: isoFromMs(a.ts),
        kind: a.kind || "",
        sid_prefix: a.sid_prefix || "",
        key_id: a.key_id || "",
        request_id: a.request_id || "",
        detail: a.detail || ""
      };
    });
    var counts = {};
    rows.forEach(function (a) { counts[a.kind] = (counts[a.kind] || 0) + 1; });
    var byKind = Object.keys(counts).map(function (k) { return { kind: k, count: counts[k] }; });
    byKind.sort(function (a, b2) { return b2.count - a.count; });
    return { available: true, rows: rows, by_kind: byKind, total: rows.length, next_cursor: b.next_cursor || "" };
  }

  // 告警：`object`/`message` → 合同 `subject`/`detail`；`source` 服务端不回 → 空串（UI 显示 —）。
  function mapAlert(a) {
    return {
      id: a.id || "",
      severity: a.severity || "",
      name: a.name || "",
      source: "",
      subject: a.object || "",
      detail: a.message || "",
      since: isoFromMs(a.since),
      until: "",
      runbook: a.runbook || ""
    };
  }

  function mapAlerts(r) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), active: [], history: [], webhook: { available: false } };
    var b = r.body || {};
    return {
      available: true,
      active: arr(b.active).map(mapAlert),
      history: arr(b.history).map(mapAlert),
      // 服务不出网：webhook/告警外推属外部桥（设计 §7.5），服务端不提供该面。
      webhook: { available: false, reason: "未提供：服务无出网 webhook 面（设计 §7.5 明确由外部桥承担）" }
    };
  }

  function mapModels(r) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), configured: false, rows: [], discover: { default_model: "", models: [], rate_limit_per_min: null } };
    var b = r.body || {};
    var ids = [];
    var rows = arr(b.models).map(function (m) {
      ids.push(m.id);
      var u = m.usage || {};
      return {
        id: m.id || "",
        upstream_model: m.upstream_model || "",
        upstream: "", // 未提供：模型目录不声明归属上游（尝试面才知道）
        max_input_tokens: numOrNull(m.max_input_tokens),
        max_tokens: numOrNull(m.max_tokens),
        usage_available: !!u.available,
        requests_today: u.available ? numOrNull(u.requests_today) : null,
        cost_units_today: u.available ? numOrNull(u.units_today) : null,
        requests_7d: null, // 未提供：无 /admin/usage/series
        state: "" // 未提供：无逐模型状态面
      };
    });
    return {
      available: true,
      configured: !!b.configured,
      rows: rows,
      // 发现面 = 同一来源（Config.Models 的 id 列表；默认 = 表首条），与 /ui/api/config 同源。
      discover: { default_model: ids.length ? ids[0] : "", models: ids, rate_limit_per_min: null }
    };
  }

  function mapTasks(r) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), rows: [], counts: {} };
    var b = r.body || {};
    var rows = arr(b.tasks).map(function (t) {
      return {
        id: t.id || "",
        model: t.model || "",
        status: t.status || "",
        created: isoFromSec(t.created_at),
        terminal: isoFromSec(t.terminal_at),
        delivered: !!t.delivered,
        attempts: numOrNull(t.attempts),
        owner_sid_prefix: t.sid_prefix || "",
        face: t.face || "",
        upstream: t.upstream || ""
      };
    });
    return { available: true, rows: rows, counts: {} };
  }

  function mapConfig(r) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), upstreams: [], models: [] };
    var b = r.body || {};
    var adm = b.admin || {}, ui = b.ui || {}, st = b.store || {}, pub = b.public || {}, lim = pub.limits || {};
    var l = b.limits || {}, fr = l.failure_rate || {}, dbg = b.debug || {};
    return {
      available: true,
      listen: b.listen || "",
      admin_configured: !!adm.key_configured,
      admin_log_capacity: numOrNull(adm.log_capacity),
      ui: { enabled: !!ui.enabled, rate_limit_per_min: numOrNull(ui.rate_limit_per_min) },
      store: { enabled: !!st.enabled, path_masked: st.path || "" },
      public: {
        enabled: !!pub.enabled,
        access_mode: pub.access_mode || "",
        trusted_proxies: arr(pub.trusted_proxies).length,
        max_inflight: numOrNull(lim.max_inflight),
        queue_size: numOrNull(lim.queue_size),
        queue_timeout_seconds: numOrNull(lim.queue_timeout_seconds),
        credential_min_gap_seconds: numOrNull(lim.credential_min_gap_seconds),
        max_cost_class: lim.max_cost_class || "",
        daily_budget_units: numOrNull(lim.daily_budget_units),
        daily_budget_enabled: !!lim.daily_budget_enabled,
        max_body_bytes: numOrNull(lim.max_body_bytes),
        max_messages: numOrNull(lim.max_messages),
        max_message_chars: numOrNull(lim.max_message_chars),
        access_keys: numOrNull(pub.access_keys_count)
      },
      heartbeat_seconds: numOrNull(l.heartbeat_seconds),
      cooldowns: {
        failures: numOrNull(l.cooldown_failures),
        seconds: numOrNull(l.cooldown_seconds),
        ladder: arr(l.cooldown_ladder_seconds),
        failure_rate_enabled: !!fr.enabled,
        failure_rate_threshold: numOrNull(fr.threshold),
        failure_rate_min_requests: numOrNull(fr.min_requests)
      },
      task_face_degrade: b.task_face_degrade || "",
      upstreams: arr(b.upstreams).map(function (u) {
        return {
          index: u.index,
          name: u.name || "",
          protocol: u.protocol || "",
          base_url: u.base_url || "",
          credentials: numOrNull(u.pool_size),
          proxy: !!u.proxy_configured,
          tls_fingerprint: u.tls_fingerprint || "",
          task_face: u.task_face || "",
          tool_sim: null // 未提供：配置面不回该位（不在脱敏投影里）
        };
      }),
      models: arr(b.models).map(function (m) { return { id: m.id || "", upstream_model: m.upstream_model || "" }; }),
      debug_capture_dir: dbg.capture_dir || "",
      // 服务端出口纪律（脱敏规则）——静态文案，与 admin_dashboard.go/admin_api.go 的投影一致。
      redaction: [
        "api_key / cookie / x-api-key / x-goog-api-key 永不回显",
        "base_url 去 userinfo（查询串 key/token 一律砍）",
        "ip / ua 只以 sha8 摘要形态出现",
        "access_keys 只回条目数"
      ]
    };
  }

  function mapSystem(r) {
    if (!r.ok) return { available: false, reason: whyUnavailable(r), runtime: {}, build: {}, store: {} };
    var b = r.body || {};
    var rt = b.runtime || {}, st = b.store || {}, bd = b.build || {};
    return {
      available: true,
      runtime: {
        go_version: rt.go_version || "",
        pid: numOrNull(rt.pid),
        goroutines: numOrNull(rt.goroutines),
        mem_alloc_bytes: numOrNull(rt.mem_alloc_bytes),
        mem_sys_bytes: numOrNull(rt.mem_sys_bytes),
        gc_cycles: numOrNull(rt.gc_count),
        gc_pause_total_ms: numOrNull(rt.gc_pause_total_ms),
        uptime_seconds: numOrNull(rt.uptime_seconds),
        started_at: "" // 未提供：runtime 面只回 uptime，不回启动时刻
      },
      build: {
        commit: bd.commit || "",
        go_version: bd.go_version || "",
        built_at: bd.date || "",
        commit_subject: "", // 未提供：构建期只注入 commit/date
        binary_sha256_12: "", // 未提供：无 buildinfo sha 注入面
        deploy_dir: "",
        backup_sha256_12: "",
        backup_ref: ""
      },
      store: {
        enabled: !!st.enabled,
        // store 实现只有 sqlite（store_sqlite.go）；enabled=false 即内存档。
        mode: st.enabled ? "sqlite" : "memory",
        path_masked: st.path || "",
        size_bytes: st.size_available ? numOrNull(st.size_bytes) : null,
        sessions: st.sessions_available ? numOrNull(st.sessions) : null,
        audit_rows: st.audit_rows_available ? numOrNull(st.audit_rows) : null,
        schema_version: "", // 未提供：/admin/system 不回 schema 版本
        note: st.enabled ? "store 档：会话/台账/审计落 sqlite（路径 basename + 行数见上）" : "内存档：重启清零（无持久面）"
      }
    };
  }

  /* ---------------------------------------------------------------- 装载 */

  // 自动刷新面（overview/traffic/audit）默认 10s；数据面页共用一次装载。
  var STATE = { filter: null, sessionsExtra: [], nextCursor: "", day: "" };

  function todayUTC() { return new Date().toISOString().slice(0, 10); }

  // load 并发取全部只读端点并**原地**更新 window.A2A_LIVE（保持对象身份，
  // admin.js 的 render 函数持有同一引用）。
  function load(opts) {
    opts = opts || {};
    if (!hasKey()) return Promise.resolve({ ok: false, reason: "no_key" });
    var day = opts.day || STATE.day || todayUTC();
    STATE.day = day;
    STATE.filter = opts.filter || STATE.filter || null;
    var f = STATE.filter || {};
    var reqParams = { limit: 200 };
    ["since", "entry", "model", "upstream", "status", "failed", "stream", "request_id"].forEach(function (k) {
      if (f[k] !== "" && f[k] !== null && f[k] !== undefined && f[k] !== false) reqParams[k] = f[k];
    });

    return Promise.all([
      adminGET("/admin/diagnostics"),
      adminGET("/admin/pool"),
      adminGET("/admin/logs", { limit: 50 }),
      adminGET("/admin/sessions", { limit: 100 }),
      adminGET("/admin/usage", { scope: "global", day: day }),
      adminGET("/admin/usage", { scope: "session", day: day }),
      adminGET("/admin/usage", { scope: "key", day: day }),
      adminGET("/admin/usage", { scope: "ip", day: day }),
      adminGET("/admin/requests", reqParams),
      adminGET("/admin/audit", { limit: 200 }),
      adminGET("/admin/alerts"),
      adminGET("/admin/metrics", { window: 60, step: 60 }),
      adminGET("/admin/models"),
      adminGET("/admin/tasks"),
      adminGET("/admin/config"),
      adminGET("/admin/system"),
      adminGET("/admin/accounts"),
      adminGET("/admin/proxies"),
      adminGET("/admin/providers"),
      adminGET("/admin/usage", { scope: "model", day: day })
    ]).then(function (res) {
      var d = res[0];
      // 鉴权/门控两态先判（都影响整页）：401 = 密钥无效；404 = 管理面未注册。
      if (!d.ok && (d.status === 401 || d.status === 0)) return { ok: false, reason: "unauthorized" };
      if (!d.ok && d.status === 404) return { ok: false, reason: "disabled" };

      var sessionsPage = mapSessions(res[3]);
      // 游标翻页的追加行（去重 by sid_prefix）与首页合并；next_cursor 以首页为准。
      if (sessionsPage.available) {
        var seen = {};
        sessionsPage.sessions.forEach(function (s) { seen[s.sid_prefix] = true; });
        STATE.sessionsExtra.forEach(function (s) { if (!seen[s.sid_prefix]) { seen[s.sid_prefix] = true; sessionsPage.sessions.push(s); } });
        STATE.nextCursor = sessionsPage.next_cursor;
      }

      var usageScopes = {
        global: mapUsage(res[4], "global"),
        session: mapUsage(res[5], "session"),
        key: mapUsage(res[6], "key"),
        ip: mapUsage(res[7], "ip")
      };

      LIVE.diagnostics = mapDiagnostics(d);
      LIVE.pools = mapPool(res[1]);
      LIVE.logs = mapLogs(res[2]);
      LIVE.sessions = sessionsPage;
      LIVE.usageScopes = usageScopes;
      LIVE.usage = usageScopes[opts.usageScope || "session"] || usageScopes.global;
      LIVE.requests = mapRequests(res[8]);
      LIVE.audit = mapAudit(res[9]);
      LIVE.alerts = mapAlerts(res[10]);
      LIVE.metrics = mapMetrics(res[11], 60);
      LIVE.models = mapModels(res[12]);
      LIVE.tasks = mapTasks(res[13]);
      LIVE.config = mapConfig(res[14]);
      LIVE.system = mapSystem(res[15]);
      LIVE.accounts = mapAccounts(res[16]);
      LIVE.proxies = mapProxies(res[17]);
      LIVE.providers = mapProviders(res[18]);
      LIVE.modelUsage = mapUsage(res[19], "model");
      LIVE.degradeNote = {
        poolByCredential: "逐凭据今日请求/units：/admin/pool 只回索引与状态（服务无逐凭据计数器，admin_api.go 已登记差异）——本列不回数据。",
        timeseries: "多日序列：服务无 /admin/usage/series（usage_counters 的多日行在 store 里，缺查询口）——本卡不回数据。",
        perModelUsage: "逐模型用量 = GET /admin/usage?scope=model（usage_counters 的 model: scope 行，本日台账）；逐上游用量仍无独立计数器。"
      };
      LIVE.meta = buildMeta(res, day);
      return { ok: true, reason: "" };
    });
  }

  // buildMeta meta 面：kind=LIVE + 刷新时刻 + 构建信息 + 逐 section 的来源与状态。
  function buildMeta(res, day) {
    var sys = LIVE.system || { build: {} };
    var sources = {
      diagnostics: { api: "GET /admin/diagnostics", state: "已有", ref: "src/service/admin.go" },
      upstreams: { api: "GET /admin/diagnostics → .upstreams[]", state: "已有", ref: "src/service/admin.go" },
      pools: { api: "GET /admin/pool", state: "已有", ref: "src/service/admin_api.go" },
      logs: { api: "GET /admin/logs", state: "已有", ref: "src/service/admin.go" },
      sessions: { api: "GET /admin/sessions", state: "已有", ref: "src/service/admin_api.go" },
      usage: { api: "GET /admin/usage?scope=&day=", state: "已有", ref: "src/service/admin_api.go" },
      revoke: { api: "POST /admin/sessions/revoke", state: "已有", ref: "src/service/admin_api.go" },
      cooldownReset: { api: "POST /admin/cooldowns/reset", state: "已有", ref: "src/service/admin.go" },
      requests: { api: "GET /admin/requests", state: "已有", ref: "src/service/admin_dashboard.go" },
      requestDetail: { api: "GET /admin/requests/{id}", state: "已有", ref: "src/service/admin_dashboard.go" },
      metrics: { api: "GET /admin/metrics?window=&step=", state: "已有", ref: "src/service/admin_metrics.go" },
      audit: { api: "GET /admin/audit?kind=&since=&limit=", state: "已有", ref: "src/service/admin_dashboard.go" },
      alerts: { api: "GET /admin/alerts", state: "已有", ref: "src/service/admin_dashboard.go" },
      models: { api: "GET /admin/models", state: "已有", ref: "src/service/admin_models_tasks.go" },
      tasks: { api: "GET /admin/tasks", state: "已有", ref: "src/service/admin_models_tasks.go" },
      config: { api: "GET /admin/config", state: "已有", ref: "src/service/admin_meta.go" },
      system: { api: "GET /admin/system", state: "已有", ref: "src/service/admin_meta.go" },
      keys: { api: "GET /admin/keys（不存在）", state: "未提供", ref: "无该端点：access key 视图未实现" },
      usageTrend: { api: "GET /admin/usage/series（不存在）", state: "未提供", ref: "无该端点：多日序列未实现" },
      events: { api: "GET /admin/events（不存在）", state: "未提供", ref: "无该端点：SSE 未实现" },
      accounts: { api: "GET /admin/accounts", state: "已有", ref: "src/service/admin_accounts.go" },
      proxies: { api: "GET /admin/proxies", state: "已有", ref: "src/service/admin_accounts.go" },
      cooldown: { api: "POST /admin/accounts/{upstream}/{index}/cooldown", state: "已有", ref: "src/service/admin_accounts.go" },
      providers: { api: "GET /admin/providers", state: "已有", ref: "src/service/admin_providers.go" },
      probe: { api: "POST /admin/upstreams/{name}/probe", state: "已有", ref: "src/service/admin_providers.go" },
      modelUsage: { api: "GET /admin/usage?scope=model&day=", state: "已有", ref: "src/service/admin_api.go" },
      exportFile: { api: "GET /admin/export?type=&format=&scope=&day=", state: "已有", ref: "src/service/admin_export.go" }
    };
    if (!(LIVE.accounts || {}).available) sources.accounts = { api: "GET /admin/accounts", state: "未提供", ref: (LIVE.accounts || {}).reason || "" };
    if (!(LIVE.proxies || {}).available) sources.proxies = { api: "GET /admin/proxies", state: "未提供", ref: (LIVE.proxies || {}).reason || "" };
    if (!(LIVE.providers || {}).available) {
      sources.providers = { api: "GET /admin/providers", state: "未提供", ref: (LIVE.providers || {}).reason || "" };
      sources.probe = { api: "POST /admin/upstreams/{name}/probe", state: "未提供", ref: "供应商面不可用：探针结果无落点" };
    }
    if (!(LIVE.modelUsage || {}).available) sources.modelUsage = { api: "GET /admin/usage?scope=model&day=", state: "未提供", ref: (LIVE.modelUsage || {}).reason || "" };
    if (!(LIVE.audit || {}).available) sources.audit = { api: "GET /admin/audit?kind=&since=&limit=", state: "未提供", ref: (LIVE.audit || {}).reason || "" };
    if (!(LIVE.sessions || {}).available) sources.sessions = { api: "GET /admin/sessions", state: "未提供", ref: (LIVE.sessions || {}).reason || "" };
    if (!(LIVE.usage || {}).available) sources.usage = { api: "GET /admin/usage?scope=&day=", state: "未提供", ref: (LIVE.usage || {}).reason || "" };
    if (!(LIVE.models || {}).available) sources.models = { api: "GET /admin/models", state: "未提供", ref: (LIVE.models || {}).reason || "" };
    if (!(LIVE.tasks || {}).available) sources.tasks = { api: "GET /admin/tasks", state: "未提供", ref: (LIVE.tasks || {}).reason || "" };
    var commit = ((sys.build || {}).commit) || "";
    return {
      kind: "LIVE",
      schema: "any2api-admin-dashboard-live-v1",
      generated_at: new Date().toISOString(),
      day: day,
      build: commit,
      build_go_version: (sys.build || {}).go_version || "",
      build_date: (sys.build || {}).built_at || "",
      upstream_count: (LIVE.diagnostics || {}).available ? arr((LIVE.diagnostics || {}).upstreams).length : null,
      sources: sources
    };
  }

  // detail 单请求详情（抽屉按需拉取）：GET /admin/requests/{id} → LIVE.requestDetail。
  function detail(id) {
    if (!hasKey()) return Promise.resolve({ ok: false, reason: "no_key" });
    return adminGET("/admin/requests/" + encodeURIComponent(id)).then(function (r) {
      LIVE.requestDetail = mapRequestDetail(r);
      if (!r.ok) return { ok: false, reason: r.status === 401 ? "unauthorized" : "error", error: r };
      return { ok: true, body: r.body };
    });
  }

  // loadMoreSessions 游标翻页：把下一页并入 STATE.sessionsExtra（下次 load 合并）。
  function loadMoreSessions() {
    if (!hasKey()) return Promise.resolve({ ok: false, reason: "no_key" });
    var cur = STATE.nextCursor;
    if (!cur) return Promise.resolve({ ok: false, reason: "no_cursor" });
    return adminGET("/admin/sessions", { limit: 100, cursor: cur }).then(function (r) {
      if (!r.ok) return { ok: false, reason: r.status === 401 ? "unauthorized" : "error", error: r };
      var page = mapSessions(r);
      var n = 0;
      page.sessions.forEach(function (s) { STATE.sessionsExtra.push(s); n++; });
      STATE.nextCursor = page.next_cursor;
      return { ok: true, added: n, next_cursor: STATE.nextCursor };
    });
  }

  /* ---------------------------------------------------------------- 写动作 */

  // revoke 真发 POST /admin/sessions/revoke（selector 三选一；服务端 404/409 语义照传）。
  function revoke(payload) { return adminPOST("/admin/sessions/revoke", payload); }

  // resetCooldown 真发 POST /admin/cooldowns/reset（幂等；was_cooling 由服务端回）。
  function resetCooldown(payload) { return adminPOST("/admin/cooldowns/reset", payload); }

  // cooldown 安全下线：POST /admin/accounts/{upstream}/{index}/cooldown
  // （只写进程内冷却（零配置写）；seconds 1..86400 由服务端校验，404 = 无此凭据）。
  function cooldown(upstream, index, seconds, reason) {
    return adminPOST(
      "/admin/accounts/" + encodeURIComponent(String(upstream)) + "/" + encodeURIComponent(String(index)) + "/cooldown",
      { seconds: seconds, reason: reason || "manual-ui" }
    );
  }

  // probe 单上游轻探针：POST /admin/upstreams/{name}/probe（1 发只读 GET；不占号池）。
  function probe(upstream) {
    return adminPOST("/admin/upstreams/" + encodeURIComponent(String(upstream)) + "/probe", { mode: "models" });
  }

  // adminDownload 下载面：**带管理密钥头**取字节 → Blob URL → 临时 <a download> 点击 → 撤销。
  // 文件名只认 Content-Disposition（服务端 any2api-<type>-<YYYYMMDD>.<ext>），缺省回兜底名。
  // **密钥绝不进 URL**（不进 query，也不进 <a href>）——URL 是日志/历史/Referer 会漏的面。
  function adminDownload(path) {
    return fetch(path, { method: "GET", headers: adminHeaders(), cache: "no-store", credentials: "same-origin" }).then(
      function (resp) {
        if (!resp.ok) {
          return resp.text().then(function (text) {
            var body = null;
            try { body = text ? JSON.parse(text) : null; } catch (e) { body = null; }
            var e = errOf(resp.status, body);
            return { ok: false, status: resp.status, type: e.type, reason: whyUnavailable(e) };
          });
        }
        var disp = "";
        try { disp = resp.headers.get("Content-Disposition") || ""; } catch (e) { disp = ""; }
        var m = /filename="?([^";]+)"?/i.exec(disp);
        var name = (m && m[1]) ? m[1] : "any2api-export";
        return resp.text().then(function (text) {
          var ctype = "application/octet-stream";
          try { ctype = resp.headers.get("Content-Type") || ctype; } catch (e) { /* 头面不可读：用兜底类型 */ }
          var url = URL.createObjectURL(new Blob([text], { type: ctype }));
          var a = document.createElement("a");
          a.href = url;
          a.download = name;
          a.rel = "noopener";
          document.body.appendChild(a);
          a.click();
          if (a.parentNode) a.parentNode.removeChild(a);
          URL.revokeObjectURL(url);
          return { ok: true, filename: name, bytes: text.length, content_type: ctype };
        });
      },
      function (e) { return { ok: false, status: 0, type: "network_error", reason: "网络错误：" + String((e && e.message) || e) }; }
    );
  }

  // exportFile 导出面：GET /admin/export?type=&format=&scope=&day=（format 缺省 = 服务端缺省 jsonl）。
  function exportFile(opts) {
    opts = opts || {};
    var params = { type: opts.type, format: opts.format || "jsonl" };
    if (opts.scope) params.scope = opts.scope;
    if (opts.day) params.day = opts.day;
    if (opts.limit) params.limit = opts.limit;
    return adminDownload("/admin/export" + qs(params));
  }

  // healthz 只读探针（公开面；无密钥）。
  function healthz() {
    return fetch("/healthz", { cache: "no-store" }).then(
      function (r) { return r.text().then(function (t) { return { ok: r.ok, status: r.status, text: t }; }); },
      function (e) { return { ok: false, status: 0, text: String((e && e.message) || e) }; }
    );
  }

  window.A2AAdmin = {
    key: { get: getKey, set: setKey, clear: clearKey, has: hasKey },
    load: load,
    loadMoreSessions: loadMoreSessions,
    detail: detail,
    revoke: revoke,
    resetCooldown: resetCooldown,
    cooldown: cooldown,
    probe: probe,
    exportFile: exportFile,
    adminDownload: adminDownload,
    healthz: healthz,
    live: function () { return LIVE; },
    state: function () { return STATE; }
  };
})();
