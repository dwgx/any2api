/* ============================================================================
 * Any2API Chat —— 前端逻辑 v2（零构建 SPA，无框架、无构建步骤）。
 * ----------------------------------------------------------------------------
 * 消费同源 OpenAI 兼容接口（契约不变，实读 src/service/relay.go / server.go）：
 *   POST /v1/chat/completions   体 {model?, messages:[{role,content}], stream:true}
 *     SSE 帧：data: {"choices":[{"index":0,"delta":{"content"|"reasoning_content"
 *              |"tool_calls":[…]},"finish_reason":…}]}；终止 data: [DONE]；
 *     保活 ": keepalive"（注释行）；流内错误 data: {"error":{…}}（即终态）。
 *   GET  /ui/api/config         {default_model, models, rate_limit_per_min}
 *     （缺失 → 回退 GET /v1/models；再失败 → 省略 model 字段）。
 *
 * UI v2 = W62 七域蒸馏整装（Claude Desktop 2.42304 设计语言）：
 *   壳/侧栏/消息/思维链/工具卡/输入区/浮层/订阅套餐；中文文案；主题双通道。
 * 会话数据默认保存在 localStorage（键 v2；自动迁移 v1）；服务端 /ui/api/sessions*
 * 可用时（P1-4）改走服务端同步：localStorage 仍是本地缓存 + 离线降级兜底，
 * pinned/order 仅本地保留。
 * 本文件由 app1.js + app2.js 拼接而成（见 tmp/lanes/L19-W63-1/build/）。
 * ========================================================================== */
(function () {
'use strict';

/* ---------- 常量 ---------- */
var LS = {
  sessions: 'any2api.ui.sessions.v2',
  active:   'any2api.ui.active.v2',
  model:    'any2api.ui.model.v2',
  theme:    'any2api.ui.theme.v2',
  density:  'any2api.ui.density.v2',
  motion:   'any2api.ui.motion.v2',
  sidebar:  'any2api.ui.sidebar.v2',
  width:    'any2api.ui.sidebarWidth.v2',
  order:    'any2api.ui.order.v2',
  plan:     'any2api.ui.plan.v2',
  effort:   'any2api.ui.effort.v2',
  syncSince:'any2api.ui.since.v1',
  synced:   'any2api.ui.synced.v1',
  outbox:   'any2api.ui.outbox.v1',
  v1: { sessions: 'any2api.ui.sessions.v1', active: 'any2api.ui.active.v1', model: 'any2api.ui.model.v1' }
};

/* 附件（Phase 1）：单文件 / 单条总量 / 数量上限对齐服务侧（20 MiB 原始字节；base64 后 < 32 MiB 入口体上限）。 */
var ATT_MAX_BYTES = 20 * 1024 * 1024;
var ATT_MAX_TOTAL = 20 * 1024 * 1024;
var ATT_MAX_COUNT = 10;
var ATT_TYPES = ['image/png', 'image/jpeg', 'image/gif', 'image/webp'];
var ATT_ACCEPT = ATT_TYPES.join(',');
var DB_NAME = 'any2api.ui.files.v1';
var DB_STORE = 'files';

/* 思考档位（线值 ↔ 中文标签）；默认不发送。 */
var EFFORTS = [
  { id: '', label: '默认', hint: '由服务端决定' },
  { id: 'low', label: '低', hint: '更快、更省' },
  { id: 'medium', label: '中', hint: '均衡（推荐）' },
  { id: 'high', label: '高', hint: '更深推理' },
  { id: 'xhigh', label: '超高', hint: '复杂任务' },
  { id: 'max', label: '最大', hint: '5× 以上使用量' }
];

var API_CHAT   = new URL('../v1/chat/completions', document.baseURI).href;
var API_CONFIG = new URL('api/config', document.baseURI).href;
var API_MODELS = new URL('../v1/models', document.baseURI).href;

var VERSION = 'v2.0.0';

/* ---------- 小工具 ---------- */
function $id(x) { return document.getElementById(x); }
function now() { return Date.now(); }
function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
    return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
  });
}
function uid() {
  return (window.crypto && crypto.randomUUID)
    ? crypto.randomUUID()
    : 's-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8);
}
function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
function titleOf(s) { return (s && s.title) || '新对话'; }
function utf8Len(s) {
  var n = 0;
  for (var i = 0; i < s.length; i++) {
    var c = s.charCodeAt(i);
    if (c < 0x80) n += 1;
    else if (c < 0x800) n += 2;
    else if (c >= 0xD800 && c <= 0xDBFF) { n += 4; i++; }
    else n += 3;
  }
  return n;
}
/* 时长（中文）：12 秒 / 1 分 20 秒 / 1 小时 5 分 */
function fmtDurCn(ms) {
  var s = Math.floor(ms / 1000);
  if (s < 60) return s + ' 秒';
  var m = Math.floor(s / 60), rs = s % 60;
  if (m < 60) return rs > 0 ? m + ' 分 ' + rs + ' 秒' : m + ' 分';
  var h = Math.floor(m / 60), rm = m % 60;
  return rm > 0 ? h + ' 小时 ' + rm + ' 分' : h + ' 小时';
}
/* 时长（narrow，状态行/计时）：12s / 1m 20s / 1h 5m */
function fmtDurNarrow(ms) {
  var s = Math.floor(ms / 1000);
  if (s < 60) return s + 's';
  var m = Math.floor(s / 60), rs = s % 60;
  if (m < 60) return rs > 0 ? m + 'm ' + rs + 's' : m + 'm';
  var h = Math.floor(m / 60), rm = m % 60;
  return rm > 0 ? h + 'h ' + rm + 'm' : h + 'h';
}
function fmtRelative(ts) {
  if (!ts) return '';
  var d = new Date(ts), n = new Date();
  var diff = n.getTime() - ts;
  if (diff < 60e3) return '刚刚';
  if (diff < 3600e3) return Math.floor(diff / 60e3) + ' 分钟前';
  if (d.toDateString() === n.toDateString()) {
    function pad(x) { return (x < 10 ? '0' : '') + x; }
    return '今天 ' + pad(d.getHours()) + ':' + pad(d.getMinutes());
  }
  if (d.getFullYear() === n.getFullYear()) return (d.getMonth() + 1) + ' 月 ' + d.getDate() + ' 日';
  return d.getFullYear() + ' 年 ' + (d.getMonth() + 1) + ' 月 ' + d.getDate() + ' 日';
}
function fmtSize(bytes) {
  if (!bytes && bytes !== 0) return '';
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / 1024 / 1024).toFixed(1) + ' MB';
}
function icon(name, cls) {
  return '<svg class="' + (cls || 'ic') + '"><use href="#i-' + name + '"/></svg>';
}

/* ---------- 全局状态 ---------- */
var state = {
  sessions: [],           // [{id,title,createdAt,updatedAt,model,pinned,messages:[…]}]
  activeId: null,
  order: [],              // 手动排序（会话 id 序列；先于 updatedAt 生效）
  models: [],
  defaultModel: '',
  pendingModel: '',
  streaming: null,        // {sessionId,msgIndex,controller,aborted,refs,…}
  plan: 'free',           // free | pro | ultra（本地演示偏好）
  theme: 'system',        // system | light | dark
  density: 'compact',     // compact | comfortable
  motion: 'full',         // full | reduced
  sidebarCollapsed: false,
  sidebarWidth: 288,
  rateLimitPerMin: 0,
  attachments: [],        // 待发送附件（读取完成后随请求发送）
  fileCache: new Map(),   // fileId → dataUrl（运行时缓存；绝不写入 localStorage）
  pendingEffort: '',      // 新会话的思考档位（localStorage 持久化）
  searchIndex: null,      // 惰性构建的消息全文索引
  editing: null,          // {sessionId,index} 编辑态
  paletteMode: 'quick',   // quick | search
  collapsedGroups: {},    // {pinned:bool, recents:bool}
  confirmAction: null,
  // P1-4 服务端会话同步（/ui/api/sessions*）；mode: unknown|server|local|offline
  sync: {
    mode: 'unknown', since: 0, outbox: [], chains: {}, busy: false, again: false,
    draining: false, pulling: false, timer: 0, forceFull: true, online: true, pulls: 0,
    authBlocked: false, tabId: '', lastPull: 0, lastError: ''
  }
};
var attSeq = 0, toastSeq = 0;

/* ---------- 持久化 ---------- */
function lsGet(k) { try { return localStorage.getItem(k); } catch (e) { return null; } }
function lsSet(k, v) { try { localStorage.setItem(k, v); } catch (e) {} }
function lsDel(k) { try { localStorage.removeItem(k); } catch (e) {} }
function ssGet(k) { try { return sessionStorage.getItem(k); } catch (e) { return null; } }
function ssSet(k, v) { try { sessionStorage.setItem(k, v); } catch (e) {} }

function loadState() {
  var raw = lsGet(LS.sessions);
  if (raw === null) raw = lsGet(LS.v1.sessions);   // v1 → v2 迁移（格式兼容）
  var sessionsRawMissing = (raw === null || raw === '');
  try {
    var arr = JSON.parse(raw || '[]');
    state.sessions = Array.isArray(arr) ? arr.filter(function (s) {
      return s && typeof s.id === 'string' && Array.isArray(s.messages);
    }) : [];
  } catch (e) { state.sessions = []; }
  /* 消息稳定 id（P1-4）：旧 localStorage 迁移时补生成 uuid；同时补 rev=0（未同步）。 */
  var idsGenerated = false;
  state.sessions.forEach(function (s) {
    if (typeof s.rev !== 'number') s.rev = 0;
    s.messages.forEach(function (m) {
      if (!m || typeof m !== 'object') return;
      if (typeof m.id !== 'string' || !m.id) { m.id = uid(); idsGenerated = true; }
    });
  });
  try {
    var o = JSON.parse(lsGet(LS.order) || '[]');
    state.order = Array.isArray(o) ? o.filter(function (x) { return typeof x === 'string'; }) : [];
  } catch (e) { state.order = []; }
  state.activeId = lsGet(LS.active) || lsGet(LS.v1.active) || null;
  if (state.activeId && !findSession(state.activeId)) state.activeId = null;
  state.pendingModel = lsGet(LS.model) || lsGet(LS.v1.model) || '';
  state.pendingEffort = lsGet(LS.effort) || '';
  state.theme = lsGet(LS.theme) || 'system';
  state.density = lsGet(LS.density) || 'compact';
  state.motion = lsGet(LS.motion) || 'full';
  state.sidebarCollapsed = lsGet(LS.sidebar) === '1';
  var w = parseInt(lsGet(LS.width) || '288', 10);
  state.sidebarWidth = (w >= 216 && w <= 420) ? w : 288;
  state.plan = lsGet(LS.plan) || 'free';
  /* 同步元数据（P1-4）。 */
  var since = parseInt(lsGet(LS.syncSince) || '0', 10);
  state.sync.since = (isFinite(since) && since > 0) ? since : 0;
  state.sync.outbox = [];
  try {
    var box = JSON.parse(lsGet(LS.outbox) || '[]');
    if (Array.isArray(box)) {
      state.sync.outbox = box.filter(function (op) {
        return op && typeof op.kind === 'string' && typeof op.sid === 'string';
      });
    }
  } catch (e) { state.sync.outbox = []; }
  state.sync.tabId = ssGet('any2api.ui.tab.v1') || uid();
  ssSet('any2api.ui.tab.v1', state.sync.tabId);
  /* 本地会话丢失（清空数据/换浏览器）但 since 还在：必须全量拉取，否则增量会漏。 */
  state.sync.forceFull = sessionsRawMissing || state.sessions.length === 0 || lsGet(LS.synced) !== '1';
  if (idsGenerated) saveSessions();
}
function saveSessions() {
  state.searchIndex = null;
  try { localStorage.setItem(LS.sessions, JSON.stringify(state.sessions)); }
  catch (e) {
    toast({ title: '本地存储空间不足', desc: '会话未能保存；请删除部分旧会话后重试。', variant: 'danger', key: 'quota' });
  }
}
function saveActive() { if (state.activeId) lsSet(LS.active, state.activeId); else lsDel(LS.active); }
function saveOrder() { lsSet(LS.order, JSON.stringify(state.order)); }
function findSession(id) {
  for (var i = 0; i < state.sessions.length; i++) if (state.sessions[i].id === id) return state.sessions[i];
  return null;
}
function activeSession() { return state.activeId ? findSession(state.activeId) : null; }

/* ---------- 排序（置顶 → 手动序 → 更新时间） ---------- */
function sortedSessions() {
  var idx = {};
  state.order.forEach(function (id, i) { idx[id] = i; });
  return state.sessions.slice().sort(function (a, b) {
    if (!!a.pinned !== !!b.pinned) return a.pinned ? -1 : 1;
    var ia = (a.id in idx) ? idx[a.id] : Infinity;
    var ib = (b.id in idx) ? idx[b.id] : Infinity;
    if (ia !== ib) return ia - ib;
    return (b.updatedAt || 0) - (a.updatedAt || 0);
  });
}
function touchOrder(id) {
  state.order = state.order.filter(function (x) { return x !== id; });
  state.order.unshift(id);
  saveOrder();
}
function moveSession(s, dir) {
  var list = sortedSessions();
  var i = list.indexOf(s), j = i + dir;
  if (i < 0 || j < 0 || j >= list.length) return;
  if (!!list[i].pinned !== !!list[j].pinned) return;   // 仅在同组内移动
  var seq = list.map(function (x) { return x.id; });
  var t = seq[i]; seq[i] = seq[j]; seq[j] = t;
  state.order = seq; saveOrder();
  renderSessionList();
}

/* ---------- 模型发现（/ui/api/config → /v1/models → 省略） ---------- */
function initConfig() {
  return fetch(API_CONFIG, { headers: { 'Accept': 'application/json' } })
    .then(function (r) { if (!r.ok) throw new Error('config ' + r.status); return r.json(); })
    .then(function (cfg) {
      var list = normalizeModels(cfg && cfg.models);
      var def = (cfg && typeof cfg.default_model === 'string') ? cfg.default_model : '';
      if (def && list.indexOf(def) === -1) list.unshift(def);
      state.rateLimitPerMin = (cfg && typeof cfg.rate_limit_per_min === 'number') ? cfg.rate_limit_per_min : 0;
      applyModels(list, def);
    })
    .catch(function () { return fallbackModels(); });
}
function fallbackModels() {
  return fetch(API_MODELS, { headers: { 'Accept': 'application/json' } })
    .then(function (r) { if (!r.ok) throw new Error('models ' + r.status); return r.json(); })
    .then(function (data) {
      var list = normalizeModels(data && data.data);
      applyModels(list, list.length ? list[0] : '');
    })
    .catch(function () { applyModels([], ''); });
}
function normalizeModels(src) {
  var out = [];
  if (!Array.isArray(src)) return out;
  src.forEach(function (m) {
    var id = typeof m === 'string' ? m : (m && typeof m.id === 'string' ? m.id : '');
    if (id && out.indexOf(id) === -1) out.push(id);
  });
  return out;
}
function applyModels(list, def) {
  state.models = list;
  state.defaultModel = def || '';
  updateModelUI();
  updateRateLimitUI();
}
function updateRateLimitUI() {
  var f = $id('rateLimitField');
  if (!f) return;
  if (state.rateLimitPerMin > 0) {
    f.hidden = false;
    $id('rateLimitDesc').textContent = '每分钟 ' + state.rateLimitPerMin + ' 次请求（服务端 per-IP 限流）';
  } else {
    f.hidden = true;
  }
}
function currentModel(session) {
  return (session && session.model) || state.pendingModel || state.defaultModel || '';
}
function currentEffort(session) {
  if (session) return session.effort || '';
  return state.pendingEffort || '';
}
function effortLabel(id) {
  for (var i = 0; i < EFFORTS.length; i++) if (EFFORTS[i].id === (id || '')) return EFFORTS[i].label;
  return '';
}
function updateModelUI() {
  var has = state.models.length > 0;
  var s = activeSession();
  var cur = currentModel(s);
  var ef = effortLabel(currentEffort(s));
  $id('modelPick').hidden = !has;
  if (cur) {
    $id('modelName').textContent = cur;
  }
  var efEl = $id('modelEffort');
  if (efEl) {
    efEl.hidden = !(cur && ef && ef !== '默认');
    efEl.textContent = ef && ef !== '默认' ? '· ' + ef : '';
  }
  var pill = $id('modelPillBtn');
  if (pill) pill.setAttribute('aria-label', '选择模型与思考档位' + (cur ? '（当前 ' + cur + (ef ? ' · ' + ef : '') + '）' : ''));
}
/* 档位条（W80-1）：顶部横向 6 档，每档 label + hint；条内横向滚动，
 * 选中项在打开时与键盘移动时滚入可视区（窄屏由 CSS 转 3×2 网格）。 */
function revealEffort(menu) {
  var strip = menu.querySelector('.effort-strip');
  if (!strip) return;
  var sel = strip.querySelector('.effort-opt[aria-checked="true"]') || strip.querySelector('.effort-opt');
  if (!sel) return;
  if (sel.offsetLeft < strip.scrollLeft) strip.scrollLeft = Math.max(0, sel.offsetLeft - 8);
  else if (sel.offsetLeft + sel.offsetWidth > strip.scrollLeft + strip.clientWidth) {
    strip.scrollLeft = sel.offsetLeft + sel.offsetWidth - strip.clientWidth + 8;
  }
}
function renderModelMenu() {
  var menu = $id('modelMenu');
  var s = activeSession();
  var cur = currentModel(s);
  var curEff = currentEffort(s);
  var html =
    '<div class="menu-section-label" id="effortSectionLabel">思考档位</div>' +
    '<div class="effort-strip" role="group" aria-labelledby="effortSectionLabel">' +
    EFFORTS.map(function (e) {
      var on = e.id === curEff;
      return '<button class="effort-opt" role="menuitemradio" type="button" data-effort="' + esc(e.id) + '"' +
        ' aria-checked="' + (on ? 'true' : 'false') + '" aria-label="' + esc(e.label + '：' + e.hint) + '">' +
        '<span class="eo-head"><span class="eo-label">' + esc(e.label) + '</span>' +
        (on ? icon('check', 'ic-sm') : '') + '</span>' +
        '<span class="eo-hint">' + esc(e.hint) + '</span></button>';
    }).join('') + '</div>' +
    '<div class="menu-sep" role="separator"></div>' +
    '<div class="menu-section-label">模型</div>' +
    state.models.map(function (m) {
      return '<button class="menu-item" role="menuitem" type="button" data-model="' + esc(m) + '">' +
        '<span class="mi-icon">' + (m === cur ? icon('check') : '') + '</span>' +
        '<span class="mi-label">' + esc(m) + '</span></button>';
    }).join('') +
    '<div class="menu-foot">档位越高，思考 token 与耗时越高；仅对支持思考的模型生效，以服务端/上游支持为准。</div>';
  menu.innerHTML = html;
  menu.querySelectorAll('[data-model]').forEach(function (b) {
    b.addEventListener('click', function () { pickModel(b.dataset.model); });
  });
  menu.querySelectorAll('[data-effort]').forEach(function (b) {
    b.addEventListener('click', function () { pickEffort(b.dataset.effort); });
  });
  revealEffort(menu);
}
function pickModel(m) {
  var s = activeSession();
  if (s) { s.model = m; saveSessions(); syncEnqueue({ kind: 'patch', sid: s.id }); }
  else { state.pendingModel = m; lsSet(LS.model, m); }
  updateModelUI(); closeTopOverlay();
}
function pickEffort(id) {
  var s = activeSession();
  if (s) { s.effort = id || ''; saveSessions(); }
  else { state.pendingEffort = id || ''; lsSet(LS.effort, id || ''); }
  updateModelUI();
  closeTopOverlay();
  if (id === 'max') toast({ title: '已选择「最大」档位', desc: '该档位消耗更高（5× 以上使用量）。', variant: 'neutral', key: 'effort-max' });
}

/* ---------- 会话操作 ---------- */
function newSession() {
  var s = { id: uid(), title: '', createdAt: now(), updatedAt: now(), model: '', effort: state.pendingEffort || '', pinned: false, titleAuto: true, messages: [], rev: 0 };
  state.sessions.push(s);
  state.activeId = s.id;
  saveSessions(); saveActive(); touchOrder(s.id);
  syncEnqueue({ kind: 'create', sid: s.id });
  return s;
}
function setActive(id) {
  state.activeId = id;
  if (state.editing && state.editing.sessionId !== id) state.editing = null;
  saveActive();
  renderSessionList(); renderChat(); updateModelUI(); updateTopbar();
  scrollToBottom(false);
  if (isMobile()) closeDrawer();
}
function newChat() {
  state.activeId = null;
  state.editing = null;
  saveActive();
  renderSessionList(); renderChat(); updateModelUI(); updateTopbar();
  if (isMobile()) closeDrawer();
  var input = $id('input');
  input.focus();
}
function deleteSession(s) {
  if (state.streaming && state.streaming.sessionId === s.id) stopStream();
  syncEnqueue({ kind: 'delete', sid: s.id });
  var i = state.sessions.indexOf(s);
  if (i >= 0) state.sessions.splice(i, 1);
  state.order = state.order.filter(function (x) { return x !== s.id; });
  if (state.activeId === s.id) {
    state.activeId = null; saveActive();
    renderChat(); updateTopbar(); updateModelUI();
  }
  saveSessions(); saveOrder(); renderSessionList();
}
function togglePin(s) {
  s.pinned = !s.pinned;
  touchOrder(s.id);
  saveSessions();
  renderSessionList();
}
function startRename(rowEl, s) {
  var input = document.createElement('input');
  input.className = 'rename-input';
  input.value = titleOf(s);
  input.maxLength = 60;
  input.setAttribute('aria-label', '重命名会话');
  var label = rowEl.querySelector('.row-label');
  var controls = rowEl.querySelector('.row-controls');
  if (label) label.style.display = 'none';
  if (controls) controls.style.display = 'none';
  rowEl.classList.add('is-editing');
  rowEl.appendChild(input);
  input.focus(); input.select();
  var done = false;
  function commit(save) {
    if (done) return; done = true;
    var v = input.value.replace(/\s+/g, ' ').trim();
    if (save && v && v !== s.title) {
      s.title = v; s.titleAuto = false; saveSessions(); updateTopbar();
      syncEnqueue({ kind: 'patch', sid: s.id });
    }
    renderSessionList();
  }
  input.addEventListener('keydown', function (ev) {
    if (ev.key === 'Enter') { ev.preventDefault(); commit(true); }
    else if (ev.key === 'Escape') { ev.preventDefault(); commit(false); }
    ev.stopPropagation();
  });
  input.addEventListener('blur', function () { commit(true); });
  input.addEventListener('click', function (ev) { ev.stopPropagation(); });
}

/* ---------- 会话列表渲染 ---------- */
function rowEl(s) {
  var el = document.createElement('div');
  el.className = 'row';
  el.dataset.id = s.id;
  el.dataset.row = '';
  if (s.id === state.activeId) el.dataset.selected = 'focused';
  if (s.pinned) el.dataset.pinned = 'true';
  el.title = titleOf(s) + ' · ' + fmtRelative(s.updatedAt);
  el.innerHTML =
    '<span class="row-pin-mark">' + icon('pin', 'ic-sm') + '</span>' +
    '<span class="row-label">' + esc(titleOf(s)) + '</span>' +
    '<div class="row-controls">' +
      '<button class="row-control" type="button" data-act="rename" aria-label="重命名" title="重命名">' + icon('edit', 'ic-sm') + '</button>' +
      '<button class="row-control" type="button" data-act="more" aria-label="更多操作" title="更多操作">' + icon('dots', 'ic-sm') + '</button>' +
    '</div>';
  el.addEventListener('click', function (ev) {
    if (ev.target.closest('.row-control')) return;
    if (s.id === state.activeId) {   // 已选中：不重建列表（否则双击改名会因元素替换而失效）
      if (isMobile()) closeDrawer();
      return;
    }
    setActive(s.id);
  });
  el.addEventListener('dblclick', function (ev) {
    if (ev.target.closest('.row-control')) return;
    startRename(el, s);
  });
  el.addEventListener('contextmenu', function (ev) {
    ev.preventDefault();
    openSessionMenu(s, ev.clientX, ev.clientY);
  });
  el.querySelector('[data-act="rename"]').addEventListener('click', function (ev) {
    ev.stopPropagation(); startRename(el, s);
  });
  el.querySelector('[data-act="more"]').addEventListener('click', function (ev) {
    ev.stopPropagation();
    var r = ev.currentTarget.getBoundingClientRect();
    openSessionMenu(s, r.right - 4, r.bottom + 4);
  });
  return el;
}
function renderSessionList() {
  var wrap = $id('sessionList');
  var list = sortedSessions();
  if (!list.length) {
    wrap.innerHTML = '<div class="sidebar-empty" style="padding:24px 12px;text-align:center;color:var(--cds-text-muted);font-size:12.5px;line-height:18px">' +
      '<svg class="ic" style="margin:0 auto 8px;color:var(--cds-text-muted)"><use href="#i-chat"/></svg>' +
      '还没有会话。<br>点击「新建对话」开始。</div>';
    return;
  }
  wrap.innerHTML = '';
  var pinned = list.filter(function (s) { return s.pinned; });
  var recents = list.filter(function (s) { return !s.pinned; });
  function group(key, name, arr) {
    if (!arr.length) return;
    var sec = document.createElement('section');
    sec.className = 'df-group';
    if (state.collapsedGroups[key]) sec.dataset.collapsed = '1';
    var label = document.createElement('div');
    label.className = 'df-group-label';
    label.setAttribute('data-sidebar-group-label', '');
    label.setAttribute('role', 'button');
    label.setAttribute('aria-expanded', state.collapsedGroups[key] ? 'false' : 'true');
    label.innerHTML = '<span class="name">' + name + '</span><span class="caret">' + icon('chev-down', 'ic-sm') + '</span>';
    label.addEventListener('click', function () {
      state.collapsedGroups[key] = !state.collapsedGroups[key];
      renderSessionList();
    });
    var rows = document.createElement('div');
    rows.className = 'df-session-rows';
    arr.forEach(function (s) { rows.appendChild(rowEl(s)); });
    sec.appendChild(label); sec.appendChild(rows);
    wrap.appendChild(sec);
  }
  group('pinned', '置顶', pinned);
  group('recents', '最近', recents);
}

/* ---------- 顶栏 ---------- */
function updateTopbar() {
  var s = activeSession();
  $id('chatTitle').textContent = s ? titleOf(s) : '新对话';
  document.title = (s && s.title ? s.title + ' · ' : '') + 'Any2API Chat';
}

/* ---------- Markdown（marked + DOMPurify，本地 vendor） ---------- */
function renderMarkdown(src) {
  var html;
  try { html = marked.parse(src || ''); } catch (e) { html = esc(src); }
  try { html = DOMPurify.sanitize(html); } catch (e) {}
  return html;
}
function renderInto(el, text) {
  el.innerHTML = renderMarkdown(text);
  enhanceContent(el);
}
function enhanceContent(container) {
  /* 表格滚动包裹 */
  var tables = container.querySelectorAll('table');
  for (var t = 0; t < tables.length; t++) {
    if (tables[t].parentElement && tables[t].parentElement.classList.contains('table-scroll')) continue;
    var sc = document.createElement('div');
    sc.className = 'table-scroll';
    tables[t].replaceWith(sc);
    sc.appendChild(tables[t]);
  }
  /* 链接新窗打开 */
  container.querySelectorAll('a').forEach(function (a) {
    a.setAttribute('target', '_blank');
    a.setAttribute('rel', 'noopener noreferrer');
  });
  enhanceCodeBlocks(container);
}

/* ---------- 代码块增强（语言标签 + 复制 + 高亮 + 超大折叠） ---------- */
var COLLAPSE_AT = 200000;   // 源口径：>204800B 折叠；取 200000 字符近似
function enhanceCodeBlocks(root) {
  var pres = root.querySelectorAll('pre');
  for (var i = 0; i < pres.length; i++) {
    var pre = pres[i];
    if (pre.closest('.code-block')) continue;
    var codeEl = pre.querySelector('code');
    var cls = codeEl ? (codeEl.className || '') : '';
    var lm = cls.match(/language-([\w#+.-]+)/);
    var lang = lm ? lm[1].toLowerCase() : '';
    var raw = codeEl ? codeEl.textContent : pre.textContent;

    var wrap = document.createElement('div');
    wrap.className = 'code-block';
    pre.replaceWith(wrap);

    var tools = document.createElement('div');
    tools.className = 'code-block__tools';
    var btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'action-btn copy-btn';
    btn.setAttribute('aria-label', '复制代码');
    btn.title = '复制';
    btn.innerHTML = '<svg class="icon-copy"><use href="#i-copy"/></svg><svg class="icon-check"><use href="#i-check"/></svg>';
    (function (text, b) {
      b.addEventListener('click', function () {
        copyText(text, b);
        b.classList.add('is-copied');
        clearTimeout(b._t);
        b._t = setTimeout(function () { b.classList.remove('is-copied'); }, 1500);
      });
    })(raw, btn);
    tools.appendChild(btn);
    wrap.appendChild(tools);

    if (lang) {
      var langEl = document.createElement('div');
      langEl.className = 'code-block__lang';
      langEl.textContent = lang;
      wrap.appendChild(langEl);
    }
    var scroll = document.createElement('div');
    scroll.className = 'code-block__scroll';
    var pre2 = document.createElement('pre');
    pre2.className = 'code-block__code';
    var code2 = document.createElement('code');
    if (raw.length <= COLLAPSE_AT) {
      try { code2.innerHTML = highlight(raw, lang); } catch (e) { code2.textContent = raw; }
    } else {
      code2.textContent = raw;
      wrap.classList.add('is-collapsed');
    }
    pre2.appendChild(code2);
    scroll.appendChild(pre2);
    wrap.appendChild(scroll);
    if (wrap.classList.contains('is-collapsed')) {
      var foot = document.createElement('div');
      foot.className = 'code-block__footer';
      var more = document.createElement('button');
      more.type = 'button';
      more.className = 'show-more-btn';
      more.innerHTML = icon('chev-down', 'ic-sm') + '<span class="more-label">显示更多</span>';
      more.addEventListener('click', function () {
        var col = wrap.classList.toggle('is-collapsed');
        more.querySelector('.more-label').textContent = col ? '显示更多' : '显示更少';
      });
      foot.appendChild(more);
      wrap.appendChild(foot);
    }
  }
}

/* ---------- 语法高亮（轻量自研；Shiki 12 角色色板 → tok-* 类） ---------- */
function span(cls, text) { return '<span class="tok-' + cls + '">' + text + '</span>'; }
var HL_KW = {
  js: 'var let const function return if else for while do break continue new delete typeof instanceof in of class extends super import export from default async await yield try catch finally throw switch case this void static get set'.split(' '),
  python: 'def class return if elif else for while break continue pass import from as with try except finally raise lambda global nonlocal yield assert del in is not and or async await'.split(' '),
  bash: 'if then else elif fi for while do done case esac function in select until time coproc return break continue local export readonly declare typeset unset shift source alias echo cd set trap'.split(' '),
  go: 'func package import var const type struct interface map chan go defer return if else for range switch case default break continue select nil true false'.split(' '),
  rust: 'fn let mut const struct enum impl trait pub use mod match if else for while loop return break continue where unsafe async await move ref self Self super crate dyn impl as in'.split(' '),
  sql: 'select from where insert into values update set delete create table alter drop join left right inner outer on group by order having limit offset union all as and or not null is distinct count sum avg min max'.split(' '),
  json: [], css: [], yaml: [], html: []
};
var HL_CONSTS = {
  js: { 'true': 1, 'false': 1, 'null': 1, 'undefined': 1, 'NaN': 1, 'Infinity': 1 },
  python: { 'True': 1, 'False': 1, 'None': 1 },
  bash: {}, go: { 'true': 1, 'false': 1, 'nil': 1, 'iota': 1 },
  rust: { 'true': 1, 'false': 1, 'None': 1, 'Some': 1, 'Ok': 1, 'Err': 1 },
  sql: {}, json: { 'true': 1, 'false': 1, 'null': 1 }, css: {}, yaml: { 'true': 1, 'false': 1, 'null': 1, '~': 1 }, html: {}
};
function hlConfig(lang) {
  var l = (lang || '').toLowerCase();
  if (l === 'js' || l === 'javascript' || l === 'jsx') return { lang: 'js', lc: '//', bc: true, bt: true };
  if (l === 'ts' || l === 'typescript' || l === 'tsx') return { lang: 'js', lc: '//', bc: true, bt: true };
  if (l === 'json' || l === 'jsonc') return { lang: 'json', json: true };
  if (l === 'python' || l === 'py') return { lang: 'python', lc: '#', bc: false, defFn: true };
  if (l === 'bash' || l === 'sh' || l === 'shell' || l === 'zsh' || l === 'console') return { lang: 'bash', lc: '#' };
  if (l === 'css' || l === 'scss' || l === 'less') return { lang: 'css', bc: true, propColon: true, atWords: true };
  if (l === 'html' || l === 'xml' || l === 'svg' || l === 'vue') return { lang: 'html' };
  if (l === 'go' || l === 'golang') return { lang: 'go', lc: '//', bc: true, bt: true };
  if (l === 'rust' || l === 'rs') return { lang: 'rust', lc: '//', bc: true, bt: true };
  if (l === 'sql') return { lang: 'sql', lc: '--', bc: true, ci: true };
  if (l === 'yaml' || l === 'yml') return { lang: 'yaml', lc: '#', propColon: true };
  return { lang: 'js', lc: '//', bc: true, bt: true };   // 兜底：近似 js
}
function highlight(code, lang) {
  var cfg = hlConfig(lang);
  if (cfg.lang === 'html') return highlightHTML(code);
  var kwSet = {}, constSet = {};
  (HL_KW[cfg.lang] || []).forEach(function (k) { kwSet[k] = 1; });
  Object.keys(HL_CONSTS[cfg.lang] || {}).forEach(function (k) { constSet[k] = 1; });
  var i = 0, n = code.length, out = '', pendingFn = false;
  function isWord(c) { return /[A-Za-z_$]/.test(c); }
  while (i < n) {
    var c = code[i];
    /* 行注释 */
    if (cfg.lc && code.substr(i, cfg.lc.length) === cfg.lc) {
      var e1 = code.indexOf('\n', i); if (e1 === -1) e1 = n;
      out += span('comment', esc(code.slice(i, e1))); i = e1; continue;
    }
    /* 块注释 */
    if (cfg.bc && c === '/' && code[i + 1] === '*') {
      var e2 = code.indexOf('*/', i + 2); e2 = e2 === -1 ? n : e2 + 2;
      out += span('comment', esc(code.slice(i, e2))); i = e2; continue;
    }
    /* 字符串 */
    if (c === '"' || c === "'" || (cfg.bt && c === '`')) {
      var q = c, j = i + 1;
      while (j < n) {
        if (code[j] === '\\') { j += 2; continue; }
        if (code[j] === q) { j++; break; }
        if (code[j] === '\n' && q !== '`') break;
        j++;
      }
      var str = code.slice(i, j);
      /* JSON 键：字符串后跟冒号 → prop */
      if (cfg.json) {
        var k2 = j; while (k2 < n && /\s/.test(code[k2])) k2++;
        if (code[k2] === ':') { out += span('prop', esc(str)); i = j; continue; }
      }
      out += span('str', esc(str)); i = j; continue;
    }
    /* 数字 / 十六进制 / 颜色 */
    if (/[0-9]/.test(c) && !isWord(code[i - 1] || '')) {
      var mnum = /^0[xX][0-9a-fA-F_]+|^[0-9][0-9_]*(\.[0-9_]+)?([eE][+-]?[0-9]+)?(%|[a-zA-Z]{1,3}\b)?/.exec(code.slice(i));
      out += span('num', esc(mnum[0])); i += mnum[0].length; continue;
    }
    if (c === '#' && (cfg.lang === 'css' || cfg.lang === 'yaml')) {
      var mhex = /^#[0-9a-fA-F]{3,8}\b/.exec(code.slice(i));
      if (mhex) { out += span('num', esc(mhex[0])); i += mhex[0].length; continue; }
    }
    /* 单词 */
    if (isWord(c) || (cfg.atWords && c === '@')) {
      if (cfg.atWords && c === '@') {
        var mat = /^@[\w-]+/.exec(code.slice(i));
        out += span('kw', esc(mat[0])); i += mat[0].length; continue;
      }
      var mw = /^[A-Za-z_$][\w$]*/.exec(code.slice(i));
      var w = mw[0], after = code.slice(i + w.length);
      var wk = cfg.ci ? w.toLowerCase() : w;
      if (kwSet[wk]) {
        out += span('kw', esc(w));
        pendingFn = (w === 'def' || w === 'function' || w === 'func' || w === 'fn' || w === 'class');
        if (w === 'class') pendingFn = 'var';
      } else if (constSet[wk]) {
        out += span('num', esc(w));
      } else if (cfg.propColon && /^\s*:/.test(after)) {
        out += span('prop', esc(w));
      } else if (pendingFn || /^\s*\(/.test(after)) {
        out += span(pendingFn === 'var' ? 'var' : 'fn', esc(w));
        pendingFn = false;
      } else if (pendingFn) {
        out += span(pendingFn === 'var' ? 'var' : 'fn', esc(w)); pendingFn = false;
      } else {
        out += esc(w);
      }
      i += w.length; continue;
    }
    /* 标点 */
    if (/[{}()\[\];,.:=+\-*/%<>!&|^~?]/.test(c)) { out += span('punct', esc(c)); i++; continue; }
    out += esc(c); i++;
  }
  return out;
}
function highlightHTML(code) {
  var out = '', i = 0, n = code.length;
  function hlTag(seg) {
    var o = '', k = 0;
    var mOpen = /^<\/?/.exec(seg);
    if (mOpen) { o += span('punct', esc(mOpen[0])); k = mOpen[0].length; }
    var mName = /^[A-Za-z][\w-]*/.exec(seg.slice(k));
    if (mName) { o += span('kw', esc(mName[0])); k += mName[0].length; }
    var rest = seg.slice(k);
    var re = /("[^"]*"|'[^']*')|([A-Za-z_:][\w:.-]*)(?==)|(\/?>)/g, last = 0, mm;
    while ((mm = re.exec(rest))) {
      if (mm.index > last) o += esc(rest.slice(last, mm.index));
      if (mm[1]) o += span('str', esc(mm[1]));
      else if (mm[2]) o += span('prop', esc(mm[2]));
      else o += span('punct', esc(mm[3]));
      last = re.lastIndex;
    }
    if (last < rest.length) o += esc(rest.slice(last));
    return o;
  }
  while (i < n) {
    if (code.substr(i, 4) === '<!--') {
      var e = code.indexOf('-->', i + 4); e = e === -1 ? n : e + 3;
      out += span('comment', esc(code.slice(i, e))); i = e; continue;
    }
    if (code[i] === '<') {
      var gt = code.indexOf('>', i);
      var seg = gt === -1 ? code.slice(i) : code.slice(i, gt + 1);
      out += hlTag(seg); i += seg.length; continue;
    }
    out += esc(code[i]); i++;
  }
  return out;
}

/* ---------- 剪贴板 ---------- */
function copyText(text, btn) {
  function ok() { if (btn) flashBtn(btn, true); }
  function fail() { if (btn) flashBtn(btn, false); }
  if (navigator.clipboard && window.isSecureContext) {
    navigator.clipboard.writeText(text).then(ok, function () { legacyCopy(text) ? ok() : fail(); });
  } else {
    legacyCopy(text) ? ok() : fail();
  }
}
function legacyCopy(text) {
  try {
    var ta = document.createElement('textarea');
    ta.value = text;
    ta.setAttribute('readonly', '');
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    var ok = document.execCommand('copy');
    document.body.removeChild(ta);
    return ok;
  } catch (e) { return false; }
}
function flashBtn(btn, ok) {
  if (!btn) return;
  btn.classList.add(ok ? 'is-copied' : '');
  clearTimeout(btn._flashT);
  btn._flashT = setTimeout(function () { btn.classList.remove('is-copied'); }, 1500);
}

/* ---------- 消息构建 ---------- */
function thinkLabel(m, streamingHere) {
  if (streamingHere) return '思考中…';
  return '已思考';
}
function buildThinkStep(m, streamingHere) {
  var step = document.createElement('div');
  step.className = 'think-step';
  step.dataset.state = streamingHere ? 'busy' : 'done';
  var open = m.thinkUser ? (m.thinkUser === 'open') : !!streamingHere;
  if (open) step.classList.add('open');
  step.innerHTML =
    '<div class="think-row" role="button" tabindex="0" aria-expanded="' + (open ? 'true' : 'false') + '">' +
      '<span class="caret">' + icon('chev-right', 'ic-sm') + '</span>' +
      '<span class="think-label' + (streamingHere ? ' shimmer' : '') + '">' + thinkLabel(m, streamingHere) + '</span>' +
      '<span class="row-actions"><button class="icon-btn think-copy" type="button" aria-label="复制思考内容" title="复制思考内容">' + icon('copy', 'ic-sm') + '</button></span>' +
    '</div>' +
    '<div class="think-panel"><div class="think-panel-inner"><div class="think-body">' +
      '<p class="think-duration"' + (m.thinkMs && !streamingHere ? '' : ' hidden') + '>思考了 ' + fmtDurCn(m.thinkMs || 0) + '</p>' +
      '<p class="think-text"></p>' +
    '</div></div></div>';
  var row = step.querySelector('.think-row');
  row.addEventListener('click', function (ev) {
    if (ev.target.closest('.think-copy')) return;
    setThinkOpen(step, m, !step.classList.contains('open'));
  });
  row.addEventListener('keydown', function (ev) {
    if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); setThinkOpen(step, m, !step.classList.contains('open')); }
  });
  var cp = step.querySelector('.think-copy');
  cp.addEventListener('click', function (ev) {
    ev.stopPropagation();
    var quote = '> **思考** (~' + Math.round(utf8Len(m.reasoning || '') / 4) + ' tok)\n>\n' +
      (m.reasoning || '').split('\n').map(function (l) { return '> ' + l; }).join('\n');
    copyText(quote, cp);
  });
  var text = step.querySelector('.think-text');
  text.textContent = m.reasoning || '';
  return step;
}
function setThinkOpen(step, m, open) {
  step.classList.toggle('open', open);
  var row = step.querySelector('.think-row');
  if (row) row.setAttribute('aria-expanded', open ? 'true' : 'false');
  m.thinkUser = open ? 'open' : 'closed';
  var s = activeSession();
  if (s) saveSessions();
}
function toolChipText(t) {
  if (t.state === 'running') return '运行中';
  if (t.state === 'failed') return '失败';
  return '完成';
}
function buildToolCard(t) {
  var card = document.createElement('div');
  card.className = 'tool-card';
  card.dataset.open = 'true';
  var args = t.args || '';
  try { var parsed = JSON.parse(args); args = JSON.stringify(parsed, null, 2); } catch (e) {}
  card.innerHTML =
    '<div class="tool-head" role="button" tabindex="0">' +
      '<span class="t-icon">' + icon('sliders', 'ic-sm') + '</span>' +
      '<span class="t-name">' + esc(t.name || '工具调用') + '</span>' +
      '<span class="tool-chip ' + (t.state === 'running' ? 'run' : (t.state === 'failed' ? 'fail' : 'ok')) + '"><span class="dot"></span>' + toolChipText(t) + '</span>' +
    '</div>' +
    '<div class="tool-body"><pre class="tool-args"></pre></div>';
  card.querySelector('.tool-args').textContent = args || '（无参数）';
  card.querySelector('.tool-head').addEventListener('click', function () {
    card.dataset.open = card.dataset.open === 'true' ? 'false' : 'true';
  });
  return card;
}
function buildStatusRow() {
  var el = document.createElement('div');
  el.className = 'status-row';
  el.dataset.role = 'status';
  el.innerHTML =
    '<svg class="spark spark-pulse"><use href="#i-spark"/></svg>' +
    '<div class="status-text">' +
      '<span class="status-word shimmer"></span>' +
      '<span class="mid">·</span><span class="status-timer">0s</span>' +
      '<span class="mid" hidden>·</span><span class="status-tokens" hidden>0 tokens</span>' +
    '</div>';
  return el;
}
function actBtn(iconName, label, onClick, extraCls) {
  var b = document.createElement('button');
  b.type = 'button';
  b.className = 'action-btn ' + (extraCls || '');
  b.innerHTML = icon(iconName, 'ic');
  b.title = label;
  b.setAttribute('aria-label', label);
  b.addEventListener('click', onClick);
  return b;
}
function buildActionsRow(s, m, i, streamingHere) {
  var row = document.createElement('div');
  row.className = 'msg-actions-row';
  if (streamingHere) row.setAttribute('data-always-visible', '');
  var acts = document.createElement('div');
  acts.className = 'msg-actions';
  var isLast = i === s.messages.length - 1;

  if (m.role === 'user') {
    var ed = actBtn('edit', '编辑', function () { startEditMessage(s, i); });
    if (state.streaming) ed.disabled = true;
    acts.appendChild(ed);
  }
  if (m.content) {
    var cp = actBtn('copy', '复制', function () { copyText(m.content, cp); });
    acts.appendChild(cp);
  }
  if (m.role === 'assistant' && isLast && !streamingHere && !m.error) {
    var rg = actBtn('refresh', '重新生成', function () { if (!state.streaming) regenerate(s); });
    if (state.streaming) rg.disabled = true;
    acts.appendChild(rg);
    var up = actBtn('thumb-up', '有帮助', function () { flashFeedback(up, true); });
    var down = actBtn('thumb-down', '没帮助', function () { flashFeedback(down, false); });
    acts.appendChild(up); acts.appendChild(down);
  }
  row.appendChild(acts);
  var time = document.createElement('span');
  time.className = 'msg-time';
  time.textContent = fmtRelative(m.ts);
  row.appendChild(time);
  return row;
}
function flashFeedback(btn, good) {
  btn.classList.add('is-on');
  toast({ title: good ? '感谢反馈' : '已记录反馈', desc: good ? '很高兴对你有帮助。' : '我们会继续改进。', variant: 'neutral', key: 'feedback' });
}

/* ---------- 消息渲染 ---------- */
function buildMessage(s, m, i) {
  var streamingHere = !!(state.streaming && state.streaming.sessionId === s.id
    && i === s.messages.length - 1 && m.role === 'assistant' && !m.done);

  if (m.role === 'user') {
    var el = document.createElement('div');
    el.className = 'trow trow-user';
    el.dataset.idx = String(i);
    var ucol = document.createElement('div');
    ucol.className = 'u-col';
    if (state.editing && state.editing.sessionId === s.id && state.editing.index === i) {
      ucol.appendChild(buildEditBox(s, m, i));
      el.appendChild(ucol);
      return el;
    }
    if (m.attachments && m.attachments.length) ucol.appendChild(renderMessageAttachments(m.attachments));
    if (m.content) {
      var bubble = document.createElement('div');
      bubble.className = 'bubble';
      var btext = document.createElement('span');
      btext.className = 'bubble-text';
      btext.textContent = m.content;
      bubble.appendChild(btext);
      ucol.appendChild(bubble);
    }
    if (m.editedAt) {
      var edited = document.createElement('span');
      edited.className = 'msg-edited';
      edited.textContent = '已编辑';
      ucol.appendChild(edited);
    }
    ucol.appendChild(buildActionsRow(s, m, i, false));
    el.appendChild(ucol);
    return el;
  }

  /* assistant */
  var el2 = document.createElement('div');
  el2.className = 'trow trow-assistant';
  el2.dataset.idx = String(i);
  if (streamingHere) el2.dataset.busy = '1';
  var acol = document.createElement('div');
  acol.className = 'a-col';

  if (m.reasoning || (streamingHere && state.streaming && state.streaming.sawThinking)) {
    acol.appendChild(buildThinkStep(m, streamingHere && !m.content));
  }
  if (m.tools && m.tools.length) {
    var tc = document.createElement('div');
    tc.className = 'tool-cards';
    m.tools.forEach(function (t) { tc.appendChild(buildToolCard(t)); });
    acol.appendChild(tc);
  }
  var contentWrap = document.createElement('div');
  contentWrap.className = 'msg-content';
  var md = document.createElement('div');
  md.className = 'prose';
  renderInto(md, m.content || '');
  contentWrap.appendChild(md);
  acol.appendChild(contentWrap);

  if (streamingHere) {
    var sr = buildStatusRow();
    acol.appendChild(sr);
  }
  if (m.stopped) {
    var stp = document.createElement('div');
    stp.className = 'msg-stopped';
    stp.innerHTML = icon('stop', 'ic-sm') + '<span>已停止生成</span>';
    acol.appendChild(stp);
  }
  if (m.error) {
    var err = document.createElement('div');
    err.className = 'msg-error';
    err.innerHTML = '<div class="err-head">' + icon('warn', 'ic-sm') + '<span>生成失败</span></div>' +
      '<div class="err-msg">' + esc(m.error.message || '未知错误') + '</div>' +
      '<div class="err-actions"></div>';
    var retry = document.createElement('button');
    retry.type = 'button';
    retry.className = 'btn btn-secondary';
    retry.textContent = '重试';
    retry.addEventListener('click', function () { if (!state.streaming) regenerate(s); });
    err.querySelector('.err-actions').appendChild(retry);
    acol.appendChild(err);
  }
  if (m.usage) acol.appendChild(buildUsageRow(m.usage));
  acol.appendChild(buildActionsRow(s, m, i, streamingHere));
  el2.appendChild(acol);
  return el2;
}
function buildUsageRow(u) {
  var el = document.createElement('div');
  el.className = 'msg-usage';
  var parts = [];
  if (u.prompt_tokens != null) parts.push('输入 ' + u.prompt_tokens);
  if (u.completion_tokens != null) parts.push('输出 ' + u.completion_tokens);
  var cached = u.prompt_tokens_details && u.prompt_tokens_details.cached_tokens;
  if (cached) parts.push('缓存 ' + cached);
  if (u.total_tokens != null && !parts.length) parts.push('合计 ' + u.total_tokens);
  if (parts.length) parts.push('tokens');
  el.textContent = parts.join(' · ');
  return el;
}
function attIcon(a) {
  if (a.state === 'missing') return icon('warn', 'ic');
  if (a.type && a.type.indexOf('image/') === 0) return icon('image', 'ic');
  return icon('file', 'ic');
}
function renderMessageAttachments(list, editable) {
  var wrap = document.createElement('div');
  wrap.className = 'msg-attachments';
  list.forEach(function (a) {
    var node;
    if (a.type && a.type.indexOf('image/') === 0) {
      node = document.createElement('button');
      node.type = 'button';
      node.className = 'msg-image';
      node.setAttribute('aria-label', '查看图片 ' + a.name);
      if (a.thumb) {
        var img = document.createElement('img');
        img.src = a.thumb;
        img.alt = a.name;
        node.appendChild(img);
      } else {
        var fb = document.createElement('span');
        fb.className = 'msg-image-fallback';
        fb.innerHTML = attIcon(a);
        var nm = document.createElement('span');
        nm.textContent = a.name;
        fb.appendChild(nm);
        node.appendChild(fb);
      }
      node.addEventListener('click', function (ev) {
        if (ev.target.closest('.msg-att-x')) return;
        openImageViewer(a, node);
      });
    } else {
      node = document.createElement('div');
      node.className = 'att-chip';
      node.innerHTML = '<span class="att-thumb">' + attIcon(a) + '</span>' +
        '<span class="att-meta"><span class="att-name">' + esc(a.name) + '</span>' +
        '<span class="att-sub">' + esc(fmtSize(a.size)) + '</span></span>';
    }
    if (editable) {
      var x = document.createElement('button');
      x.type = 'button';
      x.className = 'msg-att-x';
      x.setAttribute('aria-label', '移除 ' + a.name);
      x.innerHTML = icon('x', 'ic-sm');
      x.addEventListener('click', function (ev) {
        ev.stopPropagation();
        removeMessageAttachment(a);
      });
      node.appendChild(x);
    }
    wrap.appendChild(node);
  });
  return wrap;
}
function removeMessageAttachment(a) {
  var s = activeSession();
  if (!s) return;
  s.messages.forEach(function (m) {
    if (!m.attachments) return;
    var i = m.attachments.indexOf(a);
    if (i >= 0) m.attachments.splice(i, 1);
  });
  if (a.fileId) { deleteFileBlob(a.fileId); state.fileCache.delete(a.fileId); }
  saveSessions(); renderChat();
}

/* ---------- 编辑重发 ---------- */
function buildEditBox(s, m, i) {
  var box = document.createElement('div');
  box.className = 'edit-box';
  if (m.attachments && m.attachments.length) box.appendChild(renderMessageAttachments(m.attachments, true));
  var ta = document.createElement('textarea');
  ta.className = 'edit-field';
  ta.rows = 1;
  ta.value = m.content || '';
  ta.setAttribute('aria-label', '编辑消息');
  box.appendChild(ta);
  var acts = document.createElement('div');
  acts.className = 'edit-actions';
  var cancel = document.createElement('button');
  cancel.type = 'button'; cancel.className = 'btn btn-secondary'; cancel.textContent = '取消';
  cancel.addEventListener('click', cancelEdit);
  var save = document.createElement('button');
  save.type = 'button'; save.className = 'btn btn-primary'; save.textContent = '保存并重发';
  save.addEventListener('click', function () { requestSaveEdit(s, m, i); });
  acts.appendChild(cancel); acts.appendChild(save);
  box.appendChild(acts);
  ta.addEventListener('input', function () { autoGrow(ta); });
  ta.addEventListener('keydown', function (ev) {
    if (ev.key === 'Enter' && !ev.shiftKey && !ev.isComposing && ev.keyCode !== 229) {
      ev.preventDefault(); ev.stopPropagation(); requestSaveEdit(s, m, i);
    } else if (ev.key === 'Escape') {
      ev.preventDefault(); ev.stopPropagation(); cancelEdit();
    }
  });
  requestAnimationFrame(function () {
    autoGrow(ta); ta.focus();
    try { ta.setSelectionRange(ta.value.length, ta.value.length); } catch (e) {}
  });
  return box;
}
function startEditMessage(s, i) {
  if (state.streaming) { toast({ title: '正在生成', desc: '请先停止生成，再编辑消息。', variant: 'neutral' }); return; }
  var m = s.messages[i];
  if (!m || m.role !== 'user') return;
  state.editing = { sessionId: s.id, index: i };
  renderChat();
  var el = $id('chatColumn').querySelector('[data-idx="' + i + '"]');
  if (el) el.scrollIntoView({ block: 'center' });
}
function cancelEdit() {
  if (!state.editing) return;
  state.editing = null;
  renderChat();
}
function requestSaveEdit(s, m, i) {
  var ta = $id('chatColumn').querySelector('[data-idx="' + i + '"] .edit-field');
  if (!ta) return;
  var text = ta.value.replace(/^\s+|\s+$/g, '');
  if (!text && !(m.attachments && m.attachments.length)) {
    toast({ title: '消息不能为空', desc: '请输入内容或保留至少一个附件。', variant: 'warning', key: 'edit-empty' });
    return;
  }
  if (text === (m.content || '')) { cancelEdit(); return; }
  var apply = function () { applyEdit(s, m, i, text); };
  if (i < s.messages.length - 1) {
    openConfirm({
      title: '编辑并重发？',
      desc: '此消息之后的 ' + (s.messages.length - i - 1) + ' 条消息将被删除，且无法恢复。',
      okLabel: '编辑并重发',
      danger: false,
      onOk: apply,
      onCancel: cancelEdit
    });
  } else {
    apply();
  }
}
function ensureMessageBlobs(m) {
  if (!m.attachments || !m.attachments.length) return Promise.resolve(true);
  return Promise.all(m.attachments.map(function (a) {
    if (a.dataUrl || !a.fileId) return true;
    var cached = state.fileCache.get(a.fileId);
    if (cached) { a.dataUrl = cached; return true; }
    return getFileBlob(a.fileId).then(function (blob) {
      if (!blob) return false;
      return blobToDataURL(blob).then(function (d) {
        a.dataUrl = d; state.fileCache.set(a.fileId, d); return true;
      });
    }, function () { return false; });
  })).then(function (rs) { return rs.every(Boolean); });
}
function applyEdit(s, m, i, text) {
  ensureMessageBlobs(m).then(function (ok) {
    if (!ok) {
      toast({ title: '原图不可用', desc: '附件原图已丢失，请重新添加后再编辑重发。', variant: 'danger', key: 'edit-missing' });
      return;
    }
    m.content = text;
    m.ts = now();
    m.editedAt = now();
    s.messages.length = i + 1;
    s.syncFull = true;   // 截断后的消息集合需整会话替换
    if (i === 0 && s.titleAuto !== false) {
      s.title = titleFrom(text || (m.attachments && m.attachments[0] && m.attachments[0].name) || '');
    }
    s.updatedAt = now();
    state.editing = null;
    saveSessions();
    touchOrder(s.id);
    renderSessionList(); renderChat(); updateTopbar(); updatePlaceholder();
    buildWireAsync(s).then(function (wire) { runAssistantStream(s, wire); });
  });
}
function openImageViewer(a, anchor) {
  var dlg = $id('imageViewer');
  var img = $id('imageViewerImg');
  var cap = $id('imageViewerCaption');
  img.alt = a.name || '图片';
  img.src = a.thumb || '';
  cap.textContent = (a.name || '') + (a.size ? ' · ' + fmtSize(a.size) : '');
  dlg._anchor = anchor || null;
  showEl(dlg);
  pushOverlay('imageViewer');
  focusEl(dlg);
  var fileId = a.fileId;
  if (!fileId) return;
  getFileBlob(fileId).then(function (blob) {
    if (!blob || dlg.hidden) return;
    var url = URL.createObjectURL(blob);
    dlg._objectUrl = url;
    img.src = url;
  }, function () {});
}
function closeImageViewer() {
  var dlg = $id('imageViewer');
  if (dlg._objectUrl) { URL.revokeObjectURL(dlg._objectUrl); dlg._objectUrl = ''; }
  hideEl(dlg);
  popOverlay('imageViewer');
  if (dlg._anchor && dlg._anchor.isConnected) dlg._anchor.focus();
}

/* ---------- 空态 / 欢迎页 ---------- */
var SUGGESTIONS = [
  { icon: 'bulb', title: '解释概念', sub: '用简单的话解释什么是「OpenAI 兼容 API」' },
  { icon: 'code', title: '写代码', sub: '写一个带指数退避的 Python 重试装饰器' },
  { icon: 'list', title: '整理要点', sub: '把这段要点整理成一份周报' },
  { icon: 'sparkles', title: '头脑风暴', sub: '给出三条提升代码可读性的建议' }
];
function welcomeHTML() {
  var hour = new Date().getHours();
  var greet = hour < 6 ? '夜深了' : (hour < 12 ? '早上好' : (hour < 18 ? '下午好' : '晚上好'));
  var s = SUGGESTIONS.map(function (sg, i) {
    return '<button class="suggestion-card" type="button" data-prompt="' + esc(sg.sub) + '" style="animation-delay:' + (i * 40) + 'ms">' +
      '<span class="s-icon">' + icon(sg.icon, 'ic') + '</span>' +
      '<span class="s-body"><span class="s-title">' + sg.title + '</span><span class="s-sub">' + esc(sg.sub) + '</span></span>' +
      '</button>';
  }).join('');
  var recents = sortedSessions().slice(0, 4).map(function (x) {
    return '<button class="recent-row" type="button" data-sid="' + esc(x.id) + '">' +
      '<span class="r-icon">' + icon('chat', 'ic-sm') + '</span>' +
      '<span class="r-title">' + esc(titleOf(x)) + '</span>' +
      '<span class="r-time">' + fmtRelative(x.updatedAt) + '</span></button>';
  }).join('');
  return '<div class="welcome">' +
    '<div class="greeting greeting-animated"><svg class="spark"><use href="#i-spark"/></svg>' +
      '<h1 class="greeting-text">' + greet + '，我是 Any2API 助手</h1></div>' +
    '<p class="greeting-sub">自托管 · OpenAI 兼容 · 全功能对话界面</p>' +
    '<div class="suggestions">' + s + '</div>' +
    (recents ? '<div class="recents"><div class="recents-title">最近对话</div>' + recents + '</div>' : '') +
    '</div>';
}
function renderChat() {
  var col = $id('chatColumn');
  var s = activeSession();
  if (!s || !s.messages.length) {
    col.classList.add('is-empty');
    col.innerHTML = welcomeHTML();
    col.querySelectorAll('.suggestion-card').forEach(function (c) {
      c.addEventListener('click', function () {
        var input = $id('input');
        input.value = c.dataset.prompt || '';
        autoGrow(input); updateSendEnabled(); input.focus();
      });
    });
    col.querySelectorAll('.recent-row').forEach(function (r) {
      r.addEventListener('click', function () { setActive(r.dataset.sid); });
    });
    if (state.streaming) state.streaming.refs = null;
    updateJumpBtn();
    return;
  }
  col.classList.remove('is-empty');
  col.innerHTML = '';
  s.messages.forEach(function (m, i) { col.appendChild(buildMessage(s, m, i)); });
  if (state.streaming) {
    if (state.streaming.sessionId === s.id) bindStreamRefs(col, s);
    else state.streaming.refs = null;
  }
  updateJumpBtn();
}

/* ---------- 流式引用绑定 ---------- */
function bindStreamRefs(col, s) {
  var st = state.streaming;
  if (!st || st.sessionId !== s.id) return;
  var idx = s.messages.length - 1;
  var root = col.querySelector('[data-idx="' + idx + '"]');
  st.refs = root ? {
    root: root,
    content: root.querySelector('.prose'),
    think: root.querySelector('.think-step'),
    thinkBody: root.querySelector('.think-text'),
    thinkDuration: root.querySelector('.think-duration'),
    status: root.querySelector('.status-row'),
    tools: root.querySelector('.tool-cards')
  } : null;
}
/* ============================================================================
 * app2.js —— 流式引擎 / 输入区 / 浮层 / 主题 / 键盘 / 初始化
 * ========================================================================== */

/* ---------- 滚动 ---------- */
function isNearBottom() {
  var el = $id('chatScroll');
  return el.scrollHeight - el.scrollTop - el.clientHeight < 120;
}
function scrollToBottom(smooth) {
  var el = $id('chatScroll');
  if (smooth && el.scrollTo) el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' });
  else el.scrollTop = el.scrollHeight;
  updateJumpBtn();
}
function updateJumpBtn() {
  $id('jumpBtn').hidden = isNearBottom();
}
function markEnter(s, idx) {
  if (state.activeId !== s.id) return;
  var el = $id('chatColumn').querySelector('[data-idx="' + idx + '"]');
  if (el) el.classList.add('msg-enter');
}

/* ---------- 输入区基础 ---------- */
function autoGrow(el) {
  el.style.height = 'auto';
  el.style.height = Math.min(el.scrollHeight, 200) + 'px';
}
function updateSendEnabled() {
  var btn = $id('sendBtn');
  var hasText = $id('input').value.trim().length > 0;
  var hasAtt = state.attachments.length > 0;
  btn.disabled = !!state.streaming || (!hasText && !hasAtt) || attachmentsBusy() || !attachmentsValid();
  updateComposerNotices();
}
function setStreamingUI(on) {
  $id('sendBtn').hidden = on;
  $id('stopBtn').hidden = !on;
  document.body.classList.toggle('streaming', on);
  updateSendEnabled();
}
function updatePlaceholder() {
  var s = activeSession();
  $id('input').placeholder = (s && s.messages.length) ? '回复 Any2API…' : '有什么可以帮你的？';
}

/* ---------- 附件数据层（IndexedDB + data URL + 缩略图） ---------- */
var fileDBPromise = null;
function openFileDB() {
  if (fileDBPromise) return fileDBPromise;
  fileDBPromise = new Promise(function (resolve, reject) {
    if (!window.indexedDB) { reject(new Error('IndexedDB 不可用')); return; }
    var req = indexedDB.open(DB_NAME, 1);
    req.onupgradeneeded = function () {
      var db = req.result;
      if (!db.objectStoreNames.contains(DB_STORE)) db.createObjectStore(DB_STORE, { keyPath: 'id' });
    };
    req.onsuccess = function () { resolve(req.result); };
    req.onerror = function () { reject(req.error || new Error('IndexedDB 打开失败')); };
  });
  return fileDBPromise;
}
function idbCall(mode, fn) {
  return openFileDB().then(function (db) {
    return new Promise(function (resolve, reject) {
      var tx = db.transaction(DB_STORE, mode);
      var req = fn(tx.objectStore(DB_STORE));
      tx.oncomplete = function () { resolve(req ? req.result : undefined); };
      tx.onerror = function () { reject(tx.error || new Error('IndexedDB 事务失败')); };
      tx.onabort = function () { reject(tx.error || new Error('IndexedDB 事务中止')); };
    });
  });
}
function putFileBlob(id, blob, meta) {
  return idbCall('readwrite', function (st) {
    return st.put({ id: id, blob: blob, meta: meta || null, createdAt: now() });
  });
}
function getFileBlob(id) {
  return idbCall('readonly', function (st) { return st.get(id); }).then(function (rec) {
    return rec ? rec.blob : null;
  });
}
function deleteFileBlob(id) {
  if (!id) return Promise.resolve();
  return idbCall('readwrite', function (st) { return st.delete(id); }).catch(function () {});
}
function clearFileStore() {
  return idbCall('readwrite', function (st) { return st.clear(); }).catch(function () {});
}
function pruneFileStore() {
  var keep = {};
  state.sessions.forEach(function (s) {
    (s.messages || []).forEach(function (m) {
      (m.attachments || []).forEach(function (a) { if (a.fileId) keep[a.fileId] = 1; });
    });
  });
  return openFileDB().then(function (db) {
    return new Promise(function (resolve) {
      var tx = db.transaction(DB_STORE, 'readwrite');
      var req = tx.objectStore(DB_STORE).openCursor();
      req.onsuccess = function () {
        var cur = req.result;
        if (!cur) { resolve(); return; }
        if (!keep[cur.value.id]) cur.delete();
        cur.continue();
      };
      req.onerror = function () { resolve(); };
    });
  }).catch(function () {});
}
function makeThumb(file) {
  return new Promise(function (resolve) {
    var url = URL.createObjectURL(file);
    var img = new Image();
    img.onload = function () {
      try {
        var max = 96;
        var w = img.naturalWidth || max, h = img.naturalHeight || max;
        var scale = Math.min(1, max / Math.max(w, h));
        var cw = Math.max(1, Math.round(w * scale)), ch = Math.max(1, Math.round(h * scale));
        var c = document.createElement('canvas');
        c.width = cw; c.height = ch;
        c.getContext('2d').drawImage(img, 0, 0, cw, ch);
        var out = c.toDataURL('image/jpeg', 0.72);
        URL.revokeObjectURL(url);
        resolve(out);
      } catch (e) { URL.revokeObjectURL(url); resolve(''); }
    };
    img.onerror = function () { URL.revokeObjectURL(url); resolve(''); };
    img.src = url;
  });
}
function readFileAsDataURL(file, onProgress) {
  return new Promise(function (resolve, reject) {
    var r = new FileReader();
    r.onprogress = function (ev) {
      if (onProgress && ev.lengthComputable) onProgress(ev.loaded / ev.total);
    };
    r.onload = function () { resolve(String(r.result || '')); };
    r.onerror = function () { reject(r.error || new Error('读取失败')); };
    r.readAsDataURL(file);
  });
}
function blobToDataURL(blob) { return readFileAsDataURL(blob); }

/* ---------- 附件（选择 / 拖入 / 粘贴 → 校验 → 读取 → 随请求发送） ---------- */
function attachmentsBusy() {
  return state.attachments.some(function (a) { return a.state === 'reading' || a.state === 'uploading'; });
}
function attachmentsValid() {
  return state.attachments.every(function (a) { return a.state === 'ready'; });
}
function attachmentsTotal() {
  return state.attachments.reduce(function (n, a) { return n + (a.size || 0); }, 0);
}
function addFiles(files) {
  if (!files || !files.length) return;
  Array.prototype.slice.call(files).forEach(function (f) {
    if (state.attachments.length >= ATT_MAX_COUNT) {
      toast({ title: '附件数量已达上限', desc: '单条消息最多 ' + ATT_MAX_COUNT + ' 个附件。', variant: 'warning', key: 'att-count' });
      return;
    }
    var a = {
      id: 'att-' + (++attSeq), name: f.name || '未命名文件', size: f.size || 0,
      type: f.type || '', fileId: uid(), state: 'reading', progress: 0, error: '',
      file: f, thumb: ''
    };
    if (ATT_TYPES.indexOf(a.type) === -1) {
      a.state = 'failed';
      a.error = '仅支持 PNG/JPEG/GIF/WebP';
      state.attachments.push(a);
      return;
    }
    if (a.size > ATT_MAX_BYTES) {
      a.state = 'failed';
      a.error = '文件超过 20 MB 限制';
      state.attachments.push(a);
      return;
    }
    if (attachmentsTotal() + a.size > ATT_MAX_TOTAL) {
      a.state = 'failed';
      a.error = '单条消息附件合计不能超过 20 MB';
      state.attachments.push(a);
      return;
    }
    state.attachments.push(a);
    loadAttachment(a);
  });
  renderComposerAttachments();
  updateSendEnabled();
}
function loadAttachment(a) {
  a.state = 'reading';
  a.progress = 0;
  a.error = '';
  renderComposerAttachments();
  var pThumb = makeThumb(a.file).then(function (t) { a.thumb = t; });
  var pData = readFileAsDataURL(a.file, function (r) {
    a.progress = Math.round(r * 100);
    renderComposerAttachments();
  });
  Promise.all([pThumb, pData]).then(function (rs) {
    a.dataUrl = rs[1];
    state.fileCache.set(a.fileId, a.dataUrl);
    return putFileBlob(a.fileId, a.file, { name: a.name, type: a.type, size: a.size });
  }).then(function () {
    a.state = 'ready';
    a.progress = 100;
    a.file = null;
    renderComposerAttachments();
    updateSendEnabled();
  }).catch(function (e) {
    a.state = 'failed';
    a.error = '读取失败：' + ((e && e.message) || e);
    renderComposerAttachments();
    updateSendEnabled();
  });
}
function removeAttachment(a) {
  var i = state.attachments.indexOf(a);
  if (i >= 0) state.attachments.splice(i, 1);
  if (a.fileId) { deleteFileBlob(a.fileId); state.fileCache.delete(a.fileId); }
  renderComposerAttachments(); updateSendEnabled();
}
function retryAttachment(a) {
  if (a.file) { loadAttachment(a); return; }
  if (!a.fileId) { a.state = 'failed'; a.error = '原图不可用，请重新选择文件'; renderComposerAttachments(); return; }
  a.state = 'reading'; a.progress = 0; a.error = '';
  renderComposerAttachments();
  getFileBlob(a.fileId).then(function (blob) {
    if (!blob) throw new Error('原图不可用');
    return blobToDataURL(blob).then(function (d) {
      a.dataUrl = d;
      state.fileCache.set(a.fileId, d);
      a.state = 'ready'; a.progress = 100;
      renderComposerAttachments(); updateSendEnabled();
    });
  }).catch(function (e) {
    a.state = 'failed'; a.error = '原图不可用：' + ((e && e.message) || e);
    renderComposerAttachments(); updateSendEnabled();
  });
}
function renderComposerAttachments() {
  var box = $id('composerAttachments');
  if (!box) return;
  box.innerHTML = '';
  state.attachments.forEach(function (a) {
    var chip = document.createElement('div');
    chip.className = 'att-chip' + (a.state === 'failed' ? ' failed' : '') + ((a.type || '').indexOf('image/') === 0 ? ' image' : '');
    var thumb;
    if (a.thumb) thumb = '<img src="' + a.thumb + '" alt="">';
    else thumb = attIcon(a);
    var sub;
    if (a.state === 'reading' || a.state === 'uploading') {
      sub = '<span class="att-sub">上传中… <span class="att-bar" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="' + Math.round(a.progress) + '"><i style="width:' + Math.round(a.progress) + '%"></i></span></span>';
    } else if (a.state === 'failed') {
      sub = '<span class="att-sub">' + esc(a.error || '上传失败') + ' <button class="att-retry" type="button">重试</button></span>';
    } else {
      sub = '<span class="att-sub">' + esc(fmtSize(a.size)) + ' · 已就绪</span>';
    }
    chip.innerHTML = '<span class="att-thumb">' + thumb + '</span>' +
      '<span class="att-meta"><span class="att-name">' + esc(a.name) + '</span>' + sub + '</span>' +
      '<button class="att-x" type="button" aria-label="移除 ' + esc(a.name) + '">' + icon('x', 'ic-sm') + '</button>';
    chip.querySelector('.att-x').addEventListener('click', function () { removeAttachment(a); });
    var rt = chip.querySelector('.att-retry');
    if (rt) rt.addEventListener('click', function () { retryAttachment(a); });
    box.appendChild(chip);
  });
}
function updateComposerNotices() {
  var box = $id('composerNotices');
  if (!box) return;
  var msgs = [];
  if (attachmentsBusy()) msgs.push({ text: '附件仍在上传，请稍候…', cls: '' });
  var failed = state.attachments.filter(function (a) { return a.state === 'failed'; });
  if (failed.length) msgs.push({ text: failed.length + ' 个附件无法发送：' + (failed[0].error || '未知错误'), cls: ' danger' });
  if (!msgs.length) { box.hidden = true; box.innerHTML = ''; return; }
  box.hidden = false;
  box.innerHTML = msgs.map(function (m) {
    return '<div class="composer-notice' + m.cls + '">' + icon('info', 'ic-sm') + '<span>' + esc(m.text) + '</span></div>';
  }).join('');
}

/* ---------- 发送 / 流式消费 ---------- */
function messageWire(m) {
  if (!m.attachments || !m.attachments.length) return { role: m.role, content: m.content };
  var parts = [];
  if (m.content && m.content.trim()) parts.push({ type: 'text', text: m.content });
  m.attachments.forEach(function (a) {
    if (a.type && a.type.indexOf('image/') === 0 && a.dataUrl) {
      parts.push({ type: 'image_url', image_url: { url: a.dataUrl } });
    }
  });
  return parts.length ? { role: m.role, content: parts } : { role: m.role, content: m.content };
}
function buildWire(s) {
  var out = [];
  s.messages.forEach(function (m) {
    if (m.role !== 'user' && m.role !== 'assistant') return;
    var hasText = typeof m.content === 'string' && m.content.trim() !== '';
    var hasImg = !!(m.attachments && m.attachments.some(function (a) {
      return a.type && a.type.indexOf('image/') === 0 && a.dataUrl;
    }));
    if (hasText || hasImg) out.push(messageWire(m));
  });
  return out;
}
function buildWireAsync(s) {
  return Promise.all(s.messages.map(function (m) {
    if (!m.attachments || !m.attachments.length) return m;
    return Promise.all(m.attachments.map(function (a) {
      if (a.dataUrl || !a.fileId) return a;
      var cached = state.fileCache.get(a.fileId);
      if (cached) { a.dataUrl = cached; return a; }
      return getFileBlob(a.fileId).then(function (blob) {
        if (!blob) { a.state = 'missing'; return a; }
        return blobToDataURL(blob).then(function (d) {
          a.dataUrl = d; state.fileCache.set(a.fileId, d); return a;
        });
      }, function () { a.state = 'missing'; return a; });
    })).then(function () { return m; });
  })).then(function () { return buildWire(s); });
}
function titleFrom(text) {
  var t = String(text).replace(/[#>*`\[\]()!]/g, ' ').replace(/\s+/g, ' ').trim();
  if (!t) t = '新对话';
  return t.length > 22 ? t.slice(0, 22) + '…' : t;
}
function sendCurrent() {
  if (state.streaming) return;
  var input = $id('input');
  var text = input.value.replace(/^\s+|\s+$/g, '');
  if (!text && !state.attachments.length) return;
  if (attachmentsBusy()) {
    toast({ title: '附件仍在上传', desc: '请稍候，待附件读取完成后再发送。', variant: 'warning', key: 'att-busy' });
    updateComposerNotices();
    return;
  }
  if (!attachmentsValid()) {
    toast({ title: '附件无法发送', desc: '请移除或重试失败的附件。', variant: 'warning', key: 'att-invalid' });
    return;
  }
  var s = activeSession() || newSession();
  var m = { id: uid(), role: 'user', content: text, ts: now() };
  var atts = state.attachments.filter(function (a) { return a.state === 'ready'; })
    .map(function (a) {
      return { id: a.id, name: a.name, size: a.size, type: a.type, fileId: a.fileId, thumb: a.thumb };
    });
  if (atts.length) m.attachments = atts;
  s.messages.push(m);
  if (!s.title) { s.title = titleFrom(text || (atts[0] && atts[0].name) || ''); s.titleAuto = true; }
  s.updatedAt = now();
  input.value = '';
  autoGrow(input);
  state.attachments = [];
  renderComposerAttachments();
  updateSendEnabled();
  saveSessions();
  syncEnqueue({ kind: 'append', sid: s.id, msgId: m.id, payload: msgToServer(m) });
  touchOrder(s.id);
  renderSessionList(); renderChat(); updateTopbar(); updateModelUI(); updatePlaceholder();
  markEnter(s, s.messages.length - 1);
  scrollToBottom(true);
  buildWireAsync(s).then(function (wire) { runAssistantStream(s, wire); });
}
function stopStream() {
  if (!state.streaming) return;
  state.streaming.aborted = true;
  try { state.streaming.controller.abort(); } catch (e) {}
}
function regenerate(s) {
  if (state.streaming) return;
  var idx = -1;
  for (var i = s.messages.length - 1; i >= 0; i--) {
    if (s.messages[i].role === 'assistant') { idx = i; break; }
  }
  if (idx === -1 || idx !== s.messages.length - 1) return;   // 只允许重新生成「最后一条」
  s.messages.splice(idx, 1);
  s.syncFull = true;   // 服务端需整会话替换（删掉的助手消息要落账）
  s.updatedAt = now();
  saveSessions();
  renderChat(); scrollToBottom(false);
  buildWireAsync(s).then(function (wire) { runAssistantStream(s, wire); });
}
function httpErrorMessage(resp) {
  var retry = resp.headers.get('retry-after');
  return resp.text().then(function (text) {
    var detail = '';
    try {
      var j = JSON.parse(text);
      detail = (j && j.error && (j.error.message || j.error.type)) || (j && j.message) || '';
    } catch (e) { detail = String(text || '').slice(0, 300); }
    var msg = 'HTTP ' + resp.status + (detail ? '：' + detail : '');
    if (retry) msg += '（Retry-After: ' + retry + '）';
    return msg;
  }, function () { return 'HTTP ' + resp.status; });
}
function friendlyError(e) {
  if (!e) return '未知错误';
  var m = e.message || String(e);
  if (/failed to fetch|networkerror|load failed|network error/i.test(m)) return '网络错误：无法连接到服务。';
  return m;
}
function absorbFull(msg, data) {
  var ch = data && data.choices && data.choices[0];
  if (data && data.usage) msg.usage = data.usage;
  if (!ch) return;
  var m2 = ch.message || ch.delta || {};
  if (typeof m2.reasoning_content === 'string') msg.reasoning += m2.reasoning_content;
  if (typeof m2.content === 'string') msg.content += m2.content;
}
function absorbToolCall(msg, tc, st) {
  var idx = (typeof tc.index === 'number') ? tc.index : 0;
  var t = msg.tools[idx] || (msg.tools[idx] = { name: '', args: '', id: '', state: 'running' });
  if (tc.id) t.id = tc.id;
  var fn = tc.function || tc;
  if (typeof fn.name === 'string') t.name += fn.name;
  if (typeof fn.arguments === 'string') t.args += fn.arguments;
  st.sawTools = true;
}
function consumeSSE(resp, s, msg, st) {
  var reader = resp.body.getReader();
  var decoder = new TextDecoder('utf-8');
  var buf = '';
  var dataLines = [];
  var done = false;
  var sawDone = false, sawFinish = false;

  function dispatch() {
    if (!dataLines.length) return;
    var data = dataLines.join('\n');
    dataLines = [];
    if (data === '[DONE]') { sawDone = true; done = true; return; }
    var obj;
    try { obj = JSON.parse(data); } catch (e) { return; }
    if (obj && obj.error) {
      var em = (obj.error && (obj.error.message || obj.error.type)) || '流式错误';
      var err = new Error(em);
      err.fromStream = true;
      throw err;
    }
    if (obj && obj.usage && typeof obj.usage === 'object') msg.usage = obj.usage;
    var ch = obj && obj.choices && obj.choices[0];
    if (!ch) return;
    var d = ch.delta || {};
    var r = typeof d.reasoning_content === 'string' ? d.reasoning_content
          : (typeof d.reasoning === 'string' ? d.reasoning : '');
    if (r) {
      if (!st.thinkStart) st.thinkStart = now();
      st.thinkEnd = now();
      st.sawThinking = true;
      msg.reasoning += r;
      st.bytes += utf8Len(r);
    }
    if (typeof d.content === 'string' && d.content) {
      msg.content += d.content;
      st.bytes += utf8Len(d.content);
    }
    if (Array.isArray(d.tool_calls)) {
      d.tool_calls.forEach(function (tc) { absorbToolCall(msg, tc, st); });
    }
    if (d.function_call && typeof d.function_call === 'object') {
      absorbToolCall(msg, { function: d.function_call }, st);
    }
    if (ch.finish_reason) {
      sawFinish = true;
      if (ch.finish_reason === 'tool_calls') st.toolsFinished = true;
    }
    scheduleStreamRender(s, msg, st);
  }

  function handleLine(line) {
    if (line === '') { dispatch(); return; }
    if (line.charAt(0) === ':') return;                 // 注释 / keepalive
    var ci = line.indexOf(':');
    var field = ci === -1 ? line : line.slice(0, ci);
    var value = ci === -1 ? '' : line.slice(ci + 1);
    if (value.charAt(0) === ' ') value = value.slice(1);
    if (field === 'data') dataLines.push(value);
    /* event / id / retry 忽略 */
  }

  function cancelReader() {
    try {
      var p = reader.cancel();
      if (p && typeof p.catch === 'function') p.catch(function () {});
    } catch (e) {}
  }

  return (function loop() {
    if (done) { cancelReader(); return Promise.resolve(); }
    return reader.read().then(function (r) {
      if (r.done) {
        buf += decoder.decode();
        if (buf.length) {
          var restLines = buf.split('\n');
          buf = '';
          for (var j = 0; j < restLines.length; j++) {
            var rl = restLines[j];
            if (rl.charAt(rl.length - 1) === '\r') rl = rl.slice(0, -1);
            handleLine(rl);
          }
        }
        dispatch();
        cancelReader();
        if (!sawDone && !sawFinish) {
          throw new Error('连接中断：响应流未正常结束（未见完成信号）');
        }
        return;
      }
      buf += decoder.decode(r.value, { stream: true });
      var nl;
      while (!done && (nl = buf.indexOf('\n')) !== -1) {
        var line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);
        if (line.charAt(line.length - 1) === '\r') line = line.slice(0, -1);
        handleLine(line);
      }
      return loop();
    });
  })().catch(function (e) {
    cancelReader();
    throw e;
  });
}
function runAssistantStream(s, wireOverride) {
  if (state.streaming) return;
  var wire = wireOverride || buildWire(s);
  if (!wire.length) return;
  var msg = { id: uid(), role: 'assistant', content: '', reasoning: '', tools: [], ts: now(), done: false };
  s.messages.push(msg);
  var st = {
    sessionId: s.id,
    msgIndex: s.messages.length - 1,
    msg: msg,
    controller: new AbortController(),
    aborted: false,
    refs: null,
    turnStart: now(),
    thinkStart: 0, thinkEnd: 0,
    bytes: 0,
    sawThinking: false, sawTools: false, toolsFinished: false,
    lastContent: msg.content, lastReasoning: msg.reasoning,
    timer: 0, lastRender: 0, statusTimer: 0
  };
  state.streaming = st;
  setStreamingUI(true);
  renderChat();
  markEnter(s, s.messages.length - 1);
  scrollToBottom(false);
  bindStreamRefs($id('chatColumn'), s);
  startStatusTicker(st);

  var p;
  try {
    var body = { messages: wire, stream: true };
    var model = currentModel(s);
    if (model) body.model = model;      // 无模型清单：省略 model 字段（优雅降级）
    var effort = currentEffort(s);
    if (effort) body.reasoning_effort = effort;   // 仅用户显式选择时发送
    var chatHeaders = { 'Content-Type': 'application/json', 'Accept': 'text/event-stream' };
    /* P1-4：仅服务端同步模式 + 有活动会话时带会话头（turn_hook.go 据此落账）。 */
    if (state.sync.mode === 'server' && s && s.id) chatHeaders['X-A2A-Conversation-Id'] = s.id;
    p = fetch(API_CHAT, {
      method: 'POST',
      headers: chatHeaders,
      body: JSON.stringify(body),
      signal: st.controller.signal
    }).then(function (resp) {
      if (!resp.ok) return httpErrorMessage(resp).then(function (m) { throw new Error(m); });
      var ctype = (resp.headers.get('content-type') || '').toLowerCase();
      if (ctype.indexOf('application/json') !== -1) {
        return resp.json().then(function (data) {
          absorbFull(msg, data);
          if (!msg.content && !msg.reasoning) throw new Error('响应中没有可显示的内容');
        });
      }
      return consumeSSE(resp, s, msg, st);
    });
  } catch (e) {
    p = Promise.reject(e);
  }

  return p.then(function () {
    if (!msg.content && !msg.reasoning) throw new Error('响应为空');
  }, function (e) {
    if (st.aborted || (e && e.name === 'AbortError')) {
      msg.stopped = true;
      if (!msg.content && !msg.reasoning) {
        var i = s.messages.indexOf(msg);
        if (i >= 0) s.messages.splice(i, 1);
      }
      return;
    }
    throw e;
  }).catch(function (e) {
    msg.error = { message: friendlyError(e) };
    (msg.tools || []).forEach(function (t) { if (t.state === 'running') t.state = 'failed'; });
  }).then(function () {
    stopStatusTicker(st);
    msg.done = true;
    if (st.thinkStart) msg.thinkMs = Math.max(0, (st.thinkEnd || now()) - st.thinkStart);
    if (msg.tools && msg.tools.length) {
      msg.tools.forEach(function (t) { if (t.state === 'running') t.state = 'done'; });
    }
    state.streaming = null;
    setStreamingUI(false);
    s.updatedAt = now();
    saveSessions();
    syncAfterStream(s, msg);
    touchOrder(s.id);
    renderSessionList(); renderChat(); updateTopbar(); updateModelUI(); updatePlaceholder();
    if (isNearBottom()) scrollToBottom(false);
  });
}

/* ---------- 流式增量渲染 ---------- */
function scheduleStreamRender(s, msg, st) {
  var wait = Math.max(0, 40 - (now() - st.lastRender));
  if (st.timer) return;
  st.timer = setTimeout(function () {
    st.timer = 0;
    st.lastRender = now();
    if (state.streaming !== st) return;
    updateStreamMessage(s, msg, st);
  }, wait);
}
function statusWordFor(st, msg) {
  var phase = 'think';
  if (st.sawTools && msg.tools && msg.tools.some(function (t) { return t.state === 'running'; })) phase = 'tools';
  else if (msg.content) phase = 'reply';
  var elapsed = now() - (st.thinkStart || st.turnStart);
  if (phase === 'tools') return '正在运行工具…';
  if (phase === 'reply') return '正在回复…';
  var s = elapsed / 1000;
  if (s >= 60) return '快想好了…';
  if (s >= 45) return '再想一会儿…';
  if (s >= 30) return '还在深入思考…';
  if (s >= 15) return '仍在思考…';
  return '正在思考…';
}
function updateStatusRow(st) {
  var refs = st.refs;
  if (!refs || !refs.status || state.streaming !== st) return;
  var word = refs.status.querySelector('.status-word');
  var timer = refs.status.querySelector('.status-timer');
  var tokens = refs.status.querySelector('.status-tokens');
  var mid2 = tokens.previousElementSibling;
  var msg = st.msg;
  if (word) word.textContent = statusWordFor(st, msg);
  if (timer) timer.textContent = fmtDurNarrow(now() - (st.thinkStart || st.turnStart));
  var n = Math.round(st.bytes / 4);
  if (tokens) {
    tokens.textContent = n + ' tokens';
    tokens.hidden = n < 1;
    if (mid2 && mid2.classList.contains('mid')) mid2.hidden = n < 1;
  }
}
function startStatusTicker(st) {
  stopStatusTicker(st);
  st.statusTimer = setInterval(function () {
    if (state.streaming !== st) { stopStatusTicker(st); return; }
    updateStatusRow(st);
  }, 400);
}
function stopStatusTicker(st) {
  if (st.statusTimer) { clearInterval(st.statusTimer); st.statusTimer = 0; }
}
function updateStreamMessage(s, msg, st) {
  if (state.streaming !== st) return;
  var stick = isNearBottom();
  if (!st.refs && state.activeId === s.id) bindStreamRefs($id('chatColumn'), s);
  var refs = st.refs;
  if (!refs) return;
  /* 思考块首次出现：重建绑定 */
  if (msg.reasoning && !refs.think && state.activeId === s.id) {
    renderChat(); bindStreamRefs($id('chatColumn'), s); refs = st.refs;
    if (!refs) return;
  }
  if (refs.think && msg.reasoning) {
    st.sawThinking = true;
    if (st.lastReasoning !== msg.reasoning) {
      st.lastReasoning = msg.reasoning;
      var tb = refs.thinkBody;
      var tstick = tb.scrollHeight - tb.scrollTop - tb.clientHeight < 30;
      tb.textContent = msg.reasoning;
      if (tstick) tb.scrollTop = tb.scrollHeight;
    }
  }
  if (refs.content && st.lastContent !== msg.content) {
    st.lastContent = msg.content;
    refs.content.innerHTML = renderMarkdown(msg.content);
    enhanceContent(refs.content);
    if (msg.content) {
      var cur = document.createElement('span');
      cur.className = 'stream-cursor';
      refs.content.appendChild(cur);
    }
  }
  if (st.sawTools) updateToolCards(refs, msg);
  updateStatusRow(st);
  if (stick) scrollToBottom(false);
}
function updateToolCards(refs, msg) {
  if (!refs.tools) {
    refs.tools = document.createElement('div');
    refs.tools.className = 'tool-cards';
    var cw = refs.content.closest('.msg-content');
    if (cw) cw.parentElement.insertBefore(refs.tools, cw);
  }
  refs.tools.innerHTML = '';
  msg.tools.forEach(function (t) { refs.tools.appendChild(buildToolCard(t)); });
}

/* ---------- 浮层管理（栈 + 遮罩 + 焦点） ---------- */
var openStack = [];
var OVERLAYS = {};   // name -> {el, modal, close}
function registerOverlay(name, el, modal, close) { OVERLAYS[name] = { el: el, modal: !!modal, close: close }; }
function pushOverlay(name) {
  if (openStack.indexOf(name) === -1) openStack.push(name);
  syncScrim();
}
function popOverlay(name) {
  openStack = openStack.filter(function (n) { return n !== name; });
  syncScrim();
}
function syncScrim() {
  var need = openStack.some(function (n) { return OVERLAYS[n] && OVERLAYS[n].modal; });
  var sc = $id('scrim');
  sc._animToken = (sc._animToken || 0) + 1;
  var token = sc._animToken;
  if (need) {
    sc.hidden = false;
    requestAnimationFrame(function () { if (sc._animToken === token) sc.classList.remove('anim'); });
  } else {
    sc.classList.add('anim');
    setTimeout(function () { if (sc._animToken === token) sc.hidden = true; }, 210);
  }
}
function closeTopOverlay() {
  var top = openStack[openStack.length - 1];
  if (top && OVERLAYS[top]) { OVERLAYS[top].close(); return true; }
  return false;
}
function showEl(el) {
  el._animToken = (el._animToken || 0) + 1;
  var token = el._animToken;
  el._closing = false;
  el.hidden = false;
  el.classList.add('anim');
  requestAnimationFrame(function () { if (el._animToken === token) el.classList.remove('anim'); });
}
function hideEl(el) {
  el._animToken = (el._animToken || 0) + 1;
  var token = el._animToken;
  el._closing = true;
  el.classList.add('anim');
  setTimeout(function () {
    if (el._animToken === token) { el.hidden = true; el._closing = false; }
  }, 210);
}
function focusEl(el) {
  if (!el.hasAttribute('tabindex')) el.setAttribute('tabindex', '-1');
  try { el.focus({ preventScroll: true }); } catch (e) {}
}

/* ---------- 设置弹窗 ---------- */
function selectSettingsPane(paneId) {
  document.querySelectorAll('#settingsDialog .s-nav-item').forEach(function (b) {
    b.classList.toggle('on', b.dataset.pane === paneId);
  });
  document.querySelectorAll('#settingsDialog .s-main > div').forEach(function (p) {
    p.hidden = p.id !== paneId;
  });
}
function openSettings(pane) {
  var dlg = $id('settingsDialog');
  if (dlg.hidden) {
    selectSettingsPane(pane || 'pane-account');
    showEl(dlg);
    pushOverlay('settings');
  } else if (pane) {
    selectSettingsPane(pane);
  }
  focusEl(dlg);
}
function closeSettings() {
  hideEl($id('settingsDialog'));
  popOverlay('settings');
}

/* ---------- 订阅套餐 ---------- */
function openSubscription() {
  var dlg = $id('subscriptionDialog');
  if (dlg.hidden) { showEl(dlg); pushOverlay('subscription'); }
  focusEl(dlg);
}
function closeSubscription() {
  hideEl($id('subscriptionDialog'));
  popOverlay('subscription');
}
function setPlan(p, silent) {
  state.plan = p;
  lsSet(LS.plan, p);
  updatePlanUI();
  if (!silent) {
    if (p === 'ultra') toast({ title: '已加入 ULTRA PRO MAX 候补名单', desc: '宇宙最强，指日可待！（演示：不会产生扣费）', variant: 'success', key: 'plan' });
    else if (p === 'pro') toast({ title: '已切换到 PRO 方案（演示）', desc: '订阅功能即将上线。', variant: 'success', key: 'plan' });
    else toast({ title: '已切换回 Free 方案', desc: '随时可以再次升级。', variant: 'neutral', key: 'plan' });
  }
}
function updatePlanUI() {
  var label = { free: 'Free 方案', pro: 'PRO 方案', ultra: 'ULTRA PRO MAX' }[state.plan] || 'Free 方案';
  $id('userPlanLabel').textContent = label;
  var mpl = $id('umPlanLabel');                 // 用户菜单底部同一套餐口径（W80-1）
  if (mpl) { mpl.textContent = label; mpl.classList.toggle('ultra', state.plan === 'ultra'); }
  var chip = $id('planChip');
  chip.textContent = label;
  chip.classList.toggle('ultra', state.plan === 'ultra');
  var f = $id('subCtaFree'), p = $id('subCtaPro'), u = $id('subCtaUltra');
  f.textContent = state.plan === 'free' ? '当前方案' : '切换回 Free';
  f.disabled = state.plan === 'free';
  p.textContent = state.plan === 'pro' ? '当前方案' : '升级到 PRO';
  p.disabled = state.plan === 'pro';
  u.innerHTML = state.plan === 'ultra' ? (icon('check', 'ic-sm') + ' 已是最强') : (icon('crown', 'ic-sm') + ' 订阅 ULTRA PRO MAX');
  u.disabled = state.plan === 'ultra';
}

/* ---------- 快捷键面板 ---------- */
var SHORTCUTS = [
  { group: '常规', rows: [
    ['命令面板 / 搜索', ['Ctrl', 'K']],
    ['切换侧边栏', ['Ctrl', 'B']],
    ['新建对话', ['Ctrl', 'N']],
    ['设置', ['Ctrl', ',']],
    ['键盘快捷键', ['Ctrl', '/']]
  ] },
  { group: '对话', rows: [
    ['发送消息', ['Enter']],
    ['消息内换行', ['Shift', 'Enter']],
    ['停止生成', ['Esc']]
  ] }
];
function renderShortcutRows(container) {
  if (!container) return;
  container.innerHTML = SHORTCUTS.map(function (sec) {
    return '<div class="sc-group">' + sec.group + '</div>' + sec.rows.map(function (r) {
      return '<div class="sc-row"><span>' + r[0] + '</span><span class="keys">' +
        r[1].map(function (k) { return '<span class="kbd-cap">' + k + '</span>'; }).join('') +
        '</span></div>';
    }).join('');
  }).join('');
}
function openShortcuts() {
  var dlg = $id('shortcutsDialog');
  if (dlg.hidden) { showEl(dlg); pushOverlay('shortcuts'); }
  focusEl(dlg);
}
function closeShortcuts() {
  hideEl($id('shortcutsDialog'));
  popOverlay('shortcuts');
}

/* ---------- 确认对话框 ---------- */
function openConfirm(opts) {
  $id('confirmTitle').textContent = opts.title || '确认操作？';
  $id('confirmDesc').textContent = opts.desc || '';
  var ok = $id('confirmOk');
  ok.textContent = opts.okLabel || '确认';
  ok.className = 'btn ' + (opts.danger === false ? 'btn-primary' : 'btn-danger');
  state.confirmAction = opts.onOk || null;
  state.confirmCancelAction = opts.onCancel || null;
  var dlg = $id('confirmDialog');
  if (dlg.hidden) { showEl(dlg); pushOverlay('confirm'); }
  focusEl(ok);
}
function closeConfirm() {
  state.confirmAction = null;
  state.confirmCancelAction = null;
  hideEl($id('confirmDialog'));
  popOverlay('confirm');
}
function dismissConfirm() {
  var fn = state.confirmCancelAction;
  closeConfirm();
  if (fn) fn();
}

/* ---------- 提示气泡（data-tip）抑制 ----------
 * 问题：点击带 data-tip 的控件打开菜单后，::after 仍因 :hover/:focus-visible
 * 显示并盖在菜单上（tooltip z-index 高于 menu-popup）。规则：
 *   1. pointerdown / 键盘 click 即抑制该锚点；
 *   2. 菜单打开期间锚点同时带 aria-expanded="true"，CSS 双保险；
 *   3. pointerleave / blur / 菜单关闭动画结束后恢复。 */
function suppressTip(el) {
  if (el && el.hasAttribute && el.hasAttribute('data-tip')) el.setAttribute('data-tip-suppressed', '1');
}
function restoreTip(el) {
  if (el && el.removeAttribute) el.removeAttribute('data-tip-suppressed');
}
function initTips() {
  document.addEventListener('pointerdown', function (e) {
    var host = e.target && e.target.closest ? e.target.closest('[data-tip]') : null;
    if (host) suppressTip(host);
  }, true);
  document.addEventListener('click', function (e) {
    var host = e.target && e.target.closest ? e.target.closest('[data-tip]') : null;
    if (host && e.detail === 0) suppressTip(host);   // 键盘触发无 pointerleave，靠 blur 恢复
  }, true);
  document.querySelectorAll('[data-tip]').forEach(function (el) {
    el.addEventListener('pointerleave', function () {
      if (el.getAttribute('aria-expanded') !== 'true') restoreTip(el);
    });
    el.addEventListener('blur', function () {
      if (el.getAttribute('aria-expanded') !== 'true') restoreTip(el);   // 焦点移入菜单 ≠ 关闭
    });
  });
}

/* ---------- 菜单（用户 / 会话 / 模型） ---------- */
function openMenuAt(el, x, y) {
  el.hidden = false;
  el.classList.add('anim');
  var r = el.getBoundingClientRect();
  var px = clamp(x, 8, Math.max(8, window.innerWidth - r.width - 8));
  var py = clamp(y, 8, Math.max(8, window.innerHeight - r.height - 8));
  el.style.left = px + 'px';
  el.style.top = py + 'px';
  requestAnimationFrame(function () { el.classList.remove('anim'); });
}
function closeMenuEl(el, done) {
  el.classList.add('anim');
  setTimeout(function () {
    if (el.classList.contains('anim')) el.hidden = true;
    if (done) done();
  }, 130);
}
function toggleUserMenu() {
  var m = $id('userMenu');
  if (!m.hidden) { closeUserMenu(); return; }
  var btn = $id('userMenuBtn');
  var r = btn.getBoundingClientRect();
  /* 先显示 → 再量高 → 再定位：hidden 元素的 offsetHeight 恒为 0，旧写法
   * 因此把菜单放到按钮正上沿（视觉上向下盖住按钮）。 */
  m.hidden = false;
  m.style.transformOrigin = 'bottom left';
  openMenuAt(m, r.left, r.top - m.offsetHeight - 6);
  pushOverlay('userMenu');
  suppressTip(btn);
  btn.setAttribute('aria-expanded', 'true');
}
function closeUserMenu() {
  var menu = $id('userMenu');
  var btn = $id('userMenuBtn');
  closeMenuEl(menu, function () {
    if (!menu.hidden) return;                 // 关闭动画期间又打开了：保持抑制
    if (btn) { btn.setAttribute('aria-expanded', 'false'); restoreTip(btn); }
  });
  popOverlay('userMenu');
}
function openSessionMenu(s, x, y) {
  var menu = $id('sessionMenu');
  menu.innerHTML =
    '<button class="menu-item" role="menuitem" type="button" data-act="rename"><span class="mi-icon">' + icon('edit') + '</span><span class="mi-label">重命名</span><span class="mi-trail"><span class="kbd-cap">Ctrl</span><span class="kbd-cap">Alt</span><span class="kbd-cap">R</span></span></button>' +
    '<button class="menu-item" role="menuitem" type="button" data-act="pin"><span class="mi-icon">' + icon('pin') + '</span><span class="mi-label">' + (s.pinned ? '取消置顶' : '置顶') + '</span></button>' +
    '<button class="menu-item" role="menuitem" type="button" data-act="up"><span class="mi-icon">' + icon('arrow-up') + '</span><span class="mi-label">上移</span></button>' +
    '<button class="menu-item" role="menuitem" type="button" data-act="down"><span class="mi-icon">' + icon('arrow-down') + '</span><span class="mi-label">下移</span></button>' +
    '<div class="menu-sep" role="separator"></div>' +
    '<button class="menu-item danger" role="menuitem" type="button" data-act="delete"><span class="mi-icon">' + icon('trash') + '</span><span class="mi-label">删除</span></button>';
  var row = $id('sessionList').querySelector('[data-id="' + s.id + '"]');
  if (row) row.dataset.menuOpen = 'true';
  menu.querySelectorAll('[data-act]').forEach(function (b) {
    b.addEventListener('click', function () {
      var act = b.dataset.act;
      closeSessionMenu();
      if (act === 'rename') { if (row) startRename(row, s); }
      else if (act === 'pin') togglePin(s);
      else if (act === 'up') moveSession(s, -1);
      else if (act === 'down') moveSession(s, 1);
      else if (act === 'delete') {
        openConfirm({
          title: '删除「' + titleOf(s) + '」？',
          desc: '该会话及其全部消息将被永久删除，此操作无法撤销。',
          okLabel: '删除',
          onOk: function () { deleteSession(s); toast({ title: '已删除会话', desc: titleOf(s), variant: 'neutral' }); }
        });
      }
    });
  });
  openMenuAt(menu, x, y);
  pushOverlay('sessionMenu');
}
function closeSessionMenu() {
  var menu = $id('sessionMenu');
  closeMenuEl(menu);
  popOverlay('sessionMenu');
  document.querySelectorAll('#sessionList .row[data-menu-open]').forEach(function (r) { delete r.dataset.menuOpen; });
}
function openModelMenu() {
  var menu = $id('modelMenu');
  var pill = $id('modelPillBtn');
  renderModelMenu();
  var r = pill.getBoundingClientRect();
  menu.hidden = false;                       /* 先显示再量高（同用户菜单） */
  menu.style.transformOrigin = 'bottom left';
  openMenuAt(menu, r.left, r.top - menu.offsetHeight - 6);
  pushOverlay('modelMenu');
  suppressTip(pill);
  pill.setAttribute('aria-expanded', 'true');
  var first = menu.querySelector('.effort-opt[aria-checked="true"]') ||
              menu.querySelector('.menu-item, .effort-opt');
  if (first) first.focus();
  revealEffort(menu);                        /* 聚焦可能把档位条滚走：再对齐到当前档位 */
}
function closeModelMenu() {
  var menu = $id('modelMenu');
  var pill = $id('modelPillBtn');
  closeMenuEl(menu, function () {
    if (!menu.hidden) return;                 // 关闭动画期间又打开了：保持抑制
    if (pill) { pill.setAttribute('aria-expanded', 'false'); restoreTip(pill); }
  });
  popOverlay('modelMenu');
}

/* ---------- 命令面板 / 搜索 ---------- */
var PALETTE_ACTIONS = [
  { icon: 'plus', label: '新建对话', hint: 'Ctrl N', run: function () { newChat(); } },
  { icon: 'gear', label: '打开设置', hint: 'Ctrl ,', run: function () { openSettings(); } },
  { icon: 'crown', label: '订阅套餐', hint: '', run: function () { openSubscription(); } },
  { icon: 'keyboard', label: '键盘快捷键', hint: 'Ctrl /', run: function () { openShortcuts(); } },
  { icon: 'moon', label: '切换主题（系统 → 浅色 → 深色）', hint: '', run: cycleTheme }
];
var paletteHl = 0;

function mdToPlain(s) {
  return String(s || '')
    .replace(/```[\s\S]*?```/g, function (m) { return m.replace(/```[^\n]*\n?/g, ''); })
    .replace(/`([^`]*)`/g, '$1')
    .replace(/!\[[^\]]*\]\([^)]*\)/g, '')
    .replace(/\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/^#{1,6}\s+/gm, '')
    .replace(/[*_~]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}
function makeSnippet(text, terms, radius) {
  var low = text.toLowerCase();
  var pos = -1;
  for (var i = 0; i < terms.length; i++) {
    var p = low.indexOf(terms[i]);
    if (p !== -1 && (pos === -1 || p < pos)) pos = p;
  }
  if (pos === -1) pos = 0;
  var start = Math.max(0, pos - radius), end = Math.min(text.length, pos + radius);
  return (start > 0 ? '…' : '') + text.slice(start, end) + (end < text.length ? '…' : '');
}
function markTerms(text, terms) {
  var out = esc(text);
  if (!terms.length) return out;
  var pattern = terms.slice().sort(function (a, b) { return b.length - a.length; })
    .map(function (t) { return t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }).join('|');
  try { out = out.replace(new RegExp('(' + pattern + ')', 'gi'), '<mark>$1</mark>'); } catch (e) {}
  return out;
}
function buildSearchIndex() {
  if (state.searchIndex) return state.searchIndex;
  var idx = [];
  sortedSessions().forEach(function (s) {
    (s.messages || []).forEach(function (m, i) {
      if (m.role !== 'user' && m.role !== 'assistant') return;
      var text = mdToPlain(m.content || '');
      var names = (m.attachments || []).map(function (a) { return a.name; }).join(' ');
      if (names) text += ' ' + names;
      text = text.slice(0, 20000);
      if (!text) return;
      idx.push({ sid: s.id, idx: i, role: m.role, text: text, low: text.toLowerCase(), title: titleOf(s), updatedAt: s.updatedAt || 0 });
    });
  });
  state.searchIndex = idx;
  return idx;
}
function searchAll(q) {
  var terms = String(q || '').toLowerCase().split(/\s+/).filter(Boolean);
  if (!terms.length) return [];
  var out = [];
  sortedSessions().forEach(function (s) {
    var t = titleOf(s).toLowerCase();
    var hit = terms.every(function (term) { return t.indexOf(term) !== -1; });
    if (hit) {
      out.push({
        icon: 'chat', group: '会话', label: titleOf(s), hint: fmtRelative(s.updatedAt),
        run: function () { setActive(s.id); }
      });
    }
  });
  var rows = buildSearchIndex().filter(function (row) {
    return terms.every(function (term) { return row.low.indexOf(term) !== -1; });
  });
  rows.sort(function (a, b) { return b.updatedAt - a.updatedAt; });
  rows.forEach(function (row) {
    if (out.length >= 30) return;
    var role = row.role === 'user' ? '你' : '助手';
    var snippet = makeSnippet(row.text, terms, 42);
    out.push({
      icon: 'chat', group: '消息', label: row.title, sub: role,
      html: markTerms(snippet, terms), trail: role,
      run: (function (sid, idx) { return function () { jumpToMessage(sid, idx); }; })(row.sid, row.idx)
    });
  });
  return out.slice(0, 30);
}
function buildPaletteItems(q, mode) {
  var query = (q || '').toLowerCase();
  var searchMode = mode === 'search';
  var items = [];
  if (searchMode && query.trim()) {
    items = searchAll(query);
  } else if (searchMode) {
    PALETTE_ACTIONS.forEach(function (a) { items.push({ icon: a.icon, group: '命令', label: a.label, hint: a.hint, run: a.run }); });
    sortedSessions().slice(0, 8).forEach(function (s) {
      items.push({ icon: 'chat', group: '最近会话', label: titleOf(s), hint: fmtRelative(s.updatedAt), run: function () { setActive(s.id); } });
    });
  } else {
    PALETTE_ACTIONS.forEach(function (a) {
      if (!query || a.label.toLowerCase().indexOf(query) !== -1) items.push(a);
    });
    sortedSessions().forEach(function (s) {
      var t = titleOf(s);
      if (!query || t.toLowerCase().indexOf(query) !== -1) {
        items.push({ icon: 'chat', label: t, hint: fmtRelative(s.updatedAt), run: function () { setActive(s.id); } });
      }
    });
    items = items.slice(0, 12);
  }
  var list = $id('paletteList');
  if (!items.length) {
    list.innerHTML = '<div class="palette-empty">没有匹配的结果' +
      (searchMode ? '<br><span class="pe-hint">试试搜索消息内容或会话标题</span>' : '') + '</div>';
    paletteHl = -1;
    return;
  }
  var html = '', lastGroup = '';
  items.forEach(function (it, i) {
    if (it.group && it.group !== lastGroup) {
      html += '<div class="palette-group">' + esc(it.group) + '</div>';
      lastGroup = it.group;
    }
    html += '<button class="palette-item' + (i === 0 ? ' hl' : '') + '" type="button" data-i="' + i + '">' +
      '<span class="pi-icon">' + icon(it.icon, 'ic-sm') + '</span>';
    if (it.sub) {
      html += '<span class="pi-body"><span class="pi-sub">' + esc(it.sub) + '</span>' +
        '<span class="pi-label">' + (it.html || esc(it.label)) + '</span></span>';
    } else {
      html += '<span class="pi-label">' + (it.html || esc(it.label)) + '</span>';
    }
    if (it.hint) html += '<span class="pi-trail">' + esc(it.hint) + '</span>';
    html += '</button>';
  });
  list.innerHTML = html;
  paletteHl = 0;
  list.querySelectorAll('.palette-item').forEach(function (b) {
    b.addEventListener('click', function () { runPaletteItem(items[+b.dataset.i]); });
    b.addEventListener('mousemove', function () { setPaletteHl(+b.dataset.i); });
  });
  list._items = items;
}
function setPaletteHl(i) {
  paletteHl = i;
  var list = $id('paletteList');
  list.querySelectorAll('.palette-item').forEach(function (b) { b.classList.toggle('hl', +b.dataset.i === i); });
}
function runPaletteItem(it) {
  if (!it) return;
  closePalette();
  setTimeout(function () { it.run(); }, 0);
}
function openPalette(mode) {
  var searchMode = mode === 'search';
  state.paletteMode = searchMode ? 'search' : 'quick';
  var p = $id('commandPalette');
  if (p.hidden || p._closing) { showEl(p); pushOverlay('palette'); }
  var input = $id('paletteInput');
  input.value = '';
  input.placeholder = searchMode ? '搜索会话和消息…' : '搜索会话或运行命令…';
  input.setAttribute('aria-label', searchMode ? '搜索会话和消息' : '搜索会话或运行命令');
  buildPaletteItems('', state.paletteMode);
  input.focus();
}
function closePalette() {
  hideEl($id('commandPalette'));
  popOverlay('palette');
}
function togglePalette(mode) {
  var p = $id('commandPalette');
  if (p.hidden || p._closing) openPalette(mode); else closePalette();
}
function jumpToMessage(sid, idx) {
  setActive(sid);
  requestAnimationFrame(function () {
    var el = $id('chatColumn').querySelector('[data-idx="' + idx + '"]');
    if (!el) return;
    el.scrollIntoView({ block: 'center', behavior: 'smooth' });
    el.classList.add('msg-hit');
    setTimeout(function () { el.classList.remove('msg-hit'); }, 1700);
  });
}

/* ---------- Toast ---------- */
function toast(opts) {
  opts = opts || {};
  var vp = $id('toastViewport');
  var key = opts.key || '';
  if (key) {
    var dup = vp.querySelector('[data-key="' + key + '"]');
    if (dup) {
      var c = (parseInt(dup.dataset.count || '1', 10)) + 1;
      dup.dataset.count = String(c);
      var tc = dup.querySelector('.t-count');
      if (tc) tc.textContent = ' (×' + c + ')';
      resetToastTimer(dup);
      return;
    }
  }
  var el = document.createElement('div');
  el.className = 'toast ' + (opts.variant || 'neutral');
  el.dataset.key = key;
  el.dataset.count = '1';
  var ic = opts.variant === 'warning' ? 'warn' : (opts.variant === 'danger' ? 'warn' : (opts.variant === 'success' ? 'check' : 'info'));
  el.innerHTML = '<div class="t-inner"><span class="t-icon">' + icon(ic) + '</span>' +
    '<div class="t-body"><div class="t-title">' + esc(opts.title || '') + '<span class="t-count"></span></div>' +
    (opts.desc ? '<div class="t-desc">' + esc(opts.desc) + '</div>' : '') + '</div>' +
    '<button class="t-close" type="button" aria-label="关闭">' + icon('x', 'ic-sm') + '</button></div>';
  el.querySelector('.t-close').addEventListener('click', function () { dismissToast(el); });
  vp.appendChild(el);
  while (vp.querySelectorAll('.toast').length > 4) vp.removeChild(vp.querySelector('.toast'));
  refreshToastStack();
  resetToastTimer(el);
}
function resetToastTimer(el) {
  clearTimeout(el._timer);
  el._timer = setTimeout(function () { dismissToast(el); }, 5000);
}
function dismissToast(el) {
  if (!el.isConnected) return;
  el.classList.add('ending');
  setTimeout(function () { el.remove(); refreshToastStack(); }, 220);
}
function refreshToastStack() {
  var els = Array.prototype.slice.call($id('toastViewport').querySelectorAll('.toast'));
  var n = els.length;
  els.forEach(function (el, idx) {
    var i = n - 1 - idx;
    el.style.setProperty('--i', String(i));
    el.classList.toggle('behind', i >= 3);
  });
}

/* ---------- Banner ---------- */
function showBanner(opts) {
  var slot = $id('bannerSlot');
  slot.hidden = false;
  var el = document.createElement('div');
  el.className = 'banner ' + (opts.variant || 'neutral');
  var ic = (opts.variant === 'warning' || opts.variant === 'danger') ? 'warn' : 'info';
  el.innerHTML = '<span class="b-icon">' + icon(ic, 'ic-sm') + '</span>' +
    '<div class="b-main">' + (opts.title ? '<span class="b-title">' + esc(opts.title) + '</span> ' : '') +
    (opts.text ? '<span>' + esc(opts.text) + '</span>' : '') + '</div>' +
    (opts.action ? '<div class="b-actions"></div>' : '') +
    '<button class="b-close" type="button" aria-label="关闭">' + icon('x', 'ic-sm') + '</button>';
  if (opts.action) {
    var b = document.createElement('button');
    b.type = 'button';
    b.className = 'btn btn-secondary';
    b.textContent = opts.action.label;
    b.addEventListener('click', opts.action.run);
    el.querySelector('.b-actions').appendChild(b);
  }
  el.querySelector('.b-close').addEventListener('click', function () {
    el.remove();
    if (!slot.children.length) slot.hidden = true;
  });
  slot.appendChild(el);
  return el;
}

/* ---------- 主题 / 密度 / 动效 ---------- */
function applyTheme() {
  var r = document.documentElement;
  if (state.theme === 'dark') r.setAttribute('data-theme', 'dark');
  else if (state.theme === 'light') r.setAttribute('data-theme', 'light');
  else r.removeAttribute('data-theme');
}
function setTheme(t, silent) {
  state.theme = t;
  lsSet(LS.theme, t);
  applyTheme();
  updateThemeUI();
  if (!silent) {
    var names = { system: '跟随系统', light: '浅色', dark: '深色' };
    toast({ title: '外观已切换为「' + names[t] + '」', variant: 'neutral', key: 'theme' });
  }
}
function cycleTheme() {
  var order = ['system', 'light', 'dark'];
  var i = order.indexOf(state.theme);
  setTheme(order[(i + 1) % order.length]);
}
function updateThemeUI() {
  document.querySelectorAll('#themeSeg .seg-item').forEach(function (b) {
    b.classList.toggle('on', b.dataset.themeVal === state.theme);
  });
  var names = { system: '跟随系统', light: '浅色', dark: '深色' };
  $id('umAppearanceState').textContent = names[state.theme] || '';
}
function applyDensity() {
  $id('sidebar').dataset.density = state.density;
  document.querySelectorAll('#densitySeg .seg-item').forEach(function (b) {
    b.classList.toggle('on', b.dataset.densityVal === state.density);
  });
}
function setDensity(d) {
  state.density = d;
  lsSet(LS.density, d);
  applyDensity();
  toast({ title: '侧栏密度已切换为「' + (d === 'compact' ? '紧凑' : '舒适') + '」', variant: 'neutral', key: 'density' });
}
function applyMotion() {
  if (state.motion === 'reduced') document.documentElement.setAttribute('data-reduce-motion', 'true');
  else document.documentElement.removeAttribute('data-reduce-motion');
  $id('motionSwitch').checked = state.motion === 'reduced';
}
function setMotion(on) {
  state.motion = on ? 'reduced' : 'full';
  lsSet(LS.motion, state.motion);
  applyMotion();
  toast({ title: on ? '已开启减少动效' : '已关闭减少动效', variant: 'neutral', key: 'motion' });
}

/* ---------- 侧栏（折叠 / 抽屉 / 调宽） ---------- */
function isMobile() { return window.matchMedia('(max-width: 820px)').matches; }
function applyCollapsed() {
  $id('appRoot').toggleAttribute('data-collapsed', !!state.sidebarCollapsed);
}
function toggleSidebar() {
  if (isMobile()) {
    if ($id('appRoot').classList.contains('drawer-open')) closeDrawer();
    else openDrawer();
    return;
  }
  state.sidebarCollapsed = !state.sidebarCollapsed;
  lsSet(LS.sidebar, state.sidebarCollapsed ? '1' : '0');
  applyCollapsed();
}
function openDrawer() {
  $id('appRoot').classList.add('drawer-open');
  $id('drawerBackdrop').hidden = false;
}
function closeDrawer() {
  $id('appRoot').classList.remove('drawer-open');
  var b = $id('drawerBackdrop');
  setTimeout(function () { if (!$id('appRoot').classList.contains('drawer-open')) b.hidden = true; }, 310);
}
function applySidebarWidth(w) {
  state.sidebarWidth = w;
  document.documentElement.style.setProperty('--df-sidebar-width', w + 'px');
  lsSet(LS.width, String(w));
}
function initResize() {
  var handle = $id('resizeHandle');
  var dragging = false, startX = 0, startW = 0;
  handle.addEventListener('pointerdown', function (e) {
    if (isMobile()) return;
    dragging = true; startX = e.clientX; startW = state.sidebarWidth;
    $id('appRoot').dataset.resizing = '1';
    try { handle.setPointerCapture(e.pointerId); } catch (err) {}
    e.preventDefault();
  });
  handle.addEventListener('pointermove', function (e) {
    if (!dragging) return;
    applySidebarWidth(Math.round(clamp(startW + (e.clientX - startX), 216, 420)));
  });
  function end() {
    if (!dragging) return;
    dragging = false;
    delete $id('appRoot').dataset.resizing;
  }
  handle.addEventListener('pointerup', end);
  handle.addEventListener('pointercancel', end);
  handle.addEventListener('dblclick', function () { applySidebarWidth(288); });
}

/* ---------- 数据管理 ---------- */
function exportData() {
  var payload = {
    app: 'any2api-chat', version: VERSION, exportedAt: new Date().toISOString(),
    sessions: state.sessions,
    prefs: { theme: state.theme, density: state.density, motion: state.motion, plan: state.plan, model: state.pendingModel, effort: state.pendingEffort }
  };
  try {
    var blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'any2api-chat-export.json';
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 5000);
    toast({ title: '已导出会话数据', desc: '文件已开始下载。', variant: 'success' });
  } catch (e) {
    toast({ title: '导出失败', desc: String(e && e.message || e), variant: 'danger' });
  }
}
function clearAllData() {
  openConfirm({
    title: '清空全部数据？',
    desc: '全部会话、偏好与本地附件记录将被删除，此操作无法撤销。',
    okLabel: '清空',
    onOk: function () {
      Object.keys(LS).forEach(function (k) {
        var v = LS[k];
        if (typeof v === 'string') lsDel(v);
        else Object.keys(v).forEach(function (k2) { lsDel(v[k2]); });
      });
      state.sessions = []; state.activeId = null; state.order = [];
      state.sync.outbox = []; state.sync.since = 0; state.sync.forceFull = true;
      state.attachments = []; state.plan = 'free'; state.theme = 'system';
      state.density = 'compact'; state.motion = 'full'; state.sidebarCollapsed = false;
      state.pendingModel = ''; state.pendingEffort = ''; state.editing = null; state.searchIndex = null;
      state.fileCache.clear();
      clearFileStore();
      applyTheme(); applyDensity(); applyMotion(); applyCollapsed(); applySidebarWidth(288);
      renderComposerAttachments(); updateSendEnabled(); updatePlanUI(); updateThemeUI();
      renderSessionList(); renderChat(); updateTopbar(); updateModelUI(); updatePlaceholder();
      toast({ title: '已清空全部数据', variant: 'success' });
    }
  });
}

/* ============================================================================
 * P1-4 服务端会话同步（/ui/api/sessions E1–E8）
 * ----------------------------------------------------------------------------
 *   - 服务端可用（E1 200）→ server 模式：本地变更按幂等 outbox 推送；启动/聚焦/
 *     可见性切换/跨标签 storage 变更 → E1(since) 拉取；服务端 rev 大者胜；
 *     pinned/order 仍本地保留。
 *   - 404（public 关）/503（store_unavailable）/网络失败 → 本地模式，聊天绝不
 *     阻塞；outbox 落 localStorage，online/focus 重试。
 *   - 409 → 用响应 session+messages 覆盖本地 + toast + 用新 base_rev 重试一次。
 *   - 首连 E1 为空且本地有旧会话 → 逐条 E2（幂等）+ E4 迁移，成功后写
 *     any2api.ui.synced.v1=1；失败可重入。
 * ========================================================================== */

function sessionsURL(q) { return new URL('api/sessions' + (q || ''), document.baseURI).href; }
function sessionURL(id) { return new URL('api/sessions/' + encodeURIComponent(id), document.baseURI).href; }

/* 请求封装：任意状态码都解析 JSON，只有网络失败才 reject。 */
function syncRequest(method, url, body) {
  var opts = { method: method, headers: { 'Accept': 'application/json' }, cache: 'no-store' };
  if (body !== undefined && body !== null) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  return fetch(url, opts).then(function (res) {
    return res.text().then(function (text) {
      var data = null;
      if (text) { try { data = JSON.parse(text); } catch (e) { data = null; } }
      return { status: res.status, ok: res.ok, data: data };
    });
  });
}
function syncErrText(r) {
  var d = r && r.data;
  return (d && d.error && (d.error.message || d.error.type)) || '';
}

/* ---------- 模式与提示条 ---------- */
function setSyncMode(mode) {
  if (state.sync.mode === mode) return;
  state.sync.mode = mode;
  try { document.documentElement.dataset.sync = mode; } catch (e) {}
  if (mode === 'server') clearSyncBanner();
}
function syncBanner(key, variant, title, text) {
  var slot = $id('bannerSlot');
  var prev = slot.querySelector('.banner[data-sync-key="' + key + '"]');
  if (prev) return prev;
  slot.querySelectorAll('.banner[data-sync-key]').forEach(function (el) {
    if (el.dataset.syncKey !== key) el.remove();
  });
  var el = showBanner({ variant: variant, title: title, text: text });
  el.dataset.syncKey = key;
  return el;
}
function clearSyncBanner() {
  var slot = $id('bannerSlot');
  slot.querySelectorAll('.banner[data-sync-key]').forEach(function (el) { el.remove(); });
  if (!slot.children.length) slot.hidden = true;
}
function onSyncOffline(err) {
  state.sync.online = false;
  state.sync.lastError = (err && err.message) || String(err || '');
  if (state.sync.mode === 'server' || state.sync.mode === 'unknown') setSyncMode('offline');
  syncBanner('offline', 'warning', '离线模式', '会话仅保存在本机；网络恢复后会自动补写。聊天不受影响。');
}

/* ---------- 消息 ↔ 服务端块数组（字段映射 W64-4 §2.10-3） ---------- */
function messageStatus(m) {
  if (m.stopped) return 'stopped';
  if (m.error) return 'error';
  if (m.done === false) return 'streaming';
  return 'done';
}
function msgToServer(m) {
  var blocks = [];
  if (typeof m.content === 'string' && m.content !== '') blocks.push({ type: 'text', text: m.content });
  if (m.role === 'assistant') {
    if (m.reasoning) {
      var th = { type: 'thinking', text: m.reasoning };
      if (m.thinkMs) th.ms = m.thinkMs;
      blocks.push(th);
    }
    (m.tools || []).forEach(function (t) {
      blocks.push({ type: 'tool_use', name: t.name || '', args: t.args || '', id: t.id || '', state: t.state || 'done' });
    });
    if (m.error) blocks.push({ type: 'error', message: (m.error && m.error.message) || '生成失败' });
    if (m.usage) blocks.push({ type: 'usage', usage: m.usage });
  }
  (m.attachments || []).forEach(function (a) {
    blocks.push({ type: 'attachment', id: a.id || '', name: a.name || '', size: a.size || 0, mime: a.type || '', file_id: a.fileId || '', thumb: a.thumb || '' });
  });
  if (m.editedAt) blocks.push({ type: 'meta', edited_at: m.editedAt });
  if (!blocks.length) blocks.push({ type: 'text', text: '' });
  return { id: m.id, role: m.role, content: blocks, status: messageStatus(m), created_at: m.ts || now() };
}
function msgFromServer(v) {
  var out = { id: v.id, role: v.role, content: '', ts: v.created_at || now(), done: true };
  var blocks = [];
  if (Array.isArray(v.content)) blocks = v.content;
  else if (typeof v.content === 'string') {
    try { blocks = JSON.parse(v.content); } catch (e) { blocks = [{ type: 'text', text: v.content }]; }
  }
  if (!Array.isArray(blocks)) blocks = [];
  var texts = [], thinks = [], tools = [], atts = [];
  blocks.forEach(function (b) {
    if (!b || typeof b !== 'object') { if (typeof b === 'string') texts.push(b); return; }
    if (b.type === 'text') { if (typeof b.text === 'string') texts.push(b.text); }
    else if (b.type === 'thinking') { if (typeof b.text === 'string') thinks.push(b.text); if (typeof b.ms === 'number') out.thinkMs = b.ms; }
    else if (b.type === 'tool_use') { tools.push({ name: b.name || '', args: typeof b.args === 'string' ? b.args : JSON.stringify(b.args || ''), id: b.id || '', state: b.state || 'done' }); }
    else if (b.type === 'attachment') { atts.push({ id: b.id || '', name: b.name || '', size: b.size || 0, type: b.mime || '', fileId: b.file_id || '', thumb: b.thumb || '', state: 'ready' }); }
    else if (b.type === 'error') { out.error = { message: b.message || '生成失败' }; }
    else if (b.type === 'usage') { if (b.usage && typeof b.usage === 'object') out.usage = b.usage; }
    else if (b.type === 'meta') { if (typeof b.edited_at === 'number') out.editedAt = b.edited_at; }
    else if (typeof b.text === 'string') texts.push(b.text);
    else if (typeof b.content === 'string') texts.push(b.content);
  });
  out.content = texts.join('\n');
  if (thinks.length) out.reasoning = thinks.join('\n');
  if (atts.length) out.attachments = atts;
  if (tools.length) out.tools = tools;
  if (v.status === 'stopped') out.stopped = true;
  else if (v.status === 'error') { if (!out.error) out.error = { message: '生成失败' }; }
  return out;
}

/* ---------- outbox（幂等写队列；localStorage 持久化） ---------- */
function saveOutbox() { lsSet(LS.outbox, JSON.stringify(state.sync.outbox)); }
function hasOp(sid, kind) {
  return state.sync.outbox.some(function (o) { return o.sid === sid && (!kind || o.kind === kind); });
}
function hasPendingOps(sid) { return hasOp(sid, null); }
function localPatchPayload(sid) {
  var s = findSession(sid);
  return { title: (s && s.title) || '', model: (s && s.model) || '' };
}
function syncEnqueue(op) {
  if (!op || !op.sid || !op.kind) return;
  op.id = op.id || uid();
  op.at = now();
  op.owner = state.sync.tabId;
  var box = state.sync.outbox;
  if (op.kind === 'delete') {
    box = box.filter(function (o) { return o.sid !== op.sid; });
  } else if (op.kind === 'create') {
    if (box.some(function (o) { return o.sid === op.sid && (o.kind === 'create' || o.kind === 'put'); })) return;
    if (!op.payload) op.payload = { id: op.sid, title: '', model: '' };
  } else if (op.kind === 'append') {
    if (!op.msgId) return;
    if (box.some(function (o) { return o.sid === op.sid && o.kind === 'append' && o.msgId === op.msgId; })) return;
  } else if (op.kind === 'patch') {
    var prev = null;
    box = box.filter(function (o) {
      if (o.sid === op.sid && o.kind === 'patch') { prev = o; return false; }
      return true;
    });
    op.payload = Object.assign({}, (prev && prev.payload) || {}, op.payload || localPatchPayload(op.sid));
  } else if (op.kind === 'put') {
    box = box.filter(function (o) { return !(o.sid === op.sid && o.kind === 'create'); });
  }
  box.push(op);
  state.sync.outbox = box;
  saveOutbox();
  if (state.sync.mode === 'server') setTimeout(function () { drainOutbox(); }, 0);
}
function pickOp() {
  var t = now();
  for (var i = 0; i < state.sync.outbox.length; i++) {
    var op = state.sync.outbox[i];
    if ((op.fails || 0) >= 3) continue;   // 死信：不阻塞后续 op（仍留在 outbox 供诊断）
    if (op.owner === state.sync.tabId || !op.at || (t - op.at) > 15000) return op;
  }
  return null;
}
function removeOp(op) {
  state.sync.outbox = state.sync.outbox.filter(function (o) { return o.id !== op.id; });
  saveOutbox();
}

/* ---------- 本地 ↔ 服务端会话应用 ---------- */
function adoptServerSession(sid, view) {
  if (!view) return;
  var s = findSession(sid);
  if (!s) return;
  if (typeof view.rev === 'number') s.rev = view.rev;
  if (view.updated_at) s.updatedAt = view.updated_at;
  if (!s.title && typeof view.title === 'string') s.title = view.title;
  saveSessions();
}
function applyServerSnapshot(s, v, msgs) {
  if (!s || !v) return;
  if (typeof v.title === 'string') s.title = v.title;
  if (typeof v.model === 'string') s.model = v.model;
  if (typeof v.rev === 'number') s.rev = v.rev;
  if (v.created_at) s.createdAt = v.created_at;
  if (v.updated_at) s.updatedAt = v.updated_at;
  s.messages = (msgs || []).map(msgFromServer);
}
function addServerSession(v, msgs) {
  var s = {
    id: v.id, title: v.title || '', model: v.model || '',
    createdAt: v.created_at || now(), updatedAt: v.updated_at || now(),
    rev: v.rev || 0, effort: '', pinned: false, titleAuto: false,
    messages: (msgs || []).map(msgFromServer)
  };
  state.sessions.push(s);
  return s;
}
function removeLocalSession(id) {
  var i = -1;
  for (var k = 0; k < state.sessions.length; k++) if (state.sessions[k].id === id) { i = k; break; }
  if (i < 0) return false;
  state.sessions.splice(i, 1);
  state.order = state.order.filter(function (x) { return x !== id; });
  saveOrder();
  if (state.activeId === id) { state.activeId = null; saveActive(); }
  return true;
}
function fetchServerSession(id) {
  return syncRequest('GET', sessionURL(id)).then(function (r) {
    if (r.status === 200 && r.data) return { session: r.data.session, messages: r.data.messages || [] };
    if (r.status === 404 || r.status === 503) return null;
    throw new Error('HTTP ' + r.status);
  });
}
function applyConflictSnapshot(sid, data) {
  var s = findSession(sid);
  if (!s || !data || !data.session) return;
  var streamingHere = !!(state.streaming && state.streaming.sessionId === sid);
  var keep = [];
  if (streamingHere) {
    /* 409 覆盖发生在生成流中时，保留服务端尚不知道的在途消息（用户消息 +
       流式助手），否则聊天会被打断、助手回复丢失。 */
    var serverIds = {};
    (data.messages || []).forEach(function (m) { if (m && m.id) serverIds[m.id] = true; });
    s.messages.forEach(function (m) { if (m && m.id && !serverIds[m.id]) keep.push(m); });
  }
  applyServerSnapshot(s, data.session, data.messages || []);
  if (keep.length) s.messages = s.messages.concat(keep);
  saveSessions();
  renderSessionList();
  if (state.activeId === sid) renderChat();
  updateTopbar(); updateModelUI();
}

/* ---------- 推送（drain） ---------- */
async function drainOutbox() {
  if (state.sync.draining || state.sync.mode !== 'server') return false;
  state.sync.draining = true;
  var progressed = false;
  try {
    for (;;) {
      var op = pickOp();
      if (!op) break;
      var ok = await executeOp(op, false);
      if (!ok) break;
      removeOp(op);
      progressed = true;
    }
  } catch (e) {
    onSyncOffline(e);
  } finally {
    state.sync.draining = false;
  }
  return progressed;
}
function appendOk(op, data) {
  var s = findSession(op.sid);
  if (!s) return;
  if (data && typeof data.rev === 'number') s.rev = data.rev;
  s.updatedAt = Math.max(s.updatedAt || 0, now());
  if (data && data.message && !s.messages.some(function (m) { return m.id === data.message.id; })) {
    s.messages.push(msgFromServer(data.message));
  }
  saveSessions();
  renderSessionList();
  if (state.activeId === s.id && !(state.streaming && state.streaming.sessionId === s.id)) renderChat();
}
async function executePut(op, isRetry) {
  var s = findSession(op.sid);
  var pl = op.payload || {};
  var body = {
    base_rev: s ? (s.rev || 0) : 0,
    title: (s ? s.title : pl.title) || '',
    model: (s ? s.model : pl.model) || '',
    messages: pl.messages || (s ? s.messages.map(msgToServer) : [])
  };
  var r = await syncRequest('PUT', sessionURL(op.sid), body);
  if (r.status === 200 || r.status === 201) {
    if (s && r.data && r.data.session) applyServerSnapshot(s, r.data.session, r.data.messages || []);
    saveSessions();
    renderSessionList();
    if (state.activeId === op.sid && !(state.streaming && state.streaming.sessionId === op.sid)) renderChat();
    updateTopbar();
    return true;
  }
  return opFailure(op, r, isRetry);
}
function opFailure(op, r, isRetry) {
  var s = findSession(op.sid);
  if (r.status === 409) {
    if (r.data && r.data.session) applyConflictSnapshot(op.sid, r.data);
    if (!isRetry) {
      toast({ title: '已在其他标签页更新', desc: '已应用服务端最新版本，并用新 rev 重试一次。', variant: 'warning', key: 'sync-conflict' });
      return executeOp(op, true);
    }
    toast({ title: '同步冲突未解决', desc: '本地保留待重试，稍后自动补写。', variant: 'warning', key: 'sync-conflict-2' });
    op.fails = (op.fails || 0) + 1;
    saveOutbox();
    return false;
  }
  if (r.status === 401) {
    state.sync.authBlocked = true;
    setSyncMode('local');
    syncBanner('auth', 'danger', '需要访问密钥', '服务端拒绝了会话同步（401）。已退回本地存储模式。');
    return false;
  }
  if (r.status === 503) {
    setSyncMode('local');
    syncBanner('store', 'warning', '会话同步暂不可用', '服务端存储未启用（store_unavailable）。已退回本地存储，聊天不受影响。');
    return false;
  }
  if (r.status === 404) {
    var msg = syncErrText(r);
    if (/session not found/i.test(msg)) {
      if (s) s.rev = 0;   // 服务端已无此会话：保留本地副本，清零 rev 待下次重建
      saveSessions();
      return true;
    }
    setSyncMode('local');   // public 关闭等：整体退回本地
    return false;
  }
  if (r.status === 429) {
    scheduleSync('retry', 2000);
    return false;
  }
  if (r.status >= 400 && r.status < 500) {
    toast({ title: '会话同步被拒绝', desc: 'HTTP ' + r.status + (syncErrText(r) ? '：' + syncErrText(r) : ''), variant: 'warning', key: 'sync-reject' });
    return true;   // 无效 op 不毒化队列
  }
  state.sync.lastError = 'HTTP ' + r.status;
  return false;
}
async function executeOp(op, isRetry) {
  if (op.kind === 'delete') {
    var rd = await syncRequest('DELETE', sessionURL(op.sid));
    if (rd.status === 200 || rd.status === 404) return true;
    return opFailure(op, rd, isRetry);
  }
  if (op.kind === 'create') {
    var pl0 = op.payload || {};
    var s0 = findSession(op.sid);
    var r0 = await syncRequest('POST', sessionsURL(), {
      id: op.sid,
      title: (s0 ? (s0.title || '') : (pl0.title || '')),
      model: (s0 ? (s0.model || '') : (pl0.model || ''))
    });
    if (r0.status === 200 || r0.status === 201) {
      adoptServerSession(op.sid, r0.data && r0.data.session);
      return true;
    }
    return opFailure(op, r0, isRetry);
  }
  if (op.kind === 'patch') {
    var s1 = findSession(op.sid);
    var pl1 = op.payload || {};
    if (!s1) return true;
    if (!s1.rev) return executePut(op, isRetry);
    var r1 = await syncRequest('PATCH', sessionURL(op.sid), { base_rev: s1.rev, title: pl1.title || '', model: pl1.model || '' });
    if (r1.status === 200) {
      adoptServerSession(op.sid, r1.data && r1.data.session);
      if (typeof pl1.title === 'string') s1.title = pl1.title;
      if (typeof pl1.model === 'string') s1.model = pl1.model;
      saveSessions();
      updateTopbar();
      return true;
    }
    return opFailure(op, r1, isRetry);
  }
  if (op.kind === 'append') {
    var s2 = findSession(op.sid);
    if (s2 && !s2.rev) return executePut(op, isRetry);
    var payload = op.payload;
    if (!payload) return true;
    var req = {
      id: payload.id, role: payload.role, content: payload.content,
      status: payload.status, created_at: payload.created_at,
      base_rev: s2 ? s2.rev : 0
    };
    var r2 = await syncRequest('POST', sessionURL(op.sid) + '/messages', req);
    if (r2.status === 200 || r2.status === 201) { appendOk(op, r2.data); return true; }
    return opFailure(op, r2, isRetry);
  }
  if (op.kind === 'put') return executePut(op, isRetry);
  return true;
}

/* ---------- 拉取（E1 + E3 水合） ---------- */
async function applyServerList(data) {
  var views = Array.isArray(data && data.sessions) ? data.sessions : [];
  var changed = false;
  for (var i = 0; i < views.length; i++) {
    var v = views[i];
    if (!v || typeof v.id !== 'string') continue;
    if (v.deleted) {
      if (!hasPendingOps(v.id) && !(state.streaming && state.streaming.sessionId === v.id)) {
        if (removeLocalSession(v.id)) changed = true;
      }
      continue;
    }
    var s = findSession(v.id);
    if (!s) {
      var full = await fetchServerSession(v.id);
      if (full) { addServerSession(full.session, full.messages); changed = true; }
      continue;
    }
    if ((s.rev || 0) >= v.rev) continue;              // 服务端不比本地新
    if (hasPendingOps(v.id)) continue;                // 本地未同步项优先
    if (state.streaming && state.streaming.sessionId === v.id) continue;
    var full2 = await fetchServerSession(v.id);
    if (full2) { applyServerSnapshot(s, full2.session, full2.messages); changed = true; }
  }
  if (changed) {
    saveSessions();
    renderSessionList();
    if (state.activeId) { renderChat(); updateTopbar(); updateModelUI(); updatePlaceholder(); }
  }
  return changed;
}

/* ---------- 首连迁移（E2 + E4；可重入） ---------- */
function migrateFail(r, e) {
  if (r && r.status === 401) {
    state.sync.authBlocked = true;
    setSyncMode('local');
    syncBanner('auth', 'danger', '需要访问密钥', '服务端拒绝了会话同步（401）。已退回本地存储模式。');
    return;
  }
  if (r && r.status === 503) {
    setSyncMode('local');
    syncBanner('store', 'warning', '会话同步暂不可用', '服务端存储未启用（store_unavailable）。已退回本地存储，聊天不受影响。');
    return;
  }
  if (r && r.status === 404) { setSyncMode('local'); return; }
  if (e) onSyncOffline(e);
}
async function migrateOne(s) {
  var r = await syncRequest('POST', sessionsURL(), { id: s.id, title: s.title || '', model: s.model || '' });
  if (r.status !== 200 && r.status !== 201) { migrateFail(r, null); return false; }
  var rev = (r.data && r.data.session && r.data.session.rev) || 1;
  var messages = s.messages.map(msgToServer);
  var p = await syncRequest('PUT', sessionURL(s.id), { base_rev: rev, title: s.title || '', model: s.model || '', messages: messages });
  if (p.status === 409 && p.data && p.data.session) {
    p = await syncRequest('PUT', sessionURL(s.id), { base_rev: p.data.session.rev, title: s.title || '', model: s.model || '', messages: messages });
  }
  if (p.status !== 200 && p.status !== 201) { migrateFail(p, null); return false; }
  if (p.data && p.data.session) {
    s.rev = p.data.session.rev;
    if (p.data.session.created_at) s.createdAt = p.data.session.created_at;
    if (p.data.session.updated_at) s.updatedAt = p.data.session.updated_at;
  } else { s.rev = rev + 1; }
  saveSessions();
  return true;
}
async function migrateUnsyncedLocal() {
  if (lsGet(LS.synced) === '1') return;
  var pending = state.sessions.filter(function (s) { return !s.rev; });
  if (!pending.length) { lsSet(LS.synced, '1'); return; }
  for (var i = 0; i < pending.length; i++) {
    var ok = await migrateOne(pending[i]);
    if (!ok) return;   // 失败可重入：下次 syncNow 重来（E2/E4 幂等）
  }
  lsSet(LS.synced, '1');
  saveSessions();
}
function ensureLocalCreates() {
  state.sessions.forEach(function (s) {
    if (!s.rev && !hasOp(s.id, 'create') && !hasOp(s.id, 'put')) {
      syncEnqueue({ kind: 'create', sid: s.id, payload: { id: s.id, title: s.title || '', model: s.model || '' } });
    }
  });
}

/* ---------- 主管道 ---------- */
async function syncNow(reason) {
  if (state.sync.busy) { state.sync.again = true; return; }
  state.sync.busy = true;
  try {
    var since = state.sync.forceFull ? 0 : state.sync.since;
    var r = await syncRequest('GET', sessionsURL('?since=' + since));
    if (r.status === 200) {
      setSyncMode('server');
      state.sync.authBlocked = false;
      state.sync.online = true;
      var data = r.data || {};
      await migrateUnsyncedLocal();
      ensureLocalCreates();
      await drainOutbox();
      await applyServerList(data);
      if (typeof data.next_since === 'number' && data.next_since > 0) {
        state.sync.since = data.next_since;
        lsSet(LS.syncSince, String(data.next_since));
      }
      if (state.sessions.every(function (s) { return !!s.rev; })) lsSet(LS.synced, '1');
      state.sync.lastPull = now();
      state.sync.pulls += 1;
      state.sync.forceFull = false;
      return;
    }
    if (r.status === 401) {
      state.sync.authBlocked = true;
      setSyncMode('local');
      syncBanner('auth', 'danger', '需要访问密钥', '服务端拒绝了会话同步（401）。已退回本地存储模式。');
      return;
    }
    if (r.status === 503) {
      setSyncMode('local');
      syncBanner('store', 'warning', '会话同步暂不可用', '服务端存储未启用（store_unavailable）。已退回本地存储，聊天不受影响。');
      return;
    }
    if (r.status === 404) { clearSyncBanner(); setSyncMode('local'); return; }
    if (r.status === 429) { scheduleSync('retry', 2000); return; }
    state.sync.lastError = 'HTTP ' + r.status;
  } catch (e) {
    onSyncOffline(e);
  } finally {
    state.sync.busy = false;
    if (state.sync.again) { state.sync.again = false; scheduleSync('again', 80); }
  }
}
function scheduleSync(reason, delay) {
  clearTimeout(state.sync.timer);
  state.sync.timer = setTimeout(function () {
    state.sync.timer = 0;
    syncNow(reason);
  }, typeof delay === 'number' ? delay : 250);
}
function syncAfterStream(s, msg) {
  if (s.syncFull) {
    s.syncFull = false;
    syncEnqueue({ kind: 'put', sid: s.id, payload: { title: s.title || '', model: s.model || '', messages: s.messages.map(msgToServer) } });
    return;
  }
  if (s.messages.indexOf(msg) < 0) return;   // 空流被移除，无内容可落账
  syncEnqueue({ kind: 'append', sid: s.id, msgId: msg.id, payload: msgToServer(msg) });
}
function syncInit() {
  try { document.documentElement.dataset.sync = state.sync.mode; } catch (e) {}
  window.addEventListener('focus', function () { scheduleSync('focus'); });
  document.addEventListener('visibilitychange', function () { if (!document.hidden) scheduleSync('visible'); });
  window.addEventListener('online', function () { state.sync.online = true; scheduleSync('online'); });
  window.addEventListener('offline', function () { state.sync.online = false; onSyncOffline(new Error('browser offline')); });
  window.addEventListener('storage', function (e) {
    if (e && e.key === LS.sessions && e.newValue !== e.oldValue) scheduleSync('storage');
  });
  scheduleSync('startup', 0);
}

/* ---------- 全局键盘 ---------- */
function onGlobalKeydown(e) {
  var mod = e.ctrlKey || e.metaKey;
  if (mod && !e.shiftKey && !e.altKey) {
    var k = (e.key || '').toLowerCase();
    if (k === 'k') { e.preventDefault(); togglePalette('quick'); return; }
    if (k === 'b') { e.preventDefault(); toggleSidebar(); return; }
    if (k === 'n') { e.preventDefault(); newChat(); return; }
    if (k === ',') { e.preventDefault(); openSettings(); return; }
    if (k === '/') { e.preventDefault(); openShortcuts(); return; }
  }
  if (mod && e.shiftKey && (e.key || '').toLowerCase() === 'k') { e.preventDefault(); openPalette('search'); return; }
  if (mod && e.shiftKey && (e.key || '').toLowerCase() === 'o') { e.preventDefault(); newChat(); return; }
  if (e.key === 'Escape') {
    if ($id('appRoot').classList.contains('drawer-open')) { e.preventDefault(); closeDrawer(); return; }
    if (closeTopOverlay()) { e.preventDefault(); return; }
    if (state.streaming) { e.preventDefault(); stopStream(); }
  }
}
function onTabTrap(e) {
  if (e.key !== 'Tab') return;
  var top = openStack[openStack.length - 1];
  if (!top || !OVERLAYS[top] || !OVERLAYS[top].modal) return;
  var dlg = OVERLAYS[top].el;
  var focusables = dlg.querySelectorAll('button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])');
  if (!focusables.length) return;
  var first = focusables[0], last = focusables[focusables.length - 1];
  if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
  else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
}

/* ---------- 启动引导层 ----------
 * 首帧渲染 + 模型配置落定（上限 1.4s，绝不因网络挂起而锁屏）后淡出；
 * 最短展示 320ms，避免一闪而过。prefers-reduced-motion / 减少动效 → 无动画直隐。 */
var BOOT_MIN_MS = 320;
var BOOT_MAX_WAIT_MS = 1400;
function bootNow() {
  return (window.performance && performance.now) ? performance.now() : Date.now();
}
function bootReduceMotion() {
  if (state.motion === 'reduced') return true;
  if (document.documentElement.getAttribute('data-reduce-motion') === 'true') return true;
  return !!(window.matchMedia && matchMedia('(prefers-reduced-motion: reduce)').matches);
}
function hideBoot() {
  var boot = $id('boot');
  if (!boot || boot.hidden) return;
  document.documentElement.classList.remove('preload');   // 让淡出过渡真正生效
  document.documentElement.classList.add('app-ready');
  boot.classList.add('is-leaving');
  setTimeout(function () {
    boot.hidden = true;
    boot.classList.remove('is-leaving');
  }, bootReduceMotion() ? 0 : 280);
}
function scheduleBootExit(configReady) {
  var boot = $id('boot');
  if (!boot) return;
  var t0 = (typeof window.__bootT0 === 'number') ? window.__bootT0 : bootNow();
  var settled = false;
  function exitWhenReady() {
    if (settled) return;
    settled = true;
    var remain = Math.max(0, BOOT_MIN_MS - (bootNow() - t0));
    setTimeout(hideBoot, remain);
  }
  var cap = setTimeout(exitWhenReady, BOOT_MAX_WAIT_MS);
  Promise.resolve(configReady).then(function () { clearTimeout(cap); exitWhenReady(); },
                                   function () { clearTimeout(cap); exitWhenReady(); });
}

/* ---------- 初始化 ---------- */
function init() {
  loadState();
  applyTheme(); applyDensity(); applyMotion(); applyCollapsed(); applySidebarWidth(state.sidebarWidth);

  if (window.DOMPurify) {
    DOMPurify.addHook('afterSanitizeAttributes', function (node) {
      if (node.tagName === 'A') {
        node.setAttribute('target', '_blank');
        node.setAttribute('rel', 'noopener noreferrer');
      }
    });
  }
  if (window.marked && marked.setOptions) marked.setOptions({ gfm: true, breaks: true });

  renderSessionList(); renderChat(); updateTopbar(); updateSendEnabled();
  scrollToBottom(false);   // 打开已有会话时定位到最新消息
  updateModelUI(); updatePlanUI(); updateThemeUI(); updatePlaceholder();
  renderShortcutRows($id('shortcutsPaneList'));
  renderShortcutRows($id('shortcutsModalList'));
  pruneFileStore();

  /* —— 浮层注册（Esc / 遮罩关闭依赖此表） —— */
  registerOverlay('settings', $id('settingsDialog'), true, closeSettings);
  registerOverlay('subscription', $id('subscriptionDialog'), true, closeSubscription);
  registerOverlay('shortcuts', $id('shortcutsDialog'), true, closeShortcuts);
  registerOverlay('confirm', $id('confirmDialog'), true, dismissConfirm);
  registerOverlay('imageViewer', $id('imageViewer'), true, closeImageViewer);
  registerOverlay('palette', $id('commandPalette'), true, closePalette);
  registerOverlay('userMenu', $id('userMenu'), false, closeUserMenu);
  registerOverlay('sessionMenu', $id('sessionMenu'), false, closeSessionMenu);
  registerOverlay('modelMenu', $id('modelMenu'), false, closeModelMenu);

  var configReady = initConfig();
  syncInit();
  scheduleBootExit(configReady);   // 首帧 + 配置落定后淡出（最短 320ms / 上限 1.4s）

  /* —— 输入区 —— */
  var input = $id('input');
  input.addEventListener('input', function () { autoGrow(input); updateSendEnabled(); });
  input.addEventListener('keydown', function (e) {
    if (e.key === 'Enter' && !e.shiftKey && !e.isComposing && e.keyCode !== 229) {
      e.preventDefault();
      if (!state.streaming) sendCurrent();
    }
  });
  input.addEventListener('paste', function (e) {
    var items = (e.clipboardData && e.clipboardData.files) || [];
    if (items.length) { addFiles(items); }
  });
  $id('sendBtn').addEventListener('click', sendCurrent);
  $id('stopBtn').addEventListener('click', stopStream);
  $id('attachBtn').addEventListener('click', function () { $id('fileInput').click(); });
  $id('fileInput').addEventListener('change', function () { addFiles(this.files); this.value = ''; });
  $id('fileInput').setAttribute('accept', ATT_ACCEPT);
  $id('modelPillBtn').addEventListener('click', function () {
    $id('modelMenu').hidden ? openModelMenu() : closeModelMenu();
  });
  $id('modelMenu').addEventListener('keydown', function (e) {
    /* 档位条（横向）与模型行（纵向）共用一套顺序：↑↓ 前后移动，
     * ←→ 在档位条内横移；焦点移动后把档位项滚入可视区。 */
    var items = Array.prototype.slice.call(this.querySelectorAll('.menu-item, .effort-opt'));
    if (!items.length) return;
    var i = items.indexOf(document.activeElement);
    if (i < 0) {                              /* 焦点不在菜单项上：只有方向键才抢占焦点 */
      if (e.key.indexOf('Arrow') === 0) { e.preventDefault(); items[0].focus(); }
      return;
    }
    var step = 0;
    if (e.key === 'ArrowDown') step = 1;
    else if (e.key === 'ArrowUp') step = -1;
    else if (e.key === 'ArrowRight' && items[i].classList.contains('effort-opt')) step = 1;
    else if (e.key === 'ArrowLeft' && items[i].classList.contains('effort-opt')) step = -1;
    else return;
    e.preventDefault();
    var next = items[(i + step + items.length) % items.length];
    next.focus();
    if (next.classList.contains('effort-opt') && next.scrollIntoView) {
      next.scrollIntoView({ block: 'nearest', inline: 'nearest' });
    }
  });
  $id('dictateBtn').addEventListener('click', function () {
    toast({ title: '语音输入暂不可用', desc: '该功能将在后续版本接入。', variant: 'neutral' });
  });

  /* —— 拖放 —— */
  var composer = $id('composer');
  var overlay = $id('dropOverlay');
  var dragDepth = 0;
  function hasFiles(e) {
    return e.dataTransfer && Array.prototype.indexOf.call(e.dataTransfer.types || [], 'Files') !== -1;
  }
  function dragFileType(e) {
    var items = (e.dataTransfer && e.dataTransfer.items) || [];
    for (var i = 0; i < items.length; i++) {
      if (items[i].kind === 'file' && items[i].type) return items[i].type;
    }
    return '';
  }
  function setDropReject(on) {
    overlay.dataset.reject = on ? '1' : '0';
    overlay.querySelector('.drop-text').textContent = on ? '不支持的文件类型' : '拖放图片以添加附件';
    overlay.querySelector('.drop-hint').textContent = on ? '仅支持 PNG / JPEG / GIF / WebP' : '图片将随消息发送（单张 ≤20 MB，最多 10 张）';
  }
  function dragIsReject(e) {
    var t = dragFileType(e);
    return !!t && ATT_TYPES.indexOf(t) === -1;
  }
  composer.addEventListener('dragenter', function (e) {
    if (!hasFiles(e)) return;
    e.preventDefault();
    dragDepth++;
    setDropReject(dragIsReject(e));
    overlay.hidden = false;
  });
  composer.addEventListener('dragover', function (e) {
    if (!hasFiles(e)) return;
    e.preventDefault();
    setDropReject(dragIsReject(e));
  });
  composer.addEventListener('dragleave', function (e) {
    if (!hasFiles(e)) return;
    dragDepth = Math.max(0, dragDepth - 1);
    if (!dragDepth) { overlay.hidden = true; setDropReject(false); }
  });
  composer.addEventListener('drop', function (e) {
    if (!hasFiles(e)) return;
    e.preventDefault();
    dragDepth = 0;
    overlay.hidden = true;
    setDropReject(false);
    addFiles(e.dataTransfer.files);
  });
  window.addEventListener('dragover', function (e) { e.preventDefault(); });
  window.addEventListener('drop', function (e) { e.preventDefault(); });

  /* —— 图片查看器 —— */
  $id('imageViewer').addEventListener('click', function (e) {
    if (e.target === this || e.target.closest('[data-close="imageViewer"]')) closeImageViewer();
  });

  /* —— 侧栏 / 导航 —— */
  $id('newChatBtn').addEventListener('click', newChat);
  $id('collapsedNewBtn').addEventListener('click', newChat);
  $id('sidebarToggleBtn').addEventListener('click', toggleSidebar);
  $id('collapsedToggleBtn').addEventListener('click', toggleSidebar);
  $id('drawerBtn').addEventListener('click', openDrawer);
  $id('drawerBackdrop').addEventListener('click', closeDrawer);
  $id('searchBtn').addEventListener('click', function () { openPalette('search'); });
  $id('settingsBtn').addEventListener('click', function () { openSettings(); });
  $id('navScroll').addEventListener('scroll', function () {
    if (this.scrollTop > 8) this.setAttribute('data-scrolled', '1');
    else this.removeAttribute('data-scrolled');
  }, { passive: true });

  /* —— 用户菜单（W80-1：四个动作 + 底部用户/套餐行；「关于」并入齿轮 → 设置 → 关于页） —— */
  $id('userMenuBtn').addEventListener('click', function (e) { e.stopPropagation(); toggleUserMenu(); });
  $id('umSettings').addEventListener('click', function () { closeUserMenu(); openSettings(); });
  $id('umSubscription').addEventListener('click', function () { closeUserMenu(); openSubscription(); });
  $id('umShortcuts').addEventListener('click', function () { closeUserMenu(); openShortcuts(); });
  $id('umUsage').addEventListener('click', function () {
    closeUserMenu();
    var n = state.sessions.length;
    var msgs = state.sessions.reduce(function (a, s) { return a + s.messages.length; }, 0);
    toast({ title: '使用情况', desc: '本地共 ' + n + ' 个会话、' + msgs + ' 条消息；' + (state.rateLimitPerMin ? '速率限制 ' + state.rateLimitPerMin + ' 次/分钟。' : '服务未配置速率限制。'), variant: 'neutral', key: 'usage' });
  });
  $id('umAppearance').addEventListener('click', function () { closeUserMenu(); openSettings('pane-appearance'); });

  /* —— 设置弹窗 —— */
  $id('settingsDialog').querySelectorAll('[data-close]').forEach(function (b) {
    b.addEventListener('click', closeSettings);
  });
  $id('settingsDialog').querySelectorAll('.s-nav-item').forEach(function (b) {
    b.addEventListener('click', function () { selectSettingsPane(b.dataset.pane); });
  });
  $id('themeSeg').querySelectorAll('.seg-item').forEach(function (b) {
    b.addEventListener('click', function () { setTheme(b.dataset.themeVal); });
  });
  $id('densitySeg').querySelectorAll('.seg-item').forEach(function (b) {
    b.addEventListener('click', function () { setDensity(b.dataset.densityVal); });
  });
  $id('motionSwitch').addEventListener('change', function () { setMotion(this.checked); });
  $id('openSubscriptionBtn').addEventListener('click', function () { openSubscription(); });
  $id('exportDataBtn').addEventListener('click', exportData);
  $id('clearDataBtn').addEventListener('click', clearAllData);
  $id('checkUpdateBtn').addEventListener('click', function () {
    toast({ title: '已是最新版本', desc: '当前 ' + VERSION + '（零构建 UI）。', variant: 'success' });
  });

  /* —— 订阅弹窗 —— */
  $id('subscriptionDialog').querySelectorAll('[data-close]').forEach(function (b) {
    b.addEventListener('click', closeSubscription);
  });
  $id('subCtaFree').addEventListener('click', function () { if (state.plan !== 'free') setPlan('free'); });
  $id('subCtaPro').addEventListener('click', function () { if (state.plan !== 'pro') setPlan('pro'); });
  $id('subCtaUltra').addEventListener('click', function () { if (state.plan !== 'ultra') setPlan('ultra'); });

  /* —— 快捷键面板 / 确认框 —— */
  $id('shortcutsDialog').querySelectorAll('[data-close]').forEach(function (b) {
    b.addEventListener('click', closeShortcuts);
  });
  $id('confirmCancel').addEventListener('click', dismissConfirm);
  $id('confirmOk').addEventListener('click', function () {
    var fn = state.confirmAction;
    closeConfirm();
    if (fn) fn();
  });

  /* —— 命令面板 —— */
  $id('paletteInput').addEventListener('input', function () { buildPaletteItems(this.value, state.paletteMode); });
  $id('paletteInput').addEventListener('keydown', function (e) {
    var list = $id('paletteList');
    var items = list._items || [];
    if (e.key === 'ArrowDown') { e.preventDefault(); if (items.length) setPaletteHl((paletteHl + 1) % items.length); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); if (items.length) setPaletteHl((paletteHl - 1 + items.length) % items.length); }
    else if (e.key === 'Enter') { e.preventDefault(); runPaletteItem(items[paletteHl]); }
    else if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); closePalette(); }
  });
  $id('paletteScrim').addEventListener('click', closePalette);

  /* —— 遮罩 / 全局 —— */
  initTips();   // data-tip 锚点：pointerdown / 菜单打开时抑制 tip，离开 / blur / 关闭恢复
  $id('scrim').addEventListener('click', function () { closeTopOverlay(); });
  document.addEventListener('mousedown', function (e) {
    ['userMenu', 'sessionMenu', 'modelMenu'].forEach(function (n) {
      var el = $id(n);
      if (!el || el.hidden) return;
      if (e.target.closest('#' + n)) return;
      if (n === 'userMenu' && e.target.closest('#userMenuBtn')) return;
      if (n === 'modelMenu' && e.target.closest('#modelPillBtn')) return;
      var closer = { userMenu: closeUserMenu, sessionMenu: closeSessionMenu, modelMenu: closeModelMenu }[n];
      closer();
    });
  });
  document.addEventListener('keydown', onGlobalKeydown);
  document.addEventListener('keydown', onTabTrap);

  /* —— 滚动 / 跳转 —— */
  $id('jumpBtn').addEventListener('click', function () { scrollToBottom(true); });
  $id('chatScroll').addEventListener('scroll', updateJumpBtn, { passive: true });

  /* —— 模式分段（代码模式占位） —— */
  $id('modePills').querySelectorAll('.df-pill').forEach(function (b) {
    b.addEventListener('click', function () {
      if (b.dataset.mode === 'code') {
        toast({ title: '代码模式即将推出', desc: '当前版本仅提供对话模式。', variant: 'neutral' });
        return;
      }
      $id('modePills').querySelectorAll('.df-pill').forEach(function (x) { x.removeAttribute('data-active'); });
      b.setAttribute('data-active', '');
      $id('modePills').dataset.seg = '0';
    });
  });

  /* —— 视口变化：抽屉清理 —— */
  window.addEventListener('resize', function () {
    if (!isMobile()) {
      $id('appRoot').classList.remove('drawer-open');
      $id('drawerBackdrop').hidden = true;
    }
  });

  /* —— 测试钩子（e2e / 截图用） —— */
  window.__any2api = {
    version: VERSION,
    state: state,
    openSettings: openSettings,
    openSubscription: openSubscription,
    openShortcuts: openShortcuts,
    openPalette: openPalette,
    closeTopOverlay: closeTopOverlay,
    setTheme: setTheme,
    setPlan: setPlan,
    newChat: newChat,
    stop: stopStream,
    toast: toast,
    showBanner: showBanner,
    /* P1-4 同步钩子（e2e / 诊断） */
    sync: state.sync,
    syncNow: function (r) { return syncNow(r || 'manual'); },
    syncFlush: function () { return drainOutbox(); },
    syncMode: function () { return state.sync.mode; },
    demoBanner: function () {
      return showBanner({
        variant: 'warning',
        title: '本周免费消息额度即将用尽。',
        text: '将于 10 月 18 日 09:00 重置。',
        action: { label: '升级套餐', run: openSubscription }
      });
    }
  };
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
else init();
})();
