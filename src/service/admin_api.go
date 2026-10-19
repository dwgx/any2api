// admin_api.go —— P1-5 管理面只读视图 + 吊销入口（board A2A-L19-W66-2；
// 设计 `claude2api-public-service` §5.1 / W64-4 §2.9）。
//
// 四个端点（全部挂 adminAuth；未配 admin_key = 路由根本不注册 → 404）：
//
//	GET  /admin/pool             凭据池快照（只回下标/冷却/使用时刻；零凭据字节）
//	GET  /admin/sessions         会话清单（sid 只回 8 字符前缀；游标翻页）
//	GET  /admin/usage            台账汇总（global|session|key|ip；UTC 日）
//	POST /admin/sessions/revoke  吊销（sid 前缀 / key_id / all；幂等面）
//
// 数据源：
//   - `/admin/pool` = Config 的凭据池（`credentialPoolAt`，与请求面同一份状态）
//   - `Limiter.stats()` + 日预算台账（`budgetLedger.stats`）——**不依赖 store**；
//   - `/admin/sessions` / `/admin/usage` / revoke = Store（未启用 → 503
//     `store_unavailable`，不是 500）。
//
// 纪律：
//   - **零凭据字节**：池只回下标与冷却时刻（绝不回凭据值/长度以外的任何内容）；
//     sid 只回 8 字符前缀；key_id 已是 sha8、`ip:` scope 已是哈希——响应里不出现
//     cookie / access key / 凭据原文。
//   - **只读**（revoke 除外）：零 DDL、零迁移、不改 store 语义。
//   - **默认关闭**：`registerAdminAPI` 自门控——未配 admin_key 时一条路由都不
//     注册（既有 `/admin/*` 与四个入口面零变化）。
//
// 与 W64-4 §2.9 的差异（登记）：
//   - `cooldown_class` 不回：`credentialPool` 只存 `coolUntil`，没有类别字段
//     （R8；加字段要改 credentials_pool.go——本波写面不含该文件）。
//   - `used_today_units` / `used_window_5h` 不回：池没有逐凭据用量计数器（R8）。
//   - revoke 的 selector 为 `sid_prefix` / `key_id` / `all` 三选一；吊销实现 =
//     删会话行（与 logout 同路径：内存表 + store 级联），不是只置 `revoked=1`
//     ——`sessionStore.get` 对 revoked 行的既有语义也是「读到即删」，终态一致。
package service

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// adminSessionsDefaultLimit / adminSessionsMaxLimit 会话清单分页（§2.9）。
	adminSessionsDefaultLimit = 50
	adminSessionsMaxLimit     = 200
	// adminUsageDefaultLimit / adminUsageMaxLimit 台账分页（§2.9）。
	adminUsageDefaultLimit = 50
	adminUsageMaxLimit     = 500
	// adminRevokeMaxBody 吊销请求体上限（选择器 + 留白；64KB 足够）。
	adminRevokeMaxBody = 64 << 10
	// adminCursorVersion 会话游标令牌版本（坏版本 = 400）。
	adminCursorVersion = "v1"
	// adminSessionsCursorMinPrefix 吊销前缀最小长度（防「一个字符 = 全吊销」误操作）。
	adminSessionsCursorMinPrefix = 4

	// storeAuditKindSessionRevoke 审计行型别：管理面吊销（§2.9；detail 只含
	// selector 与前缀，不含完整 sid / cookie / key）。
	storeAuditKindSessionRevoke = "session_revoke"

	// usage_counters scope 词表前缀（与 store.go / turn_hook.go 同源；global
	// 见 limits.go storeUsageScopeGlobal）。
	storeUsageScopeSessionPrefix = "session:"
	storeUsageScopeKeyPrefix     = "key:"
	storeUsageScopeIPPrefix      = "ip:"
	// storeUsageScopeModelPrefix 逐模型 scope 前缀（W78-1）：`model:<模型名>`。
	// 写入点 = serve 收尾的 `recordModelUsage`（每请求 +1；见 turn_hook.go）；
	// 模型名**不是秘密**（它来自下游请求体/路径），展示面不回显脱敏——见
	// `adminUsageDisplayScope`。
	storeUsageScopeModelPrefix = "model:"
)

// ---------------------------------------------------------------------------
// 路由注册
// ---------------------------------------------------------------------------

// registerAdminAPI 注册 P1-5 的四个管理面端点。**自门控**：未配 admin_key
// 时一条都不注册（与既有 /admin/* 的惰性注册语义逐字一致：404 非 401）。
//
// 调用点 = `Server.Handler()` 的 admin_key 分支（与 diagnostics/cooldowns/logs
// 三条同一位置）。
func (s *Server) registerAdminAPI(mux *http.ServeMux) {
	if s == nil || s.cfg.adminKey() == "" {
		return
	}
	mux.HandleFunc("GET /admin/pool", s.adminAuth(s.handleAdminPool))
	mux.HandleFunc("GET /admin/sessions", s.adminAuth(s.handleAdminSessions))
	mux.HandleFunc("GET /admin/usage", s.adminAuth(s.handleAdminUsage))
	mux.HandleFunc("POST /admin/sessions/revoke", s.adminAuth(s.handleAdminSessionsRevoke))
}

// ---------------------------------------------------------------------------
// GET /admin/pool
// ---------------------------------------------------------------------------

// adminPoolCredentialView 单凭据快照（**只回索引与状态**；零凭据字节）。
type adminPoolCredentialView struct {
	Index int `json:"index"`
	// Cooling 当前是否在冷却窗口内。
	Cooling bool `json:"cooling"`
	// CooldownUntil 冷却截止（unix ms；未冷却 = 0）。
	CooldownUntil int64 `json:"cooldown_until"`
	// CooldownRemainingSeconds 剩余冷却整秒（向上取整；未冷却 = 0）。
	CooldownRemainingSeconds int `json:"cooldown_remaining_seconds"`
	// LastUsed 最近使用时刻（unix ms；从未使用 = 0）。
	LastUsed int64 `json:"last_used"`
	// Busy 在飞标记（只在公共闸开启时有意义；gate 关恒 false）。
	Busy bool `json:"busy"`
}

// adminPoolUpstreamView 单上游的池快照。
type adminPoolUpstreamView struct {
	Index       int                       `json:"index"`
	Name        string                    `json:"name"`
	Protocol    string                    `json:"protocol"`
	PoolSize    int                       `json:"pool_size"`
	Credentials []adminPoolCredentialView `json:"credentials"`
}

// adminPoolCapacityView 生成面容量快照（§2.9）。
type adminPoolCapacityView struct {
	// DailyUnitsBudget 日预算 units（0 = 关）。
	DailyUnitsBudget int `json:"daily_units_budget"`
	// UsedToday 当日已用 units（台账口径；store 未启用时只有本进程内存值）。
	UsedToday int `json:"used_today"`
	// Remaining 剩余 units（预算关 = -1；耗尽 = 0）。
	Remaining int `json:"remaining"`
	// Inflight 全局并发闸在飞数。
	Inflight int `json:"inflight"`
	// QueueLen 闸队列深度。
	QueueLen int `json:"queue_len"`
	// MaxInflight 闸上限（public 未启用 = 0）。
	MaxInflight int `json:"max_inflight"`
}

// adminPoolResponse `GET /admin/pool` 响应体。
type adminPoolResponse struct {
	Upstreams []adminPoolUpstreamView `json:"upstreams"`
	Capacity  adminPoolCapacityView   `json:"capacity"`
	Now       int64                   `json:"now"`
}

// adminSnapshot 池快照（调用方持锁读；返回池大小与逐索引状态）。nil 池 =
// 空池（上游无凭据/未配置）。**只读**：不改 lastUsed/coolUntil/busy。
func (p *credentialPool) adminSnapshot(now time.Time) (int, []adminPoolCredentialView) {
	if p == nil {
		return 0, []adminPoolCredentialView{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]adminPoolCredentialView, 0, len(p.creds))
	for i := range p.creds {
		v := adminPoolCredentialView{Index: i}
		if now.Before(p.coolUntil[i]) {
			v.Cooling = true
			v.CooldownUntil = p.coolUntil[i].UnixMilli()
			if d := p.coolUntil[i].Sub(now); d > 0 {
				v.CooldownRemainingSeconds = int((d + time.Second - 1) / time.Second)
			}
		}
		if !p.lastUsed[i].IsZero() {
			v.LastUsed = p.lastUsed[i].UnixMilli()
		}
		if p.gate && i < len(p.busy) {
			v.Busy = p.busy[i]
		}
		out = append(out, v)
	}
	return len(p.creds), out
}

// adminPoolCapacity 容量快照（nil 安全：public 未启用 = 零值 + remaining -1）。
func (s *Server) adminPoolCapacity() adminPoolCapacityView {
	v := adminPoolCapacityView{Remaining: -1}
	if s.limiter != nil {
		v.Inflight, v.QueueLen = s.limiter.stats()
		// max 在 NewLimiter 后不再写（构造期常量），无锁读安全。
		v.MaxInflight = s.limiter.max
	}
	if s.publicLimits != nil && s.publicLimits.ledger != nil {
		used, daily, _ := s.publicLimits.ledger.stats()
		v.UsedToday, v.DailyUnitsBudget = used, daily
		if daily > 0 {
			v.Remaining = daily - used
			if v.Remaining < 0 {
				v.Remaining = 0
			}
		}
	}
	return v
}

// handleAdminPool 池 + 容量快照。遍历**全部**上游（不只 claude_web）；池未初始化
// （启动后尚无请求）时 `credentialPoolAt` 惰性建池，行为与请求面一致。
func (s *Server) handleAdminPool(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	resp := adminPoolResponse{
		Upstreams: make([]adminPoolUpstreamView, 0, len(s.cfg.Upstreams)),
		Capacity:  s.adminPoolCapacity(),
		Now:       now.UnixMilli(),
	}
	for i := range s.cfg.Upstreams {
		u := s.cfg.Upstreams[i]
		size, creds := s.cfg.credentialPoolAt(i).adminSnapshot(now)
		resp.Upstreams = append(resp.Upstreams, adminPoolUpstreamView{
			Index:       i,
			Name:        u.Name,
			Protocol:    string(u.Protocol),
			PoolSize:    size,
			Credentials: creds,
		})
	}
	writeAdminJSON(w, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// GET /admin/sessions
// ---------------------------------------------------------------------------

// adminSessionRowView 会话清单行（sid 只回 8 字符前缀——**绝不回完整 sid**）。
type adminSessionRowView struct {
	SIDPrefix    string `json:"sid_prefix"`
	KeyID        string `json:"key_id"`
	CreatedAt    int64  `json:"created_at"`
	LastSeenAt   int64  `json:"last_seen_at"`
	ExpiresAt    int64  `json:"expires_at"`
	AbsExpiresAt int64  `json:"abs_expires_at"`
	Revoked      bool   `json:"revoked"`
	// RequestsToday / CostUnitsToday 来自 `usage_counters` 的
	// `session:<sid>` scope（P1-3 turn 钩子写；无行 = 0）。
	RequestsToday  int `json:"requests_today"`
	CostUnitsToday int `json:"cost_units_today"`
}

// adminSessionsResponse `GET /admin/sessions` 响应体。
type adminSessionsResponse struct {
	Sessions   []adminSessionRowView `json:"sessions"`
	NextCursor string                `json:"next_cursor"`
	Now        int64                 `json:"now"`
}

// adminSessionsCursor 游标载荷（base64url(`v1:<last_seen_ms>:<sid_prefix>`)）。
// 只含前缀——令牌本身不是凭据。
type adminSessionsCursor struct {
	seenMS int64
	prefix string
}

// encodeAdminSessionsCursor 编码一行 → 下一页锚。
func encodeAdminSessionsCursor(rec storeSession) string {
	raw := adminCursorVersion + ":" + strconv.FormatInt(rec.LastSeenAt.UnixMilli(), 10) + ":" + sidPrefix(rec.SID)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeAdminSessionsCursor 解码游标：空 = 首页；坏编码/坏版本/坏载荷 → error
// （请求侧 400）。
func decodeAdminSessionsCursor(raw string) (adminSessionsCursor, bool, error) {
	if raw == "" {
		return adminSessionsCursor{}, false, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return adminSessionsCursor{}, true, errors.New("malformed cursor")
	}
	parts := strings.Split(string(b), ":")
	if len(parts) != 3 || parts[0] != adminCursorVersion {
		return adminSessionsCursor{}, true, errors.New("malformed cursor")
	}
	ms, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || ms < 0 {
		return adminSessionsCursor{}, true, errors.New("malformed cursor")
	}
	prefix := parts[2]
	if len(prefix) == 0 || len(prefix) > 8 || !validSessionsID(prefix) {
		return adminSessionsCursor{}, true, errors.New("malformed cursor")
	}
	return adminSessionsCursor{seenMS: ms, prefix: prefix}, true, nil
}

// adminSessionsAfterCursor 行是否排在锚之后（排序键 = last_seen DESC, sid ASC）。
// 锚行在翻页间被删也能继续（比较键值，不依赖锚行存在）。
func adminSessionsAfterCursor(row storeSession, cur adminSessionsCursor) bool {
	ms := row.LastSeenAt.UnixMilli()
	if ms != cur.seenMS {
		return ms < cur.seenMS
	}
	return sidPrefix(row.SID) > cur.prefix
}

// parseAdminLimit 解析 limit（空 = 缺省；非整数/<1 = 400；>max = 钳到 max）。
func parseAdminLimit(raw string, def, max int) (int, bool) {
	if raw == "" {
		return def, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 0, false
	}
	if n > max {
		n = max
	}
	return n, true
}

// adminSessionsUsage 当日 `session:<sid>` 台账映射（sid → 行）。
func (s *Server) adminSessionsUsage(day string) (map[string]storeUsage, error) {
	out := make(map[string]storeUsage)
	rows, err := s.store.UsageScopes(day)
	if err != nil {
		return nil, err
	}
	for _, u := range rows {
		if strings.HasPrefix(u.Scope, storeUsageScopeSessionPrefix) {
			out[strings.TrimPrefix(u.Scope, storeUsageScopeSessionPrefix)] = u
		}
	}
	return out, nil
}

// handleAdminSessions 会话清单：`limit` 1..200（缺省 50）+ `cursor` 翻页。
// 排序 last_seen DESC, sid ASC；数据源 = Store.Sessions() + UsageScopes(today)。
func (s *Server) handleAdminSessions(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeAdminError(w, http.StatusServiceUnavailable, "store_unavailable", "store is not enabled")
		return
	}
	q := r.URL.Query()
	limit, ok := parseAdminLimit(q.Get("limit"), adminSessionsDefaultLimit, adminSessionsMaxLimit)
	if !ok {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "limit must be an integer >= 1")
		return
	}
	cur, hasCursor, err := decodeAdminSessionsCursor(q.Get("cursor"))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	now := time.Now()
	rows, err := s.store.Sessions()
	if err != nil {
		s.storeError("admin sessions list", err)
		writeAdminError(w, http.StatusServiceUnavailable, "store_unavailable", "store read failed")
		return
	}
	usage, err := s.adminSessionsUsage(storeDay(now))
	if err != nil {
		s.storeError("admin sessions usage", err)
		writeAdminError(w, http.StatusServiceUnavailable, "store_unavailable", "store read failed")
		return
	}
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].LastSeenAt.Equal(rows[j].LastSeenAt) {
			return rows[i].LastSeenAt.After(rows[j].LastSeenAt)
		}
		return rows[i].SID < rows[j].SID
	})
	start := len(rows)
	if hasCursor {
		for i := range rows {
			if adminSessionsAfterCursor(rows[i], cur) {
				start = i
				break
			}
		}
	} else {
		start = 0
	}
	page := rows[start:]
	next := ""
	if len(page) > limit {
		page = page[:limit]
		next = encodeAdminSessionsCursor(page[len(page)-1])
	}
	resp := adminSessionsResponse{
		Sessions:   make([]adminSessionRowView, 0, len(page)),
		NextCursor: next,
		Now:        now.UnixMilli(),
	}
	for _, row := range page {
		v := adminSessionRowView{
			SIDPrefix:    sidPrefix(row.SID),
			KeyID:        row.KeyID,
			CreatedAt:    row.CreatedAt.UnixMilli(),
			LastSeenAt:   row.LastSeenAt.UnixMilli(),
			ExpiresAt:    row.ExpiresAt.UnixMilli(),
			AbsExpiresAt: row.AbsExpiresAt.UnixMilli(),
			Revoked:      row.Revoked,
		}
		if u, ok := usage[row.SID]; ok {
			v.RequestsToday = u.Requests
			v.CostUnitsToday = u.CostUnits
		}
		resp.Sessions = append(resp.Sessions, v)
	}
	writeAdminJSON(w, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// GET /admin/usage
// ---------------------------------------------------------------------------

// adminUsageRowView 台账行（scope 已按类别脱敏：session 只显示 sid 前缀）。
type adminUsageRowView struct {
	Scope     string `json:"scope"`
	Requests  int    `json:"requests"`
	CostUnits int    `json:"cost_units"`
}

// adminUsageTotals 过滤后合计（limit 截断前）。
type adminUsageTotals struct {
	Requests  int `json:"requests"`
	CostUnits int `json:"cost_units"`
}

// adminUsageResponse `GET /admin/usage` 响应体。
type adminUsageResponse struct {
	Day    string              `json:"day"`
	Scope  string              `json:"scope"`
	Rows   []adminUsageRowView `json:"rows"`
	Totals adminUsageTotals    `json:"totals"`
	Now    int64               `json:"now"`
}

// adminUsageScopeClasses 合法 scope 类别（§2.9；W78-1 增 `model`——逐模型台账）。
var adminUsageScopeClasses = []string{"global", "session", "key", "ip", "model"}

// validAdminUsageScope scope 参数校验。
func validAdminUsageScope(scope string) bool {
	for _, c := range adminUsageScopeClasses {
		if scope == c {
			return true
		}
	}
	return false
}

// adminUsageScopeMatch 台账 scope 是否属于该类别。
func adminUsageScopeMatch(class, scope string) bool {
	switch class {
	case "global":
		return scope == storeUsageScopeGlobal
	case "session":
		return strings.HasPrefix(scope, storeUsageScopeSessionPrefix)
	case "key":
		return strings.HasPrefix(scope, storeUsageScopeKeyPrefix)
	case "ip":
		return strings.HasPrefix(scope, storeUsageScopeIPPrefix)
	case "model":
		return strings.HasPrefix(scope, storeUsageScopeModelPrefix)
	}
	return false
}

// adminUsageDisplayScope 展示形态：session scope 只留 8 字符 sid 前缀（key/ip
// 的 scope 后缀本身已是摘要——原样返回）；**model** 原样返回（模型名不是秘密，
// 截断反而让运营者无法对齐上游模型名/计费口径）。
func adminUsageDisplayScope(class, scope string) string {
	if class == "session" {
		return storeUsageScopeSessionPrefix + sidPrefix(strings.TrimPrefix(scope, storeUsageScopeSessionPrefix))
	}
	return scope
}

// adminUsageDay 解析 day（缺省 = 今天 UTC；坏形态 = 400）。
func adminUsageDay(raw string, now time.Time) (string, bool) {
	if raw == "" {
		return storeDay(now), true
	}
	t, err := time.ParseInLocation(storeDayLayout, raw, time.UTC)
	if err != nil {
		return "", false
	}
	return t.Format(storeDayLayout), true
}

// handleAdminUsage 台账汇总：`day`（缺省今天 UTC）+ `scope`
// （global|session|key|ip|model，缺省 global）+ `limit` 1..500（缺省 50）。
// rows 按 cost_units DESC, requests DESC, scope ASC；totals = 过滤后全量合计。
func (s *Server) handleAdminUsage(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeAdminError(w, http.StatusServiceUnavailable, "store_unavailable", "store is not enabled")
		return
	}
	q := r.URL.Query()
	now := time.Now()
	day, ok := adminUsageDay(q.Get("day"), now)
	if !ok {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "day must be YYYY-MM-DD")
		return
	}
	class := q.Get("scope")
	if class == "" {
		class = "global"
	}
	if !validAdminUsageScope(class) {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "scope must be global|session|key|ip|model")
		return
	}
	limit, ok := parseAdminLimit(q.Get("limit"), adminUsageDefaultLimit, adminUsageMaxLimit)
	if !ok {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "limit must be an integer >= 1")
		return
	}
	usage, err := s.store.UsageScopes(day)
	if err != nil {
		s.storeError("admin usage", err)
		writeAdminError(w, http.StatusServiceUnavailable, "store_unavailable", "store read failed")
		return
	}
	type agg struct{ requests, units int }
	byScope := make(map[string]*agg)
	var totals adminUsageTotals
	for _, u := range usage {
		if !adminUsageScopeMatch(class, u.Scope) {
			continue
		}
		display := adminUsageDisplayScope(class, u.Scope)
		a := byScope[display]
		if a == nil {
			a = &agg{}
			byScope[display] = a
		}
		a.requests += u.Requests
		a.units += u.CostUnits
		totals.Requests += u.Requests
		totals.CostUnits += u.CostUnits
	}
	rows := make([]adminUsageRowView, 0, len(byScope))
	for scope, a := range byScope {
		rows = append(rows, adminUsageRowView{Scope: scope, Requests: a.requests, CostUnits: a.units})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].CostUnits != rows[j].CostUnits {
			return rows[i].CostUnits > rows[j].CostUnits
		}
		if rows[i].Requests != rows[j].Requests {
			return rows[i].Requests > rows[j].Requests
		}
		return rows[i].Scope < rows[j].Scope
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	writeAdminJSON(w, http.StatusOK, adminUsageResponse{
		Day:    day,
		Scope:  class,
		Rows:   rows,
		Totals: totals,
		Now:    now.UnixMilli(),
	})
}

// ---------------------------------------------------------------------------
// POST /admin/sessions/revoke
// ---------------------------------------------------------------------------

// adminRevokeRequest 吊销选择器：三选一（多给 = 400 歧义，都不给 = 400 缺参）。
type adminRevokeRequest struct {
	SIDPrefix string `json:"sid_prefix"`
	KeyID     string `json:"key_id"`
	All       bool   `json:"all"`
}

// adminRevokeResponse 吊销结果（matched = 选择器命中数；revoked = 实际删除数）。
type adminRevokeResponse struct {
	Revoked int `json:"revoked"`
	Matched int `json:"matched"`
}

// adminRevokeConflictEnvelope 前缀歧义（409）：错误信封 + 命中数。
type adminRevokeConflictEnvelope struct {
	adminErrorEnvelope
	Matched int `json:"matched"`
}

// validAdminKeyID key_id 形态 = sha8（小写 hex 8 字符；newAccessKeys 同源）。
func validAdminKeyID(id string) bool {
	if len(id) != 8 {
		return false
	}
	for i := range len(id) {
		c := id[i]
		if c >= '0' && c <= '9' || c >= 'a' && c <= 'f' {
			continue
		}
		return false
	}
	return true
}

// adminRevokeSession 吊销一个会话：内存表 + store 双删（级联 conversations/
// messages/turns，与 logout 同路径）。持久化失败只 log/ring（best-effort，
// 与 sessionStore 的既有纪律一致）。
func (s *Server) adminRevokeSession(sid string) bool {
	if s.sessions != nil {
		s.sessions.delete(sid)
		return true
	}
	if s.store == nil {
		return false
	}
	if _, err := s.store.SessionDelete(sid); err != nil {
		s.storeError("session revoke", err)
		return false
	}
	return true
}

// handleAdminSessionsRevoke 吊销：`{"sid_prefix":"a1b2c3d4"}` /
// `{"key_id":"3106c924"}` / `{"all":true}`。
//
//   - sid 前缀命中 0 → 404；>1 → 409 ambiguous（带 matched）；=1 → 删；
//   - key_id 命中全部删（一枚 key 的全部会话 = 正常的「key 泄漏」处置）；
//   - all 删全部会话。
//
// 审计：一行 `session_revoke`（detail 只含 selector 与计数，不含完整 sid）。
func (s *Server) handleAdminSessionsRevoke(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeAdminError(w, http.StatusServiceUnavailable, "store_unavailable", "store is not enabled")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, adminRevokeMaxBody)
	var req adminRevokeRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "invalid JSON body")
		return
	}
	selectors := 0
	if req.All {
		selectors++
	}
	if req.SIDPrefix != "" {
		selectors++
	}
	if req.KeyID != "" {
		selectors++
	}
	if selectors != 1 {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "exactly one of all / sid_prefix / key_id is required")
		return
	}
	if req.SIDPrefix != "" {
		if len(req.SIDPrefix) < adminSessionsCursorMinPrefix || !validSessionsID(req.SIDPrefix) {
			writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "sid_prefix must be 4..64 chars of [A-Za-z0-9_-]")
			return
		}
	}
	if req.KeyID != "" && !validAdminKeyID(req.KeyID) {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "key_id must be 8 lowercase hex chars")
		return
	}
	rows, err := s.store.Sessions()
	if err != nil {
		s.storeError("admin revoke list", err)
		writeAdminError(w, http.StatusServiceUnavailable, "store_unavailable", "store read failed")
		return
	}
	matched := make([]storeSession, 0, len(rows))
	for _, row := range rows {
		switch {
		case req.All:
			matched = append(matched, row)
		case req.SIDPrefix != "":
			if strings.HasPrefix(row.SID, req.SIDPrefix) {
				matched = append(matched, row)
			}
		case req.KeyID != "":
			if row.KeyID == req.KeyID {
				matched = append(matched, row)
			}
		}
	}
	if len(matched) == 0 {
		writeAdminError(w, http.StatusNotFound, "not_found", "no session matched")
		return
	}
	if req.SIDPrefix != "" && len(matched) > 1 {
		var env adminRevokeConflictEnvelope
		env.Error.Type = "conflict"
		env.Error.Message = "sid_prefix is ambiguous"
		env.Matched = len(matched)
		writeAdminJSON(w, http.StatusConflict, env)
		return
	}
	revoked := 0
	for _, row := range matched {
		if s.adminRevokeSession(row.SID) {
			revoked++
		}
	}
	// 审计只落 8 字符前缀（selector 允许 4..64——即使操作员粘了完整 sid，
	// 落盘面也截到 8：sid 是凭据，审计表不存凭据原文）。
	auditPrefix := sidPrefix(req.SIDPrefix)
	detail, err := json.Marshal(map[string]any{
		"op":         "revoke",
		"sid_prefix": auditPrefix,
		"key_id":     req.KeyID,
		"all":        req.All,
		"matched":    len(matched),
		"revoked":    revoked,
	})
	if err == nil {
		s.auditStore(storeAudit{
			TS:     time.Now(),
			SID:    auditPrefix,
			Kind:   storeAuditKindSessionRevoke,
			Detail: string(detail),
		})
	}
	writeAdminJSON(w, http.StatusOK, adminRevokeResponse{Revoked: revoked, Matched: len(matched)})
}
