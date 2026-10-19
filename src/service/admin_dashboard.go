// admin_dashboard.go —— dashboard P0-0 只读壳（board A2A-L19-W71-1；设计
// `docs/admin-dashboard.md` §5.3 / §5.6 / §6.1 / §9 的 P0-0 行）。
//
// 四个端点（全部挂 adminAuth；未配 admin_key = 路由根本不注册 → 404）：
//
//	GET /admin/audit?kind=&since=&limit=   审计行（Store.AuditQuery；store 未启用 = 503）
//	GET /admin/requests?since=&limit=&…    请求行（有界内存 ring；18 列 + 游标翻页）
//	GET /admin/requests/{id}               请求详情（请求面 + attempt 时间线）
//	GET /admin/alerts                      active/history 告警（ring + 状态派生）
//
// 数据源（**零新计数器**；设计 §9 P0-0「无新计数器」）：
//   - 请求 ring：进程内有界（2000 条）请求行，写入点 = 既有请求收尾
//     （`Server.recordRequestRing`，attemps.go 的 requestRecorder 收尾 hook）。
//   - 审计：`Store.AuditQuery`（**已实现**，store.go:243-246）。
//   - 告警：`adminLogRing`（17 类事件词表）+ 进程内状态（池冷却 / 管理面封禁 /
//     Diagnostics 计数）——纯读，零出网（设计 §5.6 的「状态桥/webhook 属外部装置」）。
//
// 纪律（与 admin_api.go 同源）：
//   - **零凭据字节**：ring 只存脱敏字段（入口/模型/流位/状态/尝试/首字节/总量/
//     用量/结局/sid 前缀/key_id/task/attempt 时间线）——**绝不存 cookie / access
//     key / 上游错误原文 / 原始 URL（含查询串）**；文本字段写入前过
//     `redactAdminText`（纵深防御），审计 detail 读出时过 `redactAdminDetail`。
//   - **错误面只有枚举**：attempt 的错误摘要 = `end_reason` 六值 + 归因归类
//     （upstream|downstream|cancelled|local）——上游原文/头面/查询串一律不回。
//   - **只读**：零 DDL、零迁移、零出网、不写 store、不改既有端点形状。
//   - **默认关闭**：`registerAdminDashboard` 自门控（未配 admin_key = 404 非 401）。
//
// 与设计件的登记差异（**本单不做**，见报告「残余风险」）：
//   - `/admin/requests` 的 ring 是**进程内**内存态（重启清零；多实例各自一份——
//     设计 §7.5 明确登记为未来项）。
//   - `/admin/audit` 行序 = store 返回序（id 升序 = 时间升序；`limit` 是**截尾**
//     ——要「最近 N 条」请用 `since` 收窗口，见 §「登记：audit 行序」）。
//   - 告警的 `status_stale` 在服务侧语义 = 「观测面停摆」（口径见 §「告警口径」），
//     不是 kit 侧的「健康台账 >26h 未更新」（那是外部装置的状态，服务不感知）。
package service

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// 参数与词表常量
// ---------------------------------------------------------------------------

const (
	// adminRequestRingCapacity 请求 ring 容量（设计 §5.3/§6.1 的 N=2000；
	// 覆盖写最旧——有界内存，落盘采样属 P1）。
	adminRequestRingCapacity = 2000
	// adminRequestsDefaultLimit / adminRequestsMaxLimit 请求行分页（§5.3）。
	adminRequestsDefaultLimit = 50
	adminRequestsMaxLimit     = 200
	// adminAuditDefaultLimit / adminAuditMaxLimit 审计行分页（§5.6；缺省与
	// storeAuditDefaultLimit 同值——限流上限由本面自定）。
	adminAuditDefaultLimit = 100
	adminAuditMaxLimit     = 500
	// adminAlertsHistoryMax 历史告警上限（ring 容量远大于此；防响应膨胀）。
	adminAlertsHistoryMax = 50
	// adminPoolLowMinAvailable 池低水位阈值：池 ≥2 凭据且可用数 < 2 → pool_low
	// （kit POOL_LOW「池可用数 < 阈值」的服务侧最小版）。
	adminPoolLowMinAvailable = 2
)

// adminAlertsActiveWindow ring 派生告警的「active」时窗：窗口内 = active，窗口外
// （仍在 ring 里）= history。取 5 分钟与设计 §5.3 的缺省查询窗同源。
const adminAlertsActiveWindow = 5 * time.Minute

// 告警严重度（设计 §5.1「severity / 名称 / 对象 / 说明 / 起始 / runbook」）。
const (
	adminAlertSeverityError   = "error"
	adminAlertSeverityWarning = "warning"
)

// 告警类别词表（设计 §5.6 的 8 类；每一类的触发源见 `adminAlertFromRingEntry`
// 与 `Server.adminStateAlerts` 的逐类注释）。
const (
	adminAlertPoolLow     = "pool_low"
	adminAlertPoolDead    = "pool_dead"
	adminAlertBlocked     = "blocked"
	adminAlertKeyMismatch = "key_mismatch"
	adminAlertStatusStale = "status_stale"
	adminAlertStoreError  = "store_error"
	adminAlertQuota       = "quota"
	adminAlert5xx         = "5xx"
)

// 告警的 runbook 指针（响应字段，给人看的检索串；服务无出网、不做链接跳转）。
const (
	adminRunbookPool      = "docs/deploy-upstream.md §8.3 告警（notify.py）"
	adminRunbookAuth      = "docs/service.md §6 排障表（管理面鉴权）"
	adminRunbookStore     = "docs/service.md §7 纪律（持久化降级）"
	adminRunbookQuota     = "docs/deployment.md（预算与限额）"
	adminRunbookUpstream  = "docs/service.md §5 Failover / §6 排障表"
	adminRunbookDashboard = "docs/admin-dashboard.md §5.3/§5.6"
)

// adminAlertRunbook 类别 → runbook 检索串。
func adminAlertRunbook(name string) string {
	switch name {
	case adminAlertPoolLow, adminAlertPoolDead, adminAlertKeyMismatch:
		return adminRunbookPool
	case adminAlertBlocked:
		return adminRunbookAuth
	case adminAlertStoreError:
		return adminRunbookStore
	case adminAlertQuota:
		return adminRunbookQuota
	case adminAlert5xx:
		return adminRunbookUpstream
	}
	return adminRunbookDashboard
}

// ---------------------------------------------------------------------------
// 路由注册
// ---------------------------------------------------------------------------

// registerAdminDashboard 注册 P0-0 的四个只读端点。**自门控**：未配 admin_key
// 时一条都不注册（与 registerAdminAPI / 既有 /admin/* 的惰性注册语义逐字一致：
// 404 非 401）。
//
// 调用点 = `Server.Handler()` 的 admin_key 分支（与 diagnostics/cooldowns/logs
// 三条同一位置）。
func (s *Server) registerAdminDashboard(mux *http.ServeMux) {
	if s == nil || s.cfg.adminKey() == "" {
		return
	}
	mux.HandleFunc("GET /admin/audit", s.adminAuth(s.handleAdminAudit))
	mux.HandleFunc("GET /admin/requests", s.adminAuth(s.handleAdminRequests))
	mux.HandleFunc("GET /admin/requests/{id}", s.adminAuth(s.handleAdminRequestDetail))
	mux.HandleFunc("GET /admin/alerts", s.adminAuth(s.handleAdminAlerts))
}

// ---------------------------------------------------------------------------
// 脱敏器（零凭据字节纪律的机械面）
// ---------------------------------------------------------------------------

// adminSecretValuePatterns 明文形态的凭据模式（Bearer token / OpenAI 系 sk- /
// `name=value` 赋值）。**只用于出口投影**：命中部分整段替换为 `[redacted]`。
var adminSecretValuePatterns = []struct {
	re   *regexp.Regexp
	repl string
}{
	// `Bearer <token>`（scheme 大小写不敏感；token ≥4 字符——测试负控用短样本）。
	{regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{4,}`), "Bearer [redacted]"},
	// `sk-…`（OpenAI/Anthropic 系 key 形态）。
	{regexp.MustCompile(`\bsk-[A-Za-z0-9._-]{6,}`), "sk-[redacted]"},
	// `api_key=VALUE` / `token: VALUE` 一类赋值（值 = 非空白串）。
	{regexp.MustCompile(`(?i)\b(api[_-]?key|access[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|session[_-]?key|id[_-]?token|password|passwd|secret|token|authorization|cookie)\s*[:=]\s*\S{3,}`), "$1=[redacted]"},
}

// adminSecretKeys 敏感字段名（JSON 对象键；归一化 = 去 `-`/`_` + 小写）。
//
// 含 `credentials`/`key` 而**不含** `credential`：`credential_index`（池索引，设计
// 明确允许回显）归一化为 `credentialindex`，不会被误伤；`key_id` 同理。
var adminSecretKeys = map[string]bool{
	"key": true, "keys": true, "apikey": true, "apikeys": true,
	"accesskey": true, "accesskeys": true, "apitoken": true, "accesstoken": true,
	"refreshtoken": true, "idtoken": true, "clientsecret": true, "sessionkey": true,
	"privatekey": true, "authkey": true, "secret": true, "secrets": true,
	"password": true, "passwd": true, "token": true, "tokens": true,
	"authorization": true, "cookie": true, "cookies": true, "credentials": true,
}

// adminSecretKey 字段名是否敏感（归一化后精确匹配）。
func adminSecretKey(name string) bool {
	n := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(name, "-", ""), "_", ""))
	return adminSecretKeys[n]
}

// redactAdminText 文本脱敏投影（Bearer/sk-/赋值三类明文形态）。**幂等**：已
// `[redacted]` 的串不再命中（占位符不含凭据字符）。空串安全。
func redactAdminText(s string) string {
	if s == "" {
		return s
	}
	out := s
	for _, p := range adminSecretValuePatterns {
		out = p.re.ReplaceAllString(out, p.repl)
	}
	return out
}

// redactAdminDetail 审计 detail 的出口投影：
//   - 合法 JSON → 逐字段投影（敏感键 → `[redacted]`；其余字符串值过
//     redactAdminText）；
//   - 非 JSON → 整串过 redactAdminText；
//   - 投影失败（理论上不可达）→ 最保守值 `[redacted]`（绝不回原文）。
//
// 键序按 map 归一（Go json.Marshal 排序键）——detail 是给人看的摘要，键序非契约。
func redactAdminDetail(detail string) string {
	trimmed := strings.TrimSpace(detail)
	if trimmed == "" {
		return detail
	}
	dec := json.NewDecoder(strings.NewReader(trimmed))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return redactAdminText(detail)
	}
	b, err := json.Marshal(redactAdminValue(v))
	if err != nil {
		return "[redacted]"
	}
	return string(b)
}

// redactAdminValue JSON 值递归投影（map/slice/string；其余标量原样）。
func redactAdminValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if adminSecretKey(k) {
				out[k] = "[redacted]"
				continue
			}
			out[k] = redactAdminValue(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i := range t {
			out[i] = redactAdminValue(t[i])
		}
		return out
	case string:
		return redactAdminText(t)
	}
	return v
}

// ---------------------------------------------------------------------------
// 请求 ring（有界内存；只存脱敏字段）
// ---------------------------------------------------------------------------

// adminRequestRing 有界请求 ring（覆盖写最旧；不落盘）。零值可用（首次 add 惰性
// 分配；容量恒 adminRequestRingCapacity）。`Server.adminRequests` 零值即空 ring。
type adminRequestRing struct {
	mu    sync.Mutex
	buf   []adminRequestRow
	next  int // 下一个写入位
	count int // 已写条数（≤ len(buf)）
}

// add 追加一行（满则覆盖最旧）。
func (r *adminRequestRing) add(row adminRequestRow) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.buf) == 0 {
		r.buf = make([]adminRequestRow, adminRequestRingCapacity)
	}
	r.buf[r.next] = row
	r.next = (r.next + 1) % len(r.buf)
	if r.count < len(r.buf) {
		r.count++
	}
}

// size 当前存条数。
func (r *adminRequestRing) size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

// snapshot 全量快照（**新→旧**：按 Time DESC, RequestID DESC 稳定排序——并发收尾
// 的写入次序可能与时刻次序微异，排序保证翻页/游标语义确定）。
func (r *adminRequestRing) snapshot() []adminRequestRow {
	r.mu.Lock()
	out := make([]adminRequestRow, 0, r.count)
	for i := range r.count {
		idx := (r.next - r.count + i + len(r.buf)) % len(r.buf)
		out = append(out, r.buf[idx])
	}
	r.mu.Unlock()
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].Time.Equal(out[j].Time) {
			return out[i].Time.After(out[j].Time)
		}
		return out[i].RequestID > out[j].RequestID
	})
	return out
}

// find 按请求 id 精确查（未命中 = false）。
func (r *adminRequestRing) find(id string) (adminRequestRow, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.count {
		idx := (r.next - 1 - i + len(r.buf)) % len(r.buf)
		if r.buf[idx].RequestID == id {
			return r.buf[idx], true
		}
	}
	return adminRequestRow{}, false
}

// adminRequestRow 请求 ring 的一行（**已脱敏**；字段口径 = 设计 §5.3 的 18 列）。
type adminRequestRow struct {
	RequestID string
	Time      time.Time // 请求收尾时刻（UTC；列表排序键）
	Entry     string
	Model     string
	Stream    bool
	Status    int // 末次尝试的上游 HTTP 状态（0 = 未达响应头）
	Attempts  int
	Failovers int // 相邻尝试**换台**次数（同台重试不计——与 Diagnostics.Failovers 同口径）
	Upstreams []string
	// CredIndex 末次尝试的池凭据索引（-1 = 无池/未回填——**只记索引**）。
	CredIndex int
	TTFTMs    int64 // 末次尝试的首个下游可见产出（ms；0 = 未产出）
	TotalMs   int64 // 首次尝试开始 → 末次尝试收尾（ms；含换台间隔）
	In, Out   int64
	Outcome   string
	SIDPrefix string
	KeyID     string
	Task      string
	Failed    bool // 末次尝试上游侧失败或下游断开
	Delivered bool // 末次尝试终态产出完整写出下游
	Timeline  []adminRequestAttempt
}

// adminRequestAttempt attempt 时间线的一条（脱敏；错误面只有枚举与归类）。
type adminRequestAttempt struct {
	N               int                    `json:"n"`
	Upstream        string                 `json:"upstream"`
	Protocol        string                 `json:"protocol"`
	CredentialIndex *int                   `json:"credential_index,omitempty"`
	Status          int                    `json:"status,omitempty"`
	EndReason       string                 `json:"end_reason,omitempty"`
	Failed          bool                   `json:"failed"`
	Disconnected    bool                   `json:"disconnected,omitempty"`
	Delivered       bool                   `json:"delivered"`
	TTFTMs          int64                  `json:"ttft_ms"`
	TotalMs         int64                  `json:"total_ms"`
	In              int64                  `json:"in"`
	Out             int64                  `json:"out"`
	Error           *adminAttemptErrorView `json:"error,omitempty"`
}

// adminAttemptErrorView 尝试失败摘要（设计 §5.3 详情抽屉「错误体**脱敏摘要**」）：
// **只有**结局枚举（type = end_reason 六值 / `local`）与归因组（class）——上游
// 原文、响应头、URL 查询串一律不落 ring、不回响应。
type adminAttemptErrorView struct {
	Type  string `json:"type"`
	Class string `json:"class"`
}

// adminAttemptEndReason 尝试结局（显式值优先；兜底派生规则与 attempts.go `log()`
// 逐条同规则——**不共享函数**以免改动既有日志路径）。
func adminAttemptEndReason(ar *attemptRecord) string {
	if ar.endReason != "" {
		return ar.endReason
	}
	switch {
	case ar.failed:
		return endReasonUpstreamError
	case ar.disconnected:
		return endReasonDownstreamDisconnect
	case ar.ttft > 0:
		return endReasonCompleted
	}
	return ""
}

// adminAttemptError 失败摘要（成功尝试 = nil）：type = 结局枚举（空 = 未达上游的
// 本地失败 → `local`）；class = upstream|downstream|cancelled|local。
func adminAttemptError(ar *attemptRecord) *adminAttemptErrorView {
	reason := adminAttemptEndReason(ar)
	fail := ar.failed || ar.disconnected || (reason != "" && reason != endReasonCompleted)
	if !fail {
		return nil
	}
	v := &adminAttemptErrorView{Type: reason}
	if v.Type == "" {
		v.Type = "local"
	}
	switch reason {
	case endReasonUpstreamError, endReasonUpstreamIdle:
		v.Class = "upstream"
	case endReasonDownstreamDisconnect:
		v.Class = "downstream"
	case endReasonClientCancel, endReasonTaskCancel:
		v.Class = "cancelled"
	default:
		if ar.failed {
			v.Class = "upstream"
		} else if ar.disconnected {
			v.Class = "downstream"
		} else {
			v.Class = "local"
		}
	}
	return v
}

// adminRequestRowFrom 把请求收尾的记账器投影为 ring 行（**写入即脱敏**：所有字符串
// 字段过 redactAdminText；不放原文/URL/凭据——ring 是长期驻留内存，原文绝不入内）。
func adminRequestRowFrom(rec *requestRecorder, now time.Time) adminRequestRow {
	row := adminRequestRow{
		RequestID: redactAdminText(rec.reqID),
		Time:      now.UTC(),
		Entry:     redactAdminText(rec.entry),
		Model:     redactAdminText(rec.model),
		Stream:    rec.stream,
		Task:      redactAdminText(rec.task),
		SIDPrefix: redactAdminText(sidPrefix(rec.sid)),
		KeyID:     redactAdminText(rec.keyID),
		Outcome:   redactAdminText(rec.auditOutcome()),
		CredIndex: -1,
		Attempts:  len(rec.recs),
		Upstreams: make([]string, 0, len(rec.recs)),
		Timeline:  make([]adminRequestAttempt, 0, len(rec.recs)),
	}
	for _, ar := range rec.recs {
		v := adminRequestAttempt{
			N:            ar.attempt,
			Upstream:     redactAdminText(ar.upstream),
			Protocol:     redactAdminText(ar.protocol),
			Status:       ar.status,
			EndReason:    adminAttemptEndReason(ar),
			Failed:       ar.failed,
			Disconnected: ar.disconnected,
			Delivered:    ar.delivered,
			TTFTMs:       ar.ttft.Milliseconds(),
			In:           ar.in,
			Out:          ar.out,
			Error:        adminAttemptError(ar),
		}
		if ar.credentialIndex >= 0 {
			idx := ar.credentialIndex
			v.CredentialIndex = &idx
		}
		if !ar.start.IsZero() && !ar.end.IsZero() {
			v.TotalMs = ar.end.Sub(ar.start).Milliseconds()
		}
		row.Upstreams = append(row.Upstreams, v.Upstream)
		row.Timeline = append(row.Timeline, v)
	}
	if n := len(rec.recs); n > 0 {
		last := rec.recs[n-1]
		row.Status = last.status
		row.Failed = last.failed || last.disconnected
		row.Delivered = last.delivered
		row.CredIndex = last.credentialIndex
		row.TTFTMs = last.ttft.Milliseconds()
		row.In, row.Out = last.in, last.out
		if !rec.recs[0].start.IsZero() && !last.end.IsZero() {
			row.TotalMs = last.end.Sub(rec.recs[0].start).Milliseconds()
		}
		for i := 1; i < n; i++ {
			if rec.recs[i].upstream != rec.recs[i-1].upstream {
				row.Failovers++
			}
		}
	}
	return row
}

// ---------------------------------------------------------------------------
// 请求行视图
// ---------------------------------------------------------------------------

// adminRequestRowView 列表行（设计 §5.3 的 18 列：time / request_id / entry /
// model / stream / status / attempts / failovers / upstream / credential_index /
// ttft / total / in / out / outcome / sid_prefix / key_id / task；时间面统一 unix
// ms，延迟面统一 ms）。
type adminRequestRowView struct {
	Time            int64    `json:"time"`
	RequestID       string   `json:"request_id"`
	Entry           string   `json:"entry"`
	Model           string   `json:"model"`
	Stream          bool     `json:"stream"`
	Status          int      `json:"status"`
	Attempts        int      `json:"attempts"`
	Failovers       int      `json:"failovers"`
	Upstream        string   `json:"upstream,omitempty"`
	Upstreams       []string `json:"upstreams,omitempty"`
	CredentialIndex *int     `json:"credential_index,omitempty"`
	TTFTMs          int64    `json:"ttft_ms"`
	TotalMs         int64    `json:"total_ms"`
	In              int64    `json:"in"`
	Out             int64    `json:"out"`
	Outcome         string   `json:"outcome,omitempty"`
	SIDPrefix       string   `json:"sid_prefix,omitempty"`
	KeyID           string   `json:"key_id,omitempty"`
	Task            string   `json:"task,omitempty"`
	Failed          bool     `json:"failed"`
	Delivered       bool     `json:"delivered"`
}

// adminRequestRowViewFrom ring 行 → 列表行视图（attempt 时间线**不进列表**——
// 它只在 `GET /admin/requests/{id}` 的 `attempts` 面回）。
func adminRequestRowViewFrom(row adminRequestRow) adminRequestRowView {
	v := adminRequestRowView{
		Time:      row.Time.UnixMilli(),
		RequestID: row.RequestID,
		Entry:     row.Entry,
		Model:     row.Model,
		Stream:    row.Stream,
		Status:    row.Status,
		Attempts:  row.Attempts,
		Failovers: row.Failovers,
		TTFTMs:    row.TTFTMs,
		TotalMs:   row.TotalMs,
		In:        row.In,
		Out:       row.Out,
		Outcome:   row.Outcome,
		SIDPrefix: row.SIDPrefix,
		KeyID:     row.KeyID,
		Task:      row.Task,
		Failed:    row.Failed,
		Delivered: row.Delivered,
	}
	if n := len(row.Upstreams); n > 0 {
		v.Upstream = row.Upstreams[n-1]
		v.Upstreams = row.Upstreams
	}
	if row.CredIndex >= 0 {
		idx := row.CredIndex
		v.CredentialIndex = &idx
	}
	return v
}

// ---------------------------------------------------------------------------
// 参数解析（since / bool / cursor）
// ---------------------------------------------------------------------------

// parseAdminSince 解析 `since`：空 = 不筛（零值）；十进制整数 = unix **毫秒**；
// RFC3339（含纳秒）——三种之外 = 400。
func parseAdminSince(raw string) (time.Time, bool) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return time.Time{}, true
	}
	if n, err := strconv.ParseInt(value, 10, 64); err == nil {
		if n < 0 {
			return time.Time{}, false
		}
		return time.UnixMilli(n), true
	}
	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return t, true
	}
	return time.Time{}, false
}

// parseAdminBool 解析布尔参数（true/false/1/0；大小写不敏感）。
func parseAdminBool(raw string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "true", "1":
		return true, true
	case "false", "0":
		return false, true
	}
	return false, false
}

// adminRequestsCursor 请求行游标载荷（锚 = 排序键 (time_ms, request_id)；
// 令牌只含已经在响应里回过的值，不是凭据）。
type adminRequestsCursor struct {
	ms int64
	id string
}

// encodeAdminRequestsCursor 编码一行 → 下一页锚。
func encodeAdminRequestsCursor(row adminRequestRow) string {
	raw := adminCursorVersion + ":" + strconv.FormatInt(row.Time.UnixMilli(), 10) + ":" + row.RequestID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeAdminRequestsCursor 解码游标：空 = 首页；坏编码/坏版本/坏载荷 → error
// （请求侧 400）。
func decodeAdminRequestsCursor(raw string) (adminRequestsCursor, bool, error) {
	if raw == "" {
		return adminRequestsCursor{}, false, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return adminRequestsCursor{}, true, fmt.Errorf("malformed cursor")
	}
	parts := strings.SplitN(string(b), ":", 3)
	if len(parts) != 3 || parts[0] != adminCursorVersion {
		return adminRequestsCursor{}, true, fmt.Errorf("malformed cursor")
	}
	ms, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || parts[2] == "" {
		return adminRequestsCursor{}, true, fmt.Errorf("malformed cursor")
	}
	return adminRequestsCursor{ms: ms, id: parts[2]}, true, nil
}

// adminRequestsAfterCursor 行是否排在锚之后（排序键 = time DESC, request_id DESC；
// 比较键值——锚行被 ring 覆盖丢弃也能继续翻页）。
func adminRequestsAfterCursor(row adminRequestRow, cur adminRequestsCursor) bool {
	ms := row.Time.UnixMilli()
	if ms != cur.ms {
		return ms < cur.ms
	}
	return row.RequestID < cur.id
}

// ---------------------------------------------------------------------------
// GET /admin/requests
// ---------------------------------------------------------------------------

// adminRequestsResponse `GET /admin/requests` 响应体（rows 新→旧 + 不透明游标；
// 无下一页 = `""`）。
type adminRequestsResponse struct {
	Rows       []adminRequestRowView `json:"rows"`
	NextCursor string                `json:"next_cursor"`
}

// adminRequestFilter 请求行筛选（设计 §5.3「筛选」行：时间窗 / 入口 / 模型 /
// 上游 / 状态 / failed / stream / request_id 前缀）。显式参数与 `filter=k=v,…`
// 两种写法等价（显式参数优先；同键重复取显式）。
type adminRequestFilter struct {
	since           time.Time
	hasSince        bool
	entry           string
	model           string
	upstream        string
	status          int
	hasStatus       bool
	failed          *bool
	stream          *bool
	requestIDPrefix string
}

// match 行是否命中选择器（空选择器 = 全通过）。
func (f adminRequestFilter) match(row adminRequestRow) bool {
	if f.hasSince && row.Time.Before(f.since) {
		return false
	}
	if f.entry != "" && row.Entry != f.entry {
		return false
	}
	if f.model != "" && row.Model != f.model {
		return false
	}
	if f.upstream != "" {
		found := false
		for _, u := range row.Upstreams {
			if u == f.upstream {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if f.hasStatus && row.Status != f.status {
		return false
	}
	if f.failed != nil && row.Failed != *f.failed {
		return false
	}
	if f.stream != nil && row.Stream != *f.stream {
		return false
	}
	if f.requestIDPrefix != "" && !strings.HasPrefix(row.RequestID, f.requestIDPrefix) {
		return false
	}
	return true
}

// adminRequestFilterKeys `filter` 参数允许的键（未知键 = 400，不静默吞）。
var adminRequestFilterKeys = []string{"since", "entry", "model", "upstream", "status", "failed", "stream", "request_id"}

// adminRequestFilterSet 逐键写入筛选器（显式参数与 filter 参数共用；非法值 =
// false → 400）。
func (f *adminRequestFilter) set(key, value string) bool {
	switch key {
	case "since":
		t, ok := parseAdminSince(value)
		if !ok {
			return false
		}
		f.since, f.hasSince = t, true
	case "entry":
		f.entry = value
	case "model":
		f.model = value
	case "upstream":
		f.upstream = value
	case "status":
		n, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || n < 0 {
			return false
		}
		f.status, f.hasStatus = n, true
	case "failed":
		b, ok := parseAdminBool(value)
		if !ok {
			return false
		}
		f.failed = &b
	case "stream":
		b, ok := parseAdminBool(value)
		if !ok {
			return false
		}
		f.stream = &b
	case "request_id":
		f.requestIDPrefix = value
	default:
		return false
	}
	return true
}

// parseAdminRequestFilter 解析显式参数 + `filter=k=v,…`（显式参数后写覆盖——
// 两者都给了同一个键时以显式参数为准；顺序固定为 filter 先、显式后）。
func parseAdminRequestFilter(q url.Values) (adminRequestFilter, bool) {
	var f adminRequestFilter
	if raw := q.Get("filter"); raw != "" {
		for _, kv := range strings.Split(raw, ",") {
			kv = strings.TrimSpace(kv)
			if kv == "" {
				continue
			}
			key, value, ok := strings.Cut(kv, "=")
			if !ok {
				return f, false
			}
			if !f.set(strings.TrimSpace(key), strings.TrimSpace(value)) {
				return f, false
			}
		}
	}
	for _, key := range adminRequestFilterKeys {
		if !q.Has(key) {
			continue
		}
		if !f.set(key, q.Get(key)) {
			return f, false
		}
	}
	return f, true
}

// handleAdminRequests 请求行：ring 读出 → 筛选 → 排序（新→旧）→ 游标翻页。
// `limit` 1..200（缺省 50）；游标坏 = 400。**只回 ring 内已脱敏字段**。
func (s *Server) handleAdminRequests(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, ok := parseAdminLimit(q.Get("limit"), adminRequestsDefaultLimit, adminRequestsMaxLimit)
	if !ok {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "limit must be an integer >= 1")
		return
	}
	cur, hasCursor, err := decodeAdminRequestsCursor(q.Get("cursor"))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	filter, ok := parseAdminRequestFilter(q)
	if !ok {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error",
			"filter must be k=v pairs of "+strings.Join(adminRequestFilterKeys, "|")+
				" (same keys as the explicit query parameters)")
		return
	}
	resp := adminRequestsResponse{Rows: []adminRequestRowView{}}
	rows := s.adminRequests.snapshot() // 新→旧
	// 游标 = 本页末行（下一页从锚之后接续）；本页取满且**还有命中行**时才有下一页
	// ——「取满但后面全是过滤掉的」= 末页（游标留空，绝不发一个只会返回空页的锚）。
	var last adminRequestRow
	for i := range rows {
		if hasCursor && !adminRequestsAfterCursor(rows[i], cur) {
			continue
		}
		if !filter.match(rows[i]) {
			continue
		}
		if len(resp.Rows) >= limit {
			resp.NextCursor = encodeAdminRequestsCursor(last)
			break
		}
		resp.Rows = append(resp.Rows, adminRequestRowViewFrom(rows[i]))
		last = rows[i]
	}
	writeAdminJSON(w, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// GET /admin/requests/{id}
// ---------------------------------------------------------------------------

// adminRequestDetailResponse `GET /admin/requests/{id}` 响应体（请求面 +
// attempt 时间线；错误摘要只有枚举，见 adminAttemptErrorView）。
type adminRequestDetailResponse struct {
	Request  adminRequestRowView   `json:"request"`
	Attempts []adminRequestAttempt `json:"attempts"`
}

// handleAdminRequestDetail ring 精确查 id（未知 = 404）。**纯读**：ring 里只有脱敏
// 字段，详情面不再拼原文。
func (s *Server) handleAdminRequestDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	row, ok := s.adminRequests.find(id)
	if !ok {
		writeAdminError(w, http.StatusNotFound, "not_found", "unknown request id")
		return
	}
	attempts := row.Timeline
	if attempts == nil {
		attempts = []adminRequestAttempt{}
	}
	writeAdminJSON(w, http.StatusOK, adminRequestDetailResponse{
		Request:  adminRequestRowViewFrom(row),
		Attempts: attempts,
	})
}

// ---------------------------------------------------------------------------
// GET /admin/audit
// ---------------------------------------------------------------------------

// adminAuditRowView 审计行（design §5.6：ts / kind / sid_prefix / key_id /
// request_id / detail；ts = unix ms；detail = **出口再脱敏**后的 JSON 摘要）。
type adminAuditRowView struct {
	TS        int64  `json:"ts"`
	Kind      string `json:"kind"`
	SIDPrefix string `json:"sid_prefix,omitempty"`
	KeyID     string `json:"key_id,omitempty"`
	RequestID string `json:"request_id,omitempty"`
	Detail    string `json:"detail,omitempty"`
}

// adminAuditResponse `GET /admin/audit` 响应体（本单不做审计翻页：`next_cursor`
// 恒 `""`——行序 = store 返回序，见文件头「登记：audit 行序」）。
type adminAuditResponse struct {
	Rows       []adminAuditRowView `json:"rows"`
	NextCursor string              `json:"next_cursor"`
}

// handleAdminAudit 审计行：store 未启用 = 503 `store_unavailable`（不是 500）；
// `limit` 1..500（缺省 100）；`since` = RFC3339 或 unix ms；`kind` 原样透传
// （空 = 全部）。**出口脱敏**：detail 过 redactAdminDetail，sid 只回前缀。
//
// 登记：`Store.AuditQuery` 按 id 升序 + `limit` 截尾（取到的是**最早**的 limit
// 行）——要「最近 N 条」用 `since` 收窗口（残余风险见报告）。
func (s *Server) handleAdminAudit(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeAdminError(w, http.StatusServiceUnavailable, "store_unavailable", "store is not enabled")
		return
	}
	q := r.URL.Query()
	limit, ok := parseAdminLimit(q.Get("limit"), adminAuditDefaultLimit, adminAuditMaxLimit)
	if !ok {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "limit must be an integer >= 1")
		return
	}
	since, ok := parseAdminSince(q.Get("since"))
	if !ok {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "since must be RFC3339 or unix milliseconds")
		return
	}
	rows, err := s.store.AuditQuery(q.Get("kind"), since, limit)
	if err != nil {
		s.storeError("admin audit", err)
		writeAdminError(w, http.StatusServiceUnavailable, "store_unavailable", "store read failed")
		return
	}
	resp := adminAuditResponse{Rows: make([]adminAuditRowView, 0, len(rows))}
	for _, row := range rows {
		resp.Rows = append(resp.Rows, adminAuditRowView{
			TS:        row.TS.UnixMilli(),
			Kind:      redactAdminText(row.Kind),
			SIDPrefix: redactAdminText(sidPrefix(row.SID)),
			KeyID:     redactAdminText(row.KeyID),
			RequestID: redactAdminText(row.RequestID),
			Detail:    redactAdminDetail(row.Detail),
		})
	}
	writeAdminJSON(w, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// GET /admin/alerts
// ---------------------------------------------------------------------------

// adminAlertView 告警行（设计 §5.1/§5.6：severity / 名称 / 对象 / 说明 / 起始 /
// runbook；`since` = unix ms）。
type adminAlertView struct {
	ID       string `json:"id"`
	Severity string `json:"severity"`
	Name     string `json:"name"`
	Object   string `json:"object"`
	Message  string `json:"message"`
	Since    int64  `json:"since"`
	Runbook  string `json:"runbook"`
}

// adminAlertsResponse `GET /admin/alerts` 响应体（无数据 = 两个空数组，恒非 null）。
type adminAlertsResponse struct {
	Active  []adminAlertView `json:"active"`
	History []adminAlertView `json:"history"`
}

// adminAlertID 稳定 id（人可读；class + 对象）。
func adminAlertID(name, object string) string {
	return name + ":" + object
}

// adminAlertFromRingEntry ring 事件 → 告警类别（设计 §5.6「告警定义以 ring 事件
// 为准（17 类里挑 8 类做触发源）」的落地映射；未映射 = 不是告警——普通运营事件）：
//
//	store_error          → store_error（error）
//	quota_exhausted      → quota（warning；本服务日预算耗尽 429）
//	capacity_exhausted   → pool_dead（error；池全不可用）
//	credential_cooldown  → key_mismatch（class=credential：凭据被上游拒 = key 不一致
//	                       信号）/ 其余 → pool_low（warning；池可用面下降）
//	cooldown_trip        → 403 blocked（上游侧封禁）/ 429 quota（上游配额/限流）/
//	                       ≥500 5xx（上游故障）/ 其余不映射
//	precommit_error      → ≥500 5xx（错误信封已写下游的上游故障）
//
// `class=credential` 是串匹配：消息格式由 credentials_pool.go `reportFailure`
// 固定（`credential #N cooling for <d> (class=<class>)`），不改对方文件是本波写面
// 约束（见报告「残余风险」）。
func adminAlertFromRingEntry(e adminLogEntry) (name, severity string, ok bool) {
	switch e.Kind {
	case adminLogStoreError:
		return adminAlertStoreError, adminAlertSeverityError, true
	case adminLogQuotaExhausted:
		return adminAlertQuota, adminAlertSeverityWarning, true
	case adminLogCapacityExhausted:
		return adminAlertPoolDead, adminAlertSeverityError, true
	case adminLogCredentialCooldown:
		if strings.Contains(e.Message, "class=credential") {
			return adminAlertKeyMismatch, adminAlertSeverityError, true
		}
		return adminAlertPoolLow, adminAlertSeverityWarning, true
	case adminLogCooldownTrip:
		switch {
		case e.Status == http.StatusForbidden:
			return adminAlertBlocked, adminAlertSeverityError, true
		case e.Status == http.StatusTooManyRequests:
			return adminAlertQuota, adminAlertSeverityWarning, true
		case e.Status >= 500:
			return adminAlert5xx, adminAlertSeverityError, true
		}
	case adminLogPrecommitError:
		if e.Status >= 500 {
			return adminAlert5xx, adminAlertSeverityError, true
		}
	}
	return "", "", false
}

// adminStateAlerts 状态派生告警（**当前状态**，无历史锚——`since` = 本次观测
// 时刻）：
//
//	pool_low     池 ≥2 凭据且可用（未冷却）数 < 2；
//	pool_dead    池 ≥1 凭据且可用数 = 0（全冷却）；
//	blocked      管理面鉴权连续失败 → 当前封禁中的客户端 IP（diagnostics.admin）；
//	status_stale 观测面停摆：Diagnostics.Requests > 0 但**请求 ring 与事件 ring
//	             同时为空**（观测点没收到任何一条收尾记录 = 状态面不可信；健康档
//	             恒不触发——每次收尾都进请求 ring）。
func (s *Server) adminStateAlerts(now time.Time) []adminAlertView {
	out := make([]adminAlertView, 0, 2)
	nowMS := now.UnixMilli()
	for i := range s.cfg.Upstreams {
		name := redactAdminText(s.cfg.Upstreams[i].Name)
		size, creds := s.cfg.credentialPoolAt(i).adminSnapshot(now)
		if size == 0 {
			continue // 无池（单 api key / 未配凭据）——池面告警无对象
		}
		available := 0
		for _, c := range creds {
			if !c.Cooling {
				available++
			}
		}
		switch {
		case available == 0:
			out = append(out, adminAlertView{
				ID: adminAlertID(adminAlertPoolDead, name), Severity: adminAlertSeverityError,
				Name: adminAlertPoolDead, Object: name,
				Message: fmt.Sprintf("池 %d/%d 凭据全部冷却（池耗尽）", available, size),
				Since:   nowMS, Runbook: adminAlertRunbook(adminAlertPoolDead),
			})
		case size >= adminPoolLowMinAvailable && available < adminPoolLowMinAvailable:
			out = append(out, adminAlertView{
				ID: adminAlertID(adminAlertPoolLow, name), Severity: adminAlertSeverityWarning,
				Name: adminAlertPoolLow, Object: name,
				Message: fmt.Sprintf("池可用数 %d/%d（低于阈值 %d）", available, size, adminPoolLowMinAvailable),
				Since:   nowMS, Runbook: adminAlertRunbook(adminAlertPoolLow),
			})
		}
	}
	for _, banned := range s.adminAuthState.snapshot(now).BannedIPs {
		out = append(out, adminAlertView{
			ID: adminAlertID(adminAlertBlocked, banned.IP), Severity: adminAlertSeverityError,
			Name: adminAlertBlocked, Object: redactAdminText(banned.IP),
			Message: fmt.Sprintf("管理面鉴权连续失败 %d 次 → 封禁至 %s（剩余 %.0fs）",
				adminAuthMaxFailures, banned.BannedUntil, banned.RemainingSeconds),
			Since:   nowMS,
			Runbook: adminAlertRunbook(adminAlertBlocked),
		})
	}
	if diag := s.Diagnostics(); diag.Requests > 0 && s.adminRequests.size() == 0 {
		if count, _ := s.adminLog.stats(); count == 0 {
			out = append(out, adminAlertView{
				ID: adminAlertID(adminAlertStatusStale, "service"), Severity: adminAlertSeverityWarning,
				Name: adminAlertStatusStale, Object: "service",
				Message: fmt.Sprintf("观测面停摆：requests=%d 但请求 ring 与事件 ring 全空（收尾观测点未收到记录）",
					diag.Requests),
				Since:   nowMS,
				Runbook: adminRunbookDashboard,
			})
		}
	}
	return out
}

// handleAdminAlerts 告警面：ring 派生（窗口内 = active，窗口外 = history，按
// (class,object) 去重取最新一条）+ 状态派生（恒 active）。纯读：零计数器、零出网、
// 零写盘。
func (s *Server) handleAdminAlerts(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	resp := adminAlertsResponse{Active: []adminAlertView{}, History: []adminAlertView{}}
	seen := make(map[string]bool)
	win := s.adminLog.window(0, "", s.adminLog.capacityOrDefault(), false) // 新→旧
	for _, e := range win.Entries {
		name, severity, ok := adminAlertFromRingEntry(e)
		if !ok {
			continue
		}
		object := redactAdminText(e.Upstream)
		if object == "" {
			object = "service" // 全局面事件（store_error 等）无上游对象
		}
		key := adminAlertID(name, object)
		if seen[key] {
			continue
		}
		seen[key] = true
		alert := adminAlertView{
			ID:       key,
			Severity: severity,
			Name:     name,
			Object:   object,
			Message:  redactAdminText(e.Message),
			Since:    e.Time.UnixMilli(),
			Runbook:  adminAlertRunbook(name),
		}
		if now.Sub(e.Time) <= adminAlertsActiveWindow {
			resp.Active = append(resp.Active, alert)
			continue
		}
		if len(resp.History) < adminAlertsHistoryMax {
			resp.History = append(resp.History, alert)
		}
	}
	resp.Active = append(resp.Active, s.adminStateAlerts(now)...)
	sort.SliceStable(resp.Active, func(i, j int) bool {
		if resp.Active[i].Severity != resp.Active[j].Severity {
			return resp.Active[i].Severity == adminAlertSeverityError
		}
		if resp.Active[i].Since != resp.Active[j].Since {
			return resp.Active[i].Since < resp.Active[j].Since
		}
		return resp.Active[i].ID < resp.Active[j].ID
	})
	writeAdminJSON(w, http.StatusOK, resp)
}
