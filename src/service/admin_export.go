// admin_export.go —— 导出面（board A2A-L19-W78-1；设计 `docs/admin-dashboard.md`
// §6.1 #15「CSV/JSONL 导出（审计/台账）」· §7.2「导出前过同一套脱敏投影」）。
//
// 端点（挂 adminAuth；未配 admin_key = 路由根本不注册 → 404 非 401）：
//
//	GET /admin/export?type=accounts|proxies|requests|usage|audit&format=csv|jsonl&day=&scope=&limit=
//
// 数据源（**全部复用既有出口的脱敏投影**——「不要导出就走原表」，§7.2）：
//   - accounts：`/admin/accounts` 的扁平化（一行一凭据；空池 = 一行
//     `credential_index:-1`）；base_url/proxy 已消毒；
//   - proxies：`/admin/proxies` 的行；
//   - requests：请求 ring 的**已脱敏**行（`adminRequestRowViewFrom`）；
//   - usage：`Store.UsageScopes(day)` 按 scope 类过滤（scope 展示形态过
//     `adminUsageDisplayScope`——session 只留 8 字符前缀）；
//   - audit：`Store.AuditQuery("", zero, limit)` + `redactAdminDetail` /
//     `sidPrefix`（与 `/admin/audit` 同一条出口投影）。
//
// 红线（§7.2）：导出字节里**不出现** api_key / 凭据值 / authorization / cookie /
// x-management-key / 完整 sid / 原始 IP。落表：CSV 表头 = JSONL 键（同一批 json
// tag 派生，两面同源）；空数据 CSV 仍写表头、JSONL 空体（0 行）。
//
// 登记：`usage` 导的是**过滤后的台账行**（一行 = store 的 usage_counters 行，
// scope 已按类别脱敏），不做 `/admin/usage` 的聚合/排序/合计——导出面要的是可
// join 的原始行，不是给人看的榜单。`limit` 是**截尾**（取前 N 行），不是分页。
package service

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// adminExportDefaultLimit / adminExportMaxLimit 导出行数边界（requests /
	// usage / audit；accounts / proxies 忽略 limit——它们由上游数界定）。
	adminExportDefaultLimit = 1000
	adminExportMaxLimit     = 5000
)

// adminExportTypes 合法 type 词表（**必填**；其余 = 400）。
var adminExportTypes = []string{"accounts", "proxies", "requests", "usage", "audit"}

// 列序（= JSONL 键序；CSV 表头逐字用这些键）。accounts / proxies 的键名与
// `/admin/accounts`、`/admin/proxies` 的 JSON 键同源（投影面自定扁平形态）。
var (
	adminExportAccountsColumns = []string{
		"upstream_index", "upstream", "name", "protocol", "base_url", "proxy_display",
		"tls_fingerprint", "credential_index", "cooling", "cooldown_until", "last_used",
	}
	adminExportProxyColumns = []string{
		"upstream", "index", "configured", "scheme", "host", "port", "auth", "display", "tls_fingerprint",
	}
	adminExportRequestColumns = []string{
		"time", "request_id", "entry", "model", "stream", "status", "attempts", "failovers",
		"upstream", "upstreams", "credential_index", "ttft_ms", "total_ms", "in", "out",
		"outcome", "sid_prefix", "key_id", "task", "failed", "delivered",
	}
	adminExportUsageColumns = []string{"scope", "requests", "cost_units"}
	adminExportAuditColumns = []string{"ts", "kind", "sid_prefix", "key_id", "request_id", "detail"}
)

// ---------------------------------------------------------------------------
// 路由注册
// ---------------------------------------------------------------------------

// registerAdminExport 注册导出端点。**自门控**：未配 admin_key 时一条都不注册
// （与既有 /admin/* 逐字一致：404 非 401）。调用点 = `Server.Handler()` 的
// admin_key 分支。
func (s *Server) registerAdminExport(mux *http.ServeMux) {
	if s == nil || s.cfg.adminKey() == "" {
		return
	}
	mux.HandleFunc("GET /admin/export", s.adminAuth(s.handleAdminExport))
}

// ---------------------------------------------------------------------------
// 行投影（takes typed rows → JSON-键映射；CSV 与 JSONL 共用一份来源）
// ---------------------------------------------------------------------------

// adminExportRowMap 结构体 → JSON 键映射（**键名 = 该结构体的 json tag**——CSV
// 表头与 JSONL 键因此天然同源；`omitempty` 字段缺省即缺席，CSV 格为空串）。
func adminExportRowMap(v any) map[string]any {
	b, err := json.Marshal(v)
	if err != nil {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return map[string]any{}
	}
	return m
}

// adminExportCell 单个值 → CSV 格（JSON 往返后数值是 float64；整数值按整数打）。
func adminExportCell(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case float64:
		if t == math.Trunc(t) && math.Abs(t) < 1e15 {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return fmt.Sprint(t)
	}
}

// ---------------------------------------------------------------------------
// GET /admin/export
// ---------------------------------------------------------------------------

// validAdminExportType type 词表校验。
func validAdminExportType(typ string) bool {
	for _, t := range adminExportTypes {
		if typ == t {
			return true
		}
	}
	return false
}

// adminExportDayArg 文件名日戳（YYYYMMDD）。
func adminExportDayStamp(day string) string {
	return strings.ReplaceAll(day, "-", "")
}

// handleAdminExport 导出：`type`（必填）+ `format`（csv|jsonl，缺省 jsonl）+
// `limit`（requests|usage|audit 1..5000，缺省 1000；越界 = 400）+ `day`/`scope`
// （仅 usage）。**只读、零出网、零配置写**；store 未启用时只有 usage/audit 503。
func (s *Server) handleAdminExport(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	typ := q.Get("type")
	if !validAdminExportType(typ) {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "type must be accounts|proxies|requests|usage|audit")
		return
	}
	format := q.Get("format")
	if format == "" {
		format = "jsonl"
	}
	if format != "csv" && format != "jsonl" {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "format must be csv|jsonl")
		return
	}
	limit := adminExportDefaultLimit
	if typ == "requests" || typ == "usage" || typ == "audit" {
		if raw := q.Get("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 1 || n > adminExportMaxLimit {
				writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "limit must be an integer in 1..5000")
				return
			}
			limit = n
		}
	}
	now := time.Now()
	day := storeDay(now)
	scope := "global"
	if typ == "usage" {
		d, ok := adminUsageDay(q.Get("day"), now)
		if !ok {
			writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "day must be YYYY-MM-DD")
			return
		}
		day = d
		if raw := q.Get("scope"); raw != "" {
			scope = raw
		}
		if !validAdminUsageScope(scope) {
			writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "scope must be global|session|key|ip|model")
			return
		}
	}
	var (
		header []string
		rows   []map[string]any
	)
	switch typ {
	case "accounts":
		header, rows = adminExportAccountRows(s, now)
	case "proxies":
		header, rows = adminExportProxyRows(s, now)
	case "requests":
		header, rows = adminExportRequestRows(s, limit)
	case "usage":
		out, err := s.adminExportUsageRows(day, scope, limit)
		if err != nil {
			s.storeError("admin export usage", err)
			writeAdminError(w, http.StatusServiceUnavailable, "store_unavailable", "store read failed")
			return
		}
		header, rows = adminExportUsageColumns, out
	case "audit":
		out, err := s.adminExportAuditRows(limit)
		if err != nil {
			s.storeError("admin export audit", err)
			writeAdminError(w, http.StatusServiceUnavailable, "store_unavailable", "store read failed")
			return
		}
		header, rows = adminExportAuditColumns, out
	}
	ext, contentType := "jsonl", "application/x-ndjson; charset=utf-8"
	if format == "csv" {
		ext, contentType = "csv", "text/csv; charset=utf-8"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition",
		fmt.Sprintf("attachment; filename=%q", fmt.Sprintf("any2api-%s-%s.%s", typ, adminExportDayStamp(day), ext)))
	if format == "csv" {
		adminExportWriteCSV(w, header, rows)
		return
	}
	adminExportWriteJSONL(w, rows)
}

// adminExportAccountRows 账号导出：一行一凭据（**扁平**：上游元信息 × 凭据状态）；
// 空池上游 = 一行 `credential_index:-1`（该行只有上游面信息——空池也是要导的事实）。
func adminExportAccountRows(s *Server, now time.Time) ([]string, []map[string]any) {
	snap := s.adminAccountsSnapshot(now)
	rows := make([]map[string]any, 0, len(snap.Upstreams))
	for _, up := range snap.Upstreams {
		if len(up.Credentials) == 0 {
			rows = append(rows, adminExportFlatAccountRow(up, adminPoolCredentialView{Index: -1}))
			continue
		}
		for _, c := range up.Credentials {
			rows = append(rows, adminExportFlatAccountRow(up, c))
		}
	}
	return adminExportAccountsColumns, rows
}

// adminExportFlatAccountRow 账号导出的扁平行（键 = adminExportAccountsColumns）。
func adminExportFlatAccountRow(up adminAccountsUpstreamView, c adminPoolCredentialView) map[string]any {
	return map[string]any{
		"upstream_index":   up.Index,
		"upstream":         up.Name,
		"name":             up.Name,
		"protocol":         up.Protocol,
		"base_url":         up.BaseURL,
		"proxy_display":    up.Proxy.Display,
		"tls_fingerprint":  up.TLSFingerprint,
		"credential_index": c.Index,
		"cooling":          c.Cooling,
		"cooldown_until":   c.CooldownUntil,
		"last_used":        c.LastUsed,
	}
}

// adminExportProxyRows 代理导出：一行一上游（含未配置行）。
func adminExportProxyRows(s *Server, now time.Time) ([]string, []map[string]any) {
	snap := s.adminProxiesSnapshot(now)
	rows := make([]map[string]any, 0, len(snap.Proxies))
	for _, row := range snap.Proxies {
		rows = append(rows, adminExportRowMap(row))
	}
	return adminExportProxyColumns, rows
}

// adminExportRequestRows 请求导出：请求 ring 的**已脱敏**行（新→旧），取前 limit。
func adminExportRequestRows(s *Server, limit int) ([]string, []map[string]any) {
	snap := s.adminRequests.snapshot()
	if len(snap) > limit {
		snap = snap[:limit]
	}
	rows := make([]map[string]any, 0, len(snap))
	for _, row := range snap {
		rows = append(rows, adminExportRowMap(adminRequestRowViewFrom(row)))
	}
	return adminExportRequestColumns, rows
}

// adminExportUsageRows 台账导出：`UsageScopes(day)` 按 scope 类过滤（展示形态过
// `adminUsageDisplayScope`——session 只留前缀），按 scope 升序稳定输出，取前 limit。
func (s *Server) adminExportUsageRows(day, scope string, limit int) ([]map[string]any, error) {
	if s.store == nil {
		return nil, errStoreDisabled
	}
	usage, err := s.store.UsageScopes(day)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]any, 0, len(usage))
	for _, u := range usage {
		if !adminUsageScopeMatch(scope, u.Scope) {
			continue
		}
		rows = append(rows, map[string]any{
			"scope":      adminUsageDisplayScope(scope, u.Scope),
			"requests":   u.Requests,
			"cost_units": u.CostUnits,
		})
		if len(rows) >= limit {
			break
		}
	}
	return rows, nil
}

// adminExportAuditRows 审计导出：`AuditQuery("", 零值, limit)` + **出口同一套脱敏**
// （`redactAdminDetail` / `sidPrefix`——绝不「导出就走原表」）。
func (s *Server) adminExportAuditRows(limit int) ([]map[string]any, error) {
	if s.store == nil {
		return nil, errStoreDisabled
	}
	recs, err := s.store.AuditQuery("", time.Time{}, limit)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]any, 0, len(recs))
	for _, row := range recs {
		rows = append(rows, adminExportRowMap(adminAuditRowView{
			TS:        row.TS.UnixMilli(),
			Kind:      redactAdminText(row.Kind),
			SIDPrefix: redactAdminText(sidPrefix(row.SID)),
			KeyID:     redactAdminText(row.KeyID),
			RequestID: redactAdminText(row.RequestID),
			Detail:    redactAdminDetail(row.Detail),
		}))
	}
	return rows, nil
}

// errStoreDisabled 导出面的 store 未启用哨兵（调用方映射 503 store_unavailable）。
var errStoreDisabled = errors.New("store is not enabled")

// ---------------------------------------------------------------------------
// 写出
// ---------------------------------------------------------------------------

// adminExportWriteCSV 写 CSV：**表头恒写**（空数据 = 只有表头一行），列序 = 表头，
// 逐格取值（缺键 = 空串）。写失败只中止（对端已走，无需错误面）。
func adminExportWriteCSV(w http.ResponseWriter, header []string, rows []map[string]any) {
	cw := csv.NewWriter(w)
	if err := cw.Write(header); err != nil {
		return
	}
	cells := make([]string, len(header))
	for _, row := range rows {
		for i, key := range header {
			cells[i] = adminExportCell(row[key])
		}
		if err := cw.Write(cells); err != nil {
			return
		}
	}
	cw.Flush()
}

// adminExportWriteJSONL 写 JSONL：一行一个完整 JSON 对象（**无信封**）；空数据 =
// 空体（0 行）。
func adminExportWriteJSONL(w http.ResponseWriter, rows []map[string]any) {
	enc := json.NewEncoder(w)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return
		}
	}
}
