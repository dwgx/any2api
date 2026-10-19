// admin_export_test.go —— 导出面（/admin/export；board A2A-L19-W78-1）测试。
//
// 装置：真 Server + httptest（请求面用假上游，**零外部网络**）+ SQLite store。
//
// 反证锚点（回退即红）：
//   - **负控 1**：种子 `sk-SECRET` / `SECRET`（base_url userinfo / 代理 userinfo /
//     凭据池 / 审计 detail）——把 `adminSanitizeURL`、`adminProxyViewFor` 或
//     `redactAdminDetail` 换成原文 → TestAdminExportNoSecretBytes 红；
//   - 导出走原表（不过出口投影）→ 同上红；
//   - CSV 表头与 JSONL 键不同源 → TestAdminExportFormatsRowsAndHeaders 红。
package service

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 解析脚手架
// ---------------------------------------------------------------------------

// adminExportCSV 解析 CSV 导出体（含表头行）。
func adminExportCSV(t *testing.T, body string) [][]string {
	t.Helper()
	recs, err := csv.NewReader(strings.NewReader(body)).ReadAll()
	if err != nil {
		t.Fatalf("csv parse: %v body=%q", err, body)
	}
	return recs
}

// adminExportJSONL 解析 JSONL 导出体（一行一个完整对象；空体 = 0 行）。
func adminExportJSONL(t *testing.T, body string) []map[string]any {
	t.Helper()
	if strings.TrimSpace(body) == "" {
		return nil
	}
	lines := strings.Split(strings.TrimSuffix(body, "\n"), "\n")
	out := make([]map[string]any, 0, len(lines))
	for _, ln := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(ln), &m); err != nil {
			t.Fatalf("jsonl line %q: %v", ln, err)
		}
		out = append(out, m)
	}
	return out
}

// adminExportBody 发一次导出请求（带 key）并做 200 + 内容类型断言。
func adminExportBody(t *testing.T, url, key string) adminAPIResp {
	t.Helper()
	resp := adminAPIReq(t, http.MethodGet, url, key, "")
	if resp.status != http.StatusOK {
		t.Fatalf("GET %s = %d body=%s", url, resp.status, resp.body)
	}
	return resp
}

// adminExportFilename 从 Content-Disposition 里取文件名（形态守卫用）。
func adminExportFilename(t *testing.T, resp adminAPIResp) string {
	t.Helper()
	cd := resp.header.Get("Content-Disposition")
	const prefix, suffix = `attachment; filename="`, `"`
	if !strings.HasPrefix(cd, prefix) || !strings.HasSuffix(cd, suffix) {
		t.Fatalf("Content-Disposition = %q（want attachment; filename=\"…\"）", cd)
	}
	return strings.TrimSuffix(strings.TrimPrefix(cd, prefix), suffix)
}

// adminExportLiveConfig 带假上游（httptest）+ SQLite store 的配置（请求面导出用）。
func adminExportLiveConfig(t *testing.T, key string) *Config {
	t.Helper()
	up := goodClaudeUpstream(t)
	cfg := adminOpsStoreConfig(t, key)
	cfg.Upstreams = []Upstream{anthUp("fake", up.srv.URL)}
	return cfg
}

// ---------------------------------------------------------------------------
// 1. 形状：表头/JSONL 键同源、行数、Content-Type/Disposition、空态
// ---------------------------------------------------------------------------

func TestAdminExportFormatsRowsAndHeaders(t *testing.T) {
	const key = "w78-key"
	s, srv := adminOpsStart(t, adminOpsStoreConfig(t, key))
	day := storeDay(time.Now())
	for _, row := range []struct {
		scope string
		req   int
		units int
	}{
		{storeUsageScopeGlobal, 2, 5},
		{"session:aaaa1111aaaaaaaaaaaaaa", 3, 7},
		{"model:claude-x", 1, 2},
	} {
		if err := s.store.UsageAdd(row.scope, day, row.req, row.units); err != nil {
			t.Fatalf("seed usage %s: %v", row.scope, err)
		}
	}

	// ---- accounts：一行一凭据（空池 = credential_index:-1 的占位行）----
	resp := adminExportBody(t, srv.URL+"/admin/export?type=accounts&format=csv", key)
	if ct := resp.header.Get("Content-Type"); ct != "text/csv; charset=utf-8" {
		t.Fatalf("accounts CSV Content-Type = %q", ct)
	}
	name := adminExportFilename(t, resp)
	if !strings.HasPrefix(name, "any2api-accounts-") || !strings.HasSuffix(name, ".csv") {
		t.Fatalf("accounts 文件名 = %q", name)
	}
	const wantAccountsHeader = "upstream_index,upstream,name,protocol,base_url,proxy_display,tls_fingerprint,credential_index,cooling,cooldown_until,last_used"
	recs := adminExportCSV(t, resp.body)
	if len(recs) == 0 || strings.Join(recs[0], ",") != wantAccountsHeader {
		t.Fatalf("accounts CSV 表头 = %v", recs[0])
	}
	// 4 台上游：anthropic(1) + web(3) + empty-web(1 占位) + plain-http(1) = 6 行数据。
	if len(recs) != 7 {
		t.Fatalf("accounts CSV 行 = %d（want 7 = 表头 + 6）body=%s", len(recs), resp.body)
	}
	byKey := make(map[string][]string, len(recs))
	for _, row := range recs[1:] {
		byKey[row[1]+"#"+row[7]] = row
	}
	if got := byKey["anthropic-main#0"]; got == nil || got[4] != "https://api.example.com/v1" || got[7] != "0" {
		t.Fatalf("anthropic 行 = %v", got)
	}
	if got := byKey["web#2"]; got == nil || got[5] != "socks5h://127.0.0.1:1080" || got[6] != "chrome" {
		t.Fatalf("web 行 = %v", got)
	}
	if got := byKey["empty-web#-1"]; got == nil || got[5] != "" || got[8] != "false" || got[9] != "0" {
		t.Fatalf("空池占位行 = %v", got)
	}

	// ---- accounts JSONL：行数一致 + 键集 = CSV 表头 ----
	resp = adminExportBody(t, srv.URL+"/admin/export?type=accounts", key)
	if ct := resp.header.Get("Content-Type"); ct != "application/x-ndjson; charset=utf-8" {
		t.Fatalf("accounts JSONL Content-Type = %q", ct)
	}
	if !strings.HasSuffix(adminExportFilename(t, resp), ".jsonl") {
		t.Fatalf("缺省 format 应为 jsonl: %q", resp.header.Get("Content-Disposition"))
	}
	rows := adminExportJSONL(t, resp.body)
	if len(rows) != 6 {
		t.Fatalf("accounts JSONL 行 = %d（want 6）body=%s", len(rows), resp.body)
	}
	wantKeys := strings.Split(wantAccountsHeader, ",")
	for _, row := range rows {
		if len(row) != len(wantKeys) {
			t.Fatalf("accounts JSONL 键集 = %v", row)
		}
		for _, k := range wantKeys {
			if _, ok := row[k]; !ok {
				t.Fatalf("accounts JSONL 缺键 %q: %v", k, row)
			}
		}
	}

	// ---- proxies ----
	resp = adminExportBody(t, srv.URL+"/admin/export?type=proxies&format=csv", key)
	const wantProxyHeader = "upstream,index,configured,scheme,host,port,auth,display,tls_fingerprint"
	recs = adminExportCSV(t, resp.body)
	if strings.Join(recs[0], ",") != wantProxyHeader || len(recs) != 5 {
		t.Fatalf("proxies CSV = %v", recs)
	}
	if recs[1][2] != "false" || recs[2][7] != "socks5h://127.0.0.1:1080" || recs[2][6] != "true" {
		t.Fatalf("proxies CSV 行 = %v", recs[1:])
	}
	resp = adminExportBody(t, srv.URL+"/admin/export?type=proxies", key)
	if got := adminExportJSONL(t, resp.body); len(got) != 4 || got[3]["display"] != "http://127.0.0.1:3128" {
		t.Fatalf("proxies JSONL = %+v", got)
	}

	// ---- usage：scope 过滤 + session 只回前缀 + 空态（CSV 表头恒写 / JSONL 空体）----
	resp = adminExportBody(t, srv.URL+"/admin/export?type=usage&scope=session&format=csv", key)
	recs = adminExportCSV(t, resp.body)
	if strings.Join(recs[0], ",") != "scope,requests,cost_units" || len(recs) != 2 {
		t.Fatalf("usage CSV = %v", recs)
	}
	if recs[1][0] != "session:aaaa1111" || recs[1][1] != "3" || recs[1][2] != "7" {
		t.Fatalf("session 行未按前缀脱敏: %v", recs[1])
	}
	resp = adminExportBody(t, srv.URL+"/admin/export?type=usage&scope=model", key)
	if got := adminExportJSONL(t, resp.body); len(got) != 1 || got[0]["scope"] != "model:claude-x" || got[0]["requests"] != float64(1) {
		t.Fatalf("usage model JSONL = %+v", got)
	}
	resp = adminExportBody(t, srv.URL+"/admin/export?type=usage", key) // 缺省 scope=global
	if got := adminExportJSONL(t, resp.body); len(got) != 1 || got[0]["scope"] != "global" {
		t.Fatalf("usage 缺省 scope = %+v", got)
	}
	resp = adminExportBody(t, srv.URL+"/admin/export?type=usage&day=2020-01-01", key)
	if strings.TrimSpace(resp.body) != "" {
		t.Fatalf("空日 JSONL 应为空体: %q", resp.body)
	}
	resp = adminExportBody(t, srv.URL+"/admin/export?type=usage&day=2020-01-01&format=csv", key)
	if got := adminExportCSV(t, resp.body); len(got) != 1 || strings.Join(got[0], ",") != "scope,requests,cost_units" {
		t.Fatalf("空日 CSV 应只有表头: %v", got)
	}

	// ---- audit：安全下线本身也进导出（kind 可 join）+ 出口脱敏 ----
	cooldown := adminAPIReq(t, http.MethodPost, srv.URL+"/admin/accounts/web/0/cooldown", key, `{"seconds":60,"reason":"export-check"}`)
	if cooldown.status != http.StatusOK {
		t.Fatalf("cooldown = %d body=%s", cooldown.status, cooldown.body)
	}
	resp = adminExportBody(t, srv.URL+"/admin/export?type=audit&format=csv", key)
	recs = adminExportCSV(t, resp.body)
	if strings.Join(recs[0], ",") != "ts,kind,sid_prefix,key_id,request_id,detail" || len(recs) != 2 {
		t.Fatalf("audit CSV = %v", recs)
	}
	if recs[1][1] != storeAuditKindAccountCooldown || !strings.Contains(recs[1][5], "export-check") {
		t.Fatalf("audit 行 = %v", recs[1])
	}
}

// ---------------------------------------------------------------------------
// 2. 参数校验与边界（type/format/limit/day/scope + 405）
// ---------------------------------------------------------------------------

func TestAdminExportValidationAndLimits(t *testing.T) {
	const key = "w78-key"
	_, srv := adminOpsStart(t, adminOpsStoreConfig(t, key))

	for _, path := range []string{
		"/admin/export",                          // type 必填
		"/admin/export?type=bogus",               // 未知 type
		"/admin/export?type=accounts&format=xml", // 未知 format
		"/admin/export?type=requests&limit=0",
		"/admin/export?type=requests&limit=5001",
		"/admin/export?type=requests&limit=abc",
		"/admin/export?type=usage&limit=-3",
		"/admin/export?type=audit&limit=", // 空值 = 缺省（**不是** 400）
		"/admin/export?type=usage&day=2026-13-40",
		"/admin/export?type=usage&day=not-a-day",
		"/admin/export?type=usage&scope=bogus",
	} {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+path, key, "")
		want := http.StatusBadRequest
		if strings.HasSuffix(path, "limit=") {
			want = http.StatusOK // 空 limit = 用缺省（既有 parseAdminLimit 语义）
		}
		if resp.status != want {
			t.Fatalf("%s = %d（want %d）body=%s", path, resp.status, want, resp.body)
		}
	}
	// 边界合法值：1 与 5000。
	for _, path := range []string{"/admin/export?type=requests&limit=1", "/admin/export?type=audit&limit=5000"} {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+path, key, "")
		if resp.status != http.StatusOK {
			t.Fatalf("%s = %d（want 200）body=%s", path, resp.status, resp.body)
		}
	}
	// accounts/proxies **忽略** limit（越界/非法都不 400）。
	for _, path := range []string{
		"/admin/export?type=accounts&limit=99999",
		"/admin/export?type=accounts&limit=abc",
		"/admin/export?type=proxies&limit=0",
	} {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+path, key, "")
		if resp.status != http.StatusOK {
			t.Fatalf("%s = %d（want 200：accounts/proxies 忽略 limit）body=%s", path, resp.status, resp.body)
		}
	}
	// 方法错 = 405（注册面 = GET）。
	resp := adminAPIReq(t, http.MethodPost, srv.URL+"/admin/export?type=accounts", key, "")
	if resp.status != http.StatusMethodNotAllowed {
		t.Fatalf("POST /admin/export = %d（want 405）", resp.status)
	}
}

// ---------------------------------------------------------------------------
// 3. **负控 1**：导出字节零凭据（种子 sk-SECRET / SECRET 全形态）
// ---------------------------------------------------------------------------

func TestAdminExportNoSecretBytes(t *testing.T) {
	const key = "w78-key"
	s, srv := adminOpsStart(t, adminOpsStoreConfig(t, key))

	// 审计里再种一条**原文形态**的秘密（detail 是自由文本：redactAdminText 的
	// `sk-…` / `key=value` 两条正则都要接住它）。
	s.auditStore(storeAudit{
		TS:     time.Now(),
		Kind:   "seed",
		Detail: `note=sk-SECRET-raw api_key=SECRET-assign cookie=SECRET-cookie`,
	})

	for _, typ := range []string{"accounts", "proxies", "requests", "usage", "audit"} {
		for _, format := range []string{"csv", "jsonl"} {
			url := srv.URL + "/admin/export?type=" + typ + "&format=" + format
			resp := adminExportBody(t, url, key)
			needles := []string{"SECRET"}
			if typ != "audit" {
				// audit 的投影占位符本身含 `sk-[redacted]`（它是**输出形态**，不是
				// 凭据）；其余面连 `sk-` 都不该出现。
				needles = append(needles, "sk-")
			}
			for _, needle := range needles {
				if strings.Contains(resp.body, needle) {
					t.Fatalf("%s/%s 导出出现 %q：%s", typ, format, needle, resp.body)
				}
			}
		}
	}
	// 正面：审计原文确实被投影（说明测试真的种到了会泄露的字节）。
	resp := adminExportBody(t, srv.URL+"/admin/export?type=audit", key)
	rows := adminExportJSONL(t, resp.body)
	if len(rows) != 1 {
		t.Fatalf("audit 行 = %d（want 1）", len(rows))
	}
	detail, _ := rows[0]["detail"].(string)
	if !strings.Contains(detail, "[redacted]") || !strings.Contains(detail, "sk-[redacted]") {
		t.Fatalf("审计 detail 未按出口投影: %q", detail)
	}
	// 池凭据只在内存：账号导出只回下标（凭据值不出现，已在上面的 needle 检查里覆盖）。
	var acc adminAccountsResponse
	adminAPIGetJSON(t, srv.URL+"/admin/accounts", key, &acc)
	if acc.Upstreams[1].PoolSize != 3 {
		t.Fatalf("池大小 = %d", acc.Upstreams[1].PoolSize)
	}
}

// ---------------------------------------------------------------------------
// 4. 请求面导出：ring 行（已脱敏）→ JSONL/CSV，行数 = limit
// ---------------------------------------------------------------------------

func TestAdminExportRequestsFromRing(t *testing.T) {
	const key = "w78-key"
	_, srv := adminOpsStart(t, adminExportLiveConfig(t, key))

	for _, model := range []string{"model-a", "sk-SECRET-model"} {
		resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
			strings.NewReader(`{"model":"`+model+`","messages":[{"role":"user","content":"hi"}],"stream":true}`))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("chat (%s) = %d", model, resp.StatusCode)
		}
	}

	resp := adminExportBody(t, srv.URL+"/admin/export?type=requests&limit=1", key)
	rows := adminExportJSONL(t, resp.body)
	if len(rows) != 1 {
		t.Fatalf("requests limit=1 行 = %d（want 1）body=%s", len(rows), resp.body)
	}
	resp = adminExportBody(t, srv.URL+"/admin/export?type=requests&format=csv", key)
	recs := adminExportCSV(t, resp.body)
	if len(recs) != 3 {
		t.Fatalf("requests CSV 行 = %d（want 3 = 表头 + 2）body=%s", len(recs), resp.body)
	}
	const wantHeader = "time,request_id,entry,model,stream,status,attempts,failovers,upstream,upstreams,credential_index,ttft_ms,total_ms,in,out,outcome,sid_prefix,key_id,task,failed,delivered"
	if strings.Join(recs[0], ",") != wantHeader {
		t.Fatalf("requests CSV 表头 = %v", recs[0])
	}
	// 模型名里的 `sk-…` 形态在 ring 写入期已投影 → 导出面同样零凭据字节（投影
	// 占位符 `sk-[redacted]` 是输出形态，故只查 SECRET）。
	if strings.Contains(resp.body, "SECRET") {
		t.Fatalf("requests 导出出现凭据字节: %s", resp.body)
	}
	// 空 ring（新进程）→ CSV 只有表头；JSONL 空体。
	_, srv2 := adminOpsStart(t, adminExportLiveConfig(t, key))
	resp = adminExportBody(t, srv2.URL+"/admin/export?type=requests&format=csv", key)
	if got := adminExportCSV(t, resp.body); len(got) != 1 {
		t.Fatalf("空 ring CSV = %v", got)
	}
	resp = adminExportBody(t, srv2.URL+"/admin/export?type=requests", key)
	if strings.TrimSpace(resp.body) != "" {
		t.Fatalf("空 ring JSONL 应为空体: %q", resp.body)
	}
}

// ---------------------------------------------------------------------------
// 5. store 未启用：只有 usage/audit 503（accounts/proxies/requests 不依赖 store）
// ---------------------------------------------------------------------------

func TestAdminExportStoreUnavailable503(t *testing.T) {
	const key = "w78-key"
	cfg := adminOpsConfig(t, key) // 无 store
	_, srv := adminOpsStart(t, cfg)

	for _, typ := range []string{"usage", "audit"} {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/export?type="+typ+"&format=csv", key, "")
		if resp.status != http.StatusServiceUnavailable || !strings.Contains(resp.body, "store_unavailable") {
			t.Fatalf("type=%s = %d body=%s（want 503 store_unavailable）", typ, resp.status, resp.body)
		}
	}
	for _, typ := range []string{"accounts", "proxies", "requests"} {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/export?type="+typ+"&format=csv", key, "")
		if resp.status != http.StatusOK {
			t.Fatalf("type=%s = %d（不依赖 store，want 200）body=%s", typ, resp.status, resp.body)
		}
	}
}
