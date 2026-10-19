// admin_dashboard_test.go —— dashboard P0-0 只读壳（/admin/audit|requests|/{id}|
// alerts）测试（board A2A-L19-W71-1；设计 `docs/admin-dashboard.md` §5.3/§5.6/
// §6.1/§9）。
//
// 装置：真 Server + httptest；管理面 mux 由 `registerAdminDashboard` 装配（与
// Handler 的 admin_key 分支同一入口——覆盖「未配 key = 404 / 错 key = 401 / 对
// key = 200」三态）。审计面需要 store（sqlite，t.TempDir()）。
//
// 反证锚点（回退即红）：
//   - registerAdminDashboard 去掉 admin_key 自门控 → TestAdminDashboardNoKey404 红；
//   - 注册前访问 → TestAdminDashboardPreRegistration404 红（先 404 后 200）；
//   - 请求 ring 写入点的脱敏（adminRequestRowFrom 的 redactAdminText）或审计出口
//     的 redactAdminDetail 去掉 → TestAdminDashboardSecretRedactionNegativeControl 红
//     （负控：响应体出现 SECRET）；
//   - ring 无界 / 覆盖写坏 → TestAdminDashboardRequestRingBounded 红；
//   - 筛选/游标/分页错 → TestAdminDashboardRequestsFiltersAndPaging 红；
//   - 告警映射错（ring → 8 类） → TestAdminDashboardAlertsRingDerived 红。
package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// 装置
// ---------------------------------------------------------------------------

// adminDashConfig 管理面测试配置：admin key + 三个上游（单 key / 3 凭据池）。
func adminDashConfig(t *testing.T, key string) *Config {
	t.Helper()
	cfg := uiTestConfig()
	cfg.HeartbeatSeconds = 15
	cfg.AdminKey = key
	cfg.Upstreams = append(cfg.Upstreams, Upstream{
		Name:        "web",
		Protocol:    ProtocolClaudeWeb,
		Credentials: []string{"sk-secret-a", "sk-secret-b", "sk-secret-c"},
	})
	return cfg
}

// adminDashStart 启动被测服务；mux = registerAdminAPI + registerAdminDashboard
// （管理面注册面本体）。
func adminDashStart(t *testing.T, cfg *Config) (*Server, *httptest.Server) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	mux := http.NewServeMux()
	s.registerAdminAPI(mux)
	s.registerAdminDashboard(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.Close()
		_ = s.CloseStore()
	})
	return s, srv
}

// adminDashEndpoints 四个 dashboard 端点（无 key 时的 404 断言集）。
var adminDashEndpoints = []string{
	"/admin/audit",
	"/admin/requests",
	"/admin/requests/req-deadbeef",
	"/admin/alerts",
}

// adminDashAttemptSeed 一条尝试的种子（字段与 attemptRecord 对齐）。
type adminDashAttemptSeed struct {
	upstream     string
	credIndex    int
	status       int
	total        time.Duration
	ttft         time.Duration
	failed       bool
	disconnected bool
	delivered    bool
	endReason    string
	in, out      int64
}

// adminDashRecorder 造一个请求收尾记账器（sid/keyID 已填；attempt 时间线可控）。
func adminDashRecorder(reqID string, entry registry.Format, model, task string, stream bool, atts []adminDashAttemptSeed) *requestRecorder {
	rec := newRequestRecorder(reqID, entry, model, stream, task)
	rec.sid = sidPrefix("a1b2c3d4e5f6a7b8") // 真实路径恒 8 字符前缀
	rec.keyID = "3106c924"
	for i, a := range atts {
		ar := rec.begin(i+1, Upstream{Name: a.upstream, Protocol: ProtocolAnthropic})
		ar.credentialIndex = a.credIndex
		ar.status = a.status
		ar.failed = a.failed
		ar.disconnected = a.disconnected
		ar.delivered = a.delivered
		ar.endReason = a.endReason
		ar.ttft = a.ttft
		ar.in, ar.out = a.in, a.out
		ar.start = time.Now().Add(-a.total)
		rec.finish(ar) // end = now → total_ms ≈ a.total
	}
	return rec
}

// adminDashSeedRow 直接落一行 ring 行（排序/筛选/分页测试用——时刻可控）。
func adminDashSeedRow(s *Server, id string, at time.Time, entry, model string, stream bool, atts []adminDashAttemptSeed) adminRequestRow {
	rec := adminDashRecorder(id, registry.Format(entry), model, "", stream, atts)
	total := time.Duration(0)
	for _, a := range atts {
		total += a.total
	}
	row := adminRequestRowFrom(rec, at)
	row.TotalMs = total.Milliseconds()
	s.adminRequests.add(row)
	return row
}

// adminDashGet 一发 GET + 解码（非 200 即失败）。
func adminDashGet(t *testing.T, url, key string, dst any) string {
	t.Helper()
	resp := adminAPIReq(t, http.MethodGet, url, key, "")
	if resp.status != http.StatusOK {
		t.Fatalf("GET %s = %d body=%s", url, resp.status, resp.body)
	}
	if dst != nil {
		if err := json.Unmarshal([]byte(resp.body), dst); err != nil {
			t.Fatalf("decode %s: %v body=%s", url, err, resp.body)
		}
	}
	return resp.body
}

// adminDashAgeRingEntry 把 ring 里最新一条事件的时间改成指定时刻（history 面测试
// 用——生产路径的 Time 恒为写入时刻）。
func adminDashAgeRingEntry(t *testing.T, s *Server, at time.Time) {
	t.Helper()
	s.adminLog.mu.Lock()
	defer s.adminLog.mu.Unlock()
	if s.adminLog.count == 0 {
		t.Fatalf("ring 为空——无从老化")
	}
	idx := (s.adminLog.next - 1 + len(s.adminLog.buf)) % len(s.adminLog.buf)
	s.adminLog.buf[idx].Time = at.UTC()
}

// ---------------------------------------------------------------------------
// 1. 注册前后（自审 A）：注册前 404 → 注册后 200
// ---------------------------------------------------------------------------

func TestAdminDashboardPreRegistration404(t *testing.T) {
	const key = "w71-admin-key"
	cfg := adminDashConfig(t, key)
	cfg.Store = &StoreSection{Enabled: true, Path: filepath.Join(t.TempDir(), "any2api.db")}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() { _ = s.CloseStore() })

	// ① 注册前：路由不存在 → 404（不是 401——路由根本没挂）。
	before := http.NewServeMux()
	srvBefore := httptest.NewServer(before)
	defer srvBefore.Close()
	for _, path := range adminDashEndpoints {
		resp := adminAPIReq(t, http.MethodGet, srvBefore.URL+path, key, "")
		if resp.status != http.StatusNotFound {
			t.Fatalf("注册前 GET %s = %d（want 404）body=%s", path, resp.status, resp.body)
		}
	}

	// ② 注册后：同一 Server + 同一 key → 200 / 404(未知 id，但已过鉴权)。
	after := http.NewServeMux()
	s.registerAdminDashboard(after)
	srvAfter := httptest.NewServer(after)
	defer srvAfter.Close()
	for _, path := range adminDashEndpoints {
		resp := adminAPIReq(t, http.MethodGet, srvAfter.URL+path, key, "")
		if resp.status != http.StatusOK && resp.status != http.StatusNotFound {
			t.Fatalf("注册后 GET %s = %d（want 200/404）body=%s", path, resp.status, resp.body)
		}
		if resp.status == http.StatusNotFound && !strings.Contains(resp.body, "not_found") {
			t.Fatalf("注册后 GET %s 的 404 应为「未知 id」而非「路由不存在」：%s", path, resp.body)
		}
	}
}

// ---------------------------------------------------------------------------
// 2. 鉴权：未配 key = 404；错 key = 401；对 key = 200；方法错 = 405
// ---------------------------------------------------------------------------

func TestAdminDashboardNoKey404(t *testing.T) {
	_, srv := adminDashStart(t, adminDashConfig(t, "")) // 无 AdminKey
	for _, path := range adminDashEndpoints {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+path, "any-key", "")
		if resp.status != http.StatusNotFound {
			t.Fatalf("GET %s = %d（want 404：未配密钥 = 路由不注册）body=%s", path, resp.status, resp.body)
		}
	}
}

func TestAdminDashboardAuthGating(t *testing.T) {
	const key = "w71-admin-key"
	cfg := adminDashConfig(t, key)
	cfg.Store = &StoreSection{Enabled: true, Path: filepath.Join(t.TempDir(), "any2api.db")}
	_, srv := adminDashStart(t, cfg)

	for _, path := range adminDashEndpoints {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+path, "wrong-key", "")
		if resp.status != http.StatusUnauthorized {
			t.Fatalf("错 key GET %s = %d（want 401）body=%s", path, resp.status, resp.body)
		}
		if !strings.Contains(resp.body, `"authentication_error"`) {
			t.Fatalf("401 body: %s", resp.body)
		}
		if resp.header.Get("Cache-Control") != "no-store" {
			t.Fatalf("Cache-Control = %q（want no-store）", resp.header.Get("Cache-Control"))
		}
	}

	// 对 key：三个列表/告警面 200；详情面未知 id = 404（**不是** 401）。
	for _, path := range []string{"/admin/audit", "/admin/requests", "/admin/alerts"} {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+path, key, "")
		if resp.status != http.StatusOK {
			t.Fatalf("对 key GET %s = %d（want 200）body=%s", path, resp.status, resp.body)
		}
	}
	resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/requests/req-unknown", key, "")
	if resp.status != http.StatusNotFound || !strings.Contains(resp.body, "not_found") {
		t.Fatalf("未知 id = %d body=%s（want 404 not_found）", resp.status, resp.body)
	}

	// 方法错：路由存在但方法不匹配 → 405 + Allow。
	resp = adminAPIReq(t, http.MethodPost, srv.URL+"/admin/requests", key, "{}")
	if resp.status != http.StatusMethodNotAllowed {
		t.Fatalf("POST /admin/requests = %d（want 405）body=%s", resp.status, resp.body)
	}
	if !strings.Contains(resp.header.Get("Allow"), http.MethodGet) {
		t.Fatalf("Allow = %q", resp.header.Get("Allow"))
	}
}

// ---------------------------------------------------------------------------
// 3. /admin/audit：行 / 过滤 / 边界 / 503
// ---------------------------------------------------------------------------

func TestAdminDashboardAudit(t *testing.T) {
	const key = "w71-admin-key"
	cfg := adminDashConfig(t, key)
	cfg.Store = &StoreSection{Enabled: true, Path: filepath.Join(t.TempDir(), "any2api.db")}
	s, srv := adminDashStart(t, cfg)

	base := time.Now().Add(-time.Hour).UTC().Truncate(time.Millisecond)
	seed := []storeAudit{
		{TS: base, SID: "a1b2c3d4e5f6a7b8", KeyID: "3106c924", Kind: "request", RequestID: "req-1", Detail: `{"entry":"openai_chat","attempts":1}`},
		{TS: base.Add(30 * time.Minute), SID: "a1b2c3d4e5f6a7b8", Kind: storeAuditKindSessionRevoke, RequestID: "", Detail: `{"op":"revoke","revoked":1}`},
	}
	for _, row := range seed {
		if _, err := s.store.AuditAppend(row); err != nil {
			t.Fatalf("AuditAppend: %v", err)
		}
	}

	var all adminAuditResponse
	adminDashGet(t, srv.URL+"/admin/audit", key, &all)
	if len(all.Rows) != 2 || all.NextCursor != "" {
		t.Fatalf("audit 全量 = %+v（want 2 行 + 空游标）", all)
	}
	if all.Rows[0].Kind != "request" || all.Rows[0].TS != base.UnixMilli() {
		t.Fatalf("rows[0] = %+v（want kind=request ts=%d）", all.Rows[0], base.UnixMilli())
	}
	// sid 只回 8 字符前缀（绝不回完整 sid）。
	if all.Rows[0].SIDPrefix != "a1b2c3d4" {
		t.Fatalf("sid_prefix = %q（want a1b2c3d4）", all.Rows[0].SIDPrefix)
	}
	if strings.Contains(all.Rows[0].SIDPrefix, "e5f6a7b8") {
		t.Fatalf("sid 前缀泄漏完整 sid：%q", all.Rows[0].SIDPrefix)
	}

	var byKind adminAuditResponse
	adminDashGet(t, srv.URL+"/admin/audit?kind="+storeAuditKindSessionRevoke, key, &byKind)
	if len(byKind.Rows) != 1 || byKind.Rows[0].Kind != storeAuditKindSessionRevoke {
		t.Fatalf("kind 过滤 = %+v", byKind)
	}

	// since：unix ms 与 RFC3339 两种形态等价（都取到第二行）。
	for _, since := range []string{
		base.Add(29 * time.Minute).Format(time.RFC3339Nano),
		strings.TrimSpace(strings.TrimSuffix(time.Time(base.Add(29*time.Minute)).UTC().Format("2006-01-02T15:04:05.999Z07:00"), "Z")) + "Z",
	} {
		var out adminAuditResponse
		adminDashGet(t, srv.URL+"/admin/audit?since="+since, key, &out)
		if len(out.Rows) != 1 {
			t.Fatalf("since=%q → %d 行（want 1）", since, len(out.Rows))
		}
	}
	var ms adminAuditResponse
	adminDashGet(t, srv.URL+"/admin/audit?since="+itoa(base.Add(29*time.Minute).UnixMilli()), key, &ms)
	if len(ms.Rows) != 1 || ms.Rows[0].Kind != storeAuditKindSessionRevoke {
		t.Fatalf("since(unix ms) = %+v（want 仅 revoke 行）", ms)
	}

	// limit：钳到 max（不报错）；坏 limit / 坏 since = 400。
	var limited adminAuditResponse
	adminDashGet(t, srv.URL+"/admin/audit?limit=9999", key, &limited)
	if len(limited.Rows) != 2 {
		t.Fatalf("limit=9999 → %d 行（want 2；>max 钳到 max=%d）", len(limited.Rows), adminAuditMaxLimit)
	}
	for _, bad := range []string{"?limit=0", "?limit=abc", "?since=abc", "?since=-1"} {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/audit"+bad, key, "")
		if resp.status != http.StatusBadRequest {
			t.Fatalf("GET /admin/audit%s = %d（want 400）body=%s", bad, resp.status, resp.body)
		}
	}
}

func TestAdminDashboardAuditStoreDisabled503(t *testing.T) {
	const key = "w71-admin-key"
	_, srv := adminDashStart(t, adminDashConfig(t, key)) // 无 store 段（内存档）
	resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/audit", key, "")
	if resp.status != http.StatusServiceUnavailable || !strings.Contains(resp.body, "store_unavailable") {
		t.Fatalf("store 未启用 GET /admin/audit = %d body=%s（want 503 store_unavailable）", resp.status, resp.body)
	}
}

// ---------------------------------------------------------------------------
// 4. /admin/requests：ring 行 / 筛选 / 分页 / 边界 / 有界
// ---------------------------------------------------------------------------

func TestAdminDashboardRequestsFiltersAndPaging(t *testing.T) {
	const key = "w71-admin-key"
	s, srv := adminDashStart(t, adminDashConfig(t, key))

	now := time.Now()
	okAttempt := []adminDashAttemptSeed{{upstream: "fake", status: 200, ttft: 120 * time.Millisecond, total: 300 * time.Millisecond, delivered: true, endReason: endReasonCompleted, in: 10, out: 20, credIndex: -1}}
	adminDashSeedRow(s, "req-1", now.Add(-3*time.Second), "openai_chat", "claude-sonnet-4-5", false, okAttempt)
	adminDashSeedRow(s, "req-2", now.Add(-2*time.Second), "claude_messages", "claude-sonnet-4-5", true, []adminDashAttemptSeed{
		{upstream: "fake", status: 503, failed: true, endReason: endReasonUpstreamError, total: 200 * time.Millisecond, credIndex: -1},
		{upstream: "web", status: 200, ttft: 90 * time.Millisecond, total: 400 * time.Millisecond, delivered: true, endReason: endReasonCompleted, in: 11, out: 22, credIndex: 2},
	})
	adminDashSeedRow(s, "req-3", now.Add(-1*time.Second), "openai_chat", "gpt-5", true, []adminDashAttemptSeed{
		{upstream: "web", status: 503, disconnected: true, total: 100 * time.Millisecond, delivered: false, endReason: endReasonDownstreamDisconnect, credIndex: 0},
	})

	// 缺省：新→旧全量 + 空游标。
	var page adminRequestsResponse
	adminDashGet(t, srv.URL+"/admin/requests", key, &page)
	if len(page.Rows) != 3 || page.NextCursor != "" {
		t.Fatalf("缺省列表 = %d 行 游标=%q（want 3 + 空）", len(page.Rows), page.NextCursor)
	}
	if page.Rows[0].RequestID != "req-3" || page.Rows[2].RequestID != "req-1" {
		t.Fatalf("顺序 = %s,%s,%s（want 新→旧 req-3,req-2,req-1）",
			page.Rows[0].RequestID, page.Rows[1].RequestID, page.Rows[2].RequestID)
	}
	// 18 列的关键派生值：尝试/换台/末上游/池索引/延迟/用量/结局。
	second := page.Rows[1]
	if second.Attempts != 2 || second.Failovers != 1 || second.Upstream != "web" ||
		second.CredentialIndex == nil || *second.CredentialIndex != 2 ||
		second.Status != 200 || second.TTFTMs != 90 || second.In != 11 || second.Out != 22 ||
		second.Outcome != endReasonCompleted || second.SIDPrefix != "a1b2c3d4" || second.KeyID != "3106c924" {
		t.Fatalf("req-2 行 = %+v", second)
	}
	third := page.Rows[2]
	if third.Failovers != 0 || third.Failed || third.Delivered != true {
		t.Fatalf("req-1 行 = %+v（want 0 换台/未失败/已交付）", third)
	}
	if page.Rows[0].Failed != true || page.Rows[0].Delivered != false {
		t.Fatalf("req-3 行 = %+v（want 断开 → failed）", page.Rows[0])
	}

	// 筛选：显式参数 + filter= 两种写法逐一同判。
	filters := []string{
		"entry=claude_messages",
		"model=gpt-5",
		"upstream=web",
		"status=503",
		"failed=true",
		"stream=true",
		"request_id=req-",
		"filter=entry=openai_chat",
		"filter=model=gpt-5,stream=true",
	}
	want := []int{1, 1, 2, 1, 1, 2, 3, 2, 1}
	for i, f := range filters {
		var out adminRequestsResponse
		adminDashGet(t, srv.URL+"/admin/requests?"+f, key, &out)
		if len(out.Rows) != want[i] {
			t.Fatalf("筛选 %q → %d 行（want %d）", f, len(out.Rows), want[i])
		}
	}
	var failed adminRequestsResponse
	adminDashGet(t, srv.URL+"/admin/requests?failed=false", key, &failed)
	if len(failed.Rows) != 2 {
		t.Fatalf("failed=false → %d 行（want 2）", len(failed.Rows))
	}
	// status 口径 = 请求级（末次尝试的上游状态）——换台后成功 = 200。
	var okStatus adminRequestsResponse
	adminDashGet(t, srv.URL+"/admin/requests?status=200", key, &okStatus)
	if len(okStatus.Rows) != 2 {
		t.Fatalf("status=200 → %d 行（want 2：req-1/req-2）", len(okStatus.Rows))
	}
	var since adminRequestsResponse
	adminDashGet(t, srv.URL+"/admin/requests?since="+itoa(now.Add(-2*time.Second).UnixMilli()), key, &since)
	if len(since.Rows) != 2 {
		t.Fatalf("since 窗 → %d 行（want 2）", len(since.Rows))
	}

	// 分页：limit=1 → 逐页新→旧，末页游标空。
	seen := []string{}
	cursor := ""
	for i := range 4 {
		url := srv.URL + "/admin/requests?limit=1"
		if cursor != "" {
			url += "&cursor=" + cursor
		}
		var out adminRequestsResponse
		adminDashGet(t, url, key, &out)
		if len(out.Rows) != 1 {
			t.Fatalf("第 %d 页 = %d 行（want 1）", i+1, len(out.Rows))
		}
		seen = append(seen, out.Rows[0].RequestID)
		cursor = out.NextCursor
		if cursor == "" {
			break
		}
	}
	if strings.Join(seen, ",") != "req-3,req-2,req-1" {
		t.Fatalf("逐页 = %v（want req-3,req-2,req-1）", seen)
	}

	// 边界：坏 limit / 未知 filter 键 / 坏游标 = 400；limit 超上限 = 钳制不报错。
	for _, bad := range []string{"?limit=0", "?limit=x", "?cursor=%25%25", "?filter=nope=1", "?failed=maybe"} {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/requests"+bad, key, "")
		if resp.status != http.StatusBadRequest {
			t.Fatalf("GET /admin/requests%s = %d（want 400）body=%s", bad, resp.status, resp.body)
		}
	}
	var capped adminRequestsResponse
	adminDashGet(t, srv.URL+"/admin/requests?limit=9999", key, &capped)
	if len(capped.Rows) != 3 {
		t.Fatalf("limit=9999 → %d 行（want 3；>max 钳到 %d）", len(capped.Rows), adminRequestsMaxLimit)
	}
}

func TestAdminDashboardRequestRingBounded(t *testing.T) {
	const key = "w71-admin-key"
	s, srv := adminDashStart(t, adminDashConfig(t, key))
	base := time.Now()
	for i := range adminRequestRingCapacity + 5 {
		adminDashSeedRow(s, "req-"+itoa(int64(i)), base.Add(time.Duration(i)*time.Millisecond), "openai_chat", "m", false,
			[]adminDashAttemptSeed{{upstream: "fake", status: 200, total: time.Millisecond, delivered: true, endReason: endReasonCompleted, credIndex: -1}})
	}
	if got := s.adminRequests.size(); got != adminRequestRingCapacity {
		t.Fatalf("ring size = %d（want %d——有界覆盖写最旧）", got, adminRequestRingCapacity)
	}
	var head adminRequestsResponse
	adminDashGet(t, srv.URL+"/admin/requests?limit=1", key, &head)
	if len(head.Rows) != 1 || head.Rows[0].RequestID != "req-"+itoa(int64(adminRequestRingCapacity+4)) {
		t.Fatalf("ring 头 = %+v（want 最新一条）", head.Rows)
	}
	// 被覆盖的最旧 5 条不可查（404）。
	for _, gone := range []string{"req-0", "req-4"} {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/requests/"+gone, key, "")
		if resp.status != http.StatusNotFound {
			t.Fatalf("被覆盖的 %s = %d（want 404）body=%s", gone, resp.status, resp.body)
		}
	}
}

// ---------------------------------------------------------------------------
// 5. /admin/requests/{id}：详情（错误摘要只有枚举）
// ---------------------------------------------------------------------------

func TestAdminDashboardRequestDetail(t *testing.T) {
	const key = "w71-admin-key"
	s, srv := adminDashStart(t, adminDashConfig(t, key))
	adminDashSeedRow(s, "req-detail", time.Now(), "openai_chat", "claude-sonnet-4-5", true, []adminDashAttemptSeed{
		{upstream: "fake", status: 502, failed: true, endReason: endReasonUpstreamError, total: 150 * time.Millisecond, credIndex: -1,
			// 上游原文（若被记录会以 SECRET 形态出现在错误面上——负控见下个用例）。
			in: 0, out: 0},
		{upstream: "web", status: 200, ttft: 80 * time.Millisecond, total: 250 * time.Millisecond, delivered: true, endReason: endReasonCompleted, in: 7, out: 9, credIndex: 1},
	})

	var out adminRequestDetailResponse
	body := adminDashGet(t, srv.URL+"/admin/requests/req-detail", key, &out)
	if out.Request.RequestID != "req-detail" || out.Request.Attempts != 2 {
		t.Fatalf("详情请求面 = %+v", out.Request)
	}
	if len(out.Attempts) != 2 {
		t.Fatalf("attempt 时间线 = %d 条（want 2）", len(out.Attempts))
	}
	first := out.Attempts[0]
	if first.Error == nil || first.Error.Type != endReasonUpstreamError || first.Error.Class != "upstream" {
		t.Fatalf("attempt[0].error = %+v（want type=upstream_error class=upstream）", first.Error)
	}
	if first.Status != 502 || first.Failed != true {
		t.Fatalf("attempt[0] = %+v", first)
	}
	second := out.Attempts[1]
	if second.Error != nil {
		t.Fatalf("成功尝试不该有 error 摘要：%+v", second.Error)
	}
	if second.CredentialIndex == nil || *second.CredentialIndex != 1 || second.In != 7 || second.Out != 9 {
		t.Fatalf("attempt[1] = %+v", second)
	}
	// 错误摘要只允许 type/class 两键（绝不回上游原文/头面/URL）。
	if strings.Contains(body, "http") || strings.Contains(body, "raw") || strings.Contains(body, "body") {
		t.Fatalf("详情面出现可疑原文面：%s", body)
	}

	resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/requests/req-missing", key, "")
	if resp.status != http.StatusNotFound || !strings.Contains(resp.body, `"not_found"`) {
		t.Fatalf("未知 id = %d body=%s", resp.status, resp.body)
	}
}

// ---------------------------------------------------------------------------
// 6. ring 写入 hook（真实收尾路径）
// ---------------------------------------------------------------------------

func TestAdminDashboardRingHookPopulatesRow(t *testing.T) {
	s, _ := adminDashStart(t, adminDashConfig(t, "w71-admin-key"))
	rec := adminDashRecorder("req-hook", registry.FormatOpenAIChat, "claude-sonnet-4-5", "task-1", true, []adminDashAttemptSeed{
		{upstream: "up-a", status: 500, failed: true, endReason: endReasonUpstreamError, total: 10 * time.Millisecond, credIndex: -1},
		{upstream: "up-a", status: 500, failed: true, endReason: endReasonUpstreamError, total: 10 * time.Millisecond, credIndex: -1}, // 同台重试：不计换台
		{upstream: "up-b", status: 200, ttft: 50 * time.Millisecond, total: 90 * time.Millisecond, delivered: true, endReason: endReasonCompleted, in: 3, out: 4, credIndex: 0},
	})
	s.recordRequestRing(rec)

	row, ok := s.adminRequests.find("req-hook")
	if !ok {
		t.Fatalf("hook 未把请求写入 ring")
	}
	if row.Attempts != 3 || row.Failovers != 1 {
		t.Fatalf("attempts/failovers = %d/%d（want 3/1——同台重试不计换台）", row.Attempts, row.Failovers)
	}
	if row.Upstreams[0] != "up-a" || row.Upstreams[2] != "up-b" || row.Task != "task-1" || !row.Stream {
		t.Fatalf("行 = %+v", row)
	}
	if row.Status != 200 || row.In != 3 || row.Out != 4 || row.Outcome != endReasonCompleted || !row.Delivered || row.CredIndex != 0 {
		t.Fatalf("末次尝试面 = %+v", row)
	}
	if row.SIDPrefix != "a1b2c3d4" || row.KeyID != "3106c924" {
		t.Fatalf("会话面 = %q/%q（want 前缀 + sha8）", row.SIDPrefix, row.KeyID)
	}
	// nil 记账器 / nil Server = no-op（不 panic）。
	s.recordRequestRing(nil)
	var nilServer *Server
	nilServer.recordRequestRing(rec)
}

// ---------------------------------------------------------------------------
// 6b. 端到端：真 serve 收尾 → ring（hook 接线证明，非仅有方法）
// ---------------------------------------------------------------------------

func TestAdminDashboardRingHookEndToEnd(t *testing.T) {
	const key = "w71-admin-e2e-key"
	good := goodClaudeUpstream(t)
	cfg := &Config{
		Listen:           "127.0.0.1:0",
		HeartbeatSeconds: 15,
		AdminKey:         key,
		Upstreams:        []Upstream{anthUp("good", good.srv.URL)},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	s := New(cfg)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	if code, body := postChatStream(t, srv.URL); code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("下游请求 = %d body=%s", code, body)
	}
	var out adminRequestsResponse
	adminDashGet(t, srv.URL+"/admin/requests", key, &out)
	if len(out.Rows) != 1 {
		t.Fatalf("ring = %d 行（want 1——真实收尾路径写入）", len(out.Rows))
	}
	row := out.Rows[0]
	if row.Entry != "openai_chat" || row.Attempts != 1 || row.Failovers != 0 ||
		row.Upstream != "good" || !row.Delivered ||
		row.Outcome != endReasonCompleted || !row.Stream || row.RequestID == "" {
		t.Fatalf("e2e 行 = %+v", row)
	}
	// status/ttft 口径 = attemptRecord（W84-1 F1 修复后：流式成功面落上游 200；
	// 此前该面不写响应头状态，恒 0——见 W84-1 报告）。
	if row.Status != http.StatusOK || row.TotalMs < 0 || row.SIDPrefix != "" || row.KeyID != "" {
		t.Fatalf("延迟/状态/会话面 = %+v（无会话档：sid/key 应省略）", row)
	}
	// 详情面按同一 id 可查（ring 与响应 request-id 同源）。
	body := adminDashGet(t, srv.URL+"/admin/requests/"+row.RequestID, key, nil)
	if !strings.Contains(body, `"attempts":[{"n":1`) {
		t.Fatalf("详情面 = %s", body)
	}
	if s.adminRequests.size() != 1 {
		t.Fatalf("ring size = %d（want 1）", s.adminRequests.size())
	}
}

// ---------------------------------------------------------------------------
// 7. 告警：空态 / ring 派生 / 状态派生 / history
// ---------------------------------------------------------------------------

func TestAdminDashboardAlertsEmpty(t *testing.T) {
	const key = "w71-admin-key"
	_, srv := adminDashStart(t, adminDashConfig(t, key))
	body := adminDashGet(t, srv.URL+"/admin/alerts", key, nil)
	if !strings.Contains(body, `"active":[]`) || !strings.Contains(body, `"history":[]`) {
		t.Fatalf("空态告警 = %s（want 两个空数组——绝不 null）", body)
	}
	var out adminAlertsResponse
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Active) != 0 || len(out.History) != 0 {
		t.Fatalf("空态 = %+v", out)
	}
}

func TestAdminDashboardAlertsRingDerived(t *testing.T) {
	const key = "w71-admin-key"
	s, srv := adminDashStart(t, adminDashConfig(t, key))

	s.adminLog.add(adminLogStoreError, "", "req-x", "store_error: usage add: database is locked", 0)
	s.adminLog.add(adminLogQuotaExhausted, "web", "req-y", "daily quota exhausted", 429)
	s.adminLog.add(adminLogCapacityExhausted, "web", "req-z", "upstream_credentials_exhausted", 429)
	s.adminLog.add(adminLogCredentialCooldown, "web", "", "credential #1 cooling for 30m (class=credential)", 0)
	s.adminLog.add(adminLogPrecommitError, "fake", "req-w", "upstream status 503; error envelope written to downstream", 503)

	var out adminAlertsResponse
	adminDashGet(t, srv.URL+"/admin/alerts", key, &out)
	byName := map[string]adminAlertView{}
	for _, a := range out.Active {
		byName[a.Name] = a
	}
	for name, severity := range map[string]string{
		adminAlertStoreError:  adminAlertSeverityError,
		adminAlertQuota:       adminAlertSeverityWarning,
		adminAlertPoolDead:    adminAlertSeverityError,
		adminAlertKeyMismatch: adminAlertSeverityError,
		adminAlert5xx:         adminAlertSeverityError,
	} {
		a, ok := byName[name]
		if !ok {
			t.Fatalf("ring 派生告警缺 %s（active=%+v）", name, out.Active)
		}
		if a.Severity != severity || a.Since == 0 || a.Runbook == "" || a.ID == "" {
			t.Fatalf("告警 %s 形状 = %+v", name, a)
		}
	}
	if byName[adminAlertKeyMismatch].Object != "web" || byName[adminAlert5xx].Object != "fake" ||
		byName[adminAlertStoreError].Object != "service" {
		t.Fatalf("对象面 = %+v", out.Active)
	}
	// 非告警类事件（普通 failover）不进告警面。
	s.adminLog.add(adminLogFailover, "web", "req-f", "precommit failure, trying next upstream", 0)
	var out2 adminAlertsResponse
	adminDashGet(t, srv.URL+"/admin/alerts", key, &out2)
	for _, a := range out2.Active {
		if a.Name == adminLogFailover {
			t.Fatalf("普通 failover 事件不该成为告警：%+v", a)
		}
	}

	// 窗口外 = history（active 不再含它）。
	s.adminLog.add(adminLogStoreError, "", "req-old", "store_error: old", 0)
	adminDashAgeRingEntry(t, s, time.Now().Add(-2*adminAlertsActiveWindow))
	var out3 adminAlertsResponse
	adminDashGet(t, srv.URL+"/admin/alerts", key, &out3)
	if len(out3.History) != 1 || out3.History[0].Name != adminAlertStoreError {
		t.Fatalf("history = %+v（want 1 条 store_error）", out3.History)
	}
}

func TestAdminDashboardAlertsStateDerived(t *testing.T) {
	const key = "w71-admin-key"
	cfg := adminDashConfig(t, key)
	s, srv := adminDashStart(t, cfg)

	// ① 池全冷却 → pool_dead（对象 = 上游名）。
	pool := cfg.credentialPoolAt(1)
	pool.mu.Lock()
	for i := range pool.coolUntil {
		pool.coolUntil[i] = time.Now().Add(10 * time.Minute)
	}
	pool.mu.Unlock()

	var out adminAlertsResponse
	adminDashGet(t, srv.URL+"/admin/alerts", key, &out)
	found := false
	for _, a := range out.Active {
		if a.Name == adminAlertPoolDead && a.Object == "web" {
			found = true
		}
		if a.Name == adminAlertPoolLow && a.Object == "web" {
			t.Fatalf("pool_dead 已触发时不该同时报 pool_low（阶梯去重）：%+v", a)
		}
	}
	if !found {
		t.Fatalf("池全冷却未报 pool_dead：%+v", out.Active)
	}

	// ② 放开一个凭据 → pool_low（可用数 < 2）。
	pool.mu.Lock()
	pool.coolUntil[0] = time.Time{}
	pool.mu.Unlock()
	var out2 adminAlertsResponse
	adminDashGet(t, srv.URL+"/admin/alerts", key, &out2)
	foundLow := false
	for _, a := range out2.Active {
		if a.Name == adminAlertPoolLow && a.Object == "web" {
			foundLow = true
		}
	}
	if !foundLow {
		t.Fatalf("可用 1/3 未报 pool_low：%+v", out2.Active)
	}

	// ③ 管理面封禁 → blocked。
	now := time.Now()
	for range adminAuthMaxFailures {
		s.adminAuthState.fail("203.0.113.9", now)
	}
	var out3 adminAlertsResponse
	adminDashGet(t, srv.URL+"/admin/alerts", key, &out3)
	foundBlocked := false
	for _, a := range out3.Active {
		if a.Name == adminAlertBlocked && a.Object == "203.0.113.9" {
			foundBlocked = true
		}
	}
	if !foundBlocked {
		t.Fatalf("封禁未报 blocked：%+v", out3.Active)
	}

	// ④ 观测面停摆 → status_stale（计数非零 + 两个 ring 全空）；有 ring 行则消失。
	if s.adminRequests.size() != 0 {
		t.Fatalf("前置：请求 ring 应为空（实际 %d）", s.adminRequests.size())
	}
	s.diag.requests.Add(1)
	var out4 adminAlertsResponse
	adminDashGet(t, srv.URL+"/admin/alerts", key, &out4)
	foundStale := false
	for _, a := range out4.Active {
		if a.Name == adminAlertStatusStale {
			foundStale = true
		}
	}
	if !foundStale {
		t.Fatalf("观测面停摆未报 status_stale：%+v", out4.Active)
	}
	adminDashSeedRow(s, "req-stale", time.Now(), "openai_chat", "m", false,
		[]adminDashAttemptSeed{{upstream: "fake", status: 200, delivered: true, endReason: endReasonCompleted, credIndex: -1}})
	var out5 adminAlertsResponse
	adminDashGet(t, srv.URL+"/admin/alerts", key, &out5)
	for _, a := range out5.Active {
		if a.Name == adminAlertStatusStale {
			t.Fatalf("观测面有记录仍报 status_stale：%+v", a)
		}
	}
}

// ---------------------------------------------------------------------------
// 8. 负控：种子带 SECRET 的样本 → 响应体绝不含 SECRET（去掉脱敏即红）
// ---------------------------------------------------------------------------

func TestAdminDashboardSecretRedactionNegativeControl(t *testing.T) {
	const key = "w71-admin-key"
	cfg := adminDashConfig(t, key)
	cfg.Store = &StoreSection{Enabled: true, Path: filepath.Join(t.TempDir(), "any2api.db")}
	s, srv := adminDashStart(t, cfg)

	// ① 请求面：模型/任务/上游名里塞明文凭据形态（写入路径 = 真实 hook）。
	s.recordRequestRing(adminDashRecorder("req-secret", registry.FormatOpenAIChat, "api_key=SECRET-model-value", "Bearer SECRET-task-token", true,
		[]adminDashAttemptSeed{{upstream: "sk-SECRET-UPSTREAM", status: 200, ttft: 5 * time.Millisecond, total: 9 * time.Millisecond, delivered: true, endReason: endReasonCompleted, credIndex: -1}}))
	// ② 审计面：detail 里塞明文凭据（键面 + 值面两种形态）。
	if _, err := s.store.AuditAppend(storeAudit{
		TS: time.Now(), Kind: "request", RequestID: "req-secret",
		Detail: `{"api_key":"SECRET-audit-key","note":"Bearer SECRET-audit-token","upstream":"sk-SECRET-audit-sk","cookie":"SECRET-cookie"}`,
	}); err != nil {
		t.Fatalf("AuditAppend: %v", err)
	}

	listBody := adminDashGet(t, srv.URL+"/admin/requests", key, nil)
	detailBody := adminDashGet(t, srv.URL+"/admin/requests/req-secret", key, nil)
	auditBody := adminDashGet(t, srv.URL+"/admin/audit", key, nil)
	for name, body := range map[string]string{
		"requests":  listBody,
		"detail":    detailBody,
		"audit":     auditBody,
		"alerts(空)": adminDashGet(t, srv.URL+"/admin/alerts", key, nil),
	} {
		if strings.Contains(body, "SECRET") {
			t.Fatalf("负控失败：%s 响应体出现 SECRET：%s", name, body)
		}
	}
	// 正控：行确实在响应里（脱敏不是「整段丢掉」）。
	if !strings.Contains(listBody, "req-secret") || !strings.Contains(detailBody, `"attempts"`) ||
		!strings.Contains(auditBody, `[redacted]`) {
		t.Fatalf("正控失败（行应存在且带 [redacted] 占位）：\n%s\n%s\n%s", listBody, detailBody, auditBody)
	}
	// 池面凭据字节（sk-secret-*）也不得出现。
	if strings.Contains(listBody, "sk-secret") || strings.Contains(detailBody, "sk-secret") {
		t.Fatalf("请求面出现池凭据字节：%s", listBody)
	}
}

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

// itoa 十进制（测试内联用；避免 strconv 噪音）。
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
