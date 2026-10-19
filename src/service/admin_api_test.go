// admin_api_test.go —— P1-5 管理面（/admin/pool|sessions|usage|revoke）测试
// （board A2A-L19-W66-2）。
//
// 装置：真 Server + SQLite（t.TempDir()）+ httptest；管理面 mux 由
// `registerAdminAPI` 装配（与 Handler 的 admin_key 分支同一入口——测试因此
// 覆盖「未配 key = 404 / 错 key = 401 / 对 key = 200」三态）。
//
// 反证锚点（回退即红）：
//   - registerAdminAPI 去掉 admin_key 自门控 → TestAdminAPINoKey404 红；
//   - 响应回显凭据字节 / 完整 sid → TestAdminPoolShapeNoCredentialBytes /
//     TestAdminSessionsListPagingAndUsage / TestAdminUsageScopesTotalsAndValidation 红；
//   - 游标不分页 / 排序错 → TestAdminSessionsListPagingAndUsage 红；
//   - revoke 不删行 / 前缀歧义不 409 → TestAdminSessionsRevokeSelectors 红。
package service

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// adminAPIConfig 管理面测试配置：public 开 + store=sqlite + admin key。
func adminAPIConfig(t *testing.T, key string) *Config {
	t.Helper()
	cfg := uiTestConfig()
	cfg.HeartbeatSeconds = 15
	cfg.Public = &PublicSection{Enabled: true}
	cfg.Store = &StoreSection{Enabled: true, Path: filepath.Join(t.TempDir(), "any2api.db")}
	cfg.AdminKey = key
	return cfg
}

// adminAPIStart 启动被测服务；mux = registerAdminAPI（管理面注册面本体）。
func adminAPIStart(t *testing.T, cfg *Config) (*Server, *httptest.Server) {
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
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.Close()
		_ = s.CloseStore()
	})
	return s, srv
}

// adminAPIResp 一发管理面响应。
type adminAPIResp struct {
	status int
	body   string
	header http.Header
}

// adminAPIReq 发管理面请求（key 空 = 不带鉴权头）。
func adminAPIReq(t *testing.T, method, url, key, body string) adminAPIResp {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return adminAPIResp{status: resp.StatusCode, body: string(raw), header: resp.Header}
}

// adminAPIGetJSON GET + 解码（非 200 即失败）。
func adminAPIGetJSON(t *testing.T, url, key string, dst any) {
	t.Helper()
	resp := adminAPIReq(t, http.MethodGet, url, key, "")
	if resp.status != http.StatusOK {
		t.Fatalf("GET %s = %d body=%s", url, resp.status, resp.body)
	}
	if err := json.Unmarshal([]byte(resp.body), dst); err != nil {
		t.Fatalf("decode %s: %v body=%s", url, err, resp.body)
	}
}

// adminSeedSession 直接落一行会话（管理面只读 store）。
func adminSeedSession(t *testing.T, s *Server, sid, keyID string, created, lastSeen time.Time) {
	t.Helper()
	if err := s.store.SessionCreate(storeSession{
		SID:          sid,
		KeyID:        keyID,
		CreatedAt:    created,
		LastSeenAt:   lastSeen,
		ExpiresAt:    lastSeen.Add(sessionIdleTTL),
		AbsExpiresAt: created.Add(sessionAbsTTL),
	}); err != nil {
		t.Fatalf("seed session %s: %v", sid, err)
	}
}

// adminAPIEndpoints 四个管理面端点（无 key 时的 404 断言集）。
var adminAPIEndpoints = []struct{ method, path string }{
	{http.MethodGet, "/admin/pool"},
	{http.MethodGet, "/admin/sessions"},
	{http.MethodGet, "/admin/usage"},
	{http.MethodPost, "/admin/sessions/revoke"},
}

// ---------------------------------------------------------------------------
// 1. 惰性注册：未配 admin_key = 四条路由不存在（404 非 401）
// ---------------------------------------------------------------------------

func TestAdminAPINoKey404(t *testing.T) {
	_, srv := adminAPIStart(t, adminAPIConfig(t, "")) // 无 AdminKey
	for _, e := range adminAPIEndpoints {
		resp := adminAPIReq(t, e.method, srv.URL+e.path, "any-key", "{}")
		if resp.status != http.StatusNotFound {
			t.Fatalf("%s %s = %d（want 404：未配密钥 = 路由不注册）body=%s",
				e.method, e.path, resp.status, resp.body)
		}
	}
}

// ---------------------------------------------------------------------------
// 2. 鉴权：无 key / 错 key = 401；X-Management-Key 放行；方法错 = 405
// ---------------------------------------------------------------------------

func TestAdminAPIAuthGating(t *testing.T) {
	const key = "w66-admin-key"
	_, srv := adminAPIStart(t, adminAPIConfig(t, key))

	resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/pool", "", "")
	if resp.status != http.StatusUnauthorized {
		t.Fatalf("no key = %d（want 401）body=%s", resp.status, resp.body)
	}
	if !strings.Contains(resp.header.Get("WWW-Authenticate"), "Bearer") {
		t.Fatalf("WWW-Authenticate = %q", resp.header.Get("WWW-Authenticate"))
	}
	if !strings.Contains(resp.body, `"authentication_error"`) {
		t.Fatalf("401 body: %s", resp.body)
	}
	if got := resp.header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q（want no-store）", got)
	}

	resp = adminAPIReq(t, http.MethodGet, srv.URL+"/admin/sessions", "wrong-key", "")
	if resp.status != http.StatusUnauthorized {
		t.Fatalf("wrong key = %d（want 401）body=%s", resp.status, resp.body)
	}

	// X-Management-Key 兜底 + 成功面。
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/admin/usage", nil)
	req.Header.Set("X-Management-Key", key)
	raw, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	raw.Body.Close()
	if raw.StatusCode != http.StatusOK {
		t.Fatalf("X-Management-Key = %d（want 200）", raw.StatusCode)
	}

	// 方法不匹配：标准库 405 + Allow（路由存在但方法错）。
	resp = adminAPIReq(t, http.MethodPost, srv.URL+"/admin/pool", key, "{}")
	if resp.status != http.StatusMethodNotAllowed {
		t.Fatalf("POST /admin/pool = %d（want 405）body=%s", resp.status, resp.body)
	}
	if got := resp.header.Get("Allow"); !strings.Contains(got, http.MethodGet) {
		t.Fatalf("Allow = %q", got)
	}
}

// ---------------------------------------------------------------------------
// 3. /admin/pool：形状、容量、冷却、零凭据字节
// ---------------------------------------------------------------------------

func TestAdminPoolShapeNoCredentialBytes(t *testing.T) {
	const key = "w66-admin-key"
	cfg := adminAPIConfig(t, key)
	cfg.Upstreams = append(cfg.Upstreams, Upstream{
		Name:        "web",
		Protocol:    ProtocolClaudeWeb,
		Credentials: []string{"sk-secret-a", "sk-secret-b", "sk-secret-c"},
	})
	_, srv := adminAPIStart(t, cfg)

	resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/pool", key, "")
	if resp.status != http.StatusOK {
		t.Fatalf("pool = %d body=%s", resp.status, resp.body)
	}
	if strings.Contains(resp.body, "sk-secret") || strings.Contains(resp.body, "test-key") {
		t.Fatalf("响应出现凭据字节: %s", resp.body)
	}
	var out adminPoolResponse
	if err := json.Unmarshal([]byte(resp.body), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Upstreams) != 2 {
		t.Fatalf("upstreams = %d（want 2）", len(out.Upstreams))
	}
	if out.Upstreams[0].PoolSize != 1 || len(out.Upstreams[0].Credentials) != 1 {
		t.Fatalf("upstream[0] = %+v（want 单凭据池）", out.Upstreams[0])
	}
	web := out.Upstreams[1]
	if web.PoolSize != 3 || len(web.Credentials) != 3 {
		t.Fatalf("upstream[1] = %+v（want 3 凭据）", web)
	}
	if web.Protocol != string(ProtocolClaudeWeb) {
		t.Fatalf("protocol = %q", web.Protocol)
	}
	for i, c := range web.Credentials {
		if c.Index != i || c.Cooling || c.CooldownUntil != 0 || c.CooldownRemainingSeconds != 0 {
			t.Fatalf("credential[%d] 初始态 = %+v", i, c)
		}
	}
	// capacity：public 开 → 缺省日预算 250；闸上限 > 0。
	if out.Capacity.DailyUnitsBudget != DefaultPublicDailyBudgetUnits {
		t.Fatalf("daily_units_budget = %d（want %d）", out.Capacity.DailyUnitsBudget, DefaultPublicDailyBudgetUnits)
	}
	if out.Capacity.UsedToday != 0 || out.Capacity.Remaining != DefaultPublicDailyBudgetUnits {
		t.Fatalf("capacity = %+v", out.Capacity)
	}
	if out.Capacity.MaxInflight <= 0 || out.Capacity.MaxInflight != DefaultPublicMaxInflight {
		t.Fatalf("max_inflight = %d（want %d）", out.Capacity.MaxInflight, DefaultPublicMaxInflight)
	}
	if out.Now <= 0 {
		t.Fatalf("now = %d", out.Now)
	}

	// 冷却 + busy + last_used：池状态经快照可见（只读，不泄凭据）。
	pool := cfg.credentialPoolAt(1)
	now := time.Now()
	pool.reportFailure(1, credFailureQuota, 0, now)
	if _, ok := pool.selectCredential(now); !ok {
		t.Fatalf("selectCredential 应选中一个可用凭据")
	}
	var second adminPoolResponse
	adminAPIGetJSON(t, srv.URL+"/admin/pool", key, &second)
	web = second.Upstreams[1]
	c1 := web.Credentials[1]
	if !c1.Cooling || c1.CooldownUntil <= 0 || c1.CooldownRemainingSeconds <= 0 {
		t.Fatalf("credential[1] 冷却态 = %+v", c1)
	}
	busy := 0
	used := 0
	for _, c := range web.Credentials {
		if c.Busy {
			busy++
		}
		if c.LastUsed > 0 {
			used++
		}
	}
	if busy != 1 || used != 1 {
		t.Fatalf("busy=%d used=%d（want 各 1；gate=public.enabled）credentials=%+v", busy, used, web.Credentials)
	}
}

// ---------------------------------------------------------------------------
// 4. /admin/sessions：空态（sessions=[] 非 null）
// ---------------------------------------------------------------------------

func TestAdminSessionsEmptyState(t *testing.T) {
	const key = "w66-admin-key"
	_, srv := adminAPIStart(t, adminAPIConfig(t, key))

	resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/sessions", key, "")
	if resp.status != http.StatusOK {
		t.Fatalf("sessions = %d body=%s", resp.status, resp.body)
	}
	if !strings.Contains(resp.body, `"sessions":[]`) {
		t.Fatalf("空态应为 sessions=[]（不是 null）: %s", resp.body)
	}
	var out adminSessionsResponse
	if err := json.Unmarshal([]byte(resp.body), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.NextCursor != "" || out.Now <= 0 || len(out.Sessions) != 0 {
		t.Fatalf("空态 = %+v", out)
	}
}

// ---------------------------------------------------------------------------
// 5. /admin/sessions：排序 + 游标翻页 + 当日用量 + 只回前缀
// ---------------------------------------------------------------------------

func TestAdminSessionsListPagingAndUsage(t *testing.T) {
	const key = "w66-admin-key"
	s, srv := adminAPIStart(t, adminAPIConfig(t, key))

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	sid1 := "aaaa1111aaaaaaaaaaaaaa"
	sid2 := "bbbb2222bbbbbbbbbbbbbb"
	sid3 := "cccc3333cccccccccccccc"
	adminSeedSession(t, s, sid1, "0123abcd", base, base.Add(time.Second))
	adminSeedSession(t, s, sid2, "0123abcd", base, base.Add(2*time.Second))
	adminSeedSession(t, s, sid3, "4567cdef", base, base.Add(3*time.Second))
	if err := s.store.UsageAdd("session:"+sid2, storeDay(time.Now()), 4, 9); err != nil {
		t.Fatalf("usage: %v", err)
	}

	var page1 adminSessionsResponse
	adminAPIGetJSON(t, srv.URL+"/admin/sessions?limit=2", key, &page1)
	if len(page1.Sessions) != 2 {
		t.Fatalf("page1 = %d（want 2）", len(page1.Sessions))
	}
	if page1.Sessions[0].SIDPrefix != sidPrefix(sid3) || page1.Sessions[1].SIDPrefix != sidPrefix(sid2) {
		t.Fatalf("排序 = %s,%s（want last_seen DESC）", page1.Sessions[0].SIDPrefix, page1.Sessions[1].SIDPrefix)
	}
	if page1.NextCursor == "" {
		t.Fatalf("page1.next_cursor 为空（还有下一页）")
	}
	// 只回 8 字符前缀，不回完整 sid。
	raw := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/sessions?limit=2", key, "").body
	for _, sid := range []string{sid1, sid2, sid3} {
		if strings.Contains(raw, sid) {
			t.Fatalf("响应出现完整 sid %q: %s", sid, raw)
		}
	}

	var page2 adminSessionsResponse
	adminAPIGetJSON(t, srv.URL+"/admin/sessions?limit=2&cursor="+page1.NextCursor, key, &page2)
	if len(page2.Sessions) != 1 || page2.Sessions[0].SIDPrefix != sidPrefix(sid1) {
		t.Fatalf("page2 = %+v", page2.Sessions)
	}
	if page2.NextCursor != "" {
		t.Fatalf("page2.next_cursor = %q（应为空）", page2.NextCursor)
	}
	if page2.Sessions[0].RequestsToday != 0 || page2.Sessions[0].CostUnitsToday != 0 {
		t.Fatalf("sid1 无台账行应为 0: %+v", page2.Sessions[0])
	}
	// sid2 的当日用量映射（第 1 页第 2 行）。
	if page1.Sessions[1].RequestsToday != 4 || page1.Sessions[1].CostUnitsToday != 9 {
		t.Fatalf("sid2 用量 = %+v", page1.Sessions[1])
	}
	// 时间戳为 unix ms。
	if page1.Sessions[0].CreatedAt != base.UnixMilli() || page1.Sessions[0].LastSeenAt != base.Add(3*time.Second).UnixMilli() {
		t.Fatalf("时间戳 = %+v", page1.Sessions[0])
	}

	// 坏游标 / 坏 limit → 400。
	for _, q := range []string{"?cursor=not-base64!!", "?cursor=" + "Zm9v", "?limit=0", "?limit=abc"} {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/sessions"+q, key, "")
		if resp.status != http.StatusBadRequest {
			t.Fatalf("sessions%s = %d（want 400）body=%s", q, resp.status, resp.body)
		}
	}
}

// ---------------------------------------------------------------------------
// 6. /admin/usage：scope 过滤 / totals / 脱敏 / 参数校验
// ---------------------------------------------------------------------------

func TestAdminUsageScopesTotalsAndValidation(t *testing.T) {
	const key = "w66-admin-key"
	s, srv := adminAPIStart(t, adminAPIConfig(t, key))
	day := storeDay(time.Now())

	seed := []struct {
		scope string
		req   int
		units int
	}{
		{storeUsageScopeGlobal, 2, 5},
		{"session:aaaa1111aaaaaaaaaaaaaa", 3, 7},
		{"session:bbbb2222bbbbbbbbbbbbbb", 1, 2},
		{"key:0123abcd", 4, 9},
		{"ip:deadbeefdeadbeef", 1, 1},
	}
	for _, row := range seed {
		if err := s.store.UsageAdd(row.scope, day, row.req, row.units); err != nil {
			t.Fatalf("usage %s: %v", row.scope, err)
		}
	}

	var sess adminUsageResponse
	adminAPIGetJSON(t, srv.URL+"/admin/usage?scope=session", key, &sess)
	if len(sess.Rows) != 2 {
		t.Fatalf("session rows = %d（want 2）body=%+v", len(sess.Rows), sess)
	}
	if sess.Totals.Requests != 4 || sess.Totals.CostUnits != 9 {
		t.Fatalf("session totals = %+v（want 4/9）", sess.Totals)
	}
	for _, row := range sess.Rows {
		if !strings.HasPrefix(row.Scope, "session:") || len(row.Scope) != len("session:")+8 {
			t.Fatalf("session scope 未脱敏: %q", row.Scope)
		}
	}
	raw := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/usage?scope=session", key, "").body
	if strings.Contains(raw, "aaaa1111aaaaaaaaaaaaaa") {
		t.Fatalf("usage 响应出现完整 sid: %s", raw)
	}
	// cost_units DESC：7 在前。
	if sess.Rows[0].CostUnits != 7 || sess.Rows[1].CostUnits != 2 {
		t.Fatalf("排序 = %+v", sess.Rows)
	}

	var keys adminUsageResponse
	adminAPIGetJSON(t, srv.URL+"/admin/usage?scope=key", key, &keys)
	if len(keys.Rows) != 1 || keys.Rows[0].Scope != "key:0123abcd" || keys.Rows[0].CostUnits != 9 {
		t.Fatalf("key rows = %+v", keys.Rows)
	}

	var global adminUsageResponse
	adminAPIGetJSON(t, srv.URL+"/admin/usage", key, &global)
	if global.Scope != "global" || len(global.Rows) != 1 || global.Rows[0].Requests != 2 || global.Rows[0].CostUnits != 5 {
		t.Fatalf("global = %+v", global)
	}

	var empty adminUsageResponse
	adminAPIGetJSON(t, srv.URL+"/admin/usage?day=2020-01-01", key, &empty)
	if len(empty.Rows) != 0 || empty.Totals.Requests != 0 || empty.Totals.CostUnits != 0 {
		t.Fatalf("空日 = %+v", empty)
	}

	// limit 截断 rows，但 totals 是过滤后全量。
	var limited adminUsageResponse
	adminAPIGetJSON(t, srv.URL+"/admin/usage?scope=session&limit=1", key, &limited)
	if len(limited.Rows) != 1 || limited.Totals.Requests != 4 || limited.Totals.CostUnits != 9 {
		t.Fatalf("limit=1 = %+v", limited)
	}

	for _, q := range []string{"?scope=bogus", "?day=2026-13-40", "?day=not-a-day", "?limit=0"} {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/usage"+q, key, "")
		if resp.status != http.StatusBadRequest {
			t.Fatalf("usage%s = %d（want 400）body=%s", q, resp.status, resp.body)
		}
	}
}

// ---------------------------------------------------------------------------
// 7. /admin/sessions/revoke：前缀歧义 409 / 唯一命中 / key_id / all / 审计
// ---------------------------------------------------------------------------

func TestAdminSessionsRevokeSelectors(t *testing.T) {
	const key = "w66-admin-key"
	s, srv := adminAPIStart(t, adminAPIConfig(t, key))

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	sidA := "abcdAAAA0000000000001"
	sidB := "abcdBBBB0000000000002"
	sidC := "wxyzCCCC0000000000003"
	sidD := "zzzzDDDD0000000000004"
	adminSeedSession(t, s, sidA, "11111111", base, base)
	adminSeedSession(t, s, sidB, "22222222", base, base)
	adminSeedSession(t, s, sidC, "11111111", base, base)
	adminSeedSession(t, s, sidD, "33333333", base, base)

	// 前缀歧义 → 409 + matched（错误信封同形）。
	resp := adminAPIReq(t, http.MethodPost, srv.URL+"/admin/sessions/revoke", key, `{"sid_prefix":"abcd"}`)
	if resp.status != http.StatusConflict || !strings.Contains(resp.body, `"matched":2`) ||
		!strings.Contains(resp.body, `"conflict"`) {
		t.Fatalf("ambiguous = %d body=%s", resp.status, resp.body)
	}

	// 唯一命中 → 200；行被删。
	resp = adminAPIReq(t, http.MethodPost, srv.URL+"/admin/sessions/revoke", key, `{"sid_prefix":"wxyz"}`)
	if resp.status != http.StatusOK {
		t.Fatalf("revoke wxyz = %d body=%s", resp.status, resp.body)
	}
	var rev adminRevokeResponse
	if err := json.Unmarshal([]byte(resp.body), &rev); err != nil || rev.Revoked != 1 || rev.Matched != 1 {
		t.Fatalf("revoke wxyz = %s", resp.body)
	}
	rows, err := s.store.Sessions()
	if err != nil || len(rows) != 3 {
		t.Fatalf("revoke 后 sessions = %d err=%v（want 3）", len(rows), err)
	}

	// 再吊销同一前缀 → 404。
	resp = adminAPIReq(t, http.MethodPost, srv.URL+"/admin/sessions/revoke", key, `{"sid_prefix":"wxyz"}`)
	if resp.status != http.StatusNotFound {
		t.Fatalf("repeat = %d（want 404）body=%s", resp.status, resp.body)
	}

	// 完整 sid 作选择器（22 字符）→ 200；审计只落 8 字符前缀。
	resp = adminAPIReq(t, http.MethodPost, srv.URL+"/admin/sessions/revoke", key, `{"sid_prefix":"`+sidA+`"}`)
	if resp.status != http.StatusOK || !strings.Contains(resp.body, `"revoked":1`) {
		t.Fatalf("revoke full sid = %d body=%s", resp.status, resp.body)
	}

	// key_id：命中该 key 的全部会话（此时只剩 sidB）。
	resp = adminAPIReq(t, http.MethodPost, srv.URL+"/admin/sessions/revoke", key, `{"key_id":"22222222"}`)
	if resp.status != http.StatusOK || !strings.Contains(resp.body, `"revoked":1`) {
		t.Fatalf("revoke key = %d body=%s", resp.status, resp.body)
	}

	// all：清剩余。
	resp = adminAPIReq(t, http.MethodPost, srv.URL+"/admin/sessions/revoke", key, `{"all":true}`)
	if resp.status != http.StatusOK || !strings.Contains(resp.body, `"revoked":1`) {
		t.Fatalf("revoke all = %d body=%s", resp.status, resp.body)
	}
	if rows, err = s.store.Sessions(); err != nil || len(rows) != 0 {
		t.Fatalf("revoke all 后 sessions = %d err=%v", len(rows), err)
	}

	// 参数校验：多选择器 / 空体 / 短前缀 / 坏 key_id。
	for _, body := range []string{
		`{"all":true,"key_id":"11111111"}`,
		`{}`,
		``,
		`{"sid_prefix":"abc"}`,
		`{"sid_prefix":"abc!"}`,
		`{"key_id":"ABCDEF12"}`,
	} {
		resp = adminAPIReq(t, http.MethodPost, srv.URL+"/admin/sessions/revoke", key, body)
		if resp.status != http.StatusBadRequest {
			t.Fatalf("revoke %q = %d（want 400）body=%s", body, resp.status, resp.body)
		}
	}

	// 审计：4 次成功吊销各一行；detail/SID 只含 8 字符前缀（即使选择器是完整
	// sid——凭据不进审计表）。
	audits, err := s.store.AuditQuery(storeAuditKindSessionRevoke, time.Time{}, 10)
	if err != nil {
		t.Fatalf("audit query: %v", err)
	}
	if len(audits) != 4 {
		t.Fatalf("audit rows = %d（want 4）", len(audits))
	}
	fullSidRow := false
	for _, a := range audits {
		if len(a.SID) > 8 {
			t.Fatalf("audit sid = %q（只准前缀）", a.SID)
		}
		for _, sid := range []string{sidA, sidB, sidC, sidD} {
			if strings.Contains(a.Detail, sid) {
				t.Fatalf("audit detail 出现完整 sid: %s", a.Detail)
			}
		}
		if strings.Contains(a.Detail, `"sid_prefix":"abcdAAAA"`) {
			fullSidRow = true
		}
	}
	if !fullSidRow {
		t.Fatalf("完整 sid 吊销的审计行未落 8 字符前缀: %+v", audits)
	}
}

// ---------------------------------------------------------------------------
// 8. store 未启用：/admin/sessions|usage|revoke = 503；/admin/pool 仍 200
// ---------------------------------------------------------------------------

func TestAdminStoreUnavailable503(t *testing.T) {
	const key = "w66-admin-key"
	cfg := uiTestConfig()
	cfg.HeartbeatSeconds = 15
	cfg.Public = &PublicSection{Enabled: true}
	cfg.AdminKey = key
	_, srv := adminAPIStart(t, cfg)

	for _, e := range []struct{ method, path, body string }{
		{http.MethodGet, "/admin/sessions", ""},
		{http.MethodGet, "/admin/usage", ""},
		{http.MethodPost, "/admin/sessions/revoke", `{"all":true}`},
	} {
		resp := adminAPIReq(t, e.method, srv.URL+e.path, key, e.body)
		if resp.status != http.StatusServiceUnavailable {
			t.Fatalf("%s %s = %d（want 503）body=%s", e.method, e.path, resp.status, resp.body)
		}
		if !strings.Contains(resp.body, "store_unavailable") {
			t.Fatalf("%s %s body=%s", e.method, e.path, resp.body)
		}
	}
	resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/pool", key, "")
	if resp.status != http.StatusOK {
		t.Fatalf("/admin/pool = %d（不依赖 store）body=%s", resp.status, resp.body)
	}
}

// ---------------------------------------------------------------------------
// 8. /admin/usage 的第五类 scope：`model`（W78-1 第 7 项；词表/匹配/展示三面）
// ---------------------------------------------------------------------------

func TestAdminUsageModelScopeDisplayAndValidation(t *testing.T) {
	const key = "w66-admin-key"
	s, srv := adminAPIStart(t, adminAPIConfig(t, key))
	day := storeDay(time.Now())

	// 种两行模型台账（模型名**故意长于 8 字符**——展示面不得截断）+ 一行会话行
	// （对照：session 仍只回 8 字符前缀）。
	for _, row := range []struct {
		scope string
		req   int
		units int
	}{
		{"model:claude-3-5-sonnet-latest", 2, 3},
		{"model:gpt-4o", 1, 1},
		{"session:aaaa1111aaaaaaaaaaaaaa", 5, 5},
	} {
		if err := s.store.UsageAdd(row.scope, day, row.req, row.units); err != nil {
			t.Fatalf("seed %s: %v", row.scope, err)
		}
	}

	var models adminUsageResponse
	adminAPIGetJSON(t, srv.URL+"/admin/usage?scope=model", key, &models)
	if models.Scope != "model" || len(models.Rows) != 2 {
		t.Fatalf("model rows = %+v", models)
	}
	if models.Rows[0].Scope != "model:claude-3-5-sonnet-latest" || models.Rows[0].Requests != 2 ||
		models.Rows[0].CostUnits != 3 {
		t.Fatalf("model 行未原样展示: %+v", models.Rows[0])
	}
	if models.Rows[1].Scope != "model:gpt-4o" {
		t.Fatalf("model 行 = %+v", models.Rows[1])
	}
	if models.Totals.Requests != 3 || models.Totals.CostUnits != 4 {
		t.Fatalf("model totals = %+v", models.Totals)
	}
	// 截断只影响 rows，不影响 totals；filter 不串类（模型行不落 session 面）。
	var limited adminUsageResponse
	adminAPIGetJSON(t, srv.URL+"/admin/usage?scope=model&limit=1", key, &limited)
	if len(limited.Rows) != 1 || limited.Totals.Requests != 3 {
		t.Fatalf("model limit=1 = %+v", limited)
	}
	var sessions adminUsageResponse
	adminAPIGetJSON(t, srv.URL+"/admin/usage?scope=session", key, &sessions)
	if len(sessions.Rows) != 1 || sessions.Rows[0].Scope != "session:aaaa1111" {
		t.Fatalf("session 面混入模型行: %+v", sessions.Rows)
	}
	// 词表：未知 scope 仍 400（`model` 入词表不放松校验）。
	resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/usage?scope=model2", key, "")
	if resp.status != http.StatusBadRequest {
		t.Fatalf("scope=model2 = %d（want 400）", resp.status)
	}
	// 响应体零凭据字节（模型名不是秘密，但也不该带出任何池/凭据面）。
	if strings.Contains(resp.body, "sk-") || strings.Contains(resp.body, "aaaa1111aaaa") {
		t.Fatalf("usage 响应出现敏感字节: %s", resp.body)
	}
}
