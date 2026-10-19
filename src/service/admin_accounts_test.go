// admin_accounts_test.go —— 账号/代理清单 + 安全下线（board A2A-L19-W78-1）测试。
//
// 装置：真 Server + httptest；路由由 `Server.Handler()` 的 admin_key 分支注册
// （与生产同一入口——同时覆盖「未配 key = 404 / 错 key = 401 / 对 key = 200」）。
// 需要审计面时用 SQLite（t.TempDir()）。
//
// 反证锚点（回退即红）：
//   - `adminSetCooldown` 不写 `coolUntil` → TestAdminAccountsCooldownAndPoolVisibility 红
//     （负控 4：冷却必须在 /admin/pool 可见）；
//   - `adminSanitizeURL` / `adminProxyViewFor` 换成原文 → TestAdminExportNoSecretBytes 红
//     （负控 1：SECRET 字节出现在导出面）；
//   - 代理 display 回 userinfo → TestAdminAccountsInventoryAndProxies 红。
package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 装置
// ---------------------------------------------------------------------------

// adminOpsConfig 运营面测试配置：admin key + 四类上游（单 key / 3 凭据池 + 代理 /
// 空池 / http 代理）。凭据与 base_url **故意带 `sk-SECRET` 形态**——导出负控的种子。
func adminOpsConfig(t *testing.T, key string) *Config {
	t.Helper()
	cfg := uiTestConfig()
	cfg.HeartbeatSeconds = 15
	cfg.AdminKey = key
	cfg.Upstreams = []Upstream{
		{
			Name:     "anthropic-main",
			BaseURL:  "https://user:sk-SECRET-userinfo@api.example.com/v1",
			APIKey:   "sk-SECRET-ant",
			Protocol: ProtocolAnthropic,
		},
		{
			Name:           "web",
			Protocol:       ProtocolClaudeWeb,
			Credentials:    []string{"sk-SECRET-a", "sk-SECRET-b", "sk-SECRET-c"},
			Proxy:          "socks5h://sk-SECRET-user:sk-SECRET-pass@127.0.0.1:1080",
			TLSFingerprint: "chrome",
		},
		{
			Name:     "empty-web",
			Protocol: ProtocolClaudeWeb,
		},
		{
			Name:     "plain-http",
			BaseURL:  "http://127.0.0.1:8080",
			Protocol: ProtocolOpenAIChat,
			Proxy:    "http://127.0.0.1:3128",
		},
	}
	return cfg
}

// adminOpsStoreConfig 同 adminOpsConfig + SQLite store（审计面断言用）。
func adminOpsStoreConfig(t *testing.T, key string) *Config {
	t.Helper()
	cfg := adminOpsConfig(t, key)
	cfg.Store = &StoreSection{Enabled: true, Path: filepath.Join(t.TempDir(), "any2api.db")}
	return cfg
}

// adminOpsStart 启动被测服务（**走真实 Handler**——新路由的注册点就在那里）。
func adminOpsStart(t *testing.T, cfg *Config) (*Server, *httptest.Server) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(func() {
		srv.Close()
		_ = s.CloseStore()
	})
	return s, srv
}

// adminOpsEndpoints 本波新增的六条端点（无 key 时的 404 断言集）。
var adminOpsEndpoints = []struct{ method, path, body string }{
	{http.MethodGet, "/admin/accounts", ""},
	{http.MethodGet, "/admin/proxies", ""},
	{http.MethodGet, "/admin/export?type=accounts", ""},
	{http.MethodGet, "/admin/providers", ""},
	{http.MethodPost, "/admin/accounts/web/0/cooldown", `{"seconds":60}`},
	{http.MethodPost, "/admin/upstreams/web/probe", ""},
}

// ---------------------------------------------------------------------------
// 1. 惰性注册 + 鉴权：未配 key = 404 / 错 key = 401
// ---------------------------------------------------------------------------

func TestAdminAccountsNoKey404AndAuthGating(t *testing.T) {
	// 未配 admin_key：六条路由根本不注册（404 非 401）。
	_, srv := adminOpsStart(t, adminOpsConfig(t, ""))
	for _, e := range adminOpsEndpoints {
		resp := adminAPIReq(t, e.method, srv.URL+e.path, "any-key", e.body)
		if resp.status != http.StatusNotFound {
			t.Fatalf("%s %s = %d（want 404：未配密钥 = 路由不注册）body=%s",
				e.method, e.path, resp.status, resp.body)
		}
	}
	// 配了 key：对 key = 200（**先**做正路，避免连续失败触发该 IP 的封禁——
	// 封禁期连正确 key 也回 401，这是既有中间件的设计语义，见 TestAdminAuthBan）。
	_, srv = adminOpsStart(t, adminOpsConfig(t, "w78-key"))
	resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/accounts", "w78-key", "")
	if resp.status != http.StatusOK {
		t.Fatalf("/admin/accounts = %d body=%s", resp.status, resp.body)
	}
	// 无 key / 错 key = 401（只发 2 次：不触发 5 次失败封禁）。
	for _, key := range []string{"", "wrong-key"} {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/accounts", key, "")
		if resp.status != http.StatusUnauthorized {
			t.Fatalf("key=%q = %d（want 401）body=%s", key, resp.status, resp.body)
		}
	}
	// 方法不匹配：标准库 405 + Allow（路由存在但方法错）。
	resp = adminAPIReq(t, http.MethodPost, srv.URL+"/admin/accounts", "w78-key", "{}")
	if resp.status != http.StatusMethodNotAllowed {
		t.Fatalf("POST /admin/accounts = %d（want 405）", resp.status)
	}
	if got := resp.header.Get("Allow"); !strings.Contains(got, http.MethodGet) {
		t.Fatalf("Allow = %q", got)
	}
	resp = adminAPIReq(t, http.MethodGet, srv.URL+"/admin/accounts/web/0/cooldown", "w78-key", "")
	if resp.status != http.StatusMethodNotAllowed {
		t.Fatalf("GET cooldown = %d（want 405）", resp.status)
	}
}

// ---------------------------------------------------------------------------
// 2. /admin/accounts：形状 + 零凭据字节（店面 + 池 + 代理三面）
// ---------------------------------------------------------------------------

func TestAdminAccountsInventoryAndProxies(t *testing.T) {
	const key = "w78-key"
	_, srv := adminOpsStart(t, adminOpsConfig(t, key))

	resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/accounts", key, "")
	if resp.status != http.StatusOK {
		t.Fatalf("/admin/accounts = %d body=%s", resp.status, resp.body)
	}
	// 红线：零凭据字节（凭据 / base_url userinfo / 代理 userinfo 一律不回）。
	for _, needle := range []string{"SECRET", "sk-", "sk-SECRET-user:", "user:sk"} {
		if strings.Contains(resp.body, needle) {
			t.Fatalf("accounts 响应出现凭据字节 %q: %s", needle, resp.body)
		}
	}
	var out adminAccountsResponse
	if err := json.Unmarshal([]byte(resp.body), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Upstreams) != 4 {
		t.Fatalf("upstreams = %d（want 4）", len(out.Upstreams))
	}
	if out.Now <= 0 {
		t.Fatalf("now = %d", out.Now)
	}
	anth, web, empty, plain := out.Upstreams[0], out.Upstreams[1], out.Upstreams[2], out.Upstreams[3]

	// 上游 0：base_url 去 userinfo（保留 scheme/host/path）、池 = 单 api_key。
	if anth.Index != 0 || anth.Name != "anthropic-main" || anth.Protocol != string(ProtocolAnthropic) {
		t.Fatalf("upstream[0] = %+v", anth)
	}
	if anth.BaseURL != "https://api.example.com/v1" {
		t.Fatalf("base_url = %q（want 去 userinfo 的 https://api.example.com/v1）", anth.BaseURL)
	}
	if anth.PoolSize != 1 || len(anth.Credentials) != 1 || anth.Credentials[0].Index != 0 {
		t.Fatalf("upstream[0] 池 = %+v", anth)
	}
	if anth.Proxy.Configured {
		t.Fatalf("未配代理的上游应有 proxy.configured=false: %+v", anth.Proxy)
	}
	if anth.TLSFingerprint != "none" {
		t.Fatalf("tls_fingerprint 缺省 = %q（want none）", anth.TLSFingerprint)
	}

	// 上游 1：3 凭据池 + socks5h 代理（userinfo 只化为 auth 布尔）。
	if web.PoolSize != 3 || len(web.Credentials) != 3 {
		t.Fatalf("web 池 = %+v", web)
	}
	for i, c := range web.Credentials {
		if c.Index != i || c.Cooling || c.CooldownUntil != 0 || c.LastUsed != 0 || c.Busy {
			t.Fatalf("web credential[%d] 初始态 = %+v", i, c)
		}
	}
	p := web.Proxy
	if !p.Configured || p.Scheme != "socks5h" || p.Host != "127.0.0.1" || p.Port != 1080 || !p.Auth {
		t.Fatalf("web proxy = %+v", p)
	}
	if p.Display != "socks5h://127.0.0.1:1080" {
		t.Fatalf("proxy.display = %q（want 无 userinfo 的 scheme://host:port）", p.Display)
	}
	if web.TLSFingerprint != "chrome" {
		t.Fatalf("web tls_fingerprint = %q", web.TLSFingerprint)
	}
	// 上游 2：空池 + 未配 base_url（web 协议缺省在运行时，不在配置面发明）。
	if empty.PoolSize != 0 || len(empty.Credentials) != 0 || empty.BaseURL != "" {
		t.Fatalf("empty-web = %+v", empty)
	}
	// 上游 3：http 代理（无 userinfo → auth=false）。
	if !plain.Proxy.Configured || plain.Proxy.Scheme != "http" || plain.Proxy.Port != 3128 || plain.Proxy.Auth {
		t.Fatalf("plain-http proxy = %+v", plain.Proxy)
	}
	if plain.Proxy.Display != "http://127.0.0.1:3128" {
		t.Fatalf("plain-http display = %q", plain.Proxy.Display)
	}

	// /admin/proxies：逐上游一行（含未配置行；序 = 配置序）。
	resp = adminAPIReq(t, http.MethodGet, srv.URL+"/admin/proxies", key, "")
	if resp.status != http.StatusOK {
		t.Fatalf("/admin/proxies = %d body=%s", resp.status, resp.body)
	}
	if strings.Contains(resp.body, "SECRET") || strings.Contains(resp.body, "sk-") {
		t.Fatalf("proxies 响应出现凭据字节: %s", resp.body)
	}
	var pr adminProxiesResponse
	if err := json.Unmarshal([]byte(resp.body), &pr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(pr.Proxies) != 4 || pr.Now <= 0 {
		t.Fatalf("proxies = %+v", pr)
	}
	if pr.Proxies[0].Upstream != "anthropic-main" || pr.Proxies[0].Configured ||
		pr.Proxies[0].Display != "" || pr.Proxies[0].TLSFingerprint != "none" {
		t.Fatalf("proxies[0] = %+v", pr.Proxies[0])
	}
	if pr.Proxies[1].Display != "socks5h://127.0.0.1:1080" || !pr.Proxies[1].Auth || pr.Proxies[1].Port != 1080 {
		t.Fatalf("proxies[1] = %+v", pr.Proxies[1])
	}
	if pr.Proxies[2].Upstream != "empty-web" || pr.Proxies[2].Configured {
		t.Fatalf("proxies[2] = %+v", pr.Proxies[2])
	}
	if pr.Proxies[3].Display != "http://127.0.0.1:3128" || pr.Proxies[3].Auth {
		t.Fatalf("proxies[3] = %+v", pr.Proxies[3])
	}
	// 未配置代理的行同样要出现在清单里（**空列表不是空**——四台上游恒四行）。
	if strings.Contains(resp.body, `"proxies":null`) {
		t.Fatalf("proxies 空态形状错: %s", resp.body)
	}
}

// ---------------------------------------------------------------------------
// 3. POST /admin/accounts/{upstream}/{index}/cooldown：生效 + 可见 + 边界
// ---------------------------------------------------------------------------

func TestAdminAccountsCooldownAndPoolVisibility(t *testing.T) {
	const key = "w78-key"
	s, srv := adminOpsStart(t, adminOpsStoreConfig(t, key))

	before := time.Now()
	resp := adminAPIReq(t, http.MethodPost, srv.URL+"/admin/accounts/web/1/cooldown", key,
		`{"seconds":3600,"reason":"manual"}`)
	if resp.status != http.StatusOK {
		t.Fatalf("cooldown = %d body=%s", resp.status, resp.body)
	}
	var out adminCooldownResponse
	if err := json.Unmarshal([]byte(resp.body), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Upstream != "web" || out.Index != 1 || !out.Cooling {
		t.Fatalf("cooldown 响应 = %+v", out)
	}
	if out.CooldownRemainingSeconds != 3600 {
		t.Fatalf("remaining = %d（want 3600）", out.CooldownRemainingSeconds)
	}
	wantUntil := before.Add(3600 * time.Second)
	if got := time.UnixMilli(out.CooldownUntil); got.Before(wantUntil.Add(-2*time.Second)) || got.After(wantUntil.Add(2*time.Second)) {
		t.Fatalf("cooldown_until = %v（want ≈ %v）", got, wantUntil)
	}
	// 凭据字节绝不回。
	if strings.Contains(resp.body, "sk-") || strings.Contains(resp.body, "SECRET") {
		t.Fatalf("cooldown 响应出现凭据字节: %s", resp.body)
	}

	// 负控 4 的正面：冷却必须在 /admin/pool 可见（只写 coolUntil[i] 的机械面）。
	var pool adminPoolResponse
	adminAPIGetJSON(t, srv.URL+"/admin/pool", key, &pool)
	if len(pool.Upstreams) != 4 {
		t.Fatalf("pool upstreams = %d", len(pool.Upstreams))
	}
	webPool := pool.Upstreams[1]
	if !webPool.Credentials[1].Cooling || webPool.Credentials[1].CooldownUntil != out.CooldownUntil ||
		webPool.Credentials[1].CooldownRemainingSeconds != 3600 {
		t.Fatalf("pool credential[1] = %+v（want 冷却）", webPool.Credentials[1])
	}
	if webPool.Credentials[0].Cooling || webPool.Credentials[2].Cooling {
		t.Fatalf("只应冷却 [1]: %+v", webPool.Credentials)
	}
	// 账号清单同一形状（复用 adminSnapshot）。
	var acc adminAccountsResponse
	adminAPIGetJSON(t, srv.URL+"/admin/accounts", key, &acc)
	if !acc.Upstreams[1].Credentials[1].Cooling {
		t.Fatalf("accounts.credentials[1] 未反映冷却: %+v", acc.Upstreams[1].Credentials[1])
	}

	// 绝不缩短已有冷却：第二次给更短的窗，生效值仍是更晚者。
	resp = adminAPIReq(t, http.MethodPost, srv.URL+"/admin/accounts/web/1/cooldown", key, `{"seconds":60}`)
	if resp.status != http.StatusOK {
		t.Fatalf("cooldown #2 = %d body=%s", resp.status, resp.body)
	}
	var second adminCooldownResponse
	if err := json.Unmarshal([]byte(resp.body), &second); err != nil {
		t.Fatalf("decode #2: %v", err)
	}
	if second.CooldownUntil != out.CooldownUntil || second.CooldownRemainingSeconds < 3500 {
		t.Fatalf("冷却被缩短: first=%+v second=%+v", out, second)
	}

	// 404：未知上游 / 越界下标 / 空池 / 负数下标。
	for _, path := range []string{
		"/admin/accounts/nope/0/cooldown",
		"/admin/accounts/web/9/cooldown",
		"/admin/accounts/empty-web/0/cooldown",
		"/admin/accounts/web/-1/cooldown",
	} {
		resp := adminAPIReq(t, http.MethodPost, srv.URL+path, key, `{"seconds":60}`)
		if resp.status != http.StatusNotFound {
			t.Fatalf("%s = %d（want 404）body=%s", path, resp.status, resp.body)
		}
	}
	// 400：seconds 缺 / 0 / 越界 / 非整数；坏 JSON；未知字段；reason 超长 / 控制字符。
	longReason := strings.Repeat("字", 121)
	for _, body := range []string{
		`{}`,
		`{"seconds":0}`,
		`{"seconds":86401}`,
		`{"seconds":"60"}`,
		`not-json`,
		`{"seconds":60,"extra":1}`,
		`{"seconds":60,"reason":"` + longReason + `"}`,
		`{"seconds":60,"reason":"line\nbreak"}`,
	} {
		resp := adminAPIReq(t, http.MethodPost, srv.URL+"/admin/accounts/web/1/cooldown", key, body)
		if resp.status != http.StatusBadRequest {
			t.Fatalf("cooldown body=%q = %d（want 400）resp=%s", body, resp.status, resp.body)
		}
	}
	// 边界合法值：1 与 86400。
	for _, body := range []string{`{"seconds":1}`, `{"seconds":86400,"reason":"长窗"}`} {
		resp := adminAPIReq(t, http.MethodPost, srv.URL+"/admin/accounts/web/0/cooldown", key, body)
		if resp.status != http.StatusOK {
			t.Fatalf("cooldown body=%q = %d（want 200）resp=%s", body, resp.status, resp.body)
		}
	}

	// 审计：只落 upstream/index/seconds/reason（零凭据字节）；store 已启用。
	rows, err := s.store.AuditQuery(storeAuditKindAccountCooldown, time.Time{}, 10)
	if err != nil {
		t.Fatalf("audit query: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("audit rows = %d（want 4）", len(rows))
	}
	first := rows[0].Detail
	for _, needle := range []string{`"upstream":"web"`, `"index":1`, `"seconds":3600`, `"reason":"manual"`} {
		if !strings.Contains(first, needle) {
			t.Fatalf("audit detail 缺 %s: %s", needle, first)
		}
	}
	if strings.Contains(first, "sk-") || strings.Contains(first, "SECRET") {
		t.Fatalf("audit detail 出现凭据字节: %s", first)
	}
}

// ---------------------------------------------------------------------------
// 4. 安全下线的直接单元面：adminSetCooldown 只写 coolUntil[i]
// ---------------------------------------------------------------------------

func TestAdminAccountsSetCooldownUnitBounds(t *testing.T) {
	pool := newCredentialPool(Upstream{
		Name: "unit", Protocol: ProtocolClaudeWeb, Credentials: []string{"sk-SECRET-a", "sk-SECRET-b"},
	})
	now := time.Now()
	until := now.Add(time.Minute)
	if !pool.adminSetCooldown(1, until) {
		t.Fatalf("adminSetCooldown(1) 应成功")
	}
	size, creds := pool.adminSnapshot(now)
	if size != 2 || !creds[1].Cooling || creds[0].Cooling || creds[1].CooldownUntil != until.UnixMilli() {
		t.Fatalf("池状态 = %d %+v", size, creds)
	}
	// 越界 / 负下标 / nil 池 = false（且零 panic）。
	if pool.adminSetCooldown(2, until) || pool.adminSetCooldown(-1, until) {
		t.Fatalf("越界下标应 false")
	}
	var nilPool *credentialPool
	if nilPool.adminSetCooldown(0, until) {
		t.Fatalf("nil 池应 false")
	}
	if nilPool.size() != 0 {
		t.Fatalf("nil 池 size = %d", nilPool.size())
	}
}
