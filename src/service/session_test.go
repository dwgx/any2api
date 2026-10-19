package service

// session_test.go —— 公开服务 P0-1/P0-2 测试（board A2A-L19-W57-1）：
// 匿名会话签发/复用/滚动/过期/登出（§1 验收锚 1/4）+ gated 门控（无 key 401 /
// 建会话后凭 cookie 通过 / key 移除失效）+ 硬门零回归（public.enabled=false）+
// access_keys env-ref + 配置校验。
//
// 反证锚点（本文件；回退即红——原样输出见 tmp/lanes/W57-a/proofs/）：
//   - 撤掉 sessionWrap 的 gated 401 分支（matchAccessKey 恒放行）→
//     TestPublicGatedNoKey401 红（无 key 请求 200）；
//   - 撤掉会话 key 集校验（knownKeyID 恒 true）→
//     TestPublicGatedKeyRemovalInvalidatesSession 红（旧会话仍 200）；
//   - 无 cookie 时不签发（去掉 open 模式的自动 create）→
//     TestPublicSessionIssueAndReuse 红（无 Set-Cookie）；
//   - 挂载会话中间件不检查 public.enabled（恒挂载）→
//     TestPublicSessionDisabledZeroRegression 红（出现 Set-Cookie）。

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

// pubOpenCfg 公开面 open 模式配置（单上游占位——模型面请求不触网）。
func pubOpenCfg() *Config {
	c := uiTestConfig()
	c.HeartbeatSeconds = 15
	c.Public = &PublicSection{Enabled: true}
	return c
}

// pubGatedCfg gated 模式配置（keys = 准入 key 列表）。
func pubGatedCfg(keys ...string) *Config {
	c := uiTestConfig()
	c.HeartbeatSeconds = 15
	c.Public = &PublicSection{Enabled: true, AccessMode: "gated", AccessKeys: keys}
	return c
}

// pubReq 发请求（可选头）。
func pubReq(t *testing.T, method, url string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do %s: %v", url, err)
	}
	return resp
}

// pubReadBody 读尽并关闭。
func pubReadBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}

// sessionCookieValue 取响应里的 a2a_sid cookie 值（无 → ""）。
func sessionCookieValue(resp *http.Response) string {
	for _, c := range resp.Cookies() {
		if c.Name == sessionCookieName {
			return c.Value
		}
	}
	return ""
}

// testSha8 key_id（sha256 前 8 hex）——与服务端记账口径同式。
func testSha8(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:8]
}

// ---------------------------------------------------------------------------
// P0-1：会话签发 / 复用 / 滚动 / 过期 / 登出
// ---------------------------------------------------------------------------

// TestPublicSessionIssueAndReuse §1 验收锚 1：无 cookie 首请求 → 200 +
// Set-Cookie（HttpOnly/SameSite=Lax/Path=/；HTTP 直连不带 Secure）；带 cookie
// 再请求 → 同会话（无重复 Set-Cookie）；无效 cookie → 重新签发。
func TestPublicSessionIssueAndReuse(t *testing.T) {
	s, srv := startServiceWith(t, pubOpenCfg())
	// 假时钟（每次调用前进 1ms——绕开 Windows 时钟粒度，lastSeen 断言确定化）。
	base := time.Now()
	var ticks atomic.Int32
	s.sessions.now = func() time.Time {
		return base.Add(time.Duration(ticks.Add(1)) * time.Millisecond)
	}

	// 1) 无 cookie 首请求 → 自动签发。
	resp := pubReq(t, http.MethodGet, srv.URL+"/v1/models", nil)
	raw := resp.Header.Get("Set-Cookie")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	pubReadBody(t, resp)
	if !strings.Contains(raw, sessionCookieName+"=") {
		t.Fatalf("应签发 %s: %q", sessionCookieName, raw)
	}
	for _, attr := range []string{"HttpOnly", "SameSite=Lax", "Path=/", "Max-Age="} {
		if !strings.Contains(raw, attr) {
			t.Fatalf("cookie 缺属性 %s: %q", attr, raw)
		}
	}
	if strings.Contains(raw, "Secure") {
		t.Fatalf("HTTP 直连不得带 Secure: %q", raw)
	}
	sid1 := sessionCookieValue(resp)
	if len(sid1) != 22 {
		t.Fatalf("sid 长度 = %d（want 22 base64url）: %q", len(sid1), sid1)
	}
	rec1, ok := s.sessions.get(sid1, time.Now())
	if !ok {
		t.Fatalf("会话应已入表: %s", sid1)
	}
	if rec1.keyID != "" {
		t.Fatalf("open 模式 keyID 应为空: %q", rec1.keyID)
	}

	// 2) 带 cookie 再请求 → 同会话；有效 cookie 不重复 Set-Cookie。
	resp2 := pubReq(t, http.MethodGet, srv.URL+"/v1/models", map[string]string{
		"Cookie": sessionCookieName + "=" + sid1,
	})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("status2 = %d", resp2.StatusCode)
	}
	if sc := resp2.Header.Get("Set-Cookie"); sc != "" {
		t.Fatalf("有效 cookie 不应重复 Set-Cookie: %q", sc)
	}
	pubReadBody(t, resp2)
	if s.sessions.len() != 1 {
		t.Fatalf("会话数 = %d（want 1——同会话复用）", s.sessions.len())
	}
	rec2, ok := s.sessions.get(sid1, time.Now())
	if !ok || !rec2.lastSeen.After(rec1.lastSeen) {
		t.Fatalf("lastSeen 应滑动刷新: %v → %v", rec1.lastSeen, rec2.lastSeen)
	}

	// 3) 无效 cookie → 视同无会话 → 重新签发（新 sid）。
	resp3 := pubReq(t, http.MethodGet, srv.URL+"/v1/models", map[string]string{
		"Cookie": sessionCookieName + "=bogus-sid-value",
	})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("status3 = %d", resp3.StatusCode)
	}
	sid2 := sessionCookieValue(resp3)
	if sid2 == "" || sid2 == sid1 {
		t.Fatalf("无效 cookie 应重新签发（sid2=%q sid1=%q）", sid2, sid1)
	}
	pubReadBody(t, resp3)
	if s.sessions.len() != 2 {
		t.Fatalf("会话数 = %d（want 2）", s.sessions.len())
	}
}

// TestPublicSessionDisabledZeroRegression 硬门：public.enabled 缺省（含显式
// false）时——无会话表、无限流闸、零 Set-Cookie、聊天链路逐字节可用。
func TestPublicSessionDisabledZeroRegression(t *testing.T) {
	// 缺省（不写 public 段）。
	s, srv := startServiceWith(t, uiTestConfig())
	if s.sessions != nil || s.limiter != nil {
		t.Fatalf("缺省形态不得挂会话/闸（sessions=%v limiter=%v）", s.sessions != nil, s.limiter != nil)
	}
	for _, p := range []string{"/v1/models", "/ui/api/config", "/healthz"} {
		resp := pubReq(t, http.MethodGet, srv.URL+p, nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d", p, resp.StatusCode)
		}
		if sc := resp.Header.Get("Set-Cookie"); sc != "" {
			t.Fatalf("%s 不得有 Set-Cookie（零回归）: %q", p, sc)
		}
		pubReadBody(t, resp)
	}

	// 显式 enabled: false 同形。
	c2 := uiTestConfig()
	c2.Public = &PublicSection{Enabled: false}
	s2, srv2 := startServiceWith(t, c2)
	if s2.sessions != nil || s2.limiter != nil {
		t.Fatalf("enabled=false 不得挂会话/闸")
	}
	resp := pubReq(t, http.MethodGet, srv2.URL+"/v1/models", nil)
	if sc := resp.Header.Get("Set-Cookie"); sc != "" {
		t.Fatalf("enabled=false 不得有 Set-Cookie: %q", sc)
	}
	pubReadBody(t, resp)

	// 聊天链路（真上游 mock）零回归：200 + 正常流 + 零 Set-Cookie。
	up := goodClaudeUpstream(t)
	s3, srv3 := startServiceWith(t, failoverCfg(anthUp("good", up.srv.URL)))
	resp3 := postJSON(t, srv3.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	body3 := pubReadBody(t, resp3)
	if resp3.StatusCode != 200 || !strings.Contains(string(body3), "Hello ") {
		t.Fatalf("聊天链路回归: %d %s", resp3.StatusCode, body3)
	}
	if sc := resp3.Header.Get("Set-Cookie"); sc != "" {
		t.Fatalf("聊天链路不得有 Set-Cookie: %q", sc)
	}
	if s3.sessions != nil || s3.limiter != nil {
		t.Fatalf("聊天服务不得挂会话/闸")
	}
}

// TestPublicSessionLogout §1 验收锚 4：logout 删会话 + 清 cookie；旧 cookie
// 再请求 → 新会话签发（旧会话已删）；无会话 logout 幂等。
func TestPublicSessionLogout(t *testing.T) {
	s, srv := startServiceWith(t, pubOpenCfg())

	resp := pubReq(t, http.MethodGet, srv.URL+"/v1/models", nil)
	sid1 := sessionCookieValue(resp)
	pubReadBody(t, resp)
	if sid1 == "" {
		t.Fatal("应先签发会话")
	}

	// 方法限定：GET → 405。
	resp = pubReq(t, http.MethodGet, srv.URL+"/ui/api/session/logout", map[string]string{
		"Cookie": sessionCookieName + "=" + sid1,
	})
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET logout status = %d（want 405）", resp.StatusCode)
	}
	pubReadBody(t, resp)

	// POST logout（带 cookie）→ 200 + 清 cookie + 会话删除。
	resp = pubReq(t, http.MethodPost, srv.URL+"/ui/api/session/logout", map[string]string{
		"Cookie": sessionCookieName + "=" + sid1,
	})
	body := pubReadBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("logout status = %d body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), `"ok":true`) {
		t.Fatalf("logout body = %s", body)
	}
	if sc := resp.Header.Get("Set-Cookie"); !strings.Contains(sc, sessionCookieName+"=") || !strings.Contains(sc, "Max-Age=0") {
		t.Fatalf("logout 应清 cookie: %q", sc)
	}
	if s.sessions.len() != 0 {
		t.Fatalf("logout 后会话数 = %d（want 0）", s.sessions.len())
	}

	// 旧 cookie 再请求 → 重新签发（新 sid）。
	resp = pubReq(t, http.MethodGet, srv.URL+"/v1/models", map[string]string{
		"Cookie": sessionCookieName + "=" + sid1,
	})
	sid2 := sessionCookieValue(resp)
	pubReadBody(t, resp)
	if sid2 == "" || sid2 == sid1 {
		t.Fatalf("旧 cookie 应重新签发（sid2=%q sid1=%q）", sid2, sid1)
	}

	// 无 cookie logout → 幂等 200（不建新会话）。
	before := s.sessions.len()
	resp = pubReq(t, http.MethodPost, srv.URL+"/ui/api/session/logout", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("无会话 logout status = %d（want 200 幂等）", resp.StatusCode)
	}
	pubReadBody(t, resp)
	if s.sessions.len() != before {
		t.Fatalf("无会话 logout 不得建会话: %d → %d", before, s.sessions.len())
	}
}

// TestPublicSessionExpiryReissues 过期会话请求 = 无会话 → 重新签发（用户无感）。
func TestPublicSessionExpiryReissues(t *testing.T) {
	s, srv := startServiceWith(t, pubOpenCfg())

	t0 := time.Now()
	s.sessions.now = func() time.Time { return t0 }
	resp := pubReq(t, http.MethodGet, srv.URL+"/v1/models", nil)
	sid1 := sessionCookieValue(resp)
	pubReadBody(t, resp)
	if sid1 == "" {
		t.Fatal("应先签发会话")
	}

	// 假时钟推进超过 idle TTL（7d）。
	s.sessions.now = func() time.Time { return t0.Add(sessionIdleTTL + time.Minute) }
	resp = pubReq(t, http.MethodGet, srv.URL+"/v1/models", map[string]string{
		"Cookie": sessionCookieName + "=" + sid1,
	})
	sid2 := sessionCookieValue(resp)
	pubReadBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("过期后 status = %d", resp.StatusCode)
	}
	if sid2 == "" || sid2 == sid1 {
		t.Fatalf("过期会话应重新签发（sid2=%q sid1=%q）", sid2, sid1)
	}
}

// ---------------------------------------------------------------------------
// P0-2：access key 门控
// ---------------------------------------------------------------------------

// TestPublicGatedNoKey401 §1 验收锚 2（前半）：gated 无 key → 401（按入口协议
// 信封）；/ui/api/config 除外放行；全程零会话（无 key 的新会话不可建立）。
func TestPublicGatedNoKey401(t *testing.T) {
	s, srv := startServiceWith(t, pubGatedCfg("sk-w57-key-a"))

	cases := []struct {
		name    string
		path    string
		body    string
		wantSub string
	}{
		{"claude 入口", "/v1/messages", `{"model":"m","max_tokens":8,"messages":[]}`, "authentication_error"},
		// openai 信封把鉴权类渲染为官方 invalid_api_key（pairs error face 映射）。
		{"openai 入口", "/v1/chat/completions", `{"model":"m","messages":[]}`, "invalid_api_key"},
		{"ui api 面", "/ui/api/anything", "", "authentication_error"},
	}
	for _, tc := range cases {
		var resp *http.Response
		if tc.body != "" {
			resp = postJSON(t, srv.URL+tc.path, tc.body, nil)
		} else {
			resp = pubReq(t, http.MethodGet, srv.URL+tc.path, nil)
		}
		raw := pubReadBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s: status = %d（want 401）body=%s", tc.name, resp.StatusCode, raw)
		}
		if !strings.Contains(string(raw), tc.wantSub) {
			t.Fatalf("%s: 信封缺 %q: %s", tc.name, tc.wantSub, raw)
		}
		if sc := resp.Header.Get("Set-Cookie"); sc != "" {
			t.Fatalf("%s: 401 不得 Set-Cookie: %q", tc.name, sc)
		}
	}
	if s.sessions.len() != 0 {
		t.Fatalf("无 key 请求不得建会话: %d", s.sessions.len())
	}

	// /ui/api/config 除外（前端引导配置）→ 200 放行。
	resp := pubReq(t, http.MethodGet, srv.URL+"/ui/api/config", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("config 面 status = %d（want 200 放行）", resp.StatusCode)
	}
	pubReadBody(t, resp)

	// logout 无 key 幂等 200（不建会话——清 cookie 语义）。
	resp = pubReq(t, http.MethodPost, srv.URL+"/ui/api/session/logout", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("logout status = %d（want 200 幂等）", resp.StatusCode)
	}
	pubReadBody(t, resp)
	if s.sessions.len() != 0 {
		t.Fatalf("logout 不得建会话: %d", s.sessions.len())
	}
}

// TestPublicGatedKeyAdmitsAndCookieCarries §1 验收锚 2（中段）：带 key（双头
// 两形态）建会话（记 key_id=sha8）→ 凭 cookie 通过（key 不必每请求带）；错误
// key 401；响应体绝不回显 key。
func TestPublicGatedKeyAdmitsAndCookieCarries(t *testing.T) {
	up := goodClaudeUpstream(t)
	const key = "sk-w57-gated-secret-a"
	cfg := pubGatedCfg(key)
	cfg.Upstreams[0].BaseURL = up.srv.URL
	s, srv := startServiceWith(t, cfg)

	chat := `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`

	// 错误 key → 401。
	resp := postJSON(t, srv.URL+"/v1/chat/completions", chat, map[string]string{"X-Access-Key": "wrong-key"})
	raw := pubReadBody(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("错误 key status = %d body=%s", resp.StatusCode, raw)
	}

	// 正确 key（X-Access-Key）→ 200 + Set-Cookie；key_id = sha8 入表。
	resp = postJSON(t, srv.URL+"/v1/chat/completions", chat, map[string]string{"X-Access-Key": key})
	raw = pubReadBody(t, resp)
	if resp.StatusCode != 200 || !strings.Contains(string(raw), "Hello ") {
		t.Fatalf("带 key status=%d body=%s", resp.StatusCode, raw)
	}
	if strings.Contains(string(raw), key) {
		t.Fatalf("响应体不得回显 key")
	}
	sid1 := sessionCookieValue(resp)
	if sid1 == "" {
		t.Fatal("带 key 应签发会话")
	}
	rec, ok := s.sessions.get(sid1, time.Now())
	if !ok || rec.keyID != testSha8(key) {
		t.Fatalf("key_id = %q（want sha8=%q）", rec.keyID, testSha8(key))
	}

	// 仅凭 cookie（无 key）→ 200；不重复 Set-Cookie。
	resp = postJSON(t, srv.URL+"/v1/chat/completions", chat, map[string]string{
		"Cookie": sessionCookieName + "=" + sid1,
	})
	raw = pubReadBody(t, resp)
	if resp.StatusCode != 200 || !strings.Contains(string(raw), "Hello ") {
		t.Fatalf("cookie-only status=%d body=%s", resp.StatusCode, raw)
	}
	if sc := resp.Header.Get("Set-Cookie"); sc != "" {
		t.Fatalf("cookie-only 不应重发 Set-Cookie: %q", sc)
	}

	// Bearer 形态（新请求无 cookie）→ 200 + 新会话。
	resp = postJSON(t, srv.URL+"/v1/chat/completions", chat, map[string]string{"Authorization": "Bearer " + key})
	raw = pubReadBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("Bearer status=%d body=%s", resp.StatusCode, raw)
	}
	sid2 := sessionCookieValue(resp)
	if sid2 == "" || sid2 == sid1 {
		t.Fatalf("Bearer 请求应签发新会话（sid2=%q sid1=%q）", sid2, sid1)
	}

	// 全链零凭据回显（会话记录只存摘要）。
	if rec2, ok := s.sessions.get(sid2, time.Now()); !ok || rec2.keyID != testSha8(key) || strings.Contains(rec2.keyID, key) {
		t.Fatalf("会话记录不得含 key 原文: %+v", rec2)
	}
}

// TestPublicGatedKeyRemovalInvalidatesSession §1 验收锚 2（后半）：key 移除
// （key 集变更）→ 旧会话即时失效（401 + 会话删除）；重启档（新进程 = 新内存
// 表 + 新 key 集）→ 旧 cookie 401。
func TestPublicGatedKeyRemovalInvalidatesSession(t *testing.T) {
	up := goodClaudeUpstream(t)
	cfg := pubGatedCfg("sk-w57-gated-a")
	cfg.Upstreams[0].BaseURL = up.srv.URL
	s, srv := startServiceWith(t, cfg)
	chat := `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`

	resp := postJSON(t, srv.URL+"/v1/chat/completions", chat, map[string]string{"X-Access-Key": "sk-w57-gated-a"})
	pubReadBody(t, resp)
	sid1 := sessionCookieValue(resp)
	if sid1 == "" {
		t.Fatal("应先建会话")
	}

	// key 集变更（模拟「改配置 + 重启」的吊销语义；同进程直接替换 key 集 = 等效面）。
	s.accessKeys = newAccessKeys([]string{"sk-w57-gated-b"})
	resp = postJSON(t, srv.URL+"/v1/chat/completions", chat, map[string]string{
		"Cookie": sessionCookieName + "=" + sid1,
	})
	raw := pubReadBody(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("key 移除后旧会话 status = %d（want 401）body=%s", resp.StatusCode, raw)
	}
	if s.sessions.len() != 0 {
		t.Fatalf("key 移除后会话应被吊销删除: %d", s.sessions.len())
	}

	// 重启档：新 Server（新内存表 + 新 key 集）→ 旧 cookie 401。
	cfg2 := pubGatedCfg("sk-w57-gated-b")
	cfg2.Upstreams[0].BaseURL = up.srv.URL
	_, srv2 := startServiceWith(t, cfg2)
	resp = postJSON(t, srv2.URL+"/v1/chat/completions", chat, map[string]string{
		"Cookie": sessionCookieName + "=" + sid1,
	})
	pubReadBody(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("重启后旧 cookie status = %d（want 401）", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 配置面：env-ref + 校验
// ---------------------------------------------------------------------------

// TestPublicAccessKeysEnvRef access_keys 支持 ${VAR}（同 upstream credentials
// 机制；值绝不落盘）；未定义 → 显式报错含变量名。
func TestPublicAccessKeysEnvRef(t *testing.T) {
	t.Setenv("A2A_L19_W57_KEY_ENV", "sk-env-abc")
	p := writeEnvCfg(t, "listen: \"127.0.0.1:0\"\nupstreams:\n  - name: u\n    base_url: http://127.0.0.1:9\n    api_key: k\n    protocol: openai_chat\npublic:\n  enabled: true\n  access_mode: gated\n  access_keys: [\"${A2A_L19_W57_KEY_ENV}\"]\n")
	c, err := LoadConfig(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := c.Public.AccessKeys[0]; got != "sk-env-abc" {
		t.Fatalf("access_keys 展开 = %q（want env 值）", got)
	}

	// 未定义 → 显式报错（含变量名）。
	p2 := writeEnvCfg(t, "listen: \"127.0.0.1:0\"\nupstreams:\n  - name: u\n    base_url: http://127.0.0.1:9\n    api_key: k\n    protocol: openai_chat\npublic:\n  enabled: true\n  access_mode: gated\n  access_keys: [\"${A2A_L19_W57_KEY_MISSING}\"]\n")
	_, err = LoadConfig(p2)
	if err == nil || !strings.Contains(err.Error(), "A2A_L19_W57_KEY_MISSING") {
		t.Fatalf("未定义引用应报错: %v", err)
	}
}

// TestPublicConfigValidation 公开段校验（显式报错，绝不静默）。
func TestPublicConfigValidation(t *testing.T) {
	base := func() *Config {
		c := uiTestConfig()
		c.Public = &PublicSection{Enabled: true}
		return c
	}
	// gated 无 keys → 错误。
	c := base()
	c.Public.AccessMode = "gated"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "access_keys") {
		t.Fatalf("gated 无 keys 应报错: %v", err)
	}
	// open + keys → 错误（歧义）。
	c = base()
	c.Public.AccessKeys = []string{"k"}
	if err := c.Validate(); err == nil {
		t.Fatal("open + keys 应报错")
	}
	// 非法 access_mode → 错误。
	c = base()
	c.Public.AccessMode = "closed"
	if err := c.Validate(); err == nil {
		t.Fatal("非法 access_mode 应报错")
	}
	// 非法 trusted_proxies → 错误。
	c = base()
	c.Public.TrustedProxies = []string{"not-an-ip"}
	if err := c.Validate(); err == nil {
		t.Fatal("非法 trusted_proxies 应报错")
	}
	// 负值 → 错误。
	c = base()
	c.Public.MaxInflight = -1
	if err := c.Validate(); err == nil {
		t.Fatal("负 max_inflight 应报错")
	}
	c = base()
	c.Public.QueueSize = -1
	if err := c.Validate(); err == nil {
		t.Fatal("负 queue_size 应报错")
	}
	c = base()
	c.Public.QueueTimeoutSeconds = -1
	if err := c.Validate(); err == nil {
		t.Fatal("负 queue_timeout_seconds 应报错")
	}
	neg := -1
	c = base()
	c.Public.CredentialMinGapSeconds = &neg
	if err := c.Validate(); err == nil {
		t.Fatal("负 credential_min_gap_seconds 应报错")
	}
	// 合法组合：gated + keys + CIDR/裸 IP 混配 → 通过。
	c = base()
	c.Public.AccessMode = "gated"
	c.Public.AccessKeys = []string{"k"}
	c.Public.TrustedProxies = []string{"127.0.0.1/32", "::1"}
	if err := c.Validate(); err != nil {
		t.Fatalf("合法配置应通过: %v", err)
	}
	// 缺省值（未配）读取。
	c = base()
	if c.publicMaxInflight() != DefaultPublicMaxInflight || c.publicQueueSize() != DefaultPublicQueueSize ||
		c.publicQueueTimeout() != DefaultPublicQueueTimeout || c.credentialMinGap() != DefaultCredentialMinGap {
		t.Fatal("缺省值不符")
	}
	// 显式 0 的 min-gap = 关。
	zero := 0
	c.Public.CredentialMinGapSeconds = &zero
	if c.credentialMinGap() != 0 {
		t.Fatal("显式 0 min-gap 应为关")
	}
}

// TestPublicSIDFromContext ctx 传递面：会话经 ctx 可读（SIDFromContext）。
func TestPublicSIDFromContext(t *testing.T) {
	rec := sessionRecord{
		sid: "sid-test-123", keyID: "abcd1234",
		created: time.Unix(1, 0), lastSeen: time.Unix(2, 0),
	}
	ctx := sessionWithContext(t.Context(), rec, time.Unix(3, 0))
	sess, ok := SIDFromContext(ctx)
	if !ok {
		t.Fatal("应可从 ctx 读取会话")
	}
	if sess.SID != "sid-test-123" || sess.KeyID != "abcd1234" || !sess.LastSeen.Equal(time.Unix(3, 0)) {
		t.Fatalf("会话字段 = %+v", sess)
	}
	if _, ok := SIDFromContext(t.Context()); ok {
		t.Fatal("无会话 ctx 应 ok=false")
	}
	// 值不随记录变化（快照语义）。
	rec.keyID = "changed"
	if sess2, _ := SIDFromContext(ctx); sess2.KeyID != "abcd1234" {
		t.Fatalf("ctx 会话应快照: %+v", sess2)
	}
}

// TestPublicSessionStoreBound 内存表有界：容量超限先清过期再逐最久未用。
func TestPublicSessionStoreBound(t *testing.T) {
	st := newSessionStore()
	st.cap = 3
	base := time.Unix(1000, 0)
	r1 := st.create("", base)
	r2 := st.create("", base.Add(1*time.Second))
	_ = st.create("", base.Add(2*time.Second))
	if st.len() != 3 {
		t.Fatalf("len = %d", st.len())
	}
	// 超限：逐最久未用（r1 最早）。
	st.create("", base.Add(3*time.Second))
	if st.len() != 3 {
		t.Fatalf("超限后 len = %d（want 3 有界）", st.len())
	}
	if _, ok := st.get(r1.sid, base.Add(3*time.Second)); ok {
		t.Fatal("最久未用会话应被逐出")
	}
	if _, ok := st.get(r2.sid, base.Add(3*time.Second)); !ok {
		t.Fatal("次新会话应保留")
	}

	// 过期清理优先于逐出。
	st2 := newSessionStore()
	st2.cap = 2
	a := st2.create("", base)
	b := st2.create("", base.Add(sessionIdleTTL+30*time.Minute))
	st2.create("", base.Add(sessionIdleTTL+time.Hour)) // 建新前先清过期（a 过期；b 未过期）
	if _, ok := st2.get(a.sid, base.Add(sessionIdleTTL+time.Hour)); ok {
		t.Fatal("过期会话应被清理")
	}
	if _, ok := st2.get(b.sid, base.Add(sessionIdleTTL+time.Hour)); !ok {
		t.Fatal("未过期会话不得被逐")
	}
}

// TestPublicSessionStoreCapGuard cap<=0 不得死循环（W60-SRC1 Findings）：
// create 把非法容量归一化为 1（仍有界），调用必须返回且表长度 ≤ 1。
func TestPublicSessionStoreCapGuard(t *testing.T) {
	for _, badCap := range []int{0, -3} {
		st := newSessionStore()
		st.cap = badCap
		done := make(chan sessionRecord, 1)
		go func() {
			done <- st.create("", time.Unix(1000, 0))
		}()
		select {
		case rec := <-done:
			if rec.sid == "" {
				t.Fatalf("cap=%d: create 返回空 sid", badCap)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("cap=%d: create 死循环（未在 2s 内返回）", badCap)
		}
		if got := st.len(); got != 1 {
			t.Fatalf("cap=%d: len = %d（want 1，归一化后仍有界）", badCap, got)
		}
		// 归一化后继续建会话仍有界（容量 = 1：逐出最久未用）。
		st.create("", time.Unix(1001, 0))
		if got := st.len(); got != 1 {
			t.Fatalf("cap=%d: 第二次 create 后 len = %d（want 1）", badCap, got)
		}
	}
}
