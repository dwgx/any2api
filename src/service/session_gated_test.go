// session_gated_test.go —— L19-W68-3：gated 模式下 `/v1beta*`（Gemini/Vertex
// 入口族）access key 门控 + Gemini 客户端两种 key 携带方式（`x-goog-api-key`
// 头 / `?key=` 查询）。
//
// 缺口（修复前）：sessionEligiblePath 不含 `/v1beta*`（W57-1 刻意边界——中间件
// 当时只认 Bearer / X-Access-Key）；gated 模式下 `/v1beta/models/…` 与
// `/v1beta1/projects/…` 完全不要求 access key（公开部署绕过面）。
//
// 新契约（本文件钉死）：
//   - 无 key → 401（Google 信封 code=401 / UNAUTHENTICATED）；
//   - `x-goog-api-key: <准入 key>` → 放行；`?key=<准入 key>` → 放行；
//   - 错误 key → 401；首请求带 key → 签发会话 cookie，此后无 key 凭 cookie 放行；
//   - 客户端 key 绝不进上游 URL/查询/头/体（上游凭据只来自配置）；
//   - `/v1beta` 裸路径与 `/v1beta2/*` 不进会话面；`/v1/chat/completions` 的
//     Bearer 行为不变；`/ui/api/config` 仍免 key（200）。
//
// 反证锚点（回退即红；原样输出见 tmp/lanes/L19-W68-3/proofs/）：
//   - sessionEligiblePath 撤掉 `/v1beta/` 前缀 → TestGatedV1BetaAccessKey 的
//     no_key_401 子例变 200（门消失）；
//   - accessKeyFromRequest 撤掉 `x-goog-api-key` / `?key=` 分支 →
//     header_key_200 / query_key_200 子例变 401（误拒 Gemini 客户端）；
//   - matchAccessKey 放行任意 key → wrong_key_401 子例变 200。
package service

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

// gatedGeminiCfg gated 公开面 + 单 gemini 上游（BaseURL 由调用方填假上游）。
// 上游凭据 = 配置值 `up-cfg-credential`（≠ 客户端准入 key——外泄断言对照面）。
func gatedGeminiCfg(key string) *Config {
	c := pubGatedCfg(key)
	c.Upstreams[0].BaseURL = "http://127.0.0.1:1"
	c.Upstreams[0].Protocol = ProtocolGemini
	c.Upstreams[0].APIKey = "up-cfg-credential"
	return c
}

// gatedRecorderUpstream 记录全量上游请求面（路径 / 原始查询 / 全头 / 体）的假
// Gemini 上游——供「客户端准入 key 绝不外泄上游」的全头扫描断言（比标准
// geminiUpstream 多记录全头）。非流式回 geminiTextResponse；`:streamGenerateContent`
// 回一帧良构 SSE（finishReason 终帧）。
type gatedRecorderUpstream struct {
	srv      *httptest.Server
	mu       sync.Mutex
	path     string
	rawQuery string
	header   http.Header
	body     []byte
	hits     int
}

func newGatedRecorderUpstream(t *testing.T) *gatedRecorderUpstream {
	t.Helper()
	u := &gatedRecorderUpstream{}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		u.mu.Lock()
		u.path, u.rawQuery, u.header, u.body = r.URL.Path, r.URL.RawQuery, r.Header.Clone(), b
		u.hits++
		u.mu.Unlock()
		if strings.Contains(r.URL.Path, ":streamGenerateContent") {
			w.Header().Set("Content-Type", "text/event-stream")
			sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"Hi"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(geminiTextResponse))
	}))
	t.Cleanup(u.srv.Close)
	return u
}

func (u *gatedRecorderUpstream) request() (path, rawQuery string, header http.Header, body []byte, hits int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.path, u.rawQuery, u.header, u.body, u.hits
}

// assertNoClientSecret 全头/查询/路径/体扫描：客户端准入 key 不得出现在上游
// 任何请求面（Google 客户端凭据纪律：只认配置凭据，透传 = 泄露面）。
func assertNoClientSecret(t *testing.T, secret string, path, rawQuery string, header http.Header, body []byte) {
	t.Helper()
	if strings.Contains(path, secret) {
		t.Fatalf("上游路径含客户端 key: %q", path)
	}
	if strings.Contains(rawQuery, secret) {
		t.Fatalf("上游查询含客户端 key: %q", rawQuery)
	}
	if strings.Contains(string(body), secret) {
		t.Fatalf("上游请求体含客户端 key: %s", body)
	}
	for name, vals := range header {
		for _, v := range vals {
			if strings.Contains(v, secret) {
				t.Fatalf("上游头 %s 含客户端 key: %q", name, v)
			}
		}
	}
}

// googleErrEnvelope 解析 Google 错误信封字段（code/status/message）。
func googleErrEnvelope(t *testing.T, raw []byte) (code int, status, message string) {
	t.Helper()
	var b struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatalf("错误信封不是 JSON: %v: %s", err, raw)
	}
	return b.Error.Code, b.Error.Status, b.Error.Message
}

// ---------------------------------------------------------------------------
// 1. gated：v1beta 无 key 401 / 双形态放行 / 错误 key 401
// ---------------------------------------------------------------------------

const (
	gatedV1BetaPath      = "/v1beta/models/gemini-2.5-flash:generateContent"
	gatedVertexPath      = "/v1beta1/projects/proj-1/locations/us-central1/publishers/google/models/gemini-2.5-flash:generateContent"
	gatedV1BetaReqBody   = `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`
	gatedV1BetaAccessKey = "sk-w68-gated-v1beta-a"
	gatedV1BetaWrongKey  = "sk-w68-gated-v1beta-WRONG"
	gatedUpstreamCred    = "up-cfg-credential"
)

// TestGatedV1BetaAccessKey 子测试：无 key 401（Gemini 与 Vertex 两形态）/
// `x-goog-api-key` 头 200 / `?key=` 200 / 错误 key 401 / 带 key 签发 cookie 后
// 无 key 凭 cookie 放行。断言面：Google 401 信封、零 Set-Cookie（拒面）、
// 拒面零上游命中、会话 key_id=sha8、响应绝不回显 key。
func TestGatedV1BetaAccessKey(t *testing.T) {
	newServer := func(t *testing.T) (*Server, *httptest.Server, *geminiUpstream) {
		t.Helper()
		up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(geminiTextResponse))
		})
		cfg := gatedGeminiCfg(gatedV1BetaAccessKey)
		cfg.Upstreams[0].BaseURL = up.srv.URL
		s, srv := startServiceWith(t, cfg)
		return s, srv, up
	}

	t.Run("no_key_401", func(t *testing.T) {
		s, srv, up := newServer(t)
		for _, path := range []string{gatedV1BetaPath, gatedVertexPath} {
			resp := postJSON(t, srv.URL+path, gatedV1BetaReqBody, nil)
			raw := pubReadBody(t, resp)
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("%s: status = %d（want 401）body=%s", path, resp.StatusCode, raw)
			}
			code, status, msg := googleErrEnvelope(t, raw)
			if code != 401 || status != "UNAUTHENTICATED" {
				t.Fatalf("%s: Google 信封 code=%d status=%q msg=%q（want 401/UNAUTHENTICATED）", path, code, status, msg)
			}
			if sc := resp.Header.Get("Set-Cookie"); sc != "" {
				t.Fatalf("%s: 401 不得 Set-Cookie: %q", path, sc)
			}
		}
		if s.sessions.len() != 0 {
			t.Fatalf("无 key 请求不得建会话: %d", s.sessions.len())
		}
		if _, _, _, _, hits := up.request(); hits != 0 {
			t.Fatalf("门控拒面不得打上游: hits=%d", hits)
		}
	})

	t.Run("header_key_200", func(t *testing.T) {
		s, srv, up := newServer(t)
		resp := postJSON(t, srv.URL+gatedV1BetaPath, gatedV1BetaReqBody,
			map[string]string{"x-goog-api-key": gatedV1BetaAccessKey})
		raw := pubReadBody(t, resp)
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), `"Hi there"`) {
			t.Fatalf("x-goog-api-key 应放行: status=%d body=%s", resp.StatusCode, raw)
		}
		if strings.Contains(string(raw), gatedV1BetaAccessKey) {
			t.Fatalf("响应体不得回显准入 key")
		}
		sid := sessionCookieValue(resp)
		if sid == "" {
			t.Fatal("带 key 首请求应签发会话 cookie")
		}
		if rec, ok := s.sessions.get(sid, s.sessions.now()); !ok || rec.keyID != testSha8(gatedV1BetaAccessKey) {
			t.Fatalf("会话 key_id = %+v（want sha8=%q）", rec, testSha8(gatedV1BetaAccessKey))
		}
		if _, _, _, _, hits := up.request(); hits != 1 {
			t.Fatalf("上游命中 = %d（want 1）", hits)
		}

		// cookie 语义（gated 与 /v1/* 一致）：其后无 key、仅凭 cookie 放行。
		resp = postJSON(t, srv.URL+gatedV1BetaPath, gatedV1BetaReqBody,
			map[string]string{"Cookie": sessionCookieName + "=" + sid})
		raw = pubReadBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("cookie-only status = %d body=%s", resp.StatusCode, raw)
		}
		if sc := resp.Header.Get("Set-Cookie"); sc != "" {
			t.Fatalf("cookie-only 不应重发 Set-Cookie: %q", sc)
		}
	})

	t.Run("query_key_200", func(t *testing.T) {
		_, srv, up := newServer(t)
		resp := postJSON(t, srv.URL+gatedV1BetaPath+"?key="+gatedV1BetaAccessKey, gatedV1BetaReqBody, nil)
		raw := pubReadBody(t, resp)
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), `"Hi there"`) {
			t.Fatalf("?key= 应放行: status=%d body=%s", resp.StatusCode, raw)
		}
		if strings.Contains(string(raw), gatedV1BetaAccessKey) {
			t.Fatalf("响应体不得回显准入 key")
		}
		if sessionCookieValue(resp) == "" {
			t.Fatal("?key= 首请求应签发会话 cookie")
		}
		if path, query, _, body, hits := up.request(); hits != 1 {
			t.Fatalf("上游命中 = %d（want 1）；path=%q query=%q", hits, path, query)
		} else {
			assertNoClientSecret(t, gatedV1BetaAccessKey, path, query, nil, body)
		}
	})

	t.Run("wrong_key_401", func(t *testing.T) {
		s, srv, up := newServer(t)
		// 头形态错误 key。
		resp := postJSON(t, srv.URL+gatedV1BetaPath, gatedV1BetaReqBody,
			map[string]string{"x-goog-api-key": gatedV1BetaWrongKey})
		raw := pubReadBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("错误 key（头）status = %d（want 401）body=%s", resp.StatusCode, raw)
		}
		if code, status, _ := googleErrEnvelope(t, raw); code != 401 || status != "UNAUTHENTICATED" {
			t.Fatalf("错误 key（头）信封 code=%d status=%q", code, status)
		}
		// 查询形态错误 key。
		resp = postJSON(t, srv.URL+gatedV1BetaPath+"?key="+gatedV1BetaWrongKey, gatedV1BetaReqBody, nil)
		raw = pubReadBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("错误 key（查询）status = %d（want 401）body=%s", resp.StatusCode, raw)
		}
		if sc := resp.Header.Get("Set-Cookie"); sc != "" {
			t.Fatalf("错误 key 不得 Set-Cookie: %q", sc)
		}
		if s.sessions.len() != 0 {
			t.Fatalf("错误 key 不得建会话: %d", s.sessions.len())
		}
		if _, _, _, _, hits := up.request(); hits != 0 {
			t.Fatalf("门控拒面不得打上游: hits=%d", hits)
		}
	})
}

// ---------------------------------------------------------------------------
// 2. gated：客户端 key 绝不进上游（URL/查询/头/体）
// ---------------------------------------------------------------------------

// TestGatedV1BetaKeyNotForwarded 假上游全量记录：`?key=` 与 `x-goog-api-key`
// 的准入 key 都不出现在上游路径/查询/头/体；上游认证头只用配置凭据；流式
// 请求的 `alt=sse` 仍透传（唯一被读取的查询参数）。
func TestGatedV1BetaKeyNotForwarded(t *testing.T) {
	up := newGatedRecorderUpstream(t)
	cfg := gatedGeminiCfg(gatedV1BetaAccessKey)
	cfg.Upstreams[0].BaseURL = up.srv.URL
	_, srv := startServiceWith(t, cfg)

	// 1) 查询形态（非流式）：上游查询串必须为空（`key` 不转发）。
	resp := postJSON(t, srv.URL+gatedV1BetaPath+"?key="+gatedV1BetaAccessKey, gatedV1BetaReqBody, nil)
	raw := pubReadBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("query-key status = %d body=%s", resp.StatusCode, raw)
	}
	path, query, header, body, hits := up.request()
	if hits != 1 {
		t.Fatalf("上游命中 = %d（want 1）", hits)
	}
	if path != "/v1beta/models/gemini-2.5-flash:generateContent" || query != "" {
		t.Fatalf("上游端点 = %q?%q（want 无查询）", path, query)
	}
	assertNoClientSecret(t, gatedV1BetaAccessKey, path, query, header, body)
	if got := header.Get("x-goog-api-key"); got != gatedUpstreamCred {
		t.Fatalf("上游 x-goog-api-key = %q（want 配置凭据 %q——绝不用客户端 key）", got, gatedUpstreamCred)
	}

	// 2) 查询形态（流式 + alt=sse）：只留 `alt=sse`，`key` 不转发。
	resp = postJSON(t, srv.URL+"/v1beta/models/gemini-2.5-flash:streamGenerateContent?key="+gatedV1BetaAccessKey+"&alt=sse", gatedV1BetaReqBody, nil)
	raw = pubReadBody(t, resp)
	if resp.StatusCode != 200 || !strings.Contains(string(raw), `"Hi"`) {
		t.Fatalf("stream status = %d body=%s", resp.StatusCode, raw)
	}
	path, query, header, body, hits = up.request()
	if hits != 2 {
		t.Fatalf("上游命中 = %d（want 2）", hits)
	}
	if path != "/v1beta/models/gemini-2.5-flash:streamGenerateContent" || query != "alt=sse" {
		t.Fatalf("流式上游端点 = %q?%q（want alt=sse 唯一查询）", path, query)
	}
	assertNoClientSecret(t, gatedV1BetaAccessKey, path, query, header, body)

	// 3) 头形态 + Vertex 路径：上游认证头仍只用配置凭据。
	resp = postJSON(t, srv.URL+gatedVertexPath, gatedV1BetaReqBody,
		map[string]string{"x-goog-api-key": gatedV1BetaAccessKey})
	raw = pubReadBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("vertex header-key status = %d body=%s", resp.StatusCode, raw)
	}
	path, query, header, body, hits = up.request()
	if hits != 3 {
		t.Fatalf("上游命中 = %d（want 3）", hits)
	}
	if path != "/v1beta/models/gemini-2.5-flash:generateContent" {
		t.Fatalf("Vertex 上游端点 = %q（p/l 段不进上游）", path)
	}
	assertNoClientSecret(t, gatedV1BetaAccessKey, path, query, header, body)
	if got := header.Get("x-goog-api-key"); got != gatedUpstreamCred {
		t.Fatalf("上游 x-goog-api-key = %q（want 配置凭据）", got)
	}
}

// ---------------------------------------------------------------------------
// 3. sessionEligiblePath：v1beta 族口径（进面 / 不误伤）
// ---------------------------------------------------------------------------

// TestSessionEligiblePathV1Beta 会话面口径（W68-3）：`/v1beta/` 与
// `/v1beta1/` 进面；`/v1beta` 裸路径（404 非入口）、`/v1beta2/*`（未注册前缀）、
// `/ui/api/config`（前端引导）不进面；`/v1/*` 与 `/ui/api/*` 保持进面。
func TestSessionEligiblePathV1Beta(t *testing.T) {
	eligible := []string{
		"/v1beta/models/gemini-2.5-flash:generateContent",
		"/v1beta/models/gemini-2.5-flash:streamGenerateContent",
		"/v1beta/models", // 前缀面（注册树 /v1beta/models/）
		"/v1beta1/projects/p/locations/us-central1/publishers/google/models/gemini-2.5-flash:generateContent",
		"/v1/projects/p/locations/us/publishers/google/models/gemini-2.5-flash:generateContent",
		"/v1/chat/completions",
		"/v1/models",
		"/ui/api/sessions",
	}
	for _, p := range eligible {
		if !sessionEligiblePath(p) {
			t.Fatalf("%s: want eligible（W68-3 会话面）", p)
		}
	}
	notEligible := []string{
		"/ui/api/config",
		"/v1beta",           // 裸路径：未注册（404），不是入口——不占门控面
		"/v1beta2/models/x", // 未注册前缀——不误伤
		"/v1beta1",
		"/ui",
		"/ui/",
		"/healthz",
		"/admin/logs",
	}
	for _, p := range notEligible {
		if sessionEligiblePath(p) {
			t.Fatalf("%s: want NOT eligible", p)
		}
	}
}

// ---------------------------------------------------------------------------
// 4. 回归：/v1/chat/completions Bearer 不变、/ui/api/config 免 key、public off 零变化
// ---------------------------------------------------------------------------

// TestGatedV1BetaChatAndConfigRegressions gated 面既有行为不变：
// `/v1/chat/completions` Bearer 仍 200（chat 链正常）；`/ui/api/config` 无 key
// 仍 200 且不建会话。
func TestGatedV1BetaChatAndConfigRegressions(t *testing.T) {
	up := goodClaudeUpstream(t)
	cfg := pubGatedCfg(gatedV1BetaAccessKey)
	cfg.Upstreams[0].BaseURL = up.srv.URL
	s, srv := startServiceWith(t, cfg)

	// Bearer（既有形态）→ 200 + 正常流 + 会话签发。
	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`,
		map[string]string{"Authorization": "Bearer " + gatedV1BetaAccessKey})
	raw := pubReadBody(t, resp)
	if resp.StatusCode != 200 || !strings.Contains(string(raw), "Hello ") {
		t.Fatalf("Bearer chat status=%d body=%s", resp.StatusCode, raw)
	}
	if sessionCookieValue(resp) == "" {
		t.Fatal("Bearer 首请求应签发会话")
	}

	// /ui/api/config 免 key（前端引导配置）→ 200、零 Set-Cookie、零会话增量。
	before := s.sessions.len()
	resp = pubReq(t, http.MethodGet, srv.URL+"/ui/api/config", nil)
	raw = pubReadBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("config status = %d（want 200 免 key）body=%s", resp.StatusCode, raw)
	}
	if sc := resp.Header.Get("Set-Cookie"); sc != "" {
		t.Fatalf("config 不得 Set-Cookie: %q", sc)
	}
	if s.sessions.len() != before {
		t.Fatalf("config 不得建会话: %d → %d", before, s.sessions.len())
	}
}

// TestGatedV1BetaPublicOffZeroRegression 硬门：缺省 public.enabled=false（无
// public 段）时 v1beta 行为与现状逐字一致——无门控（无 key 200）、无会话
// （零 Set-Cookie）、客户端 `?key=` 仍被忽略且绝不转发上游。
func TestGatedV1BetaPublicOffZeroRegression(t *testing.T) {
	up := newGatedRecorderUpstream(t)
	cfg := uiTestConfig() // 无 public 段（缺省 off；协议换 gemini 接假上游）
	cfg.Upstreams[0].BaseURL = up.srv.URL
	cfg.Upstreams[0].Protocol = ProtocolGemini
	s, srv := startServiceWith(t, cfg)
	if s.sessions != nil || s.accessGated {
		t.Fatalf("public off 不得挂会话/门控（sessions=%v gated=%v）", s.sessions != nil, s.accessGated)
	}

	const clientKey = "SECRET-CLIENT-KEY-NOT-FOR-UPSTREAM"
	resp := postJSON(t, srv.URL+gatedV1BetaPath+"?key="+clientKey, gatedV1BetaReqBody, nil)
	raw := pubReadBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("public off 的 v1beta status = %d（want 200：零门控）body=%s", resp.StatusCode, raw)
	}
	if sc := resp.Header.Get("Set-Cookie"); sc != "" {
		t.Fatalf("public off 不得 Set-Cookie: %q", sc)
	}
	path, query, header, body, hits := up.request()
	if hits != 1 {
		t.Fatalf("上游命中 = %d（want 1）", hits)
	}
	if query != "" {
		t.Fatalf("上游查询 = %q（want 空：public off 时 ?key= 仍被忽略）", query)
	}
	assertNoClientSecret(t, clientKey, path, query, header, body)
}

// ---------------------------------------------------------------------------
// 5. 提取面纪律：先到先得（不回落）+ `?key=` 仅限 Gemini 路径
// ---------------------------------------------------------------------------

// TestGatedV1BetaKeyPrecedenceAndConfinement 提取顺序纪律（W68-3）：
//
//  1. 先到先得、无回落——错误的 `X-Access-Key` 先于正确的 `?key=` 被提取 →
//     401（错值不会被后续来源「覆盖」；凭据混淆面无）；
//  2. `?key=` 仅对 `/v1beta/`、`/v1beta1/` 生效——`/v1/chat/completions?key=
//     <正确 key>` 仍 401（普通查询参数不得被当作本服务凭据）；
//  3. `Authorization` 在链首：非 Bearer 一律拒（不回落）；Bearer 值即准入
//     候选——携带第三方 OAuth token 的 Vertex 风格调用链必须不带
//     Authorization，否则 Bearer 值先到先得、`?key=` 不会兜底（边界记录）。
func TestGatedV1BetaKeyPrecedenceAndConfinement(t *testing.T) {
	// 1) v1beta：错误 X-Access-Key + 正确 ?key= → 401（先到先得，不回落）。
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(geminiTextResponse))
	})
	cfg := gatedGeminiCfg(gatedV1BetaAccessKey)
	cfg.Upstreams[0].BaseURL = up.srv.URL
	_, srv := startServiceWith(t, cfg)

	resp := postJSON(t, srv.URL+gatedV1BetaPath+"?key="+gatedV1BetaAccessKey, gatedV1BetaReqBody,
		map[string]string{"X-Access-Key": gatedV1BetaWrongKey})
	raw := pubReadBody(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("错误 X-Access-Key + 正确 ?key= status = %d（want 401：先到先得不回落）body=%s", resp.StatusCode, raw)
	}
	if _, _, _, _, hits := up.request(); hits != 0 {
		t.Fatalf("拒面不得打上游: hits=%d", hits)
	}

	// 1b) 第三方 OAuth Bearer + 正确 ?key= → 401（Bearer 先到先得，不回落）。
	resp = postJSON(t, srv.URL+gatedV1BetaPath+"?key="+gatedV1BetaAccessKey, gatedV1BetaReqBody,
		map[string]string{"Authorization": "Bearer thirdparty-oauth-token"})
	raw = pubReadBody(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("第三方 Bearer + 正确 ?key= status = %d（want 401：Bearer 先到先得）body=%s", resp.StatusCode, raw)
	}

	// 1c) 非 Bearer Authorization → 拒绝（不回落 Gemini 来源，即使 ?key= 正确）。
	resp = postJSON(t, srv.URL+gatedV1BetaPath+"?key="+gatedV1BetaAccessKey, gatedV1BetaReqBody,
		map[string]string{"Authorization": "Basic dXNlcjpwdw=="})
	raw = pubReadBody(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("Basic Authorization + 正确 ?key= status = %d（want 401：非 Bearer 直接拒）body=%s", resp.StatusCode, raw)
	}
	if _, _, _, _, hits := up.request(); hits != 0 {
		t.Fatalf("拒面不得打上游: hits=%d", hits)
	}

	// 2) 非 Gemini 路径不读 `?key=`：`/v1/chat/completions?key=<正确 key>` → 401。
	chatUp := goodClaudeUpstream(t)
	chatCfg := pubGatedCfg(gatedV1BetaAccessKey)
	chatCfg.Upstreams[0].BaseURL = chatUp.srv.URL
	_, chatSrv := startServiceWith(t, chatCfg)
	chat := `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`
	resp = postJSON(t, chatSrv.URL+"/v1/chat/completions?key="+gatedV1BetaAccessKey, chat, nil)
	raw = pubReadBody(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("chat?key= status = %d（want 401：?key= 仅 Gemini 路径读取）body=%s", resp.StatusCode, raw)
	}
	if chatUp.count() != 0 {
		t.Fatalf("chat 拒面不得打上游: hits=%d", chatUp.count())
	}
}

// TestGatedV1BetaUnregisteredPrefixesNotGated gated 模式不误伤未注册前缀：
// `/v1beta` 裸路径与 `/v1beta2/*` 不是入口（无路由）→ 404（**不是 401**——
// 不进会话面/门控面）；两者都不打上游。
func TestGatedV1BetaUnregisteredPrefixesNotGated(t *testing.T) {
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(geminiTextResponse))
	})
	cfg := gatedGeminiCfg(gatedV1BetaAccessKey)
	cfg.Upstreams[0].BaseURL = up.srv.URL
	_, srv := startServiceWith(t, cfg)

	for _, p := range []string{"/v1beta", "/v1beta2/models/gemini-x:generateContent"} {
		resp := pubReq(t, http.MethodGet, srv.URL+p, nil)
		raw := pubReadBody(t, resp)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s status = %d（want 404：未注册前缀不进会话面）body=%s", p, resp.StatusCode, raw)
		}
		if sc := resp.Header.Get("Set-Cookie"); sc != "" {
			t.Fatalf("%s 不得 Set-Cookie（未进会话面）: %q", p, sc)
		}
	}
	if _, _, _, _, hits := up.request(); hits != 0 {
		t.Fatalf("未注册前缀不得打上游: hits=%d", hits)
	}
}
