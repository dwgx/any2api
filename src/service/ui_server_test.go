package service

// ui_server_test.go —— UI 托管面测试（W56-1）：
// /ui 200、静态文件（字节回读 + Content-Type）、目录穿越防护（反证）、SPA 回退、
// /ui/api/config 形状、限流（允许/拒绝/恢复/并发安全/清扫/每 IP 独立；反证）。
//
// 反证口径：穿越尝试、负值配置、enabled=false、超限放行——任一被放行即红。

import (
	"bytes"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

// uiTestConfig 最小可跑配置（单上游占位；不触网）。
func uiTestConfig() *Config {
	return &Config{
		Listen: "127.0.0.1:0",
		Upstreams: []Upstream{{
			Name: "fake", BaseURL: "http://127.0.0.1:1", APIKey: "test-key", Protocol: ProtocolAnthropic,
		}},
	}
}

// uiGet 发 GET（**不跟随重定向**——穿越断言要看原始响应）。
func uiGet(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do %s: %v", url, err)
	}
	return resp
}

// uiBody 读尽响应体并关闭。
func uiBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}

// embeddedUIFile 读嵌入资产（测试对照面）。
func embeddedUIFile(t *testing.T, name string) []byte {
	t.Helper()
	b, err := fs.ReadFile(uiAssets, name)
	if err != nil {
		t.Fatalf("read embedded %s: %v", name, err)
	}
	return b
}

// fakeClock 可注入时钟（限流测试；并发安全）。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// ---------------------------------------------------------------------------
// 静态托管
// ---------------------------------------------------------------------------

// TestUIServesIndex /ui 与 /ui/ → index.html（200 + text/html + 字节相等；HEAD 无体）。
func TestUIServesIndex(t *testing.T) {
	ts := startService(t, uiTestConfig())
	index := embeddedUIFile(t, "ui/index.html")
	for _, p := range []string{"/ui", "/ui/"} {
		resp := uiGet(t, ts.URL+p)
		body := uiBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d", p, resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
			t.Fatalf("%s: content-type %q", p, ct)
		}
		if !bytes.Equal(body, index) {
			t.Fatalf("%s: body != index.html", p)
		}
	}
	// HEAD：仅头无体。
	req, err := http.NewRequest(http.MethodHead, ts.URL+"/ui", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD /ui: status %d", resp.StatusCode)
	}
	if n := len(uiBody(t, resp)); n != 0 {
		t.Fatalf("HEAD /ui: body %d bytes", n)
	}
}

// TestUIServesStaticFiles 显式 index.html + 全部嵌入资产逐一回读（字节相等；
// css/js/html 按扩展名断言 Content-Type）。
func TestUIServesStaticFiles(t *testing.T) {
	ts := startService(t, uiTestConfig())

	resp := uiGet(t, ts.URL+"/ui/index.html")
	body := uiBody(t, resp)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, embeddedUIFile(t, "ui/index.html")) {
		t.Fatalf("GET /ui/index.html: status %d, %d bytes", resp.StatusCode, len(body))
	}

	var files []string
	err := fs.WalkDir(uiAssets, "ui", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk ui assets: %v", err)
	}
	if len(files) < 2 {
		t.Fatalf("expected >=2 embedded files, got %d", len(files))
	}
	for _, f := range files {
		rel := strings.TrimPrefix(f, "ui/")
		if !uiTestURLSafe(rel) {
			t.Logf("skip (needs escaping): %s", rel)
			continue
		}
		want := embeddedUIFile(t, f)
		resp := uiGet(t, ts.URL+"/ui/"+rel)
		got := uiBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d", rel, resp.StatusCode)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: body mismatch (%d vs %d bytes)", rel, len(got), len(want))
		}
		ct := resp.Header.Get("Content-Type")
		switch {
		case strings.HasSuffix(rel, ".css"):
			if !strings.Contains(ct, "css") {
				t.Fatalf("%s: content-type %q", rel, ct)
			}
		case strings.HasSuffix(rel, ".js"):
			if !strings.Contains(ct, "javascript") {
				t.Fatalf("%s: content-type %q", rel, ct)
			}
		case strings.HasSuffix(rel, ".html"):
			if !strings.HasPrefix(ct, "text/html") {
				t.Fatalf("%s: content-type %q", rel, ct)
			}
		}
	}
}

// uiTestURLSafe 资产名是否可直接拼进测试 URL（跳过需转义的怪名）。
func uiTestURLSafe(rel string) bool {
	for _, r := range rel {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.' || r == '_' || r == '-' || r == '/':
		default:
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// 穿越防护 / SPA 回退
// ---------------------------------------------------------------------------

// TestUITraversalBlocked 目录穿越防护（反证：以下任一泄漏即红）。
func TestUITraversalBlocked(t *testing.T) {
	ts := startService(t, uiTestConfig())
	leak := []byte("module github.com/dwgx/any2api") // src/go.mod 首行——穿越目标

	// ① 带转义 %2e%2e：ServeMux 清洗不到（转义形态），解码后到达处理器，
	//    由显式守卫 400 拦下。
	for _, p := range []string{
		"/ui/%2e%2e/go.mod",
		"/ui/%2e%2e%2fgo.mod",
		"/ui/%2e%2e/%2e%2e/go.mod",
		"/ui/%2e%2e/config.example.yaml",
	} {
		resp := uiGet(t, ts.URL+p)
		body := uiBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s: want 400, got %d", p, resp.StatusCode)
		}
		if bytes.Contains(body, leak) || bytes.Contains(body, []byte("listen:")) {
			t.Fatalf("%s: leaked file content", p)
		}
	}

	// ② 明文 ../：ServeMux 先清洗（307 重定向到等价干净 URL /go.mod）——
	//    原始响应绝不带文件内容；重定向目标不含 ..
	resp := uiGet(t, ts.URL+"/ui/../go.mod")
	body := uiBody(t, resp)
	if bytes.Contains(body, leak) {
		t.Fatalf("plain ../ leaked file content")
	}
	if resp.StatusCode != http.StatusTemporaryRedirect || resp.Header.Get("Location") != "/go.mod" {
		t.Fatalf("/ui/../go.mod: status %d location %q (want 307 -> /go.mod)", resp.StatusCode, resp.Header.Get("Location"))
	}
	// 跟随重定向的最终落点也不泄漏。
	final, err := http.Get(ts.URL + "/ui/../go.mod")
	if err != nil {
		t.Fatalf("follow: %v", err)
	}
	if bytes.Contains(uiBody(t, final), leak) {
		t.Fatalf("followed redirect leaked content")
	}

	// ③ 处理器层直接调用（绕过 ServeMux 清洗——防御纵深本体检证）。
	srv := New(uiTestConfig())
	req := httptest.NewRequest(http.MethodGet, "/ui/../go.mod", nil)
	rec := httptest.NewRecorder()
	srv.handleUIAssets(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("direct handler: want 400, got %d", rec.Code)
	}
	if bytes.Contains(rec.Body.Bytes(), leak) {
		t.Fatalf("direct handler leaked content")
	}
}

// TestUISpaFallback 未知路径回退 index.html（SPA）。
func TestUISpaFallback(t *testing.T) {
	ts := startService(t, uiTestConfig())
	index := embeddedUIFile(t, "ui/index.html")
	for _, p := range []string{"/ui/chat/abc", "/ui/a/b/c", "/ui/some/deep/route/", "/ui/no-such-file.js", "/ui/help/"} {
		resp := uiGet(t, ts.URL+p)
		body := uiBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d", p, resp.StatusCode)
		}
		if !bytes.Equal(body, index) {
			t.Fatalf("%s: not the index fallback", p)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
			t.Fatalf("%s: content-type %q", p, ct)
		}
	}
}

// ---------------------------------------------------------------------------
// /ui/api/config
// ---------------------------------------------------------------------------

// TestUIConfigShape /ui/api/config 形状与数据源（无配置 = 空数组；配置 = 表首为 default）。
func TestUIConfigShape(t *testing.T) {
	// 无配置：models 为 []（非 null）、default_model 空、限流 0。
	ts := startService(t, uiTestConfig())
	resp := uiGet(t, ts.URL+"/ui/api/config")
	body := uiBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type %q", ct)
	}
	if !strings.Contains(string(body), `"models":[]`) {
		t.Fatalf("want empty array (not null): %s", body)
	}
	var cfg struct {
		DefaultModel    string   `json:"default_model"`
		Models          []string `json:"models"`
		RateLimitPerMin int      `json:"rate_limit_per_min"`
	}
	if err := json.Unmarshal(body, &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cfg.DefaultModel != "" || len(cfg.Models) != 0 || cfg.RateLimitPerMin != 0 {
		t.Fatalf("unconfigured: %+v", cfg)
	}

	// 配置 models + 限流：id 清单与发现面同源；default = 表首条。
	cfg2 := uiTestConfig()
	cfg2.Models = []ModelEntry{{ID: "claude-sonnet-5-5"}, {ID: "claude-haiku-5-5"}}
	cfg2.UI = &UISection{RateLimitPerMin: 30}
	ts2 := startService(t, cfg2)
	resp2 := uiGet(t, ts2.URL+"/ui/api/config")
	body2 := uiBody(t, resp2)
	if err := json.Unmarshal(body2, &cfg); err != nil {
		t.Fatalf("unmarshal2: %v", err)
	}
	if cfg.DefaultModel != "claude-sonnet-5-5" {
		t.Fatalf("default_model %q", cfg.DefaultModel)
	}
	if len(cfg.Models) != 2 || cfg.Models[0] != "claude-sonnet-5-5" || cfg.Models[1] != "claude-haiku-5-5" {
		t.Fatalf("models %v", cfg.Models)
	}
	if cfg.RateLimitPerMin != 30 {
		t.Fatalf("rate_limit_per_min %d", cfg.RateLimitPerMin)
	}

	// 非 GET → 405 + Allow: GET。
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/ui/api/config", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp3, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_ = uiBody(t, resp3)
	if resp3.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST: status %d", resp3.StatusCode)
	}
	if allow := resp3.Header.Get("Allow"); allow != "GET" {
		t.Fatalf("Allow %q", allow)
	}
}

// ---------------------------------------------------------------------------
// 限流
// ---------------------------------------------------------------------------

// TestUIRateLimitAllowDenyRecover 限流行为（允许→拒绝→恢复）+ 作用面。
func TestUIRateLimitAllowDenyRecover(t *testing.T) {
	cfg := uiTestConfig()
	cfg.UI = &UISection{RateLimitPerMin: 2}
	srv := New(cfg)
	clock := &fakeClock{t: time.Unix(1700000000, 0)}
	srv.uiLimiter.now = clock.Now
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	// 容量 2：两发放行。
	for i := 1; i <= 2; i++ {
		resp := uiGet(t, ts.URL+"/ui/api/config")
		_ = uiBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status %d", i, resp.StatusCode)
		}
	}
	// 第 3 发拒绝：429 + Retry-After=30（等 1 枚令牌 = 60/2 秒）+ JSON 错误体。
	resp := uiGet(t, ts.URL+"/ui/api/config")
	body := uiBody(t, resp)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("3rd: status %d", resp.StatusCode)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "30" {
		t.Fatalf("Retry-After %q", ra)
	}
	if !strings.Contains(string(body), "rate_limit_error") {
		t.Fatalf("429 body: %s", body)
	}

	// 拒绝面覆盖 /v1/*（信封按入口协议）：/v1/models 为 claude 信封。
	resp = uiGet(t, ts.URL+"/v1/models")
	body = uiBody(t, resp)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("/v1/models: status %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), `"type":"error"`) || !strings.Contains(string(body), "rate_limit_error") {
		t.Fatalf("/v1/models 429 body: %s", body)
	}
	// openai 入口面信封（被限时绝不触上游）。
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	body2 := uiBody(t, resp2)
	if resp2.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("/v1/chat/completions: status %d", resp2.StatusCode)
	}
	if !strings.Contains(string(body2), "rate_limit_error") {
		t.Fatalf("chat 429 body: %s", body2)
	}

	// A13：/v1beta* 入口族（Gemini `/v1beta/models/` 与 Vertex
	// `/v1beta1/projects/`）同属限流面；429 信封按 Gemini 协议
	// （Google 形：code=429 / status=RESOURCE_EXHAUSTED）。
	for _, p := range []string{
		"/v1beta/models/gemini-2.5-flash:generateContent",
		"/v1beta1/projects/p/locations/us/publishers/google/models/gemini-2.5-flash:generateContent",
	} {
		req, err := http.NewRequest(http.MethodPost, ts.URL+p, strings.NewReader(`{"contents":[]}`))
		if err != nil {
			t.Fatalf("new request %s: %v", p, err)
		}
		req.Header.Set("Content-Type", "application/json")
		respB, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post %s: %v", p, err)
		}
		bodyB := uiBody(t, respB)
		if respB.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("%s: status %d (want 429)", p, respB.StatusCode)
		}
		if !strings.Contains(string(bodyB), `"status":"RESOURCE_EXHAUSTED"`) ||
			!strings.Contains(string(bodyB), `"code":429`) {
			t.Fatalf("%s: gemini 429 body: %s", p, bodyB)
		}
	}

	// 未限面：/healthz 与 /ui 静态资产直过。
	resp = uiGet(t, ts.URL+"/healthz")
	_ = uiBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz: status %d", resp.StatusCode)
	}
	resp = uiGet(t, ts.URL+"/ui/")
	_ = uiBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/ui/: status %d", resp.StatusCode)
	}

	// 恢复：推进 31s（> 30s 等 1 枚令牌）→ 放行。
	clock.Advance(31 * time.Second)
	resp = uiGet(t, ts.URL+"/ui/api/config")
	_ = uiBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("recover: status %d", resp.StatusCode)
	}
}

// TestRateLimitedPathV1Beta 限流面覆盖 /v1beta*（A13）：Gemini/Vertex 入口族
// 纳入；未注册前缀（`/v1beta` 裸路径、`/v1beta2/*`）与静态/管理面不误伤。
func TestRateLimitedPathV1Beta(t *testing.T) {
	limited := []string{
		"/v1/chat/completions",
		"/v1/models",
		"/ui/api/config",
		"/v1beta/models/gemini-2.5-flash:generateContent",
		"/v1beta/models/gemini-2.5-flash:streamGenerateContent",
		"/v1beta1/projects/p/locations/us/publishers/google/models/gemini-2.5-flash:generateContent",
	}
	for _, p := range limited {
		if !rateLimitedPath(p) {
			t.Fatalf("%s: want limited", p)
		}
	}
	free := []string{"/ui", "/ui/", "/ui/app.js", "/healthz", "/admin/logs", "/v1beta", "/v1beta2/models/x"}
	for _, p := range free {
		if rateLimitedPath(p) {
			t.Fatalf("%s: want unlimited", p)
		}
	}
	// 429 信封映射：v1beta 命名空间 = Gemini 协议（Google 信封）。
	for _, p := range []string{
		"/v1beta/models/gemini-2.5-flash:generateContent",
		"/v1beta1/projects/p/locations/us/publishers/google/models/gemini-2.5-flash:generateContent",
		"/v1/projects/p/locations/us/publishers/google/models/gemini-2.5-flash:generateContent",
	} {
		if got := rateLimitFormat(p); got != registry.FormatGeminiGenerate {
			t.Fatalf("%s: format %q, want gemini", p, got)
		}
	}
	// 会话面（W68-3 起）覆盖 /v1beta* 入口族——gated 模式下不再有 v1beta
	// 绕过 access key 的缺口（key 面含 Gemini 原生 x-goog-api-key / ?key=）；
	// 未注册前缀（/v1beta 裸路径、/v1beta2/*）仍不进会话面。全量口径见
	// session_gated_test.go TestSessionEligiblePathV1Beta。
	if !sessionEligiblePath("/v1beta/models/gemini-2.5-flash:generateContent") {
		t.Fatal("v1beta 入口族应进入会话面（W68-3）")
	}
	if sessionEligiblePath("/v1beta") || sessionEligiblePath("/v1beta2/models/x") {
		t.Fatal("未注册前缀不得进入会话面")
	}
	if !sessionEligiblePath("/v1/models") || !sessionEligiblePath("/ui/api/sessions") {
		t.Fatal("v1/* 与 /ui/api/* 应保持会话面")
	}
}

// TestUIRateLimiterUnit 桶语义（注入时钟）：容量/恢复/每 IP 独立/清扫。
func TestUIRateLimiterUnit(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1700000000, 0)}
	lim := newIPRateLimiter(2)
	lim.now = clock.Now

	// 容量 2：两发放行、第三发拒绝且等待 30s。
	if ok, _ := lim.allow("a"); !ok {
		t.Fatal("1st want allow")
	}
	if ok, _ := lim.allow("a"); !ok {
		t.Fatal("2nd want allow")
	}
	if ok, retry := lim.allow("a"); ok {
		t.Fatal("3rd want deny")
	} else if retry != 30*time.Second {
		t.Fatalf("retry %v", retry)
	}

	// 每 IP 独立：b 不受 a 影响。
	if ok, _ := lim.allow("b"); !ok {
		t.Fatal("b want allow")
	}

	// 恢复：a 等 31s 后放行；随即再发又拒（回填的令牌已用掉）。
	clock.Advance(31 * time.Second)
	if ok, _ := lim.allow("a"); !ok {
		t.Fatal("recover want allow")
	}
	if ok, _ := lim.allow("a"); ok {
		t.Fatal("want deny right after spend")
	}

	// 清扫：闲置 ≥ 2×窗（120s）的桶被丢弃（推进 3 分钟后由新键触发）。
	clock.Advance(3 * time.Minute)
	if ok, _ := lim.allow("c"); !ok {
		t.Fatal("c want allow")
	}
	lim.mu.Lock()
	_, hasA := lim.buckets["a"]
	_, hasB := lim.buckets["b"]
	_, hasC := lim.buckets["c"]
	n := len(lim.buckets)
	lim.mu.Unlock()
	if hasA || hasB || !hasC || n != 1 {
		t.Fatalf("sweep: hasA=%v hasB=%v hasC=%v n=%d", hasA, hasB, hasC, n)
	}
}

// TestUIRateLimiterConcurrent 并发安全：容量 50、200 并发恰好放行 50（冻结时钟）。
func TestUIRateLimiterConcurrent(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1700000000, 0)}
	lim := newIPRateLimiter(50)
	lim.now = clock.Now
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := lim.allow("k"); ok {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != 50 {
		t.Fatalf("allowed %d, want exactly 50", got)
	}
}

// ---------------------------------------------------------------------------
// 配置面 / 反证
// ---------------------------------------------------------------------------

// TestUIDisabled ui.enabled=false → /ui/* 全部 404（反证：路由不注册）。
func TestUIDisabled(t *testing.T) {
	cfg := uiTestConfig()
	off := false
	cfg.UI = &UISection{Enabled: &off}
	ts := startService(t, cfg)
	for _, p := range []string{"/ui", "/ui/", "/ui/app.js", "/ui/api/config"} {
		resp := uiGet(t, ts.URL+p)
		_ = uiBody(t, resp)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s: want 404, got %d", p, resp.StatusCode)
		}
	}
}

// TestUIRateLimitOff 缺省（rate_limit_per_min=0）不限流（反证：无 429）。
func TestUIRateLimitOff(t *testing.T) {
	ts := startService(t, uiTestConfig())
	for i := range 10 {
		resp := uiGet(t, ts.URL+"/v1/models")
		_ = uiBody(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status %d (want unlimited)", i+1, resp.StatusCode)
		}
	}
}

// TestUIConfigValidation ui 段校验与缺省口径（反证：负值显式报错）。
func TestUIConfigValidation(t *testing.T) {
	cfg := uiTestConfig()
	cfg.UI = &UISection{RateLimitPerMin: -1}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "ui.rate_limit_per_min") {
		t.Fatalf("want ui.rate_limit_per_min error, got %v", err)
	}
	cfg.UI = &UISection{RateLimitPerMin: 0}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("rate 0: %v", err)
	}
	// 缺省口径：无 ui 段 = enabled true + 限流 0。
	cfg.UI = nil
	if !cfg.uiEnabled() || cfg.uiRateLimitPerMin() != 0 {
		t.Fatalf("defaults: enabled=%v rate=%d", cfg.uiEnabled(), cfg.uiRateLimitPerMin())
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("nil ui: %v", err)
	}
	off := false
	cfg.UI = &UISection{Enabled: &off}
	if cfg.uiEnabled() {
		t.Fatal("explicit false want disabled")
	}
}

// TestUILoadConfig ui 段 YAML 解析（缺省/显式/负值三态）。
func TestUILoadConfig(t *testing.T) {
	dir := t.TempDir()
	write := func(s string) string {
		p := filepath.Join(dir, "cfg.yaml")
		if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	base := "listen: \"127.0.0.1:0\"\nupstreams:\n  - name: u1\n    protocol: anthropic\n    base_url: \"https://api.anthropic.com\"\n    api_key: \"k\"\n"

	// 不写 ui 段：enabled 缺省 true、限流 0。
	c1, err := LoadConfig(write(base))
	if err != nil {
		t.Fatalf("load1: %v", err)
	}
	if !c1.uiEnabled() || c1.uiRateLimitPerMin() != 0 {
		t.Fatalf("default ui: %+v", c1.UI)
	}

	// 显式 ui 段：enabled false + 限流 42。
	c2, err := LoadConfig(write(base + "ui:\n  enabled: false\n  rate_limit_per_min: 42\n"))
	if err != nil {
		t.Fatalf("load2: %v", err)
	}
	if c2.uiEnabled() || c2.uiRateLimitPerMin() != 42 {
		t.Fatalf("explicit ui: %+v", c2.UI)
	}

	// 只写限流：enabled 仍缺省 true。
	c3, err := LoadConfig(write(base + "ui:\n  rate_limit_per_min: 7\n"))
	if err != nil {
		t.Fatalf("load3: %v", err)
	}
	if !c3.uiEnabled() || c3.uiRateLimitPerMin() != 7 {
		t.Fatalf("partial ui: %+v", c3.UI)
	}

	// 负值 = 配置错误（LoadConfig 经 Validate 拒绝）。
	if _, err := LoadConfig(write(base + "ui:\n  rate_limit_per_min: -5\n")); err == nil {
		t.Fatal("want error for negative rate_limit_per_min")
	}
}
