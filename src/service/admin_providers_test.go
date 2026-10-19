// admin_providers_test.go —— 上游探针 + 供应商目录（board A2A-L19-W78-1）测试。
//
// 装置：真 Server + httptest 假上游（**全程本地回环，零外部网络**）+ 真实 Handler
// （新路由的注册点）。探针超时用包级 `adminProbeTimeout` 覆写（测试专用旋钮；
// 真机恒 5s）——超时用例因此不用等 5 秒。
//
// 反证锚点（回退即红）：
//   - 探针 auth 面写死 `verified` → TestAdminProvidersProbeStatusClasses 红
//     （负控 2：401 必须 `rejected`）；
//   - 探针把凭据/响应体回显 → TestAdminProvidersProbeStatusClasses 红（响应出现 sk-）；
//   - 供应商目录回显凭据 → TestAdminProvidersCatalogAndReadiness 红。
package service

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 假上游（记录路径/头面；响应体故意带 SECRET 串——证明探针绝不回显响应体）
// ---------------------------------------------------------------------------

const probeCanaryBody = `{"error":{"type":"api_error","message":"sk-SECRET-echo"}}`

type probeUpstream struct {
	srv     *httptest.Server
	mu      sync.Mutex
	hits    int
	lastURL string
	lastHdr http.Header
}

func newProbeUpstream(t *testing.T, status int, delay time.Duration) *probeUpstream {
	t.Helper()
	pu := &probeUpstream{}
	pu.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if delay > 0 {
			time.Sleep(delay)
		}
		pu.mu.Lock()
		pu.hits++
		pu.lastURL = r.URL.Path
		pu.lastHdr = r.Header.Clone()
		pu.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, probeCanaryBody)
	}))
	t.Cleanup(pu.srv.Close)
	return pu
}

func (pu *probeUpstream) count() int {
	pu.mu.Lock()
	defer pu.mu.Unlock()
	return pu.hits
}

func (pu *probeUpstream) snapshot() (string, http.Header) {
	pu.mu.Lock()
	defer pu.mu.Unlock()
	return pu.lastURL, pu.lastHdr
}

// probeConfig 单上游探针配置（协议 + 基址 + 可选凭据）。
func probeConfig(key, name string, proto Protocol, baseURL string, creds ...string) *Config {
	cfg := uiTestConfig()
	cfg.HeartbeatSeconds = 15
	cfg.AdminKey = key
	up := Upstream{Name: name, Protocol: proto, BaseURL: baseURL}
	if len(creds) > 0 {
		up.APIKey = creds[0]
	}
	cfg.Upstreams = []Upstream{up}
	return cfg
}

// adminProbeOnce 发一次探针并解码（非 200 即失败）。
func adminProbeOnce(t *testing.T, base, key, name, body string) adminProbeResult {
	t.Helper()
	resp := adminAPIReq(t, http.MethodPost, base+"/admin/upstreams/"+name+"/probe", key, body)
	if resp.status != http.StatusOK {
		t.Fatalf("probe %s = %d body=%s", name, resp.status, resp.body)
	}
	var out adminProbeResult
	if err := json.Unmarshal([]byte(resp.body), &out); err != nil {
		t.Fatalf("decode probe: %v body=%s", err, resp.body)
	}
	if strings.Contains(resp.body, "SECRET") || strings.Contains(resp.body, "sk-") {
		t.Fatalf("探针响应出现凭据字节: %s", resp.body)
	}
	return out
}

// ---------------------------------------------------------------------------
// 1. /admin/providers：目录形状 + 就绪态（配置派生；探针前 = configured）
// ---------------------------------------------------------------------------

func TestAdminProvidersCatalogAndReadiness(t *testing.T) {
	const key = "w78-key"
	cfg := adminOpsConfig(t, key) // anthropic / claude_web（池）/ claude_web（空）/ openai_chat
	cfg.Upstreams = append(cfg.Upstreams,
		Upstream{Name: "gem", BaseURL: "https://generativelanguage.example", Protocol: ProtocolGemini, APIKey: "sk-SECRET-gem"})
	_, srv := adminOpsStart(t, cfg)

	resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/providers", key, "")
	if resp.status != http.StatusOK {
		t.Fatalf("/admin/providers = %d body=%s", resp.status, resp.body)
	}
	if strings.Contains(resp.body, "SECRET") || strings.Contains(resp.body, "sk-") {
		t.Fatalf("providers 响应出现凭据字节: %s", resp.body)
	}
	var out adminProvidersResponse
	if err := json.Unmarshal([]byte(resp.body), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Now <= 0 || len(out.Providers) < 7 {
		t.Fatalf("providers = %+v", out)
	}
	byID := make(map[string]adminProviderView, len(out.Providers))
	for _, p := range out.Providers {
		byID[p.ID] = p
		if p.Docs != "" {
			// docs 指针必须是真文件（不发明路径）。
			if _, err := os.Stat(filepath.Join("..", "..", filepath.FromSlash(p.Docs))); err != nil {
				t.Fatalf("provider %s docs 不存在: %v", p.ID, err)
			}
		}
	}
	for _, id := range []string{"openai", "anthropic", "gemini", "interactions", "claude_web", "chatgpt_web", "generic"} {
		if _, ok := byID[id]; !ok {
			t.Fatalf("目录缺条目 %q: %+v", id, byID)
		}
	}
	if got := byID["anthropic"]; !got.Configured || got.Readiness != adminProviderConfigured ||
		len(got.Upstreams) != 1 || got.Upstreams[0] != "anthropic-main" || len(got.Capabilities) == 0 {
		t.Fatalf("anthropic 条目 = %+v", got)
	}
	if got := byID["openai"]; !got.Configured || got.Readiness != adminProviderConfigured ||
		len(got.Protocols) != 2 || got.Upstreams[0] != "plain-http" {
		t.Fatalf("openai 条目 = %+v", got)
	}
	if got := byID["claude_web"]; !got.Configured || len(got.Upstreams) != 2 {
		t.Fatalf("claude_web 条目 = %+v", got)
	}
	if got := byID["gemini"]; !got.Configured || got.Upstreams[0] != "gem" {
		t.Fatalf("gemini 条目 = %+v", got)
	}
	// 未配置协议：仍返回静态条目（UI 要能显示「支持但没配」）。
	for _, id := range []string{"interactions", "chatgpt_web", "generic"} {
		got := byID[id]
		if got.Configured || got.Readiness != adminProviderNotConfigured || len(got.Upstreams) != 0 {
			t.Fatalf("未配置条目 %s = %+v", id, got)
		}
	}
	if got := byID["generic"]; len(got.Protocols) != 0 {
		t.Fatalf("generic 兜底不该认领协议: %+v", got)
	}
	// 形状：空 upstreams 是 []（不是 null）。
	if !strings.Contains(resp.body, `"upstreams":[]`) {
		t.Fatalf("空态形状错（want upstreams:[]）: %s", resp.body)
	}
}

// ---------------------------------------------------------------------------
// 2. 探针路径 + 认证头矩阵（协议 × 基址形态）
// ---------------------------------------------------------------------------

func TestAdminProvidersProbePathsAndAuthHeaders(t *testing.T) {
	const key = "w78-key"
	cases := []struct {
		name      string
		proto     Protocol
		base      func(*probeUpstream) string
		wantPath  string
		wantHdr   map[string]string
		wantNoHdr []string
	}{
		{
			name: "anthropic", proto: ProtocolAnthropic,
			base:     func(p *probeUpstream) string { return p.srv.URL },
			wantPath: "/v1/models",
			wantHdr:  map[string]string{"x-api-key": "sk-SECRET-ant", "anthropic-version": "2023-06-01"},
		},
		{
			name: "anthropic_base_v1", proto: ProtocolAnthropic,
			base:     func(p *probeUpstream) string { return p.srv.URL + "/v1" },
			wantPath: "/v1/models",
			wantHdr:  map[string]string{"x-api-key": "sk-SECRET-ant"},
		},
		{
			name: "openai_chat", proto: ProtocolOpenAIChat,
			base:     func(p *probeUpstream) string { return p.srv.URL },
			wantPath: "/v1/models",
			wantHdr:  map[string]string{"Authorization": "Bearer sk-SECRET-ant"},
		},
		{
			name: "responses_base_v1", proto: ProtocolResponses,
			base:     func(p *probeUpstream) string { return p.srv.URL + "/v1" },
			wantPath: "/v1/models",
			wantHdr:  map[string]string{"Authorization": "Bearer sk-SECRET-ant"},
		},
		{
			name: "interactions", proto: ProtocolInteractions,
			base:     func(p *probeUpstream) string { return p.srv.URL },
			wantPath: "/v1/models",
			wantHdr:  map[string]string{"Authorization": "Bearer sk-SECRET-ant"},
		},
		{
			name: "gemini", proto: ProtocolGemini,
			base:     func(p *probeUpstream) string { return p.srv.URL },
			wantPath: "/v1beta/models",
			wantHdr:  map[string]string{"x-goog-api-key": "sk-SECRET-ant"},
		},
		{
			name: "gemini_base_v1beta", proto: ProtocolGemini,
			base:     func(p *probeUpstream) string { return p.srv.URL + "/v1beta" },
			wantPath: "/v1beta/models",
			wantHdr:  map[string]string{"x-goog-api-key": "sk-SECRET-ant"},
		},
		{
			name: "claude_web_root", proto: ProtocolClaudeWeb,
			base:      func(p *probeUpstream) string { return p.srv.URL },
			wantPath:  "/",
			wantNoHdr: []string{"x-api-key", "Authorization", "x-goog-api-key", "Cookie"},
		},
		{
			name: "chatgpt_web_root", proto: ProtocolChatGPTWeb,
			base:      func(p *probeUpstream) string { return p.srv.URL },
			wantPath:  "/",
			wantNoHdr: []string{"x-api-key", "Authorization", "Cookie"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pu := newProbeUpstream(t, http.StatusOK, 0)
			cfg := probeConfig(key, "u-"+tc.name, tc.proto, tc.base(pu), "sk-SECRET-ant")
			_, srv := adminOpsStart(t, cfg)
			out := adminProbeOnce(t, srv.URL, key, "u-"+tc.name, "")
			if !out.OK || !out.Reachable || out.Status != 200 || out.Mode != adminProbeModeModels {
				t.Fatalf("probe = %+v", out)
			}
			if out.Protocol != string(tc.proto) || out.Upstream != "u-"+tc.name {
				t.Fatalf("probe 归因 = %+v", out)
			}
			if out.ErrorClass != "" || out.CheckedAt <= 0 || out.LatencyMS < 0 {
				t.Fatalf("probe 分类 = %+v", out)
			}
			if tc.wantHdr != nil && out.Auth != adminProbeAuthVerified {
				t.Fatalf("auth = %q（want verified）", out.Auth)
			}
			if tc.wantHdr == nil && out.Auth != adminProbeAuthUnknown {
				t.Fatalf("web 根面 auth = %q（want unknown）", out.Auth)
			}
			if pu.count() != 1 {
				t.Fatalf("探针命中数 = %d（want 1：1 发、无重试）", pu.count())
			}
			path, hdr := pu.snapshot()
			if path != tc.wantPath {
				t.Fatalf("探针路径 = %q（want %q）", path, tc.wantPath)
			}
			for k, v := range tc.wantHdr {
				if got := hdr.Get(k); got != v {
					t.Fatalf("头 %s = %q（want %q）", k, got, v)
				}
			}
			for _, k := range tc.wantNoHdr {
				if got := hdr.Get(k); got != "" {
					t.Fatalf("web 根面不该带 %s = %q", k, got)
				}
			}
		})
	}
}

// 无凭据的 API 协议：不发认证头，auth = unknown（仍是可达性探针）。
func TestAdminProvidersProbeWithoutCredential(t *testing.T) {
	const key = "w78-key"
	pu := newProbeUpstream(t, http.StatusOK, 0)
	cfg := probeConfig(key, "no-cred", ProtocolAnthropic, pu.srv.URL)
	_, srv := adminOpsStart(t, cfg)
	out := adminProbeOnce(t, srv.URL, key, "no-cred", "")
	if !out.OK || out.Auth != adminProbeAuthUnknown {
		t.Fatalf("无凭据探针 = %+v（want ok + auth=unknown）", out)
	}
	_, hdr := pu.snapshot()
	if hdr.Get("x-api-key") != "" || hdr.Get("Authorization") != "" {
		t.Fatalf("无凭据时不该带认证头: %+v", hdr)
	}
	if hdr.Get("anthropic-version") != "2023-06-01" {
		t.Fatalf("anthropic 版本头（非凭据）应保留: %+v", hdr)
	}
}

// ---------------------------------------------------------------------------
// 3. 状态分类：200 / 401 / 500 / 超时 / 连接失败 / 跨主机重定向
// ---------------------------------------------------------------------------

func TestAdminProvidersProbeStatusClasses(t *testing.T) {
	const key = "w78-key"

	t.Run("http_200_verified", func(t *testing.T) {
		pu := newProbeUpstream(t, http.StatusOK, 0)
		_, srv := adminOpsStart(t, probeConfig(key, "ok", ProtocolAnthropic, pu.srv.URL, "sk-SECRET-ant"))
		out := adminProbeOnce(t, srv.URL, key, "ok", "")
		if !out.OK || out.Auth != adminProbeAuthVerified || out.ErrorClass != "" {
			t.Fatalf("200 = %+v", out)
		}
		t.Logf("probe 200 latency_ms=%d", out.LatencyMS)
	})

	t.Run("http_401_rejected", func(t *testing.T) {
		// 负控 2 的锚点：401 必须 auth=rejected（写死 verified 即红）。
		pu := newProbeUpstream(t, http.StatusUnauthorized, 0)
		_, srv := adminOpsStart(t, probeConfig(key, "unauth", ProtocolAnthropic, pu.srv.URL, "sk-SECRET-ant"))
		out := adminProbeOnce(t, srv.URL, key, "unauth", "")
		if out.Auth != adminProbeAuthRejected {
			t.Fatalf("401 auth = %q（want rejected）", out.Auth)
		}
		if out.OK || !out.Reachable || out.Status != 401 || out.ErrorClass != adminProbeErrorHTTP4xx {
			t.Fatalf("401 = %+v", out)
		}
		t.Logf("probe 401 latency_ms=%d", out.LatencyMS)
	})

	t.Run("http_403_rejected", func(t *testing.T) {
		pu := newProbeUpstream(t, http.StatusForbidden, 0)
		_, srv := adminOpsStart(t, probeConfig(key, "forbidden", ProtocolGemini, pu.srv.URL, "sk-SECRET-ant"))
		out := adminProbeOnce(t, srv.URL, key, "forbidden", "")
		if out.Auth != adminProbeAuthRejected || out.ErrorClass != adminProbeErrorHTTP4xx {
			t.Fatalf("403 = %+v", out)
		}
	})

	t.Run("http_500_not_ok", func(t *testing.T) {
		pu := newProbeUpstream(t, http.StatusInternalServerError, 0)
		_, srv := adminOpsStart(t, probeConfig(key, "boom", ProtocolAnthropic, pu.srv.URL, "sk-SECRET-ant"))
		out := adminProbeOnce(t, srv.URL, key, "boom", "")
		if out.OK || !out.Reachable || out.Status != 500 || out.ErrorClass != adminProbeErrorHTTP5xx {
			t.Fatalf("500 = %+v", out)
		}
		if out.Auth != adminProbeAuthUnknown {
			t.Fatalf("500 不判 auth = %q（want unknown）", out.Auth)
		}
		t.Logf("probe 500 latency_ms=%d", out.LatencyMS)
	})

	t.Run("timeout", func(t *testing.T) {
		old := adminProbeTimeout
		adminProbeTimeout = 100 * time.Millisecond
		t.Cleanup(func() { adminProbeTimeout = old })
		pu := newProbeUpstream(t, http.StatusOK, 400*time.Millisecond)
		_, srv := adminOpsStart(t, probeConfig(key, "slow", ProtocolAnthropic, pu.srv.URL, "sk-SECRET-ant"))
		out := adminProbeOnce(t, srv.URL, key, "slow", "")
		if out.Reachable || out.OK || out.Status != 0 || out.ErrorClass != adminProbeErrorTimeout {
			t.Fatalf("timeout = %+v", out)
		}
		t.Logf("probe timeout latency_ms=%d（注入超时 = %dms）", out.LatencyMS, adminProbeTimeout.Milliseconds())
	})

	t.Run("network_refused", func(t *testing.T) {
		// 回环 1 端口（连接被拒；零外部网络）。
		_, srv := adminOpsStart(t, probeConfig(key, "dead", ProtocolAnthropic, "http://127.0.0.1:1", "sk-SECRET-ant"))
		out := adminProbeOnce(t, srv.URL, key, "dead", "")
		if out.Reachable || out.OK || out.Status != 0 || out.ErrorClass != adminProbeErrorNetwork {
			t.Fatalf("network = %+v", out)
		}
	})

	t.Run("cross_host_redirect_rejected", func(t *testing.T) {
		target := newProbeUpstream(t, http.StatusOK, 0)
		redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", target.srv.URL+"/elsewhere")
			w.WriteHeader(http.StatusFound)
		}))
		t.Cleanup(redirector.Close)
		_, srv := adminOpsStart(t, probeConfig(key, "redir", ProtocolAnthropic, redirector.URL, "sk-SECRET-ant"))
		out := adminProbeOnce(t, srv.URL, key, "redir", "")
		if target.count() != 0 {
			t.Fatalf("跨主机重定向被跟随了（命中 %d 次）——认证头泄露面", target.count())
		}
		if out.Status != http.StatusFound || !out.Reachable || !out.OK {
			t.Fatalf("302（不跟随）= %+v", out)
		}
	})

	t.Run("mode_validation_and_404", func(t *testing.T) {
		pu := newProbeUpstream(t, http.StatusOK, 0)
		_, srv := adminOpsStart(t, probeConfig(key, "one", ProtocolAnthropic, pu.srv.URL, "sk-SECRET-ant"))
		// 合法 mode = 显式 models。
		out := adminProbeOnce(t, srv.URL, key, "one", `{"mode":"models"}`)
		if !out.OK || out.Mode != "models" {
			t.Fatalf("显式 mode = %+v", out)
		}
		for _, body := range []string{`{"mode":"chat"}`, `{"mode":"probe"}`, `not-json`, `{"mode":1}`} {
			resp := adminAPIReq(t, http.MethodPost, srv.URL+"/admin/upstreams/one/probe", key, body)
			if resp.status != http.StatusBadRequest {
				t.Fatalf("probe body=%q = %d（want 400）resp=%s", body, resp.status, resp.body)
			}
		}
		resp := adminAPIReq(t, http.MethodPost, srv.URL+"/admin/upstreams/nope/probe", key, "")
		if resp.status != http.StatusNotFound {
			t.Fatalf("未知上游 = %d（want 404）", resp.status)
		}
		if pu.count() != 1 {
			t.Fatalf("被拒请求不该打到上游（命中 %d）", pu.count())
		}
		// 方法错 = 405（路由存在但方法不匹配；在鉴权之前）。
		resp = adminAPIReq(t, http.MethodGet, srv.URL+"/admin/upstreams/one/probe", key, "")
		if resp.status != http.StatusMethodNotAllowed {
			t.Fatalf("GET probe = %d（want 405）", resp.status)
		}
	})
}

// ---------------------------------------------------------------------------
// 4. 就绪态流转：configured（未探）→ degraded（探到坏）→ ready（探到好）
// ---------------------------------------------------------------------------

func TestAdminProvidersReadinessTransitions(t *testing.T) {
	const key = "w78-key"
	good := newProbeUpstream(t, http.StatusOK, 0)
	bad := newProbeUpstream(t, http.StatusInternalServerError, 0)
	cfg := uiTestConfig()
	cfg.HeartbeatSeconds = 15
	cfg.AdminKey = key
	cfg.Upstreams = []Upstream{
		{Name: "a-good", Protocol: ProtocolAnthropic, BaseURL: good.srv.URL, APIKey: "sk-SECRET-good"},
		{Name: "a-bad", Protocol: ProtocolAnthropic, BaseURL: bad.srv.URL, APIKey: "sk-SECRET-bad"},
	}
	_, srv := adminOpsStart(t, cfg)

	readiness := func() adminProviderView {
		var out adminProvidersResponse
		adminAPIGetJSON(t, srv.URL+"/admin/providers", key, &out)
		for _, p := range out.Providers {
			if p.ID == "anthropic" {
				return p
			}
		}
		t.Fatalf("目录缺 anthropic 条目")
		return adminProviderView{}
	}
	if got := readiness(); got.Readiness != adminProviderConfigured || len(got.Upstreams) != 2 {
		t.Fatalf("未探 = %+v", got)
	}
	// 探到坏台 → degraded（可达但不可用）。
	adminProbeOnce(t, srv.URL, key, "a-bad", "")
	if got := readiness(); got.Readiness != adminProviderDegraded {
		t.Fatalf("坏台探后 = %+v（want degraded）", got)
	}
	// 探到好台 → ready（ok 优先于同组里的 degraded）。
	adminProbeOnce(t, srv.URL, key, "a-good", "")
	if got := readiness(); got.Readiness != adminProviderReady {
		t.Fatalf("好台探后 = %+v（want ready）", got)
	}
	// 401 也算 degraded（auth=rejected）。
	pu := newProbeUpstream(t, http.StatusUnauthorized, 0)
	cfg2 := probeConfig(key, "a-401", ProtocolAnthropic, pu.srv.URL, "sk-SECRET-ant")
	_, srv2 := adminOpsStart(t, cfg2)
	adminProbeOnce(t, srv2.URL, key, "a-401", "")
	var out adminProvidersResponse
	adminAPIGetJSON(t, srv2.URL+"/admin/providers", key, &out)
	for _, p := range out.Providers {
		if p.ID == "anthropic" && p.Readiness != adminProviderDegraded {
			t.Fatalf("401 探后 = %+v（want degraded）", p)
		}
	}
}

// ---------------------------------------------------------------------------
// 5. 探针结果表：有界（键 = 上游名，容量 = 上游数）
// ---------------------------------------------------------------------------

func TestAdminProvidersProbeStoreBounded(t *testing.T) {
	var st adminProbeStore
	st.set(adminProbeResult{Upstream: "a", CheckedAt: 1}, 2)
	st.set(adminProbeResult{Upstream: "b", CheckedAt: 2}, 2)
	// 覆盖已有键不占新容量。
	st.set(adminProbeResult{Upstream: "a", CheckedAt: 3}, 2)
	if got, ok := st.get("a"); !ok || got.CheckedAt != 3 {
		t.Fatalf("覆盖写 = %+v %v", got, ok)
	}
	// 第三个新键 → 逐出最旧（b，CheckedAt=2）。
	st.set(adminProbeResult{Upstream: "c", CheckedAt: 4}, 2)
	if _, ok := st.get("b"); ok {
		t.Fatalf("容量满时应逐出最旧行")
	}
	if _, ok := st.get("c"); !ok {
		t.Fatalf("新键应写入")
	}
	if len(st.rows) != 2 {
		t.Fatalf("表大小 = %d（want 2）", len(st.rows))
	}
}
