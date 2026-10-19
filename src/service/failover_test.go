// W5-14 e2e：多上游有序 failover（C1 失败分类）+ 上游级短冷却 + 诊断计数。
//
// 装置：httptest 假上游（全部本地回环；无真实上游/凭据）+ 真服务（New(cfg).Handler()
// 经 httptest.NewServer）+ 真 HTTP 客户端（postJSON）。断言面：下游状态码/字节、
// 各上游命中数、Server.Diagnostics() 计数。
package service

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

// countUpstream 计数假上游（每请求 +1）。
type countUpstream struct {
	srv  *httptest.Server
	mu   sync.Mutex
	hits int
}

func newCountUpstream(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) *countUpstream {
	t.Helper()
	cu := &countUpstream{}
	cu.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cu.mu.Lock()
		cu.hits++
		cu.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(cu.srv.Close)
	return cu
}

func (cu *countUpstream) count() int {
	cu.mu.Lock()
	defer cu.mu.Unlock()
	return cu.hits
}

// errBodyAPIError 通用上游错误体（api_error）。
const errBodyAPIError = `{"type":"error","error":{"type":"api_error","message":"boom"}}`

// failUpstream 恒定失败的上游（状态码 + 可选响应头 + 错误体）。
func failUpstream(t *testing.T, status int, headers map[string]string, body string) *countUpstream {
	t.Helper()
	return newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		for k, v := range headers {
			w.Header().Set(k, v)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
}

// goodClaudeUpstream 成功流式上游（claude 文本流；claudeTextStream 见 service_test.go）。
func goodClaudeUpstream(t *testing.T) *countUpstream {
	t.Helper()
	return newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		claudeTextStream(w)
	})
}

// failoverCfg 多上游配置（heartbeat 15；冷却参数按需另设）。
func failoverCfg(ups ...Upstream) *Config {
	return &Config{Listen: "127.0.0.1:0", HeartbeatSeconds: 15, Upstreams: ups}
}

// anthUp anthropic 上游条目（测试里最常用）。
func anthUp(name, baseURL string) Upstream {
	return Upstream{Name: name, BaseURL: baseURL, APIKey: "k-" + name, Protocol: ProtocolAnthropic}
}

// startServiceWith 启动被测服务并返回 Server 句柄（诊断计数断言用）。
func startServiceWith(t *testing.T, cfg *Config) (*Server, *httptest.Server) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	s := New(cfg)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return s, srv
}

// postChatStream 发一次流式 chat 请求并读回整体（状态码 + 原文）。
func postChatStream(t *testing.T, base string) (int, string) {
	t.Helper()
	resp := postJSON(t, base+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// ---------------------------------------------------------------------------
// e2e 18：首上游换上游类状态码 → 第二台成功（C1 通用集合）
// ---------------------------------------------------------------------------

func TestE2EFailoverRetryableStatuses(t *testing.T) {
	for _, status := range []int{401, 408, 409, 429, 500, 503} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			bad := failUpstream(t, status, nil, errBodyAPIError)
			good := goodClaudeUpstream(t)
			s, srv := startServiceWith(t, failoverCfg(
				anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

			code, body := postChatStream(t, srv.URL)
			if code != 200 || !strings.Contains(body, "Hello ") {
				t.Fatalf("status=%d body=\n%s", code, body)
			}
			if bad.count() != 1 || good.count() != 1 {
				t.Fatalf("hits: bad=%d good=%d（期望 1/1：换上游一次）", bad.count(), good.count())
			}
			if d := s.Diagnostics(); d.Requests != 1 || d.Attempts != 2 || d.Failovers != 1 {
				t.Fatalf("diagnostics = %+v", d)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// e2e 19：全部上游失败 → 下游错误信封（末台上游错误透传）
// ---------------------------------------------------------------------------

func TestE2EFailoverAllUpstreamsFail(t *testing.T) {
	a := failUpstream(t, 503, nil, errBodyAPIError)
	b := failUpstream(t, 503, nil, errBodyAPIError)
	c := failUpstream(t, 503, nil, errBodyAPIError)
	s, srv := startServiceWith(t, failoverCfg(
		anthUp("a", a.srv.URL), anthUp("b", b.srv.URL), anthUp("c", c.srv.URL)))

	code, body := postChatStream(t, srv.URL)
	if code != 503 {
		t.Fatalf("status = %d（末台上游错误必须透传）", code)
	}
	if !strings.Contains(body, `"error"`) {
		t.Fatalf("错误信封缺失: %s", body)
	}
	for name, cu := range map[string]*countUpstream{"a": a, "b": b, "c": c} {
		if cu.count() != 1 {
			t.Fatalf("upstream %s hits = %d, want 1", name, cu.count())
		}
	}
	if d := s.Diagnostics(); d.Attempts != 3 || d.Failovers != 2 {
		t.Fatalf("diagnostics = %+v", d)
	}
}

// e2e 19b：全部上游不可达（传输错误）→ 服务自产 502 信封（不重试同一台）。
func TestE2EFailoverAllTransportErrors(t *testing.T) {
	s, srv := startServiceWith(t, failoverCfg(
		anthUp("dead1", "http://127.0.0.1:1"),
		anthUp("dead2", "http://127.0.0.1:1"),
		anthUp("dead3", "http://127.0.0.1:1")))

	code, body := postChatStream(t, srv.URL)
	if code != 502 {
		t.Fatalf("status = %d（全传输错误 → 502）", code)
	}
	if !strings.Contains(body, `"error"`) {
		t.Fatalf("错误信封缺失: %s", body)
	}
	if d := s.Diagnostics(); d.Attempts != 3 || d.Failovers != 2 {
		t.Fatalf("diagnostics = %+v", d)
	}
}

// ---------------------------------------------------------------------------
// e2e 20：提交后错误 → 不换上游、不重放
// ---------------------------------------------------------------------------

func TestE2EFailoverAfterCommitNoReplay(t *testing.T) {
	a := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_f1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":0}}}`)
		sseFrame(w, "content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
		sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}`)
		sseFrame(w, "error", `{"type":"error","error":{"type":"overloaded_error","message":"busy"}}`)
	})
	good := goodClaudeUpstream(t)
	s, srv := startServiceWith(t, failoverCfg(anthUp("a", a.srv.URL), anthUp("good", good.srv.URL)))

	code, body := postChatStream(t, srv.URL)
	if code != 200 {
		t.Fatalf("status = %d（已提交后错误必须 200 + 流内错误帧）", code)
	}
	if !strings.Contains(body, "partial") || !strings.Contains(body, `"overloaded_error"`) {
		t.Fatalf("流内错误帧缺失:\n%s", body)
	}
	if good.count() != 0 {
		t.Fatalf("提交后不得换上游（good hits=%d）", good.count())
	}
	if a.count() != 1 {
		t.Fatalf("a hits = %d, want 1（不重放）", a.count())
	}
	if d := s.Diagnostics(); d.Failovers != 0 {
		t.Fatalf("diagnostics = %+v（提交后不得记换上游）", d)
	}
}

// ---------------------------------------------------------------------------
// e2e 21：上游级短冷却（连续失败阈值 → 跳过；窗口过期 → 回归）
// ---------------------------------------------------------------------------

func TestE2EFailoverCooldownSkipsFailedUpstream(t *testing.T) {
	bad := failUpstream(t, 500, nil, errBodyAPIError)
	good := goodClaudeUpstream(t)
	cfg := failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL))
	cfg.CooldownFailures = 1
	cfg.CooldownSeconds = 30
	s, srv := startServiceWith(t, cfg)

	for req := 1; req <= 2; req++ {
		if code, body := postChatStream(t, srv.URL); code != 200 || !strings.Contains(body, "Hello ") {
			t.Fatalf("req%d: status=%d body=\n%s", req, code, body)
		}
	}
	if bad.count() != 1 || good.count() != 2 {
		t.Fatalf("hits: bad=%d good=%d（期望 1/2：第二次跳过冷却中的 bad）", bad.count(), good.count())
	}
	if d := s.Diagnostics(); d.CooldownSkips == 0 || d.CooldownTrips == 0 {
		t.Fatalf("diagnostics = %+v（冷却计数缺失）", d)
	}
}

func TestE2EFailoverCooldownExpiry(t *testing.T) {
	bad := failUpstream(t, 500, nil, errBodyAPIError)
	good := goodClaudeUpstream(t)
	cfg := failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL))
	cfg.CooldownFailures = 1
	cfg.CooldownSeconds = 1
	_, srv := startServiceWith(t, cfg)

	if code, _ := postChatStream(t, srv.URL); code != 200 {
		t.Fatalf("req1: status=%d", code)
	}
	time.Sleep(1200 * time.Millisecond) // 冷却窗口 1s：等它过期
	if code, _ := postChatStream(t, srv.URL); code != 200 {
		t.Fatalf("req2: status=%d", code)
	}
	if bad.count() != 2 || good.count() != 2 {
		t.Fatalf("hits: bad=%d good=%d（期望 2/2：冷却过期后 bad 重新入链）", bad.count(), good.count())
	}
}

// e2e 21b：全部上游都在冷却 → 退化为全列表（不空转 502）。
func TestE2EFailoverAllCoolingFallsBackToFullList(t *testing.T) {
	a := failUpstream(t, 500, nil, errBodyAPIError)
	b := failUpstream(t, 500, nil, errBodyAPIError)
	cfg := failoverCfg(anthUp("a", a.srv.URL), anthUp("b", b.srv.URL))
	cfg.CooldownFailures = 1
	cfg.CooldownSeconds = 30
	s, srv := startServiceWith(t, cfg)

	for req := 1; req <= 2; req++ {
		if code, _ := postChatStream(t, srv.URL); code != 500 {
			t.Fatalf("req%d: status=%d（末台上游错误透传）", req, code)
		}
	}
	if a.count() != 2 || b.count() != 2 {
		t.Fatalf("hits: a=%d b=%d（期望 2/2：全冷却退化全列表）", a.count(), b.count())
	}
	if d := s.Diagnostics(); d.CooldownSkips != 2 {
		t.Fatalf("diagnostics = %+v（req2 两台都应被冷却跳过）", d)
	}
}

// ---------------------------------------------------------------------------
// e2e 22：x-should-retry / overloaded_error 分类（C1 头语义）
// ---------------------------------------------------------------------------

func TestE2EFailoverXShouldRetryFalseNoSwitch(t *testing.T) {
	bad := failUpstream(t, 503, map[string]string{"x-should-retry": "false"}, errBodyAPIError)
	good := goodClaudeUpstream(t)
	s, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	code, _ := postChatStream(t, srv.URL)
	if code != 503 {
		t.Fatalf("status = %d（x-should-retry:false 必须压住 503——C1）", code)
	}
	if good.count() != 0 {
		t.Fatalf("不得换上游（good hits=%d）", good.count())
	}
	if d := s.Diagnostics(); d.Failovers != 0 || d.Attempts != 1 {
		t.Fatalf("diagnostics = %+v", d)
	}
}

func TestE2EFailoverXShouldRetryTrueOn400(t *testing.T) {
	bad := failUpstream(t, 400, map[string]string{"x-should-retry": "true"}, errBodyAPIError)
	good := goodClaudeUpstream(t)
	_, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	code, body := postChatStream(t, srv.URL)
	if code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("status=%d body=\n%s（x-should-retry:true 覆盖状态码分类）", code, body)
	}
	if bad.count() != 1 || good.count() != 1 {
		t.Fatalf("hits: bad=%d good=%d（期望 1/1）", bad.count(), good.count())
	}
}

func TestE2EFailoverOverloadedErrorForcesSwitch(t *testing.T) {
	// 400 + x-should-retry:false + overloaded_error 体：体匹配先于头检查（C1）。
	bad := failUpstream(t, 400,
		map[string]string{"x-should-retry": "false"},
		`{"type":"error","error":{"type":"overloaded_error","message":"busy"}}`)
	good := goodClaudeUpstream(t)
	_, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	code, body := postChatStream(t, srv.URL)
	if code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("status=%d body=\n%s（overloaded_error 体强制换上游）", code, body)
	}
	if bad.count() != 1 || good.count() != 1 {
		t.Fatalf("hits: bad=%d good=%d（期望 1/1）", bad.count(), good.count())
	}
}

// ---------------------------------------------------------------------------
// e2e 23：retry-after 整数秒冷却 vs HTTP-date 丢弃（C1 实测对照）
// ---------------------------------------------------------------------------

func TestE2EFailoverRetryAfterIntegerCoolsUpstream(t *testing.T) {
	bad := failUpstream(t, 429, map[string]string{"retry-after": "2"}, errBodyAPIError)
	good := goodClaudeUpstream(t)
	// 连续失败阈值缺省 3：仅 retry-after 整数秒就足以冷却（C1 精确执行）。
	s, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	for req := 1; req <= 2; req++ {
		if code, body := postChatStream(t, srv.URL); code != 200 || !strings.Contains(body, "Hello ") {
			t.Fatalf("req%d: status=%d body=\n%s", req, code, body)
		}
	}
	if bad.count() != 1 || good.count() != 2 {
		t.Fatalf("hits: bad=%d good=%d（期望 1/2：retry-after 秒数内跳过 bad）", bad.count(), good.count())
	}
	if d := s.Diagnostics(); d.CooldownSkips == 0 {
		t.Fatalf("diagnostics = %+v（retry-after 冷却计数缺失）", d)
	}
}

func TestE2EFailoverRetryAfterHTTPDateDropped(t *testing.T) {
	// HTTP-date 形态被 parseInt 静默丢弃（C1 陷阱实测）→ 不冷却，第二次仍打 bad。
	bad := failUpstream(t, 429,
		map[string]string{"retry-after": "Wed, 21 Oct 2015 07:28:00 GMT"}, errBodyAPIError)
	good := goodClaudeUpstream(t)
	s, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	for req := 1; req <= 2; req++ {
		if code, body := postChatStream(t, srv.URL); code != 200 || !strings.Contains(body, "Hello ") {
			t.Fatalf("req%d: status=%d body=\n%s", req, code, body)
		}
	}
	if bad.count() != 2 || good.count() != 2 {
		t.Fatalf("hits: bad=%d good=%d（期望 2/2：HTTP-date 丢弃 → 不冷却）", bad.count(), good.count())
	}
	if d := s.Diagnostics(); d.CooldownSkips != 0 {
		t.Fatalf("diagnostics = %+v（HTTP-date 不得触发冷却）", d)
	}
}

// ---------------------------------------------------------------------------
// e2e 24：非流式 failover（同协议直通）
// ---------------------------------------------------------------------------

const openAIChatCompletionJSON = `{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"gpt-x","choices":[{"index":0,"message":{"role":"assistant","content":"Hi there"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`

func TestE2EFailoverNonStreaming(t *testing.T) {
	bad := failUpstream(t, 500, nil, errBodyAPIError)
	good := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIChatCompletionJSON))
	})
	_, srv := startServiceWith(t, failoverCfg(
		Upstream{Name: "bad", BaseURL: bad.srv.URL, APIKey: "k1", Protocol: ProtocolOpenAIChat},
		Upstream{Name: "good", BaseURL: good.srv.URL, APIKey: "k2", Protocol: ProtocolOpenAIChat},
	))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(raw), "Hi there") {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}
	if bad.count() != 1 || good.count() != 1 {
		t.Fatalf("hits: bad=%d good=%d（期望 1/1）", bad.count(), good.count())
	}
}

// ---------------------------------------------------------------------------
// 单元：分类表 / retry-after 解析 / 冷却状态机
// ---------------------------------------------------------------------------

func TestShouldFailoverClassification(t *testing.T) {
	cases := []struct {
		name   string
		status int
		xsr    string
		body   string
		want   bool
	}{
		{"429", 429, "", "", true},
		{"500", 500, "", "", true},
		{"529", 529, "", "", true},
		{"401", 401, "", "", true},
		{"408", 408, "", "", true},
		{"409", 409, "", "", true},
		{"400", 400, "", "", false},
		{"403", 403, "", "", false},
		{"404", 404, "", "", false},
		{"422", 422, "", "", false},
		{"xsr_false_503", 503, "false", "", false},
		{"xsr_false_529", 529, "false", "", false},
		{"xsr_true_400", 400, "true", "", true},
		{"xsr_unknown_falls_back", 503, "maybe", "", true},
		{"overloaded_400_xsr_false", 400, "false", `{"type":"error","error":{"type":"overloaded_error","message":"busy"}}`, true},
		{"overloaded_500_xsr_false", 500, "false", `{"type":"overloaded_error"}`, true},
		{"api_error_400_xsr_false", 400, "false", errBodyAPIError, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			if tc.xsr != "" {
				h.Set("x-should-retry", tc.xsr)
			}
			if got := shouldFailover(tc.status, h, []byte(tc.body)); got != tc.want {
				t.Fatalf("shouldFailover(%d, xsr=%q, body=%q) = %v, want %v", tc.status, tc.xsr, tc.body, got, tc.want)
			}
		})
	}
}

func TestRetryAfterParsing(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"3", 3 * time.Second},
		{" 3 ", 3 * time.Second},
		{"3.5", 3 * time.Second}, // parseInt 语义：截到整数
		{"0", 0},
		{"-1", 0},
		{"", 0},
		{"Wed, 21 Oct 2015 07:28:00 GMT", 0}, // HTTP-date 形态：丢弃（C1）
		{"999999", maxRetryAfterCap},         // 上限守卫
	}
	for _, tc := range cases {
		h := http.Header{}
		if tc.in != "" {
			h.Set("retry-after", tc.in)
		}
		if got := retryAfterSeconds(h); got != tc.want {
			t.Fatalf("retryAfterSeconds(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestCooldownStateMachine(t *testing.T) {
	cfg := failoverCfg(anthUp("a", "http://127.0.0.1:1"), anthUp("b", "http://127.0.0.1:1"))
	cfg.CooldownFailures = 2
	cfg.CooldownSeconds = 30
	s := New(cfg)

	if idxs, skipped := s.candidates(time.Now()); len(idxs) != 2 || skipped != 0 {
		t.Fatalf("fresh: idxs=%v skipped=%d", idxs, skipped)
	}
	s.recordFailure(0, 0) // 1 次：未达阈值
	if s.cooling(0, time.Now()) {
		t.Fatalf("1 次失败不得冷却")
	}
	s.recordFailure(0, 0) // 达阈值 → 冷却
	if !s.cooling(0, time.Now()) {
		t.Fatalf("连续 2 次失败必须冷却")
	}
	if idxs, skipped := s.candidates(time.Now()); len(idxs) != 1 || idxs[0] != 1 || skipped != 1 {
		t.Fatalf("冷却过滤: idxs=%v skipped=%d", idxs, skipped)
	}
	// 全部冷却 → 退化全列表（不空转）。
	s.recordFailure(1, 0)
	s.recordFailure(1, 0)
	if idxs, _ := s.candidates(time.Now()); len(idxs) != 2 {
		t.Fatalf("全冷却退化: idxs=%v", idxs)
	}
	// 成功清零冷却。
	s.recordSuccess(0)
	if s.cooling(0, time.Now()) {
		t.Fatalf("成功必须解除冷却")
	}
	// retry-after 单独生效（未达阈值也冷却）。
	s2 := New(failoverCfg(anthUp("a", "http://127.0.0.1:1")))
	s2.recordFailure(0, 90*time.Second)
	if !s2.cooling(0, time.Now()) {
		t.Fatalf("retry-after 必须冷却（C1 整数秒精确执行）")
	}
}

// ---------------------------------------------------------------------------
// W68-5：CooldownTrips 契约（新进入冷却的 trip 计数——显式 retry-after /
// 资源级 403 两条路径 W68-2 矩阵 case 01/07 实测漏计：cooling=true 而 trips=0）
// ---------------------------------------------------------------------------

// TestCooldownTripCounterExplicitRetryAfter 显式 retry-after 单独置入冷却也计一
// 次 trip；冷却到期后再次置入 = 新 trip 再 +1。分母：每次 recordFailure 调用 =
// 1 次失败事件；CooldownFailures=100 隔离阈值路径，计数只可能来自 retry-after。
func TestCooldownTripCounterExplicitRetryAfter(t *testing.T) {
	cfg := failoverCfg(anthUp("a", "http://127.0.0.1:1"))
	cfg.CooldownFailures = 100 // 隔离：阈值路径不参与
	cfg.CooldownSeconds = 30
	s := New(cfg)
	if d := s.Diagnostics(); d.CooldownTrips != 0 || d.RateCooldownTrips != 0 {
		t.Fatalf("基线计数非零: %+v", d)
	}

	s.recordFailure(0, 2*time.Second)
	if !s.cooling(0, time.Now()) {
		t.Fatalf("显式 retry-after 必须冷却（C1 精确执行）")
	}
	if d := s.Diagnostics(); d.CooldownTrips != 1 || d.RateCooldownTrips != 0 {
		t.Fatalf("单发 retry-after: cooldown_trips=%d rate_trips=%d（want 1/0）", d.CooldownTrips, d.RateCooldownTrips)
	}

	// 冷却到期（伪造为已过期，不真睡）：再次置入 = 新 trip。
	h := &s.health[0]
	h.mu.Lock()
	h.until = time.Now().Add(-time.Second)
	h.mu.Unlock()
	s.recordFailure(0, 2*time.Second)
	if d := s.Diagnostics(); d.CooldownTrips != 2 {
		t.Fatalf("到期后再次置入: cooldown_trips=%d（want 2——新 trip 各 +1）", d.CooldownTrips)
	}
}

// TestCooldownTripCounterResource403 资源级 403 端到端：upstreamCooldown 给出 5m
// → recordFailure(idx, 5m) 未达阈值也必须计一次 trip（W68-2 case 07 口径）。
func TestCooldownTripCounterResource403(t *testing.T) {
	bad := failUpstream(t, 403, nil, azureResourceBlockedBody)
	good := goodClaudeUpstream(t)
	cfg := failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL))
	cfg.CooldownFailures = 100 // 隔离：单发必不达阈值
	s, srv := startServiceWith(t, cfg)

	if code, body := postChatStream(t, srv.URL); code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("status=%d body=\n%s（资源级 403 必须换上游）", code, body)
	}
	if !s.cooling(0, time.Now()) {
		t.Fatalf("资源级 403 首次命中必须冷却")
	}
	if d := s.Diagnostics(); d.CooldownTrips != 1 || d.RateCooldownTrips != 0 {
		t.Fatalf("资源级 403: cooldown_trips=%d rate_trips=%d（want 1/0）", d.CooldownTrips, d.RateCooldownTrips)
	}
}

// TestCooldownTripCounterNoDoubleCount 同一次 recordFailure 里「阈值 + 显式
// retry-after」同时触发只 +1；窗口仍取较晚者（配置 30s > 头 2s）。
func TestCooldownTripCounterNoDoubleCount(t *testing.T) {
	cfg := failoverCfg(anthUp("a", "http://127.0.0.1:1"))
	cfg.CooldownFailures = 1 // 1 次失败即达阈值：阈值与 retry-after 同发
	cfg.CooldownSeconds = 30
	s := New(cfg)

	s.recordFailure(0, 2*time.Second)
	if !s.cooling(0, time.Now()) {
		t.Fatalf("阈值+retry-after 同发必须冷却")
	}
	if d := s.Diagnostics(); d.CooldownTrips != 1 {
		t.Fatalf("同发双路径: cooldown_trips=%d（want 1——每 trip 恰好 +1，不双计）", d.CooldownTrips)
	}
	h := &s.health[0]
	h.mu.Lock()
	rem := time.Until(h.until)
	h.mu.Unlock()
	if rem <= 25*time.Second {
		t.Fatalf("窗口 = %v（want ≈30s：配置窗口与头部取较晚者）", rem)
	}
}

// TestCooldownTripCounterAlreadyCooling 冷却中仅被显式 retry-after 延长 → 不新增
// trip（窗口仍按 C1 精确延长——防延长闸只免计数，不免窗口更新）。
func TestCooldownTripCounterAlreadyCooling(t *testing.T) {
	cfg := failoverCfg(anthUp("a", "http://127.0.0.1:1"))
	cfg.CooldownFailures = 100 // 隔离：只走显式 retry-after 路径
	cfg.CooldownSeconds = 30
	s := New(cfg)

	s.recordFailure(0, 2*time.Second) // 第 1 次：进入冷却（trip 1）
	h := &s.health[0]
	h.mu.Lock()
	before := h.until
	h.mu.Unlock()

	s.recordFailure(0, 2*time.Minute) // 冷却中延长
	h.mu.Lock()
	after := h.until
	remaining := time.Until(after)
	h.mu.Unlock()
	if !after.After(before) || remaining <= 110*time.Second || remaining > 121*time.Second {
		t.Fatalf("冷却中 retry-after 必须精确延长窗口: before=%v after=%v remaining=%v", before, after, remaining)
	}
	if d := s.Diagnostics(); d.CooldownTrips != 1 {
		t.Fatalf("冷却中延长: cooldown_trips=%d（want 1——不是新 trip）", d.CooldownTrips)
	}
}

// TestCooldownTripCounterRatePathWithRetryAfter 失败率路径与 CooldownTrips 的分
// 工：纯失败率触发（无显式头）只 +RateCooldownTrips；失败率与显式 retry-after
// 同发时两个触发面各记一次（rate +1、cooldown 因 retry-after 面 +1）。
func TestCooldownTripCounterRatePathWithRetryAfter(t *testing.T) {
	newCfg := func() *Config {
		cfg := failoverCfg(anthUp("a", "http://127.0.0.1:1"))
		cfg.CooldownFailures = 100 // 隔离：阈值路径不参与
		cfg.CooldownSeconds = 30
		cfg.FailureRate = &FailureRateCooldown{MinRequests: 1, Threshold: 0.5, WindowSeconds: 60}
		return cfg
	}

	// 纯失败率路径（retryAfter=0）：CooldownTrips 不计。
	s := New(newCfg())
	s.recordFailure(0, 0)
	if d := s.Diagnostics(); d.RateCooldownTrips != 1 || d.CooldownTrips != 0 {
		t.Fatalf("纯失败率路径: rate_trips=%d cooldown_trips=%d（want 1/0）", d.RateCooldownTrips, d.CooldownTrips)
	}

	// 失败率 + 显式 retry-after 同发：两个触发面各 +1。
	s2 := New(newCfg())
	s2.recordFailure(0, 30*time.Second)
	if d := s2.Diagnostics(); d.RateCooldownTrips != 1 || d.CooldownTrips != 1 {
		t.Fatalf("失败率+retry-after: rate_trips=%d cooldown_trips=%d（want 1/1）", d.RateCooldownTrips, d.CooldownTrips)
	}
}
