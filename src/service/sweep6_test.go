// W5-22 清扫六：FD1（gemini 直通终止证据）/ FD2（出口哨兵纪律按格式契约）/
// per-exception 换上游预算 + 失败率冷却（条目 3）/ 错误体兜底提取（条目 4）。
//
// 装置沿用既有 e2e 风格：httptest 假上游（全本地回环）+ 真服务 + 真 HTTP 客户端。
package service

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// FD1：gemini 同协议直通的终止证据（观测器单元 + e2e 复现 P11 s8a/s8b）
// ---------------------------------------------------------------------------

// TestGeminiStreamTerminalObserve 观测器口径（与跨协议解析器 Finish 同面）：
// finishReason / blockReason / error 任一即终止；正文里的 finishReason 字面量、
// `[DONE]`（非 Gemini 语义）与 null 值不算证据。
func TestGeminiStreamTerminalObserve(t *testing.T) {
	var term geminiStreamTerminal
	term.observe([]byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"the key \"finishReason\" appears in text"}]}}]}`))
	if term.terminated() {
		t.Fatalf("正文里的 finishReason 字面量不得当证据（最小结构体解析）")
	}
	term.observe([]byte(`{"candidates":[{"content":{"role":"model","parts":[]},"finishReason":null}]}`))
	if term.terminated() {
		t.Fatalf("finishReason:null 不算终止证据")
	}
	term.observe([]byte(`{"candidates":[{"content":{"role":"model","parts":[]},"finishReason":"STOP"}]}`))
	if !term.terminated() {
		t.Fatalf("finishReason 非空 = 终止证据")
	}

	var t2 geminiStreamTerminal
	t2.observe([]byte(`{"candidates":[],"promptFeedback":{"blockReason":"SAFETY"}}`))
	if !t2.terminated() {
		t.Fatalf("promptFeedback.blockReason 非空 = 终止证据（禁止空回复静默成功）")
	}

	var t3 geminiStreamTerminal
	t3.observe([]byte(`{"error":{"code":503,"message":"busy"}}`))
	if !t3.terminated() {
		t.Fatalf("error 对象 = 终止证据（错误即终止）")
	}

	var t4 geminiStreamTerminal
	t4.observe([]byte(`[DONE]`))
	if t4.terminated() {
		t.Fatalf("[DONE] 非 Gemini 语义，不算终止证据（与解析器口径一致）")
	}
}

// TestE2EGeminiPassthroughTruncationFD1 直通流缺终止证据的 EOF = 截断流：
// SSE 下游收错误帧（不再静默结束）；数组下游不闭合（截断 JSON = 失败信号）。
// 反证：带 finishReason 的良构流不收错误帧、数组正常闭合。复现 P11 s8a/s8b。
func TestE2EGeminiPassthroughTruncationFD1(t *testing.T) {
	const frameText = `{"candidates":[{"content":{"role":"model","parts":[{"text":"Hello"}]},"index":0}],"modelVersion":"gemini-2.5-flash","responseId":"r-fd1"}`
	const framePartial = `{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"get_weather","partialArgs":[{"jsonPath":"$.city","stringValue":"San"}],"willContinue":true}}]},"index":0}],"responseId":"r-fd1"}`
	const frameFinish = `{"candidates":[{"content":{"role":"model","parts":[]},"finishReason":"STOP","index":0}],"responseId":"r-fd1"}`

	upTruncated := func(t *testing.T) *countUpstream {
		t.Helper()
		return newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			sseFrame(w, "", frameText)
			sseFrame(w, "", framePartial)
			// 干净 EOF（无 finishReason）——P11 s8a/s8b 的断流形态。
		})
	}
	upComplete := func(t *testing.T) *countUpstream {
		t.Helper()
		return newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			sseFrame(w, "", frameText)
			sseFrame(w, "", frameFinish)
		})
	}
	const gemBody = `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`

	t.Run("truncated_sse_error_frame", func(t *testing.T) {
		var logBuf syncLogBuffer
		prev := log.Writer()
		log.SetOutput(&logBuf)
		defer log.SetOutput(prev)

		up := upTruncated(t)
		s, srv := startServiceWith(t, cfgFor(up.srv.URL, ProtocolGemini, 15))
		resp := postJSON(t, srv.URL+"/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse", gemBody, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		body := string(raw)
		if !strings.Contains(body, `"text":"Hello"`) {
			t.Fatalf("已收帧必须原样转发:\n%s", body)
		}
		if !strings.Contains(body, `"error"`) || !strings.Contains(body, "upstream stream closed before finishReason") {
			t.Fatalf("截断流必须收错误帧（FD1）:\n%s", body)
		}
		if strings.Contains(body, "[DONE]") {
			t.Fatalf("gemini 出口绝不发哨兵:\n%s", body)
		}
		if rec := parseRequestRecord(t, logBuf.String()); len(rec.Attempts) != 1 || !rec.Attempts[0].Failed {
			t.Fatalf("attempt 记录 = %+v（want 单条 failed=true——上游侧失败）", rec)
		}
		if d := s.Diagnostics(); d.Attempts != 1 {
			t.Fatalf("diagnostics = %+v", d)
		}
	})

	t.Run("truncated_array_not_closed", func(t *testing.T) {
		up := upTruncated(t)
		srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))
		resp := postJSON(t, srv.URL+"/v1beta/models/gemini-2.5-flash:streamGenerateContent", gemBody, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		body := string(raw)
		if !strings.HasPrefix(body, "[") {
			t.Fatalf("数组形态必须以 [ 开始:\n%s", body)
		}
		if strings.HasSuffix(strings.TrimSpace(body), "]") {
			t.Fatalf("截断流不得闭合数组（闭合会被误读为正常完成）:\n%s", body)
		}
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err == nil {
			t.Fatalf("截断数组必须是非法 JSON（客户端可见的失败信号）:\n%s", body)
		}
		if !strings.Contains(body, `"text":"Hello"`) {
			t.Fatalf("元素丢失:\n%s", body)
		}
	})

	t.Run("complete_sse_clean", func(t *testing.T) {
		up := upComplete(t)
		srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))
		resp := postJSON(t, srv.URL+"/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse", gemBody, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		body := string(raw)
		if !strings.Contains(body, `"finishReason":"STOP"`) {
			t.Fatalf("终止帧丢失:\n%s", body)
		}
		if strings.Contains(body, `"error"`) {
			t.Fatalf("有终止证据的流不得收错误帧（反证）:\n%s", body)
		}
	})

	t.Run("complete_array_closed", func(t *testing.T) {
		up := upComplete(t)
		srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))
		resp := postJSON(t, srv.URL+"/v1beta/models/gemini-2.5-flash:streamGenerateContent", gemBody, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			t.Fatalf("良构流必须闭合为合法 JSON 数组（反证）: %v\n%s", err, raw)
		}
		if len(arr) != 2 {
			t.Fatalf("元素数 = %d（want 2）", len(arr))
		}
	})
}

// TestE2EGeminiPassthroughErrorLineNoDoubleError FD1 反证：直通流以上游错误行
// 终止（错误帧已原样转发、错误即终止）→ 不得再叠加第二个错误帧。
func TestE2EGeminiPassthroughErrorLineNoDoubleError(t *testing.T) {
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"partial"}]},"index":0}],"responseId":"r1"}`)
		fmt.Fprint(w, "{\"error\":{\"code\":503,\"message\":\"upstream overloaded\",\"status\":\"UNAVAILABLE\"}}\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))
	resp := postJSON(t, srv.URL+"/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse",
		`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	body := string(raw)
	if !strings.Contains(body, "upstream overloaded") {
		t.Fatalf("上游错误行必须原样转发:\n%s", body)
	}
	if strings.Count(body, `"error"`) != 1 {
		t.Fatalf("错误行已终止流，不得叠加第二个错误帧:\n%s", body)
	}
	if strings.Contains(body, "closed before finishReason") {
		t.Fatalf("错误即终止——不得再报缺 finishReason:\n%s", body)
	}
}

// ---------------------------------------------------------------------------
// FD2：错误路径哨兵纪律按出口格式契约分派
// ---------------------------------------------------------------------------

// TestE2EErrorPathSentinelFD2 已提交后失败：gemini 出口错误帧后**绝不发**
// `data: [DONE]`（P11 S11a 的张力修复）；openai chat 出口仍以哨兵收尾（锁定面
// 的对照组——partial_usage_test 已锁，这里再钉一次分流不改动它）。
func TestE2EErrorPathSentinelFD2(t *testing.T) {
	t.Run("gemini_no_sentinel", func(t *testing.T) {
		up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"Hello"}]},"index":0}],"responseId":"r-fd2"}`)
			panic(http.ErrAbortHandler) // 已提交后断连（RST）
		})
		srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))
		resp := postJSON(t, srv.URL+"/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse",
			`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d（已提交后错误必须 200 + 流内错误帧）", resp.StatusCode)
		}
		body := string(raw)
		if !strings.Contains(body, "upstream stream read failed") {
			t.Fatalf("错误帧缺失:\n%s", body)
		}
		if strings.Contains(body, "[DONE]") {
			t.Fatalf("gemini 出口绝不发哨兵（协议污染）:\n%s", body)
		}
	})

	t.Run("openai_drops_sentinel", func(t *testing.T) {
		up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_fd2","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":0}}}`)
			panic(http.ErrAbortHandler)
		})
		srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))
		resp := postJSON(t, srv.URL+"/v1/chat/completions",
			`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		body := string(raw)
		// W5-26 改判（原 FD2「openai 面错误后仍须哨兵收尾」）：错误帧即终态，
		// 补哨兵会让哨兵唯读的消费端把截断流当正常完成（实测见
		// tmp/lanes/L19-sweep9/proofs/done-after-error.txt）。
		if !strings.Contains(body, `"error"`) || strings.Contains(body, "[DONE]") {
			t.Fatalf("openai 面错误帧后不得补哨兵（W5-26 裁决）:\n%s", body)
		}
	})
}

// ---------------------------------------------------------------------------
// 条目 3：per-exception 换上游预算（配置面）+ 失败率冷却
// ---------------------------------------------------------------------------

// TestE2EPerExceptionFailoverBudget 三台上游全 429：`rate_limit: 1` → 最多换
// 1 次（共打 2 台），随后原错误信封上抛（不再打第三台）；缺省不限次（对照组）；
// `bad_request: 1` 把缺省不换的 400 打开（对照：缺省 400 不换）。
func TestE2EPerExceptionFailoverBudget(t *testing.T) {
	triple := func(t *testing.T) ([]*countUpstream, *Config) {
		t.Helper()
		u1 := failUpstream(t, http.StatusTooManyRequests, nil, errBodyAPIError)
		u2 := failUpstream(t, http.StatusTooManyRequests, nil, errBodyAPIError)
		u3 := failUpstream(t, http.StatusTooManyRequests, nil, errBodyAPIError)
		cfg := failoverCfg(anthUp("u1", u1.srv.URL), anthUp("u2", u2.srv.URL), anthUp("u3", u3.srv.URL))
		return []*countUpstream{u1, u2, u3}, cfg
	}
	const chatBody = `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`

	t.Run("budget_one_stops_after_second", func(t *testing.T) {
		ups, cfg := triple(t)
		cfg.FailoverPolicy = map[string]int{"rate_limit": 1}
		s, srv := startServiceWith(t, cfg)
		resp := postJSON(t, srv.URL+"/v1/chat/completions", chatBody, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("status = %d（预算耗尽后原错误上抛）", resp.StatusCode)
		}
		if !strings.Contains(string(raw), "boom") {
			t.Fatalf("错误体丢失:\n%s", raw)
		}
		if ups[0].count() != 1 || ups[1].count() != 1 || ups[2].count() != 0 {
			t.Fatalf("hits = %d/%d/%d（want 1/1/0——预算 1 = 最多换 1 次）",
				ups[0].count(), ups[1].count(), ups[2].count())
		}
		if d := s.Diagnostics(); d.Failovers != 1 {
			t.Fatalf("failovers = %d（want 1）", d.Failovers)
		}
	})

	t.Run("default_unlimited", func(t *testing.T) {
		ups, cfg := triple(t)
		_, srv := startServiceWith(t, cfg)
		resp := postJSON(t, srv.URL+"/v1/chat/completions", chatBody, nil)
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if ups[0].count() != 1 || ups[1].count() != 1 || ups[2].count() != 1 {
			t.Fatalf("hits = %d/%d/%d（want 1/1/1——缺省内置换上游类不限次）",
				ups[0].count(), ups[1].count(), ups[2].count())
		}
	})

	t.Run("bad_request_default_vs_enabled", func(t *testing.T) {
		// anthropic 协议上游的合法非流式成功体（good 上游）。
		const okBody = `{"id":"msg_ok","type":"message","role":"assistant","model":"claude-x","content":[{"type":"text","text":"Hi"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}`
		bad := failUpstream(t, http.StatusBadRequest, nil, errBodyAPIError)
		good := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(okBody))
		})
		base := func() *Config {
			return failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL))
		}

		// 缺省：400 是请求问题——不换，原信封上抛。
		_, srv1 := startServiceWith(t, base())
		resp1 := postJSON(t, srv1.URL+"/v1/chat/completions", chatBody, nil)
		raw1, _ := io.ReadAll(resp1.Body)
		resp1.Body.Close()
		if resp1.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d（缺省 400 不换）", resp1.StatusCode)
		}
		if !strings.Contains(string(raw1), "boom") {
			t.Fatalf("400 错误体必须上抛:\n%s", raw1)
		}
		if good.count() != 0 {
			t.Fatalf("缺省 400 不得打第二台（hits=%d）", good.count())
		}

		// bad_request: 1：按预算换（显式开启）。
		cfg2 := base()
		cfg2.FailoverPolicy = map[string]int{"bad_request": 1}
		_, srv2 := startServiceWith(t, cfg2)
		resp2 := postJSON(t, srv2.URL+"/v1/chat/completions", chatBody, nil)
		raw2, _ := io.ReadAll(resp2.Body)
		resp2.Body.Close()
		if resp2.StatusCode != 200 || !strings.Contains(string(raw2), `"Hi"`) {
			t.Fatalf("status = %d body=\n%s", resp2.StatusCode, raw2)
		}
		if bad.count() != 2 || good.count() != 1 {
			t.Fatalf("hits = bad:%d good:%d（want 2/1）", bad.count(), good.count())
		}
	})
}

// TestFailureRateCooldownUnit 失败率冷却（LiteLLM 范式最小版）：窗口内请求 ≥ 5
// 且失败率 > 0.5 触发；min 守卫保证小样本不触发；窗口过期重置；enabled:false 关。
func TestFailureRateCooldownUnit(t *testing.T) {
	newCfg := func() *Config {
		cfg := failoverCfg(anthUp("a", "http://127.0.0.1:1"), anthUp("b", "http://127.0.0.1:1"))
		cfg.CooldownFailures = 100 // 隔离：只测失败率路径（连续失败阈值不参与）
		cfg.CooldownSeconds = 30
		cfg.FailureRate = &FailureRateCooldown{MinRequests: 5, Threshold: 0.5, WindowSeconds: 60}
		return cfg
	}
	s := New(newCfg())
	// 4 次请求 3 失败：率 0.75 但样本 < 5——不触发（min 守卫）。
	s.recordSuccess(0)
	s.recordFailure(0, 0)
	s.recordFailure(0, 0)
	s.recordFailure(0, 0)
	if s.cooling(0, time.Now()) {
		t.Fatalf("样本 < min 不得触发失败率冷却")
	}
	if d := s.Diagnostics(); d.RateCooldownTrips != 0 {
		t.Fatalf("rate trips = %d（want 0）", d.RateCooldownTrips)
	}
	// 第 5 次失败：5 请求 4 失败 = 0.8 > 0.5——触发。
	s.recordFailure(0, 0)
	if !s.cooling(0, time.Now()) {
		t.Fatalf("窗口 5 请求 4 失败（0.8 > 0.5）必须触发失败率冷却")
	}
	if d := s.Diagnostics(); d.RateCooldownTrips != 1 {
		t.Fatalf("rate trips = %d（want 1）", d.RateCooldownTrips)
	}

	// 窗口过期重置：伪造窗口起点为 2 分钟前，再失败一次——重置后样本 1，不触发。
	s2 := New(newCfg())
	s2.health[0].winStart = time.Now().Add(-2 * time.Minute)
	s2.recordFailure(0, 0)
	if s2.cooling(0, time.Now()) {
		t.Fatalf("窗口过期后不得触发（样本重置为 1）")
	}
	if s2.health[0].winReq != 1 || s2.health[0].winFail != 1 {
		t.Fatalf("window = %d/%d（want 1/1——过期重置）", s2.health[0].winReq, s2.health[0].winFail)
	}

	// enabled: false：同序列不触发。
	off := false
	cfgOff := newCfg()
	cfgOff.FailureRate = &FailureRateCooldown{Enabled: &off}
	s3 := New(cfgOff)
	for range 5 {
		s3.recordFailure(0, 0)
	}
	if s3.cooling(0, time.Now()) {
		t.Fatalf("enabled:false 不得触发失败率冷却")
	}
}

// TestE2EFailureRateCooldownSkipsUpstream 失败率冷却经真实请求链触发：脚本化
// 上游命中序 fail/fail/ok/fail/fail（第 5 次失败时窗口 5 请求 4 失败 > 0.5）→
// 冷却；第 6 次请求跳过该上游（CooldownSkips + 不再命中）。
func TestE2EFailureRateCooldownSkipsUpstream(t *testing.T) {
	// anthropic 协议上游的合法非流式成功体。
	const okBody = `{"id":"msg_ok","type":"message","role":"assistant","model":"claude-x","content":[{"type":"text","text":"Hi"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}`
	var mu sync.Mutex
	hits := 0
	scripted := []int{500, 500, 200, 500, 500}
	u1 := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		i := hits
		hits++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		code := 200
		if i < len(scripted) {
			code = scripted[i]
		}
		if code == 200 {
			_, _ = w.Write([]byte(okBody))
			return
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(errBodyAPIError))
	})
	u2 := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(okBody))
	})
	cfg := failoverCfg(anthUp("u1", u1.srv.URL), anthUp("u2", u2.srv.URL))
	cfg.CooldownFailures = 100 // 隔离：只测失败率路径
	cfg.CooldownSeconds = 60
	s, srv := startServiceWith(t, cfg)

	const chatBody = `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`
	for i := range 5 {
		resp := postJSON(t, srv.URL+"/v1/chat/completions", chatBody, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || !strings.Contains(string(raw), "Hi") {
			t.Fatalf("req %d: status=%d body=%s", i+1, resp.StatusCode, raw)
		}
	}
	if u1.count() != 5 {
		t.Fatalf("u1 hits = %d（want 5）", u1.count())
	}
	if d := s.Diagnostics(); d.RateCooldownTrips != 1 {
		t.Fatalf("rate trips = %d（want 1——第 5 次失败触发）", d.RateCooldownTrips)
	}

	// 第 6 次：u1 在冷却窗口内被跳过（CooldownSkips + 不再命中 u1）。
	resp := postJSON(t, srv.URL+"/v1/chat/completions", chatBody, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("req 6: status=%d body=%s", resp.StatusCode, raw)
	}
	if u1.count() != 5 {
		t.Fatalf("冷却中的 u1 不得再命中（hits=%d）", u1.count())
	}
	if u2.count() != 5 {
		t.Fatalf("u2 hits = %d（want 5：req1/2/4/5 换上游 + req6）", u2.count())
	}
	if d := s.Diagnostics(); d.CooldownSkips != 1 {
		t.Fatalf("cooldown skips = %d（want 1）", d.CooldownSkips)
	}
}

// ---------------------------------------------------------------------------
// 条目 4：错误体兜底提取（one-api 7 路径表——最小信息抢救）
// ---------------------------------------------------------------------------

// TestE2EErrorBodySalvage 上游错误体形态各异（无标准 error 信封）时，兜底提取
// 仍把 message 带进下游信封（跨协议错误映射链：convert 的 ParseError 回退分支）；
// HTTP 层字段（status/retry-after）照常透传。
func TestE2EErrorBodySalvage(t *testing.T) {
	t.Run("claude_entry_openai_upstream_msg_shape", func(t *testing.T) {
		up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"msg":"slow down, please retry","code":429}`))
		})
		srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))
		resp := postJSON(t, srv.URL+"/v1/messages",
			`{"model":"gpt-x","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if resp.Header.Get("retry-after") != "3" {
			t.Fatalf("retry-after = %q（HTTP 层字段透传面）", resp.Header.Get("retry-after"))
		}
		body := string(raw)
		if !strings.Contains(body, "slow down, please retry") {
			t.Fatalf("兜底提取的 message 丢失:\n%s", body)
		}
		if strings.Contains(body, "non-standard") {
			t.Fatalf("不得落到泛型消息:\n%s", body)
		}
	})

	t.Run("openai_entry_gemini_upstream_header_shape", func(t *testing.T) {
		up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"header":{"message":"blocked by edge"}}`))
		})
		srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))
		resp := postJSON(t, srv.URL+"/v1/chat/completions",
			`{"model":"gemini-2.5-flash","messages":[{"role":"user","content":"hi"}]}`, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d（普通 403 不换上游）", resp.StatusCode)
		}
		if !strings.Contains(string(raw), "blocked by edge") {
			t.Fatalf("header.message 兜底提取丢失:\n%s", raw)
		}
	})

	t.Run("openai_entry_claude_upstream_nested_shape", func(t *testing.T) {
		up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"response":{"error":{"message":"nested salvage"}}}`))
		})
		srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))
		resp := postJSON(t, srv.URL+"/v1/chat/completions",
			`{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}`, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if !strings.Contains(string(raw), "nested salvage") {
			t.Fatalf("response.error.message 兜底提取丢失:\n%s", raw)
		}
	})
}
