// W7-1（A2A-L19-W7-1）：两个默认关实现件的验收测试——
//
//	① `upstream_idle_timeout_seconds`（上游内容静默守卫：假活跃/零字节两形态；
//	   预提交换上游 / 提交后 incomplete_stream；缺省关不被误触发）；
//	② `debug_capture_dir`（debug 体录制：掩码 + 开关 + 容量上限 + 文件名含 req id）。
//
// 反证（回退即红）：把 UpstreamIdleTimeout() 改回恒 0 / maskHeaders 改回原样，
// 本文件的 TestE2EUpstreamIdleTimeout{PostCommit,PreCommitFailover} 与
// TestDebugCaptureOnWritesMaskedFile 即红（见报告 §反证）。
package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dwgx/any2api/translate/registry"
)

// postClaudeStreamC 发 claude 流式请求并读回整体（自定 client——反证场景里守卫
// 缺失时以客户端超时失败，而不是测试挂死）。
func postClaudeStreamC(t *testing.T, base string, c *http.Client) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/v1/messages",
		strings.NewReader(`{"model":"claude-x","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// waitFor 轮询等待条件成立（测试专用；超时即失败）。
func waitFor(t *testing.T, d time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("waitFor: 条件在 %s 内未成立", d)
}

// claudeIdleStreamHead 写「内容帧」头（message_start + 文本 delta "Hello "）。
func claudeIdleStreamHead(w http.ResponseWriter) {
	sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_idle","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":0}}}`)
	sseFrame(w, "content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
	sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello "}}`)
}

// claudeIdleStreamTail 写正常收尾帧（text "world" + stop）。
func claudeIdleStreamTail(w http.ResponseWriter) {
	sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"world"}}`)
	sseFrame(w, "content_block_stop", `{"type":"content_block_stop","index":0}`)
	sseFrame(w, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":3,"output_tokens":2}}`)
	sseFrame(w, "message_stop", `{"type":"message_stop"}`)
}

// ---------------------------------------------------------------------------
// ① upstream_idle_timeout_seconds
// ---------------------------------------------------------------------------

// TestE2EUpstreamIdleTimeoutPostCommit 提交后假活跃（上游只发 keepalive/ping，
// 不再有任何内容帧）：idle 到点 → 下游收到 `incomplete_stream` 错误帧收尾
// （不重放、不换上游）；流以错误收束而不是无限挂住。
//
// 反证（回退即红）：去掉守卫 → ping 无限发，客户端在 10s 超时点读失败（本测试
// 以 client 超时兜底），断言「incomplete_stream 帧」失败。
func TestE2EUpstreamIdleTimeoutPostCommit(t *testing.T) {
	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		claudeIdleStreamHead(w) // 内容 → 提交点已过
		tk := time.NewTicker(200 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-tk.C:
				sseFrame(w, "ping", `{"type":"ping"}`) // keepalive：不算内容
			}
		}
	})
	cfg := failoverCfg(anthUp("pingy", up.srv.URL))
	cfg.UpstreamIdleTimeoutSeconds = 1
	_, srv := startServiceWith(t, cfg)

	start := time.Now()
	status, raw := postClaudeStreamC(t, srv.URL, &http.Client{Timeout: 10 * time.Second})
	elapsed := time.Since(start)
	if status != 200 {
		t.Fatalf("status = %d\n%s", status, raw)
	}
	if !strings.Contains(raw, "Hello ") {
		t.Fatalf("提交前内容应已下发:\n%s", raw)
	}
	if !strings.Contains(raw, `"incomplete_stream"`) || !strings.Contains(raw, "idle for 1s") {
		t.Fatalf("应收到 idle 超时的 incomplete_stream 错误帧:\n%s", raw)
	}
	if strings.Contains(raw, "message_stop") {
		t.Fatalf("假活跃流不得有正常收尾:\n%s", raw)
	}
	if elapsed > 8*time.Second {
		t.Fatalf("elapsed = %s（idle 守卫未生效？）", elapsed)
	}
	if got := up.count(); got != 1 {
		t.Fatalf("上游尝试次数 = %d, want 1（提交后不重试/不换上游）", got)
	}
}

// TestE2EUpstreamIdleTimeoutPreCommitFailover 提交前零字节挂住（响应头已到、
// 之后一个字节都不发；first_byte_timeout 保持缺省关——只有 idle 守卫能救）：
// 到点 → 未提交 → 按 C1 换下一个上游，下游拿到第二台的完整流。
//
// 反证（回退即红）：去掉守卫 → 第一台无限挂住，客户端超时（测试红）。
func TestE2EUpstreamIdleTimeoutPreCommitFailover(t *testing.T) {
	upHang := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done() // 挂住：零产出
	})
	upGood := goodClaudeUpstream(t)
	cfg := failoverCfg(anthUp("hang", upHang.srv.URL), anthUp("good", upGood.srv.URL))
	cfg.UpstreamIdleTimeoutSeconds = 1
	if cfg.FirstByteTimeoutSeconds != 0 {
		t.Fatalf("本测试前提：first_byte 缺省关（0），idle 是唯一出口")
	}
	s, srv := startServiceWith(t, cfg)

	start := time.Now()
	status, raw := postClaudeStreamC(t, srv.URL, &http.Client{Timeout: 10 * time.Second})
	elapsed := time.Since(start)
	if status != 200 || !strings.Contains(raw, "Hello ") || !strings.Contains(raw, "message_stop") {
		t.Fatalf("下游应是第二台的完整流: status=%d\n%s", status, raw)
	}
	if elapsed > 8*time.Second {
		t.Fatalf("elapsed = %s（idle 守卫未生效？）", elapsed)
	}
	if upHang.count() != 1 || upGood.count() != 1 {
		t.Fatalf("命中数: hang=%d good=%d, want 1/1", upHang.count(), upGood.count())
	}
	d := s.Diagnostics()
	if d.Attempts != 2 || d.Failovers != 1 {
		t.Fatalf("diagnostics = %+v（want Attempts=2 Failovers=1）", d)
	}
}

// TestE2EUpstreamIdleTimeoutDisabledByDefault 缺省（0 = 关）锁：上游先内容、
// 中间 2s 只发 ping（合法长静默形态）、再补内容正常收尾——守卫缺省关，全流
// 成功、无错误帧。这是「合法 keepalive 流不被误杀」的锁（开了 idle 且阈值 <2s
// 时本场景会红——正是配置者的显式选择）。
func TestE2EUpstreamIdleTimeoutDisabledByDefault(t *testing.T) {
	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		claudeIdleStreamHead(w)
		for range 4 {
			time.Sleep(500 * time.Millisecond)
			sseFrame(w, "ping", `{"type":"ping"}`)
		}
		claudeIdleStreamTail(w)
	})
	cfg := failoverCfg(anthUp("pingy", up.srv.URL))
	if cfg.UpstreamIdleTimeoutSeconds != 0 {
		t.Fatalf("缺省必须为 0（关）: %d", cfg.UpstreamIdleTimeoutSeconds)
	}
	_, srv := startServiceWith(t, cfg)

	status, raw := postClaudeStreamC(t, srv.URL, &http.Client{Timeout: 15 * time.Second})
	if status != 200 {
		t.Fatalf("status = %d\n%s", status, raw)
	}
	if !strings.Contains(raw, "Hello ") || !strings.Contains(raw, "world") || !strings.Contains(raw, "message_stop") {
		t.Fatalf("合法 keepalive 流应完整通过（缺省关）:\n%s", raw)
	}
	if strings.Contains(raw, "incomplete_stream") {
		t.Fatalf("缺省关不得写错误帧:\n%s", raw)
	}
}

// TestUpstreamIdleGuardUnit 守卫本体：<=0 = nil（缺省关，零开销）；nil 安全；
// 无 touch → 到点触发并取消 ctx；持续 touch → 不触发。
func TestUpstreamIdleGuardUnit(t *testing.T) {
	if g := newUpstreamIdleGuard(context.Background(), func() {}, 0); g != nil {
		t.Fatalf("timeout<=0 应返回 nil（缺省关）")
	}
	var nilG *upstreamIdleGuard
	nilG.touch()
	if nilG.TimedOut() {
		t.Fatalf("nil 守卫不得报超时")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := newUpstreamIdleGuard(ctx, cancel, 200*time.Millisecond)
	waitFor(t, 3*time.Second, func() bool { return g.TimedOut() })
	if ctx.Err() == nil {
		t.Fatalf("触发时应取消上游 ctx")
	}
	g.stop()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	g2 := newUpstreamIdleGuard(ctx2, cancel2, 300*time.Millisecond)
	for range 6 {
		time.Sleep(100 * time.Millisecond)
		g2.touch()
	}
	if g2.TimedOut() {
		t.Fatalf("持续 touch 期间不得触发")
	}
	g2.stop()
}

// TestFrameCarriesContent 内容判定表（touch 判据；保守方向 = 认不出算内容）。
func TestFrameCarriesContent(t *testing.T) {
	cases := []struct {
		name  string
		proto registry.Format
		event string
		data  string
		want  bool
	}{
		{"claude_ping_event", registry.FormatClaudeMessages, "ping", `{"type":"ping"}`, false},
		{"claude_ping_payload", registry.FormatClaudeMessages, "", `{"type":"ping"}`, false},
		{"claude_message_start", registry.FormatClaudeMessages, "message_start", `{"type":"message_start"}`, true},
		{"claude_text_delta", registry.FormatClaudeMessages, "content_block_delta", `{"type":"content_block_delta","delta":{"type":"text_delta","text":"x"}}`, true},
		{"claude_invalid_json", registry.FormatClaudeMessages, "content_block_delta", `not json`, true},
		{"openai_empty_delta", registry.FormatOpenAIChat, "", `{"choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}`, false},
		{"openai_content_delta", registry.FormatOpenAIChat, "", `{"choices":[{"delta":{"content":"hi"},"finish_reason":null}]}`, true},
		{"openai_empty_string_content", registry.FormatOpenAIChat, "", `{"choices":[{"delta":{"content":""},"finish_reason":null}]}`, true},
		{"openai_reasoning_delta", registry.FormatOpenAIChat, "", `{"choices":[{"delta":{"reasoning_content":"think"},"finish_reason":null}]}`, true},
		{"openai_finish_reason", registry.FormatOpenAIChat, "", `{"choices":[{"delta":{},"finish_reason":"stop"}]}`, true},
		{"openai_usage_only", registry.FormatOpenAIChat, "", `{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}`, true},
		{"openai_done", registry.FormatOpenAIChat, "", `[DONE]`, false},
		{"responses_created", registry.FormatOpenAIResponses, "", `{"type":"response.created"}`, false},
		{"responses_in_progress", registry.FormatOpenAIResponses, "", `{"type":"response.in_progress"}`, false},
		{"responses_queued", registry.FormatOpenAIResponses, "", `{"type":"response.queued"}`, false},
		{"responses_output_delta", registry.FormatOpenAIResponses, "", `{"type":"response.output_text.delta","delta":"hi"}`, true},
		{"responses_completed", registry.FormatOpenAIResponses, "", `{"type":"response.completed"}`, true},
		{"gemini_empty_candidates", registry.FormatGeminiGenerate, "", `{"candidates":[{"content":{"role":"model"},"index":0}]}`, false},
		{"gemini_text", registry.FormatGeminiGenerate, "", `{"candidates":[{"content":{"parts":[{"text":"hi"}]}}]}`, true},
		{"gemini_finish", registry.FormatGeminiGenerate, "", `{"candidates":[{"finishReason":"STOP"}]}`, true},
		{"gemini_error", registry.FormatGeminiGenerate, "", `{"error":{"code":500,"message":"boom"}}`, true},
		{"interactions_unknown", registry.FormatInteractions, "", `{"whatever":1}`, true},
		{"empty_data", registry.FormatClaudeMessages, "", ``, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := frameCarriesContent(tc.proto, tc.event, []byte(tc.data)); got != tc.want {
				t.Fatalf("frameCarriesContent(%s, %q, %q) = %v, want %v", tc.proto, tc.event, tc.data, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ② debug_capture_dir
// ---------------------------------------------------------------------------

// TestDebugCaptureOnWritesMaskedFile 开关开：一次流式请求落一份 `<req_id>.json`
// （文件名 = 响应 request-id 头），含下游请求/响应体 + 上游尝试（请求/响应体）；
// **凭据头一律掩码**（下游 Authorization/x-api-key、上游 x-api-key 的值不得出现）；
// 非凭据头保留；录制不破坏流（下游仍拿到完整 message_stop 流）。
//
// 反证（回退即红）：maskHeaders 改回原样 → 凭据明文落盘 → 本测试红。
func TestDebugCaptureOnWritesMaskedFile(t *testing.T) {
	dir := t.TempDir()
	up := goodClaudeUpstream(t)
	cfg := failoverCfg(anthUp("fake", up.srv.URL))
	cfg.DebugCaptureDir = dir
	_, srv := startServiceWith(t, cfg)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages",
		strings.NewReader(`{"model":"claude-x","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer super-secret-token")
	req.Header.Set("X-Api-Key", "client-secret-key")
	req.Header.Set("X-Custom-Trace", "keep-me")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(raw), "message_stop") {
		t.Fatalf("录制不得破坏流:\n%s", raw)
	}
	reqID := resp.Header.Get("request-id")
	if reqID == "" {
		t.Fatalf("缺 request-id 头（文件名关联面）")
	}

	path := filepath.Join(dir, reqID+".json")
	waitFor(t, 3*time.Second, func() bool { _, err := os.Stat(path); return err == nil })
	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read capture: %v", err)
	}
	s := string(blob)

	// 掩码：任何凭据值不得落盘（下游两枚 + 上游 x-api-key=k-fake）。
	for _, secret := range []string{"super-secret-token", "client-secret-key", "k-fake"} {
		if strings.Contains(s, secret) {
			t.Fatalf("凭据泄漏 %q:\n%s", secret, s)
		}
	}
	if !strings.Contains(s, `"Authorization": "***"`) || !strings.Contains(s, `"X-Api-Key": "***"`) {
		t.Fatalf("凭据头应掩码为 ***:\n%s", s)
	}
	if !strings.Contains(s, "keep-me") {
		t.Fatalf("非凭据头应保留:\n%s", s)
	}
	// 体与上游尝试在场。
	if !strings.Contains(s, "messages") || !strings.Contains(s, "Hello ") {
		t.Fatalf("下游请求/响应体应落盘:\n%s", s)
	}
	if !strings.Contains(s, `"upstream": "fake"`) || !strings.Contains(s, `"response_status": 200`) {
		t.Fatalf("上游尝试应落盘:\n%s", s)
	}
	if !strings.Contains(s, "event: message_start") {
		t.Fatalf("上游响应原始字节应落盘:\n%s", s)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("目录应恰一份录制: err=%v entries=%d", err, len(entries))
	}
}

// TestDebugCaptureOffByDefault 开关缺省关：同目录先开录制跑一次（1 文件），
// 再以缺省配置跑一次——请求成功且目录不增长。
func TestDebugCaptureOffByDefault(t *testing.T) {
	dir := t.TempDir()
	up := goodClaudeUpstream(t)

	cfgOn := failoverCfg(anthUp("fake", up.srv.URL))
	cfgOn.DebugCaptureDir = dir
	_, srvOn := startServiceWith(t, cfgOn)
	if status, raw := postClaudeStreamC(t, srvOn.URL, &http.Client{Timeout: 10 * time.Second}); status != 200 || !strings.Contains(raw, "message_stop") {
		t.Fatalf("录制开启请求失败: %d\n%s", status, raw)
	}
	waitFor(t, 3*time.Second, func() bool {
		entries, err := os.ReadDir(dir)
		return err == nil && len(entries) == 1
	})

	cfgOff := failoverCfg(anthUp("fake", up.srv.URL))
	if cfgOff.DebugCaptureDir != "" {
		t.Fatalf("缺省必须为空: %q", cfgOff.DebugCaptureDir)
	}
	_, srvOff := startServiceWith(t, cfgOff)
	if status, raw := postClaudeStreamC(t, srvOff.URL, &http.Client{Timeout: 10 * time.Second}); status != 200 || !strings.Contains(raw, "message_stop") {
		t.Fatalf("缺省关请求失败: %d\n%s", status, raw)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("缺省关不得落盘: err=%v entries=%d", err, len(entries))
	}
}

// TestCaptureMasking 掩码表单元测试：头名大小写不敏感；查询串凭据参数掩码、
// 非凭据参数保留；解析不了的查询串整体掩码。
func TestCaptureMasking(t *testing.T) {
	h := http.Header{}
	h.Set("Authorization", "Bearer abc")
	h.Set("x-api-key", "sk-xyz")
	h.Set("X-Custom", "v")
	m := maskHeaders(h)
	if m["Authorization"] != "***" || m["X-Api-Key"] != "***" || m["X-Custom"] != "v" {
		t.Fatalf("maskHeaders = %v", m)
	}

	q := maskQuery("key=SECRET&alt=sse&token=tok123&x=1")
	if strings.Contains(q, "SECRET") || strings.Contains(q, "tok123") {
		t.Fatalf("查询串凭据未掩码: %q", q)
	}
	if !strings.Contains(q, "alt=sse") || !strings.Contains(q, "x=1") {
		t.Fatalf("非凭据参数应保留: %q", q)
	}
	if got := maskQuery("%zz"); got != "***" {
		t.Fatalf("解析失败应整体掩码: %q", got)
	}
}

// TestCaptureBodyCap 容量纪律：单体积上限 1 MiB，超限截断并置标记（请求体 /
// 上游请求体 / 上游响应体三面）。
func TestCaptureBodyCap(t *testing.T) {
	c := newRequestCapture(t.TempDir(), httptest.NewRequest(http.MethodPost, "/x", nil))
	big := bytes.Repeat([]byte("a"), debugCaptureBodyCap+10)
	c.setRequestBody(big)
	if !c.reqTrunc || len(c.reqBody) != debugCaptureBodyCap {
		t.Fatalf("请求体上限未生效: trunc=%v len=%d", c.reqTrunc, len(c.reqBody))
	}
	att := c.addAttempt("u", "http://x/v1", http.Header{}, big)
	if !att.RequestTrunc || len(att.RequestBody) != debugCaptureBodyCap {
		t.Fatalf("上游请求体上限未生效: trunc=%v len=%d", att.RequestTrunc, len(att.RequestBody))
	}
	att.appendResponseBody(big)
	att.appendResponseBody([]byte("tail"))
	if !att.trunc || len(att.raw) != debugCaptureBodyCap {
		t.Fatalf("上游响应体上限未生效: trunc=%v len=%d", att.trunc, len(att.raw))
	}
}
