package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dwgx/any2api/translate/convert/claude_messages"
	"github.com/dwgx/any2api/translate/convert/openai_chat"
	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

// sseFrame 向上游响应写一帧并 flush。
func sseFrame(w http.ResponseWriter, event, data string) {
	if event != "" {
		fmt.Fprintf(w, "event: %s\n", event)
	}
	fmt.Fprintf(w, "data: %s\n\n", data)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// startService 启动被测服务（真 HTTP，本地回环）。
func startService(t *testing.T, cfg *Config) *httptest.Server {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	srv := httptest.NewServer(New(cfg).Handler())
	t.Cleanup(srv.Close)
	return srv
}

// postJSON 发 JSON 请求（真 HTTP 客户端）。
func postJSON(t *testing.T, url, body string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}

// claudeUpstream 假 Anthropic 上游：记录收到的请求，handler 负责写流。
type claudeUpstream struct {
	srv  *httptest.Server
	mu   sync.Mutex
	body []byte
	path string
	auth string
}

func newClaudeUpstream(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) *claudeUpstream {
	t.Helper()
	cu := &claudeUpstream{}
	cu.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		cu.mu.Lock()
		cu.body = b
		cu.path = r.URL.Path
		cu.auth = r.Header.Get("x-api-key")
		cu.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(cu.srv.Close)
	return cu
}

func (cu *claudeUpstream) request() (path string, body []byte, auth string) {
	cu.mu.Lock()
	defer cu.mu.Unlock()
	return cu.path, cu.body, cu.auth
}

// cfgFor 单上游配置。
func cfgFor(baseURL string, proto Protocol, hb int) *Config {
	return &Config{
		Listen:           "127.0.0.1:0",
		HeartbeatSeconds: hb,
		Upstreams: []Upstream{{
			Name: "fake", BaseURL: baseURL, APIKey: "test-key", Protocol: proto,
		}},
	}
}

// claudeTextStream 标准文本流帧（假上游用）。
func claudeTextStream(w http.ResponseWriter) {
	sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_u1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":11,"cache_read_input_tokens":3,"output_tokens":0}}}`)
	sseFrame(w, "content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
	sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello "}}`)
	sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"world"}}`)
	sseFrame(w, "content_block_stop", `{"type":"content_block_stop","index":0}`)
	sseFrame(w, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":11,"cache_read_input_tokens":3,"output_tokens":2}}`)
	sseFrame(w, "message_stop", `{"type":"message_stop"}`)
}

// ---------------------------------------------------------------------------
// e2e 1：文本流（openai 入口 → anthropic 上游 → 下游 openai SSE）
// ---------------------------------------------------------------------------

func TestE2ETextStream(t *testing.T) {
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		claudeTextStream(w)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}
	if resp.Header.Get("request-id") == "" {
		t.Fatalf("request-id missing (CC B10)")
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.HasSuffix(raw, []byte("data: [DONE]\n\n")) {
		t.Fatalf("missing [DONE]:\n%s", raw)
	}
	events, err := openai_chat.ParseStream(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("downstream parse: %v\n%s", err, raw)
	}
	var text strings.Builder
	var usage *ir.Usage
	var reason ir.StopReason
	for _, ev := range events {
		if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockText {
			text.WriteString(ev.TextDelta)
		}
		if ev.Usage != nil {
			usage = ev.Usage
		}
		if ev.Kind == ir.EventDone {
			reason = ev.Reason
		}
	}
	if text.String() != "Hello world" {
		t.Fatalf("text = %q", text.String())
	}
	if reason != ir.StopStop {
		t.Fatalf("done reason = %q", reason)
	}
	if usage == nil || usage.Input != 11 || usage.Output != 2 || usage.Total != 16 || usage.CacheRead != 3 {
		t.Fatalf("usage = %+v", usage)
	}
	// 上游收到的是转换后的请求（max_tokens 缺省合成、system 无、模型直传）。
	path, body, auth := up.request()
	if path != "/v1/messages" || auth != "test-key" {
		t.Fatalf("upstream got path=%q auth=%q", path, auth)
	}
	var upReq struct {
		Model     string `json:"model"`
		MaxTokens int    `json:"max_tokens"`
		Messages  []struct {
			Role string `json:"role"`
		} `json:"messages"`
		Stream bool `json:"stream"`
	}
	if err := json.Unmarshal(body, &upReq); err != nil {
		t.Fatalf("upstream body: %v\n%s", err, body)
	}
	if upReq.Model != "gpt-x" || upReq.MaxTokens != 32000 || len(upReq.Messages) != 1 ||
		upReq.Messages[0].Role != "user" || !upReq.Stream {
		t.Fatalf("upstream request = %+v", upReq)
	}
}

// ---------------------------------------------------------------------------
// e2e 2：tool_calls 流（input_json_delta 分片 → 下游 tool_calls 帧）
// ---------------------------------------------------------------------------

func TestE2EToolCallsStream(t *testing.T) {
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_t1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":5,"output_tokens":0}}}`)
		sseFrame(w, "content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call_a","name":"get_weather","input":{}}}`)
		sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"city\":"}}`)
		sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"Paris\"}"}}`)
		sseFrame(w, "content_block_stop", `{"type":"content_block_stop","index":0}`)
		sseFrame(w, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"input_tokens":5,"output_tokens":9}}`)
		sseFrame(w, "message_stop", `{"type":"message_stop"}`)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"weather?"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	events, err := openai_chat.ParseStream(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("downstream parse: %v\n%s", err, raw)
	}
	calls, err := ir.AssembleToolCalls(events)
	if err != nil {
		t.Fatalf("assemble: %v\n%s", err, raw)
	}
	if len(calls) != 1 || calls[0].ID != "call_a" || calls[0].Name != "get_weather" ||
		calls[0].Arguments != `{"city":"Paris"}` {
		t.Fatalf("calls = %+v\n%s", calls, raw)
	}
	var reason ir.StopReason
	for _, ev := range events {
		if ev.Kind == ir.EventDone {
			reason = ev.Reason
		}
	}
	if reason != ir.StopToolUse {
		t.Fatalf("done reason = %q", reason)
	}
}

// ---------------------------------------------------------------------------
// e2e 3：上游中途错误（提交后 → 下游错误帧收尾）
// ---------------------------------------------------------------------------

func TestE2EUpstreamMidStreamError(t *testing.T) {
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_e1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":0}}}`)
		sseFrame(w, "content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
		sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}`)
		sseFrame(w, "error", `{"type":"error","error":{"type":"overloaded_error","message":"busy"}}`)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d（已提交后错误必须 200 + 流内错误帧）", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)
	if !strings.Contains(s, `"partial"`) {
		t.Fatalf("partial text missing:\n%s", s)
	}
	if !strings.Contains(s, `"error"`) || !strings.Contains(s, `"overloaded_error"`) {
		t.Fatalf("error frame missing:\n%s", s)
	}
	if strings.HasSuffix(s, "data: [DONE]\n\n") {
		t.Fatalf("错误帧后不得补 [DONE]（W5-26 裁决：错误即终态）:\n%s", s)
	}
}

// ---------------------------------------------------------------------------
// e2e 4：提交前失败换上游（第一台上游 500 → 第二台成功）
// ---------------------------------------------------------------------------

func TestE2EPreCommitRetry(t *testing.T) {
	var hitsA, hitsB int
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitsA++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"api_error","message":"boom"}}`))
	}))
	t.Cleanup(bad.Close)
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitsB++
		w.Header().Set("Content-Type", "text/event-stream")
		claudeTextStream(w)
	}))
	t.Cleanup(good.Close)

	cfg := &Config{
		Listen:           "127.0.0.1:0",
		HeartbeatSeconds: 15,
		Upstreams: []Upstream{
			{Name: "bad", BaseURL: bad.URL, APIKey: "k1", Protocol: ProtocolAnthropic},
			{Name: "good", BaseURL: good.URL, APIKey: "k2", Protocol: ProtocolAnthropic},
		},
	}
	srv := startService(t, cfg)
	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(raw), "Hello ") {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}
	if hitsA != 1 || hitsB != 1 {
		t.Fatalf("hits: A=%d B=%d（期望 1/1：换号一次）", hitsA, hitsB)
	}
}

// ---------------------------------------------------------------------------
// e2e 5：非流式（anthropic 上游响应 → openai 响应）
// ---------------------------------------------------------------------------

func TestE2ENonStreaming(t *testing.T) {
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id":"msg_n1","type":"message","role":"assistant","model":"claude-x",
			"content":[{"type":"text","text":"Hi there"}],
			"stop_reason":"end_turn","stop_sequence":null,
			"usage":{"input_tokens":7,"output_tokens":3}
		}`))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var out struct {
		Object  string `json:"object"`
		Choices []struct {
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
			TotalTokens      int64 `json:"total_tokens"`
		} `json:"usage"`
	}
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, body)
	}
	if out.Object != "chat.completion" || len(out.Choices) != 1 ||
		out.Choices[0].Message.Content != "Hi there" || out.Choices[0].FinishReason != "stop" {
		t.Fatalf("out = %+v", out)
	}
	if out.Usage.PromptTokens != 7 || out.Usage.CompletionTokens != 3 || out.Usage.TotalTokens != 10 {
		t.Fatalf("usage = %+v", out.Usage)
	}
}

// ---------------------------------------------------------------------------
// e2e 6：上游 4xx 错误信封转换 + 配额头透传
// ---------------------------------------------------------------------------

func TestE2EUpstreamErrorEnvelope(t *testing.T) {
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(429)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"overloaded_error","message":"busy"}}`))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 429 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "7" {
		t.Fatalf("retry-after = %q（CC C1：整数秒必须透传）", got)
	}
	body, _ := io.ReadAll(resp.Body)
	var env struct {
		Error struct {
			Type string `json:"type"`
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, body)
	}
	if env.Error.Type != "server_error" || env.Error.Code != "overloaded_error" {
		t.Fatalf("error envelope = %+v（期望 openai 映射 server_error+code）", env.Error)
	}
}

// ---------------------------------------------------------------------------
// e2e 7：客户端断开 → 上游取消
// ---------------------------------------------------------------------------

func TestE2EClientDisconnectCancelsUpstream(t *testing.T) {
	upstreamDone := make(chan struct{})
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_d1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}`)
		select {
		case <-r.Context().Done():
			close(upstreamDone)
		case <-time.After(5 * time.Second):
		}
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	// 读到首个下游帧（已提交）后断开。
	buf := make([]byte, 64)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatalf("first read: %v", err)
	}
	cancel()
	resp.Body.Close()
	select {
	case <-upstreamDone:
	case <-time.After(3 * time.Second):
		t.Fatalf("上游未被取消（客户端断开未传导）")
	}
}

// ---------------------------------------------------------------------------
// e2e 8：心跳（claude 入口 → claude 上游直通；上游静默 → 下游 ping）
// ---------------------------------------------------------------------------

func TestE2EHeartbeatPing(t *testing.T) {
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_h1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}`)
		time.Sleep(2 * time.Second) // 上游静默
		sseFrame(w, "message_stop", `{"type":"message_stop"}`)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 1)) // 心跳 1s

	resp := postJSON(t, srv.URL+"/v1/messages",
		`{"model":"claude-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)
	if !strings.Contains(s, "event: ping") {
		t.Fatalf("心跳 ping 缺失:\n%s", s)
	}
	if !strings.Contains(s, "event: message_stop") {
		t.Fatalf("message_stop 缺失:\n%s", s)
	}
	if strings.Contains(s, "[DONE]") {
		t.Fatalf("claude 出口不得有 [DONE]:\n%s", s)
	}
}

// ---------------------------------------------------------------------------
// 配置
// ---------------------------------------------------------------------------

func TestLoadConfigExample(t *testing.T) {
	// 示例配置必须可用（占位符值合法）。
	cfg, err := LoadConfig(filepath.Join("..", "config.example.yaml"))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.Upstreams) != 6 || cfg.HeartbeatSeconds != 15 || cfg.Listen == "" {
		t.Fatalf("cfg = %+v", cfg)
	}
	if cfg.CooldownFailures != 3 || cfg.CooldownSeconds != 30 {
		t.Fatalf("cooldown = %d/%d（want 3/30）", cfg.CooldownFailures, cfg.CooldownSeconds)
	}
	// 五协议各一 + 同协议（anthropic）第二台上游（W5-14：有序 failover 示例；
	// W5-15：interactions 第 5 协议）。
	want := []Protocol{ProtocolAnthropic, ProtocolAnthropic, ProtocolOpenAIChat, ProtocolResponses, ProtocolGemini, ProtocolInteractions}
	for i, p := range want {
		if cfg.Upstreams[i].Protocol != p {
			t.Fatalf("upstream %d protocol = %q（want %q）", i, cfg.Upstreams[i].Protocol, p)
		}
	}
}

func TestLoadConfigErrors(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		return p
	}
	// 未知字段（错字不静默吞）。
	if _, err := LoadConfig(write("unknown.yaml", "listen: x\nupstream: []\n")); err == nil {
		t.Fatalf("unknown field must error")
	}
	// 无上游。
	if _, err := LoadConfig(write("empty.yaml", "listen: x\n")); err == nil {
		t.Fatalf("no upstream must error")
	}
	// 非法协议。
	if _, err := LoadConfig(write("proto.yaml", "upstreams:\n  - base_url: http://x\n    protocol: cohere\n")); err == nil {
		t.Fatalf("bad protocol must error")
	}
	// gemini 协议合法（W5-7：模型名在 URL 路径、x-goog-api-key、alt=sse）。
	if cfg, err := LoadConfig(write("gemini.yaml", "upstreams:\n  - base_url: http://127.0.0.1:1\n    protocol: gemini\n")); err != nil {
		t.Fatalf("gemini protocol must load: %v", err)
	} else if cfg.Upstreams[0].Protocol.Format() != registry.FormatGeminiGenerate {
		t.Fatalf("gemini format = %q", cfg.Upstreams[0].Protocol.Format())
	}
	// 非法 base_url。
	if _, err := LoadConfig(write("url.yaml", "upstreams:\n  - base_url: ftp://x\n    protocol: anthropic\n")); err == nil {
		t.Fatalf("bad base_url must error")
	}
	// 缺省值。
	cfg, err := LoadConfig(write("ok.yaml", "upstreams:\n  - base_url: http://127.0.0.1:1\n    protocol: anthropic\n"))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Listen != DefaultListen || cfg.HeartbeatSeconds != DefaultHeartbeatSeconds {
		t.Fatalf("defaults = %+v", cfg)
	}
	// W5-14：冷却缺省值（cooldown_failures / cooldown_seconds 未配时）。
	if cfg.CooldownFailures != DefaultCooldownFailures || cfg.CooldownSeconds != DefaultCooldownSeconds {
		t.Fatalf("cooldown defaults = %d/%d", cfg.CooldownFailures, cfg.CooldownSeconds)
	}
}

// TestMethodNotAllowed 非 POST 显式 405（错误信封按入口协议）。
func TestMethodNotAllowed(t *testing.T) {
	srv := startService(t, cfgFor("http://127.0.0.1:1", ProtocolAnthropic, 15))
	resp, err := http.Get(srv.URL + "/v1/messages")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"type":"error"`) {
		t.Fatalf("claude 信封缺失: %s", body)
	}
}

// ---------------------------------------------------------------------------
// gemini 上游面（W5-7）：模型名在 URL 路径 + x-goog-api-key + alt=sse
// ---------------------------------------------------------------------------

// geminiUpstream 假 Gemini 上游：记录收到的请求（路径 / 查询 / 认证头 / 体）。
type geminiUpstream struct {
	srv   *httptest.Server
	mu    sync.Mutex
	path  string
	query string
	key   string
	body  []byte
	hits  int
}

func newGeminiUpstream(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) *geminiUpstream {
	t.Helper()
	gu := &geminiUpstream{}
	gu.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gu.mu.Lock()
		gu.path, gu.query, gu.key, gu.body = r.URL.Path, r.URL.RawQuery, r.Header.Get("x-goog-api-key"), b
		gu.hits++
		gu.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(gu.srv.Close)
	return gu
}

func (gu *geminiUpstream) request() (path, query, key string, body []byte, hits int) {
	gu.mu.Lock()
	defer gu.mu.Unlock()
	return gu.path, gu.query, gu.key, gu.body, gu.hits
}

// geminiTextResponse 非流式 GenerateContentResponse（假上游用）。
const geminiTextResponse = `{
	"candidates":[{"content":{"role":"model","parts":[{"text":"Hi there"}]},"finishReason":"STOP","index":0}],
	"usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":3,"totalTokenCount":10},
	"modelVersion":"gemini-2.5-flash","responseId":"resp_1"
}`

// e2e 9：gemini 上游非流式（openai 入口 → :generateContent → openai 响应）。
func TestE2EGeminiNonStreaming(t *testing.T) {
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(geminiTextResponse))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gemini-2.5-flash","messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	// 请求侧：URL 含模型名（非流式 = :generateContent，无查询）、头含 x-goog-api-key。
	path, query, key, upBody, _ := up.request()
	if path != "/v1beta/models/gemini-2.5-flash:generateContent" || query != "" {
		t.Fatalf("upstream url = %q?%q", path, query)
	}
	if key != "test-key" {
		t.Fatalf("x-goog-api-key = %q", key)
	}
	var upReq struct {
		Model    string `json:"model"`
		Contents []struct {
			Role  string `json:"role"`
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"contents"`
	}
	if err := json.Unmarshal(upBody, &upReq); err != nil {
		t.Fatalf("upstream body: %v\n%s", err, upBody)
	}
	if upReq.Model != "" {
		t.Fatalf("gemini 请求体不得带 model 字段（模型名在 URL 里）: %s", upBody)
	}
	if len(upReq.Contents) != 1 || upReq.Contents[0].Role != "user" ||
		len(upReq.Contents[0].Parts) != 1 || upReq.Contents[0].Parts[0].Text != "hi" {
		t.Fatalf("upstream request = %+v", upReq)
	}

	// 响应侧：gemini GenerateContentResponse → openai chat.completion。
	body, _ := io.ReadAll(resp.Body)
	var out struct {
		Object  string `json:"object"`
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
			TotalTokens      int64 `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, body)
	}
	if out.Object != "chat.completion" || len(out.Choices) != 1 ||
		out.Choices[0].Message.Content != "Hi there" || out.Choices[0].FinishReason != "stop" {
		t.Fatalf("out = %+v\n%s", out, body)
	}
	if out.Usage.PromptTokens != 7 || out.Usage.CompletionTokens != 3 || out.Usage.TotalTokens != 10 {
		t.Fatalf("usage = %+v", out.Usage)
	}
}

// e2e 10：gemini 上游流式（:streamGenerateContent?alt=sse 分帧 → 下游 openai 事件）。
func TestE2EGeminiStreaming(t *testing.T) {
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		// gemini 帧形态：data: <GenerateContentResponse> + 空行；无 event 名、无哨兵。
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"Hello "}]}}],"responseId":"r1","modelVersion":"gemini-2.5-flash"}`)
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"world"}]}}]}`)
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[]},"finishReason":"STOP","index":0}],"usageMetadata":{"promptTokenCount":11,"candidatesTokenCount":2,"totalTokenCount":13}}`)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gemini-2.5-flash","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	// 请求侧：流式路径 = :streamGenerateContent + alt=sse（两个官方 SDK 的硬编码形态）。
	path, query, key, upBody, _ := up.request()
	if path != "/v1beta/models/gemini-2.5-flash:streamGenerateContent" || query != "alt=sse" {
		t.Fatalf("upstream url = %q?%q", path, query)
	}
	if key != "test-key" {
		t.Fatalf("x-goog-api-key = %q", key)
	}
	if !strings.Contains(string(upBody), `"contents"`) {
		t.Fatalf("upstream body 不是 gemini 请求: %s", upBody)
	}

	// 下游：openai SSE（gemini 无哨兵 → 出口必须补 [DONE]）。
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.HasSuffix(raw, []byte("data: [DONE]\n\n")) {
		t.Fatalf("missing [DONE]:\n%s", raw)
	}
	events, err := openai_chat.ParseStream(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("downstream parse: %v\n%s", err, raw)
	}
	var text strings.Builder
	var usage *ir.Usage
	var reason ir.StopReason
	for _, ev := range events {
		if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockText {
			text.WriteString(ev.TextDelta)
		}
		if ev.Usage != nil {
			usage = ev.Usage
		}
		if ev.Kind == ir.EventDone {
			reason = ev.Reason
		}
	}
	if text.String() != "Hello world" {
		t.Fatalf("text = %q\n%s", text.String(), raw)
	}
	if reason != ir.StopStop {
		t.Fatalf("done reason = %q", reason)
	}
	if usage == nil || usage.Input != 11 || usage.Output != 2 || usage.Total != 13 {
		t.Fatalf("usage = %+v", usage)
	}
}

// e2e 11：gemini 上游错误信封转换（Google 信封 → 入口协议信封 + retry-after 透传）。
func TestE2EGeminiErrorEnvelope(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		body      string
		wantType  string
		wantCode  string
		wantRetry string
	}{
		{
			name: "resource_exhausted", status: 429,
			body:     `{"error":{"code":429,"message":"quota exceeded","status":"RESOURCE_EXHAUSTED"}}`,
			wantType: "rate_limit_error", wantCode: "429", wantRetry: "7",
		},
		{
			name: "invalid_argument", status: 400,
			body:     `{"error":{"code":400,"message":"bad model","status":"INVALID_ARGUMENT"}}`,
			wantType: "invalid_request_error", wantCode: "400",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if tc.wantRetry != "" {
					w.Header().Set("Retry-After", tc.wantRetry)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

			resp := postJSON(t, srv.URL+"/v1/chat/completions",
				`{"model":"gemini-2.5-flash","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
			defer resp.Body.Close()
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d（want %d）", resp.StatusCode, tc.status)
			}
			if tc.wantRetry != "" && resp.Header.Get("Retry-After") != tc.wantRetry {
				t.Fatalf("retry-after = %q（§7.2：RESOURCE_EXHAUSTED 必须带整数秒）", resp.Header.Get("Retry-After"))
			}
			body, _ := io.ReadAll(resp.Body)
			var env struct {
				Error struct {
					Message string `json:"message"`
					Type    string `json:"type"`
					Code    string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(body, &env); err != nil {
				t.Fatalf("unmarshal: %v\n%s", err, body)
			}
			if env.Error.Type != tc.wantType || env.Error.Code != tc.wantCode {
				t.Fatalf("error envelope = %+v（want type=%s code=%s）\n%s", env.Error, tc.wantType, tc.wantCode, body)
			}
			if !strings.Contains(env.Error.Message, "quota exceeded") && !strings.Contains(env.Error.Message, "bad model") {
				t.Fatalf("上游 message 丢失: %+v", env.Error)
			}
			if strings.Contains(string(body), "RESOURCE_EXHAUSTED") || strings.Contains(string(body), "INVALID_ARGUMENT") {
				t.Fatalf("上游信封结构不得透传（type 必须是入口协议语义）:\n%s", body)
			}
		})
	}
}

// e2e 12：gemini 流中错误行（非 `data:` 前缀的裸 JSON 行）→ 下游错误帧。
func TestE2EGeminiMidStreamErrorLine(t *testing.T) {
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"partial"}]}}]}`)
		// §7.1：流中错误以非 `data:` 前缀的完整 JSON 行到达。
		fmt.Fprint(w, "{\"error\":{\"code\":503,\"message\":\"upstream overloaded\",\"status\":\"UNAVAILABLE\"}}\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gemini-2.5-flash","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d（已提交后错误必须 200 + 流内错误帧）", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)
	if !strings.Contains(s, `"partial"`) {
		t.Fatalf("partial text missing:\n%s", s)
	}
	if !strings.Contains(s, `"upstream overloaded"`) || !strings.Contains(s, `"server_error"`) {
		t.Fatalf("错误行未被分帧/转换（UNAVAILABLE → server_error）:\n%s", s)
	}
	if strings.HasSuffix(s, "data: [DONE]\n\n") {
		t.Fatalf("错误帧后不得补 [DONE]（W5-26 裁决：错误即终态）:\n%s", s)
	}
}

// e2e 13：gemini 上游缺模型名 → 400（客户端错误，不打上游、不重试）。
func TestE2EGeminiMissingModel(t *testing.T) {
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(geminiTextResponse))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d（want 400）", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "model is required") {
		t.Fatalf("错误信息缺失:\n%s", body)
	}
	if _, _, _, _, hits := up.request(); hits != 0 {
		t.Fatalf("缺模型名不得打上游（hits=%d）", hits)
	}
}

// TestGeminiModelPath 模型名 → 路径段（短名补前缀 / 注入拒绝 / Vertex 全路径拒绝）。
func TestGeminiModelPath(t *testing.T) {
	cases := []struct {
		model   string
		want    string
		wantErr bool
	}{
		{"gemini-2.5-flash", "models/gemini-2.5-flash", false},
		{"models/gemini-2.5-pro", "models/gemini-2.5-pro", false},
		{"tunedModels/abc-123", "tunedModels/abc-123", false},
		{"  gemini-3-pro  ", "models/gemini-3-pro", false},
		{"", "", true},
		{"   ", "", true},
		{"gemini?x=1", "", true},
		{"a&b", "", true},
		{"a#b", "", true},
		{"../../v1beta/models/x", "", true},
		{"projects/p/locations/l/publishers/google/models/gemini", "", true},
	}
	for _, tc := range cases {
		got, err := geminiModelPath(tc.model)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("geminiModelPath(%q) = %q，期望报错", tc.model, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("geminiModelPath(%q): %v", tc.model, err)
		}
		if got != tc.want {
			t.Fatalf("geminiModelPath(%q) = %q，期望 %q", tc.model, got, tc.want)
		}
	}
	// 端点路径拼装（表内实测形态：v1beta + 模型路径 + 方法 + alt=sse）。
	if p, err := ProtocolGemini.PathFor("gemini-2.5-flash", true); err != nil ||
		p != "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse" {
		t.Fatalf("stream path = %q, %v", p, err)
	}
	if p, err := ProtocolGemini.PathFor("gemini-2.5-flash", false); err != nil ||
		p != "/v1beta/models/gemini-2.5-flash:generateContent" {
		t.Fatalf("non-stream path = %q, %v", p, err)
	}
	// anthropic/openai_chat 忽略模型名与流开关（既有行为不变）。
	if p, err := ProtocolAnthropic.PathFor("ignored", true); err != nil || p != "/v1/messages" {
		t.Fatalf("anthropic path = %q, %v", p, err)
	}
	if p, err := ProtocolOpenAIChat.PathFor("ignored", true); err != nil || p != "/v1/chat/completions" {
		t.Fatalf("openai path = %q, %v", p, err)
	}
}

// ---------------------------------------------------------------------------
// claude 入口 × gemini 上游（W5-9）：枢纽两步 e2e——请求腿 claude→openai→gemini，
// 响应腿 gemini→openai→claude。直转对不存在（Lookup 必走枢纽两步），现有 gemini
// e2e 都是 openai 入口直转对（W5-8 报告 §缺口 4）。形态对齐 tmp/lanes/L19-hub
// 的活体冒烟（fake_gemini.py + config.yaml），此处 httptest 内联实现。
// ---------------------------------------------------------------------------

// e2e 14：claude 入口 × gemini 上游 非流式（枢纽两步）。
func TestE2EClaudeEntryGeminiUpstreamNonStreaming(t *testing.T) {
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(geminiTextResponse))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

	resp := postJSON(t, srv.URL+"/v1/messages",
		`{"model":"gemini-2.5-flash","max_tokens":64,"system":"S","messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	// 请求腿：模型名在 URL（非流式 :generateContent）、x-goog-api-key；
	// claude 的 system/max_tokens 经枢纽两步落 gemini 形状。
	path, query, key, upBody, _ := up.request()
	if path != "/v1beta/models/gemini-2.5-flash:generateContent" || query != "" {
		t.Fatalf("upstream url = %q?%q", path, query)
	}
	if key != "test-key" {
		t.Fatalf("x-goog-api-key = %q", key)
	}
	var upReq struct {
		Model             string `json:"model"`
		SystemInstruction struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"systemInstruction"`
		GenerationConfig struct {
			MaxOutputTokens int64 `json:"maxOutputTokens"`
		} `json:"generationConfig"`
		Contents []struct {
			Role  string `json:"role"`
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"contents"`
	}
	if err := json.Unmarshal(upBody, &upReq); err != nil {
		t.Fatalf("upstream body: %v\n%s", err, upBody)
	}
	if upReq.Model != "" {
		t.Fatalf("gemini 请求体不得带 model 字段: %s", upBody)
	}
	if len(upReq.SystemInstruction.Parts) != 1 || upReq.SystemInstruction.Parts[0].Text != "S" {
		t.Fatalf("systemInstruction = %+v\n%s", upReq.SystemInstruction, upBody)
	}
	if upReq.GenerationConfig.MaxOutputTokens != 64 {
		t.Fatalf("maxOutputTokens = %d（claude max_tokens 应经枢纽落位）\n%s",
			upReq.GenerationConfig.MaxOutputTokens, upBody)
	}
	if len(upReq.Contents) != 1 || upReq.Contents[0].Role != "user" ||
		len(upReq.Contents[0].Parts) != 1 || upReq.Contents[0].Parts[0].Text != "hi" {
		t.Fatalf("contents = %+v", upReq.Contents)
	}
	for _, k := range []string{"max_tokens", "anthropic_version", "betas"} {
		if strings.Contains(string(upBody), `"`+k+`"`) {
			t.Fatalf("claude 独有键 %q 泄漏进 gemini 请求体: %s", k, upBody)
		}
	}

	// 响应腿：gemini GenerateContentResponse → claude Message（枢纽两步出口）。
	body, _ := io.ReadAll(resp.Body)
	var out struct {
		ID      string `json:"id"`
		Type    string `json:"type"`
		Role    string `json:"role"`
		Model   string `json:"model"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		StopReason string `json:"stop_reason"`
		Usage      struct {
			InputTokens  int64 `json:"input_tokens"`
			OutputTokens int64 `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, body)
	}
	if out.Type != "message" || out.Role != "assistant" || out.ID != "resp_1" || out.Model != "gemini-2.5-flash" {
		t.Fatalf("claude 响应头字段 = %+v\n%s", out, body)
	}
	if len(out.Content) != 1 || out.Content[0].Type != "text" || out.Content[0].Text != "Hi there" {
		t.Fatalf("content = %+v", out.Content)
	}
	if out.StopReason != "end_turn" {
		t.Fatalf("stop_reason = %q（gemini STOP → end_turn）", out.StopReason)
	}
	if out.Usage.InputTokens != 7 || out.Usage.OutputTokens != 3 {
		t.Fatalf("usage = %+v（usageMetadata 应经枢纽过桥）", out.Usage)
	}
}

// e2e 15：claude 入口 × gemini 上游 流式（枢纽两步增量管道；usage 过桥、无 [DONE]）。
func TestE2EClaudeEntryGeminiUpstreamStreaming(t *testing.T) {
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"Hel"}]},"index":0}],"responseId":"r1","modelVersion":"gemini-2.5-flash"}`)
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"lo"}]},"index":0}],"responseId":"r1"}`)
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[]},"finishReason":"STOP","index":0}],"responseId":"r1","usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":2,"totalTokenCount":6}}`)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

	resp := postJSON(t, srv.URL+"/v1/messages",
		`{"model":"gemini-2.5-flash","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	// 请求腿：流式 = :streamGenerateContent + alt=sse（枢纽两步）。
	path, query, key, upBody, _ := up.request()
	if path != "/v1beta/models/gemini-2.5-flash:streamGenerateContent" || query != "alt=sse" {
		t.Fatalf("upstream url = %q?%q", path, query)
	}
	if key != "test-key" {
		t.Fatalf("x-goog-api-key = %q", key)
	}
	if !strings.Contains(string(upBody), `"contents"`) {
		t.Fatalf("upstream body 不是 gemini 请求: %s", upBody)
	}

	// 下游：claude SSE（枢纽两步出口；claude 面绝不发 [DONE] 哨兵）。
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if bytes.Contains(raw, []byte("[DONE]")) {
		t.Fatalf("claude 出口不得发 [DONE]:\n%s", raw)
	}
	events, err := claude_messages.ParseStream(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("downstream parse: %v\n%s", err, raw)
	}
	var text strings.Builder
	var usage *ir.Usage
	var reason ir.StopReason
	sawStart, sawStop := false, false
	for _, ev := range events {
		switch ev.Kind {
		case ir.EventStart:
			sawStart = true
		case ir.EventBlockDelta:
			if ev.BlockKind == ir.BlockText {
				text.WriteString(ev.TextDelta)
			}
		case ir.EventDone:
			reason, sawStop = ev.Reason, true
		}
		if ev.Usage != nil {
			usage = ev.Usage
		}
	}
	if !sawStart || !sawStop {
		t.Fatalf("claude 流缺 message_start/message_stop（start=%v stop=%v）:\n%s", sawStart, sawStop, raw)
	}
	if text.String() != "Hello" {
		t.Fatalf("text = %q\n%s", text.String(), raw)
	}
	if reason != ir.StopStop {
		t.Fatalf("done reason = %q", reason)
	}
	// usage 必须跨枢纽腿过桥（W5-8 修复点）：gemini usageMetadata → claude message_delta。
	if usage == nil || usage.Input != 4 || usage.Output != 2 {
		t.Fatalf("usage = %+v（应来自 usageMetadata 4/2）\n%s", usage, raw)
	}
}

// e2e 16：claude 入口 × gemini 上游 错误信封（Google 信封 → claude 信封 + retry-after 透传）。
func TestE2EClaudeEntryGeminiUpstreamErrorEnvelope(t *testing.T) {
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"code":429,"message":"quota exceeded","status":"RESOURCE_EXHAUSTED"}}`))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

	resp := postJSON(t, srv.URL+"/v1/messages",
		`{"model":"gemini-2.5-flash","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d（上游 429 透传）", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") != "7" {
		t.Fatalf("retry-after = %q", resp.Header.Get("Retry-After"))
	}
	body, _ := io.ReadAll(resp.Body)
	var env struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, body)
	}
	if env.Type != "error" || env.Error.Type != "rate_limit_error" || env.Error.Message != "quota exceeded" {
		t.Fatalf("claude 错误信封 = %+v\n%s", env, body)
	}
	if strings.Contains(string(body), "RESOURCE_EXHAUSTED") {
		t.Fatalf("Google 信封结构不得透传:\n%s", body)
	}
}

// e2e 17：claude 入口 × gemini 上游 流中错误（裸 JSON 错误行）→ 下游 `event: error`。
// 已提交后的错误：不重试（hits=1）、无 message_stop、无第二错误帧。
func TestE2EClaudeEntryGeminiUpstreamMidStreamError(t *testing.T) {
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"partial"}]},"index":0}],"responseId":"r9"}`)
		// §7.1：流中错误以非 `data:` 前缀的完整 JSON 行到达（上游帧读取器按括号平衡聚合）。
		fmt.Fprint(w, "{\"error\":{\"code\":429,\"message\":\"quota exceeded\",\"status\":\"RESOURCE_EXHAUSTED\"}}\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

	resp := postJSON(t, srv.URL+"/v1/messages",
		`{"model":"gemini-2.5-flash","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d（已提交后错误 = 200 + 流内错误帧）", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)
	if !strings.Contains(s, `"partial"`) {
		t.Fatalf("已提交文本丢失:\n%s", s)
	}
	if !strings.Contains(s, "event: error") || !strings.Contains(s, `"rate_limit_error"`) {
		t.Fatalf("claude 错误帧缺失（RESOURCE_EXHAUSTED → rate_limit_error）:\n%s", s)
	}
	if strings.Contains(s, "message_stop") {
		t.Fatalf("错误收尾不得带 message_stop:\n%s", s)
	}
	if n := strings.Count(s, "event: error"); n != 1 {
		t.Fatalf("错误帧应恰一帧（got %d）:\n%s", n, s)
	}
	if _, _, _, _, hits := up.request(); hits != 1 {
		t.Fatalf("提交后错误不得重试（hits=%d）", hits)
	}
}
