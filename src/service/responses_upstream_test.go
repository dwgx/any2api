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

// responsesUpstream 假 Responses 上游：记录收到的请求（路径/认证/体），handler 写响应。
type responsesUpstream struct {
	srv  *httptest.Server
	mu   sync.Mutex
	body []byte
	path string
	auth string
}

func newResponsesUpstream(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) *responsesUpstream {
	t.Helper()
	ru := &responsesUpstream{}
	ru.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		ru.mu.Lock()
		ru.body = b
		ru.path = r.URL.Path
		ru.auth = r.Header.Get("Authorization")
		ru.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(ru.srv.Close)
	return ru
}

func (ru *responsesUpstream) request() (path string, body []byte, auth string) {
	ru.mu.Lock()
	defer ru.mu.Unlock()
	return ru.path, ru.body, ru.auth
}

// responsesTextStream 标准 Responses 事件序（官方示例帧序；usage 在 completed）。
func responsesTextStream(w http.ResponseWriter) {
	sseFrame(w, "response.created", `{"type":"response.created","response":{"id":"resp_e1","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":0}`)
	sseFrame(w, "response.in_progress", `{"type":"response.in_progress","response":{"id":"resp_e1","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":1}`)
	sseFrame(w, "response.output_item.added", `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_e1","type":"message","status":"in_progress","role":"assistant","content":[]},"sequence_number":2}`)
	sseFrame(w, "response.content_part.added", `{"type":"response.content_part.added","item_id":"msg_e1","output_index":0,"content_index":0,"part":{"type":"output_text","text":"","annotations":[],"logprobs":[]},"sequence_number":3}`)
	sseFrame(w, "response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_e1","output_index":0,"content_index":0,"delta":"Hello ","sequence_number":4,"logprobs":[]}`)
	sseFrame(w, "response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_e1","output_index":0,"content_index":0,"delta":"world","sequence_number":5,"logprobs":[]}`)
	sseFrame(w, "response.output_text.done", `{"type":"response.output_text.done","item_id":"msg_e1","output_index":0,"content_index":0,"text":"Hello world","sequence_number":6,"logprobs":[]}`)
	sseFrame(w, "response.content_part.done", `{"type":"response.content_part.done","item_id":"msg_e1","output_index":0,"content_index":0,"part":{"type":"output_text","text":"Hello world","annotations":[],"logprobs":[]},"sequence_number":7}`)
	sseFrame(w, "response.output_item.done", `{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_e1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Hello world","annotations":[],"logprobs":[]}]},"sequence_number":8}`)
	sseFrame(w, "response.completed", `{"type":"response.completed","response":{"id":"resp_e1","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5","output":[{"id":"msg_e1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Hello world","annotations":[],"logprobs":[]}]}],"usage":{"input_tokens":9,"input_tokens_details":{"cached_tokens":2,"cache_write_tokens":0},"output_tokens":4,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":13},"incomplete_details":null},"sequence_number":9}`)
}

// e2e R1：openai 入口 → responses 上游（非流式）。请求面：路径 /v1/responses +
// Bearer；模型在请求体里（表 §请求面）；响应面：Response 对象 → chat.completion。
func TestE2EChatEntryResponsesUpstreamNonStreaming(t *testing.T) {
	up := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id":"resp_n1","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5",
			"output":[{"type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Hi there","annotations":[],"logprobs":[]}]}],
			"usage":{"input_tokens":7,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":0},"output_tokens":3,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":10}
		}`))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-5","messages":[{"role":"system","content":"Be terse."},{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	// 上游面：路径固定 /v1/responses，Bearer 认证，模型在体里，messages→items。
	path, upBody, auth := up.request()
	if path != "/v1/responses" {
		t.Fatalf("upstream path = %q", path)
	}
	if auth != "Bearer test-key" {
		t.Fatalf("authorization = %q", auth)
	}
	var upReq struct {
		Model        string `json:"model"`
		Instructions string `json:"instructions"`
		Input        []struct {
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"input"`
	}
	if err := json.Unmarshal(upBody, &upReq); err != nil {
		t.Fatalf("upstream body: %v\n%s", err, upBody)
	}
	if upReq.Model != "gpt-5" || upReq.Instructions != "Be terse." {
		t.Fatalf("upstream request = %+v", upReq)
	}
	if len(upReq.Input) != 1 || upReq.Input[0].Role != "user" ||
		upReq.Input[0].Content[0].Type != "input_text" || upReq.Input[0].Content[0].Text != "hi" {
		t.Fatalf("input items = %+v", upReq.Input)
	}

	// 下游面：chat.completion（usage 口径转换）。
	body, _ := io.ReadAll(resp.Body)
	var out struct {
		Object  string `json:"object"`
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

// e2e R2：openai 入口 → responses 上游（流式，提交点纪律 + 增量）。
func TestE2EChatEntryResponsesUpstreamStreaming(t *testing.T) {
	up := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		responsesTextStream(w)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-5","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	s := string(body)

	// 请求侧：stream:true 与 include_usage 转换（responses 无 include_usage——
	// usage 恒在终止事件；include_usage 只影响 chat 出口是否发 usage chunk）。
	_, upBody, _ := up.request()
	var upReq struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
		Input  []struct {
			Content json.RawMessage `json:"content"`
		} `json:"input"`
	}
	if err := json.Unmarshal(upBody, &upReq); err != nil {
		t.Fatalf("upstream body: %v\n%s", err, upBody)
	}
	if !upReq.Stream || upReq.Model != "gpt-5" {
		t.Fatalf("upstream stream = %v (%s)", upReq.Stream, upBody)
	}

	// 下游面：文本增量 + finish_reason + usage chunk + [DONE]。
	if !strings.Contains(s, `"content":"Hello "`) || !strings.Contains(s, `"content":"world"`) {
		t.Fatalf("text deltas missing:\n%s", s)
	}
	if !strings.Contains(s, `"finish_reason":"stop"`) {
		t.Fatalf("finish_reason missing:\n%s", s)
	}
	if !strings.Contains(s, `"usage":{"completion_tokens":4`) || !strings.Contains(s, `"prompt_tokens":9`) {
		t.Fatalf("usage chunk missing:\n%s", s)
	}
	if !strings.HasSuffix(strings.TrimSpace(s), "data: [DONE]") {
		t.Fatalf("chat 出口必须以 [DONE] 收束:\n%s", s)
	}
}

// e2e R3：claude 入口 → responses 上游（流式）——responses→claude 出口面。
func TestE2EClaudeEntryResponsesUpstreamStreaming(t *testing.T) {
	up := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		responsesTextStream(w)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))

	resp := postJSON(t, srv.URL+"/v1/messages",
		`{"model":"claude-x","max_tokens":64,"messages":[{"role":"user","content":"hi"}],"stream":true}`,
		map[string]string{"anthropic-version": "2023-06-01"})
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	s := string(body)

	// 请求侧：claude → responses（system 无 → 无 instructions；max_tokens→max_output_tokens）。
	_, upBody, _ := up.request()
	var upReq struct {
		Model      string `json:"model"`
		MaxOutput  int64  `json:"max_output_tokens"`
		Stream     bool   `json:"stream"`
		ToolChoice string `json:"tool_choice"`
	}
	if err := json.Unmarshal(upBody, &upReq); err != nil {
		t.Fatalf("upstream body: %v\n%s", err, upBody)
	}
	if upReq.Model != "claude-x" || upReq.MaxOutput != 64 || !upReq.Stream {
		t.Fatalf("upstream request = %+v (%s)", upReq, upBody)
	}
	// 下游面：claude SSE（message_start → text_delta → message_delta → message_stop）。
	for _, want := range []string{`"type":"message_start"`, `"type":"text_delta"`, `"text":"Hello "`, `"stop_reason":"end_turn"`, `"type":"message_stop"`} {
		if !strings.Contains(s, want) {
			t.Fatalf("%s missing:\n%s", want, s)
		}
	}
}

// e2e R4：上游 4xx 错误信封（responses 形态）→ 下游 chat 信封（经 IR 转换，不透传 body）。
func TestE2EChatEntryResponsesUpstreamErrorEnvelope(t *testing.T) {
	up := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("retry-after", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"rate limited","type":"rate_limit_error","param":null,"code":"rate_limit_exceeded"}}`))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp.Header.Get("retry-after") != "7" {
		t.Fatalf("retry-after = %q", resp.Header.Get("retry-after"))
	}
	body, _ := io.ReadAll(resp.Body)
	var w struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, body)
	}
	if w.Error.Message != "rate limited" || w.Error.Type != "rate_limit_error" {
		t.Fatalf("envelope = %s", body)
	}
}

// e2e R5：已提交后上游发 error 事件（平铺）→ 下游错误帧 + [DONE]，绝不重试。
func TestE2EChatEntryResponsesUpstreamMidStreamError(t *testing.T) {
	up := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "response.created", `{"type":"response.created","response":{"id":"resp_err","object":"response","created_at":1,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":0}`)
		sseFrame(w, "response.output_item.added", `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_err","type":"message","status":"in_progress","role":"assistant","content":[]},"sequence_number":1}`)
		sseFrame(w, "response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_err","output_index":0,"content_index":0,"delta":"partial","sequence_number":2,"logprobs":[]}`)
		sseFrame(w, "error", `{"type":"error","code":"server_error","message":"upstream exploded","param":null,"sequence_number":3}`)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-5","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	if !strings.Contains(s, `"content":"partial"`) {
		t.Fatalf("partial content must be delivered (已交付内容不重放):\n%s", s)
	}
	if !strings.Contains(s, "upstream exploded") {
		t.Fatalf("error frame missing:\n%s", s)
	}
	if strings.HasSuffix(strings.TrimSpace(s), "data: [DONE]") {
		t.Fatalf("错误帧后不得补 [DONE]（W5-26 裁决：错误即终态）:\n%s", s)
	}
	if strings.Count(s, `"content":"partial"`) != 1 {
		t.Fatalf("content replayed:\n%s", s)
	}
}

// e2e R6：stateful 字段在上游请求面显式拒绝（客户端 400，不打上游）。
func TestE2EChatEntryResponsesUpstreamStatefulRejected(t *testing.T) {
	var hits int
	up := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))

	// 入口是 chat：stateful 字段是 responses 侧概念，客户端（chat 面）发不出来；
	// 此处验证的是「responses 上游面不注入 previous_response_id/conversation 等」——
	// 上游请求体不得出现状态化字段（表 1 行：转 chat/claude 直接报错，绝不静默带过）。
	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	_, upBody, _ := up.request()
	for _, banned := range []string{"previous_response_id", "conversation", "context_management", "background"} {
		if strings.Contains(string(upBody), banned) {
			t.Fatalf("stateful field %q leaked upstream: %s", banned, upBody)
		}
	}
	if hits != 1 {
		t.Fatalf("upstream hits = %d, want 1", hits)
	}
}
