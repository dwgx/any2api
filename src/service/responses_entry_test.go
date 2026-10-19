package service

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/dwgx/any2api/translate/convert/responses"
	"github.com/dwgx/any2api/translate/ir"
)

// ---------------------------------------------------------------------------
// Responses 入口面（/v1/responses，W5-12）e2e：入口 × 三种上游 + 错误信封。
//
// 表依据：research/topics/openai-responses/README.md（§请求面 / §流式事件族 /
// §错误信封 / §4.5）。入口路径固定 `/v1/responses`（模型与 stream 都在请求体里）。
// ---------------------------------------------------------------------------

// chatUpstream 假 OpenAI Chat 上游：记录收到的请求（路径/认证/体）。
type chatUpstream struct {
	srv  *httptest.Server
	mu   sync.Mutex
	path string
	auth string
	body []byte
}

func newChatUpstream(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) *chatUpstream {
	t.Helper()
	cu := &chatUpstream{}
	cu.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		cu.mu.Lock()
		cu.path, cu.auth, cu.body = r.URL.Path, r.Header.Get("Authorization"), b
		cu.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(cu.srv.Close)
	return cu
}

func (cu *chatUpstream) request() (path, auth string, body []byte) {
	cu.mu.Lock()
	defer cu.mu.Unlock()
	return cu.path, cu.auth, cu.body
}

// chatTextStream 假 chat 上游的文本流（role 公告 → 两个增量 → finish chunk + usage
// → [DONE]）。
func chatTextStream(w http.ResponseWriter) {
	sseFrame(w, "", `{"id":"chatcmpl-e1","object":"chat.completion.chunk","created":1741293000,"model":"gpt-5","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}`)
	sseFrame(w, "", `{"id":"chatcmpl-e1","object":"chat.completion.chunk","created":1741293000,"model":"gpt-5","choices":[{"index":0,"delta":{"content":"Hello "},"finish_reason":null}]}`)
	sseFrame(w, "", `{"id":"chatcmpl-e1","object":"chat.completion.chunk","created":1741293000,"model":"gpt-5","choices":[{"index":0,"delta":{"content":"world"},"finish_reason":null}]}`)
	sseFrame(w, "", `{"id":"chatcmpl-e1","object":"chat.completion.chunk","created":1741293000,"model":"gpt-5","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":2,"total_tokens":13}}`)
	sseFrame(w, "", `[DONE]`)
}

// e2e RE1：responses 入口 × chat 上游（流式）——items 请求 → chat 消息；chat chunk
// → Responses 事件族；下游无 [DONE] 哨兵（topics/openai-wire §4.8）。
func TestE2EResponsesEntryChatUpstreamStreaming(t *testing.T) {
	up := newChatUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		chatTextStream(w)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))

	resp := postJSON(t, srv.URL+"/v1/responses",
		`{"model":"gpt-5","instructions":"Be terse.","input":[{"role":"user","content":[{"type":"input_text","text":"hi"}]}],"stream":true}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}

	// 请求侧：路径 /v1/chat/completions + Bearer；instructions → system 消息；
	// input items → messages；stream 透传。
	path, auth, upBody := up.request()
	if path != "/v1/chat/completions" {
		t.Fatalf("upstream path = %q", path)
	}
	if auth != "Bearer test-key" {
		t.Fatalf("authorization = %q", auth)
	}
	var upReq struct {
		Model    string `json:"model"`
		Stream   bool   `json:"stream"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(upBody, &upReq); err != nil {
		t.Fatalf("upstream body: %v\n%s", err, upBody)
	}
	if upReq.Model != "gpt-5" || !upReq.Stream {
		t.Fatalf("upstream request = %+v\n%s", upReq, upBody)
	}
	if len(upReq.Messages) != 2 || upReq.Messages[0].Role != "system" || upReq.Messages[1].Role != "user" {
		t.Fatalf("messages = %+v\n%s", upReq.Messages, upBody)
	}
	if !strings.Contains(string(upReq.Messages[0].Content), "Be terse.") ||
		!strings.Contains(string(upReq.Messages[1].Content), "hi") {
		t.Fatalf("message content = %s / %s", upReq.Messages[0].Content, upReq.Messages[1].Content)
	}

	// 下游面：Responses 事件族（官方帧序），无 [DONE]。
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	s := string(raw)
	for _, want := range []string{
		`"type":"response.created"`, `"type":"response.in_progress"`,
		`"type":"response.output_item.added"`, `"type":"response.content_part.added"`,
		`"type":"response.output_text.delta"`, `"type":"response.output_text.done"`,
		`"type":"response.content_part.done"`, `"type":"response.output_item.done"`,
		`"type":"response.completed"`,
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("%s missing:\n%s", want, s)
		}
	}
	if strings.Contains(s, "[DONE]") {
		t.Fatalf("responses 面不得有 [DONE] 哨兵（topics/openai-wire §4.8）:\n%s", s)
	}

	// 结构校验：下游字节必须能被 responses 解析器吃回去（事件族 + usage 在
	// completed + stop_reason 收尾）。
	events, err := responses.ParseStream(bytes.NewReader(raw))
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

// e2e RE2：responses 入口 × chat 上游（非流式）——chat.completion → Response 对象。
func TestE2EResponsesEntryChatUpstreamNonStreaming(t *testing.T) {
	up := newChatUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id":"chatcmpl-n1","object":"chat.completion","created":1741293000,"model":"gpt-5",
			"choices":[{"index":0,"message":{"role":"assistant","content":"Hi there"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}
		}`))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))

	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi"}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("content-type = %q", ct)
	}

	// 请求侧：纯文本 input（string 形态）→ 一条 user 消息。
	_, _, upBody := up.request()
	if !strings.Contains(string(upBody), `"role":"user"`) || !strings.Contains(string(upBody), `"hi"`) {
		t.Fatalf("upstream body = %s", upBody)
	}

	// 下游面：Response 对象（status completed + message item + usage 口径转换）。
	body, _ := io.ReadAll(resp.Body)
	var out struct {
		ID     string `json:"id"`
		Object string `json:"object"`
		Status string `json:"status"`
		Model  string `json:"model"`
		Output []struct {
			Type    string `json:"type"`
			Status  string `json:"status"`
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
		Usage struct {
			InputTokens  int64 `json:"input_tokens"`
			OutputTokens int64 `json:"output_tokens"`
			TotalTokens  int64 `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, body)
	}
	if out.Object != "response" || out.Status != "completed" || out.ID == "" {
		t.Fatalf("out = %+v\n%s", out, body)
	}
	if len(out.Output) != 1 || out.Output[0].Type != "message" ||
		len(out.Output[0].Content) != 1 || out.Output[0].Content[0].Type != "output_text" ||
		out.Output[0].Content[0].Text != "Hi there" {
		t.Fatalf("output = %+v\n%s", out.Output, body)
	}
	if out.Usage.InputTokens != 7 || out.Usage.OutputTokens != 3 || out.Usage.TotalTokens != 10 {
		t.Fatalf("usage = %+v", out.Usage)
	}
}

// responsesPassthroughStream 假 responses 上游的原始字节流（同协议透传断言用）：
// 官方帧序 + 兼容层才有的 obfuscation 字段（表 §quirk 11：透传必须原样保留）。
func responsesPassthroughStream() string {
	var b strings.Builder
	frame := func(ev, data string) {
		b.WriteString("event: " + ev + "\ndata: " + data + "\n\n")
	}
	frame("response.created", `{"type":"response.created","response":{"id":"resp_pt","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":0}`)
	frame("response.in_progress", `{"type":"response.in_progress","response":{"id":"resp_pt","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":1}`)
	frame("response.output_item.added", `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_pt","type":"message","status":"in_progress","role":"assistant","content":[]},"sequence_number":2}`)
	frame("response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_pt","output_index":0,"content_index":0,"delta":"Hi","obfuscation":"zz9","sequence_number":3}`)
	frame("response.output_text.done", `{"type":"response.output_text.done","item_id":"msg_pt","output_index":0,"content_index":0,"text":"Hi","sequence_number":4}`)
	frame("response.output_item.done", `{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_pt","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Hi","annotations":[],"logprobs":[]}]},"sequence_number":5}`)
	frame("response.completed", `{"type":"response.completed","response":{"id":"resp_pt","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5","output":[{"id":"msg_pt","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Hi","annotations":[],"logprobs":[]}]}],"usage":{"input_tokens":3,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":0},"output_tokens":1,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":4},"incomplete_details":null},"sequence_number":6}`)
	return b.String()
}

// e2e RE3：responses 入口 × responses 上游（流式透传）——请求与响应都原样字节直通
// （同格式直通优先），上游兼容层补的 [DONE] 哨兵被容忍且**不转发**（topics/openai-wire §4.8）。
func TestE2EResponsesEntryResponsesUpstreamStreamingPassthrough(t *testing.T) {
	want := responsesPassthroughStream()
	up := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, want)
		// 兼容层（SGLang/vLLM 一类）可能补 chat 面哨兵：容忍 = 视为流结束。
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))

	reqBody := `{"model":"gpt-5","input":"hi","stream":true,"store":false}`
	resp := postJSON(t, srv.URL+"/v1/responses", reqBody, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	if string(raw) != want {
		t.Fatalf("透传字节必须原样（含 obfuscation；哨兵不转发）:\nwant:\n%s\ngot:\n%s", want, raw)
	}

	// 请求侧：同协议直通——上游收到的体与客户端一致（含 store:false）。
	path, upBody, auth := up.request()
	if path != "/v1/responses" || auth != "Bearer test-key" {
		t.Fatalf("path = %q auth = %q", path, auth)
	}
	if string(upBody) != reqBody {
		t.Fatalf("上游请求体必须原样透传:\nwant %s\ngot  %s", reqBody, upBody)
	}
}

// e2e RE4：responses 入口 × responses 上游（非流式透传）——Response 对象原样字节直通
// （含 responses 独有字段；不做任何改写）。
func TestE2EResponsesEntryResponsesUpstreamNonStreamingPassthrough(t *testing.T) {
	const upstreamBody = `{
		"id":"resp_np","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5",
		"instructions":"Be terse.","temperature":0.7,"store":false,"metadata":{"k":"v"},
		"output":[{"id":"msg_np","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Hi","annotations":[],"logprobs":[]}]}],
		"usage":{"input_tokens":3,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":0},"output_tokens":1,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":4}
	}`
	up := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(upstreamBody))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))

	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi"}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != upstreamBody {
		t.Fatalf("非流式响应必须原样透传:\nwant %s\ngot  %s", upstreamBody, body)
	}
}

// e2e RE5：responses 入口 × gemini 上游（流式，枢纽两步 responses→chat→gemini）——
// 模型名进 URL 路径 + alt=sse；gemini 帧 →（枢纽 chat）→ Responses 事件族。
func TestE2EResponsesEntryGeminiUpstreamStreaming(t *testing.T) {
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"Hello "}]}}],"responseId":"r1","modelVersion":"gemini-2.5-flash"}`)
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"world"}]}}]}`)
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[]},"finishReason":"STOP","index":0}],"usageMetadata":{"promptTokenCount":11,"candidatesTokenCount":2,"totalTokenCount":13}}`)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

	resp := postJSON(t, srv.URL+"/v1/responses",
		`{"model":"gemini-2.5-flash","input":"hi","stream":true}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	// 请求侧：gemini 端点（模型名在路径里）+ x-goog-api-key + gemini 请求体。
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

	// 下游面：Responses 事件族（枢纽两步后仍必须是合法 responses 流）。
	raw, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(raw), "[DONE]") {
		t.Fatalf("responses 面不得有 [DONE] 哨兵:\n%s", raw)
	}
	events, err := responses.ParseStream(bytes.NewReader(raw))
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

// e2e RE6：上游 429 → responses 错误信封（非流式 HTTP 错误与 chat 同构：顶层 error
// 对象；retry-after 透传）+ 不重放。
func TestE2EResponsesEntryUpstreamErrorEnvelope(t *testing.T) {
	up := newChatUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"rate limited","type":"rate_limit_error","param":null,"code":"rate_limit_exceeded"}}`))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))

	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi"}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") != "7" {
		t.Fatalf("retry-after = %q", resp.Header.Get("Retry-After"))
	}
	if resp.Header.Get("request-id") == "" {
		t.Fatalf("request-id 缺失（B10：诊断关联）")
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
	if env.Error.Message != "rate limited" || env.Error.Type != "rate_limit_error" ||
		env.Error.Code != "rate_limit_exceeded" {
		t.Fatalf("envelope = %s", body)
	}
}

// e2e RE7：已提交后管道失败（畸形上游帧）→ responses 面中途错误帧：平铺 `error`
// 事件 + `response.failed`（**无** [DONE]）；sequence_number 承接已下发事件；
// response.failed 的 Response 对象承接已下发 id/status。
func TestE2EResponsesEntryMidStreamErrorFrames(t *testing.T) {
	up := newChatUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", `{"id":"chatcmpl-e2","object":"chat.completion.chunk","created":1741293000,"model":"gpt-5","choices":[{"index":0,"delta":{"role":"assistant","content":"partial"},"finish_reason":null}]}`)
		// 畸形帧（非法 JSON）：管道 Feed 报错 → 已提交 → writeDownstreamError。
		sseFrame(w, "", `{"id":"chatcmpl-e2","choices":[oops`)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))

	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","stream":true}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d（已提交后错误必须 200 + 流内错误帧）", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)

	// 已交付的部分内容保留（流不幂等，绝不重放）。
	if !strings.Contains(s, `"delta":"partial"`) {
		t.Fatalf("partial content missing:\n%s", s)
	}
	if strings.Contains(s, "[DONE]") {
		t.Fatalf("responses 错误收尾不得带 [DONE] 哨兵:\n%s", s)
	}

	frames := parseSSEFrames(t, raw)
	var (
		createdID string
		flat      map[string]any
		failed    map[string]any
		seqs      []int64
	)
	for _, f := range frames {
		if n, ok := f.Data["sequence_number"].(float64); ok {
			seqs = append(seqs, int64(n))
		}
		switch f.Data["type"] {
		case "response.created":
			if r, ok := f.Data["response"].(map[string]any); ok {
				createdID, _ = r["id"].(string)
			}
		case "error":
			flat = f.Data
		case "response.failed":
			failed = f.Data
		}
	}
	if createdID == "" {
		t.Fatalf("response.created 缺 id:\n%s", s)
	}
	if flat == nil {
		t.Fatalf("平铺 error 事件缺失:\n%s", s)
	}
	if flat["code"] != "server_error" || flat["message"] == "" {
		t.Fatalf("error 事件字段 = %+v", flat)
	}
	if _, nested := flat["error"]; nested {
		t.Fatalf("error 事件必须是平铺字段（无嵌套 error 对象）: %+v", flat)
	}
	if failed == nil {
		t.Fatalf("response.failed 缺失:\n%s", s)
	}
	ro, _ := failed["response"].(map[string]any)
	if ro == nil || ro["status"] != "failed" {
		t.Fatalf("response.failed.response = %+v", failed)
	}
	if ro["id"] != createdID {
		t.Fatalf("response.failed 的 id 未承接（want %q got %v）", createdID, ro["id"])
	}
	if errObj, _ := ro["error"].(map[string]any); errObj["code"] != "server_error" || errObj["message"] == "" {
		t.Fatalf("response.failed.response.error = %+v", ro["error"])
	}

	// sequence_number 全程严格递增（中途错误帧承接已下发计数——表 §4.1）。
	if len(seqs) < 3 {
		t.Fatalf("sequence_number 缺失:\n%s", s)
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			t.Fatalf("sequence_number 不连续（%v）:\n%s", seqs, s)
		}
	}
}

// e2e RE10：上游截断（chat 上游 EOF 早于 finish_reason）→ 下游以终止事件收束
// （库层 EventError 路径：平铺 error + response.failed，**无** [DONE]）；已交付的
// 增量不重放。与 RE7（服务壳 writeDownstreamError 路径）合起来锁死两条错误出口
// 都是 responses 信封。
func TestE2EResponsesEntryTruncatedUpstream(t *testing.T) {
	up := newChatUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", `{"id":"chatcmpl-t1","object":"chat.completion.chunk","created":1741293000,"model":"gpt-5","choices":[{"index":0,"delta":{"role":"assistant","content":"partial"},"finish_reason":null}]}`)
		// 直接 EOF：无 finish_reason、无 [DONE]。
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))

	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","stream":true}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	s := string(raw)
	if !strings.Contains(s, `"delta":"partial"`) {
		t.Fatalf("partial content missing:\n%s", s)
	}
	if strings.Contains(s, "[DONE]") {
		t.Fatalf("responses 面不得有 [DONE] 哨兵:\n%s", s)
	}
	frames := parseSSEFrames(t, raw)
	n := len(frames)
	if n < 2 || frames[n-2].Data["type"] != "error" || frames[n-1].Data["type"] != "response.failed" {
		t.Fatalf("末两帧必须是 error + response.failed:\n%s", s)
	}
	if frames[n-1].Data["response"].(map[string]any)["status"] != "failed" {
		t.Fatalf("response.failed.response.status != failed:\n%s", s)
	}
	if strings.Count(s, `"delta":"partial"`) != 1 {
		t.Fatalf("已交付内容不得重放:\n%s", s)
	}
}

// e2e RE9：responses 入口 × claude 上游（流式）——表 3 直转对：items → Messages
// （max_output_tokens → max_tokens 必填）；claude SSE → Responses 事件族。
func TestE2EResponsesEntryClaudeUpstreamStreaming(t *testing.T) {
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		claudeTextStream(w)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	resp := postJSON(t, srv.URL+"/v1/responses",
		`{"model":"claude-x","input":"hi","max_output_tokens":64,"stream":true}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	// 请求侧：/v1/messages + x-api-key + anthropic-version；max_tokens 必填已带上。
	path, upBody, auth := up.request()
	if path != "/v1/messages" {
		t.Fatalf("upstream path = %q", path)
	}
	if auth != "test-key" {
		t.Fatalf("x-api-key = %q", auth)
	}
	var upReq struct {
		Model     string `json:"model"`
		MaxTokens int64  `json:"max_tokens"`
		Stream    bool   `json:"stream"`
	}
	if err := json.Unmarshal(upBody, &upReq); err != nil {
		t.Fatalf("upstream body: %v\n%s", err, upBody)
	}
	if upReq.Model != "claude-x" || upReq.MaxTokens != 64 || !upReq.Stream {
		t.Fatalf("upstream request = %+v\n%s", upReq, upBody)
	}

	// 下游面：Responses 事件族。
	raw, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(raw), "[DONE]") {
		t.Fatalf("responses 面不得有 [DONE] 哨兵:\n%s", raw)
	}
	events, err := responses.ParseStream(bytes.NewReader(raw))
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
	if usage == nil || usage.Input != 11 || usage.Output != 2 {
		t.Fatalf("usage = %+v", usage)
	}
}

// e2e RE8：服务自产错误（非 JSON 体 / 方法不允许）→ responses 信封（表 §错误信封：
// 非流式 HTTP 错误与 chat 同构；入口面错误出口不依赖上游）。
func TestE2EResponsesEntrySelfGeneratedErrors(t *testing.T) {
	srv := startService(t, cfgFor("http://127.0.0.1:1", ProtocolOpenAIChat, 15))

	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var env struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, body)
	}
	if env.Error.Type != "invalid_request_error" || env.Error.Message == "" {
		t.Fatalf("envelope = %s", body)
	}

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/responses", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	gresp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer gresp.Body.Close()
	if gresp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", gresp.StatusCode)
	}
}

// parseSSEFrames 把下游 SSE 字节切成帧（注释/心跳块跳过）；data 必须是 JSON 对象。
func parseSSEFrames(t *testing.T, raw []byte) []sseFrameData {
	t.Helper()
	var frames []sseFrameData
	for _, block := range strings.Split(string(raw), "\n\n") {
		if strings.TrimSpace(block) == "" {
			continue
		}
		var (
			f    sseFrameData
			data []string
		)
		for _, line := range strings.Split(block, "\n") {
			if v, ok := strings.CutPrefix(line, "event:"); ok {
				f.Event = strings.TrimSpace(v)
				continue
			}
			if v, ok := strings.CutPrefix(line, "data:"); ok {
				data = append(data, strings.TrimPrefix(v, " "))
			}
		}
		if len(data) == 0 {
			continue // 注释/心跳块
		}
		payload := strings.Join(data, "\n")
		if err := json.Unmarshal([]byte(payload), &f.Data); err != nil {
			t.Fatalf("帧 %q 的 data 不是 JSON: %v\n%s", f.Event, err, payload)
		}
		frames = append(frames, f)
	}
	return frames
}

// sseFrameData 一帧 SSE（event 名可空 + data 对象）。
type sseFrameData struct {
	Event string
	Data  map[string]any
}
