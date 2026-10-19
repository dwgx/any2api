// W5-15 e2e：Interactions 上游面（`protocol: interactions`）——端点 / 鉴权 /
// Api-Revision 头纪律 / store 语义。
//
// 装置：httptest 假 interactions 上游（本地回环；记录路径/鉴权/版本头/体）+
// 真服务（New(cfg).Handler() 经 httptest.NewServer）+ 真 HTTP 客户端（postJSON）。
// 帧形与响应体照 interactions-wire 官方帧序（W5-13 golden 同款）。
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

// interactionsUpstream 假 Interactions 上游：记录收到的请求（路径/鉴权/版本头/体）。
type interactionsUpstream struct {
	srv  *httptest.Server
	mu   sync.Mutex
	path string
	key  string
	rev  string
	body []byte
	hits int
}

func newInteractionsUpstream(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) *interactionsUpstream {
	t.Helper()
	iu := &interactionsUpstream{}
	iu.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		iu.mu.Lock()
		iu.path = r.URL.Path
		iu.key = r.Header.Get("x-goog-api-key")
		iu.rev = r.Header.Get("Api-Revision")
		iu.body = b
		iu.hits++
		iu.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(iu.srv.Close)
	return iu
}

func (iu *interactionsUpstream) request() (path, key, rev string, body []byte, hits int) {
	iu.mu.Lock()
	defer iu.mu.Unlock()
	return iu.path, iu.key, iu.rev, iu.body, iu.hits
}

// interactionsTextResponse 非流式 Interaction 资源（steps 时间线 + usage；
// 官方 response 形状，见 testdata/interactions/response_completed.json）。
const interactionsTextResponse = `{
  "id": "int_e2e",
  "object": "interaction",
  "status": "completed",
  "model": "gemini-3-flash-preview",
  "steps": [{"type": "model_output", "content": [{"type": "text", "text": "Hi there"}]}],
  "usage": {"total_input_tokens": 7, "total_output_tokens": 3, "total_tokens": 10, "total_cached_tokens": 0}
}`

// e2e I1：openai 入口 → interactions 上游（非流式）。请求面：路径固定
// /v1beta/interactions（模型在体里）、x-goog-api-key、Api-Revision 缺省补新 schema；
// 响应面：Interaction steps → chat.completion。
func TestE2EInteractionsUpstreamNonStreaming(t *testing.T) {
	up := newInteractionsUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(interactionsTextResponse))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolInteractions, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gemini-3-flash-preview","messages":[{"role":"system","content":"Be terse."},{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}

	// 上游面：路径 / 鉴权头 / 版本头 / 请求体形状。
	path, key, rev, upBody, hits := up.request()
	if hits != 1 {
		t.Fatalf("upstream hits = %d, want 1", hits)
	}
	if path != "/v1beta/interactions" {
		t.Fatalf("upstream path = %q（want /v1beta/interactions）", path)
	}
	if key != "test-key" {
		t.Fatalf("x-goog-api-key = %q", key)
	}
	if rev != "2026-05-20" {
		t.Fatalf("Api-Revision = %q（缺省必须补新 schema 2026-05-20）", rev)
	}
	var upReq struct {
		Model             string `json:"model"`
		SystemInstruction string `json:"system_instruction"`
		Input             []struct {
			Type    string `json:"type"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"input"`
	}
	if err := json.Unmarshal(upBody, &upReq); err != nil {
		t.Fatalf("upstream body: %v\n%s", err, upBody)
	}
	if upReq.Model != "gemini-3-flash-preview" || upReq.SystemInstruction != "Be terse." {
		t.Fatalf("upstream request = %+v", upReq)
	}
	if len(upReq.Input) != 1 || upReq.Input[0].Type != "user_input" ||
		len(upReq.Input[0].Content) != 1 || upReq.Input[0].Content[0].Text != "hi" {
		t.Fatalf("input steps = %+v", upReq.Input)
	}

	// 响应面：steps → chat.completion（内容 / finish_reason / usage 7-3-10）。
	var out struct {
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
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("downstream body: %v\n%s", err, raw)
	}
	if len(out.Choices) != 1 || out.Choices[0].Message.Content != "Hi there" || out.Choices[0].FinishReason != "stop" {
		t.Fatalf("choices = %+v", out.Choices)
	}
	if out.Usage.PromptTokens != 7 || out.Usage.CompletionTokens != 3 || out.Usage.TotalTokens != 10 {
		t.Fatalf("usage = %+v（want 7/3/10）", out.Usage)
	}
}

// e2e I2：openai 入口 → interactions 上游（流式）。上游三拍事件流
// （step.start → step.delta* → step.stop + interaction.completed + done/[DONE]）
// → 下游 chat SSE；请求体带 stream:true。
func TestE2EInteractionsUpstreamStreaming(t *testing.T) {
	up := newInteractionsUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "interaction.created", `{"interaction":{"id":"v1_e2e","status":"in_progress","object":"interaction","model":"gemini-3-flash-preview"},"event_type":"interaction.created"}`)
		sseFrame(w, "interaction.status_update", `{"interaction_id":"v1_e2e","status":"in_progress","event_type":"interaction.status_update"}`)
		sseFrame(w, "step.start", `{"index":0,"step":{"type":"model_output"},"event_type":"step.start"}`)
		sseFrame(w, "step.delta", `{"index":0,"delta":{"text":"Hello ","type":"text"},"event_type":"step.delta"}`)
		sseFrame(w, "step.delta", `{"index":0,"delta":{"text":"world","type":"text"},"event_type":"step.delta"}`)
		sseFrame(w, "step.stop", `{"index":0,"event_type":"step.stop"}`)
		sseFrame(w, "interaction.completed", `{"interaction":{"id":"v1_e2e","status":"completed","usage":{"total_tokens":13,"total_input_tokens":9,"total_output_tokens":4},"object":"interaction","model":"gemini-3-flash-preview"},"event_type":"interaction.completed"}`)
		sseFrame(w, "done", `[DONE]`)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolInteractions, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gemini-3-flash-preview","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}
	s := string(raw)
	if !strings.Contains(s, `"Hello "`) || !strings.Contains(s, `"world"`) {
		t.Fatalf("text deltas missing:\n%s", s)
	}
	if !strings.Contains(s, `"finish_reason":"stop"`) {
		t.Fatalf("finish_reason missing:\n%s", s)
	}
	if !strings.Contains(s, `"prompt_tokens":9`) || !strings.Contains(s, `"completion_tokens":4`) {
		t.Fatalf("usage chunk missing（interaction.completed usage 必须过桥）:\n%s", s)
	}
	if !strings.HasSuffix(s, "data: [DONE]\n\n") {
		t.Fatalf("chat 面 [DONE] missing:\n%s", s)
	}

	// 上游请求体带 stream:true（chat→interactions 渲染面）。
	_, _, _, upBody, _ := up.request()
	if !strings.Contains(string(upBody), `"stream":true`) {
		t.Fatalf("upstream body 缺 stream:true:\n%s", upBody)
	}
}

// e2e I3：interactions 上游错误信封（Google 信封 → 入口协议信封 + retry-after 透传；
// 上游原文不外泄）。
func TestE2EInteractionsUpstreamErrorEnvelope(t *testing.T) {
	up := newInteractionsUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(429)
		_, _ = w.Write([]byte(`{"error":{"code":429,"message":"quota exceeded","status":"RESOURCE_EXHAUSTED"}}`))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolInteractions, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gemini-3-flash-preview","messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 429 {
		t.Fatalf("status = %d（上游错误码必须透传）", resp.StatusCode)
	}
	if got := resp.Header.Get("retry-after"); got != "7" {
		t.Fatalf("retry-after = %q（want 7）", got)
	}
	s := string(raw)
	if !strings.Contains(s, "rate_limit_error") {
		t.Fatalf("downstream envelope missing rate_limit_error:\n%s", s)
	}
	if strings.Contains(s, "RESOURCE_EXHAUSTED") {
		t.Fatalf("上游原文外泄:\n%s", s)
	}
}

// e2e I4：Api-Revision 头纪律——客户端显式带头 → 转发其值（版本选择权在客户端；
// 缺省补新 schema 的对照见 I1）。
func TestE2EInteractionsUpstreamApiRevisionForwarded(t *testing.T) {
	up := newInteractionsUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(interactionsTextResponse))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolInteractions, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gemini-3-flash-preview","messages":[{"role":"user","content":"hi"}]}`,
		map[string]string{"Api-Revision": "2026-05-07"})
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	_, _, rev, _, _ := up.request()
	if rev != "2026-05-07" {
		t.Fatalf("Api-Revision = %q（客户端显式值必须转发）", rev)
	}
}

// e2e I5：store 语义——客户端显式 store:false 经转换面透传到上游体（渲染器
// interactionsPassableExtras）；未给时不发明 store（上游默认语义由上游决定，
// store=false 的边界为 UNVERIFIED——interactions-wire 未决 4，服务壳不做默认注入）。
func TestE2EInteractionsUpstreamStoreSemantics(t *testing.T) {
	newUpstream := func(t *testing.T) *interactionsUpstream {
		t.Helper()
		return newInteractionsUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(interactionsTextResponse))
		})
	}

	t.Run("client_store_false_passthrough", func(t *testing.T) {
		up := newUpstream(t)
		srv := startService(t, cfgFor(up.srv.URL, ProtocolInteractions, 15))
		resp := postJSON(t, srv.URL+"/v1/chat/completions",
			`{"model":"gemini-3-flash-preview","messages":[{"role":"user","content":"hi"}],"store":false}`, nil)
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		_, _, _, upBody, _ := up.request()
		if !strings.Contains(string(upBody), `"store":false`) {
			t.Fatalf("store:false 未透传到上游体:\n%s", upBody)
		}
	})

	t.Run("no_store_no_invention", func(t *testing.T) {
		up := newUpstream(t)
		srv := startService(t, cfgFor(up.srv.URL, ProtocolInteractions, 15))
		resp := postJSON(t, srv.URL+"/v1/chat/completions",
			`{"model":"gemini-3-flash-preview","messages":[{"role":"user","content":"hi"}]}`, nil)
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		_, _, _, upBody, _ := up.request()
		if strings.Contains(string(upBody), `"store"`) {
			t.Fatalf("不得发明 store 字段:\n%s", upBody)
		}
	})
}
