package pairs

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/convert/claude_messages"
	"github.com/dwgx/any2api/translate/convert/openai_chat"
	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/registry"
)

const (
	fOpenAI = registry.FormatOpenAIChat
	fClaude = registry.FormatClaudeMessages
	fResp   = registry.FormatOpenAIResponses
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "testdata", "cross", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return b
}

// ---------------------------------------------------------------------------
// 对装配
// ---------------------------------------------------------------------------

func TestRegistryWiring(t *testing.T) {
	reg := NewRegistry()
	for _, dir := range []struct{ from, to registry.Format }{
		{fOpenAI, fClaude},
		{fClaude, fOpenAI},
	} {
		plan, err := reg.Lookup(dir.from, dir.to)
		if err != nil {
			t.Fatalf("Lookup %s→%s: %v", dir.from, dir.to, err)
		}
		if len(plan.Steps) != 1 || plan.ViaHub || plan.Quality != registry.QualityGood {
			t.Fatalf("plan = %+v", plan)
		}
	}
	// 同格式不查表（直通是另一条路径）。
	if _, err := reg.Lookup(fClaude, fClaude); !errors.Is(err, registry.ErrNoConversion) {
		t.Fatalf("identity err = %v", err)
	}
	// 未登记格式：显式错误（绝不静默）。四个已知格式（chat/responses/claude/
	// gemini）本 slice 起全部可达，用未登记格式名验证缺省路径。
	if _, err := reg.Lookup(registry.Format("vertex_generate"), fClaude); !errors.Is(err, registry.ErrNoConversion) {
		t.Fatalf("unregistered err = %v", err)
	}
}

// ---------------------------------------------------------------------------
// 跨格式 golden（testdata/cross/）
// ---------------------------------------------------------------------------

func TestCrossGoldenRequests(t *testing.T) {
	reg := NewRegistry()
	cases := []struct {
		name string
		in   string
		want string
		from registry.Format
		to   registry.Format
	}{
		{"openai_to_claude", "request_openai_to_claude.json", "request_openai_to_claude.claude.json", fOpenAI, fClaude},
		{"claude_to_openai", "request_claude_to_openai.json", "request_claude_to_openai.openai.json", fClaude, fOpenAI},
		{"openai_to_responses", "request_openai_to_responses.json", "request_openai_to_responses.responses.json", fOpenAI, fResp},
		{"claude_to_responses", "request_claude_to_responses.json", "request_claude_to_responses.responses.json", fClaude, fResp},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := Convert(reg, tc.from, tc.to, FaceRequest, readFixture(t, tc.in), Options{})
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}
			assertJSONEqual(t, readFixture(t, tc.want), out)
		})
	}
}

func TestCrossGoldenStreams(t *testing.T) {
	reg := NewRegistry()
	cases := []struct {
		name     string
		in       string
		want     string
		from, to registry.Format
		opts     Options
	}{
		{"openai_to_claude", "stream_openai_to_claude.sse", "stream_openai_to_claude.claude.sse", fOpenAI, fClaude, Options{}},
		{"claude_to_openai_usage", "stream_claude_to_openai.sse", "stream_claude_to_openai.openai.sse", fClaude, fOpenAI, Options{IncludeUsage: true}},
		{"claude_to_openai_tool", "stream_claude_tool_to_openai.sse", "stream_claude_tool_to_openai.openai.sse", fClaude, fOpenAI, Options{}},
		{"responses_to_openai", "stream_responses_to_openai.sse", "stream_responses_to_openai.openai.sse", fResp, fOpenAI, Options{IncludeUsage: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := Convert(reg, tc.from, tc.to, FaceStream, readFixture(t, tc.in), tc.opts)
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}
			assertSSEEqual(t, readFixture(t, tc.want), out)
		})
	}
}

func TestCrossGoldenErrors(t *testing.T) {
	reg := NewRegistry()
	cases := []struct {
		name     string
		in       string
		want     string
		from, to registry.Format
	}{
		{"openai_to_claude", "error_openai.json", "error_openai.claude.json", fOpenAI, fClaude},
		{"claude_to_openai", "error_claude.json", "error_claude.openai.json", fClaude, fOpenAI},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := Convert(reg, tc.from, tc.to, FaceError, readFixture(t, tc.in), Options{})
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}
			assertJSONEqual(t, readFixture(t, tc.want), out)
		})
	}
}

// ---------------------------------------------------------------------------
// ⑥ stop_reason 双向
// ---------------------------------------------------------------------------

func TestCrossStopReasonBothWays(t *testing.T) {
	reg := NewRegistry()
	claudeCases := []struct{ in, want string }{
		{"end_turn", "stop"},
		{"max_tokens", "length"},
		{"stop_sequence", "stop"},
		{"tool_use", "tool_calls"},
		{"pause_turn", "length"}, // 产品表修正：CPA 落 stop 丢语义
		{"refusal", "content_filter"},
		{"model_context_window_exceeded", "length"},
	}
	for _, tc := range claudeCases {
		in := []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"m","content":[{"type":"text","text":"x"}],"stop_reason":"` + tc.in + `"}`)
		out, err := Convert(reg, fClaude, fOpenAI, FaceResponse, in, Options{})
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		var w struct {
			Choices []struct {
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(out, &w); err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if len(w.Choices) != 1 || w.Choices[0].FinishReason != tc.want {
			t.Fatalf("claude %s → finish_reason %q, want %q", tc.in, w.Choices[0].FinishReason, tc.want)
		}
	}
	openaiCases := []struct{ in, want string }{
		{"stop", "end_turn"},
		{"length", "max_tokens"},
		{"tool_calls", "tool_use"},
		{"content_filter", "refusal"}, // 修正 CPA 的 content_filter→end_turn 丢语义
		{"function_call", "tool_use"},
	}
	for _, tc := range openaiCases {
		in := []byte(`{"id":"c","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"x"},"finish_reason":"` + tc.in + `"}]}`)
		out, err := Convert(reg, fOpenAI, fClaude, FaceResponse, in, Options{})
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		var w struct {
			StopReason string `json:"stop_reason"`
		}
		if err := json.Unmarshal(out, &w); err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if w.StopReason != tc.want {
			t.Fatalf("openai %s → stop_reason %q, want %q", tc.in, w.StopReason, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Extra 卫生：跨格式不得泄漏来源侧独有字段
// ---------------------------------------------------------------------------

func TestCrossExtrasHygiene(t *testing.T) {
	reg := NewRegistry()

	// claude → openai：thinking/betas/output_config/system 不得进入 OpenAI body。
	claudeReq := []byte(`{
		"model": "m", "max_tokens": 10, "system": "sys",
		"thinking": {"type": "enabled", "budget_tokens": 1024},
		"betas": ["x-beta"], "output_config": {"effort": "high"}, "context_management": {"x": 1},
		"messages": [{"role": "user", "content": "hi"}]
	}`)
	out, err := Convert(reg, fClaude, fOpenAI, FaceRequest, claudeReq, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	for _, bad := range []string{`"thinking":`, `"betas":`, `"output_config":`, `"context_management":`, `"system":`} {
		if strings.Contains(string(out), bad) {
			t.Fatalf("claude-origin key %s leaked into openai body: %s", bad, out)
		}
	}
	// 但 system 消息本体必须到位。
	if !strings.Contains(string(out), `"role":"system"`) || !strings.Contains(string(out), `"content":"sys"`) {
		t.Fatalf("system message missing: %s", out)
	}

	// openai → claude：n/logprobs/seed/presence_penalty 不得进入 claude body。
	openaiReq := []byte(`{
		"model": "m",
		"messages": [{"role": "user", "content": "hi"}],
		"n": 2, "logprobs": true, "seed": 7, "presence_penalty": 0.1,
		"temperature": 0.4
	}`)
	out, err = Convert(reg, fOpenAI, fClaude, FaceRequest, openaiReq, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	for _, bad := range []string{`"n":`, `"logprobs":`, `"seed":`, `"presence_penalty":`} {
		if strings.Contains(string(out), bad) {
			t.Fatalf("openai-only key %s leaked into claude body: %s", bad, out)
		}
	}
	if !strings.Contains(string(out), `"temperature":0.4`) || !strings.Contains(string(out), `"max_tokens":32000`) {
		t.Fatalf("temperature/default max_tokens missing: %s", out)
	}
}

func TestCrossUsagePollution(t *testing.T) {
	reg := NewRegistry()

	// claude → openai：server_tool_use 不得进入 OpenAI usage；cache 折进 details。
	claudeResp := []byte(`{
		"id":"msg_1","type":"message","role":"assistant","model":"m",
		"content":[{"type":"text","text":"x"}],"stop_reason":"end_turn",
		"usage":{"input_tokens":10,"output_tokens":2,"cache_read_input_tokens":5,
			"server_tool_use":{"web_search_requests":1}}
	}`)
	out, err := Convert(reg, fClaude, fOpenAI, FaceResponse, claudeResp, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if strings.Contains(string(out), "server_tool_use") {
		t.Fatalf("server_tool_use leaked into openai usage: %s", out)
	}
	var w struct {
		Usage struct {
			PromptTokens        int64 `json:"prompt_tokens"`
			CompletionTokens    int64 `json:"completion_tokens"`
			TotalTokens         int64 `json:"total_tokens"`
			PromptTokensDetails struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if w.Usage.PromptTokens != 10 || w.Usage.CompletionTokens != 2 || w.Usage.TotalTokens != 17 ||
		w.Usage.PromptTokensDetails.CachedTokens != 5 {
		t.Fatalf("usage = %+v", w.Usage)
	}

	// openai → claude：prompt_tokens_details 不得进入 claude usage。
	openaiResp := []byte(`{
		"id":"c","object":"chat.completion","created":1,"model":"m",
		"choices":[{"index":0,"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}],
		"usage":{"prompt_tokens":9,"completion_tokens":1,"total_tokens":10,
			"prompt_tokens_details":{"cached_tokens":4}}
	}`)
	out, err = Convert(reg, fOpenAI, fClaude, FaceResponse, openaiResp, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if strings.Contains(string(out), "prompt_tokens_details") || strings.Contains(string(out), "total_tokens") {
		t.Fatalf("openai-flavored usage leaked into claude usage: %s", out)
	}
	if !strings.Contains(string(out), `"cache_read_input_tokens":4`) {
		t.Fatalf("cache read not mapped: %s", out)
	}
}

// ---------------------------------------------------------------------------
// ① system 提升往返 / ② 多轮消息
// ---------------------------------------------------------------------------

func TestCrossSystemAndMultiTurnRoundTrip(t *testing.T) {
	reg := NewRegistry()
	openaiReq := []byte(`{
		"model": "m",
		"messages": [
			{"role": "system", "content": "S"},
			{"role": "user", "content": "u1"},
			{"role": "assistant", "content": "a1"},
			{"role": "user", "content": "u2"}
		]
	}`)
	mid, err := Convert(reg, fOpenAI, fClaude, FaceRequest, openaiReq, Options{})
	if err != nil {
		t.Fatalf("o→c: %v", err)
	}
	var cw struct {
		System   string `json:"system"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(mid, &cw); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, mid)
	}
	if cw.System != "S" || len(cw.Messages) != 3 {
		t.Fatalf("claude body = %+v", cw)
	}
	if cw.Messages[0].Role != "user" || cw.Messages[1].Role != "assistant" || cw.Messages[2].Role != "user" {
		t.Fatalf("roles = %+v", cw.Messages)
	}

	back, err := Convert(reg, fClaude, fOpenAI, FaceRequest, mid, Options{})
	if err != nil {
		t.Fatalf("c→o: %v", err)
	}
	req, err := openai_chat.ParseRequest(back)
	if err != nil {
		t.Fatalf("re-parse: %v\n%s", err, back)
	}
	if len(req.Messages) != 4 {
		t.Fatalf("messages = %d, want 4", len(req.Messages))
	}
	if req.Messages[0].Role != ir.RoleSystem || req.Messages[0].Blocks[0].Text != "S" {
		t.Fatalf("m0 = %+v", req.Messages[0])
	}
	if req.Messages[3].Role != ir.RoleUser || req.Messages[3].Blocks[0].Text != "u2" {
		t.Fatalf("m3 = %+v", req.Messages[3])
	}
}

// ---------------------------------------------------------------------------
// ④ 流式工具调用跨格式组装（IR 级验证）
// ---------------------------------------------------------------------------

func TestCrossStreamToolAssemblyIR(t *testing.T) {
	reg := NewRegistry()
	out, err := Convert(reg, fOpenAI, fClaude, FaceStream, readFixture(t, "stream_openai_to_claude.sse"), Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	// claude 出口绝不发 [DONE]（AM 陷阱 5）。
	if strings.Contains(string(out), "[DONE]") {
		t.Fatalf("claude stream contains [DONE]:\n%s", out)
	}
	// 组装器能还原完整参数（input_json_delta 分片拼接）。
	events, err := claude_messages.ParseStream(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("re-parse claude stream: %v\n%s", err, out)
	}
	calls, err := ir.AssembleToolCalls(events)
	if err != nil {
		t.Fatalf("AssembleToolCalls: %v", err)
	}
	if len(calls) != 2 || calls[0].ID != "call_a" || calls[0].Arguments != `{"city":"Paris"}` ||
		calls[1].ID != "call_b" || calls[1].Arguments != "{}" {
		t.Fatalf("calls = %+v", calls)
	}
}

// ---------------------------------------------------------------------------
// ⑧ 流式错误帧双向
// ---------------------------------------------------------------------------

func TestCrossStreamErrorFrames(t *testing.T) {
	reg := NewRegistry()

	openaiErr := []byte("data: {\"error\":{\"message\":\"boom\",\"type\":\"server_error\",\"code\":\"overloaded_error\"}}\n\n")
	out, err := Convert(reg, fOpenAI, fClaude, FaceStream, openaiErr, Options{})
	if err != nil {
		t.Fatalf("o→c: %v", err)
	}
	if !strings.Contains(string(out), "event: error") ||
		!strings.Contains(string(out), `"type":"overloaded_error"`) {
		t.Fatalf("claude error frame = %s", out)
	}
	if strings.Contains(string(out), "[DONE]") {
		t.Fatalf("claude stream contains [DONE]: %s", out)
	}

	claudeErr := []byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"boom\"}}\n\n")
	out, err = Convert(reg, fClaude, fOpenAI, FaceStream, claudeErr, Options{})
	if err != nil {
		t.Fatalf("c→o: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, `"error"`) || !strings.Contains(s, `"type":"server_error"`) ||
		!strings.Contains(s, `"code":"overloaded_error"`) {
		t.Fatalf("openai error frame = %s", out)
	}
	if strings.HasSuffix(s, "data: [DONE]\n\n") {
		t.Fatalf("错误帧后不得补 [DONE]（W5-26 裁决，假成功）: %s", out)
	}
}

// ---------------------------------------------------------------------------
// 测试工具
// ---------------------------------------------------------------------------

// assertJSONEqual 语义比较两个 JSON（键序/空白不敏感）。
func assertJSONEqual(t *testing.T, want, got []byte) {
	t.Helper()
	var w, g any
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("want is not JSON: %v\n%s", err, want)
	}
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("got is not JSON: %v\n%s", err, got)
	}
	if !reflect.DeepEqual(w, g) {
		t.Fatalf("JSON mismatch:\nwant: %s\ngot:  %s", want, got)
	}
}

type frame struct {
	event string
	data  string
}

// frames 提取 SSE 帧（event 可缺省；data 多行以 \n 连接）。
func frames(t *testing.T, raw []byte) []frame {
	t.Helper()
	var out []frame
	var cur frame
	var data []string
	has := false
	flush := func() {
		if !has {
			return
		}
		cur.data = strings.Join(data, "\n")
		out = append(out, cur)
		cur = frame{}
		data = nil
		has = false
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			flush()
			continue
		}
		if v, ok := strings.CutPrefix(line, "event:"); ok {
			cur.event = strings.TrimSpace(v)
			has = true
			continue
		}
		if v, ok := strings.CutPrefix(line, "data:"); ok {
			data = append(data, strings.TrimPrefix(v, " "))
			has = true
			continue
		}
	}
	flush()
	return out
}

// assertSSEEqual 逐帧语义比较两个 SSE 流（帧数与顺序、event 名、data JSON 语义）。
func assertSSEEqual(t *testing.T, want, got []byte) {
	t.Helper()
	wf := frames(t, want)
	gf := frames(t, got)
	if len(wf) != len(gf) {
		t.Fatalf("SSE frame count mismatch: want %d, got %d\nwant:\n%s\ngot:\n%s",
			len(wf), len(gf), want, got)
	}
	for i := range wf {
		if wf[i].event != gf[i].event {
			t.Fatalf("frame %d: want event %q, got %q", i, wf[i].event, gf[i].event)
		}
		if wf[i].data == "[DONE]" || gf[i].data == "[DONE]" {
			if wf[i].data != gf[i].data {
				t.Fatalf("frame %d: want %q, got %q", i, wf[i].data, gf[i].data)
			}
			continue
		}
		assertJSONEqual(t, []byte(wf[i].data), []byte(gf[i].data))
	}
}
