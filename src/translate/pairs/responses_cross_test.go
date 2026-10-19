package pairs

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/registry"
)

// TestCrossGoldenResponsesResponse responses 非流式响应 → claude（表 3 行：
// reasoning→thinking、function_call→tool_use、usage 口径、stop_reason）。
func TestCrossGoldenResponsesResponse(t *testing.T) {
	reg := NewRegistry()
	out, err := Convert(reg, fResp, fClaude, FaceResponse, readFixture(t, "response_responses_to_claude.json"), Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	assertJSONEqual(t, readFixture(t, "response_responses_to_claude.claude.json"), out)

	var w struct {
		StopReason string `json:"stop_reason"`
		Content    []struct {
			Type      string          `json:"type"`
			Signature string          `json:"signature"`
			Input     json.RawMessage `json:"input"`
		} `json:"content"`
		Usage struct {
			Input  int64 `json:"input_tokens"`
			Output int64 `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if w.StopReason != "tool_use" {
		t.Fatalf("stop_reason = %q", w.StopReason)
	}
	if len(w.Content) != 3 || w.Content[0].Type != "thinking" || w.Content[0].Signature != "enc_c1" {
		t.Fatalf("content = %+v", w.Content)
	}
	if string(w.Content[2].Input) != `{"city":"Paris"}` {
		t.Fatalf("tool_use input = %s", w.Content[2].Input)
	}
	if w.Usage.Input != 100 || w.Usage.Output != 20 {
		t.Fatalf("usage = %+v", w.Usage)
	}
}

// TestResponsesErrorEnvelopeBothWays 错误信封双向（表「错误信封（三种）」）：
// responses 的 HTTP 错误信封与 chat 同构；claude 侧经 IR 归一。
func TestResponsesErrorEnvelopeBothWays(t *testing.T) {
	reg := NewRegistry()
	// responses → chat：信封同构（message/type/param/code 原样）。
	in := []byte(`{"error":{"message":"Invalid input","type":"invalid_request_error","param":"input","code":"invalid_prompt"}}`)
	out, err := Convert(reg, fResp, fOpenAI, FaceError, in, Options{})
	if err != nil {
		t.Fatalf("resp→chat: %v", err)
	}
	assertJSONEqual(t, in, out)

	// responses → claude：归一为 anthropic 信封（不透传上游 body 结构）。
	out, err = Convert(reg, fResp, fClaude, FaceError, in, Options{})
	if err != nil {
		t.Fatalf("resp→claude: %v", err)
	}
	var w struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if w.Type != "error" || w.Error.Type != "invalid_request_error" || w.Error.Message != "Invalid input" {
		t.Fatalf("claude envelope = %s", out)
	}

	// claude → responses：anthropic 错误体 → responses 信封。
	in = []byte(`{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}`)
	out, err = Convert(reg, fClaude, fResp, FaceError, in, Options{})
	if err != nil {
		t.Fatalf("claude→resp: %v", err)
	}
	if !strings.Contains(string(out), `"error":{`) || !strings.Contains(string(out), `"message":"slow down"`) {
		t.Fatalf("responses envelope = %s", out)
	}
}

// TestResponsesHubTwoStep gemini ↔ responses 无表可依 → 枢纽两步（Quality ≤ fair）。
func TestResponsesHubTwoStep(t *testing.T) {
	reg := NewRegistry()
	plan, err := reg.Lookup(fGemini, fResp)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(plan.Steps) != 2 || !plan.ViaHub ||
		plan.Steps[0] != (registry.Step{From: fGemini, To: fOpenAI}) ||
		plan.Steps[1] != (registry.Step{From: fOpenAI, To: fResp}) {
		t.Fatalf("plan = %+v", plan)
	}
	if plan.Quality == registry.QualityGood {
		t.Fatalf("hub plan quality = %q, want ≤ fair", plan.Quality)
	}

	in := []byte(`{"model":"gemini-2.5-pro","systemInstruction":{"parts":[{"text":"Be brief."}]},"contents":[{"role":"user","parts":[{"text":"Hi"}]}],"generationConfig":{"maxOutputTokens":64}}`)
	out, err := Convert(reg, fGemini, fResp, FaceRequest, in, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	var w struct {
		Model        string `json:"model"`
		Instructions string `json:"instructions"`
		MaxOutput    int64  `json:"max_output_tokens"`
		Input        []struct {
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"input"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v (%s)", err, out)
	}
	if w.Model != "gemini-2.5-pro" || w.Instructions != "Be brief." || w.MaxOutput != 64 {
		t.Fatalf("hub request = %+v", w)
	}
	if len(w.Input) != 1 || w.Input[0].Role != "user" || w.Input[0].Content[0].Type != "input_text" || w.Input[0].Content[0].Text != "Hi" {
		t.Fatalf("input = %+v", w.Input)
	}

	// 流：gemini SSE →（枢纽 chat）→ responses 事件流。
	stream := strings.Join([]string{
		`data: {"candidates":[{"content":{"parts":[{"text":"Hel"}],"role":"model"}}]}`,
		"",
		`data: {"candidates":[{"content":{"parts":[{"text":"lo"}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2,"totalTokenCount":5}}`,
		"",
	}, "\n")
	out, err = Convert(reg, fGemini, fResp, FaceStream, []byte(stream), Options{})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	types := sseTypes(t, out)
	if len(types) == 0 || types[0] != "response.created" || types[len(types)-1] != "response.completed" {
		t.Fatalf("frames = %+v\n%s", types, out)
	}
	if !strings.Contains(string(out), `"delta":"Hel"`) || !strings.Contains(string(out), `"delta":"lo"`) {
		t.Fatalf("text deltas missing: %s", out)
	}
	if !strings.Contains(string(out), `"usage":{"input_tokens":3`) {
		t.Fatalf("usage must ride the completed event: %s", out)
	}
}

// TestResponsesStreamPipeIncremental 增量管道（服务壳 relay 的内核）：
// responses 上游帧逐帧进 → chat 出口字节逐帧出（TTFB 与提交点纪律）。
func TestResponsesStreamPipeIncremental(t *testing.T) {
	reg := NewRegistry()
	pipe, err := NewStreamPipe(reg, fResp, fOpenAI, Options{IncludeUsage: true})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	frames := ssePayloads(t, readFixture(t, "stream_responses_to_openai.sse"))
	var got []byte
	for i, f := range frames {
		b, err := pipe.Feed("", []byte(f))
		if err != nil {
			t.Fatalf("Feed %d: %v", i, err)
		}
		got = append(got, b...)
	}
	tail, err := pipe.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	got = append(got, tail...)
	assertSSEEqual(t, readFixture(t, "stream_responses_to_openai.openai.sse"), got)

	// 增量性：首个文本 delta 帧产出下游字节（不等到流末）。
	pipe2, err := NewStreamPipe(reg, fResp, fOpenAI, Options{})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	var sawEarly bool
	for _, f := range frames {
		b, err := pipe2.Feed("", []byte(f))
		if err != nil {
			t.Fatalf("Feed: %v", err)
		}
		if len(b) > 0 && !sawEarly {
			sawEarly = true
		}
	}
	if !sawEarly {
		t.Fatalf("pipe produced no incremental bytes")
	}
}

// TestResponsesStopReasonBothWays status/incomplete_details ↔ finish_reason（表 2 行）。
func TestResponsesStopReasonBothWays(t *testing.T) {
	reg := NewRegistry()
	cases := []struct {
		status string
		reason string
		want   string
	}{
		{"completed", "", "stop"},
		{"incomplete", "max_output_tokens", "length"},
		{"incomplete", "content_filter", "content_filter"},
	}
	for _, tc := range cases {
		details := "null"
		if tc.reason != "" {
			details = `{"reason":"` + tc.reason + `"}`
		}
		in := []byte(`{"id":"resp_1","object":"response","created_at":1,"status":"` + tc.status + `","model":"m",` +
			`"output":[{"type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"x"}]}],` +
			`"incomplete_details":` + details + `,"usage":null}`)
		out, err := Convert(reg, fResp, fOpenAI, FaceResponse, in, Options{})
		if err != nil {
			t.Fatalf("%s/%s: %v", tc.status, tc.reason, err)
		}
		var w struct {
			Choices []struct {
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(out, &w); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(w.Choices) != 1 || w.Choices[0].FinishReason != tc.want {
			t.Fatalf("%s/%s → finish_reason %q, want %q", tc.status, tc.reason, w.Choices[0].FinishReason, tc.want)
		}
	}

	// chat → responses：tool_calls → 有 function_call item 的响应 → status completed。
	in := []byte(`{"id":"c1","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call_a","type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`)
	out, err := Convert(reg, fOpenAI, fResp, FaceResponse, in, Options{})
	if err != nil {
		t.Fatalf("chat→resp: %v", err)
	}
	if !strings.Contains(string(out), `"status":"completed"`) || !strings.Contains(string(out), `"type":"function_call"`) {
		t.Fatalf("responses shape = %s", out)
	}
}

// TestResponsesExtrasHygiene responses 独有键不得泄漏进 chat/claude 请求体
// （表未映射的 include/truncation/max_tool_calls/text/reasoning 在解析层即丢弃）。
func TestResponsesExtrasHygiene(t *testing.T) {
	reg := NewRegistry()
	in := []byte(`{"model":"m","input":"hi","include":["reasoning.encrypted_content"],"truncation":"auto","max_tool_calls":3,"store":true,"text":{"format":{"type":"text"}},"reasoning":{"effort":"low"}}`)
	out, err := Convert(reg, fResp, fOpenAI, FaceRequest, in, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	s := string(out)
	for _, banned := range []string{`"include"`, `"truncation"`, `"max_tool_calls"`, `"text"`, `"reasoning"`} {
		if strings.Contains(s, banned) {
			t.Fatalf("%s leaked into chat body: %s", banned, s)
		}
	}
	// 透传族保留（store 同名；reasoning.effort → reasoning_effort）。
	if !strings.Contains(s, `"store":true`) || !strings.Contains(s, `"reasoning_effort":"low"`) {
		t.Fatalf("passthrough family lost: %s", s)
	}
}

// sseTypes 提取 SSE 帧 payload 的 type 序列。
func sseTypes(t *testing.T, data []byte) []string {
	t.Helper()
	var out []string
	for _, raw := range ssePayloads(t, data) {
		var w struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(raw), &w); err != nil {
			t.Fatalf("frame: %v (%s)", err, raw)
		}
		out = append(out, w.Type)
	}
	return out
}

// ssePayloads 提取 SSE 帧的 data 载荷（多行 data 以 \n 连接）。
func ssePayloads(t *testing.T, data []byte) []string {
	t.Helper()
	var frames []string
	var cur []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			if len(cur) > 0 {
				frames = append(frames, strings.Join(cur, "\n"))
				cur = nil
			}
			continue
		}
		if v, ok := strings.CutPrefix(line, "data:"); ok {
			cur = append(cur, strings.TrimPrefix(v, " "))
		}
	}
	if len(cur) > 0 {
		frames = append(frames, strings.Join(cur, "\n"))
	}
	return frames
}
