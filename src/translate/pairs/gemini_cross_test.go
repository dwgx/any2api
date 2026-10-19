package pairs

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/convert/gemini"
	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/registry"
)

const fGemini = registry.FormatGeminiGenerate

// TestGeminiStreamPreservedAcrossConversion P53 quirk ④：gemini 请求体声明的
// `stream` 位必须跨转换保留进 IR 并落到目标请求体——直转（→openai）与枢纽两步
// （→claude 经 openai 中间腿）两条链都验；否则上游收不到 `stream` 会走非流式。
func TestGeminiStreamPreservedAcrossConversion(t *testing.T) {
	reg := NewRegistry()
	in := []byte(`{"contents":[{"role":"user","parts":[{"text":"u"}]}],"stream":true}`)

	direct, err := Convert(reg, fGemini, fOpenAI, FaceRequest, in, Options{})
	if err != nil {
		t.Fatalf("gemini→openai: %v", err)
	}
	if !strings.Contains(string(direct), `"stream":true`) {
		t.Fatalf("stream bit lost in gemini→openai request: %s", direct)
	}

	hub, err := Convert(reg, fGemini, fClaude, FaceRequest, in, Options{})
	if err != nil {
		t.Fatalf("gemini→claude (hub): %v", err)
	}
	if !strings.Contains(string(hub), `"stream":true`) {
		t.Fatalf("stream bit lost across hub (gemini→openai→claude): %s", hub)
	}

	// 未声明 = 不发明（两个方向都不得出现 stream 键）。
	plain := []byte(`{"contents":[{"role":"user","parts":[{"text":"u"}]}]}`)
	out, err := Convert(reg, fGemini, fOpenAI, FaceRequest, plain, Options{})
	if err != nil {
		t.Fatalf("gemini→openai (plain): %v", err)
	}
	if strings.Contains(string(out), `"stream"`) {
		t.Fatalf("stream key must not be invented: %s", out)
	}
}

// ---------------------------------------------------------------------------
// 对装配（网络扩展：显式对 + 枢纽两步合成）
// ---------------------------------------------------------------------------

func TestRegistryWiringGemini(t *testing.T) {
	reg := NewRegistry()
	// 显式对：openai_chat ↔ gemini_generate 直转。
	for _, dir := range []struct{ from, to registry.Format }{
		{fOpenAI, fGemini},
		{fGemini, fOpenAI},
	} {
		plan, err := reg.Lookup(dir.from, dir.to)
		if err != nil {
			t.Fatalf("Lookup %s→%s: %v", dir.from, dir.to, err)
		}
		if len(plan.Steps) != 1 || plan.ViaHub || plan.Quality != registry.QualityGood {
			t.Fatalf("plan %s→%s = %+v", dir.from, dir.to, plan)
		}
	}
	// 网络扩展：claude_messages ↔ gemini_generate 经枢纽两步合成（Quality ≤ Fair）。
	for _, dir := range []struct{ from, to registry.Format }{
		{fClaude, fGemini},
		{fGemini, fClaude},
	} {
		plan, err := reg.Lookup(dir.from, dir.to)
		if err != nil {
			t.Fatalf("hub Lookup %s→%s: %v", dir.from, dir.to, err)
		}
		if len(plan.Steps) != 2 || !plan.ViaHub || plan.Quality != registry.QualityFair {
			t.Fatalf("hub plan %s→%s = %+v", dir.from, dir.to, plan)
		}
		if plan.Steps[0].To != registry.FormatOpenAIChat {
			t.Fatalf("hub leg = %+v", plan.Steps)
		}
	}
}

// ---------------------------------------------------------------------------
// 跨格式 golden（testdata/cross/）
// ---------------------------------------------------------------------------

func TestCrossGoldenGeminiRequest(t *testing.T) {
	reg := NewRegistry()
	out, err := Convert(reg, fOpenAI, fGemini, FaceRequest, readFixture(t, "request_openai_to_gemini.json"), Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	assertJSONEqual(t, readFixture(t, "request_openai_to_gemini.gemini.json"), out)
}

func TestCrossGoldenGeminiResponse(t *testing.T) {
	reg := NewRegistry()
	out, err := Convert(reg, fGemini, fOpenAI, FaceResponse, readFixture(t, "response_gemini_to_openai.json"), Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	assertJSONEqual(t, readFixture(t, "response_gemini_to_openai.openai.json"), out)
}

func TestCrossGoldenGeminiStream(t *testing.T) {
	reg := NewRegistry()
	out, err := Convert(reg, fGemini, fOpenAI, FaceStream, readFixture(t, "stream_gemini_to_openai.sse"), Options{IncludeUsage: true})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	assertSSEEqual(t, readFixture(t, "stream_gemini_to_openai.openai.sse"), out)
}

// ---------------------------------------------------------------------------
// 错误信封双向（§7.2 映射表）
// ---------------------------------------------------------------------------

func TestCrossGeminiErrorEnvelope(t *testing.T) {
	reg := NewRegistry()

	// gemini → openai：status 映射到 OpenAI 类型，数值 code 承载原值。
	in := []byte(`{"error":{"code":429,"message":"quota exceeded for this key","status":"RESOURCE_EXHAUSTED"}}`)
	out, err := Convert(reg, fGemini, fOpenAI, FaceError, in, Options{})
	if err != nil {
		t.Fatalf("g→o: %v", err)
	}
	assertJSONEqual(t, []byte(`{"error":{"message":"quota exceeded for this key","type":"rate_limit_error","code":"429"}}`), out)

	// openai → gemini：类型映射回 Google status + 缺省 HTTP 码。
	in = []byte(`{"error":{"message":"bad param","type":"invalid_request_error","param":"model","code":"oops"}}`)
	out, err = Convert(reg, fOpenAI, fGemini, FaceError, in, Options{})
	if err != nil {
		t.Fatalf("o→g: %v", err)
	}
	assertJSONEqual(t, []byte(`{"error":{"code":400,"message":"bad param","status":"INVALID_ARGUMENT"}}`), out)

	// 上游 body 原文绝不外泄（grok2api 纪律）。
	in = []byte(`{"error":{"code":500,"message":"boom","status":"INTERNAL","details":[{"secret":"sk-live-abc"}]}}`)
	out, err = Convert(reg, fGemini, fOpenAI, FaceError, in, Options{})
	if err != nil {
		t.Fatalf("g→o leak case: %v", err)
	}
	if strings.Contains(string(out), "sk-live-abc") {
		t.Fatalf("upstream body leaked: %s", out)
	}
}

// ---------------------------------------------------------------------------
// 经枢纽（claude → gemini 请求：两步合成仍保 system/内容）
// ---------------------------------------------------------------------------

func TestCrossGeminiViaHubRequest(t *testing.T) {
	reg := NewRegistry()
	in := []byte(`{
		"model": "m", "max_tokens": 10, "system": "S",
		"messages": [{"role": "user", "content": "hi"}]
	}`)
	out, err := Convert(reg, fClaude, fGemini, FaceRequest, in, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	req, err := gemini.ParseRequest(out)
	if err != nil {
		t.Fatalf("re-parse gemini: %v\n%s", err, out)
	}
	if len(req.Messages) == 0 || req.Messages[0].Role != ir.RoleSystem || req.Messages[0].Blocks[0].Text != "S" {
		t.Fatalf("system lost via hub: %+v", req.Messages)
	}
	var userText string
	for _, m := range req.Messages {
		if m.Role == ir.RoleUser {
			for _, b := range m.Blocks {
				if b.Kind == ir.BlockText {
					userText = b.Text
				}
			}
		}
	}
	if userText != "hi" {
		t.Fatalf("user text lost via hub: %q", userText)
	}
	// max_tokens 经 OpenAI 枢纽键透传到 gemini generationConfig。
	if !strings.Contains(string(out), `"maxOutputTokens":10`) {
		t.Fatalf("max_tokens lost via hub: %s", out)
	}
}

// ---------------------------------------------------------------------------
// 流式：openai → gemini（哨兵纪律 + 文本增量）
// ---------------------------------------------------------------------------

func TestCrossStreamOpenAIToGemini(t *testing.T) {
	reg := NewRegistry()
	in := []byte("data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: [DONE]\n\n")
	out, err := Convert(reg, fOpenAI, fGemini, FaceStream, in, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if strings.Contains(string(out), "[DONE]") {
		t.Fatalf("gemini 出口绝不发哨兵:\n%s", out)
	}
	events, err := gemini.ParseStream(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("re-parse gemini stream: %v\n%s", err, out)
	}
	var text strings.Builder
	for _, ev := range events {
		if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockText {
			text.WriteString(ev.TextDelta)
		}
	}
	if text.String() != "Hi" {
		t.Fatalf("text = %q", text.String())
	}
	last := events[len(events)-1]
	if last.Kind != ir.EventDone || last.Reason != ir.StopStop {
		t.Fatalf("terminal = %+v", last)
	}
}

// ---------------------------------------------------------------------------
// 增量管道（StreamPipe）支持 gemini 两端
// ---------------------------------------------------------------------------

func TestStreamPipeGeminiSource(t *testing.T) {
	reg := NewRegistry()
	pipe, err := NewStreamPipe(reg, fGemini, fOpenAI, Options{IncludeUsage: true})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	frames := []string{
		`{"candidates":[{"content":{"role":"model","parts":[{"text":"Hel"}]},"index":0}],"responseId":"r1"}`,
		`{"candidates":[{"content":{"role":"model","parts":[{"text":"lo"}]},"finishReason":"STOP","index":0}],"responseId":"r1","usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":1,"totalTokenCount":3}}`,
	}
	var out bytes.Buffer
	for _, f := range frames {
		b, err := pipe.Feed("", []byte(f))
		if err != nil {
			t.Fatalf("Feed: %v", err)
		}
		out.Write(b)
	}
	b, err := pipe.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	out.Write(b)
	if !strings.Contains(out.String(), `"content":"Hel"`) || !strings.Contains(out.String(), `"content":"lo"`) {
		t.Fatalf("incremental text missing:\n%s", out.String())
	}
	if !strings.Contains(out.String(), `"usage":{"completion_tokens":1,"prompt_tokens":2,"total_tokens":3}`) {
		t.Fatalf("usage chunk missing:\n%s", out.String())
	}
	if !strings.HasSuffix(out.String(), "data: [DONE]\n\n") {
		t.Fatalf("openai 出口必须补哨兵:\n%s", out.String())
	}
}

func TestStreamPipeGeminiTarget(t *testing.T) {
	reg := NewRegistry()
	pipe, err := NewStreamPipe(reg, fOpenAI, fGemini, Options{})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	var out bytes.Buffer
	b, err := pipe.Feed("", []byte(`{"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"x"},"finish_reason":null}]}`))
	if err != nil {
		t.Fatalf("Feed: %v", err)
	}
	out.Write(b)
	b, err = pipe.Feed("", []byte(`{"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`))
	if err != nil {
		t.Fatalf("Feed 2: %v", err)
	}
	out.Write(b)
	b, err = pipe.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	out.Write(b)
	if !strings.Contains(out.String(), `"text":"x"`) || !strings.Contains(out.String(), `"finishReason":"STOP"`) {
		t.Fatalf("gemini incremental output:\n%s", out.String())
	}
	if strings.Contains(out.String(), "[DONE]") {
		t.Fatalf("sentinel leaked into gemini stream:\n%s", out.String())
	}
}

// TestCrossGeminiToClaudeViaHubResponse 网络扩展的响应腿：gemini→(openai 枢纽)→claude
// 两步合成仍保 stop_reason/工具块/usage。
func TestCrossGeminiToClaudeViaHubResponse(t *testing.T) {
	reg := NewRegistry()
	in := []byte(`{"responseId":"r1","modelVersion":"gemini-2.5-flash","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"Hi"},{"functionCall":{"id":"call_a","name":"get_weather","args":{"city":"Paris"}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":9,"candidatesTokenCount":2,"totalTokenCount":11}}`)
	out, err := Convert(reg, fGemini, fClaude, FaceResponse, in, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	var doc struct {
		StopReason string `json:"stop_reason"`
		Usage      struct {
			InputTokens  int64 `json:"input_tokens"`
			OutputTokens int64 `json:"output_tokens"`
		} `json:"usage"`
		Content []struct {
			Type  string `json:"type"`
			Name  string `json:"name"`
			Input struct {
				City string `json:"city"`
			} `json:"input"`
		} `json:"content"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	if doc.StopReason != "tool_use" {
		t.Fatalf("stop_reason = %q (functionCall 强制 toolUse 经枢纽丢失)\n%s", doc.StopReason, out)
	}
	var tool *struct {
		Type  string `json:"type"`
		Name  string `json:"name"`
		Input struct {
			City string `json:"city"`
		} `json:"input"`
	}
	for i := range doc.Content {
		if doc.Content[i].Type == "tool_use" {
			tool = &doc.Content[i]
		}
	}
	if tool == nil || tool.Name != "get_weather" || tool.Input.City != "Paris" {
		t.Fatalf("tool_use block = %+v\n%s", doc.Content, out)
	}
	if doc.Usage.InputTokens != 9 || doc.Usage.OutputTokens != 2 {
		t.Fatalf("usage = %+v", doc.Usage)
	}
}

// TestCrossGeminiStreamToolCalls 验证 gemini→openai 流式工具调用（稳定 index + 整对象参数）。
func TestCrossGeminiStreamToolCalls(t *testing.T) {
	reg := NewRegistry()
	in := []byte("data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"call_a\",\"name\":\"get_weather\",\"args\":{\"city\":\"Paris\"}}}]},\"index\":0}],\"responseId\":\"r1\"}\n\n" +
		"data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[]},\"finishReason\":\"STOP\",\"index\":0}],\"responseId\":\"r1\"}\n\n")
	out, err := Convert(reg, fGemini, fOpenAI, FaceStream, in, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, `"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Paris\"}"}}]`) {
		t.Fatalf("tool_calls chunk:\n%s", s)
	}
	if !strings.Contains(s, `"finish_reason":"tool_calls"`) {
		t.Fatalf("finish_reason must be tool_calls:\n%s", s)
	}
	// OpenAI 客户端按 index 合并：唯一一个 tool_calls 块。
	var calls []map[string]any
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimPrefix(line, "data: ")
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					ToolCalls []map[string]any `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(line), &chunk); err != nil {
			t.Fatalf("chunk: %v", err)
		}
		for _, c := range chunk.Choices {
			calls = append(calls, c.Delta.ToolCalls...)
		}
	}
	if len(calls) != 1 || calls[0]["index"] != float64(0) {
		t.Fatalf("tool calls = %+v", calls)
	}
}
