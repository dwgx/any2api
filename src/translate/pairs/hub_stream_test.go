package pairs

import (
	"bytes"
	"encoding/json"

	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/convert/claude_messages"

	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/registry"
)

// runStreamPipe 把 SSE 字节流逐帧喂给管道，返回全部产出（含 Finish 收尾）。
func runStreamPipe(t *testing.T, pipe *StreamPipe, raw []byte) []byte {
	t.Helper()
	var out []byte
	feedSSEFrames(t, raw, func(event string, data []byte) error {
		b, err := pipe.Feed(event, data)
		out = append(out, b...)
		return err
	})
	b, err := pipe.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	return append(out, b...)
}

// ---------------------------------------------------------------------------
// 枢纽两步流式 golden（入口格式 → 枢纽 openai → 出口格式）
// ---------------------------------------------------------------------------

// TestHubStreamGolden 枢纽两步增量管道逐帧喂入 → 出口 SSE 与期望文件逐帧语义相等。
// 覆盖：文本 / 工具调用（claude→gemini 经枢纽）、usage 快照（gemini→claude 经枢纽）、
// 中途错误（上游错误帧 → 出口 error 帧）。
func TestHubStreamGolden(t *testing.T) {
	reg := NewRegistry()
	cases := []struct {
		name     string
		in       string
		want     string
		from, to registry.Format
	}{
		{"claude_to_gemini_text", "stream_claude_to_gemini_hub.sse", "stream_claude_to_gemini_hub.gemini.sse", fClaude, fGemini},
		{"claude_to_gemini_tool", "stream_claude_tool_to_gemini_hub.sse", "stream_claude_tool_to_gemini_hub.gemini.sse", fClaude, fGemini},
		{"gemini_to_claude_usage", "stream_gemini_to_claude_hub.sse", "stream_gemini_to_claude_hub.claude.sse", fGemini, fClaude},
		{"gemini_to_claude_error", "stream_gemini_error_to_claude_hub.sse", "stream_gemini_error_to_claude_hub.claude.sse", fGemini, fClaude},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pipe, err := NewStreamPipe(reg, tc.from, tc.to, Options{})
			if err != nil {
				t.Fatalf("NewStreamPipe: %v", err)
			}
			out := runStreamPipe(t, pipe, readFixture(t, tc.in))
			assertSSEEqual(t, readFixture(t, tc.want), out)
		})
	}
}

// TestHubStreamBatchParity 批量 Convert（同一计划两步）与增量管道同语义——两条路径
// 的唯一契约差异是 claude 出口的 message_start.usage：批量预扫把 input/cache 全量落
// start；增量 start 用 0 占位、全量在 message_delta（CC B7 客户端合并规则：>0 的
// delta 覆盖占位）。比较前把两侧 message_start.usage 归零。
func TestHubStreamBatchParity(t *testing.T) {
	reg := NewRegistry()
	cases := []struct {
		name     string
		in       string
		from, to registry.Format
		opts     Options
	}{
		{"claude_to_gemini_text", "stream_claude_to_gemini_hub.sse", fClaude, fGemini, Options{}},
		{"claude_to_gemini_tool", "stream_claude_tool_to_gemini_hub.sse", fClaude, fGemini, Options{}},
		{"gemini_to_claude_usage", "stream_gemini_to_claude_hub.sse", fGemini, fClaude, Options{}},
		{"gemini_to_claude_error", "stream_gemini_error_to_claude_hub.sse", fGemini, fClaude, Options{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := readFixture(t, tc.in)
			batch, err := Convert(reg, tc.from, tc.to, FaceStream, in, tc.opts)
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}
			pipe, err := NewStreamPipe(reg, tc.from, tc.to, tc.opts)
			if err != nil {
				t.Fatalf("NewStreamPipe: %v", err)
			}
			incr := runStreamPipe(t, pipe, in)
			assertSSEEqual(t, zeroStartUsage(t, batch), zeroStartUsage(t, incr))
		})
	}
}

// zeroStartUsage 把 claude 出口 message_start 帧的 usage 归零（见 TestHubStreamBatchParity）。
func zeroStartUsage(t *testing.T, raw []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	for _, f := range frames(t, raw) {
		if f.data == "[DONE]" || !strings.HasPrefix(f.data, "{") {
			b.WriteString(f.data)
			b.WriteString("\n\n")
			continue
		}
		var doc map[string]any
		if err := json.Unmarshal([]byte(f.data), &doc); err != nil {
			t.Fatalf("frame: %v", err)
		}
		if doc["type"] == "message_start" {
			if msg, ok := doc["message"].(map[string]any); ok {
				msg["usage"] = map[string]any{}
			}
		}
		out, err := json.Marshal(doc)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if f.event != "" {
			b.WriteString("event: " + f.event + "\n")
		}
		b.WriteString("data: ")
		b.Write(out)
		b.WriteString("\n\n")
	}
	return b.Bytes()
}

// ---------------------------------------------------------------------------
// 提交点纪律（跨两步）
// ---------------------------------------------------------------------------

// TestHubStreamPreCommitFailureRetryable 提交前失败：Feed 不产任何下游字节（未提交），
// 调用方可丢弃管道换上游重试（新建管道重放同一输入 → 正常产出）；失败管道是终态
// （粘住），不回退不重启。
func TestHubStreamPreCommitFailureRetryable(t *testing.T) {
	reg := NewRegistry()
	in := readFixture(t, "stream_claude_to_gemini_hub.sse")

	pipe, err := NewStreamPipe(reg, fClaude, fGemini, Options{})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	out, err := pipe.Feed("", []byte("{not json"))
	if err == nil {
		t.Fatalf("garbage frame must error")
	}
	if len(out) != 0 {
		t.Fatalf("pre-commit failure must not produce downstream bytes: %q", out)
	}
	firstErr := err.Error()
	if _, err := pipe.Feed("", []byte("{}")); err == nil || err.Error() != firstErr {
		t.Fatalf("failed pipe must be terminal (sticky error): %v", err)
	}
	if _, err := pipe.Finish(); err == nil || err.Error() != firstErr {
		t.Fatalf("failed pipe Finish must return the sticky error: %v", err)
	}

	// 重试：新管道（换上游）重放 → 正常产出。
	retry, err := NewStreamPipe(reg, fClaude, fGemini, Options{})
	if err != nil {
		t.Fatalf("retry pipe: %v", err)
	}
	got := runStreamPipe(t, retry, in)
	assertSSEEqual(t, readFixture(t, "stream_claude_to_gemini_hub.gemini.sse"), got)
}

// TestHubStreamPostCommitErrorFrame 下游已提交后上游错误：不重试、不回退——调用方写
// 出口格式错误帧收尾（pipe.RenderError 跨两步直出出口格式）。出口流 + 错误帧仍是
// 出口解析器可消费的合法终态流。
func TestHubStreamPostCommitErrorFrame(t *testing.T) {
	reg := NewRegistry()

	pipe, err := NewStreamPipe(reg, fGemini, fClaude, Options{})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	// 首帧产出 → 下游已提交。
	committed, err := pipe.Feed("", []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"Hi"}]},"index":0}],"responseId":"r1"}`))
	if err != nil {
		t.Fatalf("first frame: %v", err)
	}
	if len(committed) == 0 {
		t.Fatalf("first frame must produce downstream bytes")
	}
	// 上游帧被拒 → 错误上抛；管道粘住（不重试/不回退）。
	if _, err := pipe.Feed("", []byte("{not json")); err == nil {
		t.Fatalf("garbage frame must error")
	}
	if _, err := pipe.Feed("", []byte("{}")); err == nil {
		t.Fatalf("pipe must not accept frames after failure (no retry)")
	}

	// 收尾：出口（claude）格式错误帧——信封直出，不经枢纽二次映射。SSE 帧包装
	//（`event: error\ndata: …`）是服务壳职责（service/relay.go writeDownstreamError），
	// 库层只产信封 body——测试按同一形状拼帧。
	body, err := pipe.RenderError(&ir.Error{
		Type: "api_error", Message: "upstream stream frame rejected", Source: "any2api",
	})
	if err != nil {
		t.Fatalf("RenderError: %v", err)
	}
	assertJSONEqual(t, []byte(`{"type":"error","error":{"type":"api_error","message":"upstream stream frame rejected"}}`), body)
	frame := []byte("event: error\ndata: " + string(body) + "\n\n")
	// 出口流（已提交部分 + 错误帧）仍是出口解析器可消费的合法终态流。
	events, err := claude_messages.ParseStream(bytes.NewReader(append(committed, frame...)))
	if err != nil {
		t.Fatalf("post-commit stream must stay parseable: %v\n%s", err, append(committed, frame...))
	}
	if len(events) == 0 || events[len(events)-1].Kind != ir.EventError {
		t.Fatalf("terminal event must be error: %+v", events)
	}

	// 另一方向：枢纽两步的 gemini 出口错误信封（Google 形状：数值 code + status）。
	pipeG, err := NewStreamPipe(reg, fClaude, fGemini, Options{})
	if err != nil {
		t.Fatalf("gemini pipe: %v", err)
	}
	bodyG, err := pipeG.RenderError(&ir.Error{Type: "overloaded_error", Message: "busy", Source: "any2api"})
	if err != nil {
		t.Fatalf("RenderError gemini: %v", err)
	}
	var doc struct {
		Error struct {
			Code    any    `json:"code"`
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal(bodyG, &doc); err != nil {
		t.Fatalf("gemini error envelope: %v\n%s", err, bodyG)
	}
	if doc.Error.Message != "busy" || doc.Error.Status == "" || doc.Error.Code == nil {
		t.Fatalf("gemini error envelope: %s", bodyG)
	}
}

// TestHubStreamUsageBridgesHub usage 必须跨枢纽腿（内部腿恒 include_usage）：gemini
// 快照 usage 经两步后落到 claude message_delta（末帧权威）。
func TestHubStreamUsageBridgesHub(t *testing.T) {
	reg := NewRegistry()
	pipe, err := NewStreamPipe(reg, fGemini, fClaude, Options{}) // 客户端未要求 usage
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	out := runStreamPipe(t, pipe, readFixture(t, "stream_gemini_to_claude_hub.sse"))
	if !strings.Contains(string(out), `"usage":{"input_tokens":3,"output_tokens":2}`) {
		t.Fatalf("usage lost across hub: %s", out)
	}
}

// ---------------------------------------------------------------------------
// Extra 防泄漏（跨格式 hygiene）
// ---------------------------------------------------------------------------

// TestCrossExtrasHygieneGeminiDirections 跨格式 Extra 防泄漏表驱动（gemini 方向）：
// 来源格式独有键绝不出现在目标请求体（含经枢纽两步的中间腿与出口腿）。4 个方向组合
// + 工具定义级来源独有键。claude↔openai 方向见 TestCrossExtrasHygiene（既有）。
func TestCrossExtrasHygieneGeminiDirections(t *testing.T) {
	reg := NewRegistry()

	geminiOnly := []string{"safetySettings", "thinkingConfig", "responseMimeType", "responseJsonSchema", "responseModalities"}
	claudeOnly := []string{"betas", "thinking", "output_config", "context_management", "speed", "metadata"}
	openaiOnly := []string{"logprobs", "top_logprobs", "store", "metadata"}
	claudeToolOnly := []string{"cache_control", "defer_loading", "eager_input_streaming"}

	geminiReq := []byte(`{
		"contents": [{"role": "user", "parts": [{"text": "hi"}]}],
		"generationConfig": {"maxOutputTokens": 64, "responseMimeType": "application/json",
			"responseJsonSchema": {"type": "object"}, "responseModalities": ["TEXT"],
			"thinkingConfig": {"thinkingBudget": 1024}},
		"safetySettings": [{"category": "HARM_CATEGORY_HARASSMENT", "threshold": "OFF"}]
	}`)
	claudeReq := []byte(`{
		"model": "m", "max_tokens": 16,
		"betas": ["x"], "thinking": {"type": "enabled"}, "output_config": {"effort": "medium"},
		"context_management": {"edits": []}, "speed": "fast", "metadata": {"user_id": "u"},
		"system": "S",
		"messages": [{"role": "user", "content": "hi"}],
		"tools": [{"name": "f", "input_schema": {"type": "object"},
			"cache_control": {"type": "ephemeral"}, "defer_loading": true, "eager_input_streaming": true}]
	}`)
	openaiReq := []byte(`{
		"model": "m",
		"messages": [{"role": "user", "content": "hi"}],
		"logprobs": true, "top_logprobs": 3, "store": true, "metadata": {"k": "v"},
		"tools": [{"type": "function", "function": {"name": "f", "parameters": {"type": "object"},
			"cache_control": {"type": "ephemeral"}}}]
	}`)

	cases := []struct {
		name    string
		from    registry.Format
		to      registry.Format
		in      []byte
		forbid  []string
		keepKey string // 必须保留的跨格式直迁键（防过滤过度）
	}{
		{"gemini_to_openai", fGemini, fOpenAI, geminiReq, geminiOnly, "max_tokens"},
		{"gemini_to_claude_via_hub", fGemini, fClaude, geminiReq, geminiOnly, "max_tokens"},
		{"openai_to_gemini", fOpenAI, fGemini, openaiReq, openaiOnly, "safetySettings"},
		{"claude_to_gemini_via_hub", fClaude, fGemini, claudeReq, claudeOnly, "maxOutputTokens"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := Convert(reg, tc.from, tc.to, FaceRequest, tc.in, Options{})
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}
			for _, k := range tc.forbid {
				if containsJSONKey(t, out, k) {
					t.Fatalf("foreign key %q leaked into %s body: %s", k, tc.to, out)
				}
			}
			if tc.keepKey != "" && !containsJSONKey(t, out, tc.keepKey) {
				t.Fatalf("cross-format key %q lost: %s", tc.keepKey, out)
			}
		})
	}

	// 工具定义级：claude 工具独有键经直转/枢纽都不得进 openai/gemini 工具定义。
	for _, dir := range []struct {
		name     string
		from, to registry.Format
	}{
		{"claude_to_openai_tools", fClaude, fOpenAI},
		{"claude_to_gemini_tools_via_hub", fClaude, fGemini},
		{"openai_to_gemini_tools", fOpenAI, fGemini},
	} {
		t.Run(dir.name, func(t *testing.T) {
			in := claudeReq
			if dir.from == fOpenAI {
				in = openaiReq
			}
			out, err := Convert(reg, dir.from, dir.to, FaceRequest, in, Options{})
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}
			for _, k := range claudeToolOnly {
				if containsJSONKey(t, out, k) {
					t.Fatalf("tool-level foreign key %q leaked: %s", k, out)
				}
			}
		})
	}

	// 枢纽中间腿（gemini→openai 的 openai 渲染）本身也必须干净：否则中间体一旦
	// 被复用为上游请求体（同协议直发）就会带上 gemini 独有键。
	intermediate, err := Convert(reg, fGemini, fOpenAI, FaceRequest, geminiReq, Options{})
	if err != nil {
		t.Fatalf("intermediate: %v", err)
	}
	for _, k := range geminiOnly {
		if containsJSONKey(t, intermediate, k) {
			t.Fatalf("foreign key %q leaked into hub intermediate body: %s", k, intermediate)
		}
	}
}

// containsJSONKey 递归检查 JSON 里是否出现指定键（任意层级）。
func containsJSONKey(t *testing.T, raw []byte, key string) bool {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	return jsonHasKey(v, key)
}

func jsonHasKey(v any, key string) bool {
	switch x := v.(type) {
	case map[string]any:
		for k, sub := range x {
			if k == key || jsonHasKey(sub, key) {
				return true
			}
		}
	case []any:
		for _, sub := range x {
			if jsonHasKey(sub, key) {
				return true
			}
		}
	}
	return false
}
