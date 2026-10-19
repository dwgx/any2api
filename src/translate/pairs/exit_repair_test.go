package pairs

import (
	"bytes"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/convert/openai_chat"
	"github.com/dwgx/any2api/translate/ir"
)

// ---------------------------------------------------------------------------
// 出口修复层接线（P53 缺口 17）：畸形 tool 参数流经生产路径（StreamPipe 出口腿）
// 被 7 步 merge / 三层修复 / 出口 `{}` 纪律收口；修不好则不发坏 JSON、终态补 error。
// 用例形状取自 P66 `assets/cases/03`（可修复截断）与 `10`（键后无值，不可修复）。
// ---------------------------------------------------------------------------

// claudeToolStream 构造一条 claude 上游工具调用流（单块，参数增量由 partial 给出）。
func claudeToolStream(partial string) []byte {
	var b strings.Builder
	frame := func(event, data string) {
		b.WriteString("event: " + event + "\n")
		b.WriteString("data: " + data + "\n\n")
	}
	frame("message_start", `{"type":"message_start","message":{"id":"msg_r1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":4,"output_tokens":0}}}`)
	frame("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tu_03","name":"Write","input":{}}}`)
	frame("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":`+jsonString(partial)+`}}`)
	frame("content_block_stop", `{"type":"content_block_stop","index":0}`)
	frame("message_delta", `{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"input_tokens":4,"output_tokens":9}}`)
	frame("message_stop", `{"type":"message_stop"}`)
	return []byte(b.String())
}

// jsonString 把字符串编码成 JSON 字符串字面量（测试输入构造用）。
func jsonString(s string) string {
	var buf bytes.Buffer
	buf.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			buf.WriteString(`\"`)
		case '\\':
			buf.WriteString(`\\`)
		case '\n':
			buf.WriteString(`\n`)
		default:
			buf.WriteRune(r)
		}
	}
	buf.WriteByte('"')
	return buf.String()
}

// TestStreamPipeRepairsTruncatedToolArgs 可修复截断（case 03 形状）：半 JSON 经出口
// 修复层补全后下发完整 arguments，下游装配结果 == 修复值；无错误帧。
func TestStreamPipeRepairsTruncatedToolArgs(t *testing.T) {
	reg := NewRegistry()
	pipe, err := NewStreamPipe(reg, fClaude, fOpenAI, Options{})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	half := `{"file_path":"/tmp/a.txt","content":"hello world`
	out := runStreamPipe(t, pipe, claudeToolStream(half))

	events, err := openai_chat.ParseStream(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("downstream parse: %v\n%s", err, out)
	}
	calls, err := ir.AssembleToolCalls(events)
	if err != nil {
		t.Fatalf("assemble: %v\n%s", err, out)
	}
	if len(calls) != 1 || calls[0].Name != "Write" {
		t.Fatalf("calls = %+v\n%s", calls, out)
	}
	want := `{"file_path":"/tmp/a.txt","content":"hello world"}`
	if calls[0].Arguments != want {
		t.Fatalf("arguments = %q, want repaired %q\n%s", calls[0].Arguments, want, out)
	}
	for _, ev := range events {
		if ev.Kind == ir.EventError {
			t.Fatalf("repairable truncation must not error: %+v", ev)
		}
	}
	if d := pipe.Diagnostics(); d.ToolArgsRepaired != 1 || d.ToolArgsFailed != 0 {
		t.Fatalf("diagnostics = %+v", d)
	} else if d.ToolArgsDefect == nil || d.ToolArgsDefect.Kind != "truncated" || d.ToolArgsDefect.RepairLayer != "structure" {
		t.Fatalf("defect attribution = %+v", d.ToolArgsDefect)
	}
}

// TestStreamPipeUnrepairableToolArgsFailClean 不可修复截断（case 10 形状：键后无值）：
// 不发坏 JSON、绝不伪装成 `{}`（下游看不到任何 arguments），终态由成功换成 error
// 帧收尾（客户端整轮重试）。
func TestStreamPipeUnrepairableToolArgsFailClean(t *testing.T) {
	reg := NewRegistry()
	pipe, err := NewStreamPipe(reg, fClaude, fOpenAI, Options{})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	out := runStreamPipe(t, pipe, claudeToolStream(`{"command":"ls","timeout":`))
	s := string(out)

	if strings.Contains(s, `"arguments"`) {
		t.Fatalf("bad/fake tool args leaked downstream: %s", s)
	}
	// 终态由成功换成 error 帧收尾（openai 出口把 api_error 映射为 server_error——
	// 错误表行为；失败细节在 message 文本里，诊断计数在管道上）。
	if !strings.Contains(s, `"error"`) || !strings.Contains(s, "工具调用参数非合法 JSON") {
		t.Fatalf("missing terminal error frame: %s", s)
	}
	// 终态是 error（不是 [DONE]-only 的成功收尾）：openai 出口按 vLLM 形态在错误帧后补哨兵。
	events, err := openai_chat.ParseStream(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("downstream parse: %v\n%s", err, out)
	}
	sawErr := false
	for _, ev := range events {
		if ev.Kind == ir.EventError {
			sawErr = true
		}
	}
	if !sawErr {
		t.Fatalf("downstream stream must terminate with error: %s", s)
	}
	if d := pipe.Diagnostics(); d.ToolArgsFailed != 1 || d.ToolArgsRepaired != 0 {
		t.Fatalf("diagnostics = %+v", d)
	} else if d.ToolArgsDefect == nil || d.ToolArgsDefect.Kind != "truncated" {
		t.Fatalf("defect attribution = %+v", d.ToolArgsDefect)
	}
}

// TestStreamPipeUnrepairableToolArgsClaudeExit 失败态与出口格式无关：openai 上游
// 半 JSON → claude 出口同样「不发坏 JSON / 不发 partial_json / 终态 error 帧」，
// 且未闭合块不阻塞收尾（claude writer 的 Finish 只要求 message_stop）。
func TestStreamPipeUnrepairableToolArgsClaudeExit(t *testing.T) {
	reg := NewRegistry()
	pipe, err := NewStreamPipe(reg, fOpenAI, fClaude, Options{})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	in := `data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}

data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"f","arguments":"{\"command\":\"ls\",\"timeout\":"}}]},"finish_reason":null}]}

data: {"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}

data: [DONE]

`
	out := runStreamPipe(t, pipe, []byte(in))
	s := string(out)
	if strings.Contains(s, "partial_json") {
		t.Fatalf("bad tool args leaked downstream: %s", s)
	}
	if !strings.Contains(s, "event: error") {
		t.Fatalf("missing claude error frame: %s", s)
	}
	if d := pipe.Diagnostics(); d.ToolArgsFailed != 1 {
		t.Fatalf("diagnostics = %+v", d)
	}
}

// ---------------------------------------------------------------------------
// thinking 过枢纽腿（P53 缺口 2）：中间腿绝不因 thinking 断链；直转面按表走
// delta.reasoning_content。
// ---------------------------------------------------------------------------

// claudeThinkingStream claude 上游流：thinking（文本 + 签名）→ text → 结束。
func claudeThinkingStream() []byte {
	var b strings.Builder
	frame := func(event, data string) {
		b.WriteString("event: " + event + "\n")
		b.WriteString("data: " + data + "\n\n")
	}
	frame("message_start", `{"type":"message_start","message":{"id":"msg_t1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":0}}}`)
	frame("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}`)
	frame("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"pondering"}}`)
	frame("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig-b64"}}`)
	frame("content_block_stop", `{"type":"content_block_stop","index":0}`)
	frame("content_block_start", `{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`)
	frame("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"answer"}}`)
	frame("content_block_stop", `{"type":"content_block_stop","index":1}`)
	frame("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":3,"output_tokens":7}}`)
	frame("message_stop", `{"type":"message_stop"}`)
	return []byte(b.String())
}

// TestStreamPipeHubThinkingDoesNotBreakChain claude→gemini（经 openai 枢纽）带
// thinking：中间腿绝不报 ErrUnsupportedBlock（链不断）。W42 ③ 起思考文本经
// reasoning_details 载体过中间腿，在 gemini 出口落为 thought part（此前整块丢弃
// ——claude→gemini 两步链静默丢思考）；不可识别签名（sig-b64）仍丢弃 + 计数。
func TestStreamPipeHubThinkingDoesNotBreakChain(t *testing.T) {
	reg := NewRegistry()
	pipe, err := NewStreamPipe(reg, fClaude, fGemini, Options{})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	out := runStreamPipe(t, pipe, claudeThinkingStream())
	s := string(out)
	if !strings.Contains(s, "answer") {
		t.Fatalf("text lost across hub: %s", s)
	}
	if !strings.Contains(s, `"text":"pondering","thought":true`) {
		t.Fatalf("hub 腿思考文本丢失（W42 ③ 应经载体到达 gemini 出口）: %s", s)
	}
	if strings.Contains(s, "sig-b64") {
		t.Fatalf("不可识别签名泄漏: %s", s)
	}
	if d := pipe.Diagnostics(); d.DroppedThinking != 0 || d.ReasoningDeltas != 1 || d.SignatureDropped != 1 {
		t.Fatalf("thinking drops not counted: %+v（want dropped=0 deltas=1 sig_dropped=1）", d)
	}
}

// TestStreamPipeDirectThinkingUsesReasoningContent claude→openai（直转面）带
// thinking：思考文本按表走 delta.reasoning_content（longtail-matrix §4.3 约定），
// 签名无载体丢弃 + 计数；不再报 ErrUnsupportedBlock。
func TestStreamPipeDirectThinkingUsesReasoningContent(t *testing.T) {
	reg := NewRegistry()
	pipe, err := NewStreamPipe(reg, fClaude, fOpenAI, Options{})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	out := runStreamPipe(t, pipe, claudeThinkingStream())
	s := string(out)
	if !strings.Contains(s, `"reasoning_content":"pondering"`) {
		t.Fatalf("reasoning_content delta missing: %s", s)
	}
	if strings.Contains(s, "sig-b64") {
		t.Fatalf("signature leaked (no carrier in openai wire): %s", s)
	}
	d := pipe.Diagnostics()
	if d.ReasoningDeltas != 1 || d.DroppedThinking != 0 || d.SignatureDropped != 1 {
		t.Fatalf("diagnostics = %+v", d)
	}
}
