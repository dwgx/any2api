package claude_messages

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

func TestRenderRequestSystemHoist(t *testing.T) {
	// 多条 system 消息 → 数组元素，不合并（CC A9）。
	req := &ir.Request{
		Model: "m",
		Messages: []ir.Message{
			{Role: ir.RoleSystem, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "S1"}}},
			{Role: ir.RoleSystem, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "S2"}}},
			{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "hi"}}},
		},
	}
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	var w struct {
		System    []map[string]any `json:"system"`
		Messages  []map[string]any `json:"messages"`
		MaxTokens int              `json:"max_tokens"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	if len(w.System) != 2 || w.System[0]["text"] != "S1" || w.System[1]["text"] != "S2" {
		t.Fatalf("system = %+v", w.System)
	}
	if len(w.Messages) != 1 || w.Messages[0]["role"] != "user" {
		t.Fatalf("messages = %+v", w.Messages)
	}
	if w.MaxTokens != defaultMaxTokens {
		t.Fatalf("max_tokens = %d, want default %d", w.MaxTokens, defaultMaxTokens)
	}

	// 单条 system 消息单 text 块 → 字符串。
	req.Messages = req.Messages[:1]
	out, err = RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	var w2 struct {
		System string `json:"system"`
	}
	if err := json.Unmarshal(out, &w2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if w2.System != "S1" {
		t.Fatalf("system = %q", w2.System)
	}
}

func TestRenderRequestToolResultGroupingAndSynthesis(t *testing.T) {
	// assistant 两个 tool_use，只回了一个结果 → 同一条 user 消息内补 is_error 占位。
	req := &ir.Request{
		Model: "m",
		Messages: []ir.Message{
			{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
				{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{ID: "a", Name: "f", Arguments: `{"x":1}`}},
				{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{ID: "b", Name: "g", Arguments: ""}},
			}},
			{Role: ir.RoleTool, Blocks: []ir.ContentBlock{
				{Kind: ir.BlockToolResult, ToolResult: &ir.ToolResult{CallID: "a", Content: "ok"}},
			}},
		},
	}
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	var w struct {
		Messages []struct {
			Role    string `json:"role"`
			Content []struct {
				Type      string          `json:"type"`
				ID        string          `json:"id"`
				ToolUseID string          `json:"tool_use_id"`
				Input     json.RawMessage `json:"input"`
				IsError   bool            `json:"is_error"`
			} `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	if len(w.Messages) != 2 {
		t.Fatalf("messages = %d, want 2", len(w.Messages))
	}
	asst := w.Messages[0]
	if asst.Role != "assistant" || len(asst.Content) != 2 || asst.Content[0].Type != "tool_use" {
		t.Fatalf("assistant = %+v", asst)
	}
	if string(asst.Content[0].Input) != `{"x":1}` || string(asst.Content[1].Input) != "{}" {
		t.Fatalf("inputs = %s / %s", asst.Content[0].Input, asst.Content[1].Input)
	}
	usr := w.Messages[1]
	if usr.Role != "user" || len(usr.Content) != 2 {
		t.Fatalf("user = %+v", usr)
	}
	if usr.Content[0].ToolUseID != "a" || usr.Content[0].IsError {
		t.Fatalf("result a = %+v", usr.Content[0])
	}
	if usr.Content[1].ToolUseID != "b" || !usr.Content[1].IsError {
		t.Fatalf("synthesized result b = %+v", usr.Content[1])
	}
}

func TestRenderRequestToolChoiceAndParallel(t *testing.T) {
	req := &ir.Request{
		Model:      "m",
		ToolChoice: json.RawMessage(`"required"`),
		Extra:      map[string]json.RawMessage{"parallel_tool_calls": json.RawMessage("false")},
		Messages:   []ir.Message{{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "hi"}}}},
	}
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	var w struct {
		ToolChoice map[string]any `json:"tool_choice"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if w.ToolChoice["type"] != "any" || w.ToolChoice["disable_parallel_tool_use"] != true {
		t.Fatalf("tool_choice = %+v", w.ToolChoice)
	}

	// 无 tool_choice + parallel false → 合成 auto+disable。
	req.ToolChoice = nil
	out, err = RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if w.ToolChoice["type"] != "auto" || w.ToolChoice["disable_parallel_tool_use"] != true {
		t.Fatalf("tool_choice = %+v", w.ToolChoice)
	}
}

func TestRenderRequestExtrasWhitelistAndStop(t *testing.T) {
	req := &ir.Request{
		Model: "m",
		Extra: map[string]json.RawMessage{
			"temperature":           json.RawMessage("0.5"),
			"stop":                  json.RawMessage(`"END"`),
			"n":                     json.RawMessage("2"),
			"logprobs":              json.RawMessage("true"),
			"presence_penalty":      json.RawMessage("0.1"),
			"max_completion_tokens": json.RawMessage("77"),
		},
		Messages: []ir.Message{{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "hi"}}}},
	}
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	s := string(out)
	for _, bad := range []string{`"n"`, "logprobs", "presence_penalty"} {
		if strings.Contains(s, bad) {
			t.Fatalf("openai-only extra %q leaked into claude body: %s", bad, s)
		}
	}
	var w struct {
		Temperature   float64  `json:"temperature"`
		StopSequences []string `json:"stop_sequences"`
		MaxTokens     int      `json:"max_tokens"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if w.Temperature != 0.5 || len(w.StopSequences) != 1 || w.StopSequences[0] != "END" {
		t.Fatalf("body = %+v", w)
	}
	if w.MaxTokens != 77 {
		t.Fatalf("max_tokens = %d, want 77 (max_completion_tokens 映射)", w.MaxTokens)
	}
}

func TestRenderResponseStopReasonPriority(t *testing.T) {
	blocks := []ir.ContentBlock{{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{ID: "t", Name: "f"}}}
	// stop + 工具块 → 强制 tool_use。
	resp := &ir.Response{ID: "msg_1", Model: "m", Message: ir.Message{Role: ir.RoleAssistant, Blocks: blocks}, StopReason: ir.StopStop}
	out, err := RenderResponse(resp)
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	var w struct {
		StopReason string         `json:"stop_reason"`
		Type       string         `json:"type"`
		Usage      map[string]any `json:"usage"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if w.StopReason != "tool_use" || w.Type != "message" {
		t.Fatalf("stop = %q type = %q", w.StopReason, w.Type)
	}
	// length + 工具块 → 保留截断语义（不伪造成功）。
	resp.StopReason = ir.StopLength
	out, err = RenderResponse(resp)
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if w.StopReason != "max_tokens" {
		t.Fatalf("stop = %q, want max_tokens", w.StopReason)
	}
}

func TestRenderResponseInvalidToolInput(t *testing.T) {
	resp := &ir.Response{
		ID: "msg_1", Model: "m",
		Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
			{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{ID: "t", Name: "f", Arguments: `{"broken":`}},
		}},
	}
	if _, err := RenderResponse(resp); !errors.Is(err, ErrInvalidToolInput) {
		t.Fatalf("err = %v, want ErrInvalidToolInput", err)
	}
}

func TestRenderErrorTypeMapping(t *testing.T) {
	// openai 侧独有类型 → claude 最近邻；overloaded_error 文本匹配优先（CC C2）。
	cases := []struct {
		in   *ir.Error
		want string
	}{
		{&ir.Error{Type: "invalid_api_key", Message: "x"}, "authentication_error"},
		{&ir.Error{Type: "insufficient_quota", Message: "x"}, "billing_error"},
		{&ir.Error{Type: "server_error", Message: "x"}, "api_error"},
		{&ir.Error{Type: "server_error", Code: "overloaded_error", Message: "x"}, "overloaded_error"},
		{&ir.Error{Type: "context_length_exceeded", Message: "x"}, "invalid_request_error"},
		{&ir.Error{Type: "invalid_request_error", Message: "x"}, "invalid_request_error"},
		{&ir.Error{Type: "brand_new", Message: "x"}, "brand_new"},
		{&ir.Error{Message: "x"}, "api_error"},
	}
	for _, tc := range cases {
		out, err := RenderError(tc.in)
		if err != nil {
			t.Fatalf("RenderError: %v", err)
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
		if w.Type != "error" || w.Error.Type != tc.want {
			t.Fatalf("%+v → %q, want %q", tc.in, w.Error.Type, tc.want)
		}
	}
}

func TestRenderStreamRedactedThinkingHoist(t *testing.T) {
	// redacted_thinking 载荷在 wire 中只出现在 content_block_start：预扫回填。
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "m", Model: "x"},
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockRedactedThinking},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockRedactedThinking, SignatureDelta: "enc+"},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockRedactedThinking, SignatureDelta: "data"},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockRedactedThinking},
		{Kind: ir.EventMessageDelta, StopReason: ir.StopStop},
		{Kind: ir.EventMessageStop},
		{Kind: ir.EventDone, Reason: ir.StopStop},
	}
	out, err := RenderStream(events)
	if err != nil {
		t.Fatalf("RenderStream: %v", err)
	}
	if strings.Contains(string(out), "[DONE]") {
		t.Fatalf("claude stream must not contain [DONE]:\n%s", out)
	}
	frames := claudeFrames(t, out)
	if len(frames) != 5 {
		t.Fatalf("frames = %d, want 5:\n%s", len(frames), out)
	}
	if frames[1].event != "content_block_start" || !strings.Contains(frames[1].data, `"data":"enc+data"`) {
		t.Fatalf("frame 1 = %+v", frames[1])
	}
	if frames[2].event != "content_block_stop" {
		t.Fatalf("frame 2 = %+v", frames[2])
	}
}

// TestRenderStreamStopSequenceBackfill W44-1（R1 出口腿）：IR StopSequence 非空 ⇒
// message_delta 渲染 `stop_reason:"stop_sequence"` + `stop_sequence:<值>`
// （claude.ai 配额截断 usage_limit 的保真回填，W43-3 §4.3 R1）；空 ⇒ 维持既有
// 形态（stopReasonFor + null）。
func TestRenderStreamStopSequenceBackfill(t *testing.T) {
	base := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "m", Model: "x"},
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockText},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockText, TextDelta: "hi"},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockText},
		{Kind: ir.EventMessageDelta, StopReason: ir.StopStop, StopSequence: "usage_limit"},
		{Kind: ir.EventMessageStop},
	}
	out, err := RenderStream(base)
	if err != nil {
		t.Fatalf("RenderStream: %v", err)
	}
	text := string(out)
	if !strings.Contains(text, `"stop_reason":"stop_sequence"`) || !strings.Contains(text, `"stop_sequence":"usage_limit"`) {
		t.Fatalf("StopSequence 未回填:\n%s", text)
	}
	// 空 StopSequence 回归：旧形态（end_turn + null）逐帧不变。
	reg := append([]ir.StreamEvent{}, base...)
	reg[4] = ir.StreamEvent{Kind: ir.EventMessageDelta, StopReason: ir.StopStop}
	out2, err := RenderStream(reg)
	if err != nil {
		t.Fatalf("RenderStream(no seq): %v", err)
	}
	text2 := string(out2)
	if !strings.Contains(text2, `"stop_reason":"end_turn"`) || !strings.Contains(text2, `"stop_sequence":null`) {
		t.Fatalf("无 StopSequence 形态回归失败:\n%s", text2)
	}
}

// TestRenderResponseStopSequenceBackfill W52-1（W44-1 §9 残留 1 非流式对齐）：
// IR StopSequence 非空 ⇒ 渲染 `stop_reason:"stop_sequence"` + `stop_sequence:<值>`
// （AM 契约：stop_sequence 非 null ⟺ stop_reason=="stop_sequence"；与流式
// emitMessageDelta 同款覆盖——含工具块强制 tool_use 的覆盖）；空 ⇒ 旧形态
// （effectiveStopReason + null）逐字节不变；parse(render) 往返不动点。
func TestRenderResponseStopSequenceBackfill(t *testing.T) {
	resp := &ir.Response{
		ID: "msg_1", Model: "m",
		Message:      ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "x"}}},
		StopReason:   ir.StopStop,
		StopSequence: "END",
	}
	out, err := RenderResponse(resp)
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	var w struct {
		StopReason   string  `json:"stop_reason"`
		StopSequence *string `json:"stop_sequence"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	if w.StopReason != "stop_sequence" || w.StopSequence == nil || *w.StopSequence != "END" {
		t.Fatalf("stop_reason/stop_sequence = %q/%v, want stop_sequence/END", w.StopReason, w.StopSequence)
	}
	// 空 StopSequence 回归：旧形态（end_turn + null）不变。
	reg := *resp
	reg.StopSequence = ""
	out2, err := RenderResponse(&reg)
	if err != nil {
		t.Fatalf("RenderResponse(reg): %v", err)
	}
	var w2 struct {
		StopReason   string  `json:"stop_reason"`
		StopSequence *string `json:"stop_sequence"`
	}
	if err := json.Unmarshal(out2, &w2); err != nil {
		t.Fatalf("unmarshal2: %v\n%s", err, out2)
	}
	if w2.StopReason != "end_turn" || w2.StopSequence != nil {
		t.Fatalf("回归形态 = %q/%v, want end_turn/null", w2.StopReason, w2.StopSequence)
	}
	// 覆盖工具块强制：stop_sequence 在场 ⇒ 即便有 tool_use 块也渲染
	// stop_sequence（与流式 emitMessageDelta 的无条件覆盖同款）。
	tool := *resp
	tool.Message = ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
		{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{ID: "t", Name: "f", Arguments: "{}"}},
	}}
	out3, err := RenderResponse(&tool)
	if err != nil {
		t.Fatalf("RenderResponse(tool): %v", err)
	}
	var w3 struct {
		StopReason   string  `json:"stop_reason"`
		StopSequence *string `json:"stop_sequence"`
	}
	if err := json.Unmarshal(out3, &w3); err != nil {
		t.Fatalf("unmarshal3: %v\n%s", err, out3)
	}
	if w3.StopReason != "stop_sequence" || w3.StopSequence == nil || *w3.StopSequence != "END" {
		t.Fatalf("工具块覆盖 = %q/%v, want stop_sequence/END", w3.StopReason, w3.StopSequence)
	}
	// 往返不动点：parse(render(x)) 回读 stop_sequence，再渲染语义相等。
	back, err := ParseResponse(out)
	if err != nil {
		t.Fatalf("re-ParseResponse: %v", err)
	}
	if back.StopSequence != "END" {
		t.Fatalf("回读 StopSequence = %q", back.StopSequence)
	}
	out4, err := RenderResponse(back)
	if err != nil {
		t.Fatalf("re-RenderResponse: %v", err)
	}
	assertJSONEqual(t, out, out4)
}

func TestRenderStreamRequiresStartAndStop(t *testing.T) {
	if _, err := RenderStream(nil); !errors.Is(err, ErrMissingStart) {
		t.Fatalf("empty err = %v", err)
	}
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "m", Model: "x"},
		{Kind: ir.EventMessageStop},
	}
	if _, err := RenderStream(events); !errors.Is(err, ErrMissingStopReason) {
		t.Fatalf("missing stop_reason err = %v", err)
	}
	events2 := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "m", Model: "x"},
		{Kind: ir.EventMessageDelta, StopReason: ir.StopStop},
	}
	if _, err := RenderStream(events2); !errors.Is(err, ErrMissingMessageStop) {
		t.Fatalf("missing message_stop err = %v", err)
	}
}

// TestRenderStreamServerToolBlocks W11-3：服务端工具块（claude_web 上游实抓形态：
// server tool_use 后恒跟 tool_result）在 claude_messages 流式出口的行为契约：
//   - server_tool_use 块**保留**（Anthropic 线原生类型；Claude Code 有专门分支
//     ——cc-wire-contract B6/未决 4；信息面：模型在上游用了什么工具对用户可见）；
//   - tool_result 块**整块丢弃**（客户端语义的 tool_result 必须配对客户端
//     tool_use——透传会把不配对结果塞进客户端会话状态；BK parseCompletionSSE
//     同款丢弃）；
//   - 丢弃**不留 index 空洞**：下游 index 连续从 0 起（CC B5「index 从 0 递增
//     且与 start 一致」）。
func TestRenderStreamServerToolBlocks(t *testing.T) {
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "m", Model: "x"},
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockThinking},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockThinking, TextDelta: "想"},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockThinking},
		{Kind: ir.EventBlockStart, BlockIndex: 1, BlockKind: ir.BlockServerToolUse, ToolCallID: "toolu_01", ToolName: "bash_tool"},
		{Kind: ir.EventBlockDelta, BlockIndex: 1, BlockKind: ir.BlockServerToolUse, ArgsDelta: `{"command":"ls"}`},
		{Kind: ir.EventBlockStop, BlockIndex: 1, BlockKind: ir.BlockServerToolUse},
		// 上游 3 = tool_result（整块丢弃；直喂 delta 验证容忍面——claude_web
		// 解析器对指向 tool_result 的 delta 本就丢弃）。
		{Kind: ir.EventBlockStart, BlockIndex: 2, BlockKind: ir.BlockToolResult, ToolCallID: "toolu_01", ToolName: "bash_tool"},
		{Kind: ir.EventBlockDelta, BlockIndex: 2, BlockKind: ir.BlockToolResult, ArgsDelta: `{"stdout":"x"}`},
		{Kind: ir.EventBlockStop, BlockIndex: 2, BlockKind: ir.BlockToolResult},
		// 上游 3（压到下游 2）：tool_result 丢弃后 text 块 index 前移。
		{Kind: ir.EventBlockStart, BlockIndex: 3, BlockKind: ir.BlockText},
		{Kind: ir.EventBlockDelta, BlockIndex: 3, BlockKind: ir.BlockText, TextDelta: "done"},
		{Kind: ir.EventBlockStop, BlockIndex: 3, BlockKind: ir.BlockText},
		{Kind: ir.EventMessageDelta, StopReason: ir.StopStop},
		{Kind: ir.EventMessageStop},
		{Kind: ir.EventDone, Reason: ir.StopStop},
	}
	out, err := RenderStream(events)
	if err != nil {
		t.Fatalf("RenderStream: %v（服务端工具块不得断流）", err)
	}
	text := string(out)
	if strings.Contains(text, "tool_result") {
		t.Fatalf("tool_result 块不得出现在出口流:\n%s", text)
	}
	frames := claudeFrames(t, out)
	var starts []int
	for _, f := range frames {
		if f.event != "content_block_start" {
			continue
		}
		var p struct {
			Index        int `json:"index"`
			ContentBlock struct {
				Type string `json:"type"`
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"content_block"`
		}
		if err := json.Unmarshal([]byte(f.data), &p); err != nil {
			t.Fatalf("frame data: %v\n%s", err, f.data)
		}
		if p.ContentBlock.Type == "tool_result" {
			t.Fatalf("tool_result 块 start 不得产出: %s", f.data)
		}
		starts = append(starts, p.Index)
		if p.ContentBlock.Type == "server_tool_use" {
			if p.ContentBlock.ID != "toolu_01" || p.ContentBlock.Name != "bash_tool" {
				t.Fatalf("server_tool_use 块 = %+v", p.ContentBlock)
			}
			if !strings.Contains(f.data, `"input":{}`) {
				t.Fatalf("input 占位必须为空对象（B6）: %s", f.data)
			}
		}
	}
	if len(starts) != 3 || starts[0] != 0 || starts[1] != 1 || starts[2] != 2 {
		t.Fatalf("下游块 index = %v, want [0 1 2]（丢弃不留空洞）", starts)
	}
	// 每个 delta / stop 的 index 必须指向已 start 的块（B5；丢弃块的 index 不出现）。
	for _, f := range frames {
		if f.event != "content_block_delta" && f.event != "content_block_stop" {
			continue
		}
		var p struct {
			Index int `json:"index"`
		}
		if err := json.Unmarshal([]byte(f.data), &p); err != nil {
			t.Fatalf("frame data: %v\n%s", err, f.data)
		}
		if p.Index < 0 || p.Index > 2 {
			t.Fatalf("%s index = %d 越界（下游块 0..2）: %s", f.event, p.Index, f.data)
		}
	}
	if !strings.Contains(text, `"partial_json":"{\"command\":\"ls\"}"`) {
		t.Fatalf("server_tool_use 参数须走 input_json_delta:\n%.600s", text)
	}
	if !strings.Contains(text, "event: message_stop") {
		t.Fatalf("缺 message_stop（B8）")
	}
}

func TestRenderStreamErrorFramePreservesType(t *testing.T) {
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "m", Model: "x"},
		{Kind: ir.EventError, ErrorReason: ir.ErrorUpstream,
			Error: &ir.Error{Type: "overloaded_error", Message: "busy", Source: "anthropic"}},
	}
	out, err := RenderStream(events)
	if err != nil {
		t.Fatalf("RenderStream: %v", err)
	}
	frames := claudeFrames(t, out)
	if len(frames) != 2 || frames[1].event != "error" {
		t.Fatalf("frames = %+v", frames)
	}
	if !strings.Contains(frames[1].data, `"type":"overloaded_error"`) {
		t.Fatalf("error type literal lost: %s", frames[1].data)
	}
}
