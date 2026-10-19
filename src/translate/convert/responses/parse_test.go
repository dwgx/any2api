package responses

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// ---------------------------------------------------------------------------
// 请求解析
// ---------------------------------------------------------------------------

func TestParseRequestStatefulFieldsEach(t *testing.T) {
	cases := map[string]string{
		"previous_response_id": `{"model":"m","input":"hi","previous_response_id":"resp_1"}`,
		"conversation":         `{"model":"m","input":"hi","conversation":{"id":"c1"}}`,
		"prompt":               `{"model":"m","input":"hi","prompt":{"id":"p1"}}`,
		"context_management":   `{"model":"m","input":"hi","context_management":[{"type":"x"}]}`,
		"background":           `{"model":"m","input":"hi","background":true}`,
		"moderation":           `{"model":"m","input":"hi","moderation":{"model":"omni"}}`,
	}
	for field, body := range cases {
		t.Run(field, func(t *testing.T) {
			_, err := ParseRequest([]byte(body))
			if !errors.Is(err, ErrStatefulFields) {
				t.Fatalf("err = %v, want ErrStatefulFields", err)
			}
			if !strings.Contains(err.Error(), field) {
				t.Fatalf("err must name %q: %v", field, err)
			}
		})
	}
	// null/缺省不报错（字段缺省≠null）。
	if _, err := ParseRequest([]byte(`{"model":"m","input":"hi","previous_response_id":null,"store":null}`)); err != nil {
		t.Fatalf("null stateful fields must be tolerated: %v", err)
	}
}

func TestParseRequestRejectsUnsupportedShapes(t *testing.T) {
	cases := []struct {
		name string
		body string
		want error
	}{
		{"hosted tool", `{"model":"m","input":"hi","tools":[{"type":"web_search"}]}`, ErrUnsupportedToolType},
		{"tool_choice allowed_tools", `{"model":"m","input":"hi","tool_choice":{"type":"allowed_tools","mode":"auto","tools":[]}}`, ErrUnsupportedToolChoice},
		{"tool_choice mcp", `{"model":"m","input":"hi","tool_choice":{"type":"mcp"}}`, ErrUnsupportedToolChoice},
		{"item_reference", `{"model":"m","input":[{"type":"item_reference","id":"msg_1"}]}`, ErrUnsupportedInputItem},
		{"unknown item", `{"model":"m","input":[{"type":"local_shell_call","id":"x"}]}`, ErrUnsupportedInputItem},
		{"no model", `{"input":"hi"}`, ErrBadRequest},
		{"no input", `{"model":"m"}`, ErrBadRequest},
		{"input object", `{"model":"m","input":{"role":"user"}}`, ErrBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseRequest([]byte(tc.body))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestParseRequestCustomToolAndOrphanOutput(t *testing.T) {
	body := []byte(`{
		"model": "m",
		"input": [
			{"type":"custom_tool_call","call_id":"call_c1","name":"apply_patch","input":"*** Begin Patch\n*** End Patch"},
			{"type":"custom_tool_call_output","call_id":"call_c1","output":"done"},
			{"type":"function_call_output","call_id":"call_orphan","output":"stray"},
			{"type":"function_call_output","output":"no call id"}
		]
	}`)
	req, err := ParseRequest(body)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	// custom_tool_call → tool_use，参数单键包装 {"input": <raw>}（表 1 行）。
	tu := req.Messages[0]
	if tu.Role != ir.RoleAssistant || tu.Blocks[0].Kind != ir.BlockToolUse {
		t.Fatalf("custom call = %+v", tu)
	}
	assertJSONEqual(t, []byte(`{"input":"*** Begin Patch\n*** End Patch"}`), []byte(tu.Blocks[0].ToolCall.Arguments))
	// custom_tool_call_output 配到 call_id → tool 消息。
	if req.Messages[1].Role != ir.RoleTool || req.Messages[1].Blocks[0].ToolResult.CallID != "call_c1" {
		t.Fatalf("custom output = %+v", req.Messages[1])
	}
	// 孤儿输出（无对应 function_call / 无 call_id）→ user 文本，绝不变 tool 消息（CPA 路线）。
	if req.Messages[2].Role != ir.RoleUser || req.Messages[2].Blocks[0].Text != "stray" {
		t.Fatalf("orphan output = %+v", req.Messages[2])
	}
	if req.Messages[3].Role != ir.RoleUser || req.Messages[3].Blocks[0].Text != "no call id" {
		t.Fatalf("orphan output without call id = %+v", req.Messages[3])
	}
}

func TestParseRequestToolOutputMediaHoisting(t *testing.T) {
	body := []byte(`{
		"model": "m",
		"input": [
			{"type":"function_call","call_id":"call_a","name":"snap","arguments":"{}"},
			{"type":"function_call_output","call_id":"call_a","output":[
				{"type":"input_text","text":"photo attached"},
				{"type":"input_image","image_url":"https://example.com/a.png","detail":"high"}
			]}
		]
	}`)
	req, err := ParseRequest(body)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if len(req.Messages) != 3 {
		t.Fatalf("messages = %+v", req.Messages)
	}
	// 文本留 tool 消息，媒体提升为后续 user 消息（表 1 行；tool 消息连续性）。
	if req.Messages[1].Role != ir.RoleTool || req.Messages[1].Blocks[0].ToolResult.Content != "photo attached" {
		t.Fatalf("tool message = %+v", req.Messages[1])
	}
	media := req.Messages[2]
	if media.Role != ir.RoleUser || len(media.Blocks) != 1 || media.Blocks[0].Kind != ir.BlockImage {
		t.Fatalf("hoisted media = %+v", media)
	}
	if media.Blocks[0].Image.URL != "https://example.com/a.png" || media.Blocks[0].Image.Detail != "high" {
		t.Fatalf("image = %+v", media.Blocks[0].Image)
	}
}

func TestParseRequestAdditionalToolsMerge(t *testing.T) {
	body := []byte(`{
		"model": "m",
		"input": [
			{"role":"user","content":"hi"},
			{"type":"additional_tools","tools":[{"type":"function","name":"extra_tool","parameters":{"type":"object"}}]}
		],
		"tools": [{"type":"function","name":"top_tool","parameters":{"type":"object"}}]
	}`)
	req, err := ParseRequest(body)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	// CPA quirk：input[].type=additional_tools 与顶层 tools 合并扫描（表 1 行）。
	if len(req.Tools) != 2 || req.Tools[0].Name != "top_tool" || req.Tools[1].Name != "extra_tool" {
		t.Fatalf("tools = %+v", req.Tools)
	}
}

func TestParseRequestTextFormatAndReasoning(t *testing.T) {
	body := []byte(`{
		"model": "m",
		"input": "hi",
		"max_output_tokens": 8,
		"text": {"format": {"type": "json_object"}},
		"reasoning": {"effort": "high", "summary": "auto"}
	}`)
	req, err := ParseRequest(body)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if string(req.Extra["response_format"]) != `{"type":"json_object"}` {
		t.Fatalf("response_format = %s", req.Extra["response_format"])
	}
	if string(req.Extra["reasoning_effort"]) != `"high"` {
		t.Fatalf("reasoning_effort = %s", req.Extra["reasoning_effort"])
	}
	// max_output_tokens <16 照转（官方拒绝由上游裁决；new-api 同款透传——表 quirk 7）。
	if string(req.Extra["max_completion_tokens"]) != "8" {
		t.Fatalf("max_completion_tokens = %s", req.Extra["max_completion_tokens"])
	}
	// responses 独有键不进 Extra（include/truncation/max_tool_calls/text/reasoning）。
	for _, k := range []string{"include", "truncation", "max_tool_calls", "text", "reasoning", "max_output_tokens"} {
		if _, ok := req.Extra[k]; ok {
			t.Fatalf("Extra[%q] must be dropped: %+v", k, req.Extra)
		}
	}
}

func TestParseRequestImageDataURL(t *testing.T) {
	body := []byte(`{"model":"m","input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}]}`)
	req, err := ParseRequest(body)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	img := req.Messages[0].Blocks[0].Image
	if img == nil || img.Kind != "inline" || img.MediaType != "image/png" || img.Data != "AAAA" {
		t.Fatalf("image = %+v", img)
	}
}

// ---------------------------------------------------------------------------
// 响应解析
// ---------------------------------------------------------------------------

func TestParseResponseHostedItemsPreserved(t *testing.T) {
	body := []byte(`{
		"id":"resp_1","object":"response","created_at":1,"status":"completed","model":"m",
		"output":[
			{"type":"web_search_call","id":"ws_1","status":"completed"},
			{"type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"hi","annotations":[],"logprobs":[]}]}
		],
		"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}
	}`)
	resp, err := ParseResponse(body)
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	// hosted 工具项：无 IR 对应 → BlockUnknown + Raw 保真（不静默丢）。
	if resp.Message.Blocks[0].Kind != ir.BlockUnknown || !strings.Contains(string(resp.Message.Blocks[0].Raw), `"web_search_call"`) {
		t.Fatalf("hosted item = %+v", resp.Message.Blocks[0])
	}
	if resp.Message.Blocks[1].Kind != ir.BlockText || resp.Message.Blocks[1].Text != "hi" {
		t.Fatalf("message block = %+v", resp.Message.Blocks[1])
	}
}

func TestParseResponseRefusalAndContentFilter(t *testing.T) {
	body := []byte(`{
		"id":"resp_2","object":"response","created_at":1,"status":"incomplete","model":"m",
		"incomplete_details":{"reason":"content_filter"},
		"output":[{"type":"message","status":"incomplete","role":"assistant","content":[{"type":"refusal","refusal":"I cannot help."}]}],
		"usage":null
	}`)
	resp, err := ParseResponse(body)
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if resp.StopReason != ir.StopError {
		t.Fatalf("stop = %q", resp.StopReason)
	}
	if resp.Message.Blocks[0].Kind != ir.BlockRefusal || resp.Message.Blocks[0].Text != "I cannot help." {
		t.Fatalf("refusal = %+v", resp.Message.Blocks[0])
	}
	if resp.Usage != nil {
		t.Fatalf("usage = %+v, want nil", resp.Usage)
	}
}

func TestParseResponseRedactedCarrier(t *testing.T) {
	body := []byte(`{
		"id":"resp_3","object":"response","created_at":1,"status":"completed","model":"m",
		"output":[{"type":"reasoning","status":"completed","summary":[],"encrypted_content":"claude-redacted-thinking:QUJD"}]
	}`)
	resp, err := ParseResponse(body)
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	b := resp.Message.Blocks[0]
	if b.Kind != ir.BlockRedactedThinking || b.Text != "QUJD" {
		t.Fatalf("redacted block = %+v", b)
	}
	// 回写：前缀载体原样（CPA 约定）。
	out, err := RenderResponse(resp)
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	if !strings.Contains(string(out), `"encrypted_content":"claude-redacted-thinking:QUJD"`) {
		t.Fatalf("carrier lost: %s", out)
	}
}

// ---------------------------------------------------------------------------
// 流式解析
// ---------------------------------------------------------------------------

func TestParseStreamSequenceNumberOptional(t *testing.T) {
	// sequence_number 缺省：消费端不能把它当必填（表 §4.1）。
	stream := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","created_at":7,"model":"m","status":"in_progress"}}`,
		"",
		`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hi"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_1","created_at":7,"model":"m","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hi"}]}],"usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}}`,
		"",
	}, "\n")
	events, err := ParseStream(strings.NewReader(stream))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	if txt := collectText(events); txt != "hi" {
		t.Fatalf("text = %q", txt)
	}
	u := lastUsage(events)
	if u == nil || u.Input != 3 || u.Output != 1 || u.Total != 4 {
		t.Fatalf("usage = %+v", u)
	}
}

func TestParseStreamDoneSentinelAndAlias(t *testing.T) {
	// `[DONE]` 非官方哨兵：容忍（new-api 扫描器同款）。
	stream := "data: [DONE]\n\n"
	events, err := ParseStream(strings.NewReader(stream))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	if len(events) != 1 || events[0].Kind != ir.EventError || events[0].ErrorReason != ir.ErrorTransport {
		t.Fatalf("events = %+v (哨兵后无终止事件 = 截断)", events)
	}
	// response.done 是非官方别名，与 completed 同权。
	stream = strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","created_at":7,"model":"m","status":"in_progress"}}`,
		"",
		`data: {"type":"response.done","response":{"id":"resp_1","created_at":7,"model":"m","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}}`,
		"",
		`data: [DONE]`,
		"",
	}, "\n")
	events, err = ParseStream(strings.NewReader(stream))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	if r, ok := doneReason(events); !ok || r != ir.StopStop {
		t.Fatalf("done = %q (%v): %+v", r, ok, events)
	}
	if txt := collectText(events); txt != "ok" {
		t.Fatalf("text = %q", txt)
	}
}

func TestParseStreamUsageMergeNonZero(t *testing.T) {
	// usage 只出现在终止事件；多个携带 response 的事件做 merge-non-zero（NA 同款）。
	stream := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","created_at":7,"model":"m","status":"in_progress","usage":null}}`,
		"",
		`data: {"type":"response.in_progress","response":{"id":"resp_1","status":"in_progress","usage":{"input_tokens":9,"output_tokens":0,"total_tokens":0}}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_1","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"x"}]}],"usage":{"input_tokens":0,"output_tokens":4,"total_tokens":13}}}`,
		"",
	}, "\n")
	events, err := ParseStream(strings.NewReader(stream))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	u := lastUsage(events)
	if u == nil || u.Input != 9 || u.Output != 4 || u.Total != 13 {
		t.Fatalf("usage = %+v", u)
	}
}

func TestParseStreamTruncatedIsTransportError(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","created_at":7,"model":"m","status":"in_progress"}}`,
		"",
		`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"partial"}`,
		"",
	}, "\n")
	events, err := ParseStream(strings.NewReader(stream))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	last := events[len(events)-1]
	if last.Kind != ir.EventError || last.ErrorReason != ir.ErrorTransport {
		t.Fatalf("last = %+v", last)
	}
	// 截断流不发 block_stop（半 JSON/半文本不闭块）。
	for _, ev := range events {
		if ev.Kind == ir.EventBlockStop {
			t.Fatalf("truncated stream must not close blocks: %+v", events)
		}
	}
}

func TestParseStreamFailedAndFlatError(t *testing.T) {
	// response.failed 的 response.error 优先（表 §4.5）。
	stream := `data: {"type":"response.failed","response":{"id":"resp_1","status":"failed","error":{"code":"server_error","message":"boom"}}}`
	events, err := ParseStream(strings.NewReader(stream + "\n\n"))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	if len(events) != 1 || events[0].Kind != ir.EventError || events[0].ErrorReason != ir.ErrorUpstream {
		t.Fatalf("events = %+v", events)
	}
	if events[0].Error.Code != "server_error" || events[0].Error.Message != "boom" {
		t.Fatalf("error = %+v", events[0].Error)
	}
	// 平铺 error 事件（无嵌套 error 对象）。
	stream = `data: {"type":"error","code":"rate_limit_exceeded","message":"slow down","param":null}`
	events, err = ParseStream(strings.NewReader(stream + "\n\n"))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	if len(events) != 1 || events[0].Error.Code != "rate_limit_exceeded" || events[0].Error.Message != "slow down" {
		t.Fatalf("error event = %+v", events[0])
	}
}

func TestParseStreamUnknownEventIgnoredAndSummaryBreak(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","created_at":7,"model":"m","status":"in_progress"}}`,
		"",
		`data: {"type":"response.queued","response":{"id":"resp_1","status":"queued"}}`,
		"",
		`data: {"type":"response.reasoning_summary_part.added","output_index":0,"summary_index":0,"part":{"type":"summary_text","text":""}}`,
		"",
		`data: {"type":"response.reasoning_summary_text.delta","output_index":0,"summary_index":0,"delta":"first"}`,
		"",
		`data: {"type":"response.reasoning_summary_text.delta","output_index":0,"summary_index":1,"delta":"second"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_1","status":"completed","output":[{"type":"reasoning","summary":[{"type":"summary_text","text":"first\n\nsecond"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
		"",
	}, "\n")
	events, err := ParseStream(strings.NewReader(stream))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	// 未知事件（response.queued）静默忽略；summary 分段补 "\n\n"（NA 断段规则）。
	if got := collectThinking(events); got != "first\n\nsecond" {
		t.Fatalf("thinking = %q", got)
	}
}

func TestParseErrorVariants(t *testing.T) {
	// 非 JSON body → 泛型 api_error（Raw 保留诊断）。
	e := ParseError([]byte("<html>bad gateway</html>"))
	if e.Type != "api_error" || e.Source != "openai" || len(e.Raw) == 0 {
		t.Fatalf("error = %+v", e)
	}
	// HTTP 信封（与 chat 相同）。
	e = ParseError([]byte(`{"error":{"message":"nope","type":"invalid_request_error","param":"input","code":"invalid_prompt"}}`))
	if e.Message != "nope" || e.Type != "invalid_request_error" || e.Param != "input" || e.Code != "invalid_prompt" {
		t.Fatalf("error = %+v", e)
	}
	// 终止失败事件（response.failed）。
	e = ParseError([]byte(`{"type":"response.failed","response":{"status":"failed","error":{"code":"server_error","message":"upstream exploded"}}}`))
	if e.Code != "server_error" || e.Message != "upstream exploded" {
		t.Fatalf("error = %+v", e)
	}
}

// ---------------------------------------------------------------------------
// 流式渲染
// ---------------------------------------------------------------------------

func TestStreamWriterSequenceAndNoSentinel(t *testing.T) {
	w := NewStreamWriter()
	var buf bytes.Buffer
	write := func(ev ir.StreamEvent) {
		t.Helper()
		b, err := w.Write(ev)
		if err != nil {
			t.Fatalf("Write %s: %v", ev.Kind, err)
		}
		buf.Write(b)
	}
	write(ir.StreamEvent{Kind: ir.EventStart, MessageID: "resp_1", Model: "m", Created: 5})
	write(ir.StreamEvent{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockText})
	write(ir.StreamEvent{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockText, TextDelta: "hi"})
	write(ir.StreamEvent{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockText})
	write(ir.StreamEvent{Kind: ir.EventMessageDelta, StopReason: ir.StopStop, Usage: &ir.Usage{Input: 2, Output: 1, Total: 3}})
	write(ir.StreamEvent{Kind: ir.EventMessageStop})
	write(ir.StreamEvent{Kind: ir.EventDone, Reason: ir.StopStop})
	tail, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	buf.Write(tail)
	out := buf.String()
	if strings.Contains(out, "[DONE]") {
		t.Fatalf("responses 出口绝不发哨兵: %s", out)
	}
	frames := sseEventTypes(t, []byte(out))
	want := []string{
		"response.created", "response.in_progress",
		"response.output_item.added", "response.content_part.added",
		"response.output_text.delta", "response.output_text.done",
		"response.content_part.done", "response.output_item.done",
		"response.completed",
	}
	if !reflect.DeepEqual(frames, want) {
		t.Fatalf("frames = %+v, want %+v", frames, want)
	}
	// sequence_number 从 0 逐事件递增（合成端的显式选择）。
	for i, raw := range sseFrames(t, []byte(out)) {
		var w struct {
			SequenceNumber *int64 `json:"sequence_number"`
		}
		if err := json.Unmarshal([]byte(raw), &w); err != nil || w.SequenceNumber == nil || *w.SequenceNumber != int64(i) {
			t.Fatalf("frame %d seq = %v (%v)", i, w.SequenceNumber, err)
		}
	}
	// usage 只在终止事件里。
	last := sseFrames(t, []byte(out))[len(frames)-1]
	if !strings.Contains(last, `"usage":{"input_tokens":2`) {
		t.Fatalf("usage missing in terminal: %s", last)
	}
}

func TestStreamWriterRejectsBadSequences(t *testing.T) {
	// 未 start 先块：显式报错。
	w := NewStreamWriter()
	if _, err := w.Write(ir.StreamEvent{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockText}); !errors.Is(err, ErrMissingStart) {
		t.Fatalf("err = %v", err)
	}
	// 无 stop_reason 的 EOF：不合成默认值（掩盖截断）。
	w = NewStreamWriter()
	if _, err := w.Write(ir.StreamEvent{Kind: ir.EventStart, MessageID: "resp_1"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := w.Finish(); !errors.Is(err, ErrMissingStopReason) {
		t.Fatalf("err = %v, want ErrMissingStopReason", err)
	}
	// 终止后事件：显式报错。
	w = NewStreamWriter()
	if _, err := w.Write(ir.StreamEvent{Kind: ir.EventStart, MessageID: "resp_1"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := w.Write(ir.StreamEvent{Kind: ir.EventMessageDelta, StopReason: ir.StopStop}); err != nil {
		t.Fatalf("delta: %v", err)
	}
	if _, err := w.Write(ir.StreamEvent{Kind: ir.EventDone, Reason: ir.StopStop}); err != nil {
		t.Fatalf("done: %v", err)
	}
	if _, err := w.Write(ir.StreamEvent{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockText}); !errors.Is(err, ErrEventsAfterTerminal) {
		t.Fatalf("err = %v, want ErrEventsAfterTerminal", err)
	}
}

func TestStreamWriterErrorFrames(t *testing.T) {
	w := NewStreamWriter()
	var buf bytes.Buffer
	for _, ev := range []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "resp_1", Model: "m"},
		{Kind: ir.EventError, ErrorReason: ir.ErrorUpstream, Error: &ir.Error{Code: "server_error", Message: "boom", Source: "openai"}},
	} {
		b, err := w.Write(ev)
		if err != nil {
			t.Fatalf("Write %s: %v", ev.Kind, err)
		}
		buf.Write(b)
	}
	out := buf.String()
	// 平铺 error 事件（无嵌套 error 对象）+ response.failed（表 §4.5）。
	frames := sseEventTypes(t, []byte(out))
	if len(frames) != 4 || frames[2] != "error" || frames[3] != "response.failed" {
		t.Fatalf("frames = %+v", frames)
	}
	if !strings.Contains(out, `"message":"boom"`) || strings.Contains(out, `"error":{"message"`) {
		t.Fatalf("flat error event expected: %s", out)
	}
	if !strings.Contains(out, `"status":"failed"`) || !strings.Contains(out, `"error":{"code":"server_error","message":"boom"}`) {
		t.Fatalf("response.failed payload: %s", out)
	}
	// 已开 SSE 后绝不写 JSON HTTP 错误体（无 `{"error":{` 顶层信封）。
	if strings.Contains(out, "data: {\"error\":{") {
		t.Fatalf("HTTP error body leaked into stream: %s", out)
	}
}

func TestStreamWriterIncompleteAndLength(t *testing.T) {
	w := NewStreamWriter()
	var buf bytes.Buffer
	for _, ev := range []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "resp_1"},
		{Kind: ir.EventMessageDelta, StopReason: ir.StopLength},
		{Kind: ir.EventDone, Reason: ir.StopLength},
	} {
		b, err := w.Write(ev)
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
		buf.Write(b)
	}
	out := buf.String()
	if !strings.Contains(out, `"status":"incomplete"`) || !strings.Contains(out, `"reason":"max_output_tokens"`) {
		t.Fatalf("incomplete shape: %s", out)
	}
	frames := sseEventTypes(t, []byte(out))
	if frames[len(frames)-1] != "response.incomplete" {
		t.Fatalf("frames = %+v", frames)
	}
}

func TestRenderStreamRoundTripRedacted(t *testing.T) {
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "resp_1", Model: "m", Created: 9},
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockRedactedThinking},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockRedactedThinking, SignatureDelta: "QUJD"},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockRedactedThinking},
		{Kind: ir.EventMessageDelta, StopReason: ir.StopStop},
		{Kind: ir.EventMessageStop},
		{Kind: ir.EventDone, Reason: ir.StopStop},
	}
	out, err := RenderStream(events)
	if err != nil {
		t.Fatalf("RenderStream: %v", err)
	}
	if !strings.Contains(string(out), redactedThinkingPrefix+"QUJD") {
		t.Fatalf("prefix carrier missing: %s", out)
	}
	back, err := ParseStream(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	if !reflect.DeepEqual(events, back) {
		t.Fatalf("round trip mismatch:\nfirst:  %+v\nsecond: %+v", events, back)
	}
}
