package claude_messages

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

func TestParseRequestSystemAndToolResultSplitting(t *testing.T) {
	in := []byte(`{
		"model": "m",
		"max_tokens": 10,
		"system": "sys",
		"messages": [
			{"role": "user", "content": [
				{"type": "text", "text": "A"},
				{"type": "tool_result", "tool_use_id": "t1", "content": "R1"},
				{"type": "text", "text": "B"}
			]}
		]
	}`)
	req, err := ParseRequest(in)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if len(req.Messages) != 4 {
		t.Fatalf("messages = %d, want 4 (system + user/tool/user 拆分)", len(req.Messages))
	}
	if req.Messages[0].Role != ir.RoleSystem || req.Messages[0].Blocks[0].Text != "sys" {
		t.Fatalf("m0 = %+v", req.Messages[0])
	}
	if req.Messages[1].Role != ir.RoleUser || req.Messages[1].Blocks[0].Text != "A" {
		t.Fatalf("m1 = %+v", req.Messages[1])
	}
	if req.Messages[2].Role != ir.RoleTool || req.Messages[2].Blocks[0].ToolResult.CallID != "t1" ||
		req.Messages[2].Blocks[0].ToolResult.Content != "R1" {
		t.Fatalf("m2 = %+v", req.Messages[2])
	}
	if req.Messages[3].Role != ir.RoleUser || req.Messages[3].Blocks[0].Text != "B" {
		t.Fatalf("m3 = %+v", req.Messages[3])
	}
	// system 原文保真（Extra["system"]）。
	if string(req.Extra["system"]) != `"sys"` {
		t.Fatalf("extra system = %s", req.Extra["system"])
	}
}

func TestParseRequestToolChoiceCanonical(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantPar bool // parallel_tool_calls=false 出现
	}{
		{`{"type":"auto"}`, `"auto"`, false},
		{`{"type":"none"}`, `"none"`, false},
		{`{"type":"any"}`, `"required"`, false},
		{`{"type":"any","disable_parallel_tool_use":true}`, `"required"`, true},
		{`{"type":"tool","name":"x"}`, `{"type":"function","function":{"name":"x"}}`, false},
	}
	for _, tc := range cases {
		in := []byte(`{"model":"m","max_tokens":1,"tool_choice":` + tc.in + `,"messages":[{"role":"user","content":"hi"}]}`)
		req, err := ParseRequest(in)
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if got := normalizeJSON(t, req.ToolChoice); got != normalizeJSON(t, []byte(tc.want)) {
			t.Fatalf("%s: tool_choice = %s, want %s", tc.in, got, tc.want)
		}
		_, hasPar := req.Extra["parallel_tool_calls"]
		if hasPar != tc.wantPar {
			t.Fatalf("%s: parallel_tool_calls present = %v, want %v", tc.in, hasPar, tc.wantPar)
		}
	}
}

func TestParseRequestExtrasMappingAndMetadataDrop(t *testing.T) {
	in := []byte(`{
		"model": "m", "max_tokens": 10,
		"stop_sequences": ["STOP"],
		"betas": ["context-1m-2025-08-07"],
		"thinking": {"type": "enabled", "budget_tokens": 1024},
		"metadata": {"user_id": "u1"},
		"messages": [{"role": "user", "content": "hi"}]
	}`)
	req, err := ParseRequest(in)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if string(req.Extra["stop"]) != `["STOP"]` {
		t.Fatalf("stop = %s", req.Extra["stop"])
	}
	if string(req.Extra["betas"]) != `["context-1m-2025-08-07"]` {
		t.Fatalf("betas = %s", req.Extra["betas"])
	}
	if _, ok := req.Extra["thinking"]; !ok {
		t.Fatalf("thinking not preserved")
	}
	if _, ok := req.Extra["metadata"]; ok {
		t.Fatalf("metadata should be dropped (shape mismatch), got %s", req.Extra["metadata"])
	}
}

func TestParseResponseStopReasonTable(t *testing.T) {
	cases := []struct {
		in   string
		want ir.StopReason
	}{
		{"end_turn", ir.StopStop},
		{"max_tokens", ir.StopLength},
		{"stop_sequence", ir.StopStop},
		{"tool_use", ir.StopToolUse},
		{"pause_turn", ir.StopLength},
		{"refusal", ir.StopError},
		{"model_context_window_exceeded", ir.StopLength},
		{"brand_new_reason", ir.StopReason("brand_new_reason")},
	}
	for _, tc := range cases {
		in := []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"m","content":[{"type":"text","text":"x"}],"stop_reason":"` + tc.in + `"}`)
		resp, err := ParseResponse(in)
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if resp.StopReason != tc.want {
			t.Fatalf("%s: stop = %q, want %q", tc.in, resp.StopReason, tc.want)
		}
	}
}

func TestParseResponseUsageCacheAndThinking(t *testing.T) {
	in := []byte(`{
		"id":"msg_1","type":"message","role":"assistant","model":"m","content":[],
		"stop_reason":"end_turn",
		"usage": {
			"input_tokens": 10, "output_tokens": 4,
			"cache_read_input_tokens": 3,
			"cache_creation_input_tokens": 7,
			"cache_creation": {"ephemeral_5m_input_tokens": 2, "ephemeral_1h_input_tokens": 5},
			"output_tokens_details": {"thinking_tokens": 6},
			"server_tool_use": {"web_search_requests": 1}
		}
	}`)
	resp, err := ParseResponse(in)
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	u := resp.Usage
	if u == nil || u.Input != 10 || u.Output != 4 || u.CacheRead != 3 ||
		u.CacheCreation == nil || u.CacheCreation.FiveMinute != 2 || u.CacheCreation.OneHour != 5 ||
		u.Reasoning != 6 || u.Total != 10+7+3+4 || u.Accounting != "anthropic" {
		t.Fatalf("usage = %+v", u)
	}
	if _, ok := u.Extra["server_tool_use"]; !ok {
		t.Fatalf("server_tool_use not preserved in Extra")
	}
}

func TestParseErrorEnvelope(t *testing.T) {
	e := ParseError([]byte(`{"type":"error","error":{"type":"overloaded_error","message":"busy"},"request_id":"r1"}`))
	if e.Type != "overloaded_error" || e.Message != "busy" || e.Source != "anthropic" {
		t.Fatalf("error = %+v", e)
	}
	// 非 JSON body：泛型 api_error + Raw 保真。
	raw := []byte("gateway timeout")
	e2 := ParseError(raw)
	if e2.Type != "api_error" || e2.Source != "anthropic" || !bytes.Equal(e2.Raw, raw) {
		t.Fatalf("error = %+v", e2)
	}
}

func TestStreamParserOrderViolations(t *testing.T) {
	p := NewStreamParser()
	if _, err := p.Feed("content_block_delta", []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x"}}`)); !errors.Is(err, ErrUnexpectedEventOrder) {
		t.Fatalf("delta before start err = %v", err)
	}
	if _, err := p.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m","model":"x","usage":{"input_tokens":1,"output_tokens":0}}}`)); err != nil {
		t.Fatalf("message_start: %v", err)
	}
	if _, err := p.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m"}}`)); !errors.Is(err, ErrUnexpectedEventOrder) {
		t.Fatalf("duplicate message_start err = %v", err)
	}
	if _, err := p.Feed("content_block_delta", []byte(`{"type":"content_block_delta","index":5,"delta":{"type":"text_delta","text":"x"}}`)); !errors.Is(err, ErrUnknownBlockIndex) {
		t.Fatalf("unknown index err = %v", err)
	}
	if _, err := p.Feed("content_block_start", []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)); err != nil {
		t.Fatalf("block start: %v", err)
	}
	if _, err := p.Feed("content_block_start", []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)); !errors.Is(err, ErrBadEvent) {
		t.Fatalf("duplicate index err = %v", err)
	}
}

func TestStreamParserMessageStopWithoutStopReasonIsFailure(t *testing.T) {
	p := NewStreamParser()
	if _, err := p.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m","model":"x"}}`)); err != nil {
		t.Fatalf("message_start: %v", err)
	}
	events, err := p.Feed("message_stop", []byte(`{"type":"message_stop"}`))
	if err != nil {
		t.Fatalf("message_stop: %v", err)
	}
	if len(events) != 1 || events[0].Kind != ir.EventError || events[0].ErrorReason != ir.ErrorTransport {
		t.Fatalf("events = %+v, want transport error", events)
	}
	if _, err := p.Feed("message_stop", []byte(`{"type":"message_stop"}`)); !errors.Is(err, ErrDataAfterTerminal) {
		t.Fatalf("data after terminal err = %v", err)
	}
}

func TestStreamParserUnknownEventsAndPingIgnored(t *testing.T) {
	p := NewStreamParser()
	if _, err := p.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m","model":"x","usage":{"input_tokens":2,"output_tokens":0}}}`)); err != nil {
		t.Fatalf("message_start: %v", err)
	}
	for _, tc := range []struct{ name, data string }{
		{"ping", `{"type":"ping"}`},
		{"completion", `{"type":"completion"}`},
		{"brand_new_event", `{"type":"brand_new_event","x":1}`},
	} {
		events, err := p.Feed(tc.name, []byte(tc.data))
		if err != nil || len(events) != 0 {
			t.Fatalf("%s: events=%+v err=%v", tc.name, events, err)
		}
	}
	if _, err := p.Feed("content_block_start", []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)); err != nil {
		t.Fatalf("block start: %v", err)
	}
	events, err := p.Feed("content_block_delta", []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"citations_delta","citation":{"x":1}}}`))
	if err != nil || len(events) != 0 {
		t.Fatalf("citations_delta: events=%+v err=%v", events, err)
	}
}

func TestStreamParserTruncation(t *testing.T) {
	p := NewStreamParser()
	if _, err := p.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m","model":"x"}}`)); err != nil {
		t.Fatalf("message_start: %v", err)
	}
	events := p.Finish()
	if len(events) != 1 || events[0].Kind != ir.EventError || events[0].ErrorReason != ir.ErrorTransport {
		t.Fatalf("finish = %+v", events)
	}
}

func TestStreamParserEventLineAbsentFallsBackToDataType(t *testing.T) {
	// 容忍丢弃 event: 行的代理：data.type 回退定位。
	in := strings.Join([]string{
		`data: {"type":"message_start","message":{"id":"m","model":"x","usage":{"input_tokens":1,"output_tokens":0}}}`,
		``,
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		``,
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}`,
		``,
		`data: {"type":"content_block_stop","index":0}`,
		``,
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`,
		``,
		`data: {"type":"message_stop"}`,
		``,
	}, "\n")
	events, err := ParseStream(strings.NewReader(in))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	if got := collectText(events); got != "hi" {
		t.Fatalf("text = %q", got)
	}
	if r, ok := doneReason(events); !ok || r != ir.StopStop {
		t.Fatalf("done reason = %q ok=%v", r, ok)
	}
}

// TestStreamParserStopSequencePassthrough W44-1（R1 跨包收口）：Anthropic 线
// message_delta 的 stop_sequence 进 IR 细节面（命中自定义停止序列时回填原文；
// null/缺失 = 未提供）。
func TestStreamParserStopSequencePassthrough(t *testing.T) {
	in := strings.Join([]string{
		`data: {"type":"message_start","message":{"id":"m","model":"x","usage":{"input_tokens":1,"output_tokens":0}}}`,
		``,
		`data: {"type":"message_delta","delta":{"stop_reason":"stop_sequence","stop_sequence":"END"},"usage":{"output_tokens":2}}`,
		``,
		`data: {"type":"message_stop"}`,
		``,
	}, "\n")
	events, err := ParseStream(strings.NewReader(in))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	var found bool
	for _, ev := range events {
		if ev.Kind == ir.EventMessageDelta && ev.StopSequence != "" {
			found = true
			if ev.StopReason != ir.StopStop || ev.StopSequence != "END" {
				t.Fatalf("message_delta = %+v", ev)
			}
		}
	}
	if !found {
		t.Fatalf("StopSequence 未进 IR：%+v", events)
	}
	// 常态（stop_sequence:null）→ 空（未提供）。
	in2 := strings.Join([]string{
		`data: {"type":"message_start","message":{"id":"m","model":"x"}}`,
		``,
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null}}`,
		``,
		`data: {"type":"message_stop"}`,
		``,
	}, "\n")
	events2, err := ParseStream(strings.NewReader(in2))
	if err != nil {
		t.Fatalf("ParseStream2: %v", err)
	}
	for _, ev := range events2 {
		if ev.Kind == ir.EventMessageDelta && ev.StopSequence != "" {
			t.Fatalf("null stop_sequence 应为空：%+v", ev)
		}
	}
}

// TestParseResponseStopSequenceBackfill W52-1（W44-1 §9 残留 1 非流式对齐）：
// 非流式 Message 的 stop_sequence 进 IR（命中自定义序列回填原文；null/缺失 =
// 未提供；非字符串显式报错——与流式面 message_delta 解析同款纪律）。
func TestParseResponseStopSequenceBackfill(t *testing.T) {
	in := []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"m",
		"content":[{"type":"text","text":"x"}],
		"stop_reason":"stop_sequence","stop_sequence":"END",
		"usage":{"input_tokens":1,"output_tokens":2}}`)
	resp, err := ParseResponse(in)
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if resp.StopSequence != "END" || resp.StopReason != ir.StopStop {
		t.Fatalf("StopSequence/StopReason = %q/%q, want END/stop", resp.StopSequence, resp.StopReason)
	}
	// 常态（stop_sequence:null）→ 空（未提供）。
	null := []byte(`{"id":"msg_2","type":"message","role":"assistant","model":"m",
		"content":[{"type":"text","text":"x"}],"stop_reason":"end_turn","stop_sequence":null}`)
	resp2, err := ParseResponse(null)
	if err != nil {
		t.Fatalf("ParseResponse(null): %v", err)
	}
	if resp2.StopSequence != "" {
		t.Fatalf("null stop_sequence 应为空：%q", resp2.StopSequence)
	}
	// 缺失 → 空。
	missing := []byte(`{"id":"msg_3","type":"message","role":"assistant","model":"m","content":[],"stop_reason":"end_turn"}`)
	resp3, err := ParseResponse(missing)
	if err != nil {
		t.Fatalf("ParseResponse(missing): %v", err)
	}
	if resp3.StopSequence != "" {
		t.Fatalf("缺失 stop_sequence 应为空：%q", resp3.StopSequence)
	}
	// 非字符串 → 显式报错（不静默）。
	bad := []byte(`{"id":"msg_4","type":"message","role":"assistant","model":"m","content":[],"stop_reason":"stop_sequence","stop_sequence":42}`)
	if _, err := ParseResponse(bad); !errors.Is(err, ErrBadResponse) {
		t.Fatalf("非字符串 stop_sequence err = %v, want ErrBadResponse", err)
	}
	// stop_details 经 Extra 保真旁路往返（W52-1 决策：不提升为 IR 字段——非流式
	// Message 契约含该字段，Extra 已可往返；提升会造成双表示。拒绝类细节面
	// `stop_details{type,category}` 同款）。
	details := []byte(`{"id":"msg_5","type":"message","role":"assistant","model":"m","content":[],
		"stop_reason":"refusal","stop_sequence":null,"stop_details":{"type":"refusal","category":"cyber"}}`)
	resp5, err := ParseResponse(details)
	if err != nil {
		t.Fatalf("ParseResponse(details): %v", err)
	}
	if resp5.StopSequence != "" || resp5.StopReason != ir.StopError {
		t.Fatalf("details 解析 = %q/%q, want \"\"/error", resp5.StopSequence, resp5.StopReason)
	}
	if raw := resp5.Extra["stop_details"]; len(raw) == 0 || !strings.Contains(string(raw), `"category":"cyber"`) {
		t.Fatalf("stop_details 未进 Extra 保真旁路：%v", resp5.Extra)
	}
	out, err := RenderResponse(resp5)
	if err != nil {
		t.Fatalf("RenderResponse(details): %v", err)
	}
	if !strings.Contains(string(out), `"stop_details":{"type":"refusal","category":"cyber"}`) {
		t.Fatalf("stop_details 未往返:\n%s", out)
	}
}

// normalizeJSON 归一化 JSON 文本用于比较。
func normalizeJSON(t *testing.T, raw []byte) string {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}
