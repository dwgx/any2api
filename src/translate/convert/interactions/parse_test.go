package interactions

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/hygiene"
	"github.com/dwgx/any2api/translate/ir"
)

func req(t *testing.T, body string) *ir.Request {
	t.Helper()
	r, err := ParseRequest([]byte(body))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	return r
}

// TestParseRequestServerToolsDropped 服务端工具声明（8 类判别式）**丢弃**、
// function 声明保留——与 gemini 转换器对内置工具的口径一致（渠道配置注入，
// IR 无对应；不发明字段）。丢弃面显式点名，不是静默旁路。
func TestParseRequestServerToolsDropped(t *testing.T) {
	r := req(t, `{"model":"m","tools":[
		{"type":"google_search","search_types":["web"]},
		{"type":"code_execution"},
		{"type":"function","name":"f","description":"d","parameters":{"type":"object"}}
	],"input":"hi"}`)
	if len(r.Tools) != 1 || r.Tools[0].Name != "f" {
		t.Fatalf("tools = %+v", r.Tools)
	}
}

// TestParseRequestFunctionCallCPAFallback Q1：CPA 把 Gemini 的 functionCall.id 写进
// interactions 的 `call_id`（do-not-copy）——**读取容忍**（id 缺失时回退 call_id），
// 渲染侧永远只写 `id`（TestGoldenInteractions/request_steps 锁死）。
func TestParseRequestFunctionCallCPAFallback(t *testing.T) {
	r := req(t, `{"model":"m","input":[
		{"type":"function_call","call_id":"call_x","name":"f","arguments":{}}
	]}`)
	tu := r.Messages[0].Blocks[0]
	if tu.Kind != ir.BlockToolUse || tu.ToolCall.ID != "call_x" {
		t.Fatalf("tool_use = %+v", tu)
	}
	// 两键都缺 → 显式报错（id 必填，OAS:FunctionCallStep.required）。
	if _, err := ParseRequest([]byte(`{"model":"m","input":[{"type":"function_call","name":"f"}]}`)); !errors.Is(err, ErrMissingFunctionID) {
		t.Fatalf("err = %v", err)
	}
}

// TestParseRequestInputShapes input 四态联合：string / 单 Content / Content[] / Step[]。
func TestParseRequestInputShapes(t *testing.T) {
	r := req(t, `{"model":"m","input":"hello"}`)
	if len(r.Messages) != 1 || r.Messages[0].Role != ir.RoleUser || r.Messages[0].Blocks[0].Text != "hello" {
		t.Fatalf("string input = %+v", r.Messages)
	}
	r = req(t, `{"model":"m","input":{"type":"text","text":"one part"}}`)
	if len(r.Messages) != 1 || r.Messages[0].Blocks[0].Text != "one part" {
		t.Fatalf("content input = %+v", r.Messages)
	}
	r = req(t, `{"model":"m","input":[{"type":"text","text":"a"},{"type":"image","mime_type":"image/png","data":"AAAA"}]}`)
	if len(r.Messages) != 1 || len(r.Messages[0].Blocks) != 2 ||
		r.Messages[0].Blocks[1].Kind != ir.BlockImage ||
		r.Messages[0].Blocks[1].Image.Data != "AAAA" || r.Messages[0].Blocks[1].Image.Kind != "inline" {
		t.Fatalf("content[] input = %+v", r.Messages)
	}
	r = req(t, `{"model":"m","input":[{"type":"user_input","content":[{"type":"text","text":"x"}]},{"type":"model_output","content":[{"type":"text","text":"y"}]}]}`)
	if len(r.Messages) != 2 || r.Messages[0].Role != ir.RoleUser || r.Messages[1].Role != ir.RoleAssistant {
		t.Fatalf("step[] input = %+v", r.Messages)
	}
}

// TestParseRequestMediaContent 媒体判别式（顶层 type + mime_type）：image 双态
// （data/uri）、audio data；video/document/只有 uri 的 audio 无 IR 载体 → Raw 保真。
func TestParseRequestMediaContent(t *testing.T) {
	r := req(t, `{"model":"m","input":[
		{"type":"image","mime_type":"image/png","uri":"https://x/y.png"},
		{"type":"audio","mime_type":"audio/wav","data":"BBBB"},
		{"type":"video","mime_type":"video/mp4","uri":"https://x/v.mp4"},
		{"type":"document","mime_type":"application/pdf","uri":"https://x/d.pdf"},
		{"type":"audio","mime_type":"audio/wav","uri":"https://x/a.wav"}
	]}`)
	blocks := r.Messages[0].Blocks
	if blocks[0].Kind != ir.BlockImage || blocks[0].Image.Kind != "file" || blocks[0].Image.URL != "https://x/y.png" {
		t.Fatalf("image uri = %+v", blocks[0])
	}
	if blocks[1].Kind != ir.BlockAudio || blocks[1].Audio.Data != "BBBB" {
		t.Fatalf("audio = %+v", blocks[1])
	}
	for i := 2; i < 5; i++ {
		if blocks[i].Kind != ir.BlockUnknown || len(blocks[i].Raw) == 0 {
			t.Fatalf("block %d = %+v", i, blocks[i])
		}
	}
	// image 既无 data 也无 uri → 显式报错。
	if _, err := ParseRequest([]byte(`{"model":"m","input":[{"type":"image","mime_type":"image/png"}]}`)); err == nil {
		t.Fatalf("image without payload must fail")
	}
}

// TestParseRequestFunctionResultMediaHoist result 部件数组里的媒体块提升为后续
// user 消息（tool 消息保持单 tool_result 块；responses 同款纪律）。
func TestParseRequestFunctionResultMediaHoist(t *testing.T) {
	r := req(t, `{"model":"m","input":[
		{"type":"function_result","call_id":"c1","name":"f","result":[{"type":"text","text":"ok"},{"type":"image","mime_type":"image/png","data":"AAAA"}]}
	]}`)
	if len(r.Messages) != 2 || r.Messages[0].Role != ir.RoleTool || r.Messages[1].Role != ir.RoleUser {
		t.Fatalf("messages = %+v", r.Messages)
	}
	if r.Messages[0].Blocks[0].ToolResult.Content != "ok" {
		t.Fatalf("tool_result = %+v", r.Messages[0].Blocks[0])
	}
	if r.Messages[1].Blocks[0].Kind != ir.BlockImage {
		t.Fatalf("hoisted = %+v", r.Messages[1].Blocks)
	}
	// result 为 string / object 两态。
	r = req(t, `{"model":"m","input":[{"type":"function_result","call_id":"c1","result":"plain"}]}`)
	if r.Messages[0].Blocks[0].ToolResult.Content != "plain" {
		t.Fatalf("string result = %+v", r.Messages[0].Blocks[0])
	}
	r = req(t, `{"model":"m","input":[{"type":"function_result","call_id":"c1","result":{"a": 1}}]}`)
	if r.Messages[0].Blocks[0].ToolResult.Content != `{"a":1}` {
		t.Fatalf("object result = %+v", r.Messages[0].Blocks[0])
	}
	// call_id 必填。
	if _, err := ParseRequest([]byte(`{"model":"m","input":[{"type":"function_result","result":"x"}]}`)); !errors.Is(err, ErrToolResultShape) {
		t.Fatalf("err = %v", err)
	}
}

// TestParseRequestToolChoiceShapes Q4：枚举 auto/any/none/validated；对象形态是
// {allowed_tools:{mode,tools[]}}——CPA 的 {"type":"function","name":…} 是 do-not-copy。
func TestParseRequestToolChoiceShapes(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{`"auto"`, `"auto"`},
		{`"any"`, `"required"`},
		{`"none"`, `"none"`},
		{`"validated"`, `"auto"`},
		{`{"allowed_tools":{"mode":"any","tools":["f"]}}`, `{"type":"function","function":{"name":"f"}}`},
		{`{"allowed_tools":{"mode":"none","tools":["f"]}}`, `"none"`},
	}
	for _, tc := range cases {
		r := req(t, `{"model":"m","input":"x","generation_config":{"tool_choice":`+tc.raw+`}}`)
		assertJSONEqual(t, []byte(tc.want), r.ToolChoice)
	}
	// 多工具 / 非官方对象形态 → 显式报错（规范形只承载单个工具名）。
	for _, bad := range []string{
		`{"allowed_tools":{"mode":"any","tools":["a","b"]}}`,
		`{"type":"function","name":"f"}`,
	} {
		if _, err := ParseRequest([]byte(`{"model":"m","input":"x","generation_config":{"tool_choice":` + bad + `}}`)); !errors.Is(err, ErrUnsupportedToolChoice) {
			t.Fatalf("tool_choice %s: err = %v", bad, err)
		}
	}
}

// TestParseRequestResponseFormat 新 schema response_format：text 家族 → 规范
// response_format（chat 形状）；image/audio/video 形态无任何上游对应物 → 报错。
func TestParseRequestResponseFormat(t *testing.T) {
	r := req(t, `{"model":"m","input":"x","response_format":{"type":"text","mime_type":"application/json","schema":{"type":"object"}}}`)
	var w struct {
		Type       string `json:"type"`
		JSONSchema struct {
			Schema struct {
				Type string `json:"type"`
			} `json:"schema"`
		} `json:"json_schema"`
	}
	if err := json.Unmarshal(r.Extra["response_format"], &w); err != nil {
		t.Fatalf("response_format = %s (%v)", r.Extra["response_format"], err)
	}
	if w.Type != "json_schema" || w.JSONSchema.Schema.Type != "object" {
		t.Fatalf("response_format = %s", r.Extra["response_format"])
	}
	r = req(t, `{"model":"m","input":"x","response_format":{"type":"text","mime_type":"text/plain"}}`)
	assertJSONEqual(t, []byte(`{"type":"text"}`), r.Extra["response_format"])
	if _, err := ParseRequest([]byte(`{"model":"m","input":"x","response_format":{"type":"image"}}`)); !errors.Is(err, ErrUnsupportedBlock) {
		t.Fatalf("image response_format: err = %v", err)
	}
	// legacy response_modalities → gemini 规范键（大写；video 无对应被丢弃）。
	r = req(t, `{"model":"m","input":"x","response_modalities":["text","image","video"]}`)
	assertJSONEqual(t, []byte(`["TEXT","IMAGE"]`), r.Extra["responseModalities"])
}

// TestParseStreamEventTypeMismatch Q6 加固：`event:` 行与 payload event_type
// 不一致 → 显式报错（不静默按 data 分派）。
func TestParseStreamEventTypeMismatch(t *testing.T) {
	p := NewStreamParser()
	_, err := p.Feed("step.start", []byte(`{"index":0,"step":{"type":"model_output"},"event_type":"step.delta"}`))
	if !errors.Is(err, ErrEventTypeMismatch) {
		t.Fatalf("err = %v", err)
	}
	// 帧无 `event:` 行（空）→ 以 event_type 为准（容忍）。
	p = NewStreamParser()
	if _, err := p.Feed("", []byte(`{"interaction":{"id":"v1","model":"m"},"event_type":"interaction.created"}`)); err != nil {
		t.Fatalf("err = %v", err)
	}
}

// TestParseStreamServerToolStepsDropped 服务端工具步（google_search_call/result）
// 无 IR 载体 → 容忍丢弃、不炸链；其后的 model_output 步照常产出（index 带洞）。
// 渲染侧把 IR 块序稠密重编号为 step index（官方形态）。
func TestParseStreamServerToolStepsDropped(t *testing.T) {
	in, err := os.ReadFile(filepath.Join("..", "..", "testdata", "interactions", "stream_server_tool_steps.sse"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	events, err := ParseStream(bytes.NewReader(in))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	var starts []ir.StreamEvent
	for _, e := range events {
		if e.Kind == ir.EventBlockStart {
			starts = append(starts, e)
		}
	}
	if len(starts) != 1 || starts[0].BlockKind != ir.BlockText || starts[0].BlockIndex != 2 {
		t.Fatalf("block starts = %+v", starts)
	}
	out, err := RenderStream(events)
	if err != nil {
		t.Fatalf("RenderStream: %v", err)
	}
	if !strings.Contains(string(out), `"index":0`) || strings.Contains(string(out), `"index":2`) {
		t.Fatalf("step index must be dense: %s", out)
	}
}

// TestParseStreamSentinelWithoutCompleted `[DONE]` 单独出现（无 interaction.completed）
// = 断流 → Finish 判 transport 失败（哨兵按官方形态恒随 completed 之后）。
func TestParseStreamSentinelWithoutCompleted(t *testing.T) {
	events, err := ParseStream(strings.NewReader("event: interaction.created\ndata: {\"interaction\":{\"id\":\"v1\",\"model\":\"m\"},\"event_type\":\"interaction.created\"}\n\nevent: done\ndata: [DONE]\n\n"))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	if len(events) != 2 || events[1].Kind != ir.EventError || events[1].ErrorReason != ir.ErrorTransport {
		t.Fatalf("events = %+v", events)
	}
}

// TestParseStreamStepStopUsageMerged OAS:StepStop 的 usage（累计）合并入记账；
// step_usage（单步）不参与（避免同一 usage 双计）。
func TestParseStreamStepStopUsageMerged(t *testing.T) {
	in := `event: interaction.created
data: {"interaction":{"id":"v1","model":"m"},"event_type":"interaction.created"}

event: step.start
data: {"index":0,"step":{"type":"model_output"},"event_type":"step.start"}

event: step.stop
data: {"index":0,"usage":{"total_input_tokens":5,"total_output_tokens":2,"total_tokens":7},"step_usage":{"total_input_tokens":99},"event_type":"step.stop"}

event: interaction.completed
data: {"interaction":{"id":"v1","status":"completed","usage":{"total_tokens":9}},"event_type":"interaction.completed"}

event: done
data: [DONE]
`
	events, err := ParseStream(strings.NewReader(in))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	var usage *ir.Usage
	for _, e := range events {
		if e.Kind == ir.EventMessageDelta && e.Usage != nil {
			usage = e.Usage
		}
	}
	if usage == nil || usage.Input != 5 || usage.Output != 2 || usage.Total != 9 {
		t.Fatalf("usage = %+v", usage)
	}
}

// TestParseUsageShortNames D2：文档例子里出现的短名（prompt_tokens/completion_tokens）
// 在 OAS 里没有——容忍读取（仅当长名缺失时）；渲染侧只发官方 total_* 一套（Q8）。
func TestParseUsageShortNames(t *testing.T) {
	resp, err := ParseResponse([]byte(`{"id":"int_1","object":"interaction","status":"completed","model":"m","steps":[],"usage":{"prompt_tokens":4,"completion_tokens":6,"total_tokens":10}}`))
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if resp.Usage.Input != 4 || resp.Usage.Output != 6 {
		t.Fatalf("usage = %+v", resp.Usage)
	}
	out, err := RenderResponse(resp)
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	if strings.Contains(string(out), "prompt_tokens") {
		t.Fatalf("short names must not be emitted: %s", out)
	}
}

// TestParseResponseUnknownStepsPreserved 响应里的服务端工具步 / user_input /
// function_result（POST 不含；GET 全时间线会有）→ BlockUnknown + Raw 保真，
// 渲染回 interactions 时原样回写（step 级 Raw）。
func TestParseResponseUnknownStepsPreserved(t *testing.T) {
	in := []byte(`{"id":"int_2","object":"interaction","status":"completed","model":"m","steps":[
		{"type":"google_search_call","id":"gs_1","arguments":{"query":"x"}},
		{"type":"model_output","content":[{"type":"text","text":"done"}]}
	]}`)
	resp, err := ParseResponse(in)
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if resp.Message.Blocks[0].Kind != ir.BlockUnknown || len(resp.Message.Blocks[0].Raw) == 0 {
		t.Fatalf("blocks = %+v", resp.Message.Blocks)
	}
	out, err := RenderResponse(resp)
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	if !strings.Contains(string(out), `"google_search_call"`) {
		t.Fatalf("raw step must be re-emitted: %s", out)
	}
}

// TestRenderRequestThinkingNormalization thinking 归一（A1-12 坑 / Q3）：
// 官方枚举直通；none/未知不发该字段；xhigh/max → high；gemini thinkingConfig
// 原文优先（thinkingLevel 小写化、includeThoughts → thinking_summaries、
// thinkingBudget 丢弃——B1-3/4/5）。
func TestRenderRequestThinkingNormalization(t *testing.T) {
	render := func(extra string) string {
		t.Helper()
		out, err := RenderRequest(&ir.Request{Model: "m", Extra: map[string]json.RawMessage{
			"reasoning_effort": json.RawMessage(extra),
		}})
		if err != nil {
			t.Fatalf("RenderRequest: %v", err)
		}
		return string(out)
	}
	if s := render(`"none"`); strings.Contains(s, "thinking_level") {
		t.Fatalf("none must be dropped: %s", s)
	}
	if s := render(`"max"`); !strings.Contains(s, `"thinking_level":"high"`) {
		t.Fatalf("max → high: %s", s)
	}
	if s := render(`"medium"`); !strings.Contains(s, `"thinking_level":"medium"`) {
		t.Fatalf("medium passthrough: %s", s)
	}
	out, err := RenderRequest(&ir.Request{Model: "m", Extra: map[string]json.RawMessage{
		"thinkingConfig": json.RawMessage(`{"thinkingLevel":"LOW","thinkingBudget":4096,"includeThoughts":true}`),
	}})
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, `"thinking_level":"low"`) || !strings.Contains(s, `"thinking_summaries":"auto"`) {
		t.Fatalf("thinkingConfig mapping: %s", s)
	}
	if strings.Contains(s, "thinking_budget") || strings.Contains(s, "4096") {
		t.Fatalf("thinking_budget is do-not-copy: %s", s)
	}
}

// TestRenderRequestMixedRevisionsAndToolChoice 版本纪律双向：legacy 与新 schema
// 混发 → 报错；tool_choice 规范形 → interactions 形态（required→any、
// function 对象 → allowed_tools）。
func TestRenderRequestMixedRevisionsAndToolChoice(t *testing.T) {
	_, err := RenderRequest(&ir.Request{Model: "m", Extra: map[string]json.RawMessage{
		"responseMimeType": json.RawMessage(`"application/json"`),
		"response_format":  json.RawMessage(`{"type":"text"}`),
	}})
	if !errors.Is(err, ErrMixedRevisions) {
		t.Fatalf("err = %v", err)
	}
	out, err := RenderRequest(&ir.Request{
		Model:      "m",
		ToolChoice: json.RawMessage(`"required"`),
	})
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	if !strings.Contains(string(out), `"tool_choice":"any"`) {
		t.Fatalf("required → any: %s", out)
	}
	out, err = RenderRequest(&ir.Request{
		Model:      "m",
		ToolChoice: json.RawMessage(`{"type":"function","function":{"name":"f"}}`),
	})
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	assertJSONEqual(t, []byte(`{"allowed_tools":{"mode":"any","tools":["f"]}}`),
		rawAt(t, out, "generation_config", "tool_choice"))
}

func rawAt(t *testing.T, body []byte, path ...string) json.RawMessage {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	cur := m
	for i, k := range path {
		v, ok := cur[k]
		if !ok {
			t.Fatalf("key %q missing in %s", k, body)
		}
		if i == len(path)-1 {
			return v
		}
		var next map[string]json.RawMessage
		if err := json.Unmarshal(v, &next); err != nil {
			t.Fatalf("unmarshal %q: %v", k, err)
		}
		cur = next
	}
	return nil
}

// TestRenderResponseRedactedThinking redacted_thinking 无 interactions 对应物：
// 以 thought 步的 signature 载体承载（思考从 part 属性升级为独立步；红色标记
// 本身有损——报告点名）。渲染必须用 `id` 键（function_call）与 `call_id` 键
// （function_result）两者不混。
func TestRenderResponseRedactedThinking(t *testing.T) {
	resp := &ir.Response{
		ID: "int_9", Model: "m",
		Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
			{Kind: ir.BlockRedactedThinking, Signature: "enc_payload"},
			{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{ID: "call_1", Name: "f", Arguments: "{}"}},
		}},
		StopReason: ir.StopToolUse,
	}
	out, err := RenderResponse(resp)
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	s := string(out)
	var body struct {
		Steps []struct {
			Type      string          `json:"type"`
			Signature string          `json:"signature"`
			ID        string          `json:"id"`
			CallID    json.RawMessage `json:"call_id"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(out, &body); err != nil {
		t.Fatalf("unmarshal: %v (%s)", err, out)
	}
	if len(body.Steps) != 2 || body.Steps[0].Type != "thought" || body.Steps[0].Signature != "enc_payload" {
		t.Fatalf("redacted carrier: %s", out)
	}
	if body.Steps[1].Type != "function_call" || body.Steps[1].ID != "call_1" || len(body.Steps[1].CallID) != 0 {
		t.Fatalf("function_call must use id and never call_id: %s", out)
	}
	// 空 ToolCall.ID → 响应 id 派生确定性合成（id 必填；W5-29 跨响应唯一）。
	resp.Message.Blocks[1].ToolCall.ID = ""
	out, err = RenderResponse(resp)
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	if !strings.Contains(string(out), `"id":"`+hygiene.SynthDerivedID("call", "int_9", 1)+`"`) {
		t.Fatalf("synthetic id: %s", out)
	}
	_ = s
}

// TestStreamWriterErrorAndTerminal 失败 = `event: error`（code 字符串）+ **不发**
// `[DONE]`；终态后的事件 → ErrEventsAfterTerminal；缺 stop_reason → 硬门。
func TestStreamWriterErrorAndTerminal(t *testing.T) {
	w := NewStreamWriter()
	if _, err := w.Write(ir.StreamEvent{Kind: ir.EventStart, MessageID: "v1", Model: "m"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	b, err := w.Write(ir.StreamEvent{Kind: ir.EventError, ErrorReason: ir.ErrorUpstream,
		Error: &ir.Error{Type: "api_error", Code: "gateway_timeout", Message: "Deadline expired."}})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	s := string(b)
	if !strings.Contains(s, `"code":"gateway_timeout"`) || !strings.Contains(s, "event: error") {
		t.Fatalf("error frame = %s", s)
	}
	if strings.Contains(s, doneSentinel) {
		t.Fatalf("error must not emit sentinel: %s", s)
	}
	if tail, err := w.Finish(); err != nil || len(tail) != 0 {
		t.Fatalf("Finish after error = %q, %v", tail, err)
	}
	if _, err := w.Write(ir.StreamEvent{Kind: ir.EventDone}); !errors.Is(err, ErrEventsAfterTerminal) {
		t.Fatalf("err = %v", err)
	}

	// 缺 stop_reason：不合成默认值掩盖截断。
	w = NewStreamWriter()
	if _, err := w.Write(ir.StreamEvent{Kind: ir.EventStart, MessageID: "v1"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := w.Write(ir.StreamEvent{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockText}); err != nil {
		t.Fatalf("block_start: %v", err)
	}
	if _, err := w.Finish(); !errors.Is(err, ErrMissingStopReason) {
		t.Fatalf("err = %v", err)
	}
}

// TestParseRequestStatefulFieldsEach 状态化/平台级字段逐个拒绝（9 字段 + store=true）；
// store=false 放行（无服务端状态期望）。
func TestParseRequestStatefulFieldsEach(t *testing.T) {
	bad := []string{
		`{"model":"m","input":"x","previous_interaction_id":"v1"}`,
		`{"agent":"deep-research-pro-preview-12-2025","input":"x"}`,
		`{"model":"m","input":"x","agent_config":{"a":1}}`,
		`{"model":"m","input":"x","environment":{"type":"x"}}`,
		`{"model":"m","input":"x","environment_id":"env_1"}`,
		`{"model":"m","input":"x","background":true}`,
		`{"model":"m","input":"x","webhook_config":{"url":"https://x"}}`,
		`{"model":"m","input":"x","continuation_token":"tok"}`,
		`{"model":"m","input":"x","cached_content":"cachedContents/1"}`,
		`{"model":"m","input":"x","store":true}`,
	}
	for _, body := range bad {
		if _, err := ParseRequest([]byte(body)); !errors.Is(err, ErrStatefulFields) {
			t.Fatalf("%s: err = %v", body, err)
		}
	}
	if _, err := ParseRequest([]byte(`{"model":"m","input":"x","store":false}`)); err != nil {
		t.Fatalf("store=false must pass: %v", err)
	}
	// model 与 agent 二选一必填。
	if _, err := ParseRequest([]byte(`{"input":"x"}`)); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("err = %v", err)
	}
}

// TestParseErrorSSEAndHTTP 两种错误信封：SSE（code 字符串）与 HTTP（Google
// status 惯例，[INFERENCE]）。
func TestParseErrorSSEAndHTTP(t *testing.T) {
	e := ParseError([]byte(`{"error":{"message":"Deadline expired before operation could complete.","code":"gateway_timeout"},"event_type":"error"}`))
	if e.Code != "gateway_timeout" || e.Type != "timeout_error" || e.Message == "" {
		t.Fatalf("sse error = %+v", e)
	}
	e = ParseError([]byte(`{"error":{"code":404,"message":"not here","status":"NOT_FOUND"}}`))
	if e.Type != "not_found_error" || e.Code != "404" {
		t.Fatalf("http error = %+v", e)
	}
	out, err := RenderError(e)
	if err != nil {
		t.Fatalf("RenderError: %v", err)
	}
	assertJSONEqual(t, []byte(`{"error":{"code":404,"message":"not here","status":"NOT_FOUND"}}`), out)
	// 非 JSON body 不炸（Raw 诊断）。
	e = ParseError([]byte("502 Bad Gateway"))
	if e.Type != "api_error" || len(e.Raw) == 0 {
		t.Fatalf("generic error = %+v", e)
	}
}
