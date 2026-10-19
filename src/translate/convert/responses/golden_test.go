package responses

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// goldenCtx 单个 golden 用例的解析产物与渲染输出（check 用）。
type goldenCtx struct {
	req    *ir.Request
	resp   *ir.Response
	events []ir.StreamEvent
	out    []byte
	err    error
}

// TestGoldenResponses Responses 侧 golden 语料（src/translate/testdata/responses/）：
// 输入 → parse → IR → render → 输出。
//
//   - request/response：exact=true 的用例输出与输入 JSON 语义相等（fixture 为
//     渲染器规范形）；所有用例验证**线格式不动点**（parse(render(x)) 再渲染与
//     render(x) 语义相等）；
//   - stream：IR 不动点（parse(render(events)) == events 深等）+ 渲染不动点；
//     exact=true 的用例另验逐帧语义相等；
//   - error：ParseError → RenderError 语义相等；
//   - request_error：ParseRequest 必须显式报错（stateful 字段等）。
func TestGoldenResponses(t *testing.T) {
	dir := filepath.Join("..", "..", "testdata", "responses")

	cases := []struct {
		name  string
		file  string
		kind  string // "request" | "request_error" | "response" | "stream" | "error"
		exact bool
		check func(t *testing.T, c *goldenCtx)
	}{
		{name: "request_full", file: "request_full.json", kind: "request", exact: true, check: checkRequestFull},
		{name: "request_tools_schema", file: "request_tools_schema.json", kind: "request", exact: true, check: checkToolsSchema},
		{name: "request_stateful", file: "request_stateful.json", kind: "request_error", check: checkStatefulError},
		{name: "response_items", file: "response_items.json", kind: "response", exact: true, check: checkResponseItems},
		{name: "response_incomplete", file: "response_incomplete.json", kind: "response", exact: true, check: checkResponseIncomplete},
		{name: "stream_event_order", file: "stream_event_order.sse", kind: "stream", check: checkEventOrder},
		{name: "stream_usage_completed", file: "stream_usage_completed.sse", kind: "stream", check: checkUsageInCompleted},
		{name: "stream_snapshot_fallback", file: "stream_snapshot_fallback.sse", kind: "stream", check: checkSnapshotFallback},
		{name: "stream_tool_calls", file: "stream_tool_calls.sse", kind: "stream", check: checkToolCalls},
		{name: "stream_redacted_thinking", file: "stream_redacted_thinking.sse", kind: "stream", check: checkRedactedThinking},
		{name: "error_envelope", file: "error_envelope.json", kind: "error", exact: true, check: checkErrorEnvelope},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in, err := os.ReadFile(filepath.Join(dir, tc.file))
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			ctx := &goldenCtx{}
			switch tc.kind {
			case "request":
				req, err := ParseRequest(in)
				if err != nil {
					t.Fatalf("ParseRequest: %v", err)
				}
				out, err := RenderRequest(req)
				if err != nil {
					t.Fatalf("RenderRequest: %v", err)
				}
				ctx.req, ctx.out = req, out
				if tc.exact {
					assertJSONEqual(t, in, out)
				}
				// 线格式不动点：parse(render(x)) 再渲染 == render(x)。
				req2, err := ParseRequest(out)
				if err != nil {
					t.Fatalf("re-ParseRequest: %v\n%s", err, out)
				}
				out2, err := RenderRequest(req2)
				if err != nil {
					t.Fatalf("re-RenderRequest: %v", err)
				}
				assertJSONEqual(t, out, out2)
			case "request_error":
				_, err := ParseRequest(in)
				ctx.err = err
				if err == nil {
					t.Fatalf("ParseRequest must fail for %s", tc.file)
				}
			case "response":
				resp, err := ParseResponse(in)
				if err != nil {
					t.Fatalf("ParseResponse: %v", err)
				}
				out, err := RenderResponse(resp)
				if err != nil {
					t.Fatalf("RenderResponse: %v", err)
				}
				ctx.resp, ctx.out = resp, out
				if tc.exact {
					assertJSONEqual(t, in, out)
				}
				resp2, err := ParseResponse(out)
				if err != nil {
					t.Fatalf("re-ParseResponse: %v\n%s", err, out)
				}
				out2, err := RenderResponse(resp2)
				if err != nil {
					t.Fatalf("re-RenderResponse: %v", err)
				}
				assertJSONEqual(t, out, out2)
			case "stream":
				events, err := ParseStream(bytes.NewReader(in))
				if err != nil {
					t.Fatalf("ParseStream: %v", err)
				}
				out, err := RenderStream(events)
				if err != nil {
					t.Fatalf("RenderStream: %v", err)
				}
				ctx.events, ctx.out = events, out
				if tc.exact {
					assertSSEEqual(t, in, out)
				}
				// IR 不动点。
				events2, err := ParseStream(bytes.NewReader(out))
				if err != nil {
					t.Fatalf("re-ParseStream: %v\n%s", err, out)
				}
				if !reflect.DeepEqual(events, events2) {
					t.Fatalf("IR fixed point broken:\nfirst:  %+v\nsecond: %+v", events, events2)
				}
				// 渲染不动点。
				out2, err := RenderStream(events2)
				if err != nil {
					t.Fatalf("re-RenderStream: %v", err)
				}
				assertSSEEqual(t, out, out2)
			case "error":
				e := ParseError(in)
				out, err := RenderError(e)
				if err != nil {
					t.Fatalf("RenderError: %v", err)
				}
				ctx.out = out
				if tc.exact {
					assertJSONEqual(t, in, out)
				}
			default:
				t.Fatalf("unknown kind %q", tc.kind)
			}
			if tc.check != nil {
				tc.check(t, ctx)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 语义断言
// ---------------------------------------------------------------------------

func checkRequestFull(t *testing.T, c *goldenCtx) {
	t.Helper()
	req := c.req
	if req.Model != "gpt-5" || !req.Stream {
		t.Fatalf("model/stream = %q/%v", req.Model, req.Stream)
	}
	// instructions → 首条 system 消息（表 1 行）。
	if len(req.Messages) == 0 || req.Messages[0].Role != ir.RoleSystem {
		t.Fatalf("messages[0] = %+v, want system", req.Messages)
	}
	if req.Messages[0].Blocks[0].Text != "You are a helpful assistant." {
		t.Fatalf("system text = %+v", req.Messages[0].Blocks)
	}
	// items 顺序即语义：user → function_call → function_call_output → assistant。
	wantRoles := []ir.Role{ir.RoleSystem, ir.RoleUser, ir.RoleAssistant, ir.RoleTool, ir.RoleAssistant}
	if len(req.Messages) != len(wantRoles) {
		t.Fatalf("messages = %d, want %d: %+v", len(req.Messages), len(wantRoles), req.Messages)
	}
	for i, r := range wantRoles {
		if req.Messages[i].Role != r {
			t.Fatalf("messages[%d].role = %q, want %q", i, req.Messages[i].Role, r)
		}
	}
	tu := req.Messages[2].Blocks[0]
	if tu.Kind != ir.BlockToolUse || tu.ToolCall == nil || tu.ToolCall.ID != "call_a" ||
		tu.ToolCall.Name != "get_weather" || tu.ToolCall.Arguments != `{"city":"Paris"}` {
		t.Fatalf("tool_use = %+v", tu)
	}
	tr := req.Messages[3].Blocks[0]
	if tr.Kind != ir.BlockToolResult || tr.ToolResult == nil || tr.ToolResult.CallID != "call_a" ||
		tr.ToolResult.Content != "22C sunny" {
		t.Fatalf("tool_result = %+v", tr)
	}
	// tools 扁平 → IR；strict 建模。
	if len(req.Tools) != 1 || req.Tools[0].Name != "get_weather" || req.Tools[0].Strict == nil || !*req.Tools[0].Strict {
		t.Fatalf("tools = %+v", req.Tools)
	}
	// 透传族（表 1 行）与三处形状差异键。
	for _, k := range []string{"temperature", "top_p", "store", "metadata", "parallel_tool_calls", "max_completion_tokens", "reasoning_effort"} {
		if _, ok := req.Extra[k]; !ok {
			t.Fatalf("Extra[%q] missing: %+v", k, req.Extra)
		}
	}
	if string(req.Extra["reasoning_effort"]) != `"low"` {
		t.Fatalf("reasoning_effort = %s", req.Extra["reasoning_effort"])
	}
	if req.StreamOptions == nil || req.StreamOptions.IncludeObfuscation == nil || !*req.StreamOptions.IncludeObfuscation {
		t.Fatalf("stream_options = %+v", req.StreamOptions)
	}
	// 出口形状：tools 扁平（name 在顶层）、input items（无 messages 键）。
	s := string(c.out)
	if !strings.Contains(s, `"input"`) || strings.Contains(s, `"messages"`) {
		t.Fatalf("input shape: %s", s)
	}
	if !strings.Contains(s, `"name":"get_weather"`) || strings.Contains(s, `"function":{"name"`) {
		t.Fatalf("tools must stay flat: %s", s)
	}
}

func checkToolsSchema(t *testing.T, c *goldenCtx) {
	t.Helper()
	req := c.req
	// tool_choice 扁平 → 规范形（chat 形状）。
	assertJSONEqual(t, []byte(`{"type":"function","function":{"name":"lookup"}}`), req.ToolChoice)
	// text.format json_schema → response_format（嵌套 json_schema；表 1 行）。
	rf := req.Extra["response_format"]
	var w struct {
		Type       string `json:"type"`
		JSONSchema struct {
			Name   string `json:"name"`
			Schema struct {
				Type string `json:"type"`
			} `json:"schema"`
			Strict *bool `json:"strict"`
		} `json:"json_schema"`
	}
	if err := json.Unmarshal(rf, &w); err != nil {
		t.Fatalf("response_format = %s (%v)", rf, err)
	}
	if w.Type != "json_schema" || w.JSONSchema.Name != "answer" || w.JSONSchema.Schema.Type != "object" ||
		w.JSONSchema.Strict == nil || !*w.JSONSchema.Strict {
		t.Fatalf("response_format = %s", rf)
	}
	// input_file：IR 无对应块 → Raw 保真（BlockUnknown）。
	var found bool
	for _, m := range req.Messages {
		for _, b := range m.Blocks {
			if b.Kind == ir.BlockUnknown && strings.Contains(string(b.Raw), `"input_file"`) {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("input_file block missing: %+v", req.Messages)
	}
	// 出口：tool_choice 回到扁平形态（name 在顶层，无嵌套 function 对象）；
	// text.format 回到扁平 json_schema。
	var out struct {
		ToolChoice struct {
			Type     string `json:"type"`
			Name     string `json:"name"`
			Function *struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tool_choice"`
		Text struct {
			Format struct {
				Type   string          `json:"type"`
				Name   string          `json:"name"`
				Schema json.RawMessage `json:"schema"`
				Strict *bool           `json:"strict"`
			} `json:"format"`
		} `json:"text"`
	}
	if err := json.Unmarshal(c.out, &out); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if out.ToolChoice.Type != "function" || out.ToolChoice.Name != "lookup" || out.ToolChoice.Function != nil {
		t.Fatalf("flat tool_choice = %+v", out.ToolChoice)
	}
	if out.Text.Format.Type != "json_schema" || out.Text.Format.Name != "answer" ||
		!strings.Contains(string(out.Text.Format.Schema), `"answer"`) ||
		out.Text.Format.Strict == nil || !*out.Text.Format.Strict {
		t.Fatalf("flat text.format = %+v", out.Text.Format)
	}
	if !strings.Contains(string(c.out), `"input_file"`) {
		t.Fatalf("input_file must round-trip: %s", c.out)
	}
}

func checkStatefulError(t *testing.T, c *goldenCtx) {
	t.Helper()
	if !errors.Is(c.err, ErrStatefulFields) {
		t.Fatalf("err = %v, want ErrStatefulFields", c.err)
	}
	for _, field := range []string{"background", "conversation", "previous_response_id"} {
		if !strings.Contains(c.err.Error(), field) {
			t.Fatalf("error must name %q: %v", field, c.err)
		}
	}
}

func checkResponseItems(t *testing.T, c *goldenCtx) {
	t.Helper()
	resp := c.resp
	if resp.ID != "resp_abc" || resp.Model != "gpt-5" || resp.Created != 1741290958 {
		t.Fatalf("meta = %+v", resp)
	}
	if resp.StopReason != ir.StopToolUse {
		t.Fatalf("stop_reason = %q (有 function_call item → toolUse)", resp.StopReason)
	}
	kinds := blockKinds(resp.Message.Blocks)
	if !reflect.DeepEqual(kinds, []ir.BlockKind{ir.BlockThinking, ir.BlockText, ir.BlockToolUse}) {
		t.Fatalf("block kinds = %+v", kinds)
	}
	if resp.Message.Blocks[0].Signature != "sig_blob" {
		t.Fatalf("reasoning signature = %q", resp.Message.Blocks[0].Signature)
	}
	if resp.Message.Blocks[1].Text != "It is 22C in Paris." {
		t.Fatalf("text = %q", resp.Message.Blocks[1].Text)
	}
	// usage：建模字段 + details 原文（chat 规范键名）。
	u := resp.Usage
	if u == nil || u.Input != 100 || u.Output != 20 || u.Total != 120 || u.Reasoning != 5 || u.Accounting != "openai" {
		t.Fatalf("usage = %+v", u)
	}
	if _, ok := u.Extra["prompt_tokens_details"]; !ok {
		t.Fatalf("usage extra = %+v", u.Extra)
	}
	// 出口：output items 保留 reasoning summary + encrypted_content。
	s := string(c.out)
	if !strings.Contains(s, `"encrypted_content":"sig_blob"`) ||
		!strings.Contains(s, `"summary":[{"text":"Check the weather tool first.","type":"summary_text"}]`) {
		t.Fatalf("reasoning item lost: %s", s)
	}
	if !strings.Contains(s, `"call_id":"call_a"`) {
		t.Fatalf("function_call item lost: %s", s)
	}
}

func checkResponseIncomplete(t *testing.T, c *goldenCtx) {
	t.Helper()
	if c.resp.StopReason != ir.StopLength {
		t.Fatalf("stop_reason = %q, want length", c.resp.StopReason)
	}
	s := string(c.out)
	if !strings.Contains(s, `"status":"incomplete"`) ||
		!strings.Contains(s, `"incomplete_details":{"reason":"max_output_tokens"}`) {
		t.Fatalf("incomplete shape lost: %s", s)
	}
}

func checkEventOrder(t *testing.T, c *goldenCtx) {
	t.Helper()
	// 事件序（官方示例帧序，[C:16468-16501]）。
	want := []ir.StreamEventKind{
		ir.EventStart,
		ir.EventBlockStart, ir.EventBlockDelta, ir.EventBlockDelta, ir.EventBlockStop,
		ir.EventMessageDelta, ir.EventMessageDelta, ir.EventMessageStop, ir.EventDone,
	}
	if got := eventKinds(c.events); !reflect.DeepEqual(got, want) {
		t.Fatalf("event kinds = %+v, want %+v", got, want)
	}
	if c.events[0].MessageID != "resp_x1" || c.events[0].Model != "gpt-5" || c.events[0].Created != 1741290958 {
		t.Fatalf("start meta = %+v", c.events[0])
	}
	if txt := collectText(c.events); txt != "Hi there" {
		t.Fatalf("text = %q", txt)
	}
	if r, ok := doneReason(c.events); !ok || r != ir.StopStop {
		t.Fatalf("done reason = %q (%v)", r, ok)
	}
	// 渲染出口：事件族顺序 + sequence_number 从 0 逐事件递增（表 §4.1 显式选择）。
	frames := sseEventTypes(t, c.out)
	wantFrames := []string{
		"response.created", "response.in_progress",
		"response.output_item.added", "response.content_part.added",
		"response.output_text.delta", "response.output_text.delta",
		"response.output_text.done", "response.content_part.done",
		"response.output_item.done", "response.completed",
	}
	if !reflect.DeepEqual(frames, wantFrames) {
		t.Fatalf("rendered frames = %+v, want %+v\n%s", frames, wantFrames, c.out)
	}
	for i, raw := range sseFrames(t, c.out) {
		var w struct {
			Type           string `json:"type"`
			SequenceNumber *int64 `json:"sequence_number"`
		}
		if err := json.Unmarshal([]byte(raw), &w); err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if w.SequenceNumber == nil || *w.SequenceNumber != int64(i) {
			t.Fatalf("frame %d sequence_number = %v (want %d)", i, w.SequenceNumber, i)
		}
		if w.Type != frames[i] {
			t.Fatalf("frame %d type = %q, want %q", i, w.Type, frames[i])
		}
	}
}

func checkUsageInCompleted(t *testing.T, c *goldenCtx) {
	t.Helper()
	u := lastUsage(c.events)
	if u == nil || u.Input != 40 || u.Output != 9 || u.Total != 49 || u.CacheRead != 8 || u.Reasoning != 6 {
		t.Fatalf("usage = %+v", u)
	}
	// usage 只在终止事件里（表 §4.4）：渲染出口的其它帧一律 usage=null。
	frames := sseFrames(t, c.out)
	for i, raw := range frames {
		var w struct {
			Type     string          `json:"type"`
			Response json.RawMessage `json:"response"`
		}
		if err := json.Unmarshal([]byte(raw), &w); err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if w.Type != "response.completed" && w.Type != "response.incomplete" {
			if len(w.Response) > 0 && strings.Contains(string(w.Response), `"usage":{`) {
				t.Fatalf("usage leaked into %s frame: %s", w.Type, raw)
			}
			continue
		}
		if !strings.Contains(string(w.Response), `"usage":{"input_tokens":40`) {
			t.Fatalf("usage missing from terminal frame: %s", raw)
		}
	}
	// 推理 summary 文本经 thinking 块承载。
	if txt := collectThinking(c.events); txt != "Weighing options." {
		t.Fatalf("thinking = %q", txt)
	}
}

func checkSnapshotFallback(t *testing.T, c *goldenCtx) {
	t.Helper()
	// 快照回退：漏发的项（output_index=1）从 response.completed.output 补发。
	texts := textByBlock(c.events)
	if len(texts) != 2 {
		t.Fatalf("blocks = %+v, want 2 (streamed + fallback)", texts)
	}
	if texts[0] != "Part" {
		t.Fatalf("block 0 = %q, want the streamed delta (不重复补)", texts[0])
	}
	if texts[1] != "Part two" {
		t.Fatalf("block 1 = %q, want snapshot fallback text", texts[1])
	}
	// 回退块必须完整（start → delta → stop）。
	if got := eventKinds(c.events); !containsSequence(got, []ir.StreamEventKind{
		ir.EventBlockStart, ir.EventBlockDelta, ir.EventBlockStop,
	}) {
		t.Fatalf("fallback block incomplete: %+v", got)
	}
}

func checkToolCalls(t *testing.T, c *goldenCtx) {
	t.Helper()
	starts := toolStarts(c.events)
	if len(starts) != 1 || starts[0].id != "call_t1" || starts[0].name != "get_weather" {
		t.Fatalf("tool starts = %+v", starts)
	}
	args := argsByBlock(c.events)
	if got := args[starts[0].index]; got != `{"city":"Paris"}` {
		t.Fatalf("tool args = %q", got)
	}
	if r, ok := doneReason(c.events); !ok || r != ir.StopToolUse {
		t.Fatalf("done reason = %q (%v)", r, ok)
	}
	// 出口：output_item.added 的 arguments 占位 ""（0 值有意义）与 done 的全量参数。
	s := string(c.out)
	if !strings.Contains(s, `"arguments":""`) || !strings.Contains(s, `"arguments":"{\"city\":\"Paris\"}"`) {
		t.Fatalf("arguments placeholder/full missing: %s", s)
	}
}

func checkRedactedThinking(t *testing.T, c *goldenCtx) {
	t.Helper()
	var redacted *ir.StreamEvent
	for i := range c.events {
		if c.events[i].Kind == ir.EventBlockStart && c.events[i].BlockKind == ir.BlockRedactedThinking {
			redacted = &c.events[i]
		}
	}
	if redacted == nil {
		t.Fatalf("redacted_thinking block missing: %+v", c.events)
	}
	// 载荷经签名通道承载，前缀已剥离。
	var payload string
	for _, ev := range c.events {
		if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockRedactedThinking {
			payload += ev.SignatureDelta
		}
	}
	if payload != "RU5DUllQVEVE" {
		t.Fatalf("redacted payload = %q", payload)
	}
	// 出口：前缀载体回写（CPA 约定）。
	if !strings.Contains(string(c.out), redactedThinkingPrefix+"RU5DUllQVEVE") {
		t.Fatalf("prefix carrier lost: %s", c.out)
	}
}

func checkErrorEnvelope(t *testing.T, c *goldenCtx) {
	t.Helper()
	e := ParseError([]byte(readGolden(t, "error_envelope.json")))
	if e.Type != "invalid_request_error" || e.Code != "invalid_prompt" || e.Param != "input" ||
		e.Source != "openai" {
		t.Fatalf("error = %+v", e)
	}
	if !strings.Contains(string(c.out), `"error":{`) {
		t.Fatalf("nested envelope lost: %s", c.out)
	}
}

func readGolden(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "responses", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return b
}

// ---------------------------------------------------------------------------
// 测试工具
// ---------------------------------------------------------------------------

func blockKinds(blocks []ir.ContentBlock) []ir.BlockKind {
	out := make([]ir.BlockKind, 0, len(blocks))
	for _, b := range blocks {
		out = append(out, b.Kind)
	}
	return out
}

func eventKinds(events []ir.StreamEvent) []ir.StreamEventKind {
	out := make([]ir.StreamEventKind, 0, len(events))
	for _, ev := range events {
		out = append(out, ev.Kind)
	}
	return out
}

func containsSequence(haystack, needle []ir.StreamEventKind) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		ok := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func collectText(events []ir.StreamEvent) string {
	var b strings.Builder
	for _, ev := range events {
		if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockText {
			b.WriteString(ev.TextDelta)
		}
	}
	return b.String()
}

func collectThinking(events []ir.StreamEvent) string {
	var b strings.Builder
	for _, ev := range events {
		if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockThinking {
			b.WriteString(ev.TextDelta)
		}
	}
	return b.String()
}

// textByBlock 按块序收集文本（首见顺序）。
func textByBlock(events []ir.StreamEvent) []string {
	var (
		order []int
		text  = map[int]string{}
	)
	for _, ev := range events {
		if ev.Kind == ir.EventBlockStart && ev.BlockKind == ir.BlockText {
			order = append(order, ev.BlockIndex)
		}
		if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockText {
			text[ev.BlockIndex] += ev.TextDelta
		}
	}
	out := make([]string, 0, len(order))
	for _, idx := range order {
		out = append(out, text[idx])
	}
	return out
}

type toolStart struct {
	index int
	id    string
	name  string
}

func toolStarts(events []ir.StreamEvent) []toolStart {
	var out []toolStart
	for _, ev := range events {
		if ev.Kind == ir.EventBlockStart && (ev.BlockKind == ir.BlockToolUse || ev.BlockKind == ir.BlockServerToolUse) {
			out = append(out, toolStart{index: ev.BlockIndex, id: ev.ToolCallID, name: ev.ToolName})
		}
	}
	return out
}

func argsByBlock(events []ir.StreamEvent) map[int]string {
	out := map[int]string{}
	for _, ev := range events {
		if ev.Kind == ir.EventBlockDelta && (ev.BlockKind == ir.BlockToolUse || ev.BlockKind == ir.BlockServerToolUse) {
			out[ev.BlockIndex] += ev.ArgsDelta
		}
	}
	return out
}

func doneReason(events []ir.StreamEvent) (ir.StopReason, bool) {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Kind == ir.EventDone {
			return events[i].Reason, true
		}
	}
	return "", false
}

func lastUsage(events []ir.StreamEvent) *ir.Usage {
	var u *ir.Usage
	for _, ev := range events {
		if ev.Usage != nil {
			u = ev.Usage
		}
	}
	return u
}

// sseFrames 提取 SSE 帧的 data 载荷序列（多行 data 以 \n 连接）。
func sseFrames(t *testing.T, data []byte) []string {
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

// sseEventTypes 提取帧内 payload 的 type 序列。
func sseEventTypes(t *testing.T, data []byte) []string {
	t.Helper()
	var out []string
	for _, raw := range sseFrames(t, data) {
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

// assertSSEEqual 逐帧语义比较两个 SSE 流（帧数与顺序必须一致）。
func assertSSEEqual(t *testing.T, want, got []byte) {
	t.Helper()
	wf := sseFrames(t, want)
	gf := sseFrames(t, got)
	if len(wf) != len(gf) {
		t.Fatalf("SSE frame count mismatch: want %d, got %d\nwant:\n%s\ngot:\n%s",
			len(wf), len(gf), want, got)
	}
	for i := range wf {
		assertJSONEqual(t, []byte(wf[i]), []byte(gf[i]))
	}
}

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
