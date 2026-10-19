package interactions

import (
	"bytes"
	"encoding/json"
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

// TestGoldenInteractions Interactions 侧 golden 语料（src/translate/testdata/interactions/）：
// 输入 → parse → IR → render → 输出。
//
//   - request/response：exact=true 的用例输出与输入 JSON 语义相等（fixture 为
//     渲染器规范形）；所有用例验证**线格式不动点**（parse(render(x)) 再渲染与
//     render(x) 语义相等）；
//   - stream：IR 不动点（parse(render(events)) == events 深等）+ 渲染不动点
//     （逐字节——writer 确定性）；
//   - error：ParseError → RenderError 语义相等；
//   - request_error：ParseRequest 必须显式报错（stateful/混发版本字段等）。
func TestGoldenInteractions(t *testing.T) {
	dir := filepath.Join("..", "..", "testdata", "interactions")

	cases := []struct {
		name  string
		file  string
		kind  string // "request" | "request_error" | "response" | "stream" | "error"
		exact bool
		check func(t *testing.T, c *goldenCtx)
	}{
		{name: "request_steps", file: "request_steps.json", kind: "request", exact: true, check: checkRequestSteps},
		{name: "request_stateful", file: "request_stateful.json", kind: "request_error", check: checkStatefulError},
		{name: "request_legacy_mixed", file: "request_legacy_mixed.json", kind: "request_error", check: checkMixedRevisions},
		{name: "response_steps", file: "response_steps.json", kind: "response", exact: true, check: checkResponseSteps},
		{name: "response_completed", file: "response_completed.json", kind: "response", exact: true, check: checkResponseCompleted},
		{name: "stream_event_order", file: "stream_event_order.sse", kind: "stream", check: checkStreamEventOrder},
		{name: "stream_tool_arguments_delta", file: "stream_tool_arguments_delta.sse", kind: "stream", check: checkStreamToolArguments},
		{name: "stream_sentinel", file: "stream_sentinel.sse", kind: "stream", check: checkStreamSentinel},
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
				// IR 不动点。
				events2, err := ParseStream(bytes.NewReader(out))
				if err != nil {
					t.Fatalf("re-ParseStream: %v\n%s", err, out)
				}
				if !reflect.DeepEqual(events, events2) {
					t.Fatalf("IR fixed point broken:\nfirst:  %+v\nsecond: %+v", events, events2)
				}
				// 渲染不动点（writer 确定性 → 逐字节）。
				out2, err := RenderStream(events2)
				if err != nil {
					t.Fatalf("re-RenderStream: %v", err)
				}
				if !bytes.Equal(out, out2) {
					t.Fatalf("render fixed point broken:\nfirst:\n%s\nsecond:\n%s", out, out2)
				}
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

func checkRequestSteps(t *testing.T, c *goldenCtx) {
	t.Helper()
	req := c.req
	if req.Model != "gemini-3-flash-preview" || !req.Stream {
		t.Fatalf("model/stream = %q/%v", req.Model, req.Stream)
	}
	// system_instruction（扁平字符串）→ 首条 system 消息。
	wantRoles := []ir.Role{ir.RoleSystem, ir.RoleUser, ir.RoleAssistant, ir.RoleTool, ir.RoleAssistant}
	if len(req.Messages) != len(wantRoles) {
		t.Fatalf("messages = %d, want %d: %+v", len(req.Messages), len(wantRoles), req.Messages)
	}
	for i, r := range wantRoles {
		if req.Messages[i].Role != r {
			t.Fatalf("messages[%d].role = %q, want %q", i, req.Messages[i].Role, r)
		}
	}
	// thought 步 → thinking 块（签名原样）。
	th := req.Messages[2].Blocks[0]
	if th.Kind != ir.BlockThinking || th.Signature != "thought_sig_abcd1234" ||
		th.Text != "The user wants the lights dimmed and warm." {
		t.Fatalf("thought = %+v", th)
	}
	// function_call 步 → tool_use 块（id 键；arguments 对象原文）。
	tu := req.Messages[2].Blocks[1]
	if tu.Kind != ir.BlockToolUse || tu.ToolCall == nil || tu.ToolCall.ID != "call_98231" ||
		tu.ToolCall.Name != "set_light_values" ||
		tu.ToolCall.Arguments != `{"brightness":30,"color_temp":"warm"}` {
		t.Fatalf("tool_use = %+v", tu)
	}
	// function_result 步 → tool 消息（call_id 键 + name 承载）。
	tr := req.Messages[3].Blocks[0]
	if tr.Kind != ir.BlockToolResult || tr.ToolResult == nil ||
		tr.ToolResult.CallID != "call_98231" || tr.ToolResult.Content != `{"ok":true}` {
		t.Fatalf("tool_result = %+v", tr)
	}
	if req.Messages[3].Name != "set_light_values" {
		t.Fatalf("tool message name = %q", req.Messages[3].Name)
	}
	// generation_config → 规范键。
	for _, k := range []string{"max_tokens", "temperature", "top_p", "stop", "seed", "reasoning_effort", "thinking_summaries"} {
		if _, ok := req.Extra[k]; !ok {
			t.Fatalf("Extra[%q] missing: %+v", k, req.Extra)
		}
	}
	if string(req.Extra["reasoning_effort"]) != `"low"` || string(req.Extra["stop"]) != `["END"]` {
		t.Fatalf("Extra = %+v", req.Extra)
	}
	// tool_choice any → 规范 required（A2-4 反向）。
	assertJSONEqual(t, []byte(`"required"`), req.ToolChoice)
	// tools 扁平 → ToolDef；strict 非官方字段不出现。
	if len(req.Tools) != 1 || req.Tools[0].Name != "set_light_values" {
		t.Fatalf("tools = %+v", req.Tools)
	}
	// 出口形状：function_call 步用 `id`，绝不用 `call_id`（Q1）。
	var body struct {
		Input []struct {
			Type      string          `json:"type"`
			ID        string          `json:"id"`
			CallID    json.RawMessage `json:"call_id"`
			Arguments json.RawMessage `json:"arguments"`
		} `json:"input"`
	}
	if err := json.Unmarshal(c.out, &body); err != nil {
		t.Fatalf("unmarshal out: %v", err)
	}
	var sawCall bool
	for _, st := range body.Input {
		if st.Type == "function_call" {
			sawCall = true
			if st.ID != "call_98231" || len(st.CallID) != 0 {
				t.Fatalf("function_call step must use id and never call_id: %s", c.out)
			}
		}
	}
	if !sawCall {
		t.Fatalf("no function_call step in output: %s", c.out)
	}
}

func checkStatefulError(t *testing.T, c *goldenCtx) {
	t.Helper()
	if !strings.Contains(c.err.Error(), "previous_interaction_id") ||
		!strings.Contains(c.err.Error(), "store=true") {
		t.Fatalf("stateful error = %v", c.err)
	}
}

func checkMixedRevisions(t *testing.T, c *goldenCtx) {
	t.Helper()
	if !strings.Contains(c.err.Error(), "legacy and new-schema") {
		t.Fatalf("mixed-revision error = %v", c.err)
	}
}

func checkResponseSteps(t *testing.T, c *goldenCtx) {
	t.Helper()
	resp := c.resp
	if resp.StopReason != ir.StopToolUse {
		t.Fatalf("stop reason = %q", resp.StopReason)
	}
	if resp.ID != "int_123" || resp.Model != "gemini-3-flash-preview" {
		t.Fatalf("id/model = %q/%q", resp.ID, resp.Model)
	}
	blocks := resp.Message.Blocks
	if len(blocks) != 3 || blocks[0].Kind != ir.BlockThinking || blocks[1].Kind != ir.BlockText ||
		blocks[2].Kind != ir.BlockToolUse {
		t.Fatalf("blocks = %+v", blocks)
	}
	if blocks[0].Signature != "thought_sig_abcd1234" || blocks[1].Text != "Let me check the weather for you." {
		t.Fatalf("blocks = %+v", blocks)
	}
	if blocks[2].ToolCall == nil || blocks[2].ToolCall.Arguments != `{"location":"Boston, MA"}` {
		t.Fatalf("tool_use = %+v", blocks[2])
	}
	if resp.Usage == nil || resp.Usage.Input != 11 || resp.Usage.Output != 90 ||
		resp.Usage.Total != 101 || resp.Usage.Reasoning != 245 || resp.Usage.Accounting != "gemini" {
		t.Fatalf("usage = %+v", resp.Usage)
	}
	if !strings.Contains(string(c.out), `"status":"requires_action"`) {
		t.Fatalf("out status: %s", c.out)
	}
}

func checkResponseCompleted(t *testing.T, c *goldenCtx) {
	t.Helper()
	resp := c.resp
	if resp.StopReason != ir.StopStop || resp.Message.Blocks[0].Text != "The capital of France is Paris." {
		t.Fatalf("resp = %+v", resp)
	}
	if resp.Usage == nil || resp.Usage.Total != 19 {
		t.Fatalf("usage = %+v", resp.Usage)
	}
	// 模态分解原文保真（Extra）并在出口回写。
	if _, ok := resp.Usage.Extra["input_tokens_by_modality"]; !ok {
		t.Fatalf("usage extra = %+v", resp.Usage.Extra)
	}
	if !strings.Contains(string(c.out), `"input_tokens_by_modality":[{"modality":"text","tokens":7}]`) {
		t.Fatalf("out usage: %s", c.out)
	}
	if !strings.Contains(string(c.out), `"status":"completed"`) {
		t.Fatalf("out status: %s", c.out)
	}
}

func checkStreamEventOrder(t *testing.T, c *goldenCtx) {
	t.Helper()
	ev := c.events
	wantKinds := []ir.StreamEventKind{
		ir.EventStart,
		ir.EventBlockStart, ir.EventBlockDelta, ir.EventBlockStop,
		ir.EventBlockStart, ir.EventBlockDelta, ir.EventBlockDelta, ir.EventBlockStop,
		ir.EventMessageDelta, ir.EventMessageDelta, ir.EventMessageStop, ir.EventDone,
	}
	if len(ev) != len(wantKinds) {
		t.Fatalf("events = %d, want %d: %+v", len(ev), len(wantKinds), ev)
	}
	for i, k := range wantKinds {
		if ev[i].Kind != k {
			t.Fatalf("events[%d].kind = %q, want %q", i, ev[i].Kind, k)
		}
	}
	if ev[0].MessageID != "v1_abc123" || ev[0].Model != "gemini-3-flash-preview" {
		t.Fatalf("start = %+v", ev[0])
	}
	if ev[1].BlockKind != ir.BlockThinking || ev[2].SignatureDelta != "thought_sig_abcd1234" {
		t.Fatalf("thought step = %+v / %+v", ev[1], ev[2])
	}
	if ev[4].BlockKind != ir.BlockText || ev[5].TextDelta != "1, 2, 3, 4, 5, 6, " {
		t.Fatalf("text step = %+v / %+v", ev[4], ev[5])
	}
	if ev[8].StopReason != ir.StopStop || ev[9].Usage == nil || ev[9].Usage.Total != 346 ||
		ev[9].Usage.Reasoning != 245 {
		t.Fatalf("terminal = %+v / %+v", ev[8], ev[9])
	}
	if ev[11].Reason != ir.StopStop {
		t.Fatalf("done = %+v", ev[11])
	}
	// 出口：哨兵与两处事件名一致。
	s := string(c.out)
	if !strings.HasSuffix(s, "event: done\ndata: [DONE]\n\n") {
		t.Fatalf("out must end with sentinel: %s", s)
	}
	if !strings.Contains(s, "event: interaction.completed\n") ||
		!strings.Contains(s, "event: interaction.status_update\n") {
		t.Fatalf("out lifecycle frames: %s", s)
	}
}

func checkStreamToolArguments(t *testing.T, c *goldenCtx) {
	t.Helper()
	ev := c.events
	var (
		start    *ir.StreamEvent
		args     strings.Builder
		terminal *ir.StreamEvent
	)
	for i := range ev {
		switch ev[i].Kind {
		case ir.EventBlockStart:
			start = &ev[i]
		case ir.EventBlockDelta:
			args.WriteString(ev[i].ArgsDelta)
		case ir.EventMessageDelta:
			if ev[i].StopReason != "" {
				terminal = &ev[i]
			}
		}
	}
	if start == nil || start.BlockKind != ir.BlockToolUse || start.ToolCallID != "un6k8t18" ||
		start.ToolName != "get_weather" {
		t.Fatalf("block_start = %+v", start)
	}
	// arguments_delta 必须累积（BC:413-419）——分片在 IR 层保持片段语义。
	if got := args.String(); got != `{"location": "San Francisco, CA"}` {
		t.Fatalf("accumulated args = %q", got)
	}
	if terminal == nil || terminal.StopReason != ir.StopToolUse {
		t.Fatalf("stop reason = %+v", terminal)
	}
	var started bool
	for _, frame := range strings.Split(string(c.out), "\n") {
		data, ok := strings.CutPrefix(frame, "data: ")
		if !ok || !strings.Contains(data, `"step.start"`) {
			continue
		}
		var w struct {
			Step struct {
				Type      string          `json:"type"`
				ID        string          `json:"id"`
				Name      string          `json:"name"`
				Arguments json.RawMessage `json:"arguments"`
			} `json:"step"`
		}
		if err := json.Unmarshal([]byte(data), &w); err != nil {
			t.Fatalf("step.start frame: %v", err)
		}
		if w.Step.Type != "function_call" || w.Step.ID != "un6k8t18" || w.Step.Name != "get_weather" ||
			string(w.Step.Arguments) != "{}" {
			t.Fatalf("step.start = %+v", w.Step)
		}
		started = true
	}
	if !started {
		t.Fatalf("no step.start frame: %s", c.out)
	}
	if !strings.Contains(string(c.out), `"status":"requires_action"`) {
		t.Fatalf("completed status: %s", c.out)
	}
}

func checkStreamSentinel(t *testing.T, c *goldenCtx) {
	t.Helper()
	var dones int
	for _, e := range c.events {
		if e.Kind == ir.EventDone {
			dones++
			if e.Reason != ir.StopStop {
				t.Fatalf("done reason = %q", e.Reason)
			}
		}
	}
	if dones != 1 {
		t.Fatalf("done events = %d, want 1", dones)
	}
	if !strings.HasSuffix(string(c.out), "event: done\ndata: [DONE]\n\n") {
		t.Fatalf("out must end with sentinel: %s", c.out)
	}
}

func checkErrorEnvelope(t *testing.T, c *goldenCtx) {
	t.Helper()
	e := ParseError(mustRead(t, filepath.Join("..", "..", "testdata", "interactions", "error_envelope.json")))
	if e.Type != "invalid_request_error" || e.Code != "400" || e.Message != "Invalid JSON payload received." {
		t.Fatalf("error = %+v", e)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

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
