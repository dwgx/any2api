package gemini

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
}

// TestGoldenGemini Gemini 侧 golden 语料（src/translate/testdata/gemini/）：
// 输入 → parse → IR → render → 输出。
//
//   - request/response：exact=true 的用例输出与输入 JSON 语义相等（fixture 为
//     渲染器规范形）；所有用例验证**线格式不动点**（parse(render(x)) 再渲染与
//     render(x) 语义相等——清洗/归一化用例从第二遍起定点）；
//   - stream：IR 不动点（parse(render(events)) == events 深等）+ 渲染不动点；
//     exact=true 的用例另验逐帧语义相等。
func TestGoldenGemini(t *testing.T) {
	dir := filepath.Join("..", "..", "testdata", "gemini")

	cases := []struct {
		name  string
		file  string
		kind  string // "request" | "response" | "stream"
		exact bool
		check func(t *testing.T, c *goldenCtx)
	}{
		{name: "request_full", file: "request_full.json", kind: "request", exact: true, check: checkRequestFull},
		{name: "request_schema_subset", file: "request_schema_subset.json", kind: "request", exact: false, check: checkSchemaSubset},
		{name: "response_tool_calls", file: "response_tool_calls.json", kind: "response", exact: true, check: checkResponseToolCalls},
		{name: "response_safety_block", file: "response_safety_block.json", kind: "response", exact: false, check: checkResponseSafetyBlock},
		{name: "stream_text_usage_snapshot", file: "stream_text_usage_snapshot.sse", kind: "stream", exact: false, check: checkTextUsageSnapshot},
		{name: "stream_functioncall_parallel", file: "stream_functioncall_parallel.sse", kind: "stream", exact: false, check: checkFunctionCallParallel},
		{name: "stream_tool_index_stable", file: "stream_tool_index_stable.sse", kind: "stream", exact: false, check: checkToolIndexStable},
		{name: "stream_thought_text_mixed", file: "stream_thought_text_mixed.sse", kind: "stream", exact: false, check: checkThoughtTextMixed},
		{name: "stream_partial_args", file: "stream_partial_args.sse", kind: "stream", exact: false, check: checkPartialArgs},
		{name: "stream_error_line", file: "stream_error_line.sse", kind: "stream", exact: false, check: checkErrorLine},
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
	if len(req.Messages) == 0 || req.Messages[0].Role != ir.RoleSystem {
		t.Fatalf("messages[0] = %+v, want system", req.Messages)
	}
	sys := req.Messages[0].Blocks
	if len(sys) != 2 || sys[0].Text != "You are a helpful assistant." || sys[1].Text != "Answer in metric units." {
		t.Fatalf("system blocks = %+v", sys)
	}
	if len(req.Tools) != 1 || req.Tools[0].Name != "get_weather" {
		t.Fatalf("tools = %+v", req.Tools)
	}
	var toolUse, toolResult bool
	for _, m := range req.Messages {
		for _, b := range m.Blocks {
			switch {
			case b.Kind == ir.BlockToolUse && b.ToolCall != nil:
				toolUse = true
				if b.ToolCall.ID != "call_a" || b.ToolCall.Name != "get_weather" {
					t.Fatalf("tool_use = %+v", b.ToolCall)
				}
				var args map[string]any
				if err := json.Unmarshal([]byte(b.ToolCall.Arguments), &args); err != nil || args["city"] != "Paris" {
					t.Fatalf("tool_use args = %q (%v)", b.ToolCall.Arguments, err)
				}
			case b.Kind == ir.BlockToolResult && b.ToolResult != nil:
				toolResult = true
				if b.ToolResult.CallID != "call_a" {
					t.Fatalf("tool_result = %+v", b.ToolResult)
				}
			}
		}
	}
	if !toolUse || !toolResult {
		t.Fatalf("tool round-trip missing: use=%v result=%v", toolUse, toolResult)
	}
	if m := findMessage(req.Messages, ir.RoleTool); m == nil || m.Name != "get_weather" {
		t.Fatalf("tool message name pairing missing: %+v", m)
	}
	for _, k := range []string{"max_tokens", "temperature", "top_p", "stop", "safetySettings"} {
		if _, ok := req.Extra[k]; !ok {
			t.Fatalf("Extra[%q] missing: %+v", k, req.Extra)
		}
	}
	if string(req.ToolChoice) != `"auto"` {
		t.Fatalf("tool_choice = %s", req.ToolChoice)
	}
	if !strings.Contains(string(c.out), `"systemInstruction"`) {
		t.Fatalf("systemInstruction missing in output: %s", c.out)
	}
}

func checkSchemaSubset(t *testing.T, c *goldenCtx) {
	t.Helper()
	// parse 保真：原始 schema（含 $ref/$defs/x-*）不被改写。
	if raw := string(c.req.Tools[0].InputSchema); !strings.Contains(raw, `"$ref"`) || !strings.Contains(raw, `"$defs"`) {
		t.Fatalf("parse must keep raw schema: %s", raw)
	}
	var doc struct {
		Tools []struct {
			FunctionDeclarations []struct {
				Parameters           map[string]any `json:"parameters"`
				ParametersJSONSchema any            `json:"parametersJsonSchema"`
			} `json:"functionDeclarations"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(c.out, &doc); err != nil {
		t.Fatalf("unmarshal output: %v\n%s", err, c.out)
	}
	if len(doc.Tools) != 1 || len(doc.Tools[0].FunctionDeclarations) != 1 {
		t.Fatalf("tools shape = %+v", doc.Tools)
	}
	decl := doc.Tools[0].FunctionDeclarations[0]
	if decl.Parameters == nil || decl.ParametersJSONSchema != nil {
		t.Fatalf("carrier selection: parameters=%v jsonSchema=%v (want tight carrier)", decl.Parameters, decl.ParametersJSONSchema)
	}
	schema := string(c.out)
	for _, banned := range []string{`"$ref"`, `"$defs"`, `"allOf"`, `"anyOf"`, `"x-vendor-note"`} {
		if strings.Contains(schema, banned) {
			t.Fatalf("%s leaked into cleaned schema: %s", banned, schema)
		}
	}
	props, _ := decl.Parameters["properties"].(map[string]any)
	if props == nil {
		t.Fatalf("properties missing: %+v", decl.Parameters)
	}
	mode, _ := props["mode"].(map[string]any)
	if mode == nil {
		t.Fatalf("mode missing")
	}
	enum, _ := mode["enum"].([]any)
	if len(enum) != 3 || enum[0] != "train" || enum[1] != "plane" || enum[2] != "1" {
		t.Fatalf("enum stringification = %+v", mode["enum"])
	}
	if mode["type"] != "string" {
		t.Fatalf("enum type = %v, want string", mode["type"])
	}
	desc, _ := mode["description"].(string)
	if !strings.Contains(desc, "See: TravelMode") || !strings.Contains(desc, "Allowed: train, plane, 1") {
		t.Fatalf("mode description hints = %q", desc)
	}
	currency, _ := props["currency"].(map[string]any)
	if currency == nil || !strings.Contains(currency["description"].(string), "See: Currency") {
		t.Fatalf("$ref hint missing: %+v", currency)
	}
	stops, _ := props["stops"].(map[string]any)
	if stops == nil {
		t.Fatalf("stops missing")
	}
	if _, ok := stops["allOf"]; ok {
		t.Fatalf("allOf not flattened: %+v", stops)
	}
	if _, ok := stops["properties"].(map[string]any)["hint"]; !ok {
		t.Fatalf("allOf branch properties not merged: %+v", stops)
	}
	budget, _ := props["budget"].(map[string]any)
	if budget == nil || budget["type"] != "number" || budget["nullable"] != true {
		t.Fatalf("anyOf projection = %+v", budget)
	}
	req, _ := decl.Parameters["required"].([]any)
	if len(req) != 2 || req[0] != "mode" || req[1] != "stops" {
		t.Fatalf("required cleanup = %+v", decl.Parameters["required"])
	}
}

func checkResponseToolCalls(t *testing.T, c *goldenCtx) {
	t.Helper()
	resp := c.resp
	if resp.StopReason != ir.StopToolUse {
		t.Fatalf("stop reason = %q, want toolUse (functionCall 强制覆盖)", resp.StopReason)
	}
	if resp.ID != "r-tool-1" || resp.Model != "gemini-2.5-flash" {
		t.Fatalf("envelope = %+v", resp)
	}
	var text, call bool
	for _, b := range resp.Message.Blocks {
		switch b.Kind {
		case ir.BlockText:
			text = b.Text == "Checking the weather."
		case ir.BlockToolUse:
			call = b.ToolCall != nil && b.ToolCall.ID == "call_a" && b.ToolCall.Name == "get_weather"
		}
	}
	if !text || !call {
		t.Fatalf("blocks = %+v", resp.Message.Blocks)
	}
	u := resp.Usage
	if u == nil || u.Input != 12 || u.Output != 10 || u.Reasoning != 2 || u.CacheRead != 3 || u.Total != 22 || u.Accounting != "gemini" {
		t.Fatalf("usage = %+v", u)
	}
}

func checkResponseSafetyBlock(t *testing.T, c *goldenCtx) {
	t.Helper()
	resp := c.resp
	if resp.StopReason != ir.StopError {
		t.Fatalf("stop reason = %q, want error (content_filter 语义)", resp.StopReason)
	}
	if len(resp.Message.Blocks) != 0 {
		t.Fatalf("blocked response must be empty, got %+v", resp.Message.Blocks)
	}
	if resp.Usage == nil || resp.Usage.Input != 5 || resp.Usage.Total != 5 {
		t.Fatalf("usage = %+v", resp.Usage)
	}
	if !strings.Contains(string(c.out), `"finishReason":"SAFETY"`) {
		t.Fatalf("blocked response must be explicit (finishReason SAFETY): %s", c.out)
	}
}

func checkTextUsageSnapshot(t *testing.T, c *goldenCtx) {
	t.Helper()
	if got := collectText(c.events); got != "I cannot do that here" {
		t.Fatalf("text = %q", got)
	}
	u := lastUsage(c.events)
	// 末帧权威 + merge-non-zero：中途 101/101 被末帧 27/32/59 覆盖。
	if u == nil || u.Input != 27 || u.Output != 32 || u.Total != 59 || u.Accounting != "gemini" || u.Estimated {
		t.Fatalf("usage = %+v", u)
	}
	if r, ok := doneReason(c.events); !ok || r != ir.StopStop {
		t.Fatalf("done reason = %q (ok=%v)", r, ok)
	}
	if strings.Contains(string(c.out), "[DONE]") {
		t.Fatalf("gemini stream must not carry a sentinel:\n%s", c.out)
	}
	// 出口规范形：usageMetadata 只随终块。
	frames := sseFrames(t, c.out)
	for i, f := range frames[:len(frames)-1] {
		if strings.Contains(f, "usageMetadata") {
			t.Fatalf("frame %d carries usageMetadata (want terminal-only): %s", i, f)
		}
	}
	if !strings.Contains(frames[len(frames)-1], "usageMetadata") {
		t.Fatalf("terminal frame missing usageMetadata: %s", frames[len(frames)-1])
	}
}

func checkFunctionCallParallel(t *testing.T, c *goldenCtx) {
	t.Helper()
	starts := toolStarts(c.events)
	if len(starts) != 3 {
		t.Fatalf("tool blocks = %+v, want 3 (1 + 2 parallel)", starts)
	}
	if starts[0].name != "gc_thermal_probe" || starts[1].name != "get_time" || starts[2].name != "get_units" {
		t.Fatalf("tool names = %+v", starts)
	}
	for i := 1; i < len(starts); i++ {
		if starts[i].index <= starts[i-1].index {
			t.Fatalf("tool indices not monotonic: %+v", starts)
		}
	}
	args := argsByBlock(c.events)
	var probe map[string]any
	if err := json.Unmarshal([]byte(args[starts[0].index]), &probe); err != nil || probe["city"] != "Oslo" {
		t.Fatalf("probe args = %q", args[starts[0].index])
	}
	if got := args[starts[2].index]; got != "{}" {
		t.Fatalf("empty args = %q, want {}", got)
	}
	if r, ok := doneReason(c.events); !ok || r != ir.StopToolUse {
		t.Fatalf("done reason = %q (ok=%v), want toolUse", r, ok)
	}
}

func checkToolIndexStable(t *testing.T, c *goldenCtx) {
	t.Helper()
	starts := toolStarts(c.events)
	if len(starts) != 2 {
		t.Fatalf("tool blocks = %+v, want 2 (duplicate call id skipped)", starts)
	}
	if starts[0].id != "call_a" || starts[1].id != "call_b" {
		t.Fatalf("tool ids = %+v", starts)
	}
	// 帧内 index 重启（两帧都是 parts[0]）不得串台：槽位按 call id 稳定。
	args := argsByBlock(c.events)
	if len(args) != 2 {
		t.Fatalf("args by block = %+v, want exactly one delta per call", args)
	}
}

func checkThoughtTextMixed(t *testing.T, c *goldenCtx) {
	t.Helper()
	var thinking, text strings.Builder
	var thinkIdx, textIdx = -1, -1
	for _, ev := range c.events {
		if ev.Kind != ir.EventBlockDelta {
			continue
		}
		switch ev.BlockKind {
		case ir.BlockThinking:
			thinking.WriteString(ev.TextDelta)
			thinkIdx = ev.BlockIndex
		case ir.BlockText:
			text.WriteString(ev.TextDelta)
			textIdx = ev.BlockIndex
		}
	}
	if thinking.String() != "Let me think" {
		t.Fatalf("thinking = %q", thinking.String())
	}
	if text.String() != "Answer more" {
		t.Fatalf("text = %q", text.String())
	}
	if thinkIdx == textIdx || thinkIdx < 0 || textIdx < 0 {
		t.Fatalf("thought/text must be separate blocks: think=%d text=%d", thinkIdx, textIdx)
	}
	u := lastUsage(c.events)
	if u == nil || u.Input != 7 || u.Output != 8 || u.Reasoning != 3 || u.Total != 12 {
		t.Fatalf("usage = %+v", u)
	}
}

func checkPartialArgs(t *testing.T, c *goldenCtx) {
	t.Helper()
	starts := toolStarts(c.events)
	if len(starts) != 1 || starts[0].name != "build_route" || starts[0].id != "call_p" {
		t.Fatalf("tool blocks = %+v", starts)
	}
	args := argsByBlock(c.events)[starts[0].index]
	var got map[string]any
	if err := json.Unmarshal([]byte(args), &got); err != nil {
		t.Fatalf("args = %q: %v", args, err)
	}
	if got["city"] != "Oslo" { // stringValue 追加拼接
		t.Fatalf("city = %v (append failed)", got["city"])
	}
	stops, _ := got["stops"].([]any)
	if len(stops) != 2 || stops[0] != "A" || stops[1] != "B" {
		t.Fatalf("stops = %+v", got["stops"])
	}
	legs, _ := got["legs"].([]any)
	if len(legs) != 1 {
		t.Fatalf("legs = %+v", got["legs"])
	}
	if leg, _ := legs[0].(map[string]any); leg["n"] != float64(3) {
		t.Fatalf("legs[0] = %+v", legs[0])
	}
	if got["ok"] != true {
		t.Fatalf("ok = %v", got["ok"])
	}
	if r, ok := doneReason(c.events); !ok || r != ir.StopToolUse {
		t.Fatalf("done reason = %q (ok=%v)", r, ok)
	}
}

func checkErrorLine(t *testing.T, c *goldenCtx) {
	t.Helper()
	var ee *ir.StreamEvent
	for i := range c.events {
		if c.events[i].Kind == ir.EventError {
			ee = &c.events[i]
		}
	}
	if ee == nil {
		t.Fatalf("no error event: %+v", c.events)
	}
	if ee.ErrorReason != ir.ErrorUpstream {
		t.Fatalf("error reason = %q", ee.ErrorReason)
	}
	if ee.Error == nil || ee.Error.Type != "rate_limit_error" || ee.Error.Code != "429" ||
		ee.Error.Message != "quota exceeded for this key" || ee.Error.Retryable == nil || !*ee.Error.Retryable {
		t.Fatalf("error = %+v", ee.Error)
	}
	if got := collectText(c.events); got != "partial" {
		t.Fatalf("text before error = %q", got)
	}
}

// ---------------------------------------------------------------------------
// 测试工具
// ---------------------------------------------------------------------------

func findMessage(msgs []ir.Message, role ir.Role) *ir.Message {
	for i := range msgs {
		if msgs[i].Role == role {
			return &msgs[i]
		}
	}
	return nil
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

// argsByBlock 收集每个 tool_use 块的参数片段（拼接后的 JSON 文本）。
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

func collectText(events []ir.StreamEvent) string {
	var b strings.Builder
	for _, ev := range events {
		if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockText {
			b.WriteString(ev.TextDelta)
		}
	}
	return b.String()
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
