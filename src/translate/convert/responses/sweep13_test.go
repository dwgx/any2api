// W5-30 条目 1：responses 出口补 SDK 必填三键——parallel_tool_calls /
// tool_choice / tools。
//
// 依据：P01（openai-python 3.29.0 实测 Response 必填 8 键 =
// id/created_at/model/object/output + 三键；spec Response.required 15 键含三键）。
// 取值纪律：IR Extra 有则原样回填（保真），否则 spec 默认——parallel_tool_calls
// 为 true（有显式 default）、tool_choice 为 "auto"、tools 为 []（后两者取 spec
// 自证示例）。流式快照无 IR 通道 → 恒 spec 默认。
//
// 反证锚点：删掉 render.go / stream_writer.go 的三键补齐 → 本文件红；
// lane 探针 tmp/lanes/L19-sweep13/probe_sdk_validate.py --expect-missing 3 报 3 missing。
package responses

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// requiredKeyDocs 非流式出口文档的顶层对象。
func requiredKeyDocs(t *testing.T, out []byte) map[string]json.RawMessage {
	t.Helper()
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	return doc
}

// TestSweep13SDKRequiredKeysNonStream 非流式出口三键：
//   - 默认分支：IR 无 Extra → spec 默认（true/"auto"/[]）；
//   - 保真分支：IR Extra 有则用（含 null 视同缺省）；
//   - 渲染不动点：parse(render(x)) 再渲染语义相等（三键保持）。
func TestSweep13SDKRequiredKeysNonStream(t *testing.T) {
	base := func() *ir.Response {
		return &ir.Response{
			ID:      "resp_s13",
			Model:   "gpt-5",
			Created: 1741293000,
			Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
				{Kind: ir.BlockText, Text: "hi"},
			}},
		}
	}

	// 默认分支。
	out, err := RenderResponse(base())
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	doc := requiredKeyDocs(t, out)
	if string(doc["parallel_tool_calls"]) != "true" {
		t.Fatalf("parallel_tool_calls = %s, want true", doc["parallel_tool_calls"])
	}
	if string(doc["tool_choice"]) != `"auto"` {
		t.Fatalf("tool_choice = %s, want \"auto\"", doc["tool_choice"])
	}
	if string(doc["tools"]) != "[]" {
		t.Fatalf("tools = %s, want []", doc["tools"])
	}

	// 保真分支：Extra 有则用。
	r := base()
	r.Extra = map[string]json.RawMessage{
		"parallel_tool_calls": json.RawMessage("false"),
		"tool_choice":         json.RawMessage(`{"type":"function","name":"get_weather"}`),
		"tools":               json.RawMessage(`[{"type":"function","name":"get_weather"}]`),
	}
	out, err = RenderResponse(r)
	if err != nil {
		t.Fatalf("RenderResponse(extra): %v", err)
	}
	doc = requiredKeyDocs(t, out)
	if string(doc["parallel_tool_calls"]) != "false" ||
		string(doc["tool_choice"]) != `{"type":"function","name":"get_weather"}` ||
		string(doc["tools"]) != `[{"type":"function","name":"get_weather"}]` {
		t.Fatalf("Extra 回填失效: %s", out)
	}

	// null Extra 视同缺省 → 默认值（不把 null 当值回填）。
	r = base()
	r.Extra = map[string]json.RawMessage{
		"parallel_tool_calls": json.RawMessage("null"),
		"tool_choice":         json.RawMessage("null"),
		"tools":               json.RawMessage("null"),
	}
	out, err = RenderResponse(r)
	if err != nil {
		t.Fatalf("RenderResponse(null extra): %v", err)
	}
	doc = requiredKeyDocs(t, out)
	if string(doc["parallel_tool_calls"]) != "true" || string(doc["tool_choice"]) != `"auto"` ||
		string(doc["tools"]) != "[]" {
		t.Fatalf("null Extra 未被视同缺省: %s", out)
	}

	// 渲染不动点：parse(render) 再 render 语义相等。
	resp2, err := ParseResponse(out)
	if err != nil {
		t.Fatalf("re-ParseResponse: %v\n%s", err, out)
	}
	out2, err := RenderResponse(resp2)
	if err != nil {
		t.Fatalf("re-RenderResponse: %v", err)
	}
	assertJSONEqual(t, out, out2)
}

// TestSweep13SDKRequiredKeysStreamSnapshots 流式出口每个 Response 快照
// （created / in_progress / completed / incomplete）都带三键（spec 默认）。
func TestSweep13SDKRequiredKeysStreamSnapshots(t *testing.T) {
	run := func(stop ir.StopReason) string {
		t.Helper()
		events := []ir.StreamEvent{
			{Kind: ir.EventStart, MessageID: "resp_s13s", Model: "gpt-5", Created: 1741293000},
			{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockText},
			{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockText, TextDelta: "hi"},
			{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockText},
			{Kind: ir.EventMessageDelta, StopReason: stop},
			{Kind: ir.EventMessageStop},
			{Kind: ir.EventDone, Reason: stop},
		}
		out, err := RenderStream(events)
		if err != nil {
			t.Fatalf("RenderStream(%s): %v", stop, err)
		}
		return string(out)
	}

	for _, stop := range []ir.StopReason{ir.StopStop, ir.StopLength} {
		sse := run(stop)
		snapshots := 0
		for _, frame := range strings.Split(sse, "\n\n") {
			var payload string
			for _, line := range strings.Split(frame, "\n") {
				if strings.HasPrefix(line, "data: ") {
					payload = strings.TrimPrefix(line, "data: ")
				}
			}
			if payload == "" {
				continue
			}
			var doc struct {
				Response json.RawMessage `json:"response"`
			}
			if err := json.Unmarshal([]byte(payload), &doc); err != nil || len(doc.Response) == 0 {
				continue
			}
			var resp map[string]json.RawMessage
			if err := json.Unmarshal(doc.Response, &resp); err != nil {
				t.Fatalf("unmarshal snapshot: %v", err)
			}
			if string(resp["parallel_tool_calls"]) != "true" ||
				string(resp["tool_choice"]) != `"auto"` || string(resp["tools"]) != "[]" {
				t.Fatalf("stop=%s 快照缺三键: %s", stop, doc.Response)
			}
			snapshots++
		}
		if snapshots < 3 { // created + in_progress + 终止快照
			t.Fatalf("stop=%s 快照数 = %d, want >= 3:\n%s", stop, snapshots, sse)
		}
	}
}
