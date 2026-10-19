// W5-35：claude 出口响应 id 合成——上游不给响应 id（IR MessageID 空）→
// `msg_synth_<32hex>`（每响应唯一），并作为合成工具 id 的派生 seed（响应内确定 +
// 跨响应唯一）。旧行为：空 id 直接进 wire 的 `id` 字段，工具 id 的 seed 为空 →
// 回落块序形态 `toolu_synth_<n>`（跨响应重复——W5-29 报告点名的残余；W5-31 只修
// 了 openai_chat 面）。
//
// 活体依据（W5-35 lane L19-demo2）：链首假上游不带 `id` 时，claude 入口
// message_start 原样输出 `"id":""`（组合 31 探针 `transcript/_probe/claude.txt`）。
// 修后 0 空 id。
//
// 反证锚点：删掉 RenderResponse / StreamWriter 的 id 合成 → 本文件红。
package claude_messages

import (
	"encoding/json"
	"regexp"
	"testing"

	"github.com/dwgx/any2api/translate/hygiene"
	"github.com/dwgx/any2api/translate/ir"
)

var sweep18SynthID = regexp.MustCompile(`^msg_synth_[0-9a-f]{32}$`)

// sweep18Render 渲染一个「上游无 id + 无工具 id」的响应，回读 wire 的响应 id 与
// 首个 tool_use 块 id。
func sweep18Render(t *testing.T, respID string) (out []byte, id, toolID string) {
	t.Helper()
	r := &ir.Response{
		ID:    respID,
		Model: "m",
		Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
			{Kind: ir.BlockText, Text: "hi"},
			{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{ID: "", Name: "f", Arguments: "{}"}},
		}},
	}
	out, err := RenderResponse(r)
	if err != nil {
		t.Fatalf("RenderResponse(%q): %v", respID, err)
	}
	var doc struct {
		ID      string `json:"id"`
		Content []struct {
			Type string `json:"type"`
			ID   string `json:"id"`
		} `json:"content"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	if len(doc.Content) != 2 || doc.Content[1].Type != "tool_use" {
		t.Fatalf("content 形状: %s", out)
	}
	return out, doc.ID, doc.Content[1].ID
}

// TestSweep18ResponseIDSynthNonStream 非流式面：IR 响应 id 空 → 合成
// `msg_synth_<32hex>`（每响应唯一）+ 工具 id = toolu_synth_<sha256(id␀1)[:16]hex>；
// 上游有 id → 原样透传；渲染不动点（合成 id 经 parse 往返稳定）。
func TestSweep18ResponseIDSynthNonStream(t *testing.T) {
	out1, id1, tool1 := sweep18Render(t, "")
	if !sweep18SynthID.MatchString(id1) {
		t.Fatalf("id = %q（want msg_synth_<32hex>）", id1)
	}
	if want := hygiene.SynthDerivedID("toolu", id1, 1); tool1 != want {
		t.Fatalf("tool id = %q, want %q（响应 id 派生）", tool1, want)
	}

	// 每响应唯一：同 IR（id 空）两次渲染不得同 id。
	_, id2, _ := sweep18Render(t, "")
	if !sweep18SynthID.MatchString(id2) || id2 == id1 {
		t.Fatalf("id2 = %q（want 另一次 msg_synth；旧值 %q）", id2, id1)
	}

	// 上游有 id → 透传（不合成、不改写）。
	_, id3, _ := sweep18Render(t, "msg_up")
	if id3 != "msg_up" {
		t.Fatalf("id3 = %q（上游 id 必须透传）", id3)
	}

	// 渲染不动点：parse(render(x)) 再渲染语义相等（合成 id 经 wire 往返稳定）。
	resp, err := ParseResponse(out1)
	if err != nil {
		t.Fatalf("re-ParseResponse: %v\n%s", err, out1)
	}
	if resp.ID != id1 {
		t.Fatalf("parse 回读 id = %q, want %q", resp.ID, id1)
	}
	out4, err := RenderResponse(resp)
	if err != nil {
		t.Fatalf("re-RenderResponse: %v", err)
	}
	assertJSONEqual(t, out1, out4)
}

// TestSweep18ResponseIDSynthStream 流式面：IR 事件 MessageID 空 → message_start
// 的 id 合成（`msg_synth_<32hex>`，每响应唯一）+ 工具块 id 以其为 seed 派生；
// 上游有 id → 原样透传（回归）。
func TestSweep18ResponseIDSynthStream(t *testing.T) {
	events := func(msgID string) []ir.StreamEvent {
		return []ir.StreamEvent{
			{Kind: ir.EventStart, MessageID: msgID, Model: "m"},
			{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockToolUse, ToolName: "f"},
			{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockToolUse, ArgsDelta: "{}"},
			{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockToolUse},
			{Kind: ir.EventMessageDelta, StopReason: ir.StopToolUse},
			{Kind: ir.EventMessageStop},
		}
	}
	read := func(t *testing.T, out []byte) (msgID, toolID string) {
		t.Helper()
		frames := claudeFrames(t, out)
		if len(frames) < 2 || frames[0].event != "message_start" || frames[1].event != "content_block_start" {
			t.Fatalf("frames = %+v\n%s", frames, out)
		}
		var start struct {
			Message struct {
				ID string `json:"id"`
			} `json:"message"`
		}
		if err := json.Unmarshal([]byte(frames[0].data), &start); err != nil {
			t.Fatalf("message_start: %v", err)
		}
		var block struct {
			ContentBlock struct {
				Type string `json:"type"`
				ID   string `json:"id"`
			} `json:"content_block"`
		}
		if err := json.Unmarshal([]byte(frames[1].data), &block); err != nil {
			t.Fatalf("content_block_start: %v", err)
		}
		if block.ContentBlock.Type != "tool_use" {
			t.Fatalf("block 形状: %s", frames[1].data)
		}
		return start.Message.ID, block.ContentBlock.ID
	}

	out1, err := RenderStream(events(""))
	if err != nil {
		t.Fatalf("RenderStream: %v", err)
	}
	id1, tool1 := read(t, out1)
	if !sweep18SynthID.MatchString(id1) {
		t.Fatalf("message_start id = %q（want msg_synth_<32hex>）", id1)
	}
	if want := hygiene.SynthDerivedID("toolu", id1, 0); tool1 != want {
		t.Fatalf("tool id = %q, want %q（响应 id 派生）", tool1, want)
	}

	// 每响应唯一。
	out2, err := RenderStream(events(""))
	if err != nil {
		t.Fatalf("RenderStream(2): %v", err)
	}
	id2, _ := read(t, out2)
	if !sweep18SynthID.MatchString(id2) || id2 == id1 {
		t.Fatalf("id2 = %q（want 另一次 msg_synth；旧值 %q）", id2, id1)
	}

	// 上游有 id → 透传。
	out3, err := RenderStream(events("msg_up"))
	if err != nil {
		t.Fatalf("RenderStream(3): %v", err)
	}
	id3, _ := read(t, out3)
	if id3 != "msg_up" {
		t.Fatalf("id3 = %q（上游 id 必须透传）", id3)
	}
}
