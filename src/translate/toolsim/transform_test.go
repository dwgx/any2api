package toolsim

// W21-2 ③ 非流式面（TransformResponse）边界补测：多块 / 坏 JSON / 未闭合 / 空参——
// 确保与流式面行为一致（同一契约两条路：Scanner/Filter ↔ TransformResponse
// 同词表同容错）。
//
// 反证锚点：TransformResponse 去掉 StopReason 改写 → 多块/空参用例红；把
// 未判定尾巴（Flush 前的半成品）也合成、或放宽 validArgs → 坏 JSON/未闭合用例红。

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// TestTransformResponseBoundaryMultiBlock 多块：多个 text 块各自扫描（块边界 =
// 判据边界）、块间 thinking 零接触、块内多调用串行合成、StopReason 改写。
func TestTransformResponseBoundaryMultiBlock(t *testing.T) {
	think := `<tool_use name="X">{"n":9}</tool_use>`
	resp := &ir.Response{
		Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
			{Kind: ir.BlockText, Text: `call one: <tool_use name="A">{"n":1}</tool_use>`},
			{Kind: ir.BlockThinking, Text: think},
			{Kind: ir.BlockText, Text: "plain middle"},
			{Kind: ir.BlockText, Text: `two: <tool_use name="B">{}</tool_use> and <tool_use name="C">{"v":"</tool_use>"}</tool_use> done`},
		}},
		StopReason: ir.StopStop,
	}
	if n := TransformResponse(resp); n != 3 {
		t.Fatalf("合成数 = %d, want 3", n)
	}
	want := []ir.ContentBlock{
		{Kind: ir.BlockText, Text: "call one: "},
		{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{Name: "A", Arguments: `{"n":1}`}},
		{Kind: ir.BlockThinking, Text: think},
		{Kind: ir.BlockText, Text: "plain middle"},
		{Kind: ir.BlockText, Text: "two: "},
		{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{Name: "B", Arguments: "{}"}},
		{Kind: ir.BlockText, Text: " and "},
		{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{Name: "C", Arguments: `{"v":"</tool_use>"}`}},
		{Kind: ir.BlockText, Text: " done"},
	}
	if !reflect.DeepEqual(resp.Message.Blocks, want) {
		t.Fatalf("块变换 = %+v", resp.Message.Blocks)
	}
	if resp.StopReason != ir.StopToolUse {
		t.Fatalf("StopReason = %q, want toolUse", resp.StopReason)
	}
}

// TestTransformResponseBoundaryBadJSON 坏 JSON：不合成块、响应逐字节不动
// （含 StopReason）——降级绝不吞内容。
func TestTransformResponseBoundaryBadJSON(t *testing.T) {
	resp := &ir.Response{
		Message:    ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: `oops <tool_use name="Read">{not json</tool_use> tail`}}},
		StopReason: ir.StopStop,
	}
	before := append([]ir.ContentBlock(nil), resp.Message.Blocks...)
	if n := TransformResponse(resp); n != 0 {
		t.Fatalf("合成数 = %d, want 0", n)
	}
	if !reflect.DeepEqual(resp.Message.Blocks, before) || resp.StopReason != ir.StopStop {
		t.Fatalf("坏 JSON 不得改动响应：%+v stop=%q", resp.Message.Blocks, resp.StopReason)
	}
}

// TestTransformResponseBoundaryUnclosed 未闭合：Flush 时整段文本降级——响应不动。
func TestTransformResponseBoundaryUnclosed(t *testing.T) {
	resp := &ir.Response{
		Message:    ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: `<tool_use name="Read">{"a":1}`}}},
		StopReason: ir.StopStop,
	}
	if n := TransformResponse(resp); n != 0 {
		t.Fatalf("合成数 = %d, want 0", n)
	}
	if resp.Message.Blocks[0].Kind != ir.BlockText || resp.Message.Blocks[0].Text != `<tool_use name="Read">{"a":1}` {
		t.Fatalf("未闭合块不得改动：%+v", resp.Message.Blocks[0])
	}
	if resp.StopReason != ir.StopStop {
		t.Fatalf("StopReason = %q, want stop", resp.StopReason)
	}
}

// TestTransformResponseBoundaryEmptyArgs 空参：合成块 Arguments 为空串（无参语义，
// 出口 `{}` 纪律接管）——与流式面 Filter 的空参处置同判。
func TestTransformResponseBoundaryEmptyArgs(t *testing.T) {
	resp := &ir.Response{
		Message:    ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: `go <tool_use name="Read"></tool_use> now`}}},
		StopReason: ir.StopStop,
	}
	if n := TransformResponse(resp); n != 1 {
		t.Fatalf("合成数 = %d, want 1", n)
	}
	blocks := resp.Message.Blocks
	if len(blocks) != 3 || blocks[1].Kind != ir.BlockToolUse || blocks[1].ToolCall == nil ||
		blocks[1].ToolCall.Name != "Read" || blocks[1].ToolCall.Arguments != "" {
		t.Fatalf("空参合成 = %+v", blocks)
	}
	if blocks[0].Text != "go " || blocks[2].Text != " now" {
		t.Fatalf("前后文本 = %q / %q", blocks[0].Text, blocks[2].Text)
	}
	if resp.StopReason != ir.StopToolUse {
		t.Fatalf("StopReason = %q, want toolUse", resp.StopReason)
	}
}

// TestTransformResponseErrorStopPreserved error 语义优先：StopReason=error 时合成
// 块照出、StopReason 不改写（与流式面 Filter 同判）。
func TestTransformResponseErrorStopPreserved(t *testing.T) {
	resp := &ir.Response{
		Message:    ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: `<tool_use name="Read">{"a":1}</tool_use>`}}},
		StopReason: ir.StopError,
	}
	if n := TransformResponse(resp); n != 1 {
		t.Fatalf("合成数 = %d, want 1", n)
	}
	if resp.StopReason != ir.StopError {
		t.Fatalf("StopReason = %q, want error（error 语义优先，不改写）", resp.StopReason)
	}
}

// TestTransformResponseBlockBoundaryIsJudgmentBoundary 块边界 = 判据边界：块尾
// 未闭合的标签不跨块拼接（Flush 降级），下一块独立扫描。
func TestTransformResponseBlockBoundaryIsJudgmentBoundary(t *testing.T) {
	resp := &ir.Response{
		Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
			{Kind: ir.BlockText, Text: `<tool_use name="A">{"a":1}`},
			{Kind: ir.BlockText, Text: `</tool_use> tail`},
		}},
		StopReason: ir.StopStop,
	}
	if n := TransformResponse(resp); n != 0 {
		t.Fatalf("合成数 = %d, want 0（标签不跨块）", n)
	}
	if resp.Message.Blocks[0].Text != `<tool_use name="A">{"a":1}` || resp.Message.Blocks[1].Text != `</tool_use> tail` {
		t.Fatalf("块内容被改动：%+v", resp.Message.Blocks)
	}
}

// TestTransformResponseNilAndNoText nil / 无 text 块 → 0（防御面，不动响应）。
func TestTransformResponseNilAndNoText(t *testing.T) {
	if n := TransformResponse(nil); n != 0 {
		t.Fatalf("nil 响应 = %d, want 0", n)
	}
	resp := &ir.Response{
		Message:    ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{{Kind: ir.BlockThinking, Text: "x"}}},
		StopReason: ir.StopStop,
	}
	if n := TransformResponse(resp); n != 0 || resp.StopReason != ir.StopStop {
		t.Fatalf("无 text 块响应被改动：n=%d stop=%q", n, resp.StopReason)
	}
}

// TestTransformResponseMatchesStreamingFace 同一契约两条路：同一输入的
// 非流式变换（TransformResponse）与流式过滤器（Filter）——调用序列（名 + 参数
// 逐字节）与文本拼接必须一致。
func TestTransformResponseMatchesStreamingFace(t *testing.T) {
	samples := []string{
		`pre <tool_use name="Read">{"file_path":"a.txt"}</tool_use> post`,
		`<tool_use name="Bash">{"command":"echo '</tool_use>'"}</tool_use>`,
		`oops <tool_use name="Read">{not json</tool_use> tail`,
		`<tool_use name="Read">{"a":1}`,
		`<tool_use name="Read"></tool_use>`,
		`<tool_use name="A">{}</tool_use> mid <tool_use name="B">{"x":1}</tool_use>`,
		"plain answer without tags",
	}
	for _, in := range samples {
		// 非流式面。
		resp := &ir.Response{
			Message:    ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: in}}},
			StopReason: ir.StopStop,
		}
		TransformResponse(resp)
		var nsCalls []string
		var nsText strings.Builder
		for _, b := range resp.Message.Blocks {
			if b.Kind == ir.BlockToolUse && b.ToolCall != nil {
				nsCalls = append(nsCalls, b.ToolCall.Name+"|"+b.ToolCall.Arguments)
			} else {
				nsText.WriteString(b.Text)
			}
		}

		// 流式面（单 delta 进 Filter）。
		f := NewFilter(nil)
		events := f.Process([]ir.StreamEvent{
			{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockText},
			{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockText, TextDelta: in},
			{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockText},
			{Kind: ir.EventMessageDelta, StopReason: ir.StopStop},
		})
		var stCalls []string
		var stText strings.Builder
		for _, ev := range events {
			switch {
			case ev.Kind == ir.EventBlockStart && ev.BlockKind == ir.BlockToolUse:
				stCalls = append(stCalls, ev.ToolName+"|")
			case ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockToolUse:
				stCalls[len(stCalls)-1] += ev.ArgsDelta
			case ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockText:
				stText.WriteString(ev.TextDelta)
			}
		}

		if !reflect.DeepEqual(nsCalls, stCalls) {
			t.Fatalf("%q 两路调用不一致：\n 非流式 %q\n 流式   %q", in, nsCalls, stCalls)
		}
		if nsText.String() != stText.String() {
			t.Fatalf("%q 两路文本不一致：%q != %q", in, nsText.String(), stText.String())
		}
	}
}
