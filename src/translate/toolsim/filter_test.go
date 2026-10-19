package toolsim

// W17 S2（出口腿过滤器 / 非流式变换）单元验收：事件面合成、非契约文本直过、
// 只动 text 块、StopReason 改写、失败态不冲刷半成品。
//
// 反证锚点：删掉 Process 的 StopReason 改写块 → TestToolSimFilterSynthesizesCall 红；
// 把 text 块之外的块也喂进 Scanner → TestToolSimFilterOnlyTextBlocks 红。

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

func evTextStart(idx int) ir.StreamEvent {
	return ir.StreamEvent{Kind: ir.EventBlockStart, BlockIndex: idx, BlockKind: ir.BlockText}
}

func evTextDelta(idx int, s string) ir.StreamEvent {
	return ir.StreamEvent{Kind: ir.EventBlockDelta, BlockIndex: idx, BlockKind: ir.BlockText, TextDelta: s}
}

func evTextStop(idx int) ir.StreamEvent {
	return ir.StreamEvent{Kind: ir.EventBlockStop, BlockIndex: idx, BlockKind: ir.BlockText}
}

// TestToolSimFilterSynthesizesCall 事件面合成：文本 → 调用 → 文本三块串行（Anthropic 线块序），
// StopReason 改写 toolUse，块 index 重编号不撞号。
func TestToolSimFilterSynthesizesCall(t *testing.T) {
	f := NewFilter(nil)
	in := []ir.StreamEvent{
		evTextStart(0),
		evTextDelta(0, `pre <tool_use name="Read">{"file_path":"a.txt"}`),
		evTextDelta(0, `</tool_use> post`),
		evTextStop(0),
		{Kind: ir.EventMessageDelta, StopReason: ir.StopStop},
	}
	out := f.Process(in)

	want := []ir.StreamEvent{
		evTextStart(0),
		evTextDelta(0, "pre "),
		evTextStop(0),
		{Kind: ir.EventBlockStart, BlockIndex: 1, BlockKind: ir.BlockToolUse, ToolName: "Read"},
		{Kind: ir.EventBlockDelta, BlockIndex: 1, BlockKind: ir.BlockToolUse, ArgsDelta: `{"file_path":"a.txt"}`},
		{Kind: ir.EventBlockStop, BlockIndex: 1, BlockKind: ir.BlockToolUse},
		evTextStart(2),
		evTextDelta(2, " post"),
		evTextStop(2),
		{Kind: ir.EventMessageDelta, StopReason: ir.StopToolUse},
	}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("事件序不符:\n got %+v\nwant %+v", out, want)
	}
	if f.Synthesized() != 1 {
		t.Fatalf("Synthesized = %d, want 1", f.Synthesized())
	}
}

// TestToolSimFilterPassthroughNonContract 非契约文本零影响：无标签的 text 流逐事件等于输入
// （含 message_delta 不改写、块 index 重编号但同序）。
func TestToolSimFilterPassthroughNonContract(t *testing.T) {
	f := NewFilter(nil)
	in := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "m1", Model: "x"},
		evTextStart(0),
		evTextDelta(0, "plain "),
		evTextDelta(0, "answer"),
		evTextStop(0),
		{Kind: ir.EventMessageDelta, StopReason: ir.StopStop},
		{Kind: ir.EventMessageStop},
	}
	out := f.Process(in)
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("非契约文本被改动:\n got %+v\nwant %+v", out, in)
	}
	if f.Synthesized() != 0 {
		t.Fatalf("Synthesized = %d, want 0", f.Synthesized())
	}
}

// summary 事件流 → 归一化摘要（合并同块相邻 delta——跨片切分的 delta 粒度是实现
// 细节；结构、内容、工具调用与 StopReason 才是断言对象）。
func summary(events []ir.StreamEvent) []string {
	var out []string
	push := func(s string) { out = append(out, s) }
	for _, ev := range events {
		switch ev.Kind {
		case ir.EventBlockStart:
			push("start/" + string(ev.BlockKind) + "/" + itoa(ev.BlockIndex) + "/" + ev.ToolName)
		case ir.EventBlockDelta:
			key := "delta/" + string(ev.BlockKind) + "/" + itoa(ev.BlockIndex) + "="
			payload := ev.TextDelta + ev.ArgsDelta
			if n := len(out); n > 0 && strings.HasPrefix(out[n-1], key) {
				out[n-1] += payload
			} else {
				push(key + payload)
			}
		case ir.EventBlockStop:
			push("stop/" + string(ev.BlockKind) + "/" + itoa(ev.BlockIndex))
		case ir.EventMessageDelta:
			push("msgdelta/" + string(ev.StopReason))
		default:
			push(string(ev.Kind))
		}
	}
	return out
}

func itoa(n int) string { return strconv.Itoa(n) }

// TestToolSimFilterChunkedDeltas 跨片切割：同一标签切成任意多片，归一化结果与单片一致。
func TestToolSimFilterChunkedDeltas(t *testing.T) {
	text := `pre <tool_use name="Read">{"file_path":"a.txt"}</tool_use> post`

	run := func(deltas []string) []ir.StreamEvent {
		f := NewFilter(nil)
		evs := []ir.StreamEvent{evTextStart(0)}
		for _, d := range deltas {
			evs = append(evs, evTextDelta(0, d))
		}
		evs = append(evs, evTextStop(0), ir.StreamEvent{Kind: ir.EventMessageDelta, StopReason: ir.StopStop})
		return f.Process(evs)
	}

	var deltas []string
	for i := range len(text) {
		deltas = append(deltas, text[i:i+1])
	}
	got := summary(run(deltas))
	want := summary(run([]string{text}))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("跨片结果 != 整片:\n got %q\nwant %q", got, want)
	}
	// 合成块确实存在（防止两边都空转假绿）。
	if !reflect.DeepEqual(got, []string{
		"start/text/0/", "delta/text/0=pre ", "stop/text/0",
		"start/tool_use/1/Read", "delta/tool_use/1={\"file_path\":\"a.txt\"}", "stop/tool_use/1",
		"start/text/2/", "delta/text/2= post", "stop/text/2",
		"msgdelta/toolUse",
	}) {
		t.Fatalf("归一化摘要 = %q", got)
	}
}

// TestToolSimFilterOnlyTextBlocks thinking / 服务端工具块零接触：标签样文本在 thinking 块里
// 原样过桥（只重编号），不合成调用。
func TestToolSimFilterOnlyTextBlocks(t *testing.T) {
	f := NewFilter(nil)
	think := `<tool_use name="Read">{"a":1}</tool_use>`
	in := []ir.StreamEvent{
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockThinking},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockThinking, TextDelta: think},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockThinking},
		{Kind: ir.EventBlockStart, BlockIndex: 1, BlockKind: ir.BlockServerToolUse, ToolName: "bash_tool"},
		{Kind: ir.EventBlockDelta, BlockIndex: 1, BlockKind: ir.BlockServerToolUse, ArgsDelta: "{}"},
		{Kind: ir.EventBlockStop, BlockIndex: 1, BlockKind: ir.BlockServerToolUse},
	}
	out := f.Process(in)
	want := []ir.StreamEvent{
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockThinking},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockThinking, TextDelta: think},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockThinking},
		{Kind: ir.EventBlockStart, BlockIndex: 1, BlockKind: ir.BlockServerToolUse, ToolName: "bash_tool"},
		{Kind: ir.EventBlockDelta, BlockIndex: 1, BlockKind: ir.BlockServerToolUse, ArgsDelta: "{}"},
		{Kind: ir.EventBlockStop, BlockIndex: 1, BlockKind: ir.BlockServerToolUse},
	}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("非 text 块被改动:\n got %+v\nwant %+v", out, want)
	}
	if f.Synthesized() != 0 {
		t.Fatalf("thinking 块不得合成调用")
	}
}

// TestToolSimFilterErrorDropsPending 失败态：未判定尾巴不冲刷（截断即失败；半成品既不补也不发）。
// 已判定的文本（`pre `）已下发，未判定的标签尾巴丢弃。
func TestToolSimFilterErrorDropsPending(t *testing.T) {
	f := NewFilter(nil)
	in := []ir.StreamEvent{
		evTextStart(0),
		evTextDelta(0, `pre <tool_use name="Read">{"a":1}`),
		{Kind: ir.EventError, ErrorReason: ir.ErrorTransport},
	}
	out := f.Process(in)
	for _, ev := range out {
		if strings.Contains(ev.TextDelta+ev.ArgsDelta, "tool_use") {
			t.Fatalf("失败态冲刷了半成品: %+v", out)
		}
	}
	if n := len(out); n != 3 || out[n-1].Kind != ir.EventError {
		t.Fatalf("失败态事件序 = %+v", out)
	}
	if out[1].TextDelta != "pre " {
		t.Fatalf("已判定文本丢失: %+v", out[1])
	}
}

// TestToolSimFilterNameRestore 合成工具名走净化名→原名还原（与既有事件面还原同表）。
func TestToolSimFilterNameRestore(t *testing.T) {
	f := NewFilter(map[string]string{"mcp_foo_bar": "mcp__foo__bar"})
	in := []ir.StreamEvent{
		evTextStart(0),
		evTextDelta(0, `<tool_use name="mcp_foo_bar">{}</tool_use>`),
		evTextStop(0),
	}
	out := f.Process(in)
	found := ""
	for _, ev := range out {
		if ev.Kind == ir.EventBlockStart && ev.BlockKind == ir.BlockToolUse {
			found = ev.ToolName
		}
	}
	if found != "mcp__foo__bar" {
		t.Fatalf("ToolName = %q, want 原名", found)
	}
}

// TestTransformResponseNonStream 非流式变换：文本块标签 → tool_use 块 + StopReason 改写；
// 无标签响应原样（返回 0，块与 StopReason 不动）。
func TestToolSimTransformResponseNonStream(t *testing.T) {
	resp := &ir.Response{
		Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
			{Kind: ir.BlockText, Text: `let me look <tool_use name="Read">{"file_path":"a.txt"}</tool_use> done`},
		}},
		StopReason: ir.StopStop,
	}
	if n := TransformResponse(resp); n != 1 {
		t.Fatalf("合成数 = %d, want 1", n)
	}
	blocks := resp.Message.Blocks
	if len(blocks) != 3 || blocks[0].Kind != ir.BlockText || blocks[0].Text != "let me look " ||
		blocks[1].Kind != ir.BlockToolUse || blocks[1].ToolCall == nil ||
		blocks[1].ToolCall.Name != "Read" || blocks[1].ToolCall.Arguments != `{"file_path":"a.txt"}` ||
		blocks[2].Kind != ir.BlockText || blocks[2].Text != " done" {
		t.Fatalf("块变换 = %+v", blocks)
	}
	if resp.StopReason != ir.StopToolUse {
		t.Fatalf("StopReason = %q, want toolUse", resp.StopReason)
	}

	plain := &ir.Response{
		Message:    ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "plain answer"}}},
		StopReason: ir.StopStop,
	}
	before := plain.Message.Blocks
	if n := TransformResponse(plain); n != 0 {
		t.Fatalf("无标签响应被改动（n=%d）", n)
	}
	if !reflect.DeepEqual(plain.Message.Blocks, before) || plain.StopReason != ir.StopStop {
		t.Fatalf("无标签响应不得改动: %+v", plain)
	}
}
