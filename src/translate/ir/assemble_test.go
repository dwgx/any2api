package ir

import (
	"errors"
	"runtime"
	"testing"
)

func TestAssembleToolCalls(t *testing.T) {
	events := []StreamEvent{
		{Kind: EventStart},
		{Kind: EventBlockStart, BlockIndex: 1, BlockKind: BlockToolUse, ToolCallID: "call_b", ToolName: "b"},
		{Kind: EventBlockStart, BlockIndex: 0, BlockKind: BlockToolUse, ToolCallID: "call_a", ToolName: "a"},
		{Kind: EventBlockDelta, BlockIndex: 0, BlockKind: BlockToolUse, ArgsDelta: `{"x":`},
		{Kind: EventBlockDelta, BlockIndex: 0, BlockKind: BlockToolUse, ArgsDelta: `1}`},
		{Kind: EventBlockDelta, BlockIndex: 1, BlockKind: BlockToolUse, ArgsDelta: `{}`},
	}
	calls, err := AssembleToolCalls(events)
	if err != nil {
		t.Fatalf("AssembleToolCalls: %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("calls = %+v", calls)
	}
	// 按块序（BlockIndex 升序），不是出现顺序。
	if calls[0].ID != "call_a" || calls[0].Name != "a" || calls[0].Arguments != `{"x":1}` {
		t.Fatalf("calls[0] = %+v", calls[0])
	}
	if calls[1].ID != "call_b" || calls[1].Arguments != "{}" {
		t.Fatalf("calls[1] = %+v", calls[1])
	}
}

func TestAssembleIncompleteArguments(t *testing.T) {
	// 半 JSON 截断 → 显式错误（修不好不静默）。
	events := []StreamEvent{
		{Kind: EventBlockStart, BlockIndex: 0, BlockKind: BlockToolUse, ToolCallID: "c", ToolName: "f"},
		{Kind: EventBlockDelta, BlockIndex: 0, BlockKind: BlockToolUse, ArgsDelta: `{"x":`},
	}
	_, err := AssembleToolCalls(events)
	if !errors.Is(err, ErrIncompleteToolArguments) {
		t.Fatalf("err = %v, want ErrIncompleteToolArguments", err)
	}
}

func TestAssembleDuplicateBlock(t *testing.T) {
	events := []StreamEvent{
		{Kind: EventBlockStart, BlockIndex: 0, BlockKind: BlockToolUse, ToolCallID: "c1", ToolName: "f"},
		{Kind: EventBlockStart, BlockIndex: 0, BlockKind: BlockToolUse, ToolCallID: "c2", ToolName: "f"},
	}
	if _, err := AssembleToolCalls(events); !errors.Is(err, ErrDuplicateToolBlock) {
		t.Fatalf("err = %v, want ErrDuplicateToolBlock", err)
	}
}

func TestAssembleIgnoresNonToolBlocks(t *testing.T) {
	events := []StreamEvent{
		{Kind: EventBlockStart, BlockIndex: 0, BlockKind: BlockText},
		{Kind: EventBlockDelta, BlockIndex: 0, BlockKind: BlockText, TextDelta: "hi"},
		{Kind: EventBlockStart, BlockIndex: 1, BlockKind: BlockToolUse, ToolCallID: "c", ToolName: "f"},
		{Kind: EventBlockDelta, BlockIndex: 1, BlockKind: BlockToolUse, ArgsDelta: `{"a":1}`},
	}
	calls, err := AssembleToolCalls(events)
	if err != nil {
		t.Fatalf("AssembleToolCalls: %v", err)
	}
	if len(calls) != 1 || calls[0].Arguments != `{"a":1}` {
		t.Fatalf("calls = %+v", calls)
	}
}

func TestAssembleEmptyArgumentsAllowed(t *testing.T) {
	// 空参数（无片段）不报错——出口渲染负责发 "{}"。
	events := []StreamEvent{
		{Kind: EventBlockStart, BlockIndex: 0, BlockKind: BlockToolUse, ToolCallID: "c", ToolName: "f"},
		{Kind: EventBlockStop, BlockIndex: 0, BlockKind: BlockToolUse},
	}
	calls, err := AssembleToolCalls(events)
	if err != nil {
		t.Fatalf("AssembleToolCalls: %v", err)
	}
	if len(calls) != 1 || calls[0].Arguments != "" {
		t.Fatalf("calls = %+v", calls)
	}
}

// TestAssembleToolCallsIndexDoesNotDriveAllocation W52-1（索引不驱动分配条款，
// 见 AssembleToolCalls 头注）：provider 给的巨 BlockIndex（2^20）只作 map 键与
// 排序键——组装分配随事件数线性、不随索引值。
//
// 反证锚点：把 accs 改成按 maxIndex 分配的切片（任何 `make([]…, index…)` 形态）
// → 本测的分配上界断言必红（2^20 项切片 ≥ 8 MiB > 1 MiB 上界）。
func TestAssembleToolCallsIndexDoesNotDriveAllocation(t *testing.T) {
	const huge = 1 << 20
	events := []StreamEvent{
		{Kind: EventBlockStart, BlockIndex: huge + 1, BlockKind: BlockToolUse, ToolCallID: "call_b", ToolName: "b"},
		{Kind: EventBlockStart, BlockIndex: huge, BlockKind: BlockToolUse, ToolCallID: "call_a", ToolName: "a"},
		{Kind: EventBlockDelta, BlockIndex: huge, BlockKind: BlockToolUse, ArgsDelta: `{"x":1}`},
		{Kind: EventBlockDelta, BlockIndex: huge + 1, BlockKind: BlockToolUse, ArgsDelta: `{}`},
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	calls, err := AssembleToolCalls(events)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatalf("AssembleToolCalls: %v", err)
	}
	// 巨索引不影响正确性（排序键 = BlockIndex 本身）。
	if len(calls) != 2 || calls[0].ID != "call_a" || calls[1].ID != "call_b" {
		t.Fatalf("calls = %+v", calls)
	}
	if delta := after.TotalAlloc - before.TotalAlloc; delta > 1<<20 {
		t.Fatalf("BlockIndex 驱动了分配：%d bytes（上界 1 MiB）", delta)
	}
}
