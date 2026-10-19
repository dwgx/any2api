package ir

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// 组装器错误哨兵（修不好不静默——KiroStudio repair 契约 / arch-options §2.3 块3）。
var (
	// ErrIncompleteToolArguments 合并后的参数不是完整 JSON（半 JSON 截断）。
	ErrIncompleteToolArguments = errors.New("any2api/ir: incomplete tool arguments JSON")
	// ErrDuplicateToolBlock 同一块索引重复 block_start（IR 流自身损坏）。
	ErrDuplicateToolBlock = errors.New("any2api/ir: duplicate tool_use block_start")
)

// AssembleToolCalls 把流式事件中的 tool_use 块合并为完整 ToolCall 列表
// （块3 组装三模式之①：增量 JSON 片段按 index 合并；block_stop 无条件终解析且权威）。
//
// 索引不驱动分配（W52-1 条款；照 GoModel `maxAssembledContentParts=256` 范式）：
// provider JSON 索引（BlockIndex ← 各线的 content_index/index 直通值）只作 map 键
// 与排序键——**绝不作切片下标或 make 容量**：分配随事件/块数量线性，不随索引值
// （防御 `index: 2^31-1` 类输入触发巨量分配/溢出）。本仓各解析器/渲染器的块状态
// 同款（一律 map[int]…：claude_web `open`、claude_messages `startedBlocks`/
// `wireIndex`、pairs `toolExitStage.slots`、responses `blocks`/`byIndex` 等）；
// 唯一「索引驱动分配」点在 gemini partialArgs 数组构造，已由
// `maxPartialArgArrayIndex` 显式封顶（gemini/schema.go）。回归测试：
// assemble_test.go TestAssembleToolCallsIndexDoesNotDriveAllocation（巨索引 +
// 分配上界断言——改成按索引分配必红）。
//
// 纪律：
//   - 片段只做拼接，不做启发式修补（repair 三层属后续 slice 的出口修复层）；
//   - 合并结果必须通过 json.Valid（空串视为无参数，允许——出口渲染为 "{}"）；
//   - 不完整 → ErrIncompleteToolArguments（调用方决定降级/报错，不静默）。
//
// 返回按 BlockIndex 升序（= 块序），流被截断时返回已完成的部分但同样校验。
func AssembleToolCalls(events []StreamEvent) ([]ToolCall, error) {
	type acc struct {
		blockIndex int
		id, name   string
		args       strings.Builder
	}
	accs := map[int]*acc{}
	for _, ev := range events {
		switch ev.Kind {
		case EventBlockStart:
			if ev.BlockKind != BlockToolUse && ev.BlockKind != BlockServerToolUse {
				continue
			}
			if _, dup := accs[ev.BlockIndex]; dup {
				return nil, fmt.Errorf("%w (block %d)", ErrDuplicateToolBlock, ev.BlockIndex)
			}
			accs[ev.BlockIndex] = &acc{blockIndex: ev.BlockIndex, id: ev.ToolCallID, name: ev.ToolName}
		case EventBlockDelta:
			a := accs[ev.BlockIndex]
			if a == nil || (ev.BlockKind != BlockToolUse && ev.BlockKind != BlockServerToolUse) {
				continue // 非 tool_use 块的 delta（text/thinking）不参与工具组装
			}
			a.args.WriteString(ev.ArgsDelta)
		}
	}

	idx := make([]int, 0, len(accs))
	for i := range accs {
		idx = append(idx, i)
	}
	sort.Ints(idx)

	calls := make([]ToolCall, 0, len(idx))
	for _, i := range idx {
		a := accs[i]
		args := a.args.String()
		if args != "" && !json.Valid([]byte(args)) {
			return nil, fmt.Errorf("%w (tool %q block %d, %d bytes)", ErrIncompleteToolArguments, a.name, a.blockIndex, len(args))
		}
		calls = append(calls, ToolCall{ID: a.id, Name: a.name, Arguments: args})
	}
	return calls, nil
}
