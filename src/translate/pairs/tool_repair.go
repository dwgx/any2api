package pairs

import (
	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/toolcall"
)

// toolExitStage 出口修复层：P66（toolcall-assembly-spec）出口契约在流式生产
// 路径上的接线。此前 toolcall 包只有 parityrunner（对拍驱动器）引用，生产出口
// （pairs 管道）不经过「7 步 merge / 三层修复 / 出口 `{}` 纪律」——半 JSON
// （max_tokens 截断必现）被原样下发（P53 缺口 17）。
//
// 位置与模式：管道**出口腿**（枢纽两步时在枢纽 parser 之后、出口 writer 之前），
// 模式 B（缓冲到块尾单 delta，规格 §3.1 保守默认）。为什么必须缓冲：三层修复
// 只能在参数完整拼接后判定；逐片转发过的坏 JSON 无法收回（下游按累积语义消费）。
// 模式 B 对合法前缀流同样正确（只是把 N 片合并成一次下发——规格 §5 出口形态表
// 明确允许：Anthropic 单 delta；OpenAI 末尾一次性给完整 JSON 亦可）。
//
// 语义逐条复用 toolcall 包（对拍 oracle 语义不变）：
//   - 合并：MergeToolInputTraced（§4.1 七步决策表：快照 / 重写 / 重复帧归一）；
//   - 出口：Finalize（Bug C 闸门 + 合法直过字节保真 + 三层修复 + 双重编码解包）
//     与 OutletArguments（空参按下游契约 `{}` / 失败态恒不发，§5 出口纪律）；
//   - 归因诊断：Defect 纯观测，绝不进控制流。
//
// 失败态（规格 §4.4 默认策略）：不发坏 JSON、绝不伪造成 `{}`——失败块的
// args delta 与 block_stop **都不下发**（出口 writer 因此没有 pending 可「补
// `{}`」，也绝不静默置空参）；失败记在管道上，流到达自然终态（done）时由本层
// 换成出口格式 error 帧收尾（「收尾顺序：flush（可能置失败态）→ 补 error」）。
//
// 未 stop 的块不做残留 flush：截断流的终态是 transport error（不变量 13「截断即
// 失败」），出口 writer 在 error 终态下不冲刷 pending——半截参数既不补也不发。
type toolExitStage struct {
	policy   toolcall.Policy
	contract toolcall.ConsumerContract

	// slots 按 IR BlockIndex 键控——索引只作 map 键，不驱动分配（W52-1 条款与
	// 回归测试见 ir/assemble.go 头注）。
	slots map[int]*toolExitSlot

	failure  *toolcall.Failure
	defect   *toolcall.Defect
	repaired int
	failed   int
}

type toolExitSlot struct {
	buf string
}

func newToolExitStage(contract toolcall.ConsumerContract) *toolExitStage {
	return &toolExitStage{
		policy:   toolcall.PolicyDefault,
		contract: contract,
		slots:    map[int]*toolExitSlot{},
	}
}

// process 处理一批事件：工具块（tool_use / server_tool_use）的参数增量缓冲到
// 块尾统一校验/修复/下发；其余事件原样透传；有失败态时把终态 done 换成 error。
func (s *toolExitStage) process(events []ir.StreamEvent) []ir.StreamEvent {
	out := make([]ir.StreamEvent, 0, len(events)+1)
	for _, ev := range events {
		switch ev.Kind {
		case ir.EventBlockStart:
			if isToolBlockKind(ev.BlockKind) {
				if _, dup := s.slots[ev.BlockIndex]; !dup {
					s.slots[ev.BlockIndex] = &toolExitSlot{}
				}
			}
			out = append(out, ev)

		case ir.EventBlockDelta:
			slot, ok := s.slots[ev.BlockIndex]
			if !ok || !isToolBlockKind(ev.BlockKind) {
				out = append(out, ev)
				break
			}
			// 7 步 merge（规格 §4.1）：快照 / 重写 / 重复帧在缓冲层归一。
			merged, _ := toolcall.MergeToolInputTraced(slot.buf, ev.ArgsDelta)
			slot.buf = merged // 模式 B：只累积，不下发

		case ir.EventBlockStop:
			slot, ok := s.slots[ev.BlockIndex]
			if !ok {
				out = append(out, ev)
				break
			}
			delete(s.slots, ev.BlockIndex)
			res := toolcall.Finalize(slot.buf, nil, s.policy)
			if s.defect == nil && res.Defect != nil {
				s.defect = res.Defect // 纯观测：修复与失败都留归因
			}
			if res.Failure != nil {
				s.failed++
				if s.failure == nil {
					s.failure = res.Failure
				}
				// 不发 args delta、不发 block_stop：坏 JSON 绝不出口，
				// 也绝不让 writer 的 pending 兜底把失败伪装成空参 `{}`。
				break
			}
			if res.Emitted != nil && *res.Emitted != slot.buf {
				s.repaired++ // 修复 / 解包 / merge 归一改写过最终值
			}
			if outlet, send := toolcall.OutletArguments(res, s.contract); send {
				out = append(out, ir.StreamEvent{
					Kind:       ir.EventBlockDelta,
					BlockIndex: ev.BlockIndex,
					BlockKind:  ev.BlockKind,
					ArgsDelta:  outlet,
				})
			}
			out = append(out, ev)

		case ir.EventDone:
			if s.failure != nil {
				// 收尾补 error（规格 §4.4 默认策略）：终态由成功换成失败，
				// 客户端退避后整轮重试；错误信封不进枢纽二次映射。
				out = append(out, ir.StreamEvent{
					Kind:        ir.EventError,
					ErrorReason: ir.ErrorUpstream,
					Error: &ir.Error{
						Type:    "api_error",
						Code:    s.failure.Code,
						Message: s.failure.Message,
						Source:  "any2api",
					},
				})
				continue
			}
			out = append(out, ev)

		default:
			out = append(out, ev)
		}
	}
	return out
}

// isToolBlockKind 客户端工具调用块（与 toolcall 组装器的过滤同款：hosted /
// server 工具结果块不是客户端工具调用——不占修复面）。
func isToolBlockKind(k ir.BlockKind) bool {
	return k == ir.BlockToolUse || k == ir.BlockServerToolUse
}
