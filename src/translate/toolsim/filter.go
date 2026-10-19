package toolsim

import (
	"github.com/dwgx/any2api/translate/ir"
)

// Filter IR 事件级流式过滤器（S2 出口腿）：把 text 块里的契约标签合成回 tool_use 块。
//
// 接线位置 = pairs 出口腿 **toolExitStage 之前**（设计单 §S2）：合成块的参数照走
// 既有 merge / 三层修复 / `{}` 收口纪律（pairs/tool_repair.go 免费复用）。
//
// 事件面行为：
//
//   - text 块：deltas 进 Scanner；文本片段原样下发（TextDelta），调用片段 →
//     关当前 text 块 → tool_use 块（start + args delta + stop）；标签之后若还有文本，
//     另开一个 text 块——Anthropic 线块序严格串行，绝不交错；
//   - 块 index 全量重编号：插入块不撞原 index；出口 writer 按到达序分配 wire index，
//     重编号对下游字节零影响；
//   - 其余块（thinking / 服务端工具块 / 工具结果块）零接触（只随重编号过桥）；
//   - message_delta 的 StopReason：本轮合成过工具块 → 改写 StopToolUse（Anthropic
//     语义）；error 语义优先，不改写；
//   - 终态（message_stop / done）前兜底：残留尾巴按文本降级并闭合未停块；
//     error 终态：半成品既不补也不发（不变量 13「截断即失败」）。
type Filter struct {
	sc    Scanner
	names map[string]string // 净化名 → 原名（toolname.Reverse；nil/空 = 直过）
	idx   map[int]int       // 原块 index → 过滤后 index
	next  int               // 下一个块 index

	textOpen bool // 下游 text 块是否打开
	textIdx  int  // 当前 text 块 index

	synth int // 合成工具块计数（诊断）
}

// NewFilter 创建过滤器；names = 净化名 → 原名（nil/空 = 直过）。
func NewFilter(names map[string]string) *Filter {
	return &Filter{names: names, idx: map[int]int{}}
}

// Synthesized 已合成的工具块数（诊断，纯观测，绝不进控制流）。
func (f *Filter) Synthesized() int { return f.synth }

// Process 处理一批事件，返回过滤后的事件（text 块内标签 → 合成块；其余原样）。
func (f *Filter) Process(events []ir.StreamEvent) []ir.StreamEvent {
	out := make([]ir.StreamEvent, 0, len(events)+4)
	for _, ev := range events {
		switch ev.Kind {
		case ir.EventBlockStart:
			ev.BlockIndex = f.assign(ev.BlockIndex)
			if ev.BlockKind == ir.BlockText {
				f.textOpen, f.textIdx = true, ev.BlockIndex
			}
			out = append(out, ev)

		case ir.EventBlockDelta:
			if ev.BlockKind == ir.BlockText {
				out = append(out, f.emit(f.sc.Feed(ev.TextDelta))...)
				continue
			}
			ev.BlockIndex = f.lookup(ev.BlockIndex)
			out = append(out, ev)

		case ir.EventBlockStop:
			if ev.BlockKind == ir.BlockText {
				out = append(out, f.emit(f.sc.Flush())...)
				if f.textOpen {
					out = append(out, ir.StreamEvent{
						Kind:       ir.EventBlockStop,
						BlockIndex: f.textIdx,
						BlockKind:  ir.BlockText,
					})
					f.textOpen = false
				}
				continue
			}
			ev.BlockIndex = f.lookup(ev.BlockIndex)
			out = append(out, ev)

		case ir.EventMessageDelta:
			if f.synth > 0 && ev.StopReason != "" && ev.StopReason != ir.StopError {
				ev.StopReason = ir.StopToolUse
			}
			out = append(out, ev)

		case ir.EventMessageStop, ir.EventDone:
			// 兜底：块尾未到但流已收尾 → 残留按文本降级 + 闭合未停块
			//（良构流走不到这里——text 块 stop 先到）。
			out = append(out, f.emit(f.sc.Flush())...)
			if f.textOpen {
				out = append(out, ir.StreamEvent{
					Kind:       ir.EventBlockStop,
					BlockIndex: f.textIdx,
					BlockKind:  ir.BlockText,
				})
				f.textOpen = false
			}
			out = append(out, ev)

		case ir.EventError:
			// 截断 / 失败：不冲刷半成品（不变量 13；半截参数既不补也不发）。
			f.sc.Reset()
			f.textOpen = false
			out = append(out, ev)

		default:
			out = append(out, ev)
		}
	}
	return out
}

// emit 扫描片段 → IR 事件（文本片段按需开关 text 块；调用片段 = 串行三件套）。
func (f *Filter) emit(segs []Segment) []ir.StreamEvent {
	var out []ir.StreamEvent
	for _, sg := range segs {
		if sg.Call == nil {
			if sg.Text == "" {
				continue
			}
			if !f.textOpen {
				f.textOpen, f.textIdx = true, f.next
				f.next++
				out = append(out, ir.StreamEvent{
					Kind:       ir.EventBlockStart,
					BlockIndex: f.textIdx,
					BlockKind:  ir.BlockText,
				})
			}
			out = append(out, ir.StreamEvent{
				Kind:       ir.EventBlockDelta,
				BlockIndex: f.textIdx,
				BlockKind:  ir.BlockText,
				TextDelta:  sg.Text,
			})
			continue
		}
		if f.textOpen {
			out = append(out, ir.StreamEvent{
				Kind:       ir.EventBlockStop,
				BlockIndex: f.textIdx,
				BlockKind:  ir.BlockText,
			})
			f.textOpen = false
		}
		idx := f.next
		f.next++
		out = append(out, ir.StreamEvent{
			Kind:       ir.EventBlockStart,
			BlockIndex: idx,
			BlockKind:  ir.BlockToolUse,
			ToolCallID: sg.Call.ID,
			ToolName:   f.restore(sg.Call.Name),
		})
		if sg.Call.Args != "" {
			// 空参（标签体为空）不发 args delta——出口 `{}` 纪律接管
			//（Anthropic 靠 start 的 `{}`；OpenAI 出口由修复层显式发 `{}`）。
			out = append(out, ir.StreamEvent{
				Kind:       ir.EventBlockDelta,
				BlockIndex: idx,
				BlockKind:  ir.BlockToolUse,
				ArgsDelta:  sg.Call.Args,
			})
		}
		out = append(out, ir.StreamEvent{
			Kind:       ir.EventBlockStop,
			BlockIndex: idx,
			BlockKind:  ir.BlockToolUse,
		})
		f.synth++
	}
	return out
}

// restore 合成工具名的净化名 → 原名还原（请求面清洗过的名字，模型照抄注入清单，
// 出口按同一反向表还原——与既有事件面 toolname.RestoreEvents 同表同纪律）。
func (f *Filter) restore(name string) string {
	if orig, ok := f.names[name]; ok && orig != "" {
		return orig
	}
	return name
}

// assign 原块 index → 过滤后 index（新分配，插入块不撞号）。
func (f *Filter) assign(orig int) int {
	idx := f.next
	f.next++
	f.idx[orig] = idx
	return idx
}

// lookup 已分配 → 过滤后 index；未见过的原 index 直过（防御）。
func (f *Filter) lookup(orig int) int {
	if idx, ok := f.idx[orig]; ok {
		return idx
	}
	return orig
}

// TransformResponse 非流式响应变换（S2 非流式面）：对每个 text 块跑同一 Scanner
// （同一词表、同一容错——流式/非流式一致），把契约标签合成 tool_use 块；合成过 →
// StopReason 改写 StopToolUse（error 语义优先，不改写）。
//
// 返回合成块数：0 = 响应未被改动（无契约标签的文本块原样保留——逐字节保真）。
func TransformResponse(resp *ir.Response) int {
	if resp == nil {
		return 0
	}
	var sc Scanner
	blocks := make([]ir.ContentBlock, 0, len(resp.Message.Blocks)+2)
	synth := 0
	for _, blk := range resp.Message.Blocks {
		if blk.Kind != ir.BlockText || blk.Text == "" {
			blocks = append(blocks, blk)
			continue
		}
		segs := append(sc.Feed(blk.Text), sc.Flush()...)
		if len(segs) == 1 && segs[0].Call == nil && segs[0].Text == blk.Text {
			blocks = append(blocks, blk) // 无契约标签：原块直过
			continue
		}
		for _, sg := range segs {
			if sg.Call == nil {
				blocks = append(blocks, ir.ContentBlock{Kind: ir.BlockText, Text: sg.Text})
				continue
			}
			blocks = append(blocks, ir.ContentBlock{
				Kind: ir.BlockToolUse,
				ToolCall: &ir.ToolCall{
					ID:        sg.Call.ID,
					Name:      sg.Call.Name,
					Arguments: sg.Call.Args,
				},
			})
			synth++
		}
	}
	if synth == 0 {
		return 0
	}
	resp.Message.Blocks = blocks
	if resp.StopReason != ir.StopError {
		resp.StopReason = ir.StopToolUse
	}
	return synth
}
