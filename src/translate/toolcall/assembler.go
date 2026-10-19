package toolcall

import (
	"fmt"

	"github.com/dwgx/any2api/translate/hygiene"
)

// Mode 组装模式（规格 §3.1）。
type Mode int

const (
	// ModeA 逐 delta 累积：每片 partial_json 直接转发（上游前缀单调且无重写）。
	ModeA Mode = iota
	// ModeB 缓冲到 stop 单 delta：stop 时校验后一次性发（保守默认）。
	ModeB
	// ModeC 结构化单发：上游已给完整对象，直接一个 delta 发完整 JSON
	//（无需缓冲状态机，调用方直接走 Finalize）。
	ModeC
)

// FrameSemantics 上游帧语义（模式判定输入）。
type FrameSemantics struct {
	Structured      bool // 整对象到达（Gemini functionCall / 非流式）
	PrefixMonotonic bool // 严格前缀单调（标准 OpenAI / Anthropic 上游）
}

// ConsumerContract 下游消费契约（决定出口形态；不参与模式选择）。
type ConsumerContract string

const (
	ConsumerAnthropic       ConsumerContract = "anthropic"
	ConsumerOpenAI          ConsumerContract = "openai_chat"
	ConsumerOpenAIResponses ConsumerContract = "openai_responses"
)

// ChooseMode 按上游语义选组装模式；保守默认 = B（模式是组装器属性，不是协议属性）。
func ChooseMode(up FrameSemantics, down ConsumerContract) Mode {
	_ = down // 下游契约只影响出口形态（OutletArguments），不影响模式选择
	switch {
	case up.Structured:
		return ModeC
	case up.PrefixMonotonic:
		return ModeA
	default:
		return ModeB
	}
}

// SlotState 槽位状态。
type SlotState int

const (
	SlotAssembling SlotState = iota
	SlotReady
	SlotInvalid
)

// FrameKind 上游工具帧类型。
type FrameKind string

const (
	FrameToolStart  FrameKind = "tool_start"  // 首片带元数据（id / name）
	FrameInputChunk FrameKind = "input_chunk" // 后续片只带增量 / 快照（语义不可信）
	FrameToolStop   FrameKind = "tool_stop"   // 边界 = finalize 时机（权威）
	FrameStreamEnd  FrameKind = "stream_end"  // 流结束残留兜底
)

// Frame 上游工具帧。
type Frame struct {
	Kind      FrameKind
	ToolUseID string
	Name      string
	Chunk     string
}

// EventKind 下游事件类型（Anthropic 出口形态）。
type EventKind string

const (
	EventContentBlockStart EventKind = "content_block_start"
	EventInputJSONDelta    EventKind = "input_json_delta"
	EventContentBlockStop  EventKind = "content_block_stop"
	EventError             EventKind = "error"
)

// Event 下游事件。按 Kind 解释有效字段。
type Event struct {
	Kind        EventKind
	Index       int
	BlockType   string // content_block_start: "tool_use"
	ToolUseID   string
	Name        string
	Input       string // content_block_start 占位 {}（空参最终值，见 §5 出口纪律）
	PartialJSON string // input_json_delta
	Failure     *Failure
}

// ToolDef 工具定义。Bug C 校验用上游形态键名（UpstreamRequired），
// 必须在形态还原之前判定。
type ToolDef struct {
	Name             string   // 下发客户端的工具名
	UpstreamName     string   // 上游形态名
	UpstreamRequired []string // 上游形态必需键（存在性判定；nil = 不校验）
}

// ToolSlot 组装槽位（规格 §3.2）。
type ToolSlot struct {
	SlotID     string
	DownIdx    int // 下游块 index（稠密 tool 索引）
	UpstreamID string
	OutName    string
	UpName     string
	Buf        string // 组装缓冲（String 级重组）
	State      SlotState
	Failure    *Failure // 失败态（绑定「不发坏 JSON」判据）
	Defect     *Defect  // 归因诊断（纯观测）
	Emitted    *string  // 出口串（nil = 不发）
	MergeSteps []MergeStep
}

// Assembler 工具槽位状态机（规格 §3.2）。
type Assembler struct {
	mode          Mode
	policy        Policy
	tools         []ToolDef
	slots         map[string]*ToolSlot
	order         []*ToolSlot
	nextToolIndex int
}

// NewAssembler 构造组装器。mode 见 ChooseMode；policy 见 §4.4。
func NewAssembler(mode Mode, tools []ToolDef, policy Policy) *Assembler {
	return &Assembler{
		mode:   mode,
		policy: policy,
		tools:  tools,
		slots:  map[string]*ToolSlot{},
	}
}

// OnFrame 处理一帧上游工具帧，返回下游事件（B 模式 input_chunk 不下发）。
func (a *Assembler) OnFrame(f Frame) []Event {
	switch f.Kind {
	case FrameToolStart:
		if _, dup := a.slots[f.ToolUseID]; dup {
			return nil // 重复 start：忽略（诊断留调用方）
		}
		upID := f.ToolUseID
		if upID == "" {
			// 缺失合成（§6.2）：组装器**没有响应 id**（纯状态机输入），合成走块序
			// 回落形态（hygiene.SynthIndexedID——`_synth` 标记 + 块序确定）。
			// 出口写出层的合成已改响应 id 派生（W5-29，跨响应唯一）；本处是
			// 无响应身份的残余面（组装器未接服务壳；见报告「残余风险」）。
			upID = hygiene.SynthIndexedID("toolu", a.nextToolIndex)
		}
		s := &ToolSlot{
			SlotID:     fmt.Sprintf("slot_%d", a.nextToolIndex),
			DownIdx:    a.nextToolIndex,
			UpstreamID: upID,
			OutName:    f.Name,
			UpName:     f.Name,
			State:      SlotAssembling,
		}
		a.slots[f.ToolUseID] = s
		a.order = append(a.order, s)
		a.nextToolIndex++
		return []Event{{
			Kind:      EventContentBlockStart,
			Index:     s.DownIdx,
			BlockType: "tool_use",
			ToolUseID: s.UpstreamID,
			Name:      s.OutName,
			Input:     "{}", // 占位 {}（空参最终值，§5）
		}}
	case FrameInputChunk:
		s := a.slots[f.ToolUseID]
		if s == nil {
			return nil // 无 start 的孤儿 chunk：忽略
		}
		merged, step := MergeToolInputTraced(s.Buf, f.Chunk)
		s.Buf = merged
		s.MergeSteps = append(s.MergeSteps, step)
		if a.mode == ModeA {
			return []Event{{Kind: EventInputJSONDelta, Index: s.DownIdx, PartialJSON: f.Chunk}}
		}
		return nil // B / C：只累积，不下发
	case FrameToolStop:
		s := a.slots[f.ToolUseID]
		if s == nil || s.State != SlotAssembling {
			return nil
		}
		return a.finalizeSlot(s)
	case FrameStreamEnd:
		// 残留 flush：所有未 stop 的槽位走 Finalize（尽力发 + 关块）。
		// 收尾顺序铁律：先 flush（可能置失败态）→ 再据失败态补 error 事件。
		var normal, errs []Event
		for _, s := range a.order {
			if s.State != SlotAssembling {
				continue
			}
			for _, ev := range a.finalizeSlot(s) {
				if ev.Kind == EventError {
					errs = append(errs, ev)
				} else {
					normal = append(normal, ev)
				}
			}
		}
		return append(normal, errs...)
	}
	return nil
}

func (a *Assembler) finalizeSlot(s *ToolSlot) []Event {
	res := Finalize(s.Buf, a.requiredFor(s), a.policy)
	s.Failure = res.Failure
	s.Defect = res.Defect
	s.Emitted = res.Emitted
	if res.Failure != nil {
		s.State = SlotInvalid
		// 失败态：不发坏 JSON，error 事件由收尾（或此处单槽收尾）补发。
		return []Event{{
			Kind:      EventError,
			Index:     s.DownIdx,
			ToolUseID: s.UpstreamID,
			Name:      s.OutName,
			Failure:   res.Failure,
		}}
	}
	s.State = SlotReady
	var evs []Event
	if res.Emitted != nil {
		evs = append(evs, Event{Kind: EventInputJSONDelta, Index: s.DownIdx, PartialJSON: *res.Emitted})
	}
	evs = append(evs, Event{Kind: EventContentBlockStop, Index: s.DownIdx, ToolUseID: s.UpstreamID})
	return evs
}

// requiredFor 按上游形态名解析槽位的 required 键；无匹配（或定义为空）= 不校验。
func (a *Assembler) requiredFor(s *ToolSlot) []string {
	for _, d := range a.tools {
		if d.UpstreamName != "" && d.UpstreamName == s.UpName {
			return d.UpstreamRequired
		}
	}
	for _, d := range a.tools {
		if d.UpstreamName == "" && d.Name == s.UpName {
			return d.UpstreamRequired
		}
	}
	return nil
}

// SlotResults 返回全部槽位快照（按打开顺序）。
func (a *Assembler) SlotResults() []ToolSlot {
	out := make([]ToolSlot, 0, len(a.order))
	for _, s := range a.order {
		out = append(out, *s)
	}
	return out
}

// Slot 按上游 tool_use id 取槽位（nil = 无此槽位）。
func (a *Assembler) Slot(toolUseID string) *ToolSlot { return a.slots[toolUseID] }
