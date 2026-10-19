package openai_chat

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/dwgx/any2api/translate/hygiene"
	"github.com/dwgx/any2api/translate/ir"
)

// StreamWriter 增量流式渲染器：IR 事件逐个进、下游 SSE 字节逐个出
// （服务壳 relay 用——上游逐帧到达，逐帧写下游）。
//
// 与批量 RenderStream 的唯一差别是工具块首片参数的实现机制：批量预扫全量事件、
// 把首片参数附着在 block_start chunk 上；增量用「缓冲到首片段或块尾」实现同一
// 帧形——正常流（首片段紧跟 block_start）下两者输出逐字节一致；无片段的工具块
// 在块尾以 "{}" 收尾（空参数门，位置从 block_start 挪到块尾）。
//
// 契约（与批量同）：序列必须以 start 开头；done/error 为终态，其后事件报错；
// [DONE] 由渲染器生成（未带 done 时 Finish 自动补）；**error 帧后不补 [DONE]**
// （W5-26 实测裁决：错误帧即终态，补哨兵会把截断流伪装成正常完成）。
type StreamWriter struct {
	opts StreamRenderOptions

	meta        ir.StreamEvent
	started     bool
	terminal    bool
	doneEmitted bool
	errored     bool // 错误帧已发：Finish 不得补 [DONE]（错误即终态）

	startedBlocks map[int]ir.BlockKind
	toolWire      map[int]int    // IR blockIndex → wire index（tool_calls[].index）
	toolWireID    map[int]string // IR blockIndex → tool_call 线 id（W42 ② 签名条目配对）
	toolCount     int
	// pending 已 start、等首片段或块尾收尾的工具块（blockIndex → 缓冲）。
	pending map[int]*pendingToolBlock
	// thinkingDetail IR blockIndex → reasoning_details 的 wire index（W41：消费端按
	// index 分组——文本拼接、签名取最后一条）。块 start 时分配，逐块 +1。
	thinkingDetail map[int]int
	detailCount    int
	// pendingThinkingSig IR blockIndex → 已到达的签名增量（W41：缓冲到块尾再出
	// 一条完整条目——逐增量出会让「签名取最后一条」的消费端只剩最后一段）。
	pendingThinkingSig map[int]string
	// toolSig IR blockIndex → 已到达的 tool 签名增量（W42 ②：块尾一条
	// reasoning.encrypted{id} 条目——与思考签名同款缓冲纪律）。
	toolSig map[int]string
	// pendingRedacted IR blockIndex → 已到达的 redacted 载荷增量（W42 ①：块尾一条
	// reasoning.encrypted{format:anthropic-claude-v1} 条目）。
	pendingRedacted map[int]string

	diag StreamDiagnostics
}

// StreamDiagnostics 流式出口诊断计数（纯观测，绝不进控制流）。
type StreamDiagnostics struct {
	// DroppedThinking 未下发到 OpenAI 线的 thinking / redacted_thinking 增量数：
	// 空思考增量；redacted 载荷增量（枢纽腿——直转面 W42 起走 reasoning.encrypted
	// 载体）。W42 ③ 起枢纽腿的思考文本不再丢弃（走载体，见 HubLeg 注释）。
	DroppedThinking int
	// ReasoningDeltas 以 delta.reasoning_content 下发的思考增量数（W41 起同帧
	// 并行出 delta.reasoning_details 文本条目；W42 ③ 起含枢纽腿）。
	ReasoningDeltas int
	// SignatureDropped 未写进载体的**签名载荷**数（W5-19 条目 5；W41 载体化后 =
	// reasoning_details 未落地的签名；W42 起含 tool_use 块签名）：形态不可识别的
	// 思考 / tool 签名增量（红线：不写未判定签名）。带签名案例可据此与批量面
	// BatchDiagnostics.SignatureDropped 逐项对拍。
	SignatureDropped int
}

// Diagnostics 返回诊断计数快照（纯观测；服务壳据此打日志/指标）。
func (w *StreamWriter) Diagnostics() StreamDiagnostics { return w.diag }

type pendingToolBlock struct {
	wireIndex int
	id        string
	name      string
}

// NewStreamWriter 创建增量渲染器。
func NewStreamWriter(opts StreamRenderOptions) *StreamWriter {
	return &StreamWriter{
		opts:               opts,
		startedBlocks:      map[int]ir.BlockKind{},
		toolWire:           map[int]int{},
		toolWireID:         map[int]string{},
		pending:            map[int]*pendingToolBlock{},
		thinkingDetail:     map[int]int{},
		pendingThinkingSig: map[int]string{},
		toolSig:            map[int]string{},
		pendingRedacted:    map[int]string{},
	}
}

// Write 处理一个 IR 事件，返回应写入下游的字节（可能为空）。
func (w *StreamWriter) Write(ev ir.StreamEvent) ([]byte, error) {
	if w.terminal {
		return nil, fmt.Errorf("%w: event %s", ErrEventsAfterTerminal, ev.Kind)
	}
	var buf bytes.Buffer
	writeChunk := func(c outChunkWire) error {
		// 帧级元数据保真（W5-30）：事件携带的三键回写到它产出的下游帧
		//（per-event，不钉住——IR 不动点要求 parse/render 同判）。
		if hasRaw(ev.ServiceTier) {
			c.ServiceTier = ev.ServiceTier
		}
		if hasRaw(ev.SystemFingerprint) {
			c.SystemFingerprint = ev.SystemFingerprint
		}
		if hasRaw(ev.Moderation) {
			c.Moderation = ev.Moderation
		}
		return writeChunkTo(&buf, c)
	}
	switch ev.Kind {
	case ir.EventStart:
		if w.started {
			return nil, fmt.Errorf("%w: duplicate start event", ErrBadIR)
		}
		w.started = true
		w.meta = ev
		if w.meta.MessageID == "" {
			// 上游不给 id → 合成每响应唯一（`r_synth_<32hex>`，W5-31；与批量面
			// 同形）：帧 id 与合成工具 id 的派生 seed 都用它（旧行为 seed 空 →
			// 工具 id 回落块序形态 `call_synth_<n>`，跨响应重复）。
			w.meta.MessageID = hygiene.SynthUniqueID("r")
		}
		empty := ""
		if err := writeChunk(outChunkWire{
			ID: w.meta.MessageID, Object: objectChatCompletionChunk,
			Created: w.meta.Created, Model: w.meta.Model,
			Choices: []outChunkChoice{{
				Index: 0,
				Delta: &outDelta{Role: string(ir.RoleAssistant), Content: &empty},
			}},
		}); err != nil {
			return nil, err
		}

	case ir.EventBlockStart:
		if !w.started {
			return nil, fmt.Errorf("%w", ErrMissingStart)
		}
		if _, dup := w.startedBlocks[ev.BlockIndex]; dup {
			return nil, fmt.Errorf("%w: duplicate block_start %d", ErrBadIR, ev.BlockIndex)
		}
		w.startedBlocks[ev.BlockIndex] = ev.BlockKind
		if ev.BlockKind == ir.BlockThinking {
			// W41：思考块的 reasoning_details wire index（块 start 时分配，逐块 +1）。
			// W42 ③：枢纽腿同表（载体是内部线成员——此前 hub 腿丢弃思考）。
			w.thinkingDetail[ev.BlockIndex] = w.detailCount
			w.detailCount++
		}
		if isToolKind(ev.BlockKind) {
			// 缓冲：首片段到达（或块尾无片段）时才发 block_start chunk。
			// 出口 id 统一卫生（hygiene）：净化 + 缺失合成（响应 id 派生 +
			// 块序，跨响应唯一；与批量渲染同键）。W42 ②：线 id 存表——签名条目
			// 的 id 必须与 tool_calls[].id 逐字相同（消费端按 id 配对）。
			wireID := hygiene.WireToolID("call", ev.ToolCallID, w.meta.MessageID, ev.BlockIndex)
			w.toolWire[ev.BlockIndex] = w.toolCount
			w.toolWireID[ev.BlockIndex] = wireID
			w.pending[ev.BlockIndex] = &pendingToolBlock{
				wireIndex: w.toolCount,
				id:        wireID,
				name:      ev.ToolName,
			}
			w.toolCount++
		}
		// text / refusal 的 block_start：OpenAI Chat 无块边界帧，无输出。

	case ir.EventBlockDelta:
		if !w.started {
			return nil, fmt.Errorf("%w", ErrMissingStart)
		}
		if _, ok := w.startedBlocks[ev.BlockIndex]; !ok {
			return nil, fmt.Errorf("%w: block_delta for unstarted block %d", ErrBadIR, ev.BlockIndex)
		}
		switch ev.BlockKind {
		case ir.BlockText:
			if ev.TextDelta == "" {
				break
			}
			t := ev.TextDelta
			if err := writeChunk(outChunkWire{
				ID: w.meta.MessageID, Object: objectChatCompletionChunk,
				Created: w.meta.Created, Model: w.meta.Model,
				Choices: []outChunkChoice{{Index: 0, Delta: &outDelta{Content: &t}}},
			}); err != nil {
				return nil, err
			}
		case ir.BlockRefusal:
			if ev.TextDelta == "" {
				break
			}
			t := ev.TextDelta
			if err := writeChunk(outChunkWire{
				ID: w.meta.MessageID, Object: objectChatCompletionChunk,
				Created: w.meta.Created, Model: w.meta.Model,
				Choices: []outChunkChoice{{Index: 0, Delta: &outDelta{Refusal: &t}}},
			}); err != nil {
				return nil, err
			}
		case ir.BlockToolUse, ir.BlockServerToolUse:
			// W42 ②：tool 签名增量缓冲（块尾一条 reasoning.encrypted{id} 条目——
			// 与思考签名同款缓冲纪律；消费端按 id 配对）。参数增量与签名增量可
			// 同帧/异帧（gemini 上游 feedFunctionCall 同事件、roo-code 流序任意）。
			if ev.SignatureDelta != "" {
				w.toolSig[ev.BlockIndex] += ev.SignatureDelta
			}
			if ev.ArgsDelta == "" {
				break
			}
			if p, ok := w.pending[ev.BlockIndex]; ok {
				// 首片附着在 block_start chunk（id/name/arguments 同帧）。
				delete(w.pending, ev.BlockIndex)
				if err := writeChunk(outChunkWire{
					ID: w.meta.MessageID, Object: objectChatCompletionChunk,
					Created: w.meta.Created, Model: w.meta.Model,
					Choices: []outChunkChoice{{
						Index: 0,
						Delta: &outDelta{ToolCalls: []outToolCallDelta{{
							Index: p.wireIndex, ID: p.id, Type: "function",
							Function: &outFunction{Name: p.name, Arguments: ev.ArgsDelta},
						}}},
					}},
				}); err != nil {
					return nil, err
				}
				break
			}
			if err := writeChunk(outChunkWire{
				ID: w.meta.MessageID, Object: objectChatCompletionChunk,
				Created: w.meta.Created, Model: w.meta.Model,
				Choices: []outChunkChoice{{Index: 0, Delta: &outDelta{
					ToolCalls: []outToolCallDelta{{
						Index:    w.toolWire[ev.BlockIndex],
						Function: &outFunction{Arguments: ev.ArgsDelta},
					}},
				}}},
			}); err != nil {
				return nil, err
			}
		case ir.BlockThinking:
			// 思考增量（P53 缺口 2 + W41 载体化 + W42 ③）：
			//   - 直转面：思考文本走 delta.reasoning_content（非官方扩展，但为
			//     OpenAI 兼容生态既有约定；longtail-matrix §4.3：DeepSeek/Kimi/
			//     Zhipu/MiniMax/SiliconFlow/Together 同款；anthropic-conversion-table
			//     「Anthropic → OpenAI Chat」行：CPA 同款；openai-responses 表同款），
			//     **同时**出 delta.reasoning_details 结构化条目（W41：text 增量逐帧；
			//     签名增量缓冲到块尾再出一条完整条目——见 pendingThinkingSig 注释）。
			//   - 枢纽中间腿（HubLeg）：W42 ③ 起与直转面同表（载体是内部线成员；
			//     hub 腿消费者是本仓解析器——此前整块丢弃让 claude→gemini 等两步链
			//     静默丢思考文本）。
			//   签名形态不可识别 → 丢弃 + SignatureDropped（红线：不写未判定签名）。
			if ev.TextDelta == "" && ev.SignatureDelta == "" {
				w.diag.DroppedThinking++
				break
			}
			if ev.SignatureDelta != "" {
				w.pendingThinkingSig[ev.BlockIndex] += ev.SignatureDelta
			}
			if ev.TextDelta != "" {
				t := ev.TextDelta
				w.diag.ReasoningDeltas++
				if err := writeChunk(outChunkWire{
					ID: w.meta.MessageID, Object: objectChatCompletionChunk,
					Created: w.meta.Created, Model: w.meta.Model,
					Choices: []outChunkChoice{{Index: 0, Delta: &outDelta{
						ReasoningContent: &t,
						ReasoningDetails: []outReasoningDetail{{
							Type: "reasoning.text", Text: t, Index: w.thinkingDetail[ev.BlockIndex],
						}},
					}}},
				}); err != nil {
					return nil, err
				}
			}

		case ir.BlockRedactedThinking:
			// W42 ①：加密思考载荷 → 块尾一条 reasoning.encrypted 条目（直转面；
			// data ↔ 载荷）。枢纽腿（HubLeg）仍丢弃 + 计数——中间腿保真会让载荷
			// 到达 gemini 出口（该出口无 redacted 载体，断链；报告 §hub 腿评估）。
			if w.opts.HubLeg {
				w.diag.DroppedThinking++
				break
			}
			if ev.SignatureDelta != "" {
				w.pendingRedacted[ev.BlockIndex] += ev.SignatureDelta
			}

		default:
			return nil, fmt.Errorf("%w: delta block kind %q", ErrUnsupportedBlock, ev.BlockKind)
		}

	case ir.EventBlockStop:
		kind, ok := w.startedBlocks[ev.BlockIndex]
		if !ok {
			return nil, fmt.Errorf("%w: block_stop for unstarted block %d", ErrBadIR, ev.BlockIndex)
		}
		if kind == ir.BlockThinking {
			// W41：缓冲的签名增量在块尾出一条完整条目（形态可识别才写；不可识别
			// → 丢弃 + SignatureDropped——红线）。W42 ③：枢纽腿同表。
			if sig := w.pendingThinkingSig[ev.BlockIndex]; sig != "" {
				delete(w.pendingThinkingSig, ev.BlockIndex)
				if format, ok := reasoningDetailFormat(sig); ok {
					if err := writeChunk(outChunkWire{
						ID: w.meta.MessageID, Object: objectChatCompletionChunk,
						Created: w.meta.Created, Model: w.meta.Model,
						Choices: []outChunkChoice{{Index: 0, Delta: &outDelta{
							ReasoningDetails: []outReasoningDetail{{
								Type: "reasoning.text", Signature: sig, Format: format,
								Index: w.thinkingDetail[ev.BlockIndex],
							}},
						}}},
					}); err != nil {
						return nil, err
					}
				} else {
					w.diag.SignatureDropped++
				}
			}
		}
		if isToolKind(kind) {
			// W42 ②：tool 签名增量在块尾出一条完整 reasoning.encrypted{id} 条目
			//（id = tool_calls[].id 线值——消费端按 id 配对；形态可识别才写，
			// 不可识别 → 丢弃 + SignatureDropped——红线）。
			if sig := w.toolSig[ev.BlockIndex]; sig != "" {
				delete(w.toolSig, ev.BlockIndex)
				if format, ok := reasoningToolSignature(sig); ok {
					if err := writeChunk(outChunkWire{
						ID: w.meta.MessageID, Object: objectChatCompletionChunk,
						Created: w.meta.Created, Model: w.meta.Model,
						Choices: []outChunkChoice{{Index: 0, Delta: &outDelta{
							ReasoningDetails: []outReasoningDetail{{
								Type: "reasoning.encrypted", Data: sig, ID: w.toolWireID[ev.BlockIndex],
								Format: format, Index: w.detailCount,
							}},
						}}},
					}); err != nil {
						return nil, err
					}
					w.detailCount++
				} else {
					w.diag.SignatureDropped++
				}
			}
		}
		if kind == ir.BlockRedactedThinking && !w.opts.HubLeg {
			// W42 ①：redacted 载荷在块尾出一条完整 reasoning.encrypted 条目
			//（format=anthropic-claude-v1——载荷来源族；空载荷不产条目）。
			if payload := w.pendingRedacted[ev.BlockIndex]; payload != "" {
				delete(w.pendingRedacted, ev.BlockIndex)
				if err := writeChunk(outChunkWire{
					ID: w.meta.MessageID, Object: objectChatCompletionChunk,
					Created: w.meta.Created, Model: w.meta.Model,
					Choices: []outChunkChoice{{Index: 0, Delta: &outDelta{
						ReasoningDetails: []outReasoningDetail{{
							Type: "reasoning.encrypted", Data: payload,
							Format: reasoningFormatClaude, Index: w.detailCount,
						}},
					}}},
				}); err != nil {
					return nil, err
				}
				w.detailCount++
			}
		}
		if p, ok := w.pending[ev.BlockIndex]; ok {
			// 无参数片段 → 块尾以 "{}" 收尾（空参数门修复）。
			delete(w.pending, ev.BlockIndex)
			if err := writeChunk(outChunkWire{
				ID: w.meta.MessageID, Object: objectChatCompletionChunk,
				Created: w.meta.Created, Model: w.meta.Model,
				Choices: []outChunkChoice{{
					Index: 0,
					Delta: &outDelta{ToolCalls: []outToolCallDelta{{
						Index: p.wireIndex, ID: p.id, Type: "function",
						Function: &outFunction{Name: p.name, Arguments: "{}"},
					}}},
				}},
			}); err != nil {
				return nil, err
			}
		}
		delete(w.startedBlocks, ev.BlockIndex)
		// OpenAI Chat 无块边界帧，其余无输出。

	case ir.EventMessageDelta:
		if !w.started {
			return nil, fmt.Errorf("%w", ErrMissingStart)
		}
		if ev.StopReason != "" {
			fr := finishReasonFor(ev.StopReason)
			if err := writeChunk(outChunkWire{
				ID: w.meta.MessageID, Object: objectChatCompletionChunk,
				Created: w.meta.Created, Model: w.meta.Model,
				Choices: []outChunkChoice{{Index: 0, Delta: &outDelta{}, FinishReason: &fr}},
			}); err != nil {
				return nil, err
			}
		}
		if ev.Usage != nil && w.opts.IncludeUsage {
			u, err := renderUsage(ev.Usage)
			if err != nil {
				return nil, err
			}
			if err := writeChunk(outChunkWire{
				ID: w.meta.MessageID, Object: objectChatCompletionChunk,
				Created: w.meta.Created, Model: w.meta.Model,
				Choices: []outChunkChoice{},
				Usage:   u,
			}); err != nil {
				return nil, err
			}
		}

	case ir.EventMessageStop:
		// 无对应帧（终止信号由 done/[DONE] 承担）。

	case ir.EventDone:
		buf.WriteString("data: " + doneSentinel + "\n\n")
		w.doneEmitted = true
		w.terminal = true

	case ir.EventError:
		ee := ev.Error
		if ee == nil {
			ee = &ir.Error{Type: "api_error", Message: "stream error", Source: "any2api"}
		}
		eb, err := RenderError(ee)
		if err != nil {
			return nil, err
		}
		buf.WriteString("data: ")
		buf.Write(eb)
		buf.WriteString("\n\n")
		// W5-26 裁决（实测：`tmp/lanes/L19-sweep9/proofs/done-after-error.txt`）：
		// **错误帧后不补 `[DONE]`**（Bifrost #5546 形态）。错误帧即终态——补哨兵
		// 会让「只认 [DONE]」的消费端把截断流当正常完成（假成功）；openai-python
		// 与五形状分类器两类消费端在两种形态下都判错误（哨兵无害但也无用）。
		w.errored = true
		w.terminal = true

	default:
		return nil, fmt.Errorf("%w: event kind %q", ErrBadIR, ev.Kind)
	}
	return buf.Bytes(), nil
}

// Finish 事件流结束（上游 EOF）时调用：冲刷未收尾的工具块并补 [DONE]。
// 终态（done/error）之后不再冲刷（错误后的悬空块不产出帧）。
func (w *StreamWriter) Finish() ([]byte, error) {
	var buf bytes.Buffer
	if !w.started && !w.terminal {
		return nil, fmt.Errorf("%w: empty event stream", ErrMissingStart)
	}
	if !w.terminal && len(w.pending) > 0 {
		idxs := make([]int, 0, len(w.pending))
		for i := range w.pending {
			idxs = append(idxs, i)
		}
		sort.Ints(idxs)
		for _, i := range idxs {
			p := w.pending[i]
			if err := writeChunkTo(&buf, outChunkWire{
				ID: w.meta.MessageID, Object: objectChatCompletionChunk,
				Created: w.meta.Created, Model: w.meta.Model,
				Choices: []outChunkChoice{{
					Index: 0,
					Delta: &outDelta{ToolCalls: []outToolCallDelta{{
						Index: p.wireIndex, ID: p.id, Type: "function",
						Function: &outFunction{Name: p.name, Arguments: "{}"},
					}}},
				}},
			}); err != nil {
				return nil, err
			}
		}
		w.pending = map[int]*pendingToolBlock{}
	}
	if !w.doneEmitted && !w.errored {
		// 哨兵是合成器职责：事件流没有 done 也要收尾（OpenAI 边缘必发 [DONE]）；
		// 错误终态例外（W5-26：错误帧后不补哨兵）。
		buf.WriteString("data: " + doneSentinel + "\n\n")
	}
	return buf.Bytes(), nil
}

// writeChunkTo 写入一个 data: 帧（SSE 帧语法：data: <json>\n\n）。
func writeChunkTo(buf *bytes.Buffer, c any) error {
	b, err := marshalJSON(c)
	if err != nil {
		return err
	}
	buf.WriteString("data: ")
	buf.Write(b)
	buf.WriteString("\n\n")
	return nil
}
