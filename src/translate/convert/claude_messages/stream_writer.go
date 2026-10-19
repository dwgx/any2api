package claude_messages

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dwgx/any2api/translate/hygiene"
	"github.com/dwgx/any2api/translate/ir"
)

// StreamWriterOptions 增量流式渲染选项。
type StreamWriterOptions struct {
	// StartUsage 已知的最终 usage（批量渲染的预扫产物）：非 nil 时 message_start
	// 的 input/cache 提前落位（CC B7「input/cache 全量在 start」）；实时 relay
	// 传 nil——start 用 0 占位、全量在 message_delta（CPA 同款；客户端合并规则
	// 保证 >0 的 delta 值覆盖占位）。
	StartUsage *ir.Usage
}

// StreamWriter 增量流式渲染器：IR 事件逐个进、命名事件 SSE 字节逐个出
// （服务壳 relay 用——上游逐帧到达，逐帧写下游）。
//
// 与批量 RenderStream 的差别：
//   - message_start.usage 只带已知值（批量预扫全量、start 即 input/cache 全量；
//     增量在 start 处 input/cache 未知 → 0 占位，全量在 message_delta）；
//   - message_delta 帧推迟到 message_stop 时发出（批量在 stop_reason 事件处发出；
//     两者之间无其他帧产出，正常流下输出逐字节一致）；
//   - redacted_thinking 的 content_block_start 缓冲到数据/块尾（批量预扫回填）。
type StreamWriter struct {
	startUsage *ir.Usage

	meta     ir.StreamEvent
	started  bool
	stopped  bool
	terminal bool

	startedBlocks map[int]ir.BlockKind
	// pendingRedacted 已 start、等不透明载荷的 redacted 块（blockIndex → 缓冲）。
	pendingRedacted map[int]*strings.Builder

	// wireIndex 上游块 index → 下游 wire index（W11-3）：下游 index 恒**连续
	// 从 0 起**（CC B5 要求「index 从 0 递增且与 start 一致」）——整块丢弃
	// （tool_result）不留空洞，deltas/stop 的 index 与 start 一致。
	wireIndex     map[int]int
	nextWireIndex int

	stopReason ir.StopReason
	// stopSequence 命中的停止序列原文（IR StopSequence 透传；非空时
	// message_delta 渲染 stop_reason:"stop_sequence" + stop_sequence 值——
	// claude.ai 配额截断 `usage_limit` 的保真出口，W43-3 §4.3 R1）。
	stopSequence string
	usage        *ir.Usage
	stopEmitted  bool
}

// NewStreamWriter 创建增量渲染器。
func NewStreamWriter(opts StreamWriterOptions) *StreamWriter {
	return &StreamWriter{
		startUsage:      opts.StartUsage,
		startedBlocks:   map[int]ir.BlockKind{},
		pendingRedacted: map[int]*strings.Builder{},
		wireIndex:       map[int]int{},
	}
}

// Write 处理一个 IR 事件，返回应写入下游的字节（可能为空）。
//
// 契约（与批量同，CC B1-B8）：start 必须最先；message_stop 收尾；
// message_delta 必带 stop_reason（缺失 → message_stop 时报 ErrMissingStopReason）；
// content_block_start 内联内容恒为空；无 [DONE]；error 帧为终态。
func (w *StreamWriter) Write(ev ir.StreamEvent) ([]byte, error) {
	if w.terminal {
		return nil, fmt.Errorf("%w: event %s", ErrEventsAfterTerminal, ev.Kind)
	}
	var buf bytes.Buffer
	writeFrame := func(name string, payload any) error {
		return writeFrameTo(&buf, name, payload)
	}
	switch ev.Kind {
	case ir.EventStart:
		if w.started {
			return nil, fmt.Errorf("%w: duplicate start event", ErrBadIR)
		}
		w.started = true
		w.meta = ev
		if w.meta.MessageID == "" {
			// 上游不给 id → 合成每响应唯一（`msg_synth_<32hex>`，W5-35；与
			// openai_chat 流式面同款）：message_start 的 id 与合成工具 id 的
			// 派生 seed 都用它（旧行为 seed 空 → 工具 id 回落块序形态，跨响应
			// 重复——W5-29 报告点名的残余）。
			w.meta.MessageID = hygiene.SynthUniqueID("msg")
		}
		if err := writeFrame(evMessageStart, map[string]json.RawMessage{
			"type": mustMarshal(evMessageStart),
			"message": mustMarshal(map[string]json.RawMessage{
				"id":            mustMarshal(w.meta.MessageID),
				"type":          mustMarshal(objectMessage),
				"role":          mustMarshal(string(ir.RoleAssistant)),
				"model":         mustMarshal(w.meta.Model),
				"content":       json.RawMessage("[]"),
				"stop_reason":   json.RawMessage("null"),
				"stop_sequence": json.RawMessage("null"),
				"usage":         renderStartUsage(w.startUsage),
			}),
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
		if ev.BlockKind == ir.BlockToolResult {
			// 服务端工具结果（上游内置工具的显示面产物，W11-3）：**整块丢弃**
			// ——客户端语义的 tool_result 必须配对客户端 tool_use（Anthropic 线
			// 仅允许出现在 user 消息），上游的 bash_tool 等结果没有配对块；
			// 透传会把不配对的 tool_result 塞进客户端会话状态。块边界消费
			// （start/delta/stop 全不产帧），**不占下游 index**（wireIndex 连续）。
			// BK parseCompletionSSE（internal/service/claude.go:433-437）同款丢弃。
			return nil, nil
		}
		wi := w.nextWireIndex
		w.nextWireIndex++
		w.wireIndex[ev.BlockIndex] = wi
		var block map[string]json.RawMessage
		switch ev.BlockKind {
		case ir.BlockText:
			block = map[string]json.RawMessage{
				"type": mustMarshal(blockTypeText),
				"text": mustMarshal(""),
			}
		case ir.BlockThinking:
			block = map[string]json.RawMessage{
				"type":      mustMarshal(blockTypeThinking),
				"thinking":  mustMarshal(""),
				"signature": mustMarshal(""),
			}
		case ir.BlockRedactedThinking:
			// wire 中不透明载荷只出现在 content_block_start：缓冲到数据/块尾。
			w.pendingRedacted[ev.BlockIndex] = &strings.Builder{}
			return nil, nil
		case ir.BlockToolUse, ir.BlockServerToolUse:
			t := blockTypeToolUse
			if ev.BlockKind == ir.BlockServerToolUse {
				t = blockTypeServerToolUse
			}
			// 出口 id 统一卫生（hygiene）：透传净化（^[a-zA-Z0-9_-]+$ + ≤64），
			// 缺失 → 合成 `<base>_synth_<响应 id 派生+块序>`（与批量 renderBlock
			// 同键同序；跨响应唯一——客户端把 id 写进历史，同 id 会撞配对/400）。
			id := hygiene.WireToolID("toolu", ev.ToolCallID, w.meta.MessageID, ev.BlockIndex)
			block = map[string]json.RawMessage{
				"type":  mustMarshal(t),
				"id":    mustMarshal(id),
				"name":  mustMarshal(ev.ToolName),
				"input": json.RawMessage("{}"),
			}
		case ir.BlockRefusal:
			// claude 无 refusal 块：以 text 块承载（stop_reason 保持 refusal）。
			block = map[string]json.RawMessage{
				"type": mustMarshal(blockTypeText),
				"text": mustMarshal(""),
			}
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnsupportedBlock, ev.BlockKind)
		}
		if err := writeFrame(evContentBlockStart, map[string]json.RawMessage{
			"type":          mustMarshal(evContentBlockStart),
			"index":         mustMarshal(wi),
			"content_block": mustMarshal(block),
		}); err != nil {
			return nil, err
		}

	case ir.EventBlockDelta:
		if !w.started {
			return nil, fmt.Errorf("%w", ErrMissingStart)
		}
		if _, ok := w.startedBlocks[ev.BlockIndex]; !ok {
			return nil, fmt.Errorf("%w: block_delta for unstarted block %d", ErrBadIR, ev.BlockIndex)
		}
		wi := w.wireIndex[ev.BlockIndex]
		writeDelta := func(delta map[string]json.RawMessage) error {
			return writeFrame(evContentBlockDelta, map[string]json.RawMessage{
				"type":  mustMarshal(evContentBlockDelta),
				"index": mustMarshal(wi),
				"delta": mustMarshal(delta),
			})
		}
		switch ev.BlockKind {
		case ir.BlockToolResult:
			// 已整块丢弃的块（见 EventBlockStart）：delta 一并丢弃，不产帧。
			break
		case ir.BlockText, ir.BlockRefusal:
			if ev.TextDelta == "" {
				break
			}
			if err := writeDelta(map[string]json.RawMessage{
				"type": mustMarshal(deltaTypeText),
				"text": mustMarshal(ev.TextDelta),
			}); err != nil {
				return nil, err
			}
		case ir.BlockThinking:
			if ev.TextDelta != "" {
				if err := writeDelta(map[string]json.RawMessage{
					"type":     mustMarshal(deltaTypeThinking),
					"thinking": mustMarshal(ev.TextDelta),
				}); err != nil {
					return nil, err
				}
			}
			if ev.SignatureDelta != "" {
				if err := writeDelta(map[string]json.RawMessage{
					"type":      mustMarshal(deltaTypeSignature),
					"signature": mustMarshal(ev.SignatureDelta),
				}); err != nil {
					return nil, err
				}
			}
		case ir.BlockRedactedThinking:
			// 载荷随 content_block_start 回填（缓冲到块尾），delta 不产帧。
			if b, ok := w.pendingRedacted[ev.BlockIndex]; ok && ev.SignatureDelta != "" {
				b.WriteString(ev.SignatureDelta)
			}
		case ir.BlockToolUse, ir.BlockServerToolUse:
			if ev.ArgsDelta == "" {
				break
			}
			if err := writeDelta(map[string]json.RawMessage{
				"type":         mustMarshal(deltaTypeInputJSON),
				"partial_json": mustMarshal(ev.ArgsDelta),
			}); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("%w: delta block kind %q", ErrUnsupportedBlock, ev.BlockKind)
		}

	case ir.EventBlockStop:
		kind, ok := w.startedBlocks[ev.BlockIndex]
		if !ok {
			return nil, fmt.Errorf("%w: block_stop for unstarted block %d", ErrBadIR, ev.BlockIndex)
		}
		if b, ok := w.pendingRedacted[ev.BlockIndex]; ok {
			delete(w.pendingRedacted, ev.BlockIndex)
			if err := writeFrame(evContentBlockStart, map[string]json.RawMessage{
				"type":  mustMarshal(evContentBlockStart),
				"index": mustMarshal(w.wireIndex[ev.BlockIndex]),
				"content_block": mustMarshal(map[string]json.RawMessage{
					"type": mustMarshal(blockTypeRedactedThink),
					"data": mustMarshal(b.String()),
				}),
			}); err != nil {
				return nil, err
			}
		}
		delete(w.startedBlocks, ev.BlockIndex)
		if kind == ir.BlockToolResult {
			// 整块丢弃：不产 content_block_stop，也不占下游 index。
			return nil, nil
		}
		wi := w.wireIndex[ev.BlockIndex]
		delete(w.wireIndex, ev.BlockIndex)
		if err := writeFrame(evContentBlockStop, map[string]json.RawMessage{
			"type":  mustMarshal(evContentBlockStop),
			"index": mustMarshal(wi),
		}); err != nil {
			return nil, err
		}

	case ir.EventMessageDelta:
		// usage 与 stop_reason 累积；帧推迟到 message_stop 统一发出
		// （批量语义：message_delta 帧承载合并后全量 usage）。
		if ev.StopReason != "" {
			w.stopReason = ev.StopReason
		}
		if ev.StopSequence != "" {
			w.stopSequence = ev.StopSequence
		}
		if ev.Usage != nil {
			w.usage = ev.Usage
		}

	case ir.EventMessageStop:
		if !w.stopEmitted {
			if w.stopReason == "" {
				return nil, fmt.Errorf("%w", ErrMissingStopReason)
			}
			if err := w.emitMessageDelta(&buf); err != nil {
				return nil, err
			}
		}
		if err := writeFrame(evMessageStop, map[string]json.RawMessage{
			"type": mustMarshal(evMessageStop),
		}); err != nil {
			return nil, err
		}
		w.stopped = true

	case ir.EventDone:
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
		buf.WriteString("event: ")
		buf.WriteString(evError)
		buf.WriteString("\ndata: ")
		buf.Write(eb)
		buf.WriteString("\n\n")
		w.terminal = true

	default:
		return nil, fmt.Errorf("%w: event kind %q", ErrBadIR, ev.Kind)
	}
	return buf.Bytes(), nil
}

// emitMessageDelta 发出 message_delta 帧（stop_reason + 合并后全量 usage）。
//
// stop_sequence 回填（W43-3 §4.3 R1）：IR StopSequence 非空 ⇒ 渲染
// `stop_reason:"stop_sequence"` + `stop_sequence:<值>`（claude.ai 配额截断
// `usage_limit` 经此保真到 Anthropic 线；CC 的 SDK 消费者读该字段——
// topics/cc-fallback-path）。空 ⇒ 维持既有形态（stopReasonFor + null）。
func (w *StreamWriter) emitMessageDelta(buf *bytes.Buffer) error {
	w.stopEmitted = true
	stopReason := mustMarshal(stopReasonFor(w.stopReason))
	stopSequence := json.RawMessage("null")
	if w.stopSequence != "" {
		stopReason = mustMarshal("stop_sequence")
		stopSequence = mustMarshal(w.stopSequence)
	}
	return writeFrameTo(buf, evMessageDelta, map[string]json.RawMessage{
		"type": mustMarshal(evMessageDelta),
		"delta": mustMarshal(map[string]json.RawMessage{
			"stop_reason":   stopReason,
			"stop_sequence": stopSequence,
		}),
		"usage": renderUsage(w.usage),
	})
}

// Finish 事件流结束（上游 EOF）时调用：校验骨架完整性。
//
//   - 未 start 且无终态 → ErrMissingStart；
//   - 未 stopped 且无终态 → ErrMissingMessageStop（不静默补帧掩盖截断）；
//   - message_stop 前缺 stop_reason 已在 Write 处报 ErrMissingStopReason。
func (w *StreamWriter) Finish() ([]byte, error) {
	if !w.started && !w.terminal {
		return nil, fmt.Errorf("%w: empty event stream", ErrMissingStart)
	}
	if !w.stopped && !w.terminal {
		return nil, fmt.Errorf("%w", ErrMissingMessageStop)
	}
	return nil, nil
}

// writeFrameTo 写入一个命名事件帧（event: <name>\ndata: <json>\n\n）。
func writeFrameTo(buf *bytes.Buffer, name string, payload any) error {
	b, err := marshalJSON(payload)
	if err != nil {
		return err
	}
	buf.WriteString("event: ")
	buf.WriteString(name)
	buf.WriteString("\ndata: ")
	buf.Write(b)
	buf.WriteString("\n\n")
	return nil
}
