package gemini

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/dwgx/any2api/translate/ir"
)

// StreamWriter 增量流式渲染器：IR 事件逐个进、下游 SSE 字节逐个出
// （服务壳 relay 用——上游逐帧到达，逐帧写下游）。
//
// 与批量 RenderStream 同一内核（RenderStream = 逐事件 Write + Finish）。
//
// 出口形态（gemini-conversion-table §流式 delta 映射表）：
//   - 文本增量逐帧出（每帧 candidates[0].content.parts[0]）；
//   - thinking 块缓冲到块尾一次成型（签名可能后到，part 必须整体携带
//     thoughtSignature——GW 陷阱 2）；tool_use 块同理（args 半 JSON 缓冲到块尾
//     物化为 functionCall.args 对象）；
//   - finishReason 只在**终块**发一次（stream-mechanics §4.6：终块是合成器职责）；
//   - usageMetadata 只随终块（末帧权威的规范形；上游逐帧复现是自由形态）；
//   - 无 `[DONE]` 哨兵；error 帧后终止、不补哨兵。
type StreamWriter struct {
	started   bool
	doneSeen  bool
	errorSeen bool

	meta ir.StreamEvent

	startedBlocks map[int]ir.BlockKind
	pendingThink  map[int]*pendingThink
	pendingTool   map[int]*pendingTool

	stopReason ir.StopReason
	doneReason ir.StopReason
	usage      *ir.Usage
}

type pendingThink struct {
	text      strings.Builder
	signature strings.Builder
}

type pendingTool struct {
	id        string
	name      string
	args      strings.Builder
	signature strings.Builder
}

// NewStreamWriter 创建增量渲染器。
func NewStreamWriter() *StreamWriter {
	return &StreamWriter{
		startedBlocks: map[int]ir.BlockKind{},
		pendingThink:  map[int]*pendingThink{},
		pendingTool:   map[int]*pendingTool{},
	}
}

// Write 处理一个 IR 事件，返回应写入下游的字节（可能为空）。
func (w *StreamWriter) Write(ev ir.StreamEvent) ([]byte, error) {
	if w.errorSeen || w.doneSeen {
		return nil, fmt.Errorf("%w: event %s", ErrEventsAfterTerminal, ev.Kind)
	}
	var buf bytes.Buffer
	writePart := func(parts []json.RawMessage) error {
		return writeGeminiFrame(&buf, w.meta, parts, "", nil)
	}
	switch ev.Kind {
	case ir.EventStart:
		if w.started {
			return nil, fmt.Errorf("%w: duplicate start event", ErrBadIR)
		}
		w.started = true
		w.meta = ev
		// Gemini 无 start 帧（responseId/modelVersion 钉在每帧上）。

	case ir.EventBlockStart:
		if !w.started {
			return nil, fmt.Errorf("%w", ErrMissingStart)
		}
		if _, dup := w.startedBlocks[ev.BlockIndex]; dup {
			return nil, fmt.Errorf("%w: duplicate block_start %d", ErrBadIR, ev.BlockIndex)
		}
		w.startedBlocks[ev.BlockIndex] = ev.BlockKind
		switch ev.BlockKind {
		case ir.BlockText, ir.BlockRefusal:
			// 无块边界帧；增量即帧。
		case ir.BlockThinking:
			w.pendingThink[ev.BlockIndex] = &pendingThink{}
		case ir.BlockToolUse, ir.BlockServerToolUse:
			w.pendingTool[ev.BlockIndex] = &pendingTool{id: ev.ToolCallID, name: ev.ToolName}
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnsupportedBlock, ev.BlockKind)
		}

	case ir.EventBlockDelta:
		if !w.started {
			return nil, fmt.Errorf("%w", ErrMissingStart)
		}
		if _, ok := w.startedBlocks[ev.BlockIndex]; !ok {
			return nil, fmt.Errorf("%w: block_delta for unstarted block %d", ErrBadIR, ev.BlockIndex)
		}
		switch ev.BlockKind {
		case ir.BlockText, ir.BlockRefusal:
			if ev.TextDelta == "" {
				break
			}
			if err := writePart([]json.RawMessage{mustMarshal(map[string]json.RawMessage{
				"text": mustMarshal(ev.TextDelta),
			})}); err != nil {
				return nil, err
			}
		case ir.BlockThinking:
			p := w.pendingThink[ev.BlockIndex]
			if p == nil {
				return nil, fmt.Errorf("%w: thinking delta without pending block %d", ErrBadIR, ev.BlockIndex)
			}
			p.text.WriteString(ev.TextDelta)
			p.signature.WriteString(ev.SignatureDelta)
		case ir.BlockToolUse, ir.BlockServerToolUse:
			p := w.pendingTool[ev.BlockIndex]
			if p == nil {
				return nil, fmt.Errorf("%w: tool_use delta without pending block %d", ErrBadIR, ev.BlockIndex)
			}
			p.args.WriteString(ev.ArgsDelta)
			p.signature.WriteString(ev.SignatureDelta)
		default:
			return nil, fmt.Errorf("%w: delta block kind %q", ErrUnsupportedBlock, ev.BlockKind)
		}

	case ir.EventBlockStop:
		if _, ok := w.startedBlocks[ev.BlockIndex]; !ok {
			return nil, fmt.Errorf("%w: block_stop for unstarted block %d", ErrBadIR, ev.BlockIndex)
		}
		delete(w.startedBlocks, ev.BlockIndex)
		switch ev.BlockKind {
		case ir.BlockText, ir.BlockRefusal:
			// 无块边界帧。
		case ir.BlockThinking:
			p := w.pendingThink[ev.BlockIndex]
			delete(w.pendingThink, ev.BlockIndex)
			if p == nil {
				break
			}
			if p.text.Len() == 0 && p.signature.Len() == 0 {
				break
			}
			part := map[string]json.RawMessage{
				"text":    mustMarshal(p.text.String()),
				"thought": mustMarshal(true),
			}
			if sig := replayableThoughtSignature(p.signature.String()); sig != "" {
				part["thoughtSignature"] = mustMarshal(sig)
			}
			if err := writePart([]json.RawMessage{mustMarshal(part)}); err != nil {
				return nil, err
			}
		case ir.BlockToolUse, ir.BlockServerToolUse:
			p := w.pendingTool[ev.BlockIndex]
			delete(w.pendingTool, ev.BlockIndex)
			if p == nil {
				break
			}
			args, err := toolArgsObject(p.args.String())
			if err != nil {
				return nil, err
			}
			fc := map[string]json.RawMessage{
				"name": mustMarshal(p.name),
				"args": args,
			}
			if p.id != "" {
				fc["id"] = mustMarshal(p.id)
			}
			part := map[string]json.RawMessage{"functionCall": mustMarshal(fc)}
			if p.signature.Len() > 0 {
				part["thoughtSignature"] = mustMarshal(p.signature.String())
			}
			if err := writePart([]json.RawMessage{mustMarshal(part)}); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("%w: block_stop kind %q", ErrUnsupportedBlock, ev.BlockKind)
		}

	case ir.EventMessageDelta:
		if !w.started {
			return nil, fmt.Errorf("%w", ErrMissingStart)
		}
		if ev.StopReason != "" {
			w.stopReason = ev.StopReason
		}
		if ev.Usage != nil {
			w.usage = ev.Usage
		}

	case ir.EventMessageStop:
		// 终块在 Finish 统一发出（finishReason 只发一次）。

	case ir.EventDone:
		w.doneSeen = true
		if ev.Reason != "" {
			w.doneReason = ev.Reason
		}

	case ir.EventError:
		ee := ev.Error
		if ee == nil {
			ee = &ir.Error{Type: "api_error", Message: "stream error", Source: "any2api"}
		}
		eb, err := RenderError(ee)
		if err != nil {
			return nil, err
		}
		// 错误即终止；不补哨兵（Gemini 无哨兵）。
		buf.WriteString("data: ")
		buf.Write(eb)
		buf.WriteString("\n\n")
		w.errorSeen = true

	default:
		return nil, fmt.Errorf("%w: event kind %q", ErrBadIR, ev.Kind)
	}
	return buf.Bytes(), nil
}

// Finish 事件流结束（上游 EOF）时调用：冲刷未收尾的缓冲块并补终块。
//
//   - 错误帧已发 → 无补充；
//   - 未收尾的 thinking/tool 块 → 以当前缓冲收尾（悬空块不静默丢）；
//   - 无 stop_reason（message_delta 与 done 都未给）→ ErrMissingStopReason
//     （不合成默认值掩盖截断——与 claude_messages CC B7 硬门对称）。
func (w *StreamWriter) Finish() ([]byte, error) {
	if !w.started && !w.errorSeen {
		return nil, fmt.Errorf("%w: empty event stream", ErrMissingStart)
	}
	if w.errorSeen {
		return nil, nil
	}
	var buf bytes.Buffer
	// 悬空块收尾（正常流不会出现；防御性冲刷，顺序稳定）。
	if len(w.pendingThink) > 0 || len(w.pendingTool) > 0 {
		idxs := make([]int, 0, len(w.pendingThink)+len(w.pendingTool))
		for i := range w.pendingThink {
			idxs = append(idxs, i)
		}
		for i := range w.pendingTool {
			idxs = append(idxs, i)
		}
		sort.Ints(idxs)
		for _, i := range idxs {
			if p, ok := w.pendingThink[i]; ok {
				if p.text.Len() > 0 || p.signature.Len() > 0 {
					part := map[string]json.RawMessage{
						"text":    mustMarshal(p.text.String()),
						"thought": mustMarshal(true),
					}
					if sig := replayableThoughtSignature(p.signature.String()); sig != "" {
						part["thoughtSignature"] = mustMarshal(sig)
					}
					if err := writeGeminiFrame(&buf, w.meta, []json.RawMessage{mustMarshal(part)}, "", nil); err != nil {
						return nil, err
					}
				}
			}
			if p, ok := w.pendingTool[i]; ok {
				args, err := toolArgsObject(p.args.String())
				if err != nil {
					return nil, err
				}
				fc := map[string]json.RawMessage{
					"name": mustMarshal(p.name),
					"args": args,
				}
				if p.id != "" {
					fc["id"] = mustMarshal(p.id)
				}
				part := map[string]json.RawMessage{"functionCall": mustMarshal(fc)}
				if p.signature.Len() > 0 {
					part["thoughtSignature"] = mustMarshal(p.signature.String())
				}
				if err := writeGeminiFrame(&buf, w.meta, []json.RawMessage{mustMarshal(part)}, "", nil); err != nil {
					return nil, err
				}
			}
		}
		w.pendingThink = map[int]*pendingThink{}
		w.pendingTool = map[int]*pendingTool{}
	}
	reason := w.stopReason
	if reason == "" {
		reason = w.doneReason
	}
	if reason == "" {
		return nil, fmt.Errorf("%w", ErrMissingStopReason)
	}
	var usage json.RawMessage
	if w.usage != nil {
		usage = renderUsageMetadata(w.usage)
	}
	// 终块：空 parts + finishReason（+usage）——「无 parts 末帧」形态。
	if err := writeGeminiFrame(&buf, w.meta, []json.RawMessage{}, finishReasonFor(reason), usage); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// writeGeminiFrame 写入一个 `data: {GenerateContentResponse}` 帧。
// responseId/modelVersion 钉在每帧上（表：ID 全流钉住首帧值）。
func writeGeminiFrame(buf *bytes.Buffer, meta ir.StreamEvent, parts []json.RawMessage, finishReason string, usage json.RawMessage) error {
	candidate := map[string]json.RawMessage{
		"index": mustMarshal(0),
		"content": mustMarshal(map[string]json.RawMessage{
			"role":  mustMarshal(roleModel),
			"parts": mustMarshal(parts),
		}),
	}
	if finishReason != "" {
		candidate["finishReason"] = mustMarshal(finishReason)
	}
	out := map[string]json.RawMessage{
		"candidates": mustMarshal([]json.RawMessage{mustMarshal(candidate)}),
	}
	if meta.MessageID != "" {
		out["responseId"] = mustMarshal(meta.MessageID)
	}
	if meta.Model != "" {
		out["modelVersion"] = mustMarshal(meta.Model)
	}
	if len(usage) > 0 {
		out["usageMetadata"] = usage
	}
	b, err := marshalJSON(out)
	if err != nil {
		return err
	}
	buf.WriteString("data: ")
	buf.Write(b)
	buf.WriteString("\n\n")
	return nil
}
