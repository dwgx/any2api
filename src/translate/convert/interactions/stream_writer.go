package interactions

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/dwgx/any2api/translate/hygiene"
	"github.com/dwgx/any2api/translate/ir"
)

// StreamWriter 增量流式渲染器：IR 事件逐个进、下游 SSE 字节逐个出
// （服务壳 relay 用——上游逐帧到达，逐帧写下游）。
//
// 出口形态（官方帧序 ST:52-86，逐字复现）：
//
//	interaction.created → interaction.status_update →
//	(step.start → step.delta* → step.stop)* →
//	interaction.completed（usage 在此，**不含 steps**）→ done（data: [DONE]）
//
// 纪律：
//   - `event:` 行与 payload 的 `event_type` 两处一致（官方全部例子如此，
//     ST:52-86；解析侧同款校验）；
//   - `arguments_delta` 分片直通（IR ArgsDelta 即片段语义；累积是消费端契约，
//     BC:413-419）——本 writer 不做累积，出口修复层已在 IR 侧收口；
//   - function_call 步用 **`id`** 键（Q1），空 id 确定性合成；step.start 带空
//     arguments `{}`（官方形态）；
//   - usage 只在 interaction.completed（ST:178-181）；step.stop 不带 usage；
//   - 失败 = `event: error`（code 为字符串）+ **不发** `[DONE]`（官方 error 例
//     ST:186-188 无哨兵；已开 SSE 后绝不写 JSON HTTP 错误体）。
type StreamWriter struct {
	started   bool
	terminal  bool
	errorSeen bool

	id      string
	model   string
	created int64

	blocks  []*wBlock
	byIndex map[int]*wBlock

	usage      *ir.Usage
	stopReason ir.StopReason
	doneReason ir.StopReason
	seq        int
}

// wBlock 一个步（step）的流式缓冲。
type wBlock struct {
	irIndex  int // IR 块定位键（可能带洞——上游块序）
	index    int // 线格式 step index（稠密重编号：0,1,2…——官方形态）
	kind     ir.BlockKind
	callID   string
	name     string
	closed   bool
	seenStep bool
}

// NewStreamWriter 创建增量渲染器。
func NewStreamWriter() *StreamWriter {
	return &StreamWriter{byIndex: map[int]*wBlock{}}
}

// Write 处理一个 IR 事件，返回应写入下游的字节（可能为空）。
func (w *StreamWriter) Write(ev ir.StreamEvent) ([]byte, error) {
	if w.errorSeen || w.terminal {
		return nil, fmt.Errorf("%w: event %s", ErrEventsAfterTerminal, ev.Kind)
	}
	var buf bytes.Buffer
	switch ev.Kind {
	case ir.EventStart:
		if w.started {
			return nil, fmt.Errorf("%w: duplicate start event", ErrBadIR)
		}
		w.started = true
		w.id = ev.MessageID
		if w.id == "" {
			w.id = hygiene.SynthUniqueID("int") // 每响应唯一（旧常量 int_synth 跨响应同 id）
		}
		w.model = ev.Model
		w.created = ev.Created
		w.frame(&buf, evInteractionCreated, map[string]json.RawMessage{
			"interaction": mustMarshal(map[string]json.RawMessage{
				"id":     mustMarshal(w.id),
				"status": mustMarshal(statusInProgress),
				"object": mustMarshal(objectInteraction),
				"model":  mustMarshal(w.model),
			}),
		})
		w.frame(&buf, evInteractionStatus, map[string]json.RawMessage{
			"interaction_id": mustMarshal(w.id),
			"status":         mustMarshal(statusInProgress),
		})

	case ir.EventBlockStart:
		if !w.started {
			return nil, fmt.Errorf("%w", ErrMissingStart)
		}
		if _, dup := w.byIndex[ev.BlockIndex]; dup {
			return nil, fmt.Errorf("%w: duplicate block_start %d", ErrBadIR, ev.BlockIndex)
		}
		// step index 稠密重编号（0,1,2…）：IR BlockIndex 是源格式块序（可能带洞，
		// 如被丢弃的服务端工具步），官方 step index 是时间线位置。
		blk := &wBlock{
			irIndex: ev.BlockIndex,
			index:   len(w.blocks),
			kind:    ev.BlockKind,
			callID:  ev.ToolCallID,
			name:    ev.ToolName,
		}
		if blk.kind == ir.BlockToolUse || blk.kind == ir.BlockServerToolUse {
			// 出口 id 统一卫生（hygiene）：净化 + 缺失合成（响应 id 派生 + 块序；
			// id 必填，OAS:FunctionCallStep.required；旧序号形态跨响应重复）。
			blk.callID = hygiene.WireToolID("call", ev.ToolCallID, w.id, ev.BlockIndex)
		}
		w.byIndex[ev.BlockIndex] = blk
		w.blocks = append(w.blocks, blk)
		step, err := blk.startStep()
		if err != nil {
			return nil, err
		}
		blk.seenStep = true
		w.frame(&buf, evStepStart, map[string]json.RawMessage{
			"index": mustMarshal(blk.index),
			"step":  mustMarshal(step),
		})

	case ir.EventBlockDelta:
		blk, ok := w.byIndex[ev.BlockIndex]
		if !ok {
			return nil, fmt.Errorf("%w: block_delta for unstarted block %d", ErrBadIR, ev.BlockIndex)
		}
		deltas, err := blk.deltaPayloads(ev)
		if err != nil {
			return nil, err
		}
		for _, d := range deltas {
			w.frame(&buf, evStepDelta, map[string]json.RawMessage{
				"index": mustMarshal(blk.index),
				"delta": mustMarshal(d),
			})
		}

	case ir.EventBlockStop:
		blk, ok := w.byIndex[ev.BlockIndex]
		if !ok {
			return nil, fmt.Errorf("%w: block_stop for unstarted block %d", ErrBadIR, ev.BlockIndex)
		}
		w.frame(&buf, evStepStop, map[string]json.RawMessage{
			"index": mustMarshal(blk.index),
		})
		blk.closed = true

	case ir.EventMessageDelta:
		if !w.started {
			return nil, fmt.Errorf("%w", ErrMissingStart)
		}
		if ev.StopReason != "" {
			w.stopReason = ev.StopReason
		}
		if ev.Usage != nil {
			w.usage = mergeUsage(w.usage, ev.Usage)
		}

	case ir.EventMessageStop:
		// 终止事件在 done 统一发出（interaction.completed + 哨兵）。

	case ir.EventDone:
		w.terminal = true
		if ev.Reason != "" {
			w.doneReason = ev.Reason
		}
		reason := w.stopReason
		if reason == "" {
			reason = w.doneReason
		}
		if err := w.emitTerminal(&buf, reason); err != nil {
			return nil, err
		}

	case ir.EventError:
		w.errorSeen = true
		ee := ev.Error
		if ee == nil {
			ee = &ir.Error{Type: "api_error", Message: "stream error", Source: "any2api"}
		}
		// SSE 错误事件的 code 是**字符串**（ST:186-188）；没有 code 时用归一类型
		// 兜底（客户端可读），绝不发数值 code。
		code := ee.Code
		if code == "" {
			code = ee.Type
		}
		if code == "" {
			code = "api_error"
		}
		w.frame(&buf, evError, map[string]json.RawMessage{
			"error": mustMarshal(map[string]json.RawMessage{
				"message": mustMarshal(ee.Message),
				"code":    mustMarshal(code),
			}),
		})

	default:
		return nil, fmt.Errorf("%w: event kind %q", ErrBadIR, ev.Kind)
	}
	return buf.Bytes(), nil
}

// Finish 事件流结束（上游 EOF）时调用：冲刷未收尾的步并补终止事件。
//
//   - 错误/终止已发 → 无补充；
//   - 悬空步 → 以 step.stop 收尾（悬空步不静默丢）；
//   - 无 stop_reason（message_delta 与 done 都未给）→ ErrMissingStopReason
//     （不合成默认值掩盖截断——与 claude_messages CC B7 / gemini / responses 硬门对称）。
func (w *StreamWriter) Finish() ([]byte, error) {
	if w.errorSeen {
		return nil, nil
	}
	if !w.started {
		return nil, fmt.Errorf("%w: empty event stream", ErrMissingStart)
	}
	if w.terminal {
		return nil, nil
	}
	var buf bytes.Buffer
	for _, blk := range w.blocks {
		if !blk.closed {
			w.frame(&buf, evStepStop, map[string]json.RawMessage{
				"index": mustMarshal(blk.index),
			})
			blk.closed = true
		}
	}
	reason := w.stopReason
	if reason == "" {
		reason = w.doneReason
	}
	if reason == "" {
		return nil, fmt.Errorf("%w", ErrMissingStopReason)
	}
	if err := w.emitTerminal(&buf, reason); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// startStep step.start 的 step 对象（按块类型）。
func (b *wBlock) startStep() (map[string]json.RawMessage, error) {
	switch b.kind {
	case ir.BlockText:
		return map[string]json.RawMessage{"type": mustMarshal(stepModelOutput)}, nil
	case ir.BlockThinking:
		return map[string]json.RawMessage{"type": mustMarshal(stepThought)}, nil
	case ir.BlockToolUse, ir.BlockServerToolUse:
		return map[string]json.RawMessage{
			"type":      mustMarshal(stepFunctionCall),
			"id":        mustMarshal(b.callID),
			"name":      mustMarshal(b.name),
			"arguments": json.RawMessage("{}"),
		}, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedBlock, b.kind)
	}
}

// deltaPayloads 一个 block_delta 事件 → 一个或多个 step.delta 载荷。
func (b *wBlock) deltaPayloads(ev ir.StreamEvent) ([]map[string]json.RawMessage, error) {
	switch b.kind {
	case ir.BlockText:
		return []map[string]json.RawMessage{{
			"type": mustMarshal(deltaText),
			"text": mustMarshal(ev.TextDelta),
		}}, nil
	case ir.BlockThinking:
		var out []map[string]json.RawMessage
		switch {
		case ev.SignatureDelta != "":
			if ev.TextDelta != "" {
				out = append(out, thoughtSummaryDelta(ev.TextDelta))
			}
			out = append(out, map[string]json.RawMessage{
				"type":      mustMarshal(deltaThoughtSignature),
				"signature": mustMarshal(ev.SignatureDelta),
			})
		case ev.TextDelta != "":
			out = append(out, thoughtSummaryDelta(ev.TextDelta))
		default:
			// 双空（病态）：发空摘要保持帧存在（round trip 不丢事件）。
			out = append(out, thoughtSummaryDelta(""))
		}
		return out, nil
	case ir.BlockToolUse, ir.BlockServerToolUse:
		return []map[string]json.RawMessage{{
			"type":      mustMarshal(deltaArguments),
			"arguments": mustMarshal(ev.ArgsDelta),
		}}, nil
	default:
		return nil, fmt.Errorf("%w: delta block kind %q", ErrUnsupportedBlock, b.kind)
	}
}

// thoughtSummaryDelta thought 步的摘要增量（content 是 Content 联合，文本形态）。
func thoughtSummaryDelta(text string) map[string]json.RawMessage {
	return map[string]json.RawMessage{
		"type": mustMarshal(deltaThoughtSummary),
		"content": mustMarshal(map[string]json.RawMessage{
			"type": mustMarshal(contentText),
			"text": mustMarshal(text),
		}),
	}
}

// emitTerminal interaction.completed + done 哨兵。
func (w *StreamWriter) emitTerminal(buf *bytes.Buffer, reason ir.StopReason) error {
	if reason == "" {
		return fmt.Errorf("%w", ErrMissingStopReason)
	}
	w.terminal = true
	created := formatRFC3339(w.created)
	iw := map[string]json.RawMessage{
		"id":      mustMarshal(w.id),
		"object":  mustMarshal(objectInteraction),
		"status":  mustMarshal(statusFor(reason)),
		"model":   mustMarshal(w.model),
		"created": mustMarshal(created),
		"updated": mustMarshal(created),
	}
	if w.usage != nil {
		iw["usage"] = renderUsage(w.usage)
	}
	w.frame(buf, evInteractionCompleted, map[string]json.RawMessage{
		"interaction": mustMarshal(iw),
	})
	// 哨兵帧：`event: done` + `data: [DONE]`（非 JSON 载荷，手写）。
	buf.WriteString("event: ")
	buf.WriteString(evDone)
	buf.WriteString("\ndata: ")
	buf.WriteString(doneSentinel)
	buf.WriteString("\n\n")
	return nil
}

// frame 写一个 SSE 帧：`event: <type>` + `data: {…,"event_type":<type>}` + 空行。
// 两处事件名必须一致（官方全部例子如此；解析侧校验）。
func (w *StreamWriter) frame(buf *bytes.Buffer, evType string, payload map[string]json.RawMessage) {
	payload["event_type"] = mustMarshal(evType)
	data, err := json.Marshal(payload)
	if err != nil {
		// 载荷全为 mustMarshal 产物——Marshal 失败 = 编程错误。
		panic(err)
	}
	buf.WriteString("event: ")
	buf.WriteString(evType)
	buf.WriteString("\ndata: ")
	buf.Write(data)
	buf.WriteString("\n\n")
}
