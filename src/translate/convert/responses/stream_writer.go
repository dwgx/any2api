package responses

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dwgx/any2api/translate/hygiene"
	"github.com/dwgx/any2api/translate/ir"
)

// StreamWriter 增量流式渲染器：IR 事件逐个进、下游 SSE 字节逐个出
// （服务壳 relay 用——上游逐帧到达，逐帧写下游）。
//
// 出口形态（官方示例帧序 [C:16468-16501]，逐字复现）：
//
//	response.created → response.in_progress →
//	response.output_item.added → response.content_part.added →
//	response.output_text.delta* → response.output_text.done →
//	response.content_part.done → response.output_item.done →
//	response.completed（**usage 在 completed 的 response.usage 里**）
//
// 纪律：
//   - `sequence_number` 逐事件分配（从 0 起）——合成端的显式选择（表 §4.1：
//     「消费端不能把它当必填，合成端不能把它当可选」）；
//   - 无 `[DONE]` 哨兵（官方以终止事件收束）；
//   - redacted_thinking 块的 output_item.added **推迟到块尾**：encrypted_content
//     必须整体携带（`claude-redacted-thinking:` 前缀载体），提前公告会把类型
//     判成 thinking（回程还原失败）；
//   - 失败 = `error` 事件（平铺字段）+ `response.failed`（含 response.error），
//     已开 SSE 后绝不写 JSON HTTP 错误体（表 §4.5）。
type StreamWriter struct {
	started   bool
	terminal  bool
	errorSeen bool
	seq       int64

	id      string
	model   string
	created int64

	items   []*wItem
	byIndex map[int]*wItem

	usage      *ir.Usage
	stopReason ir.StopReason
	doneReason ir.StopReason
}

// wItem 一个输出项（message / reasoning / function_call）的流式缓冲。
type wItem struct {
	outputIndex int
	itemID      string
	itemType    string
	blockIndex  int
	kind        ir.BlockKind
	closed      bool

	partKind ir.BlockKind // message 项：text | refusal
	text     strings.Builder

	sig      strings.Builder // reasoning：encrypted_content / redacted 载荷
	redacted bool

	callID string
	name   string
	args   strings.Builder
}

// NewStreamWriter 创建增量渲染器。
func NewStreamWriter() *StreamWriter {
	return &StreamWriter{byIndex: map[int]*wItem{}}
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
			w.id = hygiene.SynthUniqueID("resp")
		}
		w.model = ev.Model
		w.created = ev.Created
		w.frame(&buf, evCreated, map[string]json.RawMessage{
			"response": mustMarshal(w.responseObject(statusInProgress)),
		})
		w.frame(&buf, evInProgress, map[string]json.RawMessage{
			"response": mustMarshal(w.responseObject(statusInProgress)),
		})

	case ir.EventBlockStart:
		if !w.started {
			return nil, fmt.Errorf("%w", ErrMissingStart)
		}
		if _, dup := w.byIndex[ev.BlockIndex]; dup {
			return nil, fmt.Errorf("%w: duplicate block_start %d", ErrBadIR, ev.BlockIndex)
		}
		it := &wItem{
			outputIndex: len(w.items),
			// 项 id 确定性（响应 id 派生 + 块序；渲染不动点与录帧回放都靠它）
			// ——经 hygiene 统一形态 `<prefix>_synth_<32hex>`。
			itemID:     hygiene.SynthDerivedID(itemIDPrefix(ev.BlockKind), w.id, len(w.items)),
			blockIndex: ev.BlockIndex,
			kind:       ev.BlockKind,
		}
		w.byIndex[ev.BlockIndex] = it
		w.items = append(w.items, it)

		switch ev.BlockKind {
		case ir.BlockText, ir.BlockRefusal:
			it.itemType, it.partKind = itemTypeMessage, ev.BlockKind
			w.frame(&buf, evOutputItemAdded, map[string]json.RawMessage{
				"output_index": mustMarshal(it.outputIndex),
				"item": mustMarshal(map[string]json.RawMessage{
					"id":      mustMarshal(it.itemID),
					"type":    mustMarshal(itemTypeMessage),
					"status":  mustMarshal(statusInProgress),
					"role":    mustMarshal(roleAssistant),
					"content": json.RawMessage("[]"),
				}),
			})
			w.frame(&buf, evContentPartAdded, map[string]json.RawMessage{
				"item_id":       mustMarshal(it.itemID),
				"output_index":  mustMarshal(it.outputIndex),
				"content_index": mustMarshal(0),
				"part":          mustMarshal(w.emptyPart(it.partKind)),
			})
		case ir.BlockThinking:
			it.itemType = itemTypeReasoning
			w.frame(&buf, evOutputItemAdded, map[string]json.RawMessage{
				"output_index": mustMarshal(it.outputIndex),
				"item": mustMarshal(map[string]json.RawMessage{
					"id":      mustMarshal(it.itemID),
					"type":    mustMarshal(itemTypeReasoning),
					"status":  mustMarshal(statusInProgress),
					"summary": json.RawMessage("[]"),
				}),
			})
		case ir.BlockRedactedThinking:
			// added 推迟到块尾（encrypted_content 必须整体携带）。
			it.itemType, it.redacted = itemTypeReasoning, true
		case ir.BlockToolUse, ir.BlockServerToolUse:
			it.itemType = itemTypeFunctionCall
			it.callID, it.name = hygiene.WireToolID("call", ev.ToolCallID, w.id, ev.BlockIndex), ev.ToolName
			w.frame(&buf, evOutputItemAdded, map[string]json.RawMessage{
				"output_index": mustMarshal(it.outputIndex),
				"item": mustMarshal(map[string]json.RawMessage{
					"id":        mustMarshal(it.itemID),
					"type":      mustMarshal(itemTypeFunctionCall),
					"status":    mustMarshal(statusInProgress),
					"call_id":   mustMarshal(it.callID),
					"name":      mustMarshal(it.name),
					"arguments": mustMarshal(""),
				}),
			})
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnsupportedBlock, ev.BlockKind)
		}

	case ir.EventBlockDelta:
		it, ok := w.byIndex[ev.BlockIndex]
		if !ok {
			return nil, fmt.Errorf("%w: block_delta for unstarted block %d", ErrBadIR, ev.BlockIndex)
		}
		switch it.kind {
		case ir.BlockText, ir.BlockRefusal:
			if ev.TextDelta == "" {
				break
			}
			it.text.WriteString(ev.TextDelta)
			evType := evOutputTextDelta
			if it.kind == ir.BlockRefusal {
				evType = evRefusalDelta
			}
			payload := map[string]json.RawMessage{
				"item_id":       mustMarshal(it.itemID),
				"output_index":  mustMarshal(it.outputIndex),
				"content_index": mustMarshal(0),
				"delta":         mustMarshal(ev.TextDelta),
			}
			if it.kind == ir.BlockText {
				payload["logprobs"] = json.RawMessage("[]")
			}
			w.frame(&buf, evType, payload)
		case ir.BlockThinking:
			if ev.TextDelta != "" {
				it.text.WriteString(ev.TextDelta)
				w.frame(&buf, evReasoningTextDelta, map[string]json.RawMessage{
					"item_id":       mustMarshal(it.itemID),
					"output_index":  mustMarshal(it.outputIndex),
					"summary_index": mustMarshal(0),
					"delta":         mustMarshal(ev.TextDelta),
				})
			}
			it.sig.WriteString(ev.SignatureDelta)
		case ir.BlockRedactedThinking:
			it.sig.WriteString(ev.SignatureDelta)
		case ir.BlockToolUse, ir.BlockServerToolUse:
			if ev.ArgsDelta == "" {
				break
			}
			it.args.WriteString(ev.ArgsDelta)
			w.frame(&buf, evFunctionArgsDelta, map[string]json.RawMessage{
				"item_id":      mustMarshal(it.itemID),
				"output_index": mustMarshal(it.outputIndex),
				"delta":        mustMarshal(ev.ArgsDelta),
			})
		default:
			return nil, fmt.Errorf("%w: delta block kind %q", ErrUnsupportedBlock, it.kind)
		}

	case ir.EventBlockStop:
		it, ok := w.byIndex[ev.BlockIndex]
		if !ok {
			return nil, fmt.Errorf("%w: block_stop for unstarted block %d", ErrBadIR, ev.BlockIndex)
		}
		if err := w.closeItem(&buf, it); err != nil {
			return nil, err
		}

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
		// 终止事件在 done 统一发出（含全量 output 与 usage）。

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
		// 错误码缺省统一为 server_error（W5-20 条目 4：与服务壳 writeResponsesError
		// 同款缺省——两条错误出口帧形状逐字同构，客户端不会「有时有 code、有时
		// 没有」；new-api Fail() 同款缺省）。
		code := ee.Code
		if code == "" {
			code = "server_error"
		}
		// 平铺错误事件（**无嵌套 error 对象**）+ response.failed（含 response.error）。
		payload := map[string]json.RawMessage{
			"message": mustMarshal(ee.Message),
			"code":    mustMarshal(code),
		}
		if ee.Param != "" {
			payload["param"] = mustMarshal(ee.Param)
		}
		w.frame(&buf, evError, payload)
		errObj := map[string]json.RawMessage{"message": mustMarshal(ee.Message), "code": mustMarshal(code)}
		resp := w.responseObject(statusFailed)
		resp["error"] = mustMarshal(errObj)
		w.frame(&buf, evFailed, map[string]json.RawMessage{"response": mustMarshal(resp)})

	default:
		return nil, fmt.Errorf("%w: event kind %q", ErrBadIR, ev.Kind)
	}
	return buf.Bytes(), nil
}

// Finish 事件流结束（上游 EOF）时调用：冲刷未收尾的块并补终止事件。
//
//   - 错误/终止已发 → 无补充；
//   - 悬空块 → 以当前缓冲收尾（悬空块不静默丢）；
//   - 无 stop_reason（message_delta 与 done 都未给）→ ErrMissingStopReason
//     （不合成默认值掩盖截断——与 claude_messages CC B7 / gemini 硬门对称）。
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
	for _, it := range w.items {
		if !it.closed {
			if err := w.closeItem(&buf, it); err != nil {
				return nil, err
			}
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

// closeItem 输出一个项的收尾帧组（幂等）。
func (w *StreamWriter) closeItem(buf *bytes.Buffer, it *wItem) error {
	if it.closed {
		return nil
	}
	it.closed = true
	switch it.kind {
	case ir.BlockText, ir.BlockRefusal:
		text := it.text.String()
		if it.kind == ir.BlockText {
			w.frame(buf, evOutputTextDone, map[string]json.RawMessage{
				"item_id":       mustMarshal(it.itemID),
				"output_index":  mustMarshal(it.outputIndex),
				"content_index": mustMarshal(0),
				"text":          mustMarshal(text),
				"logprobs":      json.RawMessage("[]"),
			})
		} else {
			w.frame(buf, evRefusalDone, map[string]json.RawMessage{
				"item_id":       mustMarshal(it.itemID),
				"output_index":  mustMarshal(it.outputIndex),
				"content_index": mustMarshal(0),
				"refusal":       mustMarshal(text),
			})
		}
		w.frame(buf, evContentPartDone, map[string]json.RawMessage{
			"item_id":       mustMarshal(it.itemID),
			"output_index":  mustMarshal(it.outputIndex),
			"content_index": mustMarshal(0),
			"part":          mustMarshal(w.filledPart(it)),
		})
		w.frame(buf, evOutputItemDone, map[string]json.RawMessage{
			"output_index": mustMarshal(it.outputIndex),
			"item":         mustMarshal(w.messageItem(it)),
		})

	case ir.BlockThinking:
		if text := it.text.String(); text != "" {
			w.frame(buf, evReasoningTextDone, map[string]json.RawMessage{
				"item_id":       mustMarshal(it.itemID),
				"output_index":  mustMarshal(it.outputIndex),
				"summary_index": mustMarshal(0),
				"text":          mustMarshal(text),
			})
		}
		w.frame(buf, evOutputItemDone, map[string]json.RawMessage{
			"output_index": mustMarshal(it.outputIndex),
			"item":         mustMarshal(w.reasoningItem(it)),
		})

	case ir.BlockRedactedThinking:
		// added 推迟到此处（encrypted_content 整体携带）——随后立即 done。
		w.frame(buf, evOutputItemAdded, map[string]json.RawMessage{
			"output_index": mustMarshal(it.outputIndex),
			"item":         mustMarshal(w.reasoningItem(it)),
		})
		w.frame(buf, evOutputItemDone, map[string]json.RawMessage{
			"output_index": mustMarshal(it.outputIndex),
			"item":         mustMarshal(w.reasoningItem(it)),
		})

	case ir.BlockToolUse, ir.BlockServerToolUse:
		args := it.args.String()
		w.frame(buf, evFunctionArgsDone, map[string]json.RawMessage{
			"item_id":      mustMarshal(it.itemID),
			"output_index": mustMarshal(it.outputIndex),
			"arguments":    mustMarshal(args),
		})
		w.frame(buf, evOutputItemDone, map[string]json.RawMessage{
			"output_index": mustMarshal(it.outputIndex),
			"item":         mustMarshal(w.functionCallItem(it)),
		})

	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedBlock, it.kind)
	}
	return nil
}

// emitTerminal 终止事件（response.completed / response.incomplete）。
func (w *StreamWriter) emitTerminal(buf *bytes.Buffer, reason ir.StopReason) error {
	if reason == "" {
		return fmt.Errorf("%w", ErrMissingStopReason)
	}
	status, _ := statusFor(reason)
	if reason == ir.StopLength || reason == ir.StopError {
		w.terminal = true
		w.frame(buf, evIncomplete, map[string]json.RawMessage{
			"response": mustMarshal(w.responseObject(status)),
		})
		return nil
	}
	w.terminal = true
	w.frame(buf, evCompleted, map[string]json.RawMessage{
		"response": mustMarshal(w.responseObject(statusCompleted)),
	})
	return nil
}

// responseObject 组装 Response 对象（created/in_progress/completed/failed 共用；
// usage 只在终止事件非 null——表 §4.4）。
func (w *StreamWriter) responseObject(status string) map[string]json.RawMessage {
	output := make([]json.RawMessage, 0, len(w.items))
	for _, it := range w.items {
		if !it.closed {
			continue
		}
		switch it.kind {
		case ir.BlockText, ir.BlockRefusal:
			output = append(output, mustMarshal(w.messageItem(it)))
		case ir.BlockThinking, ir.BlockRedactedThinking:
			output = append(output, mustMarshal(w.reasoningItem(it)))
		case ir.BlockToolUse, ir.BlockServerToolUse:
			output = append(output, mustMarshal(w.functionCallItem(it)))
		}
	}
	out := map[string]json.RawMessage{
		"id":         mustMarshal(w.id),
		"object":     mustMarshal(objectResponse),
		"created_at": mustMarshal(w.created),
		"status":     mustMarshal(status),
		"model":      mustMarshal(w.model),
		"output":     mustMarshal(output),
		// SDK 必填三键（openai-python 3.29.0 实测，P01）：spec 默认值
		// （parallel_tool_calls=true 有显式 default；tool_choice="auto"/tools=[]
		// 取 spec 自证示例）。流式面拿不到请求面/响应 Extra（事件流无该通道），
		// 恒为 spec 默认——见 W5-30 报告「取值纪律」。
		"parallel_tool_calls": json.RawMessage("true"),
		"tool_choice":         mustMarshal("auto"),
		"tools":               json.RawMessage("[]"),
	}
	if status == statusCompleted || status == statusIncomplete || status == statusFailed {
		if w.usage != nil {
			out["usage"] = renderUsage(w.usage)
		} else {
			out["usage"] = json.RawMessage("null")
		}
	} else {
		out["usage"] = json.RawMessage("null")
	}
	if status == statusIncomplete {
		_, details := statusFor(w.stopReason)
		if details != nil {
			out["incomplete_details"] = details
		}
	}
	return out
}

// messageItem message 项的 wire 形态（done/completed 用）。
func (w *StreamWriter) messageItem(it *wItem) map[string]json.RawMessage {
	return map[string]json.RawMessage{
		"id":      mustMarshal(it.itemID),
		"type":    mustMarshal(itemTypeMessage),
		"status":  mustMarshal(statusCompleted),
		"role":    mustMarshal(roleAssistant),
		"content": mustMarshal([]json.RawMessage{mustMarshal(w.filledPart(it))}),
	}
}

// reasoningItem reasoning 项的 wire 形态（thinking 的 summary + encrypted_content；
// redacted 的加密载荷带前缀）。
func (w *StreamWriter) reasoningItem(it *wItem) map[string]json.RawMessage {
	item := map[string]json.RawMessage{
		"id":     mustMarshal(it.itemID),
		"type":   mustMarshal(itemTypeReasoning),
		"status": mustMarshal(statusCompleted),
	}
	if it.redacted {
		item["summary"] = json.RawMessage("[]")
		item["encrypted_content"] = mustMarshal(redactedThinkingPrefix + it.sig.String())
		return item
	}
	text := it.text.String()
	if text != "" {
		item["summary"] = mustMarshal([]json.RawMessage{mustMarshal(map[string]json.RawMessage{
			"type": mustMarshal(partTypeSummary),
			"text": mustMarshal(text),
		})})
	} else {
		item["summary"] = json.RawMessage("[]")
	}
	if it.sig.Len() > 0 {
		item["encrypted_content"] = mustMarshal(it.sig.String())
	}
	return item
}

// functionCallItem function_call 项的 wire 形态。
func (w *StreamWriter) functionCallItem(it *wItem) map[string]json.RawMessage {
	return map[string]json.RawMessage{
		"id":        mustMarshal(it.itemID),
		"type":      mustMarshal(itemTypeFunctionCall),
		"status":    mustMarshal(statusCompleted),
		"call_id":   mustMarshal(it.callID),
		"name":      mustMarshal(it.name),
		"arguments": mustMarshal(it.args.String()),
	}
}

// emptyPart / filledPart content_part 的 wire 形态（0 值有意义——空串占位）。
func (w *StreamWriter) emptyPart(kind ir.BlockKind) map[string]json.RawMessage {
	if kind == ir.BlockRefusal {
		return map[string]json.RawMessage{
			"type":    mustMarshal(partTypeRefusal),
			"refusal": mustMarshal(""),
		}
	}
	return map[string]json.RawMessage{
		"type":        mustMarshal(partTypeOutputText),
		"text":        mustMarshal(""),
		"annotations": json.RawMessage("[]"),
		"logprobs":    json.RawMessage("[]"),
	}
}

func (w *StreamWriter) filledPart(it *wItem) map[string]json.RawMessage {
	if it.kind == ir.BlockRefusal {
		return map[string]json.RawMessage{
			"type":    mustMarshal(partTypeRefusal),
			"refusal": mustMarshal(it.text.String()),
		}
	}
	return map[string]json.RawMessage{
		"type":        mustMarshal(partTypeOutputText),
		"text":        mustMarshal(it.text.String()),
		"annotations": json.RawMessage("[]"),
		"logprobs":    json.RawMessage("[]"),
	}
}

// frame 写一个 SSE 帧：`event: <type>` + `data: {…,"sequence_number":n}` + 空行。
func (w *StreamWriter) frame(buf *bytes.Buffer, evType string, payload map[string]json.RawMessage) {
	payload["type"] = mustMarshal(evType)
	payload["sequence_number"] = mustMarshal(w.seq)
	w.seq++
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

// itemIDPrefix 项 id 前缀（msg_/rs_/fc_，官方 id 形态的可读近似）。
func itemIDPrefix(kind ir.BlockKind) string {
	switch kind {
	case ir.BlockThinking, ir.BlockRedactedThinking:
		return "rs"
	case ir.BlockToolUse, ir.BlockServerToolUse:
		return "fc"
	default:
		return "msg"
	}
}
