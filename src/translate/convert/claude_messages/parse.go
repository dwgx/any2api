package claude_messages

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/dwgx/any2api/translate/ir"
)

// ---------------------------------------------------------------------------
// 请求：线格式 → IR
// ---------------------------------------------------------------------------

// ParseRequest 解析 Anthropic Messages 请求体。
//
// 字段动作（anthropic-conversion-table §4 中→左列）：
//   - system（string | 块数组）提升为 RoleSystem 消息，逐元素不合并（CC A9）；
//   - 未建模顶层字段保真进 Extra，但键名归一为 OpenAI 形状（IR 规范形约定，
//     见包注）——stop_sequences→stop、max_tokens/temperature/top_p 同名；
//   - tool_choice 四型（auto/any/tool/none）转 OpenAI 规范形；
//     disable_parallel_tool_use → Extra["parallel_tool_calls"]=false；
//   - metadata 显式丢弃（两侧形状不兼容：claude {user_id} vs openai {k:v}，
//     映射表「直迁但需符合上游格式」——本 slice 不做派生，报告列明取舍）。
func ParseRequest(data []byte) (*ir.Request, error) {
	var w requestWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	if len(w.Messages) == 0 {
		return nil, fmt.Errorf("%w: no messages", ErrBadRequest)
	}

	req := &ir.Request{Model: w.Model, Stream: w.Stream}

	if len(w.System) > 0 && !isJSONNull(w.System) {
		sysMsgs, err := parseSystem(w.System)
		if err != nil {
			return nil, err
		}
		req.Messages = append(req.Messages, sysMsgs...)
	}
	for i, raw := range w.Messages {
		msgs, err := parseMessages(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: message %d: %w", ErrBadRequest, i, err)
		}
		req.Messages = append(req.Messages, msgs...)
	}
	for i, raw := range w.Tools {
		td, err := parseToolDef(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: tool %d: %w", ErrBadRequest, i, err)
		}
		req.Tools = append(req.Tools, td)
	}

	extras := map[string]json.RawMessage{}
	if len(w.System) > 0 && !isJSONNull(w.System) {
		// system 原文保真（块级 cache_control 等元素级字段；渲染时优先逐字回写——
		// CC A9：system 数组元素级透传，重排/合并会打掉 prompt cache）。
		extras["system"] = w.System
	}
	if len(w.StopSequences) > 0 && !isJSONNull(w.StopSequences) {
		extras["stop"] = w.StopSequences
	}
	if len(w.MaxTokens) > 0 && !isJSONNull(w.MaxTokens) {
		extras["max_tokens"] = w.MaxTokens
	}
	if len(w.Temperature) > 0 && !isJSONNull(w.Temperature) {
		extras["temperature"] = w.Temperature
	}
	if len(w.TopP) > 0 && !isJSONNull(w.TopP) {
		extras["top_p"] = w.TopP
	}

	if len(w.ToolChoice) > 0 && !isJSONNull(w.ToolChoice) {
		tc, disableParallel, err := claudeToolChoiceToCanonical(w.ToolChoice)
		if err != nil {
			return nil, err
		}
		req.ToolChoice = tc
		if disableParallel {
			extras["parallel_tool_calls"] = json.RawMessage("false")
		}
	}

	// 其余未建模顶层字段保真（betas/thinking/output_config/context_management/…）。
	for k, v := range extrasOf(data,
		"model", "messages", "system", "tools", "tool_choice", "stream",
		"max_tokens", "stop_sequences", "temperature", "top_p", "metadata") {
		extras[k] = v
	}
	if len(extras) > 0 {
		req.Extra = extras
	}
	return req, nil
}

// ---------------------------------------------------------------------------
// 非流式响应：线格式 → IR
// ---------------------------------------------------------------------------

// ParseResponse 解析非流式 Message 响应体。
func ParseResponse(data []byte) (*ir.Response, error) {
	var w responseWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadResponse, err)
	}
	if w.Type != "" && w.Type != objectMessage {
		return nil, fmt.Errorf("%w: unexpected type %q", ErrBadResponse, w.Type)
	}
	if w.Role != "" && w.Role != string(ir.RoleAssistant) {
		return nil, fmt.Errorf("%w: %q", ErrUnexpectedRole, w.Role)
	}
	msg := ir.Message{Role: ir.RoleAssistant}
	for i, raw := range w.Content {
		b, err := parseContentBlock(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: content block %d: %w", ErrBadResponse, i, err)
		}
		msg.Blocks = append(msg.Blocks, b)
	}
	resp := &ir.Response{ID: w.ID, Model: w.Model, Message: msg}
	if w.StopReason != nil && *w.StopReason != "" {
		resp.StopReason = mapStopReason(*w.StopReason)
	}
	// W52-1（W44-1 §9 残留 1 非流式对齐）：stop_sequence 进 IR（命中自定义停止
	// 序列时回填原文；null/缺失 = 未提供）。出口回填见 RenderResponse
	// （stop_reason:"stop_sequence" + 序列值）。
	if len(w.StopSequence) > 0 && !isJSONNull(w.StopSequence) {
		var seq string
		if err := json.Unmarshal(w.StopSequence, &seq); err != nil {
			return nil, fmt.Errorf("%w: stop_sequence: %v", ErrBadResponse, err)
		}
		resp.StopSequence = seq
	}
	u, err := parseUsage(w.Usage)
	if err != nil {
		return nil, fmt.Errorf("%w: usage: %w", ErrBadResponse, err)
	}
	resp.Usage = u
	resp.Extra = extrasOf(data, "id", "type", "role", "model", "content", "stop_reason", "stop_sequence", "usage")
	return resp, nil
}

// ---------------------------------------------------------------------------
// 错误：线格式 → IR（错误默认不透传上游 body；Raw 仅供诊断）
// ---------------------------------------------------------------------------

// ParseError 解析 Anthropic 错误信封 {"type":"error","error":{type,message}}。
// 非 JSON / 非标准 body 不报错，归一为泛型 api_error（Raw 保留原文供诊断）。
func ParseError(body []byte) *ir.Error {
	var env struct {
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil || len(env.Error) == 0 || isJSONNull(env.Error) {
		// W5-22 条目 4：非标准错误体走兜底提取（one-api 7 路径表——msg/err/
		// error_msg/header.message/response.error.message 等形态的最小信息抢救）。
		return ir.SalvageError(body, "anthropic")
	}
	e := &ir.Error{Source: "anthropic", Raw: env.Error}
	var ew errorWire
	if err := json.Unmarshal(env.Error, &ew); err != nil {
		e.Type = "api_error"
		e.Message = "malformed upstream error object"
		return e
	}
	e.Type = ew.Type
	e.Message = ew.Message
	return e
}

// ---------------------------------------------------------------------------
// 流式：命名 SSE 事件 → IR 事件
// ---------------------------------------------------------------------------

// StreamParser Anthropic 流式解析状态机。用法：对每个 SSE 帧调用
// Feed(eventName, dataPayload)；流结束（EOF）后调用 Finish 取未产出的终态事件。
//
// 契约（AM §SSE 事件全表 / §usage / CC B4-B8）：
//   - 骨架：message_start → (content_block_start → content_block_delta* →
//     content_block_stop)* → message_delta → message_stop；ping 忽略；
//     message_stop 是终帧，Anthropic 无 [DONE] 哨兵（AM 陷阱 5）。
//   - message_start.message.usage 与 message_delta.usage 累计合并（覆盖非累加；
//     input/cache 仅在 >0 时覆盖、output 以 delta 为准——CC B7 合并规则）；
//     合并后的 usage 挂在 message_delta 事件上（IR start 事件无 usage 字段）。
//   - 无 stop_reason 的收尾判失败（CC B7 硬门），与 openai_chat 的无
//     finish_reason 判据对称。
//   - 未知事件名/未知 delta 子类型优雅忽略（版本策略；citations_delta 属未建模面）。
type StreamParser struct {
	started   bool
	stopped   bool
	errorSeen bool

	reasonSeen bool
	stopReason ir.StopReason

	usageBase *ir.Usage

	open map[int]ir.BlockKind
}

// NewStreamParser 创建流式解析状态机。
func NewStreamParser() *StreamParser {
	return &StreamParser{open: map[int]ir.BlockKind{}}
}

// Feed 处理一个 SSE 帧（event 名 + data 载荷；event 为空时回退 data.type，
// 容忍丢弃 event: 行的代理——CC B2 是出口要求，入口侧宽容）。
// 返回本次新增的 IR 事件；协议级违规返回包装后的哨兵错误。
func (p *StreamParser) Feed(event string, payload []byte) ([]ir.StreamEvent, error) {
	s := strings.TrimSpace(string(payload))
	if s == "" {
		return nil, nil
	}
	if p.stopped || p.errorSeen {
		return nil, fmt.Errorf("%w: %s", ErrDataAfterTerminal, truncate(s, 120))
	}
	var w streamEventWire
	if err := json.Unmarshal([]byte(s), &w); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadEvent, err)
	}
	name := event
	if name == "" {
		name = w.Type
	}

	switch name {
	case evPing:
		return nil, nil

	case evMessageStart:
		if p.started {
			return nil, fmt.Errorf("%w: duplicate message_start", ErrUnexpectedEventOrder)
		}
		var mw struct {
			ID    string          `json:"id"`
			Model string          `json:"model"`
			Usage json.RawMessage `json:"usage"`
		}
		if len(w.Message) > 0 && !isJSONNull(w.Message) {
			if err := json.Unmarshal(w.Message, &mw); err != nil {
				return nil, fmt.Errorf("%w: message_start.message: %v", ErrBadEvent, err)
			}
		}
		u, err := parseUsage(mw.Usage)
		if err != nil {
			return nil, fmt.Errorf("%w: message_start usage: %v", ErrBadEvent, err)
		}
		p.started = true
		p.usageBase = u
		// Anthropic 无 created 时间戳：Created=0（渲染侧不发明时间）。
		return []ir.StreamEvent{{
			Kind: ir.EventStart, MessageID: mw.ID, Model: mw.Model, Created: 0,
		}}, nil

	case evContentBlockStart:
		if !p.started {
			return nil, fmt.Errorf("%w: content_block_start before message_start", ErrUnexpectedEventOrder)
		}
		if w.Index == nil {
			return nil, fmt.Errorf("%w: content_block_start without index", ErrBadEvent)
		}
		if len(w.ContentBlock) == 0 || isJSONNull(w.ContentBlock) {
			return nil, fmt.Errorf("%w: content_block_start without content_block", ErrBadEvent)
		}
		idx := *w.Index
		if _, dup := p.open[idx]; dup {
			return nil, fmt.Errorf("%w: duplicate content_block_start index %d", ErrBadEvent, idx)
		}
		var bw contentBlockWire
		if err := json.Unmarshal(w.ContentBlock, &bw); err != nil {
			return nil, fmt.Errorf("%w: content_block: %v", ErrBadEvent, err)
		}
		kind := blockKindOf(bw.Type)
		p.open[idx] = kind

		events := []ir.StreamEvent{{
			Kind: ir.EventBlockStart, BlockIndex: idx, BlockKind: kind,
			ToolCallID: bw.ID, ToolName: bw.Name,
		}}
		// start 内联内容被客户端丢弃（CC B6），但上游若发了非空载荷必须保真：
		// 立即转成对应 delta（半 JSON 工具参数例外——见 toolInputInitialDelta）。
		switch kind {
		case ir.BlockText:
			if bw.Text != "" {
				events = append(events, ir.StreamEvent{Kind: ir.EventBlockDelta, BlockIndex: idx, BlockKind: kind, TextDelta: bw.Text})
			}
		case ir.BlockThinking:
			if bw.Thinking != "" {
				events = append(events, ir.StreamEvent{Kind: ir.EventBlockDelta, BlockIndex: idx, BlockKind: kind, TextDelta: bw.Thinking})
			}
			if bw.Signature != "" {
				events = append(events, ir.StreamEvent{Kind: ir.EventBlockDelta, BlockIndex: idx, BlockKind: kind, SignatureDelta: bw.Signature})
			}
		case ir.BlockRedactedThinking:
			if bw.Data != "" {
				events = append(events, ir.StreamEvent{Kind: ir.EventBlockDelta, BlockIndex: idx, BlockKind: kind, SignatureDelta: bw.Data})
			}
		case ir.BlockToolUse, ir.BlockServerToolUse:
			if d := toolInputInitialDelta(bw.Input); d != "" {
				events = append(events, ir.StreamEvent{Kind: ir.EventBlockDelta, BlockIndex: idx, BlockKind: kind, ArgsDelta: d})
			}
		}
		return events, nil

	case evContentBlockDelta:
		if !p.started {
			return nil, fmt.Errorf("%w: content_block_delta before message_start", ErrUnexpectedEventOrder)
		}
		if w.Index == nil {
			return nil, fmt.Errorf("%w: content_block_delta without index", ErrBadEvent)
		}
		idx := *w.Index
		kind, ok := p.open[idx]
		if !ok {
			return nil, fmt.Errorf("%w: %d", ErrUnknownBlockIndex, idx)
		}
		var dw blockDeltaWire
		if err := json.Unmarshal(w.Delta, &dw); err != nil {
			return nil, fmt.Errorf("%w: delta: %v", ErrBadEvent, err)
		}
		switch dw.Type {
		case deltaTypeText:
			if dw.Text == "" {
				return nil, nil // 空增量无信息，忽略
			}
			return []ir.StreamEvent{{Kind: ir.EventBlockDelta, BlockIndex: idx, BlockKind: kind, TextDelta: dw.Text}}, nil
		case deltaTypeThinking:
			if dw.Thinking == "" {
				return nil, nil
			}
			return []ir.StreamEvent{{Kind: ir.EventBlockDelta, BlockIndex: idx, BlockKind: kind, TextDelta: dw.Thinking}}, nil
		case deltaTypeSignature:
			if dw.Signature == "" {
				return nil, nil
			}
			return []ir.StreamEvent{{Kind: ir.EventBlockDelta, BlockIndex: idx, BlockKind: kind, SignatureDelta: dw.Signature}}, nil
		case deltaTypeInputJSON:
			if dw.PartialJSON == "" {
				return nil, nil // 首片可为空串（AM §tool_use 拼接）；空片无信息
			}
			return []ir.StreamEvent{{Kind: ir.EventBlockDelta, BlockIndex: idx, BlockKind: kind, ArgsDelta: dw.PartialJSON}}, nil
		default:
			// citations_delta 等未建模子类型：优雅忽略（版本策略）。
			return nil, nil
		}

	case evContentBlockStop:
		if !p.started {
			return nil, fmt.Errorf("%w: content_block_stop before message_start", ErrUnexpectedEventOrder)
		}
		if w.Index == nil {
			return nil, fmt.Errorf("%w: content_block_stop without index", ErrBadEvent)
		}
		idx := *w.Index
		kind, ok := p.open[idx]
		if !ok {
			return nil, fmt.Errorf("%w: stop for %d", ErrUnknownBlockIndex, idx)
		}
		delete(p.open, idx)
		return []ir.StreamEvent{{Kind: ir.EventBlockStop, BlockIndex: idx, BlockKind: kind}}, nil

	case evMessageDelta:
		if !p.started {
			return nil, fmt.Errorf("%w: message_delta before message_start", ErrUnexpectedEventOrder)
		}
		var mdw messageDeltaWire
		if len(w.Delta) > 0 && !isJSONNull(w.Delta) {
			if err := json.Unmarshal(w.Delta, &mdw); err != nil {
				return nil, fmt.Errorf("%w: message_delta.delta: %v", ErrBadEvent, err)
			}
		}
		merged, err := mergeDeltaUsage(p.usageBase, w.Usage)
		if err != nil {
			return nil, fmt.Errorf("%w: message_delta usage: %v", ErrBadEvent, err)
		}
		p.usageBase = merged

		ev := ir.StreamEvent{Kind: ir.EventMessageDelta}
		if mdw.StopReason != nil && *mdw.StopReason != "" {
			p.reasonSeen = true
			p.stopReason = mapStopReason(*mdw.StopReason)
			ev.StopReason = p.stopReason
		}
		// W44-1（R1 跨包收口）：stop_sequence 进 IR 细节面（命中自定义停止序列时
		// 回填原文；null/缺失 = 未提供）。出口回填见 stream_writer
		// emitMessageDelta（stop_reason:"stop_sequence" + 序列值）。
		if len(mdw.StopSequence) > 0 && !isJSONNull(mdw.StopSequence) {
			var seq string
			if err := json.Unmarshal(mdw.StopSequence, &seq); err != nil {
				return nil, fmt.Errorf("%w: message_delta.delta.stop_sequence: %v", ErrBadEvent, err)
			}
			ev.StopSequence = seq
		}
		if merged != nil {
			ev.Usage = copyUsage(merged)
		}
		if ev.StopReason == "" && ev.Usage == nil && ev.StopSequence == "" {
			return nil, nil
		}
		return []ir.StreamEvent{ev}, nil

	case evMessageStop:
		if !p.started {
			return nil, fmt.Errorf("%w: message_stop before message_start", ErrUnexpectedEventOrder)
		}
		if !p.reasonSeen {
			// CC B7 硬门：缺 stop_reason 判流失败（与 openai 侧缺 finish_reason 对称）。
			p.errorSeen = true
			return []ir.StreamEvent{terminalTransportError("stream ended (message_stop) without stop_reason")}, nil
		}
		events := p.closeBlocks()
		events = append(events,
			ir.StreamEvent{Kind: ir.EventMessageStop},
			ir.StreamEvent{Kind: ir.EventDone, Reason: p.stopReason},
		)
		p.stopped = true
		return events, nil

	case evError:
		p.errorSeen = true
		return []ir.StreamEvent{{
			Kind:        ir.EventError,
			ErrorReason: ir.ErrorUpstream,
			Error:       parseErrorObject(w.Error),
		}}, nil

	default:
		// 未知事件名（含 completion 等 SDK 白名单外的扩展）：优雅忽略。
		return nil, nil
	}
}

// Finish 流结束（EOF）时调用：产出未发出的终态事件。
//
//   - message_stop / error 已见 → 无补充；
//   - 其余 → 截断流判失败（无 message_stop = 流未完成，不补帧掩盖）。
func (p *StreamParser) Finish() []ir.StreamEvent {
	if p.stopped || p.errorSeen {
		return nil
	}
	return []ir.StreamEvent{terminalTransportError("stream closed before message_stop")}
}

func (p *StreamParser) closeBlocks() []ir.StreamEvent {
	if len(p.open) == 0 {
		return nil
	}
	idxs := make([]int, 0, len(p.open))
	for i := range p.open {
		idxs = append(idxs, i)
	}
	sort.Ints(idxs)
	events := make([]ir.StreamEvent, 0, len(idxs))
	for _, i := range idxs {
		events = append(events, ir.StreamEvent{Kind: ir.EventBlockStop, BlockIndex: i, BlockKind: p.open[i]})
	}
	p.open = map[int]ir.BlockKind{}
	return events
}

// ParseStream 便捷入口：从命名事件 SSE 字节流解析全部 IR 事件（内含最小切帧器）。
//
// 切帧规则（与 openai_chat.ParseStream 同构，增加 event: 行）：注释行（心跳）忽略；
// 多行 data 以 "\n" 连接；空行派发；行尾 \r\n 规范化；EOF 时派发未闭合帧。
func ParseStream(r io.Reader) ([]ir.StreamEvent, error) {
	p := NewStreamParser()
	var events []ir.StreamEvent
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 64*1024*1024)
	var eventName string
	var dataLines []string
	flush := func() error {
		if len(dataLines) == 0 {
			eventName = ""
			return nil
		}
		payload := strings.Join(dataLines, "\n")
		name := eventName
		dataLines = dataLines[:0]
		eventName = ""
		evs, err := p.Feed(name, []byte(payload))
		events = append(events, evs...)
		return err
	}
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, ":") {
			continue // 注释行（心跳）不产事件
		}
		if line == "" {
			if err := flush(); err != nil {
				return events, err
			}
			continue
		}
		if v, ok := strings.CutPrefix(line, "event:"); ok {
			eventName = strings.TrimSpace(v)
			continue
		}
		if v, ok := strings.CutPrefix(line, "data:"); ok {
			dataLines = append(dataLines, strings.TrimPrefix(v, " "))
			continue
		}
		// id: / retry: / 其它字段行：Messages 流不使用，忽略。
	}
	if err := sc.Err(); err != nil {
		return events, err
	}
	if err := flush(); err != nil {
		return events, err
	}
	events = append(events, p.Finish()...)
	return events, nil
}

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

// parseSystem 解析顶层 system：string → 单条 RoleSystem 消息；数组 → 逐元素一条
// RoleSystem 消息（元素边界即消息边界，不合并——CC A9；openai 侧多个 system 消息
// ↔ claude 侧 system 数组元素一一对应，双向往返稳定）。
func parseSystem(raw json.RawMessage) ([]ir.Message, error) {
	if len(raw) == 0 || isJSONNull(raw) {
		return nil, nil
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("system: %v", err)
		}
		if s == "" {
			return nil, nil
		}
		return []ir.Message{{Role: ir.RoleSystem, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: s}}}}, nil
	case '[':
		var parts []json.RawMessage
		if err := json.Unmarshal(raw, &parts); err != nil {
			return nil, fmt.Errorf("system: %v", err)
		}
		msgs := make([]ir.Message, 0, len(parts))
		for i, p := range parts {
			b, err := parseContentBlock(p)
			if err != nil {
				return nil, fmt.Errorf("system element %d: %v", i, err)
			}
			msgs = append(msgs, ir.Message{Role: ir.RoleSystem, Blocks: []ir.ContentBlock{b}})
		}
		return msgs, nil
	default:
		return nil, fmt.Errorf("system: unexpected JSON shape")
	}
}

// parseMessages 解析一条 messages[] 项，产出 1..N 条 IR 消息。
//
// 归一规则（IR 约定 role=tool 恰含一个 tool_result 块）：user 消息中的
// tool_result 块按块序拆分为独立 RoleTool 消息；其余块（text/image/…）组成
// RoleUser 消息。顺序保留：连续同类块成组，tool_result 逐个独立成条。
//
// system 消息（CC 2.1.299 实发形态：环境上下文作为 messages[] 内 role:"system"
// 下发，见 research/briefs/reports/A2A-L11-W13-2.md §3b）按 IR 系统面映射为**单条**
// RoleSystem 消息：块不拆不并（一条线格式消息 = 一条 IR 消息，与 user/assistant
// 同口径）；空内容（content 缺失/null）→ 空 RoleSystem 消息（与 user 空内容同款：
// 保消息存在性，空块消息由各出口自行跳过）。位置保留：出现在任意位置（首/中/尾）
// 都按原位进 IR；与顶层 system 并存时**不在解析面合并**——顶层 system 的消息恒在
// 最前（见 ParseRequest），两来源在 IR 中共存，各出口按自己的 system 语义承载
// （claude_web 拍平进 prompt；claude 自出口顶层 system = 原文 + 消息面追加，
// 见 render.go mergeSystemSeed）。
func parseMessages(raw json.RawMessage) ([]ir.Message, error) {
	var mw messageWire
	if err := json.Unmarshal(raw, &mw); err != nil {
		return nil, fmt.Errorf("message: %v", err)
	}
	switch mw.Role {
	case string(ir.RoleUser), string(ir.RoleAssistant), string(ir.RoleSystem):
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnexpectedRole, mw.Role)
	}

	blocks, err := parseContent(mw.Content)
	if err != nil {
		return nil, err
	}
	extra := extrasOf(raw, "role", "content")

	if mw.Role == string(ir.RoleAssistant) {
		for _, b := range blocks {
			if b.Kind == ir.BlockToolResult {
				return nil, fmt.Errorf("%w: tool_result in assistant message", ErrToolResultShape)
			}
		}
		return []ir.Message{{Role: ir.RoleAssistant, Blocks: blocks, Extra: extra}}, nil
	}

	if mw.Role == string(ir.RoleSystem) {
		// 块序原样保留：tool_result 等异类块不做 user 式拆条（system 面语义是
		// 上下文文本；拆出 RoleTool 会发明一套 system 专属策略——不发明）。
		return []ir.Message{{Role: ir.RoleSystem, Blocks: blocks, Extra: extra}}, nil
	}

	// user：tool_result 拆条，其余块成组。
	var out []ir.Message
	var pending []ir.ContentBlock
	flushPending := func() {
		if len(pending) > 0 {
			out = append(out, ir.Message{Role: ir.RoleUser, Blocks: pending})
			pending = nil
		}
	}
	for _, b := range blocks {
		if b.Kind == ir.BlockToolResult {
			flushPending()
			out = append(out, ir.Message{Role: ir.RoleTool, Blocks: []ir.ContentBlock{b}})
			continue
		}
		pending = append(pending, b)
	}
	flushPending()
	if len(out) == 0 {
		out = append(out, ir.Message{Role: ir.RoleUser})
	}
	if extra != nil {
		out[0].Extra = extra
	}
	return out, nil
}

// parseContent 解析 message.content：string | array<block> | null。
func parseContent(raw json.RawMessage) ([]ir.ContentBlock, error) {
	if len(raw) == 0 || isJSONNull(raw) {
		return nil, nil
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("content: %v", err)
		}
		return []ir.ContentBlock{{Kind: ir.BlockText, Text: s}}, nil
	case '[':
		var parts []json.RawMessage
		if err := json.Unmarshal(raw, &parts); err != nil {
			return nil, fmt.Errorf("content: %v", err)
		}
		blocks := make([]ir.ContentBlock, 0, len(parts))
		for i, p := range parts {
			b, err := parseContentBlock(p)
			if err != nil {
				return nil, fmt.Errorf("content block %d: %v", i, err)
			}
			blocks = append(blocks, b)
		}
		return blocks, nil
	default:
		return nil, fmt.Errorf("content: unexpected JSON shape")
	}
}

// parseContentBlock 解析一个内容块。未建模类型进 BlockUnknown + Raw（保真旁路）。
func parseContentBlock(raw json.RawMessage) (ir.ContentBlock, error) {
	var bw contentBlockWire
	if err := json.Unmarshal(raw, &bw); err != nil {
		return ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, nil
	}
	switch bw.Type {
	case blockTypeText:
		return ir.ContentBlock{Kind: ir.BlockText, Text: bw.Text}, nil
	case blockTypeThinking:
		return ir.ContentBlock{Kind: ir.BlockThinking, Text: bw.Thinking, Signature: bw.Signature}, nil
	case blockTypeRedactedThink:
		return ir.ContentBlock{Kind: ir.BlockRedactedThinking, Text: bw.Data}, nil
	case blockTypeToolUse:
		return ir.ContentBlock{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{
			ID: bw.ID, Name: bw.Name, Arguments: toolInputArguments(bw.Input),
		}}, nil
	case blockTypeServerToolUse:
		return ir.ContentBlock{Kind: ir.BlockServerToolUse, ToolCall: &ir.ToolCall{
			ID: bw.ID, Name: bw.Name, Arguments: toolInputArguments(bw.Input),
		}}, nil
	case blockTypeToolResult:
		content, err := toolResultContent(bw.Content)
		if err != nil {
			return ir.ContentBlock{}, err
		}
		return ir.ContentBlock{Kind: ir.BlockToolResult, ToolResult: &ir.ToolResult{
			CallID: bw.ToolUseID, Content: content, IsError: bw.IsError,
		}}, nil
	case blockTypeWebSearchResult:
		// 服务端工具结果：语义字段建模 + 原文保真（渲染回 claude 时用 Raw 逐字回写；
		// 绝不当作客户端 tool_use——AM 陷阱 3）。
		return ir.ContentBlock{Kind: ir.BlockWebSearchToolResult, ToolResult: &ir.ToolResult{CallID: bw.ToolUseID}, Raw: raw}, nil
	case blockTypeImage:
		if len(bw.Source) == 0 || isJSONNull(bw.Source) {
			return ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, nil
		}
		var sw imageSourceWire
		if err := json.Unmarshal(bw.Source, &sw); err != nil {
			return ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, nil
		}
		switch sw.Type {
		case "base64":
			return ir.ContentBlock{Kind: ir.BlockImage, Image: &ir.ImageSource{
				Kind: "inline", MediaType: sw.MediaType, Data: sw.Data,
			}}, nil
		case "url":
			return ir.ContentBlock{Kind: ir.BlockImage, Image: &ir.ImageSource{
				Kind: "file", URL: sw.URL,
			}}, nil
		default:
			return ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, nil
		}
	default:
		return ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, nil
	}
}

// parseToolDef 解析 tools[] 项：{name, description, input_schema, strict}。
func parseToolDef(raw json.RawMessage) (ir.ToolDef, error) {
	var tw toolDefWire
	if err := json.Unmarshal(raw, &tw); err != nil {
		return ir.ToolDef{}, fmt.Errorf("tool: %v", err)
	}
	if tw.Name == "" {
		return ir.ToolDef{}, fmt.Errorf("tool: missing name")
	}
	td := ir.ToolDef{Name: tw.Name, Description: tw.Description, Strict: tw.Strict}
	if len(tw.InputSchema) > 0 && !isJSONNull(tw.InputSchema) {
		td.InputSchema = tw.InputSchema
	}
	td.Extra = extrasOf(raw, "name", "description", "input_schema", "strict")
	return td, nil
}

// toolInputArguments 把 tool_use.input（对象）序列化为 IR Arguments 文本；
// 缺省/null → ""（出口渲染为 "{}"）；原文逐字保留（含空白），不重排。
func toolInputArguments(raw json.RawMessage) string {
	if len(raw) == 0 || isJSONNull(raw) {
		return ""
	}
	return string(raw)
}

// toolInputInitialDelta content_block_start 内联的非空 tool input 转初始 ArgsDelta；
// 空对象 {} / null 是规范占位，不产事件（避免与后续 input_json_delta 双重拼接）。
func toolInputInitialDelta(raw json.RawMessage) string {
	if len(raw) == 0 || isJSONNull(raw) {
		return ""
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err == nil && len(m) == 0 {
		return ""
	}
	return string(raw)
}

// toolResultContent 解析 tool_result.content：string → 原文；数组 → 仅接受 text
// 部件并 "\n" 连接（image 等非 text 结果无 IR 表示——显式报错，绝不静默丢弃）。
func toolResultContent(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || isJSONNull(raw) {
		return "", nil
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", fmt.Errorf("tool_result content: %v", err)
		}
		return s, nil
	case '[':
		var parts []json.RawMessage
		if err := json.Unmarshal(raw, &parts); err != nil {
			return "", fmt.Errorf("tool_result content: %v", err)
		}
		var texts []string
		for i, p := range parts {
			b, err := parseContentBlock(p)
			if err != nil {
				return "", fmt.Errorf("tool_result content part %d: %v", i, err)
			}
			if b.Kind != ir.BlockText {
				return "", fmt.Errorf("%w: tool_result content part kind %q", ErrToolResultShape, b.Kind)
			}
			texts = append(texts, b.Text)
		}
		return strings.Join(texts, "\n"), nil
	default:
		return "", fmt.Errorf("tool_result content: unexpected JSON shape")
	}
}

// parseUsage 解析 usage 对象（顶层字段建模；未建模键原文进 Extra）。
//
// Anthropic 无 total_tokens：Total 按 input + cache_creation + cache_read + output
// 合成（口径显式——anthropic-conversion-table §5「总量」行），Estimated 标记合成值。
func parseUsage(raw json.RawMessage) (*ir.Usage, error) {
	if len(raw) == 0 || isJSONNull(raw) {
		return nil, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("usage: %v", err)
	}
	u := &ir.Usage{Accounting: "anthropic"}
	var err error
	if u.Input, err = rawInt64(m["input_tokens"]); err != nil {
		return nil, fmt.Errorf("input_tokens: %v", err)
	}
	if u.Output, err = rawInt64(m["output_tokens"]); err != nil {
		return nil, fmt.Errorf("output_tokens: %v", err)
	}
	if u.CacheRead, err = rawInt64(m["cache_read_input_tokens"]); err != nil {
		return nil, fmt.Errorf("cache_read_input_tokens: %v", err)
	}
	cacheTotal, err := rawInt64(m["cache_creation_input_tokens"])
	if err != nil {
		return nil, fmt.Errorf("cache_creation_input_tokens: %v", err)
	}
	if cc, ok := m["cache_creation"]; ok && !isJSONNull(cc) {
		var cw struct {
			E5 *int64 `json:"ephemeral_5m_input_tokens"`
			E1 *int64 `json:"ephemeral_1h_input_tokens"`
		}
		if err := json.Unmarshal(cc, &cw); err == nil {
			cr := &ir.CacheCreation{}
			if cw.E5 != nil {
				cr.FiveMinute = *cw.E5
			}
			if cw.E1 != nil {
				cr.OneHour = *cw.E1
			}
			u.CacheCreation = cr
		}
	} else if cacheTotal > 0 {
		u.CacheCreation = &ir.CacheCreation{FiveMinute: cacheTotal}
	}
	if det, ok := m["output_tokens_details"]; ok && !isJSONNull(det) {
		var dw struct {
			Thinking *int64 `json:"thinking_tokens"`
		}
		if err := json.Unmarshal(det, &dw); err == nil && dw.Thinking != nil {
			u.Reasoning = *dw.Thinking
		}
	}
	u.Total = u.Input + cacheTotal + u.CacheRead + u.Output
	u.Estimated = true
	for k, v := range m {
		switch k {
		case "input_tokens", "output_tokens", "cache_read_input_tokens",
			"cache_creation_input_tokens", "cache_creation", "output_tokens_details":
			continue
		}
		if u.Extra == nil {
			u.Extra = map[string]json.RawMessage{}
		}
		u.Extra[k] = v
	}
	return u, nil
}

// mergeDeltaUsage 把 message_delta.usage 合并进累计状态（覆盖语义，非累加）。
// 合并规则（CC B7 客户端同款）：input/cache 仅在 >0 时覆盖；output 以 delta 为准
// （出现即覆盖，含显式 0）；message_start 出现而 delta 省略的字段保留原值。
func mergeDeltaUsage(base *ir.Usage, raw json.RawMessage) (*ir.Usage, error) {
	if len(raw) == 0 || isJSONNull(raw) {
		return base, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("usage: %v", err)
	}
	if base == nil {
		base = &ir.Usage{Accounting: "anthropic"}
	}
	var err error
	var v int64
	if v, err = rawInt64(m["input_tokens"]); err != nil {
		return nil, fmt.Errorf("input_tokens: %v", err)
	}
	if v > 0 {
		base.Input = v
	}
	if _, present := m["output_tokens"]; present {
		if base.Output, err = rawInt64(m["output_tokens"]); err != nil {
			return nil, fmt.Errorf("output_tokens: %v", err)
		}
	}
	if v, err = rawInt64(m["cache_read_input_tokens"]); err != nil {
		return nil, fmt.Errorf("cache_read_input_tokens: %v", err)
	}
	if v > 0 {
		base.CacheRead = v
	}
	cacheTotal, err := rawInt64(m["cache_creation_input_tokens"])
	if err != nil {
		return nil, fmt.Errorf("cache_creation_input_tokens: %v", err)
	}
	if cc, ok := m["cache_creation"]; ok && !isJSONNull(cc) {
		var cw struct {
			E5 *int64 `json:"ephemeral_5m_input_tokens"`
			E1 *int64 `json:"ephemeral_1h_input_tokens"`
		}
		if err := json.Unmarshal(cc, &cw); err == nil {
			cr := &ir.CacheCreation{}
			if cw.E5 != nil {
				cr.FiveMinute = *cw.E5
			}
			if cw.E1 != nil {
				cr.OneHour = *cw.E1
			}
			if cr.FiveMinute+cr.OneHour > 0 {
				base.CacheCreation = cr
			}
		}
	} else if cacheTotal > 0 {
		base.CacheCreation = &ir.CacheCreation{FiveMinute: cacheTotal}
	}
	if det, ok := m["output_tokens_details"]; ok && !isJSONNull(det) {
		var dw struct {
			Thinking *int64 `json:"thinking_tokens"`
		}
		if err := json.Unmarshal(det, &dw); err == nil && dw.Thinking != nil && *dw.Thinking > 0 {
			base.Reasoning = *dw.Thinking
		}
	}
	if base.CacheCreation != nil {
		cacheTotal = base.CacheCreation.FiveMinute + base.CacheCreation.OneHour
	}
	base.Total = base.Input + cacheTotal + base.CacheRead + base.Output
	base.Estimated = true
	for k, v := range m {
		switch k {
		case "input_tokens", "output_tokens", "cache_read_input_tokens",
			"cache_creation_input_tokens", "cache_creation", "output_tokens_details":
			continue
		}
		if base.Extra == nil {
			base.Extra = map[string]json.RawMessage{}
		}
		base.Extra[k] = v
	}
	return base, nil
}

// copyUsage 深拷贝（CacheCreation + Extra 复制；事件不可变——合并状态不得被
// 已发出事件反向引用）。
func copyUsage(u *ir.Usage) *ir.Usage {
	if u == nil {
		return nil
	}
	c := *u
	if u.CacheCreation != nil {
		cc := *u.CacheCreation
		c.CacheCreation = &cc
	}
	if u.Extra != nil {
		c.Extra = make(map[string]json.RawMessage, len(u.Extra))
		for k, v := range u.Extra {
			c.Extra[k] = v
		}
	}
	return &c
}

// parseErrorObject 解析 Anthropic 错误对象内层 {type, message}。
func parseErrorObject(raw json.RawMessage) *ir.Error {
	e := &ir.Error{Source: "anthropic", Raw: raw}
	if len(raw) == 0 || isJSONNull(raw) {
		e.Type = "api_error"
		e.Message = "upstream returned an error without a body"
		return e
	}
	var ew errorWire
	if err := json.Unmarshal(raw, &ew); err != nil {
		e.Type = "api_error"
		e.Message = "malformed upstream error object"
		return e
	}
	e.Type = ew.Type
	e.Message = ew.Message
	return e
}

// mapStopReason 把 Anthropic stop_reason 七值映射到 IR 四值
// （anthropic-conversion-table §5「结束原因」行；修正 CPA 的 pause_turn→stop 丢语义）：
// end_turn→stop、max_tokens→length、stop_sequence→stop、tool_use→toolUse、
// pause_turn→length、refusal→error、model_context_window_exceeded→length。
// 未知值原样透传（翻译策略在转换层，解析层不猜测）。
func mapStopReason(s string) ir.StopReason {
	switch s {
	case "end_turn":
		return ir.StopStop
	case "max_tokens":
		return ir.StopLength
	case "stop_sequence":
		return ir.StopStop
	case "tool_use":
		return ir.StopToolUse
	case "pause_turn":
		return ir.StopLength
	case "refusal":
		return ir.StopError
	case "model_context_window_exceeded":
		return ir.StopLength
	default:
		return ir.StopReason(s)
	}
}

// claudeToolChoiceToCanonical claude tool_choice → IR 规范形（OpenAI 形状）。
// auto→"auto"、none→"none"、any→"required"、{type:tool,name}→{type:function,…}；
// 未知形状原样透传（解析层不猜测）。返回值 bool = disable_parallel_tool_use。
func claudeToolChoiceToCanonical(raw json.RawMessage) (json.RawMessage, bool, error) {
	var w struct {
		Type            string `json:"type"`
		Name            string `json:"name"`
		DisableParallel *bool  `json:"disable_parallel_tool_use"`
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, false, fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	disable := w.DisableParallel != nil && *w.DisableParallel && w.Type != "none"
	switch w.Type {
	case "auto":
		return json.RawMessage(`"auto"`), disable, nil
	case "none":
		return json.RawMessage(`"none"`), disable, nil
	case "any":
		return json.RawMessage(`"required"`), disable, nil
	case "tool":
		if w.Name == "" {
			return nil, false, fmt.Errorf("%w: tool_choice tool without name", ErrBadRequest)
		}
		return mustMarshal(map[string]json.RawMessage{
			"type":     mustMarshal("function"),
			"function": mustMarshal(map[string]json.RawMessage{"name": mustMarshal(w.Name)}),
		}), disable, nil
	default:
		return raw, disable, nil
	}
}

func blockKindOf(t string) ir.BlockKind {
	switch t {
	case blockTypeText:
		return ir.BlockText
	case blockTypeThinking:
		return ir.BlockThinking
	case blockTypeRedactedThink:
		return ir.BlockRedactedThinking
	case blockTypeToolUse:
		return ir.BlockToolUse
	case blockTypeServerToolUse:
		return ir.BlockServerToolUse
	case blockTypeWebSearchResult:
		return ir.BlockWebSearchToolResult
	case blockTypeImage:
		return ir.BlockImage
	default:
		return ir.BlockUnknown
	}
}

func terminalTransportError(msg string) ir.StreamEvent {
	return ir.StreamEvent{
		Kind:        ir.EventError,
		ErrorReason: ir.ErrorTransport,
		Error:       &ir.Error{Type: "incomplete_stream", Message: msg, Source: "anthropic"},
	}
}

// extrasOf 返回 raw 对象中除 known 键外的全部字段（保真旁路）。
func extrasOf(raw json.RawMessage, known ...string) map[string]json.RawMessage {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil || len(m) == 0 {
		return nil
	}
	for _, k := range known {
		delete(m, k)
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

func rawInt64(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || isJSONNull(raw) {
		return 0, nil
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil {
		return 0, fmt.Errorf("%v", err)
	}
	return n, nil
}

func isJSONNull(raw json.RawMessage) bool {
	return strings.TrimSpace(string(raw)) == "null"
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
