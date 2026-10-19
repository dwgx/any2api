package chatgpt_web

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/dwgx/any2api/translate/ir"
)

// ---------------------------------------------------------------------------
// 解析状态机 + IR 映射（件 4）
// ---------------------------------------------------------------------------

// StreamParser chatgpt.com completion SSE 解析状态机。用法：对每个 SSE 帧调用
// Feed(event, dataPayload)；上游 EOF（或任一结束判据）后调用 Finish。
//
// 契约（KB §流式细节 §4 + 三实现）：
//
//   - 骨架：帧流无显式 message_start——IR start 事件在首个内容/终态事件前
//     惰性发出（MessageID/Model 取已采纳 live 消息的回读值）；
//   - 帧形状混用（①a/①b/①c/①d/②/③/④/⑤ 见 wire.go）：每帧独立判定，未知
//     形状忽略（版本策略）；
//   - 结束判据（四 + 一，见 patch.go/dispatch.go）：先到先算，幂等收束
//     （F:341-357「三选一都可能先到」）；其后重复结束标记/元数据帧容忍，
//     **内容帧显式报错**（ErrDataAfterTerminal）；
//   - 思考（thoughts/reasoning 路径）→ IR thinking 块（与 B 默认丢弃相反——
//     本产品出口要 reasoning_content；A 是唯一接思考的实现）；
//   - usage 面无：上游从不给 token 数——message_delta 绝不带 Usage（不合成）。
type StreamParser struct {
	st patchState

	// 生命周期。
	started      bool
	terminalSeen bool
	errorSeen    bool
	// definitive marker 登记（W32-1 尾读口径修正）：`message_stream_complete`
	// 帧 / `[DONE]` 哨兵——到达即「流已完结」的**收束判据**（服务壳据此立即
	// 收束，不等尾读宽限）。与 terminalSeen 的区别：terminalSeen 还含 status
	// finished/end_turn 等**非 definitive** 终态（其后仍有尾读段）。
	streamCompleteSeen bool
	doneSeen           bool

	// live 消息面（历史回放过滤；CF stream.ts:103-128 语义）。
	liveID            string
	sawLive           bool
	contextSuppressed bool // 最近一次消息观察是否被抑制（状态判据的门控）

	// 块状态机（gemini feedTextDelta 模式：同 kind 复用、切 kind 关闭重开）。
	nextBlock int
	openIdx   int
	openKind  ir.BlockKind

	// 会话级记录（IR 无载体——accessor 暴露，服务壳消费）。
	version        string
	conversationID string
	title          string
	resumeToken    string
	handoff        *HandoffInfo
	moderation     *ModerationInfo
	model          string
	finishType     string
	lastMetadata   json.RawMessage
	toolMessages   []ToolMessage
	toolOverflow   int
	markers        []MarkerInfo // message_marker 帧登记（W32-1 尾部帧面；上限 maxMarkerRecords）
	markerOverflow int          // 超上限被丢的 marker 帧数（防内存无界）

	// 观测面（W29-1 §9.1 顺带项：只加观测，不参与判据）。
	counterSeen    bool            // 是否见过 patch 帧的 `c` 计数器（D3）
	lastCounter    int64           // 最近一次观察到的 `c`
	counterFrames  int             // 带 `c` 的帧数（PatchCounterJumps 的分母）
	counterJumps   int             // `c` != 上一值+1 的次数（丢帧/乱序候选）
	detailMetadata json.RawMessage // conversation_detail_metadata 帧原文（D4）

	// 计数（诊断/测试面）。
	framesSeen     int
	historyDropped int
}

// NewStreamParser 创建流式解析状态机。
func NewStreamParser() *StreamParser {
	return &StreamParser{openIdx: -1}
}

// Feed 处理一个 SSE 帧（event 名可为空——data-only 帧）。参数签名与 pairs 的
// Feed 契约对齐；本包不依赖事件名分派（A:838-921 同款——分派以载荷形状为准，
// chatgpt 流的 event: 行只有 delta_encoding 一帧，v1 标记由载荷 "v1" 识别）。
// 载荷先过 DecodePayload（双重编码容错 + 心跳忽略），再分流映射。
func (p *StreamParser) Feed(event string, payload []byte) ([]ir.StreamEvent, error) {
	s := strings.TrimSpace(string(payload))
	if s == "" {
		return nil, nil
	}
	if s == sentinelDone {
		p.doneSeen = true                          // definitive marker 登记（W32-1）
		return p.terminal(p.terminalReason()), nil // 判据④
	}
	raw, ok := DecodePayload(s)
	if !ok {
		return nil, nil // 心跳/非 JSON——忽略（B:66-69）
	}
	already := p.terminalSeen || p.errorSeen
	events, err := p.feedPayload(raw)
	if err != nil {
		return nil, err
	}
	if already && len(events) > 0 {
		// 终态后的内容帧：显式报错（早终结风险的暴露）。重复结束标记/心跳/
		// 元数据帧不产事件，被容忍（幂等收束）。
		return nil, fmt.Errorf("%w: %s", ErrDataAfterTerminal, truncate(s, 120))
	}
	return events, nil
}

// Finish 流结束（EOF 或服务壳收尾）时调用：产出未发出的终态事件。
//
//   - 任一结束判据/错误帧已见 → 无补充（幂等）；
//   - 一帧都没解析到（空流/纯心跳）→ 无补充（调用方按「上游零产出」处理）；
//   - 有产出但无结束判据 → 截断流判失败（不补帧掩盖；B chat.go:697-706
//     「上游未发送结束标记」同款；openai_chat「无 finish_reason 的收尾判失败」同款）。
//
// 假成功面（C head_process_response:111-124：一路没有 in_progress 就当 403
// unusual activity）：本包不合成错误——SawLiveMessage()==false 供服务壳判定。
func (p *StreamParser) Finish() []ir.StreamEvent {
	if p.terminalSeen || p.errorSeen {
		return nil
	}
	if p.handoff != nil {
		// 流已交给 handoff 通道（A:857-868）：首段流不产终止属正常——服务壳
		// 按 Handoff() 续接（S3），不按截断处理。
		return nil
	}
	if p.framesSeen == 0 && !p.sawLive {
		return nil
	}
	p.errorSeen = true
	var events []ir.StreamEvent
	events = append(events, p.ensureStart()...)
	events = append(events, p.closeBlock()...)
	events = append(events, ir.StreamEvent{
		Kind:        ir.EventError,
		ErrorReason: ir.ErrorTransport,
		Error: &ir.Error{
			Type:    "incomplete_stream",
			Message: "stream closed before terminal signal",
			Source:  "chatgpt_web",
		},
	})
	return events
}

// ParseStream 便捷入口：从 completion SSE 字节流解析全部 IR 事件（内含切帧器）。
func ParseStream(r io.Reader) ([]ir.StreamEvent, error) {
	frames, err := SplitFrames(r)
	if err != nil {
		return nil, err
	}
	p := NewStreamParser()
	var events []ir.StreamEvent
	for _, f := range frames {
		evs, err := p.Feed(f.Event, []byte(f.Data))
		events = append(events, evs...)
		if err != nil {
			return events, err
		}
	}
	events = append(events, p.Finish()...)
	return events, nil
}

// ---------------------------------------------------------------------------
// IR 事件装配
// ---------------------------------------------------------------------------

// emitText 文本/思考/代码增量 → 块事件（惰性开块；kind 切换时关闭前一块——
// 下游出口有块序约束，交错块必须按序闭合）。
func (p *StreamParser) emitText(kind ir.BlockKind, text string) []ir.StreamEvent {
	if text == "" {
		return nil
	}
	var events []ir.StreamEvent
	events = append(events, p.ensureStart()...)
	if p.openIdx >= 0 && p.openKind != kind {
		events = append(events, p.closeBlock()...)
	}
	if p.openIdx < 0 {
		idx := p.nextBlock
		p.nextBlock++
		p.openIdx, p.openKind = idx, kind
		events = append(events, ir.StreamEvent{Kind: ir.EventBlockStart, BlockIndex: idx, BlockKind: kind})
	}
	events = append(events, ir.StreamEvent{
		Kind:       ir.EventBlockDelta,
		BlockIndex: p.openIdx,
		BlockKind:  kind,
		TextDelta:  text,
	})
	return events
}

// closeBlock 关闭当前打开的块（kind 切换 / 终态时）。
func (p *StreamParser) closeBlock() []ir.StreamEvent {
	if p.openIdx < 0 {
		return nil
	}
	ev := ir.StreamEvent{Kind: ir.EventBlockStop, BlockIndex: p.openIdx, BlockKind: p.openKind}
	p.openIdx, p.openKind = -1, ""
	return []ir.StreamEvent{ev}
}

// ensureStart 惰性发出 IR start（首个内容/终态事件之前）。
func (p *StreamParser) ensureStart() []ir.StreamEvent {
	if p.started {
		return nil
	}
	p.started = true
	return []ir.StreamEvent{{Kind: ir.EventStart, MessageID: p.liveID, Model: p.model}}
}

// terminal 发出终态序列（幂等：先到先算；其后调用返回 nil）。
// 顺序：closeBlock → message_delta{stop_reason} → message_stop → done。
func (p *StreamParser) terminal(reason ir.StopReason) []ir.StreamEvent {
	if p.terminalSeen || p.errorSeen {
		return nil
	}
	p.terminalSeen = true
	var events []ir.StreamEvent
	events = append(events, p.ensureStart()...)
	events = append(events, p.closeBlock()...)
	events = append(events, ir.StreamEvent{Kind: ir.EventMessageDelta, StopReason: reason})
	events = append(events, ir.StreamEvent{Kind: ir.EventMessageStop})
	events = append(events, ir.StreamEvent{Kind: ir.EventDone, Reason: reason})
	return events
}

// applyPatchOutcome 把 patch 产物映射为 IR 事件（保序：思考 → 正文 → 元数据 → 结束）。
func (p *StreamParser) applyPatchOutcome(out patchOutcome) []ir.StreamEvent {
	var events []ir.StreamEvent
	if out.Thinking != "" {
		events = append(events, p.emitText(ir.BlockThinking, out.Thinking)...)
	}
	if out.Text != "" {
		p.st.noteText(out.Text)
		events = append(events, p.emitText(ir.BlockText, out.Text)...)
	}
	if len(out.Metadata) > 0 {
		p.captureMetadata(out.Metadata)
	}
	// 判据②：状态补丁（B:95-100）——live 上下文门控（见 wire.go「关键取舍」注：
	// B 在工具消息上下文下会误终结，本包以 live 门 + end_turn 兜底）。
	if out.Final && !p.contextSuppressed {
		events = append(events, p.terminal(p.terminalReason())...)
	}
	// 补充信号：end_turn 补丁（CF:122-128）——权威，不受门控。
	if out.EndTurn {
		events = append(events, p.terminal(p.terminalReason())...)
	}
	return events
}

// ---------------------------------------------------------------------------
// message 对象路径（①d 对象首帧 / ② 旧协议快照共用）
// ---------------------------------------------------------------------------

// isReasoningRecap 思考回顾消息判据（W29-1 D1 修复；`fddc772` 证据帧
// `sample-isp-v1/sse.sanitized.txt` 第 8 帧）：
//
//	content_type == "reasoning_recap" || metadata.reasoning_status != ""
//	|| metadata.reasoning_recap_type != ""
//
// 语义：recap 是「思考结束」的 UI 回顾条（`content` 是**字符串**、`parts` 缺省、
// 文本如 "Worked for a second"），**带 `status:"finished_successfully"` 但不结束
// 回合**（实抓 `end_turn:false`，其后才是 live 正文 patch）。三判据取或：
// content_type 是主判据，metadata 两个标记作兜底（上游把该条渲染成 `text` 时
// 仍能识别）。
func isReasoningRecap(m *messageWire) bool {
	if m == nil {
		return false
	}
	return m.Content.ContentType == ctReasoningRecap ||
		m.Metadata.ReasoningStatus != "" ||
		m.Metadata.ReasoningRecapType != ""
}

// handleMessage 处理 message 对象。顺序（对照三实现）：
//
//  1. 历史回放过滤（CF:103-118）——命中即丢（不采纳、不终结、不产出）；
//  2. 元数据捕获（模型回读——A:777-781）；
//  3. role 过滤（CF:79-81 只处理 assistant；非 assistant → 记录 + 抑制）；
//  4. 采纳（新 id → 重置消息级状态；CF:119-124）／同消息观察（recipient 更新）；
//  5. 内容抑制（B:98-104 recipient != all；A:826-827 channel=commentary）→ 记录；
//  6. 内容映射（快照差分 / content_type 分支，见 messageContent）；
//  7. 结束判据（B:155-163 的 status；CF:122-128 的 end_turn）——status 判据在
//     抑制上下文下不终结（防 B 的工具消息误终结），且**排除 reasoning_recap**
//     （W29-1 D1：recap 带 finished_successfully 但回合未结束——ISP golden 实证
//     `end_turn:false` + 其后正文 patch；不排除则其后正文被 ErrDataAfterTerminal
//     整段丢弃）；end_turn 不受门控（recap 实抓 `end_turn:false`，门控无收益）。
func (p *StreamParser) handleMessage(m *messageWire) ([]ir.StreamEvent, error) {
	if isHistoryEcho(m, p.liveID) {
		p.historyDropped++
		return nil, nil
	}
	p.captureMetadataForMessage(m)
	if m.Author.Role != roleAssistant {
		p.recordMessage(m, "role")
		p.contextSuppressed = true
		return nil, nil
	}
	if m.ID != "" && m.ID != p.liveID {
		p.adoptLive(m) // 新 id：消息切换，重置消息级状态（CF:119-124）
	} else if !p.sawLive {
		p.adoptLive(m) // 首条消息（id 可空——构造帧/退化流）
	} else if m.Recipient != "" {
		p.st.recipient = m.Recipient
	}
	p.sawLive = true
	suppressed, reason := false, ""
	switch {
	case !p.st.recipientAll():
		suppressed, reason = true, "recipient"
	case m.Channel == channelCommentary:
		suppressed, reason = true, "commentary"
	}
	p.contextSuppressed = suppressed
	var events []ir.StreamEvent
	if suppressed {
		p.recordMessage(m, reason)
	} else {
		evs, err := p.messageContent(m)
		if err != nil {
			return nil, err
		}
		events = append(events, evs...)
	}
	// 结束判据：end_turn 权威（CF:122-128）；status 终态须排除 reasoning_recap
	//（W29-1 D1——recap 带 finished_successfully 但不是回合结束；见函数头注 7）。
	if m.EndTurn || ((m.Status == statusFinishedSuccessfully || m.Status == statusFinished) &&
		!p.contextSuppressed && !isReasoningRecap(m)) {
		events = append(events, p.terminal(p.stopReasonFor(m))...)
	}
	return events, nil
}

// adoptLive 采纳一条 live assistant 消息（CF:119-124：重置快照差分状态）。
func (p *StreamParser) adoptLive(m *messageWire) {
	p.liveID = m.ID
	p.sawLive = true
	p.st.resetForMessage(m)
}

// captureMetadataForMessage message.metadata 的消费面（A:777-791）。
func (p *StreamParser) captureMetadataForMessage(m *messageWire) {
	if m.Metadata.ModelSlug != "" {
		p.model = m.Metadata.ModelSlug
	}
	if m.Metadata.FinishDetails.Type != "" {
		p.finishType = m.Metadata.FinishDetails.Type
	}
}

// stopReasonFor 结束判据 → StopReason（Go request.go:526-532 / A:786-791：
// finish_details.type == max_tokens → length；其余 → stop）。
func (p *StreamParser) stopReasonFor(m *messageWire) ir.StopReason {
	ft := p.finishType
	if m != nil && m.Metadata.FinishDetails.Type != "" {
		ft = m.Metadata.FinishDetails.Type
	}
	if ft == finishMaxTokens {
		return ir.StopLength
	}
	return ir.StopStop
}

// terminalReason 无消息上下文终态的 StopReason（[DONE] / message_stream_complete /
// 状态补丁）：以已观察的 finish_details 为准（max_tokens → length）。
func (p *StreamParser) terminalReason() ir.StopReason { return p.stopReasonFor(nil) }

// messageContent 内容映射（content_type 分支；C:129-340 的最小映射）。
func (p *StreamParser) messageContent(m *messageWire) ([]ir.StreamEvent, error) {
	ci, err := extractContent(m)
	if err != nil {
		return nil, err
	}
	var events []ir.StreamEvent
	if ci.Text != "" {
		// 快照/首帧均按累积文本差分（B:168-193 + A:828-834 守卫；对象首帧的
		// 初始内容通常为空——B:149-163 的 lastFull 基线语义）。
		delta := p.st.snapshotDelta(ci.Text)
		if delta != "" {
			events = append(events, p.emitText(ci.Kind, delta)...)
		}
	}
	if len(ci.Images) > 0 {
		p.recordImages(m, ci.Images)
	}
	return events, nil
}

// contentInfo live 消息内容分支的抽取结果。
type contentInfo struct {
	Kind   ir.BlockKind
	Text   string
	Images []ImagePointer
}

// extractContent 按 content_type 分支抽取 live 消息内容（C:129-340 最小映射）：
//
//   - text（缺省）→ BlockText（字符串 parts 连接 + content.text 兜底）；
//   - code → BlockExecutableCode（content.text；C:256-262 的 fenced code 分支）；
//   - execution_output → BlockCodeExecutionResult（content.text；C:263-266）；
//   - multimodal_text → 字符串 parts 走文本、asset_pointer 走图片记录（C:268-300）；
//   - model_editable_context → 忽略（模型上下文，非用户可见；KB 字段表）；
//   - 未知 content_type → 可提取文本则当文本（宽容；C:265 的 text 兜底）；
//     否则若含 parts 内容 → ErrUnsupportedContent（无载体显式报错）。
func extractContent(m *messageWire) (contentInfo, error) {
	var ci contentInfo
	switch m.Content.ContentType {
	case ctCode:
		ci.Kind = ir.BlockExecutableCode
		ci.Text = firstNonEmpty(m.Content.Text, m.textParts())
	case ctExecutionOutput:
		ci.Kind = ir.BlockCodeExecutionResult
		ci.Text = firstNonEmpty(m.Content.Text, m.textParts())
	case ctModelEditableContext:
		return ci, nil
	default: // text / multimodal_text / 未知
		ci.Kind = ir.BlockText
		for _, raw := range m.Content.Parts {
			if s, ok := jsonString(raw); ok {
				ci.Text += s
				continue
			}
			if ptr, ok := imagePointerOf(raw); ok {
				if ptr.Scheme == "" {
					return ci, fmt.Errorf("%w: unknown asset_pointer scheme: %q",
						ErrUnsupportedContent, truncate(ptr.Pointer, 80))
				}
				ci.Images = append(ci.Images, ptr)
				continue
			}
			if t, ok := objectTextOf(raw); ok {
				ci.Text += t
				continue
			}
			return ci, fmt.Errorf("%w: content_type=%q part=%s",
				ErrUnsupportedContent, m.Content.ContentType, truncate(string(raw), 80))
		}
		if ci.Text == "" && m.Content.Text != "" {
			ci.Text = m.Content.Text
		}
	}
	return ci, nil
}

// ---------------------------------------------------------------------------
// 记录面（tool 消息最小映射 / 抑制消息 / 图片指针）
// ---------------------------------------------------------------------------

// recordMessage 记录被抑制的消息（tool 消息面最小映射——S3/S6 消费）。
func (p *StreamParser) recordMessage(m *messageWire, reason string) {
	if len(p.toolMessages) >= maxToolRecords {
		p.toolOverflow++
		return
	}
	text, images := extractRecordParts(m)
	p.toolMessages = append(p.toolMessages, ToolMessage{
		MessageID:   m.ID,
		Role:        m.Author.Role,
		Recipient:   m.Recipient,
		Channel:     m.Channel,
		ContentType: m.Content.ContentType,
		Reason:      reason,
		Text:        text,
		Images:      images,
		InitialText: m.Metadata.InitialText,
		RawParts:    append([]json.RawMessage(nil), m.Content.Parts...),
	})
}

// recordImages 记录 live 消息的图片指针（流事件面无 IR 载体——进记录供 S6 渲染）。
func (p *StreamParser) recordImages(m *messageWire, imgs []ImagePointer) {
	if len(p.toolMessages) >= maxToolRecords {
		p.toolOverflow++
		return
	}
	p.toolMessages = append(p.toolMessages, ToolMessage{
		MessageID:   m.ID,
		Role:        m.Author.Role,
		Recipient:   m.Recipient,
		Channel:     m.Channel,
		ContentType: m.Content.ContentType,
		Reason:      "live-multimodal",
		Images:      imgs,
		InitialText: m.Metadata.InitialText,
		RawParts:    append([]json.RawMessage(nil), m.Content.Parts...),
	})
}

// extractRecordParts 记录面抽取：字符串 parts 连接 + 指针分类（未知形态进
// RawParts 无损保留，不报错——记录本身就是载体）。
func extractRecordParts(m *messageWire) (string, []ImagePointer) {
	var sb strings.Builder
	var imgs []ImagePointer
	for _, raw := range m.Content.Parts {
		if s, ok := jsonString(raw); ok {
			sb.WriteString(s)
			continue
		}
		if ptr, ok := imagePointerOf(raw); ok {
			imgs = append(imgs, ptr)
			continue
		}
		if t, ok := objectTextOf(raw); ok {
			sb.WriteString(t)
		}
	}
	if sb.Len() == 0 && m.Content.Text != "" {
		sb.WriteString(m.Content.Text)
	}
	return sb.String(), imgs
}

// ---------------------------------------------------------------------------
// 错误体信封（非 2xx）
// ---------------------------------------------------------------------------

// ParseError 上游非 2xx 错误体 → IR 错误信封（Source=chatgpt_web）。
//
// 形态面（KB 未证任何非 2xx 体形状；S4/S5 golden 帧校准 [构造/待实抓校准]）：
//
//   - `{"error": {…}|"…"}`（流内错误同款形态；KB 证 `{"error": …}` 存在）→
//     复用 ir.SalvageError 的 one-api 同序兜底表；
//   - `{"detail": "…"|{code,message,…}}`（chatgpt.com 网关常见形态，未证——容忍）；
//   - 其余 JSON → SalvageError 兜底；
//   - 非 JSON（Cloudflare 挑战页 HTML 等）→ SalvageError 泛型兜底（绝不透传
//     原文；Raw 仅供诊断——渲染器纪律）。
func ParseError(body []byte) *ir.Error {
	s := strings.TrimSpace(string(body))
	if s == "" {
		return &ir.Error{Type: "api_error", Message: "empty upstream error body", Source: "chatgpt_web"}
	}
	var probe struct {
		Error  json.RawMessage `json:"error"`
		Detail json.RawMessage `json:"detail"`
	}
	if err := json.Unmarshal([]byte(s), &probe); err == nil {
		if len(probe.Error) > 0 && !isJSONNull(probe.Error) {
			return ir.SalvageError([]byte(s), "chatgpt_web")
		}
		if e := detailError(probe.Detail, s); e != nil {
			return e
		}
	}
	return ir.SalvageError([]byte(s), "chatgpt_web")
}

// detailError `detail` 字段的两形态（字符串/对象；形态未证——容忍）。
func detailError(raw json.RawMessage, whole string) *ir.Error {
	if len(raw) == 0 || isJSONNull(raw) {
		return nil
	}
	if str, ok := jsonString(raw); ok && str != "" {
		return &ir.Error{Type: "api_error", Message: str, Source: "chatgpt_web", Raw: json.RawMessage(whole)}
	}
	var obj struct {
		Type    string          `json:"type"`
		Code    json.RawMessage `json:"code"`
		Message string          `json:"message"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	if obj.Message == "" && len(obj.Code) == 0 {
		return nil
	}
	e := &ir.Error{
		Type:    obj.Type,
		Code:    jsonCodeString(obj.Code),
		Message: obj.Message,
		Source:  "chatgpt_web",
		Raw:     json.RawMessage(whole),
	}
	if e.Type == "" {
		e.Type = "api_error"
	}
	if e.Message == "" {
		e.Message = "upstream error (detail object without message)"
	}
	return e
}

// ---------------------------------------------------------------------------
// 会话级 accessor（IR 无载体的记录面；服务壳 S3 消费）
// ---------------------------------------------------------------------------

// Title 会话标题（title_generation；A:842-843）。
func (p *StreamParser) Title() string { return p.title }

// ResumeToken resume_conversation_token（A:844-856）。**不要下发给用户**
// （JWT 里有 turn_topic_id）。
func (p *StreamParser) ResumeToken() string { return p.resumeToken }

// ConversationID 观察到的会话 id（多帧来源，首个非空）。
func (p *StreamParser) ConversationID() string { return p.conversationID }

// Handoff stream_handoff 记录（nil = 未出现）。续接动作属服务壳（S3）。
func (p *StreamParser) Handoff() *HandoffInfo { return p.handoff }

// Moderation moderation 记录（nil = 未出现）。
func (p *StreamParser) Moderation() *ModerationInfo { return p.moderation }

// ObservedModel 回读的实际模型（message.metadata.model_slug / server_ste_metadata；
// 请求 auto 时唯一来源——A:777-781）。
func (p *StreamParser) ObservedModel() string { return p.model }

// ObservedMetadata 最近一次捕获的 metadata 原文（message.metadata /
// server_ste_metadata / /message/metadata 补丁；保真旁路——IR 无载体，服务壳
// 按需读取）。nil = 未出现。
func (p *StreamParser) ObservedMetadata() json.RawMessage { return p.lastMetadata }

// ProtocolVersion 协议版本标记（"v1" = delta_encoding 标记已见；"" = 未见，
// 按旧档/宽容处理——两种协议并存，KB §陷阱 1）。
func (p *StreamParser) ProtocolVersion() string { return p.version }

// ToolMessages 被抑制消息/图片指针的记录（tool 消息面最小映射；S3/S6 消费）。
func (p *StreamParser) ToolMessages() []ToolMessage { return p.toolMessages }

// ToolOverflow 记录溢出条数（超 maxToolRecords 被丢的记录——防内存无界）。
func (p *StreamParser) ToolOverflow() int { return p.toolOverflow }

// SawLiveMessage 是否观察到 live assistant 消息（false + 有帧 = 假成功面候选：
// C head_process_response:111-124 的 403 unusual activity 判据；本包不合成错误）。
func (p *StreamParser) SawLiveMessage() bool { return p.sawLive }

// HistoryDropped 历史回放帧丢弃数（诊断）。
func (p *StreamParser) HistoryDropped() int { return p.historyDropped }

// Terminal 是否已观察到终态信号（任一结束判据命中 / 流内错误帧）。
//
// 服务壳的**尾读宽限**计时用（W30-2；CF `STREAM_TAIL_GRACE_MS` 同款语义）：
// 终态后上游仍握 socket（W6-2 实测 1.3–3s 才关流）——宽限窗口（终态后连续无新
// 帧）到期即收束，不等连接关闭。只读观测，不参与任何判据。
//
// **注意与 DefinitiveMarker 的区别**：Terminal 含 status finished/end_turn 等
// 非收束终态（W33-1 起 message_stream_complete 同属——其后仍有流末 burst：
// last_token → title → ste → complete → [detail] → [DONE]）；服务壳对
// 「Terminal 且非 DefinitiveMarker」的流才启用有界宽限（W32-1 方向 B）。
func (p *StreamParser) Terminal() bool { return p.terminalSeen || p.errorSeen }

// DefinitiveMarker 是否已观察到 definitive marker（`[DONE]` 哨兵 / 流内
// error 帧）——服务壳的**收束判据**（W32-1 尾读口径修正，W33-1 裁定收窄）：
// 到达即收束（不等宽限）；终态（status finished 等）或 message_stream_complete
// 而无流末哨兵时由有界宽限兜底。
//
// W33-1 裁定（detail 帧流式面，选项 A）：`message_stream_complete` **移出**
// 收束集——三份 golden/wire 帧序为 last_token → title_generation →
// server_ste_metadata → message_stream_complete → [conversation_detail_metadata]
// → [DONE]：complete 之后还有整个流末 burst（detail/[DONE]），s4 实机（非流式
// 读全尾）时间线显示该 burst 距 complete ≈13ms（s5 分段时序外推）。收束点 =
// 流末哨兵（[DONE]/error）——流式面由此捕获 detail（2/3 金标有；ISP 无——机会性
// 元数据）与 [DONE] 登记（done=true），与非流式面尾覆盖一致；complete 后无哨兵
// 的变体流回落到有界宽限（3s——与无 marker 流同一条规则）。stream_complete 仍
// 登记（StreamCompleteSeen，观测面）并触发终态（Terminal——宽限武装）。只读
// 观测，不参与解析判据。
func (p *StreamParser) DefinitiveMarker() bool {
	return p.doneSeen || p.errorSeen
}

// StreamCompleteSeen 是否已见 `message_stream_complete` 帧（尾部帧登记；W33-1
// 起**不参与收束判据**——complete 之后仍有 [conversation_detail_metadata]/[DONE]
// 流末 burst）。只读观测。
func (p *StreamParser) StreamCompleteSeen() bool { return p.streamCompleteSeen }

// DoneSentinelSeen 是否已见 `[DONE]` 哨兵（裸/引号包裹/内嵌解层三形态同判；
// 收束判据之一——W33-1）。只读观测。
func (p *StreamParser) DoneSentinelSeen() bool { return p.doneSeen }

// Markers message_marker 帧登记（W31-1 §4 尾部帧面；last_token 等——上限
// maxMarkerRecords，溢出见 MarkerOverflow）。只读观测，不产 IR 事件。
func (p *StreamParser) Markers() []MarkerInfo { return p.markers }

// MarkerOverflow 超上限被丢的 marker 帧数（防内存无界；同 ToolOverflow 纪律）。
func (p *StreamParser) MarkerOverflow() int { return p.markerOverflow }

// PatchCounter patch 帧 `c` 计数器的最近观察值（W29-1 §4.2 D3；ok=false = 流里
// 从未出现 `c`——快照档无此字段）。只观测：不参与任何判据。
func (p *StreamParser) PatchCounter() (int64, bool) { return p.lastCounter, p.counterSeen }

// PatchCounterFrames 带 `c` 的帧数（PatchCounterJumps 的分母）。
func (p *StreamParser) PatchCounterFrames() int { return p.counterFrames }

// PatchCounterJumps `c` 未按「上一值 +1」递增的次数（丢帧/乱序候选；首帧不计）。
// 实抓两份 v1 golden 均为 0（`c:0..6` 逐帧 +1）。
func (p *StreamParser) PatchCounterJumps() int { return p.counterJumps }

// ConversationDetailMetadata conversation_detail_metadata 帧的原文（W29-1 §4.2
// D4；nil = 未出现）。字段在**帧顶层**（非 metadata 子对象）——保真整帧，供服务壳
// 取 default_model_slug / model_limits / limits_progress / banner_info。
func (p *StreamParser) ConversationDetailMetadata() json.RawMessage { return p.detailMetadata }

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

// imagePointerOf part 对象 → 图片指针（asset_pointer 字段）。
func imagePointerOf(raw json.RawMessage) (ImagePointer, bool) {
	var obj struct {
		AssetPointer string `json:"asset_pointer"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil || obj.AssetPointer == "" {
		return ImagePointer{}, false
	}
	return ImagePointer{Pointer: obj.AssetPointer, Scheme: pointerScheme(obj.AssetPointer)}, true
}

// pointerScheme 指针 scheme 分类（L 文档「Image pointer types」表；未知 → ""）。
func pointerScheme(p string) string {
	switch {
	case strings.HasPrefix(p, schemeFileService):
		return "file-service"
	case strings.HasPrefix(p, schemeSediment):
		return "sediment"
	case strings.HasPrefix(p, schemeFileUpload):
		return "file_upload"
	default:
		return ""
	}
}

// objectTextOf part 对象里的 text 字段（multimodal 对象形态的兜底面）。
func objectTextOf(raw json.RawMessage) (string, bool) {
	var obj struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil || obj.Text == "" {
		return "", false
	}
	return obj.Text, true
}

// jsonString JSON 字符串值（非字符串 → false）。
func jsonString(raw json.RawMessage) (string, bool) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

// jsonBool JSON 布尔值（非布尔 → false）。
func jsonBool(raw json.RawMessage) (bool, bool) {
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil {
		return false, false
	}
	return b, true
}

// jsonInt JSON 数值 → int64（整数优先；容忍浮点形态 `7.0`）。非数值 → false。
func jsonInt(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 || isJSONNull(raw) {
		return 0, false
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n, true
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return int64(f), true
	}
	return 0, false
}

// jsonCodeString code 的双形态（字符串枚举 / 数值）→ 字符串。
func jsonCodeString(raw json.RawMessage) string {
	if len(raw) == 0 || isJSONNull(raw) {
		return ""
	}
	if s, ok := jsonString(raw); ok {
		return s
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		return n.String()
	}
	return ""
}

// firstNonEmpty 首个非空字符串。
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// isJSONNull 值是否为 JSON null（空 RawMessage 视为缺失，不算 null）。
func isJSONNull(raw json.RawMessage) bool {
	return len(raw) > 0 && strings.TrimSpace(string(raw)) == "null"
}

// truncate 截断诊断字符串。
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
