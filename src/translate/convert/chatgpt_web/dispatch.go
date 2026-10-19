package chatgpt_web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dwgx/any2api/translate/ir"
)

// ---------------------------------------------------------------------------
// 事件分流（件 3；对照 A web_client.py:838-921 + B delta.go:71-74）
// ---------------------------------------------------------------------------

// feedPayload 处理一个已解码载荷（Feed 内核；raw = DecodePayload 的归一化输出）。
//
// 分派顺序（对照 A _parse_event:838-921）：
//
//  0. 流内 error 帧（A:841-842 最先检查）→ IR error 事件（终态）；
//  1. 类型化事件 type ∈ {message_stream_complete, title_generation,
//     resume_conversation_token, stream_handoff, server_ste_metadata, moderation,
//     conversation_detail_metadata}
//     → 分流处理（A:842-868,916-917 + B:71-74；后者只记录，W29-1 D4）；
//  2. 顶层 message（旧协议快照②；A:870-877）→ handleMessage；
//  3. v 形状：对象（①d 首帧）→ handleMessage；数组（①c）→ applyArray；
//     字符串（①a/①b）→ applyFrame；
//  4. 其余（无 v 的未知类型化事件/input_message/message_marker/标量）→ 忽略
//     （版本策略；A:877-882 只记 conversation_id/message_id）。
func (p *StreamParser) feedPayload(raw json.RawMessage) ([]ir.StreamEvent, error) {
	s := bytes.TrimSpace(raw)
	if len(s) == 0 {
		return nil, nil
	}
	switch s[0] {
	case '"':
		// JSON 字符串标量："v1" 协议标记 / 裸文本——无 IR 载体。
		var str string
		if err := json.Unmarshal(s, &str); err != nil {
			return nil, fmt.Errorf("%w: string scalar: %v", ErrBadEvent, err)
		}
		switch str {
		case markerV1:
			p.version = markerV1 // v1 档标记（F:105-130）
		case sentinelDone:
			// 容忍面：被引号包裹的 [DONE]（代理改写形态）——同判据④。
			p.doneSeen = true // definitive marker 登记（W32-1）
			return p.terminal(p.terminalReason()), nil
		}
		return nil, nil
	case '{':
		// 对象：继续。
	default:
		// 数字/布尔/null 标量；容忍面：内嵌解层后可能暴露裸 [DONE]（同判据④）。
		if string(s) == sentinelDone {
			p.doneSeen = true // definitive marker 登记（W32-1）
			return p.terminal(p.terminalReason()), nil
		}
		return nil, nil
	}

	var f patchFrameWire
	if err := json.Unmarshal(s, &f); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadEvent, err)
	}
	p.framesSeen++
	p.noteCounter(f.C) // 观测面：`c` 计数器（W29-1 §4.2 D3；不参与判据）

	// 0. 流内 error 帧（HTTP 仍是 200；KB §断流/心跳/error 帧）。
	if len(f.Error) > 0 && !isJSONNull(f.Error) {
		return p.handleErrorFrame(f.Error, s)
	}

	// 1. 类型化事件（③）。
	switch f.Type {
	case typeMessageStreamComplete:
		// 判据①（B:71-74；/f/conversation 特有结束事件）。definitive marker
		// （W32-1）：到达即收束——服务壳不等尾读宽限。
		p.streamCompleteSeen = true
		return p.terminal(p.terminalReason()), nil
	case typeTitleGeneration:
		if p.title == "" {
			if t := strings.TrimSpace(f.Title); t != "" {
				p.title = t // 会话标题（A:842-843）——不进 IR（IR 无载体），accessor 暴露
			}
		}
		return nil, nil
	case typeResumeConversationToken:
		// 存 token（JWT 里有 turn_topic_id），**不要下发给用户**（A:844-856）。
		if f.Token != "" {
			p.resumeToken = f.Token
		}
		if f.ConversationID != "" {
			p.conversationID = f.ConversationID
		}
		return nil, nil
	case typeStreamHandoff:
		p.recordHandoff(f)
		return nil, nil
	case typeServerSteMetadata:
		p.captureMetadata(f.Metadata) // 本轮调度/工具状态 + model_slug 回读（A:916-917）
		return nil, nil
	case typeConversationDetailMetadata:
		// 会话级详情（W29-1 §4.2 D4）：字段在帧**顶层**（banner_info/
		// model_limits/limits_progress/default_model_slug…）——IR 无载体，保真整帧
		// 供服务壳（ConversationDetailMetadata）；不产事件。
		p.detailMetadata = append(json.RawMessage(nil), s...)
		return nil, nil
	case typeModeration:
		p.recordModeration(f.Moderation) // 记录；文案/错误帧属出口决策（C:18,186-188）
		return nil, nil
	case typeMessageMarker:
		// W32-1 尾部帧面：marker 帧登记（原先是「整类忽略」——W31-1 实证流式
		// 4/4 截于 last_token，登记后截断点/内容生命周期在观测面可见）。
		p.recordMarker(f)
		return nil, nil
	}

	if f.ConversationID != "" {
		p.conversationID = f.ConversationID
	}

	// 2. 顶层 message（旧协议快照②）。
	if len(f.Message) > 0 && !isJSONNull(f.Message) {
		var m messageWire
		if err := json.Unmarshal(f.Message, &m); err != nil {
			return nil, fmt.Errorf("%w: snapshot message: %v", ErrBadEvent, err)
		}
		return p.handleMessage(&m)
	}

	// 3. v 形状。
	v := bytes.TrimSpace(f.V)
	if len(v) == 0 || isJSONNull(f.V) {
		// 无 v（含 input_message / message_marker / 未知类型化事件）：忽略
		//（lowkruc：input_message 是用户输入回显，别当输出）。
		return nil, nil
	}
	switch v[0] {
	case '{':
		// 对象值分两态：带 message 的对象（①d 首帧 / add 形态）→ 消息路径；
		// 显式 patch 路径的对象值（如 /message/metadata replace；A:902-906 只在
		// 数组内见过——本包放宽到顶层）→ patch 路径；其余（纯 conversation_id
		// 更新）→ 消息路径（只取 conversation_id）。
		var probe struct {
			Message json.RawMessage `json:"message"`
		}
		_ = json.Unmarshal(v, &probe)
		if len(probe.Message) > 0 && !isJSONNull(probe.Message) {
			return p.handleObjectFrame(v)
		}
		if f.P != "" || f.O != "" {
			return p.applyPatchOutcome(p.st.applyFrame(f.P, f.O, f.V)), nil
		}
		return p.handleObjectFrame(v)
	case '[':
		// ①c 批量 patch（B:107-137）——逐条产物保序发射。
		outs, err := p.st.applyArray(f.V)
		if err != nil {
			return nil, err
		}
		var events []ir.StreamEvent
		for _, out := range outs {
			evs := p.applyPatchOutcome(out)
			events = append(events, evs...)
		}
		return events, nil
	default:
		// ①a/①b 字符串增量（简写帧靠 curP 继承）；布尔/数值标量只在显式
		// 路径（status/end_turn/metadata）下有产物。
		return p.applyPatchOutcome(p.st.applyFrame(f.P, f.O, f.V)), nil
	}
}

// handleObjectFrame 处理 v 为对象的帧（①d；B:139-166）。
func (p *StreamParser) handleObjectFrame(v json.RawMessage) ([]ir.StreamEvent, error) {
	var conv struct {
		ConversationID string          `json:"conversation_id"`
		Message        json.RawMessage `json:"message"`
	}
	if err := json.Unmarshal(v, &conv); err != nil {
		return nil, fmt.Errorf("%w: object frame: %v", ErrBadEvent, err)
	}
	if conv.ConversationID != "" {
		p.conversationID = conv.ConversationID
	}
	if len(conv.Message) == 0 || isJSONNull(conv.Message) {
		return nil, nil // 无 message 的对象帧（纯 conversation_id 更新）
	}
	var m messageWire
	if err := json.Unmarshal(conv.Message, &m); err != nil {
		return nil, fmt.Errorf("%w: message: %v", ErrBadEvent, err)
	}
	return p.handleMessage(&m)
}

// handleErrorFrame 流内 error 帧（A:841-842；Go request.go:426-429 的 500 面）。
// 终态：其后只容忍重复结束标记/心跳/元数据（Feed 的 already 门）。
func (p *StreamParser) handleErrorFrame(errRaw, whole json.RawMessage) ([]ir.StreamEvent, error) {
	if p.errorSeen || p.terminalSeen {
		return nil, nil // 幂等
	}
	p.errorSeen = true
	var events []ir.StreamEvent
	events = append(events, p.ensureStart()...)
	events = append(events, p.closeBlock()...)
	events = append(events, ir.StreamEvent{
		Kind:        ir.EventError,
		ErrorReason: ir.ErrorUpstream,
		Error:       streamError(errRaw, whole),
	})
	return events, nil
}

// streamError 流内 error 载荷 → IR 错误信封。
//
// 形态面未证（KB 只证 `{"error": …}` 的存在）——复用 IR 兜底提取
// （ir.SalvageError 的 one-api 同序表：error 对象/字符串、message/msg、
// header.message…），Raw 保真整个帧（仅诊断；渲染器绝不透传）。
func streamError(errRaw, whole json.RawMessage) *ir.Error {
	_ = errRaw // 提取面以整帧为准（SalvageError 自取 error 键）
	e := ir.SalvageError(whole, "chatgpt_web")
	if e == nil {
		e = &ir.Error{Type: "api_error", Message: "upstream stream error", Source: "chatgpt_web", Raw: whole}
	}
	return e
}

// recordHandoff 记录 stream_handoff（A:857-868）：options 里
// subscribe_ws_topic / resume_sse_endpoint 的 topic_id。**续接动作属服务壳**。
func (p *StreamParser) recordHandoff(f patchFrameWire) {
	if f.ConversationID != "" {
		p.conversationID = f.ConversationID
	}
	info := &HandoffInfo{ConversationID: f.ConversationID}
	if len(f.Options) > 0 && !isJSONNull(f.Options) {
		var options []struct {
			Type    string `json:"type"`
			TopicID string `json:"topic_id"`
		}
		if err := json.Unmarshal(f.Options, &options); err == nil {
			for _, opt := range options {
				if opt.TopicID == "" {
					continue
				}
				switch opt.Type {
				case "subscribe_ws_topic":
					info.WSTopicID = opt.TopicID
				case "resume_sse_endpoint":
					info.SSETopicID = opt.TopicID
				}
			}
		}
	}
	p.handoff = info
}

// recordModeration 记录 moderation 事件（C:18,186-188；L 文档 moderation case）。
func (p *StreamParser) recordModeration(raw json.RawMessage) {
	info := &ModerationInfo{Raw: raw}
	if len(raw) > 0 && !isJSONNull(raw) {
		var mr struct {
			Blocked bool `json:"blocked"`
		}
		if err := json.Unmarshal(raw, &mr); err == nil {
			info.Blocked = mr.Blocked
		}
	}
	p.moderation = info
}

// recordMarker 登记 message_marker 帧（W32-1 尾部帧面；上限 maxMarkerRecords，
// 超限计数——防畸形流内存无界）。只读观测：不产 IR 事件、不参与判据。
func (p *StreamParser) recordMarker(f patchFrameWire) {
	if len(p.markers) >= maxMarkerRecords {
		p.markerOverflow++
		return
	}
	p.markers = append(p.markers, MarkerInfo{
		Marker:    f.Marker,
		Event:     f.Event,
		MessageID: f.MessageID,
	})
}

// noteCounter 观测 patch 帧的 `c` 计数器（W29-1 §4.2 D3：v1 档消息帧 `c:0..6`
// 逐帧 +1）。**只观测**——丢帧/乱序诊断（PatchCounter/PatchCounterFrames/
// PatchCounterJumps），不参与任何判据。非数值/缺省 → 不计入。
func (p *StreamParser) noteCounter(raw json.RawMessage) {
	n, ok := jsonInt(raw)
	if !ok {
		return
	}
	if p.counterSeen && n != p.lastCounter+1 {
		p.counterJumps++
	}
	p.counterSeen = true
	p.lastCounter = n
	p.counterFrames++
}

// captureMetadata 捕获 metadata 对象（A:777-791 _capture_metadata 的消费面）：
// model_slug/model/default_model_slug/selected_model → 回读模型（请求 auto 时唯一
// 来源）；finish_details.type → finish_reason 判据。原文保真到 lastMetadata。
func (p *StreamParser) captureMetadata(raw json.RawMessage) {
	if len(raw) == 0 || isJSONNull(raw) {
		return
	}
	var md struct {
		ModelSlug        string `json:"model_slug"`
		Model            string `json:"model"`
		DefaultModelSlug string `json:"default_model_slug"`
		SelectedModel    string `json:"selected_model"`
		FinishDetails    struct {
			Type string `json:"type"`
		} `json:"finish_details"`
	}
	if err := json.Unmarshal(raw, &md); err != nil {
		return
	}
	for _, slug := range []string{md.ModelSlug, md.Model, md.DefaultModelSlug, md.SelectedModel} {
		if slug != "" {
			p.model = slug
			break
		}
	}
	if md.FinishDetails.Type != "" {
		p.finishType = md.FinishDetails.Type
	}
	p.lastMetadata = raw
}
