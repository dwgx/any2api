package chatgpt_web

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ---------------------------------------------------------------------------
// v1 JSON-Patch 状态机（件 2；逐条对照 B internal/gateway/delta.go:14-36,81-193）
// ---------------------------------------------------------------------------

// patchState v1 patch 状态机的工作状态。纪律（B delta.go 头注逐条）：
//
//   - **curP 继承**（B:14-17）：一帧没带 `p` 时沿用上一帧的路径（简写帧语义，
//     F openai-adapter.ts:186-196「不维护会丢 ~90% 正文」）；只有本帧显式出现
//     p 才覆盖；
//   - **recipient**（B:18-20）：只有 "all"（或字段缺省）才当正文，其余
//     （python/browser/dalle.text2im/t2uay3k.sj1i4kz…）是工具调用；
//   - 顶层字符串增量**不查 op**（与 B:93-106 一致）；数组内才要求
//     op ∈ {"", append}（B:126-131）——F 的 lastOp 记忆语义本包不启用
//     （toolkit 会把简写帧按上一 op 重放；本包取 B 语义，见 dialect_test.go）；
//   - **lastFull / accText**：旧协议快照差分基线（B:168-193 前缀差分 +
//     A:828-834 的回退守卫；accText 记录 patch 路径已产出的正文，处理
//     「patch 与快照混用」——B/A/C 均无此基线组合 [差异记录]）。
type patchState struct {
	curP      string
	recipient string // "" → 视为 "all"（B:49-51 的缺省）
	lastFull  string
	accText   string
}

// recipientAll 当前 recipient 是否给用户正文（缺省 = all，B:49-51）。
func (s *patchState) recipientAll() bool { return s.recipient == "" || s.recipient == "all" }

// resetForMessage 采纳新 live 消息时重置消息级状态（CF chatgpt2api-cf
// stream.ts:119-124：「The live reply (re)starts: reset the cumulative
// snapshot-diff state」）。recipient 按消息字段重取——缺省 "all"（B:49-51 的
// 缺省规则按消息作用域应用；[差异记录] B 只在字段非空时覆盖、跨消息沿用旧值，
// 本包按消息重置更稳：新消息缺省 recipient 时不会继承上一条工具消息的抑制态）。
func (s *patchState) resetForMessage(m *messageWire) {
	if m != nil && m.Recipient != "" {
		s.recipient = m.Recipient
	} else {
		s.recipient = "all"
	}
	s.lastFull = ""
	s.accText = ""
}

// patchOutcome 一帧（或数组内一条 patch）的产物；零值 = 无内容。
type patchOutcome struct {
	Text     string          // 正文增量（recipient==all 且路径 ∈ {空继承, parts/0}）
	Thinking string          // 思考增量（thoughts/reasoning 路径，已清洗控制串）
	Final    bool            // 判据②：/message/status → finished_successfully（B:95-100）
	Status   string          // 观察到的状态值（诊断）
	EndTurn  bool            // /message/end_turn → true（补充结束信号，CF:122-128）
	Metadata json.RawMessage // /message/metadata 载荷（数组内 patch；A:902-906）
}

// empty 零值判定（数组分支只收集有产物的条目）。
func (o patchOutcome) empty() bool {
	return o.Text == "" && o.Thinking == "" && !o.Final && o.Status == "" && !o.EndTurn && len(o.Metadata) == 0
}

// applyValue 应用一条 patch（顶层帧或数组内一条）。
//
// arrayItem 区分两处的 op 门控差异（B:93-106 顶层字符串帧不查 op；B:126-131
// 数组内要求 op ∈ {"", append}）。状态/结束/元数据路径对任意 v 类型生效
// （判据②在任何 recipient 下观察——是否终结由 parse 层的 live 上下文门控，
// 见 parse.go handleMessage；B:95-104 在非 all 分支同样观察状态）。
func (s *patchState) applyValue(p, o string, v json.RawMessage, arrayItem bool) patchOutcome {
	var out patchOutcome
	if p != "" {
		s.curP = p
	}
	switch s.curP {
	case pathStatus:
		if str, ok := jsonString(v); ok {
			out.Status = str
			if str == statusFinishedSuccessfully {
				out.Final = true
			}
		}
		return out
	case pathEndTurn:
		if b, ok := jsonBool(v); ok && b {
			out.EndTurn = true
		}
		return out
	case pathMetadata:
		out.Metadata = v
		return out
	}
	// 思考路径（B:81-85 thoughts 前缀 + A:117-160 reasoning 类路径）。
	if s.thinkingPath() {
		if str, ok := jsonString(v); ok {
			out.Thinking = cleanReasoning(str)
		}
		return out
	}
	str, ok := jsonString(v)
	if !ok {
		return out
	}
	if !s.recipientAll() {
		return out // 工具消息正文：抑制（B:98-104；KB quirk 4）
	}
	if s.curP != "" && s.curP != pathParts0 {
		return out
	}
	if arrayItem && o != "" && o != opAppend {
		return out
	}
	out.Text = str
	return out
}

// applyFrame 顶层帧（v 为字符串或其它标量）。字符串增量不查 op（B:93-106）。
func (s *patchState) applyFrame(p, o string, v json.RawMessage) patchOutcome {
	return s.applyValue(p, o, v, false)
}

// applyArray 批量 patch 帧（①c；B:107-137）：数组内按序作用；结束标记常夹带
// 在数组内（B:118-123 的状态分支 + L 文档样例）。返回逐条的产物（保序——
// thinking/text 交错时顺序不丢）。
func (s *patchState) applyArray(v json.RawMessage) ([]patchOutcome, error) {
	var items []json.RawMessage
	if err := json.Unmarshal(v, &items); err != nil {
		return nil, fmt.Errorf("%w: patch array: %v", ErrBadEvent, err)
	}
	var outs []patchOutcome
	for _, item := range items {
		var it struct {
			P string          `json:"p"`
			O string          `json:"o"`
			V json.RawMessage `json:"v"`
		}
		if err := json.Unmarshal(item, &it); err != nil {
			continue // 非对象项：跳过（B:112-115 的 !ok 继续）
		}
		r := s.applyValue(it.P, it.O, it.V, true)
		if !r.empty() {
			outs = append(outs, r)
		}
	}
	return outs, nil
}

// snapshotDelta 旧协议快照正文差分（B:168-193 前缀差分 + A:828-834 回退守卫）：
//
//   - 基线 = lastFull；若 accText（patch 已产出正文）更长且为 text 前缀，取
//     accText（处理 patch 与快照混用 [差异记录]）；
//   - text 以基线为前缀 → 增量 = 后缀（B:174-176）；
//   - text 是基线的（真）前缀 / 与基线相等 → 增量 ""（回归不重发；A:828-834
//     守卫；B 无此守卫会整段重发 [差异记录]）；
//   - 其它（非前缀）→ 整段重发（B:176-179 的 else 分支）。
//
// 恒推进 lastFull（B:180 在 recipient 检查前推进基线）。
func (s *patchState) snapshotDelta(text string) string {
	base := s.lastFull
	if s.accText != "" && len(s.accText) > len(base) && strings.HasPrefix(text, s.accText) {
		base = s.accText
	}
	var delta string
	switch {
	case strings.HasPrefix(text, base):
		delta = text[len(base):]
	case base != "" && strings.HasPrefix(base, text):
		delta = "" // 回归/相等：不重发（A:828-834 守卫）
	default:
		delta = text
	}
	s.lastFull = text
	return delta
}

// noteText 记录 patch 路径已产出的正文（快照差分基线之一）。
func (s *patchState) noteText(text string) {
	if text != "" {
		s.accText += text
	}
}

// thinkingPath 当前路径是否属于思考通道：
//
//   - thoughts 前缀（B:81-85：`/message/content/thoughts…` 的增量全部当思考）；
//   - reasoning 类路径（A:117-160：路径含 reasoning_content / reasoning_delta /
//     thought，且不含 /metadata——metadata 里是 lifecycle 标签（reasoning_ended/
//     hide_all），A:139-141 明确跳过，不能当思考正文）。
func (s *patchState) thinkingPath() bool {
	if strings.HasPrefix(s.curP, prefixThoughts) {
		return true
	}
	lower := strings.ToLower(s.curP)
	if strings.Contains(lower, "/metadata") {
		return false
	}
	return strings.Contains(lower, "reasoning_content") ||
		strings.Contains(lower, "reasoning_delta") ||
		strings.Contains(lower, "thought")
}

// ---------------------------------------------------------------------------
// 思考控制串清洗（A upstream.py:60-92）
// ---------------------------------------------------------------------------

// reasoningControlValues UI 控制串整值表（A:61-70 原表）——这些值混在思考
// 增量流里，是状态标签不是正文，直接拼接会污染输出。
var reasoningControlValues = map[string]bool{
	"reasoning_started":   true,
	"reasoning_ended":     true,
	"reasoning_complete":  true,
	"reasoning_completed": true,
	"hide_all":            true,
	"show_all":            true,
	"finished reasoning":  true,
	"reasoning":           true,
}

// reasoningCompactControl 去空格后的复合控制串（A:76-81 原表）。
var reasoningCompactControl = map[string]bool{
	"reasoning_endedhide_all":     true,
	"reasoning_startedshow_all":   true,
	"reasoning_completedhide_all": true,
}

// cleanReasoning 思考增量清洗（A:72-92）：整值匹配控制串（含去空格复合形态、
// `reasoning_*` 前缀 ≤2 词规则）则丢弃；否则原样返回 trim 后的文本。
// 注：A 另有 clean_visible_text 前置清洗（其产品面）——本包不做，golden 帧后按需补。
func cleanReasoning(value string) string {
	text := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(value, "\r\n", "\n"), "\r", "\n"))
	if text == "" {
		return ""
	}
	normalized := strings.Join(strings.Fields(
		strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(text), "-", "_"), ":", " "),
	), " ")
	if reasoningControlValues[normalized] {
		return ""
	}
	if reasoningCompactControl[strings.ReplaceAll(normalized, " ", "")] {
		return ""
	}
	if strings.HasPrefix(normalized, "reasoning_") && len(strings.Fields(normalized)) <= 2 {
		return ""
	}
	return text
}

// ---------------------------------------------------------------------------
// 历史回放过滤（三实现判据合成；见 wire.go「关键取舍」注）
// ---------------------------------------------------------------------------

// isHistoryEcho 历史回放判据（CF chatgpt2api-cf stream.ts:103-118 主形态）：
//
//	msgId != liveId && status ∈ {finished_successfully, finished} &&
//	metadata.message_type != "next" && channel != "final"
//
// 语义：上游在流首把历史消息逐条重放（每条都是 finished）——必须过滤，否则
// 客户端会看到历史文本（KB §流式细节「多消息交错」）。三实现判据：
//
//   - CF:103-118（本函数主形态；专门为此而写，证据最强）；
//   - Go request.go:444-446：message_type ∉ {next, continue} 一律丢——比 CF 更宽
//     （"continue" 也保）[差异记录]；
//   - C chatFormat.py:201-202：只认第一条消息 id——比 CF 更窄（换 id 即丢）
//     [差异记录]。
//
// 与 B 的矛盾：B 的快照分支对 status==finished_successfully 直接判终止
// （:155-163），会把历史帧误判为流结束——本包以 CF 判据优先（报告 §方言差异；
// golden 帧到位后 S4/S5 重校）。
func isHistoryEcho(m *messageWire, liveID string) bool {
	if m.ID == liveID {
		return false
	}
	if m.Status != statusFinishedSuccessfully && m.Status != statusFinished {
		return false
	}
	if m.Metadata.MessageType == messageTypeNext {
		return false
	}
	if m.Channel == channelFinal {
		return false
	}
	return true
}
