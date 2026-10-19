package openai_chat

// W41 ② + W42 ①（A2A-L19-W41-1 / W42-1）：OpenRouter 兼容 `reasoning_details`
// 载体（读/写双面）。
//
// 依据：longtail-matrix §4.3 / P17（「OpenRouter 字段损耗面」）+ OpenRouter 官方
// 文档副本 `tmp/lanes/W40-oaicompat/raw/openrouter/reasoning-tokens.md:880-1030`
//（2026-10-15 实读）：
//   - 条目公共字段 {id, format, index} + 三类型：reasoning.summary{summary} /
//     reasoning.encrypted{data} / reasoning.text{text, signature}；
//   - format 9 枚举（openai-responses-v1 / azure-openai-responses-v1 /
//     bedrock-openai-responses-v1 / bedrock-xai-responses-v1 / xai-responses-v1 /
//     meta-responses-v1 / anthropic-claude-v1 / google-gemini-v1 / unknown）；
//     本仓只写能形态判定的枚举（别家枚举值不发明）；
//   - 流式 delta.reasoning_details 逐块；消费端按 index 分组（文本拼接、签名取
//     最后一条——roo-code `openai-format.ts` consolidateReasoningDetails 实证）；
//   - tool calling 面「Pass back unmodified」回传 reasoning_details（同文档
//     §Preserving Reasoning Blocks）。
//
// W42 ① 扩展（三补）：
//   - reasoning.summary 读面 → 思考文本（reasoning.text 缺省时的回退——消费端
//     consolidate 口径：text 优先、summary 仅在无 text 时成立）；
//   - reasoning.encrypted 读面：id 与同消息/同流的 tool_call 配对 → 该 tool_use
//     块签名（②Gemini 3 工具签名通道。线索复核：roo-code
//     `src/api/providers/openrouter.ts:247-286` 注入 {type:"reasoning.encrypted",
//     data:"skip_thought_signature_validator", id:toolCalls[0].id,
//     format:"google-gemini-v1", index:0} + `src/api/transform/openai-format.ts:149-235`
//     sanitizeGeminiMessages 按 d.id === tool_call.id 配对）；无 tool 配对的
//     Claude 族载荷 → redacted_thinking（data ↔ 载荷）；
//   - reasoning.encrypted 写面：tool_use 块的 Gemini 签名（或官方占位符）→
//     encrypted{data, id=tool_call 线 id, format:google-gemini-v1}；
//     redacted_thinking 载荷 → encrypted{data, format:anthropic-claude-v1}。
//
// 签名纪律（红线：绝不凭空塞未判定思考）：签名先过 signature.ProviderForSignature
// 形态校验——可识别才进载体（format 由判定结果映射）；不可识别 → 丢弃，绝不写进
// wire。读面同口径：载体带来的签名不可识别 → 不落 IR。唯一例外 = Google 官方
// 占位符 `skip_thought_signature_validator`（magic value：Google thought-signatures
// 文档「历史来自别家模型」的合法表达 + GoModel native.go:85 + 本仓
// gemini/render.go 同款）——它是「跳过校验」的显式表达，按原样透传。
//
// 边界（本波不做）：写面不产 reasoning.summary（IR 无 summary/text 区分——思考
// 文本一律走 reasoning.text；summary 是读面输入，见报告 §决策）。

import (
	"encoding/json"
	"strings"

	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/signature"
)

// reasoning_details format 枚举（本仓可判定的三个 + 读面缺省口径）。
const (
	reasoningFormatClaude = "anthropic-claude-v1"
	reasoningFormatGemini = "google-gemini-v1"
	reasoningFormatGPT    = "openai-responses-v1"
)

// reasoningSkipValidatorPlaceholder Google 官方占位符（gemini/render.go
// skipThoughtSignatureValidator 同值——转换器包不互相 import，常量各自声明）。
const reasoningSkipValidatorPlaceholder = "skip_thought_signature_validator"

// reasoningDetailFormat 签名 → OpenRouter reasoning_details 的 format 枚举值
// （9 枚举中我们能形态判定的三个；判定失败 → false，调用方丢弃签名）。
func reasoningDetailFormat(sig string) (string, bool) {
	p, ok := signature.ProviderForSignature(sig)
	if !ok {
		return "", false
	}
	switch p {
	case signature.ProviderClaude:
		return reasoningFormatClaude, true
	case signature.ProviderGemini:
		return reasoningFormatGemini, true
	case signature.ProviderGPT:
		return reasoningFormatGPT, true
	default:
		return "", false
	}
}

// reasoningToolSignature tool_use 块的签名 → chat 载体（reasoning.encrypted{id}）
// 判定（②）：Gemini 形态（field-2 信封）或官方占位符可承载（format 恒
// google-gemini-v1）；其余（不可识别 / 别族形态）→ false，调用方丢弃（红线）。
// 读/写面共用（同一门槛，hub 腿往返经本判定两次——幂等）。
func reasoningToolSignature(sig string) (string, bool) {
	if sig == "" {
		return "", false
	}
	if sig == reasoningSkipValidatorPlaceholder {
		return reasoningFormatGemini, true
	}
	if p, ok := signature.ProviderForSignature(sig); ok && p == signature.ProviderGemini {
		return reasoningFormatGemini, true
	}
	return "", false
}

// reasoningRedactedFormat 无 tool 配对的 encrypted 载荷能否当 Claude 加密思考
// 载荷（redacted_thinking）读：Claude 族 format（缺省 = 文档默认
// anthropic-claude-v1）。别族（xai/openai-*）载荷不映射——redacted_thinking 是
// Claude 原生概念，把别族密文塞给它 = 上游 400 面（红线同族纪律）。
func reasoningRedactedFormat(format string) bool {
	return format == "" || format == reasoningFormatClaude
}

// reasoningTextDetail 组装一条 reasoning.text 条目（写面；index 由调用方按消息/
// 流内的思考块序给）。text/signature 各自可缺省：签名条目只带 signature+format。
func reasoningTextDetail(text, sig, format string, index int) json.RawMessage {
	d := map[string]json.RawMessage{
		"type":  mustMarshal("reasoning.text"),
		"index": mustMarshal(index),
	}
	if text != "" {
		d["text"] = mustMarshal(text)
	}
	if sig != "" {
		d["signature"] = mustMarshal(sig)
		if format != "" {
			d["format"] = mustMarshal(format)
		}
	}
	return mustMarshal(d)
}

// reasoningEncryptedDetail 组装一条 reasoning.encrypted 条目（写面）。data 恒带；
// id 仅 tool 签名条目（= 同消息 tool_calls[].id 的线值）；format 由调用方定
// （tool 签名 = google-gemini-v1；redacted 载荷 = anthropic-claude-v1）。
func reasoningEncryptedDetail(data, id, format string, index int) json.RawMessage {
	d := map[string]json.RawMessage{
		"type":  mustMarshal("reasoning.encrypted"),
		"data":  mustMarshal(data),
		"index": mustMarshal(index),
	}
	if id != "" {
		d["id"] = mustMarshal(id)
	}
	if format != "" {
		d["format"] = mustMarshal(format)
	}
	return mustMarshal(d)
}

// reasoningDetailWire reasoning_details 单条（读面；宽容：未知类型/缺字段都不炸）。
type reasoningDetailWire struct {
	Type      string `json:"type"`
	Text      string `json:"text"`
	Summary   string `json:"summary"`
	Data      string `json:"data"`
	Signature string `json:"signature"`
	ID        string `json:"id"`
	Format    string `json:"format"`
	Index     *int   `json:"index"`
}

// reasoningDetailsOf 宽容解析 reasoning_details 原文：缺失/null/非数组 → nil；
// 数组里畸形的单条跳过（不炸整条消息/整帧）。
func reasoningDetailsOf(raw json.RawMessage) []reasoningDetailWire {
	if len(raw) == 0 || isJSONNull(raw) {
		return nil
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil
	}
	out := make([]reasoningDetailWire, 0, len(arr))
	for _, el := range arr {
		var d reasoningDetailWire
		if err := json.Unmarshal(el, &d); err != nil {
			continue
		}
		out = append(out, d)
	}
	return out
}

// reasoningEncryptedGroup encrypted 条目按 index 分组的拼接结果（消费端
// consolidate 口径：同 index 的 data 按到达顺序拼接、id/format 取组内首个非空）。
type reasoningEncryptedGroup struct {
	ID     string
	Format string
	Data   string
}

// reasoningCarrier reasoning_details 读面解析结果（非流式；parseMessage 用）。
type reasoningCarrier struct {
	// text reasoning.text 文本拼接；无 text 条目 → summary 文本拼接（回退）。
	text string
	// sig reasoning.text 最后一条**形态可识别**签名（不可识别丢弃——红线；
	// 与消费端 consolidateReasoningDetails 的「签名取最后一条」同口径）。
	sig string
	// encrypted 全部 reasoning.encrypted 组（按 index 首见顺序；组内拼接）。
	// 分类（tool 签名 / redacted 载荷 / 丢弃）由调用方按同消息 tool_calls 配对。
	encrypted []reasoningEncryptedGroup
}

// readReasoningCarrier 非流式读面：reasoning_details 原文 → 结构化结果
// （text/summary/encrypted；畸形条目跳过——宽容读取）。
func readReasoningCarrier(raw json.RawMessage) reasoningCarrier {
	var c reasoningCarrier
	details := reasoningDetailsOf(raw)
	if len(details) == 0 {
		return c
	}
	var text, summary strings.Builder
	var encOrder []int
	encs := map[int]*reasoningEncryptedGroup{}
	for _, d := range details {
		switch d.Type {
		case "reasoning.text":
			text.WriteString(d.Text)
			if d.Signature != "" {
				if _, ok := signature.ProviderForSignature(d.Signature); ok {
					c.sig = d.Signature
				} else {
					c.sig = "" // 不可识别签名不得留在 IR（红线）；后续条目可再覆盖。
				}
			}
		case "reasoning.summary":
			summary.WriteString(d.Summary)
		case "reasoning.encrypted":
			if d.Data == "" {
				continue
			}
			idx := 0
			if d.Index != nil {
				idx = *d.Index
			}
			g := encs[idx]
			if g == nil {
				g = &reasoningEncryptedGroup{}
				encs[idx] = g
				encOrder = append(encOrder, idx)
			}
			g.Data += d.Data
			if g.ID == "" {
				g.ID = d.ID
			}
			if g.Format == "" {
				g.Format = d.Format
			}
		}
	}
	c.text = text.String()
	if c.text == "" {
		c.text = summary.String() // summary 回退（text 优先——consolidate 口径）。
	}
	for _, idx := range encOrder {
		c.encrypted = append(c.encrypted, *encs[idx])
	}
	return c
}

// applyEncryptedCarrier 非流式读面：encrypted 组分类回填（② tool 签名 / redacted
// 载荷）。组 id 与同消息 tool_call 线 id 配对 → 回填该 tool_use 块签名（形态判定
// reasoningToolSignature：Gemini 形态/官方占位符；不可识别/非 Gemini → 丢弃——
// 红线）；无配对（id 空或不匹配）→ Claude 族 format 返回载荷（调用方落
// redacted_thinking），别族丢弃。返回载荷按组首见顺序。
func applyEncryptedCarrier(blocks *[]ir.ContentBlock, toolCalls []toolCallWire, groups []reasoningEncryptedGroup) []string {
	if len(groups) == 0 {
		return nil
	}
	toolIDs := map[string]bool{}
	for _, tc := range toolCalls {
		if tc.ID != "" {
			toolIDs[tc.ID] = true
		}
	}
	var redacted []string
	for _, g := range groups {
		if g.ID != "" && toolIDs[g.ID] {
			if _, ok := reasoningToolSignature(g.Data); !ok {
				continue // 形态不可识别/非 Gemini 工具签名：丢弃（红线）。
			}
			for i := range *blocks {
				b := &(*blocks)[i]
				if b.Kind == ir.BlockToolUse && b.ToolCall != nil && b.ToolCall.ID == g.ID && b.Signature == "" {
					b.Signature = g.Data
				}
			}
			continue
		}
		if reasoningRedactedFormat(g.Format) {
			redacted = append(redacted, g.Data)
		}
	}
	return redacted
}
