package service

import (
	"bytes"
	"encoding/json"

	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// usage 观测与回收（诊断面）：预提交回收（W5-15）+ per-attempt 记账（W5-18）
// ---------------------------------------------------------------------------
//
// 背景（P27 §发现 6 的移植建议）：LiteLLM 在流中途 fallback 时用
// `stream_chunk_builder` 从已收 chunk 回收失败流的 usage，并把两段 usage 合并进
// fallback 的首个 chunk（`router.py:3104-3119`、`_combine_fallback_usage`
// :2981-2997）。我们的对应面 = **预提交失败换上游**：失败尝试可能已经收到携带
// usage 的帧（如 gemini 的 usageMetadata-only 帧、claude 的 message_start）。
//
// 本 slice 的落地边界（为什么**不**做客户端可见的合并）：
//
//  1. 合并要求改写下游字节：同协议透传面是字节保真（W5-12 锚点），转换面需要在
//     出口 writer 的 usage 帧上注入偏移——pairs 管道没有该钩子，且本 slice
//     禁改 `src/translate/`（Non-Conflict）；
//  2. 失败尝试的 usage 常是**部分值**（claude message_start 只有 input_tokens，
//     output 在流尾的 message_delta；gemini 快照未必含末帧），合并出的是下界而
//     不是真值——把下界当真值上报是另一类错误；
//  3. 我们自己的实测/研究把「混源 usage」当记账陷阱：contract-exec ③（回退路径
//     usage 混源 → 计量要按请求分段）；litellm §陷阱（合并 = 客户端看到两段之和）。
//
// 故回收面 = **诊断计数 + 日志**（`Diagnostics.PartialUsage*`）：失败尝试的用量
// 可观测、可累计、可告警，但不进入下游可见流。
//
// W5-18 条目 4 把探针从「仅预提交窗口」扩到 **per-attempt 全窗口**：
//
//   - 观测（`observePartialUsage`）在每次尝试的每一帧上运行（提交后也观测；
//     非流式尝试把整段响应体当一帧），收尾 `finishAttempt`（attempts.go）记一次
//     → `Diagnostics.AttemptUsage*` + 一行日志 + per-request 记录（W5-19 条目 4）；
//   - `Diagnostics.PartialUsage*`（W5-15 回收面）保留原语义：其中「预提交失败
//     且已收 usage」是 AttemptUsage* 失败子集里更窄的一个面；
//   - 两条面都是纯观测，绝不进下游可见流（同上面的理由）。
//
// 观测窗口（W5-15 回收面）= 提交前（尚未向下游写任何字节）；per-attempt 记账面
// （W5-18）= 整个尝试。提交后失败不换上游、不回收，但**会**计入 per-attempt 面
// （失败尝试用量在记账上是真实发生过的上游消耗）。

// partialUsage 一次尝试里已观测到的 token 用量（W5-18 起 = 全窗口：提交前 +
// 提交后）。
type partialUsage struct {
	input  int64
	output int64
}

// maxProbeBytes 探针的帧大小上限：usage 对象恒小且近顶层；超过该值的帧（媒体
// 增量等）不做二次 JSON 解析（预提交窗口内可能反复出现巨型不产出帧）。
const maxProbeBytes = 1 << 20

func (u *partialUsage) zero() bool { return u.input == 0 && u.output == 0 }

// merge 并入一组数字：取 max（各协议 usage 单调或快照语义；预提交观测只求
// 「已产生多少」的下界，不追求精确重建）。
func (u *partialUsage) merge(in, out int64) {
	if in > u.input {
		u.input = in
	}
	if out > u.output {
		u.output = out
	}
}

// usageNumbers 各协议 usage 字段并集（只读观测；每个协议只用其中一组命名——
// max 合并因此无歧义）。短名容错仅读取（D2：官方 OpenAPI 只有 total_* 一套，
// 短名只出现在文档例子/参考实现里；渲染面不认）。
type usageNumbers struct {
	InputTokens          *int64 `json:"input_tokens"`
	PromptTokens         *int64 `json:"prompt_tokens"`
	OutputTokens         *int64 `json:"output_tokens"`
	CompletionTokens     *int64 `json:"completion_tokens"`
	TotalInputTokens     *int64 `json:"total_input_tokens"`
	TotalOutputTokens    *int64 `json:"total_output_tokens"`
	PromptTokenCount     *int64 `json:"promptTokenCount"`
	CandidatesTokenCount *int64 `json:"candidatesTokenCount"`
}

// pair 取输入/输出候选的最大值（各协议只用其中一组命名）。
func (u *usageNumbers) pair() (int64, int64) {
	return maxInt64(derefInt64(u.InputTokens), derefInt64(u.PromptTokens),
			derefInt64(u.TotalInputTokens), derefInt64(u.PromptTokenCount)),
		maxInt64(derefInt64(u.OutputTokens), derefInt64(u.CompletionTokens),
			derefInt64(u.TotalOutputTokens), derefInt64(u.CandidatesTokenCount))
}

func derefInt64(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func maxInt64(vs ...int64) int64 {
	var m int64
	for _, v := range vs {
		if v > m {
			m = v
		}
	}
	return m
}

// observePartialUsage 从一帧**原始上游 SSE 数据**里抽取 usage 数字（只读观测，
// 绝不改动流；调用方在**每次尝试的全窗口**喂入——W5-18 条目 4 把窗口从「仅预提交」
// 扩到全窗口；非流式面整段响应体当一帧喂）。按上游协议解释帧载荷：
//
//   - claude：`message_start` 的 message.usage / `message_delta` 的 usage；
//   - openai_chat：chunk.usage（独立 usage chunk 或与 finish 同 chunk）；
//   - gemini：usageMetadata（累计快照——merge-non-zero 语义的只读近似：取 max）；
//   - responses：response.usage（response.completed）或顶层 usage；
//   - interactions：interaction.usage / 顶层 usage / step.stop 的 step_usage。
//
// 解析失败或无 usage 字段 = 无观测（静默——本函数纯观测，绝不进控制流）。
func observePartialUsage(proto registry.Format, data []byte, acc *partialUsage) {
	if len(data) > maxProbeBytes {
		return // 巨型帧（媒体增量等）不做二次解析；usage 对象恒小且近顶层。
	}
	// 廉价前置守卫：五协议的 usage 载体字段名都含 `usage`（usage / usageMetadata /
	// step_usage）——不含该子串的帧（逐 token 增量是绝大多数）不做 JSON 解析。
	// 全窗口观测（W5-18）让本探针跑在每一帧上，这个守卫就是它的开销上限。
	if !bytes.Contains(data, []byte("usage")) {
		return
	}
	switch proto {
	case registry.FormatClaudeMessages:
		var w struct {
			Message *struct {
				Usage *usageNumbers `json:"usage"`
			} `json:"message"`
			Usage *usageNumbers `json:"usage"`
		}
		if json.Unmarshal(data, &w) != nil {
			return
		}
		if w.Message != nil && w.Message.Usage != nil {
			acc.merge(w.Message.Usage.pair())
		}
		if w.Usage != nil {
			acc.merge(w.Usage.pair())
		}
	case registry.FormatOpenAIChat:
		var w struct {
			Usage *usageNumbers `json:"usage"`
		}
		if json.Unmarshal(data, &w) != nil || w.Usage == nil {
			return
		}
		acc.merge(w.Usage.pair())
	case registry.FormatGeminiGenerate:
		var w struct {
			UsageMetadata *usageNumbers `json:"usageMetadata"`
		}
		if json.Unmarshal(data, &w) != nil || w.UsageMetadata == nil {
			return
		}
		acc.merge(w.UsageMetadata.pair())
	case registry.FormatOpenAIResponses:
		var w struct {
			Response *struct {
				Usage *usageNumbers `json:"usage"`
			} `json:"response"`
			Usage *usageNumbers `json:"usage"`
		}
		if json.Unmarshal(data, &w) != nil {
			return
		}
		if w.Response != nil && w.Response.Usage != nil {
			acc.merge(w.Response.Usage.pair())
		}
		if w.Usage != nil {
			acc.merge(w.Usage.pair())
		}
	case registry.FormatInteractions:
		var w struct {
			Interaction *struct {
				Usage *usageNumbers `json:"usage"`
			} `json:"interaction"`
			Usage     *usageNumbers `json:"usage"`
			StepUsage *usageNumbers `json:"step_usage"`
		}
		if json.Unmarshal(data, &w) != nil {
			return
		}
		if w.Interaction != nil && w.Interaction.Usage != nil {
			acc.merge(w.Interaction.Usage.pair())
		}
		if w.Usage != nil {
			acc.merge(w.Usage.pair())
		}
		if w.StepUsage != nil {
			acc.merge(w.StepUsage.pair())
		}
	}
}

// recoverPartialUsage 预提交失败换上游前：把失败尝试已收帧里观测到的 usage 记入
// W5-15 的回收面（计数）。绝不并入下游可见流（理由见本文件头注）；日志行由
// finishAttempt 统一输出（每次尝试一行——W5-18 条目 4 的合并，避免一次失败
// 尝试两行日志）。
func (s *Server) recoverPartialUsage(u partialUsage) {
	if u.zero() {
		return
	}
	s.diag.partialUsageAttempts.Add(1)
	s.diag.partialUsageIn.Add(u.input)
	s.diag.partialUsageOut.Add(u.output)
}
