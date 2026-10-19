// Package hygiene 出口卫生层（A2A-L19-W5-26）：合成 id 净化/唯一化 + 工具结果
// 配对文案的统一政策。三件（依据 `research/topics/id-and-abort/README.md`，P38P36）：
//
//  1. SanitizeToolID —— 透传 id 的客户端硬约束净化。字符集 ^[a-zA-Z0-9_-]+$
//     （Anthropic tool_use.id；roo-code `src/utils/tool-id.ts:9-13`）+ 长度
//     ≤64（Responses `call_id` 上限；roo 的 `truncateOpenAiCallId` 复现真实
//     报错「69 chars」）。超长截断保留前缀 + sha256 后缀（CPA
//     `shortenCodexReplayCallIDIfNeeded` 同款形态）。**幂等**——净化后的值再
//     净化不变：客户端把 id 原样回传（历史）时配对键不被二次改写。
//
//  2. SynthDerivedID / SynthIndexedID / SynthUniqueID —— 合成 id（上游/客户端
//     缺失 id 时），统一 `<base>_synth_<…>` 形态（`_synth` 标记保留可 grep 可
//     统计，与上游原生 id 可区分），按实体分派：
//
//     - **响应 id 派生**（`<base>_synth_<32hex>`，W5-29）：工具调用 id 与
//     Responses 输出项 id——seed = 响应 id + 块/槽序号。跨响应唯一（客户端
//     transcript/历史配对键），且确定性（渲染不动点 + 录帧逐字节回放都靠它；
//     响应 id 经 parse 往返）。无响应 id 可派生的出口（请求面 / 上游不给响应
//     id）回落 `<base>_synth_<n>`（块序形态；旧的 `toolu_%d`（组装器）与
//     `toolu_synth_%d`（写出层）双形态即 W5-26 的收口对象）。
//
//     - **每调用唯一**（`<base>_synth_<32hex>`，crypto/rand）：响应 id
//     （`resp` / `int`）。旧的常量 `resp_synth` / `int_synth` 跨响应同 id——客户端
//     transcript/日志无法区分响应（new-api 派生范式 `resp_<uuid32hex>`，
//     `relaykit/relayconvert/response_registry.go:899-903`）；响应 id 经 parse
//     往返，故随机化不破坏逐字节回放。内容寻址（CPA `cpa_gemini_` 面）对响应 id
//     不适用：同一请求的不同响应必须不同 id。
//
//  3. InterruptedResultText / FoldOrphanToolResultText —— 历史里工具结果配对
//     修复的两条统一文案（CPA `repairClaudeToolPairing` 范式，
//     `internal/translator/claude/openai/responses/claude_openai-responses_request.go:1076-1200`）：
//     缺配对结果 → 合成 `is_error:true` + 可读文案（不是空串）；孤儿结果 →
//     折叠为普通文本（绝不作为 tool_result 出口，也绝不静默丢弃）。
//
// 边界（有意）：净化只作用于**我们写出的 id**（出口腿）。同协议直通不经 IR、
// 不介入；客户端/上游请求里的 id 原样转发（上游自己的约束不是我们改写配对键
// 的理由）。
package hygiene

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// MaxToolIDLen 客户端硬约束的长度上限（Responses `call_id` ≤64 字符）。
const MaxToolIDLen = 64

// nonIDChar 合法字符集之外的字符（Anthropic tool_use.id：^[a-zA-Z0-9_-]+$）。
var nonIDChar = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

// SanitizeToolID 按客户端硬约束净化一个透传 id：非法字符 → '_'；超长（>64）
// 截断为「前 47 字符 + '_' + sha256 前 8 字节 hex」（共 64 字符，CPA 形态）。
// 空串原样返回（缺失 id 的合成由调用方按出口形态决定）。
//
// 幂等：合法字符集 + 长度 ≤64 的值再过一遍不变——客户端把 id 原样回传时
// （历史/结果配对）不会被二次改写。
func SanitizeToolID(id string) string {
	if id == "" {
		return ""
	}
	s := nonIDChar.ReplaceAllString(id, "_")
	if len(s) <= MaxToolIDLen {
		return s
	}
	sum := sha256.Sum256([]byte(s))
	suffix := "_" + hex.EncodeToString(sum[:8]) // 17 字符
	return s[:MaxToolIDLen-len(suffix)] + suffix
}

// WireToolID 出口工具调用 id 的统一形态：先净化（SanitizeToolID），空值再合成
// （SynthDerivedID(base, seed, n)——seed = 响应 id，n = 块/槽序号）。所有出口写
// id 的点都必须过这里：同一调用在同一出口形态下得到的都是同一个 wire id
// （配对键稳定）。
//
// seed 为空（无响应身份可派生；如请求面/上游不给响应 id）→ 退回块序形态
// （SynthIndexedID；确定性保留，跨响应重复的残余风险见报告）。
func WireToolID(base, id, seed string, n int) string {
	if s := SanitizeToolID(id); s != "" {
		return s
	}
	return SynthDerivedID(base, seed, n)
}

// SynthDerivedID 响应 id 派生合成 id：`<base>_synth_<32hex>`，32hex =
// sha256(seed ␀ n)[:16 字节] 的 hex（␀ = "\x00" 分隔符，防拼接歧义；seed =
// 响应 id）。**跨响应唯一**：不同响应的响应 id 不同 → 同槽位序号不撞 id——
// 客户端把 id 写进历史/transcript，跨响应同 id 会让 tool_use↔tool_result 配对
// 与日志关联失效（上游「tool_use ids must be unique」400 面）。
//
// 确定性：同 (seed, n) 恒同值——渲染不动点（golden re-render 逐字节相等）与
// 录帧逐字节回放（P11 fixture）都靠它；响应 id 经 parse 往返（响应面 id /
// 消息面 id 都从 wire 读回），故派生值在回放中逐字节可复现。
//
// seed 为空 → 退回 SynthIndexedID（无响应身份可派生；残余风险：跨响应重复，
// 见报告）。
func SynthDerivedID(base, seed string, n int) string {
	if seed == "" {
		return SynthIndexedID(base, n)
	}
	sum := sha256.Sum256([]byte(seed + "\x00" + strconv.Itoa(n)))
	return base + "_synth_" + hex.EncodeToString(sum[:16])
}

// SynthIndexedID 块序合成 id：`<base>_synth_<n>`（n = 块/槽序号）。
// 现状用途：**无响应 id 可派生的出口**（seed 为空的回落路径）。确定性是硬
// 需求（渲染不动点 + 录帧回放）；跨响应重复的边界见报告「残余风险」。
func SynthIndexedID(base string, n int) string {
	return fmt.Sprintf("%s_synth_%d", base, n)
}

// SynthUniqueID 生成 `<base>_synth_<32hex>` 形态的**每调用唯一**合成 id
// （crypto/rand 128 bit）。用于响应 id（`resp`/`int`）：客户端把它写进
// transcript/日志，常量会让跨响应关联失效；且响应 id 经 parse 往返（replay
// 逐字节回放不受影响）。
func SynthUniqueID(base string) string {
	var b [16]byte
	// crypto/rand.Read 自 Go 1.24 起保证成功（失败即进程级灾难，不做假回退）。
	if _, err := rand.Read(b[:]); err != nil {
		panic("hygiene: crypto/rand unavailable: " + err.Error())
	}
	return base + "_synth_" + hex.EncodeToString(b[:])
}

// InterruptedResultText 缺配对结果的合成文案（CPA 逐字同款）：发给模型看，
// 显式标注「调用被中断、没有输出」——比空串更可读，且与 CPA 行为可对拍。
const InterruptedResultText = "Tool call was interrupted before any output was recorded."

// EmptyResultText 孤儿结果内容为空时的显式占位（CPA 同款）：折叠后的 text 块
// 绝不为空串（Anthropic 拒收空 text 块）。
const EmptyResultText = "Tool result was empty."

// FoldOrphanToolResultText 孤儿工具结果 → 普通文本（CPA
// `normalizeClaudeToolResultMessage` 范式）：内容原样保留；空白内容 → 显式占位
// EmptyResultText（绝不产出空 text 块）。
func FoldOrphanToolResultText(content string) string {
	if strings.TrimSpace(content) == "" {
		return EmptyResultText
	}
	return content
}
