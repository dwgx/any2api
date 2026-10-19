// Package openai_chat 实现 OpenAI Chat Completions 线格式与 IR 的双向转换
// （parse: 线格式→IR；render: IR→线格式），覆盖请求 / 非流式响应 / 流式三面。
//
// 依据：docs/service.md（chunk 形状 §4.1、tool_calls 按 index
// 合并 §4.2、include_usage §4.3、obfuscation §4.4、错误信封 §4.7）；
// research/topics/stream-mechanics/README.md（哨兵是合成器职责 §4.6、终止原因枚举
// §4.7、无哨兵 EOF 也是正常路径）；research/topics/arch-options/README.md §2.3。
//
// 关键纪律：
//   - tool_calls 按 index 合并；同 chunk 同 index 多片段也要合并（openai-wire §4.2）。
//   - [DONE] 只在渲染流式时生成、解析流式时消费（哨兵是边缘职责）。
//   - usage 仅在 include_usage 且流未中断时出现；缺失是常态（openai-wire §4.3 NOTE）。
//   - 出口修复：空参数渲染为 "{}" 而非空串（cc-wire-contract 空参数门）。
//   - 序列化手写 wire 对象：0 值有意义的字段（finish_reason:null、delta:{}、
//     choices:[]）不靠 omitempty（arch-options §2.3 总原则 4）。
package openai_chat

import "encoding/json"

// 线格式常量。
const (
	objectChatCompletion      = "chat.completion"
	objectChatCompletionChunk = "chat.completion.chunk"
	doneSentinel              = "[DONE]"
)

// ---------------------------------------------------------------------------
// 解析侧 wire 结构（宽容读取：缺省=不变、未知=忽略；消息/工具项保留 Raw 以便
// 计算 Extra 保真旁路；流 chunk 不保留未知字段——高频瞬时对象，忽略即可）
// ---------------------------------------------------------------------------

type requestWire struct {
	Model         string             `json:"model"`
	Messages      []json.RawMessage  `json:"messages"`
	Tools         []json.RawMessage  `json:"tools"`
	ToolChoice    json.RawMessage    `json:"tool_choice"`
	Stream        bool               `json:"stream"`
	StreamOptions *streamOptionsWire `json:"stream_options"`
}

type streamOptionsWire struct {
	IncludeUsage       *bool `json:"include_usage"`
	IncludeObfuscation *bool `json:"include_obfuscation"`
}

type messageWire struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"` // string | array<part> | null
	ToolCalls  []toolCallWire  `json:"tool_calls"`
	ToolCallID string          `json:"tool_call_id"`
	Name       string          `json:"name"`
	Refusal    json.RawMessage `json:"refusal"` // string | null
	// ReasoningContent 推理扩展字段（DeepSeek/Kimi/Zhipu 等生态约定，非官方 Chat
	// message 字段；longtail-matrix §4.3 / anthropic-conversion-table §响应侧
	// 「思考」行 / gemini-conversion-table §请求侧「assistant 的 thinking」行）：
	// **请求面（assistant 回传轮）与响应面同口径**——解析为 IR 思考块（置首；
	// 无签名载体：只带文本，不造签名）。非 assistant 消息里的该字段不进块
	//（表 §7.1 安全规则），留在 Extra 保真旁路。
	// 裸 RawMessage + rawString 宽容读取：缺失/null/非字符串值都不因它断链
	//（与响应面旧路径 `reasoningContentOf` 的容忍度一致）。
	ReasoningContent json.RawMessage `json:"reasoning_content"`
	// Reasoning `reasoning` 字符串字段（OpenRouter：与 `reasoning_content` 互为
	// 别名——「functions identically to reasoning」，reasoning-tokens.md
	// §Preserving Reasoning Blocks；litellm 侧同款 delta.reasoning →
	// reasoning_content）。W42 读面兼容：reasoning_content 缺省/空时以它作文本
	// 回退（安全规则与 reasoning_content 同口径；字段本身留 Extra 保真）。
	Reasoning json.RawMessage `json:"reasoning"`
	// ReasoningDetails OpenRouter 兼容结构化推理载体（W41 起）：条目
	// {type: reasoning.text{text,signature} | reasoning.summary{summary} |
	// reasoning.encrypted{data,id}, format, index}（longtail-matrix §4.3 /
	// OpenRouter reasoning-tokens.md「Reasoning Details API Shape」）。
	// 读面：reasoning.text 的文本/签名 → IR 思考块（签名按形态校验，可识别才
	// 保留——红线）；reasoning.summary 文本 → 思考文本（text 条目的回退）；
	// reasoning.encrypted{id=某 tool_call.id} → 该 tool_use 块的签名（②Gemini 3
	// 工具签名通道，roo-code 实证）；无 tool 配对的 Claude 族 encrypted → 加密
	// 思考载荷（redacted_thinking，见 reasoning_details.go）。assistant 消息上
	// 消费该字段（渲染侧单一写者），其余角色留 Extra 保真（与 reasoning_content
	// 的安全规则同口径）。宽容读取：畸形元素跳过（reasoningDetailsOf）。
	ReasoningDetails json.RawMessage `json:"reasoning_details"`
}

type toolCallWire struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function functionWire `json:"function"`
}

type functionWire struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type toolDefWire struct {
	Type     string          `json:"type"`
	Function json.RawMessage `json:"function"`
}

type toolDefFunctionWire struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
	Strict      *bool           `json:"strict"`
}

type responseWire struct {
	ID      string            `json:"id"`
	Object  string            `json:"object"`
	Created int64             `json:"created"`
	Model   string            `json:"model"`
	Choices []json.RawMessage `json:"choices"`
	Usage   json.RawMessage   `json:"usage"`
}

type responseChoiceWire struct {
	Index        int             `json:"index"`
	Message      json.RawMessage `json:"message"`
	FinishReason *string         `json:"finish_reason"`
}

type chunkWire struct {
	ID      string            `json:"id"`
	Object  string            `json:"object"`
	Created int64             `json:"created"`
	Model   string            `json:"model"`
	Choices []chunkChoiceWire `json:"choices"`
	Usage   json.RawMessage   `json:"usage"`
	Error   json.RawMessage   `json:"error"`
	// 顶层元数据三键（openai-wire §4.1 的 10 字段中此前未建模者，W5-30）：
	// 原样落入 IR 事件（ServiceTier/SystemFingerprint/Moderation）保真回程。
	ServiceTier       json.RawMessage `json:"service_tier"`
	SystemFingerprint json.RawMessage `json:"system_fingerprint"`
	Moderation        json.RawMessage `json:"moderation"`
	// obfuscation **有意不建模**（§4.4）：分片规整用随机串、非语义数据，
	// 官方要求消费端忽略该字段——保真它既无消费方又会把随机噪声带进 IR。
}

type chunkChoiceWire struct {
	Index        int       `json:"index"`
	Delta        deltaWire `json:"delta"`
	FinishReason *string   `json:"finish_reason"`
}

type deltaWire struct {
	Role      string              `json:"role"`
	Content   *string             `json:"content"`
	Refusal   *string             `json:"refusal"`
	ToolCalls []toolCallDeltaWire `json:"tool_calls"`
	// ReasoningContent 上游推理扩展字段（DeepSeek/Kimi/Zhipu 等生态约定，
	// 非官方 Chat delta 字段；longtail-matrix §4.3 / anthropic-conversion-table
	// §流式「思考」行）：解析为 IR 思考块，出口按各自载体落地。
	ReasoningContent *string `json:"reasoning_content"`
	// Reasoning `delta.reasoning` 字符串字段（W42：OpenRouter 流式响应面文本
	// 通道；roo-code openrouter.ts 实证读它做展示，reasoning-tokens.md 别名语义）。
	// reasoning_content 缺省/空时以它作文本回退。
	Reasoning *string `json:"reasoning"`
	// ReasoningDetails OpenRouter 兼容结构化推理载体（W41；同 messageWire 注释）：
	// delta.reasoning_details 逐块到达，按 index 分组消费（文本拼接、签名取最后
	// 一条——roo-code consolidateReasoningDetails 口径）。裸 RawMessage 宽容读
	//（畸形元素跳过，不炸整帧）。
	ReasoningDetails json.RawMessage `json:"reasoning_details"`
}

type toolCallDeltaWire struct {
	Index    *int          `json:"index"`
	ID       string        `json:"id"`
	Type     string        `json:"type"`
	Function *functionWire `json:"function"`
}

// errorWire OpenAI 错误对象内层 {message, type, param, code}（openai-wire §4.7）。
type errorWire struct {
	Message string          `json:"message"`
	Type    string          `json:"type"`
	Param   json.RawMessage `json:"param"`
	Code    json.RawMessage `json:"code"`
}

// usageDetailsWire 仅用于派生 CacheRead/Reasoning（details 整体仍以原文进 Extra）。
type usageDetailsWire struct {
	CachedTokens    *int64 `json:"cached_tokens"`
	ReasoningTokens *int64 `json:"reasoning_tokens"`
}

// ---------------------------------------------------------------------------
// 渲染侧 wire 结构（显式存在性：指针/裸 JSON 控制 null 与缺省；
// 0 值有意义字段不走 omitempty）
// ---------------------------------------------------------------------------

type outChunkWire struct {
	ID      string           `json:"id"`
	Object  string           `json:"object"`
	Created int64            `json:"created"`
	Model   string           `json:"model"`
	Choices []outChunkChoice `json:"choices"`
	Usage   json.RawMessage  `json:"usage,omitempty"`
	// 帧级元数据保真（W5-30）：IR 事件携带时回写本帧；nil = 缺省（omitempty）。
	// obfuscation 有意不建模（chunkWire 注释）。
	ServiceTier       json.RawMessage `json:"service_tier,omitempty"`
	SystemFingerprint json.RawMessage `json:"system_fingerprint,omitempty"`
	Moderation        json.RawMessage `json:"moderation,omitempty"`
}

type outChunkChoice struct {
	Index        int       `json:"index"`
	Delta        *outDelta `json:"delta,omitempty"`
	FinishReason *string   `json:"finish_reason"`
}

type outDelta struct {
	Role             string             `json:"role,omitempty"`
	Content          *string            `json:"content,omitempty"`
	Refusal          *string            `json:"refusal,omitempty"`
	ToolCalls        []outToolCallDelta `json:"tool_calls,omitempty"`
	ReasoningContent *string            `json:"reasoning_content,omitempty"`
	// ReasoningDetails OpenRouter 兼容结构化推理载体（W41）：文本增量与
	// 块尾签名条目（见 stream_writer.go）。
	ReasoningDetails []outReasoningDetail `json:"reasoning_details,omitempty"`
}

// outReasoningDetail reasoning_details 条目（写面）。text/signature 各自可单独
// 出现（流式签名条目只带 signature）；encrypted 条目只带 data（+id 配对 tool_call
// / format=anthropic-claude-v1 的加密载荷）；index 恒输出（0 有意义——消费端按
// index 分组，omitempty 会把它吞掉）。
type outReasoningDetail struct {
	Type      string `json:"type"`
	Text      string `json:"text,omitempty"`
	Data      string `json:"data,omitempty"`
	Signature string `json:"signature,omitempty"`
	ID        string `json:"id,omitempty"`
	Format    string `json:"format,omitempty"`
	Index     int    `json:"index"`
}

type outToolCallDelta struct {
	Index    int          `json:"index"`
	ID       string       `json:"id,omitempty"`
	Type     string       `json:"type,omitempty"`
	Function *outFunction `json:"function,omitempty"`
}

// outFunction Arguments 不带 omitempty：0 值（空串）也是有效载荷位。
type outFunction struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments"`
}

type outResponseWire struct {
	ID      string              `json:"id"`
	Object  string              `json:"object"`
	Created int64               `json:"created"`
	Model   string              `json:"model"`
	Choices []outResponseChoice `json:"choices"`
	Usage   json.RawMessage     `json:"usage,omitempty"`
}

type outResponseChoice struct {
	Index        int             `json:"index"`
	Message      json.RawMessage `json:"message"`
	FinishReason *string         `json:"finish_reason"`
}
