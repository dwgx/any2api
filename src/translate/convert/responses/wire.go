// Package responses 实现 OpenAI Responses API（POST /v1/responses，items 制）线格式
// 与 IR 的双向转换（parse: 线格式→IR；render: IR→线格式），覆盖请求 / 非流式响应 /
// 流式三面。
//
// 依据：research/topics/openai-responses/README.md（wire 定稿 + 三张转换表 +
// quirks；本地官方文档副本 `tmp/lanes/L01-openai-protocols/raw/responses-*.md`）。
//
// 关键纪律（表内「不照抄」项一律按表，不按任一参考实现）：
//   - **事件族顺序**（官方示例 [C:16468-16501]）：created → in_progress →
//     output_item.added → content_part.added → delta* → output_text.done →
//     content_part.done → output_item.done → completed（usage 在 completed 的
//     response.usage 里，恒有）。出口逐字复现该序。
//   - **sequence_number**：官方从 0 逐事件递增；new-api 合成时省略、CPA 逐事件分配。
//     本实现的显式选择——**合成端逐事件分配（从 0 起）**（表 §4.1「合成端不能把它
//     当可选」），**解析端容忍缺失**（消费端不能把它当必填）。测试锁死两侧行为。
//   - **usage 只在终止事件**：`response.completed/incomplete` 的 `response.usage`；
//     解析端对任何携带 response 对象的事件做 merge-non-zero（NA `applyResponseMetadata`），
//     渲染端只写进终止事件。
//   - **items vs messages 形状**：tools 扁平（`{type:function,name,…}`）、tool_choice
//     `{"type":"function","name":…}`、结构化输出在 `text.format`——三处与 chat 全不同，
//     是最容易「看起来对」的静默错位点，本包逐处显式映射。
//   - **快照回退**：`response.completed.response.output` 是全量快照；流中漏发的项在
//     终态从快照补发（NA `terminalOutputChunks`）。解析端实现；渲染端不做。
//   - **redacted_thinking 前缀**：Claude redacted_thinking ↔ Responses
//     `reasoning.encrypted_content`，载体前缀 `claude-redacted-thinking:`（CPA
//     `claude_openai-responses_response.go:114-127`）；无前缀的 encrypted_content 走
//     thinking.signature。
//   - **stateful 字段硬报错**：previous_response_id / conversation / prompt /
//     context_management / background / moderation → ErrStatefulFields（表 1 行 +
//     请求面表行；绝不静默丢）。
//   - **无 `[DONE]` 哨兵**：官方以终止事件收束；解析端容忍 `[DONE]`（new-api 扫描器
//     同款容忍），渲染端**绝不**发哨兵。`response.done` 是非官方别名，与 completed 同权。
//   - **序列化手写 wire 对象**：0 值有意义的字段（status、output、usage、sequence_number）
//     不靠 omitempty（arch-options §2.3 总原则 4）。
package responses

import "encoding/json"

// 线格式常量。
const (
	objectResponse = "response"

	statusInProgress = "in_progress"
	statusCompleted  = "completed"
	statusIncomplete = "incomplete"
	statusFailed     = "failed"

	itemTypeMessage       = "message"
	itemTypeReasoning     = "reasoning"
	itemTypeFunctionCall  = "function_call"
	itemTypeCustomTool    = "custom_tool_call"
	itemTypeFuncOutput    = "function_call_output"
	itemTypeCustomOutput  = "custom_tool_call_output"
	itemTypeAdditionalTls = "additional_tools"

	partTypeInputText  = "input_text"
	partTypeInputImage = "input_image"
	partTypeInputFile  = "input_file"
	partTypeOutputText = "output_text"
	partTypeRefusal    = "refusal"
	partTypeSummary    = "summary_text"
	partTypeReasoning  = "reasoning_text"

	roleAssistant = "assistant"

	// redactedThinkingPrefix Claude redacted_thinking 在 Responses
	// `reasoning.encrypted_content` 里的载体前缀（CPA 同款；表 3「推理签名载体」行）。
	redactedThinkingPrefix = "claude-redacted-thinking:"
)

// 流式事件类型（[S] 事件族；本包建模转换所需子集，其余解析端忽略）。
const (
	evCreated              = "response.created"
	evInProgress           = "response.in_progress"
	evOutputItemAdded      = "response.output_item.added"
	evOutputItemDone       = "response.output_item.done"
	evContentPartAdded     = "response.content_part.added"
	evContentPartDone      = "response.content_part.done"
	evOutputTextDelta      = "response.output_text.delta"
	evOutputTextDone       = "response.output_text.done"
	evRefusalDelta         = "response.refusal.delta"
	evRefusalDone          = "response.refusal.done"
	evFunctionArgsDelta    = "response.function_call_arguments.delta"
	evFunctionArgsDone     = "response.function_call_arguments.done"
	evCustomInputDelta     = "response.custom_tool_call_input.delta"
	evCustomInputDone      = "response.custom_tool_call_input.done"
	evReasoningPartAdded   = "response.reasoning_summary_part.added"
	evReasoningPartDone    = "response.reasoning_summary_part.done"
	evReasoningTextDelta   = "response.reasoning_summary_text.delta"
	evReasoningTextDone    = "response.reasoning_summary_text.done"
	evReasoningRawDelta    = "response.reasoning_text.delta"
	evReasoningRawDone     = "response.reasoning_text.done"
	evCompleted            = "response.completed"
	evIncomplete           = "response.incomplete"
	evFailed               = "response.failed"
	evDoneAlias            = "response.done" // 非官方别名（new-api 同权容忍）
	evError                = "error"
	evOutputTextAnnotation = "response.output_text.annotation.added"
)

// ---------------------------------------------------------------------------
// 解析侧 wire 结构（宽容读取：缺省=不变、未知=忽略；请求/响应保留 Raw 以便计算
// Extra 保真旁路；流帧高频瞬时，未知字段忽略即可）
// ---------------------------------------------------------------------------

// requestWire POST /v1/responses 请求体（[C:15-6126]；建模转换相关字段，
// 状态化/托管字段建模以便**显式拒绝**而非静默丢）。
type requestWire struct {
	Model         string            `json:"model"`
	Input         json.RawMessage   `json:"input"`
	Instructions  json.RawMessage   `json:"instructions"`
	Tools         []json.RawMessage `json:"tools"`
	ToolChoice    json.RawMessage   `json:"tool_choice"`
	Temperature   json.RawMessage   `json:"temperature"`
	TopP          json.RawMessage   `json:"top_p"`
	TopLogprobs   json.RawMessage   `json:"top_logprobs"`
	Stream        bool              `json:"stream"`
	StreamOptions *struct {
		IncludeObfuscation *bool `json:"include_obfuscation"`
	} `json:"stream_options"`
	Store                json.RawMessage `json:"store"`
	PreviousResponseID   string          `json:"previous_response_id"`
	Conversation         json.RawMessage `json:"conversation"`
	Prompt               json.RawMessage `json:"prompt"`
	ContextManagement    json.RawMessage `json:"context_management"`
	Background           json.RawMessage `json:"background"`
	Moderation           json.RawMessage `json:"moderation"`
	Include              json.RawMessage `json:"include"`
	Truncation           json.RawMessage `json:"truncation"`
	Reasoning            json.RawMessage `json:"reasoning"`
	Text                 json.RawMessage `json:"text"`
	MaxOutputTokens      json.RawMessage `json:"max_output_tokens"`
	MaxToolCalls         json.RawMessage `json:"max_tool_calls"`
	ParallelToolCalls    json.RawMessage `json:"parallel_tool_calls"`
	Metadata             json.RawMessage `json:"metadata"`
	User                 json.RawMessage `json:"user"`
	SafetyIdentifier     json.RawMessage `json:"safety_identifier"`
	PromptCacheKey       json.RawMessage `json:"prompt_cache_key"`
	PromptCacheRetention json.RawMessage `json:"prompt_cache_retention"`
	ServiceTier          json.RawMessage `json:"service_tier"`
	FrequencyPenalty     json.RawMessage `json:"frequency_penalty"`
	PresencePenalty      json.RawMessage `json:"presence_penalty"`
}

// inputItemWire input[] 项联合类型（字段并集，按 type 存在性解释；[C:93-130]）。
type inputItemWire struct {
	Type             string            `json:"type"`
	ID               string            `json:"id"`
	Role             string            `json:"role"`
	Content          json.RawMessage   `json:"content"`
	CallID           string            `json:"call_id"`
	Name             string            `json:"name"`
	Arguments        json.RawMessage   `json:"arguments"`
	Input            json.RawMessage   `json:"input"`
	Output           json.RawMessage   `json:"output"`
	Summary          []json.RawMessage `json:"summary"`
	EncryptedContent string            `json:"encrypted_content"`
	Tools            []json.RawMessage `json:"tools"` // additional_tools（CPA quirk）
	Status           string            `json:"status"`
}

// partWire 内容部件联合类型（input_text / input_image / input_file /
// output_text / refusal / summary_text / reasoning_text）。
type partWire struct {
	Type       string            `json:"type"`
	Text       string            `json:"text"`
	Refusal    string            `json:"refusal"`
	ImageURL   string            `json:"image_url"`
	FileID     string            `json:"file_id"`
	FileData   string            `json:"file_data"`
	FileURL    string            `json:"file_url"`
	Filename   string            `json:"filename"`
	Detail     string            `json:"detail"`
	MediaType  string            `json:"media_type"`
	Annotation []json.RawMessage `json:"annotations"`
}

// functionToolWire tools[] 的 function 形态（**扁平**：name 在顶层，[C:5205]）。
type functionToolWire struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
	Strict      *bool           `json:"strict"`
}

// responseWire 非流式 Response 对象（[C:6129-15795]；建模转换相关字段）。
type responseWire struct {
	ID                string            `json:"id"`
	Object            string            `json:"object"`
	CreatedAt         int64             `json:"created_at"`
	Status            string            `json:"status"`
	Model             string            `json:"model"`
	Output            []json.RawMessage `json:"output"`
	Usage             json.RawMessage   `json:"usage"`
	IncompleteDetails *struct {
		Reason string `json:"reason"`
	} `json:"incomplete_details"`
	Error json.RawMessage `json:"error"`
}

// outputItemWire Response.output[] 项联合类型（message / reasoning /
// function_call / custom_tool_call / hosted 工具项）。
type outputItemWire struct {
	Type             string            `json:"type"`
	ID               string            `json:"id"`
	Status           string            `json:"status"`
	Role             string            `json:"role"`
	Content          []json.RawMessage `json:"content"`
	Summary          []json.RawMessage `json:"summary"`
	CallID           string            `json:"call_id"`
	Name             string            `json:"name"`
	Arguments        json.RawMessage   `json:"arguments"`
	Input            json.RawMessage   `json:"input"`
	EncryptedContent string            `json:"encrypted_content"`
}

// usageWire Response.usage（[C:15750-15788]；details 原文进 Extra 保真）。
type usageWire struct {
	InputTokens         *int64          `json:"input_tokens"`
	OutputTokens        *int64          `json:"output_tokens"`
	TotalTokens         *int64          `json:"total_tokens"`
	InputTokensDetails  json.RawMessage `json:"input_tokens_details"`
	OutputTokensDetails json.RawMessage `json:"output_tokens_details"`
}

// eventWire 流式事件载荷（59 事件族的公共字段并集；[S]）。
type eventWire struct {
	Type           string          `json:"type"`
	SequenceNumber *int64          `json:"sequence_number"`
	Response       json.RawMessage `json:"response"`
	OutputIndex    *int            `json:"output_index"`
	ItemID         string          `json:"item_id"`
	ContentIndex   *int            `json:"content_index"`
	Item           json.RawMessage `json:"item"`
	Part           json.RawMessage `json:"part"`
	Delta          string          `json:"delta"`
	Text           string          `json:"text"`
	Arguments      string          `json:"arguments"`
	Input          string          `json:"input"`
	Refusal        string          `json:"refusal"`
	SummaryIndex   *int            `json:"summary_index"`
	Code           string          `json:"code"`
	Message        string          `json:"message"`
	Param          string          `json:"param"`
}

// errorObjectWire 流内 error 事件的平铺字段（**无嵌套 error 对象**，[S:69014]）。
type errorObjectWire struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Param   string `json:"param"`
}

// ---------------------------------------------------------------------------
// 内部工具（各转换器包独立实现；与 openai_chat/gemini 同语义）
// ---------------------------------------------------------------------------

func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		// 仅内部常量/已校验值，Marshal 失败 = 编程错误。
		panic(err)
	}
	return b
}

func isJSONNull(raw json.RawMessage) bool {
	return len(raw) == 4 && string(raw) == "null"
}

func hasRaw(raw json.RawMessage) bool {
	return len(raw) > 0 && !isJSONNull(raw)
}

// rawString 宽松取字符串字段（null/缺省 → ""；非字符串 → ""）。
func rawString(raw json.RawMessage) string {
	if !hasRaw(raw) {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s
}

// extrasOf 从原始对象提取未建模顶层字段（保真旁路）。
func extrasOf(raw json.RawMessage, known ...string) map[string]json.RawMessage {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	skip := make(map[string]bool, len(known))
	for _, k := range known {
		skip[k] = true
	}
	var out map[string]json.RawMessage
	for k, v := range m {
		if skip[k] {
			continue
		}
		if out == nil {
			out = map[string]json.RawMessage{}
		}
		out[k] = v
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
