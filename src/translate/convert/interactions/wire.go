// Package interactions 实现 Gemini Interactions API（POST /v1beta/interactions，
// steps 时间线制）线格式与 IR 的双向转换（parse: 线格式→IR；render: IR→线格式），
// 覆盖请求 / 非流式响应 / 流式三面。
//
// 依据：docs/architecture.md（wire 定稿 + 转换表 A/B +
// quirk 清单；官方副本 `tmp/lanes/P10-interactions/raw/`）。表内 do-not-copy 项
// 一律按表、不按任一参考实现（CPA）：
//
//   - **steps 时间线**：响应不是 message 而是 step 数组。建模子集 = user_input /
//     model_output / thought / function_call / function_result（OAS:Step 21 种里的
//     5 种客户端步）；其余 16 种服务端工具步（google_search_call 等 8 对）以
//     BlockUnknown + Raw 保真旁路——绝不静默丢，也绝不发明字段。
//   - **流式三拍**：step.start → step.delta* → step.stop + 交互级生命周期事件
//     （interaction.created / interaction.status_update / interaction.completed），
//     以 `event: done` + `data: [DONE]` 收束（OAS `x-speakeasy-sse-sentinel`）。
//   - **arguments_delta 必须累积**（BC:413-419、ST:196-200）：与 generateContent
//     「一帧一个完整 functionCall」相反；本包按 IR 的 ArgsDelta 片段语义下发，
//     累积/修复由出口组装层（toolcall 出口修复）收口。
//   - **function_call 用 `id`、function_result 用 `call_id`**（Q1：CPA 把 Gemini 的
//     functionCall.id 写进 interactions 的 `call_id` 是 do-not-copy）。渲染永远用
//     `id`（OAS:FunctionCallStep.required）；读取时容忍 CPA 错键（`call_id` 回退）。
//   - **Api-Revision 版本纪律**：wire 层容忍/透传、不发明——legacy（response_mime_type /
//     response_modalities / image_config）与新 schema（response_format）在同一 OpenAPI
//     并存（D5），本包按客户端形态择一原样过桥、**绝不混发**（同时出现 → 显式报错）。
//     header 本身不属 body，由服务壳按 `HeaderAPIRevision` 透传/默认。
//   - **stateful 面显式拒绝**（照 responses 先例）：previous_interaction_id / agent /
//     agent_config / environment / environment_id / background / webhook_config /
//     continuation_token / cached_content 与 `store:true` → ErrStatefulFields
//     （静默丢 = 语义改变；`store:false` 无服务端状态期望，放行）。
//   - **事件名两套说法**（D1）：解析侧两种都吃（status_update / in_progress /
//     requires_action），发送侧跟 OpenAPI（status_update）。
//   - **event: 行与 payload 的 event_type 必须一致**（Q6 加固）：解析以 `event:` 行为准、
//     `event_type` 为校验，不一致 → ErrEventTypeMismatch（不静默按 data 分派）。
package interactions

import "encoding/json"

// 线格式常量。
const (
	// HeaderAPIRevision 版本开关头（BC:903-914；wire 层容忍/透传，不发明）。
	HeaderAPIRevision = "Api-Revision"
	// RevisionNewSchema 新 schema 显式 opt-in 值（2026-05-26 起为默认）。
	RevisionNewSchema = "2026-05-20"
	// RevisionLegacy 临时退回 legacy 的值（2026-06-08 起该头被忽略）。
	RevisionLegacy = "2026-05-07"

	// objectInteraction 资源类型（OAS:InteractionSseEventInteraction.object）。
	objectInteraction = "interaction"

	// 步类型（OAS:Step oneOf；本包建模 5 种客户端步）。
	stepUserInput      = "user_input"
	stepModelOutput    = "model_output"
	stepThought        = "thought"
	stepFunctionCall   = "function_call"
	stepFunctionResult = "function_result"

	// 内容部件类型（OAS:Content oneOf；媒体类型由顶层判别式给出）。
	contentText     = "text"
	contentImage    = "image"
	contentAudio    = "audio"
	contentVideo    = "video"
	contentDocument = "document"

	// 流式 delta 判别式（OAS:StepDeltaData.oneOf 26 种；本包建模 4 种）。
	deltaText             = "text"
	deltaThoughtSummary   = "thought_summary"
	deltaThoughtSignature = "thought_signature"
	deltaArguments        = "arguments_delta"
	deltaImage            = "image"
	deltaAudio            = "audio"
	deltaVideo            = "video"
	deltaDocument         = "document"

	// 事件类型（ST §Event types）。
	evInteractionCreated   = "interaction.created"
	evInteractionStatus    = "interaction.status_update"
	evInteractionCompleted = "interaction.completed"
	evStepStart            = "step.start"
	evStepDelta            = "step.delta"
	evStepStop             = "step.stop"
	evError                = "error"
	evDone                 = "done"

	// 状态更新别名（D1：BC:405-411 说 in_progress/requires_action 取代
	// status_update；ST/OAS 仍用 status_update——解析两种都吃，发送跟 OAS）。
	evInteractionInProgress = "interaction.in_progress"
	evInteractionRequires   = "interaction.requires_action"

	// 非 Gemini 上游的失败别名（Q7：容忍读取，绝不主动生成）。
	evInteractionFailed = "interaction.failed"
	evResponseFailed    = "response.failed"

	// doneSentinel 流终态哨兵（`event: done` + `data: [DONE]`）。
	doneSentinel = "[DONE]"

	// 交互状态枚举（OAS:Interaction.status 8 值）。
	statusInProgress     = "in_progress"
	statusRequiresAction = "requires_action"
	statusCompleted      = "completed"
	statusFailed         = "failed"
	statusCancelled      = "cancelled"
	statusIncomplete     = "incomplete"
	statusBudgetExceeded = "budget_exceeded"
	statusQueued         = "queued"

	// 工具选择枚举（OAS:AllowedTools.mode）。
	toolChoiceAuto      = "auto"
	toolChoiceAny       = "any"
	toolChoiceNone      = "none"
	toolChoiceValidated = "validated"

	// thinking 档位（OAS:ThinkingLevel；**没有 none/auto**——Q2/Q3 do-not-copy）。
	thinkingMinimal = "minimal"
	thinkingLow     = "low"
	thinkingMedium  = "medium"
	thinkingHigh    = "high"
)

// ---------------------------------------------------------------------------
// 解析侧 wire 结构（宽容读取：缺省=不变、未知=忽略；请求/响应保留 Raw 以便计算
// Extra 保真旁路；流帧高频瞬时，未知字段忽略即可）
// ---------------------------------------------------------------------------

// requestWire POST /v1beta/interactions 请求体（OAS:ModelInteraction / Interaction；
// 建模转换相关字段 + 状态化/平台级字段以便**显式拒绝**而非静默丢）。
type requestWire struct {
	Model               string            `json:"model"`
	Agent               json.RawMessage   `json:"agent"`
	Input               json.RawMessage   `json:"input"`
	SystemInstruction   string            `json:"system_instruction"`
	Tools               []json.RawMessage `json:"tools"`
	GenerationConfig    json.RawMessage   `json:"generation_config"`
	Stream              *bool             `json:"stream"`
	Store               *bool             `json:"store"`
	PreviousInteraction string            `json:"previous_interaction_id"`
	ResponseFormat      json.RawMessage   `json:"response_format"`
	ResponseMimeType    string            `json:"response_mime_type"`
	ResponseModalities  json.RawMessage   `json:"response_modalities"`
	SafetySettings      json.RawMessage   `json:"safety_settings"`
	ServiceTier         string            `json:"service_tier"`
	Background          *bool             `json:"background"`
	Environment         json.RawMessage   `json:"environment"`
	EnvironmentID       string            `json:"environment_id"`
	AgentConfig         json.RawMessage   `json:"agent_config"`
	WebhookConfig       json.RawMessage   `json:"webhook_config"`
	ContinuationToken   string            `json:"continuation_token"`
	CachedContent       string            `json:"cached_content"`
	Labels              json.RawMessage   `json:"labels"`
}

// stepWire 步联合类型（字段并集，按 type 解释；OAS:Step.oneOf 21 种）。
type stepWire struct {
	Type      string            `json:"type"`
	ID        string            `json:"id"`      // function_call（必填）
	CallID    string            `json:"call_id"` // function_result；读取容忍 function_call 上的 CPA 错键
	Name      string            `json:"name"`
	Arguments json.RawMessage   `json:"arguments"` // function_call：JSON 对象
	Result    json.RawMessage   `json:"result"`    // function_result：string | object | Content[]
	IsError   *bool             `json:"is_error"`
	Content   []json.RawMessage `json:"content"` // user_input / model_output
	Summary   []json.RawMessage `json:"summary"` // thought：TextContent | ImageContent
	Signature string            `json:"signature"`
}

// contentWire 内容部件联合类型（OAS:Content.oneOf：text/image/audio/video/document）。
type contentWire struct {
	Type        string          `json:"type"`
	Text        string          `json:"text"`
	MimeType    string          `json:"mime_type"`
	Data        string          `json:"data"`
	URI         string          `json:"uri"`
	Resolution  string          `json:"resolution"`
	Annotations json.RawMessage `json:"annotations"`
}

// interactionWire 非流式 Interaction 资源（OAS:Interaction / ModelInteraction）。
type interactionWire struct {
	ID            string            `json:"id"`
	Object        string            `json:"object"`
	Status        string            `json:"status"`
	Model         string            `json:"model"`
	Created       string            `json:"created"`
	Updated       string            `json:"updated"`
	Steps         []json.RawMessage `json:"steps"`
	Usage         json.RawMessage   `json:"usage"`
	Errors        json.RawMessage   `json:"errors"`
	FinishReason  string            `json:"finish_reason"` // 非 OAS 字段（CPA/文档例子），容忍读取
	ServiceTier   string            `json:"service_tier"`
	EnvironmentID string            `json:"environment_id"`
}

// eventWire 流式事件载荷（公共字段并集；event_type 与 `event:` 行冗余，解析校验）。
type eventWire struct {
	EventType     string          `json:"event_type"`
	EventID       string          `json:"event_id"`
	Interaction   json.RawMessage `json:"interaction"`
	InteractionID string          `json:"interaction_id"`
	Status        string          `json:"status"`
	Index         *int            `json:"index"`
	Step          json.RawMessage `json:"step"`
	Delta         json.RawMessage `json:"delta"`
	Usage         json.RawMessage `json:"usage"`
	StepUsage     json.RawMessage `json:"step_usage"`
	Error         json.RawMessage `json:"error"`
}

// usageWire Usage（OAS:Usage；主计数 + 模态分解原文进 Extra 保真）。
type usageWire struct {
	TotalInputTokens        int64           `json:"total_input_tokens"`
	TotalOutputTokens       int64           `json:"total_output_tokens"`
	TotalTokens             int64           `json:"total_tokens"`
	TotalCachedTokens       int64           `json:"total_cached_tokens"`
	TotalThoughtTokens      int64           `json:"total_thought_tokens"`
	TotalToolUseTokens      int64           `json:"total_tool_use_tokens"`
	InputTokensByModality   json.RawMessage `json:"input_tokens_by_modality"`
	OutputTokensByModality  json.RawMessage `json:"output_tokens_by_modality"`
	CachedTokensByModality  json.RawMessage `json:"cached_tokens_by_modality"`
	ToolUseTokensByModality json.RawMessage `json:"tool_use_tokens_by_modality"`
	GroundingToolCount      json.RawMessage `json:"grounding_tool_count"`
}

// ---------------------------------------------------------------------------
// 内部工具（各转换器包独立实现；与 openai_chat/gemini/responses 同语义）
// ---------------------------------------------------------------------------

func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
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

// rawTypeOf 取原始对象/数组元素的 type 字段（畸形 → ""）。
func rawTypeOf(raw json.RawMessage) string {
	var w struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return ""
	}
	return w.Type
}

// extrasOf 从原始对象提取未建模顶层字段（保真旁路）。
func extrasOf(raw json.RawMessage, known ...string) map[string]json.RawMessage {
	if !hasRaw(raw) {
		return nil
	}
	var all map[string]json.RawMessage
	if err := json.Unmarshal(raw, &all); err != nil {
		return nil
	}
	skip := make(map[string]bool, len(known))
	for _, k := range known {
		skip[k] = true
	}
	out := map[string]json.RawMessage{}
	for k, v := range all {
		if !skip[k] && hasRaw(v) {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// itemsOf 把原始 JSON 数组解成 []json.RawMessage（非数组 → nil）。
func itemsOf(raw json.RawMessage) []json.RawMessage {
	if !hasRaw(raw) {
		return nil
	}
	var out []json.RawMessage
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
