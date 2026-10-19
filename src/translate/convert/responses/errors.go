package responses

import "errors"

// 错误哨兵。全部以 fmt.Errorf("%w: …") 包装细节——显式报错，绝不静默降级/透传
//（arch-options §陷阱 1：静默透传是最贵的事故）。

// 解析侧。
var (
	// ErrBadRequest 请求体不符合 Responses 线格式。
	ErrBadRequest = errors.New("responses: malformed request")
	// ErrBadResponse 非流式响应体不符合线格式。
	ErrBadResponse = errors.New("responses: malformed response")
	// ErrBadChunk 流式事件帧不符合线格式。
	ErrBadChunk = errors.New("responses: malformed stream event")
	// ErrUnexpectedObject object 字段不是 "response"。
	ErrUnexpectedObject = errors.New("responses: unexpected object type")
	// ErrStatefulFields 请求携带状态化/托管字段（previous_response_id /
	// conversation / prompt / context_management / background / moderation）——
	// 转 chat/claude 时服务端状态无法延续，按表**显式报错**而非静默丢
	//（openai-responses 表 1「状态化字段」行 + 请求面表「状态化/托管字段」行；
	// new-api `validateResponsesRequestChatUnsupportedFields` 同旨）。
	ErrStatefulFields = errors.New("responses: stateful fields are not supported in cross-format conversion")
	// ErrUnsupportedToolType tools[] 非 function 类型（hosted 工具 17 变体无 chat/
	// claude 对应）——显式报错，绝不静默丢工具声明（表 1 只映射 function 形态）。
	ErrUnsupportedToolType = errors.New("responses: unsupported tool type")
	// ErrUnsupportedInputItem input[] 项类型无法表达为 IR（item_reference 等
	// 服务端引用、hosted 工具产物）。
	ErrUnsupportedInputItem = errors.New("responses: unsupported input item")
	// ErrUnsupportedToolChoice tool_choice 形状不在映射表内（allowed_tools / mcp）。
	ErrUnsupportedToolChoice = errors.New("responses: unsupported tool_choice shape")
	// ErrToolResultShape function_call_output / tool 消息形态不匹配。
	ErrToolResultShape = errors.New("responses: tool output/message shape mismatch")
	// ErrInvalidReasoning reasoning item 的 summary/content 既非文本也无加密载荷。
	ErrInvalidReasoning = errors.New("responses: reasoning item has no text and no encrypted content")
)

// 渲染侧。
var (
	// ErrMissingStart 事件序列必须以 start 开头。
	ErrMissingStart = errors.New("responses: stream events must begin with a start event")
	// ErrEventsAfterTerminal done/error 之后仍有事件。
	ErrEventsAfterTerminal = errors.New("responses: event after terminal done/error")
	// ErrMissingStopReason 流收尾缺 stop_reason——不合成默认值掩盖截断
	//（与 claude_messages CC B7 / gemini 硬门对称）。
	ErrMissingStopReason = errors.New("responses: stream ended without stop reason")
	// ErrUnsupportedBlock 该块类型无法用 Responses 线格式表达。
	ErrUnsupportedBlock = errors.New("responses: block kind not representable in responses")
	// ErrBadIR IR 自身不完整/自相矛盾（如 tool_use 块缺 ToolCall）。
	ErrBadIR = errors.New("responses: malformed IR")
	// ErrImageWithoutPayload 图像块既无 URL 也无内联数据。
	ErrImageWithoutPayload = errors.New("responses: image block has neither url nor inline data")
	// ErrUnsupportedToolArgs tool_use 参数不是合法 JSON 对象（Responses 的
	// function_call.arguments 是 JSON 文本，对象才能安全回放；空参由渲染层补 "{}"）。
	ErrUnsupportedToolArgs = errors.New("responses: tool_use arguments are not a valid JSON object")
)
