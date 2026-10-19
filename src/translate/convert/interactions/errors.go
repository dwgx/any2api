package interactions

import "errors"

// 错误哨兵。全部以 fmt.Errorf("%w: …") 包装细节——显式报错，绝不静默降级/透传
//（arch-options §陷阱 1：静默透传是最贵的事故）。

// 解析侧。
var (
	// ErrBadRequest 请求体不符合 Interactions 线格式。
	ErrBadRequest = errors.New("interactions: malformed request")
	// ErrBadResponse 非流式响应体不符合线格式。
	ErrBadResponse = errors.New("interactions: malformed response")
	// ErrBadChunk 流式事件帧不符合线格式。
	ErrBadChunk = errors.New("interactions: malformed stream event")
	// ErrUnexpectedObject object 字段不是 "interaction"。
	ErrUnexpectedObject = errors.New("interactions: unexpected object type")
	// ErrStatefulFields 请求携带状态化/平台级字段（previous_interaction_id /
	// agent / agent_config / environment / environment_id / background /
	// webhook_config / continuation_token / cached_content / store:true）——
	// 无状态上游无法兑现，按 responses 先例**显式报错**而非静默丢。
	ErrStatefulFields = errors.New("interactions: stateful fields are not supported in cross-format conversion")
	// ErrEventTypeMismatch `event:` 行与 payload 的 event_type 不一致
	//（Q6 加固：以 event: 行为准、event_type 为校验，不静默按 data 分派）。
	ErrEventTypeMismatch = errors.New("interactions: sse event name does not match payload event_type")
	// ErrUnsupportedToolChoice tool_choice 形状不在映射表内。
	ErrUnsupportedToolChoice = errors.New("interactions: unsupported tool_choice shape")
	// ErrToolResultShape function_result / tool 消息形态不匹配。
	ErrToolResultShape = errors.New("interactions: function_result/message shape mismatch")
	// ErrMissingStepType 步对象缺 type 判别式。
	ErrMissingStepType = errors.New("interactions: step without type")
	// ErrMissingFunctionID function_call 步既无 id 也无（CPA 错键）call_id。
	ErrMissingFunctionID = errors.New("interactions: function_call step without id")
	// ErrMixedRevisions 请求体同时携带 legacy（response_mime_type /
	// response_modalities）与新 schema（response_format）字段——D5：按客户端
	// 形态择一，绝不混发。
	ErrMixedRevisions = errors.New("interactions: legacy and new-schema fields mixed in one request")
)

// 渲染侧。
var (
	// ErrMissingStart 事件序列必须以 start 开头。
	ErrMissingStart = errors.New("interactions: stream events must begin with a start event")
	// ErrEventsAfterTerminal done/error 之后仍有事件。
	ErrEventsAfterTerminal = errors.New("interactions: event after terminal done/error")
	// ErrMissingStopReason 流收尾缺 stop_reason——不合成默认值掩盖截断
	//（与 claude_messages CC B7 / gemini / responses 硬门对称）。
	ErrMissingStopReason = errors.New("interactions: stream ended without stop reason")
	// ErrUnsupportedBlock 该块类型无法用 Interactions 线格式表达。
	ErrUnsupportedBlock = errors.New("interactions: block kind not representable in interactions")
	// ErrBadIR IR 自身不完整/自相矛盾（如 tool_use 块缺 ToolCall）。
	ErrBadIR = errors.New("interactions: malformed IR")
	// ErrImageWithoutPayload 图像块既无内联数据也无 uri。
	ErrImageWithoutPayload = errors.New("interactions: image block has neither uri nor inline data")
	// ErrUnsupportedToolArgs tool_use 参数不是合法 JSON 对象（非流式 function_call
	// 步的 `arguments` 是 JSON 对象，对象才能安全回放；空参由渲染层补 "{}"）。
	ErrUnsupportedToolArgs = errors.New("interactions: tool_use arguments are not a valid JSON object")
	// ErrNilError RenderError 收到 nil 错误。
	ErrNilError = errors.New("interactions: nil error")
)
