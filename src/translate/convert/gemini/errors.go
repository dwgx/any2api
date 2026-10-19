package gemini

import "errors"

// 错误哨兵。全部以 fmt.Errorf("%w: …") 包装细节——显式报错，绝不静默降级/透传
//（arch-options §陷阱 1：静默透传是最贵的事故）。

// 解析侧。
var (
	// ErrBadRequest 请求体不符合 Gemini generateContent 线格式。
	ErrBadRequest = errors.New("gemini: malformed request")
	// ErrBadResponse 非流式响应体不符合线格式。
	ErrBadResponse = errors.New("gemini: malformed response")
	// ErrBadChunk 流式帧不符合线格式。
	ErrBadChunk = errors.New("gemini: malformed stream chunk")
	// ErrUnsupportedMultiCandidate candidates[] 多候选不被本转换器建模——显式报错，
	// 绝不静默只取第一个（与 openai_chat 的 ErrUnsupportedMultiChoice 对称）。
	ErrUnsupportedMultiCandidate = errors.New("gemini: multiple candidates are not supported")
	// ErrToolResultShape functionResponse/消息形态不匹配。
	ErrToolResultShape = errors.New("gemini: functionResponse/message shape mismatch")
	// ErrUnsupportedFunctionCallShape functionCall 既不是整对象也不是可物化的
	// partialArgs 序列（两者混用等）——显式报错，不当空参执行。
	ErrUnsupportedFunctionCallShape = errors.New("gemini: unsupported functionCall shape")
	// ErrBadPartialArgsPath partialArgs.jsonPath 超出支持子集
	//（.member / [n] / ['k']；通配/过滤/根路径非法）。
	ErrBadPartialArgsPath = errors.New("gemini: unsupported partialArgs jsonPath")
	// ErrPartialArgsLimit partialArgs 数组下标超过 4095（NA 同款上限）。
	ErrPartialArgsLimit = errors.New("gemini: partialArgs array index out of range")
)

// 渲染侧。
var (
	// ErrMissingStart 事件序列必须以 start 开头。
	ErrMissingStart = errors.New("gemini: stream events must begin with a start event")
	// ErrEventsAfterTerminal done/error 之后仍有事件。
	ErrEventsAfterTerminal = errors.New("gemini: event after terminal done/error")
	// ErrMissingStopReason 流收尾缺 finishReason——不合成默认值掩盖截断
	//（与 claude_messages 的 CC B7 硬门对称）。
	ErrMissingStopReason = errors.New("gemini: stream ended without finishReason")
	// ErrUnsupportedBlock 该块类型无法用 generateContent 线格式表达。
	ErrUnsupportedBlock = errors.New("gemini: block kind not representable in gemini_generate")
	// ErrBadIR IR 自身不完整/自相矛盾（如 tool_use 块缺 ToolCall）。
	ErrBadIR = errors.New("gemini: malformed IR")
	// ErrInvalidToolArgs tool_use 参数不是合法 JSON 对象（functionCall.args 必须是
	// 对象；NA 同款显式报错，绝不静默塞原始字符串）。
	ErrInvalidToolArgs = errors.New("gemini: tool_use arguments are not a valid JSON object")
	// ErrStreamCandidateCount stream=true 与 candidateCount>1 互斥（GW 陷阱 7）——
	// 上游必 400，本层显式拦截，不静默降 1。
	ErrStreamCandidateCount = errors.New("gemini: stream with candidateCount>1 is not supported upstream")
	// ErrImageWithoutInlineData 图像块既无内联数据也无文件 URI。
	ErrImageWithoutInlineData = errors.New("gemini: image block has neither inline data nor file URI")
)
