package openai_chat

import "errors"

// 错误哨兵。全部以 fmt.Errorf("%w: …") 包装细节——显式报错，绝不静默降级/透传
//（arch-options §陷阱 1：静默透传是最贵的事故）。

// 解析侧。
var (
	// ErrBadRequest 请求体不符合 OpenAI Chat 线格式。
	ErrBadRequest = errors.New("openai_chat: malformed request")
	// ErrBadResponse 非流式响应体不符合线格式。
	ErrBadResponse = errors.New("openai_chat: malformed response")
	// ErrBadChunk 流式 chunk 不符合线格式。
	ErrBadChunk = errors.New("openai_chat: malformed stream chunk")
	// ErrStreamOptionsWithoutStream stream_options 只能与 stream=true 同用
	//（openai-wire 陷阱：vLLM 显式校验拒绝）。
	ErrStreamOptionsWithoutStream = errors.New("openai_chat: stream_options requires stream=true")
	// ErrUnexpectedObject object 字段与预期类型不符。
	ErrUnexpectedObject = errors.New("openai_chat: unexpected object type")
	// ErrUnexpectedRole 角色与预期不符（响应消息非 assistant 等）。
	ErrUnexpectedRole = errors.New("openai_chat: unexpected message role")
	// ErrUnsupportedMultiChoice n>1 的多 choice 不被本转换器建模——显式报错，
	// 绝不静默只取第一个（数据丢失比报错更贵）。
	ErrUnsupportedMultiChoice = errors.New("openai_chat: multiple choices are not supported")
	// ErrUnsupportedToolType 非 function 类型的工具（custom 等）不在本 slice 范围。
	ErrUnsupportedToolType = errors.New("openai_chat: unsupported tool type")
	// ErrToolResultShape tool_result 与消息形态不匹配（tool 消息必须恰含一个
	// tool_result 块且带 tool_call_id）。
	ErrToolResultShape = errors.New("openai_chat: tool_result/message shape mismatch")
	// ErrDataAfterDone 哨兵之后出现数据帧（openai-wire quirk 1：哨兵后不应再有数据）。
	ErrDataAfterDone = errors.New("openai_chat: data frame after [DONE] sentinel")
	// ErrDataAfterError 流内错误帧之后出现数据帧（[DONE] 例外——vLLM 形态容忍）。
	ErrDataAfterError = errors.New("openai_chat: data frame after stream error")
)

// 渲染侧。
var (
	// ErrMissingStart 事件序列必须以 start 开头。
	ErrMissingStart = errors.New("openai_chat: stream events must begin with a start event")
	// ErrEventsAfterTerminal done/error 之后仍有事件。
	ErrEventsAfterTerminal = errors.New("openai_chat: event after terminal done/error")
	// ErrUnsupportedBlock 该块类型无法用 OpenAI Chat 表达（thinking 等）。
	ErrUnsupportedBlock = errors.New("openai_chat: block kind not representable in openai_chat")
	// ErrBadIR IR 自身不完整/自相矛盾（如 tool_use 块缺 ToolCall）。
	ErrBadIR = errors.New("openai_chat: malformed IR")
	// ErrToolErrorFlag tool_result.is_error 在 OpenAI Chat 无对应表示——
	// 转换对必须先把错误语义编码进 content（本转换器不静默吞掉错误标记）。
	ErrToolErrorFlag = errors.New("openai_chat: tool_result is_error has no openai_chat representation")
)
