package claude_messages

import "errors"

// 错误哨兵。全部以 fmt.Errorf("%w: …") 包装细节——显式报错，绝不静默降级/透传
//（arch-options §陷阱 1：静默透传是最贵的事故）。

// 解析侧。
var (
	// ErrBadRequest 请求体不符合 Anthropic Messages 线格式。
	ErrBadRequest = errors.New("claude_messages: malformed request")
	// ErrBadResponse 非流式响应体不符合线格式。
	ErrBadResponse = errors.New("claude_messages: malformed response")
	// ErrBadEvent 流事件帧不符合线格式。
	ErrBadEvent = errors.New("claude_messages: malformed stream event")
	// ErrUnexpectedEventOrder 事件顺序违反骨架（message_start 重复 / 先于 start 的
	// block 事件等——CC B4：顺序错客户端直接抛异常）。
	ErrUnexpectedEventOrder = errors.New("claude_messages: unexpected event order")
	// ErrUnknownBlockIndex delta/stop 指向未 start 的块（CC B5：客户端
	// RangeError('Content block not found') 的同位守卫）。
	ErrUnknownBlockIndex = errors.New("claude_messages: event for unknown content block index")
	// ErrDataAfterTerminal message_stop / error 之后出现数据帧。
	ErrDataAfterTerminal = errors.New("claude_messages: data frame after message_stop/error")
	// ErrUnexpectedRole 角色与预期不符（messages[] 只允许 user/assistant）。
	ErrUnexpectedRole = errors.New("claude_messages: unexpected message role")
	// ErrToolResultShape tool_result 与消息形态不匹配（assistant 消息带 tool_result、
	// 非 text 部件的结果数组等）。
	ErrToolResultShape = errors.New("claude_messages: tool_result/message shape mismatch")
)

// 渲染侧。
var (
	// ErrMissingStart 事件序列必须以 start 开头（CC B4：message_start 必须最先）。
	ErrMissingStart = errors.New("claude_messages: stream events must begin with a start event")
	// ErrEventsAfterTerminal done/error 之后仍有事件。
	ErrEventsAfterTerminal = errors.New("claude_messages: event after terminal done/error")
	// ErrMissingStopReason 流收尾缺 stop_reason（CC B7 硬门：message_delta 必须带
	// stop_reason，缺失即判流失败——不合成默认值掩盖）。
	ErrMissingStopReason = errors.New("claude_messages: stream ended without stop_reason")
	// ErrMissingMessageStop 事件序列缺 message_stop 收尾（CC B8：帧序最后必须
	// message_stop；不静默补帧掩盖截断）。
	ErrMissingMessageStop = errors.New("claude_messages: event stream ended without message_stop")
	// ErrUnsupportedBlock 该块类型无法用 Messages 线格式表达。
	ErrUnsupportedBlock = errors.New("claude_messages: block kind not representable in claude_messages")
	// ErrBadIR IR 自身不完整/自相矛盾（如 tool_use 块缺 ToolCall）。
	ErrBadIR = errors.New("claude_messages: malformed IR")
	// ErrInvalidToolInput tool_use 参数不是合法 JSON 对象（Messages input 必须是
	// 对象；修不好不静默——映射表未决 1 的产品决策：报错而非 {}）。
	ErrInvalidToolInput = errors.New("claude_messages: tool_use input is not a valid JSON object")
	// ErrUnsupportedToolChoice tool_choice 形状不在双向映射表内。
	ErrUnsupportedToolChoice = errors.New("claude_messages: unsupported tool_choice shape")
)
