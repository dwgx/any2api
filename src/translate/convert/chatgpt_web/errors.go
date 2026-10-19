package chatgpt_web

import "errors"

// 错误哨兵。全部以 fmt.Errorf("%w: …") 包装细节——显式报错，绝不静默降级/透传
//（arch-options §陷阱 1：静默透传是最贵的事故）。

var (
	// ErrBadEvent 载荷声明为 JSON 对象但不符合读取面（防御面；DecodePayload
	// 已先验 JSON 合法性，此错只在结构性异常时触发）。
	ErrBadEvent = errors.New("chatgpt_web: malformed stream event")
	// ErrDataAfterTerminal 终态（任一结束判据命中 / 流内 error 帧）之后出现
	// **内容帧**。重复的结束标记（[DONE] / message_stream_complete / 状态补丁）
	// 与元数据帧是正常收束序列，容忍；内容帧出现 = 上游在结束信号后仍在产出
	//（早终结风险的显式暴露——绝不静默截断）。
	ErrDataAfterTerminal = errors.New("chatgpt_web: content frame after terminal signal")
	// ErrUnsupportedContent 内容 part 无可用载体（既非字符串、又无
	// asset_pointer/文本字段的 part；或指针 scheme 不在已知表内）——显式报错，
	// 绝不静默丢（claude_web ErrAttachmentCarrier 同旨；brief §Change ④
	//「无载体面显式报错」）。
	ErrUnsupportedContent = errors.New("chatgpt_web: content part without a usable carrier")
	// ErrUnsupportedFace 该转换面在 chatgpt_web 不存在（上游叶子格式：无入口面、
	// 无下游出口面）——显式报错，绝不返回半成品字节（claude_web ErrUnsupportedFace
	// 同款；W30-2 接线：parseRequest 与三个出口面）。
	ErrUnsupportedFace = errors.New("chatgpt_web: face not supported (upstream leaf format)")
	// ErrUnsupportedBlock 请求渲染面：IR 内容块在 chatgpt 匿名体不可表达
	// （image/audio 等无载体块；tool 块走标签文本面，见 render.go）。显式报错，
	// 绝不静默丢块（claude_web ErrUnsupportedBlock 同旨）。
	ErrUnsupportedBlock = errors.New("chatgpt_web: content block not supported by the anon request wire")
	// ErrBadIR IR 面不完整/不可装配（渲染面：nil 请求 / 零消息 / tool 块无
	// ToolCall 载荷；装配面：空流 / 截断流 / 未知块类型）——显式报错，绝不静默
	// 降级（claude_web ErrBadIR 同旨）。
	ErrBadIR = errors.New("chatgpt_web: bad IR payload")
)
