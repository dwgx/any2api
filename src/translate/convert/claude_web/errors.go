package claude_web

import "errors"

// 错误哨兵。全部以 fmt.Errorf("%w: …") 包装细节——显式报错，绝不静默降级/透传
//（arch-options §陷阱 1：静默透传是最贵的事故）。

// 解析侧。
var (
	// ErrBadEvent 流事件帧不符合线格式（JSON 解析失败）。
	ErrBadEvent = errors.New("claude_web: malformed stream event")
	// ErrEventNameMismatch `event:` 行与 data.type 不一致（双源核；不静默按一方分派）。
	ErrEventNameMismatch = errors.New("claude_web: sse event name does not match payload type")
	// ErrUnexpectedEventOrder 事件顺序违反骨架（重复 message_start / 先于
	// message_start 的块事件等）。
	ErrUnexpectedEventOrder = errors.New("claude_web: unexpected event order")
	// ErrUnknownBlockIndex delta/stop 指向未 start 的块。
	ErrUnknownBlockIndex = errors.New("claude_web: event for unknown content block index")
	// ErrDataAfterTerminal message_stop / error 之后出现数据帧。
	ErrDataAfterTerminal = errors.New("claude_web: data frame after message_stop/error")
	// ErrBadErrorEnvelope 错误信封既无 `error` 对象也无顶层 message。
	ErrBadErrorEnvelope = errors.New("claude_web: malformed error envelope")
)

// 渲染侧。
var (
	// ErrUnsupportedFace 该转换面在 claude_web 不存在（上游叶子格式：无入口面、
	// 无出口渲染面）——显式报错，绝不返回半成品字节。
	ErrUnsupportedFace = errors.New("claude_web: face not supported (upstream leaf format)")
	// ErrBadIR IR 自身不完整/自相矛盾（如请求缺 model）。
	ErrBadIR = errors.New("claude_web: malformed IR")
	// ErrUnsupportedBlock 该块类型无法用 claude.ai 体表达（音频/其余块——音频
	// 面无载体，见 render.go 头注）。
	ErrUnsupportedBlock = errors.New("claude_web: block kind not representable in the claude.ai body")
	// ErrAttachmentCarrier 图像块无可用载体（既无 inline base64 也无 URL）——
	// 显式报错，绝不静默丢（brief ③「无载体面保持显式报错」）。
	ErrAttachmentCarrier = errors.New("claude_web: attachment block without a usable carrier")
	// ErrAttachmentTooLarge 附件超出 MaxAttachmentBytes（KB 裁定 = BK 20MB 原值）。
	ErrAttachmentTooLarge = errors.New("claude_web: attachment exceeds size limit")
	// ErrAttachmentRef 上游 file_uuid 无法安全回填（长度/字符不符——原地等长
	// 回填的机械约束，见 attach.go 头注）。
	ErrAttachmentRef = errors.New("claude_web: upstream file reference cannot be patched in place")
	// ErrNilError RenderError 收到 nil 错误。
	ErrNilError = errors.New("claude_web: nil error")
)
