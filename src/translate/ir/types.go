// Package ir 定义 Any2API 的中间表示（IR）——跨协议翻译的统一语义层。
//
// 六块骨架（消息 / 内容块 / 工具调用 / 流式事件 / usage / 错误）按
// research/topics/arch-options/README.md §2.3 的草案落型；硬规则引用对应 KB 主题。
//
// 设计纪律（arch-options §2.3 总原则）：
//  1. 同协议直通优先：IR 只服务跨格式腿，本包不承担字节级透传。
//  2. 未知字段/未知事件不炸：解析器「缺省=不变、未知=忽略」；IR 以 Extra/Raw
//     旁路字段保留无法建模的原始数据（arch-options 风险 2 的缓解）。
//  3. 错误默认不透传上游 body：Error.Raw 仅供诊断，渲染器绝不回传（grok2api 纪律）。
//  4. 序列化层才是协议契约：0 值有意义的字段由各转换器手写 wire 对象表达；
//     本包类型不带 wire 语义（无 struct tag）。
package ir

import "encoding/json"

// ---------------------------------------------------------------------------
// 块 1 — 消息（Message）
// ---------------------------------------------------------------------------

// Role 消息角色。规范化目标为五值（arch-options §2.3 块1：system/developer/user/
// assistant/tool）；无法归一的上游原值（如 OpenAI 已弃用的 "function"）以同类型
// 原样承载——归一策略属转换层，解析层不猜测。
type Role string

const (
	RoleSystem    Role = "system"
	RoleDeveloper Role = "developer"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// Message 消息：role + 有序内容块。消息边界不允许合并
// （new-api 合并相邻同角色消息是反面教材，arch-options §2.3 块1）。
// role=tool 约定恰含一个 tool_result 块（OpenAI 要求 tool 消息紧跟 tool_calls）。
type Message struct {
	Role   Role
	Name   string // OpenAI function/tool 消息的 name 字段；缺省为空
	Blocks []ContentBlock
	// Extra 未建模的线格式字段原样保留（保真回传；未知=不炸）。
	Extra map[string]json.RawMessage
}

// ---------------------------------------------------------------------------
// 块 2 — 内容块（ContentBlock）
// ---------------------------------------------------------------------------

// BlockKind 内容块类型。union 覆盖 arch-options §2.3 块2 的清单；
// 本 slice 完整可用的是 text / thinking / tool_use / tool_result / image / refusal / unknown。
type BlockKind string

const (
	BlockText                BlockKind = "text"
	BlockThinking            BlockKind = "thinking"          // thinking{signature?}
	BlockRedactedThinking    BlockKind = "redacted_thinking" // Anthropic 加密思考
	BlockToolUse             BlockKind = "tool_use"          // 客户端工具调用
	BlockToolResult          BlockKind = "tool_result"       // tool_result{is_error}
	BlockServerToolUse       BlockKind = "server_tool_use"   // 服务端工具（不回 tool_result）
	BlockWebSearchToolResult BlockKind = "web_search_tool_result"
	BlockImage               BlockKind = "image" // inline(base64) | file(URL)
	BlockAudio               BlockKind = "audio"
	BlockRefusal             BlockKind = "refusal"
	BlockCitations           BlockKind = "citations"
	BlockExecutableCode      BlockKind = "executable_code"
	BlockCodeExecutionResult BlockKind = "code_execution_result"
	BlockUnknown             BlockKind = "unknown" // 未建模类型：Raw 原样保真
)

// ContentBlock 内容块（块2）。以 Kind 标签的联合结构承载全部块类型；
// 未建模/未知类型进 BlockUnknown 并保留 Raw（保真旁路）。
//
// 硬规则：块类型必须先于 delta 类型判定——web_search_tool_result 块也有
// input_json_delta 但不是客户端工具调用（arch-options §2.3 块2 陷阱 3）。
type ContentBlock struct {
	Kind BlockKind

	// Text 载荷：text / thinking / refusal / executable_code / code_execution_result。
	Text string
	// Signature thinking 签名——必须原样回传（Anthropic/Gemini 双证，arch-options §2.3 块2）。
	Signature string

	ToolCall   *ToolCall    // tool_use / server_tool_use
	ToolResult *ToolResult  // tool_result / web_search_tool_result
	Image      *ImageSource // image
	Audio      *AudioSource // audio

	// Raw 未建模块或无法建模块的原始载荷（保真旁路；仅 BlockUnknown 与线格式保真使用）。
	Raw json.RawMessage
}

// ImageSource 图像块载荷。Kind=inline 时用 MediaType+Data（base64）；
// Kind=file 时用 URL（OpenAI image_url.url / 文件引用）。
type ImageSource struct {
	Kind      string // "inline" | "file"
	MediaType string // inline 时的媒体类型（如 image/png）
	Data      string // inline 时的 base64 数据
	URL       string // file 时的 URL / 文件 id
	Detail    string // OpenAI detail: low|high|auto（供应商提示，可为空）
}

// AudioSource 音频块载荷（骨架；本 slice 未用）。
type AudioSource struct {
	MediaType string
	Data      string
}

// ---------------------------------------------------------------------------
// 块 3 — 工具调用（ToolCall / ToolDef）
// ---------------------------------------------------------------------------

// ToolCall 工具调用。Arguments 为 JSON 文本（请求/非流式）或合并后的完整
// JSON（流式经组装器）；流式增量在转换器内按 index 合并（openai-wire §4.2）。
// 出口契约：空参数必须渲染为 "{}" 而不是空串（Cline/Roo 的 isParsableJson 门会
// 静默丢弃空串——arch-options §2.3 块3 / cc-wire-contract）。
type ToolCall struct {
	ID        string // 原生 toolu_…/call_… 原样；无 id 时由转换层合成（omp 归一规则）
	Name      string
	Arguments string
}

// ToolResult 工具结果（tool_result 块）。IsError 对应 Anthropic is_error；
// OpenAI 侧无该字段（解析恒 false）。
type ToolResult struct {
	CallID  string
	Content string
	IsError bool
}

// ToolDef 工具定义。InputSchema 为 JSON Schema 原文（OpenAI function.parameters /
// Anthropic input_schema），不解释、不改写；schema 归一化管道属后续 slice。
type ToolDef struct {
	Name        string
	Description string
	InputSchema json.RawMessage
	Strict      *bool // OpenAI strict 模式；nil=未声明
	// Extra 未建模的 function 级字段（保真）。
	Extra map[string]json.RawMessage
}

// ---------------------------------------------------------------------------
// 块 4 — 流式事件（StreamEvent）
// ---------------------------------------------------------------------------

// StopReason 归一终止原因，四值（omp-toolconv stop-reason 翻译表；
// arch-options §2.3 块4）。string 类型枚举：未知上游值原样透传，
// 翻译策略（未知→日志+stop）在转换层，不在解析层。
type StopReason string

const (
	StopStop    StopReason = "stop"
	StopLength  StopReason = "length"
	StopToolUse StopReason = "toolUse"
	// StopError 承载「非正常完成」语义：OpenAI content_filter / Anthropic refusal /
	// Responses failed 等（omp 表：failed/cancelled/content_filter → error）。
	StopError StopReason = "error"
)

// ErrorReason 流终态错误原因（arch-options §2.3 块4：aborted|transport；可扩展）。
// 错误两分法（omp）：aborted 仅给调用方取消；watchdog 触发的本地中止保持 error 不被误标成取消。
type ErrorReason string

const (
	ErrorAborted   ErrorReason = "aborted"   // 调用方取消
	ErrorTransport ErrorReason = "transport" // 网络/解析/截断（含无 finish_reason 的断流）
	ErrorUpstream  ErrorReason = "upstream"  // 上游流内错误帧（vLLM/官方错误帧形态）
)

// StreamEventKind 统一流事件类型。契约（arch-options §2.3 块4 / stream-mechanics §7.9）：
//
//	start → (block_start → block_delta* → block_stop)* → message_delta{stop_reason,usage} → message_stop
//	终态：done(reason: stop|length|toolUse) 或 error(reason: aborted|transport)
//
// message_delta 可多次（stop_reason 与 usage 可能分属不同 wire 帧；wire 顺序保留）。
type StreamEventKind string

const (
	EventStart        StreamEventKind = "start"
	EventBlockStart   StreamEventKind = "block_start"
	EventBlockDelta   StreamEventKind = "block_delta"
	EventBlockStop    StreamEventKind = "block_stop"
	EventMessageDelta StreamEventKind = "message_delta"
	EventMessageStop  StreamEventKind = "message_stop"
	EventDone         StreamEventKind = "done"
	EventError        StreamEventKind = "error"
)

// StreamEvent 流式事件。按 Kind 解释有效字段（其余字段为零值）：
//
//	start          : MessageID, Model, Created
//	block_start    : BlockIndex, BlockKind, ToolCallID, ToolName（tool_use 首片）
//	block_delta    : BlockIndex, BlockKind, TextDelta（text/thinking/refusal）或 ArgsDelta（tool_use 半 JSON 片段）
//	block_stop     : BlockIndex, BlockKind
//	message_delta  : StopReason 与/或 Usage（各自独立），可带 StopSequence/StopDetails 细节面
//	message_stop   : （无载荷）
//	done           : Reason
//	error          : ErrorReason, Error
//
// ServiceTier/SystemFingerprint/Moderation 与 Kind 正交（帧级元数据保真，
// 任何 Kind 上都可携带）；ThinkingTruncated/SummaryPending 同为观测面字段
// （block_start/block_stop 的 thinking 摘要态，出口不读）。
type StreamEvent struct {
	Kind StreamEventKind

	// start：消息级元数据。
	MessageID string
	Model     string
	Created   int64

	// 帧级元数据保真（chat chunk 顶层未建模三键；openai-wire §4.1 的 10 字段
	// 中除 obfuscation 外全部有落点——obfuscation **有意不承载**：§4.4 要求
	// 消费端忽略该字段，它不是语义数据，只是分片规整）。chat 解析器把出现帧上
	// 的键原样落入该帧产生的每个事件；chat 出口渲染器在对应帧回写；其余格式
	// 解析器不产、出口渲染器不读（各自白名单纪律）。
	// 边界：choices:[] 且无 usage 的纯元数据帧不产事件（无载体）——官方
	// moderation chunk 形态未在 spec/文档给例，见 W5-30 报告。
	ServiceTier       json.RawMessage
	SystemFingerprint json.RawMessage
	Moderation        json.RawMessage

	// 块定位（block_*）。
	BlockIndex int
	BlockKind  BlockKind

	// tool_use 块首片携带 id/name（openai-wire §4.2：后续片只带 index）。
	ToolCallID string
	ToolName   string

	// block_delta 载荷（按 BlockKind 解释，恰好其一非空）。
	TextDelta string // text / thinking / refusal 增量
	ArgsDelta string // tool_use 参数 JSON 片段（半 JSON，不可逐片解析）
	// SignatureDelta thinking 签名增量（Anthropic signature_delta；block 2 的
	// Signature 流式对应物——签名必须原样回传，解析层不解释其内容）。
	// redacted_thinking 的不透明载荷也经此通道承载（流式 wire 中它只出现在
	// content_block_start，渲染器预扫回填）。
	SignatureDelta string

	// message_delta。
	StopReason StopReason
	Usage      *Usage
	// StopSequence / StopDetails message_delta 细节面（保真透传；出口按各自
	// 契约映射，翻译策略不在解析层）：
	//   - StopSequence：claude.ai 网页线 `delta.stop_sequence`（值 `usage_limit`
	//     = 配额烧穿截断，W28-2/W43-3 R1——同帧 stop_reason="stop_sequence"，
	//     被归一为 StopStop）；或 Anthropic Messages 线命中自定义停止序列时的
	//     序列原文。空 = 未提供。Anthropic 出口回填 `stop_sequence` 字段并渲染
	//     `stop_reason:"stop_sequence"`；OpenAI/Responses 出口不消费（契约无
	//     对应字段，flatten 为 stop/completed，文档化）。
	//   - StopDetails：`delta.stop_details` 原文（claude.ai 网页线；W43-3 语料
	//     恒 null）。保真旁路，当前出口不消费（Anthropic message_delta 契约无
	//     该字段）。
	StopSequence string
	StopDetails  json.RawMessage

	// 观测面（claude.ai 网页线 thinking 摘要态保真；出口渲染器不读，纯观测）：
	//   - ThinkingTruncated：block_start(thinking) 的 `truncated`——true = 上游
	//     只暴露摘要而非全量思考（W43-3 G1：9/9 thinking start 恒 true）；
	//   - SummaryPending：block_stop 的 `summary_pending`——true = 摘要仍在
	//     生成（W43-3 语料恒 false）。
	// nil = wire 无该键（不把「未提供」伪装成 false）。
	ThinkingTruncated *bool
	SummaryPending    *bool

	// 终态（done / error）。
	Reason      StopReason
	ErrorReason ErrorReason
	Error       *Error
}

// ---------------------------------------------------------------------------
// 块 5 — usage（记账）
// ---------------------------------------------------------------------------

// CacheCreation Anthropic 口径的 cache 写入明细（5m/1h TTL 分档）；OpenAI 侧不用。
type CacheCreation struct {
	FiveMinute int64
	OneHour    int64
}

// Usage 统一记账（块5）。语义纪律（arch-options §2.3 块5 / stream-mechanics §7.5）：
//   - 累计覆盖非累加（Anthropic message_delta.usage 是覆盖语义，写成 += 会翻倍）；
//   - OpenAI usage 仅在 include_usage 且流未中断时出现——缺失是常态，必须容忍；
//   - Extra 保存未建模字段（含 prompt_tokens_details 等）原样，保真回传；
//     CacheRead/Reasoning 为派生值（转换消费），渲染以 Extra 原文为准；
//     当 Extra 无对应 details 且派生值非零时，渲染器合成 details（跨格式构造场景）。
type Usage struct {
	Input         int64
	Output        int64
	Total         int64
	CacheRead     int64          // OpenAI prompt_tokens_details.cached_tokens / Anthropic cache_read_input_tokens
	CacheCreation *CacheCreation // Anthropic 口径
	Reasoning     int64          // OpenAI completion_tokens_details.reasoning_tokens
	ServerTool    int64          // 服务端工具用量（骨架）
	Accounting    string         // 口径标签: "openai" | "anthropic" | "gemini"
	Estimated     bool           // 来源标签: false=wire 值 / true=估算或合成（合成值永不改写 wire 值）
	Extra         map[string]json.RawMessage
}

// ---------------------------------------------------------------------------
// 块 6 — 错误（Error）
// ---------------------------------------------------------------------------

// Error 统一错误信封 {type, code, message, param?, retryable?, source}
// （arch-options §2.3 块6）。Raw 保存上游原始 body 仅供诊断——
// 渲染器绝不透传（grok2api 纪律；sub2api sanitizeStreamError 同旨）。
type Error struct {
	Type      string // 归一类型（如 invalid_request_error）；未知时保留原值
	Code      string // 上游 code（Gemini 数值 code 以字符串承载）
	Message   string
	Param     string
	Retryable *bool  // 三态：nil=未知
	Source    string // 来源: "openai" | "anthropic" | "gemini" | "transport" | "any2api"
	Raw       json.RawMessage
}

// ---------------------------------------------------------------------------
// 组装面 — 六块之外的容器类型（请求 / 非流式响应）
// ---------------------------------------------------------------------------

// Request 跨格式请求骨架。
// 参数面（temperature/max_tokens 等）暂不提升为一等字段，经 Extra 保真透传；
// 转换对需要的参数在后续 slice 按需提升。
type Request struct {
	Model         string
	Messages      []Message
	Tools         []ToolDef
	ToolChoice    json.RawMessage // 原样（"auto"|"none"|"required" 或对象）
	Stream        bool
	StreamOptions *StreamOptions
	// Extra 未建模的顶层字段（保真透传）。
	Extra map[string]json.RawMessage
}

// StreamOptions 流式请求选项（OpenAI stream_options）。
// include_usage 语义：仅当为真时 usage chunk 才出现在流末（openai-wire §4.3）；
// include_obfuscation 默认开启（§4.4），nil=未声明。
type StreamOptions struct {
	IncludeUsage       bool
	IncludeObfuscation *bool
}

// Response 跨格式非流式响应骨架（choice 0 语义；多 choice 由解析器显式报错，
// 绝不静默丢弃）。
type Response struct {
	ID         string
	Model      string
	Created    int64
	Message    Message // assistant 消息
	StopReason StopReason
	// StopSequence 命中的停止序列原文（Anthropic 非流式 Message 的
	// `stop_sequence` 字段；空 = 未命中/未提供）。claude.ai 网页线无该字段，
	// 其配额截断（`usage_limit`）经流式 message_delta 承载、由装配面回填
	// （claude_web assembleResponse）。非空 ⇒ claude 出口渲染
	// `stop_reason:"stop_sequence"` + `stop_sequence:<值>`（流式面同款见
	// StreamEvent.StopSequence；非流式面 W52-1 对齐——AM 契约：stop_sequence
	// 非 null ⟺ stop_reason=="stop_sequence"）。
	StopSequence string
	Usage        *Usage
	Extra        map[string]json.RawMessage
}
