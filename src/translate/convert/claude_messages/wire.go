// Package claude_messages 实现 Anthropic Messages v1（POST /v1/messages）线格式与
// IR 的双向转换（parse: 线格式→IR；render: IR→线格式），覆盖请求 / 非流式响应 /
// 流式命名事件三面。
//
// 依据：research/topics/anthropic-messages/README.md（SSE 事件全表、delta 子类型、
// usage 累计覆盖、stop_reason 七值、error.type 全表）；research/topics/
// anthropic-conversion-table/README.md（§4 请求表 / §5 响应表 / §6 流式四向与分片
// 决策 / §6.4 input_json_delta 分片 / §6.5 usage 收尾 / §7 签名与错误信封）；
// research/topics/cc-wire-contract/README.md（B1-B8 出口硬门：真 SSE、event: 行、
// 块序、类型配对、start 内内容被丢弃、message_delta 带 stop_reason、message_stop 收尾）。
//
// 关键纪律：
//   - 帧语法固定 `event: <name>\ndata: <json>\n\n`；事件名只用白名单（B2）。
//   - message_start 必须最先；message_stop 必须收尾；出口绝不发 [DONE]（AM 陷阱 5）。
//   - 块类型先于 delta 类型判定（web_search_tool_result 也有 input_json_delta 但不是
//     客户端工具调用——AM 陷阱 3）。
//   - content_block_start 内联内容被客户端丢弃（B6）：工具参数一律走
//     input_json_delta；空参数出口发 "{}"。
//   - usage 累计覆盖非累加；message_start 出现而 delta 省略的字段保留原值（CC B7）。
//   - thinking 签名必须原样回传（无签名可承载时整块丢弃属转换对职责，本转换器不造签名）。
//   - 错误默认不透传上游 body（Error.Raw 仅供诊断，渲染器绝不回传）。
//   - 序列化手写 wire 对象：0 值有意义的字段（stop_reason:null、content:[]、
//     input:{}）不靠 omitempty（arch-options §2.3 总原则 4）。
package claude_messages

import "encoding/json"

// 线格式常量。
const (
	objectMessage = "message"

	evMessageStart      = "message_start"
	evContentBlockStart = "content_block_start"
	evContentBlockDelta = "content_block_delta"
	evContentBlockStop  = "content_block_stop"
	evMessageDelta      = "message_delta"
	evMessageStop       = "message_stop"
	evPing              = "ping"
	evError             = "error"

	blockTypeText            = "text"
	blockTypeThinking        = "thinking"
	blockTypeRedactedThink   = "redacted_thinking"
	blockTypeToolUse         = "tool_use"
	blockTypeToolResult      = "tool_result"
	blockTypeServerToolUse   = "server_tool_use"
	blockTypeWebSearchResult = "web_search_tool_result"
	blockTypeImage           = "image"

	deltaTypeText      = "text_delta"
	deltaTypeThinking  = "thinking_delta"
	deltaTypeSignature = "signature_delta"
	deltaTypeInputJSON = "input_json_delta"
	deltaTypeCitations = "citations_delta"
)

// defaultMaxTokens max_tokens 缺省合成值（Messages API 必填）。
// 来源：anthropic-conversion-table §4「输出上限」行（CPA 用 32000；默认值合成
// 是「请求被改写」的事实——日志点在服务壳层，库层只做确定性合成）。
const defaultMaxTokens = 32000

// ---------------------------------------------------------------------------
// 解析侧 wire 结构（宽容读取：缺省=不变、未知=忽略；消息/工具项保留 Raw 以便
// 计算 Extra 保真旁路；流事件高频瞬时，未知字段忽略即可）
// ---------------------------------------------------------------------------

type requestWire struct {
	Model         string            `json:"model"`
	MaxTokens     json.RawMessage   `json:"max_tokens"`
	Messages      []json.RawMessage `json:"messages"`
	System        json.RawMessage   `json:"system"` // string | []block
	Tools         []json.RawMessage `json:"tools"`
	ToolChoice    json.RawMessage   `json:"tool_choice"`
	Stream        bool              `json:"stream"`
	StopSequences json.RawMessage   `json:"stop_sequences"`
	Temperature   json.RawMessage   `json:"temperature"`
	TopP          json.RawMessage   `json:"top_p"`
}

type messageWire struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"` // string | []block | null
}

// contentBlockWire 一个内容块（请求与响应共用）。字段并集，按 Type 解释。
type contentBlockWire struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	Thinking  string          `json:"thinking"`
	Signature string          `json:"signature"`
	Data      string          `json:"data"` // redacted_thinking 的不透明载荷
	ID        string          `json:"id"`   // tool_use / server_tool_use
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	ToolUseID string          `json:"tool_use_id"` // tool_result / web_search_tool_result
	Content   json.RawMessage `json:"content"`     // tool_result 结果载荷
	IsError   bool            `json:"is_error"`
	Source    json.RawMessage `json:"source"` // image 的 source 对象
}

type imageSourceWire struct {
	Type      string `json:"type"` // base64 | url
	MediaType string `json:"media_type"`
	Data      string `json:"data"`
	URL       string `json:"url"`
}

type toolDefWire struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
	Strict      *bool           `json:"strict"`
}

type responseWire struct {
	ID           string            `json:"id"`
	Type         string            `json:"type"`
	Role         string            `json:"role"`
	Model        string            `json:"model"`
	Content      []json.RawMessage `json:"content"`
	StopReason   *string           `json:"stop_reason"`
	StopSequence json.RawMessage   `json:"stop_sequence"`
	Usage        json.RawMessage   `json:"usage"`
}

// streamEventWire 流事件信封（event: 名与 data.type 一致；两者都可用来定位事件名）。
type streamEventWire struct {
	Type         string          `json:"type"`
	Message      json.RawMessage `json:"message"`       // message_start
	Index        *int            `json:"index"`         // content_block_*
	ContentBlock json.RawMessage `json:"content_block"` // content_block_start
	Delta        json.RawMessage `json:"delta"`         // content_block_delta / message_delta
	Usage        json.RawMessage `json:"usage"`         // message_delta
	Error        json.RawMessage `json:"error"`         // error
}

// blockDeltaWire content_block_delta.delta 子类型联合。
type blockDeltaWire struct {
	Type        string `json:"type"`
	Text        string `json:"text"`
	PartialJSON string `json:"partial_json"`
	Thinking    string `json:"thinking"`
	Signature   string `json:"signature"`
}

// messageDeltaWire message_delta.delta。
type messageDeltaWire struct {
	StopReason   *string         `json:"stop_reason"`
	StopSequence json.RawMessage `json:"stop_sequence"`
}

// errorWire Anthropic 错误对象内层 {type, message}。
type errorWire struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// ---------------------------------------------------------------------------
// 渲染侧常量表
// ---------------------------------------------------------------------------

// claudePassableExtras 是渲染 claude 请求体时允许从 IR Extra 透传的顶层键：
// 均为 Messages API 认识的字段（temperature/top_p 同名直迁；stop 由调用方转
// stop_sequences；max_tokens/max_completion_tokens 由调用方取值；其余为 claude
// 侧独有字段——跨格式腿来自 claude 侧时保真，来自 OpenAI 侧时不会出现）。
// 不在表内的 Extra（n/logprobs/seed/presence_penalty/…）一律丢弃——映射表
// 「丢弃+日志」降级策略；日志点在服务壳层，库层只做确定性丢弃。
var claudePassableExtras = []string{
	"temperature", "top_p", "service_tier",
	"thinking", "output_config", "context_management", "betas", "speed",
}
