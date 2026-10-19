// Package pairs 组装转换对：把 translate/convert/* 的转换器注册进 registry，
// 并提供跨格式转换执行入口（每一步都经 IR 中转）。
//
// 分层（docs/architecture.md 决策 3）：registry 只做「解析与分级」（哪个转换器、
// 什么质量），本包做「装配与执行」（把 Plan 的每一步落成 parse→render 调用）。
// 直转 = 1 步（源格式 parse → 目标格式 render）；经枢纽两步合成 = 2 步（IR 二次
// 中转，Quality 由 registry 压到不高于 Fair）。
package pairs

import (
	"bytes"
	"fmt"
	"io"

	"github.com/dwgx/any2api/translate/convert/chatgpt_web"
	"github.com/dwgx/any2api/translate/convert/claude_messages"
	"github.com/dwgx/any2api/translate/convert/claude_web"
	"github.com/dwgx/any2api/translate/convert/gemini"
	"github.com/dwgx/any2api/translate/convert/interactions"
	"github.com/dwgx/any2api/translate/convert/openai_chat"
	"github.com/dwgx/any2api/translate/convert/responses"
	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/registry"
	"github.com/dwgx/any2api/translate/toolname"
	"github.com/dwgx/any2api/translate/toolsim"
)

// Face 转换面（请求 / 非流式响应 / 流式）。
type Face string

const (
	FaceRequest  Face = "request"
	FaceResponse Face = "response"
	FaceStream   Face = "stream"
	FaceError    Face = "error"
)

// Options 转换选项。
type Options struct {
	// IncludeUsage openai_chat 出口流式：usage chunk 是否渲染
	//（对应请求侧 stream_options.include_usage；openai-wire §4.3）。
	IncludeUsage bool
	// Stream 调用方声明入口**路径态**为流式（gemini `:streamGenerateContent?alt=sse`
	// 语义——真 gemini 客户端的流开关在路径里，请求体无此字段；gemini-conversion-table
	// §流开关行）。请求面 parse 后落进 IR（`ir.Request.Stream`），使跨格式转换出的
	// 上游请求体带上流位（openai/claude/responses/interactions 的请求体都带 `stream`；
	// gemini 的流位在路径、由调用方按 Stream 拼端点）。**只置真、不清除**：请求体
	// 自己声明的流位不受影响（不发明/不擦除——W5-11 缺口 2 的边界条）。
	Stream bool

	// hubLeg 内部字段：本步是枢纽中间腿（由 Convert 的中间腿循环设置，调用方不设；
	// 流式面由 StreamPipe 自己按计划设置）。W42 ③ 起 openai 中间腿与响应面同表
	// （思考文本/签名经 reasoning_details 载体保真；redacted 载荷例外仍丢弃——与
	// 流式 StreamRenderOptions.HubLeg 同表，见其注释）。
	hubLeg bool

	// Model 入口**路径态**模型名（gemini `POST /v1beta/models/{model}:…`——模型名
	// 在路径里，请求体无 model 字段；gemini-conversion-table §格式兼容面）。请求面
	// parse 后**只填空、不覆盖**：IR 的 Model 为空时填入（跨格式转换出的上游请求体
	// 需要它——否则 openai/claude 上游收到空模型名）；请求体自己声明的模型优先
	// （不发明、不覆盖——与 Stream 的「只置真」同纪律）。
	Model string

	// Diag 非流式（批量）面诊断汇（可选）：传非 nil 指针即收集思考块丢弃/映射计数
	// （纯观测，绝不进控制流）。调用方对每次 Convert 传一个新鲜 sink（服务壳非流式
	// 路径的请求面/响应面各一个观测窗口；流式面用 StreamPipe.Diagnostics()）。
	Diag *BatchDiagnostics

	// ToolNames 工具名清洗表（原名 → 净化名；P67 实锤缺陷的双向映射层）。
	// 调用方（服务壳）按**入口**格式的请求体建表（BuildToolNameMap），请求面
	// 出口腿据此清洗、响应面入口腿据此还原（流式面由 StreamPipe 每帧还原）。
	// nil = 无长名/非法名（本层直过——既有路径零影响）。
	ToolNames map[string]string

	// ToolSim 客户端工具模拟（A1 形态：模拟·客户端执行；设计单
	// research/topics/claude-web-tool-sim/README.md §S1/§S2）。打开 = 请求面
	// 注入契约段（claude_web.RenderRequestWithOptions）+ 出口面把模型文本里的
	// 契约标签合成回 tool_use 块（出口腿 toolsim 过滤器，位于 toolExitStage
	// 之前——合成参数照走既有修复纪律；非流式面 TransformResponse 同词表）。
	//
	// **缺省关**：false 时既有路径逐字节不变。服务壳置位条件（设计单 §S1）：
	// 上游 = claude_web && 入口带 tools && 配置允许。
	ToolSim bool

	// AttachmentSink claude_web 请求面附件作业出口（W24-1 接缝；可选）。
	// 转换目标 = claude_web 且请求含图像块时，渲染器把附件作业写进 `sink.Job`
	//（零全局状态）；调用方（服务壳 forward）持有 sink 并把 `Job` 传给会话准备
	// 期消费（上传 + 原地等长回填——见 claude_web/attach.go 头注）。**nil 而含
	// 图像块 = 显式报错**（无处交付 = 体无法回填）。调用方纪律：每次转换传一个
	// 新鲜 sink（复用 = 后渲染覆盖；与 Diag 同款）。非 claude_web 目标 = 忽略。
	AttachmentSink *claude_web.AttachmentSink
}

// BatchDiagnostics 非流式（批量）转换的诊断计数汇——别名到计数点所在的
// openai_chat.BatchDiagnostics（非流式思考决策表在 openai 出口渲染器里）。
// 服务壳非流式路径据此对齐流式面 consumePipeDiagnostics 的口径（W5-18 条目 2）。
type BatchDiagnostics = openai_chat.BatchDiagnostics

// FormatInteractions Gemini Interactions 格式（第 5 格式）——
// 已登记进 translate/registry（FormatInteractions），此处为包内短别名。
const FormatInteractions = registry.FormatInteractions

// FormatClaudeWeb claude.ai 网页线格式（第 6 格式；上游叶子）——已登记进
// translate/registry（FormatClaudeWeb），此处为包内短别名。
const FormatClaudeWeb = registry.FormatClaudeWeb

// FormatChatGPTWeb chatgpt.com 网页线格式（第 7 格式；上游叶子）——已登记进
// translate/registry（FormatChatGPTWeb），此处为包内短别名。
const FormatChatGPTWeb = registry.FormatChatGPTWeb

// NewRegistry 创建并装配本 slice 的转换注册表：openai_chat ↔ claude_messages、
// openai_chat ↔ gemini_generate、openai_chat ↔ openai_responses（表 1）、
// claude_messages ↔ openai_responses（表 3）、interactions ↔ openai_chat、
// interactions ↔ gemini_generate（表 A/B）显式对（均经 IR 直转，Quality=good）；
// 枢纽 = openai_chat——gemini_generate ↔ openai_responses 等无表可依的对由枢纽
// 兜底合成两步计划（Quality 不高于 Fair）。
//
// responses 显式对的依据：openai-responses 表 1/表 3 的每一行都有两家网关的
// 直接实现（new-api `oai_responses/{to_oai_chat,to_claude_messages}*` +
// `oai_chat/to_oai_responses*` + `claude_messages/to_oai_responses_req.go`；
// CPA `openai/openai/responses` + `claude/openai/responses`）——直转 1 步优于
// 经枢纽的 2 步（避免枢纽腿的形状往返损耗）。
//
// interactions 显式对的依据：interactions-wire 表 A（openai_chat）与表 B
// （generateContent）逐行都有 CPA 的直接实现（`internal/translator/
// {openai/interactions,gemini/interactions,interactions/*}` 六条腿互转）——
// 同样是直转 1 步优于枢纽 2 步。claude_messages ↔ interactions 表未给（CPA 有
// 实现但本仓无表行）→ 走枢纽两步（Quality ≤ Fair）。
func NewRegistry() *registry.Registry {
	r := registry.New(registry.FormatOpenAIChat)
	r.MustRegister(registry.Conversion{
		From: registry.FormatOpenAIChat, To: registry.FormatClaudeMessages,
		Quality: registry.QualityGood,
	})
	r.MustRegister(registry.Conversion{
		From: registry.FormatClaudeMessages, To: registry.FormatOpenAIChat,
		Quality: registry.QualityGood,
	})
	r.MustRegister(registry.Conversion{
		From: registry.FormatOpenAIChat, To: registry.FormatGeminiGenerate,
		Quality: registry.QualityGood,
	})
	r.MustRegister(registry.Conversion{
		From: registry.FormatGeminiGenerate, To: registry.FormatOpenAIChat,
		Quality: registry.QualityGood,
	})
	r.MustRegister(registry.Conversion{
		From: registry.FormatOpenAIChat, To: registry.FormatOpenAIResponses,
		Quality: registry.QualityGood,
	})
	r.MustRegister(registry.Conversion{
		From: registry.FormatOpenAIResponses, To: registry.FormatOpenAIChat,
		Quality: registry.QualityGood,
	})
	r.MustRegister(registry.Conversion{
		From: registry.FormatClaudeMessages, To: registry.FormatOpenAIResponses,
		Quality: registry.QualityGood,
	})
	r.MustRegister(registry.Conversion{
		From: registry.FormatOpenAIResponses, To: registry.FormatClaudeMessages,
		Quality: registry.QualityGood,
	})
	// 第 5 格式（interactions）显式对：表 A（openai_chat）/表 B（generateContent）
	// 逐行有 CPA 直接实现 → 直转 1 步、Quality=good。
	r.MustRegister(registry.Conversion{
		From: registry.FormatOpenAIChat, To: FormatInteractions,
		Quality: registry.QualityGood,
	})
	r.MustRegister(registry.Conversion{
		From: FormatInteractions, To: registry.FormatOpenAIChat,
		Quality: registry.QualityGood,
	})
	r.MustRegister(registry.Conversion{
		From: registry.FormatGeminiGenerate, To: FormatInteractions,
		Quality: registry.QualityGood,
	})
	r.MustRegister(registry.Conversion{
		From: FormatInteractions, To: registry.FormatGeminiGenerate,
		Quality: registry.QualityGood,
	})
	// 第 6 格式（claude_web，上游叶子）显式对：四个入口 ↔ claude_web 双向直转
	// （1 步，经 IR）。依据：claude_web 是一等上游协议（服务壳的 ProtocolClaudeWeb），
	// 任一入口都要能直连它；本仓所有转换腿都是 IR 中介，直转 1 步严格优于经枢纽
	// 的 2 步（少一次线格式往返损耗）。上游叶子性只约束「不作为入口/出口」，
	// 不约束转换对的登记（转换器面由 claude_web 包显式报错守门）。
	for _, f := range []registry.Format{
		registry.FormatOpenAIChat, registry.FormatClaudeMessages,
		registry.FormatGeminiGenerate, registry.FormatOpenAIResponses,
	} {
		r.MustRegister(registry.Conversion{From: f, To: FormatClaudeWeb, Quality: registry.QualityGood})
		r.MustRegister(registry.Conversion{From: FormatClaudeWeb, To: f, Quality: registry.QualityGood})
	}
	// 第 7 格式（chatgpt_web，上游叶子）显式对：四个入口 ↔ chatgpt_web 双向直转
	// （1 步，经 IR）。依据同 claude_web（第 6 格式）：chatgpt_web 是一等上游协议
	// （服务壳的 ProtocolChatGPTWeb），任一入口都要能直连它；直转 1 步严格优于经
	// 枢纽的 2 步。上游叶子性只约束「不作为入口/出口」（转换器面由 chatgpt_web 包
	// 的 ErrUnsupportedFace 守门：入口面/出口面显式报错）。
	for _, f := range []registry.Format{
		registry.FormatOpenAIChat, registry.FormatClaudeMessages,
		registry.FormatGeminiGenerate, registry.FormatOpenAIResponses,
	} {
		r.MustRegister(registry.Conversion{From: f, To: FormatChatGPTWeb, Quality: registry.QualityGood})
		r.MustRegister(registry.Conversion{From: FormatChatGPTWeb, To: f, Quality: registry.QualityGood})
	}
	return r
}

// converter 一个格式的各面解析/渲染函数集。
// 渲染函数收 Options：出口腿需要它决定非流式面的思考块落法（openai 出口的
// HubLeg——与流式同一决策表；其余格式忽略）。
type converter struct {
	parseRequest   func([]byte) (*ir.Request, error)
	renderRequest  func(*ir.Request, Options) ([]byte, error)
	parseResponse  func([]byte) (*ir.Response, error)
	renderResponse func(*ir.Response, Options) ([]byte, error)
	parseStream    func(io.Reader) ([]ir.StreamEvent, error)
	renderStream   func([]ir.StreamEvent, Options) ([]byte, error)
	parseError     func([]byte) *ir.Error
	renderError    func(*ir.Error) ([]byte, error)
}

// renderRequestIgnoringOpts / renderResponseIgnoringOpts 非 openai 出口的渲染
// 适配：Options 对这些格式无意义（忽略）。
func renderRequestIgnoringOpts(fn func(*ir.Request) ([]byte, error)) func(*ir.Request, Options) ([]byte, error) {
	return func(req *ir.Request, _ Options) ([]byte, error) { return fn(req) }
}

func renderResponseIgnoringOpts(fn func(*ir.Response) ([]byte, error)) func(*ir.Response, Options) ([]byte, error) {
	return func(resp *ir.Response, _ Options) ([]byte, error) { return fn(resp) }
}

// openaiRenderOptions 出口腿 Options → openai_chat 非流式渲染选项
// （hubLeg：枢纽中间腿与响应面同表——W42 ③ 思考文本/签名经载体保真、redacted
// 载荷仍丢弃，见 openai_chat.StreamRenderOptions.HubLeg 注释；Diag：非流式面
// 诊断汇透传）。
func openaiRenderOptions(opts Options) openai_chat.RenderOptions {
	return openai_chat.RenderOptions{HubLeg: opts.hubLeg, Diag: opts.Diag}
}

// converters 格式 → 转换器实现。未登记格式 = 未支持（Lookup 前就会显式报错）。
var converters = map[registry.Format]converter{
	registry.FormatOpenAIChat: {
		parseRequest: openai_chat.ParseRequest,
		renderRequest: func(req *ir.Request, opts Options) ([]byte, error) {
			return openai_chat.RenderRequest(req, openaiRenderOptions(opts))
		},
		parseResponse: openai_chat.ParseResponse,
		renderResponse: func(resp *ir.Response, opts Options) ([]byte, error) {
			return openai_chat.RenderResponse(resp, openaiRenderOptions(opts))
		},
		parseStream: openai_chat.ParseStream,
		renderStream: func(events []ir.StreamEvent, opts Options) ([]byte, error) {
			return openai_chat.RenderStream(events, openai_chat.StreamRenderOptions{IncludeUsage: opts.IncludeUsage})
		},
		parseError:  openai_chat.ParseError,
		renderError: openai_chat.RenderError,
	},
	registry.FormatClaudeMessages: {
		parseRequest:   claude_messages.ParseRequest,
		renderRequest:  renderRequestIgnoringOpts(claude_messages.RenderRequest),
		parseResponse:  claude_messages.ParseResponse,
		renderResponse: renderResponseIgnoringOpts(claude_messages.RenderResponse),
		parseStream:    claude_messages.ParseStream,
		renderStream: func(events []ir.StreamEvent, _ Options) ([]byte, error) {
			return claude_messages.RenderStream(events)
		},
		parseError:  claude_messages.ParseError,
		renderError: claude_messages.RenderError,
	},
	registry.FormatOpenAIResponses: {
		parseRequest:   responses.ParseRequest,
		renderRequest:  renderRequestIgnoringOpts(responses.RenderRequest),
		parseResponse:  responses.ParseResponse,
		renderResponse: renderResponseIgnoringOpts(responses.RenderResponse),
		parseStream:    responses.ParseStream,
		renderStream: func(events []ir.StreamEvent, _ Options) ([]byte, error) {
			return responses.RenderStream(events)
		},
		parseError:  responses.ParseError,
		renderError: responses.RenderError,
	},
	registry.FormatGeminiGenerate: {
		parseRequest:   gemini.ParseRequest,
		renderRequest:  renderRequestIgnoringOpts(gemini.RenderRequest),
		parseResponse:  gemini.ParseResponse,
		renderResponse: renderResponseIgnoringOpts(gemini.RenderResponse),
		parseStream:    gemini.ParseStream,
		renderStream: func(events []ir.StreamEvent, _ Options) ([]byte, error) {
			return gemini.RenderStream(events)
		},
		parseError:  gemini.ParseError,
		renderError: gemini.RenderError,
	},
	FormatInteractions: {
		parseRequest:   interactions.ParseRequest,
		renderRequest:  renderRequestIgnoringOpts(interactions.RenderRequest),
		parseResponse:  interactions.ParseResponse,
		renderResponse: renderResponseIgnoringOpts(interactions.RenderResponse),
		parseStream:    interactions.ParseStream,
		renderStream: func(events []ir.StreamEvent, _ Options) ([]byte, error) {
			return interactions.RenderStream(events)
		},
		parseError:  interactions.ParseError,
		renderError: interactions.RenderError,
	},
	FormatClaudeWeb: {
		// 上游叶子格式：请求面只出（renderRequest），响应面只进（parseResponse/
		// parseStream/parseError）。其余面由 claude_web 包显式报 ErrUnsupportedFace
		// （绝不返回半成品字节，也绝不 nil 解引用）。
		parseRequest: claude_web.ParseRequest,
		// 请求面读 Options.ToolSim（S1 注入位）与 Options.AttachmentSink（W24-1
		// 附件作业出口；claude_web 不 import pairs——选项经 RenderOptions 镜像搬运，
		// 缺省 nil = 历史行为逐字节相等，含图像块时显式报错）。
		renderRequest: func(req *ir.Request, opts Options) ([]byte, error) {
			return claude_web.RenderRequestWithOptions(req, claude_web.RenderOptions{
				ToolSim:        opts.ToolSim,
				AttachmentSink: opts.AttachmentSink,
			})
		},
		parseResponse:  claude_web.ParseResponse,
		renderResponse: renderResponseIgnoringOpts(claude_web.RenderResponse),
		parseStream:    claude_web.ParseStream,
		renderStream: func(events []ir.StreamEvent, _ Options) ([]byte, error) {
			return claude_web.RenderStream(events)
		},
		parseError:  claude_web.ParseError,
		renderError: claude_web.RenderError,
	},
	FormatChatGPTWeb: {
		// 上游叶子格式（第 7 格式）：请求面只出（renderRequest——IR → Android
		// 匿名体，prepare 与 f/conversation 同体），响应面只进（parseResponse/
		// parseStream/parseError）。其余面由 chatgpt_web 包显式报
		// ErrUnsupportedFace（绝不返回半成品字节，也绝不 nil 解引用）。
		parseRequest:   chatgpt_web.ParseRequest,
		renderRequest:  renderRequestIgnoringOpts(chatgpt_web.RenderRequest),
		parseResponse:  chatgpt_web.ParseResponse,
		renderResponse: renderResponseIgnoringOpts(chatgpt_web.RenderResponse),
		parseStream:    chatgpt_web.ParseStream,
		renderStream: func(events []ir.StreamEvent, _ Options) ([]byte, error) {
			return chatgpt_web.RenderStream(events)
		},
		parseError:  chatgpt_web.ParseError,
		renderError: chatgpt_web.RenderError,
	},
}

// Convert 执行一次跨格式转换：registry.Lookup 出计划 → 逐步执行
// （每步 parse 源格式 → render 目标格式，经 IR 中转）。
// 缺省无转换时返回 registry 的 *NoConversionError（errors.Is 可判）。
func Convert(reg *registry.Registry, from, to registry.Format, face Face, payload []byte, opts Options) ([]byte, error) {
	if reg == nil {
		return nil, fmt.Errorf("pairs: nil registry")
	}
	if _, ok := converters[from]; !ok {
		return nil, fmt.Errorf("pairs: no converter for %s", from)
	}
	if _, ok := converters[to]; !ok {
		return nil, fmt.Errorf("pairs: no converter for %s", to)
	}
	plan, err := reg.Lookup(from, to)
	if err != nil {
		return nil, err
	}
	cur := payload
	for i, step := range plan.Steps {
		stepOpts := opts
		if i < len(plan.Steps)-1 {
			// 中间腿（枢纽）是内部腿：usage 必须过桥——openai 出口才受 include_usage
			// 门控，出口渲染器按客户端格式决定发不发。否则 gemini 快照 usage 经两步
			// 被 openai 的 include_usage 门静默吞掉（claude 出口恒发 usage）。
			stepOpts.IncludeUsage = true
			// 非流式面的枢纽腿标记：openai 中间腿与响应面同表（W42 ③——思考
			// 文本/签名经 reasoning_details 载体保真，redacted 载荷仍丢弃；绝不
			// 因思考断链）。
			stepOpts.hubLeg = true
		}
		out, err := runStep(step, face, cur, stepOpts)
		if err != nil {
			return nil, fmt.Errorf("pairs: step %d (%s→%s): %w", i, step.From, step.To, err)
		}
		cur = out
	}
	return cur, nil
}

// runStep 执行计划中的一步（源格式 parse → 目标格式 render）。
// opts.hubLeg = 该步是否为计划的中间腿（枢纽）——流式面的出口修复层只接出口腿，
// 非流式面的思考块落法也按它分岔（见 Options.hubLeg）。
func runStep(step registry.Step, face Face, payload []byte, opts Options) ([]byte, error) {
	src, ok := converters[step.From]
	if !ok {
		return nil, fmt.Errorf("pairs: no converter for %s", step.From)
	}
	dst, ok := converters[step.To]
	if !ok {
		return nil, fmt.Errorf("pairs: no converter for %s", step.To)
	}
	final := !opts.hubLeg
	switch face {
	case FaceRequest:
		req, err := src.parseRequest(payload)
		if err != nil {
			return nil, err
		}
		// 路径态流开关（W5-11 缺口 2）：调用方按入口路径声明流式（gemini
		// `:streamGenerateContent`）时把流位落进 IR——只置真、不清除（请求体自己
		// 声明的流位不受影响；不发明也不擦除）。gemini 出口的流位在端点路径里
		// （渲染器不写 body），openai/claude/responses/interactions 出口写 body。
		if opts.Stream {
			req.Stream = true
		}
		// 入口路径态模型名（gemini `POST /v1beta/models/{model}:…`）：**只填空、
		// 不覆盖**——请求体自己声明的模型优先（不发明、不覆盖）。
		if opts.Model != "" && req.Model == "" {
			req.Model = opts.Model
		}
		if final && len(opts.ToolNames) > 0 {
			// 工具名清洗（P67 实锤缺陷）：出口腿渲染前把名字换成上游合法形态
			//（只对**最终腿**——枢纽中间腿是内部线格式，不需要净化；且服务壳建表
			// 时入口腿已把原名留在表里，最终腿查表命中即洗）。
			toolname.SanitizeRequest(req, opts.ToolNames)
		}
		return dst.renderRequest(req, opts)
	case FaceResponse:
		resp, err := src.parseResponse(payload)
		if err != nil {
			return nil, err
		}
		if final && opts.ToolSim {
			// 工具模拟（S2 非流式面）：文本块里的契约标签 → tool_use 块
			//（与流式面同词表同容错）。放在还原之前——合成块的工具名同样要
			// 走净化名→原名还原。
			toolsim.TransformResponse(resp)
		}
		if final && len(opts.ToolNames) > 0 {
			// 响应侧还原（r14：上游按净化名回调 → 还原成原名客户端才认得出）。
			toolname.RestoreResponse(resp, toolname.Reverse(opts.ToolNames))
		}
		return dst.renderResponse(resp, opts)
	case FaceStream:
		events, err := src.parseStream(bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		if final && len(opts.ToolNames) > 0 {
			// 流式响应侧还原（与 FaceResponse 同表；StreamPipe 在增量面做同一件事）。
			toolname.RestoreEvents(events, toolname.Reverse(opts.ToolNames))
		}
		if final {
			if opts.ToolSim {
				// 工具模拟（S2 流式面，批量路径）：契约标签 → tool_use 块。
				// 与 StreamPipe 出口腿同一接线位置（toolExitStage **之前**）——
				// 合成参数照走修复纪律；批量与增量行为一致（测试共享）。
				events = toolsim.NewFilter(toolname.Reverse(opts.ToolNames)).Process(events)
			}
			// 出口修复层（与 StreamPipe 出口腿同一接线，同一帧形）：畸形 tool
			// 参数在出口 merge/修复/解包/`{}` 收口——批量与增量路径行为一致
			// （goldens 共享）。
			events = newToolExitStage(consumerContractFor(step.To)).process(events)
		}
		return dst.renderStream(events, opts)
	case FaceError:
		return dst.renderError(src.parseError(payload))
	default:
		return nil, fmt.Errorf("pairs: unknown face %q", face)
	}
}

// BuildToolNameMap 入口格式的**原始请求体** → 工具名清洗表（原名 → 净化名）。
// 服务壳在 serve 时建一次、传进 Options.ToolNames（请求面清洗 + 响应面还原共用）。
//
// 解析失败/无工具 → nil（本层直过；请求本身的错误由正常转换路径报出——建表是
// 尽力而为的旁路，绝不成为新的失败点）。空表（工具名全合法）→ nil。
func BuildToolNameMap(from registry.Format, payload []byte) map[string]string {
	c, ok := converters[from]
	if !ok {
		return nil
	}
	req, err := c.parseRequest(payload)
	if err != nil || req == nil {
		return nil
	}
	return toolname.Build(req.Tools)
}
