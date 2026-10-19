package openai_chat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dwgx/any2api/translate/hygiene"
	"github.com/dwgx/any2api/translate/ir"
)

// ---------------------------------------------------------------------------
// 请求：IR → 线格式
// ---------------------------------------------------------------------------

// RenderOptions 非流式渲染选项。思考块落法与流式 StreamRenderOptions 同一决策表
// （P53 缺口 1：批量面此前对 thinking 报 ErrUnsupportedBlock，W5-11 只修了流式面）。
type RenderOptions struct {
	// HubLeg 本渲染是**枢纽中间腿**（内部腿，不是给真实消费者的出口）：thinking /
	// redacted_thinking 一律丢弃 + 绝不报错——OpenAI 线格式不可表达它们，中间腿因
	// 它断链会让 claude→gemini 等经枢纽的链路整条失败（与流式同表）。
	HubLeg bool
	// Diag 非流式面诊断汇（可选，W5-18 条目 2）：非 nil 时收集思考块丢弃/映射
	// 计数（纯观测，绝不进控制流）。服务壳非流式路径每次 Convert 传一个新鲜 sink。
	Diag *BatchDiagnostics
}

// BatchDiagnostics 非流式（批量）面的诊断计数（W5-18 条目 2：批量面此前没有
// 等价于流式 `pairs.StreamPipe.Diagnostics()` 的出口）。计数点是 renderMessage 的
// 思考决策表；单次 Convert 调用内单 goroutine 使用（服务壳每次 Convert 一个
// 新鲜 sink——绝不跨请求共享）。
type BatchDiagnostics struct {
	// DroppedThinking 非流式渲染丢弃的思考块数：请求面无兼容签名（表 §7.1 回传轮
	// fallback）/ 枢纽中间腿 / 非 assistant 消息（安全规则）/ redacted_thinking /
	// 空文本思考块。与流式面 DroppedThinking 的出口腿口径同族。
	DroppedThinking int
	// ReasoningMapped 以 `message.reasoning_content` 落下的思考块数（响应面
	// assistant + 请求面签名兼容分支）——对应流式面 ReasoningDeltas。
	ReasoningMapped int
	// SignatureDropped 思考块/tool_use 块**签名未落地**的块数（W5-19 条目 5；
	// W41 载体化后口径 = 未写进 reasoning_details 的签名；W42 起含 tool_use 块
	// 签名）：不可识别签名（红线）/ 非 Gemini 工具签名 / 非 assistant / 整块丢弃
	// 的思考块签名。对应流式面 StreamDiagnostics.SignatureDropped，带签名案例可
	// 逐项对拍。
	SignatureDropped int
}

// RenderRequest 渲染 OpenAI Chat Completions 请求体。
// 保真：未建模字段经 Extra 原样回传（建模字段优先，防冲突污染）。
// 思考块（thinking / redacted_thinking）在请求面按回传轮规则落法：签名形态可识别
// → reasoning_content + reasoning_details（W41 载体），否则整块丢弃——绝不报错
// （非流式整条链不因思考断；见 renderMessage 的注释）。
func RenderRequest(req *ir.Request, opts RenderOptions) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: nil request", ErrBadIR)
	}
	out := map[string]json.RawMessage{"model": mustMarshal(req.Model)}

	mode := renderMode{request: true, hubLeg: opts.HubLeg, diag: opts.Diag}
	msgs := make([]json.RawMessage, 0, len(req.Messages))
	for i := range req.Messages {
		// 请求面（客户端历史回传）：无响应 id 可派生 → 合成工具 id 走块序回落
		// 形态（seed 空）。
		rm, err := renderMessage(req.Messages[i], mode, "")
		if err != nil {
			return nil, fmt.Errorf("message %d: %w", i, err)
		}
		msgs = append(msgs, rm)
	}
	out["messages"] = mustMarshal(msgs)

	if len(req.Tools) > 0 {
		tools := make([]json.RawMessage, 0, len(req.Tools))
		for i := range req.Tools {
			rt, err := renderToolDef(req.Tools[i])
			if err != nil {
				return nil, fmt.Errorf("tool %d: %w", i, err)
			}
			tools = append(tools, rt)
		}
		out["tools"] = mustMarshal(tools)
	}
	if len(req.ToolChoice) > 0 && !isJSONNull(req.ToolChoice) {
		out["tool_choice"] = req.ToolChoice
	}
	if req.Stream {
		out["stream"] = mustMarshal(true)
	}
	if req.StreamOptions != nil {
		so := map[string]json.RawMessage{}
		if req.StreamOptions.IncludeUsage {
			so["include_usage"] = mustMarshal(true)
		}
		if req.StreamOptions.IncludeObfuscation != nil {
			so["include_obfuscation"] = mustMarshal(*req.StreamOptions.IncludeObfuscation)
		}
		out["stream_options"] = mustMarshal(so)
	}
	mergeExtras(out, pruneForeignRequestExtras(req.Extra))
	return marshalBytes(out)
}

// claudeOriginExtras claude_messages 侧独有、OpenAI Chat 不认识的顶层字段：
// 跨格式转换（claude→openai_chat）时必须显式丢弃（anthropic-conversion-table
// 「丢弃+日志」降级策略；日志点在服务壳层——库层只做确定性丢弃，绝不透传
// 导致上游 400 Unrecognized request argument）。同格式回传（openai→openai）
// 不受影响：这些键不会出现在 openai 解析产物里。
var claudeOriginExtras = map[string]bool{
	"system":             true, // claude 顶层 system（已提升为消息，防泄漏）
	"betas":              true,
	"thinking":           true,
	"output_config":      true,
	"context_management": true,
	"speed":              true,
	"stop_sequences":     true, // 已映射为 stop，防原文泄漏
	"anthropic_version":  true,
}

// geminiOriginExtras gemini_generate 侧独有的顶层字段（gemini/wire.go
// geminiOnlyExtraKeys 的解析产物）：跨格式转换（gemini→openai_chat，含
// claude 经枢纽两步的中间腿）时必须显式丢弃——OpenAI 不认识这些键，
// 透传 = 上游 400（gemini-conversion-table §请求侧「丢弃+日志」行）。
// 规范键（max_tokens/temperature/top_p/stop/n/seed/…）不在此列：那是跨格式
// 直迁面，本表只收 gemini 独有键。
var geminiOriginExtras = map[string]bool{
	"safetySettings":     true,
	"thinkingConfig":     true,
	"responseMimeType":   true,
	"responseJsonSchema": true,
	"responseModalities": true,
}

// anthropicOnlyUsageKeys Anthropic 独有 usage 键：出 OpenAI 时剥离（防污染）。
var anthropicOnlyUsageKeys = map[string]bool{
	"server_tool_use":           true,
	"inference_geo":             true,
	"iterations":                true,
	"speed":                     true,
	"cache_creation":            true,
	"thinking_tokens":           true,
	"ephemeral_5m_input_tokens": true,
	"ephemeral_1h_input_tokens": true,
}

// pruneForeignRequestExtras 剔除本格式不认识的来源侧独有键
// （见 claudeOriginExtras / geminiOriginExtras）。
func pruneForeignRequestExtras(extra map[string]json.RawMessage) map[string]json.RawMessage {
	if len(extra) == 0 {
		return extra
	}
	out := make(map[string]json.RawMessage, len(extra))
	for k, v := range extra {
		if claudeOriginExtras[k] || geminiOriginExtras[k] {
			continue
		}
		out[k] = v
	}
	return out
}

// foreignResponseExtras 非 openai_chat 来源的响应顶层独有键（OpenAI Chat 不
// 认识）：跨格式转换（X→openai_chat，含经枢纽两步的中间/出口腿）时必须显式
// 丢弃——OpenAI 客户端/上游不认识这些键，透传 = 响应体污染。
//
// 键集全部有据（各来源解析器建模键集之外、经 Response.Extra 保真旁路的键）：
//   - claude_messages（anthropic-messages README §响应字段清单）：container /
//     stop_details / diagnostics；
//   - gemini_generate（gemini-wire §响应顶层）：modelStatus——官方字段，本仓
//     解析器未建模（落 Response.Extra）；
//   - interactions（interactions-wire §响应）：errors——平台错误清单，解析侧
//     注释即「渲染侧不回写（诊断用）」。
//   - upstream-a / upstream-private（OpenAI 兼容上游的私有响应扩展，W83-1 X1
//     实测 2026-10-19；W101 实测 2026-10-19 Gemini 非流式响应顶层同样携带）：
//     elapsed_ms / first_chunk_at——上游私有计时（总耗时毫秒 / 首块到达时刻），
//     OpenAI 响应无对应字段。
//
// responses 不建行：responses.ParseResponse 不产 Response.Extra（无键可携带）。
// 通用未知键（厂商扩展等）不在表内：保真旁路保留（只剔来源侧**独有**键）。
//
// 同格式回传（openai→openai）走服务壳字节直通、不进本渲染器；本表约束的是
// 跨格式出口腿：任一来源解析器落进 Response.Extra 的上述键在此被剔除。
var foreignResponseExtras = map[string]bool{
	"container":      true, // 代码执行容器信息（Anthropic 独有）
	"stop_details":   true, // 结构化停止细节（Anthropic 独有）
	"diagnostics":    true, // cache_miss_reason 等诊断字段（Anthropic 独有）
	"modelStatus":    true, // gemini 官方响应字段（未建模）
	"errors":         true, // interactions 平台错误清单
	"elapsed_ms":     true, // upstream-a/upstream-private 私有总耗时（W83-1 X1）
	"first_chunk_at": true, // upstream-a/upstream-private 私有首块时刻（W83-1 X1）
}

// pruneForeignResponseExtras 剔除 OpenAI 响应体不认识的来源侧独有键
// （见 foreignResponseExtras）。
func pruneForeignResponseExtras(extra map[string]json.RawMessage) map[string]json.RawMessage {
	if len(extra) == 0 {
		return extra
	}
	out := make(map[string]json.RawMessage, len(extra))
	for k, v := range extra {
		if foreignResponseExtras[k] {
			continue
		}
		out[k] = v
	}
	return out
}

// ---------------------------------------------------------------------------
// 非流式响应：IR → 线格式
// ---------------------------------------------------------------------------

// RenderResponse 渲染非流式 chat.completion 响应体（choice 0）。
//
// id：上游不给 → 合成每响应唯一 `r_synth_<32hex>`（W5-31；同流式面），并作为
// 合成工具 id 的派生 seed——响应内确定、跨响应唯一。
//
// Extra：来源侧独有键先整体剔除（foreignResponseExtras）——见 pruneForeignResponseExtras。
// 思考块：assistant 消息的思考文本走 message.reasoning_content（非官方扩展字段，
// 生态惯例）；形态可识别的签名走 message.reasoning_details（W41 载体，双通道并行；
// 见 renderMessage 的决策表）。
func RenderResponse(resp *ir.Response, opts RenderOptions) ([]byte, error) {
	if resp == nil {
		return nil, fmt.Errorf("%w: nil response", ErrBadIR)
	}
	// 上游不给响应 id → 合成每响应唯一（`r_synth_<32hex>`，W5-31）：客户端把它
	// 写进 transcript/日志；同时是合成工具 id 的派生 seed（响应内确定 + 跨响应
	// 唯一——旧行为 seed 空 → 工具 id 回落块序形态 `call_synth_<n>`，跨响应重复）。
	id := resp.ID
	if id == "" {
		id = hygiene.SynthUniqueID("r")
	}
	msg, err := renderMessage(resp.Message, renderMode{response: true, hubLeg: opts.HubLeg, diag: opts.Diag}, id)
	if err != nil {
		return nil, fmt.Errorf("message: %w", err)
	}
	choice := map[string]json.RawMessage{
		"index":         mustMarshal(0),
		"message":       msg,
		"finish_reason": marshalFinishReason(resp.StopReason),
	}
	out := map[string]json.RawMessage{
		"id":      mustMarshal(id),
		"object":  mustMarshal(objectChatCompletion),
		"created": mustMarshal(resp.Created),
		"model":   mustMarshal(resp.Model),
		"choices": mustMarshal([]json.RawMessage{mustMarshal(choice)}),
	}
	if resp.Usage != nil {
		u, err := renderUsage(resp.Usage)
		if err != nil {
			return nil, err
		}
		out["usage"] = u
	}
	mergeExtras(out, pruneForeignResponseExtras(resp.Extra))
	return marshalBytes(out)
}

// ---------------------------------------------------------------------------
// 错误：IR → 线格式（绝不透传 Error.Raw）
// ---------------------------------------------------------------------------

// RenderError 渲染 OpenAI 错误信封 {"error":{message,type,param,code}}。
// 纪律：Error.Raw 永不进入输出（错误默认不透传上游 body——grok2api 纪律）。
// type/code 按 §7.2 双向表映射（claude 独有类型 → OpenAI 最近邻；未映射值保留字面）。
func RenderError(e *ir.Error) ([]byte, error) {
	if e == nil {
		return nil, fmt.Errorf("%w: nil error", ErrBadIR)
	}
	typ, code := openaiErrorTypeFor(e)
	inner := map[string]json.RawMessage{
		"message": mustMarshal(e.Message),
		"type":    mustMarshal(typ),
	}
	if e.Param != "" {
		inner["param"] = mustMarshal(e.Param)
	}
	if code != "" {
		inner["code"] = mustMarshal(code)
	}
	return marshalBytes(map[string]json.RawMessage{"error": mustMarshal(inner)})
}

// openaiErrorTypeFor Anthropic 错误 type → OpenAI 错误 {type, code}
// （anthropic-conversion-table §7.2；未映射值原样透传——OpenAI 侧自有类型
// 如 invalid_request_error/server_error 均不在表内，同格式回传不受影响）。
func openaiErrorTypeFor(e *ir.Error) (typ, code string) {
	typ, code = e.Type, e.Code
	switch e.Type {
	case "authentication_error":
		return "invalid_api_key", "invalid_api_key"
	case "billing_error":
		return "insufficient_quota", ""
	case "api_error", "timeout_error":
		return "server_error", ""
	case "overloaded_error":
		// CC C2：错误体保留 overloaded_error 字面形态（529 文本匹配依赖）。
		return "server_error", "overloaded_error"
	case "request_too_large":
		return "invalid_request_error", "request_too_large"
	default:
		if typ == "" {
			typ = "api_error"
		}
		return typ, code
	}
}

// ---------------------------------------------------------------------------
// 流式：IR 事件 → SSE（[DONE] 只在此生成）
// ---------------------------------------------------------------------------

// StreamRenderOptions 流式渲染选项。
type StreamRenderOptions struct {
	// IncludeUsage 为真且事件带 Usage 时渲染 usage chunk（对应请求侧
	// stream_options.include_usage；openai-wire §4.3）。
	IncludeUsage bool
	// HubLeg 标记本渲染器是**枢纽中间腿**（内部腿，不是给真实消费者的出口）：
	// W42 ③ 起思考/tool 签名与直转面同表走 reasoning_content + reasoning_details
	// 载体（内部线成员；hub 腿消费者是本仓解析器，出口腿各自按 provider 门落地
	// ——此前整块丢弃让 claude→gemini 等两步链静默丢思考）。redacted 载荷例外：
	// 中间腿仍丢弃（gemini 出口无 redacted 载体，保真会断链，见报告 §hub 腿评估）。
	HubLeg bool
}

// RenderStream 把 IR 事件序列渲染为 OpenAI Chat SSE 字节流（批量入口；
// 增量/流式场景用 NewStreamWriter——同一状态机内核）。
//
// 契约：
//   - 序列必须以 start 开头（否则 ErrMissingStart）；
//   - done/error 为终态，其后事件报错（ErrEventsAfterTerminal）；
//   - [DONE] 由本渲染器生成（哨兵是合成器职责，stream-mechanics §4.6）——
//     事件流未带 done 时自动补；error 帧后按 vLLM 形态补 [DONE]；
//   - tool 块首片参数附着在 block_start chunk 上（id/name/arguments 同帧）；
//     无参数片段时 arguments 渲染为 "{}"（空参数出口修复）。
func RenderStream(events []ir.StreamEvent, opts StreamRenderOptions) ([]byte, error) {
	w := NewStreamWriter(opts)
	var buf bytes.Buffer
	for _, ev := range events {
		b, err := w.Write(ev)
		if err != nil {
			return nil, err
		}
		buf.Write(b)
	}
	b, err := w.Finish()
	if err != nil {
		return nil, err
	}
	buf.Write(b)
	return buf.Bytes(), nil
}

// ---------------------------------------------------------------------------
// 渲染内部工具
// ---------------------------------------------------------------------------

// renderMode 非流式消息渲染上下文（决定思考块的落法；与流式同一决策表）。
type renderMode struct {
	// response 响应面：思考文本有载体（message.reasoning_content）+ 结构化载体
	// （message.reasoning_details，W41）。
	response bool
	// request 请求面（回传轮）：签名形态可识别（GPT/Claude/Gemini 任一，W41 载体化
	// ——判定见 signature.ProviderForSignature）才映射 reasoning_content +
	// reasoning_details，否则整块丢弃（表 §7.1 回传轮行）。
	request bool
	// hubLeg 枢纽中间腿（内部腿）：思考与响应面同表（W42 ③——载体化后中间线可
	// 表达思考；redacted 载荷例外：中间腿仍丢弃——gemini 出口无 redacted 载体，
	// 保真会把两步链打断，见报告 §hub 腿评估）。
	hubLeg bool
	// diag 诊断汇（可选）：思考块丢弃/映射计数（纯观测）。
	diag *BatchDiagnostics
}

// dropThinking 记一次思考块丢弃（diag 为 nil 时零开销）。
func (m renderMode) dropThinking() {
	if m.diag != nil {
		m.diag.DroppedThinking++
	}
}

// countReasoning 记一次以 reasoning_content 落下的思考块（diag 为 nil 时零开销）。
func (m renderMode) countReasoning() {
	if m.diag != nil {
		m.diag.ReasoningMapped++
	}
}

// dropSignature 记一次思考块签名未落地（diag 为 nil 时零开销）。W41 起 OpenAI 线
// 有结构化载体重放签名（reasoning_details）——只有形态**可识别**且真正写进载体的
// 签名不算丢；不可识别（红线：不写）/ 非 assistant / 整块丢弃仍计数
// （W5-19 条目 5 的口径延续；W42 ③ 起枢纽腿与响应面同表——可识别签名走载体、不计）。
func (m renderMode) dropSignature() {
	if m.diag != nil {
		m.diag.SignatureDropped++
	}
}

// renderMessage 渲染一条消息（请求与响应共用）。
// content 形态规范化：无块 → null；恰一个 text 块 → 字符串；其余 → parts 数组。
//
// 思考块落法（P53 缺口 1 + W41 载体化 + W42 扩展；与流式 StreamWriter 同一决策表，
// 证据：anthropic-conversion-table §响应侧「思考」行 / §7.1 回传轮行 +
// gemini-conversion-table §思考行「拼接全部 thought parts」+
// OpenRouter reasoning-tokens.md「Reasoning Details API Shape」）：
//
//   - 响应面 assistant 消息：思考文本 → message.reasoning_content（非官方扩展
//     字段，生态惯例）；**形态可识别的签名 → message.reasoning_details**
//     （{type:reasoning.text, text, signature, format, index}，W41 载体）；不可
//     识别 → 丢弃（红线）；
//   - 请求面 assistant 消息（回传轮）：**仅当签名形态可识别**（GPT Fernet /
//     Claude E·R·C·Q / Gemini field-2——signature.ProviderForSignature）才映射
//     reasoning_content + reasoning_details，否则整块丢弃（表的 fallback，绝不
//     凭空把未判定的思考塞进请求体）；
//   - tool_use 块签名（W42 ②）：Gemini 形态或官方占位符 →
//     reasoning_details[{type:reasoning.encrypted, data, id=tool_call 线 id,
//     format:google-gemini-v1, index}]（roo-code 实证的 Gemini 3 工具签名通道）；
//     不可识别/非 Gemini → 丢弃（红线）；
//   - redacted_thinking（W42 ①）：载荷 → reasoning_details[{type:reasoning.encrypted,
//     data, format:anthropic-claude-v1, index}]（assistant 消息；中间腿例外——见下）；
//   - 枢纽中间腿（内部腿，W42 ③）：思考/tool 签名与响应面同表（载体化后中间线
//     可表达；hub 腿消费者是本仓解析器）；redacted 载荷仍丢弃（gemini 出口无
//     redacted 载体，保真会断链）；
//   - 非 assistant 消息（user/system 的 thinking）：整块丢弃（安全规则：防注入）。
//
// 以上都不报错：此前 default 分支对 thinking 报 ErrUnsupportedBlock，非流式整条链
// 会因思考断（claude→openai / claude→gemini 经枢纽的非流式响应与请求）。
// renderMessage 渲染一条消息。seed = 响应 id（合成工具 id 的派生源；请求面传空）。
func renderMessage(m ir.Message, mode renderMode, seed string) (json.RawMessage, error) {
	out := map[string]json.RawMessage{"role": mustMarshal(string(m.Role))}
	if m.Name != "" {
		out["name"] = mustMarshal(m.Name)
	}

	type part struct {
		isText bool
		text   string
		raw    json.RawMessage
	}
	var parts []part
	var toolCalls []json.RawMessage
	var toolResult *ir.ToolResult
	var refusal *string
	var reasoning []string
	// reasoning_details 条目（W41 载体；写面）。index = 本消息内思考块序
	//（消费端按 index 分组——roo-code consolidateReasoningDetails）。
	var details []json.RawMessage

	for i, b := range m.Blocks {
		switch b.Kind {
		case ir.BlockText:
			parts = append(parts, part{isText: true, text: b.Text})
		case ir.BlockThinking:
			// 签名载体判定（W41）：形态可识别 → 进 reasoning_details（format 由
			// 判定结果映射）；不可识别 → 不写（红线）。落地与否在分支后统一计数
			//（SignatureDropped = 签名未落地的块）。
			detailSig, detailFormat := "", ""
			if b.Signature != "" {
				if f, ok := reasoningDetailFormat(b.Signature); ok {
					detailSig, detailFormat = b.Signature, f
				}
			}
			sigLanded := false
			switch {
			case m.Role != ir.RoleAssistant:
				// 安全规则（表 §7.1）：只有 assistant 消息的 thinking 可映射；
				// user/system 里的 thinking 一律忽略（防注入）。
				mode.dropThinking()
			case b.Text == "" && detailSig == "":
				// 空文本且无可识别签名：无内容可承载 → 丢弃。
				mode.dropThinking()
			case mode.response || mode.hubLeg:
				// 响应面 / 枢纽中间腿（W42 ③：内部腿与响应面同表——reasoning_details
				// 是可往返的内部线成员，hub 腿的消费者是本仓解析器，出口腿各自按
				// provider 门落地；此前「整块丢弃」让 claude→gemini 等两步链静默
				// 丢思考文本。见报告 §hub 腿评估）。
				if b.Text != "" {
					reasoning = append(reasoning, b.Text)
					mode.countReasoning()
				}
				details = append(details, reasoningTextDetail(b.Text, detailSig, detailFormat, len(details)))
				sigLanded = detailSig != ""
			case mode.request:
				// 请求面（回传轮）：签名形态判定——可识别 → 文本 + 载体重放；
				// 不可识别/空 → 整块丢弃（表 §7.1 回传轮行 fallback 的载体化：
				// 绝不凭空把未判定的思考塞进请求体）。
				if detailSig == "" {
					mode.dropThinking()
					break
				}
				if b.Text != "" {
					reasoning = append(reasoning, b.Text)
					mode.countReasoning()
				}
				details = append(details, reasoningTextDetail(b.Text, detailSig, detailFormat, len(details)))
				sigLanded = true
			default:
				mode.dropThinking()
			}
			if b.Signature != "" && !sigLanded {
				// 签名未落地（不可识别 / 非 assistant / 整块丢弃）：W5-19 条目 5
				// 的对拍计数（口径 = 流式面 SignatureDropped）。
				mode.dropSignature()
			}
		case ir.BlockRedactedThinking:
			// W42 ①：加密思考载荷 ↔ reasoning.encrypted{data, format:anthropic-claude-v1}
			//（W41 §4 残项 1 的自然映射）。写面只在 assistant 消息、非 hub 腿
			//（中间腿载荷会到达 gemini 出口——该出口无 redacted 载体，会断链；
			// 见报告 §hub 腿评估）；载荷空 → 丢弃。
			if m.Role != ir.RoleAssistant || mode.hubLeg || b.Text == "" {
				mode.dropThinking()
				break
			}
			details = append(details, reasoningEncryptedDetail(b.Text, "", reasoningFormatClaude, len(details)))
		case ir.BlockRefusal:
			s := b.Text
			refusal = &s
		case ir.BlockToolUse, ir.BlockServerToolUse:
			if b.ToolCall == nil {
				return nil, fmt.Errorf("%w: block %d: tool_use without ToolCall", ErrBadIR, i)
			}
			args := b.ToolCall.Arguments
			if args == "" {
				args = "{}" // 空参数出口修复（cc-wire-contract 空参数门）
			}
			// 出口 id 统一卫生（hygiene）——签名条目的 id 必须与 tool_calls[].id
			// 逐字相同（②消费端按 id 配对：roo-code sanitizeGeminiMessages）。
			wireID := hygiene.WireToolID("call", b.ToolCall.ID, seed, i)
			tc := map[string]json.RawMessage{
				"id":   mustMarshal(wireID),
				"type": mustMarshal("function"),
				"function": mustMarshal(map[string]json.RawMessage{
					"name":      mustMarshal(b.ToolCall.Name),
					"arguments": mustMarshal(args),
				}),
			}
			toolCalls = append(toolCalls, mustMarshal(tc))
			// W42 ②：Gemini 3 工具签名经 reasoning.encrypted{data, id=tool_call 线
			// id, format:google-gemini-v1} 回传（roo-code 实证；官方占位符同款）。
			// 形态不可识别/非 Gemini → 丢弃 + 计数（红线：绝不写未判定签名）。
			if b.Signature != "" {
				if format, ok := reasoningToolSignature(b.Signature); ok && m.Role == ir.RoleAssistant {
					details = append(details, reasoningEncryptedDetail(b.Signature, wireID, format, len(details)))
				} else {
					mode.dropSignature()
				}
			}
		case ir.BlockToolResult:
			if b.ToolResult == nil {
				return nil, fmt.Errorf("%w: block %d: tool_result without payload", ErrBadIR, i)
			}
			if toolResult != nil {
				return nil, fmt.Errorf("%w: multiple tool_result blocks in one message", ErrToolResultShape)
			}
			toolResult = b.ToolResult
		case ir.BlockImage:
			if b.Image == nil {
				return nil, fmt.Errorf("%w: block %d: image without payload", ErrBadIR, i)
			}
			url := b.Image.URL
			if url == "" && b.Image.Data != "" {
				url = "data:" + b.Image.MediaType + ";base64," + b.Image.Data
			}
			iu := map[string]json.RawMessage{"url": mustMarshal(url)}
			if b.Image.Detail != "" {
				iu["detail"] = mustMarshal(b.Image.Detail)
			}
			parts = append(parts, part{raw: mustMarshal(map[string]json.RawMessage{
				"type":      mustMarshal("image_url"),
				"image_url": mustMarshal(iu),
			})})
		case ir.BlockUnknown:
			if len(b.Raw) == 0 {
				return nil, fmt.Errorf("%w: block %d: unknown block without Raw", ErrBadIR, i)
			}
			parts = append(parts, part{raw: b.Raw})
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnsupportedBlock, b.Kind)
		}
	}

	if m.Role == ir.RoleTool {
		if toolResult == nil {
			return nil, fmt.Errorf("%w: tool message without tool_result block", ErrToolResultShape)
		}
		if len(parts) > 0 || refusal != nil || len(toolCalls) > 0 {
			return nil, fmt.Errorf("%w: tool message carries non-tool_result content", ErrToolResultShape)
		}
		if toolResult.IsError {
			return nil, fmt.Errorf("%w: tool_call_id %q", ErrToolErrorFlag, toolResult.CallID)
		}
		out["tool_call_id"] = mustMarshal(hygiene.SanitizeToolID(toolResult.CallID))
		out["content"] = mustMarshal(toolResult.Content)
	} else {
		if toolResult != nil {
			return nil, fmt.Errorf("%w: tool_result block outside tool message", ErrToolResultShape)
		}
		switch {
		case len(parts) == 0:
			out["content"] = json.RawMessage("null")
		case len(parts) == 1 && parts[0].isText:
			out["content"] = mustMarshal(parts[0].text)
		default:
			arr := make([]json.RawMessage, 0, len(parts))
			for _, p := range parts {
				if p.isText {
					arr = append(arr, mustMarshal(map[string]json.RawMessage{
						"type": mustMarshal("text"),
						"text": mustMarshal(p.text),
					}))
				} else {
					arr = append(arr, p.raw)
				}
			}
			out["content"] = mustMarshal(arr)
		}
	}
	if refusal != nil {
		out["refusal"] = mustMarshal(*refusal)
	}
	if len(toolCalls) > 0 {
		out["tool_calls"] = mustMarshal(toolCalls)
	}
	if len(reasoning) > 0 {
		// 多思考块拼接（表：拼接全部 thought parts；分隔符 [INFERENCE]——CPA 的拼接
		// 分隔符未核实，取可读的换行）。文本兼容通道（W41 起与 reasoning_details
		// 并行——OpenRouter 同款双通道）。
		out["reasoning_content"] = mustMarshal(strings.Join(reasoning, "\n"))
	}
	if len(details) > 0 {
		// W41 结构化载体（text+signature+format+index）；reasoning_content 保留
		// 文本兼容。签名只写形态可识别者（红线）。
		out["reasoning_details"] = mustMarshal(details)
	}
	mergeExtras(out, m.Extra)
	return mustMarshal(out), nil
}

// renderToolDef 渲染 tools[] 项。
func renderToolDef(td ir.ToolDef) (json.RawMessage, error) {
	fn := map[string]json.RawMessage{"name": mustMarshal(td.Name)}
	if td.Description != "" {
		fn["description"] = mustMarshal(td.Description)
	}
	if len(td.InputSchema) > 0 && !isJSONNull(td.InputSchema) {
		fn["parameters"] = td.InputSchema
	}
	if td.Strict != nil {
		fn["strict"] = mustMarshal(*td.Strict)
	}
	mergeExtras(fn, pruneForeignToolExtras(td.Extra))
	return mustMarshal(map[string]json.RawMessage{
		"type":     mustMarshal("function"),
		"function": mustMarshal(fn),
	}), nil
}

// claudeOriginToolExtras claude 工具定义级独有字段（anthropic-messages §工具定义：
// strict / eager_input_streaming / cache_control / defer_loading；strict 已建模，
// 其余三个未建模 → 落在 ToolDef.Extra）：跨格式转换（claude→openai_chat）时必须
// 显式丢弃——OpenAI function 对象不认识这些键（透传 = 上游 400）。
var claudeOriginToolExtras = map[string]bool{
	"cache_control":         true,
	"defer_loading":         true,
	"eager_input_streaming": true,
}

// geminiOriginToolExtras gemini_generate 工具定义级独有、OpenAI function 对象不
// 认识的字段（官方 functionDeclarations 字段面 = name/description/parameters/
// parametersJsonSchema/response/responseJsonSchema，gemini-wire §tools[]；前四者已
// 建模，其余两个落 ToolDef.Extra —— gemini ParseRequest 的 extrasOf 排除表）：
// 跨格式转换（gemini→openai_chat，含经枢纽两步的中间腿）时必须显式丢弃——OpenAI
// 不认识这两个键，透传 = 上游 400（W5-8 §缺口 1 / W5-11 §缺口 3；与 claude 出口的
// 同名表对称）。
var geminiOriginToolExtras = map[string]bool{
	"response":           true, // Gemini 函数返回声明（Schema）
	"responseJsonSchema": true, // 同上（JSON Schema 形态）
}

// pruneForeignToolExtras 剔除工具定义级的外来源独有键
// （见 claudeOriginToolExtras / geminiOriginToolExtras）。
func pruneForeignToolExtras(extra map[string]json.RawMessage) map[string]json.RawMessage {
	if len(extra) == 0 {
		return extra
	}
	out := make(map[string]json.RawMessage, len(extra))
	for k, v := range extra {
		if claudeOriginToolExtras[k] || geminiOriginToolExtras[k] {
			continue
		}
		out[k] = v
	}
	return out
}

// renderUsage 渲染 usage 对象：建模字段 + Extra 原文；当 Extra 无 details 且
// 派生值非零时合成 details（跨格式构造场景；保真回传时 Extra 原文优先）。
func renderUsage(u *ir.Usage) (json.RawMessage, error) {
	m := map[string]json.RawMessage{
		"prompt_tokens":     mustMarshal(u.Input),
		"completion_tokens": mustMarshal(u.Output),
		"total_tokens":      mustMarshal(u.Total),
	}
	// Anthropic 独有 usage 键剥离（anthropic-conversion-table §6.5「剥污染」行：
	// 出 OpenAI 剥 Anthropic 独有键，避免 claude→openai 转换污染 OpenAI 口径）。
	for k, v := range u.Extra {
		if anthropicOnlyUsageKeys[k] {
			continue
		}
		if _, exists := m[k]; !exists {
			m[k] = v
		}
	}
	if _, ok := m["prompt_tokens_details"]; !ok && u.CacheRead != 0 {
		m["prompt_tokens_details"] = mustMarshal(map[string]json.RawMessage{
			"cached_tokens": mustMarshal(u.CacheRead),
		})
	}
	if _, ok := m["completion_tokens_details"]; !ok && u.Reasoning != 0 {
		m["completion_tokens_details"] = mustMarshal(map[string]json.RawMessage{
			"reasoning_tokens": mustMarshal(u.Reasoning),
		})
	}
	return mustMarshal(m), nil
}

func marshalFinishReason(r ir.StopReason) json.RawMessage {
	if r == "" {
		return json.RawMessage("null")
	}
	return mustMarshal(finishReasonFor(r))
}

// finishReasonFor 把 IR 四值映射回 OpenAI finish_reason（omp 表反向；
// StopError → content_filter 为跨格式约定值）。
//
// claude.ai `stop_sequence`（含配额截断 `usage_limit`，IR StopSequence 保真）
// **有意压平为 `stop`**（W43-3 §4.3 R1 / topics/anthropic-conversion-table
// 「停止序列值」行：OpenAI finish_reason 无对应字段，序列值不可塞进 finish_reason
// 文本）。截断信号的可消费面 = 流末 usage（include_usage 时逐字节下发
// `claude_web_message_limit{type:"exceeded_limit",…}`）+ IR StopSequence
// （诊断/观测）。Responses 出口同款（statusFor：StopStop → completed）。
func finishReasonFor(r ir.StopReason) string {
	switch r {
	case ir.StopStop:
		return "stop"
	case ir.StopLength:
		return "length"
	case ir.StopToolUse:
		return "tool_calls"
	case ir.StopError:
		return "content_filter"
	default:
		return string(r) // 未知/透传值原样回写
	}
}

func isToolKind(k ir.BlockKind) bool {
	return k == ir.BlockToolUse || k == ir.BlockServerToolUse
}

// mergeExtras 把 Extra 原文并入输出对象；建模字段优先（防冲突污染）。
func mergeExtras(m map[string]json.RawMessage, extra map[string]json.RawMessage) {
	for k, v := range extra {
		if _, exists := m[k]; !exists {
			m[k] = v
		}
	}
}

// marshalJSON 序列化（关闭 HTML 转义——wire 保真，避免 < > & 被转成 \u003c 形态）。
func marshalJSON(v any) (json.RawMessage, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	b := buf.Bytes()
	if n := len(b); n > 0 && b[n-1] == '\n' {
		b = b[:n-1]
	}
	return append(json.RawMessage(nil), b...), nil
}

func marshalBytes(v any) ([]byte, error) {
	b, err := marshalJSON(v)
	if err != nil {
		return nil, err
	}
	return []byte(b), nil
}

// mustMarshal 仅用于不可能失败的内部序列化（string/int64/map[RawMessage]）。
func mustMarshal(v any) json.RawMessage {
	b, err := marshalJSON(v)
	if err != nil {
		panic(err)
	}
	return b
}
