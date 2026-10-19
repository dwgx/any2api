package openai_chat

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/signature"
)

// ---------------------------------------------------------------------------
// 请求：线格式 → IR
// ---------------------------------------------------------------------------

// ParseRequest 解析 OpenAI Chat Completions 请求体。
// 宽容读取（缺省=不变、未知=忽略），未建模顶层/消息字段进 Extra 保真旁路。
func ParseRequest(data []byte) (*ir.Request, error) {
	var w requestWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	if len(w.Messages) == 0 {
		return nil, fmt.Errorf("%w: no messages", ErrBadRequest)
	}
	// stream_options 只能与 stream=true 同用（openai-wire 陷阱：vLLM 显式拒绝）。
	if w.StreamOptions != nil && !w.Stream {
		return nil, fmt.Errorf("%w", ErrStreamOptionsWithoutStream)
	}

	req := &ir.Request{
		Model:      w.Model,
		Stream:     w.Stream,
		ToolChoice: w.ToolChoice,
	}
	if w.StreamOptions != nil {
		req.StreamOptions = &ir.StreamOptions{
			IncludeUsage:       w.StreamOptions.IncludeUsage != nil && *w.StreamOptions.IncludeUsage,
			IncludeObfuscation: w.StreamOptions.IncludeObfuscation,
		}
	}
	for i, raw := range w.Messages {
		m, err := parseMessage(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: message %d: %w", ErrBadRequest, i, err)
		}
		req.Messages = append(req.Messages, m)
	}
	for i, raw := range w.Tools {
		td, err := parseToolDef(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: tool %d: %w", ErrBadRequest, i, err)
		}
		req.Tools = append(req.Tools, td)
	}
	req.Extra = extrasOf(data, "model", "messages", "tools", "tool_choice", "stream", "stream_options")
	return req, nil
}

// ---------------------------------------------------------------------------
// 非流式响应：线格式 → IR
// ---------------------------------------------------------------------------

// ParseResponse 解析非流式 chat.completion 响应体（choice 0 语义；
// 多 choice 显式报错 ErrUnsupportedMultiChoice——绝不静默丢弃）。
func ParseResponse(data []byte) (*ir.Response, error) {
	var w responseWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadResponse, err)
	}
	if w.Object != "" && w.Object != objectChatCompletion {
		return nil, fmt.Errorf("%w: %q", ErrUnexpectedObject, w.Object)
	}
	switch {
	case len(w.Choices) == 0:
		return nil, fmt.Errorf("%w: no choices", ErrBadResponse)
	case len(w.Choices) > 1:
		return nil, fmt.Errorf("%w: %d choices", ErrUnsupportedMultiChoice, len(w.Choices))
	}
	var cw responseChoiceWire
	if err := json.Unmarshal(w.Choices[0], &cw); err != nil {
		return nil, fmt.Errorf("%w: choice: %w", ErrBadResponse, err)
	}
	if cw.Index != 0 {
		return nil, fmt.Errorf("%w: choice index %d", ErrUnsupportedMultiChoice, cw.Index)
	}
	msg, err := parseMessage(cw.Message)
	if err != nil {
		return nil, fmt.Errorf("%w: message: %w", ErrBadResponse, err)
	}
	if msg.Role != ir.RoleAssistant {
		return nil, fmt.Errorf("%w: response message role %q", ErrUnexpectedRole, msg.Role)
	}
	// 上游推理扩展字段（`message.reasoning_content`）→ IR 思考块（置首）由
	// parseMessage 统一处理（请求/响应同口径；W6-1 前响应面在此单独提取）。
	resp := &ir.Response{
		ID:      w.ID,
		Model:   w.Model,
		Created: w.Created,
		Message: msg,
	}
	if cw.FinishReason != nil && *cw.FinishReason != "" {
		resp.StopReason = mapFinishReason(*cw.FinishReason)
	}
	if u, err := parseUsage(w.Usage); err != nil {
		return nil, fmt.Errorf("%w: usage: %w", ErrBadResponse, err)
	} else {
		resp.Usage = u
	}
	resp.Extra = extrasOf(data, "id", "object", "created", "model", "choices", "usage")
	return resp, nil
}

// ---------------------------------------------------------------------------
// 错误：线格式 → IR（错误默认不透传上游 body；Raw 仅供诊断）
// ---------------------------------------------------------------------------

// ParseError 解析 OpenAI 错误信封 {"error":{message,type,param,code}}（openai-wire §4.7）。
// 非 JSON / 非标准 body 不报错，归一为泛型 api_error（Raw 保留原文供诊断）。
func ParseError(body []byte) *ir.Error {
	var env struct {
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil || len(env.Error) == 0 || isJSONNull(env.Error) {
		// W5-22 条目 4：非标准错误体走兜底提取（one-api 7 路径表——msg/err/
		// error_msg/header.message/response.error.message 等形态的最小信息抢救）。
		return ir.SalvageError(body, "openai")
	}
	return parseErrorObject(env.Error)
}

// ---------------------------------------------------------------------------
// 流式：SSE data 载荷 → IR 事件
// ---------------------------------------------------------------------------

// StreamParser OpenAI Chat 流式解析状态机。用法：对每个 SSE data 载荷调用
// Feed；流结束（EOF 或 [DONE]）后调用 Finish 取未产出的终态事件。
//
// 契约（openai-wire §4.1-4.3 / stream-mechanics §4.6-4.7）：
//   - [DONE] 是字符串哨兵（startswith 容忍，对齐 openai-python）；哨兵之后不应再有数据。
//   - 流内错误帧后官方 SDK 即终止，但 vLLM 形态仍发 [DONE]——两种模式都容忍。
//   - usage 可能缺失（流中断 / 未设 include_usage）；缺失不是错误。
//   - 无 finish_reason 的收尾（[DONE] 或 EOF）判失败（CPA 502 判据）。
type StreamParser struct {
	started    bool
	finishSeen bool
	errorSeen  bool
	doneSeen   bool
	stopReason ir.StopReason

	nextBlock int
	textIdx   int
	refIdx    int
	thinkIdx  int
	open      []openBlock
	tools     map[int]*toolStreamState
	// toolByID = 线 id → 流状态（W70 R03）。tools 的 index 槽位只指向「当前」
	// 状态（同 index 不同 id 的冲突块被新块顶替），按 id 的稳定寻址（续传片
	// 路由、工具签名配对）必须走这张表——它含冲突后的全部在册块，不丢任何一个。
	toolByID map[string]*toolStreamState

	// W42 ②：tool 签名的 encrypted 缓冲。toolSig = tool 块 index → 已到达的签名
	// 载荷（块尾一条下发——与写面「块尾完整条目」同口径）；pendingToolSig =
	// tool_call id → 载荷（encrypted 条目先于 tool_call 到达时的待配缓冲，roo-code
	// 流序实证：reasoning_details 先于 tool_calls）。
	toolSig        map[int]string
	pendingToolSig map[string]string
	// W42 ①：redacted 载荷块（无 tool 配对的 Claude 族 encrypted）状态。
	// redIdx = 打开的块 index（-1 = 无）；redDetail = 该块累积的 details index
	//（同 index 的条目拼接成一条载荷——消费端 consolidate 口径）。
	redIdx    int
	redDetail int
}

type openBlock struct {
	index int
	kind  ir.BlockKind
}

type toolStreamState struct {
	blockIndex int
	id         string
	name       string
}

// NewStreamParser 创建流式解析状态机。
func NewStreamParser() *StreamParser {
	return &StreamParser{
		textIdx: -1, refIdx: -1, thinkIdx: -1, redIdx: -1,
		tools:          map[int]*toolStreamState{},
		toolByID:       map[string]*toolStreamState{},
		toolSig:        map[int]string{},
		pendingToolSig: map[string]string{},
	}
}

// Feed 处理一个 SSE data 载荷（多行 data 已按 "\n" 连接）。
// 返回本次新增的 IR 事件；协议级违规返回包装后的哨兵错误。
func (p *StreamParser) Feed(payload []byte) ([]ir.StreamEvent, error) {
	s := strings.TrimSpace(string(payload))
	if s == "" {
		return nil, nil
	}
	if isDoneSentinel(s) {
		if p.doneSeen {
			return nil, fmt.Errorf("%w: duplicate [DONE]", ErrDataAfterDone)
		}
		p.doneSeen = true
		if p.errorSeen {
			return nil, nil // vLLM 形态：错误后仍发 [DONE]——容忍
		}
		if !p.finishSeen {
			return []ir.StreamEvent{terminalTransportError("stream ended ([DONE]) without finish_reason")}, nil
		}
		return []ir.StreamEvent{
			{Kind: ir.EventMessageStop},
			{Kind: ir.EventDone, Reason: p.stopReason},
		}, nil
	}
	if p.doneSeen {
		return nil, fmt.Errorf("%w: %s", ErrDataAfterDone, truncate(s, 120))
	}
	if p.errorSeen {
		return nil, fmt.Errorf("%w: %s", ErrDataAfterError, truncate(s, 120))
	}

	var ch chunkWire
	if err := json.Unmarshal([]byte(s), &ch); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadChunk, err)
	}
	// 流内错误帧：{ "error": {...} }（字段平铺的 OpenAI 错误对象）。
	if len(ch.Error) > 0 && !isJSONNull(ch.Error) {
		p.errorSeen = true
		return []ir.StreamEvent{
			{Kind: ir.EventError, ErrorReason: ir.ErrorUpstream, Error: parseErrorObject(ch.Error)},
		}, nil
	}
	// 空帧（{} / null / choices:[] 且无 usage）：容忍忽略，不产事件也不开流。
	if len(ch.Choices) == 0 && (len(ch.Usage) == 0 || isJSONNull(ch.Usage)) {
		return nil, nil
	}
	if ch.Object != "" && ch.Object != objectChatCompletionChunk {
		return nil, fmt.Errorf("%w: %q", ErrUnexpectedObject, ch.Object)
	}
	if len(ch.Choices) > 1 {
		return nil, fmt.Errorf("%w: %d choices", ErrUnsupportedMultiChoice, len(ch.Choices))
	}

	var events []ir.StreamEvent
	if !p.started {
		p.started = true
		events = append(events, ir.StreamEvent{
			Kind: ir.EventStart, MessageID: ch.ID, Model: ch.Model, Created: ch.Created,
		})
	}

	if len(ch.Choices) == 1 {
		ch0 := ch.Choices[0]
		if ch0.Index != 0 {
			return nil, fmt.Errorf("%w: choice index %d", ErrUnsupportedMultiChoice, ch0.Index)
		}
		if ch0.Delta.Role != "" && ch0.Delta.Role != string(ir.RoleAssistant) {
			return nil, fmt.Errorf("%w: %q", ErrUnexpectedRole, ch0.Delta.Role)
		}
		// W41 结构化载体（reasoning_details）+ W42 扩展：reasoning.text 条目的
		// 文本/签名；reasoning.summary 文本（text 缺省回退）；reasoning.encrypted
		// （id 与 tool 配对 → 该块签名缓冲；无配对的 Claude 族载荷 → redacted 块）。
		// 同帧既有载体文本又有 reasoning_content 时以载体为准（OpenRouter 双通道
		// 防重复）；载体只带签名时文本仍可走 reasoning_content / `reasoning` 别名。
		// 签名逐条按形态校验——不可识别丢弃（红线），可识别 → SignatureDelta
		//（下游按各自载体落地：claude thinking.signature / responses
		// encrypted_content / …）。
		detailText, detailSummary, detailSig := "", "", ""
		for _, d := range reasoningDetailsOf(ch0.Delta.ReasoningDetails) {
			switch d.Type {
			case "reasoning.text":
				detailText += d.Text
				if d.Signature != "" {
					if _, ok := signature.ProviderForSignature(d.Signature); ok {
						detailSig = d.Signature
					} else {
						detailSig = "" // 不可识别签名不得进 IR（红线）；后续条目可覆盖。
					}
				}
			case "reasoning.summary":
				detailSummary += d.Summary
			case "reasoning.encrypted":
				events = append(events, p.feedEncryptedDetail(d)...)
			}
		}
		thinkText := detailText
		if thinkText == "" {
			thinkText = detailSummary // summary 回退（text 优先——consolidate 口径）。
		}
		if thinkText == "" && ch0.Delta.ReasoningContent != nil {
			thinkText = *ch0.Delta.ReasoningContent
		}
		if thinkText == "" && ch0.Delta.Reasoning != nil {
			thinkText = *ch0.Delta.Reasoning // W42：`reasoning` 别名（末位回退）。
		}
		if thinkText != "" || detailSig != "" {
			// 上游推理扩展字段（DeepSeek/Kimi/Zhipu 等 OpenAI 兼容生态约定；
			// longtail-matrix §4.3 / anthropic-conversion-table §流式「思考」行
			// `delta.reasoning_content` → thinking 块）：映射为 IR 思考块，出口按
			// 各自载体落地（claude→thinking / responses→reasoning item /
			// gemini→thought part / openai→reasoning_content+reasoning_details）。
			// 上游帧序异常（正文先于思考）时按到达顺序建块（IR 事件不可变；
			// claude wire 无块序守卫）。
			if p.thinkIdx < 0 {
				// 块切换纪律：thinking 开块前先收尾开着的 redacted 块。
				events = append(events, p.closeRedacted()...)
				p.thinkIdx = p.nextBlock
				p.nextBlock++
				p.open = append(p.open, openBlock{p.thinkIdx, ir.BlockThinking})
				events = append(events, ir.StreamEvent{Kind: ir.EventBlockStart, BlockIndex: p.thinkIdx, BlockKind: ir.BlockThinking})
			}
			if thinkText != "" {
				events = append(events, ir.StreamEvent{
					Kind: ir.EventBlockDelta, BlockIndex: p.thinkIdx, BlockKind: ir.BlockThinking,
					TextDelta: thinkText,
				})
			}
			if detailSig != "" {
				events = append(events, ir.StreamEvent{
					Kind: ir.EventBlockDelta, BlockIndex: p.thinkIdx, BlockKind: ir.BlockThinking,
					SignatureDelta: detailSig,
				})
			}
		}
		if ch0.Delta.Content != nil && *ch0.Delta.Content != "" {
			// 思考块先收尾（正文开始 = 思考结束）：块序与真实上游一致——
			// Anthropic 块严格顺序（stop 先于下一块 start）、gemini thought part
			// 先于 text part（gemini 出口按块 stop 落 thought part）。后续再来
			// 的 reasoning 增量会开新思考块（thinkIdx 复位）。
			events = append(events, p.closeAuxBlocks()...)
			if p.textIdx < 0 {
				p.textIdx = p.nextBlock
				p.nextBlock++
				p.open = append(p.open, openBlock{p.textIdx, ir.BlockText})
				events = append(events, ir.StreamEvent{Kind: ir.EventBlockStart, BlockIndex: p.textIdx, BlockKind: ir.BlockText})
			}
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockDelta, BlockIndex: p.textIdx, BlockKind: ir.BlockText,
				TextDelta: *ch0.Delta.Content,
			})
		}
		if ch0.Delta.Refusal != nil && *ch0.Delta.Refusal != "" {
			if p.refIdx < 0 {
				p.refIdx = p.nextBlock
				p.nextBlock++
				p.open = append(p.open, openBlock{p.refIdx, ir.BlockRefusal})
				events = append(events, ir.StreamEvent{Kind: ir.EventBlockStart, BlockIndex: p.refIdx, BlockKind: ir.BlockRefusal})
			}
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockDelta, BlockIndex: p.refIdx, BlockKind: ir.BlockRefusal,
				TextDelta: *ch0.Delta.Refusal,
			})
		}
		for _, tc := range ch0.Delta.ToolCalls {
			if tc.Index == nil {
				return nil, fmt.Errorf("%w: tool_calls entry without index", ErrBadChunk)
			}
			// 思考块先收尾（工具调用开始 = 思考结束；与正文同理）。
			events = append(events, p.closeAuxBlocks()...)
			st, fresh := p.toolStateFor(tc)
			if fresh {
				events = append(events, ir.StreamEvent{
					Kind: ir.EventBlockStart, BlockIndex: st.blockIndex, BlockKind: ir.BlockToolUse,
					ToolCallID: st.id, ToolName: st.name,
				})
			}
			if tc.Function != nil && tc.Function.Arguments != "" {
				events = append(events, ir.StreamEvent{
					Kind: ir.EventBlockDelta, BlockIndex: st.blockIndex, BlockKind: ir.BlockToolUse,
					ArgsDelta: tc.Function.Arguments,
				})
			}
		}
		if fr := ch0.FinishReason; fr != nil && *fr != "" {
			if !p.finishSeen {
				p.finishSeen = true
				p.stopReason = mapFinishReason(*fr)
				events = append(events, p.closeBlocks()...)
				events = append(events, ir.StreamEvent{Kind: ir.EventMessageDelta, StopReason: p.stopReason})
			}
			// 重复 finish_reason：容忍忽略（个别 provider 重发）。
		}
	}

	// usage：独立 usage chunk（choices:[]）或与 finish 同 chunk 都可能。
	if len(ch.Usage) > 0 && !isJSONNull(ch.Usage) {
		u, err := parseUsage(ch.Usage)
		if err != nil {
			return nil, fmt.Errorf("%w: usage: %v", ErrBadChunk, err)
		}
		if u != nil {
			events = append(events, ir.StreamEvent{Kind: ir.EventMessageDelta, Usage: u})
		}
	}
	// 顶层元数据三键保真（W5-30）：service_tier/system_fingerprint/moderation
	// 原样落入本帧产生的每个事件（null 视同缺省，与渲染侧同判——IR 不动点）。
	// obfuscation 有意忽略（chunkWire 注释）。纯元数据帧（choices:[] 且无 usage）
	// 不产事件、无载体——边界见 W5-30 报告。
	if hasRaw(ch.ServiceTier) || hasRaw(ch.SystemFingerprint) || hasRaw(ch.Moderation) {
		for i := range events {
			if hasRaw(ch.ServiceTier) {
				events[i].ServiceTier = ch.ServiceTier
			}
			if hasRaw(ch.SystemFingerprint) {
				events[i].SystemFingerprint = ch.SystemFingerprint
			}
			if hasRaw(ch.Moderation) {
				events[i].Moderation = ch.Moderation
			}
		}
	}
	return events, nil
}

// toolStateFor 解析一条 tool_calls 条目应落的流状态（W70 R03：同 index 不同非空
// id 不再按 index 合并——每个线 id 一个稳定状态，两个调用都保留）。
//
// 路由规则（openai-wire §4.2 的宽容扩展）：
//   - id 非空：先按 id 归位（续传片按 id 寻址，即使 provider 漂移 index 也回原块）；
//     id 未知时看 index 槽位——空槽位建新块，空 id 槽位就地启用（迟到补全，既有
//     行为：id/name 只在首片），**已被不同非空 id 占用的槽位 = 冲突**：建新块
//     （新 blockIndex；旧块留在 p.open 不动、按 id 仍可寻址），槽位指向新当前状态；
//   - id 空：index 槽位（纯增量片，既有行为——不做身份判断）。
//
// fresh = 本次是否新建状态（调用方据此发 block_start，每个状态恰好一次）。
func (p *StreamParser) toolStateFor(tc toolCallDeltaWire) (st *toolStreamState, fresh bool) {
	idx := *tc.Index
	if tc.ID != "" {
		if st, ok := p.toolByID[tc.ID]; ok {
			fillToolMeta(st, tc)
			return st, false
		}
		if st := p.tools[idx]; st != nil && (st.id == "" || st.id == tc.ID) {
			// 迟到 id 补全（既有行为）；st.id == tc.ID 为在册 id 的保险回退
			//（正常路径已被上方 toolByID 命中——绝不重复建块）。
			if st.id == "" {
				st.id = tc.ID
				p.toolByID[tc.ID] = st
				p.attachPendingToolSig(st)
			}
			fillToolMeta(st, tc)
			return st, false
		}
		// W70 R03 冲突：同 index、不同非空 id —— 落到下方新建块，旧块保留。
	} else if st := p.tools[idx]; st != nil {
		// id 空：纯增量片落 index 当前槽位（既有行为）。
		fillToolMeta(st, tc)
		return st, false
	}
	// 新建状态：新 index（首见）或 W70 R03 冲突（旧块仍在 p.open）。
	st = &toolStreamState{blockIndex: p.nextBlock}
	p.nextBlock++
	if tc.ID != "" {
		st.id = tc.ID
		p.toolByID[tc.ID] = st
	}
	p.tools[idx] = st
	p.open = append(p.open, openBlock{st.blockIndex, ir.BlockToolUse})
	fillToolMeta(st, tc)
	p.attachPendingToolSig(st)
	return st, true
}

// fillToolMeta 幂等补全 name（id 绑定块身份，补全/登记在 toolStateFor 里做）。
func fillToolMeta(st *toolStreamState, tc toolCallDeltaWire) {
	if tc.Function != nil && tc.Function.Name != "" && st.name == "" {
		st.name = tc.Function.Name
	}
}

// attachPendingToolSig W42 ②：encrypted 条目先于 tool_call 到达（roo-code 流序）时，
// 待配缓冲在 id 已知后附着到该块（块尾统一下发——见 closeBlocks）。幂等：
// 无待配载荷 / id 未知 = 无操作。
func (p *StreamParser) attachPendingToolSig(st *toolStreamState) {
	if st.id == "" {
		return
	}
	if sig, ok := p.pendingToolSig[st.id]; ok {
		p.toolSig[st.blockIndex] += sig
		delete(p.pendingToolSig, st.id)
	}
}

// closeThinking 思考块收尾：发出 BlockStop 并从 open 集合摘除（正文/工具调用
// 开始或流结束时调用）。幂等——无开着的思考块 = 无事件；重复调用安全。
func (p *StreamParser) closeThinking() []ir.StreamEvent {
	if p.thinkIdx < 0 {
		return nil
	}
	idx := p.thinkIdx
	p.thinkIdx = -1
	for i, b := range p.open {
		if b.index == idx {
			p.open = append(p.open[:i], p.open[i+1:]...)
			break
		}
	}
	return []ir.StreamEvent{{Kind: ir.EventBlockStop, BlockIndex: idx, BlockKind: ir.BlockThinking}}
}

// closeRedacted W42 ①：redacted 载荷块收尾（与 closeThinking 同款：新块开始或
// 流结束时调用）。幂等。
func (p *StreamParser) closeRedacted() []ir.StreamEvent {
	if p.redIdx < 0 {
		return nil
	}
	idx := p.redIdx
	p.redIdx = -1
	for i, b := range p.open {
		if b.index == idx {
			p.open = append(p.open[:i], p.open[i+1:]...)
			break
		}
	}
	return []ir.StreamEvent{{Kind: ir.EventBlockStop, BlockIndex: idx, BlockKind: ir.BlockRedactedThinking}}
}

// closeAuxBlocks 收尾开着的 thinking / redacted 块（正文/工具调用开始或流结束
// 时调用——claude wire 块严格顺序：stop 先于下一块 start）。幂等。
func (p *StreamParser) closeAuxBlocks() []ir.StreamEvent {
	events := p.closeThinking()
	return append(events, p.closeRedacted()...)
}

// toolBlockByID 已开 tool 块的线 id → block index（W42 ② tool 签名配对）。
// 走 toolByID（含冲突后被顶替出 index 槽位的块——签名配对不丢）。
func (p *StreamParser) toolBlockByID(id string) (int, bool) {
	if st, ok := p.toolByID[id]; ok {
		return st.blockIndex, true
	}
	return 0, false
}

// feedEncryptedDetail W42 ①/②：一条 reasoning.encrypted 条目的流式处理。
//   - id 与已开 tool 块配对 → 签名载荷缓冲（块尾一条下发——closeBlocks）；
//     id 尚无对应 tool 块 → 待配缓冲（tool_call 到达时附着，见 Feed 的 tool 分支）；
//   - 无 id 且 Claude 族 format → redacted 载荷块（data 逐条累积；块切换纪律
//     与 thinking 同款）；别族 / data 空 → 丢弃（红线同族纪律）。
func (p *StreamParser) feedEncryptedDetail(d reasoningDetailWire) []ir.StreamEvent {
	if d.Data == "" {
		return nil
	}
	if d.ID != "" {
		if bi, ok := p.toolBlockByID(d.ID); ok {
			p.toolSig[bi] += d.Data
		} else {
			p.pendingToolSig[d.ID] += d.Data
		}
		return nil
	}
	if !reasoningRedactedFormat(d.Format) {
		return nil
	}
	detail := 0
	if d.Index != nil {
		detail = *d.Index
	}
	var events []ir.StreamEvent
	if p.redIdx >= 0 && p.redDetail != detail {
		// 不同 index 组 = 不同载荷：先收尾再开新块。
		events = append(events, p.closeRedacted()...)
	}
	if p.redIdx < 0 {
		events = append(events, p.closeThinking()...)
		p.redIdx = p.nextBlock
		p.nextBlock++
		p.redDetail = detail
		p.open = append(p.open, openBlock{p.redIdx, ir.BlockRedactedThinking})
		events = append(events, ir.StreamEvent{Kind: ir.EventBlockStart, BlockIndex: p.redIdx, BlockKind: ir.BlockRedactedThinking})
	}
	events = append(events, ir.StreamEvent{
		Kind: ir.EventBlockDelta, BlockIndex: p.redIdx, BlockKind: ir.BlockRedactedThinking,
		SignatureDelta: d.Data,
	})
	return events
}

// Finish 流结束（EOF）时调用：产出未发出的终态事件。
//
//   - [DONE] 或错误帧已见 → 无补充；
//   - finish_reason 已见但无哨兵 → 正常结束（new-api StreamEndReason.EOF，
//     stream-mechanics §4.7：无哨兵 EOF 也是正常路径）；
//   - 其余 → 截断流判失败（CPA 502 / omp incomplete-stream），不发 block_stop
//     （块未完成，闭块会被消费端误终解析半 JSON）。
func (p *StreamParser) Finish() []ir.StreamEvent {
	if p.doneSeen || p.errorSeen {
		return nil
	}
	if p.finishSeen {
		return []ir.StreamEvent{
			{Kind: ir.EventMessageStop},
			{Kind: ir.EventDone, Reason: p.stopReason},
		}
	}
	return []ir.StreamEvent{terminalTransportError("stream closed before finish_reason")}
}

func (p *StreamParser) closeBlocks() []ir.StreamEvent {
	events := make([]ir.StreamEvent, 0, len(p.open))
	for _, b := range p.open {
		// W42 ②：tool 块尾下发缓冲的签名（形态可识别才进 IR——红线；与写面
		// 「块尾一条完整条目」同口径）。签名增量先于块 stop（claude wire 块内
		// 顺序）。
		if isToolKind(b.kind) {
			if sig := p.toolSig[b.index]; sig != "" {
				if _, ok := reasoningToolSignature(sig); ok {
					events = append(events, ir.StreamEvent{
						Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: b.kind,
						SignatureDelta: sig,
					})
				}
				delete(p.toolSig, b.index)
			}
		}
		events = append(events, ir.StreamEvent{Kind: ir.EventBlockStop, BlockIndex: b.index, BlockKind: b.kind})
	}
	p.open = nil
	return events
}

// ParseStream 便捷入口：从 SSE 字节流解析全部 IR 事件（内含最小切帧器）。
//
// 切帧规则（stream-mechanics §4.1 的 WHATWG 子集）：注释行（`: …`，心跳）与
// event:/id:/retry: 行不产事件；多行 data 以 "\n" 连接；空行派发；行尾 \r\n 规范化
// （bufio.ScanLines）；EOF 时派发未闭合帧（宽容——真实流常以空行收尾）。
// 裸 CR 行分隔不支持（HTTP SSE 实战形态为 \n / \r\n）。
func ParseStream(r io.Reader) ([]ir.StreamEvent, error) {
	p := NewStreamParser()
	var events []ir.StreamEvent
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 64*1024*1024)
	var dataLines []string
	flush := func() error {
		if len(dataLines) == 0 {
			return nil
		}
		payload := strings.Join(dataLines, "\n")
		dataLines = dataLines[:0]
		evs, err := p.Feed([]byte(payload))
		events = append(events, evs...)
		return err
	}
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, ":") {
			continue // 注释行（心跳）不产事件
		}
		if line == "" {
			if err := flush(); err != nil {
				return events, err
			}
			continue
		}
		if v, ok := strings.CutPrefix(line, "data:"); ok {
			dataLines = append(dataLines, strings.TrimPrefix(v, " "))
			continue
		}
		// event: / id: / retry: / 其它字段行：OpenAI Chat 流不使用，忽略。
	}
	if err := sc.Err(); err != nil {
		return events, err
	}
	if err := flush(); err != nil {
		return events, err
	}
	events = append(events, p.Finish()...)
	return events, nil
}

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

// parseMessage 解析一条消息（请求与响应共用）。
func parseMessage(raw json.RawMessage) (ir.Message, error) {
	var mw messageWire
	if err := json.Unmarshal(raw, &mw); err != nil {
		return ir.Message{}, fmt.Errorf("message: %v", err)
	}
	if mw.Role == "" {
		return ir.Message{}, fmt.Errorf("message without role")
	}
	msg := ir.Message{Role: ir.Role(mw.Role), Name: mw.Name}

	if mw.Role == string(ir.RoleTool) {
		// tool 消息：content 为字符串（或 text parts 数组），tool_call_id 必填。
		if mw.ToolCallID == "" {
			return ir.Message{}, fmt.Errorf("%w: tool message without tool_call_id", ErrToolResultShape)
		}
		content, err := parseToolContent(mw.Content)
		if err != nil {
			return ir.Message{}, err
		}
		msg.Blocks = append(msg.Blocks, ir.ContentBlock{
			Kind:       ir.BlockToolResult,
			ToolResult: &ir.ToolResult{CallID: mw.ToolCallID, Content: content},
		})
	} else {
		blocks, err := parseContent(mw.Content)
		if err != nil {
			return ir.Message{}, err
		}
		msg.Blocks = append(msg.Blocks, blocks...)
	}

	for _, tc := range mw.ToolCalls {
		if tc.Type != "" && tc.Type != "function" {
			return ir.Message{}, fmt.Errorf("%w: tool_call type %q", ErrUnsupportedToolType, tc.Type)
		}
		msg.Blocks = append(msg.Blocks, ir.ContentBlock{
			Kind:     ir.BlockToolUse,
			ToolCall: &ir.ToolCall{ID: tc.ID, Name: tc.Function.Name, Arguments: tc.Function.Arguments},
		})
	}
	// refusal：非空字符串提升为 refusal 块；null/空串留在 Extra 保真。
	if len(mw.Refusal) > 0 && !isJSONNull(mw.Refusal) {
		var s string
		if err := json.Unmarshal(mw.Refusal, &s); err != nil {
			return ir.Message{}, fmt.Errorf("refusal: %v", err)
		}
		if s != "" {
			msg.Blocks = append(msg.Blocks, ir.ContentBlock{Kind: ir.BlockRefusal, Text: s})
		}
	}

	// 推理扩展字段（`reasoning_content` / `reasoning` 别名）→ IR 思考块，**置首**
	// （Anthropic assistant 消息要求思考块先于文本；与响应面同口径——本函数请求/
	// 响应共用）。
	// 安全规则（表 §7.1）：只有 assistant 消息的思考映射；user/system/tool 里的该
	// 字段不进块（留在 Extra 保真旁路）。
	// W41：`reasoning_details`（OpenRouter 结构化载体）优先——reasoning.text 条目
	// 的文本/签名进块（签名按形态校验，不可识别丢弃——红线）；载体只有签名时文本
	// 回退 reasoning_content。载体存在即被消费（assistant 消息上不进 Extra——渲染侧
	// 单一写者，未判定签名绝不回写）。
	// W42：①reasoning.summary 文本 → 思考文本（reasoning.text 缺省时的回退）；
	// reasoning.encrypted{id} 与同消息 tool_call 配对 → 该 tool_use 块签名
	// （②Gemini 3 工具签名通道）；无配对的 Claude 族载荷 → redacted_thinking。
	// `reasoning` 字符串字段 = reasoning_content 的别名（OpenRouter/litellm 实证），
	// 文本回退链的末位（字段本身留 Extra 保真——与 reasoning_content 同口径）。
	// W6-1：请求面（回传轮）此前完全不读 reasoning_content（跨格式腿静默丢思考）——
	// 落法见 research/briefs/reports/A2A-L19-W6-1.md。
	if mw.Role == string(ir.RoleAssistant) {
		carrier := readReasoningCarrier(mw.ReasoningDetails)
		redacted := applyEncryptedCarrier(&msg.Blocks, mw.ToolCalls, carrier.encrypted)
		rc := rawString(mw.ReasoningContent)
		if rc == "" {
			rc = rawString(mw.Reasoning) // W42：`reasoning` 别名（末位回退）。
		}
		var head []ir.ContentBlock
		switch {
		case carrier.text != "" || carrier.sig != "":
			text := carrier.text
			if text == "" {
				text = rc // 载体只带签名：文本回退 reasoning_content / reasoning。
			}
			head = append(head, ir.ContentBlock{Kind: ir.BlockThinking, Text: text, Signature: carrier.sig})
		case rc != "":
			head = append(head, ir.ContentBlock{Kind: ir.BlockThinking, Text: rc})
		}
		for _, payload := range redacted {
			head = append(head, ir.ContentBlock{Kind: ir.BlockRedactedThinking, Text: payload})
		}
		if len(head) > 0 {
			msg.Blocks = append(head, msg.Blocks...)
		}
	}

	known := []string{"role", "content", "tool_calls", "name"}
	if mw.Role == string(ir.RoleTool) {
		known = append(known, "tool_call_id")
	}
	if mw.Role == string(ir.RoleAssistant) && len(mw.ReasoningDetails) > 0 && !isJSONNull(mw.ReasoningDetails) {
		// W41：assistant 消息上的载体被解析消费（未判定签名不回流；渲染侧重建）。
		known = append(known, "reasoning_details")
	}
	if len(msg.Blocks) > 0 {
		for _, b := range msg.Blocks {
			if b.Kind == ir.BlockRefusal {
				known = append(known, "refusal")
				break
			}
		}
	}
	msg.Extra = extrasOf(raw, known...)
	return msg, nil
}

// parseContent 解析 message.content：string | array<part> | null。
func parseContent(raw json.RawMessage) ([]ir.ContentBlock, error) {
	if len(raw) == 0 || isJSONNull(raw) {
		return nil, nil
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("content: %v", err)
		}
		return []ir.ContentBlock{{Kind: ir.BlockText, Text: s}}, nil
	case '[':
		var parts []json.RawMessage
		if err := json.Unmarshal(raw, &parts); err != nil {
			return nil, fmt.Errorf("content: %v", err)
		}
		blocks := make([]ir.ContentBlock, 0, len(parts))
		for i, p := range parts {
			b, err := parseContentPart(p)
			if err != nil {
				return nil, fmt.Errorf("content part %d: %v", i, err)
			}
			blocks = append(blocks, b)
		}
		return blocks, nil
	default:
		return nil, fmt.Errorf("content: unexpected JSON shape")
	}
}

type contentPartWire struct {
	Type     string `json:"type"`
	Text     string `json:"text"`
	ImageURL *struct {
		URL    string `json:"url"`
		Detail string `json:"detail"`
	} `json:"image_url"`
}

// parseContentPart 解析内容部件。未建模部件类型保留原文（BlockUnknown + Raw）。
func parseContentPart(raw json.RawMessage) (ir.ContentBlock, error) {
	var pw contentPartWire
	if err := json.Unmarshal(raw, &pw); err != nil {
		return ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, nil
	}
	switch pw.Type {
	case "text":
		return ir.ContentBlock{Kind: ir.BlockText, Text: pw.Text}, nil
	case "image_url":
		if pw.ImageURL == nil {
			return ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, nil
		}
		return ir.ContentBlock{Kind: ir.BlockImage, Image: &ir.ImageSource{
			Kind: "file", URL: pw.ImageURL.URL, Detail: pw.ImageURL.Detail,
		}}, nil
	default:
		return ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, nil
	}
}

// parseToolContent 解析 tool 消息 content：字符串或 text parts 数组（"\n" 连接）。
func parseToolContent(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || isJSONNull(raw) {
		return "", nil
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", fmt.Errorf("tool content: %v", err)
		}
		return s, nil
	case '[':
		blocks, err := parseContent(raw)
		if err != nil {
			return "", err
		}
		var texts []string
		for _, b := range blocks {
			if b.Kind != ir.BlockText {
				return "", fmt.Errorf("%w: tool content part kind %q", ErrToolResultShape, b.Kind)
			}
			texts = append(texts, b.Text)
		}
		return strings.Join(texts, "\n"), nil
	default:
		return "", fmt.Errorf("tool content: unexpected JSON shape")
	}
}

// parseToolDef 解析 tools[] 项（type=function）。
func parseToolDef(raw json.RawMessage) (ir.ToolDef, error) {
	var tw toolDefWire
	if err := json.Unmarshal(raw, &tw); err != nil {
		return ir.ToolDef{}, fmt.Errorf("tool: %v", err)
	}
	if tw.Type != "" && tw.Type != "function" {
		return ir.ToolDef{}, fmt.Errorf("%w: %q", ErrUnsupportedToolType, tw.Type)
	}
	if len(tw.Function) == 0 || isJSONNull(tw.Function) {
		return ir.ToolDef{}, fmt.Errorf("tool: missing function object")
	}
	var fw toolDefFunctionWire
	if err := json.Unmarshal(tw.Function, &fw); err != nil {
		return ir.ToolDef{}, fmt.Errorf("tool function: %v", err)
	}
	if fw.Name == "" {
		return ir.ToolDef{}, fmt.Errorf("tool: missing name")
	}
	td := ir.ToolDef{
		Name:        fw.Name,
		Description: fw.Description,
		Strict:      fw.Strict,
	}
	if len(fw.Parameters) > 0 && !isJSONNull(fw.Parameters) {
		td.InputSchema = fw.Parameters
	}
	// 工具项级未建模字段（含 function 级）保真；function 级优先。
	td.Extra = extrasOf(tw.Function, "name", "description", "parameters", "strict")
	for k, v := range extrasOf(raw, "type", "function") {
		if _, ok := td.Extra[k]; !ok {
			if td.Extra == nil {
				td.Extra = map[string]json.RawMessage{}
			}
			td.Extra[k] = v
		}
	}
	return td, nil
}

// parseUsage 解析 usage 对象（顶层字段建模；details 等原文进 Extra；
// CacheRead/Reasoning 为派生只读值——渲染以 Extra 原文为准）。
func parseUsage(raw json.RawMessage) (*ir.Usage, error) {
	if len(raw) == 0 || isJSONNull(raw) {
		return nil, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("usage: %v", err)
	}
	u := &ir.Usage{Accounting: "openai"}
	var err error
	if u.Input, err = rawInt64(m["prompt_tokens"]); err != nil {
		return nil, fmt.Errorf("prompt_tokens: %v", err)
	}
	if u.Output, err = rawInt64(m["completion_tokens"]); err != nil {
		return nil, fmt.Errorf("completion_tokens: %v", err)
	}
	if u.Total, err = rawInt64(m["total_tokens"]); err != nil {
		return nil, fmt.Errorf("total_tokens: %v", err)
	}
	if d, ok := m["prompt_tokens_details"]; ok {
		u.Extra = map[string]json.RawMessage{"prompt_tokens_details": d}
		var det usageDetailsWire
		if err := json.Unmarshal(d, &det); err == nil && det.CachedTokens != nil {
			u.CacheRead = *det.CachedTokens
		}
	}
	if d, ok := m["completion_tokens_details"]; ok {
		if u.Extra == nil {
			u.Extra = map[string]json.RawMessage{}
		}
		u.Extra["completion_tokens_details"] = d
		var det usageDetailsWire
		if err := json.Unmarshal(d, &det); err == nil && det.ReasoningTokens != nil {
			u.Reasoning = *det.ReasoningTokens
		}
	}
	for k, v := range m {
		switch k {
		case "prompt_tokens", "completion_tokens", "total_tokens",
			"prompt_tokens_details", "completion_tokens_details":
			continue
		}
		if u.Extra == nil {
			u.Extra = map[string]json.RawMessage{}
		}
		u.Extra[k] = v
	}
	return u, nil
}

// parseErrorObject 解析 OpenAI 错误对象内层 {message,type,param,code}。
func parseErrorObject(raw json.RawMessage) *ir.Error {
	e := &ir.Error{Source: "openai", Raw: raw}
	var ew errorWire
	if err := json.Unmarshal(raw, &ew); err != nil {
		e.Type = "api_error"
		e.Message = "malformed upstream error object"
		return e
	}
	e.Message = ew.Message
	e.Type = ew.Type
	e.Param = rawString(ew.Param)
	e.Code = rawStringOrNumber(ew.Code)
	return e
}

// mapFinishReason 把 OpenAI finish_reason 映射到 IR 四值（omp-toolconv stop-reason 表）；
// 未知值原样透传（翻译策略在转换层，解析层不猜测）。
func mapFinishReason(s string) ir.StopReason {
	switch s {
	case "stop":
		return ir.StopStop
	case "length":
		return ir.StopLength
	case "tool_calls", "function_call": // function_call 为弃用别名
		return ir.StopToolUse
	case "content_filter":
		return ir.StopError
	default:
		return ir.StopReason(s)
	}
}

func terminalTransportError(msg string) ir.StreamEvent {
	return ir.StreamEvent{
		Kind:        ir.EventError,
		ErrorReason: ir.ErrorTransport,
		Error:       &ir.Error{Type: "incomplete_stream", Message: msg, Source: "openai"},
	}
}

// extrasOf 返回 raw 对象中除 known 键外的全部字段（保真旁路）。
func extrasOf(raw json.RawMessage, known ...string) map[string]json.RawMessage {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil || len(m) == 0 {
		return nil
	}
	for _, k := range known {
		delete(m, k)
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

func rawString(raw json.RawMessage) string {
	if len(raw) == 0 || isJSONNull(raw) {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s
}

// rawStringOrNumber 兼容字符串与数值 code（Gemini 数值 code / OpenAI 字符串 code）。
func rawStringOrNumber(raw json.RawMessage) string {
	if len(raw) == 0 || isJSONNull(raw) {
		return ""
	}
	if raw[0] == '"' {
		return rawString(raw)
	}
	return strings.TrimSpace(string(raw))
}

func rawInt64(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || isJSONNull(raw) {
		return 0, nil
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil {
		return 0, fmt.Errorf("%v", err)
	}
	return n, nil
}

func isJSONNull(raw json.RawMessage) bool {
	return strings.TrimSpace(string(raw)) == "null"
}

// hasRaw 非空且非 null（null 视同缺省——IR 不动点要求 parse/render 两侧同判）。
func hasRaw(raw json.RawMessage) bool {
	return len(raw) > 0 && !isJSONNull(raw)
}

// isDoneSentinel 容忍尾随内容（对齐 openai-python 的 startswith 判法）。
func isDoneSentinel(s string) bool {
	return strings.HasPrefix(s, doneSentinel)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
