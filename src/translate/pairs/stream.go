package pairs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dwgx/any2api/translate/convert/chatgpt_web"
	"github.com/dwgx/any2api/translate/convert/claude_messages"
	"github.com/dwgx/any2api/translate/convert/claude_web"
	"github.com/dwgx/any2api/translate/convert/gemini"
	"github.com/dwgx/any2api/translate/convert/interactions"
	"github.com/dwgx/any2api/translate/convert/openai_chat"
	"github.com/dwgx/any2api/translate/convert/responses"
	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/registry"
	"github.com/dwgx/any2api/translate/toolcall"
	"github.com/dwgx/any2api/translate/toolname"
	"github.com/dwgx/any2api/translate/toolsim"
)

// StreamPipe 增量流式转换管道：上游 SSE 帧逐个进 → IR 事件 → 下游 SSE 字节逐个出。
//
// 服务壳 relay 的内核（上游逐帧到达、逐帧写下游——TTFB 与提交点纪律的前提）；
// 批量场景用 Convert(FaceStream)。支持两类计划（与 registry.Lookup 一一对应）：
//
//   - 直转（1 步）：入口 parse → 出口 writer；
//   - 枢纽两步（2 步）：入口 parse → 枢纽 writer → 枢纽 SSE 逐帧 parse → 出口 writer。
//     枢纽腿真实经过线格式（与批量 Convert 的两步同构）：空参数 "{}" 修复、工具槽位
//     稳定化、哨兵纪律都在枢纽腿上发生，出口看到的语义与批量路径一致。
//
// 提交点纪律（服务壳 relay 的契约；跨两步保持）：
//
//   - 首个有效产出前失败：本管道未向下游消费任何字节（Feed 返回空产出），调用方
//     可丢弃管道换上游重试（Feed 失败当次不产字节）；
//   - 已产出后失败：调用方绝不重试/回退——用 RenderError 写出口格式错误帧收尾；
//   - 一次失败即终态（failed 粘住）：失败后 Feed/Finish 只回同一错误，不重启不回退。
type StreamPipe struct {
	to registry.Format

	entry *streamParser
	exit  *streamWriter
	hub   *hubStage // 非 nil = 枢纽两步

	// names 工具名还原表（净化名 → 原名；P67 双向映射层的响应侧）。空表 = 直过。
	// 入口腿解析出的事件在 pump 前还原——出口修复层/渲染器看到的都是原名。
	names map[string]string

	failed error
}

// streamParser 一个格式的增量解析器（SSE 帧 → IR 事件）。
type streamParser struct {
	feed        func(event string, data []byte) ([]ir.StreamEvent, error)
	finishParse func() []ir.StreamEvent
	// cgEntry 入口 = chatgpt_web 时的解析器实例（服务壳只读观测面；nil = 其余格式）。
	cgEntry *chatgpt_web.StreamParser
}

// streamWriter 一个格式的增量渲染器（IR 事件 → SSE 字节）。
type streamWriter struct {
	write       func(ev ir.StreamEvent) ([]byte, error)
	finishWrite func() ([]byte, error)
	// stage 出口修复层（仅出口腿非 nil；枢纽腿不接——中间线格式的修复由出口腿统一做）。
	stage *toolExitStage
	// sim 工具模拟过滤器（S2；仅出口腿且 Options.ToolSim 开时非 nil；枢纽腿不接）。
	// 位于 stage **之前**：合成块的参数照走出口修复纪律（设计单 §S2 接线位置）。
	sim *toolsim.Filter
	// diag 该腿的诊断计数读取（nil = 该腿无诊断源）。
	diag func() (droppedThinking, reasoningDeltas, signatureDropped int)
}

// hubStage 枢纽两步的中间腿：入口腿产出的 IR 事件先渲染成枢纽线格式（SSE 字节），
// 再逐帧解析回 IR 交给出口 writer——批量 Convert 的「render→parse」两步的增量对应物。
type hubStage struct {
	parse *streamParser
	write *streamWriter
	buf   []byte // 未成帧的枢纽字节（跨 Feed 保留）
}

// NewStreamPipe 创建 from→to 的增量转换管道（经 registry 校验转换对存在）。
//
// 入口契约（W43-3 §4.2 `pipe_finish_error` ×5 的文档化）：喂入的帧流必须是
// from 格式的 **completion 流**（含该格式的骨架——如 claude_web 的
// message_start → … → message_stop）。非 completion 上游 SSE（如 claude.ai 的
// MCP 连接器列表流 server_list/server_base/tools/…，无 message_start）**不得
// 接管道**：出口 writer 会在 Finish 显式报错（`stream events must begin with a
// start event`——缺骨架不静默补帧，设计行为）；服务壳只把 completion 流接入
// （service/relay.go relayStream）。
func NewStreamPipe(reg *registry.Registry, from, to registry.Format, opts Options) (*StreamPipe, error) {
	if reg == nil {
		return nil, fmt.Errorf("pairs: nil registry")
	}
	plan, err := reg.Lookup(from, to)
	if err != nil {
		return nil, err
	}
	if len(plan.Steps) < 1 || len(plan.Steps) > 2 {
		return nil, fmt.Errorf("pairs: 流式计划不支持 %d 步（%s→%s）", len(plan.Steps), from, to)
	}
	entry, err := newStreamParser(from)
	if err != nil {
		return nil, err
	}
	exit, err := newStreamWriter(to, opts.IncludeUsage, false)
	if err != nil {
		return nil, err
	}
	// 出口腿接 toolcall 出口修复层（P53 缺口 17）：畸形 tool 参数在出口被
	// merge/修复/解包/`{}` 纪律收口，修不好则不发坏 JSON、终态补 error。
	exit.stage = newToolExitStage(consumerContractFor(to))
	p := &StreamPipe{to: to, entry: entry, exit: exit}
	p.names = toolname.Reverse(opts.ToolNames)
	if opts.ToolSim {
		// 工具模拟（S2 流式面）：出口腿过滤器位于修复层之前（合成参数照走修复
		// 纪律）。names 传入——合成块的工具名同样要按净化名→原名还原。
		p.exit.sim = toolsim.NewFilter(p.names)
	}

	if len(plan.Steps) == 2 {
		hub := plan.Steps[0].To // 注册表保证 Steps[1].From == Steps[0].To
		if plan.Steps[1].From != hub || hub == from || hub == to {
			return nil, fmt.Errorf("pairs: 非枢纽两步计划 %+v", plan.Steps)
		}
		hubParse, err := newStreamParser(hub)
		if err != nil {
			return nil, err
		}
		hubWrite, err := newStreamWriter(hub, true, true) // 中间腿：include_usage 恒 true + HubLeg
		if err != nil {
			return nil, err
		}
		p.hub = &hubStage{parse: hubParse, write: hubWrite}
	}
	return p, nil
}

// consumerContractFor 出口格式 → toolcall 出口形态契约（决定空参怎么发：
// Anthropic 靠 start 的 `{}` 不额外发；OpenAI 家族（含 responses）必须显式发 `{}`；
// gemini 的 args 是对象、由 writer 物化——按 OpenAI 的 `{}` 纪律走同一出口；
// interactions 的 step.start 恒带 `arguments:{}`（官方形态）→ 与 Anthropic 同款：
// 空参不额外发 arguments_delta）。
func consumerContractFor(to registry.Format) toolcall.ConsumerContract {
	switch to {
	case registry.FormatClaudeMessages, FormatInteractions:
		return toolcall.ConsumerAnthropic
	case registry.FormatOpenAIResponses:
		return toolcall.ConsumerOpenAIResponses
	default:
		return toolcall.ConsumerOpenAI
	}
}

// Diagnostics 返回管道级诊断计数快照（纯观测，绝不进控制流；服务壳据此打
// 日志/指标）：思考丢弃（枢纽腿/出口腿）、reasoning_content 下发数、出口修复层
// 的改写/失败计数。
func (p *StreamPipe) Diagnostics() StreamDiagnostics {
	d := StreamDiagnostics{}
	if p.hub != nil && p.hub.write.diag != nil {
		dt, rd, sd := p.hub.write.diag()
		d.DroppedThinking += dt
		d.ReasoningDeltas += rd
		d.SignatureDropped += sd
	}
	if p.exit.diag != nil {
		dt, rd, sd := p.exit.diag()
		d.DroppedThinking += dt
		d.ReasoningDeltas += rd
		d.SignatureDropped += sd
	}
	if p.exit.stage != nil {
		d.ToolArgsRepaired = p.exit.stage.repaired
		d.ToolArgsFailed = p.exit.stage.failed
		d.ToolArgsDefect = p.exit.stage.defect
	}
	if p.exit.sim != nil {
		d.ToolSimSynthesized = p.exit.sim.Synthesized()
	}
	return d
}

// StreamDiagnostics 管道级诊断计数（纯观测，绝不进控制流）。
type StreamDiagnostics struct {
	// DroppedThinking 未下发到 OpenAI 线的 thinking / redacted_thinking 增量数
	//（枢纽腿 = redacted 载荷增量与空思考增量——W42 ③ 起思考文本走载体、不计丢弃；
	// 出口腿 = 空思考增量，redacted 载荷走载体、不计）。
	DroppedThinking int
	// ReasoningDeltas 以 delta.reasoning_content 下发的思考增量数（W42 ③ 起含
	// 枢纽腿）。
	ReasoningDeltas int
	// SignatureDropped 未写进载体的签名载荷增量数（W5-19 条目 5：从
	// DroppedThinking 拆出；W41 载体化后 OpenAI 线有 reasoning_details 载体，
	// W42 起含 tool_use 块签名——不可识别/非 Gemini 形态丢弃时计数；带签名案例
	// 据此与批量面逐项对拍）。
	SignatureDropped int
	// ToolArgsRepaired 出口修复层改写（修复 / 解包 / merge 归一）的工具参数块数。
	ToolArgsRepaired int
	// ToolArgsFailed 出口修复层判失败态的工具参数块数（不发坏 JSON，终态补 error）。
	ToolArgsFailed int
	// ToolArgsDefect 首个异常块（修复或失败）的归因诊断（纯观测；nil = 全部合法直过）。
	ToolArgsDefect *toolcall.Defect
	// ToolSimSynthesized 工具模拟过滤器合成的 tool_use 块数（S2；0 = 本轮模型
	// 未按契约发起调用——守约率观测点，纯观测，绝不进控制流）。
	ToolSimSynthesized int
}

// newStreamParser 按格式创建增量解析器（入口腿与枢纽腿的入口面）。
func newStreamParser(f registry.Format) (*streamParser, error) {
	p := &streamParser{}
	switch f {
	case registry.FormatOpenAIChat:
		sp := openai_chat.NewStreamParser()
		p.feed = func(_ string, data []byte) ([]ir.StreamEvent, error) { return sp.Feed(data) }
		p.finishParse = sp.Finish
	case registry.FormatClaudeMessages:
		sp := claude_messages.NewStreamParser()
		p.feed = func(event string, data []byte) ([]ir.StreamEvent, error) { return sp.Feed(event, data) }
		p.finishParse = sp.Finish
	case registry.FormatGeminiGenerate:
		sp := gemini.NewStreamParser()
		p.feed = func(_ string, data []byte) ([]ir.StreamEvent, error) { return sp.Feed(data) }
		p.finishParse = sp.Finish
	case registry.FormatOpenAIResponses:
		sp := responses.NewStreamParser()
		// event: 行与 payload 的 type 冗余（表 §4.1）——payload 为准。
		p.feed = func(_ string, data []byte) ([]ir.StreamEvent, error) { return sp.Feed(data) }
		p.finishParse = sp.Finish
	case FormatInteractions:
		sp := interactions.NewStreamParser()
		// `event:` 行与 payload 的 event_type 冗余且**必须一致**（Q6 加固）——
		// 解析以 event: 行为准、event_type 为校验，不一致报错。
		p.feed = func(event string, data []byte) ([]ir.StreamEvent, error) { return sp.Feed(event, data) }
		p.finishParse = sp.Finish
	case FormatClaudeWeb:
		sp := claude_web.NewStreamParser()
		// `event:` 行 = `data.type`（W9-2 双源核）——不一致报错（同 interactions Q6）。
		p.feed = func(event string, data []byte) ([]ir.StreamEvent, error) { return sp.Feed(event, data) }
		p.finishParse = sp.Finish
	case FormatChatGPTWeb:
		sp := chatgpt_web.NewStreamParser()
		// 帧形状自描述（不依赖 event: 名分派）——与 claude_web 的 event/data.type
		// 双源核不同：chatgpt 流的 event: 行只有 delta_encoding 一帧（v1 标记由
		// 载荷 "v1" 识别，W28-1 §8 原文口径）。
		p.feed = func(event string, data []byte) ([]ir.StreamEvent, error) { return sp.Feed(event, data) }
		p.finishParse = sp.Finish
		p.cgEntry = sp // 服务壳观测面（ChatGPTWebEntry；尾读宽限 + accessor 消费）
	default:
		return nil, fmt.Errorf("pairs: no stream parser for %s", f)
	}
	return p, nil
}

// ChatGPTWebEntry 入口 = chatgpt_web 时的**只读观测面**（服务壳消费 W28-1 §8 的
// accessor 清单；nil = 入口不是 chatgpt_web）。返回的接口只暴露 accessor——Feed/
// Finish 不在面上（管道独占写面），服务壳绝不驱动解析。
func (p *StreamPipe) ChatGPTWebEntry() ChatGPTWebEntry {
	if p.entry == nil || p.entry.cgEntry == nil {
		return nil
	}
	return p.entry.cgEntry
}

// ChatGPTWebEntry chatgpt_web 入口解析器的只读观测面（W28-1 §8 的 accessor
// 清单逐条：Terminal 供服务壳尾读宽限计时；其余为收尾点观测/诊断——服务壳
// chatgptWebObserve 消费，见 service/chatgpt_web.go）。
//
// *chatgpt_web.StreamParser 实现本接口（编译期断言在下方）。
type ChatGPTWebEntry interface {
	// Terminal 是否已观察到终态信号（尾读宽限计时用；含 status finished 等
	// 非收束终态——W33-1 起 message_stream_complete 同属：其后仍有
	// [conversation_detail_metadata]/[DONE] 流末 burst）。
	Terminal() bool
	// DefinitiveMarker 是否已见 definitive marker（`[DONE]` / error 帧）——
	// 服务壳**收束判据**（W32-1 方向 B：到达即收束，不等宽限；W33-1 裁定收窄：
	// message_stream_complete 移出收束集——complete 后仍有流末 burst）。
	// 仅「Terminal 且非 DefinitiveMarker」才用有界宽限兜底。
	DefinitiveMarker() bool
	// StreamCompleteSeen 是否已见 `message_stream_complete` 帧（尾部帧登记；
	// W33-1 起不参与收束判据）。只读观测。
	StreamCompleteSeen() bool
	// DoneSentinelSeen 是否已见 `[DONE]` 哨兵（裸/引号/内嵌三形态同判——
	// W33-1 起三形态全部可达）。只读观测。
	DoneSentinelSeen() bool
	// Markers message_marker 帧登记（W31-1 §4 尾部帧面：last_token 等——
	// 截断点证据帧；只读观测）。
	Markers() []chatgpt_web.MarkerInfo
	// MarkerOverflow 超上限被丢的 marker 帧数（防内存无界）。
	MarkerOverflow() int
	// Title 会话标题（title_generation；不下发下游，诊断面）。
	Title() string
	// ResumeToken resume_conversation_token（**不下发**——JWT 里有 turn_topic_id）。
	ResumeToken() string
	// ConversationID 观察到的会话 id（诊断面）。
	ConversationID() string
	// Handoff stream_handoff 记录（nil = 未出现；续接面未接线——S4）。
	Handoff() *chatgpt_web.HandoffInfo
	// Moderation moderation 记录（nil = 未出现；文案/错误帧属出口决策——S6）。
	Moderation() *chatgpt_web.ModerationInfo
	// ObservedModel 回读的实际模型（message.metadata.model_slug / server_ste_metadata）。
	ObservedModel() string
	// ObservedMetadata 最近一次 metadata 原文（保真旁路；IR 无载体）。
	ObservedMetadata() json.RawMessage
	// ConversationDetailMetadata conversation_detail_metadata 帧原文（W29-1 §4.2
	// D4；nil = 未出现）。字段在帧顶层（default_model_slug/model_limits/…）——
	// 保真整帧；W33-1 起流式面收束于 [DONE]，该帧随流末 burst 一并捕获（观测
	// 面见服务壳日志 detail=）。
	ConversationDetailMetadata() json.RawMessage
	// ToolMessages 被抑制消息/图片指针记录（渲染属 S6）。
	ToolMessages() []chatgpt_web.ToolMessage
	// ToolOverflow 记录溢出条数（防内存无界）。
	ToolOverflow() int
	// SawLiveMessage 是否观察到 live assistant 消息（false + 有帧 = 假成功面候选）。
	SawLiveMessage() bool
	// ProtocolVersion 协议版本标记（"v1" / ""；诊断面）。
	ProtocolVersion() string
	// HistoryDropped 历史回放帧丢弃数（诊断）。
	HistoryDropped() int
}

// 编译期断言：chatgpt_web 解析器满足服务壳观测面（接口漂移即在此红）。
var _ ChatGPTWebEntry = (*chatgpt_web.StreamParser)(nil)

// newStreamWriter 按格式创建增量渲染器（出口腿与枢纽腿的出口面）。
//
// includeUsage 只对 openai 出口有意义（claude/gemini 出口的 usage 恒发）：出口腿取
// 调用方选项；枢纽腿恒 true——内部腿必须把 usage 过桥，发不发由出口按客户端格式决定
// （否则 gemini 快照 usage 经枢纽两步会被 openai 的 include_usage 门静默吞掉）。
//
// hubLeg 只对 openai 出口有意义（openai 是唯一枢纽格式）：true = 内部腿——thinking
// 等 OpenAI 线不可表达的块丢弃 + 诊断计数，绝不报错断链（P53 缺口 2）。
func newStreamWriter(f registry.Format, includeUsage, hubLeg bool) (*streamWriter, error) {
	w := &streamWriter{}
	switch f {
	case registry.FormatOpenAIChat:
		sw := openai_chat.NewStreamWriter(openai_chat.StreamRenderOptions{IncludeUsage: includeUsage, HubLeg: hubLeg})
		w.write, w.finishWrite = sw.Write, sw.Finish
		w.diag = func() (int, int, int) {
			d := sw.Diagnostics()
			return d.DroppedThinking, d.ReasoningDeltas, d.SignatureDropped
		}
	case registry.FormatClaudeMessages:
		sw := claude_messages.NewStreamWriter(claude_messages.StreamWriterOptions{})
		w.write, w.finishWrite = sw.Write, sw.Finish
	case registry.FormatGeminiGenerate:
		sw := gemini.NewStreamWriter()
		w.write, w.finishWrite = sw.Write, sw.Finish
	case registry.FormatOpenAIResponses:
		sw := responses.NewStreamWriter()
		w.write, w.finishWrite = sw.Write, sw.Finish
	case FormatInteractions:
		sw := interactions.NewStreamWriter()
		w.write, w.finishWrite = sw.Write, sw.Finish
	default:
		return nil, fmt.Errorf("pairs: no stream writer for %s", f)
	}
	return w, nil
}

// Feed 处理一个上游 SSE 帧（event 名可为空——OpenAI/Gemini 流无命名事件），
// 返回应写入下游的字节（可能为空）。帧解析/渲染错误向上抛——服务壳按提交点决定
// （未提交 → 丢弃本管道换上游重试；已提交 → 不重试，写 RenderError 错误帧收尾）。
func (p *StreamPipe) Feed(event string, data []byte) ([]byte, error) {
	if p.failed != nil {
		return nil, p.failed
	}
	events, err := p.entry.feed(event, data)
	if err != nil {
		return nil, p.fail(err)
	}
	// 工具名还原（P67 响应侧）：上游按净化名回调 → 出口看到原名（与批量
	// FaceStream/FaceResponse 同表同序——先还原，再进出口修复层）。
	toolname.RestoreEvents(events, p.names)
	return p.pump(events)
}

// Finish 上游 EOF 时调用：产出解析器终态事件（done/error）与渲染器收尾字节。
//
// 枢纽两步的收尾顺序：入口终态 → 枢纽 writer →（枢纽 writer 收尾，未发哨兵时补）
// → 枢纽 parser → 出口 writer → 出口 writer 收尾（缺骨架时显式报错，不静默补帧）。
func (p *StreamPipe) Finish() ([]byte, error) {
	if p.failed != nil {
		return nil, p.failed
	}
	var out []byte
	if p.hub == nil {
		b, err := p.exit.writeEvents(p.entry.finishParse())
		out = append(out, b...)
		if err != nil {
			return out, p.fail(err)
		}
	} else {
		hubBytes, err := p.hub.write.writeEvents(p.entry.finishParse())
		if err != nil {
			return out, p.fail(err)
		}
		fin, err := p.hub.write.finishWrite()
		if err != nil {
			return out, p.fail(err)
		}
		hubBytes = append(hubBytes, fin...)
		b, err := p.hub.pump(p, hubBytes)
		out = append(out, b...)
		if err != nil {
			return out, p.fail(err)
		}
		// 枢纽 parser 终态（EOF 补帧/截断判定）→ 出口 writer。
		b, err = p.exit.writeEvents(p.hub.parse.finishParse())
		out = append(out, b...)
		if err != nil {
			return out, p.fail(err)
		}
	}
	b, err := p.exit.finishWrite()
	out = append(out, b...)
	if err != nil {
		return out, p.fail(err)
	}
	return out, nil
}

// RenderError 按出口格式渲染错误帧（提交点已提交后的收尾路径；直转与枢纽两步同一
// 条路径——错误信封直接从 IR 渲染成出口格式，不经枢纽二次映射，避免 type/code 丢失）。
func (p *StreamPipe) RenderError(e *ir.Error) ([]byte, error) {
	return RenderErrorFor(p.to, e)
}

// pump 把入口腿解析出的事件推过剩余腿（直转 = 出口 writer；枢纽 = 枢纽 writer →
// 枢纽 parser → 出口 writer）。
func (p *StreamPipe) pump(events []ir.StreamEvent) ([]byte, error) {
	if p.hub == nil {
		b, err := p.exit.writeEvents(events)
		if err != nil {
			return b, p.fail(err)
		}
		return b, nil
	}
	hubBytes, err := p.hub.write.writeEvents(events)
	if err != nil {
		return nil, p.fail(err)
	}
	b, err := p.hub.pump(p, hubBytes)
	if err != nil {
		return b, p.fail(err)
	}
	return b, nil
}

// fail 记录首次失败（粘住）：失败即终态，后续 Feed/Finish 只回同一错误。
func (p *StreamPipe) fail(err error) error {
	if p.failed == nil {
		p.failed = err
	}
	return p.failed
}

func (w *streamWriter) writeEvents(events []ir.StreamEvent) ([]byte, error) {
	if w.sim != nil {
		// 工具模拟（S2）：契约标签 → tool_use 块；**在出口修复层之前**
		//（合成块参数照走 merge/修复/`{}` 纪律）。
		events = w.sim.Process(events)
	}
	if w.stage != nil {
		events = w.stage.process(events)
	}
	var out []byte
	for _, ev := range events {
		b, err := w.write(ev)
		if err != nil {
			return out, err
		}
		out = append(out, b...)
	}
	return out, nil
}

// pump 把枢纽 SSE 字节按帧喂给枢纽 parser，解析出的 IR 事件交给出口 writer。
// 未成帧的尾巴留在 buf（跨 Feed 保留）；帧内错误原样上抛（含已产出的出口字节）。
func (h *hubStage) pump(p *StreamPipe, raw []byte) ([]byte, error) {
	h.buf = append(h.buf, raw...)
	var out []byte
	for {
		event, data, rest, ok := nextSSEFrame(h.buf)
		if !ok {
			return out, nil
		}
		h.buf = rest
		events, err := h.parse.feed(event, data)
		if err != nil {
			return out, err
		}
		b, err := p.exit.writeEvents(events)
		out = append(out, b...)
		if err != nil {
			return out, err
		}
	}
}

// nextSSEFrame 从缓冲头部切出一个完整 SSE 帧（空行终止）。返回帧的 event 名（可空）、
// data 载荷（多行 data 以 "\n" 连接）、剩余缓冲；ok=false = 帧未完整（保留缓冲等待）。
// 注释行（`: …`）与无字段空块跳过——与各解析器的切帧口径一致。
func nextSSEFrame(buf []byte) (event string, data []byte, rest []byte, ok bool) {
	var (
		name  string
		lines []string
		seen  bool
		pos   int
	)
	for {
		nl := bytes.IndexByte(buf[pos:], '\n')
		if nl < 0 {
			return "", nil, buf, false
		}
		line := bytes.TrimSuffix(buf[pos:pos+nl], []byte("\r"))
		pos += nl + 1
		if len(line) == 0 {
			if !seen {
				continue // 空块：跳过
			}
			return name, []byte(strings.Join(lines, "\n")), buf[pos:], true
		}
		if line[0] == ':' {
			continue // 注释/心跳
		}
		if v, isEvent := bytes.CutPrefix(line, []byte("event:")); isEvent {
			name, seen = strings.TrimSpace(string(v)), true
			continue
		}
		if v, isData := bytes.CutPrefix(line, []byte("data:")); isData {
			lines = append(lines, string(bytes.TrimPrefix(v, []byte(" "))))
			seen = true
		}
	}
}

// RenderErrorFor 按目标格式渲染错误信封（服务壳自产错误用；跨格式的上游错误
// 体走 Convert(FaceError)）。
func RenderErrorFor(to registry.Format, e *ir.Error) ([]byte, error) {
	c, ok := converters[to]
	if !ok {
		return nil, fmt.Errorf("pairs: no converter for %s", to)
	}
	return c.renderError(e)
}
