package claude_web

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/dwgx/any2api/translate/ir"
)

// ---------------------------------------------------------------------------
// 帧体 wire（只声明本包读取的字段；未知字段忽略——版本策略）
// ---------------------------------------------------------------------------

// streamEventWire 帧体公共判别式（`data.type`，与 `event:` 行双源核对）。
type streamEventWire struct {
	Type string `json:"type"`
}

// messageStartWire message_start.message（W9-2 实抓全字段；本包只取 id/model，
// 其余（parent_uuid/uuid/trace_id/request_id）不进 IR——服务壳每请求新建会话，
// 无跨请求锚点需求，保真旁路留给后续 slice）。
type messageStartWire struct {
	Message struct {
		ID    string `json:"id"`
		Model string `json:"model"`
	} `json:"message"`
}

// blockStartWire content_block_start。
type blockStartWire struct {
	Index        int `json:"index"`
	ContentBlock struct {
		Type string `json:"type"`
		Text string `json:"text"`
		// tool_use 块的 id/name 在 start 帧给出（CWK client.py:947-965）。
		ID   string `json:"id"`
		Name string `json:"name"`
		// Input 容忍面：Anthropic 线在 tool_use 块 start 带初始 input（通常 {}）；
		// claude.ai 网页线是否带该字段**未证**（W9-1 G1/G2）——只在非空对象时
		// 落一个 ArgsDelta，缺失/空对象不产事件（绝不发明）。
		Input json.RawMessage `json:"input"`
		// Truncated thinking 块的摘要态标记（W43-3 G1：9/9 实抓 thinking start
		// 恒 `truncated:true`——上游只暴露摘要而非全量思考）。进 IR 观测面
		// （StreamEvent.ThinkingTruncated），不改出口语义。
		Truncated *bool `json:"truncated"`
	} `json:"content_block"`
}

// blockDeltaWire content_block_delta。
//
// delta 子类型面（W10-3 实抓 golden，2 流互证）：
//   - `text_delta{text}`：正文增量（唯一正文来源）；
//   - `thinking_summary_delta{summary{summary}}`：**思考摘要增量**（实抓形态——
//     `content_block_start{type:"thinking",truncated:true}` 开块后逐条下发**摘要**
//     而非全量思考文本；W10-3 实抓零 `thinking_delta`、零 `signature_delta`）；
//   - `input_json_delta{partial_json}`：结构化增量（上游内置工具的调用参数；
//     W10-3 实抓 237 片拼成完整 JSON）；
//   - `tool_use_block_update_delta{message,display_content}`：工具块**显示面更新**
//     （W10-3 实抓 2 例；非参数、非正文）→ 未建模，忽略；
//   - `thinking_delta{thinking}` / `signature_delta{signature}`：W9-1 主题表（源码级
//     5 实现互证）的形态，W10-3 实抓**未见**——保留容忍解析（在场才落事件，
//     绝不发明；`signature_delta` 只承载到 IR 签名通道，不合成签名）。
type blockDeltaWire struct {
	Index int `json:"index"`
	Delta struct {
		Type        string `json:"type"`
		Text        string `json:"text"`
		Thinking    string `json:"thinking"`
		Signature   string `json:"signature"`
		PartialJSON string `json:"partial_json"`
		// Summary 思考摘要（thinking_summary_delta 载荷）。
		Summary struct {
			Summary string `json:"summary"`
		} `json:"summary"`
	} `json:"delta"`
}

// blockStopWire content_block_stop。
//
// SummaryPending（W43-3：thinking 块 stop 特有，实抓 9/9 恒 false）= 摘要仍在
// 生成；进 IR 观测面（StreamEvent.SummaryPending），不改出口语义。
type blockStopWire struct {
	Index          int   `json:"index"`
	SummaryPending *bool `json:"summary_pending"`
}

// messageDeltaWire message_delta（实抓：`delta.stop_reason`；网页线该帧无 usage）。
//
// W43-3 §4.3 R1：`delta.stop_sequence`（值 `usage_limit` = 配额烧穿截断）与
// `delta.stop_details`（语料恒 null）此前被丢弃——stop_reason 被归一为 stop 后
// 截断信号在下游不可见。两者现进 IR 透传（StreamEvent.StopSequence/StopDetails），
// 出口按各自契约映射（Anthropic 出口回填 stop_sequence 字段）。
type messageDeltaWire struct {
	Delta struct {
		StopReason   string          `json:"stop_reason"`
		StopSequence string          `json:"stop_sequence"`
		StopDetails  json.RawMessage `json:"stop_details"`
	} `json:"delta"`
}

// MessageLimit 配额快照（`message_limit.message_limit`；W9-2 实抓原文）。
//
// 语义（KB §逐帧表 + CWK client.py:496-533）：`type` ∈ within_limit|hit_limit；
// `windows` 按窗口键（实抓 `5h`/`7d`）给 `{status,resets_at,utilization}`；
// `resolved.limit` 是服务端解析后的当前生效档。**无 token 数**（G7 缺口）。
type MessageLimit struct {
	Type                  string                        `json:"type"`
	RepresentativeClaim   string                        `json:"representativeClaim"`
	OverageDisabledReason string                        `json:"overageDisabledReason"`
	OverageInUse          bool                          `json:"overageInUse"`
	Windows               map[string]MessageLimitWindow `json:"windows"`
	Resolved              *MessageLimitResolved         `json:"resolved"`
}

// MessageLimitWindow 单窗口配额。
type MessageLimitWindow struct {
	Status      string  `json:"status"`
	ResetsAt    int64   `json:"resets_at"`
	Utilization float64 `json:"utilization"`
}

// MessageLimitResolved 服务端解析后的生效档（实抓 `resolved`）。
type MessageLimitResolved struct {
	Status string `json:"status"`
	Limit  *struct {
		Kind     string  `json:"kind"`
		Group    string  `json:"group"`
		Percent  float64 `json:"percent"`
		Severity string  `json:"severity"`
		ResetsAt string  `json:"resets_at"`
	} `json:"limit"`
	DisabledReason string `json:"disabled_reason"`
}

// QuotaSummary 配额派生摘要（CWK client.py:496-533 的「最差窗口 + 最早 reset」
// 规则的显式化）：各窗口取最差 utilization 与最早 resets_at；硬限判定口径见
// summarize（**非安全即硬限**——W43-3 §4.3 R5 修复：原「枚举已知硬限值」对实抓
// `exceeded_limit` 恒 false）。派生值，绝不改写 wire 原文（原文在同事件的
// Usage.Extra["claude_web_message_limit"]）。
type QuotaSummary struct {
	HardLimit        bool    `json:"hard_limit"`
	WorstUtilization float64 `json:"worst_utilization"`
	WorstWindow      string  `json:"worst_window,omitempty"`
	EarliestResetsAt int64   `json:"earliest_resets_at,omitempty"`
}

// summarize 计算派生摘要（确定性：窗口名并列时取字典序最小）。
//
// 硬限口径（W43-3 §4.3 R5；选型理由）：**非安全即硬限**——顶层 `type` 或任一
// 窗口 `status` 非空且 != `within_limit` ⇒ HardLimit=true。
//   - 上游状态词表是「安全 vs 硬限」封闭二元：实抓值域 {within_limit,
//     exceeded_limit}（type 与窗口 status 双处），CWK 源码词汇 {within_limit,
//     hit_limit, over_limit}——原实现枚举已知硬限值，漏掉实抓的 `exceeded_limit`
//     → 硬限恒 false（R5）。反转为「排除安全值」后对新增同义硬限词免疫；
//   - 未知软性状态（如未来的 warning 档）会误报硬限：词表当前无此档，且误报
//     方向保守（宁可多报，不静默漏截断——R5 的代价正是漏报）；精确状态可从
//     Extra["claude_web_message_limit"] 原文回读（逐字节保真）；
//   - 空串 = wire 无该字段（无信号），不计入硬限。
func (m *MessageLimit) summarize() QuotaSummary {
	s := QuotaSummary{HardLimit: m.Type != "" && m.Type != limitWithin}
	first := true
	for name, w := range m.Windows {
		if w.Status != "" && w.Status != limitWithin {
			s.HardLimit = true
		}
		if first || w.Utilization > s.WorstUtilization ||
			(w.Utilization == s.WorstUtilization && name < s.WorstWindow) {
			s.WorstUtilization = w.Utilization
			s.WorstWindow = name
		}
		if w.ResetsAt > 0 && (s.EarliestResetsAt == 0 || w.ResetsAt < s.EarliestResetsAt) {
			s.EarliestResetsAt = w.ResetsAt
		}
		first = false
	}
	return s
}

// ---------------------------------------------------------------------------
// 流式解析状态机
// ---------------------------------------------------------------------------

// StreamParser claude.ai completion 流解析状态机。用法：对每个 SSE 帧调用
// Feed(eventName, dataPayload)；上游 EOF（或终止帧 message_stop）后调用 Finish。
//
// 契约（W9-2 §2.3 帧序 + W9-1 §逐帧表）：
//   - 骨架：ping/conversation_ready（忽略）→ message_start → (content_block_start
//     → content_block_delta* → content_block_stop)* → message_delta → message_limit
//     → message_stop；`error` 可在任意点出现（HTTP 200 下的流内错误）。
//   - `event:` 名与 `data.type` 必须一致（双源核）——不一致显式报错。
//   - 未知事件名/未知 delta 子类型优雅忽略（版本策略；`citations_delta` 属未建模面）。
//   - message_stop / error 是终态：其后任何数据帧报 ErrDataAfterTerminal。
type StreamParser struct {
	started   bool
	stopped   bool
	errorSeen bool

	reasonSeen bool
	stopReason ir.StopReason

	// open 块状态机：index → 块类型（delta/stop 按它定 BlockKind——「块类型先于
	// delta 类型判定」，ir/types.go 硬规则）。索引只作 map 键——不驱动分配
	//（W52-1 条款与回归测试见 ir/assemble.go 头注）。
	open map[int]ir.BlockKind
}

// NewStreamParser 创建流式解析状态机。
func NewStreamParser() *StreamParser {
	return &StreamParser{open: map[int]ir.BlockKind{}}
}

// Feed 处理一个 SSE 帧（event 名 + data 载荷；event 为空时回退 data.type，
// 容忍丢弃 event: 行的代理）。返回本次新增的 IR 事件；协议级违规返回包装后的哨兵。
func (p *StreamParser) Feed(event string, payload []byte) ([]ir.StreamEvent, error) {
	// padding trim：帧内 JSON 尾部带不定长空格（`{"type":"ping"   }`），帧尾
	// 亦可能有空白——解析前统一 trim（对空白敏感的匹配一律不做）。
	s := strings.TrimSpace(string(payload))
	if s == "" {
		return nil, nil
	}
	if p.stopped || p.errorSeen {
		return nil, fmt.Errorf("%w: %s", ErrDataAfterTerminal, truncate(s, 120))
	}
	var w streamEventWire
	if err := json.Unmarshal([]byte(s), &w); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadEvent, err)
	}
	name := event
	if name == "" {
		name = w.Type
	}
	if event != "" && w.Type != "" && event != w.Type {
		return nil, fmt.Errorf("%w: event=%q data.type=%q", ErrEventNameMismatch, event, w.Type)
	}

	switch name {
	case evPing, evConversationReady:
		// 保活 / 会话就绪：无 IR 载体，忽略（帧本身已被服务壳的 idle 守卫识别为
		// 非内容帧）。
		return nil, nil

	case evMessageStart:
		if p.started {
			return nil, fmt.Errorf("%w: duplicate message_start", ErrUnexpectedEventOrder)
		}
		var mw messageStartWire
		if err := json.Unmarshal([]byte(s), &mw); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadEvent, err)
		}
		p.started = true
		return []ir.StreamEvent{{
			Kind:      ir.EventStart,
			MessageID: mw.Message.ID,
			Model:     mw.Message.Model,
		}}, nil

	case evContentBlockStart:
		if !p.started {
			return nil, fmt.Errorf("%w: content_block_start before message_start", ErrUnexpectedEventOrder)
		}
		var bw blockStartWire
		if err := json.Unmarshal([]byte(s), &bw); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadEvent, err)
		}
		kind := blockKindOf(bw.ContentBlock.Type)
		p.open[bw.Index] = kind
		events := []ir.StreamEvent{{
			Kind:       ir.EventBlockStart,
			BlockIndex: bw.Index,
			BlockKind:  kind,
			ToolCallID: bw.ContentBlock.ID,
			ToolName:   bw.ContentBlock.Name,
		}}
		// 观测面（W43-3 G1）：thinking 块 start 的 truncated 标记进 IR
		// （nil = wire 无该键；不改出口语义）。
		if kind == ir.BlockThinking {
			events[0].ThinkingTruncated = bw.ContentBlock.Truncated
		}
		// 容忍面：tool_use 块 start 带非空初始 input 时补一个 ArgsDelta（Anthropic
		// 线形态；claude.ai 网页线未证——缺失/`{}` 不产事件）。
		if kind == ir.BlockToolUse && hasNonEmptyObject(bw.ContentBlock.Input) {
			events = append(events, ir.StreamEvent{
				Kind:       ir.EventBlockDelta,
				BlockIndex: bw.Index,
				BlockKind:  kind,
				ArgsDelta:  string(bw.ContentBlock.Input),
			})
		}
		return events, nil

	case evContentBlockDelta:
		if !p.started {
			return nil, fmt.Errorf("%w: content_block_delta before message_start", ErrUnexpectedEventOrder)
		}
		var dw blockDeltaWire
		if err := json.Unmarshal([]byte(s), &dw); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadEvent, err)
		}
		kind, ok := p.open[dw.Index]
		if !ok {
			return nil, fmt.Errorf("%w: index %d", ErrUnknownBlockIndex, dw.Index)
		}
		// 无 delta 载体的块（tool_result / 未建模块）：其内容在 start 帧里
		//（W10-3 实抓：tool_result 块 start 带 content/structured_content，
		// 两流零 delta 指向该块）——delta 忽略（块边界仍保真下发；IR 与各出口
		// 都没有该块的 delta 通道，硬发会被出口判 ErrUnsupportedBlock 断流）。
		if !blockHasDeltaCarrier(kind) {
			return nil, nil
		}
		// 块类型先于 delta 类型判定（ir/types.go 硬规则）：BlockKind 取自块状态机，
		// delta.type 只决定载荷字段。
		ev := ir.StreamEvent{Kind: ir.EventBlockDelta, BlockIndex: dw.Index, BlockKind: kind}
		switch dw.Delta.Type {
		case deltaText:
			ev.TextDelta = dw.Delta.Text
		case deltaThinking:
			// W9-1 主题表形态（实抓未见；容忍）。
			ev.TextDelta = dw.Delta.Thinking
		case deltaThinkingSummary:
			// W10-3 实抓形态：思考**摘要**增量（逐条，非全量）→ IR 思考块文本增量。
			ev.TextDelta = dw.Delta.Summary.Summary
		case deltaSignature:
			ev.SignatureDelta = dw.Delta.Signature
		case deltaInputJSON:
			ev.ArgsDelta = dw.Delta.PartialJSON
		default:
			// 未知 delta 子类型（含 tool_use_block_update_delta 的显示面更新）：
			// 忽略（版本策略，不炸不断流）。
			return nil, nil
		}
		// 空增量无信息（claude_messages 同款）：忽略——W10-3 实抓首片
		// `input_json_delta{partial_json:""}` 即此形态。
		if ev.TextDelta == "" && ev.ArgsDelta == "" && ev.SignatureDelta == "" {
			return nil, nil
		}
		return []ir.StreamEvent{ev}, nil

	case evContentBlockStop:
		if !p.started {
			return nil, fmt.Errorf("%w: content_block_stop before message_start", ErrUnexpectedEventOrder)
		}
		var sw blockStopWire
		if err := json.Unmarshal([]byte(s), &sw); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadEvent, err)
		}
		kind, ok := p.open[sw.Index]
		if !ok {
			return nil, fmt.Errorf("%w: index %d", ErrUnknownBlockIndex, sw.Index)
		}
		delete(p.open, sw.Index)
		ev := ir.StreamEvent{
			Kind:       ir.EventBlockStop,
			BlockIndex: sw.Index,
			BlockKind:  kind,
		}
		// 观测面（W43-3）：thinking 块 stop 的 summary_pending 标记进 IR
		// （nil = wire 无该键；不改出口语义）。
		if kind == ir.BlockThinking {
			ev.SummaryPending = sw.SummaryPending
		}
		return []ir.StreamEvent{ev}, nil

	case evMessageDelta:
		if !p.started {
			return nil, fmt.Errorf("%w: message_delta before message_start", ErrUnexpectedEventOrder)
		}
		var mw messageDeltaWire
		if err := json.Unmarshal([]byte(s), &mw); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadEvent, err)
		}
		// W43-3 §4.3 R1：stop_sequence/stop_details 与 stop_reason 同行透传
		// （stop_reason 仍归一为 IR 四值；Anthropic 出口据 StopSequence 回填
		// `stop_reason:"stop_sequence"` + 序列值）。
		ev := ir.StreamEvent{
			Kind:         ir.EventMessageDelta,
			StopSequence: mw.Delta.StopSequence,
			StopDetails:  mw.Delta.StopDetails,
		}
		if mw.Delta.StopReason == "" {
			// 空 stop_reason 的 message_delta 不当作终止证据（网页线该帧恒带
			// 值）；但细节面（stop_sequence/stop_details）在场时仍保真下发，
			// 全空才忽略。
			if ev.StopSequence == "" && len(ev.StopDetails) == 0 {
				return nil, nil
			}
			return []ir.StreamEvent{ev}, nil
		}
		p.reasonSeen = true
		p.stopReason = mapStopReason(mw.Delta.StopReason)
		ev.StopReason = p.stopReason
		return []ir.StreamEvent{ev}, nil

	case evMessageLimit:
		if !p.started {
			return nil, fmt.Errorf("%w: message_limit before message_start", ErrUnexpectedEventOrder)
		}
		var lw struct {
			// 内层对象原文保真（未知字段逐字节保留——只解析建模子集）。
			MessageLimit json.RawMessage `json:"message_limit"`
		}
		if err := json.Unmarshal([]byte(s), &lw); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadEvent, err)
		}
		if len(lw.MessageLimit) == 0 || isJSONNull(lw.MessageLimit) {
			return nil, nil
		}
		var ml MessageLimit
		if err := json.Unmarshal(lw.MessageLimit, &ml); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadEvent, err)
		}
		return []ir.StreamEvent{{
			Kind:  ir.EventMessageDelta,
			Usage: quotaUsage(&ml, lw.MessageLimit),
		}}, nil

	case evMessageStop:
		if !p.started {
			return nil, fmt.Errorf("%w: message_stop before message_start", ErrUnexpectedEventOrder)
		}
		p.stopped = true
		return []ir.StreamEvent{{Kind: ir.EventMessageStop}}, nil

	case evError:
		var ew struct {
			Error json.RawMessage `json:"error"`
			// 容忍面：错误信息直接放顶层 message 的形态（未证，兜底用）。
			Message string `json:"message"`
		}
		if err := json.Unmarshal([]byte(s), &ew); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadEvent, err)
		}
		p.errorSeen = true
		return []ir.StreamEvent{{Kind: ir.EventError, Error: streamError(ew.Error, ew.Message, s)}}, nil

	default:
		// 未知事件名：忽略（版本策略；W9-1 五实现同款「扫描器对未知 type 一律忽略」）。
		return nil, nil
	}
}

// Finish 流结束（EOF 或服务壳在 message_stop 处收尾）时调用：产出未发出的终态事件。
//
//   - message_stop / error 已见 → 无补充；
//   - 一个帧都没解析到（空流/纯 padding）→ 无补充（调用方按「上游零产出」处理，
//     比合成错误事件更准——不把空流伪装成流内失败）；
//   - 其余（有产出但无 message_stop）→ 截断流判失败（不补帧掩盖）。
func (p *StreamParser) Finish() []ir.StreamEvent {
	if p.stopped || p.errorSeen || !p.started {
		return nil
	}
	return []ir.StreamEvent{terminalTransportError("stream closed before message_stop")}
}

// ParseStream 便捷入口：从 completion SSE 字节流解析全部 IR 事件（内含最小切帧器）。
//
// 切帧规则（与 claude_messages.ParseStream 同构）：注释行（心跳）忽略；多行 data
// 以 "\n" 连接；空行派发；行尾 \r\n 规范化；EOF 时派发未闭合帧。
func ParseStream(r io.Reader) ([]ir.StreamEvent, error) {
	p := NewStreamParser()
	var events []ir.StreamEvent
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 64*1024*1024)
	var eventName string
	var dataLines []string
	flush := func() error {
		if len(dataLines) == 0 {
			eventName = ""
			return nil
		}
		payload := strings.Join(dataLines, "\n")
		name := eventName
		dataLines = dataLines[:0]
		eventName = ""
		evs, err := p.Feed(name, []byte(payload))
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
		if v, ok := strings.CutPrefix(line, "event:"); ok {
			eventName = strings.TrimSpace(v)
			continue
		}
		if v, ok := strings.CutPrefix(line, "data:"); ok {
			dataLines = append(dataLines, strings.TrimPrefix(v, " "))
			continue
		}
		// id: / retry: / 其它字段行：completion 流不使用，忽略。
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
// 错误信封
// ---------------------------------------------------------------------------

// ParseError 上游非 2xx 错误体 → IR 错误信封（Source=claude_web）。
//
// 形态面（W9-1 G5：只知状态码分类，体形态未证）：① Anthropic 式
// `{"type":"error","error":{type,message}}`；② `{"error":{message}}`；
// ③ 顶层 `{"message":…}`；④ 非 JSON（Cloudflare 挑战页 HTML）→ 走
// ir.SalvageError 的兜底提取（绝不透传原文）。429/401 的具体字段面由服务壳按
// 状态码分类（KB §429 的两种读法），本函数只做信封提取。
func ParseError(body []byte) *ir.Error {
	s := strings.TrimSpace(string(body))
	if s == "" {
		return &ir.Error{Type: "api_error", Message: "empty upstream error body", Source: "claude_web"}
	}
	var env struct {
		Type    string          `json:"type"`
		Error   json.RawMessage `json:"error"`
		Message string          `json:"message"`
	}
	if err := json.Unmarshal([]byte(s), &env); err != nil {
		// 非 JSON（CF 挑战页等）：兜底提取（one-api 7 路径表的最小信息抢救）。
		return ir.SalvageError([]byte(s), "claude_web")
	}
	e := &ir.Error{Source: "claude_web", Raw: json.RawMessage(s)}
	if len(env.Error) > 0 && !isJSONNull(env.Error) {
		// ①/②：`error` 对象（字符串形态容忍）。
		var inner struct {
			Type    string `json:"type"`
			Message string `json:"message"`
			Code    string `json:"code"`
		}
		if err := json.Unmarshal(env.Error, &inner); err == nil {
			e.Type = inner.Type
			e.Message = inner.Message
			e.Code = inner.Code
		} else {
			var msg string
			if err := json.Unmarshal(env.Error, &msg); err == nil {
				e.Message = msg
			}
		}
		if e.Type == "" {
			e.Type = env.Type
		}
	} else {
		e.Type = env.Type
		e.Message = env.Message
	}
	if e.Type == "" {
		e.Type = "api_error"
	}
	if e.Message == "" {
		e.Message = "upstream error (no message field)"
	}
	return e
}

// streamError 流内 `error` 帧 → IR 错误（HTTP 200 下的失败；KB §逐帧表）。
func streamError(raw json.RawMessage, topMessage, whole string) *ir.Error {
	e := &ir.Error{Type: "api_error", Source: "claude_web", Raw: json.RawMessage(whole)}
	if len(raw) > 0 && !isJSONNull(raw) {
		e.Raw = raw
		var inner struct {
			Type    string `json:"type"`
			Message string `json:"message"`
			Code    string `json:"code"`
		}
		if err := json.Unmarshal(raw, &inner); err == nil {
			if inner.Type != "" {
				e.Type = inner.Type
			}
			e.Message = inner.Message
			e.Code = inner.Code
		} else {
			var msg string
			if err := json.Unmarshal(raw, &msg); err == nil {
				e.Message = msg
			}
		}
	}
	if e.Message == "" {
		e.Message = topMessage
	}
	if e.Message == "" {
		e.Message = "upstream stream error (no message field)"
	}
	return e
}

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

// quotaUsage message_limit → IR Usage（配额面映射）。
//
// 映射（W9-1 §可复用点 3 + CWK client.py:496-533）：
//   - 原样保真：`Extra["claude_web_message_limit"]` = `message_limit` **内层对象**
//     原文（未知字段逐字节保留；帧外壳的 `type` 由内层自带的 type 字段承载）；
//   - 派生摘要：`Extra["claude_web_quota"]` = QuotaSummary（最差 utilization /
//     最早 resets_at / 硬限）；
//   - Accounting="claude_web"；**不填 Input/Output/Total**——网页线配额面没有
//     token 数（G7 缺口），0 是「未提供」不是「零用量」，绝不合成。
func quotaUsage(m *MessageLimit, raw json.RawMessage) *ir.Usage {
	extra := map[string]json.RawMessage{
		"claude_web_message_limit": raw,
	}
	if b, err := json.Marshal(m.summarize()); err == nil {
		extra["claude_web_quota"] = b
	}
	return &ir.Usage{Accounting: "claude_web", Extra: extra}
}

// mapStopReason claude.ai stop_reason → IR 四值（未知值原样透传——ir.StopReason
// 是 string 枚举，翻译策略在出口层）。
//
// `stop_sequence` 归一为 stop（IR 四值契约不变）；其截断语义经
// StreamEvent.StopSequence（值 `usage_limit`）保真到出口层——Anthropic 出口据
// 此回填 `stop_reason:"stop_sequence"` + `stop_sequence` 字段（W43-3 §4.3 R1）。
func mapStopReason(s string) ir.StopReason {
	switch s {
	case "end_turn", "stop_sequence":
		return ir.StopStop
	case "max_tokens":
		return ir.StopLength
	case "tool_use":
		return ir.StopToolUse
	case "refusal":
		return ir.StopError
	default:
		return ir.StopReason(s)
	}
}

// blockKindOf 线格式块类型 → IR 块类型（未建模值 → BlockUnknown，保真旁路不炸）。
//
// `tool_use` → **BlockServerToolUse**：claude.ai 网页线的工具是**上游自己的**
// 工具（web_search/artifacts/repl/MCP；客户端在上游无法注册工具——KB §出口表
// 「工具调用」行），模型对它们的调用由上游执行并回 tool_result（W10-3 实抓：
// tool_use 块后恒跟 tool_result 块）——语义 = 服务端工具，不是客户端工具调用。
// 出口各自按既有约定落法（openai 出口：server tool use 与 tool_use 同形 →
// tool_calls；claude_messages 出口：`server_tool_use` 块——Anthropic 线原生类型）。
func blockKindOf(t string) ir.BlockKind {
	switch t {
	case blockText:
		return ir.BlockText
	case blockThinking:
		return ir.BlockThinking
	case blockToolUse:
		return ir.BlockServerToolUse
	case blockToolResult:
		return ir.BlockToolResult
	case blockServerToolUse:
		return ir.BlockServerToolUse
	default:
		return ir.BlockUnknown
	}
}

// blockHasDeltaCarrier 该块类型在 IR / 下游出口是否有 delta 载体。
// tool_result 与 BlockUnknown 没有（内容在 start 帧；出口对它们的 delta 会判
// ErrUnsupportedBlock 断流）——其 delta 在解析层忽略（块边界仍保真）。
func blockHasDeltaCarrier(kind ir.BlockKind) bool {
	switch kind {
	case ir.BlockText, ir.BlockThinking, ir.BlockRefusal, ir.BlockToolUse, ir.BlockServerToolUse:
		return true
	default:
		return false
	}
}

// terminalTransportError 截断流的终态事件（与各格式解析器同款措辞/类型）。
func terminalTransportError(msg string) ir.StreamEvent {
	return ir.StreamEvent{
		Kind:        ir.EventError,
		ErrorReason: ir.ErrorTransport,
		Error:       &ir.Error{Type: "incomplete_stream", Message: msg, Source: "claude_web"},
	}
}

// hasNonEmptyObject raw 是非空 JSON 对象（`{}` / null / 缺失 → false）。
func hasNonEmptyObject(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return len(s) > 2 && s[0] == '{' && s[len(s)-1] == '}'
}

func isJSONNull(raw json.RawMessage) bool {
	return strings.TrimSpace(string(raw)) == "null"
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
