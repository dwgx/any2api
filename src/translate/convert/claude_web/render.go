package claude_web

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/toolsim"
)

// 本文件 = render 面（IR → claude.ai 体）。
//
// 面覆盖：
//   - RenderRequest：IR 请求 → completion 体（字段集以 W9-2 实抓为准；上游对
//     多余字段**报错而非忽略**，故渲染只出白名单字段，未知 Extra 绝不过桥）；
//   - RenderConversationCreate：会话创建体（KB §②：`{uuid,name:"",model,
//     include_conversation_preferences:true}`，CW:180-215 / YS:150-193 / BK 三实现同形）；
//   - 出口面（RenderResponse/RenderStream/RenderError）与入口面（ParseRequest）：
//     claude_web 是上游叶子格式，这些面不存在 → 显式报错（ErrUnsupportedFace），
//     绝不返回半成品字节。

// ---------------------------------------------------------------------------
// 请求体（completion）
// ---------------------------------------------------------------------------

// conversationParams create_conversation_params（实抓原文；W9-2 §2.2）。
// 首回合体带它、第二回合起改带 parent_message_uuid——本服务每请求新建会话
// （服务壳的会话准备面），故恒按首回合形态渲染。
type conversationParams struct {
	Name                           string  `json:"name"`
	Model                          string  `json:"model"`
	IncludeConversationPreferences bool    `json:"include_conversation_preferences"`
	PaprikaMode                    *string `json:"paprika_mode"` // 实抓恒 null（思考档是账号级设置，本服务不改账号状态）
	CompassMode                    *string `json:"compass_mode"` // 实抓恒 null
	ToolSearchMode                 string  `json:"tool_search_mode"`
	IsTemporary                    bool    `json:"is_temporary"`
	ChatMemoryMode                 string  `json:"chat_memory_mode"`
	EnabledImagine                 bool    `json:"enabled_imagine"`
}

// turnMessageUUIDs 每请求生成的人类/助手消息 UUID（实抓：`turn_message_uuids`；
// 上游据此做消息锚点，AU/CWK 同款）。
type turnMessageUUIDs struct {
	HumanMessageUUID     string `json:"human_message_uuid"`
	AssistantMessageUUID string `json:"assistant_message_uuid"`
}

// RenderRequest IR 请求 → claude.ai completion 体。
//
// 字段集（W9-2 §2.2 实抓 12.3–12.8 KB 的并集，逐字段落点见下）；缺省值逐条注明：
//
//	prompt                   ← IR messages 拍平（flattenPrompt）
//	timezone/locale          ← DefaultTimezone/DefaultLocale（IR 无对应字段；
//	                            实抓值是浏览器本地值，服务端取 CWK 的 UTC/en-US）
//	model                    ← req.Model（必填）
//	effort                   ← req.Extra["reasoning_effort"] 档位映射；缺省 medium
//	thinking_mode            ← 缺省 auto；reasoning_effort=none 或 Claude Code 的
//	                            thinking{type:disabled} → none
//	tools                    ← webTools 三元组（实抓的 33 个 MCP 工具属账号态，不可复现）
//	turn_message_uuids       ← 每请求新生成
//	attachments/sync_sources ← 空数组（实抓恒空）
//	files                    ← 图像块占位引用（每份附件一个 v4 uuid 形态 token；
//	                            无图像块 → 空数组。真实 file_uuid 由服务壳在
//	                            会话准备期上传后原地回填——attach.go 头注）
//	completion_request_id    ← 每请求新生成
//	rendering_mode           ← "messages"（实抓恒此值）
//	create_conversation_params ← 见 conversationParams（首回合形态）
//
// **不渲染 parent_message_uuid**（每请求新建会话 = 恒首回合）；**不渲染
// max_tokens/temperature/top_p/stop_sequences**——上游对多余字段报错
// （`Extra inputs are not permitted`，KB §陷阱 2），且实抓体不含它们。
//
//		工具面（W11-3 契约 + W17 工具模拟；证据见报告 §2）：
//	  - 上游 `tools` 字段是**服务端工具开关**（web_search_v0/artifacts_v0/repl_v0
//	    三元组），不是客户端工具注册面——客户端工具定义（req.Tools）**不进该字段**
//	    （W11-3 契约不破：A1 形态下工具定义只走 prompt 文本）；
//	  - 客户端工具**历史**（tool_use/tool_result 块）拍平为标签文本进 prompt
//	    （flattenBlock），不再 ErrUnsupportedBlock 断流——Claude Code 类消费端
//	    中途切换供应商时历史里恒有工具块；
//	  - 工具模拟（S1 注入，设计单 claude-web-tool-sim §S1）：opts.ToolSim 开且
//	    req.Tools 非空 → 在 prompt **最前**插注入段（契约 + 清单 + tool_choice 映射；
//	    toolsim.BuildInjection）——模型以文本标签发起调用，出口腿（pairs）解析回
//	    tool_use 块。**缺省关**：ToolSim=false 与历史行为逐字节相等。
func RenderRequest(req *ir.Request) ([]byte, error) {
	return RenderRequestWithOptions(req, RenderOptions{})
}

// RenderOptions claude_web 请求渲染选项（pairs.Options 的同名位在此镜像——上游
// 叶子格式不 import pairs，避免包环；由 pairs 的转换器包装搬运）。
type RenderOptions struct {
	// ToolSim 客户端工具模拟（S1 注入；设计单 §S1）。false（缺省）= 历史行为。
	ToolSim bool
	// Diag 注入面诊断汇（可选；与 openai_chat.RenderOptions.Diag 同模式——W21-2
	// 配额观测）：非 nil 时记录注入段字节账（纯观测，绝不进控制流）。
	Diag *RenderDiagnostics
	// AttachmentSink 附件作业交付出口（W24-1 接缝；可选）。含图像块时渲染器把
	// 附件作业写进 `sink.Job`（无全局状态——渲染器不保留任何引用）；**nil 而
	// 含图像块 = 显式报错**（无处交付 = 体无法回填）。调用方纪律：每次渲染传一个
	// 新鲜 sink（复用 = 后渲染覆盖；与 Diag 同款）。
	AttachmentSink *AttachmentSink
}

// RenderDiagnostics 请求渲染面诊断计数（纯观测；单次渲染调用内单 goroutine 使用，
// 绝不跨请求共享——与 openai_chat.BatchDiagnostics 同纪律）。
type RenderDiagnostics struct {
	// ToolSimInjectionBytes 注入段字节数（0 = 未注入：ToolSim 关 / 无工具 /
	// 清单为空）。口径 = toolsim.BuildInjection 返回串长度（prompt 增量另含一个
	// 分隔空行 = +2，见 RenderRequestWithOptions）。
	ToolSimInjectionBytes int
	// ToolSimTools 注入清单的工具条数（未注入 = 0）。
	ToolSimTools int
}

// toolSimInjectionObserver 注入段观测钩子（W21-2 配额观测；纯观测，绝不进控制流）。
// 本包不 import log——输出侧由注册方决定（服务壳在 init 注册一行日志，见
// service/claude_web.go）。nil = 不观测；注册发生在服务启动/测试初始化（并发安全）。
var toolSimInjectionObserver atomic.Pointer[func(injectionBytes, tools int)]

// SetToolSimInjectionObserver 注册/注销注入段观测钩子（nil = 注销）。钩子在每次
// 注入渲染完成后同步调用一次（参数 = 注入段字节数、工具条数）。注册方保证钩子
// 自身不 panic（本包不 recover——观测绝不改渲染行为，也绝不成为新的失败点）。
func SetToolSimInjectionObserver(fn func(injectionBytes, tools int)) {
	if fn == nil {
		toolSimInjectionObserver.Store(nil)
		return
	}
	toolSimInjectionObserver.Store(&fn)
}

// RenderRequestWithOptions 读选项的渲染入口（pairs 出口腿用）。
func RenderRequestWithOptions(req *ir.Request, opts RenderOptions) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: nil request", ErrBadIR)
	}
	model := strings.TrimSpace(req.Model)
	if model == "" {
		return nil, fmt.Errorf("%w: model is required for claude_web upstream", ErrBadIR)
	}
	// 附件面（W22-1；接缝 W24-1）：图像块 → 载体解析（inline 解码 / URL 保留）
	// → 每份附件一个占位引用（v4 uuid 形态、36 字符）。占位引用写进 `files[]`，
	// 真实上传由服务壳在会话准备期执行并以 AttachmentJob.Resolve 原地回填；
	// 作业经 opts.AttachmentSink 交付（零全局状态；attach.go 头注）。
	atts, err := resolveAttachments(req.Messages)
	if err != nil {
		return nil, err
	}
	prompt, err := flattenPrompt(req.Messages)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(prompt) == "" && len(atts) == 0 {
		return nil, fmt.Errorf("%w: empty prompt (no text content in messages)", ErrBadIR)
	}
	if opts.ToolSim && len(req.Tools) > 0 {
		// S1 注入（设计单 §S1）：注入段置 prompt 最前（BK 的系统前缀位），与
		// 消息拍平文本之间空行分隔。tools 为空 → BuildInjection 返回 ""（直过）。
		if inj := toolsim.BuildInjection(req.Tools, req.ToolChoice); inj != "" {
			prompt = inj + "\n\n" + prompt
			// 配额观测（W21-2）：注入字节账进 sink + 观测钩子（纯观测，绝不进控制流）。
			if opts.Diag != nil {
				opts.Diag.ToolSimInjectionBytes = len(inj)
				opts.Diag.ToolSimTools = len(req.Tools)
			}
			if fn := toolSimInjectionObserver.Load(); fn != nil {
				(*fn)(len(inj), len(req.Tools))
			}
		}
	}
	effort, thinking := effortAndThinking(req.Extra)

	human, err := NewUUID()
	if err != nil {
		return nil, err
	}
	assistant, err := NewUUID()
	if err != nil {
		return nil, err
	}
	requestID, err := NewUUID()
	if err != nil {
		return nil, err
	}

	// 占位引用（每份附件一个；无附件 → `files` 恒 `[]`，与历史逐字节相等）。
	fileTokens := make([]string, 0, len(atts))
	for range atts {
		tok, err := NewUUID()
		if err != nil {
			return nil, err
		}
		fileTokens = append(fileTokens, tok)
	}
	filesField := json.RawMessage("[]")
	if len(fileTokens) > 0 {
		filesField = mustMarshal(fileTokens)
	}

	out := map[string]json.RawMessage{
		"prompt":        mustMarshal(prompt),
		"timezone":      mustMarshal(DefaultTimezone),
		"locale":        mustMarshal(DefaultLocale),
		"model":         mustMarshal(model),
		"effort":        mustMarshal(effort),
		"thinking_mode": mustMarshal(thinking),
		"tools":         mustMarshal(webTools),
		"turn_message_uuids": mustMarshal(turnMessageUUIDs{
			HumanMessageUUID:     human,
			AssistantMessageUUID: assistant,
		}),
		"attachments":           json.RawMessage("[]"),
		"files":                 filesField,
		"sync_sources":          json.RawMessage("[]"),
		"completion_request_id": mustMarshal(requestID),
		"rendering_mode":        mustMarshal(RenderingMode),
		"create_conversation_params": mustMarshal(conversationParams{
			Name:                           "",
			Model:                          model,
			IncludeConversationPreferences: true,
			ToolSearchMode:                 ToolSearchMode,
			IsTemporary:                    false,
			ChatMemoryMode:                 ChatMemoryMode,
			EnabledImagine:                 true,
		}),
	}
	body, err := marshalBytes(out)
	if err != nil {
		return nil, err
	}
	if len(atts) > 0 {
		// 交付附件作业（服务壳在会话准备期消费：上传 + 原地回填；attach.go 头注）。
		// 零全局状态：作业写进调用方传入的 sink（nil = 显式报错）。
		if err := deliverAttachmentJob(body, fileTokens, atts, opts.AttachmentSink); err != nil {
			return nil, err
		}
	}
	return body, nil
}

// RenderConversationCreate 会话创建体（`POST /api/organizations/{org}/chat_conversations`）。
//
// 形态（KB §② 三实现同形）：`{uuid, name:"", model, include_conversation_preferences:true}`。
// uuid 由**客户端生成**（CW/YS 都传自己生成的 uuid；W9-2 实测网页端连创建调用都
// 省略、直接把客户端生成的 uuid 放进 completion URL）——服务壳传入。
// model 可为空串（= 不指定，上游按账号默认档建会话）。
func RenderConversationCreate(uuid, model string) ([]byte, error) {
	if strings.TrimSpace(uuid) == "" {
		return nil, fmt.Errorf("%w: conversation uuid required", ErrBadIR)
	}
	body := map[string]json.RawMessage{
		"uuid":                             mustMarshal(uuid),
		"name":                             mustMarshal(""),
		"include_conversation_preferences": mustMarshal(true),
	}
	if m := strings.TrimSpace(model); m != "" {
		body["model"] = mustMarshal(m)
	}
	return marshalBytes(body)
}

// flattenPrompt IR 消息 → 单条 prompt 文本（网页线无多轮原生承载：5 实现里 4 个
// 拍平成单 prompt，KB §架构与数据流）。
//
// 前缀约定 = CW/YS 的 `System: `/`Human: `/`Assistant: `（YS utils/role.go:7-20、
// CW 同款；AU 的 `[System]` 与 BK 的 `**User**` 是等价变体，本包取前者）。
// 消息边界 = 空行（每条消息一段，段内块按序以换行拼接）。
//
// 块支持面（未接线面**显式报错**，绝不静默丢/发明占位）：
//   - text / thinking / refusal → 文本（思考内联为文本——网页线无 reasoning 字段，
//     与 CW/YS/BK 的 `<think>` 脚手架不同：这里连脚手架都不加，保真优先）；
//   - tool_use / server_tool_use / tool_result → 标签文本（工具历史回放面；
//     形态与理由见 flattenBlock 头注）；
//   - image → 不产文本（字节载体走 `files[]`；载体解析/无载体报错见 attach.go）；
//   - audio / 其余 → ErrUnsupportedBlock（音频面未接线）。
func flattenPrompt(messages []ir.Message) (string, error) {
	var b strings.Builder
	for _, m := range messages {
		if len(m.Blocks) == 0 {
			continue
		}
		var pieces []string
		for _, blk := range m.Blocks {
			piece, err := flattenBlock(blk)
			if err != nil {
				return "", fmt.Errorf("%w: block kind %q in role %q", err, blk.Kind, m.Role)
			}
			if piece == "" {
				continue
			}
			pieces = append(pieces, piece)
		}
		text := strings.TrimSpace(strings.Join(pieces, "\n"))
		if text == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString(rolePrefix(m.Role))
		b.WriteString(text)
	}
	return b.String(), nil
}

// flattenBlock 单块 → prompt 文本片段。
//
// 工具块形态（W11-3）：**标签 = 线格式块类型名**（`tool_use` / `tool_result` 逐字
// 来自实抓帧的 `content_block.type`，W9-2/W10-3 fixture 可查），属性/载荷逐字段
// 对应 IR：
//
//	<tool_use id="toolu_…" name="Read">{arguments JSON}</tool_use>
//	<tool_result tool_use_id="toolu_…">{content 文本}</tool_result>
//
// 选择理由（对照 BK 路线，见报告 §4）：BK 的 `<tool_calls>` / `<tool_result_json>`
// 是其标签 DSL 契约的一部分（模型被要求输出该 DSL、出口再解析回 tool_use 块）；
// 本仓不做工具模拟层（出口无 DSL 解析），借用 DSL 标签会向模型暗示一个无法兑现的
// 模仿协议。线格式块名是实抓可查的既有词汇（非发明），信息面与 BK 等价（id 配对 +
// 参数 JSON + 结果文本），故取之。
//
// 无 ToolCall/ToolResult 载荷的块 = IR 不完整 → ErrBadIR（与出口渲染同判）。
//
// 转义约定（W21-2 固化，与 toolsim/scan.go §转义约定 成对）：**不做 XML 转义**：
// 载荷为 JSON/文本原文（保真），转义会引入第二套编码约定、且与模型见过的注入
// 契约词表脱形；参数/正文里的 `</tool_use>` 子串冲突由出口解析的**候选试 JSON**
// 兜底（第一个体可解析的闭标签候选胜出，子串落入下一候选；全部候选不可解析 →
// 文本降级）。回放文本与注入契约同词表同形 = 模型零学习成本。
func flattenBlock(blk ir.ContentBlock) (string, error) {
	switch blk.Kind {
	case ir.BlockText, ir.BlockThinking, ir.BlockRefusal:
		return blk.Text, nil
	case ir.BlockImage:
		// 图像块不产 prompt 文本：字节载体在 `files[]`（渲染期产出附件作业、
		// 准备期上传回填——attach.go 头注）。载体解析/报错在 resolveAttachments
		// 完成（无载体面显式报错，绝不静默丢）。
		return "", nil
	case ir.BlockToolUse, ir.BlockServerToolUse:
		if blk.ToolCall == nil {
			return "", fmt.Errorf("%w: tool_use without ToolCall", ErrBadIR)
		}
		args := strings.TrimSpace(blk.ToolCall.Arguments)
		if args == "" {
			args = "{}"
		}
		return fmt.Sprintf(`<tool_use id=%q name=%q>%s</tool_use>`, blk.ToolCall.ID, blk.ToolCall.Name, args), nil
	case ir.BlockToolResult:
		if blk.ToolResult == nil {
			return "", fmt.Errorf("%w: tool_result without payload", ErrBadIR)
		}
		return fmt.Sprintf(`<tool_result tool_use_id=%q>%s</tool_result>`, blk.ToolResult.CallID, blk.ToolResult.Content), nil
	default:
		return "", fmt.Errorf("%w: block kind %q (attachment face not wired)", ErrUnsupportedBlock, blk.Kind)
	}
}

// rolePrefix 角色前缀（CW/YS 约定；未知角色用原值，不发明映射）。
func rolePrefix(r ir.Role) string {
	switch r {
	case ir.RoleSystem, ir.RoleDeveloper:
		return "System: "
	case ir.RoleUser:
		return "Human: "
	case ir.RoleAssistant:
		return "Assistant: "
	case ir.RoleTool:
		return "Tool: "
	default:
		if r == "" {
			return ""
		}
		return string(r) + ": "
	}
}

// effortAndThinking 请求参数 → (effort 档位, thinking_mode)。
//
// 档位表（W9-2 §2.4 实测：UI 5 档 低/中/高/超高/最大 ↔ 请求体 low/medium/high/
// xhigh/max；AU 同表 handlers/common.go:53-83）：
//
//	reasoning_effort: minimal→low / low→low / medium→medium / high→high /
//	                  xhigh→xhigh / max→max；none/auto/未知 → 缺省 medium。
//	thinking_mode:    缺省 auto；reasoning_effort=none（或 Claude Code 的
//	                  thinking{type:"disabled"}）→ none（AU 同款映射）。
//
// `reasoning_effort` 本身**不落上游体**（不在实抓字段集内 → 上游会报错），只做
// 档位映射（与 claude_messages 的 thinkingForEffort 同一纪律）。
func effortAndThinking(extra map[string]json.RawMessage) (effort, thinking string) {
	effort = DefaultEffort
	thinking = ThinkingModeAuto
	if raw, ok := extra["reasoning_effort"]; ok {
		var s string
		if json.Unmarshal(raw, &s) == nil {
			switch strings.ToLower(strings.TrimSpace(s)) {
			case "minimal", "low":
				effort = "low"
			case "medium":
				effort = "medium"
			case "high":
				effort = "high"
			case "xhigh":
				effort = "xhigh"
			case "max":
				effort = "max"
			case "none", "disabled":
				thinking = ThinkingModeNone
			}
		}
	}
	// Claude Code 的 thinking 字段（IR Extra 保真面）：type=disabled → none。
	if raw, ok := extra["thinking"]; ok {
		var th struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(raw, &th) == nil {
			switch strings.ToLower(strings.TrimSpace(th.Type)) {
			case "disabled":
				thinking = ThinkingModeNone
			case "enabled", "adaptive":
				thinking = ThinkingModeAuto
			}
		}
	}
	return effort, thinking
}

// ---------------------------------------------------------------------------
// 非流式响应面（由流式装配——上游只有流式面）
// ---------------------------------------------------------------------------

// ParseResponse 非流式响应体 → IR 响应。
//
// **上游只有流式面**（completion 恒 SSE，无非流式完成端点）：本函数把 SSE 字节流
// 按 ParseStream 解析后装配成 IR 响应（文本/思考块拼接、stop_reason/stop_sequence、
// 配额 usage）。服务壳在 `stream:false` 的下游请求上走这条路（上游仍按流式调用）。
func ParseResponse(body []byte) (*ir.Response, error) {
	events, err := ParseStream(strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	return assembleResponse(events)
}

// assembleResponse 流事件 → 非流式 IR 响应（choice 0 语义）。
//
// 装配规则：**按块**（不是一条大串）——每个源块按其类型装配成一个 IR 块，保持
// 块序（W10-3 实抓：thinking→text→tool_use→tool_result→… 交错）：
//   - thinking → BlockThinking（思考摘要文本）；
//   - text → BlockText；
//   - tool_use（上游内置工具）→ BlockServerToolUse（参数经 ir.AssembleToolCalls
//     合并；半 JSON 截断显式报错）；
//   - tool_result / 未建模块 → 跳过（上游内部工具的显示面产物：IR 无对应装配面，
//     且它们不是客户端的 tool 消息——非流式出口没有合法载体）。
//
// stop_reason 取最后一个 message_delta 的值；stop_sequence 同源回填（W52-1：
// 配额截断 `usage_limit` 等的非流式保真——空 = 未提供）；usage 取最后一个带
// Usage 的事件（message_limit 的配额面）。**无 message_stop 的截断流显式报错**
// （不静默）。
func assembleResponse(events []ir.StreamEvent) (*ir.Response, error) {
	resp := &ir.Response{}
	type acc struct {
		kind ir.BlockKind
		text strings.Builder
	}
	blocks := map[int]*acc{}
	var order []int
	stopped := false
	started := false
	for _, ev := range events {
		switch ev.Kind {
		case ir.EventStart:
			started = true
			resp.ID = ev.MessageID
			resp.Model = ev.Model
			resp.Created = ev.Created
		case ir.EventBlockStart:
			if _, dup := blocks[ev.BlockIndex]; !dup {
				blocks[ev.BlockIndex] = &acc{kind: ev.BlockKind}
				order = append(order, ev.BlockIndex)
			}
		case ir.EventBlockDelta:
			if a, ok := blocks[ev.BlockIndex]; ok {
				a.text.WriteString(ev.TextDelta)
			}
		case ir.EventMessageDelta:
			if ev.StopReason != "" {
				resp.StopReason = ev.StopReason
			}
			// W52-1：stop_sequence 细节面回填（claude.ai 配额截断 `usage_limit`
			// 的流式信号——非流式装配后经 claude 出口 RenderResponse 保真；
			// 与 claude_messages 流式解析同款「非空才回填」）。
			if ev.StopSequence != "" {
				resp.StopSequence = ev.StopSequence
			}
			if ev.Usage != nil {
				resp.Usage = ev.Usage
			}
		case ir.EventMessageStop:
			stopped = true
		case ir.EventError:
			if ev.Error != nil {
				return nil, fmt.Errorf("claude_web: upstream stream error: %s", ev.Error.Message)
			}
			return nil, fmt.Errorf("claude_web: upstream stream error (%s)", ev.ErrorReason)
		}
	}
	if !started {
		return nil, fmt.Errorf("%w: no message_start in upstream body", ErrBadEvent)
	}
	if !stopped {
		return nil, fmt.Errorf("%w: stream closed before message_stop", ErrBadEvent)
	}
	calls, err := ir.AssembleToolCalls(events)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadEvent, err)
	}
	// 块序 ↔ 调用对应：block_start 的 ToolCallID 与合并后的 ToolCall.ID 同源
	//（AssembleToolCalls 取 start 的 id/name）。
	indexByCallID := map[string]int{}
	for _, ev := range events {
		if ev.Kind == ir.EventBlockStart && ev.ToolCallID != "" {
			indexByCallID[ev.ToolCallID] = ev.BlockIndex
		}
	}
	callByIndex := map[int]*ir.ToolCall{}
	for i := range calls {
		if idx, ok := indexByCallID[calls[i].ID]; ok {
			callByIndex[idx] = &calls[i]
		}
	}
	out := make([]ir.ContentBlock, 0, len(order))
	for _, idx := range order {
		a := blocks[idx]
		switch a.kind {
		case ir.BlockThinking:
			if s := a.text.String(); s != "" {
				out = append(out, ir.ContentBlock{Kind: ir.BlockThinking, Text: s})
			}
		case ir.BlockText, ir.BlockRefusal:
			if s := a.text.String(); s != "" {
				out = append(out, ir.ContentBlock{Kind: ir.BlockText, Text: s})
			}
		case ir.BlockToolUse, ir.BlockServerToolUse:
			if c, ok := callByIndex[idx]; ok {
				out = append(out, ir.ContentBlock{Kind: ir.BlockServerToolUse, ToolCall: c})
			}
		default:
			// tool_result / 未建模块：跳过（见本函数头注）。
		}
	}
	resp.Message = ir.Message{Role: ir.RoleAssistant, Blocks: out}
	return resp, nil
}

// ---------------------------------------------------------------------------
// 不存在面（上游叶子格式）：显式报错占位
// ---------------------------------------------------------------------------

// ParseRequest claude_web 不是下游入口格式：无请求解析面。
func ParseRequest([]byte) (*ir.Request, error) {
	return nil, fmt.Errorf("%w: parse request", ErrUnsupportedFace)
}

// RenderResponse claude_web 不是下游出口格式：无非流式响应渲染面。
func RenderResponse(*ir.Response) ([]byte, error) {
	return nil, fmt.Errorf("%w: render response", ErrUnsupportedFace)
}

// RenderStream claude_web 不是下游出口格式：无流式渲染面。
func RenderStream([]ir.StreamEvent) ([]byte, error) {
	return nil, fmt.Errorf("%w: render stream", ErrUnsupportedFace)
}

// RenderError claude_web 不是下游出口格式：无错误信封渲染面（上游错误体由服务壳
// 经入口协议渲染）。
func RenderError(*ir.Error) ([]byte, error) {
	return nil, fmt.Errorf("%w: render error", ErrUnsupportedFace)
}

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

// NewUUID UUID v4（crypto/rand；无第三方依赖——实抓体的 uuid/turn_message_uuids/
// completion_request_id 都是 v4 形态，RFC 4122 变体位）。
//
// 导出理由：会话创建体与 completion URL 的 conversation uuid 由**客户端生成**
// （CW/YS 同款；W9-2 实测网页端也是客户端生成），服务壳要用同一个生成器。
func NewUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("claude_web: crypto/rand: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC 4122
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32], nil
}

func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic("claude_web: marshal: " + err.Error())
	}
	return b
}

func marshalBytes(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("claude_web: marshal: %w", err)
	}
	return b, nil
}
