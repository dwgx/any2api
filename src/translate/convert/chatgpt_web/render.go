// 本文件 = render 面（W30-2 S3 接线）：IR 请求 → chatgpt 匿名（Android anon）
// 体 + 非流式响应装配（ParseResponse）。
//
// 面覆盖：
//   - RenderRequest：IR 请求 → `/backend-anon/f/conversation{,/prepare}` 体
//     （**同一个体**——W29-1 §2.1 样本：prepare 与 f/conversation 用同一份
//     anonBody 字节；服务壳两次请求同体）；
//   - ParseResponse：completion 恒 SSE（无非流式完成端点）——SSE 字节经
//     ParseStream 装配成非流式 IR 响应（服务壳 `stream:false` 下游走这条路）；
//   - 入口面（ParseRequest）与下游出口面（RenderResponse/RenderStream/
//     RenderError）：chatgpt_web 是**上游叶子格式**，这些面不存在 → 显式报错
//     （ErrUnsupportedFace），绝不返回半成品字节（claude_web 同款）。
//
// 请求体形状 = **W29-1 §2.1 实抓原文**（零账号匿名链路；`2_prepare.json` 的
// `sent_body` 逐字段）：
//
//	{"action":"next","messages":[{"author":{"role":"user"},
//	 "content":{"content_type":"text","parts":["<prompt>"]}}],
//	 "parentMessageId":null,"conversationId":null,"model":"auto",
//	 "historyAndTrainingDisabled":false,
//	 "conversationMode":{"kind":"primary_assistant"},"forceUseSearch":false,
//	 "forceUseSse":true,"supported_encodings":["v1"],"timezone":"UTC",
//	 "timezoneOffsetMin":0,"noAuthAdPreferences":true}
//
// 关键字段裁定（逐条注明证据）：
//
//   - `supported_encodings` 取 **snake_case** `["v1"]`：W29-1 实证 camelCase
//     （`supportedEncodings`）被服务端忽略（死字段，E repo `translate.ts:118-170`
//     的 camelCase 体因此恒回快照档）——snake 才回 **v1 JSON-Patch SSE**（本仓
//     解析器主档，三 golden 两份 v1）。快照档亦兼容（解析器双档），但 v1 是更强
//     协议（增量 + 状态机 + `c` 计数器观测），故主动协商 v1。
//   - `model`：IR 模型非空 → 原样（上游按 slug 解析，未知模型 404 = 上游侧健康
//     信号，KB quirk 10）；空 → `"auto"`（样本值；guest 档实际解析出 `gpt-6`，
//     经 `ObservedModel()` 回写响应面）。
//   - `parentMessageId`/`conversationId` 恒 `null`：本服务**无状态**（每请求新
//     会话；多轮由下游每次带全量历史——与 claude_web 的「每请求新建会话」同
//     纪律），不持有跨请求的会话锚点。
//   - 消息结构 = 样本最小形（`author.role` + `content{content_type:"text",
//     parts}`）：**全量历史逐条进 `messages[]`**（无状态代理的正确多轮承载；
//     gpt2api `fchat.go` / chat2api `chatFormat.py:391-467` 同为 messages 数组
//     逐条映射，role 逐字透传）。role 透传面（user/assistant/system/tool）对匿名
//     档的接受度**未实抓**（[UNVERIFIED]——样本只有单条 user；登录档骨架与两
//     实现同款透传）——真机校准属 S4/S5，届时按实抓裁定映射。
//   - **tools 定义不渲染**：匿名线无 tools 字段（KB §格式兼容面「tool-call 无
//     原生协议」——三条社区路线全在**出口**侧；请求侧无载体）。静默丢弃 = 有
//     意缺省（工具模拟面 = 后续 slice；渲染面不发明无载体的字段，也绝不把工具
//     定义拼进 prompt 冒充契约）。工具**历史**块（tool_use/tool_result）则渲染
//     为标签文本保真（见 renderMessage）。
//   - 不渲染样本外字段（`thinkingEffort`/`serviceTier`/`systemHints`/…——CF 的
//     camelCase 扩展面）：本 slice 最小面，字段集逐字对齐实抓样本；扩展面后续
//     按实抓校准（上游对未知字段的容忍度未证，[UNVERIFIED]）。
package chatgpt_web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dwgx/any2api/translate/ir"
)

// anonContentTypeText 消息内容类型（样本唯一取值；multimodal_text 属附件面，
// 本 slice 不渲染）。
const anonContentTypeText = "text"

// anonMessage chatgpt 匿名体的一条消息（样本最小形：author + content）。
type anonMessage struct {
	Author  anonAuthor  `json:"author"`
	Content anonContent `json:"content"`
}

// anonAuthor 消息作者（role 逐字透传 IR 角色——gpt2api/chat2api 同款）。
type anonAuthor struct {
	Role string `json:"role"`
}

// anonContent 消息内容（样本形：content_type + parts 字符串数组）。
type anonContent struct {
	ContentType string   `json:"content_type"`
	Parts       []string `json:"parts"`
}

// anonConversationMode 会话模式（样本值 `primary_assistant`；gizmo 面未接线）。
type anonConversationMode struct {
	Kind string `json:"kind"`
}

// anonBody chatgpt 匿名请求体（字段序 = W29-1 样本 `sent_body` 原文序；marshal
// 输出对拍用）。prepare 与 f/conversation **同体**（样本同字节）。
type anonBody struct {
	Action                     string               `json:"action"`
	Messages                   []anonMessage        `json:"messages"`
	ParentMessageID            *string              `json:"parentMessageId"`
	ConversationID             *string              `json:"conversationId"`
	Model                      string               `json:"model"`
	HistoryAndTrainingDisabled bool                 `json:"historyAndTrainingDisabled"`
	ConversationMode           anonConversationMode `json:"conversationMode"`
	ForceUseSearch             bool                 `json:"forceUseSearch"`
	ForceUseSse                bool                 `json:"forceUseSse"`
	SupportedEncodings         []string             `json:"supported_encodings"`
	Timezone                   string               `json:"timezone"`
	TimezoneOffsetMin          int                  `json:"timezoneOffsetMin"`
	NoAuthAdPreferences        bool                 `json:"noAuthAdPreferences"`
}

// anonModelAuto 模型缺省值（样本值；guest 档服务端解析实际模型）。
const anonModelAuto = "auto"

// anonEncodingV1 编码协商值（snake_case；camelCase 是死字段——见文件头注）。
const anonEncodingV1 = "v1"

// RenderRequest IR 请求 → chatgpt 匿名体（prepare 与 f/conversation 同体）。
//
// 失败面（显式报错，绝不静默降级）：nil 请求 / 零消息 / 全部消息不可渲染 /
// 不可承载的块（image/audio/…——无载体面显式报错，ErrUnsupportedBlock）。
func RenderRequest(req *ir.Request) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: nil request", ErrUnsupportedFace)
	}
	if len(req.Messages) == 0 {
		return nil, fmt.Errorf("%w: no messages", ErrBadIR)
	}
	msgs := make([]anonMessage, 0, len(req.Messages))
	for i, m := range req.Messages {
		text, err := renderMessage(m)
		if err != nil {
			return nil, fmt.Errorf("message %d (role %q): %w", i, m.Role, err)
		}
		if text == "" {
			continue // 空消息（无块/块全空）不进体（claude_web flattenPrompt 同纪律）
		}
		msgs = append(msgs, anonMessage{
			Author:  anonAuthor{Role: string(m.Role)},
			Content: anonContent{ContentType: anonContentTypeText, Parts: []string{text}},
		})
	}
	if len(msgs) == 0 {
		return nil, fmt.Errorf("%w: no renderable messages", ErrBadIR)
	}
	model := strings.TrimSpace(req.Model)
	if model == "" {
		model = anonModelAuto
	}
	body := anonBody{
		Action:                     "next",
		Messages:                   msgs,
		ParentMessageID:            nil, // null：无状态（每请求新会话）
		ConversationID:             nil,
		Model:                      model,
		HistoryAndTrainingDisabled: false,
		ConversationMode:           anonConversationMode{Kind: "primary_assistant"},
		ForceUseSearch:             false,
		ForceUseSse:                true,
		SupportedEncodings:         []string{anonEncodingV1},
		Timezone:                   "UTC",
		TimezoneOffsetMin:          0,
		NoAuthAdPreferences:        true,
	}
	out, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("chatgpt_web: marshal request body: %w", err)
	}
	return out, nil
}

// renderMessage IR 消息 → 消息文本（块按序以换行拼接；空白修剪）。
//
// 块支持面（未接线面**显式报错**，绝不静默丢/发明占位）：
//   - text / thinking / refusal → 文本（思考内联为文本——匿名线无 reasoning 字段；
//     与 claude_web flattenBlock 同款：不加脚手架，保真优先）；
//   - tool_use / server_tool_use / tool_result → 标签文本（工具历史回放面；形态与
//     理由 = claude_web flattenBlock 头注：标签用线格式块名，不做 XML 转义）；
//   - image / audio / citations / executable_code / code_execution_result /
//     unknown → ErrUnsupportedBlock（本 slice 无附件/代码块载体面；[构造] 面）。
func renderMessage(m ir.Message) (string, error) {
	var pieces []string
	for _, blk := range m.Blocks {
		piece, err := renderBlock(blk)
		if err != nil {
			return "", err
		}
		if piece == "" {
			continue
		}
		pieces = append(pieces, piece)
	}
	return strings.TrimSpace(strings.Join(pieces, "\n")), nil
}

// renderBlock 单块 → 消息文本片段（形态与 claude_web flattenBlock 逐字同款——
// 同一标签词汇表，模型在两条网页线上看到的历史回放文本一致）。
func renderBlock(blk ir.ContentBlock) (string, error) {
	switch blk.Kind {
	case ir.BlockText, ir.BlockThinking, ir.BlockRefusal:
		return blk.Text, nil
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
		return "", fmt.Errorf("%w: block kind %q (attachment/code face not wired)", ErrUnsupportedBlock, blk.Kind)
	}
}

// ---------------------------------------------------------------------------
// 非流式响应面（由流式装配——上游只有流式面）
// ---------------------------------------------------------------------------

// ParseResponse 非流式响应体 → IR 响应。
//
// **上游只有流式面**（completion 恒 SSE，无非流式完成端点）：本函数把 SSE 字节流
// 按 ParseStream 解析后装配成 IR 响应（文本/思考/代码块拼接、stop_reason）。
// 服务壳在 `stream:false` 的下游请求上走这条路（上游仍按流式调用；有界尾读见
// service/chatgpt_web.go 的 readChatGPTWebCompletion）。
func ParseResponse(body []byte) (*ir.Response, error) {
	events, err := ParseStream(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	return assembleResponse(events)
}

// assembleResponse 流事件 → 非流式 IR 响应（choice 0 语义；claude_web
// assembleResponse 同构，面更薄：chatgpt 无 tool_use/tool_result 块）。
//
// 装配规则：**按块**（不是一条大串）——每个源块按其类型装配成一个 IR 块，保持
// 块序（text/thinking 交错时出口块序有约束）：
//   - text → BlockText；thinking → BlockThinking；
//   - executable_code → BlockExecutableCode；code_execution_result →
//     BlockCodeExecutionResult（解析器 content_type 分支的产物，逐块保真）；
//   - 其余块类型 → 显式报错（解析器不产它们；出现 = 状态机被绕过，绝不静默）。
//
// stop_reason 取最后一个 message_delta 的值；**usage 恒无**（上游从不给 token
// 数——绝不合成，W28-1 §2.6）。无 start（空流）/ 无 message_stop（截断流）显式
// 报错（不静默补帧；与 claude_web 同判）。
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
			// usage 面恒无（绝不合成——上游不给 token 数）。
		case ir.EventMessageStop:
			stopped = true
		case ir.EventError:
			if ev.Error != nil {
				return nil, fmt.Errorf("chatgpt_web: upstream stream error: %s", ev.Error.Message)
			}
			return nil, fmt.Errorf("chatgpt_web: upstream stream error (%s)", ev.ErrorReason)
		}
	}
	if !started {
		return nil, fmt.Errorf("%w: no start event in upstream body", ErrBadEvent)
	}
	if !stopped {
		return nil, fmt.Errorf("%w: stream closed before terminal signal", ErrBadEvent)
	}
	out := make([]ir.ContentBlock, 0, len(order))
	for _, idx := range order {
		a := blocks[idx]
		switch a.kind {
		case ir.BlockText, ir.BlockThinking, ir.BlockExecutableCode, ir.BlockCodeExecutionResult:
			if s := a.text.String(); s != "" {
				out = append(out, ir.ContentBlock{Kind: a.kind, Text: s})
			}
		default:
			return nil, fmt.Errorf("%w: unexpected block kind %q in upstream stream", ErrBadEvent, a.kind)
		}
	}
	resp.Message = ir.Message{Role: ir.RoleAssistant, Blocks: out}
	return resp, nil
}

// ---------------------------------------------------------------------------
// 不存在的面（上游叶子格式）
// ---------------------------------------------------------------------------

// ParseRequest chatgpt_web 不是下游入口格式：无请求解析面（服务壳绝不把
// chatgpt 体当入口；[构造] 面由测试经 IR 直构，不经本包）。
func ParseRequest([]byte) (*ir.Request, error) {
	return nil, fmt.Errorf("%w: parse request", ErrUnsupportedFace)
}

// RenderResponse chatgpt_web 不是下游出口格式：无非流式响应渲染面。
func RenderResponse(*ir.Response) ([]byte, error) {
	return nil, fmt.Errorf("%w: render response", ErrUnsupportedFace)
}

// RenderStream chatgpt_web 不是下游出口格式：无流式渲染面。
func RenderStream([]ir.StreamEvent) ([]byte, error) {
	return nil, fmt.Errorf("%w: render stream", ErrUnsupportedFace)
}

// RenderError chatgpt_web 不是下游出口格式：无错误信封渲染面（上游错误体由
// 服务壳经入口协议渲染）。
func RenderError(*ir.Error) ([]byte, error) {
	return nil, fmt.Errorf("%w: render error", ErrUnsupportedFace)
}
