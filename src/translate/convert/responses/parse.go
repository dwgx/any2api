package responses

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/dwgx/any2api/translate/hygiene"
	"github.com/dwgx/any2api/translate/ir"
)

// ---------------------------------------------------------------------------
// 请求：线格式 → IR
// ---------------------------------------------------------------------------

// ParseRequest 解析 POST /v1/responses 请求体。
// 宽容读取（缺省=不变、未知=忽略）；状态化/托管字段显式拒绝（ErrStatefulFields）；
// 透传族以 **chat 规范键名**落 Extra（跨格式腿的渲染器按各自白名单消费）。
func ParseRequest(data []byte) (*ir.Request, error) {
	var w requestWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	if strings.TrimSpace(w.Model) == "" {
		return nil, fmt.Errorf("%w: model is required", ErrBadRequest)
	}
	if err := checkStatefulFields(&w); err != nil {
		return nil, err
	}

	req := &ir.Request{Model: w.Model, Stream: w.Stream}
	if w.StreamOptions != nil {
		req.StreamOptions = &ir.StreamOptions{IncludeObfuscation: w.StreamOptions.IncludeObfuscation}
	}
	// instructions → 首条 system 消息（表 1「系统指令」行；string 形态）。
	if s := rawString(w.Instructions); s != "" {
		req.Messages = append(req.Messages, ir.Message{
			Role:   ir.RoleSystem,
			Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: s}},
		})
	}

	msgs, extraTools, err := parseInput(w.Input)
	if err != nil {
		return nil, err
	}
	req.Messages = append(req.Messages, msgs...)

	for i, raw := range w.Tools {
		td, err := parseToolDef(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: tool %d: %w", ErrBadRequest, i, err)
		}
		req.Tools = append(req.Tools, td)
	}
	// additional_tools（Codex Desktop「Responses Lite」把工具塞在 input[] 里）：
	// 与顶层 tools 合并扫描（CPA `openai_openai-responses_tools.go:96-105`；表 1 行）。
	req.Tools = append(req.Tools, extraTools...)

	tc, err := canonicalToolChoice(w.ToolChoice)
	if err != nil {
		return nil, err
	}
	req.ToolChoice = tc
	req.Extra = requestExtras(data, &w)
	return req, nil
}

// checkStatefulFields 状态化/托管字段硬报错（表 1「状态化字段」行 + 请求面表行）。
// new-api 只检 4 个（conversation/previous_response_id/prompt/context_management），
// 本包按表把 background/moderation 一并拒绝——静默丢 = 语义改变（异步执行模型与
// 托管审查无法用同步 chat 腿表达），见报告 §决策。
func checkStatefulFields(w *requestWire) error {
	var unsupported []string
	if strings.TrimSpace(w.PreviousResponseID) != "" {
		unsupported = append(unsupported, "previous_response_id")
	}
	for name, raw := range map[string]json.RawMessage{
		"conversation":       w.Conversation,
		"prompt":             w.Prompt,
		"context_management": w.ContextManagement,
		"background":         w.Background,
		"moderation":         w.Moderation,
	} {
		if hasRaw(raw) {
			unsupported = append(unsupported, name)
		}
	}
	if len(unsupported) == 0 {
		return nil
	}
	sortStrings(unsupported)
	return fmt.Errorf("%w: %s", ErrStatefulFields, strings.Join(unsupported, ", "))
}

// responsesOnlyDropped 无 chat/claude 对应且表未映射的顶层键：解析产物不落 Extra
// （渲染回 responses 时也不回写；表 1 未列 include/truncation/max_tool_calls 的映射，
// new-api 亦丢弃 include）。
var responsesOnlyDropped = []string{
	"include", "truncation", "max_tool_calls", "text", "reasoning",
	"max_output_tokens", "instructions",
}

// requestExtras 计算请求 Extra：passthrough 族用 chat 规范键名，三处形状差异键
// （max_output_tokens / text.format / reasoning）显式改名。
func requestExtras(data []byte, w *requestWire) map[string]json.RawMessage {
	extra := extrasOf(data, "model", "input", "instructions", "tools", "tool_choice",
		"stream", "stream_options", "text", "reasoning", "max_output_tokens")
	for _, k := range responsesOnlyDropped {
		delete(extra, k)
	}
	// 输出上限：max_output_tokens → max_completion_tokens（表 1「输出上限」行；
	// <16 的显式值照转——官方拒绝由上游裁决，new-api 同款透传）。
	if hasRaw(w.MaxOutputTokens) {
		if extra == nil {
			extra = map[string]json.RawMessage{}
		}
		extra["max_completion_tokens"] = w.MaxOutputTokens
	}
	// 结构化输出：text.format → 顶层 response_format（json_schema 时**整体搬进**
	// 嵌套 json_schema 对象，表 1「结构化输出」行）。
	if fmtRaw := textFormatToResponseFormat(w.Text); hasRaw(fmtRaw) {
		if extra == nil {
			extra = map[string]json.RawMessage{}
		}
		extra["response_format"] = fmtRaw
	}
	// 推理：reasoning.effort → reasoning_effort（表 1「推理」行；summary/context/mode
	// 无 chat 对应，丢弃——表未映射）。
	if eff := reasoningEffort(w.Reasoning); hasRaw(eff) {
		if extra == nil {
			extra = map[string]json.RawMessage{}
		}
		extra["reasoning_effort"] = eff
	}
	return extra
}

// textFormatToResponseFormat responses `text.format` → chat `response_format`。
// 形状差异：responses 是扁平 {type:"json_schema",name,schema,strict}，chat 是
// {type:"json_schema",json_schema:{name,schema,strict}}（表 1 行）。
func textFormatToResponseFormat(text json.RawMessage) json.RawMessage {
	if !hasRaw(text) {
		return nil
	}
	var tw struct {
		Format json.RawMessage `json:"format"`
	}
	if err := json.Unmarshal(text, &tw); err != nil || !hasRaw(tw.Format) {
		return nil
	}
	var fw struct {
		Type   string          `json:"type"`
		Name   string          `json:"name"`
		Schema json.RawMessage `json:"schema"`
		Strict *bool           `json:"strict"`
	}
	if err := json.Unmarshal(tw.Format, &fw); err != nil {
		return nil
	}
	switch fw.Type {
	case "text", "":
		return nil
	case "json_object":
		return mustMarshal(map[string]json.RawMessage{"type": mustMarshal("json_object")})
	case "json_schema":
		inner := map[string]json.RawMessage{"name": mustMarshal(fw.Name)}
		if hasRaw(fw.Schema) {
			inner["schema"] = fw.Schema
		}
		if fw.Strict != nil {
			inner["strict"] = mustMarshal(*fw.Strict)
		}
		return mustMarshal(map[string]json.RawMessage{
			"type":        mustMarshal("json_schema"),
			"json_schema": mustMarshal(inner),
		})
	default:
		return nil
	}
}

// reasoningEffort reasoning.effort → 规范键值（none/minimal/low/medium/high/xhigh/max）。
func reasoningEffort(reasoning json.RawMessage) json.RawMessage {
	if !hasRaw(reasoning) {
		return nil
	}
	var rw struct {
		Effort json.RawMessage `json:"effort"`
	}
	if err := json.Unmarshal(reasoning, &rw); err != nil {
		return nil
	}
	if !hasRaw(rw.Effort) {
		return nil
	}
	return rw.Effort
}

// canonicalToolChoice responses tool_choice → IR 规范形（chat 形状）。
// `"auto"|"none"|"required"` 直通；`{"type":"function","name":…}`（**扁平**）→
// `{"type":"function","function":{"name":…}}`（表 1「工具选择」行）；
// allowed_tools/mcp 等无 IR 表达 → 显式报错（绝不透传给下游上游）。
func canonicalToolChoice(raw json.RawMessage) (json.RawMessage, error) {
	if !hasRaw(raw) {
		return nil, nil
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadRequest, err)
		}
		switch s {
		case "auto", "none", "required":
			return mustMarshal(s), nil
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnsupportedToolChoice, s)
		}
	case '{':
		var w struct {
			Type string `json:"type"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &w); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadRequest, err)
		}
		if w.Type != "function" {
			return nil, fmt.Errorf("%w: %s", ErrUnsupportedToolChoice, truncate(string(raw), 80))
		}
		if w.Name == "" {
			return nil, fmt.Errorf("%w: function without name", ErrUnsupportedToolChoice)
		}
		return mustMarshal(map[string]json.RawMessage{
			"type":     mustMarshal("function"),
			"function": mustMarshal(map[string]json.RawMessage{"name": mustMarshal(w.Name)}),
		}), nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedToolChoice, truncate(string(raw), 80))
	}
}

// parseToolDef 解析 tools[] 项（**扁平** function 形态）。hosted 工具类型无
// chat/claude 对应 → 显式报错（表 1 只映射 function）。
func parseToolDef(raw json.RawMessage) (ir.ToolDef, error) {
	var tw functionToolWire
	if err := json.Unmarshal(raw, &tw); err != nil {
		return ir.ToolDef{}, fmt.Errorf("%w: %v", ErrUnsupportedToolType, err)
	}
	if tw.Type != "" && tw.Type != "function" {
		return ir.ToolDef{}, fmt.Errorf("%w: %q", ErrUnsupportedToolType, tw.Type)
	}
	if tw.Name == "" {
		return ir.ToolDef{}, fmt.Errorf("%w: function tool without name", ErrBadRequest)
	}
	td := ir.ToolDef{Name: tw.Name, Description: tw.Description, Strict: tw.Strict}
	if hasRaw(tw.Parameters) {
		td.InputSchema = tw.Parameters
	}
	td.Extra = extrasOf(raw, "type", "name", "description", "parameters", "strict")
	return td, nil
}

// parseInput 解析 input（字符串 | items[]）。返回消息与 input 内联工具声明。
func parseInput(raw json.RawMessage) ([]ir.Message, []ir.ToolDef, error) {
	if !hasRaw(raw) {
		return nil, nil, fmt.Errorf("%w: input is required", ErrBadRequest)
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, nil, fmt.Errorf("%w: input: %v", ErrBadRequest, err)
		}
		if s == "" {
			return nil, nil, nil
		}
		return []ir.Message{{
			Role:   ir.RoleUser,
			Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: s}},
		}}, nil, nil
	case '[':
	default:
		return nil, nil, fmt.Errorf("%w: input must be a string or an array of items", ErrBadRequest)
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, nil, fmt.Errorf("%w: input: %v", ErrBadRequest, err)
	}
	var (
		msgs      []ir.Message
		tools     []ir.ToolDef
		knownCall = map[string]bool{}
	)
	for i, item := range items {
		m, t, err := parseInputItem(item, knownCall)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: input item %d: %w", ErrBadRequest, i, err)
		}
		msgs = append(msgs, m...)
		tools = append(tools, t...)
	}
	return msgs, tools, nil
}

// parseInputItem 解析单个 input 项（表 1 请求映射：message / function_call /
// custom_tool_call / function_call_output / reasoning / additional_tools）。
func parseInputItem(item json.RawMessage, knownCall map[string]bool) ([]ir.Message, []ir.ToolDef, error) {
	var it inputItemWire
	if err := json.Unmarshal(item, &it); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrUnsupportedInputItem, err)
	}
	switch it.Type {
	case "", itemTypeMessage:
		role := ir.Role(strings.TrimSpace(it.Role))
		if role == "" {
			role = ir.RoleUser // role 缺省 user（表 1 行）
		}
		blocks, err := parseInputContent(it.Content)
		if err != nil {
			return nil, nil, err
		}
		if len(blocks) == 0 {
			return nil, nil, nil
		}
		return []ir.Message{{Role: role, Blocks: blocks}}, nil, nil

	case itemTypeFunctionCall:
		blk, err := toolUseBlock(it.CallID, it.Name, rawString(it.Arguments))
		if err != nil {
			return nil, nil, err
		}
		markCall(knownCall, it.CallID)
		return []ir.Message{{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{blk}}}, nil, nil

	case itemTypeCustomTool:
		// 自定义工具：arguments = {"input": <raw>}（单键包装；表 1 行）。
		args := mustMarshal(map[string]json.RawMessage{"input": mustMarshal(rawString(it.Input))})
		blk, err := toolUseBlock(it.CallID, it.Name, string(args))
		if err != nil {
			return nil, nil, err
		}
		markCall(knownCall, it.CallID)
		return []ir.Message{{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{blk}}}, nil, nil

	case itemTypeFuncOutput, itemTypeCustomOutput:
		text, media, err := parseToolOutput(it.Output)
		if err != nil {
			return nil, nil, err
		}
		if it.CallID == "" || !knownCall[it.CallID] {
			// 孤儿工具结果：折叠为普通文本（统一政策，文案见 hygiene——空白内容
			// 显式占位，绝不产出空 text 块），**不得**变 tool 消息（CPA 路线；
			// 表 1「孤儿工具结果」行）。
			blocks := append([]ir.ContentBlock{{
				Kind: ir.BlockText, Text: hygiene.FoldOrphanToolResultText(text),
			}}, media...)
			return []ir.Message{{Role: ir.RoleUser, Blocks: blocks}}, nil, nil
		}
		msgs := []ir.Message{{
			Role: ir.RoleTool,
			Blocks: []ir.ContentBlock{{Kind: ir.BlockToolResult, ToolResult: &ir.ToolResult{
				CallID: it.CallID, Content: text,
			}}},
		}}
		if len(media) > 0 {
			// 媒体提升为后续 user 消息（保持 tool 消息连续；表 1 行）。
			msgs = append(msgs, ir.Message{Role: ir.RoleUser, Blocks: media})
		}
		return msgs, nil, nil

	case itemTypeReasoning:
		blocks, err := reasoningBlocks(it.Summary, partsOf(it.Content), it.EncryptedContent)
		if err != nil {
			return nil, nil, err
		}
		return []ir.Message{{Role: ir.RoleAssistant, Blocks: blocks}}, nil, nil

	case itemTypeAdditionalTls:
		var tools []ir.ToolDef
		for i, raw := range it.Tools {
			td, err := parseToolDef(raw)
			if err != nil {
				return nil, nil, fmt.Errorf("additional_tools %d: %w", i, err)
			}
			tools = append(tools, td)
		}
		return nil, tools, nil

	case "item_reference":
		return nil, nil, fmt.Errorf("%w: %q requires server-side state (previous_response_id/store)", ErrUnsupportedInputItem, it.Type)
	default:
		return nil, nil, fmt.Errorf("%w: %q", ErrUnsupportedInputItem, it.Type)
	}
}

func markCall(known map[string]bool, callID string) {
	if callID != "" {
		known[callID] = true
	}
}

// partsOf 把部件数组字段（原始 JSON）解成 []json.RawMessage（单对象容忍；
// 畸形 → nil，由上层按缺省处理）。
func partsOf(raw json.RawMessage) []json.RawMessage {
	if !hasRaw(raw) {
		return nil
	}
	if raw[0] != '[' {
		return []json.RawMessage{raw}
	}
	var out []json.RawMessage
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

// toolUseBlock 组装 assistant tool_use 块（空 arguments 由渲染层补 "{}"）。
func toolUseBlock(callID, name, arguments string) (ir.ContentBlock, error) {
	if name == "" {
		return ir.ContentBlock{}, fmt.Errorf("%w: tool call without name", ErrBadRequest)
	}
	return ir.ContentBlock{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{
		ID: callID, Name: name, Arguments: arguments,
	}}, nil
}

// parseInputContent 解析 message item 的 content（字符串 | 部件数组）。
// 部件：input_text/output_text → text；input_image → image；input_file/input_audio
// 等未建模块 → BlockUnknown + Raw（保真旁路；渲染侧显式处理或报错）。
func parseInputContent(raw json.RawMessage) ([]ir.ContentBlock, error) {
	if !hasRaw(raw) {
		return nil, nil
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("%w: content: %v", ErrBadRequest, err)
		}
		if s == "" {
			return nil, nil
		}
		return []ir.ContentBlock{{Kind: ir.BlockText, Text: s}}, nil
	case '[':
	default:
		return nil, fmt.Errorf("%w: content must be a string or an array of parts", ErrBadRequest)
	}
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return nil, fmt.Errorf("%w: content: %v", ErrBadRequest, err)
	}
	blocks := make([]ir.ContentBlock, 0, len(parts))
	for _, p := range parts {
		b, err := parseContentPart(p)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, b)
	}
	return blocks, nil
}

func parseContentPart(raw json.RawMessage) (ir.ContentBlock, error) {
	var pw partWire
	if err := json.Unmarshal(raw, &pw); err != nil {
		return ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, nil
	}
	switch pw.Type {
	case partTypeInputText, partTypeOutputText:
		return ir.ContentBlock{Kind: ir.BlockText, Text: pw.Text}, nil
	case partTypeInputImage:
		img := &ir.ImageSource{Detail: pw.Detail}
		switch {
		case strings.HasPrefix(pw.ImageURL, "data:"):
			media, data := splitDataURL(pw.ImageURL)
			img.Kind, img.MediaType, img.Data = "inline", media, data
		case pw.ImageURL != "":
			img.Kind, img.URL = "file", pw.ImageURL
		case pw.FileID != "":
			img.Kind, img.URL = "file", pw.FileID
		default:
			return ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, nil
		}
		return ir.ContentBlock{Kind: ir.BlockImage, Image: img}, nil
	default:
		// input_file / input_audio / input_video 等：IR 无对应块类型 → Raw 保真
		// （渲染回 responses 时按 Raw 原样回写；跨格式腿显式处理或报错）。
		return ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, nil
	}
}

// splitDataURL 拆 `data:<media>;base64,<data>`（畸形 → 原样当媒体类型）。
func splitDataURL(s string) (media, data string) {
	rest := strings.TrimPrefix(s, "data:")
	if i := strings.Index(rest, ","); i >= 0 {
		meta, payload := rest[:i], rest[i+1:]
		return strings.TrimSuffix(meta, ";base64"), payload
	}
	return rest, ""
}

// parseToolOutput function_call_output.output（字符串 | 部件数组）。
// 部件数组时：文本拼接留 tool 结果、媒体块返回供提升为后续 user 消息（表 1 行）。
func parseToolOutput(raw json.RawMessage) (string, []ir.ContentBlock, error) {
	if !hasRaw(raw) {
		return "", nil, nil
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", nil, fmt.Errorf("%w: output: %v", ErrToolResultShape, err)
		}
		return s, nil, nil
	case '[':
		blocks, err := parseInputContent(raw)
		if err != nil {
			return "", nil, err
		}
		var (
			texts []string
			media []ir.ContentBlock
		)
		for _, b := range blocks {
			switch b.Kind {
			case ir.BlockText:
				texts = append(texts, b.Text)
			default:
				media = append(media, b)
			}
		}
		return strings.Join(texts, "\n"), media, nil
	default:
		// 对象形态（个别实现把结构化结果直接放对象）：原文序列化留文本。
		return string(raw), nil, nil
	}
}

// reasoningBlocks reasoning item → IR 块。
// summary/content 文本 + encrypted_content 签名；前缀 `claude-redacted-thinking:`
// 时还原为 redacted_thinking 块（表 3「推理签名载体」行；CPA 前缀约定）。
func reasoningBlocks(summary, content []json.RawMessage, encrypted string) ([]ir.ContentBlock, error) {
	text := reasoningText(content)
	if text == "" {
		text = reasoningText(summary)
	}
	var blocks []ir.ContentBlock
	if strings.HasPrefix(encrypted, redactedThinkingPrefix) {
		if text != "" {
			blocks = append(blocks, ir.ContentBlock{Kind: ir.BlockThinking, Text: text})
		}
		blocks = append(blocks, ir.ContentBlock{
			Kind: ir.BlockRedactedThinking,
			Text: strings.TrimPrefix(encrypted, redactedThinkingPrefix),
		})
		return blocks, nil
	}
	if text == "" && encrypted == "" {
		return nil, fmt.Errorf("%w", ErrInvalidReasoning)
	}
	blocks = append(blocks, ir.ContentBlock{Kind: ir.BlockThinking, Text: text, Signature: encrypted})
	return blocks, nil
}

// reasoningText 拼接 summary/content 文本部件（多段以 "\n\n" 连接——与 chat 侧
// reasoning_content 断段规则一致）。
func reasoningText(parts []json.RawMessage) string {
	var texts []string
	for _, p := range parts {
		var pw partWire
		if err := json.Unmarshal(p, &pw); err != nil {
			continue
		}
		if pw.Text != "" {
			texts = append(texts, pw.Text)
		}
	}
	return strings.Join(texts, "\n\n")
}

// ---------------------------------------------------------------------------
// 非流式响应：线格式 → IR
// ---------------------------------------------------------------------------

// ParseResponse 解析 Response 对象（[C:6129-15795]）。
// output[] 按序转 assistant 消息的块序列；status/incomplete_details → StopReason；
// usage 恒有于 completed（缺失容忍——表 §4.4）。
func ParseResponse(data []byte) (*ir.Response, error) {
	var w responseWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadResponse, err)
	}
	if w.Object != "" && w.Object != objectResponse {
		return nil, fmt.Errorf("%w: %q", ErrUnexpectedObject, w.Object)
	}
	blocks, err := parseOutputItems(w.Output)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadResponse, err)
	}
	resp := &ir.Response{
		ID:      w.ID,
		Model:   w.Model,
		Created: w.CreatedAt,
		Message: ir.Message{Role: ir.RoleAssistant, Blocks: blocks},
	}
	if w.Status == statusFailed || hasRaw(w.Error) {
		resp.StopReason = ir.StopError
	} else if reason, err := stopReasonFromResponse(&w); err != nil {
		return nil, err
	} else {
		resp.StopReason = reason
	}
	u, err := parseUsage(w.Usage)
	if err != nil {
		return nil, fmt.Errorf("%w: usage: %v", ErrBadResponse, err)
	}
	resp.Usage = u
	// 响应面 Extra 不建模：同格式直通不走转换（arch-options §2.3 原则 1），
	// 跨格式腿的响应渲染不消费 responses 独有回显字段。
	return resp, nil
}

// parseOutputItems output[] → 块序列（message/reasoning/function_call/
// custom_tool_call；hosted 工具项 Raw 保真）。
func parseOutputItems(items []json.RawMessage) ([]ir.ContentBlock, error) {
	var blocks []ir.ContentBlock
	for _, raw := range items {
		var it outputItemWire
		if err := json.Unmarshal(raw, &it); err != nil {
			blocks = append(blocks, ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw})
			continue
		}
		switch it.Type {
		case itemTypeMessage:
			for _, c := range it.Content {
				b, err := parseOutputContentPart(c)
				if err != nil {
					return nil, err
				}
				blocks = append(blocks, b)
			}
		case itemTypeReasoning:
			bs, err := reasoningBlocks(it.Summary, it.Content, it.EncryptedContent)
			if err != nil {
				// 空 reasoning 项（加密模型只回 id）——容忍忽略，不炸整条响应。
				continue
			}
			blocks = append(blocks, bs...)
		case itemTypeFunctionCall:
			blk, err := toolUseBlock(it.CallID, it.Name, rawString(it.Arguments))
			if err != nil {
				return nil, err
			}
			blocks = append(blocks, blk)
		case itemTypeCustomTool:
			args := mustMarshal(map[string]json.RawMessage{"input": mustMarshal(rawString(it.Input))})
			blk, err := toolUseBlock(it.CallID, it.Name, string(args))
			if err != nil {
				return nil, err
			}
			blocks = append(blocks, blk)
		default:
			// hosted 工具项（web_search_call 等 30+ 变体）：无 IR 对应 →
			// BlockUnknown + Raw 保真（跨格式腿由渲染器显式裁决）。
			blocks = append(blocks, ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw})
		}
	}
	return blocks, nil
}

// parseOutputContentPart message item 的 content 部件（output_text / refusal）。
func parseOutputContentPart(raw json.RawMessage) (ir.ContentBlock, error) {
	var pw partWire
	if err := json.Unmarshal(raw, &pw); err != nil {
		return ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, nil
	}
	switch pw.Type {
	case partTypeOutputText:
		return ir.ContentBlock{Kind: ir.BlockText, Text: pw.Text}, nil
	case partTypeRefusal:
		return ir.ContentBlock{Kind: ir.BlockRefusal, Text: pw.Refusal}, nil
	default:
		return ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, nil
	}
}

// stopReasonFromResponse status/incomplete_details.reason → IR 四值（表 2 行：
// incomplete:max_output_tokens→length、content_filter→content_filter/error）。
func stopReasonFromResponse(w *responseWire) (ir.StopReason, error) {
	hasTool := false
	if w.Status == statusCompleted || w.Status == "" {
		// 有 function_call item ⟹ toolUse（表 2「完成态」行）。
		for _, raw := range w.Output {
			var it outputItemWire
			if err := json.Unmarshal(raw, &it); err != nil {
				continue
			}
			if it.Type == itemTypeFunctionCall || it.Type == itemTypeCustomTool {
				hasTool = true
				break
			}
		}
		if hasTool {
			return ir.StopToolUse, nil
		}
		return ir.StopStop, nil
	}
	if w.Status == statusIncomplete {
		reason := ""
		if w.IncompleteDetails != nil {
			reason = w.IncompleteDetails.Reason
		}
		switch reason {
		case "max_output_tokens", "max_messages":
			return ir.StopLength, nil
		case "content_filter", "steered":
			return ir.StopError, nil
		default:
			// 未知 reason → stop（翻译策略在转换层；解析层不猜测，见 ir.StopReason 注释）。
			return ir.StopStop, nil
		}
	}
	if w.Status == statusFailed {
		return ir.StopError, nil
	}
	return ir.StopReason(w.Status), nil
}

// parseUsage Response.usage → IR Usage（表 2「usage」行）。
// details 原文以 **chat 规范键名** 落 Extra（prompt_tokens_details /
// completion_tokens_details）——跨格式腿的渲染器按该键名消费；CacheRead/Reasoning
// 为派生只读值。
func parseUsage(raw json.RawMessage) (*ir.Usage, error) {
	if !hasRaw(raw) {
		return nil, nil
	}
	var w usageWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, err
	}
	u := &ir.Usage{Accounting: "openai"}
	if w.InputTokens != nil {
		u.Input = *w.InputTokens
	}
	if w.OutputTokens != nil {
		u.Output = *w.OutputTokens
	}
	if w.TotalTokens != nil {
		u.Total = *w.TotalTokens
	}
	if hasRaw(w.InputTokensDetails) {
		u.Extra = map[string]json.RawMessage{"prompt_tokens_details": w.InputTokensDetails}
		var det struct {
			CachedTokens *int64 `json:"cached_tokens"`
		}
		if err := json.Unmarshal(w.InputTokensDetails, &det); err == nil && det.CachedTokens != nil {
			u.CacheRead = *det.CachedTokens
		}
	}
	if hasRaw(w.OutputTokensDetails) {
		if u.Extra == nil {
			u.Extra = map[string]json.RawMessage{}
		}
		u.Extra["completion_tokens_details"] = w.OutputTokensDetails
		var det struct {
			ReasoningTokens *int64 `json:"reasoning_tokens"`
		}
		if err := json.Unmarshal(w.OutputTokensDetails, &det); err == nil && det.ReasoningTokens != nil {
			u.Reasoning = *det.ReasoningTokens
		}
	}
	return u, nil
}

// ---------------------------------------------------------------------------
// 错误：线格式 → IR（错误默认不透传上游 body；Raw 仅供诊断）
// ---------------------------------------------------------------------------

// ParseError 解析三种错误信封（表「错误信封（三种）」）：
//   - 非流式 HTTP 错误 `{"error":{message,type,param,code}}`（与 chat 同信封）；
//   - 流内错误事件（平铺 {type:"error",code,message,param}）；
//   - 终止失败事件 `response.failed` 的 response.error / incomplete_details。
//
// 非 JSON / 非标准 body 不报错，归一为泛型 api_error（Raw 保留原文供诊断）。
func ParseError(body []byte) *ir.Error {
	var env struct {
		Error json.RawMessage `json:"error"`
		// 流内平铺形态。
		Type    string `json:"type"`
		Code    string `json:"code"`
		Message string `json:"message"`
		Param   string `json:"param"`
		// response.failed 形态。
		Response json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		// W5-22 条目 4：非标准错误体走兜底提取（one-api 7 路径表——msg/err/
		// error_msg/header.message/response.error.message 等形态的最小信息抢救）。
		return ir.SalvageError(body, "openai")
	}
	if hasRaw(env.Response) {
		var rw responseWire
		if err := json.Unmarshal(env.Response, &rw); err == nil {
			if hasRaw(rw.Error) {
				return parseErrorObject(rw.Error, body)
			}
			msg := "upstream response failed"
			if rw.IncompleteDetails != nil && rw.IncompleteDetails.Reason != "" {
				msg = "upstream response incomplete: " + rw.IncompleteDetails.Reason
			}
			return &ir.Error{
				Type: "api_error", Message: msg, Source: "openai", Raw: json.RawMessage(body),
			}
		}
	}
	if hasRaw(env.Error) {
		return parseErrorObject(env.Error, body)
	}
	if env.Type == evError || env.Code != "" || env.Message != "" {
		return &ir.Error{
			Type: "api_error", Code: env.Code, Message: env.Message, Param: env.Param,
			Source: "openai", Raw: json.RawMessage(body),
		}
	}
	// W5-22 条目 4：兜底提取（msg/err/error_msg/header.message/response.error.
	// message 等形态；无可用信息时同款泛型 api_error + Raw）。
	return ir.SalvageError(body, "openai")
}

// parseErrorObject 解析 ResponseError/OpenAI 错误对象 {message,type,param,code}
// （code 为字符串枚举——ResponseError.code）。
func parseErrorObject(raw json.RawMessage, full []byte) *ir.Error {
	e := &ir.Error{Source: "openai", Raw: json.RawMessage(full)}
	var ew struct {
		Message string          `json:"message"`
		Type    string          `json:"type"`
		Param   json.RawMessage `json:"param"`
		Code    json.RawMessage `json:"code"`
	}
	if err := json.Unmarshal(raw, &ew); err != nil {
		e.Type, e.Message = "api_error", "malformed upstream error object"
		return e
	}
	e.Message = ew.Message
	e.Type = ew.Type
	if e.Type == "" {
		e.Type = "api_error"
	}
	e.Param = rawString(ew.Param)
	e.Code = rawStringOrNumber(ew.Code)
	return e
}

// rawStringOrNumber code 可能是字符串（server_error）或数字（HTTP code）。
func rawStringOrNumber(raw json.RawMessage) string {
	if !hasRaw(raw) {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		return n.String()
	}
	return ""
}

// ---------------------------------------------------------------------------
// 流式：SSE 事件帧 → IR 事件
// ---------------------------------------------------------------------------

// blockKey 块定位键：Responses 的三层定位（item_id + output_index +
// content_index，表 §流式）在本包里以 (output_index, content_index) 承载——
// item_id 随每帧冗余给出，output_index 是权威寻址键（item_id 可缺省）。
type blockKey struct {
	output  int
	content int
}

// blockState 一个输出块（message 的一个 content 部件 / reasoning item /
// function_call item）的流式状态。
type blockState struct {
	key        blockKey
	index      int // IR BlockIndex（首见顺序稠密）
	kind       ir.BlockKind
	itemType   string
	callID     string
	name       string
	wrapInput  bool // custom_tool_call：参数以 {"input": …} 包装下发
	open       bool
	closed     bool
	emitted    bool // 是否已下发过内容（delta/签名）——快照回退的去重依据
	text       strings.Builder
	args       strings.Builder
	sig        strings.Builder
	summaryIdx int
}

// StreamParser 流式解析状态机（Responses 事件 → IR 事件）。
type StreamParser struct {
	started   bool
	terminal  bool
	errorSeen bool
	doneSeen  bool
	nextBlock int
	order     []*blockState
	blocks    map[blockKey]*blockState
	byIndex   map[int]*blockState

	stopReason ir.StopReason
	usage      *ir.Usage
}

// NewStreamParser 创建流式解析状态机。
func NewStreamParser() *StreamParser {
	return &StreamParser{
		blocks:  map[blockKey]*blockState{},
		byIndex: map[int]*blockState{},
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
		// 非官方哨兵：new-api 扫描器容忍（表 §4.1）；本包同样容忍——
		// 终止事件收束，哨兵出现即结束（缺终止事件时判截断）。
		p.doneSeen = true
		if p.terminal || p.errorSeen {
			return nil, nil
		}
		return []ir.StreamEvent{terminalTransportError("stream ended ([DONE]) without a terminal response event")}, nil
	}
	if p.errorSeen {
		// `error` 是**任意时刻的旁路错误**（表 §4.5），不一定是流的最后一帧：
		// 出口 writer 的 Fail() 形态 = 同流 error + response.failed 双帧
		// （new-api 同款；本包 stream_writer.go 同构）——已见错误后错误族帧
		// 幂等忽略，否则 render→parse 回路在 error 流上必挂。内容帧仍是协议
		// 违规（真上游出错后不会再产出内容）。
		switch frameType(s) {
		case evError, evFailed:
			return nil, nil
		}
		return nil, fmt.Errorf("%w: %s", ErrEventsAfterTerminal, truncate(s, 120))
	}
	if p.terminal {
		// 终态后帧忽略（宽容：个别上游重发 completed；内容事件不应出现，但
		// 报错会污染已完成的流——new-api 扫描器同样在终态即停）。
		return nil, nil
	}
	var ev eventWire
	if err := json.Unmarshal([]byte(s), &ev); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadChunk, err)
	}
	if ev.Type == "" {
		return nil, fmt.Errorf("%w: event without type", ErrBadChunk)
	}

	var events []ir.StreamEvent
	switch ev.Type {
	case evCreated, evInProgress:
		var rw responseWire
		if hasRaw(ev.Response) {
			if err := json.Unmarshal(ev.Response, &rw); err != nil {
				return nil, fmt.Errorf("%w: %s: %v", ErrBadChunk, ev.Type, err)
			}
		}
		if !p.started {
			p.started = true
			events = append(events, ir.StreamEvent{
				Kind: ir.EventStart, MessageID: rw.ID, Model: rw.Model, Created: rw.CreatedAt,
			})
		}
		if u, err := parseUsage(rw.Usage); err != nil {
			return nil, fmt.Errorf("%w: %s: usage: %v", ErrBadChunk, ev.Type, err)
		} else if u != nil {
			p.usage = mergeUsage(p.usage, u)
		}

	case evOutputItemAdded:
		if !p.started {
			p.started = true
			events = append(events, ir.StreamEvent{Kind: ir.EventStart})
		}
		var it outputItemWire
		if hasRaw(ev.Item) {
			if err := json.Unmarshal(ev.Item, &it); err != nil {
				return nil, fmt.Errorf("%w: %s: %v", ErrBadChunk, ev.Type, err)
			}
		}
		switch it.Type {
		case itemTypeFunctionCall:
			b := p.open(blockKey{output: outIndex(ev), content: 0}, ir.BlockToolUse, it.Type)
			b.callID, b.name = it.CallID, it.Name
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockStart, BlockIndex: b.index, BlockKind: ir.BlockToolUse,
				ToolCallID: it.CallID, ToolName: it.Name,
			})
		case itemTypeCustomTool:
			b := p.open(blockKey{output: outIndex(ev), content: 0}, ir.BlockToolUse, it.Type)
			b.callID, b.name, b.wrapInput = it.CallID, it.Name, true
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockStart, BlockIndex: b.index, BlockKind: ir.BlockToolUse,
				ToolCallID: it.CallID, ToolName: it.Name,
			})
		case itemTypeReasoning:
			key := blockKey{output: outIndex(ev), content: 0}
			kind := ir.BlockThinking
			if strings.HasPrefix(it.EncryptedContent, redactedThinkingPrefix) {
				// 前缀载体 ⟹ redacted_thinking（块类型在公告时定型；表 3 行）。
				kind = ir.BlockRedactedThinking
			}
			b := p.open(key, kind, it.Type)
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockStart, BlockIndex: b.index, BlockKind: b.kind,
			})
			// 公告事件里的 summary/content/encrypted_content 直接落块（部分上游
			// 把全量内容放在 item 里、后续只发 done）。
			p.seedReasoning(b, it, &events)
		case itemTypeMessage:
			// message item：块在首个 delta / content_part.added 时惰性开启
			//（部件类型未定——output_text 或 refusal）。
		default:
			// hosted 工具项（web_search_call 等）：无 IR 流式表达 → 忽略
			//（new-api 的 default 分支同样静默忽略；表 2「未知事件」行）。
		}

	case evContentPartAdded, evContentPartDone:
		var pw partWire
		if hasRaw(ev.Part) {
			_ = json.Unmarshal(ev.Part, &pw)
		}
		kind := ir.BlockText
		if pw.Type == partTypeRefusal {
			kind = ir.BlockRefusal
		}
		key := blockKey{output: outIndex(ev), content: contentIndex(ev)}
		b := p.blocks[key]
		if ev.Type == evContentPartAdded {
			if b == nil {
				b = p.open(key, kind, itemTypeMessage)
				events = append(events, ir.StreamEvent{
					Kind: ir.EventBlockStart, BlockIndex: b.index, BlockKind: b.kind,
				})
			}
			if kind == ir.BlockText {
				// content_part.added 的 part.text 通常为空；非空则作为首个 delta
				//（个别上游只发 part 不发 delta）。
				if pw.Text != "" && !b.emitted {
					b.emitted = true
					b.text.WriteString(pw.Text)
					events = append(events, ir.StreamEvent{
						Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: b.kind,
						TextDelta: pw.Text,
					})
				}
			}
		} else if b != nil && !b.closed {
			events = append(events, p.close(b)...)
		}

	case evOutputTextDelta, evRefusalDelta:
		kind := ir.BlockText
		if ev.Type == evRefusalDelta {
			kind = ir.BlockRefusal
		}
		key := blockKey{output: outIndex(ev), content: contentIndex(ev)}
		b := p.blocks[key]
		if b == nil {
			b = p.open(key, kind, itemTypeMessage)
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockStart, BlockIndex: b.index, BlockKind: b.kind,
			})
		}
		if b.closed {
			return nil, fmt.Errorf("%w: delta after block close (output %d)", ErrBadChunk, key.output)
		}
		if ev.Delta != "" {
			b.emitted = true
			b.text.WriteString(ev.Delta)
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: b.kind,
				TextDelta: ev.Delta,
			})
		}

	case evOutputTextDone, evRefusalDone:
		key := blockKey{output: outIndex(ev), content: contentIndex(ev)}
		b := p.blocks[key]
		text := ev.Text
		if ev.Type == evRefusalDone {
			text = ev.Refusal
		}
		if b == nil && text != "" {
			// 只发 done 不发 delta 的上游：全量文本补发（不丢内容）。
			kind := ir.BlockText
			if ev.Type == evRefusalDone {
				kind = ir.BlockRefusal
			}
			b = p.open(key, kind, itemTypeMessage)
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockStart, BlockIndex: b.index, BlockKind: b.kind,
			})
		}
		if b != nil && !b.emitted && text != "" {
			b.emitted = true
			b.text.WriteString(text)
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: b.kind,
				TextDelta: text,
			})
		}

	case evFunctionArgsDelta, evCustomInputDelta:
		key := blockKey{output: outIndex(ev), content: 0}
		b := p.blocks[key]
		if b == nil {
			b = p.open(key, ir.BlockToolUse, itemTypeFunctionCall)
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockStart, BlockIndex: b.index, BlockKind: ir.BlockToolUse,
				ToolCallID: ev.ItemID, ToolName: "",
			})
		}
		if b.closed {
			return nil, fmt.Errorf("%w: arguments delta after block close (output %d)", ErrBadChunk, key.output)
		}
		if ev.Delta == "" {
			break
		}
		if ev.Type == evCustomInputDelta {
			// custom_tool_call 的 input 是**原始字符串增量**（非 JSON 片段）：
			// 缓冲到 done 再以 {"input": …} 单键包装整体下发（IR 契约：ArgsDelta
			// 必须是 JSON 片段，不可逐片解析——见 ir.ToolCall 注释）。
			b.args.WriteString(ev.Delta)
			break
		}
		b.emitted = true
		b.args.WriteString(ev.Delta)
		events = append(events, ir.StreamEvent{
			Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: ir.BlockToolUse,
			ArgsDelta: ev.Delta,
		})

	case evFunctionArgsDone, evCustomInputDone:
		key := blockKey{output: outIndex(ev), content: 0}
		b := p.blocks[key]
		full := ev.Arguments
		if ev.Type == evCustomInputDone {
			full = ev.Input
		}
		if b == nil {
			itemType := itemTypeFunctionCall
			if ev.Type == evCustomInputDone {
				itemType = itemTypeCustomTool
			}
			b = p.open(key, ir.BlockToolUse, itemType)
			b.wrapInput = ev.Type == evCustomInputDone
			b.callID = ev.ItemID
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockStart, BlockIndex: b.index, BlockKind: ir.BlockToolUse,
				ToolCallID: ev.ItemID,
			})
		}
		if !b.emitted && (full != "" || b.wrapInput) {
			b.emitted = true
			delta := full
			if b.wrapInput {
				delta = string(mustMarshal(map[string]json.RawMessage{"input": mustMarshal(full)}))
			}
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: ir.BlockToolUse,
				ArgsDelta: delta,
			})
		}
		if !b.closed {
			events = append(events, p.close(b)...)
		}

	case evReasoningTextDelta, evReasoningRawDelta:
		b := p.blocks[blockKey{output: outIndex(ev), content: 0}]
		if b == nil {
			b = p.open(blockKey{output: outIndex(ev), content: 0}, ir.BlockThinking, itemTypeReasoning)
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockStart, BlockIndex: b.index, BlockKind: ir.BlockThinking,
			})
		}
		// summary 分段：段间补 "\n\n"（NA needsReasoningSummaryBreak；表 2 行）。
		if ev.Type == evReasoningTextDelta && ev.SummaryIndex != nil && *ev.SummaryIndex != b.summaryIdx {
			if b.summaryIdx >= 0 && b.emitted {
				b.text.WriteString("\n\n")
				events = append(events, ir.StreamEvent{
					Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: ir.BlockThinking,
					TextDelta: "\n\n",
				})
			}
			b.summaryIdx = *ev.SummaryIndex
		}
		if ev.Delta != "" {
			b.emitted = true
			b.text.WriteString(ev.Delta)
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: ir.BlockThinking,
				TextDelta: ev.Delta,
			})
		}

	case evReasoningTextDone, evReasoningRawDone:
		b := p.blocks[blockKey{output: outIndex(ev), content: 0}]
		if b == nil && ev.Text != "" {
			b = p.open(blockKey{output: outIndex(ev), content: 0}, ir.BlockThinking, itemTypeReasoning)
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockStart, BlockIndex: b.index, BlockKind: ir.BlockThinking,
			})
		}
		if b != nil && !b.emitted && ev.Text != "" {
			b.emitted = true
			b.text.WriteString(ev.Text)
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: ir.BlockThinking,
				TextDelta: ev.Text,
			})
		}

	case evReasoningPartAdded, evReasoningPartDone, evOutputTextAnnotation:
		// summary 段边界与标注事件：无独立 IR 事件（标注面未建模——与
		// claude/gemini 转换器同口径；分段由 summary_index 断段规则承载）。

	case evOutputItemDone:
		var it outputItemWire
		if hasRaw(ev.Item) {
			if err := json.Unmarshal(ev.Item, &it); err != nil {
				return nil, fmt.Errorf("%w: %s: %v", ErrBadChunk, ev.Type, err)
			}
		}
		switch it.Type {
		case itemTypeReasoning:
			b := p.blocks[blockKey{output: outIndex(ev), content: 0}]
			if b == nil {
				bs, err := reasoningBlocks(it.Summary, it.Content, it.EncryptedContent)
				if err != nil {
					break
				}
				for _, blk := range bs {
					b = p.open(blockKey{output: outIndex(ev), content: 0}, blk.Kind, itemTypeReasoning)
					events = append(events, ir.StreamEvent{
						Kind: ir.EventBlockStart, BlockIndex: b.index, BlockKind: b.kind,
					})
					if blk.Kind == ir.BlockRedactedThinking {
						b.emitted = true
						events = append(events, ir.StreamEvent{
							Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: b.kind,
							SignatureDelta: blk.Text,
						})
					} else if blk.Text != "" {
						b.emitted = true
						events = append(events, ir.StreamEvent{
							Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: b.kind,
							TextDelta: blk.Text,
						})
					}
					events = append(events, p.close(b)...)
					b = nil
				}
				break
			}
			// 签名（encrypted_content）在 item 收尾才到：块未发签名时补发。
			if sig := it.EncryptedContent; sig != "" && b.sig.Len() == 0 {
				payload := sig
				if strings.HasPrefix(sig, redactedThinkingPrefix) {
					// redacted 载体：签名通道承载不透明载荷（ir 块 2 注释）。
					payload = strings.TrimPrefix(sig, redactedThinkingPrefix)
				}
				b.sig.WriteString(payload)
				b.emitted = true
				events = append(events, ir.StreamEvent{
					Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: b.kind,
					SignatureDelta: payload,
				})
			}
			if !b.closed {
				events = append(events, p.close(b)...)
			}
		default:
			// message / function_call 块在 content_part.done / arguments.done 收尾；
			// 缺收尾事件时在此兜底关闭。
			for _, b := range p.blocksFor(outIndex(ev)) {
				if !b.closed {
					events = append(events, p.close(b)...)
				}
			}
		}

	case evCompleted, evIncomplete, evDoneAlias, evFailed:
		var rw responseWire
		if hasRaw(ev.Response) {
			if err := json.Unmarshal(ev.Response, &rw); err != nil {
				return nil, fmt.Errorf("%w: %s: %v", ErrBadChunk, ev.Type, err)
			}
		}
		p.terminal = true
		if u, err := parseUsage(rw.Usage); err != nil {
			return nil, fmt.Errorf("%w: %s: usage: %v", ErrBadChunk, ev.Type, err)
		} else if u != nil {
			p.usage = mergeUsage(p.usage, u)
		}
		// 未闭合块收尾（正常流不会出现；防御性）。
		for _, b := range p.order {
			if b.open && !b.closed {
				events = append(events, p.close(b)...)
			}
		}
		// 快照回退：终态 output 补流中漏项（NA terminalOutputChunks）。
		events = append(events, p.snapshotFallback(&rw)...)

		if ev.Type == evFailed {
			e := &ir.Error{Type: "api_error", Message: "upstream response failed", Source: "openai", Raw: ev.Response}
			if hasRaw(rw.Error) {
				e = parseErrorObject(rw.Error, ev.Response)
			}
			p.errorSeen = true
			events = append(events, ir.StreamEvent{Kind: ir.EventError, ErrorReason: ir.ErrorUpstream, Error: e})
			return events, nil
		}
		reason, err := stopReasonFromResponse(&rw)
		if err != nil {
			return nil, err
		}
		p.stopReason = reason
		events = append(events, ir.StreamEvent{Kind: ir.EventMessageDelta, StopReason: reason})
		if p.usage != nil {
			events = append(events, ir.StreamEvent{Kind: ir.EventMessageDelta, Usage: p.usage})
		}
		events = append(events,
			ir.StreamEvent{Kind: ir.EventMessageStop},
			ir.StreamEvent{Kind: ir.EventDone, Reason: reason},
		)

	case evError:
		var eo errorObjectWire
		_ = json.Unmarshal([]byte(s), &eo)
		p.errorSeen = true
		events = append(events, ir.StreamEvent{
			Kind: ir.EventError, ErrorReason: ir.ErrorUpstream,
			Error: &ir.Error{
				Type: "api_error", Code: eo.Code, Message: eo.Message, Param: eo.Param,
				Source: "openai", Raw: json.RawMessage(s),
			},
		})

	default:
		// 未知事件：静默忽略（表 2「未知事件」行；消费端容忍新事件类型）。
	}
	return events, nil
}

// Finish 流结束（EOF）时调用：产出未发出的终态事件。
//
//   - 终止事件 / [DONE] / 错误事件已见 → 无补充；
//   - 其余 → 截断流判失败（transport；不发 block_stop——块未完成，
//     闭块会被消费端误终解析半 JSON，openai_chat 同款纪律）。
func (p *StreamParser) Finish() []ir.StreamEvent {
	if p.terminal || p.doneSeen || p.errorSeen {
		return nil
	}
	return []ir.StreamEvent{terminalTransportError("stream closed before a terminal response event")}
}

// open 开启一个块（首见顺序分配 IR BlockIndex）。
func (p *StreamParser) open(key blockKey, kind ir.BlockKind, itemType string) *blockState {
	b := &blockState{key: key, index: p.nextBlock, kind: kind, itemType: itemType, open: true, summaryIdx: -1}
	p.nextBlock++
	p.blocks[key] = b
	p.byIndex[b.index] = b
	p.order = append(p.order, b)
	return b
}

// close 关闭块（幂等）。
func (p *StreamParser) close(b *blockState) []ir.StreamEvent {
	if b.closed {
		return nil
	}
	b.open = false
	b.closed = true
	return []ir.StreamEvent{{Kind: ir.EventBlockStop, BlockIndex: b.index, BlockKind: b.kind}}
}

// blocksFor 某 output_index 下的全部块（首见顺序）。
func (p *StreamParser) blocksFor(output int) []*blockState {
	var out []*blockState
	for _, b := range p.order {
		if b.key.output == output {
			out = append(out, b)
		}
	}
	return out
}

// seedReasoning 把 output_item.added 里直接给出的 reasoning 内容落块。
// 空 reasoning 项（加密模型只回 id）容忍忽略。
func (p *StreamParser) seedReasoning(b *blockState, it outputItemWire, events *[]ir.StreamEvent) {
	blocks, err := reasoningBlocks(it.Summary, it.Content, it.EncryptedContent)
	if err != nil {
		return
	}
	for _, blk := range blocks {
		switch {
		case blk.Kind == ir.BlockRedactedThinking:
			if b.sig.Len() > 0 {
				continue
			}
			b.sig.WriteString(blk.Text)
			b.emitted = true
			*events = append(*events, ir.StreamEvent{
				Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: b.kind,
				SignatureDelta: blk.Text,
			})
		case blk.Text != "" && b.text.Len() == 0:
			b.emitted = true
			b.text.WriteString(blk.Text)
			*events = append(*events, ir.StreamEvent{
				Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: b.kind,
				TextDelta: blk.Text,
			})
		case blk.Signature != "" && b.sig.Len() == 0:
			b.emitted = true
			b.sig.WriteString(blk.Signature)
			*events = append(*events, ir.StreamEvent{
				Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: b.kind,
				SignatureDelta: blk.Signature,
			})
		}
	}
}

// snapshotFallback 快照回退：终态 response.output 是全量快照——对**没有任何
// 流式下发**的项/部件补发（NA `terminalOutputChunks` 同旨；表 §4.2 行）。
// 已下发过的块不补（避免重复内容）。
func (p *StreamParser) snapshotFallback(rw *responseWire) []ir.StreamEvent {
	var events []ir.StreamEvent
	for idx, raw := range rw.Output {
		var it outputItemWire
		if err := json.Unmarshal(raw, &it); err != nil {
			continue
		}
		switch it.Type {
		case itemTypeMessage:
			for ci, c := range it.Content {
				var pw partWire
				if err := json.Unmarshal(c, &pw); err != nil {
					continue
				}
				key := blockKey{output: idx, content: ci}
				b := p.blocks[key]
				if b != nil && b.emitted {
					continue
				}
				text, kind := pw.Text, ir.BlockText
				if pw.Type == partTypeRefusal {
					text, kind = pw.Refusal, ir.BlockRefusal
				}
				if text == "" {
					continue
				}
				if b == nil {
					b = p.open(key, kind, itemTypeMessage)
				}
				events = append(events, ir.StreamEvent{
					Kind: ir.EventBlockStart, BlockIndex: b.index, BlockKind: b.kind,
				}, ir.StreamEvent{
					Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: b.kind,
					TextDelta: text,
				})
				b.emitted = true
				events = append(events, p.close(b)...)
			}
		case itemTypeReasoning:
			b := p.blocks[blockKey{output: idx, content: 0}]
			if b != nil && b.emitted {
				continue
			}
			blocks, err := reasoningBlocks(it.Summary, it.Content, it.EncryptedContent)
			if err != nil {
				continue
			}
			for _, blk := range blocks {
				if b == nil {
					b = p.open(blockKey{output: idx, content: 0}, blk.Kind, itemTypeReasoning)
				}
				events = append(events, ir.StreamEvent{
					Kind: ir.EventBlockStart, BlockIndex: b.index, BlockKind: b.kind,
				})
				switch {
				case blk.Kind == ir.BlockRedactedThinking:
					events = append(events, ir.StreamEvent{
						Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: b.kind,
						SignatureDelta: blk.Text,
					})
				default:
					if blk.Text != "" {
						events = append(events, ir.StreamEvent{
							Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: b.kind,
							TextDelta: blk.Text,
						})
					}
					if blk.Signature != "" {
						events = append(events, ir.StreamEvent{
							Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: b.kind,
							SignatureDelta: blk.Signature,
						})
					}
				}
				b.emitted = true
				events = append(events, p.close(b)...)
				b = nil
			}
		case itemTypeFunctionCall, itemTypeCustomTool:
			b := p.blocks[blockKey{output: idx, content: 0}]
			if b != nil && b.emitted {
				continue
			}
			args, name := rawString(it.Arguments), it.Name
			wrap := false
			if it.Type == itemTypeCustomTool {
				args, wrap = rawString(it.Input), true
			}
			if args == "" && name == "" {
				continue
			}
			if b == nil {
				b = p.open(blockKey{output: idx, content: 0}, ir.BlockToolUse, it.Type)
			}
			b.wrapInput = wrap
			events = append(events, ir.StreamEvent{
				Kind: ir.EventBlockStart, BlockIndex: b.index, BlockKind: ir.BlockToolUse,
				ToolCallID: it.CallID, ToolName: name,
			})
			delta := args
			if wrap {
				delta = string(mustMarshal(map[string]json.RawMessage{"input": mustMarshal(args)}))
			}
			if delta != "" {
				events = append(events, ir.StreamEvent{
					Kind: ir.EventBlockDelta, BlockIndex: b.index, BlockKind: ir.BlockToolUse,
					ArgsDelta: delta,
				})
			}
			b.emitted = true
			events = append(events, p.close(b)...)
		default:
			// hosted 工具项：无流式表达（忽略）。
		}
	}
	return events
}

// mergeUsage 累计覆盖非累加（stream-mechanics §7.5）：非零字段覆盖，
// 零值不覆盖已有真值（快照 merge-non-zero，NA MergeUsageNonZero）。
func mergeUsage(cur, next *ir.Usage) *ir.Usage {
	if cur == nil {
		return next
	}
	if next == nil {
		return cur
	}
	if next.Input != 0 {
		cur.Input = next.Input
	}
	if next.Output != 0 {
		cur.Output = next.Output
	}
	if next.Total != 0 {
		cur.Total = next.Total
	}
	if next.CacheRead != 0 {
		cur.CacheRead = next.CacheRead
	}
	if next.Reasoning != 0 {
		cur.Reasoning = next.Reasoning
	}
	for k, v := range next.Extra {
		if cur.Extra == nil {
			cur.Extra = map[string]json.RawMessage{}
		}
		cur.Extra[k] = v
	}
	return cur
}

func outIndex(ev eventWire) int {
	if ev.OutputIndex != nil {
		return *ev.OutputIndex
	}
	return 0
}

func contentIndex(ev eventWire) int {
	if ev.ContentIndex != nil {
		return *ev.ContentIndex
	}
	return 0
}

// isDoneSentinel 判断 `[DONE]` 哨兵（非官方；容忍）。
func isDoneSentinel(s string) bool {
	return strings.TrimSpace(s) == "[DONE]"
}

// frameType 廉价取帧的 type 字段（仅错误族帧的容忍判定用；畸形 → ""）。
func frameType(s string) string {
	var w struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(s), &w); err != nil {
		return ""
	}
	return w.Type
}

func terminalTransportError(msg string) ir.StreamEvent {
	return ir.StreamEvent{
		Kind:        ir.EventError,
		ErrorReason: ir.ErrorTransport,
		Error:       &ir.Error{Type: "incomplete_stream", Message: msg, Source: "openai"},
	}
}

// sortStrings 稳定排序（错误信息里字段名有序）。
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// ParseStream 便捷入口：从 SSE 字节流解析全部 IR 事件（内含最小切帧器）。
//
// 切帧规则（stream-mechanics §4.1 的 WHATWG 子集）：注释行（`: …`，心跳）不产事件；
// `event:` 行冗余给出（payload 的 type 为准——表 §4.1）；多行 data 以 "\n" 连接；
// 空行派发；EOF 时派发未闭合帧（宽容）。
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
		// event: / id: / retry: / 其它字段行：payload 的 type 为准，忽略。
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
