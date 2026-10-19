package gemini

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/dwgx/any2api/translate/ir"
)

// ---------------------------------------------------------------------------
// 请求：线格式 → IR
// ---------------------------------------------------------------------------

// ParseRequest 解析 Gemini generateContent 请求体。
//
// 字段动作（gemini-conversion-table §请求侧 右→左列）：
//   - systemInstruction → RoleSystem 消息（仅 text；非 text part 跳过——表 §quirk 18
//     多模态 system 无对应，属「降级」事实）；
//   - contents[] → 消息：role model → assistant；user/function/缺省 → user；
//     functionResponse part 拆为**每条一个 RoleTool 消息**（IR 约定 role=tool 恰含
//     一个 tool_result 块；name 配对信息进 Message.Name）；
//   - parts 联合类型 → 内容块（text/thought→thinking/functionCall→tool_use/
//     functionResponse→tool_result/inlineData|fileData→image/executableCode|结果→对应块）；
//   - tools[].functionDeclarations[] → ToolDef（parametersJsonSchema 优先，退 parameters）；
//     内置工具（googleSearch/codeExecution/urlContext）丢弃（表：由渠道配置注入，OpenAI 侧无对应）；
//   - toolConfig.functionCallingConfig → 规范形 tool_choice（AUTO/none/required/{function}）；
//   - generationConfig 已建模字段 → Extra 规范键（OpenAI 形状：max_tokens/top_p/…）；
//     未识别子字段丢弃（表 §quirk 20：透传=上游 400 风险）；
//   - safetySettings 保真进 Extra（渲染侧缺省注入的判据）；cachedContent 丢弃（表行）；
//   - 请求体 model 字段读入 Model（模型路径由渠道配置；渲染侧不回写）；
//   - 非官方扩展位 `stream`（真流开关在路径 `:streamGenerateContent?alt=sse`）宽容
//     读取并保留进 Request.Stream——此前被静默丢弃、恒 false（P53 quirk ④）。
func ParseRequest(data []byte) (*ir.Request, error) {
	var w requestWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	if len(w.Contents) == 0 {
		return nil, fmt.Errorf("%w: no contents", ErrBadRequest)
	}

	req := &ir.Request{Model: w.Model, Stream: w.Stream}
	extras := map[string]json.RawMessage{}

	// systemInstruction：Content 形状，仅 text（每条 text 一个 part，不合并字符串）。
	if len(w.SystemInstruction) > 0 && !isJSONNull(w.SystemInstruction) {
		sysMsg, err := parseSystemInstruction(w.SystemInstruction)
		if err != nil {
			return nil, err
		}
		if sysMsg != nil {
			req.Messages = append(req.Messages, *sysMsg)
		}
	}
	for i, raw := range w.Contents {
		msgs, err := parseContent(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: contents %d: %w", ErrBadRequest, i, err)
		}
		req.Messages = append(req.Messages, msgs...)
	}
	for i, raw := range w.Tools {
		defs, err := parseToolDefs(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: tools %d: %w", ErrBadRequest, i, err)
		}
		req.Tools = append(req.Tools, defs...)
	}
	if len(w.ToolConfig) > 0 && !isJSONNull(w.ToolConfig) {
		tc, err := parseToolConfig(w.ToolConfig)
		if err != nil {
			return nil, err
		}
		req.ToolChoice = tc
	}
	genExtra, err := parseGenerationConfig(w.GenerationConfig)
	if err != nil {
		return nil, err
	}
	for k, v := range genExtra {
		extras[k] = v
	}
	if len(w.SafetySettings) > 0 && !isJSONNull(w.SafetySettings) {
		extras["safetySettings"] = w.SafetySettings
	}
	if len(extras) > 0 {
		req.Extra = extras
	}
	return req, nil
}

// parseSystemInstruction 解析 systemInstruction（Content，仅 text）。
func parseSystemInstruction(raw json.RawMessage) (*ir.Message, error) {
	var cw contentWire
	if err := json.Unmarshal(raw, &cw); err != nil {
		return nil, fmt.Errorf("%w: systemInstruction: %v", ErrBadRequest, err)
	}
	msg := ir.Message{Role: ir.RoleSystem}
	for _, rawPart := range cw.Parts {
		var pw partWire
		if err := json.Unmarshal(rawPart, &pw); err != nil {
			continue
		}
		if pw.Text == "" {
			continue // 仅 text（表 §quirk 18：多模态 system 无对应，跳过）
		}
		msg.Blocks = append(msg.Blocks, ir.ContentBlock{Kind: ir.BlockText, Text: pw.Text})
	}
	if len(msg.Blocks) == 0 {
		return nil, nil
	}
	return &msg, nil
}

// parseContent 解析一个 Content 为一条或多条 IR 消息。
// functionResponse part 拆为独立 RoleTool 消息（IR 约定 role=tool 恰含一个
// tool_result 块；下游 openai 渲染同款约束）。
func parseContent(raw json.RawMessage) ([]ir.Message, error) {
	var cw contentWire
	if err := json.Unmarshal(raw, &cw); err != nil {
		return nil, fmt.Errorf("content: %v", err)
	}
	role := ir.RoleUser
	switch cw.Role {
	case roleModel:
		role = ir.RoleAssistant
	case roleUser, "function", "":
		role = ir.RoleUser
	default:
		// 未知 role：原样承载（解析层不猜测；表 §quirk 9 role 二义）。
		role = ir.Role(cw.Role)
	}

	var blocks []ir.ContentBlock
	var toolMsgs []ir.Message
	for _, rawPart := range cw.Parts {
		// 纯签名 part（无载荷）：挂到前一个可承载块（签名必须原样回传）。
		var probe partWire
		if err := json.Unmarshal(rawPart, &probe); err == nil &&
			probe.ThoughtSignature != "" && probe.Text == "" && !probe.Thought &&
			probe.FunctionCall == nil && probe.FunctionResponse == nil &&
			probe.InlineData == nil && probe.FileData == nil {
			attachSignature(blocks, probe.ThoughtSignature)
			continue
		}
		block, name, isToolResult, err := parsePart(rawPart)
		if err != nil {
			return nil, err
		}
		if isToolResult {
			toolMsgs = append(toolMsgs, ir.Message{
				Role:   ir.RoleTool,
				Name:   name,
				Blocks: []ir.ContentBlock{*block},
			})
			continue
		}
		if block == nil {
			continue
		}
		blocks = append(blocks, *block)
	}

	out := make([]ir.Message, 0, 1+len(toolMsgs))
	if len(blocks) > 0 {
		out = append(out, ir.Message{Role: role, Blocks: blocks})
	}
	out = append(out, toolMsgs...)
	return out, nil
}

// parsePart 解析一个 part。返回 (块, functionResponse 的 name, 是否 tool_result)。
// 纯 thoughtSignature part 挂到前一个 tool_use/thinking 块上（签名必须原样回传，
// GW 陷阱 2）；空 text / "\n" 分隔 part 跳过（NA 同款过滤）。
func parsePart(raw json.RawMessage) (*ir.ContentBlock, string, bool, error) {
	var pw partWire
	if err := json.Unmarshal(raw, &pw); err != nil {
		return &ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, "", false, nil
	}
	var probe map[string]json.RawMessage
	_ = json.Unmarshal(raw, &probe)
	_, hasText := probe["text"]
	switch {
	case pw.FunctionCall != nil:
		args := "{}"
		if len(pw.FunctionCall.Args) > 0 && !isJSONNull(pw.FunctionCall.Args) {
			args = string(pw.FunctionCall.Args)
		}
		return &ir.ContentBlock{
			Kind:      ir.BlockToolUse,
			Signature: pw.ThoughtSignature,
			ToolCall: &ir.ToolCall{
				ID:        pw.FunctionCall.ID,
				Name:      pw.FunctionCall.Name,
				Arguments: args,
			},
		}, "", false, nil
	case pw.FunctionResponse != nil:
		content := ""
		if len(pw.FunctionResponse.Response) > 0 && !isJSONNull(pw.FunctionResponse.Response) {
			content = string(pw.FunctionResponse.Response)
		}
		return &ir.ContentBlock{
			Kind: ir.BlockToolResult,
			ToolResult: &ir.ToolResult{
				CallID:  pw.FunctionResponse.ID,
				Content: content,
			},
		}, pw.FunctionResponse.Name, true, nil
	case pw.InlineData != nil:
		return &ir.ContentBlock{Kind: ir.BlockImage, Image: &ir.ImageSource{
			Kind: "inline", MediaType: pw.InlineData.MimeType, Data: pw.InlineData.Data,
		}}, "", false, nil
	case pw.FileData != nil:
		return &ir.ContentBlock{Kind: ir.BlockImage, Image: &ir.ImageSource{
			Kind: "file", MediaType: pw.FileData.MimeType, URL: pw.FileData.FileURI,
		}}, "", false, nil
	case pw.ExecutableCode != nil:
		return &ir.ContentBlock{Kind: ir.BlockExecutableCode, Text: pw.ExecutableCode.Code, Raw: raw}, "", false, nil
	case pw.CodeExecutionResult != nil:
		return &ir.ContentBlock{Kind: ir.BlockCodeExecutionResult, Text: pw.CodeExecutionResult.Output, Raw: raw}, "", false, nil
	case hasText && pw.Thought:
		if pw.Text == "" || pw.Text == "\n" {
			return nil, "", false, nil
		}
		return &ir.ContentBlock{Kind: ir.BlockThinking, Text: pw.Text, Signature: pw.ThoughtSignature}, "", false, nil
	case hasText:
		if pw.Text == "" || pw.Text == "\n" {
			return nil, "", false, nil // 空/分隔 text part 跳过（NA 同款过滤）
		}
		return &ir.ContentBlock{Kind: ir.BlockText, Text: pw.Text}, "", false, nil
	case pw.ThoughtSignature != "":
		// 纯签名 part：无载荷；由调用方挂到前一个块（此处返回 nil 表示跳过）。
		return nil, "", false, nil
	default:
		return &ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, "", false, nil
	}
}

// attachSignature 把纯签名 part 的签名挂到最后一个可承载块（tool_use/thinking）。
func attachSignature(blocks []ir.ContentBlock, sig string) {
	if sig == "" {
		return
	}
	for i := len(blocks) - 1; i >= 0; i-- {
		b := &blocks[i]
		if (b.Kind == ir.BlockToolUse || b.Kind == ir.BlockServerToolUse || b.Kind == ir.BlockThinking) && b.Signature == "" {
			b.Signature = sig
			return
		}
	}
}

// parseToolDefs 解析一个 tools[] 项：functionDeclarations → ToolDef；内置工具丢弃。
func parseToolDefs(raw json.RawMessage) ([]ir.ToolDef, error) {
	var tw toolWire
	if err := json.Unmarshal(raw, &tw); err != nil {
		return nil, fmt.Errorf("tool: %v", err)
	}
	var out []ir.ToolDef
	for i, rawDecl := range tw.FunctionDeclarations {
		var dw functionDeclarationWire
		if err := json.Unmarshal(rawDecl, &dw); err != nil {
			return nil, fmt.Errorf("functionDeclarations %d: %v", i, err)
		}
		if dw.Name == "" {
			return nil, fmt.Errorf("%w: functionDeclaration without name", ErrBadRequest)
		}
		td := ir.ToolDef{Name: dw.Name, Description: dw.Description}
		schema := dw.ParametersJSONSchema
		if len(schema) == 0 || isJSONNull(schema) {
			schema = dw.Parameters
		}
		if len(schema) > 0 && !isJSONNull(schema) {
			td.InputSchema = schema
		}
		td.Extra = extrasOf(rawDecl, "name", "description", "parameters", "parametersJsonSchema")
		out = append(out, td)
	}
	return out, nil
}

// parseToolConfig 解析 toolConfig.functionCallingConfig → 规范形 tool_choice。
// AUTO→"auto"、NONE→"none"、ANY→"required"（带 allowedFunctionNames[0] 时 →
// {type:function,function:{name}}）；VALIDATED 属未核枚举（GW 未列）→ 归 auto；
// 未识别 mode → "none"（fail-closed，表：禁止歧义调用）。
func parseToolConfig(raw json.RawMessage) (json.RawMessage, error) {
	var tw toolConfigWire
	if err := json.Unmarshal(raw, &tw); err != nil {
		return nil, fmt.Errorf("%w: toolConfig: %v", ErrBadRequest, err)
	}
	if len(tw.FunctionCallingConfig) == 0 || isJSONNull(tw.FunctionCallingConfig) {
		return nil, nil
	}
	var fw functionCallingConfigWire
	if err := json.Unmarshal(tw.FunctionCallingConfig, &fw); err != nil {
		return nil, fmt.Errorf("%w: functionCallingConfig: %v", ErrBadRequest, err)
	}
	switch strings.ToUpper(strings.TrimSpace(fw.Mode)) {
	case "":
		return nil, nil
	case "AUTO":
		return json.RawMessage(`"auto"`), nil
	case "NONE":
		return json.RawMessage(`"none"`), nil
	case "ANY":
		if len(fw.AllowedFunctionNames) > 0 {
			return mustMarshal(map[string]json.RawMessage{
				"type":     mustMarshal("function"),
				"function": mustMarshal(map[string]json.RawMessage{"name": mustMarshal(fw.AllowedFunctionNames[0])}),
			}), nil
		}
		return json.RawMessage(`"required"`), nil
	case "VALIDATED":
		// [UNVERIFIED] GW 的 mode 枚举只有 AUTO/ANY/NONE；CPA 用它承载 strict。
		// 本 slice 不发明该枚举（渲染侧忽略 strict），解析侧归 auto。
		return json.RawMessage(`"auto"`), nil
	default:
		return json.RawMessage(`"none"`), nil
	}
}

// parseGenerationConfig 把 generationConfig 已建模字段映射为 Extra 规范键
// （OpenAI 形状；表 §请求侧行内列）。未识别子字段丢弃（§quirk 20）。
func parseGenerationConfig(raw json.RawMessage) (map[string]json.RawMessage, error) {
	if len(raw) == 0 || isJSONNull(raw) {
		return nil, nil
	}
	var w generationConfigWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, fmt.Errorf("%w: generationConfig: %v", ErrBadRequest, err)
	}
	out := map[string]json.RawMessage{}
	set := func(key string, v json.RawMessage) {
		if len(v) > 0 && !isJSONNull(v) {
			out[key] = v
		}
	}
	set("max_tokens", w.MaxOutputTokens)
	set("temperature", w.Temperature)
	set("top_p", w.TopP)
	set("top_k", w.TopK)
	set("stop", w.StopSequences)
	set("n", w.CandidateCount)
	set("seed", w.Seed)
	set("presence_penalty", w.PresencePenalty)
	set("frequency_penalty", w.FrequencyPenalty)
	set("responseMimeType", w.ResponseMimeType)
	// responseSchema deprecated → 读作 responseJsonSchema（渲染侧只发新字段；表行 19）。
	schema := w.ResponseJSONSchema
	if len(schema) == 0 || isJSONNull(schema) {
		schema = w.ResponseSchema
	}
	set("responseJsonSchema", schema)
	set("responseModalities", w.ResponseModalities)
	set("thinkingConfig", w.ThinkingConfig)
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 非流式响应：线格式 → IR
// ---------------------------------------------------------------------------

// responseModeledTopKeys responseWire 建模的响应顶层键（extrasOf 排除表）。
// 建模键之外的一切顶层字段（官方 modelStatus、厂商扩展、未来新增字段）进
// Response.Extra 保真旁路——与 openai_chat/claude_messages 的响应面同款纪律
// （P53 缺口 18：此前 gemini 解析不产 Response.Extra，未知顶层字段静默丢弃）。
//
// 边界（有意，与两兄弟格式对称）：
//   - candidates 是建模容器（candidate 0 语义）——candidate 级未建模字段
//     （safetyRatings/citationMetadata/groundingMetadata/avgLogprobs/
//     logprobsResult/finishMessage/continuationToken）随容器丢弃，与 openai
//     choice 级未建模字段（logprobs 等）同款；IR 无候选级载体（Message.Extra
//     是消息对象级，不是候选级）。
//   - promptFeedback 部分建模（blockReason 是拦截信号，见下）——建模键不入
//     Extra（其 safetyRatings 子字段同 candidates 容器纪律）。
var responseModeledTopKeys = []string{
	"responseId", "modelVersion", "createTime", "candidates", "promptFeedback", "usageMetadata",
}

// ParseResponse 解析非流式 GenerateContentResponse（candidate 0 语义；多候选
// 显式报错 ErrUnsupportedMultiCandidate——绝不静默丢弃）。
//
// prompt 拦截路径（表 §响应侧「prompt 拦截」行 + GW 陷阱 3）：candidates=[] 且
// promptFeedback.blockReason 非空 → 返回 StopReason=StopError 的空回复
// （content_filter 语义）——显式化，禁止空回复静默成功（NA 仅打元数据不改变
// 响应是反面教材）。
//
// Extra：未建模顶层字段进 Response.Extra（保真旁路；未知字段不炸 + 保真两条
// 不变量同时满足）——见 responseModeledTopKeys。拦截路径同样携带（不因提前
// 返回而丢旁路）。
func ParseResponse(data []byte) (*ir.Response, error) {
	var w responseWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadResponse, err)
	}
	extra := extrasOf(data, responseModeledTopKeys...)
	if len(w.Candidates) == 0 {
		var pf promptFeedbackWire
		if len(w.PromptFeedback) > 0 && !isJSONNull(w.PromptFeedback) {
			_ = json.Unmarshal(w.PromptFeedback, &pf)
		}
		if pf.BlockReason != "" {
			u, err := parseUsageMetadata(w.UsageMetadata)
			if err != nil {
				return nil, fmt.Errorf("%w: usageMetadata: %w", ErrBadResponse, err)
			}
			return &ir.Response{
				ID:         w.ResponseID,
				Model:      w.ModelVersion,
				Created:    createdFromCreateTime(w.CreateTime),
				Message:    ir.Message{Role: ir.RoleAssistant},
				StopReason: ir.StopError,
				Usage:      u,
				Extra:      extra,
			}, nil
		}
		return nil, fmt.Errorf("%w: no candidates", ErrBadResponse)
	}
	if len(w.Candidates) > 1 {
		return nil, fmt.Errorf("%w: %d candidates", ErrUnsupportedMultiCandidate, len(w.Candidates))
	}
	var cw candidateWire
	if err := json.Unmarshal(w.Candidates[0], &cw); err != nil {
		return nil, fmt.Errorf("%w: candidate: %v", ErrBadResponse, err)
	}
	if cw.Index != nil && *cw.Index != 0 {
		return nil, fmt.Errorf("%w: candidate index %d", ErrUnsupportedMultiCandidate, *cw.Index)
	}
	msg, err := parseCandidateMessage(cw.Content)
	if err != nil {
		return nil, fmt.Errorf("%w: candidate content: %w", ErrBadResponse, err)
	}
	resp := &ir.Response{
		ID:      w.ResponseID,
		Model:   w.ModelVersion,
		Created: createdFromCreateTime(w.CreateTime),
		Message: msg,
		Extra:   extra,
	}
	if cw.FinishReason != "" && cw.FinishReason != finishUnspecified {
		resp.StopReason = mapFinishReason(cw.FinishReason)
	}
	// 出现 functionCall ⟹ 强制 toolUse（覆盖上游 finishReason；两实现一致）。
	if hasToolUse(msg.Blocks) {
		resp.StopReason = ir.StopToolUse
	}
	u, err := parseUsageMetadata(w.UsageMetadata)
	if err != nil {
		return nil, fmt.Errorf("%w: usageMetadata: %w", ErrBadResponse, err)
	}
	resp.Usage = u
	return resp, nil
}

// parseCandidateMessage 解析 candidate.content（parts → 块）。
func parseCandidateMessage(raw json.RawMessage) (ir.Message, error) {
	msg := ir.Message{Role: ir.RoleAssistant}
	if len(raw) == 0 || isJSONNull(raw) {
		return msg, nil
	}
	var cw contentWire
	if err := json.Unmarshal(raw, &cw); err != nil {
		return msg, fmt.Errorf("content: %v", err)
	}
	for _, rawPart := range cw.Parts {
		var pw partWire
		if err := json.Unmarshal(rawPart, &pw); err != nil {
			msg.Blocks = append(msg.Blocks, ir.ContentBlock{Kind: ir.BlockUnknown, Raw: rawPart})
			continue
		}
		if pw.ThoughtSignature != "" && pw.Text == "" && !pw.Thought && pw.FunctionCall == nil {
			attachSignature(msg.Blocks, pw.ThoughtSignature)
			continue
		}
		block, _, _, err := parsePart(rawPart)
		if err != nil {
			return msg, err
		}
		if block == nil {
			continue
		}
		if block.Kind == ir.BlockToolResult {
			// 响应里出现 functionResponse 属畸形（模型不能返回工具结果）。
			return msg, fmt.Errorf("%w: functionResponse part in model candidate", ErrToolResultShape)
		}
		msg.Blocks = append(msg.Blocks, *block)
	}
	return msg, nil
}

func hasToolUse(blocks []ir.ContentBlock) bool {
	for _, b := range blocks {
		if b.Kind == ir.BlockToolUse || b.Kind == ir.BlockServerToolUse {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// 错误：线格式 → IR（错误默认不透传上游 body；Raw 仅供诊断）
// ---------------------------------------------------------------------------

// ParseError 解析 Google 错误信封 {"error":{code,message,status}}（§7.1/§7.2）。
// 非 JSON / 非标准 body 不报错，归一为泛型 api_error（Raw 保留原文供诊断）。
func ParseError(body []byte) *ir.Error {
	var env struct {
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil || len(env.Error) == 0 || isJSONNull(env.Error) {
		// W5-22 条目 4：非标准错误体走兜底提取（one-api 7 路径表——msg/err/
		// error_msg/header.message/response.error.message 等形态的最小信息抢救）。
		return ir.SalvageError(body, "gemini")
	}
	return parseErrorObject(env.Error)
}

// parseErrorObject 解析 Google 错误对象内层，按 §7.2 表映射到 OpenAI 语义类型。
func parseErrorObject(raw json.RawMessage) *ir.Error {
	e := &ir.Error{Source: "gemini", Raw: raw}
	var ew errorObjectWire
	if err := json.Unmarshal(raw, &ew); err != nil {
		e.Type = "api_error"
		e.Message = "malformed upstream error object"
		return e
	}
	e.Message = ew.Message
	e.Code = rawStringOrNumber(ew.Code)
	e.Type, e.Retryable = openaiTypeForGoogleStatus(ew.Status)
	if e.Type == "" {
		e.Type = "server_error"
	}
	return e
}

// openaiTypeForGoogleStatus Google status ↔ OpenAI 错误类型（§7.2 双向表；
// 未识别值按 server_error，原文在 Raw 供排障）。
func openaiTypeForGoogleStatus(status string) (string, *bool) {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "INVALID_ARGUMENT", "FAILED_PRECONDITION", "INVALID_REQUEST":
		return "invalid_request_error", nil
	case "UNAUTHENTICATED":
		return "invalid_api_key", nil
	case "PERMISSION_DENIED":
		return "permission_error", nil
	case "NOT_FOUND":
		return "not_found_error", nil
	case "RESOURCE_EXHAUSTED", "RATE_LIMIT_EXCEEDED":
		return "rate_limit_error", new(true)
	case "UNAVAILABLE":
		return "server_error", new(true)
	case "DEADLINE_EXCEEDED":
		return "server_error", new(true)
	case "CANCELLED":
		return "api_error", nil
	case "":
		return "", nil
	default:
		return "server_error", nil
	}
}

// ---------------------------------------------------------------------------
// 流式：GenerateContentResponse 帧 → IR 事件
// ---------------------------------------------------------------------------

// StreamParser Gemini 流式解析状态机。用法：对每个 `data:` 帧载荷（或非 data:
// 前缀的错误 JSON 行）调用 Feed；流结束（EOF）后调用 Finish 取未产出的终态事件。
//
// 契约（gemini-conversion-table §流式 delta 映射表 + §7.3 quirks）：
//   - 文本是**增量**（禁止按累计语义去重）；usageMetadata 是**逐帧累计快照**——
//     合并规则 = 末帧权威 + merge-non-zero，禁止逐帧累加/取首帧（SM §4.4）；
//   - functionCall 无 index：按 call id（缺 id 退 name）稳定化槽位；整对象一次给全，
//     跨帧重复的同一 id 跳过（OpenAI 侧按 index 追加合并，重发会双倍参数）；
//   - partialArgs/willContinue 缓冲物化（jsonPath 子集；断流未收口 → 错误，
//     不得当空参工具执行）；
//   - finishReason 不提前落地：终块统一收尾（NA terminalChunk 同旨），
//     出现 functionCall 强制 toolUse；
//   - 无哨兵：`[DONE]` 若出现按非 Gemini 语义忽略；无 finishReason 的 EOF 判失败。
type StreamParser struct {
	started   bool
	errorSeen bool

	stopSeen        bool
	stopReason      ir.StopReason
	blockReasonSeen bool
	toolSeen        bool

	usage     geminiUsage
	usageSeen bool

	nextBlock int
	open      []openBlock
	textIdx   int
	thinkIdx  int

	// 工具槽位稳定化：call id（缺 id 退 name）→ 槽。
	toolSlots map[string]*toolSlot
	doneIDs   map[string]bool
}

type openBlock struct {
	index int
	kind  ir.BlockKind
}

type toolSlot struct {
	blockIndex int
	id         string
	name       string
	partial    map[string]any
	open       bool
}

// NewStreamParser 创建流式解析状态机。
func NewStreamParser() *StreamParser {
	return &StreamParser{
		textIdx:   -1,
		thinkIdx:  -1,
		toolSlots: map[string]*toolSlot{},
		doneIDs:   map[string]bool{},
	}
}

type streamFrameWire struct {
	ResponseID     string            `json:"responseId"`
	ModelVersion   string            `json:"modelVersion"`
	CreateTime     string            `json:"createTime"`
	Candidates     []json.RawMessage `json:"candidates"`
	PromptFeedback json.RawMessage   `json:"promptFeedback"`
	UsageMetadata  json.RawMessage   `json:"usageMetadata"`
	Error          json.RawMessage   `json:"error"`
}

// Feed 处理一个帧载荷（data: 行 JSON 或非 data: 前缀的错误 JSON 行）。
// 返回本次新增的 IR 事件；协议级违规返回包装后的哨兵错误。
func (p *StreamParser) Feed(payload []byte) ([]ir.StreamEvent, error) {
	s := strings.TrimSpace(string(payload))
	if s == "" {
		return nil, nil
	}
	if isDoneSentinel(s) {
		return nil, nil // 非 Gemini 语义：忽略（容忍 vLLM 式代理混入）
	}
	if p.errorSeen {
		return nil, fmt.Errorf("%w: %s", ErrBadChunk, truncate(s, 120))
	}
	var fw streamFrameWire
	if err := json.Unmarshal([]byte(s), &fw); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadChunk, err)
	}
	// 流中错误：data: 行内 error 字段或非 data: 前缀的错误行（§7.1）。
	if len(fw.Error) > 0 && !isJSONNull(fw.Error) {
		p.errorSeen = true
		return []ir.StreamEvent{{
			Kind:        ir.EventError,
			ErrorReason: ir.ErrorUpstream,
			Error:       parseErrorObject(fw.Error),
		}}, nil
	}

	var events []ir.StreamEvent
	if !p.started {
		p.started = true
		events = append(events, ir.StreamEvent{
			Kind: ir.EventStart, MessageID: fw.ResponseID, Model: fw.ModelVersion,
			Created: createdFromCreateTime(fw.CreateTime),
		})
	}

	if len(fw.Candidates) > 1 {
		return nil, fmt.Errorf("%w: %d candidates", ErrUnsupportedMultiCandidate, len(fw.Candidates))
	}
	if len(fw.Candidates) == 1 {
		var cw candidateWire
		if err := json.Unmarshal(fw.Candidates[0], &cw); err != nil {
			return nil, fmt.Errorf("%w: candidate: %v", ErrBadChunk, err)
		}
		if cw.Index != nil && *cw.Index != 0 {
			return nil, fmt.Errorf("%w: candidate index %d", ErrUnsupportedMultiCandidate, *cw.Index)
		}
		var content contentWire
		if len(cw.Content) > 0 && !isJSONNull(cw.Content) {
			if err := json.Unmarshal(cw.Content, &content); err != nil {
				return nil, fmt.Errorf("%w: content: %v", ErrBadChunk, err)
			}
		}
		for _, rawPart := range content.Parts {
			evs, err := p.feedPart(rawPart)
			if err != nil {
				return events, err
			}
			events = append(events, evs...)
		}
		if cw.FinishReason != "" && cw.FinishReason != finishUnspecified {
			// 覆盖语义（同帧多值/跨帧重发取末值）；不在此落地帧——
			// 终块统一收尾（表：STOP 帧的 finishReason 抑制，NA 同款）。
			p.stopSeen = true
			p.stopReason = mapFinishReason(cw.FinishReason)
		}
	}

	if len(fw.PromptFeedback) > 0 && !isJSONNull(fw.PromptFeedback) {
		var pf promptFeedbackWire
		if err := json.Unmarshal(fw.PromptFeedback, &pf); err == nil && pf.BlockReason != "" {
			p.blockReasonSeen = true
		}
	}
	if len(fw.UsageMetadata) > 0 && !isJSONNull(fw.UsageMetadata) {
		if err := p.usage.mergeRaw(fw.UsageMetadata); err != nil {
			return events, fmt.Errorf("%w: usageMetadata: %v", ErrBadChunk, err)
		}
		p.usageSeen = true
	}
	return events, nil
}

// feedPart 处理帧内一个 part（流式表只覆盖 text/thought/functionCall；其余忽略）。
func (p *StreamParser) feedPart(rawPart json.RawMessage) ([]ir.StreamEvent, error) {
	var pw partWire
	if err := json.Unmarshal(rawPart, &pw); err != nil {
		return nil, nil
	}
	switch {
	case pw.FunctionCall != nil:
		return p.feedFunctionCall(pw.FunctionCall, pw.ThoughtSignature)
	case pw.Thought && pw.Text != "":
		return p.feedTextDelta(pw.Text, ir.BlockThinking), nil
	case pw.Text != "" && pw.Text != "\n":
		return p.feedTextDelta(pw.Text, ir.BlockText), nil
	default:
		// 流式面未建模的 part（inlineData 等）忽略——表 §流式 delta 映射表无对应行。
		return nil, nil
	}
}

// feedTextDelta 文本/思考增量（各自惰性开一个块；kind 切换时关闭前一块——
// 下游 claude 出口有块序约束，交错块必须按序闭合）。
func (p *StreamParser) feedTextDelta(text string, kind ir.BlockKind) []ir.StreamEvent {
	var events []ir.StreamEvent
	if kind == ir.BlockThinking {
		events = append(events, p.closeTextBlock(ir.BlockText)...)
	} else {
		events = append(events, p.closeTextBlock(ir.BlockThinking)...)
	}
	idx := p.textIdx
	if kind == ir.BlockThinking {
		idx = p.thinkIdx
	}
	if idx < 0 {
		idx = p.nextBlock
		p.nextBlock++
		p.open = append(p.open, openBlock{idx, kind})
		events = append(events, ir.StreamEvent{Kind: ir.EventBlockStart, BlockIndex: idx, BlockKind: kind})
		if kind == ir.BlockThinking {
			p.thinkIdx = idx
		} else {
			p.textIdx = idx
		}
	}
	events = append(events, ir.StreamEvent{
		Kind: ir.EventBlockDelta, BlockIndex: idx, BlockKind: kind, TextDelta: text,
	})
	return events
}

// closeTextBlock 关闭当前打开的 text/thinking 块（kind 切换或工具调用打断时）。
func (p *StreamParser) closeTextBlock(kind ir.BlockKind) []ir.StreamEvent {
	idx := p.textIdx
	if kind == ir.BlockThinking {
		idx = p.thinkIdx
	}
	if idx < 0 {
		return nil
	}
	if kind == ir.BlockThinking {
		p.thinkIdx = -1
	} else {
		p.textIdx = -1
	}
	for i, b := range p.open {
		if b.index == idx {
			p.open = append(p.open[:i], p.open[i+1:]...)
			break
		}
	}
	return []ir.StreamEvent{{Kind: ir.EventBlockStop, BlockIndex: idx, BlockKind: kind}}
}

// feedFunctionCall 处理 functionCall part：整对象一次给全；partialArgs/willContinue
// 缓冲物化；同 id 跨帧重复跳过。
func (p *StreamParser) feedFunctionCall(fc *functionCallWire, signature string) ([]ir.StreamEvent, error) {
	p.toolSeen = true
	isPartial := len(fc.PartialArgs) > 0 || (fc.WillContinue != nil && *fc.WillContinue)
	if isPartial {
		return p.feedPartialFunctionCall(fc)
	}
	if fc.ID != "" && p.doneIDs[fc.ID] {
		// 同一 call id 跨帧复现：槽位已收口，重发会双倍参数（OpenAI 按 index 追加合并）。
		return nil, nil
	}
	slot := p.newSlot(fc.ID, fc.Name)
	var events []ir.StreamEvent
	// 工具调用打断文本流：先闭合打开的 text/thinking 块（块序约束）。
	events = append(events, p.closeTextBlock(ir.BlockText)...)
	events = append(events, p.closeTextBlock(ir.BlockThinking)...)
	events = append(events, ir.StreamEvent{
		Kind: ir.EventBlockStart, BlockIndex: slot.blockIndex, BlockKind: ir.BlockToolUse,
		ToolCallID: slot.id, ToolName: slot.name,
	})
	if signature != "" {
		events = append(events, ir.StreamEvent{
			Kind: ir.EventBlockDelta, BlockIndex: slot.blockIndex, BlockKind: ir.BlockToolUse,
			SignatureDelta: signature,
		})
	}
	args := "{}"
	if len(fc.Args) > 0 && !isJSONNull(fc.Args) {
		args = string(fc.Args)
	}
	events = append(events, ir.StreamEvent{
		Kind: ir.EventBlockDelta, BlockIndex: slot.blockIndex, BlockKind: ir.BlockToolUse,
		ArgsDelta: args,
	})
	events = append(events, ir.StreamEvent{
		Kind: ir.EventBlockStop, BlockIndex: slot.blockIndex, BlockKind: ir.BlockToolUse,
	})
	if fc.ID != "" {
		p.doneIDs[fc.ID] = true
	}
	return events, nil
}

// feedPartialFunctionCall 缓冲物化 partialArgs（jsonPath 子集；收口时一次发出
// block_start + 完整 args delta + block_stop）。
func (p *StreamParser) feedPartialFunctionCall(fc *functionCallWire) ([]ir.StreamEvent, error) {
	key := partialKey(fc.ID, fc.Name)
	slot := p.toolSlots[key]
	if slot == nil {
		slot = p.newSlot(fc.ID, fc.Name)
	}
	if fc.ID != "" {
		if slot.id != "" && slot.id != fc.ID {
			return nil, fmt.Errorf("%w: partial call changed id from %q to %q", ErrUnsupportedFunctionCallShape, slot.id, fc.ID)
		}
		slot.id = fc.ID
	}
	if fc.Name != "" {
		if slot.name != "" && slot.name != fc.Name {
			return nil, fmt.Errorf("%w: partial call changed name from %q to %q", ErrUnsupportedFunctionCallShape, slot.name, fc.Name)
		}
		slot.name = fc.Name
	}
	if slot.partial == nil {
		slot.partial = map[string]any{}
	}
	for _, pa := range fc.PartialArgs {
		path, err := parsePartialArgPath(pa.JSONPath)
		if err != nil {
			return nil, err
		}
		value, present := partialArgValue(pa)
		if !present {
			continue
		}
		updated, err := setPartialArgValue(slot.partial, path, value, pa.StringValue != nil)
		if err != nil {
			return nil, fmt.Errorf("%w: %q: %v", ErrBadPartialArgsPath, pa.JSONPath, err)
		}
		obj, ok := updated.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: path %q replaced the arguments object", ErrBadPartialArgsPath, pa.JSONPath)
		}
		slot.partial = obj
	}
	if fc.WillContinue != nil && *fc.WillContinue {
		slot.open = true
		return nil, nil
	}
	// 收口：name 必填（NA Finalize 同款）。
	if slot.name == "" {
		return nil, fmt.Errorf("%w: partial call completed without a name", ErrUnsupportedFunctionCallShape)
	}
	args, err := marshalJSON(slot.partial)
	if err != nil {
		return nil, err
	}
	var events []ir.StreamEvent
	// 工具调用打断文本流：先闭合打开的 text/thinking 块（块序约束）。
	events = append(events, p.closeTextBlock(ir.BlockText)...)
	events = append(events, p.closeTextBlock(ir.BlockThinking)...)
	events = append(events, ir.StreamEvent{
		Kind: ir.EventBlockStart, BlockIndex: slot.blockIndex, BlockKind: ir.BlockToolUse,
		ToolCallID: slot.id, ToolName: slot.name,
	})
	events = append(events, ir.StreamEvent{
		Kind: ir.EventBlockDelta, BlockIndex: slot.blockIndex, BlockKind: ir.BlockToolUse,
		ArgsDelta: string(args),
	})
	events = append(events, ir.StreamEvent{
		Kind: ir.EventBlockStop, BlockIndex: slot.blockIndex, BlockKind: ir.BlockToolUse,
	})
	slot.open = false
	if slot.id != "" {
		p.doneIDs[slot.id] = true
	}
	return events, nil
}

func (p *StreamParser) newSlot(id, name string) *toolSlot {
	slot := &toolSlot{blockIndex: p.nextBlock, id: id, name: name}
	p.nextBlock++
	p.toolSlots[partialKey(id, name)] = slot
	return slot
}

func partialKey(id, name string) string {
	if id != "" {
		return "id:" + id
	}
	return "name:" + name
}

// Finish 流结束（EOF）时调用：产出未发出的终态事件。
//
//   - 错误帧已见 → 无补充；
//   - 未收口的 partialArgs → 错误（NA Finalize 原文语义：不得当空参工具执行）；
//   - 无 finishReason 且无 promptFeedback.blockReason → 截断流判失败
//     （stream-mechanics §4.7-4：截断流是失败态）；
//   - 正常收尾：闭块 → message_delta{stop_reason} → message_delta{usage} →
//     message_stop → done（无哨兵——Gemini 出口绝不发 `[DONE]`）。
func (p *StreamParser) Finish() []ir.StreamEvent {
	if p.errorSeen {
		return nil
	}
	if !p.started {
		return []ir.StreamEvent{terminalTransportError("stream closed before any frame")}
	}
	for _, slot := range p.toolSlots {
		if slot.open {
			return []ir.StreamEvent{terminalTransportError("stream ended with an incomplete function call")}
		}
	}
	if !p.stopSeen && !p.blockReasonSeen {
		return []ir.StreamEvent{terminalTransportError("stream closed before finishReason")}
	}
	events := p.closeBlocks()
	reason := p.stopReason
	if p.blockReasonSeen {
		reason = ir.StopError // prompt 被拦：content_filter 语义（显式化）
	}
	if p.toolSeen {
		reason = ir.StopToolUse // 出现 functionCall ⟹ 强制 toolUse（覆盖上游 finishReason）
	}
	events = append(events, ir.StreamEvent{Kind: ir.EventMessageDelta, StopReason: reason})
	if p.usageSeen {
		u := p.usage.usage()
		events = append(events, ir.StreamEvent{Kind: ir.EventMessageDelta, Usage: u})
	}
	events = append(events, ir.StreamEvent{Kind: ir.EventMessageStop})
	events = append(events, ir.StreamEvent{Kind: ir.EventDone, Reason: reason})
	return events
}

func (p *StreamParser) closeBlocks() []ir.StreamEvent {
	events := make([]ir.StreamEvent, 0, len(p.open))
	for _, b := range p.open {
		events = append(events, ir.StreamEvent{Kind: ir.EventBlockStop, BlockIndex: b.index, BlockKind: b.kind})
	}
	p.open = nil
	return events
}

// ParseStream 便捷入口：从 SSE 字节流解析全部 IR 事件（含最小切帧器）。
//
// 切帧规则（stream-mechanics §4.1 的 WHATWG 子集 + Gemini 变体）：
//   - 注释行（心跳）跳过；多行 data 以 "\n" 连接；空行派发；
//   - **非 `data:` 前缀的 JSON 行**（流内错误的实际形态，§7.1）按括号平衡聚合成帧；
//   - `[DONE]` 容忍忽略（非 Gemini 语义）；EOF 时派发未闭合帧。
func ParseStream(r io.Reader) ([]ir.StreamEvent, error) {
	p := NewStreamParser()
	var events []ir.StreamEvent
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 64*1024*1024)
	var dataLines []string
	var jsonLine strings.Builder
	depth := 0
	flushData := func() error {
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
			if jsonLine.Len() > 0 {
				// 括号未平衡的残帧：空行结束聚合，交由 Feed 判错。
				if err := flushJSONLine(&events, p, &jsonLine, &depth); err != nil {
					return events, err
				}
				continue
			}
			if err := flushData(); err != nil {
				return events, err
			}
			continue
		}
		if v, ok := strings.CutPrefix(line, "data:"); ok {
			dataLines = append(dataLines, strings.TrimPrefix(v, " "))
			continue
		}
		// event:/id:/retry: 行忽略；其余视为括号平衡的 JSON 行（错误帧形态）。
		if strings.HasPrefix(line, "event:") || strings.HasPrefix(line, "id:") || strings.HasPrefix(line, "retry:") {
			continue
		}
		if jsonLine.Len() > 0 {
			jsonLine.WriteByte('\n')
		}
		jsonLine.WriteString(line)
		depth += braceDelta(line)
		if depth <= 0 {
			if err := flushJSONLine(&events, p, &jsonLine, &depth); err != nil {
				return events, err
			}
		}
	}
	if err := sc.Err(); err != nil {
		return events, err
	}
	if jsonLine.Len() > 0 {
		if err := flushJSONLine(&events, p, &jsonLine, &depth); err != nil {
			return events, err
		}
	}
	if err := flushData(); err != nil {
		return events, err
	}
	events = append(events, p.Finish()...)
	return events, nil
}

func flushJSONLine(events *[]ir.StreamEvent, p *StreamParser, b *strings.Builder, depth *int) error {
	payload := b.String()
	b.Reset()
	*depth = 0
	evs, err := p.Feed([]byte(payload))
	*events = append(*events, evs...)
	return err
}

// braceDelta 括号平衡增量（字符串内的括号不计）。
func braceDelta(s string) int {
	depth := 0
	inStr := false
	esc := false
	for _, r := range s {
		switch {
		case esc:
			esc = false
		case r == '\\' && inStr:
			esc = true
		case r == '"':
			inStr = !inStr
		case inStr:
		case r == '{' || r == '[':
			depth++
		case r == '}' || r == ']':
			depth--
		}
	}
	return depth
}

// ---------------------------------------------------------------------------
// partialArgs 物化（jsonPath 子集：$ 根 + .member / [n] / ['k']；NA :561-635 语义）
// ---------------------------------------------------------------------------

type partialArgSegment struct {
	member  string
	index   int
	isIndex bool
}

// parsePartialArgPath 解析 jsonPath：必须以 `$` 起（根路径 `$` 单独非法）；
// 段形式 `.member` / `[n]` / `['k']`；通配/过滤/未支持选择器报错。
func parsePartialArgPath(jsonPath string) ([]partialArgSegment, error) {
	path := strings.TrimSpace(jsonPath)
	if path == "" || path[0] != '$' {
		return nil, fmt.Errorf("%w: %q (must start with $)", ErrBadPartialArgsPath, jsonPath)
	}
	var segments []partialArgSegment
	for off := 1; off < len(path); {
		switch path[off] {
		case '.':
			off++
			start := off
			for off < len(path) && path[off] != '.' && path[off] != '[' {
				off++
			}
			if start == off {
				return nil, fmt.Errorf("%w: empty member in %q", ErrBadPartialArgsPath, jsonPath)
			}
			member := path[start:off]
			if strings.ContainsAny(member, "]*?") {
				return nil, fmt.Errorf("%w: member %q in %q", ErrBadPartialArgsPath, member, jsonPath)
			}
			segments = append(segments, partialArgSegment{member: member})
		case '[':
			off++
			if off >= len(path) {
				return nil, fmt.Errorf("%w: unterminated selector in %q", ErrBadPartialArgsPath, jsonPath)
			}
			if path[off] == '\'' || path[off] == '"' {
				quote := path[off]
				start := off
				off++
				for off < len(path) && path[off] != quote {
					off++
				}
				if off >= len(path) {
					return nil, fmt.Errorf("%w: unterminated quoted member in %q", ErrBadPartialArgsPath, jsonPath)
				}
				raw := path[start : off+1]
				if quote == '\'' {
					raw = `"` + strings.ReplaceAll(raw[1:len(raw)-1], `"`, `\"`) + `"`
				}
				var member string
				if err := json.Unmarshal([]byte(raw), &member); err != nil {
					return nil, fmt.Errorf("%w: quoted member in %q", ErrBadPartialArgsPath, jsonPath)
				}
				off++
				if off >= len(path) || path[off] != ']' {
					return nil, fmt.Errorf("%w: unterminated member selector in %q", ErrBadPartialArgsPath, jsonPath)
				}
				off++
				segments = append(segments, partialArgSegment{member: member})
				continue
			}
			start := off
			for off < len(path) && path[off] >= '0' && path[off] <= '9' {
				off++
			}
			if start == off || off >= len(path) || path[off] != ']' {
				return nil, fmt.Errorf("%w: unsupported selector in %q", ErrBadPartialArgsPath, jsonPath)
			}
			index, err := strconv.Atoi(path[start:off])
			if err != nil {
				return nil, fmt.Errorf("%w: invalid array index in %q", ErrBadPartialArgsPath, jsonPath)
			}
			if index > maxPartialArgArrayIndex {
				return nil, fmt.Errorf("%w: index %d > %d", ErrPartialArgsLimit, index, maxPartialArgArrayIndex)
			}
			off++
			segments = append(segments, partialArgSegment{index: index, isIndex: true})
		default:
			return nil, fmt.Errorf("%w: unsupported selector at offset %d in %q", ErrBadPartialArgsPath, off, jsonPath)
		}
	}
	if len(segments) == 0 {
		return nil, fmt.Errorf("%w: path %q targets the arguments root", ErrBadPartialArgsPath, jsonPath)
	}
	return segments, nil
}

// partialArgValue 取分片值（四选一；缺省 → 无值跳过）。
func partialArgValue(pa partialArgWire) (any, bool) {
	switch {
	case pa.StringValue != nil:
		return *pa.StringValue, true
	case len(pa.NumberValue) > 0:
		var f float64
		if err := json.Unmarshal(pa.NumberValue, &f); err != nil {
			return nil, false
		}
		return f, true
	case pa.BoolValue != nil:
		return *pa.BoolValue, true
	case len(pa.NullValue) > 0:
		return nil, true
	default:
		return nil, false
	}
}

// setPartialArgValue 按路径写入值；appendString 为真且叶位现值是字符串时追加拼接
// （stringValue 支持追加；NA setGeminiPartialArgValue 语义）。
func setPartialArgValue(current any, path []partialArgSegment, value any, appendString bool) (any, error) {
	if len(path) == 0 {
		if appendString {
			if existing, ok := current.(string); ok {
				if s, ok := value.(string); ok {
					return existing + s, nil
				}
			}
		}
		return value, nil
	}
	seg := path[0]
	if seg.isIndex {
		var array []any
		switch typed := current.(type) {
		case nil:
			array = make([]any, seg.index+1)
		case []any:
			array = typed
			if len(array) <= seg.index {
				array = append(array, make([]any, seg.index-len(array)+1)...)
			}
		default:
			return nil, fmt.Errorf("array index %d traverses %T", seg.index, current)
		}
		updated, err := setPartialArgValue(array[seg.index], path[1:], value, appendString)
		if err != nil {
			return nil, err
		}
		array[seg.index] = updated
		return array, nil
	}
	var object map[string]any
	switch typed := current.(type) {
	case nil:
		object = map[string]any{}
	case map[string]any:
		object = typed
	default:
		return nil, fmt.Errorf("member %q traverses %T", seg.member, current)
	}
	updated, err := setPartialArgValue(object[seg.member], path[1:], value, appendString)
	if err != nil {
		return nil, err
	}
	object[seg.member] = updated
	return object, nil
}

// ---------------------------------------------------------------------------
// usage 合并（逐帧累计快照：末帧权威 + merge-non-zero）
// ---------------------------------------------------------------------------

type geminiUsage struct {
	prompt, candidates, total, cached, thoughts *int64
}

// mergeRaw 并入一帧 usageMetadata：非零字段覆盖、缺省保留（NA
// MergeGeminiUsageMetadataNonZero 语义；禁止逐帧累加、禁止取首帧）。
func (g *geminiUsage) mergeRaw(raw json.RawMessage) error {
	var w usageMetadataWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return err
	}
	merge := func(dst **int64, src *int64) {
		if src != nil && *src != 0 {
			v := *src
			*dst = &v
		}
	}
	merge(&g.prompt, w.PromptTokenCount)
	merge(&g.candidates, w.CandidatesTokenCount)
	merge(&g.total, w.TotalTokenCount)
	merge(&g.cached, w.CachedContentTokenCount)
	merge(&g.thoughts, w.ThoughtsTokenCount)
	return nil
}

// usage 汇总为 IR usage：completion = candidates + thoughts（表行）；
// total 缺失时按 prompt+completion 兜底（Estimated 标合成来源）。
func (g *geminiUsage) usage() *ir.Usage {
	u := &ir.Usage{Accounting: "gemini"}
	if g.prompt != nil {
		u.Input = *g.prompt
	}
	if g.candidates != nil {
		u.Output = *g.candidates
	}
	if g.thoughts != nil {
		u.Output += *g.thoughts
		u.Reasoning = *g.thoughts
	}
	if g.cached != nil {
		u.CacheRead = *g.cached
	}
	if g.total != nil && *g.total != 0 {
		u.Total = *g.total
	} else {
		u.Total = u.Input + u.Output
		u.Estimated = true
	}
	return u
}

// parseUsageMetadata 解析单个 usageMetadata 对象（非流式）。
func parseUsageMetadata(raw json.RawMessage) (*ir.Usage, error) {
	if len(raw) == 0 || isJSONNull(raw) {
		return nil, nil
	}
	var g geminiUsage
	if err := g.mergeRaw(raw); err != nil {
		return nil, err
	}
	return g.usage(), nil
}

// ---------------------------------------------------------------------------
// finishReason 映射（§5.1 全枚举 22 值；「不照抄」项按表）
// ---------------------------------------------------------------------------

// mapFinishReason Gemini finishReason → IR 四值。
//
//	STOP → stop；MAX_TOKENS/CONTINUATION → length；
//	安全/内容类（SAFETY/RECITATION/BLOCKLIST/PROHIBITED_CONTENT/SPII/IMAGE_*/
//	LANGUAGE/OTHER/畸形调用类）→ error（content_filter 语义）；
//	NO_IMAGE → stop（未生成图像不是拦截——NA default 落 content_filter 不照抄）；
//	ESCALATION/PUP_LIMITED_DISABLED → stop（语义未核 [UNVERIFIED]，取不误判为拦截）；
//	FINISH_REASON_UNSPECIFIED/空 → 不落值（调用方跳过）。
func mapFinishReason(s string) ir.StopReason {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case finishStop:
		return ir.StopStop
	case finishMaxTokens, "CONTINUATION":
		return ir.StopLength
	case "SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII",
		"IMAGE_SAFETY", "IMAGE_PROHIBITED_CONTENT", "IMAGE_RECITATION", "IMAGE_OTHER",
		"LANGUAGE", "OTHER", "MALFORMED_FUNCTION_CALL", "UNEXPECTED_TOOL_CALL",
		"TOO_MANY_TOOL_CALLS", "MISSING_THOUGHT_SIGNATURE", "MALFORMED_RESPONSE":
		return ir.StopError
	case "NO_IMAGE", "ESCALATION", "PUP_LIMITED_DISABLED":
		return ir.StopStop
	default:
		// 未知值原样透传（翻译策略在转换层，解析层不猜测）。
		return ir.StopReason(s)
	}
}

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

func terminalTransportError(msg string) ir.StreamEvent {
	return ir.StreamEvent{
		Kind:        ir.EventError,
		ErrorReason: ir.ErrorTransport,
		Error:       &ir.Error{Type: "incomplete_stream", Message: msg, Source: "gemini"},
	}
}

// createdFromCreateTime Vertex createTime（RFC3339）→ unix 秒；缺失/非法 → 0
// （本层不发明时间戳，与 claude_messages 同纪律）。
func createdFromCreateTime(s string) int64 {
	if s == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0
	}
	return t.Unix()
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

func rawStringOrNumber(raw json.RawMessage) string {
	if len(raw) == 0 || isJSONNull(raw) {
		return ""
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return ""
		}
		return s
	}
	return strings.TrimSpace(string(raw))
}

func isJSONNull(raw json.RawMessage) bool {
	return strings.TrimSpace(string(raw)) == "null"
}

// isDoneSentinel 容忍尾随内容（对齐 openai-python 的 startswith 判法）。
func isDoneSentinel(s string) bool {
	return strings.HasPrefix(s, "[DONE]")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
