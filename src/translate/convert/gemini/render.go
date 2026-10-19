package gemini

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/signature"
)

// ---------------------------------------------------------------------------
// 请求：IR → 线格式
// ---------------------------------------------------------------------------

// RenderRequest 渲染 Gemini generateContent 请求体。
//
// 出口规则（gemini-conversion-table §请求侧 左→右列 + 表注）：
//   - system/developer 消息：**开头**的全部并入顶层 systemInstruction（一个
//     Content，多条各成 text part——不合并字符串、不重排）；**中段**的降级为
//     user part + `<system-reminder>` 信封（CPA 路线，保位置）；
//   - assistant → contents[].role=model；tool 消息 → 同一条 user Content 的
//     functionResponse parts（name 取自 Message.Name 或 tool_call_id→name 映射；
//     响应体包裹：JSON 对象→原样、数组→{result:[…]}、其它→{content:…}）；
//   - **尾随 model content 显式丢弃**（Gemini 无 prefill；CPA 路线，日志点在服务壳层）；
//   - tools → tools[].functionDeclarations[]（schema 子集清洗 + 载体选择，schema.go）；
//     strict 忽略（[UNVERIFIED] mode:VALIDATED 不照抄）；
//   - tool_choice → toolConfig.functionCallingConfig.mode（AUTO/ANY/NONE；
//     {function}→ANY+allowedFunctionNames；未声明不产 toolConfig；歧义/未声明函数
//     名/未知类型 → NONE fail-closed）；parallel_tool_calls=false → NONE（fail-closed，
//     CPA 路线：Gemini 无「保留 tools 又禁并行」的表达）；
//   - generationConfig：max_completion_tokens 优先于 max_tokens、stop>5 截断、
//     n>1→candidateCount（stream+n>1 显式报错）、response_format→responseMimeType+
//     responseJsonSchema（先删 responseSchema）、modalities→responseModalities（大写）、
//     reasoning_effort→thinkingConfig.thinkingBudget（官方映射的预算档；模型能力门
//     本 slice 无表，见代码注释）；
//   - safetySettings：Extra 原文优先，缺省注入 CPA 默认五类（「请求被改写」事实）；
//   - 请求体 model 字段不回写（模型路径由渠道配置拼）；
//   - 顶层字段是白名单出口（Extra 不整体回写）：openai/claude 来源的独有顶层键
//     （logprobs/store/metadata/betas/thinking/output_config/…）不会泄漏进 gemini
//     请求体；工具定义级的来源独有键见 pruneForeignToolExtras。
func RenderRequest(req *ir.Request) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: nil request", ErrBadIR)
	}
	// stream + n>1 上游互斥（GW 陷阱 7）——显式拦截，不静默降 1。
	if req.Stream {
		if n := extraInt(req.Extra, "n"); n != nil && *n > 1 {
			return nil, fmt.Errorf("%w: n=%d", ErrStreamCandidateCount, *n)
		}
	}

	out := map[string]json.RawMessage{}

	contents, sysParts, err := renderContents(req.Messages, requiresThoughtSignature(req.Model))
	if err != nil {
		return nil, err
	}
	if len(sysParts) > 0 {
		out["systemInstruction"] = mustMarshal(map[string]json.RawMessage{
			"role":  mustMarshal(roleUser),
			"parts": mustMarshal(sysParts),
		})
	}
	if len(contents) > 0 {
		out["contents"] = mustMarshal(contents)
	}

	if len(req.Tools) > 0 {
		decls := make([]json.RawMessage, 0, len(req.Tools))
		for i := range req.Tools {
			rd, err := renderToolDef(req.Tools[i])
			if err != nil {
				return nil, fmt.Errorf("tool %d: %w", i, err)
			}
			decls = append(decls, rd)
		}
		out["tools"] = mustMarshal([]json.RawMessage{mustMarshal(map[string]json.RawMessage{
			"functionDeclarations": mustMarshal(decls),
		})})
	}

	tc, err := renderToolConfig(req)
	if err != nil {
		return nil, err
	}
	if tc != nil {
		out["toolConfig"] = tc
	}

	gc, err := renderGenerationConfig(req)
	if err != nil {
		return nil, err
	}
	if gc != nil {
		out["generationConfig"] = gc
	}

	if raw, ok := req.Extra["safetySettings"]; ok && hasRaw(raw) {
		out["safetySettings"] = raw
	} else {
		out["safetySettings"] = mustMarshal(defaultSafetySettings)
	}
	return marshalBytes(out)
}

// renderContents 渲染 messages → contents[]，并收集开头 system 消息的 parts。
//
// sigRequired（Gemini 3 族）：model 轮的 functionCall part 整轮无签名时注入官方
// 占位符（见 renderModelParts）。
//
// 返回（contents, systemParts, error）。
func renderContents(messages []ir.Message, sigRequired bool) ([]json.RawMessage, []json.RawMessage, error) {
	var contents []json.RawMessage
	var sysParts []json.RawMessage
	conversationStarted := false
	// tool_call_id → name 映射（functionResponse 按 name 配对；表 §请求侧「工具结果」行）。
	toolNames := map[string]string{}
	for _, m := range messages {
		for _, b := range m.Blocks {
			if (b.Kind == ir.BlockToolUse || b.Kind == ir.BlockServerToolUse) && b.ToolCall != nil && b.ToolCall.ID != "" {
				toolNames[b.ToolCall.ID] = b.ToolCall.Name
			}
		}
	}

	i := 0
	for i < len(messages) {
		m := messages[i]
		switch m.Role {
		case ir.RoleSystem, ir.RoleDeveloper:
			parts, err := renderTextParts(m.Blocks)
			if err != nil {
				return nil, nil, err
			}
			if !conversationStarted {
				// 开头：并入 systemInstruction（多消息 → 多 part，不合并字符串）。
				sysParts = append(sysParts, parts...)
				i++
				continue
			}
			// 中段：降级为 user part + <system-reminder> 信封（保位置）。
			wrapped := make([]json.RawMessage, 0, len(parts))
			for _, p := range parts {
				var pw struct {
					Text string `json:"text"`
				}
				_ = json.Unmarshal(p, &pw)
				if strings.TrimSpace(pw.Text) == "" {
					wrapped = append(wrapped, p)
					continue
				}
				wrapped = append(wrapped, mustMarshal(map[string]json.RawMessage{
					"text": mustMarshal("<system-reminder>\n" + pw.Text + "\n</system-reminder>"),
				}))
			}
			contents = append(contents, mustMarshal(map[string]json.RawMessage{
				"role":  mustMarshal(roleUser),
				"parts": mustMarshal(wrapped),
			}))
			i++
		case ir.RoleUser:
			conversationStarted = true
			parts, err := renderUserParts(m.Blocks, toolNames)
			if err != nil {
				return nil, nil, err
			}
			contents = append(contents, mustMarshal(map[string]json.RawMessage{
				"role":  mustMarshal(roleUser),
				"parts": mustMarshal(parts),
			}))
			i++
		case ir.RoleAssistant:
			conversationStarted = true
			parts, err := renderModelParts(m.Blocks, sigRequired)
			if err != nil {
				return nil, nil, err
			}
			contents = append(contents, mustMarshal(map[string]json.RawMessage{
				"role":  mustMarshal(roleModel),
				"parts": mustMarshal(parts),
			}))
			i++
		case ir.RoleTool:
			conversationStarted = true
			// 连续 tool 消息 → 同一条 user Content 的 functionResponse parts。
			parts := make([]json.RawMessage, 0, 2)
			for i < len(messages) && messages[i].Role == ir.RoleTool {
				rs, err := renderFunctionResponseParts(messages[i], toolNames)
				if err != nil {
					return nil, nil, err
				}
				parts = append(parts, rs...)
				i++
			}
			contents = append(contents, mustMarshal(map[string]json.RawMessage{
				"role":  mustMarshal(roleUser),
				"parts": mustMarshal(parts),
			}))
		default:
			return nil, nil, fmt.Errorf("%w: role %q", ErrBadIR, m.Role)
		}
	}
	// 尾随 model content：Gemini 无 prefill——显式丢弃（CPA :302-305 路线；
	// 属「请求被改写」事实，日志点在服务壳层，库层只做确定性丢弃）。
	for len(contents) > 0 {
		var last struct {
			Role string `json:"role"`
		}
		if err := json.Unmarshal(contents[len(contents)-1], &last); err != nil || last.Role != roleModel {
			break
		}
		contents = contents[:len(contents)-1]
	}
	return contents, sysParts, nil
}

// renderTextParts 渲染 system 消息的 text parts（systemInstruction 仅 text——
// 非 text 块丢弃，表 §quirk 18）。
func renderTextParts(blocks []ir.ContentBlock) ([]json.RawMessage, error) {
	parts := make([]json.RawMessage, 0, len(blocks))
	for _, b := range blocks {
		if b.Kind != ir.BlockText {
			continue
		}
		parts = append(parts, mustMarshal(map[string]json.RawMessage{
			"text": mustMarshal(b.Text),
		}))
	}
	return parts, nil
}

// renderUserParts 渲染 user 消息的 parts。
// 图片：内联（MediaType+Data 或 data URL）→ inlineData；file URI → fileData；
// http(s) URL 无直映 → 丢弃（表行：禁止静默丢是出口层的日志职责，库层确定性丢弃）。
// tool_result 块（hub 路径形态）：name 可解析时原位渲染 functionResponse part。
func renderUserParts(blocks []ir.ContentBlock, toolNames map[string]string) ([]json.RawMessage, error) {
	parts := make([]json.RawMessage, 0, len(blocks))
	for i, b := range blocks {
		switch b.Kind {
		case ir.BlockText:
			parts = append(parts, mustMarshal(map[string]json.RawMessage{"text": mustMarshal(b.Text)}))
		case ir.BlockImage:
			p, err := renderImagePart(b.Image)
			if err != nil {
				return nil, fmt.Errorf("block %d: %w", i, err)
			}
			if p != nil {
				parts = append(parts, p)
			}
		case ir.BlockThinking, ir.BlockRedactedThinking:
			// user 消息里的 thinking 忽略（CPA 安全规则：防注入）。
		case ir.BlockToolResult:
			// Gemini 无 user 内 tool_result 概念：name 配对可解析时原位渲染
			// functionResponse part；解析不到才确定性丢弃（日志点在服务壳层）。
			if b.ToolResult == nil {
				return nil, fmt.Errorf("%w: tool_result without payload", ErrToolResultShape)
			}
			name := toolNames[b.ToolResult.CallID]
			if name == "" {
				continue
			}
			parts = append(parts, functionResponsePart(name, b.ToolResult.CallID, b.ToolResult.Content))
		case ir.BlockUnknown:
			if len(b.Raw) > 0 {
				parts = append(parts, b.Raw)
			}
		default:
			return nil, fmt.Errorf("%w: %q in user message", ErrUnsupportedBlock, b.Kind)
		}
	}
	return parts, nil
}

// replayableThoughtSignature Gemini 请求面签名门（W5-19 条目 3）：thought part 的
// thoughtSignature 只在签名被判 **Gemini 兼容**时重放（表 §格式兼容面「签名」行 +
// CPA `isRecognizedGeminiProviderSignature`：裸 UUID/未知信封不重放）。不兼容（含空）
// 返回 ""——调用方据此不发 thoughtSignature 字段（thought 文本保留；文本 part 的
// 签名不是 Gemini 的校验硬门）。
//
// W6-1 口径核对（gemini-conversion-table:82 括注「缺签名即丢弃该块+日志」）：
// **判定该括注不成立，保持「保留文本、只丢签名」**——证据：
//  1. 官方文档（thought-signatures 页 §Signatures in function calling parts：
//     「When using Gemini 4 and Gemini 3 models, you must pass back thought
//     signatures during function calling, otherwise you will get a validation
//     error」——校验面=**函数调用**的 thoughtSignature；§Model behavior：
//     Gemini 2.5 无函数调用时任何 part 都无签名、「It is optional to return that
//     part」；thinking 页：「it's *required* for function calling signatures」，
//     其余 part 只是「we recommend you always pass all signatures back」）；
//  2. CPA 源行为：`gemini_openai_request.go:203-208` 把 assistant 的
//     reasoning_content 直落 thought part（**从不带签名、从不丢弃**）；
//     `gemini_sanitize.go` 对无签名的 model part 原样放行，仅对**首个
//     functionCall** 用 `skip_thought_signature_validator` 哨兵；带签名的文本
//     part 在不兼容时丢**签名**（reason「text parts cannot carry signatures」）、
//     不丢 part；
//  3. 同表自证：§7.3 quirk 3 与陷阱表把 `MISSING_THOUGHT_SIGNATURE` 明确限定在
//     工具调用面（「丢弃 thoughtSignature → 回传轮 MISSING_THOUGHT_SIGNATURE
//     （工具调用被拒）」）。
//
// 丢弃无签名 thought 块会让 Gemini 2.5 系（无签名载体）与跨格式腿（OpenAI 侧无
// 签名承载位）静默丢思考上下文。决策与证据全文：
// research/briefs/reports/A2A-L19-W6-1.md。
//
// functionCall part 的签名**不走本门**：工具调用签名是 Gemini 3 的重放硬门
// （GW 陷阱 2「必须原样回传」），跨格式工具签名面不在本波范围（见报告 §边界）。
func replayableThoughtSignature(sig string) string {
	if out, ok := signature.CompatibleSignatureForProvider(signature.ProviderGemini, sig); ok {
		return out
	}
	return ""
}

// skipThoughtSignatureValidator Google 官方占位符：无签名 functionCall 的合法
// 表达（「历史来自别家模型」或客户端注入的调用；Google thought-signatures 文档 +
// GoModel `skipThoughtSignatureValidator` native.go:85）。Gemini 3 族对历史里
// 无签名的 functionCall 直接 400（MISSING_THOUGHT_SIGNATURE）。
const skipThoughtSignatureValidator = "skip_thought_signature_validator"

// requiresThoughtSignature 该模型是否校验历史 functionCall 的签名（Gemini 3 族；
// GoModel `requiresThoughtSignature` native.go:140-142 的同款判定）。
func requiresThoughtSignature(model string) bool {
	major, _, ok := geminiModelGeneration(model)
	return ok && major >= 3
}

// geminiModelGeneration 解析 "gemini-<major>[.<minor>]" 世代（GoModel
// `geminiGeneration` native.go:777-802 的紧凑移植）：去 provider 前缀（最后一个
// "/" 后段）；非 "gemini-" 前缀（gemma/imagen/embeddings/调优模型）→ ok=false，
// 维持 Gemini 3 之前的行为。
func geminiModelGeneration(model string) (major, minor int, ok bool) {
	model = strings.ToLower(strings.TrimSpace(model))
	if idx := strings.LastIndex(model, "/"); idx >= 0 {
		model = model[idx+1:]
	}
	rest, found := strings.CutPrefix(model, "gemini-")
	if !found {
		return 0, 0, false
	}
	version, _, _ := strings.Cut(rest, "-")
	majorText, minorText, hasMinor := strings.Cut(version, ".")
	major, err := strconv.Atoi(majorText)
	if err != nil || major < 0 {
		return 0, 0, false
	}
	if hasMinor {
		if minor, err = strconv.Atoi(minorText); err != nil || minor < 0 {
			return 0, 0, false
		}
	}
	return major, minor, true
}

// renderModelParts 渲染 model 消息的 parts：thinking→thought part、tool_use→
// functionCall part（args 必须是对象——解析失败显式报错，NA 同款）。
//
// thought part 无签名（或签名不兼容）时**保留文本**（不丢弃块）——口径与证据见
// replayableThoughtSignature 注释的「W6-1 口径核对」（gemini-conversion-table:82
// 括注「缺签名即丢弃该块+日志」经核对判定不成立）。
//
// sigRequired（Gemini 3 族）：整轮 functionCall 无签名 → 每个 functionCall part
// 注入官方占位符 skip_thought_signature_validator（GoModel native.go:320-325 同款
// ——「至少一条带签名」的轮次按原样发，不掺占位符；文本/thought part 的签名不受
// 本门影响）。
func renderModelParts(blocks []ir.ContentBlock, sigRequired bool) ([]json.RawMessage, error) {
	// 整轮签名预扫：并行批里通常只有首条 functionCall 带签名——有一条带签名即
	// 判定为真实 Gemini 轮，原样重放（GoModel 注释同口径）。
	turnSigned := false
	if sigRequired {
		for _, b := range blocks {
			if (b.Kind == ir.BlockToolUse || b.Kind == ir.BlockServerToolUse) && b.Signature != "" {
				turnSigned = true
				break
			}
		}
	}
	parts := make([]json.RawMessage, 0, len(blocks))
	for i, b := range blocks {
		switch b.Kind {
		case ir.BlockText:
			parts = append(parts, mustMarshal(map[string]json.RawMessage{"text": mustMarshal(b.Text)}))
		case ir.BlockRefusal:
			// Gemini 无 refusal 载体：以 text part 承载（stop 语义由 finishReason 承担）。
			parts = append(parts, mustMarshal(map[string]json.RawMessage{"text": mustMarshal(b.Text)}))
		case ir.BlockThinking:
			part := map[string]json.RawMessage{
				"text":    mustMarshal(b.Text),
				"thought": mustMarshal(true),
			}
			if sig := replayableThoughtSignature(b.Signature); sig != "" {
				part["thoughtSignature"] = mustMarshal(sig)
			}
			parts = append(parts, mustMarshal(part))
		case ir.BlockToolUse, ir.BlockServerToolUse:
			if b.ToolCall == nil {
				return nil, fmt.Errorf("%w: block %d: tool_use without ToolCall", ErrBadIR, i)
			}
			args, err := toolArgsObject(b.ToolCall.Arguments)
			if err != nil {
				return nil, fmt.Errorf("block %d: %w", i, err)
			}
			fc := map[string]json.RawMessage{
				"name": mustMarshal(b.ToolCall.Name),
				"args": args,
			}
			if b.ToolCall.ID != "" {
				fc["id"] = mustMarshal(b.ToolCall.ID)
			}
			part := map[string]json.RawMessage{"functionCall": mustMarshal(fc)}
			switch {
			case b.Signature != "":
				// thoughtSignature 必须原样回传（GW 陷阱 2；工具调用签名通道）。
				part["thoughtSignature"] = mustMarshal(b.Signature)
			case sigRequired && !turnSigned:
				// Gemini 3 族 + 整轮无签名：注入官方占位符（否则上游 400
				// MISSING_THOUGHT_SIGNATURE）。
				part["thoughtSignature"] = mustMarshal(skipThoughtSignatureValidator)
			}
			parts = append(parts, mustMarshal(part))
		case ir.BlockImage:
			p, err := renderImagePart(b.Image)
			if err != nil {
				return nil, fmt.Errorf("block %d: %w", i, err)
			}
			if p != nil {
				parts = append(parts, p)
			}
		case ir.BlockExecutableCode:
			if len(b.Raw) > 0 {
				parts = append(parts, b.Raw)
				break
			}
			parts = append(parts, mustMarshal(map[string]json.RawMessage{
				"executableCode": mustMarshal(map[string]json.RawMessage{"code": mustMarshal(b.Text)}),
			}))
		case ir.BlockCodeExecutionResult:
			if len(b.Raw) > 0 {
				parts = append(parts, b.Raw)
				break
			}
			parts = append(parts, mustMarshal(map[string]json.RawMessage{
				"codeExecutionResult": mustMarshal(map[string]json.RawMessage{"output": mustMarshal(b.Text)}),
			}))
		case ir.BlockUnknown:
			if len(b.Raw) > 0 {
				parts = append(parts, b.Raw)
			}
		default:
			return nil, fmt.Errorf("%w: %q in model message", ErrUnsupportedBlock, b.Kind)
		}
	}
	return parts, nil
}

// renderImagePart 渲染图像块为 inlineData/fileData part；http(s) URL 无直映 → nil（丢弃）。
func renderImagePart(img *ir.ImageSource) (json.RawMessage, error) {
	if img == nil {
		return nil, fmt.Errorf("%w: image without payload", ErrBadIR)
	}
	switch {
	case img.Data != "":
		mime := img.MediaType
		if mime == "" {
			mime = "image/png" // CPA 响应侧默认
		}
		return mustMarshal(map[string]json.RawMessage{
			"inlineData": mustMarshal(map[string]json.RawMessage{
				"mimeType": mustMarshal(mime),
				"data":     mustMarshal(img.Data),
			}),
		}), nil
	case img.URL != "" && !strings.HasPrefix(img.URL, "http://") && !strings.HasPrefix(img.URL, "https://"):
		part := map[string]json.RawMessage{
			"fileUri": mustMarshal(img.URL),
		}
		if img.MediaType != "" {
			part["mimeType"] = mustMarshal(img.MediaType)
		}
		return mustMarshal(map[string]json.RawMessage{
			"fileData": mustMarshal(part),
		}), nil
	case img.URL != "":
		// 远程 http(s) URL：需 Files API fileData——无直映，丢弃（表行）。
		return nil, nil
	default:
		return nil, fmt.Errorf("%w", ErrImageWithoutInlineData)
	}
}

// renderFunctionResponseParts 渲染一条 tool 消息的 functionResponse parts。
func renderFunctionResponseParts(m ir.Message, toolNames map[string]string) ([]json.RawMessage, error) {
	var out []json.RawMessage
	for _, b := range m.Blocks {
		if b.Kind != ir.BlockToolResult {
			return nil, fmt.Errorf("%w: tool message carries %q", ErrToolResultShape, b.Kind)
		}
		if b.ToolResult == nil {
			return nil, fmt.Errorf("%w: tool_result without payload", ErrToolResultShape)
		}
		name := m.Name
		if name == "" && b.ToolResult.CallID != "" {
			name = toolNames[b.ToolResult.CallID]
		}
		if name == "" {
			return nil, fmt.Errorf("%w: cannot resolve function name for call %q", ErrToolResultShape, b.ToolResult.CallID)
		}
		out = append(out, functionResponsePart(name, b.ToolResult.CallID, b.ToolResult.Content))
	}
	return out, nil
}

// functionResponsePart 渲染一个 functionResponse part（id 仅在非空时携带）。
func functionResponsePart(name, callID, content string) json.RawMessage {
	fr := map[string]json.RawMessage{
		"name":     mustMarshal(name),
		"response": wrapFunctionResponse(content),
	}
	if callID != "" {
		fr["id"] = mustMarshal(callID)
	}
	return mustMarshal(map[string]json.RawMessage{"functionResponse": mustMarshal(fr)})
}

// wrapFunctionResponse 包裹工具结果响应体（表行：JSON 对象→原样、数组→
// {result:[…]}、其它→{content:…}）。
func wrapFunctionResponse(content string) json.RawMessage {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return json.RawMessage("{}")
	}
	var v any
	if err := json.Unmarshal([]byte(trimmed), &v); err != nil {
		return mustMarshal(map[string]json.RawMessage{"content": mustMarshal(content)})
	}
	switch v.(type) {
	case map[string]any:
		return json.RawMessage(trimmed)
	case []any:
		return mustMarshal(map[string]json.RawMessage{"result": json.RawMessage(trimmed)})
	default:
		return mustMarshal(map[string]json.RawMessage{"content": json.RawMessage(trimmed)})
	}
}

// toolArgsObject 把 arguments JSON 文本转为对象；空 → {}；非对象 → 显式报错
// （表行：NA 的显式报错路线，绝不静默塞原始字符串）。
func toolArgsObject(args string) (json.RawMessage, error) {
	trimmed := strings.TrimSpace(args)
	if trimmed == "" {
		return json.RawMessage("{}"), nil
	}
	var v any
	if err := json.Unmarshal([]byte(trimmed), &v); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidToolArgs, err)
	}
	if _, ok := v.(map[string]any); !ok {
		return nil, fmt.Errorf("%w: arguments are %T", ErrInvalidToolArgs, v)
	}
	return json.RawMessage(trimmed), nil
}

// renderToolDef 渲染一个 functionDeclaration（schema 清洗 + 载体选择）。
func renderToolDef(td ir.ToolDef) (json.RawMessage, error) {
	decl := map[string]json.RawMessage{"name": mustMarshal(td.Name)}
	if td.Description != "" {
		decl["description"] = mustMarshal(td.Description)
	}
	if len(td.InputSchema) > 0 && !isJSONNull(td.InputSchema) {
		rich := schemaNeedsRichCarrier(td.InputSchema)
		cleaned, err := cleanSchema(td.InputSchema, rich)
		if err != nil {
			return nil, err
		}
		key := "parameters"
		if rich {
			key = "parametersJsonSchema"
		}
		decl[key] = cleaned
	}
	// strict 忽略（表行：CPA 的 mode:VALIDATED 属未核枚举——本 slice 不发明）。
	mergeExtras(decl, pruneForeignToolExtras(td.Extra))
	return mustMarshal(decl), nil
}

// foreignToolExtras 非 gemini 来源的工具定义级字段：渲染 functionDeclaration 时
// 剔除（gemini 不认识这些键，透传 = 上游 400）。
//
//   - claude 工具级可选字段（anthropic-messages §工具定义：strict /
//     eager_input_streaming / cache_control / defer_loading；strict 已建模，其余
//     三个未建模 → 落在 ToolDef.Extra）；
//   - `response` / `responseJsonSchema`：这两个是 gemini 自身的 decl 字段
//     （gemini-wire §tools[]：官方 decl 字段面 = name/description/parameters/
//     parametersJsonSchema/response/responseJsonSchema），本渲染器只在**跨格式腿**
//     上运行（同格式不转换：服务壳直通、两步计划不会产生 gemini→X→gemini），故
//     ToolDef.Extra 里的这两个键只可能来自非 gemini 客户端的手写扩展位——它们绕过
//     本渲染器的 schema 清洗契约（cleanSchema：parameters/parametersJsonSchema 都
//     清洗，Extra 直通不清洗）→ 保守剔除（W5-11 §缺口 3「两表对称补齐」；替代分支
//     「经 cleanSchema 映射」属后续面，见 W5-16 报告）。
var foreignToolExtras = map[string]bool{
	"cache_control":         true,
	"defer_loading":         true,
	"eager_input_streaming": true,
	"response":              true,
	"responseJsonSchema":    true,
}

// pruneForeignToolExtras 剔除工具定义级的外来源独有键（见 foreignToolExtras）。
func pruneForeignToolExtras(extra map[string]json.RawMessage) map[string]json.RawMessage {
	if len(extra) == 0 {
		return extra
	}
	out := make(map[string]json.RawMessage, len(extra))
	for k, v := range extra {
		if foreignToolExtras[k] {
			continue
		}
		out[k] = v
	}
	return out
}

// foreignResponseExtras 非 gemini 来源的响应顶层独有键：渲染 GenerateContentResponse
// 时剔除（gemini 响应 schema 不认识这些键，透传 = 响应体污染）。
//
// 键集全部有据（各来源解析器的建模键集之外、经 Response.Extra 保真旁路的键）：
//   - openai_chat（openai-wire §chat.completion 顶层 + xAI 兼容面）：
//     system_fingerprint / service_tier / citations / output_files；
//   - claude_messages（anthropic-messages §响应字段清单）：container /
//     stop_details / diagnostics；
//   - interactions（interactions-wire §响应）：errors（平台错误清单，解析侧
//     注释即「渲染侧不回写」）。
//   - upstream-a / upstream-private（OpenAI 兼容上游的私有响应扩展，W101 实测
//     2026-10-19：Gemini 非流式响应顶层携带）：elapsed_ms / first_chunk_at——
//     上游私有计时（总耗时毫秒 / 首块到达时刻），Gemini 响应无对应字段。
//
// responses 不建行：responses.ParseResponse 不产 Response.Extra（同格式直通
// 不走转换；跨格式腿无旁路键可携带）——无键名清单不发明过滤表（W5-8 纪律）。
//
// 通用未知键（厂商扩展等）不在表内：保真旁路保留（与 openai/claude 出口同款
// 纪律——只剔来源侧**独有**键）。
var foreignResponseExtras = map[string]bool{
	"system_fingerprint": true,
	"service_tier":       true,
	"citations":          true,
	"output_files":       true,
	"container":          true,
	"stop_details":       true,
	"diagnostics":        true,
	"errors":             true,
	"elapsed_ms":         true, // upstream-a/upstream-private 私有总耗时（W101）
	"first_chunk_at":     true, // upstream-a/upstream-private 私有首块时刻（W101）
}

// pruneForeignResponseExtras 剔除响应顶层的外来源独有键（见 foreignResponseExtras）。
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

// renderToolConfig 渲染 toolConfig（nil = 不产；Gemini 缺省即 AUTO）。
func renderToolConfig(req *ir.Request) (json.RawMessage, error) {
	// parallel_tool_calls=false：fail-closed（CPA 路线：无「保留 tools 又禁并行」表达）。
	if raw, ok := req.Extra["parallel_tool_calls"]; ok && hasRaw(raw) {
		var b bool
		if err := json.Unmarshal(raw, &b); err == nil && !b {
			return mustMarshal(map[string]json.RawMessage{
				"functionCallingConfig": mustMarshal(map[string]json.RawMessage{
					"mode": mustMarshal("NONE"),
				}),
			}), nil
		}
	}
	raw := req.ToolChoice
	if len(raw) == 0 || isJSONNull(raw) {
		return nil, nil
	}
	fcc := map[string]json.RawMessage{}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("%w: tool_choice: %v", ErrBadIR, err)
		}
		switch s {
		case "auto":
			fcc["mode"] = mustMarshal("AUTO")
		case "none":
			fcc["mode"] = mustMarshal("NONE")
		case "required":
			fcc["mode"] = mustMarshal("ANY")
		default:
			// 未识别类型 → fail-closed（表：禁止歧义调用）。
			fcc["mode"] = mustMarshal("NONE")
		}
	case '{':
		var w struct {
			Type     string `json:"type"`
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		}
		if err := json.Unmarshal(raw, &w); err != nil {
			return nil, fmt.Errorf("%w: tool_choice: %v", ErrBadIR, err)
		}
		if w.Type != "function" || w.Function.Name == "" {
			fcc["mode"] = mustMarshal("NONE")
			break
		}
		if !declaredToolName(req.Tools, w.Function.Name) {
			// 未声明函数名：fail-closed（调用会指到不存在的工具）。
			fcc["mode"] = mustMarshal("NONE")
			break
		}
		fcc["mode"] = mustMarshal("ANY")
		fcc["allowedFunctionNames"] = mustMarshal([]string{w.Function.Name})
	default:
		fcc["mode"] = mustMarshal("NONE")
	}
	return mustMarshal(map[string]json.RawMessage{
		"functionCallingConfig": mustMarshal(fcc),
	}), nil
}

func declaredToolName(tools []ir.ToolDef, name string) bool {
	for _, t := range tools {
		if t.Name == name {
			return true
		}
	}
	return false
}

// renderGenerationConfig 渲染 generationConfig（nil = 不产）。
func renderGenerationConfig(req *ir.Request) (json.RawMessage, error) {
	gc := map[string]json.RawMessage{}
	extra := req.Extra

	// 输出上限：max_completion_tokens 优先（表行；缺省不合成——Gemini 用模型默认）。
	switch {
	case hasRaw(extra["max_completion_tokens"]):
		gc["maxOutputTokens"] = extra["max_completion_tokens"]
	case hasRaw(extra["max_tokens"]):
		gc["maxOutputTokens"] = extra["max_tokens"]
	}
	for _, kv := range []struct{ from, to string }{
		{"temperature", "temperature"},
		{"top_p", "topP"},
		{"top_k", "topK"},
		{"seed", "seed"},
		{"presence_penalty", "presencePenalty"},
		{"frequency_penalty", "frequencyPenalty"},
	} {
		if hasRaw(extra[kv.from]) {
			gc[kv.to] = extra[kv.from]
		}
	}
	// 停止序列：>5 截断到 5（表行；NA 行为）。
	if hasRaw(extra["stop"]) {
		gc["stopSequences"] = normalizeStopSequences(extra["stop"])
	}
	// 多候选：n>1 → candidateCount（stream+n>1 已在 RenderRequest 拦截）。
	if n := extraInt(extra, "n"); n != nil && *n > 1 {
		gc["candidateCount"] = mustMarshal(*n)
	}
	// 结构化输出：gemini 侧原文优先，退 response_format 映射（先删 responseSchema）。
	if hasRaw(extra["responseMimeType"]) {
		gc["responseMimeType"] = extra["responseMimeType"]
	} else if hasRaw(extra["response_format"]) {
		if err := applyResponseFormat(gc, extra["response_format"]); err != nil {
			return nil, err
		}
	}
	if hasRaw(extra["responseJsonSchema"]) {
		cleaned, err := cleanSchema(extra["responseJsonSchema"], true)
		if err != nil {
			return nil, err
		}
		gc["responseJsonSchema"] = cleaned
	}
	// 模态：gemini 侧原文优先，退 OpenAI modalities 大写化直迁。
	if hasRaw(extra["responseModalities"]) {
		gc["responseModalities"] = extra["responseModalities"]
	} else if hasRaw(extra["modalities"]) {
		if mods := responseModalitiesFor(extra["modalities"]); mods != nil {
			gc["responseModalities"] = mods
		}
	}
	// 推理强度：gemini 侧 thinkingConfig 原文优先，退 reasoning_effort 官方映射。
	if hasRaw(extra["thinkingConfig"]) {
		gc["thinkingConfig"] = extra["thinkingConfig"]
	} else if hasRaw(extra["reasoning_effort"]) {
		if tc := thinkingConfigFor(extra["reasoning_effort"]); tc != nil {
			gc["thinkingConfig"] = tc
		}
	}
	if len(gc) == 0 {
		return nil, nil
	}
	return mustMarshal(gc), nil
}

// applyResponseFormat OpenAI response_format → responseMimeType(+responseJsonSchema)。
// responseSchema（deprecated）不产——「先删 responseSchema」（表行 19）。
func applyResponseFormat(gc map[string]json.RawMessage, raw json.RawMessage) error {
	var w struct {
		Type       string `json:"type"`
		JSONSchema struct {
			Schema json.RawMessage `json:"schema"`
		} `json:"json_schema"`
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return fmt.Errorf("%w: response_format: %v", ErrBadIR, err)
	}
	switch w.Type {
	case "json_object":
		gc["responseMimeType"] = mustMarshal("application/json")
	case "json_schema":
		gc["responseMimeType"] = mustMarshal("application/json")
		if len(w.JSONSchema.Schema) > 0 && !isJSONNull(w.JSONSchema.Schema) {
			cleaned, err := cleanSchema(w.JSONSchema.Schema, true)
			if err != nil {
				return err
			}
			gc["responseJsonSchema"] = cleaned
		}
	case "text", "":
		// 无对应配置（text 是默认）。
	default:
		return fmt.Errorf("%w: response_format type %q", ErrBadIR, w.Type)
	}
	return nil
}

// responseModalitiesFor OpenAI modalities → responseModalities（大写化直迁；表行）。
func responseModalitiesFor(raw json.RawMessage) json.RawMessage {
	var mods []string
	if err := json.Unmarshal(raw, &mods); err != nil {
		return nil
	}
	out := make([]string, 0, len(mods))
	for _, m := range mods {
		switch strings.ToLower(strings.TrimSpace(m)) {
		case "text":
			out = append(out, "TEXT")
		case "image":
			out = append(out, "IMAGE")
		case "audio":
			out = append(out, "AUDIO")
		}
	}
	if len(out) == 0 {
		return nil
	}
	return mustMarshal(out)
}

// thinkingConfigFor reasoning_effort → thinkingConfig（官方映射的预算档：
// minimal/low→1024、medium→8192、high→24576；none→0 关闭）。
//
// [INFERENCE] 官方表是 (level,budget) 对（GW §可复用点 2：minimal→low/1024、…）；
// 本 slice 无模型能力表（2.5 收 budget、3 收 level，互斥），单一字段取 thinkingBudget
// ——2.5 系接受面更广，越界由上游显式报错（不静默）。未知值不映射。
func thinkingConfigFor(raw json.RawMessage) json.RawMessage {
	var effort string
	if err := json.Unmarshal(raw, &effort); err != nil {
		return nil
	}
	var budget int
	switch strings.ToLower(strings.TrimSpace(effort)) {
	case "none":
		budget = 0
	case "minimal", "low":
		budget = 1024
	case "medium":
		budget = 8192
	case "high":
		budget = 24576
	default:
		return nil
	}
	return mustMarshal(map[string]json.RawMessage{
		"thinkingBudget": mustMarshal(budget),
	})
}

// normalizeStopSequences stop（string|array）→ stopSequences（≤5 截断）。
func normalizeStopSequences(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || isJSONNull(raw) {
		return nil
	}
	var arr []string
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil
		}
		arr = []string{s}
	} else if err := json.Unmarshal(raw, &arr); err != nil {
		return nil
	}
	if len(arr) > 5 {
		arr = arr[:5]
	}
	return mustMarshal(arr)
}

// ---------------------------------------------------------------------------
// 非流式响应：IR → 线格式
// ---------------------------------------------------------------------------

// RenderResponse 渲染非流式 GenerateContentResponse（candidate 0）。
// 出现 tool_use 块 → finishReason STOP（工具语义由 functionCall part 承担）。
//
// Extra：来源侧独有键先整体剔除（foreignResponseExtras）再回写——见
// pruneForeignResponseExtras。响应面与 openai_chat/claude_messages 出口同款
// 纪律（解析侧产 Extra / 出口侧过滤后回写）：通用未知顶层键保真（P53 缺口
// 18 收口——此前 gemini 出口是纯白名单，openai 来源的未知响应键在 gemini 腿
// 静默丢失），来源独有键（system_fingerprint/container/…）绝不进 gemini 响应体
// （跨格式 hygiene 测试锁死该性质）。
func RenderResponse(resp *ir.Response) ([]byte, error) {
	if resp == nil {
		return nil, fmt.Errorf("%w: nil response", ErrBadIR)
	}
	parts, err := renderModelParts(resp.Message.Blocks, false)
	if err != nil {
		return nil, fmt.Errorf("message: %w", err)
	}
	candidate := map[string]json.RawMessage{
		"index": mustMarshal(0),
		"content": mustMarshal(map[string]json.RawMessage{
			"role":  mustMarshal(roleModel),
			"parts": mustMarshal(parts),
		}),
		"finishReason": mustMarshal(finishReasonFor(resp.StopReason)),
	}
	out := map[string]json.RawMessage{
		"candidates": mustMarshal([]json.RawMessage{mustMarshal(candidate)}),
	}
	if resp.ID != "" {
		out["responseId"] = mustMarshal(resp.ID)
	}
	if resp.Model != "" {
		out["modelVersion"] = mustMarshal(resp.Model)
	}
	if resp.Usage != nil {
		out["usageMetadata"] = renderUsageMetadata(resp.Usage)
	}
	mergeExtras(out, pruneForeignResponseExtras(resp.Extra))
	return marshalBytes(out)
}

// ---------------------------------------------------------------------------
// 错误：IR → 线格式（绝不透传 Error.Raw）
// ---------------------------------------------------------------------------

// RenderError 渲染 Google 错误信封 {"error":{code,message,status}}（§7.1/§7.2）。
// 纪律：Error.Raw 永不进入输出（错误默认不透传上游 body）。
func RenderError(e *ir.Error) ([]byte, error) {
	if e == nil {
		return nil, fmt.Errorf("%w: nil error", ErrBadIR)
	}
	status, code := googleStatusFor(e)
	inner := map[string]json.RawMessage{
		"code":    code,
		"message": mustMarshal(e.Message),
		"status":  mustMarshal(status),
	}
	return marshalBytes(map[string]json.RawMessage{"error": mustMarshal(inner)})
}

// googleStatusFor IR 错误 → Google status + 数值 code（§7.2 反向；Code 为数值时
// 原样回用——round trip 友好）。
func googleStatusFor(e *ir.Error) (string, json.RawMessage) {
	status := ""
	switch e.Type {
	case "not_found_error", "model_not_found":
		status = "NOT_FOUND"
	case "invalid_request_error", "request_too_large":
		status = "INVALID_ARGUMENT"
	case "invalid_api_key", "authentication_error":
		status = "UNAUTHENTICATED"
	case "permission_error", "permission_denied":
		status = "PERMISSION_DENIED"
	case "rate_limit_error", "rate_limit_exceeded", "insufficient_quota":
		status = "RESOURCE_EXHAUSTED"
	case "overloaded_error":
		status = "UNAVAILABLE"
	case "timeout_error":
		status = "DEADLINE_EXCEEDED"
	case "server_error", "api_error", "incomplete_stream", "":
		status = "INTERNAL"
	default:
		status = "INTERNAL"
	}
	if isNumericString(e.Code) {
		return status, json.RawMessage(e.Code)
	}
	return status, mustMarshal(googleHTTPCode(status))
}

// googleHTTPCode Google status → HTTP 状态码（§7.2 表；Code 缺省值）。
func googleHTTPCode(status string) int {
	switch status {
	case "INVALID_ARGUMENT":
		return 400
	case "UNAUTHENTICATED":
		return 401
	case "PERMISSION_DENIED":
		return 403
	case "NOT_FOUND":
		return 404
	case "RESOURCE_EXHAUSTED":
		return 429
	case "CANCELLED":
		return 499
	case "UNAVAILABLE":
		return 503
	case "DEADLINE_EXCEEDED":
		return 504
	default:
		return 500
	}
}

// ---------------------------------------------------------------------------
// 流式：IR 事件 → SSE（无哨兵）
// ---------------------------------------------------------------------------

// RenderStream 把 IR 事件序列渲染为 Gemini SSE 字节流（批量入口；
// 增量/流式场景用 NewStreamWriter——同一状态机内核）。
//
// 契约：start 必须最先；done/error 为终态；finishReason 只在终块发一次；
// 无 `[DONE]` 哨兵（Gemini 出口绝不发哨兵——「无哨兵 EOF」是正常结束路径）。
func RenderStream(events []ir.StreamEvent) ([]byte, error) {
	w := NewStreamWriter()
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

// renderUsageMetadata IR usage → usageMetadata（反向：candidates = output - thoughts）。
// OpenAI/Anthropic 侧 usage Extra 不透传（防污染；表 §流式「usage 语义」行的反向）。
func renderUsageMetadata(u *ir.Usage) json.RawMessage {
	m := map[string]json.RawMessage{
		"promptTokenCount": mustMarshal(u.Input),
		"totalTokenCount":  mustMarshal(u.Total),
	}
	candidates := u.Output
	if u.Reasoning > 0 {
		m["thoughtsTokenCount"] = mustMarshal(u.Reasoning)
		candidates = u.Output - u.Reasoning
		if candidates < 0 {
			candidates = 0
		}
	}
	m["candidatesTokenCount"] = mustMarshal(candidates)
	if u.CacheRead > 0 {
		m["cachedContentTokenCount"] = mustMarshal(u.CacheRead)
	}
	return mustMarshal(m)
}

// finishReasonFor IR 四值 → Gemini finishReason（§5.1 反向；未知值原样回写）。
func finishReasonFor(r ir.StopReason) string {
	switch r {
	case ir.StopStop, "":
		return finishStop
	case ir.StopLength:
		return finishMaxTokens
	case ir.StopToolUse:
		// Gemini 的工具轮 finishReason 是 STOP（工具语义由 functionCall part 承担）。
		return finishStop
	case ir.StopError:
		return finishSafety
	default:
		return string(r)
	}
}

// mergeExtras 把 Extra 原文并入输出对象；建模字段优先（防冲突污染）。
func mergeExtras(m map[string]json.RawMessage, extra map[string]json.RawMessage) {
	for k, v := range extra {
		if _, exists := m[k]; !exists {
			m[k] = v
		}
	}
}

// hasRaw 判定 JSON 值存在且非 null。
func hasRaw(raw json.RawMessage) bool {
	return len(raw) > 0 && !isJSONNull(raw)
}

// extraInt 取 Extra 里的整数值（nil = 缺失/非整数）。
func extraInt(extra map[string]json.RawMessage, key string) *int64 {
	raw, ok := extra[key]
	if !ok || !hasRaw(raw) {
		return nil
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil {
		return nil
	}
	return &n
}

func isNumericString(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
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
