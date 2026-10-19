package responses

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

// responsesPassableExtras 渲染 responses 请求体时允许从 IR Extra 透传的顶层键
// （表 1「透传族」行：store/metadata/user/safety_identifier/prompt_cache_key/
// prompt_cache_retention/service_tier；采样与并行工具同名）。
// 不在表内的 Extra（stop/thinking/output_config/betas/…）一律丢弃——映射表
// 「丢弃+日志」降级策略；日志点在服务壳层，库层只做确定性丢弃。
var responsesPassableExtras = []string{
	"store", "metadata", "user", "safety_identifier",
	"prompt_cache_key", "prompt_cache_retention", "service_tier",
	"temperature", "top_p", "top_logprobs", "parallel_tool_calls",
	"frequency_penalty", "presence_penalty",
}

// RenderRequest 渲染 POST /v1/responses 请求体。
func RenderRequest(req *ir.Request) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: nil request", ErrBadIR)
	}
	extra := req.Extra
	out := map[string]json.RawMessage{"model": mustMarshal(req.Model)}

	items, instructions, err := renderInput(req.Messages)
	if err != nil {
		return nil, err
	}
	out["input"] = mustMarshal(items)
	if instructions != "" {
		out["instructions"] = mustMarshal(instructions)
	}

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
	tc, err := renderToolChoice(req.ToolChoice)
	if err != nil {
		return nil, err
	}
	if hasRaw(tc) {
		out["tool_choice"] = tc
	}
	if req.Stream {
		out["stream"] = mustMarshal(true)
	}
	if req.StreamOptions != nil && req.StreamOptions.IncludeObfuscation != nil {
		out["stream_options"] = mustMarshal(map[string]json.RawMessage{
			"include_obfuscation": mustMarshal(*req.StreamOptions.IncludeObfuscation),
		})
	}

	// 输出上限：max_completion_tokens/max_tokens → max_output_tokens（表 1 行）。
	switch {
	case extraHas(extra, "max_completion_tokens"):
		out["max_output_tokens"] = extra["max_completion_tokens"]
	case extraHas(extra, "max_tokens"):
		out["max_output_tokens"] = extra["max_tokens"]
	}
	// 结构化输出：response_format → text.format（反向嵌套→扁平，表 1 行）。
	if tf := responseFormatToTextFormat(extra["response_format"]); hasRaw(tf) {
		out["text"] = mustMarshal(map[string]json.RawMessage{"format": tf})
	}
	// 推理：reasoning_effort → reasoning.effort（表 1 行）。
	if eff := extra["reasoning_effort"]; hasRaw(eff) {
		out["reasoning"] = mustMarshal(map[string]json.RawMessage{"effort": eff})
	}
	for _, k := range responsesPassableExtras {
		if v, ok := extra[k]; ok && hasRaw(v) {
			out[k] = v
		}
	}
	return marshalBytes(out)
}

func extraHas(extra map[string]json.RawMessage, k string) bool {
	if extra == nil {
		return false
	}
	return hasRaw(extra[k])
}

// renderInput IR 消息 → input items + instructions（表 1：system/developer 合并为
// instructions，多条以 "\n\n" 连接；assistant 的 tool_use 拆为独立 function_call 项
// 紧随其 message 项之后——顺序即语义）。
func renderInput(messages []ir.Message) ([]json.RawMessage, string, error) {
	var (
		items        []json.RawMessage
		instructions []string
	)
	for i := range messages {
		m := messages[i]
		switch m.Role {
		case ir.RoleSystem, ir.RoleDeveloper:
			var texts []string
			for _, b := range m.Blocks {
				if b.Kind == ir.BlockText && b.Text != "" {
					texts = append(texts, b.Text)
				}
			}
			if len(texts) > 0 {
				instructions = append(instructions, strings.Join(texts, "\n"))
			}
			continue
		case ir.RoleTool:
			it, err := renderToolResultItem(m)
			if err != nil {
				return nil, "", fmt.Errorf("message %d: %w", i, err)
			}
			items = append(items, it)
			continue
		}

		role := string(m.Role)
		if role == "" {
			role = "user"
		}
		// 文本部件类型随角色（表 3 行：助手回放用 output_text、用户用 input_text）。
		textPartType := partTypeInputText
		if role == roleAssistant {
			textPartType = partTypeOutputText
		}
		// 文本/拒绝部件聚合成一个 message item；其它块各自成项（保序）。
		var pending []json.RawMessage
		flush := func() error {
			if len(pending) == 0 {
				return nil
			}
			items = append(items, mustMarshal(map[string]json.RawMessage{
				"role":    mustMarshal(role),
				"content": mustMarshal(pending),
			}))
			pending = nil
			return nil
		}
		for i, b := range m.Blocks {
			switch b.Kind {
			case ir.BlockText:
				pending = append(pending, mustMarshal(map[string]json.RawMessage{
					"type": mustMarshal(textPartType),
					"text": mustMarshal(b.Text),
				}))
			case ir.BlockRefusal:
				pending = append(pending, mustMarshal(map[string]json.RawMessage{
					"type":    mustMarshal(partTypeRefusal),
					"refusal": mustMarshal(b.Text),
				}))
			case ir.BlockImage:
				part, err := renderImagePart(b.Image)
				if err != nil {
					return nil, "", err
				}
				pending = append(pending, part)
			case ir.BlockUnknown:
				if len(b.Raw) == 0 {
					return nil, "", fmt.Errorf("%w: unknown block without Raw", ErrBadIR)
				}
				if isContentPartType(rawTypeOf(b.Raw)) {
					pending = append(pending, b.Raw)
					continue
				}
				// item 级 Raw（hosted 工具产物等）：作为顶层 input 项回写（保真旁路）。
				if err := flush(); err != nil {
					return nil, "", err
				}
				items = append(items, b.Raw)
			case ir.BlockThinking:
				if err := flush(); err != nil {
					return nil, "", err
				}
				items = append(items, renderReasoningItem(b.Text, b.Signature, false, "", ""))
			case ir.BlockRedactedThinking:
				if err := flush(); err != nil {
					return nil, "", err
				}
				items = append(items, renderReasoningItem(b.Text, b.Signature, true, "", ""))
			case ir.BlockToolUse, ir.BlockServerToolUse:
				if b.ToolCall == nil {
					return nil, "", fmt.Errorf("%w: tool_use without ToolCall", ErrBadIR)
				}
				if err := flush(); err != nil {
					return nil, "", err
				}
				items = append(items, mustMarshal(map[string]json.RawMessage{
					"type": mustMarshal(itemTypeFunctionCall),
					// 请求面（客户端历史回传）：无响应 id 可派生 → 块序回落形态
					//（seed 空）；配对键稳定靠 id 幂等净化。
					"call_id":   mustMarshal(hygiene.WireToolID("call", b.ToolCall.ID, "", i)),
					"name":      mustMarshal(b.ToolCall.Name),
					"arguments": mustMarshal(toolArgumentsText(b.ToolCall.Arguments)),
				}))
			case ir.BlockToolResult:
				return nil, "", fmt.Errorf("%w: tool_result block outside tool message", ErrToolResultShape)
			default:
				return nil, "", fmt.Errorf("%w: %q", ErrUnsupportedBlock, b.Kind)
			}
		}
		if err := flush(); err != nil {
			return nil, "", err
		}
	}
	return items, strings.Join(instructions, "\n\n"), nil
}

// renderToolResultItem tool 消息 → function_call_output 项（表 1「工具结果」行）。
func renderToolResultItem(m ir.Message) (json.RawMessage, error) {
	var tr *ir.ToolResult
	for _, b := range m.Blocks {
		switch b.Kind {
		case ir.BlockToolResult:
			if tr != nil {
				return nil, fmt.Errorf("%w: multiple tool_result blocks", ErrToolResultShape)
			}
			tr = b.ToolResult
		case ir.BlockText:
			// tool 消息里的文本块（跨格式腿把结果放在文本块）——容忍，并入输出。
			if tr == nil {
				tr = &ir.ToolResult{}
			}
			if tr.Content != "" {
				tr.Content += "\n"
			}
			tr.Content += b.Text
		default:
			return nil, fmt.Errorf("%w: tool message block %q", ErrToolResultShape, b.Kind)
		}
	}
	if tr == nil {
		return nil, fmt.Errorf("%w: tool message without tool_result", ErrToolResultShape)
	}
	if tr.CallID == "" {
		return nil, fmt.Errorf("%w: tool_result without call_id", ErrToolResultShape)
	}
	return mustMarshal(map[string]json.RawMessage{
		"type":    mustMarshal(itemTypeFuncOutput),
		"call_id": mustMarshal(hygiene.SanitizeToolID(tr.CallID)),
		"output":  mustMarshal(tr.Content),
	}), nil
}

// renderImagePart 图像块 → input_image 部件（内联 → data URL；表 1「图片部件」行）。
func renderImagePart(img *ir.ImageSource) (json.RawMessage, error) {
	if img == nil {
		return nil, fmt.Errorf("%w: image without payload", ErrBadIR)
	}
	url := img.URL
	if url == "" && img.Data != "" {
		media := img.MediaType
		if media == "" {
			media = "application/octet-stream"
		}
		url = "data:" + media + ";base64," + img.Data
	}
	if url == "" {
		return nil, fmt.Errorf("%w", ErrImageWithoutPayload)
	}
	part := map[string]json.RawMessage{
		"type":      mustMarshal(partTypeInputImage),
		"image_url": mustMarshal(url),
	}
	if img.Detail != "" {
		part["detail"] = mustMarshal(img.Detail)
	}
	return mustMarshal(part), nil
}

// renderReasoningItem reasoning item（表 3「推理签名载体」行）：
// thinking → summary[{summary_text}] + encrypted_content（签名）；
// redacted_thinking → encrypted_content 前缀载体（CPA 约定）。
// status 为空 = 请求面（input 项不回写状态字段）；id 为空 = 请求面（不回写 id）。
// 响应面由 renderOutputItems 传派生 id——SDK/spec 必填面（W5-31 探针实测：
// 缺 id 的 reasoning/message 项过不了 openai-python 的 output[] 联合校验）。
func renderReasoningItem(text, signature string, redacted bool, status, id string) json.RawMessage {
	item := map[string]json.RawMessage{"type": mustMarshal(itemTypeReasoning)}
	if id != "" {
		item["id"] = mustMarshal(id)
	}
	if status != "" {
		item["status"] = mustMarshal(status)
	}
	if redacted {
		item["encrypted_content"] = mustMarshal(redactedThinkingPrefix + text)
		return mustMarshal(item)
	}
	if text != "" {
		item["summary"] = mustMarshal([]json.RawMessage{mustMarshal(map[string]json.RawMessage{
			"type": mustMarshal(partTypeSummary),
			"text": mustMarshal(text),
		})})
	} else {
		item["summary"] = json.RawMessage("[]")
	}
	if signature != "" {
		item["encrypted_content"] = mustMarshal(signature)
	}
	return mustMarshal(item)
}

// renderToolDef tools[] 项（**扁平** function 形态，表 1「工具声明」行）。
func renderToolDef(td ir.ToolDef) (json.RawMessage, error) {
	if td.Name == "" {
		return nil, fmt.Errorf("%w: tool without name", ErrBadIR)
	}
	out := map[string]json.RawMessage{
		"type": mustMarshal("function"),
		"name": mustMarshal(td.Name),
	}
	if td.Description != "" {
		out["description"] = mustMarshal(td.Description)
	}
	if hasRaw(td.InputSchema) {
		out["parameters"] = td.InputSchema
	}
	if td.Strict != nil {
		out["strict"] = mustMarshal(*td.Strict)
	}
	for k, v := range td.Extra {
		if _, exists := out[k]; !exists && hasRaw(v) {
			out[k] = v
		}
	}
	return mustMarshal(out), nil
}

// renderToolChoice IR 规范形（chat 形状）→ responses 扁平形态（表 1 行）。
func renderToolChoice(raw json.RawMessage) (json.RawMessage, error) {
	if !hasRaw(raw) {
		return nil, nil
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("%w: tool_choice: %v", ErrBadIR, err)
		}
		switch s {
		case "auto", "none", "required":
			return mustMarshal(s), nil
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnsupportedToolChoice, s)
		}
	case '{':
		var w struct {
			Type     string `json:"type"`
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &w); err != nil {
			return nil, fmt.Errorf("%w: tool_choice: %v", ErrBadIR, err)
		}
		if w.Type != "function" {
			return nil, fmt.Errorf("%w: %s", ErrUnsupportedToolChoice, truncate(string(raw), 80))
		}
		name := w.Function.Name
		if name == "" {
			name = w.Name
		}
		if name == "" {
			return nil, fmt.Errorf("%w: function without name", ErrUnsupportedToolChoice)
		}
		return mustMarshal(map[string]json.RawMessage{
			"type": mustMarshal("function"),
			"name": mustMarshal(name),
		}), nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedToolChoice, truncate(string(raw), 80))
	}
}

// responseFormatToTextFormat chat response_format → responses text.format
// （json_schema 的嵌套 json_schema 对象拍平，表 1 行）。
func responseFormatToTextFormat(raw json.RawMessage) json.RawMessage {
	if !hasRaw(raw) {
		return nil
	}
	var w struct {
		Type       string `json:"type"`
		JSONSchema struct {
			Name   string          `json:"name"`
			Schema json.RawMessage `json:"schema"`
			Strict *bool           `json:"strict"`
		} `json:"json_schema"`
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil
	}
	switch w.Type {
	case "json_object":
		return mustMarshal(map[string]json.RawMessage{"type": mustMarshal("json_object")})
	case "json_schema":
		out := map[string]json.RawMessage{"type": mustMarshal("json_schema")}
		if w.JSONSchema.Name != "" {
			out["name"] = mustMarshal(w.JSONSchema.Name)
		}
		if hasRaw(w.JSONSchema.Schema) {
			out["schema"] = w.JSONSchema.Schema
		}
		if w.JSONSchema.Strict != nil {
			out["strict"] = mustMarshal(*w.JSONSchema.Strict)
		}
		return mustMarshal(out)
	default:
		return nil
	}
}

// ---------------------------------------------------------------------------
// 非流式响应：IR → 线格式
// ---------------------------------------------------------------------------

// RenderResponse 渲染 Response 对象。status 由 StopReason 派生
// （length/error → incomplete + incomplete_details.reason，表 2 行）。
func RenderResponse(resp *ir.Response) ([]byte, error) {
	if resp == nil {
		return nil, fmt.Errorf("%w: nil response", ErrBadIR)
	}
	status, details := statusFor(resp.StopReason)
	id := resp.ID
	if id == "" {
		// 无上游 id → 合成（每响应唯一；常量会让客户端日志/transcript 无法区分）。
		id = hygiene.SynthUniqueID("resp")
	}
	// 合成工具 id 的 seed = 响应 id（响应内确定性 + 跨响应唯一）；id 先定，
	// 保证输出项 id / call_id 派生与 response.id 一致（渲染不动点靠它）。
	output, err := renderOutputItems(resp.Message, status, id)
	if err != nil {
		return nil, err
	}
	out := map[string]json.RawMessage{
		"id":         mustMarshal(id),
		"object":     mustMarshal(objectResponse),
		"created_at": mustMarshal(resp.Created),
		"status":     mustMarshal(status),
		"model":      mustMarshal(resp.Model),
		"output":     mustMarshal(output),
		"error":      json.RawMessage("null"),
	}
	// SDK 必填三键（openai-python 3.29.0 `Response` 必填 8 键实测，P01）：
	// parallel_tool_calls / tool_choice / tools——spec 的 Response.required 15 键
	// 含此三键。取值纪律：IR Extra 有则原样回填（保真；当前无解析器产这三键，
	// 属前向通道），否则 spec 默认（parallel_tool_calls 有显式 default:true；
	// tool_choice="auto"/tools=[] 取 spec 自证示例——ResponseQueuedEvent 与
	// responses 流式示例逐字同形）。响应渲染面拿不到请求面（pairs 响应腿只见
	// 上游响应体），故无「请求值回填」通道——见 W5-30 报告。
	out["parallel_tool_calls"] = extraOr(resp.Extra, "parallel_tool_calls", json.RawMessage("true"))
	out["tool_choice"] = extraOr(resp.Extra, "tool_choice", mustMarshal("auto"))
	out["tools"] = extraOr(resp.Extra, "tools", json.RawMessage("[]"))
	if details != nil {
		out["incomplete_details"] = details
	} else {
		out["incomplete_details"] = json.RawMessage("null")
	}
	if resp.Usage != nil {
		out["usage"] = renderUsage(resp.Usage)
	} else {
		// usage 恒有于 completed（表响应面行）；IR 无 usage 时显式 null——
		// 不合成假数（计费口径宁缺毋假）。
		out["usage"] = json.RawMessage("null")
	}
	return marshalBytes(out)
}

// 合成 response id 见 hygiene.SynthUniqueID("resp")（每响应唯一：`resp_synth_<32hex>`；
// 旧常量 `resp_synth` 跨响应同 id——客户端日志/transcript 无法区分响应）。

// statusFor StopReason → status + incomplete_details（表 2「完成态/截断」行）。
//
// claude.ai `stop_sequence`（含配额截断 `usage_limit`，IR StopSequence 保真）
// 经 StopStop → `completed`（Responses 契约无对应字段；flatten 与 openai_chat
// finishReasonFor 同款，W43-3 §4.3 R1 文档化）。截断信号可消费面 = usage
// （include_usage）+ IR StopSequence。
func statusFor(r ir.StopReason) (string, json.RawMessage) {
	switch r {
	case ir.StopLength:
		return statusIncomplete, mustMarshal(map[string]json.RawMessage{
			"reason": mustMarshal("max_output_tokens"),
		})
	case ir.StopError:
		return statusIncomplete, mustMarshal(map[string]json.RawMessage{
			"reason": mustMarshal("content_filter"),
		})
	default:
		return statusCompleted, nil
	}
}

// renderOutputItems assistant 消息块 → output[] 项（message / reasoning /
// function_call / Raw 保真项）。seed = 响应 id（合成工具 id 的派生源；空 =
// 无响应身份，回落块序形态）。
func renderOutputItems(m ir.Message, status, seed string) ([]json.RawMessage, error) {
	var (
		items   []json.RawMessage
		pending []json.RawMessage
	)
	// itemID 项 id：与流式面（stream_writer.go）同源同形态——`msg_/rs_/fc_` 前缀
	// + 响应 id 派生 + 项序（hygiene.SynthDerivedID；seed 空 → 块序回落）。
	// message/reasoning 项的 `id` 是 spec/SDK 必填面（W5-31 探针实测：缺 id 的
	// 批量响应过不了 openai-python 的 output[] 联合校验）；项 id 经响应 id 往返，
	// 保渲染不动点。消息项以 BlockText 取前缀（itemIDPrefix 默认分支 → "msg"）。
	itemID := func(kind ir.BlockKind) string {
		return hygiene.SynthDerivedID(itemIDPrefix(kind), seed, len(items))
	}
	flush := func() {
		if len(pending) == 0 {
			return
		}
		items = append(items, mustMarshal(map[string]json.RawMessage{
			"id":      mustMarshal(itemID(ir.BlockText)),
			"type":    mustMarshal(itemTypeMessage),
			"status":  mustMarshal(status),
			"role":    mustMarshal(roleAssistant),
			"content": mustMarshal(pending),
		}))
		pending = nil
	}
	for i, b := range m.Blocks {
		switch b.Kind {
		case ir.BlockText:
			pending = append(pending, mustMarshal(map[string]json.RawMessage{
				"type":        mustMarshal(partTypeOutputText),
				"text":        mustMarshal(b.Text),
				"annotations": json.RawMessage("[]"),
				"logprobs":    json.RawMessage("[]"),
			}))
		case ir.BlockRefusal:
			pending = append(pending, mustMarshal(map[string]json.RawMessage{
				"type":    mustMarshal(partTypeRefusal),
				"refusal": mustMarshal(b.Text),
			}))
		case ir.BlockThinking:
			flush()
			items = append(items, renderReasoningItem(b.Text, b.Signature, false, status, itemID(ir.BlockThinking)))
		case ir.BlockRedactedThinking:
			flush()
			items = append(items, renderReasoningItem(b.Text, b.Signature, true, status, itemID(ir.BlockRedactedThinking)))
		case ir.BlockToolUse, ir.BlockServerToolUse:
			if b.ToolCall == nil {
				return nil, fmt.Errorf("%w: tool_use without ToolCall", ErrBadIR)
			}
			flush()
			items = append(items, mustMarshal(map[string]json.RawMessage{
				"id":        mustMarshal(itemID(ir.BlockToolUse)),
				"type":      mustMarshal(itemTypeFunctionCall),
				"status":    mustMarshal(status),
				"call_id":   mustMarshal(hygiene.WireToolID("call", b.ToolCall.ID, seed, i)),
				"name":      mustMarshal(b.ToolCall.Name),
				"arguments": mustMarshal(toolArgumentsText(b.ToolCall.Arguments)),
			}))
		case ir.BlockUnknown:
			if len(b.Raw) == 0 {
				return nil, fmt.Errorf("%w: unknown block without Raw", ErrBadIR)
			}
			flush()
			items = append(items, b.Raw)
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnsupportedBlock, b.Kind)
		}
	}
	flush()
	return items, nil
}

// toolArgumentsText 工具参数出口：空参数补 "{}"（ir 块 3 出口契约）。
func toolArgumentsText(args string) string {
	if strings.TrimSpace(args) == "" {
		return "{}"
	}
	return args
}

// renderUsage IR Usage → Response.usage（表 2「usage」行；details 以 chat 规范键名
// 落 Extra，此处按 responses 名回写——input_tokens_details/output_tokens_details）。
//
// details 两对象**恒写**（W5-31 裁定）：spec `ResponseUsage.required` 含两 details
// （`input_tokens_details{cached_tokens,cache_write_tokens}` /
// `output_tokens_details{reasoning_tokens}`），openai-python 3.29.0 同必填——
// 缺失时 `Response.model_validate` / `ResponseCompletedEvent.model_validate`
// 报 missing（探针实测，lane L19-sweep14），严格客户端整条响应解析失败（比
// 合成零值贵一个量级——这是「宁缺毋假」的边界所在：缺必填键不是「少说」，是
// 「客户端读不了」）。取值纪律：
//
//   - 上游 details 原文已含全部必填键 → **逐字节透传**（保真；渲染不动点与
//     录帧回放逐字节稳定）；
//   - 原文缺键 / 缺失 → 按「原文值 → 派生值（CacheRead/Reasoning）→ 0」合成
//     完整对象。零值是「无证据」的诚实表述；input_tokens/output_tokens/
//     total_tokens 三个权威值不受影响（计费面不假）。
//
// 合成路径不携带原文的额外键（chat 面 audio_tokens 等）：responses 细节面只
// 定义上述键，带过去即跨 schema 污染。
func renderUsage(u *ir.Usage) json.RawMessage {
	return mustMarshal(map[string]json.RawMessage{
		"input_tokens":          mustMarshal(u.Input),
		"input_tokens_details":  inputTokensDetails(u),
		"output_tokens":         mustMarshal(u.Output),
		"output_tokens_details": outputTokensDetails(u),
		"total_tokens":          mustMarshal(u.Total),
	})
}

// inputTokensDetails → input_tokens_details（必填两键恒在）。
func inputTokensDetails(u *ir.Usage) json.RawMessage {
	var raw struct {
		CachedTokens     *int64 `json:"cached_tokens"`
		CacheWriteTokens *int64 `json:"cache_write_tokens"`
	}
	if d, ok := u.Extra["prompt_tokens_details"]; ok && hasRaw(d) {
		if err := json.Unmarshal(d, &raw); err == nil &&
			raw.CachedTokens != nil && raw.CacheWriteTokens != nil {
			return d
		}
	}
	cached, cacheWrite := u.CacheRead, int64(0)
	if raw.CachedTokens != nil {
		cached = *raw.CachedTokens
	}
	if raw.CacheWriteTokens != nil {
		cacheWrite = *raw.CacheWriteTokens
	}
	return mustMarshal(map[string]json.RawMessage{
		"cached_tokens":      mustMarshal(cached),
		"cache_write_tokens": mustMarshal(cacheWrite),
	})
}

// outputTokensDetails → output_tokens_details（reasoning_tokens 恒在）。
func outputTokensDetails(u *ir.Usage) json.RawMessage {
	var raw struct {
		ReasoningTokens *int64 `json:"reasoning_tokens"`
	}
	if d, ok := u.Extra["completion_tokens_details"]; ok && hasRaw(d) {
		if err := json.Unmarshal(d, &raw); err == nil && raw.ReasoningTokens != nil {
			return d
		}
	}
	reasoning := u.Reasoning
	if raw.ReasoningTokens != nil {
		reasoning = *raw.ReasoningTokens
	}
	return mustMarshal(map[string]json.RawMessage{
		"reasoning_tokens": mustMarshal(reasoning),
	})
}

// ---------------------------------------------------------------------------
// 错误：IR → 线格式
// ---------------------------------------------------------------------------

// RenderError 渲染非流式 HTTP 错误信封 `{"error":{message,type,param,code}}`
// （与 chat 相同信封；表「错误信封」行）。流内错误事件由 StreamWriter 渲染
// （平铺字段，**无嵌套 error 对象**）。
func RenderError(e *ir.Error) ([]byte, error) {
	if e == nil {
		e = &ir.Error{Type: "api_error", Message: "unknown error", Source: "any2api"}
	}
	inner := map[string]json.RawMessage{"message": mustMarshal(e.Message)}
	typ := e.Type
	if typ == "" {
		typ = "api_error"
	}
	inner["type"] = mustMarshal(typ)
	if e.Param != "" {
		inner["param"] = mustMarshal(e.Param)
	}
	if e.Code != "" {
		inner["code"] = mustMarshal(e.Code)
	}
	return marshalBytes(map[string]json.RawMessage{"error": mustMarshal(inner)})
}

// RenderStream 批量渲染：IR 事件序列 → Responses SSE 字节。
// 与 StreamWriter 同一内核（RenderStream = 逐事件 Write + Finish）。
func RenderStream(events []ir.StreamEvent) ([]byte, error) {
	w := NewStreamWriter()
	var buf bytes.Buffer
	for _, ev := range events {
		b, err := w.Write(ev)
		if err != nil {
			return buf.Bytes(), err
		}
		buf.Write(b)
	}
	tail, err := w.Finish()
	if err != nil {
		return buf.Bytes(), err
	}
	buf.Write(tail)
	return buf.Bytes(), nil
}

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

// extraOr Extra 键存在则原样回填，否则用默认值（SDK 必填三键的取值纪律：
// IR 有则用 / 否则 spec 默认；Extra 键为 null 视同缺省——不把 null 当值回填）。
func extraOr(extra map[string]json.RawMessage, key string, def json.RawMessage) json.RawMessage {
	if v, ok := extra[key]; ok && hasRaw(v) {
		return v
	}
	return def
}

func rawTypeOf(raw json.RawMessage) string {
	var w struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return ""
	}
	return w.Type
}

// isContentPartType 判断 Raw 是内容部件（而非 item 级对象）。
func isContentPartType(t string) bool {
	switch t {
	case partTypeInputText, partTypeInputImage, partTypeInputFile,
		"input_audio", "input_video", partTypeOutputText, partTypeRefusal:
		return true
	default:
		return false
	}
}

func marshalBytes(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadIR, err)
	}
	return b, nil
}
