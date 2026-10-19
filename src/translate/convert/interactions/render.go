package interactions

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dwgx/any2api/translate/hygiene"
	"github.com/dwgx/any2api/translate/ir"
)

// ---------------------------------------------------------------------------
// 请求：IR → 线格式
// ---------------------------------------------------------------------------

// interactionsPassableExtras 渲染 interactions 请求体时允许从 IR Extra 透传的顶层键
// （与 parse 侧的保真键一一对应；表内「透传族」行）。
var interactionsPassableExtras = []string{
	"store", "response_format", "responseMimeType", "responseModalities",
	"safety_settings", "service_tier", "labels",
}

// RenderRequest 渲染 POST /v1beta/interactions 请求体。
func RenderRequest(req *ir.Request) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: nil request", ErrBadIR)
	}
	if req.Model == "" {
		return nil, fmt.Errorf("%w: model is required", ErrBadRequest)
	}
	extra := req.Extra
	// 版本纪律（D5）：legacy（responseMimeType/responseModalities/image_config）与
	// 新 schema（response_format）择一，绝不混发。
	legacy := extraHas(extra, "responseMimeType") || extraHas(extra, "responseModalities") ||
		extraHas(extra, "image_config")
	if legacy && extraHas(extra, "response_format") {
		return nil, fmt.Errorf("%w", ErrMixedRevisions)
	}

	out := map[string]json.RawMessage{"model": mustMarshal(req.Model)}

	// 请求面（客户端历史回传）：无响应 id 可派生 → 合成工具 id 走块序回落形态
	//（seed 空）。
	steps, systemInstruction, err := renderMessages(req.Messages, "")
	if err != nil {
		return nil, err
	}
	if systemInstruction != "" {
		out["system_instruction"] = mustMarshal(systemInstruction)
	}
	if len(steps) > 0 {
		out["input"] = mustMarshal(steps)
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
	gc, err := renderGenerationConfig(req)
	if err != nil {
		return nil, err
	}
	if hasRaw(gc) {
		out["generation_config"] = gc
	}
	if req.Stream {
		out["stream"] = mustMarshal(true)
	}
	for _, k := range interactionsPassableExtras {
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

// renderMessages IR 消息 → input steps + system_instruction。
// system/developer 合并为扁平字符串（多条以 "\n" 连接，A1-6 同规则）；
// 其余消息逐条转 step 序列（顺序即语义——stateless 回放要求步序原样）。
// renderMessages 渲染请求面消息为 steps。seed = 响应 id（请求面无 → 空，块序回落）。
func renderMessages(messages []ir.Message, seed string) ([]json.RawMessage, string, error) {
	var (
		steps   []json.RawMessage
		systems []string
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
				systems = append(systems, strings.Join(texts, "\n"))
			}
			continue
		case ir.RoleTool:
			st, err := renderFunctionResultStep(m)
			if err != nil {
				return nil, "", fmt.Errorf("message %d: %w", i, err)
			}
			steps = append(steps, st)
			continue
		}
		st, err := renderMessageSteps(m, seed)
		if err != nil {
			return nil, "", fmt.Errorf("message %d: %w", i, err)
		}
		steps = append(steps, st...)
	}
	return steps, strings.Join(systems, "\n"), nil
}

// renderMessageSteps user/assistant 消息 → steps。
// 文本/媒体聚合成一个 model_output（assistant）或 user_input（user）步；
// thinking 独立成 thought 步、tool_use 独立成 function_call 步（步是内容层，
// 类型即语义）；BlockUnknown 带 Raw 时原样回写（保真旁路：服务端工具步等）。
func renderMessageSteps(m ir.Message, seed string) ([]json.RawMessage, error) {
	stepType := stepUserInput
	if m.Role == ir.RoleAssistant {
		stepType = stepModelOutput
	}
	var (
		steps   []json.RawMessage
		pending []json.RawMessage
	)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		steps = append(steps, mustMarshal(map[string]json.RawMessage{
			"type":    mustMarshal(stepType),
			"content": mustMarshal(pending),
		}))
		pending = nil
	}
	for _, b := range m.Blocks {
		switch b.Kind {
		case ir.BlockText:
			pending = append(pending, mustMarshal(map[string]json.RawMessage{
				"type": mustMarshal(contentText),
				"text": mustMarshal(b.Text),
			}))
		case ir.BlockImage:
			part, err := renderImagePart(b.Image)
			if err != nil {
				return nil, err
			}
			pending = append(pending, part)
		case ir.BlockAudio:
			if b.Audio == nil {
				return nil, fmt.Errorf("%w: audio without payload", ErrBadIR)
			}
			part := map[string]json.RawMessage{"type": mustMarshal(contentAudio)}
			if b.Audio.MediaType != "" {
				part["mime_type"] = mustMarshal(b.Audio.MediaType)
			}
			if b.Audio.Data != "" {
				part["data"] = mustMarshal(b.Audio.Data)
			}
			pending = append(pending, mustMarshal(part))
		case ir.BlockThinking, ir.BlockRedactedThinking:
			flush()
			steps = append(steps, renderThoughtStep(b))
		case ir.BlockToolUse, ir.BlockServerToolUse:
			if b.ToolCall == nil {
				return nil, fmt.Errorf("%w: tool_use without ToolCall", ErrBadIR)
			}
			flush()
			st, err := renderFunctionCallStep(b.ToolCall, len(steps), seed)
			if err != nil {
				return nil, err
			}
			steps = append(steps, st)
		case ir.BlockUnknown:
			if len(b.Raw) == 0 {
				return nil, fmt.Errorf("%w: unknown block without Raw", ErrBadIR)
			}
			flush()
			// step 级 Raw 原样回写；content 部件级 Raw 并入当前步的 content。
			if isStepType(rawTypeOf(b.Raw)) {
				steps = append(steps, b.Raw)
				continue
			}
			pending = append(pending, b.Raw)
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnsupportedBlock, b.Kind)
		}
	}
	flush()
	return steps, nil
}

// isStepType 21 种步类型判别式（OAS:Step oneOf 的 const 值）。
func isStepType(t string) bool {
	switch t {
	case stepUserInput, stepModelOutput, stepThought, stepFunctionCall, stepFunctionResult,
		"code_execution_call", "code_execution_result", "file_search_call", "file_search_result",
		"google_maps_call", "google_maps_result", "google_search_call", "google_search_result",
		"mcp_server_tool_call", "mcp_server_tool_result", "processing_call", "processing_result",
		"retrieval_call", "retrieval_result", "url_context_call", "url_context_result":
		return true
	default:
		return false
	}
}

// renderThoughtStep thinking 块 → thought 步。
// summary 承载文本（signature 必须原样回传）；无文本时省略 summary。
func renderThoughtStep(b ir.ContentBlock) json.RawMessage {
	step := map[string]json.RawMessage{"type": mustMarshal(stepThought)}
	if b.Signature != "" {
		step["signature"] = mustMarshal(b.Signature)
	}
	if b.Text != "" {
		step["summary"] = mustMarshal([]json.RawMessage{mustMarshal(map[string]json.RawMessage{
			"type": mustMarshal(contentText),
			"text": mustMarshal(b.Text),
		})})
	}
	return mustMarshal(step)
}

// renderFunctionCallStep tool_use 块 → function_call 步。
// **`id` 键**（Q1：绝不写 call_id）；id 出口统一卫生（hygiene：净化 + 缺失合成
// `<call>_synth_<响应 id 派生+步序>`——跨响应唯一；请求面无响应 id → 块序回落）；
// arguments 是 JSON 对象（空参补 "{}"，ir 块 3 出口契约）。
func renderFunctionCallStep(tc *ir.ToolCall, seq int, seed string) (json.RawMessage, error) {
	id := hygiene.WireToolID("call", tc.ID, seed, seq)
	args, err := toolArgumentsObject(tc.Arguments)
	if err != nil {
		return nil, err
	}
	return mustMarshal(map[string]json.RawMessage{
		"type":      mustMarshal(stepFunctionCall),
		"id":        mustMarshal(id),
		"name":      mustMarshal(tc.Name),
		"arguments": args,
	}), nil
}

// toolArgumentsObject 参数文本 → JSON 对象原文（空/空白 → {}；非对象 → 报错）。
func toolArgumentsObject(args string) (json.RawMessage, error) {
	trim := strings.TrimSpace(args)
	if trim == "" {
		return json.RawMessage("{}"), nil
	}
	if trim[0] != '{' {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedToolArgs, truncate(trim, 60))
	}
	if !json.Valid([]byte(trim)) {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedToolArgs, truncate(trim, 60))
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(trim)); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedToolArgs, truncate(trim, 60))
	}
	return json.RawMessage(buf.Bytes()), nil
}

// renderFunctionResultStep tool 消息 → function_result 步。
// `call_id` 键（与 function_call 的 `id` 相对，Q1）；name 从消息 Name 回写
// （可选字段，OAS:FunctionResultStep）。
func renderFunctionResultStep(m ir.Message) (json.RawMessage, error) {
	var tr *ir.ToolResult
	for _, b := range m.Blocks {
		switch b.Kind {
		case ir.BlockToolResult:
			if tr != nil {
				return nil, fmt.Errorf("%w: multiple tool_result blocks", ErrToolResultShape)
			}
			tr = b.ToolResult
		case ir.BlockText:
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
	step := map[string]json.RawMessage{
		"type":    mustMarshal(stepFunctionResult),
		"call_id": mustMarshal(hygiene.SanitizeToolID(tr.CallID)),
		"result": mustMarshal([]json.RawMessage{mustMarshal(map[string]json.RawMessage{
			"type": mustMarshal(contentText),
			"text": mustMarshal(tr.Content),
		})}),
	}
	if m.Name != "" {
		step["name"] = mustMarshal(m.Name)
	}
	if tr.IsError {
		step["is_error"] = mustMarshal(true)
	}
	return mustMarshal(step), nil
}

// renderImagePart 图像块 → image 内容部件（内联 data / uri 引用二选一）。
func renderImagePart(img *ir.ImageSource) (json.RawMessage, error) {
	if img == nil {
		return nil, fmt.Errorf("%w: image without payload", ErrBadIR)
	}
	part := map[string]json.RawMessage{"type": mustMarshal(contentImage)}
	if img.MediaType != "" {
		part["mime_type"] = mustMarshal(img.MediaType)
	}
	switch {
	case img.Data != "":
		part["data"] = mustMarshal(img.Data)
	case img.URL != "":
		part["uri"] = mustMarshal(img.URL)
	default:
		return nil, fmt.Errorf("%w", ErrImageWithoutPayload)
	}
	return mustMarshal(part), nil
}

// renderToolDef ToolDef → tools[] 项（扁平判别式：{type:"function",name,description,
// parameters}）。interactions 的 Function 无 `strict` 字段——IR 的 Strict 丢弃
// （不发明；表 B1-9 只映射 name/description/parameters）。
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
	for k, v := range td.Extra {
		if _, exists := out[k]; !exists && hasRaw(v) {
			out[k] = v
		}
	}
	return mustMarshal(out), nil
}

// renderGenerationConfig IR → generation_config（含 tool_choice）。
// 规范键（chat 形状）→ interactions 名；thinking 走 gemini 原文优先（B1-3/4/5）
// 再退 reasoning_effort 归一（A1-12 坑：不照抄值域；Q3：none/auto 不发）。
func renderGenerationConfig(req *ir.Request) (json.RawMessage, error) {
	extra := req.Extra
	out := map[string]json.RawMessage{}
	switch {
	case extraHas(extra, "max_tokens"):
		out["max_output_tokens"] = extra["max_tokens"]
	case extraHas(extra, "max_completion_tokens"):
		out["max_output_tokens"] = extra["max_completion_tokens"]
	}
	for _, k := range []string{"temperature", "top_p", "seed"} {
		if extraHas(extra, k) {
			out[k] = extra[k]
		}
	}
	if extraHas(extra, "stop") {
		out["stop_sequences"] = extra["stop"]
	}
	// thinking：gemini 侧 thinkingConfig 原文优先（B1-3/4/5），退 reasoning_effort。
	if extraHas(extra, "thinkingConfig") {
		applyThinkingConfig(extra["thinkingConfig"], out)
	} else if extraHas(extra, "reasoning_effort") {
		if lvl := thinkingLevelFor(extra["reasoning_effort"]); lvl != "" {
			out["thinking_level"] = mustMarshal(lvl)
		}
	}
	if extraHas(extra, "thinking_summaries") {
		out["thinking_summaries"] = extra["thinking_summaries"]
	}
	// 生成侧多模态配置：原样保真（官方字段名一致；image_config 属 legacy，
	// 与 response_format 的混发在 RenderRequest 已被拒）。
	for _, k := range []string{"image_config", "speech_config", "transcription_config", "video_config"} {
		if extraHas(extra, k) {
			out[k] = extra[k]
		}
	}
	tc, err := renderToolChoice(req.ToolChoice)
	if err != nil {
		return nil, err
	}
	if hasRaw(tc) {
		out["tool_choice"] = tc
	}
	if len(out) == 0 {
		return nil, nil
	}
	return mustMarshal(out), nil
}

// applyThinkingConfig gemini thinkingConfig 原文 → interactions 档位。
// thinkingLevel → thinking_level（小写化，B1-3）；includeThoughts →
// thinking_summaries（true→auto、false→none，B1-5）；**thinkingBudget 丢弃**
// （B1-4 do-not-copy：官方 GenerationConfig 无 thinking_budget）。
func applyThinkingConfig(raw json.RawMessage, out map[string]json.RawMessage) {
	var w struct {
		ThinkingLevel   string          `json:"thinkingLevel"`
		ThinkingBudget  json.RawMessage `json:"thinkingBudget"`
		IncludeThoughts *bool           `json:"includeThoughts"`
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return
	}
	if lvl := thinkingLevelFor(mustMarshal(strings.ToLower(w.ThinkingLevel))); lvl != "" {
		out["thinking_level"] = mustMarshal(lvl)
	}
	if w.IncludeThoughts != nil {
		if *w.IncludeThoughts {
			out["thinking_summaries"] = mustMarshal("auto")
		} else {
			out["thinking_summaries"] = mustMarshal("none")
		}
	}
}

// thinkingLevelFor reasoning_effort / gemini thinkingLevel → interactions 档位。
// 官方枚举 minimal/low/medium/high（无 none/auto——Q2/Q3 do-not-copy）：
// none/auto/未知 → 不发该字段（不发明）；xhigh/max → high（档位归一）。
func thinkingLevelFor(raw json.RawMessage) string {
	s := strings.ToLower(strings.TrimSpace(rawString(raw)))
	switch s {
	case thinkingMinimal, thinkingLow, thinkingMedium:
		return s
	case thinkingHigh, "xhigh", "max":
		return thinkingHigh
	default:
		return ""
	}
}

// renderToolChoice IR 规范形（chat 形状）→ interactions tool_choice。
// auto→auto；required→any（A2-4 反向）；none→none；`{type:"function",…}` →
// `{allowed_tools:{mode:"any",tools:[name]}}`（Q4：官方对象形态，CPA 的
// {"type":"function","name":…} 是 do-not-copy）。
func renderToolChoice(raw json.RawMessage) (json.RawMessage, error) {
	if !hasRaw(raw) {
		return nil, nil
	}
	trim := bytes.TrimSpace(raw)
	switch trim[0] {
	case '"':
		var s string
		if err := json.Unmarshal(trim, &s); err != nil {
			return nil, fmt.Errorf("%w: tool_choice: %v", ErrBadIR, err)
		}
		switch s {
		case "auto":
			return mustMarshal(toolChoiceAuto), nil
		case "required":
			return mustMarshal(toolChoiceAny), nil
		case "none":
			return mustMarshal(toolChoiceNone), nil
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
		if err := json.Unmarshal(trim, &w); err != nil {
			return nil, fmt.Errorf("%w: tool_choice: %v", ErrBadIR, err)
		}
		name := w.Function.Name
		if name == "" {
			name = w.Name
		}
		if name == "" {
			return nil, fmt.Errorf("%w: function without name", ErrUnsupportedToolChoice)
		}
		return mustMarshal(map[string]json.RawMessage{
			"allowed_tools": mustMarshal(map[string]json.RawMessage{
				"mode":  mustMarshal(toolChoiceAny),
				"tools": mustMarshal([]string{name}),
			}),
		}), nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedToolChoice, truncate(string(trim), 80))
	}
}

// ---------------------------------------------------------------------------
// 非流式响应：IR → 线格式
// ---------------------------------------------------------------------------

// RenderResponse 渲染 Interaction 资源。status 由 StopReason 派生
// （length→incomplete、error→failed、toolUse→requires_action）。
func RenderResponse(resp *ir.Response) ([]byte, error) {
	if resp == nil {
		return nil, fmt.Errorf("%w: nil response", ErrBadIR)
	}
	id := resp.ID
	if id == "" {
		// 无上游 id → 合成（每响应唯一；常量跨响应同 id）。
		id = hygiene.SynthUniqueID("int")
	}
	// 合成工具 id 的 seed = 响应 id（响应内确定性 + 跨响应唯一）；id 先定，
	// 保证步内 call id 派生与 interaction.id 一致（渲染不动点靠它）。
	steps, err := renderResponseSteps(resp.Message, id)
	if err != nil {
		return nil, err
	}
	created := formatRFC3339(resp.Created)
	out := map[string]json.RawMessage{
		"id":      mustMarshal(id),
		"object":  mustMarshal(objectInteraction),
		"status":  mustMarshal(statusFor(resp.StopReason)),
		"model":   mustMarshal(resp.Model),
		"created": mustMarshal(created),
		"updated": mustMarshal(created),
		"steps":   mustMarshal(steps),
	}
	if resp.Usage != nil {
		out["usage"] = renderUsage(resp.Usage)
	} else {
		out["usage"] = json.RawMessage("null")
	}
	if v, ok := resp.Extra["service_tier"]; ok && hasRaw(v) {
		out["service_tier"] = v
	}
	if v, ok := resp.Extra["errors"]; ok && hasRaw(v) {
		out["errors"] = v
	}
	return marshalBytes(out)
}

// statusFor StopReason → Interaction.status（OAS 8 值子集）。
func statusFor(r ir.StopReason) string {
	switch r {
	case ir.StopToolUse:
		return statusRequiresAction
	case ir.StopLength:
		return statusIncomplete
	case ir.StopError:
		return statusFailed
	default:
		return statusCompleted
	}
}

// formatRFC3339 unix 秒 → RFC3339 UTC（0 → 1970-01-01T00:00:00Z，确定性输出）。
func formatRFC3339(unix int64) string {
	return time.Unix(unix, 0).UTC().Format(time.RFC3339)
}

// renderResponseSteps assistant 消息块 → 响应 steps（model_output / thought /
// function_call；BlockUnknown 带 step 级 Raw 原样回写）。
// renderResponseSteps 渲染响应面消息为 steps。seed = 响应 id（合成工具 id 的派生源）。
func renderResponseSteps(m ir.Message, seed string) ([]json.RawMessage, error) {
	var (
		steps   []json.RawMessage
		pending []json.RawMessage
	)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		steps = append(steps, mustMarshal(map[string]json.RawMessage{
			"type":    mustMarshal(stepModelOutput),
			"content": mustMarshal(pending),
		}))
		pending = nil
	}
	for _, b := range m.Blocks {
		switch b.Kind {
		case ir.BlockText:
			pending = append(pending, mustMarshal(map[string]json.RawMessage{
				"type": mustMarshal(contentText),
				"text": mustMarshal(b.Text),
			}))
		case ir.BlockImage:
			part, err := renderImagePart(b.Image)
			if err != nil {
				return nil, err
			}
			pending = append(pending, part)
		case ir.BlockAudio:
			if b.Audio == nil {
				return nil, fmt.Errorf("%w: audio without payload", ErrBadIR)
			}
			part := map[string]json.RawMessage{"type": mustMarshal(contentAudio)}
			if b.Audio.MediaType != "" {
				part["mime_type"] = mustMarshal(b.Audio.MediaType)
			}
			if b.Audio.Data != "" {
				part["data"] = mustMarshal(b.Audio.Data)
			}
			pending = append(pending, mustMarshal(part))
		case ir.BlockThinking, ir.BlockRedactedThinking:
			flush()
			steps = append(steps, renderThoughtStep(b))
		case ir.BlockToolUse, ir.BlockServerToolUse:
			if b.ToolCall == nil {
				return nil, fmt.Errorf("%w: tool_use without ToolCall", ErrBadIR)
			}
			flush()
			st, err := renderFunctionCallStep(b.ToolCall, len(steps), seed)
			if err != nil {
				return nil, err
			}
			steps = append(steps, st)
		case ir.BlockUnknown:
			if len(b.Raw) == 0 {
				return nil, fmt.Errorf("%w: unknown block without Raw", ErrBadIR)
			}
			flush()
			if isStepType(rawTypeOf(b.Raw)) {
				steps = append(steps, b.Raw)
				continue
			}
			pending = append(pending, b.Raw)
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnsupportedBlock, b.Kind)
		}
	}
	flush()
	return steps, nil
}

// renderUsage IR Usage → usage（官方 total_* 一套；Q8：短名不发）。
func renderUsage(u *ir.Usage) json.RawMessage {
	m := map[string]json.RawMessage{
		"total_input_tokens":    mustMarshal(u.Input),
		"total_output_tokens":   mustMarshal(u.Output),
		"total_tokens":          mustMarshal(u.Total),
		"total_cached_tokens":   mustMarshal(u.CacheRead),
		"total_thought_tokens":  mustMarshal(u.Reasoning),
		"total_tool_use_tokens": mustMarshal(u.ServerTool),
	}
	for _, k := range []string{
		"input_tokens_by_modality", "output_tokens_by_modality",
		"cached_tokens_by_modality", "tool_use_tokens_by_modality",
		"grounding_tool_count",
	} {
		if v, ok := u.Extra[k]; ok && hasRaw(v) {
			m[k] = v
		}
	}
	return mustMarshal(m)
}

// ---------------------------------------------------------------------------
// 错误：IR → 线格式
// ---------------------------------------------------------------------------

// RenderError 渲染非 2xx HTTP 错误信封。官方参考未给例子（[INFERENCE]），按
// Google 惯例 `{"error":{code:int,message,status}}`——与 gemini 转换器同形
// （同一 API 家族，跨格式腿行为一致）。SSE 流内错误事件（code 为字符串）由
// StreamWriter 渲染。
func RenderError(e *ir.Error) ([]byte, error) {
	if e == nil {
		return nil, fmt.Errorf("%w", ErrNilError)
	}
	status, code := googleStatusForError(e)
	inner := map[string]json.RawMessage{
		"code":    code,
		"message": mustMarshal(e.Message),
		"status":  mustMarshal(status),
	}
	return marshalBytes(map[string]json.RawMessage{"error": mustMarshal(inner)})
}

// googleStatusForError IR 错误 → Google status + 数值 code（googleTypeForStatus
// 的反向；Code 为数值时原样回用——round trip 友好）。
func googleStatusForError(e *ir.Error) (string, json.RawMessage) {
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
	default:
		status = "INTERNAL"
	}
	if isNumericString(e.Code) {
		return status, json.RawMessage(e.Code)
	}
	return status, mustMarshal(googleHTTPCode(status))
}

// isNumericString 十进制数字串。
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

// googleHTTPCode Google status → HTTP 状态码（与 gemini 转换器同表）。
func googleHTTPCode(status string) int {
	switch status {
	case "NOT_FOUND":
		return 404
	case "INVALID_ARGUMENT":
		return 400
	case "UNAUTHENTICATED":
		return 401
	case "PERMISSION_DENIED":
		return 403
	case "RESOURCE_EXHAUSTED":
		return 429
	case "UNAVAILABLE":
		return 503
	case "DEADLINE_EXCEEDED":
		return 504
	case "CANCELLED":
		return 499
	default:
		return 500
	}
}

// RenderStream 批量渲染：IR 事件序列 → Interactions SSE 字节。
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

func marshalBytes(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadIR, err)
	}
	return b, nil
}
