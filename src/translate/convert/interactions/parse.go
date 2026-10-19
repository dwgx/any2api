package interactions

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/dwgx/any2api/translate/ir"
)

// ---------------------------------------------------------------------------
// 请求：线格式 → IR
// ---------------------------------------------------------------------------

// ParseRequest 解析 POST /v1beta/interactions 请求体。
// 宽容读取（缺省=不变、未知=忽略）；状态化/平台级字段显式拒绝（ErrStatefulFields）；
// 参数面以 **chat 规范键名**落 Extra（与 gemini 转换器的 generationConfig 映射同表，
// 跨格式腿的渲染器按各自白名单消费）。
func ParseRequest(data []byte) (*ir.Request, error) {
	var w requestWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	if err := checkStatefulFields(&w); err != nil {
		return nil, err
	}
	if w.Model == "" && !hasRaw(w.Agent) {
		return nil, fmt.Errorf("%w: model or agent is required", ErrBadRequest)
	}
	req := &ir.Request{Model: w.Model}
	if w.Stream != nil {
		req.Stream = *w.Stream
	}
	if w.SystemInstruction != "" {
		// system_instruction 是扁平字符串（OAS:Interaction.system_instruction）。
		req.Messages = append(req.Messages, ir.Message{
			Role:   ir.RoleSystem,
			Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: w.SystemInstruction}},
		})
	}
	msgs, err := parseInput(w.Input)
	if err != nil {
		return nil, err
	}
	req.Messages = append(req.Messages, msgs...)

	tools, err := parseTools(w.Tools)
	if err != nil {
		return nil, err
	}
	req.Tools = tools

	gc, toolChoice, err := parseGenerationConfig(w.GenerationConfig)
	if err != nil {
		return nil, err
	}
	req.ToolChoice = toolChoice

	extra, err := requestExtras(data, &w, gc)
	if err != nil {
		return nil, err
	}
	req.Extra = extra
	return req, nil
}

// checkStatefulFields 状态化/平台级字段硬报错（表 §请求形态「服务端会话」行 +
// 「IR 压力点」；照 responses 先例：静默丢 = 语义改变）。
// `store:false` 无服务端状态期望 → 放行（丢弃不改变语义）；`store:true` 无法在
// 无状态上游兑现（客户端后续 GET /interactions/{id} 会 404）→ 拒绝。
func checkStatefulFields(w *requestWire) error {
	var bad []string
	add := func(cond bool, name string) {
		if cond {
			bad = append(bad, name)
		}
	}
	add(w.PreviousInteraction != "", "previous_interaction_id")
	add(hasRaw(w.Agent), "agent")
	add(hasRaw(w.AgentConfig), "agent_config")
	add(hasRaw(w.Environment), "environment")
	add(w.EnvironmentID != "", "environment_id")
	add(w.Background != nil && *w.Background, "background")
	add(hasRaw(w.WebhookConfig), "webhook_config")
	add(w.ContinuationToken != "", "continuation_token")
	add(w.CachedContent != "", "cached_content")
	add(w.Store != nil && *w.Store, "store=true")
	if len(bad) > 0 {
		sort.Strings(bad)
		return fmt.Errorf("%w: %s", ErrStatefulFields, strings.Join(bad, ", "))
	}
	return nil
}

// requestKnown 请求体已建模顶层键（Extras 计算时排除；其余原样落 Extra 保真）。
var requestKnown = []string{
	"model", "agent", "input", "system_instruction", "tools", "generation_config",
	"stream", "store", "previous_interaction_id", "response_format",
	"response_mime_type", "response_modalities", "safety_settings", "service_tier",
	"background", "environment", "environment_id", "agent_config", "webhook_config",
	"continuation_token", "cached_content", "labels",
}

// requestExtras 计算请求 Extra：generation_config → 规范键；legacy/新 schema 择一
// （D5：绝不混发）；未建模顶层键原样保真。
func requestExtras(data []byte, w *requestWire, gc map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	extra := map[string]json.RawMessage{}
	for k, v := range gc {
		extra[k] = v
	}
	legacy := w.ResponseMimeType != "" || hasRaw(w.ResponseModalities)
	newSchema := hasRaw(w.ResponseFormat)
	if legacy && newSchema {
		// D5：legacy（response_mime_type/response_modalities/image_config）与新 schema
		// （response_format）并存于同一 OpenAPI，我们的 render 按客户端形态择一，
		// 混发 = 上游按 revision 择一执行、另一半静默失效 → 显式报错。
		return nil, fmt.Errorf("%w", ErrMixedRevisions)
	}
	if newSchema {
		rf, err := responseFormatCanonical(w.ResponseFormat)
		if err != nil {
			return nil, err
		}
		if hasRaw(rf) {
			extra["response_format"] = rf
		}
	}
	if w.ResponseMimeType != "" {
		extra["responseMimeType"] = mustMarshal(w.ResponseMimeType)
	}
	if mods := canonicalModalities(w.ResponseModalities); hasRaw(mods) {
		extra["responseModalities"] = mods
	}
	if hasRaw(w.SafetySettings) {
		extra["safety_settings"] = w.SafetySettings
	}
	if w.ServiceTier != "" {
		extra["service_tier"] = mustMarshal(w.ServiceTier)
	}
	if w.Store != nil && !*w.Store {
		// store:false 放行（无服务端状态期望）；渲染回 interactions 时原样透传。
		extra["store"] = mustMarshal(false)
	}
	if hasRaw(w.Labels) {
		extra["labels"] = w.Labels
	}
	for k, v := range extrasOf(data, requestKnown...) {
		if _, dup := extra[k]; !dup {
			extra[k] = v
		}
	}
	if len(extra) == 0 {
		return nil, nil
	}
	return extra, nil
}

// parseGenerationConfig generation_config → Extra 规范键 + ToolChoice。
// 规范键（chat 形状，与 gemini 转换器 generationConfig 映射同一套）：
// max_output_tokens→max_tokens、stop_sequences→stop、thinking_level→reasoning_effort
// （A2-9 反向）、thinking_summaries→thinking_summaries（无 chat 等价物，原样保留）。
// **不发明字段**：thinking_budget / thinking_config / presence_penalty /
// candidate_count / top_k 在 OAS:GenerationConfig 里不存在（Q2/Q5 do-not-copy）——
// 若客户端（非官方变体）发来，落 Extra 保真、渲染侧不回写。
func parseGenerationConfig(raw json.RawMessage) (map[string]json.RawMessage, json.RawMessage, error) {
	if !hasRaw(raw) {
		return nil, nil, nil
	}
	var w struct {
		MaxOutputTokens     json.RawMessage `json:"max_output_tokens"`
		Temperature         json.RawMessage `json:"temperature"`
		TopP                json.RawMessage `json:"top_p"`
		StopSequences       json.RawMessage `json:"stop_sequences"`
		Seed                json.RawMessage `json:"seed"`
		ThinkingLevel       string          `json:"thinking_level"`
		ThinkingSummaries   string          `json:"thinking_summaries"`
		ToolChoice          json.RawMessage `json:"tool_choice"`
		SpeechConfig        json.RawMessage `json:"speech_config"`
		TranscriptionConfig json.RawMessage `json:"transcription_config"`
		VideoConfig         json.RawMessage `json:"video_config"`
		ImageConfig         json.RawMessage `json:"image_config"`
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, nil, fmt.Errorf("%w: generation_config: %v", ErrBadRequest, err)
	}
	out := map[string]json.RawMessage{}
	set := func(key string, v json.RawMessage) {
		if hasRaw(v) {
			out[key] = v
		}
	}
	set("max_tokens", w.MaxOutputTokens)
	set("temperature", w.Temperature)
	set("top_p", w.TopP)
	set("stop", w.StopSequences)
	set("seed", w.Seed)
	if w.ThinkingLevel != "" {
		// A2-9：thinking_level → reasoning_effort（反向）；值域原样（官方枚举
		// minimal/low/medium/high 均在 chat 侧可见）。
		out["reasoning_effort"] = mustMarshal(w.ThinkingLevel)
	}
	if w.ThinkingSummaries != "" {
		out["thinking_summaries"] = mustMarshal(w.ThinkingSummaries)
	}
	// 无 IR/下游载体的生成配置（speech/transcription/video/image）保真落 Extra，
	// 渲染侧只按自己的形态回写（image_config 属 legacy，见 requestExtras 的择一纪律）。
	for k, v := range extrasOf(raw, "max_output_tokens", "temperature", "top_p",
		"stop_sequences", "seed", "thinking_level", "thinking_summaries", "tool_choice") {
		if _, dup := out[k]; !dup {
			out[k] = v
		}
	}
	tc, err := canonicalToolChoice(w.ToolChoice)
	if err != nil {
		return nil, nil, err
	}
	return out, tc, nil
}

// canonicalToolChoice interactions tool_choice → IR 规范形（chat 形状）。
// 枚举：auto→"auto"、any→"required"（A2-4：OpenAI 无 any）、none→"none"、
// validated→"auto"（[UNVERIFIED]：chat 无对应档，跟 gemini 转换器对未知枚举的
// 先例归 auto，不发明）。对象形态 {allowed_tools:{mode,tools[]}}（Q4：CPA 的
// {"type":"function","name":…} 是 do-not-copy）：单工具 → 规范 function 形态；
// 多工具/空工具集 → ErrUnsupportedToolChoice（规范形只承载单个工具名，不静默丢）。
func canonicalToolChoice(raw json.RawMessage) (json.RawMessage, error) {
	if !hasRaw(raw) {
		return nil, nil
	}
	trim := bytes.TrimSpace(raw)
	switch trim[0] {
	case '"':
		var s string
		if err := json.Unmarshal(trim, &s); err != nil {
			return nil, fmt.Errorf("%w: tool_choice: %v", ErrBadRequest, err)
		}
		switch s {
		case toolChoiceAuto:
			return mustMarshal("auto"), nil
		case toolChoiceAny:
			return mustMarshal("required"), nil
		case toolChoiceNone:
			return mustMarshal("none"), nil
		case toolChoiceValidated:
			return mustMarshal("auto"), nil
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnsupportedToolChoice, s)
		}
	case '{':
		var w struct {
			AllowedTools struct {
				Mode  string   `json:"mode"`
				Tools []string `json:"tools"`
			} `json:"allowed_tools"`
		}
		if err := json.Unmarshal(trim, &w); err != nil {
			return nil, fmt.Errorf("%w: tool_choice: %v", ErrBadRequest, err)
		}
		switch {
		case w.AllowedTools.Mode == toolChoiceNone:
			return mustMarshal("none"), nil
		case len(w.AllowedTools.Tools) == 1:
			return mustMarshal(map[string]json.RawMessage{
				"type":     mustMarshal("function"),
				"function": mustMarshal(map[string]json.RawMessage{"name": mustMarshal(w.AllowedTools.Tools[0])}),
			}), nil
		default:
			return nil, fmt.Errorf("%w: %s", ErrUnsupportedToolChoice, truncate(string(trim), 80))
		}
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedToolChoice, truncate(string(trim), 80))
	}
}

// responseFormatCanonical interactions response_format（新 schema）→ 规范
// response_format（chat 形状）。text 家族映射；image/audio/video 形态无任何上游
// 对应物 → 显式报错（不静默丢，也不把多模态输出请求伪装成文本请求）。
func responseFormatCanonical(raw json.RawMessage) (json.RawMessage, error) {
	var w struct {
		Type     string          `json:"type"`
		MimeType string          `json:"mime_type"`
		Schema   json.RawMessage `json:"schema"`
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, fmt.Errorf("%w: response_format: %v", ErrBadRequest, err)
	}
	if w.Type != contentText {
		return nil, fmt.Errorf("%w: response_format type %q", ErrUnsupportedBlock, w.Type)
	}
	switch w.MimeType {
	case "application/json":
		if hasRaw(w.Schema) {
			return mustMarshal(map[string]json.RawMessage{
				"type": mustMarshal("json_schema"),
				"json_schema": mustMarshal(map[string]json.RawMessage{
					"schema": w.Schema,
				}),
			}), nil
		}
		return mustMarshal(map[string]json.RawMessage{"type": mustMarshal("json_object")}), nil
	default:
		// text/plain 或缺省 mime_type → 纯文本（chat 的 "text"）。
		return mustMarshal(map[string]json.RawMessage{"type": mustMarshal("text")}), nil
	}
}

// canonicalModalities legacy response_modalities → gemini 规范键 responseModalities
// （大写；video/document 无 gemini 对应被丢弃——B2-6 同规则）。
func canonicalModalities(raw json.RawMessage) json.RawMessage {
	items := itemsOf(raw)
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		switch rawString(it) {
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

// parseTools tools[] → IR 工具声明。只有 `type:"function"` 有 IR 载体
// （OAS:Function{name,description,parameters}）；服务端工具（google_search /
// code_execution / url_context / google_maps / file_search / retrieval /
// mcp_server / computer_use）**丢弃**——与 gemini 转换器对内置工具的口径一致
// （表：由渠道配置注入，IR 无对应；不发明字段）。丢弃面有单测点名。
func parseTools(raw []json.RawMessage) ([]ir.ToolDef, error) {
	var out []ir.ToolDef
	for i, t := range raw {
		var tw struct {
			Type        string          `json:"type"`
			Name        string          `json:"name"`
			Description string          `json:"description"`
			Parameters  json.RawMessage `json:"parameters"`
		}
		if err := json.Unmarshal(t, &tw); err != nil {
			return nil, fmt.Errorf("%w: tool %d: %v", ErrBadRequest, i, err)
		}
		if tw.Type != "function" {
			continue
		}
		if tw.Name == "" {
			return nil, fmt.Errorf("%w: tool %d without name", ErrBadRequest, i)
		}
		out = append(out, ir.ToolDef{
			Name:        tw.Name,
			Description: tw.Description,
			InputSchema: tw.Parameters,
			Extra:       extrasOf(t, "type", "name", "description", "parameters"),
		})
	}
	return out, nil
}

// parseInput input（string | Content | Content[] | Step[]，OAS:InteractionsInput）
// → 消息序列。
func parseInput(raw json.RawMessage) ([]ir.Message, error) {
	if !hasRaw(raw) {
		return nil, nil
	}
	trim := bytes.TrimSpace(raw)
	if len(trim) == 0 {
		return nil, nil
	}
	switch trim[0] {
	case '"':
		var s string
		if err := json.Unmarshal(trim, &s); err != nil {
			return nil, fmt.Errorf("%w: input: %v", ErrBadRequest, err)
		}
		return []ir.Message{{
			Role:   ir.RoleUser,
			Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: s}},
		}}, nil
	case '{':
		// 单个 Content 对象 → 一条 user 消息。
		b, err := contentPartToBlock(trim)
		if err != nil {
			return nil, err
		}
		return []ir.Message{{Role: ir.RoleUser, Blocks: []ir.ContentBlock{b}}}, nil
	case '[':
		items := itemsOf(trim)
		if len(items) == 0 {
			return nil, nil
		}
		// 四态联合判别：数组元素 type 是内容类型（text/image/…）→ Content[]，
		// 否则按 Step[]（user_input/model_output/…）。
		if isContentType(rawTypeOf(items[0])) {
			blocks := make([]ir.ContentBlock, 0, len(items))
			for i, it := range items {
				b, err := contentPartToBlock(it)
				if err != nil {
					return nil, fmt.Errorf("input[%d]: %w", i, err)
				}
				blocks = append(blocks, b)
			}
			return []ir.Message{{Role: ir.RoleUser, Blocks: blocks}}, nil
		}
		return parseSteps(items)
	default:
		return nil, fmt.Errorf("%w: input: %s", ErrBadRequest, truncate(string(trim), 40))
	}
}

// parseSteps Step[] → 消息序列。连续同类步合并进同一消息（B2-12 同规则：
// 步是内容层，消息是角色层）；function_result 恒为独立 tool 消息。
func parseSteps(steps []json.RawMessage) ([]ir.Message, error) {
	var msgs []ir.Message
	for i, raw := range steps {
		var sw stepWire
		if err := json.Unmarshal(raw, &sw); err != nil {
			return nil, fmt.Errorf("%w: step %d: %v", ErrBadRequest, i, err)
		}
		if sw.Type == "" {
			return nil, fmt.Errorf("%w: step %d", ErrMissingStepType, i)
		}
		switch sw.Type {
		case stepUserInput:
			blocks, err := contentBlocks(sw.Content)
			if err != nil {
				return nil, fmt.Errorf("step %d: %w", i, err)
			}
			msgs = appendBlocks(msgs, ir.RoleUser, blocks)
		case stepModelOutput:
			blocks, err := contentBlocks(sw.Content)
			if err != nil {
				return nil, fmt.Errorf("step %d: %w", i, err)
			}
			msgs = appendBlocks(msgs, ir.RoleAssistant, blocks)
		case stepThought:
			b, err := thoughtBlock(&sw)
			if err != nil {
				return nil, fmt.Errorf("step %d: %w", i, err)
			}
			msgs = appendBlocks(msgs, ir.RoleAssistant, []ir.ContentBlock{b})
		case stepFunctionCall:
			b, err := functionCallBlock(&sw)
			if err != nil {
				return nil, fmt.Errorf("step %d: %w", i, err)
			}
			msgs = appendBlocks(msgs, ir.RoleAssistant, []ir.ContentBlock{b})
		case stepFunctionResult:
			tr, hoist, err := functionResultBlocks(&sw)
			if err != nil {
				return nil, fmt.Errorf("step %d: %w", i, err)
			}
			msgs = append(msgs, ir.Message{Role: ir.RoleTool, Name: sw.Name, Blocks: []ir.ContentBlock{tr}})
			if len(hoist) > 0 {
				// 结果里的媒体块提升为后续 user 消息（responses 同款纪律：
				// tool 消息保持单 tool_result 块）。
				msgs = append(msgs, ir.Message{Role: ir.RoleUser, Blocks: hoist})
			}
		default:
			// 服务端工具步（16 种）与未来新类型：BlockUnknown + Raw 保真旁路。
			msgs = appendBlocks(msgs, ir.RoleAssistant, []ir.ContentBlock{{Kind: ir.BlockUnknown, Raw: raw}})
		}
	}
	return msgs, nil
}

// appendBlocks 追加块；与上一条同角色则合并（消息边界 = 角色边界）。
func appendBlocks(msgs []ir.Message, role ir.Role, blocks []ir.ContentBlock) []ir.Message {
	if len(blocks) == 0 {
		return msgs
	}
	if n := len(msgs); n > 0 && msgs[n-1].Role == role {
		msgs[n-1].Blocks = append(msgs[n-1].Blocks, blocks...)
		return msgs
	}
	return append(msgs, ir.Message{Role: role, Blocks: blocks})
}

// contentBlocks 内容部件数组 → IR 块。
func contentBlocks(parts []json.RawMessage) ([]ir.ContentBlock, error) {
	blocks := make([]ir.ContentBlock, 0, len(parts))
	for i, p := range parts {
		b, err := contentPartToBlock(p)
		if err != nil {
			return nil, fmt.Errorf("content[%d]: %w", i, err)
		}
		blocks = append(blocks, b)
	}
	return blocks, nil
}

// isContentType 内容部件类型判别式（OAS:Content oneOf 的 5 种）。
func isContentType(t string) bool {
	switch t {
	case contentText, contentImage, contentAudio, contentVideo, contentDocument:
		return true
	default:
		return false
	}
}

// contentPartToBlock 内容部件 → IR 块。媒体类型由顶层 `type` 判别式给出
// （没有 generateContent 的 inlineData/fileData 包装层，也没有 OpenAI 的
// image_url 对象）；data（base64 内联）与 uri（引用）二选一。
// video/document 与只有 uri 的 audio 无 IR 载体 → BlockUnknown + Raw 保真。
func contentPartToBlock(raw json.RawMessage) (ir.ContentBlock, error) {
	var cw contentWire
	if err := json.Unmarshal(raw, &cw); err != nil {
		return ir.ContentBlock{}, fmt.Errorf("%w: content: %v", ErrBadRequest, err)
	}
	switch cw.Type {
	case contentText:
		return ir.ContentBlock{Kind: ir.BlockText, Text: cw.Text}, nil
	case contentImage:
		img := &ir.ImageSource{MediaType: cw.MimeType, Data: cw.Data, URL: cw.URI}
		switch {
		case cw.Data != "":
			img.Kind = "inline"
		case cw.URI != "":
			img.Kind = "file"
		default:
			return ir.ContentBlock{}, fmt.Errorf("%w: image without data/uri", ErrBadRequest)
		}
		return ir.ContentBlock{Kind: ir.BlockImage, Image: img}, nil
	case contentAudio:
		if cw.Data != "" {
			return ir.ContentBlock{Kind: ir.BlockAudio, Audio: &ir.AudioSource{MediaType: cw.MimeType, Data: cw.Data}}, nil
		}
		return ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, nil
	default:
		return ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw}, nil
	}
}

// thoughtBlock thought 步 → thinking 块。summary[]（TextContent | ImageContent）
// 的文本以 "\n\n" 连接（responses 的 reasoning 断段同规则）；签名原样承载
// （必须回传）。summary 里的 image 部件无 IR 载体 → 忽略（保留签名与文本）。
func thoughtBlock(sw *stepWire) (ir.ContentBlock, error) {
	var texts []string
	for _, s := range sw.Summary {
		var cw contentWire
		if err := json.Unmarshal(s, &cw); err != nil {
			continue
		}
		if cw.Type == contentText && cw.Text != "" {
			texts = append(texts, cw.Text)
		}
	}
	return ir.ContentBlock{
		Kind:      ir.BlockThinking,
		Text:      strings.Join(texts, "\n\n"),
		Signature: sw.Signature,
	}, nil
}

// functionCallBlock function_call 步 → tool_use 块。
// **id 用 `id`**（OAS:FunctionCallStep.required）；读取容忍 CPA 错键（Q1：CPA 把
// Gemini 的 functionCall.id 写进 `call_id`）——但渲染侧永远只写 `id`。
func functionCallBlock(sw *stepWire) (ir.ContentBlock, error) {
	id := sw.ID
	if id == "" {
		id = sw.CallID
	}
	if id == "" {
		return ir.ContentBlock{}, ErrMissingFunctionID
	}
	args := "{}"
	if hasRaw(sw.Arguments) {
		trim := bytes.TrimSpace(sw.Arguments)
		if len(trim) == 0 || trim[0] != '{' {
			return ir.ContentBlock{}, fmt.Errorf("%w: %s", ErrUnsupportedToolArgs, truncate(string(trim), 60))
		}
		// 规范化为 compact JSON 文本（IR Arguments 的约定形态；往返语义不变）。
		var buf bytes.Buffer
		if err := json.Compact(&buf, trim); err != nil {
			return ir.ContentBlock{}, fmt.Errorf("%w: %s", ErrUnsupportedToolArgs, truncate(string(trim), 60))
		}
		args = buf.String()
	}
	return ir.ContentBlock{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{
		ID:        id,
		Name:      sw.Name,
		Arguments: args,
	}}, nil
}

// functionResultBlocks function_result 步 → tool_result 块（+ 提升的媒体块）。
// call_id 必填（OAS:FunctionResultStep.required）；`result` 三态（string | object |
// Content[]）。
func functionResultBlocks(sw *stepWire) (ir.ContentBlock, []ir.ContentBlock, error) {
	if sw.CallID == "" {
		return ir.ContentBlock{}, nil, fmt.Errorf("%w: function_result without call_id", ErrToolResultShape)
	}
	text, hoist, err := resultPayload(sw.Result)
	if err != nil {
		return ir.ContentBlock{}, nil, err
	}
	tr := &ir.ToolResult{
		CallID:  sw.CallID,
		Content: text,
		IsError: sw.IsError != nil && *sw.IsError,
	}
	return ir.ContentBlock{Kind: ir.BlockToolResult, ToolResult: tr}, hoist, nil
}

// resultPayload result（string | object | Content[]）→ 文本 + 提升的媒体块。
func resultPayload(raw json.RawMessage) (string, []ir.ContentBlock, error) {
	if !hasRaw(raw) {
		return "", nil, nil
	}
	trim := bytes.TrimSpace(raw)
	if len(trim) == 0 {
		return "", nil, nil
	}
	switch trim[0] {
	case '"':
		var s string
		if err := json.Unmarshal(trim, &s); err != nil {
			return "", nil, fmt.Errorf("%w: result: %v", ErrToolResultShape, err)
		}
		return s, nil, nil
	case '[':
		var texts []string
		var hoist []ir.ContentBlock
		for _, item := range itemsOf(trim) {
			b, err := contentPartToBlock(item)
			if err != nil {
				return "", nil, err
			}
			if b.Kind == ir.BlockText {
				texts = append(texts, b.Text)
				continue
			}
			hoist = append(hoist, b)
		}
		return strings.Join(texts, "\n"), hoist, nil
	case '{':
		var buf bytes.Buffer
		if err := json.Compact(&buf, trim); err != nil {
			return "", nil, fmt.Errorf("%w: result: %v", ErrToolResultShape, err)
		}
		return buf.String(), nil, nil
	default:
		return string(trim), nil, nil
	}
}

// ---------------------------------------------------------------------------
// 非流式响应：线格式 → IR
// ---------------------------------------------------------------------------

// ParseResponse 解析 Interaction 资源（POST 响应；GET 全时间线亦可读）。
// steps[] → assistant 消息的块序列；status/finish_reason → StopReason；
// usage 缺省容忍（POST 响应含 usage，官方例子恒有）。
func ParseResponse(data []byte) (*ir.Response, error) {
	var w interactionWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadResponse, err)
	}
	if w.Object != "" && w.Object != objectInteraction {
		return nil, fmt.Errorf("%w: %q", ErrUnexpectedObject, w.Object)
	}
	blocks, err := responseBlocks(w.Steps)
	if err != nil {
		return nil, err
	}
	usage, err := parseUsage(w.Usage)
	if err != nil {
		return nil, err
	}
	resp := &ir.Response{
		ID:         w.ID,
		Model:      w.Model,
		Created:    parseRFC3339Unix(w.Created),
		Message:    ir.Message{Role: ir.RoleAssistant, Blocks: blocks},
		StopReason: stopReasonFromStatus(w.Status, w.FinishReason, hasToolUseBlocks(blocks)),
		Usage:      usage,
	}
	extra := map[string]json.RawMessage{}
	if w.ServiceTier != "" {
		extra["service_tier"] = mustMarshal(w.ServiceTier)
	}
	if hasRaw(w.Errors) {
		// 平台错误清单：保真旁路（渲染侧不回写；诊断用）。
		extra["errors"] = w.Errors
	}
	if len(extra) > 0 {
		resp.Extra = extra
	}
	return resp, nil
}

// responseBlocks 响应 steps[] → 块序列。POST 只回输出步（BC:36-38）：
// model_output / thought / function_call 建模；user_input / function_result /
// 服务端工具步（GET 全时间线或未来新类型）以 BlockUnknown + Raw 保真旁路。
func responseBlocks(steps []json.RawMessage) ([]ir.ContentBlock, error) {
	var blocks []ir.ContentBlock
	for i, raw := range steps {
		var sw stepWire
		if err := json.Unmarshal(raw, &sw); err != nil {
			return nil, fmt.Errorf("%w: step %d: %v", ErrBadResponse, i, err)
		}
		switch sw.Type {
		case stepModelOutput:
			bs, err := contentBlocks(sw.Content)
			if err != nil {
				return nil, fmt.Errorf("step %d: %w", i, err)
			}
			blocks = append(blocks, bs...)
		case stepThought:
			b, err := thoughtBlock(&sw)
			if err != nil {
				return nil, fmt.Errorf("step %d: %w", i, err)
			}
			blocks = append(blocks, b)
		case stepFunctionCall:
			b, err := functionCallBlock(&sw)
			if err != nil {
				return nil, fmt.Errorf("step %d: %w", i, err)
			}
			blocks = append(blocks, b)
		default:
			blocks = append(blocks, ir.ContentBlock{Kind: ir.BlockUnknown, Raw: raw})
		}
	}
	return blocks, nil
}

// hasToolUseBlocks 块序列含客户端工具调用。
func hasToolUseBlocks(blocks []ir.ContentBlock) bool {
	for _, b := range blocks {
		if b.Kind == ir.BlockToolUse {
			return true
		}
	}
	return false
}

// stopReasonFromStatus status/finish_reason → IR 四值。
// finish_reason 不在 OpenAPI（CPA/文档例子里的非标字段）——读取作 tiebreaker，
// 渲染侧不发（不发明字段）。requires_action 与「有 function_call 步」→ toolUse
// （A3-7：function_call 步 → finish_reason tool_calls）。
func stopReasonFromStatus(status, finishReason string, sawToolUse bool) ir.StopReason {
	switch strings.ToLower(strings.TrimSpace(finishReason)) {
	case "length", "max_tokens":
		return ir.StopLength
	case "content_filter":
		return ir.StopError
	}
	switch strings.ToLower(strings.TrimSpace(status)) {
	case statusRequiresAction:
		return ir.StopToolUse
	case statusIncomplete, statusBudgetExceeded:
		return ir.StopLength
	case statusFailed, statusCancelled:
		return ir.StopError
	}
	if sawToolUse {
		return ir.StopToolUse
	}
	return ir.StopStop
}

// parseUsage Usage → IR Usage（OAS:Usage）。
// 记账：total_input_tokens→Input、total_output_tokens→Output、total_tokens→Total、
// total_cached_tokens→CacheRead、total_thought_tokens→Reasoning、
// total_tool_use_tokens→ServerTool；模态分解原文进 Extra 保真。
// D2 容错读取：文档例子里出现的短名（prompt_tokens/completion_tokens/
// input_tokens/output_tokens）在 OAS 里没有——容忍读取（仅当长名缺失时），
// 渲染侧只发官方 total_* 一套（Q8）。
func parseUsage(raw json.RawMessage) (*ir.Usage, error) {
	if !hasRaw(raw) {
		return nil, nil
	}
	var w usageWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, fmt.Errorf("%w: usage: %v", ErrBadResponse, err)
	}
	u := &ir.Usage{
		Input:      w.TotalInputTokens,
		Output:     w.TotalOutputTokens,
		Total:      w.TotalTokens,
		CacheRead:  w.TotalCachedTokens,
		Reasoning:  w.TotalThoughtTokens,
		ServerTool: w.TotalToolUseTokens,
		Accounting: "gemini",
	}
	var short struct {
		PromptTokens     *int64 `json:"prompt_tokens"`
		CompletionTokens *int64 `json:"completion_tokens"`
		InputTokens      *int64 `json:"input_tokens"`
		OutputTokens     *int64 `json:"output_tokens"`
	}
	if err := json.Unmarshal(raw, &short); err == nil {
		if u.Input == 0 {
			switch {
			case short.PromptTokens != nil:
				u.Input = *short.PromptTokens
			case short.InputTokens != nil:
				u.Input = *short.InputTokens
			}
		}
		if u.Output == 0 {
			switch {
			case short.CompletionTokens != nil:
				u.Output = *short.CompletionTokens
			case short.OutputTokens != nil:
				u.Output = *short.OutputTokens
			}
		}
	}
	extra := map[string]json.RawMessage{}
	for k, v := range map[string]json.RawMessage{
		"input_tokens_by_modality":    w.InputTokensByModality,
		"output_tokens_by_modality":   w.OutputTokensByModality,
		"cached_tokens_by_modality":   w.CachedTokensByModality,
		"tool_use_tokens_by_modality": w.ToolUseTokensByModality,
		"grounding_tool_count":        w.GroundingToolCount,
	} {
		if hasRaw(v) {
			extra[k] = v
		}
	}
	if len(extra) > 0 {
		u.Extra = extra
	}
	return u, nil
}

// mergeUsage 累计覆盖非累加（stream-mechanics §7.5）：非零字段覆盖，零值不覆盖
// 已有真值（快照 merge-non-zero；responses 同款）。
func mergeUsage(cur, next *ir.Usage) *ir.Usage {
	if next == nil {
		return cur
	}
	if cur == nil {
		return next
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
	if next.ServerTool != 0 {
		cur.ServerTool = next.ServerTool
	}
	for k, v := range next.Extra {
		if cur.Extra == nil {
			cur.Extra = map[string]json.RawMessage{}
		}
		cur.Extra[k] = v
	}
	return cur
}

// parseRFC3339Unix RFC3339 时间串 → unix 秒（空/畸形 → 0）。
func parseRFC3339Unix(s string) int64 {
	if s == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0
	}
	return t.Unix()
}

// ---------------------------------------------------------------------------
// 错误：线格式 → IR
// ---------------------------------------------------------------------------

// ParseError 解析两种错误信封：
//   - 非 2xx HTTP 响应体：Google 惯例 `{"error":{code:int,message,status}}`
//     （官方参考未给例子，[INFERENCE]——按 gemini 转换器同款归一）；
//   - SSE 错误事件载荷 `{"error":{message,code:"gateway_timeout"},"event_type":"error"}`
//     ——`code` 是**字符串**（ST:186-188）。
//
// 非 JSON / 非标准 body 不报错，归一为泛型 api_error（Raw 保留原文供诊断）。
func ParseError(body []byte) *ir.Error {
	var env struct {
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil || !hasRaw(env.Error) {
		// W5-22 条目 4：非标准错误体走兜底提取（one-api 7 路径表——msg/err/
		// error_msg/header.message/response.error.message 等形态的最小信息抢救）。
		return ir.SalvageError(body, "interactions")
	}
	var ew struct {
		Code    json.RawMessage `json:"code"`
		Message string          `json:"message"`
		Status  string          `json:"status"`
	}
	e := &ir.Error{Source: "interactions", Raw: env.Error}
	if err := json.Unmarshal(env.Error, &ew); err != nil {
		e.Type = "api_error"
		e.Message = "malformed upstream error object"
		return e
	}
	e.Message = ew.Message
	e.Code = rawStringOrNumber(ew.Code)
	e.Type, e.Retryable = googleTypeForStatus(ew.Status)
	if e.Type == "" {
		// SSE 错误事件的 code 是字符串枚举（gateway_timeout/not_found）——按
		// 已知值给类型，其余归 api_error（不猜 status 字段）。
		switch e.Code {
		case "gateway_timeout":
			e.Type = "timeout_error"
			e.Retryable = new(true)
		case "not_found":
			e.Type = "not_found_error"
		default:
			e.Type = "api_error"
		}
	}
	return e
}

// googleTypeForStatus Google status → 归一类型 + retryable（与 gemini 转换器
// openaiTypeForGoogleStatus 同表；interactions 属同一 API 家族）。
func googleTypeForStatus(status string) (string, *bool) {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "":
		return "", nil
	case "NOT_FOUND":
		return "not_found_error", new(false)
	case "INVALID_ARGUMENT", "FAILED_PRECONDITION", "OUT_OF_RANGE":
		return "invalid_request_error", new(false)
	case "UNAUTHENTICATED":
		return "invalid_api_key", new(false)
	case "PERMISSION_DENIED":
		return "permission_error", new(false)
	case "RESOURCE_EXHAUSTED":
		return "rate_limit_error", new(true)
	case "UNAVAILABLE":
		return "overloaded_error", new(true)
	case "DEADLINE_EXCEEDED":
		return "timeout_error", new(true)
	case "INTERNAL", "UNKNOWN", "ABORTED":
		return "server_error", new(true)
	case "CANCELLED":
		return "api_error", new(false)
	default:
		return "api_error", nil
	}
}

// rawStringOrNumber code 可能是字符串（gateway_timeout）或数字（HTTP code）。
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

// StreamParser 流式解析状态机（Interactions SSE 事件 → IR 事件）。
//
// 三拍纪律：step.start → step.delta* → step.stop；交互级事件
// （interaction.created / interaction.status_update / interaction.completed）
// 与 `event: done` + `[DONE]` 哨兵。
type StreamParser struct {
	started   bool
	terminal  bool
	errorSeen bool
	doneSeen  bool

	usage      *ir.Usage
	stopReason ir.StopReason
	sawToolUse bool
	status     string

	// blocks index → 步类型；服务端工具步记自身类型但不产 IR 事件（无下游载体）。
	blocks map[int]string
}

// NewStreamParser 创建流式解析状态机。
func NewStreamParser() *StreamParser {
	return &StreamParser{blocks: map[int]string{}}
}

// Feed 处理一个 SSE 帧（`event:` 行 + data 载荷）。event 可为空（帧无命名事件），
// 非空时与 payload 的 event_type 做一致性校验（Q6：不一致 → ErrEventTypeMismatch，
// 不静默按 data 分派）。返回本次新增的 IR 事件。
func (p *StreamParser) Feed(event string, data []byte) ([]ir.StreamEvent, error) {
	trim := bytes.TrimSpace(data)
	if len(trim) == 0 {
		return nil, nil
	}
	if isDoneSentinel(string(trim)) {
		p.doneSeen = true
		return nil, nil
	}
	var ev eventWire
	if err := json.Unmarshal(trim, &ev); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadChunk, err)
	}
	name := event
	if name == "" {
		name = ev.EventType
	}
	if event != "" && ev.EventType != "" && event != ev.EventType {
		return nil, fmt.Errorf("%w: event=%q event_type=%q", ErrEventTypeMismatch, event, ev.EventType)
	}
	switch name {
	case evInteractionCreated:
		if p.started {
			return nil, nil // 重复 created 容忍忽略
		}
		p.started = true
		var iw interactionWire
		if hasRaw(ev.Interaction) {
			if err := json.Unmarshal(ev.Interaction, &iw); err != nil {
				return nil, fmt.Errorf("%w: interaction.created: %v", ErrBadChunk, err)
			}
		}
		return []ir.StreamEvent{{
			Kind:      ir.EventStart,
			MessageID: iw.ID,
			Model:     iw.Model,
			Created:   parseRFC3339Unix(iw.Created),
		}}, nil

	case evInteractionStatus, evInteractionInProgress, evInteractionRequires:
		// D1：两套事件名都吃；IR 无状态事件（stop reason 在终态统一判定）。
		p.status = ev.Status
		return nil, nil

	case evStepStart:
		if ev.Index == nil {
			return nil, fmt.Errorf("%w: step.start without index", ErrBadChunk)
		}
		var sw stepWire
		if hasRaw(ev.Step) {
			if err := json.Unmarshal(ev.Step, &sw); err != nil {
				return nil, fmt.Errorf("%w: step.start: %v", ErrBadChunk, err)
			}
		}
		if sw.Type == "" {
			return nil, fmt.Errorf("%w: step.start step without type", ErrMissingStepType)
		}
		kind, callID, toolName, ok := blockKindForStep(&sw)
		if !ok {
			// 服务端工具步/未知步：无 IR 载体，容忍丢弃（不炸链）——记空类型，
			// 其 step.delta/step.stop 后继帧一并忽略。
			p.blocks[*ev.Index] = ""
			return nil, nil
		}
		p.blocks[*ev.Index] = sw.Type
		if sw.Type == stepFunctionCall {
			p.sawToolUse = true
		}
		return []ir.StreamEvent{{
			Kind:       ir.EventBlockStart,
			BlockIndex: *ev.Index,
			BlockKind:  kind,
			ToolCallID: callID,
			ToolName:   toolName,
		}}, nil

	case evStepDelta:
		if ev.Index == nil {
			return nil, fmt.Errorf("%w: step.delta without index", ErrBadChunk)
		}
		stepType, known := p.blocks[*ev.Index]
		if !known || stepType == "" {
			return nil, nil // 未知/已丢弃步的后继帧 → 忽略
		}
		return p.deltaEvents(*ev.Index, stepType, ev.Delta)

	case evStepStop:
		if ev.Index == nil {
			return nil, fmt.Errorf("%w: step.stop without index", ErrBadChunk)
		}
		if stepType, known := p.blocks[*ev.Index]; !known || stepType == "" {
			return nil, nil
		}
		// OAS:StepStop 可带 usage（累计）/step_usage（单步）：累计值合并入记账
		// （merge-non-zero，最终随 message_delta 一次下发），step_usage 不参与
		//（单步统计不是会话口径，避免同一 usage 双计）。
		if u, err := parseUsage(ev.Usage); err != nil {
			return nil, fmt.Errorf("%w: step.stop usage: %v", ErrBadChunk, err)
		} else if u != nil {
			p.usage = mergeUsage(p.usage, u)
		}
		return []ir.StreamEvent{{Kind: ir.EventBlockStop, BlockIndex: *ev.Index}}, nil

	case evInteractionCompleted:
		var iw interactionWire
		if hasRaw(ev.Interaction) {
			if err := json.Unmarshal(ev.Interaction, &iw); err != nil {
				return nil, fmt.Errorf("%w: interaction.completed: %v", ErrBadChunk, err)
			}
		}
		p.terminal = true
		if u, err := parseUsage(iw.Usage); err != nil {
			return nil, fmt.Errorf("%w: interaction.completed usage: %v", ErrBadChunk, err)
		} else if u != nil {
			p.usage = mergeUsage(p.usage, u)
		}
		status := iw.Status
		if status == "" {
			status = p.status
		}
		reason := stopReasonFromStatus(status, iw.FinishReason, p.sawToolUse)
		p.stopReason = reason
		out := []ir.StreamEvent{{Kind: ir.EventMessageDelta, StopReason: reason}}
		if p.usage != nil {
			out = append(out, ir.StreamEvent{Kind: ir.EventMessageDelta, Usage: p.usage})
		}
		out = append(out,
			ir.StreamEvent{Kind: ir.EventMessageStop},
			ir.StreamEvent{Kind: ir.EventDone, Reason: reason},
		)
		return out, nil

	case evError, evInteractionFailed, evResponseFailed:
		// Q7：官方 Gemini 流不发 failed 别名（devin 等非 Gemini 上游会），容忍读取、
		// 绝不主动生成。错误信封从 payload 的 error 对象归一。
		p.errorSeen = true
		e := &ir.Error{Type: "api_error", Message: "upstream stream error", Source: "interactions"}
		if hasRaw(ev.Error) {
			e = ParseError(mustMarshal(map[string]json.RawMessage{"error": ev.Error}))
			e.Source = "interactions"
		}
		return []ir.StreamEvent{{
			Kind:        ir.EventError,
			ErrorReason: ir.ErrorUpstream,
			Error:       e,
		}}, nil

	case evDone:
		p.doneSeen = true
		return nil, nil

	default:
		// 未知事件容忍（文档漂移/legacy 事件名；D1/D4）。
		return nil, nil
	}
}

// deltaEvents step.delta → IR block_delta。
// 判别式按 delta.type 分派；与步类型不匹配（或 IR 无载体：image/audio/video/
// document 媒体增量、服务端工具 delta、标注增量）→ 忽略（无载体，不炸链）。
func (p *StreamParser) deltaEvents(index int, stepType string, raw json.RawMessage) ([]ir.StreamEvent, error) {
	if !hasRaw(raw) {
		return nil, nil
	}
	var dw struct {
		Type      string          `json:"type"`
		Text      string          `json:"text"`
		Signature string          `json:"signature"`
		Arguments string          `json:"arguments"`
		Content   json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(raw, &dw); err != nil {
		return nil, fmt.Errorf("%w: step.delta: %v", ErrBadChunk, err)
	}
	switch dw.Type {
	case deltaText:
		if stepType != stepModelOutput {
			return nil, nil
		}
		return []ir.StreamEvent{{
			Kind: ir.EventBlockDelta, BlockIndex: index,
			BlockKind: ir.BlockText, TextDelta: dw.Text,
		}}, nil
	case deltaThoughtSummary:
		if stepType != stepThought {
			return nil, nil
		}
		var cw contentWire
		if hasRaw(dw.Content) {
			if err := json.Unmarshal(dw.Content, &cw); err != nil {
				return nil, fmt.Errorf("%w: thought_summary content: %v", ErrBadChunk, err)
			}
		}
		if cw.Type != contentText {
			return nil, nil // 图像摘要无 IR 载体
		}
		return []ir.StreamEvent{{
			Kind: ir.EventBlockDelta, BlockIndex: index,
			BlockKind: ir.BlockThinking, TextDelta: cw.Text,
		}}, nil
	case deltaThoughtSignature:
		if stepType != stepThought {
			return nil, nil
		}
		return []ir.StreamEvent{{
			Kind: ir.EventBlockDelta, BlockIndex: index,
			BlockKind: ir.BlockThinking, SignatureDelta: dw.Signature,
		}}, nil
	case deltaArguments:
		if stepType != stepFunctionCall {
			return nil, nil
		}
		// arguments_delta 是**分片 JSON 字符串**，客户端必须累积（BC:413-419）——
		// IR 的 ArgsDelta 即片段语义，累积/修复由出口组装层收口。
		return []ir.StreamEvent{{
			Kind: ir.EventBlockDelta, BlockIndex: index,
			BlockKind: ir.BlockToolUse, ArgsDelta: dw.Arguments,
		}}, nil
	case deltaImage, deltaAudio, deltaVideo, deltaDocument:
		// 媒体增量：IR block_delta 无媒体载体 → 忽略（[UNVERIFIED] 边界，
		// 见报告；文本/工具面不受影响）。
		return nil, nil
	default:
		return nil, nil
	}
}

// blockKindForStep 步类型 → IR 块（建模子集 5 种里的 3 种产流事件）。
func blockKindForStep(sw *stepWire) (kind ir.BlockKind, callID, name string, ok bool) {
	switch sw.Type {
	case stepModelOutput:
		return ir.BlockText, "", "", true
	case stepThought:
		return ir.BlockThinking, "", "", true
	case stepFunctionCall:
		id := sw.ID
		if id == "" {
			id = sw.CallID // Q1 容忍读取（CPA 错键）
		}
		return ir.BlockToolUse, id, sw.Name, true
	default:
		return "", "", "", false
	}
}

// Finish 流结束（EOF）时调用：产出未发出的终态事件。
//
//   - 已见 interaction.completed / error → 无补充；
//   - 其余（含只见 `[DONE]` 或整段未见）→ 截断流判失败（transport）——
//     `[DONE]` 按官方形态恒随 interaction.completed 之后，单独出现 = 断流。
func (p *StreamParser) Finish() []ir.StreamEvent {
	if p.terminal || p.errorSeen {
		return nil
	}
	return []ir.StreamEvent{{
		Kind:        ir.EventError,
		ErrorReason: ir.ErrorTransport,
		Error: &ir.Error{
			Type:    "incomplete_stream",
			Message: "interactions stream ended without interaction.completed",
			Source:  "interactions",
		},
	}}
}

// isDoneSentinel 判断 `[DONE]` 哨兵（`event: done` + `data: [DONE]`）。
func isDoneSentinel(s string) bool {
	return strings.TrimSpace(s) == doneSentinel
}

// ParseStream 便捷入口：从 SSE 字节流解析全部 IR 事件（内含最小切帧器）。
//
// 切帧规则（与 claude_messages 同款 WHATWG 子集）：注释行（心跳）不产事件；
// `event:` 行参与一致性校验（Q6）；多行 data 以 "\n" 连接；空行派发；EOF 时
// 派发未闭合帧（宽容）。
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
			continue
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
		// id: / retry: 等字段行：Interactions 流不使用（event_id 在 payload 里），忽略。
	}
	if err := flush(); err != nil {
		return events, err
	}
	if err := sc.Err(); err != nil {
		return events, err
	}
	events = append(events, p.Finish()...)
	return events, nil
}
