package gemini

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// ---------------------------------------------------------------------------
// schema 子集清洗（两载体差异）
// ---------------------------------------------------------------------------

func TestSchemaTightCarrierDropsUnsupported(t *testing.T) {
	raw := json.RawMessage(`{
		"type": "object",
		"properties": {
			"name": {"type": "string", "minLength": 2, "pattern": "^a"},
			"tags": {"type": "array", "items": {"type": "string"}, "minItems": 1}
		},
		"required": ["name"]
	}`)
	// 载体选择规则：标准约束是富载体独有面 → 自动选择会走 parametersJsonSchema
	//（信息保留优先）；紧载体路径由调用方显式指定时按紧口径清洗。
	if !schemaNeedsRichCarrier(raw) {
		t.Fatalf("standard constraints must select the rich carrier")
	}
	cleaned, err := cleanSchema(raw, false)
	if err != nil {
		t.Fatalf("cleanSchema: %v", err)
	}
	s := string(cleaned)
	for _, banned := range []string{`"minLength"`, `"pattern"`, `"minItems"`} {
		if strings.Contains(s, banned) {
			t.Fatalf("%s survived tight cleaning: %s", banned, s)
		}
	}
	// 约束转 description 提示（不静默丢语义）。
	if !strings.Contains(s, "minLength: 2") || !strings.Contains(s, "pattern: ^a") {
		t.Fatalf("constraint hints missing: %s", s)
	}
}

func TestSchemaCarrierSelection(t *testing.T) {
	plain := json.RawMessage(`{"type":"object","properties":{"a":{"type":"string"}}}`)
	if schemaNeedsRichCarrier(plain) {
		t.Fatalf("plain schema must use the tight carrier (parameters)")
	}
	rich := json.RawMessage(`{"type":"object","additionalProperties":false}`)
	if !schemaNeedsRichCarrier(rich) {
		t.Fatalf("additionalProperties must select the rich carrier")
	}
}

func TestSchemaRichCarrierPreserves(t *testing.T) {
	raw := json.RawMessage(`{
		"type": "object",
		"properties": {
			"name": {"type": "string", "minLength": 2, "pattern": "^a"}
		},
		"additionalProperties": false
	}`)
	if !schemaNeedsRichCarrier(raw) {
		t.Fatalf("additionalProperties/standard constraints must select the rich carrier")
	}
	cleaned, err := cleanSchema(raw, true)
	if err != nil {
		t.Fatalf("cleanSchema: %v", err)
	}
	s := string(cleaned)
	for _, want := range []string{`"additionalProperties":false`, `"minLength":2`, `"pattern":"^a"`} {
		if !strings.Contains(s, want) {
			t.Fatalf("rich carrier lost %s: %s", want, s)
		}
	}
}

func TestSchemaAdditionalPropertiesHintInTightCarrier(t *testing.T) {
	raw := json.RawMessage(`{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":false}`)
	cleaned, err := cleanSchema(raw, false)
	if err != nil {
		t.Fatalf("cleanSchema: %v", err)
	}
	s := string(cleaned)
	if strings.Contains(s, "additionalProperties") {
		t.Fatalf("additionalProperties survived tight cleaning: %s", s)
	}
	if !strings.Contains(s, "No extra properties allowed") {
		t.Fatalf("additionalProperties hint missing: %s", s)
	}
}

func TestSchemaCleaningIdempotent(t *testing.T) {
	raw := json.RawMessage(`{
		"$defs": {"C": {"type": "string"}},
		"type": "object",
		"properties": {
			"c": {"$ref": "#/$defs/C", "enum": [1, 2]},
			"u": {"anyOf": [{"type": "null"}, {"type": "string"}]},
			"x": {"type": "object", "properties": {"a": {"type": "string"}}, "allOf": [{"properties": {"b": {"type": "number"}}, "required": ["b"]}]}
		}
	}`)
	once, err := cleanSchema(raw, false)
	if err != nil {
		t.Fatalf("cleanSchema: %v", err)
	}
	twice, err := cleanSchema(once, false)
	if err != nil {
		t.Fatalf("cleanSchema 2nd: %v", err)
	}
	assertJSONEqual(t, once, twice)
}

// ---------------------------------------------------------------------------
// 请求渲染规则
// ---------------------------------------------------------------------------

func TestRenderRequestSystemAndTrailingModel(t *testing.T) {
	req := &ir.Request{
		Messages: []ir.Message{
			{Role: ir.RoleSystem, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "S"}}},
			{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "u"}}},
			{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "trailing prefill"}}},
		},
	}
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	var doc struct {
		SystemInstruction struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"systemInstruction"`
		Contents []struct {
			Role string `json:"role"`
		} `json:"contents"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	if len(doc.SystemInstruction.Parts) != 1 || doc.SystemInstruction.Parts[0].Text != "S" {
		t.Fatalf("systemInstruction = %+v", doc.SystemInstruction)
	}
	// 尾随 model content 丢弃（Gemini 无 prefill）。
	if len(doc.Contents) != 1 || doc.Contents[0].Role != roleUser {
		t.Fatalf("contents = %+v (trailing model content must be dropped)", doc.Contents)
	}
}

func TestRenderRequestMidSystemDemoted(t *testing.T) {
	req := &ir.Request{
		Messages: []ir.Message{
			{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "u"}}},
			{Role: ir.RoleSystem, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "rule"}}},
		},
	}
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	if strings.Contains(string(out), "systemInstruction") {
		t.Fatalf("mid-conversation system must not be hoisted: %s", out)
	}
	if !strings.Contains(string(out), "<system-reminder>\\nrule\\n</system-reminder>") {
		t.Fatalf("mid-conversation system envelope missing: %s", out)
	}
}

func TestRenderRequestToolChoiceFailClosed(t *testing.T) {
	cases := []struct {
		name       string
		choice     string
		extra      map[string]json.RawMessage
		wantMode   string
		wantConfig bool
	}{
		{name: "auto", choice: `"auto"`, wantMode: "AUTO", wantConfig: true},
		{name: "none", choice: `"none"`, wantMode: "NONE", wantConfig: true},
		{name: "required", choice: `"required"`, wantMode: "ANY", wantConfig: true},
		{name: "function_declared", choice: `{"type":"function","function":{"name":"f"}}`, wantMode: "ANY", wantConfig: true},
		{name: "function_undeclared", choice: `{"type":"function","function":{"name":"ghost"}}`, wantMode: "NONE", wantConfig: true},
		{name: "unknown_type", choice: `"whatever"`, wantMode: "NONE", wantConfig: true},
		{name: "absent", choice: ``, wantConfig: false},
		{name: "parallel_disabled", choice: `"auto"`, extra: map[string]json.RawMessage{"parallel_tool_calls": json.RawMessage("false")}, wantMode: "NONE", wantConfig: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := &ir.Request{
				Messages: []ir.Message{{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "u"}}}},
				Tools:    []ir.ToolDef{{Name: "f", InputSchema: json.RawMessage(`{"type":"object"}`)}},
				Extra:    tc.extra,
			}
			if tc.choice != "" {
				req.ToolChoice = json.RawMessage(tc.choice)
			}
			out, err := RenderRequest(req)
			if err != nil {
				t.Fatalf("RenderRequest: %v", err)
			}
			var doc struct {
				ToolConfig *struct {
					FunctionCallingConfig struct {
						Mode                 string   `json:"mode"`
						AllowedFunctionNames []string `json:"allowedFunctionNames"`
					} `json:"functionCallingConfig"`
				} `json:"toolConfig"`
			}
			if err := json.Unmarshal(out, &doc); err != nil {
				t.Fatalf("unmarshal: %v\n%s", err, out)
			}
			if !tc.wantConfig {
				if doc.ToolConfig != nil {
					t.Fatalf("toolConfig must be absent: %s", out)
				}
				return
			}
			if doc.ToolConfig == nil || doc.ToolConfig.FunctionCallingConfig.Mode != tc.wantMode {
				t.Fatalf("toolConfig = %+v, want mode %s", doc.ToolConfig, tc.wantMode)
			}
			if tc.name == "function_declared" && len(doc.ToolConfig.FunctionCallingConfig.AllowedFunctionNames) != 1 {
				t.Fatalf("allowedFunctionNames = %+v", doc.ToolConfig.FunctionCallingConfig.AllowedFunctionNames)
			}
		})
	}
}

func TestRenderRequestGenerationConfig(t *testing.T) {
	req := &ir.Request{
		Messages: []ir.Message{{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "u"}}}},
		Extra: map[string]json.RawMessage{
			"max_tokens":       json.RawMessage("100"),
			"temperature":      json.RawMessage("0.3"),
			"stop":             json.RawMessage(`["a","b","c","d","e","f"]`),
			"n":                json.RawMessage("2"),
			"reasoning_effort": json.RawMessage(`"high"`),
			"response_format":  json.RawMessage(`{"type":"json_schema","json_schema":{"schema":{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":false}}}`),
		},
	}
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	var doc struct {
		GenerationConfig struct {
			MaxOutputTokens    int      `json:"maxOutputTokens"`
			Temperature        float64  `json:"temperature"`
			StopSequences      []string `json:"stopSequences"`
			CandidateCount     int      `json:"candidateCount"`
			ResponseMimeType   string   `json:"responseMimeType"`
			ResponseJSONSchema any      `json:"responseJsonSchema"`
			ResponseSchema     any      `json:"responseSchema"`
			ThinkingConfig     struct {
				ThinkingBudget int `json:"thinkingBudget"`
			} `json:"thinkingConfig"`
		} `json:"generationConfig"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	gc := doc.GenerationConfig
	if gc.MaxOutputTokens != 100 || gc.Temperature != 0.3 {
		t.Fatalf("generationConfig = %+v", gc)
	}
	if len(gc.StopSequences) != 5 {
		t.Fatalf("stopSequences = %+v (must truncate to 5)", gc.StopSequences)
	}
	if gc.CandidateCount != 2 {
		t.Fatalf("candidateCount = %d", gc.CandidateCount)
	}
	if gc.ResponseMimeType != "application/json" || gc.ResponseJSONSchema == nil || gc.ResponseSchema != nil {
		t.Fatalf("response_format mapping = %+v", gc)
	}
	if gc.ThinkingConfig.ThinkingBudget != 24576 {
		t.Fatalf("thinkingBudget = %d, want 24576 (high)", gc.ThinkingConfig.ThinkingBudget)
	}
}

func TestRenderRequestStreamWithNCandidateRejected(t *testing.T) {
	req := &ir.Request{
		Stream:   true,
		Messages: []ir.Message{{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "u"}}}},
		Extra:    map[string]json.RawMessage{"n": json.RawMessage("2")},
	}
	if _, err := RenderRequest(req); !errors.Is(err, ErrStreamCandidateCount) {
		t.Fatalf("err = %v, want ErrStreamCandidateCount", err)
	}
}

func TestRenderRequestSafetyDefaults(t *testing.T) {
	req := &ir.Request{
		Messages: []ir.Message{{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "u"}}}},
	}
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	if !strings.Contains(string(out), "HARM_CATEGORY_HARASSMENT") || !strings.Contains(string(out), "BLOCK_NONE") {
		t.Fatalf("default safetySettings missing: %s", out)
	}
	// gemini 侧原文优先（保真路径）。
	req.Extra = map[string]json.RawMessage{"safetySettings": json.RawMessage(`[{"category":"HARM_CATEGORY_HARASSMENT","threshold":"BLOCK_ONLY_HIGH"}]`)}
	out, err = RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	if strings.Contains(string(out), "BLOCK_NONE") || !strings.Contains(string(out), "BLOCK_ONLY_HIGH") {
		t.Fatalf("explicit safetySettings must win: %s", out)
	}
}

func TestRenderRequestToolArgsMustBeObject(t *testing.T) {
	req := &ir.Request{
		Messages: []ir.Message{{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{{
			Kind:     ir.BlockToolUse,
			ToolCall: &ir.ToolCall{Name: "f", Arguments: `[1,2]`},
		}}}},
	}
	if _, err := RenderRequest(req); !errors.Is(err, ErrInvalidToolArgs) {
		t.Fatalf("err = %v, want ErrInvalidToolArgs", err)
	}
}

func TestRenderRequestStrictIgnored(t *testing.T) {
	strict := true
	req := &ir.Request{
		Messages: []ir.Message{{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "u"}}}},
		Tools:    []ir.ToolDef{{Name: "f", Strict: &strict, InputSchema: json.RawMessage(`{"type":"object"}`)}},
	}
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	if strings.Contains(string(out), "strict") || strings.Contains(string(out), "VALIDATED") {
		t.Fatalf("strict must be ignored (VALIDATED 未核): %s", out)
	}
}

// ---------------------------------------------------------------------------
// 响应渲染
// ---------------------------------------------------------------------------

func TestRenderResponseFinishReasons(t *testing.T) {
	cases := []struct {
		reason ir.StopReason
		want   string
	}{
		{ir.StopStop, "STOP"},
		{ir.StopLength, "MAX_TOKENS"},
		{ir.StopToolUse, "STOP"},
		{ir.StopError, "SAFETY"},
	}
	for _, tc := range cases {
		out, err := RenderResponse(&ir.Response{
			Message:    ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "x"}}},
			StopReason: tc.reason,
		})
		if err != nil {
			t.Fatalf("RenderResponse(%s): %v", tc.reason, err)
		}
		if !strings.Contains(string(out), `"finishReason":"`+tc.want+`"`) {
			t.Fatalf("%s → %s", tc.reason, out)
		}
	}
}

func TestRenderUsageReverse(t *testing.T) {
	u := &ir.Usage{Input: 10, Output: 7, Total: 17, Reasoning: 3, CacheRead: 4, Accounting: "openai"}
	got := string(renderUsageMetadata(u))
	for _, want := range []string{
		`"promptTokenCount":10`, `"candidatesTokenCount":4`, `"thoughtsTokenCount":3`,
		`"cachedContentTokenCount":4`, `"totalTokenCount":17`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("usageMetadata missing %s: %s", want, got)
		}
	}
}

// ---------------------------------------------------------------------------
// 错误信封（IR → 线格式）
// ---------------------------------------------------------------------------

func TestRenderErrorEnvelope(t *testing.T) {
	out, err := RenderError(&ir.Error{Type: "rate_limit_error", Code: "429", Message: "slow down", Source: "gemini"})
	if err != nil {
		t.Fatalf("RenderError: %v", err)
	}
	var doc struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	if doc.Error.Code != 429 || doc.Error.Status != "RESOURCE_EXHAUSTED" || doc.Error.Message != "slow down" {
		t.Fatalf("envelope = %+v", doc.Error)
	}
	// 非数值 code → 按 status 缺省 HTTP 码。
	out, err = RenderError(&ir.Error{Type: "invalid_request_error", Message: "bad"})
	if err != nil {
		t.Fatalf("RenderError: %v", err)
	}
	if !strings.Contains(string(out), `"status":"INVALID_ARGUMENT"`) || !strings.Contains(string(out), `"code":400`) {
		t.Fatalf("envelope = %s", out)
	}
}

func TestRenderErrorNeverLeaksRaw(t *testing.T) {
	out, err := RenderError(&ir.Error{
		Type: "server_error", Message: "upstream failed", Source: "gemini",
		Raw: json.RawMessage(`{"internal":"secret-key-abc","details":[1,2]}`),
	})
	if err != nil {
		t.Fatalf("RenderError: %v", err)
	}
	if strings.Contains(string(out), "secret-key-abc") || strings.Contains(string(out), "details") {
		t.Fatalf("Error.Raw leaked: %s", out)
	}
}

// ---------------------------------------------------------------------------
// 流式渲染
// ---------------------------------------------------------------------------

func TestRenderStreamNoSentinelAndTerminalOnly(t *testing.T) {
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "r1", Model: "m"},
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockText},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockText, TextDelta: "hi"},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockText},
		{Kind: ir.EventMessageDelta, StopReason: ir.StopStop},
		{Kind: ir.EventMessageDelta, Usage: &ir.Usage{Input: 1, Output: 2, Total: 3, Accounting: "gemini"}},
		{Kind: ir.EventMessageStop},
		{Kind: ir.EventDone, Reason: ir.StopStop},
	}
	out, err := RenderStream(events)
	if err != nil {
		t.Fatalf("RenderStream: %v", err)
	}
	s := string(out)
	if strings.Contains(s, "[DONE]") {
		t.Fatalf("gemini stream must not carry a sentinel: %s", s)
	}
	if !strings.HasSuffix(s, "\n\n") {
		t.Fatalf("stream must end with a dispatched frame: %q", s)
	}
	frames := sseFrames(t, out)
	if len(frames) != 2 {
		t.Fatalf("frames = %d, want 2 (text + terminal): %s", len(frames), s)
	}
	if !strings.Contains(frames[0], `"text":"hi"`) {
		t.Fatalf("frame0 = %s", frames[0])
	}
	last := frames[len(frames)-1]
	if !strings.Contains(last, `"finishReason":"STOP"`) || !strings.Contains(last, `"usageMetadata"`) {
		t.Fatalf("terminal frame = %s", last)
	}
	// 终态后事件报错。
	if _, err := NewStreamWriter().Write(ir.StreamEvent{Kind: ir.EventBlockDelta}); !errors.Is(err, ErrMissingStart) {
		t.Fatalf("err = %v, want ErrMissingStart", err)
	}
}

func TestRenderStreamMissingStopReason(t *testing.T) {
	w := NewStreamWriter()
	if _, err := w.Write(ir.StreamEvent{Kind: ir.EventStart}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := w.Finish(); !errors.Is(err, ErrMissingStopReason) {
		t.Fatalf("err = %v, want ErrMissingStopReason", err)
	}
}

func TestRenderStreamErrorFrameTerminal(t *testing.T) {
	w := NewStreamWriter()
	if _, err := w.Write(ir.StreamEvent{Kind: ir.EventStart, MessageID: "r"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	b, err := w.Write(ir.StreamEvent{Kind: ir.EventError, ErrorReason: ir.ErrorUpstream,
		Error: &ir.Error{Type: "rate_limit_error", Message: "slow"}})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !strings.Contains(string(b), "RESOURCE_EXHAUSTED") {
		t.Fatalf("error frame = %s", b)
	}
	// 错误即终止：不补终块、不补哨兵。
	rest, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("Finish after error = %q", rest)
	}
	if _, err := w.Write(ir.StreamEvent{Kind: ir.EventMessageStop}); !errors.Is(err, ErrEventsAfterTerminal) {
		t.Fatalf("err = %v, want ErrEventsAfterTerminal", err)
	}
}
