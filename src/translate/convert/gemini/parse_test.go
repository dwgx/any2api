package gemini

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// ---------------------------------------------------------------------------
// finishReason 全枚举（§5.1 22 值）
// ---------------------------------------------------------------------------

func TestMapFinishReason(t *testing.T) {
	cases := []struct {
		in   string
		want ir.StopReason
	}{
		{"STOP", ir.StopStop},
		{"MAX_TOKENS", ir.StopLength},
		{"CONTINUATION", ir.StopLength},
		{"SAFETY", ir.StopError},
		{"RECITATION", ir.StopError},
		{"BLOCKLIST", ir.StopError},
		{"PROHIBITED_CONTENT", ir.StopError},
		{"SPII", ir.StopError},
		{"IMAGE_SAFETY", ir.StopError},
		{"IMAGE_PROHIBITED_CONTENT", ir.StopError},
		{"IMAGE_RECITATION", ir.StopError},
		{"IMAGE_OTHER", ir.StopError},
		{"LANGUAGE", ir.StopError},
		{"OTHER", ir.StopError},
		{"MALFORMED_FUNCTION_CALL", ir.StopError},
		{"UNEXPECTED_TOOL_CALL", ir.StopError},
		{"TOO_MANY_TOOL_CALLS", ir.StopError},
		{"MISSING_THOUGHT_SIGNATURE", ir.StopError},
		{"MALFORMED_RESPONSE", ir.StopError},
		// 不误判为拦截（NA default 落 content_filter 不照抄）。
		{"NO_IMAGE", ir.StopStop},
		{"ESCALATION", ir.StopStop},
		{"PUP_LIMITED_DISABLED", ir.StopStop},
	}
	for _, tc := range cases {
		if got := mapFinishReason(tc.in); got != tc.want {
			t.Fatalf("mapFinishReason(%s) = %q, want %q", tc.in, got, tc.want)
		}
	}
	// 未知值原样透传（解析层不猜测）。
	if got := mapFinishReason("SOMETHING_NEW"); got != ir.StopReason("SOMETHING_NEW") {
		t.Fatalf("unknown value = %q", got)
	}
}

// ---------------------------------------------------------------------------
// 错误信封（线格式 → IR）
// ---------------------------------------------------------------------------

func TestParseErrorEnvelope(t *testing.T) {
	cases := []struct {
		status    string
		wantType  string
		retryable *bool
	}{
		{"INVALID_ARGUMENT", "invalid_request_error", nil},
		{"FAILED_PRECONDITION", "invalid_request_error", nil},
		{"UNAUTHENTICATED", "invalid_api_key", nil},
		{"PERMISSION_DENIED", "permission_error", nil},
		{"NOT_FOUND", "not_found_error", nil},
		{"RESOURCE_EXHAUSTED", "rate_limit_error", new(true)},
		{"UNAVAILABLE", "server_error", new(true)},
		{"DEADLINE_EXCEEDED", "server_error", new(true)},
		{"INTERNAL", "server_error", nil},
		{"CANCELLED", "api_error", nil},
		{"INVALID_REQUEST", "invalid_request_error", nil}, // Interactions snake_case 形态
		{"WEIRD_STATUS", "server_error", nil},
	}
	for _, tc := range cases {
		body := []byte(`{"error":{"code":400,"message":"m","status":"` + tc.status + `"}}`)
		e := ParseError(body)
		if e.Type != tc.wantType {
			t.Fatalf("%s → type %q, want %q", tc.status, e.Type, tc.wantType)
		}
		if (e.Retryable == nil) != (tc.retryable == nil) || (e.Retryable != nil && *e.Retryable != *tc.retryable) {
			t.Fatalf("%s → retryable %v", tc.status, e.Retryable)
		}
		if e.Code != "400" || e.Message != "m" || e.Source != "gemini" {
			t.Fatalf("%s → %+v", tc.status, e)
		}
	}
	// 非 JSON body：归一为泛型 api_error，Raw 保真。
	e := ParseError([]byte("<html>502</html>"))
	if e.Type != "api_error" || string(e.Raw) != "<html>502</html>" {
		t.Fatalf("non-JSON body → %+v", e)
	}
}

// ---------------------------------------------------------------------------
// 请求解析
// ---------------------------------------------------------------------------

func TestParseRequestContentsAndRoles(t *testing.T) {
	req, err := ParseRequest([]byte(`{
		"contents": [
			{"role": "user", "parts": [{"text": "u1"}]},
			{"role": "model", "parts": [{"text": "a1"}]},
			{"role": "function", "parts": [{"functionResponse": {"name": "f", "response": {"ok": true}}}]}
		]
	}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if len(req.Messages) != 3 {
		t.Fatalf("messages = %+v", req.Messages)
	}
	if req.Messages[1].Role != ir.RoleAssistant {
		t.Fatalf("model → %q", req.Messages[1].Role)
	}
	// role 二义（user/function）不依赖单一取值：functionResponse 轮 → RoleTool。
	tool := req.Messages[2]
	if tool.Role != ir.RoleTool || tool.Name != "f" || len(tool.Blocks) != 1 || tool.Blocks[0].ToolResult.Content != `{"ok": true}` {
		t.Fatalf("tool message = %+v", tool)
	}
}

func TestParseRequestStreamPreserved(t *testing.T) {
	// P53 quirk ④：`stream`（非官方扩展位；真开关在路径）此前被静默丢弃、恒 false。
	req, err := ParseRequest([]byte(`{
		"contents": [{"role": "user", "parts": [{"text": "u"}]}],
		"stream": true
	}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if !req.Stream {
		t.Fatalf("declared stream=true must be preserved into IR (got false)")
	}
	// 未声明 = false（绝不发明）。
	req2, err := ParseRequest([]byte(`{"contents": [{"role": "user", "parts": [{"text": "u"}]}]}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if req2.Stream {
		t.Fatalf("absent stream must stay false")
	}
	// 显式 false 原样。
	req3, err := ParseRequest([]byte(`{"contents": [{"role": "user", "parts": [{"text": "u"}]}], "stream": false}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if req3.Stream {
		t.Fatalf("explicit stream=false must stay false")
	}
}

func TestParseRequestMultipleFunctionResponsesSplit(t *testing.T) {
	req, err := ParseRequest([]byte(`{
		"contents": [
			{"role": "model", "parts": [{"functionCall": {"name": "a", "args": {}}}, {"functionCall": {"name": "b", "args": {}}}]},
			{"role": "user", "parts": [
				{"functionResponse": {"name": "a", "response": {"result": 1}}},
				{"functionResponse": {"name": "b", "response": {"result": 2}}}
			]}
		]
	}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if len(req.Messages) != 3 {
		t.Fatalf("messages = %+v", req.Messages)
	}
	for i, m := range req.Messages[1:] {
		if m.Role != ir.RoleTool || len(m.Blocks) != 1 {
			t.Fatalf("tool message %d = %+v", i, m)
		}
	}
}

func TestParseRequestGenerationConfigCanonical(t *testing.T) {
	req, err := ParseRequest([]byte(`{
		"contents": [{"role": "user", "parts": [{"text": "u"}]}],
		"generationConfig": {
			"maxOutputTokens": 512,
			"temperature": 0.2,
			"topP": 0.8,
			"topK": 40,
			"stopSequences": ["x"],
			"candidateCount": 1,
			"seed": 7,
			"presencePenalty": 0.1,
			"frequencyPenalty": 0.2,
			"responseSchema": {"type": "object"},
			"thinkingConfig": {"thinkingBudget": 1024},
			"mediaResolution": "MEDIA_RESOLUTION_LOW"
		}
	}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	want := map[string]string{
		"max_tokens": "512", "temperature": "0.2", "top_p": "0.8", "top_k": "40",
		"stop": `["x"]`, "n": "1", "seed": "7", "presence_penalty": "0.1", "frequency_penalty": "0.2",
	}
	for k, v := range want {
		if got := string(req.Extra[k]); got != v {
			t.Fatalf("Extra[%q] = %s, want %s", k, got, v)
		}
	}
	// responseSchema deprecated → 读作 responseJsonSchema（渲染只发新字段）。
	if _, ok := req.Extra["responseJsonSchema"]; !ok {
		t.Fatalf("responseSchema not migrated: %+v", req.Extra)
	}
	// thinkingConfig 保真（gemini 侧原文）。
	if _, ok := req.Extra["thinkingConfig"]; !ok {
		t.Fatalf("thinkingConfig missing: %+v", req.Extra)
	}
	// 未识别子字段丢弃（§quirk 20：透传=上游 400 风险）。
	if _, ok := req.Extra["mediaResolution"]; ok {
		t.Fatalf("unknown generationConfig field must be dropped: %+v", req.Extra)
	}
	if _, ok := req.Extra["cachedContent"]; ok {
		t.Fatalf("cachedContent must be dropped")
	}
}

func TestParseRequestToolConfig(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"auto", `{"functionCallingConfig":{"mode":"AUTO"}}`, `"auto"`},
		{"none", `{"functionCallingConfig":{"mode":"NONE"}}`, `"none"`},
		{"any", `{"functionCallingConfig":{"mode":"ANY"}}`, `"required"`},
		{"validated", `{"functionCallingConfig":{"mode":"VALIDATED"}}`, `"auto"`},
		{"unknown", `{"functionCallingConfig":{"mode":"FUTURE_MODE"}}`, `"none"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := ParseRequest([]byte(`{"contents":[{"role":"user","parts":[{"text":"u"}]}],"toolConfig":` + tc.in + `}`))
			if err != nil {
				t.Fatalf("ParseRequest: %v", err)
			}
			if string(req.ToolChoice) != tc.want {
				t.Fatalf("tool_choice = %s, want %s", req.ToolChoice, tc.want)
			}
		})
	}
	// ANY + allowedFunctionNames → 规范形 {function:{name}}。
	req, err := ParseRequest([]byte(`{"contents":[{"role":"user","parts":[{"text":"u"}]}],"toolConfig":{"functionCallingConfig":{"mode":"ANY","allowedFunctionNames":["get_weather"]}}}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	var tc struct {
		Type     string `json:"type"`
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	}
	if err := json.Unmarshal(req.ToolChoice, &tc); err != nil || tc.Type != "function" || tc.Function.Name != "get_weather" {
		t.Fatalf("tool_choice = %s (%v)", req.ToolChoice, err)
	}
}

func TestParseRequestToolDeclarations(t *testing.T) {
	req, err := ParseRequest([]byte(`{
		"contents": [{"role": "user", "parts": [{"text": "u"}]}],
		"tools": [
			{"googleSearch": {}},
			{"functionDeclarations": [
				{"name": "a", "description": "d", "parametersJsonSchema": {"type": "object", "additionalProperties": false}},
				{"name": "b", "parameters": {"type": "object"}}
			]}
		]
	}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	// 内置工具丢弃；两个 functionDeclaration 保留（载体各自读取）。
	if len(req.Tools) != 2 || req.Tools[0].Name != "a" || req.Tools[1].Name != "b" {
		t.Fatalf("tools = %+v", req.Tools)
	}
	if !strings.Contains(string(req.Tools[0].InputSchema), "additionalProperties") {
		t.Fatalf("parametersJsonSchema carrier not read: %s", req.Tools[0].InputSchema)
	}
}

func TestParseRequestRequiresContents(t *testing.T) {
	if _, err := ParseRequest([]byte(`{}`)); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("err = %v, want ErrBadRequest", err)
	}
}

// ---------------------------------------------------------------------------
// 响应解析
// ---------------------------------------------------------------------------

func TestParseResponseMultiCandidateRejected(t *testing.T) {
	body := []byte(`{"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"a"}]}},{"index":1,"content":{"role":"model","parts":[{"text":"b"}]}}]}`)
	if _, err := ParseResponse(body); !errors.Is(err, ErrUnsupportedMultiCandidate) {
		t.Fatalf("err = %v, want ErrUnsupportedMultiCandidate", err)
	}
}

func TestParseResponseNoCandidatesWithoutBlockReason(t *testing.T) {
	if _, err := ParseResponse([]byte(`{"candidates":[]}`)); !errors.Is(err, ErrBadResponse) {
		t.Fatalf("err = %v, want ErrBadResponse", err)
	}
}

func TestParseResponseThoughtSignatureCarriage(t *testing.T) {
	body := []byte(`{
		"candidates": [{"index": 0, "content": {"role": "model", "parts": [
			{"functionCall": {"name": "f", "args": {}}, "thoughtSignature": "sig-tool"},
			{"thoughtSignature": "sig-extra"}
		]}, "finishReason": "STOP"}]
	}`)
	resp, err := ParseResponse(body)
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if len(resp.Message.Blocks) != 1 {
		t.Fatalf("blocks = %+v (signature-only part must fold into the tool block)", resp.Message.Blocks)
	}
	if resp.Message.Blocks[0].Signature != "sig-tool" {
		t.Fatalf("tool signature = %q", resp.Message.Blocks[0].Signature)
	}
}

// ---------------------------------------------------------------------------
// 流式解析
// ---------------------------------------------------------------------------

func TestStreamParserTruncatedStreamIsFailure(t *testing.T) {
	in := "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"x\"}]},\"index\":0}]}\n\n"
	events, err := ParseStream(strings.NewReader(in))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	last := events[len(events)-1]
	if last.Kind != ir.EventError || last.ErrorReason != ir.ErrorTransport {
		t.Fatalf("truncated stream must fail: %+v", events)
	}
}

func TestStreamParserUnclosedPartialIsFailure(t *testing.T) {
	in := "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"c1\",\"name\":\"f\",\"partialArgs\":[{\"jsonPath\":\"$.a\",\"stringValue\":\"x\"}],\"willContinue\":true}}]},\"index\":0}]}\n\n"
	events, err := ParseStream(strings.NewReader(in))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	last := events[len(events)-1]
	if last.Kind != ir.EventError || !strings.Contains(last.Error.Message, "incomplete function call") {
		t.Fatalf("unclosed partial must fail (不得当空参执行): %+v", events)
	}
}

func TestStreamParserPartialPathRejected(t *testing.T) {
	in := "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"c1\",\"name\":\"f\",\"partialArgs\":[{\"jsonPath\":\"$.a[*]\",\"stringValue\":\"x\"}],\"willContinue\":false}}]},\"index\":0}]}\n\n"
	if _, err := ParseStream(strings.NewReader(in)); !errors.Is(err, ErrBadPartialArgsPath) {
		t.Fatalf("err = %v, want ErrBadPartialArgsPath", err)
	}
	// 根路径 $ 单独非法。
	if _, err := parsePartialArgPath("$"); !errors.Is(err, ErrBadPartialArgsPath) {
		t.Fatalf("root path err = %v", err)
	}
	// 数组下标上限（4095）。
	if _, err := parsePartialArgPath("$.a[4096]"); !errors.Is(err, ErrPartialArgsLimit) {
		t.Fatalf("limit err = %v", err)
	}
	if _, err := parsePartialArgPath("$.a[4095]"); err != nil {
		t.Fatalf("boundary path err = %v", err)
	}
}

func TestStreamParserPromptBlockReason(t *testing.T) {
	in := "data: {\"candidates\":[],\"promptFeedback\":{\"blockReason\":\"PROHIBITED_CONTENT\"}}\n\n"
	events, err := ParseStream(strings.NewReader(in))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	if r, ok := doneReason(events); !ok || r != ir.StopError {
		t.Fatalf("done reason = %q (ok=%v), want error (content_filter 语义)", r, ok)
	}
}

func TestStreamParserBracketBalancedErrorLine(t *testing.T) {
	// 非 data: 前缀的 JSON 行跨多行（括号平衡聚合）。
	in := "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"x\"}]},\"index\":0}]}\n\n" +
		"{\"error\":\n{\"code\":503,\n\"message\":\"upstream busy\",\n\"status\":\"UNAVAILABLE\"}}\n"
	events, err := ParseStream(strings.NewReader(in))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	last := events[len(events)-1]
	if last.Kind != ir.EventError || last.Error.Code != "503" || last.Error.Type != "server_error" {
		t.Fatalf("error line not parsed: %+v", events)
	}
}

func TestStreamParserDoneSentinelIgnored(t *testing.T) {
	in := "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"x\"}]},\"finishReason\":\"STOP\",\"index\":0}]}\n\n" +
		"data: [DONE]\n\n"
	events, err := ParseStream(strings.NewReader(in))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	if r, ok := doneReason(events); !ok || r != ir.StopStop {
		t.Fatalf("done reason = %q (ok=%v)", r, ok)
	}
}

func TestStreamParserUsageMergeNonZero(t *testing.T) {
	// 末帧缺字段 → merge-non-zero 保留前值（不取末帧整份覆盖、不逐帧累加）。
	in := "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"a\"}]},\"index\":0}],\"usageMetadata\":{\"promptTokenCount\":10,\"candidatesTokenCount\":4,\"totalTokenCount\":14}}\n\n" +
		"data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[]},\"finishReason\":\"STOP\",\"index\":0}],\"usageMetadata\":{\"totalTokenCount\":14}}\n\n"
	events, err := ParseStream(strings.NewReader(in))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	u := lastUsage(events)
	if u == nil || u.Input != 10 || u.Output != 4 || u.Total != 14 {
		t.Fatalf("usage = %+v (merge-non-zero broken)", u)
	}
}

func TestStreamParserMultiCandidateRejected(t *testing.T) {
	in := "data: {\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"a\"}]}},{\"index\":1,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"b\"}]}}]}\n\n"
	if _, err := ParseStream(strings.NewReader(in)); !errors.Is(err, ErrUnsupportedMultiCandidate) {
		t.Fatalf("err = %v, want ErrUnsupportedMultiCandidate", err)
	}
}

func TestStreamParserToolCallReemitSkipped(t *testing.T) {
	// 同一 call id 复现（帧内 index 重启）→ 槽位复用且不重发（避免 OpenAI 追加双倍参数）。
	p := NewStreamParser()
	frame := []byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"id":"c1","name":"f","args":{"a":1}}}]},"index":0}]}`)
	evs, err := p.Feed(frame)
	if err != nil {
		t.Fatalf("feed 1: %v", err)
	}
	if got := len(toolStarts(evs)); got != 1 {
		t.Fatalf("first feed tool blocks = %d", got)
	}
	evs, err = p.Feed(frame)
	if err != nil {
		t.Fatalf("feed 2: %v", err)
	}
	if got := len(toolStarts(evs)); got != 0 {
		t.Fatalf("duplicate call id must be skipped, got %d blocks", got)
	}
}

func TestStreamParserEmptyStream(t *testing.T) {
	events, err := ParseStream(bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	if len(events) != 1 || events[0].Kind != ir.EventError || events[0].ErrorReason != ir.ErrorTransport {
		t.Fatalf("empty stream = %+v", events)
	}
}
