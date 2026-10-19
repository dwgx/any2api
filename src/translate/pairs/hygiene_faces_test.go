package pairs

import (
	"encoding/json"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// 消息级 / 工具定义级 / 响应面 Extra 卫生补口（P53 缺口 18 + 对齐说明 ①②③）：
// claude 出口此前只对**顶层请求/响应**建过滤表——消息级 Extra（CMr:292,316）、
// 工具定义级 Extra（CMr:480-481）直接 mergeExtras；响应面过滤表只列了各自点名
// 的键。本文件逐面回归：来源独有键绝不出现在目标体，跨格式键保留（防过滤过度）。
// ---------------------------------------------------------------------------

// TestClaudeMessageLevelExtrasHygiene 消息级：openai 来源的消息级未建模键
// （refusal/function_call/audio/reasoning_*）不得进 claude 消息对象；请求面与
// 响应面（同一渲染路径）两方向都验。
func TestClaudeMessageLevelExtrasHygiene(t *testing.T) {
	reg := NewRegistry()
	forbid := []string{"refusal", "function_call", "audio", "reasoning_content", "reasoning_details"}

	openaiReq := []byte(`{
		"model": "m",
		"messages": [
			{"role": "user", "content": "hi", "x_keep": {"a": 1}},
			{"role": "assistant", "content": "a", "refusal": null,
			 "function_call": {"name": "f", "arguments": "{}"},
			 "audio": {"id": "a1", "data": "b64", "expires_at": 1, "transcript": "t"},
			 "reasoning_content": "r", "reasoning_details": [{"type": "x"}]}
		]
	}`)
	out, err := Convert(reg, fOpenAI, fClaude, FaceRequest, openaiReq, Options{})
	if err != nil {
		t.Fatalf("request convert: %v", err)
	}
	for _, k := range forbid {
		if containsJSONKey(t, out, k) {
			t.Fatalf("message-level foreign key %q leaked into claude request: %s", k, out)
		}
	}
	if !containsJSONKey(t, out, "x_keep") {
		t.Fatalf("benign message-level key lost (over-filtering): %s", out)
	}

	openaiResp := []byte(`{
		"id": "c1", "object": "chat.completion", "created": 1, "model": "m",
		"choices": [{"index": 0, "finish_reason": "stop",
			"message": {"role": "assistant", "content": "hi", "refusal": null,
				"reasoning_content": "r", "x_keep": 1}}],
		"usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
	}`)
	out, err = Convert(reg, fOpenAI, fClaude, FaceResponse, openaiResp, Options{})
	if err != nil {
		t.Fatalf("response convert: %v", err)
	}
	for _, k := range forbid {
		if containsJSONKey(t, out, k) {
			t.Fatalf("message-level foreign key %q leaked into claude response: %s", k, out)
		}
	}
	// claude 响应面没有消息对象（content 平铺到顶层）——消息级 Extra 整体不参与
	// 渲染（响应面天然干净）；只确认正文/终止语义在位。
	if !containsJSONKey(t, out, "content") || !containsJSONKey(t, out, "stop_reason") {
		t.Fatalf("response body shape broken: %s", out)
	}
}

// TestClaudeToolLevelExtrasHygiene 工具定义级：gemini decl 级未建模字段
// （response / responseJsonSchema，官方字段面）不得进 claude 工具对象——直转
// （openai 来源带同名键）与枢纽两步（gemini→openai→claude）都验。
func TestClaudeToolLevelExtrasHygiene(t *testing.T) {
	reg := NewRegistry()
	toolKeys := []string{"response", "responseJsonSchema"}

	geminiReq := []byte(`{
		"contents": [{"role": "user", "parts": [{"text": "hi"}]}],
		"tools": [{"functionDeclarations": [{
			"name": "f", "description": "d",
			"parameters": {"type": "object", "properties": {"a": {"type": "string"}}},
			"response": {"type": "object"},
			"responseJsonSchema": {"type": "object"}
		}]}]
	}`)
	out, err := Convert(reg, fGemini, fClaude, FaceRequest, geminiReq, Options{})
	if err != nil {
		t.Fatalf("gemini→claude (hub): %v", err)
	}
	for _, k := range toolKeys {
		if containsJSONKey(t, out, k) {
			t.Fatalf("tool-level foreign key %q leaked into claude tools (hub): %s", k, out)
		}
	}
	if !containsJSONKey(t, out, "input_schema") {
		t.Fatalf("input_schema lost: %s", out)
	}

	openaiReq := []byte(`{
		"model": "m",
		"messages": [{"role": "user", "content": "hi"}],
		"tools": [{"type": "function", "function": {"name": "f",
			"parameters": {"type": "object"},
			"response": {"type": "object"},
			"responseJsonSchema": {"type": "object"}}}]
	}`)
	out, err = Convert(reg, fOpenAI, fClaude, FaceRequest, openaiReq, Options{})
	if err != nil {
		t.Fatalf("openai→claude: %v", err)
	}
	for _, k := range toolKeys {
		if containsJSONKey(t, out, k) {
			t.Fatalf("tool-level foreign key %q leaked into claude tools: %s", k, out)
		}
	}
}

// TestClaudeResponseTableCompleted 响应面过滤表补全：openai 兼容面的响应独有键
// （system_fingerprint / service_tier + xAI 顶层 citations / output_files）不得
// 出现在 claude 响应体（W5-8 缺口 2 / P53 缺口 18 的点名形态）。
func TestClaudeResponseTableCompleted(t *testing.T) {
	reg := NewRegistry()
	openaiResp := []byte(`{
		"id": "c1", "object": "chat.completion", "created": 1, "model": "m",
		"choices": [{"index": 0, "finish_reason": "stop",
			"message": {"role": "assistant", "content": "hi"}}],
		"usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		"system_fingerprint": "fp_1", "service_tier": "default",
		"citations": ["https://example.invalid"],
		"output_files": [{"id": "file_1"}]
	}`)
	out, err := Convert(reg, fOpenAI, fClaude, FaceResponse, openaiResp, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	for _, k := range []string{"system_fingerprint", "service_tier", "citations", "output_files"} {
		if containsJSONKey(t, out, k) {
			t.Fatalf("openai response-only key %q leaked into claude response: %s", k, out)
		}
	}
	if !containsJSONKey(t, out, "stop_reason") {
		t.Fatalf("stop_reason lost: %s", out)
	}
}

// TestClaudeDropsBuddyTimingExtras 响应面来源独有计时键：upstream-a /
// upstream-private 上游在 OpenAI 兼容响应顶层附加私有计时字段 elapsed_ms /
// first_chunk_at（W83-1 X1 实测 2026-10-19；改动前两键在本仓 .go 文件中零命中，
// 说明无解析/渲染路径建模，只能经 openai_chat 的 Response.Extra 保真旁路进入
// claude 响应体）。claude 响应体不认识这两键 → 必须剔除；同时通用未知键
// （vendor_trace）仍按保真策略透传，防止过滤过度（本表只列来源侧独有键，
// 不做通用白名单）。
func TestClaudeDropsBuddyTimingExtras(t *testing.T) {
	reg := NewRegistry()
	openaiResp := []byte(`{
		"id": "chatcmpl-buddy-1", "object": "chat.completion", "created": 1,
		"model": "deepseek-chat",
		"choices": [{"index": 0, "finish_reason": "stop",
			"message": {"role": "assistant", "content": "hi"}}],
		"usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
		"elapsed_ms": 1234,
		"first_chunk_at": 1760000000.5,
		"vendor_trace": {"hop": "buddy"}
	}`)
	out, err := Convert(reg, fOpenAI, fClaude, FaceResponse, openaiResp, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	for _, k := range []string{"elapsed_ms", "first_chunk_at"} {
		if containsJSONKey(t, out, k) {
			t.Fatalf("buddy-private timing key %q leaked into claude response: %s", k, out)
		}
	}
	if !containsJSONKey(t, out, "vendor_trace") {
		t.Fatalf("generic unknown key vendor_trace lost (over-filtering): %s", out)
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(out, &body); err != nil {
		t.Fatalf("claude response is not a JSON object: %v\n%s", err, out)
	}
	if got := string(body["id"]); got != `"chatcmpl-buddy-1"` {
		t.Fatalf("id not preserved: %s", got)
	}
	if got := string(body["model"]); got != `"deepseek-chat"` {
		t.Fatalf("model not preserved: %s", got)
	}
	if got := string(body["content"]); !strings.Contains(got, `"hi"`) {
		t.Fatalf("content text not preserved: %s", got)
	}
	var usage map[string]json.RawMessage
	if err := json.Unmarshal(body["usage"], &usage); err != nil {
		t.Fatalf("usage not an object: %v", err)
	}
	if string(usage["input_tokens"]) != "3" || string(usage["output_tokens"]) != "2" {
		t.Fatalf("usage not preserved: %s", body["usage"])
	}
}

// TestOpenAIDropsBuddyTimingExtras 响应面来源独有计时键（OpenAI Chat 出口）：
// upstream-a / upstream-private 私有计时字段 elapsed_ms / first_chunk_at 不得进入
// openai_chat 响应体；通用未知键（vendor_trace）仍按保真策略透传。
//
// 同格式 openai→openai 是服务壳字节直通，registry 对 from==to 显式报
// ErrNoConversion（convert_test.go:53），不经过本渲染器；故这里用 gemini→openai
// 跨格式出口腿锁 openai_chat.foreignResponseExtras——live 探针 2026-10-19 证实
// Gemini 非流式响应顶层同样携带这两键（键经 gemini 解析落 Response.Extra）。
func TestOpenAIDropsBuddyTimingExtras(t *testing.T) {
	reg := NewRegistry()
	geminiResp := []byte(`{
		"candidates": [{"content": {"role": "model", "parts": [{"text": "hi"}]},
			"finishReason": "STOP", "index": 0}],
		"usageMetadata": {"promptTokenCount": 3, "candidatesTokenCount": 2, "totalTokenCount": 5},
		"modelVersion": "deepseek-v4.1-flash", "responseId": "resp-buddy-1",
		"elapsed_ms": 1234,
		"first_chunk_at": 1760000000.5,
		"vendor_trace": {"hop": "buddy"}
	}`)
	out, err := Convert(reg, fGemini, fOpenAI, FaceResponse, geminiResp, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	for _, k := range []string{"elapsed_ms", "first_chunk_at"} {
		if containsJSONKey(t, out, k) {
			t.Fatalf("buddy-private timing key %q leaked into openai response: %s", k, out)
		}
	}
	if !containsJSONKey(t, out, "vendor_trace") {
		t.Fatalf("generic unknown key vendor_trace lost (over-filtering): %s", out)
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(out, &body); err != nil {
		t.Fatalf("openai response is not a JSON object: %v\n%s", err, out)
	}
	if got := string(body["id"]); got != `"resp-buddy-1"` {
		t.Fatalf("id not preserved: %s", got)
	}
	if got := string(body["model"]); got != `"deepseek-v4.1-flash"` {
		t.Fatalf("model not preserved: %s", got)
	}
	var choices []map[string]json.RawMessage
	if err := json.Unmarshal(body["choices"], &choices); err != nil || len(choices) != 1 {
		t.Fatalf("choices malformed: %v %s", err, body["choices"])
	}
	var message map[string]json.RawMessage
	if err := json.Unmarshal(choices[0]["message"], &message); err != nil {
		t.Fatalf("message malformed: %v", err)
	}
	if got := string(message["content"]); got != `"hi"` {
		t.Fatalf("content text not preserved: %s", got)
	}
	var usage map[string]json.RawMessage
	if err := json.Unmarshal(body["usage"], &usage); err != nil {
		t.Fatalf("usage not an object: %v", err)
	}
	if string(usage["prompt_tokens"]) != "3" || string(usage["completion_tokens"]) != "2" {
		t.Fatalf("usage not preserved: %s", body["usage"])
	}
}

// TestGeminiDropsBuddyTimingExtras 响应面来源独有计时键（Gemini 出口）：
// openai_chat 来源的 buddy 私有计时键 elapsed_ms / first_chunk_at 不得进入
// GenerateContentResponse；通用未知键 vendor_trace 仍按保真策略透传
// （gemini 出口是「过滤来源独有键 + 通用未知键保真」，见 gemini.RenderResponse
// 头注——不是纯白名单，故这里同时断言 vendor_trace 在位）。
func TestGeminiDropsBuddyTimingExtras(t *testing.T) {
	reg := NewRegistry()
	openaiResp := []byte(`{
		"id": "chatcmpl-buddy-1", "object": "chat.completion", "created": 1,
		"model": "deepseek-chat",
		"choices": [{"index": 0, "finish_reason": "stop",
			"message": {"role": "assistant", "content": "hi"}}],
		"usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
		"elapsed_ms": 1234,
		"first_chunk_at": 1760000000.5,
		"vendor_trace": {"hop": "buddy"}
	}`)
	out, err := Convert(reg, fOpenAI, fGemini, FaceResponse, openaiResp, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	for _, k := range []string{"elapsed_ms", "first_chunk_at"} {
		if containsJSONKey(t, out, k) {
			t.Fatalf("buddy-private timing key %q leaked into gemini response: %s", k, out)
		}
	}
	if !containsJSONKey(t, out, "vendor_trace") {
		t.Fatalf("generic unknown key vendor_trace lost (over-filtering): %s", out)
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(out, &body); err != nil {
		t.Fatalf("gemini response is not a JSON object: %v\n%s", err, out)
	}
	if got := string(body["responseId"]); got != `"chatcmpl-buddy-1"` {
		t.Fatalf("responseId not preserved: %s", got)
	}
	if got := string(body["modelVersion"]); got != `"deepseek-chat"` {
		t.Fatalf("modelVersion not preserved: %s", got)
	}
	var candidates []map[string]json.RawMessage
	if err := json.Unmarshal(body["candidates"], &candidates); err != nil || len(candidates) != 1 {
		t.Fatalf("candidates malformed: %v %s", err, body["candidates"])
	}
	var content map[string]json.RawMessage
	if err := json.Unmarshal(candidates[0]["content"], &content); err != nil {
		t.Fatalf("content malformed: %v", err)
	}
	var parts []map[string]json.RawMessage
	if err := json.Unmarshal(content["parts"], &parts); err != nil || len(parts) == 0 {
		t.Fatalf("parts malformed: %v %s", err, content["parts"])
	}
	if got := string(parts[0]["text"]); got != `"hi"` {
		t.Fatalf("content text not preserved: %s", got)
	}
	var usage map[string]json.RawMessage
	if err := json.Unmarshal(body["usageMetadata"], &usage); err != nil {
		t.Fatalf("usageMetadata not an object: %v", err)
	}
	if string(usage["promptTokenCount"]) != "3" || string(usage["candidatesTokenCount"]) != "2" {
		t.Fatalf("usage not preserved: %s", body["usageMetadata"])
	}
}
