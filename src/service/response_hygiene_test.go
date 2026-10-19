package service

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/dwgx/any2api/translate/pairs"
	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// 响应面 Extra 卫生（W5-9）：来源格式独有响应键绝不出现在目标响应体。
// 库层属性（pairs.Convert FaceResponse，含枢纽两步）+ 服务壳 e2e 各锁一遍。
// 表驱动方向 ≥4：openai→claude / openai→gemini、claude→openai / claude→gemini、
// gemini→openai / gemini→claude。
// ---------------------------------------------------------------------------

// openaiRespExtras openai chat.completion + 两个响应独有键（openai-wire README
// §chat.completion 顶层字段：system_fingerprint / service_tier——均未建模，
// 进 Response.Extra）。
const openaiRespExtras = `{
	"id": "chatcmpl-1", "object": "chat.completion", "created": 1700000000, "model": "gpt-x",
	"choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}],
	"usage": {"prompt_tokens": 7, "completion_tokens": 2, "total_tokens": 9},
	"system_fingerprint": "fp_abc123",
	"service_tier": "default"
}`

// claudeRespExtras claude Message + 三个响应独有键（anthropic-messages README §响应
// 字段清单：container / stop_details / diagnostics——建模键之外，进 Response.Extra）。
const claudeRespExtras = `{
	"id": "msg_1", "type": "message", "role": "assistant", "model": "claude-x",
	"content": [{"type": "text", "text": "hi"}],
	"stop_reason": "end_turn", "stop_sequence": null,
	"usage": {"input_tokens": 7, "output_tokens": 2},
	"container": {"id": "cont_1", "expires_at": "2026-10-13T00:00:00Z"},
	"stop_details": {"type": "end_turn"},
	"diagnostics": {"cache_miss_reason": {"type": "system_changed"}}
}`

// geminiRespExtras gemini GenerateContentResponse + 响应独有键（gemini-wire README
// §响应：promptFeedback / candidates[].safetyRatings / citationMetadata /
// groundingMetadata / avgLogprobs——解析层不建模、也不进 Response.Extra，出口不得出现）。
const geminiRespExtras = `{
	"candidates": [{
		"content": {"role": "model", "parts": [{"text": "hi"}]},
		"finishReason": "STOP", "index": 0,
		"safetyRatings": [{"category": "HARM_CATEGORY_HARASSMENT", "probability": "NEGLIGIBLE"}],
		"citationMetadata": {"citationSources": [{"uri": "https://example.invalid", "startIndex": 0, "endIndex": 2}]},
		"groundingMetadata": {"webSearchQueries": ["q"]},
		"avgLogprobs": -0.1
	}],
	"promptFeedback": {"blockReason": "BLOCK_REASON_UNSPECIFIED"},
	"usageMetadata": {"promptTokenCount": 7, "candidatesTokenCount": 2, "totalTokenCount": 9},
	"modelVersion": "gemini-2.5-flash", "responseId": "resp_1"
}`

// TestResponseExtrasHygiene 响应面 Extra 防泄漏表驱动：来源格式独有响应键绝不出现在
// 目标响应体（含经枢纽两步的中间腿与出口腿），且跨格式键保留（防过滤过度）。
func TestResponseExtrasHygiene(t *testing.T) {
	reg := pairs.NewRegistry()

	openaiOnly := []string{"system_fingerprint", "service_tier"}
	claudeOnly := []string{"container", "stop_details", "diagnostics"}
	geminiOnly := []string{"promptFeedback", "safetyRatings", "citationMetadata", "groundingMetadata", "avgLogprobs"}

	cases := []struct {
		name    string
		from    registry.Format
		to      registry.Format
		in      string
		forbid  []string
		keepKey string // 必须保留的跨格式键（防过滤过度）
	}{
		{"openai_to_claude", registry.FormatOpenAIChat, registry.FormatClaudeMessages, openaiRespExtras, openaiOnly, "stop_reason"},
		{"openai_to_gemini", registry.FormatOpenAIChat, registry.FormatGeminiGenerate, openaiRespExtras, openaiOnly, "candidates"},
		{"claude_to_openai", registry.FormatClaudeMessages, registry.FormatOpenAIChat, claudeRespExtras, claudeOnly, "choices"},
		{"claude_to_gemini_via_hub", registry.FormatClaudeMessages, registry.FormatGeminiGenerate, claudeRespExtras, claudeOnly, "candidates"},
		{"gemini_to_openai", registry.FormatGeminiGenerate, registry.FormatOpenAIChat, geminiRespExtras, geminiOnly, "choices"},
		{"gemini_to_claude_via_hub", registry.FormatGeminiGenerate, registry.FormatClaudeMessages, geminiRespExtras, geminiOnly, "content"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := pairs.Convert(reg, tc.from, tc.to, pairs.FaceResponse, []byte(tc.in), pairs.Options{})
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}
			for _, k := range tc.forbid {
				if jsonHasKey(t, out, k) {
					t.Fatalf("来源独有响应键 %q 泄漏进 %s 响应体: %s", k, tc.to, out)
				}
			}
			if !jsonHasKey(t, out, tc.keepKey) {
				t.Fatalf("跨格式键 %q 丢失: %s", tc.keepKey, out)
			}
		})
	}

	// 枢纽中间腿（claude→openai→gemini 的 openai 渲染）本身必须干净：中间体在流式
	// 枢纽腿会被真实解析（render→parse），脏键会经 Response.Extra 一路带下去。
	intermediate, err := pairs.Convert(reg, registry.FormatClaudeMessages, registry.FormatOpenAIChat,
		pairs.FaceResponse, []byte(claudeRespExtras), pairs.Options{})
	if err != nil {
		t.Fatalf("intermediate: %v", err)
	}
	for _, k := range claudeOnly {
		if jsonHasKey(t, intermediate, k) {
			t.Fatalf("来源独有键 %q 泄漏进枢纽中间体: %s", k, intermediate)
		}
	}
}

// TestE2EClaudeEntryOpenAIUpstreamResponseHygiene 服务壳 e2e：claude 入口 × openai 上游
// （直转对）响应面卫生——openai 响应独有键（system_fingerprint / service_tier）不得
// 出现在 claude 响应体（W5-8 报告 §缺口 2 的点名形态，走真 HTTP 路径）。
func TestE2EClaudeEntryOpenAIUpstreamResponseHygiene(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openaiRespExtras))
	}))
	t.Cleanup(up.Close)
	srv := startService(t, cfgFor(up.URL, ProtocolOpenAIChat, 15))

	resp := postJSON(t, srv.URL+"/v1/messages",
		`{"model":"gpt-x","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	for _, k := range []string{"system_fingerprint", "service_tier"} {
		if jsonHasKey(t, body, k) {
			t.Fatalf("openai 响应独有键 %q 泄漏进 claude 响应体: %s", k, body)
		}
	}
	var out struct {
		Type    string `json:"type"`
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, body)
	}
	if out.Type != "message" || len(out.Content) != 1 || out.Content[0].Text != "hi" {
		t.Fatalf("claude 响应 = %+v\n%s", out, body)
	}
}

// jsonHasKey 递归检查 JSON 里是否出现指定键（任意层级）。
func jsonHasKey(t *testing.T, raw []byte, key string) bool {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, raw)
	}
	return jsonHasKeyValue(v, key)
}

func jsonHasKeyValue(v any, key string) bool {
	switch x := v.(type) {
	case map[string]any:
		for k, sub := range x {
			if k == key || jsonHasKeyValue(sub, key) {
				return true
			}
		}
	case []any:
		for _, sub := range x {
			if jsonHasKeyValue(sub, key) {
				return true
			}
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// W104：同格式非流式响应 extras 清洗开关（response_hygiene.scrub_same_format_extras）
// ---------------------------------------------------------------------------

// buddyTimingUpstreamBody OpenAI 同格式非流式 chat.completion：正常字段 +
// upstream-a/upstream-private 私有计时键（elapsed_ms / first_chunk_at，W83-1 X1
// 实测）+ 通用未知键 vendor_trace（防过滤过度的保留对照）。
const buddyTimingUpstreamBody = `{"id":"chatcmpl-buddy-1","object":"chat.completion","created":1760000000,"model":"deepseek-v4.1-flash","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5},"elapsed_ms":1234,"first_chunk_at":1760000000.5,"vendor_trace":{"hop":"buddy"}}`

// buddyTimingScrubbedBody 开关开启后的逐字节期望：只删两个键值及其前置逗号，
// 其余字节（字段顺序/空白/数字写法）逐字节不变——手术式剥除，非 re-marshal。
const buddyTimingScrubbedBody = `{"id":"chatcmpl-buddy-1","object":"chat.completion","created":1760000000,"model":"deepseek-v4.1-flash","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5},"vendor_trace":{"hop":"buddy"}}`

// startBuddyTimingUpstream 假 OpenAI 上游：非流式返回固定体（含两个私有计时键）。
func startBuddyTimingUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, buddyTimingUpstreamBody)
	}))
	t.Cleanup(up.Close)
	return up
}

// TestResponseHygieneSameFormatExtrasDefaultOffByteIdentical 缺省（不写
// response_hygiene 段）：同格式非流式响应体与上游逐字节相同——两个私有键仍在，
// 证明开关缺省 = 既有保真策略零变化。
func TestResponseHygieneSameFormatExtrasDefaultOffByteIdentical(t *testing.T) {
	up := startBuddyTimingUpstream(t)
	srv := startService(t, cfgFor(up.URL, ProtocolOpenAIChat, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"deepseek-v4.1-flash","messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(raw, []byte(buddyTimingUpstreamBody)) {
		t.Fatalf("default-off body is not byte-identical to upstream\n got: %s\nwant: %s", raw, buddyTimingUpstreamBody)
	}
	for _, k := range []string{"elapsed_ms", "first_chunk_at"} {
		if !jsonHasKey(t, raw, k) {
			t.Fatalf("default-off body lost upstream key %q: %s", k, raw)
		}
	}
}

// TestResponseHygieneSameFormatExtrasOn 开关开启：两个私有计时键消失，vendor_trace
// 与 id/model/content/usage 原值保留；整段字节等于「上游体 - 两个键值区间」。
func TestResponseHygieneSameFormatExtrasOn(t *testing.T) {
	up := startBuddyTimingUpstream(t)
	cfg := cfgFor(up.URL, ProtocolOpenAIChat, 15)
	cfg.ResponseHygiene = &ResponseHygieneSection{ScrubSameFormatExtras: true}
	srv := startService(t, cfg)

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"deepseek-v4.1-flash","messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(raw, []byte(buddyTimingScrubbedBody)) {
		t.Fatalf("switch-on body differs beyond removed spans\n got: %s\nwant: %s", raw, buddyTimingScrubbedBody)
	}
	for _, k := range []string{"elapsed_ms", "first_chunk_at"} {
		if jsonHasKey(t, raw, k) {
			t.Fatalf("buddy-private timing key %q leaked with switch on: %s", k, raw)
		}
	}
	if !jsonHasKey(t, raw, "vendor_trace") {
		t.Fatalf("generic unknown key vendor_trace was over-filtered: %s", raw)
	}
	var out struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, raw)
	}
	if out.ID != "chatcmpl-buddy-1" || out.Model != "deepseek-v4.1-flash" ||
		len(out.Choices) != 1 || out.Choices[0].Message.Content != "hi" ||
		out.Usage.PromptTokens != 3 || out.Usage.CompletionTokens != 2 || out.Usage.TotalTokens != 5 {
		t.Fatalf("normal fields not preserved: %+v\n%s", out, raw)
	}
}

// TestResponseHygieneSameFormatExtrasCrossFormatControl 负向对照：OpenAI 上游 →
// Anthropic 入口是跨格式出口，无论开关开关都由 claude 渲染器的
// foreignResponseExtras 过滤（W85），本开关不得改变该路径；vendor_trace 仍保留。
func TestResponseHygieneSameFormatExtrasCrossFormatControl(t *testing.T) {
	for _, on := range []bool{false, true} {
		name := "switch_off"
		if on {
			name = "switch_on"
		}
		t.Run(name, func(t *testing.T) {
			up := startBuddyTimingUpstream(t)
			cfg := cfgFor(up.URL, ProtocolOpenAIChat, 15)
			if on {
				cfg.ResponseHygiene = &ResponseHygieneSection{ScrubSameFormatExtras: true}
			}
			srv := startService(t, cfg)

			resp := postJSON(t, srv.URL+"/v1/messages",
				`{"model":"claude-x","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`, nil)
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			for _, k := range []string{"elapsed_ms", "first_chunk_at"} {
				if jsonHasKey(t, body, k) {
					t.Fatalf("buddy-private timing key %q leaked into claude response: %s", k, body)
				}
			}
			if !jsonHasKey(t, body, "vendor_trace") {
				t.Fatalf("generic unknown key vendor_trace was over-filtered: %s", body)
			}
			var msg struct {
				Type    string `json:"type"`
				Content []struct {
					Text string `json:"text"`
				} `json:"content"`
			}
			if err := json.Unmarshal(body, &msg); err != nil {
				t.Fatalf("unmarshal: %v\n%s", err, body)
			}
			if msg.Type != "message" || len(msg.Content) != 1 || msg.Content[0].Text != "hi" {
				t.Fatalf("claude response = %+v\n%s", msg, body)
			}
		})
	}
}

// TestResponseHygieneSameFormatExtrasConfigYAML 配置面：KnownFields(true) 下
// `response_hygiene.scrub_same_format_extras` 解析正确；不写本节 = false。
func TestResponseHygieneSameFormatExtrasConfigYAML(t *testing.T) {
	dir := t.TempDir()
	base := "listen: \"127.0.0.1:0\"\nupstreams:\n  - name: u\n    base_url: http://127.0.0.1:9\n    protocol: openai_chat\n"
	write := func(s string) string {
		p := filepath.Join(dir, "cfg.yaml")
		if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		return p
	}

	c, err := LoadConfig(write(base))
	if err != nil {
		t.Fatalf("load default: %v", err)
	}
	if c.scrubSameFormatExtras() {
		t.Fatal("omitted response_hygiene section must default to false")
	}
	if c.ResponseHygiene != nil {
		t.Fatalf("omitted section parsed as %+v", c.ResponseHygiene)
	}

	c2, err := LoadConfig(write(base + "response_hygiene:\n  scrub_same_format_extras: true\n"))
	if err != nil {
		t.Fatalf("load switch-on: %v", err)
	}
	if !c2.scrubSameFormatExtras() {
		t.Fatalf("response_hygiene.scrub_same_format_extras=true not parsed: %+v", c2.ResponseHygiene)
	}
}
