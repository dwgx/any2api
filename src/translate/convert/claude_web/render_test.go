package claude_web

// W10：请求面渲染测试——字段白名单（实抓字段集）/ effort 档位映射 / uuid 生成 /
// 拍平规则 / 不存在面显式报错。
//
// 反证锚点：删掉 RenderRequest 的 `reasoning_effort` 档位映射 → TestRenderRequestEffort
// 红；放开白名单（把 Extra 原样并入体）→ TestRenderRequestFieldWhitelist 红。

import (
	"bytes"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

func textReq(model string, extra map[string]json.RawMessage) *ir.Request {
	return &ir.Request{
		Model: model,
		Messages: []ir.Message{
			{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "hi"}}},
		},
		Extra: extra,
	}
}

func renderBody(t *testing.T, req *ir.Request) map[string]json.RawMessage {
	t.Helper()
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	var w map[string]json.RawMessage
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	return w
}

// TestRenderRequestFieldWhitelist 字段集 = W9-2 实抓首回合体的 14 键（多一不可、
// 少一不可——上游对多余字段报错而非忽略，KB §陷阱 2）。
func TestRenderRequestFieldWhitelist(t *testing.T) {
	w := renderBody(t, textReq("claude-sonnet-5-5", nil))
	want := []string{
		"prompt", "timezone", "locale", "model", "effort", "thinking_mode", "tools",
		"turn_message_uuids", "attachments", "files", "sync_sources",
		"completion_request_id", "rendering_mode", "create_conversation_params",
	}
	if len(w) != len(want) {
		t.Fatalf("字段数 = %d, want %d: %v", len(w), len(want), keysOf(w))
	}
	for _, k := range want {
		if _, ok := w[k]; !ok {
			t.Fatalf("缺字段 %q（有 %v）", k, keysOf(w))
		}
	}
	// 不得出现 parent_message_uuid（首回合体不带它；本服务每请求新建会话）。
	if _, ok := w["parent_message_uuid"]; ok {
		t.Fatalf("首回合体不得带 parent_message_uuid")
	}
	// 缺省值（实抓值或注明来源的缺省）。
	var s string
	_ = json.Unmarshal(w["timezone"], &s)
	if s != DefaultTimezone {
		t.Fatalf("timezone = %q, want %q", s, DefaultTimezone)
	}
	_ = json.Unmarshal(w["locale"], &s)
	if s != DefaultLocale {
		t.Fatalf("locale = %q, want %q", s, DefaultLocale)
	}
	_ = json.Unmarshal(w["effort"], &s)
	if s != DefaultEffort {
		t.Fatalf("effort = %q, want %q", s, DefaultEffort)
	}
	_ = json.Unmarshal(w["thinking_mode"], &s)
	if s != ThinkingModeAuto {
		t.Fatalf("thinking_mode = %q, want %q", s, ThinkingModeAuto)
	}
	_ = json.Unmarshal(w["rendering_mode"], &s)
	if s != RenderingMode {
		t.Fatalf("rendering_mode = %q, want %q", s, RenderingMode)
	}
	_ = json.Unmarshal(w["prompt"], &s)
	if s != "Human: hi" {
		t.Fatalf("prompt = %q, want %q", s, "Human: hi")
	}
	// 空数组字段（实抓恒空）。
	for _, k := range []string{"attachments", "files", "sync_sources"} {
		if string(w[k]) != "[]" {
			t.Fatalf("%s = %s, want []", k, w[k])
		}
	}
	// tools = 三元组（跨实现互证的最小可复现面）。
	var tools []map[string]string
	if err := json.Unmarshal(w["tools"], &tools); err != nil {
		t.Fatalf("tools: %v", err)
	}
	if len(tools) != 3 || tools[0]["name"] != "web_search" || tools[0]["type"] != "web_search_v0" ||
		tools[1]["name"] != "artifacts" || tools[2]["name"] != "repl" {
		t.Fatalf("tools = %+v", tools)
	}
	// create_conversation_params（实抓形态）。
	var cp struct {
		Name                           string  `json:"name"`
		Model                          string  `json:"model"`
		IncludeConversationPreferences bool    `json:"include_conversation_preferences"`
		PaprikaMode                    *string `json:"paprika_mode"`
		CompassMode                    *string `json:"compass_mode"`
		ToolSearchMode                 string  `json:"tool_search_mode"`
		IsTemporary                    bool    `json:"is_temporary"`
		ChatMemoryMode                 string  `json:"chat_memory_mode"`
		EnabledImagine                 bool    `json:"enabled_imagine"`
	}
	if err := json.Unmarshal(w["create_conversation_params"], &cp); err != nil {
		t.Fatalf("create_conversation_params: %v", err)
	}
	if cp.Name != "" || cp.Model != "claude-sonnet-5-5" || !cp.IncludeConversationPreferences ||
		cp.PaprikaMode != nil || cp.CompassMode != nil || cp.ToolSearchMode != "auto" ||
		cp.IsTemporary || cp.ChatMemoryMode != "enabled" || !cp.EnabledImagine {
		t.Fatalf("create_conversation_params = %+v", cp)
	}
}

// TestRenderRequestNoLeak 未建模 Extra 与参数面**绝不漏进上游体**（上游对多余字段
// 报错；reasoning_effort 只做档位映射）。
func TestRenderRequestNoLeak(t *testing.T) {
	extra := map[string]json.RawMessage{
		"reasoning_effort": json.RawMessage(`"high"`),
		"max_tokens":       json.RawMessage(`1024`),
		"temperature":      json.RawMessage(`0.7`),
		"top_p":            json.RawMessage(`1.0`),
		"stop":             json.RawMessage(`["x"]`),
		"user":             json.RawMessage(`"u1"`),
		"seed":             json.RawMessage(`7`),
		"future_field":     json.RawMessage(`{"a":1}`),
	}
	w := renderBody(t, textReq("claude-sonnet-5-5", extra))
	for _, k := range []string{"reasoning_effort", "max_tokens", "temperature", "top_p", "stop", "user", "seed", "future_field"} {
		if _, ok := w[k]; ok {
			t.Fatalf("字段 %q 漏进上游体（上游 400 Extra inputs are not permitted）", k)
		}
	}
	// stream 字段：网页线 completion 无 stream 开关（恒 SSE）——请求体不得带它。
	if _, ok := w["stream"]; ok {
		t.Fatalf("stream 漏进上游体（实抓体无该字段）")
	}
}

// TestRenderRequestEffort reasoning_effort 档位映射（UI 5 档 ↔ low/medium/high/
// xhigh/max；minimal→low；none→thinking_mode none）与 Claude Code thinking 字段。
func TestRenderRequestEffort(t *testing.T) {
	cases := []struct {
		name         string
		extra        map[string]json.RawMessage
		wantEffort   string
		wantThinking string
	}{
		{"缺省", nil, "medium", "auto"},
		{"minimal→low", map[string]json.RawMessage{"reasoning_effort": json.RawMessage(`"minimal"`)}, "low", "auto"},
		{"low", map[string]json.RawMessage{"reasoning_effort": json.RawMessage(`"low"`)}, "low", "auto"},
		{"medium", map[string]json.RawMessage{"reasoning_effort": json.RawMessage(`"medium"`)}, "medium", "auto"},
		{"high", map[string]json.RawMessage{"reasoning_effort": json.RawMessage(`"high"`)}, "high", "auto"},
		{"xhigh", map[string]json.RawMessage{"reasoning_effort": json.RawMessage(`"xhigh"`)}, "xhigh", "auto"},
		{"max", map[string]json.RawMessage{"reasoning_effort": json.RawMessage(`"max"`)}, "max", "auto"},
		{"none→thinking off", map[string]json.RawMessage{"reasoning_effort": json.RawMessage(`"none"`)}, "medium", "none"},
		{"未知档位→缺省", map[string]json.RawMessage{"reasoning_effort": json.RawMessage(`"ultra"`)}, "medium", "auto"},
		{"大小写归一", map[string]json.RawMessage{"reasoning_effort": json.RawMessage(`"HIGH"`)}, "high", "auto"},
		{"Claude Code thinking disabled", map[string]json.RawMessage{"thinking": json.RawMessage(`{"type":"disabled"}`)}, "medium", "none"},
		{"Claude Code thinking enabled", map[string]json.RawMessage{"thinking": json.RawMessage(`{"type":"enabled","budget_tokens":4096}`)}, "medium", "auto"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := renderBody(t, textReq("claude-sonnet-5-5", tc.extra))
			var effort, thinking string
			_ = json.Unmarshal(w["effort"], &effort)
			_ = json.Unmarshal(w["thinking_mode"], &thinking)
			if effort != tc.wantEffort || thinking != tc.wantThinking {
				t.Fatalf("effort/thinking = %q/%q, want %q/%q", effort, thinking, tc.wantEffort, tc.wantThinking)
			}
		})
	}
}

// TestRenderRequestUUIDs turn_message_uuids 与 completion_request_id 是 v4 形态且
// **每请求新生成**（两次渲染不重样）。
func TestRenderRequestUUIDs(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := map[string]bool{}
	for i := range 4 {
		w := renderBody(t, textReq("claude-sonnet-5-5", nil))
		var tu struct {
			Human     string `json:"human_message_uuid"`
			Assistant string `json:"assistant_message_uuid"`
		}
		if err := json.Unmarshal(w["turn_message_uuids"], &tu); err != nil {
			t.Fatalf("turn_message_uuids: %v", err)
		}
		var reqID string
		_ = json.Unmarshal(w["completion_request_id"], &reqID)
		for _, id := range []string{tu.Human, tu.Assistant, reqID} {
			if !re.MatchString(id) {
				t.Fatalf("第 %d 次渲染：uuid 形态不符（%q）", i, id)
			}
			if seen[id] {
				t.Fatalf("uuid 重复：%q", id)
			}
			seen[id] = true
		}
		if tu.Human == tu.Assistant {
			t.Fatalf("human/assistant uuid 不得相同")
		}
	}
}

// TestRenderRequestPrompt 拍平规则（CW/YS 前缀约定；消息边界 = 空行）与不支持块
// 的显式报错（图像/工具面未接线）。
func TestRenderRequestPrompt(t *testing.T) {
	req := &ir.Request{
		Model: "claude-sonnet-5-5",
		Messages: []ir.Message{
			{Role: ir.RoleSystem, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "Be terse."}}},
			{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "hi"}}},
			{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "hello"}}},
			{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "bye"}}},
		},
	}
	w := renderBody(t, req)
	var prompt string
	_ = json.Unmarshal(w["prompt"], &prompt)
	want := "System: Be terse.\n\nHuman: hi\n\nAssistant: hello\n\nHuman: bye"
	if prompt != want {
		t.Fatalf("prompt = %q, want %q", prompt, want)
	}

	// 图像块（W22-1；接缝 W24-1）：有载体 → 不报错；占位引用进 files[]，附件
	// 作业经 sink 交付、可原地回填。无载体（无 Data 无 URL）→ 显式报错（绝不静默丢）。
	img := &ir.Request{Model: "m", Messages: []ir.Message{{
		Role:   ir.RoleUser,
		Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "see"}, {Kind: ir.BlockImage, Image: &ir.ImageSource{Kind: "inline", MediaType: "image/png", Data: "aGk="}}},
	}}}
	sink := &AttachmentSink{}
	body, err := RenderRequestWithOptions(img, RenderOptions{AttachmentSink: sink})
	if err != nil {
		t.Fatalf("有载体的图像块不得报错: %v", err)
	}
	var wImg map[string]json.RawMessage
	if err := json.Unmarshal(body, &wImg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var files []string
	if err := json.Unmarshal(wImg["files"], &files); err != nil || len(files) != 1 || len(files[0]) != 36 {
		t.Fatalf("files = %v (err=%v), want 1 token of 36 chars", files, err)
	}
	if job := sink.Job; job == nil || job.Len() != 1 {
		t.Fatalf("渲染含图请求必须把附件作业交付 sink（job=%v）", job)
	} else {
		ref := "01234567-89ab-4cde-8f01-23456789abcd"
		if err := job.Resolve(0, ref); err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if !bytes.Contains(job.Body(), []byte(ref)) {
			t.Fatalf("Resolve 后渲染体必须含真实 file_uuid")
		}
	}
	// 渲染器零全局残留（W24-1）：再渲染（新 sink）必得新作业——旧「取走即销」
	// 断言的接缝等价形态。
	sink2 := &AttachmentSink{}
	if _, err := RenderRequestWithOptions(img, RenderOptions{AttachmentSink: sink2}); err != nil {
		t.Fatalf("render#2: %v", err)
	}
	if sink2.Job == nil || sink2.Job == sink.Job {
		t.Fatalf("再渲染必须产出新作业（渲染器零全局残留）")
	}
	noCarrier := &ir.Request{Model: "m", Messages: []ir.Message{{
		Role:   ir.RoleUser,
		Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "see"}, {Kind: ir.BlockImage, Image: &ir.ImageSource{Kind: "inline", MediaType: "image/png"}}},
	}}}
	if _, err := RenderRequest(noCarrier); err == nil || !errors.Is(err, ErrAttachmentCarrier) {
		t.Fatalf("无载体图像块必须显式报错（ErrAttachmentCarrier）: %v", err)
	}
	// 客户端工具块：W11-3 起不再报错——拍平为标签文本（见
	// TestRenderRequestToolHistory；本处只断言「不报错」）。
	tool := &ir.Request{Model: "m", Messages: []ir.Message{{
		Role:   ir.RoleAssistant,
		Blocks: []ir.ContentBlock{{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{ID: "call_1", Name: "f", Arguments: "{}"}}},
	}}}
	if _, err := RenderRequest(tool); err != nil {
		t.Fatalf("工具历史块不得断流: %v", err)
	}
	// 空请求 / 空模型 / 空 prompt。
	if _, err := RenderRequest(nil); err == nil {
		t.Fatalf("nil 请求必须报错")
	}
	if _, err := RenderRequest(textReq("", nil)); err == nil {
		t.Fatalf("空模型必须报错")
	}
	if _, err := RenderRequest(&ir.Request{Model: "m"}); err == nil {
		t.Fatalf("空 prompt 必须报错")
	}
}

// TestRenderRequestToolHistory W11-3：含工具历史（Claude Code 真实形态：assistant
// tool_use + user 侧 tool_result + server_tool_use）的请求**不断流**，拍平为
// 线格式块名标签文本（flattenBlock 头注的形态；fixture 词汇）。
func TestRenderRequestToolHistory(t *testing.T) {
	req := &ir.Request{
		Model: "claude-sonnet-5-5",
		Messages: []ir.Message{
			{Role: ir.RoleSystem, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "You are Claude Code."}}},
			{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "read a.go"}}},
			{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
				{Kind: ir.BlockText, Text: "Let me read it."},
				{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{ID: "toolu_01", Name: "Read", Arguments: `{"file_path":"a.go"}`}},
			}},
			{Role: ir.RoleTool, Blocks: []ir.ContentBlock{
				{Kind: ir.BlockToolResult, ToolResult: &ir.ToolResult{CallID: "toolu_01", Content: "package main"}},
			}},
			// server_tool_use（上游内置工具回放轮）与空参数 tool_use 的形态。
			{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
				{Kind: ir.BlockServerToolUse, ToolCall: &ir.ToolCall{ID: "toolu_02", Name: "bash_tool", Arguments: `{"command":"ls"}`}},
				{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{ID: "toolu_03", Name: "NoArgs"}},
			}},
			{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "now edit it"}}},
		},
		Tools: []ir.ToolDef{{Name: "Read", Description: "Read a file"}},
	}
	w := renderBody(t, req)
	var prompt string
	_ = json.Unmarshal(w["prompt"], &prompt)
	want := "System: You are Claude Code.\n\n" +
		"Human: read a.go\n\n" +
		"Assistant: Let me read it.\n" +
		`<tool_use id="toolu_01" name="Read">{"file_path":"a.go"}</tool_use>` + "\n\n" +
		`Tool: <tool_result tool_use_id="toolu_01">package main</tool_result>` + "\n\n" +
		"Assistant: " + `<tool_use id="toolu_02" name="bash_tool">{"command":"ls"}</tool_use>` + "\n" +
		`<tool_use id="toolu_03" name="NoArgs">{}</tool_use>` + "\n\n" +
		"Human: now edit it"
	if prompt != want {
		t.Fatalf("prompt = %q\nwant    %q", prompt, want)
	}
	// 客户端工具定义（req.Tools）不进 prompt、不进上游体：上游 tools 是服务端工具
	// 开关（W11-3 契约；BK/AU 的 prompt 注入属其工具模拟层，本 slice 不做）。
	var upstreamTools []map[string]string
	_ = json.Unmarshal(w["tools"], &upstreamTools)
	if len(upstreamTools) != 3 {
		t.Fatalf("上游 tools = %v, want web_search/artifacts/repl 三元组", upstreamTools)
	}
	if strings.Contains(prompt, "Read a file") {
		t.Fatalf("客户端工具定义不得注入 prompt")
	}

	// IR 不完整（tool_use 缺 ToolCall / tool_result 缺载荷）：显式报错（不静默占位）。
	bad := &ir.Request{Model: "m", Messages: []ir.Message{{
		Role:   ir.RoleAssistant,
		Blocks: []ir.ContentBlock{{Kind: ir.BlockToolUse}},
	}}}
	if _, err := RenderRequest(bad); err == nil {
		t.Fatalf("tool_use 缺 ToolCall 必须报错")
	}
	bad2 := &ir.Request{Model: "m", Messages: []ir.Message{{
		Role:   ir.RoleTool,
		Blocks: []ir.ContentBlock{{Kind: ir.BlockToolResult}},
	}}}
	if _, err := RenderRequest(bad2); err == nil {
		t.Fatalf("tool_result 缺载荷必须报错")
	}
}

// TestRenderConversationCreate 会话创建体（KB §② 三实现同形）。
func TestRenderConversationCreate(t *testing.T) {
	out, err := RenderConversationCreate("11111111-2222-4333-8444-555555555555", "claude-sonnet-5-5")
	if err != nil {
		t.Fatalf("RenderConversationCreate: %v", err)
	}
	var w map[string]json.RawMessage
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(w) != 4 {
		t.Fatalf("字段 = %v, want uuid/name/model/include_conversation_preferences", keysOf(w))
	}
	var s string
	_ = json.Unmarshal(w["uuid"], &s)
	if s != "11111111-2222-4333-8444-555555555555" {
		t.Fatalf("uuid = %q", s)
	}
	_ = json.Unmarshal(w["name"], &s)
	if s != "" {
		t.Fatalf("name = %q, want 空串", s)
	}
	var b bool
	_ = json.Unmarshal(w["include_conversation_preferences"], &b)
	if !b {
		t.Fatalf("include_conversation_preferences = false, want true")
	}
	// 空模型 = 不指定（省略 model 键）。
	out, err = RenderConversationCreate("11111111-2222-4333-8444-555555555555", "")
	if err != nil {
		t.Fatalf("RenderConversationCreate(空模型): %v", err)
	}
	w = map[string]json.RawMessage{}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := w["model"]; ok {
		t.Fatalf("空模型不得带 model 键: %v", keysOf(w))
	}
	// 缺 uuid：显式报错。
	if _, err := RenderConversationCreate("", "m"); err == nil {
		t.Fatalf("缺 uuid 必须报错")
	}
}

// TestUnsupportedFaces 上游叶子格式的入口/出口面：显式报错（绝不返回半成品字节）。
func TestUnsupportedFaces(t *testing.T) {
	if _, err := ParseRequest([]byte(`{}`)); err == nil {
		t.Fatalf("ParseRequest 必须报错")
	}
	if _, err := RenderResponse(&ir.Response{}); err == nil {
		t.Fatalf("RenderResponse 必须报错")
	}
	if _, err := RenderStream(nil); err == nil {
		t.Fatalf("RenderStream 必须报错")
	}
	if _, err := RenderError(&ir.Error{}); err == nil {
		t.Fatalf("RenderError 必须报错")
	}
}

func keysOf(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
