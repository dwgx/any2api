// W30-2 S3 渲染面 + 装配面单测：RenderRequest（IR → 匿名 Android 体）/
// ParseResponse（SSE → 非流式 IR 响应）/ Terminal accessor。
//
// 反证锚点（回退即红）：
//   - supported_encodings 改 camelCase（死字段）→ TestRenderRequestShape 红；
//   - ParseResponse 的终止判据删除 → TestParseResponseTruncated 红（截断流被
//     静默装配成成功响应）；
//   - Terminal 只看终态（不看 error 帧）→ TestTerminalAccessor 的 error 子断言红。
package chatgpt_web

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// TestRenderRequestShape IR → 匿名体：W29-1 §2.1 样本字段集逐条（snake_case
// supported_encodings / 最小消息结构 / 无状态锚点 null）。
func TestRenderRequestShape(t *testing.T) {
	req := &ir.Request{
		Model: "gpt-6",
		Messages: []ir.Message{{
			Role:   ir.RoleUser,
			Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "Reply with exactly: PONG"}},
		}},
	}
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	var w map[string]json.RawMessage
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("体: %v\n%s", err, out)
	}
	// 字段集 = 样本原文（多一个 camelCase 死字段都算红）。
	wantKeys := []string{
		"action", "messages", "parentMessageId", "conversationId", "model",
		"historyAndTrainingDisabled", "conversationMode", "forceUseSearch", "forceUseSse",
		"supported_encodings", "timezone", "timezoneOffsetMin", "noAuthAdPreferences",
	}
	if len(w) != len(wantKeys) {
		t.Fatalf("字段数 = %d（want %d）: %s", len(w), len(wantKeys), out)
	}
	for _, k := range wantKeys {
		if _, ok := w[k]; !ok {
			t.Fatalf("缺字段 %q:\n%s", k, out)
		}
	}
	if _, ok := w["supportedEncodings"]; ok {
		t.Fatalf("camelCase 死字段 supportedEncodings 不得出现（W29-1 实证被忽略）:\n%s", out)
	}
	var action, model, tz string
	_ = json.Unmarshal(w["action"], &action)
	_ = json.Unmarshal(w["model"], &model)
	_ = json.Unmarshal(w["timezone"], &tz)
	if action != "next" || model != "gpt-6" || tz != "UTC" {
		t.Fatalf("action/model/timezone = %q/%q/%q", action, model, tz)
	}
	if string(w["parentMessageId"]) != "null" || string(w["conversationId"]) != "null" {
		t.Fatalf("无状态锚点必须为 null: %s", out)
	}
	var enc []string
	_ = json.Unmarshal(w["supported_encodings"], &enc)
	if len(enc) != 1 || enc[0] != "v1" {
		t.Fatalf("supported_encodings = %v", enc)
	}
	var msgs []struct {
		Author struct {
			Role string `json:"role"`
		} `json:"author"`
		Content struct {
			ContentType string   `json:"content_type"`
			Parts       []string `json:"parts"`
		} `json:"content"`
	}
	if err := json.Unmarshal(w["messages"], &msgs); err != nil {
		t.Fatalf("messages: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Author.Role != "user" || msgs[0].Content.ContentType != "text" ||
		len(msgs[0].Content.Parts) != 1 || msgs[0].Content.Parts[0] != "Reply with exactly: PONG" {
		t.Fatalf("messages = %+v", msgs)
	}
}

// TestRenderRequestModelDefaultAndTools 空模型 → "auto"（样本值）；tools 定义
// 无载体 → 不渲染（不发明字段、不拼进 prompt）。
func TestRenderRequestModelDefaultAndTools(t *testing.T) {
	req := &ir.Request{
		Messages: []ir.Message{{
			Role:   ir.RoleUser,
			Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "hi"}},
		}},
		Tools: []ir.ToolDef{{Name: "Read", Description: "read a file"}},
	}
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	var w map[string]json.RawMessage
	_ = json.Unmarshal(out, &w)
	var model string
	_ = json.Unmarshal(w["model"], &model)
	if model != "auto" {
		t.Fatalf("空模型 = %q, want auto", model)
	}
	if _, ok := w["tools"]; ok {
		t.Fatalf("匿名线无 tools 字段（不得发明）:\n%s", out)
	}
	if strings.Contains(string(out), "Read") {
		t.Fatalf("tools 定义不得拼进 prompt:\n%s", out)
	}
}

// TestRenderRequestUnsupported 显式报错面：零消息 / 不可承载块（image）/ tool
// 块无载荷——绝不静默丢。
func TestRenderRequestUnsupported(t *testing.T) {
	if _, err := RenderRequest(&ir.Request{}); !errors.Is(err, ErrBadIR) {
		t.Fatalf("零消息 err = %v（want ErrBadIR）", err)
	}
	req := &ir.Request{Messages: []ir.Message{{
		Role:   ir.RoleUser,
		Blocks: []ir.ContentBlock{{Kind: ir.BlockImage}},
	}}}
	if _, err := RenderRequest(req); !errors.Is(err, ErrUnsupportedBlock) {
		t.Fatalf("image 块 err = %v（want ErrUnsupportedBlock）", err)
	}
	req = &ir.Request{Messages: []ir.Message{{
		Role:   ir.RoleAssistant,
		Blocks: []ir.ContentBlock{{Kind: ir.BlockToolUse}},
	}}}
	if _, err := RenderRequest(req); !errors.Is(err, ErrBadIR) {
		t.Fatalf("tool_use 无载荷 err = %v（want ErrBadIR）", err)
	}
}

// TestRenderRequestToolHistory 工具历史块 → 标签文本（与 claude_web 同词表）。
func TestRenderRequestToolHistory(t *testing.T) {
	req := &ir.Request{
		Model: "auto",
		Messages: []ir.Message{
			{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
				{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{ID: "t1", Name: "Read", Arguments: `{"path":"a"}`}},
			}},
			{Role: ir.RoleTool, Blocks: []ir.ContentBlock{
				{Kind: ir.BlockToolResult, ToolResult: &ir.ToolResult{CallID: "t1", Content: "ok"}},
			}},
		},
	}
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	var w struct {
		Messages []struct {
			Content struct {
				Parts []string `json:"parts"`
			} `json:"content"`
		} `json:"messages"`
	}
	_ = json.Unmarshal(out, &w)
	if len(w.Messages) != 2 {
		t.Fatalf("messages = %d", len(w.Messages))
	}
	if got := w.Messages[0].Content.Parts[0]; got != `<tool_use id="t1" name="Read">{"path":"a"}</tool_use>` {
		t.Fatalf("tool_use 标签 = %q", got)
	}
	if got := w.Messages[1].Content.Parts[0]; got != `<tool_result tool_use_id="t1">ok</tool_result>` {
		t.Fatalf("tool_result 标签 = %q", got)
	}
}

// TestParseResponseFromGolden 三份 golden → 非流式 IR 响应（正文/停止理由/模型
// 回读；usage 恒无——绝不合成）。
func TestParseResponseFromGolden(t *testing.T) {
	cases := []struct {
		name, text string
	}{
		{"anon_v1_direct.sse.txt", "1\n2\n3\n4\n5\n6\n7\n8\n9\n10"},
		{"anon_v1_isp.sse.txt", "PONG"},
		{"anon_snapshot.sse.txt", "PONG"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := os.ReadFile("testdata/" + tc.name)
			if err != nil {
				t.Fatalf("read golden: %v", err)
			}
			resp, err := ParseResponse(raw)
			if err != nil {
				t.Fatalf("ParseResponse: %v", err)
			}
			if resp.StopReason != ir.StopStop {
				t.Fatalf("stop_reason = %q", resp.StopReason)
			}
			if resp.Model != "gpt-6" {
				t.Fatalf("model = %q（want gpt-6——live 消息回读）", resp.Model)
			}
			if resp.Usage != nil {
				t.Fatalf("usage 不得合成（上游从不给 token 数）: %+v", resp.Usage)
			}
			var text strings.Builder
			for _, blk := range resp.Message.Blocks {
				if blk.Kind == ir.BlockText {
					text.WriteString(blk.Text)
				}
			}
			if text.String() != tc.text {
				t.Fatalf("正文 = %q, want %q", text.String(), tc.text)
			}
		})
	}
}

// TestParseResponseTruncated 截断流（无终止信号）→ 显式报错（绝不静默装配成
// 成功响应）。
func TestParseResponseTruncated(t *testing.T) {
	raw, err := os.ReadFile("testdata/anon_v1_direct.sse.txt")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	sse := string(raw)
	// 截到批量 patch（status/end_turn）之前：有 start/正文但无任何终止判据。
	cut := strings.Index(sse, `"o": "patch"`)
	if cut < 0 {
		cut = strings.Index(sse, `"o":"patch"`)
	}
	if cut < 0 {
		t.Fatalf("golden 未含批量 patch 帧")
	}
	// 回退到帧边界（前一个空行）。
	if idx := strings.LastIndex(sse[:cut], "\n\n"); idx > 0 {
		cut = idx
	}
	if _, err := ParseResponse([]byte(sse[:cut])); err == nil {
		t.Fatalf("截断流应显式报错（ParseResponse 静默装配）")
	}
}

// TestTerminalAccessor Terminal 观测面：终态判据命中 / 流内 error 帧都算终态；
// 未终态 = false（服务壳尾读宽限的武装判据）。
func TestTerminalAccessor(t *testing.T) {
	p := NewStreamParser()
	if p.Terminal() {
		t.Fatalf("空流 Terminal() = true")
	}
	if _, err := p.Feed("", []byte(`{"p":"/message/content/parts/0","o":"append","v":"hi"}`)); err != nil {
		t.Fatalf("feed: %v", err)
	}
	if p.Terminal() {
		t.Fatalf("正文增量后 Terminal() = true（未终态）")
	}
	if _, err := p.Feed("", []byte(`{"p":"/message/end_turn","o":"replace","v":true}`)); err != nil {
		t.Fatalf("feed end_turn: %v", err)
	}
	if !p.Terminal() {
		t.Fatalf("end_turn 后 Terminal() = false")
	}
	// error 帧也是终态（服务壳据此收束尾读宽限）。
	p2 := NewStreamParser()
	if _, err := p2.Feed("", []byte(`{"error":"Unusual activity detected"}`)); err != nil {
		t.Fatalf("feed error: %v", err)
	}
	if !p2.Terminal() {
		t.Fatalf("error 帧后 Terminal() = false")
	}
}
