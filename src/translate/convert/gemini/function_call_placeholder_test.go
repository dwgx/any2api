package gemini

// W41 ①（A2A-L19-W41-1）：Gemini 3 无签名 functionCall 的官方占位符注入。
//
// 现状（修前）：整轮 functionCall 无签名 → 历史 functionCall 不带 thoughtSignature
// 发出——Gemini 3 族上游 400（MISSING_THOUGHT_SIGNATURE）。修后：requiresThoughtSignature
// 模型（Gemini 3 族）且**整轮** functionCall 无签名 → 每个 functionCall part 注入
// `skip_thought_signature_validator`（Google 官方文档的「历史来自别家模型」合法
// 表达；GoModel internal/providers/gemini/native.go:85,320-325 同款）。
//
// 判定面与边界：
//   - 模型判定 = gemini-<major> 世代 ≥ 3（geminiModelGeneration；GoModel
//     geminiGeneration 紧凑移植）；2.5 及以下/非 gemini 前缀模型零改动；
//   - 有一条 functionCall 带签名（并行批常见：只有首条带）→ 整轮原样发，
//     不掺占位符（GoModel 注释：真实 Gemini 轮）；
//   - 文本/thought part 的签名不走本门（W6-1 口径）；
//   - 响应面（RenderResponse）不注入（占位符只修**上游请求**的历史校验面）。
//
// 反证：把 renderModelParts 的 `case sigRequired && !turnSigned` 分支删掉 →
// TestGemini3PlaceholderInjection 红（Gemini 3 用例缺 thoughtSignature）。

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// toolTurnRequest 一轮 user + 带 tool_use 的 model 历史（gemini 出口形态）。
func toolTurnRequest(model string, calls ...ir.ContentBlock) *ir.Request {
	blocks := append([]ir.ContentBlock{{Kind: ir.BlockText, Text: "let me check"}}, calls...)
	return &ir.Request{Model: model, Messages: []ir.Message{
		{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "weather?"}}},
		{Role: ir.RoleAssistant, Blocks: blocks},
		{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "go on"}}},
	}}
}

func toolUseBlock(id, name, args, sig string) ir.ContentBlock {
	return ir.ContentBlock{Kind: ir.BlockToolUse, Signature: sig, ToolCall: &ir.ToolCall{ID: id, Name: name, Arguments: args}}
}

// modelParts 渲染并取出 model 轮的 parts。
func modelParts(t *testing.T, req *ir.Request) []map[string]json.RawMessage {
	t.Helper()
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	var w struct {
		Contents []struct {
			Role  string            `json:"role"`
			Parts []json.RawMessage `json:"parts"`
		} `json:"contents"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	if len(w.Contents) != 3 || w.Contents[1].Role != "model" {
		t.Fatalf("contents = %s（want user/model/user）", out)
	}
	parts := make([]map[string]json.RawMessage, 0, len(w.Contents[1].Parts))
	for _, p := range w.Contents[1].Parts {
		var pm map[string]json.RawMessage
		if err := json.Unmarshal(p, &pm); err != nil {
			t.Fatalf("part: %v", err)
		}
		parts = append(parts, pm)
	}
	return parts
}

func TestGemini3PlaceholderInjection(t *testing.T) {
	const wantPlaceholder = `"` + skipThoughtSignatureValidator + `"`

	t.Run("gemini3_无签名整轮_注入", func(t *testing.T) {
		parts := modelParts(t, toolTurnRequest("gemini-3-pro-preview",
			toolUseBlock("call_1", "get_weather", `{"city":"Paris"}`, "")))
		// parts = [text, functionCall]
		if len(parts) != 2 {
			t.Fatalf("parts = %v", parts)
		}
		if _, ok := parts[1]["functionCall"]; !ok {
			t.Fatalf("functionCall part 缺失: %v", parts[1])
		}
		if got := string(parts[1]["thoughtSignature"]); got != wantPlaceholder {
			t.Fatalf("thoughtSignature = %s, want %s", got, wantPlaceholder)
		}
		if _, leaked := parts[0]["thoughtSignature"]; leaked {
			t.Fatalf("text part 不得带占位符: %v", parts[0])
		}
	})

	t.Run("gemini3_并行批全无签名_每条都注入", func(t *testing.T) {
		parts := modelParts(t, toolTurnRequest("google/gemini-3-pro",
			toolUseBlock("call_1", "a", `{}`, ""),
			toolUseBlock("call_2", "b", `{}`, "")))
		for i, p := range parts[1:] {
			if got := string(p["thoughtSignature"]); got != wantPlaceholder {
				t.Fatalf("part %d thoughtSignature = %s, want %s", i+1, got, wantPlaceholder)
			}
		}
	})

	t.Run("gemini3_有一条带签名_整轮原样发", func(t *testing.T) {
		const sig = "CoUBAdHtim9Zf3Q4b2N0ZXQtc2lnbmF0dXJlLWZpeHR1cmUtZm9yLWNvbnRyYWN0LXRlc3RzAQ=="
		parts := modelParts(t, toolTurnRequest("gemini-3.5-flash",
			toolUseBlock("call_1", "a", `{}`, sig),
			toolUseBlock("call_2", "b", `{}`, "")))
		if got := string(parts[1]["thoughtSignature"]); got != jsonQuote(sig) {
			t.Fatalf("首条签名未原样回传: %s", got)
		}
		if got := string(parts[2]["thoughtSignature"]); got != "" {
			t.Fatalf("已签名轮次不得掺占位符: %s", got)
		}
	})

	t.Run("gemini2.5_零改动", func(t *testing.T) {
		parts := modelParts(t, toolTurnRequest("gemini-2.5-flash",
			toolUseBlock("call_1", "get_weather", `{"city":"Paris"}`, "")))
		if _, ok := parts[1]["thoughtSignature"]; ok {
			t.Fatalf("2.5 轮不得注入占位符: %v", parts[1])
		}
	})

	t.Run("非gemini前缀模型_零改动", func(t *testing.T) {
		parts := modelParts(t, toolTurnRequest("gemma-3-27b",
			toolUseBlock("call_1", "get_weather", `{}`, "")))
		if _, ok := parts[1]["thoughtSignature"]; ok {
			t.Fatalf("非 gemini 前缀不得注入: %v", parts[1])
		}
	})

	t.Run("响应面不注入", func(t *testing.T) {
		resp := &ir.Response{Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
			toolUseBlock("call_1", "get_weather", `{}`, ""),
		}}}
		out, err := RenderResponse(resp)
		if err != nil {
			t.Fatalf("RenderResponse: %v", err)
		}
		if got := string(out); strings.Contains(got, skipThoughtSignatureValidator) {
			t.Fatalf("响应面不得注入占位符: %s", got)
		}
	})
}

// TestGeminiModelGeneration geminiModelGeneration 世代解析（GoModel
// geminiGeneration 的移植面：前缀去段、主/次版本、非 gemini 前缀拒绝）。
func TestGeminiModelGeneration(t *testing.T) {
	cases := []struct {
		model        string
		major, minor int
		ok           bool
	}{
		{"gemini-3-pro-preview", 3, 0, true},
		{"gemini-3.8-flash", 3, 8, true},
		{"google/gemini-3-pro", 3, 0, true},
		{"gemini-2.5-flash-image", 2, 5, true},
		{"GEMINI-3-Pro", 3, 0, true},
		{"gemma-3-27b", 0, 0, false},
		{"imagen-3.0", 0, 0, false},
		{"", 0, 0, false},
		{"gemini-x", 0, 0, false},
		{"gemini-2.5.x", 0, 0, false},
	}
	for _, tc := range cases {
		major, minor, ok := geminiModelGeneration(tc.model)
		if major != tc.major || minor != tc.minor || ok != tc.ok {
			t.Fatalf("geminiModelGeneration(%q) = (%d,%d,%v), want (%d,%d,%v)",
				tc.model, major, minor, ok, tc.major, tc.minor, tc.ok)
		}
	}
	// requiresThoughtSignature 直接面。
	if !requiresThoughtSignature("gemini-3-pro-preview") || requiresThoughtSignature("gemini-2.5-pro") {
		t.Fatal("requiresThoughtSignature 判定面错")
	}
}
