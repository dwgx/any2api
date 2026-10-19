package gemini

// W6-1 ③：请求面 thought part 的签名门口径核对（gemini-conversion-table:82 括注
// 「我们的决策：扩展字段承载，缺签名即丢弃该块+日志」）。
//
// 判定：**该括注不成立，保持「保留文本、只按兼容判定决定是否带签名」**。
// 证据（全文见 research/briefs/reports/A2A-L19-W6-1.md）：
//   - 官方文档把签名校验限定在函数调用面（thought-signatures 页「When using
//     Gemini 4 and Gemini 3 models, you must pass back thought signatures during
//     function calling, otherwise you will get a validation error」；thinking 页
//     「it's *required* for function calling signatures」，其余 part 只是
//     「we recommend you always pass all signatures back」；2.5 系无函数调用时
//     任何 part 都无签名、「It is optional to return that part」）；
//   - CPA 源行为：`gemini_openai_request.go:203-208` 把 reasoning_content 直落
//     thought part（从不带签名、从不丢弃）；`gemini_sanitize.go` 对无签名 model
//     part 原样放行，带签名的文本 part 不兼容时只丢签名（「text parts cannot
//     carry signatures」）、不丢 part；
//   - 同表自证：§7.3 quirk 3 与陷阱表把 MISSING_THOUGHT_SIGNATURE 限定在工具调用
//     面（「工具调用被拒」）。
// 反证：把 renderModelParts 的 thinking 分支改成「无兼容签名即 continue」→
// TestRenderRequestThoughtSignatureGate 的「无签名/外来签名」两例红。

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/signature"
)

// geminiSigSample Gemini field-2 信封样本（Tink 载荷首字节 0x01；与
// signature/sweep4_test.go 同构造法）。
func geminiSigSample() string {
	return base64.StdEncoding.EncodeToString([]byte{0x12, 0x07, 0x0a, 0x05, 0x01, 0xde, 0xad, 0xbe, 0xef})
}

func TestRenderRequestThoughtSignatureGate(t *testing.T) {
	// 前置：样本判定如预期（否则测试前提失效）。
	if _, ok := signature.CompatibleSignatureForProvider(signature.ProviderGemini, geminiSigSample()); !ok {
		t.Fatal("样本不是 Gemini 兼容签名——测试前提失效")
	}
	foreign := base64.StdEncoding.EncodeToString([]byte{0x12, 0x02, 0xaa, 0xbb})
	if _, ok := signature.CompatibleSignatureForProvider(signature.ProviderGemini, foreign); ok {
		t.Fatal("外来样本被判 Gemini 兼容——测试前提失效")
	}

	cases := []struct {
		name    string
		sig     string
		wantSig string
	}{
		{"兼容签名 → 重放", geminiSigSample(), geminiSigSample()},
		{"外来签名 → 丢签名保文本", foreign, ""},
		{"无签名 → 保文本（表 82 括注判定不成立）", "", ""},
	}
	for _, tc := range cases {
		req := &ir.Request{Model: "m", Messages: []ir.Message{
			{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "hi"}}},
			{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
				{Kind: ir.BlockThinking, Text: "ponder", Signature: tc.sig},
				{Kind: ir.BlockText, Text: "answer"},
			}},
			// 尾随 user 轮：gemini 出口丢弃尾随 model content（无 prefill）。
			{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "go on"}}},
		}}
		out, err := RenderRequest(req)
		if err != nil {
			t.Fatalf("%s: RenderRequest: %v", tc.name, err)
		}
		var w struct {
			Contents []struct {
				Role  string            `json:"role"`
				Parts []json.RawMessage `json:"parts"`
			} `json:"contents"`
		}
		if err := json.Unmarshal(out, &w); err != nil {
			t.Fatalf("%s: unmarshal: %v\n%s", tc.name, err, out)
		}
		if len(w.Contents) != 3 || w.Contents[1].Role != "model" || len(w.Contents[1].Parts) != 2 {
			t.Fatalf("%s: contents = %s（want user/model/user；model 两 part）", tc.name, out)
		}
		var p0, p1 map[string]json.RawMessage
		if err := json.Unmarshal(w.Contents[1].Parts[0], &p0); err != nil {
			t.Fatalf("%s: part0: %v", tc.name, err)
		}
		if err := json.Unmarshal(w.Contents[1].Parts[1], &p1); err != nil {
			t.Fatalf("%s: part1: %v", tc.name, err)
		}
		if string(p0["thought"]) != "true" || string(p0["text"]) != `"ponder"` {
			t.Fatalf("%s: thought part 丢失/变形: %s", tc.name, w.Contents[1].Parts[0])
		}
		if got := string(p0["thoughtSignature"]); got != jsonQuote(tc.wantSig) {
			t.Fatalf("%s: thoughtSignature = %s, want %s", tc.name, got, jsonQuote(tc.wantSig))
		}
		if string(p1["text"]) != `"answer"` || p1["thought"] != nil {
			t.Fatalf("%s: 正文 part 变形: %s", tc.name, w.Contents[1].Parts[1])
		}
	}
}

func jsonQuote(s string) string {
	if s == "" {
		return ""
	}
	b, _ := json.Marshal(s)
	return string(b)
}
