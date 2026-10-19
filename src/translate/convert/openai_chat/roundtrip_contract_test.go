package openai_chat_test

// W41 ④（A2A-L19-W41-1）：三方言回环契约测试。
//
// 仿 GoModel `tests/contract/extra_content_roundtrip_test.go` 的 history-builder
// 三态：同一 thinking/signature fixture 经 chat（reasoning_details）/ responses
// （encrypted_content）/ anthropic（原生签名）三方言 history 回传，断言**上游请求
// 原样重放**（签名逐字出现在上游请求的原生载体位）+ 网关自有成员不得泄漏
// （chat 载体不进 claude/responses 请求体）。
//
// fixture：Claude 原生签名（E 信封；signature/sweep4_test.go 同构造法）——一个签名
// 绑定确切的思考文本，网关从 reasoning_content 文本重建块也造不出可用的签名，
// 必须逐字带回。
//
// 覆盖矩阵（3 history × 4 上游）：
//
//	               claude 上游          responses 上游        chat 上游               gemini 上游
//	chat history   thinking.signature   encrypted_content    reasoning_details.sig   不得泄漏（形态不合）
//	responses hist thinking.signature   encrypted_content    reasoning_details.sig   不得泄漏
//	anthropic hist thinking.signature   encrypted_content    reasoning_details.sig   不得泄漏
//
// 反证：回退任一读面（chat parse 的 reasoning_details / responses parse 的
// encrypted_content / claude parse 的 thinking.signature）→ 对应 history 列红；
// 回退 chat 回传轮载体重放（renderMessage 的 details 输出）→ chat 上游列红。

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/convert/claude_messages"
	"github.com/dwgx/any2api/translate/convert/gemini"
	"github.com/dwgx/any2api/translate/convert/openai_chat"
	"github.com/dwgx/any2api/translate/convert/responses"
	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/signature"
)

const (
	rtSignature = "EggKBggLEgKquxgB" // Claude 经典 E 信封（sweep4 同构造法）
	rtText      = "Let me think this through."
)

// historyBuilders 三方言 history：同一 fixture（思考文本 + 签名）的三种回传形态。
var historyBuilders = map[string]func() []byte{
	// chat：客户端按 OpenRouter 口径把上一轮 assistant 原样带回（reasoning_content
	// 文本 + reasoning_details 结构化条目）。
	"chat": func() []byte {
		return []byte(`{"model":"m","messages":[
			{"role":"user","content":"hi"},
			{"role":"assistant","content":"answer","reasoning_content":"` + rtText + `",
			 "reasoning_details":[{"type":"reasoning.text","text":"` + rtText + `","signature":"` + rtSignature + `","format":"anthropic-claude-v1","index":0}]},
			{"role":"user","content":"go on"}]}`)
	},
	// responses：reasoning item（summary 文本 + encrypted_content 签名）。
	"responses": func() []byte {
		return []byte(`{"model":"m","input":[
			{"role":"user","content":"hi"},
			{"type":"reasoning","summary":[{"type":"summary_text","text":"` + rtText + `"}],"encrypted_content":"` + rtSignature + `"},
			{"role":"user","content":"go on"}]}`)
	},
	// anthropic：原生 thinking 块（签名在块本身）。
	"anthropic": func() []byte {
		return []byte(`{"model":"m","max_tokens":16,"messages":[
			{"role":"user","content":"hi"},
			{"role":"assistant","content":[{"type":"thinking","thinking":"` + rtText + `","signature":"` + rtSignature + `"},{"type":"text","text":"answer"}]},
			{"role":"user","content":"go on"}]}`)
	},
}

func parseHistory(t *testing.T, dialect string) *ir.Request {
	t.Helper()
	body := historyBuilders[dialect]()
	var (
		req *ir.Request
		err error
	)
	switch dialect {
	case "chat":
		req, err = openai_chat.ParseRequest(body)
	case "responses":
		req, err = responses.ParseRequest(body)
	case "anthropic":
		req, err = claude_messages.ParseRequest(body)
	default:
		t.Fatalf("未知方言 %q", dialect)
	}
	if err != nil {
		t.Fatalf("%s ParseRequest: %v", dialect, err)
	}
	return req
}

// TestThreeDialectSignatureRoundTrip 主契约：三方言 history → 上游请求原样重放。
func TestThreeDialectSignatureRoundTrip(t *testing.T) {
	if _, ok := signature.ProviderForSignature(rtSignature); !ok {
		t.Fatal("fixture 签名失效——测试前提不成立")
	}

	for _, dialect := range []string{"chat", "responses", "anthropic"} {
		t.Run(dialect, func(t *testing.T) {
			req := parseHistory(t, dialect)

			t.Run("claude_上游_原生签名重放", func(t *testing.T) {
				out, err := claude_messages.RenderRequest(req)
				if err != nil {
					t.Fatalf("claude RenderRequest: %v", err)
				}
				if !strings.Contains(string(out), `"signature":"`+rtSignature+`"`) ||
					!strings.Contains(string(out), `"thinking":"`+rtText+`"`) {
					t.Fatalf("签名未在 claude 请求的 thinking 块原样重放:\n%s", out)
				}
				// 网关自有成员不得泄漏（GoModel 同款断言：extra_content 不进
				// Anthropic 形态；我们的等价物 = chat 载体/reasoning_content）。
				for _, leak := range []string{"reasoning_details", "reasoning_content", "encrypted_content"} {
					if strings.Contains(string(out), leak) {
						t.Fatalf("chat 载体成员 %q 泄漏进 claude 请求:\n%s", leak, out)
					}
				}
			})

			t.Run("responses_上游_encrypted_content重放", func(t *testing.T) {
				out, err := responses.RenderRequest(req)
				if err != nil {
					t.Fatalf("responses RenderRequest: %v", err)
				}
				var w struct {
					Input []struct {
						Type             string `json:"type"`
						EncryptedContent string `json:"encrypted_content"`
					} `json:"input"`
				}
				if err := json.Unmarshal(out, &w); err != nil {
					t.Fatalf("unmarshal: %v\n%s", err, out)
				}
				var got string
				for _, it := range w.Input {
					if it.Type == "reasoning" {
						got = it.EncryptedContent
					}
				}
				if got != rtSignature {
					t.Fatalf("encrypted_content = %q（want 签名原样重放）:\n%s", got, out)
				}
				if strings.Contains(string(out), "reasoning_details") {
					t.Fatalf("chat 载体泄漏进 responses 请求:\n%s", out)
				}
			})

			t.Run("chat_上游_载体重放", func(t *testing.T) {
				out, err := openai_chat.RenderRequest(req, openai_chat.RenderOptions{})
				if err != nil {
					t.Fatalf("chat RenderRequest: %v", err)
				}
				type chatMsg struct {
					Role             string `json:"role"`
					ReasoningContent string `json:"reasoning_content"`
					ReasoningDetails []struct {
						Type      string `json:"type"`
						Text      string `json:"text"`
						Signature string `json:"signature"`
						Format    string `json:"format"`
					} `json:"reasoning_details"`
				}
				var w struct {
					Messages []chatMsg `json:"messages"`
				}
				if err := json.Unmarshal(out, &w); err != nil {
					t.Fatalf("unmarshal: %v\n%s", err, out)
				}
				var assistant *chatMsg
				for i := range w.Messages {
					if w.Messages[i].Role == "assistant" {
						assistant = &w.Messages[i]
					}
				}
				if assistant == nil {
					t.Fatalf("chat 请求缺 assistant 消息:\n%s", out)
				}
				if len(assistant.ReasoningDetails) != 1 {
					t.Fatalf("reasoning_details = %+v（want 1 条载体重放）:\n%s", assistant.ReasoningDetails, out)
				}
				d := assistant.ReasoningDetails[0]
				if d.Type != "reasoning.text" || d.Text != rtText || d.Signature != rtSignature || d.Format != "anthropic-claude-v1" {
					t.Fatalf("载体条目 = %+v（签名/文本/format 必须原样）", d)
				}
				if assistant.ReasoningContent != rtText {
					t.Fatalf("reasoning_content 兼容通道 = %q", assistant.ReasoningContent)
				}
			})

			t.Run("gemini_上游_不合形态不泄漏", func(t *testing.T) {
				out, err := gemini.RenderRequest(req)
				if err != nil {
					t.Fatalf("gemini RenderRequest: %v", err)
				}
				if strings.Contains(string(out), rtSignature) {
					t.Fatalf("Claude 签名泄漏进 gemini 请求（形态不合不得重放）:\n%s", out)
				}
				// 思考文本保留（W6-1 口径：丢签名保文本）。
				if !strings.Contains(string(out), rtText) {
					t.Fatalf("思考文本丢失:\n%s", out)
				}
			})
		})
	}
}

// TestChatCarrierFullLoop 全环（写面→读面）：上游 thinking（带 Claude 签名）经
// chat 出口渲染（写面：reasoning_content + reasoning_details）→ 客户端把 assistant
// 消息原样回传（读面：载体 → IR 签名）→ 上游请求重放（claude 原生签名 / chat 载体）。
// 这是「GPT 腿回传轮整块丢签名」缺陷的端到端反证面。
func TestChatCarrierFullLoop(t *testing.T) {
	// 1) 上游（claude 形态）响应 → chat 出口渲染。
	resp := &ir.Response{ID: "chatcmpl-loop", Model: "m", Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
		{Kind: ir.BlockThinking, Text: rtText, Signature: rtSignature},
		{Kind: ir.BlockText, Text: "answer"},
	}}}
	rendered, err := openai_chat.RenderResponse(resp, openai_chat.RenderOptions{})
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	var rw struct {
		Choices []struct {
			Message json.RawMessage `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(rendered, &rw); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, rendered)
	}
	if !strings.Contains(string(rendered), `"signature":"`+rtSignature+`"`) {
		t.Fatalf("写面：响应缺签名载体:\n%s", rendered)
	}

	// 2) 客户端把 assistant 消息原样回传（chat 请求）。
	echo := []byte(`{"model":"m","messages":[
		{"role":"user","content":"hi"},
		` + string(rw.Choices[0].Message) + `,
		{"role":"user","content":"go on"}]}`)
	req, err := openai_chat.ParseRequest(echo)
	if err != nil {
		t.Fatalf("ParseRequest(echo): %v", err)
	}

	// 3) 上游重放：claude 原生签名 / chat 载体。
	claudeOut, err := claude_messages.RenderRequest(req)
	if err != nil {
		t.Fatalf("claude RenderRequest: %v", err)
	}
	if !strings.Contains(string(claudeOut), `"signature":"`+rtSignature+`"`) {
		t.Fatalf("全环断裂：回传的签名未重放到 claude 上游:\n%s", claudeOut)
	}
	chatOut, err := openai_chat.RenderRequest(req, openai_chat.RenderOptions{})
	if err != nil {
		t.Fatalf("chat RenderRequest: %v", err)
	}
	if !strings.Contains(string(chatOut), `"signature":"`+rtSignature+`"`) ||
		!strings.Contains(string(chatOut), `"reasoning_content":"`+rtText+`"`) {
		t.Fatalf("全环断裂：chat 上游未收到载体重放:\n%s", chatOut)
	}
}
