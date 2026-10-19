package pairs

// W6-1 ①+③：回传轮 `reasoning_content` 的三出口落法（跨格式腿）。
//
// 请求：openai 客户端回传 assistant 历史带 `reasoning_content`（无签名载体——
// OpenAI 线没有签名字段）。
//   - claude 出口：思考块**丢弃**（anthropic-conversion-table §7.1「OpenAI（任一侧）
//     → Anthropic（无签名）」行：不能凭空造签名；claude 上游拒空/外来签名）；
//     字段本身也绝不出现在 claude 消息对象（openaiOriginMessageExtras 过滤）。
//   - gemini 出口：thought part **保留文本**（gemini-conversion-table §请求侧
//     「assistant 的 thinking」行直迁；签名门口径见 gemini/render.go 注释——表 82
//     括注「缺签名即丢弃该块」经 W6-1 核对判不成立：官方文档把签名校验限定在
//     functionCall 面，CPA 源行为同样保留无签名 thought 文本）。
//   - openai 出口：同格式（registry 显式报无转换；服务壳走直通腿），出口面由
//     openai_chat 包内往返测试锁定（TestRenderRequestReasoningContentRoundTrip）。

import (
	"encoding/json"
	"strings"
	"testing"
)

const w6Req = `{"model":"m","messages":[
	{"role":"user","content":"hi"},
	{"role":"assistant","content":"PONG","reasoning_content":"step 1"},
	{"role":"user","content":"again"}]}`

// TestW6RequestReasoningToClaude 回传轮 reasoning_content → claude 出口：
// 无签名 → 整块丢弃（表 §7.1），正文保留，字段不泄漏。
func TestW6RequestReasoningToClaude(t *testing.T) {
	reg := NewRegistry()
	out, err := Convert(reg, fOpenAI, fClaude, FaceRequest, []byte(w6Req), Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	s := string(out)
	for _, banned := range []string{"reasoning_content", "step 1", "thinking"} {
		if strings.Contains(s, banned) {
			t.Fatalf("claude 请求体不得含 %q（无签名思考块整块丢弃）: %s", banned, s)
		}
	}
	if !strings.Contains(s, `"content":"PONG"`) {
		t.Fatalf("正文丢失: %s", s)
	}
	var w struct {
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	// 单 text 块 → content 回落字符串形态（renderContent forceArray=false）。
	if len(w.Messages) != 3 || string(w.Messages[1].Content) != `"PONG"` {
		t.Fatalf("assistant 消息 = %s（want 仅正文）", out)
	}
}

// TestW6RequestReasoningToGemini 回传轮 reasoning_content → gemini 出口：
// thought part 置首（先于 text part）、无 thoughtSignature 字段（不发明签名）、
// 文本保留（表 82 括注判定不成立——见文件头与 gemini/render.go 注释）。
func TestW6RequestReasoningToGemini(t *testing.T) {
	reg := NewRegistry()
	out, err := Convert(reg, fOpenAI, fGemini, FaceRequest, []byte(w6Req), Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if strings.Contains(string(out), "thoughtSignature") {
		t.Fatalf("不得发明 thoughtSignature: %s", out)
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
	if len(w.Contents) != 3 || w.Contents[1].Role != "model" || len(w.Contents[1].Parts) != 2 {
		t.Fatalf("contents = %s（want user/model/user；model 两 part）", out)
	}
	var p0, p1 map[string]json.RawMessage
	if err := json.Unmarshal(w.Contents[1].Parts[0], &p0); err != nil {
		t.Fatalf("part0: %v", err)
	}
	if err := json.Unmarshal(w.Contents[1].Parts[1], &p1); err != nil {
		t.Fatalf("part1: %v", err)
	}
	if string(p0["thought"]) != "true" || string(p0["text"]) != `"step 1"` {
		t.Fatalf("part0 = %s（want thought part 置首）", w.Contents[1].Parts[0])
	}
	if string(p1["text"]) != `"PONG"` || p1["thought"] != nil {
		t.Fatalf("part1 = %s（want text part 紧随）", w.Contents[1].Parts[1])
	}
}
