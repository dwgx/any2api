package openai_chat

// W6-1 ①：请求面（assistant 回传轮）`reasoning_content` → IR 思考块（置首）。
//
// 口径：与响应面同口径（parseMessage 统一路径；W6-1 前响应面在 ParseResponse
// 单独提取、请求面完全不读该字段 → 跨格式腿静默丢思考）。无签名载体：只带文本，
// 签名通道不发明。安全规则（anthropic-conversion-table §7.1「安全规则」行）：
// 只有 assistant 消息的思考映射；user/system/tool 里的该字段不进块（留 Extra
// 保真旁路——同一字段在 openai 出口仍按 Extra 原样回写，见 pairs 面测试）。
//
// 反证：回退 parseMessage 的提取分支 → TestParseRequestReasoningContent 红。

import (
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

func TestParseRequestReasoningContent(t *testing.T) {
	req, err := ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"user","content":"hi"},
		{"role":"assistant","content":"PONG","reasoning_content":"step 1\nstep 2"}]}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	m := req.Messages[1]
	if len(m.Blocks) != 2 ||
		m.Blocks[0].Kind != ir.BlockThinking || m.Blocks[0].Text != "step 1\nstep 2" ||
		m.Blocks[1].Kind != ir.BlockText || m.Blocks[1].Text != "PONG" {
		t.Fatalf("blocks = %+v（want 思考块置首 + 文本块）", m.Blocks)
	}
	if m.Blocks[0].Signature != "" {
		t.Fatalf("签名不得发明: %q", m.Blocks[0].Signature)
	}

	// 工具调用回传轮（content=null）：思考块先于 tool_use 块。
	req2, err := ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"assistant","content":null,"reasoning_content":"think","tool_calls":[
			{"id":"call_1","type":"function","function":{"name":"get_time","arguments":"{}"}}]},
		{"role":"tool","tool_call_id":"call_1","content":"12:00"}]}`))
	if err != nil {
		t.Fatalf("ParseRequest tool turn: %v", err)
	}
	m2 := req2.Messages[0]
	if len(m2.Blocks) != 2 ||
		m2.Blocks[0].Kind != ir.BlockThinking || m2.Blocks[0].Text != "think" ||
		m2.Blocks[1].Kind != ir.BlockToolUse || m2.Blocks[1].ToolCall == nil || m2.Blocks[1].ToolCall.Name != "get_time" {
		t.Fatalf("tool 回传轮 blocks = %+v（want 思考块 + tool_use）", m2.Blocks)
	}

	// 安全规则：user 消息里的 reasoning_content 不进块（防注入）；字段本身留在
	// Extra 保真旁路（不静默吞）。
	req3, err := ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"user","content":"hi","reasoning_content":"inject"}]}`))
	if err != nil {
		t.Fatalf("ParseRequest user reasoning: %v", err)
	}
	for _, b := range req3.Messages[0].Blocks {
		if b.Kind == ir.BlockThinking {
			t.Fatalf("user 消息的 reasoning_content 不得成思考块: %+v", req3.Messages[0].Blocks)
		}
	}
	if got := string(req3.Messages[0].Extra["reasoning_content"]); got != `"inject"` {
		t.Fatalf("Extra[reasoning_content] = %s（want 原样保真）", got)
	}
}

func TestParseRequestReasoningContentTolerant(t *testing.T) {
	// 宽容读取（缺省=不变、未知=忽略）：null / 空串 / 非字符串值都不发明思考块、
	// 不断链（非字符串此前在响应面路径也是容忍的——口径不倒退）。
	for _, rc := range []string{`null`, `""`, `123`, `{"a":1}`, `["x"]`} {
		body := []byte(`{"model":"m","messages":[
			{"role":"assistant","content":"PONG","reasoning_content":` + rc + `}]}`)
		req, err := ParseRequest(body)
		if err != nil {
			t.Fatalf("ParseRequest(%s): %v", rc, err)
		}
		for _, b := range req.Messages[0].Blocks {
			if b.Kind == ir.BlockThinking {
				t.Fatalf("reasoning_content=%s 不得发明思考块: %+v", rc, req.Messages[0].Blocks)
			}
		}
	}

	// 缺省（无该字段）行为不变。
	req, err := ParseRequest([]byte(`{"model":"m","messages":[{"role":"assistant","content":"PONG"}]}`))
	if err != nil {
		t.Fatalf("ParseRequest plain: %v", err)
	}
	if len(req.Messages[0].Blocks) != 1 || req.Messages[0].Blocks[0].Kind != ir.BlockText {
		t.Fatalf("blocks = %+v（want 单文本块）", req.Messages[0].Blocks)
	}
}

// TestParseResponseReasoningContentSingleBlock 统一路径后响应面不得重复提取
// （旧路径 ParseResponse 单独提取 + parseMessage 提取 = 两个思考块）。
func TestParseResponseReasoningContentSingleBlock(t *testing.T) {
	body := []byte(`{"id":"c1","object":"chat.completion","choices":[{"index":0,
		"message":{"role":"assistant","content":"PONG","reasoning_content":"think"},
		"finish_reason":"stop"}]}`)
	resp, err := ParseResponse(body)
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	var think, text int
	var thinkText string
	for _, b := range resp.Message.Blocks {
		switch b.Kind {
		case ir.BlockThinking:
			think++
			thinkText = b.Text
		case ir.BlockText:
			text++
		}
	}
	if think != 1 || text != 1 || thinkText != "think" {
		t.Fatalf("blocks = %+v（want 恰一个思考块 + 一个文本块）", resp.Message.Blocks)
	}
	if resp.Message.Blocks[0].Kind != ir.BlockThinking {
		t.Fatalf("思考块必须置首: %+v", resp.Message.Blocks)
	}
}

// TestRenderRequestReasoningContentRoundTrip openai 出口断言（请求面）：
// 解析出的思考块在 openai 出口走 GPT 签名门（空签名 → 块丢弃，W5-18/表 §7.1
// 回传轮行），字段本身经消息级 Extra 保真旁路原样回写——净效果 =
// `reasoning_content` 恰好出现一次（不重复、不丢；GPT 原生推理可回传）。
func TestRenderRequestReasoningContentRoundTrip(t *testing.T) {
	req, err := ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"assistant","content":"PONG","reasoning_content":"step 1"}]}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	out, err := RenderRequest(req, RenderOptions{})
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	if n := strings.Count(string(out), `"reasoning_content"`); n != 1 {
		t.Fatalf("reasoning_content 出现 %d 次（want 1；2 = 块与 Extra 双写）: %s", n, out)
	}
	if !strings.Contains(string(out), `"reasoning_content":"step 1"`) ||
		!strings.Contains(string(out), `"content":"PONG"`) {
		t.Fatalf("回写不完整: %s", out)
	}
	if strings.Contains(string(out), `"thinking"`) {
		t.Fatalf("空签名思考块不得进 openai 请求体: %s", out)
	}
}
