package pairs

// W5-37：跨格式腿（claude 入口 ← openai 上游）思考面锚点——
// 上游 `message.reasoning_content` 经解析映射为思考块，claude 响应面落地为
// `{"type":"thinking",...,"signature":""}`（响应面不受签名门；签名不发明）。
// 修前该字段被解析侧丢弃：claude 下游只看到正文。

import (
	"encoding/json"
	"testing"
)

func TestOpenAIReasoningToClaudeResponseFace(t *testing.T) {
	reg := NewRegistry()
	in := []byte(`{"id":"c1","object":"chat.completion","created":1,"model":"m",
		"choices":[{"index":0,"message":{"role":"assistant","content":"PONG","reasoning_content":"step 1"},"finish_reason":"stop"}],
		"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`)
	out, err := Convert(reg, fOpenAI, fClaude, FaceResponse, in, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	var wire struct {
		Content []struct {
			Type      string `json:"type"`
			Thinking  string `json:"thinking"`
			Signature string `json:"signature"`
			Text      string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(out, &wire); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	if len(wire.Content) != 2 || wire.Content[0].Type != "thinking" ||
		wire.Content[0].Thinking != "step 1" || wire.Content[1].Type != "text" || wire.Content[1].Text != "PONG" {
		t.Fatalf("claude 响应内容 = %s", out)
	}
	if wire.Content[0].Signature != "" {
		t.Fatalf("签名不得发明: %s", out)
	}
}
