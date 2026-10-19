// chatgpt_web（上游叶子格式）转换对的直转面测试（W30-2 S3 接线）。
//
// 覆盖：
//   - 请求面：openai_chat / claude_messages 入口 → chatgpt_web 体（IR 渲染）；
//   - 响应面：chatgpt_web 流 → openai_chat / claude_messages 出口（golden 回放）；
//   - 增量面：StreamPipe(chatgpt_web → openai_chat) 逐帧进、逐帧出（relay 内核）；
//   - 叶子面：入口/出口方向的非法面显式报错（ErrUnsupportedFace）。
//
// 帧体 = chatgpt_web 包 testdata 的实抓 golden 回放（W29-1）。
//
// 反证锚点：删掉 NewRegistry 的 chatgpt_web 显式对 → 本文件在 Lookup 处红
// （枢纽兜底也救不了：chatgpt_web 不是枢纽格式，两条腿都缺）。
package pairs

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/convert/chatgpt_web"
	"github.com/dwgx/any2api/translate/registry"
)

// chatGPTWebGolden 读 chatgpt_web 包 testdata 的 golden SSE（原样字节）。
func chatGPTWebGolden(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile("../convert/chatgpt_web/testdata/" + name)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	return string(raw)
}

// chatGPTWebSplitFrames golden SSE → (event, data) 帧对（空块/注释跳过）。
func chatGPTWebSplitFrames(t *testing.T, sse string) [][2]string {
	t.Helper()
	var frames [][2]string
	for _, block := range strings.Split(sse, "\n\n") {
		var event, data string
		for _, line := range strings.Split(block, "\n") {
			switch {
			case strings.HasPrefix(line, "event:"):
				event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			case strings.HasPrefix(line, "data:"):
				data = strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " ")
			}
		}
		if data != "" {
			frames = append(frames, [2]string{event, data})
		}
	}
	if len(frames) == 0 {
		t.Fatalf("golden 未切出任何帧")
	}
	return frames
}

// TestChatGPTWebFromOpenAIRequest openai_chat 入口请求 → chatgpt_web 上游体
// （W29-1 §2.1 样本形状：snake_case supported_encodings / 最小消息结构）。
func TestChatGPTWebFromOpenAIRequest(t *testing.T) {
	reg := NewRegistry()
	body := `{"model":"gpt-6","messages":[{"role":"user","content":"Reply with exactly: PONG"}],"stream":true}`
	out, err := Convert(reg, registry.FormatOpenAIChat, FormatChatGPTWeb, FaceRequest, []byte(body), Options{})
	if err != nil {
		t.Fatalf("Convert(request): %v", err)
	}
	var w struct {
		Action             string   `json:"action"`
		Model              string   `json:"model"`
		ParentMessageID    *string  `json:"parentMessageId"`
		ConversationID     *string  `json:"conversationId"`
		SupportedEncodings []string `json:"supported_encodings"`
		ForceUseSse        bool     `json:"forceUseSse"`
		Messages           []struct {
			Author struct {
				Role string `json:"role"`
			} `json:"author"`
			Content struct {
				ContentType string   `json:"content_type"`
				Parts       []string `json:"parts"`
			} `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("体: %v\n%s", err, out)
	}
	if w.Action != "next" || w.Model != "gpt-6" || w.ParentMessageID != nil || w.ConversationID != nil || !w.ForceUseSse {
		t.Fatalf("顶层字段 = %+v", w)
	}
	if len(w.SupportedEncodings) != 1 || w.SupportedEncodings[0] != "v1" {
		t.Fatalf("supported_encodings = %v", w.SupportedEncodings)
	}
	if len(w.Messages) != 1 || w.Messages[0].Author.Role != "user" ||
		w.Messages[0].Content.ContentType != "text" ||
		len(w.Messages[0].Content.Parts) != 1 || w.Messages[0].Content.Parts[0] != "Reply with exactly: PONG" {
		t.Fatalf("messages = %+v", w.Messages)
	}
	// 模型缺省 → "auto"（样本值）；camelCase 死字段绝不出现。
	out2, err := Convert(reg, registry.FormatOpenAIChat, FormatChatGPTWeb, FaceRequest,
		[]byte(`{"messages":[{"role":"user","content":"hi"}]}`), Options{})
	if err != nil {
		t.Fatalf("Convert(no model): %v", err)
	}
	var m struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(out2, &m)
	if m.Model != "auto" {
		t.Fatalf("空模型 = %q, want auto", m.Model)
	}
	if strings.Contains(string(out2), "supportedEncodings") {
		t.Fatalf("不得带 camelCase 死字段:\n%s", out2)
	}
}

// TestChatGPTWebFromClaudeMessagesRequest claude_messages 入口（多轮 + system +
// 工具历史）→ chatgpt_web 体：全量历史逐条进 messages[]，role 逐字透传，工具
// 块渲染为标签文本（保真）。
func TestChatGPTWebFromClaudeMessagesRequest(t *testing.T) {
	reg := NewRegistry()
	body := `{"model":"claude-sonnet-5-5","max_tokens":1024,
		"system":"Be terse.",
		"messages":[
			{"role":"user","content":"read it"},
			{"role":"assistant","content":[
				{"type":"text","text":"reading"},
				{"type":"tool_use","id":"toolu_1","name":"Read","input":{"path":"a.txt"}}
			]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"hello"}]}
		]}`
	out, err := Convert(reg, registry.FormatClaudeMessages, FormatChatGPTWeb, FaceRequest, []byte(body), Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	var w struct {
		Model    string `json:"model"`
		Messages []struct {
			Author struct {
				Role string `json:"role"`
			} `json:"author"`
			Content struct {
				Parts []string `json:"parts"`
			} `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("体: %v\n%s", err, out)
	}
	if w.Model != "claude-sonnet-5-5" {
		t.Fatalf("model = %q", w.Model)
	}
	if len(w.Messages) != 4 {
		t.Fatalf("messages 数 = %d（want 4：system+user+assistant+user 全量历史）\n%s", len(w.Messages), out)
	}
	roles := []string{w.Messages[0].Author.Role, w.Messages[1].Author.Role, w.Messages[2].Author.Role, w.Messages[3].Author.Role}
	wantRoles := []string{"system", "user", "assistant", "tool"}
	for i := range roles {
		if roles[i] != wantRoles[i] {
			t.Fatalf("roles = %v, want %v（role 逐字透传——claude 的 tool_result 消息在 IR 里是 role=tool）", roles, wantRoles)
		}
	}
	if got := w.Messages[0].Content.Parts[0]; got != "Be terse." {
		t.Fatalf("system 文本 = %q", got)
	}
	// 工具历史保真：assistant 段含 tool_use 标签文本；tool_result 段含结果文本。
	if got := w.Messages[2].Content.Parts[0]; !strings.Contains(got, `<tool_use id="toolu_1" name="Read">`) ||
		!strings.Contains(got, `"path":"a.txt"`) {
		t.Fatalf("assistant 工具历史 = %q", got)
	}
	if got := w.Messages[3].Content.Parts[0]; got != `<tool_result tool_use_id="toolu_1">hello</tool_result>` {
		t.Fatalf("tool_result 历史 = %q", got)
	}
}

// TestChatGPTWebToOpenAIStream golden → openai_chat 出口（实抓回放）。
func TestChatGPTWebToOpenAIStream(t *testing.T) {
	reg := NewRegistry()
	sse := chatGPTWebGolden(t, "anon_v1_direct.sse.txt")
	out, err := Convert(reg, FormatChatGPTWeb, registry.FormatOpenAIChat, FaceStream, []byte(sse), Options{})
	if err != nil {
		t.Fatalf("Convert(stream): %v", err)
	}
	text, finish, done := scanOpenAIStream(string(out))
	if text != "1\n2\n3\n4\n5\n6\n7\n8\n9\n10" || finish != "stop" || !done {
		t.Fatalf("出口 = text=%q finish=%q done=%v\n%s", text, finish, done, out)
	}
}

// TestChatGPTWebToClaudeMessagesStream golden（ISP 份——含 D1 recap 帧）→
// claude_messages 出口：正文 PONG、stop_reason end_turn。
func TestChatGPTWebToClaudeMessagesStream(t *testing.T) {
	reg := NewRegistry()
	sse := chatGPTWebGolden(t, "anon_v1_isp.sse.txt")
	out, err := Convert(reg, FormatChatGPTWeb, registry.FormatClaudeMessages, FaceStream, []byte(sse), Options{})
	if err != nil {
		t.Fatalf("Convert(stream): %v", err)
	}
	s := string(out)
	if !strings.Contains(s, `"text":"PONG"`) {
		t.Fatalf("出口缺正文 PONG:\n%s", s)
	}
	if !strings.Contains(s, "message_stop") || !strings.Contains(s, `"stop_reason":"end_turn"`) {
		t.Fatalf("出口缺收尾/stop_reason:\n%s", s)
	}
	if strings.Contains(s, "Worked for a second") {
		t.Fatalf("recap 泄漏:\n%s", s)
	}
}

// TestChatGPTWebStreamPipeIncremental StreamPipe(chatgpt_web → openai_chat)：
// 逐帧 Feed（relay 内核路径）+ Finish——与批量面同结果。
func TestChatGPTWebStreamPipeIncremental(t *testing.T) {
	pipe, err := NewStreamPipe(NewRegistry(), FormatChatGPTWeb, registry.FormatOpenAIChat, Options{})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	frames := chatGPTWebSplitFrames(t, chatGPTWebGolden(t, "anon_snapshot.sse.txt"))
	var out strings.Builder
	for _, f := range frames {
		b, err := pipe.Feed(f[0], []byte(f[1]))
		if err != nil {
			t.Fatalf("Feed(%q, %.80s): %v", f[0], f[1], err)
		}
		out.Write(b)
	}
	tail, err := pipe.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	out.Write(tail)
	text, finish, done := scanOpenAIStream(out.String())
	if text != "PONG" || finish != "stop" || !done {
		t.Fatalf("管道出口 = text=%q finish=%q done=%v\n%s", text, finish, done, out.String())
	}
}

// TestChatGPTWebUnsupportedFaces 上游叶子面：入口方向（parseRequest）与出口
// 方向（renderResponse/renderStream/renderError）显式报错，绝不返回半成品字节。
func TestChatGPTWebUnsupportedFaces(t *testing.T) {
	reg := NewRegistry()
	if _, err := Convert(reg, FormatChatGPTWeb, registry.FormatOpenAIChat, FaceRequest, []byte(`{}`), Options{}); err == nil {
		t.Fatalf("chatgpt_web 入口方向（FaceRequest parse）应显式报错")
	}
	if _, err := Convert(reg, registry.FormatOpenAIChat, FormatChatGPTWeb, FaceResponse, []byte(`{}`), Options{}); err == nil {
		t.Fatalf("chatgpt_web 出口方向（FaceResponse render）应显式报错")
	}
	// 叶子包自身的哨兵（errors.Is 可判——接线层不发明第二套错误）。
	if _, err := chatgpt_web.ParseRequest([]byte(`{}`)); err == nil || !strings.Contains(err.Error(), "face not supported") {
		t.Fatalf("ParseRequest err = %v", err)
	}
	if _, err := chatgpt_web.RenderResponse(nil); err == nil || !strings.Contains(err.Error(), "face not supported") {
		t.Fatalf("RenderResponse err = %v", err)
	}
	if _, err := chatgpt_web.RenderStream(nil); err == nil || !strings.Contains(err.Error(), "face not supported") {
		t.Fatalf("RenderStream err = %v", err)
	}
	if _, err := chatgpt_web.RenderError(nil); err == nil || !strings.Contains(err.Error(), "face not supported") {
		t.Fatalf("RenderError err = %v", err)
	}
}

// TestChatGPTWebRegistryPairs 显式对登记面：四入口 ↔ chatgpt_web 双向 1 步直转
// （Quality=good），且不冒充枢纽。
func TestChatGPTWebRegistryPairs(t *testing.T) {
	reg := NewRegistry()
	entries := []registry.Format{
		registry.FormatOpenAIChat, registry.FormatClaudeMessages,
		registry.FormatGeminiGenerate, registry.FormatOpenAIResponses,
	}
	for _, f := range entries {
		for _, dir := range [][2]registry.Format{{f, FormatChatGPTWeb}, {FormatChatGPTWeb, f}} {
			plan, err := reg.Lookup(dir[0], dir[1])
			if err != nil {
				t.Fatalf("Lookup(%s→%s): %v", dir[0], dir[1], err)
			}
			if len(plan.Steps) != 1 || plan.Quality != registry.QualityGood {
				t.Fatalf("Lookup(%s→%s) = %+v（want 1 步 good）", dir[0], dir[1], plan)
			}
		}
	}
}

// scanOpenAIStream 从 openai chat SSE 字节提取 (正文, finish_reason, [DONE])。
func scanOpenAIStream(s string) (content, finish string, done bool) {
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		if payload == "[DONE]" {
			done = true
			continue
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
		}
		if json.Unmarshal([]byte(payload), &chunk) != nil {
			continue
		}
		for _, c := range chunk.Choices {
			b.WriteString(c.Delta.Content)
			if c.FinishReason != nil {
				finish = *c.FinishReason
			}
		}
	}
	return b.String(), finish, done
}
