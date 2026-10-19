// W5-25 条目 1（工具名清洗/还原层，P67 实锤缺陷）：r14 形态的端到端回归。
//
// 缺陷（P67 r3-r12）：客户端可发 84 字工具名（CC 的 MCP 工具实测）→ 上游按文档
// 校验（gemini/openai `≤64` 且字符集受限）整请求 400。修复（r7/r14 实测）=
// 请求侧截断 64 + 响应侧还原原名（74 字原名工具真执行）。
//
// 装置：真服务 + httptest 假上游（全本地回环）。三条腿：
//   - 非流式：openai 入口 → claude 上游（工具名进 `tools[].name`，响应 tool_use 回调）；
//   - 字符集：openai 入口 → claude 上游（空格/斜杠名字归一 + 还原）；
//   - 流式：claude 入口 → openai 上游（tool_calls 分片携带截断名 → 下游 SSE 带原名）。
//
// 反证/边界：同协议直通（claude 入口 → claude 上游）名字层不介入（直通字节保真）。
package service

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// longToolName 84 字客户端工具名（P67 实测形态）。
const longToolName = "mcp__p67__p67_xxx_" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestToolNameSanitizeLongNameNonStream(t *testing.T) {
	if len(longToolName) != 84 {
		t.Fatalf("测试构造错误：len=%d", len(longToolName))
	}
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// 上游按**截断名**回调（r14 形态：模型看到的声明是截断名）。
		san := longToolName[:64]
		_, _ = w.Write([]byte(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-x",` +
			`"content":[{"type":"tool_use","id":"toolu_1","name":"` + san + `","input":{"text":"P67-ECHO-OK"}}],` +
			`"stop_reason":"tool_use","stop_sequence":null,"usage":{"input_tokens":5,"output_tokens":7}}`))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 0))

	reqBody := `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],` +
		`"tools":[{"type":"function","function":{"name":"` + longToolName + `","description":"d",` +
		`"parameters":{"type":"object","properties":{}}}}]}`
	resp := postJSON(t, srv.URL+"/v1/chat/completions", reqBody, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d body=%s", resp.StatusCode, raw)
	}

	// 上游侧：名字已截断到 64（P67 r7 的「sent name len 84→64」）。
	_, upBody, _ := up.request()
	var upReq struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(upBody, &upReq); err != nil {
		t.Fatalf("上游请求不是 JSON: %v\n%s", err, upBody)
	}
	if len(upReq.Tools) != 1 {
		t.Fatalf("上游工具数 = %d（want 1）\n%s", len(upReq.Tools), upBody)
	}
	if upReq.Tools[0].Name != longToolName[:64] {
		t.Fatalf("上游工具名 = %q（want 截断 64 = %q）", upReq.Tools[0].Name, longToolName[:64])
	}

	// 下游侧：名字已还原成 84 字原名（r14 的客户端可执行前提）。
	var down struct {
		Choices []struct {
			Message struct {
				ToolCalls []struct {
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(raw, &down); err != nil {
		t.Fatalf("下游响应不是 JSON: %v\n%s", err, raw)
	}
	if len(down.Choices) != 1 || len(down.Choices[0].Message.ToolCalls) != 1 {
		t.Fatalf("下游 tool_calls 形状 = %s", raw)
	}
	got := down.Choices[0].Message.ToolCalls[0].Function.Name
	if got != longToolName {
		t.Fatalf("下游工具名 = %q（want 原名 %q——响应侧还原失效）", got, longToolName)
	}
	if args := down.Choices[0].Message.ToolCalls[0].Function.Arguments; args != `{"text":"P67-ECHO-OK"}` {
		t.Fatalf("参数被改动：%q（名字层绝不碰参数）", args)
	}
}

// TestToolNameSanitizeIllegalCharsNonStream：字符集归一（空格/斜杠）也双向——
// 上游收到合法名、下游还原原名。
func TestToolNameSanitizeIllegalCharsNonStream(t *testing.T) {
	const orig = "read file /tmp/x"
	const san = "read_file__tmp_x"
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-x",` +
			`"content":[{"type":"tool_use","id":"toolu_1","name":"` + san + `","input":{}}],` +
			`"stop_reason":"tool_use","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}`))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 0))
	reqBody := `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],` +
		`"tools":[{"type":"function","function":{"name":"` + orig + `","parameters":{"type":"object"}}}]}`
	resp := postJSON(t, srv.URL+"/v1/chat/completions", reqBody, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d body=%s", resp.StatusCode, raw)
	}
	_, upBody, _ := up.request()
	if !strings.Contains(string(upBody), `"name":"`+san+`"`) || strings.Contains(string(upBody), orig) {
		t.Fatalf("上游请求未净化：%s", upBody)
	}
	if !strings.Contains(string(raw), `"name":"`+orig+`"`) {
		t.Fatalf("下游未还原原名：%s", raw)
	}
}

// TestToolNameSanitizeStream：流式面还原（claude 入口 → openai 上游，tool_calls
// 分片携带截断名 → 下游 anthropic SSE 的 content_block_start 带原名）。
func TestToolNameSanitizeStream(t *testing.T) {
	var upBody []byte
	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		upBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		san := longToolName[:64]
		sseFrame(w, "", `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"`+san+`","arguments":""}}]},"finish_reason":null}]}`)
		sseFrame(w, "", `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"text\":\"hi\"}"}}]},"finish_reason":null}]}`)
		sseFrame(w, "", `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`)
		sseFrame(w, "", `[DONE]`)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))

	reqBody := `{"model":"claude-x","max_tokens":100,"stream":true,"messages":[{"role":"user","content":"hi"}],` +
		`"tools":[{"name":"` + longToolName + `","description":"d","input_schema":{"type":"object","properties":{}}}]}`
	resp := postJSON(t, srv.URL+"/v1/messages", reqBody, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d body=%s", resp.StatusCode, raw)
	}
	// 上游侧：截断名（openai chat 嵌套信封）。
	if !strings.Contains(string(upBody), longToolName[:64]) || strings.Contains(string(upBody), longToolName) {
		t.Fatalf("上游请求未净化：%s", upBody)
	}
	// 下游侧：content_block_start 的 tool_use 带原名。
	body := string(raw)
	if !strings.Contains(body, `"type":"tool_use"`) {
		t.Fatalf("下游流无 tool_use 块：%s", body)
	}
	if !strings.Contains(body, `"name":"`+longToolName+`"`) {
		t.Fatalf("下游流式未还原原名（r14 客户端认不出工具）：%s", body)
	}
}

// TestToolNameSanitizeSameProtocolUntouched：同协议直通（claude 入口 → claude
// 上游）不走转换器 → 名字层不介入（直通字节保真；架构决策 1 的直通优先）。
// 这是「本层只活在跨格式腿」的边界锚点。
func TestToolNameSanitizeSameProtocolUntouched(t *testing.T) {
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-x",` +
			`"content":[{"type":"tool_use","id":"toolu_1","name":"` + longToolName + `","input":{}}],` +
			`"stop_reason":"tool_use","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}`))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 0))
	reqBody := `{"model":"claude-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}],` +
		`"tools":[{"name":"` + longToolName + `","input_schema":{"type":"object"}}]}`
	resp := postJSON(t, srv.URL+"/v1/messages", reqBody, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d body=%s", resp.StatusCode, raw)
	}
	_, upBody, _ := up.request()
	if !strings.Contains(string(upBody), longToolName) {
		t.Fatalf("同协议直通被改写（直通优先纪律）：%s", upBody)
	}
	if !strings.Contains(string(raw), longToolName) {
		t.Fatalf("同协议下游被改写：%s", raw)
	}
}
