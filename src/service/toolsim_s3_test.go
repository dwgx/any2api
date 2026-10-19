// W18-1（A2A-L19-W18-1）工具模拟 S3：**服务壳置位** + 假上游全环 e2e。
//
// 装置（照 W10-1 服务级 e2e 范式）：httptest 假 claude_web 上游（3 端点：
// GET /api/organizations → POST …/chat_conversations → POST …/completion）+
// 真服务（New(cfg).Handler() 经 httptest.NewServer）+ 真 HTTP 客户端。
//
// 覆盖（设计单 research/topics/claude-web-tool-sim/README.md §S3）：
//
//   - 置位条件（三条件合取，服务壳 forward）：本台上游 tool_sim 开 &&
//     protocol = claude_web && 入口请求带 tools；
//   - ① 注入：上游 completion 体 prompt **最前**含注入段（契约句 + 工具名/描述），
//     且上游 `tools` 字段仍 web_search/artifacts/repl 三元组（W11-3 契约不破）；
//   - ② 合成：假上游回放含契约标签的文本流 → 下游 claude 面收到合成 `tool_use`
//     块（+ input_json_delta 参数）与 `stop_reason: tool_use`（流式 + 非流式两面）；
//   - ③ 三形态（开关关 / 上游非 claude_web / 入口无 tools）= 行为与现状逐字节一致
//     （上游体与下游体双双逐字节比较；sim 开但条件不全时零差异）。
//
// 反证锚点（本文件内，逐条实跑「回退即红」见报告 §5）：删 forward 的置位 gate →
// TestE2EClaudeWebToolSimLoop 在「prompt 无注入段」处红；删 `dr.entryTools` 探测
// → 入口无 tools 形态的字节相等断言红（出口解析器误扫标签）。
package service

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// 素材：入口体（claude_messages 形态）/ 假上游 DSL 回放流 / 配置
// ---------------------------------------------------------------------------

// toolsimTools 入口声明的客户端工具（CC 形态：name/description/input_schema）。
const toolsimTools = `[{"name":"Read","description":"Read a file","input_schema":{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}}]`

// toolsimEntryBody claude_messages 入口体（tools 是否带由 withTools 决定）。
func toolsimEntryBody(withTools, stream bool) string {
	tools := ""
	if withTools {
		tools = `"tools":` + toolsimTools + `,`
	}
	streamField := ""
	if stream {
		streamField = `"stream":true,`
	}
	return `{"model":"claude-sonnet-5-5","max_tokens":32000,` + streamField + tools +
		`"messages":[{"role":"user","content":"read a.go"}]}`
}

// claudeWebDSLSSE 假上游回放的 DSL 流：模型按注入契约输出一个工具调用
// （文本 + 标签；标签**跨两帧切割**——顺带覆盖跨 chunk 缓冲的服务级形态）。
// 帧形逐字照实抓 fixture（run1）：message_start → content_block_start →
// content_block_delta ×2 → content_block_stop → message_delta → message_stop。
func claudeWebDSLSSE() string {
	var b strings.Builder
	frame := func(event, data string) {
		b.WriteString("event: " + event + "\ndata: " + data + "\n\n")
	}
	frame("message_start", `{"type":"message_start","message":{"id":"chatcompl_toolsim","type":"message","role":"assistant","model":"claude-sonnet-5-5","content":[],"stop_reason":null,"stop_sequence":null}}`)
	frame("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
	frame("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"I'll read the file.\n<tool"}}`)
	frame("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"_use name=\"Read\">{\"file_path\":\"a.go\"}</tool_use>"}}`)
	frame("content_block_stop", `{"type":"content_block_stop","index":0}`)
	frame("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null}}`)
	frame("message_stop", `{"type":"message_stop"}`)
	return b.String()
}

// cfgClaudeWebToolSim claude_web 上游 + tool_sim 开关（其余同 cfgClaudeWeb；
// orgID 空 = 走 org 发现——三端点调用序与 W10-1 同）。
func cfgClaudeWebToolSim(baseURL, orgID string, toolSim bool) *Config {
	c := cfgClaudeWeb(baseURL, orgID)
	c.Upstreams[0].ToolSim = toolSim
	return c
}

// ---------------------------------------------------------------------------
// 回收面：一发 claude_web 全环请求 →（上游 completion 体, 下游体, 状态码）
// ---------------------------------------------------------------------------

func toolSimRunClaudeWeb(t *testing.T, toolSim bool, entryBody, upstreamStream string) (upBody, downBody []byte, status int) {
	t.Helper()
	up := newClaudeWebUpstream(t)
	up.streamBody = upstreamStream
	srv := startService(t, cfgClaudeWebToolSim(up.srv.URL, "", toolSim))

	resp := postJSON(t, srv.URL+"/v1/messages", entryBody, nil)
	defer resp.Body.Close()
	downBody, _ = io.ReadAll(resp.Body)
	status = resp.StatusCode
	// 四调用 = 三端点 + W13 清理面 DELETE（异步——等它到达，避免竞态）。
	calls := waitForClaudeWebCalls(t, up, 4)
	for _, c := range calls {
		if c.method == http.MethodPost && strings.HasSuffix(c.path, "/completion") {
			upBody = c.body
		}
	}
	if upBody == nil {
		t.Fatalf("上游无 completion 调用: %v", callPaths(calls))
	}
	return upBody, downBody, status
}

// completionBody 解析上游 completion 体（prompt + tools）。
type completionBody struct {
	Prompt string           `json:"prompt"`
	Tools  []map[string]any `json:"tools"`
}

func decodeCompletionBody(t *testing.T, body []byte) completionBody {
	t.Helper()
	var out completionBody
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("completion 体: %v\n%s", err, body)
	}
	return out
}

// maskVolatile 掩掉跨运行不稳定的字节（仅用于**逐字节比较**；其余字节逐字节保留）：
//   - uuid：completion 体的 turn_message_uuids/completion_request_id 每请求新生成；
//   - toolu_ 开头的合成块 id（出口 hygiene 派生——本测试里其实确定，掩掉作保险）。
var (
	uuidMaskRe   = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	toolIDMaskRe = regexp.MustCompile(`toolu_[A-Za-z0-9_]+`)
)

func maskVolatile(b []byte) []byte {
	b = uuidMaskRe.ReplaceAll(b, []byte("<UUID>"))
	return toolIDMaskRe.ReplaceAll(b, []byte("toolu_<ID>"))
}

// ---------------------------------------------------------------------------
// ① 置位 + ② 全环（流式）：注入进上游 prompt 最前；标签 → 下游 tool_use 块
// ---------------------------------------------------------------------------

func TestE2EClaudeWebToolSimLoop(t *testing.T) {
	upBody, down, status := toolSimRunClaudeWeb(t, true, toolsimEntryBody(true, true), claudeWebDSLSSE())
	if status != 200 {
		t.Fatalf("status=%d body=\n%s", status, down)
	}

	// ---- ① 请求面：注入段在 prompt 最前（契约句 + 工具清单），历史照旧拍平。----
	comp := decodeCompletionBody(t, upBody)
	if !strings.HasPrefix(comp.Prompt, "## Tool calling") {
		t.Fatalf("prompt 最前不是注入段:\n%.400s", comp.Prompt)
	}
	for _, want := range []string{
		"## Available tools", // 清单段头
		"- Read(file_path: string (required)): Read a file",           // 工具清单行（BK 同形）
		`<tool_use name="tool_name">{"argument": "value"}</tool_use>`, // 契约句里的格式样例
		"executed by the client",                                      // 客户端执行语义句
	} {
		if !strings.Contains(comp.Prompt, want) {
			t.Fatalf("注入段缺 %q:\n%.600s", want, comp.Prompt)
		}
	}
	if !strings.HasSuffix(comp.Prompt, "Human: read a.go") {
		t.Fatalf("注入段之后应是拍平消息文本（Human: read a.go）:\n%.600s", comp.Prompt)
	}
	// W11-3 契约不破：上游 tools 字段仍三元组（客户端工具定义只走 prompt 文本）。
	if len(comp.Tools) != 3 {
		t.Fatalf("上游 tools = %v, want web_search/artifacts/repl 三元组", comp.Tools)
	}
	if strings.Contains(string(upBody), `"name":"Read"`) && !strings.Contains(comp.Prompt, "Read") {
		t.Fatalf("客户端工具定义泄漏进上游体非 prompt 字段:\n%s", upBody)
	}

	// ---- ② 出口面：标签被吞、合成 tool_use 块 + input_json_delta + stop_reason。----
	facts := parseClaudeStream(t, down)
	if !facts.sawToolUse {
		t.Fatalf("下游缺 tool_use 块起始帧:\n%.800s", down)
	}
	if facts.toolName != "Read" {
		t.Fatalf("合成块 name = %q, want Read", facts.toolName)
	}
	if facts.args != `{"file_path":"a.go"}` {
		t.Fatalf("合成块参数 = %q, want %q", facts.args, `{"file_path":"a.go"}`)
	}
	if facts.stopReason != "tool_use" {
		t.Fatalf("下游 stop_reason = %q, want tool_use", facts.stopReason)
	}
	// 标签被吞、正文保留（只吞标签、不吞前后文本）。
	if facts.text != "I'll read the file.\n" {
		t.Fatalf("下游正文 = %q, want %q（契约标签应被吞）", facts.text, "I'll read the file.\n")
	}
	if !strings.Contains(string(down), "event: message_stop") {
		t.Fatalf("流未以 message_stop 收尾:\n%.800s", down)
	}
}

// claudeStreamFacts 下游 claude_messages SSE 的解析事实。
type claudeStreamFacts struct {
	text       string // text_delta 文本拼接（JSON 解码后——标签是否被吞看这里）
	sawToolUse bool   // 合成 tool_use 块起始
	toolName   string // 合成块名
	args       string // input_json_delta 参数拼接
	stopReason string // message_delta.stop_reason
}

// parseClaudeStream 下游 claude_messages SSE → 事实（text/合成块/参数/stop_reason）。
func parseClaudeStream(t *testing.T, raw []byte) claudeStreamFacts {
	t.Helper()
	var facts claudeStreamFacts
	var text, args strings.Builder
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var frame struct {
			Type         string `json:"type"`
			ContentBlock *struct {
				Type string `json:"type"`
				Name string `json:"name"`
			} `json:"content_block"`
			Delta *struct {
				Type        string `json:"type"`
				Text        string `json:"text"`
				PartialJSON string `json:"partial_json"`
				StopReason  string `json:"stop_reason"`
			} `json:"delta"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &frame); err != nil {
			t.Fatalf("下游帧: %v\n%s", err, line)
		}
		switch {
		case frame.Type == "content_block_start" && frame.ContentBlock != nil && frame.ContentBlock.Type == "tool_use":
			facts.sawToolUse = true
			facts.toolName = frame.ContentBlock.Name
		case frame.Type == "content_block_delta" && frame.Delta != nil:
			switch frame.Delta.Type {
			case "text_delta":
				text.WriteString(frame.Delta.Text)
			case "input_json_delta":
				args.WriteString(frame.Delta.PartialJSON)
			}
		case frame.Type == "message_delta" && frame.Delta != nil && frame.Delta.StopReason != "":
			facts.stopReason = frame.Delta.StopReason
		}
	}
	facts.text, facts.args = text.String(), args.String()
	return facts
}

// ---------------------------------------------------------------------------
// ② 非流式面：同词表同容错（runStep FaceResponse → toolsim.TransformResponse）
// ---------------------------------------------------------------------------

func TestE2EClaudeWebToolSimLoopNonStream(t *testing.T) {
	upBody, down, status := toolSimRunClaudeWeb(t, true, toolsimEntryBody(true, false), claudeWebDSLSSE())
	if status != 200 {
		t.Fatalf("status=%d body=\n%s", status, down)
	}
	// 请求面注入同流式面（同一渲染器）。
	comp := decodeCompletionBody(t, upBody)
	if !strings.HasPrefix(comp.Prompt, "## Tool calling") || len(comp.Tools) != 3 {
		t.Fatalf("非流式面注入/三元组不符:\n%.400s\ntools=%v", comp.Prompt, comp.Tools)
	}
	var out struct {
		Type       string `json:"type"`
		StopReason string `json:"stop_reason"`
		Content    []struct {
			Type  string          `json:"type"`
			Text  string          `json:"text"`
			Name  string          `json:"name"`
			Input json.RawMessage `json:"input"`
		} `json:"content"`
	}
	if err := json.Unmarshal(down, &out); err != nil {
		t.Fatalf("下游体: %v\n%s", err, down)
	}
	if out.Type != "message" || out.StopReason != "tool_use" {
		t.Fatalf("下游 type/stop_reason = %q/%q, want message/tool_use", out.Type, out.StopReason)
	}
	foundTool := false
	foundText := false
	for _, b := range out.Content {
		if b.Type == "text" {
			foundText = true
			if b.Text != "I'll read the file.\n" {
				t.Fatalf("非流式正文 = %q（契约标签应被吞）", b.Text)
			}
		}
		if b.Type != "tool_use" {
			continue
		}
		foundTool = true
		if b.Name != "Read" || string(b.Input) != `{"file_path":"a.go"}` {
			t.Fatalf("合成块 = %s %s %s", b.Type, b.Name, b.Input)
		}
	}
	if !foundTool {
		t.Fatalf("非流式出口缺合成 tool_use 块:\n%s", down)
	}
	if !foundText {
		t.Fatalf("非流式出口缺正文块:\n%s", down)
	}
	if strings.Contains(string(down), "<tool_use") {
		t.Fatalf("标签原样漏给下游:\n%s", down)
	}
}

// ---------------------------------------------------------------------------
// ③ 三形态：开关关 / 入口无 tools / 上游非 claude_web —— 与现状逐字节一致
// ---------------------------------------------------------------------------

// TestE2EClaudeWebToolSimFormsByteEqual 三形态的**共同判据**：sim 未置位时，
// 上游 completion 体与下游响应体都与现状（sim 关的同一请求）逐字节一致。
// 假上游回放的是**含契约标签**的 DSL 流——若出口解析器被错误置位，下游字节
// 必变（标签被吞 + 多出 tool_use 块）；若请求面被错误置位，上游 prompt 必变。
func TestE2EClaudeWebToolSimFormsByteEqual(t *testing.T) {
	t.Run("开关关", func(t *testing.T) {
		// sim 关 + 入口带 tools：注入不发生、标签按普通文本原样下发。
		upOff, downOff, status := toolSimRunClaudeWeb(t, false, toolsimEntryBody(true, true), claudeWebDSLSSE())
		if status != 200 {
			t.Fatalf("status=%d body=\n%s", status, downOff)
		}
		comp := decodeCompletionBody(t, upOff)
		if strings.Contains(comp.Prompt, "## Tool calling") {
			t.Fatalf("开关关时不得注入:\n%.400s", comp.Prompt)
		}
		if comp.Prompt != "Human: read a.go" {
			t.Fatalf("开关关时 prompt 应逐字节为现状拍平文本，got:\n%.400s", comp.Prompt)
		}
		if len(comp.Tools) != 3 {
			t.Fatalf("开关关时上游 tools = %v, want 三元组", comp.Tools)
		}
		offFacts := parseClaudeStream(t, downOff)
		if offFacts.sawToolUse {
			t.Fatalf("开关关时不得合成块:\n%.800s", downOff)
		}
		if offFacts.text != "I'll read the file.\n<tool_use name=\"Read\">{\"file_path\":\"a.go\"}</tool_use>" {
			t.Fatalf("开关关时标签应按普通文本原样下发，got text=%q", offFacts.text)
		}
		if offFacts.stopReason != "end_turn" {
			t.Fatalf("开关关时 stop_reason 应保持上游 end_turn，got %q", offFacts.stopReason)
		}

		// 交叉锚：sim 开但入口无 tools 的运行 = 开关关的运行（两形态同字节）。
		upNoTools, downNoTools, status := toolSimRunClaudeWeb(t, true, toolsimEntryBody(false, true), claudeWebDSLSSE())
		if status != 200 {
			t.Fatalf("status=%d body=\n%s", status, downNoTools)
		}
		if !bytes.Equal(maskVolatile(upNoTools), maskVolatile(upOff)) {
			t.Fatalf("上游体不等（sim 开+无 tools vs 开关关）:\n%s\n---\n%s",
				maskVolatile(upNoTools), maskVolatile(upOff))
		}
		if !bytes.Equal(downNoTools, downOff) {
			t.Fatalf("下游体不等（sim 开+无 tools vs 开关关）:\n%s\n---\n%s", downNoTools, downOff)
		}
	})

	t.Run("入口无tools", func(t *testing.T) {
		// sim 开 + 入口无 tools：注入不发生（渲染器另按 IR 工具表自守），出口
		// 解析器不得置位（否则普通文本里的标签字面量会被误判为工具调用）。
		upOn, downOn, status := toolSimRunClaudeWeb(t, true, toolsimEntryBody(false, true), claudeWebDSLSSE())
		if status != 200 {
			t.Fatalf("status=%d body=\n%s", status, downOn)
		}
		if p := decodeCompletionBody(t, upOn).Prompt; p != "Human: read a.go" {
			t.Fatalf("入口无 tools 时 prompt 应逐字节为现状拍平文本，got:\n%.400s", p)
		}
		onFacts := parseClaudeStream(t, downOn)
		if onFacts.sawToolUse {
			t.Fatalf("入口无 tools 时不得合成块:\n%.800s", downOn)
		}
		if onFacts.text != "I'll read the file.\n<tool_use name=\"Read\">{\"file_path\":\"a.go\"}</tool_use>" {
			t.Fatalf("入口无 tools 时标签应原样下发，got text=%q", onFacts.text)
		}

		// 对照：同一入口体 + sim 关（现状）→ 上游体与下游体逐字节相等。
		upOff, downOff, _ := toolSimRunClaudeWeb(t, false, toolsimEntryBody(false, true), claudeWebDSLSSE())
		if !bytes.Equal(maskVolatile(upOn), maskVolatile(upOff)) {
			t.Fatalf("上游体不等（sim 开 vs 关；入口无 tools）:\n%s\n---\n%s",
				maskVolatile(upOn), maskVolatile(upOff))
		}
		if !bytes.Equal(downOn, downOff) {
			t.Fatalf("下游体不等（sim 开 vs 关；入口无 tools）:\n%s\n---\n%s", downOn, downOff)
		}
	})

	t.Run("混链仅本台生效", func(t *testing.T) {
		// 配置位是**上游级**：链里另一台开了 tool_sim 不等于本台开。混链 [本台关,
		// 另一台开] → 请求落本台 → 不置位（缺省关的台逐字节等于现状）；反序
		// [本台开, 另一台关] → 置位（对照锚：证明本测非空转，且置位判的是**被
		// 使用的那台**）。
		up := newClaudeWebUpstream(t)
		up.streamBody = claudeWebDSLSSE()
		mk := func(first, second bool) *Config {
			return &Config{Listen: "127.0.0.1:0", HeartbeatSeconds: 15, Upstreams: []Upstream{
				{Name: "cw-1", BaseURL: up.srv.URL, APIKey: "sk-ant-sid01-test", Protocol: ProtocolClaudeWeb, ToolSim: first},
				{Name: "cw-2", BaseURL: up.srv.URL, APIKey: "sk-ant-sid01-test", Protocol: ProtocolClaudeWeb, ToolSim: second},
			}}
		}
		run := func(cfg *Config) (string, claudeStreamFacts) {
			base := len(up.snapshot())
			srv := startService(t, cfg)
			resp := postJSON(t, srv.URL+"/v1/messages", toolsimEntryBody(true, true), nil)
			defer resp.Body.Close()
			down, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != 200 {
				t.Fatalf("status=%d body=\n%s", resp.StatusCode, down)
			}
			calls := waitForClaudeWebCalls(t, up, base+4)
			for _, c := range calls[base:] {
				if c.method == http.MethodPost && strings.HasSuffix(c.path, "/completion") {
					return decodeCompletionBody(t, c.body).Prompt, parseClaudeStream(t, down)
				}
			}
			t.Fatalf("上游无 completion 调用: %v", callPaths(calls))
			return "", claudeStreamFacts{}
		}

		promptOff, factsOff := run(mk(false, true))
		if promptOff != "Human: read a.go" {
			t.Fatalf("本台关时不得注入（另一台开不传递）:\n%.400s", promptOff)
		}
		if factsOff.sawToolUse {
			t.Fatalf("本台关时不得合成块（另一台开不传递）")
		}
		if !strings.Contains(factsOff.text, `<tool_use name="Read">`) {
			t.Fatalf("本台关时标签应原样下发，got %q", factsOff.text)
		}

		promptOn, factsOn := run(mk(true, false))
		if !strings.HasPrefix(promptOn, "## Tool calling") {
			t.Fatalf("本台开时应注入（对照锚）:\n%.400s", promptOn)
		}
		if !factsOn.sawToolUse || factsOn.stopReason != "tool_use" {
			t.Fatalf("本台开时应合成块 + stop_reason=tool_use, got %+v", factsOn)
		}
	})

	t.Run("上游非claude_web", func(t *testing.T) {
		// sim 开 + 入口带 tools，但上游是 anthropic（非 claude_web）→ 条件三不全
		// （up.Protocol != claude_web）→ 不置位：上游体与下游体都与 sim 关逐字节
		// 相等。假上游正文**字面含契约标签**——若出口解析器被错误置位（丢掉
		// protocol 条件），标签会被吞 + 合成块，下游字节必变（判据非空转）。
		run := func(toolSim bool) (upBody, down []byte, status int) {
			up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				claudeTaggedTextStream(w)
			})
			srv := startService(t, cfgForToolSim(up.srv.URL, toolSim))
			body := `{"model":"gpt-x","stream":true,"tools":[{"type":"function","function":{"name":"Read","description":"Read a file","parameters":{"type":"object"}}}]` +
				`,"messages":[{"role":"user","content":"read a.go"}]}`
			resp := postJSON(t, srv.URL+"/v1/chat/completions", body, nil)
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)
			_, upBody, _ = up.request()
			return upBody, raw, resp.StatusCode
		}
		upOn, downOn, status := run(true)
		if status != 200 {
			t.Fatalf("status=%d body=\n%s", status, downOn)
		}
		// 上游体（anthropic 请求）不得含注入段（prompt 面不存在；system/messages 亦不得被注入）。
		if strings.Contains(string(upOn), "## Tool calling") || strings.Contains(string(upOn), "## Available tools") {
			t.Fatalf("非 claude_web 上游被注入:\n%s", upOn)
		}
		// 下游：标签按普通文本原样下发（未合成块）。
		content, finish, _, done := parseOpenAIStream(t, downOn)
		if !strings.Contains(content, `<tool_use name="Read">{"file_path":"a.go"}</tool_use>`) {
			t.Fatalf("非 claude_web 时标签应原样下发，got content=%q", content)
		}
		if finish != "stop" || !done {
			t.Fatalf("非 claude_web 时下游 finish/done = %q/%v", finish, done)
		}
		upOff, downOff, _ := run(false)
		if !bytes.Equal(upOn, upOff) {
			t.Fatalf("上游体不等（非 claude_web；sim 开 vs 关）:\n%s\n---\n%s", upOn, upOff)
		}
		if !bytes.Equal(downOn, downOff) {
			t.Fatalf("下游体不等（非 claude_web；sim 开 vs 关）:\n%s\n---\n%s", downOn, downOff)
		}
	})
}

// cfgForToolSim openai_chat 入口 → anthropic 上游（非 claude_web）+ tool_sim 开关。
func cfgForToolSim(baseURL string, toolSim bool) *Config {
	c := cfgFor(baseURL, ProtocolAnthropic, 15)
	c.Upstreams[0].ToolSim = toolSim
	return c
}

// claudeTaggedTextStream 假 anthropic 上游的文本流：正文**字面含契约标签**（形态
// 「上游非 claude_web」的判据——sim 若被错误置位，标签会被吞 + 合成块，下游必变）。
// 帧形同 service_test.go 的 claudeTextStream（message_start → block ×3 → stop）。
func claudeTaggedTextStream(w http.ResponseWriter) {
	sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_u1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":11,"cache_read_input_tokens":3,"output_tokens":0}}}`)
	sseFrame(w, "content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
	sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"I'll read the file.\n<tool_use name=\"Read\">{\"file_path\":\"a.go\"}</tool_use>"}}`)
	sseFrame(w, "content_block_stop", `{"type":"content_block_stop","index":0}`)
	sseFrame(w, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":11,"cache_read_input_tokens":3,"output_tokens":2}}`)
	sseFrame(w, "message_stop", `{"type":"message_stop"}`)
}

// TestConfigToolSimField YAML 面：`tool_sim` 位解析（LoadConfig 用
// KnownFields(true)——tag 写错/字段名错会在本测红，不是静默忽略）+ 缺省关。
func TestConfigToolSimField(t *testing.T) {
	const body = `listen: "127.0.0.1:0"
upstreams:
  - name: cw-on
    protocol: claude_web
    api_key: "sk-ant-sid01-test"
    tool_sim: true
  - name: cw-default
    protocol: claude_web
    api_key: "sk-ant-sid01-test"
`
	c, err := LoadConfig(writeEnvCfg(t, body))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !c.Upstreams[0].ToolSim {
		t.Fatalf("tool_sim: true 未解析进 Upstream.ToolSim")
	}
	if c.Upstreams[1].ToolSim {
		t.Fatalf("缺省（未写）应为关")
	}
	// 探测守卫（anyToolSimConfigured）与配置位一致。
	if !New(c).anyToolSimConfigured() {
		t.Fatalf("anyToolSimConfigured() = false, want true")
	}
	c2, err := LoadConfig(writeEnvCfg(t, `listen: "127.0.0.1:0"
upstreams:
  - name: cw
    protocol: claude_web
    api_key: "sk-ant-sid01-test"
`))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if New(c2).anyToolSimConfigured() {
		t.Fatalf("全缺省时 anyToolSimConfigured() = true, want false")
	}
}
