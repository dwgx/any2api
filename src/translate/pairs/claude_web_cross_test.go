package pairs

// W10：claude_web（上游叶子格式）转换对的直转面测试——claude_messages 入口 ↔
// claude_web 上游（openai 入口的 e2e 在 service 包；本文件补第二入口，证明
// 「任一入口都能直连该上游」且 claude_messages 出口能承载思考块）。
//
// 帧体 = 实抓 fixture 回放（W10-3 runB：thinking→text 交错，573 帧）。
//
// 反证锚点：删掉 NewRegistry 的 claude_web 显式对 → 本文件在 Lookup 处红
//（枢纽兜底也救不了：claude_web 不是枢纽格式，两条腿都缺）。

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/registry"
)

// claudeWebFixtureSSEPairs 读 claude_web 包 testdata 的实抓 fixture → SSE 文本。
func claudeWebFixtureSSE(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile("../convert/claude_web/testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		var f struct {
			Event string `json:"event"`
			Data  string `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &f); err != nil {
			t.Fatalf("fixture line: %v", err)
		}
		b.WriteString("event: " + f.Event + "\ndata: " + f.Data + "\n\n")
	}
	return b.String()
}

// TestClaudeWebFromClaudeMessagesRequest claude_messages 入口请求 → claude_web 上游体
// （system/user 拍平 + 模型/档位；实抓字段集）。
func TestClaudeWebFromClaudeMessagesRequest(t *testing.T) {
	reg := NewRegistry()
	body := `{"model":"claude-sonnet-5-5","max_tokens":1024,
		"system":"Be terse.",
		"messages":[{"role":"user","content":"hi"}],
		"thinking":{"type":"disabled"}}`
	out, err := Convert(reg, registry.FormatClaudeMessages, FormatClaudeWeb, FaceRequest, []byte(body), Options{})
	if err != nil {
		t.Fatalf("Convert(request): %v", err)
	}
	var w map[string]json.RawMessage
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	var prompt, model, effort, thinking string
	_ = json.Unmarshal(w["prompt"], &prompt)
	_ = json.Unmarshal(w["model"], &model)
	_ = json.Unmarshal(w["effort"], &effort)
	_ = json.Unmarshal(w["thinking_mode"], &thinking)
	if prompt != "System: Be terse.\n\nHuman: hi" {
		t.Fatalf("prompt = %q", prompt)
	}
	if model != "claude-sonnet-5-5" {
		t.Fatalf("model = %q", model)
	}
	if effort != "medium" || thinking != "none" {
		t.Fatalf("effort/thinking = %q/%q, want medium/none（thinking disabled → none）", effort, thinking)
	}
	// max_tokens 绝不漏进上游体（上游 400）。
	if _, ok := w["max_tokens"]; ok {
		t.Fatalf("max_tokens 漏进 claude_web 体")
	}
}

// TestClaudeWebToClaudeMessagesStream claude_web 上游流 → claude_messages 出口 SSE
// （实抓 runB：thinking 摘要 → thinking 块；text → text 块；message_stop 收尾）。
func TestClaudeWebToClaudeMessagesStream(t *testing.T) {
	reg := NewRegistry()
	sse := claudeWebFixtureSSE(t, "runB_55480.537.frames.jsonl.txt")
	out, err := Convert(reg, FormatClaudeWeb, registry.FormatClaudeMessages, FaceStream, []byte(sse), Options{})
	if err != nil {
		t.Fatalf("Convert(stream): %v", err)
	}
	text := string(out)
	// 帧序（Anthropic 出口契约）：message_start → content_block_start(thinking) →
	// thinking_delta* → content_block_stop → content_block_start(text) → text_delta*
	// → message_delta(stop_reason) → message_stop。
	for _, want := range []string{
		"event: message_start", "event: content_block_start", "event: content_block_delta",
		"event: content_block_stop", "event: message_delta", "event: message_stop",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("出口流缺 %q\n%.600s", want, text)
		}
	}
	if !strings.Contains(text, `"thinking_delta"`) {
		t.Fatalf("思考摘要未落 thinking_delta:\n%.600s", text)
	}
	if !strings.Contains(text, `"text_delta"`) {
		t.Fatalf("正文未落 text_delta")
	}
	if !strings.Contains(text, `"stop_reason":"end_turn"`) {
		t.Fatalf("缺 stop_reason（CC B7 硬门）:\n%.400s", text)
	}
	// 解析回 IR 复核：块序 = thinking → text（实抓 runB 两块）。
	evs, err := parseClaudeWebViaPairs(t, sse)
	if err != nil {
		t.Fatalf("解析 claude_web 流: %v", err)
	}
	var kinds []ir.BlockKind
	for _, ev := range evs {
		if ev.Kind == ir.EventBlockStart {
			kinds = append(kinds, ev.BlockKind)
		}
	}
	if len(kinds) != 2 || kinds[0] != ir.BlockThinking || kinds[1] != ir.BlockText {
		t.Fatalf("块序 = %v, want [thinking text]", kinds)
	}
}

// TestClaudeWebFromClaudeMessagesToolHistoryRequest W11-3：Claude Code 真实形态请求
// （`tools` 参数 + assistant tool_use + user 侧 tool_result 历史）→ claude_web 上游体。
// 旧行为 = flattenPrompt 对工具块 ErrUnsupportedBlock 断流；新契约 = 工具历史拍平为
// 标签文本（线格式块名），请求 200 可发。
func TestClaudeWebFromClaudeMessagesToolHistoryRequest(t *testing.T) {
	reg := NewRegistry()
	body := `{"model":"claude-sonnet-5-5","max_tokens":32000,
		"system":[{"type":"text","text":"You are Claude Code."}],
		"tools":[{"name":"Read","description":"Read a file","input_schema":{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}}],
		"messages":[
			{"role":"user","content":"read a.go"},
			{"role":"assistant","content":[{"type":"text","text":"Let me read it."},{"type":"tool_use","id":"toolu_01ABC","name":"Read","input":{"file_path":"a.go"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_01ABC","content":"package main"}]}
		]}`
	out, err := Convert(reg, registry.FormatClaudeMessages, FormatClaudeWeb, FaceRequest, []byte(body), Options{})
	if err != nil {
		t.Fatalf("Convert(request): %v（含工具历史的 claude 入口请求不得断流）", err)
	}
	var w map[string]json.RawMessage
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	var prompt string
	_ = json.Unmarshal(w["prompt"], &prompt)
	want := "System: You are Claude Code.\n\n" +
		"Human: read a.go\n\n" +
		"Assistant: Let me read it.\n" +
		`<tool_use id="toolu_01ABC" name="Read">{"file_path":"a.go"}</tool_use>` + "\n\n" +
		`Tool: <tool_result tool_use_id="toolu_01ABC">package main</tool_result>`
	if prompt != want {
		t.Fatalf("prompt = %q\nwant    %q", prompt, want)
	}
	// 客户端工具定义（`tools`）不进上游体（上游 tools 是服务端工具开关三元组）、
	// 也不注入 prompt（本 slice 不做 BK/AU 式工具模拟层——见报告 §4 契约选择）。
	var upstreamTools []map[string]string
	_ = json.Unmarshal(w["tools"], &upstreamTools)
	if len(upstreamTools) != 3 {
		t.Fatalf("上游 tools = %v, want 三元组", upstreamTools)
	}
	if strings.Contains(prompt, "Read a file") {
		t.Fatalf("客户端工具定义不得注入 prompt:\n%s", prompt)
	}
}

// TestClaudeWebToClaudeMessagesStreamToolBlocks W11-3：实抓 runA（thinking/text/
// tool_use(bash_tool)×2/tool_result×2 交错，9 块）→ claude_messages 出口：
//   - **不断流**（旧行为 = BlockToolResult 块 start 处 ErrUnsupportedBlock 断流）；
//   - server_tool_use 块保留（id/name/参数经 input_json_delta）；
//   - tool_result 块整块丢弃（无配对客户端 tool_use；BK 同款）；
//   - 下游块 index 连续从 0 起（丢弃不留空洞；CC B5）。
func TestClaudeWebToClaudeMessagesStreamToolBlocks(t *testing.T) {
	reg := NewRegistry()
	sse := claudeWebFixtureSSE(t, "runA_45534.540.frames.jsonl.txt")
	out, err := Convert(reg, FormatClaudeWeb, registry.FormatClaudeMessages, FaceStream, []byte(sse), Options{})
	if err != nil {
		t.Fatalf("Convert(stream): %v（服务端工具块不得断流）", err)
	}
	text := string(out)
	if strings.Contains(text, "tool_result") {
		t.Fatalf("tool_result 块不得出现在出口流:\n%.800s", text)
	}
	frames := sseFramesOf(t, out)
	var starts []int
	serverToolUse := 0
	for _, f := range frames {
		if f.event != "content_block_start" {
			continue
		}
		var p struct {
			Index        int `json:"index"`
			ContentBlock struct {
				Type string `json:"type"`
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"content_block"`
		}
		if err := json.Unmarshal([]byte(f.data), &p); err != nil {
			t.Fatalf("frame data: %v\n%s", err, f.data)
		}
		if p.ContentBlock.Type == "tool_result" {
			t.Fatalf("tool_result 块 start 不得产出: %s", f.data)
		}
		starts = append(starts, p.Index)
		if p.ContentBlock.Type == "server_tool_use" {
			serverToolUse++
			if p.ContentBlock.Name != "bash_tool" {
				t.Fatalf("server_tool_use name = %q, want bash_tool", p.ContentBlock.Name)
			}
		}
	}
	// 上游 9 块 - 2 个 tool_result（整块丢弃）= 下游 7 块，index 0..6 连续。
	if len(starts) != 7 {
		t.Fatalf("下游块数 = %d, want 7（starts=%v）", len(starts), starts)
	}
	for i, idx := range starts {
		if idx != i {
			t.Fatalf("下游块 index 必须连续从 0 起: %v", starts)
		}
	}
	if serverToolUse != 2 {
		t.Fatalf("server_tool_use 块数 = %d, want 2", serverToolUse)
	}
	// 实抓工具 id 透传（hygiene 净化面：纯字母数字）。
	if !strings.Contains(text, `"id":"toolu_01TGJEVo22wLnuVNywbwJ4iE"`) {
		t.Fatalf("server_tool_use id 未透传:\n%.800s", text)
	}
	if !strings.Contains(text, `"partial_json"`) {
		t.Fatalf("工具参数须走 input_json_delta（B5/B6）")
	}
	// 收尾帧（B7/B8）与 stop_reason 透传（上游 end_turn）。
	if !strings.Contains(text, `"stop_reason":"end_turn"`) || !strings.Contains(text, "event: message_stop") {
		t.Fatalf("收尾帧缺失:\n%.400s", text)
	}
	// 每个 delta / stop 的 index 指向已 start 的块（B5）。
	for _, f := range frames {
		if f.event != "content_block_delta" && f.event != "content_block_stop" {
			continue
		}
		var p struct {
			Index int `json:"index"`
		}
		if err := json.Unmarshal([]byte(f.data), &p); err != nil {
			t.Fatalf("frame data: %v\n%s", err, f.data)
		}
		if p.Index < 0 || p.Index >= len(starts) {
			t.Fatalf("%s index = %d 越界: %s", f.event, p.Index, f.data)
		}
	}
}

// parseClaudeWebViaPairs 经 pairs 的 claude_web 流解析器（增量面）拿 IR 事件
// （用包内 newStreamParser——即 StreamPipe 入口腿实际使用的那个）。
func parseClaudeWebViaPairs(t *testing.T, sse string) ([]ir.StreamEvent, error) {
	t.Helper()
	sp, err := newStreamParser(FormatClaudeWeb)
	if err != nil {
		return nil, err
	}
	var events []ir.StreamEvent
	var event string
	var data []string
	for _, line := range strings.Split(sse, "\n") {
		switch {
		case line == "":
			if len(data) > 0 {
				evs, err := sp.feed(event, []byte(strings.Join(data, "\n")))
				if err != nil {
					return events, err
				}
				events = append(events, evs...)
				event, data = "", nil
			}
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	events = append(events, sp.finishParse()...)
	return events, nil
}

// TestClaudeWebQuotaTruncationExits W44-1（R1 双出口映射）：claude.ai 配额截断
// （message_delta{stop_reason:"stop_sequence", stop_sequence:"usage_limit"}）——
// claude_messages 出口回填 `stop_reason:"stop_sequence"` + `stop_sequence` 字段；
// openai_chat 出口保持 `finish_reason:"stop"`（契约压平，文档化于 finishReasonFor；
// 截断信号可消费面 = usage/IR StopSequence）。
func TestClaudeWebQuotaTruncationExits(t *testing.T) {
	reg := NewRegistry()
	sse := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"chatcompl_1\",\"model\":\"claude-sonnet-5-5\"}}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n" +
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0,\"stop_timestamp\":\"2026-10-14T14:43:02.16658166Z\"}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"stop_sequence\",\"stop_sequence\":\"usage_limit\",\"stop_details\":null}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"

	cmOut, err := Convert(reg, FormatClaudeWeb, registry.FormatClaudeMessages, FaceStream, []byte(sse), Options{})
	if err != nil {
		t.Fatalf("claude_web→claude_messages: %v", err)
	}
	cmText := string(cmOut)
	if !strings.Contains(cmText, `"stop_reason":"stop_sequence"`) || !strings.Contains(cmText, `"stop_sequence":"usage_limit"`) {
		t.Fatalf("CM 出口未回填截断信号:\n%s", cmText)
	}
	oaiOut, err := Convert(reg, FormatClaudeWeb, registry.FormatOpenAIChat, FaceStream, []byte(sse), Options{})
	if err != nil {
		t.Fatalf("claude_web→openai_chat: %v", err)
	}
	oaiText := string(oaiOut)
	if !strings.Contains(oaiText, `"finish_reason":"stop"`) {
		t.Fatalf("OAI 出口 finish_reason 非 stop:\n%s", oaiText)
	}
	if strings.Contains(oaiText, "stop_sequence") {
		t.Fatalf("OAI 出口泄漏 stop_sequence（契约无该字段）:\n%s", oaiText)
	}
	respOut, err := Convert(reg, FormatClaudeWeb, registry.FormatOpenAIResponses, FaceStream, []byte(sse), Options{})
	if err != nil {
		t.Fatalf("claude_web→responses: %v", err)
	}
	respText := string(respOut)
	if !strings.Contains(respText, `"status":"completed"`) {
		t.Fatalf("Responses 出口 status 非 completed:\n%s", respText)
	}
	if strings.Contains(respText, "stop_sequence") {
		t.Fatalf("Responses 出口泄漏 stop_sequence（契约无该字段）:\n%s", respText)
	}
}

// TestClaudeWebQuotaTruncationResponseFace W52-1（W44-1 §9 残留 1 非流式对齐）：
// 同截断语料的**非流式面**（stream:false 下游：claude_web.ParseResponse 装配 →
// claude_messages.RenderResponse）——IR StopSequence 回填后 CM 出口渲染
// `stop_reason:"stop_sequence"` + `stop_sequence:"usage_limit"`；常态
// （stop_sequence null）维持 end_turn + null；OAI 出口不泄漏该字段（契约无）。
func TestClaudeWebQuotaTruncationResponseFace(t *testing.T) {
	reg := NewRegistry()
	sse := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"chatcompl_1\",\"model\":\"claude-sonnet-5-5\"}}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n" +
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0,\"stop_timestamp\":\"2026-10-14T14:43:02.16658166Z\"}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"stop_sequence\",\"stop_sequence\":\"usage_limit\",\"stop_details\":null}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"

	cmOut, err := Convert(reg, FormatClaudeWeb, registry.FormatClaudeMessages, FaceResponse, []byte(sse), Options{})
	if err != nil {
		t.Fatalf("claude_web→claude_messages(response): %v", err)
	}
	cmText := string(cmOut)
	if !strings.Contains(cmText, `"stop_reason":"stop_sequence"`) || !strings.Contains(cmText, `"stop_sequence":"usage_limit"`) {
		t.Fatalf("CM 非流式出口未回填截断信号:\n%s", cmText)
	}
	// 常态回归：stop_sequence null → end_turn + null（旧形态逐字节不变）。
	plain := strings.ReplaceAll(sse, `"stop_sequence":"usage_limit"`, `"stop_sequence":null`)
	plainOut, err := Convert(reg, FormatClaudeWeb, registry.FormatClaudeMessages, FaceResponse, []byte(plain), Options{})
	if err != nil {
		t.Fatalf("claude_web→claude_messages(response, plain): %v", err)
	}
	plainText := string(plainOut)
	if !strings.Contains(plainText, `"stop_reason":"end_turn"`) || !strings.Contains(plainText, `"stop_sequence":null`) {
		t.Fatalf("CM 非流式常态形态回归失败:\n%s", plainText)
	}
	// OAI 出口不泄漏（契约无 stop_sequence 字段）。
	oaiOut, err := Convert(reg, FormatClaudeWeb, registry.FormatOpenAIChat, FaceResponse, []byte(sse), Options{})
	if err != nil {
		t.Fatalf("claude_web→openai_chat(response): %v", err)
	}
	if strings.Contains(string(oaiOut), "stop_sequence") {
		t.Fatalf("OAI 非流式出口泄漏 stop_sequence:\n%s", oaiOut)
	}
}
