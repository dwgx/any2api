package claude_web

// W17 S1（工具模拟注入）验收：注入段（契约 + 清单 + tool_choice）进 prompt 最前、
// 上游体 `tools` 仍三元组、**ToolSim=false 与历史行为逐字节相等**。
//
// 反证锚点：删掉 RenderRequestWithOptions 的注入块 → TestRenderRequestToolSimInjection
// 红（prompt 无契约句/无清单）；把注入段写进 `tools` 字段或改字段集 →
// 三元组/白名单断言红。

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// toolsimReq 工具模拟请求：两工具（超长描述 + 带 schema）+ 具名 tool_choice。
func toolsimReq() *ir.Request {
	return &ir.Request{
		Model: "claude-sonnet-5-5",
		Messages: []ir.Message{
			{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "read the file"}}},
		},
		Tools: []ir.ToolDef{
			{Name: "Read", Description: strings.Repeat("读", 250)},
			{
				Name:        "Bash",
				Description: "Run a shell command.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"command":{"type":"string"},"timeout":{"type":"number"}},"required":["command"]}`),
			},
		},
		ToolChoice: json.RawMessage(`{"type":"tool","name":"Bash"}`),
	}
}

// renderToolSimBody 渲染 → 体 map；uuid 类字段（每请求新生成）掩码成占位，
// 其余字段原文保留（逐字节比较用）。
func renderToolSimBody(t *testing.T, req *ir.Request, opts RenderOptions) map[string]json.RawMessage {
	t.Helper()
	out, err := RenderRequestWithOptions(req, opts)
	if err != nil {
		t.Fatalf("RenderRequestWithOptions(%+v): %v", opts, err)
	}
	var w map[string]json.RawMessage
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	for _, k := range []string{"turn_message_uuids", "completion_request_id"} {
		if _, ok := w[k]; !ok {
			t.Fatalf("缺字段 %q", k)
		}
		w[k] = json.RawMessage(`"<masked>"`)
	}
	return w
}

// promptOf 体 → prompt 原文。
func promptOf(t *testing.T, w map[string]json.RawMessage) string {
	t.Helper()
	var prompt string
	if err := json.Unmarshal(w["prompt"], &prompt); err != nil {
		t.Fatalf("prompt: %v", err)
	}
	return prompt
}

// TestRenderRequestToolSimInjection S1 验收：注入段含工具名/描述/契约句（prompt 最前）；
// 上游体 `tools` 仍三元组；字段集不破。
func TestRenderRequestToolSimInjection(t *testing.T) {
	req := toolsimReq()
	w := renderToolSimBody(t, req, RenderOptions{ToolSim: true})
	prompt := promptOf(t, w)

	// ① 注入段在 prompt 最前（BK 系统前缀位；消息文本随其后）。
	if !strings.HasPrefix(prompt, "## Tool calling") {
		t.Fatalf("注入段不在 prompt 最前:\n%.200s", prompt)
	}
	if !strings.HasSuffix(prompt, "Human: read the file") {
		t.Fatalf("消息拍平文本缺失/错位:\n%.200s", prompt)
	}
	// ② 契约句（AU/BK 措辞的禁项逐条在）。
	for _, want := range []string{
		"EXACTLY one block per call",
		`<tool_use name="tool_name">{"argument": "value"}</tool_use>`,
		"Never emit <tool_result> blocks",
		"never fabricate observations or tool results",
		"stop immediately",
		"## Available tools",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("注入段缺契约句 %q:\n%s", want, prompt)
		}
	}
	// ③ 清单：工具名 + 描述（截断 200 rune + "..."）+ schema 参数摘要。
	if !strings.Contains(prompt, "- Bash(command: string (required), timeout: number): Run a shell command.") {
		t.Fatalf("清单缺 Bash 行（schema 摘要）:\n%s", prompt)
	}
	if !strings.Contains(prompt, strings.Repeat("读", 200)+"...") {
		t.Fatalf("清单缺 Read 行（截断描述）")
	}
	if strings.Contains(prompt, strings.Repeat("读", 201)) {
		t.Fatalf("描述截断失效（>200 rune 仍在）")
	}
	// ④ tool_choice 映射句。
	if !strings.Contains(prompt, `Tool choice is set to the tool "Bash": you must call that tool.`) {
		t.Fatalf("缺 tool_choice 映射句:\n%s", prompt)
	}
	// ⑤ 上游体 `tools` 仍三元组（W11-3 契约不破：工具定义只走 prompt 文本）。
	var tools []map[string]string
	if err := json.Unmarshal(w["tools"], &tools); err != nil {
		t.Fatalf("tools: %v", err)
	}
	if len(tools) != 3 || tools[0]["name"] != "web_search" || tools[0]["type"] != "web_search_v0" ||
		tools[1]["name"] != "artifacts" || tools[2]["name"] != "repl" {
		t.Fatalf("tools 不再是三元组: %+v", tools)
	}
	// ⑥ 字段集不破（14 键白名单；注入只在 prompt 文本内）。
	want := []string{
		"prompt", "timezone", "locale", "model", "effort", "thinking_mode", "tools",
		"turn_message_uuids", "attachments", "files", "sync_sources",
		"completion_request_id", "rendering_mode", "create_conversation_params",
	}
	if len(w) != len(want) {
		t.Fatalf("字段数 = %d, want %d", len(w), len(want))
	}
	for _, k := range want {
		if _, ok := w[k]; !ok {
			t.Fatalf("缺字段 %q", k)
		}
	}
}

// TestRenderRequestToolSimOffByteEqual S1 硬约束：ToolSim=false（缺省）与历史行为
// **逐字节相等**——RenderRequestWithOptions(req, RenderOptions{}) 与 RenderRequest(req)
// 的体（uuid 掩码后）逐字段原文相等；且 prompt 无注入段。
func TestRenderRequestToolSimOffByteEqual(t *testing.T) {
	req := toolsimReq()
	oldOut, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	var oldBody map[string]json.RawMessage
	if err := json.Unmarshal(oldOut, &oldBody); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, k := range []string{"turn_message_uuids", "completion_request_id"} {
		oldBody[k] = json.RawMessage(`"<masked>"`)
	}
	newBody := renderToolSimBody(t, req, RenderOptions{})

	if len(newBody) != len(oldBody) {
		t.Fatalf("字段数 %d != %d", len(newBody), len(oldBody))
	}
	for k, v := range oldBody {
		if string(newBody[k]) != string(v) {
			t.Fatalf("字段 %q 不逐字节相等:\nold %s\nnew %s", k, v, newBody[k])
		}
	}
	if got := promptOf(t, newBody); got != "Human: read the file" {
		t.Fatalf("缺省关 prompt = %q, want 无注入段", got)
	}

	// 无 tools 的请求：ToolSim 开也不注入（prompt 与缺省相等）。
	noTools := toolsimReq()
	noTools.Tools = nil
	noTools.ToolChoice = nil
	onBody := renderToolSimBody(t, noTools, RenderOptions{ToolSim: true})
	offBody := renderToolSimBody(t, noTools, RenderOptions{})
	if promptOf(t, onBody) != promptOf(t, offBody) {
		t.Fatalf("无 tools 时不得注入：\n%q\n%q", promptOf(t, onBody), promptOf(t, offBody))
	}
}
