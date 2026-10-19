package pairs

// W21-2 ① 工具名清洗三处一致（注入名 ↔ 合成名 ↔ 历史回放名）：CC 形态的
// `mcp__server__tool` 长名/非法字符两向——请求面净化（注入清单 + 历史回放同形）
// → 模型照抄净化名 → 出口还原成原名（客户端可执行；r14 前提）。toolname 面
// 既有净化/还原测试见 translate/toolname；本文件锁**跨三处的名字一致性**与
// 回环闭合（合成名进下一轮历史 → 再净化 → 与注入名逐字节同）。
//
// 反证锚点：Filter.restore 改成原样返回（去掉净化名→原名还原）→ 本文件响应面/
// 流式面断言红；把 SanitizeRequest 从 pairs 请求面拿掉 → 注入名/历史回放名断言红。

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/convert/claude_messages"
	"github.com/dwgx/any2api/translate/ir"
)

var (
	// w21LongName CC 形态的 MCP 长名（84 字，W19 实抓量级 `mcp__server__tool` 形态）。
	w21LongName = "mcp__srv__" + strings.Repeat("c", 74)
	// w21DirtyName MCP 形态 + 非法字符（`/`、空格、中文——P67 字符集归一量级）。
	w21DirtyName = "mcp__srv__tools/list 读"
)

// w21ToolSimBody 入口请求（claude_messages）：两工具（长名 + 脏名）+ 合法短名，
// 历史里带一次长名调用（= 上一轮出口合成后客户端回填的形态——回环第二轮的输入）。
func w21ToolSimBody() []byte {
	return []byte(`{"model":"claude-x","max_tokens":64,` +
		`"messages":[` +
		`{"role":"assistant","content":[{"type":"tool_use","id":"toolu_hist1","name":` + jsonString(w21LongName) + `,"input":{"q":"ping"}}]},` +
		`{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_hist1","content":"ok"}]},` +
		`{"role":"user","content":"continue"}` +
		`],` +
		`"tools":[` +
		`{"name":` + jsonString(w21LongName) + `,"description":"long tool","input_schema":{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}},` +
		`{"name":` + jsonString(w21DirtyName) + `,"description":"dirty tool","input_schema":{"type":"object"}},` +
		`{"name":"Read","description":"plain","input_schema":{"type":"object"}}` +
		`]}`)
}

// TestToolSimToolNameThreePlacesConsistent 三处一致：注入名 == 历史回放名（模型
// 只见过一套名字）；合成名 == 原名（客户端只认一套名字）；短合法名不被改写。
func TestToolSimToolNameThreePlacesConsistent(t *testing.T) {
	reg := NewRegistry()
	body := w21ToolSimBody()

	table := BuildToolNameMap(fClaude, body)
	if len(table) != 2 {
		t.Fatalf("清洗表 = %v（want 长名 + 脏名两条；短合法名不进表）", table)
	}
	sanLong, sanDirty := table[w21LongName], table[w21DirtyName]
	if len(sanLong) != 64 || sanLong != w21LongName[:64] {
		t.Fatalf("长名净化 = %q (len=%d)", sanLong, len(sanLong))
	}
	if sanDirty != "mcp__srv__tools_list__" {
		t.Fatalf("脏名净化 = %q", sanDirty)
	}

	// ---- 请求面：注入清单 + 历史回放 = 同一套净化名 ----
	out, err := Convert(reg, fClaude, FormatClaudeWeb, FaceRequest, body, Options{ToolSim: true, ToolNames: table})
	if err != nil {
		t.Fatalf("Convert(FaceRequest): %v", err)
	}
	var prompt string
	if err := json.Unmarshal(decodeBody(t, out)["prompt"], &prompt); err != nil {
		t.Fatalf("prompt: %v", err)
	}

	// 注入名（清单行；schema 摘要与描述在位）。
	if !strings.Contains(prompt, "- "+sanLong+`(q: string (required)): long tool`) {
		t.Fatalf("注入清单缺长名净化行:\n%.800s", prompt)
	}
	if !strings.Contains(prompt, "- "+sanDirty+"(): dirty tool") {
		t.Fatalf("注入清单缺脏名净化行:\n%.800s", prompt)
	}
	// 历史回放名（flattenBlock 的 `<tool_use id=… name=…>` 形态）。
	if !strings.Contains(prompt, `<tool_use id="toolu_hist1" name="`+sanLong+`">`) {
		t.Fatalf("历史回放未用净化名:\n%.800s", prompt)
	}
	// 三处一致：注入名与历史回放名逐字节同（各一次）——模型侧一套名字。
	if got := strings.Count(prompt, sanLong); got != 2 {
		t.Fatalf("净化长名出现 %d 次, want 2（注入 + 历史）:\n%.800s", got, prompt)
	}
	if got := strings.Count(prompt, sanDirty); got != 1 {
		t.Fatalf("净化脏名出现 %d 次, want 1（仅注入）", got)
	}
	// 原名（客户端词表）绝不进上游 prompt。
	if strings.Contains(prompt, w21LongName) || strings.Contains(prompt, w21DirtyName) {
		t.Fatalf("原名泄漏进 prompt:\n%.800s", prompt)
	}

	// ---- 响应面（非流式）：模型照抄净化名 → 合成名还原成原名 ----
	modelText := `<tool_use name="` + sanLong + `">{"q":"pong"}</tool_use> and <tool_use name="` + sanDirty + `">{"z":1}</tool_use>`
	sse := claudeWebTextStream([]string{modelText}, "end_turn")
	respOut, err := Convert(reg, FormatClaudeWeb, fClaude, FaceResponse, sse, Options{ToolSim: true, ToolNames: table})
	if err != nil {
		t.Fatalf("Convert(FaceResponse): %v", err)
	}
	var down struct {
		Content []struct {
			Type  string `json:"type"`
			Name  string `json:"name"`
			Input struct {
				Q string `json:"q"`
				Z int    `json:"z"`
			} `json:"input"`
		} `json:"content"`
	}
	if err := json.Unmarshal(respOut, &down); err != nil {
		t.Fatalf("下游响应: %v\n%s", err, respOut)
	}
	var names []string
	for _, b := range down.Content {
		if b.Type == "tool_use" {
			names = append(names, b.Name)
		}
	}
	if len(names) != 2 || names[0] != w21LongName || names[1] != w21DirtyName {
		t.Fatalf("非流式合成名 = %v, want 原名 [%s %s]", names, w21LongName, w21DirtyName)
	}

	// ---- 流式面（StreamPipe 增量）：同表同判 ----
	pipe := toolSimPipe(t, fClaude, Options{ToolSim: true, ToolNames: table})
	streamOut := runStreamPipe(t, pipe, claudeWebTextStream([]string{
		`<tool_use name="` + sanLong + `">{"q":"pong"}`,
		`</tool_use>`,
	}, "end_turn"))
	evs, err := claude_messages.ParseStream(bytes.NewReader(streamOut))
	if err != nil {
		t.Fatalf("下游流解析: %v", err)
	}
	calls, err := ir.AssembleToolCalls(evs)
	if err != nil {
		t.Fatalf("装配: %v", err)
	}
	if len(calls) != 1 || calls[0].Name != w21LongName {
		t.Fatalf("流式合成名 = %+v, want 原名 %q", calls, w21LongName)
	}
}

// TestToolSimToolNameLoopClosure 回环闭合：合成名（原名）进下一轮历史 → 再净化 →
// 与首轮注入名逐字节同（跨轮名字稳定——模型/客户端各自只见一套名字）。
func TestToolSimToolNameLoopClosure(t *testing.T) {
	reg := NewRegistry()
	body := w21ToolSimBody()
	table := BuildToolNameMap(fClaude, body)
	sanLong := table[w21LongName]

	// 第二轮请求体 = 首轮历史（客户端回填的合成名 = 原名）+ 同样的工具清单。
	// 与首轮 body 逐字节同形态（w21ToolSimBody 的历史就是用原名写的）——
	// 再建表 + 再渲染：净化名与首轮一致。
	table2 := BuildToolNameMap(fClaude, body)
	if table2[w21LongName] != sanLong {
		t.Fatalf("跨轮净化不稳定：%q != %q", table2[w21LongName], sanLong)
	}
	out, err := Convert(reg, fClaude, FormatClaudeWeb, FaceRequest, body, Options{ToolSim: true, ToolNames: table2})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	var prompt string
	if err := json.Unmarshal(decodeBody(t, out)["prompt"], &prompt); err != nil {
		t.Fatalf("prompt: %v", err)
	}
	if strings.Count(prompt, sanLong) != 2 {
		t.Fatalf("第二轮注入名/历史回放名与首轮不一致（出现 %d 次）", strings.Count(prompt, sanLong))
	}
}
