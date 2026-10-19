package toolsim

// W17 S2（出口解析）单元验收：跨 chunk 缓冲 / 未知标签与 `<tool_result>` 降级 /
// 坏 JSON 降级 / 双判据（成对开闭 + JSON 对象）。
//
// 反证锚点：把 validArgs 的 JSON 校验放宽（`return t, true`）→ 坏 JSON 用例红；
// 删掉 ambiguousTagPrefix 等待分支（直接吐文本）→ 跨片切割用例红。

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// normSegs 合并相邻文本片段（跨 Feed 的片段切分是实现细节；断言关心的是分类与内容）。
func normSegs(segs []Segment) []Segment {
	out := make([]Segment, 0, len(segs))
	for _, sg := range segs {
		if sg.Call == nil && len(out) > 0 && out[len(out)-1].Call == nil {
			out[len(out)-1].Text += sg.Text
			continue
		}
		out = append(out, sg)
	}
	return out
}

// segString 片段 → 断言用短形：`t:<文本>` / `c:<name>(<args>)[<id>]`（相邻文本已合并）。
func segString(segs []Segment) string {
	segs = normSegs(segs)
	parts := make([]string, 0, len(segs))
	for _, sg := range segs {
		if sg.Call == nil {
			parts = append(parts, "t:"+sg.Text)
			continue
		}
		parts = append(parts, "c:"+sg.Call.Name+"("+sg.Call.Args+")["+sg.Call.ID+"]")
	}
	return strings.Join(parts, "|")
}

// TestToolSimScanSegmentShape 整段扫描的原始片段形状（不合并）：`[文本][调用][文本]`。
func TestToolSimScanSegmentShape(t *testing.T) {
	segs := ScanText(`pre <tool_use name="Read">{"a":1}</tool_use> post`)
	if len(segs) != 3 || segs[0].Call != nil || segs[0].Text != "pre " ||
		segs[1].Call == nil || segs[1].Call.Name != "Read" ||
		segs[2].Call != nil || segs[2].Text != " post" {
		t.Fatalf("片段形状 = %s", segString(segs))
	}
}

// TestToolSimScanTextTable 整段扫描表：契约合成与各类降级面。
func TestToolSimScanTextTable(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"纯文本", "hello world", "t:hello world"},
		{"含小于号", "a < b", "t:a < b"},
		{"未知标签降级", "<think>pondering</think> answer", "t:<think>pondering</think> answer"},
		{"tool_result 一律文本降级", `<tool_result tool_use_id="toolu_1">{"ok":true}</tool_result>`, `t:<tool_result tool_use_id="toolu_1">{"ok":true}</tool_result>`},
		{"孤立闭标签文本", "stray </tool_use> tag", "t:stray </tool_use> tag"},
		{"开标签无 name 降级", `<tool_use>{"a":1}</tool_use>`, `t:<tool_use>{"a":1}</tool_use>`},
		{"前缀相似标签降级", `<tool_useX name="a">`, "t:<tool_useX name=\"a\">"},
		{"契约调用", `<tool_use name="Read">{"file_path":"a.txt"}</tool_use>`, `c:Read({"file_path":"a.txt"})[]`},
		{"带 id 与前置文本", `let me check <tool_use id="toolu_9" name="Bash">{"command":"ls"}</tool_use>`, `t:let me check |c:Bash({"command":"ls"})[toolu_9]`},
		{"标签后置文本", `<tool_use name="Read">{}</tool_use> done`, `c:Read({})[]|t: done`},
		{"空体无参（出口 {} 纪律接管）", `<tool_use name="Read"></tool_use>`, `c:Read()[]`},
		{"单引号与裸值属性", `<tool_use name='Read' id=toolu_7>{}</tool_use>`, `c:Read({})[toolu_7]`},
		{"坏 JSON 降级", `<tool_use name="Read">{oops</tool_use>`, `t:<tool_use name="Read">{oops</tool_use>`},
		{"非对象 JSON 降级", `<tool_use name="Read">[1,2]</tool_use>`, `t:<tool_use name="Read">[1,2]</tool_use>`},
		{"未闭合标签降级", `<tool_use name="Read">{"a":1}`, `t:<tool_use name="Read">{"a":1}`},
		{"体内闭标签字面量（U3 兜底）", `<tool_use name="Bash">{"command":"echo '</tool_use>'"}</tool_use>`, `c:Bash({"command":"echo '</tool_use>'"})[]`},
		{"两次调用", `<tool_use name="A">{}</tool_use> mid <tool_use name="B">{"x":1}</tool_use>`, `c:A({})[]|t: mid |c:B({"x":1})[]`},
		{"半截开标签后接文本", "see <tool_use", "t:see <tool_use"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := segString(ScanText(tc.in)); got != tc.want {
				t.Fatalf("ScanText(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestToolSimScanByteWiseCrossChunk 逐字节喂入（最细粒度跨片切割）与整段扫描同结果——
// `<tool` + `_use …` 这类切割由 Scanner 缓冲吸收（BK pendingTag 同款）。
func TestToolSimScanByteWiseCrossChunk(t *testing.T) {
	samples := []string{
		`before <tool_use name="Read">{"file_path":"a.txt"}</tool_use> after`,
		`<tool_use name="Bash">{"command":"echo hi"}</tool_use>`,
		`x </tool_use> y <tool_result tool_use_id="t">{}</tool_result> z`,
		`<tool_use name="Read">{bad json</tool_use>`,
		`plain text without tags`,
	}
	for _, in := range samples {
		want := segString(ScanText(in))
		var sc Scanner
		var segs []Segment
		for i := range len(in) {
			segs = append(segs, sc.Feed(in[i:i+1])...)
		}
		segs = append(segs, sc.Flush()...)
		if got := segString(segs); got != want {
			t.Fatalf("逐字节喂入 %q = %q, want %q", in, got, want)
		}
	}
}

// TestToolSimScanNonContractContentPreserved 非契约文本零损失：无调用的输入，文本片段拼接
// 逐字节等于输入（降级绝不吞内容）。
func TestToolSimScanNonContractContentPreserved(t *testing.T) {
	inputs := []string{
		"plain answer",
		"a < b and c > d",
		"<div>html</div>",
		"<tool_result>x</tool_result>",
		"<tool_use name=\"Read\">{bad</tool_use>",
		"半截 <tool_use name=\"Read\">{\"a\":1}",
	}
	for _, in := range inputs {
		var b strings.Builder
		for _, sg := range ScanText(in) {
			if sg.Call != nil {
				t.Fatalf("%q 不得合成调用", in)
			}
			b.WriteString(sg.Text)
		}
		if b.String() != in {
			t.Fatalf("内容损失：%q → %q", in, b.String())
		}
	}
}

// TestToolSimScanChunkBoundaryInTag 定点切割：标签名/属性/闭标签边界各切一刀。
func TestToolSimScanChunkBoundaryInTag(t *testing.T) {
	const in = `pre <tool_use name="Read">{"file_path":"a.txt"}</tool_use> post`
	const want = `t:pre |c:Read({"file_path":"a.txt"})[]|t: post`
	for cut := 1; cut < len(in); cut++ {
		var sc Scanner
		segs := sc.Feed(in[:cut])
		segs = append(segs, sc.Feed(in[cut:])...)
		segs = append(segs, sc.Flush()...)
		if got := segString(segs); got != want {
			t.Fatalf("cut=%d: %q, want %q", cut, got, want)
		}
	}
}

// TestToolSimBuildInjectionContract 注入段 = 契约正文 + 清单 + tool_choice 句（结构面）。
func TestToolSimBuildInjectionContract(t *testing.T) {
	tools := []ir.ToolDef{
		{Name: "Read", Description: "Reads a file.", InputSchema: json.RawMessage(`{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}`)},
		{Name: "", Description: "无名工具跳过"},
	}
	inj := BuildInjection(tools, json.RawMessage(`"required"`))
	if !strings.HasPrefix(inj, "## Tool calling") {
		t.Fatalf("段首:\n%.120s", inj)
	}
	if !strings.Contains(inj, "## Available tools") {
		t.Fatalf("缺清单段头")
	}
	if !strings.Contains(inj, "- Read(file_path: string (required)): Reads a file.") {
		t.Fatalf("清单行:\n%s", inj)
	}
	if strings.Contains(inj, "无名工具跳过") {
		t.Fatalf("无名工具不得进清单:\n%s", inj)
	}
	if !strings.Contains(inj, `Tool choice is set to "required": you must call at least one tool.`) {
		t.Fatalf("缺 tool_choice 句:\n%s", inj)
	}
	if got := BuildInjection(nil, nil); got != "" {
		t.Fatalf("无工具 → 空注入，got %q", got)
	}
}

// TestToolSimToolChoiceInstruction tool_choice 映射表（BK tool.go:40-72 同表 + 对象形态）。
func TestToolSimToolChoiceInstruction(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{``, ""},
		{`null`, ""},
		{`"auto"`, ""},
		{`"none"`, `Tool choice is set to "none": do not call any tool, answer in plain text.`},
		{`"required"`, `Tool choice is set to "required": you must call at least one tool.`},
		{`"any"`, `Tool choice is set to "required": you must call at least one tool.`},
		{`{"type":"tool","name":"Bash"}`, `Tool choice is set to the tool "Bash": you must call that tool.`},
		{`{"type":"function","function":{"name":"Bash"}}`, `Tool choice is set to the tool "Bash": you must call that tool.`},
		{`{"type":"any"}`, `Tool choice is set to "required": you must call at least one tool.`},
		{`{"type":"none"}`, `Tool choice is set to "none": do not call any tool, answer in plain text.`},
		{`{"type":"weird"}`, ""},
		{`garbage`, ""},
	}
	for _, tc := range cases {
		var raw json.RawMessage
		if tc.in != "" {
			raw = json.RawMessage(tc.in)
		}
		if got := ToolChoiceInstruction(raw); got != tc.want {
			t.Fatalf("ToolChoiceInstruction(%s) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
