package toolsim

// W21-2 ② 转义约定（U3 出口侧契约）的测试向量：注入/历史回放文本**不做 XML
// 转义**——参数 JSON 字符串值里的 `</tool_use>` 子串由「候选试 JSON」兜底；
// 讲解/引用字面量按双判据处置（成对 + 体为合法 JSON 对象 → 调用；否则文本降级）。
// 约定正文见 scan.go §转义约定 / claude_web/render.go flattenBlock。
//
// 反证锚点：放宽 validArgs 的 JSON 校验（`return t, true`）→ 子串场景红（首个
// 候选被误收，参数在子串处被截断）；stBody 只试首个闭标签（删候选循环）→ 同组红。

import (
	"strings"
	"testing"
)

// TestEscapeSubstringInArgs 参数内 `</tool_use>` 子串：候选试 JSON 取第一个可解析
// 候选——参数逐字节保真（不截断、不重编码），子串自然落入下一候选。
func TestEscapeSubstringInArgs(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			"子串单次（首个候选不可解析）",
			`<tool_use name="Bash">{"command":"echo '</tool_use>'"}</tool_use>`,
			`c:Bash({"command":"echo '</tool_use>'"})[]`,
		},
		{
			"子串两次出现",
			`<tool_use name="Write">{"a":"</tool_use>","b":"x</tool_use>y"}</tool_use>`,
			`c:Write({"a":"</tool_use>","b":"x</tool_use>y"})[]`,
		},
		{
			"子串在数组/嵌套对象值里",
			`<tool_use name="A">{"arr":["</tool_use>",{"k":"</tool_use>"}]}</tool_use>`,
			`c:A({"arr":["</tool_use>",{"k":"</tool_use>"}]})[]`,
		},
		{
			"子串后紧跟真实闭标签",
			`<tool_use name="A">{"v":"tail</tool_use>"}</tool_use>`,
			`c:A({"v":"tail</tool_use>"})[]`,
		},
		{
			"体带空白（候选判定按 trim 后原文返回）",
			`<tool_use name="A"> {"a":"</tool_use>"} </tool_use>`,
			`c:A({"a":"</tool_use>"})[]`,
		},
		{
			"嵌套开标签字面量在参数内（无碍——只找闭标签候选）",
			`<tool_use name="A">{"code":"<tool_use name=\"B\">"}</tool_use>`,
			`c:A({"code":"<tool_use name=\"B\">"})[]`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := segString(ScanText(tc.in)); got != tc.want {
				t.Fatalf("ScanText(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestEscapeFirstCandidateWins 候选先到先得：体已合法的最早候选即胜出，其后内容
// （含裸 `</tool_use>` 字面量）按普通文本继续扫描——不回看后续出现位置。
func TestEscapeFirstCandidateWins(t *testing.T) {
	in := `<tool_use name="A">{"n":1}</tool_use>{"b":"</tool_use>"}</tool_use>`
	want := `c:A({"n":1})[]|t:{"b":"</tool_use>"}</tool_use>`
	if got := segString(ScanText(in)); got != want {
		t.Fatalf("ScanText = %q, want %q", got, want)
	}

	// 连续两块（无分隔）：先到先得在块序列上的形态。
	in2 := `<tool_use name="A">{"n":1}</tool_use><tool_use name="B">{"n":"</tool_use>"}</tool_use>`
	want2 := `c:A({"n":1})[]|c:B({"n":"</tool_use>"})[]`
	if got := segString(ScanText(in2)); got != want2 {
		t.Fatalf("ScanText = %q, want %q", got, want2)
	}
}

// TestEscapeCandidateExhaustedDegrades 候选全不可解析（子串后无真实闭标签）→
// 未判定（Flush 文本降级）：绝不合成块、内容逐字节保留。
func TestEscapeCandidateExhaustedDegrades(t *testing.T) {
	for _, in := range []string{
		`<tool_use name="A">{"v":"</tool_use>"}`,
		`<tool_use name="A">{"v":1}`,
	} {
		segs := ScanText(in)
		var b strings.Builder
		for _, sg := range segs {
			if sg.Call != nil {
				t.Fatalf("%q 不得合成调用（%+v）", in, sg.Call)
			}
			b.WriteString(sg.Text)
		}
		if b.String() != in {
			t.Fatalf("内容损失：%q → %q", in, b.String())
		}
	}
}

// TestEscapeLiteralExplanationScenarios 讲解/引用场景：成对标签 + 体非 JSON 对象 /
// 未闭合标签 / 裸闭标签 → 一律原样文本降级（双判据的语义边界——与真实调用形态
// 不可区分的完整合法示例会被判为调用，见 TestEscapeFirstCandidateWins）。
func TestEscapeLiteralExplanationScenarios(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"体非 JSON（讲解占位）", `use <tool_use name="Read">the file path here</tool_use> syntax`},
		{"未闭合开标签", `write <tool_use name="Read">{"file_path":"a.txt"} without the closing tag`},
		{"裸闭标签", `closing tag is </tool_use> alone`},
		{"体为数组不是对象", `<tool_use name="Read">[1,2]</tool_use>`},
		{"无 name 属性", `<tool_use>{"a":1}</tool_use>`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := segString(ScanText(tc.in))
			want := "t:" + tc.in
			if got != want {
				t.Fatalf("ScanText(%q) = %q, want %q", tc.in, got, want)
			}
		})
	}
}

// TestEscapeSubstringByteWiseCrossChunk 子串场景的逐字节喂入（最细粒度跨片切割）
// 与整段扫描同结果——候选试 JSON 跨片缓冲不丢字节。
func TestEscapeSubstringByteWiseCrossChunk(t *testing.T) {
	samples := []string{
		`<tool_use name="Bash">{"command":"echo '</tool_use>'"}</tool_use>`,
		`<tool_use name="Write">{"a":"</tool_use>","b":"x</tool_use>y"}</tool_use>`,
		`<tool_use name="A">{"v":"</tool_use>"}`,
		`pre <tool_use name="A">{"n":1}</tool_use>{"b":"</tool_use>"}</tool_use> post`,
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
