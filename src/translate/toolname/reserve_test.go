package toolname

// A2A-L19-W80-4 回归面：合法名预约 + Build 幂等 + 撞名确定性。
//
// 缺陷复现（A2A-L19-W80-3 §Integration opportunities #2）：Build 按声明顺序
// 边净化边占用，`foo/bar` 先净化成 `foo_bar` 并占用后，**合法**名 `foo_bar`
// 被当作撞名改写成 `foo_bar_2`；二次 Build 再把 `foo_bar_2` 顶成 `foo_bar_2`
// （两次结果同形）。契约要求「只改需要改的」且确定性——合法名必须先预约。

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// TestBuildReservesValidNames：合法名（foo_bar）声明在非法名（foo/bar）之后，
// 也必须原样保留；非法名让路拿到不同的净化名，且映射值唯一。
func TestBuildReservesValidNames(t *testing.T) {
	table := Build([]ir.ToolDef{{Name: "foo/bar"}, {Name: "foo_bar"}})

	if got, ok := table["foo_bar"]; ok {
		t.Fatalf("合法名被改写：foo_bar → %q（表 = %v）", got, table)
	}
	got := table["foo/bar"]
	if got != "foo_bar_2" {
		t.Fatalf("非法名映射 = %q（want foo_bar_2；表 = %v）", got, table)
	}
	if !upstreamNameOK(got) {
		t.Fatalf("净化名不合法：%q", got)
	}
	assertUniqueSanitized(t, table)

	// 合法名声明在前时结果相同（预约与声明顺序无关）。
	front := Build([]ir.ToolDef{{Name: "foo_bar"}, {Name: "foo/bar"}})
	if !reflect.DeepEqual(front, table) {
		t.Fatalf("声明顺序影响映射：front=%v back=%v", front, table)
	}
}

// TestBuildIdempotent：Build 的输出全部是合法名；以其值作为新工具清单再 Build
// 必须返回 nil（无二次改写）。
func TestBuildIdempotent(t *testing.T) {
	tools := []ir.ToolDef{
		{Name: "foo/bar"},
		{Name: "foo_bar"},
		{Name: strings.Repeat("b", MaxLen) + "XXXX"},
		{Name: strings.Repeat("b", MaxLen) + "YYYY"},
		{Name: "工具名"},
		{Name: "9lives"},
		{Name: "ok.name:v2-x"},
	}
	table := Build(tools)
	if len(table) == 0 {
		t.Fatalf("测试构造错误：表为空（应有多条变化）")
	}

	sanitized := make([]ir.ToolDef, len(tools))
	for i, td := range tools {
		sanitized[i] = td
		if s, ok := table[td.Name]; ok {
			sanitized[i].Name = s
		}
		if !upstreamNameOK(sanitized[i].Name) {
			t.Fatalf("Build 输出不合法：%q（原 %q）", sanitized[i].Name, td.Name)
		}
	}
	if second := Build(sanitized); second != nil {
		t.Fatalf("二次 Build 非恒等：%v", second)
	}

	// 表本身必须对其输出闭合：用同一张表再走一遍请求清洗（SanitizeRequest 就是
	// 「第二遍」）不得再改写——W80-3 的实测形态 once=[foo_bar foo_bar_2] →
	// twice=[foo_bar_2 foo_bar_2]，根因是表把合法名 foo_bar 也收成了键。
	again := &ir.Request{Tools: append([]ir.ToolDef(nil), sanitized...)}
	SanitizeRequest(again, table)
	for i := range again.Tools {
		if again.Tools[i].Name != sanitized[i].Name {
			t.Fatalf("表对自身输出不闭合：%q → %q（want 原样）",
				sanitized[i].Name, again.Tools[i].Name)
		}
	}
}

// TestBuildCollisionDeterministic：两个非法名净化后同形、中间夹一个合法名——
// 合法名保留，两个非法名按声明顺序拿 `_2`/`_3`，重复运行结果一致且唯一。
func TestBuildCollisionDeterministic(t *testing.T) {
	tools := []ir.ToolDef{{Name: "a/b"}, {Name: "a_b"}, {Name: "a?b"}}

	want := map[string]string{"a/b": "a_b_2", "a?b": "a_b_3"}
	first := Build(tools)
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("映射 = %v（want %v）", first, want)
	}
	assertUniqueSanitized(t, first)

	for i := range 64 {
		if got := Build(tools); !reflect.DeepEqual(got, first) {
			t.Fatalf("第 %d 次运行不确定：%v != %v", i+1, got, first)
		}
	}

	// 合法名移到末尾不改变非法名的分配（预约先于净化）。
	reordered := Build([]ir.ToolDef{{Name: "a/b"}, {Name: "a?b"}, {Name: "a_b"}})
	if !reflect.DeepEqual(reordered, want) {
		t.Fatalf("合法名位置影响分配：%v（want %v）", reordered, want)
	}

	// 反向表无损（Build 保证净化名唯一）。
	rev := Reverse(first)
	for orig, san := range first {
		if rev[san] != orig {
			t.Fatalf("反向表丢 %q→%q：%v", san, orig, rev)
		}
	}
}

// assertUniqueSanitized 断言表中净化名两两不同。
func assertUniqueSanitized(t *testing.T, table map[string]string) {
	t.Helper()
	seen := make(map[string]string, len(table))
	for orig, san := range table {
		if prev, dup := seen[san]; dup {
			t.Fatalf("净化名重复：%q 同时来自 %q 与 %q", san, prev, orig)
		}
		seen[san] = orig
	}
}
