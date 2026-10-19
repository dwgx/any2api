package hygiene

import (
	"strings"
	"testing"
)

// 净化：字符集（Anthropic ^[a-zA-Z0-9_-]+$；roo-code tool-id.ts:9-13 同款）——
// 非法字符 → '_'；合法值直过。
func TestSanitizeToolIDCharset(t *testing.T) {
	cases := []struct{ in, want string }{
		{"toolu_01ABC-xyz", "toolu_01ABC-xyz"}, // 合法直过（不重写）
		{"call_abc.def", "call_abc_def"},       // '.' 非法
		{"a/b:c", "a_b_c"},                     // '/' ':' 非法
		{"toolu_01 2", "toolu_01_2"},           // 空格非法
		{"", ""},                               // 空串原样（合成由调用方决定）
		{"___", "___"},                         // 下划线合法
	}
	for _, c := range cases {
		if got := SanitizeToolID(c.in); got != c.want {
			t.Errorf("SanitizeToolID(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// 长度上限：>64 → 前缀 + sha256 后缀截断（roo 真实报错 69 字符的复现面）；
// 结果恒 ≤64、前缀保留、确定性、**幂等**（回传 id 不被二次改写）。
func TestSanitizeToolIDLengthCapIdempotent(t *testing.T) {
	long := "call_" + strings.Repeat("x", 64) // 69 字符（roo tool-id.spec.ts 复现值）
	if len(long) != 69 {
		t.Fatalf("fixture = %d 字符（want 69）", len(long))
	}
	got := SanitizeToolID(long)
	if len(got) != MaxToolIDLen {
		t.Fatalf("len = %d（want %d）: %q", len(got), MaxToolIDLen, got)
	}
	if !strings.HasPrefix(got, "call_xxxx") {
		t.Fatalf("前缀未保留: %q", got)
	}
	if !strings.Contains(got, "_") || got[len(got)-17] != '_' {
		t.Fatalf("哈希后缀形态: %q", got)
	}
	if again := SanitizeToolID(long); again != got {
		t.Fatalf("非确定性: %q vs %q", got, again)
	}
	// 幂等：净化值再净化不变（客户端原样回传历史时配对键稳定）。
	for _, in := range []string{long, "call_abc.def", "toolu_01", got} {
		once := SanitizeToolID(in)
		if twice := SanitizeToolID(once); twice != once {
			t.Errorf("非幂等: Sanitize(%q) = %q, 再净化 = %q", in, once, twice)
		}
	}
}

// 不同长 id 的截断后缀不同（哈希后缀保唯一——roo 的撞 id 判据）。
func TestSanitizeToolIDLongDistinctSuffix(t *testing.T) {
	a := SanitizeToolID("call_" + strings.Repeat("x", 64))
	b := SanitizeToolID("call_" + strings.Repeat("x", 63) + "y")
	if a == b {
		t.Fatalf("不同原 id 截断后撞 id: %q", a)
	}
}

// 唯一合成：<base>_synth_<32hex> 形态、每调用唯一、字符集合规、净化幂等
// （合成值过净化器不变——防止「合成 → 净化」二次改写）。
func TestSynthUniqueIDShapeUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := range 2000 {
		id := SynthUniqueID("resp")
		if !strings.HasPrefix(id, "resp_synth_") {
			t.Fatalf("形态: %q", id)
		}
		body := strings.TrimPrefix(id, "resp_synth_")
		if len(body) != 32 {
			t.Fatalf("hex 长度 = %d（want 32）: %q", len(body), id)
		}
		if len(id) > MaxToolIDLen {
			t.Fatalf("超长: %q", id)
		}
		if got := SanitizeToolID(id); got != id {
			t.Fatalf("合成值被净化改写: %q → %q", id, got)
		}
		if seen[id] {
			t.Fatalf("重复 id（第 %d 次）: %q", i, id)
		}
		seen[id] = true
	}
	if id := SynthUniqueID("int"); !strings.HasPrefix(id, "int_synth_") {
		t.Fatalf("base 前缀: %q", id)
	}
}

// 确定性合成（工具调用 id / 输出项 id）：形态 `<base>_synth_<n>`、可复现、
// 字符集合规、净化幂等；WireToolID 对合法/非法/缺失 id 的三分派（seed 空 =
// 块序回落形态；seed 非空 = 响应 id 派生）。
func TestSynthIndexedAndWireToolID(t *testing.T) {
	if a, b := SynthIndexedID("toolu", 3), SynthIndexedID("toolu", 3); a != b || a != "toolu_synth_3" {
		t.Fatalf("确定性合成: %q / %q", a, b)
	}
	if got := WireToolID("call", "call_ok", "resp_x", 0); got != "call_ok" {
		t.Fatalf("合法 id 被改写: %q", got)
	}
	if got := WireToolID("call", "call_bad.chars", "resp_x", 0); got != "call_bad_chars" {
		t.Fatalf("非法字符未净化: %q", got)
	}
	// seed 空（请求面/上游不给响应 id）→ 块序回落。
	if got := WireToolID("call", "", "", 7); got != "call_synth_7" {
		t.Fatalf("seed 空未走块序回落: %q", got)
	}
	// seed 非空 → 响应 id 派生（32hex，响应内确定、跨响应不同）。
	if got := WireToolID("call", "", "resp_x", 7); got != SynthDerivedID("call", "resp_x", 7) {
		t.Fatalf("seed 非空未走派生: %q", got)
	}
	long := "call_" + strings.Repeat("x", 64)
	got := WireToolID("call", long, "resp_x", 0)
	if len(got) != MaxToolIDLen || got == long {
		t.Fatalf("超长未截断: %q（len=%d）", got, len(got))
	}
}

// 响应 id 派生合成（W5-29 跨响应唯一）：同 (seed,n) 恒同值（渲染不动点/录帧
// 回放硬需求）；不同响应 id 同槽位序号不撞 id（客户端历史配对/日志关联）；
// 形态 `<base>_synth_<32hex>`、净化幂等（合成值过净化器不被二次改写）。
func TestSynthDerivedIDResponseScoped(t *testing.T) {
	a1, a2 := SynthDerivedID("call", "resp_a", 0), SynthDerivedID("call", "resp_a", 0)
	if a1 != a2 {
		t.Fatalf("非确定性: %q vs %q（渲染不动点会破）", a1, a2)
	}
	if !strings.HasPrefix(a1, "call_synth_") || len(a1) != len("call_synth_")+32 {
		t.Fatalf("形态: %q", a1)
	}
	if got := SanitizeToolID(a1); got != a1 {
		t.Fatalf("合成值被净化改写: %q → %q", a1, got)
	}
	// 跨响应唯一：同槽位序号、不同响应 id → 不同 id。
	if b := SynthDerivedID("call", "resp_b", 0); b == a1 {
		t.Fatalf("跨响应撞 id: %q（resp_a vs resp_b 同槽位）", b)
	}
	// 同响应不同槽位 → 不同 id；分隔符防拼接歧义（"ab"+N vs "a"+"bN"）。
	if c := SynthDerivedID("call", "resp_a", 1); c == a1 {
		t.Fatalf("同响应不同槽位撞 id: %q", c)
	}
	if SynthDerivedID("call", "ab", 1) == SynthDerivedID("call", "a", 1) {
		t.Fatalf("seed 拼接歧义未消（\\x00 分隔符失效）")
	}
	// seed 空 → 块序回落（确定性保留）。
	if got := SynthDerivedID("toolu", "", 3); got != "toolu_synth_3" {
		t.Fatalf("seed 空回落: %q", got)
	}
}

// 孤儿结果折叠文案：内容保留；空白 → 显式占位（绝不产出空 text 块）；
// 缺配对结果文案恒非空（与 CPA 逐字同款）。
func TestFoldOrphanAndInterruptedTexts(t *testing.T) {
	if got := FoldOrphanToolResultText("file contents"); got != "file contents" {
		t.Fatalf("内容被改写: %q", got)
	}
	for _, in := range []string{"", "   ", "\n\t"} {
		if got := FoldOrphanToolResultText(in); got != EmptyResultText {
			t.Errorf("FoldOrphanToolResultText(%q) = %q, want %q", in, got, EmptyResultText)
		}
	}
	if InterruptedResultText == "" {
		t.Fatal("缺配对结果文案不得为空串")
	}
}
