package toolname

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// TestSanitizeLongNameTruncates64 r14 形态的最小复现：84 字客户端名（CC 的 MCP
// 工具实测长度）→ 净化名 ≤64 且合法；表只收变化条目。
func TestSanitizeLongNameTruncates64(t *testing.T) {
	long := "mcp__p67__p67_xxx_" + strings.Repeat("a", 66) // 84 字
	if len(long) != 84 {
		t.Fatalf("测试构造错误：len=%d", len(long))
	}
	table := Build([]ir.ToolDef{{Name: long}, {Name: "short_tool"}})
	if len(table) != 1 {
		t.Fatalf("表 = %v（want 只收长名一条；合法名不进表）", table)
	}
	got, ok := table[long]
	if !ok {
		t.Fatalf("长名不在表里：%v", table)
	}
	if len(got) != MaxLen {
		t.Fatalf("净化名长度 = %d（want %d）", len(got), MaxLen)
	}
	if !upstreamNameOK(got) {
		t.Fatalf("净化名不合法：%q", got)
	}
	if got != long[:MaxLen] {
		t.Fatalf("净化名 = %q（want 前 64 字）", got)
	}
}

// TestSanitizeIllegalChars：字符集归一（空格/斜杠/中文 → `_`；首字符数字 → `_` 前缀）。
func TestSanitizeIllegalChars(t *testing.T) {
	cases := []struct{ in, want string }{
		{"read file", "read_file"},
		{"foo/bar", "foo_bar"},
		{"工具名", "___"},
		{"9lives", "_9lives"},
		{"ok.name:v2-x", "ok.name:v2-x"},
	}
	for _, c := range cases {
		table := Build([]ir.ToolDef{{Name: c.in}})
		got := table[c.in]
		if c.in == c.want {
			if _, ok := table[c.in]; ok {
				t.Fatalf("%q 合法却进了表", c.in)
			}
			continue
		}
		if got != c.want {
			t.Fatalf("%q → %q（want %q）", c.in, got, c.want)
		}
	}
}

// TestCollisionDisambiguation：两个长名截断后同形 → 第二个追加 `_2`（仍 ≤64、
// 合法、两名字不撞）。
func TestCollisionDisambiguation(t *testing.T) {
	prefix := strings.Repeat("b", 64)
	a := prefix + "XXXX"
	b := prefix + "YYYY"
	if len(a) != 68 || a[:MaxLen] != b[:MaxLen] {
		t.Fatalf("构造错误：两名应 >64 且前 64 字同形（a=%d,b=%d）", len(a), len(b))
	}
	table := Build([]ir.ToolDef{{Name: a}, {Name: b}})
	sa, sb := table[a], table[b]
	if sa == "" || sb == "" {
		t.Fatalf("表缺条目：%v", table)
	}
	if sa == sb {
		t.Fatalf("撞名未消歧：%q == %q", sa, sb)
	}
	if len(sb) > MaxLen || !upstreamNameOK(sb) {
		t.Fatalf("消歧名不合法/超长：%q (len=%d)", sb, len(sb))
	}
	if !strings.HasSuffix(sb, "_2") {
		t.Fatalf("消歧后缀 = %q（want _2 结尾）", sb)
	}
}

// TestRoundTripRequestResponse：r14 双向——请求清洗（工具定义 + 历史调用 +
// tool_choice 引用）→ 上游按净化名回调 → 响应还原成原名。
func TestRoundTripRequestResponse(t *testing.T) {
	long := "mcp__srv__" + strings.Repeat("z", 80)
	req := &ir.Request{
		Model: "m",
		Tools: []ir.ToolDef{{Name: long, Description: "d"}, {Name: "plain"}},
		ToolChoice: json.RawMessage(`{"type":"function","function":{"name":` +
			string(mustMarshal(long)) + `}}`),
		Messages: []ir.Message{{
			Role: ir.RoleAssistant,
			Blocks: []ir.ContentBlock{{
				Kind:     ir.BlockToolUse,
				ToolCall: &ir.ToolCall{ID: "tu_1", Name: long, Arguments: "{}"},
			}},
		}},
	}
	table := Build(req.Tools)
	SanitizeRequest(req, table)
	san := table[long]
	if req.Tools[0].Name != san {
		t.Fatalf("工具定义未清洗：%q", req.Tools[0].Name)
	}
	if req.Messages[0].Blocks[0].ToolCall.Name != san {
		t.Fatalf("历史调用未清洗：%q", req.Messages[0].Blocks[0].ToolCall.Name)
	}
	if !strings.Contains(string(req.ToolChoice), san) || strings.Contains(string(req.ToolChoice), long) {
		t.Fatalf("tool_choice 未清洗：%s", req.ToolChoice)
	}
	// 幂等：二次清洗不再变化。
	before := req.Tools[0].Name
	SanitizeRequest(req, table)
	if req.Tools[0].Name != before {
		t.Fatalf("SanitizeRequest 非幂等：%q → %q", before, req.Tools[0].Name)
	}

	// 响应侧：上游按净化名回调 → 还原成原名（r14 的客户端可执行前提）。
	resp := &ir.Response{Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{{
		Kind:     ir.BlockToolUse,
		ToolCall: &ir.ToolCall{ID: "call_1", Name: san, Arguments: `{"x":1}`},
	}}}}
	RestoreResponse(resp, Reverse(table))
	got := resp.Message.Blocks[0].ToolCall.Name
	if got != long {
		t.Fatalf("响应还原 = %q（want 原名 %q）", got, long)
	}

	// 流式：block_start 的 ToolName 同表还原。
	events := []ir.StreamEvent{
		{Kind: ir.EventBlockStart, BlockKind: ir.BlockToolUse, ToolName: san},
		{Kind: ir.EventBlockStart, BlockKind: ir.BlockText},
	}
	RestoreEvents(events, Reverse(table))
	if events[0].ToolName != long {
		t.Fatalf("流式还原 = %q（want 原名）", events[0].ToolName)
	}
}

// TestNilTableNoOp：表 nil（无长名/非法名）时三个变换都是直过（既有路径零影响）。
func TestNilTableNoOp(t *testing.T) {
	req := &ir.Request{Tools: []ir.ToolDef{{Name: "ok_name"}}}
	SanitizeRequest(req, Build(req.Tools))
	if req.Tools[0].Name != "ok_name" {
		t.Fatalf("合法名被改写：%q", req.Tools[0].Name)
	}
	resp := &ir.Response{Message: ir.Message{Blocks: []ir.ContentBlock{{
		ToolCall: &ir.ToolCall{Name: "upstream_native_name"},
	}}}}
	RestoreResponse(resp, nil)
	if resp.Message.Blocks[0].ToolCall.Name != "upstream_native_name" {
		t.Fatalf("nil 表改写了名字：%q", resp.Message.Blocks[0].ToolCall.Name)
	}
}

// TestBuildEmpty：空工具清单 → nil（不分配）。
func TestBuildEmpty(t *testing.T) {
	if Build(nil) != nil || Build([]ir.ToolDef{{Name: ""}}) != nil {
		t.Fatal("空清单应返回 nil")
	}
}
