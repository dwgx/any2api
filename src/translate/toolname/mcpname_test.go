package toolname

// W21-2 ① 复核：CC 形态 `mcp__server__tool` 名的两向（长名截断 / 非法字符归一）——
// 长名截断保持前缀（上游按截断名回调仍可定位）、非法字符按字符集归一、合法短名
// 直过（只改需要改的）；双向映射（Build → Reverse）无损。
//
// 既有覆盖见 toolname_test.go（84 字截断 / 字符集归一 / 撞名消歧 / r14 往返）；
// 本文件补 MCP 形态（`mcp__` 前缀 + `__` 分隔）的定点向量。

import (
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

func TestMCPFormNameLongAndDirty(t *testing.T) {
	long := "mcp__srv__" + strings.Repeat("c", 74) // 84 字（W19 实抓量级）
	dirty := "mcp__srv__tools/list 读"              // 非法字符：/ 空格 中文
	short := "mcp__srv__tool"                      // 合法短名（MCP 常规形态）

	table := Build([]ir.ToolDef{{Name: long}, {Name: dirty}, {Name: short}})
	if len(table) != 2 {
		t.Fatalf("表 = %v（want 长名 + 脏名；合法短名不进表）", table)
	}

	sanLong := table[long]
	if len(sanLong) != MaxLen || sanLong != long[:MaxLen] {
		t.Fatalf("长名净化 = %q (len=%d, want 前缀 %d 字)", sanLong, len(sanLong), MaxLen)
	}
	if !upstreamNameOK(sanLong) {
		t.Fatalf("长名净化不合法：%q", sanLong)
	}
	if !strings.HasPrefix(sanLong, "mcp__srv__") {
		t.Fatalf("截断破坏 MCP 前缀：%q", sanLong)
	}

	sanDirty := table[dirty]
	if sanDirty != "mcp__srv__tools_list__" || !upstreamNameOK(sanDirty) {
		t.Fatalf("脏名净化 = %q（want mcp__srv__tools_list__ 且合法）", sanDirty)
	}
	if _, ok := table[short]; ok {
		t.Fatalf("合法短名被改写：%q", table[short])
	}

	// 双向无损：净化名 → 原名（响应侧还原查的就是反向表）。
	rev := Reverse(table)
	if rev[sanLong] != long || rev[sanDirty] != dirty {
		t.Fatalf("反向表 = %v（want %q→%q, %q→%q）", rev, sanLong, long, sanDirty, dirty)
	}
}
