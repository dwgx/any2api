package claude_messages

// W14：claude_messages 入口接受 messages[].role:"system"（CC 2.1.299 实发形态；
// 依据 research/briefs/reports/A2A-L11-W13-2.md §3b）。
//
// 语料：testdata/cc-299-req.json —— W13-2 实发请求的脱敏形状（结构逐位保留：
// 顶层 system 3 元素 / messages[0] user 4 块 / messages[1] role:"system" 1 块
// 带 cache_control + 消息级 output_config / 22 工具定义；文本裁剪并加
// "(trimmed in fixture)" 标记，设备/会话标识与用户路径替换为占位符）。
//
// 覆盖：实发形状 parse（不报错 + IR 形状）→ claude 自出口合并（顶层 system
// 原文 + 消息面 system 追加）；边界（首/中/尾位置、多条、空内容、与顶层并存、
// 多块不拆、白名单外角色仍拒）。
//
// 反证（回退即红，见报告 §反证）：去掉 parseMessages 的 RoleSystem 白名单 →
// TestSysRoleParseRealShape / TestSysRoleBoundaries 红；回退 mergeSystemSeed →
// TestSysRoleClaudeSelfExitMerge 红。

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

func loadSysRoleFixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/cc-299-req.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return raw
}

// TestSysRoleParseRealShape W13-2 实发形状 parse：角色序列 / 块序 / Extra 保真。
func TestSysRoleParseRealShape(t *testing.T) {
	req, err := ParseRequest(loadSysRoleFixture(t))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	// IR 序：顶层 system 3 元素（各一条）→ user → messages[1] 的 system。
	wantRoles := []ir.Role{ir.RoleSystem, ir.RoleSystem, ir.RoleSystem, ir.RoleUser, ir.RoleSystem}
	if len(req.Messages) != len(wantRoles) {
		t.Fatalf("messages = %d, want %d: %+v", len(req.Messages), len(wantRoles), req.Messages)
	}
	for i, want := range wantRoles {
		if req.Messages[i].Role != want {
			t.Fatalf("m%d role = %q, want %q", i, req.Messages[i].Role, want)
		}
	}
	// 顶层 system 逐元素不合并（CC A9）：元素 1/2 各自一条消息、单块。
	if len(req.Messages[1].Blocks) != 1 ||
		req.Messages[1].Blocks[0].Text != "You are a Claude agent, built on Anthropic's Claude Agent SDK." {
		t.Fatalf("m1 = %+v", req.Messages[1])
	}
	// user：4 块原样成组（不拆——无 tool_result）。
	if got := req.Messages[3].Blocks; len(got) != 4 || got[3].Text != "用一句话介绍你自己" {
		t.Fatalf("user blocks = %+v", got)
	}
	// messages[1] 的 system：单块、块序原样、位置在 user 之后（中位/尾位保留）。
	sys := req.Messages[4]
	if len(sys.Blocks) != 1 || sys.Blocks[0].Kind != ir.BlockText {
		t.Fatalf("system msg = %+v", sys)
	}
	if !strings.Contains(sys.Blocks[0].Text, "# Environment") ||
		!strings.Contains(sys.Blocks[0].Text, "Today's date is 2026-10-14.") {
		t.Fatalf("system msg text = %.200q", sys.Blocks[0].Text)
	}
	// 消息级未建模字段进 Extra（output_config）——与 user/assistant 同口径。
	if _, ok := sys.Extra["output_config"]; !ok {
		t.Fatalf("system msg extra = %v, want output_config", sys.Extra)
	}
	// 顶层 system 原文保真（Extra["system"]，3 元素）。
	var raw []json.RawMessage
	if err := json.Unmarshal(req.Extra["system"], &raw); err != nil || len(raw) != 3 {
		t.Fatalf("Extra[system] = %s (err=%v)", req.Extra["system"], err)
	}
}

// TestSysRoleClaudeSelfExitMerge claude 自出口：顶层 system 原文逐字保留 + 消息面
// system 块追加为数组元素（不丢内容——合并规则见 mergeSystemSeed）。
func TestSysRoleClaudeSelfExitMerge(t *testing.T) {
	req, err := ParseRequest(loadSysRoleFixture(t))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	var w map[string]json.RawMessage
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	// system = 原文 3 元素（逐字，含元素级 cache_control）+ 追加 1 元素（env 块）。
	var arr []json.RawMessage
	if err := json.Unmarshal(w["system"], &arr); err != nil {
		t.Fatalf("system unmarshal: %v (%s)", err, w["system"])
	}
	if len(arr) != 4 {
		t.Fatalf("system elements = %d, want 4: %s", len(arr), w["system"])
	}
	var orig []json.RawMessage
	if err := json.Unmarshal(req.Extra["system"], &orig); err != nil {
		t.Fatalf("orig unmarshal: %v", err)
	}
	for i := range orig {
		// 语义相等（渲染管线统一 compact 空白；元素内容/cache_control 逐字段保留）。
		if normalizeJSON(t, arr[i]) != normalizeJSON(t, orig[i]) {
			t.Fatalf("system[%d] 原文被改写:\n got %s\nwant %s", i, arr[i], orig[i])
		}
	}
	var el3 struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(arr[3], &el3); err != nil {
		t.Fatalf("system[3] unmarshal: %v", err)
	}
	if el3.Type != "text" || !strings.Contains(el3.Text, "# Environment") {
		t.Fatalf("system[3] = %+v, want env text block", el3)
	}
	// messages[]：system 已提升 → 只剩 user（4 块）。
	var msgs []json.RawMessage
	if err := json.Unmarshal(w["messages"], &msgs); err != nil || len(msgs) != 1 {
		t.Fatalf("messages = %s (err=%v), want 1（system 已提升）", w["messages"], err)
	}
	var m0 struct {
		Role    string            `json:"role"`
		Content []json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(msgs[0], &m0); err != nil {
		t.Fatalf("messages[0]: %v", err)
	}
	if m0.Role != "user" || len(m0.Content) != 4 {
		t.Fatalf("messages[0] = role %q, %d blocks", m0.Role, len(m0.Content))
	}
}

// TestSysRoleBoundaries 边界：任意位置（首/中/尾）、多条、空内容、与顶层 system
// 并存、多块不拆、白名单外角色仍拒。
func TestSysRoleBoundaries(t *testing.T) {
	parse := func(t *testing.T, body string) (*ir.Request, error) {
		t.Helper()
		return ParseRequest([]byte(body))
	}

	t.Run("first_position", func(t *testing.T) {
		req, err := parse(t, `{"model":"m","max_tokens":1,"messages":[
			{"role":"system","content":"FIRST"},{"role":"user","content":"hi"}]}`)
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		if len(req.Messages) != 2 || req.Messages[0].Role != ir.RoleSystem ||
			req.Messages[0].Blocks[0].Text != "FIRST" || req.Messages[1].Role != ir.RoleUser {
			t.Fatalf("messages = %+v", req.Messages)
		}
	})

	t.Run("middle_position", func(t *testing.T) {
		req, err := parse(t, `{"model":"m","max_tokens":1,"messages":[
			{"role":"user","content":"a"},{"role":"system","content":"MID"},{"role":"user","content":"b"}]}`)
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		want := []ir.Role{ir.RoleUser, ir.RoleSystem, ir.RoleUser}
		if len(req.Messages) != 3 {
			t.Fatalf("messages = %+v", req.Messages)
		}
		for i, r := range want {
			if req.Messages[i].Role != r {
				t.Fatalf("m%d role = %q, want %q", i, req.Messages[i].Role, r)
			}
		}
		if req.Messages[1].Blocks[0].Text != "MID" {
			t.Fatalf("m1 = %+v", req.Messages[1])
		}
	})

	t.Run("multiple_system", func(t *testing.T) {
		req, err := parse(t, `{"model":"m","max_tokens":1,"messages":[
			{"role":"system","content":"S1"},{"role":"user","content":"u"},
			{"role":"system","content":"S2"},{"role":"user","content":"v"},
			{"role":"system","content":"S3"}]}`)
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		want := []ir.Role{ir.RoleSystem, ir.RoleUser, ir.RoleSystem, ir.RoleUser, ir.RoleSystem}
		if len(req.Messages) != len(want) {
			t.Fatalf("messages = %+v", req.Messages)
		}
		for i, r := range want {
			if req.Messages[i].Role != r {
				t.Fatalf("m%d role = %q, want %q", i, req.Messages[i].Role, r)
			}
		}
	})

	t.Run("empty_null_content", func(t *testing.T) {
		req, err := parse(t, `{"model":"m","max_tokens":1,"messages":[
			{"role":"system","content":null},{"role":"user","content":"hi"}]}`)
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		if len(req.Messages) != 2 || req.Messages[0].Role != ir.RoleSystem || len(req.Messages[0].Blocks) != 0 {
			t.Fatalf("messages = %+v（空内容 system 保消息存在性，与 user 空内容同款）", req.Messages)
		}
	})

	t.Run("empty_string_content", func(t *testing.T) {
		req, err := parse(t, `{"model":"m","max_tokens":1,"messages":[
			{"role":"system","content":""},{"role":"user","content":"hi"}]}`)
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		if len(req.Messages) != 2 || req.Messages[0].Role != ir.RoleSystem ||
			len(req.Messages[0].Blocks) != 1 || req.Messages[0].Blocks[0].Text != "" {
			t.Fatalf("messages = %+v", req.Messages)
		}
	})

	t.Run("coexist_with_top_system", func(t *testing.T) {
		req, err := parse(t, `{"model":"m","max_tokens":1,"system":"TOP","messages":[
			{"role":"user","content":"hi"},{"role":"system","content":"MSG"}]}`)
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		// 不合并：顶层在前，消息面按位置；两来源共存于 IR。
		want := []ir.Role{ir.RoleSystem, ir.RoleUser, ir.RoleSystem}
		if len(req.Messages) != len(want) {
			t.Fatalf("messages = %+v", req.Messages)
		}
		for i, r := range want {
			if req.Messages[i].Role != r {
				t.Fatalf("m%d role = %q, want %q", i, req.Messages[i].Role, r)
			}
		}
		if req.Messages[0].Blocks[0].Text != "TOP" || req.Messages[2].Blocks[0].Text != "MSG" {
			t.Fatalf("messages = %+v", req.Messages)
		}
	})

	t.Run("multi_block_not_split", func(t *testing.T) {
		req, err := parse(t, `{"model":"m","max_tokens":1,"messages":[
			{"role":"system","content":[{"type":"text","text":"t1"},{"type":"text","text":"t2"}]}]}`)
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		// 一条线格式消息 = 一条 IR 消息（块不拆——与顶层 system 数组逐元素一条不同）。
		if len(req.Messages) != 1 || req.Messages[0].Role != ir.RoleSystem || len(req.Messages[0].Blocks) != 2 {
			t.Fatalf("messages = %+v", req.Messages)
		}
	})

	t.Run("unknown_role_still_rejected", func(t *testing.T) {
		for _, role := range []string{"developer", "tool", "bogus"} {
			_, err := parse(t, `{"model":"m","max_tokens":1,"messages":[{"role":"`+role+`","content":"x"}]}`)
			if !errors.Is(err, ErrUnexpectedRole) {
				t.Fatalf("role %q err = %v, want ErrUnexpectedRole（白名单只加 system）", role, err)
			}
		}
	})
}

// TestSysRoleSelfExitBoundariesRender claude 自出口边界：多条消息面无顶层时提升为
// 数组；空内容 system 不产顶层字段。
func TestSysRoleSelfExitBoundariesRender(t *testing.T) {
	render := func(t *testing.T, body string) map[string]json.RawMessage {
		t.Helper()
		req, err := ParseRequest([]byte(body))
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		out, err := RenderRequest(req)
		if err != nil {
			t.Fatalf("RenderRequest: %v", err)
		}
		var w map[string]json.RawMessage
		if err := json.Unmarshal(out, &w); err != nil {
			t.Fatalf("unmarshal: %v\n%s", err, out)
		}
		return w
	}

	t.Run("multiple_promote_to_array", func(t *testing.T) {
		w := render(t, `{"model":"m","max_tokens":1,"messages":[
			{"role":"user","content":"a"},{"role":"system","content":"MID"},
			{"role":"user","content":"b"},{"role":"system","content":"TAIL"}]}`)
		var arr []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(w["system"], &arr); err != nil || len(arr) != 2 {
			t.Fatalf("system = %s (err=%v), want 2 元素数组", w["system"], err)
		}
		if arr[0].Text != "MID" || arr[1].Text != "TAIL" {
			t.Fatalf("system = %+v", arr)
		}
		var msgs []json.RawMessage
		if err := json.Unmarshal(w["messages"], &msgs); err != nil || len(msgs) != 2 {
			t.Fatalf("messages = %s（system 应已提升）", w["messages"])
		}
	})

	t.Run("empty_system_no_top_field", func(t *testing.T) {
		w := render(t, `{"model":"m","max_tokens":1,"messages":[
			{"role":"system","content":null},{"role":"user","content":"hi"}]}`)
		if _, ok := w["system"]; ok {
			t.Fatalf("空内容 system 不应产顶层 system: %s", w["system"])
		}
	})

	t.Run("single_string_seed_plus_extra_wraps", func(t *testing.T) {
		w := render(t, `{"model":"m","max_tokens":1,"system":"TOP","messages":[
			{"role":"system","content":"MSG"}]}`)
		var arr []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(w["system"], &arr); err != nil || len(arr) != 2 {
			t.Fatalf("system = %s (err=%v), want [TOP 包装, MSG]", w["system"], err)
		}
		if arr[0].Text != "TOP" || arr[1].Text != "MSG" {
			t.Fatalf("system = %+v", arr)
		}
	})
}
