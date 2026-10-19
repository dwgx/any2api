package pairs

// W14：CC 2.1.299 形状（messages[].role:"system"）全链——claude 入口 × 各上游出口。
//
// 语料：../convert/claude_messages/testdata/cc-299-req.json（W13-2 实发请求脱敏
// 形状；结构说明见该目录 sysrole_test.go 头注）。
//
// 覆盖：
//   - 旗舰路径 claude_messages 入口 → claude_web 上游：不报错（修复前 = pairs
//     step 0 的 ErrUnexpectedRole → 服务 400）+ 拍平面含两来源 system 内容 + 顺序；
//   - 边界：中位/多条/空内容 system 全链拍平；
//   - 其它出口（openai_chat/gemini/responses/claude_messages 自出口）：不报错 +
//     system 内容按各自出口语义承载（自出口经 foreign 入口覆盖——claude→claude
//     同格式是直通路径，不经转换器）。
//
// 反证（回退即红，见报告 §反证）：回退 parseMessages 的 RoleSystem 白名单 →
// TestSysRoleFlagshipClaudeWebRealShape / TestSysRoleBoundariesClaudeWeb /
// TestSysRoleOtherExitsRealShape 红。

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/registry"
)

func readSysRoleFixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("../convert/claude_messages/testdata/cc-299-req.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return raw
}

// TestSysRoleFlagshipClaudeWebRealShape 旗舰路径：W13-2 实发形状 → claude_web
// 上游体。修复前此请求在 parse 处 ErrUnexpectedRole（服务 400）。
func TestSysRoleFlagshipClaudeWebRealShape(t *testing.T) {
	reg := NewRegistry()
	out, err := Convert(reg, registry.FormatClaudeMessages, FormatClaudeWeb, FaceRequest, readSysRoleFixture(t), Options{})
	if err != nil {
		t.Fatalf("Convert(claude→claude_web): %v", err)
	}
	var w map[string]json.RawMessage
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	var prompt string
	if err := json.Unmarshal(w["prompt"], &prompt); err != nil {
		t.Fatalf("prompt: %v", err)
	}
	// 两份 system 来源 + 用户句全部落拍平面。
	for _, want := range []string{
		"System: You are a Claude agent, built on Anthropic's Claude Agent SDK.",
		"Human: ",
		"用一句话介绍你自己",
		"# Environment",
		"Today's date is 2026-10-14.",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt 缺 %q:\n%.600s", want, prompt)
		}
	}
	// 顺序 = IR 序：顶层 system → user → 消息面 system（中位/尾位保留）。
	iTop := strings.Index(prompt, "System: You are a Claude agent")
	iUser := strings.Index(prompt, "用一句话介绍你自己")
	iEnv := strings.Index(prompt, "# Environment")
	if !(iTop < iUser && iUser < iEnv) {
		t.Fatalf("prompt 顺序 = top:%d user:%d env:%d，want top<user<env", iTop, iUser, iEnv)
	}
	// 上游体字段集（W13-2 实抓同形）：model / effort / thinking_mode；max_tokens
	// 绝不漏进上游体（上游对多余字段报错）。
	var model, effort, thinking string
	_ = json.Unmarshal(w["model"], &model)
	_ = json.Unmarshal(w["effort"], &effort)
	_ = json.Unmarshal(w["thinking_mode"], &thinking)
	if model != "claude-sonnet-5-5" || effort != "medium" || thinking != "auto" {
		t.Fatalf("model/effort/thinking = %q/%q/%q", model, effort, thinking)
	}
	if _, ok := w["max_tokens"]; ok {
		t.Fatalf("max_tokens 漏进 claude_web 体")
	}
}

// TestSysRoleBoundariesClaudeWeb 边界全链：中位/多条/空内容/首位的 system 消息
// → claude_web 拍平面（prompt 逐字断言）。
func TestSysRoleBoundariesClaudeWeb(t *testing.T) {
	reg := NewRegistry()
	convert := func(t *testing.T, body string) string {
		t.Helper()
		out, err := Convert(reg, registry.FormatClaudeMessages, FormatClaudeWeb, FaceRequest, []byte(body), Options{})
		if err != nil {
			t.Fatalf("Convert: %v\nbody=%s", err, body)
		}
		var w map[string]json.RawMessage
		if err := json.Unmarshal(out, &w); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		var prompt string
		if err := json.Unmarshal(w["prompt"], &prompt); err != nil {
			t.Fatalf("prompt: %v", err)
		}
		return prompt
	}

	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "middle_and_tail_multiple",
			body: `{"model":"m","max_tokens":16,"messages":[
				{"role":"user","content":"hi"},
				{"role":"system","content":"MID-RULE"},
				{"role":"user","content":"more"},
				{"role":"system","content":"TAIL-RULE"}]}`,
			want: "Human: hi\n\nSystem: MID-RULE\n\nHuman: more\n\nSystem: TAIL-RULE",
		},
		{
			name: "first_position",
			body: `{"model":"m","max_tokens":16,"messages":[
				{"role":"system","content":"FIRST"},{"role":"user","content":"hi"}]}`,
			want: "System: FIRST\n\nHuman: hi",
		},
		{
			name: "empty_system_contributes_nothing",
			body: `{"model":"m","max_tokens":16,"messages":[
				{"role":"system","content":null},
				{"role":"system","content":""},
				{"role":"user","content":"hi"}]}`,
			want: "Human: hi",
		},
		{
			name: "coexist_with_top_system",
			body: `{"model":"m","max_tokens":16,"system":"TOP",
				"messages":[{"role":"user","content":"hi"},{"role":"system","content":"MSG"}]}`,
			want: "System: TOP\n\nHuman: hi\n\nSystem: MSG",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := convert(t, tc.body); got != tc.want {
				t.Fatalf("prompt = %q\nwant    %q", got, tc.want)
			}
		})
	}
}

// TestSysRoleOtherExitsRealShape 其它出口：W13-2 实发形状 → openai_chat / gemini /
// responses 出口体；system 内容按各出口既有语义承载，不报错。
func TestSysRoleOtherExitsRealShape(t *testing.T) {
	reg := NewRegistry()
	fixture := readSysRoleFixture(t)

	t.Run("openai_chat", func(t *testing.T) {
		out, err := Convert(reg, registry.FormatClaudeMessages, registry.FormatOpenAIChat, FaceRequest, fixture, Options{})
		if err != nil {
			t.Fatalf("Convert(claude→openai_chat): %v", err)
		}
		// 角色序列与内容：3 顶层 system + user + 消息面 system（位置保留）。
		var w struct {
			Messages []struct {
				Role    string          `json:"role"`
				Content json.RawMessage `json:"content"`
			} `json:"messages"`
		}
		if err := json.Unmarshal(out, &w); err != nil {
			t.Fatalf("unmarshal: %v\n%.400s", err, out)
		}
		roles := make([]string, 0, len(w.Messages))
		for _, m := range w.Messages {
			roles = append(roles, m.Role)
		}
		wantRoles := []string{"system", "system", "system", "user", "system"}
		if strings.Join(roles, ",") != strings.Join(wantRoles, ",") {
			t.Fatalf("roles = %v, want %v", roles, wantRoles)
		}
		last := string(w.Messages[len(w.Messages)-1].Content)
		if !strings.Contains(last, "# Environment") || !strings.Contains(last, "Today's date is 2026-10-14.") {
			t.Fatalf("末条 system 缺 env 内容: %.300s", last)
		}
		if !strings.Contains(string(out), "用一句话介绍你自己") {
			t.Fatalf("user 内容丢失")
		}
	})

	t.Run("gemini", func(t *testing.T) {
		out, err := Convert(reg, registry.FormatClaudeMessages, registry.FormatGeminiGenerate, FaceRequest, fixture, Options{})
		if err != nil {
			t.Fatalf("Convert(claude→gemini): %v", err)
		}
		text := string(out)
		// 顶层 system → systemInstruction（3 part）；消息面 system 在会话开始后 →
		// user part + <system-reminder> 信封（保位置）；user 内容在 contents。
		if !strings.Contains(text, "systemInstruction") ||
			!strings.Contains(text, "You are a Claude agent") {
			t.Fatalf("systemInstruction 缺顶层 system: %.400s", text)
		}
		if !strings.Contains(text, "<system-reminder>") || !strings.Contains(text, "# Environment") {
			t.Fatalf("消息面 system 未落 system-reminder 信封: %.400s", text)
		}
		if !strings.Contains(text, "用一句话介绍你自己") {
			t.Fatalf("user 内容丢失: %.400s", text)
		}
	})

	t.Run("responses", func(t *testing.T) {
		out, err := Convert(reg, registry.FormatClaudeMessages, registry.FormatOpenAIResponses, FaceRequest, fixture, Options{})
		if err != nil {
			t.Fatalf("Convert(claude→responses): %v", err)
		}
		var w struct {
			Instructions string `json:"instructions"`
		}
		if err := json.Unmarshal(out, &w); err != nil {
			t.Fatalf("unmarshal: %v\n%.400s", err, out)
		}
		for _, want := range []string{"You are a Claude agent", "# Environment", "Today's date is 2026-10-14."} {
			if !strings.Contains(w.Instructions, want) {
				t.Fatalf("instructions 缺 %q: %.400s", want, w.Instructions)
			}
		}
		if !strings.Contains(string(out), "用一句话介绍你自己") {
			t.Fatalf("user 内容丢失: %.400s", out)
		}
	})

	t.Run("claude_messages_self_exit_via_foreign_entry", func(t *testing.T) {
		// claude→claude 同格式是直通路径（不经转换器）；自出口由 foreign 入口覆盖：
		// openai 入口的 system 消息（含中位）→ claude 出口顶层 system。
		body := `{"model":"m","messages":[
			{"role":"user","content":"hi"},
			{"role":"system","content":"MID-RULE"},
			{"role":"user","content":"bye"}]}`
		out, err := Convert(reg, registry.FormatOpenAIChat, registry.FormatClaudeMessages, FaceRequest, []byte(body), Options{})
		if err != nil {
			t.Fatalf("Convert(openai→claude): %v", err)
		}
		var w struct {
			System   json.RawMessage `json:"system"`
			Messages []struct {
				Role string `json:"role"`
			} `json:"messages"`
		}
		if err := json.Unmarshal(out, &w); err != nil {
			t.Fatalf("unmarshal: %v\n%s", err, out)
		}
		var sys string
		if err := json.Unmarshal(w.System, &sys); err != nil || sys != "MID-RULE" {
			t.Fatalf("system = %s (err=%v), want %q", w.System, err, "MID-RULE")
		}
		if len(w.Messages) != 2 || w.Messages[0].Role != "user" || w.Messages[1].Role != "user" {
			t.Fatalf("messages = %+v（system 应已提升）", w.Messages)
		}
	})
}
