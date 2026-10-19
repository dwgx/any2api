package claude_messages

// W6-1 ②：OpenAI 侧 `reasoning_effort` → claude `thinking` 档位映射接线。
//
// 依据：anthropic-conversion-table:81「推理强度」行档位表（none=0(disabled) /
// auto=-1 / minimal=512 / low=1024 / medium=8192 / high=24576 / xhigh=32768 /
// max=128000）+ CPA `thinking/convert.go` levelToBudgetMap 与
// `claude_openai_request.go:89-102` 的 legacy/预算分支落法（0→disabled；-1→enabled
// 不带 budget_tokens；>0→enabled+budget_tokens）。
//
// 模型能力门：CPA 的 adaptive 分支（4.6+ 走 output_config.effort）依赖其模型注册表
// （registry.LookupModelInfo + Thinking.Levels）——本 slice 无注册表，按 CPA 对未知
// 模型的行为（mi==nil → 预算路径）无条件映射（见 render.go thinkingForEffort 注释）。
//
// 反证：回退 render.go 的 reasoning_effort 接线 → TestRenderRequestReasoningEffort 红。

import (
	"encoding/json"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

func effortReq(raw json.RawMessage, extra map[string]json.RawMessage) *ir.Request {
	if extra == nil {
		extra = map[string]json.RawMessage{}
	}
	if raw != nil {
		extra["reasoning_effort"] = raw
	}
	return &ir.Request{
		Model: "m",
		Messages: []ir.Message{
			{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "hi"}}},
		},
		Extra: extra,
	}
}

type claudeThinking struct {
	Type         string `json:"type"`
	BudgetTokens *int   `json:"budget_tokens"`
}

func renderThinking(t *testing.T, req *ir.Request) (map[string]json.RawMessage, *claudeThinking) {
	t.Helper()
	out, err := RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	var w map[string]json.RawMessage
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	if raw, ok := w["reasoning_effort"]; ok {
		t.Fatalf("reasoning_effort 漏进 claude 请求体（上游 400）: %s", raw)
	}
	if _, ok := w["thinking"]; !ok {
		return w, nil
	}
	var th claudeThinking
	if err := json.Unmarshal(w["thinking"], &th); err != nil {
		t.Fatalf("thinking unmarshal: %v", err)
	}
	return w, &th
}

func TestRenderRequestReasoningEffort(t *testing.T) {
	intp := func(v int) *int { return &v }
	cases := []struct {
		effort string
		want   claudeThinking
	}{
		{"none", claudeThinking{Type: "disabled"}},
		{"auto", claudeThinking{Type: "enabled"}},
		{"minimal", claudeThinking{Type: "enabled", BudgetTokens: intp(512)}},
		{"low", claudeThinking{Type: "enabled", BudgetTokens: intp(1024)}},
		{"medium", claudeThinking{Type: "enabled", BudgetTokens: intp(8192)}},
		{"high", claudeThinking{Type: "enabled", BudgetTokens: intp(24576)}},
		{"xhigh", claudeThinking{Type: "enabled", BudgetTokens: intp(32768)}},
		{"max", claudeThinking{Type: "enabled", BudgetTokens: intp(128000)}},
		{" HIGH ", claudeThinking{Type: "enabled", BudgetTokens: intp(24576)}}, // 大小写/空白归一（CPA strings.ToLower）
	}
	for _, tc := range cases {
		_, th := renderThinking(t, effortReq(mustMarshal(tc.effort), nil))
		if th == nil {
			t.Fatalf("reasoning_effort=%q 未映射出 thinking", tc.effort)
		}
		if th.Type != tc.want.Type {
			t.Fatalf("reasoning_effort=%q thinking.type = %q, want %q", tc.effort, th.Type, tc.want.Type)
		}
		switch {
		case tc.want.BudgetTokens == nil && th.BudgetTokens != nil:
			t.Fatalf("reasoning_effort=%q 不得带 budget_tokens（CPA：%d → type 分支）", tc.effort, *th.BudgetTokens)
		case tc.want.BudgetTokens != nil && (th.BudgetTokens == nil || *th.BudgetTokens != *tc.want.BudgetTokens):
			t.Fatalf("reasoning_effort=%q budget_tokens = %v, want %d", tc.effort, th.BudgetTokens, *tc.want.BudgetTokens)
		}
	}
}

// TestRenderRequestReasoningEffortFallbacks 未知/畸形档位不映射（CPA
// ConvertLevelToBudget 未命中 → 不设置；「丢弃+日志」日志点在服务壳层）；
// 空档位串同（CPA：effort=="" 整体跳过）。
func TestRenderRequestReasoningEffortFallbacks(t *testing.T) {
	for _, raw := range []json.RawMessage{
		mustMarshal("ultra"), mustMarshal(""), mustMarshal("   "), mustMarshal(123),
		json.RawMessage(`{"effort":"high"}`), json.RawMessage(`null`),
	} {
		w, th := renderThinking(t, effortReq(raw, nil))
		if th != nil {
			t.Fatalf("reasoning_effort=%s 不得映射 thinking: %s", raw, w["thinking"])
		}
	}
}

// TestRenderRequestReasoningEffortClaudeNativeWins claude 侧原文优先：请求已带
// claude 原生 `thinking`/`output_config`（白名单直通）时不覆盖——reasoning_effort
// 不参与（表：claude 侧独有字段保真；跨格式腿的强度参数不推翻原文）。
func TestRenderRequestReasoningEffortClaudeNativeWins(t *testing.T) {
	native := json.RawMessage(`{"type":"enabled","budget_tokens":2048}`)
	w, th := renderThinking(t, effortReq(mustMarshal("max"), map[string]json.RawMessage{"thinking": native}))
	if th == nil || th.BudgetTokens == nil || *th.BudgetTokens != 2048 || th.Type != "enabled" {
		t.Fatalf("claude 原生 thinking 被 reasoning_effort 覆盖: %s", w["thinking"])
	}

	w, th = renderThinking(t, effortReq(mustMarshal("max"), map[string]json.RawMessage{
		"output_config": json.RawMessage(`{"effort":"low"}`),
	}))
	if th != nil {
		t.Fatalf("claude 原生 output_config 在场时不得合成 thinking: %s", w["thinking"])
	}
	if string(w["output_config"]) != `{"effort":"low"}` {
		t.Fatalf("output_config 原文丢失: %s", w["output_config"])
	}
}

// TestRenderRequestExtraWhitelistNoLeak 逐键核对 claudePassableExtras：非白名单
// 键（reasoning_effort/seed/logprobs/n）绝不作为顶层键漏进 claude 请求体；
// 白名单键（temperature/top_p）原样透传。
func TestRenderRequestExtraWhitelistNoLeak(t *testing.T) {
	w, _ := renderThinking(t, effortReq(mustMarshal("high"), map[string]json.RawMessage{
		"temperature": json.RawMessage(`0.5`),
		"top_p":       json.RawMessage(`0.9`),
		"seed":        json.RawMessage(`7`),
		"logprobs":    json.RawMessage(`true`),
		"n":           json.RawMessage(`2`),
	}))
	for _, k := range []string{"reasoning_effort", "seed", "logprobs", "n"} {
		if raw, ok := w[k]; ok {
			t.Fatalf("非白名单键 %q 漏进 claude 请求体: %s", k, raw)
		}
	}
	if string(w["temperature"]) != `0.5` || string(w["top_p"]) != `0.9` {
		t.Fatalf("白名单键丢失: temp=%s top_p=%s", w["temperature"], w["top_p"])
	}
}
