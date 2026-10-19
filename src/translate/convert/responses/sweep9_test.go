// W5-26 条目 1①②：合成 id 的两个不变量（响应 id 每响应唯一；工具 id 确定性
// 合成 + 出口净化）。反证锚点：把 hygiene.SynthUniqueID 换回常量 / 把
// WireToolID 的净化短路 → 本文件测试红。
package responses

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/hygiene"
	"github.com/dwgx/any2api/translate/ir"
)

func respWithText(t *testing.T) *ir.Response {
	t.Helper()
	return &ir.Response{
		Model: "m",
		Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
			{Kind: ir.BlockText, Text: "hi"},
		}},
	}
}

// ② 合成 response id 每响应唯一（旧常量 `resp_synth` 两次渲染同值）。
func TestSweep9SynthResponseIDPerResponseUnique(t *testing.T) {
	idOf := func(b []byte) string {
		var doc struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(b, &doc); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		return doc.ID
	}
	out1, err := RenderResponse(respWithText(t))
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	out2, err := RenderResponse(respWithText(t))
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	id1, id2 := idOf(out1), idOf(out2)
	if id1 == "" || id2 == "" {
		t.Fatalf("id 缺失: %q / %q", id1, id2)
	}
	if id1 == id2 {
		t.Fatalf("跨响应同 id（旧常量形态回归）: %q", id1)
	}
	if !strings.HasPrefix(id1, "resp_synth_") || len(id1) > 64 {
		t.Fatalf("合成 id 形态: %q", id1)
	}
	// 反证锚点：上游给了 id 就必须原样透传（不合成、不改写）。
	r := respWithText(t)
	r.ID = "resp_upstream_1"
	out3, err := RenderResponse(r)
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	if idOf(out3) != "resp_upstream_1" {
		t.Fatalf("上游 id 被改写: %q", idOf(out3))
	}
}

// ① 出口 id 净化/上限 + 缺失合成（Responses call_id 面：≤64 是硬约束）。
func TestSweep9ResponsesCallIDHygiene(t *testing.T) {
	long := "call_" + strings.Repeat("y", 64) // 69 字符（roo 真实报错面）
	render := func(id string) []byte {
		t.Helper()
		r := &ir.Response{
			ID: "resp_x",
			Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
				{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{ID: id, Name: "f", Arguments: "{}"}},
			}},
		}
		out, err := RenderResponse(r)
		if err != nil {
			t.Fatalf("RenderResponse: %v", err)
		}
		return out
	}
	callIDOf := func(b []byte) string {
		var doc struct {
			Output []struct {
				CallID string `json:"call_id"`
			} `json:"output"`
		}
		if err := json.Unmarshal(b, &doc); err != nil {
			t.Fatalf("unmarshal: %v\n%s", err, b)
		}
		for _, it := range doc.Output {
			if it.CallID != "" {
				return it.CallID
			}
		}
		t.Fatalf("call_id 缺失:\n%s", b)
		return ""
	}
	if got := callIDOf(render(long)); len(got) != 64 || got == long || !strings.HasPrefix(got, "call_yyy") {
		t.Fatalf("超长 call_id 未按 ≤64 截断+哈希后缀: %q（len=%d）", got, len(got))
	}
	if got := callIDOf(render("call_ok")); got != "call_ok" {
		t.Fatalf("合法 call_id 被改写: %q", got)
	}
	if got := callIDOf(render("call_bad.dot")); got != "call_bad_dot" {
		t.Fatalf("非法字符未净化: %q", got)
	}
	// 缺失 id → 响应 id 派生确定性合成（渲染不动点：两次渲染逐字节一致；
	// W5-29：跨响应唯一——seed = 响应 id）。
	a, b := render(""), render("")
	if string(a) != string(b) {
		t.Fatalf("合成 id 不确定（渲染不动点破坏）:\n%s\n%s", a, b)
	}
	if got := callIDOf(a); got != hygiene.SynthDerivedID("call", "resp_x", 0) {
		t.Fatalf("缺失 id 未按响应 id 派生合成: %q", got)
	}
}
