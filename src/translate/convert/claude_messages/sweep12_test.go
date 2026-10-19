// W5-29 条目 1：claude 面合成工具 id 的**跨响应唯一**（响应 id 派生 + 块序）。
//
// 反证锚点：把 hygiene.SynthDerivedID 的 seed 忽略（退回块序形态）→ 本测试红
// （同槽位、不同响应 id 的两个响应会撞 `toolu_synth_<n>`；客户端历史里
// tool_use ids 重复 → 上游「tool_use ids must be unique」400 面）。
package claude_messages

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/hygiene"
	"github.com/dwgx/any2api/translate/ir"
)

// TestSweep12ToolIDCrossResponseUnique 同一 IR（缺失工具 id）+ 不同响应 id：
// 同响应确定（渲染不动点）；跨响应同槽位 id 不等；形态 `<base>_synth_<32hex>`。
func TestSweep12ToolIDCrossResponseUnique(t *testing.T) {
	render := func(respID string) (string, string) {
		t.Helper()
		r := &ir.Response{
			ID: respID,
			Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
				{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{ID: "", Name: "f", Arguments: "{}"}},
			}},
		}
		out, err := RenderResponse(r)
		if err != nil {
			t.Fatalf("RenderResponse(%q): %v", respID, err)
		}
		var doc struct {
			Content []struct {
				Type string `json:"type"`
				ID   string `json:"id"`
			} `json:"content"`
		}
		if err := json.Unmarshal(out, &doc); err != nil {
			t.Fatalf("unmarshal: %v\n%s", err, out)
		}
		if len(doc.Content) != 1 || doc.Content[0].Type != "tool_use" {
			t.Fatalf("content 形状: %s", out)
		}
		return string(out), doc.Content[0].ID
	}

	a1, idA1 := render("resp_a")
	a2, idA2 := render("resp_a")
	_, idB := render("resp_b")

	if a1 != a2 || idA1 != idA2 {
		t.Fatalf("同响应两次渲染不一致（渲染不动点破坏）:\n%s\n%s", a1, a2)
	}
	if idA1 == idB {
		t.Fatalf("跨响应撞 id（同槽位）: %q（resp_a vs resp_b）", idA1)
	}
	if want := hygiene.SynthDerivedID("toolu", "resp_a", 0); idA1 != want {
		t.Fatalf("id = %q, want %q（响应 id 派生）", idA1, want)
	}
	if !strings.HasPrefix(idA1, "toolu_synth_") || len(idA1) != len("toolu_synth_")+32 {
		t.Fatalf("形态: %q", idA1)
	}
}
