// W5-29 条目 1 评估落地：Responses 输出项 id 同款改法（响应 id 派生 + 项序）。
//
// 依据 `research/topics/id-and-abort/README.md` §建议 4（父 id 派生式 item id：
// 让 item id 天然唯一、可读、可关联）。项 id 只在流式面出现（批量面无 id 字段）；
// 经 parse 往返的响应 id 派生 → 渲染不动点保持（golden stream 用例锁定）。
//
// 反证锚点：把 hygiene.SynthDerivedID 的 seed 忽略（退回块序）→ 本测试红
// （跨响应同项序撞 `msg_synth_0`）。
package responses

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/hygiene"
	"github.com/dwgx/any2api/translate/ir"
)

func TestSweep12OutputItemIDResponseDerived(t *testing.T) {
	render := func(respID string) string {
		t.Helper()
		w := NewStreamWriter()
		var sb strings.Builder
		write := func(ev ir.StreamEvent) {
			t.Helper()
			b, err := w.Write(ev)
			if err != nil {
				t.Fatalf("Write %s: %v", ev.Kind, err)
			}
			sb.Write(b)
		}
		write(ir.StreamEvent{Kind: ir.EventStart, MessageID: respID, Model: "m", Created: 5})
		write(ir.StreamEvent{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockText})
		write(ir.StreamEvent{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockText, TextDelta: "hi"})
		write(ir.StreamEvent{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockText})
		write(ir.StreamEvent{Kind: ir.EventMessageDelta, StopReason: ir.StopStop, Usage: &ir.Usage{Input: 2, Output: 1, Total: 3}})
		write(ir.StreamEvent{Kind: ir.EventMessageStop})
		write(ir.StreamEvent{Kind: ir.EventDone, Reason: ir.StopStop})
		tail, err := w.Finish()
		if err != nil {
			t.Fatalf("Finish: %v", err)
		}
		sb.Write(tail)
		return sb.String()
	}
	itemIDOf := func(out string) string {
		t.Helper()
		for _, chunk := range strings.Split(out, "\n\n") {
			if !strings.Contains(chunk, "event: response.output_item.added") {
				continue
			}
			for _, line := range strings.Split(chunk, "\n") {
				if !strings.HasPrefix(line, "data: ") {
					continue
				}
				var doc struct {
					Item struct {
						ID string `json:"id"`
					} `json:"item"`
				}
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &doc); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				return doc.Item.ID
			}
		}
		t.Fatalf("未找到 output_item.added:\n%s", out)
		return ""
	}

	a1, a2 := render("resp_a"), render("resp_a")
	idA, idB := itemIDOf(a1), itemIDOf(render("resp_b"))
	if a1 != a2 {
		t.Fatalf("同响应两次渲染不一致（渲染不动点破坏）:\n%s\n%s", a1, a2)
	}
	if idA == idB {
		t.Fatalf("跨响应撞项 id（同项序）: %q（resp_a vs resp_b）", idA)
	}
	if want := hygiene.SynthDerivedID("msg", "resp_a", 0); idA != want {
		t.Fatalf("项 id = %q, want %q（响应 id 派生）", idA, want)
	}
	if !strings.HasPrefix(idA, "msg_synth_") || len(idA) != len("msg_synth_")+32 {
		t.Fatalf("形态（Continue 依赖 msg_/fc_ 前缀，不得去前缀）: %q", idA)
	}
}
