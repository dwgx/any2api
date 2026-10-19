// Command parityrunner 是 P66 对拍（Go 修复器 vs check_cases.py）的 Go 侧驱动器：
// 从 stdin 读 jobs，向 stdout 写裁决，供 tmp/lanes/P66-toolcall-spec/parity.py
// 在同一输入集上逐条比较。不是产品入口。
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/dwgx/any2api/translate/toolcall"
)

type job struct {
	Name   string `json:"name"`
	Mode   string `json:"mode"` // "frames"（默认）| "history" | "index"
	Policy string `json:"policy"`

	Tools []struct {
		Name             string   `json:"name"`
		UpstreamName     string   `json:"upstream_name"`
		UpstreamRequired []string `json:"upstream_required"`
	} `json:"tools"`
	Frames []struct {
		Kind      string `json:"kind"`
		ToolUseID string `json:"tool_use_id"`
		Name      string `json:"name"`
		Chunk     string `json:"chunk"`
	} `json:"frames"`

	HistoryTail []struct {
		Role        string `json:"role"`
		Text        string `json:"text"`
		ToolResults []struct {
			ToolUseID string `json:"tool_use_id"`
			Content   []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"tool_results"`
	} `json:"history_tail"`
	KnownToolUseIDsInTail []string `json:"known_tool_use_ids_in_tail"`
	TruncationPlaceholder string   `json:"truncation_placeholder"`

	UpstreamBlocks []struct {
		ContentBlockIndex int    `json:"content_block_index"`
		Type              string `json:"type"`
		ToolUseID         string `json:"tool_use_id"`
		Name              string `json:"name"`
	} `json:"upstream_blocks"`
}

type verdict struct {
	Name                string                    `json:"name"`
	MergedBuffer        *string                   `json:"merged_buffer"`
	Emitted             *string                   `json:"emitted"`
	Failure             *toolcall.Failure         `json:"failure"`
	UpstreamHistory     []map[string]string       `json:"upstream_history,omitempty"`
	DownstreamToolCalls []toolcall.ToolIndexEntry `json:"downstream_tool_calls,omitempty"`
}

func main() {
	var in struct {
		Jobs []job `json:"jobs"`
	}
	if err := json.NewDecoder(os.Stdin).Decode(&in); err != nil {
		fmt.Fprintln(os.Stderr, "parityrunner: decode jobs:", err)
		os.Exit(1)
	}
	out := struct {
		Verdicts []verdict `json:"verdicts"`
	}{}
	for _, j := range in.Jobs {
		out.Verdicts = append(out.Verdicts, run(j))
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, "parityrunner: encode verdicts:", err)
		os.Exit(1)
	}
}

func run(j job) verdict {
	switch j.Mode {
	case "history":
		var msgs []toolcall.HistoryMessage
		for _, m := range j.HistoryTail {
			hm := toolcall.HistoryMessage{Role: m.Role, Text: m.Text}
			for _, tr := range m.ToolResults {
				hr := toolcall.HistoryToolResult{ToolUseID: tr.ToolUseID}
				for _, c := range tr.Content {
					hr.Content = append(hr.Content, toolcall.HistoryResultContent{Type: c.Type, Text: c.Text})
				}
				hm.ToolResults = append(hm.ToolResults, hr)
			}
			msgs = append(msgs, hm)
		}
		got := toolcall.NormalizeHistoryTail(msgs, j.KnownToolUseIDsInTail, j.TruncationPlaceholder)
		v := verdict{Name: j.Name}
		for _, m := range got {
			v.UpstreamHistory = append(v.UpstreamHistory, map[string]string{"role": m.Role, "text": m.Text})
		}
		return v
	case "index":
		var blocks []toolcall.UpstreamBlock
		for _, b := range j.UpstreamBlocks {
			blocks = append(blocks, toolcall.UpstreamBlock{
				ContentBlockIndex: b.ContentBlockIndex,
				Type:              b.Type,
				ToolUseID:         b.ToolUseID,
				Name:              b.Name,
			})
		}
		return verdict{Name: j.Name, DownstreamToolCalls: toolcall.NormalizeToolIndexes(blocks).Calls}
	default: // frames
		var tools []toolcall.ToolDef
		for _, t := range j.Tools {
			tools = append(tools, toolcall.ToolDef{
				Name:             t.Name,
				UpstreamName:     t.UpstreamName,
				UpstreamRequired: t.UpstreamRequired,
			})
		}
		policy := toolcall.PolicyDefault
		if j.Policy == "passthrough_worst_case" {
			policy = toolcall.PolicyPassthroughWorstCase
		}
		a := toolcall.NewAssembler(toolcall.ModeB, tools, policy)
		for _, f := range j.Frames {
			a.OnFrame(toolcall.Frame{
				Kind:      toolcall.FrameKind(f.Kind),
				ToolUseID: f.ToolUseID,
				Name:      f.Name,
				Chunk:     f.Chunk,
			})
		}
		v := verdict{Name: j.Name}
		slots := a.SlotResults()
		if len(slots) == 1 {
			buf := slots[0].Buf
			v.MergedBuffer = &buf
			v.Emitted = slots[0].Emitted
			v.Failure = slots[0].Failure
		}
		return v
	}
}
