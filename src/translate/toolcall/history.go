package toolcall

import (
	"strings"

	"github.com/dwgx/any2api/translate/hygiene"
)

// HistoryMessage 历史消息（历史规整输入 / 输出）。
type HistoryMessage struct {
	Role        string
	Text        string
	ToolUses    []HistoryToolUse
	ToolResults []HistoryToolResult
	Tools       []string // 条目级工具清单（sanitize 清空）
}

// HistoryToolUse 历史中的工具调用。
type HistoryToolUse struct {
	ToolUseID string
	Name      string
}

// HistoryToolResult 历史中的工具结果。
type HistoryToolResult struct {
	ToolUseID string
	Content   []HistoryResultContent
}

// HistoryResultContent 工具结果内容片。
type HistoryResultContent struct {
	Type string
	Text string
}

// NormalizeHistoryTail 规整被截断的历史尾部（规格 §7 / case 06）：
//  1. 循环剥掉头部「孤立 toolResults 的 user 条 + 紧随 assistant」（保交替，
//     否则剥掉 user 后 assistant 打头同样 400 REQUEST_BODY_INVALID）；
//  2. 其余（中段）孤儿 toolResults **折叠为普通文本**（统一政策：绝不静默
//     丢弃、绝不以 toolResult 形式出口；内容原样保留，空白 → 显式占位）；
//     被剥空的 user 条整条删除（本就空——折叠过的条已带文本）；
//  3. 有截断时前置占位 user + assistant "Understood." 应答——历史必以 user 开头。
//
// 头部剥除是唯一例外（截断边界：内容已不可考，占位叙事标明损失）；中段折叠
// 保内容且保交替（旧「剥空删除」会让 assistant/assistant 相邻，同样 400）。
func NormalizeHistoryTail(msgs []HistoryMessage, knownToolUseIDs []string, truncationPlaceholder string) []HistoryMessage {
	known := make(map[string]bool, len(knownToolUseIDs))
	for _, id := range knownToolUseIDs {
		known[id] = true
	}
	out := append([]HistoryMessage(nil), msgs...)
	// 1. 头部循环：孤立 toolResults 的 user 条 + 紧随 assistant。
	for len(out) > 0 && out[0].Role == "user" && hasOrphanToolResult(out[0], known) {
		out = out[1:]
		if len(out) > 0 && out[0].Role == "assistant" {
			out = out[1:]
		}
	}
	// 2. 其余孤儿 toolResults：折叠为普通文本（统一政策）。
	kept := out[:0]
	for _, m := range out {
		var folded []string
		m.ToolResults, folded = foldOrphanToolResults(m.ToolResults, known)
		for _, f := range folded {
			if m.Text == "" {
				m.Text = f
			} else {
				m.Text += "\n" + f
			}
		}
		if m.Role == "user" && m.Text == "" && len(m.ToolResults) == 0 && len(m.ToolUses) == 0 {
			continue // 剥空的 user 条删除
		}
		kept = append(kept, m)
	}
	out = kept
	// 3. 截断占位（前置 user + 应答，保交替、保 user 开头）。
	if truncationPlaceholder != "" {
		out = append([]HistoryMessage{
			{Role: "user", Text: truncationPlaceholder},
			{Role: "assistant", Text: "Understood."},
		}, out...)
	}
	return out
}

func hasOrphanToolResult(m HistoryMessage, known map[string]bool) bool {
	for _, tr := range m.ToolResults {
		if !known[tr.ToolUseID] {
			return true
		}
	}
	return false
}

// foldOrphanToolResults 孤儿结果分流：命中的保留为结构化 toolResult；未命中的
// 折叠为普通文本（统一政策 `hygiene.FoldOrphanToolResultText`：内容原样保留，
// 空白 → 显式占位——绝不静默丢弃，也绝不作为 toolResult 出口）。
func foldOrphanToolResults(results []HistoryToolResult, known map[string]bool) (kept []HistoryToolResult, folded []string) {
	for _, tr := range results {
		if known[tr.ToolUseID] {
			kept = append(kept, tr)
			continue
		}
		var texts []string
		for _, c := range tr.Content {
			if c.Text != "" {
				texts = append(texts, c.Text)
			}
		}
		folded = append(folded, hygiene.FoldOrphanToolResultText(strings.Join(texts, " ")))
	}
	if len(kept) == 0 {
		kept = nil
	}
	return kept, folded
}

// SanitizeHistory 扁平化历史（规格 §7 sanitize_history / case 06 变体）：
//   - 非活跃轮次的 toolUses 清空（不写任何「调用了工具 X」的叙述，防模型模仿）；
//   - 非活跃 toolResults 并入正文为 "[name] text"（名字靠 id→name 全量映射保留）；
//   - 活跃轮次（currentToolResultIDs 命中的 tool_use）保持结构化；
//   - 历史条目 tools 清空。
//
// 该扁平化只在 400 CONTENT_LENGTH_EXCEEDS 压缩重试路径调用（sanitize 先行 →
// 字节截断）；正常路径的历史结构化轮次处理不在本函数范围。
func SanitizeHistory(msgs []HistoryMessage, currentToolResultIDs []string) []HistoryMessage {
	active := make(map[string]bool, len(currentToolResultIDs))
	for _, id := range currentToolResultIDs {
		active[id] = true
	}
	names := map[string]string{}
	for _, m := range msgs {
		for _, tu := range m.ToolUses {
			if tu.Name != "" {
				names[tu.ToolUseID] = tu.Name
			}
		}
	}
	out := make([]HistoryMessage, 0, len(msgs))
	for _, m := range msgs {
		m.Tools = nil // 历史条目 tools 清空
		var uses []HistoryToolUse
		for _, tu := range m.ToolUses {
			if active[tu.ToolUseID] {
				uses = append(uses, tu) // 活跃轮次保持结构化
			}
		}
		m.ToolUses = uses
		var results []HistoryToolResult
		for _, tr := range m.ToolResults {
			if active[tr.ToolUseID] {
				results = append(results, tr)
				continue
			}
			name := names[tr.ToolUseID]
			if name == "" {
				name = "tool_result"
			}
			var texts []string
			for _, c := range tr.Content {
				if c.Text != "" {
					texts = append(texts, c.Text)
				}
			}
			label := "[" + name + "]"
			if body := strings.Join(texts, " "); body != "" {
				label += " " + body
			}
			if m.Text == "" {
				m.Text = label
			} else {
				m.Text += "\n" + label
			}
		}
		m.ToolResults = results
		out = append(out, m)
	}
	return out
}
