// Package toolsim 实现 claude_web 上游的客户端工具模拟层（A1 形态：模拟·客户端执行；
// 设计单 research/topics/claude-web-tool-sim/README.md §S1/§S2，W16-1）。两面：
//
//   - 请求面（S1 注入）：BuildInjection 渲染「契约 + 工具清单 + tool_choice 映射」段；
//     claude_web 渲染器（RenderRequestWithOptions，ToolSim 开）把它插到 prompt 最前。
//     客户端工具定义**不进上游 `tools` 字段**（W11-3 契约不破：上游 `tools` 恒为
//     服务端三元组 web_search_v0/artifacts_v0/repl_v0）。
//   - 出口面（S2 解析）：Scanner（字符级，跨 chunk 缓冲）/ Filter（IR 事件级流式）/
//     TransformResponse（非流式响应级）把模型文本里的契约标签合成回 `tool_use` 块。
//
// 词表单一事实源 = W11-3 线格式块名词表（claude_web/render.go flattenBlock 的历史
// 回放面）：输出契约与历史回放同语言——模型在 prompt 里已见过同形文本（历史里的
// `<tool_use id="…" name="…">{args}</tool_use>`），零学习成本、少一套词汇。
//
// 容错纪律（BK tagged.go:379-582 行为基准；设计单 §红线 3「契约不守 → 降级为文本」）：
//
//   - 契约不守 → 原样文本降级：绝不吞内容、绝不断流、绝不伪造成空参调用；
//   - 跨 chunk 切割（`<tool` + `_use …`）缓冲到判定（BK `pendingTag` 同款）；
//   - 未知标签 / `<tool_result>` / 坏 JSON / 未闭合标签 → 文本降级（不合成块）；
//   - 只在 text 块内做（thinking / 服务端工具块零接触——W10-3 实抓块交错）。
//
// 包边界：只依赖 translate/ir 与标准库（claude_web 与 pairs 双向引用本包，本包
// 不反向引用——无包环）。
package toolsim

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/dwgx/any2api/translate/ir"
)

// 契约词表（= 线格式块类型名，逐字来自实抓帧 `content_block.type`，W9-2/W10-3
// fixture 可查；claude_web/render.go flattenBlock 的历史回放面同词）。
const (
	// TagToolUse 契约标签名（模型输出面）。
	TagToolUse = "tool_use"
	// TagToolResult 禁止模型输出的标签（出口对 `tool_result` 一律文本降级）。
	TagToolResult = "tool_result"

	// OpenTag 契约开标签前缀；CloseTag 闭标签。
	OpenTag  = "<tool_use"
	CloseTag = "</tool_use>"
)

// MaxDescription 注入清单的描述截断长度（BK tagged.go:130-134 同款 200 字符；
// 超长描述按 rune 截断 + "..."——BK 按字节切，UTF-8 描述会被切碎，本包取 rune 安全面）。
const MaxDescription = 200

// contractText 注入段契约正文。
//
// 措辞参考：AU handlers/anthropic.go:520-539（「emit EXACTLY one block per call」
// 「NOTHING else around the block」）+ BK tagged.go:23-40（「After … stop immediately」
// 「Never generate Observation, tool results」）——两家的禁项逐条落到本段：
// 只输出 `<tool_use>`、不得输出 `<tool_result>`、不得自造 observation/tool 结果、
// 调用后立即停。
const contractText = `## Tool calling

You have access to the following tools. Tool calls are executed by the client on its own machine; you never execute them yourself.

When you need to use a tool, emit EXACTLY one block per call using this format, and NOTHING else around the block:

<tool_use name="tool_name">{"argument": "value"}</tool_use>

Rules:
- The block body must be a single valid JSON object matching the tool's input schema.
- Emit exactly one block per call; repeat the block if you need several calls.
- After the closing </tool_use> tag, stop immediately and write nothing further.
- Never emit <tool_result> blocks and never fabricate observations or tool results: the client executes the tool and sends the real result in the next turn.
- Never wrap the block in markdown code fences.`

// toolListHeader 清单段头（BK tagged.go:155-166 的 `## Available tools` 同形）。
const toolListHeader = "## Available tools"

// BuildInjection 渲染注入段（prompt 最前；tools 为空 → ""——调用方据此直过）。
//
// 段结构 = 契约正文 + 清单（`- name(args): description`）+ tool_choice 映射句（有则加）。
func BuildInjection(tools []ir.ToolDef, toolChoice json.RawMessage) string {
	if len(tools) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString(contractText)
	b.WriteString("\n\n")
	b.WriteString(toolListHeader)
	b.WriteString("\n\n")
	b.WriteString(ToolList(tools))
	if instr := ToolChoiceInstruction(toolChoice); instr != "" {
		b.WriteString("\n\n")
		b.WriteString(instr)
	}
	b.WriteString("\n")
	return b.String()
}

// ToolList 工具清单渲染：每行 `- name(args): description`（BK tagged.go:79-146 同形）。
//
//   - args 取自 InputSchema 的 `properties`（键排序保证确定性——BK 用 map 迭代序，
//     每次渲染可能不同，golden 不可复现，本包排序）+ `required` 标记；
//   - 无名工具跳过（BK 同款）；schema 缺失/不可解析 → 括号为空（`- name(): desc`）；
//   - 描述超 MaxDescription 按 rune 截断 + "..."。
func ToolList(tools []ir.ToolDef) string {
	lines := make([]string, 0, len(tools))
	for _, t := range tools {
		name := strings.TrimSpace(t.Name)
		if name == "" {
			continue
		}
		lines = append(lines, "- "+name+"("+schemaArgs(t.InputSchema)+"): "+truncateDescription(t.Description))
	}
	return strings.Join(lines, "\n")
}

// truncateDescription 描述截断（rune 安全；>200 → 前 200 + "..."）。
func truncateDescription(desc string) string {
	rs := []rune(desc)
	if len(rs) <= MaxDescription {
		return desc
	}
	return string(rs[:MaxDescription]) + "..."
}

// schemaArgs JSON Schema → 参数摘要 `key: type (required), …`（键排序）。
// 不可解析 / 无 properties → ""（不发明占位）。
func schemaArgs(schema json.RawMessage) string {
	if len(schema) == 0 {
		return ""
	}
	var s struct {
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
		Required []string `json:"required"`
	}
	if err := json.Unmarshal(schema, &s); err != nil || len(s.Properties) == 0 {
		return ""
	}
	required := make(map[string]bool, len(s.Required))
	for _, r := range s.Required {
		required[r] = true
	}
	keys := make([]string, 0, len(s.Properties))
	for k := range s.Properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		typ := s.Properties[k].Type
		if typ == "" {
			typ = "any" // BK tagged.go:127 同款（无 type 声明 → any）
		}
		part := k + ": " + typ
		if required[k] {
			part += " (required)"
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, ", ")
}

// ToolChoiceInstruction tool_choice → 注入段尾句（BK tool.go:40-72 同表；无指令 → ""）。
//
//	"none"                     → 不得调用工具，直接文本回答
//	"required" / "any"         → 必须至少调用一个工具
//	{"type":"tool","name":X}   → 必须调用工具 X（Anthropic 具名形态）
//	{"type":"function","function":{"name":X}} → 同上（OpenAI 具名形态）
//	"auto" / 未声明 / 未知     → ""（不加句——缺省语义就是可调可不调）
func ToolChoiceInstruction(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return ""
	}
	var lit string
	if err := json.Unmarshal(raw, &lit); err == nil {
		switch lit {
		case "none":
			return "Tool choice is set to \"none\": do not call any tool, answer in plain text."
		case "required", "any":
			return "Tool choice is set to \"required\": you must call at least one tool."
		}
		return ""
	}
	var obj struct {
		Type     string `json:"type"`
		Name     string `json:"name"`
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	name := obj.Name
	if name == "" {
		name = obj.Function.Name
	}
	switch obj.Type {
	case "none":
		return "Tool choice is set to \"none\": do not call any tool, answer in plain text."
	case "any":
		return "Tool choice is set to \"required\": you must call at least one tool."
	case "tool", "function":
		if name != "" {
			return "Tool choice is set to the tool \"" + name + "\": you must call that tool."
		}
	}
	return ""
}
