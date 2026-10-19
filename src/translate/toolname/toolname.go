// Package toolname 工具名清洗与还原层（双向映射）——P67 实锤缺陷的修复面。
//
// 缺陷（research/topics/schema-passthrough/README.md，r3-r12 实测）：客户端可发
// 超长/非法字符的工具名（CC 的 MCP 工具实测 84 字真上线），上游按文档校验
// （gemini `^[A-Za-z_][A-Za-z0-9_.:-]{0,63}$`；openai responses/chat
// `^[a-zA-Z0-9_-]{1,64}$`）整请求 400——没有「跳过坏工具」语义，网关必须自己先净化。
//
// 修复形态（r7/r14 实测）：**截断 64 + 响应侧还原**——上游 200（按截断名回调），
// 网关把响应里的工具名映射回原名后客户端才认得（r14：74 字原名工具真执行）。
//
// 三层职责：
//  1. SanitizeName：单个名字 → 合法名字（字符集归一 + ≤64 截断）；
//  2. Build：客户端工具清单 → 清洗表（原名 → 净化名，仅收**变化了**的条目）；
//     撞名（两个原名截断后同形）追加数字后缀消歧，仍 ≤64；
//  3. 还原：`SanitizeRequest` / `RestoreResponse` / `RestoreEvents` 在 IR 面做
//     双向映射（渲染器/解析器不认识本层——IR 名字进渲染器时已是净化名）。
//
// 纪律：
//   - 只改**需要改**的名字（合法名不进表、不被改写——r14 的 74 字名是唯一变量）；
//   - 确定性：同一输入恒得同一输出（不依赖 map 迭代顺序——撞名按工具声明顺序消歧）；
//   - 名字承载调用契约：还原表以**净化名**为键（上游回什么名字就查什么）。
package toolname

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dwgx/any2api/translate/ir"
)

// MaxLen 上游工具名长度上限（Anthropic/OpenAI 文档 64；gemini 也是 64——
// gemini-conversion-table:88）。三家同值，故单一常量。
const MaxLen = 64

// upstreamNameOK 名字是否已满足上游约束（gemini 字符集是四家最严的：
// 首字符字母/下划线，其余字母数字 `_ . : -`；openai 的 `[a-zA-Z0-9_-]` 是其子集）。
// 合法即直过（不改写——「只改需要改的」）。
func upstreamNameOK(name string) bool {
	if name == "" || len(name) > MaxLen {
		return false
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case i > 0 && (r >= '0' && r <= '9' || r == '.' || r == ':' || r == '-'):
		default:
			return false
		}
	}
	return true
}

// sanitizeOne 单名净化：非法字符 → `_`，首字符数字 → 前缀 `_`，截断到 MaxLen。
func sanitizeOne(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_', r == '.', r == ':', r == '-':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	s := b.String()
	if s == "" {
		s = "_"
	}
	// 首字符必须是字母或下划线（gemini 约束；openai 无此要求但子集安全）。
	if c := s[0]; !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_') {
		s = "_" + s
	}
	if len(s) > MaxLen {
		s = s[:MaxLen]
	}
	return s
}

// Build 客户端工具清单 → 清洗表（原名 → 净化名）。只收变化了的条目（合法名
// 不进表——还原/清洗都是「查表命中才动」）。确定性：按工具声明顺序处理；撞名
// （截断后同形）追加 `_2`、`_3`… 后缀（截断保持 ≤64）。
//
// 两遍：先预约全部合法名，再净化非法名——合法名不因声明在非法名之后而被顶掉
// （「只改需要改的」）。净化名全部合法，故对 Build 的输出再 Build 返回 nil，
// 同一张表也不会再改写自己的输出（幂等）。
func Build(tools []ir.ToolDef) map[string]string {
	if len(tools) == 0 {
		return nil
	}
	// 第一遍：合法名原样保留，先占住名字空间（与声明顺序无关）。
	used := make(map[string]bool, len(tools))
	for _, td := range tools {
		if td.Name != "" && upstreamNameOK(td.Name) {
			used[td.Name] = true
		}
	}
	// 第二遍：非法名按声明顺序净化 + 对已占名字（含预约的合法名）消歧。
	table := make(map[string]string)
	for _, td := range tools {
		if td.Name == "" || upstreamNameOK(td.Name) {
			continue
		}
		s := sanitizeOne(td.Name)
		if used[s] {
			// 撞名：追加 `_2`、`_3`… 后缀（截断保持 ≤64）。
			base := s
			for n := 2; ; n++ {
				suffix := fmt.Sprintf("_%d", n)
				cand := base
				if len(cand)+len(suffix) > MaxLen {
					cand = cand[:MaxLen-len(suffix)]
				}
				s = cand + suffix
				if !used[s] {
					break
				}
			}
		}
		used[s] = true
		table[td.Name] = s // 非法名净化后必然变化（字符/首字符/长度至少一项）
	}
	if len(table) == 0 {
		return nil
	}
	return table
}

// SanitizeRequest 请求面清洗（IR 级）：工具定义名、工具选择引用与消息历史里的
// 工具调用名按表映射（表 nil = 直过）。**幂等**（净化名不再命中表）。
//
// 只动名字引用：schema 体、参数、描述一律不碰（P67 结论：体/信封是另一层）。
func SanitizeRequest(req *ir.Request, table map[string]string) {
	if req == nil || len(table) == 0 {
		return
	}
	for i := range req.Tools {
		if s, ok := table[req.Tools[i].Name]; ok {
			req.Tools[i].Name = s
		}
	}
	req.ToolChoice = sanitizeToolChoice(req.ToolChoice, table)
	for i := range req.Messages {
		m := &req.Messages[i]
		if s, ok := table[m.Name]; ok {
			m.Name = s
		}
		for j := range m.Blocks {
			if tc := m.Blocks[j].ToolCall; tc != nil {
				if s, ok := table[tc.Name]; ok {
					tc.Name = s
				}
			}
		}
	}
}

// sanitizeToolChoice 工具选择字段里的函数名引用（对象形态的
// `{"type":"function","function":{"name":…}}` / gemini 出口的 allowedFunctionNames
// 来源）。字符串形态（"auto"/"none"/"required"）无名字、原样。非对象或解析失败
// 一律原样（保真纪律：本层不改它看不懂的东西）。
func sanitizeToolChoice(raw json.RawMessage, table map[string]string) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw
	}
	changed := false
	if fn, ok := obj["function"]; ok {
		var fobj map[string]json.RawMessage
		if err := json.Unmarshal(fn, &fobj); err == nil {
			if nameRaw, ok := fobj["name"]; ok {
				var name string
				if err := json.Unmarshal(nameRaw, &name); err == nil {
					if s, ok := table[name]; ok {
						fobj["name"] = mustMarshal(s)
						obj["function"] = mustMarshal(fobj)
						changed = true
					}
				}
			}
		}
	}
	if !changed {
		return raw
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	return out
}

// RestoreResponse 响应面还原（IR 级）：工具调用名按**净化名→原名**映射回来
// （表 nil = 直过）。客户端据此才认得出工具（r14）。
func RestoreResponse(resp *ir.Response, table map[string]string) {
	if resp == nil || len(table) == 0 {
		return
	}
	for i := range resp.Message.Blocks {
		if tc := resp.Message.Blocks[i].ToolCall; tc != nil {
			if orig, ok := table[tc.Name]; ok {
				tc.Name = orig
			}
		}
	}
}

// RestoreEvents 流式事件还原：tool_use 块首片（block_start 的 ToolName）按表映射。
func RestoreEvents(events []ir.StreamEvent, table map[string]string) {
	if len(table) == 0 {
		return
	}
	for i := range events {
		if events[i].ToolName == "" {
			continue
		}
		if orig, ok := table[events[i].ToolName]; ok {
			events[i].ToolName = orig
		}
	}
}

// Reverse 清洗表反向（净化名 → 原名）。还原侧统一用反向表查（上游回的是净化名）。
// Build 保证净化名唯一（撞名消歧），故反向无损。
func Reverse(table map[string]string) map[string]string {
	if len(table) == 0 {
		return nil
	}
	out := make(map[string]string, len(table))
	for orig, s := range table {
		out[s] = orig
	}
	return out
}

func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // 不可达：string/map[string]json.RawMessage 恒可序列化
	}
	return b
}
