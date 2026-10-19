package toolcall

import (
	"fmt"
	"strconv"
	"strings"
)

// RepairLayer 修复层标识（诊断）。
type RepairLayer string

// 修复层（规格 §4.2）。
const (
	LayerNone      RepairLayer = "none"
	LayerChar      RepairLayer = "char"
	LayerStructure RepairLayer = "structure"
	LayerGlued     RepairLayer = "glued"
)

// RepairJSONCharLevel（L1）只在字符串字面量内做字符级修复：
//   - 非法转义（\U / \x / \. / 行尾孤立 \）→ 反斜杠再转义（\\）；
//   - 裸控制符 <0x20 → \n / \t / \b / \f / \r / \uXXXX；
//   - 半截 \uXXXX → 降级字面；孤立 UTF-16 代理 → 降级（合法代理对保留）。
//
// 字符串外字符一律不动（结构字符原样透传）。逐 rune 处理（String 级重组）。
func RepairJSONCharLevel(s string) string {
	chars := []rune(s)
	out := make([]rune, 0, len(chars)+8)
	hex4 := func(j int) (string, int) {
		var h []rune
		for j < len(chars) && len(h) < 4 && isHexDigit(chars[j]) {
			h = append(h, chars[j])
			j++
		}
		return string(h), j
	}
	inString := false
	for i := 0; i < len(chars); {
		c := chars[i]
		if !inString {
			out = append(out, c)
			if c == '"' {
				inString = true
			}
			i++
			continue
		}
		switch {
		case c == '"':
			out = append(out, c)
			inString = false
			i++
		case c == '\\':
			if i+1 >= len(chars) {
				out = append(out, '\\', '\\') // 行尾孤立反斜杠 → 字面反斜杠
				i++
				break
			}
			esc := chars[i+1]
			switch {
			case strings.ContainsRune(`"\/bfnrt`, esc):
				out = append(out, '\\', esc)
				i += 2
			case esc == 'u':
				h, j := hex4(i + 2)
				if len(h) != 4 {
					out = append(out, []rune(`\\u`+h)...) // 半截 \uXXXX → 降级字面
					i = j
					break
				}
				cp, _ := strconv.ParseUint(h, 16, 32)
				switch {
				case cp >= 0xD800 && cp <= 0xDBFF: // 高代理
					low := ""
					if j+1 < len(chars) && chars[j] == '\\' && chars[j+1] == 'u' {
						h2, j2 := hex4(j + 2)
						if len(h2) == 4 {
							if cp2, err := strconv.ParseUint(h2, 16, 32); err == nil && cp2 >= 0xDC00 && cp2 <= 0xDFFF {
								low = h2
								j = j2
							}
						}
					}
					if low != "" {
						out = append(out, []rune(`\u`+h+`\u`+low)...) // 合法代理对保留
					} else {
						out = append(out, []rune(`\\u`+h)...) // 孤立高代理 → 降级
					}
					i = j
				case cp >= 0xDC00 && cp <= 0xDFFF:
					out = append(out, []rune(`\\u`+h)...) // 孤立低代理 → 降级
					i = j
				default:
					out = append(out, []rune(`\u`+h)...)
					i = j
				}
			default:
				out = append(out, '\\', '\\')
				out = append(out, []rune(repairEscChar(esc))...)
				i += 2
			}
		case c < 0x20:
			out = append(out, []rune(repairEscChar(c))...)
			i++
		default:
			out = append(out, c)
			i++
		}
	}
	return string(out)
}

func isHexDigit(r rune) bool {
	return r >= '0' && r <= '9' || r >= 'a' && r <= 'f' || r >= 'A' && r <= 'F'
}

func repairEscChar(c rune) string {
	switch c {
	case '\n':
		return `\n`
	case '\r':
		return `\r`
	case '\t':
		return `\t`
	case '\b':
		return `\b`
	case '\f':
		return `\f`
	}
	if c < 0x20 {
		return fmt.Sprintf(`\u%04x`, c)
	}
	return string(c)
}

// RepairJSONStructure（L2）按括号栈逆序补收尾：先补未闭合 "，再补 } / ]，
// 末尾悬空 \ 补成字面反斜杠。键后无值 / 逗号后无元素 → 闭合后仍非法
// → 复验拒绝（不猜缺失的值）。
func RepairJSONStructure(s string) string {
	inString, escaped := false, false
	var stack []rune
	for _, c := range s {
		if inString {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{':
			stack = append(stack, '}')
		case '[':
			stack = append(stack, ']')
		case '}', ']':
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		}
	}
	var b strings.Builder
	b.WriteString(s)
	if escaped {
		b.WriteByte('\\')
	}
	if inString {
		b.WriteByte('"')
	}
	for i := len(stack) - 1; i >= 0; i-- {
		b.WriteRune(stack[i])
	}
	return b.String()
}

// RepairJSONGlued（L3）只处理「串首多余 }」形态：trim 后以 } 开头 → 从第一个
// { 截取候选，候选须复验通过才采用。单块内双完整对象不可修（不臆测保留哪个）。
func RepairJSONGlued(s string) (string, bool) {
	if !strings.HasPrefix(strings.TrimLeft(s, " \t\n\v\f\r"), "}") {
		return "", false
	}
	pos := strings.Index(s, "{")
	if pos < 0 {
		return "", false
	}
	cand := s[pos:]
	cf := RepairJSONCharLevel(cand)
	if v, ok := CanonicalJSON(cf); ok {
		return v, true
	}
	sf := RepairJSONStructure(cf)
	if v, ok := CanonicalJSON(sf); ok {
		return v, true
	}
	return "", false
}

// RepairToolJSON 三层串行修复（字符 → 结构 → 粘连），每层结果都必须重新
// parse 通过才采用（强制复验）；全失败返回 ok=false——修不好 == 不修。
//
// 入口条件：只在 json 解析已失败时由调用方介入（合法 JSON 永不进入）。
// 第二层作用于第一层的输出（与 oracle 一致）；第三层对原串做候选截取。
func RepairToolJSON(s string) (string, RepairLayer, bool) {
	if strings.TrimSpace(s) == "" {
		return "", LayerNone, false
	}
	cf := RepairJSONCharLevel(s)
	if v, ok := CanonicalJSON(cf); ok {
		return v, LayerChar, true
	}
	sf := RepairJSONStructure(cf)
	if v, ok := CanonicalJSON(sf); ok {
		return v, LayerStructure, true
	}
	if v, ok := RepairJSONGlued(s); ok {
		return v, LayerGlued, true
	}
	return "", LayerNone, false
}

// ClassifyDefect 对「修不好」的参数串做单遍 string-aware 扫描归因（纯观测，
// 不进控制流）。kind：truncated（未闭合）/ illegal_chars（字符串内非法转义或
// 裸控制符）/ truncated_and_illegal（两者兼有）/ malformed（其余）。
// subkind 为 malformed 的尽力分类（trailing_chars / trailing_comma），未覆盖
// 的形态留空——诊断只进日志，控制流只看「合法/非法 + 失败态」。
func ClassifyDefect(s string) (kind, subkind string) {
	chars := []rune(s)
	inString, escaped := false, false
	depth := 0
	illegal := false
	for i := range len(chars) {
		c := chars[i]
		if inString {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
				if i+1 >= len(chars) {
					illegal = true // 行尾孤立反斜杠
				} else {
					switch n := chars[i+1]; {
					case strings.ContainsRune(`"\/bfnrt`, n):
					case n == 'u':
						h := 0
						for j := i + 2; j < len(chars) && h < 4 && isHexDigit(chars[j]); j++ {
							h++
						}
						if h < 4 {
							illegal = true // 半截 \uXXXX
						}
					default:
						illegal = true
					}
				}
			case c == '"':
				inString = false
			case c < 0x20:
				illegal = true // 裸控制符
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			depth++
		case '}', ']':
			if depth > 0 {
				depth--
			}
		}
	}
	truncated := inString || depth > 0
	switch {
	case truncated && illegal:
		return "truncated_and_illegal", ""
	case truncated:
		return "truncated", ""
	case illegal:
		return "illegal_chars", ""
	}
	// malformed：尽力分类（trailing_chars / trailing_comma），其余留空。
	if end, ok := firstValueEnd(s); ok && strings.TrimSpace(s[end:]) != "" {
		return "malformed", "trailing_chars"
	}
	if strings.Contains(s, ",}") || strings.Contains(s, ",]") {
		return "malformed", "trailing_comma"
	}
	return "malformed", ""
}
