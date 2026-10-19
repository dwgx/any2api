package toolsim

import (
	"encoding/json"
	"strings"
)

// Segment 扫描片段：Text 或 Call 恰好其一非零。
type Segment struct {
	Text string // 文本片段（原样——含未识别标签与降级回放）
	Call *Call  // 工具调用片段
}

// Call 从模型文本解析出的工具调用。
type Call struct {
	Name string // 工具名（属性原值；净化名还原由调用方按 toolname 反向表做）
	ID   string // 模型给出的 id（可空——出口 writer 按 hygiene 合成 toolu_…）
	Args string // 参数 JSON 对象文本（已校验）；空标签体 → ""（无参——出口 `{}` 纪律接管）
}

// scanState 扫描状态。
type scanState int

const (
	stText scanState = iota // 普通文本：找 '<'
	stTag                   // 疑似标签：缓冲到判定（开标签完整 / 证伪）
	stBody                  // 契约开标签已判定：缓冲标签体到闭标签
)

// Scanner 契约标签增量扫描器（跨 chunk 缓冲；状态跨 Feed 保留）。
//
// 行为（设计单 §S2 验收 ①②③ + BK tagged.go:379-582 基准）：
//
//   - 未判定尾巴（`<` / 半截标签 / 未闭合体）留在内部，等下一片——绝不把半截标签
//     当文本吐出（BK `pendingTag` 同款）；
//   - 合成调用要过双判据：闭标签成对 + 标签体是 JSON **对象**（设计单 §U3 兜底：
//     体内含 `</tool_use>` 字面量时按候选逐个试 JSON，取第一个可解析候选）；
//   - 其余一切（未知标签 / `<tool_result>` / 坏 JSON / 未闭合 / 无 name 属性）→
//     原样文本降级：绝不吞内容、绝不合成块。
//
// 转义约定（W21-2 固化；U3 出口侧契约——注入/历史回放文本**不做 XML 转义**
// （claude_web/render.go flattenBlock 同款纪律），故参数 JSON 的字符串值里可以
// 合法出现 `</tool_use>` 子串，出口按下面三条处置）：
//
//   - 候选试 JSON：对标签体内**每个** `</tool_use>` 出现位置依次尝试，取第一个
//     标签体可解析为 JSON 对象的候选为真闭标签（`{"a":"</tool_use>"}` 这类子串
//     自然落入下一候选）；
//   - 先到先得：候选判定不回看后续出现位置——体已合法的最早候选即胜出，其后
//     内容（含裸 `</tool_use>` 字面量）按普通文本继续扫描；
//   - 全部候选不可解析 → 未判定（Flush 时整段文本降级）；讲解/引用场景按双判据
//     分流：成对标签 + 体为合法 JSON 对象 → 与真实调用形态不可区分，会被判为
//     调用（语义边界，W17-1 已裁定）；不成对 / 体不可解析 → 原样文本降级。
type Scanner struct {
	st      scanState
	buf     strings.Builder // 未判定尾巴（stText 尾段 / stTag 全文 / stBody 的体）
	openTag string          // 已判定的契约开标签原文（stBody 期间；降级回放用）
	name    string          // 开标签 name 属性
	id      string          // 开标签 id 属性（可空）
}

// Feed 追加一段文本，返回已判定的片段；未判定尾巴留在内部（跨 chunk 缓冲）。
func (s *Scanner) Feed(text string) []Segment {
	if text == "" {
		return nil
	}
	s.buf.WriteString(text)
	var segs []Segment
	for {
		switch s.st {
		case stText:
			b := s.buf.String()
			i := strings.IndexByte(b, '<')
			if i < 0 {
				segs = appendText(segs, b)
				s.buf.Reset()
				return segs
			}
			segs = appendText(segs, b[:i])
			s.buf.Reset()
			s.buf.WriteString(b[i:])
			s.st = stTag

		case stTag:
			b := s.buf.String()
			if ambiguousTagPrefix(b) {
				return segs // `<` / `<tool` / … 还可能是契约标签：等更多
			}
			if strings.HasPrefix(b, OpenTag) {
				rest := b[len(OpenTag):]
				if rest == "" {
					return segs // 恰为 "<tool_use"：还可能是 "<tool_use …"
				}
				switch rest[0] {
				case ' ', '\t', '\r', '\n', '>':
					gt := strings.IndexByte(b, '>')
					if gt < 0 {
						return segs // 开标签未闭合：等更多
					}
					name, id, ok := parseTagAttrs(b[len(OpenTag):gt])
					if !ok {
						// 属性不合法（无 name / 引号未闭合）→ 文本降级
						segs = appendText(segs, b[:gt+1])
						s.buf.Reset()
						s.buf.WriteString(b[gt+1:])
						s.st = stText
						continue
					}
					s.openTag, s.name, s.id = b[:gt+1], name, id
					s.buf.Reset()
					s.buf.WriteString(b[gt+1:])
					s.st = stBody
					continue
				default:
					// `<tool_useX`：不是契约标签 → 文本降级
					segs = appendText(segs, b)
					s.buf.Reset()
					s.st = stText
					continue
				}
			}
			// 非契约标签（含 </tool_use>、<tool_result>、未知标签）→ 原样文本
			segs = appendText(segs, b)
			s.buf.Reset()
			s.st = stText

		case stBody:
			b := s.buf.String()
			matched := false
			for idx := 0; ; {
				k := strings.Index(b[idx:], CloseTag)
				if k < 0 {
					break // 闭标签未到（可能跨片）：等更多
				}
				k += idx
				if args, ok := validArgs(b[:k]); ok {
					segs = append(segs, Segment{Call: &Call{Name: s.name, ID: s.id, Args: args}})
					s.buf.Reset()
					s.buf.WriteString(b[k+len(CloseTag):])
					s.openTag, s.name, s.id = "", "", ""
					s.st = stText
					matched = true
					break
				}
				idx = k + len(CloseTag) // 候选不成对（体内字面量）→ 试下一个候选
			}
			if !matched {
				return segs // 候选未过 / 未到 → 等更多（块尾由 Flush 降级）
			}
		}
	}
}

// Flush 输入结束（text 块尾 / 流尾）：未判定尾巴按「非契约 → 原样文本」降级吐出，
// 状态复位（标签不跨块——块边界即判据边界）。
func (s *Scanner) Flush() []Segment {
	var segs []Segment
	switch s.st {
	case stText, stTag:
		segs = appendText(segs, s.buf.String())
	case stBody:
		segs = appendText(segs, s.openTag+s.buf.String())
	}
	s.Reset()
	return segs
}

// Reset 丢弃未判定状态（截断 / 失败流：半成品既不补也不发）。
func (s *Scanner) Reset() {
	s.buf.Reset()
	s.openTag, s.name, s.id = "", "", ""
	s.st = stText
}

// ScanText 整段文本扫描（非流式面 / 测试用）：等价于 Feed(text) + Flush()。
func ScanText(text string) []Segment {
	var s Scanner
	return append(s.Feed(text), s.Flush()...)
}

// ambiguousTagPrefix b 是契约开/闭标签的**真前缀**（如 `<`、`<tool`、`</tool_use`）
// → 判定待定，必须等更多字节。
func ambiguousTagPrefix(b string) bool {
	return properPrefix(b, OpenTag) || properPrefix(b, CloseTag)
}

func properPrefix(b, tag string) bool {
	return len(b) < len(tag) && strings.HasPrefix(tag, b)
}

// appendText 追加文本片段（与上一文本片段合并，减少下游 delta 数）。
func appendText(segs []Segment, text string) []Segment {
	if text == "" {
		return segs
	}
	if n := len(segs); n > 0 && segs[n-1].Call == nil {
		segs[n-1].Text += text
		return segs
	}
	return append(segs, Segment{Text: text})
}

// validArgs 标签体 → 参数 JSON。空体 = **无参**（返回 ""——下游 `{}` 纪律接管：
// Anthropic 靠 start 的 `{}` 不发 delta，OpenAI 出口显式发 `{}`；绝不在此处替
// 出口做形态决定）；其余必须是非空 JSON **对象**（工具参数面；数组/标量不是合法参数）。
func validArgs(body string) (string, bool) {
	t := strings.TrimSpace(body)
	if t == "" {
		return "", true
	}
	if !strings.HasPrefix(t, "{") || !json.Valid([]byte(t)) {
		return "", false
	}
	return t, true
}

// parseTagAttrs 解析契约开标签属性区（容忍空白与单双引号；裸值到空白）。
// 缺 name → ok=false（调用方文本降级——无工具名的调用无从兑现）。
func parseTagAttrs(attrs string) (name, id string, ok bool) {
	s := attrs
	for {
		s = strings.TrimLeft(s, " \t\r\n")
		if s == "" {
			break
		}
		eq := strings.IndexByte(s, '=')
		if eq <= 0 {
			return "", "", false
		}
		key := strings.TrimSpace(s[:eq])
		rest := strings.TrimLeft(s[eq+1:], " \t\r\n")
		if rest == "" {
			return "", "", false
		}
		var val string
		if q := rest[0]; q == '"' || q == '\'' {
			end := strings.IndexByte(rest[1:], q)
			if end < 0 {
				return "", "", false
			}
			val = rest[1 : 1+end]
			rest = rest[1+end+1:]
		} else {
			sp := strings.IndexAny(rest, " \t\r\n")
			if sp < 0 {
				val, rest = rest, ""
			} else {
				val, rest = rest[:sp], rest[sp:]
			}
		}
		switch key {
		case "name":
			name = val
		case "id":
			id = val
		}
		s = rest
	}
	if name == "" {
		return "", "", false
	}
	return name, id, true
}
