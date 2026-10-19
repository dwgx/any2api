package toolcall

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// jsonValue 是保序 JSON 值树：
// nil / bool / json.Number / string / *jsonObject / []jsonValue。
type jsonValue any

// jsonObject 保序对象。键序 = 文档序（对齐 Python dict 插入序）；重复键按
// Python 语义就地覆盖，位置保持首次出现处——canonical 输出须与 oracle 的
// json.dumps(..., ensure_ascii=False, separators=(",", ":")) 逐字节一致。
type jsonObject struct {
	keys []string
	vals []jsonValue
}

func (o *jsonObject) set(key string, v jsonValue) {
	for i, k := range o.keys {
		if k == key {
			o.vals[i] = v
			return
		}
	}
	o.keys = append(o.keys, key)
	o.vals = append(o.vals, v)
}

// parseOrdered 解析 s 为保序值树；要求单值 + 仅空白尾随（Python json.loads 语义）。
func parseOrdered(s string) (jsonValue, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	v, err := decodeOrderedValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		if err == nil {
			return nil, errors.New("trailing content")
		}
		return nil, err
	}
	return v, nil
}

func decodeOrderedValue(dec *json.Decoder) (jsonValue, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			obj := &jsonObject{}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, ok := kt.(string)
				if !ok {
					return nil, errors.New("object key not a string")
				}
				v, err := decodeOrderedValue(dec)
				if err != nil {
					return nil, err
				}
				obj.set(key, v)
			}
			if _, err := dec.Token(); err != nil { // 收尾 '}'
				return nil, err
			}
			return obj, nil
		case '[':
			arr := []jsonValue{}
			for dec.More() {
				v, err := decodeOrderedValue(dec)
				if err != nil {
					return nil, err
				}
				arr = append(arr, v)
			}
			if _, err := dec.Token(); err != nil { // 收尾 ']'
				return nil, err
			}
			return arr, nil
		}
		return nil, fmt.Errorf("unexpected delimiter %v", t)
	case bool, json.Number, string, nil:
		return t, nil
	}
	return nil, errors.New("unexpected token")
}

// CanonicalJSON 把合法 JSON 文本重序列化为规范形式（键序保留、无多余空白、
// ensure_ascii=False 风格转义）。输出与 oracle 的
// json.dumps(v, ensure_ascii=False, separators=(",", ":")) 逐字节对齐。
func CanonicalJSON(s string) (string, bool) {
	v, err := parseOrdered(s)
	if err != nil {
		return "", false
	}
	return serializeOrdered(v), true
}

func serializeOrdered(v jsonValue) string {
	var b strings.Builder
	writeOrdered(&b, v)
	return b.String()
}

func writeOrdered(b *strings.Builder, v jsonValue) {
	switch t := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		if t {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case string:
		writeJSONString(b, t)
	case json.Number:
		writeJSONNumber(b, string(t))
	case *jsonObject:
		b.WriteByte('{')
		for i, k := range t.keys {
			if i > 0 {
				b.WriteByte(',')
			}
			writeJSONString(b, k)
			b.WriteByte(':')
			writeOrdered(b, t.vals[i])
		}
		b.WriteByte('}')
	case []jsonValue:
		b.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				b.WriteByte(',')
			}
			writeOrdered(b, e)
		}
		b.WriteByte(']')
	default:
		// 不可达：jsonValue 的闭包已穷举
		panic(fmt.Sprintf("toolcall: unexpected json value type %T", v))
	}
}

// writeJSONString 按 Python json.dumps(ensure_ascii=False) 的转义集输出字符串
// （<0x20 转义；\b \f \n \r \t 用短形式；其余字符原样）。
func writeJSONString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
}

// writeJSONNumber 对齐 Python json.dumps 的数字形态：整数原样保留字面；
// 浮点用最短 round-trip 表示（整值补 .0）。
func writeJSONNumber(b *strings.Builder, lit string) {
	if strings.ContainsAny(lit, ".eE") {
		if f, err := strconv.ParseFloat(lit, 64); err == nil {
			out := strconv.FormatFloat(f, 'g', -1, 64)
			if !strings.ContainsAny(out, ".eE") {
				out += ".0"
			}
			b.WriteString(out)
			return
		}
	}
	b.WriteString(lit)
}

// firstValueEnd 返回 s 中第一个完整 JSON 值的结束偏移（供归因分类用）。
func firstValueEnd(s string) (int, bool) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	if _, err := decodeOrderedValue(dec); err != nil {
		return 0, false
	}
	return int(dec.InputOffset()), true
}
