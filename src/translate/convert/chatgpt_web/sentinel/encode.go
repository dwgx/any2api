package sentinel

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

// 本文件把 config 槽位数组编码成与 Python 逐字节一致的紧凑 JSON：
//
//	json.dumps(config, separators=(",", ":"), ensure_ascii=False)
//
// 为什么不能用 encoding/json：Go 的 float 编码与 Python repr 有三处已知差异——
//  1. 整数值 float：Python "2.0" vs Go "2"；
//  2. 科学计数阈值：Python 在十进制指数 ≤ -5 或 ≥ 16 时用科学计数（repr(1e16)
//     == "1e+16"、repr(0.0001) == "0.0001"），Go encoding/json 用 |x| < 1e-6 或
//     ≥ 1e21；
//  3. U+2028/U+2029：Go encoding/json 强制转义，Python（ensure_ascii=False）不转。
//
// 而 p token 是 base64(JSON(config)) 的**字节级**产物（服务端结构校验 + PoW
// 哈希输入），所以必须对齐。对齐性由 testdata/pow_vectors.json 的真值向量钉死
// （向量由 Python 参考实现直接生成，见 tmp/lanes/W41-sentinel/gen_vectors.py）。

// encodeConfig 把槽位数组编码为紧凑 JSON（Python json.dumps 对齐；见文件头注）。
func encodeConfig(cfg []any) ([]byte, error) {
	b := make([]byte, 0, 256)
	b = append(b, '[')
	for i, v := range cfg {
		if i > 0 {
			b = append(b, ',')
		}
		var err error
		b, err = appendSlot(b, v)
		if err != nil {
			return nil, err
		}
	}
	b = append(b, ']')
	return b, nil
}

// appendSlot 编码单个槽位值。支持面 = Config 文档声明的类型集；其余报
// ErrSlotType（绝不静默替换成 null）。
func appendSlot(b []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(b, "null"...), nil
	case bool:
		if x {
			return append(b, "true"...), nil
		}
		return append(b, "false"...), nil
	case string:
		return appendPythonString(b, x), nil
	case json.Number:
		// 原样透传（向量文件用 json.Number 保留 Python 的原始字面量）。
		return append(b, x.String()...), nil
	case float64:
		return appendPythonFloat(b, x), nil
	case float32:
		return appendPythonFloat(b, float64(x)), nil
	case int:
		return strconv.AppendInt(b, int64(x), 10), nil
	case int8:
		return strconv.AppendInt(b, int64(x), 10), nil
	case int16:
		return strconv.AppendInt(b, int64(x), 10), nil
	case int32:
		return strconv.AppendInt(b, int64(x), 10), nil
	case int64:
		return strconv.AppendInt(b, x, 10), nil
	case uint:
		return strconv.AppendUint(b, uint64(x), 10), nil
	case uint8:
		return strconv.AppendUint(b, uint64(x), 10), nil
	case uint16:
		return strconv.AppendUint(b, uint64(x), 10), nil
	case uint32:
		return strconv.AppendUint(b, uint64(x), 10), nil
	case uint64:
		return strconv.AppendUint(b, x, 10), nil
	default:
		return nil, ErrSlotType
	}
}

// appendPythonString 转义字符串，规则 = Python json.encoder.py_encode_basestring
// （ensure_ascii=False）：仅 `"`、`\`、控制字符 <0x20（\b \f \n \r \t 或 \u00xx）
// 被转义；其余（含 U+2028/U+2029、非 ASCII）原样输出。
func appendPythonString(b []byte, s string) []byte {
	b = append(b, '"')
	for i := range len(s) {
		c := s[i]
		switch c {
		case '"':
			b = append(b, '\\', '"')
		case '\\':
			b = append(b, '\\', '\\')
		case '\b':
			b = append(b, '\\', 'b')
		case '\f':
			b = append(b, '\\', 'f')
		case '\n':
			b = append(b, '\\', 'n')
		case '\r':
			b = append(b, '\\', 'r')
		case '\t':
			b = append(b, '\\', 't')
		default:
			if c < 0x20 {
				const hexDigits = "0123456789abcdef"
				b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
			} else {
				b = append(b, c) // 字节级透传（UTF-8 多字节字符按原字节走）
			}
		}
	}
	return append(b, '"')
}

// appendPythonFloat 输出 Python repr(float)（== json.dumps 的 float 形态）：
// 最短往返十进制位数 + 科学计数阈值（十进制指数 ≤ -5 或 ≥ 16）。
//
// 例：0.0→"0.0"、2.0→"2.0"、1e15→"1000000000000000.0"、1e16→"1e+16"、
// 0.0001→"0.0001"、1e-05→"1e-05"、-0.0→"-0.0"。
func appendPythonFloat(b []byte, f float64) []byte {
	switch {
	case math.IsNaN(f):
		return append(b, "NaN"...) // Python json.dumps 输出非标准 JSON 记号
	case math.IsInf(f, 1):
		return append(b, "Infinity"...)
	case math.IsInf(f, -1):
		return append(b, "-Infinity"...)
	case f == 0:
		if math.Signbit(f) {
			return append(b, "-0.0"...)
		}
		return append(b, "0.0"...)
	}

	// strconv 的 'e' 格式给最短往返数字：如 "-1.2345e+16"、"5e-324"。
	s := strconv.FormatFloat(f, 'e', -1, 64)
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	}
	ePos := strings.IndexByte(s, 'e')
	mant, expStr := s[:ePos], s[ePos+1:]
	exp, _ := strconv.Atoi(expStr) // 'e' 格式保证可解析
	digits := strings.Replace(mant, ".", "", 1)

	if neg {
		b = append(b, '-')
	}
	if exp < -4 || exp > 15 {
		// 科学计数：d[.ddd]e±XX（指数至少两位）。
		b = append(b, digits[0])
		if len(digits) > 1 {
			b = append(b, '.')
			b = append(b, digits[1:]...)
		}
		b = append(b, 'e')
		if exp < 0 {
			b = append(b, '-')
			exp = -exp
		} else {
			b = append(b, '+')
		}
		if exp < 10 {
			b = append(b, '0')
		}
		return strconv.AppendInt(b, int64(exp), 10)
	}

	// 定点：0.00ddd / ddd.ddd / ddd…0.0
	switch {
	case exp < 0:
		b = append(b, "0."...)
		for range -exp - 1 {
			b = append(b, '0')
		}
		return append(b, digits...)
	case exp+1 >= len(digits):
		b = append(b, digits...)
		for range exp + 1 - len(digits) {
			b = append(b, '0')
		}
		return append(b, ".0"...)
	default:
		b = append(b, digits[:exp+1]...)
		b = append(b, '.')
		return append(b, digits[exp+1:]...)
	}
}

// pythonFloatString 返回 Python `repr(f)` / `format(f)` 的字符串形态（2025 代
// 自种子 `format(random.random())` 与向量的 seed 字段同款）。
func pythonFloatString(f float64) string {
	return string(appendPythonFloat(nil, f))
}
