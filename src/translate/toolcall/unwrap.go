package toolcall

// UnwrapDoubleEncoded 解一层双重编码（规格 §4.3，恒开，与修复开关正交）。
//
// 位置：parse 成功之后（顶层 parse 得 String 才可能是双重编码——漏过修复层的
// 一类）。规则：只解一层；内层必须 parse 成 object/array 才采用；标量内层
// 不动（不臆测）。命中返回 canonical 内层串。
func UnwrapDoubleEncoded(s string) (string, bool) {
	v, err := parseOrdered(s)
	if err != nil {
		return "", false
	}
	str, ok := v.(string)
	if !ok {
		return "", false
	}
	inner, err := parseOrdered(str)
	if err != nil {
		return "", false
	}
	switch inner.(type) {
	case *jsonObject, []jsonValue:
		return serializeOrdered(inner), true
	}
	return "", false
}
