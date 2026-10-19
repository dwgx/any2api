package sentinel

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
)

// TestPythonFloatRepr 钉死 Python repr(float) 对齐（阈值/科学计数/整数值 float/
// -0.0/denormal），向量由 Python json.dumps 直接生成。
func TestPythonFloatRepr(t *testing.T) {
	v := loadVectors(t)
	if len(v.FloatRepr) == 0 {
		t.Fatal("no float_repr vectors")
	}
	for _, pair := range v.FloatRepr {
		in, want := pair[0], pair[1]
		f, err := strconv.ParseFloat(in, 64)
		if err != nil {
			t.Fatalf("vector input %q unparsable: %v", in, err)
		}
		if got := pythonFloatString(f); got != want {
			t.Errorf("pythonFloatString(%s) = %q, want %q", in, got, want)
		}
	}
}

// TestPythonStringEscape 钉死 Python json.dumps(ensure_ascii=False) 的转义面
// （引号/反斜杠/控制字符 \u00xx/U+2028/U+2029 原样/非 ASCII 原样）。
func TestPythonStringEscape(t *testing.T) {
	v := loadVectors(t)
	if len(v.StringEscape) == 0 {
		t.Fatal("no string_escape vectors")
	}
	for _, pair := range v.StringEscape {
		raw, want := pair[0], pair[1]
		if got := string(appendPythonString(nil, raw)); got != want {
			t.Errorf("appendPythonString(%q) = %s, want %s", raw, got, want)
		}
	}
}

// TestEncodeConfigVectors 钉死整数组编码：Go 编码器 == Python
// json.dumps(separators=(",",":"), ensure_ascii=False)（向量存 base64 形态）。
func TestEncodeConfigVectors(t *testing.T) {
	v := loadVectors(t)
	if len(v.ConfigEncode) == 0 {
		t.Fatal("no config_encode vectors")
	}
	for i, vec := range v.ConfigEncode {
		want, err := base64.StdEncoding.DecodeString(vec.Expected)
		if err != nil {
			t.Fatalf("vector %d: bad base64: %v", i, err)
		}
		got, err := encodeConfig(vec.Config)
		if err != nil {
			t.Fatalf("vector %d: encodeConfig: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("vector %d:\n got %s\nwant %s", i, got, want)
		}
	}
}

// TestEncodeConfigUintAndFloatForms 补几处参考实现会产出的形态（token 求解时
// 槽位被替换为 int/float 后的编码）。
func TestEncodeConfigUintAndFloatForms(t *testing.T) {
	cases := []struct {
		in   []any
		want string
	}{
		{[]any{int(0), int64(1 << 40), uint32(7)}, `[0,1099511627776,7]`},
		{[]any{float64(0), float64(2000)}, `[0.0,2000.0]`},
		{[]any{float64(0.5), float64(1500)}, `[0.5,1500.0]`},
		{[]any{nil, true, false}, `[null,true,false]`},
		{[]any{json.Number("98765.4321")}, `[98765.4321]`},
	}
	for _, c := range cases {
		got, err := encodeConfig(c.in)
		if err != nil {
			t.Fatalf("encodeConfig(%v): %v", c.in, err)
		}
		if string(got) != c.want {
			t.Errorf("encodeConfig(%v) = %s, want %s", c.in, got, c.want)
		}
	}
}

// TestEncodeConfigRejectsUnknownType 未知槽位类型必须报错，绝不静默替换。
func TestEncodeConfigRejectsUnknownType(t *testing.T) {
	_, err := encodeConfig([]any{struct{ X int }{1}})
	if !errors.Is(err, ErrSlotType) {
		t.Fatalf("err = %v, want ErrSlotType", err)
	}
}
