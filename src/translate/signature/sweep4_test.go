// W5-19 条目 3：签名兼容层扩面（Claude/Gemini 目标）单元测试。
//
// 样本为**构造性**合成（protobuf 结构按 CPA claude_validation.go /
// gemini_validation.go 的信封布局手工拼装）：E/R/C/Q 四信封 + Gemini field-2
// 信封；负例覆盖错标记字节、单记录判据、外来信封与空值。
package signature

import (
	"encoding/base64"
	"strings"
	"testing"
)

// claudeChannelBlock channel block：field 1 varint（channel_id=11）+ field 2 bytes。
func claudeChannelBlock() []byte {
	return []byte{0x08, 0x0b, 0x12, 0x02, 0xaa, 0xbb}
}

// claudeContainer container：field 1 bytes = channel block。
func claudeContainer() []byte {
	ch := claudeChannelBlock()
	return append([]byte{0x0a, byte(len(ch))}, ch...)
}

// claudeClassicPayload 经典信封载荷：field 2 container + field 3 varint。
func claudeClassicPayload() []byte {
	c := claudeContainer()
	return append(append([]byte{0x12, byte(len(c))}, c...), 0x18, 0x01)
}

// claudeCAISPayload CAIS 信封载荷：field 1 varint（版本）+ field 2 container + field 3 varint。
func claudeCAISPayload() []byte {
	c := claudeContainer()
	return append(append([]byte{0x08, 0x02, 0x12, byte(len(c))}, c...), 0x18, 0x01)
}

// geminiField2SignaturePayload Gemini field-2 信封：field 2 bytes（container）恰好
// 覆盖全部；container = field 1 bytes（Tink 载荷，首字节 0x01）。
func geminiField2Payload(value []byte) []byte {
	container := append([]byte{0x0a, byte(len(value))}, value...)
	return append([]byte{0x12, byte(len(container))}, container...)
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func TestCompatibleSignatureForProviderClaudeGemini(t *testing.T) {
	classic := b64(claudeClassicPayload())
	cais := b64(claudeCAISPayload())
	// 双信封：内层是 ASCII base64 文本（首字符即信封标记）。
	double := b64([]byte(classic))
	caqs := b64([]byte(cais))
	// GPT Fernet 形态样本（73 字节载荷，首字节 0x80）。
	gptPayload := make([]byte, 73)
	gptPayload[0] = 0x80
	gpt := "gAAAA" + b64(gptPayload)[5:]
	// Gemini：Tink 载荷（首字节 0x01）。
	gemini := b64(geminiField2Payload([]byte{0x01, 0xde, 0xad, 0xbe, 0xef}))
	// Gemini：field-2 包 UUID 载荷。
	uuid := "d9743975-4bb0-4936-9e28-d5b0d21bdc48"
	geminiUUID := b64(geminiField2Payload([]byte(uuid)))
	// 错标记字节（0x0a，base64 首字符 'C'——CAIS 检查在首字节失败）。
	badMarker := b64([]byte{0x0a, 0x00})
	// 经典信封但 channel block 缺 field 1 varint（结构不完整）。
	emptyChannel := b64(append([]byte{0x12, 0x02, 0x0a, 0x00}, 0x18, 0x01))
	// R 信封但内层不是 E 开头。
	badInner := b64([]byte("xyz"))

	cases := []struct {
		name     string
		provider Provider
		sig      string
		want     bool
	}{
		{"claude_E_single_layer", ProviderClaude, classic, true},
		{"claude_R_double_layer", ProviderClaude, double, true},
		{"claude_CAIS", ProviderClaude, cais, true},
		{"claude_Q_caqs", ProviderClaude, caqs, true},
		{"claude_cache_prefix_stripped", ProviderClaude, "ccmax#" + classic, true},
		{"claude_wrong_marker_byte", ProviderClaude, badMarker, false},
		{"claude_missing_channel_id", ProviderClaude, emptyChannel, false},
		{"claude_R_bad_inner", ProviderClaude, badInner, false},
		{"claude_rejects_gpt", ProviderClaude, gpt, false},
		{"claude_rejects_gemini", ProviderClaude, gemini, false},
		{"claude_rejects_empty", ProviderClaude, "", false},
		{"gemini_field2_tink", ProviderGemini, gemini, true},
		{"gemini_field2_uuid", ProviderGemini, geminiUUID, true},
		{"gemini_rejects_bare_uuid", ProviderGemini, uuid, false},
		{"gemini_rejects_claude_cais", ProviderGemini, cais, false},
		{"gemini_rejects_claude_E", ProviderGemini, classic, false},
		{"gemini_rejects_gpt", ProviderGemini, gpt, false},
		{"gemini_rejects_junk", ProviderGemini, "not base64!!", false},
		{"gemini_rejects_empty", ProviderGemini, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := CompatibleSignatureForProvider(tc.provider, tc.sig)
			if ok != tc.want {
				t.Fatalf("CompatibleSignatureForProvider(%s, %.24q…) = %v, want %v", tc.provider, tc.sig, ok, tc.want)
			}
			if ok && got == "" {
				t.Fatalf("兼容签名不得返回空值")
			}
		})
	}

	// cache 前缀剥离：返回 provider 原生形态（CPA 归一）。
	got, ok := CompatibleSignatureForProvider(ProviderClaude, "ccmax#"+classic)
	if !ok || got != classic {
		t.Fatalf("cache 前缀未剥离: %q（want %q）", got, classic)
	}

	// 单记录判据（CPA）：经典信封的尾字段 3 使 Gemini 的 field-2 单记录检查失败；
	// 反向，Gemini 单记录信封没有 channel block 结构，Claude 检查也失败——两族
	// 互不误收（与 CPA TestGeminiEnvelopeNeverClaimsClaudeSignatures 同精神）。
	if _, ok := CompatibleSignatureForProvider(ProviderGemini, classic); ok {
		t.Fatalf("经典 Claude 信封被 Gemini 误收")
	}
	if _, ok := CompatibleSignatureForProvider(ProviderClaude, gemini); ok {
		t.Fatalf("Gemini field-2 信封被 Claude 误收")
	}

	// 长签名上限守卫（32MB）不 panic、直接拒绝（构造超长字符串成本受控：头 64KB）。
	long := strings.Repeat("A", 64<<10)
	if _, ok := CompatibleSignatureForProvider(ProviderClaude, long); ok {
		t.Fatalf("超长非信封字符串被误收")
	}
}

// TestProviderForSignature（W41）：归属判定 = 三个既有检查的短路组合（chat 方言
// reasoning_details 载体的 format 取值来源）。反证：把 ProviderForSignature 改成
// 恒返回 ProviderUnknown → 本测试红。
func TestProviderForSignature(t *testing.T) {
	classic := b64(claudeClassicPayload())
	gptPayload := make([]byte, 73)
	gptPayload[0] = 0x80
	gpt := "gAAAA" + b64(gptPayload)[5:]
	gemini := b64(geminiField2Payload([]byte{0x01, 0xde, 0xad, 0xbe, 0xef}))

	cases := []struct {
		name string
		sig  string
		want Provider
		ok   bool
	}{
		{"claude_classic", classic, ProviderClaude, true},
		{"gpt_fernet", gpt, ProviderGPT, true},
		{"gemini_field2", gemini, ProviderGemini, true},
		{"empty", "", ProviderUnknown, false},
		{"junk", "not base64!!", ProviderUnknown, false},
		{"unknown_envelope", b64([]byte{0x12, 0x02, 0xaa, 0xbb}), ProviderUnknown, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ProviderForSignature(tc.sig)
			if ok != tc.ok || (ok && got != tc.want) {
				t.Fatalf("ProviderForSignature(%.24q…) = (%s, %v), want (%s, %v)", tc.sig, got, ok, tc.want, tc.ok)
			}
		})
	}
}
