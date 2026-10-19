// W5-18 条目 3：签名兼容层（CompatibleSignatureForProvider）单元测试。
//
// 依据：CPA `internal/signature/gpt_validation.go` 的 InspectGPTReasoningSignature
// 形态判定（前缀 gAAAA + base64url + 解码 ≥73 字节 + 首字节 0x80 + 密文段为正且
// 为 AES 块长整数倍）；表 anthropic-conversion-table §7.1「签名格式」行。
package signature

import (
	"encoding/base64"
	"strings"
	"testing"
)

// gptPayload n 字节 GPT 形态载荷：首字节 0x80（版本）+ 递增填充。前 4 字节保持
// 0x80,0,0,0 —— Fernet 头部时间戳高位为 0 的形态，其 base64url 首 5 字符恒为
// `gAAAA`（前缀判据的构造性来源）。
func gptPayload(n int) []byte {
	p := make([]byte, n)
	p[0] = gptSignatureVersionByte
	for i := 4; i < n; i++ {
		p[i] = byte(i)
	}
	return p
}

func TestCompatibleSignatureForProvider(t *testing.T) {
	valid73 := base64.RawURLEncoding.EncodeToString(gptPayload(73))
	valid89 := base64.RawURLEncoding.EncodeToString(gptPayload(89)) // 密文段 32 = 2×16
	// 前缀锁定：GPT 形态载荷的 base64url 首 5 字符必然是 gAAAA（0x80 的头部形态）。
	if !strings.HasPrefix(valid73, gptSignaturePrefix) {
		t.Fatalf("样本前缀 = %q, want %q 前缀", valid73[:5], gptSignaturePrefix)
	}

	cases := []struct {
		name string
		sig  string
		want bool
	}{
		{"valid_73_bytes", valid73, true},
		{"valid_89_bytes", valid89, true},
		{"valid_with_padding", base64.URLEncoding.EncodeToString(gptPayload(73)), true},
		{"valid_trimmed", "  " + valid73 + "\n", true},
		{"empty", "", false},
		{"claude_ish_prefix", "EgAAAAAAAAAAAAAAAAAA", false},
		{"wrong_prefix", "QAAAAA" + valid73[5:], false},
		{"not_base64", "gAAAA!!!!not-base64", false},
		{"prefix_only", "gAAAA", false},
		{"too_short", base64.RawURLEncoding.EncodeToString(gptPayload(40)), false},
		{"ciphertext_not_block_multiple", base64.RawURLEncoding.EncodeToString(gptPayload(74)), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := CompatibleSignatureForProvider(ProviderGPT, tc.sig)
			if ok != tc.want {
				t.Fatalf("CompatibleSignatureForProvider(GPT, %q) = %q,%v, want ok=%v", tc.sig, got, ok, tc.want)
			}
			if ok && got != strings.TrimSpace(tc.sig) {
				t.Fatalf("返回签名必须原样（去首尾空白）: %q", got)
			}
		})
	}

	// 未实现的目标 provider 一律不兼容（本仓只做 GPT 目标——绝不假装判定）。
	if _, ok := CompatibleSignatureForProvider(ProviderUnknown, valid73); ok {
		t.Fatalf("unknown provider 必须不兼容")
	}
	if _, ok := CompatibleSignatureForProvider(Provider("claude"), valid73); ok {
		t.Fatalf("未实现的 provider 必须不兼容（fallback = 整块丢弃）")
	}
}
