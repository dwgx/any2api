package signature

import (
	"encoding/base64"
	"encoding/binary"
	"strings"
)

// 本文件：Claude / Gemini 目标的签名形态判定（W5-19 条目 3；CPA
// `internal/signature/{claude_validation,gemini_validation}.go` 的**紧凑移植**）。
//
// 与 GPT 判定（signature.go）同口径：只做**传输信封 + 顶层结构**检查，绝不
// 解密、绝不改写；判定通过 = 目标 provider 的线格式能接受这个签名（不证明其
// 有效性）。CPA 的完整 schema 面（routing_class / schema_features / envelope
// version 等维度）不移植——本仓没有消费方需要那些维度，判定面越小越好核实。

// maxEnvelopeSignatureLen 信封类签名的长度上限（与 GPT 同口径：32MB 只拦病态
// 输入；多 KB 的密文是常态）。
const maxEnvelopeSignatureLen = 32 * 1024 * 1024

// claude 信封标记字节（CPA 注释：base64 首字符 → 解码首字节）。
const (
	claudeCAISMarker     = 0x08 // 'C'：CAIS 信封（新 Claude Code 模型）
	claudeClassicMarker  = 0x12 // 'E'：经典单层信封
	claudeDoubleMarker   = 0x45 // 'R'：双层（内层是 'E' 开头的 base64 文本）
	claudeCAQSWrapper    = 0x43 // 'Q'：Antigravity 双层 CAQS（内层是 'C' 开头的 base64 文本）
	geminiTinkPrefixByte = 0x01 // field-2 载荷（Tink primitive 输出的前缀类型字节）
)

// compatibleClaudeSignature 判定 Claude 目标可重放的签名（CPA
// `NormalizeClaudeThinkingSignature` / `HasDecodableClaudeThinkingSignature` 的
// 结构面移植）：
//
//   - 去 cache 前缀（`#` 后段——CPA `stripClaudeSignaturePrefix`）；
//   - 'E'：base64(std) 解码 → 首字节 0x12 → 顶层结构（field 2 container →
//     container field 1 channel block → channel block field 1 varint channel_id）；
//   - 'R'：解码 → 载荷是 ASCII base64 且以 'E' 开头 → 再解码 → E 检查；
//   - 'C'：解码 → 首字节 0x08 → 同一顶层结构（CAIS 与经典信封只差顶层字段 1
//     的版本 varint）；
//   - 'Q'：解码 → 载荷是 ASCII base64 且以 'C' 开头 → 再解码 → CAIS 检查。
//
// 返回 (去 cache 前缀后的原签名, true)——签名必须原样回传（不改写）。
func compatibleClaudeSignature(rawSignature string) (string, bool) {
	sig := strings.TrimSpace(rawSignature)
	if sig == "" || len(sig) > maxEnvelopeSignatureLen {
		return "", false
	}
	if i := strings.IndexByte(sig, '#'); i >= 0 {
		sig = strings.TrimSpace(sig[i+1:])
	}
	if sig == "" {
		return "", false
	}
	switch sig[0] {
	case 'E':
		if claudeClassicEnvelopeOK(sig) {
			return sig, true
		}
	case 'C':
		if claudeCAISEnvelopeOK(sig) {
			return sig, true
		}
	case 'R':
		if inner, ok := decodeBase64Std(sig); ok && len(inner) > 0 && inner[0] == 'E' && claudeClassicEnvelopeOK(string(inner)) {
			return sig, true
		}
	case 'Q':
		if inner, ok := decodeBase64Std(sig); ok && len(inner) > 0 && inner[0] == 'C' && claudeCAISEnvelopeOK(string(inner)) {
			return sig, true
		}
	}
	return "", false
}

// claudeClassicEnvelopeOK 'E' 信封：base64 解码 → 首字节 0x12 → 共享顶层结构。
func claudeClassicEnvelopeOK(sig string) bool {
	decoded, ok := decodeBase64Std(sig)
	return ok && claudeEnvelopeStructureOK(decoded, claudeClassicMarker)
}

// claudeCAISEnvelopeOK 'C' 信封：base64 解码 → 首字节 0x08 → 共享顶层结构。
func claudeCAISEnvelopeOK(sig string) bool {
	decoded, ok := decodeBase64Std(sig)
	return ok && claudeEnvelopeStructureOK(decoded, claudeCAISMarker)
}

// claudeEnvelopeStructureOK 经典/CAIS 共享的顶层结构检查（CPA 注释：
// 「envelope version varint in top-level field 1 is the ONLY structural
// difference」——两信封的容器层完全同构）：
//
//	top-level  |- field 2 (bytes): container [required]
//	           `- field 3 (varint): trailer [optional]
//	container  `- field 1 (bytes): channel block [required]
//	channel    |- field 1 (varint): channel_id [required]
//	           `- 其余字段（infra/version/ECDSA/model_text/…）不校验
func claudeEnvelopeStructureOK(decoded []byte, marker byte) bool {
	if len(decoded) == 0 || decoded[0] != marker {
		return false
	}
	container, ok := protoBytesField(decoded, 2)
	if !ok || len(container) == 0 {
		return false
	}
	channel, ok := protoBytesField(container, 1)
	if !ok || len(channel) == 0 {
		return false
	}
	return protoHasVarintField(channel, 1)
}

// compatibleGeminiSignature 判定 Gemini 目标可重放的签名（CPA
// `isRecognizedGeminiProviderSignature` 的紧凑移植）：
//
//   - base64(std) 可解码、非空、≤ 32MB；
//   - 不是 Claude CAIS 信封（CPA 显式排除——CAIS 是 Claude 面）；
//   - field-2 单记录信封：顶层 field 2（bytes）恰好覆盖整个载荷 → container，
//     container 的 field 1（bytes）恰好覆盖整个 container → 不透明载荷；
//   - 载荷形态（三者其一）：Tink primitive（首字节 0x01）/ ASCII UUID（36 字符）/
//     服务端工具调用包装（protobuf 内含 0x01 开头的 Tink 载荷）。
//
// 裸 base64 UUID 之外的未知信封一律拒绝（CPA：unknown envelope 不重放）。
func compatibleGeminiSignature(rawSignature string) (string, bool) {
	sig := strings.TrimSpace(rawSignature)
	if sig == "" || len(sig) > maxEnvelopeSignatureLen {
		return "", false
	}
	decoded, ok := decodeBase64Std(sig)
	if !ok || len(decoded) == 0 {
		return "", false
	}
	if claudeEnvelopeStructureOK(decoded, claudeCAISMarker) {
		return "", false // Claude CAIS 信封不是 Gemini 签名。
	}
	value, ok := geminiField2Envelope(decoded)
	if !ok || len(value) == 0 {
		return "", false
	}
	if value[0] != geminiTinkPrefixByte && !isASCIIUUIDBytes(value) && !geminiToolInvocationPayload(value) {
		return "", false
	}
	return sig, true
}

// geminiField2Envelope field-2 单记录信封：顶层 field 2（bytes）恰好覆盖整个
// 载荷；container 的 field 1（bytes）恰好覆盖整个 container。返回不透明载荷。
func geminiField2Envelope(decoded []byte) ([]byte, bool) {
	container, ok := singleProtoBytesField(decoded, 2)
	if !ok {
		return nil, false
	}
	return singleProtoBytesField(container, 1)
}

// singleProtoBytesField 载荷恰好是一个 wire-type-2 字段（field 号匹配、长度覆盖
// 到末尾、非空）时返回其内容——CPA「single record」判据。
func singleProtoBytesField(b []byte, field int) ([]byte, bool) {
	tag, n := binary.Uvarint(b)
	if n <= 0 || int(tag>>3) != field || tag&7 != 2 {
		return nil, false
	}
	rest := b[n:]
	l, n2 := binary.Uvarint(rest)
	if n2 <= 0 {
		return nil, false
	}
	payload := rest[n2:]
	if uint64(len(payload)) != l || len(payload) == 0 {
		return nil, false
	}
	return payload, true
}

// geminiToolInvocationPayload 服务端工具调用包装：protobuf 内至少有一个
// wire-type-2 字段，其内容以 Tink 前缀字节（0x01）开头（CPA
// `isLikelyGeminiToolInvocationPayload` 的紧凑移植）。
func geminiToolInvocationPayload(value []byte) bool {
	found := false
	ok := protoWalk(value, func(_ int, wire int, payload []byte) bool {
		if wire == 2 && len(payload) > 0 && payload[0] == geminiTinkPrefixByte {
			found = true
		}
		return true
	})
	return ok && found
}

// isASCIIUUIDBytes 36 字符的 ASCII UUID（8-4-4-4-12；CPA `isASCIIUUIDBytes`）。
func isASCIIUUIDBytes(b []byte) bool {
	if len(b) != 36 {
		return false
	}
	for i, c := range b {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				return false
			}
		}
	}
	return true
}

// decodeBase64Std base64 标准字母表解码（先带 padding 再无 padding——CPA
// `decodeGeminiThoughtSignature` 同款两形态）。
func decodeBase64Std(s string) ([]byte, bool) {
	if decoded, err := base64.StdEncoding.DecodeString(s); err == nil {
		return decoded, true
	}
	if decoded, err := base64.RawStdEncoding.DecodeString(s); err == nil {
		return decoded, true
	}
	return nil, false
}

// protoWalk 遍历 protobuf 顶层字段（wire 0 varint / 1 fixed64 / 2 bytes /
// 5 fixed32；3/4 组与畸形/截断 → false）。回调对每个字段调用；回调返回 false
// 提前停止（本函数随之返回 false）。
func protoWalk(b []byte, fn func(num, wire int, payload []byte) bool) bool {
	i := 0
	for i < len(b) {
		tag, n := binary.Uvarint(b[i:])
		if n <= 0 {
			return false
		}
		i += n
		num, wire := int(tag>>3), int(tag&7)
		if num == 0 {
			return false
		}
		switch wire {
		case 0:
			_, n := binary.Uvarint(b[i:])
			if n <= 0 {
				return false
			}
			if !fn(num, wire, b[i:i+n]) {
				return false
			}
			i += n
		case 1:
			if i+8 > len(b) {
				return false
			}
			if !fn(num, wire, b[i:i+8]) {
				return false
			}
			i += 8
		case 2:
			l, n := binary.Uvarint(b[i:])
			if n <= 0 || l > uint64(len(b)-i-n) {
				return false
			}
			i += n
			if !fn(num, wire, b[i:i+int(l)]) {
				return false
			}
			i += int(l)
		case 5:
			if i+4 > len(b) {
				return false
			}
			if !fn(num, wire, b[i:i+4]) {
				return false
			}
			i += 4
		default:
			return false
		}
	}
	return true
}

// protoBytesField 取首个 wire-type-2 字段的载荷；结构不干净（畸形/截断）→ false。
func protoBytesField(b []byte, field int) ([]byte, bool) {
	var out []byte
	found := false
	if !protoWalk(b, func(num, wire int, payload []byte) bool {
		if num == field && wire == 2 && !found {
			out, found = payload, true
		}
		return true
	}) {
		return nil, false
	}
	return out, found
}

// protoHasVarintField 是否存在指定字段号的 wire-type-0 字段。
func protoHasVarintField(b []byte, field int) bool {
	found := false
	if !protoWalk(b, func(num, wire int, _ []byte) bool {
		if num == field && wire == 0 {
			found = true
		}
		return true
	}) {
		return false
	}
	return found
}
