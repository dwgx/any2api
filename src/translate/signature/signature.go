// Package signature 签名兼容层：判定一个思考块的**签名**能否被目标 provider
// 安全重放（最小移植自 CPA `internal/signature/provider_compatibility.go` +
// `gpt_validation.go` 的形态判定；依据 anthropic-conversion-table §7.1 回传轮行
// 与 §「签名格式」行）。
//
// 已实现目标（W5-19 扩面）：**GPT**（OpenAI Chat 请求面的
// thinking → `reasoning_content` 映射，W5-18）、**Claude** 与 **Gemini**
// （请求面回传轮的签名重放判定——claude 出口的 thinking 块 / gemini 出口的
// thought part；表 §7.1「Responses reasoning → Anthropic」行 + GW 陷阱 2）。
// Kimi/Grok/SWE 目标在本仓没有消费者，**不实现**：宁可显式返回 false（调用方按表
// 的 fallback 整块丢弃），也不假装判定一个我们没核实的签名族。
//
// 纪律：本包纯函数、零依赖、绝不解密/改写签名（只做形态判定与返回原值——
// 签名必须原样回传，AM §字段对照）。
package signature

import (
	"encoding/base64"
	"strings"
)

// Provider 签名目标 provider 族（CPA SignatureProvider 的最小集合）。
type Provider string

const (
	// ProviderUnknown 未知目标：一切签名都不兼容（调用方走 fallback）。
	ProviderUnknown Provider = "unknown"
	// ProviderGPT GPT/Codex 族（OpenAI Chat `reasoning_content` 的签名承载方）。
	ProviderGPT Provider = "gpt"
	// ProviderClaude Anthropic 族（thinking.signature 的签发方；E/R/C/Q 信封）。
	ProviderClaude Provider = "claude"
	// ProviderGemini Google Gemini 族（thoughtSignature 的签发方；field-2 信封）。
	ProviderGemini Provider = "gemini"
)

// maxGPTSignatureLen GPT 签名长度上限（CPA `MaxGPTReasoningSignatureLen`：
// 32MB——多 KB 的 Fernet 密文是常态，上限只为拦住病态输入）。
const maxGPTSignatureLen = 32 * 1024 * 1024

// gptSignaturePrefix GPT/Codex reasoning 密文的字面前缀（Fernet 头部的 base64
// 形态；CPA `InspectGPTReasoningSignature`：「最便宜且排除一切其它 provider 信封」
// 的判据）。
const gptSignaturePrefix = "gAAAA"

// gptSignatureVersionByte Fernet 载荷首字节（CPA：版本 0x80）。
const gptSignatureVersionByte = 0x80

// CompatibleSignatureForProvider 返回目标 provider 可重放的 provider 原生签名。
// 第二个返回值为 false = 不兼容（调用方按表 fallback 整块丢弃，绝不改内容后
// 硬塞）。
//
// GPT 目标的判定（CPA `InspectGPTReasoningSignature` 的形态检查，逐条移植）：
//
//  1. 非空、长度 ≤ 32MB、字面前缀 `gAAAA`；
//  2. base64url 可解码（容忍带/不带 padding 两形态）；
//  3. 解码载荷 ≥ 73 字节、首字节 = 0x80（版本）；
//  4. 密文段 = len - 1（版本）- 8（时间戳）- 16（IV）- 32（HMAC）为正且为
//     AES 块长整数倍。
//
// 这只是**传输形态**检查（不证明可解密），与 CPA 的注释同口径。
//
// Claude 目标（CPA `claude_validation.go` 的紧凑移植）：E/R/C/Q 信封 + 顶层
// protobuf 结构（见 compatibleClaudeSignature）。
//
// Gemini 目标（CPA `gemini_validation.go` 的紧凑移植）：field-2 单记录信封 +
// Tink 载荷首字节（见 compatibleGeminiSignature）。
func CompatibleSignatureForProvider(provider Provider, rawSignature string) (string, bool) {
	switch provider {
	case ProviderGPT:
		return compatibleGPTSignature(rawSignature)
	case ProviderClaude:
		return compatibleClaudeSignature(rawSignature)
	case ProviderGemini:
		return compatibleGeminiSignature(rawSignature)
	default:
		return "", false
	}
}

// ProviderForSignature 形态判定签名的签发 provider 族（W41：chat 方言
// reasoning_details 载体的 format 取值来源）。判定 = 依次跑与
// CompatibleSignatureForProvider 完全相同的三个检查（GPT → Claude → Gemini；
// 三族信封互斥——gAAAA 前缀 / E·R·C·Q 信封 / field-2 单记录，sweep4 测试
// 显式锁死互不误收），返回首个命中者。第二个返回值 false = 三个已知族都不认
// （调用方按红线丢弃该签名，绝不写进 wire）。
func ProviderForSignature(rawSignature string) (Provider, bool) {
	if strings.TrimSpace(rawSignature) == "" {
		return ProviderUnknown, false
	}
	if _, ok := compatibleGPTSignature(rawSignature); ok {
		return ProviderGPT, true
	}
	if _, ok := compatibleClaudeSignature(rawSignature); ok {
		return ProviderClaude, true
	}
	if _, ok := compatibleGeminiSignature(rawSignature); ok {
		return ProviderGemini, true
	}
	return ProviderUnknown, false
}

func compatibleGPTSignature(rawSignature string) (string, bool) {
	sig := strings.TrimSpace(rawSignature)
	if sig == "" || len(sig) > maxGPTSignatureLen {
		return "", false
	}
	if !strings.HasPrefix(sig, gptSignaturePrefix) {
		return "", false
	}
	decoded, ok := decodeBase64URL(sig)
	if !ok {
		return "", false
	}
	if len(decoded) < 73 {
		return "", false
	}
	if decoded[0] != gptSignatureVersionByte {
		return "", false
	}
	ciphertextLen := len(decoded) - 1 - 8 - 16 - 32
	if ciphertextLen <= 0 || ciphertextLen%16 != 0 {
		return "", false
	}
	return sig, true
}

// decodeBase64URL base64url 解码（先无 padding 再带 padding；CPA 同款两形态）。
func decodeBase64URL(sig string) ([]byte, bool) {
	if decoded, err := base64.RawURLEncoding.DecodeString(sig); err == nil {
		return decoded, true
	}
	if decoded, err := base64.URLEncoding.DecodeString(sig); err == nil {
		return decoded, true
	}
	return nil, false
}
