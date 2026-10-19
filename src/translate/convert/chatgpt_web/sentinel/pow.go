package sentinel

import (
	"bytes"
	"crypto/sha3"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// 本文件 = 两代 PoW 的冻结实现 + p token 组装。
//
// # 比较语义（两代不同——照抄参考实现的真实行为，不是伪码里的统一 ≤）
//
//	2025（proof_of_work.py:31-48）：`sha3_512(seed+enc)[:len(diffHex)] <= bytes.fromhex(diffHex)`。
//	  注意切片长度是 **hex 字符数**（= 目标字节数的两倍），Python bytes 比较又是
//	  「短序列是长序列的前缀时更小」——所以等价于：前 len(target) 字节 **严格小于**
//	  target（相等时左操作数更长 → 判大 → 不通过）。本包以 prefixLE2025 逐字复刻。
//	2026（web_client.py:139-151,153-181）：`hashHex[:len(diff)] <= diff`，
//	  hex **字符串**比较（等值即通过；难度长于 8 时按 Python 切片截断）。
//
// # 求解循环的槽位更新（两代参考实现一致）
//
//	每轮：槽 3 = 尝试次数；槽 9 = 2025 填 i>>1（**尝试数的一半**，不是耗时——
//	proof_of_work.py:38-41 的 dynamic_json_j）、2026 填真实耗时毫秒
//	（web_client.py:167-168；sdk.js 反混淆 `r[3]=o,r[9]=Math.round(performance.now()-t)`
//	同构）。编码后做哈希，命中即返回 base64 串（前缀由调用方拼）。

// sha3_512Sum 计算 sha3_512(seed || encoded)。
func sha3_512Sum(seed, encoded string) []byte {
	h := sha3.New512()
	h.Write([]byte(seed))
	h.Write([]byte(encoded))
	return h.Sum(nil)
}

// fnv1a32Avalanche 计算 2026 代哈希：32 位 FNV-1a + avalanche 收尾。
//
// 常量与位移逐字对齐三份独立实现（web_client.py:139-151 / chatgpt-creator
// internal/sentinel/fnv.go:6-19 / 官方 sdk.js 反混淆，见包注）。Go 的 uint32
// 溢出即 JS 的 `Math.imul(...) >>> 0`。
func fnv1a32Avalanche(s string) uint32 {
	h := uint32(2166136261)
	for i := range len(s) {
		h ^= uint32(s[i])
		h *= 16777619
	}
	h ^= h >> 16
	h *= 2246822507
	h ^= h >> 13
	h *= 3266489909
	h ^= h >> 16
	return h
}

// fnvAvalancheHex 返回 8 位小写 hex（参考实现 `f"{x:08x}"` / `toString(16).padStart(8,"0")`）。
func fnvAvalancheHex(s string) string { return fmt.Sprintf("%08x", fnv1a32Avalanche(s)) }

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// difficultyBytes2025 校验并解码 2025 代难度串（偶数长度 hex；Python
// bytes.fromhex 面。**差异记录**：Python fromhex 允许字节间空白，本包不允许）。
func difficultyBytes2025(difficulty string) ([]byte, error) {
	if difficulty == "" || len(difficulty)%2 != 0 {
		return nil, fmt.Errorf("%w: 2025 difficulty %q must be even-length hex", ErrDifficulty, difficulty)
	}
	for i := range len(difficulty) {
		if !isHexDigit(difficulty[i]) {
			return nil, fmt.Errorf("%w: 2025 difficulty %q must be even-length hex", ErrDifficulty, difficulty)
		}
	}
	return hex.DecodeString(difficulty)
}

// normalizeDifficulty2026 复刻 web_client.py:164-166：strip + lower + 全 [0-9a-f]。
func normalizeDifficulty2026(difficulty string) (string, error) {
	d := strings.ToLower(strings.TrimSpace(difficulty))
	if d == "" {
		return "", fmt.Errorf("%w: 2026 difficulty %q must match [0-9a-f]+", ErrDifficulty, difficulty)
	}
	for i := range len(d) {
		if d[i] < '0' || d[i] > 'f' || (d[i] > '9' && d[i] < 'a') {
			return "", fmt.Errorf("%w: 2026 difficulty %q must match [0-9a-f]+", ErrDifficulty, difficulty)
		}
	}
	return d, nil
}

// prefixLE2025 复刻 2025 比较（见文件头注）：digest[:hexLen] <= target。
func prefixLE2025(digest, target []byte, hexLen int) bool {
	h := digest
	if len(h) > hexLen {
		h = h[:hexLen]
	}
	return bytes.Compare(h, target) <= 0
}

// prefixLE2026 复刻 2026 比较：digestHex[:len(difficulty)] <= difficulty（字符串）。
func prefixLE2026(digestHex, difficulty string) bool {
	d := digestHex
	if len(difficulty) < len(d) {
		d = d[:len(difficulty)]
	}
	return d <= difficulty
}

// solve2025 执行 2025 代求解循环（proof_of_work.py:31-48 / chat2api 同构）。
//
// 静态分段是参考实现的性能手法（:33-35）：config 拆 [0:3] / [4:9] / [10:] 三段
// 各编码一次，循环里只拼 str(i) 与 str(i>>1)。分段拼装与整数组 json.dumps
// 逐字节等价（对任意 JSON 合法的槽位值成立）。
func solve2025(cfg Config, seed, difficulty string, maxAttempts int) (string, bool, error) {
	if len(cfg) != V2025.SlotCount() {
		return "", false, fmt.Errorf("%w: 2025 needs %d slots, got %d", ErrSlotCount, V2025.SlotCount(), len(cfg))
	}
	target, err := difficultyBytes2025(difficulty)
	if err != nil {
		return "", false, err
	}
	p1, err := encodeConfig(cfg[:3])
	if err != nil {
		return "", false, err
	}
	p2, err := encodeConfig(cfg[4:9])
	if err != nil {
		return "", false, err
	}
	p3, err := encodeConfig(cfg[10:])
	if err != nil {
		return "", false, err
	}
	// "[a,b,c]" → "[a,b,c,"；"[e,f,g,h,i]" → ",e,f,g,h,i,"；"[k,…,r]" → ",k,…,r]"
	head := append(append([]byte(nil), p1[:len(p1)-1]...), ',')
	mid := append([]byte{','}, p2[1:len(p2)-1]...)
	mid = append(mid, ',')
	tail := append([]byte{','}, p3[1:]...)

	h := sha3.New512()
	buf := make([]byte, 0, len(head)+len(mid)+len(tail)+24)
	digest := make([]byte, 0, h.Size())
	for i := range maxAttempts {
		buf = buf[:0]
		buf = append(buf, head...)
		buf = strconv.AppendInt(buf, int64(i), 10)
		buf = append(buf, mid...)
		buf = strconv.AppendInt(buf, int64(i>>1), 10)
		buf = append(buf, tail...)

		encoded := base64.StdEncoding.EncodeToString(buf)
		h.Reset()
		h.Write([]byte(seed))
		h.Write([]byte(encoded))
		digest = h.Sum(digest[:0])
		if prefixLE2025(digest, target, len(difficulty)) {
			return encoded, true, nil
		}
	}
	return "", false, nil
}

// solve2026 执行 2026 代求解循环（web_client.py:153-181 的 _proof_token）。
func solve2026(cfg Config, seed, difficulty string, clock func() time.Duration, maxAttempts int) (string, bool, error) {
	if len(cfg) != V2026.SlotCount() {
		return "", false, fmt.Errorf("%w: 2026 needs %d slots, got %d", ErrSlotCount, V2026.SlotCount(), len(cfg))
	}
	diff, err := normalizeDifficulty2026(difficulty)
	if err != nil {
		return "", false, err
	}
	candidate := append(Config(nil), cfg...)
	started := clock()
	for attempt := range maxAttempts {
		candidate[SlotAttempt] = attempt
		elapsed := clock() - started // 参考实现 (perf_counter()-started)*1000
		ms := elapsed.Seconds() * 1000
		if ms < 0 {
			ms = 0 // max(0.0, …)
		}
		candidate[SlotElapsed] = ms
		raw, err := encodeConfig(candidate)
		if err != nil {
			return "", false, err
		}
		// 参考实现先 _encode_sentinel_config（base64(json)），哈希输入是
		// seed + **base64 串**（web_client.py:169-170）。
		encoded := base64.StdEncoding.EncodeToString(raw)
		if prefixLE2026(fnvAvalancheHex(seed+encoded), diff) {
			return encoded, true, nil
		}
	}
	return "", false, nil
}

// LegacyFallbackToken 返回参考实现的伪解兜底串（**仅记录，不默认启用**——
// brief §Change ②；语义见 WithLegacyFallback）：
//
//	LegacyFallbackPrefix + base64('"'+seed+'"')
//
// 与 proof_of_work.py:46-48 / chat2api proofofWork.py:487-489 逐字节一致。
// 2026 侧 turb-gpt 变体载荷为 base64('["e"]')（core/sentinel.py:242-243），
// 官方 web bundle 无此路径（抛错）——两者均在包注/选项文档记录，未实现。
func LegacyFallbackToken(seed string) string {
	return LegacyFallbackPrefix + base64.StdEncoding.EncodeToString([]byte(`"`+seed+`"`))
}

// DecodeTokenPayload 解码 token 的 base64 载荷（去前缀、去 "~S" 后缀），返回
// JSON 字节。用于诊断/测试（求解与校验路径不解析 JSON）。
func DecodeTokenPayload(token string) ([]byte, error) {
	enc, err := splitToken(token)
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(enc)
}

// splitToken 拆出 token 的 base64 载荷串（保留原始 base64 文本——哈希输入用的
// 就是它，不是解码后的字节）。requirements/proof 两种前缀都接受；"~S" 后缀
// （2026 proof）在哈希输入之外，剥离。
func splitToken(token string) (string, error) {
	var rest string
	switch {
	case strings.HasPrefix(token, PrefixProof):
		rest = token[len(PrefixProof):]
	case strings.HasPrefix(token, PrefixRequirements):
		rest = token[len(PrefixRequirements):]
	default:
		return "", fmt.Errorf("%w: missing %q/%q prefix", ErrToken, PrefixRequirements, PrefixProof)
	}
	rest = strings.TrimSuffix(rest, Suffix2026)
	if rest == "" {
		return "", fmt.Errorf("%w: empty payload", ErrToken)
	}
	if _, err := base64.StdEncoding.DecodeString(rest); err != nil {
		return "", fmt.Errorf("%w: %v", ErrToken, err)
	}
	return rest, nil
}

// Verify 校验一个 token 是否满足 seed/difficulty（两代各自哈希与比较语义）。
//
// 服务端校验用的就是这个函数面（本包离线复刻）；**服务端实际是否接受未验证**
// （零真机红线，S4 授权后测）。
func Verify(v Version, token, seed, difficulty string) (bool, error) {
	enc, err := splitToken(token)
	if err != nil {
		return false, err
	}
	switch v {
	case V2025:
		target, err := difficultyBytes2025(difficulty)
		if err != nil {
			return false, err
		}
		digest := sha3_512Sum(seed, enc)
		return prefixLE2025(digest, target, len(difficulty)), nil
	case V2026:
		diff, err := normalizeDifficulty2026(difficulty)
		if err != nil {
			return false, err
		}
		return prefixLE2026(fnvAvalancheHex(seed+enc), diff), nil
	}
	return false, ErrUnknownVersion
}

// slotFloat 把槽位值取为 float64（2026 requirements 的槽 13 → 槽 9 面；
// Python `max(0.0, float(config[13] or 0.0))`）。nil 视为 0.0。
func slotFloat(v any) (float64, error) {
	switch x := v.(type) {
	case nil:
		return 0, nil
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int:
		return float64(x), nil
	case int8:
		return float64(x), nil
	case int16:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case uint:
		return float64(x), nil
	case uint8:
		return float64(x), nil
	case uint16:
		return float64(x), nil
	case uint32:
		return float64(x), nil
	case uint64:
		return float64(x), nil
	case json.Number:
		f, err := strconv.ParseFloat(x.String(), 64)
		if err != nil {
			return 0, fmt.Errorf("%w: slot value %q not numeric", ErrSlotType, x.String())
		}
		return f, nil
	default:
		return 0, fmt.Errorf("%w: slot value %T not numeric", ErrSlotType, v)
	}
}

// ---------------------------------------------------------------------------
// Builder 实现
// ---------------------------------------------------------------------------

type builder2025 struct{ opts options }

func (b *builder2025) Version() Version { return V2025 }

// RequirementsToken 2025 代：对 config 做一次 diff="0fffff" 的自求解
// （proof_of_work.py:50-53 的 get_requirements_token → generate_answer）。
// 自种子来自 WithSeedSource（默认 = Python `format(random.random())` 语义）。
func (b *builder2025) RequirementsToken(cfg Config) (string, error) {
	seed := b.opts.seedSrc()
	enc, ok, err := solve2025(cfg, seed, RequirementsDifficulty2025, b.opts.maxAttempts)
	if err != nil {
		return "", err
	}
	if !ok {
		if b.opts.legacyFallback {
			// 参考实现 get_requirements_token = 'gAAAAAC' + 兜底串（未解出时）。
			return PrefixRequirements + LegacyFallbackToken(seed), nil
		}
		return "", fmt.Errorf("%w: 2025 requirements seed=%q difficulty=%s", ErrUnsolved, seed, RequirementsDifficulty2025)
	}
	return PrefixRequirements + enc, nil
}

// ProofToken 2025 代：sha3_512 求解，返回 "gAAAAAB" + base64（无后缀）。
func (b *builder2025) ProofToken(cfg Config, seed, difficulty string) (string, error) {
	enc, ok, err := solve2025(cfg, seed, difficulty, b.opts.maxAttempts)
	if err != nil {
		return "", err
	}
	if !ok {
		if b.opts.legacyFallback {
			return PrefixProof + LegacyFallbackToken(seed), nil
		}
		return "", fmt.Errorf("%w: 2025 proof seed=%q difficulty=%s", ErrUnsolved, seed, difficulty)
	}
	return PrefixProof + enc, nil
}

type builder2026 struct{ opts options }

func (b *builder2026) Version() Version { return V2026 }

// RequirementsToken 2026 代：静态编码（web_client.py:130-137 的 _prepare_token）——
// prepared[3] = 1；prepared[9] = max(0, config[13])；返回 "gAAAAAC" + base64。
// 不求解、不用时钟（与 2025 代语义不同，接口相同）。
func (b *builder2026) RequirementsToken(cfg Config) (string, error) {
	if len(cfg) != V2026.SlotCount() {
		return "", fmt.Errorf("%w: 2026 needs %d slots, got %d", ErrSlotCount, V2026.SlotCount(), len(cfg))
	}
	prepared := append(Config(nil), cfg...)
	prepared[SlotAttempt] = 1
	ms, err := slotFloat(cfg[13])
	if err != nil {
		return "", err
	}
	if ms < 0 {
		ms = 0
	}
	prepared[SlotElapsed] = ms
	enc, err := encodeConfig(prepared)
	if err != nil {
		return "", err
	}
	return PrefixRequirements + base64.StdEncoding.EncodeToString(enc), nil
}

// ProofToken 2026 代：FNV-1a32+avalanche 求解，返回 "gAAAAAB" + base64 + "~S"。
// 耗时槽（9）来自 WithClock（默认进程单调时钟）。
func (b *builder2026) ProofToken(cfg Config, seed, difficulty string) (string, error) {
	enc, ok, err := solve2026(cfg, seed, difficulty, b.opts.clock, b.opts.maxAttempts)
	if err != nil {
		return "", err
	}
	if !ok {
		if b.opts.legacyFallback {
			return PrefixProof + LegacyFallbackToken(seed), nil
		}
		return "", fmt.Errorf("%w: 2026 proof seed=%q difficulty=%s", ErrUnsolved, seed, difficulty)
	}
	return PrefixProof + enc + Suffix2026, nil
}

// 编译期接口断言。
var (
	_ Builder = (*builder2025)(nil)
	_ Builder = (*builder2026)(nil)
)
