package sentinel

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// scriptedClock 是 2026 代求解的确定性时钟：第 n 次调用返回 n*step
// （对应向量脚本化的 perf_counter：t_i = i*step 秒 → 耗时 = i*step*1000 ms）。
type scriptedClock struct {
	step time.Duration
	n    int
}

func (c *scriptedClock) now() time.Duration {
	d := time.Duration(c.n) * c.step
	c.n++
	return d
}

// TestProofToken2025Vectors 2025 代求解与参考实现（openai-sentinel
// proof_of_work.py generate_answer）**逐字节一致**；Verify 通过。
func TestProofToken2025Vectors(t *testing.T) {
	v := loadVectors(t)
	b := New2025()
	for i, vec := range v.Tokens2025 {
		tok, err := b.ProofToken(vec.Config, vec.Seed, vec.Difficulty)
		if err != nil {
			t.Fatalf("vector %d: ProofToken: %v", i, err)
		}
		if tok != vec.Expected {
			t.Errorf("vector %d: token mismatch\n got %s\nwant %s", i, tok, vec.Expected)
		}
		ok, err := Verify(V2025, tok, vec.Seed, vec.Difficulty)
		if err != nil || ok != vec.Verify {
			t.Errorf("vector %d: Verify = %v, %v; want %v", i, ok, err, vec.Verify)
		}
	}
}

// TestProofToken2026Vectors 2026 代求解与参考实现（web_client.py _proof_token）
// 逐字节一致（脚本化时钟下确定性）；后缀 "~S"、Verify 通过。
func TestProofToken2026Vectors(t *testing.T) {
	v := loadVectors(t)
	for i, vec := range v.Tokens2026 {
		clk := &scriptedClock{step: time.Duration(vec.ClockStepS * float64(time.Second))}
		b := New2026(WithClock(clk.now))
		tok, err := b.ProofToken(vec.Config, vec.Seed, vec.Difficulty)
		if err != nil {
			t.Fatalf("vector %d: ProofToken: %v", i, err)
		}
		if tok != vec.Expected {
			t.Errorf("vector %d: token mismatch\n got %s\nwant %s", i, tok, vec.Expected)
		}
		if !strings.HasSuffix(tok, Suffix2026) {
			t.Errorf("vector %d: token lacks %q suffix", i, Suffix2026)
		}
		// 尝试次数与向量一致（脚本时钟第 n 次调用的语义钉死）。
		if got := clk.n; got != vec.Attempts+1 {
			t.Errorf("vector %d: clock calls = %d, want attempts+1 = %d", i, got, vec.Attempts+1)
		}
		ok, err := Verify(V2026, tok, vec.Seed, vec.Difficulty)
		if err != nil || ok != vec.Verify {
			t.Errorf("vector %d: Verify = %v, %v; want %v", i, ok, err, vec.Verify)
		}
	}
}

// TestRequirements2025Vector 2025 代 requirements token == 参考实现
// get_requirements_token（自种子注入后确定性）。
func TestRequirements2025Vector(t *testing.T) {
	v := loadVectors(t)
	for i, vec := range v.Requirements2025 {
		b := New2025(WithSeedSource(func() string { return vec.Seed }))
		tok, err := b.RequirementsToken(vec.Config)
		if err != nil {
			t.Fatalf("vector %d: RequirementsToken: %v", i, err)
		}
		if tok != vec.Expected {
			t.Errorf("vector %d: requirements token mismatch\n got %s\nwant %s", i, tok, vec.Expected)
		}
		// 参考实现的 requirements token 本身就是 diff="0fffff" 的解。
		ok, err := Verify(V2025, tok, vec.Seed, RequirementsDifficulty2025)
		if err != nil || ok != vec.Verify {
			t.Errorf("vector %d: Verify = %v, %v; want %v", i, ok, err, vec.Verify)
		}
	}
}

// TestRequirements2026Vector 2026 代 requirements token == 参考实现
// _prepare_token（静态编码：槽 3=1、槽 9=槽 13；无 PoW）。
func TestRequirements2026Vector(t *testing.T) {
	v := loadVectors(t)
	for i, vec := range v.Requirements2026 {
		b := New2026()
		tok, err := b.RequirementsToken(vec.Config)
		if err != nil {
			t.Fatalf("vector %d: RequirementsToken: %v", i, err)
		}
		if tok != vec.Expected {
			t.Errorf("vector %d: requirements token mismatch\n got %s\nwant %s", i, tok, vec.Expected)
		}
		// 槽位语义：解出的 JSON 里槽 3 == 1，槽 9 == 槽 13 的值。
		raw, err := DecodeTokenPayload(tok)
		if err != nil {
			t.Fatalf("vector %d: decode: %v", i, err)
		}
		var slots []any
		if err := json.Unmarshal(raw, &slots); err != nil {
			t.Fatalf("vector %d: payload not JSON: %v", i, err)
		}
		if len(slots) != 25 {
			t.Fatalf("vector %d: payload slots = %d, want 25", i, len(slots))
		}
		if n, ok := slots[SlotAttempt].(float64); !ok || n != 1 {
			t.Errorf("vector %d: slot 3 = %v, want 1", i, slots[SlotAttempt])
		}
		if slots[SlotElapsed] != slots[13] {
			t.Errorf("vector %d: slot 9 = %v, want slot 13 = %v", i, slots[SlotElapsed], slots[13])
		}
	}
}

// TestVerifyNegatives 反证（brief §Change ④）：改一字节 seed → 解不通过；
// 改一字节载荷 → 解不通过。两个变异都由参考实现（Python）预验证为 false。
func TestVerifyNegatives(t *testing.T) {
	v := loadVectors(t)
	for _, tc := range []struct {
		name    string
		version Version
		vectors []tokenVector
	}{
		{"2025", V2025, v.Tokens2025},
		{"2026", V2026, v.Tokens2026},
	} {
		for i, vec := range tc.vectors {
			ok, err := Verify(tc.version, vec.Expected, vec.Seed, vec.Difficulty)
			if err != nil || !ok {
				t.Fatalf("%s vector %d: baseline Verify = %v, %v; want true", tc.name, i, ok, err)
			}
			if vec.SeedMutated == vec.Seed {
				t.Fatalf("%s vector %d: seed mutation not recorded", tc.name, i)
			}
			ok, err = Verify(tc.version, vec.Expected, vec.SeedMutated, vec.Difficulty)
			if err != nil {
				t.Fatalf("%s vector %d: mutated-seed Verify error: %v", tc.name, i, err)
			}
			if ok != vec.VerifySeedMutated || ok {
				t.Errorf("%s vector %d: mutated seed verified (%v), want false", tc.name, i, ok)
			}
			ok, err = Verify(tc.version, vec.PayloadMutated, vec.Seed, vec.Difficulty)
			if err != nil {
				t.Fatalf("%s vector %d: mutated-payload Verify error: %v", tc.name, i, err)
			}
			if ok != vec.VerifyPayloadMutated || ok {
				t.Errorf("%s vector %d: mutated payload verified (%v), want false", tc.name, i, ok)
			}
		}
	}
}

// TestUnsolvedAndLegacyFallback 求解失败语义：默认 ErrUnsolved；显式
// WithLegacyFallback 才返回参考伪解串（且该串 Verify 不通过）。
func TestUnsolvedAndLegacyFallback(t *testing.T) {
	v := loadVectors(t)
	cfg2025 := v.Tokens2025[0].Config
	cfg2026 := v.Tokens2026[0].Config

	// 2025 的 "000000" 在严格 < 语义下**不可解**（h[:3] 永远不可能 < 0x000000）。
	b := New2025(WithMaxAttempts(8))
	_, err := b.ProofToken(cfg2025, "0.5", "000000")
	if !errors.Is(err, ErrUnsolved) {
		t.Fatalf("2025 unsolved err = %v, want ErrUnsolved", err)
	}

	// 2026：maxAttempts=1 + 极难难度（首个尝试命中概率 ~1/16^6；固定输入
	// 下确定性）。
	b26 := New2026(WithMaxAttempts(1), WithClock(func() time.Duration { return 0 }))
	_, err = b26.ProofToken(cfg2026, "0.5", "0000000000000000")
	if !errors.Is(err, ErrUnsolved) {
		t.Fatalf("2026 unsolved err = %v, want ErrUnsolved", err)
	}

	// 伪解兜底（仅记录，默认关闭）：开启后形态 = 前缀 + 参考伪解串。
	seed := "0.123456789"
	want := PrefixProof + LegacyFallbackToken(seed)
	b25fb := New2025(WithMaxAttempts(8), WithLegacyFallback(true))
	got, err := b25fb.ProofToken(cfg2025, seed, "000000")
	if err != nil {
		t.Fatalf("legacy fallback: %v", err)
	}
	if got != want {
		t.Errorf("legacy fallback token = %s, want %s", got, want)
	}
	// 伪解串**连 base64 都不合法**（前缀串与载荷直接拼接、含内嵌 padding）——
	// Verify 报 ErrToken；这正是「服务端必然拒绝」的形态（turb-gpt 注释原文）。
	if _, err := Verify(V2025, got, seed, "000000"); !errors.Is(err, ErrToken) {
		t.Errorf("verify fallback: err = %v, want ErrToken", err)
	}

	// 2026 同样面（参考实现 2026 侧无兜底路径——本包按同一选项提供，文档已注）。
	b26fb := New2026(WithMaxAttempts(1), WithClock(func() time.Duration { return 0 }), WithLegacyFallback(true))
	got26, err := b26fb.ProofToken(cfg2026, seed, "0000000000000000")
	if err != nil {
		t.Fatalf("2026 legacy fallback: %v", err)
	}
	if got26 != want {
		t.Errorf("2026 legacy fallback token = %s, want %s", got26, want)
	}
}

// TestLegacyFallbackTokenFormula 伪解串构造式 == 参考实现
// proof_of_work.py:46-48：prefix + base64('"'+seed+'"')。
func TestLegacyFallbackTokenFormula(t *testing.T) {
	seed := "0.5"
	want := LegacyFallbackPrefix + base64.StdEncoding.EncodeToString([]byte(`"0.5"`))
	if got := LegacyFallbackToken(seed); got != want {
		t.Errorf("LegacyFallbackToken = %s, want %s", got, want)
	}
	if !strings.HasPrefix(LegacyFallbackToken(seed), "wQ8Lk5FbGpA2NcR9dShT6gYjU7VxZ4D") {
		t.Error("fallback prefix drifted from the reference constant")
	}
}

// TestSlotCountValidation 两代槽位数校验（2026 参考实现显式校验 len==25）。
func TestSlotCountValidation(t *testing.T) {
	v := loadVectors(t)
	for _, n := range []int{17, 19, 24, 26} {
		cfg := make([]any, n)
		for i := range cfg {
			cfg[i] = 0
		}
		if _, err := New2025().ProofToken(cfg, "0.5", "0fffff"); !errors.Is(err, ErrSlotCount) {
			t.Errorf("2025 %d slots: err = %v, want ErrSlotCount", n, err)
		}
		if _, err := New2026().ProofToken(cfg, "0.5", "0fffff"); !errors.Is(err, ErrSlotCount) {
			t.Errorf("2026 %d slots: err = %v, want ErrSlotCount", n, err)
		}
		if _, err := New2026().RequirementsToken(cfg); !errors.Is(err, ErrSlotCount) {
			t.Errorf("2026 %d slots requirements: err = %v, want ErrSlotCount", n, err)
		}
	}
	// 正确槽位数不报 ErrSlotCount。
	if _, err := New2025().ProofToken(v.Tokens2025[0].Config, "0.5", "0fffff"); errors.Is(err, ErrSlotCount) {
		t.Errorf("18 slots rejected: %v", err)
	}
	if _, err := New2026().ProofToken(v.Tokens2026[0].Config, "0.5", "0fffff"); errors.Is(err, ErrSlotCount) {
		t.Errorf("25 slots rejected: %v", err)
	}
}

// TestDifficultyValidation 难度串校验面。
func TestDifficultyValidation(t *testing.T) {
	v := loadVectors(t)
	cfg25 := v.Tokens2025[0].Config
	cfg26 := v.Tokens2026[0].Config

	if _, err := New2025().ProofToken(cfg25, "0.5", "0ffff"); !errors.Is(err, ErrDifficulty) { // 奇数长度
		t.Errorf("odd-length hex: err = %v, want ErrDifficulty", err)
	}
	if _, err := New2025().ProofToken(cfg25, "0.5", "zzzzzz"); !errors.Is(err, ErrDifficulty) {
		t.Errorf("non-hex: err = %v, want ErrDifficulty", err)
	}
	if _, err := New2026().ProofToken(cfg26, "0.5", "0ff fff"); !errors.Is(err, ErrDifficulty) {
		t.Errorf("2026 space: err = %v, want ErrDifficulty", err)
	}
	// 2026 大写难度被 normalize 成小写后应正常工作（web_client.py:164-166）。
	tok, err := New2026(WithMaxAttempts(200000), WithClock(func() time.Duration { return 0 })).
		ProofToken(cfg26, "0.123456789", "0FFFFF")
	if err != nil {
		t.Fatalf("uppercase difficulty: %v", err)
	}
	if ok, err := Verify(V2026, tok, "0.123456789", "0fffff"); err != nil || !ok {
		t.Errorf("uppercase difficulty token Verify = %v, %v", ok, err)
	}
}

// TestVerifyTokenShape token 形态面：未知前缀 / 空载荷 / 坏 base64。
func TestVerifyTokenShape(t *testing.T) {
	if _, err := Verify(V2025, "not-a-token", "0.5", "0fffff"); !errors.Is(err, ErrToken) {
		t.Errorf("bad prefix: err = %v, want ErrToken", err)
	}
	if _, err := Verify(V2025, PrefixProof, "0.5", "0fffff"); !errors.Is(err, ErrToken) {
		t.Errorf("empty payload: err = %v, want ErrToken", err)
	}
	if _, err := Verify(V2025, PrefixProof+"!!!", "0.5", "0fffff"); !errors.Is(err, ErrToken) {
		t.Errorf("bad base64: err = %v, want ErrToken", err)
	}
	// "~S" 后缀在哈希输入之外（剥离后与无后缀形态等价）。
	v := loadVectors(t)
	vec := v.Tokens2026[0]
	stripped := strings.TrimSuffix(vec.Expected, Suffix2026)
	if ok, err := Verify(V2026, stripped, vec.Seed, vec.Difficulty); err != nil || !ok {
		t.Errorf("suffix-stripped token Verify = %v, %v; want true", ok, err)
	}
}

// TestProofToken2026RealClock 默认时钟（进程单调）路径端到端：求解→Verify 通过。
// 尝试次数不定（真实耗时），但命中难度是确定的。
func TestProofToken2026RealClock(t *testing.T) {
	v := loadVectors(t)
	vec := v.Tokens2026[0]
	b := New2026(WithMaxAttempts(200000))
	tok, err := b.ProofToken(vec.Config, "0.123456789", "0fffff")
	if err != nil {
		t.Fatalf("ProofToken(real clock): %v", err)
	}
	ok, err := Verify(V2026, tok, "0.123456789", "0fffff")
	if err != nil || !ok {
		t.Fatalf("Verify = %v, %v; want true", ok, err)
	}
	// 槽位不变式：槽 3 是尝试计数（整数）、槽 9 是耗时（float 毫秒）。
	raw, err := DecodeTokenPayload(tok)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	var slots []any
	if err := json.Unmarshal(raw, &slots); err != nil {
		t.Fatalf("payload JSON: %v", err)
	}
	if n, ok := slots[SlotAttempt].(float64); !ok || n < 0 || n != float64(int(n)) {
		t.Errorf("slot 3 = %v, want attempt int", slots[SlotAttempt])
	}
	if ms, ok := slots[SlotElapsed].(float64); !ok || ms < 0 {
		t.Errorf("slot 9 = %v, want elapsed ms float", slots[SlotElapsed])
	}
}

// TestRequirements2025RealSeed 默认种子源（Python format(random.random()) 语义）
// 路径端到端：token 形态合法 + 槽 3/9 满足 i / i>>1 不变式。
func TestRequirements2025RealSeed(t *testing.T) {
	v := loadVectors(t)
	cfg := v.Tokens2025[0].Config
	tok, err := New2025().RequirementsToken(cfg)
	if err != nil {
		t.Fatalf("RequirementsToken(real seed): %v", err)
	}
	if !strings.HasPrefix(tok, PrefixRequirements) {
		t.Fatalf("token = %s, want %q prefix", tok, PrefixRequirements)
	}
	raw, err := DecodeTokenPayload(tok)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	var slots []any
	if err := json.Unmarshal(raw, &slots); err != nil {
		t.Fatalf("payload JSON: %v", err)
	}
	if len(slots) != 18 {
		t.Fatalf("slots = %d, want 18", len(slots))
	}
	i, ok := slots[SlotAttempt].(float64)
	if !ok || i < 0 || i != float64(int(i)) {
		t.Fatalf("slot 3 = %v, want non-negative int", slots[SlotAttempt])
	}
	j, ok := slots[SlotElapsed].(float64)
	if !ok || j != float64(int(i)>>1) {
		t.Errorf("slot 9 = %v, want slot 3 >> 1 = %d", slots[SlotElapsed], int(i)>>1)
	}
}

// TestUnknownVersion 未知代际面。
func TestUnknownVersion(t *testing.T) {
	if _, err := NewBuilder(Version(99)); !errors.Is(err, ErrUnknownVersion) {
		t.Errorf("NewBuilder(99) err = %v, want ErrUnknownVersion", err)
	}
	v := loadVectors(t)
	if _, err := Verify(Version(99), v.Tokens2025[0].Expected, "0.5", "0fffff"); !errors.Is(err, ErrUnknownVersion) {
		t.Errorf("Verify(99) err = %v, want ErrUnknownVersion", err)
	}
	if got := Version(99).String(); got != "unknown" {
		t.Errorf("Version(99).String() = %q", got)
	}
	if got := Version(99).SlotCount(); got != 0 {
		t.Errorf("Version(99).SlotCount() = %d", got)
	}
}

// TestBuilderVersionAndConstructors 两代接口暴露面（同一接口、版本参数）。
func TestBuilderVersionAndConstructors(t *testing.T) {
	b25, err := NewBuilder(V2025)
	if err != nil || b25.Version() != V2025 {
		t.Fatalf("NewBuilder(V2025) = %v, %v", b25, err)
	}
	b26, err := NewBuilder(V2026)
	if err != nil || b26.Version() != V2026 {
		t.Fatalf("NewBuilder(V2026) = %v, %v", b26, err)
	}
	if New2025().Version() != V2025 || New2026().Version() != V2026 {
		t.Fatal("convenience constructors version mismatch")
	}
	if V2025.SlotCount() != 18 || V2026.SlotCount() != 25 {
		t.Fatalf("slot counts = %d/%d, want 18/25", V2025.SlotCount(), V2026.SlotCount())
	}
}
