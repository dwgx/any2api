package sentinel

import (
	"strings"
	"testing"
	"time"
)

// zeroRand 复刻向量脚本化的随机源：choice → seq[0]、random → 0.125。
type zeroRand struct{}

func (zeroRand) IntN(int) int     { return 0 }
func (zeroRand) Float64() float64 { return 0.125 }

// TestDefaultConfig2025Vector 2025 代默认 config 与参考实现 get_config
// （proof_of_work.py:12-29）逐字节一致（脚本化 rand/clock/uuid 下确定性）。
func TestDefaultConfig2025Vector(t *testing.T) {
	v := loadVectors(t)
	scripted := v.ConfigDefaults.Scripted
	cfg := NewConfig2025(Config2025Params{
		UserAgent:     v.ConfigDefaults.UserAgent,
		Scripts:       []string{"https://cdn.oaistatic.com/_next/static/chunks/x.js"},
		DPL:           "453ebaec0d44c2decab71692e1bfe39be35a24b3",
		NavigatorKeys: []string{"onmessage"},
		DocumentKeys:  []string{"attributes"},
		WindowKeys:    []string{"devicePixelRatio"},
		Cores:         []int{4},
		Rand:          zeroRand{},
		Now:           time.Date(2026, 10, 14, 10, 4, 5, 0, time.UTC), // = 05:04:05 -0500
		PerfMS:        scripted.PerfCounterS * 1000,
		UUIDHex:       scripted.UUIDHex,
	})
	got, err := encodeConfig(cfg)
	if err != nil {
		t.Fatalf("encodeConfig: %v", err)
	}
	if string(got) != v.ConfigDefaults.Config2025JSON {
		t.Errorf("config2025 mismatch:\n got %s\nwant %s", got, v.ConfigDefaults.Config2025JSON)
	}
}

// TestDefaultConfig2026Vector 2026 代默认 config 与参考实现
// _default_sentinel_config（web_client.py:85-127）逐字节一致。
func TestDefaultConfig2026Vector(t *testing.T) {
	v := loadVectors(t)
	scripted := v.ConfigDefaults.Scripted
	cfg := NewConfig2026(Config2026Params{
		UserAgent:     v.ConfigDefaults.UserAgent,
		ClientVersion: "prod-20260810913b",
		Language:      "en-US",
		SessionID:     "",
		Location:      time.FixedZone("Tokyo Standard Time", 9*3600),
		Rand:          zeroRand{},
		Now:           time.Date(2026, 10, 14, 12, 4, 5, 0, time.UTC), // = 21:04:05 +0900
		PerfMS:        scripted.PerfCounterS * 1000,
		UUIDHex:       scripted.UUIDHex,
		CPUCount:      scripted.CPUCount,
	})
	got, err := encodeConfig(cfg)
	if err != nil {
		t.Fatalf("encodeConfig: %v", err)
	}
	if string(got) != v.ConfigDefaults.Config2026JSON {
		t.Errorf("config2026 mismatch:\n got %s\nwant %s", got, v.ConfigDefaults.Config2026JSON)
	}
}

// TestDefaultConfigStructure 两代默认 config 的结构面（槽位数/常量槽/动态槽）。
func TestDefaultConfigStructure(t *testing.T) {
	c25 := NewConfig2025(Config2025Params{UserAgent: "ua"})
	if len(c25) != 18 {
		t.Fatalf("2025 slots = %d, want 18", len(c25))
	}
	if c25[2] != 4294705152 {
		t.Errorf("2025 slot 2 = %v, want 4294705152", c25[2])
	}
	if c25[SlotAttempt] != 0 || c25[SlotElapsed] != 0 {
		t.Errorf("2025 dynamic slots = %v/%v, want 0/0 placeholders", c25[SlotAttempt], c25[SlotElapsed])
	}
	if c25[7] != "en-US" || c25[8] != "en-US,es-US,en,es" {
		t.Errorf("2025 lang slots = %v/%v", c25[7], c25[8])
	}
	if perf, ok := c25[13].(float64); !ok || perf <= 0 {
		t.Errorf("2025 slot 13 = %v, want positive float", c25[13])
	}
	if _, ok := c25[17].(float64); !ok {
		t.Errorf("2025 slot 17 = %v (%T), want float", c25[17], c25[17])
	}
	if s, ok := c25[14].(string); !ok || len(s) != 36 {
		t.Errorf("2025 slot 14 = %v, want uuid string", c25[14])
	}

	c26 := NewConfig2026(Config2026Params{UserAgent: "ua"})
	if len(c26) != 25 {
		t.Fatalf("2026 slots = %d, want 25", len(c26))
	}
	if c26[0] != 2134 {
		t.Errorf("2026 slot 0 = %v, want 2134", c26[0])
	}
	if c26[2] != 4294967296 {
		t.Errorf("2026 slot 2 = %v, want 4294967296", c26[2])
	}
	if c26[5] != nil {
		t.Errorf("2026 slot 5 = %v, want nil", c26[5])
	}
	if c26[10] != "vendor\u2212Google Inc." {
		t.Errorf("2026 slot 10 = %q, want vendor−Google Inc. (U+2212)", c26[10])
	}
	if s, ok := c26[11].(string); !ok || !strings.HasPrefix(s, "_reactListening") || len(s) != len("_reactListening")+10 {
		t.Errorf("2026 slot 11 = %v, want _reactListening+10 hex", c26[11])
	}
	for i := 18; i < 25; i++ {
		if c26[i] != 0 {
			t.Errorf("2026 slot %d = %v, want 0", i, c26[i])
		}
	}
	if n, ok := c26[16].(int); !ok || n < 1 {
		t.Errorf("2026 slot 16 = %v, want >=1", c26[16])
	}
	if f, ok := c26[3].(float64); !ok || f < 0 || f >= 1 {
		t.Errorf("2026 slot 3 = %v, want [0,1) float", c26[3])
	}
	if f, ok := c26[9].(float64); !ok || f < 0 || f >= 1 {
		t.Errorf("2026 slot 9 = %v, want [0,1) float", c26[9])
	}
	if s, ok := c26[1].(string); !ok || !strings.Contains(s, " GMT") || !strings.Contains(s, " (") {
		t.Errorf("2026 slot 1 = %v, want Date.toString-like string", c26[1])
	}
}

// TestArkoseMethodPersona persona → arkose method（ChatService.py:220-224）。
func TestArkoseMethodPersona(t *testing.T) {
	cases := map[string]string{
		"chatgpt-freeaccount": "chat35",
		"chatgpt-paid":        "chat4",
		"chatgpt-noauth":      "chat4",
		"":                    "chat4",
		"other":               "chat4",
	}
	for persona, want := range cases {
		if got := ArkoseMethod(persona); got != want {
			t.Errorf("ArkoseMethod(%q) = %q, want %q", persona, got, want)
		}
	}
}
