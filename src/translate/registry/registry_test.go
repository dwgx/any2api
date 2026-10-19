package registry

import (
	"errors"
	"strings"
	"testing"
)

func TestLookupDirect(t *testing.T) {
	r := New(FormatOpenAIChat)
	r.MustRegister(Conversion{From: FormatOpenAIChat, To: FormatClaudeMessages, Quality: QualityGood})

	plan, err := r.Lookup(FormatOpenAIChat, FormatClaudeMessages)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if plan.ViaHub {
		t.Fatalf("direct hit must not be ViaHub")
	}
	if len(plan.Steps) != 1 || plan.Steps[0] != (Step{From: FormatOpenAIChat, To: FormatClaudeMessages}) {
		t.Fatalf("steps = %+v", plan.Steps)
	}
	if plan.Quality != QualityGood {
		t.Fatalf("quality = %q, want good", plan.Quality)
	}
}

func TestLookupDirectPreferredOverHub(t *testing.T) {
	// 直转优先（new-api 范式）：显式对存在时不得绕枢纽。
	r := New(FormatOpenAIChat)
	r.MustRegister(Conversion{From: FormatOpenAIChat, To: FormatClaudeMessages, Quality: QualityDiscouraged})
	r.MustRegister(Conversion{From: FormatClaudeMessages, To: FormatOpenAIChat, Quality: QualityGood})

	plan, err := r.Lookup(FormatClaudeMessages, FormatOpenAIChat)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if plan.ViaHub || len(plan.Steps) != 1 {
		t.Fatalf("expected direct plan, got %+v", plan)
	}
}

func TestLookupViaHub(t *testing.T) {
	r := New(FormatOpenAIChat)
	r.MustRegister(Conversion{From: FormatClaudeMessages, To: FormatOpenAIChat, Quality: QualityGood})
	r.MustRegister(Conversion{From: FormatOpenAIChat, To: FormatGeminiGenerate, Quality: QualityGood})

	plan, err := r.Lookup(FormatClaudeMessages, FormatGeminiGenerate)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !plan.ViaHub || len(plan.Steps) != 2 {
		t.Fatalf("expected 2-step hub plan, got %+v", plan)
	}
	if plan.Steps[0] != (Step{From: FormatClaudeMessages, To: FormatOpenAIChat}) ||
		plan.Steps[1] != (Step{From: FormatOpenAIChat, To: FormatGeminiGenerate}) {
		t.Fatalf("steps = %+v", plan.Steps)
	}
	// 枢纽两步合成不高于 Fair（丢信息风险——arch-options 陷阱 2）。
	if plan.Quality != QualityFair {
		t.Fatalf("hub plan quality = %q, want fair (capped)", plan.Quality)
	}
}

func TestLookupViaHubWorstLegWins(t *testing.T) {
	r := New(FormatOpenAIChat)
	r.MustRegister(Conversion{From: FormatGeminiGenerate, To: FormatOpenAIChat, Quality: QualityDiscouraged})
	r.MustRegister(Conversion{From: FormatOpenAIChat, To: FormatClaudeMessages, Quality: QualityGood})

	plan, err := r.Lookup(FormatGeminiGenerate, FormatClaudeMessages)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if plan.Quality != QualityDiscouraged {
		t.Fatalf("quality = %q, want discouraged (worst leg)", plan.Quality)
	}
}

func TestLookupNoConversion(t *testing.T) {
	r := New(FormatOpenAIChat)
	r.MustRegister(Conversion{From: FormatOpenAIChat, To: FormatClaudeMessages, Quality: QualityGood})
	r.MustRegister(Conversion{From: FormatClaudeMessages, To: FormatOpenAIChat, Quality: QualityGood})

	// 缺腿的枢纽路径：claude -> gemini 两腿中缺 hub->gemini。
	_, err := r.Lookup(FormatClaudeMessages, FormatGeminiGenerate)
	if err == nil {
		t.Fatal("expected error, got nil (silent passthrough would be the CPA bug)")
	}
	if !errors.Is(err, ErrNoConversion) {
		t.Fatalf("errors.Is(err, ErrNoConversion) = false; err = %v", err)
	}
	var nce *NoConversionError
	if !errors.As(err, &nce) {
		t.Fatalf("errors.As NoConversionError failed: %v", err)
	}
	if nce.From != FormatClaudeMessages || nce.To != FormatGeminiGenerate {
		t.Fatalf("nce = %+v", nce)
	}
	if len(nce.Missing) != 1 || nce.Missing[0] != (Step{From: FormatOpenAIChat, To: FormatGeminiGenerate}) {
		t.Fatalf("missing = %+v", nce.Missing)
	}
	if !strings.Contains(err.Error(), "claude_messages -> gemini_generate") {
		t.Fatalf("error message lacks from/to: %v", err)
	}
}

func TestLookupUnregisteredPairNoHub(t *testing.T) {
	r := New("")
	_, err := r.Lookup(FormatOpenAIChat, FormatClaudeMessages)
	if !errors.Is(err, ErrNoConversion) {
		t.Fatalf("expected ErrNoConversion, got %v", err)
	}
}

func TestLookupSameFormatIsNotAConversion(t *testing.T) {
	r := New(FormatOpenAIChat)
	r.MustRegister(Conversion{From: FormatOpenAIChat, To: FormatClaudeMessages, Quality: QualityGood})

	_, err := r.Lookup(FormatOpenAIChat, FormatOpenAIChat)
	if !errors.Is(err, ErrNoConversion) {
		t.Fatalf("identity lookup must not resolve via registry, got %v", err)
	}
}

func TestRegisterRejectsDuplicatesAndIdentity(t *testing.T) {
	r := New(FormatOpenAIChat)
	r.MustRegister(Conversion{From: FormatOpenAIChat, To: FormatClaudeMessages})

	if err := r.Register(Conversion{From: FormatOpenAIChat, To: FormatClaudeMessages}); !errors.Is(err, ErrDuplicateConversion) {
		t.Fatalf("duplicate register err = %v", err)
	}
	if err := r.Register(Conversion{From: FormatOpenAIChat, To: FormatOpenAIChat}); !errors.Is(err, ErrIdentityConversion) {
		t.Fatalf("identity register err = %v", err)
	}
	if err := r.Register(Conversion{From: "", To: FormatClaudeMessages}); !errors.Is(err, ErrInvalidFormat) {
		t.Fatalf("empty format err = %v", err)
	}
}

func TestRegisterDefaultsQualityGood(t *testing.T) {
	r := New(FormatOpenAIChat)
	r.MustRegister(Conversion{From: FormatOpenAIChat, To: FormatClaudeMessages})
	plan, err := r.Lookup(FormatOpenAIChat, FormatClaudeMessages)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if plan.Quality != QualityGood {
		t.Fatalf("default quality = %q, want good", plan.Quality)
	}
}

func TestFormatsSortedUnique(t *testing.T) {
	r := New(FormatOpenAIChat)
	r.MustRegister(Conversion{From: FormatOpenAIChat, To: FormatClaudeMessages})
	r.MustRegister(Conversion{From: FormatOpenAIChat, To: FormatGeminiGenerate})
	got := r.Formats()
	want := []Format{FormatClaudeMessages, FormatGeminiGenerate, FormatOpenAIChat}
	if len(got) != len(want) {
		t.Fatalf("formats = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("formats = %v, want %v", got, want)
		}
	}
}
