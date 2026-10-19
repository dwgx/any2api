package sentinel

import (
	"context"
	"errors"
	"testing"
)

// TestFakeSolverRecords FakeSolver 记录调用 + 返回预设值 + 错误路径。
func TestFakeSolverRecords(t *testing.T) {
	ctx := context.Background()
	fs := &FakeSolver{TurnstileToken: "t-token", ArkoseToken: "a-token"}

	tok, err := fs.SolveTurnstile(ctx, TurnstileRequest{
		URL: DefaultTurnstileURL,
		P:   "gAAAAACx",
		DX:  "dx-blob",
		UA:  "ua",
	})
	if err != nil || tok != "t-token" {
		t.Fatalf("SolveTurnstile = %q, %v", tok, err)
	}
	at, err := fs.SolveArkose(ctx, ArkoseRequest{Blob: "blob", Method: ArkoseMethod("chatgpt-freeaccount")})
	if err != nil || at != "a-token" {
		t.Fatalf("SolveArkose = %q, %v", at, err)
	}

	tcalls := fs.TurnstileCalls()
	if len(tcalls) != 1 || tcalls[0].P != "gAAAAACx" || tcalls[0].DX != "dx-blob" {
		t.Fatalf("turnstile calls = %+v", tcalls)
	}
	acalls := fs.ArkoseCalls()
	if len(acalls) != 1 || acalls[0].Blob != "blob" || acalls[0].Method != "chat35" {
		t.Fatalf("arkose calls = %+v", acalls)
	}

	// 副本语义：外部改动不影响内部记录。
	tcalls[0].P = "mutated"
	if fs.TurnstileCalls()[0].P != "gAAAAACx" {
		t.Error("TurnstileCalls must return a copy")
	}

	// 错误路径优先。
	wantErr := errors.New("solver down")
	fs.TurnstileErr = wantErr
	fs.ArkoseErr = wantErr
	if _, err := fs.SolveTurnstile(ctx, TurnstileRequest{}); !errors.Is(err, wantErr) {
		t.Errorf("turnstile err = %v, want %v", err, wantErr)
	}
	if _, err := fs.SolveArkose(ctx, ArkoseRequest{}); !errors.Is(err, wantErr) {
		t.Errorf("arkose err = %v, want %v", err, wantErr)
	}
}
