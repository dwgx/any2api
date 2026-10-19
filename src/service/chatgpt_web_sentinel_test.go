// chatgpt_web sentinel 求解接线（W42-2）：requirements 挑战 → 本地求解 → 提交头
// （openai-sentinel-proof-token / turnstile / arkose）。
//
// 装置：复用 chatgpt_web_upstream_test.go 的假上游（新增 sentinelCheck 校验面——
// fake 侧用 sentinel.Verify 回验 proof token，未带/不合法 → 403）+ 真服务 + 真
// HTTP 客户端。零真机：全部路径经 fake 上游回放。
//
// 测组（brief §Change ③）：
//
//	① 无挑战流（原样）：sentinel 体仍 `{}`、零挑战头、下游正文照常；
//	② 有挑战流（求解 → 提交 → 200）：PoW 2026 + turnstile/arkose（FakeSolver）
//	   + 求解器失败/缺失的显式错误面；
//	③ 求解失败（ErrUnsolved → 明确错误面，绝不打 prepare）；
//	④ 两代版本切换（2025 = 18 槽 sha3_512 / 2026 = 25 槽 FNV-1a；跨代 Verify
//	   互拒）。
//
// 反证（brief §Acceptance「把求解步骤短路 → ②红」）：原样输出见
// tmp/lanes/W42-wire/proofs/revert-red-and-green.txt。
package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dwgx/any2api/translate/convert/chatgpt_web/sentinel"
)

// ---------------------------------------------------------------------------
// 脚手架：挑战响应构造 + 注入点覆写 + fake 侧回验
// ---------------------------------------------------------------------------

// chatgptWebSentinelBody 组装 chat-requirements 响应体：默认 token 字段 + 附加
// 挑战对象（W42-2 测试面——形态照 KB §Sentinel 端点表 / chat2api
// ChatService.py:183-260）。
func chatgptWebSentinelBody(extra map[string]any) string {
	m := map[string]any{
		"token":        chatgptWebTestSentinelToken,
		"expire_after": 540,
		"expire_at":    1791982800,
	}
	for k, v := range extra {
		m[k] = v
	}
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// withChatGPTWebSentinel 覆写求解器构造注入点（package 内测试串行——与 log 捕获
// 同款纪律；收尾恢复）。
func withChatGPTWebSentinel(t *testing.T, fn func() (sentinel.Builder, error)) {
	t.Helper()
	prev := chatgptWebSentinelBuilder
	chatgptWebSentinelBuilder = fn
	t.Cleanup(func() { chatgptWebSentinelBuilder = prev })
}

// withChatGPTWebSolvers 覆写 turnstile/arkose 求解器注入点（收尾恢复）。
func withChatGPTWebSolvers(t *testing.T, turnstile sentinel.TurnstileSolver, arkose sentinel.ArkoseSolver) {
	t.Helper()
	prevT, prevA := chatgptWebTurnstileSolver, chatgptWebArkoseSolver
	chatgptWebTurnstileSolver, chatgptWebArkoseSolver = turnstile, arkose
	t.Cleanup(func() { chatgptWebTurnstileSolver, chatgptWebArkoseSolver = prevT, prevA })
}

// verifyProofHeader fake 上游的回验面（W42-2）：请求必须带 proof token 且过
// sentinel.Verify（指定代际）——缺/不合法返回错误（fake 侧据此 403）。
func verifyProofHeader(c chatgptWebCall, v sentinel.Version, seed, difficulty string) error {
	got := c.headers.Get(headerSentinelProofToken)
	if got == "" {
		return errors.New("missing openai-sentinel-proof-token")
	}
	if !strings.HasPrefix(got, sentinel.PrefixProof) {
		head := got
		if len(head) > 16 {
			head = head[:16]
		}
		return fmt.Errorf("proof token prefix = %q (want %q)", head, sentinel.PrefixProof)
	}
	ok, err := sentinel.Verify(v, got, seed, difficulty)
	if err != nil {
		return fmt.Errorf("verify %s token: %w", v, err)
	}
	if !ok {
		return fmt.Errorf("%s proof token does not satisfy seed/difficulty", v)
	}
	return nil
}

// ---------------------------------------------------------------------------
// ① 无挑战流（原样面）
// ---------------------------------------------------------------------------

// TestChatGPTWebSentinelNoChallenge 无挑战 = 完全原样：三请求调用序不变、
// sentinel 体仍 `{}`、挑战头一个都不设（绝不发明头）、下游正文照常。
func TestChatGPTWebSentinelNoChallenge(t *testing.T) {
	up := newChatGPTWebUpstream(t)
	up.streamBody = chatgptWebGolden(t, "anon_v1_isp.sse.txt")
	srv := startService(t, cfgChatGPTWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-6","messages":[{"role":"user","content":"Reply with exactly: PONG"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}
	text, _, _, done := parseOpenAIStream(t, raw)
	if text != "PONG" || !done {
		t.Fatalf("下游 = %q done=%v", text, done)
	}
	calls := waitForChatGPTWebCalls(t, up, 3)
	if len(calls) != 3 {
		t.Fatalf("调用数 = %d（want 3：sentinel/prepare/completion）: %v", len(calls), chatgptWebCallPaths(calls))
	}
	if string(calls[0].body) != "{}" {
		t.Fatalf("sentinel 体 = %q（无挑战流必须原样空体——W29-1 §1.2）", calls[0].body)
	}
	for i, c := range calls {
		for _, h := range []string{headerSentinelProofToken, headerSentinelTurnstileToken, headerSentinelArkoseToken} {
			if got := c.headers.Get(h); got != "" {
				t.Fatalf("调用[%d]（%s）无挑战流不得带 %s = %q", i, c.path, h, got)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// ② 有挑战流：PoW（求解 → 提交 → 200）
// ---------------------------------------------------------------------------

// TestChatGPTWebSentinelProofOfWork fake 下发 proofofwork 挑战（seed/difficulty）
// 并对 prepare/completion 两个请求回验 proof token（缺/不合法 → 403）：求解 →
// 提交 → 200。提交位断言：两个请求同一份 proof token（2026 代形态 gAAAAAB…~S）。
func TestChatGPTWebSentinelProofOfWork(t *testing.T) {
	const seed = "w42-2-seed"
	const difficulty = "0fffff"
	up := newChatGPTWebUpstream(t)
	up.sentinelBody = chatgptWebSentinelBody(map[string]any{
		"proofofwork": map[string]any{"required": true, "seed": seed, "difficulty": difficulty},
	})
	up.streamBody = chatgptWebGolden(t, "anon_v1_isp.sse.txt")
	var verified atomic.Int32
	up.sentinelCheck = func(c chatgptWebCall) error {
		if err := verifyProofHeader(c, sentinel.V2026, seed, difficulty); err != nil {
			return err
		}
		verified.Add(1)
		return nil
	}
	srv := startService(t, cfgChatGPTWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-6","messages":[{"role":"user","content":"Reply with exactly: PONG"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d（want 200——求解后提交应被 fake 接受）body=\n%s", resp.StatusCode, raw)
	}
	text, _, _, done := parseOpenAIStream(t, raw)
	if text != "PONG" || !done {
		t.Fatalf("下游 = %q done=%v", text, done)
	}
	calls := waitForChatGPTWebCalls(t, up, 3)
	if len(calls) != 3 {
		t.Fatalf("调用数 = %d: %v", len(calls), chatgptWebCallPaths(calls))
	}
	// sentinel 链首请求不得带 proof（挑战尚未下发——先有响应才有解）。
	if got := calls[0].headers.Get(headerSentinelProofToken); got != "" {
		t.Fatalf("sentinel 请求不得带 proof token = %q", got)
	}
	proof := calls[1].headers.Get(headerSentinelProofToken)
	if !strings.HasPrefix(proof, sentinel.PrefixProof) || !strings.HasSuffix(proof, sentinel.Suffix2026) {
		t.Fatalf("prepare proof token 形态 = %q（want gAAAAAB…~S——2026 代）", proof)
	}
	// prepare 与 completion 两个请求同一份 proof（四头提交位——W42-2）。
	if got := calls[2].headers.Get(headerSentinelProofToken); got != proof {
		t.Fatalf("completion proof token = %q（want 与 prepare 同值）", got)
	}
	// fake 回验次数 = 2（prepare + completion 各一次——证明提交位覆盖两个请求）。
	if n := verified.Load(); n != 2 {
		t.Fatalf("fake 回验次数 = %d（want 2：prepare + completion）", n)
	}
	// 无 turnstile/arkose 挑战 → 对应头不设（绝不发明头）。
	for i, c := range calls {
		for _, h := range []string{headerSentinelTurnstileToken, headerSentinelArkoseToken} {
			if got := c.headers.Get(h); got != "" {
				t.Fatalf("调用[%d]（%s）不得带 %s = %q", i, c.path, h, got)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// ② 有挑战流：turnstile / arkose（FakeSolver 注入 → 提交 → 200）+ 失败面
// ---------------------------------------------------------------------------

// TestChatGPTWebSentinelTurnstileArkose fake 出 turnstile+arkose 题（persona =
// chatgpt-freeaccount）→ 注入的 FakeSolver 求解 → 提交头 → 200；求解器请求形态
// 逐字段断言（chat2api ChatService.py:207-243 调用面）。
func TestChatGPTWebSentinelTurnstileArkose(t *testing.T) {
	const tDX = "turnstile-dx-blob-w42"
	const aDX = "arkose-dx-blob-w42"
	fake := &sentinel.FakeSolver{TurnstileToken: "turnstile-tok-1", ArkoseToken: "arkose-tok-1"}
	withChatGPTWebSolvers(t, fake, fake)

	up := newChatGPTWebUpstream(t)
	up.sentinelBody = chatgptWebSentinelBody(map[string]any{
		"persona":   "chatgpt-freeaccount",
		"turnstile": map[string]any{"required": true, "dx": tDX},
		"arkose":    map[string]any{"required": true, "dx": aDX},
	})
	up.streamBody = chatgptWebGolden(t, "anon_v1_isp.sse.txt")
	up.sentinelCheck = func(c chatgptWebCall) error {
		if got := c.headers.Get(headerSentinelTurnstileToken); got != "turnstile-tok-1" {
			return fmt.Errorf("turnstile header = %q", got)
		}
		if got := c.headers.Get(headerSentinelArkoseToken); got != "arkose-tok-1" {
			return fmt.Errorf("arkose header = %q", got)
		}
		return nil
	}
	srv := startService(t, cfgChatGPTWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-6","messages":[{"role":"user","content":"Reply with exactly: PONG"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d（want 200——求解后提交应被 fake 接受）body=\n%s", resp.StatusCode, raw)
	}
	text, _, _, done := parseOpenAIStream(t, raw)
	if text != "PONG" || !done {
		t.Fatalf("下游 = %q done=%v", text, done)
	}
	calls := waitForChatGPTWebCalls(t, up, 3)
	// prepare 与 completion 都带两个求解头（同一份）。
	for _, i := range []int{1, 2} {
		if got := calls[i].headers.Get(headerSentinelTurnstileToken); got != "turnstile-tok-1" {
			t.Fatalf("调用[%d]（%s）turnstile 头 = %q", i, calls[i].path, got)
		}
		if got := calls[i].headers.Get(headerSentinelArkoseToken); got != "arkose-tok-1" {
			t.Fatalf("调用[%d]（%s）arkose 头 = %q", i, calls[i].path, got)
		}
	}
	// 求解器调用记录：turnstile {url,p,dx,ua}（chat2api ChatService.py:207-216 形态）。
	tc := fake.TurnstileCalls()
	if len(tc) != 1 {
		t.Fatalf("turnstile 求解调用数 = %d（want 1）", len(tc))
	}
	if tc[0].URL != sentinel.DefaultTurnstileURL || tc[0].DX != tDX || tc[0].UA != chatgptWebUA || tc[0].P != "" {
		t.Fatalf("turnstile 求解请求 = %+v（want url=%s、dx 原样、ua 同源、p 空——本面单步无客户端 p）",
			tc[0], sentinel.DefaultTurnstileURL)
	}
	// arkose {blob,method}：persona=chatgpt-freeaccount → chat35（ChatService.py:220-224）。
	ac := fake.ArkoseCalls()
	if len(ac) != 1 {
		t.Fatalf("arkose 求解调用数 = %d（want 1）", len(ac))
	}
	if ac[0].Blob != aDX || ac[0].Method != "chat35" {
		t.Fatalf("arkose 求解请求 = %+v（want blob=%s method=chat35）", ac[0], aDX)
	}
}

// TestChatGPTWebSentinelSolverFailure 求解器失败/缺失 = 显式错误面（预提交失败
// ——绝不打 prepare；错误消息不回显挑战 dx）。
func TestChatGPTWebSentinelSolverFailure(t *testing.T) {
	run := func(t *testing.T, wantSubstr string) {
		t.Helper()
		up := newChatGPTWebUpstream(t)
		up.sentinelBody = chatgptWebSentinelBody(map[string]any{
			"turnstile": map[string]any{"required": true, "dx": "dx-secret-value"},
		})
		srv := startService(t, cfgChatGPTWeb(up.srv.URL, ""))

		resp := postJSON(t, srv.URL+"/v1/chat/completions",
			`{"model":"gpt-6","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode < 400 {
			t.Fatalf("status=%d（求解失败必须显式报错）body=\n%s", resp.StatusCode, raw)
		}
		if !strings.Contains(string(raw), wantSubstr) {
			t.Fatalf("错误信封缺 %q: %s", wantSubstr, raw)
		}
		if strings.Contains(string(raw), "dx-secret-value") {
			t.Fatalf("错误消息回显了挑战 dx（回显纪律）: %s", raw)
		}
		calls := waitForChatGPTWebCalls(t, up, 1)
		if len(calls) != 1 || calls[0].path != chatgptWebSentinelPath {
			t.Fatalf("调用 = %v（want 仅 sentinel——预提交失败）", chatgptWebCallPaths(calls))
		}
	}

	t.Run("solver_error", func(t *testing.T) {
		fake := &sentinel.FakeSolver{TurnstileErr: errors.New("solver upstream down")}
		withChatGPTWebSolvers(t, fake, fake)
		run(t, "turnstile solve failed")
	})
	t.Run("solver_missing", func(t *testing.T) {
		// 缺省注入点 = nil（真求解器不接——零真机红线）：出题而求解器缺失 =
		// 显式错误（绝不静默跳过挑战）。
		withChatGPTWebSolvers(t, nil, nil)
		run(t, "no solver is wired")
	})
}

// ---------------------------------------------------------------------------
// ③ 求解失败路径：ErrUnsolved → 明确错误面
// ---------------------------------------------------------------------------

// TestChatGPTWebSentinelProofUnsolved 求解上限收窄到 1 次 + 2025 代 difficulty
// "000000"（W41-2 冻结：严格 < 语义 ⇒ 数学不可解）——ErrUnsolved 必然触发
// （确定性，无概率面）：prepare 链显式失败、错误信封含 proof-of-work 面、绝不
// 打 prepare/completion（预提交失败）。
func TestChatGPTWebSentinelProofUnsolved(t *testing.T) {
	withChatGPTWebSentinel(t, func() (sentinel.Builder, error) {
		return sentinel.NewBuilder(sentinel.V2025, sentinel.WithMaxAttempts(1))
	})
	up := newChatGPTWebUpstream(t)
	up.sentinelBody = chatgptWebSentinelBody(map[string]any{
		"proofofwork": map[string]any{"required": true, "seed": "seed-unsolved", "difficulty": "000000"},
	})
	srv := startService(t, cfgChatGPTWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-6","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 400 {
		t.Fatalf("status=%d（ErrUnsolved 必须显式报错）body=\n%s", resp.StatusCode, raw)
	}
	if !strings.Contains(string(raw), "proof-of-work") {
		t.Fatalf("错误信封缺 proof-of-work 面（ErrUnsolved 归因）: %s", raw)
	}
	if strings.Contains(string(raw), "gAAAAA") {
		t.Fatalf("错误消息回显了令牌形态串（回显纪律）: %s", raw)
	}
	calls := waitForChatGPTWebCalls(t, up, 1)
	if len(calls) != 1 || calls[0].path != chatgptWebSentinelPath {
		t.Fatalf("调用 = %v（want 仅 sentinel——求解失败是预提交失败，绝不打 prepare）", chatgptWebCallPaths(calls))
	}
}

// ---------------------------------------------------------------------------
// ④ 两代版本切换：2025（18 槽 sha3_512） / 2026（25 槽 FNV-1a）
// ---------------------------------------------------------------------------

// TestChatGPTWebSentinelTwoGenerations 求解代际由注入点的 Builder.Version()
// 决定：fake 按同代回验（跨代 token 必然互拒——两代哈希/比较语义不同）。
func TestChatGPTWebSentinelTwoGenerations(t *testing.T) {
	const seed = "w42-2-two-gen"
	const difficulty = "0fffff"
	cases := []struct {
		name    string
		builder func() (sentinel.Builder, error)
		version sentinel.Version
		suffix  string
	}{
		{"v2025", func() (sentinel.Builder, error) { return sentinel.NewBuilder(sentinel.V2025) }, sentinel.V2025, ""},
		{"v2026", func() (sentinel.Builder, error) { return sentinel.NewBuilder(sentinel.V2026) }, sentinel.V2026, sentinel.Suffix2026},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withChatGPTWebSentinel(t, tc.builder)
			up := newChatGPTWebUpstream(t)
			up.sentinelBody = chatgptWebSentinelBody(map[string]any{
				"proofofwork": map[string]any{"required": true, "seed": seed, "difficulty": difficulty},
			})
			up.streamBody = chatgptWebGolden(t, "anon_v1_isp.sse.txt")
			up.sentinelCheck = func(c chatgptWebCall) error {
				return verifyProofHeader(c, tc.version, seed, difficulty)
			}
			srv := startService(t, cfgChatGPTWeb(up.srv.URL, ""))

			resp := postJSON(t, srv.URL+"/v1/chat/completions",
				`{"model":"gpt-6","messages":[{"role":"user","content":"Reply with exactly: PONG"}],"stream":true}`, nil)
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != 200 {
				t.Fatalf("status=%d（want 200——%s 代求解后提交应被 fake 接受）body=\n%s", resp.StatusCode, tc.version, raw)
			}
			text, _, _, done := parseOpenAIStream(t, raw)
			if text != "PONG" || !done {
				t.Fatalf("下游 = %q done=%v", text, done)
			}
			calls := waitForChatGPTWebCalls(t, up, 3)
			proof := calls[1].headers.Get(headerSentinelProofToken)
			// 代际形态：2025 无后缀 / 2026 带 "~S"（W41-2 冻结）。
			if tc.suffix == "" {
				if strings.HasSuffix(proof, sentinel.Suffix2026) {
					t.Fatalf("2025 代 proof token 不得带 ~S 后缀: %q", proof)
				}
			} else if !strings.HasSuffix(proof, tc.suffix) {
				t.Fatalf("2026 代 proof token 应以 %q 结尾: %q", tc.suffix, proof)
			}
			// 跨代反证：另一代 Verify 必须拒（difficulty 取 "00000000" 把
			// 偶然通过概率压到 ~1e-10——2025 严格 < 语义下为零概率）。
			other := sentinel.V2026
			if tc.version == sentinel.V2026 {
				other = sentinel.V2025
			}
			if ok, _ := sentinel.Verify(other, proof, seed, "00000000"); ok {
				t.Fatalf("跨代校验必须拒绝（%s token 过了 %s Verify）", tc.version, other)
			}
		})
	}
}
