// W18-2：上游 reset 头 → 精确冷却（W17-5 §缺 7 / 建议 1 的落地）。
//
// 三组测试：
//  1. reset → 冷却 + 钳制（Anthropic unified 族 / OpenAI x-ratelimit 族；单元 +
//     真 HTTP e2e 假上游 429）；
//  2. 优先级：`retry-after` 族产出有效值时恒赢；未产出有效值（缺失/0/HTTP-date/
//     畸形）时 reset 兜底；
//  3. 畸形/零值/门控不满足 → 保守（不冷却，退回连续失败阈值路径）。
//
// 反证（回退即红）见报告 §反证：revert-demo 树于 tmp/lanes/W18-reset/（真树零
// 触碰；全回退 / 去钳制 / 翻优先级 / 去门控 四种变异各跑出红）。
//
// 单元断言的时间基准：now 对齐到整秒（time.Unix(sec, 0)）——reset 头按 Unix 秒
// 传值，对齐后「期望时长」逐值精确、无截断误差。
package service

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 组 1：reset → 冷却 + 钳制
// ---------------------------------------------------------------------------

// TestW18ResetAnthropicUnifiedCooldown Anthropic unified 族：status=rejected 的
// 窗口 reset 参与冷却；多窗口取最晚；毫秒 epoch 自动识别。
func TestW18ResetAnthropicUnifiedCooldown(t *testing.T) {
	now := time.Unix(time.Now().Unix(), 0)
	ts := func(d time.Duration) string { return strconv.FormatInt(now.Add(d).Unix(), 10) }
	cases := []struct {
		name    string
		headers map[string]string
		want    time.Duration
	}{
		{
			name: "5h_rejected",
			headers: map[string]string{
				"anthropic-ratelimit-unified-status":    "rejected",
				"anthropic-ratelimit-unified-5h-status": "rejected",
				"anthropic-ratelimit-unified-5h-reset":  ts(2 * time.Minute),
			},
			want: 2 * time.Minute,
		},
		{
			name: "unified_top_rejected",
			headers: map[string]string{
				"anthropic-ratelimit-unified-status": "rejected",
				"anthropic-ratelimit-unified-reset":  ts(90 * time.Second),
			},
			want: 90 * time.Second,
		},
		{
			name: "multiple_rejected_windows_latest_wins",
			headers: map[string]string{
				"anthropic-ratelimit-unified-5h-status": "rejected",
				"anthropic-ratelimit-unified-5h-reset":  ts(30 * time.Second),
				"anthropic-ratelimit-unified-7d-status": "rejected",
				"anthropic-ratelimit-unified-7d-reset":  ts(4 * time.Minute),
			},
			want: 4 * time.Minute,
		},
		{
			name: "milliseconds_epoch_autodetect",
			headers: map[string]string{
				"anthropic-ratelimit-unified-7d_oi-status": "rejected",
				"anthropic-ratelimit-unified-7d_oi-reset":  strconv.FormatInt(now.Add(3*time.Minute).UnixMilli(), 10),
			},
			want: 3 * time.Minute,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			for k, v := range tc.headers {
				h.Set(k, v)
			}
			if got := resetHeaderCooldown(h, now); got != tc.want {
				t.Fatalf("resetHeaderCooldown = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestW18ResetOpenAIQuotaExhaustedCooldown OpenAI / xAI 族：remaining=0 的维度
// reset 参与冷却；值语义三形态（时长串 / 相对秒 / epoch 秒 / epoch 毫秒）；
// xAI 别名；双维度取最晚。
func TestW18ResetOpenAIQuotaExhaustedCooldown(t *testing.T) {
	now := time.Unix(time.Now().Unix(), 0)
	ts := func(d time.Duration) string { return strconv.FormatInt(now.Add(d).Unix(), 10) }
	cases := []struct {
		name    string
		headers map[string]string
		want    time.Duration
	}{
		{
			name: "tokens_duration_string",
			headers: map[string]string{
				"x-ratelimit-remaining-tokens": "0",
				"x-ratelimit-reset-tokens":     "6m0s",
			},
			want: 6 * time.Minute,
		},
		{
			name: "tokens_relative_seconds",
			headers: map[string]string{
				"x-ratelimit-remaining-tokens": "0",
				"x-ratelimit-reset-tokens":     "60",
			},
			want: 60 * time.Second,
		},
		{
			name: "requests_epoch_seconds",
			headers: map[string]string{
				"x-ratelimit-remaining-requests": "0",
				"x-ratelimit-reset-requests":     ts(90 * time.Second),
			},
			want: 90 * time.Second,
		},
		{
			name: "requests_epoch_milliseconds",
			headers: map[string]string{
				"x-ratelimit-remaining-requests": "0",
				"x-ratelimit-reset-requests":     strconv.FormatInt(now.UnixMilli()+120_000, 10),
			},
			want: 2 * time.Minute,
		},
		{
			name: "xai_alias_x_rate_limit",
			headers: map[string]string{
				"x-rate-limit-remaining-tokens": "0",
				"x-rate-limit-reset-tokens":     "90s",
			},
			want: 90 * time.Second,
		},
		{
			name: "both_dimensions_exhausted_latest_wins",
			headers: map[string]string{
				"x-ratelimit-remaining-requests": "0",
				"x-ratelimit-reset-requests":     ts(30 * time.Second),
				"x-ratelimit-remaining-tokens":   "0",
				"x-ratelimit-reset-tokens":       ts(4 * time.Minute),
			},
			want: 4 * time.Minute,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			for k, v := range tc.headers {
				h.Set(k, v)
			}
			if got := resetHeaderCooldown(h, now); got != tc.want {
				t.Fatalf("resetHeaderCooldown = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestW18ResetCooldownClamp 值域钳制：上限 = maxRetryAfterCap（被拒的 7d 窗口
// 不把上游一次罚到一周）；下限 = resetCooldownFloor（亚秒时长按 1s 执行）；
// 零值/过去时刻不冷却。
func TestW18ResetCooldownClamp(t *testing.T) {
	now := time.Unix(time.Now().Unix(), 0)
	rejected := func(reset string) http.Header {
		h := http.Header{}
		h.Set("anthropic-ratelimit-unified-status", "rejected")
		if reset != "" {
			h.Set("anthropic-ratelimit-unified-reset", reset)
		}
		return h
	}
	// 上限（单元面）：被拒的 7d 窗口（reset 7 天后）→ 钳到 maxRetryAfterCap。
	if got := resetHeaderCooldown(rejected(strconv.FormatInt(now.Add(7*24*time.Hour).Unix(), 10)), now); got != maxRetryAfterCap {
		t.Fatalf("7d 窗口: got=%v, want=%v（钳制上限）", got, maxRetryAfterCap)
	}
	// 上限（集成缝 upstreamCooldown——server.go 的实际调用点同径）。
	h7d := rejected(strconv.FormatInt(time.Now().Add(7*24*time.Hour).Unix(), 10))
	if got := upstreamCooldown(http.StatusTooManyRequests, h7d, nil); got != maxRetryAfterCap {
		t.Fatalf("upstreamCooldown 7d: got=%v, want=%v", got, maxRetryAfterCap)
	}
	// 下限：亚秒时长按 1s 执行（OpenAI 时长串 "500ms"——sub2api 同款地板）。
	hFloor := http.Header{}
	hFloor.Set("x-ratelimit-remaining-tokens", "0")
	hFloor.Set("x-ratelimit-reset-tokens", "500ms")
	if got := resetHeaderCooldown(hFloor, now); got != resetCooldownFloor {
		t.Fatalf("500ms: got=%v, want=%v（地板 1s）", got, resetCooldownFloor)
	}
	// 零值（reset == now）/过去时刻 → 不冷却。
	if got := resetHeaderCooldown(rejected(strconv.FormatInt(now.Unix(), 10)), now); got != 0 {
		t.Fatalf("reset=now: got=%v, want=0", got)
	}
	if got := resetHeaderCooldown(rejected(strconv.FormatInt(now.Add(-time.Minute).Unix(), 10)), now); got != 0 {
		t.Fatalf("reset 过去: got=%v, want=0", got)
	}
}

// TestW18E2EResetCoolsUpstream 真 HTTP e2e：假上游 429 + reset 头（两族各一）
// → 冷却到 reset 窗口；第二次请求跳过该上游。
func TestW18E2EResetCoolsUpstream(t *testing.T) {
	families := []struct {
		name    string
		headers func() map[string]string
	}{
		{"anthropic_unified", func() map[string]string {
			return map[string]string{
				"anthropic-ratelimit-unified-status":    "rejected",
				"anthropic-ratelimit-unified-5h-status": "rejected",
				"anthropic-ratelimit-unified-5h-reset":  strconv.FormatInt(time.Now().Add(30*time.Second).Unix(), 10),
			}
		}},
		{"openai_ratelimit", func() map[string]string {
			return map[string]string{
				"x-ratelimit-remaining-tokens": "0",
				"x-ratelimit-reset-tokens":     strconv.FormatInt(time.Now().Add(30*time.Second).Unix(), 10),
			}
		}},
	}
	for _, fam := range families {
		t.Run(fam.name, func(t *testing.T) {
			bad := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				for k, v := range fam.headers() {
					w.Header().Set(k, v)
				}
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(errBodyAPIError))
			})
			good := goodClaudeUpstream(t)
			s, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

			for req := 1; req <= 2; req++ {
				if code, body := postChatStream(t, srv.URL); code != 200 || !strings.Contains(body, "Hello ") {
					t.Fatalf("req%d: status=%d body=\n%s", req, code, body)
				}
			}
			if !s.cooling(0, time.Now()) {
				t.Fatalf("429 + reset 头必须冷却 bad 上游")
			}
			if bad.count() != 1 || good.count() != 2 {
				t.Fatalf("hits: bad=%d good=%d（期望 1/2：reset 窗口内跳过 bad）", bad.count(), good.count())
			}
			if d := s.Diagnostics(); d.CooldownSkips == 0 {
				t.Fatalf("diagnostics = %+v（reset 冷却跳过计数缺失）", d)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 组 2：优先级（retry-after 族恒赢；未产出有效值时 reset 兜底）
// ---------------------------------------------------------------------------

// TestW18ResetPriorityVsRetryAfter 两者并存谁赢的裁定锁定：
//
//   - `retry-after` / `retry-after-ms` 产出有效值 → 恒赢（请求级显式指令 >
//     窗口元数据；既有 C1 语义零变化）；
//   - `retry-after` 未产出有效值（HTTP-date 丢弃 / 0 退化值）→ reset 兜底；
//   - 无 reset 头时 `retry-after: 0` 语义不变（W14-2 边界）。
func TestW18ResetPriorityVsRetryAfter(t *testing.T) {
	rejected := func(h http.Header, resetAt time.Time) {
		h.Set("anthropic-ratelimit-unified-status", "rejected")
		h.Set("anthropic-ratelimit-unified-reset", strconv.FormatInt(resetAt.Unix(), 10))
	}
	// 1) retry-after 整数秒并存 → 恒赢（若 reset 赢会是 10min 上限值）。
	h := http.Header{}
	h.Set("retry-after", "5")
	rejected(h, time.Now().Add(2*time.Hour))
	if got := upstreamCooldown(http.StatusTooManyRequests, h, nil); got != 5*time.Second {
		t.Fatalf("retry-after 与 reset 并存: got=%v, want=5s（retry-after 恒赢）", got)
	}
	// 2) retry-after-ms 并存 → 同赢（毫秒优先于秒，reset 仍不参与）。
	h2 := http.Header{}
	h2.Set("retry-after", "5")
	h2.Set("retry-after-ms", "1500")
	rejected(h2, time.Now().Add(2*time.Hour))
	if got := upstreamCooldown(http.StatusTooManyRequests, h2, nil); got != 1500*time.Millisecond {
		t.Fatalf("retry-after-ms 并存: got=%v, want=1.5s", got)
	}
	// 3) retry-after HTTP-date（未产出有效值）→ reset 兜底（2h → 钳到上限）。
	h3 := http.Header{}
	h3.Set("retry-after", "Wed, 21 Oct 2015 07:28:00 GMT")
	rejected(h3, time.Now().Add(2*time.Hour))
	if got := upstreamCooldown(http.StatusTooManyRequests, h3, nil); got != maxRetryAfterCap {
		t.Fatalf("HTTP-date retry-after + reset: got=%v, want=%v（reset 兜底 + 钳制）", got, maxRetryAfterCap)
	}
	// 4) retry-after: 0（退化值，与畸形同列「未产出有效值」）→ 同兜底。
	h4 := http.Header{}
	h4.Set("retry-after", "0")
	rejected(h4, time.Now().Add(2*time.Hour))
	if got := upstreamCooldown(http.StatusTooManyRequests, h4, nil); got != maxRetryAfterCap {
		t.Fatalf("retry-after:0 + reset: got=%v, want=%v", got, maxRetryAfterCap)
	}
	// 5) 无 reset 头时 retry-after: 0 语义不变（不冷却——W14-2 边界锁定）。
	h5 := http.Header{}
	h5.Set("retry-after", "0")
	if got := upstreamCooldown(http.StatusTooManyRequests, h5, nil); got != 0 {
		t.Fatalf("retry-after:0 无 reset: got=%v, want=0", got)
	}
}

// ---------------------------------------------------------------------------
// 组 3：畸形/零值/门控不满足 → 保守（不冷却）
// ---------------------------------------------------------------------------

// TestW18ResetMalformedConservative 畸形值、零值、门控不满足（status ≠ rejected /
// remaining ≠ 0 / 头缺失）→ 一律不产出冷却（退回连续失败阈值路径）。
func TestW18ResetMalformedConservative(t *testing.T) {
	now := time.Unix(time.Now().Unix(), 0)
	future := strconv.FormatInt(now.Add(time.Hour).Unix(), 10)
	cases := []struct {
		name    string
		headers map[string]string
	}{
		// Anthropic 族：值面畸形/零值。
		{"anthropic_reset_garbage", map[string]string{
			"anthropic-ratelimit-unified-status": "rejected",
			"anthropic-ratelimit-unified-reset":  "soon",
		}},
		{"anthropic_reset_zero", map[string]string{
			"anthropic-ratelimit-unified-status": "rejected",
			"anthropic-ratelimit-unified-reset":  "0",
		}},
		{"anthropic_reset_negative", map[string]string{
			"anthropic-ratelimit-unified-status": "rejected",
			"anthropic-ratelimit-unified-reset":  "-1",
		}},
		{"anthropic_reset_trailing_garbage", map[string]string{
			"anthropic-ratelimit-unified-status": "rejected",
			"anthropic-ratelimit-unified-reset":  "1735689600xyz",
		}},
		{"anthropic_reset_float", map[string]string{
			"anthropic-ratelimit-unified-status": "rejected",
			"anthropic-ratelimit-unified-reset":  "1735689600.5",
		}},
		{"anthropic_reset_overlong", map[string]string{
			"anthropic-ratelimit-unified-status": "rejected",
			"anthropic-ratelimit-unified-reset":  "999999999999999999999",
		}},
		{"anthropic_reset_rfc3339", map[string]string{
			"anthropic-ratelimit-unified-status": "rejected",
			"anthropic-ratelimit-unified-reset":  "2030-01-01T00:00:00Z",
		}},
		// Anthropic 族：门控不满足。
		{"anthropic_status_allowed", map[string]string{
			"anthropic-ratelimit-unified-status": "allowed",
			"anthropic-ratelimit-unified-reset":  future,
		}},
		{"anthropic_status_allowed_warning", map[string]string{
			"anthropic-ratelimit-unified-status": "allowed_warning",
			"anthropic-ratelimit-unified-reset":  future,
		}},
		{"anthropic_status_missing", map[string]string{
			"anthropic-ratelimit-unified-reset": future,
		}},
		{"anthropic_rejected_reset_missing", map[string]string{
			"anthropic-ratelimit-unified-status": "rejected",
		}},
		// OpenAI 族：值面畸形/零值。
		{"openai_reset_garbage", map[string]string{
			"x-ratelimit-remaining-tokens": "0",
			"x-ratelimit-reset-tokens":     "abc",
		}},
		{"openai_reset_zero", map[string]string{
			"x-ratelimit-remaining-tokens": "0",
			"x-ratelimit-reset-tokens":     "0",
		}},
		{"openai_reset_negative", map[string]string{
			"x-ratelimit-remaining-tokens": "0",
			"x-ratelimit-reset-tokens":     "-30",
		}},
		{"openai_reset_rfc3339", map[string]string{
			"x-ratelimit-remaining-tokens": "0",
			"x-ratelimit-reset-tokens":     "2030-01-01T00:00:00Z",
		}},
		// OpenAI 族：门控不满足。
		{"openai_remaining_nonzero", map[string]string{
			"x-ratelimit-remaining-tokens": "5",
			"x-ratelimit-reset-tokens":     "1h",
		}},
		{"openai_remaining_missing", map[string]string{
			"x-ratelimit-reset-tokens": "1h",
		}},
		{"openai_remaining_garbage", map[string]string{
			"x-ratelimit-remaining-tokens": "n/a",
			"x-ratelimit-reset-tokens":     "1h",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			for k, v := range tc.headers {
				h.Set(k, v)
			}
			if got := resetHeaderCooldown(h, now); got != 0 {
				t.Fatalf("resetHeaderCooldown = %v, want 0（保守：不产出冷却）", got)
			}
		})
	}
}

// TestW18E2EResetMalformedNoCooldown 真 HTTP e2e：429 + rejected + 畸形 reset
// → 不冷却（第二次请求仍打 bad；不触发冷却跳过计数）。
func TestW18E2EResetMalformedNoCooldown(t *testing.T) {
	bad := failUpstream(t, http.StatusTooManyRequests, map[string]string{
		"anthropic-ratelimit-unified-status":    "rejected",
		"anthropic-ratelimit-unified-5h-status": "rejected",
		"anthropic-ratelimit-unified-5h-reset":  "not-a-timestamp",
	}, errBodyAPIError)
	good := goodClaudeUpstream(t)
	s, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	for req := 1; req <= 2; req++ {
		if code, body := postChatStream(t, srv.URL); code != 200 || !strings.Contains(body, "Hello ") {
			t.Fatalf("req%d: status=%d body=\n%s", req, code, body)
		}
	}
	if bad.count() != 2 || good.count() != 2 {
		t.Fatalf("hits: bad=%d good=%d（期望 2/2：畸形 reset 不冷却）", bad.count(), good.count())
	}
	if d := s.Diagnostics(); d.CooldownSkips != 0 {
		t.Fatalf("diagnostics = %+v（畸形 reset 不得触发冷却）", d)
	}
}
