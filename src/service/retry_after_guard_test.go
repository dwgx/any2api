// W14 护栏：上游 `retry-after` / `retry-after-ms` 出站转发守卫（防 CC 类消费端
// ×11 热重试放大）。
//
// 背景（W13-2 实测，2026-10-14）：claude.ai 429 带 `retry-after: 0` 经服务原样
// 透传 → CC 2.1.88 见 0 < 20s 立即热重试，一发调用放大为 11 发 completion +
// 11 条会话。规则（本文件锁定）：
//
//   - 0 / 非正 / 畸形 → 不转发该头（消费端回落自身退避逻辑）；
//   - 正值原样转发（既有毫秒/秒语义不动）。
//
// 装置与既有测试一致：httptest 假上游（本地回环）+ 真服务 + 真 HTTP 客户端。
// 反证面（回退护栏即红）：CP1 逐头独立（0 丢弃不得连带毫秒正值）/ CP2 跨协议
// 转换路径 / CP3 count_tokens 第二透传点——三者均断言「头必须缺席」，护栏回退
// （恢复无条件透传）即失败。
package service

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// claudeEntryBody 合法的 claude 入口请求体（错误面测试用；stream=true 走
// W13-2 实测同款路径）。
const claudeEntryBody = `{"model":"claude-x","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}`

// postClaudeEntry 发一次 claude 入口请求并读回（状态码 + 响应头 + 原文）。
func postClaudeEntry(t *testing.T, base string) (int, http.Header, string) {
	t.Helper()
	resp := postJSON(t, base+"/v1/messages", claudeEntryBody, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, string(raw)
}

// ---------------------------------------------------------------------------
// 三形态：0 丢弃 / 正值原样 / 畸形丢弃
// ---------------------------------------------------------------------------

// TestW14RetryAfterZeroDroppedDownstream 形态 1：429 + `retry-after: 0` →
// 下游无该头；状态码与体原样（同协议透传面）。
func TestW14RetryAfterZeroDroppedDownstream(t *testing.T) {
	up := failUpstream(t, 429, map[string]string{"retry-after": "0"}, errBodyAPIError)
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	status, hdr, body := postClaudeEntry(t, srv.URL)
	if status != 429 {
		t.Fatalf("status = %d（want 429：状态码原样）", status)
	}
	if got := hdr.Get("retry-after"); got != "" {
		t.Fatalf("retry-after = %q（want 空：0 不转发——CC 见 0 立即热重试 ×11）", got)
	}
	if body != errBodyAPIError {
		t.Fatalf("错误体非原样:\n%s", body)
	}
	if hdr.Get("request-id") == "" {
		t.Fatalf("request-id 缺失（护栏不得连带影响其它诊断头）")
	}
}

// TestW14RetryAfterPositiveForwardedVerbatim 形态 2：正值原样转发（秒/毫秒
// 两形态都不动字面）。
func TestW14RetryAfterPositiveForwardedVerbatim(t *testing.T) {
	cases := []struct {
		name   string
		header string
		value  string
	}{
		{"seconds_30", "retry-after", "30"},
		{"seconds_1", "retry-after", "1"},
		{"ms_3000", "retry-after-ms", "3000"},
		// 前导整数语义（与 retryAfterSeconds / CC parseInt 同口径）：尾随垃圾
		// 不改变前导整数，仍按该值原样转发。
		{"leading_int_trailing_garbage", "retry-after", "30xyz"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := failUpstream(t, 429, map[string]string{tc.header: tc.value}, errBodyAPIError)
			srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

			status, hdr, body := postClaudeEntry(t, srv.URL)
			if status != 429 {
				t.Fatalf("status = %d（want 429）", status)
			}
			if got := hdr.Get(tc.header); got != tc.value {
				t.Fatalf("%s = %q（want %q：正值原样转发）", tc.header, got, tc.value)
			}
			if body != errBodyAPIError {
				t.Fatalf("错误体非原样:\n%s", body)
			}
		})
	}
}

// TestW14RetryAfterMalformedDropped 形态 3：非正/畸形 → 下游无该头。
func TestW14RetryAfterMalformedDropped(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"negative", "-5"},
		{"non_numeric", "soon"},
		{"http_date", "Wed, 21 Oct 2015 07:28:00 GMT"},
		// parseInt("0.5")=0 / parseInt("0abc")=0：转发即 CC 立即热重试。
		{"fractional_zero", "0.5"},
		{"leading_zero_garbage", "0abc"},
		{"signed_positive", "+30"}, // RFC 7231 delay-seconds 无符号位
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := failUpstream(t, 429, map[string]string{"retry-after": tc.value}, errBodyAPIError)
			srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

			status, hdr, body := postClaudeEntry(t, srv.URL)
			if status != 429 {
				t.Fatalf("status = %d（want 429）", status)
			}
			if got := hdr.Get("retry-after"); got != "" {
				t.Fatalf("retry-after = %q（want 空：畸形值不转发）", got)
			}
			if body != errBodyAPIError {
				t.Fatalf("错误体非原样:\n%s", body)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 反证（回退护栏即红）
// ---------------------------------------------------------------------------

// TestW14RetryAfterZeroWithMsPositiveIndependent CP1：逐头独立——`retry-after: 0`
// 丢弃不得连带丢掉 `retry-after-ms` 的正值（毫秒形态仍有真实等待时长）。
// 护栏回退 → retry-after 以 "0" 出现 → 红。
func TestW14RetryAfterZeroWithMsPositiveIndependent(t *testing.T) {
	up := failUpstream(t, 429, map[string]string{
		"retry-after": "0", "retry-after-ms": "3000",
	}, errBodyAPIError)
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	status, hdr, body := postClaudeEntry(t, srv.URL)
	if status != 429 {
		t.Fatalf("status = %d（want 429）", status)
	}
	if got := hdr.Get("retry-after"); got != "" {
		t.Fatalf("retry-after = %q（want 空：0 丢弃）", got)
	}
	if got := hdr.Get("retry-after-ms"); got != "3000" {
		t.Fatalf("retry-after-ms = %q（want 3000：正值保留，逐头独立）", got)
	}
	if body != errBodyAPIError {
		t.Fatalf("错误体非原样:\n%s", body)
	}
}

// TestW14RetryAfterZeroDroppedCrossProtocol CP2：跨协议转换路径（gemini 上游 →
// claude 入口）同样过守卫——转换信封 + 头面同管。护栏回退 → retry-after 出现 → 红。
func TestW14RetryAfterZeroDroppedCrossProtocol(t *testing.T) {
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"code":429,"message":"quota exceeded","status":"RESOURCE_EXHAUSTED"}}`))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

	status, hdr, body := postClaudeEntry(t, srv.URL)
	if status != http.StatusTooManyRequests {
		t.Fatalf("status = %d（want 429）", status)
	}
	if got := hdr.Get("Retry-After"); got != "" {
		t.Fatalf("retry-after = %q（want 空：跨协议路径同样不转发 0）", got)
	}
	// 体仍是入口协议信封（转换面不受护栏影响）。
	if !strings.Contains(body, "rate_limit_error") || strings.Contains(body, "RESOURCE_EXHAUSTED") {
		t.Fatalf("claude 错误信封异常:\n%s", body)
	}
}

// TestW14RetryAfterZeroLeavesUpstreamFaceUntouched 边界控制（非反证）：护栏只
// 作用于出站到下游的头——`retry-after: 0` 在上游面既不冷却（0 秒 = 不冷却，
// 既有语义）也不改变换号决策，第二台上游照常兜住每次请求。
func TestW14RetryAfterZeroLeavesUpstreamFaceUntouched(t *testing.T) {
	bad := failUpstream(t, 429, map[string]string{"retry-after": "0"}, errBodyAPIError)
	good := goodClaudeUpstream(t)
	_, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	for req := 1; req <= 2; req++ {
		code, body := postChatStream(t, srv.URL)
		if code != 200 || !strings.Contains(body, "Hello ") {
			t.Fatalf("req%d: status=%d body=\n%s", req, code, body)
		}
	}
	if bad.count() != 2 || good.count() != 2 {
		t.Fatalf("hits: bad=%d good=%d（期望 2/2：retry-after:0 不冷却——上游面不受护栏影响）",
			bad.count(), good.count())
	}
}

// TestW14RetryAfterZeroDroppedCountTokens CP3：count_tokens 第二透传点
// （copyCountTokensResponseHeaders）同守卫——上游 400 + `retry-after: 0` →
// 下游 400 无该头、体原样。护栏回退 → 头出现 → 红。
func TestW14RetryAfterZeroDroppedCountTokens(t *testing.T) {
	const body400 = `{"type":"error","error":{"type":"invalid_request_error","message":"bad model"}}`
	up := failUpstream(t, 400, map[string]string{"retry-after": "0"}, body400)
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	resp := postJSON(t, srv.URL+"/v1/messages/count_tokens",
		`{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d（want 400：上游 400 照实上抛）", resp.StatusCode)
	}
	if got := resp.Header.Get("retry-after"); got != "" {
		t.Fatalf("retry-after = %q（want 空：count_tokens 面同守卫）", got)
	}
	if string(raw) != body400 {
		t.Fatalf("count_tokens 400 体非原样:\n%s", raw)
	}
}
