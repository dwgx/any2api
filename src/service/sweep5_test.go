// W5-20 e2e：剩余面清扫五——C1 两处实差（retry-after-ms / 资源级 403 冷却）、
// count_tokens 出口（anthropic 透传 + 本地加权估算 + 400 错误面）、responses
// 错误码两路径对称（服务壳 vs 库层）、interactions 断线续流边界锁（证据化不做）。
//
// 装置：httptest 假上游（本地回环；无真实上游/凭据）+ 真服务
// （New(cfg).Handler() 经 httptest.NewServer）+ 真 HTTP 客户端（postJSON）。
package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 条目 1：C1 两处实差（retry-after-ms 解析 / 资源级 403 冷却判据）
// ---------------------------------------------------------------------------

// azureResourceBlockedBody Azure 资源级 403 的错误体（滥用策略挂起；官方症状
// 原文「Error message contains "resource has been temporarily blocked"」——
// tmp/lanes/P05-azure/raw/trouble.md §HTTP 403）。
const azureResourceBlockedBody = `{"error":{"code":"Forbidden","message":"The resource has been temporarily blocked due to suspicious activity. Contact support to unblock the resource."}}`

// TestRetryAfterMsParsing `retry-after-ms`（毫秒）解析：毫秒优先、秒级兜底、
// 上限守卫、非数字丢弃（W5-20 条目 1 实差 ①）。
func TestRetryAfterMsParsing(t *testing.T) {
	cases := []struct {
		name string
		ra   string // retry-after
		ms   string // retry-after-ms
		want time.Duration
	}{
		{"ms_basic", "", "2000", 2 * time.Second},
		{"ms_subsecond", "", "500", 500 * time.Millisecond},
		{"ms_wins_over_seconds", "3", "1500", 1500 * time.Millisecond}, // Azure 指引：有 ms 用 ms
		{"seconds_when_ms_absent", "3", "", 3 * time.Second},
		{"seconds_when_ms_unparsable", "3", "abc", 3 * time.Second},
		{"ms_capped", "", "999999999", maxRetryAfterCap},
		{"seconds_capped", "999999", "", maxRetryAfterCap},
		{"ms_zero_falls_back", "4", "0", 4 * time.Second},
		{"ms_negative_falls_back", "4", "-1", 4 * time.Second},
		{"both_absent", "", "", 0},
		{"http_date_seconds_discarded", "Wed, 21 Oct 2015 07:28:00 GMT", "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			if tc.ra != "" {
				h.Set("retry-after", tc.ra)
			}
			if tc.ms != "" {
				h.Set("retry-after-ms", tc.ms)
			}
			if got := retryAfterSeconds(h); got != tc.want {
				t.Fatalf("retryAfterSeconds(ra=%q, ms=%q) = %v, want %v", tc.ra, tc.ms, got, tc.want)
			}
		})
	}
}

// TestUpstreamCooldownSelection 冷却时长选择：头优先；资源级 403 无头 → 固定
// 窗口（首次命中即冷却）；其余 → 0（走连续失败阈值）。
func TestUpstreamCooldownSelection(t *testing.T) {
	blocked := []byte(azureResourceBlockedBody)
	if got := upstreamCooldown(403, http.Header{}, blocked); got != resourceBlockedCooldown {
		t.Fatalf("resource-403 无头 = %v, want %v", got, resourceBlockedCooldown)
	}
	h := http.Header{}
	h.Set("retry-after-ms", "2000")
	if got := upstreamCooldown(403, h, blocked); got != 2*time.Second {
		t.Fatalf("resource-403 带头 = %v, want 2s（头优先）", got)
	}
	if got := upstreamCooldown(400, http.Header{}, blocked); got != 0 {
		t.Fatalf("400 = %v, want 0", got)
	}
	if got := upstreamCooldown(500, http.Header{}, []byte("oops")); got != 0 {
		t.Fatalf("500 = %v, want 0", got)
	}
}

// TestShouldFailoverResourceBlocked403 资源级 403 判据：403 + 文本特征 → 换
// 上游；文本缺失/大小写外形态不换；x-should-retry 语义仍在文本判据之前。
func TestShouldFailoverResourceBlocked403(t *testing.T) {
	cases := []struct {
		name   string
		status int
		xsr    string
		body   string
		want   bool
	}{
		{"resource_blocked_403", 403, "", azureResourceBlockedBody, true},
		{"resource_blocked_403_mixed_case", 403, "", `{"error":{"message":"The Resource Has Been Temporarily Blocked"}}`, true},
		{"plain_403", 403, "", errBodyAPIError, false},
		{"empty_403", 403, "", "", false},
		{"resource_blocked_xsr_false", 403, "false", azureResourceBlockedBody, false},
		{"resource_blocked_xsr_true", 403, "true", azureResourceBlockedBody, true},
		{"resource_blocked_400_not_matched", 400, "", azureResourceBlockedBody, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			if tc.xsr != "" {
				h.Set("x-should-retry", tc.xsr)
			}
			if got := shouldFailover(tc.status, h, []byte(tc.body)); got != tc.want {
				t.Fatalf("shouldFailover(%d, xsr=%q) = %v, want %v", tc.status, tc.xsr, got, tc.want)
			}
		})
	}
}

// TestE2EFailoverRetryAfterMsCoolsUpstream 429 + `retry-after-ms` → 按毫秒冷却
// （此前该头被静默丢弃——实差 ①）。
func TestE2EFailoverRetryAfterMsCoolsUpstream(t *testing.T) {
	bad := failUpstream(t, 429, map[string]string{"retry-after-ms": "60000"}, errBodyAPIError)
	good := goodClaudeUpstream(t)
	s, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	for req := 1; req <= 2; req++ {
		if code, body := postChatStream(t, srv.URL); code != 200 || !strings.Contains(body, "Hello ") {
			t.Fatalf("req%d: status=%d body=\n%s", req, code, body)
		}
	}
	if bad.count() != 1 || good.count() != 2 {
		t.Fatalf("hits: bad=%d good=%d（期望 1/2：retry-after-ms 冷却内跳过 bad）", bad.count(), good.count())
	}
	if d := s.Diagnostics(); d.CooldownSkips == 0 {
		t.Fatalf("diagnostics = %+v（retry-after-ms 冷却计数缺失）", d)
	}
}

// TestE2EFailoverResourceBlocked403CoolsUpstream 资源级 403（滥用挂起）→ 换
// 下一个上游 + 首次命中即冷却（实差 ②）。
func TestE2EFailoverResourceBlocked403CoolsUpstream(t *testing.T) {
	bad := failUpstream(t, 403, nil, azureResourceBlockedBody)
	good := goodClaudeUpstream(t)
	s, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	for req := 1; req <= 2; req++ {
		if code, body := postChatStream(t, srv.URL); code != 200 || !strings.Contains(body, "Hello ") {
			t.Fatalf("req%d: status=%d body=\n%s（资源级 403 必须换上游）", req, code, body)
		}
	}
	if bad.count() != 1 || good.count() != 2 {
		t.Fatalf("hits: bad=%d good=%d（期望 1/2：req1 换上游 + req2 冷却跳过）", bad.count(), good.count())
	}
	if d := s.Diagnostics(); d.CooldownSkips == 0 || d.Failovers == 0 {
		t.Fatalf("diagnostics = %+v（换上游/冷却计数缺失）", d)
	}
}

// TestE2EFailoverPlain403NoSwitch 无文本特征的普通 403 仍是「请求问题」：不换
// 上游、错误信封直接上抛（资源级判据只按文本特征命中——反证面）。
func TestE2EFailoverPlain403NoSwitch(t *testing.T) {
	bad := failUpstream(t, 403, nil, errBodyAPIError)
	good := goodClaudeUpstream(t)
	_, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	code, _ := postChatStream(t, srv.URL)
	if code != 403 {
		t.Fatalf("status=%d（普通 403 必须原样上抛）", code)
	}
	if bad.count() != 1 || good.count() != 0 {
		t.Fatalf("hits: bad=%d good=%d（期望 1/0：不换上游）", bad.count(), good.count())
	}
}

// TestE2EErrorEnvelopeRetryAfterMsForwarded 429 + `retry-after-ms` → 错误信封把
// 该头透传下游（与解析面同族：能理解毫秒形态的客户端拿到真实等待时长）。
func TestE2EErrorEnvelopeRetryAfterMsForwarded(t *testing.T) {
	bad := failUpstream(t, 429, map[string]string{"retry-after-ms": "3000"}, errBodyAPIError)
	srv := startService(t, cfgFor(bad.srv.URL, ProtocolAnthropic, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 429 {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if got := resp.Header.Get("retry-after-ms"); got != "3000" {
		t.Fatalf("retry-after-ms = %q（want 3000——头透传）", got)
	}
}

// ---------------------------------------------------------------------------
// 条目 2：count_tokens 出口
// ---------------------------------------------------------------------------

// countTokensUpstream 假 anthropic count_tokens 上游（记录路径/查询/头/体/命中数）。
type countTokensUpstream struct {
	srv                         *httptest.Server
	mu                          sync.Mutex
	path, query, key, ver, beta string
	body                        []byte
	hits                        int
}

func newCountTokensUpstream(t *testing.T, status int, respBody string, respHeaders map[string]string) *countTokensUpstream {
	t.Helper()
	cu := &countTokensUpstream{}
	cu.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		cu.mu.Lock()
		cu.path, cu.query = r.URL.Path, r.URL.RawQuery
		cu.key, cu.ver, cu.beta = r.Header.Get("x-api-key"), r.Header.Get("anthropic-version"), r.Header.Get("anthropic-beta")
		cu.body = b
		cu.hits++
		cu.mu.Unlock()
		for k, v := range respHeaders {
			w.Header().Set(k, v)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(respBody))
	}))
	t.Cleanup(cu.srv.Close)
	return cu
}

func (cu *countTokensUpstream) snapshot() (path, query, key, ver, beta string, body []byte, hits int) {
	cu.mu.Lock()
	defer cu.mu.Unlock()
	return cu.path, cu.query, cu.key, cu.ver, cu.beta, cu.body, cu.hits
}

// TestE2ECountTokensAnthropicPassthrough 上游为 anthropic → 透传：路径/查询/
// 鉴权/版本头/beta 头转发；生成参数剥除（sub2api 同款）；响应原样 + request-id
// 承接（P06 §可复用点 1 用例 ③④）。
func TestE2ECountTokensAnthropicPassthrough(t *testing.T) {
	up := newCountTokensUpstream(t, 200, `{"input_tokens":2095}`, map[string]string{"request-id": "req_up_1"})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	body := `{"model":"claude-sonnet-4-5","max_tokens":1024,"temperature":0.5,"stream":true,"messages":[{"role":"user","content":"hi"}],"system":"sys","tools":[{"name":"t","description":"d","input_schema":{"type":"object"}}]}`
	resp := postJSON(t, srv.URL+"/v1/messages/count_tokens?beta=true", body,
		map[string]string{"anthropic-version": "2023-06-01", "anthropic-beta": "token-counting-2024-11-01"})
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	if strings.TrimSpace(string(raw)) != `{"input_tokens":2095}` {
		t.Fatalf("响应未原样透传: %s", raw)
	}
	if resp.Header.Get("request-id") != "req_up_1" {
		t.Fatalf("request-id = %q（want req_up_1 承接）", resp.Header.Get("request-id"))
	}

	path, query, key, ver, beta, upBody, hits := up.snapshot()
	if path != "/v1/messages/count_tokens" || query != "beta=true" {
		t.Fatalf("上游 path=%q query=%q", path, query)
	}
	if key != "test-key" || ver != "2023-06-01" || beta != "token-counting-2024-11-01" {
		t.Fatalf("上游头: key=%q ver=%q beta=%q", key, ver, beta)
	}
	if hits != 1 {
		t.Fatalf("上游命中 = %d（want 1）", hits)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(upBody, &m); err != nil {
		t.Fatalf("上游体: %v\n%s", err, upBody)
	}
	for _, k := range []string{"max_tokens", "temperature", "stream"} {
		if _, ok := m[k]; ok {
			t.Fatalf("生成参数 %q 未被剥除: %s", k, upBody)
		}
	}
	for _, k := range []string{"model", "messages", "system", "tools"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("请求输入字段 %q 缺失: %s", k, upBody)
		}
	}
}

// TestE2ECountTokensLocalEstimate 无 anthropic 上游 → 本地加权估算：生成参数
// 被接受并忽略（不 400）、`?beta=true` 接受、数值与权重表手算一致、tools 面加
// 一次性 500 前言（CC 实测常数）。
func TestE2ECountTokensLocalEstimate(t *testing.T) {
	srv := startService(t, cfgFor("http://127.0.0.1:1", ProtocolOpenAIChat, 15))

	body := `{"model":"claude-sonnet-4-5","max_tokens":1024,"temperature":0.5,"top_p":0.9,"stream":true,"stop_sequences":["x"],"messages":[{"role":"user","content":"hello world"}],"system":"You are terse."}`
	resp := postJSON(t, srv.URL+"/v1/messages/count_tokens?beta=true", body,
		map[string]string{"anthropic-beta": "token-counting-2024-11-01"})
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		InputTokens int `json:"input_tokens"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, raw)
	}
	// 手算（Claude 档权重；语料 = "You are terse.\nuser\nhello world\n"）：
	// You 1.24 + sp 0.14 + are 1.24 + sp 0.14 + terse 1.24 + . 0.66 = 4.66
	// \n 1.19 + user 1.24 + \n 1.19 + hello 1.24 + sp 0.14 + world 1.24 + \n 1.19
	// = 12.09 → ceil 13。
	if out.InputTokens != 13 {
		t.Fatalf("input_tokens = %d, want 13（权重表手算）", out.InputTokens)
	}
	if resp.Header.Get("request-id") == "" {
		t.Fatalf("request-id 缺失（B10：诊断关联）")
	}

	// 带 tools：语料增加 + 一次性 500 前言（不是每工具）。
	withTools := `{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hello world"}],"system":"You are terse.","tools":[{"name":"get_weather","description":"Get the weather","input_schema":{"type":"object"}}]}`
	resp2 := postJSON(t, srv.URL+"/v1/messages/count_tokens", withTools, nil)
	defer resp2.Body.Close()
	raw2, _ := io.ReadAll(resp2.Body)
	var out2 struct {
		InputTokens int `json:"input_tokens"`
	}
	if err := json.Unmarshal(raw2, &out2); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, raw2)
	}
	if out2.InputTokens < out.InputTokens+countTokensPreamble {
		t.Fatalf("带 tools 估算 = %d, want ≥ %d（= %d + 前言 %d）", out2.InputTokens, out.InputTokens+countTokensPreamble, out.InputTokens, countTokensPreamble)
	}
}

// TestE2ECountTokensUpstream404FallsBackLocal anthropic 上游 404（能力缺失）→
// 本地估算兜底（200），绝不给客户端 404（那会触发 CC 的真实计费降级）。
func TestE2ECountTokensUpstream404FallsBackLocal(t *testing.T) {
	up := newCountTokensUpstream(t, 404, `{"type":"error","error":{"type":"not_found_error","message":"Not Found"}}`, nil)
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	resp := postJSON(t, srv.URL+"/v1/messages/count_tokens",
		`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hello world"}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d（404 必须被本地估算兜底）", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		InputTokens int `json:"input_tokens"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, raw)
	}
	// 语料 = "user\nhello world\n" → 1.24+1.19+2.62+1.19 = 6.24 → ceil 7。
	if out.InputTokens != 7 {
		t.Fatalf("input_tokens = %d, want 7（本地估算兜底）", out.InputTokens)
	}
	if _, _, _, _, _, _, hits := up.snapshot(); hits != 1 {
		t.Fatalf("上游命中 = %d（want 1）", hits)
	}
}

// TestE2ECountTokensInvalidBody400 体非法 → 400 invalid_request_error（Anthropic
// 信封）；非 POST → 405。绝不用 500（触发客户端重试面）。
func TestE2ECountTokensInvalidBody400(t *testing.T) {
	srv := startService(t, cfgFor("http://127.0.0.1:1", ProtocolAnthropic, 15))

	cases := []struct {
		name    string
		body    string
		wantMsg string
	}{
		{"not_json", `{"model":`, "not valid JSON"},
		{"missing_model", `{"messages":[{"role":"user","content":"hi"}]}`, "model"},
		{"missing_messages", `{"model":"claude-sonnet-4-5"}`, "messages"},
		{"empty_messages", `{"model":"claude-sonnet-4-5","messages":[]}`, "messages"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := postJSON(t, srv.URL+"/v1/messages/count_tokens", tc.body, nil)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			raw, _ := io.ReadAll(resp.Body)
			var env struct {
				Type  string `json:"type"`
				Error struct {
					Type    string `json:"type"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(raw, &env); err != nil {
				t.Fatalf("unmarshal: %v\n%s", err, raw)
			}
			if env.Type != "error" || env.Error.Type != "invalid_request_error" || !strings.Contains(env.Error.Message, tc.wantMsg) {
				t.Fatalf("envelope = %s", raw)
			}
		})
	}

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/messages/count_tokens", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	gresp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer gresp.Body.Close()
	if gresp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405", gresp.StatusCode)
	}
}

// TestEstimateTokensWeights 本地估算器单元：两档权重（Claude / Claude 4.7+）
// 与模型版本判定的手算值。
func TestEstimateTokensWeights(t *testing.T) {
	cases := []struct {
		model, text string
		want        int
	}{
		{"claude-sonnet-4-5", "hello world", 3}, // 1.24+0.14+1.24 = 2.62 → 3
		{"claude-opus-4-7", "hello world", 4},   // 1.52+0.21+1.52 = 3.25 → 4
		{"claude-3-5-sonnet-20241022", "你好", 3}, // 1.38×2 = 2.76 → 3
		{"gpt-5", "hello", 2},                   // 非 claude → 缺省档：1.24 → 2
		{"claude", "a\nb", 5},                   // 无版本 → 新档：1.52+1.38+1.52 = 4.42 → 5
		{"claude-sonnet-4-5", "🙂", 3},           // emoji 2.6 → 3
		{"claude-sonnet-4-5", "x@y.com", 8},     // x 1.24 + @ 2.82 + y 1.24 + . 0.66 + com 1.24 = 7.2 → 8
	}
	for _, tc := range cases {
		if got := estimateTokens(tc.model, tc.text); got != tc.want {
			t.Fatalf("estimateTokens(%q, %q) = %d, want %d", tc.model, tc.text, got, tc.want)
		}
	}
}

// TestClaudeNewTokenizerDetection 模型版本 → tokenizer 档判定（4.7+ 或无版本
// 为新档；日期后缀结束版本段）。
func TestClaudeNewTokenizerDetection(t *testing.T) {
	cases := []struct {
		model string
		want  bool
	}{
		{"claude-opus-4-7", true},
		{"claude-sonnet-4-5", false},
		{"claude-3-5-sonnet-20241022", false},
		{"claude-sonnet-4-20250514", false},
		{"claude-5", true},
		{"claude", true},
		{"gpt-5", false},
		{"us.anthropic.claude-opus-4-7-v1:0", true},
		{"claude-2.1", false},
	}
	for _, tc := range cases {
		if got := claudeNewTokenizer(tc.model); got != tc.want {
			t.Fatalf("claudeNewTokenizer(%q) = %v, want %v", tc.model, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// 条目 3：interactions 断线续流边界锁（证据化不做——入口面不存在）
// ---------------------------------------------------------------------------

// TestInteractionsResumeNotExposed interactions 只有上游协议面，没有入口面：
// `POST /v1beta/interactions` 与续流路径 `GET /v1beta/interactions/{id}`（P10
// 点名：`GET` + `last_event_id` 是唯一恢复路径）都必须 404——本测试锁死该边界；
// 若将来加入口面，本测试失败即提醒同步实现/裁定续流（报告 A2A-L19-W5-20 条目 3）。
func TestInteractionsResumeNotExposed(t *testing.T) {
	srv := startService(t, cfgFor("http://127.0.0.1:1", ProtocolInteractions, 15))
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/v1beta/interactions"},
		{http.MethodGet, "/v1beta/interactions/abc?stream=true&last_event_id=evt_1"},
	} {
		req, err := http.NewRequest(tc.method, srv.URL+tc.path, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s %s = %d, want 404（interactions 入口面不做）", tc.method, tc.path, resp.StatusCode)
		}
	}
}

// ---------------------------------------------------------------------------
// 条目 4：responses 错误码两路径对称（服务壳 vs 库层）
// ---------------------------------------------------------------------------

// TestResponsesErrorCodeParityAcrossPaths responses 面两条错误出口的帧形状逐字
// 同构：平铺 error 事件键集相同、code 缺省同值 server_error、response.failed
// 的 response.error 键集相同（W5-20 条目 4；W5-12 缺口 1 的统一动作）。
func TestResponsesErrorCodeParityAcrossPaths(t *testing.T) {
	type runResult struct {
		flatKeys, nestedKeys []string
		flatCode, nestedCode string
	}
	run := func(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) runResult {
		t.Helper()
		up := newChatUpstream(t, handler)
		srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))
		resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","stream":true}`, nil)
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		raw, _ := io.ReadAll(resp.Body)
		frames := parseSSEFrames(t, raw)
		var flat, failed map[string]any
		for _, f := range frames {
			switch f.Data["type"] {
			case "error":
				flat = f.Data
			case "response.failed":
				failed = f.Data
			}
		}
		if flat == nil || failed == nil {
			t.Fatalf("error/response.failed 帧缺失:\n%s", raw)
		}
		ro, _ := failed["response"].(map[string]any)
		if ro == nil {
			t.Fatalf("response.failed.response 缺失:\n%s", raw)
		}
		errObj, _ := ro["error"].(map[string]any)
		if errObj == nil {
			t.Fatalf("response.failed.response.error 缺失:\n%s", raw)
		}
		return runResult{
			flatKeys:   sortedKeys(flat),
			nestedKeys: sortedKeys(errObj),
			flatCode:   fmt.Sprint(flat["code"]),
			nestedCode: fmt.Sprint(errObj["code"]),
		}
	}

	// 路径 A（服务壳）：已提交后畸形帧 → writeDownstreamError。
	shell := run(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", `{"id":"chatcmpl-p1","object":"chat.completion.chunk","created":1741293000,"model":"gpt-5","choices":[{"index":0,"delta":{"role":"assistant","content":"partial"},"finish_reason":null}]}`)
		sseFrame(w, "", `{"id":"chatcmpl-p1","choices":[oops`)
	})
	// 路径 B（库层）：上游截断（EOF 早于 finish_reason）→ 管道 EventError →
	// responses.StreamWriter 渲染。
	lib := run(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", `{"id":"chatcmpl-p1","object":"chat.completion.chunk","created":1741293000,"model":"gpt-5","choices":[{"index":0,"delta":{"role":"assistant","content":"partial"},"finish_reason":null}]}`)
	})

	for name, r := range map[string]runResult{"shell": shell, "library": lib} {
		if r.flatCode != "server_error" || r.nestedCode != "server_error" {
			t.Fatalf("%s: code = %q/%q（want server_error——条目 4 统一缺省）", name, r.flatCode, r.nestedCode)
		}
	}
	if strings.Join(shell.flatKeys, ",") != strings.Join(lib.flatKeys, ",") {
		t.Fatalf("平铺 error 键集不对称: shell=%v library=%v", shell.flatKeys, lib.flatKeys)
	}
	if strings.Join(shell.nestedKeys, ",") != strings.Join(lib.nestedKeys, ",") {
		t.Fatalf("嵌套 error 键集不对称: shell=%v library=%v", shell.nestedKeys, lib.nestedKeys)
	}
}

// sortedKeys map 键排序（键集比较用）。
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
