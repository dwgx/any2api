package service

// public_w58_test.go —— 公开服务 P0-5/P0-6/P0-8 测试（board A2A-L19-W58-1）：
// 成本档分类表（钉住阈值）/ 日预算台账（check-and-charge + UTC 日界）/
// 429 降级信封（rate_limited / daily_quota_exhausted / capacity_exhausted 按
// 入口协议 + 正整数 Retry-After + no-store）/ 档位拒绝（400）/
// 体上限与结构守卫（413 + 可配 + 显式 0 关）/ attempts sid+key_id（前缀纪律、
// 零凭据字节）/ ring 新事件 + 内存计数 / 硬门 public.enabled=false 零回归。
//
// 反证锚点（回退即红——原样输出见 tmp/lanes/W58-a/proofs/）：
//   - serve 的预算扣账改 admit(0)（不记账）→ TestPublicDailyBudgetExhausted429
//     红（第三发 200 非 429）；
//   - serve 的档位上限比较改恒不拒 → TestPublicCostClassRejected400 红
//     （40000 token 请求 200 非 400）；
//   - guardPublicBody 恒 true（结构守卫不生效）→ TestPublicBodyAndStructureGuards413
//     红（10 条 messages 200 非 413）；
//   - serve 终态面撤掉 errors.As(*credentialExhaustedError) 映射 →
//     TestPublicCapacityExhausted429 红（502 非 429 capacity_exhausted）；
//   - serve 撤掉 SIDFromContext 回填 → TestPublicAttemptsSidKeyID 红
//     （日志行无 "sid" 字段）。

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

// w58LightBody 轻档（light=1 unit）聊天体——预算测试用小步长（流式：mock 上游
// 按 SSE 应答）。
const w58LightBody = `{"model":"claude-sonnet-5-5","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}`

// w58PostChat 发一发聊天请求（可带头），返回 chatRes（异步安全）。
func w58PostChat(t *testing.T, base, body string, headers map[string]string) chatRes {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return chatRes{
		code:         resp.StatusCode,
		body:         raw,
		retryAfter:   resp.Header.Get("Retry-After"),
		cacheControl: resp.Header.Get("Cache-Control"),
	}
}

// w58PostPath 发一发任意路径的 POST（入口协议信封形状断言用）。
func w58PostPath(t *testing.T, url, body string) chatRes {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return chatRes{
		code:         resp.StatusCode,
		body:         raw,
		retryAfter:   resp.Header.Get("Retry-After"),
		cacheControl: resp.Header.Get("Cache-Control"),
	}
}

// w58RingKinds ring 当前全部事件型别（新→旧）。
func w58RingKinds(s *Server) []string {
	win := s.adminLog.window(0, "", 500, false)
	kinds := make([]string, 0, len(win.Entries))
	for _, e := range win.Entries {
		kinds = append(kinds, e.Kind)
	}
	return kinds
}

// w58RingHasKind ring 是否含某型别事件。
func w58RingHasKind(s *Server, kind string) bool {
	for _, k := range w58RingKinds(s) {
		if k == kind {
			return true
		}
	}
	return false
}

// w58ChatCfg 单上游（成功文本流）配置 + public 段。
func w58ChatCfg(t *testing.T, pub *PublicSection) (*Server, string) {
	t.Helper()
	up := goodClaudeUpstream(t)
	cfg := failoverCfg(anthUp("good", up.srv.URL))
	cfg.Public = pub
	s, srv := startServiceWith(t, cfg)
	return s, srv.URL
}

// w58BigChatBody 构造大体聊天请求（messages 条数 + 单条内容大小可控；流式）。
func w58BigChatBody(msgs int, contentLen int) string {
	var b strings.Builder
	b.WriteString(`{"model":"claude-sonnet-5-5","stream":true,"messages":[`)
	for i := range msgs {
		if i > 0 {
			b.WriteByte(',')
		}
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		fmt.Fprintf(&b, `{"role":%q,"content":%q}`, role, strings.Repeat("x", contentLen))
	}
	b.WriteString(`]}`)
	return b.String()
}

// ---------------------------------------------------------------------------
// 单元：分类表 / 台账 / 信封时间面
// ---------------------------------------------------------------------------

// TestW58CostClassTable 分类表钉住（阈值 = 2048/16384/32768；无值 = 中档）。
func TestW58CostClassTable(t *testing.T) {
	cases := []struct {
		n    int64
		has  bool
		want costClass
	}{
		{0, false, costClassMedium}, // 无 max_tokens
		{0, true, costClassMedium},  // 显式 0
		{-5, true, costClassMedium}, // 负值（畸形）
		{1, true, costClassLight},
		{2048, true, costClassLight}, // 边界：含
		{2049, true, costClassMedium},
		{16384, true, costClassMedium}, // 边界：含
		{16385, true, costClassHeavy},
		{32768, true, costClassHeavy}, // 边界：含
		{32769, true, costClassMax},
		{1 << 20, true, costClassMax},
	}
	for _, tc := range cases {
		if got := classifyMaxTokens(tc.n, tc.has); got != tc.want {
			t.Fatalf("classifyMaxTokens(%d, has=%v) = %v, want %v", tc.n, tc.has, got, tc.want)
		}
	}
	// 权重表（设计 §3.2：A=1、B=6、C=11、D=59）。
	if costClassLight.units() != 1 || costClassMedium.units() != 6 ||
		costClassHeavy.units() != 11 || costClassMax.units() != 59 {
		t.Fatalf("units = %d/%d/%d/%d", costClassLight.units(), costClassMedium.units(),
			costClassHeavy.units(), costClassMax.units())
	}
	// 档名/字母/解析。
	for _, tc := range []struct {
		name string
		cls  costClass
		lt   string
	}{
		{"light", costClassLight, "A"},
		{"medium", costClassMedium, "B"},
		{"heavy", costClassHeavy, "C"},
		{"max", costClassMax, "D"},
	} {
		if tc.cls.String() != tc.name || tc.cls.letter() != tc.lt {
			t.Fatalf("%v/%s != %s/%s", tc.cls, tc.cls.letter(), tc.name, tc.lt)
		}
		if got, ok := parseCostClassName(tc.name); !ok || got != tc.cls {
			t.Fatalf("parseCostClassName(%q) = %v(ok=%v)", tc.name, got, ok)
		}
	}
	if _, ok := parseCostClassName("Medium"); ok {
		t.Fatal("档名解析必须精确小写（配置纪律——不做模糊匹配）")
	}
	// 请求体面：max_tokens 优先；max_output_tokens 兜底；畸形体 = 中档。
	if got := classifyRequestCost([]byte(`{"max_tokens":100}`)); got != costClassLight {
		t.Fatalf("max_tokens=100 → %v", got)
	}
	if got := classifyRequestCost([]byte(`{"max_output_tokens":100000}`)); got != costClassMax {
		t.Fatalf("max_output_tokens=100000 → %v", got)
	}
	if got := classifyRequestCost([]byte(`{"max_tokens":100,"max_output_tokens":100000}`)); got != costClassLight {
		t.Fatalf("max_tokens 应优先 → %v", got)
	}
	if got := classifyRequestCost([]byte(`not json`)); got != costClassMedium {
		t.Fatalf("畸形体 → %v（want medium）", got)
	}
	if got := classifyRequestCost([]byte(`{"max_tokens":1000.5}`)); got != costClassLight {
		t.Fatalf("浮点 max_tokens 截断 → %v", got)
	}
}

// TestW58BudgetLedger 台账 check-and-charge：不足即拒（不扣账）；关档恒放行；
// UTC 日界惰性重置。
func TestW58BudgetLedger(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	l := newBudgetLedger(2)
	l.now = func() time.Time { return now }
	if err := l.admit(1); err != nil {
		t.Fatalf("admit 1: %v", err)
	}
	if err := l.admit(1); err != nil {
		t.Fatalf("admit 2: %v", err)
	}
	if err := l.admit(1); err != ErrDailyQuotaExhausted {
		t.Fatalf("第三发 = %v（want ErrDailyQuotaExhausted）", err)
	}
	// 拒后不扣账（used 仍 = 2——重试仍拒，数字不漂移）。
	if used, daily, _ := l.stats(); used != 2 || daily != 2 {
		t.Fatalf("stats = %d/%d（want 2/2）", used, daily)
	}
	// 大档不越预算（used=2 + 59 > 2 → 拒）。
	if err := l.admit(59); err != ErrDailyQuotaExhausted {
		t.Fatalf("大档 = %v（want 拒）", err)
	}
	// UTC 日界：跨日重置。
	now = now.Add(13 * time.Hour) // 次日 01:00 UTC
	if err := l.admit(2); err != nil {
		t.Fatalf("跨日 admit: %v", err)
	}
	if used, _, day := l.stats(); used != 2 || day != "2026-10-17" {
		t.Fatalf("跨日 stats = %d/%s（want 2/2026-10-17）", used, day)
	}
	// 关档（daily<=0）：恒放行不记账。
	off := newBudgetLedger(0)
	for range 10 {
		if err := off.admit(59); err != nil {
			t.Fatalf("关档 admit: %v", err)
		}
	}
	if used, _, _ := off.stats(); used != 0 {
		t.Fatalf("关档不得记账: used=%d", used)
	}
	// nil 接收者安全。
	var nilLedger *budgetLedger
	if err := nilLedger.admit(1); err != nil {
		t.Fatalf("nil admit: %v", err)
	}
}

// TestW58RetryAfterUntilUTCMidnight 距 UTC 0 点整秒（ceil；正整数）。
func TestW58RetryAfterUntilUTCMidnight(t *testing.T) {
	cases := []struct {
		now  time.Time
		want int
	}{
		{time.Date(2026, 10, 16, 23, 59, 59, 0, time.UTC), 1},
		{time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC), 86400},
		{time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC), 43200},
	}
	for _, tc := range cases {
		if got := retryAfterUntilUTCMidnight(tc.now); got != tc.want {
			t.Fatalf("retryAfterUntilUTCMidnight(%s) = %d, want %d", tc.now, got, tc.want)
		}
	}
}

// TestW58ConfigDefaultsAndValidate 新字段缺省/显式 0/校验。
func TestW58ConfigDefaultsAndValidate(t *testing.T) {
	// 缺省（未写字段）。
	c := uiTestConfig()
	c.Public = &PublicSection{Enabled: true}
	if err := c.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	pl := newPublicLimits(c)
	if pl.maxCostClass != costClassMedium {
		t.Fatalf("maxCostClass 缺省 = %v", pl.maxCostClass)
	}
	if pl.maxBodyBytes != DefaultPublicMaxBodyBytes || pl.maxMessages != DefaultPublicMaxMessages ||
		pl.maxMessageChars != DefaultPublicMaxMessageChars {
		t.Fatalf("守卫缺省 = %d/%d/%d", pl.maxBodyBytes, pl.maxMessages, pl.maxMessageChars)
	}
	if pl.ledger == nil {
		t.Fatal("日预算缺省应开（250）")
	}
	if used, daily, _ := pl.ledger.stats(); used != 0 || daily != DefaultPublicDailyBudgetUnits {
		t.Fatalf("台账缺省 = %d/%d", used, daily)
	}
	// 显式 0 = 关（预算关 / 体上限回落 32MB / 守卫关）。
	c2 := uiTestConfig()
	c2.Public = &PublicSection{Enabled: true,
		DailyBudgetUnits: new(0), MaxBodyBytes: new(int64(0)),
		MaxMessages: new(0), MaxMessageChars: new(0)}
	if err := c2.Validate(); err != nil {
		t.Fatalf("validate 0s: %v", err)
	}
	pl2 := newPublicLimits(c2)
	if pl2.ledger != nil || pl2.maxBodyBytes != 0 || pl2.maxMessages != 0 || pl2.maxMessageChars != 0 {
		t.Fatalf("显式 0 应为关: %+v", pl2)
	}
	// 校验：未知档名 / 负值 = 启动报错。
	bad := uiTestConfig()
	bad.Public = &PublicSection{Enabled: true, MaxCostClass: "ultra"}
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "max_cost_class") {
		t.Fatalf("未知档名应报错: %v", err)
	}
	bad2 := uiTestConfig()
	bad2.Public = &PublicSection{Enabled: true, MaxBodyBytes: new(int64(-1))}
	if err := bad2.Validate(); err == nil || !strings.Contains(err.Error(), "max_body_bytes") {
		t.Fatalf("负体上限应报错: %v", err)
	}
	bad3 := uiTestConfig()
	bad3.Public = &PublicSection{Enabled: true, MaxMessages: new(-2)}
	if err := bad3.Validate(); err == nil || !strings.Contains(err.Error(), "max_messages") {
		t.Fatalf("负条数应报错: %v", err)
	}
	bad4 := uiTestConfig()
	bad4.Public = &PublicSection{Enabled: true, DailyBudgetUnits: new(-3)}
	if err := bad4.Validate(); err == nil || !strings.Contains(err.Error(), "daily_budget_units") {
		t.Fatalf("负预算应报错: %v", err)
	}
	bad5 := uiTestConfig()
	bad5.Public = &PublicSection{Enabled: true, MaxMessageChars: new(-4)}
	if err := bad5.Validate(); err == nil || !strings.Contains(err.Error(), "max_message_chars") {
		t.Fatalf("负单条上限应报错: %v", err)
	}
}

// ---------------------------------------------------------------------------
// e2e：日预算耗尽（快速 429；信封形状按入口协议）
// ---------------------------------------------------------------------------

// TestPublicDailyBudgetExhausted429 预算 = 2 units（light 每发 1）：前两发 200，
// 第三发 429 + daily_quota_exhausted + 正整数 Retry-After + no-store；信封按
// 入口协议（openai error.code / claude message token）；ring + 计数可见。
func TestPublicDailyBudgetExhausted429(t *testing.T) {
	s, base := w58ChatCfg(t, &PublicSection{Enabled: true, DailyBudgetUnits: new(2)})

	r1 := w58PostChat(t, base, w58LightBody, nil)
	if r1.code != 200 || !strings.Contains(string(r1.body), "Hello ") {
		t.Fatalf("r1 = %d %s", r1.code, r1.body)
	}
	r2 := w58PostChat(t, base, w58LightBody, nil)
	if r2.code != 200 {
		t.Fatalf("r2 = %d %s", r2.code, r2.body)
	}
	// 第三发：预算尽 → 快速 429（未触上游）。
	r3 := w58PostChat(t, base, w58LightBody, nil)
	if r3.code != http.StatusTooManyRequests {
		t.Fatalf("r3 = %d %s（want 429）", r3.code, r3.body)
	}
	if v, err := strconv.Atoi(r3.retryAfter); err != nil || v < 1 {
		t.Fatalf("Retry-After = %q（want 正整数）", r3.retryAfter)
	}
	if r3.cacheControl != "no-store" {
		t.Fatalf("Cache-Control = %q（want no-store）", r3.cacheControl)
	}
	if !strings.Contains(string(r3.body), "daily_quota_exhausted") {
		t.Fatalf("信封缺 daily_quota_exhausted: %s", r3.body)
	}
	// openai 面：error.code = daily_quota_exhausted（信封形状逐字段）。
	var env struct {
		Error struct {
			Type    string `json:"type"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(r3.body, &env); err != nil {
		t.Fatalf("openai 信封解码: %v\n%s", err, r3.body)
	}
	if env.Error.Type != "rate_limit_error" || env.Error.Code != "daily_quota_exhausted" {
		t.Fatalf("openai 信封 = %+v", env.Error)
	}
	// claude 面（/v1/messages）：message token（claude 信封无 code 字段）。
	r4 := w58PostPath(t, base+"/v1/messages", w58LightBody)
	if r4.code != http.StatusTooManyRequests || !strings.Contains(string(r4.body), "daily_quota_exhausted") {
		t.Fatalf("claude 面 = %d %s", r4.code, r4.body)
	}
	if !strings.Contains(string(r4.body), `"type":"error"`) {
		t.Fatalf("claude 信封形状: %s", r4.body)
	}
	// ring + 计数（拒绝面双落点）。
	if d := s.Diagnostics(); d.PublicQuotaRejected != 2 {
		t.Fatalf("PublicQuotaRejected = %d（want 2）", d.PublicQuotaRejected)
	}
	if !w58RingHasKind(s, adminLogQuotaExhausted) {
		t.Fatalf("ring 缺 quota_exhausted 事件: %v", w58RingKinds(s))
	}
	// 台账：两发扣账（1+1），拒的不扣。
	if used, daily, _ := s.publicLimits.ledger.stats(); used != 2 || daily != 2 {
		t.Fatalf("台账 = %d/%d（want 2/2）", used, daily)
	}
}

// ---------------------------------------------------------------------------
// e2e：档位控制（400 策略拒绝；上限可配）
// ---------------------------------------------------------------------------

// TestPublicCostClassRejected400 缺省 medium：heavy/max 档 400；提档 heavy 放行
// heavy、仍拒 max；提档 max 全放行。
func TestPublicCostClassRejected400(t *testing.T) {
	heavyBody := `{"model":"claude-sonnet-5-5","max_tokens":20000,"stream":true,"messages":[{"role":"user","content":"hi"}]}`
	maxBody := `{"model":"claude-sonnet-5-5","max_tokens":40000,"stream":true,"messages":[{"role":"user","content":"hi"}]}`

	t.Run("default_medium_rejects_heavy_and_max", func(t *testing.T) {
		s, base := w58ChatCfg(t, &PublicSection{Enabled: true})
		r := w58PostChat(t, base, heavyBody, nil)
		if r.code != http.StatusBadRequest || !strings.Contains(string(r.body), "cost class") ||
			!strings.Contains(string(r.body), "(C)") || !strings.Contains(string(r.body), "(B)") {
			t.Fatalf("heavy 档 = %d %s（want 400 档位拒绝）", r.code, r.body)
		}
		r2 := w58PostChat(t, base, maxBody, nil)
		if r2.code != http.StatusBadRequest || !strings.Contains(string(r2.body), "cost class") {
			t.Fatalf("max 档 = %d %s（want 400）", r2.code, r2.body)
		}
		// light 档照常放行。
		r3 := w58PostChat(t, base, w58LightBody, nil)
		if r3.code != 200 {
			t.Fatalf("light 档 = %d %s", r3.code, r3.body)
		}
		if d := s.Diagnostics(); d.PublicClassRejected != 2 {
			t.Fatalf("PublicClassRejected = %d（want 2）", d.PublicClassRejected)
		}
	})

	t.Run("raise_to_heavy_allows_heavy_rejects_max", func(t *testing.T) {
		_, base := w58ChatCfg(t, &PublicSection{Enabled: true, MaxCostClass: "heavy"})
		r := w58PostChat(t, base, heavyBody, nil)
		if r.code != 200 {
			t.Fatalf("heavy（上限 heavy）= %d %s", r.code, r.body)
		}
		r2 := w58PostChat(t, base, maxBody, nil)
		if r2.code != http.StatusBadRequest {
			t.Fatalf("max（上限 heavy）= %d %s（want 400）", r2.code, r2.body)
		}
	})

	t.Run("raise_to_max_allows_max", func(t *testing.T) {
		_, base := w58ChatCfg(t, &PublicSection{Enabled: true, MaxCostClass: "max"})
		r := w58PostChat(t, base, maxBody, nil)
		if r.code != 200 {
			t.Fatalf("max（上限 max）= %d %s", r.code, r.body)
		}
	})
}

// ---------------------------------------------------------------------------
// e2e：体上限 / 结构守卫（413 + 可配 + 显式 0 关）
// ---------------------------------------------------------------------------

// TestPublicBodyAndStructureGuards413 三守卫各自可配：超限 413（按入口协议
// request_too_large）；显式 0 = 关（放行到上游）。
func TestPublicBodyAndStructureGuards413(t *testing.T) {
	t.Run("max_body_bytes", func(t *testing.T) {
		_, base := w58ChatCfg(t, &PublicSection{Enabled: true, MaxBodyBytes: new(int64(2048))})
		big := w58BigChatBody(1, 3000) // ~3KB 体
		r := w58PostChat(t, base, big, nil)
		if r.code != http.StatusRequestEntityTooLarge || !strings.Contains(string(r.body), "request_too_large") ||
			!strings.Contains(string(r.body), "public.max_body_bytes") {
			t.Fatalf("体超限 = %d %s（want 413）", r.code, r.body)
		}
		// 显式 0 = 用全局 32MB（放行）。
		_, base2 := w58ChatCfg(t, &PublicSection{Enabled: true, MaxBodyBytes: new(int64(0))})
		r2 := w58PostChat(t, base2, big, nil)
		if r2.code != 200 {
			t.Fatalf("显式 0 应放行 = %d %s", r2.code, r2.body)
		}
	})

	t.Run("max_messages", func(t *testing.T) {
		_, base := w58ChatCfg(t, &PublicSection{Enabled: true, MaxMessages: new(5)})
		big := w58BigChatBody(10, 2)
		r := w58PostChat(t, base, big, nil)
		if r.code != http.StatusRequestEntityTooLarge || !strings.Contains(string(r.body), "too many messages") ||
			!strings.Contains(string(r.body), "public.max_messages") {
			t.Fatalf("条数超限 = %d %s（want 413）", r.code, r.body)
		}
		// 边界：恰为上限放行。
		_, base2 := w58ChatCfg(t, &PublicSection{Enabled: true, MaxMessages: new(10)})
		r2 := w58PostChat(t, base2, big, nil)
		if r2.code != 200 {
			t.Fatalf("恰为上限应放行 = %d %s", r2.code, r2.body)
		}
		// 显式 0 = 关。
		_, base3 := w58ChatCfg(t, &PublicSection{Enabled: true, MaxMessages: new(0)})
		r3 := w58PostChat(t, base3, big, nil)
		if r3.code != 200 {
			t.Fatalf("显式 0 应放行 = %d %s", r3.code, r3.body)
		}
	})

	t.Run("max_message_chars", func(t *testing.T) {
		_, base := w58ChatCfg(t, &PublicSection{Enabled: true, MaxMessageChars: new(16)})
		big := w58BigChatBody(1, 64) // content raw > 16B
		r := w58PostChat(t, base, big, nil)
		if r.code != http.StatusRequestEntityTooLarge || !strings.Contains(string(r.body), "public.max_message_chars") {
			t.Fatalf("单条超限 = %d %s（want 413）", r.code, r.body)
		}
		// 显式 0 = 关。
		_, base2 := w58ChatCfg(t, &PublicSection{Enabled: true, MaxMessageChars: new(0)})
		r2 := w58PostChat(t, base2, big, nil)
		if r2.code != 200 {
			t.Fatalf("显式 0 应放行 = %d %s", r2.code, r2.body)
		}
	})

	t.Run("responses_input_array", func(t *testing.T) {
		// responses 面 `input` 数组同受条数守卫（消息数组的 responses 形态）。
		_, base := w58ChatCfg(t, &PublicSection{Enabled: true, MaxMessages: new(5)})
		var items []string
		for range 10 {
			items = append(items, `{"role":"user","content":"hi"}`)
		}
		body := `{"model":"claude-sonnet-5-5","input":[` + strings.Join(items, ",") + `]}`
		r := w58PostPath(t, base+"/v1/responses", body)
		if r.code != http.StatusRequestEntityTooLarge || !strings.Contains(string(r.body), "too many input") {
			t.Fatalf("responses input 超限 = %d %s（want 413）", r.code, r.body)
		}
	})
}

// ---------------------------------------------------------------------------
// e2e：硬门——public.enabled=false 零回归（新面全关）
// ---------------------------------------------------------------------------

// TestPublicDisabledZeroRegressionW58 缺省（无 public 段）：同一请求同时携带
// 超 1MB 体 + 300 条 messages + max_tokens=100000（三个新面若挂载必拒）——
// 全链照常 200；无会话 cookie；公开面结构与计数全空。
func TestPublicDisabledZeroRegressionW58(t *testing.T) {
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		claudeTextStream(w)
	})
	cfg := failoverCfg(anthUp("good", up.srv.URL)) // 无 Public 段
	s, srv := startServiceWith(t, cfg)

	// 构造：300 条 messages（首条 1.3MB 内容）+ max_tokens=100000。
	var msgs []string
	msgs = append(msgs, fmt.Sprintf(`{"role":"user","content":%q}`, strings.Repeat("a", 1300<<10)))
	for i := 1; i < 300; i++ {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		msgs = append(msgs, fmt.Sprintf(`{"role":%q,"content":"x"}`, role))
	}
	body := `{"model":"claude-sonnet-5-5","max_tokens":100000,"stream":true,"messages":[` + strings.Join(msgs, ",") + `]}`

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(raw), "Hello ") {
		t.Fatalf("零回归请求 = %d %s", resp.StatusCode, raw)
	}
	if sc := resp.Header.Get("Set-Cookie"); sc != "" {
		t.Fatalf("public 关不得签发会话 cookie: %q", sc)
	}
	// 内部结构：新面全不挂载。
	if s.publicLimits != nil || s.limiter != nil || s.sessions != nil {
		t.Fatalf("public 关：publicLimits/limiter/sessions 应全 nil")
	}
	d := s.Diagnostics()
	if d.PublicSessionsCreated != 0 || d.PublicRateLimited != 0 || d.PublicQuotaRejected != 0 ||
		d.PublicCapacityRejected != 0 || d.PublicClassRejected != 0 {
		t.Fatalf("public 关计数应全 0: %+v", d)
	}
	if len(w58RingKinds(s)) != 0 {
		t.Fatalf("public 关 ring 不应有新事件: %v", w58RingKinds(s))
	}
}

// ---------------------------------------------------------------------------
// e2e：审计——attempts sid 前缀 + key_id；ring session_create + 计数
// ---------------------------------------------------------------------------

// TestPublicAttemptsSidKeyID attempts 日志行带 sid 前缀（8 字符——全文不入日志）
// + key_id=sha8（gated）；ring 有 session_create；计数可见。零凭据字节。
func TestPublicAttemptsSidKeyID(t *testing.T) {
	t.Run("open_mode_sid_prefix", func(t *testing.T) {
		lb := captureServiceLogs(t)
		s, base := w58ChatCfg(t, &PublicSection{Enabled: true})

		req, _ := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(w58LightBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("请求 = %d %s", resp.StatusCode, raw)
		}
		var sid string
		for _, c := range resp.Cookies() {
			if c.Name == sessionCookieName {
				sid = c.Value
			}
		}
		if len(sid) < 16 {
			t.Fatalf("应签发会话 cookie: %q", sid)
		}
		prefix := sid[:8]
		waitForLogContains(t, lb, `"sid":"`+prefix+`"`)
		if strings.Contains(lb.String(), sid) {
			t.Fatalf("attempts 日志不得含完整 sid（只前缀）:\n%s", lb.String())
		}
		if d := s.Diagnostics(); d.PublicSessionsCreated != 1 {
			t.Fatalf("PublicSessionsCreated = %d（want 1）", d.PublicSessionsCreated)
		}
		if !w58RingHasKind(s, adminLogSessionCreate) {
			t.Fatalf("ring 缺 session_create: %v", w58RingKinds(s))
		}
	})

	t.Run("gated_key_id_sha8", func(t *testing.T) {
		lb := captureServiceLogs(t)
		const key = "sk-w58-attempts-key"
		s, base := w58ChatCfg(t, &PublicSection{Enabled: true, AccessMode: "gated", AccessKeys: []string{key}})
		_ = s

		sum := sha256.Sum256([]byte(key))
		wantKeyID := hex.EncodeToString(sum[:])[:8]

		r := w58PostChat(t, base, w58LightBody, map[string]string{"X-Access-Key": key})
		if r.code != 200 {
			t.Fatalf("gated 带 key = %d %s", r.code, r.body)
		}
		waitForLogContains(t, lb, `"key_id":"`+wantKeyID+`"`)
		if strings.Contains(lb.String(), key) {
			t.Fatalf("日志不得含 key 原文:\n%s", lb.String())
		}
		if !w58RingHasKind(s, adminLogSessionCreate) {
			t.Fatalf("ring 缺 session_create: %v", w58RingKinds(s))
		}
	})
}

// ---------------------------------------------------------------------------
// e2e：容量耗尽（429 capacity_exhausted；池 busy 快速失败面）
// ---------------------------------------------------------------------------

// TestPublicCapacityExhausted429 单凭据池同号在飞（busy）→ 第二发 429
// capacity_exhausted + 正整数 Retry-After + message 保留 upstream_credentials_exhausted
// 检索 token；ring + 计数可见。
func TestPublicCapacityExhausted429(t *testing.T) {
	m := newCW57Mock(t)
	m.compBlock = make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(m.compBlock) }) }
	t.Cleanup(release)

	cfg := cfgClaudeWebPool(m.srv.URL, "sk-ant-sid01-w58-key-0")
	cfg.Public = &PublicSection{Enabled: true, CredentialMinGapSeconds: new(0)}
	s, srv := startServiceWith(t, cfg)

	ch1 := make(chan chatRes, 1)
	go func() { ch1 <- postChatRes(srv.URL) }()
	<-m.started // 第一发 completion 已进入（busy 已置）

	r2 := postChatRes(srv.URL)
	if r2.err != nil {
		t.Fatalf("r2 err = %v", r2.err)
	}
	if r2.code != http.StatusTooManyRequests {
		t.Fatalf("r2 = %d %s（want 429 capacity_exhausted）", r2.code, r2.body)
	}
	if v, err := strconv.Atoi(r2.retryAfter); err != nil || v < 1 {
		t.Fatalf("Retry-After = %q（want 正整数）", r2.retryAfter)
	}
	if r2.cacheControl != "no-store" {
		t.Fatalf("Cache-Control = %q", r2.cacheControl)
	}
	if !strings.Contains(string(r2.body), "capacity_exhausted") ||
		!strings.Contains(string(r2.body), "upstream_credentials_exhausted") {
		t.Fatalf("信封 token 面: %s", r2.body)
	}
	var env struct {
		Error struct {
			Type string `json:"type"`
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(r2.body, &env); err != nil || env.Error.Code != "capacity_exhausted" {
		t.Fatalf("openai 信封 code = %+v（err=%v）", env.Error, err)
	}
	if d := s.Diagnostics(); d.PublicCapacityRejected != 1 {
		t.Fatalf("PublicCapacityRejected = %d（want 1）", d.PublicCapacityRejected)
	}
	if !w58RingHasKind(s, adminLogCapacityExhausted) {
		t.Fatalf("ring 缺 capacity_exhausted: %v", w58RingKinds(s))
	}

	// 放行第一发 → 200。
	release()
	r1 := <-ch1
	if r1.err != nil {
		t.Fatalf("r1 err = %v", r1.err)
	}
	assertPONG(t, r1.code, r1.body)
}

// ---------------------------------------------------------------------------
// e2e：全局闸拒绝词表 rate_limited（ring + 计数）
// ---------------------------------------------------------------------------

// TestPublicRateLimitedEnvelope 队列满 429 带 `rate_limited` 词（type 保持
// rate_limit_error）；ring + 计数可见。
func TestPublicRateLimitedEnvelope(t *testing.T) {
	up, started, release := blockingUpstream(t)
	cfg := failoverCfg(anthUp("good", up.srv.URL))
	cfg.Public = &PublicSection{Enabled: true, MaxInflight: 1, QueueSize: 1, QueueTimeoutSeconds: 30}
	s, srv := startServiceWith(t, cfg)
	t.Cleanup(release)

	ch1 := make(chan chatRes, 1)
	go func() { ch1 <- postChatStreamRes(srv.URL) }()
	<-started
	ch2 := make(chan chatRes, 1)
	go func() { ch2 <- postChatStreamRes(srv.URL) }()
	waitFor(t, 5*time.Second, func() bool { _, q := s.limiter.stats(); return q == 1 })

	r3 := postChatStreamRes(srv.URL)
	if r3.code != http.StatusTooManyRequests {
		t.Fatalf("r3 = %d %s（want 429）", r3.code, r3.body)
	}
	if !strings.Contains(string(r3.body), "rate_limited") || !strings.Contains(string(r3.body), "rate_limit_error") {
		t.Fatalf("信封词表: %s", r3.body)
	}
	if v, err := strconv.Atoi(r3.retryAfter); err != nil || v < 1 {
		t.Fatalf("Retry-After = %q", r3.retryAfter)
	}
	if d := s.Diagnostics(); d.PublicRateLimited < 1 {
		t.Fatalf("PublicRateLimited = %d（want >=1）", d.PublicRateLimited)
	}
	if !w58RingHasKind(s, adminLogRateLimited) {
		t.Fatalf("ring 缺 rate_limited: %v", w58RingKinds(s))
	}

	release()
	r1 := <-ch1
	r2 := <-ch2
	if r1.code != 200 || r2.code != 200 {
		t.Fatalf("r1/r2 = %d/%d", r1.code, r2.code)
	}
}
