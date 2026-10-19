package service

// limits_test.go —— P0-4 全局并发闸 + 单号 inflight=1 / min-gap 测试
//（board A2A-L19-W57-1）：
// Limiter 单元（获取/释放/队列满/超时/ctx 取消/槽位转交不放大）、池 busy 单元、
// min-gap 单元、e2e（队列满 429 正整数 Retry-After / 排队超时 429 / mock 上游
// 同号零并发 / prepare 失败释放 busy）。
//
// 反证锚点（回退即红——原样输出见 tmp/lanes/W57-a/proofs/）：
//   - 撤掉 selectCredential 的 busy 跳过 → TestClaudeWebPoolSameCredentialNoConcurrency
//     红（mock 记录同键并发峰值 = 2）且 TestCredentialPoolBusySkipAndRelease 红；
//   - 撤掉 min-gap 校验 → TestCredentialPoolMinGap 红；
//   - 撤掉 Acquire 的队列满判定（无限排队）→ TestLimiterQueueFullAndHandoff 与
//     TestPublicLimitsQueueFull429 红；
//   - 撤掉 serve 的 limiter 接线 → TestPublicLimitsQueueFull429 红（第三发 200 非 429）；
//   - 撤掉 claudeWebPrepare 失败面的 Release → TestClaudeWebPoolBusyReleaseOnPrepareFailure 红。

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

// chatRes 一次聊天请求的结果（异步安全——绝不在 goroutine 里 t.Fatalf）。
type chatRes struct {
	code         int
	body         []byte
	retryAfter   string
	cacheControl string
	err          error
}

// postChatRes 非流式聊天请求（异步安全）。
func postChatRes(base string) chatRes { return doChatRes(base, false) }

// postChatStreamRes 流式聊天请求（异步安全）。
func postChatStreamRes(base string) chatRes { return doChatRes(base, true) }

func doChatRes(base string, stream bool) chatRes {
	body := `{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}]}`
	if stream {
		body = `{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}],"stream":true}`
	}
	req, err := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		return chatRes{err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return chatRes{err: err}
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

// blockingUpstream 假 anthropic 上游：第一发挂住（发信号 + 等放行；客户端断开/
// 30s 兜底），其余直写标准文本流。release 幂等（测试应注册 t.Cleanup——断言
// 失败路径也放行，防清理期挂死）。
func blockingUpstream(t *testing.T) (*claudeUpstream, <-chan struct{}, func()) {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	started := make(chan struct{}, 1)
	var n atomic.Int32
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			started <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
			case <-time.After(30 * time.Second):
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		claudeTextStream(w)
	})
	return up, started, func() { once.Do(func() { close(release) }) }
}

// ---------------------------------------------------------------------------
// Limiter 单元
// ---------------------------------------------------------------------------

func TestLimiterAcquireReleaseBasics(t *testing.T) {
	ctx := context.Background()
	l := NewLimiter(1, 4, time.Second)
	if err := l.Acquire(ctx); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if inf, q := l.stats(); inf != 1 || q != 0 {
		t.Fatalf("stats = %d/%d（want 1/0）", inf, q)
	}
	l.Release()
	if inf, _ := l.stats(); inf != 0 {
		t.Fatalf("release 后 inflight = %d", inf)
	}
	// 释放后立即可再取。
	if err := l.Acquire(ctx); err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	l.Release()
	// 防御：多余 Release 不得把 inflight 打成负。
	l.Release()
	if inf, _ := l.stats(); inf != 0 {
		t.Fatalf("多余 release 后 inflight = %d", inf)
	}
}

func TestLimiterQueueFullAndHandoff(t *testing.T) {
	ctx := context.Background()
	l := NewLimiter(1, 1, 5*time.Second)
	if err := l.Acquire(ctx); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	res := make(chan error, 1)
	go func() { res <- l.Acquire(ctx) }()
	waitFor(t, 5*time.Second, func() bool { _, q := l.stats(); return q == 1 })
	// 队列满 → 立即 ErrQueueFull（不排队）。
	if err := l.Acquire(ctx); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("第三发 = %v（want ErrQueueFull）", err)
	}
	// 释放 → 槽位直接转交队首（inflight 不变——绝不「先减后加」放大）。
	l.Release()
	if err := <-res; err != nil {
		t.Fatalf("队首 acquire: %v", err)
	}
	if inf, q := l.stats(); inf != 1 || q != 0 {
		t.Fatalf("转交后 stats = %d/%d（want 1/0）", inf, q)
	}
	l.Release()
	if inf, _ := l.stats(); inf != 0 {
		t.Fatalf("清空后 inflight = %d", inf)
	}
}

func TestLimiterQueueTimeout(t *testing.T) {
	ctx := context.Background()
	l := NewLimiter(1, 4, 60*time.Millisecond)
	if err := l.Acquire(ctx); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	start := time.Now()
	err := l.Acquire(ctx)
	if !errors.Is(err, ErrQueueTimeout) {
		t.Fatalf("err = %v（want ErrQueueTimeout）", err)
	}
	if el := time.Since(start); el < 40*time.Millisecond {
		t.Fatalf("不得立即超时: %v", el)
	}
	l.Release()
}

func TestLimiterContextCancel(t *testing.T) {
	ctx := context.Background()
	l := NewLimiter(1, 4, time.Hour)
	if err := l.Acquire(ctx); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- l.Acquire(cctx) }()
	waitFor(t, 5*time.Second, func() bool { _, q := l.stats(); return q == 1 })
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v（want context.Canceled）", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ctx 取消后 Acquire 未返回")
	}
	if _, q := l.stats(); q != 0 {
		t.Fatalf("取消后队列应清空: %d", q)
	}
	l.Release()
	if inf, _ := l.stats(); inf != 0 {
		t.Fatalf("inflight = %d", inf)
	}
}

// ---------------------------------------------------------------------------
// 池单元：busy / min-gap（gate 开 = public.enabled）
// ---------------------------------------------------------------------------

func TestCredentialPoolBusySkipAndRelease(t *testing.T) {
	base := time.Unix(20000, 0)

	// 单号：选中即 busy；busy 中不可再选；Release 后可再选。
	p := poolForTest("a")
	p.configureGate(true, 0)
	if i, ok := p.selectCredential(base); !ok || i != 0 {
		t.Fatalf("首发 = %d(ok=%v)", i, ok)
	}
	if _, ok := p.selectCredential(base.Add(time.Second)); ok {
		t.Fatal("busy 中不得再选中")
	}
	p.Release(0)
	if i, ok := p.selectCredential(base.Add(2 * time.Second)); !ok || i != 0 {
		t.Fatalf("Release 后 = %d(ok=%v)", i, ok)
	}
	p.Release(0)

	// 双号：busy 跳过 + LRU 轮转。
	p2 := poolForTest("a", "b")
	p2.configureGate(true, 0)
	if i, _ := p2.selectCredential(base); i != 0 {
		t.Fatalf("双号首发 = %d", i)
	}
	if i, _ := p2.selectCredential(base); i != 1 {
		t.Fatalf("0 busy 应选 1，得到 %d", i)
	}
	if _, ok := p2.selectCredential(base); ok {
		t.Fatal("全 busy 不得选中")
	}
	p2.Release(0)
	if i, _ := p2.selectCredential(base); i != 0 {
		t.Fatalf("Release(0) 后 = %d", i)
	}
	p2.Release(0)
	p2.Release(1)

	// gate 关（缺省形态）：busy/min-gap 全不生效——行为与引入前一致。
	p3 := poolForTest("a")
	p3.configureGate(false, 300*time.Second)
	if i, _ := p3.selectCredential(base); i != 0 {
		t.Fatalf("gate 关首发 = %d", i)
	}
	if i, _ := p3.selectCredential(base); i != 0 {
		t.Fatalf("gate 关：无 busy 概念（得到 %d）", i)
	}
	p3.Release(0) // no-op
	if i, _ := p3.selectCredential(base); i != 0 {
		t.Fatalf("gate 关 Release 后 = %d", i)
	}
	// 越界/未标记 Release = 防御性 no-op。
	p3.Release(-1)
	p3.Release(9)
}

func TestCredentialPoolMinGap(t *testing.T) {
	base := time.Unix(21000, 0)

	// 双号：min-gap 未到的候选被跳过；全被跳过 → 耗尽。
	p := poolForTest("a", "b")
	p.configureGate(true, 300*time.Second)
	if i, ok := p.selectCredential(base); !ok || i != 0 {
		t.Fatalf("首发 = %d(ok=%v)", i, ok)
	}
	p.Release(0)
	if i, ok := p.selectCredential(base.Add(time.Second)); !ok || i != 1 {
		t.Fatalf("1s 后应选 1（0 在 gap 内），得到 %d(ok=%v)", i, ok)
	}
	p.Release(1)
	if _, ok := p.selectCredential(base.Add(2 * time.Second)); ok {
		t.Fatal("两号均在 min-gap 内不得选中")
	}
	if i, ok := p.selectCredential(base.Add(301 * time.Second)); !ok || i != 0 {
		t.Fatalf("301s 后应回到 0，得到 %d(ok=%v)", i, ok)
	}

	// 单号 + 耗尽信息面：earliest release 计 min-gap（300s - 10s = 4m50s）。
	p2 := poolForTest("a")
	p2.configureGate(true, 300*time.Second)
	p2.selectCredential(base)
	p2.Release(0)
	if _, ok := p2.selectCredential(base.Add(10 * time.Second)); ok {
		t.Fatal("min-gap 内不得选中")
	}
	err := p2.exhaustedError(base.Add(10 * time.Second))
	if err == nil || !strings.Contains(err.Error(), "upstream_credentials_exhausted") {
		t.Fatalf("耗尽错误 = %v", err)
	}
	if !strings.Contains(err.Error(), "4m50s") {
		t.Fatalf("earliest release 应计 min-gap（want 4m50s）: %v", err)
	}

	// gate 关：minGap 参数不生效（行为与引入前一致）。
	p3 := poolForTest("a")
	p3.configureGate(false, 300*time.Second)
	p3.selectCredential(base)
	if i, ok := p3.selectCredential(base.Add(time.Second)); !ok || i != 0 {
		t.Fatalf("gate 关不得应用 min-gap: %d(ok=%v)", i, ok)
	}
}

// ---------------------------------------------------------------------------
// e2e：队列满 / 排队超时（429 + 正整数 Retry-After）
// ---------------------------------------------------------------------------

// TestPublicLimitsQueueFull429 第 7 个 inflight 进队列；队列满 → 立即 429 +
// 正整数 Retry-After（§3/§4 验收锚）；放行后队列串行消化。
func TestPublicLimitsQueueFull429(t *testing.T) {
	up, started, release := blockingUpstream(t)
	cfg := failoverCfg(anthUp("good", up.srv.URL))
	cfg.Public = &PublicSection{Enabled: true, MaxInflight: 1, QueueSize: 1, QueueTimeoutSeconds: 30}
	s, srv := startServiceWith(t, cfg)
	// release 在 srv.Close 之后注册（LIFO 先跑）——断言失败路径先放行上游，
	// 再关服务，防清理期挂死。
	t.Cleanup(release)

	ch1 := make(chan chatRes, 1)
	go func() { ch1 <- postChatStreamRes(srv.URL) }()
	<-started // 第一发已挂住上游（持有槽位）
	ch2 := make(chan chatRes, 1)
	go func() { ch2 <- postChatStreamRes(srv.URL) }()
	waitFor(t, 5*time.Second, func() bool { _, q := s.limiter.stats(); return q == 1 })

	// 第三发：队列满 → 立即 429 + 正整数 Retry-After + 按入口协议信封。
	r3 := postChatStreamRes(srv.URL)
	if r3.err != nil {
		t.Fatalf("r3 err = %v", r3.err)
	}
	if r3.code != http.StatusTooManyRequests {
		t.Fatalf("r3 = %d %s（want 429）", r3.code, r3.body)
	}
	if v, err := strconv.Atoi(r3.retryAfter); err != nil || v < 1 {
		t.Fatalf("Retry-After = %q（want 正整数）", r3.retryAfter)
	}
	if !strings.Contains(string(r3.body), "rate_limit_error") {
		t.Fatalf("信封缺 rate_limit_error: %s", r3.body)
	}
	if r3.cacheControl != "no-store" {
		t.Fatalf("Cache-Control = %q（want no-store）", r3.cacheControl)
	}

	// 放行：前两发依次完成；闸清零。
	release()
	r1 := <-ch1
	r2 := <-ch2
	if r1.code != 200 || !strings.Contains(string(r1.body), "Hello ") {
		t.Fatalf("r1 = %d %s", r1.code, r1.body)
	}
	if r2.code != 200 || !strings.Contains(string(r2.body), "Hello ") {
		t.Fatalf("r2 = %d %s", r2.code, r2.body)
	}
	waitFor(t, 5*time.Second, func() bool { inf, q := s.limiter.stats(); return inf == 0 && q == 0 })
}

// TestPublicLimitsQueueTimeout429 排队等待超时（queue_timeout_seconds）→ 429
// （不无限等待）。
func TestPublicLimitsQueueTimeout429(t *testing.T) {
	up, started, release := blockingUpstream(t)
	cfg := failoverCfg(anthUp("good", up.srv.URL))
	cfg.Public = &PublicSection{Enabled: true, MaxInflight: 1, QueueSize: 4, QueueTimeoutSeconds: 1}
	s, srv := startServiceWith(t, cfg)
	t.Cleanup(release) // 同上：先放行上游再关服务

	ch1 := make(chan chatRes, 1)
	go func() { ch1 <- postChatStreamRes(srv.URL) }()
	<-started

	start := time.Now()
	r2 := postChatStreamRes(srv.URL) // 排队 → ~1s 超时 → 429
	elapsed := time.Since(start)
	if r2.err != nil {
		t.Fatalf("r2 err = %v", r2.err)
	}
	if r2.code != http.StatusTooManyRequests {
		t.Fatalf("r2 = %d %s（want 429 超时）", r2.code, r2.body)
	}
	if v, err := strconv.Atoi(r2.retryAfter); err != nil || v < 1 {
		t.Fatalf("Retry-After = %q（want 正整数）", r2.retryAfter)
	}
	if elapsed < 900*time.Millisecond {
		t.Fatalf("不得立即超时: %v", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("超时过久: %v", elapsed)
	}

	release()
	r1 := <-ch1
	if r1.code != 200 {
		t.Fatalf("r1 = %d %s", r1.code, r1.body)
	}
	waitFor(t, 5*time.Second, func() bool { inf, q := s.limiter.stats(); return inf == 0 && q == 0 })
}

// ---------------------------------------------------------------------------
// e2e：claude_web 池同号零并发 / prepare 失败释放
// ---------------------------------------------------------------------------

// cw57Mock 假 claude_web 上游（P0-4 测试装置）：org 发现（可配「首调失败」）/
// 会话创建 / completion（可阻塞 + 按 cookie 键追踪并发峰值）/ DELETE。
type cw57Mock struct {
	t   *testing.T
	srv *httptest.Server

	mu       sync.Mutex
	cur      map[string]int
	maxSeen  map[string]int
	orgCalls int

	orgFirstFail bool
	compBlock    chan struct{} // 非 nil：completion 进入后阻塞（放行/断开/10s 兜底）
	started      chan struct{} // 每次 completion 进入发一个信号
	streamBody   string
}

func newCW57Mock(t *testing.T) *cw57Mock {
	t.Helper()
	m := &cw57Mock{
		t:          t,
		cur:        map[string]int{},
		maxSeen:    map[string]int{},
		started:    make(chan struct{}, 16),
		streamBody: claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt"),
	}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/organizations":
			m.mu.Lock()
			m.orgCalls++
			fail := m.orgFirstFail && m.orgCalls == 1
			m.mu.Unlock()
			if fail {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `[{"id":1,"uuid":%q,"name":"Test","rate_limit_tier":"default_claude_ai"}]`, claudeWebOrgUUID)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/chat_conversations"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, `{"uuid":%q}`, claudeWebConvUUID)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/completion"):
			key := ""
			if c := r.Header.Get("Cookie"); c != "" {
				if v, ok := strings.CutPrefix(c, "sessionKey="); ok {
					key = v
				}
			}
			m.mu.Lock()
			m.cur[key]++
			if m.cur[key] > m.maxSeen[key] {
				m.maxSeen[key] = m.cur[key]
			}
			m.mu.Unlock()
			select {
			case m.started <- struct{}{}:
			default:
			}
			if m.compBlock != nil {
				select {
				case <-m.compBlock:
				case <-r.Context().Done():
				case <-time.After(10 * time.Second):
				}
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, m.streamBody)
			m.mu.Lock()
			m.cur[key]--
			m.mu.Unlock()
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(m.srv.Close)
	return m
}

// maxConcurrent 该键的并发峰值（completion 在飞数）。
func (m *cw57Mock) maxConcurrent(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maxSeen[key]
}

// TestClaudeWebPoolSameCredentialNoConcurrency mock 上游断言同号零并发：单凭据
// 池两发并发请求——第二发因 busy 走耗尽语义快速失败；mock 侧同键并发峰值恒 1。
func TestClaudeWebPoolSameCredentialNoConcurrency(t *testing.T) {
	m := newCW57Mock(t)
	m.compBlock = make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(m.compBlock) }) }
	t.Cleanup(release) // 断言失败路径也放行（防 mock 挂死）

	zero := 0
	cfg := cfgClaudeWebPool(m.srv.URL, "sk-ant-sid01-w57-key-0")
	cfg.Public = &PublicSection{Enabled: true, CredentialMinGapSeconds: &zero}
	_, srv := startServiceWith(t, cfg)

	ch1 := make(chan chatRes, 1)
	go func() { ch1 <- postChatRes(srv.URL) }()
	<-m.started // 第一发 completion 已进入（busy 已置）

	// 第二发：同号在飞 → 耗尽语义快速失败（绝不第二个并发 completion）。
	r2 := postChatRes(srv.URL)
	if r2.err != nil {
		t.Fatalf("r2 err = %v", r2.err)
	}
	if r2.code == 200 || !strings.Contains(string(r2.body), "upstream_credentials_exhausted") {
		t.Fatalf("r2 = %d %s（want 非 200 + 耗尽语义）", r2.code, r2.body)
	}
	// mock 侧断言：同键并发峰值 = 1（同号零并发复用）。
	if got := m.maxConcurrent("sk-ant-sid01-w57-key-0"); got != 1 {
		t.Fatalf("同键并发峰值 = %d（want 1——单号 inflight=1）", got)
	}

	// 放行第一发 → 200 PONG。
	release()
	r1 := <-ch1
	if r1.err != nil {
		t.Fatalf("r1 err = %v", r1.err)
	}
	assertPONG(t, r1.code, r1.body)
}

// TestClaudeWebPoolBusyReleaseOnPrepareFailure 准备链失败（非换号类）→ busy 随
// 该凭据尝试收尾释放：后续请求不被残留 busy 阻塞（反证：撤掉 Release 则第二发
// 耗尽 502）。
func TestClaudeWebPoolBusyReleaseOnPrepareFailure(t *testing.T) {
	m := newCW57Mock(t)
	m.orgFirstFail = true
	zero := 0
	cfg := cfgClaudeWebPool(m.srv.URL, "sk-ant-sid01-w57-key-0")
	cfg.Public = &PublicSection{Enabled: true, CredentialMinGapSeconds: &zero}
	_, srv := startServiceWith(t, cfg)

	// 第一发：org 500 → 准备链失败（非换号类）→ 502。
	r1 := postChatRes(srv.URL)
	if r1.err != nil {
		t.Fatalf("r1 err = %v", r1.err)
	}
	if r1.code == 200 {
		t.Fatalf("r1 应失败: %d %s", r1.code, r1.body)
	}

	// 第二发：busy 已释放 → 正常 200 PONG。
	r2 := postChatRes(srv.URL)
	if r2.err != nil {
		t.Fatalf("r2 err = %v", r2.err)
	}
	assertPONG(t, r2.code, r2.body)
}
