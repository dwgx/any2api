// W5-32（L19 sweep15）：failover 三升级（P65B 裁定 #1/#2/#3）的回归测试。
//
//  1. 瞬时类同上游短退避重试再换号（`retry_same_upstream`；缺省 0 = 关）：
//     中间重试不计健康/不冷却，终态才记一次（Parrot `failover.py:2683-2712`）；
//  2. 冷却阶梯升级式窗口（`cooldown_ladder`；缺省空 = 固定窗口）：连续进入冷却
//     逐级升级，成功清零（Parrot `cooldown.py:1-8` + P65B 裁定 #2 的有限档）；
//  3. 冷却中失败不延长冷却（P65B 裁定 #3 推荐 a；Parrot 三防闸 #1）：冷却期内
//     再失败不重算窗口（显式 retry-after 例外，C1 精确执行）。
//
// 装置与既有测试一致：httptest 假上游（全部本地回环）+ 真服务 + 真 HTTP 客户端。
package service

import (
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// retryCfg 同上游重试配置（毫秒级退避——测试快跑；生产缺省 [750,1750]ms）。
func retryCfg(ups []Upstream, n int, delaysMs ...int) *Config {
	cfg := failoverCfg(ups...)
	cfg.RetrySameUpstream = n
	cfg.RetrySameUpstreamDelaysMs = delaysMs
	return cfg
}

// ---------------------------------------------------------------------------
// 条目 1：瞬时类失败 → 同上游短退避重试（反证：非瞬时类不重试）
// ---------------------------------------------------------------------------

// TestW532SameUpstreamRetryTransient 503（瞬时类）：同台重试 1 次成功后不换上游。
func TestW532SameUpstreamRetryTransient(t *testing.T) {
	var mu sync.Mutex
	hits := 0
	flaky := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		n := hits
		mu.Unlock()
		if n == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(503)
			_, _ = w.Write([]byte(errBodyAPIError))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		claudeTextStream(w)
	})
	backup := goodClaudeUpstream(t)
	cfg := retryCfg([]Upstream{anthUp("flaky", flaky.srv.URL), anthUp("backup", backup.srv.URL)}, 1, 10, 20)
	s, srv := startServiceWith(t, cfg)

	code, body := postChatStream(t, srv.URL)
	if code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("status=%d body=\n%s", code, body)
	}
	if flaky.count() != 2 {
		t.Fatalf("flaky hits = %d（want 2：首发 503 + 同台重试 1 次）", flaky.count())
	}
	if backup.count() != 0 {
		t.Fatalf("backup hits = %d（want 0：同台重试成功后不得换上游）", backup.count())
	}
	if d := s.Diagnostics(); d.SameUpstreamRetries != 1 || d.Attempts != 2 || d.Failovers != 0 {
		t.Fatalf("diagnostics = %+v（want SameUpstreamRetries=1 Attempts=2 Failovers=0）", d)
	}
}

// TestW532SameUpstreamRetryDisabledByDefault 缺省关（保守优先）：503 直接换上游，
// 同台不重试——引入前行为逐字节保持。
func TestW532SameUpstreamRetryDisabledByDefault(t *testing.T) {
	bad := failUpstream(t, 503, nil, errBodyAPIError)
	good := goodClaudeUpstream(t)
	s, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	code, body := postChatStream(t, srv.URL)
	if code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("status=%d body=\n%s", code, body)
	}
	if bad.count() != 1 || good.count() != 1 {
		t.Fatalf("hits: bad=%d good=%d（want 1/1：缺省不重试）", bad.count(), good.count())
	}
	if d := s.Diagnostics(); d.SameUpstreamRetries != 0 || d.Attempts != 2 || d.Failovers != 1 {
		t.Fatalf("diagnostics = %+v（want SameUpstreamRetries=0 Attempts=2 Failovers=1）", d)
	}
}

// TestW532SameUpstreamRetryNonTransient 反证：400 + `x-should-retry:false`（显式
// 不可重试）不触发同台重试——即使 retry_same_upstream=1。
func TestW532SameUpstreamRetryNonTransient(t *testing.T) {
	bad := failUpstream(t, 400, map[string]string{"x-should-retry": "false"}, errBodyAPIError)
	good := goodClaudeUpstream(t)
	cfg := retryCfg([]Upstream{anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)}, 1, 10)
	s, srv := startServiceWith(t, cfg)

	code, _ := postChatStream(t, srv.URL)
	if code != 400 {
		t.Fatalf("status = %d（x-should-retry:false 的 400 直接上抛）", code)
	}
	if bad.count() != 1 {
		t.Fatalf("bad hits = %d（want 1：显式不可重试不得同台重试）", bad.count())
	}
	if good.count() != 0 {
		t.Fatalf("good hits = %d（want 0：400 是请求问题，不换上游）", good.count())
	}
	if d := s.Diagnostics(); d.SameUpstreamRetries != 0 || d.Attempts != 1 {
		t.Fatalf("diagnostics = %+v（want SameUpstreamRetries=0 Attempts=1）", d)
	}
}

// TestW532SameUpstreamRetryTransportError 传输错误（连接被关）：同台重试一次成功。
func TestW532SameUpstreamRetryTransportError(t *testing.T) {
	var mu sync.Mutex
	hits := 0
	flaky := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		n := hits
		mu.Unlock()
		if n == 1 {
			// 首请求直接 panic：net/http 关连接 → 客户端拿传输错误（同台再试的
			// 标准形态；guard 未超时 → err 原样回传）。
			panic("W5-32 probe: drop connection")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		claudeTextStream(w)
	})
	cfg := retryCfg([]Upstream{anthUp("flaky", flaky.srv.URL)}, 1, 10)
	s, srv := startServiceWith(t, cfg)

	code, body := postChatStream(t, srv.URL)
	if code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("status=%d body=\n%s", code, body)
	}
	if flaky.count() != 2 {
		t.Fatalf("flaky hits = %d（want 2：传输错误 + 同台重试）", flaky.count())
	}
	if d := s.Diagnostics(); d.SameUpstreamRetries != 1 || d.Attempts != 2 {
		t.Fatalf("diagnostics = %+v（want SameUpstreamRetries=1 Attempts=2）", d)
	}
}

// TestW532SameUpstreamRetryHealthOncePerTerminal 核心纪律（W5-32 条目 1）：中间
// 重试**不计健康**——503 首发 + 同台重试再 503（两次都失败）后换台，上游只记
// **一次**失败（旧实现逐次 recordFailure 会记两次：冷却阈值 2 会被中间重试提前
// 打穿；「终态才记一次」= 重试穷尽后的结果记一次）。
func TestW532SameUpstreamRetryHealthOncePerTerminal(t *testing.T) {
	bad := failUpstream(t, 503, nil, errBodyAPIError)
	good := goodClaudeUpstream(t)
	cfg := retryCfg([]Upstream{anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)}, 1, 10)
	cfg.CooldownFailures = 2
	cfg.CooldownSeconds = 30
	s, srv := startServiceWith(t, cfg)

	code, body := postChatStream(t, srv.URL)
	if code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("status=%d body=\n%s", code, body)
	}
	if bad.count() != 2 {
		t.Fatalf("bad hits = %d（want 2：首发 + 同台重试）", bad.count())
	}
	s.health[0].mu.Lock()
	fails := s.health[0].fails
	cooling := time.Now().Before(s.health[0].until)
	s.health[0].mu.Unlock()
	if fails != 1 {
		t.Fatalf("bad fails = %d（want 1：中间重试不计健康，终态才记一次）", fails)
	}
	if cooling {
		t.Fatalf("bad 不得冷却（1 次 < 阈值 2——中间重试若各记一次会提前打穿）")
	}
	if d := s.Diagnostics(); d.SameUpstreamRetries != 1 || d.Failovers != 1 {
		t.Fatalf("diagnostics = %+v（want SameUpstreamRetries=1 Failovers=1）", d)
	}
}

// TestW532SameUpstreamRetryOnLastUpstream 链上**只有一台**时也重试（终态错误信封
// 在重试穷尽后才写；attemptLast 门）。
func TestW532SameUpstreamRetryOnLastUpstream(t *testing.T) {
	var mu sync.Mutex
	hits := 0
	flaky := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		n := hits
		mu.Unlock()
		if n == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(503)
			_, _ = w.Write([]byte(errBodyAPIError))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		claudeTextStream(w)
	})
	cfg := retryCfg([]Upstream{anthUp("flaky", flaky.srv.URL)}, 1, 10)
	s, srv := startServiceWith(t, cfg)

	code, body := postChatStream(t, srv.URL)
	if code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("status=%d body=\n%s（末台重试成功后必须 200）", code, body)
	}
	if flaky.count() != 2 {
		t.Fatalf("flaky hits = %d（want 2：末台也重试）", flaky.count())
	}
	if d := s.Diagnostics(); d.SameUpstreamRetries != 1 || d.Failovers != 0 {
		t.Fatalf("diagnostics = %+v（want SameUpstreamRetries=1 Failovers=0）", d)
	}
}

// TestW532SameUpstreamRetryHonorsRetryAfter 退避优先 `retry-after-ms`（上游显式
// 等待时间最忠实；上限 60s）。30ms 头值 → 实际等待 ≥30ms（不是配置的 10ms）。
func TestW532SameUpstreamRetryHonorsRetryAfter(t *testing.T) {
	var mu sync.Mutex
	hits := 0
	flaky := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		n := hits
		mu.Unlock()
		if n == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("retry-after-ms", "30")
			w.WriteHeader(503)
			_, _ = w.Write([]byte(errBodyAPIError))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		claudeTextStream(w)
	})
	cfg := retryCfg([]Upstream{anthUp("flaky", flaky.srv.URL)}, 1, 10)
	_, srv := startServiceWith(t, cfg)

	start := time.Now()
	code, body := postChatStream(t, srv.URL)
	elapsed := time.Since(start)
	if code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("status=%d body=\n%s", code, body)
	}
	if elapsed < 25*time.Millisecond {
		t.Fatalf("elapsed = %s（want >= ~30ms：退避按 retry-after-ms 头，而非配置的 10ms）", elapsed)
	}
}

// ---------------------------------------------------------------------------
// 条目 2：冷却阶梯升级式窗口
// ---------------------------------------------------------------------------

// ladderCfg 阶梯配置（30s→2m→10m；阈值 1 = 每次进入冷却立即推进一档）。
func ladderCfg() *Config {
	cfg := failoverCfg(anthUp("a", "http://127.0.0.1:1"))
	cfg.CooldownFailures = 1
	cfg.CooldownSeconds = 30
	cfg.CooldownLadder = []int{30, 120, 600}
	cfg.FailureRate = &FailureRateCooldown{Enabled: new(false)} // 隔离：只测阈值路径
	return cfg
}

// TestW532CooldownLadderEscalates 阶梯逐级升级（30s→2m→10m，顶档封顶）。
// 每次推进前把上一次冷却窗口设为已过期（冷却中失败不推进档位——与「不延长」同一
// 条防爆发闸；这也正是「连续**进入冷却**」的语义）。
func TestW532CooldownLadderEscalates(t *testing.T) {
	cfg := ladderCfg()
	s := New(cfg)

	for i, want := range []time.Duration{30 * time.Second, 120 * time.Second, 600 * time.Second, 600 * time.Second} {
		h := &s.health[0]
		h.mu.Lock()
		h.until = time.Now().Add(-time.Millisecond) // 模拟上一窗口已过期（新一次进入冷却）
		h.mu.Unlock()
		s.recordFailure(0, 0)
		h.mu.Lock()
		step := h.ladderStep
		remaining := time.Until(h.until)
		h.mu.Unlock()
		wantStep := i + 1
		if wantStep > len(cfg.CooldownLadder) {
			wantStep = len(cfg.CooldownLadder) // 顶档封顶：档位计数不越界
		}
		if step != wantStep {
			t.Fatalf("第 %d 次进入冷却：ladder_step = %d（want %d）", i+1, step, wantStep)
		}
		if remaining < want-2*time.Second || remaining > want+2*time.Second {
			t.Fatalf("第 %d 次进入冷却：窗口剩余 = %s（want ≈%s）", i+1, remaining.Round(time.Millisecond), want)
		}
	}
}

// TestW532CooldownLadderSuccessResets 成功清零：档位复位、冷却解除；之后首次
// 失败重新从第一档开始（成功打断连续——「升级式惩罚可被成功打断」）。
func TestW532CooldownLadderSuccessResets(t *testing.T) {
	cfg := ladderCfg()
	s := New(cfg)
	h := &s.health[0]

	// 两次进入冷却（每次前模拟上一窗口已过期）→ 第二档（120s）。
	for range 2 {
		h.mu.Lock()
		h.until = time.Now().Add(-time.Millisecond)
		h.mu.Unlock()
		s.recordFailure(0, 0)
	}
	h.mu.Lock()
	step := h.ladderStep
	remaining := time.Until(h.until)
	h.mu.Unlock()
	if step != 2 {
		t.Fatalf("两次进入冷却后 ladder_step = %d（want 2）", step)
	}
	if remaining < 118*time.Second || remaining > 122*time.Second {
		t.Fatalf("第二档窗口剩余 = %s（want ≈120s）", remaining.Round(time.Millisecond))
	}

	s.recordSuccess(0)
	h.mu.Lock()
	step = h.ladderStep
	cooling := time.Now().Before(h.until)
	fails := h.fails
	h.mu.Unlock()
	if step != 0 || cooling || fails != 0 {
		t.Fatalf("成功后 ladder_step = %d cooling = %v fails = %d（want 0/false/0）", step, cooling, fails)
	}

	// 成功后的首次失败：第一档（30s）——旧档位/旧连续计数都不得残留。
	s.recordFailure(0, 0)
	h.mu.Lock()
	step = h.ladderStep
	remaining = time.Until(h.until)
	h.mu.Unlock()
	if step != 1 || remaining > 32*time.Second {
		t.Fatalf("成功后再失败：ladder_step = %d 剩余 = %s（want 1/≈30s——成功已清零）", step, remaining.Round(time.Millisecond))
	}
}

// TestW532CooldownLadderDefaultOff 缺省（不配阶梯）行为与引入前一致：固定窗口。
func TestW532CooldownLadderDefaultOff(t *testing.T) {
	cfg := failoverCfg(anthUp("a", "http://127.0.0.1:1"))
	cfg.CooldownFailures = 1
	cfg.CooldownSeconds = 30
	s := New(cfg)
	for range 4 {
		s.recordFailure(0, 0)
		h := &s.health[0]
		h.mu.Lock()
		step := h.ladderStep
		remaining := time.Until(h.until)
		h.mu.Unlock()
		if step != 0 {
			t.Fatalf("无阶梯配置不得推进档位（step=%d）", step)
		}
		if remaining > 31*time.Second {
			t.Fatalf("固定窗口 = %s（want ≈30s）", remaining.Round(time.Millisecond))
		}
	}
}

// ---------------------------------------------------------------------------
// 条目 3：冷却中失败不延长冷却（反证：显式 retry-after 仍可更新）
// ---------------------------------------------------------------------------

// TestW532FailureDuringCooldownDoesNotExtend 冷却期内再失败**不**重算窗口
// （P65B 裁定 #3 推荐 a；旧实现 until=now+window 反复取较晚者 → 无限延长）。
func TestW532FailureDuringCooldownDoesNotExtend(t *testing.T) {
	cfg := failoverCfg(anthUp("a", "http://127.0.0.1:1"))
	cfg.CooldownFailures = 1
	cfg.CooldownSeconds = 30
	cfg.CooldownLadder = []int{30, 120, 600} // 同时锁定「冷却中失败不推进阶梯」
	s := New(cfg)

	s.recordFailure(0, 0) // 进入冷却：until ≈ now+30s
	h := &s.health[0]
	h.mu.Lock()
	firstUntil := h.until
	h.mu.Unlock()
	if !s.cooling(0, time.Now()) {
		t.Fatalf("首次失败（阈值 1）必须进入冷却")
	}

	time.Sleep(20 * time.Millisecond)
	s.recordFailure(0, 0) // 冷却中再失败：不得延长
	h.mu.Lock()
	secondUntil := h.until
	fails := h.fails
	step := h.ladderStep
	h.mu.Unlock()
	if !secondUntil.Equal(firstUntil) {
		t.Fatalf("冷却中失败延长了窗口：%s → %s（want 不变——不延长纪律）",
			time.Until(firstUntil).Round(time.Millisecond), time.Until(secondUntil).Round(time.Millisecond))
	}
	if fails != 2 {
		t.Fatalf("fails = %d（want 2：冷却中失败仍计数——只是不延长窗口）", fails)
	}
	if step != 1 {
		t.Fatalf("ladder_step = %d（want 1：冷却中失败不推进阶梯）", step)
	}
	if !s.cooling(0, time.Now()) {
		t.Fatalf("冷却必须仍然有效（未被缩短）")
	}

	// 反证：显式 retry-after（更长）仍可更新窗口（C1 精确执行优先于防延长）。
	s.recordFailure(0, 2*time.Minute)
	h.mu.Lock()
	thirdUntil := h.until
	stepAfter := h.ladderStep
	h.mu.Unlock()
	if !thirdUntil.After(secondUntil) {
		t.Fatalf("显式 retry-after 未更新窗口：%s → %s", secondUntil, thirdUntil)
	}
	if remaining := time.Until(thirdUntil); remaining < 100*time.Second || remaining > 121*time.Second {
		t.Fatalf("retry-after 更新后的剩余 = %s（want ≈120s）", remaining.Round(time.Millisecond))
	}
	if stepAfter != 1 {
		t.Fatalf("显式 retry-after 不得推进阶梯（step=%d，want 1）", stepAfter)
	}
}

// TestW532CooldownNoExtendEndToEnd 端到端：冷却期内上游持续失败，窗口不延长——
// 直接对比冷却截止时刻（请求 1 记录 until，请求 2/3 的失败不得把它推后）。
func TestW532CooldownNoExtendEndToEnd(t *testing.T) {
	a := failUpstream(t, 500, nil, errBodyAPIError)
	b := failUpstream(t, 500, nil, errBodyAPIError)
	cfg := failoverCfg(anthUp("a", a.srv.URL), anthUp("b", b.srv.URL))
	cfg.CooldownFailures = 1
	cfg.CooldownSeconds = 30
	s, srv := startServiceWith(t, cfg)

	if code, _ := postChatStream(t, srv.URL); code != 500 {
		t.Fatalf("req1: status=%d（末台上游错误透传）", code)
	}
	h := &s.health[0]
	h.mu.Lock()
	untilAfterReq1 := h.until
	h.mu.Unlock()
	if untilAfterReq1.IsZero() {
		t.Fatalf("req1 失败后 a 必须进入冷却")
	}
	// req2/req3：a 在冷却窗口内被跳过（不命中），b 持续失败——a 的窗口不得被推后。
	for req := 2; req <= 3; req++ {
		if code, _ := postChatStream(t, srv.URL); code != 500 {
			t.Fatalf("req%d: status=%d", req, code)
		}
	}
	h.mu.Lock()
	untilAfter := h.until
	h.mu.Unlock()
	if !untilAfter.Equal(untilAfterReq1) {
		t.Fatalf("冷却被延长：until %s → %s（want 不变——冷却中失败不延长）",
			untilAfterReq1.Format(time.RFC3339Nano), untilAfter.Format(time.RFC3339Nano))
	}
	// req1：a 失败（进入冷却）→ 换 b 失败（b 也进入冷却）。req2 起两台全冷却 →
	// 退化为全列表：每轮都试 a 与 b（a 的失败发生在冷却中 → 不延长，故窗口不变）。
	if a.count() != 3 {
		t.Fatalf("a hits = %d（want 3：全冷却退化全列表，每轮都试 a）", a.count())
	}
	if b.count() != 3 {
		t.Fatalf("b hits = %d（want 3：每轮都命中 b）", b.count())
	}
}

// TestW532CooldownLadderCapsAndResets 阶梯顶档封顶 + 成功清零（经 recordFailure/
// recordSuccess 单元路径；与 TestW532CooldownLadderEscalates 互证封顶语义）。
func TestW532CooldownLadderCapsAndResets(t *testing.T) {
	cfg := failoverCfg(anthUp("a", "http://127.0.0.1:1"))
	cfg.CooldownFailures = 1
	cfg.CooldownSeconds = 1
	cfg.CooldownLadder = []int{1, 2}
	s := New(cfg)

	// 连续进入冷却：1s → 2s → 2s（顶档封顶，不越界）。
	for i, want := range []time.Duration{time.Second, 2 * time.Second, 2 * time.Second} {
		time.Sleep(2100 * time.Millisecond) // 等上一窗口过期（冷却中失败不推进档位）
		s.recordFailure(0, 0)
		h := &s.health[0]
		h.mu.Lock()
		step := h.ladderStep
		remaining := time.Until(h.until)
		h.mu.Unlock()
		if step > 2 {
			t.Fatalf("第 %d 轮：step = %d（want <= 2——顶档封顶）", i+1, step)
		}
		if remaining < want-300*time.Millisecond || remaining > want+300*time.Millisecond {
			t.Fatalf("第 %d 轮：窗口剩余 = %s（want ≈%s）", i+1, remaining.Round(time.Millisecond), want)
		}
	}
	s.recordSuccess(0)
	h := &s.health[0]
	h.mu.Lock()
	step := h.ladderStep
	h.mu.Unlock()
	if step != 0 {
		t.Fatalf("成功后 step = %d（want 0）", step)
	}
}

// ---------------------------------------------------------------------------
// 配置面校验（显式报错，绝不静默降级）
// ---------------------------------------------------------------------------

func TestW532ConfigValidation(t *testing.T) {
	base := func() *Config {
		return &Config{Listen: "127.0.0.1:0", Upstreams: []Upstream{anthUp("a", "http://127.0.0.1:1")}}
	}
	// retry_same_upstream 越界（上限 3）。
	c := base()
	c.RetrySameUpstream = 4
	if err := c.Validate(); err == nil {
		t.Fatal("retry_same_upstream=4 必须报错（上限 3）")
	}
	c = base()
	c.RetrySameUpstream = -1
	if err := c.Validate(); err == nil {
		t.Fatal("retry_same_upstream=-1 必须报错")
	}
	// 延迟序列：0 与超上限（>60s）都报错。
	c = base()
	c.RetrySameUpstreamDelaysMs = []int{0}
	if err := c.Validate(); err == nil {
		t.Fatal("delays[0]=0 必须报错")
	}
	c = base()
	c.RetrySameUpstreamDelaysMs = []int{60001}
	if err := c.Validate(); err == nil {
		t.Fatal("delays[0]=60001ms 必须报错（上限 60s）")
	}
	// 阶梯：非递增 / 非正 都报错。
	c = base()
	c.CooldownLadder = []int{30, 30}
	if err := c.Validate(); err == nil {
		t.Fatal("cooldown_ladder 非严格递增必须报错")
	}
	c = base()
	c.CooldownLadder = []int{30, 0}
	if err := c.Validate(); err == nil {
		t.Fatal("cooldown_ladder 含 0 必须报错")
	}
	// 合法配置过校验。
	c = base()
	c.RetrySameUpstream = 3
	c.RetrySameUpstreamDelaysMs = []int{750, 1750}
	c.CooldownLadder = []int{30, 120, 600}
	if err := c.Validate(); err != nil {
		t.Fatalf("合法配置被拒：%v", err)
	}
}

// TestW532JitterBounds jitter 在 [0.8, 1.2) 倍之间（±20%）。
func TestW532JitterBounds(t *testing.T) {
	base := 100 * time.Millisecond
	for range 200 {
		got := jitter(base)
		if got < 80*time.Millisecond || got >= 120*time.Millisecond {
			t.Fatalf("jitter(%s) = %s（want [80ms,120ms)）", base, got)
		}
	}
	if jitter(0) != 0 {
		t.Fatalf("jitter(0) != 0")
	}
}
