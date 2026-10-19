// limits.go 全局并发闸 + 有界队列（P0-4；设计 `claude2api-public-service`
// §3.2/§4.3；board A2A-L19-W57-1）。
//
// 形态：**全局 inflight 闸 + 有界 FIFO 队列**——超出 max_inflight 的请求进
// 队列（深度 queue_size）；队列满 → 立即 `ErrQueueFull`（429 + 正整数
// Retry-After）；排队等待超时（queue_timeout_seconds）→ `ErrQueueTimeout`
// （429）。队列仅平滑短突发，不做分钟级排队（设计 §3.2 字面口径）。
//
// 接线：`serve` 入口 Acquire / 收尾 Release（server.go；覆盖全部生成请求与
// 任务流量——任务持有槽位直到终态）。**硬门：public.enabled=false（缺省）时
// limiter 为 nil，本文件全部行为不挂载**（零行为变化）。
//
// 纪律：拒绝信封按入口协议（复用 writeError 路径，server.go:1454 邻位）；
// Retry-After 只发正整数（对齐 server.go:1236-1275 既有纪律）；全链有界
// ——绝不出现无限等待。
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/registry"
)

// ErrQueueFull 队列已满（立即拒绝——不排队；429 + Retry-After）。
var ErrQueueFull = errors.New("service: global inflight limit reached; queue is full")

// ErrQueueTimeout 排队等待超时（等待上限 = queue_timeout_seconds；429）。
var ErrQueueTimeout = errors.New("service: global inflight limit reached; queue wait timed out")

// Limiter 全局 inflight 闸 + 有界 FIFO 队列（接口冻结：Acquire/Release/
// ErrQueueFull；并发安全）。
type Limiter struct {
	mu       sync.Mutex
	max      int
	inflight int
	// queue 等待者 FIFO（每项一个缓冲 1 的授予通道；Release 弹队首发送 = 槽位
	// 直接转交——绝不「先减后加」放大并发）。
	queue    []chan struct{}
	queueCap int
	timeout  time.Duration
}

// NewLimiter 建闸（调用方 = Config 访问器，已套缺省；防御性钳制）。
func NewLimiter(maxInflight, queueSize int, queueTimeout time.Duration) *Limiter {
	if maxInflight < 1 {
		maxInflight = 1
	}
	if queueSize < 0 {
		queueSize = 0
	}
	if queueTimeout <= 0 {
		queueTimeout = DefaultPublicQueueTimeout
	}
	return &Limiter{max: maxInflight, queueCap: queueSize, timeout: queueTimeout}
}

// Acquire 获取一个槽位：有空位 → 立即；否则进队列等待（≤ queue_timeout，
// 或 ctx 取消）；队列满 → ErrQueueFull；超时 → ErrQueueTimeout；ctx 取消 →
// ctx.Err()（调用方不写信封——客户端已断）。
func (l *Limiter) Acquire(ctx context.Context) error {
	l.mu.Lock()
	if l.inflight < l.max {
		l.inflight++
		l.mu.Unlock()
		return nil
	}
	if len(l.queue) >= l.queueCap {
		l.mu.Unlock()
		return ErrQueueFull
	}
	ch := make(chan struct{}, 1)
	l.queue = append(l.queue, ch)
	l.mu.Unlock()

	timer := time.NewTimer(l.timeout)
	defer timer.Stop()
	select {
	case <-ch:
		return nil
	case <-timer.C:
		return l.abandon(ch, ErrQueueTimeout)
	case <-ctx.Done():
		return l.abandon(ch, ctx.Err())
	}
}

// abandon 放弃排队（超时/ctx 取消）：从队列摘除；若槽位在竞态窗口内已授予本
// waiter（Release 的弹队+发送与本函数同锁互斥——窗口已封闭），归还之（转交
// 下一位 / 回落 inflight——绝不泄漏槽位）。
func (l *Limiter) abandon(ch chan struct{}, err error) error {
	l.mu.Lock()
	for i, c := range l.queue {
		if c == ch {
			l.queue = append(l.queue[:i], l.queue[i+1:]...)
			break
		}
	}
	select {
	case <-ch:
		l.releaseLocked() // 已授予：归还槽位
	default:
	}
	l.mu.Unlock()
	return err
}

// Release 释放一个槽位：有等待者 → 直接转交队首（inflight 不变）；否则
// inflight--（防御：不为负）。Acquire 成功后的收尾必须调用（幂等防御）。
func (l *Limiter) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.releaseLocked()
}

// releaseLocked 释放/转交（调用方持锁）。
func (l *Limiter) releaseLocked() {
	if len(l.queue) > 0 {
		ch := l.queue[0]
		l.queue = l.queue[1:]
		ch <- struct{}{} // 缓冲 1——绝不阻塞
		return
	}
	if l.inflight > 0 {
		l.inflight--
	}
}

// stats 当前 inflight / 排队数（测试与诊断用）。
func (l *Limiter) stats() (inflight, queued int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inflight, len(l.queue)
}

// writeLimitsRejected 全局并发闸拒绝：429 + 正整数 Retry-After（整秒） +
// Cache-Control: no-store；信封按入口协议（writeError 路径）。ctx 取消
// （客户端已断 / 任务已取消）不写——写向死连接无意义。
//
// P0-5：信封词表落 `rate_limited`（error.code 面 + message 检索 token，设计
// §3.3「全局 inflight 满 → error.code=rate_limited」；type 保持既有
// rate_limit_error——W56-1 先例不动）；拒绝面进 ring（rate_limited）+ 计数。
func (s *Server) writeLimitsRejected(w http.ResponseWriter, entry registry.Format, err error) {
	msg := ""
	switch {
	case errors.Is(err, ErrQueueFull):
		msg = "rate_limited: service at capacity (global inflight limit reached and queue is full); retry later"
	case errors.Is(err, ErrQueueTimeout):
		msg = "rate_limited: service at capacity (queued request timed out); retry later"
	default:
		return // ctx 取消：不写信封
	}
	w.Header().Set("Retry-After", "1")
	w.Header().Set("Cache-Control", "no-store")
	s.diag.publicRateLimited.Add(1)
	s.adminLog.add(adminLogRateLimited, "", "", msg, http.StatusTooManyRequests)
	// P1-2 审计持久化（双落点之①；store 未启用 = no-op）。
	s.auditStore(storeAudit{TS: time.Now(), Kind: adminLogRateLimited, Detail: msg})
	s.writeError(w, entry, http.StatusTooManyRequests,
		&ir.Error{Type: "rate_limit_error", Code: "rate_limited", Message: msg, Source: "any2api"})
}

// ---------------------------------------------------------------------------
// P0-5 成本档位 + 日预算台账 + 降级信封（board A2A-L19-W58-1）
// ---------------------------------------------------------------------------
//
// 形态（设计 §3.2/§3.3/§3.4；内存档——P1 迁 store usage_counters）：
//
//   - **成本档**（A/B/C/D = light/medium/heavy/max；权重 1/6/11/59 units，
//     1 unit = 0.01 窗）：请求体 `max_tokens` / `max_output_tokens` 粗分
//     （阈值表见 classifyMaxTokens——实现期定表、单测钉住）；缺省/无值 = 中档。
//   - **档位控制**：`public.max_cost_class`（缺省 medium）——请求档高于上限 →
//     400 `invalid_request_error`（策略拒绝：重试不会变好，故非 429；429 词表
//     只留三个可重试面）。
//   - **日预算**：`public.daily_budget_units`（缺省 250；显式 0 = 关）——
//     **准入即 check-and-charge**（O(1)，原子——防并发窗口超卖；比「收尾写点」
//     更保守；P1 持久化面沿设计 §3.4 收尾写点）。不足 → 429
//     `daily_quota_exhausted` + Retry-After=距 UTC 0 点整秒（快速路径）。
//   - **容量耗尽**：池全不可用（`upstream_credentials_exhausted`）→ 429
//     `capacity_exhausted` + Retry-After=最早解冻整秒（映射在 serve 终态面）。
//
// 硬门：public.enabled=false（缺省）时 publicLimits 为 nil——本段全部行为不
// 挂载（零行为变化）。

// costClass 成本档（A=light / B=medium / C=heavy / D=max；设计 §3.2）。
type costClass int

const (
	// costClassLight A 档「轻发」（短题、无长思考、无工具回路；+0.01 窗）。
	costClassLight costClass = iota
	// costClassMedium B 档「中发」（长题/推理题，单趟；+0.05~0.06 窗）。
	costClassMedium
	// costClassHeavy C 档「低档重发」（工具回路多趟续跑；≈+0.11 窗）。
	costClassHeavy
	// costClassMax D 档「max 重发」（长思考+工具回路；+0.59 窗——单发可烧穿
	// 一号；公网缺省拒绝）。
	costClassMax
)

// 分类阈值（max_tokens；实现期定表——单测钉住，U5 未决项的本波收敛）：
//
//	无值/0    → medium（缺省中档——保守但不过度拒绝无 max_tokens 的客户端）
//	≤2048     → light
//	≤16384    → medium
//	≤32768    → heavy
//	>32768    → max
const (
	costClassLightMaxTokens  = 2048
	costClassMediumMaxTokens = 16384
	costClassHeavyMaxTokens  = 32768
)

// units 档位权重（units；设计 §3.2 向上取整、保守：A=1、B=6、C=11、D=59）。
func (c costClass) units() int {
	switch c {
	case costClassLight:
		return 1
	case costClassMedium:
		return 6
	case costClassHeavy:
		return 11
	case costClassMax:
		return 59
	}
	return 6 // 防御：未知 = 中档
}

// String 档名（配置/日志/信封文案用）。
func (c costClass) String() string {
	switch c {
	case costClassLight:
		return "light"
	case costClassMedium:
		return "medium"
	case costClassHeavy:
		return "heavy"
	case costClassMax:
		return "max"
	}
	return "medium"
}

// letter 档位字母（A/B/C/D；文案用——与设计 §3.2 档名对齐）。
func (c costClass) letter() string {
	switch c {
	case costClassLight:
		return "A"
	case costClassMedium:
		return "B"
	case costClassHeavy:
		return "C"
	case costClassMax:
		return "D"
	}
	return "B"
}

// parseCostClassName 档名解析（配置校验 + 访问器；小写精确匹配——配置纪律
// 不做模糊匹配）。空/未知 → false（调用方定缺省）。
func parseCostClassName(s string) (costClass, bool) {
	switch s {
	case "light":
		return costClassLight, true
	case "medium":
		return costClassMedium, true
	case "heavy":
		return costClassHeavy, true
	case "max":
		return costClassMax, true
	}
	return 0, false
}

// classifyMaxTokens max_tokens → 档（阈值表见常量注释；has=false 或 <=0 =
// 缺省中档）。单测钉住全表。
func classifyMaxTokens(n int64, has bool) costClass {
	if !has || n <= 0 {
		return costClassMedium
	}
	switch {
	case n <= costClassLightMaxTokens:
		return costClassLight
	case n <= costClassMediumMaxTokens:
		return costClassMedium
	case n <= costClassHeavyMaxTokens:
		return costClassHeavy
	default:
		return costClassMax
	}
}

// classifyRequestCost 请求体 → 成本档（粗分）：`max_tokens` 优先，
// `max_output_tokens`（responses 面）兜底；都无 → 中档。体不可解析/字段形态
// 畸形 → 中档（防御——分类绝不成为新的拒绝面；体面错误由转换面既有路径报）。
func classifyRequestCost(body []byte) costClass {
	var probe struct {
		MaxTokens       json.Number `json:"max_tokens"`
		MaxOutputTokens json.Number `json:"max_output_tokens"`
	}
	if json.Unmarshal(body, &probe) != nil {
		return costClassMedium
	}
	if n, ok := jsonNumberInt(probe.MaxTokens); ok {
		return classifyMaxTokens(n, true)
	}
	if n, ok := jsonNumberInt(probe.MaxOutputTokens); ok {
		return classifyMaxTokens(n, true)
	}
	return costClassMedium
}

// jsonNumberInt json.Number → int64（整数原样；浮点截断；空/畸形 → false）。
func jsonNumberInt(n json.Number) (int64, bool) {
	if n == "" {
		return 0, false
	}
	if v, err := n.Int64(); err == nil {
		return v, true
	}
	if f, err := n.Float64(); err == nil {
		return int64(f), true
	}
	return 0, false
}

// ErrDailyQuotaExhausted 日预算耗尽（快速路径拒绝；调用方 429
// daily_quota_exhausted）。
var ErrDailyQuotaExhausted = errors.New("service: daily cost budget exhausted")

// budgetLedger 日预算台账（P0-5；内存档——UTC 日界惰性重置；check-and-charge
// 原子）。now 可注入（测试假时钟）。
//
// **持久化档（P1-6/P1-2）**：`persist != nil`（store.enabled=true）时——启动
// 载入当日已用额度（`usage_counters(scope='global', day=today)`）、每次扣账写穿
// 落库（**跨重启预算不重置**——设计 §5.3）。写失败只 log + ring `store_error`
// （绝不影响请求路径）。
type budgetLedger struct {
	mu    sync.Mutex
	daily int // 日预算（units；<=0 = 关——恒放行）
	used  int // 当日已用（units）
	day   string
	now   func() time.Time
	// persist 持久化层（nil = 内存台账）。
	persist Store
	// onPersistErr 持久化失败回调（由 New 注入 = Server.storeError）。
	onPersistErr func(op string, err error)
}

// storeUsageScopeGlobal usage_counters 全局 scope（设计 §2.3a scope 词表的
// `global`——日预算主体的唯一接线面；per-session/per-IP/per-key scope 登记残余）。
const storeUsageScopeGlobal = "global"

// newBudgetLedger 建台账（daily <= 0 = 关）。
func newBudgetLedger(daily int) *budgetLedger {
	return &budgetLedger{daily: daily, now: time.Now}
}

// attachStore 接持久化层（P1-6）：**启动即载入当日已用额度**（跨重启预算不重置）。
// 载入失败 = 记 ring/日志并**从 0 起**（保守方向：可能多放行一次窗口的额度，
// 绝不因存储故障拒绝启动——设计 §5.3「启动动作」不把 store 失败升级为全停）。
func (l *budgetLedger) attachStore(st Store, onErr func(op string, err error)) {
	if l == nil || st == nil {
		return
	}
	l.persist = st
	l.onPersistErr = onErr
	now := time.Now()
	if l.now != nil {
		now = l.now()
	}
	day := storeDay(now)
	u, ok, err := st.UsageGet(storeUsageScopeGlobal, day)
	if err != nil {
		l.persistError("usage load", err)
		return
	}
	l.mu.Lock()
	l.day = day
	if ok {
		l.used = u.CostUnits
	} else {
		l.used = 0
	}
	l.mu.Unlock()
}

// persistError 持久化失败面（日志 + 注入的 ring 回调；绝不进控制流）。
func (l *budgetLedger) persistError(op string, err error) {
	if err == nil {
		return
	}
	if l.onPersistErr != nil {
		l.onPersistErr(op, err)
		return
	}
	log.Printf("any2api: budget store %s: %v", op, err)
}

// admit 准入 check-and-charge：预算不足 → ErrDailyQuotaExhausted（不扣账）；
// 否则扣 units 放行。日界（UTC）惰性重置。关（daily<=0）恒放行不记账。
// 持久化档：扣账后写穿落库（best-effort）。
func (l *budgetLedger) admit(units int) error {
	if l == nil {
		return nil
	}
	now := time.Now()
	if l.now != nil {
		now = l.now()
	}
	day := storeDay(now)
	l.mu.Lock()
	if l.day != day {
		l.day, l.used = day, 0
	}
	if l.daily <= 0 {
		l.mu.Unlock()
		return nil
	}
	if l.used+units > l.daily {
		l.mu.Unlock()
		return ErrDailyQuotaExhausted
	}
	l.used += units
	l.mu.Unlock()
	// P1-6 持久化（设计 §3.4 写点面；best-effort——失败只 log/ring）。
	if l.persist != nil {
		if err := l.persist.UsageAdd(storeUsageScopeGlobal, day, 1, units); err != nil {
			l.persistError("usage add", err)
		}
	}
	return nil
}

// stats 台账快照（used, daily, day；测试/诊断用）。
func (l *budgetLedger) stats() (used, daily int, day string) {
	if l == nil {
		return 0, 0, ""
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.used, l.daily, l.day
}

// publicLimits P0-5/P0-8 公开面运行时限额（public.enabled=true 时由 New 装配；
// nil = 不挂载——硬门零行为变化）。
type publicLimits struct {
	// maxCostClass 允许的最高成本档（请求档 > 本值 → 400）。
	maxCostClass costClass
	// ledger 日预算台账（nil = 关）。
	ledger *budgetLedger
	// maxBodyBytes 请求体上限（0 = 用全局 32MB）。
	maxBodyBytes int64
	// maxMessages messages 条数上限（0 = 关）。
	maxMessages int
	// maxMessageChars 单条内容上限（0 = 关）。
	maxMessageChars int
}

// attachStore 接持久化层（P1-2/P1-6）：日预算台账落库 + 启动载入当日已用额度。
// store 未启用（缺省）= 不调用（内存台账，逐字 P0 行为）。onErr = Server.storeError
// （失败只 log + ring，绝不影响请求路径）。
func (pl *publicLimits) attachStore(st Store, onErr func(op string, err error)) {
	if pl == nil || pl.ledger == nil {
		return
	}
	pl.ledger.attachStore(st, onErr)
}

// newPublicLimits 由配置装配（调用方 = New；Validate 已保证合法）。
func newPublicLimits(c *Config) *publicLimits {
	pl := &publicLimits{
		maxCostClass:    costClassMedium,
		maxBodyBytes:    c.publicMaxBodyBytes(),
		maxMessages:     c.publicMaxMessages(),
		maxMessageChars: c.publicMaxMessageChars(),
	}
	if cls, ok := parseCostClassName(c.publicMaxCostClass()); ok {
		pl.maxCostClass = cls
	}
	if units, on := c.publicDailyBudgetUnits(); on {
		pl.ledger = newBudgetLedger(units)
	}
	return pl
}

// writeQuotaExhausted 日预算耗尽：429 + `daily_quota_exhausted`（error.code +
// message token；按入口协议信封）+ Retry-After=距 UTC 0 点整秒（正整数）+
// Cache-Control: no-store；拒绝面进 ring + 计数。
func (s *Server) writeQuotaExhausted(w http.ResponseWriter, entry registry.Format, now time.Time) {
	retry := retryAfterUntilUTCMidnight(now)
	w.Header().Set("Retry-After", strconv.Itoa(retry))
	w.Header().Set("Cache-Control", "no-store")
	msg := fmt.Sprintf("daily_quota_exhausted: daily cost budget exhausted; quota resets at 00:00 UTC (in %ds)", retry)
	s.diag.publicQuotaRejected.Add(1)
	s.adminLog.add(adminLogQuotaExhausted, "", "", msg, http.StatusTooManyRequests)
	// P1-2 审计持久化（双落点之①；store 未启用 = no-op）。
	s.auditStore(storeAudit{TS: now, Kind: adminLogQuotaExhausted, Detail: msg})
	s.writeError(w, entry, http.StatusTooManyRequests,
		&ir.Error{Type: "rate_limit_error", Code: "daily_quota_exhausted", Message: msg, Source: "any2api"})
}

// writeCostClassRejected 成本档拒绝（请求档 > 允许上限）：400
// invalid_request_error（策略面——重试不变好，故非 429；429 词表只留可重试
// 面）+ Cache-Control: no-store；计数（拒绝面）。
func (s *Server) writeCostClassRejected(w http.ResponseWriter, entry registry.Format, cls, maxCls costClass) {
	w.Header().Set("Cache-Control", "no-store")
	msg := fmt.Sprintf("cost class %s (%s) exceeds the allowed maximum %s (%s); reduce max_tokens or raise public.max_cost_class",
		cls.String(), cls.letter(), maxCls.String(), maxCls.letter())
	s.diag.publicClassRejected.Add(1)
	s.writeError(w, entry, http.StatusBadRequest,
		&ir.Error{Type: "invalid_request_error", Message: msg, Source: "any2api"})
}

// writeCapacityExhausted 容量耗尽（池全不可用）：429 + `capacity_exhausted`
// （error.code + message token）+ Retry-After=最早解冻整秒（正整数）+ no-store；
// 拒绝面进 ring + 计数。message 保留上游 `upstream_credentials_exhausted`
// 检索 token（既有词表风格——诊断可 join）。
func (s *Server) writeCapacityExhausted(w http.ResponseWriter, entry registry.Format, cause error, retryAfter time.Duration) {
	retry := int(retryAfter / time.Second)
	if retry < 1 {
		retry = 1 // 只发正整数（对齐 server.go:1236-1275 既有纪律；<1s = 1）
	}
	w.Header().Set("Retry-After", strconv.Itoa(retry))
	w.Header().Set("Cache-Control", "no-store")
	msg := fmt.Sprintf("capacity_exhausted: service capacity temporarily exhausted; %v", cause)
	s.diag.publicCapacityRejected.Add(1)
	s.adminLog.add(adminLogCapacityExhausted, "", "", msg, http.StatusTooManyRequests)
	// P1-2 审计持久化（双落点之①；store 未启用 = no-op）。
	s.auditStore(storeAudit{TS: time.Now(), Kind: adminLogCapacityExhausted, Detail: msg})
	s.writeError(w, entry, http.StatusTooManyRequests,
		&ir.Error{Type: "rate_limit_error", Code: "capacity_exhausted", Message: msg, Source: "any2api"})
}

// retryAfterUntilUTCMidnight 距下一个 UTC 0 点的整秒（ceil；>=1——正整数纪律）。
func retryAfterUntilUTCMidnight(now time.Time) int {
	utc := now.UTC()
	next := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	secs := int(next.Sub(utc).Seconds())
	if secs < 1 {
		secs = 1
	}
	return secs
}
