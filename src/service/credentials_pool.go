// credentials_pool.go 服务内凭据池（W53-2）：每 upstream 多 sessionKey 的
// 选择（最少近期使用）/ 冷却（配额类短冷却、凭据类长冷却）/ 耗尽语义。
//
// 产品面：claude2api 的多号运营——一个上游条目配多个 sessionKey，请求级轮换，
// 坏键自动让位、冷却到期自动回归。设计约束（与本仓既有语义对齐）：
//
//   - **`api_key` 保持兼容 = 单元素池**：`credentials` 未配时池 = `[api_key]`，
//     全部行为与引入前逐字节一致（选择恒为索引 0、不产标记、wire 无差异）；
//   - **记账只记索引，绝不落凭据字节**：本文件全部日志/错误只带 upstream 名与
//     索引（`credential #N`）——凭据值只活在内存（池的 creds 与按需装配的
//     请求头），绝不进日志/错误/attempt 记录/报告；
//   - **冷却语义**（与 failover.go 的类划分同源）：
//     配额类（429）→ 冷却时长 = `retry-after` 精确执行（钳到 maxRetryAfterCap）
//     否则短冷却 credentialQuotaCooldown；
//     凭据类（401 / 无盾 403——sessionKey 失效，见 claudeWebDiscoverOrg 的存活
//     三态）→ 长冷却 credentialDeadCooldown（近禁用；配置静态加载，重启即重置）；
//     其余（CF 盾 403、5xx、网络、请求问题）→ **不处置**（不是凭据的信号，
//     换号无用——走既有 failover 语义）；
//   - **耗尽**（全部凭据冷却中）→ `upstream_credentials_exhausted` 语义：显式
//     报错（绝不静默降级/空转），错误只带索引面信息。
//
// 状态共享：池挂在 `Config`（`credentialPoolAt` 惰性初始化——同一 Config 的
// 全部请求共享 LRU/冷却状态，跨请求生效）；`LoadConfig` 无需显式初始化
// （首个请求建池；直接构造的 Config 形态同样工作）。
//
// 与 completion 面的接线：`claudeWebPrepare` 选定索引后把索引附加进
// `claudeWebTarget.referer` 的标记（`#any2api-cred-<n>`）——attempt 局部、
// 并发安全；`applyClaudeWebHeaders` 在装配 completion 头时解析标记、用池内
// 该索引凭据落 cookie，并把标记从 Referer 头剥掉（wire 逐字节与引入前一致）。
// 会话清理面（DELETE）经 `noteConversation`/`conversationCredential` 用**创建
// 该会话的凭据**认证（会话是账号级对象——换号 DELETE 会 404/403）。
//
// **完成面失败回喂**（W54-3 残余 1）：completion 的配额类（429）/凭据类（401 /
// 无盾 403）失败经 `feedCompletionCredentialFailure` 记入该凭据冷却——**不换号
// 重试**（提交点纪律不变，只冷却；下次请求的准备面自然避开）。冷却事件以独立
// 型别 `adminLogCredentialCooldown` 进管理面 ring（`setEventSink` 装配，与上游级
// cooldown_trip 区分）。attempt 记录带索引（`attemptRecord.credentialIndex`——
// 只记索引，零凭据字节）。
package service

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// credentialMarkerPrefix completion referer 的池索引标记前缀（W53-2 内部通道：
// `claudeWebPrepare` 附加、`applyClaudeWebHeaders` 解析剥离——绝不进 wire）。
const credentialMarkerPrefix = "#any2api-cred-"

// credentialQuotaCooldown 配额类失败（429 无 retry-after）的冷却窗口。
const credentialQuotaCooldown = 60 * time.Second

// credentialDeadCooldown 凭据类失败（401 / 无盾 403）的冷却窗口——长冷却
// （sessionKey 失效要重新登录换号；重启进程即重置，故不做永久禁用）。
const credentialDeadCooldown = 30 * time.Minute

// credentialConvTTL 会话→凭据索引登记的存活上限（清理面查询用；清理开关关时
// 登记无人消费——按时间剪除，绝不无界增长）。
const credentialConvTTL = 10 * time.Minute

// credentialFailureClass 凭据池处置类别（准备链失败分类的结果）。
type credentialFailureClass int

const (
	// credFailureCredential 凭据类（sessionKey 失效）→ 长冷却。
	credFailureCredential credentialFailureClass = iota + 1
	// credFailureQuota 配额类（429）→ retry-after 或短冷却。
	credFailureQuota
)

func (c credentialFailureClass) String() string {
	switch c {
	case credFailureCredential:
		return "credential"
	case credFailureQuota:
		return "quota"
	}
	return "unknown"
}

// upstreamCredentials 该上游的有效凭据列表（W53-2）：
//
//   - `credentials` 非空 → 逐项使用（与 `api_key` 同给 = 配置错误，Validate
//     拒绝；直接构造的 Config 里 `credentials` 优先）；
//   - 否则 `api_key` 非空 → 单元素列表（api_key 兼容 = 单元素池）；
//   - 都没有 → nil（claude_web 在请求期显式报错——既有语义）。
//
// **池索引与 completion 标记解析共用本函数**（同一输入 → 同一顺序 → 同一索引
// 空间）；值不在此处 trim（applyClaudeWebHeaders 使用点统一 trim，与 api_key
// 现状一致）。
func upstreamCredentials(u Upstream) []string {
	if len(u.Credentials) > 0 {
		return u.Credentials
	}
	if u.APIKey != "" {
		return []string{u.APIKey}
	}
	return nil
}

// credentialConvRef 会话 → 本尝试选定凭据索引（清理面用）。
type credentialConvRef struct {
	index int
	at    time.Time
}

// credentialPoolEventSink 池事件接收端（W54-3 残余 3）：Server 在 New 期对每个
// 上游池装配一次（见 setEventSink），把凭据冷却事件转发进管理面 ring。未装配
// （直接构造的 Config/池形态）→ nil = 不接——零行为变化。
type credentialPoolEventSink func(kind, upstream, message string, status int)

// credentialPool 单 upstream 的凭据池（并发安全；nil 接收者安全——池未初始化/
// 上游下标越界时全部方法退化为空池）。
type credentialPool struct {
	name  string // upstream 名（日志/错误文案用；绝不参与选择）
	creds []string
	mu    sync.Mutex
	// lastUsed 每索引最近使用时刻（选择 = 最少近期使用；零值 = 从未使用）。
	lastUsed []time.Time
	// coolUntil 每索引冷却截止（零值 = 未冷却）。
	coolUntil []time.Time
	// convs 会话 → 选定索引（claudeWebPrepare 登记、清理面消费；TTL 剪除）。
	convs map[string]credentialConvRef
	// sink 池事件接收端（New 装配、运行期只读——见 setEventSink；nil = 不接）。
	sink credentialPoolEventSink
	// gate 公共闸开关（P0-4；public.enabled=true 时开）：false = busy/min-gap
	// 不生效——池行为与引入前逐字节一致（硬门）。
	gate bool
	// busy 每索引在飞标记（gate 时：选中即置、Release 释放——单号 inflight=1）。
	busy []bool
	// minGap 单凭据最小复用间隔（gate 时生效；0 = 关）。选中前校验
	// `now - last_used ≥ gap`，未到则跳过（全被跳过 → 耗尽语义）。
	minGap time.Duration
}

// newCredentialPool 建池（凭据列表 = upstreamCredentials(u)；空列表 = 空池，
// claudeWebPrepare 走既有单凭据路径）。
func newCredentialPool(u Upstream) *credentialPool {
	creds := upstreamCredentials(u)
	return &credentialPool{
		name:      u.Name,
		creds:     creds,
		lastUsed:  make([]time.Time, len(creds)),
		coolUntil: make([]time.Time, len(creds)),
	}
}

// configureGate 装配公共闸参数（P0-4；建池期一次、运行期只读——调用方绝不
// 并发写）。gate=false（public.enabled=false——缺省）时 busy/min-gap 全不生效。
// 直接构造的池（测试/旧形态不经过本函数）= gate 关——零行为变化。nil 安全。
func (p *credentialPool) configureGate(enabled bool, minGap time.Duration) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gate = enabled
	p.minGap = minGap
	if enabled && len(p.busy) != len(p.creds) {
		p.busy = make([]bool, len(p.creds))
	}
}

// setEventSink 装配池事件接收端（W54-3；New 期一次、运行期只读——调用方绝不
// 并发写）。nil 接收者安全。
func (p *credentialPool) setEventSink(fn credentialPoolEventSink) {
	if p == nil {
		return
	}
	p.sink = fn
}

// size 池大小（nil 安全）。
func (p *credentialPool) size() int {
	if p == nil {
		return 0
	}
	return len(p.creds)
}

// credential 索引 → 凭据值（内部使用——调用方绝不落日志/错误）。
func (p *credentialPool) credential(i int) (string, bool) {
	if p == nil || i < 0 || i >= len(p.creds) {
		return "", false
	}
	return p.creds[i], true
}

// selectCredential 选下一凭据：冷却中跳过；**公共闸（P0-4；gate）时** busy
// （在飞）与 min-gap（`now-last_used < gap`）候选同样跳过；其余取**最少近期
// 使用**（零值最先；同刻取低索引——全未用时即 0,1,2… 轮转序）。选中即记使用
// 时刻（请求开始选定，与 completion 无关）+ 置 busy（gate 时——生命周期与
// attempt 收尾绑定，见 Release）。全部不可选 → ok=false（耗尽语义）。
func (p *credentialPool) selectCredential(now time.Time) (int, bool) {
	if p == nil {
		return 0, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	best := -1
	for i := range p.creds {
		if now.Before(p.coolUntil[i]) {
			continue
		}
		if p.gate {
			if i < len(p.busy) && p.busy[i] {
				continue
			}
			if p.minGap > 0 && !p.lastUsed[i].IsZero() && now.Sub(p.lastUsed[i]) < p.minGap {
				continue
			}
		}
		if best < 0 || p.lastUsed[i].Before(p.lastUsed[best]) {
			best = i
		}
	}
	if best < 0 {
		return 0, false
	}
	p.lastUsed[best] = now
	if p.gate && best < len(p.busy) {
		p.busy[best] = true
	}
	return best, true
}

// Release 释放在飞标记（接口冻结名；P0-4 单号 inflight=1 的收尾面）。生命周期
// 与 attempt 收尾绑定：selectCredential 选中即置（gate 时）；prepare 失败该凭据
// 尝试收尾时释放（claudeWebPrepare）；prepare 成功后由 forward 在尝试任何出口
// （defer）释放。**gate 关（public.enabled=false——缺省）时 no-op——零行为变化**；
// 越界/未标记 = 防御性 no-op。
func (p *credentialPool) Release(idx int) {
	if p == nil || idx < 0 || idx >= len(p.creds) {
		return
	}
	p.mu.Lock()
	if p.gate && idx < len(p.busy) {
		p.busy[idx] = false
	}
	p.mu.Unlock()
}

// reportFailure 记一次凭据失败并按类别冷却（取较晚者——绝不缩短已有冷却）：
//
//   - credFailureQuota：retry-after 精确执行（上限 maxRetryAfterCap，与
//     failover.go 同纪律）；无头 → credentialQuotaCooldown；
//   - credFailureCredential：credentialDeadCooldown（长冷却）。
//
// 日志只带 upstream 名 + 索引 + 类别（**绝不含凭据字节**）；同一消息经 sink
// （New 装配时）以独立型别 adminLogCredentialCooldown 进管理面 ring（W54-3）。
func (p *credentialPool) reportFailure(i int, cls credentialFailureClass, retryAfter time.Duration, now time.Time) {
	if p == nil || i < 0 || i >= len(p.creds) {
		return
	}
	var d time.Duration
	switch cls {
	case credFailureQuota:
		if retryAfter > 0 {
			d = retryAfter
			if d > maxRetryAfterCap {
				d = maxRetryAfterCap
			}
		} else {
			d = credentialQuotaCooldown
		}
	default:
		d = credentialDeadCooldown
	}
	p.mu.Lock()
	if until := now.Add(d); until.After(p.coolUntil[i]) {
		p.coolUntil[i] = until
	}
	p.mu.Unlock()
	msg := fmt.Sprintf("credential #%d cooling for %s (class=%s)", i, d.Round(time.Millisecond), cls)
	log.Printf("any2api: claude_web upstream %s %s", p.name, msg)
	if p.sink != nil {
		// 管理面 ring（W54-3 残余 3）：独立型别（adminLogCredentialCooldown）——
		// 与上游级 cooldown_trip 语义区分；消息只带索引/类别，绝不含凭据字节。
		p.sink(adminLogCredentialCooldown, p.name, msg, 0)
	}
}

// credentialExhaustedError 池耗尽（全部凭据不可选）——类型化错误（P0-5）：
// serve 终态面据此映射 429 `capacity_exhausted` + Retry-After=最早解冻整秒。
// Error() 字面与引入前逐字节一致（`upstream_credentials_exhausted` 检索
// token 保留；既有测试/诊断不受影响）。**绝不含凭据字节**。
type credentialExhaustedError struct {
	upstream   string
	count      int
	retryAfter time.Duration // 最早可用剩余（冷却 / min-gap 两源取 min；可能为 0）
}

// Error 与引入前的 fmt.Errorf 字面逐字节一致（`earliest release in <时长>`；
// 时长按秒取整）。
func (e *credentialExhaustedError) Error() string {
	return fmt.Sprintf("claude_web: upstream %s: upstream_credentials_exhausted (all %d credential(s) unavailable; earliest release in %s)",
		e.upstream, e.count, e.retryAfter.Round(time.Second))
}

// exhaustedError 耗尽错误（全部凭据不可选：冷却中 / 公共闸下 busy / min-gap
// 未到；`upstream_credentials_exhausted` 语义——既有 failover 词表同款 token，
// 便于排障检索）。信息面只有池大小与最早可用剩余时间（**绝不含凭据字节**）。
func (p *credentialPool) exhaustedError(now time.Time) error {
	var name string
	n, rem := 0, time.Duration(0)
	if p != nil {
		p.mu.Lock()
		name, n = p.name, len(p.creds)
		for i := range p.coolUntil {
			if d := p.coolUntil[i].Sub(now); d > 0 && (rem == 0 || d < rem) {
				rem = d
			}
			// P0-4：min-gap 阻塞的候选同样计入最早可用时刻（信息面更准确）。
			if p.gate && p.minGap > 0 && !p.lastUsed[i].IsZero() {
				if d := p.lastUsed[i].Add(p.minGap).Sub(now); d > 0 && (rem == 0 || d < rem) {
					rem = d
				}
			}
		}
		p.mu.Unlock()
	}
	if name == "" {
		name = "(unnamed)"
	}
	log.Printf("any2api: claude_web upstream %s: upstream_credentials_exhausted (all %d credential(s) unavailable)", name, n)
	return &credentialExhaustedError{upstream: name, count: n, retryAfter: rem}
}

// noteConversation 登记「该会话由索引 i 的凭据创建」（清理面 DELETE 认证用）。
// 顺带按 TTL 剪除过期登记（有界——只在登记时扫描，条目数 = 在飞+近期请求数）。
func (p *credentialPool) noteConversation(conv string, i int, now time.Time) {
	if p == nil || conv == "" || i < 0 || i >= len(p.creds) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.convs == nil {
		p.convs = make(map[string]credentialConvRef)
	}
	for c, ref := range p.convs {
		if now.Sub(ref.at) > credentialConvTTL {
			delete(p.convs, c)
		}
	}
	p.convs[conv] = credentialConvRef{index: i, at: now}
}

// conversationCredential 查（并消费）会话的创建凭据（清理面用；查不到 → ok=false
// ——单凭据/池未初始化形态，调用方退回既有 up.APIKey 路径）。
func (p *credentialPool) conversationCredential(conv string) (string, bool) {
	if p == nil || conv == "" {
		return "", false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	ref, ok := p.convs[conv]
	if !ok {
		return "", false
	}
	delete(p.convs, conv)
	return p.credential(ref.index)
}

// credentialPoolAt 该上游的凭据池（惰性初始化：首次请求或 New 装配期建池；
// 同一 Config 的全部请求共享——冷却/LRU 跨请求生效）。idx 越界 / Config 为空 →
// nil（调用方 nil 安全）。初始化临界区由包级互斥保护（建池是低频一次性事件）。
//
// P0-4：建池期装配公共闸参数（public.enabled / credential_min_gap_seconds；
// gate=false——缺省——时池行为与引入前逐字节一致）。
func (c *Config) credentialPoolAt(idx int) *credentialPool {
	if c == nil || idx < 0 || idx >= len(c.Upstreams) {
		return nil
	}
	credentialPoolsMu.Lock()
	defer credentialPoolsMu.Unlock()
	if c.credentialPools == nil {
		c.credentialPools = make([]*credentialPool, len(c.Upstreams))
		gate := c.publicEnabled()
		gap := c.credentialMinGap()
		for i := range c.Upstreams {
			p := newCredentialPool(c.Upstreams[i])
			p.configureGate(gate, gap)
			c.credentialPools[i] = p
		}
	}
	return c.credentialPools[idx]
}

// credentialPoolsMu 池初始化的包级互斥（所有 Config 共享——临界区极小，
// 只在建池与每次取池时短暂持锁）。
var credentialPoolsMu sync.Mutex

// credentialMarkedReferer completion referer 附加池索引标记（W53-2 内部通道；
// 见 credentialMarkerPrefix 头注）。
func credentialMarkedReferer(referer string, index int) string {
	return referer + credentialMarkerPrefix + strconv.Itoa(index)
}

// splitCredentialMarker 解析 referer 的池索引标记：返回（裸 referer, 索引,
// 有标记）。标记存在但畸形（非十进制/负数）→ ok=true 且 err 非 nil（内部通道
// 畸形 = 编程错误，显式失败绝不静默）。
func splitCredentialMarker(referer string) (string, int, bool, error) {
	i := strings.LastIndex(referer, credentialMarkerPrefix)
	if i < 0 {
		return referer, 0, false, nil
	}
	raw := referer[i+len(credentialMarkerPrefix):]
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return referer, 0, true, fmt.Errorf("claude_web: malformed credential marker in referer")
	}
	return referer[:i], n, true, nil
}

// ---------------------------------------------------------------------------
// 完成面失败回喂（W54-3 残余 1）：completion 失败 → 凭据冷却（不换号重试）
// ---------------------------------------------------------------------------

// credentialClassForStatus 完成面失败（状态码 + 响应头）→ 池处置类别（W54-3）：
// 与准备面 `classifyClaudeWebPrepareFailure`（claude_web.go）**同表同判据**——
// 429 → 配额类（retry-after 精确执行）；401 / 403（**无 cf-mitigated 盾特征**）
// → 凭据类；带盾 403 / 其余 → handled=false（不处置——不是凭据信号）。
//
// 写面注记：两处同形是当前写面约束下的最小落法（本单不含 claude_web.go）；
// 语义必须成对演进——将来 claude_web.go 进写面时应统一为单一分类函数。
func credentialClassForStatus(status int, header http.Header) (cls credentialFailureClass, retryAfter time.Duration, handled bool) {
	switch status {
	case http.StatusTooManyRequests:
		return credFailureQuota, retryAfterSeconds(header), true
	case http.StatusUnauthorized, http.StatusForbidden:
		if header.Get("cf-mitigated") != "" {
			return 0, 0, false
		}
		return credFailureCredential, 0, true
	}
	return 0, 0, false
}

// feedCompletionCredentialFailure 完成面失败回喂池（W54-3 残余 1）：completion
// 的配额类（429）/凭据类（401 / 无盾 403）失败把（索引, 类别）记入该凭据冷却。
// **不换号重试**——提交点纪律不变（completion 已发出、可能已产出），只冷却：
// 下次请求的准备面自然避开该索引；完成面本身沿用既有语义（单上游上抛 / 多上游
// failover 链）。
//
// 索引来自 claudeWebPrepare 附加在 referer 上的标记（attempt 局部、并发安全）；
// 无标记（单凭据/池未接线）→ no-op；畸形（防御面——applyClaudeWebHeaders 已先
// 行挡下）→ no-op。记账只记索引（reportFailure 同纪律——绝不含凭据字节）。
func (s *Server) feedCompletionCredentialFailure(idx int, up Upstream, referer string, status int, header http.Header) {
	if up.Protocol != ProtocolClaudeWeb {
		return
	}
	_, ci, marked, err := splitCredentialMarker(referer)
	if err != nil || !marked {
		return
	}
	cls, retryAfter, handled := credentialClassForStatus(status, header)
	if !handled {
		return
	}
	if pool := s.cfg.credentialPoolAt(idx); pool != nil {
		pool.reportFailure(ci, cls, retryAfter, time.Now())
	}
}
