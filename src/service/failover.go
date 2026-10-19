package service

import (
	"bytes"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// 多上游有序 failover 的失败分类与健康状态（上游级短冷却）
// ---------------------------------------------------------------------------
//
// 失败分类按 C1 表（research/topics/contract-exec/README.md §错误面 +
// §x-should-retry 对照，P70/P70B 实测）：
//
//   - 429/529/5xx 等 → 未提交前换上游；
//   - `retry-after: <整数秒>` 被精确执行（parseInt；HTTP-date 形态被丢弃）；
//   - `x-should-retry: false` 压住一切（连 5xx 也不换）；`true` 覆盖状态码分类；
//   - 错误体 `"type":"overloaded_error"` 强制换（该匹配先于头检查）。
//
// Azure 扩展（P05 §6.3 两处实差；都是「不改变 C1 客户端语义」的纯增益）：
//
//   - `retry-after-ms`（毫秒）优先于整数秒 `retry-after`——Azure 主推毫秒
//     （quota.md:178/196「Use the retry-after-ms header value if present」），
//     此前被静默丢弃；
//   - 资源级 403（体含 "resource has been temporarily blocked"，滥用策略挂起 =
//     全量 403）按上游级持续故障处理：换上游 + 首次命中即冷却（非 C1 对齐的
//     显式新增判据——C1 把 403 当请求问题，这里按文本特征区分）。
//
// W18-2 扩展（W17-5 §缺 7 / 建议 1 的落地；纯上游侧增益——出站面零变化）：
//
//   - 上游 reset 头（`anthropic-ratelimit-unified-*-reset` / `x-ratelimit-reset-*`）
//     在「窗口被拒」（status=rejected）/「配额耗尽」（remaining=0）时参与冷却：
//     冷却到「重置时刻 - now」（钳制 [1s, maxRetryAfterCap]）；仅在 retry-after
//     族未产出有效值时兜底（优先级/值语义/门控裁定见 resetHeaderCooldown 头注）。
//
// 冷却：上游连续失败达阈值（cooldown_failures）→ 短时跳过（cooldown_seconds）；
// 带 `retry-after-ms`/`retry-after: <整数秒>`/reset 头的失败按该时长冷却（前两者
// C1 精确执行；reset 头见上）。资源级 403 首次命中即按固定窗口冷却。全部上游都在
// 冷却时退化为全列表重试——冷却绝不导致服务整体不可用。

// maxRetryAfterCap `retry-after` 冷却上限（守卫：上游给离谱值不把该上游长期拉黑）。
const maxRetryAfterCap = 10 * time.Minute

// shouldFailover 未提交前失败是否换上游（C1 表语义）：
//
//  1. 错误体文本含 `"type":"overloaded_error"` → 强制换（匹配先于头检查——P70B
//     xsr_false_529 的例外）；
//  2. `x-should-retry: false` → 一律不换（连 529 也压住——P70 http_429_retryfalse）；
//  3. `x-should-retry: true` → 强制换（覆盖状态码分类——P70B xsr_true_400）；
//  4. 通用状态码集合 408/409/429/401/5xx（C1 源码面 `Vl_` 判定集合）→ 换；
//  5. 403 + 资源级文本特征（`resourceBlocked`）→ 换（P05 §6.3 实差 ②：全量 403
//     是上游级持续故障，不是请求问题——新增判据，非 C1 对齐）。
//
// 其余 4xx（400/403/404/422…）是请求问题：换上游无意义，错误信封直接上抛。
func shouldFailover(status int, header http.Header, errBody []byte) bool {
	_, builtin, _ := classifyFailure(status, header, errBody)
	return builtin
}

// ---------------------------------------------------------------------------
// per-exception 换上游预算（W5-22 条目 3；P27 §router 对照表点名「按异常类给
// 预算（429 多试、400 不试）」）
// ---------------------------------------------------------------------------
//
// 分类从「状态码集合」扩到「每异常类策略表」（配置面 Config.FailoverPolicy）：
// classifyFailure 给出 C1 表语义；**显式信号**（错误体 overloaded_error /
// x-should-retry）优先且不受配置改写（forced）；**状态码类**的换上游决策经
// per-exception 预算——值 = 该类失败最多换上游次数（0 不换、N 上限、-1 不限；
// 缺省 = 内置判定，内置换上游类不限次）。预算按请求计（LiteLLM RetryPolicy 的
// per-call 计数语义），跨上游共享（failoverBudget）。

// failoverClass 失败异常类（FailoverPolicy 的键）。
type failoverClass string

const (
	classRateLimit   failoverClass = "rate_limit"   // 429
	classTimeout     failoverClass = "timeout"      // 408
	classConflict    failoverClass = "conflict"     // 409
	classAuth        failoverClass = "auth"         // 401
	classForbidden   failoverClass = "forbidden"    // 403
	classServerError failoverClass = "server_error" // 5xx
	classBadRequest  failoverClass = "bad_request"  // 其余 4xx（400/404/422…）
	classDefault     failoverClass = "default"      // 其余状态码
	// classQuota 额度/凭据耗尽（W5-25 条目 3）：等多久都没用、换上游也没用
	// ——不换、不冷却，错误信封直接上抛。依据（跨网关错误→动作对照表）：
	// Bifrost `core/failureclass.go` 的 quota 分类（「等也没用」vs rate_limit
	// 「等有用」）；uni-api `responses/events.rs:11` 把 insufficient_quota/
	// billing_hard_limit_reached 折 429（我们拆开：语义不同）；zcode-wire 业务码
	// 表把充值/套餐类标「终止（不依赖 Retry-After 时长）」。
	classQuota failoverClass = "quota"
)

// failoverClassNames 已知类名（校验报错文案）。
var failoverClassNames = "rate_limit|timeout|conflict|auth|forbidden|server_error|bad_request|default|quota"

// validFailoverClass 类名是否已知（配置校验）。
func validFailoverClass(name string) bool {
	switch failoverClass(name) {
	case classRateLimit, classTimeout, classConflict, classAuth, classForbidden, classServerError, classBadRequest, classDefault, classQuota:
		return true
	}
	return false
}

// bodyClassRules 上游错误**体文本** → 异常类的跨网关对照表（W5-25 条目 3；
// 先于状态码分类、后于显式信号）。逐条来源：
//
//   - `insufficient_quota` / `billing_hard_limit_reached`：uni-api
//     `responses/events.rs:11` 的白名单（→429 语义）；OpenRouter 文档错误码表
//     （额度类，等多久都没用——P29 §陷阱 4 原话「SDK 都不自动重试 402」）。
//   - `credit_balance_exhausted` / `organization_spend_limit_exceeded` /
//     `project_spend_limit_exceeded`：zcode-wire 业务码表（充值/套餐类 → 终止）。
//   - `payment_required`：OpenRouter `error_type` 词汇表（P29 §可复用点 7）。
//   - `upstream_credentials_exhausted` / `no_eligible_keys`：Bifrost 合成错码
//     （P29 §陷阱 15——网关自造，调用方看到的 502/503 不代表上游 5xx）。
//   - `openrouter_in_flight_budget`：OpenRouter 402 的**唯一**可等形态
//     （P29 §陷阱 4：`metadata.limit_source=openrouter_in_flight_budget` 且带
//     Retry-After 才是「等一会儿重试」）→ 按 rate_limit（可换/可等）。
//
// 匹配口径：小写 contains（与 `resourceBlockedText` 同款；错误体默认不透传，
// 本表只影响**我们自己的**换/停决策）。
var bodyClassRules = []struct {
	text string
	cls  failoverClass
}{
	{"insufficient_quota", classQuota},
	{"billing_hard_limit_reached", classQuota},
	{"credit_balance_exhausted", classQuota},
	{"organization_spend_limit_exceeded", classQuota},
	{"project_spend_limit_exceeded", classQuota},
	{"payment_required", classQuota},
	{"upstream_credentials_exhausted", classQuota},
	{"no_eligible_keys", classQuota},
	{"openrouter_in_flight_budget", classRateLimit},
}

// remapClass 错误体文本 → 异常类覆盖（nil = 无覆盖，用状态码类）。规则按声明
// 顺序首个命中即返回（quota 优先于 in-flight budget 之类不冲突——词面互斥）。
func remapClass(errBody []byte, statusCls failoverClass) failoverClass {
	if len(errBody) == 0 {
		return statusCls
	}
	lower := bytes.ToLower(errBody)
	for _, r := range bodyClassRules {
		if bytes.Contains(lower, []byte(r.text)) {
			return r.cls
		}
	}
	return statusCls
}

// failureClass 状态码 → 异常类。
func failureClass(status int) failoverClass {
	switch status {
	case http.StatusTooManyRequests:
		return classRateLimit
	case http.StatusRequestTimeout:
		return classTimeout
	case http.StatusConflict:
		return classConflict
	case http.StatusUnauthorized:
		return classAuth
	case http.StatusForbidden:
		return classForbidden
	}
	switch {
	case status >= 500:
		return classServerError
	case status >= 400:
		return classBadRequest
	}
	return classDefault
}

// classifyFailure 失败分类（C1 表语义）：返回异常类、内置换上游判定与是否显式
// 信号（forced=true 时判定来自 wire 显式信号——overloaded 文本 / x-should-retry，
// 不可被 per-exception 预算表改写；P70/P70B 实测语义）。
func classifyFailure(status int, header http.Header, errBody []byte) (cls failoverClass, builtin, forced bool) {
	cls = failureClass(status)
	// 1. 错误体 overloaded 文本 → 强制换（匹配先于头检查——P70B xsr_false_529
	// 的例外：错误体含该文本时 x-should-retry: false 也压不住）。
	if bytes.Contains(errBody, []byte(`"type":"overloaded_error"`)) {
		return cls, true, true
	}
	// 2. x-should-retry：false 一律不换（连 529 也压住——P70 http_429_retryfalse）；
	// true 覆盖状态码分类（P70B xsr_true_400）。
	switch strings.ToLower(strings.TrimSpace(header.Get("x-should-retry"))) {
	case "false":
		return cls, false, true
	case "true":
		return cls, true, true
	}
	// 3. 错误体文本 → 异常类覆盖（W5-25 条目 3 的跨网关对照表：quota 类
	// 「等也没用/换也没用」与 in-flight budget 的 402 等形态；在显式信号之后、
	// 状态码判定之前——显式信号优先是 C1 实测语义，body 类只细化状态码类）。
	cls = remapClass(errBody, cls)
	// 4. 状态码类（内置换上游判定）。
	switch cls {
	case classRateLimit, classTimeout, classConflict, classAuth, classServerError:
		return cls, true, false
	case classForbidden:
		return cls, resourceBlocked(errBody), false // 资源级文本特征（P05 §6.3 实差 ②）
	case classQuota:
		// 额度/凭据耗尽：换上游/等待都无意义（Bifrost quota 分类同旨）。
		return cls, false, false
	default:
		return cls, false, false
	}
}

// failoverBudget 本请求的 per-exception 换上游预算计数（类 → 已用次数）。请求内
// 单 goroutine 使用（serve/forward 同 goroutine）；加锁防御未来并发。
type failoverBudget struct {
	mu   sync.Mutex
	used map[failoverClass]int
}

// consume 记一次该类换上游并报告是否在预算内。budget：-1 = 不限（不计数）；
// 0 = 不换；N>0 = 最多 N 次（第 N+1 次拒绝）。nil 接收者（直接构造的测试路径）
// 按值判定、不计数。
func (b *failoverBudget) consume(cls failoverClass, budget int) bool {
	if budget < 0 {
		return true
	}
	if budget == 0 {
		return false
	}
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used == nil {
		b.used = map[failoverClass]int{}
	}
	if b.used[cls] >= budget {
		return false
	}
	b.used[cls]++
	return true
}

// failoverDecision 未提交前失败的处理决策：C1 分类 + per-exception 预算表
// （Config.FailoverPolicy；W5-22 条目 3）。返回 (allow, failure)：
//
//   - allow = 是否换下一个上游（显式信号直接采用内置判定、不消耗预算；状态码类
//     按配置值——缺省 = 内置判定，内置换上游类不限次、不计数）；
//   - failure = 本次是否计入上游健康（recordFailure）。口径与预算前语义一致：
//     判定为换上游 → 计入；「请求问题」类不换 → 不计（recordSuccess）。预算拒绝
//     但类本身是失败类（如 rate_limit 预算耗尽/配 0）→ 仍计入健康（上游确实
//     失败了，只是不再换号）。
func (s *Server) failoverDecision(b *failoverBudget, status int, header http.Header, errBody []byte) (allow, failure bool) {
	cls, builtin, forced := classifyFailure(status, header, errBody)
	if forced {
		return builtin, builtin
	}
	if v, ok := s.cfg.FailoverPolicy[string(cls)]; ok {
		if v < -1 {
			v = -1 // 防御：LoadConfig 校验拒绝 < -1；直接构造的配置按不限处理。
		}
		if b.consume(cls, v) {
			return true, true
		}
		return false, builtin
	}
	return builtin, builtin
}

// resourceBlockedText 资源级 403 的错误体文本特征（小写）。Azure 滥用策略挂起
// 的官方症状原文：「Error message contains "resource has been temporarily
// blocked"；All requests to the resource return 403」（P05 raw/trouble.md §HTTP
// 403 "Resource suspended (abuse policy)"，2026-10-14 抓）。
var resourceBlockedText = []byte("resource has been temporarily blocked")

// resourceBlocked 错误体是否资源级 403 特征（大小写不敏感）。
func resourceBlocked(errBody []byte) bool {
	return bytes.Contains(bytes.ToLower(errBody), resourceBlockedText)
}

// resourceBlockedCooldown 资源级 403 的冷却窗口。首次命中即冷却——滥用挂起是
// 持续故障（人工申诉才解除），不经连续失败阈值；上限由 maxRetryAfterCap 约束。
const resourceBlockedCooldown = 5 * time.Minute

// upstreamCooldown 本次上游失败的冷却时长：
//
//   - `retry-after-ms`/`retry-after` 头命中 → 按头值（上游显式指定，最忠实）；
//   - 上游 reset 头命中（窗口被拒/配额耗尽）→ 按「重置时刻 - now」（W18-2；
//     钳制 [1s, maxRetryAfterCap]，见 resetHeaderCooldown）；
//   - 资源级 403 无头 → 固定窗口（首次命中即冷却）；
//   - 其余 → 0（走连续失败阈值路径）。
func upstreamCooldown(status int, header http.Header, errBody []byte) time.Duration {
	if d := retryAfterSeconds(header); d > 0 {
		return d
	}
	if d := resetHeaderCooldown(header, time.Now()); d > 0 {
		return d
	}
	if status == http.StatusForbidden && resourceBlocked(errBody) {
		return resourceBlockedCooldown
	}
	return 0
}

// retryAfterSeconds 解析 `retry-after-ms` / `retry-after` 头为冷却时长。
//
//   - `retry-after-ms`（毫秒）优先：Azure 主推形态（quota.md:178/196「Use the
//     retry-after-ms header value if present」）——此前只认整数秒会静默丢弃
//     （P05 §6.3 实差 ①）；
//   - `retry-after` 整数秒兜底：CC C1 实测整数秒形态被 `parseInt` 精确执行
//     （无 jitter）；HTTP-date 形态被静默丢弃（parseInt→NaN→回退指数退避）——
//     故只认前导十进制整数（HTTP-date 首字符是字母 → 丢弃）。
//
// 返回 0 = 未命中/不认（丢弃）。
func retryAfterSeconds(h http.Header) time.Duration {
	if d := leadingDuration(strings.TrimSpace(h.Get("retry-after-ms")), time.Millisecond); d > 0 {
		return d
	}
	if d := leadingDuration(strings.TrimSpace(h.Get("retry-after")), time.Second); d > 0 {
		return d
	}
	return 0
}

// leadingDuration 解析开头的前导十进制整数并按 unit 换算为时长；超过上限
// （maxRetryAfterCap）一律截到上限（先做除法守卫，换算不溢出）。返回 0 = 未命中。
func leadingDuration(s string, unit time.Duration) time.Duration {
	n, width := leadingInt([]byte(s))
	if width == 0 || n <= 0 {
		return 0
	}
	if n > int64(maxRetryAfterCap/unit) {
		return maxRetryAfterCap
	}
	return time.Duration(n) * unit
}

// ---------------------------------------------------------------------------
// 上游 reset 头 → 精确冷却（W18-2；W17-5 §缺 7 / 建议 1 的落地）
// ---------------------------------------------------------------------------
//
// 上游在限流面除了 retry-after 还会带「窗口何时重置」的 reset 头；把「重置时刻
// − now」当冷却窗口用 = CPA `helps/claude_ratelimit.go:125-203` 的精确冷却范式
// （sub2api 同旨：`ratelimit_service.go:1385-1412` 的 `calculateOpenAI429ResetTime`
// 与 `:1477-1490` 的 `parseAnthropicResetTimestamp`）。读取面两族：
//
//   - Anthropic unified 族：`anthropic-ratelimit-unified[-5h|-7d|-7d_oi]-status`
//     为 `rejected`（窗口被拒）时，对应 `...-reset`（Unix 秒；>1e11 按毫秒
//     折算——sub2api 同款自动识别）参与冷却；
//   - OpenAI / xAI 族：`x-ratelimit-reset-<requests|tokens>`（xAI 别名
//     `x-rate-limit-reset-*`）在对应 `...-remaining-<dim>` 为 0（配额耗尽——
//     sub2api `calculateOpenAI429ResetTime` 的 used_percent≥100 门的等价物）时
//     参与冷却；值语义按实证三形态：整数（≥1e12 毫秒 epoch / ≥1e9 秒 epoch /
//     其余相对秒数）+ Go 时长串（OpenAI 直连 `1s`/`6m0s`；sub2api
//     `xai/quota.go parseResetHeader` 同口径）。
//
// Anthropic unified reset 的值语义有第三源直接佐证：CC 自己（C1 客户端）把
// `anthropic-ratelimit-unified[-<w>]-reset` 读作 **Unix 秒**（`claudeAiLimits.ts`
// 「resets_at: number // unix epoch seconds」；`withRetry.ts:815-820` 429 长等待
// 面 `Number(resetHeader)*1000 - Date.now()`，过去时刻 → null、cap 6h）。
//
// 判据纪律（防「把上游冷却成死号」）：
//
//   - **值域钳制**：reset 推导的冷却 clamp 到 [resetCooldownFloor,
//     maxRetryAfterCap]——上限与 retry-after 路径同源（被拒的 7d 窗口不把上游
//     一次罚到一周）；下限 1s（sub2api 对亚秒时长有同款地板）；过去时刻/零值/
//     畸形一律不产出冷却（退回连续失败阈值路径）；
//   - **门控**：只有「窗口确实被拒 / 配额确实耗尽」才采信（status=rejected /
//     remaining=0）——sub2api 原话：未耗尽时 reset 只代表窗口信息、不能证明
//     配额耗尽，拿它冷却会把健康账号长时间排除（`ratelimit_service.go:1414-1416`）；
//   - **优先级**：`retry-after` 族产出有效值时**恒赢**——它是请求级「等多久再
//     试」的显式指令（C1 精确执行面，W14-2 已锁定），reset 是窗口元数据，仅在
//     retry-after 未产出有效值时兜底（W17-5 建议 1 的「仅在无 retry-after 时
//     启用」；sub2api 429 路径同序：先 `parseRetryAfterResetTime` 后 reset 头）。
//     多窗口/多族并存时取**最晚** deadline（与 CPA 取最晚被拒窗口同旨）。登记
//     差异（诚实面）：CC 在 persistent 429 面 reset 优先（cap 6h，withRetry.ts:437
//     ——客户端对自身窗口的等待）、CPA 取 max(retry-after, reset)（credential 级
//     benching）——场景不同；若未来翻优先级，单行可改（反证 V3 已备）。
//
// 读取面只影响**我们自己的**冷却窗口（上游侧信号）——下游出口面零变化，不违反
// C1（C1 只约束我们对下游的行为）。

// resetCooldownFloor reset 头推导冷却的下限（亚秒窗口按 1s 执行——sub2api
// `xai/quota.go` 对时长串有同款地板；同时挡住「deadline 与 now 同秒」的零值面）。
const resetCooldownFloor = time.Second

// anthropicUnifiedWindows Anthropic unified 族的窗口枚举（"" = 顶层 unified 头）。
var anthropicUnifiedWindows = []string{"", "5h", "7d", "7d_oi"}

// openAIResetDimensions OpenAI 族窗口维度（`x-ratelimit-reset-<dim>` 的 <dim>）。
var openAIResetDimensions = []string{"requests", "tokens"}

// resetHeaderCooldown 上游 reset 头 → 冷却时长（retry-after 未产出有效值时的
// 兜底；now 显式传入便于测试）。返回 0 = 未命中/不采信（退回阈值路径）。
func resetHeaderCooldown(header http.Header, now time.Time) time.Duration {
	d := anthropicResetCooldown(header, now)
	if d2 := openAIResetCooldown(header, now); d2 > d {
		d = d2
	}
	return d
}

// anthropicResetCooldown Anthropic unified 族：status == rejected 的窗口取
// `...-reset`（Unix 秒；>1e11 按毫秒折算），返回最晚 deadline 折算的冷却。
func anthropicResetCooldown(header http.Header, now time.Time) time.Duration {
	var latest time.Time
	for _, w := range anthropicUnifiedWindows {
		status := strings.ToLower(strings.TrimSpace(header.Get(unifiedHeaderName(w, "status"))))
		if status != "rejected" {
			continue // 门控：只有被拒窗口的 reset 才是「等到什么时候」信号。
		}
		if t, ok := parseUnixTimestamp(header.Get(unifiedHeaderName(w, "reset"))); ok && t.After(latest) {
			latest = t
		}
	}
	return clampResetCooldown(latest, now)
}

// unifiedHeaderName Anthropic unified 族头名（window 为空 = 顶层 unified 头）。
func unifiedHeaderName(window, suffix string) string {
	if window == "" {
		return "anthropic-ratelimit-unified-" + suffix
	}
	return "anthropic-ratelimit-unified-" + window + "-" + suffix
}

// openAIResetCooldown OpenAI / xAI 族：`...-remaining-<dim>` == 0 的维度取
// `x-ratelimit-reset-<dim>`（别名 `x-rate-limit-reset-<dim>`），返回最晚 deadline
// 折算的冷却。
func openAIResetCooldown(header http.Header, now time.Time) time.Duration {
	var latest time.Time
	for _, dim := range openAIResetDimensions {
		if !quotaExhausted(header, dim) {
			continue // 门控：未耗尽维度的 reset 只是窗口元数据（sub2api 口径）。
		}
		raw := firstHeaderValue(header,
			"x-ratelimit-reset-"+dim,
			"x-rate-limit-reset-"+dim)
		if t, ok := parseOpenAIResetTime(raw, now); ok && t.After(latest) {
			latest = t
		}
	}
	return clampResetCooldown(latest, now)
}

// quotaExhausted 该维度配额是否耗尽（remaining == 0；两种头名别名）。缺失/
// 非整零/畸形 → false（保守：没有「耗尽」证据不采信 reset）。
func quotaExhausted(header http.Header, dim string) bool {
	raw := firstHeaderValue(header,
		"x-ratelimit-remaining-"+dim,
		"x-rate-limit-remaining-"+dim)
	if raw == "" {
		return false
	}
	n, width := leadingInt([]byte(raw))
	return width == len(raw) && n == 0
}

// parseUnixTimestamp 解析 Unix 秒时间戳（整数；>1e11 按毫秒折算——sub2api
// `parseAnthropicResetTimestamp` 同款自动识别）。非纯整数/非正 → 未命中
// （保守：尾随垃圾/浮点/负值一律丢弃）。
func parseUnixTimestamp(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	n, width := leadingInt([]byte(raw))
	if width == 0 || width != len(raw) || n <= 0 {
		return time.Time{}, false
	}
	if n > 1e11 { // 毫秒 epoch（秒级时间戳要到公元 5138 年才越界）
		n /= 1000
	}
	return time.Unix(n, 0), true
}

// parseOpenAIResetTime 解析 `x-ratelimit-reset-*` 值（实证三形态，sub2api
// `xai/quota.go parseResetHeader` 口径）：
//
//   - 纯整数：≥1e12 毫秒 epoch / ≥1e9 秒 epoch / 其余相对秒数（"60" = now+60s）；
//   - Go 时长串：OpenAI 直连形态（"1s"/"6m0s"）；亚秒地板 1s（sub2api 同款）。
//
// 非上述形态（RFC3339/HTTP-date/垃圾）→ 未命中（保守：sub2api/CPA 多认的形态
// 无真机样本佐证，不扩）。
func parseOpenAIResetTime(raw string, now time.Time) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	if n, width := leadingInt([]byte(raw)); width > 0 && width == len(raw) {
		switch {
		case n >= 1_000_000_000_000: // 毫秒 epoch
			return time.Unix(n/1000, 0), true
		case n >= 1_000_000_000: // 秒 epoch
			return time.Unix(n, 0), true
		default: // 相对秒数
			return now.Add(time.Duration(n) * time.Second), true
		}
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		if d < time.Second {
			d = time.Second
		}
		return now.Add(d), true
	}
	return time.Time{}, false
}

// clampResetCooldown reset deadline → 冷却时长：过去时刻/零值 → 0（不冷却）；
// 未来时刻钳制到 [resetCooldownFloor, maxRetryAfterCap]。
func clampResetCooldown(deadline, now time.Time) time.Duration {
	if deadline.IsZero() || !deadline.After(now) {
		return 0
	}
	d := deadline.Sub(now)
	if d < resetCooldownFloor {
		return resetCooldownFloor
	}
	if d > maxRetryAfterCap {
		return maxRetryAfterCap
	}
	return d
}

// firstHeaderValue 返回第一个非空头的值（别名按声明顺序取先命中者）。
func firstHeaderValue(h http.Header, names ...string) string {
	for _, n := range names {
		if v := strings.TrimSpace(h.Get(n)); v != "" {
			return v
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// 上游健康状态（连续失败 → 短冷却）
// ---------------------------------------------------------------------------

// upstreamHealth 单个上游的健康状态（与 cfg.Upstreams 下标一一对应）。
type upstreamHealth struct {
	mu    sync.Mutex
	fails int       // 连续失败计数（成功清零）
	until time.Time // 冷却截止（零值 = 未冷却）
	// ladderStep 冷却阶梯当前档位（W5-32 条目 2）：每次**进入冷却**推进一档，
	// 成功清零；无阶梯配置（cooldown_ladder 空）时恒 0、不参与窗口计算。
	ladderStep int

	// 失败率窗口（W5-22 条目 3；LiteLLM 范式的最小版）：窗口起点 + 窗口内请求/
	// 失败计数。失败 = recordFailure 调用（上游侧失败）；「请求问题」类 4xx 走
	// recordSuccess（只计请求不计失败——与健康状态纪律一致）。
	winStart time.Time
	winReq   int
	winFail  int
}

// Diagnostics 服务壳运行计数（尝试次数 / 换上游次数 / 冷却 / 预提交失败已收 usage /
// 流式管道的出口诊断 / 批量面诊断 / per-attempt usage 记账）。
//
// **JSON 标签 = 管理面 `GET /admin/diagnostics` 的线上形状**（W5-33；字段名
// snake_case 是对外契约——改字段名/加字段都要同步报告与测试）。
type Diagnostics struct {
	Requests      uint64 `json:"requests"`       // 下游请求数
	Attempts      uint64 `json:"attempts"`       // 上游尝试次数（一次请求最多 len(upstreams) 次）
	Failovers     uint64 `json:"failovers"`      // 换上游次数（失败且链上还有下一个）
	CooldownSkips uint64 `json:"cooldown_skips"` // 因冷却被跳过的上游次数
	// CooldownTrips 进入冷却（trip）次数（W68-5）：从**非冷却态**置入冷却窗口即 +1，
	// 连续失败阈值、显式 `retry-after`（含 `retry-after-ms` / reset 头）与资源级
	// 403（`upstreamCooldown` 给出 5m）三条路径都算；同一次 `recordFailure` 里多面
	// 同时触发只 +1；已在冷却中仅被显式 `retry-after` 延长窗口时**不** +1（不是新
	// trip）。失败率触发的冷却单独计入 RateCooldownTrips（见下）。
	CooldownTrips uint64 `json:"cooldown_trips"`
	// RateCooldownTrips 失败率触发的冷却次数（W5-22 条目 3：窗口内请求 ≥ min 且
	// 失败率 > threshold——与 CooldownTrips 分开计数，测试与排障可区分触发面）。
	RateCooldownTrips uint64 `json:"rate_cooldown_trips"`
	// SameUpstreamRetries 同上游瞬时类失败的重试次数（W5-32 条目 1；P65B 裁定
	// #1）：中间重试不计健康/不冷却，只在终态记一次——本计数是唯一观测面
	//（Attempts 也含这些重试，但分不出「同台重试」与「换台」）。
	SameUpstreamRetries uint64 `json:"same_upstream_retries"`
	// PartialUsageAttempts 预提交失败且已观测到 partial usage 的尝试数（W5-15：
	// P27 建议的 usage 回收落在诊断面——已收用量不并入下游可见流，见
	// partial_usage.go 头注）。
	PartialUsageAttempts uint64 `json:"partial_usage_attempts"`
	// PartialUsageInTokens/PartialUsageOutTokens 上述尝试累计观测到的 token 数
	//（诊断/记账面；未来接 billing-paths 的 per-attempt 记账）。
	PartialUsageInTokens  int64 `json:"partial_usage_in_tokens"`
	PartialUsageOutTokens int64 `json:"partial_usage_out_tokens"`
	// Pipe* 跨协议流式管道的出口诊断累计（`pairs.StreamPipe.Diagnostics()` 的
	// 消费点——W5-11 §剩余面 5：诊断此前只在管道上可查，无消费方）。同协议直通
	//（无管道）不计。
	//
	// PipeDroppedThinking 未下发到 OpenAI 线的 thinking / redacted_thinking 增量数
	//（枢纽腿 = redacted 载荷增量与空思考增量——W42 ③ 起思考文本走载体、不计丢弃；
	// 出口腿 = 空思考增量，redacted 载荷走载体、不计）；PipeReasoningDeltas 以
	// delta.reasoning_content 下发的思考增量数（W42 ③ 起含枢纽腿）；
	// PipeSignatureDropped 未写进载体的签名载荷增量数（W5-19 条目 5：从
	// PipeDroppedThinking 拆出的单列——与批量面 BatchSignatureDropped 同口径，
	// 带签名案例可逐项对拍）。
	PipeDroppedThinking  uint64 `json:"pipe_dropped_thinking"`
	PipeReasoningDeltas  uint64 `json:"pipe_reasoning_deltas"`
	PipeSignatureDropped uint64 `json:"pipe_signature_dropped"`
	// PipeToolArgsRepaired 出口修复层改写的工具参数块数；PipeToolArgsFailed
	// 判失败态的块数（不发坏 JSON、终态换 error 帧——见 pairs/tool_repair.go）。
	PipeToolArgsRepaired uint64 `json:"pipe_tool_args_repaired"`
	PipeToolArgsFailed   uint64 `json:"pipe_tool_args_failed"`
	// Batch* 非流式（批量）转换的诊断累计（W5-18 条目 2：`pairs.BatchDiagnostics`
	// 的消费点——批量面此前没有等价于 `StreamPipe.Diagnostics()` 的出口）。口径
	// 与 Pipe* 的思考面一致：BatchDroppedThinking = 非流式渲染丢弃的思考块数
	// （请求面无兼容签名 / 非 assistant / 空文本且无可识别签名 / 枢纽腿或空载荷的
	// redacted——W42 ①③ 起响应面思考文本与 redacted 载荷走载体），
	// BatchReasoningMapped = 以 `message.reasoning_content` 落下的思考块数，
	// BatchSignatureDropped = 思考块签名未落地的块数（W5-19 条目 5：OpenAI 线无
	// 签名字段——与流式面 PipeSignatureDropped 同口径，带签名案例可逐项对拍）。
	BatchDroppedThinking  uint64 `json:"batch_dropped_thinking"`
	BatchReasoningMapped  uint64 `json:"batch_reasoning_mapped"`
	BatchSignatureDropped uint64 `json:"batch_signature_dropped"`
	// AttemptUsage* per-attempt usage 记账（W5-18 条目 4：探针从「仅预提交窗口」
	// 扩到每次尝试全窗口——流式与非流式、成功与失败都记；纯观测，绝不进下游流）。
	// AttemptUsageRecords = 有 usage 观测的尝试数；Failed 三项是其中失败尝试的
	// 子集（预提交失败 + 提交后失败）。与 PartialUsage*（W5-15 预提交失败回收面）
	// 的关系：后者是前者的一个子集（失败且预提交），两者都保留、语义各自文档化。
	AttemptUsageRecords         uint64 `json:"attempt_usage_records"`
	AttemptUsageInTokens        int64  `json:"attempt_usage_in_tokens"`
	AttemptUsageOutTokens       int64  `json:"attempt_usage_out_tokens"`
	AttemptUsageFailedRecords   uint64 `json:"attempt_usage_failed_records"`
	AttemptUsageFailedInTokens  int64  `json:"attempt_usage_failed_in_tokens"`
	AttemptUsageFailedOutTokens int64  `json:"attempt_usage_failed_out_tokens"`
	// TaskUsage* 任务面终态 usage 记账（Slice 4：每任务一条记录；本计数只计有
	// usage 观测的终态任务——零观测不记，同 AttemptUsage* 纪律）。failed/cancelled
	// 无终态 usage，其下界在 AttemptUsage*（per-attempt 面，见 attempts.go §per-task）。
	TaskUsageRecords   uint64 `json:"task_usage_records"`
	TaskUsageInTokens  int64  `json:"task_usage_in_tokens"`
	TaskUsageOutTokens int64  `json:"task_usage_out_tokens"`
	// Tasks* 任务面运维计数（W36-1 S7；缓解缺口 C6——任务面此前只有 usage 面）。
	// 口径 = **托管面**（store 侧；透传任务不计数——网关不托管其终态，见报告
	// §口径）：
	//   - TasksCreated 托管任务创建数（store 新建；幂等重放不计数）；
	//   - TasksTerminal* 终态落定数（四值；与 TasksCreated 的差 = 运行中/未落终态
	//     的任务数——长任务运维可对账）；
	//   - TasksIdemReplays 幂等键重放命中数（同键同体重提——返回同任务 id，不建
	//     新任务、不触上游）。
	TasksCreated            uint64 `json:"tasks_created"`
	TasksTerminalCompleted  uint64 `json:"tasks_terminal_completed"`
	TasksTerminalFailed     uint64 `json:"tasks_terminal_failed"`
	TasksTerminalIncomplete uint64 `json:"tasks_terminal_incomplete"`
	TasksTerminalCancelled  uint64 `json:"tasks_terminal_cancelled"`
	TasksIdemReplays        uint64 `json:"tasks_idem_replays"`
	// PassthroughUsage* 透传任务终态 usage 观测（W36-2 S4；缓解缺口 B5）：透传
	// 任务回访响应（retrieve/cancel，非 SSE、2xx）经网关时**只读旁路**观测到的
	// 终态 usage——每任务一条（路由条目去重），日志行 source=passthrough。零额外
	// 上游调用；观测面绝不进下游字节（响应原样转发）。零观测（usage 缺/null 或
	// 非终态）不计数——同 AttemptUsage*/TaskUsage* 纪律。
	PassthroughUsageRecords   uint64 `json:"passthrough_usage_records"`
	PassthroughUsageInTokens  int64  `json:"passthrough_usage_in_tokens"`
	PassthroughUsageOutTokens int64  `json:"passthrough_usage_out_tokens"`
	// PassthroughFace* 能力矩阵 L2 被动证据（W36-2 S8；topic `service-task-surface`
	// §③ L2 行）：passthrough 往返（create/回访/续流）上游 404/405 的**结论性
	// 失败**计数（结论闸：网络失败/5xx/429 不计——不下结论）。404/405 两值分开
	// 计数；PassthroughFaceDegrades = 进程内降级转换数（passthrough→managed；
	// ≤ 上游数；重启清零，见 Config.TaskFaceDegrade）。
	PassthroughFace404      uint64 `json:"passthrough_face_404"`
	PassthroughFace405      uint64 `json:"passthrough_face_405"`
	PassthroughFaceDegrades uint64 `json:"passthrough_face_degrades"`
	// Public* 公开服务面运维计数（P0-6；W58-1）。口径 = 拒绝/签发**事件数**
	//（进程内累计、重启清零；与 ring 事件同点——审计面双落点）：
	//   - PublicSessionsCreated 匿名会话签发数（public.enabled=true 时）；
	//   - PublicRateLimited 服务侧限流 429 数（per-IP 令牌桶 + 全局并发闸）；
	//   - PublicQuotaRejected 日预算耗尽 429 数（daily_quota_exhausted）；
	//   - PublicCapacityRejected 容量耗尽 429 数（capacity_exhausted——池全不可用）；
	//   - PublicClassRejected 成本档拒绝 400 数（请求档 > public.max_cost_class）。
	PublicSessionsCreated  uint64 `json:"public_sessions_created"`
	PublicRateLimited      uint64 `json:"public_rate_limited"`
	PublicQuotaRejected    uint64 `json:"public_quota_rejected"`
	PublicCapacityRejected uint64 `json:"public_capacity_rejected"`
	PublicClassRejected    uint64 `json:"public_class_rejected"`
}

// diagCounters Diagnostics 的原子计数器（Server 内部）。
type diagCounters struct {
	requests              atomic.Uint64
	attempts              atomic.Uint64
	failovers             atomic.Uint64
	cooldownSkips         atomic.Uint64
	cooldownTrips         atomic.Uint64
	rateCooldownTrips     atomic.Uint64
	sameUpstreamRetries   atomic.Uint64
	partialUsageAttempts  atomic.Uint64
	partialUsageIn        atomic.Int64
	partialUsageOut       atomic.Int64
	pipeDroppedThinking   atomic.Uint64
	pipeReasoningDeltas   atomic.Uint64
	pipeSignatureDropped  atomic.Uint64
	pipeToolArgsRepaired  atomic.Uint64
	pipeToolArgsFailed    atomic.Uint64
	batchDroppedThinking  atomic.Uint64
	batchReasoningMapped  atomic.Uint64
	batchSignatureDropped atomic.Uint64
	attemptUsageRecords   atomic.Uint64
	attemptUsageIn        atomic.Int64
	attemptUsageOut       atomic.Int64
	attemptUsageFailRecs  atomic.Uint64
	attemptUsageFailIn    atomic.Int64
	attemptUsageFailOut   atomic.Int64
	taskUsageRecords      atomic.Uint64
	taskUsageIn           atomic.Int64
	taskUsageOut          atomic.Int64
	// Tasks* 任务面运维计数（W36-1 S7；口径见 Diagnostics 字段注释）。
	tasksCreated            atomic.Uint64
	tasksTerminalCompleted  atomic.Uint64
	tasksTerminalFailed     atomic.Uint64
	tasksTerminalIncomplete atomic.Uint64
	tasksTerminalCancelled  atomic.Uint64
	tasksIdemReplays        atomic.Uint64
	// Passthrough* 透传面观测/证据计数（W36-2 S4/S8；口径见 Diagnostics 字段注释）。
	passthroughUsageRecords atomic.Uint64
	passthroughUsageIn      atomic.Int64
	passthroughUsageOut     atomic.Int64
	passthroughFace404      atomic.Uint64
	passthroughFace405      atomic.Uint64
	passthroughFaceDegrades atomic.Uint64
	// Public* 公开服务面计数（P0-6；口径见 Diagnostics 字段注释）。
	publicSessionsCreated  atomic.Uint64
	publicRateLimited      atomic.Uint64
	publicQuotaRejected    atomic.Uint64
	publicCapacityRejected atomic.Uint64
	publicClassRejected    atomic.Uint64
}

// Diagnostics 当前计数快照（并发安全）。
func (s *Server) Diagnostics() Diagnostics {
	return Diagnostics{
		Requests:                    s.diag.requests.Load(),
		Attempts:                    s.diag.attempts.Load(),
		Failovers:                   s.diag.failovers.Load(),
		CooldownSkips:               s.diag.cooldownSkips.Load(),
		CooldownTrips:               s.diag.cooldownTrips.Load(),
		RateCooldownTrips:           s.diag.rateCooldownTrips.Load(),
		SameUpstreamRetries:         s.diag.sameUpstreamRetries.Load(),
		PartialUsageAttempts:        s.diag.partialUsageAttempts.Load(),
		PartialUsageInTokens:        s.diag.partialUsageIn.Load(),
		PartialUsageOutTokens:       s.diag.partialUsageOut.Load(),
		PipeDroppedThinking:         s.diag.pipeDroppedThinking.Load(),
		PipeReasoningDeltas:         s.diag.pipeReasoningDeltas.Load(),
		PipeSignatureDropped:        s.diag.pipeSignatureDropped.Load(),
		PipeToolArgsRepaired:        s.diag.pipeToolArgsRepaired.Load(),
		PipeToolArgsFailed:          s.diag.pipeToolArgsFailed.Load(),
		BatchDroppedThinking:        s.diag.batchDroppedThinking.Load(),
		BatchReasoningMapped:        s.diag.batchReasoningMapped.Load(),
		BatchSignatureDropped:       s.diag.batchSignatureDropped.Load(),
		AttemptUsageRecords:         s.diag.attemptUsageRecords.Load(),
		AttemptUsageInTokens:        s.diag.attemptUsageIn.Load(),
		AttemptUsageOutTokens:       s.diag.attemptUsageOut.Load(),
		AttemptUsageFailedRecords:   s.diag.attemptUsageFailRecs.Load(),
		AttemptUsageFailedInTokens:  s.diag.attemptUsageFailIn.Load(),
		AttemptUsageFailedOutTokens: s.diag.attemptUsageFailOut.Load(),
		TaskUsageRecords:            s.diag.taskUsageRecords.Load(),
		TaskUsageInTokens:           s.diag.taskUsageIn.Load(),
		TaskUsageOutTokens:          s.diag.taskUsageOut.Load(),
		TasksCreated:                s.diag.tasksCreated.Load(),
		TasksTerminalCompleted:      s.diag.tasksTerminalCompleted.Load(),
		TasksTerminalFailed:         s.diag.tasksTerminalFailed.Load(),
		TasksTerminalIncomplete:     s.diag.tasksTerminalIncomplete.Load(),
		TasksTerminalCancelled:      s.diag.tasksTerminalCancelled.Load(),
		TasksIdemReplays:            s.diag.tasksIdemReplays.Load(),
		PassthroughUsageRecords:     s.diag.passthroughUsageRecords.Load(),
		PassthroughUsageInTokens:    s.diag.passthroughUsageIn.Load(),
		PassthroughUsageOutTokens:   s.diag.passthroughUsageOut.Load(),
		PassthroughFace404:          s.diag.passthroughFace404.Load(),
		PassthroughFace405:          s.diag.passthroughFace405.Load(),
		PassthroughFaceDegrades:     s.diag.passthroughFaceDegrades.Load(),
		PublicSessionsCreated:       s.diag.publicSessionsCreated.Load(),
		PublicRateLimited:           s.diag.publicRateLimited.Load(),
		PublicQuotaRejected:         s.diag.publicQuotaRejected.Load(),
		PublicCapacityRejected:      s.diag.publicCapacityRejected.Load(),
		PublicClassRejected:         s.diag.publicClassRejected.Load(),
	}
}

// cooldownFailures 连续失败阈值（缺省 DefaultCooldownFailures）。
func (s *Server) cooldownFailures() int {
	if s.cfg.CooldownFailures > 0 {
		return s.cfg.CooldownFailures
	}
	return DefaultCooldownFailures
}

// cooldownWindow 冷却窗口（缺省 DefaultCooldownSeconds）。
func (s *Server) cooldownWindow() time.Duration {
	if s.cfg.CooldownSeconds > 0 {
		return time.Duration(s.cfg.CooldownSeconds) * time.Second
	}
	return DefaultCooldownSeconds * time.Second
}

// candidates 上游尝试序列（配置顺序；冷却中的上游跳过）。全部冷却时退化为全列表
// ——冷却绝不导致服务整体不可用（宁可再试一个冷却中的上游，也不空转 502）。
// 无模型上下文 = 不按 `models[].upstreams` 限制（serve 走 candidatesForModel）。
func (s *Server) candidates(now time.Time) (idxs []int, skipped uint64) {
	return s.candidatesFor(now, false)
}

// candidatesFor 上游尝试序列（无模型上下文：等价 model=""，不限制上游）。
func (s *Server) candidatesFor(now time.Time, taskFace bool) (idxs []int, skipped uint64) {
	return s.candidatesForModel(now, taskFace, "")
}

// candidatesForModel 上游尝试序列（A2A-L19-W81-2：模型 → 上游白名单）。
// taskFace=true 时 = 任务面序列：`task_face: off` 的上游**绝不**参与任务流量
// ——create 跳过、托管任务的上游尝试/failover 链同样跳过（Slice 5 能力表，见
// task_passthrough.go 头注）。model 命中 `models[]` 且声明了非空 `upstreams` 时，
// 候选只含白名单内的上游（按 `upstreams[].name` 精确匹配）。
//
// 过滤顺序：白名单 → taskFace → 冷却。白名单外的上游既不入链也不计
// cooldownSkips（它们不是「冷却跳过」，是路由排除）。其余语义与 candidates
// 逐字一致（冷却跳过；全冷却 → **过滤后**列表兜底——绝不回退到白名单外）。
// 白名单过滤后为空 → 返回空链（调用方走既有「无候选」错误，绝不空转全列表）。
func (s *Server) candidatesForModel(now time.Time, taskFace bool, model string) (idxs []int, skipped uint64) {
	allow := s.cfg.modelUpstreams(model)
	eligible := func(i int) bool {
		if taskFace && s.cfg.Upstreams[i].taskFace() == taskFaceOff {
			return false
		}
		if allow != nil && !allow[s.cfg.Upstreams[i].Name] {
			return false
		}
		return true
	}
	idxs = make([]int, 0, len(s.cfg.Upstreams))
	for i := range s.cfg.Upstreams {
		if !eligible(i) {
			continue
		}
		if s.cooling(i, now) {
			skipped++
			continue
		}
		idxs = append(idxs, i)
	}
	if len(idxs) == 0 {
		for i := range s.cfg.Upstreams {
			if eligible(i) {
				idxs = append(idxs, i)
			}
		}
	}
	return idxs, skipped
}

// cooling 该上游是否在冷却窗口内。
func (s *Server) cooling(idx int, now time.Time) bool {
	h := &s.health[idx]
	h.mu.Lock()
	defer h.mu.Unlock()
	return now.Before(h.until)
}

// recordFailure 记一次上游失败：连续失败 +1。三种冷却（取较晚者）：
//
//   - 显式头（`retry-after` 整数秒/`retry-after-ms`，C1：精确执行；或 W18-2 的
//     reset 头——窗口被拒/配额耗尽时按「重置时刻」）→ 按该时长冷却；
//   - 连续失败达阈值 → 按配置窗口冷却（配置冷却阶梯时按阶梯档位升级——W5-32
//     条目 2）；
//   - 失败率（W5-22 条目 3，LiteLLM 范式的最小版）：窗口内请求 ≥ min 且失败率 >
//     threshold → 按配置窗口冷却（间歇性失败场景连续计数会漏——P27 §router
//     对照表点名）。
//
// **冷却期内失败不延长冷却**（W5-32 条目 3；P65B 裁定 #3 推荐 a）：本上游已在
// 冷却窗口内时，阈值/失败率两条路径**都不**重算窗口（`until=now+window` 的反复
// 取较晚者会让一次持续故障把冷却无限拉长——Parrot `cooldown.py:204-260` 三防闸
// #1 的语义：冷却中不推进阶梯、只更新 message）。显式 `retry-after` 例外：上游
// 明确给了「等到什么时候」仍被精确执行（C1 语义优先于防延长；取较晚者——只在
// 更长时才覆盖）。计数（fails/失败率窗口）照记：冷却中失败仍是失败证据，只是
// 不再延长惩罚窗口。
//
// 冷却阶梯（配置 `cooldown_ladder` 非空时）：连续**进入冷却**（trip）逐级取
// 下一档窗口；成功清零（recordSuccess 复位档位）；冷却中失败不推进档位（与
// 「不延长」同一条防爆发闸）。
//
// 计数口径（W68-5）：CooldownTrips 只记「新进入冷却」——三条置入路径（阈值 /
// 显式 retry-after / 资源级 403）任一真正置入即 +1，同一次调用不双计，冷却中
// 延长不重计（字段注见 Diagnostics.CooldownTrips）。
func (s *Server) recordFailure(idx int, retryAfter time.Duration) {
	h := &s.health[idx]
	h.mu.Lock()
	h.fails++
	now := time.Now()
	coolingAtStart := now.Before(h.until)
	var until time.Time
	if retryAfter > 0 {
		// 显式 retry-after：即使已在冷却中也按头值更新（C1 精确执行；上限
		// maxRetryAfterCap 由 retryAfterSeconds/leadingDuration 保证）。
		until = now.Add(retryAfter)
	}
	extend := false
	ladderAdvanced := false
	countedTrip := false // W68-5：本次是否已由阈值路径计入 CooldownTrips
	if !coolingAtStart {
		if h.fails >= s.cooldownFailures() {
			if w := now.Add(s.cooldownWindowFor(h)); w.After(until) {
				until = w
			}
			s.diag.cooldownTrips.Add(1)
			countedTrip = true
			extend = true
			ladderAdvanced = s.advanceLadderStep(h)
		}
		if s.observeFailureWindow(h, now) {
			if w := now.Add(s.cooldownWindowFor(h)); w.After(until) {
				until = w
			}
			s.diag.rateCooldownTrips.Add(1)
			extend = true
			if !ladderAdvanced {
				ladderAdvanced = s.advanceLadderStep(h)
			}
		}
	} else {
		// 冷却中：阈值/失败率路径不延长（防无限延长），只记计数与窗口观测。
		s.observeFailureWindow(h, now)
	}
	tripped := until.After(h.until)
	// W68-5：显式 retry-after / 资源级 403（`upstreamCooldown` → `recordFailure(idx,
	// 5m)`）单独置入冷却也算一次 trip（W68-2 矩阵 case 01/07 此前只置窗口、不计数）。
	// 三条守卫保证「每个新 trip 恰好 +1」：只在真正置入时计（tripped）、只在非冷却
	// 态计（冷却中延长不是新 trip）、阈值路径已计过的不双计。
	if tripped && !coolingAtStart && !countedTrip && retryAfter > 0 {
		s.diag.cooldownTrips.Add(1)
	}
	if tripped {
		h.until = until
	}
	fails := h.fails
	step := h.ladderStep
	winReq, winFail := h.winReq, h.winFail
	h.mu.Unlock()
	if tripped {
		// 管理面 ring（W5-33）：冷却进入事件（换上游/复位在 server/admin 侧记）。
		s.adminLog.add(adminLogCooldownTrip, s.cfg.Upstreams[idx].Name, "",
			fmt.Sprintf("cooling for %s (consecutive failures=%d, ladder_step=%d, retry-after=%s)",
				time.Until(until).Round(time.Millisecond), fails, step, retryAfter), 0)
		log.Printf("any2api: upstream %s cooling for %s (consecutive failures=%d, window requests=%d failures=%d extend=%v, ladder_step=%d, retry-after=%s)",
			s.cfg.Upstreams[idx].Name, time.Until(until).Round(time.Millisecond), fails, winReq, winFail, extend, step, retryAfter)
	}
}

// observeFailureWindow 失败率窗口观测（调用方持锁）：窗口过期即重置；本次失败
// 计入窗口（请求 +1、失败 +1）；返回失败率是否触发冷却（LiteLLM 范式：请求数 ≥
// min 且失败率 > threshold；min 守卫保证小样本不触发——基态「>0.5 且本分钟
// ≥5 请求」的最小版）。
func (s *Server) observeFailureWindow(h *upstreamHealth, now time.Time) bool {
	enabled, minReq, threshold, window := s.cfg.failureRate()
	if !enabled {
		return false
	}
	if h.winStart.IsZero() || now.Sub(h.winStart) >= window {
		h.winStart = now
		h.winReq = 0
		h.winFail = 0
	}
	h.winReq++
	h.winFail++
	return h.winReq >= minReq && float64(h.winFail) > threshold*float64(h.winReq)
}

// observeSuccessWindow 成功观测（调用方持锁）：窗口计入一次请求、不计失败。
func (s *Server) observeSuccessWindow(h *upstreamHealth, now time.Time) {
	enabled, _, _, window := s.cfg.failureRate()
	if !enabled {
		return
	}
	if h.winStart.IsZero() || now.Sub(h.winStart) >= window {
		h.winStart = now
		h.winReq = 0
		h.winFail = 0
	}
	h.winReq++
}

// recordSuccess 上游成功：连续失败清零、冷却解除、冷却阶梯档位复位（W5-32 条目
// 2：「成功清零」——升级式惩罚必须能被成功打断，否则一次坏窗口把窗口永久抬到
// 顶档）。`fails` 必须同时清零：它是「连续失败」计数，成功即打断连续（否则成功
// 后的首次失败会拿旧计数越过阈值，把窗口直接抬到上一档）。失败率窗口只计请求
// ——「请求问题」类 4xx 也走本函数，不计上游失败。
func (s *Server) recordSuccess(idx int) {
	h := &s.health[idx]
	h.mu.Lock()
	h.fails = 0
	h.until = time.Time{}
	h.ladderStep = 0
	s.observeSuccessWindow(h, time.Now())
	h.mu.Unlock()
}

// ---------------------------------------------------------------------------
// 冷却阶梯升级（W5-32 条目 2；P65B 裁定 #2）
// ---------------------------------------------------------------------------
//
// 固定窗口（缺省 30s）对「偶发失败」够用，但对「上游持续劣化」太钝：每轮只冷却
// 30s，坏上游会以 30s 的节奏反复回到候选链里空转。Parrot `cooldown.py:1-8`
// 用通用阶梯 [1,3,5,10,15,0]min 逐级升级（0=永久）；sub2api
// `antigravity_internal500_penalty.go:14-18` 用平台专项 30min→2h→永久。本项取
// Parrot 的**通用阶梯**形态，但采用报告 §喂我们裁定 #2 建议的有限档
// （30s→2m→10m），不引入永久档——单进程薄壳服务没有人工解除通道，永久禁用会把
// 上游从链上摘死（全部冷却时虽退化全列表，但每轮都要先试一遍），保守优先。
//
// 语义：
//
//   - **缺省关**（`cooldown_ladder` 未配置 = 空）——行为与引入前逐字节一致
//     （固定 cooldown_seconds 窗口），符合「保守优先」；
//   - 配置后每次**进入冷却**（连续失败达阈值 / 失败率触发）取下一档；档位到顶后
//     停在顶档；
//   - 成功清零档位（recordSuccess）；
//   - 冷却中失败不推进档位（recordFailure 的防爆发闸，与「不延长」同一口径）；
//   - 显式 retry-after 不推进档位（上游给了明确时刻——那是最忠实的窗口，阶梯是
//     我们自己缺乏信号时的推断；Parrot 的 429 显式 reset 绕过阶梯同旨）；
//   - 档位窗口与固定窗口取较晚者（配置同时给了 cooldown_seconds 与阶梯时，窗口
//     = max(固定, 档位)）。

// advanceLadderStep 进入一次冷却时推进阶梯档位（调用方持锁）。返回是否真的推进
// （无阶梯配置 = false）。档位可超过最高档（继续计数——窗口由 cooldownWindowFor
// 封顶到最高档），使「进入冷却次数」语义与档位计数一致。
func (s *Server) advanceLadderStep(h *upstreamHealth) bool {
	n := len(s.cfg.cooldownLadder())
	if n == 0 {
		return false
	}
	if h.ladderStep < n {
		h.ladderStep++
	}
	return true
}

// cooldownWindowFor 本次冷却窗口：固定窗口（cooldownWindow）与阶梯当前档位窗口
// （配置时）取较长者（调用方持锁）。
func (s *Server) cooldownWindowFor(h *upstreamHealth) time.Duration {
	w := s.cooldownWindow()
	if ladder := s.cfg.cooldownLadder(); len(ladder) > 0 {
		step := h.ladderStep
		if step > len(ladder)-1 {
			step = len(ladder) - 1
		}
		if ladder[step] > w {
			w = ladder[step]
		}
	}
	return w
}

// ---------------------------------------------------------------------------
// 瞬时类失败：同上游短退避重试（W5-32 条目 1；P65B 裁定 #1）
// ---------------------------------------------------------------------------
//
// 我们此前对 overloaded/429/5xx/传输错误一律**直接换上游**（C1 语义），没有
// 「同一台上等一会儿再试」这一档；Parrot `failover.py:2683-2712` 与 sub2api
// `failover_loop.go:58-111` 都有（同渠道/同账号重试 ≤N 次 + 短退避）。本项是
// **超出 C1 的纯增益**（同 Azure `retry-after-ms`/资源级 403 两处扩展先例的登记
// 口径，见文件头）：不改 C1 客户端语义——中间重试**不计健康/不冷却，终态才记
// 一次**（Parrot 注释 :2683-2687 的语义），下游只在终态看到错误信封或成功流。
//
// 判据（瞬时类 = 重试有意义的形态）：
//
//   - 网络层错误：连接错误/超时（`net.Error` 超时、`net.OpError` 连接失败、
//     EOF 截断——上游没回完整响应，同台立刻再试是标准做法）；
//   - 上游**显式**说可重试：`x-should-retry: true`，或错误体含
//     `"type":"overloaded_error"`（C1 实测：overloaded 文本强制换号，本项在换号
//     前先给同台一次机会）；
//   - 状态码 429 / 503 / 529（rate_limit / server_error 类）——「忙/过载」形态；
//     `x-should-retry: false` 显式压住一切（C1 语义优先，不重试）。
//
// 退避：优先 `retry-after-ms`/`retry-after`（上游给的等待时间最忠实，上限
// `retrySameUpstreamCap`=60s）；否则按配置延迟序列 `retry_same_upstream_delays_ms`
// 逐次取（缺省 [750, 1750]，与 Parrot 的 0.75/1.75s 同源）；加 ±20% jitter
// （防多请求同时重试打上游——Parrot 同款）。每次重试前 sleep，随请求 ctx 取消
// （客户端断开立即停）。

// retrySameUpstreamCap 单次同上游重试等待上限（Parrot `≤60s` 口径；显式
// retry-after 超限截到此值——与冷却上限 maxRetryAfterCap 同一保守纪律）。
const retrySameUpstreamCap = 60 * time.Second

// retrySameUpstreamMax 同上游重试次数上限（配置值被截到此上限——防误配 N=100
// 把请求拖死在单台上）。
const retrySameUpstreamMax = 3

// transientFailure 本次失败是否「瞬时类」（同上游重试有意义）——仅用于同上游
// 重试决策，不改换上游分类（C1 表语义由 classifyFailure 独占）。
func transientFailure(err error, status int, header http.Header, errBody []byte) bool {
	if err != nil {
		return true // 网络层错误（连接失败/超时/截断）：同台再试是标准做法。
	}
	if bytes.Contains(errBody, []byte(`"type":"overloaded_error"`)) {
		return true // 上游自己说忙（匹配先于头检查——C1 P70B 例外同旨）。
	}
	switch strings.ToLower(strings.TrimSpace(header.Get("x-should-retry"))) {
	case "false":
		return false // 显式压住一切（C1 语义优先）。
	case "true":
		return true
	}
	// 状态码类（与换上游分类同表）：429/408/409/5xx 是瞬时/可等形态；其余 4xx
	// 是请求问题（同台重试不会变好）。
	switch remapClass(errBody, failureClass(status)) {
	case classRateLimit, classTimeout, classConflict, classServerError:
		return true
	}
	return false
}

// retrySameUpstreamDelay 本次同上游重试的等待时长（n = 本请求已用同上游重试
// 次数，0 起）。返回 0 = 不重试（非瞬时类）。
//
//   - 显式 `retry-after-ms`/`retry-after` → 精确执行（截到 retrySameUpstreamCap）；
//   - 网络层错误（无响应头）→ 配置延迟序列（越界取最后一档；缺省 [750,1750]ms）；
//   - 其余瞬时类（429/503/529 无头）→ 同上。
func (s *Server) retrySameUpstreamDelay(n int, header http.Header) time.Duration {
	if header != nil {
		if d := retryAfterSeconds(header); d > 0 {
			if d > retrySameUpstreamCap {
				d = retrySameUpstreamCap
			}
			return d
		}
	}
	delays := s.cfg.retryDelays()
	if len(delays) == 0 {
		return 0
	}
	if n >= len(delays) {
		n = len(delays) - 1
	}
	return jitter(delays[n])
}

// jitter 给退避加 ±20% 抖动（Parrot `failover.py:2683-2712` 同款；防多请求同时
// 重试打上游）。base<=0 → 0。
func jitter(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	f := 1 + (rand.Float64()*0.4 - 0.2) // [0.8, 1.2)
	return time.Duration(float64(base) * f)
}
