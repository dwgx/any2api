// admin_metrics.go —— dashboard P0-1 指标面：60 × 1 分钟环形桶 + 延迟直方图
// （board A2A-L19-W72-1；设计 `docs/admin-dashboard.md` §5.1「趋势（30 分钟 /
// 60s 桶）」· §6.1 #1 · §9 P0-1 行）。
//
// 端点（挂 adminAuth；未配 admin_key = 路由根本不注册 → 404 非 401）：
//
//	GET /admin/metrics?window=30m&step=60s
//	  window = 1..60 分钟（`30` / `30m`；缺省 30）；step 只允许 60 秒（`60` /
//	  `60s`）；其余（`90s`/`1h`/`0`/`61`/非数字）= 400。响应恒 `window/step`
//	  个桶（空桶 = 计数全 0、ts 仍在），旧 → 新。
//
// 数据源（**零新持久面**；设计 §2.2「进程内累计不能当日账」→ 本面是**时间序列**）：
//   - `adminMetricsRing`：进程内定长 60 桶（1 分钟/桶），槽位 = 分钟序号 % 60——
//     天然回绕（满则覆盖最旧），**绝不逐请求增长**（定长数组，无 slice append）；
//   - 写入点 = 请求收尾（`attempts.go` 的 `recordRequestRing`，与 P0-0 请求 ring
//     同一处）：每个请求观测一次 requests / errors / failovers + TTFT/总量直方图。
//
// 纪律：
//   - **纯观测、纯只读**：零出网、零写盘、零 store 依赖、不改既有端点形状；
//   - **绝不发明数字**：零/未知值（TTFT ≤ 0、总量 ≤ 0）不进直方图；空桶回 0 而不
//     是 null；空直方图的分位数 = 0（= 无观测，不是「0 毫秒」）；
//   - **重启清零 / 单实例**（设计 §7.5 登记的多实例未来项）：进程内内存态，重启后
//     从零开始——**不是**日账（日账看 `/admin/usage`，store 面）；
//   - **有界**：写入 O(1)（定长槽位），读面快照 O(60)（定长数组复制）。
//
// 计数口径（与既有面的对账关系）：
//   - requests++：每个**走到 failover 链**的请求一次（= `serve` 收尾一次 = 请求
//     ring 一行）。**不含**入口解析失败（405 / 体解析 400）与闸外拒绝（429
//     capacity）——那些 `Diagnostics.Requests` 有、本面没有（差分口径，见报告）。
//   - errors++：末次尝试 `failed`（上游侧失败）**或**未交付且**非下游断开/取消**
//     （`delivered=false && !disconnected`：错误信封 / 截断）；未发起任何尝试
//     （本地失败 / 无候选上游）也记一次（没有产出交付下游）。**下游主动放弃不是
//     网关错误**：客户端取消 / 断连（`disconnected=true`，attempt 行 `failed=false`）
//     不计 error——网关没失败，是下游不收了（W88-1；与 `finishAttempt` 的
//     「下游断开不置 failed」同纪律）。**任务面例外**：任务交付点在 `task.go
//     markDelivered`，attempt 行恒 `delivered=false`——逐个记 error 会把每个后台
//     任务算成失败，故任务面（`rec.task != ""`）只按 `failed` 计错（登记差异）。
//   - failovers：相邻尝试**换台**次数（上游名变化；同台重试不计）——与
//     `Diagnostics.Failovers` / 请求 ring 行的 `failovers` 列**同一口径**（缺省
//     `retry_same_upstream=0` 时恒等于 `max(0, len(attempts)-1)`）。
//   - TTFT = **首个** `ttft > 0` 的尝试（毫秒）；total = **末次**尝试的 total
//     （`end - start`）——两者的「跳过零值」见上。
//
// 直方图口径（`le` 上界；边界逐值见 `adminMetricsEdgesMs`，JSON 面见
// `adminMetricsEdges.MarshalJSON`）：
//   - `hist[i]` = 观测值 v ∈ `(edges[i-1], edges[i]]` 的条数；`i = 0` 段 = v ≤ 0
//     （恒 0——零/未知值不入图）；最后一段 = (60000, +Inf)（JSON 里 edges 的第 15
//     项是字符串 `"+Inf"`：JSON 没有无穷字面量）；
//   - 分位数 = 最近秩（nearest-rank）命中段的**上界**（`le` 语义）；命中最后一段
//     时回 60000（该段下界——「已知 > 60000」的保守值）。这是直方图近似，不是
//     精确分位（登记，见报告「残余风险」）。
package service

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// adminMetricsBucketCount 环形桶数（设计 §6.1 #1「1 分钟桶 × 60 的环形缓冲」）。
	adminMetricsBucketCount = 60
	// adminMetricsBucketMs 桶宽（毫秒；1 分钟）。
	adminMetricsBucketMs = 60_000
	// adminMetricsDefaultWindowMinutes 缺省 window（设计 §5.1「趋势（30 分钟 /
	// 60s 桶）」）。
	adminMetricsDefaultWindowMinutes = 30
	// adminMetricsMaxWindowMinutes 最大 window（= 桶数；step 恒 60s → 桶数 ≤ 60）。
	adminMetricsMaxWindowMinutes = adminMetricsBucketCount
	// adminMetricsStepSeconds step 唯一允许值（秒）。
	adminMetricsStepSeconds = 60
	// adminMetricsEdgeCount 有限边界个数（= 段数 - 1）。
	adminMetricsEdgeCount = 14
	// adminMetricsHistSlots 直方图段数 = 有限边界 + 最后一段 (60000, +Inf)。
	adminMetricsHistSlots = adminMetricsEdgeCount + 1
)

// adminMetricsEdgesMs 直方图边界（毫秒；`le` 上界，升序，固定——绝不逐请求算）。
var adminMetricsEdgesMs = [adminMetricsEdgeCount]int64{
	0, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000,
}

// adminMetricsEdges JSON 面的边界表（14 个有限边界 + `"+Inf"` 字符串收尾）。
type adminMetricsEdges [adminMetricsEdgeCount]int64

// adminMetricsEdgeView 响应里回出的边界表（包级单例：边界是常量，每次分配无意义）。
var adminMetricsEdgeView = adminMetricsEdges(adminMetricsEdgesMs)

// MarshalJSON 边界表 → JSON 数组：14 个数字 + 字符串 `"+Inf"`（JSON 无无穷字面量，
// 用字符串显式标注最后一段的开区间上界——与 `ttft_hist`/`total_hist` 的第 15 槽
// 一一对应）。
func (e adminMetricsEdges) MarshalJSON() ([]byte, error) {
	out := make([]any, 0, len(e)+1)
	for _, v := range e {
		out = append(out, v)
	}
	out = append(out, "+Inf")
	return json.Marshal(out)
}

// adminMetricsSlot 观测值（毫秒）命中的段号：第一个 `le` 上界 ≥ v 的段；超过全部
// 有限边界 = 最后一段 (60000, +Inf)。≤ 0 由调用方跳过（本函数 0 → 段 0）。
func adminMetricsSlot(ms int64) int {
	for i, e := range adminMetricsEdgesMs {
		if ms <= e {
			return i
		}
	}
	return adminMetricsHistSlots - 1
}

// adminMetricsHistogram 一个延迟直方图（固定 15 段；零值可用）。
type adminMetricsHistogram struct {
	counts [adminMetricsHistSlots]uint64
}

// observe 记一次观测（**零/未知值跳过**：ms ≤ 0 不进图，绝不发明 0 毫秒的样本）。
func (h *adminMetricsHistogram) observe(ms int64) {
	if ms <= 0 {
		return
	}
	h.counts[adminMetricsSlot(ms)]++
}

// total 观测条数。
func (h adminMetricsHistogram) total() uint64 {
	var n uint64
	for _, c := range h.counts {
		n += c
	}
	return n
}

// quantile 最近秩分位（pct = 50 / 95）：返回命中段的 `le` 上界；命中最后一段
// （> 60000）返回 60000（下界，保守值）；空直方图 = 0（无观测）。
func (h adminMetricsHistogram) quantile(pct int64) int64 {
	n := h.total()
	if n == 0 {
		return 0
	}
	rank := (uint64(pct)*n + 99) / 100 // ceil(pct * n / 100)
	var cum uint64
	for i, c := range h.counts {
		cum += c
		if cum >= rank {
			if i < adminMetricsEdgeCount {
				return adminMetricsEdgesMs[i]
			}
			return adminMetricsEdgesMs[adminMetricsEdgeCount-1]
		}
	}
	return 0 // 不可达（cum 最终 == n ≥ rank）
}

// adminMetricsObservation 一次请求收尾的观测值（由 `requestRecorder` 投影；
// 零值 = 只有 requests++）。
type adminMetricsObservation struct {
	// Error 结局失败（末次尝试 failed / 未交付；口径见文件头）。
	Error bool
	// Failovers 本次请求的换台次数（相邻尝试上游名变化次数）。
	Failovers uint64
	// TTFTMs 首字节（毫秒；≤ 0 = 无观测 → 不进直方图）。
	TTFTMs int64
	// TotalMs 末次尝试耗时（毫秒；≤ 0 = 无观测 → 不进直方图）。
	TotalMs int64
}

// adminMetricsBucket 一个 1 分钟桶（进程内；start = 桶起点 unix 毫秒，分钟对齐）。
type adminMetricsBucket struct {
	start     int64
	requests  uint64
	errors    uint64
	failovers uint64
	ttft      adminMetricsHistogram
	total     adminMetricsHistogram
}

// adminMetricsRing 定长环形桶（覆盖写最旧；零值可用；不落盘）。`Server.adminMetrics`
// 零值即空 ring——首次写入直接把槽位置为当前分钟（无惰性分配）。
type adminMetricsRing struct {
	mu    sync.Mutex
	slots [adminMetricsBucketCount]adminMetricsBucket
}

// adminMetricsBucketStart 时刻 → 桶起点（unix 毫秒，分钟对齐）。
func adminMetricsBucketStart(at time.Time) int64 {
	ms := at.UnixMilli()
	return ms - ms%adminMetricsBucketMs
}

// adminMetricsSlotOf 桶起点 → 槽号（分钟序号 % 60 = 天然回绕；负时间戳兜底取模）。
func adminMetricsSlotOf(start int64) int {
	idx := int((start / adminMetricsBucketMs) % adminMetricsBucketCount)
	if idx < 0 {
		idx += adminMetricsBucketCount
	}
	return idx
}

// observe 记一次请求收尾。槽位内的时间戳与当前分钟不符 = 该槽是上一轮的陈旧数据 →
// 整槽清零后写入（这就是「60 桶回绕」的全部机制：**没有**遍历/删除，O(1)）。
func (r *adminMetricsRing) observe(at time.Time, o adminMetricsObservation) {
	start := adminMetricsBucketStart(at)
	r.mu.Lock()
	defer r.mu.Unlock()
	b := &r.slots[adminMetricsSlotOf(start)]
	if b.start != start {
		*b = adminMetricsBucket{start: start}
	}
	b.requests++
	if o.Error {
		b.errors++
	}
	b.failovers += o.Failovers
	b.ttft.observe(o.TTFTMs)
	b.total.observe(o.TotalMs)
}

// buckets 最近 count 个分钟桶（旧 → 新，恒 count 个；空桶 = 计数全 0 + ts 仍在）。
// 一次持锁复制定长数组（快照语义：读面绝不阻塞写面以外的任何路径）。
func (r *adminMetricsRing) buckets(now time.Time, count int) []adminMetricsBucketView {
	if count <= 0 {
		return make([]adminMetricsBucketView, 0)
	}
	if count > adminMetricsBucketCount {
		count = adminMetricsBucketCount
	}
	newest := adminMetricsBucketStart(now)
	r.mu.Lock()
	slots := r.slots
	r.mu.Unlock()
	out := make([]adminMetricsBucketView, 0, count)
	for i := count - 1; i >= 0; i-- {
		ts := newest - int64(i)*adminMetricsBucketMs
		v := adminMetricsBucketView{
			TS:    ts,
			TTFT:  adminMetricsPercentiles{},
			Total: adminMetricsPercentiles{},
		}
		b := slots[adminMetricsSlotOf(ts)]
		if b.start == ts {
			v.Requests, v.Errors, v.Failovers = b.requests, b.errors, b.failovers
			v.TTFT = adminMetricsPercentiles{P50: b.ttft.quantile(50), P95: b.ttft.quantile(95)}
			v.Total = adminMetricsPercentiles{P50: b.total.quantile(50), P95: b.total.quantile(95)}
			v.TTFTHist, v.TotalHist = b.ttft.counts, b.total.counts
		}
		out = append(out, v)
	}
	return out
}

// adminMetricsPercentiles 一个延迟面的分位（毫秒；0 = 无观测）。
type adminMetricsPercentiles struct {
	P50 int64 `json:"p50"`
	P95 int64 `json:"p95"`
}

// adminMetricsBucketView 响应里的一个桶（空桶 = 计数全 0、直方图全 0、ts 仍在；
// 两个直方图恒 15 槽定长数组 → **绝不 null**）。
type adminMetricsBucketView struct {
	TS        int64                         `json:"ts"` // 桶起点 unix 毫秒（分钟对齐）
	Requests  uint64                        `json:"requests"`
	Errors    uint64                        `json:"errors"`
	Failovers uint64                        `json:"failovers"`
	TTFT      adminMetricsPercentiles       `json:"ttft_ms"`
	Total     adminMetricsPercentiles       `json:"total_ms"`
	TTFTHist  [adminMetricsHistSlots]uint64 `json:"ttft_hist"`
	TotalHist [adminMetricsHistSlots]uint64 `json:"total_hist"`
}

// adminMetricsResponse `GET /admin/metrics` 响应体（恒 `window_seconds /
// step_seconds` 个桶，旧 → 新；无 null）。
type adminMetricsResponse struct {
	WindowSeconds int                      `json:"window_seconds"`
	StepSeconds   int                      `json:"step_seconds"`
	Now           int64                    `json:"now"` // unix 毫秒（与 ts 同轴）
	EdgesMs       adminMetricsEdges        `json:"edges_ms"`
	Buckets       []adminMetricsBucketView `json:"buckets"`
}

// adminMetricsSnapshot 指标快照（minutes = 桶数 = window 分钟数；1..60）。
func (s *Server) adminMetricsSnapshot(now time.Time, minutes int) adminMetricsResponse {
	return adminMetricsResponse{
		WindowSeconds: minutes * adminMetricsStepSeconds,
		StepSeconds:   adminMetricsStepSeconds,
		Now:           now.UnixMilli(),
		EdgesMs:       adminMetricsEdgeView,
		Buckets:       s.adminMetrics.buckets(now, minutes),
	}
}

// ---------------------------------------------------------------------------
// 请求收尾投影（hook 的纯函数面）
// ---------------------------------------------------------------------------

// adminMetricsObservationFrom 把请求收尾的记账器投影为一次观测（口径见文件头；
// nil 安全）。**绝不发明**：无尝试 → 无 TTFT/总量；TTFT 取首个 > 0 的尝试；total
// 只算末次尝试（`end - start`，不含换台/排队时间——与 P1-3 turn 落账同源）。
func adminMetricsObservationFrom(rec *requestRecorder) adminMetricsObservation {
	var obs adminMetricsObservation
	if rec == nil {
		return obs
	}
	n := len(rec.recs)
	if n == 0 {
		// 未发起任何尝试（本地失败 / 无候选上游）：没有产出交付下游 = 一次 error。
		obs.Error = true
		return obs
	}
	for _, ar := range rec.recs {
		if ar.ttft > 0 {
			obs.TTFTMs = ar.ttft.Milliseconds()
			break
		}
	}
	last := rec.recs[n-1]
	// 任务面：delivered 口径在 task.go markDelivered（attempt 行恒 false）——只按
	// failed 计错，别把每个后台任务记成失败（文件头「登记差异」）。
	// 下游断开/取消（disconnected，attempt 行 failed=false）= 客户端主动放弃，
	// 不是网关错误：未交付也不计 error（W88-1；上游失败与真未交付仍计）。
	obs.Error = last.failed || (!last.delivered && rec.task == "" && !last.disconnected)
	if !last.start.IsZero() && !last.end.IsZero() {
		obs.TotalMs = last.end.Sub(last.start).Milliseconds()
	}
	for i := 1; i < n; i++ {
		if rec.recs[i].upstream != rec.recs[i-1].upstream {
			obs.Failovers++
		}
	}
	return obs
}

// observeRequest 请求收尾观测（`recordRequestRing` 的写入点调用；nil 安全）。
func (s *Server) observeRequest(rec *requestRecorder, now time.Time) {
	if s == nil || rec == nil {
		return
	}
	s.adminMetrics.observe(now, adminMetricsObservationFrom(rec))
}

// ---------------------------------------------------------------------------
// 参数解析
// ---------------------------------------------------------------------------

// parseAdminMetricsWindow 解析 `window`：空 = 缺省 30 分钟；`30`（裸整数）或
// `30m`（分钟后缀）两种形态；范围 1..60。其余（`90s` / `1h` / `30m30s` / 小数 /
// 非数字 / 越界）= false（400）。
func parseAdminMetricsWindow(raw string) (int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return adminMetricsDefaultWindowMinutes, true
	}
	num := raw
	if strings.HasSuffix(num, "m") {
		num = strings.TrimSuffix(num, "m")
	} else if strings.ContainsAny(num, "smhdSMHD") {
		return 0, false // 明确带其它单位后缀（`90s`/`1h`）→ 400，不猜
	}
	n, err := strconv.Atoi(num)
	if err != nil || n < 1 || n > adminMetricsMaxWindowMinutes {
		return 0, false
	}
	return n, true
}

// parseAdminMetricsStep 解析 `step`：空 = 60；`60`（裸整数）或 `60s`（秒后缀）；
// **只有** 60 合法（其余 = false → 400）。
func parseAdminMetricsStep(raw string) (int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return adminMetricsStepSeconds, true
	}
	num := raw
	if strings.HasSuffix(num, "s") {
		num = strings.TrimSuffix(num, "s")
	} else if strings.ContainsAny(num, "smhdSMHD") {
		return 0, false
	}
	n, err := strconv.Atoi(num)
	if err != nil || n != adminMetricsStepSeconds {
		return 0, false
	}
	return n, true
}

// handleAdminMetrics `GET /admin/metrics?window=&step=`（形状见文件头）。
// 校验：window 1..60 分钟、step 恒 60 秒；坏值 = 400（`invalid_request_error`）。
func (s *Server) handleAdminMetrics(w http.ResponseWriter, r *http.Request) {
	minutes, ok := parseAdminMetricsWindow(r.URL.Query().Get("window"))
	if !ok {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error",
			"window must be 1..60 minutes (`30` or `30m`)")
		return
	}
	if _, ok := parseAdminMetricsStep(r.URL.Query().Get("step")); !ok {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error",
			"step must be 60 seconds (`60` or `60s`)")
		return
	}
	writeAdminJSON(w, http.StatusOK, s.adminMetricsSnapshot(time.Now(), minutes))
}
