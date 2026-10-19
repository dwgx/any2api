// admin_metrics_test.go —— dashboard P0-1 指标面测试（board A2A-L19-W72-1；设计
// `docs/admin-dashboard.md` §5.1/§6.1 #1/§9 P0-1）。
//
// 装置：① 纯 ring 单测（可控时钟——回绕/并发/直方图，绝不等真实分钟翻页）；
// ② 真 Server + httptest 真上游 + 真 HTTP 客户端（契约面与 Diagnostics 交叉对拍）。
//
// 反证锚点（回退即红）：
//   - 去掉请求收尾的观测 hook（attempts.go `recordRequestRing` 里的
//     `s.observeRequest`）→ TestAdminMetricsCrossCheckDiagnostics 红（指标面恒 0，
//     Diagnostics 有数）——负控见报告 §5；
//   - 桶不再「写前比对分钟戳」（只在首次分配）→ TestAdminMetricsBucketWraparound 红
//     （同槽位累加、窗口内 requests = 61 而不是 60）；
//   - 直方图零值不跳过 / 分位口径改精确值 → TestAdminMetricsLatencyHistogram 红；
//   - 空桶回 null / 桶数不恒 window-step → TestAdminMetricsEmptyState 红；
//   - 参数校验放宽（step=59 放行 / window=61 放行）→ TestAdminMetricsValidation 红；
//   - 路由改挂错门（未配 key 仍注册 / 不挂 adminAuth）→ TestAdminMetricsAuth 红。
package service

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// 装置
// ---------------------------------------------------------------------------

// adminMetricsKey 测试管理密钥（与生产同形态；不进任何日志/报告）。
const adminMetricsKey = "metrics-test-key"

// adminMetricsWire* 契约面解码结构（json 键 = 响应形状；**不依赖实现侧结构**——
// 形状改了这里就红）。
type adminMetricsWirePercentiles struct {
	P50 int64 `json:"p50"`
	P95 int64 `json:"p95"`
}

type adminMetricsWireBucket struct {
	TS        int64                       `json:"ts"`
	Requests  uint64                      `json:"requests"`
	Errors    uint64                      `json:"errors"`
	Failovers uint64                      `json:"failovers"`
	TTFT      adminMetricsWirePercentiles `json:"ttft_ms"`
	Total     adminMetricsWirePercentiles `json:"total_ms"`
	TTFTHist  []uint64                    `json:"ttft_hist"`
	TotalHist []uint64                    `json:"total_hist"`
}

type adminMetricsWireResponse struct {
	WindowSeconds int                      `json:"window_seconds"`
	StepSeconds   int                      `json:"step_seconds"`
	Now           int64                    `json:"now"`
	EdgesMs       []any                    `json:"edges_ms"`
	Buckets       []adminMetricsWireBucket `json:"buckets"`
}

// adminMetricsStart 启动被测服务：cfg 校验 → NewServer → 真 Handler（**路由注册
// 面本体**——含 admin_key 自门控与 adminAuth）。
func adminMetricsStart(t *testing.T, cfg *Config) (*Server, *httptest.Server) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(func() {
		srv.Close()
		_ = s.CloseStore()
	})
	return s, srv
}

// adminMetricsGet 打一次 GET /admin/metrics（key 空 = 不带任何鉴权头）。
func adminMetricsGet(t *testing.T, base, query, key string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/admin/metrics"+query, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if key != "" {
		req.Header.Set("X-Management-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return resp.StatusCode, raw
}

// adminMetricsBody 打一次 GET 并断言 200（返回原文 + 解码）。
func adminMetricsBody(t *testing.T, srv *httptest.Server, query string) adminMetricsWireResponse {
	t.Helper()
	code, raw := adminMetricsGet(t, srv.URL, query, adminMetricsKey)
	if code != http.StatusOK {
		t.Fatalf("GET /admin/metrics%s = %d（want 200）；body=%s", query, code, raw)
	}
	var got adminMetricsWireResponse
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode: %v；body=%s", err, raw)
	}
	return got
}

// adminMetricsSums 窗口内各计数合计（requests/errors/failovers/两直方图条数）。
func adminMetricsSums(bs []adminMetricsWireBucket) (requests, errors, failovers, ttftHist, totalHist uint64) {
	for _, b := range bs {
		requests += b.Requests
		errors += b.Errors
		failovers += b.Failovers
		for _, c := range b.TTFTHist {
			ttftHist += c
		}
		for _, c := range b.TotalHist {
			totalHist += c
		}
	}
	return
}

// adminMetricsViewSums 窗口内各计数合计（ring 视图面；与 adminMetricsSums 同口径）。
func adminMetricsViewSums(bs []adminMetricsBucketView) (requests, errors, failovers, ttftHist, totalHist uint64) {
	for _, b := range bs {
		requests += b.Requests
		errors += b.Errors
		failovers += b.Failovers
		for _, c := range b.TTFTHist {
			ttftHist += c
		}
		for _, c := range b.TotalHist {
			totalHist += c
		}
	}
	return
}

// adminMetricsWaitFor 等指标面收到 want 次请求收尾（`serve` 的 defer 在下游读到
// EOF 之后仍可能差一步——有界等待，绝不无限等、绝不 sleep 空转超过 2s）。
func adminMetricsWaitFor(t *testing.T, s *Server, want uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := s.adminMetrics.buckets(time.Now(), 1)[0].Requests
		if got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("指标面 requests = %d（want ≥ %d；收尾 hook 未落到指标面）", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// slowUpstream 假上游：先 sleep 再回包（保证 ttft/total 的毫秒观测非零——直方图
// 断言的确定性来源，绝不靠「本机够快」）。
func slowUpstream(t *testing.T, delay time.Duration, status int, body string) *countUpstream {
	t.Helper()
	return newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
}

// ---------------------------------------------------------------------------
// 契约面：空态（零、非 null、桶数恒 window/step）
// ---------------------------------------------------------------------------

func TestAdminMetricsEmptyState(t *testing.T) {
	cfg := uiTestConfig()
	cfg.AdminKey = adminMetricsKey
	_, srv := adminMetricsStart(t, cfg)

	before := time.Now()
	code, raw := adminMetricsGet(t, srv.URL, "?window=30&step=60", adminMetricsKey)
	if code != http.StatusOK {
		t.Fatalf("status = %d（want 200）；body=%s", code, raw)
	}
	if strings.Contains(string(raw), "null") {
		t.Fatalf("空态响应出现 null（必须回 0 / [] / 定长直方图）；body=%s", raw)
	}
	got := adminMetricsBody(t, srv, "?window=30&step=60")
	if got.WindowSeconds != 1800 || got.StepSeconds != 60 {
		t.Fatalf("window_seconds/step_seconds = %d/%d（want 1800/60）", got.WindowSeconds, got.StepSeconds)
	}
	if got.Now < before.UnixMilli() || got.Now > time.Now().UnixMilli()+1000 {
		t.Fatalf("now = %d（不在本次请求时窗内）", got.Now)
	}
	// edges：15 项，前 14 项 = 固定边界，第 15 项 = 字符串 "+Inf"（JSON 无无穷字面量）。
	if len(got.EdgesMs) != adminMetricsHistSlots {
		t.Fatalf("edges_ms 长度 = %d（want %d）", len(got.EdgesMs), adminMetricsHistSlots)
	}
	wantEdges := []float64{0, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000}
	for i, want := range wantEdges {
		if v, ok := got.EdgesMs[i].(float64); !ok || v != want {
			t.Fatalf("edges_ms[%d] = %v（want %v）", i, got.EdgesMs[i], want)
		}
	}
	if v, ok := got.EdgesMs[adminMetricsEdgeCount].(string); !ok || v != "+Inf" {
		t.Fatalf("edges_ms[%d] = %v（want 字符串 \"+Inf\"）", adminMetricsEdgeCount, got.EdgesMs[adminMetricsEdgeCount])
	}
	if len(got.Buckets) != 30 {
		t.Fatalf("桶数 = %d（want 30 = window/step）", len(got.Buckets))
	}
	for i, b := range got.Buckets {
		if b.Requests != 0 || b.Errors != 0 || b.Failovers != 0 {
			t.Fatalf("空态桶[%d] 非零：%+v", i, b)
		}
		if len(b.TTFTHist) != adminMetricsHistSlots || len(b.TotalHist) != adminMetricsHistSlots {
			t.Fatalf("桶[%d] 直方图槽数 = %d/%d（want %d）", i, len(b.TTFTHist), len(b.TotalHist), adminMetricsHistSlots)
		}
		if b.TTFT.P50 != 0 || b.TTFT.P95 != 0 || b.Total.P50 != 0 || b.Total.P95 != 0 {
			t.Fatalf("空态桶[%d] 分位非零（0 = 无观测）：%+v", i, b)
		}
		if b.TS%adminMetricsBucketMs != 0 {
			t.Fatalf("桶[%d] ts = %d（未对齐分钟）", i, b.TS)
		}
		if i > 0 && b.TS-got.Buckets[i-1].TS != adminMetricsBucketMs {
			t.Fatalf("桶[%d] ts 步长 = %d（want %d）", i, b.TS-got.Buckets[i-1].TS, adminMetricsBucketMs)
		}
	}
	if want := time.Now().UnixMilli() - time.Now().UnixMilli()%adminMetricsBucketMs; got.Buckets[29].TS != want {
		t.Fatalf("末桶 ts = %d（want 当前分钟 %d）", got.Buckets[29].TS, want)
	}
	// 缺省参数与 window=60 的桶数（= window/step，恒精确）。
	if def := adminMetricsBody(t, srv, ""); def.WindowSeconds != 1800 || len(def.Buckets) != 30 {
		t.Fatalf("缺省 = %d 秒/%d 桶（want 1800/30）", def.WindowSeconds, len(def.Buckets))
	}
	if max := adminMetricsBody(t, srv, "?window=60&step=60"); max.WindowSeconds != 3600 || len(max.Buckets) != 60 {
		t.Fatalf("window=60 = %d 秒/%d 桶（want 3600/60）", max.WindowSeconds, len(max.Buckets))
	}
	if one := adminMetricsBody(t, srv, "?window=1"); one.WindowSeconds != 60 || len(one.Buckets) != 1 {
		t.Fatalf("window=1 = %d 秒/%d 桶（want 60/1）", one.WindowSeconds, len(one.Buckets))
	}
	t.Logf("空态证据：window=30/step=60 → %d 桶（window=60 → %d；window=1 → %d；缺省 → %d）；"+
		"edges_ms=%v；空桶计数全 0、直方图各 %d 槽、分位 0；无 null：%v",
		30, 60, 1, 30, got.EdgesMs, adminMetricsHistSlots, !strings.Contains(string(raw), "null"))
}

// ---------------------------------------------------------------------------
// 回绕：61+ 分钟观测 → 恒 60 桶、最旧被覆盖（不累加、不增长）
// ---------------------------------------------------------------------------

func TestAdminMetricsBucketWraparound(t *testing.T) {
	var ring adminMetricsRing
	base := time.Unix(1_800_000_000, 0).UTC() // 秒对齐 → 分钟对齐
	// 61 分钟各 1 次观测：第 61 次与第 1 次落**同一槽位**（分钟序号 % 60 相同）。
	const minutes = 61
	for i := range minutes {
		ring.observe(base.Add(time.Duration(i)*time.Minute), adminMetricsObservation{TTFTMs: 7, TotalMs: 11})
	}
	now := base.Add(time.Duration(minutes-1) * time.Minute)
	got := ring.buckets(now, adminMetricsBucketCount)
	if len(got) != adminMetricsBucketCount {
		t.Fatalf("桶数 = %d（want %d）", len(got), adminMetricsBucketCount)
	}
	sum := uint64(0)
	for _, b := range got {
		sum += b.Requests
	}
	// 61 次观测 → 窗口内只有 60（第 1 次被第 61 次覆盖：槽位复用，不累加）。
	if sum != 60 {
		t.Fatalf("窗口内 requests 合计 = %d（want 60 = 61 次观测 - 1 次被覆盖）", sum)
	}
	if got[59].Requests != 1 {
		t.Fatalf("最新桶 requests = %d（want 1：覆盖而非累加——若是 2 说明写前没比对分钟戳）", got[59].Requests)
	}
	slot := adminMetricsSlot(7)
	if got[59].TTFTHist[slot] != 1 || got[59].TotalHist[adminMetricsSlot(11)] != 1 {
		t.Fatalf("最新桶直方图 = %v/%v（want 各 1 条：覆盖而非累加）", got[59].TTFTHist, got[59].TotalHist)
	}
	if got[0].TS != base.Add(time.Minute).UnixMilli() {
		t.Fatalf("最旧桶 ts = %d（want %d = base+1min）", got[0].TS, base.Add(time.Minute).UnixMilli())
	}
	// 再绕两圈（130 分钟 × 5 次/分钟）：窗口内恒 60 分钟 × 5 = 300。
	var ring2 adminMetricsRing
	for i := range 130 {
		for range 5 {
			ring2.observe(base.Add(time.Duration(i)*time.Minute), adminMetricsObservation{TTFTMs: 3, TotalMs: 9, Error: true})
		}
	}
	got2 := ring2.buckets(base.Add(129*time.Minute), 60)
	if len(got2) != 60 {
		t.Fatalf("绕两圈后桶数 = %d（want 60）", len(got2))
	}
	sum2, errs2, _, ttft2, total2 := adminMetricsViewSums(got2)
	if sum2 != 300 || errs2 != 300 || ttft2 != 300 || total2 != 300 {
		t.Fatalf("绕两圈后窗口合计 = requests %d / errors %d / ttft %d / total %d（want 全 300）", sum2, errs2, ttft2, total2)
	}
	t.Logf("回绕证据：61 次观测 → 窗口 %d 桶 / requests 合计 %d（最旧被覆盖）+ 最新桶 requests %d；"+
		"130 分钟 × 5 次 → %d 桶 / requests %d；ts 范围 [%d, %d]（步长 %d）",
		len(got), sum, got[59].Requests, len(got2), sum2, got2[0].TS, got2[len(got2)-1].TS,
		got2[len(got2)-1].TS-got2[len(got2)-2].TS)
}

// ---------------------------------------------------------------------------
// 并发：100 goroutine 同写 + 并发读（配 -race 跑）
// ---------------------------------------------------------------------------

func TestAdminMetricsConcurrency(t *testing.T) {
	var ring adminMetricsRing
	at := time.Unix(1_800_000_000, 0).UTC()
	const workers, per = 100, 40
	var wg sync.WaitGroup
	stop := make(chan struct{})
	// 并发读面（快照）：与写面同时跑；-race 下暴露任何未同步字段。
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = ring.buckets(at, 60)
			}
		}
	}()
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range per {
				obs := adminMetricsObservation{TTFTMs: int64(1 + i%100), TotalMs: int64(1 + i%1000)}
				if (w+i)%2 == 0 {
					obs.Error = true
				}
				if i%5 == 0 {
					obs.Failovers = 1
				}
				ring.observe(at, obs)
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	readerWG.Wait()

	got := ring.buckets(at, 1)
	requests, errors, failovers, ttftHist, totalHist := adminMetricsViewSums(got)
	wantRequests := uint64(workers * per)
	wantErrors := uint64(workers * per / 2)
	wantFailovers := uint64(workers * (per / 5))
	if requests != wantRequests || errors != wantErrors || failovers != wantFailovers {
		t.Fatalf("并发写后 = requests %d / errors %d / failovers %d（want %d / %d / %d）",
			requests, errors, failovers, wantRequests, wantErrors, wantFailovers)
	}
	if ttftHist != wantRequests || totalHist != wantRequests {
		t.Fatalf("直方图条数 = ttft %d / total %d（want 各 %d）", ttftHist, totalHist, wantRequests)
	}
}

// ---------------------------------------------------------------------------
// 直方图：固定边界 / 零值跳过 / 最近秩分位
// ---------------------------------------------------------------------------

func TestAdminMetricsLatencyHistogram(t *testing.T) {
	// 段号边界（le 语义：第一个 ≥ v 的上界）。
	for _, tc := range []struct {
		ms   int64
		want int
	}{
		{0, 0}, {1, 1}, {5, 1}, {6, 2}, {10, 2}, {25, 3}, {26, 4}, {60000, 13}, {60001, 14}, {1 << 40, 14},
	} {
		if got := adminMetricsSlot(tc.ms); got != tc.want {
			t.Fatalf("adminMetricsSlot(%d) = %d（want %d）", tc.ms, got, tc.want)
		}
	}
	var ring adminMetricsRing
	at := time.Unix(1_800_000_000, 0).UTC()
	// TTFT 六个已知值；总量无观测（零值 → 不进图，绝不发明样本）。
	for _, v := range []int64{3, 7, 40, 300, 12000, 70000} {
		ring.observe(at, adminMetricsObservation{TTFTMs: v})
	}
	// 负值/零 = 未知 → 跳过（不落任何槽）。
	ring.observe(at, adminMetricsObservation{TTFTMs: 0, TotalMs: -1})
	got := ring.buckets(at, 1)[0]
	if got.Requests != 7 || got.TTFTHistTotal() != 6 {
		t.Fatalf("requests/直方图条数 = %d/%d（want 7/6：零值样本被跳过）", got.Requests, got.TTFTHistTotal())
	}
	wantSlots := map[int]uint64{1: 1, 2: 1, 4: 1, 7: 1, 12: 1, 14: 1}
	for i, c := range got.TTFTHist {
		if wantSlots[i] != c {
			t.Fatalf("ttft_hist[%d] = %d（want %d）；full=%v", i, c, wantSlots[i], got.TTFTHist)
		}
	}
	for _, c := range got.TotalHist {
		if c != 0 {
			t.Fatalf("总量直方图非零（无观测必须全 0）：%v", got.TotalHist)
		}
	}
	// 分位：nearest-rank 命中段的上界；最后一段（>60000）报下界 60000。
	if got.TTFT.P50 != 50 || got.TTFT.P95 != 60000 {
		t.Fatalf("ttft p50/p95 = %d/%d（want 50/60000）", got.TTFT.P50, got.TTFT.P95)
	}
	if got.Total.P50 != 0 || got.Total.P95 != 0 {
		t.Fatalf("空直方图分位 = %d/%d（want 0/0 = 无观测）", got.Total.P50, got.Total.P95)
	}
}

// TTFTHistTotal 桶内 TTFT 观测条数（测试辅助——契约层没有 sum 字段）。
func (b adminMetricsBucketView) TTFTHistTotal() uint64 {
	var n uint64
	for _, c := range b.TTFTHist {
		n += c
	}
	return n
}

// ---------------------------------------------------------------------------
// 收尾投影（hook 的纯函数面：TTFT 取首个 > 0 / total 取末次尝试 / 换台计数）
// ---------------------------------------------------------------------------

func TestAdminMetricsObservationDerivation(t *testing.T) {
	at := time.Unix(1_800_000_000, 0).UTC()
	// 尝试 1 失败（无 ttft）→ 尝试 2 成功（ttft 12ms、total 250ms、已交付）。
	rec := newRequestRecorder("req-metrics-1", registry.FormatOpenAIChat, "gpt-x", true, "")
	a1 := rec.begin(1, Upstream{Name: "a", Protocol: ProtocolAnthropic})
	a1.start = at
	rec.finish(a1)
	a1.end = at.Add(30 * time.Millisecond)
	a1.failed = true
	a2 := rec.begin(2, Upstream{Name: "b", Protocol: ProtocolAnthropic})
	a2.start = at.Add(30 * time.Millisecond)
	a2.ttft = 12 * time.Millisecond
	a2.delivered = true
	rec.finish(a2)
	a2.end = a2.start.Add(250 * time.Millisecond)

	obs := adminMetricsObservationFrom(rec)
	if obs.Error {
		t.Fatalf("末次尝试成功且已交付 = 非失败（obs=%+v）", obs)
	}
	if obs.TTFTMs != 12 || obs.TotalMs != 250 || obs.Failovers != 1 {
		t.Fatalf("投影 = %+v（want ttft 12 / total 250 / failovers 1）", obs)
	}

	// 末次尝试失败（未交付）→ error；无 ttft → 仍是 0（不进直方图）。
	a2.failed, a2.delivered, a2.ttft = true, false, 0
	if obs2 := adminMetricsObservationFrom(rec); !obs2.Error || obs2.TTFTMs != 0 || obs2.TotalMs != 250 {
		t.Fatalf("失败末次尝试投影 = %+v（want error=true / ttft 0 / total 250）", obs2)
	}

	// 同台重试（同上游名两次）不计换台——与 Diagnostics.Failovers / 请求 ring 行同口径。
	recSame := newRequestRecorder("req-metrics-2", registry.FormatOpenAIChat, "gpt-x", false, "")
	for i := range 2 {
		ar := recSame.begin(i+1, Upstream{Name: "a", Protocol: ProtocolAnthropic})
		ar.start = at
		recSame.finish(ar)
		ar.end = at.Add(10 * time.Millisecond)
	}
	if obsSame := adminMetricsObservationFrom(recSame); obsSame.Failovers != 0 {
		t.Fatalf("同台重试换台数 = %d（want 0——同台重试不是 failover）", obsSame.Failovers)
	}

	// 未发起任何尝试（本地失败）→ 一次 error，无延迟观测。
	if obsNone := adminMetricsObservationFrom(newRequestRecorder("req-metrics-3", registry.FormatOpenAIChat, "gpt-x", false, "")); !obsNone.Error || obsNone.TTFTMs != 0 || obsNone.TotalMs != 0 {
		t.Fatalf("无尝试投影 = %+v（want error=true / 无延迟）", obsNone)
	}

	// W88-1：下游断开/取消（disconnected=true、failed=false、未交付）不是网关错误——
	// 客户端主动放弃不计 error；同形状但无断开标记（控制组）= 真未交付 → error；
	// 上游失败（failed=true）无论交付与否 → error。
	recCancel := newRequestRecorder("req-metrics-5", registry.FormatOpenAIChat, "gpt-x", true, "")
	arCancel := recCancel.begin(1, Upstream{Name: "a", Protocol: ProtocolAnthropic})
	arCancel.start = at
	arCancel.ttft = 3 * time.Millisecond
	arCancel.disconnected = true // 客户端读首帧后 RST；failed 保持 false（W86 C1/C2 实测形态）。
	recCancel.finish(arCancel)
	arCancel.end = at.Add(40 * time.Millisecond)
	if obsCancel := adminMetricsObservationFrom(recCancel); obsCancel.Error {
		t.Fatalf("客户端取消被记成 error（obs=%+v；want error=false——disconnected 不是网关失败）", obsCancel)
	}

	// 控制组：同一形状但 disconnected=false（真·未交付）= error——排除逻辑不得放过真失败。
	arCancel.disconnected = false
	if obsCtl := adminMetricsObservationFrom(recCancel); !obsCtl.Error {
		t.Fatalf("未交付且未断开被漏记 error（obs=%+v；want error=true）", obsCtl)
	}

	// 上游失败（failed=true、delivered=false）= error——disconnected 排除不影响上游归因。
	arCancel.failed = true
	if obsUp := adminMetricsObservationFrom(recCancel); !obsUp.Error {
		t.Fatalf("上游失败被漏记 error（obs=%+v；want error=true）", obsUp)
	}

	// 任务面：delivered 口径在 task.go markDelivered（attempt 行恒 false）——成功
	// 任务不得记成失败（登记差异，见 admin_metrics.go 文件头）。
	recTask := newRequestRecorder("req-metrics-4", registry.FormatOpenAIResponses, "gpt-x", false, "task-1")
	at1 := recTask.begin(1, Upstream{Name: "a", Protocol: ProtocolAnthropic})
	at1.start = at
	at1.ttft, at1.delivered = 5*time.Millisecond, false
	recTask.finish(at1)
	at1.end = at.Add(20 * time.Millisecond)
	if obsTask := adminMetricsObservationFrom(recTask); obsTask.Error {
		t.Fatalf("成功任务被记成 error（obs=%+v）", obsTask)
	}
	if adminMetricsObservationFrom(nil) != (adminMetricsObservation{}) {
		t.Fatalf("nil 记账器必须零值安全")
	}
}

// ---------------------------------------------------------------------------
// 交叉对拍：真请求 vs Diagnostics 计数（负控锚点：hook 一去掉就红）
// ---------------------------------------------------------------------------

func TestAdminMetricsCrossCheckDiagnostics(t *testing.T) {
	// 上游 1 恒 503（3ms）、上游 2 先成功后失败（6ms/3ms）——延迟观测确定非零。
	var flaky atomic.Bool
	bad := slowUpstream(t, 3*time.Millisecond, http.StatusServiceUnavailable, errBodyAPIError)
	good := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if flaky.Load() {
			time.Sleep(3 * time.Millisecond)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(errBodyAPIError))
			return
		}
		time.Sleep(6 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		claudeTextStream(w)
	})
	cfg := failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL))
	cfg.AdminKey = adminMetricsKey
	s, srv := adminMetricsStart(t, cfg)

	// ① 换台一次 → 成功（delivered；errors 不增、failovers +1）。
	if code, body := postChatStream(t, srv.URL); code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("请求 1 = %d/%.80s（want 200 + 流内容）", code, body)
	}
	// 常规路径 1:1：serve 请求 → Diagnostics.Requests 与指标面 requests 同步 +1。
	adminMetricsWaitFor(t, s, 1)
	if d1 := s.Diagnostics(); d1.Requests != 1 || d1.Failovers != 1 {
		t.Fatalf("请求 1 后 Diagnostics = requests %d / failovers %d（want 1/1）", d1.Requests, d1.Failovers)
	}
	if r1, e1, f1, _, _ := adminMetricsViewSums(s.adminMetrics.buckets(time.Now(), 30)); r1 != 1 || e1 != 0 || f1 != 1 {
		t.Fatalf("请求 1 后指标面 = requests %d / errors %d / failovers %d（want 1/0/1）", r1, e1, f1)
	}
	// ② 两台都 503 → 末台上游状态透传（503 错误信封；未交付 → errors +1、failovers +1）。
	flaky.Store(true)
	if code, body := postChatStream(t, srv.URL); code != http.StatusServiceUnavailable || !strings.Contains(body, `"error"`) {
		t.Fatalf("请求 2 = %d/%.80s（want 503 + 错误信封）", code, body)
	}
	// ③ 入口解析失败（坏 JSON）→ 不进 failover 链：Diagnostics.Requests 有、指标面没有。
	resp := postJSON(t, srv.URL+"/v1/chat/completions", `{`, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("请求 3 = %d（want 400）", resp.StatusCode)
	}

	adminMetricsWaitFor(t, s, 2)
	d := s.Diagnostics()
	if d.Requests != 3 || d.Failovers != 2 {
		t.Fatalf("Diagnostics = requests %d / failovers %d（want 3/2）", d.Requests, d.Failovers)
	}
	got := adminMetricsBody(t, srv, "?window=30&step=60")
	requests, errors, failovers, ttftHist, totalHist := adminMetricsSums(got.Buckets)
	if requests != 2 {
		t.Fatalf("指标面 requests = %d（want 2 = 走到 failover 链的请求；入口解析失败不计）", requests)
	}
	if failovers != d.Failovers {
		t.Fatalf("指标面 failovers = %d ≠ Diagnostics.Failovers = %d（同一口径必须逐值一致）", failovers, d.Failovers)
	}
	if errors != 1 {
		t.Fatalf("指标面 errors = %d（want 1 = 请求 2 未交付）", errors)
	}
	// 首字节面：两次请求的首个 ttft>0 尝试都有观测（失败尝试的**响应头到达**也置
	// ttft——口径 = 首个 ttft>0 的尝试，见 admin_metrics.go 文件头）；总量面两次
	// 都有（末次尝试 end-start）。
	if ttftHist != 2 || totalHist != 2 {
		t.Fatalf("直方图条数 = ttft %d / total %d（want 2/2：两次请求都有首字节与总量观测）", ttftHist, totalHist)
	}
	// 全部落在当前分钟桶；末桶即最新。
	newest := got.Buckets[len(got.Buckets)-1]
	if newest.Requests != 2 || newest.Errors != 1 || newest.Failovers != 2 {
		t.Fatalf("末桶 = %+v（want requests 2 / errors 1 / failovers 2）", newest)
	}
	if want := time.Now().UnixMilli() % adminMetricsBucketMs; newest.TS%adminMetricsBucketMs != 0 || newest.TS < time.Now().UnixMilli()-adminMetricsBucketMs-want {
		t.Fatalf("末桶 ts = %d（未对齐当前分钟；now=%d）", newest.TS, time.Now().UnixMilli())
	}
	if newest.TTFT.P50 == 0 || newest.Total.P50 == 0 {
		t.Fatalf("末桶分位 = %+v（want 非零：上游有真实延迟）", newest)
	}
	t.Logf("交叉对拍：Diagnostics requests/failovers = %d/%d；指标面 requests/failovers/errors = %d/%d/%d；"+
		"直方图条数 ttft/total = %d/%d；末桶 ts=%d p50(ttft/total)=%d/%d",
		d.Requests, d.Failovers, requests, failovers, errors, ttftHist, totalHist,
		newest.TS, newest.TTFT.P50, newest.Total.P50)
	// 快照面（同一实现函数，非 HTTP）与响应逐值一致。
	snap := s.adminMetricsSnapshot(time.Now(), 30)
	if snap.Buckets[len(snap.Buckets)-1].Requests != newest.Requests {
		t.Fatalf("快照末桶 requests = %d ≠ 响应 %d", snap.Buckets[len(snap.Buckets)-1].Requests, newest.Requests)
	}
}

// ---------------------------------------------------------------------------
// 参数校验（坏值 400；window 1..60；step 只允许 60s）
// ---------------------------------------------------------------------------

func TestAdminMetricsValidation(t *testing.T) {
	cfg := uiTestConfig()
	cfg.AdminKey = adminMetricsKey
	_, srv := adminMetricsStart(t, cfg)

	cases := []struct {
		name        string
		query       string
		wantStatus  int
		wantBuckets int
	}{
		{"default", "", 200, 30},
		{"window_bare_minutes", "?window=30&step=60", 200, 30},
		{"window_and_step_suffix", "?window=30m&step=60s", 200, 30},
		{"window_min", "?window=1", 200, 1},
		{"window_max", "?window=60", 200, 60},
		{"step_suffix_only", "?step=60s", 200, 30},
		{"window_zero", "?window=0", 400, 0},
		{"window_61", "?window=61", 400, 0},
		{"window_negative", "?window=-1", 400, 0},
		{"window_seconds_unit", "?window=90s", 400, 0},
		{"window_hour_unit", "?window=1h", 400, 0},
		{"window_compound", "?window=30m30s", 400, 0},
		{"window_garbage", "?window=abc", 400, 0},
		{"step_59", "?step=59", 400, 0},
		{"step_61", "?step=61", 400, 0},
		{"step_minute_unit", "?step=1m", 400, 0},
		{"step_garbage", "?step=abc", 400, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, raw := adminMetricsGet(t, srv.URL, tc.query, adminMetricsKey)
			if code != tc.wantStatus {
				t.Fatalf("status = %d（want %d）；body=%s", code, tc.wantStatus, raw)
			}
			if code != 200 {
				if !strings.Contains(string(raw), "invalid_request_error") {
					t.Fatalf("400 信封无 invalid_request_error：%s", raw)
				}
				return
			}
			var got adminMetricsWireResponse
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.WindowSeconds != tc.wantBuckets*adminMetricsStepSeconds || len(got.Buckets) != tc.wantBuckets {
				t.Fatalf("window_seconds/桶数 = %d/%d（want %d/%d）",
					got.WindowSeconds, len(got.Buckets), tc.wantBuckets*adminMetricsStepSeconds, tc.wantBuckets)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 鉴权与注册面（未配 key = 404；缺/错 key = 401；对 key = 200；非 GET = 405）
// ---------------------------------------------------------------------------

func TestAdminMetricsAuth(t *testing.T) {
	// 未配 admin_key → 路由不注册（404 非 401）。
	noKey := uiTestConfig()
	_, srvNoKey := adminMetricsStart(t, noKey)
	if code, body := adminMetricsGet(t, srvNoKey.URL, "", ""); code != http.StatusNotFound {
		t.Fatalf("未配 admin_key = %d（want 404）；body=%s", code, body)
	}

	cfg := uiTestConfig()
	cfg.AdminKey = adminMetricsKey
	_, srv := adminMetricsStart(t, cfg)
	// 无头 / 错 key → 401。
	if code, _ := adminMetricsGet(t, srv.URL, "", ""); code != http.StatusUnauthorized {
		t.Fatalf("无 key = %d（want 401）", code)
	}
	if code, _ := adminMetricsGet(t, srv.URL, "", "wrong-key"); code != http.StatusUnauthorized {
		t.Fatalf("错 key = %d（want 401）", code)
	}
	// X-Management-Key / Bearer 两形态 → 200。
	if code, _ := adminMetricsGet(t, srv.URL, "?window=1", adminMetricsKey); code != http.StatusOK {
		t.Fatalf("X-Management-Key = %d（want 200）", code)
	}
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/admin/metrics", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+adminMetricsKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Bearer = %d（want 200）", resp.StatusCode)
	}
	// 非 GET → 405（方法模式限定；路由已注册）。
	post, err := http.NewRequest(http.MethodPost, srv.URL+"/admin/metrics", strings.NewReader(""))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	post.Header.Set("X-Management-Key", adminMetricsKey)
	postResp, err := http.DefaultClient.Do(post)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	postResp.Body.Close()
	if postResp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST = %d（want 405）", postResp.StatusCode)
	}
}
