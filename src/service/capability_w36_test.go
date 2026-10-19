// W36-2（A2A-L19-W36-2）：能力面 build 批验收测试（S4 透传回访 usage 观测 +
// S8 能力面 L2 被动证据）。
//
// 覆盖（brief §Change 逐项）：
//   - S4 透传回访 usage 观测：TestW36PassthroughUsage（retrieve/cancel 回访面
//     只读旁路观测终态 usage → per-task 记录 source=passthrough + 原样转发断言 +
//     去重 + 运行中不观测 + create/SSE 边界钉死 + 零额外上游调用）+
//     TestW36ObserveTee（tee 原样透传/有界单元面）；
//   - S8 能力面 L2 被动证据：TestW36TaskFaceEvidence（off = 计数/ring/日志但不改
//     路由；auto = 首个结论性失败进程内降级 passthrough→managed + 既有路由不受
//     影响；gate = 5xx/429 不下结论不降级）；
//   - 配置面：TestW36TaskFaceDegradeConfig（task_face_degrade 值域校验）；
//   - admin/ring 契约增量：TestW36PassthroughAdminContract（六个新诊断字段
//     snake_case 在 /admin/diagnostics 线上）。
//
// 反证（回退即红；原样输出见 tmp/lanes/W36-capability/proofs/）：
//
//	CP1 去掉 S4 tee 观测（回访面不调 observePassthroughUsage）→
//	    TestW36PassthroughUsage 红（无记录行/计数 0）；
//	CP2 去掉 S4 去重（markUsageObserved 恒 true）→ TestW36PassthroughUsage 红
//	    （重复轮询重复落账）；
//	CP3 去掉 S8 证据调用（round trip 不调 observeTaskFaceEvidence）→
//	    TestW36TaskFaceEvidence 红（计数/ring/日志缺失）；
//	CP4 降级门控去掉（auto 判断删除、恒降级）→ off 子测试红（路由被改）；
//	CP5 结论闸放宽（5xx 也记证据/降级）→ gate 子测试红；
//	CP6 effectiveTaskFace 忽略降级标记 → auto 子测试红（新 create 仍 passthrough）。
package service

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// S4 透传回访 usage 观测
// ---------------------------------------------------------------------------

// w36UsageWire S4 透传 usage 记录（task_passthrough.go recordPassthroughUsage 的
// 线上形状）。
type w36UsageWire struct {
	Task     string `json:"task"`
	Source   string `json:"source"`
	Upstream string `json:"upstream"`
	Model    string `json:"model"`
	Status   string `json:"status"`
	In       int64  `json:"in"`
	Out      int64  `json:"out"`
}

// w36UsageLines 日志里某任务的 usage 记录行（原样行；与托管面同前缀）。
func w36UsageLines(logs, taskID string) []string {
	marker := "any2api: task " + taskID + " terminal usage: "
	var out []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, marker) {
			out = append(out, line)
		}
	}
	return out
}

// w36ParseUsage 取某任务**唯一**一条 usage 记录行并解析（0 条/多条即失败）。
func w36ParseUsage(t *testing.T, logs, taskID string) w36UsageWire {
	t.Helper()
	lines := w36UsageLines(logs, taskID)
	if len(lines) != 1 {
		t.Fatalf("任务 %s usage 记录行数 = %d（want 1）:\n%s", taskID, len(lines), logs)
	}
	marker := "terminal usage: "
	i := strings.Index(lines[0], marker)
	var rec w36UsageWire
	if err := json.Unmarshal([]byte(lines[0][i+len(marker):]), &rec); err != nil {
		t.Fatalf("usage 记录 JSON 解析失败: %v\n%s", err, lines[0])
	}
	return rec
}

// TestW36PassthroughUsage S4：透传回访（retrieve/cancel）非 SSE 2xx 响应经网关
// 时只读旁路观测终态 usage → per-task 记录（source=passthrough）；原样转发字节
// 逐字不变；每任务一条（去重）；运行中/SSE/create 面不观测；零额外上游调用。
func TestW36PassthroughUsage(t *testing.T) {
	logs := captureServiceLogs(t)

	// 上游体（原样转发断言用——逐字节比对下游收到的字节）。
	bodyCreateA := `{"id":"resp_up_s4a","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`
	bodyRunning := `{"id":"resp_up_s4a","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null}`
	bodyTerminal := `{"id":"resp_up_s4a","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5","output":[],"usage":{"input_tokens":11,"output_tokens":7}}`
	bodyCreateB := `{"id":"resp_up_s4b","object":"response","created_at":1741293000,"status":"queued","model":"gpt-5","output":[],"usage":null}`
	bodyCancelled := `{"id":"resp_up_s4b","object":"response","created_at":1741293000,"status":"cancelled","model":"gpt-5","output":[],"usage":{"input_tokens":3,"output_tokens":0}}`

	var mu sync.Mutex
	createN, getA := 0, 0
	up := newPTUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/responses":
			createN++
			w.Header().Set("Content-Type", "application/json")
			if createN == 1 {
				_, _ = w.Write([]byte(bodyCreateA))
			} else {
				_, _ = w.Write([]byte(bodyCreateB))
			}
		case r.Method == http.MethodGet && r.URL.Path == "/v1/responses/resp_up_s4a":
			if r.URL.Query().Get("stream") == "true" {
				w.Header().Set("Content-Type", "text/event-stream")
				sseFrame(w, "response.completed", `{"type":"response.completed","response":{"id":"resp_up_s4a","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5","output":[],"usage":{"input_tokens":11,"output_tokens":7}},"sequence_number":9}`)
				return
			}
			getA++
			w.Header().Set("Content-Type", "application/json")
			if getA == 1 {
				_, _ = w.Write([]byte(bodyRunning))
			} else {
				_, _ = w.Write([]byte(bodyTerminal))
			}
		case r.Method == http.MethodPost && r.URL.Path == "/v1/responses/resp_up_s4b/cancel":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(bodyCancelled))
		default:
			http.NotFound(w, r)
		}
	})
	cfg := cfgFor(up.srv.URL, ProtocolResponses, 15)
	cfg.Upstreams[0].TaskFace = taskFacePassthrough
	s, srv := startServiceWith(t, cfg)

	// ① create A：passthrough 面（id = 上游 id）+ 原样转发。create 体即使为终态
	// +usage 也不观测——S4 面 = 回访（retrieve/cancel），边界钉死。
	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"a","background":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Equal(raw, []byte(bodyCreateA)) {
		t.Fatalf("① create A = %d\n%s", resp.StatusCode, raw)
	}
	if n := len(w36UsageLines(logs.String(), "resp_up_s4a")); n != 0 {
		t.Fatalf("① create 面不得观测（S4 = 回访面）: %d 行", n)
	}

	// ② retrieve A #1：运行中（usage null）→ 无观测（不落标）。
	code, got := getJSON(t, srv.URL+"/v1/responses/resp_up_s4a")
	if code != 200 || !bytes.Equal(got, []byte(bodyRunning)) {
		t.Fatalf("② retrieve#1 = %d\n%s", code, got)
	}
	if n := len(w36UsageLines(logs.String(), "resp_up_s4a")); n != 0 {
		t.Fatalf("② 运行中快照不得观测: %d 行", n)
	}

	// ③ retrieve A #2：终态 + usage → 落账；响应字节逐字原样（观测不改转发）。
	code, got = getJSON(t, srv.URL+"/v1/responses/resp_up_s4a")
	if code != 200 || !bytes.Equal(got, []byte(bodyTerminal)) {
		t.Fatalf("③ retrieve#2 = %d（响应非上游原样——观测面不得改字节）\n%s", code, got)
	}
	recA := w36ParseUsage(t, logs.String(), "resp_up_s4a")
	if recA.Source != "passthrough" || recA.Status != taskCompleted || recA.In != 11 || recA.Out != 7 ||
		recA.Model != "gpt-5" || recA.Upstream != "fake" || recA.Task != "resp_up_s4a" {
		t.Fatalf("③ usage 记录 = %+v（want source=passthrough/status=completed/in=11/out=7/model=gpt-5）", recA)
	}

	// ④ retrieve A #3（同体重复轮询）→ 去重：仍一条记录（计数不重复）。
	code, _ = getJSON(t, srv.URL+"/v1/responses/resp_up_s4a")
	if code != 200 {
		t.Fatalf("④ retrieve#3 = %d", code)
	}
	if n := len(w36UsageLines(logs.String(), "resp_up_s4a")); n != 1 {
		t.Fatalf("④ 重复轮询不得重复落账（want 1 行）: %d 行", n)
	}

	// ⑤ 续流（SSE）面：终态事件带 usage——非 SSE 才观测 → 不落账。
	code, got = getJSON(t, srv.URL+"/v1/responses/resp_up_s4a?stream=true&starting_after=0")
	if code != 200 || !bytes.Contains(got, []byte("response.completed")) {
		t.Fatalf("⑤ resume = %d\n%s", code, got)
	}
	if n := len(w36UsageLines(logs.String(), "resp_up_s4a")); n != 1 {
		t.Fatalf("⑤ SSE 面不得观测（want 1 行）: %d 行", n)
	}

	// ⑥ create B + cancel B：取消面（回访）终态 usage → 落账。
	respB := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"b","background":true}`, nil)
	rawB, _ := io.ReadAll(respB.Body)
	respB.Body.Close()
	if respB.StatusCode != 200 || !bytes.Equal(rawB, []byte(bodyCreateB)) {
		t.Fatalf("⑥ create B = %d\n%s", respB.StatusCode, rawB)
	}
	respC := postJSON(t, srv.URL+"/v1/responses/resp_up_s4b/cancel", "", nil)
	rawC, _ := io.ReadAll(respC.Body)
	respC.Body.Close()
	if respC.StatusCode != 200 || !bytes.Equal(rawC, []byte(bodyCancelled)) {
		t.Fatalf("⑥ cancel B = %d\n%s", respC.StatusCode, rawC)
	}
	recB := w36ParseUsage(t, logs.String(), "resp_up_s4b")
	if recB.Status != taskCancelled || recB.In != 3 || recB.Out != 0 {
		t.Fatalf("⑥ 取消面记录 = %+v（want cancelled/in=3/out=0）", recB)
	}

	// ⑦ 计数 + 零额外上游调用（观测纯旁路）。
	d := s.Diagnostics()
	if d.PassthroughUsageRecords != 2 || d.PassthroughUsageInTokens != 14 || d.PassthroughUsageOutTokens != 7 {
		t.Fatalf("⑦ usage 计数 = records %d / in %d / out %d（want 2/14/7）",
			d.PassthroughUsageRecords, d.PassthroughUsageInTokens, d.PassthroughUsageOutTokens)
	}
	if n := up.count(); n != 7 {
		t.Fatalf("⑦ 上游调用数 = %d（want 7：create A + retrieve ×3 + resume + create B + cancel B——观测零额外调用）", n)
	}
	if d.PassthroughFace404 != 0 || d.PassthroughFace405 != 0 || d.PassthroughFaceDegrades != 0 {
		t.Fatalf("⑦ 无 404/405 面不得有证据计数: %+v", d)
	}
}

// TestW36ObserveTee S4 tee 单元面：小体原样透传 + 副本完整；超限透传原样 +
// 缓冲弃置（over 置位——大响应不观测）。
func TestW36ObserveTee(t *testing.T) {
	tee := &observeTee{src: strings.NewReader("hello"), limit: 64}
	var out bytes.Buffer
	if _, err := io.Copy(&out, tee); err != nil {
		t.Fatalf("copy: %v", err)
	}
	if out.String() != "hello" || tee.over || string(tee.buf) != "hello" {
		t.Fatalf("tee 小体 = %q over=%v buf=%q（want 原样 + 完整副本）", out.String(), tee.over, tee.buf)
	}
	tee2 := &observeTee{src: strings.NewReader("hello world"), limit: 5}
	out.Reset()
	if _, err := io.Copy(&out, tee2); err != nil {
		t.Fatalf("copy2: %v", err)
	}
	if out.String() != "hello world" || !tee2.over || tee2.buf != nil {
		t.Fatalf("tee 超限 = %q over=%v buf=%v（want 透传原样 + over 置位 + 缓冲弃置）", out.String(), tee2.over, tee2.buf)
	}
}

// ---------------------------------------------------------------------------
// S8 能力面 L2 被动证据
// ---------------------------------------------------------------------------

// w36EvidenceWire task_face_evidence ring 事件载荷（observeTaskFaceEvidence 的
// 线上形状）。
type w36EvidenceWire struct {
	Upstream   string `json:"upstream"`
	Status     int    `json:"status"`
	Path       string `json:"path"`
	Task       string `json:"task"`
	DegradeNow bool   `json:"degrade_now"`
}

// w36EvidenceEntry ring 条目 + 解析后的载荷。
type w36EvidenceEntry struct {
	entry adminLogEntry
	wire  w36EvidenceWire
}

// w36RingByKind ring 里某类事件（按 Seq 升序）。
func w36RingByKind(s *Server, kind string) []adminLogEntry {
	var out []adminLogEntry
	win := s.adminLog.window(0, "", 200, false)
	for i := range win.Entries {
		if win.Entries[i].Kind == kind {
			out = append(out, win.Entries[i])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// w36EvidenceEntries ring 里全部 task_face_evidence 条目（解析载荷；Seq 升序）。
func w36EvidenceEntries(t *testing.T, s *Server) []w36EvidenceEntry {
	t.Helper()
	var out []w36EvidenceEntry
	for _, e := range w36RingByKind(s, adminLogTaskFaceEvidence) {
		var w w36EvidenceWire
		if err := json.Unmarshal([]byte(e.Message), &w); err != nil {
			t.Fatalf("task_face_evidence 载荷解析失败: %v\n%s", err, e.Message)
		}
		out = append(out, w36EvidenceEntry{entry: e, wire: w})
	}
	return out
}

// TestW36TaskFaceEvidence S8：L2 被动证据三场景（off / auto / gate）。
func TestW36TaskFaceEvidence(t *testing.T) {
	t.Run("off_no_route_change", testW36EvidenceOff)
	t.Run("auto_degrades", testW36EvidenceAuto)
	t.Run("gate_no_conclusion", testW36EvidenceGate)
}

// testW36EvidenceOff 场景一（缺省 off）：404/405 → 计数 + ring + 日志，但**绝不
// 改路由**（声明 = 唯一路由权威）；create 面 404 同样计证据。
func testW36EvidenceOff(t *testing.T) {
	logs := captureServiceLogs(t)
	body404 := `{"error":{"message":"No response found","type":"invalid_request_error","code":"not_found"}}`
	bodyCreate1 := `{"id":"resp_up_ev1","object":"response","created_at":1,"status":"queued","model":"gpt-5","output":[],"usage":null}`
	bodyCreate2 := `{"id":"resp_up_ev2","object":"response","created_at":1,"status":"queued","model":"gpt-5","output":[],"usage":null}`

	var mu sync.Mutex
	createN := 0
	up := newPTUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/responses":
			createN++
			w.Header().Set("Content-Type", "application/json")
			switch createN {
			case 1:
				_, _ = w.Write([]byte(bodyCreate1))
			case 2:
				_, _ = w.Write([]byte(bodyCreate2))
			default:
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(body404))
			}
		case r.Method == http.MethodGet && r.URL.Path == "/v1/responses/resp_up_ev1":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(body404))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/responses/resp_up_ev1/cancel":
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte("method not allowed"))
		default:
			http.NotFound(w, r)
		}
	})
	cfg := cfgFor(up.srv.URL, ProtocolResponses, 15)
	cfg.Upstreams[0].TaskFace = taskFacePassthrough
	// TaskFaceDegrade 缺省（off）——只观测不改路由。
	s, srv := startServiceWith(t, cfg)

	// ① create → passthrough（id = 上游 id）。
	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"x","background":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Contains(raw, []byte(`"resp_up_ev1"`)) {
		t.Fatalf("① create = %d\n%s", resp.StatusCode, raw)
	}

	// ② retrieve → 上游 404 原样直传（字节逐字）+ 证据（计数/ring/日志）。
	code, got := getJSON(t, srv.URL+"/v1/responses/resp_up_ev1")
	if code != 404 || !bytes.Equal(got, []byte(body404)) {
		t.Fatalf("② retrieve = %d（want 404 + 上游体原样）\n%s", code, got)
	}
	ev := w36EvidenceEntries(t, s)
	if len(ev) != 1 || ev[0].wire.Status != 404 || ev[0].wire.Task != "resp_up_ev1" || ev[0].wire.DegradeNow {
		t.Fatalf("② evidence = %+v（want 404/resp_up_ev1/degrade_now=false）", ev)
	}
	if ev[0].entry.Upstream != "fake" || ev[0].entry.Status != 404 || ev[0].entry.RequestID != "resp_up_ev1" {
		t.Fatalf("② ring 行关联面 = %+v（want upstream=fake/status=404/request_id=任务 id）", ev[0].entry)
	}
	if ev[0].wire.Path != "/v1/responses/resp_up_ev1" {
		t.Fatalf("② evidence path = %q（want 上游任务端点路径）", ev[0].wire.Path)
	}
	if !strings.Contains(logs.String(), `conclusive failure (status=404 path=/v1/responses/resp_up_ev1 task="resp_up_ev1" degrade_now=false)`) {
		t.Fatalf("② 证据日志行缺失:\n%s", logs.String())
	}

	// ③ cancel → 405 证据（回访面第二形）。
	respC := postJSON(t, srv.URL+"/v1/responses/resp_up_ev1/cancel", "", nil)
	rawC, _ := io.ReadAll(respC.Body)
	respC.Body.Close()
	if respC.StatusCode != 405 {
		t.Fatalf("③ cancel = %d（want 405 原样直传）\n%s", respC.StatusCode, rawC)
	}
	d := s.Diagnostics()
	if d.PassthroughFace404 != 1 || d.PassthroughFace405 != 1 || d.PassthroughFaceDegrades != 0 {
		t.Fatalf("③ 证据计数 = 404:%d 405:%d degrades:%d（want 1/1/0——off 不降级）",
			d.PassthroughFace404, d.PassthroughFace405, d.PassthroughFaceDegrades)
	}
	if n := len(w36RingByKind(s, adminLogTaskFaceDegrade)); n != 0 {
		t.Fatalf("③ off 下不得有降级事件: %d", n)
	}

	// ④ 路由不变（off = 声明权威）：新 create 仍走 passthrough（id = 上游 id）。
	resp2 := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"y","background":true}`, nil)
	raw2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	var created2 struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw2, &created2); err != nil {
		t.Fatalf("④ create2 body: %v\n%s", err, raw2)
	}
	if created2.ID != "resp_up_ev2" {
		t.Fatalf("④ off 下路由被改（create id = %q，want 上游 id resp_up_ev2）", created2.ID)
	}

	// ⑤ create 面结论性失败同样计证据（设计原文 = create/回访）。
	resp3 := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"z","background":true}`, nil)
	raw3, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()
	if resp3.StatusCode != 404 || !bytes.Equal(raw3, []byte(body404)) {
		t.Fatalf("⑤ create3 = %d（want 上游 404 原样）\n%s", resp3.StatusCode, raw3)
	}
	d = s.Diagnostics()
	if d.PassthroughFace404 != 2 || d.PassthroughFace405 != 1 || d.PassthroughFaceDegrades != 0 {
		t.Fatalf("⑤ 证据计数 = 404:%d 405:%d（want 2/1——create 面计证据）",
			d.PassthroughFace404, d.PassthroughFace405)
	}
	if ev = w36EvidenceEntries(t, s); len(ev) != 3 {
		t.Fatalf("⑤ evidence 事件数 = %d（want 3）", len(ev))
	}
}

// testW36EvidenceAuto 场景二（auto）：首个结论性失败 → 进程内降级
// passthrough→managed（新 create 走托管路径）；既有路由不受影响（在飞/既有任务
// 仍直连上游）；二次证据不再降级。
func testW36EvidenceAuto(t *testing.T) {
	logs := captureServiceLogs(t)
	body404 := `{"error":{"message":"No response found","type":"invalid_request_error","code":"not_found"}}`
	bodyCreate1 := `{"id":"resp_up_deg","object":"response","created_at":1,"status":"queued","model":"gpt-5","output":[],"usage":null}`

	var mu sync.Mutex
	createN := 0
	up := newPTUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/responses":
			createN++
			if createN == 1 {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(bodyCreate1))
				return
			}
			// 降级后的托管任务上游（background 已剥除）→ 立即终态流。
			w.Header().Set("Content-Type", "text/event-stream")
			w36TaskStream(w)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/responses/resp_up_deg":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(body404))
		default:
			http.NotFound(w, r)
		}
	})
	cfg := cfgFor(up.srv.URL, ProtocolResponses, 15)
	cfg.Upstreams[0].TaskFace = taskFacePassthrough
	cfg.TaskFaceDegrade = taskFaceDegradeAuto
	s, srv := startServiceWith(t, cfg)

	// ① create → passthrough。
	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"x","background":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Contains(raw, []byte(`"resp_up_deg"`)) {
		t.Fatalf("① create = %d\n%s", resp.StatusCode, raw)
	}

	// ② retrieve → 404 → 证据 + 首个结论性失败 → 进程内降级。
	code, got := getJSON(t, srv.URL+"/v1/responses/resp_up_deg")
	if code != 404 || !bytes.Equal(got, []byte(body404)) {
		t.Fatalf("② retrieve = %d\n%s", code, got)
	}
	d := s.Diagnostics()
	if d.PassthroughFace404 != 1 || d.PassthroughFaceDegrades != 1 {
		t.Fatalf("② 证据/降级计数 = 404:%d degrades:%d（want 1/1）", d.PassthroughFace404, d.PassthroughFaceDegrades)
	}
	ev := w36EvidenceEntries(t, s)
	if len(ev) != 1 || !ev[0].wire.DegradeNow {
		t.Fatalf("② evidence = %+v（want 单条 degrade_now=true）", ev)
	}
	deg := w36RingByKind(s, adminLogTaskFaceDegrade)
	if len(deg) != 1 {
		t.Fatalf("② 降级事件数 = %d（want 1）", len(deg))
	}
	var dw struct {
		Upstream string `json:"upstream"`
		From     string `json:"from"`
		To       string `json:"to"`
	}
	if err := json.Unmarshal([]byte(deg[0].Message), &dw); err != nil {
		t.Fatalf("② 降级载荷解析失败: %v\n%s", err, deg[0].Message)
	}
	if dw.Upstream != "fake" || dw.From != taskFacePassthrough || dw.To != taskFaceManaged {
		t.Fatalf("② 降级载荷 = %+v（want fake/passthrough→managed）", dw)
	}
	if !strings.Contains(logs.String(), "task_face degraded passthrough->managed (in-process; restart clears)") {
		t.Fatalf("② 降级日志行缺失:\n%s", logs.String())
	}
	if face := s.effectiveTaskFace(0); face != taskFaceManaged {
		t.Fatalf("② 有效任务面 = %q（want managed——进程内降级）", face)
	}

	// ③ 新 create → 托管路径（进程内降级生效）：合成 id + background 剥除。
	resp3 := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"z","background":true}`, nil)
	raw3, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()
	var created3 struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw3, &created3); err != nil {
		t.Fatalf("③ create3 body: %v\n%s", err, raw3)
	}
	if !strings.HasPrefix(created3.ID, "resp_synth_") {
		t.Fatalf("③ 降级后 create id = %q（want resp_synth_*——托管路径）", created3.ID)
	}
	waitFor(t, 5*time.Second, func() bool { return up.count() >= 3 })
	calls := up.snapshot()
	if len(calls) < 3 || bytes.Contains(calls[2].body, []byte(`"background"`)) {
		t.Fatalf("③ 托管任务上游体含 background（want 剥除——托管语义）: %s", calls[2].body)
	}
	waitTaskTerminal(t, srv.URL, created3.ID)

	// ④ 既有路由不受影响（降级只影响新裁定）：resp_up_deg 仍直连上游 → 再证据；
	// 二次证据不再降级（转换只一次）。
	code, _ = getJSON(t, srv.URL+"/v1/responses/resp_up_deg")
	if code != 404 {
		t.Fatalf("④ 既有路由 retrieve = %d（want 404 直连上游——降级不改既有路由）", code)
	}
	d = s.Diagnostics()
	if d.PassthroughFace404 != 2 || d.PassthroughFaceDegrades != 1 {
		t.Fatalf("④ 证据/降级计数 = 404:%d degrades:%d（want 2/1——二次证据不再降级）",
			d.PassthroughFace404, d.PassthroughFaceDegrades)
	}
	ev = w36EvidenceEntries(t, s)
	if len(ev) != 2 || ev[1].wire.DegradeNow {
		t.Fatalf("④ evidence = %+v（want 2 条、第二条 degrade_now=false）", ev)
	}
	if n := len(w36RingByKind(s, adminLogTaskFaceDegrade)); n != 1 {
		t.Fatalf("④ 降级事件数 = %d（want 1——转换只一次）", n)
	}
}

// testW36EvidenceGate 场景三（结论闸）：网络失败/5xx/429 不下结论——不计证据、
// 不降级（即使 task_face_degrade=auto）；响应原样直传不受影响。
func testW36EvidenceGate(t *testing.T) {
	logs := captureServiceLogs(t)
	body500 := `{"error":{"message":"boom","type":"server_error"}}`
	bodyCreate1 := `{"id":"resp_up_gate1","object":"response","created_at":1,"status":"queued","model":"gpt-5","output":[],"usage":null}`
	bodyCreate2 := `{"id":"resp_up_gate2","object":"response","created_at":1,"status":"queued","model":"gpt-5","output":[],"usage":null}`

	var mu sync.Mutex
	createN := 0
	up := newPTUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/responses":
			createN++
			w.Header().Set("Content-Type", "application/json")
			if createN == 1 {
				_, _ = w.Write([]byte(bodyCreate1))
			} else {
				_, _ = w.Write([]byte(bodyCreate2))
			}
		case r.Method == http.MethodGet && r.URL.Path == "/v1/responses/resp_up_gate1":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(body500))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/responses/resp_up_gate1/cancel":
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("rate limited"))
		default:
			http.NotFound(w, r)
		}
	})
	cfg := cfgFor(up.srv.URL, ProtocolResponses, 15)
	cfg.Upstreams[0].TaskFace = taskFacePassthrough
	cfg.TaskFaceDegrade = taskFaceDegradeAuto // 即使 auto——非结论性失败不降级。
	s, srv := startServiceWith(t, cfg)

	// ① create → passthrough；② retrieve → 500（原样直传）；③ cancel → 429。
	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"x","background":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Contains(raw, []byte(`"resp_up_gate1"`)) {
		t.Fatalf("① create = %d\n%s", resp.StatusCode, raw)
	}
	code, got := getJSON(t, srv.URL+"/v1/responses/resp_up_gate1")
	if code != 500 || !bytes.Equal(got, []byte(body500)) {
		t.Fatalf("② retrieve = %d（want 500 原样直传）\n%s", code, got)
	}
	respC := postJSON(t, srv.URL+"/v1/responses/resp_up_gate1/cancel", "", nil)
	rawC, _ := io.ReadAll(respC.Body)
	respC.Body.Close()
	if respC.StatusCode != 429 {
		t.Fatalf("③ cancel = %d（want 429 原样直传）\n%s", respC.StatusCode, rawC)
	}

	// ④ 无证据/无降级：计数全 0、无 ring 事件、无日志行、有效面不变。
	d := s.Diagnostics()
	if d.PassthroughFace404 != 0 || d.PassthroughFace405 != 0 || d.PassthroughFaceDegrades != 0 {
		t.Fatalf("④ 非结论性失败不得计证据: 404:%d 405:%d degrades:%d",
			d.PassthroughFace404, d.PassthroughFace405, d.PassthroughFaceDegrades)
	}
	if n := len(w36RingByKind(s, adminLogTaskFaceEvidence)); n != 0 {
		t.Fatalf("④ 不得有 evidence 事件: %d", n)
	}
	if n := len(w36RingByKind(s, adminLogTaskFaceDegrade)); n != 0 {
		t.Fatalf("④ 不得有降级事件: %d", n)
	}
	if strings.Contains(logs.String(), "conclusive failure") {
		t.Fatalf("④ 不得写证据日志行:\n%s", logs.String())
	}
	if face := s.effectiveTaskFace(0); face != taskFacePassthrough {
		t.Fatalf("④ 有效任务面 = %q（want passthrough——不下结论）", face)
	}

	// ⑤ 路由不变：新 create 仍 passthrough。
	resp2 := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"y","background":true}`, nil)
	raw2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	var created2 struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw2, &created2); err != nil {
		t.Fatalf("⑤ create2 body: %v\n%s", err, raw2)
	}
	if created2.ID != "resp_up_gate2" {
		t.Fatalf("⑤ 5xx/429 后路由被改（create id = %q，want resp_up_gate2）", created2.ID)
	}
}

// ---------------------------------------------------------------------------
// 配置面 + admin 契约
// ---------------------------------------------------------------------------

// TestW36TaskFaceDegradeConfig task_face_degrade 值域校验：只认 off|auto，
// 空 = off；非法值显式报错。
func TestW36TaskFaceDegradeConfig(t *testing.T) {
	base := func(v string) *Config {
		cfg := cfgFor("http://127.0.0.1:1", ProtocolResponses, 15)
		cfg.TaskFaceDegrade = v
		return cfg
	}
	if err := base("bogus").Validate(); err == nil || !strings.Contains(err.Error(), "task_face_degrade") {
		t.Fatalf("非法 task_face_degrade 值未被拒：%v", err)
	}
	for _, v := range []string{"", taskFaceDegradeOff, taskFaceDegradeAuto} {
		if err := base(v).Validate(); err != nil {
			t.Fatalf("合法 task_face_degrade %q 被拒：%v", v, err)
		}
	}
}

// TestW36PassthroughAdminContract admin JSON 契约增量：六个新诊断字段（S4 usage
// 三项 + S8 证据三项）snake_case 在 /admin/diagnostics 线上 + 初值 0。
func TestW36PassthroughAdminContract(t *testing.T) {
	up := newPTUpstream(t, func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) })
	_, srv := startServiceWith(t, adminCfg("secret-key", Upstream{
		Name: "upstream", BaseURL: up.srv.URL, APIKey: "k", Protocol: ProtocolResponses, TaskFace: taskFacePassthrough,
	}))
	raw := adminBody(t, adminReq(t, http.MethodGet, srv.URL+"/admin/diagnostics", "secret-key", ""))
	for _, field := range []string{
		`"passthrough_usage_records"`, `"passthrough_usage_in_tokens"`, `"passthrough_usage_out_tokens"`,
		`"passthrough_face_404"`, `"passthrough_face_405"`, `"passthrough_face_degrades"`,
	} {
		if !strings.Contains(raw, field) {
			t.Fatalf("诊断响应缺字段 %s:\n%s", field, raw)
		}
	}
	if d := adminDiagnostics(t, srv.URL, "secret-key").Diagnostics; d.PassthroughUsageRecords != 0 ||
		d.PassthroughUsageInTokens != 0 || d.PassthroughUsageOutTokens != 0 ||
		d.PassthroughFace404 != 0 || d.PassthroughFace405 != 0 || d.PassthroughFaceDegrades != 0 {
		t.Fatalf("初值应为 0: %+v", d)
	}
}
