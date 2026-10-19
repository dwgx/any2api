// W36-1（A2A-L19-W36-1）：记账口径收口 build 批验收测试（S1/S2/S6/S7 + admin 契约）。
//
// 覆盖（brief §Change 逐项）：
//   - S1 end_reason 枚举：TestW36EndReasonSync（completed / upstream_error /
//     upstream_idle / client_cancel / 本地失败省略）+ TestW36EndReasonTaskCancel
//     （task_cancel）；
//   - S2 delivered 布尔：TestW36EndReasonSync（同步面 true/false 两态）+
//     TestW36TaskFaceDelivery（任务面 retrieve/resume 交付 + 未交付反证）；
//   - S6 task_terminal ring：TestW36TaskTerminalRing；
//   - S7 任务面计数 + admin JSON 契约：TestW36TaskDiagnosticsCounters。
//
// 反证（回退即红；原样输出见 tmp/lanes/W36-accounting/proofs/）：
//
//	CP1 去掉 relay 的断连细分（disconnectEndReason）→ 任务 cancel 归因回落
//	    downstream_disconnect → TestW36EndReasonTaskCancel 红；
//	CP2 去掉 idle 窄化（defer 的 upstream_idle 分支 / client.Do 错误面）→
//	    TestW36EndReasonSync 的 idle 段红（回落 upstream_error）；
//	CP3 delivered 在断连/截断路径也置位 → TestW36EndReasonSync 的断开段与截断段红；
//	CP4 runTask 去掉 recordTaskTerminal → TestW36TaskTerminalRing 红；
//	CP5 幂等重放计进 tasks_created（或终态计数漏记）→
//	    TestW36TaskDiagnosticsCounters 红；
//	CP6 任务面交付口径落到 attempt 行（而非交付点）→ TestW36TaskFaceDelivery 红
//	    （attempt 行恒 false + 交付行只在 retrieve/resume 后出现）。
package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// w36TaskStream 快流（立即终态）：created → delta → completed（带 usage）。
func w36TaskStream(w http.ResponseWriter) {
	sseFrame(w, "response.created", `{"type":"response.created","response":{"id":"resp_w36","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":0}`)
	sseFrame(w, "response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_w36","output_index":0,"content_index":0,"delta":"ok","sequence_number":1}`)
	sseFrame(w, "response.completed", `{"type":"response.completed","response":{"id":"resp_w36","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5","output":[{"id":"msg_w36","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"ok","annotations":[]}]}],"usage":`+taskSlowUsageJSON+`,"incomplete_details":null},"sequence_number":2}`)
}

// w36GatedUpstream 假上游（responses）：请求体含 "gate" → 建流帧后挂住（测试释放
// 或任务取消中止）；否则立即走完整终态流（w36TaskStream）。返回上游（count() 可
// 用）与释放函数（幂等；t.Cleanup 兜底防挂）。
//
// 注意：本 helper 用 newCountUpstream（**不预读请求体**）——responsesUpstream 的
// 包装器会先读体，handler 里再读即空（W36-1 实测踩坑）。
func w36GatedUpstream(t *testing.T) (*countUpstream, func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		if !bytes.Contains(b, []byte("gate")) {
			w36TaskStream(w)
			return
		}
		sseFrame(w, "response.created", `{"type":"response.created","response":{"id":"resp_w36","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":0}`)
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
		sseFrame(w, "response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_w36","output_index":0,"content_index":0,"delta":"ok","sequence_number":1}`)
		sseFrame(w, "response.completed", `{"type":"response.completed","response":{"id":"resp_w36","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5","output":[{"id":"msg_w36","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"ok","annotations":[]}]}],"usage":`+taskSlowUsageJSON+`,"incomplete_details":null},"sequence_number":2}`)
	})
	return up, release
}

// w36DeliveredWire 任务交付行载荷（task.go markDelivered 的线上形状）。
type w36DeliveredWire struct {
	Task   string `json:"task"`
	Via    string `json:"via"`
	Status string `json:"status"`
}

// w36DeliveredLines 日志里某任务的交付行（原样行）。
func w36DeliveredLines(logs, taskID string) []string {
	marker := "any2api: task " + taskID + " terminal delivered: "
	var out []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, marker) {
			out = append(out, line)
		}
	}
	return out
}

// w36ParseDelivered 取某任务**唯一**一条交付行并解析（0 条/多条即失败）。
func w36ParseDelivered(t *testing.T, logs, taskID string) w36DeliveredWire {
	t.Helper()
	lines := w36DeliveredLines(logs, taskID)
	if len(lines) != 1 {
		t.Fatalf("任务 %s 交付行数 = %d（want 1）:\n%s", taskID, len(lines), logs)
	}
	marker := "terminal delivered: "
	i := strings.Index(lines[0], marker)
	var rec w36DeliveredWire
	if err := json.Unmarshal([]byte(lines[0][i+len(marker):]), &rec); err != nil {
		t.Fatalf("交付行 JSON 解析失败: %v\n%s", err, lines[0])
	}
	return rec
}

// w36RingWire task_terminal ring 事件载荷（task.go recordTaskTerminal 的线上形状）。
type w36RingWire struct {
	Task       string `json:"task"`
	Status     string `json:"status"`
	Attempts   int    `json:"attempts"`
	DurationMs int64  `json:"duration_ms"`
}

// w36RingTerminal 取 ring 里某任务的 task_terminal 条目（缺失 → nil）。
func w36RingTerminal(s *Server, taskID string) *adminLogEntry {
	win := s.adminLog.window(0, "", 200, false)
	for i := range win.Entries {
		e := win.Entries[i]
		if e.Kind != adminLogTaskTerminal {
			continue
		}
		var p w36RingWire
		if json.Unmarshal([]byte(e.Message), &p) != nil {
			continue
		}
		if p.Task == taskID {
			return &e
		}
	}
	return nil
}

// w36ParseRingTerminal 取 ring 里某任务的 task_terminal 条目并解析（缺失即失败）。
func w36ParseRingTerminal(t *testing.T, s *Server, taskID string) (adminLogEntry, w36RingWire) {
	t.Helper()
	e := w36RingTerminal(s, taskID)
	if e == nil {
		t.Fatalf("任务 %s 的 task_terminal ring 条目缺失", taskID)
	}
	var p w36RingWire
	if err := json.Unmarshal([]byte(e.Message), &p); err != nil {
		t.Fatalf("task_terminal 载荷解析失败: %v\n%s", err, e.Message)
	}
	return *e, p
}

// w36CreateTask 建托管任务并返回 id（快照 status 必须是 queued/in_progress）。
func w36CreateTask(t *testing.T, srvURL, body string, headers map[string]string) string {
	t.Helper()
	code, m := createTask(t, srvURL, body, headers)
	if code != 200 {
		t.Fatalf("create status = %d（%s）", code, body)
	}
	return jsonString(t, m, "id")
}

// w36Resume 拉一次续流面（GET ?stream=true&starting_after=N）到流尽，返回整体字节。
func w36Resume(t *testing.T, srvURL, id string, after int) string {
	t.Helper()
	resp, err := http.Get(srvURL + "/v1/responses/" + id + "?stream=true&starting_after=" + strconv.Itoa(after))
	if err != nil {
		t.Fatalf("resume get: %v", err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("resume status = %d\n%s", resp.StatusCode, raw)
	}
	return string(raw)
}

// w36WaitTerminal 等任务终态落定并返回状态——**store 直读**（不经 retrieve/续流
// 面：那两面会落交付位，污染交付断言）。超时即失败。
func w36WaitTerminal(t *testing.T, s *Server, id string) string {
	t.Helper()
	var status string
	waitFor(t, 5*time.Second, func() bool {
		te, _ := s.tasks.get(id)
		if te == nil {
			return false
		}
		te.mu.Lock()
		defer te.mu.Unlock()
		if te.snapshot == nil {
			return false
		}
		status = te.status
		return true
	})
	return status
}

// ---------------------------------------------------------------------------
// S1 + S2 同步面
// ---------------------------------------------------------------------------

// TestW36EndReasonSync S1/S2 同步面：① 流式成功 = completed + delivered；
// ② 预提交失败换上游 = upstream_error（delivered=false）+ completed；
// ③ 非流式成功 = completed + delivered；④ 客户端断开 = client_cancel +
// delivered=false；⑤ 截断（提交后无终止证据 EOF）= upstream_error + delivered=false
// （错误帧不是交付）；⑥ 本地失败（未达上游）= end_reason 省略 + delivered=false。
func TestW36EndReasonSync(t *testing.T) {
	var logBuf syncLogBuffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	// ① 流式成功。
	good := goodClaudeUpstream(t)
	srv := startService(t, failoverCfg(anthUp("good", good.srv.URL)))
	if code, body := postChatStream(t, srv.URL); code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("① 流式 status=%d body=\n%s", code, body)
	}
	rec := parseRequestRecord(t, logBuf.String())
	if len(rec.Attempts) != 1 {
		t.Fatalf("① attempt 数 = %d（want 1）", len(rec.Attempts))
	}
	if a := rec.Attempts[0]; a.EndReason != endReasonCompleted || !a.Delivered || a.Failed || a.Disconnected {
		t.Fatalf("① 成功归因 = %+v（want completed+delivered，无 failed/disconnected）", a)
	}
	logBuf.Reset()

	// ② 预提交失败（503）换上游。
	bad := failUpstream(t, 503, nil, errBodyAPIError)
	good2 := goodClaudeUpstream(t)
	_, srv2 := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good2.srv.URL)))
	if code, body := postChatStream(t, srv2.URL); code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("② status=%d body=\n%s", code, body)
	}
	rec2 := parseRequestRecord(t, logBuf.String())
	if len(rec2.Attempts) != 2 {
		t.Fatalf("② attempt 数 = %d（want 2）: %+v", len(rec2.Attempts), rec2.Attempts)
	}
	if a := rec2.Attempts[0]; a.EndReason != endReasonUpstreamError || a.Delivered || !a.Failed {
		t.Fatalf("② 失败尝试 = %+v（want upstream_error + delivered=false + failed）", a)
	}
	if a := rec2.Attempts[1]; a.EndReason != endReasonCompleted || !a.Delivered || a.Failed {
		t.Fatalf("② 成功尝试 = %+v（want completed + delivered）", a)
	}
	logBuf.Reset()

	// ③ 非流式成功。
	upChat := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"c1","object":"chat.completion","created":1,"model":"gpt-x","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`))
	})
	_, srv3 := startServiceWith(t, cfgFor(upChat.srv.URL, ProtocolOpenAIChat, 15))
	resp3 := postJSON(t, srv3.URL+"/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	_, _ = io.ReadAll(resp3.Body)
	resp3.Body.Close()
	if resp3.StatusCode != 200 {
		t.Fatalf("③ status = %d", resp3.StatusCode)
	}
	rec3 := parseRequestRecord(t, logBuf.String())
	if len(rec3.Attempts) != 1 {
		t.Fatalf("③ attempt 数 = %d（want 1）", len(rec3.Attempts))
	}
	if a := rec3.Attempts[0]; a.EndReason != endReasonCompleted || !a.Delivered {
		t.Fatalf("③ 非流式归因 = %+v（want completed + delivered）", a)
	}
	logBuf.Reset()

	// ④ 客户端断开（下游取消）——delivered=false（终态产出未送达）。
	upHang := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_d","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}`)
		<-r.Context().Done()
	})
	_, srv4 := startServiceWith(t, cfgFor(upHang.srv.URL, ProtocolAnthropic, 15))
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv4.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp4, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("④ do: %v", err)
	}
	buf := make([]byte, 64)
	if _, err := resp4.Body.Read(buf); err != nil {
		t.Fatalf("④ first read: %v", err)
	}
	cancel()
	resp4.Body.Close()
	waitFor(t, 3*time.Second, func() bool { return strings.Contains(logBuf.String(), `"end_reason"`) })
	rec4 := parseRequestRecord(t, logBuf.String())
	if len(rec4.Attempts) != 1 {
		t.Fatalf("④ attempt 数 = %d（want 1；断开不换上游）", len(rec4.Attempts))
	}
	if a := rec4.Attempts[0]; a.EndReason != endReasonClientCancel || a.Delivered || a.Failed || !a.Disconnected {
		t.Fatalf("④ 断开归因 = %+v（want client_cancel + delivered=false + disconnected）", a)
	}
	logBuf.Reset()

	// ⑤ 提交后截断（responses 上游：delta 后 EOF、无终止事件）——错误帧不是交付。
	upTrunc := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "response.created", `{"type":"response.created","response":{"id":"resp_t","object":"response","created_at":1,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":0}`)
		sseFrame(w, "response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_t","output_index":0,"content_index":0,"delta":"Hi","sequence_number":1}`)
	})
	_, srv5 := startServiceWith(t, cfgFor(upTrunc.srv.URL, ProtocolResponses, 15))
	resp5 := postJSON(t, srv5.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","stream":true}`, nil)
	raw5, _ := io.ReadAll(resp5.Body)
	resp5.Body.Close()
	if resp5.StatusCode != 200 || !strings.Contains(string(raw5), "response.failed") {
		t.Fatalf("⑤ 截断收尾缺失败帧: status=%d\n%s", resp5.StatusCode, raw5)
	}
	rec5 := parseRequestRecord(t, logBuf.String())
	if len(rec5.Attempts) != 1 {
		t.Fatalf("⑤ attempt 数 = %d（want 1）", len(rec5.Attempts))
	}
	if a := rec5.Attempts[0]; a.EndReason != endReasonUpstreamError || a.Delivered || !a.Failed {
		t.Fatalf("⑤ 截断归因 = %+v（want upstream_error + delivered=false + failed）", a)
	}
	logBuf.Reset()

	// ⑥ 本地失败（未达上游）：gemini 上游 + 非法模型名（vertex 全路径）→ 400，
	// end_reason 省略（不伪造枚举值）、上游零调用。
	upNever := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("⑥ 上游不该被调用")
	})
	_, srv6 := startServiceWith(t, cfgFor(upNever.srv.URL, ProtocolGemini, 15))
	resp6 := postJSON(t, srv6.URL+"/v1/chat/completions",
		`{"model":"projects/x/models/y","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	raw6, _ := io.ReadAll(resp6.Body)
	resp6.Body.Close()
	if resp6.StatusCode != 400 {
		t.Fatalf("⑥ status = %d（want 400 本地拒绝）\n%s", resp6.StatusCode, raw6)
	}
	if upNever.count() != 0 {
		t.Fatalf("⑥ 上游命中 = %d（want 0——未达上游）", upNever.count())
	}
	rec6 := parseRequestRecord(t, logBuf.String())
	if len(rec6.Attempts) != 1 {
		t.Fatalf("⑥ attempt 数 = %d（want 1）", len(rec6.Attempts))
	}
	if a := rec6.Attempts[0]; a.EndReason != "" || a.Delivered || a.Failed {
		t.Fatalf("⑥ 本地失败 = %+v（want end_reason 省略 + delivered=false + failed=false）", a)
	}
}

// TestW36EndReasonIdle S1 idle 窄化（A7）：① 提交后挂住（keepalive 不算内容）→
// upstream_idle；② 提交前零字节挂住 → upstream_idle + 换上游（第二台 completed）。
func TestW36EndReasonIdle(t *testing.T) {
	var logBuf syncLogBuffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	// ① 提交后 idle（内容帧后只发 ping）。
	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		claudeIdleStreamHead(w)
		tk := time.NewTicker(200 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-tk.C:
				sseFrame(w, "ping", `{"type":"ping"}`)
			}
		}
	})
	cfg := failoverCfg(anthUp("pingy", up.srv.URL))
	cfg.UpstreamIdleTimeoutSeconds = 1
	_, srv := startServiceWith(t, cfg)
	if status, raw := postClaudeStreamC(t, srv.URL, &http.Client{Timeout: 10 * time.Second}); status != 200 ||
		!strings.Contains(raw, `"incomplete_stream"`) {
		t.Fatalf("① 下游收尾异常: status=%d\n%s", status, raw)
	}
	rec := parseRequestRecord(t, logBuf.String())
	if len(rec.Attempts) != 1 {
		t.Fatalf("① attempt 数 = %d（want 1）", len(rec.Attempts))
	}
	if a := rec.Attempts[0]; a.EndReason != endReasonUpstreamIdle || a.Delivered || !a.Failed {
		t.Fatalf("① idle 归因 = %+v（want upstream_idle + delivered=false）", a)
	}
	logBuf.Reset()

	// ② 提交前零字节挂住（响应头已到、无产出）→ 换上游。
	upHang := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	})
	upGood := goodClaudeUpstream(t)
	cfg2 := failoverCfg(anthUp("hang", upHang.srv.URL), anthUp("good", upGood.srv.URL))
	cfg2.UpstreamIdleTimeoutSeconds = 1
	_, srv2 := startServiceWith(t, cfg2)
	if status, raw := postClaudeStreamC(t, srv2.URL, &http.Client{Timeout: 10 * time.Second}); status != 200 ||
		!strings.Contains(raw, "Hello ") || !strings.Contains(raw, "message_stop") {
		t.Fatalf("② 下游应是第二台的完整流: status=%d\n%s", status, raw)
	}
	rec2 := parseRequestRecord(t, logBuf.String())
	if len(rec2.Attempts) != 2 {
		t.Fatalf("② attempt 数 = %d（want 2）: %+v", len(rec2.Attempts), rec2.Attempts)
	}
	if a := rec2.Attempts[0]; a.EndReason != endReasonUpstreamIdle || a.Delivered {
		t.Fatalf("② 挂住尝试 = %+v（want upstream_idle + delivered=false）", a)
	}
	if a := rec2.Attempts[1]; a.EndReason != endReasonCompleted || !a.Delivered {
		t.Fatalf("② 第二台尝试 = %+v（want completed + delivered）", a)
	}
}

// TestW36EndReasonTaskCancel S1 任务面取消归因（A8）：任务 cancel → 该任务的
// per-request attempt 行 end_reason=task_cancel（不是 downstream_disconnect）；
// 既有字段纪律不动（disconnected=true / failed=false）；任务面 attempt 行恒
// delivered=false（交付口径在交付点——见 TestW36TaskFaceDelivery）。
func TestW36EndReasonTaskCancel(t *testing.T) {
	var logBuf syncLogBuffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	up, release := w36GatedUpstream(t)
	srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))
	// 释放兜底须在服务 Close 之前（t.Cleanup LIFO：晚注册先跑；httptest.Server.Close
	// 等所有在飞请求完成——挂住的任务请求会让 Close 死等）。
	t.Cleanup(release)

	id := w36CreateTask(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"gate","background":true}`, nil)
	// 等任务真打到上游（避免 cancel 落在 goroutine 启动前）。
	waitFor(t, 5*time.Second, func() bool { return up.count() == 1 })

	resp := postJSON(t, srv.URL+"/v1/responses/"+id+"/cancel", "", nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("cancel status = %d\n%s", resp.StatusCode, raw)
	}
	var cancelled map[string]json.RawMessage
	if err := json.Unmarshal(raw, &cancelled); err != nil || string(cancelled["status"]) != `"cancelled"` {
		t.Fatalf("cancel 快照 = %s（want cancelled）", raw)
	}

	rr := parseTaskRequestRecord(t, logBuf.String(), id)
	if len(rr.Attempts) != 1 {
		t.Fatalf("attempt 数 = %d（want 1）: %+v", len(rr.Attempts), rr.Attempts)
	}
	a := rr.Attempts[0]
	if a.EndReason != endReasonTaskCancel {
		t.Fatalf("任务 cancel 归因 = %q（want %q——A8：不得记成 downstream_disconnect）", a.EndReason, endReasonTaskCancel)
	}
	if a.Failed || !a.Disconnected {
		t.Fatalf("既有字段纪律被破坏: %+v（want failed=false disconnected=true）", a)
	}
	if a.Delivered {
		t.Fatalf("任务面 attempt 行不得记 delivered（交付口径在交付点）: %+v", a)
	}
}

// ---------------------------------------------------------------------------
// S2 任务面交付
// ---------------------------------------------------------------------------

// TestW36TaskFaceDelivery S2 任务面：① 终态 retrieve 写出成功 = 交付（via=retrieve，
// 幂等一行）；② 续流订阅者收到终态事件 = 交付（via=resume）；③ 反证：未交付的
// 任务无交付行；游标越过终态事件的订阅者不算交付；运行中快照不落交付。
func TestW36TaskFaceDelivery(t *testing.T) {
	var logBuf syncLogBuffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	up, release := w36GatedUpstream(t)
	s, srv := startServiceWith(t, cfgFor(up.srv.URL, ProtocolResponses, 15))
	// 释放兜底须在服务 Close 之前（t.Cleanup LIFO；httptest.Server.Close 等所有
	// 在飞请求完成——本测试末尾留了一个运行中的挂住任务）。
	t.Cleanup(release)

	// ① retrieve 交付。
	idA := w36CreateTask(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","background":true}`, nil)
	if st := w36WaitTerminal(t, s, idA); st != taskCompleted {
		t.Fatalf("任务 A 终态 = %q（want completed）", st)
	}
	teA, _ := s.tasks.get(idA)
	if teA == nil {
		t.Fatalf("任务 A 未在 store")
	}
	if got, via := teA.deliveryState(); got || via != "" {
		t.Fatalf("未 retrieve 前交付位 = %v/%q（want false/空——反证：不得提前落）", got, via)
	}
	if lines := w36DeliveredLines(logBuf.String(), idA); len(lines) != 0 {
		t.Fatalf("未 retrieve 前不得有交付行: %v", lines)
	}
	if code, body := getJSON(t, srv.URL+"/v1/responses/"+idA); code != 200 || !bytes.Contains(body, []byte(`"completed"`)) {
		t.Fatalf("retrieve A: status=%d\n%s", code, body)
	}
	if got, via := teA.deliveryState(); !got || via != "retrieve" {
		t.Fatalf("retrieve 后交付位 = %v/%q（want true/retrieve）", got, via)
	}
	if rec := w36ParseDelivered(t, logBuf.String(), idA); rec.Via != "retrieve" || rec.Status != taskCompleted || rec.Task != idA {
		t.Fatalf("交付行 A = %+v", rec)
	}
	// 幂等：再 retrieve 一次，仍只有一行。
	if code, _ := getJSON(t, srv.URL+"/v1/responses/"+idA); code != 200 {
		t.Fatalf("第二次 retrieve A status = %d", code)
	}
	if lines := w36DeliveredLines(logBuf.String(), idA); len(lines) != 1 {
		t.Fatalf("重复 retrieve 应幂等（交付行 = %d，want 1）", len(lines))
	}

	// ② resume 交付（从 0 重放，含终态事件）。
	idB := w36CreateTask(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi2","background":true}`, nil)
	if st := w36WaitTerminal(t, s, idB); st != taskCompleted {
		t.Fatalf("任务 B 终态 = %q（want completed）", st)
	}
	teB, _ := s.tasks.get(idB)
	body := w36Resume(t, srv.URL, idB, 0)
	if !strings.Contains(body, "response.completed") {
		t.Fatalf("续流重放缺终态事件:\n%s", body)
	}
	if got, via := teB.deliveryState(); !got || via != "resume" {
		t.Fatalf("resume 后交付位 = %v/%q（want true/resume）", got, via)
	}
	if rec := w36ParseDelivered(t, logBuf.String(), idB); rec.Via != "resume" || rec.Status != taskCompleted {
		t.Fatalf("交付行 B = %+v", rec)
	}

	// ③ 反证：游标越过终态事件的订阅者不算交付（终态事件 seq = 2）。
	idC := w36CreateTask(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi3","background":true}`, nil)
	if st := w36WaitTerminal(t, s, idC); st != taskCompleted {
		t.Fatalf("任务 C 终态 = %q（want completed）", st)
	}
	teC, _ := s.tasks.get(idC)
	_ = w36Resume(t, srv.URL, idC, 9999)
	if got, via := teC.deliveryState(); got {
		t.Fatalf("越过终态事件的订阅者不得落交付（got %v/%q）", got, via)
	}
	if lines := w36DeliveredLines(logBuf.String(), idC); len(lines) != 0 {
		t.Fatalf("越过终态事件的订阅者不得写交付行: %v", lines)
	}
	// 反证（续）：运行中快照不落交付——建一个挂住任务，retrieve 运行中快照。
	idD := w36CreateTask(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"gate","background":true}`, nil)
	if code, body := getJSON(t, srv.URL+"/v1/responses/"+idD); code != 200 || !bytes.Contains(body, []byte(`"in_progress"`)) {
		t.Fatalf("运行中快照: status=%d\n%s", code, body)
	}
	teD, _ := s.tasks.get(idD)
	if got, _ := teD.deliveryState(); got {
		t.Fatalf("运行中快照不得落交付位（终态才有交付口径）")
	}
	if lines := w36DeliveredLines(logBuf.String(), idD); len(lines) != 0 {
		t.Fatalf("运行中快照不得写交付行: %v", lines)
	}
}

// ---------------------------------------------------------------------------
// S6 任务终态 ring
// ---------------------------------------------------------------------------

// TestW36TaskTerminalRing S6：任务终态落 ring（task_terminal；载荷
// {task,status,attempts,duration_ms}）；覆盖 completed / failed（提交后流内失败）/
// cancelled 三终态；反证：运行中任务不得有该事件。
func TestW36TaskTerminalRing(t *testing.T) {
	up, release := w36GatedUpstream(t)
	s, srv := startServiceWith(t, cfgFor(up.srv.URL, ProtocolResponses, 15))
	// 释放兜底须在服务 Close 之前（t.Cleanup LIFO；httptest.Server.Close 等所有
	// 在飞请求完成——失败早退时挂住的任务请求会让 Close 死等）。
	t.Cleanup(release)

	// ① 运行中不落（反证）→ cancel 后落 cancelled。
	idA := w36CreateTask(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"gate","background":true}`, nil)
	waitFor(t, 5*time.Second, func() bool { return up.count() == 1 })
	if e := w36RingTerminal(s, idA); e != nil {
		t.Fatalf("运行中任务不得有 task_terminal 事件: %+v", e)
	}
	respA := postJSON(t, srv.URL+"/v1/responses/"+idA+"/cancel", "", nil)
	_, _ = io.ReadAll(respA.Body)
	respA.Body.Close()
	if respA.StatusCode != 200 {
		t.Fatalf("① cancel status = %d", respA.StatusCode)
	}
	if _, pA := w36ParseRingTerminal(t, s, idA); pA.Status != taskCancelled {
		t.Fatalf("① task_terminal 载荷 = %+v（want cancelled）", pA)
	}

	// ② completed（放行闸门）。
	idB := w36CreateTask(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"gate","background":true}`, nil)
	waitFor(t, 5*time.Second, func() bool { return up.count() == 2 })
	release()
	waitTaskTerminal(t, srv.URL, idB)
	eB, pB := w36ParseRingTerminal(t, s, idB)
	if pB.Status != taskCompleted || pB.Attempts != 1 || pB.DurationMs < 0 {
		t.Fatalf("② task_terminal 载荷 = %+v（want completed/attempts=1）", pB)
	}
	if eB.RequestID != idB || eB.Upstream != "fake" {
		t.Fatalf("② ring 行关联面 = %+v（want request_id=任务 id + upstream 名 fake）", eB)
	}
	if eB.Seq == 0 || eB.Time.IsZero() {
		t.Fatalf("② ring 行元信息缺失: %+v", eB)
	}

	// ③ failed（提交后流内失败：delta 后 EOF、无终止证据）。
	upCut := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "response.created", `{"type":"response.created","response":{"id":"resp_cut","object":"response","created_at":1,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":0}`)
		sseFrame(w, "response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_cut","output_index":0,"content_index":0,"delta":"Hi","sequence_number":1}`)
	})
	sCut, srvCut := startServiceWith(t, cfgFor(upCut.srv.URL, ProtocolResponses, 15))
	idC := w36CreateTask(t, srvCut.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","background":true}`, nil)
	finalC := waitTaskTerminal(t, srvCut.URL, idC)
	if string(finalC["status"]) != `"failed"` {
		t.Fatalf("③ 截断任务终态 = %s（want failed）", finalC["status"])
	}
	if _, pC := w36ParseRingTerminal(t, sCut, idC); pC.Status != taskFailed || pC.Attempts != 1 {
		t.Fatalf("③ task_terminal 载荷 = %+v（want failed/attempts=1）", pC)
	}
}

// ---------------------------------------------------------------------------
// S7 任务面计数 + admin 契约
// ---------------------------------------------------------------------------

// TestW36TaskDiagnosticsCounters S7：admin JSON 契约（六个新字段 snake_case 在
// /admin/diagnostics 线上）+ 计数语义（创建/终态四值/幂等重放）；反证：幂等重放
// 不增 tasks_created、同步请求不增任何 tasks_* 计数。
func TestW36TaskDiagnosticsCounters(t *testing.T) {
	up, release := w36GatedUpstream(t)
	_, srv := startServiceWith(t, adminCfg("secret-key", Upstream{
		Name: "upstream", BaseURL: up.srv.URL, APIKey: "k", Protocol: ProtocolResponses,
	}))
	// 释放兜底须在服务 Close 之前（t.Cleanup LIFO；httptest.Server.Close 等所有
	// 在飞请求完成——失败早退时挂住的任务请求会让 Close 死等）。
	t.Cleanup(release)

	// admin 契约：字段存在且初值 0（snake_case 线上名）。
	raw := adminBody(t, adminReq(t, http.MethodGet, srv.URL+"/admin/diagnostics", "secret-key", ""))
	for _, field := range []string{
		`"tasks_created"`, `"tasks_terminal_completed"`, `"tasks_terminal_failed"`,
		`"tasks_terminal_incomplete"`, `"tasks_terminal_cancelled"`, `"tasks_idem_replays"`,
	} {
		if !strings.Contains(raw, field) {
			t.Fatalf("诊断响应缺字段 %s:\n%s", field, raw)
		}
	}
	if d := adminDiagnostics(t, srv.URL, "secret-key").Diagnostics; d.TasksCreated != 0 ||
		d.TasksTerminalCompleted != 0 || d.TasksIdemReplays != 0 {
		t.Fatalf("初值应为 0: %+v", d)
	}

	// ① completed 任务 + ② 幂等重放（重放不建新任务）+ ③ cancel 任务。
	idA := w36CreateTask(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","background":true}`, nil)
	waitTaskTerminal(t, srv.URL, idA)

	key := map[string]string{"Idempotency-Key": "w36-k1"}
	body := `{"model":"gpt-5","input":"gate","background":true}`
	idB := w36CreateTask(t, srv.URL+"/v1/responses", body, key)
	idB2 := w36CreateTask(t, srv.URL+"/v1/responses", body, key)
	if idB2 != idB {
		t.Fatalf("幂等重放任务 id = %s（want %s）", idB2, idB)
	}
	waitFor(t, 5*time.Second, func() bool { return up.count() == 2 })
	respC := postJSON(t, srv.URL+"/v1/responses/"+idB+"/cancel", "", nil)
	_, _ = io.ReadAll(respC.Body)
	respC.Body.Close()
	release()

	d := adminDiagnostics(t, srv.URL, "secret-key").Diagnostics
	if d.TasksCreated != 2 {
		t.Fatalf("tasks_created = %d（want 2——幂等重放不得计新建）", d.TasksCreated)
	}
	if d.TasksIdemReplays != 1 {
		t.Fatalf("tasks_idem_replays = %d（want 1）", d.TasksIdemReplays)
	}
	if d.TasksTerminalCompleted != 1 || d.TasksTerminalCancelled != 1 ||
		d.TasksTerminalFailed != 0 || d.TasksTerminalIncomplete != 0 {
		t.Fatalf("终态计数 = completed %d / failed %d / incomplete %d / cancelled %d（want 1/0/0/1）",
			d.TasksTerminalCompleted, d.TasksTerminalFailed, d.TasksTerminalIncomplete, d.TasksTerminalCancelled)
	}

	// 反证：同步请求（非 background）不增任何 tasks_* 计数（任务面口径不外溢）。
	before := adminDiagnostics(t, srv.URL, "secret-key").Diagnostics
	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","stream":true}`, nil)
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("同步请求 status = %d", resp.StatusCode)
	}
	after := adminDiagnostics(t, srv.URL, "secret-key").Diagnostics
	if after.TasksCreated != before.TasksCreated || after.TasksTerminalCompleted != before.TasksTerminalCompleted ||
		after.TasksIdemReplays != before.TasksIdemReplays {
		t.Fatalf("同步请求污染任务面计数: before=%+v after=%+v", before, after)
	}
	if after.Attempts <= before.Attempts {
		t.Fatalf("同步请求应增 attempts（对照面）：before=%d after=%d", before.Attempts, after.Attempts)
	}
}
