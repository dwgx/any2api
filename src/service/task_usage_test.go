// W20-1（A2A-L19-W20-1）：任务面 Slice 4 验收测试（终态 usage 落账 + 幂等键）。
//
// 覆盖（提案 `service-task-surface` §实施拆分 Slice 4 验收原文）：
//   - TestTaskFaceUsage（验收主测）：
//     ① 终态 usage 与假上游逐值相等 + 进记账——TaskUsage* 计数逐值；每任务一条
//     per-task 记账行（{task,status,in,out}）；任务归因——任务 goroutine 的
//     per-request attempt 行带 `task` 字段且 attempts[].in/out 与上游逐值一致
//     （background 任务在既有 attempts 面的归因）；
//     ② 幂等键同键同体重提 → 同任务 id（1 任务、2 请求、上游只被调一次；重提在
//     任务运行中发出——gate 拦尾帧——返回运行中快照）；
//     ③ 键冲突（同键不同体）→ 409 idempotency_key_conflict（不建任务、不触上游）；
//     ④ 头优先（父代裁决 #4）：体 `idempotency_key` 字段可单独作键；头键 > 体键
//     （头覆盖时体键不入表——同体无头重提 = 新任务，证明头胜）；
//     ⑤ failed 任务无终态 usage → TaskUsage* 不增（下界在 per-attempt 面），
//     per-task 行仍写（status=failed、in=out=0）。
//   - TestTaskIdemKeyExtract：键提取单元面（头 > 体；trim；非字符串/空容错）。
//   - TestTaskStoreIdemLifecycle：store 单元面（命中/冲突/过期墓碑化键回收）。
//
// 反证（回退即红；原样输出见 tmp/lanes/W20-taskface4/proofs/）：
// CP1 去掉 runTask 的 recordTaskUsage 调用 → ① TaskUsage* 计数断言红；
// CP2 幂等命中路径恒新建 → ② 同 id 断言红；
// CP3 冲突分支当重放（跳过内容指纹比较）→ ③ 409 断言红。
package service

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// taskUsageRecordWire per-task 记账行载荷（attempts.go recordTaskUsage 的线上形状）。
type taskUsageRecordWire struct {
	Task   string `json:"task"`
	Entry  string `json:"entry"`
	Model  string `json:"model"`
	Status string `json:"status"`
	In     int64  `json:"in"`
	Out    int64  `json:"out"`
}

// taskRequestRecordWire per-request attempt 行（含 Slice 4 的 task 归因字段）。
type taskRequestRecordWire struct {
	Req      string              `json:"req"`
	Entry    string              `json:"entry"`
	Model    string              `json:"model"`
	Stream   bool                `json:"stream"`
	Task     string              `json:"task"`
	Attempts []attemptRecordWire `json:"attempts"`
}

// createTask 发任务创建 POST（可带 Idempotency-Key 头）；返回状态码 + 快照 map。
func createTask(t *testing.T, url, body string, headers map[string]string) (int, map[string]json.RawMessage) {
	t.Helper()
	resp := postJSON(t, url, body, headers)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("create body: %v\n%s", err, raw)
	}
	return resp.StatusCode, m
}

// jsonString 取快照字段的字符串值。
func jsonString(t *testing.T, m map[string]json.RawMessage, key string) string {
	t.Helper()
	var s string
	if err := json.Unmarshal(m[key], &s); err != nil {
		t.Fatalf("字段 %s 不是字符串: %s", key, m[key])
	}
	return s
}

// waitTaskTerminal 轮询到任务终态（任一）；返回终态快照。
func waitTaskTerminal(t *testing.T, url, id string) map[string]json.RawMessage {
	t.Helper()
	var final map[string]json.RawMessage
	waitFor(t, 5*time.Second, func() bool {
		code, b := getJSON(t, url+"/v1/responses/"+id)
		if code != 200 {
			return false
		}
		var m map[string]json.RawMessage
		if json.Unmarshal(b, &m) != nil {
			return false
		}
		switch string(m["status"]) {
		case `"completed"`, `"failed"`, `"incomplete"`, `"cancelled"`:
			final = m
			return true
		}
		return false
	})
	if final == nil {
		t.Fatalf("任务 %s 未收敛到终态", id)
	}
	return final
}

// taskCount store 存活任务数（测试面）。
func taskCount(s *Server) int {
	s.tasks.mu.Lock()
	defer s.tasks.mu.Unlock()
	return len(s.tasks.tasks)
}

// parseTaskUsageRecord 从日志里取指定任务 id 的 per-task 记账行并解析。
func parseTaskUsageRecord(t *testing.T, logs, taskID string) taskUsageRecordWire {
	t.Helper()
	marker := "any2api: task " + taskID + " terminal usage: "
	i := strings.LastIndex(logs, marker)
	if i < 0 {
		t.Fatalf("任务 %s 的 per-task 记账行缺失:\n%s", taskID, logs)
	}
	line := logs[i+len(marker):]
	if j := strings.IndexByte(line, '\n'); j >= 0 {
		line = line[:j]
	}
	var rec taskUsageRecordWire
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("per-task 记账 JSON 解析失败: %v\n%s", err, line)
	}
	return rec
}

// parseTaskRequestRecord 从日志里取指定任务 id 的 per-request attempt 行
// （requestRecorder 的 `task` 归因面）。
func parseTaskRequestRecord(t *testing.T, logs, taskID string) taskRequestRecordWire {
	t.Helper()
	const marker = "attempts: "
	for _, line := range strings.Split(logs, "\n") {
		k := strings.Index(line, "any2api: request ")
		if k < 0 {
			continue
		}
		j := strings.Index(line, marker)
		if j < 0 {
			continue
		}
		var rec taskRequestRecordWire
		if json.Unmarshal([]byte(line[j+len(marker):]), &rec) != nil {
			continue
		}
		if rec.Task == taskID {
			return rec
		}
	}
	t.Fatalf("任务 %s 的 per-request 记账行缺失（task 归因）:\n%s", taskID, logs)
	return taskRequestRecordWire{}
}

// TestTaskFaceUsage 任务面 Slice 4 验收主测（见文件头注）。
func TestTaskFaceUsage(t *testing.T) {
	var logBuf syncLogBuffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	// 假上游：任务面流式——头帧立即写，尾帧（delta + completed 带 usage）过门后
	// 写。门（nextGate）只拦测试指定的那一次调用（②「运行中重提」面）；input 含
	// "boom" → 500 信封（⑤ failed 任务面）。
	var upCalls atomic.Int64
	var gateMu sync.Mutex
	var nextGate chan struct{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		upCalls.Add(1)
		gateMu.Lock()
		g := nextGate
		nextGate = nil
		gateMu.Unlock()
		if bytes.Contains(b, []byte("boom")) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"message":"boom","type":"server_error"}}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "response.created", `{"type":"response.created","response":{"id":"resp_usage","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":0}`)
		sseFrame(w, "response.output_item.added", `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_usage","type":"message","status":"in_progress","role":"assistant","content":[]},"sequence_number":1}`)
		if g != nil {
			<-g
		}
		sseFrame(w, "response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_usage","output_index":0,"content_index":0,"delta":"Hello world","sequence_number":2,"logprobs":[]}`)
		sseFrame(w, "response.output_item.done", `{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_usage","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Hello world","annotations":[]}]},"sequence_number":3}`)
		sseFrame(w, "response.completed", `{"type":"response.completed","response":{"id":"resp_usage","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5","output":[{"id":"msg_usage","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Hello world","annotations":[]}]}],"usage":`+taskSlowUsageJSON+`,"incomplete_details":null},"sequence_number":4}`)
	}))
	t.Cleanup(up.Close)
	s, srv := startServiceWith(t, cfgFor(up.URL, ProtocolResponses, 15))

	// ① 终态 usage 与假上游逐值相等 + 进记账（TaskUsage* + per-task 行 + task 归因）。
	code, mA := createTask(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","background":true}`, nil)
	if code != 200 {
		t.Fatalf("A create status = %d", code)
	}
	idA := jsonString(t, mA, "id")
	finalA := waitTaskTerminal(t, srv.URL, idA)
	if string(finalA["status"]) != `"completed"` {
		t.Fatalf("task A status = %s", finalA["status"])
	}
	// 终态 usage 逐值：与假上游 completed 帧（taskSlowUsageJSON）一致。
	var wantUsage, gotUsage any
	if err := json.Unmarshal([]byte(taskSlowUsageJSON), &wantUsage); err != nil {
		t.Fatalf("want usage: %v", err)
	}
	if err := json.Unmarshal(finalA["usage"], &gotUsage); err != nil {
		t.Fatalf("terminal usage: %v\n%s", err, finalA["usage"])
	}
	if !reflect.DeepEqual(wantUsage, gotUsage) {
		t.Fatalf("终态 usage = %s\nwant %s", finalA["usage"], taskSlowUsageJSON)
	}
	// 记账面：TaskUsage* 计数逐值（终态 usage 进记账——网关跑到终态恒落账）。
	waitFor(t, 2*time.Second, func() bool { return s.Diagnostics().TaskUsageRecords == 1 })
	if dg := s.Diagnostics(); dg.TaskUsageInTokens != 9 || dg.TaskUsageOutTokens != 4 {
		t.Fatalf("TaskUsage = recs=%d in=%d out=%d（want 1/9/4）", dg.TaskUsageRecords, dg.TaskUsageInTokens, dg.TaskUsageOutTokens)
	}
	// per-task 记账行（每任务一条；值与上游逐值）。
	recA := parseTaskUsageRecord(t, logBuf.String(), idA)
	if recA.Task != idA || recA.Entry != "openai_responses" || recA.Model != "gpt-5" ||
		recA.Status != "completed" || recA.In != 9 || recA.Out != 4 {
		t.Fatalf("per-task 记账行 = %+v", recA)
	}
	// 任务归因：per-request attempt 行带 task 字段，attempts[].in/out 与上游一致。
	rrA := parseTaskRequestRecord(t, logBuf.String(), idA)
	if len(rrA.Attempts) != 1 || rrA.Attempts[0].In != 9 || rrA.Attempts[0].Out != 4 || rrA.Attempts[0].Failed {
		t.Fatalf("任务 A 的 per-request 行 = %+v", rrA)
	}

	// ② 幂等键：同键同体重提 → 同任务 id（1 任务、2 请求、上游只被调一次）。
	gateMu.Lock()
	nextGate = make(chan struct{})
	gate := nextGate
	gateMu.Unlock()
	// 失败路径防悬挂：测试提前 Fatalf 时 gate 未关 → 上游 handler 悬在 <-g →
	// httptest Close 阻塞（测试进程挂死）。defer 兜底关闭（已关则跳过）。
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}()
	beforeTasks := taskCount(s)
	beforeReqs := s.Diagnostics().Requests
	codeB1, mB1 := createTask(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi2","background":true}`,
		map[string]string{"Idempotency-Key": "key-b1"})
	if codeB1 != 200 {
		t.Fatalf("B1 create status = %d", codeB1)
	}
	idB := jsonString(t, mB1, "id")
	waitFor(t, 2*time.Second, func() bool { return upCalls.Load() == 2 }) // B 的上游调用已开始（被 gate 拦在尾帧）
	codeB2, mB2 := createTask(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi2","background":true}`,
		map[string]string{"Idempotency-Key": "key-b1"})
	if codeB2 != 200 {
		t.Fatalf("B2 replay status = %d", codeB2)
	}
	if idB2 := jsonString(t, mB2, "id"); idB2 != idB {
		t.Fatalf("幂等命中 id = %s（want %s——同键同体重提必须返回同任务 id）", idB2, idB)
	}
	if st := string(mB2["status"]); st != `"queued"` && st != `"in_progress"` {
		t.Fatalf("重提快照 status = %s（want 运行中——gate 未放行）", st)
	}
	if got := upCalls.Load(); got != 2 {
		t.Fatalf("上游调用数 = %d（want 2——重提绝不触上游）", got)
	}
	if got := taskCount(s); got != beforeTasks+1 {
		t.Fatalf("任务数 = %d（want %d——1 任务 2 请求）", got, beforeTasks+1)
	}
	if got := s.Diagnostics().Requests - beforeReqs; got != 2 {
		t.Fatalf("请求计数增量 = %d（want 2）", got)
	}
	close(gate) // 放行尾帧 → B 收敛（其 usage 进终值断言）

	// ③ 键冲突：同键不同体 → 409（不建任务、不触上游）。
	codeC1, mC1 := createTask(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi3","background":true}`,
		map[string]string{"Idempotency-Key": "key-c1"})
	if codeC1 != 200 {
		t.Fatalf("C1 create status = %d", codeC1)
	}
	idC := jsonString(t, mC1, "id")
	waitFor(t, 2*time.Second, func() bool { return upCalls.Load() == 3 }) // C 的上游调用已开始
	beforeTasks = taskCount(s)
	beforeCalls := upCalls.Load()
	respC2 := postJSON(t, srv.URL+"/v1/responses",
		`{"model":"gpt-5","input":"DIFFERENT-BODY","background":true}`,
		map[string]string{"Idempotency-Key": "key-c1"})
	rawC2, _ := io.ReadAll(respC2.Body)
	respC2.Body.Close()
	if respC2.StatusCode != 409 {
		t.Fatalf("冲突 status = %d（want 409）\n%s", respC2.StatusCode, rawC2)
	}
	if got := taskErrorCode(t, rawC2); got != "idempotency_key_conflict" {
		t.Fatalf("冲突 code = %q", got)
	}
	if got := taskCount(s); got != beforeTasks {
		t.Fatalf("冲突后任务数 = %d（want %d——绝不建第二任务）", got, beforeTasks)
	}
	if got := upCalls.Load(); got != beforeCalls {
		t.Fatalf("冲突后上游调用数 = %d（want %d——绝不触上游）", got, beforeCalls)
	}

	// ④ 头优先：体字段键单独可作键；头键 > 体键。
	bodyD := `{"model":"gpt-5","input":"hi4","background":true,"idempotency_key":"d1"}`
	codeD1, mD1 := createTask(t, srv.URL+"/v1/responses", bodyD, nil)
	if codeD1 != 200 {
		t.Fatalf("D1 create status = %d", codeD1)
	}
	idD := jsonString(t, mD1, "id")
	codeD2, mD2 := createTask(t, srv.URL+"/v1/responses", bodyD, map[string]string{"Idempotency-Key": "d1"})
	if codeD2 != 200 || jsonString(t, mD2, "id") != idD {
		t.Fatalf("体键重提 = %d %s（want 同 id %s）", codeD2, mD2["id"], idD)
	}
	bodyE := `{"model":"gpt-5","input":"hi5","background":true,"idempotency_key":"e-body"}`
	codeE1, mE1 := createTask(t, srv.URL+"/v1/responses", bodyE, map[string]string{"Idempotency-Key": "e-head"})
	if codeE1 != 200 {
		t.Fatalf("E1 create status = %d", codeE1)
	}
	idE := jsonString(t, mE1, "id")
	codeE2, mE2 := createTask(t, srv.URL+"/v1/responses", bodyE, map[string]string{"Idempotency-Key": "e-head"})
	if codeE2 != 200 || jsonString(t, mE2, "id") != idE {
		t.Fatalf("头键重提 = %d %s（want 同 id %s）", codeE2, mE2["id"], idE)
	}
	// 头键覆盖证明：同体无头（体键 e-body）——e-body 从未入表 → 新任务（头胜）。
	codeF, mF := createTask(t, srv.URL+"/v1/responses", bodyE, nil)
	if codeF != 200 {
		t.Fatalf("F create status = %d", codeF)
	}
	idF := jsonString(t, mF, "id")
	if idF == idE {
		t.Fatalf("头优先失效：体键 e-body 命中了头键 e-head 的任务（id=%s）", idF)
	}

	// 终值：全部任务收敛；TaskUsage* 累计 = 6 任务 × (9,4)（每任务一条）。
	for _, id := range []string{idA, idB, idC, idD, idE, idF} {
		if f := waitTaskTerminal(t, srv.URL, id); string(f["status"]) != `"completed"` {
			t.Fatalf("task %s status = %s", id, f["status"])
		}
	}
	waitFor(t, 2*time.Second, func() bool { return s.Diagnostics().TaskUsageRecords == 6 })
	if dg := s.Diagnostics(); dg.TaskUsageInTokens != 54 || dg.TaskUsageOutTokens != 24 {
		t.Fatalf("TaskUsage 终值 = recs=%d in=%d out=%d（want 6/54/24）", dg.TaskUsageRecords, dg.TaskUsageInTokens, dg.TaskUsageOutTokens)
	}

	// ⑤ failed 任务：无终态 usage → TaskUsage* 不增（下界在 per-attempt 面）；
	// per-task 行仍写（每任务一条；status=failed、in=out=0）。
	codeG, mG := createTask(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"boom","background":true}`, nil)
	if codeG != 200 {
		t.Fatalf("G create status = %d", codeG)
	}
	idG := jsonString(t, mG, "id")
	if f := waitTaskTerminal(t, srv.URL, idG); string(f["status"]) != `"failed"` {
		t.Fatalf("task G status = %s（want failed）", f["status"])
	}
	waitFor(t, 2*time.Second, func() bool {
		return strings.Contains(logBuf.String(), "any2api: task "+idG+" terminal usage: ")
	})
	recG := parseTaskUsageRecord(t, logBuf.String(), idG)
	if recG.Status != "failed" || recG.In != 0 || recG.Out != 0 {
		t.Fatalf("failed 任务记账行 = %+v", recG)
	}
	if dg := s.Diagnostics(); dg.TaskUsageRecords != 6 {
		t.Fatalf("failed 任务不应计入 TaskUsage*：recs=%d（want 6——下界在 AttemptUsage*）", dg.TaskUsageRecords)
	}
}

// TestTaskStoreIdemLifecycle 幂等键生命周期（store 单元面）：同键同体命中 /
// 同键不同体冲突 / 过期墓碑化键回收（同键重提 = 新任务）/ 无键路径不受影响。
func TestTaskStoreIdemLifecycle(t *testing.T) {
	st := newTaskStore()
	cur := time.Unix(1_700_000_000, 0)
	st.now = func() time.Time { return cur }
	body := []byte(`{"model":"m"}`)
	te1, replay, conflict, err := st.createIdem("t1", "m", "k1", body)
	if err != nil || replay || conflict {
		t.Fatalf("createIdem = %v %v %v %v", te1, replay, conflict, err)
	}
	// 同键同体 → 命中同任务（replay；不建第二任务）。
	te2, replay, conflict, err := st.createIdem("t2", "m", "k1", body)
	if err != nil || !replay || conflict || te2 != te1 {
		t.Fatalf("同键同体 = %v %v %v %v（want 命中 t1）", te2, replay, conflict, err)
	}
	if len(st.tasks) != 1 || len(st.idem) != 1 {
		t.Fatalf("命中后 store = tasks=%d idem=%d（want 1/1）", len(st.tasks), len(st.idem))
	}
	// 同键不同体 → 冲突（不建任务、不触 store）。
	te3, replay, conflict, err := st.createIdem("t3", "m", "k1", []byte(`{"model":"m","input":"x"}`))
	if err != nil || replay || !conflict || te3 != nil {
		t.Fatalf("同键不同体 = %v %v %v %v（want conflict）", te3, replay, conflict, err)
	}
	if len(st.tasks) != 1 || len(st.idem) != 1 {
		t.Fatalf("冲突后 store = tasks=%d idem=%d（want 1/1）", len(st.tasks), len(st.idem))
	}
	// 终态 + 过 TTL → 墓碑化同步回收键 → 同键重提 = 新任务（不重放、不冲突）。
	te1.finish(taskCompleted, []byte(`{"id":"t1"}`), cur)
	cur = cur.Add(taskTTL + time.Second)
	te4, replay, conflict, err := st.createIdem("t4", "m", "k1", body)
	if err != nil || replay || conflict || te4 == te1 {
		t.Fatalf("过期后同键 = %v %v %v %v（want 新任务 t4）", te4, replay, conflict, err)
	}
	if _, miss := st.get("t1"); miss != taskMissExpired {
		t.Fatalf("t1 墓碑 = %q（want task_expired）", miss)
	}
	if len(st.idem) != 1 || st.idem["k1"].taskID != "t4" {
		t.Fatalf("键表 = %+v（want k1→t4）", st.idem)
	}
	// 无键路径不受键表影响。
	te5, _, _, err := st.createIdem("t5", "m", "", nil)
	if err != nil || te5 == nil {
		t.Fatalf("无键创建 = %v %v", te5, err)
	}
}

// TestTaskIdemKeyExtract 键提取单元面：头 > 体、trim、非字符串/空/缺省容错。
func TestTaskIdemKeyExtract(t *testing.T) {
	cases := []struct {
		name, body, header, want string
	}{
		{"无键", `{"model":"m"}`, "", ""},
		{"头键", `{"model":"m"}`, "k1", "k1"},
		{"体键", `{"model":"m","idempotency_key":"k2"}`, "", "k2"},
		{"头胜体", `{"model":"m","idempotency_key":"kb"}`, "kh", "kh"},
		{"trim", `{"model":"m","idempotency_key":"  k3  "}`, "  k4  ", "k4"},
		{"体键空串", `{"model":"m","idempotency_key":""}`, "", ""},
		{"体键非字符串", `{"model":"m","idempotency_key":7}`, "", ""},
		{"头键空白", `{"model":"m","idempotency_key":"k5"}`, "   ", "k5"},
		{"畸形体", `not json`, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, "http://x/", nil)
			if err != nil {
				t.Fatalf("req: %v", err)
			}
			if c.header != "" {
				req.Header.Set("Idempotency-Key", c.header)
			}
			if got := taskIdempotencyKey(req, []byte(c.body)); got != c.want {
				t.Fatalf("taskIdempotencyKey = %q, want %q", got, c.want)
			}
		})
	}
}
