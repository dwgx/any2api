// W19-2（A2A-L19-W19-2）：任务面 Slice 3 验收测试（断线续流：stream + starting_after）。
//
// 覆盖：
//   - TestTaskFaceResume（验收主测；提案 §实施拆分 Slice 3 验收原文）：
//     ① 中途掐断——第一条 `?stream=true` 连接读到 2 个事件后掐断（任务继续：
//     假上游被 gate 停住，随后照常产出尾帧）；
//     ② starting_after=N 续上——第二条连接从 N+1 重放 + 接续实时（gate 保证
//     续流段全是「实时」到达的事件），直到任务终态收尾；
//     ③ 全序断言——两段合并按 sequence_number 无缺无重：恰 [0..5]，类型序一致；
//     ④ 已终态重放——第三条连接在任务终态后 starting_after=N → 重放 [N+1..5]
//     后立即收尾；
//     ⑤ 过期游标 → 快照降级——溢出驱逐（注入容量 3）后 starting_after=0 /
//     无游标 / starting_after=1（下界-1）→ 200 JSON 快照（逐字节等于普通 GET
//     响应）；starting_after=2（下界-1+1）→ 恰从下界起 [3,4,5]；starting_after=5
//     （末事件）→ 空流收尾；畸形/负游标 → 400。
//   - 单元面：缓冲有界驱逐（下界推进 + 编号不复用 + 封口）、载荷 sequence_number
//     改写（字节级：原位改/插入/重复键/嵌套不动/多行重建）。
//
// 注（自编编号的钉死）：假上游帧自带的 sequence_number = 50..55（非 0 基）——
// 客户端可见的 0..5 只可能来自网关自编（改写面回退即红）。
//
// 反证（回退即红；原样输出见 tmp/lanes/W19-taskface3/proofs/revert-red-and-green.txt）：
// CP1 驱逐时编号复用（l.next 回退）→ ⑤c 段红；CP2 去掉溢出驱逐 → ⑤a 降级断言红；
// CP3 游标 off-by-one（start = cursor）→ ①②③ 重复事件红。
package service

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// taskResumeEvents 假上游 6 帧事件序（帧 0..5；sequence_number 故意 = 50..55——
// 网关自编编号的改写面钉死）。终态帧（5）带 usage（与 taskSlowUsageJSON 同源值）。
var taskResumeEvents = []struct{ event, data string }{
	{"response.created", `{"type":"response.created","response":{"id":"resp_resume","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":50}`},
	{"response.output_item.added", `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_resume","type":"message","status":"in_progress","role":"assistant","content":[]},"sequence_number":51}`},
	{"response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_resume","output_index":0,"content_index":0,"delta":"Hel","sequence_number":52,"logprobs":[]}`},
	{"response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_resume","output_index":0,"content_index":0,"delta":"lo","sequence_number":53,"logprobs":[]}`},
	{"response.output_item.done", `{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_resume","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Hello","annotations":[]}]},"sequence_number":54}`},
	{"response.completed", `{"type":"response.completed","response":{"id":"resp_resume","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5","output":[{"id":"msg_resume","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Hello","annotations":[]}]}],"usage":` + taskSlowUsageJSON + `,"incomplete_details":null},"sequence_number":55}`},
}

// taskResumeEmit 写出事件序的 [from, to) 帧（每帧 flush）。
func taskResumeEmit(w http.ResponseWriter, from, to int) {
	for i := from; i < to; i++ {
		sseFrame(w, taskResumeEvents[i].event, taskResumeEvents[i].data)
	}
}

// readSSEFrame 读一帧（行累积到空行——SSE 帧边界；EOF 时返回残余字节）。
func readSSEFrame(br *bufio.Reader) ([]byte, error) {
	var buf bytes.Buffer
	for {
		line, err := br.ReadBytes('\n')
		buf.Write(line)
		if err != nil {
			return buf.Bytes(), err
		}
		if len(bytes.TrimSpace(line)) == 0 {
			return buf.Bytes(), nil
		}
	}
}

// sseSeqEvent 续流事件观测：type + sequence_number（网关自编）。
type sseSeqEvent struct {
	Type string `json:"type"`
	Seq  int64  `json:"sequence_number"`
}

// parseSSESeq 从帧字节抽 data 载荷 → 事件；无 data（注释/心跳帧）→ ok=false。
func parseSSESeq(frame []byte) (sseSeqEvent, bool) {
	var payload []byte
	for _, line := range bytes.Split(frame, []byte("\n")) {
		if v, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			if len(payload) > 0 {
				payload = append(payload, '\n')
			}
			payload = append(payload, bytes.TrimSpace(v)...)
		}
	}
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		return sseSeqEvent{}, false
	}
	var ev sseSeqEvent
	if json.Unmarshal(payload, &ev) != nil {
		return sseSeqEvent{}, false
	}
	return ev, true
}

// readSSESeqEvents 读到 EOF（n<0）或 n 个事件为止；注释/心跳帧跳过。
func readSSESeqEvents(t *testing.T, br *bufio.Reader, n int) []sseSeqEvent {
	t.Helper()
	var evs []sseSeqEvent
	for n < 0 || len(evs) < n {
		frame, err := readSSEFrame(br)
		if ev, ok := parseSSESeq(frame); ok {
			evs = append(evs, ev)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("读续流帧: %v", err)
		}
	}
	return evs
}

// assertFullOrder 全序断言：事件列表恰为 [0..len-1] 严格递增（无缺无重）。
func assertFullOrder(t *testing.T, label string, evs []sseSeqEvent) {
	t.Helper()
	for i, ev := range evs {
		if ev.Seq != int64(i) {
			t.Fatalf("%s: 全序[%d] seq = %d（无缺无重：want %d）\n%+v", label, i, ev.Seq, i, evs)
		}
	}
}

// TestTaskFaceResume 任务面续流验收主测（见文件头注）。
func TestTaskFaceResume(t *testing.T) {
	gate := make(chan struct{})
	var gateOnce sync.Once
	release := func() { gateOnce.Do(func() { close(gate) }) }
	defer release() // 断言中途失败也不挂死上游 handler（srv.Close 等待在飞请求）
	up := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		taskResumeEmit(w, 0, 2) // 帧 0..1（过提交点 + 可掐断窗口）
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
		taskResumeEmit(w, 2, 6) // 帧 2..5（含终态）
	})
	_, srv := startServiceWith(t, cfgFor(up.srv.URL, ProtocolResponses, 15))

	client := &http.Client{Timeout: 10 * time.Second}
	getStream := func(url string) (*http.Response, *bufio.Reader) {
		t.Helper()
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("stream connect: %v", err)
		}
		return resp, bufio.NewReader(resp.Body)
	}
	getFull := func(url string) (*http.Response, []byte) {
		t.Helper()
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, raw
	}

	// 建任务。
	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","background":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &created); err != nil || created.ID == "" {
		t.Fatalf("create body: %v\n%s", err, raw)
	}
	base := srv.URL + "/v1/responses/" + created.ID

	// ① 第一条连接：?stream=true（无游标 → 从 0 起）。读到 2 个事件后掐断。
	resp1, br1 := getStream(base + "?stream=true")
	if resp1.StatusCode != 200 {
		t.Fatalf("stream #1 status = %d", resp1.StatusCode)
	}
	if ct := resp1.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("stream #1 Content-Type = %q", ct)
	}
	seg1 := readSSESeqEvents(t, br1, 2)
	resp1.Body.Close() // 掐断（任务继续——断开 ≠ 取消）
	assertFullOrder(t, "段1", seg1)
	if len(seg1) != 2 || seg1[0].Type != "response.created" || seg1[1].Type != "response.output_item.added" {
		t.Fatalf("段1 = %+v（want 前 2 帧）", seg1)
	}

	// ② 续流：starting_after=1（段1 末事件）。此时任务仍停在 gate——连接先建立
	// （headers 已 flush），随后放行：续流段全是实时到达的事件（非重放）。
	resp2, br2 := getStream(base + "?stream=true&starting_after=1")
	if resp2.StatusCode != 200 {
		t.Fatalf("stream #2 status = %d", resp2.StatusCode)
	}
	if ct := resp2.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("stream #2 Content-Type = %q", ct)
	}
	release() // 放行尾帧 2..5
	seg2 := readSSESeqEvents(t, br2, -1)
	resp2.Body.Close()

	// ③ 全序断言：两段合并 = [0..5]，无缺无重（按 sequence_number）。
	all := append(append([]sseSeqEvent{}, seg1...), seg2...)
	if len(all) != 6 {
		t.Fatalf("事件总数 = %d（want 6——无缺无重）\nseg1=%+v\nseg2=%+v", len(all), seg1, seg2)
	}
	assertFullOrder(t, "段1+段2", all)
	if seg2[0].Seq != 2 {
		t.Fatalf("续流段首事件 seq = %d（starting_after=1 → 应从 2 起）", seg2[0].Seq)
	}
	wantTypes := []string{"response.created", "response.output_item.added", "response.output_text.delta", "response.output_text.delta", "response.output_item.done", "response.completed"}
	for i, ev := range all {
		if ev.Type != wantTypes[i] {
			t.Fatalf("全序[%d] type = %q（want %q）", i, ev.Type, wantTypes[i])
		}
	}
	// 任务照常收敛到终态（掐断不杀任务）。
	code, body := getJSON(t, base)
	if code != 200 {
		t.Fatalf("poll status = %d\n%s", code, body)
	}
	var final map[string]json.RawMessage
	if err := json.Unmarshal(body, &final); err != nil || string(final["status"]) != `"completed"` {
		t.Fatalf("终态 = %s (err=%v)", final["status"], err)
	}

	// ④ 已终态重放：starting_after=1 → 重放 [2..5] 后立即收尾。
	resp3, br3 := getStream(base + "?stream=true&starting_after=1")
	seg3 := readSSESeqEvents(t, br3, -1)
	resp3.Body.Close()
	if len(seg3) != 4 {
		t.Fatalf("终态重放段 = %+v（want [2..5] 4 条）", seg3)
	}
	for i, ev := range seg3 {
		if ev.Seq != int64(2+i) {
			t.Fatalf("终态重放[%d] seq = %d（want %d）", i, ev.Seq, 2+i)
		}
	}

	// ④b 未知 id + stream=true → 404（与轮询面同双 code）。
	code, body = getJSON(t, srv.URL+"/v1/responses/resp_synth_00000000000000000000000000000000?stream=true")
	if code != 404 || taskErrorCode(t, body) != "task_not_found" {
		t.Fatalf("未知 id stream 面 = %d %s", code, body)
	}

	// -----------------------------------------------------------------------
	// ⑤ 过期游标 → 快照降级（溢出驱逐面：注入容量 3，6 帧后保留 [3,4,5]）
	// -----------------------------------------------------------------------
	gateB := make(chan struct{})
	var gateBOnce sync.Once
	releaseB := func() { gateBOnce.Do(func() { close(gateB) }) }
	defer releaseB() // 同上：断言中途失败也不挂死
	upB := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		taskResumeEmit(w, 0, 3) // 帧 0..2
		select {
		case <-gateB:
		case <-r.Context().Done():
			return
		}
		taskResumeEmit(w, 3, 6) // 帧 3..5
	})
	sB, srvB := startServiceWith(t, cfgFor(upB.srv.URL, ProtocolResponses, 15))

	respB := postJSON(t, srvB.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","background":true}`, nil)
	rawB, _ := io.ReadAll(respB.Body)
	respB.Body.Close()
	var createdB struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rawB, &createdB); err != nil || createdB.ID == "" {
		t.Fatalf("create B body: %v\n%s", err, rawB)
	}
	baseB := srvB.URL + "/v1/responses/" + createdB.ID
	// 等前 3 帧入缓冲（网关读侧异步），再注入容量 3（此后每追加一条驱逐最老）。
	waitFor(t, 5*time.Second, func() bool {
		sB.tasks.mu.Lock()
		te := sB.tasks.tasks[createdB.ID]
		sB.tasks.mu.Unlock()
		if te == nil {
			return false
		}
		te.log.mu.Lock()
		n := te.log.next
		te.log.mu.Unlock()
		return n >= 3
	})
	if !sB.tasks.taskLogCaps(createdB.ID, 3, 1<<20) {
		t.Fatalf("taskLogCaps: 任务不存在")
	}
	releaseB()
	waitFor(t, 5*time.Second, func() bool {
		code, b := getJSON(t, baseB)
		if code != 200 {
			return false
		}
		var m map[string]json.RawMessage
		return json.Unmarshal(b, &m) == nil && string(m["status"]) == `"completed"`
	})

	// ⑤a 过期游标（starting_after=0 → start=1 < 下界 3）→ 快照降级：200 JSON 快照，
	// 且逐字节等于普通 GET 响应（「窗口过期 → 普通 GET」的降级终点）。
	plainResp, plainBody := getFull(baseB)
	if plainResp.StatusCode != 200 {
		t.Fatalf("plain get status = %d", plainResp.StatusCode)
	}
	dgResp, dgBody := getFull(baseB + "?stream=true&starting_after=0")
	if dgResp.StatusCode != 200 {
		t.Fatalf("降级 status = %d\n%s", dgResp.StatusCode, dgBody)
	}
	if ct := dgResp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("降级 Content-Type = %q（want application/json——快照降级）", ct)
	}
	if !bytes.Equal(dgBody, plainBody) {
		t.Fatalf("降级响应应逐字节等于普通 GET 响应\ngot  %s\nwant %s", dgBody, plainBody)
	}
	var dg map[string]json.RawMessage
	if err := json.Unmarshal(dgBody, &dg); err != nil || string(dg["status"]) != `"completed"` {
		t.Fatalf("降级体 = %s (err=%v)", dgBody, err)
	}

	// ⑤b 无游标（从 0 起）同样过期 → 降级。
	_, dgBody2 := getFull(baseB + "?stream=true")
	if !bytes.Equal(dgBody2, plainBody) {
		t.Fatalf("无游标降级应逐字节等于普通 GET 响应\ngot  %s", dgBody2)
	}

	// ⑤c 游标边界：starting_after=1（start=2 < 下界 3）→ 降级；starting_after=2
	// （start=3 == 下界）→ 恰从下界起 [3,4,5]。
	_, dgBody3 := getFull(baseB + "?stream=true&starting_after=1")
	if !bytes.Equal(dgBody3, plainBody) {
		t.Fatalf("边界下界-1 应降级\ngot  %s", dgBody3)
	}
	respC, brC := getStream(baseB + "?stream=true&starting_after=2")
	if ct := respC.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("边界游标 Content-Type = %q（want event-stream）", ct)
	}
	segC := readSSESeqEvents(t, brC, -1)
	respC.Body.Close()
	if len(segC) != 3 {
		t.Fatalf("边界游标段 = %+v（want [3,4,5]）", segC)
	}
	for i, ev := range segC {
		if ev.Seq != int64(3+i) {
			t.Fatalf("边界游标[%d] seq = %d（want %d）", i, ev.Seq, 3+i)
		}
	}

	// ⑤d 末游标（starting_after=5 = 末事件）→ 空流收尾。
	respD, brD := getStream(baseB + "?stream=true&starting_after=5")
	if ct := respD.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("末游标 Content-Type = %q", ct)
	}
	segD := readSSESeqEvents(t, brD, -1)
	respD.Body.Close()
	if len(segD) != 0 {
		t.Fatalf("末游标段应为空，got %+v", segD)
	}

	// ⑤e 畸形/负游标 → 400。
	code, body = getJSON(t, baseB+"?stream=true&starting_after=abc")
	if code != 400 || taskErrorCode(t, body) != "invalid_starting_after" {
		t.Fatalf("畸形游标 = %d %s", code, body)
	}
	code, body = getJSON(t, baseB+"?stream=true&starting_after=-1")
	if code != 400 || taskErrorCode(t, body) != "invalid_starting_after" {
		t.Fatalf("负游标 = %d %s", code, body)
	}
}

// TestTaskEventLogBounded 缓冲单元面：有界驱逐（下界推进、编号不复用）、过期
// 判据、订阅取数（含 behind）、封口收尾。
func TestTaskEventLogBounded(t *testing.T) {
	l := newTaskEventLog()
	l.setCaps(3, 1<<20)
	for i := range 10 {
		block := []byte(`data: {"type":"x","sequence_number":0}`)
		got := l.appendEvent(block, []byte(`{"type":"x","sequence_number":0}`))
		if got != int64(i) {
			t.Fatalf("append %d: seq = %d（编号单调不复用）", i, got)
		}
	}
	// 驱逐后保留 [7,8,9]（下界 7）；编号未复用（累计 10 条 → next=10）。
	evs, done, ok := l.snapshotFrom(7)
	if !ok || done || len(evs) != 3 || evs[0].seq != 7 || evs[2].seq != 9 {
		t.Fatalf("snapshotFrom(7) = %v done=%v ok=%v", evs, done, ok)
	}
	if !bytes.Contains(evs[0].frame, []byte(`"sequence_number":7`)) {
		t.Fatalf("帧未改写网关号: %s", evs[0].frame)
	}
	if !bytes.HasSuffix(evs[0].frame, []byte("\n\n")) {
		t.Fatalf("帧应以空行收尾: %q", evs[0].frame)
	}
	if _, _, ok := l.snapshotFrom(6); ok {
		t.Fatalf("start=6 < 下界 7 应为过期游标")
	}
	if _, _, ok := l.snapshotFrom(0); ok {
		t.Fatalf("start=0 应为过期游标")
	}
	// 订阅增量：liveBatch(9) → [9]；liveBatch(10) → 空；liveBatch(6) → behind。
	if evs, _, done, behind := l.liveBatch(9); len(evs) != 1 || evs[0].seq != 9 || done || behind {
		t.Fatalf("liveBatch(9) = %v done=%v behind=%v", evs, done, behind)
	}
	if evs, _, _, _ := l.liveBatch(10); len(evs) != 0 {
		t.Fatalf("liveBatch(10) = %v（应为空）", evs)
	}
	if _, _, _, behind := l.liveBatch(6); !behind {
		t.Fatalf("liveBatch(6) 应 behind（6 < 下界 7）")
	}
	// 封口：done 收尾；重复封口幂等。
	l.closeLog()
	l.closeLog()
	if _, done, ok := l.snapshotFrom(7); !ok || !done {
		t.Fatalf("封口后 snapshotFrom done 应为 true")
	}
	if _, _, done, _ := l.liveBatch(10); !done {
		t.Fatalf("封口后 liveBatch(10) done 应为 true")
	}
}

// TestTaskSeqRewrite 载荷改写（字节级）：原位改/插入/重复键全改/嵌套不动/非对象
// 原样；块改写（单行原位、多行重建）。
func TestTaskSeqRewrite(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"中段改写", `{"a":1,"sequence_number":50,"b":2}`, `{"a":1,"sequence_number":7,"b":2}`},
		{"末段改写", `{"a":1,"sequence_number":123456}`, `{"a":1,"sequence_number":7}`},
		{"缺省插入", `{"a":1}`, `{"a":1,"sequence_number":7}`},
		{"空对象插入", `{}`, `{"sequence_number":7}`},
		{"重复键全改", `{"sequence_number":1,"a":1,"sequence_number":2}`, `{"sequence_number":7,"a":1,"sequence_number":7}`},
		{"已等值不动", `{"sequence_number":7,"a":1}`, `{"sequence_number":7,"a":1}`},
		{"嵌套对象不动", `{"a":{"sequence_number":1}}`, `{"a":{"sequence_number":1},"sequence_number":7}`},
		{"非对象原样", `[1,2]`, `[1,2]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := string(setTopLevelSeq([]byte(c.in), 7))
			if got != c.want {
				t.Fatalf("setTopLevelSeq = %s\nwant %s", got, c.want)
			}
			if !json.Valid([]byte(got)) {
				t.Fatalf("invalid JSON: %s", got)
			}
		})
	}
	// 单行块：载荷原位替换，其余字节逐字保留。
	block := []byte("event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":50}")
	payload := []byte(`{"type":"response.created","sequence_number":50}`)
	got := string(rewriteEventSeqFrame(block, payload, 3))
	want := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":3}"
	if got != want {
		t.Fatalf("单行块改写 = %s\nwant %s", got, want)
	}
	// 多行 data（病理）：重建块（event 名 + 逐行 data）。
	block2 := []byte("event: x\ndata: {\"a\":\ndata: 1}")
	payload2 := []byte("{\"a\":\n1}")
	got2 := string(rewriteEventSeqFrame(block2, payload2, 3))
	want2 := "event: x\ndata: {\"a\":\ndata: 1,\"sequence_number\":3}\n"
	if got2 != want2 {
		t.Fatalf("多行块改写 = %q\nwant %q", got2, want2)
	}
}
