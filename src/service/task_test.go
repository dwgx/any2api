// W8-1（A2A-L19-W8-1）：任务资源最小面验收测试（responses 入口托管，提案
// `service-task-surface` §实施拆分 Slice 1）。
//
// 覆盖：
//   - TestTaskFaceRetrieve（验收主测）：假上游慢流（2 帧后停 1s 再终态）——
//     提交 <500ms 拿合成快照 → 轮询到 completed 且终态 output/usage 与上游
//     逐值一致；background 剥除（禁注入）/stream 强制 true（上游体断言）；
//     未知 id → 404 task_not_found；短 TTL（注入）→ 404 task_expired；
//     同步路径回归（background 缺省 → 同一上游普通响应直达）。
//   - 单元面：入口判据（仅布尔 true）、上游体两处手术（字节级）、扫描器终态
//     抽取（first-terminal-wins）/错误信封归因、store 过期/墓碑/满载。
//
// 反证（回退即红）：backgroundRequested 恒 false → 提交断言红（同步面返回的不是
// 快照，且提交耗时 ≥ 上游慢流 1s）；taskUpstreamBody 不剥除 → 上游体断言红。
// 实跑输出见报告 §反证与 tmp/lanes/W8-taskface1/proofs/。
package service

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

// getJSON 发 GET 并读回整体（测试专用）。
func getJSON(t *testing.T, url string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// taskSlowUsageJSON 慢流终态 usage（逐值断言用；与 taskSlowStream 的 completed
// 帧同源）。
const taskSlowUsageJSON = `{"input_tokens":9,"input_tokens_details":{"cached_tokens":2,"cache_write_tokens":0},"output_tokens":4,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":13}`

// taskSlowStream 慢速 responses 事件流：2 帧后停 1s 再终态（brief 验收场景）。
func taskSlowStream(w http.ResponseWriter) {
	sseFrame(w, "response.created", `{"type":"response.created","response":{"id":"resp_slow","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":0}`)
	sseFrame(w, "response.output_item.added", `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_slow","type":"message","status":"in_progress","role":"assistant","content":[]},"sequence_number":1}`)
	time.Sleep(1 * time.Second)
	sseFrame(w, "response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_slow","output_index":0,"content_index":0,"delta":"Hello world","sequence_number":2,"logprobs":[]}`)
	sseFrame(w, "response.output_item.done", `{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_slow","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Hello world","annotations":[]}]},"sequence_number":3}`)
	sseFrame(w, "response.completed", `{"type":"response.completed","response":{"id":"resp_slow","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5","output":[{"id":"msg_slow","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Hello world","annotations":[]}]}],"usage":`+taskSlowUsageJSON+`,"incomplete_details":null},"sequence_number":4}`)
}

// taskErrorCode 从 responses 错误信封取 error.code。
func taskErrorCode(t *testing.T, body []byte) string {
	t.Helper()
	var env struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("error envelope: %v\n%s", err, body)
	}
	return env.Error.Code
}

// TestTaskFaceRetrieve 任务面验收主测（见文件头注）。
func TestTaskFaceRetrieve(t *testing.T) {
	// 假上游双面：任务面（上游体 stream:true）回慢速 SSE 流；同步面（无
	// stream:true）回普通 JSON 响应对象——同一上游覆盖两条路径。
	var up *responsesUpstream
	up = newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, body, _ := up.request()
		if !bytes.Contains(body, []byte(`"stream":true`)) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"resp_sync","object":"response","created_at":1,"status":"completed","model":"gpt-5","output":[{"id":"msg_sync","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"sync ok","annotations":[]}]}],"usage":{"input_tokens":1,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":0},"output_tokens":1,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":2}}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		taskSlowStream(w)
	})
	s, srv := startServiceWith(t, cfgFor(up.srv.URL, ProtocolResponses, 15))

	// ① 提交：立即（<500ms）拿到合成快照——绝不等待上游（慢流 1s 在后）。
	start := time.Now()
	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","background":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	elapsed := time.Since(start)
	if resp.StatusCode != 200 {
		t.Fatalf("create status = %d\n%s", resp.StatusCode, raw)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("create elapsed = %s（任务面不得等待上游）", elapsed)
	}
	var created struct {
		ID     string            `json:"id"`
		Object string            `json:"object"`
		Status string            `json:"status"`
		Model  string            `json:"model"`
		Output []json.RawMessage `json:"output"`
		Usage  json.RawMessage   `json:"usage"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("create body: %v\n%s", err, raw)
	}
	if !strings.HasPrefix(created.ID, "resp_synth_") || len(created.ID) != len("resp_synth_")+32 {
		t.Fatalf("create id = %q（want resp_synth_<32hex>）", created.ID)
	}
	if created.Status != "queued" && created.Status != "in_progress" {
		t.Fatalf("create status = %q（want queued/in_progress）", created.Status)
	}
	if created.Object != "response" || created.Model != "gpt-5" {
		t.Fatalf("create snapshot = %s", raw)
	}
	if string(created.Usage) != "null" {
		t.Fatalf("create usage = %s（want null）", created.Usage)
	}

	// ② 轮询：首次未终态（上游 1s 慢流）；随后到 completed。
	code, body := getJSON(t, srv.URL+"/v1/responses/"+created.ID)
	if code != 200 {
		t.Fatalf("poll status = %d\n%s", code, body)
	}
	var running map[string]json.RawMessage
	if err := json.Unmarshal(body, &running); err != nil {
		t.Fatalf("poll body: %v\n%s", err, body)
	}
	if got := string(running["status"]); got != `"queued"` && got != `"in_progress"` {
		t.Fatalf("first poll status = %s（上游 1s 慢流，应未终态）", got)
	}
	var final map[string]json.RawMessage
	waitFor(t, 5*time.Second, func() bool {
		code, b := getJSON(t, srv.URL+"/v1/responses/"+created.ID)
		if code != 200 {
			return false
		}
		var m map[string]json.RawMessage
		if json.Unmarshal(b, &m) != nil {
			return false
		}
		if string(m["status"]) != `"completed"` {
			return false
		}
		final = m
		return true
	})
	// 终态 id = 创建 id（客户端轮询同一 id）。
	if got := string(final["id"]); got != `"`+created.ID+`"` {
		t.Fatalf("terminal id = %s, want %q", got, created.ID)
	}
	// 终态 output 逐值：上游文本 "Hello world"。
	var out []struct {
		ID      string `json:"id"`
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(final["output"], &out); err != nil || len(out) != 1 || len(out[0].Content) != 1 {
		t.Fatalf("terminal output = %s (err=%v)", final["output"], err)
	}
	if out[0].Content[0].Text != "Hello world" {
		t.Fatalf("terminal text = %q", out[0].Content[0].Text)
	}
	// 终态 usage 逐值：与上游 completed 帧一致。
	var wantUsage, gotUsage any
	if err := json.Unmarshal([]byte(taskSlowUsageJSON), &wantUsage); err != nil {
		t.Fatalf("want usage: %v", err)
	}
	if err := json.Unmarshal(final["usage"], &gotUsage); err != nil {
		t.Fatalf("terminal usage: %v\n%s", err, final["usage"])
	}
	if !reflect.DeepEqual(wantUsage, gotUsage) {
		t.Fatalf("terminal usage = %s\nwant %s", final["usage"], taskSlowUsageJSON)
	}

	// ③ 上游面：background 已剥除（禁注入红线）+ stream 强制 true + 其余字段保留。
	_, upBody, _ := up.request()
	if bytes.Contains(upBody, []byte("background")) {
		t.Fatalf("background leaked upstream: %s", upBody)
	}
	if !bytes.Contains(upBody, []byte(`"stream":true`)) {
		t.Fatalf("upstream body lacks stream:true: %s", upBody)
	}
	if !bytes.Contains(upBody, []byte(`"input":"hi"`)) {
		t.Fatalf("input field lost: %s", upBody)
	}

	// ④ 未知 id → 404 task_not_found。
	code, body = getJSON(t, srv.URL+"/v1/responses/resp_synth_00000000000000000000000000000000")
	if code != 404 {
		t.Fatalf("unknown id status = %d\n%s", code, body)
	}
	if got := taskErrorCode(t, body); got != "task_not_found" {
		t.Fatalf("unknown id code = %q", got)
	}

	// ⑤ 同步路径回归：background 缺省 → 现有同步面（非流式同参）零改动。
	resp2 := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi"}`, nil)
	raw2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != 200 || !bytes.Contains(raw2, []byte(`"resp_sync"`)) || !bytes.Contains(raw2, []byte("sync ok")) {
		t.Fatalf("sync path regression: status=%d body=%s", resp2.StatusCode, raw2)
	}

	// ⑥ 短 TTL（注入）→ 终态保留期过后 404 task_expired。
	resp3 := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","background":true}`, nil)
	raw3, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()
	var created3 struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw3, &created3); err != nil || created3.ID == "" {
		t.Fatalf("ttl task create: %v\n%s", err, raw3)
	}
	waitFor(t, 5*time.Second, func() bool {
		code, b := getJSON(t, srv.URL+"/v1/responses/"+created3.ID)
		if code != 200 {
			return false
		}
		var m map[string]json.RawMessage
		if json.Unmarshal(b, &m) != nil {
			return false
		}
		return string(m["status"]) == `"completed"`
	})
	s.tasks.setTTL(30 * time.Millisecond)
	time.Sleep(120 * time.Millisecond) // > ttl
	code, body = getJSON(t, srv.URL+"/v1/responses/"+created3.ID)
	if code != 404 {
		t.Fatalf("expired id status = %d\n%s", code, body)
	}
	if got := taskErrorCode(t, body); got != "task_expired" {
		t.Fatalf("expired id code = %q", got)
	}
}

// TestTaskFaceCrossFormat 跨格式任务面：responses 入口 + chat 上游 + background:true
// ——该形态此前在转换面被 checkStatefulFields 拒 400（background 属 stateful 名单）；
// 任务面在服务层消化 background（剥除后才进转换），任务照常托管：提交快照 →
// 轮询到 completed（转换后的 output/usage 与上游 chat 流逐值一致）。
func TestTaskFaceCrossFormat(t *testing.T) {
	up := newChatUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		chatTextStream(w)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))

	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","background":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("create status = %d\n%s", resp.StatusCode, raw)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &created); err != nil || created.ID == "" {
		t.Fatalf("create body: %v\n%s", err, raw)
	}
	var final map[string]json.RawMessage
	waitFor(t, 5*time.Second, func() bool {
		code, b := getJSON(t, srv.URL+"/v1/responses/"+created.ID)
		if code != 200 {
			return false
		}
		var m map[string]json.RawMessage
		if json.Unmarshal(b, &m) != nil {
			return false
		}
		if string(m["status"]) != `"completed"` {
			return false
		}
		final = m
		return true
	})
	// 转换面终态：文本 "Hello world" + chat usage 映射（prompt/completion → input/output）。
	if !bytes.Contains(final["output"], []byte("Hello world")) {
		t.Fatalf("terminal output = %s", final["output"])
	}
	var usage struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
		TotalTokens  int `json:"total_tokens"`
	}
	if err := json.Unmarshal(final["usage"], &usage); err != nil {
		t.Fatalf("usage: %v\n%s", err, final["usage"])
	}
	if usage.InputTokens != 11 || usage.OutputTokens != 2 || usage.TotalTokens != 13 {
		t.Fatalf("usage = %+v", usage)
	}
	// 上游面：chat 体（messages[]）+ stream:true；background 已剥除（禁注入）。
	_, _, upBody := up.request()
	if !bytes.Contains(upBody, []byte(`"messages"`)) || !bytes.Contains(upBody, []byte(`"stream":true`)) {
		t.Fatalf("upstream body = %s", upBody)
	}
	if bytes.Contains(upBody, []byte("background")) {
		t.Fatalf("background leaked upstream: %s", upBody)
	}
}

// TestTaskFaceFailedTerminal 任务失败面：上游失败（提交前换上游穷尽）→ create 仍
// 立即 200 快照（绝不等待上游），轮询收敛到 failed + error（错误体 → 任务 error；
// Slice 2 的完整失败语义表在此面上扩展）。防回归：任务不得悬在 in_progress。
func TestTaskFaceFailedTerminal(t *testing.T) {
	up := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`{"error":{"message":"boom","type":"server_error"}}`))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))

	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","background":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("create status = %d\n%s", resp.StatusCode, raw)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &created); err != nil || created.ID == "" {
		t.Fatalf("create body: %v\n%s", err, raw)
	}
	var final map[string]json.RawMessage
	waitFor(t, 5*time.Second, func() bool {
		code, b := getJSON(t, srv.URL+"/v1/responses/"+created.ID)
		if code != 200 {
			return false
		}
		var m map[string]json.RawMessage
		if json.Unmarshal(b, &m) != nil {
			return false
		}
		if string(m["status"]) != `"failed"` {
			return false
		}
		final = m
		return true
	})
	var errObj struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(final["error"], &errObj); err != nil {
		t.Fatalf("terminal error: %v\n%s", err, final["error"])
	}
	if errObj.Code == "" || !strings.Contains(errObj.Message, "boom") {
		t.Fatalf("terminal error = %+v", errObj)
	}
}

// TestTaskFaceBackgroundDetection 入口判据：仅布尔 true 进任务面；其余形态走
// 同步路径零改动。
func TestTaskFaceBackgroundDetection(t *testing.T) {
	cases := []struct {
		body string
		want bool
	}{
		{`{"model":"m","input":"hi"}`, false},
		{`{"model":"m","input":"hi","background":false}`, false},
		{`{"model":"m","input":"hi","background":null}`, false},
		{`{"model":"m","input":"hi","background":true}`, true},
		{`{"model":"m","input":"hi","background":"true"}`, false},
		{`{"model":"m","input":"hi","background":1}`, false},
		{`{"background":true}`, true},
		{`[{"background":true}]`, false},
		{`not json`, false},
		{`{"model":"m","input":"hi"`, false},
	}
	for _, c := range cases {
		if got := backgroundRequested([]byte(c.body)); got != c.want {
			t.Fatalf("backgroundRequested(%s) = %v, want %v", c.body, got, c.want)
		}
	}
}

// TestTaskFaceUpstreamBody 上游体三处手术（字节级）：background 剥除 + 幂等键
// 字段剥除（Slice 4）+ stream 强制 true；其余字节逐字保留（键序/嵌套不动）。
func TestTaskFaceUpstreamBody(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"首字段剥除+补流位", `{"background":true,"model":"m","input":"hi"}`, `{"model":"m","input":"hi","stream":true}`},
		{"末字段剥除", `{"model":"m","input":"hi","background":true}`, `{"model":"m","input":"hi","stream":true}`},
		{"中间字段剥除", `{"model":"m","background":true,"input":"hi"}`, `{"model":"m","input":"hi","stream":true}`},
		{"stream false 改写", `{"background":true,"model":"m","stream":false}`, `{"model":"m","stream":true}`},
		{"stream true 保留", `{"background":true,"model":"m","stream":true}`, `{"model":"m","stream":true}`},
		{"嵌套 background 不动", `{"background":true,"model":"m","input":{"background":true}}`, `{"model":"m","input":{"background":true},"stream":true}`},
		{"带空白字节保留", `{ "background" : true , "model" : "m" }`, `{  "model" : "m" ,"stream":true}`},
		{"幂等键字段剥除", `{"background":true,"model":"m","idempotency_key":"k1"}`, `{"model":"m","stream":true}`},
		{"嵌套幂等键不动", `{"background":true,"model":"m","input":{"idempotency_key":"x"}}`, `{"model":"m","input":{"idempotency_key":"x"},"stream":true}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := string(taskUpstreamBody([]byte(c.in)))
			if got != c.want {
				t.Fatalf("taskUpstreamBody = %s\nwant %s", got, c.want)
			}
			if !json.Valid([]byte(got)) {
				t.Fatalf("invalid JSON: %s", got)
			}
		})
	}
}

// TestTaskFaceStreamWriterTerminal 扫描器：终态抽取（first-terminal-wins）、
// 跨 Write 的块边界、注释帧忽略、错误信封归因。
func TestTaskFaceStreamWriterTerminal(t *testing.T) {
	tw := &taskStreamWriter{hdr: http.Header{}}
	tw.WriteHeader(200)
	stream := ": keepalive\n\n" +
		"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\"}}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n" +
		"event: error\ndata: {\"type\":\"error\",\"code\":\"late_error\",\"message\":\"after terminal\"}\n\n" +
		"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_1\",\"status\":\"failed\"}}\n\n"
	// 分两段写（40 字节处切开一个帧）：验证块边界跨 Write 缓冲。
	if _, err := tw.Write([]byte(stream[:40])); err != nil {
		t.Fatalf("write head: %v", err)
	}
	if _, err := tw.Write([]byte(stream[40:])); err != nil {
		t.Fatalf("write tail: %v", err)
	}
	status, snap := tw.result("task_1", "m", 5)
	if status != taskCompleted {
		t.Fatalf("status = %q（first-terminal-wins：终止帧后的错误帧不翻转）", status)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(snap, &m); err != nil {
		t.Fatalf("snapshot: %v\n%s", err, snap)
	}
	if string(m["id"]) != `"task_1"` {
		t.Fatalf("snapshot id = %s（应改写为任务 id）", m["id"])
	}
	if string(m["status"]) != `"completed"` {
		t.Fatalf("snapshot status = %s", m["status"])
	}

	// 错误信封归因（serve 提交前失败面）：status 502 + responses 信封 → failed。
	tw2 := &taskStreamWriter{hdr: http.Header{}}
	tw2.WriteHeader(502)
	_, _ = tw2.Write([]byte(`{"error":{"message":"upstream request failed: boom","type":"api_error"}}`))
	status2, snap2 := tw2.result("task_2", "m", 5)
	if status2 != taskFailed {
		t.Fatalf("envelope status = %q", status2)
	}
	if !bytes.Contains(snap2, []byte(`"message":"upstream request failed: boom"`)) ||
		!bytes.Contains(snap2, []byte(`"code":"server_error"`)) {
		t.Fatalf("envelope snapshot = %s", snap2)
	}

	// 空产出 → failed 兜底。
	tw3 := &taskStreamWriter{hdr: http.Header{}}
	status3, snap3 := tw3.result("task_3", "m", 5)
	if status3 != taskFailed || !bytes.Contains(snap3, []byte("without terminal event")) {
		t.Fatalf("empty result = %q %s", status3, snap3)
	}
}

// TestTaskFaceStoreExpiry store 面：运行中不过期；终态过 TTL → task_expired
// （幂等墓碑）；未知 → task_not_found；满载 → errTaskLimit。
func TestTaskFaceStoreExpiry(t *testing.T) {
	st := newTaskStore()
	base := time.Unix(1_700_000_000, 0)
	cur := base
	st.now = func() time.Time { return cur }
	te, err := st.create("t1", "m")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got, miss := st.get("t1"); got != te || miss != "" {
		t.Fatalf("live get = %v %q", got, miss)
	}
	// 运行中（无终态）不因 TTL 过期。
	cur = base.Add(24 * time.Hour)
	if got, miss := st.get("t1"); got != te || miss != "" {
		t.Fatalf("running task must not expire: %q", miss)
	}
	// 终态 + 过 TTL → task_expired（重复 GET 仍 task_expired——墓碑幂等）。
	te.finish(taskCompleted, []byte(`{"id":"t1"}`), cur)
	cur = cur.Add(taskTTL + time.Second)
	if got, miss := st.get("t1"); got != nil || miss != taskMissExpired {
		t.Fatalf("expired = %v %q", got, miss)
	}
	if _, miss := st.get("t1"); miss != taskMissExpired {
		t.Fatalf("repeat expired = %q", miss)
	}
	if _, miss := st.get("nope"); miss != taskMissNotFound {
		t.Fatalf("unknown = %q", miss)
	}

	// 满载：先清过期仍满 → errTaskLimit。
	st2 := newTaskStore()
	st2.maxTasks = 1
	if _, err := st2.create("a", "m"); err != nil {
		t.Fatalf("create a: %v", err)
	}
	if _, err := st2.create("b", "m"); err != errTaskLimit {
		t.Fatalf("want errTaskLimit, got %v", err)
	}
}
