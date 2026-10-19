// W27-1（A2A-L19-W27-1）：任务面 Slice 5 验收测试（上游透传优化 + 能力表；提案
// `service-task-surface` §实施拆分 Slice 5 + §形态 C）。
//
// 覆盖：
//   - TestTaskFacePassthrough（验收主测，两子测试——同一验收命令全覆盖）：能力
//     =passthrough——创建面 id = 上游 id（非 resp_synth_*）+ background 原样透传
//     上游（托管面才剥除）；轮询面直连上游任务端点（GET /v1/responses/{id}）；
//     续流面**游标透传**（查询串 `stream=true&starting_after=7` 逐字到上游、SSE
//     原样回下游）；取消面直连 `POST /{id}/cancel`；未命中 id → 404 task_not_found
//     （不转发）。能力=off（子测试 off）——任务创建 404 task_face_unavailable +
//     上游零调用（任务流量绝不落 off 上游）；轮询/取消任务端点 404；同步路径不受
//     影响（background 缺省 → 上游直达 200——off 只关任务面）。
//   - TestTaskFaceOffChainSkip：链 [off, managed]——托管任务落在 managed 台，
//     off 台零调用（候选链剔除；创建裁定与任务 goroutine 同源）。
//   - TestTaskFaceRouteTable：路由表单元面（record/lookup/未知/过期/上限驱逐/
//     覆盖更新）。
//   - TestTaskFaceConfigValidate：能力表配置校验（非法值 / passthrough 限
//     responses 协议 / 三值合法 + 缺省）。
//
// 反证（回退即红；实跑输出见 tmp/lanes/W27-taskface5/proofs/）：passthrough 分支
// 回退（创建回落托管）→ 主测红；off 分支回退（创建回落托管）→ off 测红；游标
// 透传回退（丢弃查询串）→ 主测红；候选链剔除回退（off 台进任务链）→ ChainSkip 红。
package service

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ptUpstreamCall 一条透传假上游收到的请求记录（method/path/query/body/auth）。
type ptUpstreamCall struct {
	method string
	path   string
	query  string
	body   []byte
	auth   string
}

// ptUpstream 透传测试假上游：逐请求记录（按序），handler 写响应。
type ptUpstream struct {
	srv     *httptest.Server
	mu      sync.Mutex
	calls   []ptUpstreamCall
	handler func(w http.ResponseWriter, r *http.Request)
}

func newPTUpstream(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) *ptUpstream {
	t.Helper()
	u := &ptUpstream{handler: handler}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		u.mu.Lock()
		u.calls = append(u.calls, ptUpstreamCall{
			method: r.Method,
			path:   r.URL.Path,
			query:  r.URL.RawQuery,
			body:   b,
			auth:   r.Header.Get("Authorization"),
		})
		u.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(u.srv.Close)
	return u
}

// snapshot 当前调用记录副本；count 记录条数。
func (u *ptUpstream) snapshot() []ptUpstreamCall {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]ptUpstreamCall(nil), u.calls...)
}

func (u *ptUpstream) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.calls)
}

// TestTaskFacePassthrough 任务面 Slice 5 验收主测（提案 §实施拆分 Slice 5 验收
// 原文：能力=passthrough 时 id = 上游 id、游标透传；能力=off 时任务端点 404）。
// 两场景 = 两子测试（同一验收命令 `go test ./service/ -run TestTaskFacePassthrough`
// 下全覆盖）：
//   - passthrough：创建 id = 上游 id + background 透传；轮询/取消直连上游任务
//     端点；续流游标（starting_after）逐字透传；未命中 id 404 不转发。
//   - off：任务创建 404 task_face_unavailable + 上游零调用；轮询/取消 404；
//     同步路径不受影响（off 只关任务面）。
func TestTaskFacePassthrough(t *testing.T) {
	t.Run("passthrough", testTaskFacePassthroughMode)
	t.Run("off", testTaskFaceOffMode)
}

// testTaskFacePassthroughMode 场景一：能力=passthrough（见主测头注）。
func testTaskFacePassthroughMode(t *testing.T) {
	up := newPTUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/responses":
			// 创建面：上游原生任务面——background 由上游消化，返回**上游 id**。
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"resp_up_pt_1","object":"response","created_at":1741293000,"status":"queued","model":"gpt-5","output":[],"usage":null}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/responses/resp_up_pt_1":
			if r.URL.Query().Get("stream") == "true" {
				w.Header().Set("Content-Type", "text/event-stream")
				// 游标 7 → 从 8 起的续流（seq 断言在客户端面）。
				sseFrame(w, "response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_pt","output_index":0,"content_index":0,"delta":"resumed","sequence_number":8}`)
				sseFrame(w, "response.completed", `{"type":"response.completed","response":{"id":"resp_up_pt_1","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5","output":[],"usage":null},"sequence_number":9}`)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"resp_up_pt_1","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5","output":[],"usage":null}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/responses/resp_up_pt_1/cancel":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"resp_up_pt_1","object":"response","created_at":1741293000,"status":"cancelled","model":"gpt-5","output":[],"usage":null}`))
		default:
			http.NotFound(w, r)
		}
	})
	cfg := cfgFor(up.srv.URL, ProtocolResponses, 15)
	cfg.Upstreams[0].TaskFace = taskFacePassthrough
	_, srv := startServiceWith(t, cfg)

	// ① 创建：id = 上游 id（透传面核心；绝非 resp_synth_*）+ background 原样透传。
	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","background":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("passthrough create status = %d（want 200）\n%s", resp.StatusCode, raw)
	}
	var created struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("create body: %v\n%s", err, raw)
	}
	if created.ID != "resp_up_pt_1" {
		t.Fatalf("create id = %q（want 上游 id resp_up_pt_1——透传面 id = 上游 id，绝不合成）", created.ID)
	}
	if created.Status != "queued" {
		t.Fatalf("create status = %q（want 上游原样 queued）", created.Status)
	}
	calls := up.snapshot()
	if len(calls) != 1 {
		t.Fatalf("create 后上游调用数 = %d（want 1）", len(calls))
	}
	if calls[0].method != http.MethodPost || calls[0].path != "/v1/responses" {
		t.Fatalf("create 上游面 = %s %s（want POST /v1/responses）", calls[0].method, calls[0].path)
	}
	if !bytes.Contains(calls[0].body, []byte(`"background":true`)) {
		t.Fatalf("上游体缺 background:true（透传面必须保留——托管面才剥除）：%s", calls[0].body)
	}
	if calls[0].auth != "Bearer test-key" {
		t.Fatalf("create 上游认证 = %q（want Bearer test-key）", calls[0].auth)
	}

	// ② 轮询：直连上游任务端点（GET /v1/responses/{上游 id}），响应原样回下游。
	code, body := getJSON(t, srv.URL+"/v1/responses/resp_up_pt_1")
	if code != 200 {
		t.Fatalf("retrieve status = %d（want 200）\n%s", code, body)
	}
	if !bytes.Contains(body, []byte(`"status":"completed"`)) {
		t.Fatalf("retrieve body 非上游原样（want status completed）：%s", body)
	}
	calls = up.snapshot()
	if len(calls) != 2 || calls[1].method != http.MethodGet || calls[1].path != "/v1/responses/resp_up_pt_1" {
		t.Fatalf("retrieve 上游面 = %+v（want GET /v1/responses/resp_up_pt_1）", calls)
	}

	// ③ 续流：游标透传——查询串 `stream=true&starting_after=7` 逐字到上游，SSE
	// 原样回下游（sequence_number 8 起）。
	code, body = getJSON(t, srv.URL+"/v1/responses/resp_up_pt_1?stream=true&starting_after=7")
	if code != 200 {
		t.Fatalf("resume status = %d（want 200）\n%s", code, body)
	}
	if !bytes.Contains(body, []byte(`"sequence_number":8`)) || !bytes.Contains(body, []byte(`"resumed"`)) {
		t.Fatalf("resume body 非上游 SSE 原样（want seq 8 起）：%s", body)
	}
	calls = up.snapshot()
	if len(calls) != 3 {
		t.Fatalf("resume 后上游调用数 = %d（want 3）", len(calls))
	}
	if got := calls[2].query; got != "stream=true&starting_after=7" {
		t.Fatalf("resume 上游查询串 = %q（want stream=true&starting_after=7——游标透传）", got)
	}

	// ④ 取消：直连上游 `POST /{id}/cancel`（上游幂等语义直传）。
	respC := postJSON(t, srv.URL+"/v1/responses/resp_up_pt_1/cancel", "", nil)
	rawC, _ := io.ReadAll(respC.Body)
	respC.Body.Close()
	if respC.StatusCode != 200 {
		t.Fatalf("cancel status = %d（want 200）\n%s", respC.StatusCode, rawC)
	}
	if !bytes.Contains(rawC, []byte(`"status":"cancelled"`)) {
		t.Fatalf("cancel body 非上游原样：%s", rawC)
	}
	calls = up.snapshot()
	if len(calls) != 4 || calls[3].method != http.MethodPost || calls[3].path != "/v1/responses/resp_up_pt_1/cancel" {
		t.Fatalf("cancel 上游面 = %+v（want POST /v1/responses/resp_up_pt_1/cancel）", calls)
	}

	// ⑤ 未命中 id（两表皆无）→ 404 task_not_found；绝不转发上游。
	code, body = getJSON(t, srv.URL+"/v1/responses/resp_up_unknown")
	if code != 404 {
		t.Fatalf("unknown id status = %d（want 404）\n%s", code, body)
	}
	if c := taskErrorCode(t, body); c != "task_not_found" {
		t.Fatalf("unknown id code = %q（want task_not_found）", c)
	}
	if n := up.count(); n != 4 {
		t.Fatalf("未命中 id 被转发到上游（调用数 %d，want 4）", n)
	}
}

// testTaskFaceOffMode 场景二：能力=off——任务端点 404（提案 §实施拆分 Slice 5
// 验收原文），任务流量绝不落该上游；同步路径不受影响（off 只关任务面）。
func testTaskFaceOffMode(t *testing.T) {
	up := newPTUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		// off 面：任务请求不该到这里；同步面正常响应。
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_sync_off","object":"response","created_at":1,"status":"completed","model":"gpt-5","output":[],"usage":null}`))
	})
	cfg := cfgFor(up.srv.URL, ProtocolResponses, 15)
	cfg.Upstreams[0].TaskFace = taskFaceOff
	_, srv := startServiceWith(t, cfg)

	// ① 任务创建 → 404 task_face_unavailable；上游零调用。
	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","background":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("off create status = %d（want 404）\n%s", resp.StatusCode, raw)
	}
	if c := taskErrorCode(t, raw); c != "task_face_unavailable" {
		t.Fatalf("off create code = %q（want task_face_unavailable）", c)
	}
	if n := up.count(); n != 0 {
		t.Fatalf("off 上游收到 %d 次任务请求（want 0——任务流量绝不落 off 上游）", n)
	}

	// ② 轮询/取消任务端点同样 404（off 下不可能有任务——既有 miss 面）。
	code, body := getJSON(t, srv.URL+"/v1/responses/resp_anything")
	if code != 404 {
		t.Fatalf("off retrieve status = %d（want 404）\n%s", code, body)
	}
	respC := postJSON(t, srv.URL+"/v1/responses/resp_anything/cancel", "", nil)
	rawC, _ := io.ReadAll(respC.Body)
	respC.Body.Close()
	if respC.StatusCode != 404 {
		t.Fatalf("off cancel status = %d（want 404）\n%s", respC.StatusCode, rawC)
	}
	if n := up.count(); n != 0 {
		t.Fatalf("off 任务端点调用上游 %d 次（want 0）", n)
	}

	// ③ 同步路径不受影响（off 只关任务面）：background 缺省 → 上游直达 200。
	respS := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi"}`, nil)
	rawS, _ := io.ReadAll(respS.Body)
	respS.Body.Close()
	if respS.StatusCode != 200 {
		t.Fatalf("off 同步路径 status = %d（want 200）\n%s", respS.StatusCode, rawS)
	}
	if !bytes.Contains(rawS, []byte(`"resp_sync_off"`)) {
		t.Fatalf("off 同步路径响应非上游直达：%s", rawS)
	}
}

// TestTaskFaceOffChainSkip 链 [off, managed]：托管任务落在 managed 台；off 台
// 零调用（候选链剔除——创建裁定与任务 goroutine 同源）。
func TestTaskFaceOffChainSkip(t *testing.T) {
	upA := newPTUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		// off 台：任务流量若被误投到这里（回归面）——返回普通 JSON（非 SSE），
		// relay 会换台，但调用已发生 → 断言红。
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_a","object":"response","created_at":1,"status":"completed","model":"gpt-5","output":[],"usage":null}`))
	})
	upB := newPTUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		responsesTextStream(w)
	})
	cfg := &Config{
		Listen:           "127.0.0.1:0",
		HeartbeatSeconds: 15,
		Upstreams: []Upstream{
			{Name: "off-a", BaseURL: upA.srv.URL, APIKey: "k-a", Protocol: ProtocolResponses, TaskFace: taskFaceOff},
			{Name: "managed-b", BaseURL: upB.srv.URL, APIKey: "k-b", Protocol: ProtocolResponses},
		},
	}
	_, srv := startServiceWith(t, cfg)

	// 创建：候选链第一个非 off = B（managed）→ 托管路径（合成 id）。
	resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","background":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("create status = %d（want 200）\n%s", resp.StatusCode, raw)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("create body: %v\n%s", err, raw)
	}
	if !strings.HasPrefix(created.ID, "resp_synth_") {
		t.Fatalf("create id = %q（want resp_synth_*——首个非 off 台为 managed）", created.ID)
	}
	// 轮询到终态（托管面）。
	waitFor(t, 5*time.Second, func() bool {
		code, b := getJSON(t, srv.URL+"/v1/responses/"+created.ID)
		if code != 200 {
			return false
		}
		var m map[string]json.RawMessage
		return json.Unmarshal(b, &m) == nil && string(m["status"]) == `"completed"`
	})
	// off 台零调用；managed 台收到任务请求且 background 已剥除（托管语义）。
	if n := upA.count(); n != 0 {
		t.Fatalf("off 台收到 %d 次任务流量（want 0——候选链剔除）", n)
	}
	if n := upB.count(); n == 0 {
		t.Fatalf("managed 台未收到任务请求")
	}
	bodyB := upB.snapshot()[0].body
	if bytes.Contains(bodyB, []byte(`"background"`)) {
		t.Fatalf("managed 台上游体含 background（want 剥除——托管语义）：%s", bodyB)
	}
}

// TestTaskFaceRouteTable 路由表单元面：record/lookup、未知、过期、上限驱逐、覆盖。
func TestTaskFaceRouteTable(t *testing.T) {
	rt := newPassthroughRoutes()
	now := time.Unix(1000, 0)
	rt.now = func() time.Time { return now }
	rt.ttl = 50 * time.Second

	rt.record("resp_x", 1)
	if idx, ok := rt.lookup("resp_x"); !ok || idx != 1 {
		t.Fatalf("lookup resp_x = (%d,%v)（want (1,true)）", idx, ok)
	}
	if _, ok := rt.lookup("resp_y"); ok {
		t.Fatalf("未知 id 命中路由表（want miss）")
	}

	// 过期（惰性删除）。
	now = now.Add(50 * time.Second)
	if _, ok := rt.lookup("resp_x"); ok {
		t.Fatalf("过期条目仍命中（want miss）")
	}

	// 上限驱逐（最老先出）+ 覆盖更新不驱逐。
	now = time.Unix(2000, 0)
	rt.max = 2
	rt.record("a", 0)
	now = now.Add(time.Millisecond)
	rt.record("b", 0)
	now = now.Add(time.Millisecond)
	rt.record("c", 0) // 满员 → 驱逐最老 a
	if _, ok := rt.lookup("a"); ok {
		t.Fatalf("最老条目未被驱逐（want miss）")
	}
	if _, ok := rt.lookup("c"); !ok {
		t.Fatalf("新条目 c 未入表")
	}
	now = now.Add(time.Millisecond)
	rt.record("b", 5) // 覆盖（同 id 更新上游，不占新位）
	if idx, ok := rt.lookup("b"); !ok || idx != 5 {
		t.Fatalf("覆盖后 lookup b = (%d,%v)（want (5,true)）", idx, ok)
	}
}

// TestTaskFaceConfigValidate 能力表配置校验：非法值 / passthrough 限 responses
// 协议 / 三值合法 + 缺省（空 = managed）。
func TestTaskFaceConfigValidate(t *testing.T) {
	base := func(proto Protocol, face string) *Config {
		cfg := cfgFor("http://127.0.0.1:1", proto, 15)
		cfg.Upstreams[0].TaskFace = face
		return cfg
	}
	if err := base(ProtocolResponses, "bogus").Validate(); err == nil || !strings.Contains(err.Error(), "task_face") {
		t.Fatalf("非法 task_face 值未被拒：%v", err)
	}
	if err := base(ProtocolAnthropic, taskFacePassthrough).Validate(); err == nil {
		t.Fatalf("passthrough + 非 responses 协议未被拒")
	}
	for _, face := range []string{"", taskFaceManaged, taskFacePassthrough, taskFaceOff} {
		if err := base(ProtocolResponses, face).Validate(); err != nil {
			t.Fatalf("合法 task_face %q 被拒：%v", face, err)
		}
	}
	if got := base(ProtocolResponses, "").Upstreams[0].taskFace(); got != taskFaceManaged {
		t.Fatalf("缺省能力 = %q（want managed——提案原文缺省）", got)
	}
}
