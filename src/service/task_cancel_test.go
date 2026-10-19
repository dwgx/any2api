// W17-2（A2A-L19-W17-2）：任务面 Slice 2 验收测试（取消 + 失败语义表 + 断线）。
//
// 覆盖：
//   - TestTaskFaceCancel（验收主测；四断言）：
//     ① 真中止——cancel 后假上游 r.Context().Done() 在 1s 内触发（在飞上游调用
//     被真取消，不是等流自然结束）；
//     ② 轮询终态 cancelled（轮询面与 cancel 面同快照）；
//     ③ 重复 cancel 同终态（幂等：第二次 cancel 返回同一终态快照——字节级）；
//     ④ idle 场景——假活跃（建流帧过提交点后只发非内容帧）+ 配置
//     `upstream_idle_timeout_seconds=1` → 任务终态 incomplete +
//     error.code=upstream_idle（失败语义表「任务挂住」行）。
//   - TestTaskFaceDisconnectKeepsTask：提交请求生命周期终结（客户端取消 ctx /
//     连接中止）不杀任务——任务 ctx 与提交请求解耦（断开 ≠ 取消；终态 completed）。
//
// 注（④ 的「只发 ping」口径）：responses 协议无 ping 帧，守卫排除集
// （frameCarriesContent）为 response.created/in_progress/queued——测试用
// response.created 过提交点、随后只发 response.in_progress（假活跃等价物）。
//
// 反证（回退即红；原样输出见 tmp/lanes/W17-taskface2/proofs/revert-red-and-green.txt）：
// CP1 requestCancel 不真取消任务 ctx → ①红；CP2 cancel 不等 goroutine 收尾 →
// ②③红；CP3 任务 ctx 不脱钩提交请求（复用 r.Context()）→ 断线测试红；
// CP4 去掉 idle→incomplete 映射 → ④红。
package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestTaskFaceCancel 任务面取消验收主测（见文件头注）。
func TestTaskFaceCancel(t *testing.T) {
	// 假上游：建流帧（过提交点）后挂住——只在任务 ctx 被取消（真中止）时退出。
	var (
		upMu   sync.Mutex
		doneAt time.Time
	)
	doneCh := make(chan struct{})
	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "response.created", `{"type":"response.created","response":{"id":"resp_cancel","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":0}`)
		select {
		case <-r.Context().Done():
			upMu.Lock()
			doneAt = time.Now()
			upMu.Unlock()
			close(doneCh)
		case <-time.After(5 * time.Second):
			// 兜底出口（仅反证场景走：cancel 未真中止时——不拖死测试收尾）。
		}
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
	// 等任务真打到上游（避免 cancel 落在 goroutine 启动前——那是另一形态，
	// 语义上仍是 cancelled，但①的「上游 ctx 触发」断言需要上游在飞）。
	waitFor(t, 5*time.Second, func() bool { return up.count() == 1 })

	// cancel：200 + 终态快照 cancelled（handler 等 goroutine 收尾后返回）。
	cancelStart := time.Now()
	resp2 := postJSON(t, srv.URL+"/v1/responses/"+created.ID+"/cancel", "", nil)
	raw2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("cancel status = %d\n%s", resp2.StatusCode, raw2)
	}
	var cancelled map[string]json.RawMessage
	if err := json.Unmarshal(raw2, &cancelled); err != nil {
		t.Fatalf("cancel body: %v\n%s", err, raw2)
	}
	if string(cancelled["status"]) != `"cancelled"` {
		t.Fatalf("cancel status = %s（want cancelled）\n%s", cancelled["status"], raw2)
	}
	if string(cancelled["id"]) != `"`+created.ID+`"` {
		t.Fatalf("cancel id = %s, want %q", cancelled["id"], created.ID)
	}

	// ① 真中止：假上游 ctx 在 1s 内触发（上游调用被真取消，不是等流自然结束）。
	select {
	case <-doneCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("上游调用未在 1s 内被中止（cancel 未真中止上游）")
	}
	upMu.Lock()
	elapsed := doneAt.Sub(cancelStart)
	upMu.Unlock()
	if elapsed > 1*time.Second {
		t.Fatalf("上游中止延迟 = %s（want ≤1s）", elapsed)
	}
	if got := up.count(); got != 1 {
		t.Fatalf("上游尝试次数 = %d, want 1（取消后不重试/不换上游）", got)
	}

	// ② 轮询终态 cancelled（轮询面与 cancel 面同快照）。
	code, body := getJSON(t, srv.URL+"/v1/responses/"+created.ID)
	if code != 200 {
		t.Fatalf("poll status = %d\n%s", code, body)
	}
	var polled map[string]json.RawMessage
	if err := json.Unmarshal(body, &polled); err != nil {
		t.Fatalf("poll body: %v\n%s", err, body)
	}
	if string(polled["status"]) != `"cancelled"` {
		t.Fatalf("poll status = %s（want cancelled）", polled["status"])
	}

	// ③ 重复 cancel 同终态（幂等：同字节快照）。
	resp3 := postJSON(t, srv.URL+"/v1/responses/"+created.ID+"/cancel", "", nil)
	raw3, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()
	if resp3.StatusCode != 200 {
		t.Fatalf("repeat cancel status = %d\n%s", resp3.StatusCode, raw3)
	}
	if !bytes.Equal(raw3, raw2) {
		t.Fatalf("重复 cancel 快照不一致:\n1st %s\n2nd %s", raw2, raw3)
	}

	// ④ idle 场景（fresh 上游 + 服务实例）：假活跃（只发非内容帧）+ idle=1s →
	// 任务终态 incomplete（失败语义表「任务挂住」行；挂住 ≠ 失败）。
	upIdle := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		// 建流帧过提交点（response.created 属守卫排除集——不算内容）。
		sseFrame(w, "response.created", `{"type":"response.created","response":{"id":"resp_idle","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":0}`)
		tk := time.NewTicker(100 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-tk.C:
				// 假活跃：只发非内容帧（responses 协议无 ping 帧；response.in_progress
				// 是排除集内等价物）——idle 守卫不 touch，到点触发。
				sseFrame(w, "response.in_progress", `{"type":"response.in_progress","response":{"id":"resp_idle","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":1}`)
			}
		}
	})
	cfg := cfgFor(upIdle.srv.URL, ProtocolResponses, 15)
	cfg.UpstreamIdleTimeoutSeconds = 1
	srv2 := startService(t, cfg)

	idleStart := time.Now()
	respI := postJSON(t, srv2.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","background":true}`, nil)
	rawI, _ := io.ReadAll(respI.Body)
	respI.Body.Close()
	var createdI struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rawI, &createdI); err != nil || createdI.ID == "" {
		t.Fatalf("idle task create: %v\n%s", err, rawI)
	}
	var final map[string]json.RawMessage
	waitFor(t, 5*time.Second, func() bool {
		code, b := getJSON(t, srv2.URL+"/v1/responses/"+createdI.ID)
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
	if string(final["status"]) != `"incomplete"` {
		t.Fatalf("idle 任务终态 = %s（want incomplete）\n%s", final["status"], final)
	}
	if d := time.Since(idleStart); d < 1*time.Second {
		t.Fatalf("idle 终态用时 = %s（1s 守卫未生效？）", d)
	}
	var errObj struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(final["error"], &errObj); err != nil {
		t.Fatalf("idle error: %v\n%s", err, final["error"])
	}
	if errObj.Code != "upstream_idle" || !strings.Contains(errObj.Message, "no content-bearing frame") {
		t.Fatalf("idle error = %+v（want code=upstream_idle + idle 文本）", errObj)
	}
	if got := upIdle.count(); got != 1 {
		t.Fatalf("idle 上游尝试次数 = %d, want 1（提交后不重放）", got)
	}
}

// TestTaskFaceDisconnectKeepsTask 断线不杀任务（Slice 2 钉死；Slice 1 已是的行为）：
// 提交请求的生命周期终结（客户端取消 ctx → 连接中止）不影响已在跑的任务——
// 任务 ctx 与提交请求解耦；断开 ≠ 取消（任务继续到 completed）。
//
// 反证（回退即红）：runTask 复用提交请求 ctx（不脱钩——CP3）→ ServeHTTP 返回即
// ctx 取消 → 任务在慢流中途被杀 → 轮询不收敛 completed（见 proofs）。
func TestTaskFaceDisconnectKeepsTask(t *testing.T) {
	up := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "response.created", `{"type":"response.created","response":{"id":"resp_disc","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":0}`)
		time.Sleep(700 * time.Millisecond) // 断线窗口：任务仍在跑
		sseFrame(w, "response.completed", `{"type":"response.completed","response":{"id":"resp_disc","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5","output":[{"id":"msg_disc","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"kept","annotations":[]}]}],"usage":{"input_tokens":3,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":0},"output_tokens":1,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":4}},"sequence_number":1}`)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses",
		strings.NewReader(`{"model":"gpt-5","input":"hi","background":true}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
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
	// 提交请求生命周期终结：取消请求 ctx（连接中止）——任务不得受影响。
	cancel()

	// 任务必须继续到 completed（断开 ≠ 取消/杀任务）。
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
	if string(final["id"]) != `"`+created.ID+`"` {
		t.Fatalf("terminal id = %s, want %q", final["id"], created.ID)
	}
	if !bytes.Contains(final["output"], []byte("kept")) {
		t.Fatalf("terminal output = %s", final["output"])
	}
}
