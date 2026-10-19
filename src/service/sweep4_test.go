// W5-19（L19-sweep4）服务壳测试：
// 条目 1（Vertex 形态入口）、条目 2（非 alt=sse JSON 数组流）、
// 条目 4（per-request attempt 记录 + 下游断开面）、条目 6（?key= 语义）、
// 条目 7（入口动作解析合并）。
package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// W5-19 ①：Vertex 形态入口
// ---------------------------------------------------------------------------

// TestE2EGeminiVertexEntry Vertex 形态（`/v1beta1/projects/{p}/locations/{l}/
// publishers/google/models/{m}:{action}` 与 `/v1` 同形）：路径段解析出模型名/
// 动作，p/l 段只做非空守卫（上游端点仍由配置拼）；非流式 + 流式（alt=sse）+ 拒收。
func TestE2EGeminiVertexEntry(t *testing.T) {
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(geminiTextResponse))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

	reqBody := `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`
	resp := postJSON(t, srv.URL+"/v1beta1/projects/proj-1/locations/us-central1/publishers/google/models/gemini-2.5-flash:generateContent", reqBody, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d\n%s", resp.StatusCode, raw)
	}
	path, query, key, upBody, hits := up.request()
	if path != "/v1beta/models/gemini-2.5-flash:generateContent" || query != "" {
		t.Fatalf("上游端点 = %q?%q（want 配置拼的裸模型名端点；p/l 段不进上游）", path, query)
	}
	if key != "test-key" || hits != 1 {
		t.Fatalf("key=%q hits=%d", key, hits)
	}
	if string(upBody) != reqBody {
		t.Fatalf("同协议直通：上游请求体必须原样\n got=%s", upBody)
	}
	if !strings.Contains(string(raw), `"Hi there"`) {
		t.Fatalf("下游必须是 gemini 响应原文:\n%s", raw)
	}

	// /v1 形态 + 流式（alt=sse → SSE 输出）。
	upStream := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		// 终止帧（FD1：直通流缺 finishReason 判截断——mock 保持良构）。
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"Hi"}]},"finishReason":"STOP"}],"responseId":"r1"}`)
	})
	srv2 := startService(t, cfgFor(upStream.srv.URL, ProtocolGemini, 15))
	resp2 := postJSON(t, srv2.URL+"/v1/projects/p/locations/l/publishers/google/models/gemini-2.5-flash:streamGenerateContent?alt=sse", reqBody, nil)
	raw2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("status = %d\n%s", resp2.StatusCode, raw2)
	}
	if !strings.Contains(string(raw2), `"text":"Hi"`) {
		t.Fatalf("流式正文丢失:\n%s", raw2)
	}
	p2, q2, _, _, _ := upStream.request()
	if p2 != "/v1beta/models/gemini-2.5-flash:streamGenerateContent" || q2 != "alt=sse" {
		t.Fatalf("流式上游端点 = %q?%q", p2, q2)
	}

	// 拒收：段序不符 / 空 location / 未知动作——Google 信封 + 不打上游。
	for _, tc := range []struct {
		name   string
		path   string
		status string
	}{
		{"vertex_missing_segments", "/v1beta1/projects/p/locations/us/models/gemini-2.5-flash:generateContent", "NOT_FOUND"},
		{"vertex_bad_literal", "/v1beta1/projects/p/locations/us/foo/google/models/gemini-2.5-flash:generateContent", "NOT_FOUND"},
		{"unknown_action", "/v1beta1/projects/p/locations/us/publishers/google/models/gemini-2.5-flash:countTokens", "NOT_FOUND"},
		{"bad_model_whitespace", "/v1beta1/projects/p/locations/us/publishers/google/models/gemini%20x:generateContent", "INVALID_ARGUMENT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := postJSON(t, srv.URL+tc.path, reqBody, nil)
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if !strings.Contains(string(raw), `"error"`) || !strings.Contains(string(raw), `"status":"`+tc.status+`"`) {
				t.Fatalf("错误信封不是 Google 形态（status=%d）:\n%s", resp.StatusCode, raw)
			}
		})
	}
	if _, _, _, _, hits := up.request(); hits != 1 {
		t.Fatalf("拒收路径不得打上游（hits=%d）", hits)
	}
}

// ---------------------------------------------------------------------------
// W5-19 ②：非 alt=sse 的 JSON 数组流
// ---------------------------------------------------------------------------

// TestE2EGeminiJSONArrayStream `:streamGenerateContent` 不带 `alt=sse` → 下游按
// 官方 JSON 数组输出（`[` 元素 `]`；Content-Type: application/json）；数组形态
// 不插 SSE 心跳（上游静默 > 心跳间隔也不得出现 `: keepalive`）；带 `alt=sse`
// 仍走 SSE（对照由既有 TestE2EGeminiEntryStreaming 锁）。
func TestE2EGeminiJSONArrayStream(t *testing.T) {
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"He"}]}}],"responseId":"r1"}`)
		time.Sleep(1200 * time.Millisecond) // 上游静默 > 心跳（1s）——数组形态不得插帧
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"llo"}]},"finishReason":"STOP"}],"responseId":"r1","usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":2}}`)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 1)) // 心跳 1s

	resp := postJSON(t, srv.URL+"/v1beta/models/gemini-2.5-flash:streamGenerateContent",
		`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d\n%s", resp.StatusCode, raw)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q（want application/json）", ct)
	}
	if strings.Contains(string(raw), "data:") || strings.Contains(string(raw), ": keepalive") || strings.Contains(string(raw), "event:") {
		t.Fatalf("数组形态含 SSE 痕迹（含心跳）:\n%s", raw)
	}
	var arr []struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(raw, &arr); err != nil {
		t.Fatalf("下游不是合法 JSON 数组: %v\n%s", err, raw)
	}
	if len(arr) != 2 {
		t.Fatalf("数组元素数 = %d（want 2）:\n%s", len(arr), raw)
	}
	if got := arr[0].Candidates[0].Content.Parts[0].Text; got != "He" {
		t.Fatalf("元素 0 文本 = %q", got)
	}
	if got := arr[1].Candidates[0].Content.Parts[0].Text; got != "llo" {
		t.Fatalf("元素 1 文本 = %q", got)
	}
	// 上游请求形态不变（仍带 alt=sse——数组是下游形态）。
	if p, q, _, _, _ := up.request(); p != "/v1beta/models/gemini-2.5-flash:streamGenerateContent" || q != "alt=sse" {
		t.Fatalf("上游端点 = %q?%q", p, q)
	}

	// 跨协议：openai 上游 → gemini 入口（无 alt）→ 数组（元素 = 转换后的 gemini 帧）。
	upChat := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{"content":"Hi"}}]}`)
		sseFrame(w, "", `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)
		sseFrame(w, "", `[DONE]`)
	})
	srv2 := startService(t, cfgFor(upChat.srv.URL, ProtocolOpenAIChat, 15))
	resp2 := postJSON(t, srv2.URL+"/v1beta/models/gemini-2.5-flash:streamGenerateContent",
		`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`, nil)
	raw2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("status = %d\n%s", resp2.StatusCode, raw2)
	}
	if err := json.Unmarshal(raw2, &arr); err != nil {
		t.Fatalf("跨协议下游不是合法 JSON 数组: %v\n%s", err, raw2)
	}
	if len(arr) == 0 || !strings.Contains(string(raw2), `"text":"Hi"`) {
		t.Fatalf("跨协议数组元素丢失正文:\n%s", raw2)
	}
	if strings.Contains(string(raw2), "data:") {
		t.Fatalf("跨协议数组含 SSE 痕迹:\n%s", raw2)
	}
}

// ---------------------------------------------------------------------------
// W5-19 ④：per-request attempt 记录 + 下游断开面
// ---------------------------------------------------------------------------

// attemptRecordWire / requestRecordWire per-request 记账 JSON 行（attempts.go 的
// 日志载荷形态；测试侧解析用）。
type attemptRecordWire struct {
	N            int    `json:"n"`
	Upstream     string `json:"upstream"`
	Proto        string `json:"proto"`
	Status       int    `json:"status"`
	Failed       bool   `json:"failed"`
	Disconnected bool   `json:"disconnected"`
	// EndReason/Delivered W36-1 S1/S2 增量（end_reason 省略 = 未达上游的本地失败）。
	EndReason string `json:"end_reason"`
	Delivered bool   `json:"delivered"`
	In        int64  `json:"in"`
	Out       int64  `json:"out"`
	TTFTMs    int64  `json:"ttft_ms"`
	TotalMs   int64  `json:"total_ms"`
}

type requestRecordWire struct {
	Req      string              `json:"req"`
	Entry    string              `json:"entry"`
	Model    string              `json:"model"`
	Stream   bool                `json:"stream"`
	Attempts []attemptRecordWire `json:"attempts"`
}

// syncLogBuffer 并发安全的日志缓冲：handler goroutine 在请求收尾写日志，测试
// 同时轮询读取——直接 bytes.Buffer 在 -race 下是数据竞争（读写无同步）。
type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *syncLogBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

// parseRequestRecord 从日志里取**最后一条** per-request 记账行并解析。
func parseRequestRecord(t *testing.T, logs string) requestRecordWire {
	t.Helper()
	i := strings.LastIndex(logs, "any2api: request ")
	if i < 0 {
		t.Fatalf("per-request 记账行缺失:\n%s", logs)
	}
	line := logs[i:]
	if j := strings.IndexByte(line, '\n'); j >= 0 {
		line = line[:j]
	}
	const marker = "attempts: "
	k := strings.Index(line, marker)
	if k < 0 {
		t.Fatalf("记账行缺 attempts 载荷:\n%s", line)
	}
	var rec requestRecordWire
	if err := json.Unmarshal([]byte(line[k+len(marker):]), &rec); err != nil {
		t.Fatalf("记账 JSON 解析失败: %v\n%s", err, line)
	}
	return rec
}

// TestE2EAttemptRecordList 流式换上游：per-request attempt 记录列表两条
// （失败尝试 failed=true / 成功尝试 failed=false），req id 与响应 request-id 头
// 同源（上游无该头 → 回退值 = 记账 id）；非流式请求同样记（单条 + status）。
func TestE2EAttemptRecordList(t *testing.T) {
	var logBuf syncLogBuffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	bad := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":11,"output_tokens":0}}`)
	})
	good := goodClaudeUpstream(t)
	_, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"claude-x","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(raw), `"prompt_tokens":11`) {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}

	rec := parseRequestRecord(t, logBuf.String())
	if rec.Entry != "openai_chat" || rec.Model != "claude-x" || !rec.Stream {
		t.Fatalf("请求元信息 = %+v", rec)
	}
	if len(rec.Attempts) != 2 {
		t.Fatalf("attempt 记录数 = %d（want 2）: %+v", len(rec.Attempts), rec.Attempts)
	}
	a0, a1 := rec.Attempts[0], rec.Attempts[1]
	if a0.N != 1 || a0.Upstream != "bad" || !a0.Failed || a0.In != 11 || a0.Out != 0 {
		t.Fatalf("失败尝试记录 = %+v（want n=1 bad failed in=11 out=0）", a0)
	}
	if a1.N != 2 || a1.Upstream != "good" || a1.Failed || a1.In != 11 || a1.Out != 2 {
		t.Fatalf("成功尝试记录 = %+v（want n=2 good ok in=11 out=2）", a1)
	}
	if a1.TTFTMs < 0 || a1.TotalMs < 0 {
		t.Fatalf("时长字段为负: %+v", a1)
	}
	// req id 与响应 request-id 头同源（claude 假上游无 request-id 头 → 回退值）。
	if got := resp.Header.Get("request-id"); got == "" || got != rec.Req {
		t.Fatalf("request-id 头 = %q，记账 req = %q（want 同源）", got, rec.Req)
	}

	// 非流式请求：单尝试一条记录（status=200，无 failed/disconnected）。
	logBuf.Reset()
	upChat := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"c1","object":"chat.completion","created":1,"model":"gpt-x",
			"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`))
	})
	_, srv2 := startServiceWith(t, cfgFor(upChat.srv.URL, ProtocolOpenAIChat, 15))
	resp2 := postJSON(t, srv2.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	raw2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp2.StatusCode, raw2)
	}
	rec2 := parseRequestRecord(t, logBuf.String())
	if len(rec2.Attempts) != 1 {
		t.Fatalf("非流式 attempt 记录数 = %d（want 1）", len(rec2.Attempts))
	}
	if b := rec2.Attempts[0]; b.N != 1 || b.Failed || b.Disconnected || b.Status != 200 || b.In != 3 || b.Out != 1 {
		t.Fatalf("非流式尝试记录 = %+v", b)
	}
}

// TestE2EDownstreamDisconnectRecorded 下游断开（客户端取消）的尝试按
// disconnected=true / failed=false 记（不是上游健康信号——与 recordFailure 同
// 纪律）；上游被取消（客户端断开传导）。
func TestE2EDownstreamDisconnectRecorded(t *testing.T) {
	var logBuf syncLogBuffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	upstreamDone := make(chan struct{})
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_d2","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}`)
		select {
		case <-r.Context().Done():
			close(upstreamDone)
		case <-time.After(5 * time.Second):
		}
	})
	s, srv := startServiceWith(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	buf := make([]byte, 64)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatalf("first read: %v", err)
	}
	cancel()
	resp.Body.Close()
	select {
	case <-upstreamDone:
	case <-time.After(3 * time.Second):
		t.Fatalf("上游未被取消（客户端断开未传导）")
	}

	// handler 收尾后记账行出现：disconnected=true 且 failed=false。
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(logBuf.String(), `"disconnected":true`) {
		if time.Now().After(deadline) {
			t.Fatalf("per-request 行未标 disconnected:\n%s", logBuf.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	rec := parseRequestRecord(t, logBuf.String())
	if len(rec.Attempts) != 1 {
		t.Fatalf("attempt 记录数 = %d（want 1；断开不得触发换上游）: %+v", len(rec.Attempts), rec.Attempts)
	}
	if a := rec.Attempts[0]; !a.Disconnected || a.Failed {
		t.Fatalf("下游断开归因错误 = %+v（want disconnected=true failed=false）", a)
	}
	// 上游健康未受罚（无冷却 trip；该上游未记失败）。
	if d := s.Diagnostics(); d.CooldownTrips != 0 {
		t.Fatalf("下游断开误记上游失败: %+v", d)
	}
}

// ---------------------------------------------------------------------------
// W5-19 ⑥：`?key=` 语义（忽略 + 绝不外泄）
// ---------------------------------------------------------------------------

// TestE2EGeminiKeyQueryIgnored 客户端 `?key=` 被忽略：不进上游 URL 查询/头/体，
// 也不回显下游；上游凭据只来自配置。`alt=sse` 是唯一被读取的查询参数。
func TestE2EGeminiKeyQueryIgnored(t *testing.T) {
	const secret = "SECRET-CLIENT-KEY-123"
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(geminiTextResponse))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

	resp := postJSON(t, srv.URL+"/v1beta/models/gemini-2.5-flash:generateContent?key="+secret,
		`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d\n%s", resp.StatusCode, raw)
	}
	path, query, key, upBody, _ := up.request()
	if query != "" {
		t.Fatalf("客户端查询串泄漏到上游: %q", query)
	}
	if key != "test-key" {
		t.Fatalf("上游鉴权头 = %q（want 配置值）", key)
	}
	if strings.Contains(path, secret) || strings.Contains(string(upBody), secret) {
		t.Fatalf("客户端 key 泄漏到上游 URL/体")
	}
	if strings.Contains(string(raw), secret) {
		t.Fatalf("客户端 key 回显到下游响应:\n%s", raw)
	}

	// 流式 + alt=sse：查询串只读 alt（客户端 key 仍不透传）。
	upStream := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		// 终止帧（FD1：直通流缺 finishReason 判截断——mock 保持良构）。
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"Hi"}]},"finishReason":"STOP"}],"responseId":"r1"}`)
	})
	srv2 := startService(t, cfgFor(upStream.srv.URL, ProtocolGemini, 15))
	resp2 := postJSON(t, srv2.URL+"/v1beta/models/gemini-2.5-flash:streamGenerateContent?key="+secret+"&alt=sse",
		`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`, nil)
	raw2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("status = %d\n%s", resp2.StatusCode, raw2)
	}
	if _, q2, _, _, _ := upStream.request(); q2 != "alt=sse" {
		t.Fatalf("流式上游查询串 = %q（want 只含 alt=sse）", q2)
	}
	if strings.Contains(string(raw2), secret) {
		t.Fatalf("客户端 key 回显到流式下游:\n%s", raw2)
	}
}

// ---------------------------------------------------------------------------
// W5-19 ⑦：入口动作解析合并（parseEntryAction 单一解析器）
// ---------------------------------------------------------------------------

// TestParseEntryActionTable 单一解析器覆盖两路径族（gemini API 形态 + Vertex
// 形态）与拒收面；`entryPathStream` 由其派生（同一动作白名单）。
func TestParseEntryActionTable(t *testing.T) {
	cases := []struct {
		name   string
		path   string
		model  string
		action string
		wantOK bool
	}{
		{"gemini_api", "/v1beta/models/gemini-2.5-flash:generateContent", "gemini-2.5-flash", "generateContent", true},
		{"gemini_api_stream", "/v1beta/models/gemini-2.5-flash:streamGenerateContent", "gemini-2.5-flash", "streamGenerateContent", true},
		{"tuned_model", "/v1beta/models/tunedModels/abc:generateContent", "tunedModels/abc", "generateContent", true},
		{"vertex_v1beta1", "/v1beta1/projects/p/locations/us-central1/publishers/google/models/gemini-3:streamGenerateContent", "gemini-3", "streamGenerateContent", true},
		{"vertex_v1", "/v1/projects/p/locations/us/publishers/google/models/gemini-3:generateContent", "gemini-3", "generateContent", true},
		{"vertex_missing_segments", "/v1beta1/projects/p/locations/us/models/gemini-3:generateContent", "", "", false},
		{"vertex_bad_literal", "/v1beta1/projects/p/locations/us/foo/google/models/gemini-3:generateContent", "", "", false},
		{"vertex_empty_project", "/v1beta1/projects//locations/us/publishers/google/models/gemini-3:generateContent", "", "", false},
		{"vertex_empty_location", "/v1/projects/p/locations//publishers/google/models/gemini-3:generateContent", "", "", false},
		{"vertex_extra_segment", "/v1/projects/p/locations/us/publishers/google/models/a/b:generateContent", "", "", false},
		{"unknown_action", "/v1beta/models/m:countTokens", "", "", false},
		{"missing_action", "/v1beta/models/m", "", "", false},
		{"bad_model_dotdot", "/v1beta/models/..:generateContent", "", "", false},
		{"non_gemini_path", "/v1/chat/completions", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model, action, err := parseEntryAction(tc.path)
			if tc.wantOK != (err == nil) {
				t.Fatalf("parseEntryAction(%q) err = %v（wantOK=%v）", tc.path, err, tc.wantOK)
			}
			if tc.wantOK && (model != tc.model || action != tc.action) {
				t.Fatalf("parseEntryAction(%q) = (%q, %q)", tc.path, model, action)
			}
		})
	}

	// entryPathStream 由同一解析器派生：Vertex 流路径同样判真。
	for _, tc := range []struct {
		path string
		want bool
	}{
		{"/v1beta/models/m:streamGenerateContent", true},
		{"/v1beta1/projects/p/locations/us/publishers/google/models/m:streamGenerateContent", true},
		{"/v1/projects/p/locations/us/publishers/google/models/m:generateContent", false},
		{"/v1/chat/completions", false},
		{"/v1beta/models/m:countTokens", false}, // 未支持动作不判流（W5-18 字面后缀判据会误判）
	} {
		if got := entryPathStream(tc.path); got != tc.want {
			t.Fatalf("entryPathStream(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}
