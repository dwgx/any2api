// W5-27 回归测试（P64B / P71P72 实锤面的锚点）：
//
//  1. 提交点前心跳门——上游「响应头先到、首帧静默 > 心跳间隔」窗口（P64B 探针
//     实锤）：心跳不得在提交点前写字节（CT 不退化 text/plain、零帧 EOF 保留真实
//     状态码）；单元面（recorder）+ e2e 正例 + e2e 反例（反证锚点）。
//  2. 成功面 anthropic-ratelimit-* 转发（官方 C4「每个响应」）：流式 + 非流式；
//     换上游无前次尝试残留泄漏（先清后写）。
//  3. GET /v1/models 模型发现：最小形状（id/max_input_tokens/max_tokens）+
//     betas 头容忍 + 查询串容忍 + 非 GET 405。
//  4. 非流式出口 Content-Type=application/json 三入口锚点（P71P72 硬门：合法
//     JSON/SSE 体配错 CT → 客户端 is_error 且不再回退）。
package service

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// 条目 1：提交点前心跳门
// ---------------------------------------------------------------------------

// 单元面：beat 在提交点前不写任何字节，提交后恢复心跳。反证：去掉 beat 的
// committed 门 → 首段断言红（body 出现 ping 字节）。
func TestSweep10BeatCommittedGate(t *testing.T) {
	rec := httptest.NewRecorder()
	d := newDownstream(rec, registry.FormatClaudeMessages, false)

	if ok := d.beat(time.Millisecond); !ok {
		t.Fatalf("beat pre-commit: ok=false（应报告下游存活）")
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("提交点前写了 %d 字节: %q", rec.Body.Len(), rec.Body.String())
	}
	if d.committed {
		t.Fatalf("beat 不应改变 committed 状态")
	}

	// 提交后：间隔已过 → 写 ping（门只挡提交点前）。
	d.commit()
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream; charset=utf-8" {
		t.Fatalf("commit CT = %q", ct)
	}
	if ok := d.beat(0); !ok {
		t.Fatalf("beat post-commit: ok=false")
	}
	if !strings.Contains(rec.Body.String(), "event: ping") {
		t.Fatalf("提交后心跳缺失: %q", rec.Body.String())
	}
}

// e2e 正例：上游先发响应头 → 首帧静默 1.5s（心跳 1s 已过一拍）→ 发合规帧。
// 断言：CT 不退化（官方 C1）、首字节是 message_start（心跳未抢先）、流字节与
// 预期帧逐字节一致（任何预提交 ping 都会破坏该等式）。
func TestSweep10HeartbeatPreCommitWindowOK(t *testing.T) {
	startPayload := `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}`
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(1500 * time.Millisecond) // > 心跳间隔（1s）：预提交窗口内必有一拍
		sseFrame(w, "message_start", startPayload)
		sseFrame(w, "message_stop", `{"type":"message_stop"}`)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 1))

	resp := postJSON(t, srv.URL+"/v1/messages",
		`{"model":"claude-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d body=%q", resp.StatusCode, raw)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream; charset=utf-8" {
		t.Fatalf("CT = %q（提交点前心跳抢先 = 退化为 text/plain，违反官方 C1）", ct)
	}
	want := "event: message_start\ndata: " + startPayload + "\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	if string(raw) != want {
		t.Fatalf("流字节不一致（预提交 ping 抢先或帧序偏差）:\n got %q\nwant %q", raw, want)
	}
}

// e2e 反例（反证锚点）：上游先发响应头 → 静默 1.5s → 零帧 EOF（预提交失败）。
// 断言：真实状态码保留（502 而非退化 200）、CT=application/json、body 无 ping
// 字节、错误信封为 claude 形态。反证：去掉 committed 门 → 状态码退化为 200 且
// body 以 ping 开头 → 本测试红。
func TestSweep10HeartbeatPreCommitZeroFrameEOF(t *testing.T) {
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(1500 * time.Millisecond)
		// 直接返回：零帧 EOF（预提交失败）。
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 1))

	resp := postJSON(t, srv.URL+"/v1/messages",
		`{"model":"claude-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d（预提交失败应保留真实 502，不退化 200）body=%q", resp.StatusCode, raw)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("CT = %q", ct)
	}
	if strings.Contains(string(raw), "event: ping") {
		t.Fatalf("错误体混入预提交 ping 字节: %q", raw)
	}
	var env struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("错误信封不是 JSON: %v body=%q", err, raw)
	}
	if env.Type != "error" || env.Error.Type != "api_error" || !strings.Contains(env.Error.Message, "produced no events") {
		t.Fatalf("错误信封形态偏差: %+v body=%q", env, raw)
	}
}

// ---------------------------------------------------------------------------
// 条目 2：成功面 ratelimit 头转发
// ---------------------------------------------------------------------------

// 流式成功面：上游 200 SSE 携带 anthropic-ratelimit-unified-* 族 → 下游逐值转发。
func TestSweep10RatelimitForwardedStreaming(t *testing.T) {
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("anthropic-ratelimit-unified-status", "allowed")
		w.Header().Set("anthropic-ratelimit-unified-reset", "1735689600")
		w.Header().Set("anthropic-ratelimit-unified-5h-utilization", "0.42")
		claudeTextStream(w)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	resp := postJSON(t, srv.URL+"/v1/messages",
		`{"model":"claude-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	for k, want := range map[string]string{
		"anthropic-ratelimit-unified-status":         "allowed",
		"anthropic-ratelimit-unified-reset":          "1735689600",
		"anthropic-ratelimit-unified-5h-utilization": "0.42",
	} {
		if got := resp.Header.Get(k); got != want {
			t.Fatalf("流式成功面 %s = %q, want %q（官方 C4：每个响应转发）", k, got, want)
		}
	}
}

// 非流式成功面：上游 200 JSON 携带配额头 → 下游转发（且体逐字节直通）。
func TestSweep10RatelimitForwardedNonStreaming(t *testing.T) {
	msg := `{"id":"msg_1","type":"message","role":"assistant","model":"claude-x","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}`
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("anthropic-ratelimit-unified-status", "allowed_warning")
		w.Header().Set("anthropic-ratelimit-unified-reset", "1735693200")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(msg))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	resp := postJSON(t, srv.URL+"/v1/messages",
		`{"model":"claude-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d body=%q", resp.StatusCode, raw)
	}
	if got := resp.Header.Get("anthropic-ratelimit-unified-status"); got != "allowed_warning" {
		t.Fatalf("非流式成功面 ratelimit-status = %q, want allowed_warning（官方 C4：每个响应转发）", got)
	}
	if got := resp.Header.Get("anthropic-ratelimit-unified-reset"); got != "1735693200" {
		t.Fatalf("非流式成功面 ratelimit-reset = %q", got)
	}
	if string(raw) != msg {
		t.Fatalf("同协议直通体不一致:\n got %q\nwant %q", raw, msg)
	}
}

// 换上游无残留：首上游 200 SSE 带 ratelimit 头后零帧 EOF（预提交失败）→ 第二台
// 上游（不带该族头）成功。断言下游**无**首上游残留头（先清后写）。反证：去掉
// copyRateLimitHeaders 的清理循环 → 本测试红（残留泄漏）。
func TestSweep10RatelimitNoLeakAcrossFailover(t *testing.T) {
	bad := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("anthropic-ratelimit-unified-status", "allowed")
		w.WriteHeader(200)
		// 零帧 EOF：预提交失败 → 换上游。
	})
	good := goodClaudeUpstream(t)
	_, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	resp := postJSON(t, srv.URL+"/v1/messages",
		`{"model":"claude-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 || !strings.Contains(string(raw), "Hello ") {
		t.Fatalf("status=%d body=%q（期望换上游后成功）", resp.StatusCode, raw)
	}
	if bad.count() != 1 || good.count() != 1 {
		t.Fatalf("hits: bad=%d good=%d（期望 1/1）", bad.count(), good.count())
	}
	if got := resp.Header.Get("anthropic-ratelimit-unified-status"); got != "" {
		t.Fatalf("首上游残留 ratelimit 头泄漏到最终响应: %q", got)
	}
}

// ---------------------------------------------------------------------------
// 条目 3：GET /v1/models 模型发现
// ---------------------------------------------------------------------------

// 最小发现面：200 + application/json + data[] 形状（id/max_input_tokens/
// max_tokens）+ betas 头与查询串容忍；非 GET → 405。
func TestSweep10ModelsDiscovery(t *testing.T) {
	srv := startService(t, cfgFor("http://127.0.0.1:1", ProtocolAnthropic, 15))

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/models?limit=1000", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("anthropic-beta", "oauth-2025-04-20,claude-code-20250219") // betas 头容忍
	req.Header.Set("Authorization", "Bearer sk-ant-fake")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d body=%q", resp.StatusCode, raw)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("CT = %q", ct)
	}
	var doc struct {
		Data []struct {
			ID             string `json:"id"`
			MaxInputTokens int    `json:"max_input_tokens"`
			MaxTokens      int    `json:"max_tokens"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode: %v body=%q", err, raw)
	}
	if len(doc.Data) == 0 {
		t.Fatalf("data[] 为空: %q", raw)
	}
	for _, m := range doc.Data {
		if !strings.Contains(m.ID, "claude") {
			t.Fatalf("id 不含 claude（CC 发现过滤会丢弃）: %q", m.ID)
		}
		if m.MaxInputTokens < 100000 {
			t.Fatalf("%s max_input_tokens = %d（< 1e5 不会作为窗口覆盖值使用）", m.ID, m.MaxInputTokens)
		}
		if m.MaxTokens <= 0 {
			t.Fatalf("%s max_tokens = %d", m.ID, m.MaxTokens)
		}
	}

	presp := postJSON(t, srv.URL+"/v1/models", `{}`, nil)
	defer presp.Body.Close()
	if presp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /v1/models status = %d, want 405", presp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 条目 4：非流式出口 Content-Type 硬门锚点（三协议入口）
// ---------------------------------------------------------------------------

// P71P72 实测：客户端要求回退（非流式）响应必须 application/json——合法 JSON
// 体配 text/plain、或合法 SSE 体配 text/event-stream，客户端一律 is_error 且
// 不再回退。本测试锚定我们非流式出口三入口的 CT（同协议直通，体逐字节一致）。
func TestSweep10NonStreamingContentTypeAnchors(t *testing.T) {
	claudeMsg := `{"id":"msg_1","type":"message","role":"assistant","model":"claude-x","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}`
	chatMsg := `{"id":"chatcmpl-1","object":"chat.completion","created":1741293000,"model":"gpt-x","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`
	respMsg := `{"id":"resp_1","object":"response","created_at":1741293000,"status":"completed","model":"gpt-x","output":[{"type":"message","id":"msg_1","status":"completed","role":"assistant","content":[{"type":"output_text","text":"hi","annotations":[]}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`

	cases := []struct {
		name    string
		proto   Protocol
		path    string
		reqBody string
		upBody  string
	}{
		{"claude_entry", ProtocolAnthropic, "/v1/messages",
			`{"model":"claude-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`, claudeMsg},
		{"openai_chat_entry", ProtocolOpenAIChat, "/v1/chat/completions",
			`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, chatMsg},
		{"responses_entry", ProtocolResponses, "/v1/responses",
			`{"model":"gpt-x","input":"hi"}`, respMsg},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				_, _ = w.Write([]byte(tc.upBody))
			})
			srv := startService(t, cfgFor(up.srv.URL, tc.proto, 15))

			resp := postJSON(t, srv.URL+tc.path, tc.reqBody, nil)
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)

			if resp.StatusCode != 200 {
				t.Fatalf("status = %d body=%q", resp.StatusCode, raw)
			}
			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Fatalf("非流式 CT = %q（P71P72 硬门：必须 application/json）", ct)
			}
			if string(raw) != tc.upBody {
				t.Fatalf("同协议直通体不一致:\n got %q\nwant %q", raw, tc.upBody)
			}
		})
	}
}
