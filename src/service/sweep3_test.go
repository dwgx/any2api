// W5-18（L19-sweep3）服务壳测试：
// 条目 1（gemini 入口面：路径模型名/动作/流位/错误信封）、
// 条目 2（批量诊断汇消费）、条目 4（per-attempt usage 记账）。
package service

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// W5-18 ①：gemini 入口面（POST /v1beta/models/{model}:{action}）
// ---------------------------------------------------------------------------

// TestE2EGeminiEntryNonStreaming gemini 入口非流式（同协议直通）：
// 路径模型名 + 动作 → 上游端点路径；请求体与响应体同协议原样（字节保真）；
// 鉴权头 x-goog-api-key。
func TestE2EGeminiEntryNonStreaming(t *testing.T) {
	up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(geminiTextResponse))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

	reqBody := `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`
	resp := postJSON(t, srv.URL+"/v1beta/models/gemini-2.5-flash:generateContent", reqBody, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	path, query, key, upBody, hits := up.request()
	if path != "/v1beta/models/gemini-2.5-flash:generateContent" || query != "" {
		t.Fatalf("upstream url = %q?%q", path, query)
	}
	if key != "test-key" {
		t.Fatalf("x-goog-api-key = %q", key)
	}
	if hits != 1 {
		t.Fatalf("upstream hits = %d", hits)
	}
	if string(upBody) != reqBody {
		t.Fatalf("同协议直通：上游请求体必须原样\n got=%s\nwant=%s", upBody, reqBody)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"candidates"`) || !strings.Contains(string(body), `"Hi there"`) {
		t.Fatalf("下游必须是 gemini 响应原文（同协议直通）:\n%s", body)
	}
}

// TestE2EGeminiEntryStreaming gemini 入口流式（`:streamGenerateContent?alt=sse`；
// 请求体**无** stream 字段——流位来自路径动作）：上游收到 SSE Accept 与
// alt=sse 端点，下游拿到 SSE 帧。
func TestE2EGeminiEntryStreaming(t *testing.T) {
	var mu = make(chan string, 1)
	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case mu <- r.Header.Get("Accept"):
		default:
		}
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"Hello "}]}}]}`)
		// 终止帧（FD1：直通流缺 finishReason 判截断——mock 保持良构）。
		sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"world"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":11,"candidatesTokenCount":2}}`)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

	resp := postJSON(t, srv.URL+"/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse",
		`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("下游 content-type = %q（路径动作应判流）", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"Hello "`) || !strings.Contains(string(body), `"world"`) {
		t.Fatalf("下游不是 SSE 帧:\n%s", body)
	}
	if got := <-mu; got != "text/event-stream" {
		t.Fatalf("上游 Accept = %q（want text/event-stream）", got)
	}
}

// TestE2EGeminiEntryCrossProtocol gemini 入口 → openai 上游（跨格式）：
// 路径模型名注入上游请求体（gemini 请求体无 model 字段——Options.Model 只填空），
// 响应经 openai→gemini 转换回 gemini 信封。
func TestE2EGeminiEntryCrossProtocol(t *testing.T) {
	var gotBody []byte
	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"c1","object":"chat.completion","created":1,"model":"gemini-2.5-flash",
			"choices":[{"index":0,"message":{"role":"assistant","content":"Hi"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))

	resp := postJSON(t, srv.URL+"/v1beta/models/gemini-2.5-flash:generateContent",
		`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if !strings.Contains(string(gotBody), `"model":"gemini-2.5-flash"`) {
		t.Fatalf("路径态模型名未注入上游请求体:\n%s", gotBody)
	}
	if !strings.Contains(string(gotBody), `"messages"`) {
		t.Fatalf("请求面未转成 openai chat:\n%s", gotBody)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"candidates"`) || !strings.Contains(string(body), `"Hi"`) {
		t.Fatalf("下游必须是 gemini 信封（openai→gemini 响应转换）:\n%s", body)
	}
}

// TestGeminiEntryPathRejections 入口面拒收路径：未知动作 / 缺动作 / 畸形模型名 /
// 非 POST / 非 JSON 体——错误信封按 gemini 形态（Google 信封 code/message/status），
// 且绝不打上游（hits=0）。
func TestGeminiEntryPathRejections(t *testing.T) {
	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(geminiTextResponse))
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolGemini, 15))
	body := `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`

	cases := []struct {
		name    string
		path    string
		method  string
		body    string
		status  int
		status1 string // Google status 字段
	}{
		{"unknown_action", "/v1beta/models/gemini-2.5-flash:countTokens", http.MethodPost, body, 404, "NOT_FOUND"},
		{"missing_action", "/v1beta/models/gemini-2.5-flash", http.MethodPost, body, 404, "NOT_FOUND"},
		{"bad_model_whitespace", "/v1beta/models/gemini%20x:generateContent", http.MethodPost, body, 400, "INVALID_ARGUMENT"},
		{"get_not_allowed", "/v1beta/models/gemini-2.5-flash:generateContent", http.MethodGet, "", 405, "INVALID_ARGUMENT"},
		{"non_json_body", "/v1beta/models/gemini-2.5-flash:generateContent", http.MethodPost, "not json", 400, "INVALID_ARGUMENT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var req *http.Request
			var err error
			if tc.method == http.MethodGet {
				req, err = http.NewRequest(http.MethodGet, srv.URL+tc.path, nil)
			} else {
				req, err = http.NewRequest(http.MethodPost, srv.URL+tc.path, strings.NewReader(tc.body))
				req.Header.Set("Content-Type", "application/json")
			}
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d（want %d）\n%s", resp.StatusCode, tc.status, raw)
			}
			if !strings.Contains(string(raw), `"error"`) || !strings.Contains(string(raw), `"status":"`+tc.status1+`"`) {
				t.Fatalf("错误信封不是 Google 形态:\n%s", raw)
			}
		})
	}
	if hits := up.count(); hits != 0 {
		t.Fatalf("拒收路径不得打上游（hits=%d）", hits)
	}
}

// ---------------------------------------------------------------------------
// W5-18 ②：批量诊断汇的服务壳消费
// ---------------------------------------------------------------------------

// TestE2EBatchDiagnosticsConsumed 非流式跨格式转换的批量诊断：请求面丢弃计数与
// 响应面映射计数进 Diagnostics + 一行日志（口径对齐流式面的 pipe diagnostics）。
func TestE2EBatchDiagnosticsConsumed(t *testing.T) {
	var logBuf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	// ① 请求面：claude 入口 → openai 上游，assistant 思考块（无签名）被丢弃。
	upChat := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"c1","object":"chat.completion","created":1,"model":"gpt-x",
			"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	})
	s1, srv1 := startServiceWith(t, cfgFor(upChat.srv.URL, ProtocolOpenAIChat, 15))
	resp := postJSON(t, srv1.URL+"/v1/messages",
		`{"model":"claude-x","max_tokens":16,"messages":[
			{"role":"assistant","content":[{"type":"thinking","thinking":"ponder","signature":""},{"type":"text","text":"answer"}]},
			{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	d1 := s1.Diagnostics()
	if d1.BatchDroppedThinking != 1 || d1.BatchReasoningMapped != 0 {
		t.Fatalf("请求面批量诊断 = %+v（want dropped=1 mapped=0）", d1)
	}
	if !strings.Contains(logBuf.String(), "batch diagnostics (request face): dropped_thinking=1 reasoning_mapped=0 signature_dropped=0") {
		t.Fatalf("请求面批量诊断日志行缺失:\n%s", logBuf.String())
	}

	// ② 响应面：openai 入口 ← claude 上游（非流式），思考文本落 reasoning_content。
	upClaude := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-x",
			"content":[{"type":"thinking","thinking":"ponder","signature":"sig"},{"type":"text","text":"answer"}],
			"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2}}`))
	})
	s2, srv2 := startServiceWith(t, cfgFor(upClaude.srv.URL, ProtocolAnthropic, 15))
	logBuf.Reset()
	resp2 := postJSON(t, srv2.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp2.Body.Close()
	raw, _ := io.ReadAll(resp2.Body)
	if resp2.StatusCode != 200 || !strings.Contains(string(raw), `"reasoning_content":"ponder"`) {
		t.Fatalf("status=%d body=\n%s", resp2.StatusCode, raw)
	}
	d2 := s2.Diagnostics()
	if d2.BatchReasoningMapped != 1 || d2.BatchDroppedThinking != 0 {
		t.Fatalf("响应面批量诊断 = %+v（want dropped=0 mapped=1）", d2)
	}
	if !strings.Contains(logBuf.String(), "batch diagnostics (response face): dropped_thinking=0 reasoning_mapped=1 signature_dropped=1") {
		t.Fatalf("响应面批量诊断日志行缺失:\n%s", logBuf.String())
	}
}

// ---------------------------------------------------------------------------
// W5-18 ④：per-attempt usage 记账
// ---------------------------------------------------------------------------

// TestE2EAttemptUsageAccountingStreaming 流式换上游：每次尝试的 usage 都进
// per-attempt 面（失败尝试 11/0 + 成功尝试 11/2），与 W5-15 的预提交回收面共存；
// 下游可见流不被改动（不合并——仍是成功尝试的 11/2）。
func TestE2EAttemptUsageAccountingStreaming(t *testing.T) {
	var logBuf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	bad := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":11,"output_tokens":0}}`)
	})
	good := goodClaudeUpstream(t)
	s, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"claude-x","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(raw), `"prompt_tokens":11`) {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}
	d := s.Diagnostics()
	// per-attempt 面：两次尝试各一条记录（失败 11/0、成功 11/2）。
	if d.AttemptUsageRecords != 2 || d.AttemptUsageInTokens != 22 || d.AttemptUsageOutTokens != 2 {
		t.Fatalf("per-attempt 记账 = %+v（want records=2 in=22 out=2）", d)
	}
	if d.AttemptUsageFailedRecords != 1 || d.AttemptUsageFailedInTokens != 11 || d.AttemptUsageFailedOutTokens != 0 {
		t.Fatalf("per-attempt 失败子集 = %+v（want records=1 in=11 out=0）", d)
	}
	// W5-15 回收面共存（预提交失败 + 已收 usage）。
	if d.PartialUsageAttempts != 1 || d.PartialUsageInTokens != 11 || d.PartialUsageOutTokens != 0 {
		t.Fatalf("预提交回收面 = %+v（want 1/11/0）", d)
	}
	// 每次尝试一行日志（attempt 序号 + failed 归因）。
	logs := logBuf.String()
	if !strings.Contains(logs, "upstream bad attempt 1 usage observed (failed=true): in=11 out=0") {
		t.Fatalf("失败尝试日志行缺失:\n%s", logs)
	}
	if !strings.Contains(logs, "upstream good attempt 2 usage observed (failed=false): in=11 out=2") {
		t.Fatalf("成功尝试日志行缺失:\n%s", logs)
	}
}

// TestE2EAttemptUsageAccountingNonStreaming 非流式尝试的 usage 也进 per-attempt 面
// （整段响应体当一帧观测）；下游响应体不被改动。
func TestE2EAttemptUsageAccountingNonStreaming(t *testing.T) {
	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"c1","object":"chat.completion","created":1,"model":"gpt-x",
			"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`))
	})
	s, srv := startServiceWith(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d\n%s", resp.StatusCode, raw)
	}
	// 同协议直通：下游响应体原样（usage 7/3 在）。
	if !strings.Contains(string(raw), `"prompt_tokens":7`) || !strings.Contains(string(raw), `"completion_tokens":3`) {
		t.Fatalf("下游响应体被改动:\n%s", raw)
	}
	d := s.Diagnostics()
	if d.AttemptUsageRecords != 1 || d.AttemptUsageInTokens != 7 || d.AttemptUsageOutTokens != 3 {
		t.Fatalf("非流式 per-attempt 记账 = %+v（want records=1 in=7 out=3）", d)
	}
	if d.AttemptUsageFailedRecords != 0 || d.PartialUsageAttempts != 0 {
		t.Fatalf("成功尝试不得进失败面 = %+v", d)
	}
}

// 编译期护栏：registry 常量在测试内被引用（入口格式断言用）。
var _ = registry.FormatGeminiGenerate
