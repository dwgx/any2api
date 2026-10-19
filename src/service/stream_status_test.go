// W84-1（F1）：流式成功尝试必须把上游状态落账。
//
// 背景：A2A-L19-W83-1 真流量实测 5 个 200 流式请求在 admin 请求台账里 status=0
// （attemptRecord.status 只在非流式成功面与上游 >=400 面赋值）。本测试用最小
// SSE 流（`data: [DONE]`）走真 serve 收尾路径，同时断言 per-request 记账日志
// 与 admin 请求 ring 两个出口。
package service

import (
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/dwgx/any2api/translate/registry"
)

// TestStreamSuccessRecordsStatus 假上游 200 + text/event-stream + `data: [DONE]`：
//   - 下游 200；
//   - attempt 记录 status=200 / delivered=true / failed=false / end_reason=completed；
//   - admin 请求 ring 行 status=200 / delivered=true / failed=false / stream=true。
func TestStreamSuccessRecordsStatus(t *testing.T) {
	var logBuf syncLogBuffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	})
	s, srv := startServiceWith(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("下游 status = %d（want 200）body=%s", resp.StatusCode, raw)
	}
	if up.count() != 1 {
		t.Fatalf("上游调用数 = %d（want 1）", up.count())
	}

	// ① per-request attempt 记录（attempts.go 的收尾日志行）。
	waitFor(t, 2*time.Second, func() bool { return strings.Contains(logBuf.String(), `"end_reason"`) })
	rec := parseRequestRecord(t, logBuf.String())
	if len(rec.Attempts) != 1 {
		t.Fatalf("attempt 数 = %d（want 1）: %+v", len(rec.Attempts), rec.Attempts)
	}
	a := rec.Attempts[0]
	if a.Status != http.StatusOK {
		t.Fatalf("attempt status = %d（want 200；F1 回归：成功流此前落 0）", a.Status)
	}
	if !a.Delivered || a.Failed || a.Disconnected || a.EndReason != endReasonCompleted {
		t.Fatalf("attempt 归因 = %+v（want delivered=true failed=false disconnected=false end_reason=%q）",
			a, endReasonCompleted)
	}

	// ② admin 请求 ring（真 serve 收尾 → recordRequestRing）。
	rid := resp.Header.Get("request-id")
	if rid == "" || rid != rec.Req {
		t.Fatalf("响应 request-id = %q / 记账 req = %q（want 同源非空）", rid, rec.Req)
	}
	waitFor(t, 2*time.Second, func() bool { _, ok := s.adminRequests.find(rid); return ok })
	row, ok := s.adminRequests.find(rid)
	if !ok {
		t.Fatalf("ring 缺请求行 %q（size=%d）", rid, s.adminRequests.size())
	}
	if row.Status != http.StatusOK {
		t.Fatalf("ring status = %d（want 200）", row.Status)
	}
	if !row.Delivered || row.Failed || !row.Stream || row.Attempts != 1 || row.Outcome != endReasonCompleted {
		t.Fatalf("ring 行 = %+v（want delivered=true failed=false stream=true attempts=1 outcome=%q）",
			row, endReasonCompleted)
	}
	t.Logf("attempt: status=%d delivered=%v failed=%v end_reason=%s; ring: status=%d delivered=%v failed=%v stream=%v",
		a.Status, a.Delivered, a.Failed, a.EndReason, row.Status, row.Delivered, row.Failed, row.Stream)
}

// TestStreamPostCommitFailureKeepsStatus 提交后截断（有产出、无终止证据 EOF）：
// 失败由 failed/end_reason 承载，status 保持上游 200（W84-1 Change ①「不覆盖」）。
func TestStreamPostCommitFailureKeepsStatus(t *testing.T) {
	var logBuf syncLogBuffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		// 仅一帧内容 delta，无 finish_reason / 无 [DONE] → EOF = 截断流。
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	})
	s, srv := startServiceWith(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("下游 status = %d（want 200：提交后失败仍以流内错误帧收尾）body=%s", resp.StatusCode, raw)
	}

	waitFor(t, 2*time.Second, func() bool { return strings.Contains(logBuf.String(), `"end_reason"`) })
	rec := parseRequestRecord(t, logBuf.String())
	if len(rec.Attempts) != 1 {
		t.Fatalf("attempt 数 = %d（want 1）: %+v", len(rec.Attempts), rec.Attempts)
	}
	a := rec.Attempts[0]
	if a.Status != http.StatusOK || !a.Failed || a.Delivered || a.EndReason != endReasonUpstreamError {
		t.Fatalf("attempt = %+v（want status=200 failed=true delivered=false end_reason=%q）",
			a, endReasonUpstreamError)
	}

	rid := resp.Header.Get("request-id")
	waitFor(t, 2*time.Second, func() bool { _, ok := s.adminRequests.find(rid); return ok })
	row, ok := s.adminRequests.find(rid)
	if !ok {
		t.Fatalf("ring 缺请求行 %q（size=%d）", rid, s.adminRequests.size())
	}
	if row.Status != http.StatusOK || !row.Failed || row.Delivered || row.Outcome != endReasonUpstreamError {
		t.Fatalf("ring 行 = %+v（want status=200 failed=true delivered=false outcome=%q）",
			row, endReasonUpstreamError)
	}
	t.Logf("post-commit: attempt status=%d failed=%v delivered=%v end_reason=%s; ring status=%d failed=%v delivered=%v",
		a.Status, a.Failed, a.Delivered, a.EndReason, row.Status, row.Failed, row.Delivered)
}

// TestUpstreamErrorFrameMarksFailed W87-1（D1）：同协议直通流里的上游**流内错误帧**
// 必须落成失败归因（failed=true / delivered=false / end_reason=upstream_error），
// 但字节面零改动（错误帧与前后帧照旧原样转发 = 同协议保真）。
func TestUpstreamErrorFrameMarksFailed(t *testing.T) {
	var logBuf syncLogBuffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":null}]}`+"\n\n")
		_, _ = io.WriteString(w, `data: {"error":{"code":"upstream_stream_interrupted","message":"upstream stream interrupted","type":"server_error"},"type":"error"}`+"\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	})
	s, srv := startServiceWith(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	body := string(raw)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("下游 status = %d（want 200：已提交）body=%s", resp.StatusCode, body)
	}
	// 字节保真：内容帧 + 上游错误帧 + [DONE] 原样到达下游。
	if !strings.Contains(body, `"content":"Hi"`) || !strings.Contains(body, `"upstream_stream_interrupted"`) {
		t.Fatalf("下游字节面缺帧（want 内容帧 + 上游错误帧原样）:\n%s", body)
	}
	snippet := body
	if len(snippet) > 220 {
		snippet = snippet[:220]
	}
	t.Logf("downstream body (%d bytes, head): %s", len(body), strings.ReplaceAll(snippet, "\n", "\\n"))

	waitFor(t, 2*time.Second, func() bool { return strings.Contains(logBuf.String(), `"end_reason"`) })
	rec := parseRequestRecord(t, logBuf.String())
	if len(rec.Attempts) != 1 {
		t.Fatalf("attempt 数 = %d（want 1）: %+v", len(rec.Attempts), rec.Attempts)
	}
	a := rec.Attempts[0]
	if !a.Failed || a.Delivered || a.EndReason != endReasonUpstreamError || a.Status != http.StatusOK {
		t.Fatalf("attempt = %+v（want failed=true delivered=false end_reason=%q status=200）", a, endReasonUpstreamError)
	}

	rid := resp.Header.Get("request-id")
	waitFor(t, 2*time.Second, func() bool { _, ok := s.adminRequests.find(rid); return ok })
	row, ok := s.adminRequests.find(rid)
	if !ok {
		t.Fatalf("ring 缺请求行 %q（size=%d）", rid, s.adminRequests.size())
	}
	if !row.Failed || row.Delivered || row.Outcome != endReasonUpstreamError || row.Status != http.StatusOK {
		t.Fatalf("ring 行 = %+v（want failed=true delivered=false outcome=%q status=200）", row, endReasonUpstreamError)
	}
	t.Logf("attempt: failed=%v delivered=%v end_reason=%s status=%d; ring: failed=%v delivered=%v outcome=%s status=%d",
		a.Failed, a.Delivered, a.EndReason, a.Status, row.Failed, row.Delivered, row.Outcome, row.Status)
}

// TestCrossProtocolUpstreamErrorFrameMarksFailed W90-1（D1 跨协议半面）：OpenAI chat
// 上游 → Anthropic Messages 入口的**流内错误帧**必须落成失败归因，与同协议
// （W87-1）同口径。pipe.Feed 对合法错误事件**不报错**——它把 IR EventError 转换
// 成下游错误帧字节返回，归因面此前只挂在 Feed 错误上（永不触发），ledger/ring
// 因此落 completed/delivered=true。字节面零改动：错误帧照常转换写出。
//
// 控制组：正常内容 + finish_reason + [DONE] → completed/delivered=true，证明
// 观测器不把正常流误判成错误（判据 = 本帧首次置上 errorSeen 的差分）。
func TestCrossProtocolUpstreamErrorFrameMarksFailed(t *testing.T) {
	const (
		contentFrame = `data: {"choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":null}]}` + "\n\n"
		errorFrame   = `data: {"error":{"code":"upstream_stream_interrupted","message":"upstream stream interrupted","type":"server_error"},"type":"error"}` + "\n\n"
		finishFrame  = `data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n"
		doneFrame    = "data: [DONE]\n\n"
	)
	cases := []struct {
		name          string
		frames        []string
		wantBody      []string
		wantAbsent    []string
		wantFailed    bool
		wantDelivered bool
		wantReason    string
	}{
		{
			name:   "upstream_error_frame",
			frames: []string{contentFrame, errorFrame, doneFrame},
			// 转换后的 Anthropic 错误帧（event: error + data 里的 type=error /
			// 上游 message）；内容帧照旧保留——转换面零改动。
			wantBody:      []string{`"text":"Hi"`, "event: error", `"type":"error"`, `"message":"upstream stream interrupted"`},
			wantFailed:    true,
			wantDelivered: false,
			wantReason:    endReasonUpstreamError,
		},
		{
			name:          "control_normal_stream",
			frames:        []string{contentFrame, finishFrame, doneFrame},
			wantBody:      []string{`"text":"Hi"`, "event: message_stop"},
			wantAbsent:    []string{"event: error"},
			wantFailed:    false,
			wantDelivered: true,
			wantReason:    endReasonCompleted,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var logBuf syncLogBuffer
			prev := log.Writer()
			log.SetOutput(&logBuf)
			defer log.SetOutput(prev)

			up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				for _, f := range tc.frames {
					_, _ = io.WriteString(w, f)
				}
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			})
			s, srv := startServiceWith(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))

			resp := postJSON(t, srv.URL+"/v1/messages",
				`{"model":"gpt-x","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			body := string(raw)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("下游 status = %d（want 200）body=%s", resp.StatusCode, body)
			}
			if up.count() != 1 {
				t.Fatalf("上游调用数 = %d（want 1：流内错误帧不重试/不换上游）", up.count())
			}
			for _, want := range tc.wantBody {
				if !strings.Contains(body, want) {
					t.Fatalf("下游缺 %q（转换/字节面被改动？）:\n%s", want, body)
				}
			}
			for _, absent := range tc.wantAbsent {
				if strings.Contains(body, absent) {
					t.Fatalf("下游不应含 %q:\n%s", absent, body)
				}
			}

			waitFor(t, 2*time.Second, func() bool { return strings.Contains(logBuf.String(), `"end_reason"`) })
			rec := parseRequestRecord(t, logBuf.String())
			if len(rec.Attempts) != 1 {
				t.Fatalf("attempt 数 = %d（want 1）: %+v", len(rec.Attempts), rec.Attempts)
			}
			a := rec.Attempts[0]
			if a.Status != http.StatusOK || a.Failed != tc.wantFailed || a.Delivered != tc.wantDelivered || a.EndReason != tc.wantReason {
				t.Fatalf("attempt = %+v（want status=200 failed=%v delivered=%v end_reason=%q）",
					a, tc.wantFailed, tc.wantDelivered, tc.wantReason)
			}

			rid := resp.Header.Get("request-id")
			if rid == "" || rid != rec.Req {
				t.Fatalf("响应 request-id = %q / 记账 req = %q（want 同源非空）", rid, rec.Req)
			}
			waitFor(t, 2*time.Second, func() bool { _, ok := s.adminRequests.find(rid); return ok })
			row, ok := s.adminRequests.find(rid)
			if !ok {
				t.Fatalf("ring 缺请求行 %q（size=%d）", rid, s.adminRequests.size())
			}
			if row.Status != http.StatusOK || row.Failed != tc.wantFailed || row.Delivered != tc.wantDelivered ||
				row.Outcome != tc.wantReason || !row.Stream || row.Attempts != 1 {
				t.Fatalf("ring 行 = %+v（want status=200 failed=%v delivered=%v outcome=%q stream=true attempts=1）",
					row, tc.wantFailed, tc.wantDelivered, tc.wantReason)
			}
			t.Logf("attempt: status=%d failed=%v delivered=%v end_reason=%s; ring: status=%d failed=%v delivered=%v outcome=%s",
				a.Status, a.Failed, a.Delivered, a.EndReason, row.Status, row.Failed, row.Delivered, row.Outcome)
		})
	}
}

// TestDirectFrameTerminalWebErrorFrames W91-1：两个网页上游协议（claude_web /
// chatgpt_web）的终止判据与错误判据必须**分离**——`message_stop` / `[DONE]` 是
// 正常流末形态（终止帧，不是失败证据），`message_stream_complete` 是尾部帧
// （既非终止也非失败），只有流内 `error` 帧才是上游失败证据。修前这两个格式在
// directFrameTerminal 里没有分支、恒返回 (false,false)：流内 error 帧会漏记
// 失败归因（attempt 落 completed/delivered=true）。
//
// 直接打 (terminalFrame, errorFrame) 两个返回值；归因面接线（failed/end_reason/
// ring）由 W87/W90 的 serve 级测试覆盖，本测试只锁判据。
func TestDirectFrameTerminalWebErrorFrames(t *testing.T) {
	cases := []struct {
		name      string
		format    registry.Format
		event     string
		data      string
		wantTerm  bool
		wantError bool
	}{
		{
			name:      "claude_web_error_frame",
			format:    registry.FormatClaudeWeb,
			event:     "error",
			data:      `{"type":"error","error":{"type":"overloaded_error","message":"busy"}}`,
			wantTerm:  true,
			wantError: true,
		},
		{
			name:      "claude_web_message_stop",
			format:    registry.FormatClaudeWeb,
			event:     "message_stop",
			data:      `{"type":"message_stop"}`,
			wantTerm:  true,
			wantError: false,
		},
		{
			name:      "chatgpt_web_error_frame",
			format:    registry.FormatChatGPTWeb,
			event:     "",
			data:      `{"error":{"message":"busy","type":"server_error"}}`,
			wantTerm:  true,
			wantError: true,
		},
		{
			name:      "chatgpt_web_done",
			format:    registry.FormatChatGPTWeb,
			event:     "",
			data:      `[DONE]`,
			wantTerm:  true,
			wantError: false,
		},
		{
			name:      "chatgpt_web_message_stream_complete",
			format:    registry.FormatChatGPTWeb,
			event:     "",
			data:      `{"type":"message_stream_complete"}`,
			wantTerm:  false,
			wantError: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var term geminiStreamTerminal
			var chat openaiStreamTerminal
			var resp responsesStreamTerminal
			terminal, errFrame := directFrameTerminal(tc.format, tc.event, []byte(tc.data), &term, &chat, &resp)
			if terminal != tc.wantTerm || errFrame != tc.wantError {
				t.Fatalf("directFrameTerminal(%s, event=%q, data=%s) = (terminal=%v, errorFrame=%v), want (%v, %v)",
					tc.format, tc.event, tc.data, terminal, errFrame, tc.wantTerm, tc.wantError)
			}
			t.Logf("directFrameTerminal(%s, event=%q, data=%s) = terminal=%v errorFrame=%v",
				tc.format, tc.event, tc.data, terminal, errFrame)
		})
	}
}
