// W5-23 清扫七（条目 1）：三协议（openai_chat / claude_messages / responses）同协议
// 直通的终止判据收尾——判据文档见报告 §判据；本文件是判据的测试锚点：
//
//   - openai chat：保守版终止证据校验（finish_reason / [DONE] / error 任一即终止；
//     零证据 EOF = 截断流 → 错误帧 + [DONE] 收尾）。反证：有任一证据的流逐字节直通。
//   - responses：同款保守校验（终止事件 / error 事件 / [DONE] 任一即终止；零证据
//     EOF = 截断流 → error + response.failed 收尾，续号承接）。
//   - claude_messages：**不**校验（CC B7/B8 条件硬门 + P70 实测容忍矩阵：有内容时
//     缺 stop_reason 被容忍；注入错误帧会把容忍流变硬失败——判据在客户端）。
//     锚点锁「直通字节保真 + 无注入 + 不计失败」。
//
// 装置沿用既有 e2e 风格：httptest 假上游（全本地回环）+ 真服务 + 真 HTTP 客户端。
package service

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// 观测器单元（证据面）
// ---------------------------------------------------------------------------

// TestOpenAIStreamTerminalObserve 证据面 = finish_reason（非 null 非空）/ [DONE] /
// error 对象；finish_reason:null、空串、正文里的字面量不算。
func TestOpenAIStreamTerminalObserve(t *testing.T) {
	var t1 openaiStreamTerminal
	t1.observe([]byte(`{"choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}`))
	t1.observe([]byte(`{"choices":[{"index":0,"delta":{},"finish_reason":""}]}`))
	t1.observe([]byte(`{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	if t1.terminated() {
		t.Fatalf("finish_reason:null/空串、usage chunk 不算终止证据")
	}
	t1.observe([]byte(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`))
	if !t1.terminated() {
		t.Fatalf("finish_reason 非空 = 终止证据")
	}

	var t2 openaiStreamTerminal
	t2.observe([]byte(`[DONE]`))
	if !t2.terminated() {
		t.Fatalf("[DONE] = 传输级终止证据（客户端 SDK 以它判流结束）")
	}

	var t3 openaiStreamTerminal
	t3.observe([]byte(`{"error":{"message":"boom","type":"server_error"}}`))
	if !t3.terminated() {
		t.Fatalf("error 对象 = 终止证据（错误即终止）")
	}

	var t4 openaiStreamTerminal
	t4.observe([]byte(`{"choices":[{"delta":{"content":"the key \"finish_reason\" appears in text"}}]}`))
	t4.observe([]byte(`{"error":null}`))
	if t4.terminated() {
		t.Fatalf("正文里的 finish_reason 字面量 / error:null 不算证据（最小结构体解析）")
	}
}

// TestResponsesStreamTerminalObserve 证据面 = 终止事件（completed/incomplete/
// failed/done 别名）/ error 事件 / [DONE]；内容事件不算。
func TestResponsesStreamTerminalObserve(t *testing.T) {
	for _, typ := range []string{"response.completed", "response.incomplete", "response.failed", "response.done"} {
		var term responsesStreamTerminal
		term.observe([]byte(`{"type":"` + typ + `","response":{"id":"r1"}}`))
		if !term.terminated() {
			t.Fatalf("%s = 终止证据", typ)
		}
	}
	var t1 responsesStreamTerminal
	t1.observe([]byte(`{"type":"response.created","response":{"id":"r1"}}`))
	t1.observe([]byte(`{"type":"response.output_text.delta","delta":"Hi"}`))
	t1.observe([]byte(`{"type":"response.in_progress"}`))
	t1.observe([]byte(`{"no_type":true}`))
	t1.observe([]byte(`not json`))
	if t1.terminated() {
		t.Fatalf("非终止事件 / 无 type / 非 JSON 不算证据")
	}
	t1.observe([]byte(`{"type":"error","code":"server_error","message":"boom"}`))
	if !t1.terminated() {
		t.Fatalf("error 事件 = 终止证据")
	}

	var t2 responsesStreamTerminal
	t2.observe([]byte(`[DONE]`))
	if !t2.terminated() {
		t.Fatalf("[DONE] = 传输级终止证据（兼容层哨兵，既有容忍面）")
	}
}

// ---------------------------------------------------------------------------
// openai chat 直通锚点
// ---------------------------------------------------------------------------

// TestE2EOpenAIPassthroughTermination 同协议直通（openai 入口 × openai 上游）：
//
//   - 零证据 EOF（无 finish_reason、无 [DONE]、无错误）= 截断流 → 错误帧
//     （incomplete_stream「upstream stream closed before finish_reason」）+
//     [DONE] 收尾（vLLM 形态），attempt 记上游失败；
//   - 反证：finish_reason / [DONE] / error 任一证据在 → 逐字节直通、无错误帧。
func TestE2EOpenAIPassthroughTermination(t *testing.T) {
	const frameRole = `{"id":"chatcmpl-w523","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}`
	const frameText = `{"id":"chatcmpl-w523","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":null}]}`
	const frameFinish = `{"id":"chatcmpl-w523","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`
	const frameError = `{"error":{"message":"upstream exploded","type":"server_error"}}`
	const chatBody = `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`

	// stream 假上游：写 raw（逐帧 flush）后返回（干净 EOF）。
	stream := func(t *testing.T, frames ...string) *countUpstream {
		t.Helper()
		return newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			for _, f := range frames {
				sseFrame(w, "", f)
			}
		})
	}
	rawOf := func(frames ...string) string {
		var b strings.Builder
		for _, f := range frames {
			b.WriteString("data: " + f + "\n\n")
		}
		return b.String()
	}

	t.Run("truncated_error_frame", func(t *testing.T) {
		var logBuf syncLogBuffer
		prev := log.Writer()
		log.SetOutput(&logBuf)
		defer log.SetOutput(prev)

		up := stream(t, frameRole, frameText) // 干净 EOF：无 finish_reason、无 [DONE]
		srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))
		resp := postJSON(t, srv.URL+"/v1/chat/completions", chatBody, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d（已提交后错误必须 200 + 流内错误帧）", resp.StatusCode)
		}
		body := string(raw)
		if !strings.HasPrefix(body, rawOf(frameRole, frameText)) {
			t.Fatalf("已收帧必须原样转发:\n%s", body)
		}
		if strings.HasSuffix(body, "data: [DONE]\n\n") {
			t.Fatalf("截断流收尾 = 错误帧（错误即终态，不补 [DONE]；W5-26 裁决）:\n%s", body)
		}
		var env struct {
			Error struct {
				Message string `json:"message"`
				Type    string `json:"type"`
			} `json:"error"`
		}
		rest := strings.TrimPrefix(body, rawOf(frameRole, frameText))
		payload, ok := strings.CutPrefix(rest, "data: ")
		if !ok {
			t.Fatalf("错误帧缺失:\n%s", body)
		}
		payload, _, _ = strings.Cut(payload, "\n\n")
		if err := json.Unmarshal([]byte(payload), &env); err != nil {
			t.Fatalf("错误帧不是 JSON: %v\n%s", err, payload)
		}
		if env.Error.Message != "upstream stream closed before finish_reason" || env.Error.Type != "incomplete_stream" {
			t.Fatalf("错误帧字段 = %+v", env.Error)
		}
		if rec := parseRequestRecord(t, logBuf.String()); len(rec.Attempts) != 1 || !rec.Attempts[0].Failed {
			t.Fatalf("attempt 记录 = %+v（want 单条 failed=true——上游侧失败）", rec)
		}
	})

	t.Run("finish_without_sentinel_clean", func(t *testing.T) {
		// new-api StreamEndReason.EOF 语义：finish_reason 已见、无哨兵 = 正常结束。
		up := stream(t, frameRole, frameText, frameFinish)
		srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))
		resp := postJSON(t, srv.URL+"/v1/chat/completions", chatBody, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		want := rawOf(frameRole, frameText, frameFinish)
		if string(raw) != want {
			t.Fatalf("有 finish_reason 的流必须逐字节直通:\nwant:\n%s\ngot:\n%s", want, raw)
		}
	})

	t.Run("sentinel_without_finish_clean", func(t *testing.T) {
		// [DONE] = 传输级终止证据（保守版刻意与解析器不同口径——语义完整性留给
		// 客户端 SDK；见报告 §判据）。
		up := stream(t, frameRole, frameText, "[DONE]")
		srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))
		resp := postJSON(t, srv.URL+"/v1/chat/completions", chatBody, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		want := rawOf(frameRole, frameText, "[DONE]")
		if string(raw) != want {
			t.Fatalf("[DONE] 收尾的流必须逐字节直通（不判截断、不叠加错误帧）:\nwant:\n%s\ngot:\n%s", want, raw)
		}
	})

	t.Run("error_frame_terminal_no_double", func(t *testing.T) {
		up := stream(t, frameRole, frameText, frameError)
		srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))
		resp := postJSON(t, srv.URL+"/v1/chat/completions", chatBody, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		body := string(raw)
		if !strings.Contains(body, "upstream exploded") {
			t.Fatalf("上游错误帧必须原样转发:\n%s", body)
		}
		if strings.Contains(body, "closed before finish_reason") {
			t.Fatalf("错误即终止——不得再报缺 finish_reason:\n%s", body)
		}
		if strings.Count(body, `"error"`) != 1 {
			t.Fatalf("错误帧已终止流，不得叠加第二个错误帧:\n%s", body)
		}
	})
}

// ---------------------------------------------------------------------------
// responses 直通锚点
// ---------------------------------------------------------------------------

// TestE2EResponsesPassthroughTermination 同协议直通（responses 入口 × responses
// 上游）：
//
//   - 零证据 EOF（无终止事件、无 error、无 [DONE]）= 截断流 → error + response.failed
//     收尾（续号承接已下发事件、id 承接首个 response 对象），attempt 记上游失败；
//   - 反证：completed 终止事件 / [DONE] 兼容哨兵在 → 逐字节直通（哨兵不转发）、
//     无注入。
func TestE2EResponsesPassthroughTermination(t *testing.T) {
	const frameCreated = `{"type":"response.created","response":{"id":"resp_w523","object":"response","created_at":1,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":0}`
	const frameDelta = `{"type":"response.output_text.delta","item_id":"msg_w523","output_index":0,"content_index":0,"delta":"Hi","sequence_number":1}`
	const frameCompleted = `{"type":"response.completed","response":{"id":"resp_w523","object":"response","created_at":1,"status":"completed","model":"gpt-5","output":[],"usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}},"sequence_number":2}`
	const respBody = `{"model":"gpt-5","input":"hi","stream":true}`

	type respFrame struct{ event, data string }
	fCreated := respFrame{"response.created", frameCreated}
	fDelta := respFrame{"response.output_text.delta", frameDelta}
	fCompleted := respFrame{"response.completed", frameCompleted}
	stream := func(t *testing.T, trailingDone bool, frames ...respFrame) *responsesUpstream {
		t.Helper()
		return newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			for _, f := range frames {
				sseFrame(w, f.event, f.data)
			}
			if trailingDone {
				_, _ = io.WriteString(w, "data: [DONE]\n\n")
			}
		})
	}
	rawOf := func(frames ...respFrame) string {
		var b strings.Builder
		for _, f := range frames {
			b.WriteString("event: " + f.event + "\ndata: " + f.data + "\n\n")
		}
		return b.String()
	}

	t.Run("truncated_error_terminal", func(t *testing.T) {
		var logBuf syncLogBuffer
		prev := log.Writer()
		log.SetOutput(&logBuf)
		defer log.SetOutput(prev)

		up := stream(t, false, fCreated, fDelta) // 干净 EOF：无终止事件
		srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))
		resp := postJSON(t, srv.URL+"/v1/responses", respBody, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d（已提交后错误必须 200 + 流内错误帧）", resp.StatusCode)
		}
		s := string(raw)
		if !strings.Contains(s, `"delta":"Hi"`) {
			t.Fatalf("已收事件必须原样转发:\n%s", s)
		}
		if strings.Contains(s, "[DONE]") {
			t.Fatalf("responses 面不得有 [DONE] 哨兵:\n%s", s)
		}
		frames := parseSSEFrames(t, raw)
		n := len(frames)
		if n < 4 || frames[n-2].Data["type"] != "error" || frames[n-1].Data["type"] != "response.failed" {
			t.Fatalf("末两帧必须是 error + response.failed:\n%s", s)
		}
		flat, failed := frames[n-2].Data, frames[n-1].Data
		if flat["code"] != "server_error" || flat["message"] != "upstream stream closed before a terminal response event" {
			t.Fatalf("error 事件字段 = %+v", flat)
		}
		// 续号承接已下发事件（delta seq=1 → error seq=2、failed seq=3）。
		if flat["sequence_number"].(float64) != 2 || failed["sequence_number"].(float64) != 3 {
			t.Fatalf("sequence_number 未承接（%v / %v）:\n%s", flat["sequence_number"], failed["sequence_number"], s)
		}
		ro, _ := failed["response"].(map[string]any)
		if ro == nil || ro["status"] != "failed" || ro["id"] != "resp_w523" {
			t.Fatalf("response.failed.response = %+v（want id 承接 + status failed）", ro)
		}
		if rec := parseRequestRecord(t, logBuf.String()); len(rec.Attempts) != 1 || !rec.Attempts[0].Failed {
			t.Fatalf("attempt 记录 = %+v（want 单条 failed=true）", rec)
		}
	})

	t.Run("completed_clean", func(t *testing.T) {
		up := stream(t, false, fCreated, fDelta, fCompleted)
		srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))
		resp := postJSON(t, srv.URL+"/v1/responses", respBody, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		want := rawOf(fCreated, fDelta, fCompleted)
		if string(raw) != want {
			t.Fatalf("有终止事件的流必须逐字节直通:\nwant:\n%s\ngot:\n%s", want, raw)
		}
	})

	t.Run("done_without_terminal_clean", func(t *testing.T) {
		// [DONE] 兼容哨兵 = 传输级终止证据（既有容忍面：视为流结束、不转发）——
		// 保守版刻意不判截断（见报告 §判据）。
		up := stream(t, true, fCreated, fDelta)
		srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))
		resp := postJSON(t, srv.URL+"/v1/responses", respBody, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		s := string(raw)
		if strings.Contains(s, "[DONE]") {
			t.Fatalf("哨兵不得转发:\n%s", s)
		}
		if strings.Contains(s, "response.failed") || strings.Contains(s, `"type":"error"`) {
			t.Fatalf("[DONE] 收尾的流不得判截断:\n%s", s)
		}
		if !strings.Contains(s, `"delta":"Hi"`) {
			t.Fatalf("已收事件丢失:\n%s", s)
		}
	})
}

// ---------------------------------------------------------------------------
// claude_messages 直通锚点（锁「不校验」——判据在客户端）
// ---------------------------------------------------------------------------

// TestE2EClaudePassthroughTerminationTolerance 同协议直通（claude 入口 × claude
// 上游）**不注入**任何终止判据：CC B7/B8 条件硬门 + P70 实测容忍矩阵——
// ④ 有内容时缺 stop_reason 被容忍（1×S 无回退）；P70B 完整块 + 干净 EOF 被容忍
// （工具照常执行）；⑥ 半路断连由客户端自己判失败回退（代理无块完整性信息面）。
// 锚点锁「逐字节直通 + 无错误帧 + 不记上游失败」——未来若给 claude 直通加校验，
// 这些断言必须显式改写（防止误改）。
func TestE2EClaudePassthroughTerminationTolerance(t *testing.T) {
	const msgStart = `{"type":"message_start","message":{"id":"msg_w523","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":0}}}`
	const blockStart = `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`
	const delta = `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hi"}}`
	const blockStop = `{"type":"content_block_stop","index":0}`
	const msgDeltaNoReason = `{"type":"message_delta","delta":{},"usage":{"output_tokens":1}}`
	const msgStop = `{"type":"message_stop"}`
	const claudeBody = `{"model":"claude-x","max_tokens":16,"messages":[{"role":"user","content":"hi"}],"stream":true}`

	type claudeFrame struct{ event, data string }
	fStart := claudeFrame{"message_start", msgStart}
	fBlockStart := claudeFrame{"content_block_start", blockStart}
	fDelta := claudeFrame{"content_block_delta", delta}
	fBlockStop := claudeFrame{"content_block_stop", blockStop}
	fMsgDeltaNoReason := claudeFrame{"message_delta", msgDeltaNoReason}
	fMsgStop := claudeFrame{"message_stop", msgStop}

	stream := func(t *testing.T, frames ...claudeFrame) *countUpstream {
		t.Helper()
		return newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			for _, f := range frames {
				sseFrame(w, f.event, f.data)
			}
		})
	}
	rawOf := func(frames ...claudeFrame) string {
		var b strings.Builder
		for _, f := range frames {
			b.WriteString("event: " + f.event + "\ndata: " + f.data + "\n\n")
		}
		return b.String()
	}
	run := func(t *testing.T, frames ...claudeFrame) string {
		t.Helper()
		var logBuf syncLogBuffer
		prev := log.Writer()
		log.SetOutput(&logBuf)
		defer log.SetOutput(prev)

		up := stream(t, frames...)
		srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))
		resp := postJSON(t, srv.URL+"/v1/messages", claudeBody, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		body := string(raw)
		if body != rawOf(frames...) {
			t.Fatalf("claude 直通必须逐字节保真:\nwant:\n%s\ngot:\n%s", rawOf(frames...), body)
		}
		if strings.Contains(body, "event: error") || strings.Contains(body, `"incomplete_stream"`) || strings.Contains(body, "[DONE]") {
			t.Fatalf("claude 直通不得注入错误帧/哨兵（判据在客户端）:\n%s", body)
		}
		if rec := parseRequestRecord(t, logBuf.String()); len(rec.Attempts) != 1 || rec.Attempts[0].Failed {
			t.Fatalf("attempt 记录 = %+v（want 单条 failed=false——容忍面不记上游失败）", rec)
		}
		return body
	}

	t.Run("content_without_stop_reason_tolerated", func(t *testing.T) {
		// P69 ④ 复现：完整文本 + message_delta 缺 stop_reason + message_stop。
		body := run(t, fStart, fBlockStart, fDelta, fBlockStop, fMsgDeltaNoReason, fMsgStop)
		if !strings.Contains(body, "message_stop") {
			t.Fatalf("message_stop 丢失:\n%s", body)
		}
	})

	t.Run("cut_mid_block_raw", func(t *testing.T) {
		// P69 ⑥ 复现：块未闭合的干净 EOF——客户端自己判失败回退，代理不得替它判。
		run(t, fStart, fBlockStart, fDelta)
	})

	t.Run("complete_block_eof_raw", func(t *testing.T) {
		// P70B tool_cut_after_block 复现：完整块 + 干净 EOF（无 message_delta/message_stop）
		// ——被容忍（工具照常执行），不得注入错误帧。
		run(t, fStart, fBlockStart, fDelta, fBlockStop)
	})
}
