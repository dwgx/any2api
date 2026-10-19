package service

// R01（A2A-L19-W70-3）：超限 SSE 帧丢内容时的静默假成功。
//
// 缺陷：framer 的 1 MiB 单行上限（RESYNC）此前对**内容帧**与**非内容帧**
// （注释/保活/event:/id:/retry:）一视同仁地丢弃——一条内容帧丢失后流照常走到
// `[DONE]`/finish_reason，relay 记 delivered=true / end_reason=completed。下游
// 拿到的是**缺了一段正文**的流，却被宣告成功（对账面与真实交付不符）。
//
// 修后口径：
//   - framer 记录被丢弃超限行是否携带内容（`LostContent()`）；
//   - relay 在读循环后、正常成功收尾前检查该位：内容帧丢失 → 记上游失败 +
//     end_reason=upstream_error；已提交 → 写 `incomplete_stream` /
//     `upstream_frame_too_large` 错误帧收尾（delivered=false）；未提交 → 换上游。
//
// 反证（回退即红）：去掉 relay.go 的 `reader.LostContent()` 检查分支 →
// TestOversizeCommittedContentFrameErrorsDownstream 红（下游无 incomplete_stream、
// attempt 行 delivered=true）。

import (
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"
)

// openAIChatUp openai chat 上游条目（本文件专用；同协议直通 = pipe 为 nil）。
func openAIChatUp(name, baseURL string) Upstream {
	return Upstream{Name: name, BaseURL: baseURL, APIKey: "k-" + name, Protocol: ProtocolOpenAIChat}
}

// oversizeChunk 一个超限（> 1 MiB）的 openai chat 内容 chunk 行（单行；含正文
// delta——内容承载帧）。
func oversizeChunk(fill string) string {
	return `{"id":"chatcmpl-ov","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{"content":"` +
		strings.Repeat(fill, maxSSELineBytes+1024) + `"},"finish_reason":null}]}`
}

// ovFrameRole / ovFrameText / ovFrameFinish 正常 openai chat 直通帧（首帧提交、
// 正文帧、终态帧）。
const (
	ovFrameRole   = `{"id":"chatcmpl-ov","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}`
	ovFrameText   = `{"id":"chatcmpl-ov","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":null}]}`
	ovFrameFinish = `{"id":"chatcmpl-ov","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`
)

// waitAttemptsLine 等 attempts 记账行落日志（handler 收尾 defer 与客户端读完响应
// 体之间有窗口——轮询而不是直接读）。
func waitAttemptsLine(t *testing.T, lb *syncLogBuffer) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if strings.Contains(lb.String(), "attempts: ") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 attempts 记账行超时，已见日志:\n%s", lb.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestOversizeCommittedContentFrameErrorsDownstream（e2e a · 已提交）：上游发一个
// 正常内容帧（提交点已过）→ 一个超限内容帧（RESYNC 丢弃）→ `[DONE]`。修前：下游
// 只见正常帧 + 哨兵，attempt 行 delivered=true/failed=false/end_reason=completed
// （静默丢内容的假成功）。修后：下游尾部必须出现 incomplete_stream /
// upstream_frame_too_large 错误帧，attempt 行 failed=true/delivered=false/
// end_reason=upstream_error。
func TestOversizeCommittedContentFrameErrorsDownstream(t *testing.T) {
	var logBuf syncLogBuffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", ovFrameRole)
		sseFrame(w, "", ovFrameText)
		_, _ = io.WriteString(w, "data: "+oversizeChunk("q")+"\n\n") // RESYNC 丢弃点
		sseFrame(w, "", "[DONE]")
	})
	s, srv := startServiceWith(t, failoverCfg(openAIChatUp("a", up.srv.URL)))

	code, body := postChatStream(t, srv.URL)
	if code != 200 {
		t.Fatalf("status = %d\n%s", code, body)
	}
	// 记账行先取（handler 收尾 defer 与客户端读完响应体之间有窗口）；失败信息里
	// 带上 attempt 归因——静默假成功的**对账证据**（修前 delivered=true）。
	waitAttemptsLine(t, &logBuf)
	rec := parseRequestRecord(t, logBuf.String())
	if !strings.Contains(body, ovFrameText) {
		t.Fatalf("提交前的正常内容帧应已下发:\n%s", body)
	}
	if strings.Contains(body, "qqqqqqqq") {
		t.Fatal("超限畸形帧不得到达下游")
	}
	// 核心断言：丢内容帧 = 失败收尾（不得静默当成功）。
	if !strings.Contains(body, `"incomplete_stream"`) || !strings.Contains(body, `"upstream_frame_too_large"`) {
		t.Fatalf("内容帧被丢弃后下游必须收到 incomplete_stream/upstream_frame_too_large 错误帧（attempt 归因 %+v）:\n%s",
			rec.Attempts, body)
	}
	// R01 二次修：终止证据帧（`[DONE]`）**不得**在丢内容后转发——否则唯读哨兵的
	// 消费端提前收流，看不到错误尾（还是假成功）。
	if strings.Contains(body, "[DONE]") {
		t.Fatalf("丢内容帧后 [DONE] 哨兵不得转发（错误尾必须是下游看到的最后事件）:\n%s", body)
	}
	// 错误尾在最后：错误码出现位置必须晚于所有正常内容帧。
	if last, tail := strings.LastIndex(body, "upstream_frame_too_large"), strings.LastIndex(body, ovFrameText); last < tail {
		t.Fatalf("错误尾必须晚于正常内容帧（last_err=%d last_text=%d）:\n%s", last, tail, body)
	}
	if got := up.count(); got != 1 {
		t.Fatalf("上游尝试次数 = %d（want 1——已提交不换上游）", got)
	}

	if len(rec.Attempts) != 1 {
		t.Fatalf("attempt 数 = %d（want 1）: %+v", len(rec.Attempts), rec.Attempts)
	}
	if a := rec.Attempts[0]; !a.Failed || a.Delivered || a.EndReason != endReasonUpstreamError {
		t.Fatalf("attempt 归因 = %+v（want failed=true delivered=false end_reason=upstream_error）", a)
	}
	if d := s.Diagnostics(); d.Failovers != 0 {
		t.Fatalf("diagnostics failovers = %d（want 0：已提交不换上游）", d.Failovers)
	}
	// 顺序证据（`-v` 可见）：内容帧在前、错误尾在最后、无 [DONE]。
	t.Logf("downstream body（%d bytes）:\n%s", len(body), body)
}

// TestOversizePreCommitContentFrameFailsOver（e2e b · 未提交）：上游 A 首帧即超限
// 内容帧（下游零字节、提交点未过）→ 必须换上游；B 的正常流完整到达下游，且下游
// 不出现错误帧（A 的失败对下游不可见——提交前换台纪律）。
func TestOversizePreCommitContentFrameFailsOver(t *testing.T) {
	upA := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: "+oversizeChunk("z")+"\n\n") // 仅超限帧，随后关流
	})
	upB := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", ovFrameText)
		sseFrame(w, "", ovFrameFinish)
	})
	s, srv := startServiceWith(t, failoverCfg(
		openAIChatUp("a", upA.srv.URL), openAIChatUp("b", upB.srv.URL)))

	code, body := postChatStream(t, srv.URL)
	if code != 200 || !strings.Contains(body, `"Hi"`) || !strings.Contains(body, ovFrameFinish) {
		t.Fatalf("下游应是第二台的完整流: status=%d\n%s", code, body)
	}
	if strings.Contains(body, `"incomplete_stream"`) || strings.Contains(body, `"upstream_frame_too_large"`) {
		t.Fatalf("预提交失败不得把错误帧写到下游（换台对下游不可见）:\n%s", body)
	}
	if upA.count() != 1 || upB.count() != 1 {
		t.Fatalf("hits: A=%d B=%d（want 1/1：换上游一次）", upA.count(), upB.count())
	}
	if d := s.Diagnostics(); d.Failovers < 1 {
		t.Fatalf("diagnostics failovers = %d（want >=1）", d.Failovers)
	}
}
