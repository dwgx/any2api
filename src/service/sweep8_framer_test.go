// W5-25 条目 2（framer 工程）：SSE 帧读取器的三件工程件回归——Higress
// `provider/sse_framing.go:10-70` 的四条细节里我们缺的三条：
//
//  1. last-chunk flush（上游无结尾空行就关流，尾部事件=usage 不丢）；
//  2. 1 MiB 单行上限（不无限攒内存）；
//  3. RESYNC（畸形巨行丢弃到下一个帧边界后**继续读**——一条坏行不终止整条流）。
//
// 第四条（只在发出时归一化行尾）不适用：我们按行读、不重写保留字节。
package service

import (
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
)

// TestFramerLastChunkFlushNoTrailingBlank：尾部帧无结尾空行（vertex 关流形态）
// → EOF 时仍派发（丢尾部事件 = 丢 usage）。
func TestFramerLastChunkFlushNoTrailingBlank(t *testing.T) {
	// 注意：最后一帧后**没有**空行（正是 Higress 注释点名的 vertex 行为）。
	stream := "data: {\"a\":1}\n\ndata: {\"usage\":{\"output_tokens\":7}}"
	r := newSSEFrameReader(strings.NewReader(stream), false)

	var got []string
	for {
		ok, err := r.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, string(r.Data()))
	}
	if len(got) != 2 {
		t.Fatalf("帧数 = %d（want 2——尾部无空行的帧必须 flush）: %q", len(got), got)
	}
	if got[1] != `{"usage":{"output_tokens":7}}` {
		t.Fatalf("尾部帧 = %q（usage 丢了）", got[1])
	}
}

// TestFramerLastChunkFlushEventAndData：event 行 + data 行、无结尾空行——
// event 名一并保留（claude/openai 上游的具名帧）。
func TestFramerLastChunkFlushEventAndData(t *testing.T) {
	r := newSSEFrameReader(strings.NewReader("event: message_stop\ndata: {\"type\":\"message_stop\"}"), false)
	ok, err := r.Next()
	if err != nil || !ok {
		t.Fatalf("Next = %v, %v", ok, err)
	}
	if r.Event() != "message_stop" || string(r.Data()) != `{"type":"message_stop"}` {
		t.Fatalf("帧 = (%q, %q)", r.Event(), r.Data())
	}
	if ok, _ := r.Next(); ok {
		t.Fatal("flush 后应结束")
	}
}

// TestFramerResyncOversizedLine：1 MiB 巨行 → 丢弃到下一个帧边界，**后续帧照常
// 读出**（旧实现 = 整条流断掉）。R01：被丢弃的 `data:` 巨行承载内容 →
// LostContent() 必须置位（relay 据此判失败，不再静默当成功）。
func TestFramerResyncOversizedLine(t *testing.T) {
	huge := strings.Repeat("x", maxSSELineBytes+4096) // 超限行（无换行，直到 EOF 前才结束）
	stream := "data: {\"before\":true}\n\n" +
		"data: " + huge + "\n\n" + // 畸形帧（巨行 + 空行边界）
		"data: {\"after\":true}\n\n"
	r := newSSEFrameReader(strings.NewReader(stream), false)

	var got []string
	for {
		ok, err := r.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, string(r.Data()))
	}
	if len(got) != 2 {
		t.Fatalf("帧数 = %d（want 2：畸形帧被丢弃，前后帧保留）: %v", len(got), got)
	}
	if got[0] != `{"before":true}` || got[1] != `{"after":true}` {
		t.Fatalf("帧内容 = %v", got)
	}
	if r.resyncs != 1 {
		t.Fatalf("resyncs = %d（want 1）", r.resyncs)
	}
	if !r.LostContent() {
		t.Fatal("data: 超限行承载内容——LostContent() 必须为 true（R01）")
	}
}

// TestFramerOversizeCommentLostContentFalse：超限的**非内容**行（注释/保活形态）
// 被丢弃 → LostContent() 必须为 false（丢保活不丢正文，不得把健康流的一次 resync
// 判成失败）；RESYNC 恢复行为不变（后续帧照常读出）。
func TestFramerOversizeCommentLostContentFalse(t *testing.T) {
	stream := "data: {\"before\":true}\n\n" +
		":" + strings.Repeat("keepalive", maxSSELineBytes/len("keepalive")+1) + "\n\n" + // 超限注释行
		"data: {\"after\":true}\n\n"
	r := newSSEFrameReader(strings.NewReader(stream), false)

	var got []string
	for {
		ok, err := r.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, string(r.Data()))
	}
	if len(got) != 2 || got[0] != `{"before":true}` || got[1] != `{"after":true}` {
		t.Fatalf("RESYNC 后帧序 = %v（want 前后两帧）", got)
	}
	if r.resyncs != 1 {
		t.Fatalf("resyncs = %d（want 1）", r.resyncs)
	}
	if r.LostContent() {
		t.Fatal("注释/保活超限行不承载内容——LostContent() 必须为 false（R01）")
	}
}

// TestFramerOversizeJSONFallbackContent：jsonFallback（gemini 裸 JSON 上游）下超限
// 的**裸 JSON 行**同样承载内容 → LostContent() 置位；RESYNC + 后续帧不受影响。
func TestFramerOversizeJSONFallbackContent(t *testing.T) {
	hugeJSON := `{"candidates":[{"content":{"parts":[{"text":"` + strings.Repeat("g", maxSSELineBytes+1024) + `"}]}}]}`
	stream := `{"candidates":[{"content":{"parts":[{"text":"before"}]}}]}` + "\n\n" +
		hugeJSON + "\n\n" + // 超限裸 JSON 行（RESYNC 丢弃点；空行 = 帧边界）
		`{"candidates":[{"content":{"parts":[{"text":"after"}]}}]}` + "\n\n"
	r := newSSEFrameReader(strings.NewReader(stream), true)

	var got []string
	for {
		ok, err := r.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, string(r.Data()))
	}
	if len(got) != 2 || !strings.Contains(got[0], "before") || !strings.Contains(got[1], "after") {
		t.Fatalf("RESYNC 后帧序 = %v（want before/after 两帧）", got)
	}
	if r.resyncs != 1 {
		t.Fatalf("resyncs = %d（want 1）", r.resyncs)
	}
	if !r.LostContent() {
		t.Fatal("jsonFallback 裸 JSON 超限行承载内容——LostContent() 必须为 true（R01）")
	}
}

// TestFramerResyncAtEOF：畸形巨行后直接 EOF（无后续帧）→ 干净结束（不报错、
// 不悬挂）；resync 计数可观测。
func TestFramerResyncAtEOF(t *testing.T) {
	stream := "data: " + strings.Repeat("y", maxSSELineBytes+16)
	r := newSSEFrameReader(strings.NewReader(stream), false)
	ok, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if ok {
		t.Fatalf("巨行残帧不应派发（超限即弃）: %q", r.Data())
	}
	if r.resyncs != 1 {
		t.Fatalf("resyncs = %d（want 1）", r.resyncs)
	}
}

// TestFramerNormalStreamUnchanged：常规流（空行分隔、注释行、多行 data）行为不变
// ——帧边界/注释忽略/多行连接三条既有语义的锚点（回归护栏）。
func TestFramerNormalStreamUnchanged(t *testing.T) {
	stream := ": keepalive\n\n" +
		"event: content_block_delta\n" +
		"data: {\"type\":\"content_block_delta\",\n" +
		"data: \"index\":0}\n\n" +
		"data: [DONE]\n\n"
	r := newSSEFrameReader(strings.NewReader(stream), false)

	ok, err := r.Next()
	if err != nil || !ok {
		t.Fatalf("帧1 = %v, %v", ok, err)
	}
	if r.Event() != "content_block_delta" {
		t.Fatalf("帧1 event = %q", r.Event())
	}
	if string(r.Data()) != "{\"type\":\"content_block_delta\",\n\"index\":0}" {
		t.Fatalf("帧1 多行 data 连接 = %q", r.Data())
	}
	ok, err = r.Next()
	if err != nil || !ok {
		t.Fatalf("帧2 = %v, %v", ok, err)
	}
	if string(r.Data()) != "[DONE]" {
		t.Fatalf("帧2 = %q", r.Data())
	}
	if ok, _ := r.Next(); ok {
		t.Fatal("流应结束")
	}
	if r.resyncs != 0 {
		t.Fatalf("正常流不应触发 resync：%d", r.resyncs)
	}
}

// TestFramerJSONFallbackUnchanged：gemini 裸 JSON 行聚合（既有语义锚点）——
// 括号平衡成帧 + EOF 残帧宽容派发。
func TestFramerJSONFallbackUnchanged(t *testing.T) {
	stream := `{"candidates":[{"content":{"parts":[{"text":"hi"}]}}],"usageMetadata":{"totalTokenCount":5}}` + "\n"
	r := newSSEFrameReader(strings.NewReader(stream), true)
	ok, err := r.Next()
	if err != nil || !ok {
		t.Fatalf("帧 = %v, %v", ok, err)
	}
	if !strings.Contains(string(r.Data()), "usageMetadata") {
		t.Fatalf("裸 JSON 帧 = %q", r.Data())
	}
}

// TestFramerResyncE2E：服务壳级 RESYNC——上游流中夹一个超限帧，**后续内容帧**
// 必须逐字节到达下游，且日志出现丢弃计数（观测面）。旧实现（bufio.Scanner 64 MiB
// 上限）在超限时整条流断掉 → 下游只会看到错误帧。
//
// R01（W70-3 + 二次修）修订：被丢弃的是**内容帧**（`data:` 巨行）→ 本尝试判失败，
// 且丢帧后到达的**终止证据帧**（此处 `finish_reason:"stop"`）不再转发（唯读
// finish_reason 的消费端会提前收流、看不到错误尾）——下游看到的最后事件是
// incomplete_stream/upstream_frame_too_large 错误帧；普通内容帧（frameRole/
// frameText）照常到达。
func TestFramerResyncE2E(t *testing.T) {
	const frameRole = `{"id":"chatcmpl-f8","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}`
	const frameText = `{"id":"chatcmpl-f8","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":null}]}`
	const frameFinish = `{"id":"chatcmpl-f8","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`
	huge := strings.Repeat("q", maxSSELineBytes+1024)

	var logBuf syncLogBuffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", frameRole)
		// 畸形帧：单行超 1 MiB（无换行直到巨行末尾）。
		_, _ = io.WriteString(w, "data: "+huge+"\n\n")
		sseFrame(w, "", frameText)
		sseFrame(w, "", frameFinish)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))
	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body := string(raw)
	for _, f := range []string{frameRole, frameText} {
		if !strings.Contains(body, f) {
			t.Fatalf("正常内容帧缺失（RESYNC 未续读）: %s\n---\n%s", f, body)
		}
	}
	if strings.Contains(body, frameFinish) {
		t.Fatalf("丢内容帧后终止证据帧不得转发（否则消费端提前收流）:\n%s", body)
	}
	if !strings.Contains(body, `"code":"upstream_frame_too_large"`) {
		t.Fatalf("错误尾缺失:\n%s", body)
	}
	if last, tail := strings.LastIndex(body, "upstream_frame_too_large"), strings.LastIndex(body, frameText); last < tail {
		t.Fatalf("错误尾必须晚于正常内容帧（last_err=%d last_text=%d）:\n%s", last, tail, body)
	}
	if strings.Contains(body, huge[:64]) {
		t.Fatal("畸形帧不应到达下游")
	}
	if !strings.Contains(logBuf.String(), "oversized SSE frame(s) dropped") {
		t.Fatalf("RESYNC 日志缺失：%s", logBuf.String())
	}
}
