package chatgpt_web

// 件 3/4：事件分流 + IR 映射 + 终态语义。

import (
	"errors"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

func TestTypedEventDispatch(t *testing.T) {
	frames := []tframe{
		dataFrame(`{"type":"title_generation","title":"My chat","conversation_id":"c9"}`),
		dataFrame(`{"type":"resume_conversation_token","kind":"topic","token":"tok123","conversation_id":"c9"}`),
		dataFrame(`{"type":"stream_handoff","conversation_id":"c9","options":[{"type":"subscribe_ws_topic","topic_id":"ws1"},{"type":"resume_sse_endpoint","topic_id":"sse1"}]}`),
		dataFrame(`{"type":"server_ste_metadata","metadata":{"tool_invoked":true,"turn_use_case":"text","model_slug":"i-mini-m"}}`),
		dataFrame(`{"type":"moderation","moderation_response":{"blocked":true},"conversation_id":"c9"}`),
		dataFrame(`{"type":"input_message","input_message":{"author":{"role":"user"}},"conversation_id":"c9"}`),
		dataFrame(`{"type":"message_marker","marker":"user_visible_token","event":"first"}`),
		dataFrame(`{"type":"message_stream_complete","conversation_id":"c9"}`),
	}
	events, p := mustRun(t, frames)

	// 分流结果：全部类型化事件无内容产出；message_stream_complete 收束（判据①）。
	text, thinking := collectText(events)
	if text != "" || thinking != "" {
		t.Fatalf("typed events produced content: text=%q thinking=%q", text, thinking)
	}
	want := []ir.StreamEventKind{ir.EventStart, ir.EventMessageDelta, ir.EventMessageStop, ir.EventDone}
	got := eventKinds(events)
	if len(got) != len(want) {
		t.Fatalf("kinds = %v（want %v）", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("kinds = %v（want %v）", got, want)
		}
	}
	if events[1].StopReason != ir.StopStop {
		t.Fatalf("stop reason = %q", events[1].StopReason)
	}
	// accessor 面。
	if p.Title() != "My chat" {
		t.Fatalf("title = %q", p.Title())
	}
	if p.ResumeToken() != "tok123" {
		t.Fatalf("resume token = %q", p.ResumeToken())
	}
	h := p.Handoff()
	if h == nil || h.WSTopicID != "ws1" || h.SSETopicID != "sse1" || h.ConversationID != "c9" {
		t.Fatalf("handoff = %+v", h)
	}
	if p.ObservedModel() != "i-mini-m" {
		t.Fatalf("model = %q", p.ObservedModel())
	}
	if p.Moderation() == nil || !p.Moderation().Blocked {
		t.Fatalf("moderation = %+v", p.Moderation())
	}
	if p.ConversationID() != "c9" {
		t.Fatalf("conversation = %q", p.ConversationID())
	}
	if p.SawLiveMessage() {
		t.Fatalf("typed-only stream must not report a live message")
	}
}

func TestErrorFrame(t *testing.T) {
	events, p := mustRun(t, []tframe{
		dataFrame(`{"error":"Unusual activity detected"}`),
		dataFrame(`[DONE]`), // 错误后 [DONE]：容忍（幂等收束）
	})
	if len(events) != 2 || events[0].Kind != ir.EventStart || events[1].Kind != ir.EventError {
		t.Fatalf("kinds = %v", eventKinds(events))
	}
	ev := events[1]
	if ev.ErrorReason != ir.ErrorUpstream {
		t.Fatalf("error reason = %q", ev.ErrorReason)
	}
	if ev.Error == nil || ev.Error.Message != "Unusual activity detected" || ev.Error.Source != "chatgpt_web" {
		t.Fatalf("error = %+v", ev.Error)
	}
	_ = p
}

func TestDataAfterTerminal(t *testing.T) {
	p := NewStreamParser()
	if _, err := p.Feed("", []byte(`[DONE]`)); err != nil {
		t.Fatalf("[DONE]: %v", err)
	}
	// 重复结束标记 / 元数据帧 / 心跳：容忍（幂等收束——F:341-357「三选一都可能先到」）。
	if _, err := p.Feed("", []byte(`{"type":"message_stream_complete"}`)); err != nil {
		t.Fatalf("duplicate terminal: %v", err)
	}
	if _, err := p.Feed("", []byte(`{"type":"server_ste_metadata","metadata":{"model_slug":"m"}}`)); err != nil {
		t.Fatalf("metadata after terminal: %v", err)
	}
	if _, err := p.Feed("", []byte(": keep-alive")); err != nil {
		t.Fatalf("heartbeat after terminal: %v", err)
	}
	// 内容帧：显式报错（早终结风险暴露）。
	_, err := p.Feed("", []byte(`{"p":"/message/content/parts/0","o":"append","v":"late"}`))
	if !errors.Is(err, ErrDataAfterTerminal) {
		t.Fatalf("err = %v（want ErrDataAfterTerminal）", err)
	}
}

func TestIRMappingBlockSequence(t *testing.T) {
	// text → thinking → text：块按序闭合、新块新 index（gemini feedTextDelta 模式）。
	events, _ := mustRun(t, []tframe{
		dataFrame(`{"p":"/message/content/parts/0","o":"append","v":"Answer"}`),
		dataFrame(`{"p":"/message/content/thoughts/0","o":"append","v":"Wait"}`),
		dataFrame(`{"p":"/message/content/parts/0","o":"append","v":"!"}`),
		dataFrame(`[DONE]`),
	})
	want := []ir.StreamEventKind{
		ir.EventStart,
		ir.EventBlockStart, ir.EventBlockDelta, ir.EventBlockStop,
		ir.EventBlockStart, ir.EventBlockDelta, ir.EventBlockStop,
		ir.EventBlockStart, ir.EventBlockDelta, ir.EventBlockStop,
		ir.EventMessageDelta, ir.EventMessageStop, ir.EventDone,
	}
	got := eventKinds(events)
	if len(got) != len(want) {
		t.Fatalf("kinds = %v（want %v）", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("kinds = %v（want %v）", got, want)
		}
	}
	var blockKinds []ir.BlockKind
	var indices []int
	for _, ev := range events {
		if ev.Kind == ir.EventBlockStart {
			blockKinds = append(blockKinds, ev.BlockKind)
			indices = append(indices, ev.BlockIndex)
		}
	}
	if len(blockKinds) != 3 || blockKinds[0] != ir.BlockText || blockKinds[1] != ir.BlockThinking || blockKinds[2] != ir.BlockText {
		t.Fatalf("block kinds = %v", blockKinds)
	}
	if indices[0] != 0 || indices[1] != 1 || indices[2] != 2 {
		t.Fatalf("block indices = %v（want 0,1,2）", indices)
	}
	text, thinking := collectText(events)
	if text != "Answer!" || thinking != "Wait" {
		t.Fatalf("text=%q thinking=%q", text, thinking)
	}
}

func TestIRMappingContentTypes(t *testing.T) {
	events, _ := mustRun(t, []tframe{
		dataFrame(`{"v":{"message":{"id":"m1","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"code","text":"print(1)","language":"python"},"status":"in_progress"}}}`),
		dataFrame(`{"v":{"message":{"id":"m2","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"execution_output","text":"1"},"status":"in_progress"}}}`),
		dataFrame(`{"v":{"message":{"id":"m3","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"model_editable_context","text":"ctx"},"status":"in_progress"}}}`),
		dataFrame(`[DONE]`),
	})
	var blockKinds []ir.BlockKind
	for _, ev := range events {
		if ev.Kind == ir.EventBlockStart {
			blockKinds = append(blockKinds, ev.BlockKind)
		}
	}
	if len(blockKinds) != 2 || blockKinds[0] != ir.BlockExecutableCode || blockKinds[1] != ir.BlockCodeExecutionResult {
		t.Fatalf("block kinds = %v", blockKinds)
	}
	all := ""
	for _, d := range deltaTexts(events) {
		all += d.Text
	}
	if all != "print(1)1" {
		t.Fatalf("deltas = %q", all)
	}
	if containsText(events, "ctx") {
		t.Fatalf("model_editable_context 泄漏进事件流")
	}
}

func TestToolMessageRecords(t *testing.T) {
	events, p := mustRun(t, []tframe{
		// 工具消息（role=tool；图片指针）——记录，不产出（A/B/Go/gpt2api 同向）。
		dataFrame(`{"v":{"message":{"id":"t1","author":{"role":"tool"},"content":{"content_type":"multimodal_text","parts":[{"asset_pointer":"file-service://file_abc"},{"asset_pointer":"sediment://file_def"}]},"metadata":{"async_task_type":"image_gen","initial_text":"Made an image"}},"conversation_id":"c1"}}`),
		// live 正文。
		dataFrame(`{"v":{"message":{"id":"m1","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"text","parts":[""]},"status":"in_progress"}}}`),
		dataFrame(`{"p":"/message/content/parts/0","o":"append","v":"final"}`),
		dataFrame(`{"p":"/message/status","o":"replace","v":"finished_successfully"}`),
	})
	text, _ := collectText(events)
	if text != "final" {
		t.Fatalf("text = %q", text)
	}
	recs := p.ToolMessages()
	if len(recs) != 1 {
		t.Fatalf("records = %d（want 1）", len(recs))
	}
	rec := recs[0]
	if rec.Reason != "role" || rec.Role != "tool" || rec.ContentType != "multimodal_text" {
		t.Fatalf("record = %+v", rec)
	}
	if rec.InitialText != "Made an image" {
		t.Fatalf("initial text = %q", rec.InitialText)
	}
	if len(rec.Images) != 2 || rec.Images[0].Scheme != "file-service" || rec.Images[1].Scheme != "sediment" {
		t.Fatalf("images = %+v", rec.Images)
	}
	if len(rec.RawParts) != 2 {
		t.Fatalf("raw parts = %d", len(rec.RawParts))
	}
}

func TestCommentarySuppressed(t *testing.T) {
	events, p := mustRun(t, []tframe{
		dataFrame(`{"v":{"message":{"id":"m2","author":{"role":"assistant"},"channel":"commentary","content":{"content_type":"text","parts":["thinking out loud"]},"status":"in_progress"}}}`),
		dataFrame(`[DONE]`),
	})
	if containsText(events, "thinking out loud") {
		t.Fatalf("commentary 泄漏进事件流")
	}
	recs := p.ToolMessages()
	if len(recs) != 1 || recs[0].Reason != "commentary" || recs[0].Text != "thinking out loud" {
		t.Fatalf("records = %+v", recs)
	}
}

func TestLiveMultimodalImagesRecorded(t *testing.T) {
	events, p := mustRun(t, []tframe{
		dataFrame(`{"v":{"message":{"id":"m3","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"multimodal_text","parts":["see: ",{"asset_pointer":"file-service://file_x"}]},"status":"in_progress"}}}`),
		dataFrame(`[DONE]`),
	})
	text, _ := collectText(events)
	if text != "see: " {
		t.Fatalf("text = %q", text)
	}
	recs := p.ToolMessages()
	if len(recs) != 1 || recs[0].Reason != "live-multimodal" || len(recs[0].Images) != 1 {
		t.Fatalf("records = %+v", recs)
	}
	if recs[0].Images[0].Pointer != "file-service://file_x" {
		t.Fatalf("image = %+v", recs[0].Images[0])
	}
}

func TestUnsupportedContentErrors(t *testing.T) {
	cases := []struct {
		name  string
		frame string
	}{
		{"unknown pointer scheme", `{"v":{"message":{"id":"m4","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"multimodal_text","parts":[{"asset_pointer":"weird://x"}]},"status":"in_progress"}}}`},
		{"part without carrier", `{"v":{"message":{"id":"m5","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"multimodal_text","parts":[{"foo":1}]},"status":"in_progress"}}}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := NewStreamParser()
			_, err := p.Feed("", []byte(c.frame))
			if !errors.Is(err, ErrUnsupportedContent) {
				t.Fatalf("err = %v（want ErrUnsupportedContent）", err)
			}
		})
	}
}

func TestTerminalIdempotence(t *testing.T) {
	events, _ := mustRun(t, []tframe{
		dataFrame(`{"p":"/message/content/parts/0","o":"append","v":"hi"}`),
		dataFrame(`{"p":"/message/status","o":"replace","v":"finished_successfully"}`), // 判据②
		dataFrame(`{"type":"message_stream_complete"}`),                                // 判据①（重复）
		dataFrame(`[DONE]`), // 判据④（重复）
	})
	if n := countKind(events, ir.EventDone); n != 1 {
		t.Fatalf("done = %d（want 1）", n)
	}
	if n := countKind(events, ir.EventMessageStop); n != 1 {
		t.Fatalf("message_stop = %d（want 1）", n)
	}
	if n := countKind(events, ir.EventMessageDelta); n != 1 {
		t.Fatalf("message_delta = %d（want 1）", n)
	}
}

func TestStopReasonFromFinishDetails(t *testing.T) {
	// finish_details.type == max_tokens → length（Go:526-532 / A:786-791）。
	events, _ := mustRun(t, []tframe{
		dataFrame(`{"v":{"message":{"id":"m1","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"text","parts":["x"]},"status":"in_progress","metadata":{"finish_details":{"type":"max_tokens"}}}}}`),
		dataFrame(`[DONE]`),
	})
	var reason ir.StopReason
	for _, ev := range events {
		if ev.Kind == ir.EventMessageDelta {
			reason = ev.StopReason
		}
	}
	if reason != ir.StopLength {
		t.Fatalf("stop reason = %q（want length）", reason)
	}
	// /message/metadata 补丁面同样生效 + 模型回读。
	p := NewStreamParser()
	if _, err := p.Feed("", []byte(`{"p":"/message/metadata","o":"replace","v":{"model_slug":"gpt-5-3","finish_details":{"type":"max_tokens"}}}`)); err != nil {
		t.Fatalf("metadata patch: %v", err)
	}
	if p.ObservedModel() != "gpt-5-3" {
		t.Fatalf("model = %q", p.ObservedModel())
	}
	events, err := p.Feed("", []byte(`[DONE]`))
	if err != nil {
		t.Fatalf("[DONE]: %v", err)
	}
	reason = ""
	for _, ev := range events {
		if ev.Kind == ir.EventMessageDelta {
			reason = ev.StopReason
		}
	}
	if reason != ir.StopLength {
		t.Fatalf("stop reason after metadata patch = %q（want length）", reason)
	}
}

func TestFinishHandoff(t *testing.T) {
	// stream_handoff 后首段流关闭不产终止属正常——Finish 不按截断报错
	//（续接动作属服务壳 S3；A:857-868/922-1054）。
	p := NewStreamParser()
	if _, err := p.Feed("", []byte(`{"type":"stream_handoff","conversation_id":"c1","options":[{"type":"resume_sse_endpoint","topic_id":"t1"}]}`)); err != nil {
		t.Fatalf("handoff: %v", err)
	}
	if evs := p.Finish(); evs != nil {
		t.Fatalf("finish after handoff = %v（want nil）", eventKinds(evs))
	}
	if h := p.Handoff(); h == nil || h.SSETopicID != "t1" {
		t.Fatalf("handoff = %+v", h)
	}
}

func TestFinishTruncation(t *testing.T) {
	p := NewStreamParser()
	if _, err := p.Feed("", []byte(`{"p":"/message/content/parts/0","o":"append","v":"partial"}`)); err != nil {
		t.Fatalf("feed: %v", err)
	}
	events := p.Finish()
	if len(events) == 0 || events[len(events)-1].Kind != ir.EventError {
		t.Fatalf("finish events = %v", eventKinds(events))
	}
	ev := events[len(events)-1]
	if ev.ErrorReason != ir.ErrorTransport || ev.Error == nil || ev.Error.Type != "incomplete_stream" {
		t.Fatalf("error = %+v / %+v", ev.ErrorReason, ev.Error)
	}
	// 幂等：再调一次无补充。
	if again := p.Finish(); again != nil {
		t.Fatalf("second Finish = %v", eventKinds(again))
	}
}

func TestFinishEmptyStream(t *testing.T) {
	p := NewStreamParser()
	if events := p.Finish(); events != nil {
		t.Fatalf("empty stream finish = %v（want nil）", eventKinds(events))
	}
	// 纯心跳：同样零产出。
	p2 := NewStreamParser()
	if _, err := p2.Feed("", []byte("not json")); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if events := p2.Finish(); events != nil {
		t.Fatalf("heartbeat-only finish = %v（want nil）", eventKinds(events))
	}
}

func TestParseErrorEnvelope(t *testing.T) {
	// detail 字符串（形态未证——容忍面）。
	e := ParseError([]byte(`{"detail":"Unusual activity detected. Please try again later."}`))
	if e == nil || e.Message != "Unusual activity detected. Please try again later." || e.Source != "chatgpt_web" {
		t.Fatalf("detail string = %+v", e)
	}
	// error 对象（KB 证 `{"error": …}` 存在）。
	e = ParseError([]byte(`{"error":{"message":"Invalid or expired proof token","type":"invalid_request_error","code":"invalid_proof"}}`))
	if e == nil || e.Message != "Invalid or expired proof token" || e.Type != "invalid_request_error" || e.Code != "invalid_proof" {
		t.Fatalf("error object = %+v", e)
	}
	// detail 对象（code+message）。
	e = ParseError([]byte(`{"detail":{"code":"model_not_found","message":"The model does not exist."}}`))
	if e == nil || e.Code != "model_not_found" || e.Message != "The model does not exist." {
		t.Fatalf("detail object = %+v", e)
	}
	// 非 JSON（CF 挑战页）：泛型兜底，不泄漏原文进 Message 面。
	html := []byte(`<!DOCTYPE html><html><head><title>Just a moment...</title></head><body><div id="challenge-error-text">Enable JavaScript and cookies to continue</div></body></html>`)
	e = ParseError(html)
	if e == nil || e.Message == "" {
		t.Fatalf("html body = %+v", e)
	}
	if strings.Contains(e.Message, "<html") || strings.Contains(e.Message, "challenge-error-text") {
		t.Fatalf("html leaked into message: %q", e.Message)
	}
	if string(e.Raw) != string(html) {
		t.Fatalf("raw not preserved")
	}
	// 空体。
	e = ParseError(nil)
	if e == nil || e.Message == "" {
		t.Fatalf("empty body = %+v", e)
	}
}

// ---------------------------------------------------------------------------
// W53-1 实机校准（2026-10-15，chatgpt.com 直出全链实抓）：live 帧与 golden 的
// 出入锚点——行为已正确，本组测试把两个 live 变体钉成永久回归锚。
// ---------------------------------------------------------------------------

// TestLiveBulkPatchWithoutP live 校准（W53-1 §③）：真机批量 patch 帧**可以不带
// `p` 字段**——顶层只有 `{"o":"patch","v":[...]}`（golden 的 `p:""` 不是判据；
// 路由按 `v` 形状：数组 → applyArray）。帧形逐字节对齐 W53-1 实抓（chat 出口
// 第 12 帧：内容 append + status/end_turn replace + metadata append **子集**——
// 无 finish_details/is_complete；且整帧无 `c` 计数器）。
//
// 反证锚点：feedPayload 若改成「先按 f.O/f.P 分派、要求 p 非空才走数组」→ 本测红
// （text 空、终态缺失）。
func TestLiveBulkPatchWithoutP(t *testing.T) {
	frames := []tframe{
		dataFrame(`"v1"`),
		dataFrame(`{"v":{"message":{"id":"m1","author":{"role":"assistant"},"content":{"content_type":"text","parts":[""]},"status":"in_progress","metadata":{"resolved_model_slug":"gpt-6","model_slug":"gpt-6"}},"conversation_id":"c1"}}`),
		dataFrame(`{"o":"patch","v":[{"p":"/message/content/parts/0","o":"append","v":"PONG"},{"p":"/message/status","o":"replace","v":"finished_successfully"},{"p":"/message/end_turn","o":"replace","v":true},{"p":"/message/metadata","o":"append","v":{"can_save":true,"search_result_groups":[]}}]}`),
		dataFrame(`[DONE]`),
	}
	events, p := mustRun(t, frames)
	text, thinking := collectText(events)
	if text != "PONG" || thinking != "" {
		t.Fatalf("text=%q thinking=%q（want PONG——p 缺失不应影响数组路由）", text, thinking)
	}
	if !p.Terminal() {
		t.Fatalf("未收束（end_turn replace 在数组内）")
	}
	if !p.DoneSentinelSeen() {
		t.Fatalf("[DONE] 未登记")
	}
	if p.ObservedModel() != "gpt-6" {
		t.Fatalf("model = %q（want gpt-6——消息级 model_slug 回读）", p.ObservedModel())
	}
}

// TestLiveSystemEndTurnNotTerminal live 校准（W53-1 §③）：真机 system 消息带
// `end_turn:true`（实抓帧 4/5；`recipient:"all"` + `metadata.message_type:"next"`）
// ——role 过滤（非 assistant 早退）必须先于结束判据，绝不触发终态；其后 live
// assistant 正文照常。
//
// 反证锚点：handleMessage 若在 role 过滤前评估 end_turn（或对非 assistant 也走
// 结束判据）→ 本测中间断言红（system 帧后 Terminal()=true）。
func TestLiveSystemEndTurnNotTerminal(t *testing.T) {
	p := NewStreamParser()
	feed := func(data string) {
		t.Helper()
		if _, err := p.Feed("", []byte(data)); err != nil {
			t.Fatalf("feed %s: %v", data, err)
		}
	}
	feed(`"v1"`)
	feed(`{"v":{"message":{"id":"s1","author":{"role":"system"},"recipient":"all","content":{"content_type":"text","parts":["hidden"]},"status":"finished_successfully","end_turn":true,"metadata":{"message_type":"next"}},"conversation_id":"c1"}}`)
	feed(`{"v":{"message":{"id":"s2","author":{"role":"system"},"recipient":"all","content":{"content_type":"text","parts":["hidden2"]},"status":"finished_successfully","end_turn":true,"metadata":{"message_type":"next"}},"conversation_id":"c1"}}`)
	if p.Terminal() {
		t.Fatalf("system 消息的 end_turn 误触终态（role 过滤应先于结束判据）")
	}
	feed(`{"v":{"message":{"id":"m1","author":{"role":"assistant"},"content":{"content_type":"text","parts":[""]},"status":"in_progress"}}}`)
	feed(`{"p":"/message/content/parts/0","o":"append","v":"OK"}`)
	feed(`{"p":"/message/end_turn","o":"replace","v":true}`)
	feed(`[DONE]`)
	if !p.Terminal() {
		t.Fatalf("assistant end_turn 未收束")
	}
	if got := len(p.ToolMessages()); got != 2 {
		t.Fatalf("tool records = %d（want 2——system 抑制记录）", got)
	}
}
