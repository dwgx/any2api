package chatgpt_web

// W30-1：匿名档 golden 语料（W29-1 实抓帧入包）——把 W29-1 的 overlay 测试
// （`tmp/lanes/W29-chatgpt-s1/golden_w29_test.go`，经 `go test -overlay` 注入、
// 虚拟文件不落盘）正式化为本包 testdata 驱动测试。
//
// 语料（来源/脱敏/凭据扫描/sha256 见 testdata/README.md）：
//
//	anon_v1_direct.sse.txt  Android 匿名面 v1 档（direct 出口；21 帧）
//	anon_v1_isp.sse.txt     Android 匿名面 v1 档（ISP 出口；19 帧；含 D1 证据帧）
//	anon_snapshot.sse.txt   Android 匿名面快照档（17 帧）
//
// 装置：逐帧喂 Feed（增量面，pairs.StreamPipe 的形态）+ EOF 后 Finish；**三份
// 都不允许任何 feed 错误**——ISP 份在 D1 修复前正是在这里红
// （`ErrDataAfterTerminal` + TEXT=""，W29-1 §4.2）。
//
// 反证锚点（已实测回退即红，证据见报告 §反证）：
//   - 回退 D1 修复（状态判据不排除 reasoning_recap）→ TestGoldenAnonV1ISP 红；
//   - 回退 noteCounter 观测（accessor 恒零）→ TestGoldenAnonV1Direct 计数器断言红；
//   - 回退 golden 断言（ISP 期望改回 W29-1 的 TEXT=""）→ TestGoldenAnonV1ISP 红。

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// goldenStream 读 testdata 的 golden SSE 并喂完全部帧（**不早停**——首个错误
// 记下后继续，失败时能看到完整帧序与后续产物）。
func goldenStream(t *testing.T, name string) ([]ir.StreamEvent, *StreamParser, error) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	frames, err := SplitFrames(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatalf("split golden %s: %v", name, err)
	}
	p := NewStreamParser()
	var events []ir.StreamEvent
	var firstErr error
	for i, fr := range frames {
		evs, ferr := p.Feed(fr.Event, []byte(fr.Data))
		if ferr != nil {
			t.Logf("%s frame %d (event=%q data=%.60s): %v", name, i, fr.Event, fr.Data, ferr)
			if firstErr == nil {
				firstErr = ferr
			}
		}
		events = append(events, evs...)
	}
	events = append(events, p.Finish()...)
	return events, p, firstErr
}

// goldenNoErr 断言 golden 全程无错误（D1 面：终态后内容帧报错会在此暴露）。
func goldenNoErr(t *testing.T, name string, err error) {
	t.Helper()
	if err == nil {
		return
	}
	if errors.Is(err, ErrDataAfterTerminal) {
		t.Fatalf("%s: %v（终态判据误触——D1 面：状态判据须排除 reasoning_recap）", name, err)
	}
	t.Fatalf("%s: feed error: %v", name, err)
}

// goldenDone 断言流以 done 收束且 stop_reason 正确。
func goldenDone(t *testing.T, name string, events []ir.StreamEvent, want ir.StopReason) {
	t.Helper()
	done := 0
	var got ir.StopReason
	for _, ev := range events {
		if ev.Kind == ir.EventDone {
			done++
			got = ev.Reason
		}
	}
	if done != 1 {
		t.Fatalf("%s: done events = %d（want 1）kinds=%v", name, done, eventKinds(events))
	}
	if got != want {
		t.Fatalf("%s: stop_reason = %q（want %q）", name, got, want)
	}
}

// goldenDetail 断言 conversation_detail_metadata 已记录（accessor 面；D4）并
// 回读 default_model_slug。
func goldenDetail(t *testing.T, name string, p *StreamParser) {
	t.Helper()
	raw := p.ConversationDetailMetadata()
	if len(raw) == 0 {
		t.Fatalf("%s: ConversationDetailMetadata() 为空（该帧未记录）", name)
	}
	var detail struct {
		DefaultModelSlug         string          `json:"default_model_slug"`
		IntendedDefaultModelSlug string          `json:"intended_default_model_slug"`
		LimitsProgress           json.RawMessage `json:"limits_progress"`
	}
	if err := json.Unmarshal(raw, &detail); err != nil {
		t.Fatalf("%s: detail 原文非 JSON 对象: %v（%s）", name, err, truncate(string(raw), 80))
	}
	if detail.DefaultModelSlug != "auto" || detail.IntendedDefaultModelSlug != "auto" {
		t.Fatalf("%s: detail slugs = %q/%q（want auto/auto）",
			name, detail.DefaultModelSlug, detail.IntendedDefaultModelSlug)
	}
	// guest 档无额度面（W29-1 §2.2「limits_progress = null」）。
	if len(detail.LimitsProgress) > 0 && !isJSONNull(detail.LimitsProgress) {
		t.Fatalf("%s: limits_progress = %s（guest 档 want null）", name, detail.LimitsProgress)
	}
}

// goldenCounter 断言 `c` 计数器观测面（D3）：逐帧 +1、无跳变。
func goldenCounter(t *testing.T, name string, p *StreamParser, wantLast int64, wantFrames int) {
	t.Helper()
	last, seen := p.PatchCounter()
	if !seen || last != wantLast {
		t.Fatalf("%s: PatchCounter() = (%d, %v)（want (%d, true)）", name, last, seen, wantLast)
	}
	if p.PatchCounterFrames() != wantFrames {
		t.Fatalf("%s: PatchCounterFrames() = %d（want %d）", name, p.PatchCounterFrames(), wantFrames)
	}
	if p.PatchCounterJumps() != 0 {
		t.Fatalf("%s: PatchCounterJumps() = %d（实抓 c 逐帧 +1，want 0）", name, p.PatchCounterJumps())
	}
}

// TestGoldenAnonV1Direct v1 档 direct 出口：正文跨简写帧拼接 + 批量 patch 收束 +
// 计数器/会话详情观测面。
func TestGoldenAnonV1Direct(t *testing.T) {
	const name = "anon_v1_direct.sse.txt"
	events, p, err := goldenStream(t, name)
	goldenNoErr(t, name, err)

	text, thinking := collectText(events)
	if text != "1\n2\n3\n4\n5\n6\n7\n8\n9\n10" {
		t.Fatalf("%s: TEXT=%q（want 1..10 逐行）", name, text)
	}
	if thinking != "" {
		t.Fatalf("%s: thinking=%q（匿名档无思考通道）", name, thinking)
	}
	goldenDone(t, name, events, ir.StopStop)

	if p.ProtocolVersion() != "v1" {
		t.Fatalf("%s: version=%q（want v1——delta_encoding 标记）", name, p.ProtocolVersion())
	}
	if p.ObservedModel() != "gpt-6" {
		t.Fatalf("%s: model=%q（want gpt-6）", name, p.ObservedModel())
	}
	if p.Title() != "Count to ten" {
		t.Fatalf("%s: title=%q", name, p.Title())
	}
	if p.ConversationID() == "" {
		t.Fatalf("%s: conversation id 空", name)
	}
	if !p.SawLiveMessage() {
		t.Fatalf("%s: sawLive=false", name)
	}
	if p.HistoryDropped() != 1 {
		t.Fatalf("%s: historyDropped=%d（want 1——user 回显帧）", name, p.HistoryDropped())
	}
	goldenCounter(t, name, p, 6, 7)
	goldenDetail(t, name, p)
}

// TestGoldenAnonV1ISP v1 档 ISP 出口——**D1 回归锚**：第 8 帧是
// `content_type:"reasoning_recap"` 助手消息（`status:"finished_successfully"`、
// `end_turn:false`），修复前它触发状态终态判据 → 其后正文 patch 被
// ErrDataAfterTerminal 拒绝（TEXT=""）。本测试同时守住「正文非空」与「零错误」。
func TestGoldenAnonV1ISP(t *testing.T) {
	const name = "anon_v1_isp.sse.txt"
	events, p, err := goldenStream(t, name)
	goldenNoErr(t, name, err)

	text, thinking := collectText(events)
	if text == "" {
		t.Fatalf("%s: TEXT 为空（D1：recap 帧误触终态，其后正文全丢）", name)
	}
	if text != "PONG" {
		t.Fatalf("%s: TEXT=%q（want PONG）", name, text)
	}
	if thinking != "" {
		t.Fatalf("%s: thinking=%q（recap 是 UI 回顾条，不是思考增量）", name, thinking)
	}
	goldenDone(t, name, events, ir.StopStop)

	if p.ProtocolVersion() != "v1" {
		t.Fatalf("%s: version=%q", name, p.ProtocolVersion())
	}
	if p.ObservedModel() != "gpt-6" {
		t.Fatalf("%s: model=%q", name, p.ObservedModel())
	}
	if !p.SawLiveMessage() || p.HistoryDropped() != 1 {
		t.Fatalf("%s: sawLive=%v historyDropped=%d", name, p.SawLiveMessage(), p.HistoryDropped())
	}
	// live 消息是 recap 之后的正文消息（不是 recap 自己）——IR start 的 MessageID
	// 取采纳时的 live id。
	if len(events) == 0 || events[0].Kind != ir.EventStart || events[0].MessageID != "<id-10>" {
		t.Fatalf("%s: start=%+v（want MessageID=<id-10>——recap 的 <id-9> 不算 live 正文消息）",
			name, events[0])
	}
	goldenCounter(t, name, p, 6, 7)
	if raw := p.ConversationDetailMetadata(); len(raw) != 0 {
		t.Fatalf("%s: 本份无 conversation_detail_metadata 帧，accessor 应为空：%s",
			name, truncate(string(raw), 80))
	}
}

// TestGoldenAnonSnapshot 快照档（`supported_encodings:["text/plain"]`）：全对象
// 帧 + parts 全量 → 前缀差分（无重复）；无 delta_encoding、无 `c`。
func TestGoldenAnonSnapshot(t *testing.T) {
	const name = "anon_snapshot.sse.txt"
	events, p, err := goldenStream(t, name)
	goldenNoErr(t, name, err)

	text, _ := collectText(events)
	if text != "PONG" {
		t.Fatalf("%s: TEXT=%q（want PONG——快照差分不得重发/漏发）", name, text)
	}
	goldenDone(t, name, events, ir.StopStop)

	if p.ProtocolVersion() != "" {
		t.Fatalf("%s: version=%q（want 空——快照档无 delta_encoding 标记）", name, p.ProtocolVersion())
	}
	if p.ObservedModel() != "gpt-6" {
		t.Fatalf("%s: model=%q", name, p.ObservedModel())
	}
	if p.Title() != "Pong reply" {
		t.Fatalf("%s: title=%q", name, p.Title())
	}
	if !p.SawLiveMessage() || p.HistoryDropped() != 1 {
		t.Fatalf("%s: sawLive=%v historyDropped=%d", name, p.SawLiveMessage(), p.HistoryDropped())
	}
	// 快照档无 `c`：观测面必须如实报「未见」（不合成）。
	if _, seen := p.PatchCounter(); seen {
		t.Fatalf("%s: PatchCounter() seen=true（快照档无 c 字段）", name)
	}
	if p.PatchCounterFrames() != 0 || p.PatchCounterJumps() != 0 {
		t.Fatalf("%s: counter frames=%d jumps=%d（want 0/0）",
			name, p.PatchCounterFrames(), p.PatchCounterJumps())
	}
	goldenDetail(t, name, p)
}

// TestGoldenAnonV1ISPRecapDoesNotTerminate D1 判据的最小单元面（不依赖 golden
// 文件）：recap 消息单独喂进去不得产生终态；其后正文 patch 必须照常产出。
func TestGoldenAnonV1ISPRecapDoesNotTerminate(t *testing.T) {
	// 实抓帧形态（W29-1 §2.2 帧 8；`content` 是字符串、无 parts）。
	recap := dataFrame(`{"v":{"message":{"id":"m-recap","author":{"role":"assistant"},` +
		`"channel":"final",` +
		`"content":{"content_type":"reasoning_recap","content":"Worked for a second"},` +
		`"status":"finished_successfully","end_turn":false,` +
		`"metadata":{"reasoning_status":"reasoning_ended","reasoning_recap_type":"hide_all",` +
		`"message_type":"next","model_slug":"gpt-6"}}}}`)

	events, p := mustRun(t, []tframe{
		recap,
		dataFrame(`{"p":"/message/content/parts/0","o":"append","v":"PONG"}`),
		dataFrame(`{"p":"","o":"patch","v":[{"p":"/message/status","o":"replace","v":"finished_successfully"},{"p":"/message/end_turn","o":"replace","v":true}]}`),
	})
	text, _ := collectText(events)
	if text != "PONG" {
		t.Fatalf("TEXT=%q（recap 后的正文必须保留）", text)
	}
	goldenDone(t, "recap-synthetic", events, ir.StopStop)
	if p.ProtocolVersion() != "" || !p.SawLiveMessage() {
		t.Fatalf("version=%q sawLive=%v", p.ProtocolVersion(), p.SawLiveMessage())
	}

	// 反向守卫：**没有** recap 标记的助手消息（finished_successfully）照旧终结
	// ——排除面不能放宽成「所有助手消息都不终结」。
	_, p2 := mustRun(t, []tframe{
		dataFrame(`{"v":{"message":{"id":"m-1","author":{"role":"assistant"},` +
			`"channel":"final",` +
			`"content":{"content_type":"text","parts":["done"]},` +
			`"status":"finished_successfully","end_turn":false,"metadata":{}}}}`),
	})
	if !p2.terminalSeen {
		t.Fatalf("普通助手消息 finished_successfully 未终结（排除面过宽）")
	}

	// 兜底判据：content_type 是 text 但带 reasoning 标记 → 同样不终结。
	_, p3 := mustRun(t, []tframe{
		dataFrame(`{"v":{"message":{"id":"m-2","author":{"role":"assistant"},` +
			`"channel":"final",` +
			`"content":{"content_type":"text","parts":[""]},` +
			`"status":"finished_successfully","end_turn":false,` +
			`"metadata":{"reasoning_status":"reasoning_ended"}}}}`),
	})
	if p3.terminalSeen {
		t.Fatalf("reasoning_status 标记的助手消息被误判终态（D1 兜底判据失效）")
	}
}
