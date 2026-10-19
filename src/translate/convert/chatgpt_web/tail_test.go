package chatgpt_web

// W32-1：尾读口径（definitive marker / 有界宽限）与尾部帧观测面（last_token 等
// message_marker 登记）的测试。
//
// 依据：W31-1 §4/§10.1（流式 4/4 被 250ms 宽限截于 last_token——尾部帧观测面
// 缺失）与三份 golden 的尾部帧序：
//
//	direct：… batch patch → marker(last_token) → title_generation →
//	        server_ste_metadata → message_stream_complete →
//	        conversation_detail_metadata → [DONE]（21 帧）
//	isp   ：… batch patch → marker(last_token) → title → ste → stream_complete
//	        → [DONE]（19 帧；无 detail）
//	snapshot：… marker(last_token) → title → ste → stream_complete → detail
//	        → [DONE]（17 帧）
//
// 反证锚点（回退即红）：
//   - recordMarker 回退成忽略 → TestGoldenTailFramesRegistered 红
//     （Markers() 空）；TestGoldenTruncatedTailAccessor 红；
//   - stream_complete/[DONE] 登记回退 → DefinitiveMarker/StreamCompleteSeen/
//     DoneSentinelSeen 断言红；
//   - W33-1 收束集收窄：回退成含 stream_complete → TestDefinitiveMarkerSemantics
//     红（complete 不再收束——尾帧 burst 捕获依赖它）；
//   - W33-1 DecodePayload 哨兵放行回退 → 引号/内嵌 [DONE] 登记断言红。

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// TestDefinitiveMarkerSemantics 收束判据（W33-1 裁定收窄后）的最小单元面：
// status/end_turn 终态与 message_stream_complete = Terminal 但不 = DefinitiveMarker
// （其后仍有尾读段/流末 burst）；[DONE]（裸/引号/内嵌三形态——W33-1 修复后全部
// 可达）/ error 帧 = DefinitiveMarker（收束点）。
func TestDefinitiveMarkerSemantics(t *testing.T) {
	// status 补丁终态（非收束——其后还有尾读段）。
	p := NewStreamParser()
	if _, err := p.Feed("", []byte(`{"p":"/message/content/parts/0","o":"append","v":"hi"}`)); err != nil {
		t.Fatalf("feed content: %v", err)
	}
	if _, err := p.Feed("", []byte(`{"p":"","o":"patch","v":[{"p":"/message/status","o":"replace","v":"finished_successfully"},{"p":"/message/end_turn","o":"replace","v":true}]}`)); err != nil {
		t.Fatalf("feed status patch: %v", err)
	}
	if !p.Terminal() {
		t.Fatalf("status/end_turn 终态后 Terminal() = false")
	}
	if p.DefinitiveMarker() {
		t.Fatalf("status/end_turn 终态后 DefinitiveMarker() = true（want false——尾读段仍可能到达）")
	}
	if p.StreamCompleteSeen() || p.DoneSentinelSeen() {
		t.Fatalf("stream_complete/done 登记误置")
	}

	// message_stream_complete 帧（终态但**非**收束——W33-1：其后仍有
	// [conversation_detail_metadata]/[DONE] 流末 burst；follow-up [DONE] 收束）。
	p2 := NewStreamParser()
	if _, err := p2.Feed("", []byte(`{"type": "message_stream_complete", "conversation_id": "c1"}`)); err != nil {
		t.Fatalf("feed stream_complete: %v", err)
	}
	if !p2.Terminal() {
		t.Fatalf("stream_complete 后 Terminal() = false（want true——宽限武装面）")
	}
	if p2.DefinitiveMarker() || !p2.StreamCompleteSeen() || p2.DoneSentinelSeen() {
		t.Fatalf("stream_complete 登记 = definitive:%v complete:%v done:%v（want false/true/false——complete 不再收束）",
			p2.DefinitiveMarker(), p2.StreamCompleteSeen(), p2.DoneSentinelSeen())
	}
	// 流末 burst：detail（登记面，非收束信号）+ [DONE]（收束面）。
	if _, err := p2.Feed("", []byte(`{"type": "conversation_detail_metadata", "default_model_slug": "auto", "conversation_id": "c1"}`)); err != nil {
		t.Fatalf("feed detail: %v", err)
	}
	if p2.DefinitiveMarker() {
		t.Fatalf("detail 帧后 DefinitiveMarker() = true（want false——detail 非收束信号）")
	}
	if len(p2.ConversationDetailMetadata()) == 0 {
		t.Fatalf("detail 帧未登记")
	}
	if _, err := p2.Feed("", []byte(`[DONE]`)); err != nil {
		t.Fatalf("feed [DONE]: %v", err)
	}
	if !p2.DefinitiveMarker() || !p2.DoneSentinelSeen() {
		t.Fatalf("complete→detail→[DONE] 后 definitive:%v done:%v（want true/true——流末哨兵收束）",
			p2.DefinitiveMarker(), p2.DoneSentinelSeen())
	}

	// 裸 [DONE]。
	p3 := NewStreamParser()
	if _, err := p3.Feed("", []byte(`[DONE]`)); err != nil {
		t.Fatalf("feed [DONE]: %v", err)
	}
	if !p3.DefinitiveMarker() || !p3.DoneSentinelSeen() || p3.StreamCompleteSeen() {
		t.Fatalf("裸 [DONE] 登记 = definitive:%v complete:%v done:%v（want true/false/true）",
			p3.DefinitiveMarker(), p3.StreamCompleteSeen(), p3.DoneSentinelSeen())
	}

	// 引号包裹 [DONE]（代理改写形态；W33-1 修复后可达——DecodePayload 放行
	// 解层后的裸 [DONE]，feedPayload 的哨兵分支自动跟上）。
	p4 := NewStreamParser()
	if _, err := p4.Feed("", []byte(`"[DONE]"`)); err != nil {
		t.Fatalf("feed quoted [DONE]: %v", err)
	}
	if !p4.DefinitiveMarker() || !p4.DoneSentinelSeen() || p4.StreamCompleteSeen() {
		t.Fatalf("引号 [DONE] 登记 = definitive:%v complete:%v done:%v（want true/false/true）",
			p4.DefinitiveMarker(), p4.StreamCompleteSeen(), p4.DoneSentinelSeen())
	}

	// 内嵌 `data: [DONE]`（双重包装形态；W33-1 修复后可达）。
	p4b := NewStreamParser()
	if _, err := p4b.Feed("", []byte(`data: [DONE]`)); err != nil {
		t.Fatalf("feed embedded [DONE]: %v", err)
	}
	if !p4b.DefinitiveMarker() || !p4b.DoneSentinelSeen() || p4b.StreamCompleteSeen() {
		t.Fatalf("内嵌 [DONE] 登记 = definitive:%v complete:%v done:%v（want true/false/true）",
			p4b.DefinitiveMarker(), p4b.StreamCompleteSeen(), p4b.DoneSentinelSeen())
	}

	// 流内 error 帧（definitive——终态错误）。
	p5 := NewStreamParser()
	if _, err := p5.Feed("", []byte(`{"error": "Unusual activity detected"}`)); err != nil {
		t.Fatalf("feed error: %v", err)
	}
	if !p5.DefinitiveMarker() || !p5.Terminal() {
		t.Fatalf("error 帧后 definitive:%v terminal:%v（want true/true）", p5.DefinitiveMarker(), p5.Terminal())
	}

	// 空流：全 false。
	p6 := NewStreamParser()
	if p6.Terminal() || p6.DefinitiveMarker() {
		t.Fatalf("空流 Terminal/DefinitiveMarker = true")
	}
}

// goldenTailWant 三份 golden 的尾部帧登记期望（帧序逐一对照 frames_compare.txt）。
type goldenTailWant struct {
	markers        []string // 登记顺序（Marker 名）
	title          string
	streamComplete bool
	done           bool
	detail         bool // conversation_detail_metadata 是否出现
}

var goldenTails = map[string]goldenTailWant{
	"anon_v1_direct.sse.txt": {
		markers:        []string{"user_visible_token", "final_channel_token", "last_token"},
		title:          "Count to ten",
		streamComplete: true,
		done:           true,
		detail:         true,
	},
	"anon_v1_isp.sse.txt": {
		markers:        []string{"cot_token", "user_visible_token", "final_channel_token", "last_token"},
		title:          "Reply PONG",
		streamComplete: true,
		done:           true,
		detail:         false,
	},
	"anon_snapshot.sse.txt": {
		markers:        []string{"user_visible_token", "final_channel_token", "last_token"},
		title:          "Pong reply",
		streamComplete: true,
		done:           true,
		detail:         true,
	},
}

// TestGoldenTailFramesRegistered 三份 golden 的尾部帧登记：marker（last_token
// 等）/stream_complete/[DONE]/title/detail——W31-1 §4 尾部帧观测面逐帧对齐。
func TestGoldenTailFramesRegistered(t *testing.T) {
	for name, want := range goldenTails {
		t.Run(name, func(t *testing.T) {
			_, p, err := goldenStream(t, name)
			goldenNoErr(t, name, err)
			got := p.Markers()
			if len(got) != len(want.markers) {
				t.Fatalf("%s: Markers() = %d 条（want %d）: %+v", name, len(got), len(want.markers), got)
			}
			for i, w := range want.markers {
				if got[i].Marker != w {
					t.Fatalf("%s: Markers()[%d] = %q（want %q）", name, i, got[i].Marker, w)
				}
				if got[i].MessageID == "" {
					t.Fatalf("%s: Markers()[%d] 缺 MessageID（登记面须带消息关联）", name, i)
				}
			}
			if last := got[len(got)-1]; last.Marker != "last_token" || last.Event != "last" {
				t.Fatalf("%s: 末条 marker = %+v（want last_token/last——尾部第一帧）", name, last)
			}
			if p.MarkerOverflow() != 0 {
				t.Fatalf("%s: MarkerOverflow() = %d（want 0）", name, p.MarkerOverflow())
			}
			if p.Title() != want.title {
				t.Fatalf("%s: Title() = %q（want %q）", name, p.Title(), want.title)
			}
			if p.StreamCompleteSeen() != want.streamComplete || p.DoneSentinelSeen() != want.done {
				t.Fatalf("%s: stream_complete=%v done=%v（want %v/%v）",
					name, p.StreamCompleteSeen(), p.DoneSentinelSeen(), want.streamComplete, want.done)
			}
			if !p.DefinitiveMarker() {
				t.Fatalf("%s: DefinitiveMarker() = false（金标尾部含 stream_complete/[DONE]）", name)
			}
			hasDetail := len(p.ConversationDetailMetadata()) > 0
			if hasDetail != want.detail {
				t.Fatalf("%s: detail 记录 = %v（want %v）", name, hasDetail, want.detail)
			}
		})
	}
}

// TestGoldenTruncatedTailAccessor 截断回归（W31-1 实证流式 4/4 的真实形态）：
// ISP golden 截到 last_token 帧为止（尾部 title/ste/stream_complete/[DONE] 未到）
// ——last_token 必须可见（登记），definitive marker 全 false（服务壳据此判定
// 「终态无 marker → 有界宽限」），正文照常产出。
func TestGoldenTruncatedTailAccessor(t *testing.T) {
	const name = "anon_v1_isp.sse.txt"
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	sse := string(raw)
	// 截断点 = last_token marker 帧之后（含该帧）：帧边界 = 下一个空行。
	idx := strings.Index(sse, `"marker": "last_token"`)
	if idx < 0 {
		t.Fatalf("golden 不含 last_token marker 帧——测试前提不成立")
	}
	end := strings.Index(sse[idx:], "\n\n")
	if end < 0 {
		t.Fatalf("last_token 帧后无空行——帧边界不可定位")
	}
	cut := sse[:idx+end+2]

	frames, err := SplitFrames(strings.NewReader(cut))
	if err != nil {
		t.Fatalf("split truncated: %v", err)
	}
	p := NewStreamParser()
	var text strings.Builder
	for _, fr := range frames {
		evs, ferr := p.Feed(fr.Event, []byte(fr.Data))
		if ferr != nil {
			t.Fatalf("feed truncated frame (event=%q data=%.60s): %v", fr.Event, fr.Data, ferr)
		}
		for _, ev := range evs {
			if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockText {
				text.WriteString(ev.TextDelta)
			}
		}
	}
	events := p.Finish()
	if len(events) != 0 {
		t.Fatalf("截断流 Finish() 产出 %d 事件（终态已见——幂等 want 0）", len(events))
	}

	// last_token 可见（截断点证据帧）。
	ms := p.Markers()
	if len(ms) == 0 || ms[len(ms)-1].Marker != "last_token" {
		t.Fatalf("截断流 Markers() = %+v（want 末条 last_token）", ms)
	}
	// definitive marker 全 false：服务壳据此判定「终态无 marker → 有界宽限」。
	if p.DefinitiveMarker() || p.StreamCompleteSeen() || p.DoneSentinelSeen() {
		t.Fatalf("截断流 definitive:%v complete:%v done:%v（want 全 false）",
			p.DefinitiveMarker(), p.StreamCompleteSeen(), p.DoneSentinelSeen())
	}
	if !p.Terminal() {
		t.Fatalf("截断流 Terminal() = false（status 补丁已终态）")
	}
	// 正文照常（截断不伤正文——W31-1 实证 PONG 完整）。
	if text.String() != "PONG" {
		t.Fatalf("截断流正文 = %q（want PONG）", text.String())
	}
	// title 未到（被截断）——W31-1 观测日志 title="" 的直证形态。
	if p.Title() != "" {
		t.Fatalf("截断流 Title() = %q（want 空——title_generation 在截断点后）", p.Title())
	}
}

// TestMarkerOverflowBounded marker 登记上限：超 maxMarkerRecords 丢新帧并计数
// （防畸形流内存无界；已登记的前缀保留）。
func TestMarkerOverflowBounded(t *testing.T) {
	p := NewStreamParser()
	for i := range maxMarkerRecords + 2 {
		if _, err := p.Feed("", []byte(`{"type": "message_marker", "message_id": "m1", "marker": "last_token", "event": "last"}`)); err != nil {
			t.Fatalf("feed marker %d: %v", i, err)
		}
	}
	if got := len(p.Markers()); got != maxMarkerRecords {
		t.Fatalf("Markers() = %d（want 上限 %d）", got, maxMarkerRecords)
	}
	if got := p.MarkerOverflow(); got != 2 {
		t.Fatalf("MarkerOverflow() = %d（want 2）", got)
	}
}
