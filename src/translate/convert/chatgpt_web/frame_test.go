package chatgpt_web

// W28-1：chatgpt_web 解析器测试。全部 fixture 为**构造帧**（从三实现源码行为
// 反推帧形——标注「构造，非实抓」；真机 golden 帧到位后 S4/S5 重校）。
//
// 装置：tframe 按序喂 StreamParser（增量面），EOF 后 Finish；断言面 = 完整
// IR 事件序列/通道文本/accessor 状态（不是子集计数——关键用例逐事件断言）。
//
// 反证锚点（已实测回退即红，证据见报告 §反证 + `tmp/lanes/W28-chatgpt-s2/proofs/`）：
//   - 删掉 patch.go 空路径允许面（applyValue 的 `s.curP != "" &&`）→ TestPatchEmptyPathIsParts0 红；
//   - 停用 isHistoryEcho 过滤（恒 false）→ TestQuirkMatrix/Q03 红；
//   - 停用 cleanReasoning 的控制串表 → TestQuirkMatrix/Q05 红；
//   - 删掉 applyValue 的 recipient 门 → TestQuirkMatrix/Q04 红。

import (
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// ---------------------------------------------------------------------------
// 测试装置
// ---------------------------------------------------------------------------

// tframe 一条构造帧（event 可空）。
type tframe struct {
	event string
	data  string
}

// dataFrame 便捷构造 data-only 帧。
func dataFrame(data string) tframe { return tframe{data: data} }

// runFrames 按序喂帧（增量面），EOF 后调 Finish，返回全部事件与解析器。
func runFrames(t *testing.T, frames []tframe) ([]ir.StreamEvent, *StreamParser, error) {
	t.Helper()
	p := NewStreamParser()
	var events []ir.StreamEvent
	for _, f := range frames {
		evs, err := p.Feed(f.event, []byte(f.data))
		events = append(events, evs...)
		if err != nil {
			return events, p, err
		}
	}
	events = append(events, p.Finish()...)
	return events, p, nil
}

// mustRun 断言无错误地跑完。
func mustRun(t *testing.T, frames []tframe) ([]ir.StreamEvent, *StreamParser) {
	t.Helper()
	events, p, err := runFrames(t, frames)
	if err != nil {
		t.Fatalf("run frames: %v", err)
	}
	return events, p
}

// collectText 汇总 text/thinking 通道增量。
func collectText(events []ir.StreamEvent) (text, thinking string) {
	for _, ev := range events {
		if ev.Kind != ir.EventBlockDelta {
			continue
		}
		switch ev.BlockKind {
		case ir.BlockThinking:
			thinking += ev.TextDelta
		case ir.BlockText:
			text += ev.TextDelta
		}
	}
	return text, thinking
}

// countKind 统计事件类型出现次数。
func countKind(events []ir.StreamEvent, kind ir.StreamEventKind) int {
	n := 0
	for _, ev := range events {
		if ev.Kind == kind {
			n++
		}
	}
	return n
}

// eventKinds 事件类型序列。
func eventKinds(events []ir.StreamEvent) []ir.StreamEventKind {
	out := make([]ir.StreamEventKind, 0, len(events))
	for _, ev := range events {
		out = append(out, ev.Kind)
	}
	return out
}

// deltaTexts 所有 block_delta 的 (kind, text)。
func deltaTexts(events []ir.StreamEvent) []struct {
	Kind ir.BlockKind
	Text string
} {
	var out []struct {
		Kind ir.BlockKind
		Text string
	}
	for _, ev := range events {
		if ev.Kind == ir.EventBlockDelta {
			out = append(out, struct {
				Kind ir.BlockKind
				Text string
			}{ev.BlockKind, ev.TextDelta})
		}
	}
	return out
}

// containsText 任一事件文本包含子串。
func containsText(events []ir.StreamEvent, sub string) bool {
	for _, ev := range events {
		if strings.Contains(ev.TextDelta, sub) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// 件 1：切帧
// ---------------------------------------------------------------------------

func TestSplitFrames(t *testing.T) {
	raw := "event: delta_encoding\n" +
		"data: \"v1\"\n" +
		"\n" +
		": 注释行（心跳）\n" +
		"data: {\"p\":\"a\"}\n" +
		"data: {\"p\":\"b\"}\n" +
		"\n" +
		"data: [DONE]\r\n" +
		"\r\n" +
		"data: tail-no-blank-line\n" // EOF 派发未闭合帧（A:219-220）
	frames, err := SplitFrames(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("SplitFrames: %v", err)
	}
	if len(frames) != 4 {
		t.Fatalf("frames = %d（want 4）: %+v", len(frames), frames)
	}
	if frames[0].Event != "delta_encoding" || frames[0].Data != `"v1"` {
		t.Fatalf("frame0 = %+v", frames[0])
	}
	// 多行 data 以 "\n" 连接；注释行不产帧。
	if frames[1].Data != "{\"p\":\"a\"}\n{\"p\":\"b\"}" {
		t.Fatalf("frame1 = %+v", frames[1])
	}
	if frames[2].Data != "[DONE]" {
		t.Fatalf("frame2 = %+v", frames[2])
	}
	if frames[3].Data != "tail-no-blank-line" {
		t.Fatalf("frame3 = %+v", frames[3])
	}
}

func TestDecodePayload(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{"plain object", `{"p":"x"}`, `{"p":"x"}`, true},
		{"v1 marker", `"v1"`, `"v1"`, true},
		{"plain string", `"hello"`, `"hello"`, true},
		{"nested json string", `"{\"p\":\"x\"}"`, `{"p":"x"}`, true},
		{"nested array string", `"[1,2]"`, `[1,2]`, true},
		// 字符串里内嵌 SSE 文本：取最后一帧的 data（A:226-228）。
		{"embedded sse in string", `"data: {\"p\":\"x\"}\n\ndata: {\"p\":\"y\"}"`, `{"p":"y"}`, true},
		{"embedded sse raw item", "data: {\"p\":\"x\"}\n\ndata: {\"p\":\"y\"}", `{"p":"y"}`, true},
		// W33-1：解层后暴露的裸 [DONE] 哨兵——三形态汇合（旧逻辑恒 ok=false，
		// 上层 feedPayload 的哨兵容忍分支不可达；W32-1 §2 实证）。
		{"quoted done sentinel", `"[DONE]"`, `[DONE]`, true},
		{"double-quoted done sentinel", `"\"[DONE]\""`, `[DONE]`, true},
		{"embedded done sentinel raw", "data: [DONE]", `[DONE]`, true},
		{"embedded done sentinel in string", `"data: [DONE]"`, `[DONE]`, true},
		{"heartbeat non-json", "hello", "", false},
		{"empty", "   ", "", false},
		{"colon comment remnant", ": keep-alive", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := DecodePayload(c.in)
			if ok != c.ok {
				t.Fatalf("ok = %v（want %v）", ok, c.ok)
			}
			if ok && string(got) != c.want {
				t.Fatalf("got %q（want %q）", got, c.want)
			}
		})
	}
}

func TestParseStreamHeartbeatsAndComments(t *testing.T) {
	// ⑤ 非 JSON 行 / `:` 注释行：忽略（B:66-69；A:213-214）——不炸不断流。
	raw := ": keep-alive\n" +
		"\n" +
		"data: totally not json\n" +
		"\n" +
		"data: {\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":\"hi\"}\n" +
		"\n" +
		"data: [DONE]\n"
	events, err := ParseStream(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	text, _ := collectText(events)
	if text != "hi" {
		t.Fatalf("text = %q（want hi）", text)
	}
	if countKind(events, ir.EventDone) != 1 {
		t.Fatalf("done = %d（want 1）", countKind(events, ir.EventDone))
	}
}
