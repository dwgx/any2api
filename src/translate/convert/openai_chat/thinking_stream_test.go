package openai_chat

import (
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// P53 缺口 2（thinking 过 openai 枢纽腿断链）回归：
//   - 枢纽中间腿（HubLeg）：thinking 一律丢弃 + 诊断计数，绝不报
//     ErrUnsupportedBlock（否则 claude→gemini 等经枢纽的链路整条断）；
//   - 直转面（给真实消费者）：思考文本走 delta.reasoning_content
//     （longtail-matrix §4.3 / anthropic-conversion-table 行：生态既有约定）；
//     仅签名 / redacted 载荷无载体 → 丢弃 + 计数。

func thinkingStreamEvents() []ir.StreamEvent {
	return []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "c1", Model: "m", Created: 1},
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockThinking},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockThinking, TextDelta: "hmm"},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockThinking, SignatureDelta: "sig"},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockThinking},
		{Kind: ir.EventBlockStart, BlockIndex: 1, BlockKind: ir.BlockRedactedThinking},
		{Kind: ir.EventBlockDelta, BlockIndex: 1, BlockKind: ir.BlockRedactedThinking, SignatureDelta: "b64data"},
		{Kind: ir.EventBlockStop, BlockIndex: 1, BlockKind: ir.BlockRedactedThinking},
		{Kind: ir.EventBlockStart, BlockIndex: 2, BlockKind: ir.BlockText},
		{Kind: ir.EventBlockDelta, BlockIndex: 2, BlockKind: ir.BlockText, TextDelta: "hi"},
		{Kind: ir.EventBlockStop, BlockIndex: 2, BlockKind: ir.BlockText},
		{Kind: ir.EventMessageDelta, StopReason: ir.StopStop},
		{Kind: ir.EventDone, Reason: ir.StopStop},
	}
}

// TestStreamWriterHubLegCarrier 枢纽中间腿（W42 ③ 新行为）：thinking 文本与
// tool/思考签名走 reasoning_content + reasoning_details 载体（内部线成员——
// hub 腿消费者是本仓解析器，出口腿各自按 provider 门落地；此前整块丢弃让
// claude→gemini 等两步链静默丢思考文本）。redacted 载荷例外：中间腿仍丢弃
// （gemini 出口无 redacted 载体，保真会断链）。不报错（链不断）。
//
// W42 说明（本测试此前钉 HubLeg 全丢弃）：载体化后「丢弃保链」的前提消失——
// 中间线可表达思考；「不丢字节」由本测试的载体断言锚定。
func TestStreamWriterHubLegCarrier(t *testing.T) {
	w := NewStreamWriter(StreamRenderOptions{HubLeg: true})
	var out strings.Builder
	for _, ev := range thinkingStreamEvents() {
		b, err := w.Write(ev)
		if err != nil {
			t.Fatalf("hub leg must never break on thinking: %v", err)
		}
		out.Write(b)
	}
	if _, err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	s := out.String()
	if !strings.Contains(s, `"content":"hi"`) {
		t.Fatalf("text block missing: %s", s)
	}
	// W42 ③：思考文本过载体（hub 腿不再丢弃）。
	if !strings.Contains(s, `"reasoning_content":"hmm"`) ||
		!strings.Contains(s, `"reasoning_details":[{"type":"reasoning.text","text":"hmm","index":0}]`) {
		t.Fatalf("hub leg thinking text missing (W42 载体化): %s", s)
	}
	// 不可识别签名（"sig"）仍丢弃（红线）；redacted 载荷（"b64data"）中间腿仍丢弃。
	for _, bad := range []string{"sig", "b64data", "reasoning.encrypted"} {
		if strings.Contains(s, bad) {
			t.Fatalf("hub leg leaked %q: %s", bad, s)
		}
	}
	d := w.Diagnostics()
	if d.DroppedThinking != 1 || d.ReasoningDeltas != 1 || d.SignatureDropped != 1 {
		t.Fatalf("diagnostics = %+v（want DroppedThinking=1[redacted] ReasoningDeltas=1 SignatureDropped=1）", d)
	}
}

// TestStreamWriterDirectFaceReasoningContent 直转面：思考文本 → delta.reasoning_content
// （逐帧），不可识别签名丢弃 + 计数；redacted 载荷 → reasoning.encrypted 条目
// （W42 ①——data ↔ 载荷，此前丢弃）。不报错。
func TestStreamWriterDirectFaceReasoningContent(t *testing.T) {
	w := NewStreamWriter(StreamRenderOptions{})
	var frames []string
	for _, ev := range thinkingStreamEvents() {
		b, err := w.Write(ev)
		if err != nil {
			t.Fatalf("Write(%s): %v", ev.Kind, err)
		}
		if len(b) > 0 {
			frames = append(frames, sseFrames(t, b)...)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	var reasoning []string
	for _, f := range frames {
		if strings.Contains(f, "reasoning_content") {
			reasoning = append(reasoning, f)
		}
	}
	if len(reasoning) != 1 || !strings.Contains(reasoning[0], `"reasoning_content":"hmm"`) {
		t.Fatalf("reasoning frames = %v", reasoning)
	}
	// 不可识别签名（"sig"）不产条目、不泄漏（红线）。
	for _, f := range frames {
		if strings.Contains(f, `"signature"`) {
			t.Fatalf("signature payload leaked: %s", f)
		}
	}
	// W42 ①：redacted 载荷 → 块尾一条 encrypted 条目（format=anthropic-claude-v1）。
	joined := strings.Join(frames, "\n")
	wantEnc := `"reasoning_details":[{"type":"reasoning.encrypted","data":"b64data","format":"anthropic-claude-v1","index":1}]`
	if !strings.Contains(joined, wantEnc) {
		t.Fatalf("redacted 载荷条目缺/变形（want %s）: %s", wantEnc, joined)
	}
	d := w.Diagnostics()
	if d.ReasoningDeltas != 1 || d.DroppedThinking != 0 || d.SignatureDropped != 1 {
		t.Fatalf("diagnostics = %+v（want DroppedThinking=0——redacted 已走载体）", d)
	}
}
