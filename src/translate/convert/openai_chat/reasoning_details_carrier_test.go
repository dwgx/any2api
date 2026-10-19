package openai_chat

// W41 ②（A2A-L19-W41-1）：chat 方言 `reasoning_details` 载体（读写双面）回归。
//
// 修前：OpenAI 线无签名字段——GPT 腿回传轮的签名整块丢（表 §7.1 回传轮行的
// 载体化缺口）。修后：thinking 块 ↔ OpenRouter 兼容 reasoning_details
// （{type: reasoning.text, text, signature, format, index}；longtail-matrix §4.3 /
// P17 + OpenRouter reasoning-tokens.md:880-1030）。
//
// 口径（红线）：签名先过 signature.ProviderForSignature 形态校验——可识别才进
// 载体（format = 判定结果映射的枚举值），不可识别 → 丢弃（绝不写进 wire）。
//
// 反证：
//   - 回退 renderMessage 的载体输出（details 不写进 out）→
//     TestReasoningDetailsWriteFace / TestReasoningDetailsRoundTrip 红；
//   - 回退 parseMessage 的 reasoning_details 读取 → TestReasoningDetailsReadFace 红；
//   - 回退 stream_writer 的签名缓冲/条目 → TestReasoningDetailsStreamWriteFace 红。

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/signature"
)

// 三个形态样本（构造法见 signature/sweep4_test.go；判定前置在 sigSamples 里核对）。
const (
	claudeSig = "EggKBggLEgKquxgB" // Claude 经典 E 信封（CPA 结构）
	geminiSig = "EgcKBQHerb7v"     // Gemini field-2 单记录（Tink 首字节 0x01）
	gptSig    = "gAAAAgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4fICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj9AQUJDREVGRw"
)

func sigSamples(t *testing.T) {
	t.Helper()
	if _, ok := signature.ProviderForSignature(claudeSig); !ok {
		t.Fatal("claudeSig 样本失效")
	}
	if _, ok := signature.ProviderForSignature(geminiSig); !ok {
		t.Fatal("geminiSig 样本失效")
	}
	if _, ok := signature.ProviderForSignature(gptSig); !ok {
		t.Fatal("gptSig 样本失效")
	}
	if _, ok := signature.ProviderForSignature("sha256:abc"); ok {
		t.Fatal("未识别形态被判可识别——红线前提失效")
	}
}

// TestReasoningDetailsWriteFace 写面（非流式响应）：thinking 文本 → reasoning_content
// （兼容通道）**与** reasoning_details（结构化通道）并行；format 按签名形态映射。
func TestReasoningDetailsWriteFace(t *testing.T) {
	sigSamples(t)
	cases := []struct {
		name   string
		sig    string
		format string
	}{
		{"claude", claudeSig, "anthropic-claude-v1"},
		{"gemini", geminiSig, "google-gemini-v1"},
		{"gpt", gptSig, "openai-responses-v1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := &ir.Response{ID: "c1", Model: "m", Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
				{Kind: ir.BlockThinking, Text: "ponder", Signature: tc.sig},
				{Kind: ir.BlockText, Text: "answer"},
			}}}
			out, err := RenderResponse(resp, RenderOptions{})
			if err != nil {
				t.Fatalf("RenderResponse: %v", err)
			}
			var w struct {
				Choices []struct {
					Message struct {
						ReasoningContent string `json:"reasoning_content"`
						ReasoningDetails []struct {
							Type      string `json:"type"`
							Text      string `json:"text"`
							Signature string `json:"signature"`
							Format    string `json:"format"`
							Index     *int   `json:"index"`
						} `json:"reasoning_details"`
					} `json:"message"`
				} `json:"choices"`
			}
			if err := json.Unmarshal(out, &w); err != nil {
				t.Fatalf("unmarshal: %v\n%s", err, out)
			}
			msg := w.Choices[0].Message
			if msg.ReasoningContent != "ponder" {
				t.Fatalf("reasoning_content 兼容通道缺失: %s", out)
			}
			if len(msg.ReasoningDetails) != 1 {
				t.Fatalf("reasoning_details = %+v（want 1 条）: %s", msg.ReasoningDetails, out)
			}
			d := msg.ReasoningDetails[0]
			if d.Type != "reasoning.text" || d.Text != "ponder" || d.Signature != tc.sig || d.Format != tc.format {
				t.Fatalf("条目 = %+v（want text/sig/format=%s）", d, tc.format)
			}
			if d.Index == nil || *d.Index != 0 {
				t.Fatalf("index 缺失/非 0: %+v", d)
			}
		})
	}

	t.Run("不可识别签名不写进载体", func(t *testing.T) {
		resp := &ir.Response{ID: "c2", Model: "m", Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
			{Kind: ir.BlockThinking, Text: "ponder", Signature: "sha256:abc"},
		}}}
		out, err := RenderResponse(resp, RenderOptions{Diag: &BatchDiagnostics{}})
		if err != nil {
			t.Fatalf("RenderResponse: %v", err)
		}
		if strings.Contains(string(out), "sha256:abc") {
			t.Fatalf("未判定签名泄漏进响应: %s", out)
		}
		if !strings.Contains(string(out), `"reasoning_content":"ponder"`) {
			t.Fatalf("文本兼容通道丢失: %s", out)
		}
		// 载体条目在（文本），但不带 signature/format。
		if strings.Contains(string(out), `"signature"`) || strings.Contains(string(out), `"format"`) {
			t.Fatalf("未判定签名不得进条目: %s", out)
		}
	})
}

// TestReasoningDetailsReadFace 读面（非流式请求）：载体 → IR 签名（形态校验）；
// 回传轮 render 输出载体（端到端恰好一次、不重复不丢）。
func TestReasoningDetailsReadFace(t *testing.T) {
	sigSamples(t)
	req, err := ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"user","content":"hi"},
		{"role":"assistant","content":"PONG","reasoning_content":"ponder",
		 "reasoning_details":[{"type":"reasoning.text","text":"ponder","signature":"` + claudeSig + `","format":"anthropic-claude-v1","index":0}]}]}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	m := req.Messages[1]
	if len(m.Blocks) != 2 || m.Blocks[0].Kind != ir.BlockThinking ||
		m.Blocks[0].Text != "ponder" || m.Blocks[0].Signature != claudeSig {
		t.Fatalf("blocks = %+v（want 思考块置首，文本+签名；无重复块）", m.Blocks)
	}
	// 载体被消费：不进 Extra（单一写者——未判定签名绝不回流）。
	if _, ok := m.Extra["reasoning_details"]; ok {
		t.Fatalf("reasoning_details 应被消费（Extra 不得留存）: %+v", m.Extra)
	}

	out, err := RenderRequest(req, RenderOptions{})
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	if n := strings.Count(string(out), `"reasoning_content"`); n != 1 {
		t.Fatalf("reasoning_content 出现 %d 次（want 1）: %s", n, out)
	}
	if n := strings.Count(string(out), `"reasoning_details"`); n != 1 {
		t.Fatalf("reasoning_details 出现 %d 次（want 1）: %s", n, out)
	}
	for _, want := range []string{`"signature":"` + claudeSig + `"`, `"format":"anthropic-claude-v1"`, `"text":"ponder"`} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("回传轮载体缺 %s: %s", want, out)
		}
	}

	t.Run("不可识别签名_读面丢弃且不回流", func(t *testing.T) {
		req, err := ParseRequest([]byte(`{"model":"m","messages":[
			{"role":"assistant","content":"PONG","reasoning_content":"ponder",
			 "reasoning_details":[{"type":"reasoning.text","text":"ponder","signature":"sha256:abc","format":"anthropic-claude-v1","index":0}]}]}`))
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		if sig := req.Messages[0].Blocks[0].Signature; sig != "" {
			t.Fatalf("不可识别签名进了 IR: %q", sig)
		}
		if _, ok := req.Messages[0].Extra["reasoning_details"]; ok {
			t.Fatal("载体应被消费（防未判定签名回流）")
		}
		out, err := RenderRequest(req, RenderOptions{})
		if err != nil {
			t.Fatalf("RenderRequest: %v", err)
		}
		if strings.Contains(string(out), "sha256:abc") {
			t.Fatalf("未判定签名回流进请求: %s", out)
		}
	})

	t.Run("载体只带签名_文本回退reasoning_content", func(t *testing.T) {
		req, err := ParseRequest([]byte(`{"model":"m","messages":[
			{"role":"assistant","content":"PONG","reasoning_content":"ponder",
			 "reasoning_details":[{"type":"reasoning.text","signature":"` + geminiSig + `","index":0}]}]}`))
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		b := req.Messages[0].Blocks[0]
		if b.Kind != ir.BlockThinking || b.Text != "ponder" || b.Signature != geminiSig {
			t.Fatalf("blocks = %+v（want 文本回退 + 载体签名）", b)
		}
	})

	t.Run("载体文本与reasoning_content同帧_不重复", func(t *testing.T) {
		req, err := ParseRequest([]byte(`{"model":"m","messages":[
			{"role":"assistant","content":"PONG","reasoning_content":"ponder",
			 "reasoning_details":[{"type":"reasoning.text","text":"ponder","signature":"` + claudeSig + `","index":0}]}]}`))
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		think := 0
		var text string
		for _, b := range req.Messages[0].Blocks {
			if b.Kind == ir.BlockThinking {
				think++
				text += b.Text
			}
		}
		if think != 1 || text != "ponder" {
			t.Fatalf("思考块 = %d 个 / 文本 %q（want 1 个、不重复）", think, text)
		}
	})
}

// TestReasoningDetailsStreamWriteFace 流式写面：文本增量逐帧出 reasoning_content +
// reasoning_details 文本条目（同 index）；签名增量缓冲到块尾出一条完整条目；
// 不可识别签名丢弃 + SignatureDropped。
func TestReasoningDetailsStreamWriteFace(t *testing.T) {
	sigSamples(t)
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "c1", Model: "m", Created: 1},
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockThinking},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockThinking, TextDelta: "pon"},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockThinking, TextDelta: "der"},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockThinking, SignatureDelta: claudeSig[:6]},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockThinking, SignatureDelta: claudeSig[6:]},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockThinking},
		{Kind: ir.EventBlockStart, BlockIndex: 1, BlockKind: ir.BlockText},
		{Kind: ir.EventBlockDelta, BlockIndex: 1, BlockKind: ir.BlockText, TextDelta: "answer"},
		{Kind: ir.EventBlockStop, BlockIndex: 1, BlockKind: ir.BlockText},
		{Kind: ir.EventMessageDelta, StopReason: ir.StopStop},
		{Kind: ir.EventDone, Reason: ir.StopStop},
	}
	w := NewStreamWriter(StreamRenderOptions{})
	var frames []string
	for _, ev := range events {
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
	joined := strings.Join(frames, "\n")
	// 文本两帧：reasoning_content 与载体文本条目同帧（同 index=0）。
	if n := strings.Count(joined, `"reasoning_content":"pon"`); n != 1 {
		t.Fatalf("文本增量帧 = %d（want 1）: %s", n, joined)
	}
	if !strings.Contains(joined, `"reasoning_details":[{"type":"reasoning.text","text":"pon","index":0}]`) {
		t.Fatalf("载体文本条目缺/变形: %s", joined)
	}
	if !strings.Contains(joined, `"reasoning_details":[{"type":"reasoning.text","text":"der","index":0}]`) {
		t.Fatalf("第二帧载体文本条目缺/变形: %s", joined)
	}
	// 块尾签名条目：完整签名一条（两段增量拼接），带 format。
	wantSigEntry := `"reasoning_details":[{"type":"reasoning.text","signature":"` + claudeSig + `","format":"anthropic-claude-v1","index":0}]`
	if !strings.Contains(joined, wantSigEntry) {
		t.Fatalf("块尾签名条目缺/变形（want %s）: %s", wantSigEntry, joined)
	}
	// 签名两段不得各出一条（消费端签名取最后一条——逐段出会只剩最后一段）。
	if strings.Contains(joined, `"signature":"`+claudeSig[:6]+`"`) {
		t.Fatalf("签名增量未缓冲到块尾: %s", joined)
	}
	if d := w.Diagnostics(); d.ReasoningDeltas != 2 || d.SignatureDropped != 0 {
		t.Fatalf("diagnostics = %+v", d)
	}

	// 不可识别签名：不产条目、计数。
	w2 := NewStreamWriter(StreamRenderOptions{})
	var out2 strings.Builder
	for _, ev := range []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "c2", Model: "m", Created: 1},
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockThinking},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockThinking, SignatureDelta: "sha256:abc"},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockThinking},
		{Kind: ir.EventDone, Reason: ir.StopStop},
	} {
		b, err := w2.Write(ev)
		if err != nil {
			t.Fatalf("Write(%s): %v", ev.Kind, err)
		}
		out2.Write(b)
	}
	if strings.Contains(out2.String(), "sha256") {
		t.Fatalf("未判定签名泄漏: %s", out2.String())
	}
	if d := w2.Diagnostics(); d.SignatureDropped != 1 {
		t.Fatalf("diagnostics = %+v（want SignatureDropped=1）", d)
	}
}

// TestReasoningDetailsStreamReadFace 流式读面：delta.reasoning_details 文本/签名
// → IR（thinking 块 + SignatureDelta）；不可识别签名不进 IR；同帧双通道不重复。
func TestReasoningDetailsStreamReadFace(t *testing.T) {
	sigSamples(t)
	p := NewStreamParser()
	var evs []ir.StreamEvent
	feed := func(s string) {
		t.Helper()
		got, err := p.Feed([]byte(s))
		if err != nil {
			t.Fatalf("Feed(%s): %v", s, err)
		}
		evs = append(evs, got...)
	}
	feed(`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","reasoning_details":[{"type":"reasoning.text","text":"pon","index":0}]}}]}`)
	feed(`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"reasoning_content":"IGNORED","reasoning_details":[{"type":"reasoning.text","text":"der","index":0}]}}]}`)
	feed(`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"reasoning_details":[{"type":"reasoning.text","signature":"` + claudeSig + `","index":0}]}}]}`)
	feed(`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"reasoning_details":[{"type":"reasoning.text","signature":"sha256:abc","index":1}]}}]}`)
	feed(`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"PONG"}}]}`)
	feed(`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)
	feed(`[DONE]`)

	var thinkText, textOut, sigOut strings.Builder
	var starts []ir.StreamEvent
	for _, ev := range evs {
		switch ev.Kind {
		case ir.EventBlockStart:
			starts = append(starts, ev)
		case ir.EventBlockDelta:
			switch ev.BlockKind {
			case ir.BlockThinking:
				thinkText.WriteString(ev.TextDelta)
				sigOut.WriteString(ev.SignatureDelta)
			case ir.BlockText:
				textOut.WriteString(ev.TextDelta)
			}
		}
	}
	if len(starts) != 2 || starts[0].BlockKind != ir.BlockThinking || starts[1].BlockKind != ir.BlockText {
		t.Fatalf("block starts = %+v", starts)
	}
	if thinkText.String() != "ponder" {
		t.Fatalf("思考文本 = %q（want ponder；IGNORED 不得混入）", thinkText.String())
	}
	if textOut.String() != "PONG" {
		t.Fatalf("正文 = %q", textOut.String())
	}
	if sigOut.String() != claudeSig {
		t.Fatalf("签名 = %q（want %q；不可识别签名不得进 IR）", sigOut.String(), claudeSig)
	}
}

// TestReasoningDetailsMalformedTolerance 宽容读取：畸形载体不炸解析（单条跳过）。
func TestReasoningDetailsMalformedTolerance(t *testing.T) {
	req, err := ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"assistant","content":"PONG",
		 "reasoning_details":[123,{"type":"reasoning.text","text":"ok","signature":"` + claudeSig + `"},null]}]}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	b := req.Messages[0].Blocks[0]
	if b.Kind != ir.BlockThinking || b.Text != "ok" || b.Signature != claudeSig {
		t.Fatalf("blocks = %+v", b)
	}
	// 非数组/非对象载体：整条忽略（回退 reasoning_content）。
	req2, err := ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"assistant","content":"PONG","reasoning_content":"rc","reasoning_details":"junk"}]}`))
	if err != nil {
		t.Fatalf("ParseRequest junk: %v", err)
	}
	b2 := req2.Messages[0].Blocks[0]
	if b2.Kind != ir.BlockThinking || b2.Text != "rc" {
		t.Fatalf("blocks = %+v（want reasoning_content 回退）", b2)
	}
}
