// W5-19 条目 3/5（pairs 层）：签名兼容层的请求面消费者（claude/gemini 出口）与
// 带签名案例的批量/流式逐项对拍。
package pairs

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/convert/gemini"
	"github.com/dwgx/any2api/translate/ir"
)

// claudeClassicSig 构造一个经典 Claude 信封签名（E 前缀：0x12 → base64 首字符
// 'E'；field 2 container → field 1 channel block → field 1 varint channel_id）。
func claudeClassicSig() string {
	ch := []byte{0x08, 0x0b, 0x12, 0x02, 0xaa, 0xbb}
	container := append([]byte{0x0a, byte(len(ch))}, ch...)
	payload := append(append([]byte{0x12, byte(len(container))}, container...), 0x18, 0x01)
	return base64.StdEncoding.EncodeToString(payload)
}

// geminiThoughtSig 构造一个 Gemini field-2 信封签名（Tink 载荷首字节 0x01）。
func geminiThoughtSig() string {
	value := []byte{0x01, 0xde, 0xad, 0xbe, 0xef}
	container := append([]byte{0x0a, byte(len(value))}, value...)
	payload := append([]byte{0x12, byte(len(container))}, container...)
	return base64.StdEncoding.EncodeToString(payload)
}

// gptFernetSig 构造一个 GPT Fernet 形态签名（73 字节载荷，首字节 0x80）。
func gptFernetSig() string {
	payload := make([]byte, 73)
	payload[0] = 0x80
	enc := base64.StdEncoding.EncodeToString(payload)
	return "gAAAA" + enc[5:]
}

// TestClaudeRequestFaceSignatureGate claude 出口请求面的签名门（表 §7.1
// 「Responses reasoning → Anthropic」行）：thinking 块只在签名被判 Claude 兼容时
// 重放；空/外来签名整块丢弃（Anthropic 拒空签名，绝不硬塞）。
func TestClaudeRequestFaceSignatureGate(t *testing.T) {
	reg := NewRegistry()
	claudeSig := claudeClassicSig()

	cases := []struct {
		name      string
		encrypted string
		wantBlock bool
	}{
		{"claude_compatible_rebuilt", claudeSig, true},
		{"foreign_encrypted_content_dropped", "enc_c1", false},
		{"gpt_signature_dropped", gptFernetSig(), false},
		{"empty_signature_dropped", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reasoning := `{"type":"reasoning","summary":[{"type":"summary_text","text":"ponder"}]`
			if tc.encrypted != "" {
				reasoning += `,"encrypted_content":` + jsonString(tc.encrypted)
			}
			reasoning += `}`
			in := []byte(`{"model":"m","input":[` + reasoning +
				`,{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}` +
				`,{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`)
			out, err := Convert(reg, fResp, fClaude, FaceRequest, in, Options{})
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}
			s := string(out)
			if tc.wantBlock {
				if !strings.Contains(s, `"type":"thinking"`) || !strings.Contains(s, jsonString(claudeSig)) {
					t.Fatalf("Claude 兼容签名未重建 thinking 块:\n%s", s)
				}
			} else {
				if strings.Contains(s, `"type":"thinking"`) || strings.Contains(s, `"signature"`) {
					t.Fatalf("不兼容签名的 thinking 块未丢弃:\n%s", s)
				}
			}
			// 正文与用户消息不受影响。
			if !strings.Contains(s, `"answer"`) || !strings.Contains(s, `"hi"`) {
				t.Fatalf("正文丢失:\n%s", s)
			}
		})
	}

	// 空签名（openai 请求面 reasoning_content 回传轮）→ 整块丢弃。
	t.Run("openai_reasoning_content_empty_signature", func(t *testing.T) {
		in := []byte(`{"model":"m","max_tokens":16,"messages":[
			{"role":"assistant","content":"answer","reasoning_content":"ponder"},
			{"role":"user","content":"hi"}]}`)
		out, err := Convert(reg, fOpenAI, fClaude, FaceRequest, in, Options{})
		if err != nil {
			t.Fatalf("Convert: %v", err)
		}
		if strings.Contains(string(out), `"type":"thinking"`) || strings.Contains(string(out), "ponder") {
			t.Fatalf("无签名思考块未丢弃:\n%s", out)
		}
	})

	// 响应面（RenderResponse）不走本门（给客户端看，签名缺失是可接受降级）：
	// 同一 responses→claude 响应转换保留思考块（与既有 golden 一致）。
	t.Run("response_face_ungated", func(t *testing.T) {
		in := []byte(`{"id":"resp_1","object":"response","created_at":1,"status":"completed","model":"m",
			"output":[{"id":"rs_1","type":"reasoning","status":"completed","summary":[{"type":"summary_text","text":"ponder"}],"encrypted_content":"enc_c1"},
			{"id":"msg_1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"answer","annotations":[]}]}],
			"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}`)
		out, err := Convert(reg, fResp, fClaude, FaceResponse, in, Options{})
		if err != nil {
			t.Fatalf("Convert: %v", err)
		}
		if !strings.Contains(string(out), `"type":"thinking"`) {
			t.Fatalf("响应面思考块被误丢:\n%s", out)
		}
	})
}

// TestGeminiRequestFaceThoughtSignatureGate gemini 出口请求面的 thought 签名门
// （W5-19 条目 3）：thought part 的 thoughtSignature 只在签名被判 Gemini 兼容时
// 重放；外来签名丢弃（thought 文本保留）。
func TestGeminiRequestFaceThoughtSignatureGate(t *testing.T) {
	geminiSig := geminiThoughtSig()
	req := &ir.Request{Model: "gemini-2.5-flash", Messages: []ir.Message{
		{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "hi"}}},
		{
			Role: ir.RoleAssistant,
			Blocks: []ir.ContentBlock{
				{Kind: ir.BlockThinking, Text: "ponder", Signature: geminiSig},
				{Kind: ir.BlockThinking, Text: "junk", Signature: "not-a-gemini-signature"},
				{Kind: ir.BlockThinking, Text: "claude", Signature: claudeClassicSig()},
				{Kind: ir.BlockText, Text: "answer"},
			},
		},
		// 尾随 user 消息：assistant 不是末条（gemini 无 prefill——尾随 model
		// content 会被显式丢弃）。
		{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "next"}}},
	}}
	out, err := gemini.RenderRequest(req)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, `"thoughtSignature":`+jsonString(geminiSig)) {
		t.Fatalf("Gemini 兼容签名未重放:\n%s", s)
	}
	if got := strings.Count(s, `"thoughtSignature"`); got != 1 {
		t.Fatalf("外来签名未被丢弃（thoughtSignature 计数=%d）:\n%s", got, s)
	}
	for _, text := range []string{`"text":"ponder"`, `"text":"junk"`, `"text":"claude"`, `"text":"answer"`} {
		if !strings.Contains(s, text) {
			t.Fatalf("thought 文本丢失 %s:\n%s", text, s)
		}
	}

	// 流式出口同一门（块尾成型路径）。
	w := gemini.NewStreamWriter()
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "m", Model: "gemini-2.5-flash"},
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockThinking},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockThinking, TextDelta: "ponder"},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockThinking, SignatureDelta: geminiSig},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockThinking},
		{Kind: ir.EventBlockStart, BlockIndex: 1, BlockKind: ir.BlockThinking},
		{Kind: ir.EventBlockDelta, BlockIndex: 1, BlockKind: ir.BlockThinking, TextDelta: "junk"},
		{Kind: ir.EventBlockDelta, BlockIndex: 1, BlockKind: ir.BlockThinking, SignatureDelta: "junk-sig"},
		{Kind: ir.EventBlockStop, BlockIndex: 1, BlockKind: ir.BlockThinking},
		{Kind: ir.EventMessageDelta, StopReason: ir.StopStop},
		{Kind: ir.EventDone, Reason: ir.StopStop},
	}
	var stream strings.Builder
	for _, ev := range events {
		b, err := w.Write(ev)
		if err != nil {
			t.Fatalf("Write(%s): %v", ev.Kind, err)
		}
		stream.Write(b)
	}
	b, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	stream.Write(b)
	ss := stream.String()
	if !strings.Contains(ss, `"thoughtSignature":`+jsonString(geminiSig)) {
		t.Fatalf("流式出口未重放 Gemini 兼容签名:\n%s", ss)
	}
	if got := strings.Count(ss, `"thoughtSignature"`); got != 1 {
		t.Fatalf("流式出口外来签名未被丢弃（计数=%d）:\n%s", got, ss)
	}
}

// TestSignedThinkingBatchStreamAgreement 带签名案例的批量/流式逐项对拍
// （W5-19 条目 5：流式单列 SignatureDropped 后两侧口径一致）。
func TestSignedThinkingBatchStreamAgreement(t *testing.T) {
	reg := NewRegistry()
	// 批量（非流式响应面）：claude 响应含带签名思考块 + 正文。
	resp := []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-x",
		"content":[{"type":"thinking","thinking":"pondering","signature":"sig-b64"},{"type":"text","text":"answer"}],
		"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":7}}`)
	var diag BatchDiagnostics
	if _, err := Convert(reg, fClaude, fOpenAI, FaceResponse, resp, Options{Diag: &diag}); err != nil {
		t.Fatalf("Convert: %v", err)
	}
	// 流式（同一语义；claudeThinkingStream 见 exit_repair_test.go）。
	pipe, err := NewStreamPipe(reg, fClaude, fOpenAI, Options{})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	runStreamPipe(t, pipe, claudeThinkingStream())
	sd := pipe.Diagnostics()

	// 逐项对拍：批量 ReasoningMapped ⟷ 流式 ReasoningDeltas；
	// 签名面两侧都单列（W5-19 条目 5）；思考文本面一致。
	if diag.ReasoningMapped != sd.ReasoningDeltas {
		t.Fatalf("思考映射数不一致：批量=%d 流式=%d", diag.ReasoningMapped, sd.ReasoningDeltas)
	}
	if diag.SignatureDropped != sd.SignatureDropped {
		t.Fatalf("签名丢弃数不一致：批量=%d 流式=%d", diag.SignatureDropped, sd.SignatureDropped)
	}
	if diag.DroppedThinking != sd.DroppedThinking {
		t.Fatalf("思考丢弃数不一致：批量=%d 流式=%d", diag.DroppedThinking, sd.DroppedThinking)
	}
	if diag.ReasoningMapped != 1 || diag.SignatureDropped != 1 || diag.DroppedThinking != 0 {
		t.Fatalf("批量口径 = %+v（want mapped=1 signature=1 dropped=0）", diag)
	}
	if sd.ReasoningDeltas != 1 || sd.SignatureDropped != 1 || sd.DroppedThinking != 0 {
		t.Fatalf("流式口径 = %+v（want deltas=1 signature=1 dropped=0）", sd)
	}
}
