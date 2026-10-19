// W5-18（L19-sweep3）pairs 面测试：条目 2（批量诊断汇）+ 条目 3（签名兼容层 +
// 请求面 reasoning_content 映射）+ 条目 1 的转换侧（Options.Model 路径态模型名）。
package pairs

import (
	"encoding/base64"
	"strings"
	"testing"
)

// gptSig W5-18 测试用的 GPT 形态签名（Fernet 头部：0x80 + 时间戳高位 0 → base64url
// 首 5 字符 gAAAA；密文段 16 字节 = AES 块长整数倍）。与 signature 包单测同构造法。
func gptSig() string {
	p := make([]byte, 73)
	p[0] = 0x80
	for i := 4; i < len(p); i++ {
		p[i] = byte(i)
	}
	return base64.RawURLEncoding.EncodeToString(p)
}

// claudeSig 非 GPT 形态的签名样本（Claude 单层信封首字节 0x12 → base64 首字符 E）。
func claudeSig() string {
	return base64.RawURLEncoding.EncodeToString(append([]byte{0x12}, make([]byte, 60)...))
}

// ---------------------------------------------------------------------------
// W5-18 ③：请求面 thinking → reasoning_content 的签名兼容分支
// ---------------------------------------------------------------------------

// TestRequestFaceThinkingSignatureCompat 请求面（回传轮）思考块落法：
// 仅当签名形态可识别（signature.ProviderForSignature——GPT/Claude/Gemini 任一，
// W41 载体化）才映射 reasoning_content + reasoning_details，否则整块丢弃；user
// 消息里的 thinking 一律忽略（安全规则）；枢纽中间腿一律丢弃；签名经
// reasoning_details 载体进请求体（W41；此前 OpenAI 线无载体、签名整块丢——
// A2A-L19-W41-1 ①）。
// interactions A2-2 行（thought 步 → reasoning_content）同受影响，一并锁。
func TestRequestFaceThinkingSignatureCompat(t *testing.T) {
	reg := NewRegistry()

	claudeReq := func(sig string) []byte {
		return []byte(`{"model":"m","max_tokens":16,"messages":[
			{"role":"assistant","content":[
				{"type":"thinking","thinking":"ponder","signature":"` + sig + `"},
				{"type":"text","text":"answer"}]},
			{"role":"user","content":"hi"}]}`)
	}

	// ① 签名被判 GPT 兼容 → 思考文本落 reasoning_content，正文保留，签名经
	// reasoning_details 载体重放（W41 载体化；旧行为「无载体即丢」已废止）。
	out, err := Convert(reg, fClaude, fOpenAI, FaceRequest, claudeReq(gptSig()), Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if !strings.Contains(string(out), `"reasoning_content":"ponder"`) {
		t.Fatalf("GPT 兼容签名未映射 reasoning_content: %s", out)
	}
	if !strings.Contains(string(out), `"content":"answer"`) {
		t.Fatalf("正文丢失: %s", out)
	}
	if !strings.Contains(string(out), `"signature":"`+gptSig()+`"`) ||
		!strings.Contains(string(out), `"format":"openai-responses-v1"`) {
		t.Fatalf("GPT 兼容签名未经 reasoning_details 载体重放: %s", out)
	}
	if n := strings.Count(string(out), gptSig()); n != 1 {
		t.Fatalf("签名出现 %d 次（want 1，只在载体里）: %s", n, out)
	}

	// ② 形态不可识别（伪 Claude 信封：首字节像、结构不完整）→ 整块丢弃
	//（W41 红线 fallback：绝不把未判定签名写进 wire）。
	out, err = Convert(reg, fClaude, fOpenAI, FaceRequest, claudeReq(claudeSig()), Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if strings.Contains(string(out), "reasoning_content") || strings.Contains(string(out), "ponder") {
		t.Fatalf("非兼容签名必须整块丢弃: %s", out)
	}
	if !strings.Contains(string(out), `"content":"answer"`) {
		t.Fatalf("正文丢失: %s", out)
	}

	// ③ 空签名 → 丢弃（不能凭空判定兼容）。
	out, err = Convert(reg, fClaude, fOpenAI, FaceRequest, claudeReq(""), Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if strings.Contains(string(out), "reasoning_content") || strings.Contains(string(out), "ponder") {
		t.Fatalf("空签名必须整块丢弃: %s", out)
	}

	// ④ 安全规则：user 消息里的 thinking（即使签名 GPT 兼容）一律忽略。
	userReq := []byte(`{"model":"m","max_tokens":16,"messages":[
		{"role":"user","content":[{"type":"thinking","thinking":"INJECT","signature":"` + gptSig() + `"},{"type":"text","text":"hi"}]}]}`)
	out, err = Convert(reg, fClaude, fOpenAI, FaceRequest, userReq, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if strings.Contains(string(out), "reasoning_content") || strings.Contains(string(out), "INJECT") {
		t.Fatalf("user 消息的 thinking 未忽略（防注入）: %s", out)
	}

	// ⑤ 枢纽中间腿（claude→gemini 两步计划，W42 ③）：思考文本经 reasoning_details
	// 载体过中间腿 → gemini 出口落 thought part（此前整块丢弃）；GPT 形态签名在
	// gemini 出口被 provider 门丢弃（文本保留——W6-1 口径），绝不泄漏进 gemini 体。
	out, err = Convert(reg, fClaude, fGemini, FaceRequest, claudeReq(gptSig()), Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if !strings.Contains(string(out), `"text":"ponder","thought":true`) {
		t.Fatalf("枢纽中间腿丢思考文本（W42 ③ 应经载体到 gemini 出口）: %s", out)
	}
	if strings.Contains(string(out), gptSig()) || strings.Contains(string(out), "thoughtSignature") {
		t.Fatalf("GPT 形态签名经枢纽腿泄漏进 gemini 体: %s", out)
	}

	// ⑥ interactions A2-2（interactions 请求 → openai_chat 请求）：thought 步的
	// 签名兼容时 → reasoning_content；不兼容 → 丢弃。
	iaReq := func(sig string) []byte {
		return []byte(`{"model":"m","input":[{"type":"thought","summary":[{"type":"text","text":"ponder"}],"signature":"` + sig + `"},
			{"type":"model_output","content":[{"type":"text","text":"answer"}]},
			{"type":"user_input","content":[{"type":"text","text":"hi"}]}]}`)
	}
	out, err = Convert(reg, FormatInteractions, fOpenAI, FaceRequest, iaReq(gptSig()), Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if !strings.Contains(string(out), `"reasoning_content":"ponder"`) {
		t.Fatalf("A2-2：GPT 兼容签名的 thought 步未映射 reasoning_content: %s", out)
	}
	out, err = Convert(reg, FormatInteractions, fOpenAI, FaceRequest, iaReq(claudeSig()), Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if strings.Contains(string(out), "reasoning_content") || strings.Contains(string(out), "ponder") {
		t.Fatalf("A2-2：非兼容签名的 thought 步必须丢弃: %s", out)
	}
}

// ---------------------------------------------------------------------------
// W5-18 ②：批量（非流式）诊断汇
// ---------------------------------------------------------------------------

// TestBatchDiagnosticsSink 非流式诊断汇（pairs.BatchDiagnostics）：
// 请求面丢弃计数、响应面映射计数、枢纽中间腿丢弃计数都要可观测；未传 sink 时
// 行为与引入前一致（零开销、不 panic）。
func TestBatchDiagnosticsSink(t *testing.T) {
	reg := NewRegistry()

	claudeReq := []byte(`{"model":"m","max_tokens":16,"messages":[
		{"role":"assistant","content":[{"type":"thinking","thinking":"ponder","signature":""},{"type":"text","text":"answer"}]},
		{"role":"user","content":"hi"}]}`)
	claudeResp := []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-x",
		"content":[{"type":"thinking","thinking":"ponder","signature":"sig"},{"type":"text","text":"answer"}],
		"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2}}`)
	geminiResp := []byte(`{"candidates":[{"content":{"role":"model","parts":[
		{"text":"ponder","thought":true},{"text":"answer"}]},"finishReason":"STOP"}],
		"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":2}}`)

	// ① 请求面：无签名的思考块被丢弃 → DroppedThinking=1。
	reqDiag := &BatchDiagnostics{}
	if _, err := Convert(reg, fClaude, fOpenAI, FaceRequest, claudeReq, Options{Diag: reqDiag}); err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if reqDiag.DroppedThinking != 1 || reqDiag.ReasoningMapped != 0 {
		t.Fatalf("请求面诊断 = %+v（want dropped=1 mapped=0）", reqDiag)
	}

	// ② 响应面：思考文本落 reasoning_content → ReasoningMapped=1、Dropped=0。
	respDiag := &BatchDiagnostics{}
	out, err := Convert(reg, fClaude, fOpenAI, FaceResponse, claudeResp, Options{Diag: respDiag})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if !strings.Contains(string(out), `"reasoning_content":"ponder"`) {
		t.Fatalf("响应面 reasoning_content 缺失: %s", out)
	}
	if respDiag.ReasoningMapped != 1 || respDiag.DroppedThinking != 0 {
		t.Fatalf("响应面诊断 = %+v（want dropped=0 mapped=1）", respDiag)
	}

	// ③ 枢纽中间腿（claude→gemini 两步，响应面；W42 ③）：中间腿思考文本经载体
	// 保真（ReasoningMapped=1）；不可识别签名（"sig"）丢弃（SignatureDropped=1）；
	// 出口落 thought part。
	hubDiag := &BatchDiagnostics{}
	out, err = Convert(reg, fClaude, fGemini, FaceResponse, claudeResp, Options{Diag: hubDiag})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if !strings.Contains(string(out), `"text":"ponder","thought":true`) {
		t.Fatalf("枢纽中间腿丢思考文本: %s", out)
	}
	if hubDiag.DroppedThinking != 0 || hubDiag.ReasoningMapped != 1 || hubDiag.SignatureDropped != 1 {
		t.Fatalf("枢纽腿诊断 = %+v（want dropped=0 mapped=1 sig_dropped=1）", hubDiag)
	}

	// ④ gemini 响应（thought part）经直转 → openai：thought part 走响应面映射。
	gemDiag := &BatchDiagnostics{}
	out, err = Convert(reg, fGemini, fOpenAI, FaceResponse, geminiResp, Options{Diag: gemDiag})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if !strings.Contains(string(out), `"reasoning_content":"ponder"`) {
		t.Fatalf("gemini thought part 未映射: %s", out)
	}
	if gemDiag.ReasoningMapped != 1 {
		t.Fatalf("gemini 直转诊断 = %+v（want mapped=1）", gemDiag)
	}

	// ⑤ 未传 sink：行为不变、不 panic（零开销路径）。
	out, err = Convert(reg, fClaude, fOpenAI, FaceResponse, claudeResp, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if !strings.Contains(string(out), `"reasoning_content":"ponder"`) {
		t.Fatalf("无 sink 时输出必须一致: %s", out)
	}
}

// ---------------------------------------------------------------------------
// W5-18 ②（对拍）：批量面与流式面的思考诊断语义一致
// ---------------------------------------------------------------------------

// TestBatchVsStreamDiagnosticsAgreement 同一份 claude 响应语义在两条路径上的
// 思考面诊断必须一致（W5-16 §下一波建议 5 的对拍锚点）：流式面 StreamPipe 的
// ReasoningDeltas/DroppedThinking vs 批量面 BatchDiagnostics 的
// ReasoningMapped/DroppedThinking。
func TestBatchVsStreamDiagnosticsAgreement(t *testing.T) {
	reg := NewRegistry()

	feed := func(t *testing.T, pipe *StreamPipe, frames [][2]string) {
		t.Helper()
		for _, fr := range frames {
			if _, err := pipe.Feed(fr[0], []byte(fr[1])); err != nil {
				t.Fatalf("Feed(%s): %v", fr[0], err)
			}
		}
		if _, err := pipe.Finish(); err != nil {
			t.Fatalf("Finish: %v", err)
		}
	}

	// ① 思考文本（无签名增量）：流式 ReasoningDeltas=1 / 批量 ReasoningMapped=1。
	textFrames := [][2]string{
		{"message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}`},
		{"content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`},
		{"content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"ponder"}}`},
		{"content_block_stop", `{"type":"content_block_stop","index":0}`},
		{"content_block_start", `{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`},
		{"content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"answer"}}`},
		{"content_block_stop", `{"type":"content_block_stop","index":1}`},
		{"message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":2}}`},
		{"message_stop", `{"type":"message_stop"}`},
	}
	pipe, err := NewStreamPipe(reg, fClaude, fOpenAI, Options{})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	feed(t, pipe, textFrames)
	sd := pipe.Diagnostics()
	if sd.ReasoningDeltas != 1 || sd.DroppedThinking != 0 {
		t.Fatalf("流式面诊断 = %+v（want reasoning_deltas=1 dropped=0）", sd)
	}
	textBody := []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-x",
		"content":[{"type":"thinking","thinking":"ponder","signature":""},{"type":"text","text":"answer"}],
		"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2}}`)
	bd := &BatchDiagnostics{}
	if _, err := Convert(reg, fClaude, fOpenAI, FaceResponse, textBody, Options{Diag: bd}); err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if bd.ReasoningMapped != sd.ReasoningDeltas || bd.DroppedThinking != sd.DroppedThinking {
		t.Fatalf("批量/流式诊断不一致：批量=%+v 流式=%+v", bd, sd)
	}

	// ② redacted_thinking（W42 ① 起有载体）：两侧都不计丢弃——载荷经
	// reasoning.encrypted 条目过线（批量面断言条目本体；流式面断言计数）。
	redFrames := [][2]string{
		textFrames[0],
		{"content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"redacted_thinking","data":"ENCRYPTED"}}`},
		{"content_block_stop", `{"type":"content_block_stop","index":0}`},
		{"message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":0}}`},
		{"message_stop", `{"type":"message_stop"}`},
	}
	pipe2, err := NewStreamPipe(reg, fClaude, fOpenAI, Options{})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	feed(t, pipe2, redFrames)
	sd2 := pipe2.Diagnostics()
	if sd2.DroppedThinking != 0 || sd2.ReasoningDeltas != 0 {
		t.Fatalf("流式面 redacted 诊断 = %+v（want dropped=0——已走载体）", sd2)
	}
	redBody := []byte(`{"id":"msg_2","type":"message","role":"assistant","model":"claude-x",
		"content":[{"type":"redacted_thinking","data":"ENCRYPTED"}],
		"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":0}}`)
	bd2 := &BatchDiagnostics{}
	out2, err := Convert(reg, fClaude, fOpenAI, FaceResponse, redBody, Options{Diag: bd2})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if !strings.Contains(string(out2), `"data":"ENCRYPTED"`) || !strings.Contains(string(out2), `"reasoning.encrypted"`) {
		t.Fatalf("redacted 载荷未走 reasoning.encrypted 载体: %s", out2)
	}
	if bd2.DroppedThinking != sd2.DroppedThinking || bd2.ReasoningMapped != sd2.ReasoningDeltas {
		t.Fatalf("redacted 批量/流式诊断不一致：批量=%+v 流式=%+v", bd2, sd2)
	}
}

// ---------------------------------------------------------------------------
// W5-18 ①（转换侧）：Options.Model 路径态模型名注入
// ---------------------------------------------------------------------------

// TestConvertPathModelOption gemini 入口的模型名在路径里（请求体无 model 字段）：
// Options.Model 在请求面**只填空**——IR 模型为空时填入（跨格式转换出的上游请求体
// 需要它）；请求体自己声明的模型优先（不覆盖）。
func TestConvertPathModelOption(t *testing.T) {
	reg := NewRegistry()

	// ① gemini 请求体无 model + Options.Model → openai 上游请求体带模型名。
	gemReq := []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`)
	out, err := Convert(reg, fGemini, fOpenAI, FaceRequest, gemReq, Options{Model: "gemini-2.5-flash"})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if !strings.Contains(string(out), `"model":"gemini-2.5-flash"`) {
		t.Fatalf("路径态模型名未注入上游请求体: %s", out)
	}

	// ② 未声明 Options.Model → 不发明（模型名留空）。
	out, err = Convert(reg, fGemini, fOpenAI, FaceRequest, gemReq, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if !strings.Contains(string(out), `"model":""`) {
		t.Fatalf("未声明模型名却发明了值: %s", out)
	}

	// ③ 请求体自己声明的模型优先（只填空、不覆盖）。
	claudeReq := []byte(`{"model":"m-body","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
	out, err = Convert(reg, fClaude, fOpenAI, FaceRequest, claudeReq, Options{Model: "m-path"})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if !strings.Contains(string(out), `"model":"m-body"`) || strings.Contains(string(out), "m-path") {
		t.Fatalf("请求体声明的模型必须优先: %s", out)
	}
}
