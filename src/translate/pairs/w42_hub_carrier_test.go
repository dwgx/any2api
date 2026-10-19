package pairs

// W42 ③（A2A-L19-W42-1）：hub 腿载体保真回归——中间腿（openai_chat，HubLeg）经
// reasoning_details 载体把思考文本/签名带过两步计划（此前整块丢弃：P53 缺口 2）。
// 本文件锚「保签名」的具体落点：入口腿产出的**形态可识别**签名经中间腿往返后，
// 在支持签名载体的出口腿逐字落地；不可识别签名仍丢弃（红线）。
//
// 反证：回退 openai_chat 的 HubLeg 载体分支（renderMessage / stream_writer 的
// hub 腿丢思考）→ 本文件测试红（出口无签名/无文本）。

import (
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/signature"
)

// 形态样本（构造法见 signature/sweep4_test.go）。
const (
	w42HubGeminiSig = "EgcKBQHerb7v"     // Gemini field-2 单记录
	w42HubClaudeSig = "EggKBggLEgKquxgB" // Claude 经典 E 信封
)

func w42HubSamples(t *testing.T) {
	t.Helper()
	if _, ok := signature.ProviderForSignature(w42HubGeminiSig); !ok {
		t.Fatal("w42HubGeminiSig 样本失效")
	}
	if _, ok := signature.ProviderForSignature(w42HubClaudeSig); !ok {
		t.Fatal("w42HubClaudeSig 样本失效")
	}
	if _, ok := signature.ProviderForSignature("SIG-UNRECOGNIZED"); ok {
		t.Fatal("不可识别样本被判可识别——红线前提失效")
	}
}

// TestW42HubCarriesThinkingSignature gemini→responses（经 openai 枢纽两步，无直转
// 对）：thought part 的文本与签名经中间腿载体保真——responses 出口 reasoning item
// 的 summary 文本 + encrypted_content 签名逐字（此前中间腿整块丢弃）。
func TestW42HubCarriesThinkingSignature(t *testing.T) {
	w42HubSamples(t)
	reg := NewRegistry()

	resp := []byte(`{"candidates":[{"index":0,"content":{"role":"model","parts":[
		{"text":"ponder","thought":true,"thoughtSignature":"` + w42HubGeminiSig + `"},
		{"text":"answer"}]},"finishReason":"STOP"}],
		"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":2,"totalTokenCount":3}}`)
	out, err := Convert(reg, fGemini, fResp, FaceResponse, resp, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if !strings.Contains(string(out), `"encrypted_content":"`+w42HubGeminiSig+`"`) {
		t.Fatalf("hub 腿丢签名（responses 出口 encrypted_content 应逐字）: %s", out)
	}
	if !strings.Contains(string(out), "ponder") {
		t.Fatalf("hub 腿丢思考文本（responses 出口 summary）: %s", out)
	}
	if !strings.Contains(string(out), `"text":"answer"`) {
		t.Fatalf("正文丢失: %s", out)
	}
}

// TestW42HubCarriesSignatureToInteractions claude→interactions（经 openai 枢纽两步，
// 无直转对）：thinking 的 Claude 签名经中间腿载体到 interactions 出口 thought 步
// signature 逐字；不可识别签名仍丢弃（红线）。
func TestW42HubCarriesSignatureToInteractions(t *testing.T) {
	w42HubSamples(t)
	reg := NewRegistry()

	resp := func(sig string) []byte {
		return []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"m",
			"content":[{"type":"thinking","thinking":"ponder","signature":"` + sig + `"},
				{"type":"text","text":"answer"}],
			"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2}}`)
	}
	out, err := Convert(reg, fClaude, FormatInteractions, FaceResponse, resp(w42HubClaudeSig), Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if !strings.Contains(string(out), `"signature":"`+w42HubClaudeSig+`"`) {
		t.Fatalf("hub 腿丢签名（interactions 出口 thought 步 signature 应逐字）: %s", out)
	}
	if !strings.Contains(string(out), "ponder") {
		t.Fatalf("hub 腿丢思考文本: %s", out)
	}

	// 不可识别签名：中间腿丢弃（红线），出口不得出现。
	out2, err := Convert(reg, fClaude, FormatInteractions, FaceResponse, resp("SIG-UNRECOGNIZED"), Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if strings.Contains(string(out2), "SIG-UNRECOGNIZED") {
		t.Fatalf("不可识别签名经 hub 腿泄漏: %s", out2)
	}
	if !strings.Contains(string(out2), "ponder") {
		t.Fatalf("文本应保留（只丢签名）: %s", out2)
	}
}

// TestW42HubToolSignatureNotLeaked 工具签名经 hub 腿不泄漏、不断链：gemini 入口
// 请求（functionCall + thoughtSignature）→ claude 出口（无工具签名载体）——
// 签名不得出现在出口体（红线），functionCall 形状照常。
func TestW42HubToolSignatureNotLeaked(t *testing.T) {
	w42HubSamples(t)
	reg := NewRegistry()

	req := []byte(`{"contents":[
		{"role":"user","parts":[{"text":"hi"}]},
		{"role":"model","parts":[{"functionCall":{"name":"f","args":{}},"thoughtSignature":"` + w42HubGeminiSig + `"}]},
		{"role":"user","parts":[{"text":"go on"}]}]}`)
	out, err := Convert(reg, fGemini, fClaude, FaceRequest, req, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if strings.Contains(string(out), w42HubGeminiSig) || strings.Contains(string(out), "thoughtSignature") {
		t.Fatalf("工具签名经 hub 腿泄漏进 claude 出口: %s", out)
	}
	if !strings.Contains(string(out), `"name":"f"`) {
		t.Fatalf("工具调用丢失: %s", out)
	}
}
