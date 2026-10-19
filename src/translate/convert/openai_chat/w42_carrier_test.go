package openai_chat_test

// W42 ①/②（A2A-L19-W42-1）：chat 载体三补回归——
//   ① reasoning.summary 读面（→ 思考文本，text 优先）；reasoning.encrypted
//     读写面（无 tool 配对的 Claude 族载荷 ↔ redacted_thinking；data ↔ 载荷）；
//     `reasoning` 字符串别名读面（reasoning_content 的 OpenRouter 别名）；
//   ② Gemini 3 工具签名经 chat 载体（reasoning.encrypted{id=tool_call.id}）往返：
//     线索复核 = roo-code `src/api/providers/openrouter.ts:247-286`（注入
//     {type:"reasoning.encrypted", data:"skip_thought_signature_validator",
//     id:toolCalls[0].id, format:"google-gemini-v1", index:0}）+
//     `src/api/transform/openai-format.ts:149-235`（sanitizeGeminiMessages 按
//     d.id === tool_call.id 配对）。
//
// 反证：回退 reasoning_details.go 的 readReasoningCarrier / applyEncryptedCarrier /
// reasoningToolSignature / renderMessage 的 encrypted 输出 / parseChunk 的
// feedEncryptedDetail → 对应测试红（见报告 §反证）。

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/convert/claude_messages"
	"github.com/dwgx/any2api/translate/convert/gemini"
	"github.com/dwgx/any2api/translate/convert/openai_chat"
	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/signature"
)

const (
	w42GeminiSig   = "EgcKBQHerb7v"                     // Gemini field-2 单记录（sweep4 同构造法）
	w42ClaudeSig   = "EggKBggLEgKquxgB"                 // Claude 经典 E 信封
	w42Placeholder = "skip_thought_signature_validator" // Google 官方占位符
)

func w42CheckSamples(t *testing.T) {
	t.Helper()
	if p, ok := signature.ProviderForSignature(w42GeminiSig); !ok || p != signature.ProviderGemini {
		t.Fatal("w42GeminiSig 样本失效")
	}
	if _, ok := signature.ProviderForSignature(w42ClaudeSig); !ok {
		t.Fatal("w42ClaudeSig 样本失效")
	}
	if _, ok := signature.ProviderForSignature(w42Placeholder); ok {
		t.Fatal("占位符被判形态可识别——magic value 前提失效")
	}
}

func w42ThinkingBlocks(msg ir.Message) []ir.ContentBlock {
	var out []ir.ContentBlock
	for _, b := range msg.Blocks {
		if b.Kind == ir.BlockThinking {
			out = append(out, b)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// ① reasoning.summary 读面
// ---------------------------------------------------------------------------

// TestW42SummaryReadFace summary 文本 → 思考文本（reasoning.text 缺省回退；
// text 存在时 summary 不混入——消费端 consolidate 口径）。
func TestW42SummaryReadFace(t *testing.T) {
	// ① summary-only 载体 → 思考块（文本 = summary 拼接）。
	req, err := openai_chat.ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"user","content":"hi"},
		{"role":"assistant","content":"PONG","reasoning_details":[
			{"type":"reasoning.summary","summary":"sum-1","format":"anthropic-claude-v1","index":0},
			{"type":"reasoning.summary","summary":"sum-2","index":0}]}]}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	th := w42ThinkingBlocks(req.Messages[1])
	if len(th) != 1 || th[0].Text != "sum-1sum-2" || th[0].Signature != "" {
		t.Fatalf("summary 未落思考文本: %+v", req.Messages[1].Blocks)
	}
	// 载体被消费（assistant 消息不进 Extra——渲染侧单一写者）。
	if _, ok := req.Messages[1].Extra["reasoning_details"]; ok {
		t.Fatalf("reasoning_details 应被消费: %+v", req.Messages[1].Extra)
	}

	// ② text + summary 同帧 → text 优先（summary 不混入）。
	req2, err := openai_chat.ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"assistant","content":"PONG","reasoning_details":[
			{"type":"reasoning.summary","summary":"SUMM","index":0},
			{"type":"reasoning.text","text":"TEXT","index":1}]}]}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if th := w42ThinkingBlocks(req2.Messages[0]); len(th) != 1 || th[0].Text != "TEXT" {
		t.Fatalf("text 未优先于 summary: %+v", req2.Messages[0].Blocks)
	}

	// ③ 响应面：summary → 思考块 → 出口走 reasoning_content（文本兼容通道）。
	resp, err := openai_chat.ParseResponse([]byte(`{"id":"c1","object":"chat.completion",
		"choices":[{"index":0,"message":{"role":"assistant","content":"PONG",
			"reasoning_details":[{"type":"reasoning.summary","summary":"sum-x","index":0}]},
			"finish_reason":"stop"}]}`))
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	out, err := openai_chat.RenderResponse(resp, openai_chat.RenderOptions{})
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	if !strings.Contains(string(out), `"reasoning_content":"sum-x"`) {
		t.Fatalf("summary 未回写 reasoning_content: %s", out)
	}

	// ④ 流式：summary 帧 → 思考文本增量。
	p := openai_chat.NewStreamParser()
	evs, err := p.Feed([]byte(`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,
		"delta":{"reasoning_details":[{"type":"reasoning.summary","summary":"sum-S","index":0}]}}]}`))
	if err != nil {
		t.Fatalf("Feed: %v", err)
	}
	var text string
	for _, ev := range evs {
		if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockThinking {
			text += ev.TextDelta
		}
	}
	if text != "sum-S" {
		t.Fatalf("流式 summary 文本 = %q（want sum-S）", text)
	}
}

// ---------------------------------------------------------------------------
// ① `reasoning` 别名读面
// ---------------------------------------------------------------------------

// TestW42ReasoningAliasReadFace `message.reasoning` / `delta.reasoning` =
// reasoning_content 的 OpenRouter 别名（reasoning-tokens.md §Preserving
// Reasoning Blocks：「reasoning_content … functions identically to reasoning」）。
func TestW42ReasoningAliasReadFace(t *testing.T) {
	// ① 非流式：reasoning 缺省回退（无 reasoning_content 时）→ 思考块。
	req, err := openai_chat.ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"assistant","content":"PONG","reasoning":"alias-think"}]}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if th := w42ThinkingBlocks(req.Messages[0]); len(th) != 1 || th[0].Text != "alias-think" {
		t.Fatalf("reasoning 别名未落思考文本: %+v", req.Messages[0].Blocks)
	}
	// 字段本身留 Extra 保真（与 reasoning_content 同口径——未消费）。
	if got := string(req.Messages[0].Extra["reasoning"]); got != `"alias-think"` {
		t.Fatalf("Extra[reasoning] = %s（want 原样保真）", got)
	}

	// ② reasoning_content 优先（两者同帧不重复）。
	req2, err := openai_chat.ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"assistant","content":"PONG","reasoning_content":"rc-wins","reasoning":"alias-loses"}]}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	th := w42ThinkingBlocks(req2.Messages[0])
	if len(th) != 1 || th[0].Text != "rc-wins" {
		t.Fatalf("reasoning_content 未优先: %+v", req2.Messages[0].Blocks)
	}

	// ③ 安全规则：user 消息的 reasoning 不进块（留 Extra）。
	req3, err := openai_chat.ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"user","content":"hi","reasoning":"INJECT"}]}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if th := w42ThinkingBlocks(req3.Messages[0]); len(th) != 0 {
		t.Fatalf("user 消息的 reasoning 不得成块: %+v", req3.Messages[0].Blocks)
	}

	// ④ 流式：delta.reasoning → 思考文本（reasoning_content 缺省时）。
	p := openai_chat.NewStreamParser()
	evs, err := p.Feed([]byte(`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"reasoning":"alias-delta"}}]}`))
	if err != nil {
		t.Fatalf("Feed: %v", err)
	}
	var text string
	for _, ev := range evs {
		if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockThinking {
			text += ev.TextDelta
		}
	}
	if text != "alias-delta" {
		t.Fatalf("流式 reasoning 别名文本 = %q", text)
	}
}

// ---------------------------------------------------------------------------
// ② 工具签名：读面（id 配对）
// ---------------------------------------------------------------------------

// TestW42ToolSignatureReadFace encrypted{id} 与同消息 tool_call 配对 → 该 tool_use
// 块签名；占位符原样接受；不可识别丢弃（红线）；无配对不作 tool 签名。
func TestW42ToolSignatureReadFace(t *testing.T) {
	w42CheckSamples(t)

	parse := func(t *testing.T, details, toolCalls string) ir.Message {
		t.Helper()
		req, err := openai_chat.ParseRequest([]byte(`{"model":"m","messages":[
			{"role":"assistant","content":"","tool_calls":` + toolCalls + `,"reasoning_details":` + details + `}]}`))
		if err != nil {
			t.Fatalf("ParseRequest: %v", err)
		}
		return req.Messages[0]
	}
	tc := `[{"id":"call_1","type":"function","function":{"name":"f","arguments":"{}"}}]`
	toolSigOf := func(t *testing.T, m ir.Message) string {
		t.Helper()
		for _, b := range m.Blocks {
			if b.Kind == ir.BlockToolUse {
				return b.Signature
			}
		}
		t.Fatalf("tool_use 块缺失: %+v", m.Blocks)
		return ""
	}

	// ① Gemini 形态 → 回填签名。
	m := parse(t, `[{"type":"reasoning.encrypted","data":"`+w42GeminiSig+`","id":"call_1","format":"google-gemini-v1","index":0}]`, tc)
	if got := toolSigOf(t, m); got != w42GeminiSig {
		t.Fatalf("tool 签名未回填: %q（want %q）", got, w42GeminiSig)
	}

	// ② 官方占位符 → 原样接受（magic value：Google「跳过校验」的显式表达）。
	m = parse(t, `[{"type":"reasoning.encrypted","data":"`+w42Placeholder+`","id":"call_1","format":"google-gemini-v1","index":0}]`, tc)
	if got := toolSigOf(t, m); got != w42Placeholder {
		t.Fatalf("占位符未原样接受: %q", got)
	}

	// ③ 不可识别载荷 → 丢弃（红线：绝不写进 wire）。
	m = parse(t, `[{"type":"reasoning.encrypted","data":"junk-data","id":"call_1","format":"google-gemini-v1","index":0}]`, tc)
	if got := toolSigOf(t, m); got != "" {
		t.Fatalf("不可识别载荷进了 IR: %q", got)
	}

	// ④ id 不配对（无该 tool_call）→ 不作 tool 签名。
	m = parse(t, `[{"type":"reasoning.encrypted","data":"`+w42GeminiSig+`","id":"call_x","format":"google-gemini-v1","index":0}]`, tc)
	if got := toolSigOf(t, m); got != "" {
		t.Fatalf("无配对的载荷进了 tool 签名: %q", got)
	}

	// ⑤ 回传轮 render：签名经载体重放，id = tool_calls[].id（逐字相同——消费端配对）。
	req, err := openai_chat.ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"assistant","content":"","tool_calls":` + tc + `,"reasoning_details":[
			{"type":"reasoning.encrypted","data":"` + w42GeminiSig + `","id":"call_1","format":"google-gemini-v1","index":0}]}]}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	out, err := openai_chat.RenderRequest(req, openai_chat.RenderOptions{})
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	var w struct {
		Messages []struct {
			ToolCalls []struct {
				ID string `json:"id"`
			} `json:"tool_calls"`
			ReasoningDetails []struct {
				Type   string `json:"type"`
				Data   string `json:"data"`
				ID     string `json:"id"`
				Format string `json:"format"`
			} `json:"reasoning_details"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	am := w.Messages[0]
	if len(am.ReasoningDetails) != 1 || am.ReasoningDetails[0].Type != "reasoning.encrypted" ||
		am.ReasoningDetails[0].Data != w42GeminiSig || am.ReasoningDetails[0].Format != "google-gemini-v1" {
		t.Fatalf("载体重放缺/变形: %s", out)
	}
	if am.ReasoningDetails[0].ID != am.ToolCalls[0].ID {
		t.Fatalf("载体 id 与 tool_calls[].id 不一致: 载体=%q tool=%q", am.ReasoningDetails[0].ID, am.ToolCalls[0].ID)
	}
}

// ---------------------------------------------------------------------------
// ② 工具签名：写面 + 全环（chat 请求 → gemini 上游）
// ---------------------------------------------------------------------------

// TestW42ToolSignatureWriteFace 写面（响应/请求）：Gemini 形态签名 → encrypted
// 条目（format google-gemini-v1、id = tool_call 线 id）；非 Gemini 形态 → 丢弃 +
// 计数（红线）。
func TestW42ToolSignatureWriteFace(t *testing.T) {
	w42CheckSamples(t)

	resp := &ir.Response{ID: "c1", Model: "m", Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
		{Kind: ir.BlockToolUse, Signature: w42GeminiSig, ToolCall: &ir.ToolCall{ID: "call_9", Name: "f", Arguments: `{}`}},
	}}}
	diag := &openai_chat.BatchDiagnostics{}
	out, err := openai_chat.RenderResponse(resp, openai_chat.RenderOptions{Diag: diag})
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	var w struct {
		Choices []struct {
			Message struct {
				ToolCalls []struct {
					ID string `json:"id"`
				} `json:"tool_calls"`
				ReasoningDetails []struct {
					Type   string `json:"type"`
					Data   string `json:"data"`
					ID     string `json:"id"`
					Format string `json:"format"`
					Index  *int   `json:"index"`
				} `json:"reasoning_details"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	msg := w.Choices[0].Message
	if len(msg.ReasoningDetails) != 1 {
		t.Fatalf("encrypted 条目 = %+v（want 1 条）: %s", msg.ReasoningDetails, out)
	}
	d := msg.ReasoningDetails[0]
	if d.Type != "reasoning.encrypted" || d.Data != w42GeminiSig || d.Format != "google-gemini-v1" || d.ID != msg.ToolCalls[0].ID {
		t.Fatalf("条目 = %+v（want data/id/format 配对）", d)
	}
	if d.Index == nil || *d.Index != 0 {
		t.Fatalf("index 缺失/非 0: %+v", d)
	}
	if diag.SignatureDropped != 0 {
		t.Fatalf("签名已落地却计丢弃: %+v", diag)
	}

	// 非 Gemini 形态（Claude 签名挂在 tool_use 块上）→ 丢弃 + 计数（红线）。
	resp2 := &ir.Response{ID: "c2", Model: "m", Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
		{Kind: ir.BlockToolUse, Signature: w42ClaudeSig, ToolCall: &ir.ToolCall{ID: "call_1", Name: "f", Arguments: `{}`}},
	}}}
	diag2 := &openai_chat.BatchDiagnostics{}
	out2, err := openai_chat.RenderResponse(resp2, openai_chat.RenderOptions{Diag: diag2})
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	if strings.Contains(string(out2), w42ClaudeSig) {
		t.Fatalf("非 Gemini 工具签名泄漏: %s", out2)
	}
	if diag2.SignatureDropped != 1 {
		t.Fatalf("未计签名丢弃: %+v", diag2)
	}
}

// TestW42ToolSignatureChatToGeminiLoop chat 请求（tool_calls + encrypted{id}）→
// gemini 上游请求：functionCall.thoughtSignature 逐字重放（② 的跨格式环）。
// 占位符同款（Gemini 3 族无签名 functionCall 的合法表达）。
func TestW42ToolSignatureChatToGeminiLoop(t *testing.T) {
	w42CheckSamples(t)

	for _, tc := range []struct{ name, data string }{
		{"gemini_sig", w42GeminiSig},
		{"placeholder", w42Placeholder},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := openai_chat.ParseRequest([]byte(`{"model":"gemini-3-pro-preview","messages":[
				{"role":"user","content":"hi"},
				{"role":"assistant","content":"","tool_calls":[{"id":"call_1","type":"function","function":{"name":"f","arguments":"{}"}}],
				 "reasoning_details":[{"type":"reasoning.encrypted","data":"` + tc.data + `","id":"call_1","format":"google-gemini-v1","index":0}]},
				{"role":"tool","tool_call_id":"call_1","content":"ok"}]}`))
			if err != nil {
				t.Fatalf("ParseRequest: %v", err)
			}
			out, err := gemini.RenderRequest(req)
			if err != nil {
				t.Fatalf("gemini RenderRequest: %v", err)
			}
			var w struct {
				Contents []struct {
					Parts []map[string]json.RawMessage `json:"parts"`
				} `json:"contents"`
			}
			if err := json.Unmarshal(out, &w); err != nil {
				t.Fatalf("unmarshal: %v\n%s", err, out)
			}
			// 助手轮 = contents[1]：functionCall part 带 thoughtSignature 逐字。
			found := false
			for _, p := range w.Contents[1].Parts {
				if p["functionCall"] != nil {
					found = true
					if got := string(p["thoughtSignature"]); got != `"`+tc.data+`"` {
						t.Fatalf("thoughtSignature = %s（want %q）\n%s", got, tc.data, out)
					}
				}
			}
			if !found {
				t.Fatalf("functionCall part 缺失: %s", out)
			}
		})
	}
}

// TestW42ToolSignatureGeminiToChatToGeminiLoop 全环（② 的端到端）：gemini 上游
// 响应（functionCall + thoughtSignature）→ chat 响应（encrypted{id}）→ 客户端
// 原样回传 → gemini 上游请求（thoughtSignature 逐字）。
func TestW42ToolSignatureGeminiToChatToGeminiLoop(t *testing.T) {
	w42CheckSamples(t)

	// ① gemini 响应 → chat 响应。
	gresp, err := gemini.ParseResponse([]byte(`{"candidates":[{"index":0,"content":{"role":"model","parts":[
		{"functionCall":{"name":"f","args":{}},"thoughtSignature":"` + w42GeminiSig + `"}]},"finishReason":"STOP"}]}`))
	if err != nil {
		t.Fatalf("gemini ParseResponse: %v", err)
	}
	chatResp, err := openai_chat.RenderResponse(&ir.Response{
		ID: "r1", Model: "gemini-3-pro-preview", Message: gresp.Message, StopReason: gresp.StopReason,
	}, openai_chat.RenderOptions{})
	if err != nil {
		t.Fatalf("chat RenderResponse: %v", err)
	}
	if !strings.Contains(string(chatResp), w42GeminiSig) {
		t.Fatalf("chat 响应未带工具签名载体: %s", chatResp)
	}

	// ② 客户端把助手消息原样带回（chat 请求）→ gemini 上游请求。
	var chatW struct {
		Choices []struct {
			Message json.RawMessage `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(chatResp, &chatW); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	reqBody := []byte(`{"model":"gemini-3-pro-preview","messages":[{"role":"user","content":"hi"},` + string(chatW.Choices[0].Message) + `,{"role":"user","content":"go on"}]}`)
	req, err := openai_chat.ParseRequest(reqBody)
	if err != nil {
		t.Fatalf("ParseRequest（回传轮）: %v", err)
	}
	gout, err := gemini.RenderRequest(req)
	if err != nil {
		t.Fatalf("gemini RenderRequest: %v", err)
	}
	if !strings.Contains(string(gout), `"thoughtSignature":"`+w42GeminiSig+`"`) {
		t.Fatalf("工具签名未逐字回到 gemini 上游: %s", gout)
	}
}

// ---------------------------------------------------------------------------
// ① encrypted ↔ redacted_thinking（无 tool 配对的 Claude 族载荷）
// ---------------------------------------------------------------------------

// TestW42RedactedCarrierReadWrite redacted 载荷读写：Claude 族 encrypted → redacted
// 块（claude 出口 data 逐字）；xai 族 → 丢弃（别族密文不塞 Claude 载体）；
// 写面 redacted_thinking → encrypted{format:anthropic-claude-v1}。
func TestW42RedactedCarrierReadWrite(t *testing.T) {
	// ① 读面：Claude 族载荷 → redacted 块。
	req, err := openai_chat.ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"assistant","content":"PONG","reasoning_details":[
			{"type":"reasoning.encrypted","data":"REDACTED-X","format":"anthropic-claude-v1","index":0}]}]}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	var red *ir.ContentBlock
	for i := range req.Messages[0].Blocks {
		if req.Messages[0].Blocks[i].Kind == ir.BlockRedactedThinking {
			red = &req.Messages[0].Blocks[i]
		}
	}
	if red == nil || red.Text != "REDACTED-X" {
		t.Fatalf("redacted 载荷未落块: %+v", req.Messages[0].Blocks)
	}

	// ② 别族（xai）载荷 → 丢弃（红线同族纪律）。
	req2, err := openai_chat.ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"assistant","content":"PONG","reasoning_details":[
			{"type":"reasoning.encrypted","data":"XAI-DATA","format":"xai-responses-v1","index":0}]}]}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	for _, b := range req2.Messages[0].Blocks {
		if b.Kind == ir.BlockRedactedThinking || strings.Contains(b.Text, "XAI-DATA") {
			t.Fatalf("别族载荷进了 IR: %+v", req2.Messages[0].Blocks)
		}
	}

	// ③ 写面：redacted_thinking → chat 响应 encrypted 条目。
	resp := &ir.Response{ID: "c1", Model: "m", Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
		{Kind: ir.BlockRedactedThinking, Text: "REDACTED-Y"},
		{Kind: ir.BlockText, Text: "ok"},
	}}}
	out, err := openai_chat.RenderResponse(resp, openai_chat.RenderOptions{})
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	if !strings.Contains(string(out), `"data":"REDACTED-Y"`) ||
		!strings.Contains(string(out), `"type":"reasoning.encrypted"`) ||
		!strings.Contains(string(out), `"format":"anthropic-claude-v1"`) {
		t.Fatalf("redacted 载荷未走 encrypted 载体: %s", out)
	}

	// ④ 往返：chat 请求（encrypted 载荷）→ claude 上游请求（redacted_thinking.data 逐字）。
	creq, err := openai_chat.ParseRequest([]byte(`{"model":"m","messages":[
		{"role":"assistant","content":"PONG","reasoning_details":[
			{"type":"reasoning.encrypted","data":"REDACTED-Z","format":"anthropic-claude-v1","index":0}]}]}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	cout, err := claude_messages.RenderRequest(creq)
	if err != nil {
		t.Fatalf("claude RenderRequest: %v", err)
	}
	if !strings.Contains(string(cout), `"data":"REDACTED-Z"`) || !strings.Contains(string(cout), `"type":"redacted_thinking"`) {
		t.Fatalf("redacted 载荷未逐字回到 claude 上游: %s", cout)
	}
}

// ---------------------------------------------------------------------------
// ② 工具签名：流式双面
// ---------------------------------------------------------------------------

// TestW42StreamToolSignatureWriteFace 流式写面：tool 签名增量缓冲到块尾一条
// encrypted{id} 条目（两段拼接；id = tool_calls[].id 线值）；不可识别 →
// 不产条目 + SignatureDropped。
func TestW42StreamToolSignatureWriteFace(t *testing.T) {
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "c1", Model: "m", Created: 1},
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockToolUse, ToolCallID: "call_7", ToolName: "f"},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockToolUse, ArgsDelta: `{}`},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockToolUse, SignatureDelta: w42GeminiSig[:6]},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockToolUse, SignatureDelta: w42GeminiSig[6:]},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockToolUse},
		{Kind: ir.EventMessageDelta, StopReason: ir.StopToolUse},
		{Kind: ir.EventDone, Reason: ir.StopToolUse},
	}
	w := openai_chat.NewStreamWriter(openai_chat.StreamRenderOptions{})
	var out strings.Builder
	for _, ev := range events {
		b, err := w.Write(ev)
		if err != nil {
			t.Fatalf("Write(%s): %v", ev.Kind, err)
		}
		out.Write(b)
	}
	if _, err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	s := out.String()
	want := `"reasoning_details":[{"type":"reasoning.encrypted","data":"` + w42GeminiSig + `","id":"call_7","format":"google-gemini-v1","index":0}]`
	if !strings.Contains(s, want) {
		t.Fatalf("块尾工具签名条目缺/变形（want %s）:\n%s", want, s)
	}
	if n := strings.Count(s, w42GeminiSig); n != 1 {
		t.Fatalf("签名出现 %d 次（want 1——两段须拼接为一条）", n)
	}
	if d := w.Diagnostics(); d.SignatureDropped != 0 {
		t.Fatalf("diagnostics = %+v", d)
	}

	// 不可识别签名：不产条目 + SignatureDropped。
	w2 := openai_chat.NewStreamWriter(openai_chat.StreamRenderOptions{})
	var out2 strings.Builder
	for _, ev := range []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "c2", Model: "m", Created: 1},
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockToolUse, ToolCallID: "call_1", ToolName: "f"},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockToolUse, ArgsDelta: `{}`},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockToolUse, SignatureDelta: "sha256:abc"},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockToolUse},
		{Kind: ir.EventDone, Reason: ir.StopToolUse},
	} {
		b, err := w2.Write(ev)
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
		out2.Write(b)
	}
	if strings.Contains(out2.String(), "sha256") || strings.Contains(out2.String(), "reasoning.encrypted") {
		t.Fatalf("不可识别工具签名泄漏: %s", out2.String())
	}
	if d := w2.Diagnostics(); d.SignatureDropped != 1 {
		t.Fatalf("diagnostics = %+v（want SignatureDropped=1）", d)
	}
}

// TestW42StreamToolSignatureReadFace 流式读面：encrypted{id} 先于/后于 tool_call
// 到达都配对（块尾一条 SignatureDelta，先于块 stop）；不可识别不进 IR。
func TestW42StreamToolSignatureReadFace(t *testing.T) {
	w42CheckSamples(t)

	run := func(t *testing.T, frames []string) []ir.StreamEvent {
		t.Helper()
		p := openai_chat.NewStreamParser()
		var evs []ir.StreamEvent
		for _, f := range frames {
			got, err := p.Feed([]byte(f))
			if err != nil {
				t.Fatalf("Feed(%s): %v", f, err)
			}
			evs = append(evs, got...)
		}
		return evs
	}
	chunk := func(delta string) string {
		return `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":` + delta + `}]}`
	}
	finish := `{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`
	toolCall := `{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"f","arguments":"{}"}}]}`
	encSig := func(data string) string {
		return `{"reasoning_details":[{"type":"reasoning.encrypted","data":"` + data + `","id":"call_1","format":"google-gemini-v1","index":0}]}`
	}
	assertSig := func(t *testing.T, evs []ir.StreamEvent, want string) {
		t.Helper()
		var got string
		sigBeforeStop := false
		for _, ev := range evs {
			if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockToolUse && ev.SignatureDelta != "" {
				got += ev.SignatureDelta
				sigBeforeStop = true
			}
			if ev.Kind == ir.EventBlockStop && ev.BlockKind == ir.BlockToolUse {
				if !sigBeforeStop {
					t.Fatalf("签名 delta 未先于块 stop: %+v", evs)
				}
			}
		}
		if got != want {
			t.Fatalf("tool 签名 = %q（want %q）", got, want)
		}
	}

	// ① encrypted 先于 tool_call（roo-code 流序）→ 块尾配对。
	evs := run(t, []string{chunk(encSig(w42GeminiSig)), chunk(toolCall), finish, `[DONE]`})
	assertSig(t, evs, w42GeminiSig)

	// ② tool_call 先于 encrypted → 同款配对。
	evs = run(t, []string{chunk(toolCall), chunk(encSig(w42GeminiSig)), finish, `[DONE]`})
	assertSig(t, evs, w42GeminiSig)

	// ③ 不可识别载荷 → 不进 IR。
	evs = run(t, []string{chunk(toolCall), chunk(encSig("junk-data")), finish, `[DONE]`})
	for _, ev := range evs {
		if ev.SignatureDelta != "" {
			t.Fatalf("不可识别载荷进了 IR: %+v", ev)
		}
	}
}

// TestW42StreamRedactedReadFace 流式读面：无 tool 配对的 Claude 族 encrypted →
// redacted 块（同 index 的载荷拼接；正文开始前收尾——块序纪律）。
func TestW42StreamRedactedReadFace(t *testing.T) {
	p := openai_chat.NewStreamParser()
	var evs []ir.StreamEvent
	feed := func(s string) {
		t.Helper()
		got, err := p.Feed([]byte(s))
		if err != nil {
			t.Fatalf("Feed: %v", err)
		}
		evs = append(evs, got...)
	}
	feed(`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"reasoning_details":[{"type":"reasoning.encrypted","data":"ENC-A","format":"anthropic-claude-v1","index":0}]}}]}`)
	feed(`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"reasoning_details":[{"type":"reasoning.encrypted","data":"ENC-B","format":"anthropic-claude-v1","index":0}]}}]}`)
	feed(`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hi"}}]}`)
	feed(`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)
	feed(`[DONE]`)

	var payload string
	var redStart, redStop, textStart int
	for i, ev := range evs {
		switch {
		case ev.Kind == ir.EventBlockStart && ev.BlockKind == ir.BlockRedactedThinking:
			redStart = i + 1
		case ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockRedactedThinking:
			payload += ev.SignatureDelta
		case ev.Kind == ir.EventBlockStop && ev.BlockKind == ir.BlockRedactedThinking:
			redStop = i + 1
		case ev.Kind == ir.EventBlockStart && ev.BlockKind == ir.BlockText:
			textStart = i + 1
		}
	}
	if redStart == 0 || redStop == 0 || textStart == 0 {
		t.Fatalf("redacted/正文块序缺: %+v", evs)
	}
	if payload != "ENC-AENC-B" {
		t.Fatalf("载荷拼接 = %q（want ENC-AENC-B）", payload)
	}
	if !(redStart < redStop && redStop < textStart) {
		t.Fatalf("块序错（redacted stop 须先于正文 start）: red=%d/%d text=%d", redStart, redStop, textStart)
	}
}
