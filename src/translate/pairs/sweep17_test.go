package pairs

import (
	"testing"

	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// L19 sweep17：响应面 hygiene 三缺口收口 + 枢纽两步完整回归。
//
// 缺口来源：`research/topics/ir-spec/README.md` §缺口 18 + 「不变量与 W5-8 hygiene
// 的对齐说明」①②③：
//
//	① claude 出口的消息级 Extra 无过滤（W5-11 已落 openaiOriginMessageExtras）；
//	② claude 出口的工具定义级 Extra 无过滤（W5-11 已落 geminiOriginToolExtras）；
//	③ gemini 响应解析不产 Extra、响应面过滤表单向建表（本单收口：gemini 解析产
//	   Extra + gemini 出口过滤后回写 + 三出口响应面表逐来源补齐）。
//
// 本文件另含枢纽两步（8 组合 × 请求/响应面）表驱动回归：防泄漏 + 未知字段保真
// （出口政策内的）+ 中间腿无损（第二步不丢字节）。
// ---------------------------------------------------------------------------

// TestSweep17ClaudeResponseForeignKeys 响应面过滤表逐来源补齐（缺口 ③ 出口侧）：
// 三个来源的响应独有键不得出现在 claude/openai 响应体。interactions 的 `errors`
// 是**反证点**——修复前实测经 Response.Extra 泄漏进两出口（见报告「反证」节），
// 本测在修复前红、修复后绿；gemini 的 `modelStatus` 为新增表行（gemini 解析产
// Extra 后才可能携带——回归锁）。
func TestSweep17ClaudeResponseForeignKeys(t *testing.T) {
	reg := NewRegistry()

	interResp := `{"id":"i1","object":"interaction","model":"im","created":"2026-01-01T00:00:00Z","status":"completed",` +
		`"steps":[{"type":"model_output","content":[{"type":"text","text":"hi"}]}],` +
		`"service_tier":"standard","errors":[{"code":"x"}],` +
		`"usage":{"total_input_tokens":1,"total_output_tokens":1,"total_tokens":2,"total_cached_tokens":0,"total_thought_tokens":0,"total_tool_use_tokens":0}}`
	geminiResp := `{"responseId":"r1","modelVersion":"gm","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"hi"}]},"finishReason":"STOP"}],` +
		`"modelStatus":{"x":1},"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2,"totalTokenCount":5}}`

	cases := []struct {
		name    string
		from    registry.Format
		to      registry.Format
		in      string
		forbid  []string
		keepKey string
	}{
		{"interactions_to_openai", registry.FormatInteractions, fOpenAI, interResp,
			[]string{"errors", "container", "stop_details", "diagnostics", "modelStatus"}, "choices"},
		{"interactions_to_claude_via_hub", registry.FormatInteractions, fClaude, interResp,
			[]string{"errors", "service_tier", "container", "stop_details", "diagnostics", "modelStatus"}, "content"},
		{"gemini_to_openai", fGemini, fOpenAI, geminiResp,
			[]string{"modelStatus", "promptFeedback", "safetyRatings"}, "choices"},
		{"gemini_to_claude_via_hub", fGemini, fClaude, geminiResp,
			[]string{"modelStatus", "promptFeedback", "safetyRatings"}, "content"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := Convert(reg, tc.from, tc.to, FaceResponse, []byte(tc.in), Options{})
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}
			for _, k := range tc.forbid {
				if containsJSONKey(t, out, k) {
					t.Fatalf("来源独有响应键 %q 泄漏进 %s 响应体: %s", k, tc.to, out)
				}
			}
			if !containsJSONKey(t, out, tc.keepKey) {
				t.Fatalf("跨格式键 %q 丢失: %s", tc.keepKey, out)
			}
		})
	}
}

// TestSweep17ClaudeMessageToolExtrasLock 缺口 ①②（W5-11 已落、本单反证锁死）：
// claude 出口的消息级（openaiOriginMessageExtras）与工具定义级
// （geminiOriginToolExtras）过滤不得回退——回退即本测红（foreign 键出现在 claude
// 请求体），同时断言良性未知键保留（防过滤过度）。
func TestSweep17ClaudeMessageToolExtrasLock(t *testing.T) {
	reg := NewRegistry()

	// 消息级：function_call / audio / reasoning_content / refusal(null) 都是
	// openai 消息独有（claude 无对应字段）；x_keep 是良性未知键（须保留）。
	openaiReq := `{"model":"m","messages":[{"role":"assistant","content":"hi",` +
		`"function_call":{"name":"f","arguments":"{}"},"audio":{"id":"a"},"reasoning_content":"r",` +
		`"refusal":null,"x_keep":{"a":1}}]}`
	out, err := Convert(reg, fOpenAI, fClaude, FaceRequest, []byte(openaiReq), Options{})
	if err != nil {
		t.Fatalf("msg-level convert: %v", err)
	}
	for _, k := range []string{"function_call", "audio", "reasoning_content", "refusal"} {
		if containsJSONKey(t, out, k) {
			t.Fatalf("openai 消息级独有键 %q 泄漏进 claude 请求体: %s", k, out)
		}
	}
	if !containsJSONKey(t, out, "x_keep") {
		t.Fatalf("良性消息级未知键丢失（过滤过度）: %s", out)
	}

	// 工具定义级：gemini decl 的 response/responseJsonSchema（官方字段面）须剔除；
	// x_tool_note 良性未知键保留（claude 与 openai 两出口都验）。
	geminiReq := `{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"tools":[{"functionDeclarations":[{` +
		`"name":"f","parameters":{"type":"object"},"response":{"type":"object"},` +
		`"responseJsonSchema":{"type":"object"},"x_tool_note":"keep"}]}]}`
	for _, to := range []registry.Format{fClaude, fOpenAI} {
		out, err := Convert(reg, fGemini, to, FaceRequest, []byte(geminiReq), Options{})
		if err != nil {
			t.Fatalf("tool-level convert to %s: %v", to, err)
		}
		for _, k := range []string{"response", "responseJsonSchema"} {
			if containsJSONKey(t, out, k) {
				t.Fatalf("gemini 工具级独有键 %q 泄漏进 %s 请求体: %s", k, to, out)
			}
		}
		if !containsJSONKey(t, out, "x_tool_note") {
			t.Fatalf("良性工具级未知键丢失（过滤过度）→ %s: %s", to, out)
		}
	}
}

// TestSweep17GeminiResponseExtrasFidelity 缺口 ③ 解析侧（反证）：gemini 响应顶层
// 未建模键（modelStatus 官方字段 / 厂商扩展 x_vendor_meta / vendor_ext）必须经
// Response.Extra 保真旁路到达 openai 与 claude 出口——修复前解析不产 Extra，这些
// 键被静默丢弃（本测修复前红）；来源独有键在出口剔除、candidate 级字段随容器
// 丢弃（两条不变量同时成立：未知字段不炸 + 保真）。
func TestSweep17GeminiResponseExtrasFidelity(t *testing.T) {
	reg := NewRegistry()

	in := `{"responseId":"r1","modelVersion":"gm","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"hi"}]},` +
		`"finishReason":"STOP","safetyRatings":[{"category":"C","probability":"NEGLIGIBLE"}]}],` +
		`"promptFeedback":{"blockReason":"SAFETY"},"modelStatus":{"x":1},` +
		`"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2,"totalTokenCount":5},` +
		`"x_vendor_meta":{"a":1},"vendor_ext":[1,2]}`

	// 直转（openai）与枢纽两步（claude）：通用未知键两腿都必须保留。
	mid, err := Convert(reg, fGemini, fOpenAI, FaceResponse, []byte(in), Options{})
	if err != nil {
		t.Fatalf("mid convert: %v", err)
	}
	for _, k := range []string{"x_vendor_meta", "vendor_ext"} {
		if !containsJSONKey(t, mid, k) {
			t.Fatalf("未知顶层键 %q 在中间腿（gemini→openai）丢失: %s", k, mid)
		}
	}
	for _, k := range []string{"modelStatus", "promptFeedback", "safetyRatings"} {
		if containsJSONKey(t, mid, k) {
			t.Fatalf("来源独有/容器内键 %q 泄漏进 openai 响应体: %s", k, mid)
		}
	}
	if !containsJSONKey(t, mid, "choices") || !containsJSONKey(t, mid, "usage") {
		t.Fatalf("建模字段丢失: %s", mid)
	}

	out, err := Convert(reg, fGemini, fClaude, FaceResponse, []byte(in), Options{})
	if err != nil {
		t.Fatalf("hub convert: %v", err)
	}
	for _, k := range []string{"x_vendor_meta", "vendor_ext"} {
		if !containsJSONKey(t, out, k) {
			t.Fatalf("未知顶层键 %q 经枢纽两步丢失: %s", k, out)
		}
	}
	if !containsJSONKey(t, out, "content") || !containsJSONKey(t, out, "stop_reason") {
		t.Fatalf("建模字段丢失: %s", out)
	}
}

// TestSweep17GeminiResponseRenderExtras 缺口 ③ 出口侧（反证）：gemini 出口此前是
// 纯白名单（resp.Extra 不整体回写），openai/claude 来源的**通用未知**响应键在
// gemini 腿静默丢失——修复后过滤后回写（来源独有键仍剔除，反证：本测修复前红）。
func TestSweep17GeminiResponseRenderExtras(t *testing.T) {
	reg := NewRegistry()

	openaiResp := `{"id":"c1","object":"chat.completion","created":1,"model":"m",` +
		`"system_fingerprint":"fp_x","service_tier":"default",` +
		`"choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],` +
		`"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2},"x_vendor_meta":{"a":1}}`
	out, err := Convert(reg, fOpenAI, fGemini, FaceResponse, []byte(openaiResp), Options{})
	if err != nil {
		t.Fatalf("openai→gemini: %v", err)
	}
	if !containsJSONKey(t, out, "x_vendor_meta") {
		t.Fatalf("通用未知响应键在 gemini 出口丢失: %s", out)
	}
	for _, k := range []string{"system_fingerprint", "service_tier"} {
		if containsJSONKey(t, out, k) {
			t.Fatalf("openai 响应独有键 %q 泄漏进 gemini 响应体: %s", k, out)
		}
	}
	if !containsJSONKey(t, out, "candidates") || !containsJSONKey(t, out, "usageMetadata") {
		t.Fatalf("建模字段丢失: %s", out)
	}

	claudeResp := `{"id":"m1","type":"message","role":"assistant","model":"cm",` +
		`"content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn",` +
		`"container":{"id":"cont_1"},"stop_details":{"type":"end_turn"},"diagnostics":{"cache_miss_reason":{"type":"x"}},` +
		`"x_vendor_meta":{"a":1},"usage":{"input_tokens":1,"output_tokens":1}}`
	out, err = Convert(reg, fClaude, fGemini, FaceResponse, []byte(claudeResp), Options{})
	if err != nil {
		t.Fatalf("claude→gemini hub: %v", err)
	}
	if !containsJSONKey(t, out, "x_vendor_meta") {
		t.Fatalf("通用未知响应键经枢纽两步在 gemini 出口丢失: %s", out)
	}
	for _, k := range []string{"container", "stop_details", "diagnostics"} {
		if containsJSONKey(t, out, k) {
			t.Fatalf("claude 响应独有键 %q 泄漏进 gemini 响应体: %s", k, out)
		}
	}
}

// TestSweep17HubTwoStepMatrix 枢纽两步完整回归：8 组枢纽组合 × 请求/响应面。
// 每行断言四件：① 计划确为 2 步经枢纽（Quality ≤ fair）；② 出口体无来源独有键；
// ③ 出口体保留预期键（跨格式映射键 / 出口政策内的未知键保真）；④ 中间腿
// （第一步 = X→openai）干净且无损——第二步不丢字节的机械保证。
func TestSweep17HubTwoStepMatrix(t *testing.T) {
	reg := NewRegistry()

	reqs := map[registry.Format]string{
		fGemini: `{"contents":[{"role":"user","parts":[{"text":"hi"}]}],` +
			`"generationConfig":{"maxOutputTokens":64},` +
			`"safetySettings":[{"category":"HARM_CATEGORY_HATE_SPEECH","threshold":"BLOCK_ONLY_HIGH"}]}`,
		fClaude: `{"model":"m","max_tokens":64,"messages":[{"role":"user","content":"hi"}],` +
			`"betas":["x"],"x_vendor_meta":{"a":1}}`,
		fResp: `{"model":"m","input":"hi","max_output_tokens":64,"store":false,"x_vendor_meta":{"a":1}}`,
		registry.FormatInteractions: `{"model":"m","input":[{"type":"user_input","content":[{"type":"text","text":"hi"}]}],` +
			`"generation_config":{"max_output_tokens":64},"response_mime_type":"application/json","x_vendor_meta":{"a":1}}`,
	}
	resps := map[registry.Format]string{
		fGemini: `{"responseId":"r1","modelVersion":"gm","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"hi"}]},"finishReason":"STOP"}],` +
			`"modelStatus":{"x":1},"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2,"totalTokenCount":5},"x_vendor_meta":{"a":1}}`,
		fClaude: `{"id":"m1","type":"message","role":"assistant","model":"cm","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn",` +
			`"container":{"id":"cont_1"},"diagnostics":{"cache_miss_reason":{"type":"x"}},"x_vendor_meta":{"a":1},` +
			`"usage":{"input_tokens":1,"output_tokens":1}}`,
		fResp: `{"id":"rsp_1","object":"response","created_at":1,"status":"completed","model":"rm",` +
			`"output":[{"type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"hi"}]}],"usage":null}`,
		registry.FormatInteractions: `{"id":"i1","object":"interaction","model":"im","created":"2026-01-01T00:00:00Z","status":"completed",` +
			`"steps":[{"type":"model_output","content":[{"type":"text","text":"hi"}]}],"service_tier":"standard","errors":[{"code":"x"}],` +
			`"usage":{"total_input_tokens":1,"total_output_tokens":1,"total_tokens":2,"total_cached_tokens":0,"total_thought_tokens":0,"total_tool_use_tokens":0}}`,
	}

	type row struct {
		name string
		from registry.Format
		to   registry.Format
		face Face
		// forbidMid/forbidFinal：中间腿（第一步 X→openai）与出口体不得出现的
		// 来源独有键（split 是必要的：service_tier 对 openai 是自有键、对 claude
		// 是外来键——中间腿保留、出口腿剔除）。
		forbidMid   []string
		forbidFinal []string
		// expectMid：中间腿必须保留的键——「第二步不丢字节」的机械保证
		// （通用未知键在解析侧产 Extra 的来源上必须无损过中间腿）。
		expectMid []string
		// expectFinal：出口体必须保留的键（跨格式映射键 + 出口政策内的保真键）。
		expectFinal []string
	}
	rows := []row{
		// 请求面：claude/responses/interactions 解析产顶层 Extra（含通用未知键），
		// gemini 请求解析是映射式（顶层未知键不入 Extra——保真边界见报告）。
		{"req_gemini_to_claude", fGemini, fClaude, FaceRequest,
			[]string{"safetySettings"}, []string{"safetySettings"}, nil, []string{"max_tokens"}},
		{"req_claude_to_gemini", fClaude, fGemini, FaceRequest,
			[]string{"betas"}, []string{"betas", "x_vendor_meta"}, []string{"x_vendor_meta"},
			[]string{"generationConfig", "maxOutputTokens"}},
		{"req_gemini_to_responses", fGemini, fResp, FaceRequest,
			[]string{"safetySettings"}, []string{"safetySettings"}, nil, []string{"max_output_tokens"}},
		{"req_responses_to_gemini", fResp, fGemini, FaceRequest,
			nil, nil, []string{"x_vendor_meta"}, []string{"generationConfig", "maxOutputTokens"}},
		{"req_claude_to_interactions", fClaude, registry.FormatInteractions, FaceRequest,
			[]string{"betas"}, []string{"betas", "x_vendor_meta"}, []string{"x_vendor_meta"},
			[]string{"generation_config", "max_output_tokens"}},
		{"req_interactions_to_claude", registry.FormatInteractions, fClaude, FaceRequest,
			[]string{"responseMimeType"}, []string{"responseMimeType", "x_vendor_meta"}, []string{"x_vendor_meta"},
			[]string{"max_tokens"}},
		{"req_responses_to_interactions", fResp, registry.FormatInteractions, FaceRequest,
			nil, []string{"x_vendor_meta"}, []string{"x_vendor_meta"},
			[]string{"generation_config", "max_output_tokens"}},
		{"req_interactions_to_responses", registry.FormatInteractions, fResp, FaceRequest,
			[]string{"responseMimeType"}, []string{"responseMimeType", "x_vendor_meta"}, []string{"x_vendor_meta"},
			[]string{"input", "max_output_tokens"}},
		// 响应面：gemini/claude 解析产顶层 Extra；出口是否保真由出口政策决定
		// （claude/gemini 出口过滤后回写；responses/interactions 出口白名单读取）。
		{"resp_gemini_to_claude", fGemini, fClaude, FaceResponse,
			[]string{"modelStatus", "promptFeedback", "safetyRatings"}, []string{"modelStatus", "promptFeedback", "safetyRatings"},
			[]string{"x_vendor_meta"}, []string{"x_vendor_meta", "content"}},
		{"resp_claude_to_gemini", fClaude, fGemini, FaceResponse,
			[]string{"container", "stop_details", "diagnostics"}, []string{"container", "stop_details", "diagnostics"},
			[]string{"x_vendor_meta"}, []string{"x_vendor_meta", "candidates"}},
		{"resp_gemini_to_responses", fGemini, fResp, FaceResponse,
			[]string{"modelStatus", "promptFeedback"}, []string{"modelStatus", "promptFeedback"},
			[]string{"x_vendor_meta"}, []string{"output"}},
		{"resp_responses_to_gemini", fResp, fGemini, FaceResponse,
			nil, nil, nil, []string{"candidates"}},
		{"resp_claude_to_interactions", fClaude, registry.FormatInteractions, FaceResponse,
			[]string{"container", "stop_details", "diagnostics"}, []string{"container", "stop_details", "diagnostics"},
			[]string{"x_vendor_meta"}, []string{"steps"}},
		{"resp_interactions_to_claude", registry.FormatInteractions, fClaude, FaceResponse,
			[]string{"errors"}, []string{"errors", "service_tier"}, nil, []string{"content"}},
		{"resp_responses_to_interactions", fResp, registry.FormatInteractions, FaceResponse,
			nil, nil, nil, []string{"steps"}},
		{"resp_interactions_to_responses", registry.FormatInteractions, fResp, FaceResponse,
			[]string{"errors"}, []string{"errors", "service_tier"}, nil, []string{"output"}},
	}

	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			plan, err := reg.Lookup(r.from, r.to)
			if err != nil {
				t.Fatalf("Lookup: %v", err)
			}
			if len(plan.Steps) != 2 || !plan.ViaHub ||
				plan.Steps[0] != (registry.Step{From: r.from, To: fOpenAI}) ||
				plan.Steps[1] != (registry.Step{From: fOpenAI, To: r.to}) {
				t.Fatalf("plan = %+v（期望 2 步经 openai 枢纽）", plan)
			}
			if plan.Quality == registry.QualityGood {
				t.Fatalf("枢纽计划质量 = %q，期望 ≤ fair", plan.Quality)
			}

			payload := reqs[r.from]
			if r.face == FaceResponse {
				payload = resps[r.from]
			}

			// 中间腿 = 第一步（X→openai，注册表直转对）。
			mid, err := Convert(reg, r.from, fOpenAI, r.face, []byte(payload), Options{})
			if err != nil {
				t.Fatalf("middle leg: %v", err)
			}
			for _, k := range r.forbidMid {
				if containsJSONKey(t, mid, k) {
					t.Fatalf("来源独有键 %q 泄漏进中间腿: %s", k, mid)
				}
			}
			for _, k := range r.expectMid {
				if !containsJSONKey(t, mid, k) {
					t.Fatalf("中间腿丢失 %q（第二步无字节可保真）: %s", k, mid)
				}
			}

			// 出口腿：两步完整转换。
			out, err := Convert(reg, r.from, r.to, r.face, []byte(payload), Options{})
			if err != nil {
				t.Fatalf("hub convert: %v", err)
			}
			for _, k := range r.forbidFinal {
				if containsJSONKey(t, out, k) {
					t.Fatalf("来源独有键 %q 泄漏进 %s 出口体: %s", k, r.to, out)
				}
			}
			for _, k := range r.expectFinal {
				if !containsJSONKey(t, out, k) {
					t.Fatalf("出口体丢失 %q: %s", k, out)
				}
			}
		})
	}
}

// TestSweep17UnknownFieldsDoNotExplode 「未知字段不炸」不变量：五来源各自在顶层 /
// 消息级 / 工具定义级塞未知键，转换到其余四格式（两组合 × 两面的代表集）都必须
// 成功——未知键只走「保真旁路或确定性丢弃」，绝不触发错误。
func TestSweep17UnknownFieldsDoNotExplode(t *testing.T) {
	reg := NewRegistry()

	payloads := map[registry.Format]string{
		fOpenAI: `{"model":"m","x_top":{"a":1},"messages":[{"role":"user","content":"hi","x_msg":"v"}],` +
			`"tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object"},"x_tool":1}}]}`,
		fClaude: `{"model":"m","max_tokens":16,"x_top":{"a":1},"messages":[{"role":"user","content":"hi","x_msg":"v"}],` +
			`"tools":[{"name":"f","input_schema":{"type":"object"},"x_tool":1}]}`,
		fGemini: `{"model":"m","contents":[{"role":"user","parts":[{"text":"hi"}]}],"x_top":{"a":1},` +
			`"tools":[{"functionDeclarations":[{"name":"f","parameters":{"type":"object"},"x_tool":1}]}]}`,
		fResp: `{"model":"m","input":"hi","x_top":{"a":1},` +
			`"tools":[{"type":"function","name":"f","parameters":{"type":"object"},"x_tool":1}]}`,
		registry.FormatInteractions: `{"model":"m","input":[{"type":"user_input","content":[{"type":"text","text":"hi"}]}],"x_top":{"a":1},` +
			`"tools":[{"type":"function","name":"f","parameters":{"type":"object"},"x_tool":1}]}`,
	}
	all := []registry.Format{fOpenAI, fClaude, fGemini, fResp, registry.FormatInteractions}
	for from, payload := range payloads {
		for _, to := range all {
			if to == from {
				continue
			}
			if _, err := Convert(reg, from, to, FaceRequest, []byte(payload), Options{}); err != nil {
				t.Fatalf("%s→%s：未知键不得触发错误: %v", from, to, err)
			}
		}
	}
}
