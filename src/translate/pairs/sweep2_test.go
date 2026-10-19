package pairs

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/convert/gemini"
	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// W5-16 ①：路径态流开关（Options.Stream）
// ---------------------------------------------------------------------------

// TestConvertStreamOptionPathState 路径态流开关：真 gemini 客户端的流开关在路径
// （`:streamGenerateContent?alt=sse`），请求体无该字段——调用方声明 Options.Stream
// 后流位必须落进 IR 并出现在下游请求体；未声明不发明；请求体自己的流位不被擦除。
func TestConvertStreamOptionPathState(t *testing.T) {
	reg := NewRegistry()

	// ① gemini 路径态流式（请求体无 stream 位）→ openai 出口请求体带 stream:true。
	gemReq := []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`)
	out, err := Convert(reg, fGemini, fOpenAI, FaceRequest, gemReq, Options{Stream: true})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if !strings.Contains(string(out), `"stream":true`) {
		t.Fatalf("路径态流位丢失（want stream:true）: %s", out)
	}

	// ② 未声明路径态 → 不发明流位。
	out, err = Convert(reg, fGemini, fOpenAI, FaceRequest, gemReq, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if strings.Contains(string(out), `"stream"`) {
		t.Fatalf("未声明路径态却发明了流位: %s", out)
	}

	// ③ 请求体自己声明的流位不被擦除（Options.Stream=false 也不清除）。
	openReq := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}`)
	out, err = Convert(reg, fOpenAI, fClaude, FaceRequest, openReq, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if !strings.Contains(string(out), `"stream":true`) {
		t.Fatalf("请求体声明的流位被擦除: %s", out)
	}

	// ④ 路径态流位必须到达 gemini 出口的「路径态语义」处：stream+n>1 的上游互斥
	//    守卫（GW 陷阱 7）按它拦截——证明流位真的落在 IR 上而不是只对某个出口生效。
	n2 := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"n":2}`)
	if _, err := Convert(reg, fOpenAI, fGemini, FaceRequest, n2, Options{Stream: true}); !errors.Is(err, gemini.ErrStreamCandidateCount) {
		t.Fatalf("err = %v, want ErrStreamCandidateCount（流位未到达 gemini 出口）", err)
	}
	if _, err := Convert(reg, fOpenAI, fGemini, FaceRequest, n2, Options{}); err != nil {
		t.Fatalf("非流式 n=2 不该拦截: %v", err)
	}
}

// ---------------------------------------------------------------------------
// W5-16 ③：非流式（批量）thinking 面（与流式同一决策表）
// ---------------------------------------------------------------------------

// TestBatchThinkingFaceTable 批量面思考块落法逐格断言（P53 缺口 1：此前
// ErrUnsupportedBlock 断链）：
//   - 响应面 assistant：思考文本 → message.reasoning_content（拼接全部思考块；签名不外泄）；
//   - 请求面 / 非 assistant：整块丢弃，绝不报错；
//   - 枢纽中间腿（W42 ③）：思考文本经 reasoning_details 载体保真（出口腿按
//     provider 门落地——claude→gemini 落 thought part）；redacted 载荷中间腿仍丢弃；
//   - redacted_thinking（W42 ①，直转面）：载荷 → reasoning.encrypted 载体。
func TestBatchThinkingFaceTable(t *testing.T) {
	reg := NewRegistry()

	t.Run("response_assistant_uses_reasoning_content", func(t *testing.T) {
		resp := []byte(`{
			"id":"msg_1","type":"message","role":"assistant","model":"m",
			"content":[{"type":"thinking","thinking":"ponder","signature":"SECRETSIG"},
				{"type":"thinking","thinking":"ing"},
				{"type":"text","text":"answer"}],
			"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":2}
		}`)
		out, err := Convert(reg, fClaude, fOpenAI, FaceResponse, resp, Options{})
		if err != nil {
			t.Fatalf("Convert: %v", err)
		}
		var w struct {
			Choices []struct {
				Message struct {
					Content          string `json:"content"`
					ReasoningContent string `json:"reasoning_content"`
				} `json:"message"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(out, &w); err != nil {
			t.Fatalf("unmarshal: %v\n%s", err, out)
		}
		if got := w.Choices[0].Message.ReasoningContent; got != "ponder\ning" {
			t.Fatalf("reasoning_content = %q（want 拼接全部思考块）\n%s", got, out)
		}
		if w.Choices[0].Message.Content != "answer" {
			t.Fatalf("content = %q", w.Choices[0].Message.Content)
		}
		if strings.Contains(string(out), "SECRETSIG") {
			t.Fatalf("签名外泄到 OpenAI 线: %s", out)
		}
	})

	t.Run("response_gemini_thought_parts_concatenated", func(t *testing.T) {
		resp := []byte(`{
			"candidates":[{"content":{"role":"model","parts":[
				{"text":"think-a","thought":true,"thoughtSignature":"SIG1"},
				{"text":"think-b","thought":true},
				{"text":"visible"}]},"finishReason":"STOP","index":0}],
			"usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":2,"totalTokenCount":6},
			"responseId":"resp_1","modelVersion":"gemini-2.5-flash"
		}`)
		out, err := Convert(reg, fGemini, fOpenAI, FaceResponse, resp, Options{})
		if err != nil {
			t.Fatalf("Convert: %v", err)
		}
		if !strings.Contains(string(out), `"reasoning_content":"think-a\nthink-b"`) {
			t.Fatalf("思考块未拼接进 reasoning_content: %s", out)
		}
		if !strings.Contains(string(out), `"content":"visible"`) {
			t.Fatalf("正文丢失: %s", out)
		}
		if strings.Contains(string(out), "SIG1") {
			t.Fatalf("thoughtSignature 外泄: %s", out)
		}
	})

	t.Run("hub_leg_carries_thinking", func(t *testing.T) {
		// claude→gemini（经 openai 枢纽两步）的非流式响应：W42 ③ 起中间腿经
		// reasoning_details 载体保真思考文本——gemini 出口落 thought part
		//（此前整块丢弃：claude→gemini 两步链静默丢思考）；不可识别签名（SIG）
		// 仍丢弃（红线）。
		resp := []byte(`{
			"id":"msg_2","type":"message","role":"assistant","model":"m",
			"content":[{"type":"thinking","thinking":"ponder","signature":"SIG"},
				{"type":"text","text":"answer"}],
			"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":2}
		}`)
		out, err := Convert(reg, fClaude, fGemini, FaceResponse, resp, Options{})
		if err != nil {
			t.Fatalf("Convert（此前此处 ErrUnsupportedBlock 断链）: %v", err)
		}
		if !strings.Contains(string(out), `"text":"ponder","thought":true`) {
			t.Fatalf("枢纽中间腿丢思考文本（W42 ③ 应经载体到 gemini 出口）: %s", out)
		}
		if strings.Contains(string(out), "SIG") {
			t.Fatalf("不可识别签名经枢纽腿泄漏: %s", out)
		}
		if !strings.Contains(string(out), `"text":"answer"`) {
			t.Fatalf("正文丢失: %s", out)
		}
	})

	t.Run("request_face_drops_thinking", func(t *testing.T) {
		// 回传轮（assistant 消息带 thinking 块）：请求面丢弃整块（签名兼容判定层
		// 不在本仓 → 表的 fallback 分支）；不报错、不塞 reasoning_content。
		req := []byte(`{
			"model":"m","max_tokens":16,
			"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"t","signature":"s"},
				{"type":"text","text":"a"}]},{"role":"user","content":"hi"}]
		}`)
		out, err := Convert(reg, fClaude, fOpenAI, FaceRequest, req, Options{})
		if err != nil {
			t.Fatalf("Convert: %v", err)
		}
		if strings.Contains(string(out), `"thinking"`) || strings.Contains(string(out), `"reasoning_content"`) {
			t.Fatalf("请求面思考未丢弃: %s", out)
		}
		if !strings.Contains(string(out), `"content":"a"`) {
			t.Fatalf("正文丢失: %s", out)
		}
	})

	t.Run("redacted_thinking_carrier", func(t *testing.T) {
		// W42 ①：redacted 载荷 ↔ reasoning.encrypted{data, format:anthropic-claude-v1}
		//（此前丢弃——表「加密思考 → 无」已废止；载荷可经 chat 载体往返）。
		resp := []byte(`{
			"id":"msg_3","type":"message","role":"assistant","model":"m",
			"content":[{"type":"redacted_thinking","data":"ENCRYPTED"},
				{"type":"text","text":"ok"}],
			"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}
		}`)
		out, err := Convert(reg, fClaude, fOpenAI, FaceResponse, resp, Options{})
		if err != nil {
			t.Fatalf("Convert: %v", err)
		}
		if !strings.Contains(string(out), `"data":"ENCRYPTED"`) ||
			!strings.Contains(string(out), `"type":"reasoning.encrypted"`) ||
			!strings.Contains(string(out), `"format":"anthropic-claude-v1"`) {
			t.Fatalf("redacted 载荷未走 reasoning.encrypted 载体: %s", out)
		}
		if strings.Contains(string(out), "reasoning_content") {
			t.Fatalf("redacted 载荷不得混入 reasoning_content: %s", out)
		}
	})

	t.Run("user_message_thinking_dropped", func(t *testing.T) {
		// 安全规则：只有 assistant 消息的 thinking 才映射（防注入）。
		req := []byte(`{
			"model":"m","max_tokens":16,
			"messages":[{"role":"user","content":[{"type":"thinking","thinking":"INJECT"},
				{"type":"text","text":"hi"}]}]
		}`)
		out, err := Convert(reg, fClaude, fOpenAI, FaceRequest, req, Options{})
		if err != nil {
			t.Fatalf("Convert: %v", err)
		}
		if strings.Contains(string(out), "INJECT") {
			t.Fatalf("user 消息的 thinking 未忽略: %s", out)
		}
	})
}

// ---------------------------------------------------------------------------
// W5-16 ④：工具定义面 {response, responseJsonSchema} 两表对称
// ---------------------------------------------------------------------------

// TestToolDefExtrasSymmetry gemini decl 独有键 {response, responseJsonSchema} 在
// 两个出口（openai / gemini）都必须被剔除（W5-11 只修了 claude 侧），且跨格式
// 直迁键（max_tokens / temperature）保留（防过滤过度）。
func TestToolDefExtrasSymmetry(t *testing.T) {
	reg := NewRegistry()

	gemReq := []byte(`{
		"contents":[{"role":"user","parts":[{"text":"hi"}]}],
		"tools":[{"functionDeclarations":[{"name":"f","description":"d",
			"parameters":{"type":"object"},
			"response":{"type":"object"},"responseJsonSchema":{"type":"object"}}]}]
	}`)
	openReq := []byte(`{
		"model":"m","temperature":0.5,
		"messages":[{"role":"user","content":"hi"}],
		"tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object"},
			"response":{"type":"object"},"responseJsonSchema":{"type":"object"}}}]
	}`)

	// gemini → openai（直转）：两个 decl 键不得进 OpenAI function 对象。
	out, err := Convert(reg, fGemini, fOpenAI, FaceRequest, gemReq, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	for _, k := range []string{"response", "responseJsonSchema"} {
		if containsJSONKey(t, out, k) {
			t.Fatalf("gemini decl 键 %q 泄漏进 openai 请求体: %s", k, out)
		}
	}

	// openai → gemini（直转）：非 gemini 来源的同名键不得进 functionDeclaration。
	out, err = Convert(reg, fOpenAI, fGemini, FaceRequest, openReq, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	for _, k := range []string{"response", "responseJsonSchema"} {
		if containsJSONKey(t, out, k) {
			t.Fatalf("来源独有键 %q 泄漏进 gemini 请求体: %s", k, out)
		}
	}
	// 防过滤过度：跨格式直迁键仍在（temperature 是直迁键面）。
	if !containsJSONKey(t, out, "temperature") {
		t.Fatalf("直迁键 temperature 被误删: %s", out)
	}

	// claude → gemini（经枢纽两步）：claude 工具级独有键不进 decl（W5-8 既有面，
	// 枢纽中间腿 + 出口腿都不含）。
	claudeReq := []byte(`{
		"model":"m","max_tokens":16,
		"messages":[{"role":"user","content":"hi"}],
		"tools":[{"name":"f","input_schema":{"type":"object"},
			"cache_control":{"type":"ephemeral"},"defer_loading":true,"eager_input_streaming":true}]
	}`)
	out, err = Convert(reg, fClaude, fGemini, FaceRequest, claudeReq, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	for _, k := range []string{"cache_control", "defer_loading", "eager_input_streaming"} {
		if containsJSONKey(t, out, k) {
			t.Fatalf("claude 工具级键 %q 泄漏进 gemini 请求体: %s", k, out)
		}
	}

	// 注册表方向核对：本次断言覆盖的三条腿（gemini→openai 直转、openai→gemini
	// 直转、claude→gemini 枢纽两步）与 registry 计划一致。
	if plan, err := reg.Lookup(fGemini, fOpenAI); err != nil || len(plan.Steps) != 1 {
		t.Fatalf("gemini→openai plan = %+v, err=%v", plan, err)
	}
	if plan, err := reg.Lookup(fClaude, fGemini); err != nil || len(plan.Steps) != 2 {
		t.Fatalf("claude→gemini plan = %+v, err=%v", plan, err)
	}
}

// 编译期护栏：registry 常量在测试内被引用（格式别名在别处定义）。
var _ = registry.FormatGeminiGenerate
