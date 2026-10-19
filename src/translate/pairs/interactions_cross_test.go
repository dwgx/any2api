package pairs

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/registry"
)

// fInter interactions 格式别名（pairs 本地常量；registry 追加行见
// research/briefs/reports/A2A-L19-W5-13.md——登记后等价替换为
// registry.FormatInteractions）。
const fInter = FormatInteractions

// TestCrossGoldenInteractionsRequests interactions 请求面跨格式 golden：
// ① openai_chat → interactions（表 A1，显式对 1 步）；② gemini → interactions
// （表 B1，无显式对 → 枢纽两步，Quality ≤ fair）。
func TestCrossGoldenInteractionsRequests(t *testing.T) {
	reg := NewRegistry()

	// ① chat → interactions：role→step 类型、工具参数对象化、required→any。
	out, err := Convert(reg, fOpenAI, fInter, FaceRequest, readFixture(t, "request_openai_to_interactions.json"), Options{})
	if err != nil {
		t.Fatalf("chat→interactions: %v", err)
	}
	assertJSONEqual(t, readFixture(t, "request_openai_to_interactions.interactions.json"), out)

	// ② gemini → interactions：表 B1 显式对（1 步、good）。
	plan, err := reg.Lookup(fGemini, fInter)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(plan.Steps) != 1 || plan.ViaHub || plan.Quality != registry.QualityGood {
		t.Fatalf("gemini→interactions plan = %+v", plan)
	}
	out, err = Convert(reg, fGemini, fInter, FaceRequest, readFixture(t, "request_gemini_to_interactions.json"), Options{})
	if err != nil {
		t.Fatalf("gemini→interactions: %v", err)
	}
	assertJSONEqual(t, readFixture(t, "request_gemini_to_interactions.interactions.json"), out)

	var w struct {
		Model      string `json:"model"`
		System     string `json:"system_instruction"`
		Stream     bool   `json:"stream"`
		Generation struct {
			MaxOutput int64   `json:"max_output_tokens"`
			Temp      float64 `json:"temperature"`
			Thinking  string  `json:"thinking_level"`
		} `json:"generation_config"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v (%s)", err, out)
	}
	if w.Model != "gemini-3-flash-preview" || w.System != "Be brief." || !w.Stream ||
		w.Generation.MaxOutput != 64 || w.Generation.Temp != 0.3 || w.Generation.Thinking != "low" {
		t.Fatalf("hub request = %+v", w)
	}
}

// TestCrossGoldenInteractionsHubRequest claude → interactions 表未给（CPA 有实现但
// 本仓无表行）→ 枢纽两步（claude→chat→interactions，Quality ≤ fair）。
func TestCrossGoldenInteractionsHubRequest(t *testing.T) {
	reg := NewRegistry()
	plan, err := reg.Lookup(fClaude, fInter)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(plan.Steps) != 2 || !plan.ViaHub ||
		plan.Steps[0] != (registry.Step{From: fClaude, To: fOpenAI}) ||
		plan.Steps[1] != (registry.Step{From: fOpenAI, To: fInter}) {
		t.Fatalf("plan = %+v", plan)
	}
	if plan.Quality == registry.QualityGood {
		t.Fatalf("hub plan quality = %q, want ≤ fair", plan.Quality)
	}
	out, err := Convert(reg, fClaude, fInter, FaceRequest, readFixture(t, "request_claude_to_interactions_hub.json"), Options{})
	if err != nil {
		t.Fatalf("claude→interactions: %v", err)
	}
	assertJSONEqual(t, readFixture(t, "request_claude_to_interactions_hub.interactions.json"), out)
}

// TestCrossGoldenInteractionsStream interactions 流 → openai_chat 流（表 A3）：
// 三拍事件 → chat delta 帧 + finish_reason；批量与增量（StreamPipe）同源同帧。
func TestCrossGoldenInteractionsStream(t *testing.T) {
	reg := NewRegistry()
	in := readFixture(t, "stream_interactions_to_openai.sse")
	out, err := Convert(reg, fInter, fOpenAI, FaceStream, in, Options{})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	assertSSEEqual(t, readFixture(t, "stream_interactions_to_openai.openai.sse"), out)

	s := string(out)
	for _, want := range []string{
		`"delta":{"role":"assistant","content":""}`,
		`"delta":{"content":"Hello"}`,
		`"delta":{"content":" world"}`,
		`"finish_reason":"stop"`,
		"data: [DONE]",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("chat stream missing %q: %s", want, s)
		}
	}
	if strings.Contains(s, `"finish_reason":"tool_calls"`) {
		t.Fatalf("unexpected tool finish: %s", s)
	}

	// 增量管道与批量同帧（入口逐帧 Feed）。
	pipe, err := NewStreamPipe(reg, fInter, fOpenAI, Options{})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	var got []byte
	var sawEarly bool
	for _, f := range sseFramesOf(t, in) {
		b, err := pipe.Feed(f.event, []byte(f.data))
		if err != nil {
			t.Fatalf("Feed(%s): %v", f.event, err)
		}
		if len(b) > 0 {
			sawEarly = true
		}
		got = append(got, b...)
	}
	tail, err := pipe.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	got = append(got, tail...)
	if !sawEarly {
		t.Fatalf("pipe produced no incremental bytes")
	}
	assertSSEEqual(t, out, got)
}

// TestInteractionsRequestToOpenAI 反向（表 A2）：interactions 请求 → chat 请求。
// system_instruction → 首条 system；steps → messages；function_result → tool 消息；
// 参数面 max_tokens/stop/tool_choice（any→required）；tools 嵌套包装。
func TestInteractionsRequestToOpenAI(t *testing.T) {
	reg := NewRegistry()
	in := []byte(`{"model":"gemini-3-flash-preview","system_instruction":"Be terse.","input":[
		{"type":"user_input","content":[{"type":"text","text":"hi"}]},
		{"type":"function_call","id":"call_1","name":"f","arguments":{"a":1}},
		{"type":"function_result","call_id":"call_1","name":"f","result":[{"type":"text","text":"ok"}]}
	],"tools":[{"type":"function","name":"f","description":"d","parameters":{"type":"object"}}],
	"generation_config":{"max_output_tokens":10,"temperature":0.5,"stop_sequences":["X"],"tool_choice":"any"}}`)
	out, err := Convert(reg, fInter, fOpenAI, FaceRequest, in, Options{})
	if err != nil {
		t.Fatalf("interactions→chat: %v", err)
	}
	var w struct {
		Model      string   `json:"model"`
		MaxTokens  int64    `json:"max_tokens"`
		Temp       float64  `json:"temperature"`
		Stop       []string `json:"stop"`
		ToolChoice string   `json:"tool_choice"`
		Messages   []struct {
			Role       string          `json:"role"`
			Content    json.RawMessage `json:"content"`
			ToolCallID string          `json:"tool_call_id"`
			ToolCalls  []struct {
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"messages"`
		Tools []struct {
			Type     string `json:"type"`
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v (%s)", err, out)
	}
	if w.Model != "gemini-3-flash-preview" || w.MaxTokens != 10 || w.Temp != 0.5 ||
		len(w.Stop) != 1 || w.Stop[0] != "X" || w.ToolChoice != "required" {
		t.Fatalf("params = %+v", w)
	}
	if len(w.Messages) != 4 || w.Messages[0].Role != "system" || w.Messages[1].Role != "user" ||
		w.Messages[2].Role != "assistant" || w.Messages[3].Role != "tool" {
		t.Fatalf("messages = %+v", w.Messages)
	}
	tc := w.Messages[2].ToolCalls
	if len(tc) != 1 || tc[0].ID != "call_1" || tc[0].Function.Name != "f" ||
		tc[0].Function.Arguments != `{"a":1}` {
		t.Fatalf("tool_calls = %+v", tc)
	}
	if w.Messages[3].ToolCallID != "call_1" {
		t.Fatalf("tool message = %+v", w.Messages[3])
	}
	if len(w.Tools) != 1 || w.Tools[0].Type != "function" || w.Tools[0].Function.Name != "f" {
		t.Fatalf("tools = %+v", w.Tools)
	}
}

// TestInteractionsHubTwoStep claude ↔ interactions 表未给 → 枢纽两步
// （Quality ≤ fair）；显式对四方向 Quality=good。
func TestInteractionsHubTwoStep(t *testing.T) {
	reg := NewRegistry()
	plan, err := reg.Lookup(fClaude, fInter)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(plan.Steps) != 2 || !plan.ViaHub || plan.Quality == registry.QualityGood {
		t.Fatalf("plan = %+v", plan)
	}
	for _, dir := range []struct{ from, to registry.Format }{
		{fOpenAI, fInter}, {fInter, fOpenAI}, {fGemini, fInter}, {fInter, fGemini},
	} {
		p, err := reg.Lookup(dir.from, dir.to)
		if err != nil {
			t.Fatalf("Lookup %s→%s: %v", dir.from, dir.to, err)
		}
		if len(p.Steps) != 1 || p.ViaHub || p.Quality != registry.QualityGood {
			t.Fatalf("explicit pair %s→%s = %+v", dir.from, dir.to, p)
		}
	}
}

// sseFrame 一对 (event, data)——pairs 测试内的最小切帧（保留 event 名）。
type sseFrame struct {
	event string
	data  string
}

func sseFramesOf(t *testing.T, raw []byte) []sseFrame {
	t.Helper()
	var (
		frames []sseFrame
		name   string
		data   []string
	)
	flush := func() {
		if len(data) == 0 {
			name = ""
			return
		}
		frames = append(frames, sseFrame{event: name, data: strings.Join(data, "\n")})
		name = ""
		data = nil
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			flush()
			continue
		}
		if v, ok := strings.CutPrefix(line, "event:"); ok {
			name = strings.TrimSpace(v)
			continue
		}
		if v, ok := strings.CutPrefix(line, "data:"); ok {
			data = append(data, strings.TrimPrefix(v, " "))
		}
	}
	flush()
	return frames
}
