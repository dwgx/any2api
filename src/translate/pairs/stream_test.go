package pairs

import (
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/registry"
)

// feedSSEFrames 把 SSE 字节流按帧喂给回调（event 名可为空；多行 data 以 \n 连接）。
func feedSSEFrames(t *testing.T, raw []byte, fn func(event string, data []byte) error) {
	t.Helper()
	var event string
	var data []string
	flush := func() error {
		if len(data) == 0 {
			event = ""
			return nil
		}
		payload := strings.Join(data, "\n")
		data = data[:0]
		ev := event
		event = ""
		return fn(ev, []byte(payload))
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			if err := flush(); err != nil {
				t.Fatalf("feed: %v", err)
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		if v, ok := strings.CutPrefix(line, "event:"); ok {
			event = strings.TrimSpace(v)
			continue
		}
		if v, ok := strings.CutPrefix(line, "data:"); ok {
			data = append(data, strings.TrimPrefix(v, " "))
			continue
		}
	}
	if err := flush(); err != nil {
		t.Fatalf("feed: %v", err)
	}
}

// TestStreamPipeMatchesBatchGolden 增量管道逐帧喂入与批量转换产出同一结果
// （帧级语义相等）——服务壳 relay 与 pairs.Convert 行为一致性锚点。
func TestStreamPipeMatchesBatchGolden(t *testing.T) {
	reg := NewRegistry()
	cases := []struct {
		name     string
		in       string
		want     string
		from, to registry.Format
		opts     Options
	}{
		{"openai_to_claude", "stream_openai_to_claude.sse", "stream_openai_to_claude.claude.sse", fOpenAI, fClaude, Options{}},
		{"claude_to_openai_usage", "stream_claude_to_openai.sse", "stream_claude_to_openai.openai.sse", fClaude, fOpenAI, Options{IncludeUsage: true}},
		{"claude_to_openai_tool", "stream_claude_tool_to_openai.sse", "stream_claude_tool_to_openai.openai.sse", fClaude, fOpenAI, Options{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pipe, err := NewStreamPipe(reg, tc.from, tc.to, tc.opts)
			if err != nil {
				t.Fatalf("NewStreamPipe: %v", err)
			}
			var out []byte
			feedSSEFrames(t, readFixture(t, tc.in), func(event string, data []byte) error {
				b, err := pipe.Feed(event, data)
				out = append(out, b...)
				return err
			})
			b, err := pipe.Finish()
			if err != nil {
				t.Fatalf("Finish: %v", err)
			}
			out = append(out, b...)
			assertSSEEqual(t, readFixture(t, tc.want), out)
		})
	}
}

func TestStreamPipeRejectsIdentityAndUnknownFormats(t *testing.T) {
	reg := NewRegistry()
	if _, err := NewStreamPipe(reg, fClaude, fClaude, Options{}); err == nil {
		t.Fatalf("identity pipe must error")
	}
	// 未登记格式（无任何腿）：显式报错。
	if _, err := NewStreamPipe(reg, registry.Format("vertex_generate"), fClaude, Options{}); err == nil {
		t.Fatalf("unknown format pipe must error")
	}
	// gemini→claude 是枢纽两步（本 slice 起受支持）——不再是错误路径。
	if _, err := NewStreamPipe(reg, registry.FormatGeminiGenerate, fClaude, Options{}); err != nil {
		t.Fatalf("hub two-step pipe must be accepted: %v", err)
	}
}

func TestStreamPipeFeedErrorPropagates(t *testing.T) {
	reg := NewRegistry()
	pipe, err := NewStreamPipe(reg, fOpenAI, fClaude, Options{})
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	if _, err := pipe.Feed("", []byte("{not json")); err == nil {
		t.Fatalf("garbage frame must error")
	}
}

func TestRenderErrorFor(t *testing.T) {
	out, err := RenderErrorFor(fClaude, &ir.Error{Type: "overloaded_error", Message: "busy"})
	if err != nil {
		t.Fatalf("RenderErrorFor: %v", err)
	}
	if !strings.Contains(string(out), `"type":"error"`) || !strings.Contains(string(out), `"type":"overloaded_error"`) {
		t.Fatalf("claude envelope = %s", out)
	}
	out, err = RenderErrorFor(fOpenAI, &ir.Error{Type: "overloaded_error", Message: "busy"})
	if err != nil {
		t.Fatalf("RenderErrorFor: %v", err)
	}
	if !strings.Contains(string(out), `"error"`) || !strings.Contains(string(out), `"code":"overloaded_error"`) {
		t.Fatalf("openai envelope = %s", out)
	}
}
