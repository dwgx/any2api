package openai_chat

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// TestGoldenOpenAIChat OpenAI 侧 golden 语料（src/translate/testdata/openai_chat/）：
// 输入 → parse → IR → render → 输出。
//
//   - request/response：输出与输入 JSON 语义相等（容忍键序/空白）；
//   - stream：exact=true 的用例逐帧语义相等（fixture 为渲染器规范形）；
//     所有用例验证 ① IR 不动点（parse(render(events)) == events 深等）
//     与 ② 渲染不动点（render 结果再渲染语义相等）。
//
// exact=false 的用例（stream_tool_calls）故意使用「同 chunk 同 index 多片段」
// 的输入形态（openai-wire §4.2 必须合并的情形）——合并后 IR 无帧边界信息，
// 渲染输出为规范分帧，故只验语义与不动点。
func TestGoldenOpenAIChat(t *testing.T) {
	dir := filepath.Join("..", "..", "testdata", "openai_chat")

	type goldenCase struct {
		name  string
		file  string
		kind  string // "request" | "response" | "stream"
		exact bool   // stream：渲染输出与输入逐帧语义相等
		opts  StreamRenderOptions
		check func(t *testing.T, events []ir.StreamEvent)
	}
	cases := []goldenCase{
		{name: "request_tools", file: "request_tools.json", kind: "request"},
		{name: "response_tool_calls", file: "response_tool_calls.json", kind: "response"},
		{
			name: "stream_tool_calls", file: "stream_tool_calls.sse", kind: "stream",
			opts: StreamRenderOptions{IncludeUsage: true},
			check: func(t *testing.T, events []ir.StreamEvent) {
				t.Helper()
				calls, err := ir.AssembleToolCalls(events)
				if err != nil {
					t.Fatalf("AssembleToolCalls: %v", err)
				}
				if len(calls) != 2 {
					t.Fatalf("calls = %+v, want 2", calls)
				}
				if calls[0].ID != "call_a" || calls[0].Name != "get_weather" || calls[0].Arguments != `{"city":"Paris"}` {
					t.Fatalf("calls[0] = %+v", calls[0])
				}
				if calls[1].ID != "call_b" || calls[1].Name != "get_time" || calls[1].Arguments != "{}" {
					t.Fatalf("calls[1] = %+v", calls[1])
				}
				if r, ok := doneReason(events); !ok || r != ir.StopToolUse {
					t.Fatalf("done reason = %q (ok=%v), want toolUse", r, ok)
				}
			},
		},
		{
			name: "stream_usage", file: "stream_usage.sse", kind: "stream", exact: true,
			opts: StreamRenderOptions{IncludeUsage: true},
			check: func(t *testing.T, events []ir.StreamEvent) {
				t.Helper()
				if got := collectText(events); got != "Hello world" {
					t.Fatalf("text = %q", got)
				}
				u := lastUsage(events)
				if u == nil || u.Input != 9 || u.Output != 2 || u.Total != 11 || u.Accounting != "openai" {
					t.Fatalf("usage = %+v", u)
				}
				if r, ok := doneReason(events); !ok || r != ir.StopStop {
					t.Fatalf("done reason = %q (ok=%v), want stop", r, ok)
				}
			},
		},
		{
			name: "stream_usage_absent", file: "stream_usage_absent.sse", kind: "stream", exact: true,
			opts: StreamRenderOptions{IncludeUsage: true},
			check: func(t *testing.T, events []ir.StreamEvent) {
				t.Helper()
				if got := collectText(events); got != "Bye" {
					t.Fatalf("text = %q", got)
				}
				if u := lastUsage(events); u != nil {
					t.Fatalf("usage = %+v, want none (缺失是常态)", u)
				}
				if r, ok := doneReason(events); !ok || r != ir.StopStop {
					t.Fatalf("done reason = %q (ok=%v), want stop", r, ok)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in, err := os.ReadFile(filepath.Join(dir, tc.file))
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			switch tc.kind {
			case "request":
				req, err := ParseRequest(in)
				if err != nil {
					t.Fatalf("ParseRequest: %v", err)
				}
				out, err := RenderRequest(req, RenderOptions{})
				if err != nil {
					t.Fatalf("RenderRequest: %v", err)
				}
				assertJSONEqual(t, in, out)
			case "response":
				resp, err := ParseResponse(in)
				if err != nil {
					t.Fatalf("ParseResponse: %v", err)
				}
				out, err := RenderResponse(resp, RenderOptions{})
				if err != nil {
					t.Fatalf("RenderResponse: %v", err)
				}
				assertJSONEqual(t, in, out)
			case "stream":
				events, err := ParseStream(bytes.NewReader(in))
				if err != nil {
					t.Fatalf("ParseStream: %v", err)
				}
				if tc.check != nil {
					tc.check(t, events)
				}
				out, err := RenderStream(events, tc.opts)
				if err != nil {
					t.Fatalf("RenderStream: %v", err)
				}
				if tc.exact {
					assertSSEEqual(t, in, out)
				}
				// IR 不动点。
				events2, err := ParseStream(bytes.NewReader(out))
				if err != nil {
					t.Fatalf("re-ParseStream: %v\n%s", err, out)
				}
				if !reflect.DeepEqual(events, events2) {
					t.Fatalf("IR fixed point broken:\nfirst:  %+v\nsecond: %+v", events, events2)
				}
				// 渲染不动点。
				out2, err := RenderStream(events2, tc.opts)
				if err != nil {
					t.Fatalf("re-RenderStream: %v", err)
				}
				assertSSEEqual(t, out, out2)
			default:
				t.Fatalf("unknown kind %q", tc.kind)
			}
		})
	}
}

// assertJSONEqual 语义比较两个 JSON（键序/空白不敏感）。
func assertJSONEqual(t *testing.T, want, got []byte) {
	t.Helper()
	var w, g any
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("want is not JSON: %v\n%s", err, want)
	}
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("got is not JSON: %v\n%s", err, got)
	}
	if !reflect.DeepEqual(w, g) {
		t.Fatalf("JSON mismatch:\nwant: %s\ngot:  %s", want, got)
	}
}

// sseFrames 提取 SSE 帧的 data 载荷序列（"[DONE]" 原样；多行 data 以 \n 连接）。
func sseFrames(t *testing.T, data []byte) []string {
	t.Helper()
	var frames []string
	var cur []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			if len(cur) > 0 {
				frames = append(frames, strings.Join(cur, "\n"))
				cur = nil
			}
			continue
		}
		if v, ok := strings.CutPrefix(line, "data:"); ok {
			cur = append(cur, strings.TrimPrefix(v, " "))
		}
	}
	if len(cur) > 0 {
		frames = append(frames, strings.Join(cur, "\n"))
	}
	return frames
}

// assertSSEEqual 逐帧语义比较两个 SSE 流（帧数与顺序必须一致）。
func assertSSEEqual(t *testing.T, want, got []byte) {
	t.Helper()
	wf := sseFrames(t, want)
	gf := sseFrames(t, got)
	if len(wf) != len(gf) {
		t.Fatalf("SSE frame count mismatch: want %d, got %d\nwant:\n%s\ngot:\n%s",
			len(wf), len(gf), want, got)
	}
	for i := range wf {
		if wf[i] == doneSentinel || gf[i] == doneSentinel {
			if wf[i] != gf[i] {
				t.Fatalf("frame %d: want %q, got %q", i, wf[i], gf[i])
			}
			continue
		}
		assertJSONEqual(t, []byte(wf[i]), []byte(gf[i]))
	}
}

func doneReason(events []ir.StreamEvent) (ir.StopReason, bool) {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Kind == ir.EventDone {
			return events[i].Reason, true
		}
	}
	return "", false
}

func lastUsage(events []ir.StreamEvent) *ir.Usage {
	var u *ir.Usage
	for _, ev := range events {
		if ev.Usage != nil {
			u = ev.Usage
		}
	}
	return u
}

func collectText(events []ir.StreamEvent) string {
	var b strings.Builder
	for _, ev := range events {
		if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockText {
			b.WriteString(ev.TextDelta)
		}
	}
	return b.String()
}
