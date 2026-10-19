package claude_messages

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

// TestGoldenClaudeMessages claude_messages 侧 golden 语料（src/translate/testdata/claude_messages/）：
// 输入 → parse → IR → render → 输出。
//
//   - request/response：输出与输入 JSON 语义相等（容忍键序/空白）；
//   - stream：exact=true 的用例逐帧语义相等（fixture 为渲染器规范形）；
//     exact=false 的用例（stream_usage_cumulative）故意使用「message_delta.usage
//     省略 message_start 已有字段」的真实形态（AM usage 累计覆盖语义）——合并后
//     IR 无「省略」信息，渲染输出为规范全量，故只验语义与不动点。
//
// 所有 stream 用例验证 ① IR 不动点（parse(render(events)) == events 深等）与
// ② 渲染不动点（render 结果再渲染逐帧语义相等）。
func TestGoldenClaudeMessages(t *testing.T) {
	dir := filepath.Join("..", "..", "testdata", "claude_messages")

	type goldenCase struct {
		name  string
		file  string
		kind  string // "request" | "response" | "stream"
		exact bool
		check func(t *testing.T, events []ir.StreamEvent)
	}
	cases := []goldenCase{
		{name: "request_full", file: "request_full.json", kind: "request"},
		{name: "response_thinking", file: "response_thinking.json", kind: "response"},
		{
			name: "stream_text_tool", file: "stream_text_tool.sse", kind: "stream", exact: true,
			check: func(t *testing.T, events []ir.StreamEvent) {
				t.Helper()
				if got := collectText(events); got != "Hello" {
					t.Fatalf("text = %q", got)
				}
				calls, err := ir.AssembleToolCalls(events)
				if err != nil {
					t.Fatalf("AssembleToolCalls: %v", err)
				}
				if len(calls) != 1 || calls[0].ID != "toolu_03" || calls[0].Name != "get_weather" ||
					calls[0].Arguments != `{"city":"Paris"}` {
					t.Fatalf("calls = %+v", calls)
				}
				if r, ok := doneReason(events); !ok || r != ir.StopToolUse {
					t.Fatalf("done reason = %q (ok=%v), want toolUse", r, ok)
				}
				u := lastUsage(events)
				if u == nil || u.Input != 10 || u.CacheRead != 4 || u.Output != 17 || u.Total != 31 ||
					u.Accounting != "anthropic" || !u.Estimated {
					t.Fatalf("usage = %+v", u)
				}
			},
		},
		{
			name: "stream_thinking", file: "stream_thinking.sse", kind: "stream", exact: true,
			check: func(t *testing.T, events []ir.StreamEvent) {
				t.Helper()
				if got := collectText(events); got != "Answer." {
					t.Fatalf("text = %q", got)
				}
				if got := collectSignature(events); got != "Qabc-def" {
					t.Fatalf("signature = %q", got) // ⑦ 签名原样保留（含多片拼接）
				}
				if r, ok := doneReason(events); !ok || r != ir.StopStop {
					t.Fatalf("done reason = %q (ok=%v), want stop", r, ok)
				}
			},
		},
		{
			name: "stream_usage_cumulative", file: "stream_usage_cumulative.sse", kind: "stream", exact: false,
			check: func(t *testing.T, events []ir.StreamEvent) {
				t.Helper()
				// ⑤ 累计覆盖语义：message_delta 只给 output_tokens，input/cache 必须
				// 保留 message_start 原值（写成覆盖为 0 或 += 都是缺陷）。
				u := lastUsage(events)
				if u == nil || u.Input != 100 || u.CacheRead != 50 || u.Output != 7 || u.Total != 157 {
					t.Fatalf("usage = %+v, want input=100 cache_read=50 output=7 total=157", u)
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
				out, err := RenderRequest(req)
				if err != nil {
					t.Fatalf("RenderRequest: %v", err)
				}
				assertJSONEqual(t, in, out)
			case "response":
				resp, err := ParseResponse(in)
				if err != nil {
					t.Fatalf("ParseResponse: %v", err)
				}
				out, err := RenderResponse(resp)
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
				out, err := RenderStream(events)
				if err != nil {
					t.Fatalf("RenderStream: %v", err)
				}
				if tc.exact {
					assertSSEEqualClaude(t, in, out)
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
				out2, err := RenderStream(events2)
				if err != nil {
					t.Fatalf("re-RenderStream: %v", err)
				}
				assertSSEEqualClaude(t, out, out2)
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

// claudeFrame 一个命名事件帧（event: 名 + data 载荷）。
type claudeFrame struct {
	event string
	data  string
}

// claudeFrames 提取命名事件 SSE 的帧序列（event/data 配对；空行分帧）。
func claudeFrames(t *testing.T, raw []byte) []claudeFrame {
	t.Helper()
	var frames []claudeFrame
	var cur claudeFrame
	has := false
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			if has {
				frames = append(frames, cur)
				cur = claudeFrame{}
				has = false
			}
			continue
		}
		if v, ok := strings.CutPrefix(line, "event:"); ok {
			cur.event = strings.TrimSpace(v)
			has = true
			continue
		}
		if v, ok := strings.CutPrefix(line, "data:"); ok {
			cur.data = strings.TrimPrefix(v, " ")
			has = true
			continue
		}
	}
	if has {
		frames = append(frames, cur)
	}
	return frames
}

// assertSSEEqualClaude 逐帧语义比较两个命名事件 SSE 流（帧数与顺序必须一致；
// 每帧的 event 名与 data JSON 语义相等）。
func assertSSEEqualClaude(t *testing.T, want, got []byte) {
	t.Helper()
	wf := claudeFrames(t, want)
	gf := claudeFrames(t, got)
	if len(wf) != len(gf) {
		t.Fatalf("SSE frame count mismatch: want %d, got %d\nwant:\n%s\ngot:\n%s",
			len(wf), len(gf), want, got)
	}
	for i := range wf {
		if wf[i].event != gf[i].event {
			t.Fatalf("frame %d: want event %q, got %q", i, wf[i].event, gf[i].event)
		}
		assertJSONEqual(t, []byte(wf[i].data), []byte(gf[i].data))
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

func collectSignature(events []ir.StreamEvent) string {
	var b strings.Builder
	for _, ev := range events {
		if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockThinking {
			b.WriteString(ev.SignatureDelta)
		}
	}
	return b.String()
}
