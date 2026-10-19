package responses

// W34-3：W33-3 golden-corpus Responses 事件族语料入包（逐事件对拍）。
//
// 语料出处（research/topics/golden-corpus/assets/fixtures/，自建，2026-10-15）：
//   - responses-function-call-arguments.json        → testdata/responses/stream_function_call_arguments.sse
//   - responses-reasoning-summary.json              → testdata/responses/stream_reasoning_summary.sse
//   - responses-error-event-flat.json               → testdata/responses/stream_error_event_flat.sse
//   - responses-incomplete-max-output-tokens.json   → testdata/responses/stream_incomplete_max_output_tokens.sse
//
// 入包转换（event: 行 + data: 行；凭据扫描 0 命中）见 tmp/lanes/W34-resp/extract_sse.py。
// 逐事件期望 = 每帧独立 Feed 的 IR 事件（含空帧），EOF 的 Finish 单独断言——
// 事件族顺序/字段语义任何回归都会在本文件红。

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// w34Usage 期望 usage（openai 口径；details 原文进 Extra——解析侧保真旁路）。
func w34Usage(input, output, total, reasoning int64) *ir.Usage {
	return &ir.Usage{
		Input: input, Output: output, Total: total, Reasoning: reasoning,
		Accounting: "openai",
		Extra: map[string]json.RawMessage{
			"prompt_tokens_details":     json.RawMessage(`{"cached_tokens":0}`),
			"completion_tokens_details": json.RawMessage(fmt.Sprintf(`{"reasoning_tokens":%d}`, reasoning)),
		},
	}
}

// TestW34CorpusPerEvent 逐事件对拍：四组语料每帧的 IR 事件逐一断言。
func TestW34CorpusPerEvent(t *testing.T) {
	cases := []struct {
		name   string
		file   string
		frames [][]ir.StreamEvent // 逐帧期望（nil = 该帧无 IR 事件）
		finish []ir.StreamEvent   // EOF 期望
		// semantic 语料语义断言的 src 侧镜像（在逐帧断言之后跑）。
		semantic func(t *testing.T, events []ir.StreamEvent)
	}{
		{
			// #18 function_call_arguments：delta×2（无 content_index）→ done 全量
			// 不重发 → completed(toolUse)。
			name: "function_call_arguments",
			file: "stream_function_call_arguments.sse",
			semantic: func(t *testing.T, events []ir.StreamEvent) {
				// 语料 json_concat_equals：delta 拼接 == done 全量（合法 JSON）。
				if got := argsConcat(events); got != `{"city":"Paris"}` || !json.Valid([]byte(got)) {
					t.Fatalf("args concat = %q (valid=%v)", got, json.Valid([]byte(got)))
				}
			},
			frames: [][]ir.StreamEvent{
				{{Kind: ir.EventStart, MessageID: "resp_gc18", Model: "gc-model", Created: 1742000000}},
				nil,
				{{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockToolUse, ToolCallID: "call_gc18", ToolName: "get_weather"}},
				{{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockToolUse, ArgsDelta: `{"city"`}},
				{{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockToolUse, ArgsDelta: `:"Paris"}`}},
				{{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockToolUse}},
				nil, // output_item.done：块已收尾（幂等）
				{
					{Kind: ir.EventMessageDelta, StopReason: ir.StopToolUse},
					{Kind: ir.EventMessageDelta, Usage: w34Usage(20, 8, 28, 0)},
					{Kind: ir.EventMessageStop},
					{Kind: ir.EventDone, Reason: ir.StopToolUse},
				},
			},
		},
		{
			// #19 reasoning_summary：part.added/done 无独立 IR 事件（分段由
			// summary_index 断段规则承载）；text.delta 落 thinking 块；completed(stop)。
			name: "reasoning_summary",
			file: "stream_reasoning_summary.sse",
			semantic: func(t *testing.T, events []ir.StreamEvent) {
				// 语料 text_merge：delta 拼接 == done 全量。
				if got := collectThinking(events); got != "Let me think." {
					t.Fatalf("thinking concat = %q", got)
				}
			},
			frames: [][]ir.StreamEvent{
				{{Kind: ir.EventStart, MessageID: "resp_gc19", Model: "gc-model", Created: 1742000000}},
				nil,
				{{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockThinking}},
				nil, // reasoning_summary_part.added：无 IR 事件
				{{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockThinking, TextDelta: "Let me "}},
				{{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockThinking, TextDelta: "think."}},
				nil, // reasoning_summary_text.done：已下发，不重发
				nil, // reasoning_summary_part.done：无 IR 事件
				{{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockThinking}},
				{
					{Kind: ir.EventMessageDelta, StopReason: ir.StopStop},
					{Kind: ir.EventMessageDelta, Usage: w34Usage(12, 5, 17, 5)},
					{Kind: ir.EventMessageStop},
					{Kind: ir.EventDone, Reason: ir.StopStop},
				},
			},
		},
		{
			// #20 error_event_flat：文本已下发后流内平铺 error——error 帧 Raw 保真
			// （期望在读到语料后按帧原文构造，见下）。
			name: "error_event_flat",
			file: "stream_error_event_flat.sse",
			semantic: func(t *testing.T, events []ir.StreamEvent) {
				// 语料 text_merge：已下发的部分文本不丢。
				if got := collectText(events); got != "partial" {
					t.Fatalf("text concat = %q", got)
				}
				// 语料 field_absent(error)：流内 error 帧字段平铺（无嵌套 error 对象）。
				var e *ir.Error
				for i := range events {
					if events[i].Kind == ir.EventError {
						e = events[i].Error
					}
				}
				if e == nil {
					t.Fatalf("no error event: %+v", events)
				}
				var m map[string]json.RawMessage
				if err := json.Unmarshal(e.Raw, &m); err != nil {
					t.Fatalf("error Raw: %v", err)
				}
				if _, ok := m["error"]; ok {
					t.Fatalf("nested error object present: %s", e.Raw)
				}
				if string(m["code"]) != `"server_error"` {
					t.Fatalf("code = %s", m["code"])
				}
			},
			frames: [][]ir.StreamEvent{
				{{Kind: ir.EventStart, MessageID: "resp_gc20", Model: "gc-model", Created: 1742000000}},
				nil, // message item 惰性开块
				{
					{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockText},
					{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockText, TextDelta: "partial"},
				},
				nil, // 占位：运行时替换为 error 期望
			},
		},
		{
			// #23 incomplete_max_output_tokens：半 JSON 参数 delta×2 → incomplete
			// 收尾（块闭 + stop=length + usage）。
			name: "incomplete_max_output_tokens",
			file: "stream_incomplete_max_output_tokens.sse",
			semantic: func(t *testing.T, events []ir.StreamEvent) {
				// 语料 json_concat_invalid：截断半 JSON——拼接必须**非法**。
				if got := argsConcat(events); got != `{"city":"San Francisco` || json.Valid([]byte(got)) {
					t.Fatalf("truncated args concat = %q (valid=%v)", got, json.Valid([]byte(got)))
				}
			},
			frames: [][]ir.StreamEvent{
				{{Kind: ir.EventStart, MessageID: "resp_gc23", Model: "gc-model", Created: 1742000000}},
				{{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockToolUse, ToolCallID: "call_gc23", ToolName: "get_weather"}},
				{{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockToolUse, ArgsDelta: `{"city":"San`}},
				{{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockToolUse, ArgsDelta: ` Francisco`}},
				{
					{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockToolUse},
					{Kind: ir.EventMessageDelta, StopReason: ir.StopLength},
					{Kind: ir.EventMessageDelta, Usage: w34Usage(18, 6, 24, 0)},
					{Kind: ir.EventMessageStop},
					{Kind: ir.EventDone, Reason: ir.StopLength},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := readGolden(t, tc.file)
			frames := sseFrames(t, data)
			if len(frames) != len(tc.frames) {
				t.Fatalf("frame count = %d, want %d", len(frames), len(tc.frames))
			}
			if tc.name == "error_event_flat" {
				// error 帧的期望：平铺字段解析 + Raw = 帧原文（诊断保真）。
				tc.frames[3] = []ir.StreamEvent{{
					Kind: ir.EventError, ErrorReason: ir.ErrorUpstream,
					Error: &ir.Error{
						Type: "api_error", Code: "server_error",
						Message: "An error occurred while processing your request.",
						Source:  "openai", Raw: json.RawMessage(frames[3]),
					},
				}}
			}
			p := NewStreamParser()
			var all []ir.StreamEvent
			for i, payload := range frames {
				got, err := p.Feed([]byte(payload))
				if err != nil {
					t.Fatalf("frame %d feed: %v", i, err)
				}
				if !reflect.DeepEqual(got, tc.frames[i]) {
					t.Fatalf("frame %d events mismatch:\ngot:  %+v\nwant: %+v", i, got, tc.frames[i])
				}
				all = append(all, got...)
			}
			if got := p.Finish(); !reflect.DeepEqual(got, tc.finish) {
				t.Fatalf("finish mismatch:\ngot:  %+v\nwant: %+v", got, tc.finish)
			}
			if tc.semantic != nil {
				tc.semantic(t, all)
			}
		})
	}
}

// argsConcat 按块序拼接全部 tool_use ArgsDelta（半 JSON 逐片拼接的消费端口径）。
func argsConcat(events []ir.StreamEvent) string {
	var b []byte
	for _, ev := range events {
		if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockToolUse {
			b = append(b, ev.ArgsDelta...)
		}
	}
	return string(b)
}

// TestW34CorpusRenderReparse 四组语料 render→parse 回路：writer 输出必须能被自家
// parser 重读（事件 kind 序列逐项相等；终态语义相等）。error 流锁 W34-3 缺口修复：
// Fail() 形态 error + response.failed 双帧此前会撞 ErrEventsAfterTerminal。
func TestW34CorpusRenderReparse(t *testing.T) {
	files := []string{
		"stream_function_call_arguments.sse",
		"stream_reasoning_summary.sse",
		"stream_error_event_flat.sse",
		"stream_incomplete_max_output_tokens.sse",
	}
	for _, f := range files {
		t.Run(f, func(t *testing.T) {
			events, err := ParseStream(bytes.NewReader(readGolden(t, f)))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			out, err := RenderStream(events)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			back, err := ParseStream(bytes.NewReader(out))
			if err != nil {
				t.Fatalf("re-parse (writer 输出必须能被自家 parser 重读): %v\n%s", err, out)
			}
			if len(back) != len(events) {
				t.Fatalf("event count: %d vs %d", len(events), len(back))
			}
			for i := range events {
				if back[i].Kind != events[i].Kind {
					t.Fatalf("event %d kind: %s vs %s", i, back[i].Kind, events[i].Kind)
				}
			}
			last, got := events[len(events)-1], back[len(back)-1]
			switch last.Kind {
			case ir.EventDone:
				if got.Kind != ir.EventDone || got.Reason != last.Reason {
					t.Fatalf("done reason: %q vs %q", got.Reason, last.Reason)
				}
			case ir.EventError:
				if got.Kind != ir.EventError || got.Error == nil || last.Error == nil ||
					got.Error.Code != last.Error.Code || got.Error.Message != last.Error.Message {
					t.Fatalf("error terminal: %+v vs %+v", got.Error, last.Error)
				}
			default:
				t.Fatalf("unexpected terminal kind %s", last.Kind)
			}
		})
	}
}

// TestW34CorpusErrorFamilyToleranceNarrow error 后的容忍必须是窄的：错误族帧
// （error / response.failed）幂等忽略，内容帧仍判协议违规（ErrEventsAfterTerminal）。
func TestW34CorpusErrorFamilyToleranceNarrow(t *testing.T) {
	p := NewStreamParser()
	errFrame := `{"type":"error","code":"server_error","message":"boom","param":null,"sequence_number":0}`
	evs, err := p.Feed([]byte(errFrame))
	if err != nil || len(evs) != 1 || evs[0].Kind != ir.EventError {
		t.Fatalf("first error frame: %+v, %v", evs, err)
	}
	// 错误族重复帧：幂等忽略（Fail() 双帧形态 / 上游重复通知）。
	for _, dup := range []string{
		errFrame,
		`{"type":"response.failed","response":{"status":"failed","error":{"code":"server_error","message":"boom"}}}`,
	} {
		evs, err := p.Feed([]byte(dup))
		if err != nil || evs != nil {
			t.Fatalf("dup error-family frame %s: %+v, %v", dup, evs, err)
		}
	}
	// 内容帧：仍是协议违规。
	if _, err := p.Feed([]byte(`{"type":"response.output_text.delta","output_index":0,"delta":"late"}`)); !errors.Is(err, ErrEventsAfterTerminal) {
		t.Fatalf("content frame after error: %v, want ErrEventsAfterTerminal", err)
	}
}
