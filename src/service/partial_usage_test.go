// W5-15 e2e：预提交 partial usage 回收（P27 §发现 6 的移植面，落在诊断）。
//
// 装置：httptest 假上游（本地回环）+ 真服务 + 真 HTTP 客户端。
// 断言面：下游字节（不合并——单次尝试语义）、各上游命中数、Server.Diagnostics()
// 的 PartialUsage* 计数。
package service

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// 单元：帧级 usage 观测（五协议）
// ---------------------------------------------------------------------------

func TestObservePartialUsage(t *testing.T) {
	cases := []struct {
		name  string
		proto registry.Format
		data  string
		want  partialUsage
	}{
		{"claude_message_start", registry.FormatClaudeMessages,
			`{"type":"message_start","message":{"usage":{"input_tokens":11,"cache_read_input_tokens":3,"output_tokens":0}}}`,
			partialUsage{input: 11}},
		{"claude_message_delta", registry.FormatClaudeMessages,
			`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":11,"output_tokens":2}}`,
			partialUsage{input: 11, output: 2}},
		{"openai_chunk_usage", registry.FormatOpenAIChat,
			`{"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`,
			partialUsage{input: 7, output: 3}},
		{"openai_short_names_read_only", registry.FormatOpenAIChat,
			`{"choices":[],"usage":{"input_tokens":5,"output_tokens":2}}`,
			partialUsage{input: 5, output: 2}},
		{"gemini_usage_metadata", registry.FormatGeminiGenerate,
			`{"usageMetadata":{"promptTokenCount":9,"candidatesTokenCount":4,"totalTokenCount":13}}`,
			partialUsage{input: 9, output: 4}},
		{"responses_completed", registry.FormatOpenAIResponses,
			`{"type":"response.completed","response":{"usage":{"input_tokens":9,"output_tokens":4}}}`,
			partialUsage{input: 9, output: 4}},
		{"interactions_completed", registry.FormatInteractions,
			`{"interaction":{"status":"completed","usage":{"total_input_tokens":9,"total_output_tokens":4}},"event_type":"interaction.completed"}`,
			partialUsage{input: 9, output: 4}},
		{"interactions_step_stop_usage", registry.FormatInteractions,
			`{"index":0,"step_usage":{"total_input_tokens":3,"total_output_tokens":1},"event_type":"step.stop"}`,
			partialUsage{input: 3, output: 1}},
		{"no_usage_fields", registry.FormatClaudeMessages,
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x"}}`,
			partialUsage{}},
		{"non_json", registry.FormatGeminiGenerate, `not json at all`, partialUsage{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var acc partialUsage
			observePartialUsage(tc.proto, []byte(tc.data), &acc)
			if acc != tc.want {
				t.Fatalf("observePartialUsage(%s) = %+v, want %+v", tc.proto, acc, tc.want)
			}
		})
	}

	// max 合并语义：多次观测取较大值（不累加——各协议 usage 单调/快照）。
	var acc partialUsage
	observePartialUsage(registry.FormatGeminiGenerate, []byte(`{"usageMetadata":{"promptTokenCount":5}}`), &acc)
	observePartialUsage(registry.FormatGeminiGenerate, []byte(`{"usageMetadata":{"promptTokenCount":9,"candidatesTokenCount":2}}`), &acc)
	if acc != (partialUsage{input: 9, output: 2}) {
		t.Fatalf("merge = %+v, want {9 2}", acc)
	}

	// 巨型帧守卫：超过 maxProbeBytes 的帧不做二次解析（即使含 usage 字段）。
	huge := `{"usage":{"prompt_tokens":9},"pad":"` + strings.Repeat("x", maxProbeBytes) + `"}`
	var acc2 partialUsage
	observePartialUsage(registry.FormatOpenAIChat, []byte(huge), &acc2)
	if acc2 != (partialUsage{}) {
		t.Fatalf("巨型帧必须跳过探针 = %+v, want zero", acc2)
	}
}

// ---------------------------------------------------------------------------
// e2e P1：预提交失败前已收 usage → 回收进诊断；下游可见流保持单次尝试语义
// ---------------------------------------------------------------------------

func TestE2EPreCommitPartialUsageRecovered(t *testing.T) {
	// 首台上游（claude）：首帧就是携带 usage 的 message_delta（缺 message_start）
	// ——claude 解析器以 ErrUnexpectedOrder 拒绝该帧 → 预提交失败；探针先于 Feed
	// 运行，故该帧的 usage 已被观测（in=11）。窗口的构造性说明见报告：正常流在
	// 首个解析帧即提交（每个解析器在首个真实帧产 EventStart），预提交窗口只含
	// 不产出的帧（ping/被拒帧）。
	bad := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":11,"output_tokens":0}}`)
	})
	// 第二台上游（claude）：成功流，usage 与首台刻意不同（5/2）——用于锁「不合并」。
	good := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_g1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":5,"output_tokens":0}}}`)
		sseFrame(w, "content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
		sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello "}}`)
		sseFrame(w, "content_block_stop", `{"type":"content_block_stop","index":0}`)
		sseFrame(w, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":5,"output_tokens":2}}`)
		sseFrame(w, "message_stop", `{"type":"message_stop"}`)
	})
	s, srv := startServiceWith(t, failoverCfg(
		anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"claude-x","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(raw), "Hello ") {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}
	// 换上游一次：bad 1 次、good 1 次。
	if bad.count() != 1 || good.count() != 1 {
		t.Fatalf("hits: bad=%d good=%d（期望 1/1）", bad.count(), good.count())
	}
	// 不合并：下游 usage = 成功尝试的 5/2（若并入失败尝试的 11，prompt 会是 16）。
	body := string(raw)
	if !strings.Contains(body, `"prompt_tokens":5`) || !strings.Contains(body, `"completion_tokens":2`) {
		t.Fatalf("下游 usage 必须只来自成功尝试（5/2）:\n%s", body)
	}
	if strings.Contains(body, `"prompt_tokens":16`) {
		t.Fatalf("失败尝试 usage 不得并入下游可见流:\n%s", body)
	}
	// 诊断面：已收 usage 回收（in=11 out=0）。
	d := s.Diagnostics()
	if d.Attempts != 2 || d.Failovers != 1 {
		t.Fatalf("diagnostics = %+v", d)
	}
	if d.PartialUsageAttempts != 1 || d.PartialUsageInTokens != 11 || d.PartialUsageOutTokens != 0 {
		t.Fatalf("partial usage diagnostics = %+v（want attempts=1 in=11 out=0）", d)
	}
}

// ---------------------------------------------------------------------------
// e2e P2：提交点边界——usage 在首帧（claude message_start）时首帧即提交，
// 已收 usage 不再回收（提交后不换上游，走流内错误帧收尾）
// ---------------------------------------------------------------------------

func TestE2EPreCommitWindowClosesAtFirstOutput(t *testing.T) {
	bad := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		// claude message_start：claude→openai 直转在 EventStart 即产 role chunk
		// （非空产出 → 提交），随后断流。
		sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_b1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":11,"output_tokens":0}}}`)
		panic(http.ErrAbortHandler)
	})
	good := goodClaudeUpstream(t)
	s, srv := startServiceWith(t, failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL)))

	code, body := postChatStream(t, srv.URL)
	if code != 200 {
		t.Fatalf("status = %d（已提交后错误必须 200 + 流内错误帧）", code)
	}
	if !strings.Contains(body, `"error"`) || strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("流内错误帧缺失或错误后补了 [DONE]（W5-26 裁决：错误即终态）:\n%s", body)
	}
	if good.count() != 0 {
		t.Fatalf("已提交不得换上游（good hits=%d）", good.count())
	}
	d := s.Diagnostics()
	if d.Failovers != 0 || d.PartialUsageAttempts != 0 {
		t.Fatalf("diagnostics = %+v（提交后失败不回收 partial usage）", d)
	}
}
