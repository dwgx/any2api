// W5-26 条目 1③ 集成面：跨协议出口腿的 id 净化 + 上限（同协议直通不介入——
// 架构决策 1：直通不经 IR）。反证锚点：把 hygiene.WireToolID 的净化短路 →
// 本测试红（见 tmp/lanes/L19-sweep9/proofs/）。
package service

import (
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/hygiene"
)

// TestSweep9ToolIDSanitizedAtExit claude 上游给出「非法字符 + 超长」的
// tool_use.id → openai 出口必须净化（字符集 ^[a-zA-Z0-9_-]+$ + ≤64 + 哈希后缀），
// 且前缀保留（可读/可关联）。
func TestSweep9ToolIDSanitizedAtExit(t *testing.T) {
	bad := "call_bad.chars/" + strings.Repeat("x", 70) // 71 字符 + 非法 '.' '/'
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_h1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}`)
		sseFrame(w, "content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"`+bad+`","name":"f","input":{}}}`)
		sseFrame(w, "content_block_stop", `{"type":"content_block_stop","index":0}`)
		sseFrame(w, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"input_tokens":1,"output_tokens":1}}`)
		sseFrame(w, "message_stop", `{"type":"message_stop"}`)
	})
	srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	id := ""
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "data: ") || strings.Contains(line, "[DONE]") {
			continue
		}
		var doc struct {
			Choices []struct {
				Delta struct {
					ToolCalls []struct {
						ID string `json:"id"`
					} `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &doc) != nil {
			continue
		}
		for _, ch := range doc.Choices {
			for _, tc := range ch.Delta.ToolCalls {
				if tc.ID != "" {
					id = tc.ID
				}
			}
		}
	}
	if id == "" {
		t.Fatalf("下游工具 id 缺失:\n%s", raw)
	}
	if id == bad {
		t.Fatalf("非法/超长 id 原样透传（未净化）: %q", id)
	}
	if len(id) > hygiene.MaxToolIDLen {
		t.Fatalf("长度 %d > %d: %q", len(id), hygiene.MaxToolIDLen, id)
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]+$`).MatchString(id) {
		t.Fatalf("字符集违规: %q", id)
	}
	if !strings.HasPrefix(id, "call_bad_chars_") {
		t.Fatalf("前缀未保留（关联性丢失）: %q", id)
	}
}
