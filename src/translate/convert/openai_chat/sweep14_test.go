// W5-31 条目 3：openai_chat 出口响应 id 合成——上游不给 id → `r_synth_<32hex>`
// （每响应唯一），并作为合成工具 id 的派生 seed（响应内确定 + 跨响应唯一）。
// 旧行为：id 空串进 wire、工具 id 回落块序形态 `call_synth_<n>`（跨响应重复，
// 客户端 transcript/日志无法关联）。
//
// 依据（lane L19-sweep14）：修前探针报 `id = ” (want r_synth_<32hex>)` 与
// 工具 id 块序形态；修后 0 missing。
// 反证锚点：删掉 RenderResponse / StreamWriter 的 id 合成 → 本文件红。
package openai_chat

import (
	"encoding/json"
	"regexp"
	"testing"

	"github.com/dwgx/any2api/translate/hygiene"
)

var sweep14SynthID = regexp.MustCompile(`^r_synth_[0-9a-f]{32}$`)

// sweep14ChatWire 非流式 chat.completion 的 id 与首个工具调用 id。
func sweep14ChatWire(t *testing.T, out []byte) (string, string) {
	t.Helper()
	var w struct {
		ID      string `json:"id"`
		Choices []struct {
			Message struct {
				ToolCalls []struct {
					ID string `json:"id"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out)
	}
	if len(w.Choices) != 1 || len(w.Choices[0].Message.ToolCalls) != 1 {
		t.Fatalf("choices/tool_calls 形状 = %s", out)
	}
	return w.ID, w.Choices[0].Message.ToolCalls[0].ID
}

// sweep14ToolIDFromFrames 下游帧里首个工具调用 id（首片携带）。
func sweep14ToolIDFromFrames(t *testing.T, docs []map[string]json.RawMessage) string {
	t.Helper()
	for _, doc := range docs {
		var w struct {
			Choices []struct {
				Delta struct {
					ToolCalls []struct {
						ID string `json:"id"`
					} `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(mustRaw(t, doc)), &w); err != nil {
			t.Fatalf("unmarshal frame: %v", err)
		}
		for _, ch := range w.Choices {
			for _, tc := range ch.Delta.ToolCalls {
				if tc.ID != "" {
					return tc.ID
				}
			}
		}
	}
	return ""
}

// mustRaw 把帧对象重新序列化（测试内比较用）。
func mustRaw(t *testing.T, doc map[string]json.RawMessage) string {
	t.Helper()
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	return string(b)
}

const sweep14NoIDBody = `{"object":"chat.completion","created":1741293000,"model":"m",` +
	`"choices":[{"index":0,"message":{"role":"assistant","content":null,` +
	`"tool_calls":[{"type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`

// TestSweep14ResponseIDSynthNonStream 上游无 id（响应 + 工具调用）→ 合成
// `r_synth_<32hex>`（每响应唯一）+ 工具 id = call_synth_<sha256(id␀0)[:16]hex>。
func TestSweep14ResponseIDSynthNonStream(t *testing.T) {
	resp, err := ParseResponse([]byte(sweep14NoIDBody))
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if resp.ID != "" {
		t.Fatalf("前提：上游无 id（resp.ID = %q）", resp.ID)
	}
	out, err := RenderResponse(resp, RenderOptions{})
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	id, toolID := sweep14ChatWire(t, out)
	if !sweep14SynthID.MatchString(id) {
		t.Fatalf("id = %q（want r_synth_<32hex>）", id)
	}
	if want := hygiene.SynthDerivedID("call", id, 0); toolID != want {
		t.Fatalf("tool id = %q, want %q（响应 id 派生）", toolID, want)
	}

	// 每响应唯一：同 IR 两次渲染不得同 id（旧行为：id 空 + 工具 id 块序形态）。
	resp2, err := ParseResponse([]byte(sweep14NoIDBody))
	if err != nil {
		t.Fatalf("ParseResponse(2): %v", err)
	}
	out2, err := RenderResponse(resp2, RenderOptions{})
	if err != nil {
		t.Fatalf("RenderResponse(2): %v", err)
	}
	id2, _ := sweep14ChatWire(t, out2)
	if !sweep14SynthID.MatchString(id2) || id2 == id {
		t.Fatalf("id2 = %q（want 另一次 r_synth；旧值 %q）", id2, id)
	}

	// 渲染不动点：parse(render(x)) 再渲染语义相等（合成 id 经 wire 往返稳定）。
	resp3, err := ParseResponse(out)
	if err != nil {
		t.Fatalf("re-ParseResponse: %v\n%s", err, out)
	}
	out3, err := RenderResponse(resp3, RenderOptions{})
	if err != nil {
		t.Fatalf("re-RenderResponse: %v", err)
	}
	assertJSONEqual(t, out, out3)
}

// TestSweep14ResponseIDSynthStream 流式面：上游无 id → 帧 id 合成（全程一致）+
// 工具 id 派生；上游有 id → 原样透传（回归）。
func TestSweep14ResponseIDSynthStream(t *testing.T) {
	noID := []string{
		`{"object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant",` +
			`"tool_calls":[{"index":0,"type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":null}]}`,
		`{"object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		"[DONE]",
	}
	_, out := renderPayloads(t, noID)
	docs := frameDocs(t, out)
	if len(docs) == 0 {
		t.Fatalf("无数据帧:\n%s", out)
	}
	var id string
	for i, doc := range docs {
		var got string
		if err := json.Unmarshal(doc["id"], &got); err != nil {
			t.Fatalf("frame %d id: %v", i, err)
		}
		if i == 0 {
			id = got
		}
		if got != id {
			t.Fatalf("frame %d id = %q, want %q（同一响应全程一致）", i, got, id)
		}
	}
	if !sweep14SynthID.MatchString(id) {
		t.Fatalf("帧 id = %q（want r_synth_<32hex>）", id)
	}
	if want := hygiene.SynthDerivedID("call", id, 0); sweep14ToolIDFromFrames(t, docs) != want {
		t.Fatalf("tool id = %q, want %q（响应 id 派生）", sweep14ToolIDFromFrames(t, docs), want)
	}

	// 上游有 id → 透传（不合成、不改写）。
	withID := []string{
		`{"id":"chatcmpl-s14","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant",` +
			`"tool_calls":[{"index":0,"id":"call_x","type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":null}]}`,
		`{"id":"chatcmpl-s14","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		"[DONE]",
	}
	_, out2 := renderPayloads(t, withID)
	docs2 := frameDocs(t, out2)
	for i, doc := range docs2 {
		var got string
		if err := json.Unmarshal(doc["id"], &got); err != nil {
			t.Fatalf("frame %d id: %v", i, err)
		}
		if got != "chatcmpl-s14" {
			t.Fatalf("frame %d id = %q（上游 id 必须透传）", i, got)
		}
	}
	if got := sweep14ToolIDFromFrames(t, docs2); got != "call_x" {
		t.Fatalf("tool id = %q（上游 id 必须透传）", got)
	}
}
