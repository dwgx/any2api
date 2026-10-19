// W5-31 条目 1/2：responses 出口两处 SDK/spec 必填收口——
//
//	① 批量 output[] 项 id：`msg_/rs_/fc_` 前缀 + 响应 id 派生 + 项序
//	   （hygiene.SynthDerivedID，与流式面同源同形态）。message/reasoning 项的
//	   id 是 spec/SDK 必填——缺 id 的批量响应过不了 openai-python 的 output[]
//	   联合校验（W5-30 探针暴露的残余）。
//	② usage details 恒写：input_tokens_details{cached_tokens,cache_write_tokens} /
//	   output_tokens_details{reasoning_tokens}（spec ResponseUsage.required，SDK
//	   同必填）。原文完整 → 逐字节透传；缺键/缺失 → 合成（原文值 → 派生值 → 0）。
//
// 依据（lane L19-sweep14，openai-python 3.29.0）：修前探针报
// output.0 (reasoning)/output.1 (message) missing=['id']、usage missing 两
// details；修后 0 missing（proofs/{prefix,postfix}）。
// 反证锚点：删掉 renderOutputItems 的 id 写入 / renderUsage 的 details 恒写
// → 本文件红。
package responses

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/hygiene"
	"github.com/dwgx/any2api/translate/ir"
)

// sweep14Output 解析 Response.output。
func sweep14Output(t *testing.T, out []byte) []map[string]json.RawMessage {
	t.Helper()
	var doc struct {
		Output []map[string]json.RawMessage `json:"output"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("unmarshal output: %v\n%s", err, out)
	}
	return doc.Output
}

// sweep14Usage 解析 Response.usage。
func sweep14Usage(t *testing.T, out []byte) map[string]json.RawMessage {
	t.Helper()
	var doc struct {
		Usage map[string]json.RawMessage `json:"usage"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("unmarshal usage: %v\n%s", err, out)
	}
	return doc.Usage
}

// sweep14String 取 JSON 字符串值。
func sweep14String(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("unmarshal string %s: %v", raw, err)
	}
	return s
}

// sweep14Resp 三项输出（reasoning / message / function_call）+ 可选 usage。
func sweep14Resp(id string, usage *ir.Usage) *ir.Response {
	return &ir.Response{
		ID: id, Model: "gpt-5", Created: 1741293000,
		StopReason: ir.StopToolUse,
		Message: ir.Message{Role: ir.RoleAssistant, Blocks: []ir.ContentBlock{
			{Kind: ir.BlockThinking, Text: "ponder", Signature: "sig_a"},
			{Kind: ir.BlockText, Text: "Hi"},
			{Kind: ir.BlockToolUse, ToolCall: &ir.ToolCall{ID: "call_a", Name: "f", Arguments: `{"a":1}`}},
		}},
		Usage: usage,
	}
}

// TestSweep14BatchOutputItemIDs ①：批量项 id 三形态 + 跨响应唯一 + 渲染不动点。
func TestSweep14BatchOutputItemIDs(t *testing.T) {
	out, err := RenderResponse(sweep14Resp("resp_s14", nil))
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	items := sweep14Output(t, out)
	if len(items) != 3 {
		t.Fatalf("items = %d, want 3", len(items))
	}
	want := []struct{ typ, id, prefix string }{
		{"reasoning", hygiene.SynthDerivedID("rs", "resp_s14", 0), "rs_synth_"},
		{"message", hygiene.SynthDerivedID("msg", "resp_s14", 1), "msg_synth_"},
		{"function_call", hygiene.SynthDerivedID("fc", "resp_s14", 2), "fc_synth_"},
	}
	for i, w := range want {
		if got := sweep14String(t, items[i]["type"]); got != w.typ {
			t.Fatalf("items[%d].type = %q, want %q", i, got, w.typ)
		}
		id := sweep14String(t, items[i]["id"])
		if id != w.id {
			t.Fatalf("items[%d].id = %q, want %q（响应 id 派生 + 项序）", i, id, w.id)
		}
		if !strings.HasPrefix(id, w.prefix) || len(id) != len(w.prefix)+32 {
			t.Fatalf("items[%d].id 形态 = %q（want %s<32hex>）", i, id, w.prefix)
		}
	}
	// 项 id 互异（同响应内不得撞——Continue 依赖前缀，客户端按 id 重建 items）。
	if id0, id1, id2 := sweep14String(t, items[0]["id"]), sweep14String(t, items[1]["id"]), sweep14String(t, items[2]["id"]); id0 == id1 || id1 == id2 || id0 == id2 {
		t.Fatalf("项 id 撞车: %s %s %s", id0, id1, id2)
	}

	// 跨响应唯一：换响应 id → 项 id 全变（客户端历史/日志关联）。
	out2, err := RenderResponse(sweep14Resp("resp_other", nil))
	if err != nil {
		t.Fatalf("RenderResponse(other): %v", err)
	}
	items2 := sweep14Output(t, out2)
	for i := range items {
		if sweep14String(t, items[i]["id"]) == sweep14String(t, items2[i]["id"]) {
			t.Fatalf("items[%d] 跨响应撞 id: %s", i, items[i]["id"])
		}
	}

	// 渲染不动点：parse(render(x)) 再渲染语义相等（项 id 经响应 id 往返）。
	resp2, err := ParseResponse(out)
	if err != nil {
		t.Fatalf("re-ParseResponse: %v\n%s", err, out)
	}
	out3, err := RenderResponse(resp2)
	if err != nil {
		t.Fatalf("re-RenderResponse: %v", err)
	}
	assertJSONEqual(t, out, out3)
}

// TestSweep14UsageDetailsSynthesized ②：IR 无 details → 合成完整对象
// （原文值 → 派生值 → 0；必填两键恒在）。
func TestSweep14UsageDetailsSynthesized(t *testing.T) {
	// 无 cache/reasoning 证据 → 全零（「无证据」的诚实表述）。
	out, err := RenderResponse(sweep14Resp("resp_u1", &ir.Usage{
		Input: 7, Output: 3, Total: 10, Accounting: "openai",
	}))
	if err != nil {
		t.Fatalf("RenderResponse: %v", err)
	}
	u := sweep14Usage(t, out)
	assertJSONEqual(t, []byte(`{"cached_tokens":0,"cache_write_tokens":0}`), u["input_tokens_details"])
	assertJSONEqual(t, []byte(`{"reasoning_tokens":0}`), u["output_tokens_details"])

	// 边界（有意钉住）：合成 details 会被 reparse 读回 Extra——原 IR 无 details 的
	// 场景经一次 render 后 IR 增补（单向；渲染不动点不受影响——二次渲染走「原文
	// 完整 → 逐字节透传」路径）。改动该边界需显式改本断言。
	resp2, err := ParseResponse(out)
	if err != nil {
		t.Fatalf("re-ParseResponse: %v\n%s", err, out)
	}
	if resp2.Usage == nil || len(resp2.Usage.Extra["prompt_tokens_details"]) == 0 {
		t.Fatalf("合成 details 应经 reparse 读回 Extra: %+v", resp2.Usage)
	}

	// 派生值非零 → 用派生值（CacheRead → cached_tokens；Reasoning → reasoning_tokens）。
	out, err = RenderResponse(sweep14Resp("resp_u2", &ir.Usage{
		Input: 7, Output: 3, Total: 10, CacheRead: 4, Reasoning: 2, Accounting: "openai",
	}))
	if err != nil {
		t.Fatalf("RenderResponse(derived): %v", err)
	}
	u = sweep14Usage(t, out)
	assertJSONEqual(t, []byte(`{"cached_tokens":4,"cache_write_tokens":0}`), u["input_tokens_details"])
	assertJSONEqual(t, []byte(`{"reasoning_tokens":2}`), u["output_tokens_details"])
}

// TestSweep14UsageDetailsPassthrough ②：原文完整 → 逐字节透传（保真）；原文
// 缺键（chat 形状）→ 合成完整对象，chat 风味额外键不落入（跨 schema 污染）。
func TestSweep14UsageDetailsPassthrough(t *testing.T) {
	// 原文完整（含额外键）→ 逐字节透传（渲染不动点/录帧回放靠它）。
	u := &ir.Usage{Input: 7, Output: 3, Total: 10, Accounting: "openai",
		Extra: map[string]json.RawMessage{
			"prompt_tokens_details":     json.RawMessage(`{"cached_tokens":4,"cache_write_tokens":9,"audio_tokens":1}`),
			"completion_tokens_details": json.RawMessage(`{"reasoning_tokens":2}`),
		}}
	out, err := RenderResponse(sweep14Resp("resp_u3", u))
	if err != nil {
		t.Fatalf("RenderResponse(passthrough): %v", err)
	}
	got := sweep14Usage(t, out)
	if string(got["input_tokens_details"]) != `{"cached_tokens":4,"cache_write_tokens":9,"audio_tokens":1}` {
		t.Fatalf("完整原文必须逐字节透传: %s", got["input_tokens_details"])
	}
	if string(got["output_tokens_details"]) != `{"reasoning_tokens":2}` {
		t.Fatalf("output details 原文必须逐字节透传: %s", got["output_tokens_details"])
	}

	// 原文缺 cache_write_tokens（chat 形状：cached_tokens + audio_tokens）→
	// 合成完整对象：cached_tokens 用原文值、cache_write_tokens 补 0、
	// audio_tokens 不落入（responses 细节面只定义必填两键）。
	u = &ir.Usage{Input: 7, Output: 3, Total: 10, Accounting: "openai",
		Extra: map[string]json.RawMessage{
			"prompt_tokens_details": json.RawMessage(`{"cached_tokens":4,"audio_tokens":1}`),
		}}
	out, err = RenderResponse(sweep14Resp("resp_u4", u))
	if err != nil {
		t.Fatalf("RenderResponse(incomplete): %v", err)
	}
	got = sweep14Usage(t, out)
	assertJSONEqual(t, []byte(`{"cached_tokens":4,"cache_write_tokens":0}`), got["input_tokens_details"])
	assertJSONEqual(t, []byte(`{"reasoning_tokens":0}`), got["output_tokens_details"])
}

// TestSweep14UsageDetailsStreamCompleted ②（流式面）：response.completed 快照的
// usage 同样恒写 details（ResponseCompletedEvent 的 SDK 必填面）。
func TestSweep14UsageDetailsStreamCompleted(t *testing.T) {
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "resp_u9", Model: "m", Created: 1},
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockText},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockText, TextDelta: "hi"},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockText},
		{Kind: ir.EventMessageDelta, StopReason: ir.StopStop,
			Usage: &ir.Usage{Input: 3, Output: 1, Total: 4}},
		{Kind: ir.EventMessageStop},
		{Kind: ir.EventDone, Reason: ir.StopStop},
	}
	out, err := RenderStream(events)
	if err != nil {
		t.Fatalf("RenderStream: %v", err)
	}
	found := false
	for _, raw := range sseFrames(t, out) {
		var w struct {
			Type     string `json:"type"`
			Response struct {
				Usage map[string]json.RawMessage `json:"usage"`
			} `json:"response"`
		}
		if err := json.Unmarshal([]byte(raw), &w); err != nil || w.Type != "response.completed" {
			continue
		}
		found = true
		assertJSONEqual(t, []byte(`{"cached_tokens":0,"cache_write_tokens":0}`), w.Response.Usage["input_tokens_details"])
		assertJSONEqual(t, []byte(`{"reasoning_tokens":0}`), w.Response.Usage["output_tokens_details"])
	}
	if !found {
		t.Fatalf("response.completed 帧缺失:\n%s", out)
	}
}
