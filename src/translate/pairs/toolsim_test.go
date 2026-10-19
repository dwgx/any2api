package pairs

// W17 S2 接线验收（设计单 §S2）：claude_web 上游文本流 → 出口腿 toolsim 过滤器
//（位于 toolExitStage **之前**）→ 下游 claude_messages / openai SSE。
//
// 验收四条（设计单 §S2 原文）：① 标签跨片切割合成；② 非契约文本零影响（逐字节
// 等于现状）；③ 坏 JSON → 原样文本降级 + 不发坏块；④ 合成的 tool_use 参数过
// toolExitStage 的 `{}`/修复纪律。
//
// 反证锚点：删掉 streamWriter.writeEvents 的 sim 分支 → TestToolSimPipeSynthesizesCall
// 红；删掉 Filter 的 StopReason 改写 → 同用例红；把 sim 挪到 stage 之后（或删掉
// 空参 outlet 纪律）→ TestToolSimPipeArgsGoThroughToolExitStage 红。

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/convert/claude_messages"
	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/registry"
)

// claudeWebTextStream claude_web 上游流构造：单 text 块按 deltas 切片 + 收尾。
func claudeWebTextStream(deltas []string, stopReason string) []byte {
	var b strings.Builder
	frame := func(event, data string) {
		b.WriteString("event: " + event + "\n")
		b.WriteString("data: " + data + "\n\n")
	}
	frame("message_start", `{"type":"message_start","message":{"id":"msg_sim1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":4,"output_tokens":0}}}`)
	frame("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
	for _, d := range deltas {
		frame("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":`+jsonString(d)+`}}`)
	}
	frame("content_block_stop", `{"type":"content_block_stop","index":0}`)
	frame("message_delta", `{"type":"message_delta","delta":{"stop_reason":"`+stopReason+`","stop_sequence":null},"usage":{"input_tokens":4,"output_tokens":9}}`)
	frame("message_stop", `{"type":"message_stop"}`)
	return []byte(b.String())
}

// downstreamText 下游 claude 流 → 全部 text_delta 拼接（降级/存活文本断言用）。
func downstreamText(t *testing.T, raw []byte) string {
	t.Helper()
	evs, err := claude_messages.ParseStream(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("下游解析: %v", err)
	}
	var b strings.Builder
	for _, ev := range evs {
		if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockText {
			b.WriteString(ev.TextDelta)
		}
	}
	return b.String()
}

// toolSimPipe claude_web 上游 → to 出口的增量管道。
func toolSimPipe(t *testing.T, to registry.Format, opts Options) *StreamPipe {
	t.Helper()
	pipe, err := NewStreamPipe(NewRegistry(), FormatClaudeWeb, to, opts)
	if err != nil {
		t.Fatalf("NewStreamPipe: %v", err)
	}
	return pipe
}

// TestToolSimPipeSynthesizesCall ① 标签跨片切割（`<tool` + `_use …`）正确合成：
// 下游 SSE 含 tool_use 块（start + input_json_delta + stop）+ `stop_reason:"tool_use"`，
// 标签前后文本存活、块序串行。
func TestToolSimPipeSynthesizesCall(t *testing.T) {
	in := claudeWebTextStream([]string{
		"let me check ",
		"<tool",
		`_use name="Read">{"file_path":"a.txt"}`,
		"</tool_use>",
		" done",
	}, "end_turn")
	pipe := toolSimPipe(t, fClaude, Options{ToolSim: true})
	out := runStreamPipe(t, pipe, in)
	s := string(out)

	// 下游帧面：text → tool_use → text 三块，index 连续 0/1/2。
	frames := sseFramesOf(t, out)
	var starts []string
	for _, f := range frames {
		if f.event != "content_block_start" {
			continue
		}
		var p struct {
			Index        int `json:"index"`
			ContentBlock struct {
				Type  string `json:"type"`
				ID    string `json:"id"`
				Name  string `json:"name"`
				Input struct {
					FilePath string `json:"file_path"`
				} `json:"input"`
			} `json:"content_block"`
		}
		if err := json.Unmarshal([]byte(f.data), &p); err != nil {
			t.Fatalf("frame: %v\n%s", err, f.data)
		}
		if p.Index != len(starts) {
			t.Fatalf("块 index 不连续: %v", starts)
		}
		starts = append(starts, p.ContentBlock.Type)
		if p.ContentBlock.Type == "tool_use" {
			if p.ContentBlock.Name != "Read" || p.ContentBlock.Input.FilePath != "" {
				t.Fatalf("tool_use 块 start 形态: %s", f.data)
			}
			if p.ContentBlock.ID == "" {
				t.Fatalf("合成块 id 缺失（writer 应合成 toolu_…）: %s", f.data)
			}
		}
	}
	if len(starts) != 3 || starts[0] != "text" || starts[1] != "tool_use" || starts[2] != "text" {
		t.Fatalf("块序 = %v, want [text tool_use text]", starts)
	}
	if !strings.Contains(s, `"stop_reason":"tool_use"`) {
		t.Fatalf("stop_reason 未改写 tool_use:\n%.600s", s)
	}
	if !strings.Contains(s, "let me check ") || !strings.Contains(s, " done") {
		t.Fatalf("标签前后文本丢失:\n%.600s", s)
	}
	// 解析回 IR 复核装配面（名字/参数/id）。
	events, err := claude_messages.ParseStream(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("下游解析: %v", err)
	}
	calls, err := ir.AssembleToolCalls(events)
	if err != nil {
		t.Fatalf("装配: %v", err)
	}
	if len(calls) != 1 || calls[0].Name != "Read" || calls[0].Arguments != `{"file_path":"a.txt"}` {
		t.Fatalf("装配调用 = %+v", calls)
	}
	if d := pipe.Diagnostics(); d.ToolSimSynthesized != 1 {
		t.Fatalf("ToolSimSynthesized = %d, want 1", d.ToolSimSynthesized)
	}
}

// TestToolSimPipeNonContractTextBytesEqual ② 非契约文本零影响：同一上游流在
// ToolSim 开/关下下游字节**逐字节相等**（含 stop_reason 不改写）。
func TestToolSimPipeNonContractTextBytesEqual(t *testing.T) {
	in := claudeWebTextStream([]string{"plain ", "answer ", "text"}, "end_turn")
	off := runStreamPipe(t, toolSimPipe(t, fClaude, Options{}), in)
	on := runStreamPipe(t, toolSimPipe(t, fClaude, Options{ToolSim: true}), in)
	if !bytes.Equal(off, on) {
		t.Fatalf("非契约文本被改动:\n off %q\n on %q", off, on)
	}
	if got := downstreamText(t, on); got != "plain answer text" {
		t.Fatalf("文本丢失: %q", got)
	}
}

// TestToolSimPipeBadJSONDegradesToText ③ 坏 JSON → 原样文本降级：不合成块、不发
// 坏参数、stop_reason 保持上游值（绝不伪造成空参调用）。
func TestToolSimPipeBadJSONDegradesToText(t *testing.T) {
	in := claudeWebTextStream([]string{`oops <tool_use name="Read">{not json</tool_use> tail`}, "end_turn")
	pipe := toolSimPipe(t, fClaude, Options{ToolSim: true})
	out := runStreamPipe(t, pipe, in)
	s := string(out)

	if strings.Contains(s, `"tool_use"`) {
		t.Fatalf("坏 JSON 不得合成块:\n%.600s", s)
	}
	if strings.Contains(s, "input_json_delta") {
		t.Fatalf("不得下发坏参数:\n%.600s", s)
	}
	if !strings.Contains(s, `"stop_reason":"end_turn"`) {
		t.Fatalf("stop_reason 不得改写:\n%.400s", s)
	}
	if got := downstreamText(t, out); got != `oops <tool_use name="Read">{not json</tool_use> tail` {
		t.Fatalf("降级文本必须原样保留: %q", got)
	}
	if d := pipe.Diagnostics(); d.ToolSimSynthesized != 0 {
		t.Fatalf("ToolSimSynthesized = %d, want 0", d.ToolSimSynthesized)
	}
}

// TestToolSimPipeArgsGoThroughToolExitStage ④ 合成参数过 toolExitStage 纪律：
//   - 空体调用 → 出口 `{}` 纪律（openai 出口显式 `"arguments":"{}"`；不经修复层
//     会留下 `""` 空串——Cline/Roo 静默丢参门）；Anthropic 出口空参不发 delta；
//   - 合法参数 → 合法直过字节保真（既有修复层语义）。
func TestToolSimPipeArgsGoThroughToolExitStage(t *testing.T) {
	empty := claudeWebTextStream([]string{`<tool_use name="Read"></tool_use>`}, "end_turn")

	// openai 出口：空参显式 `{}`。
	oai := runStreamPipe(t, toolSimPipe(t, fOpenAI, Options{ToolSim: true}), empty)
	if !strings.Contains(string(oai), `"arguments":"{}"`) {
		t.Fatalf("openai 出口空参须显式 {}:\n%.600s", oai)
	}
	if strings.Contains(string(oai), `"arguments":""`) {
		t.Fatalf("空串参数门（Cline/Roo 会静默丢弃）:\n%.600s", oai)
	}

	// Anthropic 出口：空参不额外发 delta（start 的 input `{}` 即最终值）。
	claude := runStreamPipe(t, toolSimPipe(t, fClaude, Options{ToolSim: true}), empty)
	if strings.Contains(string(claude), "input_json_delta") {
		t.Fatalf("Anthropic 出口空参不应发 delta:\n%.600s", claude)
	}
	if !strings.Contains(string(claude), `"input":{}`) {
		t.Fatalf("tool_use 块 start 须带 input {}:\n%.600s", claude)
	}
	evs, err := claude_messages.ParseStream(bytes.NewReader(claude))
	if err != nil {
		t.Fatalf("下游解析: %v", err)
	}
	calls, err := ir.AssembleToolCalls(evs)
	if err != nil {
		t.Fatalf("装配: %v", err)
	}
	// 装配侧 Arguments 为空串 = 无 args delta（wire 的 input 对象 `{}` 即最终值；
	// 空串在 IR 里就是「无参」语义，各出口渲染器按契约落 `{}`）。
	if len(calls) != 1 || calls[0].Name != "Read" || calls[0].Arguments != "" {
		t.Fatalf("空参装配 = %+v, want 无参单调用", calls)
	}

	// 合法参数：字节保真直过（`{"a": 1, "b": "x"}` 原样，未被 canonical 改写）。
	legal := claudeWebTextStream([]string{`<tool_use name="Write">{"a": 1, "b": "x"}</tool_use>`}, "end_turn")
	lc := runStreamPipe(t, toolSimPipe(t, fClaude, Options{ToolSim: true}), legal)
	evs2, err := claude_messages.ParseStream(bytes.NewReader(lc))
	if err != nil {
		t.Fatalf("下游解析: %v", err)
	}
	calls2, err := ir.AssembleToolCalls(evs2)
	if err != nil {
		t.Fatalf("装配: %v", err)
	}
	if len(calls2) != 1 || calls2[0].Arguments != `{"a": 1, "b": "x"}` {
		t.Fatalf("合法参数被改写: %+v", calls2)
	}
}

// TestToolSimPipeToolResultDegrades `<tool_result>`（模型自造结果）一律文本降级——
// 绝不合成块（幻觉结果污染会话的防线）。
func TestToolSimPipeToolResultDegrades(t *testing.T) {
	in := claudeWebTextStream([]string{`<tool_result tool_use_id="toolu_1">{"ok":true}</tool_result>`}, "end_turn")
	out := runStreamPipe(t, toolSimPipe(t, fClaude, Options{ToolSim: true}), in)
	if strings.Contains(string(out), `"tool_use"`) {
		t.Fatalf("tool_result 不得合成块:\n%.600s", out)
	}
	if !strings.Contains(string(out), `tool_result tool_use_id=`) {
		t.Fatalf("降级文本丢失:\n%.600s", out)
	}
}

// TestToolSimRequestInjectionThroughPairs S1 过 pairs：请求面注入（prompt 最前）+
// 上游体 `tools` 仍三元组；ToolSim=false 与历史行为逐字节相等（prompt 原文）。
func TestToolSimRequestInjectionThroughPairs(t *testing.T) {
	reg := NewRegistry()
	body := `{"model":"claude-sonnet-5-5","max_tokens":1024,
		"system":"Be terse.",
		"messages":[{"role":"user","content":"hi"}],
		"tools":[{"name":"Read","description":"Reads a file.","input_schema":{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}}],
		"tool_choice":{"type":"auto"}}`

	off, err := Convert(reg, registry.FormatClaudeMessages, FormatClaudeWeb, FaceRequest, []byte(body), Options{})
	if err != nil {
		t.Fatalf("Convert(off): %v", err)
	}
	on, err := Convert(reg, registry.FormatClaudeMessages, FormatClaudeWeb, FaceRequest, []byte(body), Options{ToolSim: true})
	if err != nil {
		t.Fatalf("Convert(on): %v", err)
	}

	offBody := decodeBody(t, off)
	onBody := decodeBody(t, on)

	// 缺省关：prompt 与历史 golden 逐字节相等（无注入段）。
	var offPrompt string
	_ = json.Unmarshal(offBody["prompt"], &offPrompt)
	if offPrompt != "System: Be terse.\n\nHuman: hi" {
		t.Fatalf("缺省关 prompt = %q", offPrompt)
	}
	// 开：注入段在最前 + 清单行 + 消息文本在尾。
	var onPrompt string
	_ = json.Unmarshal(onBody["prompt"], &onPrompt)
	if !strings.HasPrefix(onPrompt, "## Tool calling") {
		t.Fatalf("注入段不在最前:\n%.200s", onPrompt)
	}
	if !strings.Contains(onPrompt, "- Read(file_path: string (required)): Reads a file.") ||
		!strings.Contains(onPrompt, "EXACTLY one block per call") {
		t.Fatalf("注入段缺清单/契约句:\n%.600s", onPrompt)
	}
	if !strings.HasSuffix(onPrompt, "System: Be terse.\n\nHuman: hi") {
		t.Fatalf("消息文本错位:\n%.300s", onPrompt)
	}
	// 两体 `tools` 均三元组（W11-3 契约不破）。
	for name, b := range map[string]map[string]json.RawMessage{"off": offBody, "on": onBody} {
		var tools []map[string]string
		if err := json.Unmarshal(b["tools"], &tools); err != nil || len(tools) != 3 || tools[0]["name"] != "web_search" {
			t.Fatalf("%s tools 不再是三元组: %v (%v)", name, tools, err)
		}
	}
}

// TestToolSimResponseFaceTransform 非流式面（S2 stage 语义「流式/非流式一致」）：
// FaceResponse 路径把文本标签合成 tool_use 块 + StopReason 改写；缺省关不动。
func TestToolSimResponseFaceTransform(t *testing.T) {
	reg := NewRegistry()
	in := claudeWebTextStream([]string{`here: <tool_use name="Read">{"file_path":"a.txt"}</tool_use>`}, "end_turn")

	off, err := Convert(reg, FormatClaudeWeb, fClaude, FaceResponse, in, Options{})
	if err != nil {
		t.Fatalf("Convert(off): %v", err)
	}
	var offResp struct {
		Content []struct {
			Type string `json:"type"`
		} `json:"content"`
		StopReason string `json:"stop_reason"`
	}
	if err := json.Unmarshal(off, &offResp); err != nil {
		t.Fatalf("off 体: %v\n%s", err, off)
	}
	if offResp.StopReason != "end_turn" {
		t.Fatalf("缺省关 stop_reason = %q", offResp.StopReason)
	}

	on, err := Convert(reg, FormatClaudeWeb, fClaude, FaceResponse, in, Options{ToolSim: true})
	if err != nil {
		t.Fatalf("Convert(on): %v", err)
	}
	var onResp struct {
		Content []struct {
			Type  string          `json:"type"`
			Name  string          `json:"name"`
			Input json.RawMessage `json:"input"`
		} `json:"content"`
		StopReason string `json:"stop_reason"`
	}
	if err := json.Unmarshal(on, &onResp); err != nil {
		t.Fatalf("on 体: %v\n%s", err, on)
	}
	if onResp.StopReason != "tool_use" {
		t.Fatalf("非流式 stop_reason = %q, want tool_use", onResp.StopReason)
	}
	toolUse := 0
	for _, b := range onResp.Content {
		if b.Type == "tool_use" {
			toolUse++
			if b.Name != "Read" || string(b.Input) != `{"file_path":"a.txt"}` {
				t.Fatalf("非流式合成块 = %+v", b)
			}
		}
	}
	if toolUse != 1 {
		t.Fatalf("tool_use 块数 = %d, want 1\n%s", toolUse, on)
	}
}

// TestToolSimPipeRealFixtureUnchanged 实抓 fixture（runB，W10-3 573 帧）在 ToolSim
// 开/关下下游字节**逐字节相等**——「非契约文本零影响」在真实形态（思考摘要+正文交错）
// 上的锚点。
func TestToolSimPipeRealFixtureUnchanged(t *testing.T) {
	sse := claudeWebFixtureSSE(t, "runB_55480.537.frames.jsonl.txt")
	off := runStreamPipe(t, toolSimPipe(t, fClaude, Options{}), []byte(sse))
	on := runStreamPipe(t, toolSimPipe(t, fClaude, Options{ToolSim: true}), []byte(sse))
	if !bytes.Equal(off, on) {
		t.Fatalf("实抓 fixture 在 ToolSim 下被改动（len off=%d on=%d）", len(off), len(on))
	}
}

// TestToolSimBatchFaceStream 批量 FaceStream 路径（runStep 接线）与增量面同一语义：
// 合成块 + 参数 + stop_reason 改写；缺省关不动。
func TestToolSimBatchFaceStream(t *testing.T) {
	reg := NewRegistry()
	in := claudeWebTextStream([]string{"hi ", `<tool_use name="Read">{"file_path":"a.txt"}`, "</tool_use> bye"}, "end_turn")

	on, err := Convert(reg, FormatClaudeWeb, fClaude, FaceStream, in, Options{ToolSim: true})
	if err != nil {
		t.Fatalf("Convert(on): %v", err)
	}
	evs, err := claude_messages.ParseStream(bytes.NewReader(on))
	if err != nil {
		t.Fatalf("下游解析: %v", err)
	}
	calls, err := ir.AssembleToolCalls(evs)
	if err != nil {
		t.Fatalf("装配: %v", err)
	}
	if len(calls) != 1 || calls[0].Name != "Read" || calls[0].Arguments != `{"file_path":"a.txt"}` {
		t.Fatalf("批量面装配 = %+v", calls)
	}
	if !strings.Contains(string(on), `"stop_reason":"tool_use"`) {
		t.Fatalf("批量面 stop_reason 未改写:\n%.600s", on)
	}
	if got := downstreamText(t, on); got != "hi  bye" {
		t.Fatalf("批量面文本 = %q", got)
	}

	off, err := Convert(reg, FormatClaudeWeb, fClaude, FaceStream, in, Options{})
	if err != nil {
		t.Fatalf("Convert(off): %v", err)
	}
	if strings.Contains(string(off), `"type":"tool_use"`) {
		t.Fatalf("缺省关不得合成:\n%.600s", off)
	}
	if !strings.Contains(string(off), `"stop_reason":"end_turn"`) {
		t.Fatalf("缺省关 stop_reason 不得改写:\n%.400s", off)
	}
}

// decodeBody 渲染体 → 顶层字段 map（测试断言用）。
func decodeBody(t *testing.T, raw []byte) map[string]json.RawMessage {
	t.Helper()
	var w map[string]json.RawMessage
	if err := json.Unmarshal(raw, &w); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, raw)
	}
	return w
}
