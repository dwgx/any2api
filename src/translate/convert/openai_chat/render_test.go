package openai_chat

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

func TestRenderEmptyToolArgumentsBecomeEmptyObject(t *testing.T) {
	// 空参数出口必须发 "{}" 而不是空串（cc-wire-contract 空参数门）。
	req := &ir.Request{
		Model: "m",
		Messages: []ir.Message{{
			Role: ir.RoleAssistant,
			Blocks: []ir.ContentBlock{{
				Kind:     ir.BlockToolUse,
				ToolCall: &ir.ToolCall{ID: "call_1", Name: "f"},
			}},
		}},
	}
	out, err := RenderRequest(req, RenderOptions{})
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	if !strings.Contains(string(out), `"arguments":"{}"`) {
		t.Fatalf("arguments not fixed to {}: %s", out)
	}
	// 往返确认。
	back, err := ParseRequest(out)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if got := back.Messages[0].Blocks[0].ToolCall.Arguments; got != "{}" {
		t.Fatalf("round-trip arguments = %q", got)
	}
}

func TestRenderStreamToolBlockWithoutFragmentsGetsEmptyObject(t *testing.T) {
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "c1", Model: "m", Created: 1},
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockToolUse, ToolCallID: "call_1", ToolName: "f"},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockToolUse},
		{Kind: ir.EventMessageDelta, StopReason: ir.StopToolUse},
		{Kind: ir.EventMessageStop},
		{Kind: ir.EventDone, Reason: ir.StopToolUse},
	}
	out, err := RenderStream(events, StreamRenderOptions{})
	if err != nil {
		t.Fatalf("RenderStream: %v", err)
	}
	if !strings.Contains(string(out), `"arguments":"{}"`) {
		t.Fatalf("arguments not fixed to {}: %s", out)
	}
	events2, err := ParseStream(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("re-ParseStream: %v", err)
	}
	calls, err := ir.AssembleToolCalls(events2)
	if err != nil {
		t.Fatalf("AssembleToolCalls: %v", err)
	}
	if len(calls) != 1 || calls[0].Arguments != "{}" {
		t.Fatalf("calls = %+v", calls)
	}
}

func TestRenderStreamIncludeUsageControlsUsageChunk(t *testing.T) {
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "c1", Model: "m", Created: 1},
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockText},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockText, TextDelta: "hi"},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockText},
		{Kind: ir.EventMessageDelta, StopReason: ir.StopStop},
		{Kind: ir.EventMessageDelta, Usage: &ir.Usage{Input: 9, Output: 2, Total: 11, Accounting: "openai"}},
		{Kind: ir.EventMessageStop},
		{Kind: ir.EventDone, Reason: ir.StopStop},
	}
	withUsage, err := RenderStream(events, StreamRenderOptions{IncludeUsage: true})
	if err != nil {
		t.Fatalf("RenderStream: %v", err)
	}
	if !strings.Contains(string(withUsage), `"usage":{"completion_tokens":2,"prompt_tokens":9,"total_tokens":11}`) {
		t.Fatalf("usage chunk missing: %s", withUsage)
	}
	withoutUsage, err := RenderStream(events, StreamRenderOptions{IncludeUsage: false})
	if err != nil {
		t.Fatalf("RenderStream: %v", err)
	}
	if strings.Contains(string(withoutUsage), "usage") {
		t.Fatalf("usage chunk must be dropped when include_usage=false: %s", withoutUsage)
	}
}

func TestRenderStreamEventsAfterTerminal(t *testing.T) {
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "c1", Model: "m", Created: 1},
		{Kind: ir.EventDone, Reason: ir.StopStop},
		{Kind: ir.EventMessageStop},
	}
	if _, err := RenderStream(events, StreamRenderOptions{}); !errors.Is(err, ErrEventsAfterTerminal) {
		t.Fatalf("err = %v, want ErrEventsAfterTerminal", err)
	}
}

func TestRenderStreamRequiresStart(t *testing.T) {
	if _, err := RenderStream(nil, StreamRenderOptions{}); !errors.Is(err, ErrMissingStart) {
		t.Fatalf("empty: err = %v, want ErrMissingStart", err)
	}
	events := []ir.StreamEvent{{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockText, TextDelta: "x"}}
	if _, err := RenderStream(events, StreamRenderOptions{}); !errors.Is(err, ErrMissingStart) {
		t.Fatalf("no-start: err = %v, want ErrMissingStart", err)
	}
}

func TestRenderStreamAppendsSentinelWithoutDoneEvent(t *testing.T) {
	// 哨兵是合成器职责：事件流没带 done 也要收尾。
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "c1", Model: "m", Created: 1},
		{Kind: ir.EventMessageStop},
	}
	out, err := RenderStream(events, StreamRenderOptions{})
	if err != nil {
		t.Fatalf("RenderStream: %v", err)
	}
	if !bytes.HasSuffix(out, []byte("data: [DONE]\n\n")) {
		t.Fatalf("missing sentinel: %s", out)
	}
}

// W5-26 裁决：错误帧后**不补** `[DONE]`（Bifrost #5546 形态；旧 vLLM 形态已改判）。
// 反证：`done` 事件仍必须发哨兵（下一条测试锁）；Finish 在错误终态下也不补。
func TestRenderStreamErrorFrameNoSentinel(t *testing.T) {
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "c1", Model: "m", Created: 1},
		{Kind: ir.EventError, ErrorReason: ir.ErrorUpstream, Error: &ir.Error{
			Type: "server_error", Message: "boom", Source: "openai",
		}},
	}
	out, err := RenderStream(events, StreamRenderOptions{})
	if err != nil {
		t.Fatalf("RenderStream: %v", err)
	}
	if !strings.Contains(string(out), `"error":{"message":"boom","type":"server_error"}`) {
		t.Fatalf("error frame missing: %s", out)
	}
	if bytes.Contains(out, []byte("[DONE]")) {
		t.Fatalf("错误帧后不得补 [DONE]（假成功）: %s", out)
	}
	// Finish 路径同判：错误终态下不补哨兵（增量 writer 的上游 EOF 收尾）。
	w := NewStreamWriter(StreamRenderOptions{})
	for _, ev := range events {
		if _, err := w.Write(ev); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	tail, err := w.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if bytes.Contains(tail, []byte("[DONE]")) {
		t.Fatalf("Finish 在错误终态后不得补 [DONE]: %s", tail)
	}
}

func TestRenderErrorNeverLeaksRaw(t *testing.T) {
	// 错误默认不透传上游 body：Raw 只供诊断。
	e := &ir.Error{
		Type: "invalid_request_error", Message: "bad", Param: "model", Code: "x",
		Raw: []byte(`{"secret":"token123"}`),
	}
	out, err := RenderError(e)
	if err != nil {
		t.Fatalf("RenderError: %v", err)
	}
	if strings.Contains(string(out), "secret") || strings.Contains(string(out), "token123") {
		t.Fatalf("raw leaked: %s", out)
	}
	if !strings.Contains(string(out), `"message":"bad"`) || !strings.Contains(string(out), `"param":"model"`) {
		t.Fatalf("fields missing: %s", out)
	}
}

func TestRenderToolErrorFlagRejected(t *testing.T) {
	// is_error 无 OpenAI Chat 表示——不静默吞掉错误标记。
	req := &ir.Request{
		Model: "m",
		Messages: []ir.Message{{
			Role: ir.RoleTool,
			Blocks: []ir.ContentBlock{{
				Kind:       ir.BlockToolResult,
				ToolResult: &ir.ToolResult{CallID: "call_1", Content: "boom", IsError: true},
			}},
		}},
	}
	if _, err := RenderRequest(req, RenderOptions{}); !errors.Is(err, ErrToolErrorFlag) {
		t.Fatalf("err = %v, want ErrToolErrorFlag", err)
	}
}

func TestRenderUnsupportedBlockKindRejected(t *testing.T) {
	// 真正无载体的块（audio 未建模进 OpenAI 线）仍显式报错；thinking 自 W5-16 起
	// 走决策表（请求面丢弃，见 TestRenderThinkingFaces）。
	req := &ir.Request{
		Model: "m",
		Messages: []ir.Message{{
			Role:   ir.RoleAssistant,
			Blocks: []ir.ContentBlock{{Kind: ir.BlockAudio, Text: "hmm"}},
		}},
	}
	if _, err := RenderRequest(req, RenderOptions{}); !errors.Is(err, ErrUnsupportedBlock) {
		t.Fatalf("err = %v, want ErrUnsupportedBlock", err)
	}
}

func TestRenderMessageContentForms(t *testing.T) {
	// content 形态规范化：无块 → null；单 text → 字符串；text+image → parts 数组。
	cases := []struct {
		name   string
		blocks []ir.ContentBlock
		want   string
	}{
		{"empty", nil, `"content":null`},
		{"single-text", []ir.ContentBlock{{Kind: ir.BlockText, Text: "hi"}}, `"content":"hi"`},
		{
			"text-image",
			[]ir.ContentBlock{
				{Kind: ir.BlockText, Text: "look"},
				{Kind: ir.BlockImage, Image: &ir.ImageSource{Kind: "file", URL: "https://x/y.png", Detail: "auto"}},
			},
			`"content":[{"text":"look","type":"text"},{"image_url":{"detail":"auto","url":"https://x/y.png"},"type":"image_url"}]`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := &ir.Request{Model: "m", Messages: []ir.Message{{Role: ir.RoleUser, Blocks: tc.blocks}}}
			out, err := RenderRequest(req, RenderOptions{})
			if err != nil {
				t.Fatalf("RenderRequest: %v", err)
			}
			if !strings.Contains(string(out), tc.want) {
				t.Fatalf("out = %s\nwant contains %s", out, tc.want)
			}
		})
	}
}

func TestRenderStreamToolFragmentsAttachFirstToBlockStart(t *testing.T) {
	// 首片参数附着在 block_start chunk（id/name/arguments 同帧），后续片独立成帧。
	events := []ir.StreamEvent{
		{Kind: ir.EventStart, MessageID: "c1", Model: "m", Created: 1},
		{Kind: ir.EventBlockStart, BlockIndex: 0, BlockKind: ir.BlockToolUse, ToolCallID: "call_1", ToolName: "f"},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockToolUse, ArgsDelta: `{"a":`},
		{Kind: ir.EventBlockDelta, BlockIndex: 0, BlockKind: ir.BlockToolUse, ArgsDelta: `1}`},
		{Kind: ir.EventBlockStop, BlockIndex: 0, BlockKind: ir.BlockToolUse},
		{Kind: ir.EventDone, Reason: ir.StopToolUse},
	}
	out, err := RenderStream(events, StreamRenderOptions{})
	if err != nil {
		t.Fatalf("RenderStream: %v", err)
	}
	frames := sseFrames(t, out)
	// frames: role, block_start+frag0, frag1, [DONE]
	if len(frames) != 4 {
		t.Fatalf("frames = %d: %s", len(frames), out)
	}
	if !strings.Contains(frames[1], `"arguments":"{\"a\":"`) || !strings.Contains(frames[1], `"name":"f"`) {
		t.Fatalf("frame[1] = %s", frames[1])
	}
	if !strings.Contains(frames[2], `"arguments":"1}"`) || strings.Contains(frames[2], `"name"`) {
		t.Fatalf("frame[2] = %s", frames[2])
	}
}
