// W5-30 条目 2：chat chunk 顶层字段保真（service_tier / system_fingerprint /
// moderation 三键 parse→IR→render 全程保真；obfuscation 有意忽略）。
//
// 依据：openai-wire §4.1（chunk 顶层 10 字段，P01 diff 2026-10-14）+ §4.4
// （obfuscation 是分片规整随机串，官方要求消费端忽略——不建模）。
// 通道：ir.StreamEvent.ServiceTier/SystemFingerprint/Moderation（帧级元数据，
// 与 Kind 正交）；解析器把出现帧的键原样落入该帧产生的每个事件，渲染器在对应
// 帧回写（per-event，不钉住——IR 不动点要求两侧同判）。
//
// 反证锚点：删掉 chunkWire 三键 / parse 的附着循环 / writer 的注入 → 本文件红。
package openai_chat

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// dataPayloads 提取 SSE 输出中的 data 载荷（含 [DONE] 哨兵）。
func dataPayloads(t *testing.T, sse string) []string {
	t.Helper()
	var payloads []string
	for _, frame := range strings.Split(sse, "\n\n") {
		for _, line := range strings.Split(frame, "\n") {
			if strings.HasPrefix(line, "data: ") {
				payloads = append(payloads, strings.TrimPrefix(line, "data: "))
			}
		}
	}
	return payloads
}

// parsePayloads 逐载荷 Feed（含哨兵）→ IR 事件序列。
func parsePayloads(t *testing.T, payloads []string) []ir.StreamEvent {
	t.Helper()
	sp := NewStreamParser()
	var events []ir.StreamEvent
	for _, p := range payloads {
		evs, err := sp.Feed([]byte(p))
		if err != nil {
			t.Fatalf("Feed %q: %v", p, err)
		}
		events = append(events, evs...)
	}
	events = append(events, sp.Finish()...)
	return events
}

// renderPayloads parse → render，返回 IR 事件与下游 SSE 字节。
func renderPayloads(t *testing.T, payloads []string) ([]ir.StreamEvent, string) {
	t.Helper()
	events := parsePayloads(t, payloads)
	out, err := RenderStream(events, StreamRenderOptions{IncludeUsage: true})
	if err != nil {
		t.Fatalf("RenderStream: %v", err)
	}
	return events, string(out)
}

// frameDocs 下游 SSE 的每个 data 帧（跳过哨兵）的顶层对象。
func frameDocs(t *testing.T, sse string) []map[string]json.RawMessage {
	t.Helper()
	var docs []map[string]json.RawMessage
	for _, payload := range dataPayloads(t, sse) {
		if payload == doneSentinel {
			continue
		}
		var doc map[string]json.RawMessage
		if err := json.Unmarshal([]byte(payload), &doc); err != nil {
			t.Fatalf("unmarshal %q: %v", payload, err)
		}
		docs = append(docs, doc)
	}
	return docs
}

// TestSweep13ChunkTopLevelFidelityRoundTrip 源 chunk 带三键（+obfuscation）：
// IR 事件三键齐、obfuscation 无落点；下游帧三键原值、obfuscation 缺席；
// IR 不动点（parse(render) == events）与渲染不动点（再渲染字节相等）。
func TestSweep13ChunkTopLevelFidelityRoundTrip(t *testing.T) {
	meta := `"service_tier":"default","system_fingerprint":"fp_demo",` +
		`"moderation":{"input":{"type":"moderation_results","model":"m","results":[]}},"obfuscation":"abc"`
	src := []string{
		`{"id":"chatcmpl-m1","object":"chat.completion.chunk","created":1741293000,"model":"gpt-5",` + meta +
			`,"choices":[{"index":0,"delta":{"role":"assistant","content":"Hi"},"finish_reason":null}]}`,
		`{"id":"chatcmpl-m1","object":"chat.completion.chunk","created":1741293000,"model":"gpt-5",` + meta +
			`,"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		"[DONE]",
	}
	events, out := renderPayloads(t, src)

	if len(events) == 0 {
		t.Fatalf("no IR events")
	}
	for _, ev := range events {
		if ev.Kind == ir.EventStart || ev.Kind == ir.EventBlockStart || ev.Kind == ir.EventBlockDelta ||
			ev.Kind == ir.EventMessageDelta {
			if string(ev.ServiceTier) != `"default"` || string(ev.SystemFingerprint) != `"fp_demo"` {
				t.Fatalf("IR 事件缺元数据: %+v", ev)
			}
			if len(ev.Moderation) == 0 {
				t.Fatalf("IR 事件缺 moderation: %+v", ev)
			}
		}
	}

	frames := frameDocs(t, out)
	if len(frames) == 0 {
		t.Fatalf("no downstream frames:\n%s", out)
	}
	for i, fr := range frames {
		if string(fr["service_tier"]) != `"default"` {
			t.Fatalf("frame %d service_tier = %s", i, fr["service_tier"])
		}
		if string(fr["system_fingerprint"]) != `"fp_demo"` {
			t.Fatalf("frame %d system_fingerprint = %s", i, fr["system_fingerprint"])
		}
		if len(fr["moderation"]) == 0 {
			t.Fatalf("frame %d moderation 缺失", i)
		}
		if _, ok := fr["obfuscation"]; ok {
			t.Fatalf("frame %d obfuscation 不应回写（有意忽略）", i)
		}
	}

	// IR 不动点：下游字节再 parse，事件与首轮深等。
	events2 := parsePayloads(t, dataPayloads(t, out))
	if !reflect.DeepEqual(events, events2) {
		t.Fatalf("IR 不动点破坏:\nfirst:  %+v\nsecond: %+v", events, events2)
	}
	// 渲染不动点：同一事件集再渲染逐字节相等。
	out2, err := RenderStream(events2, StreamRenderOptions{IncludeUsage: true})
	if err != nil {
		t.Fatalf("re-RenderStream: %v", err)
	}
	if string(out2) != out {
		t.Fatalf("渲染不动点破坏:\nfirst:  %s\nsecond: %s", out, out2)
	}
}

// TestSweep13ChunkMetaPartialFramesRoundTrip 源只在部分帧带键（首帧带、收尾帧
// 不带）：下游只在对应帧回写（**不钉住**——per-event；钉住会把首帧的键扩散到
// 收尾帧并破坏 IR 不动点），且 IR 不动点保持。
func TestSweep13ChunkMetaPartialFramesRoundTrip(t *testing.T) {
	events, out := renderPayloads(t, []string{
		`{"id":"chatcmpl-m6","object":"chat.completion.chunk","created":1,"model":"gpt-5",` +
			`"service_tier":"default","system_fingerprint":"fp_demo",` +
			`"choices":[{"index":0,"delta":{"role":"assistant","content":"Hi"},"finish_reason":null}]}`,
		`{"id":"chatcmpl-m6","object":"chat.completion.chunk","created":1,"model":"gpt-5",` +
			`"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		"[DONE]",
	})
	frames := frameDocs(t, out)
	if len(frames) < 2 {
		t.Fatalf("frames = %d, want >= 2:\n%s", len(frames), out)
	}
	for i, fr := range frames[:len(frames)-1] {
		if string(fr["service_tier"]) != `"default"` || string(fr["system_fingerprint"]) != `"fp_demo"` {
			t.Fatalf("frame %d 该带键（源首帧带）: %s", i, out)
		}
	}
	last := frames[len(frames)-1]
	if _, ok := last["service_tier"]; ok {
		t.Fatalf("收尾帧不该带键（源未带——不得钉住扩散）: %s", out)
	}
	if _, ok := last["system_fingerprint"]; ok {
		t.Fatalf("收尾帧不该带 system_fingerprint: %s", out)
	}
	events2 := parsePayloads(t, dataPayloads(t, out))
	if !reflect.DeepEqual(events, events2) {
		t.Fatalf("IR 不动点破坏:\nfirst:  %+v\nsecond: %+v", events, events2)
	}
}

// TestSweep13ChunkMetaAbsentNoInvention 源 chunk 无三键（或三键为 null）：
// 下游帧不得发明这些键；null 视同缺省（不落 IR）。
func TestSweep13ChunkMetaAbsentNoInvention(t *testing.T) {
	// 无三键。
	_, out := renderPayloads(t, []string{
		`{"id":"chatcmpl-m2","object":"chat.completion.chunk","created":1,"model":"gpt-5",` +
			`"choices":[{"index":0,"delta":{"role":"assistant","content":"Hi"},"finish_reason":"stop"}]}`,
		"[DONE]",
	})
	for i, fr := range frameDocs(t, out) {
		for _, k := range []string{"service_tier", "system_fingerprint", "moderation"} {
			if _, ok := fr[k]; ok {
				t.Fatalf("frame %d 发明了 %s（源无此键）", i, k)
			}
		}
	}

	// 显式 null：视同缺省（IR 无落点、下游无键）。
	events, out := renderPayloads(t, []string{
		`{"id":"chatcmpl-m3","object":"chat.completion.chunk","created":1,"model":"gpt-5",` +
			`"service_tier":null,"system_fingerprint":null,"moderation":null,` +
			`"choices":[{"index":0,"delta":{"role":"assistant","content":"Hi"},"finish_reason":"stop"}]}`,
		"[DONE]",
	})
	for _, ev := range events {
		if len(ev.ServiceTier) > 0 || len(ev.SystemFingerprint) > 0 || len(ev.Moderation) > 0 {
			t.Fatalf("null 元数据落入 IR: %+v", ev)
		}
	}
	for i, fr := range frameDocs(t, out) {
		for _, k := range []string{"service_tier", "system_fingerprint", "moderation"} {
			if _, ok := fr[k]; ok {
				t.Fatalf("frame %d 回写了 null 元数据 %s", i, k)
			}
		}
	}
}

// TestSweep13ChunkMetaOnUsageChunk usage chunk（choices:[] + usage + 元数据）
// 保真：usage 帧带三键（官方逐帧同值的形态）。
func TestSweep13ChunkMetaOnUsageChunk(t *testing.T) {
	events, out := renderPayloads(t, []string{
		`{"id":"chatcmpl-m4","object":"chat.completion.chunk","created":1,"model":"gpt-5",` +
			`"choices":[{"index":0,"delta":{"role":"assistant","content":"Hi"},"finish_reason":"stop"}]}`,
		`{"id":"chatcmpl-m4","object":"chat.completion.chunk","created":1,"model":"gpt-5",` +
			`"service_tier":"priority","system_fingerprint":"fp_x","choices":[],` +
			`"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`,
		"[DONE]",
	})
	sawMetaEvent := false
	for _, ev := range events {
		if string(ev.ServiceTier) == `"priority"` {
			sawMetaEvent = true
		}
	}
	if !sawMetaEvent {
		t.Fatalf("usage chunk 元数据未落 IR: %+v", events)
	}
	frames := frameDocs(t, out)
	last := frames[len(frames)-1]
	if len(last["usage"]) == 0 {
		t.Fatalf("末帧不是 usage 帧: %s", out)
	}
	if string(last["service_tier"]) != `"priority"` || string(last["system_fingerprint"]) != `"fp_x"` {
		t.Fatalf("usage 帧元数据丢失: %s", out)
	}
}

// TestSweep13MetaOnlyChunkBoundary 边界（有意不覆盖）：choices:[] 且无 usage 的
// **纯元数据帧**（官方 moderation chunk 的推定形态，spec/文档未给例）不产 IR 事件
// ——IR 无帧级元数据事件，发明事件类型超出本单；此帧仍按既有「空帧容忍忽略」
// 路径丢弃（行为与 W5-30 前一致）。本测试把该边界钉住（有意变更需显式改这里）。
func TestSweep13MetaOnlyChunkBoundary(t *testing.T) {
	sp := NewStreamParser()
	events, err := sp.Feed([]byte(`{"id":"chatcmpl-m5","object":"chat.completion.chunk","created":1,` +
		`"model":"gpt-5","service_tier":"default","choices":[],` +
		`"moderation":{"input":{"type":"moderation_results","model":"m","results":[]}}}`))
	if err != nil {
		t.Fatalf("Feed: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("纯元数据帧应无事件（边界），got %+v", events)
	}
}
