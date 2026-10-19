package openai_chat

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

const (
	chunkRole   = `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}`
	chunkTextX  = `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"x"},"finish_reason":null}]}`
	chunkFinish = `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`
)

func feedAll(t *testing.T, p *StreamParser, payloads ...string) []ir.StreamEvent {
	t.Helper()
	var events []ir.StreamEvent
	for _, s := range payloads {
		evs, err := p.Feed([]byte(s))
		if err != nil {
			t.Fatalf("Feed(%q): %v", s, err)
		}
		events = append(events, evs...)
	}
	return events
}

func TestParseStreamSameChunkSameIndexMerge(t *testing.T) {
	// openai-wire §4.2：同一 chunk 内同 index 的多片段必须合并（python accumulate_delta）。
	p := NewStreamParser()
	events := feedAll(t, p,
		`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"f","arguments":"{\"a\":"}},{"index":0,"function":{"arguments":"1}"}}]},"finish_reason":null}]}`)
	calls, err := ir.AssembleToolCalls(events)
	if err != nil {
		t.Fatalf("AssembleToolCalls: %v", err)
	}
	if len(calls) != 1 || calls[0].ID != "call_1" || calls[0].Name != "f" || calls[0].Arguments != `{"a":1}` {
		t.Fatalf("calls = %+v", calls)
	}
}

func TestParseStreamDoneStartswithTolerance(t *testing.T) {
	// openai-python 用 startswith 判哨兵（容忍尾随内容）——解析端取更宽容的一极。
	for _, payload := range []string{"[DONE]", " [DONE] ", "[DONE]extra"} {
		p := NewStreamParser()
		feedAll(t, p, chunkRole, chunkFinish)
		evs, err := p.Feed([]byte(payload))
		if err != nil {
			t.Fatalf("Feed(%q): %v", payload, err)
		}
		if len(evs) != 2 || evs[0].Kind != ir.EventMessageStop ||
			evs[1].Kind != ir.EventDone || evs[1].Reason != ir.StopStop {
			t.Fatalf("Feed(%q) events = %+v", payload, evs)
		}
	}
}

func TestParseStreamDataAfterDoneRejected(t *testing.T) {
	p := NewStreamParser()
	feedAll(t, p, chunkRole, chunkFinish)
	if _, err := p.Feed([]byte("[DONE]")); err != nil {
		t.Fatalf("Feed([DONE]): %v", err)
	}
	if _, err := p.Feed([]byte(chunkTextX)); !errors.Is(err, ErrDataAfterDone) {
		t.Fatalf("err = %v, want ErrDataAfterDone", err)
	}
	if _, err := p.Feed([]byte("[DONE]")); !errors.Is(err, ErrDataAfterDone) {
		t.Fatalf("duplicate sentinel err = %v, want ErrDataAfterDone", err)
	}
}

func TestParseStreamErrorFrame(t *testing.T) {
	p := NewStreamParser()
	evs, err := p.Feed([]byte(`{"error":{"message":"boom","type":"server_error","param":null,"code":"server_error"}}`))
	if err != nil {
		t.Fatalf("Feed: %v", err)
	}
	if len(evs) != 1 || evs[0].Kind != ir.EventError || evs[0].ErrorReason != ir.ErrorUpstream {
		t.Fatalf("events = %+v", evs)
	}
	if evs[0].Error == nil || evs[0].Error.Message != "boom" || evs[0].Error.Type != "server_error" ||
		evs[0].Error.Code != "server_error" || evs[0].Error.Source != "openai" {
		t.Fatalf("error = %+v", evs[0].Error)
	}
	// vLLM 形态：错误后仍发 [DONE]——容忍（openai-wire 陷阱：两种模式并存）。
	evs2, err := p.Feed([]byte("[DONE]"))
	if err != nil || len(evs2) != 0 {
		t.Fatalf("Feed([DONE]) after error: %+v, %v", evs2, err)
	}
	if fin := p.Finish(); len(fin) != 0 {
		t.Fatalf("Finish after error = %+v", fin)
	}
}

func TestParseStreamErrorThenDataRejected(t *testing.T) {
	p := NewStreamParser()
	if _, err := p.Feed([]byte(`{"error":{"message":"boom"}}`)); err != nil {
		t.Fatalf("Feed(error): %v", err)
	}
	if _, err := p.Feed([]byte(chunkTextX)); !errors.Is(err, ErrDataAfterError) {
		t.Fatalf("err = %v, want ErrDataAfterError", err)
	}
}

func TestParseStreamTruncatedWithoutFinish(t *testing.T) {
	// 截断流是失败态，不是「更短的正常流」（CPA 502 / omp incomplete-stream）。
	p := NewStreamParser()
	feedAll(t, p, chunkRole, chunkTextX)
	fin := p.Finish()
	if len(fin) != 1 || fin[0].Kind != ir.EventError || fin[0].ErrorReason != ir.ErrorTransport {
		t.Fatalf("Finish = %+v", fin)
	}
	if fin[0].Error == nil || fin[0].Error.Type != "incomplete_stream" {
		t.Fatalf("error = %+v", fin[0].Error)
	}
}

func TestParseStreamEOFAfterFinishIsNormal(t *testing.T) {
	// 无哨兵 EOF 也是正常结束路径（new-api StreamEndReason.EOF，stream-mechanics §4.7）。
	p := NewStreamParser()
	feedAll(t, p, chunkRole, chunkTextX, chunkFinish)
	fin := p.Finish()
	if len(fin) != 2 || fin[0].Kind != ir.EventMessageStop ||
		fin[1].Kind != ir.EventDone || fin[1].Reason != ir.StopStop {
		t.Fatalf("Finish = %+v", fin)
	}
}

func TestParseStreamDoneWithoutFinishRejected(t *testing.T) {
	p := NewStreamParser()
	evs, err := p.Feed([]byte("[DONE]"))
	if err != nil {
		t.Fatalf("Feed: %v", err)
	}
	if len(evs) != 1 || evs[0].Kind != ir.EventError || evs[0].ErrorReason != ir.ErrorTransport {
		t.Fatalf("events = %+v", evs)
	}
}

func TestParseStreamUnknownFieldsTolerated(t *testing.T) {
	// obfuscation 默认开启 + 未知字段/未知 delta 字段：解析器忽略不炸（openai-wire §4.4）。
	p := NewStreamParser()
	events := feedAll(t, p,
		`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","obfuscation":"xyz","vendor_ext":{},
		  "choices":[{"index":0,"delta":{"content":"x","unknown_vendor_field":42},"finish_reason":null}]}`)
	if got := collectText(events); got != "x" {
		t.Fatalf("text = %q", got)
	}
}

func TestParseStreamEmptyFramesTolerated(t *testing.T) {
	p := NewStreamParser()
	for _, payload := range []string{"null", "{}", `{"choices":[]}`} {
		evs, err := p.Feed([]byte(payload))
		if err != nil || len(evs) != 0 {
			t.Fatalf("Feed(%q) = %+v, %v", payload, evs, err)
		}
	}
}

func TestParseStreamMultiChoiceRejected(t *testing.T) {
	p := NewStreamParser()
	_, err := p.Feed([]byte(`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[
		{"index":0,"delta":{"content":"a"},"finish_reason":null},
		{"index":1,"delta":{"content":"b"},"finish_reason":null}]}`))
	if !errors.Is(err, ErrUnsupportedMultiChoice) {
		t.Fatalf("err = %v, want ErrUnsupportedMultiChoice", err)
	}
}

func TestParseStreamToolCallWithoutIndexRejected(t *testing.T) {
	p := NewStreamParser()
	_, err := p.Feed([]byte(`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"id":"call_1","type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":null}]}`))
	if !errors.Is(err, ErrBadChunk) {
		t.Fatalf("err = %v, want ErrBadChunk", err)
	}
}

func TestParseStreamRefusalDelta(t *testing.T) {
	p := NewStreamParser()
	events := feedAll(t, p,
		`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"refusal":"cannot"},"finish_reason":null}]}`)
	found := false
	for _, ev := range events {
		if ev.Kind == ir.EventBlockDelta && ev.BlockKind == ir.BlockRefusal && ev.TextDelta == "cannot" {
			found = true
		}
	}
	if !found {
		t.Fatalf("events = %+v", events)
	}
}

func TestParseStreamBadJSONRejected(t *testing.T) {
	p := NewStreamParser()
	if _, err := p.Feed([]byte("{not json")); !errors.Is(err, ErrBadChunk) {
		t.Fatalf("err = %v, want ErrBadChunk", err)
	}
}

func TestParseStreamViaReader(t *testing.T) {
	// 内含最小切帧器：注释行（心跳）不产事件、CRLF 行尾规范化、
	// EOF 时未闭合帧（末行 [DONE] 无尾随空行）也派发。
	raw := ": keepalive\n" +
		"data: " + chunkRole + "\r\n\r\n" +
		"data: " + chunkTextX + "\n\n" +
		"data: " + chunkFinish + "\n\n" +
		"data: [DONE]\n"
	events, err := ParseStream(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("ParseStream: %v", err)
	}
	if got := collectText(events); got != "x" {
		t.Fatalf("text = %q", got)
	}
	if r, ok := doneReason(events); !ok || r != ir.StopStop {
		t.Fatalf("done = %q ok=%v", r, ok)
	}
}

func TestParseResponseMultiChoiceRejected(t *testing.T) {
	_, err := ParseResponse([]byte(`{"id":"r1","object":"chat.completion","created":1,"model":"m","choices":[
		{"index":0,"message":{"role":"assistant","content":"a"},"finish_reason":"stop"},
		{"index":1,"message":{"role":"assistant","content":"b"},"finish_reason":"stop"}]}`))
	if !errors.Is(err, ErrUnsupportedMultiChoice) {
		t.Fatalf("err = %v, want ErrUnsupportedMultiChoice", err)
	}
}

func TestParseResponseRefusalBlock(t *testing.T) {
	resp, err := ParseResponse([]byte(`{"id":"r1","object":"chat.completion","created":1,"model":"m","choices":[
		{"index":0,"message":{"role":"assistant","content":null,"refusal":"I cannot help with that."},"finish_reason":"stop"}]}`))
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	var found bool
	for _, b := range resp.Message.Blocks {
		if b.Kind == ir.BlockRefusal && b.Text == "I cannot help with that." {
			found = true
		}
	}
	if !found {
		t.Fatalf("blocks = %+v", resp.Message.Blocks)
	}
}

func TestParseRequestStreamOptionsRequiresStream(t *testing.T) {
	_, err := ParseRequest([]byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"stream_options":{"include_usage":true}}`))
	if !errors.Is(err, ErrStreamOptionsWithoutStream) {
		t.Fatalf("err = %v, want ErrStreamOptionsWithoutStream", err)
	}
}

func TestParseRequestUnknownRolePassthrough(t *testing.T) {
	// 已弃用 function 角色原样承载（归一策略在转换层，不在解析层）。
	req, err := ParseRequest([]byte(`{"model":"m","messages":[{"role":"function","name":"f","content":"result"}]}`))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if got := req.Messages[0].Role; got != ir.Role("function") {
		t.Fatalf("role = %q", got)
	}
}

func TestParseRequestUnsupportedToolType(t *testing.T) {
	_, err := ParseRequest([]byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"web_search"}]}`))
	if !errors.Is(err, ErrUnsupportedToolType) {
		t.Fatalf("err = %v, want ErrUnsupportedToolType", err)
	}
}

func TestParseErrorEnvelope(t *testing.T) {
	e := ParseError([]byte(`{"error":{"message":"bad key","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}`))
	if e.Message != "bad key" || e.Type != "invalid_request_error" || e.Code != "invalid_api_key" || e.Param != "" {
		t.Fatalf("error = %+v", e)
	}
	if e.Source != "openai" || len(e.Raw) == 0 {
		t.Fatalf("source/raw = %q / %q", e.Source, e.Raw)
	}
	// 数值 code（Gemini 风格）以字符串承载。
	e2 := ParseError([]byte(`{"error":{"message":"quota","type":"rate_limit","code":429}}`))
	if e2.Code != "429" {
		t.Fatalf("code = %q", e2.Code)
	}
}

func TestParseErrorNonJSONBody(t *testing.T) {
	body := []byte("<html>502 Bad Gateway</html>")
	e := ParseError(body)
	if e.Type != "api_error" || e.Source != "openai" {
		t.Fatalf("error = %+v", e)
	}
	if !bytes.Equal(e.Raw, body) {
		t.Fatalf("raw not preserved: %q", e.Raw)
	}
}

func TestParseUsageDetailsDerivedAndVerbatim(t *testing.T) {
	// details 原文整体进 Extra（保真回传）；CacheRead/Reasoning 为派生只读值。
	resp, err := ParseResponse([]byte(`{"id":"r1","object":"chat.completion","created":1,"model":"m",
		"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
		"usage":{"prompt_tokens":100,"completion_tokens":20,"total_tokens":120,
			"prompt_tokens_details":{"cached_tokens":64,"audio_tokens":0},
			"completion_tokens_details":{"reasoning_tokens":8}}}`))
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	u := resp.Usage
	if u == nil || u.Input != 100 || u.Output != 20 || u.Total != 120 || u.CacheRead != 64 || u.Reasoning != 8 {
		t.Fatalf("usage = %+v", u)
	}
	if len(u.Extra["prompt_tokens_details"]) == 0 || len(u.Extra["completion_tokens_details"]) == 0 {
		t.Fatalf("details not verbatim in Extra: %+v", u.Extra)
	}
}
