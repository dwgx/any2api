package openai_chat

// W5-37 缺陷小修测试锚点：上游 `reasoning_content`（非官方扩展，OpenAI 兼容
// 生态约定；longtail-matrix §4.3 / anthropic-conversion-table §响应侧/流式
// 「思考」行）在解析侧映射为 IR 思考块。
//
// 修前：解析侧完全不读该字段——跨格式腿（claude/gemini/responses 入口 ←
// openai 上游）静默丢思考（同协议腿是字节直通，不受影响）。修后：非流式
// `message.reasoning_content` 置首成思考块；流式 `delta.reasoning_content`
// 逐帧开块；无签名载体（签名通道不发明）。

import (
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

func TestParseResponseReasoningContent(t *testing.T) {
	body := []byte(`{"id":"c1","object":"chat.completion","created":1,"model":"m",
		"choices":[{"index":0,"message":{"role":"assistant","content":"PONG","reasoning_content":"The user wants PONG."},"finish_reason":"stop"}],
		"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`)
	resp, err := ParseResponse(body)
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if len(resp.Message.Blocks) != 2 ||
		resp.Message.Blocks[0].Kind != ir.BlockThinking || resp.Message.Blocks[0].Text != "The user wants PONG." ||
		resp.Message.Blocks[1].Kind != ir.BlockText || resp.Message.Blocks[1].Text != "PONG" {
		t.Fatalf("blocks = %+v（want 思考块置首 + 文本块）", resp.Message.Blocks)
	}

	// 无该字段 → 不发明思考块（缺省行为不变）。
	resp2, err := ParseResponse([]byte(`{"id":"c2","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}`))
	if err != nil {
		t.Fatalf("ParseResponse plain: %v", err)
	}
	for _, b := range resp2.Message.Blocks {
		if b.Kind == ir.BlockThinking {
			t.Fatalf("无字段却出现思考块: %+v", resp2.Message.Blocks)
		}
	}

	// null / 空串 → 不发明（宽容读取）。
	for _, rc := range []string{`null`, `""`} {
		resp3, err := ParseResponse([]byte(`{"id":"c3","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"hi","reasoning_content":` + rc + `},"finish_reason":"stop"}]}`))
		if err != nil {
			t.Fatalf("ParseResponse rc=%s: %v", rc, err)
		}
		for _, b := range resp3.Message.Blocks {
			if b.Kind == ir.BlockThinking {
				t.Fatalf("rc=%s 却出现思考块: %+v", rc, resp3.Message.Blocks)
			}
		}
	}
}

func TestStreamParserReasoningContent(t *testing.T) {
	p := NewStreamParser()
	var evs []ir.StreamEvent
	feed := func(s string) {
		t.Helper()
		got, err := p.Feed([]byte(s))
		if err != nil {
			t.Fatalf("Feed(%s): %v", s, err)
		}
		evs = append(evs, got...)
	}
	feed(`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"hmm"}}]}`)
	feed(`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"reasoning_content":" more"}}]}`)
	feed(`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"PONG"}}]}`)
	feed(`{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`)
	feed(`[DONE]`)

	// 首个块 = 思考块（index 0），思考增量逐帧、无签名增量。
	var thinkingText, textOut strings.Builder
	var starts []ir.StreamEvent
	var stops []ir.StreamEvent
	var sawSignatureDelta bool
	for _, ev := range evs {
		switch ev.Kind {
		case ir.EventBlockStart:
			starts = append(starts, ev)
		case ir.EventBlockDelta:
			switch ev.BlockKind {
			case ir.BlockThinking:
				if ev.SignatureDelta != "" {
					sawSignatureDelta = true
				}
				thinkingText.WriteString(ev.TextDelta)
			case ir.BlockText:
				textOut.WriteString(ev.TextDelta)
			}
		case ir.EventBlockStop:
			stops = append(stops, ev)
		}
	}
	if len(starts) != 2 || starts[0].BlockKind != ir.BlockThinking || starts[0].BlockIndex != 0 ||
		starts[1].BlockKind != ir.BlockText || starts[1].BlockIndex != 1 {
		t.Fatalf("block starts = %+v（want 思考块 0 + 文本块 1）", starts)
	}
	if thinkingText.String() != "hmm more" || textOut.String() != "PONG" {
		t.Fatalf("thinking=%q text=%q", thinkingText.String(), textOut.String())
	}
	if sawSignatureDelta {
		t.Fatal("签名通道不得发明（reasoning_content 无签名载体）")
	}
	if len(stops) != 2 || stops[0].BlockKind != ir.BlockThinking || stops[1].BlockKind != ir.BlockText {
		t.Fatalf("block stops = %+v", stops)
	}
	// 块序纪律：思考块 stop 必须早于正文块 start（Anthropic 块严格顺序；
	// gemini 出口按 stop 落 thought part——thought part 先于 text part）。
	var stopThink, startText = -1, -1
	for i, ev := range evs {
		if ev.Kind == ir.EventBlockStop && ev.BlockKind == ir.BlockThinking && stopThink < 0 {
			stopThink = i
		}
		if ev.Kind == ir.EventBlockStart && ev.BlockKind == ir.BlockText && startText < 0 {
			startText = i
		}
	}
	if stopThink < 0 || startText < 0 || stopThink > startText {
		t.Fatalf("思考块 stop（%d）应早于正文块 start（%d）", stopThink, startText)
	}

	// 负对照：无 reasoning_content 的流不出现思考块。
	p2 := NewStreamParser()
	got, err := p2.Feed([]byte(`{"id":"c2","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hi"}}]}`))
	if err != nil {
		t.Fatalf("Feed plain: %v", err)
	}
	for _, ev := range got {
		if ev.BlockKind == ir.BlockThinking {
			t.Fatalf("负对照出现思考块: %+v", got)
		}
	}
}
