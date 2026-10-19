package openai_chat

import (
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// W70 R03：同 index、不同非空 id 的 tool_calls 条目是**两个**调用——绝不静默
// 合并（合并 = 丢掉其中一个）。修复后两条都保留，各带自己的 id/name/args。
func TestToolCallIndexConflictKeepsBoth(t *testing.T) {
	chunk := func(delta string) string {
		return `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{` + delta + `},"finish_reason":null}]}`
	}
	finish := `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`

	t.Run("empty args distinct ids", func(t *testing.T) {
		p := NewStreamParser()
		events := feedAll(t, p,
			chunk(`"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"alpha","arguments":""}}]`),
			chunk(`"tool_calls":[{"index":0,"id":"call_b","type":"function","function":{"name":"beta","arguments":""}}]`),
			finish,
		)
		calls, err := ir.AssembleToolCalls(events)
		if err != nil {
			t.Fatalf("AssembleToolCalls: %v", err)
		}
		if len(calls) != 2 {
			t.Fatalf("len(calls) = %d (%+v), want 2（同 index 不同 id 是两次调用，不得合并）", len(calls), calls)
		}
		if calls[0].ID != "call_a" || calls[0].Name != "alpha" {
			t.Fatalf("calls[0] = %+v, want id=call_a name=alpha", calls[0])
		}
		if calls[1].ID != "call_b" || calls[1].Name != "beta" {
			t.Fatalf("calls[1] = %+v, want id=call_b name=beta", calls[1])
		}
	})

	t.Run("complete args stay per call", func(t *testing.T) {
		p := NewStreamParser()
		events := feedAll(t, p,
			chunk(`"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"alpha","arguments":"{\"x\":1}"}}]`),
			chunk(`"tool_calls":[{"index":0,"id":"call_b","type":"function","function":{"name":"beta","arguments":"{\"y\":2}"}}]`),
			finish,
		)
		calls, err := ir.AssembleToolCalls(events)
		if err != nil {
			t.Fatalf("AssembleToolCalls: %v", err)
		}
		if len(calls) != 2 {
			t.Fatalf("len(calls) = %d (%+v), want 2", len(calls), calls)
		}
		if calls[0].ID != "call_a" || calls[0].Arguments != `{"x":1}` {
			t.Fatalf("calls[0] = %+v, want call_a with {\"x\":1}", calls[0])
		}
		if calls[1].ID != "call_b" || calls[1].Arguments != `{"y":2}` {
			t.Fatalf("calls[1] = %+v, want call_b with {\"y\":2}", calls[1])
		}
	})

	t.Run("conflict inside one chunk", func(t *testing.T) {
		p := NewStreamParser()
		events := feedAll(t, p,
			chunk(`"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"alpha","arguments":""}},{"index":0,"id":"call_b","type":"function","function":{"name":"beta","arguments":""}}]`),
			finish,
		)
		calls, err := ir.AssembleToolCalls(events)
		if err != nil {
			t.Fatalf("AssembleToolCalls: %v", err)
		}
		if len(calls) != 2 || calls[0].ID != "call_a" || calls[1].ID != "call_b" {
			t.Fatalf("calls = %+v, want call_a + call_b", calls)
		}
	})
}

// 回归：同 index、同 id 的分片续传仍合并为一个调用——冲突修复不得把身份相同的
// 续传片拆成新块（续传片带 id / 不带 id 两形态）。
func TestToolCallSameIndexSameIDSplitArgsMerge(t *testing.T) {
	head := `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"f","arguments":"{\"a\":"}}]},"finish_reason":null}]}`
	finish := `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`
	tail := map[string]string{
		"repeats id": `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"arguments":"1}"}}]},"finish_reason":null}]}`,
		"omits id":   `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]},"finish_reason":null}]}`,
	}
	for name, cont := range tail {
		t.Run(name, func(t *testing.T) {
			p := NewStreamParser()
			events := feedAll(t, p, head, cont, finish)
			calls, err := ir.AssembleToolCalls(events)
			if err != nil {
				t.Fatalf("AssembleToolCalls: %v", err)
			}
			if len(calls) != 1 {
				t.Fatalf("len(calls) = %d (%+v), want 1（同 id 续传片必须合并）", len(calls), calls)
			}
			if calls[0].ID != "call_1" || calls[0].Name != "f" || calls[0].Arguments != `{"a":1}` {
				t.Fatalf("calls[0] = %+v, want call_1/f/{\"a\":1}", calls[0])
			}
		})
	}
}
