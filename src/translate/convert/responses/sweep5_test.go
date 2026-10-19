// W5-20 条目 4：库层 responses.StreamWriter 的 EventError 错误码缺省与出口层
// （服务壳 writeResponsesError）对称——两条错误出口帧形状逐字同构。
package responses

import (
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

func TestStreamWriterErrorCodeDefault(t *testing.T) {
	render := func(t *testing.T, e *ir.Error) string {
		t.Helper()
		w := NewStreamWriter()
		if _, err := w.Write(ir.StreamEvent{Kind: ir.EventStart, MessageID: "resp_1", Model: "m"}); err != nil {
			t.Fatalf("start: %v", err)
		}
		b, err := w.Write(ir.StreamEvent{Kind: ir.EventError, ErrorReason: ir.ErrorUpstream, Error: e})
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		return string(b)
	}

	// 缺省：无 code → 平铺事件与嵌套 response.error 都写 server_error（与
	// 服务壳 writeResponsesError 同款缺省——W5-12 缺口 1 的统一动作）。
	out := render(t, &ir.Error{Type: "incomplete_stream", Message: "boom", Source: "any2api"})
	if !strings.Contains(out, `"code":"server_error"`) {
		t.Fatalf("平铺 error 事件缺省 code 缺失: %s", out)
	}
	if !strings.Contains(out, `"error":{"code":"server_error","message":"boom"}`) {
		t.Fatalf("response.failed.response.error 缺省 code 缺失: %s", out)
	}

	// 显式 code 不被缺省覆盖。
	out = render(t, &ir.Error{Code: "overloaded_error", Message: "busy", Source: "anthropic"})
	if !strings.Contains(out, `"code":"overloaded_error"`) {
		t.Fatalf("显式 code 缺失: %s", out)
	}
	if strings.Contains(out, `"code":"server_error"`) {
		t.Fatalf("显式 code 场景不得出现缺省值: %s", out)
	}

	// nil Error：合成默认错误，同样带缺省 code。
	out = render(t, nil)
	if !strings.Contains(out, `"code":"server_error"`) || !strings.Contains(out, `"message":"stream error"`) {
		t.Fatalf("nil error 缺省帧: %s", out)
	}
}
