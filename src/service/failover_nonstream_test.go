// W70-1（R07）e2e：非流式上游 HTTP 200 + 响应体不可转换——**提交点未过**，
// 必须按「未提交失败」换下一台上游；只有末台（链上无可换）才落 502 信封。
// 缺陷形态：旧实现在此处直接 writeError(502) 并返回 committed=true，下游拿到
// 502 且换上游链被截断（B 零命中）。
//
// 装置同 failover_test.go：本地 httptest 假上游（无真实上游/凭据）+ 真服务
// （New(cfg).Handler() 经 httptest.NewServer）+ 真 HTTP 客户端（postJSON）。
package service

import (
	"io"
	"log"
	"net/http"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/pairs"
	"github.com/dwgx/any2api/translate/registry"
)

// nonStreamBadAnthropicBody HTTP 200 + application/json，但 claude_messages
// ParseResponse 拒绝的体（夹具的「不可转换」性由 fixture_unconvertible 子测试
// 先行自证——不靠注释约定）。
const nonStreamBadAnthropicBody = `{"role":"user","type":"message"}`

// goodAnthropicMessageJSON 合法 anthropic Message 响应（与 TestE2ENonStreaming
// 同款；转出下游含 `Hi there`）。
const goodAnthropicMessageJSON = `{"id":"msg_n1","type":"message","role":"assistant","model":"claude-x","content":[{"type":"text","text":"Hi there"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":7,"output_tokens":3}}`

// nonStreamChatReqBody 非流式 chat 入口请求体（无 stream 字段）。
const nonStreamChatReqBody = `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`

// postChatNonStream 发一次非流式 chat 请求并读回整体（状态码 + 原文）。
func postChatNonStream(t *testing.T, base string) (int, string) {
	t.Helper()
	resp := postJSON(t, base+"/v1/chat/completions", nonStreamChatReqBody, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// jsonBodyUpstream 恒定返回 200 + application/json + 固定体的假上游。
func jsonBodyUpstream(t *testing.T, body string) *countUpstream {
	t.Helper()
	return newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	})
}

// TestNonStreamConvertFailure R07：非流式 200+不可转换体的换上游面。
func TestNonStreamConvertFailure(t *testing.T) {
	// ① 前置自证：夹具体确实让 pairs.Convert(FaceResponse) 报错（否则本测试
	// 断言的是别的路径——直接红）。
	t.Run("fixture_unconvertible", func(t *testing.T) {
		out, err := pairs.Convert(pairs.NewRegistry(),
			registry.FormatClaudeMessages, registry.FormatOpenAIChat,
			pairs.FaceResponse, []byte(nonStreamBadAnthropicBody), pairs.Options{})
		if err == nil {
			t.Fatalf("夹具可转换（err=nil, out=%s）——选错体：%s", out, nonStreamBadAnthropicBody)
		}
	})

	// ② 换上游：A 200+不可转换体 → B 200 合法体；下游必须 200 + B 的内容，
	// A/B 各命中一次，failover 计数 +1；尝试记账 A=upstream_error、B=completed。
	t.Run("failover_to_next_upstream", func(t *testing.T) {
		var logBuf syncLogBuffer
		prev := log.Writer()
		log.SetOutput(&logBuf)
		defer log.SetOutput(prev)

		badA := jsonBodyUpstream(t, nonStreamBadAnthropicBody)
		goodB := jsonBodyUpstream(t, goodAnthropicMessageJSON)
		s, srv := startServiceWith(t, failoverCfg(
			anthUp("a", badA.srv.URL), anthUp("b", goodB.srv.URL)))

		code, body := postChatNonStream(t, srv.URL)
		t.Logf("hits: A=%d B=%d（期望 1/1）", badA.count(), goodB.count())
		if code != 200 || !strings.Contains(body, "Hi there") {
			t.Fatalf("status=%d body=\n%s（A 不可转换必须换 B；不得直接 502）", code, body)
		}
		if badA.count() != 1 || goodB.count() != 1 {
			t.Fatalf("hits: A=%d B=%d（期望 1/1）", badA.count(), goodB.count())
		}
		if d := s.Diagnostics(); d.Failovers < 1 {
			t.Fatalf("diagnostics failovers = %d（期望 >=1）: %+v", d.Failovers, d)
		}

		rec := parseRequestRecord(t, logBuf.String())
		if len(rec.Attempts) != 2 {
			t.Fatalf("attempt 数 = %d（want 2）: %+v", len(rec.Attempts), rec.Attempts)
		}
		if a := rec.Attempts[0]; !a.Failed || a.Delivered || a.Disconnected ||
			a.EndReason != endReasonUpstreamError || a.Status != 200 {
			t.Fatalf("A 尝试 = %+v（want failed + delivered=false + upstream_error + status=200）", a)
		}
		if a := rec.Attempts[1]; a.Failed || a.Disconnected || !a.Delivered ||
			a.EndReason != endReasonCompleted {
			t.Fatalf("B 尝试 = %+v（want completed + delivered=true）", a)
		}
	})

	// ③ 末台：只有 A（不可转换）→ 502 + `cannot be converted`；单次命中、不 panic。
	t.Run("last_upstream_502", func(t *testing.T) {
		only := jsonBodyUpstream(t, nonStreamBadAnthropicBody)
		_, srv := startServiceWith(t, failoverCfg(anthUp("only", only.srv.URL)))

		code, body := postChatNonStream(t, srv.URL)
		if code != 502 {
			t.Fatalf("status=%d body=\n%s（末台不可转换 → 502）", code, body)
		}
		if !strings.Contains(body, "cannot be converted") {
			t.Fatalf("body 缺 `cannot be converted`：\n%s", body)
		}
		if only.count() != 1 {
			t.Fatalf("hits: only=%d（期望 1）", only.count())
		}
	})
}
