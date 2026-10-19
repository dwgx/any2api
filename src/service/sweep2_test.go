package service

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// W5-16 ⑤：首产出超时 → 换上游（TTFT 的保守替代；保留提交点）
// ---------------------------------------------------------------------------

// TestE2EFirstByteTimeoutFailover 上游在提交点前挂住（headers 已到、零产出）：
// first_byte_timeout_seconds 到点即中止本尝试（未提交）→ 按 C1 换下一个上游；
// 下游拿到的是第二个上游的完整流。缺省（0）时守卫不生效（下方 off 子测）。
func TestE2EFirstByteTimeoutFailover(t *testing.T) {
	upHang := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done() // 挂住：一个产出字节都不发
	})
	upGood := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}`)
		sseFrame(w, "", `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}`)
		sseFrame(w, "", `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"gpt-x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	})

	cfg := failoverCfg(
		Upstream{Name: "hang", BaseURL: upHang.srv.URL, APIKey: "k1", Protocol: ProtocolOpenAIChat},
		Upstream{Name: "good", BaseURL: upGood.srv.URL, APIKey: "k2", Protocol: ProtocolOpenAIChat},
	)
	cfg.FirstByteTimeoutSeconds = 1
	s, srv := startServiceWith(t, cfg)

	start := time.Now()
	status, raw := postChatStream(t, srv.URL)
	elapsed := time.Since(start)
	if status != 200 {
		t.Fatalf("status = %d\n%s", status, raw)
	}
	if !strings.Contains(raw, `"content":"Hello"`) || !strings.Contains(raw, "data: [DONE]") {
		t.Fatalf("下游不是第二个上游的完整流:\n%s", raw)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("elapsed = %s（首产出超时未生效？）", elapsed)
	}
	if got := upHang.count(); got != 1 {
		t.Fatalf("挂住的尝试次数 = %d, want 1", got)
	}
	if got := upGood.count(); got != 1 {
		t.Fatalf("第二个上游尝试次数 = %d, want 1", got)
	}
	d := s.Diagnostics()
	if d.Attempts != 2 || d.Failovers != 1 {
		t.Fatalf("diagnostics = %+v（want Attempts=2 Failovers=1）", d)
	}
}

// TestE2EFirstByteTimeoutDisabledByDefault 缺省（first_byte_timeout_seconds 未设 /
// 0 = 关）：守卫不介入——首产出慢（2s）的合法上游不被换掉。这是「合法慢上游
// （首产出可达 ~100s，P24/W5-15 决策理由 ②）不被误杀」的锁。
func TestE2EFirstByteTimeoutDisabledByDefault(t *testing.T) {
	upSlow := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		time.Sleep(2 * time.Second) // 首产出慢，但合法
		sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_slow","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":0}}}`)
		sseFrame(w, "content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
		sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"slow-ok"}}`)
		sseFrame(w, "content_block_stop", `{"type":"content_block_stop","index":0}`)
		sseFrame(w, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":3,"output_tokens":2}}`)
		sseFrame(w, "message_stop", `{"type":"message_stop"}`)
	})
	upFast := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_fast","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":0}}}`)
		sseFrame(w, "content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
		sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"fast"}}`)
		sseFrame(w, "content_block_stop", `{"type":"content_block_stop","index":0}`)
		sseFrame(w, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":3,"output_tokens":2}}`)
		sseFrame(w, "message_stop", `{"type":"message_stop"}`)
	})

	cfg := failoverCfg(anthUp("slow", upSlow.srv.URL), anthUp("fast", upFast.srv.URL))
	if cfg.FirstByteTimeoutSeconds != 0 {
		t.Fatalf("缺省必须为 0（关）: %d", cfg.FirstByteTimeoutSeconds)
	}
	s, srv := startServiceWith(t, cfg)

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"stream":true}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "slow-ok") {
		t.Fatalf("慢上游被误换掉（缺省应关守卫）:\n%s", raw)
	}
	d := s.Diagnostics()
	if d.Attempts != 1 || d.Failovers != 0 {
		t.Fatalf("diagnostics = %+v（want Attempts=1 Failovers=0：守卫缺省关）", d)
	}
	if got := upFast.count(); got != 0 {
		t.Fatalf("第二个上游被误触发: %d", got)
	}
}

// ---------------------------------------------------------------------------
// W5-16 ②：StreamPipe.Diagnostics() 在服务壳消费（计数 + 日志一行）
// ---------------------------------------------------------------------------

// TestE2EPipeDiagnosticsConsumed 跨协议流式管道的出口诊断在服务壳收尾被消费：
// 计数进 Diagnostics（W5-11 §剩余面 5 此前无消费方），任一非零打一行日志。
func TestE2EPipeDiagnosticsConsumed(t *testing.T) {
	t.Run("direct_face_reasoning_content_and_dropped_signature", func(t *testing.T) {
		// openai 入口 × claude 上游：直转面（非枢纽）——思考文本走
		// delta.reasoning_content，签名无载体丢弃。
		up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_t1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":0}}}`)
			sseFrame(w, "content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`)
			sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"ponder"}}`)
			sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"SECRETSIG"}}`)
			sseFrame(w, "content_block_stop", `{"type":"content_block_stop","index":0}`)
			sseFrame(w, "content_block_start", `{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`)
			sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Hi"}}`)
			sseFrame(w, "content_block_stop", `{"type":"content_block_stop","index":1}`)
			sseFrame(w, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":3,"output_tokens":2}}`)
			sseFrame(w, "message_stop", `{"type":"message_stop"}`)
		})
		s, srv := startServiceWith(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

		status, raw := postChatStream(t, srv.URL)
		if status != 200 {
			t.Fatalf("status = %d\n%s", status, raw)
		}
		if !strings.Contains(raw, `"reasoning_content":"ponder"`) {
			t.Fatalf("直转面思考文本未走 reasoning_content:\n%s", raw)
		}
		if strings.Contains(raw, "SECRETSIG") {
			t.Fatalf("签名外泄:\n%s", raw)
		}
		d := s.Diagnostics()
		if d.PipeReasoningDeltas != 1 || d.PipeDroppedThinking != 0 || d.PipeSignatureDropped != 1 {
			t.Fatalf("diagnostics = %+v（want PipeReasoningDeltas=1 PipeSignatureDropped=1 PipeDroppedThinking=0）", d)
		}
	})

	t.Run("hub_leg_carries_thinking", func(t *testing.T) {
		// claude 入口 × gemini 上游：枢纽两步（gemini→openai→claude）——W42 ③ 起
		// 中间腿经 reasoning_details 载体保真思考文本，claude 出口落 thinking 块
		//（此前整块丢弃：两步链静默丢思考；载体化后中间线可表达——P53 缺口 2 收口）。
		up := newGeminiUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"ponder","thought":true}]},"index":0}],"responseId":"r1"}`)
			sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[{"text":"Hi"}]},"index":0}],"responseId":"r1"}`)
			sseFrame(w, "", `{"candidates":[{"content":{"role":"model","parts":[]},"finishReason":"STOP","index":0}],"responseId":"r1","usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":2,"totalTokenCount":6}}`)
		})
		s, srv := startServiceWith(t, cfgFor(up.srv.URL, ProtocolGemini, 15))

		resp := postJSON(t, srv.URL+"/v1/messages",
			`{"model":"gemini-2.5-flash","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d\n%s", resp.StatusCode, raw)
		}
		if !strings.Contains(string(raw), "ponder") {
			t.Fatalf("枢纽中间腿丢思考文本（W42 ③ 应经载体到 claude 出口）:\n%s", raw)
		}
		if !strings.Contains(string(raw), `"text":"Hi"`) {
			t.Fatalf("正文丢失:\n%s", raw)
		}
		d := s.Diagnostics()
		if d.PipeDroppedThinking != 0 || d.PipeReasoningDeltas != 1 {
			t.Fatalf("diagnostics = %+v（want PipeDroppedThinking=0 PipeReasoningDeltas=1）", d)
		}
	})

	t.Run("tool_args_repaired_counted_and_logged", func(t *testing.T) {
		// openai 入口 × claude 上游：出口修复层修好半 JSON 参数——计数 +
		// 日志一行（日志出口的锁）。
		up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			sseFrame(w, "message_start", `{"type":"message_start","message":{"id":"msg_t2","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":3,"output_tokens":0}}}`)
			sseFrame(w, "content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"write","input":{}}}`)
			sseFrame(w, "content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"file_path\":\"/tmp/a.txt\",\"content\":\"hello world"}}`)
			sseFrame(w, "content_block_stop", `{"type":"content_block_stop","index":0}`)
			sseFrame(w, "message_delta", `{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"input_tokens":3,"output_tokens":2}}`)
			sseFrame(w, "message_stop", `{"type":"message_stop"}`)
		})
		s, srv := startServiceWith(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))

		var logBuf bytes.Buffer
		prev := log.Writer()
		log.SetOutput(&logBuf)
		defer log.SetOutput(prev)

		status, raw := postChatStream(t, srv.URL)
		if status != 200 {
			t.Fatalf("status = %d\n%s", status, raw)
		}
		if !strings.Contains(raw, `"arguments":"{\"file_path\":\"/tmp/a.txt\",\"content\":\"hello world\"}"`) {
			t.Fatalf("出口修复层未修复参数:\n%s", raw)
		}
		d := s.Diagnostics()
		if d.PipeToolArgsRepaired != 1 {
			t.Fatalf("diagnostics = %+v（want PipeToolArgsRepaired=1）", d)
		}
		if !strings.Contains(logBuf.String(), "stream diagnostics: dropped_thinking=0 reasoning_deltas=0 signature_dropped=0 tool_args_repaired=1 tool_args_failed=0") {
			t.Fatalf("诊断日志行缺失:\n%s", logBuf.String())
		}
	})
}

// TestEntryPathStream 入口路径态流开关判据（gemini 动作后缀；其余入口面恒 false）。
func TestEntryPathStream(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{"/v1beta/models/gemini-2.5-flash:streamGenerateContent", true},
		{"/v1beta/models/gemini-2.5-flash:generateContent", false},
		{"/v1/chat/completions", false},
		{"/v1/messages", false},
		{"/v1/responses", false},
		{"/v1beta/interactions", false},
	}
	for _, tc := range cases {
		if got := entryPathStream(tc.path); got != tc.want {
			t.Fatalf("entryPathStream(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

// TestHandlePathStateStreamSwitch 服务壳接线：流开关来源 = 下游路径 + 请求体。
// 用一个 gemini 动作路径直接调入口处理器（gemini 入口路由本波未注册——路径态
// 语义在此锁死：请求体**没有** stream 字段，仅路径态声明流式 → 服务按流式
// 转发（上游收到 Accept: text/event-stream、下游拿到 SSE），非流式路径则不然。
func TestHandlePathStateStreamSwitch(t *testing.T) {
	var mu sync.Mutex
	var gotAccept string
	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAccept = r.Header.Get("Accept")
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		sseFrame(w, "", `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":"Hi"},"finish_reason":null}]}`)
		sseFrame(w, "", `{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)
	})
	cfg := cfgFor(up.srv.URL, ProtocolOpenAIChat, 15)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	s := New(cfg)

	body := `{"model":"m","messages":[{"role":"user","content":"hi"}]}` // 无 stream 字段
	rec := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodPost, "http://svc/v1beta/models/m:streamGenerateContent?alt=sse", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	s.handle(registry.FormatOpenAIChat)(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d\n%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "data: ") {
		t.Fatalf("路径态声明流式但下游不是 SSE:\n%s", rec.Body.String())
	}
	mu.Lock()
	accept := gotAccept
	mu.Unlock()
	if accept != "text/event-stream" {
		t.Fatalf("上游 Accept = %q（want text/event-stream：路径态流位未接线）", accept)
	}

	// 同一 body 走常规路径（无路径态声明）→ 非流式：上游收到的是 JSON 响应路径
	//（无 Accept: text/event-stream），下游拿到 chat.completion。
	up2 := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"c2","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"Hi"},"finish_reason":"stop"}]}`))
	})
	s2 := New(cfgFor(up2.srv.URL, ProtocolOpenAIChat, 15))
	rec2 := httptest.NewRecorder()
	req2, _ := http.NewRequest(http.MethodPost, "http://svc/v1/chat/completions", strings.NewReader(body))
	s2.handle(registry.FormatOpenAIChat)(rec2, req2)
	if rec2.Code != 200 || !strings.Contains(rec2.Body.String(), `"object":"chat.completion"`) {
		t.Fatalf("非流式路径输出异常: %d\n%s", rec2.Code, rec2.Body.String())
	}
}
