// chatgpt_web 上游面 e2e（W30-2）：匿名 Android 三步链（sentinel → prepare →
// f/conversation SSE）。
//
// 装置：httptest 假 chatgpt_web 上游（三端点；记录全部调用）+ 真服务
// （New(cfg).Handler() 经 httptest.NewServer）+ 真 HTTP 客户端。completion 帧体 =
// chatgpt_web 包 testdata **三份 golden 原样回放**（W29-1 实抓样本入包；不是手写
// 帧形）。
//
// 反证锚点（本文件内，回退即红；原样输出见 tmp/lanes/W30-s3/proofs/ 与
// tmp/lanes/W32-chatgpt-final/proofs/）：
//   - 删 forward 的 chatgpt_web prepare 分支 → TestE2EChatGPTWebUpstreamStreaming
//     在三端点调用序处红（path 空 → PathFor 错误 → 400）；
//   - 删 relay 的 chatgpt_web 终止帧 break → TestE2EChatGPTWebHeldSocketBounded
//     红（挂到 fake 的 5s 兜底）；
//   - 删尾读宽限（cgTail 武装/触发）→ TestE2EChatGPTWebTailGraceBounded 红
//     （挂到 fake 的 5s 兜底）；
//   - 删 definitive marker 收束（relay 的 DefinitiveMarker break）→
//     TestE2EChatGPTWebMarkerCollects/no_sentinel_grace 红（挂到 fake 的 5s 兜底）；
//   - 回退收束集含 stream_complete（W33-1 前口径）→
//     TestE2EChatGPTWebMarkerCollects/done_sentinel 红（收束在 complete：流末
//     burst 的 detail/[DONE] 不读——done=false/detail=absent）；
//   - 回退 detail 观测面（日志 detail= 字段）→ done_sentinel 红（断言缺字段）；
//   - 回退 marker 登记（recordMarker 空实现）→ TestE2EChatGPTWebTailGraceBounded
//     的观测日志断言红（markers 无 last_token）；
//   - 回退 moderation 出口注入 → TestE2EChatGPTWebModerationExit 红（无
//     content_filter 错误帧/信封）；
//   - 回退渲染面（supported_encodings 改 camelCase / 消息结构改形）→ 请求体断言红。
package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 脚手架：假 chatgpt_web 上游（匿名 Android 面三端点）
// ---------------------------------------------------------------------------

// chatgptWebTestSentinelToken / chatgptWebTestConduit 假上游的固定令牌值
// （形态 = W29-1 实抓：sentinel `gAAAAAB…` / conduit JWT）。
const (
	chatgptWebTestSentinelToken = "gAAAAAB-test-sentinel-token"
	chatgptWebTestConduitToken  = "test-conduit-token-0001"
)

// chatgptWebCall 一次上游调用记录。
type chatgptWebCall struct {
	method  string
	path    string
	headers http.Header
	body    []byte
}

// chatgptWebUpstream 假 chatgpt_web 上游（三端点；记录全部调用）。
type chatgptWebUpstream struct {
	srv *httptest.Server

	mu    sync.Mutex
	calls []chatgptWebCall

	// 可配行为（缺省 = 正常）。
	sentinelStatus int    // 0 → 200
	sentinelBody   string // 空 → 默认 token 体
	prepareStatus  int    // 0 → 200
	prepareBody    string // 空 → 默认 conduit 体
	conduitToken   string // 空 → chatgptWebTestConduitToken
	streamBody     string // completion 的 SSE 体（golden 回放）
	// keepOpenEnd 写完 SSE 后保持连接（模拟真实上游终态后仍握 socket 1.3–3s 的
	// 实况——终止帧收束/尾读宽限的判据面；5s 兜底防测试挂死）。
	keepOpenEnd bool
	// sentinelCheck 非 nil 时：prepare/completion 请求在放行前先过该校验
	// （W42-2 挑战头面——fake 侧用 sentinel.Verify 回验 proof token；返回非 nil
	// → 403，模拟上游拒绝）。sentinel 链首请求不校验（挑战尚未下发）。
	sentinelCheck func(call chatgptWebCall) error
}

func newChatGPTWebUpstream(t *testing.T) *chatgptWebUpstream {
	t.Helper()
	u := &chatgptWebUpstream{}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		c := chatgptWebCall{method: r.Method, path: r.URL.Path, headers: r.Header.Clone(), body: b}
		u.mu.Lock()
		u.calls = append(u.calls, c)
		u.mu.Unlock()
		rejectChallenge := func() bool {
			if u.sentinelCheck == nil {
				return false
			}
			if err := u.sentinelCheck(c); err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(w, `{"detail":"sentinel challenge rejected"}`)
				return true
			}
			return false
		}

		switch r.URL.Path {
		case chatgptWebSentinelPath:
			if u.sentinelStatus >= 400 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(u.sentinelStatus)
				_, _ = io.WriteString(w, `{"detail":"Unauthorized"}`)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			body := u.sentinelBody
			if body == "" {
				body = `{"token":"` + chatgptWebTestSentinelToken + `","expire_after":540,"expire_at":1791982800}`
			}
			_, _ = io.WriteString(w, body)
		case chatgptWebPreparePath:
			if rejectChallenge() {
				return
			}
			if u.prepareStatus >= 400 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(u.prepareStatus)
				_, _ = io.WriteString(w, `{"detail":"prepare rejected"}`)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			body := u.prepareBody
			if body == "" {
				conduit := u.conduitToken
				if conduit == "" {
					conduit = chatgptWebTestConduitToken
				}
				body = `{"status":"ok","conduit_token":"` + conduit + `"}`
			}
			_, _ = io.WriteString(w, body)
		case chatgptWebCompletionPath:
			if rejectChallenge() {
				return
			}
			w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, u.streamBody)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			if u.keepOpenEnd {
				// 真实上游在终态后不主动关闭（W6-2 实测 1.3–3s）——等客户端断开
				// （或兜底 5s，防测试挂死）。
				select {
				case <-r.Context().Done():
				case <-time.After(5 * time.Second):
				}
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(u.srv.Close)
	return u
}

func (u *chatgptWebUpstream) snapshot() []chatgptWebCall {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]chatgptWebCall(nil), u.calls...)
}

// waitForChatGPTWebCalls 有界等待上游调用数达到 want 并返回快照。
func waitForChatGPTWebCalls(t *testing.T, u *chatgptWebUpstream, want int) []chatgptWebCall {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		calls := u.snapshot()
		if len(calls) >= want {
			return calls
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待上游调用数 %d 超时，已见: %v", want, chatgptWebCallPaths(calls))
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func chatgptWebCallPaths(calls []chatgptWebCall) []string {
	out := make([]string, 0, len(calls))
	for _, c := range calls {
		out = append(out, c.method+" "+c.path)
	}
	return out
}

// chatgptWebGolden 读 chatgpt_web 包的 golden SSE（实抓样本回放——原样字节）。
func chatgptWebGolden(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile("../translate/convert/chatgpt_web/testdata/" + name)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	return string(raw)
}

// cfgChatGPTWeb 服务配置：chatgpt_web 上游（匿名面——无 api_key）。
func cfgChatGPTWeb(baseURL, deviceID string) *Config {
	return &Config{
		Listen:           "127.0.0.1:0",
		HeartbeatSeconds: 15,
		Upstreams: []Upstream{{
			Name: "fake-chatgpt-web", BaseURL: baseURL,
			Protocol: ProtocolChatGPTWeb, DeviceID: deviceID,
		}},
	}
}

// sseData 构造一帧 SSE data 载荷（构造流用；golden 回放不用）。
func sseData(payload string) string { return "data: " + payload + "\n\n" }

// chatgptWebGoldenTruncatedAtLastToken ISP golden 截到 last_token 帧为止（含）——
// W31-1 实证的流式截断形态（尾部 title/ste/stream_complete/[DONE] 未到）。
func chatgptWebGoldenTruncatedAtLastToken(t *testing.T) string {
	t.Helper()
	sse := chatgptWebGolden(t, "anon_v1_isp.sse.txt")
	idx := strings.Index(sse, `"marker": "last_token"`)
	if idx < 0 {
		t.Fatalf("golden 不含 last_token marker 帧——测试前提不成立")
	}
	end := strings.Index(sse[idx:], "\n\n")
	if end < 0 {
		t.Fatalf("last_token 帧后无空行——帧边界不可定位")
	}
	return sse[:idx+end+2]
}

// syncLogBuf 并发安全日志缓冲（服务观测面断言用——handler goroutine 写、
// 测试 goroutine 读）。
type syncLogBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *syncLogBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *syncLogBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// captureServiceLogs 捕获标准日志输出（W32-1 观测面断言；package 内测试串行）。
func captureServiceLogs(t *testing.T) *syncLogBuf {
	t.Helper()
	lb := &syncLogBuf{}
	prev := log.Writer()
	log.SetOutput(lb)
	t.Cleanup(func() { log.SetOutput(prev) })
	return lb
}

// waitForLogContains 有界等待日志出现子串（观测行在收尾 defer 写出——响应 EOF
// 之后仍可能差一拍；2s 上限防挂死）。
func waitForLogContains(t *testing.T, lb *syncLogBuf, substr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if strings.Contains(lb.String(), substr) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待日志 %q 超时，已见:\n%s", substr, lb.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// e2e 1：openai 入口 → chatgpt_web 上游（流式；三 golden 回放）
// ---------------------------------------------------------------------------

func TestE2EChatGPTWebUpstreamStreaming(t *testing.T) {
	cases := []struct {
		name     string
		golden   string
		wantText string
	}{
		{"v1_direct", "anon_v1_direct.sse.txt", "1\n2\n3\n4\n5\n6\n7\n8\n9\n10"},
		{"v1_isp", "anon_v1_isp.sse.txt", "PONG"},
		{"snapshot", "anon_snapshot.sse.txt", "PONG"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := newChatGPTWebUpstream(t)
			up.streamBody = chatgptWebGolden(t, tc.golden)
			const deviceID = "11111111-2222-4333-8444-555555555555"
			srv := startService(t, cfgChatGPTWeb(up.srv.URL, deviceID))

			resp := postJSON(t, srv.URL+"/v1/chat/completions",
				`{"model":"gpt-6","messages":[{"role":"user","content":"Count from 1 to 10, one number per line."}],"stream":true}`, nil)
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != 200 {
				t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
			}

			// ---- 上游面 1：三步链调用序（sentinel → prepare → completion）。----
			calls := waitForChatGPTWebCalls(t, up, 3)
			if len(calls) != 3 {
				t.Fatalf("调用数 = %d（want 3：sentinel/prepare/completion）: %v", len(calls), chatgptWebCallPaths(calls))
			}
			wantPaths := []string{chatgptWebSentinelPath, chatgptWebPreparePath, chatgptWebCompletionPath}
			for i, want := range wantPaths {
				if calls[i].method != http.MethodPost || calls[i].path != want {
					t.Fatalf("调用[%d] = %s %s, want POST %s", i, calls[i].method, calls[i].path, want)
				}
			}

			// ---- 上游面 2：头面（三请求同一份——W29-1 §2.1 实抓逐头）。----
			for i, c := range calls {
				if got := c.headers.Get("User-Agent"); got != chatgptWebUA {
					t.Fatalf("调用[%d] User-Agent = %q, want %q", i, got, chatgptWebUA)
				}
				if got := c.headers.Get(headerOAIClientType); got != chatgptWebClientType {
					t.Fatalf("调用[%d] OAI-Client-Type = %q", i, got)
				}
				if got := c.headers.Get(headerOAIPackageName); got != chatgptWebPackageName {
					t.Fatalf("调用[%d] OAI-Package-Name = %q", i, got)
				}
				if got := c.headers.Get(headerOAIDeviceID); got != deviceID {
					t.Fatalf("调用[%d] OAI-Device-Id = %q, want 配置值 %q", i, got, deviceID)
				}
				if got := c.headers.Get(headerOAINoHTTPLogging); got != "1" {
					t.Fatalf("调用[%d] X-OpenAI-No-Http-Logging = %q", i, got)
				}
				if got := c.headers.Get("Accept"); got != "text/event-stream" {
					t.Fatalf("调用[%d] Accept = %q, want text/event-stream", i, got)
				}
				if got := c.headers.Get("Content-Type"); got != "application/json" {
					t.Fatalf("调用[%d] Content-Type = %q", i, got)
				}
			}
			// sentinel：空体 `{}`、无令牌头（链首）。
			if string(calls[0].body) != "{}" {
				t.Fatalf("sentinel 体 = %q, want {}", calls[0].body)
			}
			if calls[0].headers.Get(headerSentinelToken) != "" || calls[0].headers.Get(headerConduitToken) != "" {
				t.Fatalf("sentinel 不得带令牌头: %v", calls[0].headers)
			}
			// prepare：sentinel 令牌 + `x-conduit-token: no-token`（占位）。
			if got := calls[1].headers.Get(headerSentinelToken); got != chatgptWebTestSentinelToken {
				t.Fatalf("prepare sentinel 令牌 = %q", got)
			}
			if got := calls[1].headers.Get(headerConduitToken); got != chatgptWebConduitNoToken {
				t.Fatalf("prepare x-conduit-token = %q, want no-token", got)
			}
			// completion：sentinel 令牌 + prepare 返回的 conduit 令牌。
			if got := calls[2].headers.Get(headerSentinelToken); got != chatgptWebTestSentinelToken {
				t.Fatalf("completion sentinel 令牌 = %q", got)
			}
			if got := calls[2].headers.Get(headerConduitToken); got != chatgptWebTestConduitToken {
				t.Fatalf("completion x-conduit-token = %q, want prepare 返回值", got)
			}

			// ---- 上游面 3：请求体（W29-1 §2.1 样本形状；prepare 与 completion 同体）。----
			if string(calls[1].body) != string(calls[2].body) {
				t.Fatalf("prepare 体与 completion 体必须同体（W29-1 样本同字节）:\nprepare=%s\nconv=%s", calls[1].body, calls[2].body)
			}
			var body struct {
				Action   string `json:"action"`
				Messages []struct {
					Author struct {
						Role string `json:"role"`
					} `json:"author"`
					Content struct {
						ContentType string   `json:"content_type"`
						Parts       []string `json:"parts"`
					} `json:"content"`
				} `json:"messages"`
				ParentMessageID    *string  `json:"parentMessageId"`
				ConversationID     *string  `json:"conversationId"`
				Model              string   `json:"model"`
				ForceUseSse        bool     `json:"forceUseSse"`
				SupportedEncodings []string `json:"supported_encodings"`
				Timezone           string   `json:"timezone"`
			}
			if err := json.Unmarshal(calls[1].body, &body); err != nil {
				t.Fatalf("请求体: %v\n%s", err, calls[1].body)
			}
			if body.Action != "next" || body.ParentMessageID != nil || body.ConversationID != nil {
				t.Fatalf("请求体顶层 = %+v", body)
			}
			if body.Model != "gpt-6" || !body.ForceUseSse || body.Timezone != "UTC" {
				t.Fatalf("请求体字段 = %+v", body)
			}
			if len(body.SupportedEncodings) != 1 || body.SupportedEncodings[0] != "v1" {
				t.Fatalf("supported_encodings = %v（want [\"v1\"]——snake_case 才生效，W29-1 实证）", body.SupportedEncodings)
			}
			if len(body.Messages) != 1 || body.Messages[0].Author.Role != "user" ||
				body.Messages[0].Content.ContentType != "text" ||
				len(body.Messages[0].Content.Parts) != 1 ||
				body.Messages[0].Content.Parts[0] != "Count from 1 to 10, one number per line." {
				t.Fatalf("messages = %+v", body.Messages)
			}
			// 体不得带 camelCase 死字段（W29-1：被服务端忽略）。
			if strings.Contains(string(calls[1].body), "supportedEncodings") {
				t.Fatalf("请求体不得带 camelCase 死字段 supportedEncodings:\n%s", calls[1].body)
			}

			// ---- 下游面：openai chat SSE（正文 + finish_reason + [DONE]）。----
			text, finish, _, done := parseOpenAIStream(t, raw)
			if text != tc.wantText {
				t.Fatalf("下游正文 = %q, want %q", text, tc.wantText)
			}
			if finish != "stop" {
				t.Fatalf("下游 finish_reason = %q, want stop", finish)
			}
			if !done {
				t.Fatalf("下游流未以 [DONE] 收尾:\n%s", raw)
			}
			// 抑制面：隐藏 system 消息/历史回显/recap 绝不泄漏到下游。
			for _, leak := range []string{"rebase_system_message", "Worked for a second", "reasoning_recap"} {
				if strings.Contains(string(raw), leak) {
					t.Fatalf("下游泄漏上游内部帧内容 %q:\n%s", leak, raw)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// e2e 1b（W94-1）：上游流内 error 帧 → attempt / admin ring 归因（failed +
// end_reason=upstream_error + 未交付），下游保留内容并收到转换后的错误帧
// ---------------------------------------------------------------------------

// TestE2EChatGPTWebUpstreamErrorFrameMarksFailed W94-1：chatgpt_web completion
// 流里的 `{"error":...}` 帧（HTTP 200 下的流内错误，W91-1 判据）必须**同时**
// 满足两面：
//   - 字节面：错误帧经 chatgpt_web → openai_chat 转换后照常写出，前面的内容帧
//     保留；错误帧即终态，不补 `[DONE]`；
//   - 归因面：attempt 与 admin ring 均落 failed=true / delivered=false /
//     end_reason(Outcome)=upstream_error。
//
// 为什么 detector 单测不够：TestDirectFrameTerminalWebErrorFrames 只证明
// directFrameTerminal 返回 errorFrame=true，不证明 relayStream 的 pipe != nil
// 分支真的把它接到 ledger/ring（W90-1 之前跨协议分支只挂 Feed 错误，合法错误
// 事件永不触发，落 completed/delivered=true）。本测试走真 serve 收尾路径。
//
// 帧位：golden 原样回放（实抓样本 `anon_v1_isp.sse.txt`——v1 patch 档，内容
// append 帧与终态补丁帧分离），流内 error 帧拼在**内容帧之后、终态补丁帧之前**
// （补丁 = /message/status finished_successfully + end_turn=true——解析器
// terminalSeen 置位处；快照档 golden 的内容与终态同帧，拼不进）。error 之后
// 不再消费（relay 在错误帧处收束）。不手写畸形帧。
//
// 反证（已演练）：删掉 directFrameTerminal 的 FormatChatGPTWeb errorFrame 分支 →
// 下游字节面仍绿，attempt/ring 在 failed/delivered/end_reason 断言处红。
func TestE2EChatGPTWebUpstreamErrorFrameMarksFailed(t *testing.T) {
	logBuf := captureServiceLogs(t)

	up := newChatGPTWebUpstream(t)
	// 流内错误帧（chatgpt 面形态：data-only，载荷带非 null error；W91-1 判据）。
	const errorFrame = `data: {"error":{"message":"busy","type":"server_error"}}` + "\n\n"
	golden := chatgptWebGolden(t, "anon_v1_isp.sse.txt")
	at := strings.Index(golden, `event: delta`+"\n"+`data: {"p": "", "o": "patch", "v": [{"p": "/message/status"`)
	if at < 0 {
		t.Fatalf("golden 缺终态补丁帧——测试前提不成立:\n%s", golden)
	}
	up.streamBody = golden[:at] + errorFrame + golden[at:]
	s, srv := startServiceWith(t, cfgChatGPTWeb(up.srv.URL, "11111111-2222-4333-8444-555555555555"))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-6","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	body := string(raw)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("下游 status = %d（want 200：错误帧在已提交之后）body=%s", resp.StatusCode, body)
	}

	// 字节面：内容保留 + 转换后的 openai 错误帧（message/type 字面保真）；错误帧
	// 即终态，不补 [DONE]。
	text, _, _, done := parseOpenAIStream(t, raw)
	if text != "PONG" {
		t.Fatalf("下游内容 = %q, want PONG（内容帧不得被错误帧吞掉）:\n%s", text, body)
	}
	for _, want := range []string{`"error"`, `"busy"`, `"server_error"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("下游缺转换后的错误信号 %q:\n%s", want, body)
		}
	}
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(line, `"error"`) {
			t.Logf("downstream error frame: %s", line)
			break
		}
	}
	if done {
		t.Fatalf("错误帧后不得补 [DONE]（假成功）:\n%s", body)
	}

	// 归因面 ①：per-request attempt 记录（attempts.go 的收尾日志行）。
	waitFor(t, 2*time.Second, func() bool { return strings.Contains(logBuf.String(), `"end_reason"`) })
	rec := parseRequestRecord(t, logBuf.String())
	if len(rec.Attempts) != 1 {
		t.Fatalf("attempt 数 = %d（want 1：流内错误帧不重试/不换台）: %+v", len(rec.Attempts), rec.Attempts)
	}
	a := rec.Attempts[0]
	if a.Status != http.StatusOK || !a.Failed || a.Delivered || a.EndReason != endReasonUpstreamError {
		t.Fatalf("attempt = %+v（want status=200 failed=true delivered=false end_reason=%q）",
			a, endReasonUpstreamError)
	}

	// 归因面 ②：admin 请求 ring（同一口径）。
	rid := resp.Header.Get("request-id")
	if rid == "" || rid != rec.Req {
		t.Fatalf("响应 request-id = %q / 记账 req = %q（want 同源非空）", rid, rec.Req)
	}
	waitFor(t, 2*time.Second, func() bool { _, ok := s.adminRequests.find(rid); return ok })
	row, ok := s.adminRequests.find(rid)
	if !ok {
		t.Fatalf("ring 缺请求行 %q（size=%d）", rid, s.adminRequests.size())
	}
	if row.Status != http.StatusOK || !row.Failed || row.Delivered || row.Outcome != endReasonUpstreamError ||
		!row.Stream || row.Attempts != 1 {
		t.Fatalf("ring 行 = %+v（want status=200 failed=true delivered=false outcome=%q stream=true attempts=1）",
			row, endReasonUpstreamError)
	}
	t.Logf("attempt: status=%d failed=%v delivered=%v end_reason=%s; ring: status=%d failed=%v delivered=%v outcome=%s",
		a.Status, a.Failed, a.Delivered, a.EndReason, row.Status, row.Failed, row.Delivered, row.Outcome)
}

// ---------------------------------------------------------------------------
// e2e 2：终态后上游握 socket（不关流）——终止帧收束（有界）
// ---------------------------------------------------------------------------

func TestE2EChatGPTWebHeldSocketBounded(t *testing.T) {
	up := newChatGPTWebUpstream(t)
	up.streamBody = chatgptWebGolden(t, "anon_v1_direct.sse.txt")
	up.keepOpenEnd = true // 终态后握 socket 5s（模拟实况 1.3–3s）
	srv := startService(t, cfgChatGPTWeb(up.srv.URL, ""))

	start := time.Now()
	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-6","messages":[{"role":"user","content":"Count from 1 to 10, one number per line."}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	elapsed := time.Since(start)

	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}
	text, _, _, done := parseOpenAIStream(t, raw)
	if text != "1\n2\n3\n4\n5\n6\n7\n8\n9\n10" || !done {
		t.Fatalf("下游 = %q done=%v", text, done)
	}
	// definitive marker（stream_complete/[DONE]）收束：不等 socket 关闭。上限 3s
	// 是宽松界——回退 break 会挂到 fake 的 5s 兜底（回退即红）。
	if elapsed > 3*time.Second {
		t.Fatalf("握 socket 场景耗时 %v（want < 3s——definitive marker 应即刻收束，不等连接关闭）", elapsed)
	}
}

// ---------------------------------------------------------------------------
// e2e 3a：definitive marker（message_stream_complete）即收束——不等宽限（W32-1）
// ---------------------------------------------------------------------------

// TestE2EChatGPTWebMarkerCollects W33-1 收束判据（裁定收窄后）：流式面收束于
// **流末哨兵**（[DONE]）——message_stream_complete 后不即刻收束，流末 burst
// （detail/[DONE]）随哨兵一并捕获（done=true；direct 档 detail 可见），**不等
// 3s 宽限**；无哨兵的变体流（complete 后上游静默）回落有界宽限（3s——与无
// marker 流同一条规则）。回退收束集含 complete（W33-1 前口径）→ 两子测各红
// 一头（done_sentinel：detail/done 断言红；no_sentinel_grace：下界红）。
func TestE2EChatGPTWebMarkerCollects(t *testing.T) {
	t.Run("done_sentinel", func(t *testing.T) {
		logs := captureServiceLogs(t)
		// 全尾 direct golden（末帧 [DONE]；detail 在 complete 之后）+ 上游握 socket。
		up := newChatGPTWebUpstream(t)
		up.streamBody = chatgptWebGolden(t, "anon_v1_direct.sse.txt")
		up.keepOpenEnd = true
		srv := startService(t, cfgChatGPTWeb(up.srv.URL, ""))

		start := time.Now()
		resp := postJSON(t, srv.URL+"/v1/chat/completions",
			`{"model":"gpt-6","messages":[{"role":"user","content":"Count from 1 to 10, one number per line."}],"stream":true}`, nil)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		elapsed := time.Since(start)

		if resp.StatusCode != 200 {
			t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
		}
		text, finish, _, done := parseOpenAIStream(t, raw)
		if text != "1\n2\n3\n4\n5\n6\n7\n8\n9\n10" || finish != "stop" || !done {
			t.Fatalf("下游 = %q finish=%q done=%v", text, finish, done)
		}
		// 流末哨兵即收束：远小于 3s 宽限（回退收束集含 complete → 收束在
		// complete → done=false/detail 不读，本组断言红）。
		if elapsed > 1500*time.Millisecond {
			t.Fatalf("哨兵收束耗时 %v（want < 1.5s——[DONE] 应即刻收束，不等宽限）", elapsed)
		}
		// 尾帧观测面（W33-1 裁定：流式面尾覆盖 = 非流式面）：complete + 流末
		// 哨兵 + detail（complete 之后的流末 burst）全收。
		waitForLogContains(t, logs, "stream_complete=true done=true detail=present(256B)")
		if !strings.Contains(logs.String(), `title="Count to ten"`) {
			t.Fatalf("观测日志缺 title（尾部帧未收进观测面）:\n%s", logs.String())
		}
		if !strings.Contains(logs.String(), "markers=user_visible_token,final_channel_token,last_token") {
			t.Fatalf("观测日志缺 marker 登记:\n%s", logs.String())
		}
	})

	t.Run("no_sentinel_grace", func(t *testing.T) {
		logs := captureServiceLogs(t)
		golden := chatgptWebGolden(t, "anon_v1_isp.sse.txt")
		// 砍掉末帧 `data: [DONE]`（变体流：stream_complete 后不再有哨兵）+ 上游握 socket。
		trimmed := strings.Replace(golden, "data: [DONE]\n", "", 1)
		if trimmed == golden {
			t.Fatalf("golden 不含 [DONE] 末帧——测试前提不成立")
		}
		up := newChatGPTWebUpstream(t)
		up.streamBody = trimmed
		up.keepOpenEnd = true
		srv := startService(t, cfgChatGPTWeb(up.srv.URL, ""))

		start := time.Now()
		resp := postJSON(t, srv.URL+"/v1/chat/completions",
			`{"model":"gpt-6","messages":[{"role":"user","content":"Reply with exactly: PONG"}],"stream":true}`, nil)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		elapsed := time.Since(start)

		if resp.StatusCode != 200 {
			t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
		}
		// 下游零影响：finish+[DONE] 在终态帧即写出（宽限只延长连接保持）。
		text, finish, _, done := parseOpenAIStream(t, raw)
		if text != "PONG" || finish != "stop" || !done {
			t.Fatalf("下游 = %q finish=%q done=%v", text, finish, done)
		}
		// complete 后无哨兵 + 上游静默：有界宽限（3s）到期收束——不即刻
		// （回退收束集含 complete → 即刻收束 → 下界红）、不挂到 fake 的 5s 兜底。
		if elapsed < 2500*time.Millisecond || elapsed > 4800*time.Millisecond {
			t.Fatalf("无哨兵变体收束耗时 %v（want 2.5s–4.8s——有界宽限 3s 到期收束）", elapsed)
		}
		// 截断面：complete/title/markers 已收进，流末哨兵未到、detail 未到。
		waitForLogContains(t, logs, "stream_complete=true done=false detail=absent")
		if !strings.Contains(logs.String(), `title="Reply PONG"`) {
			t.Fatalf("观测日志缺 title（尾部帧未收进观测面）:\n%s", logs.String())
		}
		if !strings.Contains(logs.String(), "markers=cot_token,user_visible_token,final_channel_token,last_token") {
			t.Fatalf("观测日志缺 marker 登记:\n%s", logs.String())
		}
	})
}

// ---------------------------------------------------------------------------
// e2e 3b：无 definitive marker + 上游握 socket——有界宽限（3s）收束（W32-1）
// ---------------------------------------------------------------------------

// TestE2EChatGPTWebTailGraceBounded 无 marker 终态（status finished 已见、
// stream_complete/[DONE] 未到——W31-1 实证流式 4/4 的真实截断形态：止于
// last_token）+ 上游握 socket：**有界宽限（3s）**到期收束——不即刻（回退成
// 250ms 级 → 下界红）、不挂到 fake 兜底（宽限失效 → 上界红）。截断回归：
// last_token 必须进观测面（accessor 可见——recordMarker 回退即红）。
func TestE2EChatGPTWebTailGraceBounded(t *testing.T) {
	logs := captureServiceLogs(t)
	up := newChatGPTWebUpstream(t)
	up.streamBody = chatgptWebGoldenTruncatedAtLastToken(t)
	up.keepOpenEnd = true // 握 socket 5s：宽限 3s 到期收束（不等连接关闭）
	srv := startService(t, cfgChatGPTWeb(up.srv.URL, ""))

	start := time.Now()
	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-6","messages":[{"role":"user","content":"Reply with exactly: PONG"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	elapsed := time.Since(start)

	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}
	// 下游零影响：finish+[DONE] 在终态帧即写出（宽限只延长连接保持）。
	text, finish, _, done := parseOpenAIStream(t, raw)
	if text != "PONG" || finish != "stop" || !done {
		t.Fatalf("下游 = %q finish=%q done=%v", text, finish, done)
	}
	// 有界宽限 3s：≥2.5s（不是即刻收束）、≤4.8s（不是 fake 的 5s 兜底）。
	if elapsed < 2500*time.Millisecond || elapsed > 4800*time.Millisecond {
		t.Fatalf("宽限收束耗时 %v（want 2.5s–4.8s——有界宽限 3s 到期收束）", elapsed)
	}
	// 截断回归（accessor 可见）：last_token 登记 + definitive marker 未到。
	waitForLogContains(t, logs, "last_token")
	logsAll := logs.String()
	if !strings.Contains(logsAll, "stream_complete=false done=false") {
		t.Fatalf("观测日志 definitive marker 面异常（want stream_complete=false done=false）:\n%s", logsAll)
	}
	if !strings.Contains(logsAll, `title=""`) {
		t.Fatalf("观测日志 title 应为空（title_generation 在截断点后——W31-1 同款观测）:\n%s", logsAll)
	}
	// 证据面：把截断流的观测行原样打出来（报告 §5 引用；-v 可见）。
	t.Logf("截断流观测行: %s", strings.TrimSpace(logsAll))
}

// ---------------------------------------------------------------------------
// e2e 3c：moderation 拦截出口（S6 出口面 W32-1）——blocked=true → content_filter
// 错误帧/信封；真机 moderation 流未采样（条件/形态为工作裁定 [UNVERIFIED]）
// ---------------------------------------------------------------------------

func TestE2EChatGPTWebModerationExit(t *testing.T) {
	t.Run("committed", func(t *testing.T) {
		// 正文已提交后 moderation 拦截：下游 = 正文 + 流内错误帧（无 [DONE]——
		// W5-26 纪律：错误帧即终态，不补哨兵）。
		up := newChatGPTWebUpstream(t)
		up.streamBody = sseData(`{"v": {"message": {"id": "m1", "author": {"role": "assistant"}, "recipient": "all", "channel": "final", "content": {"content_type": "text", "parts": [""]}, "status": "in_progress"}, "conversation_id": "c1"}}`) +
			sseData(`{"p": "/message/content/parts/0", "o": "append", "v": "partial answer"}`) +
			sseData(`{"type": "moderation", "moderation_response": {"blocked": true}}`)
		srv := startService(t, cfgChatGPTWeb(up.srv.URL, ""))

		resp := postJSON(t, srv.URL+"/v1/chat/completions",
			`{"model":"gpt-6","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
		}
		body := string(raw)
		if !strings.Contains(body, "partial answer") {
			t.Fatalf("下游缺已提交正文:\n%s", body)
		}
		if !strings.Contains(body, "content_filter") || !strings.Contains(body, chatgptWebModerationMessage) {
			t.Fatalf("下游缺 moderation 错误帧（content_filter）:\n%s", body)
		}
		if strings.Contains(body, "[DONE]") {
			t.Fatalf("错误帧后不得补 [DONE]（W5-26 纪律）:\n%s", body)
		}
	})

	t.Run("pre-commit", func(t *testing.T) {
		// 首帧即 moderation 拦截（未提交）：真实状态码信封（403）。
		up := newChatGPTWebUpstream(t)
		up.streamBody = sseData(`{"type": "moderation", "moderation_response": {"blocked": true}}`)
		srv := startService(t, cfgChatGPTWeb(up.srv.URL, ""))

		resp := postJSON(t, srv.URL+"/v1/chat/completions",
			`{"model":"gpt-6","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status=%d（want 403——未提交 moderation 走真实状态码信封）body=\n%s", resp.StatusCode, raw)
		}
		if !strings.Contains(string(raw), "content_filter") || !strings.Contains(string(raw), chatgptWebModerationMessage) {
			t.Fatalf("错误信封缺 content_filter:\n%s", raw)
		}
	})
}

// ---------------------------------------------------------------------------
// e2e 3d：S6 出口观测渲染（tool 记录面 W32-1）——记录摘要进观测日志、下游零泄漏
// ---------------------------------------------------------------------------

// TestE2EChatGPTWebToolRecordObservability tool 记录面（recipient != all 的抑制
// 消息 + 图片指针）：**下游无载体**（抑制是解析判据——绝不进正文），出口观测
// 渲染 = 记录摘要（recipient 分桶 + 图片数）进日志行（S6 深研/附件校准面）。
func TestE2EChatGPTWebToolRecordObservability(t *testing.T) {
	logs := captureServiceLogs(t)
	up := newChatGPTWebUpstream(t)
	up.streamBody = sseData(`{"v": {"message": {"id": "m-tool", "author": {"role": "assistant"}, "recipient": "python", "content": {"content_type": "text", "parts": ["print('hi')", {"asset_pointer": "file-service://file_abc"}]}, "status": "in_progress", "metadata": {"initial_text": "Running python"}}, "conversation_id": "c1"}}`) +
		sseData(`{"v": {"message": {"id": "m-live", "author": {"role": "assistant"}, "recipient": "all", "channel": "final", "content": {"content_type": "text", "parts": [""]}, "status": "in_progress"}, "conversation_id": "c1"}}`) +
		sseData(`{"p": "/message/content/parts/0", "o": "append", "v": "OK"}`) +
		sseData(`{"p": "", "o": "patch", "v": [{"p": "/message/status", "o": "replace", "v": "finished_successfully"}, {"p": "/message/end_turn", "o": "replace", "v": true}]}`) +
		sseData(`[DONE]`)
	srv := startService(t, cfgChatGPTWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-6","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}
	text, _, _, done := parseOpenAIStream(t, raw)
	if text != "OK" || !done {
		t.Fatalf("下游 = %q done=%v", text, done)
	}
	// 抑制纪律：工具消息文本/图片指针绝不泄漏到下游。
	for _, leak := range []string{"print('hi')", "file_abc", "Running python"} {
		if strings.Contains(string(raw), leak) {
			t.Fatalf("下游泄漏工具记录 %q:\n%s", leak, raw)
		}
	}
	// S6 出口观测渲染：记录摘要（recipient 分桶 + 图片数）。
	waitForLogContains(t, logs, "tool_records=1(python×1,images=1)")
	// definitive marker 收束面（[DONE]——本构造流无 stream_complete）。
	waitForLogContains(t, logs, "stream_complete=false done=true")
}

// ---------------------------------------------------------------------------
// e2e 4：openai 入口（stream:false）→ chatgpt_web 上游（上游仍 SSE，有界尾读 +
// ParseResponse 装配）
// ---------------------------------------------------------------------------

func TestE2EChatGPTWebUpstreamNonStreaming(t *testing.T) {
	up := newChatGPTWebUpstream(t)
	up.streamBody = chatgptWebGolden(t, "anon_v1_direct.sse.txt")
	up.keepOpenEnd = true // 非流式路径同样不得等 socket 关闭
	srv := startService(t, cfgChatGPTWeb(up.srv.URL, ""))

	start := time.Now()
	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-6","messages":[{"role":"user","content":"Count from 1 to 10, one number per line."}]}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	elapsed := time.Since(start)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("content-type = %q, want application/json", ct)
	}
	var out struct {
		Object  string `json:"object"`
		Choices []struct {
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("响应体: %v\n%s", err, raw)
	}
	if len(out.Choices) != 1 || out.Choices[0].Message.Content != "1\n2\n3\n4\n5\n6\n7\n8\n9\n10" ||
		out.Choices[0].FinishReason != "stop" {
		t.Fatalf("响应 = %s", raw)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("非流式路径耗时 %v（want < 3s——有界尾读在终止帧停读，不等 socket 关闭）", elapsed)
	}
}

// ---------------------------------------------------------------------------
// e2e 5：链首失败（sentinel 401）→ 下游错误信封，绝不打到 completion
// ---------------------------------------------------------------------------

func TestE2EChatGPTWebSentinelFailure(t *testing.T) {
	up := newChatGPTWebUpstream(t)
	up.sentinelStatus = http.StatusUnauthorized
	srv := startService(t, cfgChatGPTWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"gpt-6","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 400 {
		t.Fatalf("status=%d（want >= 400——链首失败必须显式报错）body=\n%s", resp.StatusCode, raw)
	}
	// 错误信封是转换层产物（绝不透传上游原文结构——上游体是 {"detail":...}）。
	var env struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &env); err != nil || env.Error.Message == "" {
		t.Fatalf("错误信封 = %s（err=%v）", raw, err)
	}
	// 只打到 sentinel：prepare/completion 绝不发（预提交失败）。
	calls := waitForChatGPTWebCalls(t, up, 1)
	if len(calls) != 1 || calls[0].path != chatgptWebSentinelPath {
		t.Fatalf("调用 = %v（want 仅 sentinel）", chatgptWebCallPaths(calls))
	}
	time.Sleep(50 * time.Millisecond)
	if calls := up.snapshot(); len(calls) != 1 {
		t.Fatalf("链首失败后仍打了后续端点: %v", chatgptWebCallPaths(calls))
	}
}

// ---------------------------------------------------------------------------
// 配置面：device_id 缺省（每请求现生成）与校验
// ---------------------------------------------------------------------------

func TestE2EChatGPTWebDeviceIDGeneratedPerRequest(t *testing.T) {
	up := newChatGPTWebUpstream(t)
	up.streamBody = chatgptWebGolden(t, "anon_snapshot.sse.txt")
	srv := startService(t, cfgChatGPTWeb(up.srv.URL, "")) // device_id 缺省

	for i := range 2 {
		resp := postJSON(t, srv.URL+"/v1/chat/completions",
			`{"model":"gpt-6","messages":[{"role":"user","content":"Reply with exactly: PONG"}],"stream":true}`, nil)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("第 %d 发: status=%d body=\n%s", i, resp.StatusCode, raw)
		}
	}
	calls := waitForChatGPTWebCalls(t, up, 6)
	seen := map[string]bool{}
	for i, c := range calls {
		id := c.headers.Get(headerOAIDeviceID)
		if !isUUIDv4(id) {
			t.Fatalf("调用[%d] OAI-Device-Id = %q（want uuid v4——缺省现生成）", i, id)
		}
		seen[id] = true
	}
	if len(seen) != 6 {
		t.Fatalf("device id 去重数 = %d（want 6——每请求现生成、逐请求新设备）", len(seen))
	}
}

func TestConfigChatGPTWebValidation(t *testing.T) {
	// 合法：base_url 缺省（https://android.chat.openai.com）+ device_id 可选。
	cfg := &Config{Upstreams: []Upstream{{Name: "cg", Protocol: ProtocolChatGPTWeb}}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("chatgpt_web 应允许空 base_url/device_id: %v", err)
	}
	// Protocol.Format 映射 + PathFor 显式报错（prepare 链前置）。
	if got := ProtocolChatGPTWeb.Format(); got != "chatgpt_web" {
		t.Fatalf("Format() = %q", got)
	}
	if _, err := ProtocolChatGPTWeb.PathFor("m", true); err == nil || !strings.Contains(err.Error(), "prepare chain") {
		t.Fatalf("PathFor err = %v（want 显式指向 chatgptWebPrepare）", err)
	}
	// device_id 非法形态：CR/LF、空白、超长 → 显式报错。
	for _, bad := range []string{"bad\r\nid", "bad id", strings.Repeat("a", chatgptWebDeviceIDMaxLen+1)} {
		cfg := &Config{Upstreams: []Upstream{{Name: "cg", Protocol: ProtocolChatGPTWeb, DeviceID: bad}}}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("device_id %q 应被拒绝", bad)
		}
	}
	// 合法 device_id（uuid 形态）通过。
	cfg = &Config{Upstreams: []Upstream{{Name: "cg", Protocol: ProtocolChatGPTWeb, DeviceID: "11111111-2222-4333-8444-555555555555"}}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("合法 device_id 被拒: %v", err)
	}
	// 未知协议仍被拒（错误消息含新协议名——人工可读）。
	cfg = &Config{Upstreams: []Upstream{{Name: "x", Protocol: "chatgpt", BaseURL: "http://x"}}}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "chatgpt_web") {
		t.Fatalf("未知协议 err = %v", err)
	}
}

// ---------------------------------------------------------------------------
// W53-1 实机 CF/TLS 对照探针（env 门控：A2A_W53_LIVE=1 才运行；缺省 skip）
// ---------------------------------------------------------------------------

// w53ProbeCase 一个对照案：出站 TLS 形态 × 主机 × 头面。
type w53ProbeCase struct {
	name  string
	mode  string // "" = 缺省 none（共享 client：crypto/tls + Go h2）| "chrome"（uTLS Chrome ClientHello + 真 h2）
	host  string
	full  bool        // true = 服务真实装配（applyChatGPTWebHeaders 全身份头面）
	extra []w53Header // full=false 时在 UA+Content-Type 之上追加的头（逐头分离）
}

// w53Header 逐头分离用的一对头。
type w53Header struct{ k, v string }

// w53ProbeResult 单案结果（落 summary.json；也是证据 txt 的机读面）。
type w53ProbeResult struct {
	Name        string `json:"name"`
	Mode        string `json:"mode"`
	Host        string `json:"host"`
	HeaderFace  string `json:"header_face"`
	Status      int    `json:"status"`
	Proto       string `json:"proto"`
	TLS         string `json:"tls"`
	CFMitigated string `json:"cf_mitigated"`
	BodyLen     int    `json:"body_len"`
	BodySHA256  string `json:"body_sha256"`
	Prefix      string `json:"body_prefix"`
	StartedAt   string `json:"started_at"`
	DurationMS  int64  `json:"duration_ms"`
	Err         string `json:"error,omitempty"`
}

// TestW53LiveCFProbe W53-1 §Goal① 实机对照实验：**同一匿名 sentinel 请求**（体
// `{}`）在两种出站 TLS 形态下的字节级对照（server 实机；本地/CI 不设 env 即 skip
// ——常规 go test 不受影响）：
//
//	none（缺省）  共享 client → crypto/tls ClientHello + Go net/http（h2）
//	chrome        newUpstreamClient → uTLS HelloChrome_Auto + x/net/http2
//
// 十案矩阵：
//
//	A-F  {std, chrome} × {chatgpt.com, android.chat.openai.com} × 服务全身份头面
//	     + {std, chrome} × chatgpt.com × 仅 UA 最小面；
//	G-J  chatgpt.com + std，逐头分离（Client-Type / Accept / 全 OAI 无 Accept /
//	     Client-Type+Accept）——判定 CF 挑战的触发元素。
//
// 每案原始字节（状态行/全头/体）落 `<dir>/<case>.txt` + `<dir>/<case>.body.bin`，
// 汇总 `<dir>/summary.json`。
//
// 反证锚点（回退即失效）：B/D/F 案 `TLSFingerprint` 回退为 ""（none）→ 与
// A/C/E 案同形（结构断言 `client == s.client` 先红）；A/C/E 案若被改成 chrome →
// 结构断言红。判别力即来自该开关——两档结果分化 = 实验有效。
func TestW53LiveCFProbe(t *testing.T) {
	if os.Getenv("A2A_W53_LIVE") != "1" {
		t.Skip("live CF/TLS probe — set A2A_W53_LIVE=1 + A2A_W53_EVIDENCE_DIR (server)")
	}
	dir := os.Getenv("A2A_W53_EVIDENCE_DIR")
	if dir == "" {
		t.Fatalf("A2A_W53_EVIDENCE_DIR 未设置（证据目录）")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("evidence dir: %v", err)
	}
	devID, err := newUUIDv4()
	if err != nil {
		t.Fatalf("device id: %v", err)
	}
	cases := []w53ProbeCase{
		{"A-std-android-chatgpt", "", "chatgpt.com", true, nil},
		{"B-chrome-android-chatgpt", "chrome", "chatgpt.com", true, nil},
		{"C-std-android-androidhost", "", "android.chat.openai.com", true, nil},
		{"D-chrome-android-androidhost", "chrome", "android.chat.openai.com", true, nil},
		{"E-std-uaonly-chatgpt", "", "chatgpt.com", false, nil},
		{"F-chrome-uaonly-chatgpt", "chrome", "chatgpt.com", false, nil},
		{"G-std-clienttype-chatgpt", "", "chatgpt.com", false, []w53Header{{headerOAIClientType, chatgptWebClientType}}},
		{"H-std-accept-chatgpt", "", "chatgpt.com", false, []w53Header{{"Accept", "text/event-stream"}}},
		{"I-std-identity-noaccept-chatgpt", "", "chatgpt.com", false, []w53Header{
			{headerOAIClientType, chatgptWebClientType},
			{headerOAIDeviceID, devID},
			{headerOAIPackageName, chatgptWebPackageName},
			{headerOAINoHTTPLogging, "1"},
		}},
		{"J-std-clienttype-accept-chatgpt", "", "chatgpt.com", false, []w53Header{
			{headerOAIClientType, chatgptWebClientType},
			{"Accept", "text/event-stream"},
		}},
	}
	var results []w53ProbeResult
	for _, c := range cases {
		up := Upstream{Name: "w53-probe", BaseURL: "https://" + c.host, Protocol: ProtocolChatGPTWeb, TLSFingerprint: c.mode}
		cfg := &Config{Listen: "127.0.0.1:0", Upstreams: []Upstream{up}}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("cfg %s: %v", c.name, err)
		}
		s := New(cfg)
		client, err := s.clientFor(0)
		if err != nil {
			t.Fatalf("clientFor %s: %v", c.name, err)
		}
		// 结构断言（反证锚点）：开关必须真实分派两条出站路径。
		if c.mode == "chrome" && client == s.client {
			t.Fatalf("%s: chrome 档返回共享 client（指纹开关未生效——对照实验失效）", c.name)
		}
		if c.mode == "" && client != s.client {
			t.Fatalf("%s: none 档未走共享 client（缺省路径偏移）", c.name)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		url := "https://" + c.host + chatgptWebSentinelPath
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader("{}"))
		if err != nil {
			cancel()
			t.Fatalf("request %s: %v", c.name, err)
		}
		if c.full {
			if err := applyChatGPTWebHeaders(req.Header, up, chatgptWebTokens{}, ""); err != nil {
				cancel()
				t.Fatalf("headers %s: %v", c.name, err)
			}
		} else {
			req.Header.Set("User-Agent", chatgptWebUA)
			req.Header.Set("Content-Type", "application/json")
			for _, h := range c.extra {
				req.Header.Set(h.k, h.v)
			}
		}
		face := "full (applyChatGPTWebHeaders)"
		if !c.full {
			face = "minimal (UA+CT"
			for _, h := range c.extra {
				face += " + " + h.k
			}
			face += ")"
		}
		res := w53ProbeResult{Name: c.name, Mode: c.mode, Host: c.host, HeaderFace: face,
			StartedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		start := time.Now()
		resp, err := client.Do(req)
		res.DurationMS = time.Since(start).Milliseconds()
		var body []byte
		if err == nil {
			res.Status = resp.StatusCode
			res.Proto = resp.Proto
			if resp.TLS != nil {
				res.TLS = fmt.Sprintf("%s/%s alpn=%q", w53TLSName(resp.TLS.Version),
					tls.CipherSuiteName(resp.TLS.CipherSuite), resp.TLS.NegotiatedProtocol)
			}
			res.CFMitigated = resp.Header.Get("cf-mitigated")
			body, err = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			if err == nil {
				res.BodyLen = len(body)
				sum := sha256.Sum256(body)
				res.BodySHA256 = hex.EncodeToString(sum[:])
				res.Prefix = w53Prefix(body, 1024)
			}
		}
		if err != nil {
			res.Err = err.Error()
		}
		cancel()
		w53WriteEvidence(t, dir, c, req, resp, body, res)
		results = append(results, res)
		t.Logf("W53 %s: status=%d proto=%s tls=%s cf-mitigated=%q len=%d err=%q",
			c.name, res.Status, res.Proto, res.TLS, res.CFMitigated, res.BodyLen, res.Err)
	}
	sum, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatalf("summary marshal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "summary.json"), sum, 0o600); err != nil {
		t.Fatalf("summary write: %v", err)
	}
}

// w53WriteEvidence 落单案证据（txt = 状态行/全头/体前缀；body.bin = 原始体字节）。
func w53WriteEvidence(t *testing.T, dir string, c w53ProbeCase, req *http.Request, resp *http.Response, body []byte, res w53ProbeResult) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "# W53-1 live CF probe — %s\n", c.name)
	fmt.Fprintf(&b, "started_at: %s\nduration_ms: %d\n", res.StartedAt, res.DurationMS)
	mode := c.mode
	if mode == "" {
		mode = "none (std)"
	}
	fmt.Fprintf(&b, "mode: %s\nurl: %s %s\n", mode, req.Method, req.URL.String())
	fmt.Fprintf(&b, "request headers (assembled; wire adds Host/Accept-Encoding:gzip/Content-Length):\n")
	for _, k := range w53SortedKeys(req.Header) {
		fmt.Fprintf(&b, "  %s: %s\n", k, strings.Join(req.Header.Values(k), ", "))
	}
	if resp != nil {
		fmt.Fprintf(&b, "response: %s\nproto: %s\ntls: %s\ncf-mitigated: %q\n",
			resp.Status, res.Proto, res.TLS, res.CFMitigated)
		fmt.Fprintf(&b, "response headers:\n")
		for _, k := range w53SortedKeys(resp.Header) {
			fmt.Fprintf(&b, "  %s: %s\n", k, strings.Join(resp.Header.Values(k), ", "))
		}
	}
	if res.Err != "" {
		fmt.Fprintf(&b, "error: %s\n", res.Err)
	}
	fmt.Fprintf(&b, "body: %d bytes sha256=%s\nbody prefix (<=1024B):\n%s\n", len(body), res.BodySHA256, res.Prefix)
	if err := os.WriteFile(filepath.Join(dir, c.name+".txt"), []byte(b.String()), 0o600); err != nil {
		t.Errorf("write evidence %s: %v", c.name, err)
	}
	if err := os.WriteFile(filepath.Join(dir, c.name+".body.bin"), body, 0o600); err != nil {
		t.Errorf("write body %s: %v", c.name, err)
	}
}

// w53TLSName TLS 版本名（证据可读性）。
func w53TLSName(v uint16) string {
	switch v {
	case tls.VersionTLS13:
		return "TLS1.3"
	case tls.VersionTLS12:
		return "TLS1.2"
	default:
		return fmt.Sprintf("0x%04x", v)
	}
}

// w53Prefix 体前缀（≤n 字节；二进制原样保留）。
func w53Prefix(b []byte, n int) string {
	if len(b) > n {
		b = b[:n]
	}
	return string(b)
}

// w53SortedKeys 头名排序（证据文件可读、可 diff）。
func w53SortedKeys(h http.Header) []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
