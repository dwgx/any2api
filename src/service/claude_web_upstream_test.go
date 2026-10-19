// W10 e2e：claude_web 上游面（`protocol: claude_web`）——会话准备（org 发现 +
// conversation 创建）/ 鉴权与头面 / completion 流式回放实抓样本帧 / 下游 openai
// 出口（含 usage=配额面）/ 终止帧收尾 / 反例（反证锚点）。
//
// 装置：httptest 假 claude_web 上游（3 端点：GET /api/organizations、
// POST …/chat_conversations、POST …/chat_conversations/{conv}/completion）+
// 真服务（New(cfg).Handler() 经 httptest.NewServer）+ 真 HTTP 客户端。
//
// completion 的帧体 = **实抓样本回放**：读 claude_web 包的 testdata fixture
// （W9-2 脱敏样本，run1 = `Reply with exactly: PONG` → `PONG`，17 帧含 message_stop）
// 还原成 SSE 字节——「样本帧回放」不是手写帧形。
//
// 反证锚点（本文件内）：
//   - 删掉 forward 的 claude_web 会话准备分支 → TestE2EClaudeWebUpstreamStreaming
//     在「3 端点调用序」断言处红（path 空 → 400/502）；
//   - 删掉 relayStream 的终止帧 break → TestE2EClaudeWebTerminalFrameNoEOF 红
//     （挂到连接超时，耗时断言失败）；
//   - 删掉 pairs 的 claude_web 注册 → 两个 e2e 都在 NewStreamPipe/Convert 处红。
package service

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dwgx/any2api/translate/pairs"
	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// 脚手架：假 claude_web 上游
// ---------------------------------------------------------------------------

// claudeWebCall 一次上游调用记录。
type claudeWebCall struct {
	method  string
	path    string
	headers http.Header
	body    []byte
}

// claudeWebUpstream 假 claude_web 上游（3 端点；记录全部调用）。
type claudeWebUpstream struct {
	srv *httptest.Server

	mu    sync.Mutex
	calls []claudeWebCall

	// 可配行为（缺省 = 正常）。
	orgStatus   int    // GET /api/organizations 的状态码（0 → 200）
	orgBody     string // 自定义响应体（空 → 单组织数组）
	convStatus  int    // 会话创建状态码（0 → 201）
	convUUID    string // 会话创建返回的 uuid（空 → 服务端指定值）
	streamBody  string // completion 的 SSE 体（空 → 实抓样本回放）
	keepOpenEnd bool   // message_stop 后保持连接（模拟真实上游不主动关闭）
	// compStatus completion 的失败状态码（非 0 → 不写 SSE，回错误体 +
	// retry-after 头；claude_web.ParseError 的错误信封面）。
	compStatus  int
	compErrBody string
	// W13 清理面（DELETE …/chat_conversations/{conv}）：
	deleteStatus int  // 非 0 → 固定状态（默认 204）
	deleteHang   bool // 挂起直到客户端取消（或 10s 兜底）——超时/停机面
}

// claudeWebOrgUUID / claudeWebConvUUID 假上游的固定标识（服务端指定值——
// 服务必须用响应里的 conv uuid 而不是自己生成的那个）。
const (
	claudeWebOrgUUID  = "0f0f0f0f-1111-4222-8333-444444444444"
	claudeWebConvUUID = "5a5a5a5a-6666-4777-8888-999999999999"
)

func newClaudeWebUpstream(t *testing.T) *claudeWebUpstream {
	t.Helper()
	u := &claudeWebUpstream{convUUID: claudeWebConvUUID}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		u.mu.Lock()
		u.calls = append(u.calls, claudeWebCall{method: r.Method, path: r.URL.Path, headers: r.Header.Clone(), body: b})
		u.mu.Unlock()

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/organizations":
			if u.orgStatus >= 400 {
				w.WriteHeader(u.orgStatus)
				_, _ = w.Write([]byte(`{"error":{"message":"sessionKey invalid"}}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			body := u.orgBody
			if body == "" {
				body = fmt.Sprintf(`[{"id":1,"uuid":%q,"name":"Test","rate_limit_tier":"default_claude_ai"}]`, claudeWebOrgUUID)
			}
			_, _ = w.Write([]byte(body))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/chat_conversations"):
			if u.convStatus >= 400 {
				w.WriteHeader(u.convStatus)
				_, _ = w.Write([]byte(`{"error":{"message":"cannot create conversation"}}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, `{"uuid":%q}`, u.convUUID)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/completion"):
			if u.compStatus >= 400 {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("retry-after", "7")
				w.WriteHeader(u.compStatus)
				_, _ = io.WriteString(w, u.compErrBody)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, u.streamBody)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			if u.keepOpenEnd {
				// 真实上游在 message_stop 后不主动关闭（W9-2 §2.5）——等客户端
				// 断开（或兜底 5s，防测试挂死）。
				select {
				case <-r.Context().Done():
				case <-time.After(5 * time.Second):
				}
			}
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/chat_conversations/"):
			// W13 清理面：会话删除。缺省 204（BK/CW/YS 判据 200/204 之一）。
			if u.deleteHang {
				select {
				case <-r.Context().Done():
				case <-time.After(10 * time.Second):
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if u.deleteStatus >= 400 {
				w.WriteHeader(u.deleteStatus)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(u.srv.Close)
	return u
}

func (u *claudeWebUpstream) snapshot() []claudeWebCall {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]claudeWebCall(nil), u.calls...)
}

// waitForClaudeWebCalls 有界等待上游调用数达到 want 并返回快照（W13：清理面
// DELETE 在 completion 收尾后**异步**发出——固定计数断言必须等它到达，否则
// 竞态；本 helper 让既有三端点断言升级为四调用断言）。
func waitForClaudeWebCalls(t *testing.T, u *claudeWebUpstream, want int) []claudeWebCall {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		calls := u.snapshot()
		if len(calls) >= want {
			return calls
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待上游调用数 %d 超时，已见: %v", want, callPaths(calls))
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// assertNoClaudeWebDelete 宽限期内调用数恒为 want 且无 DELETE（W13 开关关 =
// 现状行为的负断言——宽限窗让「错误地异步发出」也有机会被抓到）。
func assertNoClaudeWebDelete(t *testing.T, u *claudeWebUpstream, want int) {
	t.Helper()
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		calls := u.snapshot()
		if len(calls) != want {
			t.Fatalf("调用数 = %d, want %d（开关关时不得发 DELETE）: %v", len(calls), want, callPaths(calls))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// claudeWebFixtureSSE 读 claude_web 包的实抓 fixture（JSONL 帧记录）并还原成
// 原始 SSE 文本（样本帧回放）。
func claudeWebFixtureSSE(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile("../translate/convert/claude_web/testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		var f struct {
			Event string `json:"event"`
			Data  string `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &f); err != nil {
			t.Fatalf("fixture line: %v", err)
		}
		b.WriteString("event: " + f.Event + "\ndata: " + f.Data + "\n\n")
	}
	return b.String()
}

// cfgClaudeWeb 服务配置：claude_web 上游（api_key = sessionKey）。
func cfgClaudeWeb(baseURL string, orgID string) *Config {
	return &Config{
		Listen:           "127.0.0.1:0",
		HeartbeatSeconds: 15,
		Upstreams: []Upstream{{
			Name: "fake-claude-web", BaseURL: baseURL, APIKey: "sk-ant-sid01-test-session-key",
			Protocol: ProtocolClaudeWeb, OrgID: orgID,
		}},
	}
}

// ---------------------------------------------------------------------------
// e2e 1：openai 入口 → claude_web 上游（流式；实抓样本帧回放）
// ---------------------------------------------------------------------------

func TestE2EClaudeWebUpstreamStreaming(t *testing.T) {
	up := newClaudeWebUpstream(t)
	up.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}

	// ---- 上游面 1：三端点调用序（org 发现 → 会话创建 → completion）+ W13 清理面
	// 的异步 DELETE（缺省开——用完即删；等它到达后按四调用断言）。----
	calls := waitForClaudeWebCalls(t, up, 4)
	if calls[0].method != http.MethodGet || calls[0].path != "/api/organizations" {
		t.Fatalf("调用[0] = %s %s, want GET /api/organizations", calls[0].method, calls[0].path)
	}
	if calls[1].method != http.MethodPost || calls[1].path != "/api/organizations/"+claudeWebOrgUUID+"/chat_conversations" {
		t.Fatalf("调用[1] = %s %s, want POST …/chat_conversations", calls[1].method, calls[1].path)
	}
	wantCompletion := "/api/organizations/" + claudeWebOrgUUID + "/chat_conversations/" + claudeWebConvUUID + "/completion"
	if calls[2].method != http.MethodPost || calls[2].path != wantCompletion {
		t.Fatalf("调用[2] = %s %s, want POST %s（必须用会话创建响应里的 conv uuid）", calls[2].method, calls[2].path, wantCompletion)
	}
	// 调用[3] = W13 清理面：completion 收尾后的异步 DELETE 会话（开关缺省开）。
	wantDelete := "/api/organizations/" + claudeWebOrgUUID + "/chat_conversations/" + claudeWebConvUUID
	if calls[3].method != http.MethodDelete || calls[3].path != wantDelete {
		t.Fatalf("调用[3] = %s %s, want DELETE %s（W13 清理面）", calls[3].method, calls[3].path, wantDelete)
	}

	// ---- 上游面 2：鉴权（cookie 形态）与协议常量头（KB §鉴权）。----
	for i, c := range calls {
		if got := c.headers.Get("Cookie"); got != "sessionKey=sk-ant-sid01-test-session-key" {
			t.Fatalf("调用[%d] Cookie = %q, want sessionKey cookie", i, got)
		}
		if got := c.headers.Get("anthropic-client-platform"); got != "web_claude_ai" {
			t.Fatalf("调用[%d] anthropic-client-platform = %q", i, got)
		}
		if got := c.headers.Get("anthropic-client-sha"); got != "3d8a03e3010c5025c6b2d997970df8ffbd9ce966" {
			t.Fatalf("调用[%d] anthropic-client-sha = %q", i, got)
		}
		if got := c.headers.Get("anthropic-client-version"); got != "1.0.0" {
			t.Fatalf("调用[%d] anthropic-client-version = %q", i, got)
		}
		if got := c.headers.Get("Origin"); got != up.srv.URL {
			t.Fatalf("调用[%d] Origin = %q, want %q", i, got, up.srv.URL)
		}
		if c.headers.Get("User-Agent") == "" || c.headers.Get("Accept-Language") == "" {
			t.Fatalf("调用[%d] 缺 UA/Accept-Language 拟真头", i)
		}
		anon, dev := c.headers.Get("anthropic-anonymous-id"), c.headers.Get("anthropic-device-id")
		if !strings.HasPrefix(anon, "claudeai.v1.") || dev == "" {
			t.Fatalf("调用[%d] 身份头 = %q/%q", i, anon, dev)
		}
	}
	// referer 按阶段切换：org 发现 /new → 会话创建 /new → completion /chat/{conv}。
	if got := calls[0].headers.Get("Referer"); !strings.HasSuffix(got, "/new") {
		t.Fatalf("org 发现 referer = %q, want …/new", got)
	}
	if got := calls[2].headers.Get("Referer"); !strings.HasSuffix(got, "/new") {
		t.Fatalf("completion referer = %q, want …/new（首回合实抓形态：conversation 隐式创建，页面仍在 /new）", got)
	}
	if got := calls[2].headers.Get("Accept"); got != "text/event-stream" {
		t.Fatalf("completion Accept = %q, want text/event-stream（上游恒 SSE）", got)
	}

	// ---- 上游面 3：会话创建体（KB §② 三实现同形）。----
	var convReq struct {
		UUID                           string `json:"uuid"`
		Name                           string `json:"name"`
		Model                          string `json:"model"`
		IncludeConversationPreferences bool   `json:"include_conversation_preferences"`
	}
	if err := json.Unmarshal(calls[1].body, &convReq); err != nil {
		t.Fatalf("会话创建体: %v\n%s", err, calls[1].body)
	}
	if !isUUIDv4(convReq.UUID) || convReq.Name != "" || convReq.Model != "claude-sonnet-5-5" || !convReq.IncludeConversationPreferences {
		t.Fatalf("会话创建体 = %+v", convReq)
	}

	// ---- 上游面 4：completion 体（实抓字段集；无 reasoning_effort/max_tokens）。----
	var compReq map[string]json.RawMessage
	if err := json.Unmarshal(calls[2].body, &compReq); err != nil {
		t.Fatalf("completion 体: %v\n%s", err, calls[2].body)
	}
	var prompt string
	_ = json.Unmarshal(compReq["prompt"], &prompt)
	if prompt != "Human: hi" {
		t.Fatalf("completion prompt = %q, want %q", prompt, "Human: hi")
	}
	for _, k := range []string{"prompt", "timezone", "locale", "model", "effort", "thinking_mode", "tools",
		"turn_message_uuids", "attachments", "files", "sync_sources", "completion_request_id",
		"rendering_mode", "create_conversation_params"} {
		if _, ok := compReq[k]; !ok {
			t.Fatalf("completion 体缺字段 %q", k)
		}
	}
	for _, k := range []string{"reasoning_effort", "max_tokens", "stream"} {
		if _, ok := compReq[k]; ok {
			t.Fatalf("completion 体不得带 %q（上游 400）", k)
		}
	}

	// ---- 下游面：openai chat SSE（内容 + finish_reason + usage=配额面 + [DONE]）。----
	text, finish, usage, done := parseOpenAIStream(t, raw)
	if text != "PONG" {
		t.Fatalf("下游内容 = %q, want PONG", text)
	}
	if finish != "stop" {
		t.Fatalf("下游 finish_reason = %q, want stop（end_turn）", finish)
	}
	if !done {
		t.Fatalf("下游流未以 [DONE] 收尾:\n%s", raw)
	}
	if usage == nil {
		t.Fatalf("include_usage=true 时下游应有 usage 帧:\n%s", raw)
	}
	var u struct {
		PromptTokens     int64           `json:"prompt_tokens"`
		CompletionTokens int64           `json:"completion_tokens"`
		TotalTokens      int64           `json:"total_tokens"`
		MessageLimit     json.RawMessage `json:"claude_web_message_limit"`
		Quota            struct {
			HardLimit        bool    `json:"hard_limit"`
			WorstUtilization float64 `json:"worst_utilization"`
			WorstWindow      string  `json:"worst_window"`
			EarliestResetsAt int64   `json:"earliest_resets_at"`
		} `json:"claude_web_quota"`
	}
	if err := json.Unmarshal(usage, &u); err != nil {
		t.Fatalf("usage 帧: %v\n%s", err, usage)
	}
	if u.PromptTokens != 0 || u.CompletionTokens != 0 || u.TotalTokens != 0 {
		t.Fatalf("配额面无 token 数——不得合成: %+v", u)
	}
	// message_limit 映射（windows/utilization/resets_at → usage/Extra → 下游 usage 键）。
	if len(u.MessageLimit) == 0 {
		t.Fatalf("usage 缺 claude_web_message_limit（配额原文）: %s", usage)
	}
	var ml struct {
		Type    string `json:"type"`
		Windows map[string]struct {
			Status      string  `json:"status"`
			ResetsAt    int64   `json:"resets_at"`
			Utilization float64 `json:"utilization"`
		} `json:"windows"`
	}
	if err := json.Unmarshal(u.MessageLimit, &ml); err != nil {
		t.Fatalf("message_limit: %v", err)
	}
	if ml.Type != "within_limit" || ml.Windows["5h"].Utilization != 0.02 || ml.Windows["5h"].ResetsAt != 1791982800 ||
		ml.Windows["7d"].ResetsAt != 1792519200 {
		t.Fatalf("message_limit 映射 = %+v", ml)
	}
	if u.Quota.HardLimit || u.Quota.WorstWindow != "5h" || u.Quota.WorstUtilization != 0.02 || u.Quota.EarliestResetsAt != 1791982800 {
		t.Fatalf("claude_web_quota 派生摘要 = %+v", u.Quota)
	}
}

// ---------------------------------------------------------------------------
// e2e 1b：上游**流内错误帧**（W93-1）——服务级归因面接线
// ---------------------------------------------------------------------------

// TestE2EClaudeWebUpstreamErrorFrameMarksFailed W93-1：claude_web completion 流里
// 的 `event: error` 帧（HTTP 200 下的流内错误）必须**同时**满足两面：
//   - 字节面：错误帧经 claude_web → openai_chat 转换后照常写出，前面的内容帧保留；
//   - 归因面：attempt 与 admin ring 均落 failed=true / delivered=false /
//     end_reason=upstream_error。
//
// 为什么 detector 单测不够：TestDirectFrameTerminalWebErrorFrames 只证明
// directFrameTerminal 返回 errorFrame=true，不证明 relayStream 的 pipe != nil
// 分支真的把它接到 ledger/ring（W90-1 之前该分支只挂 Feed 错误，合法错误事件
// 永不触发，落 completed/delivered=true）。本测试走真 serve 收尾路径。
//
// 反证（已演练）：删掉 directFrameTerminal 的 FormatClaudeWeb error 分支 →
// 下游字节面仍绿，attempt/ring 在 failed/delivered/end_reason 断言处红。
func TestE2EClaudeWebUpstreamErrorFrameMarksFailed(t *testing.T) {
	var logBuf syncLogBuffer
	prev := log.Writer()
	log.SetOutput(&logBuf)
	defer log.SetOutput(prev)

	up := newClaudeWebUpstream(t)
	// 实抓 run1 样本帧回放 + 在 message_stop 前拼入 `event: error`（上游流内错误
	// 的标准形态；error 是终止帧，循环在它处收束，后续 message_stop 不再消费——
	// 流仍合法，不是手写畸形帧）。
	const errorFrame = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"busy\"}}\n\n"
	fixture := claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	stop := strings.Index(fixture, "event: message_stop")
	if stop < 0 {
		t.Fatalf("run1 fixture 缺 message_stop 帧:\n%s", fixture)
	}
	up.streamBody = fixture[:stop] + errorFrame + fixture[stop:]
	s, srv := startServiceWith(t, cfgClaudeWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	body := string(raw)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("下游 status = %d（want 200：错误帧在已提交之后）body=%s", resp.StatusCode, body)
	}
	// 字节面：内容帧保留 + 转换后的 openai 错误帧（overloaded_error 字面保真进
	// code，type 归一 server_error）；错误帧即终态，不补 [DONE]。
	text, _, _, done := parseOpenAIStream(t, raw)
	if text != "PONG" {
		t.Fatalf("下游内容 = %q, want PONG（内容帧不得被错误帧吞掉）:\n%s", text, body)
	}
	for _, want := range []string{`"error"`, `"overloaded_error"`, `"busy"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("下游缺转换后的错误信号 %q:\n%s", want, body)
		}
	}
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(line, "overloaded_error") {
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
// e2e 2：openai 入口（stream:false）→ claude_web 上游（上游仍 SSE，装配面收口）
// ---------------------------------------------------------------------------

func TestE2EClaudeWebUpstreamNonStreaming(t *testing.T) {
	up := newClaudeWebUpstream(t)
	up.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
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
		t.Fatalf("下游体: %v\n%s", err, raw)
	}
	if out.Object != "chat.completion" || len(out.Choices) != 1 {
		t.Fatalf("下游体 = %+v", out)
	}
	if out.Choices[0].Message.Content != "PONG" || out.Choices[0].FinishReason != "stop" {
		t.Fatalf("choice = %+v", out.Choices[0])
	}
	// 上游面：仍按流式调用（completion 恒 SSE），三端点序不变 + W13 清理面 DELETE。
	if calls := waitForClaudeWebCalls(t, up, 4); calls[3].method != http.MethodDelete {
		t.Fatalf("上游调用 = %v, want 3 端点 + DELETE（W13 清理面）", callPaths(calls))
	}
}

// ---------------------------------------------------------------------------
// e2e 3：终止帧收尾（真实上游 message_stop 后不关连接——不等 EOF）
// ---------------------------------------------------------------------------

func TestE2EClaudeWebTerminalFrameNoEOF(t *testing.T) {
	for _, stream := range []bool{true, false} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			up := newClaudeWebUpstream(t)
			up.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
			up.keepOpenEnd = true // message_stop 后连接保持（W9-2 §2.5 实测形态）
			srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

			body := `{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}]}`
			if stream {
				body = `{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}],"stream":true}`
			}
			start := time.Now()
			resp := postJSON(t, srv.URL+"/v1/chat/completions", body, nil)
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			elapsed := time.Since(start)
			if resp.StatusCode != 200 {
				t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
			}
			if stream {
				text, _, _, done := parseOpenAIStream(t, raw)
				if text != "PONG" || !done {
					t.Fatalf("流式下游体 = %q done=%v\n%s", text, done, raw)
				}
			} else if !strings.Contains(string(raw), `"PONG"`) {
				t.Fatalf("非流式下游体缺内容:\n%s", raw)
			}
			// 反证：不做终止帧收尾 → 服务挂到连接超时（假上游兜底 5s）→ 本断言红。
			if elapsed > 3*time.Second {
				t.Fatalf("响应耗时 %v——终止帧未收尾（真实上游不关连接，等 EOF 会挂）", elapsed)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// e2e 4：org_id 配置（跳过 org 发现）
// ---------------------------------------------------------------------------

func TestE2EClaudeWebOrgIDConfigured(t *testing.T) {
	up := newClaudeWebUpstream(t)
	up.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	srv := startService(t, cfgClaudeWeb(up.srv.URL, claudeWebOrgUUID))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	// W13：org_id 配置形态同样走清理面（跳过 org 发现 → 创建 + completion + DELETE）。
	calls := waitForClaudeWebCalls(t, up, 3)
	if calls[0].path != "/api/organizations/"+claudeWebOrgUUID+"/chat_conversations" {
		t.Fatalf("调用[0] = %s, want 会话创建", calls[0].path)
	}
	if calls[2].method != http.MethodDelete || calls[2].path != "/api/organizations/"+claudeWebOrgUUID+"/chat_conversations/"+claudeWebConvUUID {
		t.Fatalf("调用[2] = %s %s, want DELETE 会话（W13 清理面，org_id 配置形态）", calls[2].method, calls[2].path)
	}
}

// ---------------------------------------------------------------------------
// 反例（反证锚点）：会话准备失败 / 非 SSE 上游 / 截断流
// ---------------------------------------------------------------------------

// 反例 E：completion 面 429（错误信封面）→ 下游 429 + 入口协议信封 + retry-after
// 透传；上游原文形状不外泄（跨协议错误经 claude_web.ParseError → 入口渲染）。
func TestClaudeWebCompletionErrorEnvelope(t *testing.T) {
	up := newClaudeWebUpstream(t)
	up.compStatus = http.StatusTooManyRequests
	up.compErrBody = `{"type":"error","error":{"type":"rate_limit_error","message":"Rate limited"}}`
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429\n%s", resp.StatusCode, raw)
	}
	if got := resp.Header.Get("retry-after"); got != "7" {
		t.Fatalf("retry-after = %q, want 7（上游头透传）", got)
	}
	var env struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("错误信封: %v\n%s", err, raw)
	}
	if env.Error.Message != "Rate limited" {
		t.Fatalf("错误消息 = %q, want Rate limited（ParseError 提取）", env.Error.Message)
	}
	// 入口协议（openai）信封：不得是 claude_web 的原样体（`{"type":"error","error":…}` 顶层 type）。
	if strings.Contains(string(raw), `"type":"error"`) {
		t.Fatalf("上游错误体形状泄漏到下游（应为入口协议信封）:\n%s", raw)
	}
	// 凭据不外泄。
	if strings.Contains(string(raw), "sk-ant-sid01-test-session-key") {
		t.Fatalf("凭据泄漏:\n%s", raw)
	}
	// W13 清理面：429 是 completion 步失败，但会话已在准备步创建——收尾后照样
	// 异步 DELETE（W12-1 实锤：429 请求也泄会话——泄漏面最大的失败形态）。
	calls := waitForClaudeWebCalls(t, up, 4)
	if calls[3].method != http.MethodDelete {
		t.Fatalf("调用[3] = %s %s, want DELETE（429 收尾后清理）", calls[3].method, calls[3].path)
	}
}

// 反例 A：org 发现 401（sessionKey 失效）→ 502 错误信封（上游原文不外泄）；
// 不发会话创建与 completion。
func TestClaudeWebOrgDiscoveryAuthFailure(t *testing.T) {
	up := newClaudeWebUpstream(t)
	up.orgStatus = http.StatusUnauthorized
	up.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502\n%s", resp.StatusCode, raw)
	}
	if calls := up.snapshot(); len(calls) != 1 {
		t.Fatalf("上游调用数 = %d, want 1（org 发现失败即止）: %v", len(calls), callPaths(calls))
	}
	// 错误信封是入口协议形态；凭据值绝不外泄。
	if strings.Contains(string(raw), "sk-ant-sid01-test-session-key") {
		t.Fatalf("凭据值泄漏到下游错误体:\n%s", raw)
	}
	if !strings.Contains(string(raw), "401") && !strings.Contains(string(raw), "sessionKey") {
		t.Fatalf("错误信封未说明原因:\n%s", raw)
	}
}

// 反例 B：会话创建失败（500）→ 502；不发 completion。
func TestClaudeWebConversationCreateFailure(t *testing.T) {
	up := newClaudeWebUpstream(t)
	up.convStatus = http.StatusInternalServerError
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502\n%s", resp.StatusCode, raw)
	}
	calls := up.snapshot()
	if len(calls) != 2 {
		t.Fatalf("上游调用数 = %d, want 2（创建失败即止）: %v", len(calls), callPaths(calls))
	}
	if strings.HasSuffix(calls[1].path, "/completion") {
		t.Fatalf("创建失败后不得发 completion")
	}
}

// 反例 C：completion 返回非 SSE（JSON 体）→ 502（B1 反向守卫：200+非 SSE = 协议违规）。
func TestClaudeWebNonSSECompletion(t *testing.T) {
	up := newClaudeWebUpstream(t)
	up.streamBody = `{"error":"not an event stream"}` // 非 SSE 体
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502（非 SSE 上游）\n%s", resp.StatusCode, raw)
	}
}

// 反例 D：截断流（message_stop 前 EOF）→ 下游不得静默成功（错误帧收尾）。
func TestClaudeWebTruncatedStream(t *testing.T) {
	up := newClaudeWebUpstream(t)
	// 只回 message_start + 一个正文 delta（无 message_stop）——上游断开。
	up.streamBody = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"claude-sonnet-5-5\"}}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"half\"}}\n\n"
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	body := string(raw)
	// 已提交（首产出后）→ 流内错误帧收尾（incomplete_stream / transport），绝不补 [DONE] 假成功。
	if strings.Contains(body, "[DONE]") {
		t.Fatalf("截断流不得以 [DONE] 收尾（假成功）:\n%s", body)
	}
	if !strings.Contains(body, "incomplete_stream") && !strings.Contains(body, "message_stop") {
		t.Fatalf("截断流应以错误帧收尾:\n%s", body)
	}
	// W13 清理面：非终态中断（上游截断）同样收尾即清理——会话不得因失败形态残留。
	calls := waitForClaudeWebCalls(t, up, 4)
	if calls[3].method != http.MethodDelete {
		t.Fatalf("调用[3] = %s %s, want DELETE（截断流收尾后清理）", calls[3].method, calls[3].path)
	}
}

// ---------------------------------------------------------------------------
// 反证：接线本身（删注册/删 case 即红）
// ---------------------------------------------------------------------------

// TestClaudeWebWiringRegistered 钉住三处接线（删任一处 → 本测试红，e2e 也会红）：
//   - registry：claude_web ↔ openai_chat 显式对（1 步、Quality=good——不是枢纽
//     两步）；
//   - pairs：NewStreamPipe(claude_web → openai_chat) 可建（newStreamParser 的
//     claude_web case 在场）；
//   - service：ProtocolClaudeWeb 的 Format 映射 + PathFor 显式报错（动态路径契约）。
func TestClaudeWebWiringRegistered(t *testing.T) {
	reg := pairs.NewRegistry()
	plan, err := reg.Lookup(registry.FormatClaudeWeb, registry.FormatOpenAIChat)
	if err != nil {
		t.Fatalf("Lookup(claude_web → openai_chat): %v", err)
	}
	if len(plan.Steps) != 1 || plan.Quality != registry.QualityGood {
		t.Fatalf("plan = %+v, want 1 步 Quality=good（显式对，非枢纽两步）", plan)
	}
	if _, err := pairs.NewStreamPipe(reg, registry.FormatClaudeWeb, registry.FormatOpenAIChat, pairs.Options{}); err != nil {
		t.Fatalf("NewStreamPipe(claude_web → openai_chat): %v", err)
	}
	if got := ProtocolClaudeWeb.Format(); got != registry.FormatClaudeWeb {
		t.Fatalf("ProtocolClaudeWeb.Format() = %q", got)
	}
	if _, err := ProtocolClaudeWeb.PathFor("m", true); err == nil {
		t.Fatalf("claude_web 的 PathFor 必须显式报错（动态路径走会话准备）")
	}
	// 四入口 × claude_web 双向显式对都在（任一入口都能直连该上游）。
	for _, f := range []registry.Format{
		registry.FormatOpenAIChat, registry.FormatClaudeMessages,
		registry.FormatGeminiGenerate, registry.FormatOpenAIResponses,
	} {
		for _, pair := range [][2]registry.Format{{f, registry.FormatClaudeWeb}, {registry.FormatClaudeWeb, f}} {
			p, err := reg.Lookup(pair[0], pair[1])
			if err != nil || len(p.Steps) != 1 {
				t.Fatalf("Lookup(%s → %s) = %+v err=%v, want 1 步", pair[0], pair[1], p, err)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// 工具/思考面的下游落法（W10-3 实抓样本：runA = thinking→text→tool_use→…）
// ---------------------------------------------------------------------------

func TestE2EClaudeWebThinkingAndTools(t *testing.T) {
	up := newClaudeWebUpstream(t)
	up.streamBody = claudeWebFixtureSSE(t, "runA_45534.540.frames.jsonl.txt")
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}
	var reasoning, content strings.Builder
	var toolNames []string
	var sawToolCall bool
	sc := bufio.NewScanner(bytes.NewReader(raw))
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") || strings.Contains(line, "[DONE]") {
			continue
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content          string `json:"content"`
					ReasoningContent string `json:"reasoning_content"`
					ToolCalls        []struct {
						Function struct {
							Name string `json:"name"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &chunk); err != nil {
			t.Fatalf("chunk: %v\n%s", err, line)
		}
		for _, c := range chunk.Choices {
			reasoning.WriteString(c.Delta.ReasoningContent)
			content.WriteString(c.Delta.Content)
			for _, tc := range c.Delta.ToolCalls {
				sawToolCall = true
				if tc.Function.Name != "" {
					toolNames = append(toolNames, tc.Function.Name)
				}
			}
		}
	}
	// 思考摘要 → reasoning_content（openai 出口既有约定）；正文 → content。
	if reasoning.Len() == 0 {
		t.Fatalf("思考摘要未下发（reasoning_content 空）:\n%s", raw)
	}
	if !strings.Contains(reasoning.String(), "解答") && !strings.Contains(reasoning.String(), "推导") {
		t.Fatalf("reasoning_content 与实抓摘要不符（前 200 字符）: %.200s", reasoning.String())
	}
	if content.Len() == 0 {
		t.Fatalf("正文未下发")
	}
	// 上游内置工具调用（bash_tool）→ 出口 tool_calls（server tool use 的既有约定）。
	if !sawToolCall {
		t.Fatalf("工具调用未下发:\n%s", raw)
	}
	found := false
	for _, n := range toolNames {
		if n == "bash_tool" {
			found = true
		}
	}
	if !found {
		t.Fatalf("工具名 = %v, want bash_tool", toolNames)
	}
}

// TestE2EClaudeWebClaudeEntryToolHistory W11-3：claude_messages 入口（Claude Code
// 形态：`tools` 参数 + tool_use/tool_result 历史）→ claude_web 上游 → claude_messages
// 出口，**全链 HTTP 路径不断流**（旧行为：请求面 flattenPrompt ErrUnsupportedBlock
// → 400；出口面 BlockToolResult ErrUnsupportedBlock → 流中断）：
//   - 上游 completion 体：工具历史拍平进 prompt（标签文本）；`tools` 参数不落上游体；
//   - 下游 SSE：实抓 runA 回放（server tool 块）→ server_tool_use 块保留 +
//     message_stop 收尾；tool_result 块不出现（整块丢弃）。
func TestE2EClaudeWebClaudeEntryToolHistory(t *testing.T) {
	up := newClaudeWebUpstream(t)
	up.streamBody = claudeWebFixtureSSE(t, "runA_45534.540.frames.jsonl.txt")
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

	body := `{"model":"claude-sonnet-5-5","max_tokens":32000,"stream":true,
		"tools":[{"name":"Read","description":"Read a file","input_schema":{"type":"object","properties":{"file_path":{"type":"string"}}}}],
		"messages":[
			{"role":"user","content":"read a.go"},
			{"role":"assistant","content":[{"type":"tool_use","id":"toolu_01ABC","name":"Read","input":{"file_path":"a.go"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_01ABC","content":"package main"}]}
		]}`
	resp := postJSON(t, srv.URL+"/v1/messages", body, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}
	text := string(raw)
	if !strings.Contains(text, `"type":"server_tool_use"`) || !strings.Contains(text, `"name":"bash_tool"`) {
		t.Fatalf("server_tool_use 块缺失:\n%.800s", text)
	}
	if strings.Contains(text, "tool_result") {
		t.Fatalf("tool_result 块不得出现在出口流:\n%.800s", text)
	}
	if !strings.Contains(text, "event: message_stop") || !strings.Contains(text, `"stop_reason":"end_turn"`) {
		t.Fatalf("流未收尾（B7/B8）:\n%.400s", text)
	}
	// 上游面：三端点调用序不变（+ W13 清理面 DELETE）；completion 体 prompt 含拍平
	// 工具历史、tools 三元组。
	calls := waitForClaudeWebCalls(t, up, 4)
	if calls[3].method != http.MethodDelete {
		t.Fatalf("上游调用 = %v, want 3 端点 + DELETE（W13 清理面）", callPaths(calls))
	}
	var reqBody struct {
		Prompt string           `json:"prompt"`
		Tools  []map[string]any `json:"tools"`
	}
	if err := json.Unmarshal(calls[2].body, &reqBody); err != nil {
		t.Fatalf("completion body: %v", err)
	}
	for _, want := range []string{
		`<tool_use id="toolu_01ABC" name="Read">{"file_path":"a.go"}</tool_use>`,
		`<tool_result tool_use_id="toolu_01ABC">package main</tool_result>`,
	} {
		if !strings.Contains(reqBody.Prompt, want) {
			t.Fatalf("prompt 缺 %q:\n%s", want, reqBody.Prompt)
		}
	}
	if len(reqBody.Tools) != 3 {
		t.Fatalf("上游 tools = %v, want web_search/artifacts/repl 三元组", reqBody.Tools)
	}
	if strings.Contains(reqBody.Prompt, "Read a file") {
		t.Fatalf("客户端工具定义不得注入 prompt:\n%s", reqBody.Prompt)
	}
}

// TestE2EClaudeWebClaudeEntryToolHistoryNonStream W11-3：**非流式面**（Claude Code
// 的流式失败回退路径，cc-wire-contract C3 实测必发生）——同请求 `stream:false`：
// 上游恒流式 → ParseResponse 按块装配（tool_result 跳过、tool_use → server_tool_use）
// → claude_messages JSON 出口。断言：200 + server_tool_use 块 + 无 tool_result 块。
//
// stop_reason 现状钉死 = `tool_use`（非流式面走 effectiveStopReason「有工具块 ⟹
// tool_use」既有规则；**流式面透传上游 end_turn**——两面差异是 W10 前既有规则，
// 本单保持现状 + 记录（报告 §5 残余 1），改它会影响全部 claude_messages 非流式出口）。
func TestE2EClaudeWebClaudeEntryToolHistoryNonStream(t *testing.T) {
	up := newClaudeWebUpstream(t)
	up.streamBody = claudeWebFixtureSSE(t, "runA_45534.540.frames.jsonl.txt")
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

	body := `{"model":"claude-sonnet-5-5","max_tokens":32000,"stream":false,
		"tools":[{"name":"Read","description":"Read a file","input_schema":{"type":"object","properties":{"file_path":{"type":"string"}}}}],
		"messages":[
			{"role":"user","content":"read a.go"},
			{"role":"assistant","content":[{"type":"tool_use","id":"toolu_01ABC","name":"Read","input":{"file_path":"a.go"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_01ABC","content":"package main"}]}
		]}`
	resp := postJSON(t, srv.URL+"/v1/messages", body, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, raw)
	}
	var out struct {
		Type       string `json:"type"`
		StopReason string `json:"stop_reason"`
		Content    []struct {
			Type string `json:"type"`
			Name string `json:"name"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, raw)
	}
	if out.Type != "message" {
		t.Fatalf("type = %q, want message", out.Type)
	}
	foundServerTool := false
	for _, b := range out.Content {
		if b.Type == "tool_result" {
			t.Fatalf("tool_result 块不得出现在非流式出口（客户端语义无配对 tool_use）: %s", raw)
		}
		if b.Type == "server_tool_use" && b.Name == "bash_tool" {
			foundServerTool = true
		}
	}
	if !foundServerTool {
		t.Fatalf("server_tool_use(bash_tool) 块缺失: %s", raw)
	}
	if out.StopReason != "tool_use" {
		t.Fatalf("stop_reason = %q, want tool_use（非流式面现状规则；流式面为 end_turn——差异见报告 §5）", out.StopReason)
	}
}

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

func callPaths(calls []claudeWebCall) []string {
	out := make([]string, 0, len(calls))
	for _, c := range calls {
		out = append(out, c.method+" "+c.path)
	}
	return out
}

// parseOpenAIStream 解析下游 openai chat SSE：内容串 / finish_reason / usage 帧 / [DONE]。
func parseOpenAIStream(t *testing.T, raw []byte) (content, finish string, usage json.RawMessage, done bool) {
	t.Helper()
	var b strings.Builder
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		if payload == "[DONE]" {
			done = true
			continue
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
			Usage json.RawMessage `json:"usage"`
		}
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			t.Fatalf("chunk: %v\n%s", err, payload)
		}
		if len(chunk.Usage) > 0 {
			usage = chunk.Usage
		}
		for _, c := range chunk.Choices {
			b.WriteString(c.Delta.Content)
			if c.FinishReason != nil {
				finish = *c.FinishReason
			}
		}
	}
	return b.String(), finish, usage, done
}

// isUUIDv4 v4 形态校验（服务端生成的 uuid）。
func isUUIDv4(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	if s[14] != '4' {
		return false
	}
	switch s[19] {
	case '8', '9', 'a', 'b':
	default:
		return false
	}
	for i := range len(s) {
		c := s[i]
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// W48-1：头面自洽性回归钉（直连路径裁定后的新鲜度/一致性契约）
// ---------------------------------------------------------------------------

// TestClaudeWebFaceSelfConsistency 钉住 W48-1 裁定后的头面契约：
//
//	① client-sha = W43-2 浏览器实测实况值（回退旧值/任意漂移即红——本波反证锚点）；
//	② UA 与 sec-ch-ua 三件套版本/平台逐项自洽（只改一头不改另一头即红）；
//	③ 身份头对同一凭据确定性（两次装配一致——uuid5 派生，跨请求不变）。
func TestClaudeWebFaceSelfConsistency(t *testing.T) {
	up := Upstream{APIKey: "sk-ant-sid02-test-session-key"}
	h := http.Header{}
	if err := applyClaudeWebHeaders(h, up, "https://claude.ai/new", false); err != nil {
		t.Fatalf("applyClaudeWebHeaders: %v", err)
	}
	// ① client-sha 与实况对齐（W43-2 浏览器实测 2026-10-15；旧值 e640a19a… 已过时）。
	const liveSHA = "3d8a03e3010c5025c6b2d997970df8ffbd9ce966"
	if got := h.Get("anthropic-client-sha"); got != liveSHA {
		t.Fatalf("client-sha = %q, want %q（实况值；回退即红）", got, liveSHA)
	}
	// ② UA 与 sec-ch-ua 版本自洽：Chrome 大版本号逐项一致。
	ua := h.Get("User-Agent")
	const marker = "Chrome/"
	i := strings.Index(ua, marker)
	if i < 0 {
		t.Fatalf("UA 缺 Chrome 版本: %q", ua)
	}
	j := i + len(marker)
	for j < len(ua) && ua[j] >= '0' && ua[j] <= '9' {
		j++
	}
	wantMajor := ua[i+len(marker) : j]
	if wantMajor == "" {
		t.Fatalf("UA Chrome 版本为空: %q", ua)
	}
	if got := h.Get("sec-ch-ua"); !strings.Contains(got, `"Chromium";v="`+wantMajor+`"`) {
		t.Fatalf("sec-ch-ua = %q 与 UA Chrome/%s 不自洽", got, wantMajor)
	}
	// ②b 平台自洽：UA 平台 token 与 sec-ch-ua-platform 一致。
	if !strings.Contains(ua, "X11; Linux x86_64") || h.Get("sec-ch-ua-platform") != `"Linux"` {
		t.Fatalf("平台不自洽: UA=%q platform=%q", ua, h.Get("sec-ch-ua-platform"))
	}
	// ③ 身份头确定性：同一凭据两次装配一致（uuid5 派生）。
	h2 := http.Header{}
	if err := applyClaudeWebHeaders(h2, up, "https://claude.ai/new", false); err != nil {
		t.Fatalf("applyClaudeWebHeaders(2): %v", err)
	}
	if a1, a2 := h.Get("anthropic-anonymous-id"), h2.Get("anthropic-anonymous-id"); a1 == "" || a1 != a2 {
		t.Fatalf("anonymous-id 不确定: %q vs %q", a1, a2)
	}
	if d1, d2 := h.Get("anthropic-device-id"), h2.Get("anthropic-device-id"); d1 == "" || d1 != d2 {
		t.Fatalf("device-id 不确定: %q vs %q", d1, d2)
	}
}
