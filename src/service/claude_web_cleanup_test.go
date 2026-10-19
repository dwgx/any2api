// W13 清理面 e2e：claude_web 会话删除（`DELETE …/chat_conversations/{conv}`，
// body `{"uuid": conv}`——KB §清理面「用完即删」；BK claude.go:189-224 /
// YS handle.go:229-244 / CW handle.go:169-174 三方互证）。
//
// 覆盖（brief §Change 4 的四类 + 两条加固）：
//   - ① 终态帧后 DELETE 确实发出（路径/body/头面正确；org 现取与 org_id 配置两形态）；
//   - ② 删除失败（500 / 挂起超时面）下游逐字节不受影响（对照「清理关」基线）；
//   - ③ 开关关（cleanup_conversations: false）= 现状行为（不发 DELETE）；
//   - ④ 非终态中断（客户端断开）也清理——泄漏面最大的形态（W12-1 实锤）；
//   - ⑤ 有界重试：失败时恰好 3 次尝试后即止（BK 原值）；
//   - ⑥ 停机有界收尾：ShutdownCleanup 取消在飞删除、有界等待、取消后不再重试。
//
// 反证锚点（回退即红——原始输出落 tmp/lanes/W13-cleanup/proofs/）：
//   - 删 forward 的 schedule defer → TestE2EClaudeWebCleanupDeleteSent 红；
//   - 开关被忽略（恒清理）→ TestE2EClaudeWebCleanupSwitchOff 红；
//   - 清理改同步（挂响应路径）→ TestE2EClaudeWebCleanupHangDoesNotBlockResponse 红；
//   - 重试不响应停机取消 → TestClaudeWebCleanupShutdownBounded 红。
package service

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

const claudeWebChatBody = `{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}],"stream":true}`

// cfgClaudeWebSwitchOff 清理开关关的配置（现状行为：不发 DELETE）。
func cfgClaudeWebSwitchOff(baseURL string) *Config {
	off := false
	cfg := cfgClaudeWeb(baseURL, "")
	cfg.Upstreams[0].CleanupConversations = &off
	return cfg
}

// ---------------------------------------------------------------------------
// ① 终态帧后 DELETE 确实发出（两 org 形态）
// ---------------------------------------------------------------------------

func TestE2EClaudeWebCleanupDeleteSent(t *testing.T) {
	for _, orgConfigured := range []bool{false, true} {
		name, orgID := "org_discovered", ""
		if orgConfigured {
			name, orgID = "org_configured", claudeWebOrgUUID
		}
		t.Run(name, func(t *testing.T) {
			up := newClaudeWebUpstream(t)
			up.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
			s, srv := startServiceWith(t, cfgClaudeWeb(up.srv.URL, orgID))
			t.Cleanup(func() { s.ShutdownCleanup(2 * time.Second) })

			code, body := postChatStream(t, srv.URL)
			if code != 200 || !strings.Contains(body, "PONG") {
				t.Fatalf("下游响应异常: status=%d\n%s", code, body)
			}

			want := 4
			if orgConfigured {
				want = 3 // org_id 配置 → 跳过 org 发现（创建 + completion + DELETE）
			}
			calls := waitForClaudeWebCalls(t, up, want)
			del := calls[want-1]
			if del.method != http.MethodDelete {
				t.Fatalf("调用[%d] = %s %s, want DELETE（收尾后清理）", want-1, del.method, del.path)
			}
			wantPath := "/api/organizations/" + claudeWebOrgUUID + "/chat_conversations/" + claudeWebConvUUID
			if del.path != wantPath {
				t.Fatalf("DELETE path = %q, want %q", del.path, wantPath)
			}
			// body `{"uuid": conv}`（KB §清理面；BK/CW/YS 同形）。
			var db struct {
				UUID string `json:"uuid"`
			}
			if err := json.Unmarshal(del.body, &db); err != nil || db.UUID != claudeWebConvUUID {
				t.Fatalf("DELETE body = %s (err=%v), want {uuid:%q}", del.body, err, claudeWebConvUUID)
			}
			// 头面：cookie 鉴权 + 协议常量 + referer `/chat/{conv}`（BK/CW/YS 三方
			// 同形——删除阶段在会话页）+ 非流式 Accept。
			if got := del.headers.Get("Cookie"); got != "sessionKey=sk-ant-sid01-test-session-key" {
				t.Fatalf("DELETE Cookie = %q", got)
			}
			if got := del.headers.Get("Content-Type"); got != "application/json" {
				t.Fatalf("DELETE Content-Type = %q", got)
			}
			if got := del.headers.Get("Referer"); got != up.srv.URL+"/chat/"+claudeWebConvUUID {
				t.Fatalf("DELETE Referer = %q, want …/chat/{conv}", got)
			}
			if got := del.headers.Get("Accept"); got != "*/*" {
				t.Fatalf("DELETE Accept = %q, want */*（非流式面）", got)
			}
			if got := del.headers.Get("User-Agent"); got != claudeWebUA {
				t.Fatalf("DELETE User-Agent = %q", got)
			}
			if got := del.headers.Get("anthropic-client-platform"); got != "web_claude_ai" {
				t.Fatalf("DELETE anthropic-client-platform = %q", got)
			}
			if orgConfigured {
				// 配置形态：不得有 org 发现调用（2 端点 + DELETE）。
				for i, c := range calls {
					if c.method == http.MethodGet && c.path == "/api/organizations" {
						t.Fatalf("org_id 配置时不得发 org 发现（调用[%d]）", i)
					}
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ② 删除失败（500 / 挂起）绝不影响下游（逐字节一致）；挂起不阻塞响应
// ---------------------------------------------------------------------------

func TestE2EClaudeWebCleanupFailureDoesNotAffectDownstream(t *testing.T) {
	// 基线：清理关（不发 DELETE）——下游字节基准。
	baseUp := newClaudeWebUpstream(t)
	baseUp.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	baseSrv := startService(t, cfgClaudeWebSwitchOff(baseUp.srv.URL))
	baseCode, baseBody := postChatStream(t, baseSrv.URL)
	if baseCode != 200 {
		t.Fatalf("基线 status = %d", baseCode)
	}

	for _, tc := range []struct {
		name string
		set  func(*claudeWebUpstream)
	}{
		{"delete_500", func(u *claudeWebUpstream) { u.deleteStatus = http.StatusInternalServerError }},
		{"delete_hang", func(u *claudeWebUpstream) { u.deleteHang = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := newClaudeWebUpstream(t)
			up.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
			tc.set(up)
			s, srv := startServiceWith(t, cfgClaudeWeb(up.srv.URL, ""))

			code, body := postChatStream(t, srv.URL)
			if code != 200 || body != baseBody {
				t.Fatalf("删除失败改变了下游响应（status=%d）:\n--- 基线 ---\n%s\n--- 实际 ---\n%s", code, baseBody, body)
			}
			// 确认清理确实发生了（失败面而非「没触发」面）。
			calls := waitForClaudeWebCalls(t, up, 4)
			if calls[3].method != http.MethodDelete {
				t.Fatalf("调用[3] = %s %s, want DELETE", calls[3].method, calls[3].path)
			}
			// 停机收尾：挂起/重试中的在飞删除被取消（有界，不泄漏）。
			if !s.ShutdownCleanup(2 * time.Second) {
				t.Fatalf("停机收尾超时（在飞清理未收束）")
			}
		})
	}
}

// TestE2EClaudeWebCleanupHangDoesNotBlockResponse 挂起的 DELETE 绝不挂在响应
// 路径上：DELETE 永不返回时，下游响应仍及时收尾（异步面 + 有界超时）。
func TestE2EClaudeWebCleanupHangDoesNotBlockResponse(t *testing.T) {
	up := newClaudeWebUpstream(t)
	up.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	up.deleteHang = true
	s, srv := startServiceWith(t, cfgClaudeWeb(up.srv.URL, ""))
	t.Cleanup(func() { s.ShutdownCleanup(2 * time.Second) })

	start := time.Now()
	code, body := postChatStream(t, srv.URL)
	elapsed := time.Since(start)
	if code != 200 || !strings.Contains(body, "PONG") {
		t.Fatalf("下游响应异常: status=%d\n%s", code, body)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("响应耗时 %v——挂起的 DELETE 阻塞了响应路径（应异步 + 有界）", elapsed)
	}
	// DELETE 已发出且仍在挂起中（fake 等客户端取消）——随后由停机收尾取消。
	if calls := waitForClaudeWebCalls(t, up, 4); calls[3].method != http.MethodDelete {
		t.Fatalf("调用[3] = %s %s, want DELETE", calls[3].method, calls[3].path)
	}
}

// ---------------------------------------------------------------------------
// ③ 开关关 = 现状行为（不发 DELETE）
// ---------------------------------------------------------------------------

func TestE2EClaudeWebCleanupSwitchOff(t *testing.T) {
	up := newClaudeWebUpstream(t)
	up.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	srv := startService(t, cfgClaudeWebSwitchOff(up.srv.URL))

	code, body := postChatStream(t, srv.URL)
	if code != 200 || !strings.Contains(body, "PONG") {
		t.Fatalf("下游响应异常: status=%d\n%s", code, body)
	}
	calls := waitForClaudeWebCalls(t, up, 3)
	for i, c := range calls {
		if c.method == http.MethodDelete {
			t.Fatalf("开关关时不得发 DELETE（调用[%d]）", i)
		}
	}
	// 宽限窗：异步面若错误触发也会被抓住。
	assertNoClaudeWebDelete(t, up, 3)
}

// ---------------------------------------------------------------------------
// ④ 非终态中断（客户端断开）也清理
// ---------------------------------------------------------------------------

func TestE2EClaudeWebCleanupClientDisconnect(t *testing.T) {
	up := newClaudeWebUpstream(t)
	// 半截流 + 保持连接：服务已提交下游但无终态帧——客户端取消是唯一出口。
	up.streamBody = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"claude-sonnet-5-5\"}}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"half\"}}\n\n"
	up.keepOpenEnd = true
	s, srv := startServiceWith(t, cfgClaudeWeb(up.srv.URL, ""))
	t.Cleanup(func() { s.ShutdownCleanup(2 * time.Second) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(claudeWebChatBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	// 读到首个产出（提交点已过）后断开客户端。
	buf := make([]byte, 64)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatalf("读首字节: %v", err)
	}
	cancel()
	resp.Body.Close()

	// 客户端断开 ≠ 会话免删：清理面与请求 ctx 解耦（泄漏面最大的形态——W12-1
	// 实锤失败/中断请求也建会话；YS handle.go:229-234 同款：SendMessage 失败也清）。
	calls := waitForClaudeWebCalls(t, up, 4)
	if calls[3].method != http.MethodDelete {
		t.Fatalf("调用[3] = %s %s, want DELETE（客户端断开后仍清理）", calls[3].method, calls[3].path)
	}
}

// ---------------------------------------------------------------------------
// ⑤ 有界重试：3 次尝试后即止（BK 原值）
// ---------------------------------------------------------------------------

func TestClaudeWebCleanupRetriesBounded(t *testing.T) {
	up := newClaudeWebUpstream(t)
	up.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	up.deleteStatus = http.StatusInternalServerError
	s, srv := startServiceWith(t, cfgClaudeWeb(up.srv.URL, ""))
	s.cwCleanupInterval = 10 * time.Millisecond // 测试加速（生产 2s = BK 原值）

	if code, _ := postChatStream(t, srv.URL); code != 200 {
		t.Fatalf("status = %d", code)
	}
	waitForClaudeWebCalls(t, up, 6) // 3 端点 + 3 次 DELETE 尝试（恰好有界）
	// 宽限窗（≥10 个重试间隔）：不得出现第 4 次（BK 3 次即止）。
	time.Sleep(150 * time.Millisecond)
	n := 0
	for _, c := range up.snapshot() {
		if c.method == http.MethodDelete {
			n++
		}
	}
	if n != 3 {
		t.Fatalf("DELETE 尝试数 = %d, want 3（有界重试——BK claude.go:189-224）", n)
	}
}

// ---------------------------------------------------------------------------
// ⑥ 停机有界收尾：取消在飞删除、有界等待、取消后不再重试
// ---------------------------------------------------------------------------

func TestClaudeWebCleanupShutdownBounded(t *testing.T) {
	up := newClaudeWebUpstream(t)
	up.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	up.deleteHang = true
	s, srv := startServiceWith(t, cfgClaudeWeb(up.srv.URL, ""))
	s.cwCleanupInterval = 20 * time.Millisecond // 若取消失效，第 2 次尝试会很快到达

	if code, _ := postChatStream(t, srv.URL); code != 200 {
		t.Fatalf("status = %d", code)
	}
	calls := waitForClaudeWebCalls(t, up, 4) // 第 1 次 DELETE 已发出（挂起中）
	if calls[3].method != http.MethodDelete {
		t.Fatalf("调用[3] = %s %s, want DELETE", calls[3].method, calls[3].path)
	}

	start := time.Now()
	if !s.ShutdownCleanup(3 * time.Second) {
		t.Fatalf("ShutdownCleanup 未收束（在飞删除泄漏）")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("ShutdownCleanup 耗时 %v（挂起的删除应立即被取消）", elapsed)
	}
	// 取消后不得再重试（有界收尾）。
	time.Sleep(100 * time.Millisecond)
	n := 0
	for _, c := range up.snapshot() {
		if c.method == http.MethodDelete {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("停机后 DELETE 尝试数 = %d, want 1（取消即止）", n)
	}
}
