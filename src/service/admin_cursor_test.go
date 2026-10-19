// W5-36（L19 sweep19）：管理面补全的回归测试——日志游标（`since_id` /
// `next_cursor` / `cursor_reset`）、鉴权面读面（累计失败计数 + 封禁 IP/到期）、
// ring 容量配置化（`admin_log_capacity`）。
//
// 装置与 admin_test.go 一致（httptest 真服务 + 真 HTTP）；鉴权面读面用
// httptest.NewRequest + 显式 RemoteAddr（封禁按客户端 IP 记账，读面必须能从另一个
// IP 读到——被封的 IP 自己连读面都进不去）。
package service

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// adminLogsRead 取 /admin/logs（query 形如 "" 或 "?since_id=…"；非 200 即失败）。
func adminLogsRead(t *testing.T, base, key, query string) adminLogsResponse {
	t.Helper()
	resp := adminReq(t, http.MethodGet, base+"/admin/logs"+query, key, "")
	raw := adminBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("logs%s status = %d body=%s", query, resp.StatusCode, raw)
	}
	var out adminLogsResponse
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("logs decode: %v", err)
	}
	return out
}

// adminResetAll 复位全部上游（单上游配置下每次调用 = 一条 cooldown_reset 事件）。
func adminResetAll(t *testing.T, base, key string) {
	t.Helper()
	resp := adminReq(t, http.MethodPost, base+"/admin/cooldowns/reset", key, `{"all":true}`)
	body := adminBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reset all status = %d body=%s", resp.StatusCode, body)
	}
}

// ---------------------------------------------------------------------------
// 1. 游标增量读：next_cursor → since_id → 只取新增（升序）；十进制锚等价
// ---------------------------------------------------------------------------

func TestAdminLogsCursorIncrement(t *testing.T) {
	good := goodClaudeUpstream(t)
	srv := startService(t, adminCfg("secret-key", anthUp("good", good.srv.URL)))

	adminResetAll(t, srv.URL, "secret-key")
	adminResetAll(t, srv.URL, "secret-key")

	// 缺省读：新→旧（W5-33 契约不变）+ next_cursor 恒带。
	first := adminLogsRead(t, srv.URL, "secret-key", "")
	if len(first.Entries) != 2 || first.NextCursor == "" {
		t.Fatalf("缺省读: entries=%d cursor=%q", len(first.Entries), first.NextCursor)
	}
	if first.CursorReset {
		t.Fatalf("缺省读不该 cursor_reset: %+v", first)
	}
	if first.Entries[0].Seq <= first.Entries[1].Seq {
		t.Fatalf("缺省读必须保持新→旧: %+v", first.Entries)
	}

	// 无新事件的增量读（令牌形态）：空 + 游标不前进 + 不重置。
	same := adminLogsRead(t, srv.URL, "secret-key", "?since_id="+url.QueryEscape(first.NextCursor))
	if len(same.Entries) != 0 || same.CursorReset {
		t.Fatalf("无新事件增量读: entries=%d reset=%v", len(same.Entries), same.CursorReset)
	}
	if same.NextCursor != first.NextCursor {
		t.Fatalf("无新事件时游标不应前进: %q → %q", first.NextCursor, same.NextCursor)
	}

	// 新事件 → 只返回新增（升序）+ 游标前进。
	adminResetAll(t, srv.URL, "secret-key")
	inc := adminLogsRead(t, srv.URL, "secret-key", "?since_id="+url.QueryEscape(first.NextCursor))
	if len(inc.Entries) != 1 || inc.Entries[0].Kind != adminLogCooldownReset {
		t.Fatalf("增量读 = %+v", inc.Entries)
	}
	if inc.NextCursor == first.NextCursor {
		t.Fatalf("有新事件后游标必须前进")
	}

	// 十进制锚（= 刚返回那条的 seq）：无更新 → 空。
	dec := adminLogsRead(t, srv.URL, "secret-key", fmt.Sprintf("?since_id=%d", inc.Entries[0].Seq))
	if len(dec.Entries) != 0 || dec.CursorReset {
		t.Fatalf("十进制锚: entries=%d reset=%v", len(dec.Entries), dec.CursorReset)
	}

	// since_id=0 → 全量升序（与缺省读的新→旧相反）。
	all := adminLogsRead(t, srv.URL, "secret-key", "?since_id=0")
	if len(all.Entries) != 3 || all.Entries[0].Seq != 1 {
		t.Fatalf("since_id=0 = %+v", all.Entries)
	}
	for i := 1; i < len(all.Entries); i++ {
		if all.Entries[i].Seq <= all.Entries[i-1].Seq {
			t.Fatalf("增量读必须升序 @%d: %d <= %d", i, all.Entries[i].Seq, all.Entries[i-1].Seq)
		}
	}
	// limit 在增量读同样生效（取最早的 limit 条 = 消费端积压排空语义）。
	one := adminLogsRead(t, srv.URL, "secret-key", "?since_id=0&limit=1")
	if len(one.Entries) != 1 || one.Entries[0].Seq != 1 {
		t.Fatalf("增量读 limit=1 = %+v", one.Entries)
	}
}

// ---------------------------------------------------------------------------
// 2. cursor_reset：覆盖丢弃 / 超前锚 → 重置回落；边界锚不重置（反证）
// ---------------------------------------------------------------------------

func TestAdminLogsCursorResetOnOverwrite(t *testing.T) {
	good := goodClaudeUpstream(t)
	cfg := adminCfg("secret-key", anthUp("good", good.srv.URL))
	cap3 := 3
	cfg.AdminLogCapacity = &cap3
	srv := startService(t, cfg)

	adminResetAll(t, srv.URL, "secret-key") // seq 1
	first := adminLogsRead(t, srv.URL, "secret-key", "")
	if len(first.Entries) != 1 || first.Entries[0].Seq != 1 {
		t.Fatalf("首条: %+v", first.Entries)
	}
	cursor1 := first.NextCursor

	for range 4 { // seq 2..5 → 覆盖掉 1/2
		adminResetAll(t, srv.URL, "secret-key")
	}

	// 容量配置生效（HTTP 面：诊断 log.capacity + limit 上限）。
	snap := adminDiagnostics(t, srv.URL, "secret-key")
	if snap.Log.Capacity != 3 || snap.Log.Count != 3 || snap.Log.LatestSeq != 5 {
		t.Fatalf("容量 3 的 ring 统计: %+v", snap.Log)
	}
	big := adminLogsRead(t, srv.URL, "secret-key", "?limit=100000")
	if big.Capacity != 3 || len(big.Entries) != 3 {
		t.Fatalf("limit 未按配置容量截断: cap=%d len=%d", big.Capacity, len(big.Entries))
	}

	// 被覆盖的锚（seq 1）→ cursor_reset + 最新 limit 条（升序）。
	got := adminLogsRead(t, srv.URL, "secret-key", "?since_id="+url.QueryEscape(cursor1))
	if !got.CursorReset {
		t.Fatalf("被覆盖的锚必须 cursor_reset: %+v", got)
	}
	if len(got.Entries) != 3 || got.Entries[0].Seq != 3 || got.Entries[2].Seq != 5 {
		t.Fatalf("重置回落 = 最新 limit 条升序: %+v", got.Entries)
	}
	// 反证：边界锚（= 最早可用 - 1 = 2）不重置——3/4/5 全可用，没有丢东西。
	edge := adminLogsRead(t, srv.URL, "secret-key", "?since_id=2")
	if edge.CursorReset {
		t.Fatalf("边界锚不该重置（3/4/5 全可用）: %+v", edge)
	}
	if len(edge.Entries) != 3 || edge.Entries[0].Seq != 3 {
		t.Fatalf("边界锚增量读: %+v", edge.Entries)
	}
	// 反证：超前锚（> latest）→ 重置（本实例发出的锚恒 ≤ 发出时的 latest；静默返回
	// 空会让客户端永远收不到新事件）。
	ahead := adminLogsRead(t, srv.URL, "secret-key", "?since_id=99")
	if !ahead.CursorReset {
		t.Fatalf("超前锚必须 cursor_reset: %+v", ahead)
	}
}

// ---------------------------------------------------------------------------
// 3. 指纹（跨实例令牌失效）+ 坏 since_id → 400（绝不静默当 0 = 全量重放）
// ---------------------------------------------------------------------------

func TestAdminLogsCursorForeignAndInvalid(t *testing.T) {
	good := goodClaudeUpstream(t)
	srvA := startService(t, adminCfg("key-a", anthUp("good", good.srv.URL)))
	adminResetAll(t, srvA.URL, "key-a")
	tokenA := adminLogsRead(t, srvA.URL, "key-a", "").NextCursor
	if tokenA == "" {
		t.Fatalf("next_cursor 缺失")
	}

	// 另一个实例（另一个进程的等价物）用 A 的令牌 → 指纹不符 → 重置回落。
	srvB := startService(t, adminCfg("key-b", anthUp("good", good.srv.URL)))
	adminResetAll(t, srvB.URL, "key-b")
	foreign := adminLogsRead(t, srvB.URL, "key-b", "?since_id="+url.QueryEscape(tokenA))
	if !foreign.CursorReset {
		t.Fatalf("跨实例令牌必须 cursor_reset（指纹不符）: %+v", foreign)
	}
	if len(foreign.Entries) != 1 {
		t.Fatalf("重置回落 = 最新 limit 条: %+v", foreign.Entries)
	}
	// 反证：B 自己刚拿到的令牌不重置（指纹相符）。
	own := adminLogsRead(t, srvB.URL, "key-b", "?since_id="+url.QueryEscape(foreign.NextCursor))
	if own.CursorReset {
		t.Fatalf("同实例令牌不该重置: %+v", own)
	}
	// 反证：十进制锚不带指纹（文档化差异）——B 的 seq 空间里 0 是有效锚。
	decB := adminLogsRead(t, srvB.URL, "key-b", "?since_id=0")
	if decB.CursorReset || len(decB.Entries) != 1 {
		t.Fatalf("十进制锚: reset=%v entries=%d", decB.CursorReset, len(decB.Entries))
	}

	// 坏 since_id → 400。
	badVersion := base64.RawURLEncoding.EncodeToString([]byte(`{"v":9,"seq":1,"fp":"x"}`))
	badPayload := base64.RawURLEncoding.EncodeToString([]byte("not json"))
	for _, q := range []string{
		"?since_id=abc",                  // 非十进制、非 JSON 载荷
		"?since_id=-1",                   // 负数
		"?since_id=1.5",                  // 小数
		"?since_id=18446744073709551616", // uint64 溢出
		"?since_id=" + badVersion,        // 版本不符
		"?since_id=" + badPayload,        // 坏载荷
		"?since_id=!!!",                  // 坏编码
	} {
		resp := adminReq(t, http.MethodGet, srvA.URL+"/admin/logs"+q, "key-a", "")
		body := adminBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("since_id %s = %d（want 400）body=%s", q, resp.StatusCode, body)
		}
	}
}

// ---------------------------------------------------------------------------
// 4. 鉴权面读面：累计失败计数 + 封禁 IP/到期（封禁按 IP 记账——读面须换 IP 读）
// ---------------------------------------------------------------------------

func TestAdminDiagnosticsAuthBlock(t *testing.T) {
	good := goodClaudeUpstream(t)
	cfg := adminCfg("secret-key", anthUp("good", good.srv.URL))
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	h := New(cfg).Handler()

	const bannedIP = "198.51.100.9:41001" // 触发封禁的客户端
	const readerIP = "203.0.113.7:41002"  // 读诊断的另一个客户端

	call := func(ip, key string) (int, string) {
		req := httptest.NewRequest(http.MethodGet, "/admin/diagnostics", nil)
		req.RemoteAddr = ip
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	snapshot := func(ip string) adminDiagnosticsResponse {
		t.Helper()
		code, body := call(ip, "secret-key")
		if code != http.StatusOK {
			t.Fatalf("diagnostics(%s) = %d body=%s", ip, code, body)
		}
		var out adminDiagnosticsResponse
		if err := json.Unmarshal([]byte(body), &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return out
	}

	// 初始：无失败、无封禁、无状态表条目。
	if snap := snapshot(readerIP); snap.Admin.AuthFailures != 0 || snap.Admin.TrackedIPs != 0 || len(snap.Admin.BannedIPs) != 0 {
		t.Fatalf("初始鉴权面: %+v", snap.Admin)
	}

	// 1 次失败 → 读面可见（累计计数 + 状态表条目），但还没封禁。
	if code, _ := call(bannedIP, "wrong"); code != http.StatusUnauthorized {
		t.Fatalf("wrong key = %d（want 401）", code)
	}
	snap := snapshot(readerIP)
	if snap.Admin.AuthFailures != 1 || snap.Admin.TrackedIPs != 1 || len(snap.Admin.BannedIPs) != 0 {
		t.Fatalf("1 次失败后鉴权面: %+v", snap.Admin)
	}

	// 补到阈值（5 次）→ 封禁；读面（另一个 IP）能看到 IP 与到期。
	for range 4 {
		call(bannedIP, "wrong")
	}
	snap = snapshot(readerIP)
	if snap.Admin.AuthFailures != 5 || len(snap.Admin.BannedIPs) != 1 {
		t.Fatalf("封禁后鉴权面: %+v", snap.Admin)
	}
	b := snap.Admin.BannedIPs[0]
	if b.IP != "198.51.100.9" { // 只回 host 段（不含端口）
		t.Fatalf("封禁 IP = %q（want 198.51.100.9）", b.IP)
	}
	until, err := time.Parse(time.RFC3339, b.BannedUntil)
	if err != nil {
		t.Fatalf("banned_until 不是 RFC3339: %q", b.BannedUntil)
	}
	if d := time.Until(until); d <= 25*time.Minute || d > 30*time.Minute {
		t.Fatalf("封禁窗口 = %v（want ≈30m）", d)
	}
	if b.RemainingSeconds <= 0 || b.RemainingSeconds > 30*60 {
		t.Fatalf("remaining_seconds = %v", b.RemainingSeconds)
	}

	// 反证：封禁期该 IP 的正确密钥也 401——连读面都读不到（封禁先于密钥比较）。
	if code, _ := call(bannedIP, "secret-key"); code != http.StatusUnauthorized {
		t.Fatalf("封禁 IP 正确密钥 = %d（want 401）", code)
	}
	// 反证：读面只回 IP/计数/到期，绝不回密钥。
	code, raw := call(readerIP, "secret-key")
	if code != http.StatusOK || strings.Contains(raw, "secret-key") {
		t.Fatalf("鉴权面泄露密钥或状态 %d: %s", code, raw)
	}
	if !strings.Contains(raw, `"auth_failures"`) || !strings.Contains(raw, `"banned_ips"`) {
		t.Fatalf("鉴权面字段缺失: %s", raw)
	}
	// 反证：状态表按 IP 记账——另一个 IP 不受影响。
	if code, _ := call(readerIP, "secret-key"); code != http.StatusOK {
		t.Fatalf("另一个 IP 被误封: %d", code)
	}
}

// ---------------------------------------------------------------------------
// 5. admin_log_capacity：缺省 256 / 显式生效 / 越界报错（含边界反证）
// ---------------------------------------------------------------------------

func TestAdminLogCapacityConfig(t *testing.T) {
	dir := t.TempDir()
	n := 0
	write := func(t *testing.T, body string) string {
		t.Helper()
		n++
		p := filepath.Join(dir, fmt.Sprintf("cfg-%d.yaml", n))
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatalf("write config: %v", err)
		}
		return p
	}
	base := "listen: \"127.0.0.1:0\"\nupstreams:\n  - name: u\n    base_url: http://127.0.0.1:9/v1\n    api_key: k\n    protocol: anthropic\n"

	// 缺省：未写 → 256（常量缺省面）。
	c, err := LoadConfig(write(t, base))
	if err != nil {
		t.Fatalf("load default: %v", err)
	}
	if got := c.adminLogCapacity(); got != DefaultAdminLogCapacity {
		t.Fatalf("未写 admin_log_capacity = %d（want %d）", got, DefaultAdminLogCapacity)
	}
	if got := New(c).adminLog.capacityOrDefault(); got != DefaultAdminLogCapacity {
		t.Fatalf("未配时 ring 容量 = %d", got)
	}

	// 显式 3 → 生效（ring 有界 + 配置面可读）。
	c3, err := LoadConfig(write(t, base+"admin_log_capacity: 3\n"))
	if err != nil {
		t.Fatalf("load 3: %v", err)
	}
	if c3.adminLogCapacity() != 3 {
		t.Fatalf("显式容量未生效: %d", c3.adminLogCapacity())
	}
	s3 := New(c3)
	if got := s3.adminLog.capacityOrDefault(); got != 3 {
		t.Fatalf("ring 容量 = %d（want 3）", got)
	}
	for i := range 5 {
		s3.adminLog.add(adminLogFailover, "u", "", fmt.Sprintf("m%d", i), 0)
	}
	if count, latest := s3.adminLog.stats(); count != 3 || latest != 5 {
		t.Fatalf("容量 3 的 ring: count=%d latest=%d", count, latest)
	}

	// 边界合法（反证：不是「只有 256 合法」）。
	for _, cap := range []int{1, 4096} {
		if _, err := LoadConfig(write(t, base+fmt.Sprintf("admin_log_capacity: %d\n", cap))); err != nil {
			t.Fatalf("admin_log_capacity=%d 应合法: %v", cap, err)
		}
	}
	// 越界 → 配置错误（0/负值/超上限都不静默降级）。
	for _, cap := range []int{0, -1, 4097} {
		if _, err := LoadConfig(write(t, base+fmt.Sprintf("admin_log_capacity: %d\n", cap))); err == nil {
			t.Fatalf("admin_log_capacity=%d 应报错（越界校验）", cap)
		}
	}
}

// ---------------------------------------------------------------------------
// 6. 游标/窗口纯单元：边界语义 + 令牌编解码 round-trip
// ---------------------------------------------------------------------------

func TestAdminLogWindowUnit(t *testing.T) {
	var r adminLogRing
	r.capacity = 3
	for i := range 5 {
		r.add(adminLogFailover, "u", "", fmt.Sprintf("m%d", i), 0)
	}

	// 缺省读：最新 limit 条，新→旧；next = 最新序号。
	w := r.window(0, "", 10, false)
	if len(w.Entries) != 3 || w.Entries[0].Seq != 5 || w.Entries[2].Seq != 3 || w.Reset || w.NextSeq != 5 {
		t.Fatalf("缺省读: %+v", w)
	}

	// 增量读边界（容量 3：可用序号 3..5）。
	cases := []struct {
		anchor uint64
		want   []uint64
		reset  bool
	}{
		{5, nil, false},               // 已到最新 → 空，不重置
		{4, []uint64{5}, false},       // 一条新增
		{2, []uint64{3, 4, 5}, false}, // 边界：最早可用 - 1 → 无丢弃
		{1, []uint64{3, 4, 5}, true},  // 覆盖丢弃 → 重置回落
		{9, []uint64{3, 4, 5}, true},  // 超前锚 → 重置回落
	}
	for _, c := range cases {
		got := r.window(c.anchor, "", 10, true)
		if got.Reset != c.reset {
			t.Fatalf("anchor=%d reset=%v（want %v）", c.anchor, got.Reset, c.reset)
		}
		if len(got.Entries) != len(c.want) {
			t.Fatalf("anchor=%d entries=%d（want %d）", c.anchor, len(got.Entries), len(c.want))
		}
		for i := range c.want {
			if got.Entries[i].Seq != c.want[i] {
				t.Fatalf("anchor=%d entries[%d]=%d（want %d）", c.anchor, i, got.Entries[i].Seq, c.want[i])
			}
		}
	}

	// 指纹：相符不重置；外来指纹重置（即使锚本身有效）。
	fp := r.fingerprint()
	if got := r.window(4, fp, 10, true); got.Reset {
		t.Fatalf("本实例指纹不该重置: %+v", got)
	}
	if got := r.window(4, "deadbeefdeadbeef", 10, true); !got.Reset {
		t.Fatalf("外来指纹必须重置: %+v", got)
	}

	// 令牌 round-trip + 坏输入拒绝。
	tok := encodeAdminLogCursor(adminLogCursor{Version: adminLogCursorVersion, Seq: 7, FP: fp})
	c, err := decodeAdminLogCursor(tok)
	if err != nil || c.Seq != 7 || c.FP != fp || c.Version != adminLogCursorVersion {
		t.Fatalf("round-trip: %+v err=%v", c, err)
	}
	if _, err := decodeAdminLogCursor("!!!"); err == nil {
		t.Fatalf("坏编码必须报错")
	}
	if seq, fp2, err := adminLogAnchor("42"); err != nil || seq != 42 || fp2 != "" {
		t.Fatalf("十进制锚: seq=%d fp=%q err=%v", seq, fp2, err)
	}
	if _, _, err := adminLogAnchor("42x"); err == nil {
		t.Fatalf("非法锚必须报错")
	}
}
