// W5-33（L19 sweep16）：最小管理面（P63 裁定落地）的回归测试。
//
// 覆盖：惰性注册（未配 = 404）/ 鉴权（Bearer、X-Management-Key、401）/ 405 /
// 封禁（5 次失败 30 分钟）/ 诊断快照形状 / 冷却复位幂等（+404/400）/ ring 上限与
// limit 边界 / 四类事件。装置与既有测试一致：httptest 假上游 + 真服务 + 真 HTTP。
package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

// adminCfg 管理面配置（key 非空 = 注册管理面；上游由调用方给）。
func adminCfg(key string, ups ...Upstream) *Config {
	cfg := failoverCfg(ups...)
	cfg.AdminKey = key
	return cfg
}

// adminReq 发管理面请求（key 空 = 不带鉴权头；body 空 = 无体）。
func adminReq(t *testing.T, method, url, key, body string) *http.Response {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}

// adminBody 读响应体并关闭。
func adminBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// adminDiagnostics 取诊断快照（带密钥；非 200 即失败）。
func adminDiagnostics(t *testing.T, base, key string) adminDiagnosticsResponse {
	t.Helper()
	resp := adminReq(t, http.MethodGet, base+"/admin/diagnostics", key, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("diagnostics status = %d body=%s", resp.StatusCode, adminBody(t, resp))
	}
	var out adminDiagnosticsResponse
	if err := json.Unmarshal([]byte(adminBody(t, resp)), &out); err != nil {
		t.Fatalf("diagnostics decode: %v", err)
	}
	return out
}

// ---------------------------------------------------------------------------
// 1. 惰性注册：未配 admin_key = 三条路由不存在（404 非 401）
// ---------------------------------------------------------------------------

func TestAdminDisabledWithoutKey(t *testing.T) {
	good := goodClaudeUpstream(t)
	srv := startService(t, failoverCfg(anthUp("good", good.srv.URL))) // 无 AdminKey

	cases := []struct{ method, path string }{
		{http.MethodGet, "/admin/diagnostics"},
		{http.MethodPost, "/admin/cooldowns/reset"},
		{http.MethodGet, "/admin/logs"},
		// 未配时连方法不匹配也是 404（路由根本不存在——不是 405）。
		{http.MethodDelete, "/admin/logs"},
	}
	for _, c := range cases {
		resp := adminReq(t, c.method, srv.URL+c.path, "any-key", "{}")
		body := adminBody(t, resp)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s %s = %d（want 404：未配密钥 = 路由不注册）body=%s", c.method, c.path, resp.StatusCode, body)
		}
		if resp.StatusCode == http.StatusUnauthorized {
			t.Fatalf("%s %s = 401（未配必须 404 非 401）", c.method, c.path)
		}
	}
}

// ---------------------------------------------------------------------------
// 2. 鉴权：Bearer / X-Management-Key / 401 形状
// ---------------------------------------------------------------------------

func TestAdminAuthHeaders(t *testing.T) {
	good := goodClaudeUpstream(t)
	srv := startService(t, adminCfg("secret-key", anthUp("good", good.srv.URL)))
	const path = "/admin/diagnostics"

	// 无密钥 → 401（+ WWW-Authenticate，body 是管理面错误信封）。
	resp := adminReq(t, http.MethodGet, srv.URL+path, "", "")
	body := adminBody(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no key status = %d（want 401）body=%s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("WWW-Authenticate"); !strings.Contains(got, "Bearer") {
		t.Fatalf("WWW-Authenticate = %q（want Bearer 挑战）", got)
	}
	if !strings.Contains(body, `"authentication_error"`) {
		t.Fatalf("401 body 形状: %s", body)
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q（want no-store）", got)
	}

	// 错误密钥 → 401。
	resp = adminReq(t, http.MethodGet, srv.URL+path, "wrong-key", "")
	adminBody(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong key status = %d（want 401）", resp.StatusCode)
	}

	// Authorization: Bearer <key> → 200。
	resp = adminReq(t, http.MethodGet, srv.URL+path, "secret-key", "")
	adminBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bearer status = %d（want 200）", resp.StatusCode)
	}

	// X-Management-Key: <key> → 200（无 Authorization）。
	req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	req.Header.Set("X-Management-Key", "secret-key")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	adminBody(t, resp2)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("X-Management-Key status = %d（want 200）", resp2.StatusCode)
	}

	// 其它 scheme（Basic）不算提供密钥 → 401。
	req, _ = http.NewRequest(http.MethodGet, srv.URL+path, nil)
	req.Header.Set("Authorization", "Basic c2VjcmV0LWtleTo=")
	resp3, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	adminBody(t, resp3)
	if resp3.StatusCode != http.StatusUnauthorized {
		t.Fatalf("basic scheme status = %d（want 401）", resp3.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 3. 405：路径命中、方法不匹配（标准库 Allow 头）；未知 /admin 路径 404
// ---------------------------------------------------------------------------

func TestAdminMethodNotAllowed(t *testing.T) {
	good := goodClaudeUpstream(t)
	srv := startService(t, adminCfg("secret-key", anthUp("good", good.srv.URL)))

	cases := []struct {
		method, path, allow string
	}{
		{http.MethodPost, "/admin/diagnostics", "GET"},
		{http.MethodGet, "/admin/cooldowns/reset", "POST"},
		{http.MethodPost, "/admin/logs", "GET"},
	}
	for _, c := range cases {
		resp := adminReq(t, c.method, srv.URL+c.path, "secret-key", "{}")
		adminBody(t, resp)
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("%s %s = %d（want 405）", c.method, c.path, resp.StatusCode)
		}
		// 反证：405 = 「路径存在、方法不匹配」——不是 404（路径不存在）。
		if resp.StatusCode == http.StatusNotFound {
			t.Fatalf("%s %s = 404（路径存在，必须是 405）", c.method, c.path)
		}
		if got := resp.Header.Get("Allow"); !strings.Contains(got, c.allow) {
			t.Fatalf("%s %s Allow = %q（want 含 %s）", c.method, c.path, got, c.allow)
		}
	}

	// 反证：方法判定先于鉴权（错误密钥 + 错误方法 = 405 而非 401）——405 是路由面
	// 语义，不被中间件改写。
	resp := adminReq(t, http.MethodPost, srv.URL+"/admin/diagnostics", "wrong-key", "{}")
	adminBody(t, resp)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("wrong key + wrong method = %d（want 405：方法先于鉴权）", resp.StatusCode)
	}

	// 未注册的 /admin 子路径 → 404（不是 405/401）。
	resp = adminReq(t, http.MethodGet, srv.URL+"/admin/nope", "secret-key", "")
	adminBody(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown admin path = %d（want 404）", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 4. 封禁：连续 5 次失败 → 该 IP 30 分钟（正确密钥也被拒）；成功清零计数
// ---------------------------------------------------------------------------

func TestAdminAuthBan(t *testing.T) {
	good := goodClaudeUpstream(t)
	srv := startService(t, adminCfg("secret-key", anthUp("good", good.srv.URL)))
	const path = "/admin/diagnostics"

	// 4 次失败 + 1 次成功（清零）→ 阈值未达、且成功清零：正确密钥 200。
	for range 4 {
		resp := adminReq(t, http.MethodGet, srv.URL+path, "wrong-key", "")
		adminBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("pre-ban wrong key = %d（want 401）", resp.StatusCode)
		}
	}
	resp := adminReq(t, http.MethodGet, srv.URL+path, "secret-key", "")
	adminBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("成功鉴权 = %d（want 200：4 次失败未达阈值）", resp.StatusCode)
	}

	// 成功清零后 4 次失败仍不封禁（计数从 0 重新起算）。
	for range 4 {
		resp = adminReq(t, http.MethodGet, srv.URL+path, "wrong-key", "")
		adminBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("wrong key = %d（want 401）", resp.StatusCode)
		}
	}
	resp = adminReq(t, http.MethodGet, srv.URL+path, "secret-key", "")
	adminBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("成功清零后 4 次失败 = %d（want 200）", resp.StatusCode)
	}

	// 再来 5 次连续失败 → 封禁：正确密钥也被拒（401；封禁期与密钥错误同形）。
	for range 5 {
		resp = adminReq(t, http.MethodGet, srv.URL+path, "wrong-key", "")
		adminBody(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("ban-cycle wrong key = %d（want 401）", resp.StatusCode)
		}
	}
	resp = adminReq(t, http.MethodGet, srv.URL+path, "secret-key", "")
	adminBody(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("封禁期正确密钥 = %d（want 401：IP 已封禁）", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 5. 诊断快照形状（换上游后：冷却状态/失败计数/计数面）
// ---------------------------------------------------------------------------

func TestAdminDiagnosticsShape(t *testing.T) {
	bad := failUpstream(t, 503, nil, errBodyAPIError)
	good := goodClaudeUpstream(t)
	cfg := adminCfg("secret-key", anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL))
	cfg.CooldownFailures = 1 // 一次失败即冷却（测试快）
	srv := startService(t, cfg)

	// 一次请求：bad 503 → 换 good 成功（failover 事件 + bad 进入冷却）。
	if code, body := postChatStream(t, srv.URL); code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("chat status=%d body=\n%s", code, body)
	}

	resp := adminReq(t, http.MethodGet, srv.URL+"/admin/diagnostics", "secret-key", "")
	raw := adminBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("diagnostics status = %d body=%s", resp.StatusCode, raw)
	}
	// 凭据面绝不进管理响应（上游 api_key 是 "k-bad" / "k-good"）。
	for _, leak := range []string{"k-bad", "k-good", "api_key", "base_url"} {
		if strings.Contains(raw, leak) {
			t.Fatalf("诊断响应泄露凭据面 %q: %s", leak, raw)
		}
	}
	// 线上字段名（snake_case 契约）。
	for _, field := range []string{`"same_upstream_retries"`, `"attempts"`, `"failovers"`, `"cooldown_trips"`, `"upstreams"`, `"cooldown_remaining_seconds"`} {
		if !strings.Contains(raw, field) {
			t.Fatalf("诊断响应缺字段 %s: %s", field, raw)
		}
	}

	var snap adminDiagnosticsResponse
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if snap.Now.IsZero() {
		t.Fatalf("now 缺失（快照时间戳）")
	}
	if len(snap.Upstreams) != 2 {
		t.Fatalf("upstreams = %d（want 2）", len(snap.Upstreams))
	}
	b, g := snap.Upstreams[0], snap.Upstreams[1]
	if b.Name != "bad" || g.Name != "good" || b.Index != 0 || g.Index != 1 {
		t.Fatalf("upstreams 顺序/名字: %+v", snap.Upstreams)
	}
	if !b.Cooling || b.CooldownUntil == "" || b.CooldownRemainingSeconds <= 0 {
		t.Fatalf("bad 冷却状态: %+v", b)
	}
	if b.ConsecutiveFailures != 1 || b.WindowFailures != 1 || b.WindowRequests != 1 {
		t.Fatalf("bad 失败计数: %+v", b)
	}
	if g.Cooling || g.ConsecutiveFailures != 0 || g.CooldownUntil != "" {
		t.Fatalf("good 健康态: %+v", g)
	}
	if d := snap.Diagnostics; d.Failovers != 1 || d.Attempts != 2 || d.CooldownTrips != 1 || d.SameUpstreamRetries != 0 {
		t.Fatalf("diagnostics 计数: %+v", d)
	}
	if snap.Log.Capacity != DefaultAdminLogCapacity || snap.Log.Count == 0 || snap.Log.LatestSeq == 0 {
		t.Fatalf("log 统计: %+v", snap.Log)
	}
}

// ---------------------------------------------------------------------------
// 6. 冷却复位：幂等 + 404（未知目标）+ 400（缺参/歧义）
// ---------------------------------------------------------------------------

func TestAdminCooldownResetIdempotent(t *testing.T) {
	bad1 := failUpstream(t, 503, nil, errBodyAPIError)
	bad2 := failUpstream(t, 503, nil, errBodyAPIError)
	cfg := adminCfg("secret-key", anthUp("bad1", bad1.srv.URL), anthUp("bad2", bad2.srv.URL))
	cfg.CooldownFailures = 1
	srv := startService(t, cfg)

	// 触发冷却：两台都进冷却（bad1 失败换 bad2；bad2 末台失败状态直写 503）。
	if code, _ := postChatStream(t, srv.URL); code != http.StatusServiceUnavailable {
		t.Fatalf("chat status = %d（want 503：末台上游失败状态直写）", code)
	}
	if snap := adminDiagnostics(t, srv.URL, "secret-key"); !snap.Upstreams[0].Cooling || !snap.Upstreams[1].Cooling {
		t.Fatalf("复位前两台都应冷却: %+v", snap.Upstreams)
	}

	// 按名复位 bad1：was_cooling=true。
	resp := adminReq(t, http.MethodPost, srv.URL+"/admin/cooldowns/reset", "secret-key", `{"upstream":"bad1"}`)
	raw := adminBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reset status = %d body=%s", resp.StatusCode, raw)
	}
	var out adminResetResponse
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("reset decode: %v", err)
	}
	if out.Status != "ok" || out.Count != 1 || len(out.Reset) != 1 || !out.Reset[0].WasCooling || out.Reset[0].Name != "bad1" {
		t.Fatalf("reset 响应: %+v", out)
	}
	snap := adminDiagnostics(t, srv.URL, "secret-key")
	if snap.Upstreams[0].Cooling || snap.Upstreams[0].ConsecutiveFailures != 0 || snap.Upstreams[0].LadderStep != 0 {
		t.Fatalf("复位后健康态未清: %+v", snap.Upstreams[0])
	}
	// 反证：按名复位只清指定上游——bad2 仍冷却且失败计数保留。
	if !snap.Upstreams[1].Cooling || snap.Upstreams[1].ConsecutiveFailures != 1 {
		t.Fatalf("反证失败：bad1 的复位波及了 bad2: %+v", snap.Upstreams[1])
	}

	// 幂等：重复复位仍 200，was_cooling=false。
	resp = adminReq(t, http.MethodPost, srv.URL+"/admin/cooldowns/reset", "secret-key", `{"index":0}`)
	raw = adminBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("repeat reset status = %d body=%s", resp.StatusCode, raw)
	}
	out = adminResetResponse{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("repeat decode: %v", err)
	}
	if out.Count != 1 || out.Reset[0].WasCooling {
		t.Fatalf("重复复位（幂等）: %+v", out)
	}

	// 全部复位（{"all":true}）→ 200；两台都不再冷却。
	resp = adminReq(t, http.MethodPost, srv.URL+"/admin/cooldowns/reset", "secret-key", `{"all":true}`)
	adminBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reset all status = %d", resp.StatusCode)
	}
	if snap := adminDiagnostics(t, srv.URL, "secret-key"); snap.Upstreams[0].Cooling || snap.Upstreams[1].Cooling {
		t.Fatalf("全量复位后仍冷却: %+v", snap.Upstreams)
	}

	// 未知目标 → 404；缺参/歧义/非法 JSON/未知字段 → 400。
	cases := []struct {
		body string
		want int
	}{
		{`{"upstream":"ghost"}`, http.StatusNotFound},
		{`{"index":5}`, http.StatusNotFound},
		{`{}`, http.StatusBadRequest},
		{`{"all":true,"upstream":"bad"}`, http.StatusBadRequest},
		{`{"upstreem":"bad"}`, http.StatusBadRequest},
		{`{`, http.StatusBadRequest},
	}
	for _, c := range cases {
		resp = adminReq(t, http.MethodPost, srv.URL+"/admin/cooldowns/reset", "secret-key", c.body)
		body := adminBody(t, resp)
		if resp.StatusCode != c.want {
			t.Fatalf("reset %s = %d（want %d）body=%s", c.body, resp.StatusCode, c.want, body)
		}
	}
}

// ---------------------------------------------------------------------------
// 7. 错误日志只读：四类事件 + limit 边界 + 新→旧
// ---------------------------------------------------------------------------

func TestAdminLogsRing(t *testing.T) {
	// 服务 A：换上游（bad 503 → good）+ 冷却进入（阈值 1）。
	bad := failUpstream(t, 503, nil, errBodyAPIError)
	good := goodClaudeUpstream(t)
	cfgA := adminCfg("secret-key", anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL))
	cfgA.CooldownFailures = 1
	srvA := startService(t, cfgA)
	if code, _ := postChatStream(t, srvA.URL); code != 200 {
		t.Fatalf("chat A status = %d", code)
	}
	// 复位 → cooldown_reset 事件。
	resp := adminReq(t, http.MethodPost, srvA.URL+"/admin/cooldowns/reset", "secret-key", `{"all":true}`)
	adminBody(t, resp)

	// 服务 B：提交前错误（400 + x-should-retry:false → 错误信封直写，noRetry）。
	hardFail := failUpstream(t, 400, map[string]string{"x-should-retry": "false"}, errBodyAPIError)
	srvB := startService(t, adminCfg("secret-key", anthUp("hard", hardFail.srv.URL)))
	if code, _ := postChatStream(t, srvB.URL); code != 400 {
		t.Fatalf("chat B status = %d（want 400）", code)
	}

	// A：事件新→旧 = 复位（good）→ 复位（bad）→ 换上游 → 冷却进入
	//（serve 内健康记账先于换上游决策——事件序 = 真实发生时序）。
	resp = adminReq(t, http.MethodGet, srvA.URL+"/admin/logs", "secret-key", "")
	raw := adminBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("logs status = %d body=%s", resp.StatusCode, raw)
	}
	var logs adminLogsResponse
	if err := json.Unmarshal([]byte(raw), &logs); err != nil {
		t.Fatalf("logs decode: %v", err)
	}
	if logs.Capacity != DefaultAdminLogCapacity || logs.Count < 4 {
		t.Fatalf("logs stats: %+v", logs)
	}
	wantKinds := []string{adminLogCooldownReset, adminLogCooldownReset, adminLogFailover, adminLogCooldownTrip}
	if len(logs.Entries) < len(wantKinds) {
		t.Fatalf("entries = %d（want ≥%d）", len(logs.Entries), len(wantKinds))
	}
	for i, want := range wantKinds {
		if logs.Entries[i].Kind != want {
			t.Fatalf("entries[%d].kind = %q（want %q）: %+v", i, logs.Entries[i].Kind, want, logs.Entries)
		}
	}
	if logs.Entries[0].Seq <= logs.Entries[1].Seq || logs.Entries[1].Seq <= logs.Entries[2].Seq {
		t.Fatalf("entries 非新→旧: %+v", logs.Entries)
	}
	// 复位事件覆盖两台上游；换上游事件带状态码与 request_id；冷却事件带上游名。
	resetNames := map[string]bool{logs.Entries[0].Upstream: true, logs.Entries[1].Upstream: true}
	if !resetNames["bad"] || !resetNames["good"] {
		t.Fatalf("复位事件上游 = %+v（want bad+good）", resetNames)
	}
	if logs.Entries[2].Upstream != "bad" || logs.Entries[2].Status != 503 || logs.Entries[2].RequestID == "" {
		t.Fatalf("换上游事件: %+v", logs.Entries[2])
	}
	if logs.Entries[3].Upstream != "bad" {
		t.Fatalf("冷却进入事件: %+v", logs.Entries[3])
	}

	// B：precommit_error（状态 400 带 request_id）。
	resp = adminReq(t, http.MethodGet, srvB.URL+"/admin/logs", "secret-key", "")
	rawB := adminBody(t, resp)
	logsB := adminLogsResponse{}
	if err := json.Unmarshal([]byte(rawB), &logsB); err != nil {
		t.Fatalf("logs B decode: %v", err)
	}
	if len(logsB.Entries) == 0 || logsB.Entries[0].Kind != adminLogPrecommitError || logsB.Entries[0].Status != 400 {
		t.Fatalf("B entries: %+v", logsB.Entries)
	}
	if logsB.Entries[0].RequestID == "" {
		t.Fatalf("precommit_error 缺 request_id: %+v", logsB.Entries[0])
	}

	// limit 边界：1 → 1 条；0/负数/非整数 → 400；超容量 → 截到容量。
	resp = adminReq(t, http.MethodGet, srvA.URL+"/admin/logs?limit=1", "secret-key", "")
	raw = adminBody(t, resp)
	one := adminLogsResponse{}
	if err := json.Unmarshal([]byte(raw), &one); err != nil {
		t.Fatalf("limit=1 decode: %v", err)
	}
	if len(one.Entries) != 1 || one.Entries[0].Seq != logs.Entries[0].Seq {
		t.Fatalf("limit=1: %+v", one.Entries)
	}
	for _, q := range []string{"limit=0", "limit=-3", "limit=abc"} {
		resp = adminReq(t, http.MethodGet, srvA.URL+"/admin/logs?"+q, "secret-key", "")
		adminBody(t, resp)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s = %d（want 400）", q, resp.StatusCode)
		}
	}
	resp = adminReq(t, http.MethodGet, srvA.URL+"/admin/logs?limit=100000", "secret-key", "")
	raw = adminBody(t, resp)
	big := adminLogsResponse{}
	if err := json.Unmarshal([]byte(raw), &big); err != nil {
		t.Fatalf("limit=100000 decode: %v", err)
	}
	if len(big.Entries) > DefaultAdminLogCapacity || big.Capacity != DefaultAdminLogCapacity {
		t.Fatalf("limit 超容量未截: len=%d cap=%d", len(big.Entries), big.Capacity)
	}
}

// ---------------------------------------------------------------------------
// 8. ring 上限（纯单元）：覆盖写最旧 + 新→旧 + 空 ring
// ---------------------------------------------------------------------------

func TestAdminLogRingBounded(t *testing.T) {
	var r adminLogRing
	if got := r.window(0, "", 5, false).Entries; len(got) != 0 {
		t.Fatalf("空 ring entries = %d（want 0）", len(got))
	}
	total := DefaultAdminLogCapacity + 10
	for i := range total {
		r.add(adminLogFailover, "u", "", fmt.Sprintf("m%d", i), 0)
	}
	count, latest := r.stats()
	if count != DefaultAdminLogCapacity || latest != uint64(total) {
		t.Fatalf("stats = (%d, %d)（want (%d, %d)）", count, latest, DefaultAdminLogCapacity, total)
	}
	es := r.window(0, "", 0, false).Entries
	if len(es) != DefaultAdminLogCapacity {
		t.Fatalf("window(0).Entries = %d（want %d）", len(es), DefaultAdminLogCapacity)
	}
	if es[0].Seq != uint64(total) || es[len(es)-1].Seq != uint64(total-DefaultAdminLogCapacity+1) {
		t.Fatalf("覆盖写边界: first=%d last=%d", es[0].Seq, es[len(es)-1].Seq)
	}
	for i := 1; i < len(es); i++ {
		if es[i].Seq >= es[i-1].Seq {
			t.Fatalf("非新→旧 @%d: %d >= %d", i, es[i].Seq, es[i-1].Seq)
		}
	}
	if got := r.window(0, "", 7, false).Entries; len(got) != 7 {
		t.Fatalf("window(0).Entries(7) = %d", len(got))
	}
}
