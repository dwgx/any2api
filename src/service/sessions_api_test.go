package service

// sessions_api_test.go —— P1-3 会话/消息 API 测试（board A2A-L19-W65-1）：
// 16 例验收（规格 §2.8）+ Store 契约（§2.4.3）+ turn 台账（§2.6.4）。
//
// 装置：真 HTTP（httptest.NewServer）+ cookie jar 会话；SQLite 一律
// t.TempDir()；限流/时钟用 fakeClock；聊天面用既有 goodClaudeUpstream /
// failUpstream 假上游。
//
// 反证锚点（回退即红）：
//   - ConversationSave 去掉 rev CAS → TestSessionsAPIRevConflict409 /
//     TestSessionsAPIConcurrentPutOneWins 红；
//   - 路由不注册 /ui/api/sessions* → TestSessionsAPICreateListGet 红（SPA 200）；
//   - store 未启用不回 503 → TestSessionsAPIStoreDisabled503 红；
//   - ConversationSoftDelete 不 bump rev/updated_at →
//     TestSessionsAPIDeleteTombstoneAndIdempotent 红；
//   - recordTurn 不落 turns → TestTurnHookJoinAttempts 红。

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

// sessionsResp 一发响应（状态码 + 原文 + 头）。
type sessionsResp struct {
	status int
	body   []byte
	header http.Header
}

// sessionsJSON 解码响应体。
func sessionsJSON(t *testing.T, raw []byte, dst any) {
	t.Helper()
	if err := json.Unmarshal(raw, dst); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
}

// sessionsSetCookieValue 取 Set-Cookie 里的 a2a_sid 值。
func sessionsSetCookieValue(h http.Header) string {
	for _, line := range h.Values("Set-Cookie") {
		if !strings.HasPrefix(line, sessionCookieName+"=") {
			continue
		}
		rest := strings.TrimPrefix(line, sessionCookieName+"=")
		if i := strings.IndexByte(rest, ';'); i >= 0 {
			rest = rest[:i]
		}
		return rest
	}
	return ""
}

// sessionsClient 带 cookie jar 的客户端（一次测试 = 一个浏览器会话）。
func sessionsClient(t *testing.T) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar: %v", err)
	}
	return &http.Client{Jar: jar, Timeout: 30 * time.Second}
}

// sessionsReq 发一发请求（body 为 nil = 无体；ct 非空 = 设置 Content-Type）。
func sessionsReq(t *testing.T, c *http.Client, method, url, ct string, body []byte, headers map[string]string) sessionsResp {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, url, err)
	}
	if ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s %s: %v", method, url, err)
	}
	return sessionsResp{status: resp.StatusCode, body: raw, header: resp.Header}
}

// sessionsJSONReq 发 JSON 请求（map/struct 自动编码）。
func sessionsJSONReq(t *testing.T, c *http.Client, method, url string, body any) sessionsResp {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return sessionsReq(t, c, method, url, "application/json", raw, nil)
}

// sessionsAPIConfig 最小可用配置：public 开 + store=sqlite（临时目录）。
func sessionsAPIConfig(t *testing.T) *Config {
	t.Helper()
	cfg := uiTestConfig()
	cfg.HeartbeatSeconds = 15
	cfg.Public = &PublicSection{Enabled: true}
	cfg.Store = &StoreSection{Enabled: true, Path: filepath.Join(t.TempDir(), "any2api.db")}
	return cfg
}

// sessionsAPIStart 启动被测服务（返回 Server + httptest 地址；Cleanup 收尾）。
func sessionsAPIStart(t *testing.T, cfg *Config) (*Server, *httptest.Server) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(func() {
		srv.Close()
		_ = s.CloseStore()
	})
	return s, srv
}

// sessionsCreateConv 建会话并断言 201。
func sessionsCreateConv(t *testing.T, c *http.Client, base, id, title string) sessionView {
	t.Helper()
	resp := sessionsJSONReq(t, c, http.MethodPost, base+sessionsAPIPrefix, map[string]any{
		"id": id, "title": title, "model": "claude-sonnet-5-5",
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("create %s: status %d body=%s", id, resp.status, resp.body)
	}
	var env sessionEnvelope
	sessionsJSON(t, resp.body, &env)
	return env.Session
}

// sessionsCountTurns 直接数 turns 表（测试断言用；同包可触 db）。
func sessionsCountTurns(t *testing.T, s *Server) int {
	t.Helper()
	st, ok := s.store.(*sqliteStore)
	if !ok {
		t.Fatalf("store = %T（want *sqliteStore）", s.store)
	}
	var n int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM turns`).Scan(&n); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	return n
}

// ---------------------------------------------------------------------------
// 1. TestSessionsAPIAuthGated401
// ---------------------------------------------------------------------------

func TestSessionsAPIAuthGated401(t *testing.T) {
	cfg := uiTestConfig()
	cfg.HeartbeatSeconds = 15
	cfg.Public = &PublicSection{Enabled: true, AccessMode: "gated", AccessKeys: []string{"w65-access-key"}}
	cfg.Store = &StoreSection{Enabled: true, Path: filepath.Join(t.TempDir(), "any2api.db")}
	_, srv := sessionsAPIStart(t, cfg)

	anon := sessionsClient(t)
	endpoints := []struct{ method, path string }{
		{http.MethodGet, sessionsAPIPrefix},
		{http.MethodPost, sessionsAPIPrefix},
		{http.MethodGet, sessionsAPIPrefix + "/x"},
		{http.MethodPut, sessionsAPIPrefix + "/x"},
		{http.MethodPatch, sessionsAPIPrefix + "/x"},
		{http.MethodDelete, sessionsAPIPrefix + "/x"},
		{http.MethodGet, sessionsAPIPrefix + "/x/messages"},
		{http.MethodPost, sessionsAPIPrefix + "/x/messages"},
	}
	for _, e := range endpoints {
		resp := sessionsReq(t, anon, e.method, srv.URL+e.path, "application/json", []byte(`{}`), nil)
		if resp.status != http.StatusUnauthorized {
			t.Fatalf("%s %s: status %d（want 401）body=%s", e.method, e.path, resp.status, resp.body)
		}
		if !strings.Contains(string(resp.body), "authentication_error") {
			t.Fatalf("%s %s: body %s", e.method, e.path, resp.body)
		}
		if sc := sessionsSetCookieValue(resp.header); sc != "" {
			t.Fatalf("%s %s: 401 不得 Set-Cookie: %q", e.method, e.path, sc)
		}
	}

	// /ui/api/config 不受会话门控：200 且零 Set-Cookie。
	resp := sessionsReq(t, anon, http.MethodGet, srv.URL+"/ui/api/config", "", nil, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("/ui/api/config status %d", resp.status)
	}
	if sc := sessionsSetCookieValue(resp.header); sc != "" {
		t.Fatalf("/ui/api/config 不得 Set-Cookie: %q", sc)
	}

	// 带 key → E1 200 + Set-Cookie；随后 cookie 可读。
	keyClient := sessionsClient(t)
	resp = sessionsReq(t, keyClient, http.MethodGet, srv.URL+sessionsAPIPrefix, "", nil,
		map[string]string{"X-Access-Key": "w65-access-key"})
	if resp.status != http.StatusOK {
		t.Fatalf("key GET: status %d body=%s", resp.status, resp.body)
	}
	if sessionsSetCookieValue(resp.header) == "" {
		t.Fatalf("key GET 应 Set-Cookie")
	}
	resp = sessionsReq(t, keyClient, http.MethodGet, srv.URL+sessionsAPIPrefix, "", nil, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("cookie GET: status %d body=%s", resp.status, resp.body)
	}
}

// ---------------------------------------------------------------------------
// 2. TestSessionsAPIStoreDisabled503
// ---------------------------------------------------------------------------

func TestSessionsAPIStoreDisabled503(t *testing.T) {
	endpoints := []struct{ method, path string }{
		{http.MethodGet, sessionsAPIPrefix},
		{http.MethodPost, sessionsAPIPrefix},
		{http.MethodGet, sessionsAPIPrefix + "/x"},
		{http.MethodPut, sessionsAPIPrefix + "/x"},
		{http.MethodPatch, sessionsAPIPrefix + "/x"},
		{http.MethodDelete, sessionsAPIPrefix + "/x"},
		{http.MethodGet, sessionsAPIPrefix + "/x/messages"},
		{http.MethodPost, sessionsAPIPrefix + "/x/messages"},
	}

	// store 未启用（public 开）：8 条全 503 store_unavailable（不是 SPA 200/404）。
	cfg := uiTestConfig()
	cfg.HeartbeatSeconds = 15
	cfg.Public = &PublicSection{Enabled: true}
	_, srv := sessionsAPIStart(t, cfg)
	c := sessionsClient(t)
	for _, e := range endpoints {
		resp := sessionsReq(t, c, e.method, srv.URL+e.path, "application/json", []byte(`{}`), nil)
		if resp.status != http.StatusServiceUnavailable {
			t.Fatalf("store off %s %s: status %d（want 503）body=%s", e.method, e.path, resp.status, resp.body)
		}
		if !strings.Contains(string(resp.body), "store_unavailable") {
			t.Fatalf("store off %s %s: body %s", e.method, e.path, resp.body)
		}
		if ra := resp.header.Get("Retry-After"); ra != "1" {
			t.Fatalf("store off Retry-After = %q（want 1）", ra)
		}
	}
	if resp := sessionsReq(t, c, http.MethodGet, srv.URL+"/v1/models", "", nil, nil); resp.status != http.StatusOK {
		t.Fatalf("/v1/models 应 200（store 关不影响生成面）: %d", resp.status)
	}
	if resp := sessionsReq(t, c, http.MethodGet, srv.URL+"/ui", "", nil, nil); resp.status != http.StatusOK {
		t.Fatalf("/ui 应 200: %d", resp.status)
	}

	// public 未启用（store 开）：8 条全 404 JSON（不是 SPA 200）。
	cfg2 := uiTestConfig()
	cfg2.HeartbeatSeconds = 15
	cfg2.Store = &StoreSection{Enabled: true, Path: filepath.Join(t.TempDir(), "any2api.db")}
	_, srv2 := sessionsAPIStart(t, cfg2)
	c2 := sessionsClient(t)
	for _, e := range endpoints {
		resp := sessionsReq(t, c2, e.method, srv2.URL+e.path, "application/json", []byte(`{}`), nil)
		if resp.status != http.StatusNotFound {
			t.Fatalf("public off %s %s: status %d（want 404）body=%s", e.method, e.path, resp.status, resp.body)
		}
		if !strings.Contains(string(resp.body), "not_found") {
			t.Fatalf("public off %s %s: body %s", e.method, e.path, resp.body)
		}
	}
	if resp := sessionsReq(t, c2, http.MethodGet, srv2.URL+"/v1/models", "", nil, nil); resp.status != http.StatusOK {
		t.Fatalf("public off /v1/models 应 200: %d", resp.status)
	}
	if resp := sessionsReq(t, c2, http.MethodGet, srv2.URL+"/ui", "", nil, nil); resp.status != http.StatusOK {
		t.Fatalf("public off /ui 应 200: %d", resp.status)
	}
}

// ---------------------------------------------------------------------------
// 3. TestSessionsAPICreateListGet
// ---------------------------------------------------------------------------

func TestSessionsAPICreateListGet(t *testing.T) {
	_, srv := sessionsAPIStart(t, sessionsAPIConfig(t))
	c := sessionsClient(t)

	created := sessionsCreateConv(t, c, srv.URL, "conv-create-1", "新对话")
	if created.Rev != 1 || created.Title != "新对话" || created.Model != "claude-sonnet-5-5" {
		t.Fatalf("created = %+v", created)
	}
	if created.CreatedAt <= 0 || created.UpdatedAt != created.CreatedAt || created.Deleted {
		t.Fatalf("created 时间/删除位 = %+v", created)
	}
	if created.CreatedAt < 1_000_000_000_000 {
		t.Fatalf("created_at 应为 unix ms: %d", created.CreatedAt)
	}

	resp := sessionsReq(t, c, http.MethodGet, srv.URL+sessionsAPIPrefix, "", nil, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("list status %d", resp.status)
	}
	var list sessionsListResponse
	sessionsJSON(t, resp.body, &list)
	if len(list.Sessions) != 1 || list.Sessions[0].ID != "conv-create-1" {
		t.Fatalf("list = %+v", list)
	}
	if list.Now <= 0 || list.NextSince != list.Now {
		t.Fatalf("now/next_since = %d/%d", list.Now, list.NextSince)
	}

	resp = sessionsReq(t, c, http.MethodGet, srv.URL+sessionsAPIPrefix+"/conv-create-1", "", nil, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("get status %d body=%s", resp.status, resp.body)
	}
	var got sessionMessagesEnvelope
	sessionsJSON(t, resp.body, &got)
	if got.Session.ID != "conv-create-1" || got.Session.Rev != 1 {
		t.Fatalf("get session = %+v", got.Session)
	}
	if got.Messages == nil || len(got.Messages) != 0 {
		t.Fatalf("get messages = %#v（want 空数组）", got.Messages)
	}
}

// ---------------------------------------------------------------------------
// 4. TestSessionsAPIRevConflict409
// ---------------------------------------------------------------------------

func TestSessionsAPIRevConflict409(t *testing.T) {
	_, srv := sessionsAPIStart(t, sessionsAPIConfig(t))
	c := sessionsClient(t)
	sessionsCreateConv(t, c, srv.URL, "conv-rev", "rev")

	put := func(baseRev int, ids ...string) sessionsResp {
		msgs := make([]map[string]any, 0, len(ids))
		for _, id := range ids {
			msgs = append(msgs, map[string]any{
				"id": id, "role": "user", "content": []any{map[string]any{"type": "text", "text": id}},
			})
		}
		return sessionsJSONReq(t, c, http.MethodPut, srv.URL+sessionsAPIPrefix+"/conv-rev", map[string]any{
			"base_rev": baseRev, "title": "rev", "model": "m", "messages": msgs,
		})
	}
	if resp := put(1, "m1"); resp.status != http.StatusOK {
		t.Fatalf("put rev1 status %d body=%s", resp.status, resp.body)
	}
	if resp := put(2, "m1", "m2"); resp.status != http.StatusOK {
		t.Fatalf("put rev2 status %d body=%s", resp.status, resp.body)
	}

	resp := put(2, "m9")
	if resp.status != http.StatusConflict {
		t.Fatalf("stale put status %d（want 409）body=%s", resp.status, resp.body)
	}
	var conflict conflictEnvelope
	sessionsJSON(t, resp.body, &conflict)
	if conflict.Error.Type != "conflict" {
		t.Fatalf("conflict error = %+v", conflict.Error)
	}
	if conflict.Session.Rev != 3 {
		t.Fatalf("conflict session.rev = %d（want 3）", conflict.Session.Rev)
	}
	if len(conflict.Messages) != 2 {
		t.Fatalf("conflict 应带最新 messages（2 条）: %d", len(conflict.Messages))
	}
	resp = put(3, "m1", "m2", "m3")
	if resp.status != http.StatusOK {
		t.Fatalf("put rev3 status %d body=%s", resp.status, resp.body)
	}
	var okEnv sessionMessagesEnvelope
	sessionsJSON(t, resp.body, &okEnv)
	if okEnv.Session.Rev != 4 {
		t.Fatalf("rev = %d（want 4）", okEnv.Session.Rev)
	}
}

// ---------------------------------------------------------------------------
// 5. TestSessionsAPIPutMessagesReplace
// ---------------------------------------------------------------------------

func TestSessionsAPIPutMessagesReplace(t *testing.T) {
	_, srv := sessionsAPIStart(t, sessionsAPIConfig(t))
	c := sessionsClient(t)
	url := srv.URL + sessionsAPIPrefix + "/conv-put"

	put := func(body map[string]any) sessionsResp {
		return sessionsJSONReq(t, c, http.MethodPut, url, body)
	}
	resp := put(map[string]any{
		"title": "t", "model": "m",
		"messages": []any{
			map[string]any{"id": "a", "role": "user", "content": []any{map[string]any{"type": "text", "text": "A"}}},
			map[string]any{"id": "b", "role": "assistant", "content": []any{map[string]any{"type": "text", "text": "B"}}},
			map[string]any{"id": "c", "role": "user", "content": []any{map[string]any{"type": "text", "text": "C"}}},
		},
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("put create status %d body=%s", resp.status, resp.body)
	}
	var created sessionMessagesEnvelope
	sessionsJSON(t, resp.body, &created)
	if len(created.Messages) != 3 || created.Messages[2].Seq != 3 {
		t.Fatalf("created messages = %+v", created.Messages)
	}

	contentB := `[{"type":"text","text":"B2"}]`
	contentA := `[{"type":"thinking","text":"A2"}]`
	raw := []byte(fmt.Sprintf(`{"base_rev":1,"title":"t2","model":"m2","messages":[
		{"id":"b2","role":"assistant","content":%s},
		{"id":"a2","role":"user","content":%s}]}`, contentB, contentA))
	resp = sessionsReq(t, c, http.MethodPut, url, "application/json", raw, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("put replace status %d body=%s", resp.status, resp.body)
	}
	var replaced sessionMessagesEnvelope
	sessionsJSON(t, resp.body, &replaced)
	if len(replaced.Messages) != 2 {
		t.Fatalf("replace messages = %d（want 2）", len(replaced.Messages))
	}
	if replaced.Messages[0].ID != "b2" || replaced.Messages[0].Seq != 1 ||
		replaced.Messages[1].ID != "a2" || replaced.Messages[1].Seq != 2 {
		t.Fatalf("seq 应按数组序 1..N: %+v", replaced.Messages)
	}
	if string(replaced.Messages[0].Content) != contentB || string(replaced.Messages[1].Content) != contentA {
		t.Fatalf("content 应逐字节往返: %s / %s", replaced.Messages[0].Content, replaced.Messages[1].Content)
	}
	if replaced.Session.Title != "t2" || replaced.Session.Model != "m2" {
		t.Fatalf("meta = %+v", replaced.Session)
	}

	// 旧第 3 条已删：单读只剩 2 条。
	resp = sessionsReq(t, c, http.MethodGet, url, "", nil, nil)
	sessionsJSON(t, resp.body, &replaced)
	if len(replaced.Messages) != 2 {
		t.Fatalf("get after replace = %d（want 2）", len(replaced.Messages))
	}
}

// ---------------------------------------------------------------------------
// 6. TestSessionsAPIPatchRename
// ---------------------------------------------------------------------------

func TestSessionsAPIPatchRename(t *testing.T) {
	s, srv := sessionsAPIStart(t, sessionsAPIConfig(t))
	clock := &fakeClock{t: time.Unix(1700000000, 0).UTC()}
	s.sessions.now = clock.Now
	c := sessionsClient(t)
	created := sessionsCreateConv(t, c, srv.URL, "conv-patch", "旧名")

	clock.Advance(time.Second)
	resp := sessionsJSONReq(t, c, http.MethodPatch, srv.URL+sessionsAPIPrefix+"/conv-patch", map[string]any{
		"base_rev": 1, "title": "新名",
	})
	if resp.status != http.StatusOK {
		t.Fatalf("patch status %d body=%s", resp.status, resp.body)
	}
	var env sessionEnvelope
	sessionsJSON(t, resp.body, &env)
	if env.Session.Rev != 2 || env.Session.Title != "新名" {
		t.Fatalf("patch session = %+v", env.Session)
	}
	if env.Session.UpdatedAt <= created.UpdatedAt {
		t.Fatalf("updated_at 应增大: %d → %d", created.UpdatedAt, env.Session.UpdatedAt)
	}
	if env.Session.Model != "claude-sonnet-5-5" {
		t.Fatalf("model 不应变: %+v", env.Session)
	}

	// 旧 base_rev → 409。
	resp = sessionsJSONReq(t, c, http.MethodPatch, srv.URL+sessionsAPIPrefix+"/conv-patch", map[string]any{
		"base_rev": 1, "title": "再次改名",
	})
	if resp.status != http.StatusConflict {
		t.Fatalf("stale patch status %d（want 409）body=%s", resp.status, resp.body)
	}

	// messages 不变（空）。
	resp = sessionsReq(t, c, http.MethodGet, srv.URL+sessionsAPIPrefix+"/conv-patch", "", nil, nil)
	var got sessionMessagesEnvelope
	sessionsJSON(t, resp.body, &got)
	if len(got.Messages) != 0 || got.Session.Title != "新名" {
		t.Fatalf("patch 后 = %+v / %d msgs", got.Session, len(got.Messages))
	}
}

// ---------------------------------------------------------------------------
// 7. TestSessionsAPIDeleteTombstoneAndIdempotent
// ---------------------------------------------------------------------------

func TestSessionsAPIDeleteTombstoneAndIdempotent(t *testing.T) {
	_, srv := sessionsAPIStart(t, sessionsAPIConfig(t))
	c := sessionsClient(t)
	url := srv.URL + sessionsAPIPrefix + "/conv-del"
	resp := sessionsJSONReq(t, c, http.MethodPut, url, map[string]any{
		"title": "待删", "model": "m",
		"messages": []any{map[string]any{"id": "dm1", "role": "user", "content": []any{map[string]any{"type": "text", "text": "x"}}}},
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("put create status %d body=%s", resp.status, resp.body)
	}

	resp = sessionsReq(t, c, http.MethodDelete, url, "", nil, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("delete status %d body=%s", resp.status, resp.body)
	}
	var del deleteEnvelope
	sessionsJSON(t, resp.body, &del)
	if !del.OK || del.DeletedAt <= 0 || del.Rev != 2 {
		t.Fatalf("delete = %+v", del)
	}

	if resp = sessionsReq(t, c, http.MethodGet, url, "", nil, nil); resp.status != http.StatusNotFound {
		t.Fatalf("deleted GET status %d（want 404）", resp.status)
	}
	resp = sessionsReq(t, c, http.MethodGet, url+"?include_deleted=1", "", nil, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("include_deleted status %d body=%s", resp.status, resp.body)
	}
	var got sessionMessagesEnvelope
	sessionsJSON(t, resp.body, &got)
	if !got.Session.Deleted || len(got.Messages) != 1 {
		t.Fatalf("include_deleted = %+v / %d msgs", got.Session, len(got.Messages))
	}

	// 重复删除：同 rev / 同 deleted_at（幂等）。
	resp = sessionsReq(t, c, http.MethodDelete, url, "", nil, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("delete again status %d body=%s", resp.status, resp.body)
	}
	var del2 deleteEnvelope
	sessionsJSON(t, resp.body, &del2)
	if del2.Rev != del.Rev || del2.DeletedAt != del.DeletedAt {
		t.Fatalf("重复删除 = %+v（want rev=%d deleted_at=%d）", del2, del.Rev, del.DeletedAt)
	}

	// since 列表含墓碑（updated_at = 删除时刻）。
	resp = sessionsReq(t, c, http.MethodGet, fmt.Sprintf("%s%s?since=%d", srv.URL, sessionsAPIPrefix, del.DeletedAt), "", nil, nil)
	var list sessionsListResponse
	sessionsJSON(t, resp.body, &list)
	if len(list.Sessions) != 1 || !list.Sessions[0].Deleted {
		t.Fatalf("since 列表墓碑 = %+v", list.Sessions)
	}
}

// ---------------------------------------------------------------------------
// 8. TestSessionsAPIIdempotentCreateAndAppend
// ---------------------------------------------------------------------------

func TestSessionsAPIIdempotentCreateAndAppend(t *testing.T) {
	_, srv := sessionsAPIStart(t, sessionsAPIConfig(t))
	c := sessionsClient(t)
	base := srv.URL + sessionsAPIPrefix

	first := sessionsCreateConv(t, c, srv.URL, "conv-idem", "幂等")
	resp := sessionsJSONReq(t, c, http.MethodPost, base, map[string]any{"id": "conv-idem", "title": "第二次"})
	if resp.status != http.StatusOK {
		t.Fatalf("idempotent create status %d body=%s", resp.status, resp.body)
	}
	var env sessionEnvelope
	sessionsJSON(t, resp.body, &env)
	if env.Session.Rev != first.Rev || env.Session.Title != "幂等" {
		t.Fatalf("幂等创建不应 bump/覆盖: %+v", env.Session)
	}

	msgBody := map[string]any{
		"id": "msg-idem-1", "base_rev": 1, "role": "user",
		"content": []any{map[string]any{"type": "text", "text": "hi"}},
	}
	resp = sessionsJSONReq(t, c, http.MethodPost, base+"/conv-idem/messages", msgBody)
	if resp.status != http.StatusCreated {
		t.Fatalf("append status %d body=%s", resp.status, resp.body)
	}
	var appended messageEnvelope
	sessionsJSON(t, resp.body, &appended)
	if appended.Rev != 2 || appended.Message.Seq != 1 {
		t.Fatalf("append = %+v", appended)
	}

	// 同 id 第二次（base_rev 已过期也应 200 幂等）。
	msgBody["base_rev"] = 2
	resp = sessionsJSONReq(t, c, http.MethodPost, base+"/conv-idem/messages", msgBody)
	if resp.status != http.StatusOK {
		t.Fatalf("idempotent append status %d body=%s", resp.status, resp.body)
	}
	var again messageEnvelope
	sessionsJSON(t, resp.body, &again)
	if again.Rev != 2 || again.Message.ID != "msg-idem-1" {
		t.Fatalf("idempotent append = %+v", again)
	}
	resp = sessionsReq(t, c, http.MethodGet, base+"/conv-idem/messages", "", nil, nil)
	var msgs messagesEnvelope
	sessionsJSON(t, resp.body, &msgs)
	if len(msgs.Messages) != 1 || msgs.MaxSeq != 1 {
		t.Fatalf("重复 append 不应重复插入: %+v", msgs)
	}

	// 第二条消息：seq = 2（UNIQUE(conv_id,seq) 不报错）。
	resp = sessionsJSONReq(t, c, http.MethodPost, base+"/conv-idem/messages", map[string]any{
		"id": "msg-idem-2", "base_rev": 2, "role": "assistant",
		"content": []any{map[string]any{"type": "text", "text": "yo"}},
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("second append status %d body=%s", resp.status, resp.body)
	}
	sessionsJSON(t, resp.body, &appended)
	if appended.Message.Seq != 2 || appended.Rev != 3 {
		t.Fatalf("second append = %+v", appended)
	}
}

// ---------------------------------------------------------------------------
// 9. TestSessionsAPIValidationLimits413
// ---------------------------------------------------------------------------

func TestSessionsAPIValidationLimits413(t *testing.T) {
	_, srv := sessionsAPIStart(t, sessionsAPIConfig(t))
	c := sessionsClient(t)
	sessionsCreateConv(t, c, srv.URL, "conv-limits", "limits")
	url := srv.URL + sessionsAPIPrefix + "/conv-limits"

	putRaw := func(raw string) sessionsResp {
		return sessionsReq(t, c, http.MethodPut, url, "application/json", []byte(raw), nil)
	}

	// >2MB body → 413。
	big := strings.Repeat("a", sessionsAPIMaxBody+1024)
	if resp := putRaw(fmt.Sprintf(`{"base_rev":1,"messages":[{"id":"big","role":"user","content":[{"type":"text","text":%q}]}]}`, big)); resp.status != http.StatusRequestEntityTooLarge {
		t.Fatalf("big body status %d（want 413）body=%.200s", resp.status, resp.body)
	}

	// 501 messages → 413。
	var b strings.Builder
	b.WriteString(`{"base_rev":1,"messages":[`)
	for i := range sessionsAPIMaxMessages + 1 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id":"m%d","role":"user","content":[{"type":"text","text":"x"}]}`, i)
	}
	b.WriteString(`]}`)
	if resp := putRaw(b.String()); resp.status != http.StatusRequestEntityTooLarge {
		t.Fatalf("501 messages status %d（want 413）body=%.200s", resp.status, resp.body)
	}

	// 单条 content >256KiB → 413。
	huge := strings.Repeat("b", sessionsAPIMaxContentBytes+1024)
	if resp := putRaw(fmt.Sprintf(`{"base_rev":1,"messages":[{"id":"huge","role":"user","content":[{"type":"text","text":%q}]}]}`, huge)); resp.status != http.StatusRequestEntityTooLarge {
		t.Fatalf("huge content status %d（want 413）body=%.200s", resp.status, resp.body)
	}

	// 65 blocks → 413。
	var blocks []string
	for range sessionsAPIMaxContentBlocks + 1 {
		blocks = append(blocks, `{"type":"text","text":"x"}`)
	}
	rawBlocks := `[` + strings.Join(blocks, ",") + `]`
	if resp := putRaw(fmt.Sprintf(`{"base_rev":1,"messages":[{"id":"blocks","role":"user","content":%s}]}`, rawBlocks)); resp.status != http.StatusRequestEntityTooLarge {
		t.Fatalf("65 blocks status %d（want 413）body=%.200s", resp.status, resp.body)
	}

	// 未知字段 → 400。
	if resp := putRaw(`{"base_rev":1,"bogus":true,"messages":[]}`); resp.status != http.StatusBadRequest {
		t.Fatalf("unknown field status %d（want 400）body=%.200s", resp.status, resp.body)
	}

	// 恰为上限（500 条）放行 → 200。
	var ok strings.Builder
	ok.WriteString(`{"base_rev":1,"messages":[`)
	for i := range sessionsAPIMaxMessages {
		if i > 0 {
			ok.WriteByte(',')
		}
		fmt.Fprintf(&ok, `{"id":"ok%d","role":"user","content":[{"type":"text","text":"x"}]}`, i)
	}
	ok.WriteString(`]}`)
	if resp := putRaw(ok.String()); resp.status != http.StatusOK {
		t.Fatalf("500 messages status %d（want 200）body=%.300s", resp.status, resp.body)
	}
}

// ---------------------------------------------------------------------------
// 10. TestSessionsAPIIsolation404
// ---------------------------------------------------------------------------

func TestSessionsAPIIsolation404(t *testing.T) {
	_, srv := sessionsAPIStart(t, sessionsAPIConfig(t))
	a := sessionsClient(t)
	b := sessionsClient(t)
	sessionsCreateConv(t, a, srv.URL, "conv-a", "A")
	sessionsCreateConv(t, b, srv.URL, "conv-b", "B")

	// A 的列表不含 B。
	resp := sessionsReq(t, a, http.MethodGet, srv.URL+sessionsAPIPrefix, "", nil, nil)
	var list sessionsListResponse
	sessionsJSON(t, resp.body, &list)
	if len(list.Sessions) != 1 || list.Sessions[0].ID != "conv-a" {
		t.Fatalf("A 列表 = %+v", list.Sessions)
	}

	url := srv.URL + sessionsAPIPrefix + "/conv-b"
	cases := []struct {
		method string
		body   []byte
	}{
		{http.MethodGet, nil},
		{http.MethodPut, []byte(`{"base_rev":1,"messages":[]}`)},
		{http.MethodPatch, []byte(`{"base_rev":1,"title":"x"}`)},
		{http.MethodDelete, nil},
	}
	for _, tc := range cases {
		resp := sessionsReq(t, a, tc.method, url, "application/json", tc.body, nil)
		if resp.status != http.StatusNotFound {
			t.Fatalf("A %s B 的 conv: status %d（want 404）body=%s", tc.method, resp.status, resp.body)
		}
	}
	// B 反向。
	if resp := sessionsReq(t, b, http.MethodGet, srv.URL+sessionsAPIPrefix+"/conv-a", "", nil, nil); resp.status != http.StatusNotFound {
		t.Fatalf("B GET A 的 conv: %d", resp.status)
	}
}

// ---------------------------------------------------------------------------
// 11. TestSessionsAPIRateLimit429
// ---------------------------------------------------------------------------

func TestSessionsAPIRateLimit429(t *testing.T) {
	s, srv := sessionsAPIStart(t, sessionsAPIConfig(t))
	clock := &fakeClock{t: time.Unix(1700000000, 0).UTC()}
	if s.sessionsWriteLimiter == nil {
		t.Fatalf("sessionsWriteLimiter 未装配")
	}
	s.sessionsWriteLimiter.now = clock.Now
	c := sessionsClient(t)

	// GET 不计数（会话首请求也不进写桶）。
	if resp := sessionsReq(t, c, http.MethodGet, srv.URL+sessionsAPIPrefix, "", nil, nil); resp.status != http.StatusOK {
		t.Fatalf("GET status %d", resp.status)
	}
	for i := range sessionsWritePerMin {
		resp := sessionsJSONReq(t, c, http.MethodPost, srv.URL+sessionsAPIPrefix, map[string]any{
			"id": fmt.Sprintf("rl-%02d", i), "title": "x",
		})
		if resp.status != http.StatusCreated {
			t.Fatalf("write %d status %d body=%s", i+1, resp.status, resp.body)
		}
	}
	resp := sessionsJSONReq(t, c, http.MethodPost, srv.URL+sessionsAPIPrefix, map[string]any{"id": "rl-over"})
	if resp.status != http.StatusTooManyRequests {
		t.Fatalf("第 61 次写 status %d（want 429）body=%s", resp.status, resp.body)
	}
	if !strings.Contains(string(resp.body), "rate_limited") {
		t.Fatalf("429 body = %s", resp.body)
	}
	ra, err := strconv.Atoi(resp.header.Get("Retry-After"))
	if err != nil || ra < 1 {
		t.Fatalf("Retry-After = %q（want 正整数）", resp.header.Get("Retry-After"))
	}
	// GET 仍放行（不计数）。
	if resp := sessionsReq(t, c, http.MethodGet, srv.URL+sessionsAPIPrefix, "", nil, nil); resp.status != http.StatusOK {
		t.Fatalf("限流后 GET status %d", resp.status)
	}
	// 60s 后回填放行。
	clock.Advance(61 * time.Second)
	resp = sessionsJSONReq(t, c, http.MethodPost, srv.URL+sessionsAPIPrefix, map[string]any{"id": "rl-after"})
	if resp.status != http.StatusCreated {
		t.Fatalf("61s 后 status %d body=%s", resp.status, resp.body)
	}
}

// ---------------------------------------------------------------------------
// 12. TestSessionsAPIAuditNoContent
// ---------------------------------------------------------------------------

func TestSessionsAPIAuditNoContent(t *testing.T) {
	s, srv := sessionsAPIStart(t, sessionsAPIConfig(t))
	c := sessionsClient(t)
	base := srv.URL + sessionsAPIPrefix

	sessionsCreateConv(t, c, srv.URL, "conv-audit", "秘密标题")
	// E4 update（含 content）。
	resp := sessionsJSONReq(t, c, http.MethodPut, base+"/conv-audit", map[string]any{
		"base_rev": 1, "title": "秘密标题2", "model": "secret-model",
		"messages": []any{map[string]any{"id": "am1", "role": "user",
			"content": []any{map[string]any{"type": "text", "text": "秘密内容"}}}},
	})
	if resp.status != http.StatusOK {
		t.Fatalf("put status %d body=%s", resp.status, resp.body)
	}
	// E5 patch。
	if resp := sessionsJSONReq(t, c, http.MethodPatch, base+"/conv-audit", map[string]any{"base_rev": 2, "title": "秘密标题3"}); resp.status != http.StatusOK {
		t.Fatalf("patch status %d body=%s", resp.status, resp.body)
	}
	// E8 append。
	if resp := sessionsJSONReq(t, c, http.MethodPost, base+"/conv-audit/messages", map[string]any{
		"id": "am2", "base_rev": 3, "role": "assistant",
		"content": []any{map[string]any{"type": "text", "text": "秘密内容2"}},
	}); resp.status != http.StatusCreated {
		t.Fatalf("append status %d body=%s", resp.status, resp.body)
	}
	// E6 delete。
	if resp := sessionsReq(t, c, http.MethodDelete, base+"/conv-audit", "", nil, nil); resp.status != http.StatusOK {
		t.Fatalf("delete status %d body=%s", resp.status, resp.body)
	}

	rows, err := s.store.AuditQuery("", time.Time{}, 200)
	if err != nil {
		t.Fatalf("AuditQuery: %v", err)
	}
	counts := map[string]int{}
	for _, row := range rows {
		switch row.Kind {
		case adminLogConversationCreate, adminLogConversationUpdate, adminLogConversationDelete, adminLogMessageAppend:
			counts[row.Kind]++
			if len(row.SID) != 8 {
				t.Fatalf("audit sid 应为 8 字符前缀: %q", row.SID)
			}
			for _, banned := range []string{"秘密标题", "secret-model", "秘密内容", "w65-access-key"} {
				if strings.Contains(row.Detail, banned) {
					t.Fatalf("audit detail 含敏感内容 %q: %s", banned, row.Detail)
				}
			}
			var detail map[string]any
			if err := json.Unmarshal([]byte(row.Detail), &detail); err != nil {
				t.Fatalf("detail JSON: %v (%s)", err, row.Detail)
			}
			for _, key := range []string{"op", "conv", "rev", "messages"} {
				if _, ok := detail[key]; !ok {
					t.Fatalf("detail 缺 %q: %s", key, row.Detail)
				}
			}
		}
	}
	want := map[string]int{
		adminLogConversationCreate: 1,
		adminLogConversationUpdate: 2, // E4 + E5
		adminLogConversationDelete: 1,
		adminLogMessageAppend:      1,
	}
	for kind, n := range want {
		if counts[kind] != n {
			t.Fatalf("audit %s = %d（want %d）；counts=%v", kind, counts[kind], n, counts)
		}
	}
}

// ---------------------------------------------------------------------------
// 13. TestTurnHookJoinAttempts
// ---------------------------------------------------------------------------

func TestTurnHookJoinAttempts(t *testing.T) {
	logs := captureServiceLogs(t)
	bad := failUpstream(t, http.StatusInternalServerError, nil, errBodyAPIError)
	good := goodClaudeUpstream(t)
	cfg := failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL))
	cfg.Public = &PublicSection{Enabled: true}
	cfg.Store = &StoreSection{Enabled: true, Path: filepath.Join(t.TempDir(), "any2api.db")}
	s, srv := sessionsAPIStart(t, cfg)
	c := sessionsClient(t)
	sessionsCreateConv(t, c, srv.URL, "conv-turn", "turn")

	resp := sessionsReq(t, c, http.MethodPost, srv.URL+"/v1/chat/completions", "application/json",
		[]byte(w58LightBody), map[string]string{"X-A2A-Conversation-Id": "conv-turn"})
	if resp.status != http.StatusOK || !strings.Contains(string(resp.body), "Hello ") {
		t.Fatalf("chat status %d body=%.300s", resp.status, resp.body)
	}
	reqID := resp.header.Get("request-id")
	if reqID == "" {
		t.Fatalf("缺 request-id 响应头")
	}
	turns, err := s.store.TurnsByRequest(reqID)
	if err != nil {
		t.Fatalf("TurnsByRequest: %v", err)
	}
	if len(turns) != 2 {
		t.Fatalf("turns = %d（want 2：每 attempt 一行）", len(turns))
	}
	if turns[0].AttemptNo != 1 || turns[0].Status != "failed" || turns[0].Upstream != "bad" {
		t.Fatalf("turn[0] = %+v", turns[0])
	}
	if turns[1].AttemptNo != 2 || turns[1].Status != "completed" || turns[1].Upstream != "good" {
		t.Fatalf("turn[1] = %+v", turns[1])
	}
	for i, turn := range turns {
		if turn.RequestID != reqID || turn.ConvID != "conv-turn" {
			t.Fatalf("turn[%d] join 字段 = %+v", i, turn)
		}
		if turn.CredentialIndex != -1 {
			t.Fatalf("turn[%d] credential_index = %d（want -1/无池）", i, turn.CredentialIndex)
		}
		var usage map[string]any
		if err := json.Unmarshal([]byte(turn.UsageJSON), &usage); err != nil {
			t.Fatalf("turn[%d] usage_json: %v", i, err)
		}
		for _, key := range []string{"in", "out", "ttft_ms", "total_ms"} {
			if _, ok := usage[key]; !ok {
				t.Fatalf("turn[%d] usage_json 缺 %q: %s", i, key, turn.UsageJSON)
			}
		}
	}
	// attempts 日志同 req id（join 面）。
	waitForLogContains(t, logs, "request "+reqID+" attempts:")
}

// ---------------------------------------------------------------------------
// 14. TestTurnHookForeignConvIgnored
// ---------------------------------------------------------------------------

func TestTurnHookForeignConvIgnored(t *testing.T) {
	good := goodClaudeUpstream(t)
	cfg := failoverCfg(anthUp("good", good.srv.URL))
	cfg.Public = &PublicSection{Enabled: true}
	cfg.Store = &StoreSection{Enabled: true, Path: filepath.Join(t.TempDir(), "any2api.db")}
	s, srv := sessionsAPIStart(t, cfg)
	a := sessionsClient(t)
	b := sessionsClient(t)
	sessionsCreateConv(t, b, srv.URL, "conv-foreign", "B")

	resp := sessionsReq(t, a, http.MethodPost, srv.URL+"/v1/chat/completions", "application/json",
		[]byte(w58LightBody), map[string]string{"X-A2A-Conversation-Id": "conv-foreign"})
	if resp.status != http.StatusOK {
		t.Fatalf("chat status %d body=%.300s", resp.status, resp.body)
	}
	if n := sessionsCountTurns(t, s); n != 0 {
		t.Fatalf("跨会话 conv 不应落 turn: %d 行", n)
	}
	rows, err := s.store.AuditQuery(storeAuditKindRequest, time.Time{}, 10)
	if err != nil || len(rows) == 0 {
		t.Fatalf("audit request 行 = %d err=%v", len(rows), err)
	}
}

// ---------------------------------------------------------------------------
// 15. TestSessionsAPIRestartRecovery
// ---------------------------------------------------------------------------

func TestSessionsAPIRestartRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "any2api.db")
	cfg := uiTestConfig()
	cfg.HeartbeatSeconds = 15
	cfg.Public = &PublicSection{Enabled: true}
	cfg.Store = &StoreSection{Enabled: true, Path: path}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	s1, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv1 := httptest.NewServer(s1.Handler())
	c := sessionsClient(t)
	created := sessionsCreateConv(t, c, srv1.URL, "conv-restart", "重启")
	resp := sessionsJSONReq(t, c, http.MethodPut, srv1.URL+sessionsAPIPrefix+"/conv-restart", map[string]any{
		"base_rev": 1, "title": "重启2", "model": "m",
		"messages": []any{map[string]any{"id": "rm1", "role": "user",
			"content": []any{map[string]any{"type": "text", "text": "持久化"}}}},
	})
	if resp.status != http.StatusOK {
		t.Fatalf("put status %d body=%s", resp.status, resp.body)
	}
	srv1.Close()
	if err := s1.CloseStore(); err != nil {
		t.Fatalf("CloseStore: %v", err)
	}

	s2, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.CloseStore() }()
	srv2 := httptest.NewServer(s2.Handler())
	defer srv2.Close()

	resp = sessionsReq(t, c, http.MethodGet, srv2.URL+sessionsAPIPrefix+"/conv-restart", "", nil, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("restart GET status %d body=%s", resp.status, resp.body)
	}
	if sc := sessionsSetCookieValue(resp.header); sc != "" {
		t.Fatalf("重启后 cookie 仍有效，不应重发 Set-Cookie: %q", sc)
	}
	var got sessionMessagesEnvelope
	sessionsJSON(t, resp.body, &got)
	if got.Session.Rev != 2 || got.Session.Title != "重启2" || len(got.Messages) != 1 || got.Messages[0].ID != "rm1" {
		t.Fatalf("restart 恢复 = %+v / %+v", got.Session, got.Messages)
	}
	resp = sessionsReq(t, c, http.MethodGet, srv2.URL+sessionsAPIPrefix, "", nil, nil)
	var list sessionsListResponse
	sessionsJSON(t, resp.body, &list)
	if len(list.Sessions) != 1 || list.Sessions[0].ID != "conv-restart" {
		t.Fatalf("restart 列表 = %+v", list.Sessions)
	}
	_ = created
}

// ---------------------------------------------------------------------------
// 16. TestSessionsAPIConcurrentPutOneWins
// ---------------------------------------------------------------------------

func TestSessionsAPIConcurrentPutOneWins(t *testing.T) {
	_, srv := sessionsAPIStart(t, sessionsAPIConfig(t))
	c := sessionsClient(t)
	sessionsCreateConv(t, c, srv.URL, "conv-race", "race")
	url := srv.URL + sessionsAPIPrefix + "/conv-race"

	const n = 10
	var wg sync.WaitGroup
	statuses := make([]int, n)
	bodies := make([][]byte, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			raw, _ := json.Marshal(map[string]any{
				"base_rev": 1, "title": fmt.Sprintf("t%d", i), "model": "m",
				"messages": []any{map[string]any{"id": fmt.Sprintf("race-%d", i), "role": "user",
					"content": []any{map[string]any{"type": "text", "text": "x"}}}},
			})
			resp := sessionsReq(t, c, http.MethodPut, url, "application/json", raw, nil)
			statuses[i], bodies[i] = resp.status, resp.body
		}(i)
	}
	wg.Wait()
	okCount, conflictCount := 0, 0
	for i := range n {
		switch statuses[i] {
		case http.StatusOK:
			okCount++
		case http.StatusConflict:
			conflictCount++
		default:
			t.Fatalf("并发 PUT %d status %d body=%.300s", i, statuses[i], bodies[i])
		}
		if strings.Contains(string(bodies[i]), "database is locked") {
			t.Fatalf("出现 database is locked: %.300s", bodies[i])
		}
	}
	if okCount != 1 || conflictCount != n-1 {
		t.Fatalf("并发 PUT 结果 ok=%d conflict=%d（want 1/%d）", okCount, conflictCount, n-1)
	}
	resp := sessionsReq(t, c, http.MethodGet, url, "", nil, nil)
	var got sessionMessagesEnvelope
	sessionsJSON(t, resp.body, &got)
	if got.Session.Rev != 2 {
		t.Fatalf("最终 rev = %d（want 2）", got.Session.Rev)
	}
}

// ---------------------------------------------------------------------------
// 17. TestTurnHookUsageScopes
// ---------------------------------------------------------------------------

func TestTurnHookUsageScopes(t *testing.T) {
	good := goodClaudeUpstream(t)
	cfg := failoverCfg(anthUp("good", good.srv.URL))
	cfg.Public = &PublicSection{Enabled: true, AccessMode: "gated", AccessKeys: []string{"w65-usage-key"}}
	cfg.Store = &StoreSection{Enabled: true, Path: filepath.Join(t.TempDir(), "any2api.db")}
	s, srv := sessionsAPIStart(t, cfg)
	c := sessionsClient(t)
	resp := sessionsReq(t, c, http.MethodGet, srv.URL+sessionsAPIPrefix, "", nil,
		map[string]string{"X-Access-Key": "w65-usage-key"})
	if resp.status != http.StatusOK {
		t.Fatalf("gated GET status %d body=%s", resp.status, resp.body)
	}
	sid := sessionsSetCookieValue(resp.header)
	if sid == "" {
		t.Fatalf("缺会话 cookie")
	}
	sessionsCreateConv(t, c, srv.URL, "conv-usage", "usage")
	resp = sessionsReq(t, c, http.MethodPost, srv.URL+"/v1/chat/completions", "application/json",
		[]byte(w58LightBody), map[string]string{"X-A2A-Conversation-Id": "conv-usage"})
	if resp.status != http.StatusOK {
		t.Fatalf("chat status %d body=%.300s", resp.status, resp.body)
	}
	day := storeDay(time.Now())
	sessionUsage, ok, err := s.store.UsageGet("session:"+sid, day)
	if err != nil || !ok || sessionUsage.Requests != 1 || sessionUsage.CostUnits != 1 {
		t.Fatalf("session 台账 = %+v ok=%v err=%v", sessionUsage, ok, err)
	}
	keyUsage, ok, err := s.store.UsageGet("key:"+testSha8("w65-usage-key"), day)
	if err != nil || !ok || keyUsage.Requests != 1 || keyUsage.CostUnits != 1 {
		t.Fatalf("key 台账 = %+v ok=%v err=%v", keyUsage, ok, err)
	}
	global, ok, err := s.store.UsageGet(storeUsageScopeGlobal, day)
	if err != nil || !ok || global.Requests != 1 || global.CostUnits != 1 {
		t.Fatalf("global 台账 = %+v ok=%v err=%v（不应双计）", global, ok, err)
	}
}

// ---------------------------------------------------------------------------
// 19. 方法错 / 未知子路径 / Origin 双保险（§2.7-3、§2.3.1-9）
// ---------------------------------------------------------------------------

func TestSessionsAPIMethodNotAllowedAndOrigin(t *testing.T) {
	_, srv := sessionsAPIStart(t, sessionsAPIConfig(t))
	c := sessionsClient(t)

	// 集合面方法错 → 405 + Allow。
	resp := sessionsReq(t, c, http.MethodPut, srv.URL+sessionsAPIPrefix, "application/json", []byte(`{}`), nil)
	if resp.status != http.StatusMethodNotAllowed {
		t.Fatalf("PUT collection status %d（want 405）body=%s", resp.status, resp.body)
	}
	if allow := resp.header.Get("Allow"); allow != "GET, POST" {
		t.Fatalf("Allow = %q（want GET, POST）", allow)
	}
	// 单条面方法错 → 405 + Allow。
	resp = sessionsReq(t, c, http.MethodPost, srv.URL+sessionsAPIPrefix+"/x", "application/json", []byte(`{}`), nil)
	if resp.status != http.StatusMethodNotAllowed {
		t.Fatalf("POST item status %d（want 405）body=%s", resp.status, resp.body)
	}
	if allow := resp.header.Get("Allow"); allow != "GET, PUT, PATCH, DELETE" {
		t.Fatalf("Allow = %q", allow)
	}
	// 未知子路径 → 404 JSON（不是 SPA 200）。
	resp = sessionsReq(t, c, http.MethodGet, srv.URL+sessionsAPIPrefix+"/x/y/z", "", nil, nil)
	if resp.status != http.StatusNotFound || !strings.Contains(string(resp.body), "not_found") {
		t.Fatalf("unknown subpath status %d body=%s", resp.status, resp.body)
	}
	// 跨源 Origin → 403；同源 Origin → 放行。
	resp = sessionsJSONReq(t, c, http.MethodPost, srv.URL+sessionsAPIPrefix, map[string]any{"id": "csrf-1"})
	if resp.status != http.StatusCreated {
		t.Fatalf("no-origin create status %d body=%s", resp.status, resp.body)
	}
	reqBody := []byte(`{"id":"csrf-2"}`)
	req, err := http.NewRequest(http.MethodPost, srv.URL+sessionsAPIPrefix, bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://evil.example")
	evilResp, err := c.Do(req)
	if err != nil {
		t.Fatalf("evil origin: %v", err)
	}
	evilBody, _ := io.ReadAll(evilResp.Body)
	evilResp.Body.Close()
	if evilResp.StatusCode != http.StatusForbidden {
		t.Fatalf("跨源 status %d（want 403）body=%s", evilResp.StatusCode, evilBody)
	}
	req, err = http.NewRequest(http.MethodPost, srv.URL+sessionsAPIPrefix, bytes.NewReader([]byte(`{"id":"csrf-3"}`)))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", srv.URL)
	sameResp, err := c.Do(req)
	if err != nil {
		t.Fatalf("same origin: %v", err)
	}
	sameBody, _ := io.ReadAll(sameResp.Body)
	sameResp.Body.Close()
	if sameResp.StatusCode != http.StatusCreated {
		t.Fatalf("同源 status %d（want 201）body=%s", sameResp.StatusCode, sameBody)
	}
}

// ---------------------------------------------------------------------------
// 18. Store 契约（§2.4.3 两档同断言）
// ---------------------------------------------------------------------------

func TestStoreConversationSaveContract(t *testing.T) {
	t.Run("memory", func(t *testing.T) { storeConversationSaveContract(t, newMemoryStore()) })
	t.Run("sqlite", func(t *testing.T) { storeConversationSaveContract(t, w59OpenSQLite(t, w59TempStorePath(t))) })
}

func storeConversationSaveContract(t *testing.T, st Store) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := st.SessionCreate(storeSession{
		SID: "sid-cs", CreatedAt: now, LastSeenAt: now,
		ExpiresAt: now.Add(time.Hour), AbsExpiresAt: now.Add(24 * time.Hour),
	}); err != nil {
		t.Fatalf("SessionCreate: %v", err)
	}
	c := storeConversation{ID: "conv-cs", SID: "sid-cs", Title: "t1", Model: "m1", Rev: 1, CreatedAt: now, UpdatedAt: now}
	ok, err := st.ConversationSave(c, []storeMessage{
		{ID: "cs-1", ConvID: c.ID, Seq: 1, Role: "user", Content: `[{"type":"text","text":"a"}]`, Status: "done", CreatedAt: now},
	}, 0, true)
	if err != nil || !ok {
		t.Fatalf("create = %v/%v", ok, err)
	}
	if ok, _ := st.ConversationSave(c, nil, 0, true); ok {
		t.Fatalf("重复 create 应 false")
	}

	// CAS 成功 + 消息替换。
	c.Title, c.UpdatedAt = "t2", now.Add(time.Millisecond)
	ok, err = st.ConversationSave(c, []storeMessage{
		{ID: "cs-2", ConvID: c.ID, Seq: 1, Role: "assistant", Content: `[]`, Status: "done", CreatedAt: now},
		{ID: "cs-3", ConvID: c.ID, Seq: 2, Role: "user", Content: `[]`, Status: "done", CreatedAt: now},
	}, 1, false)
	if err != nil || !ok {
		t.Fatalf("update = %v/%v", ok, err)
	}
	got, found, _ := st.ConversationGet(c.ID)
	if !found || got.Rev != 2 || got.Title != "t2" {
		t.Fatalf("after update = %+v found=%v", got, found)
	}
	msgs, _ := st.MessagesByConversation(c.ID)
	if len(msgs) != 2 || msgs[0].ID != "cs-2" || msgs[1].ID != "cs-3" {
		t.Fatalf("messages replace = %+v", msgs)
	}
	// 旧 rev → CAS 失败。
	if ok, _ := st.ConversationSave(c, nil, 1, false); ok {
		t.Fatalf("旧 rev CAS 应 false")
	}

	// MessageGet 命中/未命中。
	if m, found, err := st.MessageGet("cs-2"); err != nil || !found || m.ConvID != c.ID {
		t.Fatalf("MessageGet = %+v found=%v err=%v", m, found, err)
	}
	if _, found, _ := st.MessageGet("nope"); found {
		t.Fatalf("MessageGet(未命中) found=true")
	}
	// MessageAppendRev：CAS 失败 → false；成功 → rev+1。
	if ok, _ := st.MessageAppendRev(storeMessage{ID: "cs-4", ConvID: c.ID, Seq: 3, Role: "user", Content: `[]`, CreatedAt: now}, 1); ok {
		t.Fatalf("append 旧 rev 应 false")
	}
	if ok, err := st.MessageAppendRev(storeMessage{ID: "cs-4", ConvID: c.ID, Seq: 3, Role: "user", Content: `[]`, CreatedAt: now}, 2); err != nil || !ok {
		t.Fatalf("append = %v/%v", ok, err)
	}
	got, _, _ = st.ConversationGet(c.ID)
	if got.Rev != 3 {
		t.Fatalf("append 后 rev = %d（want 3）", got.Rev)
	}
	if ok, err := st.MessageAppendRev(storeMessage{ID: "cs-4", ConvID: c.ID, Seq: 4, Role: "user", Content: `[]`, CreatedAt: now}, 3); ok || !errors.Is(err, errStoreMessageIDConflict) {
		t.Fatalf("重复消息 id = %v/%v（want errStoreMessageIDConflict）", ok, err)
	}

	// 软删 bump updated_at + rev；重复删除幂等。
	at := now.Add(2 * time.Second)
	if err := st.ConversationSoftDelete(c.ID, at); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	got, _, _ = st.ConversationGet(c.ID)
	if got.DeletedAt.IsZero() || got.Rev != 4 || !got.UpdatedAt.Equal(at) {
		t.Fatalf("soft delete = %+v（want rev=4 updated_at=%v）", got, at)
	}
	if err := st.ConversationSoftDelete(c.ID, at.Add(time.Second)); err != nil {
		t.Fatalf("soft delete again: %v", err)
	}
	got2, _, _ := st.ConversationGet(c.ID)
	if got2.Rev != 4 || !got2.UpdatedAt.Equal(at) {
		t.Fatalf("重复软删应幂等: %+v", got2)
	}
	// 已软删 → ConversationSave 更新失败。
	if ok, _ := st.ConversationSave(c, nil, 4, false); ok {
		t.Fatalf("软删后 save 应 false")
	}
}
