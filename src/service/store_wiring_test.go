package service

// store_wiring_test.go —— P1-2 持久化接线测试（board A2A-L19-W59-1）：**重启矩阵**
// 关键行（设计 §5.3）——sessions 恢复 / usage_counters 恢复（跨重启预算不重置）/
// audit_log 恢复（含 attempts 面 join）/ 对照行（ring 清零、限流桶清零、池冷却
// 清零）/ 启动动作（打开 → GC → 摘要行）/ **持久化失败绝不影响请求路径**（设计
// §2.4-E）。
//
// 反证锚点（回退即红——原样输出见 tmp/lanes/W59-a/proofs/）：
//   - sessionStore.get 去掉 store 回查 → TestStoreRestartMatrixSessions 红
//     （重启后旧 cookie 被当成无效会话、重发 Set-Cookie）；
//   - budgetLedger.attachStore 去掉启动载入 → TestStoreRestartMatrixBudget 红
//     （重启后额度重置、第四发 200）；
//   - serve 的 defer auditRequest 撤掉 → TestStoreRestartMatrixAudit 红
//     （重启后库内无 request 行）；
//   - auditStore 改成「失败即拒绝请求」→ TestStorePersistenceFailure 红。

import (
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

// w59PostChat 发一发聊天请求（可带会话 cookie），返回结果 + 本次 Set-Cookie
// （会话 cookie；未重发 = nil——「同一会话继续有效」的可观测判据）。
func w59PostChat(t *testing.T, base, body, sid string) (chatRes, *http.Cookie) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if sid != "" {
		req.Header.Set("Cookie", sessionCookieName+"="+sid)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var set *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == sessionCookieName {
			set = c
		}
	}
	return chatRes{code: resp.StatusCode, body: raw, retryAfter: resp.Header.Get("Retry-After")}, set
}

// w59StartServer 启动被测服务（NewServer 的启动错误面：失败即 Fatal）。
func w59StartServer(t *testing.T, cfg *Config) (*Server, *httptest.Server) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg.Validate: %v", err)
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := httptest.NewServer(s.Handler())
	return s, srv
}

// w59StopServer 停机（httptest 关 + 既有停机钩子：关闭 store——main.go 的接线
// 不在本单写面，ShutdownCleanup 是 main 唯一调用的收尾面）。
func w59StopServer(t *testing.T, s *Server, srv *httptest.Server) {
	t.Helper()
	srv.Close()
	if !s.ShutdownCleanup(2 * time.Second) {
		t.Fatalf("ShutdownCleanup 未收束")
	}
}

// w59ChatCfg 单上游（成功文本流）+ public(open) + 可选 store 的配置。
func w59ChatCfg(t *testing.T, storePath string, pub *PublicSection) *Config {
	t.Helper()
	up := goodClaudeUpstream(t)
	cfg := failoverCfg(anthUp("good", up.srv.URL))
	cfg.Public = pub
	if storePath != "" {
		cfg.Store = &StoreSection{Enabled: true, Path: storePath}
	}
	return cfg
}

// w59StoreSessions 直开库读会话行数（重启恢复面断言用）。
func w59StoreSessions(t *testing.T, path string) []storeSession {
	t.Helper()
	st, err := OpenStore(&Config{Store: &StoreSection{Enabled: true, Path: path}})
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer func() { _ = st.Close() }()
	rows, err := st.Sessions()
	if err != nil {
		t.Fatalf("Sessions: %v", err)
	}
	return rows
}

// ---------------------------------------------------------------------------
// 重启矩阵①：sessions 恢复（store 档）/ 全员重来（内存档）
// ---------------------------------------------------------------------------

// TestStoreRestartMatrixSessions 设计 §5.3 行「sessions/conversations/messages
// 重启后恢复」：store 档重启后旧 cookie 继续有效（不重发 Set-Cookie、库内仍 1 行
// 会话）；对照 = 内存档重启后旧 cookie 失效（重发新会话——文档化行为）。
func TestStoreRestartMatrixSessions(t *testing.T) {
	t.Run("store", func(t *testing.T) {
		path := w59TempStorePath(t)
		cfg := w59ChatCfg(t, path, &PublicSection{Enabled: true})
		s1, srv1 := w59StartServer(t, cfg)
		res1, c1 := w59PostChat(t, srv1.URL, w58LightBody, "")
		if res1.code != http.StatusOK || c1 == nil || c1.Value == "" {
			t.Fatalf("首请求 = %d set-cookie=%v（want 200 + 会话 cookie）", res1.code, c1)
		}
		if rows := w59StoreSessions(t, path); len(rows) != 1 || rows[0].SID != c1.Value {
			t.Fatalf("库内会话 = %+v（want 1 行 = %s）", rows, c1.Value)
		}
		// 重启（关 store → 新 Server 实例：内存会话表为空）。
		w59StopServer(t, s1, srv1)
		s2, srv2 := w59StartServer(t, cfg)
		defer w59StopServer(t, s2, srv2)
		if s2.sessions.len() != 0 {
			t.Fatalf("新实例内存会话表 = %d（want 0——恢复靠 store）", s2.sessions.len())
		}
		res2, c2 := w59PostChat(t, srv2.URL, w58LightBody, c1.Value)
		if res2.code != http.StatusOK {
			t.Fatalf("重启后带旧 cookie = %d（want 200——sessions 恢复）", res2.code)
		}
		if c2 != nil {
			t.Fatalf("重启后重发了 Set-Cookie（sid=%s——会话未恢复，§5.3 sessions 行红）", c2.Value)
		}
		rows := w59StoreSessions(t, path)
		if len(rows) != 1 || rows[0].SID != c1.Value {
			t.Fatalf("重启后库内会话 = %+v（want 仍 1 行 = %s——未重建新会话）", rows, c1.Value)
		}
	})
	t.Run("memory", func(t *testing.T) {
		cfg := w59ChatCfg(t, "", &PublicSection{Enabled: true})
		s1, srv1 := w59StartServer(t, cfg)
		_, c1 := w59PostChat(t, srv1.URL, w58LightBody, "")
		if c1 == nil {
			t.Fatalf("首请求未签发会话 cookie")
		}
		w59StopServer(t, s1, srv1)
		s2, srv2 := w59StartServer(t, cfg)
		defer w59StopServer(t, s2, srv2)
		res2, c2 := w59PostChat(t, srv2.URL, w58LightBody, c1.Value)
		if res2.code != http.StatusOK {
			t.Fatalf("内存档重启后 = %d（want 200——重新签发）", res2.code)
		}
		if c2 == nil || c2.Value == c1.Value {
			t.Fatalf("内存档重启应重新签发会话（set-cookie=%v，旧=%s——§5.3 对照行）", c2, c1.Value)
		}
	})
}

// ---------------------------------------------------------------------------
// 重启矩阵②：usage_counters 恢复（跨重启预算不重置）
// ---------------------------------------------------------------------------

// TestStoreRestartMatrixBudget 设计 §5.3 行「usage_counters（日额度）重启后恢复」：
// 预算 2 units（轻档 = 1 unit/发）——两发 200 后第三发 429；**重启后第四发仍 429**
// （额度未重置，P1-6）。对照 = 内存档重启后额度重置（第四发 200——文档化行为）。
func TestStoreRestartMatrixBudget(t *testing.T) {
	t.Run("store", func(t *testing.T) {
		path := w59TempStorePath(t)
		cfg := w59ChatCfg(t, path, &PublicSection{Enabled: true, DailyBudgetUnits: new(2)})
		s1, srv1 := w59StartServer(t, cfg)
		for i := 1; i <= 2; i++ {
			if res, _ := w59PostChat(t, srv1.URL, w58LightBody, ""); res.code != http.StatusOK {
				t.Fatalf("第 %d 发 = %d（want 200）", i, res.code)
			}
		}
		if res, _ := w59PostChat(t, srv1.URL, w58LightBody, ""); res.code != http.StatusTooManyRequests ||
			!strings.Contains(string(res.body), "daily_quota_exhausted") {
			t.Fatalf("预算耗尽 = %d %s（want 429 daily_quota_exhausted）", res.code, res.body)
		}
		// 台账落库（global / 当日）：2 req / 2 units。
		st, err := OpenStore(cfg)
		if err != nil {
			t.Fatalf("OpenStore: %v", err)
		}
		day := storeDay(time.Now())
		u, ok, err := st.UsageGet(storeUsageScopeGlobal, day)
		if err != nil || !ok || u.Requests != 2 || u.CostUnits != 2 {
			t.Fatalf("库内台账 = %+v ok=%v err=%v（want 2 req / 2 units）", u, ok, err)
		}
		_ = st.Close()
		w59StopServer(t, s1, srv1)
		// 重启：额度必须从库内载入（**不重置**）。
		s2, srv2 := w59StartServer(t, cfg)
		defer w59StopServer(t, s2, srv2)
		used, daily, gotDay := s2.publicLimits.ledger.stats()
		if used != 2 || daily != 2 || gotDay != day {
			t.Fatalf("重启后台账 = used=%d daily=%d day=%s（want 2/2/%s——§5.3 usage 行）", used, daily, gotDay, day)
		}
		if res, _ := w59PostChat(t, srv2.URL, w58LightBody, ""); res.code != http.StatusTooManyRequests {
			t.Fatalf("重启后第四发 = %d（want 429——跨重启预算不重置）", res.code)
		}
	})
	t.Run("memory", func(t *testing.T) {
		cfg := w59ChatCfg(t, "", &PublicSection{Enabled: true, DailyBudgetUnits: new(2)})
		s1, srv1 := w59StartServer(t, cfg)
		for i := 1; i <= 2; i++ {
			if res, _ := w59PostChat(t, srv1.URL, w58LightBody, ""); res.code != http.StatusOK {
				t.Fatalf("第 %d 发 = %d（want 200）", i, res.code)
			}
		}
		w59StopServer(t, s1, srv1)
		s2, srv2 := w59StartServer(t, cfg)
		defer w59StopServer(t, s2, srv2)
		if res, _ := w59PostChat(t, srv2.URL, w58LightBody, ""); res.code != http.StatusOK {
			t.Fatalf("内存档重启后 = %d（want 200——额度重置，§5.3 对照行）", res.code)
		}
	})
}

// ---------------------------------------------------------------------------
// 重启矩阵③：audit_log 恢复 + ring 清零（对照行）
// ---------------------------------------------------------------------------

// TestStoreRestartMatrixAudit 设计 §5.3 行「audit_log 重启后恢复」+ 对照行
// 「adminLog ring 清零」：请求 + 会话签发落库；重启后库内仍可查（session_create
// + request 行），request 行与 attempts 日志面同 request_id（join 可对账），
// sid 只落前缀（8 字符——设计 §1.1）；新实例 ring 为空。
func TestStoreRestartMatrixAudit(t *testing.T) {
	lb := captureServiceLogs(t)
	path := w59TempStorePath(t)
	cfg := w59ChatCfg(t, path, &PublicSection{Enabled: true})
	s1, srv1 := w59StartServer(t, cfg)
	res, c1 := w59PostChat(t, srv1.URL, w58LightBody, "")
	if res.code != http.StatusOK || c1 == nil {
		t.Fatalf("请求 = %d set-cookie=%v（want 200 + cookie）", res.code, c1)
	}
	if !w58RingHasKind(s1, adminLogSessionCreate) {
		t.Fatalf("ring 缺 %s（P0-6 面回归）", adminLogSessionCreate)
	}
	w59StopServer(t, s1, srv1)

	// 重启：ring 清零（对照行），审计面在库内（恢复行）。
	s2, srv2 := w59StartServer(t, cfg)
	defer w59StopServer(t, s2, srv2)
	if kinds := w58RingKinds(s2); len(kinds) != 0 {
		t.Fatalf("重启后 ring = %v（want 空——§5.3 ring 清零行）", kinds)
	}
	st, err := OpenStore(cfg)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer func() { _ = st.Close() }()
	creates, err := st.AuditQuery(adminLogSessionCreate, time.Time{}, 10)
	if err != nil || len(creates) != 1 {
		t.Fatalf("库内 session_create = %+v err=%v（want 1 行——§5.3 audit 恢复行）", creates, err)
	}
	if creates[0].SID != c1.Value[:8] {
		t.Fatalf("审计行 sid = %q（want 前缀 %q——只落前缀，设计 §1.1）", creates[0].SID, c1.Value[:8])
	}
	reqs, err := st.AuditQuery(storeAuditKindRequest, time.Time{}, 10)
	if err != nil || len(reqs) != 1 {
		t.Fatalf("库内 request 行 = %+v err=%v（want 1 行）", reqs, err)
	}
	row := reqs[0]
	if row.SID != c1.Value[:8] || row.RequestID == "" {
		t.Fatalf("request 行 sid/req = %q/%q（want 前缀 + 非空 request_id）", row.SID, row.RequestID)
	}
	if !strings.Contains(row.Detail, `"entry":"openai_chat"`) || !strings.Contains(row.Detail, `"outcome":"completed"`) {
		t.Fatalf("request 行 detail = %s（want entry + outcome）", row.Detail)
	}
	// attempts 面 join：日志行的 req 与审计行 request_id 同源。
	re := regexp.MustCompile(`"req":"(req_[0-9a-f]+)"`)
	m := re.FindStringSubmatch(lb.String())
	if m == nil {
		t.Fatalf("未找到 attempts 日志行的 req（日志面回归）：\n%s", lb.String())
	}
	if m[1] != row.RequestID {
		t.Fatalf("审计 request_id = %s（want 与 attempts 行同源 %s）", row.RequestID, m[1])
	}
}

// ---------------------------------------------------------------------------
// 重启矩阵④：限流桶清零（对照行；store 无关）
// ---------------------------------------------------------------------------

// TestStoreRestartMatrixRateLimitBucketsCleared 设计 §5.3 行「限流桶重启后清零
// （全量回填）」：per-IP 桶打满 → 429；重启 → 200（内存态、重启清零）。
func TestStoreRestartMatrixRateLimitBucketsCleared(t *testing.T) {
	path := w59TempStorePath(t)
	cfg := w59ChatCfg(t, path, &PublicSection{Enabled: true})
	cfg.UI = &UISection{RateLimitPerMin: 1}
	s1, srv1 := w59StartServer(t, cfg)
	if res, _ := w59PostChat(t, srv1.URL, w58LightBody, ""); res.code != http.StatusOK {
		t.Fatalf("第一发 = %d（want 200）", res.code)
	}
	if res, _ := w59PostChat(t, srv1.URL, w58LightBody, ""); res.code != http.StatusTooManyRequests {
		t.Fatalf("第二发 = %d（want 429——桶已满）", res.code)
	}
	w59StopServer(t, s1, srv1)
	s2, srv2 := w59StartServer(t, cfg)
	defer w59StopServer(t, s2, srv2)
	if res, _ := w59PostChat(t, srv2.URL, w58LightBody, ""); res.code != http.StatusOK {
		t.Fatalf("重启后 = %d（want 200——限流桶清零行）", res.code)
	}
}

// ---------------------------------------------------------------------------
// 启动动作：打开 → GC → 摘要行（设计 §5.3）
// ---------------------------------------------------------------------------

// TestStoreStartupGCAction 启动动作：过期会话/软删超期列表/超期审计在 NewServer
// 期间被 GC 清掉；启动日志一行摘要（无敏感面：只有路径 + 会话数 + 逐表行数）。
func TestStoreStartupGCAction(t *testing.T) {
	lb := captureServiceLogs(t)
	path := w59TempStorePath(t)
	now := time.Now().UTC()
	st, err := OpenStore(&Config{Store: &StoreSection{Enabled: true, Path: path}})
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	// 过期会话 + 列表 + 消息 + 回合 + 超期审计（启动 GC 应全清）。
	if err := st.SessionCreate(storeSession{
		SID: "sid-expired", CreatedAt: now.Add(-72 * time.Hour), LastSeenAt: now.Add(-72 * time.Hour),
		ExpiresAt: now.Add(-48 * time.Hour), AbsExpiresAt: now.Add(-24 * time.Hour),
	}); err != nil {
		t.Fatalf("SessionCreate: %v", err)
	}
	if err := st.ConversationUpsert(storeConversation{ID: "conv-x", SID: "sid-expired", Rev: 1, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("ConversationUpsert: %v", err)
	}
	if err := st.MessageAppend(storeMessage{ID: "msg-x", ConvID: "conv-x", Seq: 1, Role: "user", Content: "x", CreatedAt: now}); err != nil {
		t.Fatalf("MessageAppend: %v", err)
	}
	if err := st.TurnAppend(storeTurn{ID: "turn-x", ConvID: "conv-x", RequestID: "req-x", AttemptNo: 1, Status: "completed", CredentialIndex: -1, CreatedAt: now}); err != nil {
		t.Fatalf("TurnAppend: %v", err)
	}
	if _, err := st.AuditAppend(storeAudit{TS: now.Add(-40 * 24 * time.Hour), Kind: "request"}); err != nil {
		t.Fatalf("AuditAppend: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	cfg := w59ChatCfg(t, path, &PublicSection{Enabled: true})
	s, srv := w59StartServer(t, cfg)
	defer w59StopServer(t, s, srv)
	rows, err := s.store.Sessions()
	if err != nil || len(rows) != 0 {
		t.Fatalf("启动 GC 后会话 = %+v err=%v（want 0）", rows, err)
	}
	if list, _ := s.store.ConversationsBySession("sid-expired"); len(list) != 0 {
		t.Fatalf("启动 GC 后列表 = %d（want 0——级联）", len(list))
	}
	if turns, _ := s.store.TurnsByRequest("req-x"); len(turns) != 0 {
		t.Fatalf("启动 GC 后回合 = %d（want 0——级联）", len(turns))
	}
	if audit, _ := s.store.AuditQuery("", time.Time{}, 10); len(audit) != 0 {
		t.Fatalf("启动 GC 后审计 = %+v（want 0——保留期 30d）", audit)
	}
	logs := lb.String()
	if !strings.Contains(logs, "store ready (path=") || !strings.Contains(logs, "sessions=0") {
		t.Fatalf("缺启动摘要行（want `store ready (path=… sessions=0 …)`）：\n%s", logs)
	}
	if !strings.Contains(logs, "gc: sessions=1") {
		// 摘要里的 GC 计数应反映本次清理（1 会话 + 1 列表 + 1 消息 + 1 回合 + 1 审计）。
		t.Fatalf("启动摘要缺 GC 计数：\n%s", logs)
	}
}

// ---------------------------------------------------------------------------
// 持久化失败绝不影响请求路径（设计 §2.4-E）
// ---------------------------------------------------------------------------

// TestStorePersistenceFailure 注入故障 store（读写全错）：请求仍 200（会话在
// 内存档照常签发）、ring 落 `store_error`（新事件型别，零敏感面）。
func TestStorePersistenceFailure(t *testing.T) {
	lb := captureServiceLogs(t)
	path := w59TempStorePath(t)
	cfg := w59ChatCfg(t, path, &PublicSection{Enabled: true})
	s, srv := w59StartServer(t, cfg)
	defer w59StopServer(t, s, srv)
	// 换入故障 store（会话/台账/审计面全错）；原句柄保留——收尾时还原，让停机
	// 钩子照常关闭（Windows 下泄漏句柄会挡住 t.TempDir 清理）。
	orig := s.store
	fs := &w59FailStore{err: errInjectedStore}
	s.store = fs
	defer func() { s.store = orig }()
	s.sessions.persist = fs
	s.sessions.onPersistErr = s.storeError
	s.publicLimits.attachStore(fs, s.storeError)

	res, c := w59PostChat(t, srv.URL, w58LightBody, "")
	if res.code != http.StatusOK {
		t.Fatalf("故障 store 下请求 = %d（want 200——持久化失败绝不进控制流）", res.code)
	}
	if c == nil {
		t.Fatalf("故障 store 下未签发会话 cookie（内存档应照常）")
	}
	if !w58RingHasKind(s, adminLogStoreError) {
		t.Fatalf("ring 缺 %s（故障面应可见）：%v", adminLogStoreError, w58RingKinds(s))
	}
	waitForLogContains(t, lb, "store_error: session create")
	if !strings.Contains(lb.String(), errInjectedStore.Error()) {
		t.Fatalf("日志缺存储层错误文本：\n%s", lb.String())
	}
	// 会话回查面（重启恢复路径）失败也走同一 sink。
	if _, ok := s.sessions.get("sid-any", time.Now()); ok {
		t.Fatalf("故障 store 的会话回查 ok=true（want false——回退「无会话」）")
	}
	if !w58RingHasKind(s, adminLogStoreError) {
		t.Fatalf("回查失败未落 ring")
	}
}

// errInjectedStore 故障注入错误（测试用哨兵）。
var errInjectedStore = errInjected("injected store failure")

// errInjected 测试用错误型别（避免与仓储错误文本撞车）。
type errInjected string

func (e errInjected) Error() string { return string(e) }
