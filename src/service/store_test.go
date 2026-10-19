package service

// store_test.go —— P1-1 持久化骨架测试（board A2A-L19-W59-1）：两档（内存 /
// SQLite）契约一致（会话/列表/消息/回合/台账/审计 CRUD）、schema 六表 + 迁移锚
// + 索引、PRAGMA（WAL/foreign_keys）、**迁移幂等**（重复 Migrate / 重复打开）、
// **GC 级联**（过期会话 → conversations → messages → turns；软删硬删；保留期
// turns 30d / audit 30d / usage 90d）、**store 缺省零回归硬门**（OpenStore 返回
// nil + 配置校验）。
//
// 反证锚点（回退即红——原样输出见 tmp/lanes/W59-a/proofs/）：
//   - GC 的会话删除去掉级联计数/删除（只删 sessions 行）→ TestStoreGCCascade 红；
//   - Migrate 去掉版本短路（每次都重放 DDL）→ TestStoreSQLiteMigrationIdempotent
//     红（schema_meta 版本行重复/表数漂移）；
//   - OpenStore 去掉 enabled 门（恒开库）→ TestStoreDisabledZeroRegression 红；
//   - 会话回查去掉过期判定 → TestStoreGCCascade / 重启矩阵红。

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// w59TempStorePath 临时库路径（t.TempDir 下；不落仓库）。
func w59TempStorePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "any2api.db")
}

// w59OpenSQLite 打开 SQLite 档（测试用；失败即 Fatal）。
func w59OpenSQLite(t *testing.T, path string) *sqliteStore {
	t.Helper()
	st, err := OpenStore(&Config{Store: &StoreSection{Enabled: true, Path: path}})
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	sql, ok := st.(*sqliteStore)
	if !ok {
		t.Fatalf("OpenStore 返回 %T（want *sqliteStore）", st)
	}
	t.Cleanup(func() { _ = sql.Close() })
	return sql
}

// w59HasName 名字表是否含目标（sqlite_master 面）。
func w59HasName(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// 硬门：store 缺省 = 不挂载（零行为变化）
// ---------------------------------------------------------------------------

// TestStoreDisabledZeroRegression store 缺省（enabled=false / 不写本节）：
// OpenStore 返回 (nil, nil)——全部消费方走 P0 内存路径（零回归硬门）。
func TestStoreDisabledZeroRegression(t *testing.T) {
	for _, cfg := range []*Config{
		nil,
		{},
		{Store: &StoreSection{}},
		{Store: &StoreSection{Enabled: false}},
	} {
		st, err := OpenStore(cfg)
		if err != nil {
			t.Fatalf("OpenStore(%+v) err = %v（want nil）", cfg, err)
		}
		if st != nil {
			t.Fatalf("OpenStore(%+v) = %T（want nil——硬门）", cfg, st)
		}
	}
	// 服务装配面：缺省配置下 store / 会话持久化 / 台账持久化三处都不挂。
	s, err := NewServer(failoverCfg())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if s.store != nil {
		t.Fatalf("缺省 store = %T（want nil）", s.store)
	}
	if s.sessions != nil || s.publicLimits != nil {
		t.Fatalf("缺省 public 面应不挂载（sessions=%v publicLimits=%v）", s.sessions != nil, s.publicLimits != nil)
	}
	// public 开 + store 关：会话/台账仍是内存档（persist == nil）。
	s2, err := NewServer(&Config{
		Listen: "127.0.0.1:0", HeartbeatSeconds: 15,
		Public: &PublicSection{Enabled: true},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if s2.sessions == nil || s2.sessions.persist != nil {
		t.Fatalf("public 开 + store 关：会话表应为内存档（persist=%v）", s2.sessions.persist)
	}
	if s2.publicLimits == nil || s2.publicLimits.ledger == nil || s2.publicLimits.ledger.persist != nil {
		t.Fatalf("public 开 + store 关：台账应为内存档")
	}
}

// TestStoreConfigValidate store 段配置校验（显式报错，绝不静默降级）。
func TestStoreConfigValidate(t *testing.T) {
	// enabled=true 缺 path → 报错（绝不静默降级为内存档——那会静默丢数据）。
	if err := (&Config{Store: &StoreSection{Enabled: true}}).Validate(); err == nil ||
		!strings.Contains(err.Error(), "store.path required") {
		t.Fatalf("enabled 缺 path: err = %v（want store.path required）", err)
	}
	if err := (&Config{Store: &StoreSection{Enabled: true, Path: "   "}}).Validate(); err == nil {
		t.Fatalf("enabled + 全空白 path: want 报错")
	}
	// enabled=false 给 path → 报错（无意义配置 = 歧义，与 public.access_keys 同纪律）。
	if err := (&Config{Store: &StoreSection{Path: "data/x.db"}}).Validate(); err == nil ||
		!strings.Contains(err.Error(), "only valid when store.enabled") {
		t.Fatalf("disabled + path: err = %v（want 显式报错）", err)
	}
	// 合法形态：不报错。
	legal := failoverCfg(anthUp("good", "http://127.0.0.1:1"))
	legal.Store = &StoreSection{Enabled: true, Path: "data/x.db"}
	if err := legal.Validate(); err != nil {
		t.Fatalf("enabled + path: err = %v（want nil）", err)
	}
	// OpenStore 的缺 path 防御（Validate 之外的第二道）。
	if _, err := OpenStore(&Config{Store: &StoreSection{Enabled: true}}); err == nil {
		t.Fatalf("OpenStore 缺 path: want 报错")
	}
}

// ---------------------------------------------------------------------------
// 两档契约（内存 / SQLite 同一断言集）
// ---------------------------------------------------------------------------

// TestStoreContractTiers 两档（内存档 / SQLite 档）跑同一契约断言集。
func TestStoreContractTiers(t *testing.T) {
	t.Run("memory", func(t *testing.T) { storeContract(t, newMemoryStore()) })
	t.Run("sqlite", func(t *testing.T) { storeContract(t, w59OpenSQLite(t, w59TempStorePath(t))) })
}

// storeContract 两档共用的契约断言（CRUD + 唯一约束 + 台账 + 审计）。
func storeContract(t *testing.T, st Store) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Millisecond)
	day := storeDay(now)

	// ① 会话 CRUD（含 upsert 幂等与 touch）。
	sess := storeSession{
		SID: "sid-contract-1", KeyID: "abcd1234",
		CreatedAt: now, LastSeenAt: now,
		ExpiresAt: now.Add(time.Hour), AbsExpiresAt: now.Add(24 * time.Hour),
	}
	if err := st.SessionCreate(sess); err != nil {
		t.Fatalf("SessionCreate: %v", err)
	}
	if err := st.SessionCreate(sess); err != nil {
		t.Fatalf("SessionCreate（重放，应幂等）: %v", err)
	}
	got, ok, err := st.SessionGet(sess.SID)
	if err != nil || !ok {
		t.Fatalf("SessionGet: ok=%v err=%v", ok, err)
	}
	if got.KeyID != sess.KeyID || !got.CreatedAt.Equal(sess.CreatedAt) || !got.AbsExpiresAt.Equal(sess.AbsExpiresAt) {
		t.Fatalf("SessionGet 往返漂移: %+v", got)
	}
	if _, ok, _ := st.SessionGet("sid-absent"); ok {
		t.Fatalf("SessionGet(缺席) ok=true（want false）")
	}
	if err := st.SessionTouch(sess.SID, now.Add(time.Minute), now.Add(2*time.Hour)); err != nil {
		t.Fatalf("SessionTouch: %v", err)
	}
	got, _, _ = st.SessionGet(sess.SID)
	if !got.LastSeenAt.Equal(now.Add(time.Minute)) || !got.ExpiresAt.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("SessionTouch 未落库: %+v", got)
	}
	// touch 缺席会话 = 静默（会话面按无会话重新签发）。
	if err := st.SessionTouch("sid-absent", now, now); err != nil {
		t.Fatalf("SessionTouch(缺席) err = %v（want nil——静默）", err)
	}
	all, err := st.Sessions()
	if err != nil || len(all) != 1 || all[0].SID != sess.SID {
		t.Fatalf("Sessions = %+v err=%v（want 1 行）", all, err)
	}

	// ② 会话列表 / 消息 / 回合（外键链：session → conversation → messages/turns）。
	conv := storeConversation{
		ID: "conv-1", SID: sess.SID, Title: "标题", Model: "m-1", Rev: 1,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := st.ConversationUpsert(conv); err != nil {
		t.Fatalf("ConversationUpsert: %v", err)
	}
	conv.Rev, conv.Title = 2, "改标题"
	if err := st.ConversationUpsert(conv); err != nil {
		t.Fatalf("ConversationUpsert（更新）: %v", err)
	}
	gotConv, ok, err := st.ConversationGet(conv.ID)
	if err != nil || !ok || gotConv.Rev != 2 || gotConv.Title != "改标题" || !gotConv.DeletedAt.IsZero() {
		t.Fatalf("ConversationGet 往返: %+v ok=%v err=%v", gotConv, ok, err)
	}
	if list, err := st.ConversationsBySession(sess.SID); err != nil || len(list) != 1 {
		t.Fatalf("ConversationsBySession = %+v err=%v（want 1）", list, err)
	}
	for i, msg := range []storeMessage{
		{ID: "msg-1", ConvID: conv.ID, Seq: 1, Role: "user", Content: `[{"type":"text","text":"hi"}]`, Status: "done", CreatedAt: now},
		{ID: "msg-2", ConvID: conv.ID, Seq: 2, Role: "assistant", Content: `[{"type":"thinking","text":"…"}]`, Status: "streaming", CreatedAt: now},
	} {
		if err := st.MessageAppend(msg); err != nil {
			t.Fatalf("MessageAppend[%d]: %v", i, err)
		}
	}
	// seq 复用 = 报错（UNIQUE(conv_id, seq)）——绝不静默覆盖。
	if err := st.MessageAppend(storeMessage{ID: "msg-3", ConvID: conv.ID, Seq: 1, Role: "user", Content: "x", CreatedAt: now}); err == nil {
		t.Fatalf("MessageAppend(seq 复用) err = nil（want 报错）")
	}
	msgs, err := st.MessagesByConversation(conv.ID)
	if err != nil || len(msgs) != 2 || msgs[0].Seq != 1 || msgs[1].Seq != 2 || msgs[1].Status != "streaming" {
		t.Fatalf("MessagesByConversation = %+v err=%v", msgs, err)
	}
	turns := []storeTurn{
		{ID: "turn-1", ConvID: conv.ID, RequestID: "req-1", AttemptNo: 1, Upstream: "good", CredentialIndex: -1, Status: "completed", UsageJSON: `{"in":1,"out":2}`, CreatedAt: now},
		{ID: "turn-2", ConvID: conv.ID, RequestID: "req-1", AttemptNo: 2, Upstream: "good", CredentialIndex: 3, Status: "failed", CreatedAt: now},
	}
	for i, tn := range turns {
		if err := st.TurnAppend(tn); err != nil {
			t.Fatalf("TurnAppend[%d]: %v", i, err)
		}
	}
	gotTurns, err := st.TurnsByRequest("req-1")
	if err != nil || len(gotTurns) != 2 {
		t.Fatalf("TurnsByRequest = %+v err=%v（want 2）", gotTurns, err)
	}
	if gotTurns[0].CredentialIndex != -1 || gotTurns[1].CredentialIndex != 3 {
		t.Fatalf("credential_index 往返漂移: %d / %d（want -1 / 3——-1 = 无池，绝不伪造成 0）",
			gotTurns[0].CredentialIndex, gotTurns[1].CredentialIndex)
	}

	// ③ 台账（累加 + 读回 + scope 列表）。
	if err := st.UsageAdd(storeUsageScopeGlobal, day, 1, 6); err != nil {
		t.Fatalf("UsageAdd: %v", err)
	}
	if err := st.UsageAdd(storeUsageScopeGlobal, day, 1, 1); err != nil {
		t.Fatalf("UsageAdd: %v", err)
	}
	u, ok, err := st.UsageGet(storeUsageScopeGlobal, day)
	if err != nil || !ok || u.Requests != 2 || u.CostUnits != 7 {
		t.Fatalf("UsageGet = %+v ok=%v err=%v（want 2 req / 7 units）", u, ok, err)
	}
	if _, ok, _ := st.UsageGet("session:sid-contract-1", day); ok {
		t.Fatalf("UsageGet(未记账 scope) ok=true（want false）")
	}
	if scopes, err := st.UsageScopes(day); err != nil || len(scopes) != 1 || scopes[0].Scope != storeUsageScopeGlobal {
		t.Fatalf("UsageScopes = %+v err=%v", scopes, err)
	}

	// ④ 审计（追加 + 查询 + 过滤 + detail 往返）。
	if _, err := st.AuditAppend(storeAudit{TS: now, SID: "sid-cont", KeyID: "abcd1234", Kind: adminLogSessionCreate}); err != nil {
		t.Fatalf("AuditAppend: %v", err)
	}
	if _, err := st.AuditAppend(storeAudit{TS: now.Add(time.Second), SID: "sid-cont", Kind: storeAuditKindRequest, RequestID: "req-1", Detail: `{"entry":"openai_chat"}`}); err != nil {
		t.Fatalf("AuditAppend: %v", err)
	}
	reqs, err := st.AuditQuery(storeAuditKindRequest, time.Time{}, 10)
	if err != nil || len(reqs) != 1 || reqs[0].RequestID != "req-1" || !strings.Contains(reqs[0].Detail, "openai_chat") {
		t.Fatalf("AuditQuery(kind) = %+v err=%v", reqs, err)
	}
	allAudit, err := st.AuditQuery("", time.Time{}, 10)
	if err != nil || len(allAudit) != 2 {
		t.Fatalf("AuditQuery(全部) = %d err=%v（want 2）", len(allAudit), err)
	}
	since, err := st.AuditQuery("", now.Add(500*time.Millisecond), 10)
	if err != nil || len(since) != 1 || since[0].Kind != storeAuditKindRequest {
		t.Fatalf("AuditQuery(since) = %+v err=%v（want 仅 request 行）", since, err)
	}
	if _, err := st.AuditAppend(storeAudit{Kind: "no_ts"}); err != nil { // ts 零值 = 调用时刻
		t.Fatalf("AuditAppend(零 ts): %v", err)
	}
	if lim, err := st.AuditQuery("", time.Time{}, 1); err != nil || len(lim) != 1 {
		t.Fatalf("AuditQuery(limit=1) = %d err=%v", len(lim), err)
	}

	// ⑤ 删会话 = 级联真删（列表/消息/回合一并消失——设计 §2.5 删除路径）。
	if ok, err := st.SessionDelete(sess.SID); err != nil || !ok {
		t.Fatalf("SessionDelete: ok=%v err=%v", ok, err)
	}
	if ok, _ := st.SessionDelete(sess.SID); ok {
		t.Fatalf("SessionDelete（重复）ok=true（want false）")
	}
	if list, _ := st.ConversationsBySession(sess.SID); len(list) != 0 {
		t.Fatalf("级联后会话列表 = %d 行（want 0）", len(list))
	}
	if msgs, _ := st.MessagesByConversation(conv.ID); len(msgs) != 0 {
		t.Fatalf("级联后消息 = %d 行（want 0）", len(msgs))
	}
	if turns, _ := st.TurnsByRequest("req-1"); len(turns) != 0 {
		t.Fatalf("级联后回合 = %d 行（want 0）", len(turns))
	}
	if all, _ := st.Sessions(); len(all) != 0 {
		t.Fatalf("删除后会话 = %d 行（want 0）", len(all))
	}
}

// ---------------------------------------------------------------------------
// GC（级联 + 保留期）
// ---------------------------------------------------------------------------

// TestStoreGCCascade GC 级联：过期会话带删 conversations → messages → turns，
// 软删超期硬删，保留期（turns 30d / audit 30d / usage 90d）逐条生效。
func TestStoreGCCascade(t *testing.T) {
	t.Run("memory", func(t *testing.T) { storeGCCascade(t, newMemoryStore()) })
	t.Run("sqlite", func(t *testing.T) { storeGCCascade(t, w59OpenSQLite(t, w59TempStorePath(t))) })
}

// storeGCCascade 两档共用的 GC 断言。
func storeGCCascade(t *testing.T, st Store) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Millisecond)
	// 活会话（未过期——必须活下来）。
	live := storeSession{
		SID: "sid-live", CreatedAt: now, LastSeenAt: now,
		ExpiresAt: now.Add(time.Hour), AbsExpiresAt: now.Add(24 * time.Hour),
	}
	if err := st.SessionCreate(live); err != nil {
		t.Fatalf("SessionCreate(live): %v", err)
	}
	// 过期会话（idle 点与绝对点都在过去）+ 列表 + 两条消息 + 一条回合。
	dead := storeSession{
		SID: "sid-dead", CreatedAt: now.Add(-48 * time.Hour), LastSeenAt: now.Add(-48 * time.Hour),
		ExpiresAt: now.Add(-time.Hour), AbsExpiresAt: now.Add(-time.Hour),
	}
	if err := st.SessionCreate(dead); err != nil {
		t.Fatalf("SessionCreate(dead): %v", err)
	}
	deadConv := storeConversation{ID: "conv-dead", SID: dead.SID, Title: "x", Rev: 1, CreatedAt: now, UpdatedAt: now}
	if err := st.ConversationUpsert(deadConv); err != nil {
		t.Fatalf("ConversationUpsert(dead): %v", err)
	}
	for i, seq := range []int{1, 2} {
		if err := st.MessageAppend(storeMessage{
			ID: "msg-dead-" + string(rune('a'+i)), ConvID: deadConv.ID, Seq: seq,
			Role: "user", Content: "x", CreatedAt: now,
		}); err != nil {
			t.Fatalf("MessageAppend(dead): %v", err)
		}
	}
	if err := st.TurnAppend(storeTurn{
		ID: "turn-dead", ConvID: deadConv.ID, RequestID: "req-dead", AttemptNo: 1,
		Status: "completed", CredentialIndex: -1, CreatedAt: now,
	}); err != nil {
		t.Fatalf("TurnAppend(dead): %v", err)
	}
	// 保留期面：软删超期列表（7d）、超期回合（30d）、超期审计（30d）、超期台账（90d）。
	softConv := storeConversation{ID: "conv-soft", SID: live.SID, Title: "s", Rev: 1, CreatedAt: now, UpdatedAt: now}
	if err := st.ConversationUpsert(softConv); err != nil {
		t.Fatalf("ConversationUpsert(soft): %v", err)
	}
	if err := st.MessageAppend(storeMessage{ID: "msg-soft", ConvID: softConv.ID, Seq: 1, Role: "user", Content: "x", CreatedAt: now}); err != nil {
		t.Fatalf("MessageAppend(soft): %v", err)
	}
	if err := st.TurnAppend(storeTurn{ID: "turn-soft", ConvID: softConv.ID, RequestID: "req-soft", AttemptNo: 1, Status: "completed", CredentialIndex: -1, CreatedAt: now}); err != nil {
		t.Fatalf("TurnAppend(soft): %v", err)
	}
	if err := st.ConversationSoftDelete(softConv.ID, now.Add(-8*24*time.Hour)); err != nil {
		t.Fatalf("ConversationSoftDelete: %v", err)
	}
	oldTurn := storeTurn{ID: "turn-old", ConvID: "", RequestID: "req-old", AttemptNo: 1, Status: "completed", CredentialIndex: -1, CreatedAt: now.Add(-31 * 24 * time.Hour)}
	// 超期回合挂在活列表上（否则被级联先删——GC 的两条路径都要有覆盖）。
	freshConv := storeConversation{ID: "conv-fresh", SID: live.SID, Title: "f", Rev: 1, CreatedAt: now, UpdatedAt: now}
	if err := st.ConversationUpsert(freshConv); err != nil {
		t.Fatalf("ConversationUpsert(fresh): %v", err)
	}
	oldTurn.ConvID = freshConv.ID
	if err := st.TurnAppend(oldTurn); err != nil {
		t.Fatalf("TurnAppend(old): %v", err)
	}
	if _, err := st.AuditAppend(storeAudit{TS: now.Add(-31 * 24 * time.Hour), Kind: "request"}); err != nil {
		t.Fatalf("AuditAppend(old): %v", err)
	}
	if _, err := st.AuditAppend(storeAudit{TS: now, Kind: "request"}); err != nil {
		t.Fatalf("AuditAppend(fresh): %v", err)
	}
	oldDay := storeDay(now.Add(-91 * 24 * time.Hour))
	if err := st.UsageAdd(storeUsageScopeGlobal, oldDay, 5, 30); err != nil {
		t.Fatalf("UsageAdd(old): %v", err)
	}
	if err := st.UsageAdd(storeUsageScopeGlobal, storeDay(now), 1, 1); err != nil {
		t.Fatalf("UsageAdd(fresh): %v", err)
	}

	stats, err := st.GC(now)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	// ① 过期会话级联：1 会话 + 1 列表 + 2 消息 + 1 回合。
	if stats.Sessions != 1 {
		t.Fatalf("GC sessions = %d（want 1）", stats.Sessions)
	}
	if stats.Conversations < 2 { // dead 级联 + soft 硬删
		t.Fatalf("GC conversations = %d（want >= 2）", stats.Conversations)
	}
	if stats.Messages < 3 { // dead 2 + soft 1
		t.Fatalf("GC messages = %d（want >= 3）", stats.Messages)
	}
	if stats.Turns < 2 { // dead 1 + soft 1（+ 超期回合 1）
		t.Fatalf("GC turns = %d（want >= 2）", stats.Turns)
	}
	if stats.Audit != 1 || stats.Usage != 1 {
		t.Fatalf("GC audit/usage = %d/%d（want 1/1）", stats.Audit, stats.Usage)
	}
	// ② 行面核验（两档同断言）。
	if all, _ := st.Sessions(); len(all) != 1 || all[0].SID != live.SID {
		t.Fatalf("GC 后会话 = %+v（want 仅 live）", all)
	}
	if list, _ := st.ConversationsBySession(dead.SID); len(list) != 0 {
		t.Fatalf("GC 后过期会话的列表 = %d（want 0——级联）", len(list))
	}
	if msgs, _ := st.MessagesByConversation(deadConv.ID); len(msgs) != 0 {
		t.Fatalf("GC 后过期会话的消息 = %d（want 0——级联）", len(msgs))
	}
	if turns, _ := st.TurnsByRequest("req-dead"); len(turns) != 0 {
		t.Fatalf("GC 后过期会话的回合 = %d（want 0——级联）", len(turns))
	}
	if list, _ := st.ConversationsBySession(live.SID); len(list) != 1 || list[0].ID != freshConv.ID {
		t.Fatalf("GC 后活会话的列表 = %+v（want 仅 conv-fresh——软删超期硬删）", list)
	}
	if msgs, _ := st.MessagesByConversation(softConv.ID); len(msgs) != 0 {
		t.Fatalf("GC 后软删列表的消息 = %d（want 0——级联）", len(msgs))
	}
	if turns, _ := st.TurnsByRequest("req-old"); len(turns) != 0 {
		t.Fatalf("GC 后超期回合 = %d（want 0——保留期 30d）", len(turns))
	}
	if turns, _ := st.TurnsByRequest("req-soft"); len(turns) != 0 {
		t.Fatalf("GC 后软删列表的回合 = %d（want 0——级联）", len(turns))
	}
	if audit, _ := st.AuditQuery("", time.Time{}, 10); len(audit) != 1 || audit[0].TS.Before(now.Add(-time.Minute)) {
		t.Fatalf("GC 后审计 = %+v（want 仅今日行——保留期 30d）", audit)
	}
	if scopes, _ := st.UsageScopes(oldDay); len(scopes) != 0 {
		t.Fatalf("GC 后超期台账 = %+v（want 0——保留期 90d）", scopes)
	}
	if u, ok, _ := st.UsageGet(storeUsageScopeGlobal, storeDay(now)); !ok || u.CostUnits != 1 {
		t.Fatalf("GC 误删今日台账: %+v ok=%v", u, ok)
	}
	// ③ GC 幂等：再跑一次无删除。
	again, err := st.GC(now)
	if err != nil {
		t.Fatalf("GC(重复): %v", err)
	}
	if again.total() != 0 {
		t.Fatalf("GC(重复) 删除 %s（want 0）", again)
	}
}

// ---------------------------------------------------------------------------
// SQLite 档：schema / PRAGMA / 迁移幂等 / 打开面
// ---------------------------------------------------------------------------

// TestStoreSQLiteSchemaAndPragmas schema 六数据表 + 迁移锚表 + 索引 + PRAGMA。
func TestStoreSQLiteSchemaAndPragmas(t *testing.T) {
	path := w59TempStorePath(t)
	st := w59OpenSQLite(t, path)
	tables, err := st.tableNames()
	if err != nil {
		t.Fatalf("tableNames: %v", err)
	}
	for _, want := range storeSQLiteTables {
		if !w59HasName(tables, want) {
			t.Fatalf("缺表 %q（实见 %v）", want, tables)
		}
	}
	// 只允许 sqlite 内部表（sqlite_sequence —— AUTOINCREMENT）超出设计表集。
	for _, got := range tables {
		if w59HasName(storeSQLiteTables, got) || strings.HasPrefix(got, "sqlite_") {
			continue
		}
		t.Fatalf("多余表 %q（实见 %v）", got, tables)
	}
	idx, err := st.indexNames()
	if err != nil {
		t.Fatalf("indexNames: %v", err)
	}
	for _, want := range storeSQLiteIndexes {
		if !w59HasName(idx, want) {
			t.Fatalf("缺索引 %q（实见 %v）", want, idx)
		}
	}
	// PRAGMA：WAL + 外键 ON（级联的前提——单连接模型下断言即全局）。
	if v, err := st.pragmaValue("journal_mode"); err != nil || v != "wal" {
		t.Fatalf("journal_mode = %q err=%v（want wal）", v, err)
	}
	if v, err := st.pragmaValue("foreign_keys"); err != nil || v != "1" {
		t.Fatalf("foreign_keys = %q err=%v（want 1）", v, err)
	}
	// schema_meta 版本锚。
	var ver string
	if err := st.db.QueryRow(`SELECT value FROM schema_meta WHERE key = ?`, storeSchemaVersionKey).Scan(&ver); err != nil {
		t.Fatalf("读 schema_version: %v", err)
	}
	if ver != storeSchemaVersion {
		t.Fatalf("schema_version = %q（want %q）", ver, storeSchemaVersion)
	}
}

// TestStoreSQLiteMigrationIdempotent 迁移幂等：重复 Migrate / 重复打开同一库。
func TestStoreSQLiteMigrationIdempotent(t *testing.T) {
	path := w59TempStorePath(t)
	st := w59OpenSQLite(t, path)
	before, err := st.tableNames()
	if err != nil {
		t.Fatalf("tableNames: %v", err)
	}
	// 重复 Migrate：不报错、不重复建表/不重复写版本行。
	for i := range 3 {
		if err := st.Migrate(); err != nil {
			t.Fatalf("Migrate#%d: %v", i, err)
		}
	}
	after, err := st.tableNames()
	if err != nil {
		t.Fatalf("tableNames: %v", err)
	}
	if strings.Join(before, ",") != strings.Join(after, ",") {
		t.Fatalf("重复迁移改了表集: %v → %v", before, after)
	}
	var n int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM schema_meta WHERE key = ?`, storeSchemaVersionKey).Scan(&n); err != nil {
		t.Fatalf("count schema_meta: %v", err)
	}
	if n != 1 {
		t.Fatalf("schema_meta 版本行 = %d（want 1——迁移幂等）", n)
	}
	// 写入数据 → 关闭 → 重新打开（重复打开不重复迁移、数据仍在）。
	if err := st.SessionCreate(storeSession{
		SID: "sid-reopen", CreatedAt: time.Now(), LastSeenAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour), AbsExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("SessionCreate: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	st2 := w59OpenSQLite(t, path)
	rows, err := st2.Sessions()
	if err != nil || len(rows) != 1 || rows[0].SID != "sid-reopen" {
		t.Fatalf("重开后会话 = %+v err=%v（want sid-reopen）", rows, err)
	}
	after2, _ := st2.tableNames()
	if strings.Join(after, ",") != strings.Join(after2, ",") {
		t.Fatalf("重开后表集漂移: %v → %v", after, after2)
	}
}

// TestStoreSQLiteOpenCreatesDirAndFailsOnBadPath 打开面：父目录自动创建；
// 坏路径（目录当库文件）= 报错（绝不静默降级）。
func TestStoreSQLiteOpenCreatesDirAndFailsOnBadPath(t *testing.T) {
	nested := filepath.Join(t.TempDir(), "a", "b", "c", "any2api.db")
	st := w59OpenSQLite(t, nested)
	if _, err := os.Stat(nested); err != nil {
		t.Fatalf("库文件未创建: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// 目录当库文件 → open 失败（PRAGMA 阶段即触发真实打开）。
	dir := t.TempDir()
	if _, err := OpenStore(&Config{Store: &StoreSection{Enabled: true, Path: dir}}); err == nil {
		t.Fatalf("OpenStore(目录当库) err = nil（want 报错）")
	}
	// 启动面：NewServer 把 store 打开失败**原样返回**（不静默降级为内存档）。
	if _, err := NewServer(&Config{
		Listen: "127.0.0.1:0", HeartbeatSeconds: 15,
		Store: &StoreSection{Enabled: true, Path: dir},
	}); err == nil {
		t.Fatalf("NewServer(坏 store 路径) err = nil（want 启动报错）")
	}
}

// TestStoreMemoryTierParity 内存档 = Store 接口完整实现（P0 现状的 store 形态；
// 接线测试与单进程档用）。
func TestStoreMemoryTierParity(t *testing.T) {
	var st Store = newMemoryStore()
	if err := st.Migrate(); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if stats, err := st.GC(time.Now()); err != nil || stats.total() != 0 {
		t.Fatalf("GC(空库) = %s err=%v（want 0）", stats, err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := st.SessionCreate(storeSession{SID: "x"}); err == nil {
		t.Fatalf("Close 后 SessionCreate err = nil（want 报错）")
	}
}

// TestStoreGCStatsString GC 摘要行（启动日志面；无敏感面）。
func TestStoreGCStatsString(t *testing.T) {
	got := storeGCStats{Sessions: 1, Conversations: 2, Messages: 3, Turns: 4, Usage: 5, Audit: 6}.String()
	want := "sessions=1 conversations=2 messages=3 turns=4 usage=5 audit=6"
	if got != want {
		t.Fatalf("GC 摘要 = %q（want %q）", got, want)
	}
	if n := (storeGCStats{Sessions: 1, Audit: 1}).total(); n != 2 {
		t.Fatalf("total = %d（want 2）", n)
	}
}

// w59FailStore 故障注入 store（P1-2「持久化失败绝不影响请求路径」反证装置）：
// 会话/台账/审计写读全返回错误；其余走内存档实现。
type w59FailStore struct {
	memoryStore
	err error
}

func (f *w59FailStore) fail() error {
	if f.err != nil {
		return f.err
	}
	return errors.New("injected store failure")
}

func (f *w59FailStore) SessionCreate(storeSession) error { return f.fail() }
func (f *w59FailStore) SessionGet(string) (storeSession, bool, error) {
	return storeSession{}, false, f.fail()
}
func (f *w59FailStore) SessionTouch(string, time.Time, time.Time) error { return f.fail() }
func (f *w59FailStore) SessionDelete(string) (bool, error)              { return false, f.fail() }
func (f *w59FailStore) UsageAdd(string, string, int, int) error         { return f.fail() }
func (f *w59FailStore) UsageGet(string, string) (storeUsage, bool, error) {
	return storeUsage{}, false, f.fail()
}
func (f *w59FailStore) AuditAppend(storeAudit) (int64, error) { return 0, f.fail() }
