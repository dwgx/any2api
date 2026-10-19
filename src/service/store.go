// store.go 持久化骨架（P1-1；设计 `claude2api-public-service` §2/§5.3；board
// A2A-L19-W59-1）。
//
// 形态（**接口 + 两档实现**）：
//
//   - `Store` 接口：窄面——迁移 / 会话 CRUD / 会话列表 CRUD / turn append /
//     usage counter bump / audit append / GC / Close（设计 §2.4「接口窄」）。
//   - **内存档**（`memoryStore`，本文件）：进程内 map 实现（P0 现状的 store 形态
//     ——测试与「单进程档」；重启清零）。
//   - **SQLite 档**（`store_sqlite.go`，modernc 纯 Go）：`store.enabled=true` 时
//     由 `OpenStore` 打开（DDL = 设计 §2.3a 逐列同构；WAL + 外键级联）。
//
// 硬门（**零回归**）：`store.enabled` 缺省 false → `OpenStore` 返回
// `(nil, nil)`——全部消费方（会话/配额/审计）走 P0 内存路径，行为逐字不变。
// `store.enabled=true` 且 `path` 为空 = 配置错误（Validate 报错，绝不静默降级）。
//
// GC（设计 §2.5 保留策略；`GC(now)`）：过期会话（级联 conversations → messages
// → turns）→ 软删超期会话硬删（级联）→ 超期 turns（30d）→ 超期 audit_log
// （30d）→ 超期 usage_counters（90d）。返回逐表删除行数（诊断/启动摘要用）。
//
// 纪律：**零凭据字节**——本层只存索引/摘要（sid 前缀、key_id=sha8、IP/UA 只
// 存 HMAC 形态字段，本波不写值）；审计 detail 不含内容/凭据/cookie/key 原文。
package service

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// 保留策略（设计 §2.5：turns 30d / audit 30d / usage 90d / 软删硬删 7d）
// ---------------------------------------------------------------------------

const (
	// storeTurnsRetention turns 保留期（设计 §2.5 缺省 30d）。
	storeTurnsRetention = 30 * 24 * time.Hour
	// storeAuditRetention audit_log 保留期（设计 §2.5 缺省 30d）。
	storeAuditRetention = 30 * 24 * time.Hour
	// storeUsageRetention usage_counters 保留期（设计 §2.5 缺省 90d）。
	storeUsageRetention = 90 * 24 * time.Hour
	// storeSoftDeleteRetention 软删会话硬删宽限期（设计 §2.5 缺省 7d）。
	storeSoftDeleteRetention = 7 * 24 * time.Hour
	// storeSchemaVersion 当前 schema 版本（schema_meta['schema_version']；
	// 迁移幂等判据——重复 Migrate 不重复建表）。
	storeSchemaVersion = "1"
	// storeSchemaVersionKey schema_meta 版本键。
	storeSchemaVersionKey = "schema_version"
	// storeDayLayout usage_counters.day 形态（UTC 日界，设计 §2.3a）。
	storeDayLayout = "2006-01-02"
)

// storeDay usage 台账日键（UTC；设计 §3.4「重置：UTC 日界」）。
func storeDay(t time.Time) string {
	return t.UTC().Format(storeDayLayout)
}

// gcCutoffs GC 时间切点（now 派生；一次算好传给两档实现）。
type gcCutoffs struct {
	sessionExpiredBefore time.Time // 会话过期切点（= now）
	turnsBefore          time.Time // now - 30d
	auditBefore          time.Time // now - 30d
	usageBeforeDay       string    // now - 90d 的 UTC 日键（day < 该值 → 删）
	softDeletedBefore    time.Time // now - 7d
}

// gcCutoffsAt 由 now 算切点。
func gcCutoffsAt(now time.Time) gcCutoffs {
	return gcCutoffs{
		sessionExpiredBefore: now,
		turnsBefore:          now.Add(-storeTurnsRetention),
		auditBefore:          now.Add(-storeAuditRetention),
		usageBeforeDay:       storeDay(now.Add(-storeUsageRetention)),
		softDeletedBefore:    now.Add(-storeSoftDeleteRetention),
	}
}

// ---------------------------------------------------------------------------
// 记录形态（与设计 §2.3a DDL 逐列同构；时间 = time.Time，落库转 unix ms UTC）
// ---------------------------------------------------------------------------

// storeSession 会话行（§2.3a `sessions`）。KeyID 空 = open 模式会话。
type storeSession struct {
	SID          string
	KeyID        string
	CreatedAt    time.Time
	LastSeenAt   time.Time
	ExpiresAt    time.Time // idle 过期点（滑动 = last_seen + idle TTL）
	AbsExpiresAt time.Time // 绝对过期点
	Revoked      bool
	IPHash       string // HMAC(ip, salt)——本波不写值（登记残余）
	UAHash       string
}

// storeConversation 会话列表行（§2.3a `conversations`）。DeletedAt 零值 = 未软删。
type storeConversation struct {
	ID        string
	SID       string
	Title     string
	Model     string
	Rev       int
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt time.Time
}

// storeMessage 消息行（§2.3a `messages`）。Content = 内容块 JSON（TEXT）。
type storeMessage struct {
	ID        string
	ConvID    string
	Seq       int
	Role      string
	Content   string
	Status    string
	CreatedAt time.Time
}

// storeTurn 请求-应答回合行（§2.3a `turns`）。CredentialIndex = -1 表示无池
// （wire 省略——**只记索引，零凭据字节**）。
type storeTurn struct {
	ID              string
	ConvID          string
	RequestID       string
	AttemptNo       int
	Upstream        string
	CredentialIndex int
	Status          string
	UsageJSON       string
	CreatedAt       time.Time
}

// storeUsage usage_counters 行（§2.3a；scope = 'global' | 'session:<sid>' |
// 'ip:<hash>' | 'key:<sha8>'）。
type storeUsage struct {
	Scope     string
	Day       string
	Requests  int
	CostUnits int
}

// storeAudit audit_log 行（§2.3a；detail = JSON 摘要——不含内容/凭据/cookie/
// key/IP 原文）。
type storeAudit struct {
	ID        int64
	TS        time.Time
	SID       string
	KeyID     string
	IPHash    string
	Kind      string
	RequestID string
	Detail    string
}

// storeGCStats 一次 GC 的逐表删除行数（启动摘要 + 测试断言用）。
type storeGCStats struct {
	Sessions      int
	Conversations int
	Messages      int
	Turns         int
	Usage         int
	Audit         int
}

// total 删除行数合计（日志用）。
func (s storeGCStats) total() int {
	return s.Sessions + s.Conversations + s.Messages + s.Turns + s.Usage + s.Audit
}

// String 一行摘要（启动日志——无敏感面）。
func (s storeGCStats) String() string {
	return fmt.Sprintf("sessions=%d conversations=%d messages=%d turns=%d usage=%d audit=%d",
		s.Sessions, s.Conversations, s.Messages, s.Turns, s.Usage, s.Audit)
}

// ---------------------------------------------------------------------------
// Store 接口（接口冻结：Migrate / GC(now)；见 brief §接口冻结）
// ---------------------------------------------------------------------------

// Store 持久化层（接口冻结）。实现：`memoryStore`（本文件）/ `sqliteStore`
// （store_sqlite.go）。**全部方法并发安全**；错误只在「真存储失败」时返回
// （调用方按「绝不影响请求路径」纪律只记日志 + ring store_error）。
type Store interface {
	// Migrate 建表/升版（幂等：schema 版本已到当前值 → no-op）。
	Migrate() error
	// GC 过期清理（设计 §2.5；now 可注入——测试用）。返回逐表删除行数。
	GC(now time.Time) (storeGCStats, error)
	// Close 关库（停机钩子；幂等）。

	// SessionCreate 插入会话行（已存在 = 覆盖：会话 id 由服务端签发，冲突不可
	// 能；防御性 upsert 保证重启后重放幂等）。
	SessionCreate(rec storeSession) error
	// SessionGet 查会话行（不含过期判定——调用方判定）。
	SessionGet(sid string) (storeSession, bool, error)
	// SessionTouch 刷新 last_seen / idle 过期点（滑动过期落库）。
	SessionTouch(sid string, lastSeen, expiresAt time.Time) error
	// SessionDelete 删会话行（级联 conversations → messages → turns）。返回是否
	// 存在。
	SessionDelete(sid string) (bool, error)
	// Sessions 全部会话行（管理面/诊断/测试；按 created_at 升序）。
	Sessions() ([]storeSession, error)

	// ConversationUpsert 整行 upsert（§2.6 单会话写；rev 由调用方维护）。
	ConversationUpsert(c storeConversation) error
	// ConversationGet 查会话行（含软删——调用方判 DeletedAt）。
	ConversationGet(id string) (storeConversation, bool, error)
	// ConversationSoftDelete 软删（deleted_at = at；updated_at = at；rev+1；
	// 已软删 = no-op——重复删除幂等。GC 7d 后硬删）。
	ConversationSoftDelete(id string, at time.Time) error
	// ConversationsBySession 某会话的列表（含软删；updated_at 降序）。
	ConversationsBySession(sid string) ([]storeConversation, error)
	// ConversationSave 原子保存会话行 +（msgs != nil 时）**替换**消息集合：
	//   - create=true：INSERT（rev=1）；同 id 已存在 → (false, nil)；
	//   - create=false：`UPDATE … WHERE id=? AND sid=? AND rev=expectedRev AND
	//     deleted_at IS NULL`；CAS 失败 → (false, nil)；
	//   - msgs 内的消息 id 已属于**别的会话** → (false, errStoreMessageIDConflict)。
	ConversationSave(c storeConversation, msgs []storeMessage, expectedRev int, create bool) (bool, error)
	// MessageAppendRev 原子追加消息 + 会话 rev+1/updated_at 刷新；CAS 失败 →
	// (false, nil)；消息 id 已存在 → (false, errStoreMessageIDConflict)。
	MessageAppendRev(m storeMessage, expectedRev int) (bool, error)
	// MessageGet 按消息 id 查（E8 幂等面；无行 = ok=false）。
	MessageGet(id string) (storeMessage, bool, error)
	// MessageAppend 追加消息（(conv_id, seq) 唯一——冲突 = 调用方 seq 复用，
	// 报错不静默）。
	MessageAppend(m storeMessage) error
	// MessagesByConversation 某会话的消息（seq 升序）。
	MessagesByConversation(convID string) ([]storeMessage, error)
	// TurnAppend 追加 turn（request_id ↔ attempts 行 join 面）。
	TurnAppend(t storeTurn) error
	// TurnsByRequest 按 request_id 查 turns（对账面；created_at 升序）。
	TurnsByRequest(requestID string) ([]storeTurn, error)

	// UsageAdd 台账累加（requests + units；行不存在则建）。
	UsageAdd(scope, day string, requests, units int) error
	// UsageGet 读台账行（无行 = 零值 + ok=false）。
	UsageGet(scope, day string) (storeUsage, bool, error)
	// UsageScopes 某日全部 scope 行（管理面/测试）。
	UsageScopes(day string) ([]storeUsage, error)

	// AuditAppend 审计追加（ts 零值 = 调用时刻；返回行 id）。
	AuditAppend(e storeAudit) (int64, error)
	// AuditQuery 审计查询（kind 空 = 全部；since 零值 = 不限；limit <= 0 =
	// 缺省 100；按 id 升序）。
	AuditQuery(kind string, since time.Time, limit int) ([]storeAudit, error)
	Close() error
}

// storeAuditDefaultLimit AuditQuery 缺省条数。
const storeAuditDefaultLimit = 100

// errStoreMessageIDConflict 消息 id 已属于**别的会话**（messages.id 全局主键
// 冲突；E4/E8 映射 409 conflict——错误信息不回显对方会话）。
var errStoreMessageIDConflict = errors.New("store: message id belongs to another conversation")

// storeEnabled 持久化开关（缺省 false——硬门：false 时 OpenStore 返回 nil，
// 全链走 P0 内存路径）。nil 安全。
func (c *Config) storeEnabled() bool {
	return c != nil && c.Store != nil && c.Store.Enabled
}

// storePath SQLite 文件路径（trim 后原样；空 = 配置错误——Validate 已拦）。
func (c *Config) storePath() string {
	if c == nil || c.Store == nil {
		return ""
	}
	return strings.TrimSpace(c.Store.Path)
}

// OpenStore 打开持久化层（接口冻结）。
//
//   - `store.enabled=false`（缺省）→ `(nil, nil)`：**不挂载**，全部消费方走
//     P0 内存路径（零行为变化——硬门）。
//   - `store.enabled=true` → 打开 SQLite（path 必填；父目录自动建）→ `Migrate()`
//     → 返回。**任何失败原样返回**（调用方 = 启动报错，绝不静默降级）。
func OpenStore(cfg *Config) (Store, error) {
	if !cfg.storeEnabled() {
		return nil, nil
	}
	path := cfg.storePath()
	if path == "" {
		return nil, errors.New("store: enabled but path is empty")
	}
	return openSQLiteStore(path)
}

// ---------------------------------------------------------------------------
// 内存档（P0 现状的 store 形态；测试与单进程档）
// ---------------------------------------------------------------------------

// memoryStore 进程内 Store 实现（互斥 + map；重启清零）。语义与 sqliteStore
// 对齐（GC 级联/保留策略同源），供 store 未启用档的接线测试与单进程档使用。
type memoryStore struct {
	mu      sync.Mutex
	session map[string]storeSession
	conv    map[string]storeConversation
	msg     map[string][]storeMessage
	turn    map[string][]storeTurn
	usage   map[string]storeUsage // key = scope + "\x00" + day
	audit   []storeAudit
	auditID int64
	closed  bool
}

// newMemoryStore 建内存档（P0 现状形态；测试与单进程档用）。
func newMemoryStore() *memoryStore {
	return &memoryStore{
		session: make(map[string]storeSession),
		conv:    make(map[string]storeConversation),
		msg:     make(map[string][]storeMessage),
		turn:    make(map[string][]storeTurn),
		usage:   make(map[string]storeUsage),
	}
}

// Migrate 内存档无 schema（no-op；幂等语义与 sqlite 档一致）。
func (m *memoryStore) Migrate() error { return nil }

// usageKey usage 行键（scope + day）。
func usageKey(scope, day string) string { return scope + "\x00" + day }

// GC 过期清理（设计 §2.5；语义与 sqlite 档逐条对齐）。
func (m *memoryStore) GC(now time.Time) (storeGCStats, error) {
	c := gcCutoffsAt(now)
	var st storeGCStats
	m.mu.Lock()
	defer m.mu.Unlock()
	// ① 过期会话（级联 conversations → messages → turns）。
	for sid, s := range m.session {
		if now.After(s.AbsExpiresAt) || now.After(s.ExpiresAt) {
			delete(m.session, sid)
			st.Sessions++
			for id, cv := range m.conv {
				if cv.SID != sid {
					continue
				}
				st.Messages += len(m.msg[id])
				st.Turns += len(m.turn[id])
				delete(m.msg, id)
				delete(m.turn, id)
				delete(m.conv, id)
				st.Conversations++
			}
		}
	}
	// ② 软删超期硬删（级联 messages/turns）。
	for id, cv := range m.conv {
		if cv.DeletedAt.IsZero() || !cv.DeletedAt.Before(c.softDeletedBefore) {
			continue
		}
		st.Messages += len(m.msg[id])
		st.Turns += len(m.turn[id])
		delete(m.msg, id)
		delete(m.turn, id)
		delete(m.conv, id)
		st.Conversations++
	}
	// ③ 超期 turns（级联已删的按 conv 键连带清空——此处只清剩余）。
	for id, turns := range m.turn {
		keep := turns[:0]
		for _, t := range turns {
			if t.CreatedAt.Before(c.turnsBefore) {
				st.Turns++
				continue
			}
			keep = append(keep, t)
		}
		if len(keep) == 0 {
			delete(m.turn, id)
			continue
		}
		m.turn[id] = keep
	}
	// ④ 超期 audit。
	keepAudit := m.audit[:0]
	for _, e := range m.audit {
		if e.TS.Before(c.auditBefore) {
			st.Audit++
			continue
		}
		keepAudit = append(keepAudit, e)
	}
	m.audit = keepAudit
	// ⑤ 超期 usage（day < now-90d 的 UTC 日键）。
	for k, u := range m.usage {
		if u.Day < c.usageBeforeDay {
			delete(m.usage, k)
			st.Usage++
		}
	}
	return st, nil
}

// Close 关内存档（幂等；后续调用报错）。
func (m *memoryStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// SessionCreate 插入/覆盖会话行。
func (m *memoryStore) SessionCreate(rec storeSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("store: memory store closed")
	}
	m.session[rec.SID] = rec
	return nil
}

// SessionGet 查会话行。
func (m *memoryStore) SessionGet(sid string) (storeSession, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.session[sid]
	return rec, ok, nil
}

// SessionTouch 刷新 last_seen / idle 过期点。
func (m *memoryStore) SessionTouch(sid string, lastSeen, expiresAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.session[sid]
	if !ok {
		return nil // 已被 GC/logout 删：静默（会话面按无会话重新签发）。
	}
	rec.LastSeenAt, rec.ExpiresAt = lastSeen, expiresAt
	m.session[sid] = rec
	return nil
}

// SessionDelete 删会话行（级联 conversations/messages/turns）。
func (m *memoryStore) SessionDelete(sid string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.session[sid]; !ok {
		return false, nil
	}
	delete(m.session, sid)
	for id, cv := range m.conv {
		if cv.SID != sid {
			continue
		}
		delete(m.msg, id)
		delete(m.turn, id)
		delete(m.conv, id)
	}
	return true, nil
}

// Sessions 全部会话行（created_at 升序）。
func (m *memoryStore) Sessions() ([]storeSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]storeSession, 0, len(m.session))
	for _, rec := range m.session {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].SID < out[j].SID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

// ConversationUpsert 整行 upsert。
func (m *memoryStore) ConversationUpsert(c storeConversation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conv[c.ID] = c
	return nil
}

// ConversationGet 查会话行。
func (m *memoryStore) ConversationGet(id string) (storeConversation, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.conv[id]
	return c, ok, nil
}

// ConversationSoftDelete 软删（P1-3 行为修正：bump updated_at + rev——否则
// E1 的 since 增量拉不到墓碑；已软删 = no-op，重复删除幂等）。
func (m *memoryStore) ConversationSoftDelete(id string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.conv[id]
	if !ok || !c.DeletedAt.IsZero() {
		return nil
	}
	c.DeletedAt = at
	c.UpdatedAt = at
	c.Rev++
	m.conv[id] = c
	return nil
}

// messageIDOwnerLocked 消息 id 当前归属的会话（无 → ""；调用方持锁）。
func (m *memoryStore) messageIDOwnerLocked(id string) string {
	for convID, list := range m.msg {
		for _, e := range list {
			if e.ID == id {
				return convID
			}
		}
	}
	return ""
}

// ConversationSave 原子保存会话行 + 消息集合替换（契约见 Store 接口）。
func (m *memoryStore) ConversationSave(c storeConversation, msgs []storeMessage, expectedRev int, create bool) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false, errors.New("store: memory store closed")
	}
	existing, ok := m.conv[c.ID]
	if create {
		if ok {
			return false, nil
		}
		if c.Rev <= 0 {
			c.Rev = 1
		}
		if c.CreatedAt.IsZero() {
			c.CreatedAt = time.Now()
		}
		if c.UpdatedAt.IsZero() {
			c.UpdatedAt = c.CreatedAt
		}
		m.conv[c.ID] = c
		if msgs != nil {
			m.msg[c.ID] = append([]storeMessage(nil), msgs...)
		}
		return true, nil
	}
	if !ok || existing.Rev != expectedRev || existing.SID != c.SID || !existing.DeletedAt.IsZero() {
		return false, nil
	}
	if msgs != nil {
		for _, msg := range msgs {
			if owner := m.messageIDOwnerLocked(msg.ID); owner != "" && owner != c.ID {
				return false, errStoreMessageIDConflict
			}
		}
	}
	c.Rev = expectedRev + 1
	if c.CreatedAt.IsZero() {
		c.CreatedAt = existing.CreatedAt
	}
	if c.UpdatedAt.IsZero() {
		c.UpdatedAt = time.Now()
	}
	m.conv[c.ID] = c
	if msgs != nil {
		m.msg[c.ID] = append([]storeMessage(nil), msgs...)
	}
	return true, nil
}

// MessageAppendRev 原子追加消息 + 会话 rev+1/updated_at 刷新（契约见 Store 接口）。
func (m *memoryStore) MessageAppendRev(msg storeMessage, expectedRev int) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false, errors.New("store: memory store closed")
	}
	c, ok := m.conv[msg.ConvID]
	if !ok || c.Rev != expectedRev || !c.DeletedAt.IsZero() {
		return false, nil
	}
	if owner := m.messageIDOwnerLocked(msg.ID); owner != "" {
		return false, errStoreMessageIDConflict
	}
	if msg.Status == "" {
		msg.Status = "done"
	}
	m.msg[msg.ConvID] = append(m.msg[msg.ConvID], msg)
	c.Rev = expectedRev + 1
	now := time.Now()
	if !now.After(c.UpdatedAt) {
		now = c.UpdatedAt.Add(time.Millisecond)
	}
	c.UpdatedAt = now
	m.conv[c.ID] = c
	return true, nil
}

// MessageGet 按消息 id 查（契约见 Store 接口）。
func (m *memoryStore) MessageGet(id string) (storeMessage, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, list := range m.msg {
		for _, e := range list {
			if e.ID == id {
				return e, true, nil
			}
		}
	}
	return storeMessage{}, false, nil
}

// ConversationsBySession 某会话的列表（updated_at 降序）。
func (m *memoryStore) ConversationsBySession(sid string) ([]storeConversation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]storeConversation, 0)
	for _, c := range m.conv {
		if c.SID == sid {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out, nil
}

// MessageAppend 追加消息（(conv_id, seq) 唯一——重复 seq 报错）。
func (m *memoryStore) MessageAppend(msg storeMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.msg[msg.ConvID] {
		if e.Seq == msg.Seq {
			return fmt.Errorf("store: message seq %d already used in conversation %s", msg.Seq, msg.ConvID)
		}
	}
	m.msg[msg.ConvID] = append(m.msg[msg.ConvID], msg)
	return nil
}

// MessagesByConversation 某会话的消息（seq 升序）。
func (m *memoryStore) MessagesByConversation(convID string) ([]storeMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := append([]storeMessage(nil), m.msg[convID]...)
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

// TurnAppend 追加 turn。
func (m *memoryStore) TurnAppend(t storeTurn) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.turn[t.ConvID] = append(m.turn[t.ConvID], t)
	return nil
}

// TurnsByRequest 按 request_id 查 turns（created_at 升序）。
func (m *memoryStore) TurnsByRequest(requestID string) ([]storeTurn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]storeTurn, 0)
	for _, turns := range m.turn {
		for _, t := range turns {
			if t.RequestID == requestID {
				out = append(out, t)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

// UsageAdd 台账累加。
func (m *memoryStore) UsageAdd(scope, day string, requests, units int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := usageKey(scope, day)
	u := m.usage[k]
	u.Scope, u.Day = scope, day
	u.Requests += requests
	u.CostUnits += units
	m.usage[k] = u
	return nil
}

// UsageGet 读台账行。
func (m *memoryStore) UsageGet(scope, day string) (storeUsage, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.usage[usageKey(scope, day)]
	return u, ok, nil
}

// UsageScopes 某日全部 scope 行（scope 升序）。
func (m *memoryStore) UsageScopes(day string) ([]storeUsage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]storeUsage, 0)
	for _, u := range m.usage {
		if u.Day == day {
			out = append(out, u)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Scope < out[j].Scope })
	return out, nil
}

// AuditAppend 审计追加。
func (m *memoryStore) AuditAppend(e storeAudit) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.auditID++
	e.ID = m.auditID
	if e.TS.IsZero() {
		e.TS = time.Now()
	}
	m.audit = append(m.audit, e)
	return e.ID, nil
}

// AuditQuery 审计查询（id 升序；kind/since 过滤；limit 截尾）。
func (m *memoryStore) AuditQuery(kind string, since time.Time, limit int) ([]storeAudit, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 {
		limit = storeAuditDefaultLimit
	}
	out := make([]storeAudit, 0)
	for _, e := range m.audit {
		if kind != "" && e.Kind != kind {
			continue
		}
		if !since.IsZero() && e.TS.Before(since) {
			continue
		}
		out = append(out, e)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}
