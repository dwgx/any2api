// store_sqlite.go SQLite 持久化实现（P1-1；设计 `claude2api-public-service`
// §2.2/§2.3a；board A2A-L19-W59-1）。
//
// **依赖引入点**：`modernc.org/sqlite`（纯 Go、无 cgo——设计 §2.2 推荐；U3 实测
// 见报告 §U3）。驱动名 `sqlite`；DSN = 文件路径（不拼 `_pragma=` 查询串——Windows
// 盘符路径在 file: URI 下有歧义，改用「单连接 + 显式 PRAGMA」，见 openSQLiteStore）。
//
// 形态：
//
//   - DDL = 设计 §2.3a **逐列同构**（六数据表 sessions/conversations/messages/
//     turns/usage_counters/audit_log + 迁移锚表 schema_meta + 索引）；`PRAGMA
//     journal_mode=WAL` / `foreign_keys=ON` / `busy_timeout=5000`。
//   - 迁移幂等：`schema_meta['schema_version']` 已是当前版本 → `Migrate()` 直接
//     返回（不重复建表；重复打开同一文件同理）。
//   - 级联：`conversations.sid REFERENCES sessions(sid) ON DELETE CASCADE`、
//     `messages/turns.conv_id REFERENCES conversations(id) ON DELETE CASCADE`
//     ——GC 删会话即带删列表/消息/回合（foreign_keys=ON 是前提，已实测断言）。
//   - 并发：`SetMaxOpenConns(1)` + `SetMaxIdleConns(1)`（单写者模型——薄壳流量
//     下 SQLITE_BUSY 面直接消掉；PRAGMA 只作用于该连接，连接不回收）。WAL 仍
//     提供崩溃安全（设计 §2.2 的核心诉求）。
//
// 纪律：零凭据字节（只存索引/摘要）；错误原样返回给 Store 消费方（调用方只记
// 日志 + ring store_error，绝不影响请求路径）。
package service

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite" // 纯 Go 驱动（无 cgo；U3 实测面）
)

// storeSQLiteDriver modernc 驱动名。
const storeSQLiteDriver = "sqlite"

// storeSQLitePragmas 打开即设的连接级 PRAGMA（顺序即语义：WAL 先落）。
var storeSQLitePragmas = []string{
	"PRAGMA journal_mode=WAL",
	"PRAGMA foreign_keys=ON",
	"PRAGMA busy_timeout=5000",
}

// storeSQLiteDDL schema v1 全部语句（设计 §2.3a 逐列同构；`IF NOT EXISTS` 使
// 迁移对「同版本重放」天然幂等——版本表仍作权威判据）。
var storeSQLiteDDL = []string{
	`CREATE TABLE IF NOT EXISTS schema_meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
)`,
	`CREATE TABLE IF NOT EXISTS sessions (
  sid            TEXT PRIMARY KEY,
  key_id         TEXT,
  created_at     INTEGER NOT NULL,
  last_seen_at   INTEGER NOT NULL,
  expires_at     INTEGER NOT NULL,
  abs_expires_at INTEGER NOT NULL,
  revoked        INTEGER NOT NULL DEFAULT 0,
  ip_hash        TEXT,
  ua_hash        TEXT
)`,
	`CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at)`,
	`CREATE INDEX IF NOT EXISTS idx_sessions_key ON sessions(key_id)`,
	`CREATE TABLE IF NOT EXISTS conversations (
  id         TEXT PRIMARY KEY,
  sid        TEXT NOT NULL REFERENCES sessions(sid) ON DELETE CASCADE,
  title      TEXT NOT NULL DEFAULT '',
  model      TEXT,
  rev        INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  deleted_at INTEGER
)`,
	`CREATE INDEX IF NOT EXISTS idx_conv_sid ON conversations(sid, updated_at DESC)`,
	`CREATE TABLE IF NOT EXISTS messages (
  id         TEXT PRIMARY KEY,
  conv_id    TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  seq        INTEGER NOT NULL,
  role       TEXT NOT NULL,
  content    TEXT NOT NULL,
  status     TEXT NOT NULL DEFAULT 'done',
  created_at INTEGER NOT NULL,
  UNIQUE(conv_id, seq)
)`,
	`CREATE TABLE IF NOT EXISTS turns (
  id               TEXT PRIMARY KEY,
  conv_id          TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  request_id       TEXT NOT NULL,
  attempt_no       INTEGER NOT NULL,
  upstream         TEXT,
  credential_index INTEGER,
  status           TEXT NOT NULL,
  usage_json       TEXT,
  created_at       INTEGER NOT NULL
)`,
	`CREATE INDEX IF NOT EXISTS idx_turns_req ON turns(request_id)`,
	`CREATE TABLE IF NOT EXISTS usage_counters (
  scope      TEXT NOT NULL,
  day        TEXT NOT NULL,
  requests   INTEGER NOT NULL DEFAULT 0,
  cost_units INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (scope, day)
)`,
	`CREATE TABLE IF NOT EXISTS audit_log (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  ts         INTEGER NOT NULL,
  sid        TEXT,
  key_id     TEXT,
  ip_hash    TEXT,
  kind       TEXT NOT NULL,
  request_id TEXT,
  detail     TEXT
)`,
	`CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts)`,
}

// sqliteStore SQLite 档 Store 实现。
type sqliteStore struct {
	db   *sql.DB
	path string
}

// openSQLiteStore 打开（并迁移）SQLite 档：建父目录 → open → 单连接 + PRAGMA
// → Migrate。任何失败原样返回（调用方 = 启动报错）。
func openSQLiteStore(path string) (Store, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("store: create directory %s: %w", dir, err)
		}
	}
	db, err := sql.Open(storeSQLiteDriver, path)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	// 单写者模型：一个连接（PRAGMA 连接级生效；WAL 仍保崩溃安全）。
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	for _, p := range storeSQLitePragmas {
		if _, err := db.Exec(p); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("store: %s: %w", p, err)
		}
	}
	st := &sqliteStore{db: db, path: path}
	if err := st.Migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return st, nil
}

// Path 库文件路径（诊断/日志用）。
func (s *sqliteStore) Path() string { return s.path }

// Migrate 建表/升版（幂等：schema_meta 版本已是当前值 → no-op）。
func (s *sqliteStore) Migrate() error {
	// 迁移锚表自身先建（无版本概念——`IF NOT EXISTS` 幂等）。
	if _, err := s.db.Exec(storeSQLiteDDL[0]); err != nil {
		return fmt.Errorf("store: create schema_meta: %w", err)
	}
	var cur string
	err := s.db.QueryRow(`SELECT value FROM schema_meta WHERE key = ?`, storeSchemaVersionKey).Scan(&cur)
	switch {
	case err == nil && cur == storeSchemaVersion:
		return nil // 已到当前版本：不重复迁移（幂等）
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("store: read schema version: %w", err)
	}
	// 建表 + 写版本（同一事务：失败不留半套 schema）。
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("store: begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, stmt := range storeSQLiteDDL[1:] {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("store: migrate: %w", err)
		}
	}
	if _, err := tx.Exec(
		`INSERT INTO schema_meta(key, value) VALUES(?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		storeSchemaVersionKey, storeSchemaVersion); err != nil {
		return fmt.Errorf("store: write schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit migration: %w", err)
	}
	return nil
}

// Close 关库（幂等）。
func (s *sqliteStore) Close() error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// msOf unix ms UTC（零值 → 0 = 「未设」，落 NULL 语义由调用点决定）。
func msOf(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixMilli()
}

// timeOfMs unix ms → time（0 → 零值）。
func timeOfMs(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// nullText 空串 → NULL（可选列落 NULL，读回 COALESCE 成空串）。
func nullText(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// ---------------------------------------------------------------------------
// sessions
// ---------------------------------------------------------------------------

// SessionCreate 插入/覆盖会话行。
func (s *sqliteStore) SessionCreate(rec storeSession) error {
	_, err := s.db.Exec(
		`INSERT INTO sessions(sid, key_id, created_at, last_seen_at, expires_at, abs_expires_at, revoked, ip_hash, ua_hash)
		 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(sid) DO UPDATE SET
		   key_id = excluded.key_id, created_at = excluded.created_at,
		   last_seen_at = excluded.last_seen_at, expires_at = excluded.expires_at,
		   abs_expires_at = excluded.abs_expires_at, revoked = excluded.revoked,
		   ip_hash = excluded.ip_hash, ua_hash = excluded.ua_hash`,
		rec.SID, nullText(rec.KeyID), msOf(rec.CreatedAt), msOf(rec.LastSeenAt),
		msOf(rec.ExpiresAt), msOf(rec.AbsExpiresAt), boolToInt(rec.Revoked),
		nullText(rec.IPHash), nullText(rec.UAHash))
	if err != nil {
		return fmt.Errorf("store: session create: %w", err)
	}
	return nil
}

// SessionGet 查会话行。
func (s *sqliteStore) SessionGet(sid string) (storeSession, bool, error) {
	row := s.db.QueryRow(
		`SELECT sid, COALESCE(key_id, ''), created_at, last_seen_at, expires_at, abs_expires_at,
		        revoked, COALESCE(ip_hash, ''), COALESCE(ua_hash, '')
		   FROM sessions WHERE sid = ?`, sid)
	rec, err := scanSession(row)
	if errors.Is(err, sql.ErrNoRows) {
		return storeSession{}, false, nil
	}
	if err != nil {
		return storeSession{}, false, fmt.Errorf("store: session get: %w", err)
	}
	return rec, true, nil
}

// SessionTouch 刷新 last_seen / idle 过期点（不存在 = 静默——会话面按无会话重
// 新签发）。
func (s *sqliteStore) SessionTouch(sid string, lastSeen, expiresAt time.Time) error {
	if _, err := s.db.Exec(
		`UPDATE sessions SET last_seen_at = ?, expires_at = ? WHERE sid = ?`,
		msOf(lastSeen), msOf(expiresAt), sid); err != nil {
		return fmt.Errorf("store: session touch: %w", err)
	}
	return nil
}

// SessionDelete 删会话行（级联 conversations → messages → turns）。
func (s *sqliteStore) SessionDelete(sid string) (bool, error) {
	res, err := s.db.Exec(`DELETE FROM sessions WHERE sid = ?`, sid)
	if err != nil {
		return false, fmt.Errorf("store: session delete: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, nil // 驱动不支持 RowsAffected：删除已执行，存在性未知
	}
	return n > 0, nil
}

// Sessions 全部会话行（created_at 升序）。
func (s *sqliteStore) Sessions() ([]storeSession, error) {
	rows, err := s.db.Query(
		`SELECT sid, COALESCE(key_id, ''), created_at, last_seen_at, expires_at, abs_expires_at,
		        revoked, COALESCE(ip_hash, ''), COALESCE(ua_hash, '')
		   FROM sessions ORDER BY created_at ASC, sid ASC`)
	if err != nil {
		return nil, fmt.Errorf("store: sessions: %w", err)
	}
	defer rows.Close()
	out := make([]storeSession, 0)
	for rows.Next() {
		rec, err := scanSession(rows)
		if err != nil {
			return nil, fmt.Errorf("store: sessions: %w", err)
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// rowScanner *sql.Row / *sql.Rows 的共同面。
type rowScanner interface{ Scan(dest ...any) error }

// scanSession 扫一行 sessions（列序见调用点）。
func scanSession(r rowScanner) (storeSession, error) {
	var rec storeSession
	var created, lastSeen, expires, absExpires int64
	var revoked int
	if err := r.Scan(&rec.SID, &rec.KeyID, &created, &lastSeen, &expires, &absExpires,
		&revoked, &rec.IPHash, &rec.UAHash); err != nil {
		return storeSession{}, err
	}
	rec.CreatedAt, rec.LastSeenAt = timeOfMs(created), timeOfMs(lastSeen)
	rec.ExpiresAt, rec.AbsExpiresAt = timeOfMs(expires), timeOfMs(absExpires)
	rec.Revoked = revoked != 0
	return rec, nil
}

// boolToInt bool → 0/1（SQLite 无 bool 类型）。
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// ---------------------------------------------------------------------------
// conversations / messages / turns
// ---------------------------------------------------------------------------

// ConversationUpsert 整行 upsert。
func (s *sqliteStore) ConversationUpsert(c storeConversation) error {
	rev := c.Rev
	if rev <= 0 {
		rev = 1 // DDL 缺省（调用方未给 rev 时的防御）
	}
	_, err := s.db.Exec(
		`INSERT INTO conversations(id, sid, title, model, rev, created_at, updated_at, deleted_at)
		 VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   sid = excluded.sid, title = excluded.title, model = excluded.model, rev = excluded.rev,
		   created_at = excluded.created_at, updated_at = excluded.updated_at, deleted_at = excluded.deleted_at`,
		c.ID, c.SID, c.Title, nullText(c.Model), rev, msOf(c.CreatedAt), msOf(c.UpdatedAt), nullTimeMs(c.DeletedAt))
	if err != nil {
		return fmt.Errorf("store: conversation upsert: %w", err)
	}
	return nil
}

// nullTimeMs 零值时间 → NULL。
func nullTimeMs(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().UnixMilli()
}

// ConversationGet 查会话行（含软删）。
func (s *sqliteStore) ConversationGet(id string) (storeConversation, bool, error) {
	row := s.db.QueryRow(
		`SELECT id, sid, COALESCE(title, ''), COALESCE(model, ''), rev, created_at, updated_at, deleted_at
		   FROM conversations WHERE id = ?`, id)
	c, err := scanConversation(row)
	if errors.Is(err, sql.ErrNoRows) {
		return storeConversation{}, false, nil
	}
	if err != nil {
		return storeConversation{}, false, fmt.Errorf("store: conversation get: %w", err)
	}
	return c, true, nil
}

// ConversationSoftDelete 软删（P1-3 行为修正：bump updated_at + rev——否则
// E1 的 since 增量拉不到墓碑；`deleted_at IS NULL` 保证重复删除幂等）。
func (s *sqliteStore) ConversationSoftDelete(id string, at time.Time) error {
	if _, err := s.db.Exec(
		`UPDATE conversations SET deleted_at = ?, updated_at = ?, rev = rev + 1
		  WHERE id = ? AND deleted_at IS NULL`, msOf(at), msOf(at), id); err != nil {
		return fmt.Errorf("store: conversation soft delete: %w", err)
	}
	return nil
}

// ConversationSave 原子保存会话行 + 消息集合替换（契约见 Store 接口）。
// 单事务：CAS 更新/插入 +（msgs != nil）消息 id 归属检查 + 全量替换。
func (s *sqliteStore) ConversationSave(c storeConversation, msgs []storeMessage, expectedRev int, create bool) (bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, fmt.Errorf("store: conversation save begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rev := c.Rev
	if rev <= 0 {
		rev = 1
	}
	if create {
		res, err := tx.Exec(
			`INSERT INTO conversations(id, sid, title, model, rev, created_at, updated_at, deleted_at)
			 VALUES(?, ?, ?, ?, ?, ?, ?, NULL)
			 ON CONFLICT(id) DO NOTHING`,
			c.ID, c.SID, c.Title, nullText(c.Model), rev, msOf(c.CreatedAt), msOf(c.UpdatedAt))
		if err != nil {
			return false, fmt.Errorf("store: conversation save insert: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return false, fmt.Errorf("store: conversation save insert rows: %w", err)
		}
		if n == 0 {
			return false, nil // 同 id 已存在（幂等面由调用方判定）
		}
	} else {
		res, err := tx.Exec(
			`UPDATE conversations SET title = ?, model = ?, rev = rev + 1, updated_at = ?
			  WHERE id = ? AND sid = ? AND rev = ? AND deleted_at IS NULL`,
			c.Title, nullText(c.Model), msOf(c.UpdatedAt), c.ID, c.SID, expectedRev)
		if err != nil {
			return false, fmt.Errorf("store: conversation save update: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return false, fmt.Errorf("store: conversation save update rows: %w", err)
		}
		if n == 0 {
			return false, nil // CAS 失败（rev 不匹配 / 已软删 / 非本会话）
		}
	}
	if msgs != nil {
		for _, msg := range msgs {
			var owner string
			err := tx.QueryRow(`SELECT conv_id FROM messages WHERE id = ?`, msg.ID).Scan(&owner)
			switch {
			case err == nil && owner != c.ID:
				return false, errStoreMessageIDConflict
			case err != nil && !errors.Is(err, sql.ErrNoRows):
				return false, fmt.Errorf("store: conversation save message check: %w", err)
			}
		}
		if _, err := tx.Exec(`DELETE FROM messages WHERE conv_id = ?`, c.ID); err != nil {
			return false, fmt.Errorf("store: conversation save clear messages: %w", err)
		}
		for _, msg := range msgs {
			status := msg.Status
			if status == "" {
				status = "done"
			}
			if _, err := tx.Exec(
				`INSERT INTO messages(id, conv_id, seq, role, content, status, created_at)
				 VALUES(?, ?, ?, ?, ?, ?, ?)`,
				msg.ID, msg.ConvID, msg.Seq, msg.Role, msg.Content, status, msOf(msg.CreatedAt)); err != nil {
				return false, fmt.Errorf("store: conversation save message insert: %w", err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("store: conversation save commit: %w", err)
	}
	return true, nil
}

// MessageAppendRev 原子追加消息 + 会话 rev+1/updated_at 刷新（契约见 Store 接口）。
func (s *sqliteStore) MessageAppendRev(m storeMessage, expectedRev int) (bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, fmt.Errorf("store: message append begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var owner string
	err = tx.QueryRow(`SELECT conv_id FROM messages WHERE id = ?`, m.ID).Scan(&owner)
	if err == nil {
		return false, errStoreMessageIDConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("store: message append id check: %w", err)
	}
	var curUpdated int64
	err = tx.QueryRow(
		`SELECT updated_at FROM conversations WHERE id = ? AND rev = ? AND deleted_at IS NULL`,
		m.ConvID, expectedRev).Scan(&curUpdated)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil // CAS 失败（会话不存在 / rev 不匹配 / 已软删）
	}
	if err != nil {
		return false, fmt.Errorf("store: message append conversation check: %w", err)
	}
	updated := time.Now().UTC()
	if !updated.After(timeOfMs(curUpdated)) {
		updated = timeOfMs(curUpdated).Add(time.Millisecond)
	}
	res, err := tx.Exec(
		`UPDATE conversations SET rev = rev + 1, updated_at = ?
		  WHERE id = ? AND rev = ? AND deleted_at IS NULL`,
		msOf(updated), m.ConvID, expectedRev)
	if err != nil {
		return false, fmt.Errorf("store: message append rev bump: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: message append rev bump rows: %w", err)
	}
	if n == 0 {
		return false, nil
	}
	status := m.Status
	if status == "" {
		status = "done"
	}
	if _, err := tx.Exec(
		`INSERT INTO messages(id, conv_id, seq, role, content, status, created_at)
		 VALUES(?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.ConvID, m.Seq, m.Role, m.Content, status, msOf(m.CreatedAt)); err != nil {
		return false, fmt.Errorf("store: message append insert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("store: message append commit: %w", err)
	}
	return true, nil
}

// MessageGet 按消息 id 查（契约见 Store 接口）。
func (s *sqliteStore) MessageGet(id string) (storeMessage, bool, error) {
	var m storeMessage
	var created int64
	err := s.db.QueryRow(
		`SELECT id, conv_id, seq, role, content, status, created_at FROM messages WHERE id = ?`, id).
		Scan(&m.ID, &m.ConvID, &m.Seq, &m.Role, &m.Content, &m.Status, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return storeMessage{}, false, nil
	}
	if err != nil {
		return storeMessage{}, false, fmt.Errorf("store: message get: %w", err)
	}
	m.CreatedAt = timeOfMs(created)
	return m, true, nil
}

// ConversationsBySession 某会话的列表（updated_at 降序）。
func (s *sqliteStore) ConversationsBySession(sid string) ([]storeConversation, error) {
	rows, err := s.db.Query(
		`SELECT id, sid, COALESCE(title, ''), COALESCE(model, ''), rev, created_at, updated_at, deleted_at
		   FROM conversations WHERE sid = ? ORDER BY updated_at DESC, id ASC`, sid)
	if err != nil {
		return nil, fmt.Errorf("store: conversations: %w", err)
	}
	defer rows.Close()
	out := make([]storeConversation, 0)
	for rows.Next() {
		c, err := scanConversation(rows)
		if err != nil {
			return nil, fmt.Errorf("store: conversations: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// scanConversation 扫一行 conversations（列序见调用点）。
func scanConversation(r rowScanner) (storeConversation, error) {
	var c storeConversation
	var created, updated int64
	var deleted sql.NullInt64
	if err := r.Scan(&c.ID, &c.SID, &c.Title, &c.Model, &c.Rev, &created, &updated, &deleted); err != nil {
		return storeConversation{}, err
	}
	c.CreatedAt, c.UpdatedAt = timeOfMs(created), timeOfMs(updated)
	if deleted.Valid {
		c.DeletedAt = timeOfMs(deleted.Int64)
	}
	return c, nil
}

// MessageAppend 追加消息（(conv_id, seq) 唯一——重复 seq 报错）。
func (s *sqliteStore) MessageAppend(m storeMessage) error {
	status := m.Status
	if status == "" {
		status = "done" // DDL 缺省
	}
	if _, err := s.db.Exec(
		`INSERT INTO messages(id, conv_id, seq, role, content, status, created_at)
		 VALUES(?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.ConvID, m.Seq, m.Role, m.Content, status, msOf(m.CreatedAt)); err != nil {
		return fmt.Errorf("store: message append: %w", err)
	}
	return nil
}

// MessagesByConversation 某会话的消息（seq 升序）。
func (s *sqliteStore) MessagesByConversation(convID string) ([]storeMessage, error) {
	rows, err := s.db.Query(
		`SELECT id, conv_id, seq, role, content, status, created_at
		   FROM messages WHERE conv_id = ? ORDER BY seq ASC`, convID)
	if err != nil {
		return nil, fmt.Errorf("store: messages: %w", err)
	}
	defer rows.Close()
	out := make([]storeMessage, 0)
	for rows.Next() {
		var m storeMessage
		var created int64
		if err := rows.Scan(&m.ID, &m.ConvID, &m.Seq, &m.Role, &m.Content, &m.Status, &created); err != nil {
			return nil, fmt.Errorf("store: messages: %w", err)
		}
		m.CreatedAt = timeOfMs(created)
		out = append(out, m)
	}
	return out, rows.Err()
}

// TurnAppend 追加 turn。
func (s *sqliteStore) TurnAppend(t storeTurn) error {
	if _, err := s.db.Exec(
		`INSERT INTO turns(id, conv_id, request_id, attempt_no, upstream, credential_index, status, usage_json, created_at)
		 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.ID, t.ConvID, t.RequestID, t.AttemptNo, nullText(t.Upstream),
		credentialIndexValue(t.CredentialIndex), t.Status, nullText(t.UsageJSON),
		msOf(t.CreatedAt)); err != nil {
		return fmt.Errorf("store: turn append: %w", err)
	}
	return nil
}

// credentialIndexValue -1（无池）→ NULL（**绝不伪造 0**；与 attempts wire 同纪律）。
func credentialIndexValue(idx int) any {
	if idx < 0 {
		return nil
	}
	return idx
}

// TurnsByRequest 按 request_id 查 turns（created_at 升序）。
func (s *sqliteStore) TurnsByRequest(requestID string) ([]storeTurn, error) {
	rows, err := s.db.Query(
		`SELECT id, conv_id, request_id, attempt_no, COALESCE(upstream, ''), credential_index,
		        status, COALESCE(usage_json, ''), created_at
		   FROM turns WHERE request_id = ? ORDER BY created_at ASC, id ASC`, requestID)
	if err != nil {
		return nil, fmt.Errorf("store: turns: %w", err)
	}
	defer rows.Close()
	out := make([]storeTurn, 0)
	for rows.Next() {
		var t storeTurn
		var credIdx sql.NullInt64
		var created int64
		if err := rows.Scan(&t.ID, &t.ConvID, &t.RequestID, &t.AttemptNo, &t.Upstream, &credIdx,
			&t.Status, &t.UsageJSON, &created); err != nil {
			return nil, fmt.Errorf("store: turns: %w", err)
		}
		t.CredentialIndex = -1
		if credIdx.Valid {
			t.CredentialIndex = int(credIdx.Int64)
		}
		t.CreatedAt = timeOfMs(created)
		out = append(out, t)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// usage_counters / audit_log
// ---------------------------------------------------------------------------

// UsageAdd 台账累加（UPSERT：行不存在则建）。
func (s *sqliteStore) UsageAdd(scope, day string, requests, units int) error {
	if _, err := s.db.Exec(
		`INSERT INTO usage_counters(scope, day, requests, cost_units) VALUES(?, ?, ?, ?)
		 ON CONFLICT(scope, day) DO UPDATE SET
		   requests = requests + excluded.requests,
		   cost_units = cost_units + excluded.cost_units`,
		scope, day, requests, units); err != nil {
		return fmt.Errorf("store: usage add: %w", err)
	}
	return nil
}

// UsageGet 读台账行。
func (s *sqliteStore) UsageGet(scope, day string) (storeUsage, bool, error) {
	var u storeUsage
	err := s.db.QueryRow(
		`SELECT scope, day, requests, cost_units FROM usage_counters WHERE scope = ? AND day = ?`,
		scope, day).Scan(&u.Scope, &u.Day, &u.Requests, &u.CostUnits)
	if errors.Is(err, sql.ErrNoRows) {
		return storeUsage{}, false, nil
	}
	if err != nil {
		return storeUsage{}, false, fmt.Errorf("store: usage get: %w", err)
	}
	return u, true, nil
}

// UsageScopes 某日全部 scope 行（scope 升序）。
func (s *sqliteStore) UsageScopes(day string) ([]storeUsage, error) {
	rows, err := s.db.Query(
		`SELECT scope, day, requests, cost_units FROM usage_counters WHERE day = ? ORDER BY scope ASC`, day)
	if err != nil {
		return nil, fmt.Errorf("store: usage scopes: %w", err)
	}
	defer rows.Close()
	out := make([]storeUsage, 0)
	for rows.Next() {
		var u storeUsage
		if err := rows.Scan(&u.Scope, &u.Day, &u.Requests, &u.CostUnits); err != nil {
			return nil, fmt.Errorf("store: usage scopes: %w", err)
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// AuditAppend 审计追加（ts 零值 = 调用时刻；返回行 id）。
func (s *sqliteStore) AuditAppend(e storeAudit) (int64, error) {
	ts := e.TS
	if ts.IsZero() {
		ts = time.Now()
	}
	res, err := s.db.Exec(
		`INSERT INTO audit_log(ts, sid, key_id, ip_hash, kind, request_id, detail)
		 VALUES(?, ?, ?, ?, ?, ?, ?)`,
		msOf(ts), nullText(e.SID), nullText(e.KeyID), nullText(e.IPHash),
		e.Kind, nullText(e.RequestID), nullText(e.Detail))
	if err != nil {
		return 0, fmt.Errorf("store: audit append: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, nil // 驱动不支持 LastInsertId：行已写入（id 未知）
	}
	return id, nil
}

// AuditQuery 审计查询（id 升序；kind/since 过滤；limit <= 0 = 缺省 100）。
func (s *sqliteStore) AuditQuery(kind string, since time.Time, limit int) ([]storeAudit, error) {
	if limit <= 0 {
		limit = storeAuditDefaultLimit
	}
	args := make([]any, 0, 3)
	q := `SELECT id, ts, COALESCE(sid, ''), COALESCE(key_id, ''), COALESCE(ip_hash, ''),
	             kind, COALESCE(request_id, ''), COALESCE(detail, '')
	        FROM audit_log WHERE 1 = 1`
	if kind != "" {
		q += ` AND kind = ?`
		args = append(args, kind)
	}
	if !since.IsZero() {
		q += ` AND ts >= ?`
		args = append(args, msOf(since))
	}
	q += ` ORDER BY id ASC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("store: audit query: %w", err)
	}
	defer rows.Close()
	out := make([]storeAudit, 0)
	for rows.Next() {
		var e storeAudit
		var ts int64
		if err := rows.Scan(&e.ID, &ts, &e.SID, &e.KeyID, &e.IPHash, &e.Kind, &e.RequestID, &e.Detail); err != nil {
			return nil, fmt.Errorf("store: audit query: %w", err)
		}
		e.TS = timeOfMs(ts)
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// GC（设计 §2.5：逐表保留策略 + 级联）
// ---------------------------------------------------------------------------

// GC 过期清理（单事务；逐表删除行数入 stats——级联行按删除前计数显式统计）。
func (s *sqliteStore) GC(now time.Time) (storeGCStats, error) {
	c := gcCutoffsAt(now)
	var st storeGCStats
	tx, err := s.db.Begin()
	if err != nil {
		return st, fmt.Errorf("store: gc begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	count := func(q string, args ...any) (int, error) {
		var n int
		if err := tx.QueryRow(q, args...).Scan(&n); err != nil {
			return 0, fmt.Errorf("store: gc count: %w", err)
		}
		return n, nil
	}
	exec := func(q string, args ...any) error {
		if _, err := tx.Exec(q, args...); err != nil {
			return fmt.Errorf("store: gc: %w", err)
		}
		return nil
	}
	// ① 过期会话（expires_at = idle 滑动点；abs_expires_at = 绝对点）——
	// 级联带删 conversations → messages → turns（删除前计数）。
	const expiredSessions = `SELECT sid FROM sessions WHERE expires_at < ? OR abs_expires_at < ?`
	cut := msOf(c.sessionExpiredBefore)
	if st.Sessions, err = count(`SELECT COUNT(*) FROM sessions WHERE expires_at < ? OR abs_expires_at < ?`, cut, cut); err != nil {
		return st, err
	}
	if st.Sessions > 0 {
		if st.Conversations, err = count(
			`SELECT COUNT(*) FROM conversations WHERE sid IN (`+expiredSessions+`)`, cut, cut); err != nil {
			return st, err
		}
		if st.Messages, err = count(
			`SELECT COUNT(*) FROM messages WHERE conv_id IN (SELECT id FROM conversations WHERE sid IN (`+expiredSessions+`))`,
			cut, cut); err != nil {
			return st, err
		}
		if st.Turns, err = count(
			`SELECT COUNT(*) FROM turns WHERE conv_id IN (SELECT id FROM conversations WHERE sid IN (`+expiredSessions+`))`,
			cut, cut); err != nil {
			return st, err
		}
		if err = exec(`DELETE FROM sessions WHERE expires_at < ? OR abs_expires_at < ?`, cut, cut); err != nil {
			return st, err
		}
	}
	// ② 软删超期会话硬删（级联 messages/turns）。
	softCut := msOf(c.softDeletedBefore)
	var softConvs, softMsgs, softTurns int
	if softConvs, err = count(`SELECT COUNT(*) FROM conversations WHERE deleted_at IS NOT NULL AND deleted_at < ?`, softCut); err != nil {
		return st, err
	}
	if softConvs > 0 {
		if softMsgs, err = count(
			`SELECT COUNT(*) FROM messages WHERE conv_id IN (SELECT id FROM conversations WHERE deleted_at IS NOT NULL AND deleted_at < ?)`,
			softCut); err != nil {
			return st, err
		}
		if softTurns, err = count(
			`SELECT COUNT(*) FROM turns WHERE conv_id IN (SELECT id FROM conversations WHERE deleted_at IS NOT NULL AND deleted_at < ?)`,
			softCut); err != nil {
			return st, err
		}
		if err = exec(`DELETE FROM conversations WHERE deleted_at IS NOT NULL AND deleted_at < ?`, softCut); err != nil {
			return st, err
		}
		st.Conversations += softConvs
		st.Messages += softMsgs
		st.Turns += softTurns
	}
	// ③ 超期 turns（保留 30d；② 已带删的不重复计）。
	turnCut := msOf(c.turnsBefore)
	var n int
	if n, err = count(`SELECT COUNT(*) FROM turns WHERE created_at < ?`, turnCut); err != nil {
		return st, err
	}
	if n > 0 {
		if err = exec(`DELETE FROM turns WHERE created_at < ?`, turnCut); err != nil {
			return st, err
		}
		st.Turns += n
	}
	// ④ 超期 audit_log（保留 30d）。
	auditCut := msOf(c.auditBefore)
	if n, err = count(`SELECT COUNT(*) FROM audit_log WHERE ts < ?`, auditCut); err != nil {
		return st, err
	}
	if n > 0 {
		if err = exec(`DELETE FROM audit_log WHERE ts < ?`, auditCut); err != nil {
			return st, err
		}
		st.Audit += n
	}
	// ⑤ 超期 usage_counters（保留 90d；day 是 'YYYY-MM-DD' 字符串——字典序即
	// 时间序）。
	if n, err = count(`SELECT COUNT(*) FROM usage_counters WHERE day < ?`, c.usageBeforeDay); err != nil {
		return st, err
	}
	if n > 0 {
		if err = exec(`DELETE FROM usage_counters WHERE day < ?`, c.usageBeforeDay); err != nil {
			return st, err
		}
		st.Usage += n
	}
	if err := tx.Commit(); err != nil {
		return st, fmt.Errorf("store: gc commit: %w", err)
	}
	return st, nil
}

// storeSQLiteTables 迁移断言用：schema v1 的全部表名（六数据表 + 迁移锚）。
var storeSQLiteTables = []string{
	"schema_meta", "sessions", "conversations", "messages", "turns", "usage_counters", "audit_log",
}

// storeSQLiteIndexes 迁移断言用：schema v1 的全部索引名。
var storeSQLiteIndexes = []string{
	"idx_sessions_expires", "idx_sessions_key", "idx_conv_sid", "idx_turns_req", "idx_audit_ts",
}

// tableNames 当前库中的表名（sqlite_master；诊断/测试用）。
func (s *sqliteStore) tableNames() ([]string, error) {
	rows, err := s.db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name ASC`)
	if err != nil {
		return nil, fmt.Errorf("store: sqlite_master: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("store: sqlite_master: %w", err)
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// indexNames 当前库中的索引名（sqlite_master；诊断/测试用）。
func (s *sqliteStore) indexNames() ([]string, error) {
	rows, err := s.db.Query(`SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%' ORDER BY name ASC`)
	if err != nil {
		return nil, fmt.Errorf("store: sqlite_master: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("store: sqlite_master: %w", err)
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// pragmaValue 读连接级 PRAGMA（单连接模型下即全局——测试断言用）。
func (s *sqliteStore) pragmaValue(name string) (string, error) {
	var v string
	if err := s.db.QueryRow(`PRAGMA ` + name).Scan(&v); err != nil {
		return "", fmt.Errorf("store: pragma %s: %w", name, err)
	}
	return strings.ToLower(strings.TrimSpace(v)), nil
}

// countRows 表行数（测试断言用；表名来自本文件常量——非用户输入）。
func (s *sqliteStore) countRows(table string) (int, error) {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
		return 0, fmt.Errorf("store: count %s: %w", table, err)
	}
	return n, nil
}
