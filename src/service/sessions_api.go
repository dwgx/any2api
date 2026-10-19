// sessions_api.go 会话/消息 API（P1-3；E1–E8；设计 `claude2api-public-service`
// §2.6/§2.3a；board A2A-L19-W65-1）。
//
// 端点（统一前缀 `/ui/api/sessions`；全部走既有 sessionWrap → 有效会话）：
//
//	E1 GET    /ui/api/sessions                     列表（since 增量 + 墓碑）
//	E2 POST   /ui/api/sessions                     创建（客户端 id 幂等）
//	E3 GET    /ui/api/sessions/{id}                单读（session + messages）
//	E4 PUT    /ui/api/sessions/{id}                整会话权威写（base_rev CAS）
//	E5 PATCH  /ui/api/sessions/{id}                只改 title/model（base_rev CAS）
//	E6 DELETE /ui/api/sessions/{id}                软删（幂等）
//	E7 GET    /ui/api/sessions/{id}/messages       消息增量读（after_seq）
//	E8 POST   /ui/api/sessions/{id}/messages       消息追加（base_rev CAS + id 幂等）
//
// 纪律：
//   - **租户隔离**：所有 conversations 查询先判 `c.SID == sess.SID`；跨会话一律
//     404（不泄露存在性）。
//   - **默认关闭零回归**：public 未启用 → 404 JSON（不是 SPA 200）；store 未启用
//     → 503 `store_unavailable`（不是 500）；既有 /v1/*、/ui 行为零变化。
//   - **不接生成面限额**：CRUD 不占全局并发闸/日预算（只有 turn 钩子写台账）。
//   - **零凭据字节**：审计 detail 只含 op/conv/rev/messages（无 title/content/
//     cookie/key）；响应不回显 sid。
package service

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// 会话/消息 API 常量（规格 §2.3.1/§2.5.3）。
const (
	// sessionsAPIPrefix 资源前缀（路由注册与 fallback 解析共用）。
	sessionsAPIPrefix = "/ui/api/sessions"
	// sessionsAPIMaxBody 请求体上限（2 MiB；超限 413 request_too_large）。
	sessionsAPIMaxBody = 2 << 20
	// sessionsAPIMaxMessages 单次 PUT 的 messages 条数上限（超限 413）。
	sessionsAPIMaxMessages = 500
	// sessionsAPIMaxContentBytes 单条 content JSON 字节上限（256 KiB；超限 413）。
	sessionsAPIMaxContentBytes = 256 << 10
	// sessionsAPIMaxContentBlocks 单条 content 块数上限（超限 413）。
	sessionsAPIMaxContentBlocks = 64
	// sessionsAPIMaxTitleRunes title 字符上限（超限 400）。
	sessionsAPIMaxTitleRunes = 200
	// sessionsAPIMaxModelBytes model 字节上限（超限 400）。
	sessionsAPIMaxModelBytes = 200
	// sessionsAPIDefaultLimit 列表/消息增量读缺省条数。
	sessionsAPIDefaultLimit = 200
	// sessionsAPIMaxLimit 列表/消息增量读条数上限（超过钳到 500）。
	sessionsAPIMaxLimit = 500
	// sessionsWritePerMin per-session 写桶容量（60 写/分钟；规格 §2.5.3）。
	sessionsWritePerMin = 60
	// sessionsAPIClockSkew 客户端 created_at 允许的未来时钟漂移（5 分钟）。
	sessionsAPIClockSkew = 5 * time.Minute
)

// 会话/消息 API 校验错误（映射：413 面用前三个，其余 400）。
var (
	errSessionsTooManyMessages = errors.New("too many messages")
	errSessionsContentTooLarge = errors.New("content too large")
	errSessionsTooManyBlocks   = errors.New("too many content blocks")
)

// ---------------------------------------------------------------------------
// JSON 视图（API 字段 ↔ store 列；规格 §2.4.2）
// ---------------------------------------------------------------------------

// sessionView 会话列表行视图（created_at/updated_at/deleted_at 全部 unix ms）。
type sessionView struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Model     string `json:"model"`
	Rev       int    `json:"rev"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
	Deleted   bool   `json:"deleted"`
}

// messageView 消息视图（content = 块数组原样 JSON——未知块型前向兼容）。
type messageView struct {
	ID        string          `json:"id"`
	Seq       int             `json:"seq"`
	Role      string          `json:"role"`
	Content   json.RawMessage `json:"content"`
	Status    string          `json:"status"`
	CreatedAt int64           `json:"created_at"`
}

// conversationViewOf store 行 → 视图。
func conversationViewOf(c storeConversation) sessionView {
	return sessionView{
		ID:        c.ID,
		Title:     c.Title,
		Model:     c.Model,
		Rev:       c.Rev,
		CreatedAt: msOf(c.CreatedAt),
		UpdatedAt: msOf(c.UpdatedAt),
		Deleted:   !c.DeletedAt.IsZero(),
	}
}

// messageViewOf store 行 → 视图（content 原样字节）。
func messageViewOf(m storeMessage) messageView {
	content := json.RawMessage(m.Content)
	if len(content) == 0 {
		content = json.RawMessage("[]")
	}
	return messageView{
		ID:        m.ID,
		Seq:       m.Seq,
		Role:      m.Role,
		Content:   content,
		Status:    m.Status,
		CreatedAt: msOf(m.CreatedAt),
	}
}

// messagesViewOf 批量转换。
func messagesViewOf(msgs []storeMessage) []messageView {
	out := make([]messageView, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, messageViewOf(m))
	}
	return out
}

// 响应信封（字段名 = 规格 §2.3 逐字）。
type sessionsListResponse struct {
	Sessions  []sessionView `json:"sessions"`
	Now       int64         `json:"now"`
	NextSince int64         `json:"next_since"`
}

type sessionEnvelope struct {
	Session sessionView `json:"session"`
}

type sessionMessagesEnvelope struct {
	Session  sessionView   `json:"session"`
	Messages []messageView `json:"messages"`
}

type messagesEnvelope struct {
	Messages []messageView `json:"messages"`
	MaxSeq   int           `json:"max_seq"`
}

type messageEnvelope struct {
	Message messageView `json:"message"`
	Rev     int         `json:"rev"`
}

type deleteEnvelope struct {
	OK        bool  `json:"ok"`
	DeletedAt int64 `json:"deleted_at"`
	Rev       int   `json:"rev"`
}

// sessionsAPIErrorDetail 错误明细（与 writeSessionErrorJSON 同形）。
type sessionsAPIErrorDetail struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// conflictEnvelope 冲突响应（错误明细 + 服务端最新会话/消息）。
type conflictEnvelope struct {
	Error    sessionsAPIErrorDetail `json:"error"`
	Session  sessionView            `json:"session"`
	Messages []messageView          `json:"messages"`
}

// ---------------------------------------------------------------------------
// 路由注册（E1–E8 + 兜底）
// ---------------------------------------------------------------------------

// registerSessionsAPI 注册 8 个 method pattern + 一个子树兜底（未知方法/子路径
// 回 405/404 JSON——绝不落进 "/ui/" SPA 回退）。**始终注册**（见 server.go
// Handler 注释）。
func (s *Server) registerSessionsAPI(mux *http.ServeMux) {
	// E1–E8：method pattern 逐条注册（grep "ui/api/sessions" 可见 8 条）。
	mux.HandleFunc("GET /ui/api/sessions", s.handleSessionsList)                         // E1
	mux.HandleFunc("POST /ui/api/sessions", s.handleSessionsCreate)                      // E2
	mux.HandleFunc("GET /ui/api/sessions/{id}", s.handleSessionsGet)                     // E3
	mux.HandleFunc("PUT /ui/api/sessions/{id}", s.handleSessionsPut)                     // E4
	mux.HandleFunc("PATCH /ui/api/sessions/{id}", s.handleSessionsPatch)                 // E5
	mux.HandleFunc("DELETE /ui/api/sessions/{id}", s.handleSessionsDelete)               // E6
	mux.HandleFunc("GET /ui/api/sessions/{id}/messages", s.handleSessionsMessagesList)   // E7
	mux.HandleFunc("POST /ui/api/sessions/{id}/messages", s.handleSessionsMessageAppend) // E8
	// 子树兜底（未知方法/子路径 → 405/404 JSON；绝不落进 "/ui/" SPA 回退）。
	mux.HandleFunc("/ui/api/sessions/", s.handleSessionsAPIFallback)
}

// handleSessionsAPIFallback 未匹配的 method/子路径（405 + Allow / 404 JSON）。
func (s *Server) handleSessionsAPIFallback(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, sessionsAPIPrefix)
	rest = strings.Trim(rest, "/")
	segs := []string{}
	if rest != "" {
		segs = strings.Split(rest, "/")
	}
	switch {
	case len(segs) == 0:
		w.Header().Set("Allow", "GET, POST")
		s.writeSessionErrorJSON(w, http.StatusMethodNotAllowed, "invalid_request_error", "method not allowed")
	case len(segs) == 1:
		w.Header().Set("Allow", "GET, PUT, PATCH, DELETE")
		s.writeSessionErrorJSON(w, http.StatusMethodNotAllowed, "invalid_request_error", "method not allowed")
	case len(segs) == 2 && segs[1] == "messages":
		w.Header().Set("Allow", "GET, POST")
		s.writeSessionErrorJSON(w, http.StatusMethodNotAllowed, "invalid_request_error", "method not allowed")
	default:
		s.writeSessionErrorJSON(w, http.StatusNotFound, "not_found", "unknown sessions api path")
	}
}

// ---------------------------------------------------------------------------
// 公共守卫（就绪 / 写面 / 解码 / 存储失败）
// ---------------------------------------------------------------------------

// sessionsAPIReady 就绪守卫：public 未启用 → 404 JSON；store 未启用 → 503
// store_unavailable；无会话（理论不可达）→ 401。
func (s *Server) sessionsAPIReady(w http.ResponseWriter, r *http.Request) (Session, bool) {
	if s.sessions == nil {
		s.writeSessionErrorJSON(w, http.StatusNotFound, "not_found", "sessions api is not enabled")
		return Session{}, false
	}
	if s.store == nil {
		w.Header().Set("Retry-After", "1")
		s.writeSessionErrorJSON(w, http.StatusServiceUnavailable, "store_unavailable", "store is not enabled")
		return Session{}, false
	}
	sess, ok := SIDFromContext(r.Context())
	if !ok {
		s.writeSessionErrorJSON(w, http.StatusUnauthorized, "authentication_error", "session required")
		return Session{}, false
	}
	return sess, true
}

// sessionsAPIWriteGuard 变更方法守卫：Origin 同源双保险（无 Origin 放行）+
// per-session 写桶（60/min；超限 429 + 正整数 Retry-After）。
func (s *Server) sessionsAPIWriteGuard(w http.ResponseWriter, r *http.Request, sess Session) bool {
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		if err != nil || !strings.EqualFold(u.Host, r.Host) {
			s.writeSessionErrorJSON(w, http.StatusForbidden, "forbidden", "cross-origin request rejected")
			return false
		}
	}
	if s.sessionsWriteLimiter != nil {
		if ok, retry := s.sessionsWriteLimiter.allow(sess.SID); !ok {
			secs := int(math.Ceil(retry.Seconds()))
			if secs < 1 {
				secs = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(secs))
			s.writeSessionErrorJSON(w, http.StatusTooManyRequests, "rate_limited", "too many session writes")
			return false
		}
	}
	return true
}

// decodeSessionsBody 解码 JSON 体：Content-Type 必须 application/json（415）；
// 2 MiB 上限（413）；DisallowUnknownFields（400）。
func (s *Server) decodeSessionsBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	ct := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	if !strings.HasPrefix(ct, "application/json") {
		s.writeSessionErrorJSON(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "content type must be application/json")
		return false
	}
	body := http.MaxBytesReader(w, r.Body, sessionsAPIMaxBody)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			s.writeSessionErrorJSON(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds 2MB")
			return false
		}
		s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

// sessionsStoreFailure store 读写失败 → 503 store_unavailable（Retry-After: 1）
// + log/ring store_error。绝不 500（前端据此降级 localStorage）。
func (s *Server) sessionsStoreFailure(w http.ResponseWriter, op string, err error) {
	s.storeError(op, err)
	w.Header().Set("Retry-After", "1")
	s.writeSessionErrorJSON(w, http.StatusServiceUnavailable, "store_unavailable", "store operation failed")
}

// writeSessionsJSON 成功响应（application/json; charset=utf-8 + no-store）。
func (s *Server) writeSessionsJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		s.writeSessionErrorJSON(w, http.StatusInternalServerError, "internal_error", "encode response failed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// sessionsNow 服务端当前时刻（测试可注入 = 会话表时钟）。
func (s *Server) sessionsNow() time.Time {
	if s != nil && s.sessions != nil && s.sessions.now != nil {
		return s.sessions.now()
	}
	return time.Now()
}

// auditSessionsOp 会话/消息写审计（detail 只含 op/conv/rev/messages——**不含
// title/content/cookie/key/IP 原文**；sid 只落 8 字符前缀）。
func (s *Server) auditSessionsOp(sess Session, kind, op, convID string, rev, messages int) {
	detail, err := json.Marshal(struct {
		Op       string `json:"op"`
		Conv     string `json:"conv"`
		Rev      int    `json:"rev"`
		Messages int    `json:"messages"`
	}{Op: op, Conv: convID, Rev: rev, Messages: messages})
	if err != nil {
		return
	}
	s.auditStore(storeAudit{
		TS:     time.Now(),
		SID:    sidPrefix(sess.SID),
		KeyID:  sess.KeyID,
		Kind:   kind,
		Detail: string(detail),
	})
}

// loadOwnedConversation 读会话并做归属校验（不存在 / 跨会话 → 404，不泄露
// 存在性）；store 失败 → 503。
func (s *Server) loadOwnedConversation(w http.ResponseWriter, sess Session, id string) (storeConversation, bool) {
	conv, found, err := s.store.ConversationGet(id)
	if err != nil {
		s.sessionsStoreFailure(w, "conversation get", err)
		return storeConversation{}, false
	}
	if !found || conv.SID != sess.SID {
		s.writeSessionErrorJSON(w, http.StatusNotFound, "not_found", "session not found")
		return storeConversation{}, false
	}
	return conv, true
}

// writeSessionsConflict 409 + 服务端最新（session + messages；前端据此拉回）。
func (s *Server) writeSessionsConflict(w http.ResponseWriter, msg string, conv storeConversation, msgs []storeMessage) {
	var body conflictEnvelope
	body.Error.Type = "conflict"
	body.Error.Message = msg
	body.Session = conversationViewOf(conv)
	body.Messages = messagesViewOf(msgs)
	s.writeSessionsJSON(w, http.StatusConflict, body)
}

// ---------------------------------------------------------------------------
// 输入校验
// ---------------------------------------------------------------------------

// validSessionsID id 形态：`[A-Za-z0-9_-]{1,64}`（规格 §2.3.1-4）。
func validSessionsID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for i := range len(id) {
		c := id[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}

// newSessionsUUID 生成 uuid v4（crypto/rand；服务端缺省 id）。
func newSessionsUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 极端失败：退回时间熵（绝不返回空 id）。
		return "s-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	dst := make([]byte, 36)
	hex.Encode(dst[0:8], b[0:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], b[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], b[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], b[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:36], b[10:16])
	return string(dst)
}

// validateSessionMeta title/model 上限（title = 字符，model = 字节）。
func validateSessionMeta(title, model string) error {
	if utf8.RuneCountInString(title) > sessionsAPIMaxTitleRunes {
		return fmt.Errorf("title exceeds %d characters", sessionsAPIMaxTitleRunes)
	}
	if len(model) > sessionsAPIMaxModelBytes {
		return fmt.Errorf("model exceeds %d bytes", sessionsAPIMaxModelBytes)
	}
	return nil
}

// sessionsAPIMessageIn 消息输入（E4/E8 共用；seq 由服务端分配）。
type sessionsAPIMessageIn struct {
	ID        string          `json:"id"`
	Role      string          `json:"role"`
	Content   json.RawMessage `json:"content"`
	Status    string          `json:"status"`
	CreatedAt int64           `json:"created_at"`
}

// normalizeSessionsContent 校验 content 块数组并原样返回字节（未知块型前向兼容）。
func normalizeSessionsContent(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", errors.New("content is required")
	}
	if len(raw) > sessionsAPIMaxContentBytes {
		return "", errSessionsContentTooLarge
	}
	var blocks []json.RawMessage
	if err := json.Unmarshal(raw, &blocks); err != nil || blocks == nil {
		return "", errors.New("content must be an array of blocks")
	}
	if len(blocks) > sessionsAPIMaxContentBlocks {
		return "", errSessionsTooManyBlocks
	}
	for _, block := range blocks {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(block, &obj); err != nil || obj == nil {
			return "", errors.New("content block must be an object")
		}
	}
	return string(raw), nil
}

// buildSessionMessages 输入消息数组 → store 行（seq = 1..N 按数组序）。
func (s *Server) buildSessionMessages(in []sessionsAPIMessageIn, convID string, now time.Time) ([]storeMessage, error) {
	if len(in) > sessionsAPIMaxMessages {
		return nil, errSessionsTooManyMessages
	}
	out := make([]storeMessage, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for i, m := range in {
		switch m.Role {
		case "user", "assistant", "system":
		default:
			return nil, fmt.Errorf("messages[%d]: invalid role %q", i, m.Role)
		}
		status := m.Status
		if status == "" {
			status = "done"
		}
		switch status {
		case "done", "streaming", "stopped", "error":
		default:
			return nil, fmt.Errorf("messages[%d]: invalid status %q", i, status)
		}
		content, err := normalizeSessionsContent(m.Content)
		if err != nil {
			return nil, fmt.Errorf("messages[%d]: %w", i, err)
		}
		id := strings.TrimSpace(m.ID)
		if id == "" {
			id = newSessionsUUID()
		}
		if !validSessionsID(id) {
			return nil, fmt.Errorf("messages[%d]: invalid id", i)
		}
		if _, dup := seen[id]; dup {
			return nil, fmt.Errorf("messages[%d]: duplicate id", i)
		}
		seen[id] = struct{}{}
		created := now
		if m.CreatedAt > 0 && m.CreatedAt <= now.Add(sessionsAPIClockSkew).UnixMilli() {
			created = time.UnixMilli(m.CreatedAt).UTC()
		}
		out = append(out, storeMessage{
			ID:        id,
			ConvID:    convID,
			Seq:       i + 1,
			Role:      m.Role,
			Content:   content,
			Status:    status,
			CreatedAt: created,
		})
	}
	return out, nil
}

// sessionsBuildError 把 buildSessionMessages 错误映射为 HTTP 状态。
func sessionsBuildError(w http.ResponseWriter, s *Server, err error) {
	switch {
	case errors.Is(err, errSessionsTooManyMessages),
		errors.Is(err, errSessionsContentTooLarge),
		errors.Is(err, errSessionsTooManyBlocks):
		s.writeSessionErrorJSON(w, http.StatusRequestEntityTooLarge, "request_too_large", err.Error())
	default:
		s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", err.Error())
	}
}

// nextSessionWriteTime 写时刻（保证严格大于旧 updated_at——Windows 毫秒粒度下
// 测试与前端排序都稳定）。
func nextSessionWriteTime(now, old time.Time) time.Time {
	if old.IsZero() || now.After(old) {
		return now
	}
	return old.Add(time.Millisecond)
}

// ---------------------------------------------------------------------------
// E1 GET /ui/api/sessions
// ---------------------------------------------------------------------------

func (s *Server) handleSessionsList(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.sessionsAPIReady(w, r)
	if !ok {
		return
	}
	var since int64
	if v := r.URL.Query().Get("since"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", "since must be a non-negative integer (unix ms)")
			return
		}
		since = n
	}
	limit := sessionsAPIDefaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", "limit must be a positive integer")
			return
		}
		if n > sessionsAPIMaxLimit {
			n = sessionsAPIMaxLimit
		}
		limit = n
	}
	convs, err := s.store.ConversationsBySession(sess.SID)
	if err != nil {
		s.sessionsStoreFailure(w, "conversations by session", err)
		return
	}
	views := make([]sessionView, 0, len(convs))
	for _, c := range convs {
		if since > 0 && msOf(c.UpdatedAt) < since {
			continue
		}
		views = append(views, conversationViewOf(c))
	}
	sort.Slice(views, func(i, j int) bool {
		if views[i].UpdatedAt == views[j].UpdatedAt {
			return views[i].ID < views[j].ID
		}
		return views[i].UpdatedAt > views[j].UpdatedAt
	})
	if len(views) > limit {
		views = views[:limit]
	}
	now := s.sessionsNow().UTC().UnixMilli()
	s.writeSessionsJSON(w, http.StatusOK, sessionsListResponse{Sessions: views, Now: now, NextSince: now})
}

// ---------------------------------------------------------------------------
// E2 POST /ui/api/sessions
// ---------------------------------------------------------------------------

func (s *Server) handleSessionsCreate(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.sessionsAPIReady(w, r)
	if !ok {
		return
	}
	if !s.sessionsAPIWriteGuard(w, r, sess) {
		return
	}
	var in struct {
		ID    string `json:"id"`
		Title string `json:"title"`
		Model string `json:"model"`
	}
	if !s.decodeSessionsBody(w, r, &in) {
		return
	}
	id := strings.TrimSpace(in.ID)
	if id == "" {
		id = newSessionsUUID()
	}
	if !validSessionsID(id) {
		s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", "invalid session id")
		return
	}
	if err := validateSessionMeta(in.Title, in.Model); err != nil {
		s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	now := s.sessionsNow()
	c := storeConversation{ID: id, SID: sess.SID, Title: in.Title, Model: in.Model, Rev: 1, CreatedAt: now, UpdatedAt: now}
	created, err := s.store.ConversationSave(c, nil, 0, true)
	if err != nil {
		s.sessionsStoreFailure(w, "conversation create", err)
		return
	}
	if !created {
		existing, found, err := s.store.ConversationGet(id)
		if err != nil {
			s.sessionsStoreFailure(w, "conversation get", err)
			return
		}
		if !found || existing.SID != sess.SID || !existing.DeletedAt.IsZero() {
			s.writeSessionErrorJSON(w, http.StatusConflict, "conflict", "session id already exists")
			return
		}
		// 幂等：同 id 同会话已存在 → 200（不 bump rev）。
		s.writeSessionsJSON(w, http.StatusOK, sessionEnvelope{Session: conversationViewOf(existing)})
		return
	}
	s.auditSessionsOp(sess, adminLogConversationCreate, "create", id, 1, 0)
	s.writeSessionsJSON(w, http.StatusCreated, sessionEnvelope{Session: conversationViewOf(c)})
}

// ---------------------------------------------------------------------------
// E3 GET /ui/api/sessions/{id}
// ---------------------------------------------------------------------------

func (s *Server) handleSessionsGet(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.sessionsAPIReady(w, r)
	if !ok {
		return
	}
	includeDeleted := false
	switch r.URL.Query().Get("include_deleted") {
	case "", "0":
	case "1":
		includeDeleted = true
	default:
		s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", "include_deleted must be 0 or 1")
		return
	}
	conv, ok := s.loadOwnedConversation(w, sess, r.PathValue("id"))
	if !ok {
		return
	}
	if !conv.DeletedAt.IsZero() && !includeDeleted {
		s.writeSessionErrorJSON(w, http.StatusNotFound, "not_found", "session not found")
		return
	}
	msgs, err := s.store.MessagesByConversation(conv.ID)
	if err != nil {
		s.sessionsStoreFailure(w, "messages by conversation", err)
		return
	}
	s.writeSessionsJSON(w, http.StatusOK, sessionMessagesEnvelope{
		Session: conversationViewOf(conv), Messages: messagesViewOf(msgs),
	})
}

// ---------------------------------------------------------------------------
// E4 PUT /ui/api/sessions/{id}（整会话权威写）
// ---------------------------------------------------------------------------

func (s *Server) handleSessionsPut(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.sessionsAPIReady(w, r)
	if !ok {
		return
	}
	if !s.sessionsAPIWriteGuard(w, r, sess) {
		return
	}
	var in struct {
		BaseRev  *int                    `json:"base_rev"`
		Title    string                  `json:"title"`
		Model    string                  `json:"model"`
		Messages *[]sessionsAPIMessageIn `json:"messages"`
	}
	if !s.decodeSessionsBody(w, r, &in) {
		return
	}
	if in.Messages == nil {
		s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", "messages is required")
		return
	}
	if err := validateSessionMeta(in.Title, in.Model); err != nil {
		s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	baseRev := 0
	if in.BaseRev != nil {
		baseRev = *in.BaseRev
		if baseRev < 0 {
			s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", "base_rev must be >= 0")
			return
		}
	}
	id := r.PathValue("id")
	if !validSessionsID(id) {
		s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", "invalid session id")
		return
	}
	now := s.sessionsNow()
	msgs, err := s.buildSessionMessages(*in.Messages, id, now)
	if err != nil {
		sessionsBuildError(w, s, err)
		return
	}
	existing, found, err := s.store.ConversationGet(id)
	if err != nil {
		s.sessionsStoreFailure(w, "conversation get", err)
		return
	}
	if found && existing.SID != sess.SID {
		s.writeSessionErrorJSON(w, http.StatusNotFound, "not_found", "session not found")
		return
	}
	if found && !existing.DeletedAt.IsZero() {
		s.writeSessionErrorJSON(w, http.StatusNotFound, "not_found", "session not found")
		return
	}
	if !found {
		if baseRev != 0 {
			s.writeSessionErrorJSON(w, http.StatusNotFound, "not_found", "session not found")
			return
		}
		c := storeConversation{ID: id, SID: sess.SID, Title: in.Title, Model: in.Model, Rev: 1, CreatedAt: now, UpdatedAt: now}
		created, err := s.store.ConversationSave(c, msgs, 0, true)
		if err != nil {
			if errors.Is(err, errStoreMessageIDConflict) {
				s.writeSessionErrorJSON(w, http.StatusConflict, "conflict", "message id belongs to another session")
				return
			}
			s.sessionsStoreFailure(w, "conversation save", err)
			return
		}
		if !created {
			latest, ok2, _ := s.store.ConversationGet(id)
			if ok2 {
				latestMsgs, _ := s.store.MessagesByConversation(id)
				s.writeSessionsConflict(w, "session already exists", latest, latestMsgs)
				return
			}
			s.writeSessionErrorJSON(w, http.StatusConflict, "conflict", "session already exists")
			return
		}
		s.auditSessionsOp(sess, adminLogConversationCreate, "create", id, 1, len(msgs))
		s.writeSessionsJSON(w, http.StatusCreated, sessionMessagesEnvelope{
			Session: conversationViewOf(c), Messages: messagesViewOf(msgs),
		})
		return
	}
	if baseRev != existing.Rev {
		latestMsgs, err := s.store.MessagesByConversation(id)
		if err != nil {
			s.sessionsStoreFailure(w, "messages by conversation", err)
			return
		}
		s.writeSessionsConflict(w,
			fmt.Sprintf("rev mismatch: base_rev=%d, current=%d", baseRev, existing.Rev), existing, latestMsgs)
		return
	}
	c := existing
	c.Title = in.Title
	c.Model = in.Model
	c.UpdatedAt = nextSessionWriteTime(now, existing.UpdatedAt)
	saved, err := s.store.ConversationSave(c, msgs, baseRev, false)
	if err != nil {
		if errors.Is(err, errStoreMessageIDConflict) {
			s.writeSessionErrorJSON(w, http.StatusConflict, "conflict", "message id belongs to another session")
			return
		}
		s.sessionsStoreFailure(w, "conversation save", err)
		return
	}
	if !saved {
		latest, ok2, _ := s.store.ConversationGet(id)
		if ok2 {
			latestMsgs, _ := s.store.MessagesByConversation(id)
			s.writeSessionsConflict(w, "rev changed concurrently", latest, latestMsgs)
			return
		}
		s.writeSessionErrorJSON(w, http.StatusConflict, "conflict", "rev changed concurrently")
		return
	}
	updated, ok2, err := s.store.ConversationGet(id)
	if err != nil || !ok2 {
		s.sessionsStoreFailure(w, "conversation get", err)
		return
	}
	savedMsgs, err := s.store.MessagesByConversation(id)
	if err != nil {
		s.sessionsStoreFailure(w, "messages by conversation", err)
		return
	}
	s.auditSessionsOp(sess, adminLogConversationUpdate, "update", id, updated.Rev, len(savedMsgs))
	s.writeSessionsJSON(w, http.StatusOK, sessionMessagesEnvelope{
		Session: conversationViewOf(updated), Messages: messagesViewOf(savedMsgs),
	})
}

// ---------------------------------------------------------------------------
// E5 PATCH /ui/api/sessions/{id}（只改元数据）
// ---------------------------------------------------------------------------

func (s *Server) handleSessionsPatch(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.sessionsAPIReady(w, r)
	if !ok {
		return
	}
	if !s.sessionsAPIWriteGuard(w, r, sess) {
		return
	}
	var in struct {
		BaseRev *int    `json:"base_rev"`
		Title   *string `json:"title"`
		Model   *string `json:"model"`
	}
	if !s.decodeSessionsBody(w, r, &in) {
		return
	}
	if in.BaseRev == nil || *in.BaseRev < 0 {
		s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", "base_rev is required and must be >= 0")
		return
	}
	id := r.PathValue("id")
	if !validSessionsID(id) {
		s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", "invalid session id")
		return
	}
	conv, ok := s.loadOwnedConversation(w, sess, id)
	if !ok {
		return
	}
	if !conv.DeletedAt.IsZero() {
		s.writeSessionErrorJSON(w, http.StatusNotFound, "not_found", "session not found")
		return
	}
	if *in.BaseRev != conv.Rev {
		msgs, err := s.store.MessagesByConversation(id)
		if err != nil {
			s.sessionsStoreFailure(w, "messages by conversation", err)
			return
		}
		s.writeSessionsConflict(w,
			fmt.Sprintf("rev mismatch: base_rev=%d, current=%d", *in.BaseRev, conv.Rev), conv, msgs)
		return
	}
	if in.Title != nil {
		conv.Title = *in.Title
	}
	if in.Model != nil {
		conv.Model = *in.Model
	}
	if err := validateSessionMeta(conv.Title, conv.Model); err != nil {
		s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	conv.UpdatedAt = nextSessionWriteTime(s.sessionsNow(), conv.UpdatedAt)
	saved, err := s.store.ConversationSave(conv, nil, *in.BaseRev, false)
	if err != nil {
		s.sessionsStoreFailure(w, "conversation save", err)
		return
	}
	if !saved {
		latest, ok2, _ := s.store.ConversationGet(id)
		if ok2 {
			msgs, _ := s.store.MessagesByConversation(id)
			s.writeSessionsConflict(w, "rev changed concurrently", latest, msgs)
			return
		}
		s.writeSessionErrorJSON(w, http.StatusConflict, "conflict", "rev changed concurrently")
		return
	}
	updated, ok2, err := s.store.ConversationGet(id)
	if err != nil || !ok2 {
		s.sessionsStoreFailure(w, "conversation get", err)
		return
	}
	s.auditSessionsOp(sess, adminLogConversationUpdate, "patch", id, updated.Rev, 0)
	s.writeSessionsJSON(w, http.StatusOK, sessionEnvelope{Session: conversationViewOf(updated)})
}

// ---------------------------------------------------------------------------
// E6 DELETE /ui/api/sessions/{id}（软删；幂等）
// ---------------------------------------------------------------------------

func (s *Server) handleSessionsDelete(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.sessionsAPIReady(w, r)
	if !ok {
		return
	}
	if !s.sessionsAPIWriteGuard(w, r, sess) {
		return
	}
	id := r.PathValue("id")
	if !validSessionsID(id) {
		s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", "invalid session id")
		return
	}
	conv, found, err := s.store.ConversationGet(id)
	if err != nil {
		s.sessionsStoreFailure(w, "conversation get", err)
		return
	}
	if !found {
		// 幂等：不存在也 200（不泄露存在性；无 rev 可报）。
		s.writeSessionsJSON(w, http.StatusOK, deleteEnvelope{OK: true})
		return
	}
	if conv.SID != sess.SID {
		// 跨会话按 §2.3.1-2/测试 #10 统一 404（E6 的「不属于本会话 200」与全局
		// 隔离条冲突——取 404：不泄露存在性且与其余七端点一致）。
		s.writeSessionErrorJSON(w, http.StatusNotFound, "not_found", "session not found")
		return
	}
	if !conv.DeletedAt.IsZero() {
		// 重复删除：返回原 rev / 原 deleted_at（幂等）。
		s.writeSessionsJSON(w, http.StatusOK, deleteEnvelope{
			OK: true, DeletedAt: msOf(conv.DeletedAt), Rev: conv.Rev,
		})
		return
	}
	if err := s.store.ConversationSoftDelete(id, s.sessionsNow()); err != nil {
		s.sessionsStoreFailure(w, "conversation soft delete", err)
		return
	}
	updated, ok2, err := s.store.ConversationGet(id)
	if err != nil || !ok2 {
		s.sessionsStoreFailure(w, "conversation get", err)
		return
	}
	s.auditSessionsOp(sess, adminLogConversationDelete, "delete", id, updated.Rev, 0)
	s.writeSessionsJSON(w, http.StatusOK, deleteEnvelope{
		OK: true, DeletedAt: msOf(updated.DeletedAt), Rev: updated.Rev,
	})
}

// ---------------------------------------------------------------------------
// E7 GET /ui/api/sessions/{id}/messages（增量读）
// ---------------------------------------------------------------------------

func (s *Server) handleSessionsMessagesList(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.sessionsAPIReady(w, r)
	if !ok {
		return
	}
	var afterSeq int
	if v := r.URL.Query().Get("after_seq"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", "after_seq must be a non-negative integer")
			return
		}
		afterSeq = n
	}
	limit := sessionsAPIDefaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", "limit must be a positive integer")
			return
		}
		if n > sessionsAPIMaxLimit {
			n = sessionsAPIMaxLimit
		}
		limit = n
	}
	conv, ok := s.loadOwnedConversation(w, sess, r.PathValue("id"))
	if !ok {
		return
	}
	if !conv.DeletedAt.IsZero() {
		s.writeSessionErrorJSON(w, http.StatusNotFound, "not_found", "session not found")
		return
	}
	msgs, err := s.store.MessagesByConversation(conv.ID)
	if err != nil {
		s.sessionsStoreFailure(w, "messages by conversation", err)
		return
	}
	out := make([]messageView, 0, len(msgs))
	maxSeq := afterSeq
	for _, m := range msgs {
		if m.Seq <= afterSeq {
			continue
		}
		out = append(out, messageViewOf(m))
		maxSeq = m.Seq
		if len(out) >= limit {
			break
		}
	}
	s.writeSessionsJSON(w, http.StatusOK, messagesEnvelope{Messages: out, MaxSeq: maxSeq})
}

// ---------------------------------------------------------------------------
// E8 POST /ui/api/sessions/{id}/messages（追加）
// ---------------------------------------------------------------------------

func (s *Server) handleSessionsMessageAppend(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.sessionsAPIReady(w, r)
	if !ok {
		return
	}
	if !s.sessionsAPIWriteGuard(w, r, sess) {
		return
	}
	var in struct {
		ID        string          `json:"id"`
		BaseRev   *int            `json:"base_rev"`
		Role      string          `json:"role"`
		Content   json.RawMessage `json:"content"`
		Status    string          `json:"status"`
		CreatedAt int64           `json:"created_at"`
	}
	if !s.decodeSessionsBody(w, r, &in) {
		return
	}
	if in.BaseRev == nil || *in.BaseRev < 0 {
		s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", "base_rev is required and must be >= 0")
		return
	}
	id := r.PathValue("id")
	if !validSessionsID(id) {
		s.writeSessionErrorJSON(w, http.StatusBadRequest, "invalid_request_error", "invalid session id")
		return
	}
	conv, ok := s.loadOwnedConversation(w, sess, id)
	if !ok {
		return
	}
	if !conv.DeletedAt.IsZero() {
		s.writeSessionErrorJSON(w, http.StatusNotFound, "not_found", "session not found")
		return
	}
	now := s.sessionsNow()
	built, err := s.buildSessionMessages([]sessionsAPIMessageIn{{
		ID: in.ID, Role: in.Role, Content: in.Content, Status: in.Status, CreatedAt: in.CreatedAt,
	}}, id, now)
	if err != nil {
		sessionsBuildError(w, s, err)
		return
	}
	msg := built[0]
	// 幂等优先：同 id 已存在（本会话）→ 200 返回现有消息（不 bump rev）——即使
	// 客户端重试时的 base_rev 已过期。
	if existing, found, err := s.store.MessageGet(msg.ID); err != nil {
		s.sessionsStoreFailure(w, "message get", err)
		return
	} else if found {
		if existing.ConvID == id {
			s.writeSessionsJSON(w, http.StatusOK, messageEnvelope{
				Message: messageViewOf(existing), Rev: conv.Rev,
			})
			return
		}
		s.writeSessionErrorJSON(w, http.StatusConflict, "conflict", "message id belongs to another session")
		return
	}
	msgs, err := s.store.MessagesByConversation(id)
	if err != nil {
		s.sessionsStoreFailure(w, "messages by conversation", err)
		return
	}
	maxSeq := 0
	for _, m := range msgs {
		if m.Seq > maxSeq {
			maxSeq = m.Seq
		}
	}
	msg.Seq = maxSeq + 1
	appended, err := s.store.MessageAppendRev(msg, *in.BaseRev)
	if err != nil {
		if errors.Is(err, errStoreMessageIDConflict) {
			// 竞态兜底：同 id 并发插入——同会话按幂等 200，跨会话 409。
			if existing, found, gerr := s.store.MessageGet(msg.ID); gerr == nil && found && existing.ConvID == id {
				s.writeSessionsJSON(w, http.StatusOK, messageEnvelope{
					Message: messageViewOf(existing), Rev: conv.Rev,
				})
				return
			}
			s.writeSessionErrorJSON(w, http.StatusConflict, "conflict", "message id belongs to another session")
			return
		}
		s.sessionsStoreFailure(w, "message append", err)
		return
	}
	if !appended {
		latest, ok2, _ := s.store.ConversationGet(id)
		if ok2 {
			latestMsgs, _ := s.store.MessagesByConversation(id)
			s.writeSessionsConflict(w,
				fmt.Sprintf("rev mismatch: base_rev=%d, current=%d", *in.BaseRev, latest.Rev), latest, latestMsgs)
			return
		}
		s.writeSessionErrorJSON(w, http.StatusConflict, "conflict", "rev changed concurrently")
		return
	}
	s.auditSessionsOp(sess, adminLogMessageAppend, "append", id, *in.BaseRev+1, 1)
	s.writeSessionsJSON(w, http.StatusCreated, messageEnvelope{
		Message: messageViewOf(msg), Rev: *in.BaseRev + 1,
	})
}
