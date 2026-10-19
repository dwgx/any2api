// session.go 匿名会话中间件（P0-1）+ access key 门控（P0-2；设计
// `claude2api-public-service` §1；board A2A-L19-W57-1）。
//
// 形态（内存档；P1 迁 store——§2.3a sessions 表同构）：
//
//   - sid = crypto/rand 16 字节 → base64url（22 字符；纯随机、cookie 值不含
//     状态——服务端查表）；cookie `a2a_sid`：HttpOnly / SameSite=Lax / Path=/
//     / Max-Age=<绝对 TTL 秒>；Secure 视 TLS 面（直连 TLS 或可信反代
//     X-Forwarded-Proto=https——auto 语义，与 §6.2 trust proxy 同源）。
//   - 挂载面 = `/v1/*`、`/v1beta/` 与 `/v1beta1/`（Gemini/Vertex 入口族；
//     W68-3 扩面）与 `/ui/api/*`（`/ui/api/config` 除外）——见
//     sessionEligiblePath；静态面 /ui、/healthz、/admin/* 不挂。
//   - 创建时机：首个挂载面请求惰性建会话；**无 cookie / cookie 无效 = 自动
//     签发**（open 模式——API 客户端无 cookie 属常态，绝不因缺 cookie 拒绝）。
//   - 只在「无 cookie / cookie 无效」或「cookie 剩余寿命 < 7d（滚动续期）」
//     时 Set-Cookie——避免每响应重复 Set-Cookie（缓存/代理噪声）。
//   - gated 模式（P0-2）：无 cookie 且无有效 access key → 401（按入口协议
//     信封；v1beta 面 = Google 信封）；带 key 校验通过 → 建会话（记 key_id=
//     sha8）→ Set-Cookie；此后浏览器凭 cookie（key 不必每请求带）。提取面
//     顺序见 accessKeyFromRequest——Gemini 客户端另认 `x-goog-api-key` 头与
//     `?key=` 查询（仅 v1beta 路径读取；W68-3）。key 比较 = sha256 常量时间；
//     key 原文绝不进日志/响应；**key 移除 → 会话即时失效**（校验时对照当前
//     key 集；配置静态加载 → 移除 key + 重启生效）。
//   - logout：`POST /ui/api/session/logout`（删会话 + 清 cookie；幂等）。
//
// 纪律：sid 不进日志/URL；错误信封不含内部状态；**硬门：public.enabled=false
// （缺省）时本文件全部行为不挂载**（零行为变化——见 server.go New/Handler）。
package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dwgx/any2api/translate/ir"
)

// 会话面常量（P0-1；TTL 取设计 §1.3 缺省——P1 配置化，本波固定）。
const (
	// sessionCookieName 会话 cookie 名（接口冻结：`a2a_sid`）。
	sessionCookieName = "a2a_sid"
	// sessionIdleTTL 空闲过期（滑动：每次请求刷新）——缺省 7 天。
	sessionIdleTTL = 168 * time.Hour
	// sessionAbsTTL 绝对过期——缺省 90 天。
	sessionAbsTTL = 90 * 24 * time.Hour
	// sessionRollWindow cookie 滚动续期窗口：剩余寿命 < 7d 时重发一次。
	sessionRollWindow = 7 * 24 * time.Hour
	// sessionStoreCap 内存会话表上限（有界纪律；超限先清过期、再逐最久未用）。
	sessionStoreCap = 10000
	// sessionLogoutPath 显式登出端点（POST；设计 §1.3）。
	sessionLogoutPath = "/ui/api/session/logout"
)

// Session 会话读取面（接口冻结：SIDFromContext(ctx) (Session, bool)）。
// 字段 = 冻结形态；记录其余字段（过期点等）只在内部。
type Session struct {
	SID      string
	KeyID    string
	Created  time.Time
	LastSeen time.Time
}

// sessionCtxKey ctx 传递键（私有类型——外部只能经 SIDFromContext 读取）。
type sessionCtxKey struct{}

// SIDFromContext 从 ctx 取会话（接口冻结）。无会话 → ok=false。
func SIDFromContext(ctx context.Context) (Session, bool) {
	if ctx == nil {
		return Session{}, false
	}
	s, ok := ctx.Value(sessionCtxKey{}).(Session)
	return s, ok
}

// sidPrefix 会话 id 的前缀形态（审计/日志面——设计 §1.1/§4.4：sid 不进日志，
// 只前缀显示，如 `a1b2c3d4`）。零凭据字节纪律的一部分。
func sidPrefix(sid string) string {
	const n = 8
	if len(sid) > n {
		return sid[:n]
	}
	return sid
}

// accessKey 一枚准入 key 的比对态：id = sha8（记账/审计用——绝不落原文），
// sum = 全量 sha256（subtle.ConstantTimeCompare 定长比较，无长度侧信道）。
type accessKey struct {
	id  string
	sum [sha256.Size]byte
}

// newAccessKeys 由准入 key 原文构建比对态（id = sha256 前 8 hex = key_id）。
// 调用方绝不把原文写进日志/响应/记录。
func newAccessKeys(keys []string) []accessKey {
	out := make([]accessKey, 0, len(keys))
	for _, k := range keys {
		sum := sha256.Sum256([]byte(k))
		out = append(out, accessKey{id: hex.EncodeToString(sum[:])[:8], sum: sum})
	}
	return out
}

// sessionRecord 服务端会话记录（§2.3a sessions 表同构的内存档）。
type sessionRecord struct {
	sid        string
	keyID      string
	created    time.Time
	lastSeen   time.Time
	absExpires time.Time
	// cookieExp 上次 Set-Cookie 的 Max-Age 到期点（滚动续期判据；只在
	// set/markCookie 时更新）。
	cookieExp time.Time
}

// sessionStore 内存会话表（互斥 + 上限；过期惰性剔除 + 容量超限清扫）。
// now 可注入（测试假时钟）。全部方法返回记录**副本**——记录字段只在本表
// 互斥下读写（并发请求共享同一记录时无数据竞态）。
//
// **持久化档（P1-2）**：`persist != nil`（store.enabled=true）时，create/touch/
// delete 写穿落库、get 未命中内存表时回查（**重启恢复面**——设计 §5.3「sessions
// 重启后恢复」）；写失败只 log + ring `store_error`，**绝不影响请求路径**（设计
// §2.4-E）。`persist == nil`（缺省）= 逐字 P0 行为（零回归硬门）。
type sessionStore struct {
	mu  sync.Mutex
	m   map[string]*sessionRecord
	cap int
	now func() time.Time
	// persist 持久化层（nil = 纯内存档）。调用点绝不持 st.mu 调它（I/O 在锁外
	// ——内存表互斥不被磁盘延迟放大）。
	persist Store
	// onPersistErr 持久化失败回调（由 New 注入 = Server.storeError；nil = 只
	// 日志）。绝不进控制流。
	onPersistErr func(op string, err error)
}

// newSessionStore 建内存会话表（上限 = sessionStoreCap）。
func newSessionStore() *sessionStore {
	return &sessionStore{m: make(map[string]*sessionRecord), cap: sessionStoreCap, now: time.Now}
}

// newSessionID 生成 sid：crypto/rand 16 字节 → base64url（22 字符）。
func newSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 极端失败（系统熵源不可用）：退回时间熵——绝不返回空 id（空 id 会让
		// 全部请求共享一个会话，安全面不可接受）。
		return "t" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}

// expiredLocked 记录是否已过期（空闲滑动 / 绝对；调用方持锁）。
func (st *sessionStore) expiredLocked(r *sessionRecord, now time.Time) bool {
	return sessionExpired(r, now)
}

// sessionExpired 会话过期判据（空闲滑动 / 绝对）——内存档与持久化档（回查
// store 后）共用同一判据。
func sessionExpired(r *sessionRecord, now time.Time) bool {
	return now.After(r.absExpires) || now.Sub(r.lastSeen) > sessionIdleTTL
}

// toStoreRecord 内存记录 → store 行（§2.3a `sessions` 列映射；expires_at = idle
// 滑动点 = last_seen + idle TTL；cookie 到期点（cookieExp）是 cookie 记账，
// 落库面不需要——恢复时按 abs_expires_at 重建）。
func (r sessionRecord) toStoreRecord() storeSession {
	return storeSession{
		SID:          r.sid,
		KeyID:        r.keyID,
		CreatedAt:    r.created,
		LastSeenAt:   r.lastSeen,
		ExpiresAt:    r.lastSeen.Add(sessionIdleTTL),
		AbsExpiresAt: r.absExpires,
	}
}

// sessionRecordFromStore store 行 → 内存记录（重启恢复面；cookieExp = 绝对过期
// 点——浏览器侧 cookie 的 Max-Age 即按 abs 过期点签发，滚动续期判据因此同源）。
func sessionRecordFromStore(s storeSession) sessionRecord {
	return sessionRecord{
		sid:        s.SID,
		keyID:      s.KeyID,
		created:    s.CreatedAt,
		lastSeen:   s.LastSeenAt,
		absExpires: s.AbsExpiresAt,
		cookieExp:  s.AbsExpiresAt,
	}
}

// persistError 持久化失败面：日志 +（注入的）ring `store_error`。**绝不返回、
// 绝不改控制流**——设计 §2.4-E「失败只 log，绝不影响请求路径」。
func (st *sessionStore) persistError(op string, err error) {
	if err == nil {
		return
	}
	if st.onPersistErr != nil {
		st.onPersistErr(op, err)
		return
	}
	log.Printf("any2api: session store %s: %v", op, err)
}

// persistCreate 写穿：新建会话落库（best-effort）。
func (st *sessionStore) persistCreate(rec sessionRecord) {
	if st.persist == nil {
		return
	}
	st.persistError("session create", st.persist.SessionCreate(rec.toStoreRecord()))
}

// persistTouch 写穿：滑动过期落库（last_seen + idle 过期点）。
func (st *sessionStore) persistTouch(sid string, now time.Time) {
	if st.persist == nil {
		return
	}
	st.persistError("session touch", st.persist.SessionTouch(sid, now, now.Add(sessionIdleTTL)))
}

// persistDelete 写穿：删会话落库（logout/过期惰性清理/吊销——级联 conversations/
// messages/turns 由 DDL 外键负责）。
func (st *sessionStore) persistDelete(sid string) {
	if st.persist == nil {
		return
	}
	_, err := st.persist.SessionDelete(sid)
	st.persistError("session delete", err)
}

// create 建会话（签发 sid；返回记录副本）。容量超限：先清过期，再逐最久未用
// （有界——绝不无界增长）。持久化档：新建记录写穿落库（失败只 log）。
func (st *sessionStore) create(keyID string, now time.Time) sessionRecord {
	rec := &sessionRecord{
		sid:        newSessionID(),
		keyID:      keyID,
		created:    now,
		lastSeen:   now,
		absExpires: now.Add(sessionAbsTTL),
		cookieExp:  now.Add(sessionAbsTTL),
	}
	st.mu.Lock()
	if st.m == nil {
		st.m = make(map[string]*sessionRecord)
	}
	// 边界守卫（A16 / W60-SRC1 Findings）：cap ≤ 0 归一化为 1。
	//
	// 非生产路径：cap 只在 newSessionStore 由常量 sessionStoreCap（10000）
	// 赋值，配置面不暴露会话容量；只有同包测试/代码直改 st.cap 才能触发。
	// 归一化是必要的：下方逐出循环的终止条件是 len(st.m) >= st.cap，cap=0
	// 时该式恒真（空表也成立），循环永不收敛（死循环）。钳到 1 后退化为
	// 「单会话表」——有界、可收敛，而非崩溃或无限循环。
	if st.cap <= 0 {
		st.cap = 1
	}
	if len(st.m) >= st.cap {
		for sid, r := range st.m {
			if st.expiredLocked(r, now) {
				delete(st.m, sid)
			}
		}
		for len(st.m) >= st.cap {
			oldestSid := ""
			var oldest time.Time
			first := true
			for sid, r := range st.m {
				if first || r.lastSeen.Before(oldest) {
					oldestSid, oldest, first = sid, r.lastSeen, false
				}
			}
			delete(st.m, oldestSid)
		}
	}
	st.m[rec.sid] = rec
	st.mu.Unlock()
	st.persistCreate(*rec)
	return *rec
}

// get 查会话（含过期校验：过期即删、视同无会话——调用方走重新签发）。返回副本。
//
// 持久化档（P1-2）：内存未命中 → 回查 store（**重启恢复面**——设计 §5.3）；命中
// 且未过期/未吊销 → 回填内存表并返回（用户无感，cookie 继续有效）；已过期/已吊销
// → 删库 + 视同无会话。
func (st *sessionStore) get(sid string, now time.Time) (sessionRecord, bool) {
	st.mu.Lock()
	r, ok := st.m[sid]
	if ok {
		if st.expiredLocked(r, now) {
			delete(st.m, sid)
			st.mu.Unlock()
			st.persistDelete(sid)
			return sessionRecord{}, false
		}
		rec := *r
		st.mu.Unlock()
		return rec, true
	}
	st.mu.Unlock()
	if st.persist == nil {
		return sessionRecord{}, false
	}
	row, found, err := st.persist.SessionGet(sid)
	if err != nil {
		st.persistError("session get", err)
		return sessionRecord{}, false
	}
	if !found {
		return sessionRecord{}, false
	}
	rec := sessionRecordFromStore(row)
	if row.Revoked || sessionExpired(&rec, now) {
		st.persistDelete(sid) // 惰性清理（过期/吊销——GC 之外的第一道）
		return sessionRecord{}, false
	}
	st.mu.Lock()
	if st.m == nil {
		st.m = make(map[string]*sessionRecord)
	}
	if len(st.m) < st.cap {
		cached := rec
		st.m[sid] = &cached
	}
	st.mu.Unlock()
	return rec, true
}

// touch 刷新 lastSeen（滑动过期）。持久化档：写穿落库（失败只 log）。
func (st *sessionStore) touch(sid string, now time.Time) {
	st.mu.Lock()
	if r, ok := st.m[sid]; ok {
		r.lastSeen = now
	}
	st.mu.Unlock()
	st.persistTouch(sid, now)
}

// markCookie 记录本次 Set-Cookie 的到期点（滚动续期判据）。
func (st *sessionStore) markCookie(sid string, exp time.Time) {
	st.mu.Lock()
	if r, ok := st.m[sid]; ok {
		r.cookieExp = exp
	}
	st.mu.Unlock()
}

// delete 删会话（logout / 吊销）。返回是否存在。持久化档：删库（级联
// conversations/messages/turns——DDL 外键）。
func (st *sessionStore) delete(sid string) bool {
	st.mu.Lock()
	_, ok := st.m[sid]
	if ok {
		delete(st.m, sid)
	}
	st.mu.Unlock()
	if st.persist != nil {
		_, err := st.persist.SessionDelete(sid)
		st.persistError("session delete", err)
	}
	return ok
}

// len 当前会话数（测试/诊断用）。
func (st *sessionStore) len() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.m)
}

// ---------------------------------------------------------------------------
// 中间件（挂载面 / 解析 / 签发 / 门控）
// ---------------------------------------------------------------------------

// sessionEligiblePath 会话面路径：/v1/*、/v1beta/、/v1beta1/（Gemini/Vertex
// 入口族；W68-3 扩面）与 /ui/api/*（/ui/api/config 除外——前端引导配置不建
// 会话、不门控，设计 §1.1 字面口径）。
//
// 历史（W57-1/A13 边界 → W68-3 收口）：v1beta 族曾被刻意排除在会话面之外
// ——当时 accessKeyFromRequest 只认 `Authorization: Bearer` 与 `X-Access-Key`，
// 直接纳入会误拒以 `x-goog-api-key` / `?key=` 携带 key 的合法 Gemini 客户端。
// 后果：gated 模式下 `/v1beta*` 绕过 access key 门控（公开部署安全缺口）。
// 本单先补 key 提取面（见 accessKeyFromRequest 的 Gemini 分支），再扩本函数。
//
// 前缀带尾斜杠（与 rateLimitedPath 同口径）：`/v1beta` 裸路径不是入口
// （仍 404）、`/v1beta2/*` 这类未注册前缀不误伤——两者都不进会话面。
func sessionEligiblePath(p string) bool {
	if p == "/ui/api/config" {
		return false
	}
	return strings.HasPrefix(p, "/v1/") ||
		strings.HasPrefix(p, "/v1beta/") ||
		strings.HasPrefix(p, "/v1beta1/") ||
		strings.HasPrefix(p, "/ui/api/")
}

// sessionWrap 会话中间件（P0-1；仅 public.enabled=true 时由 Handler 挂载）。
//
// 流程：cookie 解析（有效 → 校验 gated key 集 → 滚动续期 → ctx 传递）→
// 无 cookie/无效：gated 需 key（无/错 → 401）→ 签发（open 自动 / gated 记
// key_id）→ Set-Cookie → ctx 传递。logout 路径无有效会话时直过（幂等清 cookie）。
func (s *Server) sessionWrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !sessionEligiblePath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		now := s.sessions.now()
		if rec, ok := s.sessionFromCookie(r, now); ok {
			// gated 吊销面（P0-2）：会话 key_id 必须仍在当前 key 集——key 移除
			// （改配置 + 重启）后旧会话即时失效。
			if s.accessGated && !s.knownKeyID(rec.keyID) {
				s.sessions.delete(rec.sid)
				s.writeSessionUnauthorized(w, r, "access key no longer valid")
				return
			}
			// 滚动续期（剩余寿命 < 7d 才重发——避免每响应 Set-Cookie）。
			if rec.cookieExp.Add(-sessionRollWindow).Before(now) {
				s.sessions.markCookie(rec.sid, rec.absExpires)
				s.setSessionCookie(w, r, rec.sid, rec.absExpires, now)
			}
			s.sessions.touch(rec.sid, now)
			next.ServeHTTP(w, r.WithContext(sessionWithContext(r.Context(), rec, now)))
			return
		}
		// 无有效会话：logout 直过（handler 幂等清 cookie；绝不「先发新再清」）。
		if r.URL.Path == sessionLogoutPath {
			next.ServeHTTP(w, r)
			return
		}
		// gated：无 key 的新会话不可建立（401——按入口协议信封）。
		keyID := ""
		if s.accessGated {
			kid, ok := s.matchAccessKey(r)
			if !ok {
				s.writeSessionUnauthorized(w, r, "access key required")
				return
			}
			keyID = kid
		}
		rec := s.sessions.create(keyID, now)
		// P0-6 审计最小版：会话签发进管理面 ring + 内存计数（**只记 sid 前缀 /
		// key_id=sha8，零凭据字节**——设计 §4.4 双落点）。
		msg := "session created (sid=" + sidPrefix(rec.sid) + ")"
		if rec.keyID != "" {
			msg += ", key_id=" + rec.keyID
		}
		s.adminLog.add(adminLogSessionCreate, "", "", msg, 0)
		// P1-2 审计持久化（设计 §4.4 双落点之①）：同一事件落 store.audit_log
		// （store 未启用 = no-op）。**sid 只落前缀 + key_id=sha8——零凭据字节**。
		s.auditStore(storeAudit{
			TS:    now,
			SID:   sidPrefix(rec.sid),
			KeyID: rec.keyID,
			Kind:  adminLogSessionCreate,
		})
		s.diag.publicSessionsCreated.Add(1)
		s.setSessionCookie(w, r, rec.sid, rec.absExpires, now)
		next.ServeHTTP(w, r.WithContext(sessionWithContext(r.Context(), rec, now)))
	})
}

// sessionWithContext 把会话挂进 ctx（ctx 值 = 冻结的 Session 形态）。
func sessionWithContext(ctx context.Context, rec sessionRecord, now time.Time) context.Context {
	return context.WithValue(ctx, sessionCtxKey{}, Session{
		SID:      rec.sid,
		KeyID:    rec.keyID,
		Created:  rec.created,
		LastSeen: now,
	})
}

// sessionFromCookie 解析 cookie 并校验（无 cookie/无效/过期 → false）。
func (s *Server) sessionFromCookie(r *http.Request, now time.Time) (sessionRecord, bool) {
	c, err := r.Cookie(sessionCookieName)
	if err != nil || c.Value == "" {
		return sessionRecord{}, false
	}
	return s.sessions.get(c.Value, now)
}

// setSessionCookie 写会话 cookie（HttpOnly / SameSite=Lax / Path=/；
// Max-Age = 剩余绝对 TTL 秒；Secure = TLS 面判定——auto 语义）。
func (s *Server) setSessionCookie(w http.ResponseWriter, r *http.Request, sid string, expires time.Time, now time.Time) {
	maxAge := int(expires.Sub(now).Seconds())
	if maxAge < 1 {
		maxAge = 1
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    sid,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   s.sessionSecure(r),
	})
}

// clearSessionCookie 清会话 cookie（logout；Max-Age=0）。
func (s *Server) clearSessionCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   s.sessionSecure(r),
	})
}

// sessionSecure Secure 属性判定（auto：直连 TLS 或「可信反代 + X-Forwarded-
// Proto=https」——与 P0-3 trust proxy 同源配置；HTTP 直连调试允许 false）。
func (s *Server) sessionSecure(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	if len(s.trustedProxies) > 0 && s.peerTrusted(r) &&
		strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
		return true
	}
	return false
}

// peerTrusted TCP 对端（RemoteAddr）是否命中 trusted_proxies。
func (s *Server) peerTrusted(r *http.Request) bool {
	host := clientIP(r)
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, n := range s.trustedProxies {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// knownKeyID 会话 key_id 是否仍在当前 key 集（gated 吊销面；定长比较）。
func (s *Server) knownKeyID(id string) bool {
	for i := range s.accessKeys {
		if subtle.ConstantTimeCompare([]byte(id), []byte(s.accessKeys[i].id)) == 1 {
			return true
		}
	}
	return false
}

// matchAccessKey 校验请求携带的准入 key：sha256 + 常量时间比较（遍历全部 key
// 不早退——比较工作量恒定）；命中 → 返回 key_id（sha8）。绝不回显 key 原文。
func (s *Server) matchAccessKey(r *http.Request) (string, bool) {
	got := accessKeyFromRequest(r)
	if got == "" || len(s.accessKeys) == 0 {
		return "", false
	}
	sum := sha256.Sum256([]byte(got))
	matched := -1
	for i := range s.accessKeys {
		if subtle.ConstantTimeCompare(sum[:], s.accessKeys[i].sum[:]) == 1 {
			matched = i
		}
	}
	if matched < 0 {
		return "", false
	}
	return s.accessKeys[matched].id, true
}

// accessKeyFromRequest 取准入 key，按入口提取面顺序（W68-3 扩面）：
//
//  1. `Authorization: Bearer <key>`（scheme 大小写不敏感）；
//  2. `X-Access-Key` 兜底（与 admin 面同款双头先例，admin.go:461）。
//
// 出现非 Bearer 的 Authorization 即视为凭据面污染 → 直接返回空（不回落
// 其它来源；既有纪律，未改）。
//
// Gemini 客户端扩展面（W68-3；仅 `/v1beta/`、`/v1beta1/` 前缀读取）：
//
//  3. `x-goog-api-key` 头（Gemini 原生携带方式）；
//  4. `?key=` 查询参数（Gemini SDK 第二形态）。
//
// 其它路径不读这两者——普通查询串里的 `key`（如 `/v1/...?key=...`）不是本
// 服务凭据，读它会把常规请求误判成「带凭据」而误拒。
//
// 返回的是候选原文（调用方只做 sha256 定长比对）；本函数绝不回显/记录它。
func accessKeyFromRequest(r *http.Request) string {
	if v := r.Header.Get("Authorization"); v != "" {
		if parts := strings.SplitN(v, " ", 2); len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			return strings.TrimSpace(parts[1])
		}
		return ""
	}
	if v := strings.TrimSpace(r.Header.Get("X-Access-Key")); v != "" {
		return v
	}
	if geminiKeyPath(r.URL.Path) {
		if v := strings.TrimSpace(r.Header.Get("x-goog-api-key")); v != "" {
			return v
		}
		return strings.TrimSpace(r.URL.Query().Get("key"))
	}
	return ""
}

// geminiKeyPath 客户端 key 的 Gemini/Vertex 扩展面：仅 `/v1beta/` 与
// `/v1beta1/`（前缀带尾斜杠——`/v1beta` 裸路径与 `/v1beta2/*` 不在内，
// 与 sessionEligiblePath / rateLimitedPath 同口径）。
func geminiKeyPath(p string) bool {
	return strings.HasPrefix(p, "/v1beta/") || strings.HasPrefix(p, "/v1beta1/")
}

// writeSessionUnauthorized gated 401（无 key / key 无效 / key 已移除）。按入口
// 协议信封：/ui/api/* = 最小 JSON；其余 = writeError（复用限流面的路径 →
// 格式映射：/v1beta* 与 /v1/projects/* → Gemini 面 Google 信封
// `{"error":{"code":401,"status":"UNAUTHENTICATED",…}}`，其余 /v1/* 按各自
// 入口协议）。消息绝不含 key 原文。
func (s *Server) writeSessionUnauthorized(w http.ResponseWriter, r *http.Request, msg string) {
	w.Header().Set("Cache-Control", "no-store")
	if strings.HasPrefix(r.URL.Path, "/ui/api/") {
		type errBody struct {
			Error struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		var b errBody
		b.Error.Type = "authentication_error"
		b.Error.Message = msg
		body, err := json.Marshal(b)
		if err != nil {
			body = []byte(`{"error":{"type":"authentication_error"}}`)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write(body)
		return
	}
	s.writeError(w, rateLimitFormat(r.URL.Path), http.StatusUnauthorized,
		&ir.Error{Type: "authentication_error", Message: msg, Source: "any2api"})
}

// handleSessionLogout POST /ui/api/session/logout：删会话 + 清 cookie（幂等：
// 无会话也清 cookie 回 200）。方法限定在处理器内做（注册为无方法精确路径——
// 避免 "/ui/" 子树 SPA 回退把非 POST 吞成 200）；变更类端点的 Origin 双保险
// （设计 §1.6：同源要求；无 Origin 的 curl 类请求放行——由 key/限流面兜底）。
func (s *Server) handleSessionLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		s.writeSessionErrorJSON(w, http.StatusMethodNotAllowed, "invalid_request_error", "method not allowed")
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		if err != nil || !strings.EqualFold(u.Host, r.Host) {
			s.writeSessionErrorJSON(w, http.StatusForbidden, "forbidden", "cross-origin request rejected")
			return
		}
	}
	if c, err := r.Cookie(sessionCookieName); err == nil && c.Value != "" {
		s.sessions.delete(c.Value)
	}
	s.clearSessionCookie(w, r)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// writeSessionErrorJSON 最小 JSON 错误（/ui/api 面）。
func (s *Server) writeSessionErrorJSON(w http.ResponseWriter, status int, typ, msg string) {
	type errBody struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	var b errBody
	b.Error.Type = typ
	b.Error.Message = msg
	body, err := json.Marshal(b)
	if err != nil {
		body = []byte(`{"error":{"type":"internal_error"}}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
