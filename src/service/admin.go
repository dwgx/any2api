package service

// ---------------------------------------------------------------------------
// 最小管理面（W5-33；P63 裁定落地：topics/cpa §管理 API v8 的「对照我们服务壳」
// 裁定 + §下一波建议 1）
// ---------------------------------------------------------------------------
//
// 三条端点 + 一套统一密钥中间件（三条全部挂同一中间件）：
//
//   - `GET  /admin/diagnostics`     诊断快照（Diagnostics 计数 + per-upstream
//                                   健康：冷却状态/失败计数/失败率窗口 + ring 统计
//                                   + 鉴权面：累计失败数/当前封禁 IP 与到期）；
//   - `POST /admin/cooldowns/reset` 冷却复位（按名/按下标/全部；幂等）；
//   - `GET  /admin/logs`            错误日志只读（有界内存 ring，容量由
//                                   `admin_log_capacity` 配（缺省 256）；不落盘）
//                                   + 游标增量读（`since_id` / `next_cursor` /
//                                   `cursor_reset`，见「日志游标」节）。
//
// **纪律（P63「管理面纪律」整抄）**：
//
//   - 惰性注册：未配 `admin_key` = 三条路由根本不注册 → 404（非 401；CPA
//     `server_management.go:204-211` 同款语义）；
//   - 鉴权：`Authorization: Bearer <key>` 或 `X-Management-Key: <key>`（CPA
//     `handlers/management/handler.go:266-302` 同款）；常量时间比较（sha256 定长
//     比较——先哈希再比，消除长度侧信道）；连续 5 次失败封禁该 IP 30 分钟
//     （handler.go:364-407 同款数字）；
//   - 只读面纪律：`Cache-Control: no-store` + 读面带快照时间戳（CPA
//     auth_files.go:103-174 / logs.go:40-132 同款）。
//
// **不抄（P63 明列六项）**：凭据池 CRUD / OAuth 接管 / 配置热改 / 插件面 /
// `/requests/api-call` 代打 / 自更新（`/server/latest-version`）。理由见
// topics/cpa §管理 API v8「不抄」节——与我们「不做多账号池、不转发客户端凭据、
// 配置启动期静态加载」的边界无关，或风险 > 收益。CPA 的版本戳响应头
// （X-CPA-VERSION/…）不抄：本仓没有版本常量，发明版本串是装饰不是信息。
//
// **部署注记**：管理面仅建议**本机监听**（`listen` 缺省 127.0.0.1）——密钥是
// 唯一门；本仓不实现 TLS / 远端白名单 / allow-remote-management（不做即不暴露）。

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultAdminLogCapacity 管理面错误日志 ring 缺省容量（未配 `admin_log_capacity`
	// 时；有界、不落盘）。
	DefaultAdminLogCapacity = 256
	// adminLogCapacityMax `admin_log_capacity` 配置上限（越界 = 配置错误，见
	// config.go Validate）。
	adminLogCapacityMax = 4096
	// adminLogDefaultLimit `GET /admin/logs` 缺省返回条数。
	adminLogDefaultLimit = 100
	// adminLogCursorVersion 游标令牌版本（对齐 CPA `logCursor.Version` 的版本纪律：
	// 载荷形态变了必须升版本，旧版本令牌按「无法解析」拒绝而不是误读）。
	adminLogCursorVersion = 1
	// adminMaxBodyBytes 管理面请求体上限（复位端点只有小 JSON）。
	adminMaxBodyBytes = 64 << 10
	// adminAuthMaxFailures 连续鉴权失败阈值（CPA handler.go:364-407 的 5 次同源）。
	adminAuthMaxFailures = 5
	// adminAuthBanWindow 鉴权失败封禁窗口（CPA 30 分钟同源）。
	adminAuthBanWindow = 30 * time.Minute
	// adminAuthTrackedIPs 鉴权失败状态表上限（防无界增长；超限先清过期项，
	// 仍超限丢最旧条目）。
	adminAuthTrackedIPs = 128
)

// ---------------------------------------------------------------------------
// 错误日志 ring（有界内存；不落盘）
// ---------------------------------------------------------------------------

// 事件类别（brief 点名的四类关键事件 + W36-1 的任务终态面 + W36-2 的能力面证据）。
const (
	adminLogFailover       = "failover"        // 换上游（未提交前失败，链上还有下一个）
	adminLogCooldownTrip   = "cooldown_trip"   // 冷却进入（阈值/失败率/retry-after）
	adminLogCooldownReset  = "cooldown_reset"  // 冷却复位（管理面人工复位）
	adminLogPrecommitError = "precommit_error" // 提交前错误（错误信封已写下游 / 本地失败）
	// adminLogCredentialCooldown 凭据冷却（W54-3 残余 3；claude_web 凭据池）：
	// **单个凭据（池索引）进入冷却**——独立型别，与上游级 cooldown_trip 语义
	// 区分（前者只影响池内轮换/该号的后续请求；后者把整个上游从候选链跳过）。
	// 事件 message = `credential #N cooling for <时长> (class=<类别>)`——只记
	// 索引与类别，**绝不含凭据字节**（见 credentials_pool.go 的记账纪律）。
	adminLogCredentialCooldown = "credential_cooldown"
	// adminLogTaskTerminal 任务终态（W36-1 S6；托管任务，缓解缺口 C5）：提交后
	// 流内失败/截断/挂住/取消同批——事件载荷 = message 里的 JSON
	// `{task,status,attempts,duration_ms}`（见 task.go recordTaskTerminal）。
	adminLogTaskTerminal = "task_terminal"
	// adminLogTaskFaceEvidence 能力矩阵 L2 被动证据（W36-2 S8；topic
	// `service-task-surface` §③ L2 行）：passthrough 往返的结论性失败（上游
	// 404/405）——事件载荷 = message 里的 JSON
	// `{upstream,status,path,task,degrade_now}`（见 task_passthrough.go
	// observeTaskFaceEvidence）。
	adminLogTaskFaceEvidence = "task_face_evidence"
	// adminLogTaskFaceDegrade 能力面进程内降级（W36-2 S8；task_face_degrade=auto）：
	// 首个结论性失败后 passthrough→managed 的降级转换（每上游至多一条；重启
	// 清零）——事件载荷 = message 里的 JSON `{upstream,from,to}`。
	adminLogTaskFaceDegrade = "task_face_degrade"
	// adminLogSessionCreate 匿名会话签发（P0-6；public.enabled=true）：事件
	// message = `session created (sid=<前缀>[, key_id=<sha8>])`——**只记前缀/
	// key_id（sha8），零凭据字节**（设计 §1.1/§4.4）。
	adminLogSessionCreate = "session_create"
	// adminLogRateLimited 服务侧限流拒绝（P0-6；429 面：per-IP 令牌桶 / 全局
	// 并发闸）。message 含 `rate_limited` token；拒绝面计数同点。
	adminLogRateLimited = "rate_limited"
	// adminLogQuotaExhausted 日预算耗尽拒绝（P0-6；429 `daily_quota_exhausted`）。
	adminLogQuotaExhausted = "quota_exhausted"
	// adminLogCapacityExhausted 容量耗尽（P0-6；429 `capacity_exhausted`——池
	// 全不可用；message 保留 upstream_credentials_exhausted 检索 token）。
	adminLogCapacityExhausted = "capacity_exhausted"
	// adminLogStoreError 持久化层失败（P1-2；设计 §2.4-I 词表 `store_error`）：
	// message = `store_error: <op>: <err>`——**只记操作名与存储层错误文本**（文件
	// 路径/sqlite 消息），零凭据/cookie/key/IP 原文。持久化失败绝不影响请求路径
	// （设计 §2.4-E）。
	adminLogStoreError = "store_error"
	// 会话/消息 API 审计型别（P1-3；设计 §2.3a audit_log.kind 词表）：只落
	// `{op,conv,rev,messages}`——**不含 title/content/cookie/key/IP 原文**。
	adminLogConversationCreate = "conversation_create"
	adminLogConversationUpdate = "conversation_update"
	adminLogConversationDelete = "conversation_delete"
	adminLogMessageAppend      = "message_append"
)

// adminLogEntry 管理面 ring 的一条事件。
type adminLogEntry struct {
	Seq       uint64    `json:"seq"`                  // 单调递增序号（1 起；读面排序锚）
	Time      time.Time `json:"time"`                 // 事件时刻
	Kind      string    `json:"kind"`                 // 见上方类别常量
	Upstream  string    `json:"upstream,omitempty"`   // 相关上游名（复位/全局事件可空）
	Status    int       `json:"status,omitempty"`     // 上游状态码（无 = 省略）
	RequestID string    `json:"request_id,omitempty"` // 下游请求 ID（可关联响应头/记账日志）
	Message   string    `json:"message"`
}

// adminLogRing 有界内存 ring（覆盖写最旧；不落盘）。零值可用（首次 add 惰性分配；
// capacity 0 = 用 DefaultAdminLogCapacity 缺省）。
type adminLogRing struct {
	mu       sync.Mutex
	capacity int    // 容量（0 = 缺省；构造期由 admin_log_capacity 定，运行期只读）
	fp       string // 实例指纹（游标令牌跨进程失效检测；惰性生成）
	buf      []adminLogEntry
	next     int    // 下一个写入位
	count    int    // 已写条数（≤ len(buf)）
	seq      uint64 // 最近分配的序号（0 = 未写）
}

// capacityOrDefault 有效容量（未配 = DefaultAdminLogCapacity）。
func (r *adminLogRing) capacityOrDefault() int {
	if r.capacity > 0 {
		return r.capacity
	}
	return DefaultAdminLogCapacity
}

// fingerprintLocked 本实例指纹（惰性生成：8 字节随机 → 16 位 hex）。游标令牌带它，
// 跨进程/跨实例使用即判失效——对齐 CPA 游标的 fingerprint 面（它防的是日志文件
// 轮转/截断，我们防的是进程重启：seq 在新进程里从 0 重来，旧锚无意义）。调用方持锁。
func (r *adminLogRing) fingerprintLocked() string {
	if r.fp == "" {
		var b [8]byte
		_, _ = rand.Read(b[:]) // Go 1.24+ crypto/rand.Read 不会失败（失败即进程崩溃）
		r.fp = hex.EncodeToString(b[:])
	}
	return r.fp
}

// fingerprint 本实例 ring 指纹（游标令牌用；惰性生成）。
func (r *adminLogRing) fingerprint() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fingerprintLocked()
}

// add 追加一条事件（满则覆盖最旧）。
func (r *adminLogRing) add(kind, upstream, reqID, message string, status int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.buf) == 0 {
		r.buf = make([]adminLogEntry, r.capacityOrDefault())
	}
	r.seq++
	r.buf[r.next] = adminLogEntry{
		Seq: r.seq, Time: time.Now().UTC(), Kind: kind,
		Upstream: upstream, Status: status, RequestID: reqID, Message: message,
	}
	r.next = (r.next + 1) % len(r.buf)
	if r.count < len(r.buf) {
		r.count++
	}
}

// stats 当前存条数与最近序号。
func (r *adminLogRing) stats() (count int, latestSeq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count, r.seq
}

// ---------------------------------------------------------------------------
// 日志游标（W5-36；对齐 CPA logs.go 的 cursor/after/limit 三件套）
// ---------------------------------------------------------------------------
//
// 对齐与简化（对照 CPA `internal/api/handlers/management/logs.go:40-132,607-1010`）：
//
//   - **保留**：`next-cursor`（响应内游标令牌）+ 请求侧增量锚 + `cursor-reset`
//     失效信号 + 令牌带版本号（`v`）与实例指纹（`fingerprint`）+ limit 截断。
//   - **简化**：CPA 的位置锚是「文件 + 偏移 + 大小 + mtime + 内容指纹」（它的源是
//     落盘日志文件，要防轮转/截断/追加），我们的源是内存 ring，位置锚就是 ring 的
//     **单调 seq**（令牌载荷 = `{"v":1,"seq":N,"fp":"…"}`）。`after` 时间戳面不抄
//     （seq 锚没有时钟回拨/时钟精度问题）；文件/偏移/大小/mtime 四面无对应物。
//   - **失效（cursor_reset）三因**：① 覆盖丢弃（锚早于最早可用序号-1）；② 指纹
//     不符（令牌来自另一个进程/实例）；③ 锚超前（> 最近序号——本实例发出的锚恒
//     ≤ 发出时的最近序号，且序号单调，超前 = 外来锚；此时静默返回空会让客户端
//     永远收不到新事件）。重置时回落为「最新 limit 条」（对齐 CPA 重置即 tail）。

// adminLogWindow 一次游标读的结果。
type adminLogWindow struct {
	Entries   []adminLogEntry // incremental = 升序；缺省读 = 新→旧
	Count     int             // ring 当前存条数（同一锁内快照）
	LatestSeq uint64          // ring 最近序号（同一锁内快照）
	NextSeq   uint64          // 下次增量读的锚 = max(请求锚, 本窗口最新返回序号)
	Reset     bool            // 锚已失效 → 客户端须按返回批次重新同步
}

// window 读窗口。
//
//   - incremental=false（缺省读）：最新 limit 条，新→旧（W5-33 线上契约不变）；
//   - incremental=true（since_id 读）：seq > afterSeq 的可用条目，升序（增量消费
//     按发生时序）；锚失效（见上方三因）→ Reset=true + 最新 limit 条（升序）。
//
// fp 非空 = 令牌形态（校验实例指纹）；十进制锚 fp = ""（不校验）。
func (r *adminLogRing) window(afterSeq uint64, fp string, limit int, incremental bool) adminLogWindow {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := adminLogWindow{Count: r.count, LatestSeq: r.seq, Entries: []adminLogEntry{}, NextSeq: afterSeq}
	if incremental && fp != "" && fp != r.fingerprintLocked() {
		out.Reset = true // 指纹不符：令牌来自另一个实例
	}
	if r.count == 0 {
		if incremental && afterSeq > r.seq {
			out.Reset = true // 锚超前（空 ring：本进程还没发过任何事件）
		}
		return out
	}
	oldest := r.seq - uint64(r.count) + 1
	if incremental && !out.Reset && (afterSeq < oldest-1 || afterSeq > r.seq) {
		out.Reset = true
	}
	if limit <= 0 || limit > r.count {
		limit = r.count
	}
	from, n := r.count-limit, limit // 缺省读 / 重置回落：最新 limit 条
	if incremental && !out.Reset {
		start := afterSeq + 1
		if start < oldest {
			start = oldest
		}
		switch {
		case start > r.seq:
			from, n = 0, 0
		default:
			avail := int(r.seq - start + 1)
			from = int(start - oldest)
			n = min(avail, limit)
		}
	}
	for i := range n {
		idx := (r.next - r.count + from + i + len(r.buf)) % len(r.buf)
		out.Entries = append(out.Entries, r.buf[idx])
	}
	if !incremental {
		// 缺省读：反转为新→旧（线上契约）。
		for i, j := 0, len(out.Entries)-1; i < j; i, j = i+1, j-1 {
			out.Entries[i], out.Entries[j] = out.Entries[j], out.Entries[i]
		}
	}
	if len(out.Entries) > 0 {
		newest := out.Entries[len(out.Entries)-1].Seq // 升序末条 = 最新
		if !incremental {
			newest = out.Entries[0].Seq // 新→旧首条 = 最新
		}
		if newest > out.NextSeq {
			out.NextSeq = newest
		}
	}
	return out
}

// adminLogCursor 游标令牌载荷（对齐 CPA `logCursor` 的版本 + 位置锚 + 指纹三面；
// 简化见上方「日志游标」节）。
type adminLogCursor struct {
	Version int    `json:"v"`
	Seq     uint64 `json:"seq"`
	FP      string `json:"fp"`
}

// encodeAdminLogCursor 编码游标令牌（base64url 无填充；对齐 CPA encodeLogCursor）。
func encodeAdminLogCursor(c adminLogCursor) string {
	raw, err := json.Marshal(c)
	if err != nil {
		return "" // 结构体全可序列化，不会发生
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeAdminLogCursor 解码游标令牌：空/坏编码/坏载荷/版本不符 → error（请求侧 400）。
func decodeAdminLogCursor(raw string) (adminLogCursor, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return adminLogCursor{}, fmt.Errorf("empty cursor")
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		data, err = base64.URLEncoding.DecodeString(value)
	}
	if err != nil {
		return adminLogCursor{}, fmt.Errorf("invalid cursor encoding")
	}
	var c adminLogCursor
	if err := json.Unmarshal(data, &c); err != nil {
		return adminLogCursor{}, fmt.Errorf("invalid cursor payload")
	}
	if c.Version != adminLogCursorVersion {
		return adminLogCursor{}, fmt.Errorf("unsupported cursor version %d", c.Version)
	}
	return c, nil
}

// adminLogAnchor 解析 `since_id`：十进制序号（简单形态）或 `next_cursor` 令牌
// （带版本/指纹）。返回锚序号与令牌指纹（十进制形态 fp = ""）。两种形态都坏 → error。
func adminLogAnchor(raw string) (seq uint64, fp string, err error) {
	value := strings.TrimSpace(raw)
	if n, errNum := strconv.ParseUint(value, 10, 64); errNum == nil {
		return n, "", nil
	}
	c, errDec := decodeAdminLogCursor(value)
	if errDec != nil {
		return 0, "", errDec
	}
	return c.Seq, c.FP, nil
}

// ---------------------------------------------------------------------------
// 统一密钥中间件（Bearer / X-Management-Key；常量时间；连续失败封禁）
// ---------------------------------------------------------------------------

// adminAuthEntry 单个客户端 IP 的鉴权失败状态。
type adminAuthEntry struct {
	count int       // 连续失败计数（成功清零；封禁时清零）
	until time.Time // 封禁截止（零值 = 未封禁）
	last  time.Time // 最近一次失败时刻（清理用）
}

// adminAuthState 鉴权失败状态表（内存态；键 = 客户端 IP）。
type adminAuthState struct {
	mu       sync.Mutex
	failures map[string]*adminAuthEntry
	total    uint64 // 累计鉴权失败次数（进程内；只增——读面 auth_failures）
}

// blocked 该 IP 是否在封禁窗口内。
func (a *adminAuthState) blocked(ip string, now time.Time) bool {
	if ip == "" {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	e := a.failures[ip]
	return e != nil && now.Before(e.until)
}

// fail 记一次鉴权失败；连续失败达阈值即封禁（封禁时计数清零——窗口过后重新给满
// 5 次机会）。
func (a *adminAuthState) fail(ip string, now time.Time) {
	if ip == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failures == nil {
		a.failures = make(map[string]*adminAuthEntry)
	}
	a.total++
	e := a.failures[ip]
	if e == nil {
		e = &adminAuthEntry{}
		a.failures[ip] = e
	}
	e.last = now
	e.count++
	if e.count >= adminAuthMaxFailures {
		e.count = 0
		e.until = now.Add(adminAuthBanWindow)
	}
	a.pruneLocked(now)
}

// adminBannedIP 当前封禁中的一个客户端 IP（读面；**不含密钥**）。
type adminBannedIP struct {
	IP               string  `json:"ip"`
	BannedUntil      string  `json:"banned_until"` // RFC3339（UTC）
	RemainingSeconds float64 `json:"remaining_seconds"`
}

// adminAuthView `GET /admin/diagnostics` 的鉴权面快照（只读；W5-33 §缺口 1）。
// **绝不回显密钥**——只有 IP、计数、到期时刻。
type adminAuthView struct {
	AuthFailures uint64          `json:"auth_failures"` // 累计鉴权失败（进程内）
	TrackedIPs   int             `json:"tracked_ips"`   // 失败状态表条目数（上界 adminAuthTrackedIPs）
	BannedIPs    []adminBannedIP `json:"banned_ips"`    // 当前封禁中（按到期升序；无 = 空表）
}

// snapshot 鉴权面快照（只读）：累计失败 + 状态表大小 + 当前封禁 IP/到期。过期条目
// 不算「封禁中」（计数仍在表里，由 pruneLocked 回收）。
func (a *adminAuthState) snapshot(now time.Time) adminAuthView {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := adminAuthView{AuthFailures: a.total, TrackedIPs: len(a.failures), BannedIPs: []adminBannedIP{}}
	for ip, e := range a.failures {
		if !now.Before(e.until) {
			continue
		}
		out.BannedIPs = append(out.BannedIPs, adminBannedIP{
			IP:               ip,
			BannedUntil:      e.until.UTC().Format(time.RFC3339),
			RemainingSeconds: e.until.Sub(now).Seconds(),
		})
	}
	sort.Slice(out.BannedIPs, func(i, j int) bool {
		if out.BannedIPs[i].BannedUntil != out.BannedIPs[j].BannedUntil {
			return out.BannedIPs[i].BannedUntil < out.BannedIPs[j].BannedUntil
		}
		return out.BannedIPs[i].IP < out.BannedIPs[j].IP
	})
	return out
}

// succeed 一次成功鉴权：清除该 IP 的失败计数与封禁。
func (a *adminAuthState) succeed(ip string) {
	if ip == "" {
		return
	}
	a.mu.Lock()
	delete(a.failures, ip)
	a.mu.Unlock()
}

// pruneLocked 状态表有界（超上限先清过期项，仍超限丢最旧）。调用方持锁。
func (a *adminAuthState) pruneLocked(now time.Time) {
	if len(a.failures) <= adminAuthTrackedIPs {
		return
	}
	for ip, e := range a.failures {
		if !now.Before(e.until) && now.Sub(e.last) > adminAuthBanWindow {
			delete(a.failures, ip)
		}
	}
	for len(a.failures) > adminAuthTrackedIPs {
		oldestIP, first := "", true
		var oldest time.Time
		for ip, e := range a.failures {
			if first || e.last.Before(oldest) {
				oldestIP, oldest, first = ip, e.last, false
			}
		}
		delete(a.failures, oldestIP)
	}
}

// adminAuth 管理面统一密钥中间件。挂载前提 = 已配 admin_key（未配时路由根本不
// 注册——404 非 401）。失败一律 401（封禁期同形——不向请求方暴露封禁状态，状态
// 面保持 401/404/405 三态）；只读面纪律 `Cache-Control: no-store` 在此统一设。
func (s *Server) adminAuth(next http.HandlerFunc) http.HandlerFunc {
	want := sha256.Sum256([]byte(s.cfg.adminKey()))
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		ip := adminClientIP(r)
		now := time.Now()
		if s.adminAuthState.blocked(ip, now) {
			writeAdminError(w, http.StatusUnauthorized, "authentication_error", "invalid or missing admin key")
			return
		}
		got := sha256.Sum256([]byte(adminRequestKey(r)))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			s.adminAuthState.fail(ip, now)
			writeAdminError(w, http.StatusUnauthorized, "authentication_error", "invalid or missing admin key")
			return
		}
		s.adminAuthState.succeed(ip)
		next(w, r)
	}
}

// adminRequestKey 从请求里取管理密钥：`Authorization: Bearer <key>` 优先（scheme
// 大小写不敏感），`X-Management-Key: <key>` 兜底；都没有/其它 scheme = 空串
// （比较必失败）。绝不回显密钥。
func adminRequestKey(r *http.Request) string {
	if v := r.Header.Get("Authorization"); v != "" {
		if parts := strings.SplitN(v, " ", 2); len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			return strings.TrimSpace(parts[1])
		}
		return ""
	}
	return strings.TrimSpace(r.Header.Get("X-Management-Key"))
}

// adminClientIP 客户端 IP（RemoteAddr 的 host 段）。**不信 X-Forwarded-For**：
// 本服务建议本机监听，代理头是伪造面（封禁绕过）；解析失败用原串（宁可分组粗）。
func adminClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// adminErrorEnvelope 管理面错误信封（本面自定形状——不属于四个入口协议，不套
// claude/openai/gemini 信封）。
type adminErrorEnvelope struct {
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

// writeAdminError 写管理面错误信封（401 附 WWW-Authenticate）。
func writeAdminError(w http.ResponseWriter, status int, typ, msg string) {
	var env adminErrorEnvelope
	env.Error.Type = typ
	env.Error.Message = msg
	w.Header().Set("Content-Type", "application/json")
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="any2api-admin"`)
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(env)
}

// writeAdminJSON 写管理面 JSON 响应（200/错误信封以外的成功面）。
func writeAdminJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ---------------------------------------------------------------------------
// GET /admin/diagnostics
// ---------------------------------------------------------------------------

// adminUpstreamView 单个上游的健康快照（只读；**不含** base_url/api_key——
// 凭据面绝不进管理响应）。
type adminUpstreamView struct {
	Index                    int     `json:"index"`
	Name                     string  `json:"name"`
	Protocol                 string  `json:"protocol"`
	Cooling                  bool    `json:"cooling"`
	CooldownUntil            string  `json:"cooldown_until,omitempty"` // RFC3339（未冷却 = 省略）
	CooldownRemainingSeconds float64 `json:"cooldown_remaining_seconds"`
	ConsecutiveFailures      int     `json:"consecutive_failures"`
	LadderStep               int     `json:"ladder_step"`
	WindowRequests           int     `json:"window_requests"`
	WindowFailures           int     `json:"window_failures"`
}

// adminLogStats ring 的读面统计。
type adminLogStats struct {
	Capacity  int    `json:"capacity"`
	Count     int    `json:"count"`
	LatestSeq uint64 `json:"latest_seq"`
}

// adminDiagnosticsResponse `GET /admin/diagnostics` 响应体（诊断快照）。
type adminDiagnosticsResponse struct {
	Now         time.Time           `json:"now"`
	Diagnostics Diagnostics         `json:"diagnostics"`
	Upstreams   []adminUpstreamView `json:"upstreams"`
	Log         adminLogStats       `json:"log"`
	Admin       adminAuthView       `json:"admin"` // 鉴权面（失败计数 + 封禁 IP/到期；W5-36）
}

// adminDiagnosticsSnapshot 诊断快照（并发安全；upstream 健康逐个持锁读）。
func (s *Server) adminDiagnosticsSnapshot() adminDiagnosticsResponse {
	now := time.Now()
	count, latest := s.adminLog.stats()
	return adminDiagnosticsResponse{
		Now:         now.UTC(),
		Diagnostics: s.Diagnostics(),
		Upstreams:   s.adminUpstreamViews(now),
		Log:         adminLogStats{Capacity: s.adminLog.capacityOrDefault(), Count: count, LatestSeq: latest},
		Admin:       s.adminAuthState.snapshot(now),
	}
}

// adminUpstreamViews per-upstream 健康快照（冷却状态/失败计数/失败率窗口）。
func (s *Server) adminUpstreamViews(now time.Time) []adminUpstreamView {
	out := make([]adminUpstreamView, 0, len(s.cfg.Upstreams))
	for i := range s.cfg.Upstreams {
		v := adminUpstreamView{
			Index:    i,
			Name:     s.cfg.Upstreams[i].Name,
			Protocol: string(s.cfg.Upstreams[i].Protocol),
		}
		if i < len(s.health) {
			h := &s.health[i]
			h.mu.Lock()
			until, fails, step := h.until, h.fails, h.ladderStep
			winReq, winFail := h.winReq, h.winFail
			h.mu.Unlock()
			v.Cooling = now.Before(until)
			v.ConsecutiveFailures = fails
			v.LadderStep = step
			v.WindowRequests = winReq
			v.WindowFailures = winFail
			if !until.IsZero() {
				v.CooldownUntil = until.UTC().Format(time.RFC3339)
				if rem := until.Sub(now); rem > 0 {
					v.CooldownRemainingSeconds = rem.Seconds()
				}
			}
		}
		out = append(out, v)
	}
	return out
}

func (s *Server) handleAdminDiagnostics(w http.ResponseWriter, r *http.Request) {
	writeAdminJSON(w, http.StatusOK, s.adminDiagnosticsSnapshot())
}

// ---------------------------------------------------------------------------
// POST /admin/cooldowns/reset
// ---------------------------------------------------------------------------

// adminResetRequest 复位请求体：三选一（`all: true` / `upstream: <名>` /
// `index: <下标>`；多给 = 400 歧义，都不给 = 400 缺参——CPA 对偶
// `POST /routing/cooldown/reset` {auth_index} 的「必填参数」纪律）。
type adminResetRequest struct {
	All      bool   `json:"all"`
	Upstream string `json:"upstream"`
	Index    *int   `json:"index"`
}

// adminResetResult 单个上游的复位结果（`was_cooling` 使幂等可观测：重复复位
// 第二次起为 false）。
type adminResetResult struct {
	Index      int    `json:"index"`
	Name       string `json:"name"`
	WasCooling bool   `json:"was_cooling"`
}

// adminResetResponse `POST /admin/cooldowns/reset` 响应体。
type adminResetResponse struct {
	Status string             `json:"status"`
	Count  int                `json:"count"`
	Reset  []adminResetResult `json:"reset"`
}

// resetCooldown 复位该上游的健康状态：清冷却截止 + 连续失败计数 + 阶梯档位
// （等价 recordSuccess 的复位面；**不动失败率窗口**——时间窗自会过期，复位是
// 人工信号，不伪造观测）。返回复位前是否在冷却窗口内。幂等。
func (s *Server) resetCooldown(idx int) (wasCooling bool) {
	h := &s.health[idx]
	now := time.Now()
	h.mu.Lock()
	wasCooling = now.Before(h.until)
	h.until = time.Time{}
	h.fails = 0
	h.ladderStep = 0
	h.mu.Unlock()
	return wasCooling
}

func (s *Server) handleAdminCooldownsReset(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, adminMaxBodyBytes))
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "request body too large or unreadable")
		return
	}
	var req adminResetRequest
	if len(strings.TrimSpace(string(body))) > 0 {
		dec := json.NewDecoder(strings.NewReader(string(body)))
		dec.DisallowUnknownFields() // 错字不静默吞（`upstreem` 必须报错，绝不空复位）
		if err := dec.Decode(&req); err != nil {
			writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "invalid JSON body: "+err.Error())
			return
		}
	}

	var targets []int
	switch {
	case req.All && (req.Upstream != "" || req.Index != nil),
		req.Upstream != "" && req.Index != nil:
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error",
			"ambiguous target: provide exactly one of all/upstream/index")
		return
	case req.All:
		targets = make([]int, 0, len(s.cfg.Upstreams))
		for i := range s.cfg.Upstreams {
			targets = append(targets, i)
		}
	case req.Upstream != "":
		idx := -1
		for i := range s.cfg.Upstreams {
			if s.cfg.Upstreams[i].Name == req.Upstream {
				idx = i
				break
			}
		}
		if idx < 0 {
			writeAdminError(w, http.StatusNotFound, "not_found_error",
				fmt.Sprintf("unknown upstream %q", req.Upstream))
			return
		}
		targets = []int{idx}
	case req.Index != nil:
		if *req.Index < 0 || *req.Index >= len(s.cfg.Upstreams) {
			writeAdminError(w, http.StatusNotFound, "not_found_error",
				fmt.Sprintf("upstream index %d out of range (0..%d)", *req.Index, len(s.cfg.Upstreams)-1))
			return
		}
		targets = []int{*req.Index}
	default:
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error",
			"missing target: provide one of all/upstream/index")
		return
	}

	resp := adminResetResponse{Status: "ok", Reset: make([]adminResetResult, 0, len(targets))}
	for _, idx := range targets {
		wasCooling := s.resetCooldown(idx)
		name := s.cfg.Upstreams[idx].Name
		resp.Reset = append(resp.Reset, adminResetResult{Index: idx, Name: name, WasCooling: wasCooling})
		s.adminLog.add(adminLogCooldownReset, name, "",
			fmt.Sprintf("cooldown reset via admin API (was_cooling=%v)", wasCooling), 0)
	}
	resp.Count = len(resp.Reset)
	writeAdminJSON(w, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// GET /admin/logs
// ---------------------------------------------------------------------------

// adminLogsResponse `GET /admin/logs` 响应体。
//
// 顺序契约：缺省读（无 since_id）= 新→旧（W5-33 不变）；`since_id` 增量读 = 升序
// （按发生时序消费）。`next_cursor` 恒非空（不透明令牌，可直接作为下次 `since_id`）；
// `cursor_reset` 仅在锚失效时出现（见「日志游标」节的三因）。
type adminLogsResponse struct {
	Entries     []adminLogEntry `json:"entries"`
	Capacity    int             `json:"capacity"`
	Count       int             `json:"count"`
	LatestSeq   uint64          `json:"latest_seq"`
	NextCursor  string          `json:"next_cursor"`
	CursorReset bool            `json:"cursor_reset,omitempty"`
}

func (s *Server) handleAdminLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	capacity := s.adminLog.capacityOrDefault()
	limit := adminLogDefaultLimit
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "limit must be a positive integer")
			return
		}
		limit = n
		if limit > capacity {
			limit = capacity // 上限 = ring 容量（物理上限）
		}
	}
	var (
		anchor      uint64
		anchorFP    string
		incremental bool
	)
	if raw := q.Get("since_id"); raw != "" {
		seq, fp, err := adminLogAnchor(raw)
		if err != nil {
			writeAdminError(w, http.StatusBadRequest, "invalid_request_error",
				"since_id must be a non-negative integer seq or a next_cursor token: "+err.Error())
			return
		}
		anchor, anchorFP, incremental = seq, fp, true
	}
	win := s.adminLog.window(anchor, anchorFP, limit, incremental)
	writeAdminJSON(w, http.StatusOK, adminLogsResponse{
		Entries:   win.Entries,
		Capacity:  capacity,
		Count:     win.Count,
		LatestSeq: win.LatestSeq,
		NextCursor: encodeAdminLogCursor(adminLogCursor{
			Version: adminLogCursorVersion,
			Seq:     win.NextSeq,
			FP:      s.adminLog.fingerprint(),
		}),
		CursorReset: win.Reset,
	})
}
