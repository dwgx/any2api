// admin_accounts.go —— 账号 / 代理清单 + 安全下线（board A2A-L19-W78-1；设计
// `docs/admin-dashboard.md` §5.2「上游与池」· §6.1 #11/#16 · §7.2「零凭据字节」·
// §7.4「破坏性动作」）。
//
// 端点（全部挂 adminAuth；未配 admin_key = 路由根本不注册 → 404 非 401）：
//
//	GET  /admin/accounts                              上游 × 凭据池 × 代理清单
//	GET  /admin/proxies                               逐上游代理行（含未配置行）
//	POST /admin/accounts/{upstream}/{index}/cooldown  安全下线（临时冷却；零配置写）
//
// 数据源（**零新计数器、零出网、零配置写**）：
//   - 上游面 = `Config.Upstreams`（启动期静态；`index` = 配置序，与 /admin/pool 同源）；
//   - 池面 = `Config.credentialPoolAt(i).adminSnapshot(now)`（与 /admin/pool **同一
//     份状态、同一形状**——本面不新增字段，也不改既有字段）；
//   - 代理面 = `Upstream.Proxy` 过 `parseUpstreamProxy`（只读；与出站构造、启动期
//     Validate 同一判据）。
//
// 纪律：
//   - **零凭据字节**：池只回下标与状态；base_url 过 `adminSanitizeURL`（去
//     userinfo / query / fragment，解析失败回 `invalid` 不回原文）；代理只回
//     scheme / host / port + `display`（= `scheme://host[:port]`，由 `u.Host` 构造
//     ——`url.URL` 的 Host 天然不含 userinfo）+ `auth` 布尔。**任何一处都不回
//     api_key / credentials / cookie / Authorization / 管理密钥**。
//   - **零配置写**：安全下线只写 `credentialPool.coolUntil[i]`（进程内内存态）——
//     重启即清；`config.yaml` / store 一行不动。语义 = 让该凭据在冷却窗内不可选
//     （`selectCredential` 既有判据），等价于「临时停用」而不是「删账号」。
//   - **绝不缩短已有冷却**：取较晚者（与 `reportFailure` 同纪律）——人工下线不该
//     把一次更长的失败冷却改短。
//   - 审计：只落 `account_cooldown` 一行（detail 只含 upstream/index/seconds/reason，
//     零凭据字节）；store 未启用 = no-op（`auditStore` 既有语义）。
package service

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// storeAuditKindAccountCooldown 审计行型别：管理面安全下线（本面自定；与
	// session_revoke / request 并列）。
	storeAuditKindAccountCooldown = "account_cooldown"
	// adminCooldownSecondsMin / adminCooldownSecondsMax 安全下线的时长边界
	// （秒）：1s（= 显式「立刻放行」的最小窗）… 24h（= 人工下线的上限——更长
	// 的停用应改配置，不是按一次按钮）。
	adminCooldownSecondsMin = 1
	adminCooldownSecondsMax = 86400
	// adminCooldownReasonMax 下线原因上限（**字符数**，按 rune 计——中文原因按字
	// 算，不按字节算）。
	adminCooldownReasonMax = 120
	// adminCooldownMaxBody 下线请求体上限（seconds + reason + 留白；4KB 足够）。
	adminCooldownMaxBody = 4 << 10
)

// ---------------------------------------------------------------------------
// 路由注册
// ---------------------------------------------------------------------------

// registerAdminAccounts 注册账号/代理清单 + 安全下线三条端点。**自门控**：未配
// admin_key 时一条都不注册（与 registerAdminAPI / 既有 /admin/* 逐字一致：404 非
// 401）。调用点 = `Server.Handler()` 的 admin_key 分支。
func (s *Server) registerAdminAccounts(mux *http.ServeMux) {
	if s == nil || s.cfg.adminKey() == "" {
		return
	}
	mux.HandleFunc("GET /admin/accounts", s.adminAuth(s.handleAdminAccounts))
	mux.HandleFunc("GET /admin/proxies", s.adminAuth(s.handleAdminProxies))
	mux.HandleFunc("POST /admin/accounts/{upstream}/{index}/cooldown", s.adminAuth(s.handleAdminAccountCooldown))
}

// ---------------------------------------------------------------------------
// 代理投影（accounts / proxies / export 三面共用）
// ---------------------------------------------------------------------------

// adminProxyView 单上游代理的投影（**零凭据字节**：userinfo 只化为 `auth` 布尔）。
// 未配置（`Upstream.Proxy` 空）= 全零值对象（`configured:false`）。
type adminProxyView struct {
	Configured bool   `json:"configured"`
	Scheme     string `json:"scheme"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	// Auth userinfo 是否存在（`scheme://user:pass@host:port`）——**只回布尔**，
	// 用户名/口令绝不回。
	Auth bool `json:"auth"`
	// Display `scheme://host[:port]`（零 userinfo；端口缺省 = 只回 host）。
	Display string `json:"display"`
}

// adminProxyViewFor 代理投影（**只读**：`parseUpstreamProxy` 与启动期 Validate /
// 出站构造同一判据；不建连接、不改配置）。非法/空白值 = 未配置（**绝不回原文**
// ——原文可能自带 userinfo 凭据）。出口再过 `redactAdminText`（纵深防御：主机名
// 里的 `sk-…` 形态同样投影掉）。
func adminProxyViewFor(raw string) adminProxyView {
	if strings.TrimSpace(raw) == "" {
		return adminProxyView{}
	}
	u, err := parseUpstreamProxy(raw)
	if err != nil || u == nil {
		return adminProxyView{}
	}
	v := adminProxyView{
		Configured: true,
		Scheme:     u.Scheme,
		Host:       redactAdminText(u.Hostname()),
		Auth:       u.User != nil,
		// url.URL.Host = host[:port]，**不含** userinfo（凭据在 u.User 里）。
		Display: redactAdminText(u.Scheme + "://" + u.Host),
	}
	if p := u.Port(); p != "" {
		if n, err := strconv.Atoi(p); err == nil {
			v.Port = n
		}
	}
	return v
}

// ---------------------------------------------------------------------------
// GET /admin/accounts
// ---------------------------------------------------------------------------

// adminAccountsUpstreamView 单上游的账号清单行（上游元信息 + 代理 + 池）。
type adminAccountsUpstreamView struct {
	Index int    `json:"index"`
	Name  string `json:"name"`
	// Protocol 上游协议（string(Protocol)——与 /admin/pool 同形）。
	Protocol string `json:"protocol"`
	// BaseURL 配置里的 base_url **消毒后**的形态（去 userinfo/query/fragment；
	// 未配置 = ""）。运行时有效基址（web 协议的缺省 https://claude.ai /
	// https://android.chat.openai.com）见 `/admin/upstreams/{name}/probe` 与
	// `/admin/config` 的口径登记（本字段 = 配置值，不发明缺省）。
	BaseURL        string         `json:"base_url"`
	Proxy          adminProxyView `json:"proxy"`
	TLSFingerprint string         `json:"tls_fingerprint"`
	PoolSize       int            `json:"pool_size"`
	// Credentials 池快照（与 `/admin/pool` 的 credentials[] **同一形状**：只回
	// 下标与状态，零凭据字节）。
	Credentials []adminPoolCredentialView `json:"credentials"`
}

// adminAccountsResponse `GET /admin/accounts` 响应体。
type adminAccountsResponse struct {
	Upstreams []adminAccountsUpstreamView `json:"upstreams"`
	Now       int64                       `json:"now"`
}

// adminAccountsSnapshot 账号清单快照（upstream 序 = `Config.Upstreams`；池未初始化
// 时惰性建池——与 /admin/pool 同一路径，空池 = `credentials: []` 不是伪造数据）。
func (s *Server) adminAccountsSnapshot(now time.Time) adminAccountsResponse {
	resp := adminAccountsResponse{
		Upstreams: make([]adminAccountsUpstreamView, 0, len(s.cfg.Upstreams)),
		Now:       now.UnixMilli(),
	}
	for i := range s.cfg.Upstreams {
		u := s.cfg.Upstreams[i]
		size, creds := s.cfg.credentialPoolAt(i).adminSnapshot(now)
		resp.Upstreams = append(resp.Upstreams, adminAccountsUpstreamView{
			Index:          i,
			Name:           u.Name,
			Protocol:       string(u.Protocol),
			BaseURL:        adminSanitizeURL(u.BaseURL),
			Proxy:          adminProxyViewFor(u.Proxy),
			TLSFingerprint: u.tlsFingerprintMode(),
			PoolSize:       size,
			Credentials:    creds,
		})
	}
	return resp
}

// handleAdminAccounts 账号清单：上游 × 池 × 代理（**零凭据字节**；只读）。
func (s *Server) handleAdminAccounts(w http.ResponseWriter, r *http.Request) {
	writeAdminJSON(w, http.StatusOK, s.adminAccountsSnapshot(time.Now()))
}

// ---------------------------------------------------------------------------
// GET /admin/proxies
// ---------------------------------------------------------------------------

// adminProxiesRowView 单上游代理行（**含未配置行**——`configured:false` 的行也是
// 信息：运营者要能一眼看出哪台上游直连）。嵌入的 `adminProxyView` 由
// encoding/json 就地展开（字段即扁平 JSON 键）。
type adminProxiesRowView struct {
	Upstream string `json:"upstream"`
	Index    int    `json:"index"`
	adminProxyView
	TLSFingerprint string `json:"tls_fingerprint"`
}

// adminProxiesResponse `GET /admin/proxies` 响应体。
type adminProxiesResponse struct {
	Proxies []adminProxiesRowView `json:"proxies"`
	Now     int64                 `json:"now"`
}

// adminProxiesSnapshot 代理清单快照（逐上游一行，含未配置行；序 = 配置序）。
func (s *Server) adminProxiesSnapshot(now time.Time) adminProxiesResponse {
	resp := adminProxiesResponse{
		Proxies: make([]adminProxiesRowView, 0, len(s.cfg.Upstreams)),
		Now:     now.UnixMilli(),
	}
	for i := range s.cfg.Upstreams {
		u := s.cfg.Upstreams[i]
		resp.Proxies = append(resp.Proxies, adminProxiesRowView{
			Upstream:       u.Name,
			Index:          i,
			adminProxyView: adminProxyViewFor(u.Proxy),
			TLSFingerprint: u.tlsFingerprintMode(),
		})
	}
	return resp
}

// handleAdminProxies 代理清单（只读；零凭据字节——`display` 恒 `scheme://host[:port]`）。
func (s *Server) handleAdminProxies(w http.ResponseWriter, r *http.Request) {
	writeAdminJSON(w, http.StatusOK, s.adminProxiesSnapshot(time.Now()))
}

// ---------------------------------------------------------------------------
// POST /admin/accounts/{upstream}/{index}/cooldown
// ---------------------------------------------------------------------------

// adminCooldownRequest 安全下线请求体（`seconds` 必填；`reason` 可选）。
type adminCooldownRequest struct {
	Seconds int    `json:"seconds"`
	Reason  string `json:"reason"`
}

// adminCooldownResponse `POST /admin/accounts/{upstream}/{index}/cooldown` 响应体
// （**生效后的真实状态**——若该凭据已在更长的冷却窗内，回的是那个更晚的时刻，
// 不是请求值）。
type adminCooldownResponse struct {
	Upstream                 string `json:"upstream"`
	Index                    int    `json:"index"`
	Cooling                  bool   `json:"cooling"`
	CooldownUntil            int64  `json:"cooldown_until"`
	CooldownRemainingSeconds int    `json:"cooldown_remaining_seconds"`
	Now                      int64  `json:"now"`
}

// adminSetCooldown 安全下线：把第 i 个凭据的冷却截止推到 `until`（**绝不缩短**已有
// 冷却——取较晚者，与 `reportFailure` 同纪律）。**只写 `coolUntil[i]`**：零凭据
// 字节、零配置写、零 store 写（进程内内存态，重启即清）。越界 / 空池 / 池未初始化
// = false（调用方回 404）。
func (p *credentialPool) adminSetCooldown(i int, until time.Time) bool {
	if p == nil || i < 0 || i >= len(p.creds) {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if i >= len(p.coolUntil) {
		return false
	}
	if until.After(p.coolUntil[i]) {
		p.coolUntil[i] = until
	}
	return true
}

// adminCooldownReasonInvalid 下线原因校验：≤120 字符（按 rune 计）+ 无控制字符。
// 返回空串 = 合法。（控制字符会污染日志/审计的形态；原因进审计 detail，形态守卫
// 在入口处做。）
func adminCooldownReasonInvalid(reason string) string {
	if utf8.RuneCountInString(reason) > adminCooldownReasonMax {
		return "reason must be <= 120 characters"
	}
	for _, r := range reason {
		if r < 0x20 || r == 0x7f {
			return "reason must not contain control characters"
		}
	}
	return ""
}

// handleAdminAccountCooldown 安全下线（按上游名 + 池下标）。语义与错误面：
//
//   - 上游按 **Name 首匹配**（重名配置取第一条；配置重名本身由 Validate 之外的
//     运营纪律兜底）→ 找不到 = 404；
//   - 池下标越界 / 空池 / 无池 = 404（**不是 400**：下标是资源路径，不是请求参数）；
//   - `seconds` 1..86400（缺 / 0 / 越界 = 400）；`reason` ≤120 字符、无控制字符；
//   - 审计只落 upstream/index/seconds/reason（零凭据字节）；store 未启用 = no-op。
func (s *Server) handleAdminAccountCooldown(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("upstream")
	i, err := strconv.Atoi(r.PathValue("index"))
	if err != nil || i < 0 {
		writeAdminError(w, http.StatusNotFound, "not_found", "no such credential index")
		return
	}
	upIdx := -1
	for j := range s.cfg.Upstreams {
		if s.cfg.Upstreams[j].Name == name {
			upIdx = j
			break
		}
	}
	if upIdx < 0 {
		writeAdminError(w, http.StatusNotFound, "not_found", "no such upstream")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, adminCooldownMaxBody)
	var req adminCooldownRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "invalid JSON body")
		return
	}
	if req.Seconds < adminCooldownSecondsMin || req.Seconds > adminCooldownSecondsMax {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "seconds must be an integer in 1..86400")
		return
	}
	if msg := adminCooldownReasonInvalid(req.Reason); msg != "" {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", msg)
		return
	}
	pool := s.cfg.credentialPoolAt(upIdx)
	if pool.size() == 0 || i >= pool.size() {
		writeAdminError(w, http.StatusNotFound, "not_found", "no such credential index")
		return
	}
	now := time.Now()
	if !pool.adminSetCooldown(i, now.Add(time.Duration(req.Seconds)*time.Second)) {
		writeAdminError(w, http.StatusNotFound, "not_found", "no such credential index")
		return
	}
	// 响应 = **生效后的真实状态**（取快照，不回声请求值——已有更长冷却时为更长者）。
	resp := adminCooldownResponse{Upstream: name, Index: i, Now: now.UnixMilli()}
	_, creds := pool.adminSnapshot(now)
	for _, c := range creds {
		if c.Index == i {
			resp.Cooling = c.Cooling
			resp.CooldownUntil = c.CooldownUntil
			resp.CooldownRemainingSeconds = c.CooldownRemainingSeconds
			break
		}
	}
	// 审计（detail 只含 upstream/index/seconds/reason；失败只 log/ring——store
	// 未启用 = no-op）。
	if detail, err := json.Marshal(map[string]any{
		"upstream": name,
		"index":    i,
		"seconds":  req.Seconds,
		"reason":   req.Reason,
	}); err == nil {
		s.auditStore(storeAudit{
			TS:     now,
			Kind:   storeAuditKindAccountCooldown,
			Detail: string(detail),
		})
	}
	writeAdminJSON(w, http.StatusOK, resp)
}
