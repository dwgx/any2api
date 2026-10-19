// admin_providers.go —— 上游探针 + 供应商目录（board A2A-L19-W78-1；设计
// `docs/admin-dashboard.md` §5.2「测试上游」动作 · §6.1 #16 · §7.2）。
//
// 端点（全部挂 adminAuth；未配 admin_key = 路由根本不注册 → 404 非 401）：
//
//	POST /admin/upstreams/{name}/probe   单上游轻探针（1 发只读 GET；不占号池）
//	GET  /admin/providers                静态供应商目录 + 配置/探针就绪态
//
// 探针语义（设计 §5.2 的「只发 1 次轻请求且不占号池配额」）：
//   - **只读 GET**：`/v1/models`（API 协议）/ `/v1beta/models`（gemini）/ 根路径
//     （web 协议）——**绝不**发 completion、绝不建会话、绝不花 token；
//   - 超时 5s（`adminProbeTimeout`；测试可覆写）、**1 发**、无重试；跨主机重定向
//     拒绝（认证头绝不跟到别的 host）；
//   - 认证头（仅 API 协议；值来自配置，**绝不落日志/响应/审计**）：anthropic =
//     `x-api-key` + `anthropic-version: 2023-06-01`；gemini = `x-goog-api-key`；
//     openai_chat / responses / interactions = `Authorization: Bearer`；
//   - web 协议（claude_web / chatgpt_web）= 根路径裸 GET（**不带 cookie/凭据**）——
//     只探可达性（`auth:"unknown"`；CF 盾可能回 403，那是 `reachable` 面不是错误）。
//
// 结果表（`Server.adminProbes`）：**有界**（键 = 上游名，容量 = 上游数；只增新键
// 时才逐出最旧）——`/admin/providers` 的 `readiness` 由它派生；**进程内内存态、
// 重启清零、绝不持久化**（探针结果是运行观测，不是配置）。
//
// 供应商目录：**静态声明**（协议常量 + 既有 KB/转换表；capabilities 见报告
// `[INFERENCE]` 标记——不是运行时探测结果）。`configured` 由配置派生，
// `readiness` 由探针结果派生；`not_configured` 时仍返回条目（UI 要能显示
// 「这台协议本服务支持、只是你没配」）。
package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	// adminProbeModeModels 探针模式词表（当前唯一取值；其余 = 400）。
	adminProbeModeModels = "models"
	// adminProbeMaxBody 探针请求体上限（可选 mode 字段；1KB 足够）。
	adminProbeMaxBody = 1 << 10
	// adminProbeModelPath 模型列表路径（API 协议）。
	adminProbeModelPath = "/models"
	// adminProbeDrainBytes 响应体最多读这么多字节再关（复用连接用；**绝不回显**）。
	adminProbeDrainBytes = 4 << 10
	// adminProbeRedirectMax 同主机重定向最多跟随的跳数（跨主机恒拒绝）。
	adminProbeRedirectMax = 2

	// 探针 auth 三值（设计口径：2xx/3xx + 带凭据 = verified；401/403 = rejected；
	// 未带凭据（无凭据上游 / web 协议根面）= unknown）。
	adminProbeAuthVerified = "verified"
	adminProbeAuthRejected = "rejected"
	adminProbeAuthUnknown  = "unknown"

	// 探针 error_class 词表（error_class 只有这五值 + 空串；**绝不回响应体**）。
	adminProbeErrorTimeout = "timeout"
	adminProbeErrorNetwork = "network"
	adminProbeErrorTLS     = "tls"
	adminProbeErrorHTTP4xx = "http_4xx"
	adminProbeErrorHTTP5xx = "http_5xx"

	// 供应商就绪态词表（设计 §5.2/§6.1）。
	adminProviderNotConfigured = "not_configured"
	adminProviderConfigured    = "configured"
	adminProviderReady         = "ready"
	adminProviderDegraded      = "degraded"
)

// adminProbeTimeout 探针整体超时（连接 + 响应头 + 读尽小体）。**变量**（非 const）
// 的唯一理由 = 测试注入（真机恒 5s；见报告「残余风险」）。
var adminProbeTimeout = 5 * time.Second

// ---------------------------------------------------------------------------
// 路由注册
// ---------------------------------------------------------------------------

// registerAdminProviders 注册探针 + 供应商目录两条端点。**自门控**：未配
// admin_key 时一条都不注册（404 非 401）。调用点 = `Server.Handler()` 的
// admin_key 分支。
func (s *Server) registerAdminProviders(mux *http.ServeMux) {
	if s == nil || s.cfg.adminKey() == "" {
		return
	}
	mux.HandleFunc("POST /admin/upstreams/{name}/probe", s.adminAuth(s.handleAdminUpstreamProbe))
	mux.HandleFunc("GET /admin/providers", s.adminAuth(s.handleAdminProviders))
}

// ---------------------------------------------------------------------------
// 探针结果表（有界内存；键 = 上游名）
// ---------------------------------------------------------------------------

// adminProbeResult 一次探针的结果（**同时是响应体**——字段即契约）。
type adminProbeResult struct {
	Upstream  string `json:"upstream"`
	Protocol  string `json:"protocol"`
	Mode      string `json:"mode"`
	OK        bool   `json:"ok"`
	Reachable bool   `json:"reachable"`
	// Status 上游 HTTP 状态（0 = 未收到响应头）。
	Status     int    `json:"status"`
	LatencyMS  int64  `json:"latency_ms"`
	Auth       string `json:"auth"`
	ErrorClass string `json:"error_class"`
	CheckedAt  int64  `json:"checked_at"`
}

// adminProbeStore 探针结果表（**有界**：键 = 上游名；`set` 收到新键且已满时逐出
// 最旧一行——容量由调用方按上游数给）。零值可用；重启清零。
type adminProbeStore struct {
	mu   sync.Mutex
	rows map[string]adminProbeResult
}

// set 写一行（新键且已满 → 逐出 checked_at 最小者）。
func (p *adminProbeStore) set(res adminProbeResult, capacity int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rows == nil {
		p.rows = make(map[string]adminProbeResult)
	}
	if _, exists := p.rows[res.Upstream]; !exists && capacity > 0 && len(p.rows) >= capacity {
		oldest := ""
		for name, row := range p.rows {
			if oldest == "" || row.CheckedAt < p.rows[oldest].CheckedAt {
				oldest = name
			}
		}
		if oldest != "" {
			delete(p.rows, oldest)
		}
	}
	p.rows[res.Upstream] = res
}

// get 读一行（未命中 = false）。
func (p *adminProbeStore) get(name string) (adminProbeResult, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	row, ok := p.rows[name]
	return row, ok
}

// ---------------------------------------------------------------------------
// POST /admin/upstreams/{name}/probe
// ---------------------------------------------------------------------------

// adminProbeTarget 探针目标（URL + 认证头）。**零凭据回声**：HeaderValue 只在
// 本次出站请求里用；调用方绝不落日志/响应/审计。
type adminProbeTarget struct {
	URL string
	// HeaderName / HeaderValue 认证头（HeaderName 空 = 不带认证头行）。
	HeaderName  string
	HeaderValue string
	// CredentialSent HeaderName 非空（= 带凭据发请求）——auth 三值的判据。
	CredentialSent bool
}

// adminProbeURL 探针路径（设计 §6.1 #16 的三种形态）：
//
//	openai_chat|responses|interactions|anthropic → `<base>/v1/models`
//	    （base 已以 `/v1` 结尾时用 `<base>/models`）；
//	gemini → `<base>/v1beta/models`（base 已含 `/v1beta` 或 `/v1` 时追加 `/models`）；
//	claude_web|chatgpt_web → 根路径 `/`（**有效基址**：config 缺省 https://claude.ai /
//	    https://android.chat.openai.com，与请求面同一 helper）。
//
// ok=false = 该上游没有可用基址（web 协议恒有缺省；其余协议 Validate 已挡空值——
// 防御面）。
func adminProbeURL(up Upstream) (string, bool) {
	switch up.Protocol {
	case ProtocolClaudeWeb:
		return claudeWebBase(up) + "/", true
	case ProtocolChatGPTWeb:
		return chatgptWebBase(up) + "/", true
	}
	base := strings.TrimRight(up.BaseURL, "/")
	if base == "" {
		return "", false
	}
	if up.Protocol == ProtocolGemini {
		if strings.Contains(base, "/v1beta") || strings.Contains(base, "/v1") {
			return base + adminProbeModelPath, true
		}
		return base + "/v1beta" + adminProbeModelPath, true
	}
	if strings.HasSuffix(base, "/v1") {
		return base + adminProbeModelPath, true
	}
	return base + "/v1" + adminProbeModelPath, true
}

// adminProbeTargetFor 目标装配（URL + 认证头）。凭据取配置里的**第一条**
// （`upstreamCredentials`：`credentials[0]` 或 `api_key`；web 协议恒不带凭据）。
func adminProbeTargetFor(up Upstream) (adminProbeTarget, bool) {
	url, ok := adminProbeURL(up)
	if !ok {
		return adminProbeTarget{}, false
	}
	t := adminProbeTarget{URL: url}
	if up.Protocol == ProtocolClaudeWeb || up.Protocol == ProtocolChatGPTWeb {
		return t, true // 根面：只探可达性，不带任何凭据
	}
	creds := upstreamCredentials(up)
	cred := ""
	if len(creds) > 0 {
		cred = creds[0]
	}
	if cred == "" {
		return t, true // 无凭据 = 不发认证头（auth 恒 unknown）
	}
	switch up.Protocol {
	case ProtocolAnthropic:
		t.HeaderName, t.HeaderValue = "x-api-key", cred
	case ProtocolGemini:
		t.HeaderName, t.HeaderValue = "x-goog-api-key", cred
	default: // openai_chat | responses | interactions
		t.HeaderName, t.HeaderValue = "Authorization", "Bearer "+cred
	}
	t.CredentialSent = true
	return t, true
}

// adminProbeErrorClass 出站错误 → 词表三值（timeout|network|tls；其余归 network）。
func adminProbeErrorClass(err error) string {
	if err == nil {
		return ""
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return adminProbeErrorTimeout
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return adminProbeErrorTimeout
	}
	var certVerify *tls.CertificateVerificationError
	if errors.As(err, &certVerify) {
		return adminProbeErrorTLS
	}
	var unknownAuth x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var certInvalid x509.CertificateInvalidError
	var roots x509.SystemRootsError
	var recordHeader tls.RecordHeaderError
	if errors.As(err, &unknownAuth) || errors.As(err, &hostname) || errors.As(err, &certInvalid) ||
		errors.As(err, &roots) || errors.As(err, &recordHeader) {
		return adminProbeErrorTLS
	}
	return adminProbeErrorNetwork
}

// adminProbeCheckRedirect 重定向纪律：**跨主机恒拒绝**（认证头绝不跟到别的
// host），同主机最多跟 `adminProbeRedirectMax` 跳（`http.ErrUseLastResponse` =
// 不跟随但把当前响应交回调用方——探针看到的就是那一跳的状态码）。
func adminProbeCheckRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= adminProbeRedirectMax {
		return http.ErrUseLastResponse
	}
	if len(via) > 0 && req.URL.Host != via[0].URL.Host {
		return http.ErrUseLastResponse
	}
	return nil
}

// probeUpstream 执行一次探针（**1 发、无重试**；只读 GET）。出站客户端复用该上游
// 的 egress（per-upstream 代理 / TLS 指纹，见 outbound.go `clientFor`）——探针走
// 与请求面同一条出站链；整体超时 = adminProbeTimeout。
func (s *Server) probeUpstream(idx int, now time.Time) adminProbeResult {
	up := s.cfg.Upstreams[idx]
	res := adminProbeResult{
		Upstream:  up.Name,
		Protocol:  string(up.Protocol),
		Mode:      adminProbeModeModels,
		Auth:      adminProbeAuthUnknown,
		CheckedAt: now.UnixMilli(),
	}
	target, ok := adminProbeTargetFor(up)
	if !ok {
		res.ErrorClass = adminProbeErrorNetwork
		return res
	}
	req, err := http.NewRequest(http.MethodGet, target.URL, nil)
	if err != nil {
		res.ErrorClass = adminProbeErrorNetwork
		return res
	}
	if target.HeaderName != "" {
		req.Header.Set(target.HeaderName, target.HeaderValue)
	}
	if up.Protocol == ProtocolAnthropic {
		// A2 硬门（与请求面同源）：anthropic 上游恒带版本头（非凭据）。
		req.Header.Set("anthropic-version", "2023-06-01")
	}
	base, err := s.clientFor(idx)
	if err != nil {
		res.ErrorClass = adminProbeErrorNetwork
		return res
	}
	transport := base.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	client := &http.Client{
		Transport:     transport,
		Timeout:       adminProbeTimeout,
		CheckRedirect: adminProbeCheckRedirect,
	}
	start := time.Now()
	resp, err := client.Do(req)
	res.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		res.ErrorClass = adminProbeErrorClass(err)
		return res
	}
	// 只读尽小体（复用连接）；**绝不回显**。
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, adminProbeDrainBytes))
	_ = resp.Body.Close()
	res.Reachable = true
	res.Status = resp.StatusCode
	res.OK = resp.StatusCode < 400
	switch {
	case resp.StatusCode < 400:
		res.ErrorClass = ""
	case resp.StatusCode < 500:
		res.ErrorClass = adminProbeErrorHTTP4xx
	default:
		res.ErrorClass = adminProbeErrorHTTP5xx
	}
	if target.CredentialSent {
		switch {
		case resp.StatusCode < 400:
			res.Auth = adminProbeAuthVerified
		case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
			res.Auth = adminProbeAuthRejected
		}
	}
	return res
}

// handleAdminUpstreamProbe 单上游探针：`{name}` = 上游名（**首匹配**）；可选请求体
// `{"mode":"models"}`（其余取值 400）；未知上游 404。响应 = 探针结果（只记
// 状态/时延/分类——上游响应体、头面、URL 查询串一律不回）。
func (s *Server) handleAdminUpstreamProbe(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	idx := -1
	for i := range s.cfg.Upstreams {
		if s.cfg.Upstreams[i].Name == name {
			idx = i
			break
		}
	}
	if idx < 0 {
		writeAdminError(w, http.StatusNotFound, "not_found", "no such upstream")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, adminProbeMaxBody)
	var req struct {
		Mode string `json:"mode"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "invalid JSON body")
		return
	}
	if req.Mode != "" && req.Mode != adminProbeModeModels {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error", "mode must be models")
		return
	}
	res := s.probeUpstream(idx, time.Now())
	s.adminProbes.set(res, len(s.cfg.Upstreams))
	writeAdminJSON(w, http.StatusOK, res)
}

// ---------------------------------------------------------------------------
// GET /admin/providers
// ---------------------------------------------------------------------------

// adminProviderCatalogEntry 静态供应商条目（**声明式**：协议词表 + 能力词表 +
// KB 指针；不含端点/凭据）。
type adminProviderCatalogEntry struct {
	ID           string
	Label        string
	Protocols    []Protocol
	Capabilities []string
	Docs         string
}

// adminProviderCatalog 静态目录（≥7 条：六个协议族 + generic 兜底）。能力词表是
// **声明**（协议常量 + 既有转换表/KB 覆盖），不是运行时探测结果——见报告
// `[INFERENCE]` 标记。
var adminProviderCatalog = []adminProviderCatalogEntry{
	{
		ID:           "openai",
		Label:        "OpenAI",
		Protocols:    []Protocol{ProtocolOpenAIChat, ProtocolResponses},
		Capabilities: []string{"chat", "stream", "tools"},
		Docs:         "docs/service.md",
	},
	{
		ID:           "anthropic",
		Label:        "Anthropic",
		Protocols:    []Protocol{ProtocolAnthropic},
		Capabilities: []string{"chat", "stream", "tools", "thinking", "count_tokens"},
		Docs:         "docs/service.md",
	},
	{
		ID:           "gemini",
		Label:        "Google Gemini",
		Protocols:    []Protocol{ProtocolGemini},
		Capabilities: []string{"chat", "stream", "tools", "vision"},
		Docs:         "docs/architecture.md",
	},
	{
		ID:           "interactions",
		Label:        "Gemini Interactions",
		Protocols:    []Protocol{ProtocolInteractions},
		Capabilities: []string{"chat", "stream"},
		Docs:         "docs/architecture.md",
	},
	{
		ID:           "claude_web",
		Label:        "Claude Web (web2api)",
		Protocols:    []Protocol{ProtocolClaudeWeb},
		Capabilities: []string{"chat", "stream", "tools"},
		Docs:         "docs/architecture.md",
	},
	{
		ID:           "chatgpt_web",
		Label:        "ChatGPT Web (web2api)",
		Protocols:    []Protocol{ProtocolChatGPTWeb},
		Capabilities: []string{"chat", "stream"},
		Docs:         "docs/architecture.md",
	},
	{
		// generic 兜底：协议表为空（当前七个协议常量已被上面六条全覆盖）——
		// 未来新增协议时在这里显式登记，而不是让某个条目「碰巧」接住。
		ID:           "generic",
		Label:        "Generic (OpenAI-compatible)",
		Protocols:    []Protocol{},
		Capabilities: []string{},
		Docs:         "docs/service.md",
	},
}

// adminProviderView `GET /admin/providers` 的单条目（静态声明 + 配置/探针派生）。
type adminProviderView struct {
	ID           string   `json:"id"`
	Label        string   `json:"label"`
	Protocols    []string `json:"protocols"`
	Configured   bool     `json:"configured"`
	Upstreams    []string `json:"upstreams"`
	Capabilities []string `json:"capabilities"`
	Readiness    string   `json:"readiness"`
	Docs         string   `json:"docs"`
}

// adminProvidersResponse `GET /admin/providers` 响应体。
type adminProvidersResponse struct {
	Providers []adminProviderView `json:"providers"`
	Now       int64               `json:"now"`
}

// adminProvidersSnapshot 供应商目录快照：静态目录 × 配置命中（`configured` /
// `upstreams`，序 = 配置序）× 探针结果（`readiness`）。
//
// readiness：无命中上游 = not_configured；有命中但该组上游一条探针结果都没有 =
// configured；任一条 `ok` = ready；否则任一条「可达但不可用」或 `auth=rejected`
// = degraded（**不发明探针结果**：没探过就是 configured）。
func (s *Server) adminProvidersSnapshot(now time.Time) adminProvidersResponse {
	resp := adminProvidersResponse{
		Providers: make([]adminProviderView, 0, len(adminProviderCatalog)),
		Now:       now.UnixMilli(),
	}
	for _, e := range adminProviderCatalog {
		view := adminProviderView{
			ID:           e.ID,
			Label:        e.Label,
			Protocols:    make([]string, 0, len(e.Protocols)),
			Upstreams:    []string{},
			Capabilities: append([]string{}, e.Capabilities...),
			Readiness:    adminProviderNotConfigured,
			Docs:         e.Docs,
		}
		for _, p := range e.Protocols {
			view.Protocols = append(view.Protocols, string(p))
		}
		ready, reachableBad := false, false
		for i := range s.cfg.Upstreams {
			u := s.cfg.Upstreams[i]
			if !adminProviderMatches(e, u.Protocol) {
				continue
			}
			view.Configured = true
			view.Upstreams = append(view.Upstreams, u.Name)
			row, ok := s.adminProbes.get(u.Name)
			if !ok {
				continue
			}
			if row.OK {
				ready = true
				continue
			}
			if row.Reachable || row.Auth == adminProbeAuthRejected {
				reachableBad = true
			}
		}
		switch {
		case !view.Configured:
			view.Readiness = adminProviderNotConfigured
		case ready:
			view.Readiness = adminProviderReady
		case reachableBad:
			view.Readiness = adminProviderDegraded
		default:
			view.Readiness = adminProviderConfigured
		}
		resp.Providers = append(resp.Providers, view)
	}
	return resp
}

// adminProviderMatches 上游协议是否属于该供应商条目。
func adminProviderMatches(e adminProviderCatalogEntry, p Protocol) bool {
	for _, want := range e.Protocols {
		if want == p {
			return true
		}
	}
	return false
}

// handleAdminProviders 供应商目录（只读；零凭据字节、零出网——**不**触发探针）。
func (s *Server) handleAdminProviders(w http.ResponseWriter, r *http.Request) {
	writeAdminJSON(w, http.StatusOK, s.adminProvidersSnapshot(time.Now()))
}
