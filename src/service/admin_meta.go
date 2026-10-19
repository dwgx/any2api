// admin_meta.go —— dashboard P0-2 配置面 / 系统面（board A2A-L19-W73-1；设计
// `docs/admin-dashboard.md` §5.8「配置与系统」· §6.1 #6/#7 · §9 P0-2 行）。
//
// 端点（挂 adminAuth；未配 admin_key = 路由根本不注册 → 404 非 401）：
//
//	GET /admin/config   启动期配置树（**白名单投影 + 脱敏**）
//	GET /admin/system   runtime / store / build 三面快照
//
// 数据源（**零新持久面、零出网、零写盘、零 store 依赖**）：
//   - `/admin/config`：`Config` 的启动期静态字段（只读投影；**绝不回原文**）；
//   - `/admin/system`：`runtime`（go 版本/goroutine/内存/GC）、buildinfo 变量
//     （buildinfo.go）、`os.Stat(store.path)`（store 启用时；只 stat 不打开库）、
//     `Store.Sessions()`（行数）。
//
// 脱敏纪律（设计 §7.2「零凭据字节」硬约束）：
//   - **白名单投影**：响应结构逐字段显式列出（allowlist by construction）——不走
//     「遍历 Config 再删敏感键」的 denylist 路线（Config 加字段即漏）；
//   - **绝不回**：`admin_key` / `api_key` / `credentials` / `workspace_id` /
//     `org_id` / `device_id` / `access_keys` 的**值**（只有 `*_configured` 布尔与
//     `*_count` 计数）、proxy/base_url 的 userinfo/query/fragment；
//   - `adminSanitizeURL`：只保留 scheme/host/port/path；解析失败/非绝对 URL 回
//     `"invalid"`——**绝不回原文**（原文可能自带 userinfo 凭据）；
//   - `adminSanitizePathBasename`：路径只回文件名（家目录/部署目录前缀不回）；
//   - 纵深防御：全部字符串字段过既有 `redactAdminText`（admin_dashboard.go 的明文
//     形态投影）——白名单之外再加一层。
//
// 登记差异（**本单不做**，见报告「残余风险」）：
//   - `runtime.uptime_seconds` = **进程启动近似时刻**（本包变量 `adminMetaProcess
//     Start` 的初始化时刻；Go 在 main 之前完成包级初始化，与 main 里 NewServer 的
//     间隔 = LoadConfig，毫秒级）。`Server` 没有启动时刻字段，本单写面只含
//     server.go 的路由注册行（加字段要动 Server 结构体）→ 用进程口径。
//   - `store.audit_rows` 恒 `available:false`：`Store` 接口（冻结）没有 count 查询，
//     `AuditQuery` 回的是行不是计数（全表物化 = 不便宜）→ 显式标注不可用，**不用 0
//     冒充真实计数**（设计 §5.1「不显示 0 冒充真实」同源）。
package service

import (
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// adminMetaInvalidURL URL 消毒器的失败哨兵（**绝不回原文**：解析失败的串可能携带
// userinfo/query 凭据）。
const adminMetaInvalidURL = "invalid"

// adminMetaProcessStart 进程启动近似时刻（见头注「登记差异」；本包变量初始化 =
// main 之前的进程早期）。
var adminMetaProcessStart = time.Now()

// ---------------------------------------------------------------------------
// 路由注册
// ---------------------------------------------------------------------------

// registerAdminMeta 注册 P0-2 的两条只读端点。**自门控**：未配 admin_key 时一条
// 都不注册（与既有 /admin/* 的惰性注册语义逐字一致：404 非 401）。
//
// 调用点 = `Server.Handler()` 的 admin_key 分支（与 diagnostics / registerAdminAPI
// / registerAdminDashboard / metrics 同一位置）。
func (s *Server) registerAdminMeta(mux *http.ServeMux) {
	if s == nil || s.cfg.adminKey() == "" {
		return
	}
	mux.HandleFunc("GET /admin/config", s.adminAuth(s.handleAdminConfig))
	mux.HandleFunc("GET /admin/system", s.adminAuth(s.handleAdminSystem))
}

// ---------------------------------------------------------------------------
// 消毒器（零凭据字节纪律的机械面）
// ---------------------------------------------------------------------------

// adminSanitizeURL URL 的脱敏投影（设计 §7.2「base_url 去 userinfo；查询串里的
// key/token 一律砍」）：只保留 scheme/host/port/path，剥掉 userinfo / query /
// fragment；空串 → ""（= 未配置）；解析失败 / 非绝对 URL（无 scheme 或 host）→
// `"invalid"`（**绝不回原文**）。出口再过 `redactAdminText`（路径段里的
// `sk-…`/`Bearer …` 形态也投影掉）。
func adminSanitizeURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	u, err := url.Parse(trimmed)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return adminMetaInvalidURL
	}
	u.User = nil    // userinfo（`user:pass@`）绝不回——凭据字节
	u.RawQuery = "" // 查询串（`?key=…`）绝不回
	u.ForceQuery = false
	u.Fragment = "" // fragment 绝不回
	u.RawFragment = ""
	return redactAdminText(u.String())
}

// adminSanitizePathBasename 路径只回文件名（家目录/部署目录前缀不回；设计 §5.8
// 的 store/debug 面纪律）。空串 → ""；纯目录（`.` / `..` / 分隔符）→ ""（无信息
// 可回，不用占位符冒充路径）。
func adminSanitizePathBasename(p string) string {
	trimmed := strings.TrimSpace(p)
	if trimmed == "" {
		return ""
	}
	base := filepath.Base(filepath.Clean(trimmed))
	switch base {
	case ".", "..", string(filepath.Separator):
		return ""
	}
	return redactAdminText(base)
}

// ---------------------------------------------------------------------------
// GET /admin/config —— 响应结构（白名单：字段即契约）
// ---------------------------------------------------------------------------

// adminConfigResponse `GET /admin/config` 响应体（**白名单投影**）。
type adminConfigResponse struct {
	Now             int64                     `json:"now"` // unix ms（快照时刻）
	Listen          string                    `json:"listen"`
	Admin           adminConfigAdminView      `json:"admin"`
	UI              adminConfigUIView         `json:"ui"`
	Store           adminConfigStoreView      `json:"store"`
	Public          adminConfigPublicView     `json:"public"`
	Models          []adminConfigModelView    `json:"models"`
	Upstreams       []adminConfigUpstreamView `json:"upstreams"`
	Limits          adminConfigLimitsView     `json:"limits"`
	Debug           adminConfigDebugView      `json:"debug"`
	TaskFaceDegrade string                    `json:"task_face_degrade"`
}

// adminConfigAdminView 管理面（**只有布尔与容量——密钥值绝不回**）。
type adminConfigAdminView struct {
	KeyConfigured bool `json:"key_configured"`
	LogCapacity   int  `json:"log_capacity"`
}

// adminConfigUIView UI 托管面。
type adminConfigUIView struct {
	Enabled         bool `json:"enabled"`
	RateLimitPerMin int  `json:"rate_limit_per_min"`
}

// adminConfigStoreView 持久化面（路径只回 basename）。
type adminConfigStoreView struct {
	Enabled bool   `json:"enabled"`
	Path    string `json:"path"` // basename；未启用/空 = ""
}

// adminConfigPublicView 公开服务面（准入 key 只回条目数）。
type adminConfigPublicView struct {
	Enabled         bool                        `json:"enabled"`
	AccessMode      string                      `json:"access_mode"`
	AccessKeysCount int                         `json:"access_keys_count"`
	TrustedProxies  []string                    `json:"trusted_proxies"`
	Limits          adminConfigPublicLimitsView `json:"limits"`
}

// adminConfigPublicLimitsView 公开面的限额/闸/预算（有效值——缺省已填充）。
type adminConfigPublicLimitsView struct {
	MaxInflight             int    `json:"max_inflight"`
	QueueSize               int    `json:"queue_size"`
	QueueTimeoutSeconds     int    `json:"queue_timeout_seconds"`
	CredentialMinGapSeconds int    `json:"credential_min_gap_seconds"`
	MaxCostClass            string `json:"max_cost_class"`
	DailyBudgetUnits        int    `json:"daily_budget_units"`
	DailyBudgetEnabled      bool   `json:"daily_budget_enabled"`
	MaxBodyBytes            int64  `json:"max_body_bytes"`
	MaxMessages             int    `json:"max_messages"`
	MaxMessageChars         int    `json:"max_message_chars"`
}

// adminConfigUpstreamView 单上游（**零凭据字节**：只有 `*_configured` 布尔、
// 池大小与消毒后的 URL）。
type adminConfigUpstreamView struct {
	Index                 int    `json:"index"`
	Name                  string `json:"name"`
	Protocol              string `json:"protocol"`
	BaseURL               string `json:"base_url"` // 消毒；未配置 = ""
	PoolSize              int    `json:"pool_size"`
	TLSFingerprint        string `json:"tls_fingerprint"`
	ProxyConfigured       bool   `json:"proxy_configured"`
	Proxy                 string `json:"proxy"` // 消毒；未配置 = ""
	APIKeyConfigured      bool   `json:"api_key_configured"`
	WorkspaceIDConfigured bool   `json:"workspace_id_configured"`
	OrgIDConfigured       bool   `json:"org_id_configured"`
	DeviceIDConfigured    bool   `json:"device_id_configured"`
	TaskFace              string `json:"task_face"`
}

// adminConfigModelView 模型表条目（别名映射 + 发现面上限；非凭据面）。
type adminConfigModelView struct {
	ID             string `json:"id"`
	UpstreamModel  string `json:"upstream_model"`
	MaxInputTokens int    `json:"max_input_tokens"`
	MaxTokens      int    `json:"max_tokens"`
}

// adminConfigLimitsView 请求路径的限额/超时/冷却/重试参数（**有效值**——缺省已按
// 运行时同源的口径填充：运营者看到的必须是与请求路径一致的数字）。
type adminConfigLimitsView struct {
	HeartbeatSeconds           int                        `json:"heartbeat_seconds"`
	CooldownFailures           int                        `json:"cooldown_failures"`
	CooldownSeconds            int                        `json:"cooldown_seconds"`
	CooldownLadderSeconds      []int                      `json:"cooldown_ladder_seconds"`
	FailureRate                adminConfigFailureRateView `json:"failure_rate"`
	RetrySameUpstream          int                        `json:"retry_same_upstream"`
	RetrySameUpstreamDelaysMs  []int                      `json:"retry_same_upstream_delays_ms"`
	FirstByteTimeoutSeconds    int                        `json:"first_byte_timeout_seconds"`
	UpstreamIdleTimeoutSeconds int                        `json:"upstream_idle_timeout_seconds"`
	FailoverPolicy             map[string]int             `json:"failover_policy"`
}

// adminConfigFailureRateView 失败率冷却（有效值；enabled 缺省 true）。
type adminConfigFailureRateView struct {
	Enabled       bool    `json:"enabled"`
	MinRequests   int     `json:"min_requests"`
	Threshold     float64 `json:"threshold"`
	WindowSeconds int     `json:"window_seconds"`
}

// adminConfigDebugView debug 体录制面（**路径形态**；内容/文件绝不回）。
type adminConfigDebugView struct {
	CaptureConfigured bool   `json:"capture_configured"`
	CaptureDir        string `json:"capture_dir"` // basename；未配置 = ""
}

// ---------------------------------------------------------------------------
// GET /admin/config —— 投影
// ---------------------------------------------------------------------------

// handleAdminConfig 启动期配置树（脱敏）。**只读**：零副作用、不建池、不 stat。
func (s *Server) handleAdminConfig(w http.ResponseWriter, r *http.Request) {
	writeAdminJSON(w, http.StatusOK, s.adminConfigSnapshot())
}

// adminConfigSnapshot 配置投影快照（nil Config 安全——直接构造的测试 Server）。
func (s *Server) adminConfigSnapshot() adminConfigResponse {
	cfg := s.cfg
	if cfg == nil {
		cfg = &Config{}
	}
	frOn, frMinReq, frThreshold, frWindow := cfg.failureRate()
	budget, budgetOn := cfg.publicDailyBudgetUnits()

	// 模型表（空表 = `[]` 不 null——空态是 0 条，不是「未知」）。
	models := make([]adminConfigModelView, 0, len(cfg.Models))
	for _, m := range cfg.Models {
		models = append(models, adminConfigModelView{
			ID:             redactAdminText(m.ID),
			UpstreamModel:  redactAdminText(m.UpstreamModel),
			MaxInputTokens: m.MaxInputTokens,
			MaxTokens:      m.MaxTokens,
		})
	}

	// 上游（逐条白名单投影；凭据只化为池大小/布尔）。
	upstreams := make([]adminConfigUpstreamView, 0, len(cfg.Upstreams))
	for i := range cfg.Upstreams {
		u := &cfg.Upstreams[i]
		upstreams = append(upstreams, adminConfigUpstreamView{
			Index:                 i,
			Name:                  redactAdminText(u.Name),
			Protocol:              string(u.Protocol),
			BaseURL:               adminSanitizeURL(u.BaseURL),
			PoolSize:              adminMetaPoolSize(u),
			TLSFingerprint:        u.TLSFingerprint,
			ProxyConfigured:       strings.TrimSpace(u.Proxy) != "",
			Proxy:                 adminSanitizeURL(u.Proxy),
			APIKeyConfigured:      u.APIKey != "",
			WorkspaceIDConfigured: u.WorkspaceID != "",
			OrgIDConfigured:       u.OrgID != "",
			DeviceIDConfigured:    u.DeviceID != "",
			TaskFace:              u.taskFace(),
		})
	}

	// 冷却阶梯 / 重试延迟序列（有效值；空 = `[]` 不 null）。
	ladder := make([]int, 0, len(cfg.CooldownLadder))
	for _, d := range cfg.cooldownLadder() {
		ladder = append(ladder, int(d/time.Second))
	}
	delays := make([]int, 0, len(cfg.RetrySameUpstreamDelaysMs))
	for _, d := range cfg.retryDelays() {
		delays = append(delays, int(d/time.Millisecond))
	}
	policy := make(map[string]int, len(cfg.FailoverPolicy))
	for k, v := range cfg.FailoverPolicy {
		policy[k] = v
	}
	proxies := make([]string, 0, len(adminConfigTrustedProxies(cfg)))
	proxies = append(proxies, adminConfigTrustedProxies(cfg)...)

	return adminConfigResponse{
		Now:    time.Now().UnixMilli(),
		Listen: cfg.Listen,
		Admin: adminConfigAdminView{
			KeyConfigured: cfg.adminKey() != "", // **布尔**——密钥值绝不回
			LogCapacity:   cfg.adminLogCapacity(),
		},
		UI: adminConfigUIView{
			Enabled:         cfg.uiEnabled(),
			RateLimitPerMin: cfg.uiRateLimitPerMin(),
		},
		Store: adminConfigStoreView{
			Enabled: cfg.storeEnabled(),
			Path:    adminSanitizePathBasename(cfg.storePath()),
		},
		Public: adminConfigPublicView{
			Enabled:         cfg.publicEnabled(),
			AccessMode:      cfg.publicAccessMode(),
			AccessKeysCount: len(cfg.publicAccessKeys()),
			TrustedProxies:  proxies,
			Limits: adminConfigPublicLimitsView{
				MaxInflight:             cfg.publicMaxInflight(),
				QueueSize:               cfg.publicQueueSize(),
				QueueTimeoutSeconds:     int(cfg.publicQueueTimeout() / time.Second),
				CredentialMinGapSeconds: int(cfg.credentialMinGap() / time.Second),
				MaxCostClass:            cfg.publicMaxCostClass(),
				DailyBudgetUnits:        budget,
				DailyBudgetEnabled:      budgetOn,
				MaxBodyBytes:            cfg.publicMaxBodyBytes(),
				MaxMessages:             cfg.publicMaxMessages(),
				MaxMessageChars:         cfg.publicMaxMessageChars(),
			},
		},
		Models:    models,
		Upstreams: upstreams,
		Limits: adminConfigLimitsView{
			// 心跳 = 配置字段现值（与运行时的 `Server.hb`/`Config.Heartbeat()` 同源，
			// **不在读面发明缺省**：部署路径恒过 LoadConfig 的 applyDefaults，未写
			// 即 15）。
			HeartbeatSeconds:      cfg.HeartbeatSeconds,
			CooldownFailures:      s.cooldownFailures(),
			CooldownSeconds:       int(s.cooldownWindow() / time.Second),
			CooldownLadderSeconds: ladder,
			FailureRate: adminConfigFailureRateView{
				Enabled:       frOn,
				MinRequests:   frMinReq,
				Threshold:     frThreshold,
				WindowSeconds: int(frWindow / time.Second),
			},
			RetrySameUpstream:          cfg.retrySameUpstream(),
			RetrySameUpstreamDelaysMs:  delays,
			FirstByteTimeoutSeconds:    int(cfg.FirstByteTimeout() / time.Second),
			UpstreamIdleTimeoutSeconds: int(cfg.UpstreamIdleTimeout() / time.Second),
			FailoverPolicy:             policy,
		},
		Debug: adminConfigDebugView{
			CaptureConfigured: strings.TrimSpace(cfg.DebugCaptureDir) != "",
			CaptureDir:        adminSanitizePathBasename(cfg.DebugCaptureDir),
		},
		TaskFaceDegrade: adminMetaTaskFaceDegrade(cfg.TaskFaceDegrade),
	}
}

// adminMetaPoolSize 上游池大小（与 `upstreamCredentials` 同口径的**纯计数**版：
// 不复制 Upstream 结构体、不建池）：`credentials` 非空 = 池大小；否则 `api_key`
// 非空 = 1；否则 0。**只回数字**。
func adminMetaPoolSize(u *Upstream) int {
	if u == nil {
		return 0
	}
	if len(u.Credentials) > 0 {
		return len(u.Credentials)
	}
	if u.APIKey != "" {
		return 1
	}
	return 0
}

// adminConfigTrustedProxies 反代信任列表（非凭据面；出口过 redactAdminText；
// 恒非 nil——空表 = `[]` 不 null）。
func adminConfigTrustedProxies(cfg *Config) []string {
	out := []string{}
	if cfg == nil || cfg.Public == nil {
		return out
	}
	for _, p := range cfg.Public.TrustedProxies {
		out = append(out, redactAdminText(strings.TrimSpace(p)))
	}
	return out
}

// adminMetaTaskFaceDegrade 能力面降级开关有效值（空 = off——Validate 已校验收敛）。
func adminMetaTaskFaceDegrade(v string) string {
	if v == "" {
		return taskFaceDegradeOff
	}
	return v
}

// ---------------------------------------------------------------------------
// GET /admin/system —— 响应结构 + 投影
// ---------------------------------------------------------------------------

// adminSystemResponse `GET /admin/system` 响应体（runtime / store / build 三面）。
type adminSystemResponse struct {
	Now     int64                  `json:"now"` // unix ms
	Runtime adminSystemRuntimeView `json:"runtime"`
	Store   adminSystemStoreView   `json:"store"`
	Build   adminSystemBuildView   `json:"build"`
}

// adminSystemRuntimeView 运行时快照（`runtime.*`；**无环境变量、无命令行、无
// 目录列表**——设计 §5.8 的 runtime 面只到这些计数）。
type adminSystemRuntimeView struct {
	GoVersion      string  `json:"go_version"`
	PID            int     `json:"pid"`
	Goroutines     int     `json:"goroutines"`
	MemAllocBytes  uint64  `json:"mem_alloc_bytes"`
	MemSysBytes    uint64  `json:"mem_sys_bytes"`
	GCCount        uint32  `json:"gc_count"`
	GCPauseTotalMs int64   `json:"gc_pause_total_ms"`
	UptimeSeconds  float64 `json:"uptime_seconds"`
}

// adminSystemStoreView 持久化面快照（**counts 逐项标注可用性**：没有便宜的计数
// 路径就 `available:false`，不用 0 冒充真实计数）。
type adminSystemStoreView struct {
	Enabled            bool   `json:"enabled"`
	Path               string `json:"path"` // basename；未启用 = ""
	SizeBytes          int64  `json:"size_bytes"`
	SizeAvailable      bool   `json:"size_available"`
	Sessions           int    `json:"sessions"`
	SessionsAvailable  bool   `json:"sessions_available"`
	AuditRows          int    `json:"audit_rows"`
	AuditRowsAvailable bool   `json:"audit_rows_available"`
}

// adminSystemBuildView 构建面（buildinfo.go 的注入变量；未注入 = "unknown"）。
type adminSystemBuildView struct {
	Commit    string `json:"commit"`
	Date      string `json:"date"`
	GoVersion string `json:"go_version"`
}

// handleAdminSystem runtime / store / build 三面快照。**只读**：stat + 内存读，
// 零出网、零写盘、零 DDL。
func (s *Server) handleAdminSystem(w http.ResponseWriter, r *http.Request) {
	writeAdminJSON(w, http.StatusOK, s.adminSystemSnapshot())
}

// adminSystemSnapshot 系统快照投影。
func (s *Server) adminSystemSnapshot() adminSystemResponse {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	size, sizeOK := s.adminMetaStoreSize()
	sessions, sessionsOK := s.adminMetaStoreSessions()
	cfg := s.cfg
	if cfg == nil {
		cfg = &Config{}
	}
	return adminSystemResponse{
		Now: time.Now().UnixMilli(),
		Runtime: adminSystemRuntimeView{
			GoVersion:      runtime.Version(),
			PID:            os.Getpid(),
			Goroutines:     runtime.NumGoroutine(),
			MemAllocBytes:  ms.Alloc,
			MemSysBytes:    ms.Sys,
			GCCount:        ms.NumGC,
			GCPauseTotalMs: int64(ms.PauseTotalNs / uint64(time.Millisecond)),
			UptimeSeconds:  float64(time.Since(adminMetaProcessStart).Milliseconds()) / 1000,
		},
		Store: adminSystemStoreView{
			Enabled:           cfg.storeEnabled(),
			Path:              adminSanitizePathBasename(cfg.storePath()),
			SizeBytes:         size,
			SizeAvailable:     sizeOK,
			Sessions:          sessions,
			SessionsAvailable: sessionsOK,
			// Store 接口（冻结）没有 count 查询（AuditQuery 回行不回计数，全表
			// 物化不便宜）→ 显式不可用。
			AuditRows:          0,
			AuditRowsAvailable: false,
		},
		Build: adminSystemBuildView{
			Commit:    BuildCommit,
			Date:      BuildDate,
			GoVersion: runtime.Version(),
		},
	}
}

// adminMetaStoreSize store 库文件尺寸（只 `os.Stat`，**不打开库**）：store 未启用 /
// 路径空 / stat 失败 / 非常规文件 → (0, false)（0 绝不冒充真实尺寸）。
func (s *Server) adminMetaStoreSize() (int64, bool) {
	path := s.cfg.storePath()
	if !s.cfg.storeEnabled() || path == "" {
		return 0, false
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return 0, false
	}
	return info.Size(), true
}

// adminMetaStoreSessions 会话行数（store 未启用 / 查询失败 → (0, false)）。
// `Sessions()` 是接口里的现成读面（会话表本身很小；sqlite 档一次全表读）。
func (s *Server) adminMetaStoreSessions() (int, bool) {
	if s.store == nil {
		return 0, false
	}
	recs, err := s.store.Sessions()
	if err != nil {
		return 0, false
	}
	return len(recs), true
}
