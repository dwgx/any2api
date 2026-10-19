package service

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/dwgx/any2api/translate/convert/claude_web"
	"github.com/dwgx/any2api/translate/convert/interactions"
	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/pairs"
	"github.com/dwgx/any2api/translate/registry"
)

// maxBodyBytes 请求/响应体上限（Anthropic Messages 32MB 口径；超限 413）。
const maxBodyBytes = 32 << 20

// maxErrorBodyBytes 上游错误体读取上限（错误体只用于转信封，超限截断）。
const maxErrorBodyBytes = 1 << 20

// Server 薄服务壳。
type Server struct {
	cfg    *Config
	reg    *registry.Registry
	client *http.Client
	hb     time.Duration

	// health 每个上游的健康状态（下标与 cfg.Upstreams 一一对应）；diag 运行计数。
	health []upstreamHealth
	diag   diagCounters
	// adminAuthState 管理面鉴权失败/封禁状态；adminLog 管理面错误日志 ring
	//（W5-33 最小管理面；恒记录、有界、不落盘——未配 admin_key 时不可读）。
	adminAuthState adminAuthState
	adminLog       adminLogRing
	// adminRequests dashboard P0-0 请求 ring（A2A-L19-W71-1；有界 2000 条，只存
	// 脱敏字段——见 admin_dashboard.go）。零值可用；写入点 = serve 收尾
	// `recordRequestRing`（进程内内存态，重启清零）。
	adminRequests adminRequestRing
	// adminMetrics dashboard P0-1 指标面（A2A-L19-W72-1；定长 60 桶 × 1 分钟 +
	// TTFT/总量直方图——见 admin_metrics.go）。零值可用；写入点 = 同一收尾
	// `recordRequestRing`（进程内内存态，重启清零；定长数组，绝不逐请求增长）。
	adminMetrics adminMetricsRing
	// adminProbes 上游探针结果表（A2A-L19-W78-1；`POST /admin/upstreams/{name}/probe`
	// 写入、`GET /admin/providers` 派生 readiness——见 admin_providers.go）。**有界**
	// （键 = 上游名，容量 = 上游数；只在出现新键且已满时逐出最旧）；进程内内存态、
	// 重启清零、绝不持久化、零凭据字节。
	adminProbes adminProbeStore

	// tasks 任务面 store（W8-1 / Slice 1：responses 入口 background:true 托管
	// 最小面；单进程内存、TTL 有界——见 task.go 头注）。
	tasks *taskStore
	// taskRoutes 透传任务 id→上游 路由表（W27-1 / Slice 5：task_face=passthrough
	// 上游的任务 id 是上游 id——后续 retrieve/cancel/续流按此表回找上游；单进程
	// 内存、TTL 有界，见 task_passthrough.go 头注）。
	taskRoutes *passthroughRoutes
	// taskFaceDegraded 能力面进程内降级标记（W36-2 S8；task_face_degrade=auto
	// 时被结论性证据置位；键 = 上游下标，值恒 true）。只影响本进程、重启清零、
	// 绝不写回配置——声明（L1）仍是唯一配置权威。sync.Map 零值可用（Server 直
	// 接构造的测试路径无需初始化）。
	taskFaceDegraded sync.Map

	// uiLimiter UI 托管面 per-IP 限流器（W56-1）：`ui.rate_limit_per_min` > 0
	// 时非 nil——对 /v1/* 与 /ui/api/* 生效（见 ui_server.go）；nil = 不限流。
	uiLimiter *ipRateLimiter
	// sessionsWriteLimiter 会话/消息 API per-session 写桶（P1-3；60 写/分钟）：
	// 只对 POST/PUT/PATCH/DELETE 计数——保护 SQLite 单写者（见 sessions_api.go）。
	// nil = 不挂桶（store/public 未启用时 API 本身 404/503）。
	sessionsWriteLimiter *ipRateLimiter

	// sessions 匿名会话表（P0-1；public.enabled=true 时非 nil——nil = 会话
	// 中间件不挂载、logout 不注册，零行为变化）。
	sessions *sessionStore
	// accessGated 准入模式（P0-2；gated = 无 key 的新会话不可建立）。
	accessGated bool
	// accessKeys gated 准入 key 比对态（sha256 + sha8 id；只存摘要——原文
	// 绝不落日志/记录）。
	accessKeys []accessKey
	// trustedProxies 反代信任网段（P0-3；nil = 现状——clientIP 不读 XFF）。
	trustedProxies []*net.IPNet
	// limiter 全局并发闸（P0-4；public.enabled=true 时非 nil——serve 入口
	// Acquire/收尾 Release；nil = 不挂闸，零行为变化）。
	limiter *Limiter
	// publicLimits P0-5/P0-8 运行时限额（成本档 / 日预算台账 / 体上限 / 结构
	// 守卫；public.enabled=true 时非 nil——nil = 全不挂载，零行为变化）。
	publicLimits *publicLimits
	// store 持久化层（P1-1；store.enabled=true 时非 nil——nil = 全部消费方走
	// P0 内存路径，零行为变化）。会话/配额/审计的落库面见 session.go / limits.go
	// / auditStore；停机经 ShutdownCleanup → CloseStore 有界收尾。
	store Store

	// clients/clientErrs 每个上游的出站客户端（W11：per-upstream proxy / TLS
	// 指纹；下标与 cfg.Upstreams 对齐）。nil 项 = 用共享 client（直连 + 无指纹）。
	// 构建错误进 clientErrs——请求面显式报错（绝不静默直连，见 outbound.go）。
	clients    []*http.Client
	clientErrs []error
	// testRootCAs 仅测试注入：自签测试 CA（指纹/代理出站的 TLS 信任面）。
	// 生产恒 nil（系统根证书 + 正常校验）。
	testRootCAs *x509.CertPool

	// cwCleanup* 会话清理面（W13；claude_web 专用——见 claude_web.go 清理面头注）：
	// 异步 DELETE goroutine 的收尾记账与停机取消。cwCleanupCtx 与请求 ctx 解耦
	// （请求返回/下游断开绝不掐在飞清理）；停机经 ShutdownCleanup 取消 + 有界
	// 等待（goroutine 绝不无限等）。cwCleanupInterval 测试可覆写（生产恒
	// claudeWebCleanupInterval = 2s——BK claude.go:189-224 原值）。
	cwCleanupWG       sync.WaitGroup
	cwCleanupCtx      context.Context
	cwCleanupStop     context.CancelFunc
	cwCleanupInterval time.Duration
}

// New 创建服务（注册表 = pairs.NewRegistry()；上游请求不设整体超时——
// 流式长连接；仅约束响应头等待）。**启动期错误（P1-1 store 打开/迁移失败）=
// `log.Fatalf`**（启动报错，绝不静默降级——与 main 的配置加载错误同款口径）。
// 需要拿到错误的调用方（测试/嵌入式）用 NewServer。
func New(cfg *Config) *Server {
	s, err := NewServer(cfg)
	if err != nil {
		log.Fatalf("any2api: %v", err)
	}
	return s
}

// NewServer 创建服务并返回启动期错误（P1-1：store 打开/迁移/GC 失败 = 启动报错
// ——设计 §2.4-C「失败 = 启动报错（不静默降级）」；main 侧接线不在本单写面，
// 由 New 包一层 log.Fatalf）。
func NewServer(cfg *Config) (*Server, error) {
	s := &Server{
		cfg: cfg,
		reg: pairs.NewRegistry(),
		client: &http.Client{
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				ResponseHeaderTimeout: 60 * time.Second,
			},
		},
		hb:     cfg.Heartbeat(),
		health: make([]upstreamHealth, len(cfg.Upstreams)),
		// 管理面错误日志 ring 容量来自配置（W5-36；未配 = 缺省 256）。
		adminLog: adminLogRing{capacity: cfg.adminLogCapacity()},
		// 任务面 store（W8-1 / Slice 1）+ 透传任务路由表（W27-1 / Slice 5）。
		tasks:      newTaskStore(),
		taskRoutes: newPassthroughRoutes(),
	}
	// 出站客户端（W11：per-upstream proxy / TLS 指纹；见 outbound.go）。
	s.buildOutbound()
	// 会话清理面（W13）：根 ctx 与请求面解耦 + 缺省重试间隔（BK 原值 2s）。
	s.cwCleanupCtx, s.cwCleanupStop = context.WithCancel(context.Background())
	s.cwCleanupInterval = claudeWebCleanupInterval
	// W54-3 残余 3：凭据池事件接收端装配——凭据冷却以**独立型别**
	//（adminLogCredentialCooldown）进管理面 ring，与上游级 cooldown_trip 区分。
	// 池挂 Config（惰性建池）；此处预建并注入 sink（幂等覆盖；直接构造的 Config
	// 形态不经过本函数 → 零行为变化）。
	for i := range cfg.Upstreams {
		if pool := cfg.credentialPoolAt(i); pool != nil {
			pool.setEventSink(func(kind, upstream, message string, status int) {
				s.adminLog.add(kind, upstream, "", message, status)
			})
		}
	}
	// UI 限流器（W56-1）：`ui.rate_limit_per_min` > 0 时建 per-IP 令牌桶
	//（缺省 0 = 关——Handler 不包限流中间件）。
	if n := cfg.uiRateLimitPerMin(); n > 0 {
		s.uiLimiter = newIPRateLimiter(n)
	}
	// 持久化层（P1-1）：**只在 store.enabled=true 时打开**（缺省 false =
	// OpenStore 返回 nil——全部消费方走 P0 内存路径，零行为变化——硬门）。
	// 打开/迁移/GC 失败 = 启动报错（绝不静默降级为内存档——设计 §2.4-C）。
	st, err := OpenStore(cfg)
	if err != nil {
		return nil, err
	}
	s.store = st
	if st != nil {
		// 启动动作（设计 §5.3）：打开 → 迁移（OpenStore 内完成）→ GC（过期会话/
		// 软删硬删/过期审计/过期台账）→ 一行摘要（无敏感面）。
		gcStats, err := st.GC(time.Now())
		if err != nil {
			_ = st.Close()
			return nil, err
		}
		nSessions := 0
		if recs, err := st.Sessions(); err == nil {
			nSessions = len(recs)
		}
		log.Printf("any2api: store ready (path=%s, sessions=%d, gc: %s)",
			cfg.storePath(), nSessions, gcStats)
	}
	// 公开服务面（P0-1..4）：**只在 public.enabled=true 时接线**（缺省 false =
	// 全链零行为变化——硬门）。会话表 / 门控 key 集（sha256 比对态）/ 反代
	// 信任网段 / 全局并发闸；Validate 已保证配置合法（此处防御性忽略解析错误）。
	if cfg.publicEnabled() {
		s.sessions = newSessionStore()
		s.accessGated = cfg.publicAccessMode() == publicAccessModeGated
		s.accessKeys = newAccessKeys(cfg.publicAccessKeys())
		if cfg.Public != nil {
			if nets, err := parseTrustedProxies(cfg.Public.TrustedProxies); err == nil {
				s.trustedProxies = nets
			}
		}
		s.limiter = NewLimiter(cfg.publicMaxInflight(), cfg.publicQueueSize(), cfg.publicQueueTimeout())
		// P0-5/P0-8：成本档 / 日预算台账 / 体上限 / 结构守卫（同一 enabled 门）。
		s.publicLimits = newPublicLimits(cfg)
		// P1-2/P1-6 持久化接线：会话表落库（重启后会话仍有效）+ 日预算台账落库
		// （跨重启预算不重置）。store 未启用（st == nil）→ 两处都不挂，逐字现状。
		if st != nil {
			s.sessions.persist = st
			s.sessions.onPersistErr = s.storeError
			s.publicLimits.attachStore(st, s.storeError)
			// P1-3 per-session 写桶：只有「会话面 + store」都就绪时才需要
			//（store 未启用 = sessions API 503；public 未启用 = 404）。
			s.sessionsWriteLimiter = newIPRateLimiter(sessionsWritePerMin)
		}
	}
	return s, nil
}

// CloseStore 关闭持久化层（停机钩子；设计 §2.4-J）。store 未启用 = no-op。
// 幂等（重复调用只关一次）。返回关闭错误（调用方只记日志——停机不因此中断）。
func (s *Server) CloseStore() error {
	if s == nil || s.store == nil {
		return nil
	}
	st := s.store
	s.store = nil
	return st.Close()
}

// auditStore 审计双落点的**持久化侧**（P1-2；设计 §4.4）：store 启用时把事件
// 落 `audit_log`。失败只记日志 + ring `store_error`（**绝不影响请求路径**——
// 设计 §2.4-E 纪律）；store 未启用 = 全 no-op（零行为变化）。ring 侧的
// adminLog.add 仍由各调用点自己负责（既有行为不动）。
func (s *Server) auditStore(e storeAudit) {
	if s == nil || s.store == nil {
		return
	}
	if _, err := s.store.AuditAppend(e); err != nil {
		s.storeError("audit append", err)
	}
}

// storeError 持久化失败面（只记日志 + ring；绝不进控制流）。零敏感面：错误文本
// 来自 store 层（文件路径/sqlite 消息），不含凭据/cookie/key/IP 原文。
func (s *Server) storeError(op string, err error) {
	msg := "store_error: " + op + ": " + err.Error()
	log.Printf("any2api: %s", msg)
	s.adminLog.add(adminLogStoreError, "", "", msg, 0)
}

// storeAuditKindRequest 审计行型别：一次入口请求的终态（设计 §2.3a audit_log.kind
// 词表的 `request`）。
const storeAuditKindRequest = "request"

// auditRequestDetail 请求审计行的 detail（JSON；**不含内容/凭据/cookie/key/IP
// 原文**——只入口/模型/流位/尝试数/末上游/结局/用量观测/池索引）。
type auditRequestDetail struct {
	Entry    string `json:"entry"`
	Model    string `json:"model"`
	Stream   bool   `json:"stream"`
	Attempts int    `json:"attempts"`
	Upstream string `json:"upstream,omitempty"`
	Outcome  string `json:"outcome,omitempty"`
	Status   int    `json:"status,omitempty"`
	In       int64  `json:"in,omitempty"`
	Out      int64  `json:"out,omitempty"`
	CredIdx  *int   `json:"credential_index,omitempty"`
}

// auditRequest 请求收尾审计落库（P1-2；serve 的 defer 调用）。store 未启用 =
// no-op。sid 落**前缀**（与 attempts 行/ring 同纪律——设计 §1.1）。
func (s *Server) auditRequest(dr downstreamRequest) {
	if s == nil || s.store == nil || dr.rec == nil {
		return
	}
	detail := auditRequestDetail{
		Entry:    dr.rec.entry,
		Model:    dr.rec.model,
		Stream:   dr.rec.stream,
		Attempts: len(dr.rec.recs),
	}
	if n := len(dr.rec.recs); n > 0 {
		last := dr.rec.recs[n-1]
		detail.Upstream = last.upstream
		detail.Status = last.status
		detail.In, detail.Out = last.in, last.out
		if last.credentialIndex >= 0 {
			ci := last.credentialIndex
			detail.CredIdx = &ci
		}
	}
	detail.Outcome = dr.rec.auditOutcome()
	body := ""
	if b, err := json.Marshal(detail); err == nil {
		body = string(b)
	}
	s.auditStore(storeAudit{
		TS:        time.Now(),
		SID:       dr.rec.sid,
		KeyID:     dr.rec.keyID,
		Kind:      storeAuditKindRequest,
		RequestID: dr.rec.reqID,
		Detail:    body,
	})
}

// Handler 路由：四个入口面 + count_tokens 出口 + /healthz + 最小管理面
// （/admin/*；仅配了 admin_key 时注册，见 admin.go）。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", s.handle(registry.FormatOpenAIChat))
	mux.HandleFunc("/v1/messages", s.handle(registry.FormatClaudeMessages))
	// count_tokens 出口（W5-20 条目 2）：Anthropic 客户端的免费计数面——上游为
	// anthropic 时透传（精确值），否则本地加权估算（见 count_tokens.go 头注）。
	// 路径独立注册（`/v1/messages` 是精确匹配，不含子路径）。
	mux.HandleFunc("/v1/messages/count_tokens", s.handleCountTokens)
	// 模型发现（W5-27 条目 3）：`GET /v1/models`（官方可选端点，`llm-gateway-protocol`
	// §Model discovery；SDK 形态为 `GET /v1/models?beta=true`——ServeMux 精确匹配
	// path、查询串不参与）。最小实现：静态一线模型表（见 handleModels）。
	mux.HandleFunc("/v1/models", s.handleModels)
	// 模型检索（W5-28 条目 2）：`GET /v1/models/{id}`（SDK `models.retrieve` 面，
	// 形态 `/v1/models/${model_id}?beta=true`）——前缀注册 + 手工取 id（与
	// gemini 入口同理：ServeMux 精确匹配 path，查询串不参与）。
	mux.HandleFunc("/v1/models/", s.handleModelRetrieve)
	// Responses 入口：路径固定 /v1/responses（表 §请求面；模型与 stream 在请求体里
	// ——与 gemini 的模型名进 URL 路径相反）。别名/Azure 形态（/openai/v1/responses、
	// `/v1/responses/compact`）本 slice 不做（见报告 §边界）。
	mux.HandleFunc("/v1/responses", s.handle(registry.FormatOpenAIResponses))
	// 任务面轮询（W8-1 / Slice 1）：GET /v1/responses/{id} → 任务快照。创建面 =
	// 同一路径 POST 带 `background:true`（handle 内分流到 handleTaskCreate）。
	// 取消面（Slice 2）= POST /v1/responses/{id}/cancel（幂等；真中止在飞上游
	// 调用；重复 cancel 返回同终态）。方法与路径双限定：其余子路径方法 → 标准库
	// 405 + Allow。
	mux.HandleFunc("GET /v1/responses/{id}", s.handleTaskRetrieve)
	mux.HandleFunc("POST /v1/responses/{id}/cancel", s.handleTaskCancel)
	// Gemini 入口：模型名与动作都在路径里（`/v1beta/models/{model}:{action}`，
	// gemini-conversion-table §格式兼容面）——Go 1.26 ServeMux 的通配符必须占满
	// 一段（`{model}:generateContent` 不是合法模式），故注册前缀 + 手工解析。
	// Vertex 形态（同表 §端点行：`/v1beta1/projects/{p}/locations/{l}/publishers/
	// google/models/{m}:…`，`/v1` 同形）注册两个前缀，同一处理器——路径段解析
	// 见 parseEntryAction（project/location 段只做非空守卫，上游仍走配置的
	// gemini 端点；Vertex 上游形态不在本仓配置模型里，见报告 §边界）。
	mux.HandleFunc("/v1beta/models/", s.handleGeminiEntry)
	mux.HandleFunc("/v1beta1/projects/", s.handleGeminiEntry)
	mux.HandleFunc("/v1/projects/", s.handleGeminiEntry)
	// Interactions 入口面**不做**（W5-20 条目 3，证据化不做）：interactions 目前
	// 只是上游协议（ProtocolInteractions，W5-13/W5-15），服务壳没有第 5 入口面。
	// 断线续流（`GET /v1beta/interactions/{id}?stream=true&last_event_id=<event_id>`
	// ——interactions-wire §端点族，POST 不能续流）是 interactions 客户端对
	// interactions 服务端的恢复路径：没有入口面就没有可服务的客户端形态；下游
	// 四协议的恢复语义是客户端整请求重试（P70 S→N 回退），与本路径无关。做入口
	// 面 + 续流的前置条件与形态见 research/briefs/reports/A2A-L19-W5-20.md 条目 3。
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok"))
	})
	// 最小管理面（W5-33；P63 裁定）：**惰性注册**——未配 admin_key 时三条路由
	// 根本不注册（/admin/* 全部 404，非 401；对齐 CPA「未配管理密钥 = 路由不
	// 存在」）。方法模式（Go 1.22+ ServeMux）：路径命中而方法不匹配 → 405 +
	// Allow 头（标准库语义）。见 admin.go 头注。
	if s.cfg.adminKey() != "" {
		mux.HandleFunc("GET /admin/diagnostics", s.adminAuth(s.handleAdminDiagnostics))
		mux.HandleFunc("POST /admin/cooldowns/reset", s.adminAuth(s.handleAdminCooldownsReset))
		mux.HandleFunc("GET /admin/logs", s.adminAuth(s.handleAdminLogs))
		s.registerAdminAPI(mux)
		// dashboard P0-0 只读壳（A2A-L19-W71-1）：audit / requests(+detail) / alerts
		// ——同一自门控（未配 admin_key = 一条都不注册）。
		s.registerAdminDashboard(mux)
		// dashboard P0-1 指标面（A2A-L19-W72-1）：60 × 1 分钟环形桶 + 延迟直方图
		// ——同一自门控（未配 admin_key = 404 非 401；见 admin_metrics.go）。
		mux.HandleFunc("GET /admin/metrics", s.adminAuth(s.handleAdminMetrics))
		// dashboard P0-2 配置面/系统面（A2A-L19-W73-1）：脱敏配置树 + runtime/
		// store/build 快照——同一自门控（未配 admin_key = 404 非 401；见 admin_meta.go）。
		s.registerAdminMeta(mux)
		// dashboard P1-2 模型目录 + 任务快照（A2A-L19-W73-2）：`GET /admin/models`
		// （Config.Models 逐条 + 诚实缺数标记）+ `GET /admin/tasks`（有界只读任务
		// 行，sid 只前缀、key 只 sha8，无取消动作）——同一自门控（未配 admin_key
		// = 404 非 401；见 admin_models_tasks.go）。
		s.registerAdminModelsTasks(mux)
		// dashboard P1-5 运营面（A2A-L19-W78-1）：账号/代理清单 + CSV/JSONL 导出 +
		// 上游探针 + 供应商目录（见 admin_accounts.go / admin_export.go /
		// admin_providers.go）——同一自门控（未配 admin_key = 404 非 401）。
		s.registerAdminAccounts(mux)
		s.registerAdminExport(mux)
		s.registerAdminProviders(mux)
	}
	// UI 托管面（W56-1）：`ui.enabled` 缺省 true（只多挂 /ui 路由，无害）；
	// false = 不注册（/ui/* 全部 404）。/ui/api/config 为精确路径（ServeMux
	// 具体度规则下优先于 /ui/ 子树）；静态面见 ui_server.go。
	if s.cfg.uiEnabled() {
		mux.HandleFunc("/ui", s.handleUIAssets)
		mux.HandleFunc("/ui/", s.handleUIAssets)
		mux.HandleFunc("/ui/api/config", s.handleUIConfig)
		// 会话显式端点（P0-1；public.enabled=true 时）：logout——删会话 + 清
		// cookie。注册为**无方法精确路径**（处理器内做方法限定回 405）——
		// 若只注册 POST，GET 会落进 "/ui/" 子树 SPA 回退（200），语义错误。
		if s.sessions != nil {
			mux.HandleFunc(sessionLogoutPath, s.handleSessionLogout)
		}
		// 会话/消息 API（P1-3；E1–E8）：**始终注册**（不按 store/sessions 条件
		// 跳过——否则 "/ui/" SPA 回退会把 /ui/api/sessions* 吞成 200）。handler
		// 内分流：public 未启用 → 404 JSON；store 未启用 → 503 store_unavailable。
		s.registerSessionsAPI(mux)
	}
	// per-IP 限流（W56-1）：`ui.rate_limit_per_min` > 0 时对 /v1/* 与 /ui/api/*
	// 生效（mux 外层统一挂——见 ui_server.go）。
	//
	// 会话中间件（P0-1）：挂在限流内层（先限流后签发——429 的请求不建会话）；
	// 覆盖 /v1/* 与 /ui/api/*（/ui/api/config 除外）。public.enabled=false
	// （缺省）时 sessions 为 nil = 不挂载（零行为变化——硬门）。
	var h http.Handler = mux
	if s.sessions != nil {
		h = s.sessionWrap(h)
	}
	if s.uiLimiter != nil {
		h = s.rateLimitWrap(h)
	}
	return h
}

// handle 入口处理器（openai / claude / responses 三面）：解析请求体元信息 →
// serve（模型名与流开关都在请求体里）。
func (s *Server) handle(entry registry.Format) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// debug 体录制（debug_capture_dir；缺省关 = 原样直过）：入口面统一挂载
		// （ctx 挂句柄 + ResponseWriter 包装），请求收尾落盘。
		r, w, captureDone := s.attachCapture(r, w)
		defer captureDone()
		body, ok := s.readEntryBody(w, r, entry)
		if !ok {
			return
		}
		var meta entryMeta
		if !s.parseEntryMeta(w, entry, body, &meta) {
			return
		}
		// 流开关来源 = 下游**路径** + 请求体（W5-11 边界条）：真 gemini 客户端的
		// 流开关在路径里（`:streamGenerateContent?alt=sse`），请求体无该字段；其余
		// 入口面（openai/claude/responses/interactions）在请求体里。路径态声明同时
		// 传进转换选项（pairs.Options.Stream）——转换出的上游请求体才带流位
		//（openai/claude/responses/interactions 的 body 带 `stream`；gemini 的流位
		// 在端点路径，由 PathFor 拼）。
		stream := meta.Stream || entryPathStream(r.URL.Path)
		// 任务面分流（W8-1 / Slice 1）：responses 入口 + `background:true` → 任务
		// 路径（合成快照 + 后台 goroutine）。background 缺省/false 与其余入口面
		// 走下方同步路径，字节零改动。
		if entry == registry.FormatOpenAIResponses && backgroundRequested(body) {
			s.handleTaskCreate(w, r, body, meta)
			return
		}
		dr := downstreamRequest{
			entry:  entry,
			body:   body,
			model:  meta.Model,
			stream: stream,
			opts: pairs.Options{
				IncludeUsage: meta.IncludeUsage,
				Stream:       stream,
			},
		}
		// P1-3 turn 落账归属：解析 X-A2A-Conversation-Id（无效头静默忽略——绝不
		// 因无效头拒绝聊天），同时回填会话 sid/key_id（只进内存记账，绝不落日志）。
		s.attachTurnContext(r, &dr)
		s.serve(w, r, dr)
	}
}

// handleGeminiEntry gemini 入口面（W5-16 §缺口 1；W5-19 条目 1/2/6 扩面）：
// `POST /v1beta/models/{model}:{action}` 与 Vertex 形态
// `/v1beta1/projects/{p}/locations/{l}/publishers/google/models/{model}:{action}`
// （`/v1` 同形；模型名与动作都在路径里——请求体无 model/stream 字段；
// gemini-conversion-table §格式兼容面）。`:streamGenerateContent`
// 判流（路径动作）；`?alt=sse` 是 SSE 形态参数（官方 SDK 硬编码两者同行）。
// 模型名同时落 pairs.Options.Model（跨格式转换时填 IR——请求体无模型名）。
//
// **非 alt=sse 的流形态**（W5-19 条目 2）：`:streamGenerateContent` 且
// `alt != "sse"` → 下游按官方 **JSON 数组**形态输出（`[` + `,` 分隔元素 + `]`，
// Content-Type: application/json；SM §4.1「不加 alt 的 JSON-array 流」）；
// `?alt=sse` 走既有 SSE 路径。数组形态下不写 SSE 错误帧、中途失败不闭合数组
// （截断的 JSON = 客户端可见的失败；闭合会被误读为正常完成）——见 relayStream。
//
// **`?key=` 语义（W5-19 条目 6）**：忽略——本服务对下游无鉴权面（与三入口面
// 一致）；客户端查询串里的 key **绝不**进上游 URL/头/日志（上游凭据只来自配置；
// 透传客户端 key = 凭据泄露面）。查询串只读 `alt`。
//
// 错误信封按 gemini 形态（Google 信封 `{"error":{code,message,status}}`——
// writeError → RenderErrorFor(FormatGeminiGenerate)）。
//
// 不做（见报告 §边界）：Vertex **上游**形态（OAuth Bearer / 区域 host /
// x-goog-user-project）、`:countTokens`。
func (s *Server) handleGeminiEntry(w http.ResponseWriter, r *http.Request) {
	entry := registry.FormatGeminiGenerate
	model, action, perr := parseEntryAction(r.URL.Path)
	if perr != nil {
		// 路径/动作不支持 → 404 NOT_FOUND（Google 对未知方法的形态）；模型名段
		// 畸形（空/注入字符）→ 400 INVALID_ARGUMENT。
		status, typ := http.StatusNotFound, "not_found_error"
		if errors.Is(perr, errGeminiEntryBadModel) {
			status, typ = http.StatusBadRequest, "invalid_request_error"
		}
		s.writeError(w, entry, status,
			&ir.Error{Type: typ, Message: perr.Error(), Source: "any2api"})
		return
	}
	// debug 体录制（debug_capture_dir；缺省关 = 原样直过）。
	r, w, captureDone := s.attachCapture(r, w)
	defer captureDone()
	body, ok := s.readEntryBody(w, r, entry)
	if !ok {
		return
	}
	var meta entryMeta
	if !s.parseEntryMeta(w, entry, body, &meta) {
		return
	}
	// 流位 = 路径动作（判据）∥ 请求体（容错：gemini 请求体无该字段，但手写客户端
	// 可能带）——与三入口面的「路径 + 请求体」并集纪律一致。
	stream := meta.Stream || action == "streamGenerateContent"
	// 数组形态（W5-19 条目 2）：只有流式动作才有数组形态；`alt=sse` 之外的
	// 取值（含缺省）都按官方 JSON 数组输出。
	arrayStream := stream && r.URL.Query().Get("alt") != "sse"
	s.serve(w, r, downstreamRequest{
		entry:       entry,
		body:        body,
		model:       model,
		stream:      stream,
		arrayStream: arrayStream,
		opts: pairs.Options{
			IncludeUsage: meta.IncludeUsage,
			Stream:       stream,
			Model:        model,
		},
	})
}

// errGeminiEntryNotFound gemini 入口路径/动作不受支持（未知动作、缺动作、非该面
// 路径）——调用方回 404 NOT_FOUND。
var errGeminiEntryNotFound = errors.New("unsupported gemini entry path")

// errGeminiEntryBadModel gemini 入口模型名段畸形（空 / 注入字符 / 控制字符）——
// 调用方回 400 INVALID_ARGUMENT。
var errGeminiEntryBadModel = errors.New("invalid model name")

// parseEntryAction 入口路径 → (模型名, 动作)。**路径态语义的唯一解析器**
// （W5-19 条目 7：合并 W5-18 遗留的 `entryPathStream` 后缀判据与 gemini 入口
// 解析——两处字面重复收成一个函数）。
//
// 支持两种路径族（gemini-conversion-table §端点行/§模型路径行）：
//
//  1. `POST /v1beta/models/{model}:{action}`（Gemini API 形态）；
//  2. Vertex 形态 `/{version}/projects/{p}/locations/{l}/publishers/google/models/
//     {model}:{action}`（version = v1beta1 / v1）——project/location 段只做非空
//     守卫（上游端点由配置决定，路径段不进上游 URL；报告 §边界）。
//
// 动作白名单：generateContent / streamGenerateContent；未知动作（含
// `:countTokens` 等未支持面）→ errGeminiEntryNotFound。
// 模型名守卫：空 / 含 `..` / 含空白与控制字符 → errGeminiEntryBadModel（GW 陷阱 13
// 精神：绝不把畸形模型名带进上游 URL 或转换面）。
func parseEntryAction(path string) (model, action string, err error) {
	rest, ok := strings.CutPrefix(path, "/v1beta/models/")
	if !ok {
		rest, ok = vertexModelSuffix(path, "/v1beta1/projects/")
		if !ok {
			rest, ok = vertexModelSuffix(path, "/v1/projects/")
		}
		if !ok {
			return "", "", fmt.Errorf("%w: unsupported path %q", errGeminiEntryNotFound, path)
		}
	}
	i := strings.LastIndex(rest, ":")
	if i < 0 {
		return "", "", fmt.Errorf("%w: missing action in path %q (want /v1beta/models/{model}:{action})", errGeminiEntryNotFound, path)
	}
	model, action = rest[:i], rest[i+1:]
	if model == "" {
		return "", "", fmt.Errorf("%w: model is required in path %q", errGeminiEntryBadModel, path)
	}
	if strings.Contains(model, "..") || strings.ContainsAny(model, " \t\n\r") || strings.ContainsFunc(model, unicode.IsControl) {
		return "", "", fmt.Errorf("%w: %q", errGeminiEntryBadModel, model)
	}
	switch action {
	case "generateContent", "streamGenerateContent":
	default:
		return "", "", fmt.Errorf("%w: unsupported action %q (want generateContent or streamGenerateContent)", errGeminiEntryNotFound, action)
	}
	return model, action, nil
}

// vertexModelSuffix 从 Vertex 形态路径里取 `.../models/{model}:{action}` 尾巴。
// 段序固定：`{p}/locations/{l}/publishers/google/models/{m}:{action}`；p/l 非空
// （空段 = 畸形路径 → 不匹配）；p/l 里的值不参与上游拼接（上游由配置决定）。
func vertexModelSuffix(path, prefix string) (string, bool) {
	rest, ok := strings.CutPrefix(path, prefix)
	if !ok {
		return "", false
	}
	// {p}/locations/{l}/publishers/google/models/{m}:{action}
	parts := strings.Split(rest, "/")
	if len(parts) != 7 {
		return "", false
	}
	if parts[0] == "" || parts[1] != "locations" || parts[2] == "" ||
		parts[3] != "publishers" || parts[4] != "google" || parts[5] != "models" {
		return "", false
	}
	return parts[6], true
}

// entryMeta 入口请求体的元信息（模型名 / 流开关 / include_usage）。
type entryMeta struct {
	Model        string
	Stream       bool
	IncludeUsage bool
}

// readEntryBody 读入口请求体（POST 限定 + 体上限；失败时已按入口协议写出错误
// 信封并返回 false）。请求计数在此 +1（每个入口请求一次，含解析失败）。
//
// P0-8：public.enabled=true 时体上限 = `public.max_body_bytes`（缺省 1MB；显式
// 0 = 用全局 32MB）；并挂结构守卫（messages 条数 / 单条内容长度——见
// guardPublicBody）。缺省（public 关）= 逐字节现状（32MB 口径 + 原错误文案）。
func (s *Server) readEntryBody(w http.ResponseWriter, r *http.Request, entry registry.Format) ([]byte, bool) {
	s.diag.requests.Add(1)
	if r.Method != http.MethodPost {
		s.writeError(w, entry, http.StatusMethodNotAllowed,
			&ir.Error{Type: "invalid_request_error", Message: "method not allowed", Source: "any2api"})
		return nil, false
	}
	limit := int64(maxBodyBytes)
	if s.publicLimits != nil && s.publicLimits.maxBodyBytes > 0 {
		limit = s.publicLimits.maxBodyBytes
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		s.writeError(w, entry, http.StatusBadRequest,
			&ir.Error{Type: "invalid_request_error", Message: "failed to read request body", Source: "any2api"})
		return nil, false
	}
	if int64(len(body)) > limit {
		msg := "request body exceeds 32MB"
		if limit != int64(maxBodyBytes) {
			msg = fmt.Sprintf("request body exceeds %d bytes (public.max_body_bytes)", limit)
		}
		s.writeError(w, entry, http.StatusRequestEntityTooLarge,
			&ir.Error{Type: "request_too_large", Message: msg, Source: "any2api"})
		return nil, false
	}
	// P0-8 结构守卫（public.enabled=true 时；缺省 nil = 直过——零行为变化）。
	if !s.guardPublicBody(w, entry, body) {
		return nil, false
	}
	// debug 体录制（debug_capture_dir；缺省关 = no-op）：记下游请求体。
	if c := captureFrom(r.Context()); c != nil {
		c.setRequestBody(body)
	}
	return body, true
}

// guardPublicBody P0-8 结构守卫（`public.max_messages` / `public.max_message_chars`；
// 两者显式 0 = 关）：`messages`（claude/openai chat）与 `input` 数组（responses）
// 条数上限 + 单条 content 长度上限（计量 = content 字段 JSON 原始字节——≥ 解码
// 后字节的上界，保守）。超限 → 413 request_too_large（按入口协议信封）。
//
// 纪律：① 只在 public.enabled=true 挂载（nil = true 直过）；② 体不可解析 /
// 消息项非对象 → 直过（守卫绝不成为新的误杀面——畸形体由转换面既有路径报错）；
// ③ 缺省零回归：守卫关闭时零解析开销。
func (s *Server) guardPublicBody(w http.ResponseWriter, entry registry.Format, body []byte) bool {
	pl := s.publicLimits
	if pl == nil || (pl.maxMessages <= 0 && pl.maxMessageChars <= 0) {
		return true
	}
	type msgProbe struct {
		Content json.RawMessage `json:"content"`
	}
	var probe struct {
		Messages []msgProbe `json:"messages"`
		Input    []msgProbe `json:"input"`
	}
	if json.Unmarshal(body, &probe) != nil {
		return true // 畸形体：交给既有转换面报错（守卫不误杀）
	}
	check := func(field string, items []msgProbe) bool {
		if pl.maxMessages > 0 && len(items) > pl.maxMessages {
			s.writeError(w, entry, http.StatusRequestEntityTooLarge,
				&ir.Error{Type: "request_too_large",
					Message: fmt.Sprintf("too many %s entries: %d exceeds public.max_messages=%d", field, len(items), pl.maxMessages),
					Source:  "any2api"})
			return false
		}
		if pl.maxMessageChars > 0 {
			for _, it := range items {
				if len(it.Content) > pl.maxMessageChars {
					s.writeError(w, entry, http.StatusRequestEntityTooLarge,
						&ir.Error{Type: "request_too_large",
							Message: fmt.Sprintf("%s content exceeds public.max_message_chars=%d bytes", field, pl.maxMessageChars),
							Source:  "any2api"})
					return false
				}
			}
		}
		return true
	}
	return check("messages", probe.Messages) && check("input", probe.Input)
}

// parseEntryMeta 解析入口请求体的元信息（模型名/流开关/include_usage；失败时已按
// 入口协议写出错误信封并返回 false）。gemini 入口的模型名在路径里——此处的
// meta.Model 由调用方按面取舍（gemini 面以路径为准）。
func (s *Server) parseEntryMeta(w http.ResponseWriter, entry registry.Format, body []byte, meta *entryMeta) bool {
	var m struct {
		Model         string `json:"model"`
		Stream        bool   `json:"stream"`
		StreamOptions *struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		s.writeError(w, entry, http.StatusBadRequest,
			&ir.Error{Type: "invalid_request_error", Message: "request body is not valid JSON", Source: "any2api"})
		return false
	}
	meta.Model = m.Model
	meta.Stream = m.Stream
	meta.IncludeUsage = m.StreamOptions != nil && m.StreamOptions.IncludeUsage
	return true
}

// rewriteModelField 请求体顶层 `model` 字段的字符串值替换（A6 别名映射的字节级
// 实现，W5-28 条目 1）：只替换该字符串值本身（含引号，重新 JSON 编码），**其余
// 字节逐字节保留**——同协议直通面的字节级纪律（E1/B8 开放列表：body 除模型名
// 外原样进上游）不因别名映射而破。返回 (改写后体, 是否改写)：
//
//   - 顶层不是 JSON 对象 / 无 `model` 字段 / 值是字符串以外的类型 → (nil, false)
//     （调用方回退透传——绝不发明字段、绝不改写畸形体）；
//   - 找到字符串值 → (新体, true)。
//
// 实现：json.Decoder 流式走键值（值经 RawMessage 精确定位跨度：InputOffset 给出
// 值 token 的结束偏移；值的起始偏移从键 token 结束处跳过空白与 `:` 得到）。
func rewriteModelField(body []byte, newModel string) ([]byte, bool) {
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return nil, false
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, false
	}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, false
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, false
		}
		afterKey := dec.InputOffset()
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, false
		}
		valueEnd := dec.InputOffset()
		if key != "model" {
			continue
		}
		// 值起始：键 token 结束处 → 跳空白 → `:` → 跳空白。
		i := afterKey
		for i < valueEnd && isJSONSpace(body[i]) {
			i++
		}
		if i >= valueEnd || body[i] != ':' {
			return nil, false
		}
		i++
		for i < valueEnd && isJSONSpace(body[i]) {
			i++
		}
		if i >= valueEnd || body[i] != '"' {
			return nil, false // 非字符串值：不改写（透传）
		}
		enc, err := json.Marshal(newModel)
		if err != nil {
			return nil, false
		}
		out := make([]byte, 0, len(body)-int(valueEnd-i)+len(enc))
		out = append(out, body[:i]...)
		out = append(out, enc...)
		out = append(out, body[valueEnd:]...)
		return out, true
	}
	return nil, false
}

// isJSONSpace JSON 空白字符（RFC 8259：space / tab / LF / CR）。
func isJSONSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// anyToolSimConfigured 配置里是否有上游开了工具模拟（W18-1 §S3）。serve 的探测
// 前置守卫：全关（缺省）→ 跳过入口体的工具探测，缺省配置零额外解析。
func (s *Server) anyToolSimConfigured() bool {
	for i := range s.cfg.Upstreams {
		if s.cfg.Upstreams[i].ToolSim {
			return true
		}
	}
	return false
}

// entryDeclaresTools 入口请求体是否声明了客户端工具（工具模拟置位条件之二
// 「入口带 tools」，W18-1 §S3）。判据 = 该入口格式的**工具声明字段**非空
// （IR `Request.Tools` 非空的必要近似——转换器各自的 parseTools 是权威面，本函数
// 只是服务壳侧的入口体快探，不引入第二套解析器）：
//
//   - claude_messages / openai_chat / interactions：顶层 `tools[]` 非空；
//   - responses：顶层 `tools[]` 非空，或 `input[]` 里的 `additional_tools` 条目
//     带 tools（Codex Desktop「Responses Lite」形态；responses/parse.go:383-393）；
//   - gemini：`tools[].functionDeclarations[]` 非空（顶层 tools 只是容器——
//     空 declarations 的元素不产生 IR 工具）。
//
// 解析失败/形态不识别 → false（**绝不置位**：置位错 = 出口解析器对普通文本也
// 扫标签，破坏「与现状逐字节一致」；false 的代价只是模拟不激活——渲染器另有
// IR 工具表非空自守，注入不会半发生）。
func entryDeclaresTools(entry registry.Format, body []byte) bool {
	var probe struct {
		Tools []json.RawMessage `json:"tools"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return false
	}
	if entry == registry.FormatGeminiGenerate {
		for _, raw := range probe.Tools {
			var t struct {
				FunctionDeclarations []json.RawMessage `json:"functionDeclarations"`
			}
			if json.Unmarshal(raw, &t) == nil && len(t.FunctionDeclarations) > 0 {
				return true
			}
		}
		return false
	}
	if len(probe.Tools) > 0 {
		return true
	}
	if entry == registry.FormatOpenAIResponses {
		// `input` 是数组形态才可能载 additional_tools（字符串形态 input 在本
		// 结构体下解码失败 → 返回 false——不置位，安全侧）。
		var in struct {
			Input []struct {
				Type  string            `json:"type"`
				Tools []json.RawMessage `json:"tools"`
			} `json:"input"`
		}
		if json.Unmarshal(body, &in) == nil {
			for _, it := range in.Input {
				if it.Type == "additional_tools" && len(it.Tools) > 0 {
					return true
				}
			}
		}
	}
	return false
}

// serve 有序 failover 链：按上游协议转换 → 转发（未提交前失败按 C1 分类换下一个
// 上游）→ 流式 relay / 非流式回写。dr.attempt = 本请求内的尝试序号（1 起；
// per-attempt 记账与日志用——W5-18 条目 4）。
//
// W5-19 条目 4：本函数为每个请求建一个 **per-request attempt 记录器**
// （billing-paths `usage.Record` 范式）——每次尝试收尾追加一条记录，请求收尾打
// 一行 JSON 日志（见 attempts.go）。reqID 同时是响应 `request-id` 头在无上游值
// 时的回退值（日志 ↔ 响应头可关联）。
func (s *Server) serve(w http.ResponseWriter, r *http.Request, dr downstreamRequest) {
	// 全局并发闸（P0-4；public.enabled=true 时非 nil）：超出 max_inflight 进
	// 有界队列；队列满 / 等待超时 → 429（正整数 Retry-After、按入口协议信封）；
	// 本请求任何出口经 defer Release。nil = 不挂闸（缺省——零行为变化）。
	if s.limiter != nil {
		// P0-5 快速路径（在排队之前——「日预算尽 → 快速 429」设计 §3.2 字面口径）：
		//   ① 成本档：请求体 max_tokens/max_output_tokens 粗分；档高于允许上限 →
		//      400 策略拒绝（重试不变好）；
		//   ② 日预算：准入即 check-and-charge（原子——防并发超卖）→ 不足 →
		//      429 daily_quota_exhausted（Retry-After=距 UTC 0 点整秒）。
		if s.publicLimits != nil {
			cls := classifyRequestCost(dr.body)
			// P1-3 turn 落账：本次请求的成本档 units（per-session/per-key 台账用；
			// 无 public 限额时保持 0——绝不双计 global，global 由 ledger.admit 写）。
			dr.costUnits = cls.units()
			if cls > s.publicLimits.maxCostClass {
				s.writeCostClassRejected(w, dr.entry, cls, s.publicLimits.maxCostClass)
				return
			}
			if err := s.publicLimits.ledger.admit(cls.units()); err != nil {
				s.writeQuotaExhausted(w, dr.entry, time.Now())
				return
			}
		}
		if err := s.limiter.Acquire(r.Context()); err != nil {
			s.writeLimitsRejected(w, dr.entry, err)
			return
		}
		defer s.limiter.Release()
	}
	// A6 模型别名映射（W5-28 条目 1）：请求面 alias → 上游 model 改写。
	// 配置表（顶层 `models:`）命中且声明 upstream_model 时才改写；缺省/未命中 =
	// 透传（现状不变）。改写面：
	//   ① 请求体顶层 `model` 字段——surgical 局部替换（只换该字符串值，其余字节
	//      逐字节保留；同协议直通面的字节级纪律不破）；
	//   ② dr.model——gemini 上游 PathFor 的路径段；
	//   ③ dr.opts.Model——仅当已设置（gemini 入口的路径态模型名）：跨协议转换的
	//      IR 模型填充用上游名。
	// 改写后的模型名进入记账日志（与上游请求同值，便于上游侧关联）。
	// 模型路由白名单（A2A-L19-W81-2）按**请求面模型 id**（`models[].id`）匹配：
	// A6 别名映射会把 dr.model 改写成 upstream_model（上游名），故在改写前留档。
	routeModel := dr.model
	if resolved := s.cfg.upstreamModelFor(dr.model); resolved != dr.model {
		dr.model = resolved
		if nb, ok := rewriteModelField(dr.body, resolved); ok {
			dr.body = nb
		}
		if dr.opts.Model != "" {
			dr.opts.Model = resolved
		}
	}

	dr.reqID = newRequestID()
	// debug 体录制（debug_capture_dir；缺省关 = no-op）：文件名/响应头/日志同源。
	if c := captureFrom(r.Context()); c != nil {
		c.setReqID(dr.reqID)
	}
	dr.rec = newRequestRecorder(dr.reqID, dr.entry, dr.model, dr.stream, dr.taskID)
	// P0-6 审计最小版：attempts 行带会话标识（从 session ctx 取；**只记
	// sid 前缀 + key_id=sha8，零凭据字节**——设计 §1.1/§4.4）。public.enabled
	// =false 时 ctx 无会话 → 两字段省略（wire omitempty；零行为变化）。
	if sess, ok := SIDFromContext(r.Context()); ok {
		dr.rec.sid = sidPrefix(sess.SID)
		dr.rec.keyID = sess.KeyID
	}
	defer dr.rec.log()
	// dashboard P0-0 请求 ring（A2A-L19-W71-1）：收尾把本请求的记账投影为脱敏请求
	// 行（见 admin_dashboard.go）——纯观测，绝不进控制流、绝不影响上面的日志行。
	defer s.recordRequestRing(dr.rec)
	// P1-2 审计持久化（设计 §2.4-E：收尾处落审计；store 未启用 = no-op，零行为
	// 变化）。**失败只 log + ring store_error，绝不影响请求路径**。
	defer s.auditRequest(dr)
	// P1-3 turn 落账（defer LIFO：本行后注册 → 先于 auditRequest 执行；只 log/
	// ring，绝不影响响应）。条件见 recordTurn：store 就绪 + 合法 conv + 非任务面。
	defer s.recordTurn(dr)
	// P1-3+ 逐模型台账（W78-1；设计 §5.5「按模型」）：每请求一行 `model:<模型名>`
	// ——与 recordTurn 相互独立（不依赖会话/conv），**只写 model scope**（不写
	// global/session/key，零双计）。注册点在限额/闸的早退之后：被拒（429/400）的
	// 请求不进模型台账。见 turn_hook.go recordModelUsage。
	defer s.recordModelUsage(dr)
	if dr.task != nil {
		// S6（W36-1）：任务终态 ring 事件（task_terminal）的 attempts/末上游来源
		// ——任务 goroutine 收尾（runTask）读同一记账器（与 attempt 日志行同源，
		// 按任务 id 可 join）。纯观测回填，绝不进控制流。
		dr.task.setRecorder(dr.rec)
	}

	// 工具名清洗表（P67 实锤缺陷）：入口请求体 → 原名→净化名映射，一次建表、
	// 请求面清洗与响应面还原共用（流式/非流式同一表）。上游协议由配置决定，本
	// 服务所有上游同协议（配置模型），取首个即可；空上游列表由 forward 路径兜底
	// （此处不建表）。表空（无长名/非法名）→ nil，本层直过。
	if len(s.cfg.Upstreams) > 0 {
		dr.opts.ToolNames = pairs.BuildToolNameMap(dr.entry, dr.body)
	}

	// 工具模拟探测（W18-1 §S3；置位条件之二「入口带 tools」）：只有配置里至少
	// 一个上游开了 tool_sim 才解析入口体（缺省全关 = 零额外解析——探测是一次
	// JSON 解码，绝不进缺省路径）。实际置位在 forward 内按**本台上游**判（还要
	// 求 protocol = claude_web，见那里的 gate）。
	if s.anyToolSimConfigured() {
		dr.entryTools = entryDeclaresTools(dr.entry, dr.body)
	}

	// 有序 failover 链：配置顺序；冷却中的上游跳过（全冷却 → 全列表）。任务流量
	//（dr.taskFace）时 `task_face: off` 的上游被剔除（Slice 5 能力表）；请求模型
	// 命中 `models[].upstreams` 时只保留白名单内的上游（A2A-L19-W81-2——过滤在
	// 健康/冷却之前，白名单外绝不入链）。
	idxs, skipped := s.candidatesForModel(time.Now(), dr.taskFace, routeModel)
	s.diag.cooldownSkips.Add(skipped)
	// per-exception 换上游预算（W5-22 条目 3）：按请求计（跨上游共享——
	// LiteLLM RetryPolicy 的 per-call 计数语义；见 failover.go）。
	dr.budget = &failoverBudget{}
	attemptNo := 0
	sameUpRetries := 0 // 本请求已用同上游重试次数（W5-32 条目 1；跨上游累计）
	for i, idx := range idxs {
		up := s.cfg.Upstreams[idx]
		last := i == len(idxs)-1
		// 同上游瞬时重试（W5-32 条目 1；P65B 裁定 #1）：同一台上的瞬时类失败先
		// 短退避重试（≤ retry_same_upstream 次，缺省 0 = 关），仍失败才换下一台
		// （或最后一台上抛）。**中间重试不计健康/不冷却，终态才记一次**——故
		// forward 不写终态错误信封，由本循环在重试穷尽后才放行（attemptLast）。
		for {
			attemptNo++
			dr.attempt = attemptNo
			// attemptLast = 本台重试已穷尽且是链上最后一台 → forward 写错误信封
			// （终态）；否则返回错误给本循环决定重试/换台。
			attemptLast := last && sameUpRetries >= s.cfg.retrySameUpstream()
			res := s.forward(w, r, idx, up, dr, attemptLast)
			// 同上游重试决策（未提交失败且本台仍允许重试）：先于健康记账与终态
			// 收尾——中间重试不计健康（recordFailure 只在重试穷尽后调一次）。
			retryOK := false
			delay := time.Duration(0)
			if !res.committed && !res.noRetry && !res.localErr &&
				sameUpRetries < s.cfg.retrySameUpstream() &&
				transientFailure(res.err, res.status, res.header, res.body) {
				delay = s.retrySameUpstreamDelay(sameUpRetries, res.header)
				retryOK = delay > 0
			}
			if !retryOK {
				// 终态健康记账（W5-32 条目 1 的「终态才记一次」纪律；原实现在
				// forward 内逐次记——中间重试会计多次失败，已移出）。
				if res.recordHealth {
					s.recordFailure(idx, upstreamCooldown(res.status, res.header, res.body))
				}
			}
			if res.committed {
				// 提交前错误的终态（W5-33 ring）：上游失败且错误信封已写下游
				//（末台 / 不可换类）。提交后的流内失败在 relay 内记，不在此面。
				if res.status >= 400 {
					s.adminLog.add(adminLogPrecommitError, up.Name, dr.reqID,
						fmt.Sprintf("upstream status %d; error envelope written to downstream", res.status), res.status)
				}
				return
			}
			if retryOK {
				sameUpRetries++
				s.diag.sameUpstreamRetries.Add(1)
				log.Printf("any2api: upstream %s transient failure (status=%d, err=%v); retrying same upstream in %s (retry %d/%d)",
					up.Name, res.status, res.err, delay.Round(time.Millisecond), sameUpRetries, s.cfg.retrySameUpstream())
				select {
				case <-time.After(delay):
					continue // 同上游再试一次（不计健康——recordFailure 在上方被跳过）。
				case <-r.Context().Done():
					s.adminLog.add(adminLogPrecommitError, up.Name, dr.reqID,
						"client disconnected while waiting to retry same upstream", 0)
					s.writeError(w, dr.entry, http.StatusBadGateway,
						&ir.Error{Type: "api_error", Message: "upstream request failed: client disconnected", Source: "any2api"})
					return
				}
			}
			// 终态：未提交失败（换台/上抛前，健康已在上方记过一次）。
			if res.err == nil {
				// 防御：未提交必带错误。
				s.adminLog.add(adminLogPrecommitError, up.Name, dr.reqID,
					"upstream request failed before commit (no error detail)", 0)
				s.writeError(w, dr.entry, http.StatusBadGateway,
					&ir.Error{Type: "api_error", Message: "upstream request failed (no error detail)", Source: "any2api"})
				return
			}
			if last || res.localErr {
				// P0-5：池耗尽（全部凭据不可用）→ 429 `capacity_exhausted` +
				// Retry-After=最早解冻整秒（设计 §3.3 映射；message 保留
				// upstream_credentials_exhausted 检索 token）。其余未提交失败
				// 沿既有 502 路径。
				var cee *credentialExhaustedError
				if errors.As(res.err, &cee) {
					s.writeCapacityExhausted(w, dr.entry, cee, cee.retryAfter)
					return
				}
				s.adminLog.add(adminLogPrecommitError, up.Name, dr.reqID,
					"upstream request failed before commit: "+res.err.Error(), res.status)
				s.writeError(w, dr.entry, http.StatusBadGateway,
					&ir.Error{Type: "api_error", Message: "upstream request failed: " + res.err.Error(), Source: "any2api"})
				return
			}
			// 换下一个上游（未提交前，提交点纪律允许；提交后 forward 恒 committed）。
			s.diag.failovers.Add(1)
			next := s.cfg.Upstreams[idxs[i+1]].Name
			s.adminLog.add(adminLogFailover, up.Name, dr.reqID,
				fmt.Sprintf("failing over to %s: %v", next, res.err), res.status)
			log.Printf("any2api: upstream %s failed (%v); failing over to %s",
				up.Name, res.err, next)
			break
		}
	}
}

// downstreamRequest 下游请求的解析结果（入口面 + 原始体 + 模型名/流开关/转换选项）。
// 模型名是 gemini 上游拼端点路径的输入（gemini 请求体不带 model 字段——
// 模型名在 URL 里，见 gemini-conversion-table §格式兼容面）。
type downstreamRequest struct {
	entry  registry.Format
	body   []byte
	model  string
	stream bool
	// arrayStream gemini 入口的**非 alt=sse** 流形态（W5-19 条目 2）：下游按
	// 官方 JSON 数组输出（relayStream 的数组写入路径）。
	arrayStream bool
	// attempt 本请求内的上游尝试序号（1 起；serve 的 failover 循环设置）。
	// per-attempt 记账（usage 观测）与日志用（W5-18 条目 4）。
	attempt int
	// reqID 本请求的自造 ID（serve 设置；响应 request-id 头的回退值 + 记账日志）。
	reqID string
	// rec per-request attempt 记录器（serve 设置；W5-19 条目 4）。
	rec *requestRecorder
	// taskID 任务面归因（Slice 4；handleTaskCreate 的任务 goroutine 设置）：
	// 任务 goroutine 复用 serve 跑上游——该请求的 per-request 记账行带任务 id
	// （attempts 数组按任务 join），空 = 非任务请求（wire 省略）。
	taskID string
	// task 任务条目（W36-1；handleTaskCreate 的托管任务 goroutine 设置）：S1 的
	// 取消归因细分（`cancelRequested()` → end_reason=task_cancel）与 S6 的
	// task_terminal ring 事件（serve 建记账器后回填——attempts/末上游来源）。
	// 同步请求与透传面为 nil（零行为变化）。
	task *taskEntry
	// taskFace 任务流量标记（W27-1 / Slice 5；任务 goroutine 设置 true）：候选链
	// 剔除 `task_face: off` 的上游（candidatesFor）——任务流量绝不落 off 上游。
	// 同步路径恒 false（字节零改动）。
	taskFace bool
	// budget 本请求的 per-exception 换上游预算（serve 设置；W5-22 条目 3——
	// forward 的失败分类决策按该类预算计数，见 failover.go 的 failoverBudget）。
	budget *failoverBudget
	// entryTools 入口请求体是否声明了客户端工具（W18-1 §S3；serve 设置——
	// 仅当配置里有上游开了 tool_sim 才探测，见 anyToolSimConfigured）。工具模拟
	// 置位条件之二（入口带 tools）；探测面是**请求体形态**（IR 面 tools 非空的
	// 必要近似，见 entryDeclaresTools）。
	entryTools bool
	// convID 本请求归属的会话列表 id（P1-3；handle 从 `X-A2A-Conversation-Id`
	// 解析并经 SID 归属校验；无效/未带 = ""——不落 turn，绝不拒绝聊天）。
	convID string
	// costUnits 本次请求的成本档 units（P1-3；serve 的成本档分类处回填；
	// 0 = 无 public 限额/未分类——只用于 per-session/per-key 台账）。
	costUnits int
	// sessionSID / sessionKeyID 会话身份（P1-3；handle 从 session ctx 回填——
	// **只进内存记账**，绝不落日志；keyID = gated key 的 sha8）。
	sessionSID   string
	sessionKeyID string
	opts         pairs.Options
}

// failoverResult 一次上游尝试（forward）的结果（W5-32 条目 1：健康记账与换上游
// 决策上移到 serve 的 failover 循环——同上游重试要求「中间重试不记健康，终态才
// 记一次」，故 forward 不再自行 recordFailure）。
type failoverResult struct {
	// committed=true：下游已提交（成功完成或已写出错误信封）——不再换上游/重试。
	committed bool
	// err 未提交前失败（committed=false 时非 nil）。
	err error
	// status/header/body 上游失败响应面（状态码失败时；供 serve 决策重试与终态
	// recordFailure 用——header 保留 retry-after 等）。
	status int
	header http.Header
	body   []byte
	// noRetry=true：本次失败不可换上游（直接上抛类 4xx——调用方写错误信封）。
	noRetry bool
	// recordHealth=true：本次失败应计入上游健康（终态时由 serve 调 recordFailure
	// 一次；noRetry 类失败不计——「请求问题」不是上游健康信号）。
	recordHealth bool
	// localErr=true：失败发生在本地（未打到上游——如 URL 构造失败），不计健康、
	// 不重试；serve 直接写 502 错误信封。
	localErr bool
}

// forward 向上游发起一次尝试（idx = 上游在配置里的下标）。
//
// 返回 failoverResult（committed / err / 上游状态面 / 健康记账旗标）：健康状态
// **不在此函数记**——serve 的 failover 循环在同上游重试穷尽后的**终态**才调一次
// recordFailure（W5-32 条目 1 的「中间重试不计健康，终态才记一次」纪律）。
// 提交后（流内/转换）失败仍在 relayStream 内记（那是终态——提交后不重试）。
//
// 失败分类按 C1 表（shouldFailover）：换上游类失败在非末位时返回错误给调用方；
// 直接上抛类失败（其余 4xx）写错误信封并提交（noRetry）。
func (s *Server) forward(w http.ResponseWriter, r *http.Request, idx int, up Upstream, dr downstreamRequest, last bool) failoverResult {
	s.diag.attempts.Add(1)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// per-request attempt 记录（W5-19 条目 4）：本尝试一条，收尾（任何路径）追加
	// 到请求记录器（nil 安全——直接构造 downstreamRequest 的测试不记账）。
	ar := dr.rec.begin(dr.attempt, up)
	defer dr.rec.finish(ar)

	entry := dr.entry
	upProto := up.Protocol.Format()

	// 工具模拟置位（W18-1 §S3；设计单 §S1 三条件合取）：本台上游开 tool_sim &&
	// 上游 protocol = claude_web && 入口请求带 tools。置位是**成对**的（请求面
	// 注入 + 出口面解析，设计单 §红线 2）：入口无 tools 时绝不置位——注入不会
	// 发生（渲染器另有 IR 工具表非空自守），而出口解析器一旦置位就会扫描契约
	// 标签（普通文本里的标签字面量会被误判为工具调用 = 与现状不再逐字节一致）。
	// 逐台上游判（非请求级一次）：failover 链里换到非 claude_web 台时自动关，
	// 出口管道按本台协议重建（relayStream 用本函数的 dr 副本建管道）。
	if up.ToolSim && dr.entryTools && up.Protocol == ProtocolClaudeWeb {
		dr.opts.ToolSim = true
	}

	// 请求转换（跨协议才转；同协议直通——同格式直通优先，arch-options §2.3）。
	//
	// 附件作业出口（W24-1 接缝）：目标 = claude_web 且请求含图像块时，渲染面把
	// 附件作业写进本尝试的 sink（无全局状态；见 claude_web/attach.go 头注）。
	// sink 是**每次上游尝试的局部对象**——并发请求零共享面。作业在下面的会话
	// 准备期消费（上传 + 原地回填）；非 claude_web 目标 = 零值，零行为变化。
	var attSink claude_web.AttachmentSink
	upBody := dr.body
	if upProto != entry {
		// 批量面诊断汇（W5-18 条目 2）：每次 Convert 一个新鲜 sink（请求面/响应面
		// 各自一个观测窗口；与流式面「每次尝试一个管道」同口径），收尾消费。
		reqDiag := &pairs.BatchDiagnostics{}
		reqOpts := dr.opts
		reqOpts.Diag = reqDiag
		reqOpts.AttachmentSink = &attSink
		out, err := pairs.Convert(s.reg, entry, upProto, pairs.FaceRequest, dr.body, reqOpts)
		s.consumeBatchDiagnostics(up, "request", reqDiag)
		if err != nil {
			// 客户端请求在目标协议不可表达：客户端错误，直接回 400（不重试）。
			s.writeError(w, entry, http.StatusBadRequest,
				&ir.Error{Type: "invalid_request_error", Message: "request cannot be converted: " + err.Error(), Source: "any2api"})
			return failoverResult{committed: true}
		}
		upBody = out
	}

	// 上游端点路径（gemini 按模型名拼路径；模型名缺省/含注入字符 = 客户端错误，
	// 直接回 400，不重试也不打畸形 URL 给上游）。
	//
	// claude_web（W10）：completion 路径含 org/conversation 两段**动态段**——
	// 先做会话准备（org 发现 + conversation 创建，见 claude_web.go 头注），
	// 再拿路径。失败 = 预提交失败（未提交下游 → 按 C1 换上游；凭据失效 / CF 盾
	// / 429 都是上游侧健康信号）。
	var cwTarget claudeWebTarget
	var cgTarget chatgptWebTarget
	var path string
	if up.Protocol == ProtocolClaudeWeb {
		// 附件作业（W24-1 接缝）：渲染面（上面的转换）把作业交付 attSink；这里把
		// 它交给会话准备期消费（上传 + 原地回填）。同协议直通（无转换）/无图像块
		// = nil，零行为变化。
		t, err := s.claudeWebPrepare(ctx, idx, up, dr.model, attSink.Job)
		if err != nil {
			ar.failed = true
			return failoverResult{err: err, recordHealth: true}
		}
		cwTarget = t
		path = t.path
		// W54-3 残余 2：attempt 记录回填池凭据索引（只记索引，零凭据字节；无标记
		// = 单凭据路径——保持 -1 省略）。标记由 prepare 包装附加、completion 面
		// 的 applyClaudeWebHeaders 消费——此处只读解析。
		if _, ci, marked, cerr := splitCredentialMarker(t.referer); marked && cerr == nil {
			ar.credentialIndex = ci
			// P0-4 单号 inflight=1：busy 生命周期与 attempt 收尾绑定——本尝试
			// 任何出口（成功流完 / 失败 / 下游断开）经 defer 释放（public 闸
			// 关闭时 Release no-op；prepare 失败面在 claudeWebPrepare 内释放）。
			if pool := s.cfg.credentialPoolAt(idx); pool != nil {
				defer pool.Release(ci)
			}
		}
		// W13 清理面：conversation 已创建——本尝试**任何收尾出口**（终态帧 /
		// 流内错误 / 上游失败 / 截断 / 下游断开 / 非流式完成）都异步 DELETE 它
		// （开关缺省开；失败只 warn，绝不改下游——见 claude_web.go 清理面头注）。
		// defer 挂在本函数上：每次上游尝试各自清理自己创建的会话；下游断开的
		// 会话同样要删（泄漏面最大的形态——W12-1 实锤 429 请求也建了会话）。
		defer s.scheduleClaudeWebCleanup(idx, up, t.org, t.conv)
	} else if up.Protocol == ProtocolChatGPTWeb {
		// chatgpt_web（W30-2）：匿名 Android 面三步链的前两步（sentinel →
		// prepare，见 chatgpt_web.go 头注）——prepare 与 completion **同体**
		// （渲染面产出的 upBody 字节，样本同体）。失败 = 预提交失败（下游未提交
		// → 按 C1 换上游；401 = 身份头被拒 / 403 = 盾 / 404 = 模型不可用 / 429 =
		// 限流，都是上游侧健康信号）。**无清理面**（匿名档无账号会话可删）。
		t, err := s.chatgptWebPrepare(ctx, idx, up, upBody)
		if err != nil {
			ar.failed = true
			return failoverResult{err: err, recordHealth: true}
		}
		cgTarget = t
		path = t.path
	} else {
		p, perr := up.Protocol.PathFor(dr.model, dr.stream)
		if perr != nil {
			s.writeError(w, entry, http.StatusBadRequest,
				&ir.Error{Type: "invalid_request_error", Message: perr.Error(), Source: "any2api"})
			return failoverResult{committed: true}
		}
		path = p
	}
	base := strings.TrimRight(up.BaseURL, "/")
	if up.Protocol == ProtocolClaudeWeb {
		// base_url 缺省 = https://claude.ai（claude_web 的 config 校验放宽，见
		// Validate；其余协议 base_url 仍必填）。
		base = claudeWebBase(up)
	}
	if up.Protocol == ProtocolChatGPTWeb {
		// base_url 缺省 = https://android.chat.openai.com（chatgpt_web 的 config
		// 校验同样放宽，见 Validate）。
		base = chatgptWebBase(up)
	}
	url := base + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(upBody))
	if err != nil {
		// 本地构造失败（URL 非法等）：上游没被打到，不计健康、不重试（recordHealth
		// 缺省 false；与引入前一致——原实现在此处直接返回错误、无 recordFailure）。
		return failoverResult{err: err, localErr: true}
	}
	req.Header.Set("Content-Type", "application/json")
	if dr.stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	switch up.Protocol {
	case ProtocolAnthropic:
		if up.APIKey != "" {
			req.Header.Set("x-api-key", up.APIKey)
		}
		// A2 硬门：anthropic-version 必须收（缺省按 2023-06-01）；直通场景
		// 透传客户端值，转换场景用标准值。
		av := "2023-06-01"
		if upProto == entry {
			if v := r.Header.Get("anthropic-version"); v != "" {
				av = v
			}
		}
		req.Header.Set("anthropic-version", av)
		// A3：beta 头转发集合 = 收到集合（仅直通场景；转换场景 beta 面不适用）。
		if upProto == entry {
			if v := r.Header.Get("anthropic-beta"); v != "" {
				req.Header.Set("anthropic-beta", v)
			}
		}
		// B3（W5-28 条目 3）：`anthropic-workspace-id` 转发（仅 anthropic 上游
		// 面）。保守口径：客户端带该头则原样转发（上游 = Claude Platform on AWS
		// 时该平台逐请求要求）；配置 `workspace_id` 非空时注入/覆盖（配置优先——
		// 上游凭据与工作区归属由配置决定，客户端值只是透传候选）。客户端不带且
		// 配置未设 → 不发明该头。P64B §缺口 4。
		ws := r.Header.Get("anthropic-workspace-id")
		if up.WorkspaceID != "" {
			ws = up.WorkspaceID
		}
		if ws != "" {
			req.Header.Set("anthropic-workspace-id", ws)
		}
	case ProtocolOpenAIChat, ProtocolResponses:
		// responses 与 chat 同为 Bearer 认证（表 §请求面）。
		if up.APIKey != "" {
			req.Header.Set("Authorization", "Bearer "+up.APIKey)
		}
	case ProtocolGemini:
		// gemini 鉴权头（x-goog-api-key；key 不进 URL 查询——日志泄露风险）。
		if up.APIKey != "" {
			req.Header.Set("x-goog-api-key", up.APIKey)
		}
	case ProtocolInteractions:
		// Interactions 上游（Google 面）：鉴权头与 gemini 同款（x-goog-api-key；
		// key 不进 URL 查询——interactions-wire §格式兼容面）。
		if up.APIKey != "" {
			req.Header.Set("x-goog-api-key", up.APIKey)
		}
		// Api-Revision 头纪律（BC:903-914；interactions-wire §格式兼容面）：
		// 客户端显式带头 → 转发其值（版本选择权在客户端——CPA 同款）；
		// 缺省补 new schema 默认 `2026-05-20`（与渲染面 canonical 一致：
		// 转换器按新 schema 字段面渲染，绝不混发——W5-13 ErrMixedRevisions）。
		rev := r.Header.Get(interactions.HeaderAPIRevision)
		if rev == "" {
			rev = interactions.RevisionNewSchema
		}
		req.Header.Set(interactions.HeaderAPIRevision, rev)
	case ProtocolClaudeWeb:
		// claude.ai 网页线（W10）：鉴权 = **cookie**（sessionKey），协议常量头 +
		// 按凭据派生的稳定身份头 + 阶段 referer（`/chat/{conv}`）——装配见
		// claude_web.go 的 applyClaudeWebHeaders（与 org 发现/会话创建同一份，
		// 不重复字面）。stream=true：completion 端点恒 SSE（网页线无非流式完成
		// 面），Accept 固定 text/event-stream——与下游是否流式无关。
		if err := applyClaudeWebHeaders(req.Header, up, cwTarget.referer, true); err != nil {
			ar.failed = true
			return failoverResult{err: err, recordHealth: true}
		}
	case ProtocolChatGPTWeb:
		// chatgpt.com 网页线（W30-2）：**无凭据**（匿名面）——身份头
		// （UA(Android)/Device-Id/Client-Type/Package）+ 三步链令牌（sentinel →
		// prepare 阶段取得，见 chatgptWebPrepare）经 applyChatGPTWebHeaders 装配
		// （与 sentinel/prepare 同一份，不重复字面）。Accept 固定
		// text/event-stream（completion 端点恒 SSE——与下游是否流式无关）。
		if err := applyChatGPTWebHeaders(req.Header, up, cgTarget.sentinelToken, cgTarget.conduitToken); err != nil {
			ar.failed = true
			return failoverResult{err: err, recordHealth: true}
		}
	}

	// debug 录制（W6-6 建议 ④；缺省关 = no-op）：登记本次上游尝试（上游名/URL/
	// 掩码请求头/转换后请求体）；响应体经 captureBody tee 落账（流式面即 relay
	// 读到的原始 SSE 字节——「翻译层逐字段归因」物证）。
	var captureAtt *captureAttempt
	if c := captureFrom(ctx); c != nil {
		captureAtt = c.addAttempt(up.Name, url, req.Header, upBody)
	}

	// 首产出超时守卫（流式面；缺省关——Config.FirstByteTimeoutSeconds<=0 → nil）：
	// 从**发起上游请求**起计时，到首个产出字节（提交点）为止；超时即取消本尝试
	// （未提交 → 调用方按 C1 换上游）。非流式面不设：没有部分产出，「首产出」= 整段
	// 响应体，属另一个面（响应头等待已由 ResponseHeaderTimeout 兜底）。
	//
	// 上游内容静默守卫（流式面；缺省关——Config.UpstreamIdleTimeoutSeconds<=0 →
	// nil）：同一计时起点，但只被**内容帧**刷新（keepalive/空 delta 不算）且覆盖
	// 整个尝试（提交后仍计时）——提交后触发 = `incomplete_stream` 错误帧收尾。
	var guard *firstByteGuard
	var idle *upstreamIdleGuard
	if dr.stream {
		guard = newFirstByteGuard(ctx, cancel, s.cfg.FirstByteTimeout())
		if guard != nil {
			defer guard.stop()
		}
		idle = newUpstreamIdleGuard(ctx, cancel, s.cfg.UpstreamIdleTimeout())
		if idle != nil {
			defer idle.stop()
		}
	}

	client, err := s.clientFor(idx)
	if err != nil {
		// 该上游出站客户端构建失败（防御面——Validate 已挡配置错误）：本地错误，
		// 上游没被打到，不计健康、不重试（与本地构造失败同款处置）。
		return failoverResult{err: err, localErr: true}
	}
	resp, err := client.Do(req)
	if err != nil {
		// 网络层失败：健康记账上移（serve 在同上游重试穷尽后终态记一次；W5-32
		// 条目 1）。err 保留 firstByteErr / idleErr 的守卫错误（瞬时类）。
		ar.failed = true
		e := err
		if fe := firstByteErr(up, guard); fe != nil {
			e = fe
		} else if ie := idleErr(up, idle); ie != nil {
			e = ie
			// S1（W36-1）：内容静默守卫在**响应头到达前**触发（A7：idle 码此前
			// 不进 attempt 记录）——窄化归因 upstream_idle。
			ar.endReason = endReasonUpstreamIdle
		}
		return failoverResult{err: e, recordHealth: true}
	}
	defer resp.Body.Close()
	if captureAtt != nil {
		// 上游响应体 tee（含流式面 relay 逐帧读到的字节；体上限见 capture.go）。
		captureAtt.Status = resp.StatusCode
		resp.Body = &captureBody{rc: resp.Body, att: captureAtt}
	}

	if resp.StatusCode >= 400 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		ar.status = resp.StatusCode
		ar.ttft = time.Since(ar.start)
		// W54-3 残余 1：完成面失败回喂池（completion 的 429/401/无盾 403 → 该
		// 凭据冷却；**不换号重试**——提交点纪律不变，只冷却，下次请求的准备面
		// 自然避开）。非 claude_web / 无池标记 → no-op。
		s.feedCompletionCredentialFailure(idx, up, cwTarget.referer, resp.StatusCode, resp.Header)
		// 失败分类（C1 表 + per-exception 预算）：换上游类失败在非末位时不写
		// 下游（错误体丢弃）；其余 4xx 是请求问题，错误信封直接上抛（换上游
		// 无意义）。冷却时长 = retry-after-ms/retry-after 头优先，资源级 403
		// 按固定窗口（首次命中）。预算（W5-22 条目 3）按该类计数（0 不换 /
		// N 上限 / 缺省内置）；显式信号（overloaded 文本 / x-should-retry）不受
		// 预算表改写。健康记账/同上游重试上移 serve（W5-32 条目 1）。
		allow, failure := s.failoverDecision(dr.budget, resp.StatusCode, resp.Header, errBody)
		if failure {
			ar.failed = true
		} else {
			s.recordSuccess(idx) // 请求问题类：上游本身健康（原语义：换上游前先记成功）。
		}
		if allow && !last {
			return failoverResult{
				err:          fmt.Errorf("upstream %s: status %d", up.Name, resp.StatusCode),
				status:       resp.StatusCode,
				header:       resp.Header,
				body:         errBody,
				recordHealth: failure,
			}
		}
		s.forwardError(w, entry, upProto, resp, errBody)
		// 已提交（错误信封已写下游）：serve 收尾前仍要记一次健康——末台上游失败是
		// 终态（原实现在此处 recordFailure 后写信封；记账上移后由 committed 结果
		// 携带 recordHealth 回传）。
		return failoverResult{
			committed:    true,
			status:       resp.StatusCode,
			header:       resp.Header,
			body:         errBody,
			recordHealth: failure,
		}
	}

	if !dr.stream {
		var respBody []byte
		var err error
		if up.Protocol == ProtocolClaudeWeb {
			// claude_web：上游 completion **恒 SSE**，且 `message_stop` 后不主动
			// 关闭连接（W9-2 §2.5 实测：流以客户端 abort 结束）——非流式下游也
			// 必须按终止帧停读（等 EOF 会挂到连接超时）。读到的完整 SSE 字节经
			// pairs 的 claude_web.ParseResponse 装配成非流式响应。
			respBody, err = readClaudeWebCompletion(resp.Body, maxBodyBytes)
		} else if up.Protocol == ProtocolChatGPTWeb {
			// chatgpt_web：同上——上游 completion 恒 SSE，且终态后仍握 socket
			// （W6-2 实测 1.3–3s）——按终止帧（`[DONE]`/error）停读。读到的 SSE
			// 字节经 pairs 的 chatgpt_web.ParseResponse 装配成非流式响应。
			respBody, err = readChatGPTWebCompletion(resp.Body, maxBodyBytes)
		} else {
			respBody, err = io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
		}
		if err != nil {
			ar.failed = true
			return failoverResult{
				err:          fmt.Errorf("read upstream response: %w", err),
				status:       resp.StatusCode,
				header:       resp.Header,
				recordHealth: true,
			}
		}
		if len(respBody) > maxBodyBytes {
			ar.failed = true
			return failoverResult{
				err:          fmt.Errorf("upstream response exceeds 32MB"),
				status:       resp.StatusCode,
				header:       resp.Header,
				recordHealth: true,
			}
		}
		// 非流式的「首产出」= 上游响应头到达（响应体已可读）。
		ar.status = resp.StatusCode
		ar.ttft = time.Since(ar.start)
		out := respBody
		if upProto != entry {
			respDiag := &pairs.BatchDiagnostics{}
			respOpts := dr.opts
			respOpts.Diag = respDiag
			out, err = pairs.Convert(s.reg, upProto, entry, pairs.FaceResponse, respBody, respOpts)
			s.consumeBatchDiagnostics(up, "response", respDiag)
			if err != nil {
				ar.failed = true
				// R07（W70-1）：非流式整段体在**写出前**转换——提交点未过，
				// 下游零字节。此处**不写**错误信封，按未提交失败上交 failover
				// 循环：先换下一台上游；只有末台（或同上游重试穷尽）才由循环写
				// 502（形态与读失败/超长体两条未提交失败一致）。
				return failoverResult{
					err:          fmt.Errorf("upstream response cannot be converted: %w", err),
					status:       resp.StatusCode,
					header:       resp.Header,
					recordHealth: true,
				}
			}
		}
		// per-attempt usage 记账（W5-18 条目 4）：非流式整段体 = 一帧（探针按上游
		// 协议取 usage 字段；纯观测，绝不进下游流）。W5-19：同一收尾点也追加
		// per-request attempt 记录。
		var partial partialUsage
		observePartialUsage(upProto, respBody, &partial)
		s.finishAttempt(ar, partial, false)
		// W104：同格式非流式成功体清洗（缺省关 = 字节直通）。手术式剥除
		// upstream-a/upstream-private 私有计时键——usage 观测已在上方完成（记账
		// 不受影响），只删键值/必要逗号，其余字节逐字节保留（不 re-marshal）。
		// 跨格式出口由各渲染器过滤表处理（W85/W101），此处不碰转换结果。
		if upProto == entry && s.cfg.scrubSameFormatExtras() {
			out = removeTopLevelField(out, "elapsed_ms")
			out = removeTopLevelField(out, "first_chunk_at")
		}
		s.recordSuccess(idx)
		s.ensureRequestID(w, resp, dr.reqID)
		// C4（W5-27 条目 2）：成功面 ratelimit 头转发（非流式；官方要求该族在
		// 每个响应上原样转发——成功响应供客户端显示额度）。
		copyRateLimitHeaders(w.Header(), resp.Header)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		// S1/S2（W36-1）：非流式终态产出 = 整段响应体；写出无错 = 交付（写失败
		// 只落 delivered=false——既有 `disconnected` 字段取值纪律不动）。
		ar.endReason = endReasonCompleted
		if _, werr := w.Write(out); werr == nil {
			ar.delivered = true
		}
		return failoverResult{committed: true}
	}

	// 流式：上游 200 必须是 SSE 形态（B1 反向——200+非 SSE 体 = 协议违规；
	// 提交前失败可换上游）。
	br := bufio.NewReader(resp.Body)
	if !looksLikeSSE(resp, br) {
		ar.failed = true
		e := fmt.Errorf("upstream %s returned non-SSE body for streaming request", up.Name)
		if fe := firstByteErr(up, guard); fe != nil {
			e = fe
		} else if ie := idleErr(up, idle); ie != nil {
			e = ie
		}
		if !last {
			return failoverResult{
				err:          e,
				status:       resp.StatusCode,
				header:       resp.Header,
				recordHealth: true,
			}
		}
		s.writeError(w, entry, http.StatusBadGateway,
			&ir.Error{Type: "api_error", Message: "upstream returned non-SSE body for streaming request", Source: "any2api"})
		return failoverResult{committed: true}
	}
	// W84-1（F1）：上游 200 + SSE 形态校验通过 = 流式尝试的响应头已到达，提交前
	// 落账上游状态（此前只在非流式成功面与上游 >=400 面写，成功流在 admin 台账
	// 里恒为 0）。提交后的流内失败由 failed/end_reason 归因，本字段保持 200。
	ar.status = resp.StatusCode
	committed, rerr := s.relayStream(ctx, cancel, guard, idle, w, entry, idx, upProto, resp, br, dr, ar)
	// relayStream 的失败已自行记账（提交后失败是终态——不重试、不换上游）；此
	// 分支仅在极端情形（未提交即返回错误）触发，recordHealth 保持 false 防双记。
	return failoverResult{committed: committed, err: rerr}
}

// entryPathStream 入口**路径态**流开关（gemini 语义：`:streamGenerateContent`
// 动作后缀——gemini-conversion-table §流开关行：官方 SDK 把动作放路径、
// 请求体无流字段）。其余入口面的流开关都在请求体里，路径态恒 false。
// 这是「Request.Stream 来源 = 下游路径/请求体」的路径半边；`alt=sse` 只是同一
// 动作路径上的 SSE 形态参数（官方 SDK 硬编码两者同行），单独出现不判流
// （防 `/v1/chat/completions?alt=sse` 之类误判）。
//
// W5-19 条目 7：判据与 gemini 入口解析合并为一个解析器（parseEntryAction）——
// 同一动作白名单、同一模型名守卫；不再有独立的字面后缀判据。
func entryPathStream(path string) bool {
	_, action, err := parseEntryAction(path)
	return err == nil && action == "streamGenerateContent"
}

// forwardError 上游非 2xx：按入口协议回写错误信封（errBody 已在分类时读出）。
//
//   - 同协议：错误体原样透传（最忠实——CC C2 字面形态不可改写）；
//   - 跨协议：经 pairs error face 转换（绝不透传上游 body 结构）；
//   - 配额头透传：retry-after / retry-after-ms / request-id / x-should-retry /
//     anthropic-ratelimit-*（retry-after-ms 与解析面同族——Azure 主推形态，
//     W5-20 条目 1）；
//   - retry-after / retry-after-ms 过 retryAfterForwardable 守卫（W14）：仅
//     正值转发，0/非正/畸形丢弃（防消费端见 0 立即热重试放大）。
func (s *Server) forwardError(w http.ResponseWriter, entry registry.Format, upProto registry.Format, resp *http.Response, errBody []byte) {
	var out []byte
	if upProto == entry {
		out = errBody
	} else {
		converted, cerr := pairs.Convert(s.reg, upProto, entry, pairs.FaceError, errBody, pairs.Options{})
		if cerr == nil {
			out = converted
		} else {
			out, _ = pairs.RenderErrorFor(entry, &ir.Error{
				Type: "api_error", Message: "upstream returned an error", Source: "any2api",
			})
		}
	}
	for _, h := range []string{"request-id", "x-request-id", "x-should-retry"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	// W14 护栏：retry-after / retry-after-ms 仅正值转发（0/非正/畸形丢弃——
	// 消费端回落自身退避逻辑；见 retryAfterForwardable）。
	for _, h := range []string{"retry-after", "retry-after-ms"} {
		if v := resp.Header.Get(h); retryAfterForwardable(v) {
			w.Header().Set(h, v)
		}
	}
	copyRateLimitHeaders(w.Header(), resp.Header)
	if w.Header().Get("request-id") == "" {
		w.Header().Set("request-id", newRequestID())
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(out)
}

// retryAfterForwardable 上游 `retry-after` / `retry-after-ms` 头值是否可转发下游
// （W14 护栏：防 CC 类消费端「0 秒 → 立即热重试」放大——W13-2 实测 claude.ai
// 429 带 `retry-after: 0` 经服务原样透传 → CC 2.1.88 见 0 < 20s 立即重试，
// 一发调用放大为 11 发 completion + 11 条会话）。
//
// 规则（与 failover 解析面 retryAfterSeconds 同口径——只认前导十进制整数）：
//
//   - 前导整数 > 0 → 可转发，且**原样**转发（不改字面、不做毫秒/秒换算——
//     既有 ms/秒语义不动；尾随垃圾不改变前导整数，与 CC parseInt 同语义）；
//   - 0 / 负数 / 非数字 / 空 / 其它畸形（含 HTTP-date——首字符非数字；以及
//     `0.5` 这类前导整数为 0 的形态）→ 不转发该头：消费端回落自身退避逻辑
//     （CC：parseInt→NaN/0 走指数退避，而不是「立即重试」）。
//
// 边界：本判据是**出站面**护栏，只作用于下游响应头；上游请求、换号重试与
// 冷却决策（forward 的 failoverDecision / upstreamCooldown）不受影响。
func retryAfterForwardable(v string) bool {
	n, width := leadingInt([]byte(v))
	return width > 0 && n > 0
}

// copyRateLimitHeaders 把上游响应头里 `anthropic-ratelimit-*` 族写入下游（官方
// C4：该族在**每个响应**上原样转发——成功响应供客户端显示额度，429 供区分计划
// 限额与临时限流；W5-27 条目 2 补成功面）。**先清后写**：serve 的换上游链复用
// 同一 ResponseWriter，前次尝试写入的残留头不得混入最终响应（预提交失败换上游
// 场景；单上游场景下清写等价）。
func copyRateLimitHeaders(dst, src http.Header) {
	for k := range dst {
		if strings.HasPrefix(strings.ToLower(k), "anthropic-ratelimit-") {
			dst.Del(k)
		}
	}
	for k, vs := range src {
		if strings.HasPrefix(strings.ToLower(k), "anthropic-ratelimit-") {
			for _, v := range vs {
				dst.Add(k, v)
			}
		}
	}
}

// looksLikeSSE content-type 声明 event-stream，或首字节形态像 SSE（容忍不声明
// 的代理——B1 的反向守卫：200+非 SSE 体是判流失败的根因，提交前必须识别）。
func looksLikeSSE(resp *http.Response, br *bufio.Reader) bool {
	if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
		return true
	}
	head, err := br.Peek(16)
	if err != nil && len(head) == 0 {
		return false
	}
	s := strings.TrimSpace(string(head))
	return strings.HasPrefix(s, "data:") || strings.HasPrefix(s, "event:") || strings.HasPrefix(s, ":")
}

// ensureRequestID 响应头 request-id（B10：客户端诊断关联的唯一来源；
// 上游有则透传，无则用本请求的自造 ID——W5-19 条目 4：与 per-request 记账
// 日志同源，日志 ↔ 响应头可关联）。
func (s *Server) ensureRequestID(w http.ResponseWriter, resp *http.Response, fallback string) {
	if v := resp.Header.Get("request-id"); v != "" {
		w.Header().Set("request-id", v)
		return
	}
	if v := resp.Header.Get("x-request-id"); v != "" {
		w.Header().Set("request-id", v)
		return
	}
	if fallback == "" {
		fallback = newRequestID()
	}
	w.Header().Set("request-id", fallback)
}

// ---------------------------------------------------------------------------
// 模型发现（GET /v1/models，W5-27 条目 3）
// ---------------------------------------------------------------------------

// modelDiscoveryEntry 模型发现条目（最小形状）：`id` + 两个上限字段。CC 的解析
// schema 恰为 `{id, max_input_tokens?, max_tokens?}`（cli.js 2.1.88 modelCapabilities
// 路径的 zod schema；`max_input_tokens` ≥ 1e5 时被用作该模型的上下文窗口覆盖值）；
// 新版 picker 发现另读 `id`/`display_name?`/`description?`（多余字段被忽略）——
// `display_name`/`description` 我们有意不发（id 可被客户端识别）。上限字段
// `omitempty`（W5-28 条目 1）：静态表恒带值；配置表条目未声明上限时省略字段
// （zod optional——客户端按自己的缺省能力假设；绝不发明数值）。
type modelDiscoveryEntry struct {
	ID             string `json:"id"`
	MaxInputTokens int    `json:"max_input_tokens,omitempty"`
	MaxTokens      int    `json:"max_tokens,omitempty"`
}

// defaultModelDiscovery 静态模型表（一线四模型；值来源：官方 Models overview
// 2026-10-14 实读——四模型均 1M 上下文 / 128K 输出）。**维护纪律**：静态快照、
// 不随上游探测（模型别名映射表 = 独立缺口，未做）；随官方 lineup 更新。id 含
// `claude` 才被 CC 的发现过滤保留（官方 §Model discovery）。
var defaultModelDiscovery = []modelDiscoveryEntry{
	{ID: "claude-fable-5-1", MaxInputTokens: 1000000, MaxTokens: 128000},
	{ID: "claude-opus-5-5", MaxInputTokens: 1000000, MaxTokens: 128000},
	{ID: "claude-sonnet-5-5", MaxInputTokens: 1000000, MaxTokens: 128000},
	{ID: "claude-haiku-5-5", MaxInputTokens: 1000000, MaxTokens: 128000},
}

// modelDiscoveryEntries 发现面模型表（W5-28 条目 1 配置化）：配置了顶层
// `models:` → 逐条从配置读（id + 上限字段；未声明上限的条目省略字段——客户端
// 按自己的缺省能力假设）；未配置 → 静态四模型表（W5-27 缺省行为不变）。
// 检索面（handleModelRetrieve）与发现面同源。
func (s *Server) modelDiscoveryEntries() []modelDiscoveryEntry {
	if len(s.cfg.Models) == 0 {
		return defaultModelDiscovery
	}
	out := make([]modelDiscoveryEntry, 0, len(s.cfg.Models))
	for _, m := range s.cfg.Models {
		out = append(out, modelDiscoveryEntry{ID: m.ID, MaxInputTokens: m.MaxInputTokens, MaxTokens: m.MaxTokens})
	}
	return out
}

// handleModels GET /v1/models：模型发现（官方可选端点，`llm-gateway-protocol`
// §Model discovery）。契约：无重定向（本端点直接服务）、3s 内应答（本实现即时
// 返回）、`{"data":[...]}`；`anthropic-beta`/`betas` 与任意未知头容忍（本服务不
// 校验下游请求头）。非 GET → 405（claude 错误信封）。数据源 = 配置表或静态
// 四模型（modelDiscoveryEntries，W5-28 条目 1）。
func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	s.diag.requests.Add(1)
	if r.Method != http.MethodGet {
		s.writeError(w, registry.FormatClaudeMessages, http.StatusMethodNotAllowed,
			&ir.Error{Type: "invalid_request_error", Message: "method not allowed", Source: "any2api"})
		return
	}
	body, err := json.Marshal(struct {
		Data []modelDiscoveryEntry `json:"data"`
	}{Data: s.modelDiscoveryEntries()})
	if err != nil {
		s.writeError(w, registry.FormatClaudeMessages, http.StatusInternalServerError,
			&ir.Error{Type: "api_error", Message: "internal error", Source: "any2api"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// handleModelRetrieve GET /v1/models/{id}：模型检索（SDK `models.retrieve` 面，
// 形态 `/v1/models/${model_id}?beta=true`——查询串不参与路由，`?beta=true`
// 天然容忍；W5-28 条目 2）。静态表检索（与发现面同源：配置表或缺省四模型）；
// 未命中 / 空 id / 多段子路径 → 404 `not_found_error`（claude 错误信封）。
// 响应形状 = 发现面条目（id + 可选上限字段；消费端 strip 多余/缺失可选字段——
// 与发现面同一 zod/消费纪律）。非 GET → 405。
func (s *Server) handleModelRetrieve(w http.ResponseWriter, r *http.Request) {
	s.diag.requests.Add(1)
	if r.Method != http.MethodGet {
		s.writeError(w, registry.FormatClaudeMessages, http.StatusMethodNotAllowed,
			&ir.Error{Type: "invalid_request_error", Message: "method not allowed", Source: "any2api"})
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/models/")
	if id == "" || strings.Contains(id, "/") {
		s.writeError(w, registry.FormatClaudeMessages, http.StatusNotFound,
			&ir.Error{Type: "not_found_error", Message: "model not found: " + id, Source: "any2api"})
		return
	}
	for _, m := range s.modelDiscoveryEntries() {
		if m.ID != id {
			continue
		}
		body, err := json.Marshal(m)
		if err != nil {
			s.writeError(w, registry.FormatClaudeMessages, http.StatusInternalServerError,
				&ir.Error{Type: "api_error", Message: "internal error", Source: "any2api"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return
	}
	s.writeError(w, registry.FormatClaudeMessages, http.StatusNotFound,
		&ir.Error{Type: "not_found_error", Message: "model not found: " + id, Source: "any2api"})
}

// writeError 按入口协议渲染并写出错误信封（服务自产错误路径）。
func (s *Server) writeError(w http.ResponseWriter, entry registry.Format, status int, e *ir.Error) {
	body, err := pairs.RenderErrorFor(entry, e)
	if err != nil {
		body = []byte(`{"error":{"message":"internal error","type":"api_error"}}`)
	}
	if w.Header().Get("request-id") == "" {
		w.Header().Set("request-id", newRequestID())
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// newRequestID 自造请求 ID（诊断关联用；非安全敏感）。
func newRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("req_%d", time.Now().UnixNano())
	}
	return "req_" + hex.EncodeToString(b[:])
}
