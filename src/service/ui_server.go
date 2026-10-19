package service

// UI 托管面（W56-1）：前端资产（src/service/ui/，W56-2 维护）经 go:embed 编译
// 进二进制，由 /ui 路由族托管——
//
//   - GET /ui 与 /ui/ → index.html；GET /ui/<path> → 静态文件（按扩展名定
//     Content-Type；目录穿越防护；未知路径回退 index.html——SPA）；
//   - GET /ui/api/config → 前端配置 JSON（default_model / models /
//     rate_limit_per_min）；
//   - per-IP 令牌桶限流（ui.rate_limit_per_min > 0 时对 /v1/*、/v1beta* 与
//     /ui/api/* 生效；超限 429 + Retry-After；内存态、1 分钟窗、过期桶清扫）。
//
// 纪律：本文件只读 ui/ 资产（绝不写）；静态面只经 embed FS 读文件（fs 路径
// 合法性 + 显式守卫双重防护，绝不触碰宿主文件系统）。

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/registry"
)

//go:embed ui
var uiAssets embed.FS

// adminAssetPaths 后台管理 dashboard 的静态资产（L19-W74-1）：`/ui/admin.html` 与
// 三个伴随资产。它们是**公开**的静态面（不含任何密钥/数据——数据面 /admin/* 仍全部挂
// adminAuth），与本文件其余静态路径的区别只在**语义**：
//
//   - `GET /ui/admin`（无斜杠）→ 302 `/ui/admin.html`（人不会记得扩展名，且它必须
//     **不能**落进 SPA 回退，否则会静默渲染聊天前端首页——症状是"页面在、内容不对"）；
//   - 命中 adminAssetPaths 的路径**不做 SPA 回退**：嵌入文件缺失即显式 404（构建期
//     少一个资产必须响亮失败，而不是换一张页面顶上）。
//
// 其余 `/ui/<path>` 语义不变（见文件头注）。
var adminAssetPaths = map[string]bool{
	"admin.html":    true,
	"admin.css":     true,
	"admin.js":      true,
	"admin-data.js": true,
}

// handleUIAssets /ui 与 /ui/<path>（静态资产 + SPA 回退；见文件头注）。
func (s *Server) handleUIAssets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// "/ui" 与 "/ui/" 都归一到空子路径；"/ui/<path>" 取余段。
	sub := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/ui"), "/")
	// 后台管理面（L19-W74-1）：无扩展名入口 302 到 admin.html。
	if sub == "admin" {
		http.Redirect(w, r, "/ui/admin.html", http.StatusFound)
		return
	}
	// 根与目录形态（尾斜杠）→ index（SPA：前端路由 /ui/chat/… 也在此落点）。
	if sub == "" || strings.HasSuffix(sub, "/") {
		s.serveUIIndex(w, r)
		return
	}
	// 目录穿越防护（防御纵深）：解码后含 ".."/"."/空段的子路径一律 400——
	// 绝不拼进 embed FS。明文 `../` 由 ServeMux 先行清洗（307 重定向到等价
	// 干净 URL）；带转义（%2e%2e）的形态解码后原样到达这里，由此拦下。
	if !uiAssetPathOK(sub) {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	// 后台管理面资产：精确嵌入文件（无 SPA 回退；缺文件 = 404）。Cache-Control
	// no-cache：管理面升级后浏览器必须取到新资产（静态面公开，无凭据字节）。
	if adminAssetPaths[sub] {
		data, err := fs.ReadFile(uiAssets, "ui/"+sub)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-cache")
		http.ServeContent(w, r, sub, time.Time{}, bytes.NewReader(data))
		return
	}
	data, err := fs.ReadFile(uiAssets, "ui/"+sub)
	if err != nil {
		// 未知路径 → SPA 回退 index.html。
		s.serveUIIndex(w, r)
		return
	}
	// Content-Type：ServeContent 先按扩展名查表、再嗅探（首 512B）；含
	// Content-Length、Range 与 HEAD 处理。
	http.ServeContent(w, r, sub, time.Time{}, bytes.NewReader(data))
}

// serveUIIndex 输出 SPA 首页（/ui、/ui/、目录形态与未知路径的统一落点）。
func (s *Server) serveUIIndex(w http.ResponseWriter, r *http.Request) {
	data, err := fs.ReadFile(uiAssets, "ui/index.html")
	if err != nil {
		http.Error(w, "ui index missing", http.StatusInternalServerError)
		return
	}
	http.ServeContent(w, r, "index.html", time.Time{}, bytes.NewReader(data))
}

// uiAssetPathOK 解码后子路径是否可作为 ui/ 下的资产名（fs.ValidPath 语义的
// 显式版）：拒绝 "."/".."/空段与前导斜杠。embed.FS 自身也会拒绝非法路径
// （第二道防线），本守卫让拒绝成为显式、可测的行为。
func uiAssetPathOK(sub string) bool {
	if sub == "" || strings.HasPrefix(sub, "/") {
		return false
	}
	for _, seg := range strings.Split(sub, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// handleUIConfig GET /ui/api/config：前端配置面（W56-1）——
//
//	{"default_model": "<服务首选模型别名>", "models": [...], "rate_limit_per_min": N}
//
// 模型清单与发现面（handleModels）**同源**：`models:` 配置表逐条取 id（别名
// 即请求面可用模型名）；**无配置时给空数组**（[] 而非 null——不启用发现面的
// 静态四模型缺省，那是 /v1/models 的专属缺省；前端无模型时自行省略 model
// 字段，见 ui/app.js）。default_model = 表首条（“首选”）；空表 = ""。
func (s *Server) handleUIConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	models := make([]string, 0, len(s.cfg.Models))
	for _, m := range s.cfg.Models {
		models = append(models, m.ID)
	}
	defaultModel := ""
	if len(models) > 0 {
		defaultModel = models[0]
	}
	body, err := json.Marshal(struct {
		DefaultModel    string   `json:"default_model"`
		Models          []string `json:"models"`
		RateLimitPerMin int      `json:"rate_limit_per_min"`
	}{defaultModel, models, s.cfg.uiRateLimitPerMin()})
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// ---------------------------------------------------------------------------
// per-IP 令牌桶限流（W56-1；ui.rate_limit_per_min > 0 时启用）
// ---------------------------------------------------------------------------

// uiRateWindow 限流窗（1 分钟——brief 口径：rate_limit_per_min = 每窗容量）。
const uiRateWindow = time.Minute

// uiBucketTTL 桶过期时长：闲置 ≥ 2×窗（120s）的桶等价于新桶（全量回填只需
// 60s = 窗长；留 1 倍余量），清扫时丢弃。
const uiBucketTTL = 2 * uiRateWindow

// uiSweepInterval 清扫节流：最多每分钟扫一次（避免每请求 O(n)）。
const uiSweepInterval = uiRateWindow

// ipRateLimiter per-IP 令牌桶（内存态、1 分钟窗）：桶容量 = rate_limit_per_min，
// 回填速率 = 容量/60s（「每分钟 N 次」的平滑版：突发 ≤ N、持续速率 ≤ N/min）。
// 惰性建桶；闲置 ≥ uiBucketTTL 的桶由节流清扫（uiSweepInterval）丢弃——丢弃
// 时机晚于全量回填（60s），绝不因清扫多放令牌。now 可注入（测试）。
type ipRateLimiter struct {
	mu        sync.Mutex
	perMinute int
	rate      float64 // 令牌/秒
	burst     float64 // 桶容量
	buckets   map[string]*ipBucket
	lastSweep time.Time
	now       func() time.Time
}

// ipBucket 单 IP 桶状态。
type ipBucket struct {
	tokens float64   // 当前令牌数
	last   time.Time // 上次访问（回填/清扫基准）
}

// newIPRateLimiter 建限流器（调用方保证 perMinute > 0；防御性钳到 1）。
func newIPRateLimiter(perMinute int) *ipRateLimiter {
	if perMinute < 1 {
		perMinute = 1
	}
	return &ipRateLimiter{
		perMinute: perMinute,
		rate:      float64(perMinute) / uiRateWindow.Seconds(),
		burst:     float64(perMinute),
		buckets:   make(map[string]*ipBucket),
		now:       time.Now,
	}
}

// allow 判定 key（IP）本次是否放行；拒绝时返回建议重试等待（整秒、≥1s）。
func (l *ipRateLimiter) allow(key string) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if now.Sub(l.lastSweep) >= uiSweepInterval {
		for k, b := range l.buckets {
			if now.Sub(b.last) >= uiBucketTTL {
				delete(l.buckets, k)
			}
		}
		l.lastSweep = now
	}
	b := l.buckets[key]
	if b == nil {
		b = &ipBucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	if d := now.Sub(b.last); d > 0 {
		b.tokens = math.Min(l.burst, b.tokens+d.Seconds()*l.rate)
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	// 等 1 枚令牌：tokensNeeded×窗/容量（先乘窗再除容量，整数参数下无双重
	// 舍入误差）。
	waitSec := (1 - b.tokens) * uiRateWindow.Seconds() / float64(l.perMinute)
	retry := time.Duration(math.Ceil(waitSec)) * time.Second
	if retry < time.Second {
		retry = time.Second
	}
	return false, retry
}

// rateLimitedPath 限流面：/v1/*、/v1beta*（Gemini 入口 `/v1beta/models/` 与
// Vertex 形态 `/v1beta1/projects/`）与 /ui/api/*（A13：W56-1 只覆盖前两者，
// v1beta 入口族漏网）。其余路径（/ui 静态资产、/healthz、/admin/*）直过。
// 前缀带尾斜杠：`/v1beta` 裸路径不是入口（404），不占限流桶；`/v1beta2/*`
// 这类未注册前缀也不匹配。
func rateLimitedPath(p string) bool {
	return strings.HasPrefix(p, "/v1/") ||
		strings.HasPrefix(p, "/v1beta/") ||
		strings.HasPrefix(p, "/v1beta1/") ||
		strings.HasPrefix(p, "/ui/api/")
}

// clientIP 取源 IP（RemoteAddr 主机段；解析失败回退原串）。**不读
// X-Forwarded-For / X-Real-IP**：下游无鉴权面时该头可伪造（限流绕过），故只认
// TCP 对端；反代部署需在反代侧限流、或接受同 IP 合并计数（见报告 §边界）。
func clientIP(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// clientIP 源 IP 解析（P0-3 trust proxy 修正；设计 §4.1）：
//
//   - `public.trusted_proxies` **未配置**（或 public 面关闭）→ 现状：只认 TCP
//     对端（绝不读 XFF——防伪造）；
//   - TCP 对端命中信任网段 → 取 X-Forwarded-For **最右非信任跳**为客户端 IP
//     （右往左跳过信任代理；伪造的左侧注入条目会被其右侧的真实跳遮蔽）；
//     全为信任跳 / 头缺失 / 无可解析条目 → 回落 TCP 对端。
//
// 纪律：**只有对端可信才看 XFF**——直连客户端伪造 XFF 一律无效。
func (s *Server) clientIP(r *http.Request) string {
	peer := clientIP(r)
	if len(s.trustedProxies) == 0 {
		return peer
	}
	if !s.peerTrusted(r) {
		return peer
	}
	if ip := xffClientIP(r.Header.Get("X-Forwarded-For"), s.trustedProxies); ip != "" {
		return ip
	}
	return peer
}

// xffClientIP 取 XFF 最右非信任跳：从右往左扫，跳过信任网段命中项；第一个
// 非信任且可解析的条目即客户端 IP。无可解析的非信任条目 → ""（调用方回落）。
// 畸形条目跳过（解析失败不可能是合法客户端 IP；继续向左找）。
func xffClientIP(xff string, trusted []*net.IPNet) string {
	if xff == "" {
		return ""
	}
	parts := strings.Split(xff, ",")
	for i := len(parts) - 1; i >= 0; i-- {
		ipStr := strings.TrimSpace(parts[i])
		if ipStr == "" {
			continue
		}
		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}
		trustedHop := false
		for _, n := range trusted {
			if n.Contains(ip) {
				trustedHop = true
				break
			}
		}
		if !trustedHop {
			return ipStr
		}
	}
	return ""
}

// rateLimitWrap per-IP 令牌桶中间件（仅 rateLimitedPath 生效）。超限：
// 429 + Retry-After（整秒） + 错误信封（/v1/* 按入口协议、/ui/api/* 最小
// JSON——见 writeRateLimited）。
func (s *Server) rateLimitWrap(next http.Handler) http.Handler {
	lim := s.uiLimiter
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !rateLimitedPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		ok, retry := lim.allow(s.clientIP(r))
		if !ok {
			w.Header().Set("Retry-After", strconv.Itoa(int(retry/time.Second)))
			s.writeRateLimited(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// rateLimitFormat 限流 429 的入口信封面（与 Handler 路由表对齐；未识别
// /v1/* 路径回 claude 面——本服务最大消费面）。v1beta 命名空间 = Gemini 协议
// （`/v1beta/models/` 与 Vertex `/v1beta1/projects/`），429 走 Google 信封
// （`{"error":{"code":429,...,"status":"RESOURCE_EXHAUSTED"}}`）。
func rateLimitFormat(p string) registry.Format {
	switch {
	case strings.HasPrefix(p, "/v1/chat/completions"):
		return registry.FormatOpenAIChat
	case strings.HasPrefix(p, "/v1/responses"):
		return registry.FormatOpenAIResponses
	case strings.HasPrefix(p, "/v1/projects/"),
		strings.HasPrefix(p, "/v1beta/"),
		strings.HasPrefix(p, "/v1beta1/"):
		return registry.FormatGeminiGenerate
	default: // /v1/messages*、/v1/models* 与其余 /v1/*
		return registry.FormatClaudeMessages
	}
}

// writeRateLimited 写限流 429 响应体（Retry-After 头由调用方先行设置）。
// P0-6：拒绝面进 ring（rate_limited）+ 内存计数（消息不含 IP 原文——只记
// 事件面；设计 §4.1「rate_limited 事件进审计 + ring」）。
func (s *Server) writeRateLimited(w http.ResponseWriter, r *http.Request) {
	msg := fmt.Sprintf("rate_limited: per-IP rate limit exceeded (limit: %d/min)", s.cfg.uiRateLimitPerMin())
	s.diag.publicRateLimited.Add(1)
	s.adminLog.add(adminLogRateLimited, "", "", msg, http.StatusTooManyRequests)
	if strings.HasPrefix(r.URL.Path, "/ui/api/") {
		type errBody struct {
			Error struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		var b errBody
		b.Error.Type = "rate_limit_error"
		b.Error.Message = msg
		body, err := json.Marshal(b)
		if err != nil {
			body = []byte(`{"error":{"type":"rate_limit_error"}}`)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write(body)
		return
	}
	s.writeError(w, rateLimitFormat(r.URL.Path), http.StatusTooManyRequests,
		&ir.Error{Type: "rate_limit_error", Message: msg, Source: "any2api"})
}
