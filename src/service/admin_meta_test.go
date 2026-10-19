// admin_meta_test.go —— dashboard P0-2 配置面/系统面测试（board A2A-L19-W73-1；
// 设计 `docs/admin-dashboard.md` §5.8/§6.1 #6-7/§9 P0-2）。
//
// 装置：真 Server + 真 `Handler()`（**server.go 的路由注册面本体**，含 admin_key
// 自门控与 adminAuth）+ httptest；哨兵凭据配置（每个哨兵都必须出现在配置里，否则
// 检测器对它是空转）。
//
// 反证锚点（回退即红；负控实测见报告 §自审 B）：
//   - `adminSanitizeURL` 换成原样回 `BaseURL`/`Proxy` → TestAdminMetaConfigRedaction
//     红（`user:pass` / `SECRET-query` / `SECRET-workspace` 命中哨兵）；
//   - `Admin.KeyConfigured` 换成回密钥值 → TestAdminMetaConfigRedaction 红
//     （`SECRET-admin`）；
//   - `adminMetaPoolSize` 换成回凭据列表 → TestAdminMetaConfigRedaction 红
//     （`SECRET-cred`）；
//   - 响应结构换成「遍历 Config 快照」→ TestAdminMetaConfigAllowlist 红
//     （禁名字段名出现：admin_key/api_key/credentials/...）；
//   - `registerAdminMeta` 去掉自门控 / 漏挂 adminAuth → TestAdminMetaAuth 红；
//   - buildinfo 变量换成硬编码 "unknown" → TestAdminMetaSystemBuildFallback 红
//     （注入态断言）；
//   - `audit_rows` 用 0 冒充可用 / 路径回全路径 → TestAdminMetaSystemStoreStats 红。
package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 装置：哨兵配置 + 启动 + 泄漏检测器
// ---------------------------------------------------------------------------

// adminMetaKey 测试管理密钥（与生产同形态；不进任何日志/报告）。
const adminMetaKey = "meta-test-key"

// adminMetaLeakKey 带哨兵的测试管理密钥（= 哨兵表第一项：响应体出现它 = 泄漏；
// 请求方也用它，故「配了 key 且只回布尔」两条都被同一份配置证到）。
const adminMetaLeakKey = "SECRET-admin"

// adminMetaOrgID claude_web org_id 哨兵（org_id 只接受 hex+dash 的路径安全段——
// 见 isSafePathSegment，故哨兵本身必须是合法形态）。
const adminMetaOrgID = "deadbeef-cafe-4321-abcd"

// adminMetaHomeDirName 模拟「家目录」的名字（配置里的 store/debug 路径都在它下面；
// 响应体里出现该名字 = 路径前缀泄漏——只回 basename 的纪律）。
const adminMetaHomeDirName = "home-dwgx1"

// adminMetaSecretSentinels 泄漏哨兵：注入到配置里的凭据/标识原文。响应体里出现
// 任何一个 = 泄漏（检测器本体；`TestAdminMetaSecretLeakDetectorHasTeeth` 证明它
// 能在坏样本上变红——绿不是装饰）。
var adminMetaSecretSentinels = []string{
	"SECRET-admin",  // admin_key
	"SECRET-key",    // upstream.api_key
	"SECRET-cred",   // upstream.credentials[]
	"SECRET-access", // public.access_keys[]
	"SECRET-query",  // base_url 的查询串
	"SECRET-workspace",
	"SECRET-device",
	adminMetaOrgID, // upstream.org_id
	"user:pass",    // base_url / proxy 的 userinfo
}

// adminMetaLeaks 响应体里命中的哨兵（空 = 无泄漏）。
func adminMetaLeaks(body string) []string {
	var hits []string
	for _, s := range adminMetaSecretSentinels {
		if strings.Contains(body, s) {
			hits = append(hits, s)
		}
	}
	return hits
}

// adminMetaLeakFixture 带全部哨兵的配置（每个哨兵都真的在配置里）+ 模拟家目录名。
func adminMetaLeakFixture(t *testing.T, key string) *Config {
	t.Helper()
	home := filepath.Join(t.TempDir(), adminMetaHomeDirName)
	return &Config{
		Listen:           "127.0.0.1:0",
		AdminKey:         key,
		HeartbeatSeconds: 15,
		DebugCaptureDir:  filepath.Join(home, "captures"),
		Store:            &StoreSection{Enabled: true, Path: filepath.Join(home, "data", "any2api.db")},
		Public: &PublicSection{
			Enabled:        true,
			AccessMode:     publicAccessModeGated,
			AccessKeys:     []string{"SECRET-access"},
			TrustedProxies: []string{"10.0.0.0/8"},
		},
		Upstreams: []Upstream{
			{
				Name: "alpha", Protocol: ProtocolAnthropic,
				BaseURL: "https://user:pass@example.com/v1?key=SECRET-query",
				APIKey:  "SECRET-key", WorkspaceID: "SECRET-workspace",
				Proxy: "socks5://user:pass@proxy:1080", TLSFingerprint: "chrome",
			},
			{Name: "beta", Protocol: ProtocolClaudeWeb, Credentials: []string{"SECRET-cred"}, OrgID: adminMetaOrgID},
			{Name: "gamma", Protocol: ProtocolChatGPTWeb, DeviceID: "SECRET-device"},
		},
		Models: []ModelEntry{{ID: "m-1", UpstreamModel: "up-1", MaxInputTokens: 1000, MaxTokens: 100}},
	}
}

// adminMetaConfig 瘦配置（无哨兵；store 关——测系统面/鉴权的缺省形态）：
// 上游沿用 uiTestConfig 的本地假上游（测试绝不连真实上游）。
func adminMetaConfig(t *testing.T, key string) *Config {
	t.Helper()
	cfg := uiTestConfig()
	cfg.HeartbeatSeconds = 15
	cfg.AdminKey = key
	return cfg
}

// adminMetaStart 启动被测服务：cfg 校验 → NewServer → **真 Handler()**。
func adminMetaStart(t *testing.T, cfg *Config) (*Server, *httptest.Server) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(func() {
		srv.Close()
		_ = s.CloseStore()
	})
	return s, srv
}

// adminMetaPaths P0-2 的两条端点（鉴权/方法断言集）。
var adminMetaPaths = []string{"/admin/config", "/admin/system"}

// ---------------------------------------------------------------------------
// 契约面解码结构（json 键 = 响应形状；**不依赖实现侧结构**——形状改了这里就红）
// ---------------------------------------------------------------------------

type adminMetaWireAdmin struct {
	KeyConfigured bool `json:"key_configured"`
	LogCapacity   int  `json:"log_capacity"`
}

type adminMetaWireUI struct {
	Enabled         bool `json:"enabled"`
	RateLimitPerMin int  `json:"rate_limit_per_min"`
}

type adminMetaWireStore struct {
	Enabled bool   `json:"enabled"`
	Path    string `json:"path"`
}

type adminMetaWirePublicLimits struct {
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

type adminMetaWirePublic struct {
	Enabled         bool                      `json:"enabled"`
	AccessMode      string                    `json:"access_mode"`
	AccessKeysCount int                       `json:"access_keys_count"`
	TrustedProxies  []string                  `json:"trusted_proxies"`
	Limits          adminMetaWirePublicLimits `json:"limits"`
}

type adminMetaWireUpstream struct {
	Index                 int    `json:"index"`
	Name                  string `json:"name"`
	Protocol              string `json:"protocol"`
	BaseURL               string `json:"base_url"`
	PoolSize              int    `json:"pool_size"`
	TLSFingerprint        string `json:"tls_fingerprint"`
	ProxyConfigured       bool   `json:"proxy_configured"`
	Proxy                 string `json:"proxy"`
	APIKeyConfigured      bool   `json:"api_key_configured"`
	WorkspaceIDConfigured bool   `json:"workspace_id_configured"`
	OrgIDConfigured       bool   `json:"org_id_configured"`
	DeviceIDConfigured    bool   `json:"device_id_configured"`
	TaskFace              string `json:"task_face"`
}

type adminMetaWireModel struct {
	ID             string `json:"id"`
	UpstreamModel  string `json:"upstream_model"`
	MaxInputTokens int    `json:"max_input_tokens"`
	MaxTokens      int    `json:"max_tokens"`
}

type adminMetaWireFailureRate struct {
	Enabled       bool    `json:"enabled"`
	MinRequests   int     `json:"min_requests"`
	Threshold     float64 `json:"threshold"`
	WindowSeconds int     `json:"window_seconds"`
}

type adminMetaWireLimits struct {
	HeartbeatSeconds           int                      `json:"heartbeat_seconds"`
	CooldownFailures           int                      `json:"cooldown_failures"`
	CooldownSeconds            int                      `json:"cooldown_seconds"`
	CooldownLadderSeconds      []int                    `json:"cooldown_ladder_seconds"`
	FailureRate                adminMetaWireFailureRate `json:"failure_rate"`
	RetrySameUpstream          int                      `json:"retry_same_upstream"`
	RetrySameUpstreamDelaysMs  []int                    `json:"retry_same_upstream_delays_ms"`
	FirstByteTimeoutSeconds    int                      `json:"first_byte_timeout_seconds"`
	UpstreamIdleTimeoutSeconds int                      `json:"upstream_idle_timeout_seconds"`
	FailoverPolicy             map[string]int           `json:"failover_policy"`
}

type adminMetaWireDebug struct {
	CaptureConfigured bool   `json:"capture_configured"`
	CaptureDir        string `json:"capture_dir"`
}

type adminMetaWireConfig struct {
	Now             int64                   `json:"now"`
	Listen          string                  `json:"listen"`
	Admin           adminMetaWireAdmin      `json:"admin"`
	UI              adminMetaWireUI         `json:"ui"`
	Store           adminMetaWireStore      `json:"store"`
	Public          adminMetaWirePublic     `json:"public"`
	Models          []adminMetaWireModel    `json:"models"`
	Upstreams       []adminMetaWireUpstream `json:"upstreams"`
	Limits          adminMetaWireLimits     `json:"limits"`
	Debug           adminMetaWireDebug      `json:"debug"`
	TaskFaceDegrade string                  `json:"task_face_degrade"`
}

type adminMetaWireRuntime struct {
	GoVersion      string  `json:"go_version"`
	PID            int     `json:"pid"`
	Goroutines     int     `json:"goroutines"`
	MemAllocBytes  uint64  `json:"mem_alloc_bytes"`
	MemSysBytes    uint64  `json:"mem_sys_bytes"`
	GCCount        uint32  `json:"gc_count"`
	GCPauseTotalMs int64   `json:"gc_pause_total_ms"`
	UptimeSeconds  float64 `json:"uptime_seconds"`
}

type adminMetaWireSystemStore struct {
	Enabled            bool   `json:"enabled"`
	Path               string `json:"path"`
	SizeBytes          int64  `json:"size_bytes"`
	SizeAvailable      bool   `json:"size_available"`
	Sessions           int    `json:"sessions"`
	SessionsAvailable  bool   `json:"sessions_available"`
	AuditRows          int    `json:"audit_rows"`
	AuditRowsAvailable bool   `json:"audit_rows_available"`
}

type adminMetaWireBuild struct {
	Commit    string `json:"commit"`
	Date      string `json:"date"`
	GoVersion string `json:"go_version"`
}

type adminMetaWireSystem struct {
	Now     int64                    `json:"now"`
	Runtime adminMetaWireRuntime     `json:"runtime"`
	Store   adminMetaWireSystemStore `json:"store"`
	Build   adminMetaWireBuild       `json:"build"`
}

// ---------------------------------------------------------------------------
// 1. 消毒器（纯单测：URL / 路径）
// ---------------------------------------------------------------------------

func TestAdminMetaURLSanitizer(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"   ", ""},
		{"https://user:pass@example.com/v1?key=SECRET#frag", "https://example.com/v1"},
		{"https://example.com", "https://example.com"},
		{"http://127.0.0.1:8787/v1/messages", "http://127.0.0.1:8787/v1/messages"},
		{"socks5://user:pass@proxy:1080", "socks5://proxy:1080"},
		{"https://example.com/v1#frag", "https://example.com/v1"},
		{"https://example.com/sk-abcdef123456", "https://example.com/sk-[redacted]"}, // 路径里的明文形态也投影
		{"not-a-url", adminMetaInvalidURL},
		{"localhost:8787", adminMetaInvalidURL}, // 无 host（opaque 形态）→ 不猜
		{"://bad", adminMetaInvalidURL},
		{"https://[::1", adminMetaInvalidURL}, // 解析失败（错括号）
	}
	for _, c := range cases {
		if got := adminSanitizeURL(c.in); got != c.want {
			t.Errorf("adminSanitizeURL(%q) = %q（want %q）", c.in, got, c.want)
		}
		if strings.Contains(adminSanitizeURL(c.in), "user:pass") {
			t.Errorf("adminSanitizeURL(%q) 回了 userinfo", c.in)
		}
	}
}

func TestAdminMetaPathBasename(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"data/any2api.db", "any2api.db"},
		{filepath.Join("C:", "Users", "dwgx1", "data", "any2api.db"), "any2api.db"},
		{"captures", "captures"},
		{".", ""},
		{"..", ""},
		{"/", ""},
	}
	for _, c := range cases {
		if got := adminSanitizePathBasename(c.in); got != c.want {
			t.Errorf("adminSanitizePathBasename(%q) = %q（want %q）", c.in, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// 2. 配置面：泄漏（绿）+ 形状（白名单/非 null 数组）+ 有效值
// ---------------------------------------------------------------------------

func TestAdminMetaConfigRedaction(t *testing.T) {
	cfg := adminMetaLeakFixture(t, adminMetaLeakKey)
	_, srv := adminMetaStart(t, cfg)

	resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/config", adminMetaLeakKey, "")
	if resp.status != http.StatusOK {
		t.Fatalf("GET /admin/config = %d body=%s", resp.status, resp.body)
	}
	if hits := adminMetaLeaks(resp.body); len(hits) > 0 {
		t.Fatalf("响应体泄漏哨兵 %v\nbody=%s", hits, resp.body)
	}
	if strings.Contains(resp.body, adminMetaHomeDirName) {
		t.Fatalf("路径前缀泄漏（只准回 basename）：%s", resp.body)
	}

	var wire adminMetaWireConfig
	if err := json.Unmarshal([]byte(resp.body), &wire); err != nil {
		t.Fatalf("decode: %v body=%s", err, resp.body)
	}
	// 密钥只有布尔；条目只有计数。
	if !wire.Admin.KeyConfigured {
		t.Errorf("admin.key_configured = false（配了 key 却报未配）")
	}
	if wire.Public.AccessKeysCount != 1 {
		t.Errorf("public.access_keys_count = %d（want 1：只回条目数）", wire.Public.AccessKeysCount)
	}
	// 消毒后的 URL 仍在（运营者可读：打到哪），凭据字节全无。
	if len(wire.Upstreams) != 3 {
		t.Fatalf("upstreams = %d 条（want 3）body=%s", len(wire.Upstreams), resp.body)
	}
	alpha := wire.Upstreams[0]
	if alpha.BaseURL != "https://example.com/v1" {
		t.Errorf("alpha.base_url = %q（want 消毒后的 https://example.com/v1）", alpha.BaseURL)
	}
	if alpha.Proxy != "socks5://proxy:1080" || !alpha.ProxyConfigured {
		t.Errorf("alpha.proxy = %q configured=%v（want socks5://proxy:1080 + true）", alpha.Proxy, alpha.ProxyConfigured)
	}
	if alpha.PoolSize != 1 || !alpha.APIKeyConfigured || !alpha.WorkspaceIDConfigured {
		t.Errorf("alpha 池/凭据布尔错：%+v", alpha)
	}
	if !wire.Upstreams[1].OrgIDConfigured || wire.Upstreams[1].PoolSize != 1 {
		t.Errorf("beta（credentials 池）投影错：%+v", wire.Upstreams[1])
	}
	if !wire.Upstreams[2].DeviceIDConfigured {
		t.Errorf("gamma.device_id_configured = false（配了 device_id 却报未配）")
	}
	// store / debug 只回 basename。
	if wire.Store.Path != "any2api.db" {
		t.Errorf("store.path = %q（want basename any2api.db）", wire.Store.Path)
	}
	if wire.Debug.CaptureDir != "captures" || !wire.Debug.CaptureConfigured {
		t.Errorf("debug.capture_dir = %q configured=%v（want captures + true）", wire.Debug.CaptureDir, wire.Debug.CaptureConfigured)
	}
}

func TestAdminMetaConfigAllowlist(t *testing.T) {
	cfg := adminMetaLeakFixture(t, adminMetaLeakKey)
	_, srv := adminMetaStart(t, cfg)
	resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/config", adminMetaLeakKey, "")
	if resp.status != http.StatusOK {
		t.Fatalf("GET /admin/config = %d body=%s", resp.status, resp.body)
	}

	var raw map[string]any
	if err := json.Unmarshal([]byte(resp.body), &raw); err != nil {
		t.Fatalf("decode: %v body=%s", err, resp.body)
	}
	// ① 白名单字段必在。
	for _, key := range []string{"now", "listen", "admin", "ui", "store", "public", "models", "upstreams", "limits", "debug", "task_face_degrade"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("缺字段 %s（白名单面不许缺）body=%s", key, resp.body)
		}
	}
	// ② 空表恒 `[]` 不 null（空态 = 0 条，不是「未知」）。
	for _, path := range []string{
		"upstreams", "models", "public.trusted_proxies",
		"limits.cooldown_ladder_seconds", "limits.retry_same_upstream_delays_ms",
	} {
		adminMetaRawList(t, raw, path)
	}
	// ③ 禁名键绝不出现（递归；`*_configured`/`*_count` 归一化后不命中）。
	bad := adminMetaForbiddenKeysIn(raw, "")
	if len(bad) > 0 {
		t.Fatalf("响应里出现禁名字段 %v（白名单投影被绕过）body=%s", bad, resp.body)
	}
	// ④ admin.key_configured 必须是 **JSON 布尔**（回字符串/数字都是错误形状）。
	admin, ok := raw["admin"].(map[string]any)
	if !ok {
		t.Fatalf("admin 不是对象：%T", raw["admin"])
	}
	if _, ok := admin["key_configured"].(bool); !ok {
		t.Fatalf("admin.key_configured 不是布尔：%T", admin["key_configured"])
	}
	// ⑤ debug 关闭档 = `[]`/"" 形态（不带路径）。
	var wire adminMetaWireConfig
	if err := json.Unmarshal([]byte(resp.body), &wire); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if wire.Listen != cfg.Listen || wire.Now < 1_700_000_000_000 {
		t.Errorf("listen/now 形状错：listen=%q now=%d", wire.Listen, wire.Now)
	}
}

// adminMetaForbiddenKeys 禁名键（归一化：去 `-`/`_` + 小写）——值与条目数之外的
// 任何形态都不该出现。
var adminMetaForbiddenKeys = []string{
	"adminkey", "apikey", "credentials", "accesskeys", "workspaceid",
	"orgid", "deviceid", "password", "userinfo", "authorization", "cookie", "secret",
}

// adminMetaForbiddenKeysIn 递归找禁名键（返回 JSON 路径；空 = 干净）。
func adminMetaForbiddenKeysIn(v any, path string) []string {
	var bad []string
	switch x := v.(type) {
	case map[string]any:
		for k, val := range x {
			n := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(k, "-", ""), "_", ""))
			for _, f := range adminMetaForbiddenKeys {
				if n == f {
					bad = append(bad, path+"."+k)
				}
			}
			bad = append(bad, adminMetaForbiddenKeysIn(val, path+"."+k)...)
		}
	case []any:
		for i, val := range x {
			bad = append(bad, adminMetaForbiddenKeysIn(val, fmt.Sprintf("%s[%d]", path, i))...)
		}
	}
	return bad
}

// adminMetaRawList 断言 raw JSON 的 path 字段是数组（null/缺失/非数组 = 报错）。
func adminMetaRawList(t *testing.T, raw map[string]any, path string) {
	t.Helper()
	var cur any = raw
	for _, seg := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			t.Errorf("字段 %s 的父级不是对象（%s）", path, seg)
			return
		}
		cur, ok = m[seg]
		if !ok {
			t.Errorf("缺字段 %s（空表必须是 []，不是缺字段）", path)
			return
		}
	}
	if _, ok := cur.([]any); !ok {
		t.Errorf("字段 %s 不是 JSON 数组（null/标量都是错误形状）：%#v", path, cur)
	}
}

func TestAdminMetaConfigValues(t *testing.T) {
	_, srv := adminMetaStart(t, adminMetaLeakFixture(t, adminMetaLeakKey))
	var wire adminMetaWireConfig
	adminAPIGetJSON(t, srv.URL+"/admin/config", adminMetaLeakKey, &wire)

	// 有效值 = 与请求路径同源的口径（缺省已填充，不是「零值」）。
	if wire.Limits.HeartbeatSeconds != DefaultHeartbeatSeconds {
		t.Errorf("heartbeat_seconds = %d（want %d）", wire.Limits.HeartbeatSeconds, DefaultHeartbeatSeconds)
	}
	if wire.Limits.CooldownFailures != DefaultCooldownFailures || wire.Limits.CooldownSeconds != DefaultCooldownSeconds {
		t.Errorf("cooldown 缺省错：%+v", wire.Limits)
	}
	fr := wire.Limits.FailureRate
	if !fr.Enabled || fr.MinRequests != DefaultFailureRateMinRequests ||
		fr.Threshold != DefaultFailureRateThreshold || fr.WindowSeconds != DefaultFailureRateWindowSeconds {
		t.Errorf("failure_rate 缺省错：%+v", fr)
	}
	if wire.Limits.RetrySameUpstream != 0 || len(wire.Limits.RetrySameUpstreamDelaysMs) != 2 {
		t.Errorf("retry 缺省错：%+v", wire.Limits)
	}
	if wire.Limits.CooldownLadderSeconds == nil || len(wire.Limits.CooldownLadderSeconds) != 0 {
		t.Errorf("cooldown_ladder_seconds = %#v（未配 = 空表）", wire.Limits.CooldownLadderSeconds)
	}
	if wire.Limits.FailoverPolicy == nil || len(wire.Limits.FailoverPolicy) != 0 {
		t.Errorf("failover_policy = %#v（未配 = 空对象）", wire.Limits.FailoverPolicy)
	}
	pl := wire.Public.Limits
	if pl.MaxInflight != DefaultPublicMaxInflight || pl.QueueSize != DefaultPublicQueueSize ||
		pl.QueueTimeoutSeconds != int(DefaultPublicQueueTimeout/time.Second) ||
		pl.CredentialMinGapSeconds != int(DefaultCredentialMinGap/time.Second) {
		t.Errorf("public 闸缺省错：%+v", pl)
	}
	if pl.MaxCostClass != DefaultPublicMaxCostClass || pl.DailyBudgetUnits != DefaultPublicDailyBudgetUnits || !pl.DailyBudgetEnabled {
		t.Errorf("public 预算缺省错：%+v", pl)
	}
	if pl.MaxBodyBytes != DefaultPublicMaxBodyBytes || pl.MaxMessages != DefaultPublicMaxMessages ||
		pl.MaxMessageChars != DefaultPublicMaxMessageChars {
		t.Errorf("public 体上限缺省错：%+v", pl)
	}
	if wire.Admin.LogCapacity != DefaultAdminLogCapacity {
		t.Errorf("admin.log_capacity = %d（want %d）", wire.Admin.LogCapacity, DefaultAdminLogCapacity)
	}
	if len(wire.Models) != 1 || wire.Models[0].ID != "m-1" || wire.Models[0].UpstreamModel != "up-1" {
		t.Errorf("models 投影错：%+v", wire.Models)
	}
	if wire.TaskFaceDegrade != taskFaceDegradeOff {
		t.Errorf("task_face_degrade = %q（未配 = off）", wire.TaskFaceDegrade)
	}
	if wire.Upstreams[0].TaskFace != taskFaceManaged {
		t.Errorf("upstreams[0].task_face = %q（未配 = managed）", wire.Upstreams[0].TaskFace)
	}
	if wire.Upstreams[0].TLSFingerprint != "chrome" {
		t.Errorf("upstreams[0].tls_fingerprint = %q", wire.Upstreams[0].TLSFingerprint)
	}
	if wire.Store.Enabled != true {
		t.Errorf("store.enabled = %v（fixture 开了 store）", wire.Store.Enabled)
	}
}

// TestAdminMetaSecretLeakDetectorHasTeeth 负控（**检测器本体**）：把未脱敏的原始
// 配置直写进一个 JSON 坏样本，检测器必须命中**全部**哨兵——否则
// TestAdminMetaConfigRedaction 的绿是装饰（「不能红的门是装饰」）。
func TestAdminMetaSecretLeakDetectorHasTeeth(t *testing.T) {
	cfg := adminMetaLeakFixture(t, adminMetaLeakKey)
	dump := map[string]any{
		"listen":     cfg.Listen,
		"admin_key":  cfg.AdminKey,
		"access_key": cfg.Public.AccessKeys,
		"store":      map[string]any{"path": cfg.Store.Path},
		"debug":      map[string]any{"capture_dir": cfg.DebugCaptureDir},
		"upstreams": []any{
			map[string]any{
				"name": "alpha", "base_url": cfg.Upstreams[0].BaseURL, "api_key": cfg.Upstreams[0].APIKey,
				"proxy": cfg.Upstreams[0].Proxy, "workspace_id": cfg.Upstreams[0].WorkspaceID,
			},
			map[string]any{"name": "beta", "credentials": cfg.Upstreams[1].Credentials, "org_id": cfg.Upstreams[1].OrgID},
			map[string]any{"name": "gamma", "device_id": cfg.Upstreams[2].DeviceID},
		},
	}
	raw, err := json.Marshal(dump)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	hits := adminMetaLeaks(string(raw))
	if len(hits) != len(adminMetaSecretSentinels) {
		t.Fatalf("检测器对坏样本只命中 %d/%d 个哨兵：%v\n坏样本=%s",
			len(hits), len(adminMetaSecretSentinels), hits, raw)
	}
	if !strings.Contains(string(raw), adminMetaHomeDirName) || strings.Contains(strings.Join(adminMetaSecretSentinels, " "), adminMetaHomeDirName) {
		t.Fatalf("坏样本未含家目录哨兵 / 哨兵表被污染：%s", raw)
	}
}

// ---------------------------------------------------------------------------
// 3. 系统面：build 注入面 / runtime + store 形状 / store 计数
// ---------------------------------------------------------------------------

func TestAdminMetaSystemBuildFallback(t *testing.T) {
	// 未注入态 + 注入态都测：**只测 "unknown" 会把「硬编码 unknown」放过去**。
	prevCommit, prevDate := BuildCommit, BuildDate
	t.Cleanup(func() { BuildCommit, BuildDate = prevCommit, prevDate })

	_, srv := adminMetaStart(t, adminMetaConfig(t, adminMetaKey))

	BuildCommit, BuildDate = "unknown", "unknown"
	var wire adminMetaWireSystem
	adminAPIGetJSON(t, srv.URL+"/admin/system", adminMetaKey, &wire)
	if wire.Build.Commit != "unknown" || wire.Build.Date != "unknown" {
		t.Fatalf("未注入态 build = %+v（want 双 unknown 哨兵）", wire.Build)
	}

	BuildCommit, BuildDate = "deadbeef", "2026-10-18T00:00:00Z"
	adminAPIGetJSON(t, srv.URL+"/admin/system", adminMetaKey, &wire)
	if wire.Build.Commit != "deadbeef" || wire.Build.Date != "2026-10-18T00:00:00Z" {
		t.Fatalf("注入态 build = %+v（-ldflags 注入面未接线：变量没被读）", wire.Build)
	}
	if wire.Build.GoVersion != runtime.Version() {
		t.Errorf("build.go_version = %q（want %q）", wire.Build.GoVersion, runtime.Version())
	}
}

func TestAdminMetaSystemShape(t *testing.T) {
	_, srv := adminMetaStart(t, adminMetaConfig(t, adminMetaKey)) // store 关
	resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/system", adminMetaKey, "")
	if resp.status != http.StatusOK {
		t.Fatalf("GET /admin/system = %d body=%s", resp.status, resp.body)
	}
	var wire adminMetaWireSystem
	if err := json.Unmarshal([]byte(resp.body), &wire); err != nil {
		t.Fatalf("decode: %v body=%s", err, resp.body)
	}
	rt := wire.Runtime
	if rt.GoVersion != runtime.Version() {
		t.Errorf("runtime.go_version = %q（want %q）", rt.GoVersion, runtime.Version())
	}
	if rt.PID != os.Getpid() {
		t.Errorf("runtime.pid = %d（want %d）", rt.PID, os.Getpid())
	}
	if rt.Goroutines <= 0 || rt.MemSysBytes == 0 {
		t.Errorf("runtime 计数不合理：%+v", rt)
	}
	if rt.UptimeSeconds < 0 || rt.UptimeSeconds > 3600 {
		t.Errorf("runtime.uptime_seconds = %v（进程口径，不可能超 1h）", rt.UptimeSeconds)
	}
	if rt.GCPauseTotalMs < 0 {
		t.Errorf("runtime.gc_pause_total_ms = %d", rt.GCPauseTotalMs)
	}
	// store 关闭档：**enabled=false + 全 0 + 可用性 false**（0 不冒充真实值）。
	st := wire.Store
	if st.Enabled || st.Path != "" || st.SizeBytes != 0 || st.SizeAvailable ||
		st.Sessions != 0 || st.SessionsAvailable || st.AuditRows != 0 || st.AuditRowsAvailable {
		t.Fatalf("store 关闭档形状错：%+v", st)
	}
	if wire.Now < 1_700_000_000_000 {
		t.Errorf("now = %d（want unix ms）", wire.Now)
	}
	// 白名单：系统面同样不许出现禁名键（无环境 dump、无命令行）。
	var raw map[string]any
	if err := json.Unmarshal([]byte(resp.body), &raw); err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	if bad := adminMetaForbiddenKeysIn(raw, ""); len(bad) > 0 {
		t.Fatalf("系统面出现禁名字段 %v：%s", bad, resp.body)
	}
	for _, key := range []string{"env", "environ", "cmdline", "argv", "args"} {
		if _, ok := raw[key]; ok {
			t.Fatalf("系统面不该有 %s（不做环境 dump / 命令行回显）：%s", key, resp.body)
		}
	}
}

func TestAdminMetaSystemStoreStats(t *testing.T) {
	cfg := adminMetaConfig(t, adminMetaKey)
	dbPath := filepath.Join(t.TempDir(), "any2api.db")
	cfg.Store = &StoreSection{Enabled: true, Path: dbPath}
	s, srv := adminMetaStart(t, cfg)
	adminSeedSession(t, s, "sid-meta-1", "abcd1234", time.Now().Add(-time.Minute), time.Now())

	var wire adminMetaWireSystem
	adminAPIGetJSON(t, srv.URL+"/admin/system", adminMetaKey, &wire)
	st := wire.Store
	if !st.Enabled {
		t.Fatalf("store.enabled = false（配了 store）")
	}
	if st.Path != "any2api.db" || strings.ContainsAny(st.Path, `/\`) {
		t.Fatalf("store.path = %q（只准 basename——全路径/家目录前缀不回）", st.Path)
	}
	if !st.SizeAvailable || st.SizeBytes <= 0 {
		t.Fatalf("store 尺寸不可用（want stat 成功且 > 0）：%+v", st)
	}
	if !st.SessionsAvailable || st.Sessions != 1 {
		t.Fatalf("store.sessions = %d available=%v（want 1/true）", st.Sessions, st.SessionsAvailable)
	}
	if st.AuditRowsAvailable || st.AuditRows != 0 {
		t.Fatalf("store.audit_rows = %d available=%v（无 cheap count 路径 → 必须 available:false）",
			st.AuditRows, st.AuditRowsAvailable)
	}
}

// ---------------------------------------------------------------------------
// 4. 鉴权 / 方法（404 非 401：未配 key = 路由不存在）
// ---------------------------------------------------------------------------

func TestAdminMetaAuth(t *testing.T) {
	// 未配 admin_key → 两条路由根本不存在（404，不是 401）。
	_, srvNoKey := adminMetaStart(t, adminMetaConfig(t, ""))
	for _, path := range adminMetaPaths {
		resp := adminAPIReq(t, http.MethodGet, srvNoKey.URL+path, "any-key", "")
		if resp.status != http.StatusNotFound {
			t.Fatalf("未配 key GET %s = %d（want 404）body=%s", path, resp.status, resp.body)
		}
	}

	_, srv := adminMetaStart(t, adminMetaConfig(t, adminMetaKey))
	for _, path := range adminMetaPaths {
		if resp := adminAPIReq(t, http.MethodGet, srv.URL+path, "", ""); resp.status != http.StatusUnauthorized {
			t.Fatalf("无 key GET %s = %d（want 401）body=%s", path, resp.status, resp.body)
		}
		if resp := adminAPIReq(t, http.MethodGet, srv.URL+path, "wrong-key", ""); resp.status != http.StatusUnauthorized {
			t.Fatalf("错 key GET %s = %d（want 401）body=%s", path, resp.status, resp.body)
		}
		resp := adminAPIReq(t, http.MethodGet, srv.URL+path, adminMetaKey, "")
		if resp.status != http.StatusOK {
			t.Fatalf("对 key GET %s = %d（want 200）body=%s", path, resp.status, resp.body)
		}
		if got := resp.header.Get("Cache-Control"); !strings.Contains(got, "no-store") {
			t.Fatalf("GET %s 缺 Cache-Control: no-store（got %q）", path, got)
		}
	}
}

func TestAdminMetaMethodNotAllowed(t *testing.T) {
	_, srv := adminMetaStart(t, adminMetaConfig(t, adminMetaKey))
	for _, path := range adminMetaPaths {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
			resp := adminAPIReq(t, method, srv.URL+path, adminMetaKey, "")
			if resp.status != http.StatusMethodNotAllowed {
				t.Fatalf("%s %s = %d（want 405）body=%s", method, path, resp.status, resp.body)
			}
			if got := resp.header.Get("Allow"); !strings.Contains(got, http.MethodGet) {
				t.Fatalf("%s %s 的 Allow = %q（want 含 GET）", method, path, got)
			}
		}
	}
}
