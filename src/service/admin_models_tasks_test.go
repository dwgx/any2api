// admin_models_tasks_test.go —— dashboard P1-2 只读面测试（board A2A-L19-W73-2；
// 设计 `docs/admin-dashboard.md` §5.7 / §6.1 #8-9 / §9 P1-2 行）。
//
// 装置：真 Server + httptest。两条注册面都覆盖——
//   - `Server.Handler()`（server.go 的 admin_key 分支本体，证明路由真的挂出来了）；
//   - `registerAdminModelsTasks` 直接装配在空 mux 上（证明**自门控**：未配 key
//     时一条都不注册 → 404 非 401）。
//
// 契约面（wire 结构在测试里独立声明，**不依赖实现侧结构**——形状改了这里就红）。
//
// 反证锚点（回退即红；负控实测见报告 §自审 B）：
//   - task.go `adminView` 的 `sidPrefix(rec.sid)` 换成 `rec.sid` →
//     TestAdminModelsTasksRedactionNegativeControl 红（响应体出现全量 sid）；
//   - `redactAdminText` 从 key_id / 模型 / 上游名投影去掉 → 同上红（明文 key 形态）；
//   - `registerAdminModelsTasks` 去掉自门控 / 漏挂 adminAuth →
//     TestAdminModelsTasksAuthGating 红（404/401 三态）；
//   - `snapshot` 的排序/截断去掉 → TestAdminModelsTasksBoundMaxRowsNewestFirst 红；
//   - `usage.available` 换成 true / 两个 null 换成 0 → TestAdminModelsTasksModels*
//     红（诚实缺数标记）。
package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// 装置
// ---------------------------------------------------------------------------

// adminMTPaths P1-2 的两条端点（鉴权/方法断言集）。
var adminMTPaths = []string{"/admin/models", "/admin/tasks"}

// adminMTConfig 测试配置：admin key + uiTestConfig 的本地假上游（不触网）。
func adminMTConfig(t *testing.T, key string) *Config {
	t.Helper()
	cfg := uiTestConfig()
	cfg.HeartbeatSeconds = 15
	cfg.AdminKey = key
	return cfg
}

// adminMTStart 启动被测服务：真 `Server.Handler()`（含 admin_key 自门控与
// adminAuth；server.go 的注册面本体）。
func adminMTStart(t *testing.T, cfg *Config) (*Server, *httptest.Server) {
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

// adminMTStartRegistered 只挂 `registerAdminModelsTasks` 的空 mux（自门控面）。
func adminMTStartRegistered(t *testing.T, cfg *Config) (*Server, *httptest.Server) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	mux := http.NewServeMux()
	s.registerAdminModelsTasks(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.Close()
		_ = s.CloseStore()
	})
	return s, srv
}

// ---------------------------------------------------------------------------
// 契约面 wire 结构（json 键 = 响应形状）
// ---------------------------------------------------------------------------

type adminMTWireUsage struct {
	RequestsToday *int64 `json:"requests_today"`
	UnitsToday    *int64 `json:"units_today"`
	Available     bool   `json:"available"`
}

type adminMTWireModel struct {
	ID             string           `json:"id"`
	UpstreamModel  *string          `json:"upstream_model"`
	MaxInputTokens *int             `json:"max_input_tokens"`
	MaxTokens      *int             `json:"max_tokens"`
	Usage          adminMTWireUsage `json:"usage"`
}

type adminMTWireModels struct {
	Configured bool               `json:"configured"`
	Models     []adminMTWireModel `json:"models"`
	Now        int64              `json:"now"`
}

type adminMTWireTask struct {
	ID         string  `json:"id"`
	Model      string  `json:"model"`
	Status     string  `json:"status"`
	CreatedAt  int64   `json:"created_at"`
	TerminalAt *int64  `json:"terminal_at"`
	Delivered  bool    `json:"delivered"`
	Attempts   *int    `json:"attempts"`
	Upstream   *string `json:"upstream"`
	SIDPrefix  string  `json:"sid_prefix"`
	KeyID      string  `json:"key_id"`
	Face       string  `json:"face"`
}

type adminMTWireTasks struct {
	Tasks []adminMTWireTask `json:"tasks"`
	Now   int64             `json:"now"`
}

// adminMTGet 一发 GET + 解码（非 200 即失败）+ 原始体。
func adminMTGet(t *testing.T, url, key string, dst any) string {
	t.Helper()
	resp := adminAPIReq(t, http.MethodGet, url, key, "")
	if resp.status != http.StatusOK {
		t.Fatalf("GET %s = %d body=%s", url, resp.status, resp.body)
	}
	if dst != nil {
		if err := json.Unmarshal([]byte(resp.body), dst); err != nil {
			t.Fatalf("decode %s: %v body=%s", url, err, resp.body)
		}
	}
	return resp.body
}

// ---------------------------------------------------------------------------
// 任务种子（直写 store；全部发生在 HTTP 请求之前——单线程播种）
// ---------------------------------------------------------------------------

// adminMTSeed 一条任务的测试种子。
type adminMTSeed struct {
	id         string
	model      string
	createdAt  int64     // 覆盖 store 的 unix 秒快照（排序面可控）
	terminalAt time.Time // 零值 = 运行中（queued / started=true 时 in_progress）
	status     string    // terminalAt 非零时生效
	started    bool      // 运行中任务标记 in_progress（复刻 runTask 的 te.start()）
	delivered  bool      // 终态交付位（markDelivered）
	sid        string    // 记账器 sid（**原样写入**——投影必须自己截断）
	keyID      string    // 记账器 key_id
	upstreams  []string  // 逐次尝试的上游名（空 = 不挂记账器）
}

// adminMTSeedTask 落一条任务记录：create → 覆盖 createdAt → 挂记账器 →
// （终态时）finish + markDelivered + close(done)。
//
// close(done) 复刻 runTask 的收尾顺序（serve 返回后才 close）——adminView 的
// attempts 读取门（见 task.go 头注）；不 close = 模拟在飞任务。
func adminMTSeedTask(t *testing.T, s *Server, seed adminMTSeed) *taskEntry {
	t.Helper()
	te, err := s.tasks.create(seed.id, seed.model)
	if err != nil {
		t.Fatalf("create %s: %v", seed.id, err)
	}
	te.createdAt = seed.createdAt
	if seed.started {
		te.start()
	}
	if seed.sid != "" || seed.keyID != "" || len(seed.upstreams) > 0 {
		rec := newRequestRecorder("req-"+seed.id, registry.FormatOpenAIResponses, seed.model, true, seed.id)
		rec.sid = seed.sid
		rec.keyID = seed.keyID
		for i, up := range seed.upstreams {
			ar := rec.begin(i+1, Upstream{Name: up, Protocol: ProtocolAnthropic})
			rec.finish(ar)
		}
		te.setRecorder(rec)
	}
	if !seed.terminalAt.IsZero() {
		te.finish(seed.status, json.RawMessage(`{"id":"`+seed.id+`"}`), seed.terminalAt)
		if seed.delivered {
			te.markDelivered("retrieve")
		}
		close(te.done)
	}
	return te
}

// ---------------------------------------------------------------------------
// 1. 路由注册（真 Handler）+ 自门控
// ---------------------------------------------------------------------------

func TestAdminModelsTasksHandlerRegistration(t *testing.T) {
	const key = "w73-2-admin-key"
	cfg := adminMTConfig(t, key)
	cfg.Models = []ModelEntry{{ID: "m-1"}}
	_, srv := adminMTStart(t, cfg)
	for _, path := range adminMTPaths {
		if got := adminAPIReq(t, http.MethodGet, srv.URL+path, key, "").status; got != http.StatusOK {
			t.Fatalf("真 Handler GET %s = %d（want 200：server.go 的 admin_key 分支应挂出路由）", path, got)
		}
	}

	// 未配 admin_key：`Handler()` 的 admin_key 分支不进入 → 404。
	_, srvNoKey := adminMTStart(t, adminMTConfig(t, ""))
	for _, path := range adminMTPaths {
		if got := adminAPIReq(t, http.MethodGet, srvNoKey.URL+path, "any-key", "").status; got != http.StatusNotFound {
			t.Fatalf("未配 key GET %s = %d（want 404）", path, got)
		}
	}
}

// ---------------------------------------------------------------------------
// 2. 鉴权三态 + 方法限定（直接装配注册面——证自门控本体）
// ---------------------------------------------------------------------------

func TestAdminModelsTasksAuthGating(t *testing.T) {
	const key = "w73-2-admin-key"

	// ① 未配 key：`registerAdminModelsTasks` 一条都不注册 → 404（非 401）。
	_, srvNone := adminMTStartRegistered(t, adminMTConfig(t, ""))
	for _, path := range adminMTPaths {
		resp := adminAPIReq(t, http.MethodGet, srvNone.URL+path, "any-key", "")
		if resp.status != http.StatusNotFound {
			t.Fatalf("未配 key GET %s = %d（want 404：路由不注册）body=%s", path, resp.status, resp.body)
		}
	}

	cfg := adminMTConfig(t, key)
	cfg.Models = []ModelEntry{{ID: "m-1"}}
	_, srv := adminMTStartRegistered(t, cfg)

	// ② 错 key：401 + 信封 + no-store（封禁面同形——不暴露封禁状态）。
	for _, path := range adminMTPaths {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+path, "wrong-key", "")
		if resp.status != http.StatusUnauthorized {
			t.Fatalf("错 key GET %s = %d（want 401）body=%s", path, resp.status, resp.body)
		}
		if !strings.Contains(resp.body, `"authentication_error"`) {
			t.Fatalf("401 信封：%s", resp.body)
		}
		if resp.header.Get("Cache-Control") != "no-store" {
			t.Fatalf("Cache-Control = %q（want no-store）", resp.header.Get("Cache-Control"))
		}
	}

	// ③ 对 key：200。
	for _, path := range adminMTPaths {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+path, key, "")
		if resp.status != http.StatusOK {
			t.Fatalf("对 key GET %s = %d（want 200）body=%s", path, resp.status, resp.body)
		}
	}

	// ④ 方法限定：只读面（POST 不提供任何动作）→ 405 + Allow: GET。
	for _, path := range adminMTPaths {
		resp := adminAPIReq(t, http.MethodPost, srv.URL+path, key, "{}")
		if resp.status != http.StatusMethodNotAllowed {
			t.Fatalf("POST %s = %d（want 405）body=%s", path, resp.status, resp.body)
		}
		if !strings.Contains(resp.header.Get("Allow"), http.MethodGet) {
			t.Fatalf("POST %s Allow = %q（want GET）", path, resp.header.Get("Allow"))
		}
	}
}

// ---------------------------------------------------------------------------
// 3. /admin/models：已配置（逐条 + 诚实缺数）
// ---------------------------------------------------------------------------

func TestAdminModelsTasksModelsConfigured(t *testing.T) {
	const key = "w73-2-admin-key"
	cfg := adminMTConfig(t, key)
	cfg.Models = []ModelEntry{
		{ID: "alias-a", UpstreamModel: "up-a", MaxInputTokens: 200000, MaxTokens: 8192},
		{ID: "alias-b"}, // 未声明映射/上限：null 而不是 0/空串
	}
	_, srv := adminMTStart(t, cfg)

	var got adminMTWireModels
	body := adminMTGet(t, srv.URL+"/admin/models", key, &got)

	if !got.Configured {
		t.Fatalf("configured = false（want true）body=%s", body)
	}
	if len(got.Models) != 2 {
		t.Fatalf("models 条数 = %d（want 2）body=%s", len(got.Models), body)
	}
	a := got.Models[0]
	if a.ID != "alias-a" || a.UpstreamModel == nil || *a.UpstreamModel != "up-a" ||
		a.MaxInputTokens == nil || *a.MaxInputTokens != 200000 ||
		a.MaxTokens == nil || *a.MaxTokens != 8192 {
		t.Fatalf("models[0] = %+v（want alias-a/up-a/200000/8192）body=%s", a, body)
	}
	b := got.Models[1]
	if b.ID != "alias-b" || b.UpstreamModel != nil || b.MaxInputTokens != nil || b.MaxTokens != nil {
		t.Fatalf("models[1] = %+v（want alias-b + 三 null：未声明 ≠ 0/空串）body=%s", b, body)
	}
	// 每模型用量：恒缺数（available:false + 两 null）——绝不发明 0。
	for i, m := range got.Models {
		if m.Usage.Available || m.Usage.RequestsToday != nil || m.Usage.UnitsToday != nil {
			t.Fatalf("models[%d].usage = %+v（want available:false + 两 null）body=%s", i, m.Usage, body)
		}
	}
	for _, want := range []string{`"usage":{"requests_today":null,"units_today":null,"available":false}`} {
		if !strings.Contains(body, want) {
			t.Fatalf("响应体缺 %s：%s", want, body)
		}
	}
	if strings.Contains(body, `"requests_today":0`) || strings.Contains(body, `"units_today":0`) {
		t.Fatalf("出现假零（0 冒充「今天无人用」）：%s", body)
	}
	if got.Now <= 0 {
		t.Fatalf("now = %d（want unix ms > 0）", got.Now)
	}
}

// ---------------------------------------------------------------------------
// 4. /admin/models：未配置（configured:false + 空表——绝不回退静态发现表）
// ---------------------------------------------------------------------------

func TestAdminModelsTasksModelsEmpty(t *testing.T) {
	const key = "w73-2-admin-key"
	_, srv := adminMTStart(t, adminMTConfig(t, key)) // uiTestConfig 无 models:

	var got adminMTWireModels
	body := adminMTGet(t, srv.URL+"/admin/models", key, &got)
	if got.Configured {
		t.Fatalf("configured = true（want false：未配 models:）body=%s", body)
	}
	if got.Models == nil || len(got.Models) != 0 {
		t.Fatalf("models = %v（want 空切片——`[]` 非 null）body=%s", got.Models, body)
	}
	if !strings.Contains(body, `"configured":false`) || !strings.Contains(body, `"models":[]`) {
		t.Fatalf("空配置形状：%s", body)
	}
	// 静态发现表的四模型名绝不出现（那是 /v1/models 缺省面的值）。
	for _, name := range []string{"claude-sonnet", "gpt-", "gemini-"} {
		if strings.Contains(body, name) {
			t.Fatalf("空配置回退了静态发现表（出现 %s）：%s", name, body)
		}
	}
}

// ---------------------------------------------------------------------------
// 5. /admin/tasks：空表（`[]` 非 null）
// ---------------------------------------------------------------------------

func TestAdminModelsTasksTasksEmpty(t *testing.T) {
	const key = "w73-2-admin-key"
	_, srv := adminMTStart(t, adminMTConfig(t, key))

	var got adminMTWireTasks
	body := adminMTGet(t, srv.URL+"/admin/tasks", key, &got)
	if got.Tasks == nil || len(got.Tasks) != 0 {
		t.Fatalf("tasks = %v（want 空切片）body=%s", got.Tasks, body)
	}
	if !strings.Contains(body, `"tasks":[]`) {
		t.Fatalf("空表形状应为 `[]`：%s", body)
	}
	if got.Now <= 0 {
		t.Fatalf("now = %d（want unix ms > 0）", got.Now)
	}
}

// ---------------------------------------------------------------------------
// 6. /admin/tasks：填充 + 排序 + 字段 + 状态过滤（含在飞任务）
// ---------------------------------------------------------------------------

func TestAdminModelsTasksTasksPopulatedAndFilter(t *testing.T) {
	const key = "w73-2-admin-key"
	s, srv := adminMTStart(t, adminMTConfig(t, key))
	const base = int64(1700000000)

	// 终态时刻必须落在保留期（taskTTL=10min）内——过期任务对快照不可见。
	now := time.Now()
	termA := now.Add(-time.Minute).Truncate(time.Second)
	termC := now.Add(-2 * time.Minute).Truncate(time.Second)
	// A：终态 completed，2 次尝试（末次 a2），带会话归因（前 8 字符 = `a1b2c3d4`）。
	adminMTSeedTask(t, s, adminMTSeed{
		id: "resp_synth_0001", model: "alias-a", createdAt: base + 30,
		terminalAt: termA, status: taskCompleted, delivered: true,
		sid: "a1b2c3d4e5f6a7b8", keyID: "3106c924", upstreams: []string{"a1", "a2"},
	})
	// B：在飞（无终态）——attempts/upstream 必须回 null（记账器单写者，见 task.go）。
	adminMTSeedTask(t, s, adminMTSeed{
		id: "resp_synth_0002", model: "alias-b", createdAt: base + 20,
		started: true, upstreams: []string{"b1"},
	})
	// C：终态 failed，1 次尝试，未交付。
	adminMTSeedTask(t, s, adminMTSeed{
		id: "resp_synth_0003", model: "alias-c", createdAt: base + 10,
		terminalAt: termC, status: taskFailed,
		sid: "ffffffff00000000", keyID: "deadbeef", upstreams: []string{"c1"},
	})

	var got adminMTWireTasks
	body := adminMTGet(t, srv.URL+"/admin/tasks", key, &got)
	if len(got.Tasks) != 3 {
		t.Fatalf("tasks 条数 = %d（want 3）body=%s", len(got.Tasks), body)
	}

	// 排序：created_at 降序（新 → 旧）。
	wantOrder := []string{"resp_synth_0001", "resp_synth_0002", "resp_synth_0003"}
	for i, want := range wantOrder {
		if got.Tasks[i].ID != want {
			t.Fatalf("tasks[%d].id = %s（want %s：created_at 降序）body=%s", i, got.Tasks[i].ID, want, body)
		}
	}

	a := got.Tasks[0]
	if a.Model != "alias-a" || a.Status != taskCompleted || a.CreatedAt != base+30 {
		t.Fatalf("A 基本字段：%+v", a)
	}
	if a.TerminalAt == nil || *a.TerminalAt != termA.Unix() {
		t.Fatalf("A terminal_at = %v（want %d）", a.TerminalAt, termA.Unix())
	}
	if !a.Delivered {
		t.Fatalf("A delivered = false（want true：markDelivered 已置位）")
	}
	if a.Attempts == nil || *a.Attempts != 2 {
		t.Fatalf("A attempts = %v（want 2）", a.Attempts)
	}
	if a.Upstream == nil || *a.Upstream != "a2" {
		t.Fatalf("A upstream = %v（want a2：末次尝试）", a.Upstream)
	}
	if a.SIDPrefix != "a1b2c3d4" || a.KeyID != "3106c924" {
		t.Fatalf("A sid_prefix/key_id = %q/%q（want 8 字符前缀 / sha8）", a.SIDPrefix, a.KeyID)
	}
	if a.Face != adminTaskFaceUnknown {
		t.Fatalf("A face = %q（want %q：记录不存该位，绝不推断）", a.Face, adminTaskFaceUnknown)
	}

	b := got.Tasks[1]
	if b.Status != taskInProgress || b.TerminalAt != nil || b.Delivered {
		t.Fatalf("B 基本字段：%+v", b)
	}
	if b.Attempts != nil || b.Upstream != nil {
		t.Fatalf("B attempts/upstream = %v/%v（want 两 null：在飞任务记账器不可读）", b.Attempts, b.Upstream)
	}
	if b.SIDPrefix != "" || b.KeyID != "" {
		t.Fatalf("B sid_prefix/key_id = %q/%q（want 空：无会话归因，wire 省略）", b.SIDPrefix, b.KeyID)
	}

	c := got.Tasks[2]
	if c.Status != taskFailed || c.Delivered {
		t.Fatalf("C 基本字段：%+v", c)
	}
	if c.Attempts == nil || *c.Attempts != 1 || c.Upstream == nil || *c.Upstream != "c1" {
		t.Fatalf("C attempts/upstream = %v/%v（want 1/c1）", c.Attempts, c.Upstream)
	}
	if c.TerminalAt == nil || *c.TerminalAt != termC.Unix() {
		t.Fatalf("C terminal_at = %v（want %d）", c.TerminalAt, termC.Unix())
	}

	// 状态过滤：精确匹配；无命中 = `[]`。
	cases := []struct {
		status string
		want   []string
	}{
		{taskCompleted, []string{"resp_synth_0001"}},
		{taskFailed, []string{"resp_synth_0003"}},
		{taskQueued, []string{}},
		{taskCancelled, []string{}},
	}
	for _, tc := range cases {
		var f adminMTWireTasks
		fbody := adminMTGet(t, srv.URL+"/admin/tasks?status="+tc.status, key, &f)
		if len(f.Tasks) != len(tc.want) {
			t.Fatalf("status=%s 命中 %d 条（want %d）body=%s", tc.status, len(f.Tasks), len(tc.want), fbody)
		}
		for i, want := range tc.want {
			if f.Tasks[i].ID != want {
				t.Fatalf("status=%s tasks[%d] = %s（want %s）", tc.status, i, f.Tasks[i].ID, want)
			}
		}
		if len(tc.want) == 0 && !strings.Contains(fbody, `"tasks":[]`) {
			t.Fatalf("status=%s 空结果应为 `[]`：%s", tc.status, fbody)
		}
	}
}

// ---------------------------------------------------------------------------
// 7. /admin/tasks：坏 status → 400；六值词表全通
// ---------------------------------------------------------------------------

func TestAdminModelsTasksStatusInvalid400(t *testing.T) {
	const key = "w73-2-admin-key"
	_, srv := adminMTStart(t, adminMTConfig(t, key))

	for _, bad := range []string{"bogus", "Completed", "done", "queued,completed", "null"} {
		resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/tasks?status="+bad, key, "")
		if resp.status != http.StatusBadRequest {
			t.Fatalf("status=%q = %d（want 400）body=%s", bad, resp.status, resp.body)
		}
		if !strings.Contains(resp.body, `"invalid_request_error"`) {
			t.Fatalf("status=%q 400 信封：%s", bad, resp.body)
		}
	}
	// 合法词表 + 缺省：全 200。
	for _, ok := range []string{"", taskQueued, taskInProgress, taskCompleted, taskFailed, taskIncomplete, taskCancelled} {
		url := srv.URL + "/admin/tasks"
		if ok != "" {
			url += "?status=" + ok
		}
		if resp := adminAPIReq(t, http.MethodGet, url, key, ""); resp.status != http.StatusOK {
			t.Fatalf("status=%q = %d（want 200）body=%s", ok, resp.status, resp.body)
		}
	}
}

// ---------------------------------------------------------------------------
// 8. /admin/tasks：有界（≤500）+ 最新优先 + 过期不可见
// ---------------------------------------------------------------------------

func TestAdminModelsTasksBoundMaxRowsNewestFirst(t *testing.T) {
	const key = "w73-2-admin-key"
	s, srv := adminMTStart(t, adminMTConfig(t, key))
	s.tasks.maxTasks = 600 // 放开 store 上限（默认 256）以造 >500 条
	const base = int64(1700000000)
	const n = 505
	for i := range n {
		adminMTSeedTask(t, s, adminMTSeed{
			id: fmt.Sprintf("resp_synth_%03d", i), model: "alias-a",
			createdAt: base + int64(i), // 秒级递增：id 越大越新
		})
	}
	// 一条过期终态任务：终态落在保留期（10min）之前 → 快照里不可见。
	adminMTSeedTask(t, s, adminMTSeed{
		id: "resp_synth_expired", model: "alias-a", createdAt: base + 9000,
		terminalAt: time.Now().Add(-11 * time.Minute), status: taskCompleted,
	})

	var got adminMTWireTasks
	body := adminMTGet(t, srv.URL+"/admin/tasks", key, &got)
	if len(got.Tasks) != adminTasksMaxRows {
		t.Fatalf("tasks 条数 = %d（want %d：有界截断）body 长度=%d", len(got.Tasks), adminTasksMaxRows, len(body))
	}
	if got.Tasks[0].ID != fmt.Sprintf("resp_synth_%03d", n-1) {
		t.Fatalf("tasks[0].id = %s（want 最新 %03d）", got.Tasks[0].ID, n-1)
	}
	if last := got.Tasks[len(got.Tasks)-1].ID; last != "resp_synth_005" {
		t.Fatalf("tasks[499].id = %s（want resp_synth_005：最新 500 条，尾巴 5 条被截）", last)
	}
	for i := 1; i < len(got.Tasks); i++ {
		if got.Tasks[i-1].CreatedAt < got.Tasks[i].CreatedAt {
			t.Fatalf("排序破：tasks[%d]=%d < tasks[%d]=%d", i-1, got.Tasks[i-1].CreatedAt, i, got.Tasks[i].CreatedAt)
		}
	}
	for _, id := range []string{"resp_synth_000", "resp_synth_004", "resp_synth_expired"} {
		if strings.Contains(body, id) {
			t.Fatalf("响应体出现应被截断/已过期的任务 %s", id)
		}
	}
}

// ---------------------------------------------------------------------------
// 9. 负控：全量 sid / 明文 key 形态绝不出现（去掉投影即红）
// ---------------------------------------------------------------------------

func TestAdminModelsTasksRedactionNegativeControl(t *testing.T) {
	const key = "w73-2-admin-key"
	s, srv := adminMTStart(t, adminMTConfig(t, key))

	// 种子把**全量 sid**（23 字符）与**明文 key 形态**直写进记账器（真实路径里
	// serve 已截断 sid、key_id 已是 sha8——本种子故意绕过，证明投影自带牙齿）。
	const fullSID = "SECRET-full-sid-0123456789"
	const rawKey = "sk-SECRET-key-abcdef"
	adminMTSeedTask(t, s, adminMTSeed{
		id: "resp_synth_sec1", model: "api_key=SECRET-model", createdAt: 1700000000,
		terminalAt: time.Now().Add(-time.Minute), status: taskCompleted, delivered: true,
		sid: fullSID, keyID: rawKey, upstreams: []string{"sk-SECRET-upstream"},
	})

	var got adminMTWireTasks
	body := adminMTGet(t, srv.URL+"/admin/tasks", key, &got)

	// 全量 sid 绝不出现（含尾段）；只回 8 字符前缀。
	if strings.Contains(body, fullSID) || strings.Contains(body, "0123456789") {
		t.Fatalf("全量 sid 泄漏：%s", body)
	}
	if !strings.Contains(body, `"sid_prefix":"SECRET-f"`) {
		t.Fatalf("sid_prefix 应为 8 字符前缀 SECRET-f：%s", body)
	}
	// 明文 key 形态（key_id / 上游名）出口投影掉。
	if strings.Contains(body, "SECRET-key") || strings.Contains(body, "sk-SECRET") {
		t.Fatalf("明文 key 形态泄漏：%s", body)
	}
	if !strings.Contains(body, "sk-[redacted]") {
		t.Fatalf("key_id 应回投影占位 sk-[redacted]（字段仍在）：%s", body)
	}
	// 模型名（客户端可控串）里的凭据形态同样投影。
	if strings.Contains(body, "SECRET-model") || !strings.Contains(body, "api_key=[redacted]") {
		t.Fatalf("模型名未投影：%s", body)
	}
	// 正控：行确实在响应里（脱敏不是「整段丢掉」）。
	if !strings.Contains(body, "resp_synth_sec1") || got.Tasks[0].Attempts == nil || *got.Tasks[0].Attempts != 1 {
		t.Fatalf("正控失败（行应存在且带 attempts）：%s", body)
	}
	if got.Tasks[0].Upstream == nil || *got.Tasks[0].Upstream != "sk-[redacted]" {
		t.Fatalf("上游名投影 = %v（want sk-[redacted]）", got.Tasks[0].Upstream)
	}
}
