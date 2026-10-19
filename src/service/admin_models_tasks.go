// admin_models_tasks.go —— dashboard P1-2 只读面：模型目录 + 任务快照（board
// A2A-L19-W73-2；设计 `docs/admin-dashboard.md` §5.7 / §6.1 #8-9 / §9 P1-2 行）。
//
// 两个端点（全部挂 adminAuth；未配 admin_key = 路由根本不注册 → 404 非 401）：
//
//	GET /admin/models         模型目录（`Config.Models` 配置表 + 声明上限）
//	GET /admin/tasks?status=  任务托管网关的只读快照（有界 500；脱敏行）
//
// 数据源（**零新计数器**；设计 §9 P1-2「只读」）：
//   - 模型目录 = `Config.Models`（W5-28 的别名映射表）**逐条**；配置空 = 「未配置」
//     （`configured:false` + `models:[]`）——**绝不**回退到 `/v1/models` 的静态发现
//     表（那是缺省发现面的值，不是"配置过的模型"；本面**不调用自己**的 HTTP 面）。
//   - 任务快照 = `taskStore.snapshot`（task.go；同样是进程内内存态，重启清零）。
//
// 诚实缺数（**宁缺毋假**；设计 §5.5/§6.1 #12 登记「per-model / per-upstream usage 属
// P1-3，需新计数器」）：
//   - 每模型用量**不存在**（`usage_counters` 的 scope 词表只有 global|session|key|ip；
//     `admin_api.go` §登记差异）——故 `usage:{requests_today:null, units_today:null,
//     available:false}`：`available:false` 明说"本面没有这个数"，两个 `null` 是
//     "未知"，**不是 0**（绝不用 0 冒充"今天没人用"）。
//   - 模型的 `max_input_tokens`/`max_tokens` = 配置未声明（0）→ `null`；`upstream_model`
//     未声明 = `null`（不是空串占位）。
//   - 任务行 `face` 恒 `"unknown"`：`taskEntry` **不存**任务面归属（managed/
//     passthrough 是创建期的路由裁定，见 task_passthrough.go），本面**绝不推断**
//     （登记差异，见报告）。
//   - 任务行 `attempts`/`upstream` 在任务运行中回 `null`（未收尾 = 未定）：记账器
//     `requestRecorder` 是单写者结构（attempts.go §requestRecorder），在飞记录的
//     `recs` 正被任务 goroutine 追加——跨 goroutine 读是数据竞争。收尾（任务
//     goroutine 关闭 `done`）后才投影，见 task.go `taskEntry.adminView`。
//
// 脱敏（零凭据字节纪律，与 admin_api.go / admin_dashboard.go 同源）：
//   - `sid_prefix` = **只回 8 字符前缀**（`sidPrefix`；全量 sid/cookie 绝不进响应）；
//   - `key_id` = 记账面的 sha8（已脱敏）——出口再过 `redactAdminText`，防御性覆盖
//     "明文 key 意外落进该字段"的形态；
//   - 全部文本字段过 `redactAdminText`（模型名/上游名是客户端可控串）。
//
// 只读纪律：零 DDL、零出网、零 store 写、不改既有端点形状；`/admin/tasks` **无取消
// 动作**（取消走上游既有任务取消路径 `/v1/responses/{id}/cancel`，见 §5.7「取消」）。
package service

import (
	"net/http"
	"strings"
	"time"
)

// adminTasksMaxRows 任务快照行数上限（有界响应；最新优先——旧行被截断）。
const adminTasksMaxRows = 500

// adminTaskFaceUnknown 任务行 `face` 的未知值（taskEntry 不存任务面归属；**绝不
// 推断**——设计 §5.7 的 managed|passthrough 词表只在创建期路由裁定里存在）。
const adminTaskFaceUnknown = "unknown"

// adminTaskStatusFilter 任务状态过滤词表（= task.go 的六个状态常量；设计 §5.7
// 的任务行 status 列）。"" = 不过滤（全部）。
var adminTaskStatusFilter = map[string]bool{
	taskQueued:     true,
	taskInProgress: true,
	taskCompleted:  true,
	taskFailed:     true,
	taskIncomplete: true,
	taskCancelled:  true,
}

// ---------------------------------------------------------------------------
// 路由注册
// ---------------------------------------------------------------------------

// registerAdminModelsTasks 注册 P1-2 的两条只读端点。**自门控**：未配 admin_key
// 时一条都不注册（与既有 /admin/* 的惰性注册语义逐字一致：404 非 401）。
//
// 调用点 = `Server.Handler()` 的 admin_key 分支（与 diagnostics / registerAdminAPI
// / registerAdminDashboard / metrics / meta 同一位置）。
func (s *Server) registerAdminModelsTasks(mux *http.ServeMux) {
	if s == nil || s.cfg.adminKey() == "" {
		return
	}
	mux.HandleFunc("GET /admin/models", s.adminAuth(s.handleAdminModels))
	mux.HandleFunc("GET /admin/tasks", s.adminAuth(s.handleAdminTasks))
}

// ---------------------------------------------------------------------------
// GET /admin/models —— 响应结构（白名单：字段即契约）
// ---------------------------------------------------------------------------

// adminModelUsageView 单模型的今日用量视图。**本面恒缺数**：per-model 计数器不
// 存在（§5.5/§6.1 #12 属 P1-3），故 `available:false` + 两 `null`——绝不回 0。
type adminModelUsageView struct {
	RequestsToday *int64 `json:"requests_today"` // null = 无计数器（不是 0）
	UnitsToday    *int64 `json:"units_today"`    // null = 无计数器（不是 0）
	Available     bool   `json:"available"`      // false = 本面没有每模型用量
}

// adminModelView 单模型的目录行：配置值逐条 + 上游映射 + 声明上限。
type adminModelView struct {
	ID string `json:"id"` // 发现面 id / 请求面别名（Config.Models[].id）
	// UpstreamModel 别名映射的目标上游模型名；null = 未声明（不缺省改写）。
	UpstreamModel *string `json:"upstream_model"`
	// MaxInputTokens / MaxTokens 配置声明上限；null = 未声明（0 = 不下发该字段，
	// 见 ModelEntry 注释）——null 不是「0 token 上限」。
	MaxInputTokens *int                `json:"max_input_tokens"`
	MaxTokens      *int                `json:"max_tokens"`
	Usage          adminModelUsageView `json:"usage"`
}

// adminModelsResponse `GET /admin/models` 响应体（**白名单投影**）。
type adminModelsResponse struct {
	// Configured 是否配了 `models:` 表（false → models 恒 `[]`——静态发现表是
	// 缺省发现面的值，不是"已配置的模型"）。
	Configured bool             `json:"configured"`
	Models     []adminModelView `json:"models"` // 空 = `[]`（非 null）
	Now        int64            `json:"now"`    // unix ms（快照时刻）
}

// handleAdminModels GET /admin/models。
func (s *Server) handleAdminModels(w http.ResponseWriter, r *http.Request) {
	writeAdminJSON(w, http.StatusOK, s.adminModelsSnapshot())
}

// adminModelsSnapshot 模型目录快照：**只读** `Config.Models`（启动期静态配置；
// 不做 I/O、不调 `/v1/models`）。空配置 = `configured:false` + 空表。
func (s *Server) adminModelsSnapshot() adminModelsResponse {
	resp := adminModelsResponse{
		Configured: len(s.cfg.Models) > 0,
		Models:     make([]adminModelView, 0, len(s.cfg.Models)),
		Now:        time.Now().UnixMilli(),
	}
	for _, m := range s.cfg.Models {
		v := adminModelView{
			ID:    redactAdminText(m.ID),
			Usage: adminModelUsageView{Available: false}, // 恒缺数（见类型注释）
		}
		if m.UpstreamModel != "" {
			up := redactAdminText(m.UpstreamModel)
			v.UpstreamModel = &up
		}
		if m.MaxInputTokens > 0 {
			n := m.MaxInputTokens
			v.MaxInputTokens = &n
		}
		if m.MaxTokens > 0 {
			n := m.MaxTokens
			v.MaxTokens = &n
		}
		resp.Models = append(resp.Models, v)
	}
	return resp
}

// ---------------------------------------------------------------------------
// GET /admin/tasks —— 响应结构 + 行视图
// ---------------------------------------------------------------------------

// adminTaskView 单条任务行（dashboard §5.7「后台任务表」的只读投影；脱敏规则见
// 文件头注）。字段顺序 = 契约顺序。
type adminTaskView struct {
	ID        string `json:"id"`
	Model     string `json:"model"`
	Status    string `json:"status"`     // queued|in_progress|completed|failed|incomplete|cancelled
	CreatedAt int64  `json:"created_at"` // unix 秒（= 任务快照 created_at 同单位）
	// TerminalAt 终态落定时刻（unix 秒）；null = 运行中（queued/in_progress）。
	TerminalAt *int64 `json:"terminal_at"`
	// Delivered 终态快照是否已交付下游（≥1 次 retrieve / 续流终态事件；运行中恒 false）。
	Delivered bool `json:"delivered"`
	// Attempts 上游尝试数；null = 任务未收尾（在飞记录不可读——见文件头注）。
	Attempts *int `json:"attempts"`
	// Upstream 末次尝试的上游名；null = 未收尾 / 无尝试记录。
	Upstream *string `json:"upstream"`
	// SIDPrefix 会话 id 的 8 字符前缀（**全量 sid 绝不出现**）；空 = 无会话归因。
	SIDPrefix string `json:"sid_prefix,omitempty"`
	// KeyID 准入 key 的 sha8（出口过 redactAdminText）；空 = 无 key 归因。
	KeyID string `json:"key_id,omitempty"`
	// Face 任务面归属；恒 `"unknown"`（taskEntry 不存该位——绝不推断）。
	Face string `json:"face"`
}

// adminTasksResponse `GET /admin/tasks` 响应体（**白名单投影**）。
type adminTasksResponse struct {
	Tasks []adminTaskView `json:"tasks"` // 空 = `[]`（非 null）
	Now   int64           `json:"now"`   // unix ms（快照时刻）
}

// handleAdminTasks GET /admin/tasks?status=：有界只读快照（最多
// adminTasksMaxRows 条，最新优先）。坏 status → 400；无取消动作（见文件头注）。
func (s *Server) handleAdminTasks(w http.ResponseWriter, r *http.Request) {
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status != "" && !adminTaskStatusFilter[status] {
		writeAdminError(w, http.StatusBadRequest, "invalid_request_error",
			"status must be queued|in_progress|completed|failed|incomplete|cancelled")
		return
	}
	writeAdminJSON(w, http.StatusOK, adminTasksResponse{
		Tasks: s.tasks.snapshot(status),
		Now:   time.Now().UnixMilli(),
	})
}
