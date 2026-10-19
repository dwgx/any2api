// 任务面实现（A2A-L19-W8-1 Slice 1 + A2A-L19-W17-2 Slice 2 + A2A-L19-W19-2
// Slice 3 + A2A-L19-W20-1 Slice 4 + A2A-L19-W27-1 Slice 5；提案
// `research/topics/service-task-surface` §实施拆分 + §父代裁决 2026-10-14）。
//
// 面（第一期仅 responses 入口；父代裁决 #5）：
//
//	POST /v1/responses 带 background:true → 能力表裁定（Slice 5）：候选链第一个
//	  非 off 上游 task_face=passthrough → 直连透传（见 task_passthrough.go）；
//	  =managed（缺省）→ 下方托管路径；全 off → 404 task_face_unavailable。
//	托管路径（缺省）：立即（<500ms）返回合成快照
//	  {id, object, created_at, status:"queued", model, output:[], usage:null}；
//	  后台 goroutine 复用现有 serve/relay 跑上游长流（提交点/心跳/错误帧语义照旧）；
//	  幂等键（Slice 4；父代裁决 #4 两者都收、头优先）：`Idempotency-Key` 头 >
//	  体 `idempotency_key` 字段——同键同体重提 → 同任务 id（1 任务、N 请求，
//	  绝不建第二个任务/上游调用）；同键不同体 → 409 idempotency_key_conflict；
//	  键生命周期随任务（过期墓碑化 → 键回收 → 重提 = 新任务）；
//	任务终态 usage 落账（Slice 4）：每任务一条 per-task 记账（{task,status,in,out}
//	  + TaskUsage* 计数）；任务 goroutine 的 per-request attempt 行带 task 归因
//	  （见 attempts.go §per-task）；failed/cancelled 无终态 usage → 下界在
//	  per-attempt 面（AttemptUsage*）；
//	GET /v1/responses/{id} → 任务快照（queued/in_progress/completed/failed/
//	  incomplete/cancelled）；终态 = 上游终态 response 对象（output/usage 逐字段
//	  一致；id/status 改写为任务面值）；
//	POST /v1/responses/{id}/cancel → 幂等取消（Slice 2）：运行中 → 真中止在飞
//	  上游调用（任务 ctx 取消）→ 等 goroutine 收尾（≤taskCancelWait）→ 返回
//	  cancelled 终态快照；已终态 → 原样返回现有终态快照（重复 cancel 同终态）；
//	GET /v1/responses/{id}?stream=true&starting_after=N → 续流（Slice 3，见
//	  task_resume.go）：从 N+1 重放缓冲 + 接续实时；过期游标（早于缓冲下界）→
//	  快照降级（普通 GET 响应）；无 stream=true → 普通快照（字节零改动）；
//	未知 id → 404 error.code=task_not_found；过期 id → 404 error.code=task_expired
//	  （父代裁决 #2；保留期 = 10min 常量，父代裁决 #1；retrieve/cancel 同双 code）。
//
// 失败语义表（Slice 2；提案 §形态 B 失败表原文 + 落点；error_message = 任务快照
// 的 `error` 对象 {code,message}）：
//
//	事件                        任务终态     快照 error / 备注
//	上游提交前失败（换上游穷尽） failed       error ← serve 错误信封（code/message）
//	运行中上游失败/截断          failed       response.failed 对象原样（error 保留）；
//	                                         截断 = relay 合成 failed 帧（带错误文本）
//	任务挂住（idle 守卫触发）    incomplete   error.code=upstream_idle + idle 文本
//	                                          （relay 的 response.failed 合成帧按码
//	                                          映射；只覆盖「已提交后挂住」——预提交
//	                                          挂住走换上游/错误信封路径，与首行同面）
//	客户端取消（幂等）           cancelled    无 error；重复 cancel 返回同快照
//	客户端断开                   任务继续     断开 ≠ 取消（任务 ctx 与提交请求解耦）
//	网关重启                     任务丢失     未知 id → 404（内存 store，父代裁决 #6）
//
// store = 单进程内存（父代裁决 #6：重启即丢，文档写明）；有界三件套：终态 TTL
// （taskTTL）+ 任务数上限（taskMaxTasks）+ 流扫描器单块上限（taskMaxPending）。
//
// 红线对齐（提案 §红线 1-7）：
//  1. 任务面 = 新增路由；同步路径（background 缺省/false 与其余入口）字节零改动；
//     goroutine 内部复用现有 relay——提交点/心跳/错误帧语义不平行再造。
//  2. 心跳与看门狗预算不破：任务面走同一 hb 缺省（15s）与同一 relay 循环。
//  3. failover 分类不破：提交前失败走 serve 的 C1 链原样；提交后失败 = 任务终态
//     failed（绝不重放）；取消 = ctx 取消走 relay 的 disconnected 归因（不计
//     上游健康、不触发 failover/重试）。
//  4. 记账不破：per-attempt 记账在 serve 内部原样发生；任务面新增的只是终态快照。
//  5. stateful 禁注入不破：background 在进 relay 前**手术剥除**，绝不注入上游；
//     其余 stateful 字段（previous_response_id/conversation/prompt/
//     context_management/moderation）照旧拒绝（转换面默认拒绝未动——任务面在服务
//     层消化 background，转换器永不看到它）。
//  6. 下游回退兼容不破：非流式同参重发路径与任务面互不影响（同一入口分流）。
//  7. idle 守卫/录制缺省关语义不破：任务面复用全局 `upstream_idle_timeout_seconds`
//     （缺省 0=关不动——未引入任务粒度覆盖；红线「按任务粒度传参」的现状实现 =
//     任务复用同一 serve 路径的同一配置），不接 capture（见边界）。
//
// 边界（有意；见报告 §边界）：
//   - 任务 goroutine 的 ctx 与下游请求解耦（下游拿到快照即完成；断开不杀任务——
//     托管语义核心），但任务自持取消函数（cancel 面真中止在飞上游调用）。debug
//     capture 不覆盖任务 goroutine：fresh ctx 无 capture 句柄，且入口 handler
//     返回时 captureDone 已收尾（录制生命周期与任务不同寿）。
//   - stream:true + background:true 同发仍回 JSON 快照（创建面不做内联流——
//     续流走 GET ?stream=true&starting_after=N，Slice 3）。
//   - 运行中任务不因 TTL 过期（终态落定才开始计时）。cancel 的取消快照 = 合成
//     最小快照（不保留部分产出）。幂等键边界（Slice 4，见报告）：只对任务创建
//     面生效（同步路径零改动）；同键判定 = 剥除键字段后的请求体字节级一致
//     （键字段本身/载体头或体不参与比较）；键长度上限 taskIdemKeyMax（超限 400）；
//     任务墓碑化（过期）→ 键同步回收。
//   - 事件缓冲有界（Slice 3）：单任务 1024 事件 / 1 MiB 双上限，超限驱逐最老
//     （下界推进）→ 过期游标走快照降级；编号（sequence_number）单调不复用。
package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dwgx/any2api/translate/hygiene"
	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/pairs"
	"github.com/dwgx/any2api/translate/registry"
)

const (
	// taskTTL 终态任务快照保留期（父代裁决 #1：10min 常量，Slice 1 不可配）。
	taskTTL = 10 * time.Minute
	// taskMaxTasks 同时存活任务数上限（有界内存；满载先清过期，仍满 → 503）。
	taskMaxTasks = 256
	// taskMaxExpired 过期 id 墓碑上限（task_expired 与 task_not_found 的区分面；
	// FIFO 淘汰——超限后最老墓碑回落 task_not_found，内存有界）。
	taskMaxExpired = 4096
	// taskMaxPending 任务流扫描器单块待定缓冲上限（SSE 块；超限丢块）。relay 侧
	// sseFrameReader 已按 1 MiB 限帧，此处 2 MiB 是防御性兜底。
	taskMaxPending = 2 << 20
	// taskCancelWait 取消端点等待任务 goroutine 收尾的上限（Slice 2）：ctx 取消
	// 后 relay 立即中止在飞上游请求（本地上游毫秒级收尾）；超时/调用方断开 →
	// 返回当前快照（任务仍会收敛到终态；重复 cancel 幂等）。
	taskCancelWait = 5 * time.Second
)

// 任务状态枚举：queued/in_progress 运行中；completed/failed/incomplete/cancelled
// 终态（cancelled = Slice 2 的显式取消面）。
const (
	taskQueued     = "queued"
	taskInProgress = "in_progress"
	taskCompleted  = "completed"
	taskFailed     = "failed"
	taskIncomplete = "incomplete"
	taskCancelled  = "cancelled"
)

// 任务 store 查找未命中原因（GET /v1/responses/{id} 的 404 双 code 面）。
const (
	taskMissNotFound = "not_found"
	taskMissExpired  = "expired"
)

// errTaskLimit 任务数满载（create 拒绝面）。
var errTaskLimit = errors.New("task store is full")

// taskStore 单进程内存任务 store。now/ttl 可注入（测试用假时钟/短 TTL——brief
// 「测试可注入时钟/短 TTL」；字段在 mu 保护下读写，runTask 用 stamp() 取时）。
type taskStore struct {
	mu    sync.Mutex
	tasks map[string]*taskEntry
	// expired 过期 id 墓碑（有界 FIFO）：区分 task_expired（曾存在、已过保留期）
	// 与 task_not_found（从未存在/重启丢失）。
	expired      map[string]struct{}
	expiredOrder []string
	// idem 幂等键表（Slice 4）：键 → 任务。生命周期随任务——任务墓碑化时同步
	// 回收（tombstoneLocked）；有界 = 存活任务数（≤ maxTasks，每任务至多一键）。
	idem map[string]*idemEntry

	now      func() time.Time
	ttl      time.Duration
	maxTasks int
}

// idemEntry 幂等键记录（Slice 4）：任务 id + 内容指纹（剥除键字段后的请求体
// 字节 sha256——同键不同体 = 冲突，绝不重放不同请求）。
type idemEntry struct {
	taskID string
	hash   [32]byte
}

func newTaskStore() *taskStore {
	return &taskStore{
		tasks:    map[string]*taskEntry{},
		expired:  map[string]struct{}{},
		idem:     map[string]*idemEntry{},
		now:      time.Now,
		ttl:      taskTTL,
		maxTasks: taskMaxTasks,
	}
}

// create 建一条任务记录（status=queued）。满载时先清过期墓碑化，仍满 → errTaskLimit。
func (st *taskStore) create(id, model string) (*taskEntry, error) {
	te, _, _, err := st.createIdem(id, model, "", nil)
	return te, err
}

// createIdem 建任务或幂等命中（Slice 4）。key == "" → 无键路径（等价原 create）。
// body = 内容指纹输入（调用方已剥除键字段，见 taskIdemCompareBody）。
//
// 返回：te = 新建或命中的任务；replay=true = 同键同体命中（返回既有任务——
// 绝不新建、绝不启动第二个上游调用）；conflict=true = 同键不同体（保守裁定：
// 409——绝不重放不同请求、绝不建第二任务）。
//
// 键生命周期 = 随任务：任务在 → 键在（终态保留期内重提恒同 id）；任务墓碑化
// （过期/驱逐）→ 键同步回收 → 重提 = 新任务（重放一个已 404 的 id 无意义）。
func (st *taskStore) createIdem(id, model, key string, body []byte) (te *taskEntry, replay, conflict bool, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	now := st.now()
	st.purgeLocked(now)
	if key != "" {
		if e, ok := st.idem[key]; ok {
			if hit := st.tasks[e.taskID]; hit != nil && !st.expiredLocked(hit, now) {
				if sha256.Sum256(body) != e.hash {
					return nil, false, true, nil
				}
				return hit, true, false, nil
			}
			// 防御：键记录指向已不在/已过期的任务（正常路径 tombstoneLocked 已
			// 同步回收——此处只兜底清理）。
			delete(st.idem, key)
		}
	}
	if len(st.tasks) >= st.maxTasks {
		return nil, false, false, errTaskLimit
	}
	te = &taskEntry{
		id:        id,
		model:     model,
		createdAt: now.Unix(),
		status:    taskQueued,
		done:      make(chan struct{}),
		log:       newTaskEventLog(),
		idemKey:   key,
	}
	st.tasks[id] = te
	if key != "" {
		st.idem[key] = &idemEntry{taskID: id, hash: sha256.Sum256(body)}
	}
	return te, false, false, nil
}

// get 查任务：(entry, "") 命中；(nil, taskMissExpired) 过期；(nil, taskMissNotFound)
// 未知。终态且过保留期 → 惰性墓碑化（幂等：重复 GET 仍报 task_expired）。
func (st *taskStore) get(id string) (*taskEntry, string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	te, ok := st.tasks[id]
	if !ok {
		if _, ok := st.expired[id]; ok {
			return nil, taskMissExpired
		}
		return nil, taskMissNotFound
	}
	if st.expiredLocked(te, st.now()) {
		st.tombstoneLocked(id)
		return nil, taskMissExpired
	}
	return te, ""
}

// snapshot 任务只读快照（dashboard P1-2 `GET /admin/tasks` 的数据源；
// A2A-L19-W73-2）。**纯读**：不改任务状态、不写日志、不出网、不写 store。
//
//   - 过期（终态且过保留期）任务**跳过但不墓碑化**——墓碑化属写面（get/create
//     路径），本方法绝不推进 store 状态；
//   - 过滤 = 精确状态匹配（"" = 全部；词表见 admin_models_tasks.go）；
//   - 排序 = created_at 降序（新 → 旧），同秒按 id 升序（created_at 是 unix 秒，
//     真实任务可能同秒；id 决定性去重 → 输出确定）；
//   - 截断 = 最新 adminTasksMaxRows 条（有界响应）。
//
// 行内容是脱敏投影（`taskEntry.adminView`）——调用方直接进 JSON，无需再洗。
func (st *taskStore) snapshot(status string) []adminTaskView {
	st.mu.Lock()
	defer st.mu.Unlock()
	now := st.now()
	out := make([]adminTaskView, 0, len(st.tasks))
	for _, te := range st.tasks {
		if st.expiredLocked(te, now) {
			continue
		}
		v := te.adminView()
		if status != "" && v.Status != status {
			continue
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt > out[j].CreatedAt
		}
		return out[i].ID < out[j].ID
	})
	if len(out) > adminTasksMaxRows {
		out = out[:adminTasksMaxRows]
	}
	return out
}

// stamp 取当前时钟（任务 goroutine 落终态时刻用；mu 保护下读 now 注入面）。
func (st *taskStore) stamp() time.Time {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.now()
}

// setTTL 测试注入：调整保留期（并发安全）。
func (st *taskStore) setTTL(d time.Duration) {
	st.mu.Lock()
	st.ttl = d
	st.mu.Unlock()
}

// taskLogCaps 测试注入：调整某任务的事件缓冲容量（Slice 3 溢出驱逐面；
// 并发安全；任务不存在 → false）。
func (st *taskStore) taskLogCaps(id string, maxEvents, maxBytes int) bool {
	st.mu.Lock()
	te := st.tasks[id]
	st.mu.Unlock()
	if te == nil {
		return false
	}
	te.log.setCaps(maxEvents, maxBytes)
	return true
}

// expiredLocked 终态任务是否已过保留期（运行中任务永不过期——terminalAt 零值）。
func (st *taskStore) expiredLocked(te *taskEntry, now time.Time) bool {
	te.mu.Lock()
	at := te.terminalAt
	te.mu.Unlock()
	return !at.IsZero() && now.Sub(at) >= st.ttl
}

// purgeLocked 清所有过期终态任务（create 满载前调用；range 中 delete 安全）。
func (st *taskStore) purgeLocked(now time.Time) {
	for id, te := range st.tasks {
		if st.expiredLocked(te, now) {
			st.tombstoneLocked(id)
		}
	}
}

// tombstoneLocked 任务移入过期墓碑（FIFO 有界）。幂等键随任务同步回收（Slice 4：
// 键表有界 = 存活任务数；过期后重提 = 新任务，不重放已 404 的 id）。
func (st *taskStore) tombstoneLocked(id string) {
	if _, ok := st.expired[id]; ok {
		return
	}
	if te, ok := st.tasks[id]; ok && te.idemKey != "" {
		delete(st.idem, te.idemKey)
	}
	delete(st.tasks, id)
	st.expired[id] = struct{}{}
	st.expiredOrder = append(st.expiredOrder, id)
	for len(st.expiredOrder) > taskMaxExpired {
		old := st.expiredOrder[0]
		st.expiredOrder = st.expiredOrder[1:]
		delete(st.expired, old)
	}
}

// taskEntry 单条任务记录。状态迁移：queued → in_progress → {completed,failed,
// incomplete,cancelled}；终态只落一次（first-terminal-wins——relay 的「终止帧后
// idle 错误帧」边界不翻转已宣判终态；取消语义同理：已观测上游终态 → 取消是
// no-op，未观测 + 已请求取消 → cancelled）。
type taskEntry struct {
	id        string
	model     string
	createdAt int64         // 快照 created_at（unix 秒）
	done      chan struct{} // 终态落定后关闭（测试等待/取消端点收尾等待用）

	// log 任务事件缓冲（Slice 3；create 时构造，任务 goroutine 单一写者，
	// 续流端点订阅者多读者——见 task_resume.go）。
	log *taskEventLog

	mu         sync.Mutex
	status     string
	terminalAt time.Time       // 终态落定时刻（零 = 运行中；保留期起点）
	snapshot   json.RawMessage // 终态快照（上游 response 对象 + id/status 改写）
	// cancel 任务 ctx 取消函数（Slice 2；goroutine 启动前设置——cancel 端点在
	// 任何时刻调用都有效，含 goroutine 尚未调度时）。
	cancel context.CancelFunc
	// cancelReq 显式取消已请求（终态落定前的取消标记；runTask 收尾据此把
	// 「未观测到上游终态」的任务落为 cancelled）。
	cancelReq bool
	// idemKey 创建时的幂等键（Slice 4；""= 无键；mu 保护下随创建写入，墓碑化
	// 时用于回收 idem 表项）。
	idemKey string
	// rec 任务 goroutine 的 per-request 记账器（W36-1 S6：task_terminal ring 事件
	// 的 attempts/末上游来源；serve 建记账器后回填——单写者 = 任务 goroutine 内
	// 的 serve，收尾读在同一 goroutine，无并发面）。
	rec *requestRecorder
	// delivered 终态交付位（W36-1 S2；mu 保护）：终态快照被 ≥1 次 retrieve 或
	// 续流订阅者收到终态事件 → true（首次交付写一行日志；幂等）。纯对账面。
	delivered bool
	// deliveredVia 首次交付方式（`retrieve` / `resume`；delivered=false 时 ""）。
	deliveredVia string
}

// setRecorder 回填 per-request 记账器（W36-1 S6；serve 建记账器后调用——任务
// goroutine 的同一调用栈，收尾（runTask）读它取 attempts/末上游）。
func (te *taskEntry) setRecorder(rec *requestRecorder) {
	te.mu.Lock()
	te.rec = rec
	te.mu.Unlock()
}

// attemptStats 本任务的上游尝试数 + 末次尝试上游名（W36-1 S6：task_terminal
// ring 事件载荷；来源 = per-request 记账器——与 attempt 日志行同源可 join）。
// 必须在任务 goroutine 的 serve 返回后调用（记账器 append 已完成）。
func (te *taskEntry) attemptStats() (int, string) {
	te.mu.Lock()
	rec := te.rec
	te.mu.Unlock()
	if rec == nil || len(rec.recs) == 0 {
		return 0, ""
	}
	return len(rec.recs), rec.recs[len(rec.recs)-1].upstream
}

// adminView 任务行的只读脱敏投影（dashboard P1-2；A2A-L19-W73-2）。
//
// 脱敏（零凭据字节）：sid 只回 8 字符前缀（`sidPrefix` 幂等——真实写入方已在
// serve 里截断，本投影绝不假设上游已截断）；key_id 走 `redactAdminText`（真值 =
// 记账面的 sha8，原样通过；明文 key 形态意外落入时被投影掉）；模型/上游/状态等
// 文本同样过 `redactAdminText`（模型名是客户端可控串）。
//
// attempts / upstream 的**收尾门**：`requestRecorder` 是单写者结构（attempts.go
// 头注「请求内单 goroutine 写」）——在飞任务的 `rec.recs` 正被任务 goroutine
// 追加，跨 goroutine 读是数据竞争（切片头撕裂 → 可能的越界/脏计数）。任务
// goroutine 在 serve 返回后 `defer close(te.done)`（runTask）——`done` 关闭 ⇒
// 追加已完成（happens-before）⇒ 读取安全。未收尾 → 两字段回零值（JSON null：
// 未知，不发明数字）。
//
// 纯读：绝不改任务状态、不写日志（`deliveryState` 的只读面同理）。
func (te *taskEntry) adminView() adminTaskView {
	te.mu.Lock()
	v := adminTaskView{
		ID:        redactAdminText(te.id),
		Model:     redactAdminText(te.model),
		Status:    te.status, // 六值枚举（task.go 常量）
		CreatedAt: te.createdAt,
		Delivered: te.delivered,
		Face:      adminTaskFaceUnknown, // 记录不存该位——绝不推断
	}
	if !te.terminalAt.IsZero() {
		at := te.terminalAt.Unix()
		v.TerminalAt = &at
	}
	if rec := te.rec; rec != nil {
		v.SIDPrefix = redactAdminText(sidPrefix(rec.sid))
		v.KeyID = redactAdminText(rec.keyID)
	}
	te.mu.Unlock()

	select {
	case <-te.done: // 已收尾：记账器写者已停（见函数头注）
		n, up := te.attemptStats()
		v.Attempts = &n
		if n > 0 && up != "" {
			name := redactAdminText(up)
			v.Upstream = &name
		}
	default: // 在飞：attempts/upstream 留 null
	}
	return v
}

// markDelivered 标记终态已交付（W36-1 S2；解缺口 A6 的任务面）：终态快照被 ≥1 次
// retrieve 或续流订阅者收到终态事件时调用。幂等——首次交付写一行日志（对账：
// 「终态已交付 vs 未交付」）；运行中任务调用 = no-op（终态才有交付口径）。纯
// 观测：绝不改任务状态、绝不进下游字节。返回本次是否落定（测试面）。
func (te *taskEntry) markDelivered(via string) bool {
	te.mu.Lock()
	if te.snapshot == nil || te.delivered {
		te.mu.Unlock()
		return false
	}
	te.delivered = true
	te.deliveredVia = via
	status := te.status
	te.mu.Unlock()
	payload, err := json.Marshal(struct {
		Task   string `json:"task"`
		Via    string `json:"via"`
		Status string `json:"status"`
	}{Task: te.id, Via: via, Status: status})
	if err != nil {
		return true // 全标量载荷——Marshal 失败 = 编程错误；绝不写半截日志。
	}
	log.Printf("any2api: task %s terminal delivered: %s", te.id, payload)
	return true
}

// deliveryState 终态交付位当前值 + 交付方式（测试/诊断面；false 时 via = ""）。
func (te *taskEntry) deliveryState() (bool, string) {
	te.mu.Lock()
	defer te.mu.Unlock()
	return te.delivered, te.deliveredVia
}

// setCancel 挂任务 ctx 取消函数（goroutine 启动前；mu 保护）。
func (te *taskEntry) setCancel(cancel context.CancelFunc) {
	te.mu.Lock()
	te.cancel = cancel
	te.mu.Unlock()
}

// requestCancel 请求取消（幂等）：已终态 → false（调用方直接返回现有终态）；
// 运行中 → 标记 + 真取消任务 ctx（中止在飞上游调用）→ true。
func (te *taskEntry) requestCancel() bool {
	te.mu.Lock()
	defer te.mu.Unlock()
	if te.snapshot != nil {
		return false
	}
	te.cancelReq = true
	if te.cancel != nil {
		te.cancel()
	}
	return true
}

// cancelRequested 是否已请求取消（runTask 收尾用）。
func (te *taskEntry) cancelRequested() bool {
	te.mu.Lock()
	defer te.mu.Unlock()
	return te.cancelReq
}

// start 运行中标记（queued → in_progress）。
func (te *taskEntry) start() {
	te.mu.Lock()
	if te.status == taskQueued {
		te.status = taskInProgress
	}
	te.mu.Unlock()
}

// finish 落终态（first-terminal-wins：已落过终态则不再改写）。
func (te *taskEntry) finish(status string, snapshot json.RawMessage, now time.Time) {
	te.mu.Lock()
	if te.snapshot == nil {
		te.status = status
		te.snapshot = snapshot
		te.terminalAt = now
	}
	te.mu.Unlock()
}

// snapshotFor 当前快照：运行中 = 合成最小快照；终态 = 终态快照原样。
func (te *taskEntry) snapshotFor() json.RawMessage {
	te.mu.Lock()
	defer te.mu.Unlock()
	if te.snapshot != nil {
		return te.snapshot
	}
	body, _ := json.Marshal(map[string]any{
		"id":         te.id,
		"object":     "response",
		"created_at": te.createdAt,
		"status":     te.status,
		"model":      te.model,
		"output":     []any{},
		"usage":      nil,
	})
	return body
}

// ---------------------------------------------------------------------------
// 任务 goroutine：下游"客户端"= 字节收集器 + SSE 终态扫描器
// ---------------------------------------------------------------------------

// taskTerminal 任务流里观测到的终态事件（response.completed/incomplete/failed）。
type taskTerminal struct {
	status string          // completed / incomplete / failed
	resp   json.RawMessage // response 对象（原样；落终态时改写 id）
}

// taskErrorInfo 流内 error 事件的兜底信息（无 response.* 终态时用）。
type taskErrorInfo struct {
	code    string
	message string
}

// taskStreamWriter 任务 goroutine 的"下游"：只收字节、不写网络。增量扫描 SSE
// 块，抽取终态事件与错误事件；状态码记第一写（writeError 的错误信封 ≥400 用于
// 归因）。并发保护与 downstream 同款（relay 主循环与心跳 goroutine 并发写）。
// log（Slice 3）：每个合法 JSON 事件块入有界缓冲（seq 自编 + 载荷改写）——
// 续流端点的数据源；nil = 不缓冲（单元测试直接构造 writer 时）。
type taskStreamWriter struct {
	mu       sync.Mutex
	hdr      http.Header
	status   int
	pending  []byte
	overflow bool
	term     *taskTerminal
	lastErr  *taskErrorInfo
	log      *taskEventLog
}

func (t *taskStreamWriter) Header() http.Header { return t.hdr }

func (t *taskStreamWriter) WriteHeader(code int) {
	t.mu.Lock()
	if t.status == 0 {
		t.status = code
	}
	t.mu.Unlock()
}

// Flush no-op（无网络面）。
func (t *taskStreamWriter) Flush() {}

// Write 收字节：切块 → 逐块扫描 → 残余留存（超限丢块防无界）。
func (t *taskStreamWriter) Write(b []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending = append(t.pending, b...)
	for {
		i := bytes.Index(t.pending, []byte("\n\n"))
		if i < 0 {
			break
		}
		t.scanBlock(t.pending[:i])
		t.pending = t.pending[i+2:]
	}
	if len(t.pending) > taskMaxPending {
		t.pending = t.pending[:0]
		t.overflow = true
	}
	return len(b), nil
}

// scanBlock 解析一个 SSE 块：抽 data 载荷（多行按 "\n" 连接）→ 事件观测。
// 注释/心跳帧（`: keepalive`）/哨兵/非 JSON 一律忽略。
func (t *taskStreamWriter) scanBlock(block []byte) {
	var payload []byte
	for _, line := range bytes.Split(block, []byte("\n")) {
		line = bytes.TrimRight(line, "\r")
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		d := bytes.TrimSpace(line[len("data:"):])
		if len(payload) > 0 {
			payload = append(payload, '\n')
		}
		payload = append(payload, d...)
	}
	d := bytes.TrimSpace(payload)
	if len(d) == 0 || d[0] != '{' {
		return
	}
	var probe struct {
		Type     string          `json:"type"`
		Code     string          `json:"code"`
		Message  string          `json:"message"`
		Response json.RawMessage `json:"response"`
	}
	if json.Unmarshal(d, &probe) != nil {
		return
	}
	if t.log != nil {
		// Slice 3：合法 JSON 事件入有界缓冲（seq 自编 + 载荷 sequence_number 改写；
		// 重放/实时同源）。非 JSON 块（注释/心跳）与畸形 JSON 不入——不是事件。
		t.log.appendEvent(block, d)
	}
	switch probe.Type {
	case "response.completed", "response.incomplete", "response.failed":
		// first-terminal-wins：首个 response.* 终态事件定终局。
		if t.term == nil && len(probe.Response) > 0 {
			status := strings.TrimPrefix(probe.Type, "response.")
			// 失败语义表（Slice 2）：本壳 idle 守卫合成的 response.failed（error.code
			// = upstream_idle）映射为任务 incomplete（「任务挂住」——提案 §失败语义
			// 表；其余 response.failed 保持 failed）。
			if status == taskFailed && responseErrorCode(probe.Response) == upstreamIdleCode {
				status = taskIncomplete
			}
			t.term = &taskTerminal{
				status: status,
				resp:   probe.Response,
			}
		}
	case "error":
		if t.lastErr == nil {
			t.lastErr = &taskErrorInfo{code: probe.Code, message: probe.Message}
		}
	}
}

// responseErrorCode 取终态 response 对象的 error.code（无/解析失败 → ""）。
func responseErrorCode(resp json.RawMessage) string {
	var w struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(resp, &w) != nil {
		return ""
	}
	return w.Error.Code
}

// result 任务收尾决策（serve 返回后调用）：终态状态 + 快照。
//
//	① 有 response.* 终态事件 → 该状态 + 上游 response 对象（id 改写为任务 id）；
//	② 无终态但写过错信封（status ≥ 400，serve 的提交前失败面）→ failed +
//	   信封 error（code/message）合成快照；
//	③ 其余（流内 error 事件 / 空产出）→ failed + 兜底信息。
func (t *taskStreamWriter) result(id, model string, createdAt int64) (string, json.RawMessage) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.term != nil {
		return t.term.status, rewriteTaskTerminal(t.term.resp, id, t.term.status)
	}
	code, msg := "server_error", "task ended without terminal event"
	switch {
	case t.status >= 400:
		code, msg = parseTaskErrorEnvelope(t.pending, code, msg)
	case t.lastErr != nil:
		if t.lastErr.code != "" {
			code = t.lastErr.code
		}
		if t.lastErr.message != "" {
			msg = t.lastErr.message
		}
	case t.overflow:
		msg = "task output exceeded buffer limit before terminal event"
	}
	return taskFailed, taskFailedSnapshot(id, model, createdAt, code, msg)
}

// parseTaskErrorEnvelope 解析 serve 错误信封（responses 形态
// `{"error":{code,message,type}}`；解析失败 → 兜底值）。
func parseTaskErrorEnvelope(body []byte, defCode, defMsg string) (string, string) {
	var env struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(bytes.TrimSpace(body), &env) == nil && env.Error.Message != "" {
		code := env.Error.Code
		if code == "" {
			code = defCode
		}
		return code, env.Error.Message
	}
	return defCode, defMsg
}

// taskFailedSnapshot 合成 failed 快照（无上游 response 对象可用的收尾面）。
func taskFailedSnapshot(id, model string, createdAt int64, code, msg string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"id":         id,
		"object":     "response",
		"created_at": createdAt,
		"status":     taskFailed,
		"model":      model,
		"output":     []any{},
		"usage":      nil,
		"error":      map[string]string{"code": code, "message": msg},
	})
	return b
}

// taskCancelledSnapshot 合成 cancelled 快照（取消面收尾：abort 无上游终态对象
// 可用；形状与 create 快照同族——status 落 cancelled；取消不是错误，无 error）。
func taskCancelledSnapshot(id, model string, createdAt int64) json.RawMessage {
	b, _ := json.Marshal(map[string]any{
		"id":         id,
		"object":     "response",
		"created_at": createdAt,
		"status":     taskCancelled,
		"model":      model,
		"output":     []any{},
		"usage":      nil,
	})
	return b
}

// rewriteTaskTerminal 终态 response 对象的 id/status 改写为任务 id/任务终态状态
// （创建/轮询同一 id 的客户端一致性；status 恒与任务状态一致——idle→incomplete
// 映射时上游帧里仍是 failed）。output/usage/error 等其余字段逐字段保留。解析
// 失败原样返回（保真优先）。
func rewriteTaskTerminal(resp json.RawMessage, id, status string) json.RawMessage {
	var m map[string]json.RawMessage
	if json.Unmarshal(resp, &m) != nil {
		return resp
	}
	if enc, err := json.Marshal(id); err == nil {
		m["id"] = enc
	}
	if status != "" {
		if enc, err := json.Marshal(status); err == nil {
			m["status"] = enc
		}
	}
	out, err := json.Marshal(m)
	if err != nil {
		return resp
	}
	return out
}

// sawTerminal 流内是否观测到 response.* 终态事件（runTask 的取消收尾判据：
// 已观测终态 → 终态优先，取消不翻转；未观测 + 已请求取消 → cancelled）。
func (t *taskStreamWriter) sawTerminal() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.term != nil
}

// ---------------------------------------------------------------------------
// 入口分流 + 处理器
// ---------------------------------------------------------------------------

// backgroundRequested 顶层 `background` 是否为布尔 true——任务面唯一入口判据。
// 非对象/畸形/缺省/false/其它类型 → false：走现有同步路径零改动。
func backgroundRequested(body []byte) bool {
	var probe struct {
		Background json.RawMessage `json:"background"`
	}
	if json.Unmarshal(body, &probe) != nil {
		return false
	}
	return bytes.Equal(bytes.TrimSpace(probe.Background), []byte("true"))
}

// taskUpstreamBody 任务面上游请求体的三处手术（其余字节逐字节保留，同
// rewriteModelField 的字节纪律）：
//
//  1. 剥除顶层 `background`——托管语义 = 网关自己轮询/缓冲，上游只被当作一次
//     正常（长）流式调用；background 绝不注入上游（红线 5 的禁注入面）。
//  2. 剥除顶层 `idempotency_key`（Slice 4）——网关自有字段（幂等键语义在任务
//     创建面消化），上游协议不认识它（严格上游会 400 未知参数）；头载体的键
//     本就不进上游请求头（上游请求头由 relay 自建）。
//  3. 顶层 `stream` 强制 true——上游调用恒为流式（relay 语义）；客户端体
//     缺省/false 也要改（任务面 = 网关消费事件流）。
//
// 入参已由 backgroundRequested 验证为 JSON 对象（畸形体进不来）。
func taskUpstreamBody(body []byte) []byte {
	body = removeTopLevelField(body, "background")
	body = removeTopLevelField(body, "idempotency_key")
	return forceTopLevelFieldTrue(body, "stream")
}

// removeTopLevelField 手术式剥除顶层 JSON 字段（键 + 值 + 必要逗号），其余字节
// 逐字节保留。重复键逐个剥（循环到没有）；非对象/字段不存在 → 原样返回。
func removeTopLevelField(body []byte, key string) []byte {
	for {
		out, removed := removeFirstTopLevelField(body, key)
		if !removed {
			return out
		}
		body = out
	}
}

// removeFirstTopLevelField 剥除第一个匹配字段。逗号处理：非首字段 → 删除区间
// 从上一值结束处起（吃掉前置逗号）；首字段 → 删除区间延伸到后继逗号（若有）。
func removeFirstTopLevelField(body []byte, key string) ([]byte, bool) {
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return body, false
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return body, false
	}
	fields := 0
	for dec.More() {
		beforeKey := int(dec.InputOffset()) // 上一值结束处（首字段 = `{` 之后）
		keyTok, err := dec.Token()
		if err != nil {
			return body, false
		}
		k, ok := keyTok.(string)
		if !ok {
			return body, false
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return body, false
		}
		valueEnd := int(dec.InputOffset())
		fields++
		if k != key {
			continue
		}
		start, end := beforeKey, valueEnd
		if fields == 1 {
			i := end
			for i < len(body) && isJSONSpace(body[i]) {
				i++
			}
			if i < len(body) && body[i] == ',' {
				end = i + 1
			}
		}
		out := append([]byte(nil), body[:start]...)
		out = append(out, body[end:]...)
		return out, true
	}
	return body, false
}

// forceTopLevelFieldTrue 顶层字段强制布尔 true：已存在 → 所有出现位置的值改写
// 为 true（重复键按 JSON 生态「最后值胜」惯例，全部改掉最稳）；缺省 → 在闭合
// `}` 前插入 `"<key>":true`。其余字节逐字节保留。
func forceTopLevelFieldTrue(body []byte, key string) []byte {
	for {
		out, found, rewrote := rewriteFirstNonTrueBool(body, key)
		if rewrote {
			body = out
			continue
		}
		if found {
			return body
		}
		return insertTopLevelBoolField(body, key)
	}
}

// rewriteFirstNonTrueBool 把第一个值非 true 的顶层 `<key>` 改写为 true。
// 返回 (新体, 字段存在, 是否改写)。
func rewriteFirstNonTrueBool(body []byte, key string) ([]byte, bool, bool) {
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return body, false, false
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return body, false, false
	}
	found := false
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return body, found, false
		}
		k, ok := keyTok.(string)
		if !ok {
			return body, found, false
		}
		afterKey := int(dec.InputOffset())
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return body, found, false
		}
		valueEnd := int(dec.InputOffset())
		if k != key {
			continue
		}
		found = true
		if bytes.Equal(bytes.TrimSpace(raw), []byte("true")) {
			continue
		}
		// 值起始：键结束 → 跳空白 → `:` → 跳空白。
		i := afterKey
		for i < valueEnd && isJSONSpace(body[i]) {
			i++
		}
		if i >= valueEnd || body[i] != ':' {
			return body, found, false
		}
		i++
		for i < valueEnd && isJSONSpace(body[i]) {
			i++
		}
		if i >= valueEnd {
			return body, found, false
		}
		out := make([]byte, 0, len(body)-(valueEnd-i)+4)
		out = append(out, body[:i]...)
		out = append(out, "true"...)
		out = append(out, body[valueEnd:]...)
		return out, found, true
	}
	return body, found, false
}

// insertTopLevelBoolField 在顶层对象闭合 `}` 前插入 `"<key>":true`（空对象不加
// 逗号）。非对象 → 原样返回。
func insertTopLevelBoolField(body []byte, key string) []byte {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return body
	}
	closeIdx := bytes.LastIndexByte(body, '}')
	if closeIdx < 0 {
		return body
	}
	insert := []byte(`"` + key + `":true`)
	empty := len(bytes.TrimSpace(body[1:closeIdx])) == 0
	out := make([]byte, 0, len(body)+len(insert)+1)
	out = append(out, body[:closeIdx]...)
	if !empty {
		out = append(out, ',')
	}
	out = append(out, insert...)
	out = append(out, body[closeIdx:]...)
	return out
}

// taskIdemKeyMax 幂等键长度上限（有界内存：键表随任务有界、键本身也须有界——
// 请求体上限 32MB，键字段可极大；超限 400）。
const taskIdemKeyMax = 255

// taskIdempotencyKey 提取幂等键（Slice 4；父代裁决 #4：两者都收、头优先）：
// `Idempotency-Key` 头 > 顶层 `idempotency_key` 字符串字段（均 trim 后非空）。
// 两者皆无/非字符串/空 → ""（无键 = 原路径）。
func taskIdempotencyKey(r *http.Request, body []byte) string {
	if k := strings.TrimSpace(r.Header.Get("Idempotency-Key")); k != "" {
		return k
	}
	var probe struct {
		Key json.RawMessage `json:"idempotency_key"`
	}
	if json.Unmarshal(body, &probe) != nil || len(probe.Key) == 0 {
		return ""
	}
	var k string
	if json.Unmarshal(probe.Key, &k) != nil {
		return ""
	}
	return strings.TrimSpace(k)
}

// taskIdemCompareBody 幂等内容指纹输入 = 剥除顶层 `idempotency_key` 字段后的
// 请求体（键字段本身不参与内容比较——头/体两种载体的同一请求判定一致；比较是
// 字节级：同键不同字节 = 冲突（409），绝不重放不同请求）。
func taskIdemCompareBody(body []byte) []byte {
	return removeTopLevelField(body, "idempotency_key")
}

// handleTaskCreate 任务创建面（POST /v1/responses + background:true）：立即合成
// 快照（绝不等待上游）→ 后台 goroutine 跑上游（runTask）。
//
// 幂等键（Slice 4；父代裁决 #4「两者都收、头优先」）：同键同体重提 → 原任务 id
// 原样返回当前快照（1 任务、N 请求——绝不新建任务/第二个上游调用）；同键不同体
// → 409 `idempotency_key_conflict`（保守裁定：绝不重放不同请求、绝不建第二任务）；
// 键生命周期随任务（任务过期墓碑化 → 键回收 → 重提 = 新任务）。
func (s *Server) handleTaskCreate(w http.ResponseWriter, r *http.Request, body []byte, meta entryMeta) {
	if strings.TrimSpace(meta.Model) == "" {
		s.writeError(w, registry.FormatOpenAIResponses, http.StatusBadRequest,
			&ir.Error{Type: "invalid_request_error", Message: "model is required", Source: "any2api"})
		return
	}
	// Slice 5 任务面目标裁定（task_passthrough.go 头注「形态裁定」）：候选链第一个
	// 非 off 上游的能力声明决定形态——passthrough → 直连透传（id = 上游 id）；
	// managed → 下方托管路径原样。model = 请求面模型 id（A6 改写前）→ 命中
	// `models[].upstreams` 时任务创建面与托管任务候选链同源（A2A-L19-W82-1）。
	// 全 off / 白名单过滤后为空 → 404 task_face_unavailable。
	idx, face, ok := s.taskFaceTarget(meta.Model)
	if !ok {
		s.writeError(w, registry.FormatOpenAIResponses, http.StatusNotFound,
			&ir.Error{Type: "invalid_request_error", Code: "task_face_unavailable",
				Message: "task face is disabled for all upstreams (task_face: off)", Source: "any2api"})
		return
	}
	if face == taskFacePassthrough {
		s.handleTaskPassthroughCreate(w, r, body, meta, idx)
		return
	}
	key := taskIdempotencyKey(r, body)
	if len(key) > taskIdemKeyMax {
		s.writeError(w, registry.FormatOpenAIResponses, http.StatusBadRequest,
			&ir.Error{Type: "invalid_request_error", Code: "invalid_idempotency_key",
				Message: fmt.Sprintf("idempotency key exceeds %d bytes", taskIdemKeyMax), Source: "any2api"})
		return
	}
	id := hygiene.SynthUniqueID("resp")
	te, replay, conflict, err := s.tasks.createIdem(id, meta.Model, key, taskIdemCompareBody(body))
	if conflict {
		s.writeError(w, registry.FormatOpenAIResponses, http.StatusConflict,
			&ir.Error{Type: "invalid_request_error", Code: "idempotency_key_conflict",
				Message: "Idempotency-Key was already used with a different request body", Source: "any2api"})
		return
	}
	if err != nil {
		s.writeError(w, registry.FormatOpenAIResponses, http.StatusServiceUnavailable,
			&ir.Error{Type: "service_unavailable_error", Code: "task_limit_reached",
				Message: "task store is full", Source: "any2api"})
		return
	}
	if replay {
		// 幂等命中（同键同体重提）：同任务 id 返回当前快照——不建任务、不起
		// goroutine、不触上游（键表在 store 锁内查得，命中判定与首建互斥）。
		// S7（W36-1）：幂等重放计数（任务面运维面；tasks_created 不增——未建新任务）。
		s.diag.tasksIdemReplays.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(te.snapshotFor())
		return
	}
	// S7（W36-1）：托管任务创建计数（store 新建；幂等重放不计——见上）。
	s.diag.tasksCreated.Add(1)
	// 任务 ctx（Slice 2）：与提交请求生命周期解耦（下游拿到快照即完成；断开不杀
	// 任务），但由任务自持取消函数（cancel 面真中止在飞上游调用）。setCancel 先于
	// 快照写出——客户端拿到 id 后任何时刻 cancel 都有效（含 goroutine 未调度时）。
	ctx, cancel := context.WithCancel(context.Background())
	te.setCancel(cancel)

	snap := te.snapshotFor()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(snap)

	// 后台 goroutine：复用 serve——failover（C1 链）/提交点/心跳/错误帧/记账语义照旧。
	dr := downstreamRequest{
		entry:  registry.FormatOpenAIResponses,
		body:   taskUpstreamBody(body),
		model:  meta.Model,
		stream: true,
		// taskID（Slice 4）：记账归因——该 goroutine 的 per-request 记账行带任务
		// id（attempts 数组按任务 join；终态 usage 另有 per-task 记账行）。
		taskID: id,
		// task（W36-1 S1/S6）：取消归因细分（end_reason=task_cancel）与任务终态
		// ring 事件的 attempts/末上游来源（serve 回填记账器）。
		task: te,
		// taskFace（Slice 5）：候选链剔除 task_face=off 上游（任务流量绝不落
		// off 上游；与创建裁定的候选面同源）。
		taskFace: true,
		opts: pairs.Options{
			IncludeUsage: meta.IncludeUsage,
			Stream:       true,
		},
	}
	r2 := r.WithContext(ctx)
	go func() {
		defer cancel() // goroutine 退出即释放任务 ctx（cancel 幂等，可重复调用）
		s.runTask(te, r2, dr)
	}()
}

// runTask 任务 goroutine：复用 serve 跑上游长流，把终态落进 store。
//
// 收尾决策（失败语义表；first-terminal-wins 的取消面）：流内观测到上游终态 →
// 该终态（取消不翻转）；未观测到 + 已请求显式取消 → cancelled（取消 abort 不是
// 上游失败——relay 的 ctx 取消路径归因 disconnected，不计上游健康）；其余 →
// result 的失败映射（信封/截断/兜底）。
func (s *Server) runTask(te *taskEntry, r *http.Request, dr downstreamRequest) {
	defer close(te.done)
	te.start()
	tw := &taskStreamWriter{hdr: http.Header{}, log: te.log}
	s.serve(tw, r, dr)
	status, snap := tw.result(te.id, te.model, te.createdAt)
	if !tw.sawTerminal() && te.cancelRequested() {
		status, snap = taskCancelled, taskCancelledSnapshot(te.id, te.model, te.createdAt)
	}
	te.finish(status, snap, s.tasks.stamp())
	// S7（W36-1）：任务终态计数（四值；与 tasks_created 的差 = 运行中/未落终态）。
	switch status {
	case taskCompleted:
		s.diag.tasksTerminalCompleted.Add(1)
	case taskFailed:
		s.diag.tasksTerminalFailed.Add(1)
	case taskIncomplete:
		s.diag.tasksTerminalIncomplete.Add(1)
	case taskCancelled:
		s.diag.tasksTerminalCancelled.Add(1)
	}
	// S6（W36-1）：任务终态落 ring（task_terminal；提交后流内失败同批——本行
	// 覆盖全部终态出口：completed/failed/incomplete/cancelled）。
	s.recordTaskTerminal(te, status)
	// Slice 4：任务终态 usage 落账（每任务一条；failed/cancelled 无终态 usage →
	// 0/0 不计数，下界在 per-attempt 面——提案 §记账点）。
	s.recordTaskUsage(te.id, dr.entry, te.model, status, snap)
	// Slice 3：缓冲封口（终态落定——不再有新事件；续流订阅者据此收尾）。终态事件
	// 已在 serve 内入缓冲（若有）——封口只影响「之后」。
	te.log.closeLog()
}

// recordTaskTerminal 任务终态落 ring（W36-1 S6；缓解缺口 C5 的任务面）：载荷 =
// `{task,status,attempts,duration_ms}`（JSON 进 ring message）；ring 行的
// `upstream` = 末次尝试上游名（无尝试 = 空）、`request_id` = 任务 id（与
// attempt/usage 记账行、续流日志可关联）。纯观测面：ring 满即覆盖最旧，绝不改
// 任务终态、绝不阻塞任务收尾。透传任务（task_face=passthrough）不落本事件——
// 网关不托管其终态（见报告 §口径）。
func (s *Server) recordTaskTerminal(te *taskEntry, status string) {
	attempts, lastUp := te.attemptStats()
	te.mu.Lock()
	at := te.terminalAt
	te.mu.Unlock()
	dur := int64(0)
	if !at.IsZero() {
		dur = at.Sub(time.Unix(te.createdAt, 0)).Milliseconds()
	}
	payload, err := json.Marshal(struct {
		Task       string `json:"task"`
		Status     string `json:"status"`
		Attempts   int    `json:"attempts"`
		DurationMs int64  `json:"duration_ms"`
	}{Task: te.id, Status: status, Attempts: attempts, DurationMs: dur})
	if err != nil {
		return // 全标量载荷——Marshal 失败 = 编程错误；绝不写半截 ring 事件。
	}
	s.adminLog.add(adminLogTaskTerminal, lastUp, te.id, string(payload), 0)
}

// writeTaskMiss 未知/过期任务 id 的 404 收尾（error.code 区分 task_not_found /
// task_expired；父代裁决 #2；retrieve/cancel 两面同形）。
func (s *Server) writeTaskMiss(w http.ResponseWriter, id, miss string) {
	code, msg := "task_not_found", "task not found: "+id
	if miss == taskMissExpired {
		code, msg = "task_expired", "task expired: "+id
	}
	s.writeError(w, registry.FormatOpenAIResponses, http.StatusNotFound,
		&ir.Error{Type: "invalid_request_error", Code: code, Message: msg, Source: "any2api"})
}

// handleTaskRetrieve 任务轮询面：GET /v1/responses/{id} → 任务快照。
// 未知/过期 → 404（error.code 区分 task_not_found / task_expired；父代裁决 #2）。
// 不经 debug capture（轮询高频、无上游尝试可录——录制面只在入口请求）。
//
// `?stream=true`（Slice 3）→ 续流面（重放 + 接续实时；过期游标 → 快照降级），
// 见 handleTaskResume。无 stream=true → 本函数原快照面（字节零改动）。
func (s *Server) handleTaskRetrieve(w http.ResponseWriter, r *http.Request) {
	s.diag.requests.Add(1)
	id := r.PathValue("id")
	te, miss := s.tasks.get(id)
	if te == nil {
		// Slice 5：透传任务（id = 上游 id）——本地 store 未命中 → 路由表命中 →
		// 直连上游任务端点（查询串/游标逐字透传：`stream=true&starting_after=N`
		// 续流同路径）；两表都未命中 → 既有 404 双 code。
		if idx, ok := s.taskRoutes.lookup(id); ok {
			s.forwardTaskRoute(w, r, idx, false)
			return
		}
		s.writeTaskMiss(w, id, miss)
		return
	}
	if r.URL.Query().Get("stream") == "true" {
		s.handleTaskResume(w, r, te)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	// S2（W36-1）：终态快照被 retrieve 且写出无错 = 交付（对账面；运行中快照
	// 不落——markDelivered 内部按终态判定，幂等一次）。
	if _, werr := w.Write(te.snapshotFor()); werr == nil {
		te.markDelivered("retrieve")
	}
}

// handleTaskCancel 任务取消面（POST /v1/responses/{id}/cancel；Slice 2）。
//
// 幂等语义（提案 §失败语义表「客户端取消」行）：运行中 → 标记取消 + 取消任务
// ctx（真中止在飞上游调用）→ 等 goroutine 收尾（≤taskCancelWait）→ 返回终态
// 快照 cancelled；已终态 → 原样返回现有终态快照（重复 cancel 同终态——同字节）。
// 未知/过期 id → 404（与 retrieve 同双 code）。超时/调用方断开 → 返回当前快照
// （任务仍会收敛到终态；再次 cancel 仍是幂等面）。
func (s *Server) handleTaskCancel(w http.ResponseWriter, r *http.Request) {
	s.diag.requests.Add(1)
	id := r.PathValue("id")
	te, miss := s.tasks.get(id)
	if te == nil {
		// Slice 5：透传任务取消——路由表命中 → 直连上游 `POST /{id}/cancel`
		// （幂等语义由上游承担；响应原样转发）；两表都未命中 → 既有 404 双 code。
		if idx, ok := s.taskRoutes.lookup(id); ok {
			s.forwardTaskRoute(w, r, idx, true)
			return
		}
		s.writeTaskMiss(w, id, miss)
		return
	}
	if te.requestCancel() {
		select {
		case <-te.done:
		case <-time.After(taskCancelWait):
		case <-r.Context().Done():
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(te.snapshotFor())
}
