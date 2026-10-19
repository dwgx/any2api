// 任务面 Slice 5（A2A-L19-W27-1）：上游透传优化 + 能力表。
//
// 能力表（config `upstreams[].task_face`，提案 `service-task-surface` §形态 C
// 原文；缺省 = managed）：
//
//	managed（缺省）  网关托管（Slice 1-4 面原样）：background 服务层剥除、
//	                网关自造 id（resp_synth_<32hex>）+ 事件缓冲 + 续流/取消/
//	                记账；任务流量（托管任务的上游尝试）可落该上游。
//	passthrough     上游原生任务面（**仅 ProtocolResponses 合法**——OpenAI 类）：
//	                background **原样透传**，**id = 上游 id**，retrieve/cancel/
//	                续流（`?stream=true&starting_after=N` 查询串逐字）直连上游
//	                任务端点；网关只持 id→上游 路由表（本文件），不缓冲、
//	                不合成终态、不重放。create **不重试**（重试可能多建上游
//	                任务——孤儿任务无法回滚；单次尝试，失败 = 上游错误直传）。
//	off             任务面关闭：任务流量**绝不**落该上游（create 跳过；托管任务
//	                的候选链同样跳过——candidatesForModel(taskFace=true, model)）；
//	                链上无非 off 上游时任务创建 404（error.code=task_face_unavailable）。
//	                同步路径（background 缺省/false）不受影响——off 只关任务面。
//
// 形态裁定（创建面）：候选链（模型 `upstreams` 白名单 → taskFace → 冷却；全冷却
// → 该面过滤后列表）里**第一个** `task_face != off` 的上游的能力声明决定形态
// ——passthrough → 直连透传该台；managed → 既有托管路径（任务 goroutine 的候选
// 链 = candidatesForModel(taskFace=true, model)，首个尝试台与创建裁定同源，模型
// 取请求面 id）。链序决定选台（运营者控制顺序），能力声明决定该台的形态（提案
// 「上游能透传就透传，不能就托管」的逐台映射）。
//
// 透传任务回访（retrieve/cancel/续流）：本地托管 store 未命中 → 路由表命中 →
// 直连上游任务端点（GET/POST `/v1/responses/{id}` 与 `/{id}/cancel`，查询串
// 逐字透传）；两表都未命中 → 既有 404 双 code（task_not_found / task_expired）。
// 路由表在 create 成功（上游 2xx + JSON 体带 id）时写入；**SSE 形态的创建响应
// 不解析 id**（stream:true + background:true 同发：响应原样转发，id 不入路由表
// ——该 id 的轮询/续流将 404；见报告 §边界）。
//
// 有界（提案 §形态 A「网关零存储（只需 id→上游 路由表，TTL ≥ 上游窗口）」）：
// 内存 + 双门（TTL passthroughRouteTTL + 条目上限 passthroughRouteMax，FIFO
// 驱逐最老）；单进程 = 重启即丢（同托管 store 边界，父代裁决 #6）。
//
// 纪律对齐：
//   - 网关自有字段（Slice 4 幂等键 `idempotency_key`）在透传创建面剥除（上游
//     协议不认识；键语义只随托管任务——透传面 id 由上游定，幂等 = 上游责任）；
//     `Idempotency-Key` 头同样不转发（上游请求头自建，与 relay 转发纪律一致）。
//   - A6 模型别名映射照旧（透传创建面同样走 upstreamModelFor + rewriteModelField
//     ——入口级特性，与托管/同步面一致）。
//   - 上游请求头 = 自建（Bearer 认证；不转发客户端头）——与 forward 的
//     ProtocolResponses 分支同款；响应 = 状态码 + Content-Type + 体原样转发
//     （SSE 逐块 flush），失败语义全部上游直传（提案 §形态 A「网关不合成终态、
//     不兜底」）——唯一合成面 = 网络层失败（502 api_error 信封，上游语义不可得）。
//   - 不接 debug capture（透传往返不经 forward/relay；同托管 goroutine 的录制
//     边界——见 task.go 头注）；不计上游健康（透传面不做 failover/冷却记账——
//     亲和表钉死单台，换台 = 任务丢）。
//
// W36-2（S4 回访 usage 观测 + S8 能力面被动证据）：
//   - S4 只读旁路：回访响应（retrieve/cancel，非 SSE、2xx）经网关时用 tee 观测
//     终态 usage（**原样转发字节/时机零改动**；流尽才解析——部分读/断开不落账）
//     → per-task 记录（source=passthrough；路由条目去重 = 每任务一条）——零额外
//     上游调用、绝不进下游字节。
//   - S8 被动证据：往返上游 404/405（**结论性失败**；网络失败/5xx/429 不下结论
//     ——同 sub2api 结论闸）→ 诊断计数 + ring + 日志；`task_face_degrade: auto`
//     时首个结论性失败进程内降级该上游 passthrough→managed（重启清零、绝不写回
//     配置——声明 L1 仍是唯一配置权威）。见 observeTaskFaceEvidence。
package service

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/registry"
)

// 透传路由表有界双门（见文件头注）。
const (
	// passthroughRouteTTL 路由条目保留期：> 上游任务窗口（OpenAI background
	// 轮询窗口 ~10min 的 6 倍余量；上游保留期政策仍在网关之上——条目过期 =
	// 路由丢失 → 404 task_not_found，文档写明）。
	passthroughRouteTTL = time.Hour
	// passthroughRouteMax 路由表条目上限（满员先清过期，仍满 → 驱逐最老）。
	passthroughRouteMax = 4096
)

// passthroughRoute 单条 id→上游 亲和记录。
type passthroughRoute struct {
	upstream int
	at       time.Time
	// usageObserved 该任务是否已观测终态 usage（W36-2 S4：回访响应只读旁路
	// 观测的去重位——每任务一条记录；条目覆盖（同 id 重记）即重置生命周期）。
	usageObserved bool
}

// passthroughRoutes 透传任务 id→上游 路由表（单进程内存；mu 保护）。
// now/ttl/max 可注入（测试用假时钟/短 TTL）。
type passthroughRoutes struct {
	mu      sync.Mutex
	entries map[string]passthroughRoute
	now     func() time.Time
	ttl     time.Duration
	max     int
}

func newPassthroughRoutes() *passthroughRoutes {
	return &passthroughRoutes{
		entries: map[string]passthroughRoute{},
		now:     time.Now,
		ttl:     passthroughRouteTTL,
		max:     passthroughRouteMax,
	}
}

// record 记一条路由（create 成功、响应体带 id 时；重复 id 覆盖 = 更新时刻/上游）。
// 满员先清过期；仍满 → 驱逐最老（FIFO by at；有界内存）。
func (rt *passthroughRoutes) record(id string, idx int) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	now := rt.now()
	rt.purgeLocked(now)
	if _, exists := rt.entries[id]; !exists {
		for len(rt.entries) >= rt.max {
			var oldestID string
			var oldestAt time.Time
			first := true
			for k, e := range rt.entries {
				if first || e.at.Before(oldestAt) {
					oldestID, oldestAt, first = k, e.at, false
				}
			}
			if oldestID == "" {
				break
			}
			delete(rt.entries, oldestID)
		}
	}
	rt.entries[id] = passthroughRoute{upstream: idx, at: now}
}

// lookup 查路由：命中且未过期 → (上游下标, true)；过期 → 惰性删除 + miss；
// 未知 → miss（调用方回落既有 404 面）。
func (rt *passthroughRoutes) lookup(id string) (int, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	e, ok := rt.entries[id]
	if !ok {
		return 0, false
	}
	if rt.now().Sub(e.at) >= rt.ttl {
		delete(rt.entries, id)
		return 0, false
	}
	return e.upstream, true
}

// purgeLocked 清过期条目（record 前调用）。
func (rt *passthroughRoutes) purgeLocked(now time.Time) {
	for id, e := range rt.entries {
		if now.Sub(e.at) >= rt.ttl {
			delete(rt.entries, id)
		}
	}
}

// markUsageObserved 标记该任务已观测终态 usage（W36-2 S4 去重；返回 true = 首次
// 落账——调用方据此写记录）。未命中/已过期（惰性删除，同 lookup 语义）/已标记
// → false（不落账）。条目在 = 回访面刚刚路由命中（lookup 已判活），本函数仍
// 自判过期以防调用序竞态。
func (rt *passthroughRoutes) markUsageObserved(id string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	e, ok := rt.entries[id]
	if !ok {
		return false
	}
	if rt.now().Sub(e.at) >= rt.ttl {
		delete(rt.entries, id)
		return false
	}
	if e.usageObserved {
		return false
	}
	e.usageObserved = true
	rt.entries[id] = e
	return true
}

// ---------------------------------------------------------------------------
// 创建面
// ---------------------------------------------------------------------------

// taskFaceTarget 任务面目标裁定（见文件头注「形态裁定」）：候选链（模型
// `upstreams` 白名单 → taskFace → 冷却；全冷却 → 该面过滤后列表）里第一个
// `task_face != off` 的上游。model = 请求面模型 id（A6 改写前；`models[].id`
// 匹配键）——命中白名单时任务创建面与托管任务的 serve 候选链同源，非 Claude
// 模型绝不先选 claude_web（A2A-L19-W82-1）。无可用上游（全 off / 白名单过滤
// 后为空）→ ok=false（调用方 404 task_face_unavailable）。返回 = **有效**任务面
// （W36-2 S8：被结论性证据进程内降级的上游按 managed 返回，见 effectiveTaskFace
// ——off 剔除仍按声明判定，降级只改 passthrough→managed）。
func (s *Server) taskFaceTarget(model string) (idx int, face string, ok bool) {
	idxs, _ := s.candidatesForModel(time.Now(), true, model)
	if len(idxs) == 0 {
		return 0, "", false
	}
	i := idxs[0]
	return i, s.effectiveTaskFace(i), true
}

// effectiveTaskFace 有效任务面（W36-2 S8）：声明能力（L1——配置期唯一权威），
// 除非该上游已被结论性证据**进程内**降级——passthrough→managed（「上游能透传就
// 透传，不能就托管」）；只影响本进程、重启清零、绝不写回配置（见
// observeTaskFaceEvidence / Config.TaskFaceDegrade）。
func (s *Server) effectiveTaskFace(idx int) string {
	face := s.cfg.Upstreams[idx].taskFace()
	if face == taskFacePassthrough {
		if _, degraded := s.taskFaceDegraded.Load(idx); degraded {
			return taskFaceManaged
		}
	}
	return face
}

// handleTaskPassthroughCreate 透传创建面（task_face=passthrough 上游）：
// 请求体**原样**转发上游 `POST /v1/responses`（`background` 保留——透传面核心；
// 仅剥网关自有 `idempotency_key` 字段 + A6 别名映射），上游响应原样转发下游；
// 2xx + JSON 体带 id → 记路由表（retrieve/cancel/续流回找）。
func (s *Server) handleTaskPassthroughCreate(w http.ResponseWriter, r *http.Request, body []byte, meta entryMeta, idx int) {
	// A6 模型别名映射（与同步路径 serve 同款；命中配置表且声明 upstream_model
	// 时改写请求体顶层 model）。
	if resolved := s.cfg.upstreamModelFor(meta.Model); resolved != meta.Model {
		if nb, ok := rewriteModelField(body, resolved); ok {
			body = nb
		}
	}
	// 网关自有字段剥除（Slice 4 幂等键字段——上游协议不认识，严格上游会 400；
	// 键语义只随托管任务）。background **保留**（与托管路径 taskUpstreamBody 的
	// 手术相反——透传面核心：上游原生任务面需要它）。
	body = removeTopLevelField(body, "idempotency_key")
	s.taskPassthroughRoundTrip(w, r, idx, http.MethodPost, "/v1/responses", body, true)
}

// ---------------------------------------------------------------------------
// 回访面（retrieve/cancel/续流）
// ---------------------------------------------------------------------------

// forwardTaskRoute 透传任务回访：GET /v1/responses/{id}（含
// `?stream=true&starting_after=N` 续流——查询串逐字透传 = 游标透传）与
// POST /v1/responses/{id}/cancel 直连上游任务端点；响应原样转发。
func (s *Server) forwardTaskRoute(w http.ResponseWriter, r *http.Request, idx int, cancel bool) {
	path := "/v1/responses/" + url.PathEscape(r.PathValue("id"))
	if cancel {
		path += "/cancel"
	}
	s.taskPassthroughRoundTrip(w, r, idx, r.Method, path, nil, false)
}

// ---------------------------------------------------------------------------
// 往返（create/retrieve/cancel/续流同一装配）
// ---------------------------------------------------------------------------

// taskPassthroughRoundTrip 透传请求-响应往返（见文件头注「纪律对齐」）：
//
//   - URL = 上游 base_url + path + 客户端查询串**逐字**（`stream=true` 与
//     `starting_after` 游标透传）；id 路径段转义（防路径注入）。
//   - 认证 = Bearer（responses 协议，与 forward 同款；passthrough 已由配置
//     校验限定 responses 协议）；上游请求头自建（不转发客户端头）。
//   - 响应 = 状态码 + Content-Type + 体原样（SSE 逐块 flush；非 SSE 同路径）。
//   - recordID（create）且上游 2xx 且非 SSE → 整段读（有界 maxBodyBytes）→
//     抽顶层 id 记路由 → 原样转发。
//   - 观测面（W36-2）：上游 404/405 → L2 被动证据（计数/ring/日志 + 可选进程内
//     降级）；回访（非 create）非 SSE 2xx → tee 只读旁路观测终态 usage（原样
//     转发字节/时机零改动）。
//   - 网络层失败（上游语义不可得）→ 502 api_error 信封（唯一合成面）。
func (s *Server) taskPassthroughRoundTrip(w http.ResponseWriter, r *http.Request, idx int, method, path string, body []byte, recordID bool) {
	up := s.cfg.Upstreams[idx]
	// 上游尝试计数（与 forward 的 diag.attempts 同口径：透传往返 = 一次尝试；
	// 逐台钉死无 failover，不计健康/冷却）。
	s.diag.attempts.Add(1)
	target := strings.TrimRight(up.BaseURL, "/") + path
	if q := r.URL.RawQuery; q != "" {
		target += "?" + q
	}
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(r.Context(), method, target, rd)
	if err != nil {
		s.writeError(w, registry.FormatOpenAIResponses, http.StatusBadGateway,
			&ir.Error{Type: "api_error", Message: "task upstream request cannot be built: " + err.Error(), Source: "any2api"})
		return
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if v := r.Header.Get("Accept"); v != "" {
		req.Header.Set("Accept", v)
	} else if r.URL.Query().Get("stream") == "true" {
		req.Header.Set("Accept", "text/event-stream")
	}
	if up.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+up.APIKey)
	}
	client, err := s.clientFor(idx)
	if err != nil {
		s.writeError(w, registry.FormatOpenAIResponses, http.StatusBadGateway,
			&ir.Error{Type: "api_error", Message: "task upstream client unavailable: " + err.Error(), Source: "any2api"})
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		s.writeError(w, registry.FormatOpenAIResponses, http.StatusBadGateway,
			&ir.Error{Type: "api_error", Message: "task upstream request failed: " + err.Error(), Source: "any2api"})
		return
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	// S8（W36-2）：L2 被动证据——passthrough 往返的**结论性失败**（上游 404/405）
	// → 诊断计数 + ring + 日志（可选降级门控见 observeTaskFaceEvidence）。结论闸
	// 同 sub2api：网络失败/5xx/429 不下结论（此面不触）。纯观测，绝不改本次转发。
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
		s.observeTaskFaceEvidence(idx, resp.StatusCode, path, r.PathValue("id"))
	}
	if recordID && resp.StatusCode >= 200 && resp.StatusCode < 300 && !isSSEContentType(resp.Header.Get("Content-Type")) {
		rb, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
		if err != nil {
			s.writeError(w, registry.FormatOpenAIResponses, http.StatusBadGateway,
				&ir.Error{Type: "api_error", Message: "task upstream response read failed: " + err.Error(), Source: "any2api"})
			return
		}
		if len(rb) > maxBodyBytes {
			s.writeError(w, registry.FormatOpenAIResponses, http.StatusBadGateway,
				&ir.Error{Type: "api_error", Message: "task upstream response exceeds 32MB", Source: "any2api"})
			return
		}
		if id := responseIDOf(rb); id != "" {
			s.taskRoutes.record(id, idx)
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(rb)
		return
	}
	w.WriteHeader(resp.StatusCode)
	// S4（W36-2）：回访面（retrieve/cancel，非 create）非 SSE 2xx 响应——只读
	// 旁路观测终态 usage。tee 透传字节/时机零改动；仅在流尽后解析（客户端断开/
	// 超限不落账）。观测绝不进下游字节、绝不触上游（零额外调用）。
	if !recordID && resp.StatusCode >= 200 && resp.StatusCode < 300 && !isSSEContentType(resp.Header.Get("Content-Type")) {
		tee := &observeTee{src: resp.Body, limit: maxBodyBytes}
		streamCopyDownstream(w, tee)
		if !tee.over {
			s.observePassthroughUsage(r.PathValue("id"), idx, tee.buf)
		}
		return
	}
	streamCopyDownstream(w, resp.Body)
}

// isSSEContentType Content-Type 是否为 SSE（大小写不敏感子串判定）。
func isSSEContentType(ct string) bool {
	return strings.Contains(strings.ToLower(ct), "text/event-stream")
}

// responseIDOf 响应对象顶层字符串 id（非 JSON/无 id/非字符串 → ""）。
func responseIDOf(body []byte) string {
	var probe struct {
		ID json.RawMessage `json:"id"`
	}
	if json.Unmarshal(bytes.TrimSpace(body), &probe) != nil || len(probe.ID) == 0 {
		return ""
	}
	var id string
	if json.Unmarshal(probe.ID, &id) != nil {
		return ""
	}
	return strings.TrimSpace(id)
}

// streamCopyDownstream 上游响应体 → 下游字节级转发（逐读逐写 + Flush；读尽/出错
// 即止——SSE 面依赖 flush 逐块送达）。客户端断开 → 写失败 → 返回（上游请求随
// r.Context() 取消）。
func streamCopyDownstream(w http.ResponseWriter, src io.Reader) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		if err != nil {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// S4 回访 usage 只读旁路观测（W36-2）
// ---------------------------------------------------------------------------

// observeTee 只读旁路 tee（S4）：透传上游字节的同时累积一份**有界**副本供收尾
// 解析——读面字节/时机与不经 tee 逐字一致（纯包装，绝不改写/延迟下游字节）。
// 超限（> limit）= over 置位并弃缓冲（大响应不观测——防内存与半截 JSON 误读）。
type observeTee struct {
	src   io.Reader
	buf   []byte
	limit int
	over  bool
}

func (t *observeTee) Read(p []byte) (int, error) {
	n, err := t.src.Read(p)
	if n > 0 && !t.over {
		if len(t.buf)+n > t.limit {
			t.over = true
			t.buf = nil
		} else {
			t.buf = append(t.buf, p[:n]...)
		}
	}
	return n, err
}

// passthroughTerminalUsage 从透传回访响应体解析终态 usage 观测（S4）：仅当顶层
// status 为任务终态（completed/failed/incomplete/cancelled）且 usage 对象存在
// （非 null）时 ok=true；其余（运行中/无 usage/非 JSON/半截体）= 无观测不落账
// （「不下结论不落标」——绝不从异形体推断）。命名并集取 max 同 taskTerminalUsage。
func passthroughTerminalUsage(body []byte) (in, out int64, status, model string, ok bool) {
	var w struct {
		Status string        `json:"status"`
		Model  string        `json:"model"`
		Usage  *usageNumbers `json:"usage"`
	}
	if json.Unmarshal(bytes.TrimSpace(body), &w) != nil || w.Usage == nil {
		return 0, 0, "", "", false
	}
	switch w.Status {
	case taskCompleted, taskFailed, taskIncomplete, taskCancelled:
	default:
		return 0, 0, "", "", false
	}
	in, out = w.Usage.pair()
	return in, out, w.Status, w.Model, true
}

// observePassthroughUsage 回访响应体收尾观测（S4）：解析终态 usage + 路由条目
// 去重（每任务一条；markUsageObserved）→ recordPassthroughUsage。无观测/非首次
// = 静默（纯观测面）。
func (s *Server) observePassthroughUsage(taskID string, idx int, body []byte) {
	if taskID == "" {
		return
	}
	in, out, status, model, ok := passthroughTerminalUsage(body)
	if !ok || !s.taskRoutes.markUsageObserved(taskID) {
		return
	}
	s.recordPassthroughUsage(taskID, idx, status, model, in, out)
}

// recordPassthroughUsage 透传任务终态 usage 落账（S4；每任务一条，source=
// passthrough——与托管面 recordTaskUsage 日志行同前缀、以 source 字段区分）。有
// 观测（非零）→ 计数（PassthroughUsage*）；日志行恒写（含 0/0——下界纪律同托管
// 面：记录恒写、计数不记）。纯观测：绝不进下游字节、绝不进控制流。
func (s *Server) recordPassthroughUsage(taskID string, idx int, status, model string, in, out int64) {
	up := s.cfg.Upstreams[idx]
	if in != 0 || out != 0 {
		s.diag.passthroughUsageRecords.Add(1)
		s.diag.passthroughUsageIn.Add(in)
		s.diag.passthroughUsageOut.Add(out)
	}
	payload, err := json.Marshal(struct {
		Task     string `json:"task"`
		Source   string `json:"source"`
		Upstream string `json:"upstream"`
		Model    string `json:"model"`
		Status   string `json:"status"`
		In       int64  `json:"in"`
		Out      int64  `json:"out"`
	}{Task: taskID, Source: "passthrough", Upstream: up.Name, Model: model, Status: status, In: in, Out: out})
	if err != nil {
		return // 全标量载荷——Marshal 失败 = 编程错误；绝不写半截日志。
	}
	log.Printf("any2api: task %s terminal usage: %s", taskID, payload)
}

// ---------------------------------------------------------------------------
// S8 能力面 L2 被动证据（W36-2）
// ---------------------------------------------------------------------------

// observeTaskFaceEvidence L2 被动证据（S8；topic `service-task-surface` §③ L2
// 行原文）：passthrough 往返（create/回访/续流）的**结论性失败**——上游 404/405
// → 诊断计数（PassthroughFace404/405）+ ring 事件（task_face_evidence）+ 日志。
// 结论闸（同 sub2api）：网络失败/5xx/429 → 不下结论——本函数只在 404/405 时被
// 调用（调用点唯一：taskPassthroughRoundTrip 响应头后）。
//
// `task_face_degrade: auto`（Config.TaskFaceDegrade）时**首个**结论性失败即进程内
// 降级该上游 passthrough→managed（「上游能透传就透传，不能就托管」）：置位降级
// 标记（effectiveTaskFace 消费——后续 create 裁定走托管路径；既有路由表条目不受
// 影响——在飞任务仍在原上游）+ 降级计数 + 一条 task_face_degrade ring 事件 +
// 日志。只影响本进程、**重启清零**、绝不写回配置（L1 声明仍是配置期权威）。
func (s *Server) observeTaskFaceEvidence(idx, status int, path, taskID string) {
	up := s.cfg.Upstreams[idx]
	switch status {
	case http.StatusNotFound:
		s.diag.passthroughFace404.Add(1)
	case http.StatusMethodNotAllowed:
		s.diag.passthroughFace405.Add(1)
	}
	degraded := false
	if s.cfg.TaskFaceDegrade == taskFaceDegradeAuto {
		_, loaded := s.taskFaceDegraded.LoadOrStore(idx, true)
		degraded = !loaded // 首次置位 = 本次降级转换（转换只落一条 ring/日志）
	}
	payload, err := json.Marshal(struct {
		Upstream   string `json:"upstream"`
		Status     int    `json:"status"`
		Path       string `json:"path"`
		Task       string `json:"task,omitempty"`
		DegradeNow bool   `json:"degrade_now"`
	}{Upstream: up.Name, Status: status, Path: path, Task: taskID, DegradeNow: degraded})
	if err == nil {
		s.adminLog.add(adminLogTaskFaceEvidence, up.Name, taskID, string(payload), status)
	}
	log.Printf("any2api: upstream %s task_face passthrough conclusive failure (status=%d path=%s task=%q degrade_now=%v)",
		up.Name, status, path, taskID, degraded)
	if degraded {
		s.diag.passthroughFaceDegrades.Add(1)
		dpayload, derr := json.Marshal(struct {
			Upstream string `json:"upstream"`
			From     string `json:"from"`
			To       string `json:"to"`
		}{Upstream: up.Name, From: taskFacePassthrough, To: taskFaceManaged})
		if derr == nil {
			s.adminLog.add(adminLogTaskFaceDegrade, up.Name, taskID, string(dpayload), status)
		}
		log.Printf("any2api: upstream %s task_face degraded passthrough->managed (in-process; restart clears)", up.Name)
	}
}
