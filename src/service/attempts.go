package service

import (
	"encoding/json"
	"log"
	"time"

	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// per-request attempt 记录（W5-19 条目 4；billing-paths `usage.Record` 范式）
// ---------------------------------------------------------------------------
//
// P27 billing-paths 的范式（`usage.Record`：**每请求一组 attempt 记录**，含
// RequestID / 上游 / TTFT / Failed / 用量明细；W5-18 只有进程级累计计数）在本仓
// 落地为：
//
//   - 每个入口请求一个 requestRecorder（serve 建；请求内单 goroutine 写——
//     forward/relayStream 的收尾都在请求 goroutine 上，无并发）；
//   - 每次上游尝试一条 attemptRecord（**任何结局都记**：成功 / 上游失败 /
//     下游断开——W5-18 的 usage 面只在有 usage 观测时记，本面恒记）；
//   - 请求收尾（serve 的 defer）打一行 JSON 日志，`attempts` 数组即 per-request
//     记录列表；req id 与响应 request-id 头的回退值同源（日志 ↔ 响应头可关联）。
//
// 纯观测：记录只进日志，绝不进下游可见流，也不参与控制流。
//
// **下游断开面**：下游断开（写失败 / 心跳探测 / 父 ctx 取消）的尝试标
// `disconnected` 且**不置** `failed`（不是上游健康信号——与 recordFailure 同
// 纪律）；用量观测为断点前的下界。
//
// **池凭据索引**（W54-3 残余 2）：claude_web 尝试经 `credentialIndex` 记本尝试
// completion 使用的池索引（forward 从 prepare 的 referer 标记回填）——只记
// 索引，零凭据字节；无池/未回填省略 wire 字段（绝不伪造 0）。

// ---------------------------------------------------------------------------
// 尝试结束原因枚举 + 交付位（W36-1 S1/S2；纯日志面增量）
// ---------------------------------------------------------------------------
//
// 出处：`research/briefs/reports/A2A-L19-W35-5.md` §2 S1/S2（记账口径收口，缓解
// 缺口 A6/A7/A8）。两字段**只进日志**（对账用），绝不进控制流、绝不进下游字节。
//
// end_reason 值域（六值；归因优先级 = 先命中者定）：
//
//	completed             正常结束（上游终态到达 / 非流式响应体收全 / 收尾写出）；
//	upstream_error        上游侧失败（C1 换上游类 / 网络 / 读失败 / 转换失败 /
//	                      提交后流内失败 / 截断 / 无终止证据 EOF）；
//	upstream_idle         上游内容静默（idle 守卫触发）——upstream_error 的窄化
//	                      子类（A7：idle 码此前只在流内错误帧与任务 error 对象）；
//	downstream_disconnect 下游连接级断裂（写失败 / 心跳探测失败）；
//	client_cancel         下游请求 ctx 取消且未伴随连接级断裂（客户端中止）；
//	task_cancel           任务面显式取消（cancel 端点 → 任务 ctx 取消 → 在飞上游
//	                      中止；A8：此前与 downstream_disconnect 不可分）。
//
// **未达上游的本地失败省略该字段**（请求不可转换 400 / 路径构造失败 / 出站请求
// 构造失败 / 出站客户端构建失败）：六值无一成立（非上游、非下游、非取消），且
// 仓内既有纪律同样不把这些记 `failed`（`server.go:824,843,943,953` 只在真上游侧
// 失败置位）——绝不伪造枚举值。
//
// delivered 口径（对账：终态产出是否到达下游）：
//
//	同步面（非任务请求）  终止产出完整写出下游 = true（流式 = 收尾写出成功；
//	                      非流式 = 响应体写出无错）；错误信封/断开/截断 = false。
//	任务面（dr.task != nil）恒 false——attempt 行在任务终态落定**之前**写出，
//	                      客户端 retrieve/续流只能在其后发生（时序不可能）；交付
//	                      口径落在交付点（task.go `markDelivered`：per-task 一行
//	                      `terminal delivered` + `taskEntry.delivered` 位）。
const (
	endReasonCompleted            = "completed"
	endReasonUpstreamError        = "upstream_error"
	endReasonUpstreamIdle         = "upstream_idle"
	endReasonDownstreamDisconnect = "downstream_disconnect"
	endReasonClientCancel         = "client_cancel"
	endReasonTaskCancel           = "task_cancel"
)

// attemptRecord 一次上游尝试的记账（尝试进行中可变字段逐个填充，收尾冻结）。
type attemptRecord struct {
	attempt  int
	upstream string
	protocol string
	// credentialIndex 本尝试 completion 使用的池凭据索引（W54-3 残余 2；-1 =
	// 无池/未回填——wire 省略）。由 forward 从 prepare 的 referer 标记回填。
	// **只记索引，绝不落凭据字节**（与池记账同纪律）。
	credentialIndex int
	start           time.Time
	// end 本尝试收尾时刻（P1-3 turn 落账：`total_ms = end - start`；在
	// requestRecorder.finish 置时——绝不用 recordTurn 时刻，否则会把后续
	// attempt/排队时间算进前一次尝试）。
	end          time.Time
	ttft         time.Duration // 首个下游可见产出（流式 = 提交点；非流式 = 响应头到达；未产出 = 0）
	failed       bool          // 上游侧失败（C1 换上游类 / 网络 / 读失败 / 转换失败）
	disconnected bool          // 下游断开（写失败 / 心跳探测 / 父 ctx 取消）
	status       int           // 上游 HTTP 状态（0 = 未到达响应头）
	in, out      int64         // usage 观测（0 = 无观测）
	// endReason 结束原因（W36-1 S1；见上方枚举；"" = 未达上游的本地失败）。
	endReason string
	// delivered 终态产出是否完整写出下游（W36-1 S2；任务面恒 false——见上方口径）。
	delivered bool
}

// requestRecorder 一个请求的 attempt 记录器。nil 接收者安全——测试直接构造
// downstreamRequest（不设 rec）的路径不记账、不 panic。
//
// task（Slice 4）：任务面归因——任务 goroutine 的 serve 请求带任务 id（background
// 任务的 per-attempt 记录据此与任务 join：同 id 的任务记账行 + 请求记账行）；非
// 任务请求为空（wire 省略）。
type requestRecorder struct {
	reqID  string
	entry  string
	model  string
	stream bool
	task   string
	// sid / keyID 会话审计面（P0-6；W58-1）：sid = **前缀**（8 字符——设计
	// §1.1「sid 不进日志，只前缀显示」），keyID = gated 准入 key 的 sha8。
	// 从 session ctx 取（serve 回填）；public.enabled=false / 无会话 = 空
	// （wire 省略）。**零凭据字节**（绝不记 cookie / key 原文）。
	sid   string
	keyID string
	recs  []*attemptRecord
}

func newRequestRecorder(reqID string, entry registry.Format, model string, stream bool, task string) *requestRecorder {
	return &requestRecorder{reqID: reqID, entry: string(entry), model: model, stream: stream, task: task}
}

// begin 开始一次尝试的记录（nil 接收者也返回非 nil 记录——调用方写字段安全）。
func (r *requestRecorder) begin(attempt int, up Upstream) *attemptRecord {
	return &attemptRecord{
		attempt:         attempt,
		upstream:        up.Name,
		protocol:        string(up.Protocol),
		credentialIndex: -1, // 无池/未回填（forward 从 prepare 标记回填——W54-3）。
		start:           time.Now(),
	}
}

// finish 收尾一次尝试：追加到请求记录列表（nil 安全）。
func (r *requestRecorder) finish(ar *attemptRecord) {
	if r == nil || ar == nil {
		return
	}
	// P1-3 turn 落账：本尝试的收尾时刻（total_ms 只算本尝试，不含后续换台/
	// 排队时间）。
	ar.end = time.Now()
	r.recs = append(r.recs, ar)
}

// auditOutcome 请求级结局（P1-2 审计行 outcome 字段）：取**末次尝试**的
// end_reason 派生值（与 attempts 日志行的派生规则同源——绝不发明第二套词表）。
// 无尝试（未达上游的本地失败）= ""（审计行 outcome 留空，绝不伪造枚举值）。
func (r *requestRecorder) auditOutcome() string {
	if r == nil || len(r.recs) == 0 {
		return ""
	}
	ar := r.recs[len(r.recs)-1]
	if ar.endReason != "" {
		return ar.endReason
	}
	switch {
	case ar.failed:
		return endReasonUpstreamError
	case ar.disconnected:
		return endReasonDownstreamDisconnect
	case ar.ttft > 0:
		return endReasonCompleted
	}
	return ""
}

// log 请求收尾：有记录则打一行 JSON（无记录 = 未发起任何尝试，静默）。
func (r *requestRecorder) log() {
	if r == nil || len(r.recs) == 0 {
		return
	}
	type attemptWire struct {
		N        int    `json:"n"`
		Upstream string `json:"upstream"`
		Proto    string `json:"proto"`
		// CredentialIndex 池凭据索引（W54-3 残余 2；省略 = 无池/未回填——**绝不
		// 伪造 0**；只记索引，零凭据字节）。
		CredentialIndex *int `json:"credential_index,omitempty"`
		Status          int  `json:"status,omitempty"`
		Failed          bool `json:"failed"`
		Disconnected    bool `json:"disconnected,omitempty"`
		// EndReason 结束原因枚举（W36-1 S1；六值见文件头枚举；省略 = 未达上游的
		// 本地失败——绝不伪造值）。
		EndReason string `json:"end_reason,omitempty"`
		// Delivered 终态产出是否完整写出下游（W36-1 S2；任务面恒 false——交付口径
		// 在交付点，见 task.go markDelivered）。
		Delivered bool  `json:"delivered"`
		In        int64 `json:"in"`
		Out       int64 `json:"out"`
		TTFTMs    int64 `json:"ttft_ms"`
		TotalMs   int64 `json:"total_ms"`
	}
	wire := struct {
		Req    string `json:"req"`
		Entry  string `json:"entry"`
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
		Task   string `json:"task,omitempty"`
		// Sid / KeyID 会话审计面（P0-6；W58-1）：sid = 前缀（8 字符，设计 §1.1
		// 只前缀显示）；key_id = gated 准入 key 的 sha8。无会话/未启用省略——
		// **零凭据字节**（不含 cookie / key 原文）。
		Sid      string        `json:"sid,omitempty"`
		KeyID    string        `json:"key_id,omitempty"`
		Attempts []attemptWire `json:"attempts"`
	}{Req: r.reqID, Entry: r.entry, Model: r.model, Stream: r.stream, Task: r.task, Sid: r.sid, KeyID: r.keyID}
	now := time.Now()
	for _, ar := range r.recs {
		// 兜底派生（W36-1 S1）：显式归因点（relay 的断连/idle/完成、非流式成功）
		// 之外的路径——上游侧失败统一 upstream_error；断连类 downstream_disconnect；
		// 已产出（ttft>0）completed；其余保持空（未达上游的本地失败）。绝不改写
		// 显式值。
		reason := ar.endReason
		if reason == "" {
			switch {
			case ar.failed:
				reason = endReasonUpstreamError
			case ar.disconnected:
				reason = endReasonDownstreamDisconnect
			case ar.ttft > 0:
				reason = endReasonCompleted
			}
		}
		var credIdx *int
		if ar.credentialIndex >= 0 {
			ci := ar.credentialIndex
			credIdx = &ci
		}
		wire.Attempts = append(wire.Attempts, attemptWire{
			N:               ar.attempt,
			Upstream:        ar.upstream,
			Proto:           ar.protocol,
			CredentialIndex: credIdx,
			Status:          ar.status,
			Failed:          ar.failed,
			Disconnected:    ar.disconnected,
			EndReason:       reason,
			Delivered:       ar.delivered,
			In:              ar.in,
			Out:             ar.out,
			TTFTMs:          ar.ttft.Milliseconds(),
			TotalMs:         now.Sub(ar.start).Milliseconds(),
		})
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return // 全标量载荷——Marshal 失败 = 编程错误；绝不写半截日志。
	}
	log.Printf("any2api: request %s attempts: %s", r.reqID, payload)
}

// finishAttempt 收尾一次尝试的记账（W5-18 计数/日志 + W5-19 per-request 记录）：
//
//   - usage 计数（AttemptUsage*）：零观测（无 usage 字段）不记——口径同 W5-18；
//   - 一行 usage 日志（有观测时；字面形态与 W5-18 一致）；
//   - per-request 记录：**恒记**（任何结局；ar 由 forward 的 defer 追加）。
//
// failed = 上游侧失败（下游断开由调用方在 ar 上置位并压住 failed——本函数据此
// 不把下游断开记成上游失败）。
func (s *Server) finishAttempt(ar *attemptRecord, u partialUsage, failed bool) {
	if ar == nil {
		return
	}
	if ar.disconnected {
		failed = false // 下游断开不是上游健康信号（与 recordFailure 同纪律）。
	}
	ar.failed = failed
	ar.in, ar.out = u.input, u.output
	if u.zero() {
		return
	}
	s.diag.attemptUsageRecords.Add(1)
	s.diag.attemptUsageIn.Add(u.input)
	s.diag.attemptUsageOut.Add(u.output)
	if failed {
		s.diag.attemptUsageFailRecs.Add(1)
		s.diag.attemptUsageFailIn.Add(u.input)
		s.diag.attemptUsageFailOut.Add(u.output)
	}
	log.Printf("any2api: upstream %s attempt %d usage observed (failed=%v): in=%d out=%d (diagnostics only; never merged downstream)",
		ar.upstream, ar.attempt, failed, u.input, u.output)
}

// ---------------------------------------------------------------------------
// per-task 终态 usage 记账（Slice 4；任务面）
// ---------------------------------------------------------------------------
//
// 提案 §记账点（`service-task-surface` §形态 B/C）：任务终态 usage **恒落账**——
// 网关自己跑到终态（托管语义），usage 不依赖客户端是否来取；failed/cancelled 无
// 终态 usage，其下界在既有 per-attempt 面（AttemptUsage*，relay 全窗口观测）。
//
// 归因（background 任务如何进记账）：
//   - 任务 goroutine 复用 serve 跑上游 → 同一 per-request attempt 记账面，请求行
//     带 `task` 字段（requestRecorder.task）——attempts 数组按任务 id 可 join；
//   - 任务终态再落一条 per-task 记录（本段）：{task, entry, model, status, in, out}
//     ——终态 usage 的权威值（与轮询快照逐值同源 = 快照 usage 字段）；
//   - 两面关系：per-attempt = 尝试级观测（失败任务的下界也在此），per-task =
//     任务级终态值。零观测（无 usage 字段）不计数——同 AttemptUsage* 纪律。

// taskTerminalUsage 从任务终态快照抽取 usage（响应对象顶层 `usage`；命名并集取
// max，同 observePartialUsage 的 usageNumbers）。无/解析失败 → (0,0)。
func taskTerminalUsage(snap json.RawMessage) (int64, int64) {
	var w struct {
		Usage *usageNumbers `json:"usage"`
	}
	if json.Unmarshal(snap, &w) != nil || w.Usage == nil {
		return 0, 0
	}
	return w.Usage.pair()
}

// recordTaskUsage 任务终态 usage 落账（每任务一条；runTask 落终态后调用）。
// 有观测（in/out 非零）→ 计数（TaskUsage*）；日志行恒写（status + 观测值——
// failed/cancelled 为 0/0，下界见 per-attempt 面）。
func (s *Server) recordTaskUsage(taskID string, entry registry.Format, model, status string, snap json.RawMessage) {
	in, out := taskTerminalUsage(snap)
	if in != 0 || out != 0 {
		s.diag.taskUsageRecords.Add(1)
		s.diag.taskUsageIn.Add(in)
		s.diag.taskUsageOut.Add(out)
	}
	payload, err := json.Marshal(struct {
		Task   string `json:"task"`
		Entry  string `json:"entry"`
		Model  string `json:"model"`
		Status string `json:"status"`
		In     int64  `json:"in"`
		Out    int64  `json:"out"`
	}{Task: taskID, Entry: string(entry), Model: model, Status: status, In: in, Out: out})
	if err != nil {
		return // 全标量载荷——Marshal 失败 = 编程错误；绝不写半截日志。
	}
	log.Printf("any2api: task %s terminal usage: %s", taskID, payload)
}

// ---------------------------------------------------------------------------
// dashboard P0-0 请求 ring 写入 hook（A2A-L19-W71-1）
// ---------------------------------------------------------------------------

// recordRequestRing 请求收尾（serve 的 defer）把本请求的记账投影为 dashboard
// 请求 ring 的一行。**只写脱敏字段**（转换口径见 admin_dashboard.go
// `adminRequestRowFrom`：入口/模型/流位/状态/尝试/换台/上游序/首字节/总量/用量/
// 结局/sid 前缀/key_id/task/attempt 时间线——零 cookie / 零 access key / 零上游
// 错误原文 / 零原始 URL）。nil 安全；纯观测，绝不进控制流。
//
// dashboard P0-1 指标面（A2A-L19-W72-1）复用**同一收尾点**：把本请求投影为一次
// 观测（requests/errors/failovers + TTFT/总量直方图，口径见 admin_metrics.go
// `adminMetricsObservationFrom`）——同一 `now`，两条观测面（请求 ring 行 ↔ 分钟桶）
// 时刻一致；纯观测，绝不进控制流、绝不影响上面的日志行。
func (s *Server) recordRequestRing(rec *requestRecorder) {
	if s == nil || rec == nil {
		return
	}
	now := time.Now()
	s.adminRequests.add(adminRequestRowFrom(rec, now))
	s.observeRequest(rec, now)
}
