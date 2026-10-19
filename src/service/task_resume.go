// 任务面 Slice 3（A2A-L19-W19-2）：断线续流——事件缓冲 + `starting_after` 重放/接续。
//
// 面（提案 `service-task-surface` §实施拆分 Slice 3 原文 + §决策矩阵 #4 形态 B）：
//
//	GET /v1/responses/{id}?stream=true&starting_after=N → 任务事件流：
//	  ① 重放：缓冲中 seq ≥ N+1 的事件按序写出（无 N → 从 0 起）；
//	  ② 接续实时：重放后继续推送新事件，直到任务终态（缓冲封口）；
//	  ③ 过期游标（N+1 早于缓冲下界——请求区间部分已被驱逐）→ **快照降级**：
//	     返回普通 GET 的响应（200 + JSON 快照，字节一致；对齐 Perplexity
//	     「窗口过期 → 普通 GET」模式——本实现对客户端零二次请求：降级响应即
//	     普通 GET 响应）；
//	  ④ 无 stream=true → 普通 GET 快照（Slice 1 面，字节零改动）。
//
// 编号纪律（提案 §陷阱表「续流缓冲与实时双写竞态」行）：sequence_number 由网关
// **自编**——从 0 起严格递增、绝不复用（驱逐只推进下界，编号空间不回退）；单一
// 写者 = 任务 goroutine（经 taskStreamWriter.scanBlock 入缓冲）；每条事件的 data
// 载荷 sequence_number 在入缓冲时改写为网关号（上游原编号——缺失/跳号/重启——
// 不进客户端可见面；starting_after 游标语义因此恒有定义）。多订阅者各自持游标
// 读取，互不消费（重放与实时同源 = 无缺无重）。
//
// 有界（提案 §决策矩阵 #15 / §陷阱表「内存 store 无界」）：单任务缓冲双上限
// （事件数 taskMaxEvents + 字节 taskMaxEventBytes），超限驱逐最老 → 下界推进；
// 订阅者落后于窗口（写端慢于驱逐）→ 关流（无缺交付不可得；重连命中快照降级）。
//
// 心跳（提案 §红线 2）：续流的下游心跳沿用 15s 缺省（s.hb），空转间隔 ≥ hb 时
// 写 `: keepalive`（与 relay 的提交后心跳同帧形态）。
//
// 边界（有意；见报告 §边界）：
//   - 快照降级响应 = 普通 GET 响应（同字节）；与 Perplexity 的「400 + 客户端
//     回退 GET」两跳形态不同——对齐的是降级**终点**（窗口过期 → 普通 GET 的
//     响应），本实现对客户端零二次请求。
//   - 无上游终态事件的任务（提交前失败/取消）→ 续流以已交付事件收尾，不合成
//     终止帧（终态以 GET 快照为准；提案失败表承诺的流内 response.failed 只覆盖
//     「运行中失败/截断/挂住」行——那条经 relay 错误帧真实入缓冲）。
//   - 游标指向未来（N ≥ next）→ 等待（运行中）/ 空流收尾（已终态），不报错。
//   - 任务 TTL 过期（墓碑）→ 404（get 前置，与轮询面同双 code）。
package service

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/registry"
)

// 单任务事件缓冲上限（有界内存；超出驱逐最老，下界推进 → 过期游标走快照降级）。
// 双门：事件数（小帧任务）与字节预算（大帧任务）各自约束。
const (
	taskMaxEvents     = 1024
	taskMaxEventBytes = 1 << 20
)

// taskEvent 一条缓冲事件：seq = 网关自编序号（0 起严格递增、绝不复用）；frame =
// 完整 SSE 帧（含帧尾空行；data 载荷的 sequence_number 已改写为 seq）。
type taskEvent struct {
	seq   int64
	frame []byte
}

// taskEventLog 任务事件有界缓冲。单一写者 = 任务 goroutine（taskStreamWriter.
// scanBlock）；多读者 = 续流端点订阅者（各自持游标，互不消费）。驱逐只从头部
// （推进下界），编号空间单调不复用——「start 早于下界」= 过期游标（请求区间
// 已被部分驱逐，无法无缺交付）→ 快照降级。
type taskEventLog struct {
	mu     sync.Mutex
	events []taskEvent // 连续区间 [lower, lower+len)
	lower  int64       // 缓冲下界（首个保留事件的 seq）
	next   int64       // 下一个待分配 seq（累计事件数；驱逐不改变）
	bytes  int         // 保留帧总字节（驱逐记账）
	closed bool        // 任务终态落定：不再有新事件
	// termSeq 终态事件（response.completed/incomplete/failed）的 seq（W36-1 S2；
	// -1 = 未出现）。续流交付判据用：订阅者游标 ≤ termSeq 且 termSeq 仍在保留
	// 窗口内 = 该订阅者**收到过**终态事件（task.go markDelivered 的 resume 面）。
	termSeq int64
	// notify 广播：每次 append/close 关闭并换新（订阅者持旧通道 = 等下一批）。
	notify chan struct{}
	// 容量（有界；测试可注入——taskStore.taskLogCaps）。
	maxEvents int
	maxBytes  int
}

func newTaskEventLog() *taskEventLog {
	return &taskEventLog{
		notify:    make(chan struct{}),
		maxEvents: taskMaxEvents,
		maxBytes:  taskMaxEventBytes,
		termSeq:   -1,
	}
}

// appendEvent 缓冲一条事件（单一写者：任务 goroutine）。分配 seq（单调不复用）→
// 改写载荷 sequence_number → 入缓冲 → 超限驱逐最老（下界推进）→ 广播订阅者。
// 返回分配的 seq。
func (l *taskEventLog) appendEvent(block, payload []byte) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	seq := l.next
	l.next++
	if l.termSeq < 0 {
		// S2（W36-1）：终态事件登记（首见即记——first-terminal-wins 与
		// taskStreamWriter.scanBlock 的终态判据同集合；event 行缺失时回退载荷
		// 顶层 type）。
		name := blockEventName(block)
		if name == "" {
			name = payloadEventType(payload)
		}
		if isTerminalEventName(name) {
			l.termSeq = seq
		}
	}
	frame := rewriteEventSeqFrame(block, payload, seq)
	buf := make([]byte, 0, len(frame)+2)
	buf = append(buf, frame...)
	buf = append(buf, '\n', '\n')
	l.events = append(l.events, taskEvent{seq: seq, frame: buf})
	l.bytes += len(buf)
	for len(l.events) > l.maxEvents || (l.bytes > l.maxBytes && len(l.events) > 1) {
		l.bytes -= len(l.events[0].frame)
		l.events[0] = taskEvent{} // 清引用（底层数组防滞留）
		l.events = l.events[1:]
		l.lower++
		// 头指针推进后的定期搬家：防底层数组随驱逐无界（摊销 O(1)）。
		if cap(l.events) > 2*len(l.events)+16 {
			l.events = append([]taskEvent(nil), l.events...)
		}
	}
	l.notifyLocked()
	return seq
}

// snapshotFrom 订阅者首取（续流端点）：返回 [start, next) 的保留事件与任务是否
// 已终态（done）。ok=false = 过期游标（start 早于缓冲下界——请求区间部分已被
// 驱逐，无法无缺交付）→ 调用方走快照降级。与后续 liveBatch 之间无丢失唤醒：
// 期间追加的事件已入缓冲，liveBatch 的取数覆盖（订阅通道在 liveBatch 内取）。
func (l *taskEventLog) snapshotFrom(start int64) (events []taskEvent, done, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if start < l.lower {
		return nil, false, false
	}
	for _, ev := range l.events {
		if ev.seq >= start {
			events = append(events, ev)
		}
	}
	return events, l.closed, true
}

// liveBatch 订阅者增量取数：返回 seq ≥ pos 的保留事件、当前广播通道、任务已终态
// 且无剩余（done）；behind=true = 订阅者游标落后于缓冲下界（事件已被驱逐）——
// 调用方应关流（重连 → 快照降级）。
func (l *taskEventLog) liveBatch(pos int64) (events []taskEvent, notify chan struct{}, done, behind bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if pos < l.lower {
		return nil, nil, false, true
	}
	for _, ev := range l.events {
		if ev.seq >= pos {
			events = append(events, ev)
		}
	}
	return events, l.notify, l.closed && pos >= l.next, false
}

// closeLog 任务终态落定：封口（不再有新事件）+ 广播（订阅者收尾）。幂等。
func (l *taskEventLog) closeLog() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	l.notifyLocked()
}

// notifyLocked 广播（mu 保护）：关闭当前通道并换新。
func (l *taskEventLog) notifyLocked() {
	close(l.notify)
	l.notify = make(chan struct{})
}

// setCaps 测试注入：调整容量（并发安全；下限钳制）。
func (l *taskEventLog) setCaps(maxEvents, maxBytes int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if maxEvents < 1 {
		maxEvents = 1
	}
	l.maxEvents, l.maxBytes = maxEvents, maxBytes
}

// isTerminalEventName 事件名是否终态事件（W36-1 S2；集合 = taskStreamWriter.
// scanBlock 的终态判据：response.completed/incomplete/failed）。
func isTerminalEventName(name string) bool {
	switch name {
	case "response.completed", "response.incomplete", "response.failed":
		return true
	}
	return false
}

// payloadEventType 载荷顶层 `type` 字段（event: 行缺失的代理面回退；非对象/无
// 该字段/解析失败 → ""）。
func payloadEventType(payload []byte) string {
	var probe struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(payload, &probe) != nil {
		return ""
	}
	return probe.Type
}

// deliveredTerminalFrom 订阅者从 start（含）起重放是否**收到**终态事件（W36-1
// S2 任务面 resume 判据）：终态事件在保留窗口内（未被驱逐——termSeq ≥ lower）
// 且订阅者游标未越过它（termSeq ≥ start）。与「缓冲已封口且已排空」（done）合取
// = 交付；无终态事件（提交前失败/取消任务——续流以已交付事件收尾，终态只在
// 快照面）恒 false（那条路径的交付走 retrieve）。
func (l *taskEventLog) deliveredTerminalFrom(start int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.termSeq >= 0 && l.termSeq >= l.lower && l.termSeq >= start
}

// ---------------------------------------------------------------------------
// 载荷 sequence_number 改写（字节级手术，其余字节逐字保留）
// ---------------------------------------------------------------------------

// setTopLevelSeq 把事件载荷（顶层 JSON 对象）的 `sequence_number` 值改写为 seq
// （全部出现位置；缺省则插入）。其余字节逐字保留（同 forceTopLevelFieldTrue 的
// 字节纪律）。非对象/解析失败 → 原样返回。
func setTopLevelSeq(payload []byte, seq int64) []byte {
	val := strconv.FormatInt(seq, 10)
	body := payload
	for {
		out, found, rewrote := rewriteFirstSeqField(body, val)
		if rewrote {
			body = out
			continue
		}
		if found {
			return body
		}
		return insertTopLevelRawField(body, "sequence_number", val)
	}
}

// rewriteFirstSeqField 把第一个值 ≠ val 的顶层 `sequence_number` 值改写为 val。
// 返回 (新体, 字段存在, 是否改写)。非对象/解析失败 → (原体, false, false)。
func rewriteFirstSeqField(body []byte, val string) ([]byte, bool, bool) {
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
		if k != "sequence_number" {
			continue
		}
		found = true
		if string(bytes.TrimSpace(raw)) == val {
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
		out := make([]byte, 0, len(body)-len(raw)+len(val))
		out = append(out, body[:i]...)
		out = append(out, val...)
		out = append(out, body[valueEnd:]...)
		return out, found, true
	}
	return body, found, false
}

// insertTopLevelRawField 在顶层对象闭合 `}` 前插入 `"<key>":<val>`（空对象不加
// 逗号）。非对象 → 原样返回。
func insertTopLevelRawField(body []byte, key, val string) []byte {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return body
	}
	closeIdx := bytes.LastIndexByte(body, '}')
	if closeIdx < 0 {
		return body
	}
	insert := []byte(`"` + key + `":` + val)
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

// rewriteEventSeqFrame 把 SSE 块内 data 载荷的 sequence_number 改写为 seq；返回
// 新块字节（不含帧尾空行——调用方补 "\n\n"）。
//
// 单行 data（常态）：载荷字节在块内连续出现——原位替换（其余字节逐字保留）。
// 多行 data（病理，载荷含裸换行）：无法原位映射——重建块（event 名 + 逐行 data）。
// 块内找不到载荷（防御）→ 原样返回（仍缓冲原块）。
func rewriteEventSeqFrame(block, payload []byte, seq int64) []byte {
	np := setTopLevelSeq(payload, seq)
	if bytes.Equal(np, payload) {
		return block
	}
	if i := bytes.Index(block, payload); i >= 0 {
		out := make([]byte, 0, len(block)-len(payload)+len(np))
		out = append(out, block[:i]...)
		out = append(out, np...)
		out = append(out, block[i+len(payload):]...)
		return out
	}
	var b bytes.Buffer
	if ev := blockEventName(block); ev != "" {
		b.WriteString("event: ")
		b.WriteString(ev)
		b.WriteByte('\n')
	}
	for _, line := range bytes.Split(np, []byte("\n")) {
		b.WriteString("data: ")
		b.Write(line)
		b.WriteByte('\n')
	}
	return b.Bytes()
}

// blockEventName 取 SSE 块的首个 `event:` 行值（无 → ""）。
func blockEventName(block []byte) string {
	for _, line := range bytes.Split(block, []byte("\n")) {
		line = bytes.TrimRight(line, "\r")
		if v, ok := bytes.CutPrefix(line, []byte("event:")); ok {
			return string(bytes.TrimSpace(v))
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// 续流端点
// ---------------------------------------------------------------------------

// handleTaskResume 续流面（Slice 3）：GET /v1/responses/{id}?stream=true&starting_after=N。
//
// 语义（文件头注）：重放 [start, next) → 接续实时 → 终态收尾；过期游标（start
// 早于缓冲下界，含「无游标但首部已驱逐」）→ 快照降级（普通 GET 响应，字节一致）；
// 畸形游标 → 400。未知/过期 id 的 404 在 handleTaskRetrieve 前置完成。
func (s *Server) handleTaskResume(w http.ResponseWriter, r *http.Request, te *taskEntry) {
	cursor := int64(-1)
	if v := r.URL.Query().Get("starting_after"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			s.writeError(w, registry.FormatOpenAIResponses, http.StatusBadRequest,
				&ir.Error{Type: "invalid_request_error", Code: "invalid_starting_after",
					Message: "starting_after must be a non-negative integer: " + v, Source: "any2api"})
			return
		}
		cursor = n
	}
	start := cursor + 1

	events, done, ok := te.log.snapshotFrom(start)
	if !ok {
		// 快照降级（过期游标）：响应 = 普通 GET 响应（同字节；对齐 Perplexity
		// 「窗口过期 → 普通 GET」模式的降级终点）。
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// S2（W36-1）：降级响应 = 普通 GET（终态快照）——写出无错即交付（retrieve
		// 面；markDelivered 内部按终态判定）。
		if _, werr := w.Write(te.snapshotFor()); werr == nil {
			te.markDelivered("retrieve")
		}
		return
	}

	// 提交响应头（续流 = 纯读面：任务已建立，无上游可失败——立即提交 + flush，
	// 客户端即刻拿到开流信号；空转由心跳兜底）。
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	flush := func() {
		if flusher != nil {
			flusher.Flush()
		}
	}
	last := time.Now()
	flush()
	writeFrame := func(f []byte) bool {
		if _, err := w.Write(f); err != nil {
			return false
		}
		last = time.Now()
		flush()
		return true
	}
	pos := start
	for _, ev := range events {
		if !writeFrame(ev.frame) {
			return
		}
		pos = ev.seq + 1
	}
	if done {
		// S2（W36-1）：重放即排空（缓冲已封口）——订阅者收到过终态事件 = 交付
		// （写帧全部成功才会走到这里；判据见 deliveredTerminalFrom）。
		if te.log.deliveredTerminalFrom(start) {
			te.markDelivered("resume")
		}
		return
	}
	tick := time.NewTicker(s.hb)
	defer tick.Stop()
	for {
		events, notify, done, behind := te.log.liveBatch(pos)
		if behind {
			return // 落后于窗口：关流（重连 → 快照降级）
		}
		for _, ev := range events {
			if !writeFrame(ev.frame) {
				return
			}
			pos = ev.seq + 1
		}
		if done {
			// S2（W36-1）：实时段收到终态事件 = 交付（同上）。
			if te.log.deliveredTerminalFrom(start) {
				te.markDelivered("resume")
			}
			return
		}
		select {
		case <-notify:
		case <-r.Context().Done():
			return
		case <-tick.C:
			if time.Since(last) >= s.hb {
				if !writeFrame([]byte(": keepalive\n\n")) {
					return
				}
			}
		}
	}
}
