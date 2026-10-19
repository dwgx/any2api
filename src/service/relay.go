package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/pairs"
	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// 下游写入器（提交点 + 心跳 + 并发保护）
// ---------------------------------------------------------------------------

// downstream 下游 SSE 写入器。提交点纪律（new-api stream_scanner 范式）：
// 响应头在首个有效产出（commit）时才写出——未提交前上游失败可换号重试；
// 提交后失败只写错误帧，绝不重试（避免下游重复内容）。
// 心跳由独立 goroutine 按间隔在**提交后**上游静默时补保活帧（W5-27 提交点门：
// 提交前不发——提交前写字节会隐式提交响应，见 beat）；写互斥保护 ResponseWriter。
//
// 决策（W5-15）：**不做 TTFT 提前开流**（LiteLLM `open_sse_before_first_byte` /
// `sse_keepalive_ping_interval_seconds` 的「首字节前保活」；P27 §对照表点名）。
// 理由：① 代价 = 提交点前移——首 ping 之后一切失败只能走流内错误帧，真实状态码
// （429/5xx）与换上游全部失效；② 收益面不成立：客户端（CC）预算 600s
// （cc-wire-contract A8），且 P24 实测 ping 不重置其看门狗、提前开流反而让
// 看门狗更早起算；③ 上游静默面已由既有心跳（提交后）覆盖，上游响应头等待由
// ResponseHeaderTimeout（60s）+ 换上游覆盖；④ LiteLLM 同款默认关（issue #28384）。
// 若未来要做：按 `sse_keepalive.py` 的帧边界插入 + `seal_open_sse_frame` 规则实现，
// 并显式接受上述代价。
//
// 替代面已落地（W5-16）：`first_byte_timeout_seconds`（缺省关）——「上游挂住不产出」
// 给一个出口，走**预提交**路径（取消本尝试 → 按 C1 换上游），提交点纪律与真实状态码
// 全部保留；见 firstByteGuard。
type downstream struct {
	mu    sync.Mutex
	w     http.ResponseWriter
	flush func()
	entry registry.Format

	// jsonArray 下游为 gemini 的**非 alt=sse** JSON 数组形态（W5-19 条目 2）：
	// 提交写 `[`、元素以 `,` 分隔、收尾写 `]`；不写 SSE 心跳/错误帧（它们不是
	// 合法 JSON）。arrayFirst 标记首个元素（首个不带逗号）。
	jsonArray  bool
	arrayFirst bool

	committed bool
	last      time.Time // 最近一次下游写入时刻（mu 保护）
	broken    error     // 客户端断开/写失败（mu 保护）

	// responses 面中途错误帧的续号与响应标识（mu 保护；其余入口不用）：
	// 合成端不把 sequence_number 当可选（表 §4.1），中途错误帧必须承接已下发
	// 事件的计数；response.failed 的 Response 对象用首个事件里的 id/model/created_at。
	seq         int64  // 下一个可用 sequence_number
	respID      string // 响应 id（首个 response 对象事件）
	respModel   string
	respCreated int64
	identSeen   bool
	// renumbered RESYNC 后重编号过的 responses 帧数（W41 条目 3；纯观测——
	// 服务壳日志用）。0 = 从未丢帧（字节保真路径）。
	renumbered int
}

func newDownstream(w http.ResponseWriter, entry registry.Format, jsonArray bool) *downstream {
	d := &downstream{w: w, entry: entry, jsonArray: jsonArray, arrayFirst: true}
	if f, ok := w.(http.Flusher); ok {
		d.flush = f.Flush
	} else {
		d.flush = func() {}
	}
	return d
}

// commit 写下游响应头并转入已提交态（调用方保证在首个有效产出时调用一次）。
// 数组形态（jsonArray）：Content-Type: application/json + 首字节 `[`。
func (d *downstream) commit() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.committed {
		return
	}
	h := d.w.Header()
	if d.jsonArray {
		h.Set("Content-Type", "application/json")
		d.w.WriteHeader(http.StatusOK)
		_, _ = d.w.Write([]byte("["))
		d.flush()
		d.committed = true
		d.last = time.Now()
		return
	}
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	d.w.WriteHeader(http.StatusOK)
	d.flush()
	d.committed = true
	d.last = time.Now()
}

// write 写入下游字节并 flush（空字节为 no-op）。
func (d *downstream) write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.writeLocked(b)
}

func (d *downstream) writeLocked(b []byte) error {
	if d.broken != nil {
		return d.broken
	}
	if _, err := d.w.Write(b); err != nil {
		d.broken = err
		return err
	}
	d.flush()
	d.last = time.Now()
	d.observeResponsesChunk(b)
	return nil
}

// writeArrayElements 数组形态写元素（W5-19 条目 2）：首个元素前无分隔符，其后
// 以 `,` 分隔（`[` 已由 commit 写出）。elems 为已抽取的 JSON 元素载荷。
func (d *downstream) writeArrayElements(elems [][]byte) error {
	if len(elems) == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.broken != nil {
		return d.broken
	}
	var buf bytes.Buffer
	for _, e := range elems {
		if d.arrayFirst {
			d.arrayFirst = false
		} else {
			buf.WriteByte(',')
		}
		buf.Write(e)
	}
	return d.writeLocked(buf.Bytes())
}

// finishArray 数组形态收尾：写 `]` 闭合（正常结束路径）。未提交（从未产出元素）
// = no-op——调用方按预提交失败路径回错误信封（数组未开始，状态码仍自由）。
// 中途失败**不**调用本方法：截断的 JSON 是客户端可见的失败，闭合会被误读为
// 正常完成（错误面在数组形态下没有合法的帧内表达）。
func (d *downstream) finishArray() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.broken != nil {
		return d.broken
	}
	if !d.committed {
		return nil
	}
	return d.writeLocked([]byte("]"))
}

// isBroken 下游是否已断（写失败/心跳探测；调用方据此把失败归因到下游而非上游）。
func (d *downstream) isBroken() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.broken != nil
}

// observeResponsesChunk responses 面下行字节观测（仅 responses 入口；调用方持锁）：
//
//   - sequence_number：取本块最大值的 +1 作为下一个可用号（中途错误帧续号——
//     表 §4.1：合成端不把 sequence_number 当可选；读取端容忍缺失）；
//   - 响应标识（id/model/created_at）：取首个携带 response 对象的事件
//     （response.created 恒在流首，终止事件重复同一值）——response.failed 的
//     Response 对象要用；上游/出口都没给时留空。
func (d *downstream) observeResponsesChunk(b []byte) {
	if d.entry != registry.FormatOpenAIResponses {
		return
	}
	const seqKey = `"sequence_number":`
	for rest := b; ; {
		i := bytes.Index(rest, []byte(seqKey))
		if i < 0 {
			break
		}
		rest = rest[i+len(seqKey):]
		n, width := leadingInt(rest)
		if width > 0 && n >= d.seq {
			d.seq = n + 1
		}
		rest = rest[width:]
	}
	if !d.identSeen {
		d.identSeen = d.captureResponsesIdentity(b)
	}
}

// captureResponsesIdentity 从下行字节里取首个 response 对象的标识：定位 `data:`
// 后交给 json.Decoder（在首个 JSON 值处停，帧尾的 `\n\n` 与后续帧不受影响）。
// 返回 false = 本块没有可用的响应对象（继续在后续块里找）。
func (d *downstream) captureResponsesIdentity(b []byte) bool {
	i := bytes.Index(b, []byte("data:"))
	if i < 0 {
		return false
	}
	start := bytes.IndexByte(b[i:], '{')
	if start < 0 {
		return false
	}
	var wire struct {
		Response *struct {
			ID        string `json:"id"`
			Model     string `json:"model"`
			CreatedAt int64  `json:"created_at"`
		} `json:"response"`
	}
	if err := json.NewDecoder(bytes.NewReader(b[i+start:])).Decode(&wire); err != nil || wire.Response == nil {
		return false
	}
	if wire.Response.ID == "" && wire.Response.Model == "" {
		return false // 非 response 对象事件（如 output_text.delta）：继续找
	}
	d.respID, d.respModel, d.respCreated = wire.Response.ID, wire.Response.Model, wire.Response.CreatedAt
	return true
}

// renumberResponsesFrame responses 直通帧的 RESYNC 后重编号（W41 条目 3；
// 调用方只在 reader.Resyncs() > 0 的路径上调用——未丢帧路径字节保真，零改动）：
// 把帧里的 `"sequence_number":<n>` 改写为 d.seq（客户端已见的下一号），返回新
// 字节并推进 d.seq。上游丢帧后继续按原号计数会留下洞（0..K 后跳到 K+2）——
// 重编号把客户端可见帧序压回连续单调（GoModel `Renumberer` 纪律：
// 丢/改/注入事件的出口必须重编号）。帧里没有 sequence_number 字段 → 原样返回
// （不改字节、不推号）。
func (d *downstream) renumberResponsesFrame(b []byte) []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	out, ok := renumberSequenceNumber(b, d.seq)
	if !ok {
		return b
	}
	d.seq++
	d.renumbered++
	return out
}

// renumberedCount 已重编号的 responses 帧数（mu 保护；纯观测）。
func (d *downstream) renumberedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.renumbered
}

// renumberSequenceNumber 把 b 里首个 `"sequence_number":` 后的十进制数字段改写为
// seq——其余字节逐字不动（键与空白原样；只换数字段）。返回 (nil,false) = 未命中
// （无字段/无数字，畸形输入不猜）。
func renumberSequenceNumber(b []byte, seq int64) ([]byte, bool) {
	const key = `"sequence_number":`
	i := bytes.Index(b, []byte(key))
	if i < 0 {
		return nil, false
	}
	rest := b[i+len(key):]
	j := 0
	for j < len(rest) && (rest[j] == ' ' || rest[j] == '\t') {
		j++
	}
	k := j
	for k < len(rest) && rest[k] >= '0' && rest[k] <= '9' {
		k++
	}
	if k == j {
		return nil, false
	}
	out := make([]byte, 0, len(b)+8)
	out = append(out, b[:i+len(key)+j]...) // 到数字起点（含键与空白）
	out = append(out, strconv.FormatInt(seq, 10)...)
	out = append(out, rest[k:]...) // 数字之后的其余字节
	return out, true
}

// leadingInt 解析开头的十进制整数（跳前导空白）；返回数值与消耗字节数（width=0
// 表示未命中）。超长数字当未命中（畸形输入不猜）。
func leadingInt(b []byte) (int64, int) {
	i := 0
	for i < len(b) && (b[i] == ' ' || b[i] == '\t') {
		i++
	}
	start := i
	var n int64
	for ; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
		n = n*10 + int64(b[i]-'0')
		if n > 1<<52 {
			return 0, 0
		}
	}
	if i == start {
		return 0, 0
	}
	return n, i
}

// heartbeatFrame 保活帧：claude 面 = 命名 ping 事件（SDK 解码层吞掉，不重置
// 看门狗——CC B9）；openai 面 = 注释行（WHATWG SSE 客户端忽略）；数组形态
// （jsonArray）= nil——数组里没有合法的心跳帧（SSE 注释/事件都不是 JSON；
// 唯一合法插入是空白，但空白不携带信息，跳过）。
func (d *downstream) heartbeatFrame() []byte {
	if d.jsonArray {
		return nil
	}
	if d.entry == registry.FormatClaudeMessages {
		return []byte("event: ping\ndata: {\"type\":\"ping\"}\n\n")
	}
	return []byte(": keepalive\n\n")
}

// beat 心跳一次：距上次写入 ≥ 间隔才发（上游活跃时不打扰）；返回 false 表示
// 下游已断（调用方应取消上游）。空帧（数组形态）为 no-op（不写、不算断）。
//
// 提交点门（W5-27 条目 1，P64B 探针实锤）：**未提交（commit 未调用）不发心跳**。
// 上游「响应头先到、首帧静默 > 心跳间隔」时，提交前写心跳会隐式提交响应——
// Go 自动 200 + `text/plain; charset=utf-8`（违反官方 C1），随后 `commit()` 记
// `superfluous WriteHeader`，预提交失败的真实状态码退化为 200（客户端见
// 200 + ping + 错误 JSON）。提交前的长静默由上游响应头等待（ResponseHeaderTimeout
// 60s）与 `first_byte_timeout_seconds` 覆盖——这正是 W5-15 的决策（不做 TTFT
// 提前开流：心跳提前写字节 = 隐式提前开流，代价面同款），本门把实现对齐该决策。
func (d *downstream) beat(interval time.Duration) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.broken != nil {
		return false
	}
	if !d.committed {
		return true
	}
	if time.Since(d.last) < interval {
		return true
	}
	f := d.heartbeatFrame()
	if len(f) == 0 {
		return true
	}
	return d.writeLocked(f) == nil
}

// startHeartbeat 启动心跳 goroutine（ctx 取消或下游断开时退出）。
func (d *downstream) startHeartbeat(ctx context.Context, interval time.Duration, cancel context.CancelFunc) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if !d.beat(interval) {
					cancel()
					return
				}
			}
		}
	}()
}

// ---------------------------------------------------------------------------
// 首产出超时（TTFT 的保守替代：保留提交点）
// ---------------------------------------------------------------------------

// firstByteGuard 首产出超时守卫（W5-15 建议 2 / 本波第 5 条）：从上游请求发起起
// 计时，超时未产出（提交点未到）→ 取消本次尝试的上游请求——relay 读端随即报错，
// 提交点纪律保证调用方可以换下一个上游（真实状态码与 failover 全部保留；这与
// 「TTFT 提前开流」的代价面正好相反，见 downstream 的决策注释）。
//
// 关闭条件（缺省）：Config.FirstByteTimeoutSeconds <= 0 → 构造返回 nil（零开销，
// 行为与引入前完全一致）。计时窗口在**提交点**结束（commit 后上游慢是另一个面：
// 已提交不换上游，心跳帧兜住）。
type firstByteGuard struct {
	timeout  time.Duration
	timedOut atomic.Bool
	stop     func()
}

// newFirstByteGuard 启动守卫；timeout<=0 → nil（关）。cancel = 本次上游尝试的
// ctx 取消函数（超时即取消上游请求）。
func newFirstByteGuard(ctx context.Context, cancel context.CancelFunc, timeout time.Duration) *firstByteGuard {
	if timeout <= 0 {
		return nil
	}
	g := &firstByteGuard{timeout: timeout}
	done := make(chan struct{})
	var once sync.Once
	g.stop = func() { once.Do(func() { close(done) }) }
	go func() {
		t := time.NewTimer(timeout)
		defer t.Stop()
		select {
		case <-done:
		case <-ctx.Done():
		case <-t.C:
			g.timedOut.Store(true)
			cancel()
		}
	}()
	return g
}

// TimedOut 超时是否已触发（nil 安全）。
func (g *firstByteGuard) TimedOut() bool { return g != nil && g.timedOut.Load() }

// ---------------------------------------------------------------------------
// 上游 SSE 逐帧读取
// ---------------------------------------------------------------------------

// maxSSELineBytes 单行/单帧上限（1 MiB）——Higress `provider/sse_framing.go:10-70`
// 的同值工程件（`sseFramer` 的 tail 上限）。上游发畸形巨行时：不无限攒内存，
// 丢弃到下一个帧边界（RESYNC）继续读——见 sseFrameReader.Next 的 resync 分支。
const maxSSELineBytes = 1 << 20

// sseFrameReader 上游 SSE 帧读取器（event 名 + data 载荷；注释行忽略；
// 多行 data 以 "\n" 连接；EOF 时派发未闭合帧——宽容，真实流常以空行收尾）。
//
// jsonFallback（gemini 上游形态）：非 `data:` 前缀的 JSON 行按括号平衡聚合成帧
// ——gemini 流中错误即以这种裸 JSON 行到达（gemini-conversion-table §流式
// 「流中错误」行 / §7.1；只按 `data:` 解析会把整条错误静默丢掉）。
//
// 三件工程件（W5-25 条目 2；Higress sse_framing.go 的四条细节里我们缺的三条）：
//
//  1. **last-chunk flush**（`:46 frameSSEEvents` 的 isLastChunk 分支）：上游可能
//     不发结尾空行就关流（Higress 注释点名 vertex）——EOF 时把已攒的 data 派发成
//     最后一个帧（**不丢 usage**；丢尾部事件 = 丢 usage）。`[DONE]` 哨兵在 data
//     里，同一条 flush 路径覆盖。
//  2. **1 MiB 上限**：单行/单帧超限不无限攒（`maxSSELineBytes`）。
//  3. **RESYNC**：畸形巨行（或巨帧）丢弃到下一个帧边界后**继续读**——一条坏行
//     不再终止整条流（旧实现靠 bufio.Scanner 的 64 MiB 缓冲，超限即断流）。
//
// 与 Higress 的差异（有意）：保留的 tail 不做行尾归一（我们逐行读、不重写字节，
// 直通帧由 frameBytes 按行重建——归一没有意义）；resyncCarry（≤3 字节跨回调
// 分隔符匹配）也不需要——我们按行读，分隔符匹配天然跨读。
type sseFrameReader struct {
	br       *bufio.Reader
	event    string
	data     []string
	curEvent string
	curData  string
	done     bool

	jsonFallback bool
	jsonLine     strings.Builder
	jsonDepth    int

	// pending 跨读残行（readLine 的拼接缓冲；每条完整行返回后重置）。
	pending []byte
	// resyncs 已丢弃的畸形帧数（超限行；纯观测——服务壳日志用）。
	resyncs int
	// lostContent 被丢弃的超限行是否**承载内容**（R01：`data:` 行 / jsonFallback
	// 的裸 JSON 行——见 noteOversized）。丢弃非内容行（注释/保活/字段行）不算丢
	// 内容；relay 据此把「丢内容帧」的尝试判为失败，不再以 completed/delivered
	// =true 收尾（静默假成功）。纯观测位，绝不改 RESYNC 的恢复行为。
	lostContent bool
}

func newSSEFrameReader(r io.Reader, jsonFallback bool) *sseFrameReader {
	return &sseFrameReader{br: bufio.NewReaderSize(r, 64*1024), jsonFallback: jsonFallback}
}

// errLineTooLong 单行超过 maxSSELineBytes（readLine 内部信号；Next 转 RESYNC）。
var errLineTooLong = errors.New("service: sse line exceeds 1MiB cap")

// readLine 读一行（不含换行；行尾 \r 剥除）。行内容以内部缓冲承载——**下一条
// readLine 前有效**（调用方立即消费）。超过 maxSSELineBytes：**丢弃该行余部到
// 换行为止**后返回 errLineTooLong（返回即处于「下一行开头」——调用方可继续读，
// 这是 RESYNC 能续读的前提）。
func (f *sseFrameReader) readLine() ([]byte, error) {
	tooLong := false
	for {
		chunk, err := f.br.ReadSlice('\n')
		if !tooLong {
			f.pending = append(f.pending, chunk...)
			if len(f.pending) > maxSSELineBytes {
				f.noteOversized(f.pending) // R01：丢弃前判定是否丢内容
				tooLong = true
				f.pending = f.pending[:0]
			}
		}
		if err == nil {
			if tooLong {
				return nil, errLineTooLong // 余部已读到换行，reader 在下一行开头
			}
			line := f.pending[:len(f.pending)-1] // 去 '\n'
			if n := len(line); n > 0 && line[n-1] == '\r' {
				line = line[:n-1]
			}
			out := line
			f.pending = f.pending[:0]
			return out, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue // 缓冲满、行未完：继续攒（或继续丢余部）
		}
		// EOF / 底层读错：残行（无换行结尾）也要返回——last-chunk flush 的数据面。
		if tooLong {
			f.pending = f.pending[:0]
			return nil, errLineTooLong
		}
		line := f.pending
		f.pending = f.pending[:0]
		if len(line) > 0 && err == io.EOF {
			return line, nil
		}
		return nil, err
	}
}

// noteOversized 在丢弃一条超限行**之前**判定它是否承载内容（R01）：prefix = 该行
// 已攒字节（含行首，≥ maxSSELineBytes——判定所需的行首前缀完整）。判据与 Next 的
// 派发面同源：
//
//   - `data:` 行 → 内容（SSE 载荷：正文/usage 的唯一载体）；
//   - jsonFallback（gemini 裸 JSON 形态）下首字节为 `{`/`[` 的非注释行 → 内容
//     （裸 JSON 帧；与 Next 的 jsonLine 聚合分支同一入口集合）；
//   - 注释（`:`）/ `event:` / `id:` / `retry:` / 空行 / 其它字段行 → **非内容**
//     （纯保活/元数据；丢它们不丢正文——既有 RESYNC 语义不变）。
//
// 只置位、绝不改控制流；RESYNC（丢弃到帧边界后继续读）行为不变。
func (f *sseFrameReader) noteOversized(prefix []byte) {
	if f.lostContent {
		return
	}
	if bytes.HasPrefix(prefix, []byte("data:")) {
		f.lostContent = true
		return
	}
	if !f.jsonFallback {
		return
	}
	trimmed := bytes.TrimLeft(prefix, " \t")
	if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
		f.lostContent = true
	}
}

// LostContent 是否曾丢弃**承载内容**的超限行（1 MiB 上限触发的 RESYNC；R01）。
// relay 据此把本尝试判为上游失败：丢内容帧不得以 completed/delivered=true 收尾。
func (f *sseFrameReader) LostContent() bool { return f.lostContent }

// resyncAfterOversized 丢弃畸形帧的剩余字节：丢到下一个空行边界（帧边界），然后
// 继续正常读——一条坏行不终止整条流。EOF 也终止丢弃（reader 已由 readLine 推进到
// 巨行之后的行首，这里从下一行开始丢）。
func (f *sseFrameReader) resyncAfterOversized() {
	for {
		line, err := f.readLine()
		if err == nil && len(line) == 0 {
			return // 空行 = 帧边界
		}
		if errors.Is(err, errLineTooLong) {
			continue // 又一个巨行：readLine 已丢弃并推进到其行尾之后
		}
		if err != nil { // io.EOF / 底层读错：流结束
			f.done = true
			return
		}
	}
}

// Next 读取下一个帧；ok=false 表示流结束。err 为底层读取错误（断流）。
func (f *sseFrameReader) Next() (ok bool, err error) {
	if f.done {
		return false, nil
	}
	f.event = ""
	f.data = f.data[:0]
	for {
		line, lerr := f.readLine()
		if errors.Is(lerr, errLineTooLong) {
			// RESYNC（Higress sse_framing.go:46-70 同款）：畸形巨行丢弃到下一个
			// 帧边界后继续读。已攒的帧缓冲一并丢弃（它属于畸形帧）。
			f.resyncs++
			f.event = ""
			f.data = f.data[:0]
			f.jsonLine.Reset()
			f.jsonDepth = 0
			f.resyncAfterOversized()
			if f.done {
				return false, nil
			}
			continue
		}
		if lerr != nil && !errors.Is(lerr, io.EOF) {
			f.done = true
			return false, lerr
		}
		atEOF := errors.Is(lerr, io.EOF)
		if len(line) == 0 && atEOF {
			break
		}
		if strings.HasPrefix(string(line), ":") {
			if atEOF {
				break
			}
			continue // 注释/保活行
		}
		if f.jsonLine.Len() > 0 {
			// 聚合中的裸 JSON 行：其后所有非注释行都属同一文档，括号平衡即成帧。
			f.jsonLine.WriteByte('\n')
			f.jsonLine.Write(line)
			f.jsonDepth += braceDelta(string(line))
			if f.jsonDepth <= 0 {
				return f.dispatchJSONLine(), nil
			}
			if atEOF {
				return f.dispatchJSONLine(), nil // 残帧：宽容派发，交由解析层判错
			}
			continue
		}
		if len(line) == 0 {
			if len(f.data) > 0 {
				f.curEvent = f.event
				f.curData = strings.Join(f.data, "\n")
				return true, nil
			}
			f.event = ""
			if atEOF {
				break
			}
			continue
		}
		if v, ok := strings.CutPrefix(string(line), "event:"); ok {
			f.event = strings.TrimSpace(v)
			if atEOF {
				break
			}
			continue
		}
		if v, ok := strings.CutPrefix(string(line), "data:"); ok {
			f.data = append(f.data, strings.TrimPrefix(v, " "))
			if atEOF {
				break
			}
			continue
		}
		if strings.HasPrefix(string(line), "id:") || strings.HasPrefix(string(line), "retry:") {
			if atEOF {
				break
			}
			continue // 其它 SSE 字段行忽略。
		}
		if !f.jsonFallback {
			if atEOF {
				break
			}
			continue // 其它字段行忽略（anthropic/openai 上游的既有行为）。
		}
		f.jsonLine.Write(line)
		f.jsonDepth = braceDelta(string(line))
		if f.jsonDepth <= 0 {
			return f.dispatchJSONLine(), nil
		}
		if atEOF {
			return f.dispatchJSONLine(), nil
		}
	}
	f.done = true
	if f.jsonLine.Len() > 0 {
		return f.dispatchJSONLine(), nil // 残帧：宽容派发，交由解析层判错。
	}
	if len(f.data) > 0 {
		// last-chunk flush（Higress `:46` isLastChunk 分支；理由同注释：vertex 会
		// 在没有结尾空行的情况下关流——不派发就是丢 usage/丢尾部事件）。
		f.curEvent = f.event
		f.curData = strings.Join(f.data, "\n")
		return true, nil
	}
	return false, nil
}

// dispatchJSONLine 派发聚合完成的裸 JSON 行为一帧（无 event 名；data 为原文）。
func (f *sseFrameReader) dispatchJSONLine() bool {
	f.curEvent = ""
	f.curData = f.jsonLine.String()
	f.jsonLine.Reset()
	f.jsonDepth = 0
	return true
}

// braceDelta 括号平衡增量（字符串内的括号不计）。与 gemini 解析器
// （translate/convert/gemini parse.go 的 braceDelta）同语义——服务壳不依赖
// 该包的内部辅助，故此处独立实现（两处需同步修改）。
func braceDelta(s string) int {
	depth := 0
	inStr := false
	esc := false
	for _, r := range s {
		switch {
		case esc:
			esc = false
		case r == '\\' && inStr:
			esc = true
		case r == '"':
			inStr = !inStr
		case inStr:
		case r == '{' || r == '[':
			depth++
		case r == '}' || r == ']':
			depth--
		}
	}
	return depth
}

// Event 当前帧事件名（可为空）。
func (f *sseFrameReader) Event() string { return f.curEvent }

// Resyncs 已丢弃的畸形帧数（1 MiB 上限触发的 RESYNC；纯观测——服务壳日志用）。
func (f *sseFrameReader) Resyncs() int { return f.resyncs }

// Data 当前帧 data 载荷。
func (f *sseFrameReader) Data() []byte { return []byte(f.curData) }

// frameBytes 直通模式：按帧重建线格式（event 行 + data 行 + 空行）。
func (f *sseFrameReader) frameBytes() []byte {
	var b bytes.Buffer
	if f.curEvent != "" {
		b.WriteString("event: ")
		b.WriteString(f.curEvent)
		b.WriteByte('\n')
	}
	for _, line := range strings.Split(f.curData, "\n") {
		b.WriteString("data: ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return b.Bytes()
}

// geminiArrayElements 把 gemini SSE 帧字节切成 JSON 数组元素（W5-19 条目 2）：
// 官方非 SSE 形态的 body 是 GenerateContentResponse 的 JSON 数组——每个 SSE 帧的
// data 载荷恰是一个元素（gemini 流无 event 名、无哨兵）。空块/无 data 的块
// （注释、保活）不产元素。帧 = 空行分隔的块；块内多行 data 以 "\n" 连接
// （与 sseFrameReader 同口径）。
func geminiArrayElements(frames []byte) [][]byte {
	var elems [][]byte
	for _, block := range bytes.Split(frames, []byte("\n\n")) {
		var payload []byte
		for _, line := range bytes.Split(block, []byte("\n")) {
			v, ok := bytes.CutPrefix(line, []byte("data:"))
			if !ok {
				continue
			}
			v = bytes.TrimPrefix(v, []byte(" "))
			if len(payload) > 0 {
				payload = append(payload, '\n')
			}
			payload = append(payload, v...)
		}
		if len(payload) > 0 {
			elems = append(elems, payload)
		}
	}
	return elems
}

// geminiStreamTerminal gemini 同协议直通流的**终止证据**观测器（W5-22 FD1 裁决）。
// 直通路径此前不校验 finishReason——断流 EOF 被当正常结束（SSE 无错误帧、数组
// `]` 闭合 = 语法合法的假成功），与跨协议 Finish()（gemini parse.go 的「无
// finishReason 的 EOF 判失败」）不对称。证据面与解析器 Finish 同口径：
//
//   - finishReason 非空（正常终止；22 值枚举任一）；
//   - promptFeedback.blockReason 非空（prompt 被拦——显式化，禁止空回复静默成功）；
//   - error 对象非空（错误即终止；上游错误行在直通中原样转发，不再叠加错误帧）。
//
// 用最小结构体解析而不是字节扫描：正文里出现的 `"finishReason"` 字面量（模型
// 输出原文）不得被误判为终止证据。`[DONE]` 不计（非 Gemini 语义；与解析器口径
// 一致——严格缺证据 = 失败，[DONE] 由解析器忽略）。
type geminiStreamTerminal struct {
	finishSeen bool
	blockSeen  bool
	errorSeen  bool
}

// observe 观测一帧 gemini 载荷（多行 data 已按 "\n" 连接；非 JSON 不算证据）。
func (t *geminiStreamTerminal) observe(data []byte) {
	var wire struct {
		Candidates []struct {
			FinishReason string `json:"finishReason"`
		} `json:"candidates"`
		PromptFeedback struct {
			BlockReason string `json:"blockReason"`
		} `json:"promptFeedback"`
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return
	}
	for _, c := range wire.Candidates {
		if c.FinishReason != "" {
			t.finishSeen = true
		}
	}
	if wire.PromptFeedback.BlockReason != "" {
		t.blockSeen = true
	}
	if len(wire.Error) > 0 && !bytes.Equal(bytes.TrimSpace(wire.Error), []byte("null")) {
		t.errorSeen = true
	}
}

// terminated 是否已见终止证据（finishReason / blockReason / error 任一）。
func (t *geminiStreamTerminal) terminated() bool {
	return t.finishSeen || t.blockSeen || t.errorSeen
}

// openaiStreamTerminal openai chat 同协议直通流的**终止证据**观测器（W5-23 裁定：
// 保守版校验）。证据面 = 传输级收尾（任一即终止）：
//
//   - finish_reason 非 null 且非空（任一 choices 元素；与解析器同判据）；
//   - `[DONE]` 哨兵（chat 面传输终止形态——消费端 SDK 以它判流结束）；
//   - error 对象非空（错误即终止；错误帧在直通中原样转发，不再叠加错误帧）。
//
// 与跨协议解析器 Finish() 的差异是刻意的（保守方向）：解析器要求 finish_reason
// （即便见了 [DONE] 也判失败——它要合成下游终态事件，必须有 stop reason）；
// 直通只需保证「传输收尾真实」：见了 [DONE] 的流传输层是干净收尾，语义完整性
// （finish_reason）留给客户端 SDK。只判「零证据 EOF」——唯一会被新判据波及的
// 形态是真正被截断的流（无 finish_reason、无 [DONE]、无错误）。
type openaiStreamTerminal struct {
	finishSeen bool
	doneSeen   bool
	errorSeen  bool
}

// observe 观测一帧 chat 载荷（多行 data 已按 "\n" 连接；非 JSON 不算证据）。
func (t *openaiStreamTerminal) observe(data []byte) {
	if isDoneSentinel(data) {
		t.doneSeen = true
		return
	}
	var wire struct {
		Choices []struct {
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return
	}
	for _, c := range wire.Choices {
		if c.FinishReason != nil && *c.FinishReason != "" {
			t.finishSeen = true
		}
	}
	if len(wire.Error) > 0 && !bytes.Equal(bytes.TrimSpace(wire.Error), []byte("null")) {
		t.errorSeen = true
	}
}

// terminated 是否已见终止证据（finish_reason / [DONE] / error 任一）。
func (t *openaiStreamTerminal) terminated() bool {
	return t.finishSeen || t.doneSeen || t.errorSeen
}

// responsesStreamTerminal responses 同协议直通流的**终止证据**观测器（W5-23
// 裁定：保守版校验）。证据面 = 传输级收尾（任一即终止）：
//
//   - 终止事件 type ∈ {response.completed, response.incomplete, response.failed,
//     response.done}（response.done 为非官方别名——与解析器 evDoneAlias 同权容忍）；
//   - `error` 事件（平铺 type=="error"）；
//   - `[DONE]` 兼容哨兵（上游兼容层补发——既有容忍面：视为流结束、不转发；
//     传输层干净收尾即算证据，同 openai 面）。
//
// 与解析器 Finish()（要求终止事件；[DONE] 无终止事件也判失败）的差异同
// openaiStreamTerminal：直通只判「零证据 EOF」，不替客户端判语义完整性。
type responsesStreamTerminal struct {
	terminalSeen bool
	errorSeen    bool
	doneSeen     bool
}

// observe 观测一帧 responses 载荷（非 JSON / 无 type 不算证据）。
func (t *responsesStreamTerminal) observe(data []byte) {
	if isDoneSentinel(data) {
		t.doneSeen = true
		return
	}
	var wire struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return
	}
	switch wire.Type {
	case "response.completed", "response.incomplete", "response.failed", "response.done":
		t.terminalSeen = true
	case "error":
		t.errorSeen = true
	}
}

// terminated 是否已见终止证据（终止事件 / error 事件 / [DONE] 任一）。
func (t *responsesStreamTerminal) terminated() bool {
	return t.terminalSeen || t.errorSeen || t.doneSeen
}

// claudeWebErrorFrame claude 面（同协议直通）的**流内错误帧**判据：event 行
// `error`，或 event 行缺失时 data.type == "error"（宽容回退与
// claudeWebTerminalFrame 同款）。只取 error 半边——`message_stop` 是正常终止帧、
// **不**是失败证据（W87-1：终止判据与错误判据分离，否则正常收尾会被记成上游失败）。
func claudeWebErrorFrame(event string, data []byte) bool {
	name := strings.TrimSpace(event)
	if name == "" {
		var w struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(bytes.TrimSpace(data), &w) != nil {
			return false
		}
		name = w.Type
	}
	return name == "error"
}

// chatgptWebErrorFrame chatgpt 面（同协议直通）的**流内错误帧**判据：data 去掉
// 首尾空白后必须是 JSON 对象，且 `error` 字段非空、非 `null`（与
// chatgptWebTerminalFrame 的 error 半边同判据）。`[DONE]` 是正常终止哨兵、
// **不**是失败证据；`message_stream_complete` 等尾部帧同理——终止判据与错误
// 判据分离（W91-1，同 W87-1 对 claude 面的口径）。
func chatgptWebErrorFrame(event string, data []byte) bool {
	_ = event // chatgpt 流的 event: 行只有 delta_encoding——错误判据只看载荷
	d := bytes.TrimSpace(data)
	if len(d) == 0 || d[0] != '{' {
		return false
	}
	var probe struct {
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(d, &probe) != nil {
		return false
	}
	e := bytes.TrimSpace(probe.Error)
	return len(e) > 0 && string(e) != "null"
}

// directFrameTerminal 直通（同协议）模式逐帧终止证据观测 + 「**本帧**是否终止帧」/
// 「**本帧**是否流内错误帧」判定（R01/W70-3 二次修：两个动作必须同点同序——
// 观测面先更新，再取差分）。
//
// 判定复用同一观测器（不复制解析面）：差分 = 观测后 `terminated()` 由假转真
// （finish_reason 帧 / responses 终态事件 / gemini finishReason·流内错误）；
// 哨兵帧（chat/responses 的 `[DONE]`）**恒**算终止帧（重复哨兵也拦）；
// claude 面（直通）按 `message_stop` / `error` 帧（与 claude_web 终止帧同判据）；
// 两个网页上游（claude_web / chatgpt_web）按各自 `*WebTerminalFrame` 判据
// （W91-1：`message_stop` / `[DONE]` 是正常流末，`error` 帧才是失败证据）。
//
// 第二返回值 errorFrame（W87-1 D1）= **本帧**首次置上「流内错误」证据：三类观测器
// 按 errorSeen 差分；claude 面与 claude_web 按 claudeWebErrorFrame、chatgpt_web
// 按 chatgptWebErrorFrame（W91-1）。它只进 **ledger/健康归因**
// （failed/end_reason/交付位），**不**改字节转发面、不改换台（已提交流不重试）。
//
// 语义：terminalFrame 为真 = 该帧本身是终止证据。丢内容帧后（reader.LostContent）
// 这类帧**不得转发**——唯读哨兵/终态帧的消费端会提前收流，看不到随后的
// incomplete_stream 错误尾（假成功）；普通内容帧不受影响（照常转发）。
func directFrameTerminal(upProto registry.Format, event string, data []byte,
	term *geminiStreamTerminal, chat *openaiStreamTerminal, resp *responsesStreamTerminal) (bool, bool) {
	termBefore := term.terminated()
	chatBefore := chat.terminated()
	respBefore := resp.terminated()
	termErrBefore := term.errorSeen
	chatErrBefore := chat.errorSeen
	respErrBefore := resp.errorSeen
	switch upProto {
	case registry.FormatGeminiGenerate:
		term.observe(data) // FD1：见 geminiStreamTerminal
	case registry.FormatOpenAIChat:
		chat.observe(data) // W5-23：见 openaiStreamTerminal
	case registry.FormatOpenAIResponses:
		resp.observe(data) // W5-23：见 responsesStreamTerminal
	}
	var errorFrame bool
	switch upProto {
	case registry.FormatGeminiGenerate:
		errorFrame = !termErrBefore && term.errorSeen
	case registry.FormatOpenAIChat:
		errorFrame = !chatErrBefore && chat.errorSeen
	case registry.FormatOpenAIResponses:
		errorFrame = !respErrBefore && resp.errorSeen
	case registry.FormatClaudeMessages:
		errorFrame = claudeWebErrorFrame(event, data)
	case registry.FormatClaudeWeb:
		errorFrame = claudeWebErrorFrame(event, data)
	case registry.FormatChatGPTWeb:
		errorFrame = chatgptWebErrorFrame(event, data)
	}
	switch upProto {
	case registry.FormatGeminiGenerate:
		return !termBefore && term.terminated(), errorFrame
	case registry.FormatOpenAIChat:
		return isDoneSentinel(data) || (!chatBefore && chat.terminated()), errorFrame
	case registry.FormatOpenAIResponses:
		return isDoneSentinel(data) || (!respBefore && resp.terminated()), errorFrame
	case registry.FormatClaudeMessages:
		return claudeWebTerminalFrame(event, data), errorFrame
	case registry.FormatClaudeWeb:
		return claudeWebTerminalFrame(event, data), errorFrame
	case registry.FormatChatGPTWeb:
		return chatgptWebTerminalFrame(event, data), errorFrame
	}
	return false, errorFrame
}

// ---------------------------------------------------------------------------
// relay
// ---------------------------------------------------------------------------

// relayStream 上游 SSE → 下游 SSE 的流式 relay（提交点纪律见 downstream）。
//
//   - 逐帧读上游 → 转换（跨协议走 pairs.StreamPipe；同协议原样转发）
//   - 首个有效产出才提交响应头（上游失败在提交前 → (false, err) 可重试）
//   - 提交后上游断流/违规 → 下游错误帧收尾（不重试）
//   - 客户端断开 → ctx 取消 → 上游请求取消（**不计**上游失败——W5-19 条目 4）
//   - 上游静默 → 心跳保活帧（**提交后**才发——提交点门见 downstream.beat，W5-27；
//     数组形态无心跳——非合法 JSON）
//   - 首产出超时（guard 非 nil）→ 取消本尝试（未提交 → 换上游；W5-15 建议 2）
//   - per-attempt usage 记账（W5-18）：本尝试全窗口观测 usage，收尾记一次
//     （诊断面 + 一行日志；绝不进下游可见流）
//   - per-request attempt 记录（W5-19 条目 4）：收尾把 ar 交给 forward 的 defer
//     追加（下游断开标 disconnected 且不置 failed）
//   - 数组形态（dr.arrayStream；W5-19 条目 2）：下游写 JSON 数组（元素 = 每帧
//     data 载荷；提交写 `[`、`]` 正常收尾；中途失败不闭合——截断 JSON = 失败信号）
//
// idx = 上游在配置里的下标；上游侧失败（提交前后）记入健康状态（连续失败 →
// 冷却），客户端断开不记（不是上游的健康信号）。
// dr.attempt = 本请求内的尝试序号（1 起）；ar = 本尝试的记录（W5-19 条目 4）。
func (s *Server) relayStream(ctx context.Context, cancel context.CancelFunc, guard *firstByteGuard, idle *upstreamIdleGuard, w http.ResponseWriter, entry registry.Format, idx int, upProto registry.Format, resp *http.Response, br *bufio.Reader, dr downstreamRequest, ar *attemptRecord) (bool, error) {
	up := s.cfg.Upstreams[idx]
	var pipe *pairs.StreamPipe
	// 入口契约（W43-3 §4.2 文档化）：pipe 只接**completion 流**。claude_web 上游的
	// MCP 连接器列表流（server_list/server_base/tools/…，无 message_start 骨架）
	// 不属 completion 面、不得进 pipe——出口 writer 会显式报错（缺骨架不静默补帧，
	// 设计行为；该流在服务壳里不经 relayStream 的 completion 路径）。
	if upProto != entry {
		var err error
		pipe, err = pairs.NewStreamPipe(s.reg, upProto, entry, dr.opts)
		if err != nil {
			s.writeError(w, entry, http.StatusBadGateway,
				&ir.Error{Type: "api_error", Message: "no stream conversion: " + err.Error(), Source: "any2api"})
			return true, nil
		}
		// 管道出口诊断消费（W5-11 §剩余面 5）：收尾读一次计数——日志一行 + 服务壳
		// 计数。纯观测、绝不进控制流；同协议直通（无管道）无此面。
		defer s.consumePipeDiagnostics(up, pipe)
	}
	// chatgpt_web 尾读宽限 + accessor 观测面（W30-2；W32-1 口径修正）：**definitive
	// marker**（`message_stream_complete`/`[DONE]`/error 帧）到达即收束（不等宽限）；
	// 终态（status finished 等）无 marker 时由宽限窗口（终态后连续 3s 没有新帧）
	// 兜底收束，不等连接关闭（上游实测终态后握 socket 1.3–3s）。目标：尾部帧
	// （last_token/title/ste/stream_complete）收进 accessor/观测面（W31-1 实证
	// 250ms 时流式 4/4 截于 last_token）。只对 chatgpt_web 入口（其余格式
	// cgTail/cgEntry 为 nil 零开销）。观测面（ChatGPTWebEntry）在收尾 defer 里
	// 消费（chatgptWebObserve——accessor 清单逐条，纯诊断）。
	var cgEntry pairs.ChatGPTWebEntry
	var cgTail *chatgptWebTailGuard
	var moderationHandled bool // S6 出口面：moderation 拦截只注入一次
	if upProto == registry.FormatChatGPTWeb && pipe != nil {
		cgEntry = pipe.ChatGPTWebEntry()
		cgTail = newChatGPTWebTailGuard(ctx, cancel, chatgptWebTailGrace)
		defer cgTail.stopGuard()
	}
	s.ensureRequestID(w, resp, dr.reqID)
	// C4（W5-27 条目 2）：成功面 ratelimit 头转发（流式；官方要求该族在**每个
	// 响应**上原样转发）。提交点前写入 w.Header()——未提交时上游失败换上游，
	// 下次尝试由 copyRateLimitHeaders 的先清后写覆盖（无残留泄漏）。
	copyRateLimitHeaders(w.Header(), resp.Header)
	d := newDownstream(w, entry, dr.arrayStream)
	d.startHeartbeat(ctx, s.hb, cancel)

	reader := newSSEFrameReader(br, upProto == registry.FormatGeminiGenerate)
	committed := false
	// 本尝试已收帧的 usage 观测（诊断面）。W5-18 条目 4：探针从「仅预提交窗口」
	// 扩到**全窗口**（提交后也观测——per-attempt 记账要看到真实尝试用量）；
	// 预提交失败仍走 W5-15 的回收面（绝不并入下游可见流——见 partial_usage.go
	// 头注）。
	var partial partialUsage
	// FD1（W5-22）：gemini 同协议直通的终止证据观测（缺证据的 EOF = 截断流；
	// 判定与错误收尾见循环后的 EOF 处理）。
	var term geminiStreamTerminal
	// W5-23（三协议判据收尾）：openai chat / responses 直通的保守版终止证据观测
	// （只判「零证据 EOF」；claude 面不校验——判据在客户端，见报告 §判据）。
	var chatTerm openaiStreamTerminal
	var respTerm responsesStreamTerminal
	// 本尝试收尾记一次（成功 / 预提交失败 / 提交后失败）：failedAttempt = 上游侧
	// 失败（下游断开不计——不是上游健康信号，与 recordFailure 同纪律）。
	failedAttempt := false
	// W87-1（D1）：本尝试是否已观测到上游**流内错误帧**——失败归因 + 健康记账的
	// 单次闸（错误帧之后的读错误/丢帧不再重复计数）；同时决定交付位（错误帧
	// 流 = 非交付，与 W36-1 S2 的「错误信封/截断」同口径）。
	errorFrameSeen := false
	defer func() {
		// chatgpt_web accessor 消费（W30-2）：已提交的流在收尾点读一次观测面
		// （model/标题/会话 id/历史丢弃/工具记录/宽限命中/假成功候选/续接/moderation
		// ——chatgptWebObserve，纯诊断，绝不改下游字节）。
		if cgEntry != nil && committed {
			chatgptWebObserve(cgEntry, up)
		}
		// RESYNC 观测（W5-25 条目 2）：畸形帧丢弃数（1 MiB 上限触发）——纯观测，
		// 绝不进控制流；非零说明上游发了超限帧（丢帧是恢复行为，不是错误）。
		if n := reader.Resyncs(); n > 0 {
			log.Printf("any2api: upstream %s: %d oversized SSE frame(s) dropped (1MiB cap resync)", up.Name, n)
		}
		// W41 条目 3：丢帧后 responses 出口重编号计数（帧序连续性修复；纯观测）。
		if n := d.renumberedCount(); n > 0 {
			log.Printf("any2api: upstream %s: %d responses event(s) renumbered after resync (sequence_number kept contiguous)", up.Name, n)
		}
		broken := d.isBroken()
		if broken {
			// 下游断开（W5-19 条目 4）：失败归因到下游，不记上游失败、不进回收面。
			ar.disconnected = true
			failedAttempt = false
			if ar.endReason == "" {
				// S1 细分：任务面显式取消 → task_cancel（任务面写缓冲不失败，断连类
				// 归因只可能是取消）；其余连接级断裂 → downstream_disconnect。
				ar.endReason = disconnectEndReason(dr, true)
			}
		}
		if failedAttempt && !committed {
			// 预提交失败：W5-15 回收面（计数；日志在 finishAttempt 统一一行）。
			s.recoverPartialUsage(partial)
		}
		// S1（W36-1）：结束原因收口——显式归因点（断连/非流式成功）未设时的兜底；
		// idle 窄化优先于泛化上游失败（A7）。
		if ar.endReason == "" {
			switch {
			case failedAttempt && idle.TimedOut():
				ar.endReason = endReasonUpstreamIdle
			case failedAttempt:
				ar.endReason = endReasonUpstreamError
			case ar.disconnected:
				ar.endReason = disconnectEndReason(dr, false)
			default:
				ar.endReason = endReasonCompleted
			}
		}
		s.finishAttempt(ar, partial, failedAttempt)
	}()
	for {
		var ok bool
		var rerr error
		// terminalFrame 本帧是否上游协议的终止证据（仅直通模式判定；见
		// directFrameTerminal）。丢内容帧后据此拦下终止帧（R01 二次修）。
		var terminalFrame bool
		if idle.TimedOut() {
			// 内容静默超时（守卫已取消本尝试 ctx）：keepalive/空 delta 帧可能仍在
			// 读缓冲里——读不一定先报错，故在循环顶显式合成失败（分类见下方错误
			// 路径的 idleErr / 提交后错误帧分支）。
			rerr = errUpstreamIdle
		} else {
			ok, rerr = reader.Next()
		}
		if rerr != nil {
			// chatgpt_web 尾读宽限到期（终态后上游静默、socket 仍握）：按**正常
			// 流尾**收束——不记失败、不写错误帧；收尾补帧走循环后的 pipe.Finish()。
			// 必须排在下游断开判据之前：守卫的 ctx 取消不是下游断开（是我们自己
			// 掐的上游连接）。
			if cgTail.TimedOut() {
				break
			}
			// 下游断开（写失败 / 心跳探测 / 父 ctx 取消）：读错误是取消的后果，
			// 不是上游健康信号（W5-19 条目 4 的下游断开面）。**idle 守卫取消
			// 除外**——那是上游侧失败（守卫显式标记，归因见下）。
			if d.isBroken() || (ctx.Err() != nil && !guard.TimedOut() && !idle.TimedOut()) {
				ar.disconnected = true
				// S1（W36-1）：断连类细分（任务取消 / 连接级断裂 / 请求 ctx 取消）。
				ar.endReason = disconnectEndReason(dr, d.isBroken())
				return true, nil
			}
			// 上游断流 / 内容静默超时（提交前后都是上游侧失败）。W87-1：本尝试
			// 已因上游流内错误帧记过一次失败 → 不再重复计（单次闸）。
			if !errorFrameSeen {
				s.recordFailure(idx, 0)
			}
			failedAttempt = true
			if !committed {
				if e := firstByteErr(up, guard); e != nil {
					return false, e
				}
				if e := idleErr(up, idle); e != nil {
					return false, e
				}
				return false, fmt.Errorf("upstream %s stream read failed: %w", up.Name, rerr)
			}
			msg := "upstream stream read failed"
			if idle.TimedOut() {
				msg = fmt.Sprintf("upstream idle for %s with no content-bearing frame", idle.timeout)
			}
			// Code=upstream_idle（Slice 2）：错误帧带上机器可读的挂住判别码——任务
			// 面据此把该 failed 帧映射为任务终态 incomplete（task.go §失败语义表）。
			// Type 保持 incomplete_stream（claude 面错误帧既有形态不动）。
			s.writeDownstreamError(d, entry, &ir.Error{
				Type: "incomplete_stream", Code: upstreamIdleCode, Message: msg, Source: "any2api",
			})
			return true, nil
		}
		if !ok {
			break
		}
		observePartialUsage(upProto, reader.Data(), &partial)
		if frameCarriesContent(upProto, reader.Event(), reader.Data()) {
			idle.touch() // 内容静默守卫：仅内容帧刷新计时（keepalive/空 delta 不算）
		}
		if pipe == nil {
			// 同协议直通的终止证据观测（FD1/W5-23；逐帧、先于任何 break——
			// responses 面的 [DONE] 容忍 break 之前必须已观测到哨兵）。
			// R01（W70-3 二次修）：同一调用返回「本帧是否终止帧」——丢内容帧后
			// 终止帧不转发（判据与理由见 directFrameTerminal）。
			// W87-1（D1）：第二返回值 = 本帧是否上游**流内错误帧**（失败证据）。
			// 字节面零改动（错误帧照旧原样转发）；只落 ledger/健康归因，且每次
			// 尝试只记一次（后续读错误/丢帧路径不重复计——见 errorFrameSeen）。
			var frameError bool
			terminalFrame, frameError = directFrameTerminal(upProto, reader.Event(), reader.Data(), &term, &chatTerm, &respTerm)
			if frameError && !errorFrameSeen {
				errorFrameSeen = true
				failedAttempt = true
				ar.endReason = endReasonUpstreamError
				s.recordFailure(idx, 0)
			}
		} else {
			// W90-1（D1 跨协议半面）：跨协议管道同样要观测上游**流内错误帧**——
			// pipe.Feed 对合法错误事件**不报错**（错误事件是 IR 的 EventError，
			// 转换后返回下游错误帧字节），失败归因面此前因此漏记：ledger/ring
			// 落 completed/delivered=true（W87-1 只修了同协议 pipe == nil 分支）。
			// 观测器与同协议同一份 directFrameTerminal（不复制解析面）：只取
			// errorFrame；终止帧判定在管道面由出口 writer 收尾，本分支不使用。
			// 字节面零改动：错误帧仍照常进 pipe.Feed 并转换写出；每次尝试只记
			// 一次（errorFrameSeen 闸，Feed 错误分支同闸——见下）。
			_, frameError := directFrameTerminal(upProto, reader.Event(), reader.Data(), &term, &chatTerm, &respTerm)
			if frameError && !errorFrameSeen {
				errorFrameSeen = true
				failedAttempt = true
				ar.endReason = endReasonUpstreamError
				s.recordFailure(idx, 0)
			}
		}
		var out []byte
		if pipe == nil {
			// responses 面无 [DONE] 哨兵（topics/openai-wire §4.8：终止事件收束，哨兵是 chat 面
			// 形态）：兼容上游可能补发（new-api 扫描器同款容忍）——容忍 = 视为
			// 流结束，**不转发**下游（透传字节里夹哨兵会让严格客户端提前收流）。
			if entry == registry.FormatOpenAIResponses && isDoneSentinel(reader.Data()) {
				break
			}
			if reader.LostContent() && terminalFrame {
				// R01（W70-3 二次修）：内容帧已被 RESYNC 丢弃 → **终止证据帧不转发**
				// （哨兵 / finish_reason / 终态事件 / finishReason / message_stop）：
				// 唯读这类帧的消费端会当场收流，看不到循环后 LostContent 分支写的
				// incomplete_stream/upstream_frame_too_large 错误尾——那还是假成功。
				// 直接收束，错误尾成为下游看到的**最后**事件；普通内容帧不受影响
				// （照常转发，见 TestFramerResyncE2E / resync_renumber_test）。
				break
			}
			out = reader.frameBytes()
			// W41 条目 3：RESYNC 已丢帧（reader.Resyncs() > 0）→ 后续帧重编号，
			// 保 responses 出口 sequence_number 连续（未丢帧 = 字节保真零改动）。
			if entry == registry.FormatOpenAIResponses && reader.Resyncs() > 0 {
				out = d.renumberResponsesFrame(out)
			}
		} else {
			b, err := pipe.Feed(reader.Event(), reader.Data())
			if err != nil {
				// W90-1：本帧若已作为上游流内错误帧记过一次（上方跨协议观测），
				// Feed 的解析/渲染错误不再重复计健康失败；下游错误行为不变。
				if !errorFrameSeen {
					s.recordFailure(idx, 0)
				}
				failedAttempt = true
				if !committed {
					if e := firstByteErr(up, guard); e != nil {
						return false, e
					}
					return false, fmt.Errorf("upstream %s stream frame rejected: %w", up.Name, err)
				}
				s.writeDownstreamError(d, entry, &ir.Error{
					Type: "api_error", Message: "upstream stream frame rejected: " + err.Error(), Source: "any2api",
				})
				return true, nil
			}
			out = b
			if cgEntry != nil && cgEntry.Terminal() {
				// 终态已见：武装/刷新尾读宽限（终态后的每一帧都刷新——CF 的
				// idle-based window：窗口 = 自最后一帧起的静默时长）。definitive
				// marker（[DONE]/error）到达的帧会在循环尾直接 break——宽限只
				// 兜底「终态/complete 但无流末哨兵」的流（W32-1 方向 B；W33-1
				// 起 message_stream_complete 也走本兜底）。
				cgTail.arm()
			}
		}
		if cgEntry != nil && !moderationHandled {
			// S6 出口面（W32-1）：moderation 拦截（blocked=true）——上游不会给出
			// 正文。出口裁定：不合成正文文案（C 的替换文案是产品内容决策），走
			// **错误帧**（唯一真实载体）：未提交 = 真实状态码信封（403），已提交
			// = 流内错误帧（content_filter；数组形态按既有纪律无错误帧载体——
			// 不闭合数组即失败信号）。本帧不产 IR 事件（out 恒空），检查先于
			// 写出安全。只注入一次；终态后的 moderation 记录不注入（下游已完成，
			// 绝不把成功流改判——只进观测）。真机 moderation 流未采样：条件/形态
			// 为工作裁定 [UNVERIFIED]（报告 §3 S6 出口渲染面）。
			if m := cgEntry.Moderation(); m != nil && m.Blocked && !cgEntry.Terminal() {
				moderationHandled = true
				e := &ir.Error{Type: "content_filter", Code: "moderation_blocked", Message: chatgptWebModerationMessage, Source: "any2api"}
				if committed {
					log.Printf("any2api: chatgpt_web upstream %s: moderation blocked the response: downstream error frame (content_filter)", up.Name)
					s.writeDownstreamError(d, entry, e)
					return true, nil
				}
				log.Printf("any2api: chatgpt_web upstream %s: moderation blocked the response: downstream error envelope (content_filter)", up.Name)
				s.writeError(w, entry, http.StatusForbidden, e)
				return true, nil
			}
		}
		if d.jsonArray {
			// 数组形态（W5-19 条目 2）：先抽元素——无元素（注释/保活帧）不提交；
			// 元素以 `,` 分隔写入（`[` 在 commit 时已写）。
			elems := geminiArrayElements(out)
			if len(elems) == 0 {
				continue
			}
			if !committed {
				d.commit()
				committed = true
				ar.ttft = time.Since(ar.start)
				stopFirstByte(guard) // 提交点：首产出计时窗口关闭
			}
			if err := d.writeArrayElements(elems); err != nil {
				cancel() // 客户端断开 → 取消上游
				return true, nil
			}
			continue
		}
		if !committed {
			if len(out) == 0 {
				continue // 尚无有效产出（如上游 ping）：不提交
			}
			d.commit()
			committed = true
			ar.ttft = time.Since(ar.start)
			stopFirstByte(guard) // 提交点：首产出计时窗口关闭
		}
		if err := d.write(out); err != nil {
			cancel() // 客户端断开 → 取消上游
			return true, nil
		}
		if upProto == registry.FormatClaudeWeb && claudeWebTerminalFrame(reader.Event(), reader.Data()) {
			// claude.ai 网页线（W10）：`message_stop` / `error` 是**终止帧**——
			// 真实上游在终止帧后不主动关闭连接（W9-2 §2.5 实测：流以客户端
			// abort 结束），等 EOF 会把请求挂到连接超时。本帧已喂给管道并写出，
			// 收尾补帧（出口 writer 的 [DONE]/终态）走循环后的 pipe.Finish()。
			break
		}
		if upProto == registry.FormatChatGPTWeb {
			// chatgpt.com 网页线（W30-2 + W32-1 口径修正 + W33-1 裁定收窄）：
			// **definitive marker** 到达即收束——`[DONE]` 哨兵 / 流内 error 帧
			// （raw 面或解析器面；三份 golden 末帧）。`message_stream_complete`
			// **不**收束（W33-1：complete 后仍有 [conversation_detail_metadata]/
			// [DONE] 流末 burst——s4 实机距 complete ≈13ms；流式面由此随流末
			// 哨兵收束并捕获 detail/[DONE]，尾覆盖与非流式路径对齐）。
			// 本帧已喂给管道并写出（error 帧的下游错误帧已在 out 里），收尾补帧
			// 走循环后的 pipe.Finish()。无流末哨兵的终态/complete 由尾读宽限
			// （cgTail）兜底。
			if chatgptWebTerminalFrame(reader.Event(), reader.Data()) || (cgEntry != nil && cgEntry.DefinitiveMarker()) {
				break
			}
		}
	}

	// R01：内容承载的超限帧被 RESYNC 丢弃 = 本尝试**不可信**——先于一切成功面
	// 收尾（含 [DONE]/finish_reason 终止证据、数组闭合、pipe.Finish 补帧）判定。
	// 修前该形态静默走成功路径：下游拿到缺段正文却被宣告 completed/delivered
	// =true（对账面与真实交付不符）。丢弃非内容帧（注释/保活）不受影响（resyncs
	// 日志照旧）。
	if reader.LostContent() {
		s.recordFailure(idx, 0)
		failedAttempt = true
		ar.endReason = endReasonUpstreamError
		if !committed {
			// 下游零字节（提交点未过）→ 换上游（与「零产出 EOF」同路）。
			return false, fmt.Errorf("upstream %s dropped an oversized content frame", up.Name)
		}
		// 已提交：流内错误帧收尾（错误即终态，不补 [DONE]——writeDownstreamError
		// 的哨兵纪律）；delivered 保持 false。
		s.writeDownstreamError(d, entry, &ir.Error{
			Type:    "incomplete_stream",
			Code:    upstreamFrameTooLargeCode,
			Message: "upstream SSE frame exceeded 1 MiB and was dropped",
			Source:  "any2api",
		})
		return true, nil
	}

	// FD1（W5-22 裁决）+ W5-23（三协议判据收尾）：同协议直通缺终止证据的 EOF =
	// 截断流（P11 s8a/s8b 复现：SSE 无错误帧、数组 `]` 闭合——语法合法的假成功），
	// 不得静默当成功。已提交 → 按出口协议写错误帧（SSE）/ 不闭合数组（截断 JSON
	// = 失败信号）；未提交（无任何产出）→ 预提交失败可换上游（与既有 "no events"
	// 同路）。证据面按协议分派（gemini FD1 原判 + W5-23 保守版 openai/responses；
	// claude 面**不**校验——CC B7/B8 条件硬门 + P70 实测容忍矩阵，注入错误帧会把
	// 被容忍的流变硬失败，判据在客户端）。消息与解析器同措辞、类型同
	// incomplete_stream（两路径同构不变量）。
	if pipe == nil {
		var msg string
		switch upProto {
		case registry.FormatGeminiGenerate:
			if !term.terminated() {
				msg = "upstream stream closed before finishReason"
			}
		case registry.FormatOpenAIChat:
			if !chatTerm.terminated() {
				msg = "upstream stream closed before finish_reason"
			}
		case registry.FormatOpenAIResponses:
			if !respTerm.terminated() {
				msg = "upstream stream closed before a terminal response event"
			}
		}
		if msg != "" {
			s.recordFailure(idx, 0)
			failedAttempt = true
			if !committed {
				if e := firstByteErr(up, guard); e != nil {
					return false, e
				}
				return false, fmt.Errorf("upstream %s stream produced no events", up.Name)
			}
			s.writeDownstreamError(d, entry, &ir.Error{
				Type: "incomplete_stream", Message: msg, Source: "any2api",
			})
			return true, nil
		}
	}

	// 上游 EOF：解析器终态 + 渲染器收尾。
	var tail []byte
	if pipe != nil {
		t, err := pipe.Finish()
		tail = t
		if err != nil {
			s.recordFailure(idx, 0)
			failedAttempt = true
			if !committed {
				if e := firstByteErr(up, guard); e != nil {
					return false, e
				}
				return false, fmt.Errorf("upstream %s stream incomplete: %w", up.Name, err)
			}
			s.writeDownstreamError(d, entry, &ir.Error{
				Type: "incomplete_stream", Message: "upstream stream incomplete: " + err.Error(), Source: "any2api",
			})
			return true, nil
		}
	}
	if d.jsonArray {
		elems := geminiArrayElements(tail)
		if !committed && len(elems) == 0 {
			s.recordFailure(idx, 0)
			failedAttempt = true
			if e := firstByteErr(up, guard); e != nil {
				return false, e
			}
			return false, fmt.Errorf("upstream %s stream produced no events", up.Name)
		}
		if !committed {
			d.commit()
			committed = true
			ar.ttft = time.Since(ar.start)
			stopFirstByte(guard)
		}
		if err := d.writeArrayElements(elems); err != nil {
			cancel()
			return true, nil
		}
		if err := d.finishArray(); err != nil {
			cancel()
		} else if !errorFrameSeen {
			// S2（W36-1）：终止产出（数组闭合）完整写出下游 = 交付。
			ar.delivered = true
		}
		if !errorFrameSeen {
			s.recordSuccess(idx)
		}
		return true, nil
	}
	if !committed {
		if len(tail) == 0 {
			s.recordFailure(idx, 0)
			failedAttempt = true
			if e := firstByteErr(up, guard); e != nil {
				return false, e
			}
			return false, fmt.Errorf("upstream %s stream produced no events", up.Name)
		}
		d.commit()
		committed = true
		ar.ttft = time.Since(ar.start)
		stopFirstByte(guard)
	}
	if err := d.write(tail); err != nil {
		cancel()
	} else if !errorFrameSeen {
		// S2（W36-1）：收尾帧（含终态补帧）完整写出下游 = 交付；错误信封/截断/
		// 断连路径不置位（对账面：未交付可辨）。W87-1（D1）：上游流内错误帧同属
		// 未交付——错误帧不是生成结果（与截断路径同口径）。
		ar.delivered = true
	}
	if !errorFrameSeen {
		s.recordSuccess(idx)
	}
	return true, nil
}

// stopFirstByte 关闭首产出计时窗口（nil 安全）。
func stopFirstByte(g *firstByteGuard) {
	if g != nil {
		g.stop()
	}
}

// disconnectEndReason 断连类结束原因的细分（W36-1 S1；解缺口 A8：任务面显式取消
// 此前与下游断开不可分——两者都记 `disconnected`，但审计上必须可分）。
//
//   - 任务面（dr.task 非 nil）且取消已请求 → task_cancel（cancel 端点 → 任务 ctx
//     取消 → 在飞上游中止；客户端并未断开——它在轮询）；
//   - 连接级断裂（broken = 写失败 / 心跳探测失败）→ downstream_disconnect；
//   - 其余（请求 ctx 取消且无写失败）→ client_cancel（客户端中止同步请求）。
//
// 纯日志归因：`disconnected`/`failed` 两字段的取值纪律零改动（本函数只决定
// end_reason 的细分值）。
func disconnectEndReason(dr downstreamRequest, broken bool) string {
	if dr.task != nil && dr.task.cancelRequested() {
		return endReasonTaskCancel
	}
	if broken {
		return endReasonDownstreamDisconnect
	}
	return endReasonClientCancel
}

// ---------------------------------------------------------------------------
// 上游内容静默守卫（假活跃检测；默认关）
// ---------------------------------------------------------------------------

// upstreamIdleCode 提交后内容静默超时的错误码（Slice 2）：relay 的挂住错误帧
// 带上该码（responses 面 = 平铺 error 事件 / response.failed.error 的 code；
// 其余面按各自渲染纪律）。任务面据此把「任务挂住」映射为任务终态 incomplete
// （task.go §失败语义表——挂住 ≠ 失败：可续/可弃）。
const upstreamIdleCode = "upstream_idle"

// upstreamFrameTooLargeCode 内容承载超限帧被丢弃（RESYNC）的错误码（R01）：
// relay 的 incomplete_stream 错误帧带上该码，任务面/对账面据此区分「上游发了
// 超过 1 MiB 的帧且丢了内容」与其它截断形态。
const upstreamFrameTooLargeCode = "upstream_frame_too_large"

// upstreamIdleGuard 上游「内容静默」守卫（W7-1 即落件 A；默认关）：从构造（上游
// 请求发起）起计时，relayStream 每收一个**内容帧**（frameCarriesContent）touch
// 一次；连续 timeout 无内容帧 → 置 timedOut 并取消本次尝试的 ctx（上游请求随之
// 中止）。语义参照 topics/grok-build-wire:75-76 的假活跃防抖：外层 idle_timeout
// 包 `stream.next()`（上游零字节挂住）+ 内层「keepalive/空 delta 不算内容」
// （上游假活跃）——本守卫一个计时器同时覆盖两面（touch 与否即「内容判定」）。
//
// 关闭条件（缺省）：Config.UpstreamIdleTimeoutSeconds <= 0 → 构造返回 nil
// （零开销，行为与引入前完全一致）。与 firstByteGuard 的关系：first_byte 计时窗
// 在**提交点**关闭（提交后慢是心跳面）；本守卫覆盖整个尝试——提交前触发 = 另一
// 形态的首产出失败（换上游）；提交后触发 = 假活跃收尾（`incomplete_stream`
// 错误帧，不重放）。已知边界：终止帧之后的上游连接挂起（不发 EOF）也会在 N 秒后
// 触发——错误帧落在终止帧之后（消费端通常已收流；见报告 §边界）。
type upstreamIdleGuard struct {
	timeout  time.Duration
	last     atomic.Int64 // 最近内容帧时刻（unix nano）
	timedOut atomic.Bool
	stop     func()
}

// newUpstreamIdleGuard 启动守卫；timeout<=0 → nil（关）。cancel = 本次上游尝试的
// ctx 取消函数（超时即取消上游请求）。
func newUpstreamIdleGuard(ctx context.Context, cancel context.CancelFunc, timeout time.Duration) *upstreamIdleGuard {
	if timeout <= 0 {
		return nil
	}
	g := &upstreamIdleGuard{timeout: timeout}
	g.last.Store(time.Now().UnixNano())
	done := make(chan struct{})
	var once sync.Once
	g.stop = func() { once.Do(func() { close(done) }) }
	// 巡检间隔 = timeout/4（下限 100ms、上限 1s）：够准且不空转。守卫只在开启时
	// 存在（缺省关零开销）。
	interval := timeout / 4
	if interval > time.Second {
		interval = time.Second
	}
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if time.Since(time.Unix(0, g.last.Load())) >= g.timeout {
					g.timedOut.Store(true)
					cancel()
					return
				}
			}
		}
	}()
	return g
}

// touch 标记一次内容帧（nil 安全——缺省关时调用方无需分支）。
func (g *upstreamIdleGuard) touch() {
	if g == nil {
		return
	}
	g.last.Store(time.Now().UnixNano())
}

// TimedOut 内容静默超时是否已触发（nil 安全）。
func (g *upstreamIdleGuard) TimedOut() bool { return g != nil && g.timedOut.Load() }

// idleErr 内容静默超时的统一错误（guard 未触发 → nil）：预提交失败路径据此把
// 「上游静默」归因成 idle 超时（failover 日志与测试据此断言），而不是笼统的读失败。
func idleErr(up Upstream, g *upstreamIdleGuard) error {
	if g.TimedOut() {
		return fmt.Errorf("upstream %s idle for %s with no content (idle timeout)", up.Name, g.timeout)
	}
	return nil
}

// errUpstreamIdle 内容静默超时的哨兵（relay 读循环在帧缓冲仍有余量、读不先报错时
// 合成；错误文本由 idleErr / 提交后错误帧分支生成，哨兵只作控制流标记）。
var errUpstreamIdle = errors.New("service: upstream idle (no content-bearing frame)")

// frameCarriesContent 帧是否携带「内容证据」（内容静默守卫的 touch 判据；与
// grok-build-wire:70-76 的内容判定同款纪律：keepalive/ping/空 delta/排队态不算
// 内容）。**保守方向 = 算内容**：认不出的帧（非 JSON、未知协议、interactions）
// 一律算内容——宁可漏报假活跃，不可把合法流误杀（缺省关的守卫，误杀代价更高）。
//
// 判定面（按上游协议；只判帧自身，不做跨帧状态）：
//   - claude_messages：`ping` 事件 / `{"type":"ping"}` 载荷不算；其余算。
//   - openai_chat：空 delta（choices[].delta 无 content/reasoning_content/
//     reasoning/tool_calls/function_call/refusal 且无 finish_reason）不算；
//     usage / error / 内容 delta / finish_reason 算（含空串 content——字段在场
//     即算，保守）。
//   - openai_responses：`response.created|in_progress|queued` 不算（建流/排队
//     态）；其余（output 事件族、终止事件、error）算。
//   - gemini_generate：空帧（无内容 part / finishReason / error / promptFeedback
//     / usageMetadata）不算；其余算。
func frameCarriesContent(upProto registry.Format, event string, data []byte) bool {
	d := bytes.TrimSpace(data)
	if len(d) == 0 {
		return false
	}
	switch upProto {
	case registry.FormatClaudeMessages:
		if event == "ping" {
			return false
		}
		var w struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(d, &w) == nil && w.Type == "ping" {
			return false
		}
		return true
	case registry.FormatOpenAIChat:
		if isDoneSentinel(d) {
			return false
		}
		var w struct {
			Choices []struct {
				Delta struct {
					Content          json.RawMessage `json:"content"`
					ReasoningContent json.RawMessage `json:"reasoning_content"`
					Reasoning        json.RawMessage `json:"reasoning"`
					ToolCalls        json.RawMessage `json:"tool_calls"`
					FunctionCall     json.RawMessage `json:"function_call"`
					Refusal          json.RawMessage `json:"refusal"`
				} `json:"delta"`
				FinishReason json.RawMessage `json:"finish_reason"`
			} `json:"choices"`
			Usage json.RawMessage `json:"usage"`
			Error json.RawMessage `json:"error"`
		}
		if err := json.Unmarshal(d, &w); err != nil {
			return true // 非 JSON：保守算内容
		}
		if nonNullJSON(w.Usage) || nonNullJSON(w.Error) {
			return true
		}
		for _, c := range w.Choices {
			if nonNullJSON(c.FinishReason) ||
				nonNullJSON(c.Delta.Content) || nonNullJSON(c.Delta.ReasoningContent) ||
				nonNullJSON(c.Delta.Reasoning) || nonNullJSON(c.Delta.ToolCalls) ||
				nonNullJSON(c.Delta.FunctionCall) || nonNullJSON(c.Delta.Refusal) {
				return true
			}
		}
		return false
	case registry.FormatOpenAIResponses:
		if isDoneSentinel(d) {
			return false
		}
		var w struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(d, &w) != nil {
			return true // 非 JSON：保守算内容
		}
		switch w.Type {
		case "response.created", "response.in_progress", "response.queued":
			return false
		}
		return true
	case registry.FormatGeminiGenerate:
		var w struct {
			Candidates []struct {
				FinishReason string `json:"finishReason"`
				Content      *struct {
					Parts []json.RawMessage `json:"parts"`
				} `json:"content"`
			} `json:"candidates"`
			PromptFeedback json.RawMessage `json:"promptFeedback"`
			UsageMetadata  json.RawMessage `json:"usageMetadata"`
			Error          json.RawMessage `json:"error"`
		}
		if err := json.Unmarshal(d, &w); err != nil {
			return true // 非 JSON：保守算内容（gemini 流中裸错误行同此）
		}
		if nonNullJSON(w.PromptFeedback) || nonNullJSON(w.UsageMetadata) || nonNullJSON(w.Error) {
			return true
		}
		for _, c := range w.Candidates {
			if c.FinishReason != "" {
				return true
			}
			if c.Content != nil {
				for _, p := range c.Content.Parts {
					if !isTrivialJSON(p) {
						return true
					}
				}
			}
		}
		return false
	case registry.FormatClaudeWeb:
		// claude.ai 网页线：`ping`（保活，JSON 带不定长尾部空格）与
		// `conversation_ready`（新会话就绪信号）不算内容；其余（message_start /
		// content_block_* / message_delta / message_limit / message_stop / error）
		// 算——保守方向（缺省关的守卫，误杀代价更高）。
		if event == "ping" || event == "conversation_ready" {
			return false
		}
		var w struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(d, &w) != nil {
			return true // 非 JSON：保守算内容
		}
		return w.Type != "ping" && w.Type != "conversation_ready"
	}
	return true // interactions / 未知协议：保守算内容
}

// nonNullJSON JSON 值存在且非 null（RawMessage 空 = 字段缺席）。
func nonNullJSON(raw json.RawMessage) bool {
	t := bytes.TrimSpace(raw)
	return len(t) > 0 && !bytes.Equal(t, []byte("null"))
}

// isTrivialJSON 平凡 JSON 值（空对象/空数组/null）——gemini part 的「无内容」形态。
func isTrivialJSON(raw json.RawMessage) bool {
	t := bytes.TrimSpace(raw)
	switch string(t) {
	case "", "null", "{}", "[]":
		return true
	}
	return false
}

// firstByteErr 首产出超时的统一错误（guard 未触发 → nil）：预提交失败路径据此
// 把「上游无产出」归因成首产出超时（failover 日志与测试据此断言），而不是笼统的
// 读失败。
func firstByteErr(up Upstream, g *firstByteGuard) error {
	if g.TimedOut() {
		return fmt.Errorf("upstream %s produced no output within %s (first-byte timeout)", up.Name, g.timeout)
	}
	return nil
}

// consumePipeDiagnostics 消费一次跨协议流式管道的出口诊断（W5-11 §剩余面 5：
// StreamPipe.Diagnostics() 此前无消费方）：计数进服务壳 Diagnostics，任一非零打
// 一行日志（纯观测，绝不进控制流）。ToolArgsDefect 是首个异常块的归因，随日志
// 输出（nil = 全部合法直过）。SignatureDropped 是 W5-19 条目 5 的单列计数
// （无载体签名增量；与批量面 BatchSignatureDropped 同口径）。
func (s *Server) consumePipeDiagnostics(up Upstream, pipe *pairs.StreamPipe) {
	d := pipe.Diagnostics()
	if d.DroppedThinking == 0 && d.ReasoningDeltas == 0 && d.SignatureDropped == 0 && d.ToolArgsRepaired == 0 && d.ToolArgsFailed == 0 {
		return
	}
	s.diag.pipeDroppedThinking.Add(uint64(d.DroppedThinking))
	s.diag.pipeReasoningDeltas.Add(uint64(d.ReasoningDeltas))
	s.diag.pipeSignatureDropped.Add(uint64(d.SignatureDropped))
	s.diag.pipeToolArgsRepaired.Add(uint64(d.ToolArgsRepaired))
	s.diag.pipeToolArgsFailed.Add(uint64(d.ToolArgsFailed))
	defect := ""
	if dd := d.ToolArgsDefect; dd != nil {
		defect = fmt.Sprintf(" defect=%s/%s/%s", dd.Kind, dd.Subkind, dd.RepairLayer)
	}
	log.Printf("any2api: upstream %s stream diagnostics: dropped_thinking=%d reasoning_deltas=%d signature_dropped=%d tool_args_repaired=%d tool_args_failed=%d%s",
		up.Name, d.DroppedThinking, d.ReasoningDeltas, d.SignatureDropped, d.ToolArgsRepaired, d.ToolArgsFailed, defect)
}

// consumeBatchDiagnostics 消费一次**非流式**转换的诊断汇（W5-18 条目 2：批量面
// 此前没有等价于 `StreamPipe.Diagnostics()` 的出口）：计数进服务壳 Diagnostics，
// 任一非零打一行日志（纯观测，绝不进控制流）。face = "request" / "response"
// ——同一尝试内请求面与响应面各一个观测窗口（每次 Convert 一个新鲜 sink）。
// SignatureDropped 是 W5-19 条目 5 的单列计数（思考块签名未落地；与流式面
// PipeSignatureDropped 同口径）。
func (s *Server) consumeBatchDiagnostics(up Upstream, face string, d *pairs.BatchDiagnostics) {
	if d == nil || (d.DroppedThinking == 0 && d.ReasoningMapped == 0 && d.SignatureDropped == 0) {
		return
	}
	s.diag.batchDroppedThinking.Add(uint64(d.DroppedThinking))
	s.diag.batchReasoningMapped.Add(uint64(d.ReasoningMapped))
	s.diag.batchSignatureDropped.Add(uint64(d.SignatureDropped))
	log.Printf("any2api: upstream %s batch diagnostics (%s face): dropped_thinking=%d reasoning_mapped=%d signature_dropped=%d",
		up.Name, face, d.DroppedThinking, d.ReasoningMapped, d.SignatureDropped)
}

// writeDownstreamError 已提交后中途失败：按入口协议写错误帧收尾。
//
// 数组形态（W5-19 条目 2）：no-op——数组里没有合法的错误帧表达（SSE 错误帧不是
// JSON）；也不闭合数组（截断的 JSON 是客户端可见的失败，闭合会被误读为正常
// 完成）。预提交失败不受影响（未提交 → 调用方走 writeError 真实状态码信封）。
//
// 哨兵纪律（W5-22 FD2 裁决 + W5-26 修订）：按各出口格式契约分派——
// **openai chat 面错误帧后不补 `data: [DONE]`**（W5-26 实测裁决改判：原 vLLM
// 形态「错误不拆流」会被哨兵唯读的消费端当正常完成；证据
// `tmp/lanes/L19-sweep9/proofs/done-after-error.txt` 见报告 §关键发现 2）；gemini 面**绝不发哨兵**
// （表 §流式「流中错误」行 + 陷阱表「给 Gemini 出口发哨兵=协议污染」——此前
// 落到 default 分支被误补，P11 S11a 录帧为证）；claude/responses 面无哨兵
// （各自事件收束）。
func (s *Server) writeDownstreamError(d *downstream, entry registry.Format, e *ir.Error) {
	if d.jsonArray {
		return
	}
	if entry == registry.FormatOpenAIResponses {
		// Responses：平铺 `error` 事件 + `response.failed`（表 §错误信封/§4.5），
		// **无** `[DONE]` 哨兵——Responses 以终止事件收束（哨兵是 chat 面形态；
		// new-api 的 Fail() 同款只发这两个事件）。
		d.writeResponsesError(e)
		return
	}
	body, err := pairs.RenderErrorFor(entry, e)
	if err != nil {
		return
	}
	switch entry {
	case registry.FormatClaudeMessages:
		// `event: error` + 标准信封（CC B3：错误体是唯一信息来源）。
		var b bytes.Buffer
		b.WriteString("event: error\ndata: ")
		b.Write(body)
		b.WriteString("\n\n")
		_ = d.write(b.Bytes())
	case registry.FormatGeminiGenerate:
		// Gemini 流内错误帧 = `data: {"error":{…}}`；错误即终止、**无哨兵**
		// （W5-22 FD2：与「Gemini 出口绝不发哨兵」对齐——此前误走 default 分支）。
		var b bytes.Buffer
		b.WriteString("data: ")
		b.Write(body)
		b.WriteString("\n\n")
		_ = d.write(b.Bytes())
	default:
		// OpenAI 流内错误帧（嵌套信封）。**错误帧后不补 `[DONE]`**（W5-26 实测
		// 裁决，Bifrost #5546 形态；证据 `tmp/lanes/L19-sweep9/proofs/
		// done-after-error.txt`：哨兵唯读的消费端会把「错误帧 + [DONE]」当正常
		// 完成——补哨兵 = 把截断流伪装成成功；错误帧即终态）。
		var b bytes.Buffer
		b.WriteString("data: ")
		b.Write(body)
		b.WriteString("\n\n")
		_ = d.write(b.Bytes())
	}
}

// writeResponsesError responses 面的中途错误收尾帧：平铺 `error` 事件 +
// `response.failed`（表 §错误信封/§4.5），**无** `[DONE]` 哨兵。续号取自已下发
// 事件、标识取首个 response 对象（观测状态在写锁内读——心跳 goroutine 并发写）。
func (d *downstream) writeResponsesError(e *ir.Error) {
	if e == nil {
		e = &ir.Error{Type: "api_error", Message: "stream error", Source: "any2api"}
	}
	code := e.Code
	if code == "" {
		code = "server_error" // 官方错误码缺省（new-api Fail() 同款；与库层
		// responses.StreamWriter 的 EventError 缺省同值——W5-20 条目 4 的
		// 两路径同构不变量）
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.broken != nil {
		return
	}
	flat := responsesErrorEventWire{
		Type: "error", Code: code, Message: e.Message, Param: e.Param,
		SequenceNumber: d.seq,
	}
	failed := responsesFailedEventWire{Type: "response.failed", SequenceNumber: d.seq + 1}
	failed.Response.ID = d.respID
	failed.Response.Object = "response"
	failed.Response.CreatedAt = d.respCreated
	failed.Response.Status = "failed"
	failed.Response.Model = d.respModel
	failed.Response.Output = []json.RawMessage{}
	failed.Response.Error = responsesErrorObjectWire{Code: code, Message: e.Message}

	flatData, err1 := json.Marshal(flat)
	failedData, err2 := json.Marshal(failed)
	if err1 != nil || err2 != nil {
		return // 载荷全为标量——Marshal 失败 = 编程错误；绝不写半截帧
	}
	var buf bytes.Buffer
	buf.WriteString("event: error\ndata: ")
	buf.Write(flatData)
	buf.WriteString("\n\n")
	buf.WriteString("event: response.failed\ndata: ")
	buf.Write(failedData)
	buf.WriteString("\n\n")

	d.seq += 2
	_ = d.writeLocked(buf.Bytes())
}

// responsesErrorEventWire 流内错误事件（平铺——**无嵌套 error 对象**；表 §错误信封）。
type responsesErrorEventWire struct {
	Type           string `json:"type"`
	Code           string `json:"code"`
	Message        string `json:"message"`
	Param          string `json:"param,omitempty"`
	SequenceNumber int64  `json:"sequence_number"`
}

// responsesErrorObjectWire 终止失败事件的 response.error（ResponseError{code,message}）。
type responsesErrorObjectWire struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// responsesFailedEventWire 终止失败事件（表 §4.5：response.failed + response.error；
// usage 恒 null——表 §4.4：usage 只在正常终止非 null）。
type responsesFailedEventWire struct {
	Type     string `json:"type"`
	Response struct {
		ID        string                   `json:"id"`
		Object    string                   `json:"object"`
		CreatedAt int64                    `json:"created_at"`
		Status    string                   `json:"status"`
		Model     string                   `json:"model"`
		Output    []json.RawMessage        `json:"output"`
		Error     responsesErrorObjectWire `json:"error"`
		Usage     any                      `json:"usage"`
	} `json:"response"`
	SequenceNumber int64 `json:"sequence_number"`
}

// isDoneSentinel data 载荷是否 chat 面 [DONE] 哨兵。
func isDoneSentinel(data []byte) bool {
	return bytes.Equal(bytes.TrimSpace(data), []byte("[DONE]"))
}
