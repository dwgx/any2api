// W53-2 凭据池测试：单元（选择/冷却/耗尽/标记）+ 集成（假 claude_web 上游
// 全链：坏键回退、配额轮换、冷却跳过、并发无串扰、单键兼容、不泄漏）。
//
// W54-3 增量：完成面回喂（completion 429 → 池冷却 → 后续请求避开）+ attempt
// 记录凭据索引（含 0 值不丢）+ 管理面 credential_cooldown 事件（独立型别）。
//
// 装置：poolMock（假 claude_web 上游，**按 cookie 里的 sessionKey 分档**响应
// ——键编号从 `-<n>` 后缀解析；会话 UUID 编码创建者编号，completion/DELETE 的
// cookie 与所有者不匹配即 403 = 账号错配检测）+ 真服务（New(cfg).Handler() 经
// httptest.NewServer）+ 真 HTTP 客户端。
//
// 反证锚点（本文件）：
//   - 撤掉 claudeWebPrepare 的换号重试循环 → TestClaudeWebPoolBadKeyFallback 红
//     （坏键 401 直接上抛，下游 502）；
//   - 撤掉 reportFailure/冷却 → TestClaudeWebPoolQuotaRotation 红（第二发仍打
//     429 键）；
//   - 撤掉 exhaustedError（改为放行）→ TestClaudeWebPoolExhaustedFastFail 红；
//   - 把 completion 的标记解析换成固定 up.APIKey → TestClaudeWebPoolBadKeyFallback
//     红（completion cookie = 坏键 → mock 403）且
//     TestClaudeWebPoolConcurrentNoCrosstalk 红（串扰计数 > 0）；
//   - 把 reportFailure/retry 日志里的索引换成凭据值 →
//     TestClaudeWebPoolNoCredentialLeak 红；
//   - 撤掉 forward 的完成面回喂（feedCompletionCredentialFailure 调用）→
//     TestClaudeWebPoolCompletionFeedBack 红（无 `credential #0 cooling` 冷却
//     日志；且第三发 LRU 回到冷却中的键 0 → completion 429 → 非 200）；
//   - 撤掉 forward 的 credentialIndex 回填 → TestClaudeWebPoolAttemptCredentialIndex
//     红（字段缺失或值错）；
//   - 撤掉 New 的 setEventSink 装配 → TestClaudeWebPoolCooldownAdminEvent 红
//     （adminLog 无 credential_cooldown 事件）。
package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 脚手架：按凭据分档的假 claude_web 上游
// ---------------------------------------------------------------------------

// poolCall 一次上游调用记录（测试断言用；生产面绝不记录凭据——本装置仅测试）。
type poolCall struct {
	method  string
	path    string
	key     string // cookie 的 sessionKey 值（空 = 未带）
	referer string
}

// poolMock 假 claude_web 上游（按凭据分档）。
type poolMock struct {
	t   *testing.T
	srv *httptest.Server

	mu    sync.Mutex
	calls []poolCall
	// crosstalk 账号错配计数（completion/DELETE 的 cookie 编号 ≠ 会话所有者）。
	crosstalk int

	// orgStatusByIndex 该编号键的 org 发现状态码（缺省 200；>=400 走失败体）。
	orgStatusByIndex map[int]int
	// retryAfterByIndex 失败时的 retry-after 头（可选）。
	retryAfterByIndex map[int]string
	// compStatusByIndex 该编号键的 completion 状态码（缺省 200 + SSE）。
	compStatusByIndex map[int]int
	// streamBody completion 成功体（SSE）。
	streamBody string
}

// poolKeyIndex 键编号：`…-<n>` 后缀（非数字后缀 = -1——「垃圾键」档）。
func poolKeyIndex(key string) int {
	i := strings.LastIndexByte(key, '-')
	if i < 0 {
		return -1
	}
	n, err := strconv.Atoi(key[i+1:])
	if err != nil {
		return -1
	}
	return n
}

// poolConvUUID 会话 UUID：编码创建者键编号（36 字符、hex+dash——服务侧
// isSafePathSegment 形态守卫要求）。
func poolConvUUID(n int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", n)
}

// poolConvOwner 从路径提取会话 UUID 并解出创建者编号（无 → -1）。
func poolConvOwner(path string) int {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	for i, s := range segs {
		if s == "chat_conversations" && i+1 < len(segs) {
			conv := segs[i+1]
			if len(conv) != 36 {
				return -1
			}
			n, err := strconv.Atoi(conv[24:])
			if err != nil {
				return -1
			}
			return n
		}
	}
	return -1
}

func newPoolMock(t *testing.T) *poolMock {
	t.Helper()
	m := &poolMock{
		t:                 t,
		orgStatusByIndex:  map[int]int{},
		retryAfterByIndex: map[int]string{},
		compStatusByIndex: map[int]int{},
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.srv.Close)
	return m
}

func (m *poolMock) handle(w http.ResponseWriter, r *http.Request) {
	key := ""
	if c := r.Header.Get("Cookie"); c != "" {
		if v, ok := strings.CutPrefix(c, "sessionKey="); ok {
			key = v
		}
	}
	m.mu.Lock()
	m.calls = append(m.calls, poolCall{method: r.Method, path: r.URL.Path, key: key, referer: r.Header.Get("Referer")})
	m.mu.Unlock()
	idx := poolKeyIndex(key)

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/api/organizations":
		if st := m.orgStatusByIndex[idx]; st >= 400 {
			if ra := m.retryAfterByIndex[idx]; ra != "" {
				w.Header().Set("retry-after", ra)
			}
			w.WriteHeader(st)
			_, _ = w.Write([]byte(`{"error":{"message":"sessionKey invalid"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `[{"id":1,"uuid":%q,"name":"Test","rate_limit_tier":"default_claude_ai"}]`, claudeWebOrgUUID)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/chat_conversations"):
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(w, `{"uuid":%q}`, poolConvUUID(idx))
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/completion"):
		if owner := poolConvOwner(r.URL.Path); owner >= 0 && owner != idx {
			m.mu.Lock()
			m.crosstalk++
			m.mu.Unlock()
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"message":"conversation belongs to another account"}}`))
			return
		}
		if st := m.compStatusByIndex[idx]; st >= 400 {
			if ra := m.retryAfterByIndex[idx]; ra != "" {
				w.Header().Set("retry-after", ra)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(st)
			_, _ = w.Write([]byte(`{"error":{"message":"rate limited"}}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, m.streamBody)
	case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/chat_conversations/"):
		if owner := poolConvOwner(r.URL.Path); owner >= 0 && owner != idx {
			m.mu.Lock()
			m.crosstalk++
			m.mu.Unlock()
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (m *poolMock) snapshot() []poolCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]poolCall(nil), m.calls...)
}

func (m *poolMock) crosstalkCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.crosstalk
}

// waitPoolCalls 有界等待上游调用数达到 want。
func waitPoolCalls(t *testing.T, m *poolMock, want int) []poolCall {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		calls := m.snapshot()
		if len(calls) >= want {
			return calls
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待上游调用数 %d 超时，已见 %d 条", want, len(calls))
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// assertNoMorePoolCalls 宽限期内调用数恒为 want（负断言——防「错误地多发」）。
func assertNoMorePoolCalls(t *testing.T, m *poolMock, want int) {
	t.Helper()
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := len(m.snapshot()); got != want {
			t.Fatalf("上游调用数 = %d, want %d（不得多发）", got, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// lockedBuffer 并发安全日志缓冲（W54-3）：服务侧 goroutine（清理面等）可能在
// 测试读取时仍在写日志——直接读 bytes.Buffer 会与 log 包写者竞态。
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

// lastAttemptsLine 取日志里最后一条 attempts 行（测试断言用；无 → t.Fatalf）。
func lastAttemptsLine(t *testing.T, out string) string {
	t.Helper()
	idx := strings.LastIndex(out, "attempts: ")
	if idx < 0 {
		t.Fatalf("日志无 attempts 行:\n%s", out)
	}
	line := out[idx+len("attempts: "):]
	if nl := strings.IndexByte(line, '\n'); nl >= 0 {
		line = line[:nl]
	}
	return line
}

// attemptCredentialIndex 解析 attempt 行首个 attempt 的 credential_index。
// 返回 *int（nil = 字段缺失——区分「0」与「缺失」，omitempty 陷阱锚点）。
func attemptCredentialIndex(t *testing.T, line string) *int {
	t.Helper()
	var w struct {
		Attempts []struct {
			CredentialIndex *int `json:"credential_index"`
		} `json:"attempts"`
	}
	if err := json.Unmarshal([]byte(line), &w); err != nil {
		t.Fatalf("attempt 行解析: %v\n%s", err, line)
	}
	if len(w.Attempts) == 0 {
		t.Fatalf("attempts 为空: %s", line)
	}
	return w.Attempts[0].CredentialIndex
}

// cfgClaudeWebPool 服务配置：claude_web 上游 + credentials 池（无 api_key）。
func cfgClaudeWebPool(baseURL string, creds ...string) *Config {
	return &Config{
		Listen:           "127.0.0.1:0",
		HeartbeatSeconds: 15,
		Upstreams: []Upstream{{
			Name: "fake-claude-web-pool", BaseURL: baseURL,
			Credentials: creds,
			Protocol:    ProtocolClaudeWeb,
		}},
	}
}

// postChat 发一发轻 completion 并返回（状态码, 体）。
func postChat(t *testing.T, srv *httptest.Server) (int, []byte) {
	t.Helper()
	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// assertPONG 下游体应为非流式 chat.completion（内容 PONG）。
func assertPONG(t *testing.T, status int, raw []byte) {
	t.Helper()
	if status != 200 {
		t.Fatalf("status=%d body=\n%s", status, raw)
	}
	var out struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("下游体: %v\n%s", err, raw)
	}
	if len(out.Choices) != 1 || out.Choices[0].Message.Content != "PONG" {
		t.Fatalf("下游内容 = %s, want PONG", raw)
	}
}

// ---------------------------------------------------------------------------
// 单元：选择顺序 / 冷却 / 耗尽 / 标记
// ---------------------------------------------------------------------------

func poolForTest(creds ...string) *credentialPool {
	return newCredentialPool(Upstream{Name: "u", Protocol: ProtocolClaudeWeb, Credentials: creds})
}

// TestCredentialPoolRotationOrder 选择 = 最少近期使用：全未用 → 0,1,2 轮转；
// 已用过的先出列（0,1,2,0…）。反证：选择若固定取 0（或随机），序列即红。
func TestCredentialPoolRotationOrder(t *testing.T) {
	p := poolForTest("a", "b", "c")
	base := time.Unix(1000, 0)
	want := []int{0, 1, 2, 0, 1, 2}
	for i, w := range want {
		got, ok := p.selectCredential(base.Add(time.Duration(i) * time.Second))
		if !ok || got != w {
			t.Fatalf("第 %d 次选择 = %d(ok=%v), want %d", i, got, ok, w)
		}
	}
	// 冷却中的键跳过：冷却 0 号 → 序列跳过 0（此时 LRU = 0，被冷却 → 选 1）。
	p.reportFailure(0, credFailureQuota, 0, base.Add(10*time.Second))
	got, ok := p.selectCredential(base.Add(11 * time.Second))
	if !ok || got != 1 {
		t.Fatalf("冷却 0 后选择 = %d(ok=%v), want 1", got, ok)
	}
}

// TestCredentialPoolCooldownSkipAndExpiry 配额冷却（60s 缺省）与凭据冷却（30m）
// 生效/到期；冷却取较晚者（不缩短）。
func TestCredentialPoolCooldownSkipAndExpiry(t *testing.T) {
	p := poolForTest("a", "b")
	base := time.Unix(2000, 0)
	if i, _ := p.selectCredential(base); i != 0 {
		t.Fatalf("首发 = %d, want 0", i)
	}
	// 配额类（无 retry-after）→ 60s。
	p.reportFailure(0, credFailureQuota, 0, base)
	if i, _ := p.selectCredential(base.Add(59 * time.Second)); i != 1 {
		t.Fatalf("冷却中应跳过 0，得到 %d", i)
	}
	if i, _ := p.selectCredential(base.Add(61 * time.Second)); i != 0 {
		t.Fatalf("冷却到期应回到 0，得到 %d", i)
	}
	// 凭据类 → 30m（更长）；再叠加一条更短的配额冷却不缩短它。
	p.reportFailure(0, credFailureCredential, 0, base.Add(61*time.Second))
	p.reportFailure(0, credFailureQuota, 0, base.Add(61*time.Second))
	if i, _ := p.selectCredential(base.Add(30 * time.Minute)); i != 1 {
		t.Fatalf("凭据冷却未到期应跳过 0，得到 %d", i)
	}
	if i, _ := p.selectCredential(base.Add(61*time.Second + 31*time.Minute)); i != 0 {
		t.Fatalf("凭据冷却到期应回到 0，得到 %d", i)
	}
	// retry-after 精确执行（429 + retry-after: 7 → 7s 后即可用）。
	p2 := poolForTest("a", "b")
	p2.reportFailure(0, credFailureQuota, 7*time.Second, base)
	if i, _ := p2.selectCredential(base.Add(8 * time.Second)); i != 0 {
		t.Fatalf("retry-after 到期应回到 0，得到 %d", i)
	}
}

// TestCredentialPoolRetryAfterCapped retry-after 超上限（maxRetryAfterCap=10m）
// 截断——上游给离谱值不把凭据长期拉黑。
func TestCredentialPoolRetryAfterCapped(t *testing.T) {
	p := poolForTest("a", "b")
	base := time.Unix(3000, 0)
	p.reportFailure(0, credFailureQuota, time.Hour, base)
	if i, _ := p.selectCredential(base.Add(11 * time.Minute)); i != 0 {
		t.Fatalf("retry-after 超限应截到 10m（11m 已到期），得到 %d", i)
	}
}

// TestCredentialPoolExhaustion 全部冷却 → ok=false；最早解冻后回归。
func TestCredentialPoolExhaustion(t *testing.T) {
	p := poolForTest("a", "b")
	base := time.Unix(4000, 0)
	p.reportFailure(0, credFailureQuota, 0, base)
	p.reportFailure(1, credFailureCredential, 0, base)
	if _, ok := p.selectCredential(base.Add(time.Second)); ok {
		t.Fatalf("全部冷却中不得选出凭据")
	}
	if _, ok := p.selectCredential(base.Add(61 * time.Second)); !ok {
		t.Fatalf("最早解冻（60s）后应可选出")
	}
	if err := p.exhaustedError(base); err == nil || !strings.Contains(err.Error(), "upstream_credentials_exhausted") {
		t.Fatalf("耗尽错误 = %v", err)
	}
}

// TestSplitCredentialMarker 标记解析：无标记原样；有效标记剥离；畸形显式报错。
func TestSplitCredentialMarker(t *testing.T) {
	if bare, _, ok, err := splitCredentialMarker("https://claude.ai/new"); ok || err != nil || bare != "https://claude.ai/new" {
		t.Fatalf("无标记: bare=%q ok=%v err=%v", bare, ok, err)
	}
	bare, idx, ok, err := splitCredentialMarker("https://claude.ai/new" + credentialMarkerPrefix + "3")
	if !ok || err != nil || idx != 3 || bare != "https://claude.ai/new" {
		t.Fatalf("有效标记: bare=%q idx=%d ok=%v err=%v", bare, idx, ok, err)
	}
	if _, _, ok, err := splitCredentialMarker("https://claude.ai/new" + credentialMarkerPrefix + "x"); !ok || err == nil {
		t.Fatalf("畸形标记应显式报错（ok=%v err=%v）", ok, err)
	}
}

// TestCredentialPoolLogIndexOnly reportFailure 日志只带索引（绝不含凭据字节）。
func TestCredentialPoolLogIndexOnly(t *testing.T) {
	var buf bytes.Buffer
	old := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(old)

	const secret = "sk-ant-sid01-W53UNITLOG-SECRET"
	p := poolForTest(secret, "sk-ant-sid01-W53UNITLOG-OTHER")
	p.reportFailure(0, credFailureCredential, 0, time.Unix(5000, 0))
	out := buf.String()
	if strings.Contains(out, "W53UNITLOG-SECRET") {
		t.Fatalf("日志泄漏凭据字节:\n%s", out)
	}
	if !strings.Contains(out, "credential #0") {
		t.Fatalf("日志应带索引:\n%s", out)
	}
}

// TestCredentialPoolCompletionClassify 完成面失败分类（W54-3）：429 → 配额类
// （retry-after 精确执行）；401 / 无盾 403 → 凭据类；带盾 403 / 其余 → 不处置。
func TestCredentialPoolCompletionClassify(t *testing.T) {
	if cls, ra, ok := credentialClassForStatus(http.StatusTooManyRequests, http.Header{}); !ok || cls != credFailureQuota || ra != 0 {
		t.Fatalf("429 无头: cls=%v ra=%v ok=%v", cls, ra, ok)
	}
	if cls, ra, ok := credentialClassForStatus(http.StatusTooManyRequests, http.Header{"Retry-After": []string{"7"}}); !ok || cls != credFailureQuota || ra != 7*time.Second {
		t.Fatalf("429+retry-after:7: cls=%v ra=%v ok=%v", cls, ra, ok)
	}
	if cls, _, ok := credentialClassForStatus(http.StatusUnauthorized, http.Header{}); !ok || cls != credFailureCredential {
		t.Fatalf("401: cls=%v ok=%v", cls, ok)
	}
	if cls, _, ok := credentialClassForStatus(http.StatusForbidden, http.Header{}); !ok || cls != credFailureCredential {
		t.Fatalf("403 无盾: cls=%v ok=%v", cls, ok)
	}
	if _, _, ok := credentialClassForStatus(http.StatusForbidden, http.Header{"Cf-Mitigated": []string{"challenge"}}); ok {
		t.Fatalf("带盾 403 不应处置（不是凭据信号）")
	}
	if _, _, ok := credentialClassForStatus(http.StatusInternalServerError, http.Header{}); ok {
		t.Fatalf("5xx 不应处置")
	}
}

// ---------------------------------------------------------------------------
// 集成 1：坏键失败 → 池内换号回退真键 → 200（本单核心）
// ---------------------------------------------------------------------------

func TestClaudeWebPoolBadKeyFallback(t *testing.T) {
	m := newPoolMock(t)
	m.orgStatusByIndex[-1] = http.StatusUnauthorized // 垃圾键（编号 -1）→ 401
	m.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	const badKey, goodKey = "sk-ant-sid01-pool-badkey", "sk-ant-sid01-pool-key-0"
	srv := startService(t, cfgClaudeWebPool(m.srv.URL, badKey, goodKey))

	status, raw := postChat(t, srv)
	assertPONG(t, status, raw)

	// 调用序：org(坏键 401) → org(好键 200) → 会话创建(好键) → completion(好键)
	// → DELETE(好键——清理面必须用创建会话的凭据)。等 5 条（DELETE 异步）。
	calls := waitPoolCalls(t, m, 5)
	want := []struct {
		method string
		key    string
	}{
		{http.MethodGet, badKey},
		{http.MethodGet, goodKey},
		{http.MethodPost, goodKey},
		{http.MethodPost, goodKey},
		{http.MethodDelete, goodKey},
	}
	if len(calls) != 5 {
		t.Fatalf("调用数 = %d, want 5: %+v", len(calls), calls)
	}
	for i, w := range want {
		if calls[i].method != w.method || calls[i].key != w.key {
			t.Fatalf("调用[%d] = %s key=%q, want %s key=%q", i, calls[i].method, calls[i].key, w.method, w.key)
		}
	}
	if !strings.HasSuffix(calls[2].path, "/chat_conversations") || !strings.HasSuffix(calls[3].path, "/completion") {
		t.Fatalf("调用[2]/[3] 路径 = %s / %s", calls[2].path, calls[3].path)
	}
	// completion 的 Referer 必须是**剥离标记后的原值**（wire 与引入前逐字节一致）。
	if got := calls[3].referer; got != m.srv.URL+"/new" {
		t.Fatalf("completion Referer = %q, want %q（标记必须剥离）", got, m.srv.URL+"/new")
	}
	if m.crosstalkCount() != 0 {
		t.Fatalf("账号错配计数 = %d, want 0", m.crosstalkCount())
	}
}

// TestClaudeWebPoolQuotaRotation 配额类（429 + retry-after: 7）→ 换号重试；
// 冷却生效：第二发请求直接走好键（不再打 429 键）。
func TestClaudeWebPoolQuotaRotation(t *testing.T) {
	m := newPoolMock(t)
	m.orgStatusByIndex[0] = http.StatusTooManyRequests
	m.retryAfterByIndex[0] = "7"
	m.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	const limited, good = "sk-ant-sid01-pool-key-0", "sk-ant-sid01-pool-key-1"
	srv := startService(t, cfgClaudeWebPool(m.srv.URL, limited, good))

	status, raw := postChat(t, srv)
	assertPONG(t, status, raw)
	calls := waitPoolCalls(t, m, 5)
	// 第一发：429(键 0) → org(键 1) → 会话 → completion → DELETE。
	if calls[0].key != limited || calls[1].key != good || calls[3].key != good {
		t.Fatalf("首发调用序 = %+v", calls)
	}

	// 第二发：冷却中（7s 内）→ 直接走键 1（org 首发即键 1，无 429 命中）。
	status2, raw2 := postChat(t, srv)
	assertPONG(t, status2, raw2)
	calls = waitPoolCalls(t, m, 9) // 第一发 5 条（429 换号）+ 第二发 4 条
	if calls[5].method != http.MethodGet || calls[5].key != good {
		t.Fatalf("第二发 org 调用 = %s key=%q, want GET %q（冷却跳过 429 键）", calls[5].method, calls[5].key, good)
	}
	for _, c := range calls[5:] {
		if c.key == limited {
			t.Fatalf("冷却期内不得再打受限键: %+v", c)
		}
	}
}

// TestClaudeWebPoolExhaustedFastFail 单坏键：第一发（本轮试尽）→ 上游 401 上抛；
// 第二发（全部冷却中）→ upstream_credentials_exhausted 显式报错，**不再打上游**。
func TestClaudeWebPoolExhaustedFastFail(t *testing.T) {
	m := newPoolMock(t)
	m.orgStatusByIndex[-1] = http.StatusUnauthorized
	srv := startService(t, cfgClaudeWebPool(m.srv.URL, "sk-ant-sid01-pool-badkey"))

	status1, raw1 := postChat(t, srv)
	if status1 == 200 {
		t.Fatalf("坏键应失败: %s", raw1)
	}
	if !strings.Contains(string(raw1), "sessionKey invalid") {
		t.Fatalf("第一发错误应带上游语义: %s", raw1)
	}
	calls := waitPoolCalls(t, m, 1)

	status2, raw2 := postChat(t, srv)
	if status2 == 200 {
		t.Fatalf("冷却中应显式失败: %s", raw2)
	}
	if !strings.Contains(string(raw2), "upstream_credentials_exhausted") {
		t.Fatalf("第二发错误应带耗尽语义: %s", raw2)
	}
	assertNoMorePoolCalls(t, m, len(calls)) // 冷却期内零上游调用
}

// TestClaudeWebPoolRotationAcrossRequests 多请求轮转序（键 0,1,0）。
func TestClaudeWebPoolRotationAcrossRequests(t *testing.T) {
	m := newPoolMock(t)
	m.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	const k0, k1 = "sk-ant-sid01-pool-key-0", "sk-ant-sid01-pool-key-1"
	srv := startService(t, cfgClaudeWebPool(m.srv.URL, k0, k1))

	want := []string{k0, k1, k0}
	for i, w := range want {
		status, raw := postChat(t, srv)
		assertPONG(t, status, raw)
		calls := waitPoolCalls(t, m, (i+1)*4)
		if calls[i*4].key != w {
			t.Fatalf("第 %d 发 org 键 = %q, want %q（轮转序）", i+1, calls[i*4].key, w)
		}
		if calls[i*4+2].key != w {
			t.Fatalf("第 %d 发 completion 键 = %q, want %q", i+1, calls[i*4+2].key, w)
		}
	}
}

// TestClaudeWebPoolConcurrentNoCrosstalk 并发 8 发：每发选定键与 completion 键
// 必须同号（mock 强校验）；零串扰。
func TestClaudeWebPoolConcurrentNoCrosstalk(t *testing.T) {
	m := newPoolMock(t)
	m.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	creds := []string{"sk-ant-sid01-pool-key-0", "sk-ant-sid01-pool-key-1", "sk-ant-sid01-pool-key-2", "sk-ant-sid01-pool-key-3"}
	srv := startService(t, cfgClaudeWebPool(m.srv.URL, creds...))

	const n = 8
	var wg sync.WaitGroup
	errs := make([]string, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 注意：goroutine 内绝不调 t.Fatalf（只收集错误，主 goroutine 断言）。
			resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
				strings.NewReader(`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"hi"}]}`))
			if err != nil {
				errs[i] = fmt.Sprintf("req%d: %v", i, err)
				return
			}
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != 200 {
				errs[i] = fmt.Sprintf("req%d: status=%d body=%s", i, resp.StatusCode, raw)
			}
		}(i)
	}
	wg.Wait()
	for _, e := range errs {
		if e != "" {
			t.Fatalf("并发请求失败: %s", e)
		}
	}
	if got := m.crosstalkCount(); got != 0 {
		t.Fatalf("账号错配计数 = %d, want 0（并发下 attempt 局部选定）", got)
	}
	// 全部 completion 调用的键必须与所属会话同号（mock 校验的显式复述）。
	for _, c := range m.snapshot() {
		if strings.HasSuffix(c.path, "/completion") {
			if owner := poolConvOwner(c.path); owner != poolKeyIndex(c.key) {
				t.Fatalf("completion 键错配: path=%s key=%q", c.path, c.key)
			}
		}
	}
}

// TestClaudeWebPoolCompletionFailureNoSwitch 边界（已产出/准备后不换号）：准备
// 成功、completion 429——completion 面**不做**池内换号重试（server.go 冻结面；
// 单上游时按既有语义上抛），且不产生第二个凭据的任何调用。W54-3：失败只**冷却**
// （回喂断言见 TestClaudeWebPoolCompletionFeedBack）——不换号纪律不变。
func TestClaudeWebPoolCompletionFailureNoSwitch(t *testing.T) {
	m := newPoolMock(t)
	m.compStatusByIndex[0] = http.StatusTooManyRequests
	const k0, k1 = "sk-ant-sid01-pool-key-0", "sk-ant-sid01-pool-key-1"
	srv := startService(t, cfgClaudeWebPool(m.srv.URL, k0, k1))

	status, raw := postChat(t, srv)
	if status == 200 {
		t.Fatalf("completion 429 应失败: %s", raw)
	}
	calls := waitPoolCalls(t, m, 4) // org/conv/completion/DELETE（键 0）
	for _, c := range calls {
		if c.key == k1 {
			t.Fatalf("completion 失败后不得换号（键 1 被调用）: %+v", c)
		}
	}
	completions := 0
	for _, c := range calls {
		if strings.HasSuffix(c.path, "/completion") {
			completions++
		}
	}
	if completions != 1 {
		t.Fatalf("completion 调用数 = %d, want 1（不重试换号）", completions)
	}
}

// TestClaudeWebPoolCompletionFeedBack 完成面回喂（W54-3 残余 1）：completion
// 429（+retry-after: 120）→ 该凭据冷却（**不换号重试**）；后续请求避开冷却键。
//
// 判别点 = **第三发**：第一发后 LRU 自然轮到键 1（第二发不判别）；第三发无回喂
// 时 LRU 会回到键 0（其 completion 仍 429 → 失败），有回喂时键 0 仍在冷却 →
// 必须走键 1 → 200。
func TestClaudeWebPoolCompletionFeedBack(t *testing.T) {
	var buf lockedBuffer
	old := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(old)

	m := newPoolMock(t)
	m.compStatusByIndex[0] = http.StatusTooManyRequests
	m.retryAfterByIndex[0] = "120" // 完成面 retry-after 精确执行（120s ≠ 缺省 60s）
	m.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	const k0, k1 = "sk-ant-sid01-pool-key-0", "sk-ant-sid01-pool-key-1"
	s, srv := startServiceWith(t, cfgClaudeWebPool(m.srv.URL, k0, k1))

	// 第一发：completion 429（键 0）→ 下游失败；**不换号**（恰 1 次 completion）。
	status1, raw1 := postChat(t, srv)
	if status1 == 200 {
		t.Fatalf("completion 429 应失败: %s", raw1)
	}
	calls := waitPoolCalls(t, m, 4)
	if !strings.Contains(buf.String(), "credential #0 cooling for 2m0s (class=quota)") {
		t.Fatalf("完成面 429 应回喂池冷却（retry-after 120s/配额类）:\n%s", buf.String())
	}
	for _, c := range calls {
		if c.key == k1 {
			t.Fatalf("第一发不得换号（键 1 被调用）: %+v", c)
		}
	}

	// 第二发：LRU 自然轮到键 1 → 200。
	status2, raw2 := postChat(t, srv)
	assertPONG(t, status2, raw2)
	calls = waitPoolCalls(t, m, 8)
	if calls[4].key != k1 {
		t.Fatalf("第二发 org 键 = %q, want %q", calls[4].key, k1)
	}

	// 第三发（判别点）：键 0 仍在冷却 → 即使 LRU 轮到它也必须跳过 → 键 1 → 200。
	status3, raw3 := postChat(t, srv)
	assertPONG(t, status3, raw3)
	calls = waitPoolCalls(t, m, 12)
	if calls[8].key != k1 {
		t.Fatalf("第三发 org 键 = %q, want %q（冷却中的键 0 必须被跳过）", calls[8].key, k1)
	}
	for _, c := range calls[8:] {
		if c.key == k0 {
			t.Fatalf("冷却期内不得再打键 0: %+v", c)
		}
	}

	// adminLog：完成面回喂以独立型别进 ring（credential_cooldown；索引-only）。
	entries := s.adminLog.window(0, "", 100, false).Entries
	found := false
	for _, e := range entries {
		if e.Kind == adminLogCredentialCooldown && e.Upstream == "fake-claude-web-pool" && strings.Contains(e.Message, "credential #0") {
			found = true
		}
	}
	if !found {
		t.Fatalf("adminLog 应有 credential_cooldown 事件: %+v", entries)
	}
}

// TestClaudeWebPoolAttemptCredentialIndex attempt 记录凭据索引（W54-3 残余 2）：
// 场景 A（全好池）→ credential_index=0 **必须出现在 wire**（omitempty 陷阱：
// 0 值不得被吞）；场景 B（坏键 0 换号到键 1）→ credential_index=1；两行均零
// 凭据字节。
func TestClaudeWebPoolAttemptCredentialIndex(t *testing.T) {
	var buf lockedBuffer
	old := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(old)

	// 场景 A：首发键 0 全链成功。
	mA := newPoolMock(t)
	mA.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	srvA := startService(t, cfgClaudeWebPool(mA.srv.URL, "sk-ant-sid01-pool-key-0", "sk-ant-sid01-pool-key-1"))
	statusA, rawA := postChat(t, srvA)
	assertPONG(t, statusA, rawA)
	waitPoolCalls(t, mA, 4)
	lineA := lastAttemptsLine(t, buf.String())
	if got := attemptCredentialIndex(t, lineA); got == nil || *got != 0 {
		t.Fatalf("场景 A credential_index = %v, want 0（0 值不得被省略——行: %s）", got, lineA)
	}
	if strings.Contains(lineA, "sk-ant") {
		t.Fatalf("attempt 行泄漏凭据字节: %s", lineA)
	}

	// 场景 B：坏键 0 → 换号到键 1。
	buf.Reset()
	mB := newPoolMock(t)
	mB.orgStatusByIndex[0] = http.StatusUnauthorized
	mB.streamBody = mA.streamBody
	srvB := startService(t, cfgClaudeWebPool(mB.srv.URL, "sk-ant-sid01-pool-key-0", "sk-ant-sid01-pool-key-1"))
	statusB, rawB := postChat(t, srvB)
	assertPONG(t, statusB, rawB)
	waitPoolCalls(t, mB, 5)
	lineB := lastAttemptsLine(t, buf.String())
	if got := attemptCredentialIndex(t, lineB); got == nil || *got != 1 {
		t.Fatalf("场景 B credential_index = %v, want 1（行: %s）", got, lineB)
	}
	if strings.Contains(lineB, "sk-ant") {
		t.Fatalf("attempt 行泄漏凭据字节: %s", lineB)
	}
}

// TestClaudeWebPoolCooldownAdminEvent 凭据冷却进管理面 ring 的独立型别
// （W54-3 残余 3）：准备面坏键 401 → adminLog 出现 credential_cooldown 事件
// （上游名 + `credential #0` + class=credential；零凭据字节）——与上游级
// cooldown_trip 型别区分。
func TestClaudeWebPoolCooldownAdminEvent(t *testing.T) {
	m := newPoolMock(t)
	m.orgStatusByIndex[0] = http.StatusUnauthorized
	m.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	s, srv := startServiceWith(t, cfgClaudeWebPool(m.srv.URL, "sk-ant-sid01-pool-key-0", "sk-ant-sid01-pool-key-1"))
	status, raw := postChat(t, srv)
	assertPONG(t, status, raw)
	waitPoolCalls(t, m, 5)

	entries := s.adminLog.window(0, "", 100, false).Entries
	var got *adminLogEntry
	for i := range entries {
		if entries[i].Kind == adminLogCredentialCooldown {
			got = &entries[i]
		}
	}
	if got == nil {
		t.Fatalf("adminLog 缺 credential_cooldown 事件: %+v", entries)
	}
	if got.Kind == adminLogCooldownTrip {
		t.Fatalf("凭据冷却不得记成上游级 cooldown_trip")
	}
	if got.Upstream != "fake-claude-web-pool" || !strings.Contains(got.Message, "credential #0") || !strings.Contains(got.Message, "class=credential") {
		t.Fatalf("credential_cooldown 事件 = %+v", got)
	}
	if strings.Contains(got.Message, "sk-ant") {
		t.Fatalf("事件消息泄漏凭据字节: %+v", got)
	}
}

// TestClaudeWebPoolSingleKeyCompat api_key 兼容 = 单元素池：全链可用、全调用同键、
// 标记不进 wire（Referer 逐字节 = {base}/new）。
func TestClaudeWebPoolSingleKeyCompat(t *testing.T) {
	m := newPoolMock(t)
	m.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	const key = "sk-ant-sid01-pool-key-0"
	cfg := &Config{
		Listen:           "127.0.0.1:0",
		HeartbeatSeconds: 15,
		Upstreams: []Upstream{{
			Name: "fake-claude-web-pool", BaseURL: m.srv.URL, APIKey: key,
			Protocol: ProtocolClaudeWeb,
		}},
	}
	srv := startService(t, cfg)
	status, raw := postChat(t, srv)
	assertPONG(t, status, raw)
	calls := waitPoolCalls(t, m, 4)
	for i, c := range calls {
		if c.key != key {
			t.Fatalf("调用[%d] key = %q, want %q（单键全链同键）", i, c.key, key)
		}
	}
	if got := calls[2].referer; got != m.srv.URL+"/new" {
		t.Fatalf("completion Referer = %q, want %q", got, m.srv.URL+"/new")
	}

	// credentials 单元素（无 api_key）同样全链可用。
	m2 := newPoolMock(t)
	m2.streamBody = m.streamBody
	srv2 := startService(t, cfgClaudeWebPool(m2.srv.URL, "sk-ant-sid01-pool-key-0"))
	status2, raw2 := postChat(t, srv2)
	assertPONG(t, status2, raw2)
}

// TestClaudeWebPoolNoCredentialLeak 凭据不泄漏：日志（含 attempt 记录行与池日志）
// 与下游响应体不含凭据子串；池日志带索引。
func TestClaudeWebPoolNoCredentialLeak(t *testing.T) {
	var buf bytes.Buffer
	old := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(old)

	m := newPoolMock(t)
	m.orgStatusByIndex[0] = http.StatusUnauthorized
	m.streamBody = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	const badKey, goodKey = "sk-ant-sid01-W53LEAK-bad-0", "sk-ant-sid01-W53LEAK-good-1"
	srv := startService(t, cfgClaudeWebPool(m.srv.URL, badKey, goodKey))

	status, raw := postChat(t, srv)
	assertPONG(t, status, raw)
	waitPoolCalls(t, m, 5)

	out := buf.String()
	if strings.Contains(out, "W53LEAK") {
		t.Fatalf("日志泄漏凭据字节:\n%s", out)
	}
	if strings.Contains(string(raw), "W53LEAK") {
		t.Fatalf("下游体泄漏凭据字节: %s", raw)
	}
	if !strings.Contains(out, "credential #0") {
		t.Fatalf("日志应带索引记账（credential #0）:\n%s", out)
	}
	if !strings.Contains(out, "attempts:") {
		t.Fatalf("应有 attempt 记录行:\n%s", out)
	}
	// W54-3：attempt 行带池凭据索引（换号后 = 1）且零凭据字节（上方 W53LEAK
	// 检查已覆盖字节面——此处锚定字段在场）。
	if !strings.Contains(out, "\"credential_index\":1") {
		t.Fatalf("attempt 行应带 credential_index（=1）:\n%s", out)
	}
}

// TestClaudeWebPoolExhaustedErrorNoLeak 耗尽错误不泄漏（错误消息只带索引面信息）。
func TestClaudeWebPoolExhaustedErrorNoLeak(t *testing.T) {
	p := poolForTest("sk-ant-sid01-W53EXH-SECRET-0", "sk-ant-sid01-W53EXH-SECRET-1")
	base := time.Unix(6000, 0)
	p.reportFailure(0, credFailureCredential, 0, base)
	p.reportFailure(1, credFailureCredential, 0, base)
	err := p.exhaustedError(base)
	if err == nil || strings.Contains(err.Error(), "W53EXH-SECRET") {
		t.Fatalf("耗尽错误泄漏凭据: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 配置面：credentials 校验 + env-ref 展开
// ---------------------------------------------------------------------------

func TestConfigCredentialsValidation(t *testing.T) {
	base := func(proto Protocol, mut func(u *Upstream)) *Config {
		u := Upstream{Name: "u", BaseURL: "http://127.0.0.1:9", Protocol: proto}
		if mut != nil {
			mut(&u)
		}
		return &Config{Upstreams: []Upstream{u}}
	}
	cases := []struct {
		name    string
		cfg     *Config
		wantErr string
	}{
		{"api_key 与 credentials 互斥", base(ProtocolClaudeWeb, func(u *Upstream) {
			u.APIKey = "k"
			u.Credentials = []string{"a"}
		}), "mutually exclusive"},
		{"空列表", base(ProtocolClaudeWeb, func(u *Upstream) { u.Credentials = []string{} }), "at least one"},
		{"空白项", base(ProtocolClaudeWeb, func(u *Upstream) { u.Credentials = []string{"a", "  "} }), "blank"},
		{"非 claude_web 协议", base(ProtocolOpenAIChat, func(u *Upstream) { u.Credentials = []string{"a"} }), "only supported for protocol claude_web"},
		{"合法池", base(ProtocolClaudeWeb, func(u *Upstream) { u.Credentials = []string{"a", "b"} }), ""},
		{"单元素池合法", base(ProtocolClaudeWeb, func(u *Upstream) { u.Credentials = []string{"a"} }), ""},
	}
	for _, c := range cases {
		err := c.cfg.Validate()
		if c.wantErr == "" {
			if err != nil {
				t.Fatalf("%s: 不应报错: %v", c.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), c.wantErr) {
			t.Fatalf("%s: err = %v, want 含 %q", c.name, err, c.wantErr)
		}
	}
}

func TestConfigCredentialsEnvRefExpansion(t *testing.T) {
	t.Setenv("A2A_W53_CRED_KEY", "sk-live-w53-cred")
	body := "listen: \"127.0.0.1:0\"\nupstreams:\n  - name: u\n    protocol: claude_web\n    credentials:\n      - \"bad-literal-key\"\n      - \"${A2A_W53_CRED_KEY}\"\n"
	c, err := LoadConfig(writeEnvCfg(t, body))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	creds := c.Upstreams[0].Credentials
	if len(creds) != 2 || creds[0] != "bad-literal-key" || creds[1] != "sk-live-w53-cred" {
		t.Fatalf("credentials = %q（want 逐项展开）", creds)
	}

	// 未定义变量 → 显式报错（错误只含变量名——不含任何值）。
	body2 := "listen: \"127.0.0.1:0\"\nupstreams:\n  - name: u\n    protocol: claude_web\n    credentials:\n      - \"${A2A_W53_CRED_MISSING}\"\n"
	if _, err := LoadConfig(writeEnvCfg(t, body2)); err == nil || !strings.Contains(err.Error(), "A2A_W53_CRED_MISSING") {
		t.Fatalf("未定义变量应显式报错: %v", err)
	}

	// 空列表（显式 []）→ 启动报错。
	body3 := "listen: \"127.0.0.1:0\"\nupstreams:\n  - name: u\n    protocol: claude_web\n    credentials: []\n"
	if _, err := LoadConfig(writeEnvCfg(t, body3)); err == nil || !strings.Contains(err.Error(), "at least one") {
		t.Fatalf("空列表应报错: %v", err)
	}
}
