// A2A-L19-W81-2：模型 → 上游白名单（`models[].upstreams`）的候选过滤与
// serve 接线。核心断言：非 Claude 模型**绝不**先打 claude_web（W80 实测新鲜
// claude_web 凭据对非 Claude 模型 403 + 30m 凭据冷却）。
package service

import (
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// routingTestConfig 三台上游：claude-web 在链首（最危险），upstream-a 允许给
// deepseek，upstream-b 声明 task_face: off（验证白名单与任务面过滤叠加）。
func routingTestConfig() *Config {
	return &Config{
		Listen:           "127.0.0.1:0",
		HeartbeatSeconds: 15,
		Upstreams: []Upstream{
			{Name: "claude-web", BaseURL: "http://127.0.0.1:1", Protocol: ProtocolClaudeWeb, APIKey: "k1"},
			{Name: "upstream-a", BaseURL: "http://127.0.0.1:2", Protocol: ProtocolOpenAIChat, APIKey: "k2"},
			{Name: "upstream-b", BaseURL: "http://127.0.0.1:3", Protocol: ProtocolOpenAIChat, APIKey: "k3", TaskFace: taskFaceOff},
		},
		Models: []ModelEntry{
			{ID: "deepseek", UpstreamModel: "deepseek-v4", Upstreams: []string{"upstream-a"}},
			{ID: "claude-sonnet"}, // 缺省 upstreams = 不限制（向后兼容）
		},
	}
}

func assertCandidateIdx(t *testing.T, label string, got []int, want ...int) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: candidates = %v, want %v", label, got, want)
	}
}

func TestCandidatesForModelFilter(t *testing.T) {
	cfg := routingTestConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	s := New(cfg)
	now := time.Now()

	// 白名单模型：只留 upstream-a（链首的 claude-web 被路由排除）。
	idxs, skipped := s.candidatesForModel(now, false, "deepseek")
	assertCandidateIdx(t, "deepseek", idxs, 1)
	if skipped != 0 {
		t.Fatalf("deepseek skipped = %d, want 0（白名单排除不算冷却跳过）", skipped)
	}

	// 无白名单模型：全链（现状行为）。
	idxs, _ = s.candidatesForModel(now, false, "claude-sonnet")
	assertCandidateIdx(t, "claude-sonnet", idxs, 0, 1, 2)

	// 未配置模型：不限制（透传宽容面不变）。
	idxs, _ = s.candidatesForModel(now, false, "gpt-6.1-sol")
	assertCandidateIdx(t, "unknown-model", idxs, 0, 1, 2)

	// 任务面：`task_face: off` 的 upstream-b 仍被剔除。
	idxs, _ = s.candidatesForModel(now, true, "claude-sonnet")
	assertCandidateIdx(t, "claude-sonnet/taskFace", idxs, 0, 1)

	// 白名单 + 任务面叠加：只剩 upstream-a（不是 off）。
	idxs, _ = s.candidatesForModel(now, true, "deepseek")
	assertCandidateIdx(t, "deepseek/taskFace", idxs, 1)

	// 旧调用面（无模型上下文）保持逐字行为。
	idxs, _ = s.candidates(now)
	assertCandidateIdx(t, "candidates", idxs, 0, 1, 2)
}

func TestCandidatesForModelFilterAllCooling(t *testing.T) {
	cfg := routingTestConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	s := New(cfg)
	now := time.Now()

	// 三台全部冷却：白名单模型的「全冷却」兜底必须只回白名单内（upstream-a），
	// 绝不回退到链首的 claude-web。
	for i := range s.health {
		s.health[i].until = now.Add(time.Minute)
	}
	idxs, skipped := s.candidatesForModel(now, false, "deepseek")
	assertCandidateIdx(t, "deepseek/all-cooling", idxs, 1)
	if skipped != 1 {
		t.Fatalf("skipped = %d, want 1", skipped)
	}
	if slices.Contains(idxs, 0) {
		t.Fatalf("all-cooling fallback 回退到了 claude-web: %v", idxs)
	}

	// 对照：无白名单模型的全冷却兜底仍是全列表（既有语义不变）。
	idxs, skipped = s.candidatesForModel(now, false, "claude-sonnet")
	assertCandidateIdx(t, "claude-sonnet/all-cooling", idxs, 0, 1, 2)
	if skipped != 3 {
		t.Fatalf("claude-sonnet skipped = %d, want 3", skipped)
	}
}

// buddyProbe 记录 upstream-a 假上游收到的请求体（handler 在服务 goroutine 跑）。
type buddyProbe struct {
	mu   sync.Mutex
	body string
}

func (p *buddyProbe) set(b []byte) {
	p.mu.Lock()
	p.body = string(b)
	p.mu.Unlock()
}

func (p *buddyProbe) get() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.body
}

// TestServeModelRoutingSkipsUnlisted：端到端证明非 Claude 模型不会尝试链首的
// claude_web（负控制：删掉白名单过滤后 claude_web 必被尝试 → 本测试红）。
func TestServeModelRoutingSkipsUnlisted(t *testing.T) {
	claudeWeb := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		// 若被调用：返回 500（真实场景是 403；两者都是上游失败信号）。
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(errBodyAPIError))
	})
	probe := &buddyProbe{}
	buddy := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		probe.set(body)
		w.Header().Set("Content-Type", "text/event-stream")
		claudeTextStream(w)
	})

	cfg := &Config{
		Listen:           "127.0.0.1:0",
		HeartbeatSeconds: 15,
		Upstreams: []Upstream{
			{Name: "claude-web", BaseURL: claudeWeb.srv.URL, Protocol: ProtocolClaudeWeb, APIKey: "k1"},
			{Name: "upstream-a", BaseURL: buddy.srv.URL, Protocol: ProtocolAnthropic, APIKey: "k2"},
		},
		Models: []ModelEntry{
			{ID: "deepseek", UpstreamModel: "deepseek-v4", Upstreams: []string{"upstream-a"}},
		},
	}
	s, srv := startServiceWith(t, cfg)

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"model":"deepseek","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	body := string(raw)

	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "Hello ") {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, body)
	}
	if got := claudeWeb.count(); got != 0 {
		t.Fatalf("claude_web attempts = %d, want 0（非 Claude 模型绝不打 claude_web）", got)
	}
	if got := buddy.count(); got != 1 {
		t.Fatalf("upstream-a attempts = %d, want 1", got)
	}
	if upBody := probe.get(); !strings.Contains(upBody, `"model":"deepseek-v4"`) {
		t.Fatalf("上游请求体缺 upstream_model 改写: %s", upBody)
	}
	if d := s.Diagnostics(); d.Attempts != 1 || d.CooldownSkips != 0 {
		t.Fatalf("diagnostics = %+v（want attempts=1, cooldownSkips=0）", d)
	}
}
