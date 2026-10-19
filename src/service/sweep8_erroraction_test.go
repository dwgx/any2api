// W5-25 条目 3（上游错误→我们动作 对照表）：跨网关错误体 → 异常类的映射回归。
//
// 表来源：P29（OpenRouter error_type / Bifrost FailureClass / uni-api 状态改写 /
// Higress 状态正则）+ zcode-wire 业务码表；实现面 `failover.go` 的 bodyClassRules
// 与 classQuota。逐条锁「同状态码、不同错误体 → 不同动作」的分离实验。
package service

import (
	"net/http"
	"testing"
)

func hdr(pairs ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(pairs); i += 2 {
		h.Set(pairs[i], pairs[i+1])
	}
	return h
}

// TestErrorActionQuotaVsRateLimit：429 的两种形态——额度类（等/换都没用）不换；
// 限流类（等有用）换。Bifrost quota vs rate_limit 的区分（P29 §可复用点 3）。
func TestErrorActionQuotaVsRateLimit(t *testing.T) {
	// 429 + insufficient_quota（OpenAI/uni-api 词面）→ quota：不换。
	cls, builtin, forced := classifyFailure(429, hdr(), []byte(`{"error":{"message":"You exceeded your current quota","type":"insufficient_quota"}}`))
	if cls != classQuota || builtin || forced {
		t.Fatalf("429+insufficient_quota = (%s,%v,%v)（want quota,false,false）", cls, builtin, forced)
	}
	if shouldFailover(429, hdr(), []byte(`{"error":{"type":"insufficient_quota"}}`)) {
		t.Fatal("额度类 429 不应换上游（换也没用）")
	}
	// 429 + billing_hard_limit_reached（uni-api 白名单）→ quota。
	if cls, _, _ := classifyFailure(429, hdr(), []byte(`{"error":{"code":"billing_hard_limit_reached"}}`)); cls != classQuota {
		t.Fatalf("billing_hard_limit_reached = %s（want quota）", cls)
	}
	// 429 普通限流（无额度词面）→ rate_limit：换。
	if cls, builtin, _ := classifyFailure(429, hdr(), []byte(`{"error":{"type":"rate_limit_exceeded"}}`)); cls != classRateLimit || !builtin {
		t.Fatalf("普通 429 = (%s,%v)（want rate_limit,true）", cls, builtin)
	}
}

// TestErrorActionOpenRouter402：OpenRouter 402 的两种形态（P29 §陷阱 4）——
// 普通 402（余额不足）停；`metadata.limit_source=openrouter_in_flight_budget`
// （带 Retry-After 才成立）按 rate_limit 换。
func TestErrorActionOpenRouter402(t *testing.T) {
	plain := []byte(`{"error":{"code":402,"message":"Insufficient credits","metadata":{"error_type":"payment_required"}}}`)
	if shouldFailover(402, hdr(), plain) {
		t.Fatal("普通 402（余额不足）不应换上游")
	}
	if cls, _, _ := classifyFailure(402, hdr(), plain); cls != classQuota {
		t.Fatalf("402+payment_required = %s（want quota）", cls)
	}
	inflight := []byte(`{"error":{"code":402,"message":"In-flight budget exhausted","metadata":{"limit_source":"openrouter_in_flight_budget"}}}`)
	if !shouldFailover(402, hdr("retry-after", "60"), inflight) {
		t.Fatal("402+in_flight_budget 应换上游（等一会儿可重试的形态）")
	}
	if cls, _, _ := classifyFailure(402, hdr("retry-after", "60"), inflight); cls != classRateLimit {
		t.Fatalf("402+in_flight_budget = %s（want rate_limit）", cls)
	}
}

// TestErrorActionBifrostSynthetic：Bifrost 合成错码（P29 §陷阱 15）——
// 502 upstream_credentials_exhausted / 503 no_eligible_keys 是网关自造，
// 语义是「凭据耗尽」→ quota（不换不冷却；上游本身没坏）。
func TestErrorActionBifrostSynthetic(t *testing.T) {
	cases := []struct {
		status int
		body   string
	}{
		{502, `{"is_bifrost_error":true,"error":{"code":"upstream_credentials_exhausted","message":"all keys dead"}}`},
		{503, `{"is_bifrost_error":true,"error":{"code":"no_eligible_keys","message":"no keys"}}`},
	}
	for _, c := range cases {
		if cls, builtin, _ := classifyFailure(c.status, hdr(), []byte(c.body)); cls != classQuota || builtin {
			t.Fatalf("%d 合成错 = (%s,%v)（want quota,false）", c.status, cls, builtin)
		}
		if shouldFailover(c.status, hdr(), []byte(c.body)) {
			t.Fatalf("%d 合成错不应换上游", c.status)
		}
	}
}

// TestErrorActionExplicitSignalsStillWin：显式信号优先于 body 类（C1 实测语义：
// x-should-retry: false 压住一切、overloaded 文本先于头检查——body 表不得改写）。
func TestErrorActionExplicitSignalsStillWin(t *testing.T) {
	// x-should-retry: false + quota 词面 → 仍 false（forced 语义）。
	body := []byte(`{"error":{"type":"insufficient_quota"}}`)
	if shouldFailover(429, hdr("x-should-retry", "false"), body) {
		t.Fatal("x-should-retry:false 必须压住 body 类")
	}
	// overloaded 文本 + 额度词面 → overloaded 强制换（文本匹配先于 body 表）。
	over := []byte(`{"error":{"type":"overloaded_error","message":"insufficient_quota mentioned in message"}}`)
	if !shouldFailover(529, hdr("x-should-retry", "false"), over) {
		t.Fatal("overloaded 文本必须强制换（先于一切）")
	}
	// x-should-retry: true + quota 词面 → 换（显式 true 覆盖）。
	if !shouldFailover(429, hdr("x-should-retry", "true"), body) {
		t.Fatal("x-should-retry:true 必须覆盖 body 类")
	}
}

// TestErrorActionZcodeBusinessCodes：zcode 业务码表的充值/套餐类（等/换都没用）
// → quota（P29 未决 6 的并集里我们收的最小集）。
func TestErrorActionZcodeBusinessCodes(t *testing.T) {
	for _, code := range []string{
		"credit_balance_exhausted", "organization_spend_limit_exceeded", "project_spend_limit_exceeded",
	} {
		body := []byte(`{"error":{"code":"` + code + `","message":"no funds"}}`)
		if shouldFailover(400, hdr(), body) {
			t.Fatalf("%s（400）不应换上游", code)
		}
		if cls, _, _ := classifyFailure(400, hdr(), body); cls != classQuota {
			t.Fatalf("%s = %s（want quota）", code, cls)
		}
	}
}

// TestErrorActionQuotaBudgetConfigurable：quota 类可被 failover_policy 显式改写
// （写 N>0 强制换 N 次）——配置面与内置判定的接线锚点。
func TestErrorActionQuotaBudgetConfigurable(t *testing.T) {
	body := []byte(`{"error":{"type":"insufficient_quota"}}`)
	cfg := &Config{FailoverPolicy: map[string]int{"quota": 2}}
	srv := &Server{cfg: cfg}
	budget := &failoverBudget{}
	if allow, failure := srv.failoverDecision(budget, 429, hdr(), body); !allow || !failure {
		t.Fatalf("quota 预算 2 的首次决策 = (%v,%v)（want true,true）", allow, failure)
	}
	if allow, failure := srv.failoverDecision(budget, 429, hdr(), body); !allow || !failure {
		t.Fatalf("第二次 = (%v,%v)（want true,true）", allow, failure)
	}
	if allow, _ := srv.failoverDecision(budget, 429, hdr(), body); allow {
		t.Fatal("第三次应超预算（N=2）")
	}
}
