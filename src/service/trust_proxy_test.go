package service

// trust_proxy_test.go —— P0-3 trust proxy 修正测试（board A2A-L19-W57-1）：
// 三态（未配=现状 / 配反代=XFF 生效 / 伪造 XFF 被拒）+ XFF 最右非信任跳单元 +
// Secure cookie 的 auto 判定。
//
// 装置：httptest 回环服务（对端 = 127.0.0.1）；限流面 = 每 IP 1 次/分钟——桶
// 消耗后同 IP 立即 429，用「谁被限流」反推客户端 IP 解析结果。
//
// 反证锚点（回退即红）：
//   - clientIP 恒走 RemoteAddr（删掉信任分支）→ TestTrustProxyConfiguredUsesXFF 红；
//   - 未配 trusted_proxies 也读 XFF → TestTrustProxyUnconfiguredIgnoresXFF 红；
//   - 取 XFF 最左跳 / 不过滤信任跳 → TestXFFClientIPWalk 与
//     TestTrustProxyConfiguredUsesXFF（右跳形态）红。

import (
	"net/http"
	"testing"
)

// pubRateLimitCfg 公开面 + per-IP 限流（1 次/分钟）+ 可选信任网段。
func pubRateLimitCfg(trusted []string) *Config {
	c := uiTestConfig()
	c.UI = &UISection{RateLimitPerMin: 1}
	c.Public = &PublicSection{Enabled: true, TrustedProxies: trusted}
	return c
}

// TestTrustProxyUnconfiguredIgnoresXFF 未配 trusted_proxies = 现状：XFF 一律
// 不读——伪造不同 XFF 的请求仍按 TCP 对端合并计数（第二发 429）。
func TestTrustProxyUnconfiguredIgnoresXFF(t *testing.T) {
	_, srv := startServiceWith(t, pubRateLimitCfg(nil))

	r1 := pubReq(t, http.MethodGet, srv.URL+"/v1/models", map[string]string{"X-Forwarded-For": "9.9.9.9"})
	pubReadBody(t, r1)
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("r1 status = %d", r1.StatusCode)
	}
	r2 := pubReq(t, http.MethodGet, srv.URL+"/v1/models", map[string]string{"X-Forwarded-For": "8.8.8.8"})
	pubReadBody(t, r2)
	if r2.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("r2 status = %d（want 429——XFF 伪造不得绕桶）", r2.StatusCode)
	}
}

// TestTrustProxyConfiguredUsesXFF 配反代（对端 127.0.0.1 命中信任集）：XFF 最右
// 非信任跳 = 客户端 IP——不同 XFF 各自独立桶；同 XFF 第二发 429；带右侧信任跳
// 的形态（`client, 127.0.0.1`）仍取 client。
func TestTrustProxyConfiguredUsesXFF(t *testing.T) {
	_, srv := startServiceWith(t, pubRateLimitCfg([]string{"127.0.0.1/32"}))

	get := func(xff string) *http.Response {
		return pubReq(t, http.MethodGet, srv.URL+"/v1/models", map[string]string{"X-Forwarded-For": xff})
	}
	// 1.1.1.1 与 2.2.2.2 各自独立桶 → 都放行。
	r1 := get("1.1.1.1")
	pubReadBody(t, r1)
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("r1 status = %d", r1.StatusCode)
	}
	r2 := get("2.2.2.2")
	pubReadBody(t, r2)
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("r2 status = %d（XFF 生效后应独立计数）", r2.StatusCode)
	}
	// 1.1.1.1 桶已消耗 → 429。
	r3 := get("1.1.1.1")
	pubReadBody(t, r3)
	if r3.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("r3 status = %d（want 429——同客户端）", r3.StatusCode)
	}
	// 右跳形态：最右非信任跳仍是 1.1.1.1（127.0.0.1 是信任跳被跳过）。
	r4 := get("1.1.1.1, 127.0.0.1")
	pubReadBody(t, r4)
	if r4.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("r4 status = %d（want 429——最右非信任跳 = 1.1.1.1）", r4.StatusCode)
	}
	// 新客户端 3.3.3.3（带信任跳）→ 放行。
	r5 := get("3.3.3.3, 127.0.0.1")
	pubReadBody(t, r5)
	if r5.StatusCode != http.StatusOK {
		t.Fatalf("r5 status = %d（新客户端应放行）", r5.StatusCode)
	}
}

// TestTrustProxySpoofedXFFRejected 对端不在信任集（直连）→ XFF 一律忽略：
// 伪造不同 XFF 仍按对端合并计数。
func TestTrustProxySpoofedXFFRejected(t *testing.T) {
	_, srv := startServiceWith(t, pubRateLimitCfg([]string{"10.0.0.0/8"}))

	r1 := pubReq(t, http.MethodGet, srv.URL+"/v1/models", map[string]string{"X-Forwarded-For": "7.7.7.7"})
	pubReadBody(t, r1)
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("r1 status = %d", r1.StatusCode)
	}
	r2 := pubReq(t, http.MethodGet, srv.URL+"/v1/models", map[string]string{"X-Forwarded-For": "6.6.6.6"})
	pubReadBody(t, r2)
	if r2.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("r2 status = %d（want 429——不可信对端的 XFF 必须被拒）", r2.StatusCode)
	}
}

// TestXFFClientIPWalk 单元：最右非信任跳的边界（畸形跳过 / 全信任回落 / 空白）。
func TestXFFClientIPWalk(t *testing.T) {
	nets, err := parseTrustedProxies([]string{"10.0.0.0/8", "::1"})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cases := []struct{ xff, want string }{
		{"1.2.3.4, 10.1.1.1", "1.2.3.4"}, // 右跳信任 → 左跳客户端
		{"1.2.3.4", "1.2.3.4"},           // 单跳
		{"10.1.1.1", ""},                 // 全信任 → 回落
		{"", ""},                         // 空
		{"1.2.3.4, garbage", "1.2.3.4"},  // 畸形跳过
		{"garbage, 1.2.3.4, 10.1.1.1", "1.2.3.4"},
		{" 5.6.7.8 , 10.0.0.1 ", "5.6.7.8"}, // 空白修剪
		{"2001:db8::1, ::1", "2001:db8::1"}, // IPv6
		{"10.0.0.1, 10.0.0.2", ""},          // 多级信任 → 回落
	}
	for _, tc := range cases {
		if got := xffClientIP(tc.xff, nets); got != tc.want {
			t.Fatalf("xffClientIP(%q) = %q, want %q", tc.xff, got, tc.want)
		}
	}
}

// TestTrustProxySecureCookieAuto Secure cookie 的 auto 判定：可信反代 +
// X-Forwarded-Proto=https → Set-Cookie 带 Secure；缺该头 → 不带。
func TestTrustProxySecureCookieAuto(t *testing.T) {
	c := uiTestConfig()
	c.Public = &PublicSection{Enabled: true, TrustedProxies: []string{"127.0.0.1/32"}}
	_, srv := startServiceWith(t, c)

	// 无 X-Forwarded-Proto（HTTP）→ 无 Secure。
	r1 := pubReq(t, http.MethodGet, srv.URL+"/v1/models", nil)
	pubReadBody(t, r1)
	if sc := r1.Header.Get("Set-Cookie"); sc == "" || containsAttr(sc, "Secure") {
		t.Fatalf("HTTP 直连不得带 Secure: %q", sc)
	}
	// 可信反代 + https → Secure。
	r2 := pubReq(t, http.MethodGet, srv.URL+"/v1/models", map[string]string{"X-Forwarded-Proto": "https"})
	pubReadBody(t, r2)
	if sc := r2.Header.Get("Set-Cookie"); !containsAttr(sc, "Secure") {
		t.Fatalf("可信反代 https 应带 Secure: %q", sc)
	}
}

// containsAttr Set-Cookie 属性判定（按 `; ` 段精确匹配——避免值内子串误判）。
func containsAttr(setCookie, attr string) bool {
	for _, seg := range splitCookieSegs(setCookie) {
		if seg == attr {
			return true
		}
	}
	return false
}

func splitCookieSegs(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ';' {
			seg := s[start:i]
			// 修剪前导空格
			for len(seg) > 0 && seg[0] == ' ' {
				seg = seg[1:]
			}
			if seg != "" {
				out = append(out, seg)
			}
			start = i + 1
		}
	}
	return out
}
