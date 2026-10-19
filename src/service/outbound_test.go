// W11 出站面测试：per-upstream 代理（http CONNECT / socks5）+ TLS 指纹开关
// （uTLS chrome）+ 本地假 CF 探针（双门：TLS ClientHello GREASE + UA 头面）。
//
// 装置（全部本地回环，零真实上游）：
//   - fakeConnectProxy：假 HTTP CONNECT 代理（记录 CONNECT 目标与
//     Proxy-Authorization；字节级隧道）；
//   - fakeSocks5Proxy：假 SOCKS5 代理（记录目标 host:port 与用户名密码；可
//     重写目标地址——域名形态断言用，目标域名由代理解析 = socks5h 语义）；
//   - fakeCFProbe：假 Cloudflare（TLS 层按 ClientHello GREASE 放行/403+
//     `cf-mitigated: challenge`；HTTP 层按 UA 放行/403——两层可单独开关，镜像
//     W11-2 实测的盾判据形态；GET/POST 三端点 + /v1/messages 分流）；
//   - tlsUpstream：TLS 假 anthropic 上游（h1；claudeTextStream；可配固定失败码）。
//
// 探针能力边界（如实声明，报告同）：Go 的 tls.ClientHelloInfo 暴露密码套件/
// 扩展 ID 列表/ALPN/版本/曲线——GREASE 与 ALPN 可判、套件顺序可判；**原始
// ClientHello 字节与扩展内部载荷不可见**（完整 JA3 串/签名算法逐项不在此层）。
package service

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	utls "github.com/refraction-networking/utls"
)

// ---------------------------------------------------------------------------
// 装置 1：假 HTTP CONNECT 代理
// ---------------------------------------------------------------------------

type fakeConnectProxy struct {
	ln net.Listener

	mu      sync.Mutex
	targets []string // CONNECT 目标（host:port，原样）
	auths   []string // Proxy-Authorization 头原值
}

func newFakeConnectProxy(t *testing.T) *fakeConnectProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &fakeConnectProxy{ln: ln}
	go p.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return p
}

func (p *fakeConnectProxy) addr() string { return p.ln.Addr().String() }

func (p *fakeConnectProxy) snapshot() (targets, auths []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.targets...), append([]string(nil), p.auths...)
}

func (p *fakeConnectProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *fakeConnectProxy) handle(c net.Conn) {
	defer c.Close()
	br := bufio.NewReader(c)
	req, err := http.ReadRequest(br)
	if err != nil {
		return
	}
	if req.Method != http.MethodConnect {
		_, _ = io.WriteString(c, "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n")
		return
	}
	p.mu.Lock()
	p.targets = append(p.targets, req.Host)
	p.auths = append(p.auths, req.Header.Get("Proxy-Authorization"))
	p.mu.Unlock()
	dst, err := net.Dial("tcp", req.Host)
	if err != nil {
		_, _ = io.WriteString(c, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
		return
	}
	defer dst.Close()
	if _, err := io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	go func() { _, _ = io.Copy(dst, br) }()
	_, _ = io.Copy(c, dst)
}

// ---------------------------------------------------------------------------
// 装置 2：假 SOCKS5 代理
// ---------------------------------------------------------------------------

type fakeSocks5Proxy struct {
	ln       net.Listener
	wantAuth bool
	// dialOverride 目标重写（"host:port" → 实际拨号地址）：域名形态断言用——
	// 客户端把域名交给代理解析（socks5h），代理侧把它映射回本地回环。
	dialOverride map[string]string

	mu      sync.Mutex
	targets []string // 目标 host:port（域名原样——socks5h 语义断言）
	users   []string // "user:pass"（认证面）
}

func newFakeSocks5Proxy(t *testing.T, wantAuth bool) *fakeSocks5Proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("socks5 listen: %v", err)
	}
	p := &fakeSocks5Proxy{ln: ln, wantAuth: wantAuth, dialOverride: map[string]string{}}
	go p.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return p
}

func (p *fakeSocks5Proxy) addr() string { return p.ln.Addr().String() }

func (p *fakeSocks5Proxy) snapshot() (targets, users []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.targets...), append([]string(nil), p.users...)
}

func (p *fakeSocks5Proxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *fakeSocks5Proxy) handle(c net.Conn) {
	defer c.Close()
	br := bufio.NewReader(c)
	// 握手：VER NMETHODS METHODS...
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(br, hdr); err != nil || hdr[0] != 5 {
		return
	}
	methods := make([]byte, int(hdr[1]))
	if _, err := io.ReadFull(br, methods); err != nil {
		return
	}
	offerAuth := false
	for _, m := range methods {
		if m == 2 {
			offerAuth = true
		}
	}
	if p.wantAuth && offerAuth {
		if _, err := c.Write([]byte{5, 2}); err != nil {
			return
		}
		if _, err := io.ReadFull(br, hdr); err != nil || hdr[0] != 1 {
			return
		}
		u := make([]byte, int(hdr[1]))
		if _, err := io.ReadFull(br, u); err != nil {
			return
		}
		plen := make([]byte, 1) // 密码长度 = 单字节（VER ULEN UNAME PLEN PASSWD）
		if _, err := io.ReadFull(br, plen); err != nil {
			return
		}
		pw := make([]byte, int(plen[0]))
		if _, err := io.ReadFull(br, pw); err != nil {
			return
		}
		p.mu.Lock()
		p.users = append(p.users, string(u)+":"+string(pw))
		p.mu.Unlock()
		if _, err := c.Write([]byte{1, 0}); err != nil {
			return
		}
	} else {
		if _, err := c.Write([]byte{5, 0}); err != nil {
			return
		}
	}
	// 请求：VER CMD RSV ATYP DST.ADDR DST.PORT
	req := make([]byte, 4)
	if _, err := io.ReadFull(br, req); err != nil || req[0] != 5 || req[1] != 1 {
		return
	}
	var host string
	switch req[3] {
	case 1: // IPv4
		b := make([]byte, 4)
		if _, err := io.ReadFull(br, b); err != nil {
			return
		}
		host = net.IP(b).String()
	case 3: // domain
		l := make([]byte, 1)
		if _, err := io.ReadFull(br, l); err != nil {
			return
		}
		b := make([]byte, int(l[0]))
		if _, err := io.ReadFull(br, b); err != nil {
			return
		}
		host = string(b)
	case 4: // IPv6
		b := make([]byte, 16)
		if _, err := io.ReadFull(br, b); err != nil {
			return
		}
		host = net.IP(b).String()
	default:
		return
	}
	pb := make([]byte, 2)
	if _, err := io.ReadFull(br, pb); err != nil {
		return
	}
	target := net.JoinHostPort(host, strconv.Itoa(int(pb[0])<<8|int(pb[1])))
	p.mu.Lock()
	p.targets = append(p.targets, target)
	dialAddr := target
	if alt, ok := p.dialOverride[target]; ok {
		dialAddr = alt
	}
	p.mu.Unlock()
	dst, err := net.Dial("tcp", dialAddr)
	if err != nil {
		_, _ = c.Write([]byte{5, 5, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}
	defer dst.Close()
	if _, err := c.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}
	go func() { _, _ = io.Copy(dst, br) }()
	_, _ = io.Copy(c, dst)
}

// ---------------------------------------------------------------------------
// 装置 3：假 CF 探针（TLS ClientHello GREASE 门 + UA 门）
// ---------------------------------------------------------------------------

type probeHello struct {
	grease   bool
	ciphers  []uint16
	exts     []uint16
	versions []uint16
	curves   []tls.CurveID
	alpn     []string
}

type probeRequest struct {
	remoteAddr string
	method     string
	path       string
	ua         string
	headers    http.Header
	greaseOK   bool
	uaOK       bool
	blocked    bool
}

type fakeCFProbe struct {
	srv *httptest.Server
	// requireGREASE 开启 TLS 门（无 GREASE → 403+cf-mitigated）；requireUA 开启
	// UA 门（非浏览器 UA → 403+cf-mitigated）。两门可单独关（隔离测另一面）。
	requireGREASE bool
	requireUA     bool
	// completionSSE completion 端点的 SSE 体（测试注入 fixture 回放）。
	completionSSE string

	mu       sync.Mutex
	hellos   map[string]probeHello
	requests []probeRequest
}

func newFakeCFProbe(t *testing.T) *fakeCFProbe {
	t.Helper()
	p := &fakeCFProbe{requireGREASE: true, requireUA: true, hellos: map[string]probeHello{}}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(p.handle))
	srv.EnableHTTP2 = true
	srv.TLS = &tls.Config{
		// 两形态都要能握手：h2 给指纹面，http/1.1 给默认栈/代理形态。
		NextProtos: []string{"h2", "http/1.1"},
		GetConfigForClient: func(chi *tls.ClientHelloInfo) (*tls.Config, error) {
			p.recordHello(chi)
			return nil, nil
		},
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	p.srv = srv
	return p
}

func (p *fakeCFProbe) recordHello(chi *tls.ClientHelloInfo) {
	h := probeHello{
		ciphers:  append([]uint16(nil), chi.CipherSuites...),
		exts:     append([]uint16(nil), chi.Extensions...),
		versions: append([]uint16(nil), chi.SupportedVersions...),
		curves:   append([]tls.CurveID(nil), chi.SupportedCurves...),
		alpn:     append([]string(nil), chi.SupportedProtos...),
	}
	for _, v := range h.ciphers {
		if isGREASEValue(v) {
			h.grease = true
		}
	}
	for _, v := range h.exts {
		if isGREASEValue(v) {
			h.grease = true
		}
	}
	for _, v := range h.versions {
		if isGREASEValue(v) {
			h.grease = true
		}
	}
	for _, v := range h.curves {
		if isGREASEValue(uint16(v)) {
			h.grease = true
		}
	}
	p.mu.Lock()
	p.hellos[chi.Conn.RemoteAddr().String()] = h
	p.mu.Unlock()
}

func (p *fakeCFProbe) handle(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	h := p.hellos[r.RemoteAddr]
	p.mu.Unlock()
	rec := probeRequest{
		remoteAddr: r.RemoteAddr,
		method:     r.Method,
		path:       r.URL.Path,
		ua:         r.UserAgent(),
		headers:    r.Header.Clone(),
		greaseOK:   h.grease,
		uaOK:       isBrowserUA(r.UserAgent()),
	}
	if (p.requireGREASE && !rec.greaseOK) || (p.requireUA && !rec.uaOK) {
		rec.blocked = true
		p.mu.Lock()
		p.requests = append(p.requests, rec)
		p.mu.Unlock()
		w.Header().Set("cf-mitigated", "challenge")
		w.Header().Set("Content-Type", "text/html; charset=UTF-8")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, "<html><body>cf challenge</body></html>")
		return
	}
	p.mu.Lock()
	p.requests = append(p.requests, rec)
	p.mu.Unlock()
	p.serveEndpoint(w, r)
}

func (p *fakeCFProbe) serveEndpoint(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/api/organizations":
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `[{"id":1,"uuid":%q,"name":"Probe","rate_limit_tier":"default_claude_ai"}]`, claudeWebOrgUUID)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/chat_conversations"):
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(w, `{"uuid":%q}`, claudeWebConvUUID)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/completion"):
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, p.completionSSE)
	case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/chat_conversations/"):
		// W13 清理面：会话删除（204——BK/CW/YS 判据之一）。
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/messages":
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		claudeTextStream(w)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (p *fakeCFProbe) snapshot() []probeRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]probeRequest(nil), p.requests...)
}

func (p *fakeCFProbe) helloFor(remoteAddr string) (probeHello, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h, ok := p.hellos[remoteAddr]
	return h, ok
}

// isGREASEValue GREASE 值判定（RFC 8701：0x?a?a——高低字节相等且低半字节为 a）。
func isGREASEValue(v uint16) bool {
	return v&0x0f0f == 0x0a0a && byte(v>>8) == byte(v)
}

// isBrowserUA 假 CF 的 UA 门（镜像 W11-2 实测：curl 默认 UA 403 / Chrome UA 200）。
func isBrowserUA(ua string) bool {
	return strings.Contains(ua, "Mozilla/5.0") && strings.Contains(ua, "Chrome/")
}

// ---------------------------------------------------------------------------
// 装置 4：TLS 假 anthropic 上游 + 服务启动（注入测试 CA）
// ---------------------------------------------------------------------------

type tlsUpstream struct {
	srv *httptest.Server

	mu      sync.Mutex
	status  int // 非 0 = 固定失败状态（换上游类测试用）
	hits    int
	paths   []string
	headers []http.Header
}

func newTLSClaudeUpstream(t *testing.T) *tlsUpstream {
	t.Helper()
	u := &tlsUpstream{}
	u.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		u.hits++
		u.paths = append(u.paths, r.URL.Path)
		u.headers = append(u.headers, r.Header.Clone())
		st := u.status
		u.mu.Unlock()
		if st >= 400 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(st)
			_, _ = io.WriteString(w, `{"error":{"type":"rate_limit_error","message":"slow down"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		claudeTextStream(w)
	}))
	t.Cleanup(u.srv.Close)
	return u
}

func (u *tlsUpstream) host() string { return strings.TrimPrefix(u.srv.URL, "https://") }

func (u *tlsUpstream) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.hits
}

// testCertPool 汇总若干 httptest TLS 服务器的证书为信任池（测试注入用）。
func testCertPool(t *testing.T, srvs ...*httptest.Server) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	for _, s := range srvs {
		pool.AddCert(s.Certificate())
	}
	return pool
}

// startServiceTLS 启动被测服务并注入测试 CA（自签 TLS 上游/探针的信任面）。
func startServiceTLS(t *testing.T, cfg *Config, roots *x509.CertPool) (*Server, *httptest.Server) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	s := New(cfg)
	s.testRootCAs = roots
	s.buildOutbound() // 注入信任根后重建出站客户端
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return s, srv
}

// ---------------------------------------------------------------------------
// 代理面：经代理 / 直连两形态 + 仅该上游（W11 条目 ①）
// ---------------------------------------------------------------------------

func TestUpstreamProxyHTTPConnect(t *testing.T) {
	up := newTLSClaudeUpstream(t)
	proxy := newFakeConnectProxy(t)
	cfg := failoverCfg(anthUp("proxied", up.srv.URL))
	cfg.Upstreams[0].Proxy = "http://user:secret@" + proxy.addr()
	_, srv := startServiceTLS(t, cfg, testCertPool(t, up.srv))

	code, body := postChatStream(t, srv.URL)
	if code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("经 CONNECT 代理请求失败: status=%d body=%s", code, body)
	}
	targets, auths := proxy.snapshot()
	if len(targets) != 1 || targets[0] != up.host() {
		t.Fatalf("代理 CONNECT 目标 = %v, want [%s]", targets, up.host())
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:secret"))
	if len(auths) != 1 || auths[0] != wantAuth {
		t.Fatalf("Proxy-Authorization = %v, want [%s]（userinfo 认证面）", auths, wantAuth)
	}
	if up.count() != 1 {
		t.Fatalf("上游直收请求数 = %d, want 1（经代理到达）", up.count())
	}
}

func TestUpstreamProxySocks5(t *testing.T) {
	probe := newFakeCFProbe(t)
	probe.requireGREASE = false
	probe.requireUA = false
	// 目标用 example.com（httptest 证书 SAN 含 example.com）——域名交由代理
	// 解析（socks5h 语义断言）；代理侧把域名重写回本地探针。
	targetHost := strings.Replace(probe.srv.URL, "127.0.0.1", "example.com", 1)
	socks := newFakeSocks5Proxy(t, false)
	cfg := failoverCfg(anthUp("proxied", targetHost))
	cfg.Upstreams[0].Proxy = "socks5h://" + socks.addr()
	_, srv := startServiceTLS(t, cfg, testCertPool(t, probe.srv))

	// 代理侧重写：example.com:<probe port> → 探针实际地址（模拟代理自己解析域名）。
	target := strings.TrimPrefix(targetHost, "https://")
	socks.dialOverride[target] = strings.TrimPrefix(probe.srv.URL, "https://")

	code, body := postChatStream(t, srv.URL)
	if code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("经 SOCKS5 代理请求失败: status=%d body=%s", code, body)
	}
	targets, _ := socks.snapshot()
	if len(targets) != 1 || targets[0] != target {
		t.Fatalf("SOCKS5 目标 = %v, want [%s]（域名形态——主机名交给代理解析）", targets, target)
	}
}

func TestUpstreamProxySocks5Auth(t *testing.T) {
	up := newTLSClaudeUpstream(t)
	socks := newFakeSocks5Proxy(t, true) // 要求用户名密码
	cfg := failoverCfg(anthUp("proxied", up.srv.URL))
	cfg.Upstreams[0].Proxy = "socks5://user:secret@" + socks.addr()
	_, srv := startServiceTLS(t, cfg, testCertPool(t, up.srv))

	code, body := postChatStream(t, srv.URL)
	if code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("SOCKS5 认证形态失败: status=%d body=%s", code, body)
	}
	_, users := socks.snapshot()
	if len(users) != 1 || users[0] != "user:secret" {
		t.Fatalf("SOCKS5 认证 = %v, want [user:secret]", users)
	}
}

func TestUpstreamProxyPerUpstreamIsolation(t *testing.T) {
	proxied := newTLSClaudeUpstream(t)
	proxied.status = http.StatusTooManyRequests // 换上游类失败 → failover 到第二台
	direct := newTLSClaudeUpstream(t)
	proxy := newFakeConnectProxy(t)
	cfg := failoverCfg(anthUp("proxied", proxied.srv.URL), anthUp("direct", direct.srv.URL))
	cfg.Upstreams[0].Proxy = "http://" + proxy.addr()
	_, srv := startServiceTLS(t, cfg, testCertPool(t, proxied.srv, direct.srv))

	code, body := postChatStream(t, srv.URL)
	if code != 200 || !strings.Contains(body, "Hello ") {
		t.Fatalf("failover 后应成功: status=%d body=%s", code, body)
	}
	targets, _ := proxy.snapshot()
	if len(targets) != 1 || targets[0] != proxied.host() {
		t.Fatalf("代理应只承载配了 proxy 的第一台: %v", targets)
	}
	for _, tg := range targets {
		if tg == direct.host() {
			t.Fatalf("未配 proxy 的第二台不应经代理: %v", targets)
		}
	}
	if proxied.count() != 1 || direct.count() != 1 {
		t.Fatalf("hits: proxied=%d direct=%d, want 1/1", proxied.count(), direct.count())
	}
}

func TestUpstreamProxyConfigValidation(t *testing.T) {
	cases := []struct {
		name  string
		proxy string
		ok    bool
	}{
		{"http 基本形态", "http://127.0.0.1:3128", true},
		{"http userinfo", "http://user:pass@127.0.0.1:3128", true},
		{"socks5", "socks5://127.0.0.1:1080", true},
		{"socks5h userinfo", "socks5h://user:pass@127.0.0.1:1080", true},
		{"未知 scheme", "ftp://127.0.0.1:21", false},
		{"socks5 缺端口", "socks5://127.0.0.1", false},
		{"带路径", "http://127.0.0.1:3128/x", false},
		{"带查询", "http://127.0.0.1:3128?x=1", false},
		{"全空白", "   ", false},
		{"无主机", "http://", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := failoverCfg(anthUp("x", "http://127.0.0.1:1"))
			cfg.Upstreams[0].Proxy = tc.proxy
			err := cfg.Validate()
			if tc.ok && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("Validate() = nil, want error（显式报错，绝不静默降级）")
			}
		})
	}
	// 错误消息绝不回显凭据（userinfo 密码）。
	cfg := failoverCfg(anthUp("x", "http://127.0.0.1:1"))
	cfg.Upstreams[0].Proxy = "ftp://user:supersecret@127.0.0.1:21"
	err := cfg.Validate()
	if err == nil || strings.Contains(err.Error(), "supersecret") {
		t.Fatalf("代理校验错误回显了凭据: %v", err)
	}
	// tls_fingerprint 取值域：chrome|none|空；其余显式报错。
	for _, v := range []string{"", "chrome", "none"} {
		c := failoverCfg(anthUp("x", "http://127.0.0.1:1"))
		c.Upstreams[0].TLSFingerprint = v
		if err := c.Validate(); err != nil {
			t.Fatalf("tls_fingerprint=%q: %v", v, err)
		}
	}
	c := failoverCfg(anthUp("x", "http://127.0.0.1:1"))
	c.Upstreams[0].TLSFingerprint = "firefox"
	if err := c.Validate(); err == nil {
		t.Fatalf("tls_fingerprint=firefox 应显式报错")
	}
}

// ---------------------------------------------------------------------------
// 指纹面：开关两形态（假 CF 探针 GREASE 门）+ 经代理组合 + h2 硬门（W11 条目 ②）
// ---------------------------------------------------------------------------

func TestTLSFingerprintSwitchForms(t *testing.T) {
	probe := newFakeCFProbe(t)
	probe.requireUA = false // 本测试隔离 TLS 面（UA 门见头面测试）

	// 形态 ON：chrome（直连）→ GREASE ClientHello + ALPN h2 → 探针放行。
	cfgOn := failoverCfg(anthUp("on", probe.srv.URL))
	cfgOn.Upstreams[0].TLSFingerprint = fingerprintChrome
	_, srvOn := startServiceTLS(t, cfgOn, testCertPool(t, probe.srv))
	codeOn, bodyOn := postChatStream(t, srvOn.URL)
	if codeOn != 200 || !strings.Contains(bodyOn, "Hello ") {
		t.Fatalf("chrome 形态: status=%d body=%s", codeOn, bodyOn)
	}

	// 形态 OFF：none（经 CONNECT 代理——自定义 h1 transport 需要 proxy 或指纹
	// 才存在；此形态仍是标准 crypto/tls 栈）→ 无 GREASE → 探针 403+cf-mitigated。
	proxy := newFakeConnectProxy(t)
	cfgOff := failoverCfg(anthUp("off", probe.srv.URL))
	cfgOff.Upstreams[0].TLSFingerprint = fingerprintNone
	cfgOff.Upstreams[0].Proxy = "http://" + proxy.addr()
	_, srvOff := startServiceTLS(t, cfgOff, testCertPool(t, probe.srv))
	codeOff, bodyOff := postChatStream(t, srvOff.URL)
	if codeOff != http.StatusForbidden {
		t.Fatalf("none 形态应被假 CF 拦（403+cf-mitigated）: status=%d body=%s", codeOff, bodyOff)
	}

	// 探针记录：两形态的请求面（方法/路径）一致——只有 TLS 面不同。
	reqs := probe.snapshot()
	if len(reqs) != 2 {
		t.Fatalf("探针请求数 = %d, want 2: %+v", len(reqs), reqs)
	}
	on, off := reqs[0], reqs[1]
	if on.blocked || !on.greaseOK {
		t.Fatalf("chrome 形态应过 GREASE 门: %+v", on)
	}
	if !off.blocked || off.greaseOK {
		t.Fatalf("none 形态应被 GREASE 门拦: %+v", off)
	}
	if on.method != http.MethodPost || off.method != http.MethodPost || on.path != "/v1/messages" || off.path != "/v1/messages" {
		t.Fatalf("两形态请求面不一致: on=%s %s / off=%s %s", on.method, on.path, off.method, off.path)
	}

	// hello 面：chrome 形态 GREASE + ALPN h2 + 密码套件（去 GREASE 占位）与 uTLS
	// HelloChrome_Auto 规格逐项一致；none 形态无任何 GREASE。
	hOn, ok := probe.helloFor(on.remoteAddr)
	if !ok || !hOn.grease {
		t.Fatalf("chrome hello 记录: %+v ok=%v", hOn, ok)
	}
	if !containsString(hOn.alpn, "h2") {
		t.Fatalf("chrome ALPN = %v, want 含 h2", hOn.alpn)
	}
	spec, err := utls.UTLSIdToSpec(utls.HelloChrome_Auto)
	if err != nil {
		t.Fatalf("UTLSIdToSpec: %v", err)
	}
	got, want := stripGREASE(hOn.ciphers), stripGREASE(spec.CipherSuites)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("chrome 密码套件（去 GREASE）不符:\n got %v\nwant %v", got, want)
	}
	hOff, ok := probe.helloFor(off.remoteAddr)
	if !ok || hOff.grease {
		t.Fatalf("none hello 不应有 GREASE: %+v ok=%v", hOff, ok)
	}
}

func TestTLSFingerprintViaProxies(t *testing.T) {
	probe := newFakeCFProbe(t)
	probe.requireUA = false
	pool := testCertPool(t, probe.srv)

	t.Run("http CONNECT", func(t *testing.T) {
		proxy := newFakeConnectProxy(t)
		cfg := failoverCfg(anthUp("fp", probe.srv.URL))
		cfg.Upstreams[0].TLSFingerprint = fingerprintChrome
		cfg.Upstreams[0].Proxy = "http://" + proxy.addr()
		_, srv := startServiceTLS(t, cfg, pool)
		code, body := postChatStream(t, srv.URL)
		if code != 200 || !strings.Contains(body, "Hello ") {
			t.Fatalf("chrome+CONNECT: status=%d body=%s", code, body)
		}
		targets, _ := proxy.snapshot()
		if len(targets) != 1 {
			t.Fatalf("CONNECT 目标 = %v, want 1 次", targets)
		}
	})
	t.Run("socks5", func(t *testing.T) {
		socks := newFakeSocks5Proxy(t, false)
		cfg := failoverCfg(anthUp("fp", probe.srv.URL))
		cfg.Upstreams[0].TLSFingerprint = fingerprintChrome
		cfg.Upstreams[0].Proxy = "socks5h://" + socks.addr()
		_, srv := startServiceTLS(t, cfg, pool)
		code, body := postChatStream(t, srv.URL)
		if code != 200 || !strings.Contains(body, "Hello ") {
			t.Fatalf("chrome+socks5: status=%d body=%s", code, body)
		}
		targets, _ := socks.snapshot()
		if len(targets) != 1 {
			t.Fatalf("SOCKS5 目标 = %v, want 1 次", targets)
		}
	})

	// 两形态都过了 GREASE 门（探针记录）。
	reqs := probe.snapshot()
	if len(reqs) != 2 {
		t.Fatalf("探针请求数 = %d, want 2", len(reqs))
	}
	for i, r := range reqs {
		if r.blocked || !r.greaseOK {
			t.Fatalf("chrome 经代理[%d]应过 GREASE 门: %+v", i, r)
		}
	}
}

func TestTLSFingerprintRequiresHTTP2(t *testing.T) {
	up := newTLSClaudeUpstream(t) // httptest 缺省 NextProtos = http/1.1（h1-only）
	cfg := failoverCfg(anthUp("fp", up.srv.URL))
	cfg.Upstreams[0].TLSFingerprint = fingerprintChrome
	_, srv := startServiceTLS(t, cfg, testCertPool(t, up.srv))

	code, body := postChatStream(t, srv.URL)
	if code != http.StatusBadGateway || !strings.Contains(body, "HTTP/2") {
		t.Fatalf("h1-only 上游应显式报 HTTP/2 要求: status=%d body=%s", code, body)
	}
}

func TestTLSFingerprintModesAndWiring(t *testing.T) {
	// 缺省 none（全协议）——W11-2 证据：Go 默认栈过 CF；chrome 仅显式开关。
	if got := (Upstream{Protocol: ProtocolClaudeWeb}).tlsFingerprintMode(); got != fingerprintNone {
		t.Fatalf("claude_web 缺省指纹 = %q, want none", got)
	}
	if got := (Upstream{Protocol: ProtocolAnthropic}).tlsFingerprintMode(); got != fingerprintNone {
		t.Fatalf("anthropic 缺省指纹 = %q, want none", got)
	}
	if got := (Upstream{Protocol: ProtocolClaudeWeb, TLSFingerprint: fingerprintChrome}).tlsFingerprintMode(); got != fingerprintChrome {
		t.Fatalf("显式 chrome 未生效: %q", got)
	}

	// 接线：chrome → 专用 client（upstreamEgress 指纹面）；缺省 → 共享 client。
	cfg := failoverCfg(anthUp("fp", "https://example.invalid"))
	cfg.Upstreams[0].TLSFingerprint = fingerprintChrome
	s := New(cfg)
	if s.clients[0] == nil {
		t.Fatalf("chrome 上游应有专用出站客户端")
	}
	eg, ok := s.clients[0].Transport.(*upstreamEgress)
	if !ok || !eg.fingerprint {
		t.Fatalf("chrome 上游 transport = %T, want *upstreamEgress{fingerprint:true}", s.clients[0].Transport)
	}
	if c, err := s.clientFor(0); err != nil || c != s.clients[0] {
		t.Fatalf("clientFor(0) = %v, %v", c, err)
	}

	cfg2 := failoverCfg(anthUp("plain", "https://example.invalid"))
	s2 := New(cfg2)
	if s2.clients[0] != nil {
		t.Fatalf("缺省上游不该有专用出站客户端（共享 client 语义不变）")
	}
	if c, err := s2.clientFor(0); err != nil || c != s2.client {
		t.Fatalf("缺省上游应复用共享 client: %v, %v", c, err)
	}

	// proxy 配置 → 专用 client（h1 transport）。
	cfg3 := failoverCfg(anthUp("prox", "https://example.invalid"))
	cfg3.Upstreams[0].Proxy = "http://127.0.0.1:3128"
	s3 := New(cfg3)
	if s3.clients[0] == nil {
		t.Fatalf("配了 proxy 的上游应有专用出站客户端")
	}
	if _, ok := s3.clients[0].Transport.(*http.Transport); !ok {
		t.Fatalf("proxy-only 上游 transport = %T, want *http.Transport", s3.clients[0].Transport)
	}
}

// ---------------------------------------------------------------------------
// 头面：claude_web 三端点全量浏览器头面（假 CF UA 门；W11-2 实测的 CF 触发因子）
// ---------------------------------------------------------------------------

func TestClaudeWebHeaderFaceAtProbe(t *testing.T) {
	probe := newFakeCFProbe(t)
	probe.requireGREASE = false // claude_web 缺省无指纹（W11-2：Go 默认栈过 CF）
	probe.completionSSE = claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")

	cfg := &Config{Listen: "127.0.0.1:0", HeartbeatSeconds: 15, Upstreams: []Upstream{{
		Name:     "claude-web",
		Protocol: ProtocolClaudeWeb,
		BaseURL:  probe.srv.URL,
		APIKey:   "sk-ant-sid02-test-session-key",
	}}}
	_, srv := startServiceTLS(t, cfg, testCertPool(t, probe.srv))

	code, body := postChatStream(t, srv.URL)
	if code != 200 || !strings.Contains(body, "PONG") {
		t.Fatalf("claude_web 头面应过假 CF 的 UA 门: status=%d body=%s", code, body)
	}

	reqs := probe.snapshot()
	wantPaths := []string{
		"/api/organizations",
		"/api/organizations/" + claudeWebOrgUUID + "/chat_conversations",
		"/api/organizations/" + claudeWebOrgUUID + "/chat_conversations/" + claudeWebConvUUID + "/completion",
		// W13 清理面：completion 收尾后的异步 DELETE（缺省开——用完即删）。
		"/api/organizations/" + claudeWebOrgUUID + "/chat_conversations/" + claudeWebConvUUID,
	}
	// 清理 DELETE 异步发出——有界等待四调用齐（固定计数断言必须等，否则竞态）。
	deadline := time.Now().Add(5 * time.Second)
	for len(reqs) < len(wantPaths) {
		if time.Now().After(deadline) {
			t.Fatalf("探针请求数 = %d, want %d: %+v", len(reqs), len(wantPaths), reqs)
		}
		time.Sleep(2 * time.Millisecond)
		reqs = probe.snapshot()
	}
	if len(reqs) != len(wantPaths) {
		t.Fatalf("探针请求数 = %d, want %d: %+v", len(reqs), len(wantPaths), reqs)
	}
	for i, r := range reqs {
		if r.blocked {
			t.Fatalf("调用[%d] 被假 CF 拦: %+v", i, r)
		}
		if r.path != wantPaths[i] {
			t.Fatalf("调用[%d] path = %q, want %q", i, r.path, wantPaths[i])
		}
		checks := map[string]string{
			"User-Agent":                claudeWebUA,
			"sec-ch-ua":                 `"Chromium";v="155", "Not(A:Brand";v="24"`,
			"sec-ch-ua-mobile":          "?0",
			"sec-ch-ua-platform":        `"Linux"`,
			"Accept-Language":           "en-US,en;q=0.9",
			"Priority":                  "u=1, i",
			"Sec-Fetch-Dest":            "empty",
			"Sec-Fetch-Mode":            "cors",
			"Sec-Fetch-Site":            "same-origin",
			"anthropic-client-platform": "web_claude_ai",
			"anthropic-client-version":  "1.0.0",
			"anthropic-client-sha":      "3d8a03e3010c5025c6b2d997970df8ffbd9ce966",
			"Origin":                    probe.srv.URL,
		}
		for k, want := range checks {
			if got := r.headers.Get(k); got != want {
				t.Fatalf("调用[%d] %s = %q, want %q", i, k, got, want)
			}
		}
		// referer 按阶段：org 发现/创建/completion 首回合 = `/new`；DELETE = `/chat/{conv}`
		//（BK/CW/YS 三方同形——删除阶段在会话页）。
		wantReferer := probe.srv.URL + "/new"
		if i == len(wantPaths)-1 {
			wantReferer = probe.srv.URL + "/chat/" + claudeWebConvUUID
		}
		if got := r.headers.Get("Referer"); got != wantReferer {
			t.Fatalf("调用[%d] Referer = %q, want %q", i, got, wantReferer)
		}
		if got := r.headers.Get("Cookie"); got != "sessionKey=sk-ant-sid02-test-session-key" {
			t.Fatalf("调用[%d] Cookie = %q", i, got)
		}
		if anon := r.headers.Get("anthropic-anonymous-id"); !strings.HasPrefix(anon, "claudeai.v1.") {
			t.Fatalf("调用[%d] anthropic-anonymous-id = %q", i, anon)
		}
		if r.headers.Get("anthropic-device-id") == "" {
			t.Fatalf("调用[%d] 缺 anthropic-device-id", i)
		}
	}
	// 端点面差异：completion 恒 SSE（Accept + Content-Type）；GET 无 Content-Type。
	if got := reqs[2].headers.Get("Accept"); got != "text/event-stream" {
		t.Fatalf("completion Accept = %q, want text/event-stream", got)
	}
	if got := reqs[2].headers.Get("Content-Type"); got != "application/json" {
		t.Fatalf("completion Content-Type = %q", got)
	}
	if got := reqs[0].headers.Get("Accept"); got != "*/*" {
		t.Fatalf("org 发现 Accept = %q, want */*", got)
	}
	if got := reqs[0].headers.Get("Content-Type"); got != "" {
		t.Fatalf("org 发现不该带 Content-Type: %q", got)
	}
}

// stripGREASE 去掉 GREASE 占位值（0x?a?a；uTLS 规格里是占位常量，握手时被随机
// 替换——比较两侧都剥掉才可比）。
func stripGREASE(in []uint16) []uint16 {
	out := make([]uint16, 0, len(in))
	for _, v := range in {
		if !isGREASEValue(v) {
			out = append(out, v)
		}
	}
	return out
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
