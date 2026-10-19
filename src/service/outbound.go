// 上游出站硬化（W11）：per-upstream 代理 + TLS 指纹（开关）。
//
// 背景（证据在库，按最新证据定缺省）：
//   - W11-2 实测（2026-10-14，真 claude.ai + 家宽出口，见
//     research/briefs/reports/A2A-L11-W11-2.md §3）：**Go 默认 TLS 栈
//     `GET /api/organizations` = 200**（h1 与强制 h2 双过，无 `cf-mitigated`）；
//     403 的触发因子是 **UA/头面**（同 curl 仅换 Chrome UA 即 403→200），TLS
//     指纹与 ALPN 均非判定因子。→ **指纹缺省 = none（全部协议）**；头面装配是
//     CF 门的正解（见 claude_web.go 的 applyClaudeWebHeaders）。
//   - W9-1 旧结论（CR 实测：httpx 403 / curl_cffi 200 → 「拦 Python TLS 指纹」）
//     已被 W11-2 改写：httpx 403 同样可用其默认 UA 解释 [INFERENCE]。
//   - `chrome` 开关保留（「保持双路」）：completion POST 面的 CF 档未实测——
//     真机若 403，配置一行 `tls_fingerprint: chrome` 切 uTLS Chrome ClientHello。
//     参照 BK（basketikun/claude2api 用 bogdanfinn/tls-client，profiles.Chrome_146，
//     `internal/service/claude.go:46-52`）；本仓选 **uTLS**
//     （refraction-networking/utls）+ net/http/x/net 栈：tls-client 需要整套
//     fhttp 客户端替换（依赖树含 quic-go/websocket 等），而 uTLS 只需替换
//     **TLS 握手层**（`DialTLSContext`），出站仍走 net/http 语义——最小依赖、
//     最小行为偏移。两形态测试与反证见 reports/A2A-L19-W11-1.md。
//
// 结构（每上游一个出站客户端；未配代理且无指纹 = 共享 client，行为不变）：
//
//	Upstream.Proxy ──┬─ nil + 指纹 none → 共享 client（现状；env 代理仍生效）
//	                 ├─ 有值 + 指纹 none → h1 transport（net/http 原生代理：
//	                 │     http CONNECT / socks5 / socks5h + userinfo 认证；
//	                 │     https 目标的 TLS = crypto/tls 标准栈）
//	                 └─ 指纹 chrome → upstreamEgress：
//	                       https 目标 → uTLS Chrome ClientHello + 真 HTTP/2
//	                       （代理由本层自己拨：http CONNECT / socks5）；
//	                       http 目标 → h1（无 TLS 层可指纹化）
//
// 纪律：
//   - 代理凭据（userinfo）绝不进日志/错误消息（错误消息只含 scheme/host 类别）；
//   - 配了代理就必须走代理——构造失败 = 该上游出站显式报错，绝不静默直连
//     （Validate 在启动时挡掉非法配置，构造错误是防御面）；
//   - 开关只影响该上游出站（下游 / 其它上游逐字节不受影响）。
package service

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	utls "github.com/refraction-networking/utls"
	"golang.org/x/net/http2"
	"golang.org/x/net/proxy"
)

// 出站 TLS 指纹模式（Upstream.TLSFingerprint 的取值域）。
const (
	// fingerprintNone 标准库 TLS 栈（引入前行为；显式关闭用）。
	fingerprintNone = "none"
	// fingerprintChrome uTLS Chrome ClientHello（HelloChrome_Auto = Chrome 133，
	// uTLS v1.8.2）+ 真 HTTP/2（ALPN h2——JA4 的 ALPN 面与 Chrome 一致）。
	fingerprintChrome = "chrome"
)

// upstreamHeaderWait 上游响应头等待（与共享 transport 的 ResponseHeaderTimeout
// 同值同义：只约束响应头，不约束流式体——流式长连接不设整体超时）。
const upstreamHeaderWait = 60 * time.Second

// tlsFingerprintMode 有效指纹模式：显式值优先；空 = 协议缺省 **none**（全部
// 协议）。缺省口径依据 W11-2 实测（2026-10-14，真 claude.ai + 家宽出口）：
// **Go 默认 TLS 栈打 `GET /api/organizations` = 200**（h1/h2 双过，无
// `cf-mitigated`）——CF 的 403 触发因子是 UA/头面而非 TLS 指纹（同 curl 仅换
// Chrome UA 即 403→200）；`chrome` 保留为**显式开关**（「保持双路」：completion
// POST 面的 CF 档未实测，若真机 403 再翻此开关，见 reports/A2A-L19-W11-1.md）。
func (u Upstream) tlsFingerprintMode() string {
	if u.TLSFingerprint != "" {
		return u.TLSFingerprint
	}
	return fingerprintNone
}

// parseUpstreamProxy 解析并校验 Upstream.Proxy（Validate 与出站构造共用同一
// 判据）。**错误消息绝不含 userinfo**（url 原文可能带密码——凭据纪律）。
func parseUpstreamProxy(raw string) (*url.URL, error) {
	if raw == "" {
		return nil, nil
	}
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("proxy is blank")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("invalid proxy url") // 不回显原文（可能含凭据）
	}
	switch u.Scheme {
	case "http":
	case "socks5", "socks5h":
		if u.Port() == "" {
			return nil, errors.New("socks5 proxy requires an explicit port (host:port)")
		}
	default:
		return nil, fmt.Errorf("unsupported proxy scheme %q (want http|socks5|socks5h)", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("proxy host is required")
	}
	if u.Path != "" && u.Path != "/" {
		return nil, errors.New("proxy url must not contain a path")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("proxy url must not contain query or fragment")
	}
	return u, nil
}

// envProxyFor 进程环境代理解析（https 目标语义——本层只用于 https 目标的指纹
// 出站；与 net/http 的 ProxyFromEnvironment 同源：NO_PROXY / 回环地址恒直连）。
func envProxyFor(addr string) *url.URL {
	req := &http.Request{URL: &url.URL{Scheme: "https", Host: addr}}
	p, err := http.ProxyFromEnvironment(req)
	if err != nil {
		return nil
	}
	return p
}

// fingerprintDialer 指纹出站的拨号链：显式代理（up.Proxy）优先，未配置时按
// 环境代理兜底（与共享 transport 语义一致），都无 = 直连。
type fingerprintDialer struct {
	explicit *url.URL // 已校验的显式代理；nil = 环境/直连
	roots    *x509.CertPool
	direct   net.Dialer
}

// dial 建立到目标（host:port）的原始连接；https 目标的 TLS 由
// dialFingerprintTLS 在其上做（uTLS）。
func (d *fingerprintDialer) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	p := d.explicit
	if p == nil {
		p = envProxyFor(addr)
	}
	if p == nil {
		return d.direct.DialContext(ctx, network, addr)
	}
	switch p.Scheme {
	case "socks5", "socks5h":
		return dialSocks5(ctx, p, addr, &d.direct)
	case "http":
		return dialHTTPConnect(ctx, p, addr, &d.direct)
	default:
		// 防御：Validate 已挡未知 scheme（构造期显式报错，绝不静默直连）。
		return nil, fmt.Errorf("unsupported proxy scheme %q", p.Scheme)
	}
}

// dialFingerprintTLS 建立指纹 TLS 连接：拨号（直连/代理）→ uTLS Chrome
// ClientHello 握手 → 断言 h2 协商（http2.Transport 的前提；上游不支持 h2 时
// 显式报错，绝不降级到 h1——降级会破坏 ALPN 指纹一致性）。
func (d *fingerprintDialer) dialFingerprintTLS(ctx context.Context, network, addr string) (net.Conn, error) {
	raw, err := d.dial(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("fingerprint dial: invalid addr %q", addr)
	}
	cfg := &utls.Config{ServerName: host}
	if d.roots != nil {
		cfg.RootCAs = d.roots // 仅测试注入（自签测试 CA）；生产 nil = 系统根证书
	}
	uconn := utls.UClient(raw, cfg, utls.HelloChrome_Auto)
	if err := uconn.HandshakeContext(ctx); err != nil {
		raw.Close()
		return nil, fmt.Errorf("tls fingerprint handshake: %w", err)
	}
	if proto := uconn.ConnectionState().NegotiatedProtocol; proto != "h2" {
		raw.Close()
		return nil, fmt.Errorf("tls fingerprint requires an HTTP/2 upstream (negotiated %q)", proto)
	}
	return uconn, nil
}

// dialHTTPConnect 经 HTTP 代理建 CONNECT 隧道到 addr（https 目标专用形态；
// 与 net/http 自身 CONNECT 请求同形——`Request{Method: CONNECT, URL.Opaque: addr}`）。
func dialHTTPConnect(ctx context.Context, p *url.URL, addr string, direct *net.Dialer) (net.Conn, error) {
	conn, err := direct.DialContext(ctx, "tcp", p.Host)
	if err != nil {
		return nil, fmt.Errorf("proxy dial %s: %w", p.Host, err)
	}
	hdr := make(http.Header)
	if u := p.User; u != nil {
		pass, _ := u.Password()
		hdr.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(u.Username()+":"+pass)))
	}
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: addr},
		Host:   addr,
		Header: hdr,
	}
	if err := req.Write(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("proxy CONNECT write: %w", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("proxy CONNECT read: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		conn.Close()
		return nil, fmt.Errorf("proxy CONNECT rejected: status %d", resp.StatusCode)
	}
	// CONNECT 成功后 bufio 可能已预读隧道字节——接回读路径。
	return &bufferedConn{Conn: conn, r: br}, nil
}

// bufferedConn 把 bufio 预读的字节接回 Read（CONNECT 隧道用）。
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) { return c.r.Read(b) }

// dialSocks5 经 SOCKS5 代理拨号（x/net/proxy：目标主机名交由代理解析——即
// socks5h 语义；本实现对 socks5:// 与 socks5h:// 同款处理）。
func dialSocks5(ctx context.Context, p *url.URL, addr string, direct *net.Dialer) (net.Conn, error) {
	var auth *proxy.Auth
	if u := p.User; u != nil {
		pass, _ := u.Password()
		auth = &proxy.Auth{User: u.Username(), Password: pass}
	}
	d, err := proxy.SOCKS5("tcp", p.Host, auth, direct)
	if err != nil {
		return nil, fmt.Errorf("socks5 proxy: %w", err)
	}
	if cd, ok := d.(proxy.ContextDialer); ok {
		return cd.DialContext(ctx, "tcp", addr)
	}
	return d.Dial("tcp", addr)
}

// upstreamEgress 单个上游的出站 RoundTripper（W11）：
//   - 指纹开 + https 目标 → uTLS Chrome + 真 h2（代理由 fingerprintDialer 拨）；
//   - 其余（指纹关 / http 目标）→ h1 标准栈（net/http 原生代理处理）。
//
// 响应头等待（60s，与共享 transport 对齐）在 h2 面用看门狗补齐——
// http2.Transport 无 ResponseHeaderTimeout 字段（仅约束响应头到达，不约束流式体）。
type upstreamEgress struct {
	fingerprint bool
	h1          *http.Transport
	h2          *http2.Transport
	headerWait  time.Duration
}

func (e *upstreamEgress) RoundTrip(req *http.Request) (*http.Response, error) {
	if !e.fingerprint || req.URL.Scheme != "https" {
		return e.h1.RoundTrip(req)
	}
	ctx, cancel := context.WithCancel(req.Context())
	var timer *time.Timer
	if e.headerWait > 0 {
		timer = time.AfterFunc(e.headerWait, cancel)
	}
	resp, err := e.h2.RoundTrip(req.WithContext(ctx))
	fired := timer != nil && !timer.Stop()
	if err != nil {
		cancel()
		if fired {
			return nil, fmt.Errorf("upstream response header timeout (%s)", e.headerWait)
		}
		return nil, err
	}
	if fired {
		// 响应头恰在超时同刻到达：保守按超时上抛（体可能已被 cancel 打断）。
		cancel()
		_ = resp.Body.Close()
		return nil, fmt.Errorf("upstream response header timeout (%s)", e.headerWait)
	}
	resp.Body = &cancelOnCloseBody{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

// cancelOnCloseBody 响应体收尾时释放看门狗 context（防泄漏；Close 由
// forward/relay 的 defer 恒调用）。
type cancelOnCloseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelOnCloseBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

// newUpstreamClient 按上游配置构造出站客户端（W11）。返回 (nil, nil) = 该上游
// 无需专用出站（直连 + 无指纹 → 共享 client，行为与引入前逐字节一致）。
func (s *Server) newUpstreamClient(up Upstream) (*http.Client, error) {
	pu, err := parseUpstreamProxy(up.Proxy)
	if err != nil {
		return nil, err
	}
	fp := up.tlsFingerprintMode()
	if pu == nil && fp == fingerprintNone {
		return nil, nil
	}

	// h1 面：代理走 net/http 原生（http CONNECT / socks5 / socks5h + userinfo
	// 认证都在标准库内）；指纹关时的 https 也走这里（crypto/tls 标准栈）。
	proxyFunc := http.ProxyFromEnvironment
	if pu != nil {
		proxyFunc = http.ProxyURL(pu)
	}
	h1 := &http.Transport{
		Proxy:                 proxyFunc,
		ResponseHeaderTimeout: upstreamHeaderWait,
	}
	if s.testRootCAs != nil {
		// 仅测试注入：信任自签测试 CA（生产 nil = 系统根证书 + 正常校验）。
		h1.TLSClientConfig = &tls.Config{RootCAs: s.testRootCAs}
	}

	rt := http.RoundTripper(h1)
	if fp == fingerprintChrome {
		dialer := &fingerprintDialer{explicit: pu, roots: s.testRootCAs}
		h2 := &http2.Transport{
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return dialer.dialFingerprintTLS(ctx, network, addr)
			},
		}
		rt = &upstreamEgress{fingerprint: true, h1: h1, h2: h2, headerWait: upstreamHeaderWait}
	}
	return &http.Client{Transport: rt}, nil
}

// buildOutbound 构造每个上游的出站客户端（New 与测试注入信任根后重建用）。
// 构建失败进 clientErrs（防御面——Validate 已在启动时挡掉非法配置）。
func (s *Server) buildOutbound() {
	// 仅测试注入：共享 client 也信任自签测试 CA（TLS 假上游的直连形态）。
	// 生产恒 nil（系统根证书 + 正常校验）。
	if s.testRootCAs != nil {
		s.client = &http.Client{Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			ResponseHeaderTimeout: upstreamHeaderWait,
			TLSClientConfig:       &tls.Config{RootCAs: s.testRootCAs},
		}}
	}
	s.clients = make([]*http.Client, len(s.cfg.Upstreams))
	s.clientErrs = make([]error, len(s.cfg.Upstreams))
	for i, up := range s.cfg.Upstreams {
		c, err := s.newUpstreamClient(up)
		s.clients[i], s.clientErrs[i] = c, err
	}
}

// clientFor 该上游（cfg.Upstreams 下标）的出站客户端。未配代理/指纹 → 共享
// client；构建错误 → 显式报错（绝不静默直连——配了代理就必须走代理）。
func (s *Server) clientFor(idx int) (*http.Client, error) {
	if idx < len(s.clientErrs) && s.clientErrs[idx] != nil {
		return nil, fmt.Errorf("upstream %s: %w", s.cfg.Upstreams[idx].Name, s.clientErrs[idx])
	}
	if idx < len(s.clients) && s.clients[idx] != nil {
		return s.clients[idx], nil
	}
	return s.client, nil
}
