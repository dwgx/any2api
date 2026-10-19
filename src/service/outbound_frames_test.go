// W54-2 帧观测台（h2 帧级断言）：出站客户端「我们实际发什么帧」的字节级观测
// 测试——本地回环 TLS 终结 + 手工 h2 服务器（golang.org/x/net/http2 Framer +
// hpack），两档各跑一次：
//
//   - std 档（tls_fingerprint none）：服务共享 client（none + 无代理 →
//     newUpstreamClient 返回 nil → 请求期落共享 client；经 buildOutbound 注入
//     测试 CA）；
//   - chrome 档（tls_fingerprint chrome）：upstreamEgress（uTLS Chrome
//     ClientHello + x/net http2.Transport）。
//
// 观测面（补 W11-1 假 CF 探针的自述缺口——「原始 ClientHello 与 h2 SETTINGS
// 不可测」，见 reports/A2A-L19-W11-1.md §4.1；W53-3 的 h2 五维对照表此前只有
// 源码值，无字节级断言）：
//   - TLS 层（server 侧可观测）：ALPN offers（GetConfigForClient 协商前原始
//     列表）、协商结果、密码套件；
//   - h2 层五维：SETTINGS 键值 + 键序、连接级 WINDOW_UPDATE、PRIORITY 帧有无、
//     伪头序（HPACK 解码 :method/:scheme/:authority/:path 的顺序）。
//
// 断言 = 当前实测值（防回归基线；与 Chrome 目标值的差异表在
// research/briefs/reports/A2A-L19-W54-2.md）。反证面：两档都**不得**出现
// Chrome h2 目标值（半真形态的字节级证据：TLS 真、h2 全 Go）。
//
// 纪律：全本地回环、零外网；零 outbound.go 行为改动（只观测）。
//
// 测试侧等价性说明（std 档）：测试注入自签 CA 需要 TLSClientConfig 非 nil，
// 这会命中 net/http 的保守路径（自定义 TLS 配置 → 不自动启用 h2，Issue
// 14275）；测试以 ForceAttemptHTTP2 恢复「生产缺省（TLSClientConfig==nil →
// auto-h2）」的等价协议形态——h2 栈仍为 net/http 内置副本、帧面与生产一致，
// 差异仅在信任根（测试 CA，不影响任何帧字节）。
package service

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// ---------------------------------------------------------------------------
// 装置：帧观测台（TLS 终结 + 手工 h2）
// ---------------------------------------------------------------------------

// framesBooth 帧观测台：接受一次出站客户端连接，记录 TLS 面 + h2 面观测值。
type framesBooth struct {
	baseURL string // https://127.0.0.1:<port>（上游 base_url 与请求 URL 用）
	pool    *x509.CertPool

	ln   net.Listener
	done chan struct{}

	mu  sync.Mutex
	rec framesRecord
}

// framesRecord 一次连接的观测记录（字段零值 = 未观测到）。
type framesRecord struct {
	// TLS 层（server 侧可观测）
	alpnOffers  []string // 协商前原始 ALPN 列表（ClientHelloInfo.SupportedProtos）
	negotiated  string   // 协商结果（ALPN）
	tlsVersion  uint16   // 协商 TLS 版本
	cipherSuite uint16   // 协商密码套件

	// h2 层（单连接内帧有序；到达序 = 发送序）
	settings    []http2.Setting // SETTINGS 键值 + 键序
	connWindow  []uint32        // 连接级 WINDOW_UPDATE 增量（stream 0）
	priorityN   int             // PRIORITY 帧计数
	pseudoOrder []string        // 请求 HEADERS 伪头序（HPACK 解码）
	ackSeen     bool            // 客户端对服务端 SETTINGS 的 ACK
	frameSeq    []string        // 全部帧类型序列（边界断言用）

	err error
}

// newFramesBooth 起观测台（自签证书；仅本地回环）。服务端首个 SETTINGS 为空
// 缺省（镜像真实服务端形态）；观测面见 framesRecord。
func newFramesBooth(t *testing.T) *framesBooth {
	t.Helper()
	cert, pool := framesBoothCert(t)
	b := &framesBooth{pool: pool, done: make(chan struct{})}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"h2"},
		GetConfigForClient: func(chi *tls.ClientHelloInfo) (*tls.Config, error) {
			b.mu.Lock()
			b.rec.alpnOffers = append([]string(nil), chi.SupportedProtos...)
			b.mu.Unlock()
			return nil, nil // 继续用基础配置（NextProtos = h2）
		},
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("booth listen: %v", err)
	}
	b.ln = ln
	b.baseURL = "https://" + ln.Addr().String()
	t.Cleanup(func() { _ = ln.Close() })
	go b.serveOne()
	return b
}

// wait 等观测记录（观测台在「响应已回 + 客户端 SETTINGS ACK」收齐后收尾；
// 上限 10s 防挂）。
func (b *framesBooth) wait(t *testing.T) framesRecord {
	t.Helper()
	select {
	case <-b.done:
	case <-time.After(10 * time.Second):
		t.Fatal("帧观测台超时（10s 未收尾）")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rec.err != nil {
		t.Fatalf("帧观测台错误: %v", b.rec.err)
	}
	return b.rec
}

func (b *framesBooth) setErr(err error) {
	b.mu.Lock()
	if b.rec.err == nil {
		b.rec.err = err
	}
	b.mu.Unlock()
}

// serveOne 单连接：TLS 握手 → 记录协商结果 → 手工 h2 握手（preface → SETTINGS
// → 帧循环）→ 请求 HEADERS 到达后回 200 空体 → 等客户端 SETTINGS ACK 收尾。
func (b *framesBooth) serveOne() {
	defer close(b.done)
	conn, err := b.ln.Accept()
	if err != nil {
		b.setErr(fmt.Errorf("accept: %w", err))
		return
	}
	defer conn.Close()

	tconn, ok := conn.(*tls.Conn)
	if !ok {
		b.setErr(fmt.Errorf("accepted conn %T, want *tls.Conn", conn))
		return
	}
	if err := tconn.Handshake(); err != nil {
		b.setErr(fmt.Errorf("TLS handshake: %w", err))
		return
	}
	cs := tconn.ConnectionState()
	b.mu.Lock()
	b.rec.negotiated = cs.NegotiatedProtocol
	b.rec.tlsVersion = cs.Version
	b.rec.cipherSuite = cs.CipherSuite
	b.mu.Unlock()

	// 客户端 preface（24 字节；Framer 之前手工消费）。
	pre := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(conn, pre); err != nil {
		b.setErr(fmt.Errorf("read client preface: %w", err))
		return
	}
	if string(pre) != http2.ClientPreface {
		b.setErr(fmt.Errorf("client preface = %q", pre))
		return
	}

	fr := http2.NewFramer(conn, conn)
	// 服务端首个帧：SETTINGS（空 = 全缺省）。
	if err := fr.WriteSettings(); err != nil {
		b.setErr(fmt.Errorf("write server settings: %w", err))
		return
	}

	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	dec := hpack.NewDecoder(4096, nil)
	var (
		hdrFrag   []byte
		reqStream uint32
		responded bool
		ackSent   bool
	)
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			if responded && (isTimeoutErr(err) || errors.Is(err, io.EOF)) {
				return // 响应后收尾（ACK 已到或短窗超时/对端收尾）
			}
			b.setErr(fmt.Errorf("read frame: %w", err))
			return
		}

		b.mu.Lock()
		switch f := f.(type) {
		case *http2.SettingsFrame:
			if f.IsAck() {
				b.rec.ackSeen = true
				b.rec.frameSeq = append(b.rec.frameSeq, "SETTINGS-ACK")
			} else {
				b.rec.frameSeq = append(b.rec.frameSeq, "SETTINGS")
				for i := range f.NumSettings() {
					b.rec.settings = append(b.rec.settings, f.Setting(i))
				}
			}
		case *http2.WindowUpdateFrame:
			b.rec.frameSeq = append(b.rec.frameSeq, fmt.Sprintf("WINDOW_UPDATE(%d)", f.StreamID))
			if f.StreamID == 0 {
				b.rec.connWindow = append(b.rec.connWindow, f.Increment)
			}
		case *http2.PriorityFrame:
			b.rec.frameSeq = append(b.rec.frameSeq, "PRIORITY")
			b.rec.priorityN++
		case *http2.HeadersFrame:
			b.rec.frameSeq = append(b.rec.frameSeq, "HEADERS")
			hdrFrag = append(hdrFrag, f.HeaderBlockFragment()...)
			reqStream = f.StreamID
			if f.HeadersEnded() {
				fields, derr := dec.DecodeFull(hdrFrag)
				if derr != nil {
					b.rec.err = fmt.Errorf("hpack decode: %w", derr)
				}
				for _, hf := range fields {
					if strings.HasPrefix(hf.Name, ":") {
						b.rec.pseudoOrder = append(b.rec.pseudoOrder, hf.Name)
					}
				}
			}
		default:
			b.rec.frameSeq = append(b.rec.frameSeq, f.Header().Type.String())
		}
		b.mu.Unlock()

		// 动作面（I/O 放锁外）。
		switch f := f.(type) {
		case *http2.SettingsFrame:
			if !f.IsAck() && !ackSent {
				if err := fr.WriteSettingsAck(); err != nil {
					b.setErr(fmt.Errorf("write settings ack: %w", err))
					return
				}
				ackSent = true
			}
		case *http2.HeadersFrame:
			if f.HeadersEnded() && !responded {
				var buf bytes.Buffer
				enc := hpack.NewEncoder(&buf)
				if err := enc.WriteField(hpack.HeaderField{Name: ":status", Value: "200"}); err != nil {
					b.setErr(fmt.Errorf("hpack encode: %w", err))
					return
				}
				if err := fr.WriteHeaders(http2.HeadersFrameParam{
					StreamID:      reqStream,
					BlockFragment: buf.Bytes(),
					EndHeaders:    true,
					EndStream:     true,
				}); err != nil {
					b.setErr(fmt.Errorf("write response: %w", err))
					return
				}
				responded = true
				_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			}
		}

		b.mu.Lock()
		done := responded && b.rec.ackSeen
		b.mu.Unlock()
		if done {
			return
		}
	}
}

// framesBoothCert 自签服务端证书（SAN：127.0.0.1 / ::1 / localhost；ECDSA
// P-256）+ 对应信任池——两档客户端的信任面（测试注入）。
func framesBoothCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("booth key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(0x54),
		Subject:               pkix.Name{CommonName: "any2api-frames-booth"},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("booth cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("booth cert parse: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}

// isTimeoutErr 读超时判定（观测台响应后的短窗收尾用）。
func isTimeoutErr(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// settingsString SETTINGS 的「键值 + 键序」紧凑表示（失败消息与日志用）。
func settingsString(ss []http2.Setting) string {
	parts := make([]string, len(ss))
	for i, s := range ss {
		parts[i] = fmt.Sprintf("%d=%d", s.ID, s.Val)
	}
	return strings.Join(parts, ",")
}

// ---------------------------------------------------------------------------
// 期望值 = 当前实测基线（Go 默认 h2 栈；x/net v0.61.0 与 net/http 内置副本
// go1.26.7 同值；Akamai 论文 Go-http-client 行共同键逐项同值，本版另含
// MAX_FRAME_SIZE=16384）：
//
//   - SETTINGS 键序/键值：ENABLE_PUSH=0 → INITIAL_WINDOW_SIZE=4<<20 →
//     MAX_FRAME_SIZE=16384（1<<14；未设值先被 minMaxFrameSize 裁剪、setDefault
//     不再覆写——字节级实测。注：W53-3 源码表记 1<<20 = defaultMaxReadFrameSize，
//     未计裁剪路径；本单以实测为准）→ MAX_HEADER_LIST_SIZE=10<<20；
//   - 连接级 WINDOW_UPDATE = 1<<30（transportDefaultConnFlow）；
//   - 无 PRIORITY 帧（客户端默认不发）；
//   - 伪头序 = a,m,p,s（:authority :method :path :scheme——与 Chrome 的
//     masp 不同，见报告差异表）。
// ---------------------------------------------------------------------------

var (
	wantGoSettings = []http2.Setting{
		{ID: http2.SettingEnablePush, Val: 0},
		{ID: http2.SettingInitialWindowSize, Val: 4 << 20},
		{ID: http2.SettingMaxFrameSize, Val: 1 << 14}, // minMaxFrameSize 裁剪值（实测 16384）
		{ID: http2.SettingMaxHeaderListSize, Val: 10 << 20},
	}
	wantGoPseudo = []string{":authority", ":method", ":path", ":scheme"}
	wantALPN     = []string{"h2", "http/1.1"}
	wantGoConnWU = uint32(1 << 30)
)

// ---------------------------------------------------------------------------
// 两档观测（服务真实构造路径）
// ---------------------------------------------------------------------------

// observeTier 跑一档观测：起观测台 → 经服务真实构造路径取出站客户端 → 发一次
// GET → 返回记录。tier = "std"（tls_fingerprint none）/ "chrome"（uTLS）。
func observeTier(t *testing.T, tier string) framesRecord {
	t.Helper()
	booth := newFramesBooth(t)
	up := Upstream{Name: "frames-" + tier, BaseURL: booth.baseURL, APIKey: "k-frames", Protocol: ProtocolAnthropic}
	if tier == "chrome" {
		up.TLSFingerprint = "chrome"
	}
	cfg := failoverCfg(up)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	s := New(cfg)
	s.testRootCAs = booth.pool
	s.buildOutbound() // 注入信任根后重建出站客户端（startServiceTLS 同款）

	// 构造面边界：档位语义 + 请求期解析路径。
	c, err := s.newUpstreamClient(up)
	if err != nil {
		t.Fatalf("newUpstreamClient: %v", err)
	}
	if tier == "std" {
		if c != nil {
			t.Fatalf("std 档（none + 无代理）应返回 (nil, nil)（→ 共享 client），got %p", c)
		}
		tr, ok := s.client.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("共享 client transport = %T, want *http.Transport", s.client.Transport)
		}
		// 测试侧等价性（见文件头注）：恢复生产缺省 auto-h2 的协议形态。
		tr.ForceAttemptHTTP2 = true
		c, err = s.clientFor(0)
		if err != nil {
			t.Fatalf("clientFor: %v", err)
		}
		if c != s.client {
			t.Fatal("std 档 clientFor 应落共享 client")
		}
	} else {
		if c == nil {
			t.Fatal("chrome 档应返回专用出站 client（非共享）")
		}
		if c2, err := s.clientFor(0); err != nil || c2 == nil {
			t.Fatalf("chrome 档 clientFor = (%v, %v), want 专用 client", c2, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, booth.baseURL+"/frames/probe", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("[%s] 出站请求失败: %v", tier, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	c.CloseIdleConnections()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("[%s] 状态码 = %d, want 200", tier, resp.StatusCode)
	}
	if resp.Proto != "HTTP/2.0" {
		t.Fatalf("[%s] 响应协议 = %q, want HTTP/2.0（h2 未生效）", tier, resp.Proto)
	}
	return booth.wait(t)
}

// assertGoFrames 断言 = 当前实测基线（两档共用：Go 默认 h2 栈）。
func assertGoFrames(t *testing.T, rec framesRecord, tier string) {
	t.Helper()
	t.Logf("[%s] TLS: offers=%v negotiated=%q version=%#04x suite=%#04x",
		tier, rec.alpnOffers, rec.negotiated, rec.tlsVersion, rec.cipherSuite)
	t.Logf("[%s] h2: settings=[%s] connWU=%v prio=%d pseudo=%v seq=%v ack=%v",
		tier, settingsString(rec.settings), rec.connWindow, rec.priorityN, rec.pseudoOrder, rec.frameSeq, rec.ackSeen)

	// TLS 面
	if !slices.Equal(rec.alpnOffers, wantALPN) {
		t.Errorf("ALPN offers = %v, want %v", rec.alpnOffers, wantALPN)
	}
	if rec.negotiated != "h2" {
		t.Errorf("协商 ALPN = %q, want \"h2\"", rec.negotiated)
	}
	if rec.tlsVersion != tls.VersionTLS13 {
		t.Errorf("协商 TLS 版本 = %#04x, want TLS 1.3 (%#04x)", rec.tlsVersion, tls.VersionTLS13)
	}
	switch rec.cipherSuite {
	case tls.TLS_AES_128_GCM_SHA256, tls.TLS_AES_256_GCM_SHA384, tls.TLS_CHACHA20_POLY1305_SHA256:
	default:
		t.Errorf("协商密码套件 = %#04x, want TLS 1.3 套件之一", rec.cipherSuite)
	}

	// h2 面五维
	if got, want := settingsString(rec.settings), settingsString(wantGoSettings); got != want {
		t.Errorf("SETTINGS（键值 + 键序）= [%s], want [%s]", got, want)
	}
	if len(rec.connWindow) != 1 || rec.connWindow[0] != wantGoConnWU {
		t.Errorf("连接级 WINDOW_UPDATE = %v, want [%d]", rec.connWindow, wantGoConnWU)
	}
	if rec.priorityN != 0 {
		t.Errorf("PRIORITY 帧 = %d 个, want 0（Go 默认客户端不发）", rec.priorityN)
	}
	if !slices.Equal(rec.pseudoOrder, wantGoPseudo) {
		t.Errorf("伪头序 = %v, want %v", rec.pseudoOrder, wantGoPseudo)
	}

	// 帧序边界：首两帧固定（同一 flush 内 SETTINGS → WINDOW_UPDATE）；请求
	// HEADERS 与 SETTINGS ACK 各一（尾两帧次序由客户端读/写循环竞速决定，不钉）。
	if len(rec.frameSeq) != 4 || rec.frameSeq[0] != "SETTINGS" || rec.frameSeq[1] != "WINDOW_UPDATE(0)" {
		t.Errorf("帧序 = %v, want 4 帧：SETTINGS → WINDOW_UPDATE(0) 固定在前，HEADERS 与 SETTINGS-ACK 各一", rec.frameSeq)
	}
	if !slices.Contains(rec.frameSeq, "HEADERS") {
		t.Errorf("未观测到请求 HEADERS（帧序：%v）", rec.frameSeq)
	}
	if !rec.ackSeen {
		t.Errorf("未观测到 SETTINGS ACK（帧序：%v）", rec.frameSeq)
	}
}

// assertNotChromeFrames 反证面：Chrome h2 目标值（W53-3 表）**一个都不出现**——
// 任一命中 = 我们发出了比预期更强的 h2 拟真（或观测台串值），两者都该红。
func assertNotChromeFrames(t *testing.T, rec framesRecord) {
	t.Helper()
	for _, s := range rec.settings {
		switch s.Val {
		case 65536, 6291456, 262144: // Chrome：HEADER_TABLE_SIZE / INITIAL_WINDOW_SIZE / MAX_HEADER_LIST_SIZE
			t.Errorf("SETTINGS 出现 Chrome 目标值 %d（id=%d）", s.Val, s.ID)
		}
	}
	for _, inc := range rec.connWindow {
		if inc == 15663105 {
			t.Errorf("连接级 WINDOW_UPDATE = Chrome 目标值 15663105")
		}
	}
	if slices.Equal(rec.pseudoOrder, []string{":method", ":authority", ":scheme", ":path"}) {
		t.Errorf("伪头序 = masp（Chrome 目标）——h2 拟真超出预期")
	}
}

// ---------------------------------------------------------------------------
// 两档帧观测（std / chrome）+ 半真形态交叉核对
// ---------------------------------------------------------------------------

// TestOutboundFramesBooth 帧观测台两档：std（tls_fingerprint none，共享
// client）与 chrome（uTLS + x/net h2）各发一次 GET，逐项断言当前实测基线；
// 交叉核对「两档 h2 五维逐项相等」= 半真形态（TLS 真、h2 全 Go）的字节级证据。
func TestOutboundFramesBooth(t *testing.T) {
	var std, chrome framesRecord
	okStd := t.Run("std", func(t *testing.T) {
		std = observeTier(t, "std")
		assertGoFrames(t, std, "std")
		assertNotChromeFrames(t, std)
	})
	okChrome := t.Run("chrome", func(t *testing.T) {
		chrome = observeTier(t, "chrome")
		assertGoFrames(t, chrome, "chrome")
		assertNotChromeFrames(t, chrome)
	})
	if !okStd || !okChrome {
		return
	}
	t.Run("half_true_form", func(t *testing.T) {
		if got, want := settingsString(chrome.settings), settingsString(std.settings); got != want {
			t.Errorf("两档 SETTINGS 不等：chrome=[%s] std=[%s]（uTLS 只换 TLS 握手层，h2 应逐字节同 Go 缺省）", got, want)
		}
		if !slices.Equal(chrome.connWindow, std.connWindow) {
			t.Errorf("两档连接级 WINDOW_UPDATE 不等：chrome=%v std=%v", chrome.connWindow, std.connWindow)
		}
		if chrome.priorityN != std.priorityN {
			t.Errorf("两档 PRIORITY 计数不等：chrome=%d std=%d", chrome.priorityN, std.priorityN)
		}
		if !slices.Equal(chrome.pseudoOrder, std.pseudoOrder) {
			t.Errorf("两档伪头序不等：chrome=%v std=%v", chrome.pseudoOrder, std.pseudoOrder)
		}
		if chrome.negotiated != std.negotiated || chrome.cipherSuite != std.cipherSuite {
			t.Errorf("两档 TLS 协商面不等：chrome=(%q,%#04x) std=(%q,%#04x)",
				chrome.negotiated, chrome.cipherSuite, std.negotiated, std.cipherSuite)
		}
	})
}
