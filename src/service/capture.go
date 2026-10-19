// debug 体录制（`debug_capture_dir`；**缺省空 = 关**）——W6-6 建议 ④ 的落地面：
// 「上游返回体逐字节留档缺口」（服务日志只记 attempt 摘要，无请求/响应体回放）
// 的最小补法。每个**入口请求**在目录内落一份 `<req_id>.json`：
//
//   - 下游请求（方法/路径/查询串/掩码头/体）+ 下游响应（状态/掩码头/体）；
//   - 每个上游尝试：上游名/URL/掩码请求头/上游请求体（转换后）/状态码/上游响应体
//     （流式为 relay 读到的原始 SSE 字节——「翻译层逐字段归因」级物证）。
//
// 纪律：
//   - **凭据头一律掩码**（captureMaskedHeaders：authorization / x-api-key /
//     x-goog-api-key / cookie 等；查询串里的 key/token 参数同款掩码）——录制产物
//     可安全入库（W5-37 凭据纪律的延伸）；
//   - **容量纪律**：每体上限 debugCaptureBodyCap（1 MiB），超限截断并置
//     `*_truncated: true`；每请求最多 4 体（下游请求/下游响应/每尝试请求与响应）
//     → 单文件约 ≤4 MiB；
//   - **绝不进控制流**：目录按需创建；任何写盘失败只打日志，请求路径不受影响；
//   - 文件名含 req id（与响应 `request-id` 头、记账日志可关联）。
//
// 范围边界：只覆盖入口面（handle / handleGeminiEntry 的请求路径）——count_tokens /
// 模型发现 / 管理面不录制（非上游物证面）。
package service

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// debugCaptureBodyCap 单体积上限（1 MiB；见头注容量纪律）。
const debugCaptureBodyCap = 1 << 20

// captureCtxKey 请求上下文里的录制句柄键。
type captureCtxKey struct{}

// captureFrom 从 ctx 取录制句柄（nil = 未开启 / 非入口面）。
func captureFrom(ctx context.Context) *requestCapture {
	c, _ := ctx.Value(captureCtxKey{}).(*requestCapture)
	return c
}

// captureMaskedHeaders 凭据头掩码集合（小写名；值一律替换为 `***`）。
var captureMaskedHeaders = map[string]bool{
	"authorization":       true,
	"proxy-authorization": true,
	"x-api-key":           true,
	"x-goog-api-key":      true,
	"api-key":             true,
	"cookie":              true,
	"set-cookie":          true,
}

// captureMaskedQuery 查询串里的凭据参数名（小写；值一律替换为 `***`）。
var captureMaskedQuery = map[string]bool{
	"key":          true,
	"api_key":      true,
	"apikey":       true,
	"access_token": true,
	"token":        true,
}

// maskHeaders 掩码复制请求/响应头（凭据头 → `***`；多值以 ", " 连接）。
func maskHeaders(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, vs := range h {
		v := strings.Join(vs, ", ")
		if captureMaskedHeaders[strings.ToLower(k)] {
			v = "***"
		}
		out[k] = v
	}
	return out
}

// maskQuery 查询串掩码（凭据参数值 → `***`；键序稳定便于 diff；解析失败整体掩码）。
func maskQuery(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	vals, err := url.ParseQuery(rawQuery)
	if err != nil {
		return "***"
	}
	keys := make([]string, 0, len(vals))
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('&')
		}
		v := strings.Join(vals[k], ",")
		if captureMaskedQuery[strings.ToLower(k)] {
			v = "***"
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v)
	}
	return b.String()
}

// captureAttempt 一个上游尝试的录制（请求 = 转换后真实发上游的体；响应 = 上游
// 返回的原始体，流式为 relay 逐帧读到的 SSE 字节）。
type captureAttempt struct {
	mu sync.Mutex

	Upstream      string            `json:"upstream"`
	URL           string            `json:"url"`
	RequestHeader map[string]string `json:"request_headers"`
	RequestBody   string            `json:"request_body"`
	RequestTrunc  bool              `json:"request_body_truncated,omitempty"`
	Status        int               `json:"status,omitempty"`
	ResponseBody  string            `json:"response_body"`
	ResponseTrunc bool              `json:"response_body_truncated,omitempty"`

	raw   []byte // 响应体累积（≤ debugCaptureBodyCap）
	trunc bool
}

// appendResponseBody 追加一段上游响应字节（≤ 上限；超出后只置截断标记）。
func (a *captureAttempt) appendResponseBody(b []byte) {
	if a == nil || len(b) == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	room := debugCaptureBodyCap - len(a.raw)
	if room <= 0 {
		a.trunc = true
		return
	}
	if len(b) > room {
		a.raw = append(a.raw, b[:room]...)
		a.trunc = true
		return
	}
	a.raw = append(a.raw, b...)
}

// requestCapture 一次入口请求的录制（并发安全；写盘在 finish）。
type requestCapture struct {
	dir string

	mu        sync.Mutex
	reqID     string
	started   time.Time
	method    string
	path      string
	query     string // 已掩码
	reqHdr    map[string]string
	reqBody   []byte
	reqTrunc  bool
	rw        http.ResponseWriter // 响应头在 finish 时从活头表读
	respCode  int
	respRaw   []byte
	respTrunc bool
	attempts  []*captureAttempt
}

// newRequestCapture 创建录制（入口处理器调用；调用方负责挂 ctx/wrap/finish）。
func newRequestCapture(dir string, r *http.Request) *requestCapture {
	return &requestCapture{
		dir:     dir,
		started: time.Now(),
		method:  r.Method,
		path:    r.URL.Path,
		query:   maskQuery(r.URL.RawQuery),
		reqHdr:  maskHeaders(r.Header),
	}
}

// setReqID 记请求 id（serve 生成后调用；文件名与响应头/日志同源）。
func (c *requestCapture) setReqID(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reqID = id
}

// setRequestBody 记下游请求体（readEntryBody 成功后调用）。
func (c *requestCapture) setRequestBody(b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(b) > debugCaptureBodyCap {
		c.reqBody = append([]byte(nil), b[:debugCaptureBodyCap]...)
		c.reqTrunc = true
		return
	}
	c.reqBody = append([]byte(nil), b...)
}

// addAttempt 登记一个上游尝试（forward 在发起上游请求前调用）；返回句柄供响应体
// 落账（captureBody tee / 状态码）。
func (c *requestCapture) addAttempt(name, url string, hdr http.Header, body []byte) *captureAttempt {
	att := &captureAttempt{
		Upstream:      name,
		URL:           url,
		RequestHeader: maskHeaders(hdr),
	}
	if len(body) > debugCaptureBodyCap {
		att.RequestBody = string(body[:debugCaptureBodyCap])
		att.RequestTrunc = true
	} else {
		att.RequestBody = string(body)
	}
	c.mu.Lock()
	c.attempts = append(c.attempts, att)
	c.mu.Unlock()
	return att
}

// wrap 包装 ResponseWriter（记录响应体/状态码；Flusher 透传——流式面依赖）。
func (c *requestCapture) wrap(w http.ResponseWriter) http.ResponseWriter {
	c.mu.Lock()
	c.rw = w
	c.mu.Unlock()
	return &captureResponseWriter{ResponseWriter: w, cap: c}
}

// appendResponse 追加一段下游响应字节（≤ 上限）。
func (c *requestCapture) appendResponse(b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	room := debugCaptureBodyCap - len(c.respRaw)
	if room <= 0 {
		c.respTrunc = true
		return
	}
	if len(b) > room {
		c.respRaw = append(c.respRaw, b[:room]...)
		c.respTrunc = true
		return
	}
	c.respRaw = append(c.respRaw, b...)
}

// setRespCode 记响应状态码（首次写出时）。
func (c *requestCapture) setRespCode(code int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.respCode == 0 {
		c.respCode = code
	}
}

// captureResponseWriter 录制用 ResponseWriter 包装（Write/WriteHeader 透传 + 记账；
// Flush/Unwrap 透传——提交点纪律与流式 flush 不受影响）。
type captureResponseWriter struct {
	http.ResponseWriter
	cap *requestCapture
}

func (w *captureResponseWriter) WriteHeader(code int) {
	w.cap.setRespCode(code)
	w.ResponseWriter.WriteHeader(code)
}

func (w *captureResponseWriter) Write(b []byte) (int, error) {
	w.cap.setRespCode(http.StatusOK) // 未显式 WriteHeader 的隐式 200
	w.cap.appendResponse(b)
	return w.ResponseWriter.Write(b)
}

// Flush 透传（底层非 Flusher 时 no-op——与 newDownstream 的缺省口径一致）。
func (w *captureResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap 暴露底层 writer（http.ResponseController 语义）。
func (w *captureResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// captureBody 上游响应体读取的 tee 包装（relay / 批量读全量都经此累计字节）。
type captureBody struct {
	rc  io.ReadCloser
	att *captureAttempt
}

func (c *captureBody) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	if n > 0 {
		c.att.appendResponseBody(p[:n])
	}
	return n, err
}

func (c *captureBody) Close() error { return c.rc.Close() }

// captureDoc 录制文件的 JSON 形状（文件名 `<req_id>.json`）。
type captureDoc struct {
	ReqID                 string            `json:"req_id"`
	Started               string            `json:"started"`
	Method                string            `json:"method"`
	Path                  string            `json:"path"`
	Query                 string            `json:"query,omitempty"`
	RequestHeaders        map[string]string `json:"request_headers"`
	RequestBody           string            `json:"request_body"`
	RequestBodyTruncated  bool              `json:"request_body_truncated,omitempty"`
	ResponseStatus        int               `json:"response_status"`
	ResponseHeaders       map[string]string `json:"response_headers"`
	ResponseBody          string            `json:"response_body"`
	ResponseBodyTruncated bool              `json:"response_body_truncated,omitempty"`
	UpstreamAttempts      []*captureAttempt `json:"upstream_attempts,omitempty"`
}

// finish 落盘录制（请求收尾 defer 调用）。任何失败只打日志——录制绝不影响请求。
func (c *requestCapture) finish() {
	c.mu.Lock()
	id := c.reqID
	if id == "" {
		id = newRequestID() // 未走到 serve 的请求（405 等）自造
	}
	doc := captureDoc{
		ReqID:                 id,
		Started:               c.started.UTC().Format(time.RFC3339Nano),
		Method:                c.method,
		Path:                  c.path,
		Query:                 c.query,
		RequestHeaders:        c.reqHdr,
		RequestBody:           string(c.reqBody),
		RequestBodyTruncated:  c.reqTrunc,
		ResponseStatus:        c.respCode,
		ResponseBody:          string(c.respRaw),
		ResponseBodyTruncated: c.respTrunc,
		UpstreamAttempts:      c.attempts,
	}
	if c.rw != nil {
		doc.ResponseHeaders = maskHeaders(c.rw.Header())
	}
	dir := c.dir
	c.mu.Unlock()

	for _, att := range doc.UpstreamAttempts {
		att.mu.Lock()
		att.ResponseBody = string(att.raw)
		att.ResponseTrunc = att.trunc
		att.raw = nil
		att.mu.Unlock()
	}
	blob, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		log.Printf("any2api: debug capture %s: marshal failed: %v", id, err)
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("any2api: debug capture %s: mkdir %s failed: %v", id, dir, err)
		return
	}
	path := filepath.Join(dir, id+".json")
	if err := os.WriteFile(path, blob, 0o600); err != nil {
		log.Printf("any2api: debug capture %s: write %s failed: %v", id, path, err)
		return
	}
	log.Printf("any2api: debug capture %s written (%d bytes)", id, len(blob))
}

// newCapture 入口面创建录制句柄（debug_capture_dir 未配 = nil，零开销）。
func (s *Server) newCapture(r *http.Request) *requestCapture {
	if s.cfg.DebugCaptureDir == "" {
		return nil
	}
	return newRequestCapture(s.cfg.DebugCaptureDir, r)
}

// attachCapture 入口处理器统一挂载：ctx 挂句柄 + ResponseWriter 包装；返回新的
// (r, w) 与收尾 finish 函数（未开启 = 原样返回 + no-op）。
func (s *Server) attachCapture(r *http.Request, w http.ResponseWriter) (*http.Request, http.ResponseWriter, func()) {
	c := s.newCapture(r)
	if c == nil {
		return r, w, func() {}
	}
	return r.WithContext(context.WithValue(r.Context(), captureCtxKey{}, c)), c.wrap(w), c.finish
}
