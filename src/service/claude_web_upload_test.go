package service

// W22-1 服务级 e2e：claude_web 附件上传链（`POST /api/{org}/upload` multipart）——
// 假上游**四端点**（org 发现 / 会话创建 / 上传 / completion）+ 真服务（
// New(cfg).Handler() 经 httptest.NewServer）+ 真 HTTP 客户端。
//
// 四类测试（brief §Change ④）：
//   ① multipart 形状（字段名 file / 文件名按内容类型 / 部件 mime / 字节原样）；
//   ② 上传成功 → completion 请求体 `files[]` 含真实 file_uuid（回填）；
//   ③ 上传失败（500 / 缺 file_uuid / 畸形 uuid）→ 下游显式错误、不发 completion；
//   ④ 边界（缺 org / 超大 / SSRF 守卫拒绝的内网 URL）按 KB 裁定。
//
// 反证锚点（本文件）：
//   - 撤掉 claudeWebPrepare 的上传块 → TestUploadMultipartShapeAndRefs 红（无
//     /api/{org}/upload 调用、completion 体 files 是占位引用）；
//   - 上传失败改为吞错（不 return err）→ TestUploadFailureIsExplicit 红（下游
//     收到成功响应但 body 里是占位引用 = 静默丢）；
//   - 撤掉 job.Resolve 回填 → ② 红（files 仍是占位引用）；
//   - 撤掉 claudeWebPublicURL 的内网守卫 → TestUploadURLCarrierRejectsPrivate 红；
//   - 撤掉附件接缝（forward 不设 reqOpts.AttachmentSink / 不把 sink.Job 传给
//     claudeWebPrepare）→ ① 红（无上传、占位引用留在 completion 体）；
//   - 把附件 sink 改为包级共享槽位（回归全局态）→ TestUploadConcurrentNoCrosstalk
//     红（并发请求载荷 ↔ uuid 错配）。

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dwgx/any2api/translate/convert/claude_web"
)

// claudeWebAttachment 构造直调面附件（inline 载体）。
func claudeWebAttachment(raw []byte) claude_web.Attachment {
	return claude_web.Attachment{MediaType: "image/png", Data: raw}
}

// ---------------------------------------------------------------------------
// 脚手架：假 claude_web 上游（四端点）
// ---------------------------------------------------------------------------

type cwAttachCall struct {
	method  string
	path    string
	headers http.Header
	body    []byte
	// seq 上传调用在记录序里的下标（从 0 起；仅 /upload 调用 ≥ 0）——与记录
	// 同锁分配，供 uploadSeqUUID 响应与断言按记录序重建 payload ↔ uuid 映射。
	seq int
}

type cwAttachUpstream struct {
	srv *httptest.Server

	mu    sync.Mutex
	calls []cwAttachCall
	// uploadCount 已记录的上传调用数（seq 分配器）。
	uploadCount int

	orgStatus  int    // GET /api/organizations 状态（0 → 200）
	orgBody    string // 空 → 单组织数组
	uploadStat int    // POST /api/{org}/upload 状态（0 → 200）
	uploadBody string // 空 → {"file_uuid":"<cwAttachFileUUID>"}
	// uploadDelay > 0：上传挂住不响应（超时分支注入；客户端超时/ctx 取消后返回，
	// 不写响应体——调用仍被记录，供「恰 1 次尝试」断言）。
	uploadDelay time.Duration
	imgStatus   int    // GET /img.png 状态（0 → 200）
	imgBody     []byte // 空 → cwAttachImgBytes
	// imgContentType 空 → 按路径缺省（/img.png = image/png、/doc.pdf = application/pdf）。
	imgContentType string
	compFixture    bool // completion 回放实抓 fixture（缺省 true）

	// uploadSeqUUID 非 nil：上传响应按 seq 返回 uploadSeqUUID(seq)（并发不串台
	// 测试用——每次上传唯一 uuid，断言可按记录序重建 payload ↔ uuid 映射）。
	uploadSeqUUID func(seq int) string

	// orgGate 非 nil：org 发现响应等全部门到齐后一起放行（并发不串台测试用——
	// 强制所有请求先完成渲染再进入会话准备；任何跨请求共享槽位都会在此形态下
	// 暴露。nil = 直过）。
	orgGate *cwOrgGate
}

// cwOrgGate org 发现门：arrive 计数到 n 后全部放行（2s 上限防挂——异常路径
// 下不足 n 个到达也有界放行，测试绝不悬挂）。
type cwOrgGate struct {
	n       int
	mu      sync.Mutex
	arrived int
	open    chan struct{}
}

func (g *cwOrgGate) arrive() {
	g.mu.Lock()
	g.arrived++
	if g.arrived >= g.n {
		select {
		case <-g.open:
		default:
			close(g.open)
		}
	}
	g.mu.Unlock()
	select {
	case <-g.open:
	case <-time.After(2 * time.Second):
	}
}

const (
	cwAttachOrgUUID  = "abababab-1111-4222-8333-444444444444"
	cwAttachConvUUID = "cdcdcdcd-5555-4666-8777-888888888888"
	// cwAttachFileUUID 36 字符（回填等长约束；形态 = 上游 file_uuid [UNVERIFIED]，
	// 测试按 uuid 形态钉死）。
	cwAttachFileUUID = "c0ffee00-1234-4abc-8def-000000000001"
)

var cwAttachImgBytes = []byte("fake-png-image-bytes")

func newCWAttachUpstream(t *testing.T) *cwAttachUpstream {
	t.Helper()
	// fixture 在测试 goroutine 预读（handler 跑在 httptest goroutine——不得在
	// 那里 t.Fatalf）。
	stream := claudeWebFixtureSSE(t, "run1_4087359.531.frames.jsonl.txt")
	u := &cwAttachUpstream{}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seq := -1
		u.mu.Lock()
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/upload") {
			seq = u.uploadCount
			u.uploadCount++
		}
		u.calls = append(u.calls, cwAttachCall{method: r.Method, path: r.URL.Path, headers: r.Header.Clone(), body: b, seq: seq})
		u.mu.Unlock()

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/organizations":
			if u.orgGate != nil {
				// 并发不串台测试：全部门到齐后一起放行（所有请求先完成渲染，再
				// 进入会话准备——最大交错形态）。
				u.orgGate.arrive()
			}
			if u.orgStatus >= 400 {
				w.WriteHeader(u.orgStatus)
				_, _ = w.Write([]byte(`{"error":{"message":"sessionKey invalid"}}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			body := u.orgBody
			if body == "" {
				body = fmt.Sprintf(`[{"id":1,"uuid":%q,"name":"Test","rate_limit_tier":"default_claude_ai"}]`, cwAttachOrgUUID)
			}
			_, _ = w.Write([]byte(body))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/chat_conversations"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, `{"uuid":%q}`, cwAttachConvUUID)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/upload"):
			if u.uploadDelay > 0 {
				// 超时形态注入：记录调用后挂住不响应（客户端超时/ctx 取消后返回）。
				select {
				case <-r.Context().Done():
				case <-time.After(u.uploadDelay):
				}
				return
			}
			if u.uploadStat >= 400 {
				w.WriteHeader(u.uploadStat)
				_, _ = w.Write([]byte(`{"error":{"message":"upload rejected"}}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			body := u.uploadBody
			if body == "" {
				uuid := cwAttachFileUUID
				if u.uploadSeqUUID != nil {
					uuid = u.uploadSeqUUID(seq)
				}
				body = fmt.Sprintf(`{"file_uuid":%q}`, uuid)
			}
			_, _ = w.Write([]byte(body))
		case r.Method == http.MethodGet && (r.URL.Path == "/img.png" || r.URL.Path == "/doc.pdf"):
			if u.imgStatus >= 400 {
				w.WriteHeader(u.imgStatus)
				return
			}
			ct := u.imgContentType
			if ct == "" {
				ct = "image/png"
				if r.URL.Path == "/doc.pdf" {
					ct = "application/pdf"
				}
			}
			w.Header().Set("Content-Type", ct)
			body := u.imgBody
			if body == nil {
				body = cwAttachImgBytes
			}
			_, _ = w.Write(body)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/completion"):
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, stream)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/chat_conversations/"):
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(u.srv.Close)
	return u
}

func (u *cwAttachUpstream) snapshot() []cwAttachCall {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]cwAttachCall(nil), u.calls...)
}

// callsWithSuffix 快照中路径后缀匹配的调用（按序）。
func (u *cwAttachUpstream) callsWithSuffix(suffix string) []cwAttachCall {
	var out []cwAttachCall
	for _, c := range u.snapshot() {
		if strings.HasSuffix(c.path, suffix) {
			out = append(out, c)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// 请求构造
// ---------------------------------------------------------------------------

// cwAttachClaudeBody claude_messages 入口体：text + image（source.type=base64）。
func cwAttachClaudeBody(raw []byte) string {
	return cwAttachClaudeBodyText("look", raw)
}

// cwAttachClaudeBodyText 同 cwAttachClaudeBody，文本可定制（并发不串台测试用
// 文本标记归属请求）。
func cwAttachClaudeBodyText(text string, raw []byte) string {
	return fmt.Sprintf(`{"model":"claude-sonnet-5-5","max_tokens":64,"messages":[{"role":"user","content":[{"type":"text","text":%q},{"type":"image","source":{"type":"base64","media_type":"image/png","data":%q}}]}]}`,
		text, base64.StdEncoding.EncodeToString(raw))
}

// cwAttachOpenAIBody openai 入口体：text + image_url（data-URI 或 http URL）。
func cwAttachOpenAIBody(url string) string {
	body, _ := json.Marshal(map[string]any{
		"model": "gpt-x",
		"messages": []map[string]any{{
			"role": "user",
			"content": []map[string]any{
				{"type": "text", "text": "look"},
				{"type": "image_url", "image_url": map[string]any{"url": url}},
			},
		}},
	})
	return string(body)
}

// cwAttachImage 一份图像载荷（媒体类型 + 字节）——多图/PDF 构造器共用。
type cwAttachImage struct {
	mediaType string
	raw       []byte
}

// cwAttachClaudeBodyImages claude_messages 入口体：text（空串 = 省略文本块——④
// 图像-only 用）+ N 个 image 块（base64，顺序 = 块序）。
func cwAttachClaudeBodyImages(text string, images ...cwAttachImage) string {
	parts := make([]map[string]any, 0, len(images)+1)
	if text != "" {
		parts = append(parts, map[string]any{"type": "text", "text": text})
	}
	for _, img := range images {
		parts = append(parts, map[string]any{
			"type": "image",
			"source": map[string]any{
				"type": "base64", "media_type": img.mediaType,
				"data": base64.StdEncoding.EncodeToString(img.raw),
			},
		})
	}
	body, _ := json.Marshal(map[string]any{
		"model": "claude-sonnet-5-5", "max_tokens": 64,
		"messages": []map[string]any{{"role": "user", "content": parts}},
	})
	return string(body)
}

// cwAttachOpenAIBodyMulti openai 入口体：text + N 个 image_url（顺序 = 部件序）。
func cwAttachOpenAIBodyMulti(urls ...string) string {
	parts := []map[string]any{{"type": "text", "text": "look"}}
	for _, u := range urls {
		parts = append(parts, map[string]any{"type": "image_url", "image_url": map[string]any{"url": u}})
	}
	body, _ := json.Marshal(map[string]any{
		"model":    "gpt-x",
		"messages": []map[string]any{{"role": "user", "content": parts}},
	})
	return string(body)
}

// cwAttachDataURI data-URI 载体（PDF/多图用例用）。
func cwAttachDataURI(mediaType string, raw []byte) string {
	return "data:" + mediaType + ";base64," + base64.StdEncoding.EncodeToString(raw)
}

// parseMultipartUpload 解析上传调用：返回部件名/文件名/部件 mime/字节。
func parseMultipartUpload(t *testing.T, call cwAttachCall) (name, filename, partType string, data []byte) {
	t.Helper()
	mt, params, err := mime.ParseMediaType(call.headers.Get("Content-Type"))
	if err != nil || mt != "multipart/form-data" {
		t.Fatalf("上传 Content-Type = %q, want multipart/form-data（err=%v）", call.headers.Get("Content-Type"), err)
	}
	boundary := params["boundary"]
	if boundary == "" {
		t.Fatalf("上传缺 multipart boundary")
	}
	mr := multipart.NewReader(bytes.NewReader(call.body), boundary)
	part, err := mr.NextPart()
	if err != nil {
		t.Fatalf("读部件: %v", err)
	}
	name = part.FormName()
	filename = part.FileName()
	partType = part.Header.Get("Content-Type")
	data, err = io.ReadAll(part)
	if err != nil {
		t.Fatalf("读部件体: %v", err)
	}
	if _, err := mr.NextPart(); err != io.EOF {
		t.Fatalf("上传必须恰一个部件（第二部件 err=%v）", err)
	}
	return name, filename, partType, data
}

// completionFiles 从 completion 调用体解析 files[] 与 prompt。
func completionFiles(t *testing.T, call cwAttachCall) ([]string, string) {
	t.Helper()
	var body struct {
		Files  []string `json:"files"`
		Prompt string   `json:"prompt"`
	}
	if err := json.Unmarshal(call.body, &body); err != nil {
		t.Fatalf("completion 体解析: %v", err)
	}
	return body.Files, body.Prompt
}

// waitCWAttachCompletion 有界等待 completion 调用出现（前序上传/建会话为同步，
// 但慢机器上留裕量）。
func waitCWAttachCompletion(t *testing.T, u *cwAttachUpstream) cwAttachCall {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if cs := u.callsWithSuffix("/completion"); len(cs) > 0 {
			return cs[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 completion 调用超时；已见: %v", u.snapshot())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// assertNoCWAttachPath 宽限期内不得出现指定路径后缀的调用（失败面负断言）。
func assertNoCWAttachPath(t *testing.T, u *cwAttachUpstream, suffix string) {
	t.Helper()
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if cs := u.callsWithSuffix(suffix); len(cs) > 0 {
			t.Fatalf("不得出现 %s 调用（见 %v）", suffix, cs[0])
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// ①+②：multipart 形状 + 回填接线（claude_messages 入口）
// ---------------------------------------------------------------------------

func TestUploadMultipartShapeAndRefs(t *testing.T) {
	up := newCWAttachUpstream(t)
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/messages", cwAttachClaudeBody(cwAttachImgBytes), nil)
	defer resp.Body.Close()
	rawResp, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, rawResp)
	}
	if !strings.Contains(string(rawResp), "PONG") {
		t.Fatalf("下游未见 fixture 回放内容: %s", rawResp)
	}

	// ---- ① multipart 形状 ----
	uploads := up.callsWithSuffix("/upload")
	if len(uploads) != 1 {
		t.Fatalf("上传调用数 = %d, want 1（全部调用: %v）", len(uploads), up.snapshot())
	}
	call := uploads[0]
	if call.method != http.MethodPost {
		t.Fatalf("上传方法 = %s, want POST", call.method)
	}
	// 端点 = /api/{org}/upload（org 来自发现面；CW/YS/BK 三实现同形）。
	if want := "/api/" + cwAttachOrgUUID + "/upload"; call.path != want {
		t.Fatalf("上传路径 = %s, want %s", call.path, want)
	}
	name, filename, partType, data := parseMultipartUpload(t, call)
	if name != "file" {
		t.Fatalf("部件字段名 = %q, want file（CW/YS/BK 三实现一致）", name)
	}
	if filename != "image.png" {
		t.Fatalf("部件文件名 = %q, want image.png（CW/YS 内容类型映射）", filename)
	}
	if partType != "image/png" {
		t.Fatalf("部件 mime = %q, want image/png", partType)
	}
	if !bytes.Equal(data, cwAttachImgBytes) {
		t.Fatalf("上传字节 = %q, want %q（base64 解码原样）", data, cwAttachImgBytes)
	}
	// 头面（与三端点同一份装配）。
	if !strings.Contains(call.headers.Get("Cookie"), "sessionKey=sk-ant-sid01-test-session-key") {
		t.Fatalf("上传缺 sessionKey cookie: %q", call.headers.Get("Cookie"))
	}
	if want := up.srv.URL + "/new"; call.headers.Get("Referer") != want {
		t.Fatalf("上传 referer = %q, want %q", call.headers.Get("Referer"), want)
	}
	if got := call.headers.Get("anthropic-client-platform"); got != "web_claude_ai" {
		t.Fatalf("上传缺协议常量头: %q", got)
	}

	// ---- ② 上传成功 → completion 体含真实引用 ----
	comp := waitCWAttachCompletion(t, up)
	files, prompt := completionFiles(t, comp)
	if len(files) != 1 || files[0] != cwAttachFileUUID {
		t.Fatalf("completion files = %v, want [%s]（回填后的真实 file_uuid）", files, cwAttachFileUUID)
	}
	if !strings.Contains(prompt, "look") {
		t.Fatalf("prompt 丢文本: %q", prompt)
	}
	// 上传在 completion 之前（调用序：org → 上传 → 建会话 → completion）。
	all := up.snapshot()
	uploadIdx, convIdx, compIdx := -1, -1, -1
	for i, c := range all {
		switch {
		case strings.HasSuffix(c.path, "/upload"):
			uploadIdx = i
		case strings.HasSuffix(c.path, "/chat_conversations"):
			convIdx = i
		case strings.HasSuffix(c.path, "/completion"):
			compIdx = i
		}
	}
	if !(0 <= uploadIdx && uploadIdx < convIdx && convIdx < compIdx) {
		t.Fatalf("调用序必须 上传(%d) < 建会话(%d) < completion(%d): %v", uploadIdx, convIdx, compIdx, all)
	}
}

// ---------------------------------------------------------------------------
// ②b：openai 入口 image_url（data-URI 载体）同链
// ---------------------------------------------------------------------------

func TestUploadOpenAIDataURIEntry(t *testing.T) {
	up := newCWAttachUpstream(t)
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

	uri := "data:image/png;base64," + base64.StdEncoding.EncodeToString(cwAttachImgBytes)
	resp := postJSON(t, srv.URL+"/v1/chat/completions", cwAttachOpenAIBody(uri), nil)
	defer resp.Body.Close()
	rawResp, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, rawResp)
	}
	uploads := up.callsWithSuffix("/upload")
	if len(uploads) != 1 {
		t.Fatalf("上传调用数 = %d, want 1", len(uploads))
	}
	_, filename, partType, data := parseMultipartUpload(t, uploads[0])
	if filename != "image.png" || partType != "image/png" || !bytes.Equal(data, cwAttachImgBytes) {
		t.Fatalf("data-URI 载体上传 = %s/%s/%q, want image.png/image/png/原字节", filename, partType, data)
	}
	comp := waitCWAttachCompletion(t, up)
	files, _ := completionFiles(t, comp)
	if len(files) != 1 || files[0] != cwAttachFileUUID {
		t.Fatalf("completion files = %v, want [%s]", files, cwAttachFileUUID)
	}
}

// ---------------------------------------------------------------------------
// ②c：openai 入口 image_url（http URL 载体）→ 服务壳取回 → 上传
// ---------------------------------------------------------------------------

func TestUploadURLCarrierFetchesThenUploads(t *testing.T) {
	up := newCWAttachUpstream(t)
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))
	// SSRF 守卫的解析面注入：httptest 是 127.0.0.1（内网），守卫按解析结果判——
	// 注入公网解析值让取回路径可测（连接仍打到 httptest）。
	old := claudeWebLookupIP
	claudeWebLookupIP = func(string) ([]net.IP, error) { return []net.IP{net.ParseIP("93.184.216.34")}, nil }
	t.Cleanup(func() { claudeWebLookupIP = old })

	resp := postJSON(t, srv.URL+"/v1/chat/completions", cwAttachOpenAIBody(up.srv.URL+"/img.png"), nil)
	defer resp.Body.Close()
	rawResp, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, rawResp)
	}
	if len(up.callsWithSuffix("/img.png")) != 1 {
		t.Fatalf("URL 载体必须先取回（GET /img.png）")
	}
	uploads := up.callsWithSuffix("/upload")
	if len(uploads) != 1 {
		t.Fatalf("上传调用数 = %d, want 1", len(uploads))
	}
	_, filename, _, data := parseMultipartUpload(t, uploads[0])
	if filename != "image.png" || !bytes.Equal(data, cwAttachImgBytes) {
		t.Fatalf("URL 载体上传 = %s/%q, want image.png/取回字节", filename, data)
	}
	comp := waitCWAttachCompletion(t, up)
	files, _ := completionFiles(t, comp)
	if len(files) != 1 || files[0] != cwAttachFileUUID {
		t.Fatalf("completion files = %v, want [%s]", files, cwAttachFileUUID)
	}
}

// TestUploadURLCarrierRejectsPrivate 边界：内网/回环 URL 被 SSRF 守卫拒绝（真实
// 解析面）——显式失败、不上传、不发 completion。
func TestUploadURLCarrierRejectsPrivate(t *testing.T) {
	up := newCWAttachUpstream(t)
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/chat/completions", cwAttachOpenAIBody(up.srv.URL+"/img.png"), nil)
	defer resp.Body.Close()
	rawResp, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 200 {
		t.Fatalf("内网 URL 必须显式失败: %s", rawResp)
	}
	if !strings.Contains(string(rawResp), "non-public address") {
		t.Fatalf("失败原因必须是 SSRF 守卫: %s", rawResp)
	}
	if len(up.callsWithSuffix("/upload")) != 0 {
		t.Fatalf("守卫拒绝后不得上传")
	}
	assertNoCWAttachPath(t, up, "/completion")
}

// ---------------------------------------------------------------------------
// ③ 上传失败 → 下游显式错误（不静默、不发 completion）
// ---------------------------------------------------------------------------

func TestUploadFailureIsExplicit(t *testing.T) {
	for name, cfg := range map[string]struct {
		status int
		body   string
	}{
		"500":                 {status: 500},
		"缺 file_uuid":         {body: `{}`},
		"畸形 uuid（错长）":         {body: `{"file_uuid":"short"}`},
		"file_uuid 非 JSON 安全": {body: `{"file_uuid":"c0ffee00-1234-4abc-8def-0000000000\"1"}`},
	} {
		t.Run(name, func(t *testing.T) {
			up := newCWAttachUpstream(t)
			up.uploadStat = cfg.status
			up.uploadBody = cfg.body
			srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

			resp := postJSON(t, srv.URL+"/v1/messages", cwAttachClaudeBody(cwAttachImgBytes), nil)
			defer resp.Body.Close()
			rawResp, _ := io.ReadAll(resp.Body)
			if resp.StatusCode == 200 {
				t.Fatalf("上传失败必须显式报错，不得成功（体: %s）", rawResp)
			}
			if !strings.Contains(string(rawResp), "upload") {
				t.Fatalf("错误信封必须点明上传面: %s", rawResp)
			}
			// 恰好一次上传尝试；不得发 completion（不静默：既不是成功也不是空白）。
			if n := len(up.callsWithSuffix("/upload")); n != 1 {
				t.Fatalf("上传尝试 = %d, want 1", n)
			}
			assertNoCWAttachPath(t, up, "/completion")
		})
	}
}

// TestUploadNon200BranchesExplicit 非 200 上传分支补齐（W22-1 只覆盖 500 的非 200
// 断言）：403 / 413 / 500 / 非 JSON 响应体 → 下游**显式 502**（预提交失败终态）
// + 恰 1 次上传尝试 + 不发 completion。既有 TestUploadFailureIsExplicit 保持
// W22-1 原样（本测试补 502 断言与其余分支）。
func TestUploadNon200BranchesExplicit(t *testing.T) {
	for name, cfg := range map[string]struct {
		status int
		body   string
	}{
		"403":        {status: 403},
		"413":        {status: 413, body: `{"error":{"message":"file too large"}}`},
		"500":        {status: 500},
		"非 JSON 响应体": {body: `<!doctype html><html>bad gateway</html>`},
	} {
		t.Run(name, func(t *testing.T) {
			up := newCWAttachUpstream(t)
			up.uploadStat = cfg.status
			up.uploadBody = cfg.body
			srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

			resp := postJSON(t, srv.URL+"/v1/messages", cwAttachClaudeBody(cwAttachImgBytes), nil)
			defer resp.Body.Close()
			rawResp, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusBadGateway {
				t.Fatalf("上传失败下游必须 502（预提交失败终态），got %d（体: %s）", resp.StatusCode, rawResp)
			}
			if !strings.Contains(string(rawResp), "upload") {
				t.Fatalf("错误信封必须点明上传面: %s", rawResp)
			}
			if n := len(up.callsWithSuffix("/upload")); n != 1 {
				t.Fatalf("上传尝试 = %d, want 1", n)
			}
			assertNoCWAttachPath(t, up, "/completion")
		})
	}
}

// ---------------------------------------------------------------------------
// ④ 边界：缺 org / 超大附件 / 无图像块（回归）
// ---------------------------------------------------------------------------

// TestUploadRequiresOrg 缺 org：上传链显式报错（直调面——org 缺失在服务流程里
// 先被 org 发现/守卫拦下，这里钉死上传链自身的守卫）。
func TestUploadRequiresOrg(t *testing.T) {
	up := newCWAttachUpstream(t)
	s, _ := startServiceWith(t, cfgClaudeWeb(up.srv.URL, ""))
	att := claudeWebAttachment(cwAttachImgBytes)
	if _, err := s.claudeWebUpload(t.Context(), 0, s.cfg.Upstreams[0], "", att); err == nil || !strings.Contains(err.Error(), "organization") {
		t.Fatalf("缺 org 必须显式报错: %v", err)
	}
	if _, err := s.claudeWebUpload(t.Context(), 0, s.cfg.Upstreams[0], "../evil", att); err == nil || !strings.Contains(err.Error(), "safe path segment") {
		t.Fatalf("非安全 org 必须显式报错: %v", err)
	}
	if len(up.snapshot()) != 0 {
		t.Fatalf("守卫失败不得打上游")
	}
}

// TestUploadRejectsOversize 大文件边界（KB 裁定 = BK 20MB 原值）：直调面显式报错。
func TestUploadRejectsOversize(t *testing.T) {
	up := newCWAttachUpstream(t)
	s, _ := startServiceWith(t, cfgClaudeWeb(up.srv.URL, ""))
	att := claudeWebAttachment(make([]byte, 20<<20+1))
	if _, err := s.claudeWebUpload(t.Context(), 0, s.cfg.Upstreams[0], cwAttachOrgUUID, att); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("超大附件必须显式报错: %v", err)
	}
	if len(up.snapshot()) != 0 {
		t.Fatalf("超大附件不得打上游")
	}
}

// TestUploadSkippedWithoutImages 无图像块：零上传调用（缺省关回归）。
func TestUploadSkippedWithoutImages(t *testing.T) {
	up := newCWAttachUpstream(t)
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))
	resp := postJSON(t, srv.URL+"/v1/messages",
		`{"model":"claude-sonnet-5-5","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()
	rawResp, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=\n%s", resp.StatusCode, rawResp)
	}
	comp := waitCWAttachCompletion(t, up)
	files, _ := completionFiles(t, comp)
	if len(files) != 0 {
		t.Fatalf("无图像块 files = %v, want []", files)
	}
	if n := len(up.callsWithSuffix("/upload")); n != 0 {
		t.Fatalf("无图像块不得上传（%d 次）", n)
	}
}

// ---------------------------------------------------------------------------
// 并发不串台（W24-1 接缝语义：sink 按请求局部，零全局共享面）
// ---------------------------------------------------------------------------

// TestUploadConcurrentNoCrosstalk N 个并发请求各带**不同**图像载荷：每个请求的
// 上传字节必须 = 自身载荷，completion files[] 必须 = 自身载荷那次上传返回的
// uuid——任何跨请求错配（共享槽位/全局配对表回归）都会在此红。
//
// 交错强制（org 门）：假上游的 org 发现等全部 N 个请求到齐才放行——所有请求先
// 完成渲染、再一起进入会话准备 = 「渲染↔准备」的最大交错形态（共享槽位必然
// 串台；sink 按请求局部 = 天然隔离）。
func TestUploadConcurrentNoCrosstalk(t *testing.T) {
	up := newCWAttachUpstream(t)
	// 上传响应按记录序返回唯一 uuid（seq 从 0 起，与记录同锁分配——断言可按
	// 记录序重建 payload ↔ uuid 映射）。
	up.uploadSeqUUID = func(seq int) string { return fmt.Sprintf("c0ffee00-1234-4abc-8def-%012d", seq+1) }

	const n = 4
	up.orgGate = &cwOrgGate{n: n, open: make(chan struct{})}
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))
	payloads := make([][]byte, n)
	markers := make([]string, n)
	for i := range n {
		payloads[i] = []byte(fmt.Sprintf("image-bytes-%d", i))
		markers[i] = fmt.Sprintf("look-%d", i)
	}

	// 并发发起；断言不在子 goroutine 里 t.Fatalf——结果经通道回主 goroutine。
	type shot struct {
		i      int
		status int
		body   []byte
		err    error
	}
	results := make(chan shot, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := http.Post(srv.URL+"/v1/messages", "application/json",
				strings.NewReader(cwAttachClaudeBodyText(markers[i], payloads[i])))
			if err != nil {
				results <- shot{i: i, err: err}
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			results <- shot{i: i, status: resp.StatusCode, body: body}
		}(i)
	}
	wg.Wait()
	close(results)
	for s := range results {
		if s.err != nil {
			t.Fatalf("请求 %d: %v", s.i, s.err)
		}
		if s.status != 200 {
			t.Fatalf("请求 %d: status=%d body=\n%s", s.i, s.status, s.body)
		}
	}

	// 上传记录 → payload ↔ uuid 映射（记录序 = seq 序）。
	uploads := up.callsWithSuffix("/upload")
	if len(uploads) != n {
		t.Fatalf("上传调用数 = %d, want %d", len(uploads), n)
	}
	uuidByPayload := map[string]string{}
	for _, call := range uploads {
		if call.seq < 0 {
			t.Fatalf("上传记录缺 seq: %+v", call)
		}
		_, _, _, data := parseMultipartUpload(t, call)
		uuidByPayload[string(data)] = up.uploadSeqUUID(call.seq)
	}
	if len(uuidByPayload) != n {
		t.Fatalf("上传载荷必须两两不同（串台：映射 = %v）", uuidByPayload)
	}

	// completion 记录 → 文本标记归属 → files[] 必须 = 自身载荷的 uuid。
	comps := up.callsWithSuffix("/completion")
	if len(comps) != n {
		t.Fatalf("completion 调用数 = %d, want %d", len(comps), n)
	}
	seen := map[int]bool{}
	for _, comp := range comps {
		files, prompt := completionFiles(t, comp)
		hit := -1
		for i, m := range markers {
			if strings.Contains(prompt, m) {
				hit = i
				break
			}
		}
		if hit < 0 {
			t.Fatalf("completion 无法归属请求（prompt=%q）", prompt)
		}
		if seen[hit] {
			t.Fatalf("请求 %d 出现多个 completion", hit)
		}
		seen[hit] = true
		want := uuidByPayload[string(payloads[hit])]
		if want == "" {
			t.Fatalf("请求 %d 的载荷未出现在上传记录中（串台）", hit)
		}
		if len(files) != 1 || files[0] != want {
			t.Fatalf("请求 %d completion files = %v, want [%s]（载荷 ↔ uuid 必须同请求配对）", hit, files, want)
		}
	}
	if len(seen) != n {
		t.Fatalf("completion 归属 = %v, want 全部 %d 个请求", seen, n)
	}
}

// ---------------------------------------------------------------------------
// ①b PDF 映射（KB §④：application/pdf → document.pdf；CW:522-534 / YS:498-510）
// ---------------------------------------------------------------------------

// TestUploadFilenameMapping 内容类型 → 上传文件名映射表（CW/YS 原表：image.jpg /
// image.png / document.pdf，其余 `file`；大小写/空白容忍——claudeWebUploadFilename
// 先 ToLower+TrimSpace）。
func TestUploadFilenameMapping(t *testing.T) {
	cases := map[string]string{
		"image/jpeg":               "image.jpg",
		"image/jpg":                "image.jpg",
		"image/png":                "image.png",
		"IMAGE/PNG":                "image.png",
		" image/png ":              "image.png",
		"application/pdf":          "document.pdf",
		"APPLICATION/PDF":          "document.pdf",
		"image/webp":               "file",
		"application/octet-stream": "file",
		"":                         "file",
	}
	for mediaType, want := range cases {
		if got := claudeWebUploadFilename(mediaType); got != want {
			t.Errorf("claudeWebUploadFilename(%q) = %q, want %q", mediaType, got, want)
		}
	}
}

// TestUploadPDFDocumentFilename PDF 载体（media type application/pdf）→ multipart
// 文件名 document.pdf + 部件 mime application/pdf + 字节原样；completion files[]
// 回填真实 uuid。三载体形态：claude_messages image 块（base64）/ openai data-URI /
// URL 取回（响应头 application/pdf）。
func TestUploadPDFDocumentFilename(t *testing.T) {
	pdfBytes := []byte("%PDF-1.7 fake-pdf-attachment-bytes")

	// assertPDFUpload 断言唯一一次上传的 multipart 形状 = PDF 映射。
	assertPDFUpload := func(t *testing.T, up *cwAttachUpstream) {
		t.Helper()
		uploads := up.callsWithSuffix("/upload")
		if len(uploads) != 1 {
			t.Fatalf("上传调用数 = %d, want 1（全部调用: %v）", len(uploads), up.snapshot())
		}
		name, filename, partType, data := parseMultipartUpload(t, uploads[0])
		if name != "file" || filename != "document.pdf" || partType != "application/pdf" {
			t.Fatalf("PDF 上传 = %s/%s/%s, want file/document.pdf/application/pdf", name, filename, partType)
		}
		if !bytes.Equal(data, pdfBytes) {
			t.Fatalf("上传字节 = %q, want %q", data, pdfBytes)
		}
		comp := waitCWAttachCompletion(t, up)
		files, _ := completionFiles(t, comp)
		if len(files) != 1 || files[0] != cwAttachFileUUID {
			t.Fatalf("completion files = %v, want [%s]", files, cwAttachFileUUID)
		}
	}

	t.Run("claude_messages image 块", func(t *testing.T) {
		up := newCWAttachUpstream(t)
		srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))
		resp := postJSON(t, srv.URL+"/v1/messages",
			cwAttachClaudeBodyImages("look", cwAttachImage{mediaType: "application/pdf", raw: pdfBytes}), nil)
		defer resp.Body.Close()
		rawResp, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("status=%d body=\n%s", resp.StatusCode, rawResp)
		}
		assertPDFUpload(t, up)
	})

	t.Run("openai data-URI", func(t *testing.T) {
		up := newCWAttachUpstream(t)
		srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))
		resp := postJSON(t, srv.URL+"/v1/chat/completions",
			cwAttachOpenAIBody(cwAttachDataURI("application/pdf", pdfBytes)), nil)
		defer resp.Body.Close()
		rawResp, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("status=%d body=\n%s", resp.StatusCode, rawResp)
		}
		assertPDFUpload(t, up)
	})

	t.Run("URL 取回（响应头 application/pdf）", func(t *testing.T) {
		up := newCWAttachUpstream(t)
		up.imgBody = pdfBytes
		srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))
		// SSRF 守卫解析面注入（同 TestUploadURLCarrierFetchesThenUploads）。
		old := claudeWebLookupIP
		claudeWebLookupIP = func(string) ([]net.IP, error) { return []net.IP{net.ParseIP("93.184.216.34")}, nil }
		t.Cleanup(func() { claudeWebLookupIP = old })

		resp := postJSON(t, srv.URL+"/v1/chat/completions", cwAttachOpenAIBody(up.srv.URL+"/doc.pdf"), nil)
		defer resp.Body.Close()
		rawResp, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("status=%d body=\n%s", resp.StatusCode, rawResp)
		}
		if len(up.callsWithSuffix("/doc.pdf")) != 1 {
			t.Fatalf("URL 载体必须先取回（GET /doc.pdf）")
		}
		assertPDFUpload(t, up)
	})
}

// ---------------------------------------------------------------------------
// ②b 多图：一次请求多张图 → 逐张上传 + files[] 顺序稳定
// ---------------------------------------------------------------------------

// TestUploadMultiImageOrderStable 一次请求多张图（claude_messages 3 块 / openai 2
// image_url）：上传次数 = 图数、上传记录序 = 块序（载荷逐一对应、文件名按各自
// 内容类型）、completion files[] = 各次上传返回的 uuid 按同一序（uploadSeqUUID
// 按 seq 唯一）。任何顺序错位（反序/乱序上传、files 与上传错配）必红。
func TestUploadMultiImageOrderStable(t *testing.T) {
	// assertMultiOrder 断言：上传记录序 ↔ 期望载荷/文件名一一对应；completion
	// files[] 按同一序 = 各 seq 的 uuid。
	assertMultiOrder := func(t *testing.T, up *cwAttachUpstream, wants []cwAttachImage, wantNames []string) {
		t.Helper()
		uploads := up.callsWithSuffix("/upload")
		if len(uploads) != len(wants) {
			t.Fatalf("上传调用数 = %d, want %d", len(uploads), len(wants))
		}
		for k, call := range uploads {
			name, filename, partType, data := parseMultipartUpload(t, call)
			if name != "file" || filename != wantNames[k] || partType != wants[k].mediaType || !bytes.Equal(data, wants[k].raw) {
				t.Fatalf("上传[%d] = %s/%s/%s/%q, want file/%s/%s/%q",
					k, name, filename, partType, data, wantNames[k], wants[k].mediaType, wants[k].raw)
			}
		}
		comp := waitCWAttachCompletion(t, up)
		files, _ := completionFiles(t, comp)
		if len(files) != len(wants) {
			t.Fatalf("completion files = %v, want %d", files, len(wants))
		}
		for k := range wants {
			if want := up.uploadSeqUUID(k); files[k] != want {
				t.Fatalf("completion files[%d] = %q, want %q（files 顺序必须 = 上传序）", k, files[k], want)
			}
		}
	}

	t.Run("claude_messages 三图", func(t *testing.T) {
		up := newCWAttachUpstream(t)
		up.uploadSeqUUID = func(seq int) string { return fmt.Sprintf("c0ffee00-1234-4abc-8def-%012d", seq+1) }
		srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))
		wants := []cwAttachImage{
			{mediaType: "image/png", raw: []byte("multi-image-0")},
			{mediaType: "image/jpeg", raw: []byte("multi-image-1")},
			{mediaType: "application/pdf", raw: []byte("%PDF-multi-image-2")},
		}
		resp := postJSON(t, srv.URL+"/v1/messages", cwAttachClaudeBodyImages("look", wants...), nil)
		defer resp.Body.Close()
		rawResp, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("status=%d body=\n%s", resp.StatusCode, rawResp)
		}
		assertMultiOrder(t, up, wants, []string{"image.png", "image.jpg", "document.pdf"})
	})

	t.Run("openai 两 image_url", func(t *testing.T) {
		up := newCWAttachUpstream(t)
		up.uploadSeqUUID = func(seq int) string { return fmt.Sprintf("c0ffee00-1234-4abc-8def-%012d", seq+1) }
		srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))
		wants := []cwAttachImage{
			{mediaType: "image/png", raw: []byte("openai-multi-0")},
			{mediaType: "image/jpeg", raw: []byte("openai-multi-1")},
		}
		urls := []string{
			cwAttachDataURI(wants[0].mediaType, wants[0].raw),
			cwAttachDataURI(wants[1].mediaType, wants[1].raw),
		}
		resp := postJSON(t, srv.URL+"/v1/chat/completions", cwAttachOpenAIBodyMulti(urls...), nil)
		defer resp.Body.Close()
		rawResp, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("status=%d body=\n%s", resp.StatusCode, rawResp)
		}
		assertMultiOrder(t, up, wants, []string{"image.png", "image.jpg"})
	})
}

// ---------------------------------------------------------------------------
// ③b 上传超时（挂住不响应）→ 显式错误
// ---------------------------------------------------------------------------

// TestUploadTimeoutIsExplicit 上传挂住（假上游延迟不响应）→ 出站客户端超时 →
// 下游 502（错误信封含 "upload" + "deadline"）+ 恰 1 次上传尝试 + 不发 completion。
//
// 超时注入：假上游 uploadDelay 挂住（调用仍被记录）；出站客户端（s.client——
// 生产 = ResponseHeaderTimeout 60s，此处测试缩到 500ms）与生产同一取消面
// （请求 ctx 取消 → client.Do 返回错误），只是时限缩短。
func TestUploadTimeoutIsExplicit(t *testing.T) {
	up := newCWAttachUpstream(t)
	up.uploadDelay = 5 * time.Second
	s, srv := startServiceWith(t, cfgClaudeWeb(up.srv.URL, ""))
	s.client = &http.Client{Transport: s.client.Transport, Timeout: 500 * time.Millisecond}

	resp := postJSON(t, srv.URL+"/v1/messages", cwAttachClaudeBody(cwAttachImgBytes), nil)
	defer resp.Body.Close()
	rawResp, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("上传超时必须显式报错（502），got %d（体: %s）", resp.StatusCode, rawResp)
	}
	if body := string(rawResp); !strings.Contains(body, "upload") || !strings.Contains(body, "deadline") {
		t.Fatalf("错误信封必须点明上传面 + 超时: %s", body)
	}
	if n := len(up.callsWithSuffix("/upload")); n != 1 {
		t.Fatalf("上传尝试 = %d, want 1", n)
	}
	assertNoCWAttachPath(t, up, "/completion")
}

// ---------------------------------------------------------------------------
// ④b 空 prompt + files[]（W25-1 §9 未验面）：行为裁定 = 能走
// ---------------------------------------------------------------------------

// TestUploadEmptyPromptWithFiles 图像-only 请求（无文本块）：渲染面守卫是「无
// 文本**且无附件**」才报错——空 prompt + 非空 files[] 照常走全链（上传 + 发
// completion，prompt 为空串）。上游对空 prompt 的接受度 [UNVERIFIED]（真机未发，
// W25-1 §9）；本地钉死本仓行为 = 能走（不拒绝）。
func TestUploadEmptyPromptWithFiles(t *testing.T) {
	up := newCWAttachUpstream(t)
	srv := startService(t, cfgClaudeWeb(up.srv.URL, ""))

	resp := postJSON(t, srv.URL+"/v1/messages",
		cwAttachClaudeBodyImages("", cwAttachImage{mediaType: "image/png", raw: cwAttachImgBytes}), nil)
	defer resp.Body.Close()
	rawResp, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("图像-only 请求必须能走（裁定=能走）: status=%d body=\n%s", resp.StatusCode, rawResp)
	}
	if !strings.Contains(string(rawResp), "PONG") {
		t.Fatalf("下游未见 fixture 回放内容: %s", rawResp)
	}
	if n := len(up.callsWithSuffix("/upload")); n != 1 {
		t.Fatalf("上传调用数 = %d, want 1", n)
	}
	comp := waitCWAttachCompletion(t, up)
	files, prompt := completionFiles(t, comp)
	if len(files) != 1 || files[0] != cwAttachFileUUID {
		t.Fatalf("completion files = %v, want [%s]", files, cwAttachFileUUID)
	}
	if prompt != "" {
		t.Fatalf("空 prompt + files[] 组合的 prompt = %q, want 空串", prompt)
	}
}
