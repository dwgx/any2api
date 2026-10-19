// claude_web 上游客户端面（W10）：会话准备（org 发现 + conversation 创建）+ 头面
// 常量装配。
//
// 依据：docs/architecture.md（W9-1 源码级，7 实现互证）
// + A2A-L19-W9-2 实抓（4 条完整 completion SSE 流 + 端点全表）。
//
// 调用链（每个 completion 请求一次；KB §架构与数据流「六段流水线」的最小面）：
//
//	org：配置 org_id 非空 → 直接用（跳过网络调用）；否则 GET /api/organizations
//	     → 选择规则见 claudeWebDiscoverOrg（YS core/api.go:112-148 同款）
//	附件（W22-1；接缝 W24-1）：请求体含图像块时，渲染面产出附件作业并经
//	     `pairs.Options.AttachmentSink` 交付（forward 持有 sink 并传入本函数）
//	     ——逐份 `POST /api/{org}/upload` multipart → file_uuid 原地等长回填进
//	     completion 体 `files[]`（见本文件「附件上传面」节；零全局状态）；
//	     无图像块 / 同协议直通 = nil 作业，零行为变化
//	会话：POST /api/organizations/{org}/chat_conversations
//	     body = claude_web.RenderConversationCreate（客户端生成 uuid，CW/YS 同款）
//	     → 200/201 取响应 uuid；400/409 = 已存在 → 用我们生成的 uuid（CWK
//	     client.py:655-680 同款）；其余状态 = 失败（换上游可救，属预提交失败）
//	补全：POST .../chat_conversations/{conv}/completion（SSE；forward 的主体）
//
// 纪律：
//   - **凭据（sessionKey）只进 cookie 头**，绝不进 URL/查询串/日志/错误消息
//     （头注入守卫：含 CR/LF/`;` 的值直接报错，绝不拼进 Cookie 头）；
//   - 上游错误体经 claude_web.ParseError 提取消息（绝不把上游原文透传给下游）；
//   - 路径段（org/conv）只接受 UUID 形态（十六进制 + `-`）——绝不拼畸形/注入 URL
//     （GW 陷阱 13 同款守卫）；
//   - 三个端点都是**每请求**调用（无跨请求状态：本 slice 不做会话复用——多轮由
//     下游每次带全量历史、服务端拍平成单 prompt，见 claude_web 包 RenderRequest）。
package service

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"time"

	"github.com/dwgx/any2api/translate/convert/claude_web"
)

// init 注册注入段字节账观测（W21-2 配额观测；纯观测，绝不进控制流、不改协议）：
// 每次工具模拟注入渲染完成后打一行服务日志（字节数 + 工具条数）——U5 的 prompt
// 体积压力（CC 22 工具每轮重注入）在服务日志里可直接观测。
//
// 接线约束：本单（A2A-L19-W21-2）只准写本文件——forward 的批量诊断消费行
// （server.go / relay.go 的 consumeBatchDiagnostics）在 scope 外，故经包级观测
// 钩子注册（服务启动即生效，零改动其余 service 文件）；钩子在 claude_web 渲染
// 路径同步调用（服务进程内所有 pairs.Convert 请求面渲染都经过它）。
func init() {
	claude_web.SetToolSimInjectionObserver(func(injectionBytes, tools int) {
		log.Printf("any2api: claude_web tool_sim: prompt injection rendered (%d bytes, %d tools)", injectionBytes, tools)
	})
}

// claudeWebUA 浏览器 UA——**W9-2 实抓原值**（真实浏览器会话，零 CF 挑战）：
// `X11; Linux x86_64` + Chrome 155。W11-2 实测（2026-10-14）：CF 403 的触发因子
// 是 UA/头面而非 TLS 指纹（同 curl 仅换 Chrome UA 即 403→200）——UA 是本面上
// 唯一的硬门，取实抓原值。sec-ch-ua 三件套（见 applyClaudeWebHeaders）与本 UA
// 同版本同平台（实抓同源，逐字节一致）。
// W48-1 复核（2026-10-15）：本 UA/头面与产品直连腿（Go + ALPN h2）组合直连全绿
// （GET 200 + 实机 completion 200）；只有与 UA 不自洽的「无 ALPN 非浏览器腿」才被
// 挑战——使用约束见 applyClaudeWebHeaders 头注。
const claudeWebUA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 " +
	"(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36"

// claudeWebDefaultBaseURL 缺省基址（配置 base_url 为空时用；KB：claude.ai）。
const claudeWebDefaultBaseURL = "https://claude.ai"

// claudeWebBase claude_web 上游基址（去尾斜杠；缺省 claude.ai）。
func claudeWebBase(up Upstream) string {
	b := strings.TrimSpace(up.BaseURL)
	if b == "" {
		return claudeWebDefaultBaseURL
	}
	return strings.TrimRight(b, "/")
}

// claudeWebTarget 一次 completion 调用的目标面（会话准备的结果）。
type claudeWebTarget struct {
	// path completion 端点路径（含 org/conversation 两段动态段）。
	path string
	// referer 该阶段的 referer（浏览器按阶段切换：/new → /chat/{conv}，
	// KB §头面「拟真杂项」）。
	referer string
	org     string
	conv    string
}

// claudeWebPrepare 会话准备（W53-2 凭据池包装）：选凭据 → 准备链 → 未产出任何
// 字节且失败类别 = 配额/凭据类 → 池内换号重试 → 耗尽语义。
//
// 池语义（credentials_pool.go 头注）：选择 = 最少近期使用（跨请求轮转）；
// 单凭据（api_key 兼容 = 单元素池）与引入前行为逐字节一致。**准备链恒未产出
// 下游字节**（completion 尚未发出），换号重试安全；失败分类（classifyClaudeWebPrepareFailure）：
// 401/403 无盾（sessionKey 失效）/ 429 → 冷却该索引并换下一凭据（每凭据至多
// 试一次）；CF 盾 403 / 5xx / 网络 / 请求问题 → 不换号，原样上抛（既有 failover
// 语义处理）。全部冷却 → upstream_credentials_exhausted 显式报错。
//
// 选定索引经 claudeWebTarget.referer 的标记（credentialMarkerPrefix）传给
// completion 面——applyClaudeWebHeaders 据此用**同一凭据**装配 cookie（attempt
// 局部、并发安全）；会话登记到池（noteConversation）供清理面 DELETE 认证。
//
// 失败 = 预提交失败（下游未提交）——调用方按 C1 分类决定换上游；本函数只负责
// 显式报错（错误消息不含凭据值）。idx = 上游下标（W11：出站客户端按上游选——
// proxy / TLS 指纹只作用于该上游）。job = 本请求的附件作业（渲染面经
// `pairs.Options.AttachmentSink` 交付、forward 传入；无图像块/同协议直通 = nil，
// 零行为变化）——上传 + 原地等长回填见「附件上传面」节（claude_web/attach.go
// 头注）。
func (s *Server) claudeWebPrepare(ctx context.Context, idx int, up Upstream, model string, job *claude_web.AttachmentJob) (claudeWebTarget, error) {
	pool := s.cfg.credentialPoolAt(idx)
	if pool.size() == 0 {
		// 无凭据（claude_web 未配 api_key/credentials）：既有单凭据路径（请求期
		// 显式报错——行为与引入前一致）。
		return s.claudeWebPrepareOnce(ctx, idx, up, model, job)
	}
	now := time.Now()
	var lastErr error
	for range pool.size() {
		ci, ok := pool.selectCredential(now)
		if !ok {
			// 全部凭据冷却中（耗尽）：本调用尚未试过任何凭据 → 耗尽错误；
			// 已试过（本轮逐个冷却）→ 返回最后一个凭据的失败（更具信息量）。
			if lastErr != nil {
				return claudeWebTarget{}, lastErr
			}
			return claudeWebTarget{}, pool.exhaustedError(now)
		}
		cred, ok := pool.credential(ci)
		if !ok {
			return claudeWebTarget{}, pool.exhaustedError(now)
		}
		upCred := up
		upCred.APIKey = cred // 本尝试凭据只进内存（请求头）；绝不进日志/错误。
		t, err := s.claudeWebPrepareOnce(ctx, idx, upCred, model, job)
		if err == nil {
			pool.noteConversation(t.conv, ci, now)
			t.referer = credentialMarkedReferer(t.referer, ci)
			return t, nil
		}
		// P0-4：该凭据本次准备尝试收尾 → 释放 busy（成功面由 forward 的 attempt
		// 收尾经 defer 释放；public 闸关闭时 Release no-op——零行为变化）。
		pool.Release(ci)
		lastErr = err
		cls, retryAfter, rotate := classifyClaudeWebPrepareFailure(err)
		if !rotate {
			return claudeWebTarget{}, err // 非配额/凭据类：不换号（既有 failover 语义）。
		}
		pool.reportFailure(ci, cls, retryAfter, now)
		if pool.size() > 1 {
			log.Printf("any2api: claude_web upstream %s credential #%d failed (class=%s); retrying with another credential",
				up.Name, ci, cls)
		}
	}
	return claudeWebTarget{}, lastErr
}

// claudeWebPrepareOnce 一次准备链（org 发现 → 附件上传 → conversation 创建 →
// completion 目标），**用传入 up 的凭据**（池选定者——见 claudeWebPrepare）。
func (s *Server) claudeWebPrepareOnce(ctx context.Context, idx int, up Upstream, model string, job *claude_web.AttachmentJob) (claudeWebTarget, error) {
	org := strings.TrimSpace(up.OrgID)
	if org == "" {
		o, err := s.claudeWebDiscoverOrg(ctx, idx, up)
		if err != nil {
			return claudeWebTarget{}, err
		}
		org = o
	}
	if !isSafePathSegment(org) {
		return claudeWebTarget{}, fmt.Errorf("claude_web: organization uuid %q is not a safe path segment", org)
	}
	if job != nil {
		// 上传在**建会话之前**：上传失败不留待清理的会话（W13 清理面只挂在本
		// 函数返回后的会话上）；失败 = 预提交失败（下游未提交，按 C1 换上游），
		// 错误显式上抛，绝不静默（brief ③：失败不静默）。
		for i, att := range job.Attachments() {
			ref, err := s.claudeWebUpload(ctx, idx, up, org, att)
			if err != nil {
				return claudeWebTarget{}, err
			}
			if err := job.Resolve(i, ref); err != nil {
				return claudeWebTarget{}, err
			}
		}
	}
	conv, err := claude_web.NewUUID()
	if err != nil {
		return claudeWebTarget{}, err
	}
	body, err := claude_web.RenderConversationCreate(conv, model)
	if err != nil {
		return claudeWebTarget{}, err
	}
	url := claudeWebBase(up) + "/api/organizations/" + org + "/chat_conversations"
	status, respHeader, respBody, err := s.claudeWebCall(ctx, idx, up, http.MethodPost, url, body, claudeWebBase(up)+"/new")
	if err != nil {
		return claudeWebTarget{}, err
	}
	switch status {
	case http.StatusOK, http.StatusCreated:
		// 200/201：响应体带 uuid 时以它为准（CW/YS 期望 201 + `{uuid}`）；
		// 体解析失败/无 uuid 时用我们生成的（客户端生成的 uuid 就是 URL 里那个，
		// W9-2 §2.1「隐式创建」实测）。
		var out struct {
			UUID string `json:"uuid"`
		}
		if json.Unmarshal(respBody, &out) == nil && out.UUID != "" {
			if !isSafePathSegment(out.UUID) {
				return claudeWebTarget{}, fmt.Errorf("claude_web: conversation uuid %q from upstream is not a safe path segment", out.UUID)
			}
			conv = out.UUID
		}
	case http.StatusBadRequest, http.StatusConflict:
		// 400/409 = 会话已存在（CWK client.py:655-680 `ensure_conversation`）→
		// 用客户端生成的 uuid 继续。
	default:
		return claudeWebTarget{}, claudeWebStatusError("create conversation", status, respHeader, respBody)
	}
	return claudeWebTarget{
		path: "/api/organizations/" + org + "/chat_conversations/" + conv + "/completion",
		// referer 与实抓首回合对齐：W9-2 实抓的 completion（首回合，conversation
		// 隐式创建）referer 恒为 `/new`（三 run 一致）——本实现每请求新建会话 =
		// 恒首回合，故 completion 也用 `/new`（第二回合起才切 `/chat/{conv}`，
		// run2 实测；本 slice 不产生该形态）。W53-2：池包装在返回前附加索引标记
		// （credentialMarkedReferer）——applyClaudeWebHeaders 剥离后 Referer 头
		// 即此值，wire 逐字节不变。
		referer: claudeWebBase(up) + "/new",
		org:     org,
		conv:    conv,
	}, nil
}

// claudeWebDiscoverOrg `GET /api/organizations` → 组织 UUID。
//
// 选择规则（YS core/api.go:112-148，KB §组织发现主路径）：单组织 → 直接用；
// 多组织 → 取第一个 `rate_limit_tier` ∈ {default_claude_ai, default_claude_max_20x,
// default_raven_enterprise} 的组织；无命中 → 显式报错（不猜——逃生舱 = 配置
// `org_id` 显式指定；CR oauth.py:117-131 的 `raven_type=="team"` 优先是变体规则，
// 本 slice 不采用）。
//
// 存活三态（CR session_check.py:186-213）在本函数收敛为两种错误措辞：401/403 且
// 带 `cf-mitigated` 头 = 盾拦截（blocked，无法判定）；401/403 无盾 = sessionKey
// 失效（dead）。错误消息只说状态与类别，绝不含凭据值。
func (s *Server) claudeWebDiscoverOrg(ctx context.Context, idx int, up Upstream) (string, error) {
	url := claudeWebBase(up) + "/api/organizations"
	status, respHeader, respBody, err := s.claudeWebCall(ctx, idx, up, http.MethodGet, url, nil, claudeWebBase(up)+"/new")
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", claudeWebStatusError("list organizations", status, respHeader, respBody)
	}
	var orgs []struct {
		UUID          string `json:"uuid"`
		RateLimitTier string `json:"rate_limit_tier"`
	}
	if err := json.Unmarshal(respBody, &orgs); err != nil {
		return "", fmt.Errorf("claude_web: organizations response is not a JSON array: %w", err)
	}
	if len(orgs) == 0 {
		return "", errors.New("claude_web: no organizations found for this sessionKey")
	}
	if len(orgs) == 1 {
		return orgs[0].UUID, nil
	}
	for _, o := range orgs {
		switch o.RateLimitTier {
		case "default_claude_ai", "default_claude_max_20x", "default_raven_enterprise":
			return o.UUID, nil
		}
	}
	return "", fmt.Errorf("claude_web: %d organizations but none with a default rate_limit_tier (set upstream org_id explicitly)", len(orgs))
}

// claudeWebHTTPError 准备链非 2xx 的显式错误（W53-2）：除消息外携带分类面
// （状态码 + 响应头）——凭据池的冷却/换号分类用（cf-mitigated 盾特征、
// retry-after 解析）。**消息绝不含凭据字节**（上游原文经 ParseError 提取）。
type claudeWebHTTPError struct {
	stage  string
	status int
	header http.Header
	msg    string
}

func (e *claudeWebHTTPError) Error() string { return e.msg }

// claudeWebStatusError 非 2xx → 显式错误（消息经 claude_web.ParseError 提取；
// 上游原文只进 Raw 供日志，绝不透传下游）。W53-2：返回 *claudeWebHTTPError
// （带状态/头——凭据池分类用；消息形态与引入前逐字一致）。
func claudeWebStatusError(stage string, status int, header http.Header, body []byte) error {
	e := claude_web.ParseError(body)
	hint := ""
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		hint = " (sessionKey invalid or Cloudflare-blocked)"
	case http.StatusTooManyRequests:
		hint = " (rate limited)"
	}
	return &claudeWebHTTPError{
		stage:  stage,
		status: status,
		header: header,
		msg:    fmt.Sprintf("claude_web: %s: upstream status %d%s: %s", stage, status, hint, e.Message),
	}
}

// classifyClaudeWebPrepareFailure 准备链失败 → 凭据池处置（W53-2）：
//
//   - 401、403（**无 cf-mitigated 盾特征**——sessionKey 失效，见
//     claudeWebDiscoverOrg 存活三态）→ 凭据类（长冷却）+ 换号重试；
//   - 429 → 配额类（retry-after 精确执行，缺省短冷却）+ 换号重试；
//   - 403 带盾（CF 挑战是客户端级、换号无用）/ 5xx / 网络 / 请求问题 / 本地
//     失败 → 不处置（rotate=false——原样上抛，走既有 failover 语义）。
func classifyClaudeWebPrepareFailure(err error) (cls credentialFailureClass, retryAfter time.Duration, rotate bool) {
	var he *claudeWebHTTPError
	if !errors.As(err, &he) {
		return 0, 0, false
	}
	switch he.status {
	case http.StatusUnauthorized, http.StatusForbidden:
		if he.header.Get("cf-mitigated") != "" {
			return 0, 0, false // 盾拦截：不是凭据问题（换号无用）。
		}
		return credFailureCredential, 0, true
	case http.StatusTooManyRequests:
		return credFailureQuota, retryAfterSeconds(he.header), true
	}
	return 0, 0, false
}

// claudeWebCall 发一次 claude_web 请求（org 发现 / 会话创建；completion 由 forward
// 主体发）。返回状态码、响应头与响应体（上限 maxErrorBodyBytes——这三个端点都是
// 小体）。W53-2：响应头一并返回（凭据池分类用——cf-mitigated 盾特征 / retry-after）。
// idx = 上游下标（W11：出站客户端按上游选）。
func (s *Server) claudeWebCall(ctx context.Context, idx int, up Upstream, method, url string, body []byte, referer string) (int, http.Header, []byte, error) {
	return s.claudeWebCallCT(ctx, idx, up, method, url, body, "application/json", referer)
}

// claudeWebCallCT claudeWebCall 的显式 Content-Type 变体（multipart 上传面用；
// body == nil 时不设 Content-Type——GET 无体）。
func (s *Server) claudeWebCallCT(ctx context.Context, idx int, up Upstream, method, url string, body []byte, contentType, referer string) (int, http.Header, []byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("claude_web: build %s request: %w", method, err)
	}
	if err := applyClaudeWebHeaders(req.Header, up, referer, false); err != nil {
		return 0, nil, nil, err
	}
	if body != nil && contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	client, err := s.clientFor(idx)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("claude_web: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("claude_web: %s %s: %w", method, url, err)
	}
	defer resp.Body.Close()
	rb, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	if err != nil {
		return resp.StatusCode, resp.Header, nil, fmt.Errorf("claude_web: read %s response: %w", method, err)
	}
	return resp.StatusCode, resp.Header, rb, nil
}

// ---------------------------------------------------------------------------
// 附件上传面（W22-1）：`POST /api/{org}/upload` multipart → {file_uuid}
// ---------------------------------------------------------------------------
//
// 形态（KB §④ + 五实现互证，逐条注明）：
//   - 端点 = org 级 `/api/{org}/upload`（CW core/api.go:533 / YS :509 /
//     BK claude.go:156 三实现一致；CWK 的 `wiggle/upload-file` 是带会话上下文
//     的另一端点（client.py:590-620）、AU 无上传面——本实现取 org 级形态，
//     与会话准备期的 org 面同源）；
//   - multipart 字段名 `file`（三实现一致：CW SetFileBytes("file",…)（:539）/
//     BK CreateFormFile("file","image")（:147））；
//   - 文件名按内容类型定（CW:517-530 / YS:493-506 的映射：image/jpeg→image.jpg、
//     image/png→image.png、application/pdf→document.pdf、其余 `file`；BK 恒
//     "image"——取 CW/YS 的内容类型映射，PDF 面保留）；
//   - 部件 Content-Type = 内容类型（CWK 显式 mime；CW/YS 经 resty 按扩展名推导同效；
//     BK 用 Go CreateFormFile 的 application/octet-stream 缺省——取显式 mime）；
//   - 头面 = applyClaudeWebHeaders（协议常量 + 身份头 + UA/拟真头；referer
//     `{base}/new`——CW:537 / BK:161 同款）；Accept 非流式 `*/*`（BK claudeAccept）；
//   - 响应 200 + `{"file_uuid":"…"}`（CW/YS/BK 三实现同取该字段；CWK 的 `id`
//     兜底属 wiggle 端点面，不取）。
//
// 边界裁定（KB §④ 五实现并集）：org 缺失/非安全路径段 → 显式报错（绝不拼畸形
// URL）；附件为空或 > claude_web.MaxAttachmentBytes（20MB = BK maxImageBytes
// 原值）→ 显式报错；非 200 → 显式报错（上游原文只进错误消息摘要，绝不含凭据）；
// file_uuid 缺失/非 36 字符 JSON 安全 ASCII → 显式报错（原地等长回填的机械
// 约束，见 claude_web/attach.go；真实上游的 uuid 形态 [UNVERIFIED]——不符时
// 显式失败而非静默）。
func (s *Server) claudeWebUpload(ctx context.Context, idx int, up Upstream, org string, att claude_web.Attachment) (string, error) {
	if strings.TrimSpace(org) == "" {
		return "", errors.New("claude_web: upload requires an organization uuid")
	}
	if !isSafePathSegment(org) {
		return "", fmt.Errorf("claude_web: organization uuid %q is not a safe path segment", org)
	}
	raw, mediaType, err := s.claudeWebAttachmentBytes(ctx, idx, up, att)
	if err != nil {
		return "", err
	}
	if len(raw) == 0 {
		return "", errors.New("claude_web: empty attachment payload")
	}
	if len(raw) > claude_web.MaxAttachmentBytes {
		return "", fmt.Errorf("claude_web: attachment is %d bytes (limit %d)", len(raw), claude_web.MaxAttachmentBytes)
	}
	filename := claudeWebUploadFilename(mediaType)
	body, contentType, err := claudeWebMultipartUpload(filename, mediaType, raw)
	if err != nil {
		return "", err
	}
	url := claudeWebBase(up) + "/api/" + org + "/upload"
	status, respHeader, respBody, err := s.claudeWebCallCT(ctx, idx, up, http.MethodPost, url, body, contentType, claudeWebBase(up)+"/new")
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", claudeWebStatusError("upload file", status, respHeader, respBody)
	}
	var out struct {
		FileUUID string `json:"file_uuid"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return "", fmt.Errorf("claude_web: upload file: malformed response body: %w", err)
	}
	ref := strings.TrimSpace(out.FileUUID)
	if ref == "" {
		return "", errors.New("claude_web: upload file: response has no file_uuid")
	}
	// 回填机械约束（claude_web.AttachmentJob.Resolve）：36 字符 + JSON 字符串安全。
	// 长度不符 = 显式失败（真实上游形态 [UNVERIFIED]，绝不静默截断）。
	if len(ref) != 36 {
		return "", fmt.Errorf("claude_web: upload file: file_uuid is %d chars, want 36 (in-place ref patch requires equal length)", len(ref))
	}
	for i := range len(ref) {
		c := ref[i]
		if c < 0x21 || c > 0x7e || c == '"' || c == '\\' {
			return "", fmt.Errorf("claude_web: upload file: file_uuid contains non-JSON-safe byte %q", c)
		}
	}
	return ref, nil
}

// claudeWebAttachmentBytes 附件字节：inline 载体已解码（渲染期）；URL 载体经
// SSRF 守卫取回（http/https；解析到内网/回环地址即拒绝——BK publicImageURL 同款
// 纪律，image.go:100-120）。返回字节 + 媒体类型（响应头优先，缺省嗅探）。
func (s *Server) claudeWebAttachmentBytes(ctx context.Context, idx int, up Upstream, att claude_web.Attachment) ([]byte, string, error) {
	if att.Data != nil {
		return att.Data, att.MediaType, nil
	}
	if strings.TrimSpace(att.URL) == "" {
		return nil, "", errors.New("claude_web: attachment has neither inline data nor URL")
	}
	target, err := claudeWebPublicURL(att.URL)
	if err != nil {
		return nil, "", err
	}
	client, err := s.clientFor(idx)
	if err != nil {
		return nil, "", fmt.Errorf("claude_web: %w", err)
	}
	// redirect 守卫：≤5 跳，且每跳重过 publicURL（BK CheckRedirect 同款）。
	c := *client
	c.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return errors.New("claude_web: attachment URL redirect limit exceeded")
		}
		_, err := claudeWebPublicURL(req.URL.String())
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, "", fmt.Errorf("claude_web: build attachment fetch request: %w", err)
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("claude_web: fetch attachment URL: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("claude_web: fetch attachment URL: upstream status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, claude_web.MaxAttachmentBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("claude_web: read attachment URL body: %w", err)
	}
	if len(raw) > claude_web.MaxAttachmentBytes {
		return nil, "", fmt.Errorf("claude_web: attachment is larger than %d bytes", claude_web.MaxAttachmentBytes)
	}
	mediaType := strings.TrimSpace(strings.Split(resp.Header.Get("Content-Type"), ";")[0])
	if mediaType == "" {
		mediaType = http.DetectContentType(raw)
	}
	return raw, mediaType, nil
}

// claudeWebPublicURL 附件 URL 守卫（SSRF）：http/https、有主机、无 userinfo、
// 解析出的全部 IP 必须是全局单播且非私有（BK publicImageURL 同款；回环/链路本地/
// 未指定地址都不是全局单播 → 拒绝）。解析面经 claudeWebLookupIP（测试注入点）。
func claudeWebPublicURL(raw string) (*url.URL, error) {
	target, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, errors.New("claude_web: attachment URL is malformed")
	}
	target.Scheme = strings.ToLower(target.Scheme)
	if (target.Scheme != "http" && target.Scheme != "https") || target.Hostname() == "" || target.User != nil {
		return nil, errors.New("claude_web: attachment URL must be http/https without credentials")
	}
	ips, err := claudeWebLookupIP(target.Hostname())
	if err != nil {
		return nil, fmt.Errorf("claude_web: resolve attachment URL host: %w", err)
	}
	for _, ip := range ips {
		if !ip.IsGlobalUnicast() || ip.IsPrivate() {
			return nil, errors.New("claude_web: attachment URL resolves to a non-public address")
		}
	}
	return target, nil
}

// claudeWebLookupIP 附件 URL 主机解析（SSRF 守卫的解析面；测试注入点）。
var claudeWebLookupIP = net.LookupIP

// claudeWebUploadFilename 内容类型 → 上传文件名（CW:522-534 / YS:498-510 映射；
// 未知类型 → `file`。文件名恒来自本固定集合，绝不取用户输入——无头注入面）。
func claudeWebUploadFilename(mediaType string) string {
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "image/jpeg", "image/jpg":
		return "image.jpg"
	case "image/png":
		return "image.png"
	case "application/pdf":
		return "document.pdf"
	default:
		return "file"
	}
}

// claudeWebMultipartUpload 构造 multipart 请求体：字段名 `file`（三实现一致），
// 部件头 = Content-Disposition（filename）+ 显式 Content-Type（媒体类型）。
func claudeWebMultipartUpload(filename, mediaType string, raw []byte) ([]byte, string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, filename))
	if mt := strings.TrimSpace(mediaType); mt != "" {
		h.Set("Content-Type", mt)
	}
	pw, err := w.CreatePart(h)
	if err != nil {
		return nil, "", fmt.Errorf("claude_web: build multipart upload: %w", err)
	}
	if _, err := pw.Write(raw); err != nil {
		return nil, "", fmt.Errorf("claude_web: build multipart upload: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, "", fmt.Errorf("claude_web: build multipart upload: %w", err)
	}
	return buf.Bytes(), w.FormDataContentType(), nil
}

// applyClaudeWebHeaders 装配 claude.ai 头面（KB §鉴权 + W9-2 实抓 + W11-2 CF 实测）。
//
// 常量三方互证：`anthropic-client-platform: web_claude_ai`（五实现一致）、
// `anthropic-client-version: 1.0.0`（BK/RF/CWK 同值）+ `anthropic-client-sha`
// （claude_web.ClientSHA——W43-2 浏览器实测实况值，2026-10-15）。身份头
// `anthropic-anonymous-id`/`anthropic-device-id` 按凭据派生（BK headers.go:20-41 /
// RF 的 uuid5 命名空间同款——同一账号跨请求身份不变）。
//
// 浏览器拟真面（W11-2 实测：CF 403 触发因子 = UA/头面，非 TLS 指纹——本函数是
// CF 门的唯一装配点，**三个端点（org 发现/会话创建/completion）共用**）：
//   - User-Agent（claudeWebUA）+ **sec-ch-ua 三件套**（与 UA 同版本同平台；
//     W9-2 实抓原值）+ accept-language/priority/Sec-Fetch-* 三件套（KB §头面
//     「拟真杂项」；Sec-Fetch 值 = fetch() 语义 empty/cors/same-origin，CWK
//     client.py:189-191 与 AU client.py 同款）。
//   - 实抓未复制的头（有意排除，见报告）：`traceparent`/`tracestate`/`x-datadog-*`
//     （RUM 遥测，逐请求随机，非判定面）；`anthropic-client-build`/`-capabilities`/
//     `x-activity-session-id`（应用态头，语义未证，5 实现均不发）。
//
// W48-1 头面矩阵裁定（2026-10-15，真机，test@local）：本头面**直连可用**——产品
// 直连腿（Go TLS1.3 + ALPN h2）下 GET /api/organizations 200、实机 completion
// 全流 200；但同一头面挂在**非浏览器 MITM 腿**（无 ALPN、HTTP/1.1）上被盾拦
// 403 `cf-mitigated: challenge`（W45-1/W48-A2 复证）——**全浏览器型头面必须走
// 自洽的直连腿**（浏览器化 TLS/HTTP 形态）；**字节级捕获/调试腿一律改用最小
// 头面**（UA + accept + anthropic-client-platform + sessionKey；W45-1/W48 实测
// 两腿均 200）。
//
// stream=true 时补 `Accept: text/event-stream`（completion；W9-2 实抓同值）；
// 否则 `*/*`（org 发现 / 会话创建——BK claudeAccept 常量）。
//
// W53-2（凭据池）：completion 路径的 referer 携带池索引标记（
// `#any2api-cred-<n>`，claudeWebPrepare 附加）——凭据用池内该索引（本尝试选定
// 者，attempt 局部、并发安全），Referer 头写剥离标记后的原值（wire 逐字节与
// 引入前一致）；无标记 = 既有单凭据路径（up.APIKey）。
func applyClaudeWebHeaders(h http.Header, up Upstream, referer string, stream bool) error {
	key := strings.TrimSpace(up.APIKey)
	bare, markerIdx, marked, err := splitCredentialMarker(referer)
	if err != nil {
		return err
	}
	if marked {
		creds := upstreamCredentials(up)
		if markerIdx >= len(creds) {
			return errors.New("claude_web: credential marker out of range")
		}
		key = strings.TrimSpace(creds[markerIdx])
		referer = bare
	}
	if key == "" {
		return errors.New("claude_web: api_key (sessionKey) is required")
	}
	// 头注入守卫：cookie 值只允许可见 ASCII 且不含 `;`/空白/控制字符
	//（sessionKey 形态 `sk-ant-sid0*` 天然满足；畸形值显式报错，绝不拼进头）。
	if strings.ContainsAny(key, ";\r\n\t ") || !isPrintableASCII(key) {
		return errors.New("claude_web: api_key contains characters not allowed in a cookie value")
	}
	h.Set("Cookie", claude_web.CookieSessionKey+"="+key)
	h.Set(claude_web.HeaderClientPlatform, claude_web.ClientPlatform)
	h.Set(claude_web.HeaderClientVersion, claude_web.ClientVersion)
	h.Set(claude_web.HeaderClientSHA, claude_web.ClientSHA)
	anon, device := claudeWebIdentity(key)
	h.Set(claude_web.HeaderAnonymousID, anon)
	h.Set(claude_web.HeaderDeviceID, device)
	h.Set("Origin", claudeWebBase(up))
	h.Set("Referer", referer)
	h.Set("User-Agent", claudeWebUA)
	// sec-ch-ua 三件套（W9-2 实抓原值；与 claudeWebUA 同版本同平台）。
	h.Set("sec-ch-ua", `"Chromium";v="155", "Not(A:Brand";v="24"`)
	h.Set("sec-ch-ua-mobile", "?0")
	h.Set("sec-ch-ua-platform", `"Linux"`)
	h.Set("Accept-Language", "en-US,en;q=0.9")
	h.Set("Priority", "u=1, i")
	// Sec-Fetch 三件套（fetch() 同源语义；CWK/AU 同款常量）。
	h.Set("Sec-Fetch-Dest", "empty")
	h.Set("Sec-Fetch-Mode", "cors")
	h.Set("Sec-Fetch-Site", "same-origin")
	if stream {
		h.Set("Accept", "text/event-stream")
	} else {
		h.Set("Accept", "*/*")
	}
	return nil
}

// claudeWebIdentity 凭据 → 账号级稳定身份（uuid5 形态：SHA-1 + RFC 4122 变体/版本位；
// 两个不同命名空间分别派生 anon/device——BK headers.go:20-41 的 uuid5 同义）。
func claudeWebIdentity(seed string) (anonID, deviceID string) {
	return "claudeai.v1." + uuid5("any2api/claude_web/anonymous/", seed),
		uuid5("any2api/claude_web/device/", seed)
}

// uuid5 命名空间 + 名 → uuid5 形态字符串（确定性；输入含凭据 → 输出不可逆）。
func uuid5(namespace, name string) string {
	sum := sha1.Sum([]byte(namespace + name))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x50 // version 5
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC 4122
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// isSafePathSegment URL 路径段守卫：只接受 UUID 形态（十六进制 + `-`，长度
// 8..64）——org/conversation 段绝不接受 `/`、`..`、空白、控制字符、查询分隔符
// （GW 陷阱 13：绝不拼畸形/注入 URL 打给上游）。
func isSafePathSegment(s string) bool {
	if len(s) < 8 || len(s) > 64 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f', c >= 'A' && c <= 'F', c == '-':
		default:
			return false
		}
	}
	return true
}

// isPrintableASCII 可见 ASCII（0x21..0x7e）——cookie 值守卫用。
func isPrintableASCII(s string) bool {
	for i := range len(s) {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// 清理面（W13）：completion 收尾后异步 DELETE 会话（用完即删）
// ---------------------------------------------------------------------------
//
// 依据：docs/architecture.md §清理面（五实现互证）+ W12-1 实锤
// 缺口（每请求泄一条会话，4 请求 4 会话；W10-1 §7.5 记录不修）。KB 惯例：
// `DELETE /api/organizations/{org}/chat_conversations/{conv}`（体 `{uuid}`）
// **用完即删**——BK 3 次重试 + 2s 间隔、仍失败只 warn（claude.go:189-224）；
// YS 后台 goroutine + `ChatDelete` 开关缺省开（handle.go:229-244 / config.go:172
// `os.Getenv("CHAT_DELETE") != "false"`）；CW 两删一睡 + 失败只记（handle.go:169-
// 174）；CWK 200/204 判据（client.py:571）。**清理面必须异步 + 容错，不能挂在
// 响应路径上**（KB §陷阱 8）。
//
// 设计（本实现）：
//   - 触发：forward 在 claudeWebPrepare 成功后 `defer` 排一次清理——completion
//     的任何收尾出口（终态帧 / 流内错误 / 上游失败 / 截断 / 下游断开）都覆盖
//     （泄漏面最大的正是失败/断开形态：W12-1 实锤 429 请求也建了会话）；
//   - 异步：goroutine + 服务级 cwCleanupCtx（与请求 ctx 解耦——请求返回/下游
//     断开绝不掐掉在飞清理）；只 warn，绝不改下游状态码/帧流；
//   - 有界：≤3 次尝试、间隔 2s（可被停机取消打断）、每次尝试 30s 上限（BK 的
//     per-attempt ctx 30s）——goroutine 绝不无限等；停机 ShutdownCleanup 取消 +
//     有界等待；
//   - 开关：上游 `cleanup_conversations`（缺省开；显式 false = 现状行为）。
const (
	// claudeWebCleanupAttempts 删除尝试上限（BK claude.go:189-224 原值 3）。
	claudeWebCleanupAttempts = 3
	// claudeWebCleanupInterval 尝试间隔（BK 原值 2s）。
	claudeWebCleanupInterval = 2 * time.Second
	// claudeWebCleanupTimeout 单次尝试上限（BK 的 per-attempt ctx 30s）。
	claudeWebCleanupTimeout = 30 * time.Second
)

// cleanupConversations 会话清理开关有效值：缺省（未写）**开**（用完即删是五
// 实现惯例）；显式 false 才关。nil 安全。
func (u Upstream) cleanupConversations() bool {
	return u.CleanupConversations == nil || *u.CleanupConversations
}

// scheduleClaudeWebCleanup 排一次异步会话清理（非阻塞——只起 goroutine，绝不
// 挂在响应路径上）。调用方 = forward 在 claudeWebPrepare 成功后的 defer（每次
// 上游尝试各自清理自己创建的会话）。
func (s *Server) scheduleClaudeWebCleanup(idx int, up Upstream, org, conv string) {
	if !up.cleanupConversations() {
		return
	}
	if org == "" || conv == "" {
		// 防御面：目标不完整绝不发（两段都过了 isSafePathSegment，正常不可达）。
		return
	}
	// W53-2 凭据池：DELETE 必须用**创建该会话的凭据**（会话是账号级对象——
	// 换号 DELETE 会 404/403）。池内按 conv 登记了本尝试选定的索引（prepare
	// 成功时 noteConversation）；查得 → 用池内凭据装配本次 DELETE 的 up 副本；
	// 查不到（单凭据/池未初始化）→ 既有 up 原样（up.APIKey）。
	if pool := s.cfg.credentialPoolAt(idx); pool != nil {
		if cred, ok := pool.conversationCredential(conv); ok {
			up.APIKey = cred
		}
	}
	s.cwCleanupWG.Add(1)
	go func() {
		defer s.cwCleanupWG.Done()
		s.runClaudeWebCleanup(idx, up, org, conv)
	}()
}

// runClaudeWebCleanup 删除循环（有界）：≤3 次尝试、间隔 2s（可被停机取消打断）、
// 每次尝试 30s 上限；成功（200/204——BK/CW/YS 同判据）即止；全部失败打一行
// warn（BK `slog.Warn` 同义）。绝不 panic、绝不改下游任何状态。
func (s *Server) runClaudeWebCleanup(idx int, up Upstream, org, conv string) {
	url := claudeWebBase(up) + "/api/organizations/" + org + "/chat_conversations/" + conv
	body, err := json.Marshal(map[string]string{"uuid": conv})
	if err != nil {
		log.Printf("any2api: claude_web: cleanup: marshal delete body: %v", err)
		return
	}
	// referer 按删除阶段：`{base}/chat/{conv}`（BK/CW/YS 三方同形——与 org 发现/
	// 创建/completion 的 `/new` 不同）。
	referer := claudeWebBase(up) + "/chat/" + conv
	ctx := s.cleanupContext()
	interval := s.cwCleanupInterval
	if interval <= 0 {
		interval = claudeWebCleanupInterval
	}
	var lastErr error
	for i := range claudeWebCleanupAttempts {
		if i > 0 {
			select {
			case <-time.After(interval):
			case <-ctx.Done():
				return // 停机：有界收尾，不再重试
			}
		}
		if ctx.Err() != nil {
			return
		}
		attemptCtx, cancel := context.WithTimeout(ctx, claudeWebCleanupTimeout)
		status, _, _, callErr := s.claudeWebCall(attemptCtx, idx, up, http.MethodDelete, url, body, referer)
		cancel()
		switch {
		case callErr == nil && (status == http.StatusOK || status == http.StatusNoContent):
			return // 删除成功
		case callErr != nil:
			lastErr = callErr
		default:
			lastErr = fmt.Errorf("claude_web: delete conversation: upstream status %d", status)
		}
	}
	log.Printf("any2api: claude_web: delete conversation %s failed after %d attempt(s): %v; conversation may remain in account history",
		conv, claudeWebCleanupAttempts, lastErr)
}

// cleanupContext 清理面根 ctx（New 初始化）。直接构造的 Server（仅单元测试形态）
// 无该字段 → 退化为 Background：清理仍与请求 ctx 解耦，行为不变。
func (s *Server) cleanupContext() context.Context {
	if s.cwCleanupCtx == nil {
		return context.Background()
	}
	return s.cwCleanupCtx
}

// ShutdownCleanup 停机收尾（W13）：取消在飞清理并等待其结束（≤timeout）。
// 返回 false = 超时仍有未收尾者——它们各自的尝试仍有 30s 上限且取消即断，
// **绝不无限等**。服务停机在 http.Server.Shutdown 之后调用（main 的接线不在本
// slice 写面；钩子在此提供）。
func (s *Server) ShutdownCleanup(timeout time.Duration) bool {
	if s.cwCleanupStop != nil {
		s.cwCleanupStop()
	}
	// P1-1 停机收尾：关闭持久化层（设计 §2.4-J；main.go 不在本单写面 → 挂既有
	// 停机钩子——这是 main 唯一调用的收尾面）。**有界**：sqlite Close 立即返回，
	// 失败只 log（停机不因此中断，返回值语义仍是「claude_web 在飞清理是否收束」）。
	if err := s.CloseStore(); err != nil {
		log.Printf("any2api: store close: %v", err)
	}
	done := make(chan struct{})
	go func() {
		s.cwCleanupWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// claudeWebTerminalFrame completion 流的终止帧判据（服务壳的读循环用）：真实上游在
// `message_stop` 之后**不主动关闭连接**（W9-2 §2.5 实测：流以客户端 abort 结束），
// 故不能等 EOF——以终止帧收尾。`error` 帧也是终态（HTTP 200 下的流内错误）。
func claudeWebTerminalFrame(event string, data []byte) bool {
	name := strings.TrimSpace(event)
	if name == "" {
		// event: 行被代理丢弃时回退 data.type（与解析器同款宽容）。
		var w struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(bytes.TrimSpace(data), &w) != nil {
			return false
		}
		name = w.Type
	}
	return name == "message_stop" || name == "error"
}

// readClaudeWebCompletion 按帧读到**终止帧**为止（含该帧），返回完整 SSE 字节。
// 非流式下游路径专用（流式下游走 relayStream 的逐帧循环，同样在终止帧收尾）。
//
// 上界 limit：超过即报错（防上游无界输出把内存吃穿）；EOF 早于终止帧 = 截断，
// 也返回已读字节（由 ParseResponse 判「缺 message_stop」显式失败，不在此静默）。
func readClaudeWebCompletion(r io.Reader, limit int) ([]byte, error) {
	br := bufio.NewReaderSize(r, 64*1024)
	var out bytes.Buffer
	var event string
	var data []string
	for {
		line, err := br.ReadString('\n')
		if line != "" {
			if out.Len()+len(line) > limit {
				return out.Bytes(), fmt.Errorf("claude_web: completion exceeds %d bytes", limit)
			}
			out.WriteString(line)
			trimmed := strings.TrimRight(line, "\r\n")
			switch {
			case trimmed == "":
				// 帧结束：终止帧 → 立即收尾（不等 EOF）。
				if claudeWebTerminalFrame(event, []byte(strings.Join(data, "\n"))) {
					return out.Bytes(), nil
				}
				event = ""
				data = data[:0]
			default:
				if v, ok := strings.CutPrefix(trimmed, "event:"); ok {
					event = strings.TrimSpace(v)
				} else if v, ok := strings.CutPrefix(trimmed, "data:"); ok {
					data = append(data, strings.TrimPrefix(v, " "))
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return out.Bytes(), nil
			}
			return out.Bytes(), err
		}
	}
}
