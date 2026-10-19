// chatgpt_web 上游客户端面（W30-2）：匿名 Android 面的三步链（sentinel →
// prepare → f/conversation SSE）+ 头面装配 + 有界尾读 + accessor 消费。
//
// 依据：docs/architecture.md（W6 深挖）+ A2A-L11-W29-1 实抓
// （2026-10-15，零账号；三份 golden 帧 `translate/convert/chatgpt_web/testdata/`）。
//
// 调用链（每个 completion 请求一次；**零账号、零凭据**——匿名面身份 = 客户端头）：
//
//	sentinel：POST {base}/backend-anon/sentinel/chat-requirements  体 `{}`
//	     → 200 {"token":"gAAAAAB…","expire_after":540,"expire_at":<epoch>}
//	     ★单步、无 PoW/turnstile/arkose（W29-1 §1.2/§2.1 实证；两步式属 web 匿名面）
//	     ★W42-2 接线：响应若携带挑战（proofofwork{required,seed,difficulty} /
//	     turnstile{required,dx} / arkose{required,dx}——W29-1 实抓缺席，防御面），
//	     本地求解（sentinel 包两代 PoW；turnstile/arkose 走注入的 Solver 接口，
//	     真求解器不接）→ 提交头 openai-sentinel-proof-token /
//	     openai-sentinel-turnstile-token / openai-sentinel-arkose-token
//	     （prepare 与 completion 两个请求同一份——见 chatgptWebTokens）
//	prepare： POST {base}/backend-anon/f/conversation/prepare  体 = 渲染体
//	     headers: openai-sentinel-chat-requirements-token: <S1 token>;
//	              x-conduit-token: no-token
//	     → 200 {"status":"ok","conduit_token":"<JWT ~350 chars>"}
//	补全：    POST {base}/backend-anon/f/conversation  体 = **同一渲染体**（样本同
//	     字节；prepare 与 conversation 共用）
//	     headers: openai-sentinel-chat-requirements-token: <S1 token>;
//	              x-conduit-token: <conduit_token>
//	     → 200 text/event-stream（SSE；v1 JSON-Patch 档——渲染体协商
//	     `supported_encodings:["v1"]`）
//
// 头面（三请求同一份——W29-1 §2.1 实抓；UA 形状决定响应档：Android UA 无
// `persona`）：User-Agent(ChatGPT/1.2026.237 Android) + OAI-Client-Type: android +
// OAI-Package-Name + OAI-Device-Id（**非空必需**）+ X-OpenAI-No-Http-Logging: 1。
// Device-Id = 配置 `device_id`，空则**每请求现生成 UUID v4**（匿名档无身份可保；
// 规避「同一 oai-did 并发提交 → 403/challenge」的 quirk——KB quirk 8）。
//
// 纪律：
//   - **凭据零物**（匿名面）：无 cookie/无 Bearer；上游令牌（sentinel/conduit）
//     只进头面，绝不进 URL/日志/错误消息（头值守卫：CR/LF/控制字符即拒）；
//   - 上游错误体经 chatgpt_web.ParseError 提取消息（绝不把上游原文透传给下游）；
//   - **prepare 不可省**（KB §陷阱：跳过 prepare 会被判反爬）——三步链是
//     completion 的前置（forward 在发 completion 前调 chatgptWebPrepare）；
//   - **有界尾读**（W32-1 口径修正 + W33-1 裁定收窄）：**definitive marker**
//     （`[DONE]` / error 帧）到达即收束；`message_stream_complete` 与终态
//     （status finished 等）不再即刻收束（W33-1：complete 后仍有
//     [conversation_detail_metadata]/[DONE] 流末 burst——s4 实机距 complete
//     ≈13ms），无流末哨兵时由尾读宽限（`chatgptWebTailGrace` = 3s）兜底——
//     目标是把 last_token/title/ste/complete/detail/[DONE] 尾部帧全收进
//     accessor/观测面（W31-1 实证 250ms 时流式 4/4 截于 last_token）。下游写
//     时机不变（finish+[DONE] 在终态帧即出，W31-1 §4 实证）。见
//     chatgptWebTailGuard 与 readChatGPTWebCompletion；
//   - **S6 出口面（W32-1）**：moderation 拦截（blocked=true）在 relay 循环注入
//     下游错误帧（content_filter）；ToolMessages 下游无载体（裁定+观测渲染见
//     chatgptWebObserve）。
//   - 本 slice **零真机网络**：全部路径经 fake upstream 回放 golden 验证（服务壳
//     测试）；真机发送属另单。
package service

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dwgx/any2api/translate/convert/chatgpt_web"
	"github.com/dwgx/any2api/translate/convert/chatgpt_web/sentinel"
	"github.com/dwgx/any2api/translate/pairs"
)

// ---------------------------------------------------------------------------
// 常量（W29-1 §2.1 实抓原值；头面/路径逐字节对齐样本）
// ---------------------------------------------------------------------------

// chatgptWebUA Android app UA——**W29-1 实抓原值**（curl_cffi impersonate=chrome
// 携带；CF 只查 TLS+UA 面：非浏览器指纹 → 403 challenge，实测零 CF 拦截）。
// UA 形状还决定响应档：Android UA → app 档（无 persona）；Chrome UA → web 档
// （persona:"chatgpt-noauth"）——本客户端固定 app 档。
const chatgptWebUA = "ChatGPT/1.2026.237 (Android 14; sdk_gphone64_arm64; build 2623711)"

// chatgptWebDefaultBaseURL 缺省基址（配置 base_url 为空时用；W29-1 实抓主机）。
const chatgptWebDefaultBaseURL = "https://android.chat.openai.com"

// 匿名 Android 面客户端身份常量（W29-1 §1.2 实抓：`OAI-Device-Id` 非空必需 +
// `OAI-Client-Type`/`OAI-Package-Name` 二者其一；缺 → 401）。
const (
	chatgptWebClientType  = "android"
	chatgptWebPackageName = "com.openai.chatgpt.internal"
)

// 匿名面端点路径（含 `/backend-anon` 前缀；W29-1 §2.1 实抓）。
const (
	chatgptWebSentinelPath   = "/backend-anon/sentinel/chat-requirements"
	chatgptWebPreparePath    = "/backend-anon/f/conversation/prepare"
	chatgptWebCompletionPath = "/backend-anon/f/conversation"
)

// 匿名面请求头名（W29-1 §2.1 实抓）。
const (
	headerOAIClientType    = "OAI-Client-Type"
	headerOAIDeviceID      = "OAI-Device-Id"
	headerOAIPackageName   = "OAI-Package-Name"
	headerOAINoHTTPLogging = "X-OpenAI-No-Http-Logging"
	headerSentinelToken    = "openai-sentinel-chat-requirements-token"
	headerConduitToken     = "x-conduit-token"
)

// sentinel 挑战提交头（W42-2；KB §Sentinel「提交位（4 个头）」：chat_token +
// proof(p) + turnstile(t) + arkose——后三者在 requirements 出题时由求解产出，
// prepare/completion 两个请求同一份）。
const (
	headerSentinelProofToken     = "openai-sentinel-proof-token"
	headerSentinelTurnstileToken = "openai-sentinel-turnstile-token"
	headerSentinelArkoseToken    = "openai-sentinel-arkose-token"
)

// chatgptWebConduitNoToken prepare 阶段的 conduit 占位值（W29-1 §2.1 实抓原值）。
const chatgptWebConduitNoToken = "no-token"

// chatgptWebTailGrace 尾读宽限（W32-1 尾读口径修正：250ms → 3s）。
//
// 语义（CF `STREAM_TAIL_GRACE_MS` 同款，但**只兜底非收束终态**）：终态
// （status finished / end_turn 等，或 W33-1 起的 `message_stream_complete`）
// 已见而 **definitive marker**（`[DONE]` / error 帧）未到时，上游仍握 socket
// （W6-2 实测 1.3–3s 才关流；W31-1 实测尾部静默 ≈0.2–2.3s）——宽限窗口内
// 没有新帧到达即收束，不等连接关闭。definitive marker 一到即收束（不等宽限）。
//
// W33-1 裁定（detail 帧流式面，选项 A）：`message_stream_complete` 移出收束集
// ——complete 之后仍有 [conversation_detail_metadata]/[DONE] 流末 burst（s4
// 实机距 complete ≈13ms）——complete 后无哨兵的变体流由本宽限兜底（3s，与
// 无 marker 流同一条规则）。
//
// 取值裁定（W32-1；读 W31-1 §4 + W6-2 实测）：区间 1.5–3s 取上限 3s——
// 捕获优先（尾部帧 last_token/title/ste/stream_complete/detail/[DONE] 尽量收进
// accessor：W31-1 实证 250ms 时流式 4/4 被截于 last_token）；3s 覆盖 W31-1 观测
// 上限 ≈2.3s + W6-2 记载上限 3s。代价只在「终态后无哨兵且上游静默」的流上
// （连接保持 ≤3s+巡检粒度），**下游写时机不变**：finish+[DONE] 在终态帧即
// 写出（W31-1 §4 实证早于截断 ~310ms），宽限只延长连接保持，不改变下游字节。
const chatgptWebTailGrace = 3 * time.Second

// chatgptWebTokenMaxLen 上游令牌头的长度上界（防御面：sentinel token ~60 字符、
// conduit JWT ~350 字符；超限 = 异常值，显式拒绝）。
const chatgptWebTokenMaxLen = 4096

// chatgptWebModerationMessage moderation 拦截的出口固定消息（S6 出口面，W32-1）。
// 出口裁定：blocked=true 时上游不会给出正文——未提交走真实状态码信封、已提交
// 走流内错误帧（content_filter）；**不合成正文文案**（C 的替换文案是产品内容
// 决策且真机 moderation 流未采样——错误帧是唯一真实载体）。[UNVERIFIED] 面：
// 真机 blocked 帧的时序/形态（报告 §3 S6 出口渲染面）。
const chatgptWebModerationMessage = "upstream content moderation blocked this response"

// chatgptWebDeviceIDMaxLen device_id 长度上界（UUID 形态 36 字符；放宽到 64）。
const chatgptWebDeviceIDMaxLen = 64

// ---------------------------------------------------------------------------
// 基址与目标
// ---------------------------------------------------------------------------

// chatgptWebBase chatgpt_web 上游基址（去尾斜杠；缺省 android.chat.openai.com）。
func chatgptWebBase(up Upstream) string {
	b := strings.TrimSpace(up.BaseURL)
	if b == "" {
		b = chatgptWebDefaultBaseURL
	}
	return strings.TrimRight(b, "/")
}

// chatgptWebTarget 一次 completion 调用的目标面（prepare 链的结果）。
type chatgptWebTarget struct {
	// path completion 路径（`/backend-anon/f/conversation`；静态，经 prepare 链后
	// 由 forward 拼 base）。
	path string
	// sentinelToken 三步链令牌组（sentinel 令牌 + W42-2 挑战求解产物——prepare
	// 与 completion 两个请求共用）。**字段名沿 server.go 的调用点**
	// （`applyChatGPTWebHeaders(req.Header, up, cgTarget.sentinelToken,
	// cgTarget.conduitToken)`——该文件不在 W42-2 写面）；类型 = chatgptWebTokens
	// （见该类型注：结构体形态让 completion 请求也能装配全部四个提交头）。
	sentinelToken chatgptWebTokens
	// conduitToken prepare 返回的 conduit 令牌（completion 请求的头）。
	conduitToken string
}

// chatgptWebTokens 三步链令牌组（W42-2）：sentinel 令牌 + 挑战求解产物
// （proof/turnstile/arkose）。空串 = 该项不存在/无需提交（不设头）。
//
// 为什么是结构体而不是四个平行字段：completion 请求的头面装配在 server.go 的
// 固定调用点 `applyChatGPTWebHeaders(req.Header, up, cgTarget.sentinelToken,
// cgTarget.conduitToken)`（W42-2 写面不含 server.go）——`sentinelToken` 字段
// 类型从 string 升级为令牌组后，completion 请求也能装配全部四个提交头
// （W29-1 的 sentinel 令牌 + W42-2 的 proof/turnstile/arkose），无需改
// server.go（其调用按字段名取值，类型匹配即编译通过）。
type chatgptWebTokens struct {
	sentinel  string // chat-requirements 返回的 chat_token（三请求共用——W29-1 §2.1）。
	proof     string // PoW 求解产物（openai-sentinel-proof-token；两代）。
	turnstile string // turnstile 求解器产物（openai-sentinel-turnstile-token）。
	arkose    string // arkose 求解器产物（openai-sentinel-arkose-token）。
}

// ---------------------------------------------------------------------------
// 挑战求解接线（W42-2）：requirements 响应 → 本地求解 → 提交头
// ---------------------------------------------------------------------------

// chatgptWebClientVersion Android 客户端版本串（2026 代 config 槽 6 的
// client-version/data-build 面）。取 UA 的版本号（与 HTTP 层同源——指纹一致性）；
// 上游对 Android 面不发 `oai-client-version` 头（W29-1 §2.1），该槽无线上对照值
// [UNVERIFIED]，S4 校准时按实抓改。
const chatgptWebClientVersion = "1.2026.237"

// chatgptWebSentinelBuilder 求解器构造注入点（W42-2；缺省 = 2026 代 Builder——
// 客户端 build 串属 2026 代（ChatGPT/1.2026.237）；[UNVERIFIED]：服务端对匿名
// Android 面按哪代出题未实测，两代算法已源码级冻结，见 sentinel 包注 /
// topic §7.6）。返回 Builder 的 Version() 决定求解代际与 config 槽位形状；
// 测试与 S4 真机校准注入两代切换（NewBuilder(V2025/V2026)）与选项
// （WithMaxAttempts 等——失败路径/反证面）。
var chatgptWebSentinelBuilder = func() (sentinel.Builder, error) {
	return sentinel.NewBuilder(sentinel.V2026)
}

// chatgptWebTurnstileSolver / chatgptWebArkoseSolver 第三方求解器注入点
// （turnstile/arkose 出题时用；调用形态照 chat2api ChatService.py:207-243）。
// 缺省 nil = 未接线——**真求解器不接**（零真机红线）：出题而求解器缺失/失败 =
// 显式错误面（预提交失败；绝不静默跳过挑战继续打上游）。测试注入
// sentinel.FakeSolver。
var (
	chatgptWebTurnstileSolver sentinel.TurnstileSolver
	chatgptWebArkoseSolver    sentinel.ArkoseSolver
)

// chatgptWebRequirements 是 chat-requirements 响应里的令牌与挑战清单（W42-2）。
// 挑战字段在 Android 匿名面 W29-1 实抓里缺席（单步、无 PoW/turnstile）——按需
// 出现；形态照 chat2api ChatService.py:183-260 / KB §Sentinel 端点表
// （{token, persona, proofofwork{required,seed,difficulty}, turnstile{required,dx},
// arkose{required,dx}}）。
type chatgptWebRequirements struct {
	// Token chat-requirements 令牌（chat_token；后续请求的
	// openai-sentinel-chat-requirements-token）。
	Token string `json:"token"`
	// Persona 身份档（arkose 求解 method 选择用——ArkoseMethod）。
	Persona string `json:"persona"`
	// ProofOfWork 服务端下发的 PoW 挑战（required 时本地求解 → proof token）。
	ProofOfWork *chatgptWebPowChallenge `json:"proofofwork"`
	// Turnstile / Arkose 第三方求解器挑战（required 时走注入的 Solver）。
	Turnstile *chatgptWebDXChallenge `json:"turnstile"`
	Arkose    *chatgptWebDXChallenge `json:"arkose"`
}

// chatgptWebPowChallenge 是 proofofwork 挑战（两代同形——seed/difficulty 由
// 服务端下发，算法与槽位按求解代际分派）。
type chatgptWebPowChallenge struct {
	Required   bool   `json:"required"`
	Seed       string `json:"seed"`
	Difficulty string `json:"difficulty"`
}

// chatgptWebDXChallenge 是 turnstile/arkose 挑战（两形同为 required+dx）。
type chatgptWebDXChallenge struct {
	Required bool   `json:"required"`
	DX       string `json:"dx"`
}

// chatgptWebSentinelConfig 构造求解代际对应的 config（两代槽位形状不同：
// 2025 = 18 槽 / 2026 = 25 槽——sentinel 包 NewConfig2025/NewConfig2026 镜像
// 参考实现）。UA 与 HTTP 层同源（指纹一致性——Config 文档纪律）。浏览器面槽位
// （scripts/navigator/window keys）与 Android 面对照值缺失——保持空/零
// [UNVERIFIED]，S4 校准时按实抓补。
func chatgptWebSentinelConfig(v sentinel.Version) sentinel.Config {
	switch v {
	case sentinel.V2025:
		return sentinel.NewConfig2025(sentinel.Config2025Params{UserAgent: chatgptWebUA})
	default:
		return sentinel.NewConfig2026(sentinel.Config2026Params{
			UserAgent:     chatgptWebUA,
			ClientVersion: chatgptWebClientVersion,
		})
	}
}

// chatgptWebSolveRequirements 求解 requirements 响应里的挑战（W42-2 接线点），
// 返回要提交的令牌组：
//
//	proofofwork.required → chatgptWebSentinelBuilder 构造的 Builder 本地求解
//	    （两代——config 形状按 Builder.Version()；求解失败 = ErrUnsolved 显式
//	    错误面，绝不带伪解兜底上线——sentinel 包 WithLegacyFallback 默认关）；
//	turnstile.required   → chatgptWebTurnstileSolver（未接线/失败 = 显式错误）；
//	arkose.required      → chatgptWebArkoseSolver（method 按 persona——
//	    ArkoseMethod：freeaccount→chat35，其余→chat4）。
//
// 无挑战 = 空令牌组（原样面：一个挑战头都不设——绝不发明头）。错误消息绝不
// 回显求解产物；token 值经 applyChatGPTWebHeaders 的头值守卫后才上头。
func chatgptWebSolveRequirements(ctx context.Context, reqs chatgptWebRequirements) (chatgptWebTokens, error) {
	tokens := chatgptWebTokens{sentinel: reqs.Token}
	if p := reqs.ProofOfWork; p != nil && p.Required {
		b, err := chatgptWebSentinelBuilder()
		if err != nil {
			return chatgptWebTokens{}, fmt.Errorf("chatgpt_web: sentinel: build proof-of-work solver: %w", err)
		}
		cfg := chatgptWebSentinelConfig(b.Version())
		proof, err := b.ProofToken(cfg, p.Seed, p.Difficulty)
		if err != nil {
			return chatgptWebTokens{}, fmt.Errorf("chatgpt_web: sentinel: proof-of-work unsolved (%s): %w", b.Version(), err)
		}
		tokens.proof = proof
	}
	if ts := reqs.Turnstile; ts != nil && ts.Required {
		if chatgptWebTurnstileSolver == nil {
			return chatgptWebTokens{}, errors.New("chatgpt_web: sentinel: turnstile required but no solver is wired")
		}
		// P = ""：本面单步空体、无客户端 p 令牌（W29-1 §1.2 / chatgpt2api-cf
		// client.ts:154-172 双源实证）；turb-gpt 的 dx-p 绑定属其 web 面流程，
		// 本面提交值 [UNVERIFIED]（S4 校准时按实抓定）。
		tok, err := chatgptWebTurnstileSolver.SolveTurnstile(ctx, sentinel.TurnstileRequest{
			URL: sentinel.DefaultTurnstileURL,
			P:   "",
			DX:  ts.DX,
			UA:  chatgptWebUA,
		})
		if err != nil {
			return chatgptWebTokens{}, fmt.Errorf("chatgpt_web: sentinel: turnstile solve failed: %w", err)
		}
		if tok == "" {
			return chatgptWebTokens{}, errors.New("chatgpt_web: sentinel: turnstile solver returned an empty token")
		}
		tokens.turnstile = tok
	}
	if a := reqs.Arkose; a != nil && a.Required {
		if chatgptWebArkoseSolver == nil {
			return chatgptWebTokens{}, errors.New("chatgpt_web: sentinel: arkose required but no solver is wired")
		}
		tok, err := chatgptWebArkoseSolver.SolveArkose(ctx, sentinel.ArkoseRequest{
			Blob:   a.DX,
			Method: sentinel.ArkoseMethod(reqs.Persona),
		})
		if err != nil {
			return chatgptWebTokens{}, fmt.Errorf("chatgpt_web: sentinel: arkose solve failed: %w", err)
		}
		if tok == "" {
			return chatgptWebTokens{}, errors.New("chatgpt_web: sentinel: arkose solver returned an empty token")
		}
		tokens.arkose = tok
	}
	return tokens, nil
}

// ---------------------------------------------------------------------------
// 三步链：sentinel → prepare → completion（completion 由 forward 主体发）
// ---------------------------------------------------------------------------

// chatgptWebPrepare 会话准备（sentinel → prepare），返回 completion 目标。
//
// 失败 = 预提交失败（下游未提交）——调用方按 C1 分类决定换上游；本函数只负责
// 显式报错（错误消息不含令牌值）。idx = 上游下标（出站客户端按上游选）。body =
// 渲染面产出的 anon 体（prepare 与 completion **同体**——W29-1 样本同字节）。
//
// W42-2：sentinel 响应里的挑战（proofofwork/turnstile/arkose——W29-1 实抓缺席，
// 防御面）经 chatgptWebSolveRequirements 求解，产物与 sentinel 令牌一起进
// prepare/completion 两个请求的提交头（chatgptWebTokens）。
func (s *Server) chatgptWebPrepare(ctx context.Context, idx int, up Upstream, body []byte) (chatgptWebTarget, error) {
	if len(body) == 0 {
		return chatgptWebTarget{}, errors.New("chatgpt_web: prepare requires a rendered request body")
	}
	base := chatgptWebBase(up)
	// 1) sentinel（空体 `{}`；单步——W29-1 §1.2 实证：Android 面响应无
	// PoW/turnstile；有挑战时按 W42-2 求解）。
	status, respBody, err := s.chatgptWebCall(ctx, idx, up, base+chatgptWebSentinelPath, []byte("{}"), chatgptWebTokens{}, "")
	if err != nil {
		return chatgptWebTarget{}, err
	}
	if status != http.StatusOK {
		return chatgptWebTarget{}, chatgptWebStatusError("sentinel chat-requirements", status, respBody)
	}
	var reqs chatgptWebRequirements
	if err := json.Unmarshal(respBody, &reqs); err != nil {
		return chatgptWebTarget{}, fmt.Errorf("chatgpt_web: sentinel response is not JSON: %w", err)
	}
	if err := chatgptWebTokenGuard("sentinel token", reqs.Token); err != nil {
		return chatgptWebTarget{}, err
	}
	// 1b) 挑战求解（W42-2；无挑战 = 原样：空令牌组，不设挑战头）。
	tokens, err := chatgptWebSolveRequirements(ctx, reqs)
	if err != nil {
		return chatgptWebTarget{}, err
	}
	// 2) prepare（体 = 渲染体；`x-conduit-token: no-token` 占位——W29-1 §2.1）。
	status, respBody, err = s.chatgptWebCall(ctx, idx, up, base+chatgptWebPreparePath, body, tokens, chatgptWebConduitNoToken)
	if err != nil {
		return chatgptWebTarget{}, err
	}
	if status != http.StatusOK {
		return chatgptWebTarget{}, chatgptWebStatusError("f/conversation/prepare", status, respBody)
	}
	var prep struct {
		Status       string `json:"status"`
		ConduitToken string `json:"conduit_token"`
	}
	if err := json.Unmarshal(respBody, &prep); err != nil {
		return chatgptWebTarget{}, fmt.Errorf("chatgpt_web: prepare response is not JSON: %w", err)
	}
	if err := chatgptWebTokenGuard("conduit token", prep.ConduitToken); err != nil {
		return chatgptWebTarget{}, err
	}
	if st := strings.TrimSpace(prep.Status); st != "" && st != "ok" {
		return chatgptWebTarget{}, fmt.Errorf("chatgpt_web: prepare returned status %q (want ok)", st)
	}
	return chatgptWebTarget{
		path:          chatgptWebCompletionPath,
		sentinelToken: tokens,
		conduitToken:  prep.ConduitToken,
	}, nil
}

// chatgptWebStatusError 非 2xx → 显式错误（消息经 chatgpt_web.ParseError 提取；
// 上游原文只进 Raw 供日志，绝不透传下游；错误消息不含令牌值）。
func chatgptWebStatusError(stage string, status int, body []byte) error {
	e := chatGPTWebParseError(body)
	hint := ""
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		hint = " (device headers rejected or Cloudflare-blocked)"
	case http.StatusNotFound:
		hint = " (model unavailable on the anon tier)"
	case http.StatusTooManyRequests:
		hint = " (rate limited)"
	}
	return fmt.Errorf("chatgpt_web: %s: upstream status %d%s: %s", stage, status, hint, e)
}

// chatgptWebCall 发一次匿名面请求（sentinel / prepare；completion 由 forward 主体
// 发）。返回状态码与响应体（上限 maxErrorBodyBytes——两个端点都是小体）。
// tokens 的空字段/conduitToken 为空时不设对应头（sentinel 阶段全空）。
func (s *Server) chatgptWebCall(ctx context.Context, idx int, up Upstream, url string, body []byte, tokens chatgptWebTokens, conduitToken string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, fmt.Errorf("chatgpt_web: build request: %w", err)
	}
	if err := applyChatGPTWebHeaders(req.Header, up, tokens, conduitToken); err != nil {
		return 0, nil, err
	}
	client, err := s.clientFor(idx)
	if err != nil {
		return 0, nil, fmt.Errorf("chatgpt_web: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("chatgpt_web: POST %s: %w", url, err)
	}
	defer resp.Body.Close()
	rb, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("chatgpt_web: read response: %w", err)
	}
	return resp.StatusCode, rb, nil
}

// applyChatGPTWebHeaders 装配匿名 Android 面头面（W29-1 §2.1 实抓逐头；三请求
// 同一份——sentinel/prepare/completion 共用，与实抓的 H() 同形）。
//
// W42-2：提交头扩到四件套——sentinel 令牌 + 挑战求解产物（proof/turnstile/
// arkose；无挑战时为空串 = 不设头）。
//
// 守卫（头注入面）：各令牌非空时必须是无 CR/LF/控制字符的值（上游值与求解产物
// 同样不信任——绝不把畸形值拼进头）；device_id 同守卫（配置期 Validate 已挡
// 一层，运行期再挡一层）。Content-Type: application/json + Accept:
// text/event-stream 三请求同值（实抓如此——sentinel/prepare 也带 SSE Accept）。
func applyChatGPTWebHeaders(h http.Header, up Upstream, tokens chatgptWebTokens, conduitToken string) error {
	deviceID, err := chatgptWebDeviceID(up)
	if err != nil {
		return err
	}
	if tokens.sentinel != "" {
		if err := chatgptWebTokenGuard("sentinel token", tokens.sentinel); err != nil {
			return err
		}
		h.Set(headerSentinelToken, tokens.sentinel)
	}
	if tokens.proof != "" {
		if err := chatgptWebTokenGuard("proof token", tokens.proof); err != nil {
			return err
		}
		h.Set(headerSentinelProofToken, tokens.proof)
	}
	if tokens.turnstile != "" {
		if err := chatgptWebTokenGuard("turnstile token", tokens.turnstile); err != nil {
			return err
		}
		h.Set(headerSentinelTurnstileToken, tokens.turnstile)
	}
	if tokens.arkose != "" {
		if err := chatgptWebTokenGuard("arkose token", tokens.arkose); err != nil {
			return err
		}
		h.Set(headerSentinelArkoseToken, tokens.arkose)
	}
	if conduitToken != "" {
		if err := chatgptWebTokenGuard("conduit token", conduitToken); err != nil {
			return err
		}
		h.Set(headerConduitToken, conduitToken)
	}
	h.Set("User-Agent", chatgptWebUA)
	h.Set("Content-Type", "application/json")
	h.Set("Accept", "text/event-stream")
	h.Set(headerOAIClientType, chatgptWebClientType)
	h.Set(headerOAIDeviceID, deviceID)
	h.Set(headerOAIPackageName, chatgptWebPackageName)
	h.Set(headerOAINoHTTPLogging, "1")
	return nil
}

// chatgptWebDeviceID 本请求的 device id：配置 `device_id` 非空 → 用之；空 →
// 现生成随机 UUID v4（匿名档无身份可保；逐请求新设备规避同 did 并发 quirk）。
func chatgptWebDeviceID(up Upstream) (string, error) {
	if id := strings.TrimSpace(up.DeviceID); id != "" {
		if err := validateChatGPTWebDeviceID(id); err != nil {
			return "", err
		}
		return id, nil
	}
	id, err := newUUIDv4()
	if err != nil {
		return "", fmt.Errorf("chatgpt_web: generate device id: %w", err)
	}
	return id, nil
}

// validateChatGPTWebDeviceID device_id 形态守卫：非空、≤64 字节、可见 ASCII
// （0x21..0x7e——无空白/控制字符；头值注入面）。配置期（Validate）与运行期
// （装配）共用。
func validateChatGPTWebDeviceID(id string) error {
	if id == "" {
		return errors.New("device_id is empty")
	}
	if len(id) > chatgptWebDeviceIDMaxLen {
		return fmt.Errorf("device_id is %d bytes (limit %d)", len(id), chatgptWebDeviceIDMaxLen)
	}
	if !isPrintableASCII(id) {
		return errors.New("device_id contains characters not allowed in a header value")
	}
	return nil
}

// chatgptWebTokenGuard 上游令牌头值守卫：非空、≤chatgptWebTokenMaxLen、无
// CR/LF/其它控制字符（头注入面——上游响应值同样不信任）。
func chatgptWebTokenGuard(name, v string) error {
	if v == "" {
		return fmt.Errorf("chatgpt_web: %s is empty", name)
	}
	if len(v) > chatgptWebTokenMaxLen {
		return fmt.Errorf("chatgpt_web: %s is %d bytes (limit %d)", name, len(v), chatgptWebTokenMaxLen)
	}
	for i := range len(v) {
		if c := v[i]; c < 0x21 || c > 0x7e {
			return fmt.Errorf("chatgpt_web: %s contains characters not allowed in a header value", name)
		}
	}
	return nil
}

// chatGPTWebParseError 上游错误体 → 消息文本（chatgpt_web.ParseError 的信封；
// 空消息退化为定长摘要——绝不把整段上游原文透传）。
func chatGPTWebParseError(body []byte) string {
	if e := chatgpt_web.ParseError(body); e != nil && e.Message != "" {
		return e.Message
	}
	return "upstream error"
}

// newUUIDv4 UUID v4（crypto/rand；无第三方依赖——device id 形态 = 实抓 uuid4）。
func newUUIDv4() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC 4122
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32], nil
}

// ---------------------------------------------------------------------------
// 有界尾读（终止帧收束 + 尾读宽限）
// ---------------------------------------------------------------------------

// chatgptWebTerminalFrame 流的终止帧判据（raw 面）：`[DONE]` 哨兵（判据④；三份
// golden 的末帧），或流内 error 帧（HTTP 200 下的终态错误——解析器同判据）。
//
// W32-1 收束口径（W33-1 收窄）：本函数是 streaming 收束的 **raw 快速判据**
// （[DONE]/error）；引号/内嵌 [DONE] 形态在解析器面登记（DefinitiveMarker——
// relay 循环两判据并列；W33-1 起三形态全部可达）。`message_stream_complete`
// **不**收束（W33-1 裁定：complete 之后仍有 [conversation_detail_metadata]/
// [DONE] 流末 burst——流式面随流末哨兵收束，尾覆盖与非流式路径对齐）。非流式
// 路径（readChatGPTWebCompletion）按本判据读到 [DONE]/error——两条路径的收束点
// 一致（读全尾含 conversation_detail_metadata；W31-1 实测 2/2 全尾）。
//
// **尾部帧不是 raw 终止帧**：`title_generation` / `server_ste_metadata` /
// `conversation_detail_metadata` / `last_token` marker / `message_stream_complete`
// 在 end_turn 后仍会到达（解析器容忍并记录——标题/model/详情/尾部帧回读面），
// 由流末哨兵或尾读宽限窗口收束，绝不提前砍掉它们。
func chatgptWebTerminalFrame(event string, data []byte) bool {
	_ = event // chatgpt 流的 event: 行只有 delta_encoding——终止判据只看载荷
	d := bytes.TrimSpace(data)
	if string(d) == "[DONE]" {
		return true
	}
	if len(d) == 0 || d[0] != '{' {
		return false
	}
	var probe struct {
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(d, &probe) != nil {
		return false
	}
	e := bytes.TrimSpace(probe.Error)
	return len(e) > 0 && string(e) != "null"
}

// readChatGPTWebCompletion 按帧读到**终止帧**为止（含该帧），返回完整 SSE 字节。
// 非流式下游路径专用（流式下游走 relayStream 的逐帧循环，同样在终止帧收束 +
// 尾读宽限）。
//
// 上界 limit：超过即报错（防上游无界输出把内存吃穿）；EOF 早于终止帧 = 截断，
// 也返回已读字节（由 ParseResponse 判「缺终止信号」显式失败，不在此静默）。
// 读到终止帧即停（不等 socket 关闭——上游在终态后握 socket 1.3–3s，W6-2 实测）。
func readChatGPTWebCompletion(r io.Reader, limit int) ([]byte, error) {
	br := bufio.NewReaderSize(r, 64*1024)
	var out bytes.Buffer
	var event string
	var data []string
	for {
		line, err := br.ReadString('\n')
		if line != "" {
			if out.Len()+len(line) > limit {
				return out.Bytes(), fmt.Errorf("chatgpt_web: completion exceeds %d bytes", limit)
			}
			out.WriteString(line)
			trimmed := strings.TrimRight(line, "\r\n")
			switch {
			case trimmed == "":
				// 帧结束：终止帧 → 立即收尾（不等 EOF）。
				if chatgptWebTerminalFrame(event, []byte(strings.Join(data, "\n"))) {
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

// chatgptWebTailGuard 尾读宽限守卫（CF `STREAM_TAIL_GRACE_MS` 同款语义；W32-1
// 口径修正：**只兜底非收束终态**）：终态（status finished 等，或 W33-1 起的
// message_stream_complete）已见而 definitive marker（[DONE]/error）未到时，
// 上游仍握 socket（实测 1.3–3s）——宽限窗口（终态后连续 timeout 没有新帧）
// 到期即取消本次尝试 ctx（上游连接随之中止），读循环把「取消 + 宽限已触发」
// 当**正常流尾**收束（不记失败、不写错误帧；收尾补帧走循环后的 pipe.Finish()）。
// definitive marker 到达则由读循环直接收束（不等宽限——见 relayStream 的
// 收束判据）。
//
// 武装/刷新 = 终态后的每一帧（idle-based window——CF 原文「reset AFTER emission」：
// 窗口是「自最后一帧起的静默时长」，不是绝对时刻）。终态前恒不武装（nil 语义 =
// 未触发）。与 upstreamIdleGuard 的区别：本守卫**只**覆盖终态后的尾读窗口（恒
// 开、不依赖配置），idle 守卫覆盖整个尝试（缺省关、假活跃检测）。
type chatgptWebTailGuard struct {
	timeout  time.Duration
	deadline atomic.Int64 // 宽限截止（unix nano；0 = 未武装）
	fired    atomic.Bool
	stop     func()
}

// newChatGPTWebTailGuard 启动守卫；timeout<=0 → nil（关——调用方 nil 安全）。
// cancel = 本次上游尝试的 ctx 取消函数（宽限到期即取消上游请求）；ctx = 本尝试
// ctx（尝试收尾即退出巡检 goroutine，绝不在请求返回后误触发）。
func newChatGPTWebTailGuard(ctx context.Context, cancel context.CancelFunc, timeout time.Duration) *chatgptWebTailGuard {
	if timeout <= 0 {
		return nil
	}
	g := &chatgptWebTailGuard{timeout: timeout}
	done := make(chan struct{})
	var once sync.Once
	g.stop = func() { once.Do(func() { close(done) }) }
	// 巡检间隔 = timeout/4（下限 25ms、上限 250ms）：够准且不空转。上限从 1s
	// 收紧到 250ms（W32-1）：3s 宽限下 timeout/4 = 750ms 会让「到期 → 实际
	// 收束」延迟膨胀到 ~750ms（实际界 3.75s）；250ms 上限把实际界压到
	// grace+250ms（W31-1 pcap 实测的 312ms = 250ms 宽限 + 62ms 巡检，同款
	// 关系）。
	interval := timeout / 4
	if interval > 250*time.Millisecond {
		interval = 250 * time.Millisecond
	}
	if interval < 25*time.Millisecond {
		interval = 25 * time.Millisecond
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				d := g.deadline.Load()
				if d > 0 && time.Now().UnixNano() >= d {
					g.fired.Store(true)
					cancel()
					return
				}
			}
		}
	}()
	return g
}

// arm 武装/刷新宽限窗口（终态后的每一帧调用——CF「idle-based window」同款）。
// nil 安全。
func (g *chatgptWebTailGuard) arm() {
	if g == nil {
		return
	}
	g.deadline.Store(time.Now().Add(g.timeout).UnixNano())
}

// TimedOut 宽限是否已触发（nil 安全——未武装/未触发/守卫为 nil 都返回 false）。
func (g *chatgptWebTailGuard) TimedOut() bool { return g != nil && g.fired.Load() }

// stopGuard 停表（nil 安全）。
func (g *chatgptWebTailGuard) stopGuard() {
	if g != nil {
		g.stop()
	}
}

// ---------------------------------------------------------------------------
// accessor 消费（W28-1 §8 清单；收尾点观测——纯诊断，绝不进控制流）
// ---------------------------------------------------------------------------

// chatgptWebObserve 收尾点的 accessor 消费（W28-1 §8 清单逐条落地）：
//
//	Terminal       → 尾读宽限计时（relay 循环内消费；本函数不重复）
//	DefinitiveMarker/StreamCompleteSeen/DoneSentinelSeen → 日志（收束判据命中面：
//	                 哪类 definitive marker 收束了本流——W32-1）
//	Markers/MarkerOverflow → 日志（尾部帧登记：last_token 等——截断点证据面）
//	Title          → 日志（不下发下游——IR/出口无标题载体）
//	ResumeToken    → **不下发**（JWT 里有 turn_topic_id；只记长度，绝不记值）
//	ConversationID → 日志（诊断关联）
//	ObservedModel  → 日志（请求 auto 时响应面模型回写的来源——IR start 已带）
//	ObservedMetadata → 只记字节数（保真旁路；IR 无载体）
//	ConversationDetailMetadata → 只记在场/字节数（W33-1 流式面裁定后可见：
//	                 收束于 [DONE]，detail 随流末 burst 捕获；保真旁路）
//	ToolMessages/ToolOverflow → 日志（S6 出口观测渲染：记录摘要 + 溢出即 warn；
//	                 下游无载体——见下方裁定注）
//	Moderation     → 命中即 warn（出口错误帧在 relay 循环注入；本函数只记记录面）
//	Handoff        → 命中即 warn（续接面未接线——无 resume_sse_endpoint 时按 A
//	                 语义属错误面；S4 求解器/续接单）
//	SawLiveMessage → false + 有帧 = 假成功面候选（C head_process_response 同判据）
//	ProtocolVersion/HistoryDropped → 日志（诊断）
//
// S6 出口渲染裁定（W32-1）：ToolMessages 的下游（下游客户端可见字节）**无载体**
// ——抑制是解析判据（A/B/Go/gpt2api 四实现同向），把记录混进正文 = 污染；C 的
// 引用块渲染是产品决策且真机工具帧未采样 [UNVERIFIED]。故可行部分 = 出口观测
// 渲染（本函数）：记录摘要（recipient/role 分桶 + 图片数）进日志行，溢出显式
// warn——S6 深研/附件切片校准时直接读这行。
//
// 只读观测：绝不改下游字节、绝不进控制流（nil 安全——入口非 chatgpt_web 时）。
func chatgptWebObserve(ob pairs.ChatGPTWebEntry, up Upstream) {
	if ob == nil {
		return
	}
	log.Printf("any2api: chatgpt_web upstream %s: stream end: model=%q title=%q conv=%s proto=%q history_dropped=%d tool_records=%d%s tool_overflow=%d resume_token=%s metadata=%dB markers=%s marker_overflow=%d stream_complete=%v done=%v detail=%s",
		up.Name, ob.ObservedModel(), ob.Title(), logSafeID(ob.ConversationID()), ob.ProtocolVersion(),
		ob.HistoryDropped(), len(ob.ToolMessages()), toolRecordsSummary(ob.ToolMessages()), ob.ToolOverflow(),
		presenceLen(ob.ResumeToken()), len(ob.ObservedMetadata()),
		markerNames(ob.Markers()), ob.MarkerOverflow(), ob.StreamCompleteSeen(), ob.DoneSentinelSeen(),
		presenceLen(string(ob.ConversationDetailMetadata())))
	if n := ob.ToolOverflow(); n > 0 {
		// 记录面丢条（超 maxToolRecords 上限）：S6 校准前先看见——丢条 = 工具/
		// 图片记录不完整（防内存无界的代价，显式暴露不静默）。
		log.Printf("any2api: chatgpt_web upstream %s: tool record face dropped %d entries (cap reached)", up.Name, n)
	}
	if h := ob.Handoff(); h != nil {
		log.Printf("any2api: chatgpt_web upstream %s: stream_handoff received (ws_topic=%q sse_topic=%q): resumption is not wired (S4); downstream closed after the first segment",
			up.Name, h.WSTopicID, h.SSETopicID)
	}
	if m := ob.Moderation(); m != nil {
		log.Printf("any2api: chatgpt_web upstream %s: moderation event (blocked=%v); blocked pre-terminal frames are emitted downstream as content_filter errors (relay loop), this line is the record face",
			up.Name, m.Blocked)
	}
	if !ob.SawLiveMessage() {
		// 假成功面候选（C head_process_response:111-124：一路没有 in_progress 就当
		// 403 unusual activity）——本包不合成错误（W28-1 §2.6），只如实记录。
		log.Printf("any2api: chatgpt_web upstream %s: no live assistant message observed (fake-success candidate)", up.Name)
	}
}

// toolRecordsSummary 记录面摘要（S6 出口观测渲染的可行部分）：抑制原因分桶计数
// + 图片指针总数——如 "(role:system×5)" / "(python×1,images=2)"；无记录 = ""。
// 分桶键：recipient 抑制 → recipient 值（python/browser…）；role 抑制 →
// "role:<role>"（"all" 作键会误导——recipient=all 的系统消息实为 role 抑制）；
// 其余（commentary/live-multimodal）→ 原因字面量。
func toolRecordsSummary(recs []chatgpt_web.ToolMessage) string {
	if len(recs) == 0 {
		return ""
	}
	counts := map[string]int{}
	var order []string
	images := 0
	for _, r := range recs {
		key := r.Reason
		switch {
		case key == "recipient" && r.Recipient != "":
			key = r.Recipient
		case key == "role" && r.Role != "":
			key = "role:" + r.Role
		case key == "":
			key = r.Recipient
			if key == "" {
				key = r.Role
			}
			if key == "" {
				key = "unknown"
			}
		}
		if counts[key] == 0 {
			order = append(order, key)
		}
		counts[key]++
		images += len(r.Images)
	}
	parts := make([]string, 0, len(order)+1)
	for _, k := range order {
		parts = append(parts, fmt.Sprintf("%s×%d", k, counts[k]))
	}
	if images > 0 {
		parts = append(parts, fmt.Sprintf("images=%d", images))
	}
	return "(" + strings.Join(parts, ",") + ")"
}

// markerNames marker 登记的名字序列（日志面；空 = "-"）。绝不回显 MessageID。
func markerNames(ms []chatgpt_web.MarkerInfo) string {
	if len(ms) == 0 {
		return "-"
	}
	names := make([]string, 0, len(ms))
	for _, m := range ms {
		if m.Marker == "" {
			names = append(names, "?")
			continue
		}
		names = append(names, m.Marker)
	}
	return strings.Join(names, ",")
}

// logSafeID 日志安全的标识片段（诊断字段原样打印——不截断不掩码：会话 id 无凭据
// 面；空值显示 "-"）。
func logSafeID(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

// presenceLen 值在场时的长度描述（**绝不回显令牌值**——JWT 里有 turn_topic_id）。
func presenceLen(v string) string {
	if v == "" {
		return "absent"
	}
	return fmt.Sprintf("present(%dB)", len(v))
}
