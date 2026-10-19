// Package service 实现 Any2API 的薄服务壳：HTTP 入口（OpenAI 面 + Anthropic 面
// + Responses 面 /v1/responses）→ 经 pairs 转换（按上游协议）→ 转发配置的上游
// → 流式 relay 回客户端。
//
// 上游协议面：anthropic（/v1/messages + x-api-key）、openai_chat
// （/v1/chat/completions + Bearer）、gemini（模型名在 URL 路径里
// `/v1beta/models/{m}:generateContent`，流式 `:streamGenerateContent?alt=sse`，
// 认证头 `x-goog-api-key`——gemini-conversion-table §格式兼容面）、
// responses（/v1/responses + Bearer；模型在请求体里——openai-responses 表
// 「请求面」：`model` 是 body 字段，路径固定）、
// interactions（/v1beta/interactions + `x-goog-api-key`；模型名与 stream 开关都在
// 请求体里，路径固定；`Api-Revision` 版本开关头——interactions-wire §格式兼容面）。
//
// 依据：docs/architecture.md（形态=内核库+薄服务；YAML 起步）；
// research/topics/new-api/README.md（stream_scanner 三 goroutine 通路 + 提交点
// 纪律的范式来源）；research/topics/contract-exec/README.md（下游消费硬门：
// 缺 stop_reason 条件、空参 `{}`、未知事件忽略）；research/topics/stream-mechanics
// （分帧/心跳/断流）；docs/architecture.md
// （§错误信封、流式 alt=sse 节）。
//
// 纪律：不碰真实上游（测试只连本地回环）；凭据值不入产物（示例配置用占位符）。
package service

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/dwgx/any2api/translate/registry"
)

// Protocol 上游线格式（config 里的人类可读名）。
type Protocol string

const (
	// ProtocolAnthropic Anthropic Messages（POST /v1/messages）。
	ProtocolAnthropic Protocol = "anthropic"
	// ProtocolOpenAIChat OpenAI Chat Completions（POST /v1/chat/completions）。
	ProtocolOpenAIChat Protocol = "openai_chat"
	// ProtocolGemini Gemini generateContent（POST /v1beta/models/{m}:generateContent；
	// 流式 :streamGenerateContent?alt=sse）——模型名在 URL 路径里，不在请求体里。
	ProtocolGemini Protocol = "gemini"
	// ProtocolResponses OpenAI Responses（POST /v1/responses + Bearer）——
	// items 制线格式；模型名与 stream 开关都在请求体里，路径固定
	//（openai-responses 表 §请求面）。
	ProtocolResponses Protocol = "responses"
	// ProtocolInteractions Gemini Interactions（POST /v1beta/interactions）——
	// steps 时间线制第 5 格式；模型名与 stream 开关都在请求体里，路径固定；
	// 认证头 x-goog-api-key，版本开关头 Api-Revision（interactions-wire
	// §格式兼容面；W5-13 转换对已注册）。
	ProtocolInteractions Protocol = "interactions"
	// ProtocolClaudeWeb claude.ai 网页线（W10 新增，上游叶子格式）——
	// 调用链：org 发现（`GET /api/organizations`，或配置 org_id 跳过）→
	// conversation 创建（`POST /api/organizations/{org}/chat_conversations`）→
	// completion 流式（`POST .../chat_conversations/{conv}/completion`，SSE）。
	// api_key = **sessionKey**（`sk-ant-sid0*`，以 cookie 形态携带；见
	// claude_web.go 的头面常量）；路径两段动态 → PathFor 不适用（forward 前置
	// 会话准备）。依据：docs/architecture.md（W9-1）+ W9-2 实抓。
	ProtocolClaudeWeb Protocol = "claude_web"
	// ProtocolChatGPTWeb chatgpt.com 网页线（W30-2 新增，上游叶子格式；**匿名
	// Android 面**）——调用链：sentinel（`POST /backend-anon/sentinel/
	// chat-requirements`，空体 `{}`、单步、无 PoW/turnstile）→ prepare
	// （`POST /backend-anon/f/conversation/prepare`，体 = 渲染体、头
	// `x-conduit-token: no-token`）→ completion SSE（`POST /backend-anon/
	// f/conversation`，体同 prepare、头 `x-conduit-token: <conduit>`）。
	// **无凭据**（匿名面）：身份 = 客户端头 `OAI-Device-Id`（配置 device_id 或
	// 每请求现生成）+ UA(Android)/Client-Type/Package（见 chatgpt_web.go 头面
	// 常量）。路径含 `/backend-anon` 前缀且 prepare 链必须先跑 → PathFor 不适用
	// （forward 前置 prepare 链）。依据：docs/architecture.md
	// （W6）+ W29-1 实抓（三 golden 帧）。
	ProtocolChatGPTWeb Protocol = "chatgpt_web"
)

// Format 映射到转换内核的格式标识。
func (p Protocol) Format() registry.Format {
	switch p {
	case ProtocolAnthropic:
		return registry.FormatClaudeMessages
	case ProtocolOpenAIChat:
		return registry.FormatOpenAIChat
	case ProtocolGemini:
		return registry.FormatGeminiGenerate
	case ProtocolResponses:
		return registry.FormatOpenAIResponses
	case ProtocolInteractions:
		return registry.FormatInteractions
	case ProtocolClaudeWeb:
		return registry.FormatClaudeWeb
	case ProtocolChatGPTWeb:
		return registry.FormatChatGPTWeb
	default:
		return registry.Format(p)
	}
}

// PathFor 该协议的上游端点路径。gemini 的模型名在 URL 路径里
// （/v1beta/models/{m}:generateContent / :streamGenerateContent?alt=sse，
// gemini-conversion-table §格式兼容面），故需要请求体里的模型名与流式开关；
// anthropic/openai_chat 忽略两者。模型名缺省或含注入字符 → 显式报错，
// 绝不拼畸形 URL 打给上游（GW 陷阱 13）。
func (p Protocol) PathFor(model string, stream bool) (string, error) {
	switch p {
	case ProtocolAnthropic:
		return "/v1/messages", nil
	case ProtocolOpenAIChat:
		return "/v1/chat/completions", nil
	case ProtocolGemini:
		m, err := geminiModelPath(model)
		if err != nil {
			return "", err
		}
		if stream {
			return "/v1beta/" + m + ":streamGenerateContent?alt=sse", nil
		}
		return "/v1beta/" + m + ":generateContent", nil
	case ProtocolResponses:
		// Responses 端点路径固定；流式由请求体 `stream:true` 决定（SSE 语义
		// 事件流），模型名不进路径（表 §请求面）。
		return "/v1/responses", nil
	case ProtocolInteractions:
		// Interactions 端点路径固定（POST /v1beta/interactions，官方 OpenAPI
		// servers 前缀 `/v1beta`；interactions-wire §端点族）；模型名与 stream
		// 开关都在请求体里（OAS:Interaction），不进路径——与 gemini 的
		// 「模型名进路径」相反，故两参忽略。鉴权头与 Api-Revision 在 forward 里设。
		return "/v1beta/interactions", nil
	case ProtocolClaudeWeb:
		// claude_web 的 completion 路径含 org/conversation 两段**动态段**
		// （`/api/organizations/{org}/chat_conversations/{conv}/completion`）——
		// 静态拼不出。forward 在调用本函数前已为该协议做会话准备并拿到路径
		// （claudeWebPrepare）；走到这里 = 接线错误，显式报错（绝不拼畸形 URL）。
		return "", errors.New("claude_web upstream path is dynamic (org/conversation); use claudeWebPrepare")
	case ProtocolChatGPTWeb:
		// chatgpt_web 的 completion 路径**静态**（`/backend-anon/f/conversation`），
		// 但 prepare 链（sentinel → prepare）必须先跑、conduit/sentinel 令牌必须
		// 进头面——forward 前置 chatgptWebPrepare 拿路径与令牌；走到这里 = 接线
		// 错误，显式报错（绝不跳过 prepare 直接打 completion——上游会判反爬，
		// KB §陷阱「/f/ 路径与 prepare 不可省」）。
		return "", errors.New("chatgpt_web upstream requires the prepare chain; use chatgptWebPrepare")
	default:
		return "", fmt.Errorf("service: protocol %q has no upstream path", p)
	}
}

// geminiModelPath 模型名 → gemini 路径段：短名补 `models/` 前缀，已限定名
// （models/… / tunedModels/…）原样用。注入守卫（GW 陷阱 13：官方 SDK 拒绝
// `..`/`?`/`&`）：含 `..`/`?`/`&`/`#`/`%`/空白/控制字符 → 显式报错；
// 其它含 `/` 的全路径（Vertex `projects/…`）本 slice 不支持 → 显式报错。
func geminiModelPath(model string) (string, error) {
	m := strings.TrimSpace(model)
	if m == "" {
		return "", errors.New("model is required for gemini upstream")
	}
	if strings.ContainsAny(m, "?&#%\t\n\r ") || strings.Contains(m, "..") {
		return "", fmt.Errorf("invalid model name %q for gemini upstream", model)
	}
	if strings.HasPrefix(m, "models/") || strings.HasPrefix(m, "tunedModels/") {
		return m, nil
	}
	if strings.Contains(m, "/") {
		return "", fmt.Errorf("model %q: vertex-style full paths are not supported (want a bare model name, models/… or tunedModels/…)", model)
	}
	return "models/" + m, nil
}

// Upstream 单个上游端点。
type Upstream struct {
	Name    string `yaml:"name"`
	BaseURL string `yaml:"base_url"`
	APIKey  string `yaml:"api_key"`
	// Credentials 多凭据池（W53-2；**仅 ProtocolClaudeWeb 生效**——池接线目前
	// 落在 claude_web 面，其余协议给出该字段 = 配置错误，显式报错）。可选列表；
	// 每项支持 `${VAR}` 环境变量引用（与 `api_key` 同机制，见 LoadConfig）。
	// 与 `api_key` **互斥**（两处给凭据 = 歧义，Validate 拒绝）；`api_key` 保持
	// 兼容 = 单元素池。选择 = 最少近期使用（轮转）；冷却 = 配额类（429）
	// retry-after/短窗、凭据类（401/无盾 403）长窗（见 credentials_pool.go）；
	// 全部冷却 → `upstream_credentials_exhausted` 显式报错。**记账只记索引**：
	// 日志/错误/报告绝不出现凭据值。
	Credentials []string `yaml:"credentials"`
	Protocol    Protocol `yaml:"protocol"`
	// WorkspaceID anthropic 上游的 `anthropic-workspace-id` 注入/覆盖值
	//（W5-28 条目 3 / B3；仅 ProtocolAnthropic 生效）：非空 = 每个上游请求带上
	// 该值（覆盖客户端自带值）；空 = 客户端带该头则原样转发、不带则不发明。
	// 场景：上游 = Claude Platform on AWS 时该平台逐请求要求此头（P64B §缺口 4）。
	WorkspaceID string `yaml:"workspace_id"`
	// OrgID claude_web 上游的组织 UUID（**仅 ProtocolClaudeWeb 生效**；W10）：
	// 非空 = 跳过 org 发现（`GET /api/organizations`）直接用该值；空 = 每次
	// 按 KB §组织发现规则现取（多组织时按 rate_limit_tier 选，见 claude_web.go）。
	OrgID string `yaml:"org_id"`
	// DeviceID chatgpt_web 上游的客户端设备 id（**仅 ProtocolChatGPTWeb 生效**；
	// W30-2）。值 = `OAI-Device-Id` 请求头——匿名 Android 面的身份头之一
	// （W29-1 §1.2 实证：非空必需 + `OAI-Client-Type`/`OAI-Package-Name` 二者
	// 其一；缺 → 401 `{"detail":"Unauthorized"}`）。**空 = 每请求现生成一个
	// 随机 UUID v4**：匿名档无身份可保（零账号），且「同一 oai-did 并发提交 →
	// 403/challenge」是已知 quirk（KB quirk 8）——逐请求新设备规避之；非空 =
	// 钉住固定值（对齐真机 / 有意复用设备的场景）。形态守卫：可见 ASCII、无
	// 空白/控制字符、≤64 字节（头值注入面；配置期显式报错）。
	DeviceID string `yaml:"device_id"`
	// CleanupConversations claude_web 会话清理开关（**仅 ProtocolClaudeWeb
	// 生效**；W13 清理面）：缺省（未写）**开**——每次 completion 收尾后异步
	// DELETE 本请求创建的 conversation（KB §清理面「用完即删是常态」：BK 3 次
	// 重试 + 2s 间隔失败只 warn，claude.go:189-224；YS 后台 goroutine，
	// handle.go:229-244；YS 的 `ChatDelete` 同款缺省开：`os.Getenv("CHAT_DELETE")
	// != "false"`）。显式 `false` = 关（会话保留——调试/保历史场景；行为与引入
	// 前一致）。删除走独立异步面：失败/超时绝不改下游响应（见 claude_web.go
	// 清理面头注）。放置在上游条目上（与 org_id/proxy 同面）：清理用该上游的
	// 凭据与 org，天然是上游级行为；多账号时可逐账号开关。
	CleanupConversations *bool `yaml:"cleanup_conversations"`
	// ToolSim 客户端工具模拟（**仅 ProtocolClaudeWeb 生效**；W18-1 §S3；设计单
	// research/topics/claude-web-tool-sim/README.md §S1/§S3）。缺省（未写）**关**。
	// 开 = 该上游的**模拟·客户端执行**形态（A1）：请求面在 prompt 最前注入工具
	// 契约段（客户端 tools 定义进 prompt 文本；上游 `tools` 字段仍恒三元组——
	// W11-3 契约不破），出口面把模型文本里的契约标签
	// `<tool_use name="X">{args}</tool_use>` 合成回 `tool_use` 块（stop_reason →
	// tool_use）——注入与解析**成对**（绝不半承诺，设计单 §红线 2）。
	//
	// 置位是**三条件合取**（服务壳判）：本开关 && 上游 protocol = claude_web &&
	// 入口请求带 tools（`ir.Request.Tools` 非空）；三者全真才置位（gate 见
	// server.go forward）。缺省关的理由：① 模拟面改变上游 prompt 文本与下游出口
	// 块序（新增 tool_use 块），不属最小可用集（对齐两仓基线 = 零客户端工具面，
	// 设计单 §E3）；② 模型侧守约率未实测（设计单 §未决 U2）——opt-in 把风险留给
	// 知情使用者；③ 上游级（非全局）——failover 链里换到非 claude_web 台时模拟
	// 自动不置位，语义随台而变。
	ToolSim bool `yaml:"tool_sim"`
	// Proxy 该上游的出站代理（W11；**仅该上游的出站流量**——下游与其它上游
	// 不受影响）。支持 `http://`（HTTP CONNECT 隧道）、`socks5://` / `socks5h://`
	// （SOCKS5；主机名一律交由代理解析——两 scheme 在本实现等价），可带
	// userinfo 认证（`scheme://user:pass@host:port`）。空 = 不配（出站语义与
	// 引入前一致：进程环境 HTTP_PROXY/HTTPS_PROXY/NO_PROXY 仍生效）。
	Proxy string `yaml:"proxy"`
	// TLSFingerprint 该上游的出站 TLS 指纹（W11；**仅该上游**）：`chrome` =
	// uTLS Chrome ClientHello（HelloChrome_Auto）+ 真 HTTP/2——CF 盾对抗面；
	// `none` = 标准库 TLS 栈；空 = 缺省 **none**（全部协议）。缺省依据 W11-2
	// 实测（2026-10-14，真 claude.ai + 家宽出口）：Go 默认栈 GET /api/organizations
	// = 200（h1/h2 双过）——CF 403 的触发因子是 UA/头面而非 TLS 指纹（同 curl
	// 仅换 Chrome UA 即 403→200）；`chrome` 保留为显式开关（completion POST 面
	// CF 档未实测，「保持双路」）。开关只影响该上游出站，绝不作用于下游。
	TLSFingerprint string `yaml:"tls_fingerprint"`
	// TaskFace 任务面能力声明（W27-1 / Slice 5；提案 `service-task-surface`
	// §形态 C 能力表）。值域 `managed | passthrough | off`；缺省（未写）=
	// **managed**（提案原文缺省）。
	//
	//   - `managed`：网关托管（Slice 1-4 面）——`background` 在服务层手术剥除
	//     （绝不注入上游），网关自造 id（`resp_synth_<32hex>`）、自跑长流、自缓冲
	//     事件（续流/取消/记账全在网关）。任务流量（托管任务的上游尝试）可落该
	//     上游。
	//   - `passthrough`：上游原生任务面（**仅 ProtocolResponses 合法**——OpenAI
	//     类端点族 `POST/GET /v1/responses` + `/{id}/cancel`）：`background` 原样
	//     透传，**id = 上游 id**（客户端拿到的是上游的任务 id），retrieve/cancel/
	//     续流的查询串（含 `starting_after` 游标）逐字透传上游任务端点；网关只持
	//     id→上游路由表（内存、有界 TTL），不缓冲、不合成终态。create 不重试
	//     （重试可能多建上游任务——孤儿任务无法回滚）。
	//   - `off`：任务面关闭——任务流量**绝不**落该上游（create 跳过；托管任务的
	//     failover 链同样跳过）；链上无可用上游时任务创建 404
	//     （error.code=task_face_unavailable）。同步路径（background 缺省/false）
	//     不受影响——off 只关任务面，不关上游。
	//
	// 语义见 `task_passthrough.go` 头注（Slice 5）与报告 A2A-L19-W27-1。
	TaskFace string `yaml:"task_face"`
}

// 任务面能力值（Upstream.TaskFace；缺省 = managed，提案原文缺省）。
const (
	// taskFaceManaged 网关托管（缺省）：background 剥除、网关自造 id + 缓冲。
	taskFaceManaged = "managed"
	// taskFacePassthrough 上游原生任务面：background/id/游标全透传（仅 responses 协议）。
	taskFacePassthrough = "passthrough"
	// taskFaceOff 任务面关闭：任务流量绝不落该上游；无可用上游时任务端点 404。
	taskFaceOff = "off"
)

// 能力面降级开关值（Config.TaskFaceDegrade；缺省 = off——W36-2 / S8）。
const (
	// taskFaceDegradeOff 只观测不降级（缺省）：声明（L1）是唯一路由权威。
	taskFaceDegradeOff = "off"
	// taskFaceDegradeAuto 首个结论性失败（404/405）后进程内降级该上游
	// passthrough→managed（重启清零、绝不写回配置）。
	taskFaceDegradeAuto = "auto"
)

// taskFace 有效任务面能力（缺省 managed——提案原文缺省）。
func (u Upstream) taskFace() string {
	if u.TaskFace == "" {
		return taskFaceManaged
	}
	return u.TaskFace
}

// ModelEntry 模型表条目（顶层 `models:`；W5-28 条目 1）：
// **A6 模型别名映射**（请求面 alias → 上游 model 改写）+ **发现面数据源**
// （`GET /v1/models` 从表读）。缺省（不写 `models:`）= 现状：发现面静态四模型、
// 请求面模型名透传（配置非空时发现面只列配置条目——静态表是缺省值，不是并集）。
type ModelEntry struct {
	// ID 模型 id（必填、唯一）：发现面 `GET /v1/models` 的 id，也是请求面别名
	//（客户端使用的模型名）。
	ID string `yaml:"id"`
	// UpstreamModel 上游模型名（可选）：请求体顶层 `model` 命中 ID 时改写为该值
	//（同协议直通面字节级局部替换；其余字节不动）；缺省 = 不改写（id 原样进上游）。
	UpstreamModel string `yaml:"upstream_model"`
	// MaxInputTokens / MaxTokens 发现面上限字段（可选；0 = 不下发该字段——客户端
	// 按自己的缺省能力假设；≥1e5 时 CC 用作上下文窗口覆盖值，W5-27）。
	MaxInputTokens int `yaml:"max_input_tokens"`
	MaxTokens      int `yaml:"max_tokens"`
	// Upstreams 可选上游白名单（按 `upstreams[].name` 精确匹配；A2A-L19-W81-2）：
	// 空/未写 = 不限制（现状——该模型可落任一上游）；非空 = 该模型的候选链
	// **只含这些上游**。白名单在健康/冷却筛选**之前**生效；过滤后为空 = 既有
	// 「无候选」错误，绝不回退到全列表。名字必须存在于 `upstreams[]`（Validate
	// 显式报错；同表重复同样报错）。用途：非 Claude 模型钉在 upstream-a/upstream-b
	// 等台，绝不先打 claude_web（新鲜 claude_web 凭据对非 Claude 模型 403 +
	// 30m 凭据冷却，W80 真实探针实测）。
	Upstreams []string `yaml:"upstreams"`
}

// UISection UI 托管面配置（`ui:` 段；W56-1）。指针字段区分「未写」与「显式
// 值」：Enabled 缺省 true（只多挂 /ui 路由，无害）；RateLimitPerMin 缺省 0 = 关。
type UISection struct {
	// Enabled 开关（缺省 true）：false = 不注册 /ui 路由族（/ui/* 全部 404）。
	Enabled *bool `yaml:"enabled"`
	// RateLimitPerMin per-IP 限流（次/分钟；缺省 0 = 关）。> 0 = 对 /v1/* 与
	// /ui/api/* 做令牌桶限流（内存态、1 分钟窗；超限 429 + Retry-After）。
	// 公开部署建议开启（如 30；见 config.example.yaml）。
	RateLimitPerMin int `yaml:"rate_limit_per_min"`
}

// StoreSection 持久化配置（`store:` 段；P1-1，设计 `claude2api-public-service`
// §2.2/§2.7）。**硬门：Enabled 缺省 false——缺省时 `OpenStore` 返回 nil，会话/
// 配额/审计全部走 P0 内存路径，行为逐字不变。**
type StoreSection struct {
	// Enabled 开关（缺省 false）：true = 打开 SQLite 持久化档（会话/会话列表/
	// 消息/回合/用量台账/审计落库；重启恢复）。false（缺省）= 内存现状档。
	Enabled bool `yaml:"enabled"`
	// Path SQLite 库文件路径（**Enabled=true 时必填**——空 = 配置错误，绝不静默
	// 降级；父目录不存在时自动创建）。相对路径按进程工作目录解析。
	// 例：`data/any2api.db`。
	Path string `yaml:"path"`
}

// PublicSection 公开服务配置（`public:` 段；P0-1..4：匿名会话 / access key 门控 /
// trust proxy / 并发闸）。**硬门：Enabled 缺省 false——全部新行为只在
// enabled=true 生效，缺省时全链与引入前逐字节一致。**
type PublicSection struct {
	// Enabled 开关（缺省 false）：true = 启用公开服务面（会话 cookie 签发/
	// 解析、gated 门控、trusted_proxies、全局并发闸、单凭据 busy/min-gap）。
	// false（缺省）= 上述全部关闭（零行为变化）。
	Enabled bool `yaml:"enabled"`
	// AccessMode 准入模式（`open | gated`；缺省/空 = open）。
	//   - open：匿名即用（无 cookie 请求自动签发会话）；
	//   - gated：无 access key 的新会话不可建立——无 cookie 且无有效 key 的
	//     请求 401（按入口协议信封）；带 key 校验通过建会话（记 key_id）。
	AccessMode string `yaml:"access_mode"`
	// AccessKeys gated 模式的准入 key 列表（支持 `${VAR}` env-ref，同 upstream
	// credentials 机制；**gated 必填**；比较 = sha256 常量时间；key 原文绝不进
	// 日志/响应；key_id = sha256 前 8 hex）。open 模式给该字段 = 配置错误（歧义）。
	AccessKeys []string `yaml:"access_keys"`
	// TrustedProxies 反代信任列表（CIDR 或裸 IP）。TCP 对端命中时取
	// X-Forwarded-For **最右非信任跳**为客户端 IP（限流/会话面），否则回落
	// RemoteAddr。**未配置 = 现状**（不读 XFF——防伪造）。
	TrustedProxies []string `yaml:"trusted_proxies"`
	// MaxInflight 全局并发闸上限（缺省 6；<=0 用缺省）。超出的请求进有界队列
	// （QueueSize），队列满/等待超时 → 429 + 正整数 Retry-After。
	MaxInflight int `yaml:"max_inflight"`
	// QueueSize 并发闸有界队列深度（缺省 32；<=0 用缺省）。仅平滑短突发。
	QueueSize int `yaml:"queue_size"`
	// QueueTimeoutSeconds 排队等待上限秒数（缺省 60；<=0 用缺省）。超时 → 429。
	QueueTimeoutSeconds int `yaml:"queue_timeout_seconds"`
	// CredentialMinGapSeconds 单凭据最小复用间隔秒数（指针以区分「未写」与
	// 「显式 0」：nil = 缺省 300（与 W52-2 运营配方一致）；显式 0 = 关闭；
	// >0 = 间隔秒数）。仅 claude_web 凭据池 + Enabled 时生效。
	CredentialMinGapSeconds *int `yaml:"credential_min_gap_seconds"`
	// MaxCostClass 允许的最高成本档（P0-5；设计 §3.2 档位控制）。取值 =
	// `light | medium | heavy | max`（= A/B/C/D 四档，权重 1/6/11/59 units；
	// 空 = 缺省 medium）。**请求成本档高于本值 → 400 拒绝**（缺省 medium =
	// 拒绝 heavy/max 档——max 单发 0.59 窗可烧穿一号；heavy 按需放开）。
	// 分类 = 请求体 max_tokens / max_output_tokens 粗分（阈值表见 limits.go
	// classifyMaxTokens）。
	MaxCostClass string `yaml:"max_cost_class"`
	// DailyBudgetUnits 日预算 units（P0-5；设计 §3.2 缺省 250——6 号池保守档
	// ≈254 units/日）。指针以区分「未写」与「显式 0」：nil = 缺省 250；
	// 显式 0 = 关（不限预算）；>0 = 日预算（UTC 日界重置）。耗尽 → 429 +
	// `daily_quota_exhausted`（快速路径，准入即拒）。
	DailyBudgetUnits *int `yaml:"daily_budget_units"`
	// MaxBodyBytes 请求体上限字节（P0-8；设计 §4.2 缺省 1MB——聊天文本量级）。
	// 指针以区分「未写」与「显式 0」：nil = 缺省 1MB；显式 0 = 用全局 32MB
	// （不额外收紧）；>0 = 上限。超限 → 413（按入口协议信封）。
	MaxBodyBytes *int64 `yaml:"max_body_bytes"`
	// MaxMessages messages 数组条数上限（P0-8；设计 §4.2 缺省 200）。指针以
	// 区分「未写」与「显式 0」：nil = 缺省 200；显式 0 = 关；>0 = 上限。
	// 超限 → 413（按入口协议信封）。覆盖 `messages`（claude/openai chat）与
	// `input` 数组（responses）。
	MaxMessages *int `yaml:"max_messages"`
	// MaxMessageChars 单条消息内容长度上限（P0-8；设计 §4.2 缺省 256KB）。
	// 指针以区分「未写」与「显式 0」：nil = 缺省 256KB；显式 0 = 关；>0 =
	// 上限。计量 = content 字段的 JSON 原始字节（≥ 解码后字节的上界——保守）。
	// 超限 → 413（按入口协议信封）。
	MaxMessageChars *int `yaml:"max_message_chars"`
}

// ResponseHygieneSection 响应卫生配置（`response_hygiene:` 段；W104）。**硬门：
// ScrubSameFormatExtras 缺省 false——缺省时同格式非流式成功响应体与上游逐字节
// 一致（既有保真策略不变）。**
type ResponseHygieneSection struct {
	// ScrubSameFormatExtras 同格式非流式成功响应清洗开关（缺省 false）。
	//
	//   - false（缺省）：同格式非流式响应体原样直通——上游字节逐字节等于下游
	//     字节（`out := respBody` 后直接 `w.Write`，零解析零重排）。
	//   - true：仅对**同格式非流式成功**响应体做手术式剥除：顶层
	//     `elapsed_ms` / `first_chunk_at`（upstream-a/upstream-private 私有计时键，
	//     W83-1 X1 实测）连同键值/必要逗号删除，**其余字节逐字节保留**
	//     （不 re-marshal、不重排、不重编码）。usage 观测在剥除前完成，不影响
	//     记账。
	//
	// 生效边界：只作用于 `upProto == entry` 的非流式成功体。流式帧与跨格式
	// 转换出口**不受本开关影响**——跨格式由各渲染器的 foreignResponseExtras
	// 过滤表处理（W85/W101），流式 extras 观测面为干净（W83/W101 实测）。
	ScrubSameFormatExtras bool `yaml:"scrub_same_format_extras"`
}

// 公开服务缺省（P0-1..4；enabled=true 且未显式配置时）。
const (
	// DefaultPublicMaxInflight 全局并发闸缺省上限（设计 §3.2 cap=6）。
	DefaultPublicMaxInflight = 6
	// DefaultPublicQueueSize 有界队列缺省深度（设计 §3.2）。
	DefaultPublicQueueSize = 32
	// DefaultPublicQueueTimeout 排队等待缺省上限。
	DefaultPublicQueueTimeout = 60 * time.Second
	// DefaultCredentialMinGap 单凭据最小复用间隔缺省（W52-2 配方 ≥300s）。
	DefaultCredentialMinGap = 300 * time.Second
	// DefaultPublicDailyBudgetUnits 日预算缺省 units（P0-5；设计 §3.2——6 号池
	// 保守档 ≈28 重 + 86 轻/日 ≈ 254 units，取 250）。
	DefaultPublicDailyBudgetUnits = 250
	// DefaultPublicMaxBodyBytes 公网请求体缺省上限（P0-8；设计 §4.2——1MB）。
	DefaultPublicMaxBodyBytes = 1 << 20
	// DefaultPublicMaxMessages messages 条数缺省上限（P0-8；设计 §4.2——200）。
	DefaultPublicMaxMessages = 200
	// DefaultPublicMaxMessageChars 单条内容缺省上限（P0-8；设计 §4.2——256KB）。
	DefaultPublicMaxMessageChars = 256 << 10
	// DefaultPublicMaxCostClass 成本档缺省上限（P0-5；设计 §3.2——medium）。
	DefaultPublicMaxCostClass = "medium"
)

// Config 服务配置。
type Config struct {
	// Listen HTTP 监听地址（缺省 127.0.0.1:8787——默认只绑回环）。
	Listen string `yaml:"listen"`
	// AdminKey 管理面密钥（可选；W5-33 最小管理面，P63 裁定）。
	// **缺省空 = 管理面不注册**：`/admin/*` 全部 404（对齐 CPA「未配管理密钥 =
	// 路由根本不存在（404 非 401）」，server_management.go:204-211）。非空 =
	// 注册三条端点（GET /admin/diagnostics、POST /admin/cooldowns/reset、
	// GET /admin/logs），统一密钥中间件：`Authorization: Bearer <key>` 或
	// `X-Management-Key: <key>`，常量时间比较（sha256 后定长比较，无长度侧信道），
	// 连续 5 次鉴权失败封禁该客户端 IP 30 分钟（CPA handler.go:364-407 同源；
	// 封禁期一律 401，不暴露封禁状态）。**管理面仅建议本机监听**：密钥是唯一门，
	// 本仓不实现 TLS / 远端白名单（listen 保持回环即可）。
	AdminKey string `yaml:"admin_key"`
	// AdminLogCapacity 管理面错误日志 ring 容量（可选；缺省 256，允许 1..4096）。
	// **未写 = 缺省 256；显式越界（含 0/负值）= 配置错误**——指针区分「未写」与
	// 「写了 0」：写 0 想要「关掉 ring」的意图在本面不存在（ring 恒记录，见
	// server.go），静默变 256 才是事故。W5-36。
	AdminLogCapacity *int `yaml:"admin_log_capacity"`
	// Upstreams 上游列表（至少一个）。**有序 failover 链**：按配置顺序尝试，
	// 未提交下游响应前失败（C1 分类）换下一个；提交后失败绝不重试/重放。
	// 同协议多上游 = 在同一列表里追加同 protocol 的条目（示例配置演示）。
	Upstreams []Upstream `yaml:"upstreams"`
	// HeartbeatSeconds 下游保活帧间隔秒数（上游静默时；缺省 15）。
	HeartbeatSeconds int `yaml:"heartbeat_seconds"`
	// CooldownFailures 上游连续失败达该次数后进入短冷却（缺省 3；<=0 用缺省）。
	CooldownFailures int `yaml:"cooldown_failures"`
	// CooldownSeconds 冷却窗口秒数（缺省 30；<=0 用缺省）。上游带
	// `retry-after: <整数秒>` 的失败按该秒数冷却（C1 精确执行），两者取较晚者。
	CooldownSeconds int `yaml:"cooldown_seconds"`
	// FirstByteTimeoutSeconds 首产出超时秒数（**0 = 关，缺省**）：流式尝试从发起
	// 上游请求到**首个产出字节**（提交点）超过该秒数 → 中止本次尝试（未提交 →
	// 按 C1 换上游）。这是 W5-15「TTFT keepalive 不做」决策的**替代面**（保留
	// 提交点与真实状态码；relay.go 的 downstream 决策注释）。缺省关的理由：P24/
	// W5-15 决策理由 ②——存在首产出 ~100s 的合法上游（思考型模型），默认值会把它
	// 们误杀；上游响应头等待另有 ResponseHeaderTimeout（60s）兜底。
	FirstByteTimeoutSeconds int `yaml:"first_byte_timeout_seconds"`
	// UpstreamIdleTimeoutSeconds 上游**内容静默**超时秒数（**0 = 关，缺省**；与
	// first_byte_timeout_seconds 同款结构）：流式尝试内连续该秒数**没有任何
	// 内容帧**（keepalive/ping/空 delta/`response.created|in_progress|queued`
	// 等非内容帧不算内容——grok-build-wire 假活跃计时器同款判据，见
	// frameCarriesContent）→ 中止本次尝试：未提交 → 按 C1 换上游；已提交 →
	// 写 `incomplete_stream` 错误帧收尾（绝不重放）。覆盖两个面：① 上游挂住
	// 零字节（外层 idle_timeout 语义）；② 上游只发 keepalive/空 delta 的假活跃
	// （内层「内容判定」语义——first_byte 拦不住它：pass-through 面 keepalive
	// 帧本身就会提交）。缺省关的理由同 first_byte：存在合法长静默上游（长思考
	// 期只发 ping；CC 侧看门狗预算 90s，见 topics/heartbeats）——要开时取
	// > 最长合法静默，建议 ≥90s。
	UpstreamIdleTimeoutSeconds int `yaml:"upstream_idle_timeout_seconds"`
	// FailoverPolicy per-exception 换上游预算（W5-22 条目 3；P27 §router 对照表
	// 点名「按异常类给预算」）。键 = 异常类，值 = 该类失败最多换上游次数：
	// 0 = 不换（原错误信封上抛）；N>0 = 最多 N 次；-1 = 不限。缺省（不写该类）
	// = 内置 C1 分类：rate_limit/timeout/conflict/auth/server_error 不限次；
	// forbidden 仅资源级文本特征；bad_request/default 不换；quota（错误体命中
	// 额度/凭据词表——insufficient_quota / billing_hard_limit_reached /
	// credit_balance_exhausted / upstream_credentials_exhausted / no_eligible_keys /
	// payment_required）不换也不冷却（等/换都没用）。显式信号（错误体
	// `overloaded_error`、`x-should-retry`）优先于本表（C1 实测语义，配置不可
	// 改写）。类名：rate_limit|timeout|conflict|auth|forbidden|server_error|
	// bad_request|default|quota。
	FailoverPolicy map[string]int `yaml:"failover_policy"`
	// FailureRate 失败率冷却（W5-22 条目 3；LiteLLM `failure-rate >0.5 & ≥5
	// req/min` 范式的最小版）：同一上游在窗口内请求数 ≥ min_requests 且失败率 >
	// threshold → 进入冷却（与连续失败阈值/retry-after 取较晚者）。缺省 = 开
	// （5 / 0.5 / 60s）；enabled: false 关闭。
	FailureRate *FailureRateCooldown `yaml:"failure_rate_cooldown"`
	// RetrySameUpstream 同上游瞬时类失败的重试次数上限（W5-32 条目 1；P65B
	// 裁定 #1）。瞬时类 = 网络层错误（连接失败/超时/截断）+ 显式信号
	// （x-should-retry: true / 错误体 overloaded_error 文本）+ 429/503/529
	// （rate_limit / server_error 类）；`x-should-retry: false` 压住一切。
	// 重试在同一台上短退避后再试，仍失败才换下一个上游；**中间重试不计健康/
	// 不冷却，终态才记一次**（Parrot `failover.py:2683-2687` 语义）。**缺省 0
	// = 关**（保守优先：不改引入前的换上游节奏；建议值 1——见 config.example）。
	// 上限截到 3（retrySameUpstreamMax；防误配把请求拖死在单台）。
	RetrySameUpstream int `yaml:"retry_same_upstream"`
	// RetrySameUpstreamDelaysMs 同上游重试的退避延迟序列（毫秒；越界取最后一
	// 档）。缺省 [750, 1750]（Parrot `failover.py:2683-2712` 的 0.75/1.75s
	// 同源）；每次实际等待加 ±20% jitter。上游带 retry-after-ms/retry-after 时
	// 按头值精确执行（上限 60s），本序列只用于无头形态。空/零值 = 用缺省。
	RetrySameUpstreamDelaysMs []int `yaml:"retry_same_upstream_delays_ms"`
	// CooldownLadder 冷却阶梯（秒；W5-32 条目 2；P65B 裁定 #2）。非空 = 每次
	// **进入冷却**（连续失败达阈值 / 失败率触发）逐级取下一档窗口，成功清零，
	// 冷却中失败不推进档位；与 cooldown_seconds 取较晚者。空（缺省）= 行为与
	// 引入前一致（固定 cooldown_seconds 窗口）。报告建议档 = [30, 120, 600]
	// （30s→2m→10m，有限档、无永久——单进程薄壳无人工解除通道，保守优先）。
	// 必须逐档严格递增、每档 > 0（Validate 显式报错）。
	CooldownLadder []int `yaml:"cooldown_ladder"`
	// TaskFaceDegrade 能力矩阵 L2 被动证据的降级开关（W36-2 / S8；topic
	// `service-task-surface` §③ L2 行原文）。值域 `off | auto`；缺省（未写）=
	// **off**。
	//
	//   - `off`（缺省）：证据只观测（诊断计数 + ring + 日志），**绝不改路由**
	//     ——声明（L1）是唯一路由权威。
	//   - `auto`：passthrough 上游出现首个**结论性失败**（任务端点 404/405）后
	//     进程内降级该上游 passthrough→managed（网关托管兜底——「上游能透传就
	//     透传，不能就托管」）；只影响本进程、**重启清零**、绝不写回配置。结论闸
	//     同 sub2api：网络失败/5xx/429 不下结论（不计证据、不降级）。
	//
	// 语义见 `task_passthrough.go` observeTaskFaceEvidence 与报告 A2A-L19-W36-2。
	TaskFaceDegrade string `yaml:"task_face_degrade"`
	// Models 模型表（可选；W5-28 条目 1）。非空 = 发现面（GET /v1/models）逐条从
	// 表读 + 请求面按表把别名改写为上游模型；缺省 = 现状（发现面静态四模型、
	// 请求面模型名透传）。见 ModelEntry。
	Models []ModelEntry `yaml:"models"`
	// DebugCaptureDir debug 体录制目录（**缺省空 = 关**；W6-6 建议 ④ 的落地面）。
	// 非空 = 每个入口请求在目录内落一份 `<req_id>.json` 录制（文件名含请求 id；
	// 请求/响应体 + 每个上游尝试的请求/响应体，逐字节留档——「翻译层逐字段
	// 归因」级物证）。**凭据头一律掩码**（authorization/x-api-key/x-goog-api-key/
	// cookie 等 + URL 查询串里的 key/token 参数；见 capture.go 的掩码表）。
	// 容量纪律：单文件上限 4 体 × debugCaptureBodyCap（1 MiB），超限截断并标记
	// `*_truncated`。目录按需创建；录制失败只打日志，绝不影响请求路径。
	DebugCaptureDir string `yaml:"debug_capture_dir"`
	// UI UI 托管面（可选；W56-1）。缺省（不写本节）= enabled true（只多挂
	// /ui 路由，无害）+ 限流关。`enabled: false` = 不注册 /ui 路由族（/ui/*
	// 全部 404）；`rate_limit_per_min` > 0 = per-IP 令牌桶限流 /v1/* 与
	// /ui/api/*（超限 429 + Retry-After；内存态、1 分钟窗——见 ui_server.go）。
	UI *UISection `yaml:"ui"`
	// Store 持久化面（可选；P1-1）。缺省（不写本节 / enabled: false）= 内存现状
	// 档（OpenStore 返回 nil——零行为变化）。见 StoreSection。
	Store *StoreSection `yaml:"store"`
	// Public 公开服务面（可选；P0-1..4）。缺省（不写本节 / enabled: false）=
	// **全链零行为变化**（会话/门控/trust proxy/并发闸/busy/min-gap 全关）；
	// 见 PublicSection。
	Public *PublicSection `yaml:"public"`
	// ResponseHygiene 响应卫生面（可选；W104）。缺省（不写本节）= 全部关闭——
	// 同格式非流式响应体逐字节直通（零行为变化）；见 ResponseHygieneSection。
	ResponseHygiene *ResponseHygieneSection `yaml:"response_hygiene"`

	// credentialPools 每上游凭据池（W53-2 运行时状态；yaml 忽略——非配置面）。
	// 惰性初始化（credentialPoolAt 首次请求建池）：同一 Config 的全部请求共享
	// LRU/冷却状态。**凭据字节只活在本结构与装配期的请求头里**，绝不进日志/
	// 错误/attempt 记录/报告。
	credentialPools []*credentialPool
}

// FailureRateCooldown 失败率冷却参数（W5-22 条目 3）。零值字段用缺省填充。
type FailureRateCooldown struct {
	// Enabled 开关（缺省 true；指针以区分「未写」与「写 false」）。
	Enabled *bool `yaml:"enabled"`
	// MinRequests 触发下限：窗口内请求数达到该值才评估失败率（缺省 5）。
	MinRequests int `yaml:"min_requests"`
	// Threshold 失败率阈值（严格大于；缺省 0.5）。
	Threshold float64 `yaml:"threshold"`
	// WindowSeconds 统计窗口秒数（缺省 60）。
	WindowSeconds int `yaml:"window_seconds"`
}

// DefaultHeartbeatSeconds 心跳缺省间隔。
const DefaultHeartbeatSeconds = 15

// DefaultCooldownFailures 上游连续失败阈值缺省值。
const DefaultCooldownFailures = 3

// DefaultCooldownSeconds 上游冷却窗口缺省秒数。
const DefaultCooldownSeconds = 30

// 失败率冷却缺省（W5-22 条目 3；LiteLLM 范式常量：failure-rate > 0.5 且
// 本分钟 ≥ 5 请求——constants.py DEFAULT_FAILURE_THRESHOLD_PERCENT /
// DEFAULT_FAILURE_THRESHOLD_MINIMUM_REQUESTS 的最小版）。
const (
	// DefaultFailureRateMinRequests 失败率评估的最小窗口请求数。
	DefaultFailureRateMinRequests = 5
	// DefaultFailureRateThreshold 失败率阈值（严格大于才触发）。
	DefaultFailureRateThreshold = 0.5
	// DefaultFailureRateWindowSeconds 失败率统计窗口秒数。
	DefaultFailureRateWindowSeconds = 60
)

// failureRate 失败率冷却的有效参数（缺省填充；enabled 缺省 true——LiteLLM
// 基态口径；nil 配置安全）。
func (c *Config) failureRate() (enabled bool, minReq int, threshold float64, window time.Duration) {
	enabled = true
	minReq = DefaultFailureRateMinRequests
	threshold = DefaultFailureRateThreshold
	window = DefaultFailureRateWindowSeconds * time.Second
	if c == nil {
		return
	}
	fr := c.FailureRate
	if fr == nil {
		return
	}
	if fr.Enabled != nil {
		enabled = *fr.Enabled
	}
	if fr.MinRequests > 0 {
		minReq = fr.MinRequests
	}
	if fr.Threshold > 0 {
		threshold = fr.Threshold
	}
	if fr.WindowSeconds > 0 {
		window = time.Duration(fr.WindowSeconds) * time.Second
	}
	return
}

// DefaultListen 监听缺省值（回环）。
const DefaultListen = "127.0.0.1:8787"

// 同上游重试缺省（W5-32 条目 1）：延迟序列 [750, 1750]ms（Parrot
// `failover.py:2683-2712` 的 0.75/1.75s 同源）；次数缺省 0 = 关（保守优先，
// 不改引入前行为——要开就显式配 `retry_same_upstream`）。
var DefaultRetrySameUpstreamDelays = []time.Duration{750 * time.Millisecond, 1750 * time.Millisecond}

// retrySameUpstream 同上游瞬时类失败重试次数上限（有效值：0..retrySameUpstreamMax；
// <=0 = 关）。nil 安全。
func (c *Config) retrySameUpstream() int {
	if c == nil {
		return 0
	}
	n := c.RetrySameUpstream
	if n > retrySameUpstreamMax {
		n = retrySameUpstreamMax
	}
	return n
}

// retryDelays 同上游重试退避序列（毫秒配置 → 时长；空/全零 = 缺省序列）。nil 安全。
func (c *Config) retryDelays() []time.Duration {
	if c == nil || len(c.RetrySameUpstreamDelaysMs) == 0 {
		return DefaultRetrySameUpstreamDelays
	}
	out := make([]time.Duration, 0, len(c.RetrySameUpstreamDelaysMs))
	for _, ms := range c.RetrySameUpstreamDelaysMs {
		if ms > 0 {
			out = append(out, time.Duration(ms)*time.Millisecond)
		}
	}
	if len(out) == 0 {
		return DefaultRetrySameUpstreamDelays
	}
	return out
}

// cooldownLadder 冷却阶梯（秒配置 → 时长；空 = 关）。nil 安全。
func (c *Config) cooldownLadder() []time.Duration {
	if c == nil || len(c.CooldownLadder) == 0 {
		return nil
	}
	out := make([]time.Duration, 0, len(c.CooldownLadder))
	for _, s := range c.CooldownLadder {
		out = append(out, time.Duration(s)*time.Second)
	}
	return out
}

// adminKey 管理面密钥（trim 后；空 = 管理面不注册）。nil 安全。
func (c *Config) adminKey() string {
	if c == nil {
		return ""
	}
	return strings.TrimSpace(c.AdminKey)
}

// adminLogCapacity 管理面日志 ring 有效容量（未配 = DefaultAdminLogCapacity）。nil 安全。
func (c *Config) adminLogCapacity() int {
	if c == nil || c.AdminLogCapacity == nil {
		return DefaultAdminLogCapacity
	}
	return *c.AdminLogCapacity
}

// uiEnabled UI 托管面开关（缺省 true——只多挂 /ui 路由，无害）。nil 安全。
func (c *Config) uiEnabled() bool {
	if c == nil || c.UI == nil || c.UI.Enabled == nil {
		return true
	}
	return *c.UI.Enabled
}

// uiRateLimitPerMin per-IP 限流有效值（次/分钟；缺省 0 = 关）。nil 安全。
func (c *Config) uiRateLimitPerMin() int {
	if c == nil || c.UI == nil {
		return 0
	}
	return c.UI.RateLimitPerMin
}

// 准入模式取值（PublicSection.AccessMode；缺省 open）。
const (
	publicAccessModeOpen  = "open"
	publicAccessModeGated = "gated"
)

// publicEnabled 公开服务面开关（缺省 false——硬门：false 时全链零行为变化）。nil 安全。
func (c *Config) publicEnabled() bool {
	return c != nil && c.Public != nil && c.Public.Enabled
}

// scrubSameFormatExtras 同格式非流式成功体清洗开关（缺省 false = 字节直通）。
// nil 安全：未配 `response_hygiene:` 段 → false（零行为变化）。
func (c *Config) scrubSameFormatExtras() bool {
	return c != nil && c.ResponseHygiene != nil && c.ResponseHygiene.ScrubSameFormatExtras
}

// publicAccessMode 准入模式有效值（缺省 open）。nil 安全。
func (c *Config) publicAccessMode() string {
	if c == nil || c.Public == nil || c.Public.AccessMode == "" {
		return publicAccessModeOpen
	}
	return c.Public.AccessMode
}

// publicAccessKeys gated 准入 key 列表（nil 安全）。
func (c *Config) publicAccessKeys() []string {
	if c == nil || c.Public == nil {
		return nil
	}
	return c.Public.AccessKeys
}

// publicMaxInflight 全局并发闸有效上限（缺省 6）。nil 安全。
func (c *Config) publicMaxInflight() int {
	if c == nil || c.Public == nil || c.Public.MaxInflight <= 0 {
		return DefaultPublicMaxInflight
	}
	return c.Public.MaxInflight
}

// publicQueueSize 有界队列有效深度（缺省 32）。nil 安全。
func (c *Config) publicQueueSize() int {
	if c == nil || c.Public == nil || c.Public.QueueSize <= 0 {
		return DefaultPublicQueueSize
	}
	return c.Public.QueueSize
}

// publicQueueTimeout 排队等待有效上限（缺省 60s）。nil 安全。
func (c *Config) publicQueueTimeout() time.Duration {
	if c == nil || c.Public == nil || c.Public.QueueTimeoutSeconds <= 0 {
		return DefaultPublicQueueTimeout
	}
	return time.Duration(c.Public.QueueTimeoutSeconds) * time.Second
}

// credentialMinGap 单凭据最小复用间隔有效值（nil = 缺省 300s；显式 0 = 关）。
// nil 安全。
func (c *Config) credentialMinGap() time.Duration {
	if c == nil || c.Public == nil || c.Public.CredentialMinGapSeconds == nil {
		return DefaultCredentialMinGap
	}
	return time.Duration(*c.Public.CredentialMinGapSeconds) * time.Second
}

// publicMaxCostClass 允许的最高成本档（P0-5；空 = 缺省 medium）。nil 安全。
// 返回值保证是合法档名（Validate 已校验；未知值防御性回落缺省）。
func (c *Config) publicMaxCostClass() string {
	if c == nil || c.Public == nil || c.Public.MaxCostClass == "" {
		return DefaultPublicMaxCostClass
	}
	if _, ok := parseCostClassName(c.Public.MaxCostClass); !ok {
		return DefaultPublicMaxCostClass
	}
	return c.Public.MaxCostClass
}

// publicDailyBudgetUnits 日预算有效值（P0-5）：返回 (units, on)。nil = 缺省
// 250 + on；显式 0 = off（不限）；>0 = units + on。nil 安全。
func (c *Config) publicDailyBudgetUnits() (int, bool) {
	if c == nil || c.Public == nil || c.Public.DailyBudgetUnits == nil {
		return DefaultPublicDailyBudgetUnits, true
	}
	if *c.Public.DailyBudgetUnits <= 0 {
		return 0, false
	}
	return *c.Public.DailyBudgetUnits, true
}

// publicMaxBodyBytes 请求体上限有效值（P0-8）：nil = 缺省 1MB；显式 0 = 0
// （= 用全局 32MB，不额外收紧）；>0 = 上限字节。nil 安全。
func (c *Config) publicMaxBodyBytes() int64 {
	if c == nil || c.Public == nil || c.Public.MaxBodyBytes == nil {
		return DefaultPublicMaxBodyBytes
	}
	if *c.Public.MaxBodyBytes <= 0 {
		return 0
	}
	return *c.Public.MaxBodyBytes
}

// publicMaxMessages messages 条数上限有效值（P0-8）：nil = 缺省 200；显式
// 0 = 关；>0 = 上限。nil 安全。
func (c *Config) publicMaxMessages() int {
	if c == nil || c.Public == nil || c.Public.MaxMessages == nil {
		return DefaultPublicMaxMessages
	}
	if *c.Public.MaxMessages <= 0 {
		return 0
	}
	return *c.Public.MaxMessages
}

// publicMaxMessageChars 单条内容长度上限有效值（P0-8）：nil = 缺省 256KB；
// 显式 0 = 关；>0 = 上限。nil 安全。
func (c *Config) publicMaxMessageChars() int {
	if c == nil || c.Public == nil || c.Public.MaxMessageChars == nil {
		return DefaultPublicMaxMessageChars
	}
	if *c.Public.MaxMessageChars <= 0 {
		return 0
	}
	return *c.Public.MaxMessageChars
}

// parseTrustedProxies 解析 `public.trusted_proxies`（CIDR 或裸 IP——裸 IP 视为
// /32 或 /128）。非法项显式报错（绝不静默丢弃——配置纪律）。
func parseTrustedProxies(list []string) ([]*net.IPNet, error) {
	var out []*net.IPNet
	for i, s := range list {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, fmt.Errorf("trusted_proxies[%d]: empty entry", i)
		}
		if _, n, err := net.ParseCIDR(s); err == nil {
			out = append(out, n)
			continue
		}
		ip := net.ParseIP(s)
		if ip == nil {
			return nil, fmt.Errorf("trusted_proxies[%d]: %q is not a CIDR or IP", i, s)
		}
		bits := 32
		if ip.To4() == nil {
			bits = 128
		}
		out = append(out, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
	}
	return out, nil
}

// expandEnvRefs 展开字符串里的 `${VAR}` 环境变量引用（W5-37 凭据纪律：
// 上游 key 只进进程环境，配置文件只写引用名，值绝不落盘）。规则：
//   - 只识别 `${NAME}` 形态；单个 `$` 或 `$NAME`（无花括号）原样保留；
//   - 引用未定义 / 定义为空 / 未闭合 → 显式报错（绝不静默降级为空 key 打上游）；
//   - 报错只含变量名，绝不含已解析的值。
func expandEnvRefs(s string) (string, error) {
	if !strings.Contains(s, "${") {
		return s, nil
	}
	var b strings.Builder
	rest := s
	for {
		i := strings.Index(rest, "${")
		if i < 0 {
			b.WriteString(rest)
			return b.String(), nil
		}
		b.WriteString(rest[:i])
		end := strings.IndexByte(rest[i+2:], '}')
		if end < 0 {
			return "", errors.New("unterminated ${...} env reference")
		}
		name := rest[i+2 : i+2+end]
		if name == "" {
			return "", errors.New("empty env var name in ${}")
		}
		val, ok := os.LookupEnv(name)
		if !ok {
			return "", fmt.Errorf("env var %s is not set (referenced as ${%s})", name, name)
		}
		if val == "" {
			return "", fmt.Errorf("env var %s is empty (referenced as ${%s})", name, name)
		}
		b.WriteString(val)
		rest = rest[i+2+end+1:]
	}
}

// LoadConfig 读取并校验 YAML 配置（未知字段报错——配置错字不静默吞）。
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("service: read config: %w", err)
	}
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("service: parse config %s: %w", path, err)
	}
	// 凭据面环境变量引用（W5-37）：上游 `api_key` 支持 `${VAR}`——key 只活在
	// 进程环境里（如 `~/.omp/agent/.env` 运行时注入），配置文件与产物只留引用名。
	// W53-2：`credentials` 列表逐项同机制（同纪律——值绝不落盘）。
	for i := range c.Upstreams {
		k, err := expandEnvRefs(c.Upstreams[i].APIKey)
		if err != nil {
			name := c.Upstreams[i].Name
			if name == "" {
				name = fmt.Sprintf("#%d", i)
			}
			return nil, fmt.Errorf("service: upstream %s: api_key: %w", name, err)
		}
		c.Upstreams[i].APIKey = k
		for j := range c.Upstreams[i].Credentials {
			v, err := expandEnvRefs(c.Upstreams[i].Credentials[j])
			if err != nil {
				name := c.Upstreams[i].Name
				if name == "" {
					name = fmt.Sprintf("#%d", i)
				}
				return nil, fmt.Errorf("service: upstream %s: credentials[%d]: %w", name, j, err)
			}
			c.Upstreams[i].Credentials[j] = v
		}
	}
	// 公开服务面（P0-2）：`public.access_keys` 逐项同机制（准入 key 只活进程
	// 环境，配置只留引用名；错误只含变量名，绝不含已解析值）。
	if c.Public != nil {
		for j := range c.Public.AccessKeys {
			v, err := expandEnvRefs(c.Public.AccessKeys[j])
			if err != nil {
				return nil, fmt.Errorf("service: public.access_keys[%d]: %w", j, err)
			}
			c.Public.AccessKeys[j] = v
		}
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = DefaultListen
	}
	if c.HeartbeatSeconds <= 0 {
		c.HeartbeatSeconds = DefaultHeartbeatSeconds
	}
	if c.CooldownFailures <= 0 {
		c.CooldownFailures = DefaultCooldownFailures
	}
	if c.CooldownSeconds <= 0 {
		c.CooldownSeconds = DefaultCooldownSeconds
	}
}

// Validate 配置校验（显式报错，绝不静默降级）。
func (c *Config) Validate() error {
	// 管理面密钥（W5-33）：全空白串是配置错误——绝不静默降级为「未配」（那会把
	// 管理面 404 掉，与「配了但写错」的意图相反）。非空且非全空白 = 注册管理面。
	if c.AdminKey != "" && strings.TrimSpace(c.AdminKey) == "" {
		return errors.New("service: admin_key is blank (want a non-blank key, or omit the field to disable the admin surface)")
	}
	// 管理面日志 ring 容量（W5-36）：显式给出时必须 1..adminLogCapacityMax；未写 =
	// 缺省 256。0/负值/超上限都是配置错误（不静默降级——见字段注释）。
	if c.AdminLogCapacity != nil {
		if n := *c.AdminLogCapacity; n < 1 || n > adminLogCapacityMax {
			return fmt.Errorf("service: admin_log_capacity must be within 1..%d (got %d; omit the field for the default %d)",
				adminLogCapacityMax, n, DefaultAdminLogCapacity)
		}
	}
	// UI 托管面（W56-1）：rate_limit_per_min 显式给出时必须 ≥ 0（负值 = 配置
	// 错误，绝不静默降级为 0）；enabled 无值域问题。
	if c.UI != nil && c.UI.RateLimitPerMin < 0 {
		return fmt.Errorf("service: ui.rate_limit_per_min = %d (want >= 0; 0 = off)", c.UI.RateLimitPerMin)
	}
	// 响应卫生面（W104）：`response_hygiene.scrub_same_format_extras` 是布尔
	// 开关，无值域可校验（true/false 均合法，缺省 false）。语义边界见
	// ResponseHygieneSection：只影响**非流式同格式成功体**；流式/跨格式不变。
	// 持久化面（P1-1）：enabled=true 时 path 必填（空 = 配置错误——绝不静默降级
	// 为内存档，那会静默丢数据）；enabled=false 时给 path = 无意义配置（显式
	// 报错，与 public.access_keys 的歧义纪律同源）。
	if st := c.Store; st != nil {
		if st.Enabled && strings.TrimSpace(st.Path) == "" {
			return errors.New("service: store.path required when store.enabled is true (want a SQLite file path, e.g. data/any2api.db)")
		}
		if !st.Enabled && strings.TrimSpace(st.Path) != "" {
			return errors.New("service: store.path is only valid when store.enabled is true (memory tier would silently ignore it)")
		}
	}
	// 公开服务面（P0-1..4）：显式报错纪律同源（配错即报错，与 enabled 无关
	// ——绝不「打开开关才炸」）。
	if p := c.Public; p != nil {
		switch p.AccessMode {
		case "", publicAccessModeOpen, publicAccessModeGated:
		default:
			return fmt.Errorf("service: public.access_mode %q (want open|gated; empty = open)", p.AccessMode)
		}
		if p.AccessMode == publicAccessModeGated {
			if len(p.AccessKeys) == 0 {
				return errors.New("service: public.access_keys required when access_mode is gated")
			}
			for j, k := range p.AccessKeys {
				if strings.TrimSpace(k) == "" {
					return fmt.Errorf("service: public.access_keys[%d] is blank", j)
				}
			}
		} else if len(p.AccessKeys) > 0 {
			return errors.New("service: public.access_keys is only valid when access_mode is gated (open mode would silently ignore them)")
		}
		if _, err := parseTrustedProxies(p.TrustedProxies); err != nil {
			return fmt.Errorf("service: public.%w", err)
		}
		if p.MaxInflight < 0 {
			return fmt.Errorf("service: public.max_inflight = %d (want >= 0; 0 = default %d)", p.MaxInflight, DefaultPublicMaxInflight)
		}
		if p.QueueSize < 0 {
			return fmt.Errorf("service: public.queue_size = %d (want >= 0; 0 = default %d)", p.QueueSize, DefaultPublicQueueSize)
		}
		if p.QueueTimeoutSeconds < 0 {
			return fmt.Errorf("service: public.queue_timeout_seconds = %d (want >= 0; 0 = default %ds)", p.QueueTimeoutSeconds, int(DefaultPublicQueueTimeout/time.Second))
		}
		if p.CredentialMinGapSeconds != nil && *p.CredentialMinGapSeconds < 0 {
			return fmt.Errorf("service: public.credential_min_gap_seconds = %d (want >= 0; 0 = off)", *p.CredentialMinGapSeconds)
		}
		// P0-5/P0-8 新面（错字/越界 = 启动报错——配置纪律；显式 0 = 关闭语义
		// 见各字段注释）。
		if p.MaxCostClass != "" {
			if _, ok := parseCostClassName(p.MaxCostClass); !ok {
				return fmt.Errorf("service: public.max_cost_class %q (want light|medium|heavy|max)", p.MaxCostClass)
			}
		}
		if p.DailyBudgetUnits != nil && *p.DailyBudgetUnits < 0 {
			return fmt.Errorf("service: public.daily_budget_units = %d (want >= 0; 0 = off)", *p.DailyBudgetUnits)
		}
		if p.MaxBodyBytes != nil && *p.MaxBodyBytes < 0 {
			return fmt.Errorf("service: public.max_body_bytes = %d (want >= 0; 0 = use global 32MB)", *p.MaxBodyBytes)
		}
		if p.MaxMessages != nil && *p.MaxMessages < 0 {
			return fmt.Errorf("service: public.max_messages = %d (want >= 0; 0 = off)", *p.MaxMessages)
		}
		if p.MaxMessageChars != nil && *p.MaxMessageChars < 0 {
			return fmt.Errorf("service: public.max_message_chars = %d (want >= 0; 0 = off)", *p.MaxMessageChars)
		}
	}
	if len(c.Upstreams) == 0 {
		return errors.New("service: at least one upstream is required")
	}
	for i, u := range c.Upstreams {
		name := u.Name
		if name == "" {
			name = fmt.Sprintf("#%d", i)
		}
		if u.BaseURL == "" && u.Protocol != ProtocolClaudeWeb && u.Protocol != ProtocolChatGPTWeb {
			return fmt.Errorf("service: upstream %s: base_url required", name)
		}
		// claude_web 的 base_url 缺省 = https://claude.ai（W10）；chatgpt_web 的
		// base_url 缺省 = https://android.chat.openai.com（W30-2；其余协议必填）。
		if u.BaseURL != "" && !strings.HasPrefix(u.BaseURL, "http://") && !strings.HasPrefix(u.BaseURL, "https://") {
			return fmt.Errorf("service: upstream %s: base_url must start with http:// or https://", name)
		}
		switch u.Protocol {
		case ProtocolAnthropic, ProtocolOpenAIChat, ProtocolGemini, ProtocolResponses, ProtocolInteractions, ProtocolClaudeWeb, ProtocolChatGPTWeb:
		default:
			return fmt.Errorf("service: upstream %s: unknown protocol %q (want anthropic|openai_chat|gemini|responses|interactions|claude_web|chatgpt_web)", name, u.Protocol)
		}
		// 凭据池（W53-2）：`credentials` 显式给出时必须至少 1 项、逐项非空白；
		// 与 `api_key` 互斥（两处给凭据 = 歧义——用 credentials 表达池）；池接线
		// 目前只落在 claude_web 面，其余协议给出该字段 = 配置错误（显式报错，
		// 绝不静默忽略）。
		if u.Credentials != nil && len(u.Credentials) == 0 {
			return fmt.Errorf("service: upstream %s: credentials must contain at least one entry (omit the field for api_key-only)", name)
		}
		if len(u.Credentials) > 0 {
			if u.APIKey != "" {
				return fmt.Errorf("service: upstream %s: api_key and credentials are mutually exclusive (use credentials for a pool)", name)
			}
			if u.Protocol != ProtocolClaudeWeb {
				return fmt.Errorf("service: upstream %s: credentials is only supported for protocol claude_web (got %s)", name, u.Protocol)
			}
			for j, cred := range u.Credentials {
				if strings.TrimSpace(cred) == "" {
					return fmt.Errorf("service: upstream %s: credentials[%d] is blank", name, j)
				}
			}
		}
		// claude_web 的 org_id（可选）：显式给出时必须是非空、无路径注入字符的
		// UUID 形态（它会进 URL 路径段——GW 陷阱 13 同款守卫；空 = 现取）。
		if u.Protocol == ProtocolClaudeWeb && u.OrgID != "" {
			if !isSafePathSegment(u.OrgID) {
				return fmt.Errorf("service: upstream %s: org_id %q is not a safe uuid path segment", name, u.OrgID)
			}
		}
		// chatgpt_web 的 device_id（可选）：显式给出时必须是安全的头值（可见
		// ASCII、无空白/控制字符、≤64 字节——头注入守卫；空 = 每请求现生成）。
		if u.Protocol == ProtocolChatGPTWeb && u.DeviceID != "" {
			if err := validateChatGPTWebDeviceID(u.DeviceID); err != nil {
				return fmt.Errorf("service: upstream %s: device_id: %w", name, err)
			}
		}
		// 出站代理（W11；可选）：非法值显式报错（绝不静默直连——配了代理就必须
		// 走代理）。错误消息不回显 URL 原文（可能含 userinfo 凭据）。
		if _, err := parseUpstreamProxy(u.Proxy); err != nil {
			return fmt.Errorf("service: upstream %s: proxy: %w", name, err)
		}
		// 出站 TLS 指纹（W11；可选）：只认 chrome|none，空 = 缺省 **none**
		// （**全部协议**——与实现同口径，见 outbound.go tlsFingerprintMode）。
		switch u.TLSFingerprint {
		case "", fingerprintNone, fingerprintChrome:
		default:
			return fmt.Errorf("service: upstream %s: tls_fingerprint %q (want chrome|none; empty = protocol default)", name, u.TLSFingerprint)
		}
		// 任务面能力（W27-1 / Slice 5；可选）：只认 managed|passthrough|off，
		// 空 = 缺省 managed（提案原文缺省）。passthrough 仅 responses 协议合法
		// （上游任务端点族 = OpenAI 类 `POST/GET /v1/responses` + `/{id}/cancel`；
		// 其余协议无任务端点可直连——显式报错，绝不静默降级为 managed）。
		switch u.TaskFace {
		case "", taskFaceManaged, taskFacePassthrough, taskFaceOff:
		default:
			return fmt.Errorf("service: upstream %s: task_face %q (want managed|passthrough|off; empty = managed)", name, u.TaskFace)
		}
		if u.TaskFace == taskFacePassthrough && u.Protocol != ProtocolResponses {
			return fmt.Errorf("service: upstream %s: task_face passthrough requires protocol responses (got %s)", name, u.Protocol)
		}
	}
	// per-exception 换上游预算（W5-22 条目 3）：键必须为已知异常类、值 ≥ -1。
	for k, v := range c.FailoverPolicy {
		if !validFailoverClass(k) {
			return fmt.Errorf("service: failover_policy: unknown class %q (want %s)", k, failoverClassNames)
		}
		if v < -1 {
			return fmt.Errorf("service: failover_policy[%s] = %d (want -1=unlimited, 0=never, N>0=max failovers)", k, v)
		}
	}
	// 失败率冷却（W5-22 条目 3）：参数面校验（显式报错，绝不静默降级）。
	if fr := c.FailureRate; fr != nil {
		if fr.MinRequests < 0 {
			return fmt.Errorf("service: failure_rate_cooldown.min_requests = %d (want >= 0)", fr.MinRequests)
		}
		if fr.Threshold < 0 || fr.Threshold > 1 {
			return fmt.Errorf("service: failure_rate_cooldown.threshold = %v (want 0 < threshold <= 1)", fr.Threshold)
		}
		if fr.WindowSeconds < 0 {
			return fmt.Errorf("service: failure_rate_cooldown.window_seconds = %d (want >= 0)", fr.WindowSeconds)
		}
	}
	// 同上游重试（W5-32 条目 1）：次数 0..3（retrySameUpstreamMax；0 = 关）；
	// 延迟序列逐项 > 0 且不超过上限（60s）。
	if c.RetrySameUpstream < 0 || c.RetrySameUpstream > retrySameUpstreamMax {
		return fmt.Errorf("service: retry_same_upstream = %d (want 0..%d; 0 = off)", c.RetrySameUpstream, retrySameUpstreamMax)
	}
	for i, ms := range c.RetrySameUpstreamDelaysMs {
		if ms <= 0 || time.Duration(ms)*time.Millisecond > retrySameUpstreamCap {
			return fmt.Errorf("service: retry_same_upstream_delays_ms[%d] = %d (want 1..%d)", i, ms, retrySameUpstreamCap/time.Millisecond)
		}
	}
	// 冷却阶梯（W5-32 条目 2）：非空时必须逐档严格递增、每档 > 0（秒）。
	for i, s := range c.CooldownLadder {
		if s <= 0 {
			return fmt.Errorf("service: cooldown_ladder[%d] = %d (want > 0 seconds)", i, s)
		}
		if i > 0 && s <= c.CooldownLadder[i-1] {
			return fmt.Errorf("service: cooldown_ladder must be strictly increasing: [%d]=%d <= [%d]=%d", i, s, i-1, c.CooldownLadder[i-1])
		}
	}
	// 能力面降级开关（W36-2 / S8；可选）：只认 off|auto，空 = off（缺省——
	// 声明是唯一路由权威）。显式非法值报错，绝不静默降级语义。
	switch c.TaskFaceDegrade {
	case "", taskFaceDegradeOff, taskFaceDegradeAuto:
	default:
		return fmt.Errorf("service: task_face_degrade %q (want off|auto; empty = off)", c.TaskFaceDegrade)
	}
	// 模型表（W5-28 条目 1）：id 必填且唯一；上限字段非负（0 = 不下发）；
	// `upstreams` 白名单（A2A-L19-W81-2）：名字必须精确命中 `upstreams[].name`
	//（env 展开已在 LoadConfig 完成）、同表不得重复、空 = 不限制。
	upstreamNames := make(map[string]bool, len(c.Upstreams))
	for _, u := range c.Upstreams {
		upstreamNames[u.Name] = true
	}
	seenModel := make(map[string]bool, len(c.Models))
	for i, m := range c.Models {
		if strings.TrimSpace(m.ID) == "" {
			return fmt.Errorf("service: models[%d]: id required", i)
		}
		if seenModel[m.ID] {
			return fmt.Errorf("service: models: duplicate id %q", m.ID)
		}
		seenModel[m.ID] = true
		if m.MaxInputTokens < 0 || m.MaxTokens < 0 {
			return fmt.Errorf("service: models[%s]: max_input_tokens/max_tokens must be >= 0 (0 = omit)", m.ID)
		}
		seenUpstream := make(map[string]bool, len(m.Upstreams))
		for j, name := range m.Upstreams {
			if strings.TrimSpace(name) == "" {
				return fmt.Errorf("service: models[%s].upstreams[%d] is blank (want a name from upstreams[])", m.ID, j)
			}
			if !upstreamNames[name] {
				return fmt.Errorf("service: models[%s].upstreams[%d]: unknown upstream %q (want a name from upstreams[])", m.ID, j, name)
			}
			if seenUpstream[name] {
				return fmt.Errorf("service: models[%s].upstreams: duplicate upstream %q", m.ID, name)
			}
			seenUpstream[name] = true
		}
	}
	return nil
}

// upstreamModelFor 请求面模型名解析（A6 别名映射，W5-28 条目 1）：命中配置表且
// 声明了 `upstream_model` → 返回上游模型名；未配置 / 未命中 / 未声明 → 原样返回
// （透传——缺省行为不变；未知模型不拒绝，保持既有宽容面）。
func (c *Config) upstreamModelFor(id string) string {
	for _, m := range c.Models {
		if m.ID == id && m.UpstreamModel != "" {
			return m.UpstreamModel
		}
	}
	return id
}

// modelUpstreams 模型 → 上游白名单（A2A-L19-W81-2；`models[].upstreams`）：
// 按 ModelEntry.ID 精确匹配。未配置 / 未命中 / 列表为空 → nil（不限制——缺省
// 行为与引入前逐字一致）；非空 → 返回名字集合（调用方按 Upstreams[].Name 过滤）。
// nil 安全。**注意匹配键是请求面模型 id**：A6 的 upstream_model 改写发生在
// serve 的候选链之前，故调用方须传改写前的模型名（见 server.go 的 routeModel）。
func (c *Config) modelUpstreams(model string) map[string]bool {
	if c == nil || model == "" {
		return nil
	}
	for i := range c.Models {
		m := &c.Models[i]
		if m.ID != model {
			continue
		}
		if len(m.Upstreams) == 0 {
			return nil
		}
		set := make(map[string]bool, len(m.Upstreams))
		for _, name := range m.Upstreams {
			set[name] = true
		}
		return set
	}
	return nil
}

// Heartbeat 心跳间隔。
func (c *Config) Heartbeat() time.Duration {
	return time.Duration(c.HeartbeatSeconds) * time.Second
}

// FirstByteTimeout 首产出超时（0 = 关；见 Config.FirstByteTimeoutSeconds）。
func (c *Config) FirstByteTimeout() time.Duration {
	if c.FirstByteTimeoutSeconds <= 0 {
		return 0
	}
	return time.Duration(c.FirstByteTimeoutSeconds) * time.Second
}

// UpstreamIdleTimeout 上游内容静默超时（0 = 关；见 Config.UpstreamIdleTimeoutSeconds）。
func (c *Config) UpstreamIdleTimeout() time.Duration {
	if c.UpstreamIdleTimeoutSeconds <= 0 {
		return 0
	}
	return time.Duration(c.UpstreamIdleTimeoutSeconds) * time.Second
}
