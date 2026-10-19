// Package claude_web 实现 claude.ai 网页线（网页端 completion SSE）与 IR 的双向
// 转换：parse = 上游线格式 → IR；render = IR → 上游请求体。
//
// **上游叶子格式**：claude_web 只作上游（服务壳把下游入口请求渲染成 claude.ai
// 体、把上游 completion SSE 解析成 IR），不作下游入口/出口——故本包的出口面
// （renderStream / renderResponse）与入口面（parseRequest）是显式报错占位。
//
// 依据：docs/architecture.md（W9-1 源码级互证，7 实现）
// + W9-2 实抓样本（4 条完整 completion SSE 流 + 请求体；`tmp/lanes/W9-claude-live/
// sample/`，脱敏副本见本包 testdata/）。表内 do-not-copy 项一律按表、不按任一参考
// 实现：
//
//   - **帧序**（4/4 流一致，W9-2 §2.3）：`ping`×7–9 → `conversation_ready`
//     （仅新会话首回合）→ `message_start` → `content_block_start/delta/stop`* →
//     `message_delta`（stop_reason）→ `message_limit`（配额）→ `message_stop`。
//   - **`event:` 名 = `data.type`**（两处一致，4/4 流）——解析侧双源核对，不一致
//     显式报错（interactions Q6 同款加固），绝不静默按一方分派。
//   - **JSON 尾部不定长空格 padding**（`{"type":"ping"   }`，帧内 + 帧尾两处）——
//     解析前 trim；对 padding 敏感的匹配（前缀/后缀相等）一律不做。
//   - **终止是命名事件 `message_stop`，不是 `[DONE]`**（上游从不发哨兵；哨兵是
//     各实现自己向下游补的）。真实上游在 `message_stop` 后**不主动关闭连接**
//     （W9-2 §2.5：流以客户端 abort 结束）——服务壳必须以终止帧收尾，不能等 EOF。
//   - **`message_limit` 是网页线唯一的真 usage 面**：`windows{5h,7d}`
//     {status,utilization,resets_at} + `resolved.limit`——无 token 数（G7 缺口），
//     本包按「原样保真进 Usage.Extra + 派生摘要」映射，绝不发明 token 计数。
//   - **思考是首类流事件**（`content_block_start(type=thinking)` + `thinking_delta`）
//     ——W9-2 实抓 4 流均未触发 thinking（短题；缺口），帧面按 W9-1 源码级帧集
//     （5 实现互证）+ `signature_delta` 容忍实现。
//   - **请求体字段集以实抓为准**（12.3–12.8 KB，W9-2 §2.2）：`prompt/timezone/
//     locale/model/effort/thinking_mode/tools/turn_message_uuids/attachments/
//     files/sync_sources/completion_request_id/rendering_mode/
//     create_conversation_params`（首回合）；第二回合起 `parent_message_uuid`
//     取代 `create_conversation_params`。**上游对多余字段报错而非忽略**
//     （`max_tokens` → `Extra inputs are not permitted`，KB §陷阱 2）——渲染器
//     只出白名单字段，未知 Extra 一律不过桥。
package claude_web

// ---------------------------------------------------------------------------
// SSE 事件名（= data.type；W9-2 §2.3 逐帧表）
// ---------------------------------------------------------------------------

const (
	evPing              = "ping"               // 保活（JSON 带不定长尾部空格 padding）
	evConversationReady = "conversation_ready" // 新会话首回合就绪信号（无载荷）
	evMessageStart      = "message_start"
	evContentBlockStart = "content_block_start"
	evContentBlockDelta = "content_block_delta"
	evContentBlockStop  = "content_block_stop"
	evMessageDelta      = "message_delta" // delta.stop_reason
	evMessageLimit      = "message_limit" // 配额快照（windows/utilization/resets_at）
	evMessageStop       = "message_stop"  // 终止帧（非 [DONE]）
	evError             = "error"         // HTTP 200 下的流内错误
)

// delta 子类型（content_block_delta.delta.type；W10-3 实抓 golden 为准）。
const (
	deltaText            = "text_delta"             // 正文增量（唯一正文来源）
	deltaThinkingSummary = "thinking_summary_delta" // 思考**摘要**增量（W10-3 实抓形态）
	deltaThinking        = "thinking_delta"         // 思考全量增量（W9-1 主题表；实抓未见，容忍）
	deltaSignature       = "signature_delta"        // 思考签名增量（W9-1 主题表；实抓未见，容忍）
	deltaInputJSON       = "input_json_delta"       // 结构化增量（上游内置工具调用参数）
)

// 块类型（content_block_start.content_block.type；W10-3 实抓 = text/thinking/
// tool_use/tool_result；未建模值走 BlockUnknown 保真旁路，不炸）。
const (
	blockText          = "text"
	blockThinking      = "thinking"
	blockToolUse       = "tool_use"
	blockToolResult    = "tool_result"
	blockServerToolUse = "server_tool_use"
)

// 配额面常量（message_limit.message_limit）。
//
// 词汇表（W43-3 §4.3 R5 修复：CWK 源码词汇与实抓语料值域不一致）：
//   - 顶层 type：`within_limit`（实抓 32/33）| `exceeded_limit`（实抓 1/33，w28）|
//     `hit_limit`（CWK 源码词汇，实抓未见）；
//   - 窗口 status：`within_limit` | `exceeded_limit`（实抓两处同值）| `over_limit`
//     （CWK 源码词汇，实抓未见）。
//
// 硬限判定口径见 summarize（「非安全即硬限」，不再枚举硬限值）。
const (
	// limitWithin 未超限（顶层 type 与窗口 status 共用同一字面量）。
	limitWithin = "within_limit"
	// limitHit / limitExceeded 已知硬限词汇（CWK 源码 / W28-2 实抓语料各一）。
	limitHit      = "hit_limit"
	limitExceeded = "exceeded_limit"
	// windowStatusOverLimit 单窗口超限标记（CWK 源码词汇，实抓未见）。
	windowStatusOverLimit = "over_limit"
)

// ---------------------------------------------------------------------------
// 协议头面常量（KB §鉴权；BK headers.go:7-13 / RF / CWK 三方同值互证）
// ---------------------------------------------------------------------------

// HeaderClientPlatform 等为 claude.ai 的协议常量头：`anthropic-client-platform`
// 五实现一致（web_claude_ai）；`anthropic-client-sha` 的 SHA 三方同值互证。
const (
	HeaderClientPlatform = "anthropic-client-platform"
	ClientPlatform       = "web_claude_ai"
	HeaderClientVersion  = "anthropic-client-version"
	ClientVersion        = "1.0.0"
	HeaderClientSHA      = "anthropic-client-sha"
	// ClientSHA claude.ai web 应用构建 sha。**W43-2 浏览器实测新值**（2026-10-15
	// 真实浏览器会话；W9-2 实抓的 `e640a19a…` 已过时——旧值当日直连仍 200，但头面
	// 新鲜度按实况对齐，见 reports/A2A-L11-W43-2.md §4 + A2A-L19-W48-1 §C）。
	ClientSHA = "3d8a03e3010c5025c6b2d997970df8ffbd9ce966"
	// HeaderAnonymousID / HeaderDeviceID 是账号级稳定身份（BK/RF 用 uuid5 命名空间
	// 派生；服务壳按 seed 派生——同一凭据跨请求身份不变）。
	HeaderAnonymousID = "anthropic-anonymous-id"
	HeaderDeviceID    = "anthropic-device-id"
	// CookieSessionKey 主凭据 cookie 名（值 = `sk-ant-sid0*` 形态的 sessionKey）。
	CookieSessionKey = "sessionKey"
)

// ---------------------------------------------------------------------------
// 请求面常量（W9-2 §2.2/§2.4 实抓）
// ---------------------------------------------------------------------------

// 缺省值（实抓体未覆盖/可变字段的缺省；逐条注明来源）。
const (
	// DefaultTimezone / DefaultLocale：实抓为浏览器本地值（Asia/Tokyo + ja-JP）；
	// 服务端无浏览器 locale → 取 CWK 注释的 `UTC` / `en-US`（CWK client.py:713-745）。
	DefaultTimezone = "UTC"
	DefaultLocale   = "en-US"
	// DefaultEffort 缺省档位（实抓 run1/run2 = medium；UI 5 档 = 低/中/高/超高/最大
	// → low/medium/high/xhigh/max，run3 精确点击「高」= effort:"high"，W9-2 §2.4）。
	DefaultEffort = "medium"
	// ThinkingModeAuto / ThinkingModeNone：实抓恒 `auto`；`none` 由 Claude Code 的
	// `thinking{type:disabled}` 映射（AU handlers/common.go:63-83）。
	ThinkingModeAuto = "auto"
	ThinkingModeNone = "none"
	// RenderingMode 实抓恒 `messages`（KB §③ 四实现一致）。
	RenderingMode = "messages"
	// ToolSearchMode / ChatMemoryMode 实抓 create_conversation_params 原值。
	ToolSearchMode = "auto"
	ChatMemoryMode = "enabled"
)

// webTools claude.ai 网页端内置工具三元组（YS core/api.go:89-96 / BK claude.go:337-341 /
// CWK client.py:33-45 三方同形；CW core/api.go:131-135 只有 web_search）。
//
// 与实抓的差别：浏览器会话下发的是**账号级 MCP 工具定义**（33 个，含
// `mcp_server_uuid`/`mcp_server_url`/`integration_name` 等账号绑定字段，W9-2 §2.2）
// ——不可复现（uuid 属账号状态）；三元组是跨实现互证的最小可复现面，本 slice 按它
// 渲染（工具面字段名与实抓体一致：type/name）。
var webTools = []map[string]string{
	{"type": "web_search_v0", "name": "web_search"},
	{"type": "artifacts_v0", "name": "artifacts"},
	{"type": "repl_v0", "name": "repl"},
}
