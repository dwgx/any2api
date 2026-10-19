// Package chatgpt_web 实现 chatgpt.com 网页线（completion SSE）到 IR 的解析
// （A2A-L19-W28-1 切片 S2：**只做 parse 侧**——不注册 registry、不接 service、
// 不渲染请求体；prepare/conduit 链、Sentinel 求解器、指纹头属 S3/S4）。
//
// **上游叶子格式**：chatgpt_web 只作上游（服务壳把下游入口请求渲染成 chatgpt
// 体、把上游 completion SSE 解析成 IR）。本 slice 的出口面（请求渲染）与
// 入口面（parseRequest）不存在——S3 接线时补。
//
// 依据：docs/architecture.md（W6 深挖 `744737d` + W26-3 实现
// 可行性）。**三方言对照（A/B/C 三实现 + 两个佐证源；克隆在
// `tmp/lanes/W6-chatgpt-web/repos/`，只读）**：
//
//	A = DestroyerDarkNess/chatgpt_web_python_api@76e0731（Py，登录档；唯一接思考）
//	    web_client.py:205-252（SSE 切帧 + 双重编码容错）、:774-921（元数据/快照
//	    差分/事件分流）、upstream.py:60-174（思考控制串清洗 + reasoning 提取）
//	B = jiji262/gpt2api@956b46a（Go；v1 patch 解析器 + thoughts 路径）
//	    internal/gateway/delta.go:14-36（注释：curP 继承/recipient/thoughts/四判据）、
//	    :66-69（非 JSON 心跳忽略）、:71-74（message_stream_complete）、:81-137
//	    （thoughts 前缀 / 字符串增量 / 数组 patch）、:139-193（对象首帧 / 旧快照）
//	C = lanqian528/chat2api@85971f0（Py；旧快照为主）
//	    chatgpt/chatFormat.py:129-340（content_type 分支 / tool 消息 / moderation）
//	F = adam-s/toolkit@4c03e3e（TS；浏览器内）openai-adapter.ts:105-130（v1 档
//	    检测）、:186-196（简写帧复用 lastPath/lastOp——「不维护会丢 ~90% 正文」）、
//	    :236-262（patch 应用）
//	L = lowkruc/chatgpt2api docs/upstream-sse-conversation.md（实现方协议文档，
//	    落盘 `tmp/lanes/W6-chatgpt-web/lowkruc-sse.md`）
//
// 帧面（KB §流式细节 §4 六类帧形，同流混用）：
//
//	①a 显式 patch {"p","o","v"}（F:186-196；B:93-106；L 帧形状表）
//	①b 简写帧 {"v":s}——复用上一条 p（B:14-17 的 curP 继承；F 的 lastPath/lastOp）
//	①c 批量 patch {"p":"","o":"patch","v":[{p,o,v},…]}——数组内按序作用，结束
//	   标记常夹带在此（F:196-205；B:107-137；L「Text increment case」样例）
//	①d 对象首帧 {"v":{"message":{…},"conversation_id":"…"}}（B:139-166）
//	②  累积快照 {"message":{…}}（旧协议，仍兼容——B:168-193；C:157-186；A:802-836）
//	③  类型化事件 {"type":…}（A:838-921 + B:71-74）
//	④  data: [DONE]（全实现）
//	⑤  非 JSON 行/注释行——心跳，忽略（B:66-69；A:213-214 的 `:` 注释行）
//
// 四件（本包结构，对照 brief §Change ①）：
//
//  1. 切帧 frame.go：SSE 行切帧 + 双重编码容错（A:205-252：_iter_sse /
//     _parse_encoded_stream_item / _decode_event_payload）+ 心跳（非 JSON）忽略；
//  2. patch 状态机 patch.go：curP 继承 / recipient / thoughts 前缀（思考）/
//     四种结束判据 / 字符串增量 / 数组 patch / 对象首帧 / 旧快照兼容（逐条对照
//     B delta.go:14-36,81-137,139-193）；
//  3. 事件分流 dispatch.go：error / title / resume_conversation_token /
//     stream_handoff / patch（A:838-921）+ message_stream_complete（B:71-74）；
//  4. IR 映射 parse.go：text 增量 / thinking（thoughts 路径 B:81-85 + reasoning
//     类路径 A:94-160）/ 结束判据 → StopReason；tool 消息面（C:129-340 的
//     content_type 分支）最小映射（无载体面显式报错）。
//
// 关键取舍（与参考实现的差异，逐条注明理由；golden 帧到位后 S4/S5 重校）：
//
//   - **思考默认映射**（A 语义，B 默认丢弃）：thoughts 路径增量（B:81-85 的
//     `HasPrefix(curP, "/message/content/thoughts")`）+ reasoning 类路径/字段
//     （A upstream.py:117-160：reasoning_content/reasoning_delta/thought）→ IR
//     thinking 块；控制串（reasoning_started/hide_all/…，A:60-92）必须清洗——
//     直接拼接会污染输出。出口按各自约定转 reasoning_content（openai 直转面）。
//   - **历史回放过滤**以 CF worker（chatgpt2api-cf stream.ts:103-118）的
//     live-message 判据为准：B 的状态结束判据会把历史回放帧误判为终止（两实现
//     直接矛盾）——本包以 live 上下文门控 B 的状态判据（见 patch.go 的
//     contextSuppressed 注）。三实现判据合成：CF（id + message_type + channel）+
//     Go request.go:444-446（message_type）+ C:201-202（id 切换）。
//   - **快照差分** = B 前缀差分（:172-179）+ A 的回退守卫（:828-834：text 短于
//     基线/相等时不重发）；C 无守卫（盲切 len_last_content）。
//   - **结束判据** = B 四判据（:29-36）+ CF 的 end_turn 补充信号（stream.ts:122-128）。
//     C 更严（finished_successfully 必须配合 end_turn 才收尾，:270-292）——差异
//     记录，golden 帧裁定。**三选一都可能先到，幂等收束**（F:341-357）。
//   - **tool 消息面** = 抑制 + 结构化记录（ToolMessage；A/B/Go/gpt2api 四实现均
//     不把 recipient != all 的消息当正文输出）+ 无载体 part 显式报错
//     （ErrUnsupportedContent——claude_web 的 ErrAttachmentCarrier 同旨）。
//   - **usage 面无**：上游从不给 token 数（KB §陷阱「usage 全为估算」）——本包
//     绝不合成 usage（message_delta 不带 Usage）。
package chatgpt_web

import "encoding/json"

// ---------------------------------------------------------------------------
// 常量（帧面字面量；每条带证据）
// ---------------------------------------------------------------------------

const (
	// evDeltaEncoding v1 档标记帧的 event 名（F openai-adapter.ts:105-130：
	// `event: delta_encoding` + `data: "v1"`）。
	evDeltaEncoding = "delta_encoding"
	// markerV1 协议版本标记值（F:105-130；L 帧形状表）。
	markerV1 = "v1"
	// sentinelDone 流终止哨兵（全实现；KB §4 ④）。
	sentinelDone = "[DONE]"

	// patch op（F:236-262；L 文档「Key points」表）。
	opAdd     = "add"
	opAppend  = "append"
	opReplace = "replace"
	opRemove  = "remove"
	opPatch   = "patch"

	// 类型化事件（③；A web_client.py:838-921 + B delta.go:71-74 + L 事件表）。
	typeMessageStreamComplete   = "message_stream_complete"
	typeTitleGeneration         = "title_generation"
	typeResumeConversationToken = "resume_conversation_token"
	typeStreamHandoff           = "stream_handoff"
	typeServerSteMetadata       = "server_ste_metadata"
	typeModeration              = "moderation"
	// typeConversationDetailMetadata 会话级详情事件（W29-1 §2.2 帧 20 实抓；
	// 字段在帧顶层）——只记录（D4）。
	typeConversationDetailMetadata = "conversation_detail_metadata"
	typeInputMessage               = "input_message"
	typeMessageMarker              = "message_marker"

	// message.author.role（KB §message 字段表）。
	roleAssistant = "assistant"
	roleTool      = "tool"

	// message.status（KB §message 字段表；B:95-100；A:818-821）。
	statusInProgress           = "in_progress"
	statusFinishedSuccessfully = "finished_successfully"
	statusFinished             = "finished"

	// message.channel（A:826-827；CF stream.ts:107-108）。
	channelFinal      = "final"
	channelCommentary = "commentary"

	// message.metadata.message_type（历史回放判据之一；CF:110-114；Go:444-446）。
	messageTypeNext = "next"

	// content.content_type（KB §message 字段表 + C chatFormat.py:129-340 分支）。
	ctText                 = "text"
	ctMultimodalText       = "multimodal_text"
	ctCode                 = "code"
	ctExecutionOutput      = "execution_output"
	ctModelEditableContext = "model_editable_context"
	// ctReasoningRecap 思考回顾消息（W29-1 §2.2 帧 8 实抓：`content_type:
	// "reasoning_recap"`、`content` 为字符串、`status:"finished_successfully"`、
	// `end_turn:false`）。**不是终态**——它是「思考已结束」的 UI 回顾条，其后才是
	// 正文（W29-1 D1）。
	ctReasoningRecap = "reasoning_recap"

	// patch 路径（B:81-137 + L 文档）。
	pathParts0     = "/message/content/parts/0"
	pathStatus     = "/message/status"
	pathEndTurn    = "/message/end_turn"
	pathMetadata   = "/message/metadata"
	prefixThoughts = "/message/content/thoughts"

	// finish_details.type（Go request.go:526-532；A web_client.py:786-791）。
	finishMaxTokens = "max_tokens"

	// asset_pointer scheme（L 文档「Image pointer types」表）。
	schemeFileService = "file-service://"
	schemeSediment    = "sediment://"
	schemeFileUpload  = "file_upload"

	// maxDecodeDepth DecodePayload 的最大解层数（A:231-252 只解一层嵌套 +
	// :226-228 的内嵌 SSE 项；本包给一个硬上限防循环）。
	maxDecodeDepth = 4
	// maxToolRecords ToolMessage 记录的条数上限（防长会话内存无界；溢出计数）。
	maxToolRecords = 128
	// maxMarkerRecords message_marker 帧的登记条数上限（防畸形流内存无界；
	// 溢出计数）。真机流一回合 ~4 条（W29-1/W31-1 实抓）。
	maxMarkerRecords = 128
)

// ---------------------------------------------------------------------------
// 帧体 wire（只声明本包读取的字段；未知字段忽略——版本策略）
// ---------------------------------------------------------------------------

// patchFrameWire 顶层载荷的读取面（①a/①c/①d/②/③ 共用；形状判定见 dispatch.go）。
type patchFrameWire struct {
	// ①a/①c：显式 patch 字段。
	P string          `json:"p"`
	O string          `json:"o"`
	V json.RawMessage `json:"v"`
	// ①d 消息帧/patch 帧的递增计数器 `c`（W29-1 §4.2 D3 实抓：v1 档消息帧
	// `c:0..6` 逐帧 +1）——本包只观测（丢帧/乱序诊断，见 PatchCounter*）。
	C json.RawMessage `json:"c"`
	// ③：类型化事件判别式。
	Type string `json:"type"`
	// ②：顶层 message（旧协议快照帧）。
	Message json.RawMessage `json:"message"`
	// ③ 载荷。
	Error          json.RawMessage `json:"error"`
	ConversationID string          `json:"conversation_id"`
	Title          string          `json:"title"`
	Token          string          `json:"token"`
	Options        json.RawMessage `json:"options"`
	Metadata       json.RawMessage `json:"metadata"`
	Moderation     json.RawMessage `json:"moderation_response"`
	// ③ message_marker 载荷（W31-1 §4 尾部帧面）：
	// `{"type":"message_marker","message_id":"…","marker":"last_token","event":"last"}`。
	Marker    string `json:"marker"`
	Event     string `json:"event"`
	MessageID string `json:"message_id"`
}

// messageWire 上游 message 对象（①d/② 共用；KB §message 快照字段表 17 字段，
// 本包只声明消费面；其余字段忽略）。
type messageWire struct {
	ID     string `json:"id"`
	Author struct {
		Role string `json:"role"`
	} `json:"author"`
	// Recipient 只有 "all" 是给用户的正文，其余是工具调用（B:18-20；Go:441-443）。
	Recipient string `json:"recipient"`
	// Channel final/commentary（A:826-827：commentary 不输出）。
	Channel string `json:"channel"`
	// Status in_progress 才有增量；finished_successfully 收尾。
	Status string `json:"status"`
	// EndTurn 真结束信号（配合 status；CF:122-128）。
	EndTurn bool `json:"end_turn"`
	Content struct {
		ContentType string            `json:"content_type"`
		Parts       []json.RawMessage `json:"parts"`
		Text        string            `json:"text"`
		Language    string            `json:"language"`
	} `json:"content"`
	Metadata struct {
		ModelSlug     string `json:"model_slug"`
		MessageType   string `json:"message_type"`
		InitialText   string `json:"initial_text"`
		AsyncTaskType string `json:"async_task_type"`
		FinishDetails struct {
			Type string `json:"type"`
		} `json:"finish_details"`
		// 思考回顾标记（W29-1 §2.2 帧 8 实抓：`reasoning_status:"reasoning_ended"`
		// + `reasoning_recap_type:"hide_all"`）——recap 消息的判据之一（W29-1 D1）。
		ReasoningStatus    string `json:"reasoning_status"`
		ReasoningRecapType string `json:"reasoning_recap_type"`
	} `json:"metadata"`
}

// textParts 消息 parts 里的字符串连接（非字符串 part 不在此面；KB：文本取
// parts[0]/join）。
func (m *messageWire) textParts() string {
	out := ""
	for _, raw := range m.Content.Parts {
		if s, ok := jsonString(raw); ok {
			out += s
		}
	}
	return out
}

// HandoffInfo stream_handoff 事件记录（A:857-868）。
//
// 语义（KB §类型化事件表 + A:922-1054）：上游把流甩给另一条通道——options 里
// `subscribe_ws_topic` → WS 主题，`resume_sse_endpoint` → SSE 续接端点；
// 无 resume_sse_endpoint 时实现直接报错（A 的 _resume_handoff）。**续接动作属
// 服务壳（S3）**：本包只记录，不产 IR 事件（IR 无该载体）。
type HandoffInfo struct {
	ConversationID string
	WSTopicID      string // subscribe_ws_topic 的 topic_id
	SSETopicID     string // resume_sse_endpoint 的 topic_id
}

// ModerationInfo moderation 事件记录（C:18,186-188；L 文档 moderation case）。
//
// 语义：blocked=true 即策略拦截。C 直接替换成固定文案——**文案是出口产品
// 决策**（不是线格式事实），本包只记录；出口（S3+）决定常量文案/错误帧。
type ModerationInfo struct {
	Blocked bool
	Raw     json.RawMessage
}

// MarkerInfo 一条 message_marker 帧的登记（W31-1 §4 尾部帧面；W32-1 补齐）。
//
// 语义：上游用 marker 帧给消息打「内容生命周期」标记（W29-1 实抓 + 三 golden）：
//
//	cot_token/first           思考开始（ISP 档；随后是 reasoning_recap 帧）
//	user_visible_token/first  正文开始（首个可见 token）
//	final_channel_token/first 终稿通道开始
//	last_token/last           正文结束（**尾部第一帧**——其后才是 title/ste/
//	                          stream_complete/[detail]/[DONE] 尾读段）
//
// 本包原先把 message_marker 整类忽略（版本策略）；W31-1 实证流式 4/4 被宽限
// 截断于 last_token——last_token 是「截断点」的直接证据帧，故补登记（只读观测，
// 不产 IR 事件、不参与判据）。
type MarkerInfo struct {
	Marker    string // cot_token | user_visible_token | final_channel_token | last_token | …
	Event     string // first | last（原文；空 = 帧未带）
	MessageID string
}

// ImagePointer 图片指针（C:268-300；L 文档「Image pointer types」表）。
type ImagePointer struct {
	Pointer string // 原文（file-service://file_xxx / sediment://file_xxx / file_upload）
	Scheme  string // "file-service" | "sediment" | "file_upload" | ""（未知 scheme）
}

// ToolMessage 被抑制消息的结构化记录（tool 消息面最小映射；S3/S6 消费）。
//
// 语义：流里混着的工具消息（recipient != "all"：python/browser/dalle.text2im/
// t2uay3k.sj1i4kz…，KB quirk 4）与 commentary/非 assistant 消息不产出正文——
// 参考实现里 A/B/Go/gpt2api 一律丢弃，只有 C 渲染成引用块+围栏（产品决策）。
// 本包不丢弃信息：分类为记录（正文/图片指针/原文 parts 全保留），由出口决定
// 渲染；**无载体 part（既非字符串、又无可用指针）显式报错**，绝不静默丢。
type ToolMessage struct {
	MessageID   string
	Role        string
	Recipient   string
	Channel     string
	ContentType string
	// Reason 抑制原因："role"（非 assistant）|"recipient"（!= all）|
	// "commentary"（channel=commentary）|"live-multimodal"（live 消息的图片指针——
	// 流事件面无 IR 载体，进记录供 S6 渲染）。
	Reason      string
	Text        string // 字符串 parts 连接（+ content.text 兜底）
	Images      []ImagePointer
	InitialText string // metadata.initial_text（工具「做了什么」摘要；C:178,202-207）
	RawParts    []json.RawMessage
}
