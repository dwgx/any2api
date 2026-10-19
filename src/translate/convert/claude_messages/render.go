package claude_messages

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dwgx/any2api/translate/hygiene"
	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/signature"
)

// ---------------------------------------------------------------------------
// 请求：IR → 线格式
// ---------------------------------------------------------------------------

// RenderRequest 渲染 Anthropic Messages 请求体。
//
// 出口硬门（cc-wire-contract A9 / anthropic-conversion-table §4）：
//   - system 由 RoleSystem/RoleDeveloper 消息提升为顶层字段（多条 → 数组元素，
//     不合并、不重排）；Extra["system"] 存在时优先原文回写（claude 侧保真路径，
//     含块级 cache_control），消息面 system 消息中超出原文覆盖面（前 K 条）的块
//     追加为数组元素——合并规则见 mergeSystemSeed（W14）；
//   - 连续 RoleTool 消息合并为一条 user 消息的 tool_result 块（全部结果必须在
//     同一条 user 消息、紧跟对应 assistant tool_use）；assistant 的 tool_use 缺
//     配对结果 → 合成 is_error:true 占位（可读文案，CPA 范式；否则上游拒）；
//     无对应 tool_use 的孤儿 tool_result → 折叠为普通文本（统一政策，绝不透传，
//     见 hygiene 包）；tool id 出口统一卫生（净化 + 缺失合成）；
//   - max_tokens 必填：Extra["max_tokens"]/["max_completion_tokens"] 取值，
//     缺省合成 32000（CPA 默认；「请求被改写」事实的日志点在服务壳层）；
//   - Extra 白名单透传（claudePassableExtras）+ 显式映射（stop→stop_sequences、
//     tool_choice、parallel_tool_calls、reasoning_effort→thinking）；其余丢弃
//     （映射表「丢弃+日志」降级策略）；
//   - 来源侧独有键先整体剔除（geminiOriginExtras）——见 pruneForeignRequestExtras。
func RenderRequest(req *ir.Request) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: nil request", ErrBadIR)
	}
	extra := pruneForeignRequestExtras(req.Extra)
	out := map[string]json.RawMessage{"model": mustMarshal(req.Model)}

	// system 提升。
	if raw, ok := extra["system"]; ok && len(raw) > 0 && !isJSONNull(raw) {
		merged, err := mergeSystemSeed(raw, req.Messages)
		if err != nil {
			return nil, err
		}
		out["system"] = merged
	} else {
		sys, err := renderSystem(req.Messages, "")
		if err != nil {
			return nil, err
		}
		if sys != nil {
			out["system"] = sys
		}
	}

	// 请求面（客户端历史回传）：无响应 id 可派生 → 合成工具 id 走块序回落形态
	//（seed 空；配对键稳定靠 id 幂等净化）。
	msgs, err := renderMessages(req.Messages, "")
	if err != nil {
		return nil, err
	}
	out["messages"] = mustMarshal(msgs)

	// max_tokens：必填，缺省合成。
	switch {
	case hasRaw(extra["max_tokens"]):
		out["max_tokens"] = extra["max_tokens"]
	case hasRaw(extra["max_completion_tokens"]):
		out["max_tokens"] = extra["max_completion_tokens"]
	default:
		out["max_tokens"] = mustMarshal(defaultMaxTokens)
	}

	if len(req.Tools) > 0 {
		tools := make([]json.RawMessage, 0, len(req.Tools))
		for i := range req.Tools {
			rt, err := renderToolDef(req.Tools[i])
			if err != nil {
				return nil, fmt.Errorf("tool %d: %w", i, err)
			}
			tools = append(tools, rt)
		}
		out["tools"] = mustMarshal(tools)
	}

	parallelDisabled := false
	if raw, ok := extra["parallel_tool_calls"]; ok {
		var b bool
		if err := json.Unmarshal(raw, &b); err == nil && !b {
			parallelDisabled = true
		}
	}
	tc, err := toolChoiceToClaude(req.ToolChoice, parallelDisabled)
	if err != nil {
		return nil, err
	}
	if tc != nil {
		out["tool_choice"] = tc
	}

	if raw, ok := extra["stop"]; ok && hasRaw(raw) {
		out["stop_sequences"] = normalizeStopSequences(raw)
	}
	for _, k := range claudePassableExtras {
		if raw, ok := extra[k]; ok && hasRaw(raw) {
			out[k] = raw
		}
	}
	// 推理强度（anthropic-conversion-table:81「推理强度」行）：claude 侧原文
	// （thinking / output_config，白名单直通）优先；缺省退 OpenAI 侧
	// reasoning_effort 的档位映射（thinkingForEffort，CPA `thinking/convert.go`
	// 档位表 + `claude_openai_request.go:59-106` 预算分支）。reasoning_effort
	// 本身不在 claudePassableExtras 白名单 → 绝不作为未知顶层键漏进上游。
	if !hasRaw(extra["thinking"]) && !hasRaw(extra["output_config"]) {
		if raw, ok := extra["reasoning_effort"]; ok && hasRaw(raw) {
			if th := thinkingForEffort(raw); th != nil {
				out["thinking"] = th
			}
		}
	}
	if req.Stream {
		out["stream"] = mustMarshal(true)
	}
	return marshalBytes(out)
}

// thinkingForEffort OpenAI reasoning_effort → claude `thinking` 参数（档位表：
// anthropic-conversion-table:81 行；CPA `thinking/convert.go:11-22` levelToBudgetMap
// 原值 + `claude_openai_request.go:89-102` 的 legacy/预算分支落法）。
//
// 档位→预算：none=0 / auto=-1 / minimal=512 / low=1024 / medium=8192 /
// high=24576 / xhigh=32768 / max=128000。落法（CPA 预算分支）：0 →
// `thinking:{type:"disabled"}`；-1（auto）→ `thinking:{type:"enabled"}`（不带
// budget_tokens）；>0 → `thinking:{type:"enabled",budget_tokens:N}`（xhigh/max
// 不夹取——CPA 注释「rely on per-model clamping」）。未知档位不映射（CPA
// ConvertLevelToBudget 未命中 → 不设置；「丢弃+日志」日志点在服务壳层）。
//
// 模型能力门（CPA 有门、本 slice 无）：CPA 的 adaptive 分支（Claude 4.6+ 走
// `thinking:{type:"adaptive"}` + `output_config.effort`，minimal→low、xhigh→high
// 归一）以 `registry.LookupModelInfo(modelName,"claude")` 的 Thinking.Levels 非空
// 为门（`claude_openai_request.go:63-88`）。本 slice 无模型注册表 → 无条件走预算
// 路径——即 CPA 对未知模型（mi==nil → supportsAdaptive=false）的行为。代价：
// 4.6+ 系收到 legacy 形态（`enabled`+budget_tokens 仍在 Anthropic 支持面，
// adaptive 是 4.6+ 的新增推荐面 [INFERENCE]）；**不支持 thinking 的模型传了会被
// 上游拒**（CPA 同——它同样无条件落预算路径，靠上游校验）。决策记录见
// research/briefs/reports/A2A-L19-W6-1.md。
func thinkingForEffort(raw json.RawMessage) json.RawMessage {
	var effort string
	if err := json.Unmarshal(raw, &effort); err != nil {
		return nil
	}
	enabled := func(budget int) json.RawMessage {
		return mustMarshal(map[string]json.RawMessage{
			"type":          mustMarshal("enabled"),
			"budget_tokens": mustMarshal(budget),
		})
	}
	switch strings.ToLower(strings.TrimSpace(effort)) {
	case "none":
		return mustMarshal(map[string]json.RawMessage{"type": mustMarshal("disabled")})
	case "auto":
		return mustMarshal(map[string]json.RawMessage{"type": mustMarshal("enabled")})
	case "minimal":
		return enabled(512)
	case "low":
		return enabled(1024)
	case "medium":
		return enabled(8192)
	case "high":
		return enabled(24576)
	case "xhigh":
		return enabled(32768)
	case "max":
		return enabled(128000)
	default:
		return nil
	}
}

// geminiOriginExtras gemini_generate 侧独有的顶层字段（gemini/wire.go
// geminiOnlyExtraKeys 的解析产物）：渲染 claude 请求体前整体剔除——claude 不认识
// 这些键，透传 = 上游 400（gemini-conversion-table §请求侧「丢弃+日志」行）。
//
// 当前 claude 出口的白名单（claudePassableExtras）与逐名读取（system/max_tokens/
// stop/parallel_tool_calls）本就不含这些键；此表是显式兜底——白名单扩张时不再依赖
// 「两集合恰好无交集」这一隐含前提（跨格式 hygiene 测试锁死该性质）。
var geminiOriginExtras = map[string]bool{
	"safetySettings":     true,
	"thinkingConfig":     true,
	"responseMimeType":   true,
	"responseJsonSchema": true,
	"responseModalities": true,
}

// pruneForeignRequestExtras 剔除 claude 请求体不认识的来源侧独有键（geminiOriginExtras）。
func pruneForeignRequestExtras(extra map[string]json.RawMessage) map[string]json.RawMessage {
	if len(extra) == 0 {
		return extra
	}
	out := make(map[string]json.RawMessage, len(extra))
	for k, v := range extra {
		if geminiOriginExtras[k] {
			continue
		}
		out[k] = v
	}
	return out
}

// foreignResponseExtras 非 claude 来源的响应顶层独有键（claude 响应体不认识）：
// 跨格式转换（X→claude，含经枢纽两步的中间/出口腿）时必须显式丢弃——claude
// 客户端/上游不认识这些键，透传 = 响应体污染（W5-8 报告 §缺口 2 点名；
// P53 缺口 18「响应面过滤表只列各自点名的键」在本表逐来源补齐）。
//
// 键集全部有据（各来源解析器建模键集之外、经 Response.Extra 保真旁路的键）：
//   - openai_chat（openai-wire §chat.completion 顶层字段）：system_fingerprint /
//     service_tier——两者都未建模，经 Response.Extra 保真旁路。
//   - openai 兼容面扩展（xAI/grok，grok-wire README §响应侧差异）：顶层
//     citations（仅末帧）/ output_files（代码执行产物）——claude 的 citations 是
//     content 块级字段、无顶层同名概念。
//   - gemini_generate（gemini-wire §响应顶层）：modelStatus——官方字段，本仓
//     解析器未建模（落 Response.Extra）。
//   - interactions（interactions-wire §响应）：errors——平台错误清单，解析侧
//     注释即「渲染侧不回写（诊断用）」。
//   - upstream-a / upstream-private（OpenAI 兼容上游的私有响应扩展，W83-1 X1
//     实测 2026-10-19：Anthropic 入口 → upstream-a 的响应顶层带这两个计时字段；
//     本仓解析/渲染路径均未建模这两键——改动前 .go 文件零命中，仅
//     chatgpt_web/sentinel/testdata/pow_vectors.json 的注释文本提及
//     elapsed_ms——只能经 openai_chat 的 Response.Extra 保真旁路进入 claude
//     响应体）：elapsed_ms / first_chunk_at——上游私有计时（总耗时毫秒 /
//     首块到达时刻），claude 响应无对应概念。
//
// responses 不建行：responses.ParseResponse 不产 Response.Extra（无键可携带）。
// 通用未知键（厂商扩展等）不在表内：保真旁路保留（只剔来源侧**独有**键）。
//
// 同格式回传（claude→claude）不受影响：这些键不会出现在 claude 解析产物里
// （claude ParseResponse 的建模键集不含它们）。
var foreignResponseExtras = map[string]bool{
	"system_fingerprint": true,
	"service_tier":       true,
	"citations":          true, // xAI 顶层（块级 citations 不受影响——本表只管响应顶层）
	"output_files":       true, // xAI 代码执行产物
	"modelStatus":        true, // gemini 官方响应字段（未建模）
	"errors":             true, // interactions 平台错误清单
	"elapsed_ms":         true, // upstream-a/upstream-private 私有总耗时（W83-1 X1）
	"first_chunk_at":     true, // upstream-a/upstream-private 私有首块时刻（W83-1 X1）
}

// pruneForeignResponseExtras 剔除 claude 响应体不认识的来源侧独有键
// （见 foreignResponseExtras）。
func pruneForeignResponseExtras(extra map[string]json.RawMessage) map[string]json.RawMessage {
	if len(extra) == 0 {
		return extra
	}
	out := make(map[string]json.RawMessage, len(extra))
	for k, v := range extra {
		if foreignResponseExtras[k] {
			continue
		}
		out[k] = v
	}
	return out
}

// openaiOriginMessageExtras openai_chat 消息级独有、claude 消息对象不认识的字段
// （P53 缺口：claude 出口的消息级 Extra 此前直接 mergeExtras、不过滤——openai 来源的
// 消息级未知键会进 claude 请求/响应体）。键集全部有据可查（不发明键名）：
//   - refusal：OpenAI assistant 字段；本解析器仅在非空字符串提升为块，null/空串
//     留在 Message.Extra（OCp parseMessage）——claude 无该字段；
//   - function_call：OpenAI 弃用字段（openai-wire §chat message：deprecated）；
//   - audio：OpenAI assistant 音频输出 {id,data,expires_at,transcript}
//     （openai-wire §流式 delta 字段表）；claude 无该字段；
//   - reasoning_content / reasoning / reasoning_details：OpenAI 兼容生态的推理
//     扩展字段（longtail-matrix §4.3；anthropic-conversion-table §思考行），
//     claude 的思考走 content 块（thinking），不认识这些消息级键。
//
// 同格式回传（claude→claude）不受影响：这些键不会出现在 claude 解析产物里。
var openaiOriginMessageExtras = map[string]bool{
	"refusal":           true,
	"function_call":     true,
	"audio":             true,
	"reasoning_content": true,
	"reasoning":         true,
	"reasoning_details": true,
}

// pruneForeignMessageExtras 剔除 claude 消息对象不认识的来源侧独有键
// （见 openaiOriginMessageExtras）。
func pruneForeignMessageExtras(extra map[string]json.RawMessage) map[string]json.RawMessage {
	if len(extra) == 0 {
		return extra
	}
	out := make(map[string]json.RawMessage, len(extra))
	for k, v := range extra {
		if openaiOriginMessageExtras[k] {
			continue
		}
		out[k] = v
	}
	return out
}

// renderSystem 从 RoleSystem/RoleDeveloper 消息合成顶层 system：
// 单 text 块 → 字符串；其余 → 块数组（逐元素不合并——CC A9）。
func renderSystem(messages []ir.Message, seed string) (json.RawMessage, error) {
	var blocks []ir.ContentBlock
	for _, m := range messages {
		if m.Role == ir.RoleSystem || m.Role == ir.RoleDeveloper {
			blocks = append(blocks, m.Blocks...)
		}
	}
	if len(blocks) == 0 {
		return nil, nil
	}
	if len(blocks) == 1 && blocks[0].Kind == ir.BlockText {
		return mustMarshal(blocks[0].Text), nil
	}
	arr := make([]json.RawMessage, 0, len(blocks))
	for i, b := range blocks {
		rb, err := renderBlock(b, i, seed)
		if err != nil {
			return nil, fmt.Errorf("system block %d: %w", i, err)
		}
		arr = append(arr, rb)
	}
	return mustMarshal(arr), nil
}

// seedSystemMessageCount 顶层 system 原文（Extra["system"]）在 IR 中对应的
// RoleSystem 消息数——口径与 parseSystem 一一对应（string 非空 → 1；数组 → len；
// 空串/非法形状 → 0）。用于区分「原文已覆盖」与「消息面新增」的 system 消息
// （见 mergeSystemSeed）。
func seedSystemMessageCount(raw json.RawMessage) int {
	if len(raw) == 0 || isJSONNull(raw) {
		return 0
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil || s == "" {
			return 0
		}
		return 1
	case '[':
		var parts []json.RawMessage
		if err := json.Unmarshal(raw, &parts); err != nil {
			return 0
		}
		return len(parts)
	default:
		return 0
	}
}

// mergeSystemSeed 组合顶层 system 原文（Extra["system"]，逐字保真——CC A9：
// 元素级 cache_control 只有这条通道）与消息面 system 消息（messages[] 内
// role:"system"，W14）的块。合并规则：
//
//   - 原文解析出的前 K 条 RoleSystem 消息（K = seedSystemMessageCount）视为已被
//     原文覆盖——跳过，不重复回写（ParseRequest 保证顶层 system 的消息恒排在
//     消息面之前，故「前 K 条」是确定性判定，不是启发式）；
//   - 其余系统面消息的块按 IR 序**追加为数组元素**：原文是数组 → 直接追加；
//     原文是单字符串 → 包装为 text 块后追加（数组元素只能是块对象）；其余形状
//     （claude 解析面不可能产出）→ 原文原样返回，追加面不介入；
//   - 无追加块 → 原文逐字返回（纯 claude→claude 回传面行为不变）。
//
// 服务壳路径说明：同协议直通（claude 入口 × claude 上游）不经转换器，pairs
// Lookup 对同格式显式报错——追加分支的实际触发面 = 跨格式腿（入口非 claude；
// 此时 IR 的 Extra["system"] 只可能来自外来入口的未知顶层键保真旁路，属病态
// 输入，按同一 K 规则确定性处理）或库级 claude→claude 往返（原文元素逐字保留
// + 消息面 system 块追加，不丢内容）。
func mergeSystemSeed(raw json.RawMessage, messages []ir.Message) (json.RawMessage, error) {
	skip := seedSystemMessageCount(raw)
	var blocks []ir.ContentBlock
	for _, m := range messages {
		if m.Role != ir.RoleSystem && m.Role != ir.RoleDeveloper {
			continue
		}
		if m.Role == ir.RoleSystem && skip > 0 {
			skip-- // 顶层 system 原文覆盖面（parseSystem 产物）
			continue
		}
		blocks = append(blocks, m.Blocks...)
	}
	if len(blocks) == 0 {
		return raw, nil
	}
	var arr []json.RawMessage
	switch raw[0] {
	case '[':
		if err := json.Unmarshal(raw, &arr); err != nil {
			return nil, fmt.Errorf("system: %v", err)
		}
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("system: %v", err)
		}
		arr = append(arr, mustMarshal(map[string]json.RawMessage{
			"type": mustMarshal(blockTypeText),
			"text": mustMarshal(s),
		}))
	default:
		// 非 string/数组的原文（外来入口的保真旁路可能带入）：不发明包装，
		// 维持原文回写（现有行为）。
		return raw, nil
	}
	for i, b := range blocks {
		rb, err := renderBlock(b, i, "")
		if err != nil {
			return nil, fmt.Errorf("system block %d: %w", i, err)
		}
		arr = append(arr, rb)
	}
	return mustMarshal(arr), nil
}

// renderMessages 渲染 messages[]（system 已提升；tool 结果按配对规则合并/补齐）。
// seed = 响应 id（合成工具 id 的派生源；请求面传空 → 块序回落形态）。
func renderMessages(messages []ir.Message, seed string) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(messages))
	i := 0
	for i < len(messages) {
		m := messages[i]
		switch m.Role {
		case ir.RoleSystem, ir.RoleDeveloper:
			i++ // 已提升为顶层 system
		case ir.RoleTool:
			// 连续 tool 消息 → 一条 user 消息；**孤儿结果折叠为文本**（统一政策：
			// 无对应 tool_use 的 tool_result 绝不作为 tool_result 出口——上游 400）。
			results, folded, _, next, err := toolRun(messages, i, nil)
			if err != nil {
				return nil, err
			}
			i = next
			if content := append(results, folded...); len(content) > 0 {
				out = append(out, mustMarshal(map[string]json.RawMessage{
					"role":    mustMarshal(string(ir.RoleUser)),
					"content": mustMarshal(content),
				}))
			}
		case ir.RoleAssistant:
			rm, toolIDs, err := renderAssistantMessage(m, seed)
			if err != nil {
				return nil, err
			}
			out = append(out, rm)
			// 前瞻：收集后续连续 tool 消息。已配对 → tool_result（领队）；孤儿结果
			// → 折叠为文本（统一政策）；assistant 的 tool_use 缺配对结果 → 合成
			// is_error:true 占位（可读文案，CPA 范式；否则上游拒）。
			known := make(map[string]bool, len(toolIDs))
			for _, id := range toolIDs {
				known[id] = true
			}
			results, folded, seen, next, err := toolRun(messages, i+1, known)
			if err != nil {
				return nil, err
			}
			j := next
			for _, id := range toolIDs {
				if seen[id] {
					continue
				}
				results = append(results, mustMarshal(map[string]json.RawMessage{
					"type":        mustMarshal(blockTypeToolResult),
					"tool_use_id": mustMarshal(id),
					"content":     mustMarshal(hygiene.InterruptedResultText),
					"is_error":    mustMarshal(true),
				}))
			}
			if content := append(results, folded...); len(content) > 0 {
				out = append(out, mustMarshal(map[string]json.RawMessage{
					"role":    mustMarshal(string(ir.RoleUser)),
					"content": mustMarshal(content),
				}))
			}
			i = j
		case ir.RoleUser:
			rm, err := renderUserMessage(m, seed)
			if err != nil {
				return nil, err
			}
			out = append(out, rm)
			i++
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnexpectedRole, m.Role)
		}
	}
	return out, nil
}

// renderAssistantMessage 渲染 assistant 消息；返回消息 JSON 与其 tool_use 块 id 列表。
//
// 请求面（回传轮）签名门（W5-19 条目 3；表 §7.1「Responses reasoning → Anthropic」
// 行 + §「OpenAI（任一侧）→ Anthropic（无签名）」行）：thinking 块**仅在签名被判
// Claude 兼容**时重放；不兼容（含空签名）→ 整块丢弃——Anthropic 拒空/外来签名，
// 本转换器绝不凭空造签名、也不把外来 provider 的签名塞给 Claude 上游。
// redacted_thinking 是 Claude 原生载荷（前缀载体还原而来），保持原样。
// 响应面（RenderResponse）不走本门：响应给客户端看，签名缺失是可接受降级（§6.2）。
func renderAssistantMessage(m ir.Message, seed string) (json.RawMessage, []string, error) {
	blocks := make([]ir.ContentBlock, 0, len(m.Blocks))
	for _, b := range m.Blocks {
		if b.Kind == ir.BlockThinking {
			sig, ok := signature.CompatibleSignatureForProvider(signature.ProviderClaude, b.Signature)
			if !ok {
				continue // 不兼容/空签名：整块丢弃（绝不改内容后硬塞）。
			}
			b.Signature = sig // cache 前缀剥离后的 provider 原生形态（CPA 归一）。
		}
		blocks = append(blocks, b)
	}
	content, err := renderContent(blocks, false, seed)
	if err != nil {
		return nil, nil, err
	}
	var ids []string
	for i, b := range blocks { // 与 renderContent 同一（已过滤）切片同序——id 键一致
		if b.Kind == ir.BlockToolUse || b.Kind == ir.BlockServerToolUse {
			if b.ToolCall != nil {
				ids = append(ids, hygiene.WireToolID("toolu", b.ToolCall.ID, seed, i))
			}
		}
	}
	out := map[string]json.RawMessage{
		"role":    mustMarshal(string(ir.RoleAssistant)),
		"content": content,
	}
	mergeExtras(out, pruneForeignMessageExtras(m.Extra))
	return mustMarshal(out), ids, nil
}

// renderUserMessage 渲染 user 消息。thinking 块忽略（CPA 安全规则：user/system
// 里的 thinking 一律忽略，防注入）；tool_result 块保留原位（hub 路径形态）。
func renderUserMessage(m ir.Message, seed string) (json.RawMessage, error) {
	blocks := make([]ir.ContentBlock, 0, len(m.Blocks))
	for _, b := range m.Blocks {
		if b.Kind == ir.BlockThinking || b.Kind == ir.BlockRedactedThinking {
			continue
		}
		blocks = append(blocks, b)
	}
	content, err := renderContent(blocks, false, seed)
	if err != nil {
		return nil, err
	}
	out := map[string]json.RawMessage{
		"role":    mustMarshal(string(ir.RoleUser)),
		"content": content,
	}
	mergeExtras(out, pruneForeignMessageExtras(m.Extra))
	return mustMarshal(out), nil
}

// toolRun 处理 messages[i] 起连续的 tool 消息（返回新下标）：按 known
// （assistant 的 wire tool_use id 集合；nil = 无对应 assistant → 全部孤儿）分流——
// 命中 → tool_result 块（领队）；未命中 → 折叠 text 块（统一孤儿政策：
// 绝不作为 tool_result 出口，绝不静默丢弃）。seen = 命中的 wire id 集合。
func toolRun(messages []ir.Message, i int, known map[string]bool) (results, folded []json.RawMessage, seen map[string]bool, next int, err error) {
	seen = map[string]bool{}
	for next = i; next < len(messages) && messages[next].Role == ir.RoleTool; next++ {
		if len(messages[next].Blocks) == 0 {
			return nil, nil, nil, next, fmt.Errorf("%w: tool message without tool_result blocks", ErrToolResultShape)
		}
		for k, b := range messages[next].Blocks {
			if b.Kind != ir.BlockToolResult || b.ToolResult == nil {
				return nil, nil, nil, next, fmt.Errorf("%w: tool message block %d kind %q", ErrToolResultShape, k, b.Kind)
			}
			wire := hygiene.SanitizeToolID(b.ToolResult.CallID)
			if known[wire] {
				rb, err := renderToolResultBlock(b)
				if err != nil {
					return nil, nil, nil, next, err
				}
				results = append(results, rb)
				seen[wire] = true
				continue
			}
			fb, err := foldToolResultBlock(b)
			if err != nil {
				return nil, nil, nil, next, err
			}
			folded = append(folded, fb)
		}
	}
	return results, folded, seen, next, nil
}

// renderToolResultBlock 渲染一个 tool_result 块（tool_use_id 过出口净化）。
func renderToolResultBlock(b ir.ContentBlock) (json.RawMessage, error) {
	if b.Kind != ir.BlockToolResult || b.ToolResult == nil {
		return nil, fmt.Errorf("%w: block kind %q", ErrToolResultShape, b.Kind)
	}
	tr := map[string]json.RawMessage{
		"type":        mustMarshal(blockTypeToolResult),
		"tool_use_id": mustMarshal(hygiene.SanitizeToolID(b.ToolResult.CallID)),
		"content":     mustMarshal(b.ToolResult.Content),
	}
	if b.ToolResult.IsError {
		tr["is_error"] = mustMarshal(true)
	}
	return mustMarshal(tr), nil
}

// foldToolResultBlock 孤儿 tool_result 块 → 普通 text 块（hygiene 统一文案：
// 空白内容 → 显式占位，绝不产出空 text 块）。
func foldToolResultBlock(b ir.ContentBlock) (json.RawMessage, error) {
	if b.Kind != ir.BlockToolResult || b.ToolResult == nil {
		return nil, fmt.Errorf("%w: block kind %q", ErrToolResultShape, b.Kind)
	}
	return mustMarshal(map[string]json.RawMessage{
		"type": mustMarshal(blockTypeText),
		"text": mustMarshal(hygiene.FoldOrphanToolResultText(b.ToolResult.Content)),
	}), nil
}

// renderContent 渲染消息 content：单 text 块 → 字符串；其余 → 块数组。
// seed = 响应 id（合成工具 id 的派生源；请求面传空）。
func renderContent(blocks []ir.ContentBlock, forceArray bool, seed string) (json.RawMessage, error) {
	if !forceArray && len(blocks) == 1 && blocks[0].Kind == ir.BlockText {
		return mustMarshal(blocks[0].Text), nil
	}
	arr := make([]json.RawMessage, 0, len(blocks))
	for i, b := range blocks {
		rb, err := renderBlock(b, i, seed)
		if err != nil {
			return nil, fmt.Errorf("block %d: %w", i, err)
		}
		arr = append(arr, rb)
	}
	return mustMarshal(arr), nil
}

// renderBlock 渲染一个内容块为 claude 线格式对象（blockIdx = 块序，合成 id 用）。
func renderBlock(b ir.ContentBlock, blockIdx int, seed string) (json.RawMessage, error) {
	switch b.Kind {
	case ir.BlockText:
		return mustMarshal(map[string]json.RawMessage{
			"type": mustMarshal(blockTypeText),
			"text": mustMarshal(b.Text),
		}), nil
	case ir.BlockThinking:
		// signature 必须原样回传（含空——空签名是上游拒收事实，出口不造签名）。
		return mustMarshal(map[string]json.RawMessage{
			"type":      mustMarshal(blockTypeThinking),
			"thinking":  mustMarshal(b.Text),
			"signature": mustMarshal(b.Signature),
		}), nil
	case ir.BlockRedactedThinking:
		return mustMarshal(map[string]json.RawMessage{
			"type": mustMarshal(blockTypeRedactedThink),
			"data": mustMarshal(b.Text),
		}), nil
	case ir.BlockToolUse, ir.BlockServerToolUse:
		if b.ToolCall == nil {
			return nil, fmt.Errorf("%w: tool_use without ToolCall", ErrBadIR)
		}
		input, err := toolInputObject(b.ToolCall.Arguments)
		if err != nil {
			return nil, err
		}
		t := blockTypeToolUse
		if b.Kind == ir.BlockServerToolUse {
			t = blockTypeServerToolUse
		}
		return mustMarshal(map[string]json.RawMessage{
			"type":  mustMarshal(t),
			"id":    mustMarshal(hygiene.WireToolID("toolu", b.ToolCall.ID, seed, blockIdx)),
			"name":  mustMarshal(b.ToolCall.Name),
			"input": input,
		}), nil
	case ir.BlockToolResult:
		if b.ToolResult == nil {
			return nil, fmt.Errorf("%w: tool_result without payload", ErrBadIR)
		}
		return renderToolResultBlock(b)
	case ir.BlockImage:
		if b.Image == nil {
			return nil, fmt.Errorf("%w: image without payload", ErrBadIR)
		}
		var src map[string]json.RawMessage
		switch {
		case b.Image.Data != "":
			src = map[string]json.RawMessage{
				"type":       mustMarshal("base64"),
				"media_type": mustMarshal(b.Image.MediaType),
				"data":       mustMarshal(b.Image.Data),
			}
		case b.Image.URL != "":
			src = map[string]json.RawMessage{
				"type": mustMarshal("url"),
				"url":  mustMarshal(b.Image.URL),
			}
		default:
			return nil, fmt.Errorf("%w: image without data/url", ErrBadIR)
		}
		return mustMarshal(map[string]json.RawMessage{
			"type":   mustMarshal(blockTypeImage),
			"source": mustMarshal(src),
		}), nil
	case ir.BlockRefusal:
		// claude 无 refusal 块类型：拒绝文本以 text 块承载，拒绝语义在 stop_reason。
		return mustMarshal(map[string]json.RawMessage{
			"type": mustMarshal(blockTypeText),
			"text": mustMarshal(b.Text),
		}), nil
	case ir.BlockWebSearchToolResult, ir.BlockUnknown:
		if len(b.Raw) == 0 {
			return nil, fmt.Errorf("%w: %q block without Raw", ErrBadIR, b.Kind)
		}
		return append(json.RawMessage(nil), b.Raw...), nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedBlock, b.Kind)
	}
}

// toolInputObject 把 IR Arguments 文本转 claude input 对象：
// "" → {}；合法 JSON 对象 → 原文；其余 → ErrInvalidToolInput（修不好不静默）。
func toolInputObject(args string) (json.RawMessage, error) {
	if args == "" {
		return json.RawMessage("{}"), nil
	}
	if !json.Valid([]byte(args)) {
		return nil, fmt.Errorf("%w: %s", ErrInvalidToolInput, truncate(args, 80))
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(args), &m); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidToolInput, truncate(args, 80))
	}
	return json.RawMessage(args), nil
}

// geminiOriginToolExtras gemini_generate 工具定义级独有、claude 工具对象不认识的
// 字段（P53 缺口：claude 出口的工具级 Extra 此前直接 mergeExtras、不过滤——gemini
// 来源的 decl 级未建模字段会透传进 claude 请求体）。键集有据可查（不发明键名）：
// 官方 functionDeclarations 字段 = name/description/parameters/parametersJsonSchema/
// response/responseJsonSchema（gemini-wire §tools[]），前四者已建模，其余两个落
// ToolDef.Extra（gemini ParseRequest 的 extrasOf 排除表）→ claude 侧显式剔除。
var geminiOriginToolExtras = map[string]bool{
	"response":           true, // Gemini 函数返回声明（Schema）
	"responseJsonSchema": true, // 同上（JSON Schema 形态）
}

// pruneForeignToolExtras 剔除 claude 工具对象不认识的来源侧独有键
// （见 geminiOriginToolExtras）。
func pruneForeignToolExtras(extra map[string]json.RawMessage) map[string]json.RawMessage {
	if len(extra) == 0 {
		return extra
	}
	out := make(map[string]json.RawMessage, len(extra))
	for k, v := range extra {
		if geminiOriginToolExtras[k] {
			continue
		}
		out[k] = v
	}
	return out
}

// renderToolDef 渲染 tools[] 项：{name, description?, input_schema, strict?}。
// input_schema 缺失时合成最小合法 schema（Messages API 该字段必需）。
func renderToolDef(td ir.ToolDef) (json.RawMessage, error) {
	fn := map[string]json.RawMessage{"name": mustMarshal(td.Name)}
	if td.Description != "" {
		fn["description"] = mustMarshal(td.Description)
	}
	if len(td.InputSchema) > 0 && !isJSONNull(td.InputSchema) {
		fn["input_schema"] = td.InputSchema
	} else {
		fn["input_schema"] = json.RawMessage(`{"type":"object"}`)
	}
	if td.Strict != nil {
		fn["strict"] = mustMarshal(*td.Strict)
	}
	mergeExtras(fn, pruneForeignToolExtras(td.Extra))
	return mustMarshal(fn), nil
}

// normalizeStopSequences OpenAI stop（string | array）→ claude stop_sequences（数组）。
func normalizeStopSequences(raw json.RawMessage) json.RawMessage {
	if len(raw) > 0 && raw[0] == '"' {
		return mustMarshal([]json.RawMessage{append(json.RawMessage(nil), raw...)})
	}
	return raw
}

// toolChoiceToClaude IR 规范形（OpenAI 形状）tool_choice → claude 四型。
// parallelDisabled 且非 none 时附加 disable_parallel_tool_use:true（映射表
// 「并行工具开关」行）；无 tool_choice 时合成 {"type":"auto",disable…}。
func toolChoiceToClaude(raw json.RawMessage, parallelDisabled bool) (json.RawMessage, error) {
	withDisable := func(m map[string]json.RawMessage, typ string) json.RawMessage {
		if parallelDisabled && typ != "none" {
			m["disable_parallel_tool_use"] = mustMarshal(true)
		}
		return mustMarshal(m)
	}
	if len(raw) == 0 || isJSONNull(raw) {
		if parallelDisabled {
			return withDisable(map[string]json.RawMessage{"type": mustMarshal("auto")}, "auto"), nil
		}
		return nil, nil
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrUnsupportedToolChoice, err)
		}
		switch s {
		case "auto":
			return withDisable(map[string]json.RawMessage{"type": mustMarshal("auto")}, "auto"), nil
		case "none":
			return mustMarshal(map[string]json.RawMessage{"type": mustMarshal("none")}), nil
		case "required":
			return withDisable(map[string]json.RawMessage{"type": mustMarshal("any")}, "any"), nil
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnsupportedToolChoice, s)
		}
	case '{':
		var w struct {
			Type     string `json:"type"`
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &w); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrUnsupportedToolChoice, err)
		}
		switch w.Type {
		case "function":
			if w.Function.Name == "" {
				return nil, fmt.Errorf("%w: function without name", ErrUnsupportedToolChoice)
			}
			return withDisable(map[string]json.RawMessage{
				"type": mustMarshal("tool"),
				"name": mustMarshal(w.Function.Name),
			}, "tool"), nil
		case "auto":
			return withDisable(map[string]json.RawMessage{"type": mustMarshal("auto")}, "auto"), nil
		case "none":
			return mustMarshal(map[string]json.RawMessage{"type": mustMarshal("none")}), nil
		case "any":
			return withDisable(map[string]json.RawMessage{"type": mustMarshal("any")}, "any"), nil
		case "tool":
			return withDisable(map[string]json.RawMessage{
				"type": mustMarshal("tool"),
				"name": mustMarshal(w.Name),
			}, "tool"), nil
		default:
			return nil, fmt.Errorf("%w: type %q", ErrUnsupportedToolChoice, w.Type)
		}
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedToolChoice, truncate(string(raw), 80))
	}
}

// ---------------------------------------------------------------------------
// 非流式响应：IR → 线格式
// ---------------------------------------------------------------------------

// RenderResponse 渲染非流式 Message 响应体。
//
// id：上游不给 → 合成每响应唯一 `msg_synth_<32hex>`（W5-35；与 openai_chat
// `r_synth` / responses `resp_synth` 同族纪律，W5-31）——空 id 进 wire 会让客户端
// transcript/日志失去响应关联，工具 id 的派生 seed 也回落块序形态（跨响应重复，
// W5-29 报告点名的残余）。
//
// stop_reason 合成（映射表「结束原因优先级」行）：有 tool_use 块且上游原因为
// stop/缺省 → 强制 tool_use（CPA :945-947 的修正版：length/error 等截断/异常
// 语义优先保留，不伪造成功）。stop_sequence 回填（W52-1；与流式 emitMessageDelta
// 同款）：IR StopSequence 非空 ⇒ 覆写 `stop_reason:"stop_sequence"` + 序列值
// （AM 契约：stop_sequence 非 null ⟺ stop_reason=="stop_sequence"）；空 ⇒
// `stop_sequence:null`（旧形态逐字节不变）。
//
// Extra：来源侧独有键先整体剔除（foreignResponseExtras）——见 pruneForeignResponseExtras。
func RenderResponse(resp *ir.Response) ([]byte, error) {
	if resp == nil {
		return nil, fmt.Errorf("%w: nil response", ErrBadIR)
	}
	id := resp.ID
	if id == "" {
		// 上游不给响应 id → 合成每响应唯一（`msg_synth_<32hex>`）：wire 的 `id`
		// 与工具 id 派生 seed 都用它（响应内确定、跨响应唯一）。
		id = hygiene.SynthUniqueID("msg")
	}
	content, err := renderContent(resp.Message.Blocks, true, id)
	if err != nil {
		return nil, fmt.Errorf("content: %w", err)
	}
	stop := effectiveStopReason(resp.Message.Blocks, resp.StopReason)
	stopReason := marshalStopReasonClaude(stop)
	stopSequence := json.RawMessage("null")
	if resp.StopSequence != "" {
		stopReason = mustMarshal("stop_sequence")
		stopSequence = mustMarshal(resp.StopSequence)
	}
	out := map[string]json.RawMessage{
		"id":            mustMarshal(id),
		"type":          mustMarshal(objectMessage),
		"role":          mustMarshal(string(ir.RoleAssistant)),
		"model":         mustMarshal(resp.Model),
		"content":       content,
		"stop_reason":   stopReason,
		"stop_sequence": stopSequence,
		"usage":         renderUsage(resp.Usage),
	}
	mergeExtras(out, pruneForeignResponseExtras(resp.Extra))
	return marshalBytes(out)
}

// effectiveStopReason 结束原因优先级：有工具调用块且原因为 stop/空 → tool_use。
func effectiveStopReason(blocks []ir.ContentBlock, r ir.StopReason) ir.StopReason {
	if r == ir.StopStop || r == "" {
		for _, b := range blocks {
			if b.Kind == ir.BlockToolUse || b.Kind == ir.BlockServerToolUse {
				return ir.StopToolUse
			}
		}
	}
	return r
}

// marshalStopReasonClaude 渲染 stop_reason 字段（空 → null）。
func marshalStopReasonClaude(r ir.StopReason) json.RawMessage {
	if r == "" {
		return json.RawMessage("null")
	}
	return mustMarshal(stopReasonFor(r))
}

// stopReasonFor IR 四值 → Anthropic stop_reason（映射表反向；StopError → refusal，
// 修正 CPA 的 content_filter→end_turn 丢语义）。
func stopReasonFor(r ir.StopReason) string {
	switch r {
	case ir.StopStop:
		return "end_turn"
	case ir.StopLength:
		return "max_tokens"
	case ir.StopToolUse:
		return "tool_use"
	case ir.StopError:
		return "refusal"
	default:
		return string(r) // 未知/透传值原样回写
	}
}

// ---------------------------------------------------------------------------
// 错误：IR → 线格式（绝不透传 Error.Raw）
// ---------------------------------------------------------------------------

// RenderError 渲染 Anthropic 错误信封 {"type":"error","error":{type,message}}。
// 纪律：Error.Raw 永不进入输出；type 按 §7.2 双向表映射，未映射值保留字面
// （CC C2：overloaded_error 等文本匹配依赖不可改写）。
func RenderError(e *ir.Error) ([]byte, error) {
	if e == nil {
		return nil, fmt.Errorf("%w: nil error", ErrBadIR)
	}
	inner := map[string]json.RawMessage{
		"type":    mustMarshal(claudeErrorTypeFor(e)),
		"message": mustMarshal(e.Message),
	}
	return marshalBytes(map[string]json.RawMessage{
		"type":  mustMarshal(evError),
		"error": mustMarshal(inner),
	})
}

// claudeErrorTypeFor OpenAI 错误 type → Anthropic error.type（§7.2 表）。
func claudeErrorTypeFor(e *ir.Error) string {
	if e.Code == "overloaded_error" {
		return "overloaded_error" // 过载文本匹配优先（CC C2）
	}
	switch e.Type {
	case "invalid_request_error":
		return "invalid_request_error"
	case "invalid_api_key", "authentication_error":
		return "authentication_error"
	case "insufficient_quota":
		return "billing_error"
	case "permission_error":
		return "permission_error"
	case "not_found_error":
		return "not_found_error"
	case "conflict_error":
		return "conflict_error"
	case "request_too_large":
		return "request_too_large"
	case "rate_limit_error":
		return "rate_limit_error"
	case "server_error", "api_error":
		return "api_error"
	case "timeout_error":
		return "timeout_error"
	case "overloaded_error":
		return "overloaded_error"
	case "content_policy_violation", "context_length_exceeded", "invalid_prompt":
		return "invalid_request_error"
	case "":
		return "api_error"
	default:
		return e.Type
	}
}

// ---------------------------------------------------------------------------
// 流式：IR 事件 → 命名 SSE（出口绝不发 [DONE]）
// ---------------------------------------------------------------------------

// RenderStream 把 IR 事件序列渲染为 Anthropic 命名事件 SSE 字节流（批量入口；
// 增量/流式场景用 NewStreamWriter——同一状态机内核）。
//
// 批量语义：预扫最终 usage，使 message_start 提前携带 input/cache
// （CC B7「input/cache 全量在 start」）——实时 relay 做不到（全量在 message_delta，
// 客户端合并规则保证 >0 的 delta 值覆盖 start 占位）。
//
// 契约（CC B1-B8）：
//   - 帧语法 `event: <name>\ndata: <json>\n\n`；序列以 start 开头；
//   - message_delta.usage 承载合并后全量（累计覆盖语义）；
//   - content_block_start 内联内容恒为空（text:""/thinking:""/input:{}——B6：
//     客户端会丢弃 start 内内容，工具参数只走 input_json_delta；空参数发 "{}"）；
//   - message_delta 必带 stop_reason（B7）；message_stop 必为末帧（B8）；
//   - 无 [DONE]（AM 陷阱 5）；error 帧为终态。
func RenderStream(events []ir.StreamEvent) ([]byte, error) {
	// 预扫：最终 usage（最后一个非 nil——累计覆盖）用于 message_start 落位。
	var usage *ir.Usage
	for _, ev := range events {
		if ev.Kind == ir.EventMessageDelta && ev.Usage != nil {
			usage = ev.Usage
		}
	}
	w := NewStreamWriter(StreamWriterOptions{StartUsage: usage})
	var buf bytes.Buffer
	for _, ev := range events {
		b, err := w.Write(ev)
		if err != nil {
			return nil, err
		}
		buf.Write(b)
	}
	b, err := w.Finish()
	if err != nil {
		return nil, err
	}
	buf.Write(b)
	return buf.Bytes(), nil
}

// renderUsage Anthropic usage 对象：建模字段 + claude 侧白名单 Extra（OpenAI
// 风味 details 剥离——AM 陷阱 7 的污染防护）；cache 字段仅 >0 回写；无 total_tokens。
func renderUsage(u *ir.Usage) json.RawMessage {
	m := map[string]json.RawMessage{}
	if u == nil {
		m["input_tokens"] = mustMarshal(0)
		m["output_tokens"] = mustMarshal(0)
		return mustMarshal(m)
	}
	m["input_tokens"] = mustMarshal(u.Input)
	m["output_tokens"] = mustMarshal(u.Output)
	if u.CacheRead > 0 {
		m["cache_read_input_tokens"] = mustMarshal(u.CacheRead)
	}
	if u.CacheCreation != nil && u.CacheCreation.FiveMinute+u.CacheCreation.OneHour > 0 {
		m["cache_creation_input_tokens"] = mustMarshal(u.CacheCreation.FiveMinute + u.CacheCreation.OneHour)
		m["cache_creation"] = mustMarshal(map[string]json.RawMessage{
			"ephemeral_5m_input_tokens": mustMarshal(u.CacheCreation.FiveMinute),
			"ephemeral_1h_input_tokens": mustMarshal(u.CacheCreation.OneHour),
		})
	}
	if u.Reasoning > 0 {
		m["output_tokens_details"] = mustMarshal(map[string]json.RawMessage{
			"thinking_tokens": mustMarshal(u.Reasoning),
		})
	}
	for _, k := range []string{"server_tool_use", "service_tier", "inference_geo", "iterations", "speed"} {
		if raw, ok := u.Extra[k]; ok && hasRaw(raw) {
			m[k] = raw
		}
	}
	return mustMarshal(m)
}

// renderStartUsage message_start.message.usage：预扫到的 input/cache 提前落位
// （CC B7：input/cache 全量在 start），output 置 0（以 message_delta 为准）。
func renderStartUsage(u *ir.Usage) json.RawMessage {
	if u == nil {
		return renderUsage(nil)
	}
	c := *u
	c.Output = 0
	c.Reasoning = 0
	return renderUsage(&c)
}

// ---------------------------------------------------------------------------
// 渲染内部工具
// ---------------------------------------------------------------------------

func hasRaw(raw json.RawMessage) bool {
	return len(raw) > 0 && !isJSONNull(raw)
}

// mergeExtras 把 Extra 原文并入输出对象；建模字段优先（防冲突污染）。
func mergeExtras(m map[string]json.RawMessage, extra map[string]json.RawMessage) {
	for k, v := range extra {
		if _, exists := m[k]; !exists {
			m[k] = v
		}
	}
}

// marshalJSON 序列化（关闭 HTML 转义——wire 保真，避免 < > & 被转成 \u003c 形态）。
func marshalJSON(v any) (json.RawMessage, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	b := buf.Bytes()
	if n := len(b); n > 0 && b[n-1] == '\n' {
		b = b[:n-1]
	}
	return append(json.RawMessage(nil), b...), nil
}

func marshalBytes(v any) ([]byte, error) {
	b, err := marshalJSON(v)
	if err != nil {
		return nil, err
	}
	return []byte(b), nil
}

// mustMarshal 仅用于不可能失败的内部序列化（string/int64/map[RawMessage]）。
func mustMarshal(v any) json.RawMessage {
	b, err := marshalJSON(v)
	if err != nil {
		panic(err)
	}
	return b
}
