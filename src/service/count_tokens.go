package service

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode"

	"github.com/dwgx/any2api/translate/ir"
	"github.com/dwgx/any2api/translate/registry"
)

// ---------------------------------------------------------------------------
// count_tokens 出口（POST /v1/messages/count_tokens，W5-20 条目 2）
// ---------------------------------------------------------------------------
//
// 决策依据（research/topics/count-tokens-batches/README.md §我们的决策，
// P06）：上游为 anthropic → 透传（官方精确值）；其余上游（或 anthropic 上游
// 不可用）→ 本地加权估算。为什么必须实现：我们回 404 会让客户端（Claude Code）
// 降级到第 2 级 = 打一次真实的 Haiku 生成请求计数（要钱 + 延迟，
// tokenEstimation.ts:251-322）——本地估算的成本远低于该降级。
//
// 兼容面（P06 §可复用点 1）：
//   - 接受并忽略生成参数（temperature/top_p/top_k/stream/stop_sequences/stop/
//     max_tokens）——透传前剥除（官方 count_tokens 只收请求输入字段；sub2api
//     sanitizeCountTokensRequestBody 同款剥除），本地估算不读它们；
//   - `?beta=true` 接受（beta SDK 恒带；透传时原值转发）；
//   - 错误：400 invalid_request_error（体非法：非 JSON / 缺 model / 缺 messages）；
//     404 not_found_error 是 P06 §错误形状 为「无法计数」保留的形态——本实现恒有
//     本地兜底（估算器不会失败），不伪造 404；绝不用 500（触发客户端重试面）。

// countTokensPreamble 带 tools 的请求 API 追加的一次性工具前言 token 量。CC
// 实测常数（analyzeContext.ts:73-80）：「The API adds a tool prompt preamble
// (~500 tokens) once per API call when tools are present」——本地估算复刻该
// 量级（一次而非每工具；不这么做工具面会 N 倍膨胀）。
const countTokensPreamble = 500

// countTokensGenerationParams 透传前剥除的生成参数（官方 count_tokens 只收
// 请求输入字段；被上游伪装层/客户端注入的生成参数不能带进透传体——sub2api
// gateway_count_tokens.go:625-646 同款）。
var countTokensGenerationParams = []string{
	"max_tokens", "temperature", "top_p", "top_k", "stream", "stop_sequences", "stop",
}

// handleCountTokens Anthropic count_tokens 出口：结构校验 → anthropic 上游透传
// （按配置顺序，首个 200/400 即返回）→ 兜底本地加权估算。
//
// count_tokens 不读写上游健康状态：404 是能力缺失、不是 messages 面的健康信号，
// 也不进冷却/换上游计数（纯旁路请求）。
func (s *Server) handleCountTokens(w http.ResponseWriter, r *http.Request) {
	entry := registry.FormatClaudeMessages
	body, ok := s.readEntryBody(w, r, entry)
	if !ok {
		return
	}
	var req struct {
		Model    string            `json:"model"`
		Messages []json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeError(w, entry, http.StatusBadRequest,
			&ir.Error{Type: "invalid_request_error", Message: "request body is not valid JSON", Source: "any2api"})
		return
	}
	if req.Model == "" {
		s.writeError(w, entry, http.StatusBadRequest,
			&ir.Error{Type: "invalid_request_error", Message: "model: field required", Source: "any2api"})
		return
	}
	if len(req.Messages) == 0 {
		s.writeError(w, entry, http.StatusBadRequest,
			&ir.Error{Type: "invalid_request_error", Message: "messages: field required", Source: "any2api"})
		return
	}

	// A6 模型别名映射（W5-28 条目 1）：count_tokens 也是请求面（同一上游模型
	// 命名空间）——透传体与本地估算的模型名都按表解析（未配置/未命中 = 透传，
	// 现状不变；别名用户的精确计数才不会因上游不认识别名而丢）。
	model := s.cfg.upstreamModelFor(req.Model)
	if model != req.Model {
		if nb, ok := rewriteModelField(body, model); ok {
			body = nb
		}
	}

	// 上游透传（精确值来源）。200/400 即终：400 = 体被权威计数方拒绝，照实上抛
	//（不把合法错误吞成估算）；404/5xx/传输错误 = 能力或可用性缺失，试下一个
	// anthropic 上游，全部失败才落本地估算。
	beta := r.URL.Query().Get("beta")
	for i := range s.cfg.Upstreams {
		up := s.cfg.Upstreams[i]
		if up.Protocol != ProtocolAnthropic {
			continue
		}
		status, respBody, resp, err := s.forwardCountTokens(r, i, up, body, beta)
		if err != nil {
			log.Printf("any2api: upstream %s count_tokens failed (%v); falling back to local estimate", up.Name, err)
			continue
		}
		if status == http.StatusOK || status == http.StatusBadRequest {
			copyCountTokensResponseHeaders(w, resp)
			if w.Header().Get("request-id") == "" {
				w.Header().Set("request-id", newRequestID())
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write(respBody)
			return
		}
		log.Printf("any2api: upstream %s count_tokens returned %d; falling back to local estimate", up.Name, status)
	}

	// 本地加权估算（P06 §可复用点 3：new-api tokenkit 字符类权重档）。
	text, toolsPresent := countTokensCorpus(body)
	n := estimateTokens(model, text)
	if toolsPresent {
		n += countTokensPreamble
	}
	w.Header().Set("request-id", newRequestID())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"input_tokens":` + strconv.Itoa(n) + `}`))
}

// forwardCountTokens 一次 count_tokens 透传尝试：剥生成参数 → POST
// {base}/v1/messages/count_tokens（客户端带 `?beta=` 时原值转发）→ 返回状态码 /
// 体（限长读） / 响应。err != nil = 传输失败（调用方继续下一个上游或落估算）。
// idx = 上游下标（W11：出站客户端按上游选——proxy / TLS 指纹只作用于该上游）。
func (s *Server) forwardCountTokens(r *http.Request, idx int, up Upstream, body []byte, beta string) (int, []byte, *http.Response, error) {
	target := strings.TrimRight(up.BaseURL, "/") + "/v1/messages/count_tokens"
	if beta != "" {
		target += "?beta=" + url.QueryEscape(beta)
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, target, bytes.NewReader(stripCountTokensParams(body)))
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if up.APIKey != "" {
		req.Header.Set("x-api-key", up.APIKey)
	}
	// anthropic-version 必须收（缺省 2023-06-01）；透传客户端值（与 messages 面
	// forward 同纪律）。beta 头按开放列表透传（P06 §决策 5：不白名单化）。
	av := "2023-06-01"
	if v := r.Header.Get("anthropic-version"); v != "" {
		av = v
	}
	req.Header.Set("anthropic-version", av)
	if v := r.Header.Get("anthropic-beta"); v != "" {
		req.Header.Set("anthropic-beta", v)
	}
	client, err := s.clientFor(idx)
	if err != nil {
		return 0, nil, nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	return resp.StatusCode, respBody, resp, nil
}

// stripCountTokensParams 剥除生成参数（其余字段原值保留；体不是 JSON 对象时原样
// 返回——调用方已校验过对象形态，此处防御性兜底）。
func stripCountTokensParams(body []byte) []byte {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}
	for _, k := range countTokensGenerationParams {
		delete(m, k)
	}
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}

// copyCountTokensResponseHeaders 透传诊断/限流响应头（request-id 是 B10 的诊断
// 关联锚点；ratelimit 族保留上游配额信息）。retry-after / retry-after-ms 过
// retryAfterForwardable 守卫（W14：仅正值转发，0/非正/畸形丢弃——与错误信封
// 面同口径）。
func copyCountTokensResponseHeaders(w http.ResponseWriter, resp *http.Response) {
	rid := resp.Header.Get("request-id")
	if rid == "" {
		rid = resp.Header.Get("x-request-id")
	}
	if rid != "" {
		w.Header().Set("request-id", rid)
	}
	for _, h := range []string{"retry-after", "retry-after-ms"} {
		if v := resp.Header.Get(h); retryAfterForwardable(v) {
			w.Header().Set(h, v)
		}
	}
	copyRateLimitHeaders(w.Header(), resp.Header)
}

// ---------------------------------------------------------------------------
// 本地估算（P06 §可复用点 3：字符类加权，不跑真 tokenizer）
// ---------------------------------------------------------------------------

// countTokensWeights 字符类权重（每特征估计 token 数）。来源：new-api tokenkit
// 的 FamilyClaude / FamilyClaude47 实测校准档（tokenkit/tokenkit.go familyWeights；
// 权重是校准数据，算法为本仓独立实现）。FamilyClaude47 对应 Claude 4.7+ 的新
// tokenizer（官方 Note：同文本约 +30%）。
type countTokensWeights struct {
	Word       float64 // 拉丁词（每段连续字母）
	Number     float64 // 数字串
	CJK        float64 // 每个中日韩字符
	Symbol     float64 // 普通标点
	MathSymbol float64 // 数学符号
	URLDelim   float64 // URL 分隔符 / : ? & = ; # %
	AtSign     float64 // @
	Emoji      float64 // 每个 emoji
	Newline    float64 // 换行/制表
	Space      float64 // 其余空白
}

var (
	countTokensWeightsClaude = countTokensWeights{
		Word: 1.24, Number: 1.53, CJK: 1.38, Symbol: 0.66, MathSymbol: 4.52,
		URLDelim: 0.74, AtSign: 2.82, Emoji: 2.6, Newline: 1.19, Space: 0.14,
	}
	countTokensWeightsClaude47 = countTokensWeights{
		Word: 1.52, Number: 1.77, CJK: 1.4, Symbol: 0.89, MathSymbol: 4.52,
		URLDelim: 1.45, AtSign: 2.82, Emoji: 2.6, Newline: 1.38, Space: 0.21,
	}
)

// estimateTokens 本地加权估算：按字符类累计权重后向上取整。模型名指向 Claude
// 4.7+（或无版本）→ 新 tokenizer 档；其余（含非 claude 模型）→ Claude 档
// （缺省档，P06 §可复用点 3）。
func estimateTokens(model, text string) int {
	w := countTokensWeightsClaude
	if claudeNewTokenizer(model) {
		w = countTokensWeightsClaude47
	}
	var count float64
	run := 0 // 0=无 1=拉丁词 2=数字串
	for _, r := range text {
		switch {
		case unicode.IsSpace(r):
			run = 0
			if r == '\n' || r == '\t' {
				count += w.Newline
			} else {
				count += w.Space
			}
		case isCJKRune(r):
			run = 0
			count += w.CJK
		case isEmojiRune(r):
			run = 0
			count += w.Emoji
		case unicode.IsLetter(r) || unicode.IsNumber(r):
			next := 1
			if unicode.IsNumber(r) {
				next = 2
			}
			if run != next {
				if next == 2 {
					count += w.Number
				} else {
					count += w.Word
				}
				run = next
			}
		default:
			run = 0
			switch {
			case isMathSymbolRune(r):
				count += w.MathSymbol
			case r == '@':
				count += w.AtSign
			case strings.ContainsRune("/:?&=;#%", r):
				count += w.URLDelim
			default:
				count += w.Symbol
			}
		}
	}
	return int(math.Ceil(count))
}

// claudeNewTokenizer 模型名是否指向 Claude 4.7+ 的新 tokenizer。判定口径同
// new-api（claudeUsesOpus47Tokenizer）：取 claude 之后的前两个 1-2 位数字段为
// 主/次版本（跳过词段；日期后缀如 20250929 结束版本）；主版本 >4，或 =4 且
// 次版本 ≥7；无版本段 → 视为新（4.7+ 起为默认）。非 claude 名 → false。
func claudeNewTokenizer(model string) bool {
	m := strings.ToLower(model)
	i := strings.Index(m, "claude")
	if i < 0 {
		return false
	}
	var ver []int
	for _, part := range strings.FieldsFunc(m[i:], func(r rune) bool { return r == '-' || r == '.' }) {
		digits := len(part) - len(strings.TrimLeft(part, "0123456789"))
		if digits == 0 || digits > 2 {
			if len(ver) > 0 {
				break
			}
			continue
		}
		n, _ := strconv.Atoi(part[:digits])
		ver = append(ver, n)
		if len(ver) == 2 || digits < len(part) {
			break
		}
	}
	switch {
	case len(ver) == 0:
		return true
	case ver[0] != 4:
		return ver[0] > 4
	default:
		return len(ver) > 1 && ver[1] >= 7
	}
}

func isCJKRune(r rune) bool {
	return unicode.Is(unicode.Han, r) ||
		(r >= 0x3040 && r <= 0x30FF) || // 日文假名
		(r >= 0xAC00 && r <= 0xD7A3) // 韩文音节
}

func isEmojiRune(r rune) bool {
	return (r >= 0x1F300 && r <= 0x1F9FF) ||
		(r >= 0x2600 && r <= 0x26FF) ||
		(r >= 0x2700 && r <= 0x27BF) ||
		(r >= 0x1FA00 && r <= 0x1FAFF)
}

func isMathSymbolRune(r rune) bool {
	if strings.ContainsRune("∑∫∂√∞≤≥≠≈±×÷∈∉∋∌⊂⊃⊆⊇∪∩∧∨¬∀∃∄∅∆∇∝∟∠∡∢°′″‴⁺⁻⁼⁽⁾ⁿ₀₁₂₃₄₅₆₇₈₉₊₋₌₍₎²³¹⁴⁵⁶⁷⁸⁹⁰", r) {
		return true
	}
	return (r >= 0x2200 && r <= 0x22FF) ||
		(r >= 0x2A00 && r <= 0x2AFF) ||
		(r >= 0x1D400 && r <= 0x1D7FF)
}

// ---------------------------------------------------------------------------
// 计数语料提取（system + messages + tools）
// ---------------------------------------------------------------------------

// countTokensCorpus 从 count_tokens 请求体提取参与计数的文本，口径镜像 new-api
// ClaudeRequest.GetTokenCountMeta（P06 §可复用点 3）：
//
//   - system：字符串或块数组的 text；
//   - messages：role + 内容（text 的文本；tool_use 的 name + input JSON；
//     tool_result 的文本/文本块）；
//   - tools：name + description + input_schema。
//
// image 与其它二进制块不计数（本地无法解码，宁欠估不虚估）；thinking 块不计数
// （与 new-api 同口径）。子结构畸形 → 跳过该段（估算面宽容；顶层结构已由
// handleCountTokens 校验）。返回 (文本, 是否带 tools)。
func countTokensCorpus(body []byte) (string, bool) {
	var req struct {
		System   json.RawMessage   `json:"system"`
		Messages []json.RawMessage `json:"messages"`
		Tools    []json.RawMessage `json:"tools"`
	}
	_ = json.Unmarshal(body, &req) // 顶层已校验；防御性忽略错误

	var b strings.Builder
	appendBlocks := func(raw json.RawMessage, allowToolUse bool) {
		var blocks []struct {
			Type    string          `json:"type"`
			Text    string          `json:"text"`
			Name    string          `json:"name"`
			Input   json.RawMessage `json:"input"`
			Content json.RawMessage `json:"content"`
		}
		if err := json.Unmarshal(raw, &blocks); err != nil {
			return
		}
		for _, blk := range blocks {
			switch blk.Type {
			case "text":
				b.WriteString(blk.Text)
				b.WriteByte('\n')
			case "tool_use":
				if !allowToolUse {
					continue
				}
				b.WriteString(blk.Name)
				b.WriteByte('\n')
				if len(blk.Input) > 0 {
					b.Write(blk.Input)
					b.WriteByte('\n')
				}
			case "tool_result":
				// 工具输出按模型读到的文本计；image 等其它块跳过（不把 base64
				// 当文本——new-api 同款纪律）。
				var s string
				if json.Unmarshal(blk.Content, &s) == nil {
					b.WriteString(s)
					b.WriteByte('\n')
					continue
				}
				var inner []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				}
				if json.Unmarshal(blk.Content, &inner) == nil {
					for _, ib := range inner {
						if ib.Type == "text" {
							b.WriteString(ib.Text)
							b.WriteByte('\n')
						}
					}
				}
			}
		}
	}

	if len(req.System) > 0 {
		var s string
		if json.Unmarshal(req.System, &s) == nil {
			b.WriteString(s)
			b.WriteByte('\n')
		} else {
			appendBlocks(req.System, false)
		}
	}
	for _, raw := range req.Messages {
		var m struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		}
		if json.Unmarshal(raw, &m) != nil {
			continue
		}
		b.WriteString(m.Role)
		b.WriteByte('\n')
		var s string
		if json.Unmarshal(m.Content, &s) == nil {
			b.WriteString(s)
			b.WriteByte('\n')
			continue
		}
		appendBlocks(m.Content, true)
	}
	for _, raw := range req.Tools {
		var t struct {
			Name        string          `json:"name"`
			Description string          `json:"description"`
			InputSchema json.RawMessage `json:"input_schema"`
		}
		if json.Unmarshal(raw, &t) != nil {
			continue
		}
		b.WriteString(t.Name)
		b.WriteByte('\n')
		b.WriteString(t.Description)
		b.WriteByte('\n')
		if len(t.InputSchema) > 0 {
			b.Write(t.InputSchema)
			b.WriteByte('\n')
		}
	}
	return b.String(), len(req.Tools) > 0
}
