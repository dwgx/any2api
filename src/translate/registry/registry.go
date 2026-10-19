// Package registry 实现 (from,to) 转换注册表：显式对 + 枢纽兜底 + 缺省报错。
//
// 结构 = CPA 显式对注册表（MIT，可借鉴代码：sdk/translator/registry.go 的
// (入口,上游) 二维表）+ new-api 的枢纽兜底与 Quality 降级标签（AGPL，只抄范式）
// + 修正 CPA 的默认值：查不到时绝不静默透传（CPA registry.go:148-155 是最危险的
// 事故面），而是按优先级降级：① 显式转换器 → ② 经枢纽两步合成（打 Quality 标签）
// → ③ 返回明确的 ErrNoConversion。
//
// 依据：docs/architecture.md 决策 3；arch-options §2.2「转换注册表范式对照」、
// §2.5 推荐 3、§陷阱 1-2。
//
// 本包只做「解析与分级」（哪个转换器、什么质量），不执行转换——
// 转换函数由 translate/convert/* 提供，服务壳按 Plan 组装执行。
package registry

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Format 线格式标识。常量先登记第一批转换对的成员（arch-options §2.5 推荐 4）。
type Format string

const (
	FormatOpenAIChat      Format = "openai_chat"
	FormatOpenAIResponses Format = "openai_responses"
	FormatClaudeMessages  Format = "claude_messages"
	FormatGeminiGenerate  Format = "gemini_generate"
	FormatInteractions    Format = "interactions"
	// FormatClaudeWeb claude.ai 网页线（completion SSE）——**上游叶子格式**：
	// 只作上游（服务壳把入口请求渲染成 claude.ai 体、把 completion 流解析成 IR），
	// 不作下游入口/出口。转换器 translate/convert/claude_web（W10 新增）。
	FormatClaudeWeb Format = "claude_web"
	// FormatChatGPTWeb chatgpt.com 网页线（completion SSE）——**上游叶子格式**：
	// 只作上游（服务壳把入口请求渲染成 chatgpt 体、把 completion 流解析成 IR），
	// 不作下游入口/出口。转换器 translate/convert/chatgpt_web（W28 S2 parse 侧；
	// 请求渲染/接线属 S3，挑战求解属 S4）。
	FormatChatGPTWeb Format = "chatgpt_web"
)

// Quality 转换质量标签（new-api 范式：Good/Fair/Discouraged）。
// 约定：Discouraged 的显式对仍可用，但调用方（服务壳/路由）应把它当作
// 降级信号（可观测、可告警）；枢纽两步合成的计划一律不高于 Fair
// （两步合成会丢信息——arch-options §陷阱 2）。
type Quality string

const (
	QualityGood        Quality = "good"
	QualityFair        Quality = "fair"
	QualityDiscouraged Quality = "discouraged"
)

func qualityRank(q Quality) int {
	switch q {
	case QualityGood:
		return 0
	case QualityFair:
		return 1
	case QualityDiscouraged:
		return 2
	default:
		return 3 // 未知质量视为最差，防止默认放行
	}
}

func worseQuality(a, b Quality) Quality {
	if qualityRank(b) > qualityRank(a) {
		return b
	}
	return a
}

// Conversion 一条显式转换对（注册项）。
type Conversion struct {
	From    Format
	To      Format
	Quality Quality
}

// Step 计划中的一步转换。
type Step struct {
	From Format
	To   Format
}

// Plan 转换计划：Steps 长度 1 = 显式对直转；长度 2 = 经枢纽两步合成。
type Plan struct {
	Steps   []Step
	Quality Quality
	ViaHub  bool
}

// 注册表错误哨兵。
var (
	// ErrNoConversion 缺省无转换——显式错误，绝不静默透传（本包存在的理由）。
	ErrNoConversion = errors.New("any2api/registry: no conversion available")
	// ErrDuplicateConversion 重复注册同一 (from,to)。
	ErrDuplicateConversion = errors.New("any2api/registry: conversion already registered")
	// ErrIdentityConversion 同格式对不是转换（同协议走字节级直通路径，不进注册表）。
	ErrIdentityConversion = errors.New("any2api/registry: identity conversion not allowed")
	// ErrInvalidFormat 空格式标识。
	ErrInvalidFormat = errors.New("any2api/registry: empty format")
)

// NoConversionError 携带 from/to 明细的无转换错误；errors.Is(err, ErrNoConversion) 为真。
type NoConversionError struct {
	From Format
	To   Format
	// Hub 为空表示未配置枢纽；非空但缺腿时 Missing 列出缺失的步骤。
	Hub     Format
	Missing []Step
}

func (e *NoConversionError) Error() string {
	msg := fmt.Sprintf("%s: %s -> %s", ErrNoConversion.Error(), e.From, e.To)
	if e.Hub != "" {
		msg += fmt.Sprintf(" (hub %s)", e.Hub)
	}
	for _, m := range e.Missing {
		msg += fmt.Sprintf("; missing %s -> %s", m.From, m.To)
	}
	return msg
}

func (e *NoConversionError) Unwrap() error { return ErrNoConversion }

// Registry (from,to) 转换注册表。并发安全（注册在启动期，查询在运行期）。
type Registry struct {
	mu      sync.RWMutex
	hub     Format
	entries map[Step]Quality
}

// New 创建注册表。hub 为枢纽兜底格式（如 FormatOpenAIChat）；空串 = 无枢纽。
func New(hub Format) *Registry {
	return &Registry{hub: hub, entries: map[Step]Quality{}}
}

// Hub 返回配置的枢纽格式（可能为空串）。
func (r *Registry) Hub() Format { return r.hub }

// Register 注册一条显式转换对。
func (r *Registry) Register(c Conversion) error {
	if c.From == "" || c.To == "" {
		return fmt.Errorf("%w: from=%q to=%q", ErrInvalidFormat, c.From, c.To)
	}
	if c.From == c.To {
		return fmt.Errorf("%w: %s", ErrIdentityConversion, c.From)
	}
	if c.Quality == "" {
		c.Quality = QualityGood
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	k := Step{From: c.From, To: c.To}
	if _, dup := r.entries[k]; dup {
		return fmt.Errorf("%w: %s -> %s", ErrDuplicateConversion, c.From, c.To)
	}
	r.entries[k] = c.Quality
	return nil
}

// MustRegister 启动期注册；失败即 panic（初始化错误不允许静默）。
func (r *Registry) MustRegister(c Conversion) {
	if err := r.Register(c); err != nil {
		panic(err)
	}
}

// Lookup 解析转换计划。优先级（arch-options §2.2）：
//  1. 显式对直转（Quality 取注册值）；
//  2. 经枢纽两步合成（hub 非空且两腿均已注册；Quality = 两腿较差者，且不高于 Fair）；
//  3. 否则 *NoConversionError（errors.Is 匹配 ErrNoConversion）。
//
// 同格式（from==to）不查表：直通是另一条路径（本包不负责），显式返回错误。
func (r *Registry) Lookup(from, to Format) (Plan, error) {
	if from == "" || to == "" {
		return Plan{}, fmt.Errorf("%w: from=%q to=%q", ErrInvalidFormat, from, to)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	if from == to {
		return Plan{}, &NoConversionError{From: from, To: to,
			Missing: nil}
	}
	if q, ok := r.entries[Step{From: from, To: to}]; ok {
		return Plan{Steps: []Step{{From: from, To: to}}, Quality: q}, nil
	}
	if r.hub != "" && from != r.hub && to != r.hub {
		leg1, ok1 := r.entries[Step{From: from, To: r.hub}]
		leg2, ok2 := r.entries[Step{From: r.hub, To: to}]
		if ok1 && ok2 {
			q := worseQuality(worseQuality(leg1, leg2), QualityFair)
			return Plan{
				Steps:   []Step{{From: from, To: r.hub}, {From: r.hub, To: to}},
				Quality: q,
				ViaHub:  true,
			}, nil
		}
		missing := make([]Step, 0, 2)
		if !ok1 {
			missing = append(missing, Step{From: from, To: r.hub})
		}
		if !ok2 {
			missing = append(missing, Step{From: r.hub, To: to})
		}
		return Plan{}, &NoConversionError{From: from, To: to, Hub: r.hub, Missing: missing}
	}
	return Plan{}, &NoConversionError{From: from, To: to, Hub: r.hub}
}

// Formats 返回已注册的格式集合（去重排序；用于诊断与测试）。
func (r *Registry) Formats() []Format {
	r.mu.RLock()
	defer r.mu.RUnlock()
	set := map[Format]struct{}{}
	for s := range r.entries {
		set[s.From] = struct{}{}
		set[s.To] = struct{}{}
	}
	out := make([]Format, 0, len(set))
	for f := range set {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
