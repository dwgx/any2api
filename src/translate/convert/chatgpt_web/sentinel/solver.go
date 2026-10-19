package sentinel

import (
	"context"
	"sync"
)

// 本文件 = 第三方求解器客户端**接口面 + FakeSolver**（brief §Change ③）。
//
// 只定义接口与测试用假实现——**不接任何第三方求解服务、不发请求**（红线：零
// 真机）。形态照抄 chat2api 的调用点（lanqian528/chat2api@85971f0
// chatgpt/ChatService.py:207-243）：
//
//	Turnstile：POST solver_url  {url:"https://chatgpt.com", p:<本次 p token>, dx, ua}
//	           → 响应 {"t": <turnstile token>}（:207-216；缺 t 即忽略/降级）
//	Arkose   ：POST solver_url  {blob:<arkose.dx>, method:"chat35"|"chat4"}
//	           → 响应 {"solved": bool, "token": <token>}（:218-243）
//
// 位置：两者都在 chat-requirements 响应体里按需出题（不是独立端点），
// turnstile 的 dx 与**发起请求的那份 p** 绑定（turb-gpt 深读 §2.2：dx 用该 p 做
// XOR 混淆——所以 TurnstileRequest 必须带上本次 p）。

// TurnstileSolver 第三方 turnstile 求解服务客户端。
type TurnstileSolver interface {
	// SolveTurnstile 提交一次 turnstile 挑战，返回 turnstile token（"t"）。
	// 失败必须返回错误——调用方（S4 服务壳）决定降级/忽略（chat2api 忽略
	// turnstile 异常继续走，:214-216；那是产品决策，不在本包）。
	SolveTurnstile(ctx context.Context, req TurnstileRequest) (string, error)
}

// TurnstileRequest 是 turnstile 求解服务的请求体（chat2api 形态）。
type TurnstileRequest struct {
	// URL 出题页（chat2api 恒 "https://chatgpt.com"——DefaultTurnstileURL）。
	URL string
	// P 本次 requirements p token（**同一份 p**——dx 与该 p 绑定）。
	P string
	// DX 响应里的 turnstile.dx。
	DX string
	// UA 与 HTTP 层同源的 user agent。
	UA string
}

// DefaultTurnstileURL 是 chat2api 请求体里硬编码的 url 值（ChatService.py:210）。
const DefaultTurnstileURL = "https://chatgpt.com"

// ArkoseSolver 第三方 arkose 求解服务客户端。
type ArkoseSolver interface {
	// SolveArkose 提交一次 arkose blob，返回 arkose token。
	SolveArkose(ctx context.Context, req ArkoseRequest) (string, error)
}

// ArkoseRequest 是 arkose 求解服务的请求体（chat2api 形态）。
type ArkoseRequest struct {
	// Blob 响应里的 arkose.dx（chat2api 字段名 "blob"）。
	Blob string
	// Method "chat35" | "chat4"（见 ArkoseMethod）。
	Method string
}

// ArkoseMethod 按 persona 选求解方法（ChatService.py:220-224 原文：
// `if self.persona == "chatgpt-freeaccount": "chat35" else: "chat4"`）。
//
// persona 取值面（KB §Sentinel）：chatgpt-paid / chatgpt-noauth（匿名档实抓，
// W29-1）/ chatgpt-freeaccount / …。非 freeaccount 一律 "chat4"。
func ArkoseMethod(persona string) string {
	if persona == "chatgpt-freeaccount" {
		return "chat35"
	}
	return "chat4"
}

// FakeSolver 是测试用假求解器（实现两个接口）：记录调用 + 返回预设值。
// 并发安全（S4 服务壳可能并发消费）。
type FakeSolver struct {
	mu sync.Mutex

	// TurnstileToken / ArkoseToken 是成功路径的返回值。
	TurnstileToken string
	ArkoseToken    string
	// TurnstileErr / ArkoseErr 非 nil 时优先返回（模拟求解失败）。
	TurnstileErr error
	ArkoseErr    error

	turnstileCalls []TurnstileRequest
	arkoseCalls    []ArkoseRequest
}

// SolveTurnstile 实现 TurnstileSolver。
func (f *FakeSolver) SolveTurnstile(_ context.Context, req TurnstileRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.turnstileCalls = append(f.turnstileCalls, req)
	if f.TurnstileErr != nil {
		return "", f.TurnstileErr
	}
	return f.TurnstileToken, nil
}

// SolveArkose 实现 ArkoseSolver。
func (f *FakeSolver) SolveArkose(_ context.Context, req ArkoseRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.arkoseCalls = append(f.arkoseCalls, req)
	if f.ArkoseErr != nil {
		return "", f.ArkoseErr
	}
	return f.ArkoseToken, nil
}

// TurnstileCalls 返回已记录的 turnstile 请求副本。
func (f *FakeSolver) TurnstileCalls() []TurnstileRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]TurnstileRequest(nil), f.turnstileCalls...)
}

// ArkoseCalls 返回已记录的 arkose 请求副本。
func (f *FakeSolver) ArkoseCalls() []ArkoseRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ArkoseRequest(nil), f.arkoseCalls...)
}

// 编译期接口断言。
var (
	_ TurnstileSolver = (*FakeSolver)(nil)
	_ ArkoseSolver    = (*FakeSolver)(nil)
)
