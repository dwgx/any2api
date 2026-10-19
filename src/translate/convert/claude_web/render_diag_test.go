package claude_web

// W21-2 ④ 注入段字节账（观测面）：RenderOptions.Diag sink（与 openai_chat 的
// RenderOptions.Diag 同模式）+ 观测钩子（服务日志注册面，见 service/claude_web.go）。
// 口径：字节数 == len(toolsim.BuildInjection(...))；prompt 增量 == 字节数 + 2
// （分隔空行）；ToolSim 关 / 无工具 → 不注入、不记账、不回调。
//
// 反证锚点：删掉 RenderRequestWithOptions 的 Diag/钩子记录块 → 本文件全红；
// 把字节数记成 len(prompt 增量)（含分隔空行）→ 口径断言红。

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/toolsim"
)

// promptOfBody 渲染体 → prompt 原文。
func promptOfBody(t *testing.T, raw []byte) string {
	t.Helper()
	var w map[string]json.RawMessage
	if err := json.Unmarshal(raw, &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var p string
	if err := json.Unmarshal(w["prompt"], &p); err != nil {
		t.Fatalf("prompt: %v", err)
	}
	return p
}

// TestRenderRequestToolSimDiagBytes 字节账口径：sink 字节数 == BuildInjection 长度；
// prompt 增量 == 字节数 + 2（"\n\n" 分隔）；工具条数 == 请求声明数。
func TestRenderRequestToolSimDiagBytes(t *testing.T) {
	req := toolsimReq()
	inj := toolsim.BuildInjection(req.Tools, req.ToolChoice)
	if inj == "" {
		t.Fatal("构造错误：注入段为空")
	}

	diag := &RenderDiagnostics{}
	on, err := RenderRequestWithOptions(req, RenderOptions{ToolSim: true, Diag: diag})
	if err != nil {
		t.Fatalf("RenderRequestWithOptions(on): %v", err)
	}
	if diag.ToolSimInjectionBytes != len(inj) || diag.ToolSimTools != len(req.Tools) {
		t.Fatalf("字节账 = %d bytes / %d tools, want %d / %d",
			diag.ToolSimInjectionBytes, diag.ToolSimTools, len(inj), len(req.Tools))
	}

	off, err := RenderRequestWithOptions(req, RenderOptions{})
	if err != nil {
		t.Fatalf("RenderRequestWithOptions(off): %v", err)
	}
	onPrompt, offPrompt := promptOfBody(t, on), promptOfBody(t, off)
	if !strings.HasPrefix(onPrompt, inj+"\n\n") {
		t.Fatalf("注入段不在 prompt 最前（前缀 = %q）:\n%.200s", inj[:40], onPrompt)
	}
	if got, want := len(onPrompt)-len(offPrompt), diag.ToolSimInjectionBytes+2; got != want {
		t.Fatalf("prompt 增量 = %d, want %d（字节账 + 分隔空行）", got, want)
	}
}

// TestRenderRequestToolSimDiagOff 未注入不记账：ToolSim 关 / 无工具 → sink 保持零值。
func TestRenderRequestToolSimDiagOff(t *testing.T) {
	req := toolsimReq()
	diag := &RenderDiagnostics{}
	if _, err := RenderRequestWithOptions(req, RenderOptions{Diag: diag}); err != nil {
		t.Fatalf("off: %v", err)
	}
	if diag.ToolSimInjectionBytes != 0 || diag.ToolSimTools != 0 {
		t.Fatalf("ToolSim 关被记账：%+v", diag)
	}

	noTools := toolsimReq()
	noTools.Tools = nil
	noTools.ToolChoice = nil
	diag2 := &RenderDiagnostics{}
	if _, err := RenderRequestWithOptions(noTools, RenderOptions{ToolSim: true, Diag: diag2}); err != nil {
		t.Fatalf("no tools: %v", err)
	}
	if diag2.ToolSimInjectionBytes != 0 || diag2.ToolSimTools != 0 {
		t.Fatalf("无工具被记账：%+v", diag2)
	}
}

// TestRenderRequestToolSimObserver 观测钩子：注入渲染回调一次（同数字）；ToolSim 关
// 不回调；注销后不回调。
func TestRenderRequestToolSimObserver(t *testing.T) {
	var calls, gotBytes, gotTools int
	SetToolSimInjectionObserver(func(b, n int) {
		calls++
		gotBytes, gotTools = b, n
	})
	defer SetToolSimInjectionObserver(nil)

	req := toolsimReq()
	inj := toolsim.BuildInjection(req.Tools, req.ToolChoice)
	if _, err := RenderRequestWithOptions(req, RenderOptions{ToolSim: true}); err != nil {
		t.Fatalf("on: %v", err)
	}
	if calls != 1 || gotBytes != len(inj) || gotTools != len(req.Tools) {
		t.Fatalf("回调 = %d 次 (%d bytes, %d tools), want 1 (%d, %d)", calls, gotBytes, gotTools, len(inj), len(req.Tools))
	}

	if _, err := RenderRequestWithOptions(req, RenderOptions{}); err != nil {
		t.Fatalf("off: %v", err)
	}
	if calls != 1 {
		t.Fatalf("ToolSim 关仍回调（%d 次）", calls)
	}

	SetToolSimInjectionObserver(nil)
	if _, err := RenderRequestWithOptions(req, RenderOptions{ToolSim: true}); err != nil {
		t.Fatalf("after unregister: %v", err)
	}
	if calls != 1 {
		t.Fatalf("注销后仍回调（%d 次）", calls)
	}
}
