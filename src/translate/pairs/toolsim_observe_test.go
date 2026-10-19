package pairs

// W21-2 ④ 观测钩子过 pairs 全环：Convert(FaceRequest) 的 claude_web 注入渲染触发
// 钩子一次，字节账与 prompt 增量对账（字节数 + 2 = prompt 长度差）；ToolSim 关
// 不回调（缺省关零影响）。
//
// 反证锚点：删掉 claude_web 渲染里的钩子调用 → 本文件红。

import (
	"encoding/json"
	"testing"

	"github.com/dwgx/any2api/translate/convert/claude_web"
)

func TestToolSimInjectionObserverThroughConvert(t *testing.T) {
	reg := NewRegistry()
	body := []byte(`{"model":"m","max_tokens":16,"messages":[{"role":"user","content":"hi"}],` +
		`"tools":[{"name":"Read","description":"Reads a file.",` +
		`"input_schema":{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}}]}`)

	var calls, gotBytes, gotTools int
	claude_web.SetToolSimInjectionObserver(func(b, n int) {
		calls++
		gotBytes, gotTools = b, n
	})
	defer claude_web.SetToolSimInjectionObserver(nil)

	on, err := Convert(reg, fClaude, FormatClaudeWeb, FaceRequest, body, Options{ToolSim: true})
	if err != nil {
		t.Fatalf("Convert(on): %v", err)
	}
	off, err := Convert(reg, fClaude, FormatClaudeWeb, FaceRequest, body, Options{})
	if err != nil {
		t.Fatalf("Convert(off): %v", err)
	}

	if calls != 1 || gotTools != 1 || gotBytes <= 0 {
		t.Fatalf("钩子 = %d 次 (%d bytes, %d tools), want 1 (>0, 1)", calls, gotBytes, gotTools)
	}
	var onPrompt, offPrompt string
	if err := json.Unmarshal(decodeBody(t, on)["prompt"], &onPrompt); err != nil {
		t.Fatalf("on prompt: %v", err)
	}
	if err := json.Unmarshal(decodeBody(t, off)["prompt"], &offPrompt); err != nil {
		t.Fatalf("off prompt: %v", err)
	}
	if got, want := len(onPrompt)-len(offPrompt), gotBytes+2; got != want {
		t.Fatalf("prompt 增量 = %d, want %d（钩子字节数 + 分隔空行）", got, want)
	}

	// ToolSim 关：不回调（钩子只在注入渲染后触发）。
	if _, err := Convert(reg, fClaude, FormatClaudeWeb, FaceRequest, body, Options{}); err != nil {
		t.Fatalf("Convert(off#2): %v", err)
	}
	if calls != 1 {
		t.Fatalf("ToolSim 关仍回调（%d 次）", calls)
	}
}
