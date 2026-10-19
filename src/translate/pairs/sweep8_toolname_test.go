// W5-25 条目 1：gemini 腿的工具名双向映射（r14 的原始协议路径）。
//
// r14 实测：CC 84 字名 → gemini 富载体 200（按截断名回调）→ 网关还原 → 74 字原名
// 工具真执行。本文件锁 pairs 面的两层：
//  1. 请求面：openai_chat → gemini_generate，长名在 functionDeclarations 里被截断
//     到 64（且不与 schema 清洗冲突）；
//  2. 响应面：gemini functionCall（截断名）→ 出口 IR 还原成原名。
package pairs

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/toolname"
)

var sweep8LongName = "mcp__srv__" + strings.Repeat("c", 74)

func TestToolNameGeminiLegRoundTrip(t *testing.T) {
	if len(sweep8LongName) != 84 {
		t.Fatalf("测试构造错误：len=%d", len(sweep8LongName))
	}
	reg := NewRegistry()
	reqBody := []byte(`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],` +
		`"tools":[{"type":"function","function":{"name":"` + sweep8LongName + `","description":"d",` +
		`"parameters":{"type":"object","properties":{"text":{"type":"string"}}}}}]}`)

	table := BuildToolNameMap(fOpenAI, reqBody)
	if len(table) != 1 || table[sweep8LongName] == "" {
		t.Fatalf("建表失败：%v", table)
	}
	san := table[sweep8LongName]
	if len(san) != toolname.MaxLen {
		t.Fatalf("净化名长度 = %d（want 64）", len(san))
	}

	out, err := Convert(reg, fOpenAI, fGemini, FaceRequest, reqBody, Options{ToolNames: table})
	if err != nil {
		t.Fatalf("请求转换: %v", err)
	}
	// gemini 信封：tools[].functionDeclarations[].name 必须是截断名。
	var gem struct {
		Tools []struct {
			FunctionDeclarations []struct {
				Name string `json:"name"`
			} `json:"functionDeclarations"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(out, &gem); err != nil {
		t.Fatalf("gemini 请求不是 JSON: %v\n%s", err, out)
	}
	if len(gem.Tools) != 1 || len(gem.Tools[0].FunctionDeclarations) != 1 {
		t.Fatalf("functionDeclarations 形状 = %s", out)
	}
	if got := gem.Tools[0].FunctionDeclarations[0].Name; got != san {
		t.Fatalf("gemini 声明名 = %q（want %q）", got, san)
	}
	if strings.Contains(string(out), sweep8LongName) {
		t.Fatalf("上游请求仍带原名：%s", out)
	}

	// 响应面：gemini functionCall 按截断名回调 → 还原成原名。
	respBody := []byte(`{"candidates":[{"content":{"role":"model","parts":[` +
		`{"functionCall":{"name":"` + san + `","args":{"text":"hi"}}}]},"finishReason":"STOP"}],` +
		`"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2,"totalTokenCount":5}}`)
	resp, err := Convert(reg, fGemini, fOpenAI, FaceResponse, respBody, Options{ToolNames: table})
	if err != nil {
		t.Fatalf("响应转换: %v", err)
	}
	var down struct {
		Choices []struct {
			Message struct {
				ToolCalls []struct {
					Function struct {
						Name string `json:"name"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(resp, &down); err != nil {
		t.Fatalf("下游响应不是 JSON: %v\n%s", err, resp)
	}
	if len(down.Choices) != 1 || len(down.Choices[0].Message.ToolCalls) != 1 {
		t.Fatalf("tool_calls 形状 = %s", resp)
	}
	if got := down.Choices[0].Message.ToolCalls[0].Function.Name; got != sweep8LongName {
		t.Fatalf("还原名 = %q（want 原名——r14 的客户端可执行前提）", got)
	}
}

// TestToolNameNoMapUntouched：表 nil 时两腿都不动（既有路径零影响）。
func TestToolNameNoMapUntouched(t *testing.T) {
	reg := NewRegistry()
	reqBody := []byte(`{"model":"gpt-x","messages":[{"role":"user","content":"hi"}],` +
		`"tools":[{"type":"function","function":{"name":"plain_tool","parameters":{"type":"object"}}}]}`)
	out, err := Convert(reg, fOpenAI, fGemini, FaceRequest, reqBody, Options{})
	if err != nil {
		t.Fatalf("请求转换: %v", err)
	}
	if !strings.Contains(string(out), `"name":"plain_tool"`) {
		t.Fatalf("合法名被改写：%s", out)
	}
}
