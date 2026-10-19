package service

// W5-37 缺陷小修测试锚点：上游 `api_key` 的 `${VAR}` 环境变量引用。
//
// 背景：活体 E2E 要求 key 只活在进程环境里（配置文件/产物只留引用名）。
// 修前 LoadConfig 不做任何环境展开——`api_key: ${VAR}` 会原样当字面 key
// 打上游。本文件锚定展开语义与「绝不静默降级」的失败面。

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeEnvCfg(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return p
}

const envCfgBase = "listen: \"127.0.0.1:0\"\nupstreams:\n  - name: u\n    base_url: http://127.0.0.1:9\n    api_key: %s\n    protocol: openai_chat\n"

func TestConfigAPIKeyEnvRefExpands(t *testing.T) {
	t.Setenv("A2A_ENV_TEST_KEY", "sk-live-abc123")

	// 纯引用 → 展开为环境值。
	c, err := LoadConfig(writeEnvCfg(t, fmt.Sprintf(envCfgBase, "${A2A_ENV_TEST_KEY}")))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := c.Upstreams[0].APIKey; got != "sk-live-abc123" {
		t.Fatalf("api_key = %q（want 展开值）", got)
	}

	// 前缀/后缀拼接也展开。
	c2, err := LoadConfig(writeEnvCfg(t, fmt.Sprintf(envCfgBase, "pre-${A2A_ENV_TEST_KEY}-post")))
	if err != nil {
		t.Fatalf("load concat: %v", err)
	}
	if got := c2.Upstreams[0].APIKey; got != "pre-sk-live-abc123-post" {
		t.Fatalf("api_key 拼接 = %q", got)
	}

	// 无引用 → 原样（缺省行为不变）。
	c3, err := LoadConfig(writeEnvCfg(t, fmt.Sprintf(envCfgBase, "plain-key")))
	if err != nil {
		t.Fatalf("load plain: %v", err)
	}
	if got := c3.Upstreams[0].APIKey; got != "plain-key" {
		t.Fatalf("字面 api_key = %q", got)
	}

	// `$VAR`（无花括号）= 字面量：只认 `${...}` 形态。
	c4, err := LoadConfig(writeEnvCfg(t, fmt.Sprintf(envCfgBase, "$A2A_ENV_TEST_KEY")))
	if err != nil {
		t.Fatalf("load bare-dollar: %v", err)
	}
	if got := c4.Upstreams[0].APIKey; got != "$A2A_ENV_TEST_KEY" {
		t.Fatalf("裸 $VAR 应原样保留，得到 %q", got)
	}

	// 空 api_key（不引用）仍合法（无 key 上游，行为不变）。
	if _, err := LoadConfig(writeEnvCfg(t, "listen: \"127.0.0.1:0\"\nupstreams:\n  - name: u\n    base_url: http://127.0.0.1:9\n    api_key: \"\"\n    protocol: openai_chat\n")); err != nil {
		t.Fatalf("空 api_key 应仍合法: %v", err)
	}
}

func TestConfigAPIKeyEnvRefErrors(t *testing.T) {
	// 未定义 → 显式报错，含变量名与上游名（绝不静默空 key 打上游）。
	_, err := LoadConfig(writeEnvCfg(t, fmt.Sprintf(envCfgBase, "${A2A_ENV_TEST_MISSING}")))
	if err == nil {
		t.Fatal("未定义引用应报错")
	}
	if !strings.Contains(err.Error(), "A2A_ENV_TEST_MISSING") || !strings.Contains(err.Error(), "u") {
		t.Fatalf("报错应含变量名与上游名: %v", err)
	}

	// 定义了但为空 → 同样显式报错（空 key 打上游 = 凭据面事故）。
	t.Setenv("A2A_ENV_TEST_EMPTY", "")
	_, err = LoadConfig(writeEnvCfg(t, fmt.Sprintf(envCfgBase, "${A2A_ENV_TEST_EMPTY}")))
	if err == nil || !strings.Contains(err.Error(), "A2A_ENV_TEST_EMPTY") {
		t.Fatalf("空值引用应报错: %v", err)
	}

	// 未闭合 `${` → 显式报错。
	_, err = LoadConfig(writeEnvCfg(t, fmt.Sprintf(envCfgBase, "${A2A_ENV_TEST_KEY")))
	if err == nil || !strings.Contains(err.Error(), "unterminated") {
		t.Fatalf("未闭合引用应报错: %v", err)
	}

	// 报错绝不回显其它已解析内容：定义变量后让第二个引用失败，
	// 确认首个引用的值不出现在错误里。
	t.Setenv("A2A_ENV_TEST_SECRET", "sk-secret-value")
	_, err = LoadConfig(writeEnvCfg(t, fmt.Sprintf(envCfgBase, "${A2A_ENV_TEST_SECRET}-${A2A_ENV_TEST_MISSING2}")))
	if err == nil || strings.Contains(err.Error(), "sk-secret-value") {
		t.Fatalf("报错泄露已解析值: %v", err)
	}
}
