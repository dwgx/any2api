// A2A-L19-W81-2：`models[].upstreams` 白名单的配置校验（LoadConfig 路径——
// 同时证明 yaml 标签与 Validate 接线）。缺省/空列表 = 不限制（向后兼容）；
// 未知名/同表重复 = 配置错误。
package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// loadRoutingConfig 把 YAML 写进临时文件并走真实 LoadConfig（含 KnownFields）。
func loadRoutingConfig(t *testing.T, body string) (*Config, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return LoadConfig(p)
}

const routingConfigBase = `
listen: 127.0.0.1:0
upstreams:
  - name: claude-web
    protocol: claude_web
    api_key: k1
  - name: upstream-a
    base_url: http://127.0.0.1:1
    protocol: openai_chat
    api_key: k2
`

func TestModelUpstreamsValidation(t *testing.T) {
	t.Run("allowlist_ok", func(t *testing.T) {
		cfg, err := loadRoutingConfig(t, routingConfigBase+`
models:
  - id: deepseek
    upstream_model: deepseek-v4
    upstreams: [upstream-a]
`)
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}
		got := cfg.modelUpstreams("deepseek")
		if len(got) != 1 || !got["upstream-a"] {
			t.Fatalf("modelUpstreams(deepseek) = %v, want {upstream-a}", got)
		}
		if cfg.modelUpstreams("not-configured") != nil {
			t.Fatalf("未配置模型必须不限制（nil）")
		}
	})

	t.Run("absent_is_unrestricted", func(t *testing.T) {
		cfg, err := loadRoutingConfig(t, routingConfigBase+`
models:
  - id: claude-sonnet
`)
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}
		if got := cfg.modelUpstreams("claude-sonnet"); got != nil {
			t.Fatalf("缺省 upstreams 必须不限制（nil），got %v", got)
		}
	})

	t.Run("empty_list_is_unrestricted", func(t *testing.T) {
		cfg, err := loadRoutingConfig(t, routingConfigBase+`
models:
  - id: claude-sonnet
    upstreams: []
`)
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}
		if got := cfg.modelUpstreams("claude-sonnet"); got != nil {
			t.Fatalf("空 upstreams 必须不限制（nil），got %v", got)
		}
	})

	t.Run("unknown_upstream", func(t *testing.T) {
		_, err := loadRoutingConfig(t, routingConfigBase+`
models:
  - id: deepseek
    upstreams: [upstream-nope]
`)
		if err == nil {
			t.Fatal("未知上游名必须报错")
		}
		for _, want := range []string{`models[deepseek].upstreams[0]`, `unknown upstream "upstream-nope"`} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error = %q, want substring %q", err, want)
			}
		}
	})

	t.Run("duplicate_upstream", func(t *testing.T) {
		_, err := loadRoutingConfig(t, routingConfigBase+`
models:
  - id: deepseek
    upstreams: [upstream-a, upstream-a]
`)
		if err == nil {
			t.Fatal("同表重复上游名必须报错")
		}
		for _, want := range []string{`models[deepseek].upstreams`, `duplicate upstream "upstream-a"`} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error = %q, want substring %q", err, want)
			}
		}
	})
}
