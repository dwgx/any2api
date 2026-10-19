// A2A-L19-W82-1：任务创建面（taskFaceTarget）也按 `models[].upstreams` 选路。
// 非 Claude 模型的托管任务不得先选 claude_web（W81-2 残余；W81-3 真实探针）。
package service

import (
	"testing"
)

// taskRoutingCfg 两台 managed 上游：claude-web 在链首（最危险），upstream-a 是
// deepseek-v4.1-flash 的唯一白名单上游。mut 用于逐例改 task_face。
func taskRoutingCfg(mut func(*Config)) *Config {
	cfg := &Config{
		Listen:           "127.0.0.1:0",
		HeartbeatSeconds: 15,
		Upstreams: []Upstream{
			{Name: "claude-web", Protocol: ProtocolClaudeWeb, APIKey: "k1", TaskFace: taskFaceManaged},
			{Name: "upstream-a", BaseURL: "http://127.0.0.1:1", Protocol: ProtocolOpenAIChat, APIKey: "k2", TaskFace: taskFaceManaged},
		},
		Models: []ModelEntry{
			{ID: "deepseek-v4.1-flash", Upstreams: []string{"upstream-a"}},
		},
	}
	if mut != nil {
		mut(cfg)
	}
	return cfg
}

func newTaskRoutingServer(t *testing.T, mut func(*Config)) *Server {
	t.Helper()
	cfg := taskRoutingCfg(mut)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("cfg: %v", err)
	}
	return New(cfg)
}

func TestTaskFaceTargetModelFilter(t *testing.T) {
	// 基线：两台都 managed。映射模型 → 只允许 upstream-a（链首 claude-web 被路由
	// 排除）；未映射 Claude 模型 → 保持旧行为（首个配置上游 = claude-web）。
	t.Run("mapped_model_targets_allowlisted_upstream", func(t *testing.T) {
		s := newTaskRoutingServer(t, nil)
		idx, face, ok := s.taskFaceTarget("deepseek-v4.1-flash")
		if !ok || idx != 1 || face != taskFaceManaged {
			t.Fatalf("taskFaceTarget(deepseek) = (%d, %q, %v), want (1, managed, true)", idx, face, ok)
		}
	})

	t.Run("unmapped_model_keeps_first_configured", func(t *testing.T) {
		s := newTaskRoutingServer(t, nil)
		idx, face, ok := s.taskFaceTarget("claude-sonnet-5-5")
		if !ok || idx != 0 || face != taskFaceManaged {
			t.Fatalf("taskFaceTarget(unmapped) = (%d, %q, %v), want (0, managed, true)", idx, face, ok)
		}
	})

	// task_face: off 过滤仍生效（白名单 + off 叠加）。
	t.Run("allowlisted_upstream_off_has_no_target", func(t *testing.T) {
		s := newTaskRoutingServer(t, func(cfg *Config) {
			cfg.Upstreams[1].TaskFace = taskFaceOff // upstream-a（唯一白名单台）关闭任务面
		})
		idx, face, ok := s.taskFaceTarget("deepseek-v4.1-flash")
		if ok || idx != 0 || face != "" {
			t.Fatalf("taskFaceTarget(deepseek, buddy off) = (%d, %q, %v), want (0, \"\", false)", idx, face, ok)
		}
	})

	// 未映射模型：链首 off 时仍跳过（既有 taskFace 语义不变）。
	t.Run("unmapped_model_skips_off_upstream", func(t *testing.T) {
		s := newTaskRoutingServer(t, func(cfg *Config) {
			cfg.Upstreams[0].TaskFace = taskFaceOff // claude-web 关闭任务面
		})
		idx, face, ok := s.taskFaceTarget("claude-sonnet-5-5")
		if !ok || idx != 1 || face != taskFaceManaged {
			t.Fatalf("taskFaceTarget(unmapped, claude-web off) = (%d, %q, %v), want (1, managed, true)", idx, face, ok)
		}
	})
}
