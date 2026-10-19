// admin_model_usage_test.go —— 逐模型台账（W78-1 第 7 项）测试。
//
// 装置：真 Server（**真实 Handler**）+ httptest 假上游（claude 流样本，零外部
// 网络）+ SQLite store。请求面走 `/v1/chat/completions` 入口（chat → anthropic
// 转换），与生产路径同源。
//
// 反证锚点（回退即红）：
//   - **负控 3**：把 `recordModelUsage` 挪进 `recordTurn`（只在「有 conv」时执行）
//     → TestAdminModelUsagePerModelCounters 红（本文件请求**不带**
//     `X-A2A-Conversation-Id`——没有 conv 就不该丢模型计数）；
//   - `recordModelUsage` 顺手写 global/session → 差分断言红（种下的 global 行必须
//     逐字节不变、session/key 必须零行）；
//   - scope 词表不认 `model:` → `/admin/usage?scope=model` 400 红。
package service

import (
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// adminModelUsageConfig 逐模型台账配置：store=sqlite + 单假上游（**无 public 限额**：
// cost_units 面因此为 0——公共档关闭时没有成本档，如实回 0 不冒充）。
func adminModelUsageConfig(t *testing.T, key string, up Upstream) *Config {
	t.Helper()
	cfg := uiTestConfig()
	cfg.HeartbeatSeconds = 15
	cfg.AdminKey = key
	cfg.Store = &StoreSection{Enabled: true, Path: filepath.Join(t.TempDir(), "any2api.db")}
	cfg.Upstreams = []Upstream{up}
	return cfg
}

// adminModelChat 发一次流式 chat（非 200 即失败）+ 读尽整体。
func adminModelChat(t *testing.T, base, model string) {
	t.Helper()
	body := `{"model":"` + model + `","messages":[{"role":"user","content":"hi"}],"stream":true}`
	resp, err := http.Post(base+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post chat (%s): %v", model, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("chat (%s) = %d（want 200）", model, resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 1. 每模型计数：同模型 2 次 → requests=2；不同模型分行；global/session 不变
// ---------------------------------------------------------------------------

func TestAdminModelUsagePerModelCounters(t *testing.T) {
	const key = "w78-key"
	up := goodClaudeUpstream(t)
	s, srv := adminOpsStart(t, adminModelUsageConfig(t, key, anthUp("fake", up.srv.URL)))

	// 种一行 global（差分锚：模型计数**绝不**碰 global——它由 budgetLedger.admit 写）。
	day := storeDay(time.Now())
	if err := s.store.UsageAdd(storeUsageScopeGlobal, day, 7, 7); err != nil {
		t.Fatalf("seed global: %v", err)
	}

	adminModelChat(t, srv.URL, "model-a")
	adminModelChat(t, srv.URL, "model-a")
	adminModelChat(t, srv.URL, "model-b")

	var models adminUsageResponse
	adminAPIGetJSON(t, srv.URL+"/admin/usage?scope=model", key, &models)
	if models.Scope != "model" || len(models.Rows) != 2 {
		t.Fatalf("model rows = %+v", models)
	}
	// 展示形态 = 原样模型名（不截断——模型名不是秘密）。
	if models.Rows[0].Scope != "model:model-a" || models.Rows[0].Requests != 2 {
		t.Fatalf("model-a 行 = %+v", models.Rows[0])
	}
	if models.Rows[1].Scope != "model:model-b" || models.Rows[1].Requests != 1 {
		t.Fatalf("model-b 行 = %+v", models.Rows[1])
	}
	if models.Totals.Requests != 3 || models.Totals.CostUnits != 0 {
		t.Fatalf("model totals = %+v（公共档关闭 → units 恒 0）", models.Totals)
	}
	if models.Day != day {
		t.Fatalf("model day = %q（want %q）", models.Day, day)
	}

	// 差分：global 一行不变；session/key 零行（模型钩子只写 model scope）。
	var global adminUsageResponse
	adminAPIGetJSON(t, srv.URL+"/admin/usage", key, &global)
	if len(global.Rows) != 1 || global.Rows[0].Scope != storeUsageScopeGlobal ||
		global.Rows[0].Requests != 7 || global.Rows[0].CostUnits != 7 {
		t.Fatalf("global 被模型计数污染: %+v", global)
	}
	for _, scope := range []string{"session", "key"} {
		var out adminUsageResponse
		adminAPIGetJSON(t, srv.URL+"/admin/usage?scope="+scope, key, &out)
		if len(out.Rows) != 0 || out.Totals.Requests != 0 {
			t.Fatalf("scope=%s 不该有行（无会话头）: %+v", scope, out)
		}
	}

	// 存储面直读：scope 行就是 `model:<名>`（与展示面同源，零二次编码）。
	rows, err := s.store.UsageScopes(day)
	if err != nil {
		t.Fatalf("usage scopes: %v", err)
	}
	got := map[string]int{}
	for _, row := range rows {
		got[row.Scope] = row.Requests
	}
	if got["model:model-a"] != 2 || got["model:model-b"] != 1 || got[storeUsageScopeGlobal] != 7 {
		t.Fatalf("store 行 = %+v", got)
	}
}

// ---------------------------------------------------------------------------
// 2. store 未启用：请求照常、模型 scope 503（不是 500/panic）
// ---------------------------------------------------------------------------

func TestAdminModelUsageStoreDisabled(t *testing.T) {
	const key = "w78-key"
	up := goodClaudeUpstream(t)
	cfg := uiTestConfig()
	cfg.HeartbeatSeconds = 15
	cfg.AdminKey = key
	cfg.Upstreams = []Upstream{anthUp("fake", up.srv.URL)} // 无 store
	_, srv := adminOpsStart(t, cfg)

	adminModelChat(t, srv.URL, "model-a") // store 未启用 = 钩子 no-op，请求照常
	resp := adminAPIReq(t, http.MethodGet, srv.URL+"/admin/usage?scope=model", key, "")
	if resp.status != http.StatusServiceUnavailable || !strings.Contains(resp.body, "store_unavailable") {
		t.Fatalf("store 未启用 usage = %d body=%s（want 503 store_unavailable）", resp.status, resp.body)
	}
}
