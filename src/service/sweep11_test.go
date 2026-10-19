// W5-28 回归测试（第十一遍清扫）：
//
//  1. 模型表配置化 + A6 别名映射（顶层 `models:`）：请求面 alias→上游 model 改写
//     （同协议直通面字节级局部替换——其余字节逐字节保留）+ 缺省/未命中透传 +
//     gemini 上游路径改写 + 跨协议 IR 模型填充。
//  2. `GET /v1/models/{id}`（SDK retrieve 面）：静态/配置表检索 + 404
//     `not_found_error` + `?beta=true` 容忍 + 非 GET 405。
//  3. `anthropic-workspace-id` 转发（B3）：客户端有则转发 / 配置注入覆盖 /
//     只对 anthropic 上游面（非 anthropic 上游不带该头）。
//  4. 心跳门 × `first_byte_timeout_seconds` 组合：预提交 guard 取消（心跳门拦住
//     预提交 ping——CT 不退化为 text/plain、真实状态码保留）+ 提交点后 guard
//     窗口关闭（心跳恢复、已提交流不被 guard 掐断）。
//  5. 配置化发现面：`GET /v1/models` 从配置读（上限字段可省略、静态表被替换）。
package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

// sweep11Captured 记录最近一次上游请求的路径/体/头（W5-28 测试断言面）。
type sweep11Captured struct {
	mu   sync.Mutex
	path string
	body string
	hdrs map[string]string
}

func (c *sweep11Captured) get(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if key == "path" {
		return c.path
	}
	if key == "body" {
		return c.body
	}
	return c.hdrs[key]
}

// newSweep11Upstream 计数 + 捕获（路径/体/常用头）假上游。
func newSweep11Upstream(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) (*countUpstream, *sweep11Captured) {
	t.Helper()
	c := &sweep11Captured{hdrs: map[string]string{}}
	up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		c.mu.Lock()
		c.path = r.URL.Path
		c.body = string(b)
		for _, k := range []string{"anthropic-workspace-id", "anthropic-version", "anthropic-beta"} {
			c.hdrs[k] = r.Header.Get(k)
		}
		c.mu.Unlock()
		handler(w, r)
	})
	return up, c
}

// sweep11Get 发 GET 请求（真 HTTP 客户端）。
func sweep11Get(t *testing.T, url string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}

// sweep11AliasModels 测试用模型表：别名 → 上游模型（+ 一条纯发现条目）。
func sweep11AliasModels() []ModelEntry {
	return []ModelEntry{
		{ID: "claude-alias-1", UpstreamModel: "claude-real-9", MaxInputTokens: 1000000, MaxTokens: 128000},
		{ID: "claude-haiku-5-5"}, // 纯发现条目：不改写（id 原样进上游）
	}
}

// sweep11ClaudeReq 带空白的 claude 请求体（`model` 值替换的字节级锚点：
// 冒号/逗号周围留空白，改写只许动模型字符串本身）。
func sweep11ClaudeReq(model string) string {
	return fmt.Sprintf(`{"model" : %q , "max_tokens":10, "messages":[{"role":"user","content":"hi"}],"stream":true}`, model)
}

// sweep11ErrEnvelope 解 claude 错误信封（`{"type":"error","error":{...}}`）。
type sweep11ErrEnvelope struct {
	Type  string `json:"type"`
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

// ---------------------------------------------------------------------------
// 条目 1：模型表配置化 + A6 别名映射（请求面改写）
// ---------------------------------------------------------------------------

// 同协议直通面：别名命中 → 上游收到改写后的模型名（只换字符串值，其余字节
// 逐字节保留）；缺省（无 models）与未命中 → 原样透传。反证：去掉 serve 的
// 改写块 → alias_rewritten 子例红（上游收到别名）。
func TestSweep11ModelAliasRewriteSameProto(t *testing.T) {
	cases := []struct {
		name   string
		models []ModelEntry
		model  string
		want   string
	}{
		{"alias_rewritten", sweep11AliasModels(), "claude-alias-1", sweep11ClaudeReq("claude-real-9")},
		{"discovery_only_passthrough", sweep11AliasModels(), "claude-haiku-5-5", sweep11ClaudeReq("claude-haiku-5-5")},
		{"default_passthrough", nil, "claude-alias-1", sweep11ClaudeReq("claude-alias-1")},
		{"unmapped_passthrough", sweep11AliasModels(), "claude-other", sweep11ClaudeReq("claude-other")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				claudeTextStream(w)
			})
			cfg := cfgFor(up.srv.URL, ProtocolAnthropic, 15)
			cfg.Models = tc.models
			srv := startService(t, cfg)

			resp := postJSON(t, srv.URL+"/v1/messages", sweep11ClaudeReq(tc.model), nil)
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != 200 {
				t.Fatalf("status = %d body=%q", resp.StatusCode, raw)
			}

			path, body, _ := up.request()
			if path != "/v1/messages" {
				t.Fatalf("upstream path = %q", path)
			}
			if string(body) != tc.want {
				t.Fatalf("上游请求体不一致（改写应只动 model 字符串，其余字节逐字节保留）:\n got %q\nwant %q", body, tc.want)
			}
		})
	}
}

// 改写助手单元面：字节级局部替换的边界（无 model / 非对象 / 非字符串值 → 不改写；
// 特殊字符正确 JSON 编码）。
func TestSweep11RewriteModelFieldUnit(t *testing.T) {
	if got, ok := rewriteModelField([]byte(`{"model":"a","x":1}`), "b"); !ok || string(got) != `{"model":"b","x":1}` {
		t.Fatalf("basic rewrite = %q ok=%v", got, ok)
	}
	// 空白/换行/制表符包围（JSON 允许的空白都要跳过）。
	in := "{\n\t\"model\"\t:\t\"a\" ,\n \"x\": 2\n}"
	want := "{\n\t\"model\"\t:\t\"b\" ,\n \"x\": 2\n}"
	if got, ok := rewriteModelField([]byte(in), "b"); !ok || string(got) != want {
		t.Fatalf("whitespace rewrite:\n got %q\nwant %q", got, want)
	}
	// 特殊字符：重新 JSON 编码（引号/反斜杠）。
	if got, ok := rewriteModelField([]byte(`{"model":"a"}`), `b"c\d`); !ok || string(got) != `{"model":"b\"c\\d"}` {
		t.Fatalf("escaping rewrite = %q ok=%v", got, ok)
	}
	// 非字符串值 / 无 model / 非对象 → (nil, false)。
	for _, in := range []string{`{"model":7}`, `{"x":1}`, `[1,2]`, `"s"`, ``, `{"model":`} {
		if got, ok := rewriteModelField([]byte(in), "b"); ok {
			t.Fatalf("畸形输入被改写: %q → %q", in, got)
		}
	}
}

// gemini 上游路径面：别名 → 上游模型改写进 URL 路径（PathFor 用改写后的名字）；
// 跨协议（gemini 入口 → openai 上游）时 IR 模型填充也用上游名（opts.Model）。
func TestSweep11ModelAliasGeminiPath(t *testing.T) {
	gemResp := `{"candidates":[{"content":{"role":"model","parts":[{"text":"hi"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}`

	t.Run("gemini_upstream_path", func(t *testing.T) {
		up, cap := newSweep11Upstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			_, _ = w.Write([]byte(gemResp))
		})
		cfg := cfgFor(up.srv.URL, ProtocolGemini, 15)
		cfg.Models = []ModelEntry{{ID: "claude-gemini-alias", UpstreamModel: "gemini-2.5-flash-001"}}
		srv := startService(t, cfg)

		resp := postJSON(t, srv.URL+"/v1beta/models/claude-gemini-alias:generateContent",
			`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`, nil)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d body=%q", resp.StatusCode, raw)
		}
		if got := cap.get("path"); got != "/v1beta/models/gemini-2.5-flash-001:generateContent" {
			t.Fatalf("上游路径 = %q（别名未改写为上游模型名）", got)
		}
	})

	t.Run("cross_proto_model_fill", func(t *testing.T) {
		chatResp := `{"id":"c1","object":"chat.completion","created":1,"model":"gemini-2.5-flash-001","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`
		up, cap := newSweep11Upstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			_, _ = w.Write([]byte(chatResp))
		})
		cfg := cfgFor(up.srv.URL, ProtocolOpenAIChat, 15)
		cfg.Models = []ModelEntry{{ID: "claude-gemini-alias", UpstreamModel: "gemini-2.5-flash-001"}}
		srv := startService(t, cfg)

		resp := postJSON(t, srv.URL+"/v1beta/models/claude-gemini-alias:generateContent",
			`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`, nil)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d body=%q", resp.StatusCode, raw)
		}
		body := cap.get("body")
		if !strings.Contains(body, `"model":"gemini-2.5-flash-001"`) {
			t.Fatalf("跨协议上游体未带改写后的模型名: %q", body)
		}
		if strings.Contains(body, "claude-gemini-alias") {
			t.Fatalf("跨协议上游体残留别名: %q", body)
		}
	})
}

// ---------------------------------------------------------------------------
// 条目 1/2：配置化发现面 + 检索面（同源）
// ---------------------------------------------------------------------------

// 配置了 `models:` → 发现面只列配置条目（静态四模型被替换）；上限字段可省略
// （缺省条目不含 max_* 键）；检索面同源（配置 id 可检索、静态 id 404）。
func TestSweep11ModelsConfigDiscovery(t *testing.T) {
	cfg := cfgFor("http://127.0.0.1:1", ProtocolAnthropic, 15)
	cfg.Models = sweep11AliasModels()
	srv := startService(t, cfg)

	resp := sweep11Get(t, srv.URL+"/v1/models?limit=1000", nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d body=%q", resp.StatusCode, raw)
	}
	var doc struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode: %v body=%q", err, raw)
	}
	if len(doc.Data) != 2 {
		t.Fatalf("data 条目数 = %d（配置表 2 条；静态表应被替换）body=%q", len(doc.Data), raw)
	}
	idOf := func(m map[string]json.RawMessage) string {
		var s string
		_ = json.Unmarshal(m["id"], &s)
		return s
	}
	if idOf(doc.Data[0]) != "claude-alias-1" || idOf(doc.Data[1]) != "claude-haiku-5-5" {
		t.Fatalf("id 序 = %q,%q", idOf(doc.Data[0]), idOf(doc.Data[1]))
	}
	if _, ok := doc.Data[0]["max_input_tokens"]; !ok {
		t.Fatalf("声明了上限的条目缺 max_input_tokens: %q", raw)
	}
	if _, ok := doc.Data[0]["max_tokens"]; !ok {
		t.Fatalf("声明了上限的条目缺 max_tokens: %q", raw)
	}
	if _, ok := doc.Data[1]["max_input_tokens"]; ok {
		t.Fatalf("未声明上限的条目下发了 max_input_tokens（应省略）: %q", raw)
	}
	if _, ok := doc.Data[1]["max_tokens"]; ok {
		t.Fatalf("未声明上限的条目下发了 max_tokens（应省略）: %q", raw)
	}

	// 检索面同源：配置 id 可检索。
	rr := sweep11Get(t, srv.URL+"/v1/models/claude-haiku-5-5?beta=true", nil)
	defer rr.Body.Close()
	rraw, _ := io.ReadAll(rr.Body)
	if rr.StatusCode != 200 {
		t.Fatalf("retrieve 配置 id: status = %d body=%q", rr.StatusCode, rraw)
	}
	var one map[string]json.RawMessage
	if err := json.Unmarshal(rraw, &one); err != nil {
		t.Fatalf("retrieve decode: %v body=%q", err, rraw)
	}
	if idOf(one) != "claude-haiku-5-5" {
		t.Fatalf("retrieve id = %q", idOf(one))
	}
	// 静态表被替换：缺省四模型里的 id 不再可检索（404）。
	sr := sweep11Get(t, srv.URL+"/v1/models/claude-opus-5-5", nil)
	defer sr.Body.Close()
	if sr.StatusCode != 404 {
		t.Fatalf("静态 id 在配置表下应 404: status = %d", sr.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 条目 2：GET /v1/models/{id}（SDK retrieve 面）
// ---------------------------------------------------------------------------

// 缺省（无 models 配置）：静态表检索 200（id + 上限字段，与发现面同形状）；
// `?beta=true` 与 betas 头容忍；未知 id / 空 id / 多段路径 → 404
// `not_found_error`；非 GET → 405。反证：去掉路由注册 → 本测试红（404 page not found）。
func TestSweep11ModelRetrieve(t *testing.T) {
	srv := startService(t, cfgFor("http://127.0.0.1:1", ProtocolAnthropic, 15))

	resp := sweep11Get(t, srv.URL+"/v1/models/claude-sonnet-5-5?beta=true",
		map[string]string{"anthropic-beta": "oauth-2025-04-20"})
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d body=%q", resp.StatusCode, raw)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("CT = %q", ct)
	}
	var m struct {
		ID             string `json:"id"`
		MaxInputTokens int    `json:"max_input_tokens"`
		MaxTokens      int    `json:"max_tokens"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode: %v body=%q", err, raw)
	}
	if m.ID != "claude-sonnet-5-5" || m.MaxInputTokens < 100000 || m.MaxTokens <= 0 {
		t.Fatalf("检索形状偏差: %+v body=%q", m, raw)
	}

	for _, path := range []string{"/v1/models/claude-nope-9", "/v1/models/", "/v1/models/a/b"} {
		r := sweep11Get(t, srv.URL+path, nil)
		rr, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if r.StatusCode != 404 {
			t.Fatalf("GET %s status = %d body=%q（want 404）", path, r.StatusCode, rr)
		}
		var env sweep11ErrEnvelope
		if err := json.Unmarshal(rr, &env); err != nil {
			t.Fatalf("GET %s 错误信封不是 JSON: %v body=%q", path, err, rr)
		}
		if env.Type != "error" || env.Error.Type != "not_found_error" {
			t.Fatalf("GET %s 错误信封形态偏差: %+v body=%q", path, env, rr)
		}
	}

	presp := postJSON(t, srv.URL+"/v1/models/claude-sonnet-5-5", `{}`, nil)
	defer presp.Body.Close()
	if presp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST 检索面 status = %d, want 405", presp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 条目 3：anthropic-workspace-id 转发（B3）
// ---------------------------------------------------------------------------

// 客户端有则转发；配置 workspace_id 注入/覆盖（配置优先）；都不给 → 不发明；
// 非 anthropic 上游面不带该头。反证：去掉 forward 的转发块 → 前两子例红。
func TestSweep11WorkspaceIDForwarding(t *testing.T) {
	streamHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		claudeTextStream(w)
	}
	reqBody := `{"model":"claude-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}],"stream":true}`

	t.Run("forward_when_client_sends", func(t *testing.T) {
		up, cap := newSweep11Upstream(t, streamHandler)
		srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))
		resp := postJSON(t, srv.URL+"/v1/messages", reqBody,
			map[string]string{"anthropic-workspace-id": "wrkspc-client"})
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if got := cap.get("anthropic-workspace-id"); got != "wrkspc-client" {
			t.Fatalf("上游 workspace-id = %q, want wrkspc-client（客户端有则转发）", got)
		}
	})

	t.Run("config_overrides_client", func(t *testing.T) {
		up, cap := newSweep11Upstream(t, streamHandler)
		cfg := cfgFor(up.srv.URL, ProtocolAnthropic, 15)
		cfg.Upstreams[0].WorkspaceID = "wrkspc-config"
		srv := startService(t, cfg)
		resp := postJSON(t, srv.URL+"/v1/messages", reqBody,
			map[string]string{"anthropic-workspace-id": "wrkspc-client"})
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		if got := cap.get("anthropic-workspace-id"); got != "wrkspc-config" {
			t.Fatalf("上游 workspace-id = %q, want wrkspc-config（配置覆盖客户端值）", got)
		}
	})

	t.Run("config_injects_without_client", func(t *testing.T) {
		up, cap := newSweep11Upstream(t, streamHandler)
		cfg := cfgFor(up.srv.URL, ProtocolAnthropic, 15)
		cfg.Upstreams[0].WorkspaceID = "wrkspc-config"
		srv := startService(t, cfg)
		resp := postJSON(t, srv.URL+"/v1/messages", reqBody, nil)
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		if got := cap.get("anthropic-workspace-id"); got != "wrkspc-config" {
			t.Fatalf("上游 workspace-id = %q, want wrkspc-config（配置注入）", got)
		}
	})

	t.Run("absent_both_no_header", func(t *testing.T) {
		up, cap := newSweep11Upstream(t, streamHandler)
		srv := startService(t, cfgFor(up.srv.URL, ProtocolAnthropic, 15))
		resp := postJSON(t, srv.URL+"/v1/messages", reqBody, nil)
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		if got := cap.get("anthropic-workspace-id"); got != "" {
			t.Fatalf("上游 workspace-id = %q（都不给时不发明）", got)
		}
	})

	t.Run("non_anthropic_upstream_untouched", func(t *testing.T) {
		chatResp := `{"id":"c1","object":"chat.completion","created":1,"model":"gpt-x","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`
		up, cap := newSweep11Upstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			_, _ = w.Write([]byte(chatResp))
		})
		srv := startService(t, cfgFor(up.srv.URL, ProtocolOpenAIChat, 15))
		resp := postJSON(t, srv.URL+"/v1/messages",
			`{"model":"claude-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`,
			map[string]string{"anthropic-workspace-id": "wrkspc-client"})
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if got := cap.get("anthropic-workspace-id"); got != "" {
			t.Fatalf("非 anthropic 上游收到 workspace-id = %q（只对 anthropic 上游面转发）", got)
		}
	})
}

// ---------------------------------------------------------------------------
// 条目 4：心跳门 × first_byte_timeout_seconds 组合
// ---------------------------------------------------------------------------

// 预提交 guard 取消：上游「响应头先到、首帧静默 > 心跳间隔」→ 心跳拍点被门拦住
// （不得写字节）；guard 超时取消本尝试 → 单上游 = 真实 502（CT=application/json、
// 体无 ping 字节、归因 first-byte timeout）；双上游 = 换上游成功（最终流无首台
// 的 ping 残留）。
func TestSweep11HeartbeatFirstByteCombo(t *testing.T) {
	reqBody := `{"model":"claude-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}],"stream":true}`

	t.Run("pre_commit_guard_single_502", func(t *testing.T) {
		up := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done() // 挂住不产出；guard 取消上游请求时解除
		})
		cfg := cfgFor(up.srv.URL, ProtocolAnthropic, 1)
		cfg.FirstByteTimeoutSeconds = 2
		srv := startService(t, cfg)

		resp := postJSON(t, srv.URL+"/v1/messages", reqBody, nil)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)

		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("status = %d body=%q（guard 取消 → 预提交失败应保留真实 502）", resp.StatusCode, raw)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
			t.Fatalf("CT = %q（预提交 ping 抢先 = 退化 text/plain）", ct)
		}
		if strings.Contains(string(raw), "event: ping") {
			t.Fatalf("错误体混入预提交 ping 字节: %q", raw)
		}
		var env sweep11ErrEnvelope
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatalf("错误信封不是 JSON: %v body=%q", err, raw)
		}
		if !strings.Contains(env.Error.Message, "first-byte timeout") {
			t.Fatalf("错误未归因首产出超时: %q", env.Error.Message)
		}
	})

	t.Run("pre_commit_guard_failover", func(t *testing.T) {
		bad := newCountUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(200)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
		})
		good := goodClaudeUpstream(t)
		cfg := failoverCfg(anthUp("bad", bad.srv.URL), anthUp("good", good.srv.URL))
		cfg.HeartbeatSeconds = 1
		cfg.FirstByteTimeoutSeconds = 2
		_, srv := startServiceWith(t, cfg)

		resp := postJSON(t, srv.URL+"/v1/messages", reqBody, nil)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)

		if resp.StatusCode != 200 {
			t.Fatalf("status = %d body=%q（guard 取消首台 → 换上游应成功）", resp.StatusCode, raw)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream; charset=utf-8" {
			t.Fatalf("CT = %q", ct)
		}
		if strings.Contains(string(raw), "event: ping") {
			t.Fatalf("最终流混入首台预提交 ping 字节: %q", raw)
		}
		if !strings.Contains(string(raw), "event: message_stop") {
			t.Fatalf("最终流缺终止帧: %q", raw)
		}
		if bad.count() != 1 || good.count() != 1 {
			t.Fatalf("hits: bad=%d good=%d（期望 1/1）", bad.count(), good.count())
		}
	})
}

// 提交点后 guard 窗口关闭：首帧在 guard 超时前产出（提交）→ 上游随后静默
// > guard 超时——已提交流不得被 guard 掐断（stream 正常收尾），且提交后心跳
// 恢复（静默期补 ping）。反证：不 stop guard（提交点不关窗）→ 流被取消 → 无
// message_stop（error 帧收尾）；去掉心跳 → 无 ping。
func TestSweep11GuardWindowClosedAtCommit(t *testing.T) {
	startPayload := `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-x","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}`
	up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(300 * time.Millisecond)
		sseFrame(w, "message_start", startPayload) // 提交点（< guard 2s）
		time.Sleep(2500 * time.Millisecond)        // > guard 超时：窗口未关则此间被取消
		sseFrame(w, "message_stop", `{"type":"message_stop"}`)
	})
	cfg := cfgFor(up.srv.URL, ProtocolAnthropic, 1)
	cfg.FirstByteTimeoutSeconds = 2
	srv := startService(t, cfg)

	resp := postJSON(t, srv.URL+"/v1/messages",
		`{"model":"claude-x","max_tokens":10,"messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d body=%q", resp.StatusCode, raw)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream; charset=utf-8" {
		t.Fatalf("CT = %q", ct)
	}
	if !strings.HasPrefix(string(raw), "event: message_start") {
		t.Fatalf("首字节不是 message_start（预提交字节混入）: %q", raw)
	}
	if !strings.Contains(string(raw), "event: ping") {
		t.Fatalf("提交后静默期缺心跳帧: %q", raw)
	}
	if !strings.Contains(string(raw), "event: message_stop") {
		t.Fatalf("已提交流被 guard 掐断（无 message_stop）: %q", raw)
	}
	if strings.Contains(string(raw), "incomplete_stream") || strings.Contains(string(raw), `"error"`) {
		t.Fatalf("流混入错误帧: %q", raw)
	}
}

// 配置校验：模型表 id 必填/唯一、上限非负（显式报错，绝不静默降级）。
func TestSweep11ModelConfigValidation(t *testing.T) {
	cases := []struct {
		name   string
		models []ModelEntry
		wantOK bool
	}{
		{"ok", sweep11AliasModels(), true},
		{"empty_id", []ModelEntry{{ID: "  "}}, false},
		{"duplicate_id", []ModelEntry{{ID: "a"}, {ID: "a"}}, false},
		{"negative_limit", []ModelEntry{{ID: "a", MaxTokens: -1}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := cfgFor("http://127.0.0.1:1", ProtocolAnthropic, 15)
			cfg.Models = tc.models
			err := cfg.Validate()
			if tc.wantOK && err != nil {
				t.Fatalf("Validate = %v, want nil", err)
			}
			if !tc.wantOK && err == nil {
				t.Fatalf("Validate = nil, want error")
			}
		})
	}
}

// count_tokens 请求面同款：别名命中 → 上游透传体改写（精确计数不因别名丢）；
// 缺省/未命中 → 原样透传。
func TestSweep11CountTokensAliasRewrite(t *testing.T) {
	cases := []struct {
		name   string
		models []ModelEntry
		model  string
		want   string
	}{
		{"alias_rewritten", sweep11AliasModels(), "claude-alias-1", `"model":"claude-real-9"`},
		{"default_passthrough", nil, "claude-alias-1", `"model":"claude-alias-1"`},
		{"unmapped_passthrough", sweep11AliasModels(), "claude-other", `"model":"claude-other"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := newClaudeUpstream(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				_, _ = w.Write([]byte(`{"input_tokens":7}`))
			})
			cfg := cfgFor(up.srv.URL, ProtocolAnthropic, 15)
			cfg.Models = tc.models
			srv := startService(t, cfg)

			resp := postJSON(t, srv.URL+"/v1/messages/count_tokens",
				fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}]}`, tc.model), nil)
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != 200 || !strings.Contains(string(raw), `"input_tokens":7`) {
				t.Fatalf("status = %d body=%q", resp.StatusCode, raw)
			}
			path, body, _ := up.request()
			if path != "/v1/messages/count_tokens" {
				t.Fatalf("upstream path = %q", path)
			}
			if !strings.Contains(string(body), tc.want) {
				t.Fatalf("上游 count_tokens 体 = %q, want 含 %s", body, tc.want)
			}
		})
	}
}

// YAML 装载面：`models:` 段解析（KnownFields 纪律——错字不静默吞）+ 别名解析
// （upstreamModelFor 三条：命中改写 / 纯发现条目不改写 / 未命中透传）。
func TestSweep11ModelsConfigYAML(t *testing.T) {
	dir := t.TempDir()
	writeYAML := func(name, content string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return p
	}

	p := writeYAML("models.yaml",
		"upstreams:\n  - base_url: http://127.0.0.1:1\n    protocol: anthropic\n"+
			"models:\n"+
			"  - id: claude-alias-1\n    upstream_model: claude-real-9\n    max_input_tokens: 1000000\n    max_tokens: 128000\n"+
			"  - id: claude-haiku-5-5\n")
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.Models) != 2 || cfg.Models[0].UpstreamModel != "claude-real-9" ||
		cfg.Models[0].MaxInputTokens != 1000000 || cfg.Models[0].MaxTokens != 128000 ||
		cfg.Models[1].UpstreamModel != "" {
		t.Fatalf("models = %+v", cfg.Models)
	}
	if got := cfg.upstreamModelFor("claude-alias-1"); got != "claude-real-9" {
		t.Fatalf("upstreamModelFor(alias) = %q, want claude-real-9", got)
	}
	if got := cfg.upstreamModelFor("claude-haiku-5-5"); got != "claude-haiku-5-5" {
		t.Fatalf("upstreamModelFor(discovery-only) = %q, want 原样", got)
	}
	if got := cfg.upstreamModelFor("claude-unknown"); got != "claude-unknown" {
		t.Fatalf("upstreamModelFor(unmapped) = %q, want 原样", got)
	}

	// 模型表条目的错字（未知字段）必须报错。
	bad := writeYAML("bad.yaml",
		"upstreams:\n  - base_url: http://127.0.0.1:1\n    protocol: anthropic\n"+
			"models:\n  - id: a\n    upstream: b\n")
	if _, err := LoadConfig(bad); err == nil {
		t.Fatalf("models 条目未知字段必须报错（KnownFields 纪律）")
	}
}
