// W5-24（清扫七条目 2）：P11 录帧矩阵并入回归 fixture。
//
// 语料 = tmp/lanes/P11-gemini-frames/（P11 lane 的 14 条下游录帧 + 7 份上游原始
// 字节 + manifest）→ testdata/p11/。期望值 = **W5-22 后的新行为**（FD1/FD2：
// s8a/s8b/s11a 三条与 P11 原录帧不同——见 manifest.note 与重放基线
// tmp/lanes/L19-sweep6/p11-replay/）：
//
//   - s8a/s8b：gemini 直通缺 finishReason 的 EOF = 截断流 → SSE 收错误帧
//     （470B）/ 数组不闭合（351B）；
//   - s11a：上游断连错误帧后**无** `data: [DONE]` 哨兵（FD2——gemini 出口绝不
//     发哨兵；原录帧 255B 带哨兵）。
//
// 本测试 = 真服务 + 真 HTTP 客户端 + 本地 mock 上游（按上游录帧逐字节回放，
// 语义对齐 P11 的 mock_upstream.py：按路径模型名供帧、gemini-array-forced 恒发
// 数组体（对抗性上游）、gemini-text-abort 写出帧后不终止 chunked 直接断连）：
//
//   - fixture 自检：manifest 的 bytes/sha256 与文件逐一核对（防误改）；
//   - 每条场景断言：下游状态码 == manifest.status；下游 Content-Type 形态；
//     下游字节 == 录帧**逐字节**（含上游请求必须带 alt=sse 的不变量）。
package service

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// p11ManifestEntry testdata/p11/manifest.json 的一行（P11 run_matrix.py save()
// 的输出形态——file 已改为相对 testdata/p11 的路径）。
type p11ManifestEntry struct {
	Name   string `json:"name"`
	File   string `json:"file"`
	Status int    `json:"status"`
	Bytes  int    `json:"bytes"`
	Sha256 string `json:"sha256"`
	Note   string `json:"note"`
}

// p11Scenario 一条重放场景：入口协议（注释面）+ 下游请求（路径决定入口与流
// 形态）+ 期望录帧名。body 与 P11 run_matrix.py 的请求体同形。
type p11Scenario struct {
	name  string
	entry Protocol
	path  string
	body  string
	ct    string // 期望下游 Content-Type 子串（SSE 形态 / JSON 形态）
}

func p11Read(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

// p11LoadManifest 读 manifest 并做 fixture 自检：bytes/sha256 与文件逐字节
// 核对——录帧被误改时先在这里失败（而不是散落在各场景断言里）。
func p11LoadManifest(t *testing.T, dir string) map[string]p11ManifestEntry {
	t.Helper()
	var entries []p11ManifestEntry
	if err := json.Unmarshal(p11Read(t, filepath.Join(dir, "manifest.json")), &entries); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	out := make(map[string]p11ManifestEntry, len(entries))
	for _, e := range entries {
		if _, dup := out[e.Name]; dup {
			t.Fatalf("manifest 重名: %s", e.Name)
		}
		data := p11Read(t, filepath.Join(dir, filepath.FromSlash(e.File)))
		if len(data) != e.Bytes {
			t.Errorf("fixture %s: %d 字节 != manifest %d（录帧被误改？）", e.Name, len(data), e.Bytes)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != e.Sha256 {
			t.Errorf("fixture %s: sha256 %s != manifest %s（录帧被误改？）", e.Name, got, e.Sha256)
		}
		out[e.Name] = e
	}
	return out
}

// p11LoadUpstream 读 testdata/p11/upstream/ 的上游录帧：模型名 → 原始字节
// （.sse.raw / .array.raw）。
func p11LoadUpstream(t *testing.T, dir string) (sse, array map[string][]byte) {
	t.Helper()
	upDir := filepath.Join(dir, "upstream")
	ents, err := os.ReadDir(upDir)
	if err != nil {
		t.Fatalf("read upstream dir: %v", err)
	}
	sse, array = map[string][]byte{}, map[string][]byte{}
	for _, de := range ents {
		name := de.Name()
		switch {
		case strings.HasSuffix(name, ".sse.raw"):
			sse[strings.TrimSuffix(name, ".sse.raw")] = p11Read(t, filepath.Join(upDir, name))
		case strings.HasSuffix(name, ".array.raw"):
			array[strings.TrimSuffix(name, ".array.raw")] = p11Read(t, filepath.Join(upDir, name))
		}
	}
	return sse, array
}

// p11PathModel 从 `/v1beta/models/{model}:{action}` 路径段取模型名（对齐
// mock_upstream.py 的解析）。
func p11PathModel(path string) string {
	i := strings.Index(path, "/models/")
	if i < 0 {
		return ""
	}
	seg := path[i+len("/models/"):]
	if j := strings.IndexByte(seg, ':'); j >= 0 {
		seg = seg[:j]
	}
	return seg
}

// p11Upstream P11 回放 mock（对齐 mock_upstream.py）：
//
//   - gemini-array-forced：无视 alt 恒发数组录帧（对抗性上游；Content-Type
//     application/json——服务必须 fail-closed 502）；
//   - 其余模型：**断言上游请求带 alt=sse**（P11 矩阵的请求形态不变量——数组是
//     下游形态，上游恒 SSE），供 SSE 录帧；
//   - gemini-text-abort：写出录帧后不发 chunked 终止符直接断连（上游读错误面；
//     先排空请求体，避免 close 时 RST 吞掉已发帧）。
func p11Upstream(t *testing.T, sse, array map[string][]byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		model := p11PathModel(r.URL.Path)
		if model == "gemini-array-forced" {
			raw, ok := array[model]
			if !ok {
				t.Errorf("mock: 无 %s 数组录帧", model)
				http.Error(w, "no recording", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(raw)
			return
		}
		if alt := r.URL.Query().Get("alt"); alt != "sse" {
			t.Errorf("mock: %s 上游请求缺 alt=sse（query=%q）——P11 矩阵上游请求形态不变量", model, r.URL.RawQuery)
		}
		raw, ok := sse[model]
		if !ok {
			t.Errorf("mock: 无 %s SSE 录帧", model)
			http.Error(w, "no recording", http.StatusInternalServerError)
			return
		}
		if model == "gemini-text-abort" {
			_, _ = io.Copy(io.Discard, r.Body)
			conn, buf, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("mock: hijack: %v", err)
				return
			}
			_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n")
			_, _ = fmt.Fprintf(buf, "%x\r\n%s\r\n", len(raw), raw)
			_ = buf.Flush()
			_ = conn.Close() // 不发 `0\r\n\r\n`：上游读错误面（mock_upstream.py close_connection 同款）
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(raw)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestP11FixtureReplay P11 14 条录帧矩阵的回归重放（对 W5-22 后的新行为锁）。
func TestP11FixtureReplay(t *testing.T) {
	dir := filepath.Join("testdata", "p11")
	manifest := p11LoadManifest(t, dir)
	sseRaw, arrayRaw := p11LoadUpstream(t, dir)
	up := p11Upstream(t, sseRaw, arrayRaw)
	srv := startService(t, cfgFor(up.URL, ProtocolGemini, 15))

	gemBody := `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`
	oaiBody := func(model string) string {
		return fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}`, model)
	}
	const sseCT = "text/event-stream"
	const jsonCT = "application/json"
	scenarios := []p11Scenario{
		{name: "s1_gemini_sse_text-usage.sse", entry: ProtocolGemini, path: "/v1beta/models/gemini-text-usage:streamGenerateContent?alt=sse", body: gemBody, ct: sseCT},
		{name: "s2_gemini_array_text-usage.json", entry: ProtocolGemini, path: "/v1beta/models/gemini-text-usage:streamGenerateContent", body: gemBody, ct: jsonCT},
		{name: "s3_gemini_array_fc-partial.json", entry: ProtocolGemini, path: "/v1beta/models/gemini-fc-partial:streamGenerateContent", body: gemBody, ct: jsonCT},
		{name: "s4_openai_fc-partial.sse", entry: ProtocolOpenAIChat, path: "/v1/chat/completions", body: oaiBody("gemini-fc-partial"), ct: sseCT},
		{name: "s5_openai_fc-partial-cut.txt", entry: ProtocolOpenAIChat, path: "/v1/chat/completions", body: oaiBody("gemini-fc-partial-cut"), ct: sseCT},
		{name: "s6_openai_text-usage.sse", entry: ProtocolOpenAIChat, path: "/v1/chat/completions", body: oaiBody("gemini-text-usage"), ct: sseCT},
		{name: "s7a_gemini_sse_array-forced.txt", entry: ProtocolGemini, path: "/v1beta/models/gemini-array-forced:streamGenerateContent?alt=sse", body: gemBody, ct: jsonCT},
		{name: "s7b_openai_array-forced.txt", entry: ProtocolOpenAIChat, path: "/v1/chat/completions", body: oaiBody("gemini-array-forced"), ct: jsonCT},
		{name: "s8a_gemini_sse_text-cut-partial.txt", entry: ProtocolGemini, path: "/v1beta/models/gemini-text-cut-partial:streamGenerateContent?alt=sse", body: gemBody, ct: sseCT},
		{name: "s8b_gemini_array_text-cut-partial.json", entry: ProtocolGemini, path: "/v1beta/models/gemini-text-cut-partial:streamGenerateContent", body: gemBody, ct: jsonCT},
		{name: "s9_gemini_sse_fc-parallel.sse", entry: ProtocolGemini, path: "/v1beta/models/gemini-fc-parallel:streamGenerateContent?alt=sse", body: gemBody, ct: sseCT},
		{name: "s10_openai_fc-parallel.sse", entry: ProtocolOpenAIChat, path: "/v1/chat/completions", body: oaiBody("gemini-fc-parallel"), ct: sseCT},
		{name: "s11a_gemini_sse_text-abort.txt", entry: ProtocolGemini, path: "/v1beta/models/gemini-text-abort:streamGenerateContent?alt=sse", body: gemBody, ct: sseCT},
		{name: "s11b_gemini_array_text-abort.json", entry: ProtocolGemini, path: "/v1beta/models/gemini-text-abort:streamGenerateContent", body: gemBody, ct: jsonCT},
	}

	if len(scenarios) != len(manifest) {
		t.Fatalf("场景数 %d != manifest 条数 %d（矩阵与语料必须一一对应）", len(scenarios), len(manifest))
	}
	seen := map[string]bool{}
	for _, sc := range scenarios {
		sc := sc
		want, ok := manifest[sc.name]
		if !ok {
			t.Fatalf("场景 %s 不在 manifest 里", sc.name)
		}
		if seen[sc.name] {
			t.Fatalf("场景重复: %s", sc.name)
		}
		seen[sc.name] = true
		t.Run(sc.name, func(t *testing.T) {
			expected := p11Read(t, filepath.Join(dir, filepath.FromSlash(want.File)))
			resp := postJSON(t, srv.URL+sc.path, sc.body, nil)
			got, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("read downstream: %v", err)
			}
			if resp.StatusCode != want.Status {
				t.Fatalf("status = %d, want %d（%s）\n%s", resp.StatusCode, want.Status, want.Note, got)
			}
			if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, sc.ct) {
				t.Errorf("Content-Type = %q, want 含 %q", ct, sc.ct)
			}
			if !bytes.Equal(got, expected) {
				t.Fatalf("下游字节与录帧不一致（%s）:\ngot  (%d): %q\nwant (%d): %q", want.Note, len(got), got, len(expected), expected)
			}
		})
	}
}
