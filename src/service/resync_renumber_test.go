package service

// W41 条目 3（A2A-L19-W41-1）：responses 直通出口的 RESYNC 重编号。
//
// 问题：直通路径丢帧（1 MiB 超限行的 RESYNC，`sseFrameReader.Next`）后，上游继续
// 按原号计数的 sequence_number 会留下洞（0..K 后跳到 K+2）——responses 客户端按
// 连续号校验/续读时会断。修后：**丢帧发生后**对后续帧重编号（单调连续，接客户端
// 已见的下一号），未丢帧路径零改动（字节保真）。GoModel `Renumberer` 纪律：
// 「丢/改/注入事件的出口必须重编号」（kernel-comparison §7.6.2 落地建议 3）。
//
// R01 修订（W70-3/W70-4）：本文件原记「丢帧是恢复行为、不是错误」——该口径已被
// W70-3 取代：**承载内容的**超限帧被丢弃 = 该尝试失败（relay 记 upstream_error，
// 已提交则写 incomplete_stream/upstream_frame_too_large 错误尾，见
// relay.go 的 `reader.LostContent()` 分支）。W70-3 二次修再进一步：丢内容帧后
// 到达的**终止证据帧**（本测试的 `response.completed`）不再转发——唯读终态事件的
// 消费端会提前收流、看不到错误尾（假成功）。故本测试的丢帧场景下游 = 3 个内容帧
// （重编号 0,1,2；上游原号 0,1,3）+ error seq=3 + response.failed seq=4，
// 全流无洞；未丢帧场景仍逐字节保真（含 response.completed，抑制只对丢帧路径生效）。
//
// 反证：
//   - 回退 relay.go 的 `reader.Resyncs() > 0` 重编号分支 → TestResponsesResyncRenumberE2E
//     红（帧序出现洞）；
//   - 回退 relay.go 的 `reader.LostContent()` 分支 → 本测试红（错误尾缺失，丢内容帧
//     被当成功）；
//   - 回退 renumberSequenceNumber 的数字替换 → TestRenumberSequenceNumber 红。

import (
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestRenumberSequenceNumber(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		seq   int64
		want  string
		match bool
	}{
		{"基本替换", `{"type":"x","sequence_number":7}`, 3, `{"type":"x","sequence_number":3}`, true},
		{"空白保留", `{"sequence_number": 42 ,"t":1}`, 5, `{"sequence_number": 5 ,"t":1}`, true},
		{"零替换", `{"sequence_number":0}`, 9, `{"sequence_number":9}`, true},
		{"多位数", `{"sequence_number":123456}`, 7, `{"sequence_number":7}`, true},
		{"无字段", `{"type":"x"}`, 3, `{"type":"x"}`, false},
		{"无数字", `{"sequence_number":}`, 3, `{"sequence_number":}`, false},
		{"非数字值", `{"sequence_number":"3"}`, 3, `{"sequence_number":"3"}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, ok := renumberSequenceNumber([]byte(tc.in), tc.seq)
			if ok != tc.match {
				t.Fatalf("match = %v, want %v", ok, tc.match)
			}
			if !tc.match {
				if out != nil {
					t.Fatalf("未命中应返回 nil: %q", out)
				}
				return
			}
			if string(out) != tc.want {
				t.Fatalf("out = %q, want %q", out, tc.want)
			}
		})
	}
}

// TestResponsesResyncRenumberE2E：responses 入口 × responses 上游直通。
//   - 丢帧场景：上游流中夹一个超限帧（原号 2）——下游帧序必须连续（0..N 无洞），
//     正常帧内容逐字节到达（畸形帧不漏）；
//   - 未丢帧场景：同一 fixture 无超限帧 → 下游字节与上游逐字节相同（零改动）。
func TestResponsesResyncRenumberE2E(t *testing.T) {
	frame := func(ev, data string) string { return "event: " + ev + "\ndata: " + data + "\n\n" }
	created := frame("response.created", `{"type":"response.created","response":{"id":"resp_rn","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":0}`)
	inProgress := frame("response.in_progress", `{"type":"response.in_progress","response":{"id":"resp_rn","object":"response","created_at":1741293000,"status":"in_progress","model":"gpt-5","output":[],"usage":null},"sequence_number":1}`)
	// 被 RESYNC 丢弃的帧（原号 2；单行超 1 MiB）。
	huge := "data: " + `{"type":"response.output_text.delta","item_id":"msg_rn","output_index":0,"content_index":0,"delta":"` + strings.Repeat("q", maxSSELineBytes+1024) + `","sequence_number":2}` + "\n\n"
	delta := frame("response.output_text.delta", `{"type":"response.output_text.delta","item_id":"msg_rn","output_index":0,"content_index":0,"delta":"Hi","sequence_number":3}`)
	completed := frame("response.completed", `{"type":"response.completed","response":{"id":"resp_rn","object":"response","created_at":1741293000,"status":"completed","model":"gpt-5","output":[],"usage":null,"incomplete_details":null},"sequence_number":4}`)
	noDrop := created + inProgress + delta + completed

	t.Run("未丢帧_字节逐字节不变", func(t *testing.T) {
		up := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, noDrop)
		})
		srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))
		resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","stream":true}`, nil)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if string(raw) != noDrop {
			t.Fatalf("未丢帧必须字节保真:\nwant:\n%s\ngot:\n%s", noDrop, raw)
		}
	})

	t.Run("丢帧后_帧序连续且正常帧不丢", func(t *testing.T) {
		up := newResponsesUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, created)
			_, _ = io.WriteString(w, inProgress)
			_, _ = io.WriteString(w, huge) // RESYNC 丢弃点（原号 2 的帧）
			_, _ = io.WriteString(w, delta)
			_, _ = io.WriteString(w, completed)
		})
		srv := startService(t, cfgFor(up.srv.URL, ProtocolResponses, 15))
		resp := postJSON(t, srv.URL+"/v1/responses", `{"model":"gpt-5","input":"hi","stream":true}`, nil)
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		raw, _ := io.ReadAll(resp.Body)
		body := string(raw)

		// 正常帧内容在位（丢帧未波及后续帧）；畸形帧不漏。
		for _, want := range []string{`"response.created"`, `"response.in_progress"`, `"delta":"Hi"`} {
			if !strings.Contains(body, want) {
				t.Fatalf("正常帧缺失 %s:\n%s", want, body)
			}
		}
		// R01 二次修：`response.completed` 是终止证据帧——丢内容帧后**不转发**
		// （唯读终态事件的消费端会提前收流、看不到 response.failed 错误尾）。
		if strings.Contains(body, `"response.completed"`) {
			t.Fatalf("丢内容帧后终止事件不得转发:\n%s", body)
		}
		if strings.Contains(body, `"qqq`) {
			t.Fatal("畸形帧不得到达下游")
		}

		// 帧切分（SSE 空行分隔）：3 个上游内容帧（含重编号）+ R01 错误尾两帧。
		var frames []string
		for _, chunk := range strings.Split(strings.TrimRight(body, "\n"), "\n\n") {
			if strings.TrimSpace(chunk) != "" {
				frames = append(frames, chunk)
			}
		}
		if len(frames) != 5 {
			t.Fatalf("帧数 = %d（want 5 = 3 正常帧 + error/response.failed 错误尾）:\n%s", len(frames), body)
		}

		// 帧序连续（W41）：全流 sequence_number 必须 0..4 无洞——上游原号 0,1,3
		// （丢帧后由重编号压回 0,1,2），错误尾接 3,4。未修时这里是 0,1,3（洞）。
		re := regexp.MustCompile(`"sequence_number":\s*([0-9]+)`)
		var got []int64
		for _, f := range frames {
			m := re.FindStringSubmatch(f)
			if m == nil {
				t.Fatalf("帧缺 sequence_number: %q", f)
			}
			n, err := strconv.ParseInt(m[1], 10, 64)
			if err != nil {
				t.Fatalf("sequence_number 解析: %v", err)
			}
			got = append(got, n)
		}
		for i, n := range got {
			if n != int64(i) {
				t.Fatalf("帧序不连续（want 0..N 单调连续）: %v\n%s", got, body)
			}
		}

		// R01（W70-3/W70-4）：被 RESYNC 丢弃的是**内容帧**（`data:` 承载 delta）——
		// 流不得以 response.completed 静默收尾：错误尾必须出现，且续号接在正常帧后。
		// 修前这里只有 4 帧、无错误尾（丢内容帧被当成功）。
		errFrame, failedFrame := frames[3], frames[4]
		if !strings.HasPrefix(errFrame, "event: error\ndata: ") ||
			!strings.Contains(errFrame, `"code":"upstream_frame_too_large"`) ||
			!strings.Contains(errFrame, `"sequence_number":3`) {
			t.Fatalf("错误尾缺 event: error/upstream_frame_too_large/seq=3:\n%s", errFrame)
		}
		if !strings.HasPrefix(failedFrame, "event: response.failed\ndata: ") ||
			!strings.Contains(failedFrame, `"status":"failed"`) ||
			!strings.Contains(failedFrame, `"code":"upstream_frame_too_large"`) ||
			!strings.Contains(failedFrame, `"sequence_number":4`) {
			t.Fatalf("错误尾缺 response.failed/failed 状态/seq=4:\n%s", failedFrame)
		}
	})
}
