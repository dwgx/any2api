package claude_web

// W10：claude_web 转换器逐帧测试——**实抓 fixture 驱动**（W9-2 的 4 条完整
// completion SSE 流，脱敏副本在 testdata/）。
//
// 装置：fixture 是 `{"event":…,"data":…}` 的 JSONL（W9-2 捕获器的帧记录形态），
// 本测试把它还原成原始 SSE 文本（`event: …\ndata: …\n\n`），两条路径都跑：
//  ① 逐帧 Feed（增量管道用的形态：pairs.StreamPipe 的 feed）；
//  ② ParseStream（批量形态：pairs.Convert 的 FaceStream / ParseResponse）。
// 两者必须产出同一事件序列。
//
// 逐帧断言 = 每条 fixture 的**完整** IR 事件列表（不是子集/计数）——padding
// （ping 帧的尾部空格）、`conversation_ready`、`message_limit` 都在断言面内。
//
// padding 面的处理方式：帧内不定长空格由 JSON 解析器的空白容忍覆盖（`{"type":"ping"  }`
// 是合法 JSON），帧外空白由 Feed 的 TrimSpace 覆盖；本包不做任何对空白敏感的字面
// 匹配——fixture 逐帧断言即证据（17/16/14/15 帧全解析）。
//
// 反证锚点（已实测回退即红，证据见报告 §反证）：
//   - 删掉 delta 分派的 `deltaThinkingSummary` 分支 → TestThinkingGoldenFixtures 红
//     （思考摘要整段丢失）；
//   - 删掉 event/data.type 双源核对 → TestEventNameMismatch 红；
//   - 回退 R5 硬限口径（改回枚举 hit_limit/over_limit）→
//     TestMessageLimitHardLimitRealCorpus 红（exceeded_limit 漏判）；
//   - 回退 R1 细节面透传（message_delta 不填 StopSequence/StopDetails）→
//     TestStopSequencePassthrough 红（pairs 侧 TestClaudeWebQuotaTruncationExits 同红）；
//   - 回退 thinking 观测面两处（ThinkingTruncated/SummaryPending 不落）→
//     TestThinkingObservationFlags 红。

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// fixture 一条实抓流。
type fixture struct {
	name string // 文件名（含 run 号）
	// 帧级期望：每条 fixture 的完整 IR 事件序列。
	msgID string
	model string
	text  string
	// utilization 该流的 5h 窗口利用率（实抓值：run1/run2 = 0.02，run3 = 0.03
	// ——同一账号晚一点的会话，W9-2 §2.3「utilization:0.02…0.03」）。
	utilization float64
	wantKinds   []ir.StreamEventKind
	// 帧计数（含被忽略的 ping/conversation_ready）。
	frames int
	pings  int
	ready  bool
}

var fixtures = []fixture{
	{
		name:        "run1_4087359.531.frames.jsonl.txt",
		msgID:       "chatcompl_011Cg1nKSAB9kpUcRACZhqnX",
		model:       "claude-sonnet-5-5",
		text:        "PONG",
		utilization: 0.02,
		frames:      17,
		pings:       9,
		ready:       true,
		wantKinds: []ir.StreamEventKind{
			ir.EventStart, ir.EventBlockStart, ir.EventBlockDelta, ir.EventBlockStop,
			ir.EventMessageDelta, // message_delta（stop_reason）
			ir.EventMessageDelta, // message_limit（配额 usage）
			ir.EventMessageStop,
		},
	},
	{
		name:        "run2_4117663.534.frames.jsonl.txt",
		msgID:       "chatcompl_011Cg1o3SCptxWVpFPfCG49v",
		model:       "claude-sonnet-5-5",
		text:        "PONG",
		utilization: 0.02,
		frames:      16,
		pings:       8,
		ready:       true,
		wantKinds: []ir.StreamEventKind{
			ir.EventStart, ir.EventBlockStart, ir.EventBlockDelta, ir.EventBlockStop,
			ir.EventMessageDelta, ir.EventMessageDelta, ir.EventMessageStop,
		},
	},
	{
		name:        "run2_4117663.567.frames.jsonl.txt",
		msgID:       "chatcompl_011Cg1o4DPuz54dq6Y6j1AcQ",
		model:       "claude-sonnet-5-5",
		text:        "PONG2",
		utilization: 0.02,
		frames:      14,
		pings:       7,
		ready:       false, // 第二回合：无 conversation_ready（W9-2 §2.3）
		wantKinds: []ir.StreamEventKind{
			ir.EventStart, ir.EventBlockStart, ir.EventBlockDelta, ir.EventBlockStop,
			ir.EventMessageDelta, ir.EventMessageDelta, ir.EventMessageStop,
		},
	},
	{
		name:        "run3_4144380.538.frames.jsonl.txt",
		msgID:       "chatcompl_011Cg1oVy6AtcYz6V5qPX3GQ",
		model:       "claude-sonnet-5-5",
		text:        "15:50", // effort=high 推理题（W9-2 §2.4）
		utilization: 0.03,
		frames:      15,
		pings:       7,
		ready:       true,
		wantKinds: []ir.StreamEventKind{
			ir.EventStart, ir.EventBlockStart, ir.EventBlockDelta, ir.EventBlockStop,
			ir.EventMessageDelta, ir.EventMessageDelta, ir.EventMessageStop,
		},
	},
}

// loadFixture 读 fixture 的帧列表（保持顺序）。
func loadFixture(t *testing.T, name string) []struct {
	Event string `json:"event"`
	Data  string `json:"data"`
} {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var frames []struct {
		Event string `json:"event"`
		Data  string `json:"data"`
	}
	for i, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		var f struct {
			Event string `json:"event"`
			Data  string `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &f); err != nil {
			t.Fatalf("%s line %d: %v", name, i+1, err)
		}
		frames = append(frames, f)
	}
	return frames
}

// toSSE 帧列表 → 原始 SSE 文本（捕获器落盘的 src.raw.txt 形态）。
func toSSE(frames []struct {
	Event string `json:"event"`
	Data  string `json:"data"`
}) string {
	var b strings.Builder
	for _, f := range frames {
		b.WriteString("event: " + f.Event + "\n")
		b.WriteString("data: " + f.Data + "\n\n")
	}
	return b.String()
}

// TestFixturesFrameByFrame 4 条实抓流逐帧断言（Feed 增量面）。
func TestFixturesFrameByFrame(t *testing.T) {
	for _, fx := range fixtures {
		t.Run(fx.name, func(t *testing.T) {
			frames := loadFixture(t, fx.name)
			if len(frames) != fx.frames {
				t.Fatalf("frames = %d, want %d", len(frames), fx.frames)
			}
			p := NewStreamParser()
			var events []ir.StreamEvent
			pings, ready := 0, false
			for i, f := range frames {
				// padding quirk 在场断言（帧内 JSON 尾部不定长空格）。
				switch f.Event {
				case "ping":
					pings++
				case "conversation_ready":
					ready = true
				}
				evs, err := p.Feed(f.Event, []byte(f.Data))
				if err != nil {
					t.Fatalf("frame %d (%s): %v", i+1, f.Event, err)
				}
				events = append(events, evs...)
			}
			if pings != fx.pings {
				t.Fatalf("ping frames = %d, want %d", pings, fx.pings)
			}
			if ready != fx.ready {
				t.Fatalf("conversation_ready = %v, want %v", ready, fx.ready)
			}
			if tail := p.Finish(); len(tail) != 0 {
				t.Fatalf("Finish after message_stop = %+v, want empty", tail)
			}
			assertEvents(t, fx, events)
		})
	}
}

// TestFixturesParseStreamBatch 同 4 条流的批量面（ParseStream + ParseResponse）
// 与增量面事件序列一致。
func TestFixturesParseStreamBatch(t *testing.T) {
	for _, fx := range fixtures {
		t.Run(fx.name, func(t *testing.T) {
			frames := loadFixture(t, fx.name)
			sse := toSSE(frames)
			// padding quirk 在场：至少一个 ping 帧带帧内尾部空格。
			if !strings.Contains(sse, `"ping" `) {
				t.Fatalf("fixture 无 padding quirk，样本不可信")
			}
			events, err := ParseStream(strings.NewReader(sse))
			if err != nil {
				t.Fatalf("ParseStream: %v", err)
			}
			assertEvents(t, fx, events)

			// ParseResponse：同一 SSE → 非流式 IR 响应（上游只有流式面，非流式
			// 下游走装配）。
			resp, err := ParseResponse([]byte(sse))
			if err != nil {
				t.Fatalf("ParseResponse: %v", err)
			}
			if resp.ID != fx.msgID || resp.Model != fx.model {
				t.Fatalf("resp id/model = %q/%q, want %q/%q", resp.ID, resp.Model, fx.msgID, fx.model)
			}
			if resp.StopReason != ir.StopStop {
				t.Fatalf("stop reason = %q, want stop", resp.StopReason)
			}
			if len(resp.Message.Blocks) != 1 || resp.Message.Blocks[0].Kind != ir.BlockText ||
				resp.Message.Blocks[0].Text != fx.text {
				t.Fatalf("resp blocks = %+v, want one text block %q", resp.Message.Blocks, fx.text)
			}
			if resp.Usage == nil || resp.Usage.Accounting != "claude_web" {
				t.Fatalf("resp usage = %+v, want claude_web quota usage", resp.Usage)
			}
		})
	}
}

// TestAssembleResponseStopSequence W52-1：流式 message_delta 的 stop_sequence
// （claude.ai 配额截断 `usage_limit`）经非流式装配回填 IR Response——`stream:false`
// 下游（claude_web.ParseResponse → claude 出口 RenderResponse）保真。
func TestAssembleResponseStopSequence(t *testing.T) {
	sse := strings.Join([]string{
		`event: message_start`,
		`data: {"type":"message_start","message":{"id":"c1","model":"m","usage":{"input_tokens":1}}}`,
		``,
		`event: content_block_start`,
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		``,
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}`,
		``,
		`event: content_block_stop`,
		`data: {"type":"content_block_stop","index":0}`,
		``,
		`event: message_delta`,
		`data: {"type":"message_delta","delta":{"stop_reason":"stop_sequence","stop_sequence":"usage_limit","stop_details":null}}`,
		``,
		`event: message_stop`,
		`data: {"type":"message_stop"}`,
		``,
	}, "\n")
	resp, err := ParseResponse([]byte(sse))
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if resp.StopSequence != "usage_limit" {
		t.Fatalf("StopSequence = %q, want usage_limit", resp.StopSequence)
	}
	if resp.StopReason != ir.StopStop {
		t.Fatalf("StopReason = %q, want stop", resp.StopReason)
	}
	// 常态（stop_sequence:null）→ 空（未提供）。
	plain := strings.ReplaceAll(sse, `"stop_sequence":"usage_limit"`, `"stop_sequence":null`)
	resp2, err := ParseResponse([]byte(plain))
	if err != nil {
		t.Fatalf("ParseResponse(plain): %v", err)
	}
	if resp2.StopSequence != "" {
		t.Fatalf("null stop_sequence 应为空：%q", resp2.StopSequence)
	}
}

// TestFeedHugeBlockIndexDoesNotDriveAllocation W52-1（索引不驱动分配条款，见
// ir/assemble.go 头注）：provider 给的巨 index（2^20）只作 open map 键——解析
// 分配随帧数线性、不随索引值；巨索引下 start/delta/stop 仍正常配对。
//
// 反证锚点：把 open 改成按索引扩切片（任何 `make([]…, index…)` 形态）→ 本测的
// 分配上界断言必红（2^20 项切片 ≥ 8 MiB > 1 MiB 上界）。
func TestFeedHugeBlockIndexDoesNotDriveAllocation(t *testing.T) {
	const huge = 1048576 // = 1<<20（与下方 JSON 字面量一致）
	p := NewStreamParser()
	if _, err := p.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m","model":"x"}}`)); err != nil {
		t.Fatalf("Feed(message_start): %v", err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	evs, err := p.Feed("content_block_start", []byte(`{"type":"content_block_start","index":1048576,"content_block":{"type":"text","text":""}}`))
	if err != nil {
		t.Fatalf("Feed(content_block_start): %v", err)
	}
	if len(evs) != 1 || evs[0].Kind != ir.EventBlockStart || evs[0].BlockIndex != huge || evs[0].BlockKind != ir.BlockText {
		t.Fatalf("block_start events = %+v", evs)
	}
	evs, err = p.Feed("content_block_delta", []byte(`{"type":"content_block_delta","index":1048576,"delta":{"type":"text_delta","text":"hi"}}`))
	if err != nil {
		t.Fatalf("Feed(content_block_delta): %v", err)
	}
	if len(evs) != 1 || evs[0].Kind != ir.EventBlockDelta || evs[0].BlockIndex != huge || evs[0].TextDelta != "hi" {
		t.Fatalf("block_delta events = %+v", evs)
	}
	evs, err = p.Feed("content_block_stop", []byte(`{"type":"content_block_stop","index":1048576}`))
	if err != nil {
		t.Fatalf("Feed(content_block_stop): %v", err)
	}
	if len(evs) != 1 || evs[0].Kind != ir.EventBlockStop || evs[0].BlockIndex != huge {
		t.Fatalf("block_stop events = %+v", evs)
	}
	runtime.ReadMemStats(&after)
	if delta := after.TotalAlloc - before.TotalAlloc; delta > 1<<20 {
		t.Fatalf("index 驱动了分配：%d bytes（上界 1 MiB）", delta)
	}
}

// assertEvents 逐事件断言（完整序列：kind + 块定位 + 载荷）。
func assertEvents(t *testing.T, fx fixture, events []ir.StreamEvent) {
	t.Helper()
	if len(events) != len(fx.wantKinds) {
		t.Fatalf("events = %d, want %d: %+v", len(events), len(fx.wantKinds), events)
	}
	for i, want := range fx.wantKinds {
		if events[i].Kind != want {
			t.Fatalf("event[%d].Kind = %q, want %q", i, events[i].Kind, want)
		}
	}
	// 逐字段断言（位置固定：0=start, 1=block_start, 2=block_delta, 3=block_stop,
	// 4=message_delta(stop_reason), 5=message_delta(usage), 6=message_stop）。
	start, bs, bd, be, md, lim, stop :=
		events[0], events[1], events[2], events[3], events[4], events[5], events[6]
	if start.MessageID != fx.msgID || start.Model != fx.model {
		t.Fatalf("start = %+v, want id=%s model=%s", start, fx.msgID, fx.model)
	}
	if bs.BlockIndex != 0 || bs.BlockKind != ir.BlockText {
		t.Fatalf("block_start = %+v, want index 0 text", bs)
	}
	if bd.BlockIndex != 0 || bd.BlockKind != ir.BlockText || bd.TextDelta != fx.text {
		t.Fatalf("block_delta = %+v, want index 0 text %q", bd, fx.text)
	}
	if be.BlockIndex != 0 || be.BlockKind != ir.BlockText {
		t.Fatalf("block_stop = %+v, want index 0 text", be)
	}
	if md.StopReason != ir.StopStop {
		t.Fatalf("message_delta.stop_reason = %q, want stop (end_turn)", md.StopReason)
	}
	// message_limit → usage（配额面）。
	if lim.Usage == nil {
		t.Fatalf("message_limit 事件无 usage: %+v", lim)
	}
	if lim.Usage.Accounting != "claude_web" {
		t.Fatalf("usage.Accounting = %q, want claude_web", lim.Usage.Accounting)
	}
	if lim.Usage.Input != 0 || lim.Usage.Output != 0 || lim.Usage.Total != 0 {
		t.Fatalf("配额面不得合成 token 数: %+v", lim.Usage)
	}
	raw, ok := lim.Usage.Extra["claude_web_message_limit"]
	if !ok {
		t.Fatalf("usage.Extra 缺原文旁路: %+v", lim.Usage.Extra)
	}
	var ml MessageLimit
	if err := json.Unmarshal(raw, &ml); err != nil {
		t.Fatalf("message_limit 原文: %v", err)
	}
	if ml.Type != limitWithin || ml.RepresentativeClaim != "five_hour" {
		t.Fatalf("message_limit = %+v", ml)
	}
	if w, ok := ml.Windows["5h"]; !ok || w.Status != "within_limit" || w.Utilization != fx.utilization || w.ResetsAt != 1791982800 {
		t.Fatalf("5h 窗口 = %+v", ml.Windows)
	}
	if w, ok := ml.Windows["7d"]; !ok || w.ResetsAt != 1792519200 {
		t.Fatalf("7d 窗口 = %+v", ml.Windows)
	}
	// 派生摘要（最差窗口 / 最早 reset / 硬限）。
	qraw, ok := lim.Usage.Extra["claude_web_quota"]
	if !ok {
		t.Fatalf("usage.Extra 缺派生摘要: %+v", lim.Usage.Extra)
	}
	var q QuotaSummary
	if err := json.Unmarshal(qraw, &q); err != nil {
		t.Fatalf("quota summary: %v", err)
	}
	if q.HardLimit || q.WorstWindow != "5h" || q.WorstUtilization != fx.utilization || q.EarliestResetsAt != 1791982800 {
		t.Fatalf("quota summary = %+v", q)
	}
	if stop.Kind != ir.EventMessageStop {
		t.Fatalf("last event = %+v, want message_stop", stop)
	}
}

// TestPaddingTrim 尾部 padding 逐形态（帧内 + 帧尾空白）——W9-2 的 quirk 面。
func TestPaddingTrim(t *testing.T) {
	p := NewStreamParser()
	cases := []struct {
		name  string
		event string
		data  string
	}{
		{"帧内不定长空格", "ping", `{"type":"ping"                    }`},
		{"帧内单空格", "ping", `{"type":"ping" }`},
		{"帧内零空格", "ping", `{"type":"ping"}`},
		{"帧尾空白", "conversation_ready", "{\"type\":\"conversation_ready\"}\n  "},
		{"帧内 + 帧尾", "conversation_ready", `{"type":"conversation_ready" }  `},
	}
	for _, tc := range cases {
		evs, err := p.Feed(tc.event, []byte(tc.data))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if len(evs) != 0 {
			t.Fatalf("%s: events = %+v, want none", tc.name, evs)
		}
	}
}

// TestMessageLimitHardLimit 硬限面（hit_limit / 窗口 over_limit）——实抓 4 流都是
// within_limit，硬限形态按 CWK client.py:496-533 的字段语义合成断言。
func TestMessageLimitHardLimit(t *testing.T) {
	p := NewStreamParser()
	if _, err := p.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m","model":"claude-sonnet-5-5"}}`)); err != nil {
		t.Fatalf("start: %v", err)
	}
	frame := `{"type":"message_limit","message_limit":{"type":"hit_limit","representativeClaim":"five_hour",` +
		`"windows":{"5h":{"status":"over_limit","resets_at":1791982800,"utilization":1.0},` +
		`"7d":{"status":"within_limit","resets_at":1792519200,"utilization":0.4}}}}`
	evs, err := p.Feed("message_limit", []byte(frame))
	if err != nil {
		t.Fatalf("message_limit: %v", err)
	}
	if len(evs) != 1 || evs[0].Usage == nil {
		t.Fatalf("events = %+v", evs)
	}
	var q QuotaSummary
	if err := json.Unmarshal(evs[0].Usage.Extra["claude_web_quota"], &q); err != nil {
		t.Fatalf("quota: %v", err)
	}
	if !q.HardLimit || q.WorstUtilization != 1.0 || q.WorstWindow != "5h" {
		t.Fatalf("hard limit summary = %+v", q)
	}
}

// TestMessageLimitHardLimitRealCorpus W43-3 §4.3 R5：真实语料硬限词是
// `exceeded_limit`（type 与窗口 status 双处），旧枚举口径（hit_limit/over_limit）
// 对其恒 false。本测试钉两件：① 全路径（Feed → Usage.Extra["claude_web_quota"]）
// 对 w28 原样帧判硬限；② summarize 的「非安全即硬限」口径表（含旧词汇回归与
// 未知词保守面）。帧体为 W43-3 语料 w28 原样（931 B，含帧内 padding）。
func TestMessageLimitHardLimitRealCorpus(t *testing.T) {
	p := NewStreamParser()
	if _, err := p.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m","model":"claude-sonnet-5-5"}}`)); err != nil {
		t.Fatalf("start: %v", err)
	}
	frame := `{"type":"message_limit","message_limit":{"type":"exceeded_limit","resetsAt":1792000800,"remaining":null,"perModelLimit":false,"representativeClaim":"five_hour","overageDisabledReason":"overage_not_provisioned","overageInUse":false,"windows":{"5h":{"status":"exceeded_limit","resets_at":1792000800,"utilization":1.01,"surpassed_threshold":1.0},"7d":{"status":"within_limit","resets_at":1792519200,"utilization":0.04}},"resolved":{"status":"exceeded","limit":{"kind":"session","group":"session","percent":100,"severity":"critical","resets_at":"2026-10-14T18:00:00+00:00","scope":null,"is_active":true},"spend":null,"disabled_reason":"overage_not_provisioned","notice":{"title":"Limit reached","text":null,"cta":{"copy":"Upgrade","intent":"upgrade","redirect_hint":null},"is_dismissible":false}},"notice":{"title":"Limit reached","text":null,"cta":{"copy":"Upgrade","intent":"upgrade","redirect_hint":null},"is_dismissible":false}}  }`
	evs, err := p.Feed("message_limit", []byte(frame))
	if err != nil {
		t.Fatalf("message_limit: %v", err)
	}
	if len(evs) != 1 || evs[0].Usage == nil {
		t.Fatalf("events = %+v", evs)
	}
	var q QuotaSummary
	if err := json.Unmarshal(evs[0].Usage.Extra["claude_web_quota"], &q); err != nil {
		t.Fatalf("quota: %v", err)
	}
	if !q.HardLimit || q.WorstUtilization != 1.01 || q.WorstWindow != "5h" {
		t.Fatalf("w28 原样帧硬限摘要 = %+v（want HardLimit=true worst=1.01/5h）", q)
	}

	// summarize 口径表：「非安全即硬限」——含旧词汇回归（hit_limit/over_limit）
	// 与未知非安全词的保守面。
	win := func(status string) map[string]MessageLimitWindow {
		return map[string]MessageLimitWindow{"5h": {Status: status, ResetsAt: 1, Utilization: 0.5}}
	}
	cases := []struct {
		name string
		in   MessageLimit
		want bool
	}{
		{"within 双处", MessageLimit{Type: "within_limit", Windows: win("within_limit")}, false},
		{"type 空 + 窗口空 status", MessageLimit{Type: "", Windows: win("")}, false},
		{"type 空 + 无窗口", MessageLimit{Type: ""}, false},
		{"实抓 exceeded 双处", MessageLimit{Type: "exceeded_limit", Windows: win("exceeded_limit")}, true},
		{"CWK hit_limit", MessageLimit{Type: "hit_limit", Windows: win("within_limit")}, true},
		{"CWK 窗口 over_limit", MessageLimit{Type: "within_limit", Windows: win("over_limit")}, true},
		{"type exceeded + 窗口 within", MessageLimit{Type: "exceeded_limit", Windows: win("within_limit")}, true},
		{"未知非安全词（保守判硬限）", MessageLimit{Type: "within_limit", Windows: win("future_limit_v2")}, true},
	}
	for _, tc := range cases {
		if got := tc.in.summarize().HardLimit; got != tc.want {
			t.Fatalf("%s: HardLimit = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestEventNameMismatch 双源核：`event:` 行与 data.type 不一致 → 显式报错
// （不静默按一方分派；interactions Q6 同款加固）。
func TestEventNameMismatch(t *testing.T) {
	p := NewStreamParser()
	if _, err := p.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m","model":"x"}}`)); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := p.Feed("message_stop", []byte(`{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`)); err == nil {
		t.Fatalf("event/data.type 不一致必须报错（ErrEventNameMismatch）")
	} else if !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("err = %v, want ErrEventNameMismatch", err)
	}
}

// TestTerminalAndOrder 终态纪律：message_stop 后数据帧报错；块事件先于
// message_start 报错；未知事件/未知 delta 子类型忽略（版本策略）。
func TestTerminalAndOrder(t *testing.T) {
	// 未知事件名：忽略（不炸）。
	p := NewStreamParser()
	if evs, err := p.Feed("some_future_event", []byte(`{"type":"some_future_event","x":1}`)); err != nil || len(evs) != 0 {
		t.Fatalf("未知事件: evs=%+v err=%v, want none", evs, err)
	}
	// 块事件先于 message_start：显式报错。
	if _, err := p.Feed("content_block_start", []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"text"}}`)); err == nil {
		t.Fatalf("块事件先于 message_start 必须报错")
	}
	// 未知 delta 子类型：忽略（块已开）。
	p2 := NewStreamParser()
	_, _ = p2.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m","model":"x"}}`))
	_, _ = p2.Feed("content_block_start", []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"text"}}`))
	if evs, err := p2.Feed("content_block_delta", []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"citations_delta"}}`)); err != nil || len(evs) != 0 {
		t.Fatalf("未知 delta: evs=%+v err=%v, want none", evs, err)
	}
	// 未开块索引：显式报错。
	if _, err := p2.Feed("content_block_delta", []byte(`{"type":"content_block_delta","index":7,"delta":{"type":"text_delta","text":"x"}}`)); err == nil {
		t.Fatalf("未知块索引必须报错")
	}
	// message_stop 后数据帧：显式报错。
	_, _ = p2.Feed("content_block_stop", []byte(`{"type":"content_block_stop","index":0}`))
	_, _ = p2.Feed("message_delta", []byte(`{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`))
	_, _ = p2.Feed("message_stop", []byte(`{"type":"message_stop"}`))
	if _, err := p2.Feed("message_delta", []byte(`{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`)); err == nil {
		t.Fatalf("message_stop 后数据帧必须报错")
	}
	if tail := p2.Finish(); len(tail) != 0 {
		t.Fatalf("正常收尾 Finish = %+v, want empty", tail)
	}
}

// TestTruncatedStream 截断流（无 message_stop 的 EOF）→ 终态 transport 错误事件
// （不补帧掩盖）；零产出流 → 无补充（调用方按「上游零产出」处理）。
func TestTruncatedStream(t *testing.T) {
	p := NewStreamParser()
	_, _ = p.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m","model":"x"}}`))
	_, _ = p.Feed("content_block_start", []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"text"}}`))
	_, _ = p.Feed("content_block_delta", []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"half"}}`))
	tail := p.Finish()
	if len(tail) != 1 || tail[0].Kind != ir.EventError || tail[0].ErrorReason != ir.ErrorTransport {
		t.Fatalf("截断流 Finish = %+v, want transport error event", tail)
	}
	if !strings.Contains(tail[0].Error.Message, "message_stop") {
		t.Fatalf("错误消息 = %q", tail[0].Error.Message)
	}
	empty := NewStreamParser()
	if tail := empty.Finish(); len(tail) != 0 {
		t.Fatalf("零产出流 Finish = %+v, want empty", tail)
	}
}

// TestParseError 错误信封面（429/401/403 与流内 error 帧；W9-1 G5 未证形态 →
// 多形态容忍 + 兜底提取）。
func TestParseError(t *testing.T) {
	// Anthropic 式信封。
	e := ParseError([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"Rate limited"}}`))
	if e.Source != "claude_web" || e.Type != "rate_limit_error" || e.Message != "Rate limited" {
		t.Fatalf("anthropic 式信封 = %+v", e)
	}
	// `{"error":{"message":…}}`。
	e = ParseError([]byte(`{"error":{"message":"nope"}}`))
	if e.Message != "nope" || e.Type != "api_error" {
		t.Fatalf("简化信封 = %+v", e)
	}
	// 顶层 message。
	e = ParseError([]byte(`{"message":"bad request"}`))
	if e.Message != "bad request" {
		t.Fatalf("顶层 message = %+v", e)
	}
	// 非 JSON（CF 挑战页 HTML）→ 兜底提取（绝不透传原文）。
	e = ParseError([]byte(`<html><body>Just a moment...</body></html>`))
	if e.Message == "" || strings.Contains(e.Message, "<html>") {
		t.Fatalf("HTML 兜底 = %+v", e)
	}
	// 空体。
	e = ParseError(nil)
	if e.Type != "api_error" || e.Message == "" {
		t.Fatalf("空体 = %+v", e)
	}
	// 流内 error 帧（HTTP 200 下）。
	p := NewStreamParser()
	_, _ = p.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m","model":"x"}}`))
	evs, err := p.Feed("error", []byte(`{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`))
	if err != nil {
		t.Fatalf("error 帧: %v", err)
	}
	if len(evs) != 1 || evs[0].Kind != ir.EventError || evs[0].Error.Type != "overloaded_error" || evs[0].Error.Message != "Overloaded" {
		t.Fatalf("流内 error = %+v", evs)
	}
	// error 帧之后的数据帧：终态（显式报错）。
	if _, err := p.Feed("message_stop", []byte(`{"type":"message_stop"}`)); err == nil {
		t.Fatalf("error 后数据帧必须报错")
	}
	if tail := p.Finish(); len(tail) != 0 {
		t.Fatalf("error 终态 Finish = %+v, want empty", tail)
	}
}

// TestThinkingAndToolBlocks 思考/工具块面（W9-2 实抓未触发 thinking——按 W9-1
// 源码级帧集互证形态断言：thinking 块 + thinking_delta + signature_delta +
// tool_use 块 + input_json_delta）。
func TestThinkingAndToolBlocks(t *testing.T) {
	p := NewStreamParser()
	_, _ = p.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m","model":"claude-sonnet-5-5"}}`))
	// thinking 块。
	evs, err := p.Feed("content_block_start", []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`))
	if err != nil || len(evs) != 1 || evs[0].BlockKind != ir.BlockThinking {
		t.Fatalf("thinking block_start = %+v err=%v", evs, err)
	}
	evs, err = p.Feed("content_block_delta", []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"let me think"}}`))
	if err != nil || len(evs) != 1 || evs[0].BlockKind != ir.BlockThinking || evs[0].TextDelta != "let me think" {
		t.Fatalf("thinking_delta = %+v err=%v", evs, err)
	}
	evs, err = p.Feed("content_block_delta", []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"c2ln"}}`))
	if err != nil || len(evs) != 1 || evs[0].SignatureDelta != "c2ln" {
		t.Fatalf("signature_delta = %+v err=%v", evs, err)
	}
	_, _ = p.Feed("content_block_stop", []byte(`{"type":"content_block_stop","index":0}`))
	// tool_use 块（id/name 在 start 帧；input_json_delta → ArgsDelta）。
	evs, err = p.Feed("content_block_start", []byte(`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"get_weather","input":{}}}`))
	if err != nil || len(evs) != 1 || evs[0].BlockKind != ir.BlockServerToolUse || evs[0].ToolCallID != "toolu_1" || evs[0].ToolName != "get_weather" {
		t.Fatalf("tool_use block_start = %+v err=%v", evs, err)
	}
	evs, err = p.Feed("content_block_delta", []byte(`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"city\":"}}`))
	if err != nil || len(evs) != 1 || evs[0].BlockKind != ir.BlockServerToolUse || evs[0].ArgsDelta != `{"city":` {
		t.Fatalf("input_json_delta = %+v err=%v", evs, err)
	}
	_, _ = p.Feed("content_block_stop", []byte(`{"type":"content_block_stop","index":1}`))
	// 未知块类型 → BlockUnknown（保真旁路，不炸）。
	evs, err = p.Feed("content_block_start", []byte(`{"type":"content_block_start","index":2,"content_block":{"type":"future_block_v9"}}`))
	if err != nil || len(evs) != 1 || evs[0].BlockKind != ir.BlockUnknown {
		t.Fatalf("未知块 = %+v err=%v", evs, err)
	}
}

// TestStopReasonMapping stop_reason 映射（end_turn/stop_sequence → stop；
// max_tokens → length；tool_use → toolUse；refusal → error；未知原样透传）。
func TestStopReasonMapping(t *testing.T) {
	cases := []struct {
		in   string
		want ir.StopReason
	}{
		{"end_turn", ir.StopStop},
		{"stop_sequence", ir.StopStop},
		{"max_tokens", ir.StopLength},
		{"tool_use", ir.StopToolUse},
		{"refusal", ir.StopError},
		{"some_future_reason", ir.StopReason("some_future_reason")},
	}
	for _, tc := range cases {
		if got := mapStopReason(tc.in); got != tc.want {
			t.Fatalf("mapStopReason(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestStopSequencePassthrough W43-3 §4.3 R1：message_delta 的 `stop_sequence`
// （值 usage_limit = 配额烧穿截断）与 `stop_details` 不再被丢弃——进 IR 细节面
// （StopReason 仍归一为 stop，四值契约不变；Anthropic 出口据 StopSequence 回填）。
func TestStopSequencePassthrough(t *testing.T) {
	p := NewStreamParser()
	_, _ = p.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m","model":"claude-sonnet-5-5"}}`))
	// w28 原样形态：stop_reason=stop_sequence + stop_sequence=usage_limit + stop_details=null。
	evs, err := p.Feed("message_delta", []byte(`{"type":"message_delta","delta":{"stop_reason":"stop_sequence","stop_sequence":"usage_limit","stop_details":null}}`))
	if err != nil || len(evs) != 1 {
		t.Fatalf("message_delta = %+v err=%v", evs, err)
	}
	ev := evs[0]
	if ev.StopReason != ir.StopStop || ev.StopSequence != "usage_limit" || string(ev.StopDetails) != "null" {
		t.Fatalf("R1 细节面 = %+v（want StopReason=stop / StopSequence=usage_limit / StopDetails=null）", ev)
	}
	// 常态（end_turn + null 双字段）：StopSequence 空（null=未提供），StopDetails
	// 原样保真（null 在场）。
	evs, err = p.Feed("message_delta", []byte(`{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null,"stop_details":null}}`))
	if err != nil || len(evs) != 1 {
		t.Fatalf("end_turn message_delta = %+v err=%v", evs, err)
	}
	if evs[0].StopSequence != "" || string(evs[0].StopDetails) != "null" {
		t.Fatalf("常态细节面 = %+v（want StopSequence 空 / StopDetails null）", evs[0])
	}
	// 全空 message_delta（无 stop_reason、无细节面）→ 无事件（既有语义）。
	evs, err = p.Feed("message_delta", []byte(`{"type":"message_delta","delta":{}}`))
	if err != nil || len(evs) != 0 {
		t.Fatalf("空 delta = %+v err=%v, want none", evs, err)
	}
}

// TestThinkingObservationFlags W43-3 §6-3：thinking 摘要态标记进 IR 观测面
// （block_start.truncated → ThinkingTruncated；block_stop.summary_pending →
// SummaryPending；nil = wire 无键，不把「未提供」伪装成 false；仅 thinking 块）。
func TestThinkingObservationFlags(t *testing.T) {
	p := NewStreamParser()
	_, _ = p.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m","model":"claude-sonnet-5-5"}}`))
	// 实抓形态（W43-3 G1）：truncated:true / summaries:[] / cut_off:false。
	evs, err := p.Feed("content_block_start", []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","truncated":true,"summaries":[],"cut_off":false}}`))
	if err != nil || len(evs) != 1 || evs[0].ThinkingTruncated == nil || !*evs[0].ThinkingTruncated {
		t.Fatalf("thinking start truncated = %+v err=%v", evs, err)
	}
	// 实抓形态：stop 带 summary_pending:false（在场且 false ≠ 未提供）。
	evs, err = p.Feed("content_block_stop", []byte(`{"type":"content_block_stop","index":0,"stop_timestamp":"2026-10-14T10:26:10.975253603Z","summary_pending":false}`))
	if err != nil || len(evs) != 1 || evs[0].SummaryPending == nil || *evs[0].SummaryPending {
		t.Fatalf("thinking stop summary_pending = %+v err=%v", evs, err)
	}
	// 无键 → nil（未提供）。
	evs, err = p.Feed("content_block_start", []byte(`{"type":"content_block_start","index":1,"content_block":{"type":"thinking","thinking":""}}`))
	if err != nil || len(evs) != 1 || evs[0].ThinkingTruncated != nil {
		t.Fatalf("thinking start 无 truncated = %+v err=%v", evs, err)
	}
	evs, err = p.Feed("content_block_stop", []byte(`{"type":"content_block_stop","index":1}`))
	if err != nil || len(evs) != 1 || evs[0].SummaryPending != nil {
		t.Fatalf("thinking stop 无 summary_pending = %+v err=%v", evs, err)
	}
	// 非 thinking 块带 truncated（合成）→ 不落观测面（thinking 专用门）。
	evs, err = p.Feed("content_block_start", []byte(`{"type":"content_block_start","index":2,"content_block":{"type":"text","text":"","truncated":true}}`))
	if err != nil || len(evs) != 1 || evs[0].ThinkingTruncated != nil {
		t.Fatalf("text 块 truncated 泄漏 = %+v err=%v", evs, err)
	}
}
