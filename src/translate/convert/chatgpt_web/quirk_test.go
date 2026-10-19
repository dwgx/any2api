package chatgpt_web

// quirk 矩阵：KB `docs/architecture.md` §可复用点
// 「可复用的测试用例 / quirk 清单」10 条**逐条入测**。
//
// 全部 fixture 为**构造帧/构造响应体**（从三实现源码行为反推帧形——标注
// 「构造，非实抓」）；真机 golden 帧到位后 S4/S5 重校（brief：真机校准留 S4/S5）。
//
// quirk 7-10 本体在请求/求解器侧（S3/S4 面）——其线上失败面 = HTTP 403/404/HTML
// 响应；解析侧唯一职责 = 安全错误信封（ParseError）。fixture 为构造响应体，
// 形态未证处标注 [构造/待实抓校准]。

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

type quirkFixture struct {
	id       int
	name     string
	kb       string   // KB 原文摘录（§可复用点 quirk 清单 N）
	evidence string   // 三实现 file:line
	note     string   // 构造标注 + 帧形来源
	frames   []tframe // 帧面 fixture（nil = 响应体面）
	body     string   // 响应体 fixture（frames 为 nil 时用）
	check    func(t *testing.T, r *quirkRun)
}

type quirkRun struct {
	events       []ir.StreamEvent
	perFrame     [][]ir.StreamEvent
	finishEvents []ir.StreamEvent
	p            *StreamParser
	err          error
	e            *ir.Error
	body         string // 响应体面 fixture 原文（Raw 保真断言用）
}

func runQuirk(t *testing.T, f quirkFixture) *quirkRun {
	t.Helper()
	r := &quirkRun{body: f.body}
	if f.frames == nil {
		r.e = ParseError([]byte(f.body))
		return r
	}
	r.p = NewStreamParser()
	for _, fr := range f.frames {
		evs, err := r.p.Feed(fr.event, []byte(fr.data))
		r.perFrame = append(r.perFrame, evs)
		r.events = append(r.events, evs...)
		if err != nil {
			r.err = err
			return r
		}
	}
	r.finishEvents = r.p.Finish()
	r.events = append(r.events, r.finishEvents...)
	return r
}

var quirkMatrix = []quirkFixture{
	{
		id:       1,
		name:     "简写帧状态机",
		kb:       "简写帧状态机：`{v}` 复用 lastPath/lastOp（toolkit 原话：不维护会丢 ~90% 正文）",
		evidence: "toolkit openai-adapter.ts:186-196；gpt2api delta.go:14-17（curP 继承）",
		note:     "构造，非实抓：v1 标记帧 + 显式帧 + 两条简写帧（帧形反推自 F:186-196 / B:93-106）",
		frames: []tframe{
			{event: evDeltaEncoding, data: `"v1"`},
			dataFrame(`{"p":"/message/content/parts/0","o":"append","v":"Hello"}`),
			dataFrame(`{"v":" world"}`),
			dataFrame(`{"v":"!"}`),
			dataFrame(`[DONE]`),
		},
		check: func(t *testing.T, r *quirkRun) {
			text, _ := collectText(r.events)
			if text != "Hello world!" {
				t.Fatalf("text = %q（want 'Hello world!'——简写帧继承 curP）", text)
			}
			if r.p.ProtocolVersion() != markerV1 {
				t.Fatalf("version = %q（want v1）", r.p.ProtocolVersion())
			}
			if countKind(r.events, ir.EventDone) != 1 {
				t.Fatalf("done = %d（want 1）", countKind(r.events, ir.EventDone))
			}
		},
	},
	{
		id:       2,
		name:     "批量 patch 夹结束标记",
		kb:       "批量 patch 里夹结束标记：`{\"p\":\"\",\"o\":\"patch\",\"v\":[{p:\"/message/status\",o:\"replace\",v:\"finished_successfully\"},…]}`（lowkruc 文档样例）——状态机必须在数组内部就能判结束",
		evidence: "lowkruc 文档「Text increment case」；gpt2api delta.go:107-137；toolkit :196-205",
		note:     "构造，非实抓：数组内 append + status + end_turn 三连（帧形 = lowkruc 样例原文）",
		frames: []tframe{
			dataFrame(`{"p":"/message/content/parts/0","o":"append","v":"Hello"}`),
			dataFrame(`{"p":"","o":"patch","v":[{"p":"/message/content/parts/0","o":"append","v":"!"},{"p":"/message/status","o":"replace","v":"finished_successfully"},{"p":"/message/end_turn","o":"replace","v":true}]}`),
			dataFrame(`[DONE]`),
		},
		check: func(t *testing.T, r *quirkRun) {
			text, _ := collectText(r.events)
			if text != "Hello!" {
				t.Fatalf("text = %q（want 'Hello!'）", text)
			}
			if countKind(r.events, ir.EventDone) != 1 {
				t.Fatalf("done = %d（want 1）", countKind(r.events, ir.EventDone))
			}
			// 结束在数组帧内即判定（不是等 [DONE]）。
			if countKind(r.perFrame[1], ir.EventDone) != 1 {
				t.Fatalf("batch frame events = %v（数组内未判结束）", eventKinds(r.perFrame[1]))
			}
		},
	},
	{
		id:       3,
		name:     "历史回放",
		kb:       "历史回放：同一流里先重放 N 条已完成消息，再给新回复；判据三选（id 切换 / message_type / channel）",
		evidence: "CF stream.ts:103-118（主判据）；Go request.go:444-446；chat2api chatFormat.py:201-202",
		note:     "构造，非实抓：1 条历史 assistant 帧（finished + message_type=continue）+ live 消息（in_progress）+ 补丁 + 终帧",
		frames: []tframe{
			dataFrame(`{"v":{"message":{"id":"hist-1","author":{"role":"assistant"},"content":{"content_type":"text","parts":["old answer"]},"status":"finished_successfully","metadata":{"message_type":"continue"}}},"conversation_id":"c1"}`),
			dataFrame(`{"v":{"message":{"id":"live-1","author":{"role":"assistant"},"content":{"content_type":"text","parts":[""]},"status":"in_progress","metadata":{"message_type":"next"}}},"conversation_id":"c1"}`),
			dataFrame(`{"p":"/message/content/parts/0","o":"append","v":"new answer"}`),
			dataFrame(`{"v":{"message":{"id":"live-1","author":{"role":"assistant"},"content":{"content_type":"text","parts":["new answer"]},"status":"finished_successfully","end_turn":true,"metadata":{"message_type":"next"}}},"conversation_id":"c1"}`),
			dataFrame(`[DONE]`),
		},
		check: func(t *testing.T, r *quirkRun) {
			text, _ := collectText(r.events)
			if text != "new answer" {
				t.Fatalf("text = %q（want 'new answer'）", text)
			}
			if containsText(r.events, "old answer") {
				t.Fatalf("历史回放文本泄漏进事件流")
			}
			if r.p.HistoryDropped() != 1 {
				t.Fatalf("history dropped = %d（want 1）", r.p.HistoryDropped())
			}
			if countKind(r.events, ir.EventDone) != 1 {
				t.Fatalf("done = %d（want 1）", countKind(r.events, ir.EventDone))
			}
		},
	},
	{
		id:       4,
		name:     "recipient != all 过滤",
		kb:       "`recipient != \"all\"` 过滤：工具消息（python/browser/dalle.text2im/t2uay3k.sj1i4kz）混在主流里",
		evidence: "gpt2api delta.go:18-20；Go request.go:441-443；web_client.py:824-825",
		note:     "构造，非实抓：recipient=python 消息（含正文补丁）后接 recipient=all 消息",
		frames: []tframe{
			dataFrame(`{"v":{"message":{"id":"m1","author":{"role":"assistant"},"recipient":"python","content":{"content_type":"text","parts":[""]},"status":"in_progress"}},"conversation_id":"c1"}`),
			dataFrame(`{"p":"/message/content/parts/0","o":"append","v":"import os"}`),
			dataFrame(`{"v":{"message":{"id":"m2","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"text","parts":[""]},"status":"in_progress"}},"conversation_id":"c1"}`),
			dataFrame(`{"p":"/message/content/parts/0","o":"append","v":"answer"}`),
			dataFrame(`{"p":"/message/status","o":"replace","v":"finished_successfully"}`),
			dataFrame(`[DONE]`),
		},
		check: func(t *testing.T, r *quirkRun) {
			text, _ := collectText(r.events)
			if text != "answer" {
				t.Fatalf("text = %q（want 'answer'）", text)
			}
			if containsText(r.events, "import os") {
				t.Fatalf("工具消息正文泄漏进事件流")
			}
			if countKind(r.events, ir.EventDone) != 1 {
				t.Fatalf("done = %d（want 1）", countKind(r.events, ir.EventDone))
			}
			recs := r.p.ToolMessages()
			if len(recs) != 1 || recs[0].Reason != "recipient" || recs[0].Recipient != "python" {
				t.Fatalf("records = %+v（want 1 条 recipient=python）", recs)
			}
		},
	},
	{
		id:       5,
		name:     "思考控制串",
		kb:       "思考控制串：reasoning_started/hide_all 之类标签混在思考文本里，拼接前必须清洗",
		evidence: "web_client.py upstream.py:60-92（清洗表）；:94-114/117-160（思考提取）",
		note:     "构造，非实抓：thoughts 路径正文 + 两条控制串（简写帧继承 thoughts 路径）+ 正文切换",
		frames: []tframe{
			dataFrame(`{"p":"/message/content/thoughts/0","o":"append","v":"Let me think"}`),
			dataFrame(`{"v":"reasoning_started"}`),
			dataFrame(`{"v":" hide_all"}`),
			dataFrame(`{"p":"/message/content/parts/0","o":"append","v":"Answer"}`),
			dataFrame(`[DONE]`),
		},
		check: func(t *testing.T, r *quirkRun) {
			text, thinking := collectText(r.events)
			if text != "Answer" {
				t.Fatalf("text = %q（want 'Answer'）", text)
			}
			if thinking != "Let me think" {
				t.Fatalf("thinking = %q（want 'Let me think'——控制串必须清洗）", thinking)
			}
			for _, label := range []string{"reasoning_started", "hide_all"} {
				if containsText(r.events, label) {
					t.Fatalf("控制串 %q 污染输出", label)
				}
			}
		},
	},
	{
		id:       6,
		name:     "尾部静默",
		kb:       "尾部静默：end_turn 后上游再握 1.3–3s，需要 250ms 级有界尾读，否则代理端卡住",
		evidence: "CF stream.ts:306-316,416-427（STREAM_TAIL_GRACE_MS=250）；:122-128（end_turn）",
		note:     "构造，非实抓：终帧（finished + end_turn）后**无 [DONE]**、EOF 直接到来——解析侧必须已在终帧收束，不依赖连接关闭（尾读是服务壳的活）",
		frames: []tframe{
			dataFrame(`{"v":{"message":{"id":"m1","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"text","parts":[""]},"status":"in_progress"}}}`),
			dataFrame(`{"p":"/message/content/parts/0","o":"append","v":"done"}`),
			dataFrame(`{"v":{"message":{"id":"m1","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"text","parts":["done"]},"status":"finished_successfully","end_turn":true}}}`),
		},
		check: func(t *testing.T, r *quirkRun) {
			if countKind(r.events, ir.EventDone) != 1 {
				t.Fatalf("done = %d（want 1）", countKind(r.events, ir.EventDone))
			}
			if countKind(r.perFrame[2], ir.EventDone) != 1 {
				t.Fatalf("end_turn 帧未收束（events = %v）", eventKinds(r.perFrame[2]))
			}
			// EOF（Finish）不再补任何事件——终帧即收束，无截断误报。
			if len(r.finishEvents) != 0 {
				t.Fatalf("finish events = %v（want 空——已收束）", eventKinds(r.finishEvents))
			}
		},
	},
	{
		id:       7,
		name:     "oai-client-version 漂移（403）",
		kb:       "`oai-client-version` 漂移：跟不上下游 build 会被判 \"Unusual activity\"（403）",
		evidence: "toolkit routes.ts:56-61（注释：落后会被判 Unusual activity）",
		note:     "构造响应体，非实抓 [构造/待实抓校准]：quirk 本体在请求侧（S3/S4 面）；解析侧职责 = 403 体 → 安全错误信封（detail 形态未证）",
		body:     `{"detail":"Unusual activity detected. Please refresh the page and try again."}`,
		check: func(t *testing.T, r *quirkRun) {
			if r.e == nil || r.e.Type != "api_error" || r.e.Source != "chatgpt_web" {
				t.Fatalf("envelope = %+v", r.e)
			}
			if !strings.Contains(r.e.Message, "Unusual activity") {
				t.Fatalf("message = %q", r.e.Message)
			}
		},
	},
	{
		id:       8,
		name:     "同一 oai-did 并发提交（403/挑战页）",
		kb:       "同一 `oai-did` 并发提交：毫秒级两连发被判 burst abuse（toolkit 用 per-page mutex 串行化）",
		evidence: "toolkit routes.ts:63-76（并发互斥）",
		note:     "构造响应体，非实抓 [构造/待实抓校准]：burst 判罚常以 Cloudflare 挑战页返回——解析侧职责 = 非 JSON 体安全兜底（SalvageError），绝不透传原文",
		body:     `<!DOCTYPE html><html><head><title>Just a moment...</title></head><body><div id="challenge-error-text">Enable JavaScript and cookies to continue</div></body></html>`,
		check: func(t *testing.T, r *quirkRun) {
			if r.e == nil || r.e.Message == "" {
				t.Fatalf("envelope = %+v", r.e)
			}
			if strings.Contains(r.e.Message, "<") || strings.Contains(r.e.Message, "challenge-error-text") {
				t.Fatalf("挑战页原文泄漏进 Message 面：%q", r.e.Message)
			}
			if string(r.e.Raw) != r.body || len(r.e.Raw) == 0 {
				t.Fatalf("Raw 未保真（诊断面）")
			}
		},
	},
	{
		id:       9,
		name:     "PoW 算法换代（proof 失败面）",
		kb:       "PoW 算法换代：2025 sha3_512 → 2026 FNV-1a+avalanche；求解器要做成可插拔，别把哈希钉死",
		evidence: "openai-sentinel proof_of_work.py:31-48；web_client.py:139-151（2026 代）",
		note:     "构造响应体，非实抓 [构造/待实抓校准]：quirk 本体在求解器侧（S4 面，可插拔设计）；解析侧职责 = 求解失败 403（proof 拒绝）的安全信封",
		body:     `{"error":{"message":"Invalid or expired proof token","type":"invalid_request_error","code":"invalid_proof"}}`,
		check: func(t *testing.T, r *quirkRun) {
			if r.e == nil || r.e.Code != "invalid_proof" || r.e.Type != "invalid_request_error" {
				t.Fatalf("envelope = %+v", r.e)
			}
			if !strings.Contains(r.e.Message, "proof") {
				t.Fatalf("message = %q", r.e.Message)
			}
		},
	},
	{
		id:       10,
		name:     "匿名档模型降级（404）",
		kb:       "匿名档模型降级：匿名 persona 非 `chatgpt-paid` 时高档模型直接 404",
		evidence: "chat2api ChatService.py:189-199（persona 门控）",
		note:     "构造响应体，非实抓 [构造/待实抓校准]：quirk 本体在请求/persona 侧（S3 面）；解析侧职责 = 404 体的安全信封",
		body:     `{"detail":{"code":"model_not_found","message":"The model does not exist or you do not have access to it."}}`,
		check: func(t *testing.T, r *quirkRun) {
			if r.e == nil || r.e.Code != "model_not_found" {
				t.Fatalf("envelope = %+v", r.e)
			}
			if !strings.Contains(r.e.Message, "model") {
				t.Fatalf("message = %q", r.e.Message)
			}
		},
	},
}

func TestQuirkMatrix(t *testing.T) {
	if len(quirkMatrix) != 10 {
		t.Fatalf("quirk 矩阵 = %d 条（want 10）", len(quirkMatrix))
	}
	for _, f := range quirkMatrix {
		t.Run(fmt.Sprintf("Q%02d_%s", f.id, f.name), func(t *testing.T) {
			r := runQuirk(t, f)
			if r.err != nil {
				t.Fatalf("parse error: %v", r.err)
			}
			f.check(t, r)
		})
	}
}
