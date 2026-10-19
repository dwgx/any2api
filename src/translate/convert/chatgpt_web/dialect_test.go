package chatgpt_web

// 三方言对照（A/B/C 三实现的帧形差异，逐条 file:line）——每个用例断言**本包
// 取哪一家**及为什么（与参考实现的差异在 wire.go「关键取舍」与报告 §方言差异
// 里逐条记录；golden 帧到位后 S4/S5 重校）。
//
//	A = DestroyerDarkNess/chatgpt_web_python_api@76e0731（登录档；唯一接思考）
//	B = jiji262/gpt2api@956b46a（/f/ 代 v1 patch 解析器）
//	C = lanqian528/chat2api@85971f0（旧快照为主）
//	CF = 6Kmfi6HP/chatgpt2api-cf@de9dad3（Android 匿名档；历史过滤/尾读最细）
//	F = adam-s/toolkit@4c03e3e（浏览器内；简写帧 lastPath/lastOp）

import (
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

func TestDialect01ShorthandOpMemory(t *testing.T) {
	// F:186-196：lastPath/lastOp 都记忆并重放（简写帧按上一 op 作用）；
	// B:14-17/93-106：只继承路径（curP），顶层字符串帧不查 op；
	// C：无简写帧概念（快照为主）。本包取 B 语义（/f/ 代最强证据）。
	events, _ := mustRun(t, []tframe{
		dataFrame(`{"p":"/message/content/parts/0","o":"append","v":"A"}`),
		dataFrame(`{"v":"B"}`), // 简写帧：继承路径
		dataFrame(`{"p":"/message/content/parts/0","o":"replace","v":"C"}`),
		dataFrame(`[DONE]`),
	})
	text, _ := collectText(events)
	if text != "ABC" {
		t.Fatalf("text = %q（want ABC——B 语义：简写继承路径、顶层不查 op）", text)
	}
}

func TestDialect02ArrayOpGate(t *testing.T) {
	// B:126-131：数组内 op ∈ {"", append} 才产出（replace 跳过）；
	// F:236-262：数组内按序 apply（op 语义完整——replace 会覆盖工作文档）。
	// 本包取 B 语义。
	events, _ := mustRun(t, []tframe{
		dataFrame(`{"p":"","o":"patch","v":[{"p":"/message/content/parts/0","o":"replace","v":"X"},{"p":"/message/content/parts/0","o":"append","v":"Y"}]}`),
		dataFrame(`[DONE]`),
	})
	text, _ := collectText(events)
	if text != "Y" {
		t.Fatalf("text = %q（want Y——数组内 replace 被 op 门控跳过）", text)
	}
}

func TestDialect03ThoughtsRouting(t *testing.T) {
	// B:81-85：thoughts 前缀增量**全部丢弃**（当前不回给客户端）；
	// A:94-160：映射为 reasoning 文本（唯一接思考的实现）；
	// C：无 thoughts 分支。本包取 A 语义（产品出口要 reasoning_content）。
	events, _ := mustRun(t, []tframe{
		dataFrame(`{"p":"/message/content/thoughts/0","o":"append","v":"inner monologue"}`),
		dataFrame(`[DONE]`),
	})
	text, thinking := collectText(events)
	if text != "" || thinking != "inner monologue" {
		t.Fatalf("text=%q thinking=%q（want 思考进 thinking 块、不进正文）", text, thinking)
	}
}

func TestDialect04TerminalCriteria(t *testing.T) {
	// B:95-100：status finished_successfully → 终止（任意上下文）；
	// C:270-292：finished_successfully 必须配 end_turn 才收尾（更严）；
	// CF:122-128：live 消息 finished + end_turn → 收束（end_turn 是权威信号）。
	// 本包：B 判据 + live 上下文门控（防 B 在工具消息上下文下误终结 [差异记录]）
	// + CF 的 end_turn 兜底。

	// ① live 上下文 status finished（无 end_turn）→ 终止（B 语义）。
	events, _ := mustRun(t, []tframe{
		dataFrame(`{"v":{"message":{"id":"m1","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"text","parts":["x"]},"status":"in_progress"}}}`),
		dataFrame(`{"p":"/message/status","o":"replace","v":"finished_successfully"}`),
	})
	if countKind(events, ir.EventDone) != 1 {
		t.Fatalf("live status-finish: done = %d（want 1——B 语义）", countKind(events, ir.EventDone))
	}

	// ② 抑制上下文（工具消息）status finished（无 end_turn）→ **不**终止
	//（本包安全门；B 会在此误终结——差异记录）。
	events, _ = mustRun(t, []tframe{
		dataFrame(`{"v":{"message":{"id":"t1","author":{"role":"assistant"},"recipient":"python","content":{"content_type":"text","parts":[""]},"status":"in_progress"}}}`),
		dataFrame(`{"p":"/message/status","o":"replace","v":"finished_successfully"}`),
	})
	if countKind(events, ir.EventDone) != 0 {
		t.Fatalf("suppressed status-finish: done = %d（want 0——本包安全门）", countKind(events, ir.EventDone))
	}

	// ③ 抑制上下文 end_turn → 终止（CF 权威信号，不受门控）。
	//（同 id 演进：先 in_progress 采纳，再同 id 终帧——否则 finished 新 id 会被
	// 历史回放判据先行丢弃，到不了结束判据。）
	events, _ = mustRun(t, []tframe{
		dataFrame(`{"v":{"message":{"id":"t1","author":{"role":"assistant"},"recipient":"python","content":{"content_type":"text","parts":[""]},"status":"in_progress"}}}`),
		dataFrame(`{"v":{"message":{"id":"t1","author":{"role":"assistant"},"recipient":"python","content":{"content_type":"text","parts":[""]},"status":"finished_successfully","end_turn":true}}}`),
	})
	if countKind(events, ir.EventDone) != 1 {
		t.Fatalf("suppressed end_turn: done = %d（want 1——end_turn 权威）", countKind(events, ir.EventDone))
	}
}

func TestDialect05SnapshotGuard(t *testing.T) {
	// B:168-193：纯前缀差分——快照回归（更短）→ 整段重发；
	// A:828-834：回退守卫——相等/回归 → 不重发；
	// C:200-266：盲切 len_last_content（无前缀校验）。本包取 A 守卫。
	events, _ := mustRun(t, []tframe{
		dataFrame(`{"v":{"message":{"id":"m1","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"text","parts":["full answer"]},"status":"in_progress"}}}`),
		dataFrame(`{"v":{"message":{"id":"m1","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"text","parts":["full"]},"status":"in_progress"}}}`),
		dataFrame(`[DONE]`),
	})
	text, _ := collectText(events)
	if text != "full answer" {
		t.Fatalf("text = %q（want 'full answer'——回归不重发，A 守卫）", text)
	}
}

func TestDialect06HistoryPredicate(t *testing.T) {
	// CF:103-118：id+status+message_type+channel 四元组；channel=final 是 live 标记；
	// Go:444-446：message_type ∉ {next, continue} 一律丢；C:201-202：只认第一条 id
	//（换 id 即丢——本例 C 会误丢）。本包取 CF：channel=final 的 finished 帧不是历史。
	events, p := mustRun(t, []tframe{
		dataFrame(`{"v":{"message":{"id":"new-1","author":{"role":"assistant"},"channel":"final","content":{"content_type":"text","parts":["tail"]},"status":"finished_successfully","end_turn":true}}}`),
		dataFrame(`[DONE]`),
	})
	text, _ := collectText(events)
	if text != "tail" {
		t.Fatalf("text = %q（want tail——channel=final 非历史）", text)
	}
	if p.HistoryDropped() != 0 {
		t.Fatalf("history dropped = %d（want 0）", p.HistoryDropped())
	}
}

func TestDialect07ErrorFrame(t *testing.T) {
	// A:841-842：流内 error 帧 → 抛错；B delta.go：无 error 分支（静默忽略）；
	// Go request.go:426-429：error 帧 → 500。本包取 A 语义：IR error 事件（终态）。
	events, _ := mustRun(t, []tframe{
		dataFrame(`{"error":"boom"}`),
	})
	last := events[len(events)-1]
	if last.Kind != ir.EventError || last.Error == nil || last.Error.Message != "boom" {
		t.Fatalf("events = %v / %+v", eventKinds(events), last.Error)
	}
}

func TestDialect08DoubleEncoding(t *testing.T) {
	// A:226-252：双重编码容错（一层嵌套 JSON 字符串 + 内嵌 SSE 项取最后一帧）；
	// B/C：直接 json.Unmarshal——嵌套层即丢。本包取 A 容错。
	events, _ := mustRun(t, []tframe{
		// 嵌套 JSON 字符串层。
		dataFrame(`"{\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":\"nested\"}"`),
		// 内嵌 SSE 项（整帧 = 一段 SSE 文本，取最后一帧的 data）。
		dataFrame(`data: {"p":"/message/content/parts/0","o":"append","v":"embedded"}`),
		dataFrame(`[DONE]`),
	})
	text, _ := collectText(events)
	if text != "nestedembedded" {
		t.Fatalf("text = %q（want nestedembedded）", text)
	}
}

func TestDialect09Heartbeat(t *testing.T) {
	// B:66-69：非 JSON（心跳）忽略；A:213-214：`:` 注释行跳过（切帧层）。
	// 两方言同向，本包两层都容忍。
	events, _ := mustRun(t, []tframe{
		dataFrame("some non-json heartbeat"),
		dataFrame(`{"p":"/message/content/parts/0","o":"append","v":"ok"}`),
		dataFrame(`[DONE]`),
	})
	text, _ := collectText(events)
	if text != "ok" {
		t.Fatalf("text = %q", text)
	}
}

func TestDialect10ContentTypeBranches(t *testing.T) {
	// C:129-340：content_type 分支（text/multimodal_text/code/execution_output）；
	// A/B 不看 content_type（B 只认 parts/0 补丁；A 只在思考匹配里用）。
	// 本包：C 的分支集 → IR 块类型；未知分支有文本则当文本（C:265 的 text 兜底）。
	events, _ := mustRun(t, []tframe{
		dataFrame(`{"v":{"message":{"id":"m1","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"future_type","text":"still text"},"status":"in_progress"}}}`),
		dataFrame(`[DONE]`),
	})
	text, _ := collectText(events)
	if text != "still text" {
		t.Fatalf("text = %q（want 'still text'——未知 content_type 文本兜底）", text)
	}
}
