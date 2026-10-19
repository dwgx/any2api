package chatgpt_web

// 件 2：v1 patch 状态机的直测（不经 StreamParser——状态机可独立使用）。

import (
	"encoding/json"
	"testing"
)

func TestPatchCurPInheritance(t *testing.T) {
	var s patchState
	// 显式帧设置 curP。
	r := s.applyFrame(pathParts0, opAppend, json.RawMessage(`"A"`))
	if r.Text != "A" {
		t.Fatalf("explicit frame text = %q（want A）", r.Text)
	}
	// 简写帧（无 p）继承 curP（B:14-17；F:186-196）。
	r = s.applyFrame("", "", json.RawMessage(`"B"`))
	if r.Text != "B" {
		t.Fatalf("shorthand text = %q（want B）——curP 继承失败", r.Text)
	}
	// 显式 p 覆盖继承。
	r = s.applyFrame("/message/other", opAppend, json.RawMessage(`"C"`))
	if r.Text != "" {
		t.Fatalf("non-parts path text = %q（want 空）", r.Text)
	}
	// 继承切到 status 路径：字符串值被当状态观察，不是正文。
	r = s.applyFrame(pathStatus, opReplace, json.RawMessage(`"in_progress"`))
	if r.Status != "in_progress" || r.Final {
		t.Fatalf("status frame = %+v", r)
	}
	r = s.applyFrame("", "", json.RawMessage(`"X"`))
	if r.Text != "" || r.Status != "X" {
		t.Fatalf("shorthand after status = %+v（want Status=X, Text 空）", r)
	}
}

func TestPatchStringFramesDoNotGateOp(t *testing.T) {
	// B:93-106：顶层字符串帧不查 op（数组内才要求 op ∈ {"", append}）。
	var s patchState
	r := s.applyFrame(pathParts0, opReplace, json.RawMessage(`"full"`))
	if r.Text != "full" {
		t.Fatalf("top-level replace text = %q（want full——B 顶层无 op 门控）", r.Text)
	}
}

func TestPatchEmptyPathIsParts0(t *testing.T) {
	// B:93-106：curP 为空（首 append 未声明 p）→ 当 parts/0 正文（空路径允许面）。
	var s patchState
	r := s.applyFrame("", "", json.RawMessage(`"bare"`))
	if r.Text != "bare" {
		t.Fatalf("empty-path text = %q（want bare——空路径 = parts/0 语义）", r.Text)
	}
}

func TestPatchArray(t *testing.T) {
	var s patchState
	outs, err := s.applyArray(json.RawMessage(`[
		{"p":"/message/content/parts/0","o":"append","v":"A"},
		{"p":"/message/content/parts/0","o":"replace","v":"B"},
		{"p":"/message/content/parts/0","o":"","v":"C"},
		{"p":"/message/content/parts/0","o":"append","v":"D"},
		{"p":"/message/status","o":"replace","v":"finished_successfully"},
		{"p":"/message/end_turn","o":"replace","v":true},
		"not-an-object",
		{"p":"/message/content/thoughts/0","o":"append","v":"think"}
	]`))
	if err != nil {
		t.Fatalf("applyArray: %v", err)
	}
	var text, thinking string
	var final, endTurn bool
	for _, o := range outs {
		text += o.Text
		thinking += o.Thinking
		final = final || o.Final
		endTurn = endTurn || o.EndTurn
	}
	// replace 项被 op 门控跳过（B:126-131）；append/"" 通过。
	if text != "ACD" {
		t.Fatalf("array text = %q（want ACD）", text)
	}
	if !final || !endTurn {
		t.Fatalf("final = %v endTurn = %v（want both true）", final, endTurn)
	}
	if thinking != "think" {
		t.Fatalf("array thinking = %q（want think）", thinking)
	}
}

func TestPatchThoughtsRouting(t *testing.T) {
	var s patchState
	// thoughts 前缀 → 思考通道（B:81-85；我们映射而非丢弃）。
	r := s.applyFrame("/message/content/thoughts/0", opAppend, json.RawMessage(`"deep"`))
	if r.Thinking != "deep" || r.Text != "" {
		t.Fatalf("thoughts frame = %+v", r)
	}
	// reasoning 类路径 → 思考通道（A:117-160）。
	r = s.applyFrame("/message/reasoning_delta", opAppend, json.RawMessage(`"more"`))
	if r.Thinking != "more" || r.Text != "" {
		t.Fatalf("reasoning_delta frame = %+v", r)
	}
	// /metadata 路径 → 忽略（A:139-141：lifecycle 标签不是正文）。
	r = s.applyFrame("/message/metadata/reasoning_content", opReplace, json.RawMessage(`"hide_all"`))
	if r.Thinking != "" || r.Text != "" {
		t.Fatalf("metadata reasoning path = %+v（want 空——A:139-141 跳过）", r)
	}
	// summary 路径：非 thoughts 前缀、非 reasoning 类 → 不是正文（B 注释提及
	// summary/thoughts 并列丢弃；本包只映射 thoughts/reasoning）。
	r = s.applyFrame("/message/content/summary/0", opAppend, json.RawMessage(`"sum"`))
	if r.Text != "" || r.Thinking != "" {
		t.Fatalf("summary path = %+v（want 空）", r)
	}
}

func TestPatchRecipientGate(t *testing.T) {
	var s patchState
	s.recipient = "python"
	r := s.applyFrame(pathParts0, opAppend, json.RawMessage(`"import os"`))
	if r.Text != "" {
		t.Fatalf("python recipient text = %q（want 空）", r.Text)
	}
	// 状态观察不受 recipient 门控（B:95-104 两个分支都观察状态）。
	r = s.applyFrame(pathStatus, opReplace, json.RawMessage(`"finished_successfully"`))
	if !r.Final {
		t.Fatalf("status under python recipient = %+v（want Final）", r)
	}
	s.recipient = "all"
	r = s.applyFrame(pathParts0, opAppend, json.RawMessage(`"answer"`))
	if r.Text != "answer" {
		t.Fatalf("all recipient text = %q", r.Text)
	}
}

func TestPatchSnapshotDelta(t *testing.T) {
	var s patchState
	// 首帧：全量。
	if got := s.snapshotDelta("Hello"); got != "Hello" {
		t.Fatalf("first snapshot delta = %q（want Hello）", got)
	}
	// 前缀增长：后缀。
	if got := s.snapshotDelta("Hello world"); got != " world" {
		t.Fatalf("growth delta = %q（want ' world'）", got)
	}
	// 相等：无增量。
	if got := s.snapshotDelta("Hello world"); got != "" {
		t.Fatalf("equal delta = %q（want 空）", got)
	}
	// 回归（更短）：不重发（A:828-834 守卫；B 会整段重发——差异记录）。
	if got := s.snapshotDelta("Hello"); got != "" {
		t.Fatalf("regression delta = %q（want 空——A 守卫）", got)
	}
	// 非前缀（内容分叉）：整段重发（B:176-179）。
	if got := s.snapshotDelta("World"); got != "World" {
		t.Fatalf("non-prefix delta = %q（want World）", got)
	}
	// patch 与快照混用：accText 作基线（本包差异面）。
	var s2 patchState
	s2.noteText("Hello")
	if got := s2.snapshotDelta("Hello world"); got != " world" {
		t.Fatalf("mixed accText delta = %q（want ' world'）", got)
	}
}

func TestCleanReasoning(t *testing.T) {
	drop := []string{
		"reasoning_started", "reasoning_ended", "reasoning_complete",
		"reasoning_completed", "hide_all", "show_all", "finished reasoning",
		"reasoning",
		" reasoning_started ",     // trim 后匹配
		"reasoning-ended",         // - → _
		"reasoning_endedhide_all", // 复合形态（去空格）
		"reasoning_startedshow_all",
		"reasoning_completedhide_all",
		"reasoning_delta",      // reasoning_* 前缀 ≤2 词规则
		"reasoning_delta here", // 前缀 + 2 词
	}
	for _, v := range drop {
		if got := cleanReasoning(v); got != "" {
			t.Fatalf("cleanReasoning(%q) = %q（want 空）", v, got)
		}
	}
	keep := map[string]string{
		"Let me think about this": "Let me think about this",
		"reasoning over 3 words":  "reasoning over 3 words", // 前缀但 4 词 → 保留
		"hide_all_but_visible":    "hide_all_but_visible",   // 非整值匹配
	}
	for in, want := range keep {
		if got := cleanReasoning(in); got != want {
			t.Fatalf("cleanReasoning(%q) = %q（want %q）", in, got, want)
		}
	}
	if got := cleanReasoning("   "); got != "" {
		t.Fatalf("blank = %q", got)
	}
}

func TestHistoryEchoPredicate(t *testing.T) {
	mk := func(id, status, mtype, channel string) *messageWire {
		var m messageWire
		m.ID = id
		m.Status = status
		m.Metadata.MessageType = mtype
		m.Channel = channel
		return &m
	}
	cases := []struct {
		name   string
		m      *messageWire
		liveID string
		want   bool
	}{
		{"same id never history", mk("m1", statusFinishedSuccessfully, "", ""), "m1", false},
		{"finished + no markers", mk("h1", statusFinishedSuccessfully, "", ""), "", true},
		{"finished_successfully + no markers", mk("h1", statusFinishedSuccessfully, "continue", ""), "", true},
		{"message_type next → live", mk("h1", statusFinishedSuccessfully, messageTypeNext, ""), "", false},
		{"channel final → live", mk("h1", statusFinishedSuccessfully, "", channelFinal), "", false},
		{"in_progress never history", mk("h1", statusInProgress, "", ""), "", false},
		{"finished (老值)", mk("h1", statusFinished, "", ""), "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isHistoryEcho(c.m, c.liveID); got != c.want {
				t.Fatalf("isHistoryEcho = %v（want %v）", got, c.want)
			}
		})
	}
}
