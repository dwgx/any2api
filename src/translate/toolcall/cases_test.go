package toolcall

// 规格 §8.2 的 11 组回归用例 → Go 表驱动测试。
// 用例数据来自 research/topics/toolcall-assembly-spec/assets/cases/*.json（只读转录）；
// 断言口径与 tmp/lanes/P66-toolcall-spec/check_cases.py（oracle）逐条一致：
// merged_buffer / emitted / failure / client_parses_to，另加本实现的诊断断言。

import (
	"reflect"
	"testing"
)

type frameSpec struct {
	kind  FrameKind
	id    string
	name  string
	chunk string
}

type caseSpec struct {
	name            string
	tools           []ToolDef
	frames          []frameSpec
	policy          Policy
	wantBuf         string
	wantEmitted     *string
	wantFailureCode string
	wantMissing     []string
	wantMessage     string
	wantLayer       RepairLayer // "" = 不检查
	wantDefectKind  string
	wantSubkind     string
	wantMergeSteps  []MergeStep
	skipParseCheck  bool // passthrough 最坏路径：emitted 本就是坏 JSON
}

func (c caseSpec) run(t *testing.T) {
	t.Helper()
	a := NewAssembler(ModeB, c.tools, c.policy)
	for _, f := range c.frames {
		a.OnFrame(Frame{Kind: f.kind, ToolUseID: f.id, Name: f.name, Chunk: f.chunk})
	}
	slots := a.SlotResults()
	if len(slots) != 1 {
		t.Fatalf("slots: got %d want 1", len(slots))
	}
	s := slots[0]
	if s.Buf != c.wantBuf {
		t.Errorf("merged_buffer: got %q want %q", s.Buf, c.wantBuf)
	}
	switch {
	case c.wantEmitted == nil && s.Emitted != nil:
		t.Errorf("emitted: got %q want none", *s.Emitted)
	case c.wantEmitted != nil && s.Emitted == nil:
		t.Errorf("emitted: got none want %q", *c.wantEmitted)
	case c.wantEmitted != nil && *s.Emitted != *c.wantEmitted:
		t.Errorf("emitted: got %q want %q", *s.Emitted, *c.wantEmitted)
	}
	// client_parses_to：emitted 必须可解析，且与期望出口串解析值一致。
	if c.wantEmitted != nil && s.Emitted != nil && !c.skipParseCheck {
		got, ok := CanonicalJSON(*s.Emitted)
		if !ok {
			t.Errorf("client_parses_to: emitted 不可解析 %q", *s.Emitted)
		} else if want, ok := CanonicalJSON(*c.wantEmitted); !ok || got != want {
			t.Errorf("client_parses_to: got %q want %q", got, want)
		}
	}
	// failure（oracle 口径：存在性 + code + missing）。
	if c.wantFailureCode == "" {
		if s.Failure != nil {
			t.Errorf("unexpected failure: %+v", s.Failure)
		}
	} else if s.Failure == nil {
		t.Errorf("expected failure %s, got success", c.wantFailureCode)
	} else {
		if s.Failure.Code != c.wantFailureCode {
			t.Errorf("failure.code: got %q want %q", s.Failure.Code, c.wantFailureCode)
		}
		if len(c.wantMissing) > 0 && !reflect.DeepEqual(s.Failure.Missing, c.wantMissing) {
			t.Errorf("failure.missing: got %v want %v", s.Failure.Missing, c.wantMissing)
		}
		if c.wantMessage != "" && s.Failure.Message != c.wantMessage {
			t.Errorf("failure.message: got %q want %q", s.Failure.Message, c.wantMessage)
		}
	}
	wantState := SlotReady
	if c.wantFailureCode != "" {
		wantState = SlotInvalid
	}
	if s.State != wantState {
		t.Errorf("state: got %v want %v", s.State, wantState)
	}
	// 诊断（纯观测，规格 §7 / 用例 diagnosis 字段）。
	if c.wantLayer != "" && (s.Defect == nil || s.Defect.RepairLayer != string(c.wantLayer)) {
		t.Errorf("defect.repair_layer: got %+v want %q", s.Defect, c.wantLayer)
	}
	if c.wantDefectKind != "" && (s.Defect == nil || s.Defect.Kind != c.wantDefectKind) {
		t.Errorf("defect.kind: got %+v want %q", s.Defect, c.wantDefectKind)
	}
	if c.wantSubkind != "" && (s.Defect == nil || s.Defect.Subkind != c.wantSubkind) {
		t.Errorf("defect.subkind: got %+v want %q", s.Defect, c.wantSubkind)
	}
	if len(c.wantMergeSteps) > 0 && !reflect.DeepEqual(s.MergeSteps, c.wantMergeSteps) {
		t.Errorf("merge steps: got %v want %v", s.MergeSteps, c.wantMergeSteps)
	}
}

func TestSpecCases(t *testing.T) {
	cases := []caseSpec{
		// 01 windows-path-escape：\U / \d 非法转义 → 字符级再转义。
		{
			name:  "01-windows-path-escape",
			tools: []ToolDef{{Name: "Bash"}},
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_01", name: "Bash"},
				{kind: FrameInputChunk, id: "tu_01", chunk: `{"command":"dir C:\Users\dwgx1"}`},
				{kind: FrameToolStop, id: "tu_01"},
			},
			wantBuf:        `{"command":"dir C:\Users\dwgx1"}`,
			wantEmitted:    new(`{"command":"dir C:\\Users\\dwgx1"}`),
			wantLayer:      LayerChar,
			wantDefectKind: "illegal_chars",
			wantMergeSteps: []MergeStep{MergeStepFirst},
		},
		// 02 bare-control-chars：字符串内裸 TAB / LF → \t / \n。
		{
			name:  "02-bare-control-chars",
			tools: []ToolDef{{Name: "Bash"}},
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_02", name: "Bash"},
				{kind: FrameInputChunk, id: "tu_02", chunk: "{\"command\":\"echo a\techo b\necho c\"}"},
				{kind: FrameToolStop, id: "tu_02"},
			},
			wantBuf:        "{\"command\":\"echo a\techo b\necho c\"}",
			wantEmitted:    new(`{"command":"echo a\techo b\necho c"}`),
			wantLayer:      LayerChar,
			wantDefectKind: "illegal_chars",
			wantMergeSteps: []MergeStep{MergeStepFirst},
		},
		// 03 truncated-unclosed：结构级补收尾（先 " 再 }，括号栈逆序）。
		{
			name:  "03-truncated-unclosed",
			tools: []ToolDef{{Name: "Write"}},
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_03", name: "Write"},
				{kind: FrameInputChunk, id: "tu_03", chunk: `{"file_path":"/tmp/a.txt","content":"hello world`},
				{kind: FrameToolStop, id: "tu_03"},
			},
			wantBuf:        `{"file_path":"/tmp/a.txt","content":"hello world`,
			wantEmitted:    new(`{"file_path":"/tmp/a.txt","content":"hello world"}`),
			wantLayer:      LayerStructure,
			wantDefectKind: "truncated",
			wantMergeSteps: []MergeStep{MergeStepFirst},
		},
		// 04 double-encoded：parse 成功得 String → 解包一层（object/array 才用）。
		{
			name: "04-double-encoded",
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_04", name: "Write"},
				{kind: FrameInputChunk, id: "tu_04", chunk: `"{\"file_path\":\"/tmp/a.txt\",\"content\":\"hi\"}"`},
				{kind: FrameToolStop, id: "tu_04"},
			},
			wantBuf:     `"{\"file_path\":\"/tmp/a.txt\",\"content\":\"hi\"}"`,
			wantEmitted: new(`{"file_path":"/tmp/a.txt","content":"hi"}`),
		},
		// 04 变体：内层是标量（不猜）→ 原样下发。
		{
			name: "04-double-encoded/variant-scalar-inner",
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_04b", name: "Write"},
				{kind: FrameInputChunk, id: "tu_04b", chunk: `"\"hello\""`},
				{kind: FrameToolStop, id: "tu_04b"},
			},
			wantBuf:     `"\"hello\""`,
			wantEmitted: new(`"\"hello\""`),
		},
		// 05 glued-objects：merge 第 6 步只留最新完整对象（无 }{ 粘连）。
		{
			name: "05-glued-objects",
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_05", name: "Bash"},
				{kind: FrameInputChunk, id: "tu_05", chunk: `{"command":"ls -la"}`},
				{kind: FrameInputChunk, id: "tu_05", chunk: `{"command":"ls"}`},
				{kind: FrameToolStop, id: "tu_05"},
			},
			wantBuf:        `{"command":"ls"}`,
			wantEmitted:    new(`{"command":"ls"}`),
			wantMergeSteps: []MergeStep{MergeStepFirst, MergeStepRewrite},
		},
		// 05 变体 A：串首多余 }（修复层第 3 层）。
		{
			name: "05-glued-objects/variant-leading-brace",
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_05b", name: "Write"},
				{kind: FrameInputChunk, id: "tu_05b", chunk: `}{"file_path":"/tmp/a.txt"}`},
				{kind: FrameToolStop, id: "tu_05b"},
			},
			wantBuf:        `}{"file_path":"/tmp/a.txt"}`,
			wantEmitted:    new(`{"file_path":"/tmp/a.txt"}`),
			wantLayer:      LayerGlued,
			wantDefectKind: "malformed",
			wantSubkind:    "glued",
		},
		// 05 变体 B：单块内双完整对象（不可修的边界）→ 失败态。
		{
			name: "05-glued-objects/variant-double-object-in-one-block",
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_05c", name: "Bash"},
				{kind: FrameInputChunk, id: "tu_05c", chunk: `{"a":1}{"b":2}`},
				{kind: FrameToolStop, id: "tu_05c"},
			},
			wantBuf:         `{"a":1}{"b":2}`,
			wantEmitted:     nil,
			wantFailureCode: CodeInvalidToolInput,
			wantMessage:     unrepairableMessage,
			wantLayer:       LayerNone,
			wantDefectKind:  "malformed",
			wantSubkind:     "trailing_chars",
		},
		// 07 missing-required：Bug C 闸门（上游形态键、修复前、只判存在性）。
		{
			name:  "07-missing-required",
			tools: []ToolDef{{Name: "Write", UpstreamName: "fs_write", UpstreamRequired: []string{"path", "text"}}},
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_07", name: "fs_write"},
				{kind: FrameInputChunk, id: "tu_07", chunk: `{"path":"/tmp/a.txt"}`},
				{kind: FrameToolStop, id: "tu_07"},
			},
			wantBuf:         `{"path":"/tmp/a.txt"}`,
			wantEmitted:     nil,
			wantFailureCode: CodeInvalidToolInput,
			wantMissing:     []string{"text"},
			wantMessage:     "工具调用缺少必需参数：text（模型侧生成异常），请重试。",
		},
		// 07 变体 A：空 input（等价 {}）对 required 工具 → 全部缺失 → 失败态。
		{
			name:  "07-missing-required/variant-empty-input",
			tools: []ToolDef{{Name: "Bash", UpstreamName: "Bash", UpstreamRequired: []string{"command"}}},
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_07b", name: "Bash"},
				{kind: FrameToolStop, id: "tu_07b"},
			},
			wantBuf:         "",
			wantEmitted:     nil,
			wantFailureCode: CodeInvalidToolInput,
			wantMissing:     []string{"command"},
			wantMessage:     "工具调用缺少必需参数：command（模型侧生成异常），请重试。",
		},
		// 07 变体 B：无 required 的工具（不干预）→ 空参不发 delta、无失败。
		{
			name:  "07-missing-required/variant-no-required",
			tools: []ToolDef{{Name: "TodoRead", UpstreamName: "TodoRead", UpstreamRequired: []string{}}},
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_07c", name: "TodoRead"},
				{kind: FrameToolStop, id: "tu_07c"},
			},
			wantBuf:     "",
			wantEmitted: nil,
		},
		// 08 empty-arguments-brace-discipline（frames 面）：空串永不作为参数下发。
		{
			name: "08-empty-arguments-brace-discipline",
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_08", name: "TodoRead"},
				{kind: FrameInputChunk, id: "tu_08", chunk: ""},
				{kind: FrameToolStop, id: "tu_08"},
			},
			wantBuf:     "",
			wantEmitted: nil,
		},
		// 09 late-short-snapshot：迟到旧短快照丢弃（第 5 步，缓冲完整才触发）。
		{
			name: "09-late-short-snapshot",
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_09", name: "Bash"},
				{kind: FrameInputChunk, id: "tu_09", chunk: `{"command":"ls -la"}`},
				{kind: FrameInputChunk, id: "tu_09", chunk: `{"command":"ls -l`},
				{kind: FrameToolStop, id: "tu_09"},
			},
			wantBuf:        `{"command":"ls -la"}`,
			wantEmitted:    new(`{"command":"ls -la"}`),
			wantMergeSteps: []MergeStep{MergeStepFirst, MergeStepLateShort},
		},
		// 09 变体：缓冲未完成 + 单帧 { → 前置条件不满足 → 第 7 步追加（不吞 {）。
		{
			name: "09-late-short-snapshot/variant-incomplete-buffer",
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_09b", name: "Bash"},
				{kind: FrameInputChunk, id: "tu_09b", chunk: `{"outer":`},
				{kind: FrameInputChunk, id: "tu_09b", chunk: `{`},
				{kind: FrameInputChunk, id: "tu_09b", chunk: `"inner":1}}`},
				{kind: FrameToolStop, id: "tu_09b"},
			},
			wantBuf:        `{"outer":{"inner":1}}`,
			wantEmitted:    new(`{"outer":{"inner":1}}`),
			wantMergeSteps: []MergeStep{MergeStepFirst, MergeStepAppend, MergeStepAppend},
		},
		// 10 truncated-unrecoverable：结构层闭合后仍非法 → 失败态（不猜值）。
		{
			name:  "10-truncated-unrecoverable",
			tools: []ToolDef{{Name: "Bash"}},
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_10", name: "Bash"},
				{kind: FrameInputChunk, id: "tu_10", chunk: `{"command":"ls","timeout":`},
				{kind: FrameToolStop, id: "tu_10"},
			},
			wantBuf:         `{"command":"ls","timeout":`,
			wantEmitted:     nil,
			wantFailureCode: CodeInvalidToolInput,
			wantMessage:     unrepairableMessage,
			wantLayer:       LayerNone,
			wantDefectKind:  "truncated",
		},
		// 10 变体：②③全关（最坏兜底）→ 原样透传坏 JSON，仍绝不静默成 {}。
		{
			name:   "10-truncated-unrecoverable/variant-passthrough-worst-case",
			tools:  []ToolDef{{Name: "Bash"}},
			policy: PolicyPassthroughWorstCase,
			frames: []frameSpec{
				{kind: FrameToolStart, id: "tu_10", name: "Bash"},
				{kind: FrameInputChunk, id: "tu_10", chunk: `{"command":"ls","timeout":`},
				{kind: FrameToolStop, id: "tu_10"},
			},
			wantBuf:        `{"command":"ls","timeout":`,
			wantEmitted:    new(`{"command":"ls","timeout":`),
			wantDefectKind: "truncated",
			skipParseCheck: true,
		},
	}
	if len(cases) != 16 {
		t.Fatalf("用例表长度 %d（11 用例 + 5 变体）", len(cases))
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.run(t) })
	}
}

// 08 的出口形态面：Anthropic 靠 start 的 {}；OpenAI 写 "{}"；空串禁用。
func TestCase08OutletDiscipline(t *testing.T) {
	a := NewAssembler(ModeB, nil, PolicyDefault)
	var evs []Event
	evs = append(evs, a.OnFrame(Frame{Kind: FrameToolStart, ToolUseID: "tu_08", Name: "TodoRead"})...)
	evs = append(evs, a.OnFrame(Frame{Kind: FrameInputChunk, ToolUseID: "tu_08", Chunk: ""})...)
	evs = append(evs, a.OnFrame(Frame{Kind: FrameToolStop, ToolUseID: "tu_08"})...)
	if len(evs) != 2 || evs[0].Kind != EventContentBlockStart || evs[0].Input != "{}" {
		t.Fatalf("events: %+v（want start{} + stop）", evs)
	}
	if evs[1].Kind != EventContentBlockStop {
		t.Fatalf("events[1]: got %v want content_block_stop", evs[1].Kind)
	}
	for _, ev := range evs {
		if ev.Kind == EventInputJSONDelta {
			t.Errorf("空参不得额外发 input_json_delta")
		}
	}
	slots := a.SlotResults()
	res := FinalizeResult{Emitted: slots[0].Emitted, Failure: slots[0].Failure}
	if out, send := OutletArguments(res, ConsumerAnthropic); send || out != "" {
		t.Errorf("Anthropic 空参出口: got (%q, send=%v) want 不发 delta", out, send)
	}
	if out, send := OutletArguments(res, ConsumerOpenAI); !send || out != "{}" {
		t.Errorf("OpenAI 空参出口: got (%q, send=%v) want (\"{}\", true)", out, send)
	}
}

// 06 孤儿 toolResult：剥头部孤立轮次 + 占位 user/Understood. 应答，保交替。
func TestCase06OrphanToolResult(t *testing.T) {
	const placeholder = "[Earlier conversation history was truncated to fit the model's input limit. Older messages and tool activity have been omitted.]"
	got := NormalizeHistoryTail([]HistoryMessage{
		{Role: "user", ToolResults: []HistoryToolResult{{
			ToolUseID: "tu_dropped",
			Content:   []HistoryResultContent{{Type: "text", Text: "old file contents"}},
		}}},
		{Role: "assistant", Text: "Got it."},
		{Role: "user", Text: "now run the tests"},
	}, nil, placeholder)
	want := []struct{ role, text string }{
		{"user", placeholder},
		{"assistant", "Understood."},
		{"user", "now run the tests"},
	}
	if len(got) != len(want) {
		t.Fatalf("history len: got %d want %d（%+v）", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i].Role != w.role || got[i].Text != w.text {
			t.Errorf("msg %d: got (%q,%q) want (%q,%q)", i, got[i].Role, got[i].Text, w.role, w.text)
		}
	}
	// oracle 同款结构判定：user 开头 + 严格交替。
	roles := make([]string, len(got))
	for i, m := range got {
		roles[i] = m.Role
	}
	if len(roles) == 0 || roles[0] != "user" {
		t.Errorf("历史必须以 user 开头: %v", roles)
	}
	for i := 1; i < len(roles); i++ {
		if roles[i] == roles[i-1] {
			t.Errorf("非严格交替: %v", roles)
		}
	}
	for _, m := range got {
		for _, tr := range m.ToolResults {
			if tr.ToolUseID == "tu_dropped" {
				t.Errorf("孤儿 toolResult 未被剥掉")
			}
		}
	}
}

// 06 变体：sanitize 扁平化（非活跃轮次清空、toolResults 并入正文、活跃保持结构化）。
func TestCase06VariantSanitizeFlattening(t *testing.T) {
	got := SanitizeHistory([]HistoryMessage{
		{Role: "assistant", Text: "calling", ToolUses: []HistoryToolUse{{ToolUseID: "tu_old", Name: "read_file"}}},
		{Role: "user", Text: "", ToolResults: []HistoryToolResult{{
			ToolUseID: "tu_old",
			Content:   []HistoryResultContent{{Type: "text", Text: "x"}},
		}}},
		{Role: "assistant", Text: "calling", ToolUses: []HistoryToolUse{{ToolUseID: "tu_now", Name: "Bash"}}},
	}, []string{"tu_now"})
	if len(got) != 3 {
		t.Fatalf("len: got %d want 3", len(got))
	}
	if len(got[0].ToolUses) != 0 || got[0].Text != "calling" {
		t.Errorf("非活跃轮次 toolUses 应清空、正文保留: %+v", got[0])
	}
	if got[1].Text != "[read_file] x" || len(got[1].ToolResults) != 0 {
		t.Errorf("非活跃 toolResults 应并入正文 [read_file] x: %+v", got[1])
	}
	if len(got[2].ToolUses) != 1 || got[2].ToolUses[0].ToolUseID != "tu_now" {
		t.Errorf("活跃轮次应保持结构化: %+v", got[2])
	}
}

// 06 变体（W5-29 统一政策）：中段孤儿结果**折叠为文本**（保内容、保交替）；
// 空内容 → 显式占位。旧行为「剥空整条删除」会让 assistant/assistant 相邻
// （严格交替破坏——与孤儿 toolResult 同为 400 成因）。
// 用例数据 = assets/cases/06-orphan-tool-result.json 的「中段孤儿结果折叠为文本」变体。
func TestCase06VariantMidOrphanFold(t *testing.T) {
	const placeholder = "[Earlier conversation history was truncated to fit the model's input limit. Older messages and tool activity have been omitted.]"
	got := NormalizeHistoryTail([]HistoryMessage{
		{Role: "user", ToolResults: []HistoryToolResult{{
			ToolUseID: "tu_dropped",
			Content:   []HistoryResultContent{{Type: "text", Text: "old file contents"}},
		}}},
		{Role: "assistant", Text: "Got it."},
		{Role: "user", Text: "check the earlier output"},
		{Role: "assistant", Text: "running"},
		{Role: "user", ToolResults: []HistoryToolResult{{
			ToolUseID: "tu_mid",
			Content:   []HistoryResultContent{{Type: "text", Text: "stale tool output"}},
		}}},
		{Role: "assistant", Text: "ok"},
		{Role: "user", ToolResults: []HistoryToolResult{{ToolUseID: "tu_mid2", Content: nil}}},
		{Role: "assistant", Text: "continue"},
		{Role: "user", Text: "done?"},
	}, nil, placeholder)
	want := []struct{ role, text string }{
		{"user", placeholder},
		{"assistant", "Understood."},
		{"user", "check the earlier output"},
		{"assistant", "running"},
		{"user", "stale tool output"}, // 中段孤儿 → 折叠为文本（内容原样）
		{"assistant", "ok"},
		{"user", "Tool result was empty."}, // 空内容 → 显式占位
		{"assistant", "continue"},
		{"user", "done?"},
	}
	if len(got) != len(want) {
		t.Fatalf("history len: got %d want %d（%+v）", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i].Role != w.role || got[i].Text != w.text {
			t.Errorf("msg %d: got (%q,%q) want (%q,%q)", i, got[i].Role, got[i].Text, w.role, w.text)
		}
	}
	roles := make([]string, len(got))
	for i, m := range got {
		roles[i] = m.Role
	}
	if roles[0] != "user" {
		t.Errorf("历史必须以 user 开头: %v", roles)
	}
	for i := 1; i < len(roles); i++ {
		if roles[i] == roles[i-1] {
			t.Errorf("非严格交替（旧剥除行为会在此破坏）: %v", roles)
		}
	}
	for _, m := range got {
		for _, tr := range m.ToolResults {
			if tr.ToolUseID == "tu_mid" || tr.ToolUseID == "tu_mid2" {
				t.Errorf("中段孤儿 toolResult 仍以结构化出口（应折叠为文本）: %q", tr.ToolUseID)
			}
		}
	}
}

// 06 变体补充：同一条消息内「命中保留 + 孤儿折叠」分流（known 命中者保持
// 结构化，孤儿内容并入正文——统一政策在单条消息内的两条路径）。
func TestCase06MidOrphanMixedWithKnown(t *testing.T) {
	got := NormalizeHistoryTail([]HistoryMessage{
		{Role: "user", Text: "anchor"},
		{Role: "assistant", Text: "ok"},
		{Role: "user", Text: "run", ToolResults: []HistoryToolResult{
			{ToolUseID: "tu_ok", Content: []HistoryResultContent{{Type: "text", Text: "fresh output"}}},
			{ToolUseID: "tu_orphan", Content: []HistoryResultContent{{Type: "text", Text: "stale output"}}},
		}},
	}, []string{"tu_ok"}, "")
	if len(got) != 3 {
		t.Fatalf("len: got %d want 3（%+v）", len(got), got)
	}
	if got[2].Text != "run\nstale output" {
		t.Errorf("孤儿内容应并入正文（\\n 连接）: %q", got[2].Text)
	}
	if len(got[2].ToolResults) != 1 || got[2].ToolResults[0].ToolUseID != "tu_ok" {
		t.Errorf("命中的 toolResult 应保持结构化: %+v", got[2].ToolResults)
	}
}

// 11 index 归一：text / thinking / hosted 不占号；稠密无空洞。
func TestCase11IndexNormalization(t *testing.T) {
	got := NormalizeToolIndexes([]UpstreamBlock{
		{ContentBlockIndex: 0, Type: "text"},
		{ContentBlockIndex: 1, Type: "tool_use", ToolUseID: "tu_a", Name: "Read"},
		{ContentBlockIndex: 2, Type: "thinking"},
		{ContentBlockIndex: 3, Type: "tool_use", ToolUseID: "tu_b", Name: "Grep"},
		{ContentBlockIndex: 4, Type: "server_tool_use", ToolUseID: "srv_1", Name: "web_search"},
	})
	want := []ToolIndexEntry{{Index: 0, ToolUseID: "tu_a"}, {Index: 1, ToolUseID: "tu_b"}}
	if !reflect.DeepEqual(got.Calls, want) {
		t.Fatalf("downstream_tool_calls: got %+v want %+v", got.Calls, want)
	}
	// 稠密无空洞。
	for i, e := range got.Calls {
		if e.Index != i {
			t.Errorf("index 不稠密: %+v", got.Calls)
		}
	}
	// 映射：仅 tool_use 块有映射；text / thinking / server_tool_use 不占号。
	if got.ByContentBlockIndex[1] != 0 || got.ByContentBlockIndex[3] != 1 {
		t.Errorf("映射错位: %v", got.ByContentBlockIndex)
	}
	for _, b := range []int{0, 2, 4} {
		if _, ok := got.ByContentBlockIndex[b]; ok {
			t.Errorf("块 %d（text/thinking/hosted）不应占 tool index", b)
		}
	}
}
