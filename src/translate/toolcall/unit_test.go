package toolcall

// 边界与负例单测：merge 7 步逐条、三层修复的极端输入、canonical 编码的
// 对拍关键性质（键序 / 转义 / 数字形态）、解包、模式选择、收尾顺序。

import (
	"reflect"
	"testing"
)

func TestMergeToolInputSteps(t *testing.T) {
	rows := []struct {
		name  string
		buf   string
		frame string
		want  string
		step  MergeStep
	}{
		{"1 空帧", "x", "", "x", MergeStepEmptyFrame},
		{"2 首帧", "", "a", "a", MergeStepFirst},
		{"3 完全重复", "a", "a", "a", MergeStepDuplicate},
		{"4 累积快照", `{"a":`, `{"a":1}`, `{"a":1}`, MergeStepSnapshot},
		{"4 半截快照", `{"a":1`, `{"a":1,"b":2}`, `{"a":1,"b":2}`, MergeStepSnapshot},
		{"5 迟到旧短快照", `{"a":1,"b":2}`, `{"a":1`, `{"a":1,"b":2}`, MergeStepLateShort},
		{"5 前置条件：缓冲未完成不吞 {", `{"outer":`, `{`, `{"outer":{`, MergeStepAppend},
		{"6 非前缀重写", `{"a":1}`, `{"b":2}`, `{"b":2}`, MergeStepRewrite},
		{"6 完整对象被更短完整对象重写", `{"a":1,"b":2}`, `{"a":1}`, `{"a":1}`, MergeStepRewrite},
		{"7 增量碎片", `{"a":`, `1}`, `{"a":1}`, MergeStepAppend},
		{"7 更长但非前缀", `{"a":1}`, `{"a":1,"b":2`, `{"a":1}{"a":1,"b":2`, MergeStepAppend},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			got, step := MergeToolInputTraced(r.buf, r.frame)
			if got != r.want || step != r.step {
				t.Errorf("got (%q, step %d) want (%q, step %d)", got, step, r.want, r.step)
			}
			if MergeToolInput(r.buf, r.frame) != r.want {
				t.Errorf("MergeToolInput 不一致")
			}
		})
	}
}

func TestRepairCharLevelEdges(t *testing.T) {
	rows := []struct{ name, in, want string }{
		{"非法转义 \\q", `{"a":"x\q"}`, `{"a":"x\\q"}`},
		{"行尾孤立反斜杠", `{"a":"x\`, `{"a":"x\\`},
		{"半截 \\uXXXX", `{"a":"\u12"}`, `{"a":"\\u12"}`},
		{"孤立高代理", `"\uD83D"`, `"\\uD83D"`},
		{"孤立低代理", `"\uDE00"`, `"\\uDE00"`},
		{"合法代理对保留", `"\uD83D\uDE00"`, `"\uD83D\uDE00"`},
		{"合法转义保留", `{"a":"x\"y\\z\/"}`, `{"a":"x\"y\\z\/"}`},
		{"裸控制符", "{\"a\":\"x\x01y\"}", `{"a":"x\u0001y"}`},
		{"字符串外控制符不动", "{\n\"a\":1}", "{\n\"a\":1}"},
		{"反斜杠+控制符", "{\"a\":\"x\\\ny\"}", `{"a":"x\\\ny"}`},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			if got := RepairJSONCharLevel(r.in); got != r.want {
				t.Errorf("got %q want %q", got, r.want)
			}
		})
	}
}

func TestRepairStructureEdges(t *testing.T) {
	rows := []struct{ name, in, want string }{
		{"悬空反斜杠补字面", `{"a":"x\`, `{"a":"x\\"}`},
		{"括号栈逆序", `{"a":[1,2`, `{"a":[1,2]}`},
		{"嵌套逆序", `{"a":[{"b":1`, `{"a":[{"b":1}]}`},
		{"完整不动", `{"a":1}`, `{"a":1}`},
		{"字符串内括号不计数", `{"a":"}}}"}`, `{"a":"}}}"}`},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			if got := RepairJSONStructure(r.in); got != r.want {
				t.Errorf("got %q want %q", got, r.want)
			}
		})
	}
}

func TestRepairToolJSONLayers(t *testing.T) {
	rows := []struct {
		name      string
		in        string
		want      string
		wantLayer RepairLayer
		wantOK    bool
	}{
		{"字符层", `{"a":"x\q"}`, `{"a":"x\\q"}`, LayerChar, true},
		{"结构层", `{"a":[1`, `{"a":[1]}`, LayerStructure, true},
		{"粘连层", `}{"a":1}`, `{"a":1}`, LayerGlued, true},
		{"全失败", `{"a":1}{"b":2}`, "", LayerNone, false},
		{"空白", "  ", "", LayerNone, false},
		{"键后无值不可修", `{"a":`, "", LayerNone, false},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			got, layer, ok := RepairToolJSON(r.in)
			if ok != r.wantOK || layer != r.wantLayer || (ok && got != r.want) {
				t.Errorf("got (%q, %q, %v) want (%q, %q, %v)", got, layer, ok, r.want, r.wantLayer, r.wantOK)
			}
		})
	}
}

// canonical 编码的对拍关键性质：键序保留（Python dict 插入序）、转义集、
// 数字形态——任何一处偏差都会让 Go 与 check_cases.py 的 emitted 逐字节不一致。
func TestCanonicalJSON(t *testing.T) {
	rows := []struct{ name, in, want string }{
		{"键序保留", `{"b":1,"a":2,"c":3}`, `{"b":1,"a":2,"c":3}`},
		{"重复键就地覆盖", `{"a":1,"b":2,"a":3}`, `{"a":3,"b":2}`},
		{"空白剥离", ` { "a" : 1 } `, `{"a":1}`},
		{"\\u 解码后原样", `"a\u0041b"`, `"aAb"`},
		{"控制符转义", `"\u0007"`, `"\u0007"`},
		{"短转义形式", `"\b\f\n\r\t"`, `"\b\f\n\r\t"`},
		{"整值浮点补 .0", `[1,1.0,1e3,0.5]`, `[1,1.0,1000.0,0.5]`},
		{"非 ASCII 原样", `{"a":"日"}`, `{"a":"日"}`},
		{"数组嵌套", `[[],{}]`, `[[],{}]`},
		{"布尔与 null", `[true,false,null]`, `[true,false,null]`},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			got, ok := CanonicalJSON(r.in)
			if !ok || got != r.want {
				t.Errorf("got (%q, %v) want (%q, true)", got, ok, r.want)
			}
		})
	}
	for _, bad := range []string{"", "{", `{"a":1,}`, `{"a":1}{"b":2}`, "NaN"} {
		if _, ok := CanonicalJSON(bad); ok {
			t.Errorf("CanonicalJSON(%q) 应为非法", bad)
		}
	}
}

func TestUnwrapDoubleEncoded(t *testing.T) {
	if got, ok := UnwrapDoubleEncoded(`"{\"a\":1}"`); !ok || got != `{"a":1}` {
		t.Errorf("object 内层: got (%q, %v)", got, ok)
	}
	if got, ok := UnwrapDoubleEncoded(`"[1,2]"`); !ok || got != `[1,2]` {
		t.Errorf("array 内层: got (%q, %v)", got, ok)
	}
	if _, ok := UnwrapDoubleEncoded(`"\"hi\""`); ok {
		t.Errorf("标量内层不得解包")
	}
	if _, ok := UnwrapDoubleEncoded(`"123"`); ok {
		t.Errorf("数字内层不得解包")
	}
	if _, ok := UnwrapDoubleEncoded(`{"a":1}`); ok {
		t.Errorf("非字符串顶层不得解包")
	}
	if _, ok := UnwrapDoubleEncoded(`"{\"a\":1"`); ok {
		t.Errorf("内层非法不得解包")
	}
}

func TestChooseMode(t *testing.T) {
	if got := ChooseMode(FrameSemantics{Structured: true}, ConsumerAnthropic); got != ModeC {
		t.Errorf("Structured → %v want C", got)
	}
	if got := ChooseMode(FrameSemantics{PrefixMonotonic: true}, ConsumerAnthropic); got != ModeA {
		t.Errorf("PrefixMonotonic → %v want A", got)
	}
	if got := ChooseMode(FrameSemantics{}, ConsumerAnthropic); got != ModeB {
		t.Errorf("保守默认 → %v want B", got)
	}
}

func TestClassifyDefect(t *testing.T) {
	rows := []struct{ name, in, kind, sub string }{
		{"键后无值截断", `{"command":"ls","timeout":`, "truncated", ""},
		{"未闭合字符串", `{"a":"x`, "truncated", ""},
		{"双对象粘连", `{"a":1}{"b":2}`, "malformed", "trailing_chars"},
		{"裸控制符", "{\"a\":\"x\x01\"}", "illegal_chars", ""},
		{"非法转义", `{"a":"x\q"}`, "illegal_chars", ""},
		{"截断+非法", `{"a":"x\`, "truncated_and_illegal", ""},
		{"尾随逗号", `{"a":1,}`, "malformed", "trailing_comma"},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			kind, sub := ClassifyDefect(r.in)
			if kind != r.kind || sub != r.sub {
				t.Errorf("got (%q, %q) want (%q, %q)", kind, sub, r.kind, r.sub)
			}
		})
	}
}

func TestStreamEndFlush(t *testing.T) {
	a := NewAssembler(ModeB, nil, PolicyDefault)
	a.OnFrame(Frame{Kind: FrameToolStart, ToolUseID: "tu_a", Name: "Read"})
	a.OnFrame(Frame{Kind: FrameInputChunk, ToolUseID: "tu_a", Chunk: `{"a":1}`})
	a.OnFrame(Frame{Kind: FrameToolStart, ToolUseID: "tu_b", Name: "Grep"})
	a.OnFrame(Frame{Kind: FrameInputChunk, ToolUseID: "tu_b", Chunk: `{"b":2`})
	evs := a.OnFrame(Frame{Kind: FrameStreamEnd})
	var kinds []EventKind
	for _, ev := range evs {
		kinds = append(kinds, ev.Kind)
	}
	want := []EventKind{EventInputJSONDelta, EventContentBlockStop, EventInputJSONDelta, EventContentBlockStop}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("stream_end 事件序: %v want %v", kinds, want)
	}
	// 残留 flush 也必须走修复：`{"b":2` 结构级补全。
	slots := a.SlotResults()
	if slots[1].Emitted == nil || *slots[1].Emitted != `{"b":2}` {
		t.Errorf("残留槽位修复: %+v", slots[1])
	}
}

func TestStreamEndFlushErrorLast(t *testing.T) {
	a := NewAssembler(ModeB, nil, PolicyDefault)
	a.OnFrame(Frame{Kind: FrameToolStart, ToolUseID: "tu_ok", Name: "Read"})
	a.OnFrame(Frame{Kind: FrameInputChunk, ToolUseID: "tu_ok", Chunk: `{"a":1}`})
	a.OnFrame(Frame{Kind: FrameToolStart, ToolUseID: "tu_bad", Name: "Bash"})
	a.OnFrame(Frame{Kind: FrameInputChunk, ToolUseID: "tu_bad", Chunk: `{"timeout":`})
	evs := a.OnFrame(Frame{Kind: FrameStreamEnd})
	if len(evs) != 3 {
		t.Fatalf("events: %+v", evs)
	}
	// 收尾顺序铁律：先 flush（正常事件）→ 再补 error。
	if evs[0].Kind != EventInputJSONDelta || evs[1].Kind != EventContentBlockStop || evs[2].Kind != EventError {
		t.Fatalf("收尾顺序: %v", []EventKind{evs[0].Kind, evs[1].Kind, evs[2].Kind})
	}
	if evs[2].Failure == nil || evs[2].Failure.Code != CodeInvalidToolInput {
		t.Errorf("error 事件失败态: %+v", evs[2])
	}
}

func TestModeAEmitRawDeltas(t *testing.T) {
	a := NewAssembler(ModeA, nil, PolicyDefault)
	a.OnFrame(Frame{Kind: FrameToolStart, ToolUseID: "tu_a", Name: "Read"})
	evs := a.OnFrame(Frame{Kind: FrameInputChunk, ToolUseID: "tu_a", Chunk: `{"a":`})
	if len(evs) != 1 || evs[0].Kind != EventInputJSONDelta || evs[0].PartialJSON != `{"a":` {
		t.Fatalf("ModeA 应逐片转发原始 chunk: %+v", evs)
	}
	evs = a.OnFrame(Frame{Kind: FrameInputChunk, ToolUseID: "tu_a", Chunk: `1}`})
	if len(evs) != 1 || evs[0].PartialJSON != `1}` {
		t.Fatalf("ModeA 第二片: %+v", evs)
	}
}

func TestOutletArguments(t *testing.T) {
	fail := FinalizeResult{Failure: &Failure{Code: CodeInvalidToolInput}}
	if out, send := OutletArguments(fail, ConsumerOpenAI); send || out != "" {
		t.Errorf("失败态不得出口: (%q, %v)", out, send)
	}
	empty := FinalizeResult{}
	if out, send := OutletArguments(empty, ConsumerAnthropic); send || out != "" {
		t.Errorf("Anthropic 空参: (%q, %v)", out, send)
	}
	if out, send := OutletArguments(empty, ConsumerOpenAI); !send || out != "{}" {
		t.Errorf("OpenAI 空参: (%q, %v)", out, send)
	}
	if out, send := OutletArguments(empty, ConsumerOpenAIResponses); !send || out != "{}" {
		t.Errorf("Responses 空参: (%q, %v)", out, send)
	}
	full := FinalizeResult{Emitted: new(`{"x":1}`)}
	if out, send := OutletArguments(full, ConsumerOpenAI); !send || out != `{"x":1}` {
		t.Errorf("有参原样出口: (%q, %v)", out, send)
	}
}

func TestFinalizeBugCGateDetails(t *testing.T) {
	// 显式 null 算存在（只判键存在性，不判值类型）。
	if res := Finalize(`{"path":null,"text":"x"}`, []string{"path", "text"}, PolicyDefault); res.Failure != nil {
		t.Errorf("null 算存在: %+v", res.Failure)
	}
	// 合法 JSON 缺键：即使其它键值类型离谱也只判存在性。
	if res := Finalize(`{"path":1,"text":[2]}`, []string{"path", "text"}, PolicyDefault); res.Failure != nil {
		t.Errorf("类型不参与判定: %+v", res.Failure)
	}
	// 修复前的串不参与 Bug C（非法 JSON → 走修复路径，修复成功即通过）。
	if res := Finalize(`{"path":"/x","text":"y"`, []string{"path", "text"}, PolicyDefault); res.Failure != nil || res.Emitted == nil {
		t.Errorf("非法 JSON 缺键判定应交给修复路径: %+v", res)
	}
	// 空串当 {} 判（required 非空 → 失败）。
	if res := Finalize("", []string{"command"}, PolicyDefault); res.Failure == nil || res.Failure.Code != CodeInvalidToolInput {
		t.Errorf("空串当 {}: %+v", res)
	}
	// 合法 JSON 字节保真（不重序列化）。
	raw := `{"b" : 1, "a": 2}`
	if res := Finalize(raw, nil, PolicyDefault); res.Emitted == nil || *res.Emitted != raw {
		t.Errorf("合法 JSON 必须字节保真: %+v", res.Emitted)
	}
}
