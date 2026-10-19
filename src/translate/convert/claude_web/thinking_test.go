package claude_web

// W10-3 实抓 golden：**thinking 帧形态**（`thinking_summary_delta`）与工具块交错
// （2 条完整流，脱敏副本在 testdata/；`tmp/lanes/W10-thinking/sample/` 原件）。
//
// 实抓结论（W10-3，本测试逐条钉死）：
//   - 思考块 = `content_block_start{type:"thinking",truncated:true}` +
//     `content_block_delta{delta.type:"thinking_summary_delta",summary.summary}`×N
//     + `content_block_stop{summary_pending}`——**零 `thinking_delta`、零
//     `signature_delta`**（与 W9-1 主题表（源码级）不同：实抓为准）；
//   - 摘要增量是**逐条摘要**（非全量思考文本），映射 = IR 思考块文本增量；
//   - 块与正文/工具块**交错**（runA：thinking→text→tool_use→tool_result→…）；
//   - 上游内置工具的调用参数走 `input_json_delta`（runA 235 片非空拼成完整
//     JSON），显示面更新走 `tool_use_block_update_delta`（未建模 → 忽略）。
//
// 反证锚点：把 delta 分派里的 `deltaThinkingSummary` 分支删掉 → 本文件的
// thinking 文本断言红（思考面整段丢失）。

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

// thinkingFixture 一条 thinking golden 流的期望面。
type thinkingFixture struct {
	name       string
	frames     int
	blockKinds []ir.BlockKind // content_block_start 的块类型序（映射到 IR 后）
	summaries  int            // thinking_summary_delta 帧数
	textDeltas int            // text_delta 帧数（非空）
	argDeltas  int            // input_json_delta 非空帧数
	blockCount int            // content_block_start 帧数
}

var thinkingFixtures = []thinkingFixture{
	{
		name:   "runA_45534.540.frames.jsonl.txt",
		frames: 738,
		blockKinds: []ir.BlockKind{
			ir.BlockThinking, ir.BlockText, ir.BlockServerToolUse, ir.BlockToolResult,
			ir.BlockThinking, ir.BlockServerToolUse, ir.BlockToolResult,
			ir.BlockThinking, ir.BlockText,
		},
		summaries:  5,
		textDeltas: 458,
		argDeltas:  235,
		blockCount: 9,
	},
	{
		name:       "runB_55480.537.frames.jsonl.txt",
		frames:     573,
		blockKinds: []ir.BlockKind{ir.BlockThinking, ir.BlockText},
		summaries:  6,
		textDeltas: 548,
		argDeltas:  0,
		blockCount: 2,
	},
}

// TestThinkingGoldenFixtures W10-3 实抓流逐帧：块序 / 思考摘要文本 / 正文 /
// 工具调用参数（独立从 fixture 派生期望，再与解析结果对拍）。
func TestThinkingGoldenFixtures(t *testing.T) {
	for _, fx := range thinkingFixtures {
		t.Run(fx.name, func(t *testing.T) {
			frames := loadFixture(t, fx.name)
			if len(frames) != fx.frames {
				t.Fatalf("frames = %d, want %d", len(frames), fx.frames)
			}

			// ① 实抓形态核验：零 thinking_delta / 零 signature_delta（W10-3 结论
			// 在 fixture 上成立——否则本测试测的不是实抓形态）。
			var rawDeltaTypes []string
			// ② 期望值独立派生（不复用解析器逻辑）。
			var (
				wantThinking strings.Builder
				wantText     strings.Builder
				wantArgs     strings.Builder
				wantBlockSeq []ir.BlockKind
				wantToolIDs  []string
				wantToolName []string
				summaries    int
				textDeltas   int
				argDeltas    int
				blockStarts  int
			)
			for _, f := range frames {
				switch f.Event {
				case "content_block_start":
					var j struct {
						Index        int `json:"index"`
						ContentBlock struct {
							Type string `json:"type"`
							ID   string `json:"id"`
							Name string `json:"name"`
						} `json:"content_block"`
					}
					if err := json.Unmarshal([]byte(f.Data), &j); err != nil {
						t.Fatalf("block_start: %v", err)
					}
					blockStarts++
					wantBlockSeq = append(wantBlockSeq, blockKindOf(j.ContentBlock.Type))
					if j.ContentBlock.Type == "tool_use" {
						wantToolIDs = append(wantToolIDs, j.ContentBlock.ID)
						wantToolName = append(wantToolName, j.ContentBlock.Name)
					}
				case "content_block_delta":
					var j struct {
						Delta struct {
							Type  string `json:"type"`
							Text  string `json:"text"`
							PJSON string `json:"partial_json"`
							Summ  struct {
								Summary string `json:"summary"`
							} `json:"summary"`
						} `json:"delta"`
					}
					if err := json.Unmarshal([]byte(f.Data), &j); err != nil {
						t.Fatalf("block_delta: %v", err)
					}
					rawDeltaTypes = append(rawDeltaTypes, j.Delta.Type)
					switch j.Delta.Type {
					case "thinking_summary_delta":
						summaries++
						wantThinking.WriteString(j.Delta.Summ.Summary)
					case "text_delta":
						if j.Delta.Text != "" {
							textDeltas++
							wantText.WriteString(j.Delta.Text)
						}
					case "input_json_delta":
						if j.Delta.PJSON != "" {
							argDeltas++
							wantArgs.WriteString(j.Delta.PJSON)
						}
					}
				}
			}
			for _, dt := range rawDeltaTypes {
				if dt == "thinking_delta" || dt == "signature_delta" {
					t.Fatalf("fixture 含 %s——W10-3 实抓结论是「零 thinking_delta/零 signature_delta」", dt)
				}
			}
			if blockStarts != fx.blockCount || summaries != fx.summaries || textDeltas != fx.textDeltas || argDeltas != fx.argDeltas {
				t.Fatalf("fixture 面变化：blocks=%d(summaries=%d,texts=%d,args=%d), want %d(%d,%d,%d)",
					blockStarts, summaries, textDeltas, argDeltas, fx.blockCount, fx.summaries, fx.textDeltas, fx.argDeltas)
			}

			// ③ 解析（增量面）→ 与派生期望对拍。
			p := NewStreamParser()
			var events []ir.StreamEvent
			for i, f := range frames {
				evs, err := p.Feed(f.Event, []byte(f.Data))
				if err != nil {
					t.Fatalf("frame %d (%s): %v", i+1, f.Event, err)
				}
				events = append(events, evs...)
			}
			if tail := p.Finish(); len(tail) != 0 {
				t.Fatalf("Finish = %+v, want empty", tail)
			}

			var (
				gotThinking strings.Builder
				gotText     strings.Builder
				gotArgs     strings.Builder
				gotBlockSeq []ir.BlockKind
				gotToolIDs  []string
				gotToolName []string
				started     bool
				stopped     bool
				stopReason  ir.StopReason
				usageSeen   bool
			)
			for _, ev := range events {
				switch ev.Kind {
				case ir.EventStart:
					started = true
				case ir.EventBlockStart:
					gotBlockSeq = append(gotBlockSeq, ev.BlockKind)
					if ev.BlockKind == ir.BlockServerToolUse {
						gotToolIDs = append(gotToolIDs, ev.ToolCallID)
						gotToolName = append(gotToolName, ev.ToolName)
					}
				case ir.EventBlockDelta:
					switch ev.BlockKind {
					case ir.BlockThinking:
						gotThinking.WriteString(ev.TextDelta)
					case ir.BlockText:
						gotText.WriteString(ev.TextDelta)
					case ir.BlockServerToolUse:
						gotArgs.WriteString(ev.ArgsDelta)
					}
				case ir.EventMessageDelta:
					if ev.StopReason != "" {
						stopReason = ev.StopReason
					}
					if ev.Usage != nil {
						usageSeen = true
					}
				case ir.EventMessageStop:
					stopped = true
				}
			}
			if !started || !stopped {
				t.Fatalf("骨架不完整：started=%v stopped=%v", started, stopped)
			}
			if stopReason != ir.StopStop {
				t.Fatalf("stop reason = %q, want stop", stopReason)
			}
			if !usageSeen {
				t.Fatalf("message_limit 未映射出 usage")
			}
			if len(gotBlockSeq) != len(fx.blockKinds) {
				t.Fatalf("块数 = %d, want %d", len(gotBlockSeq), len(fx.blockKinds))
			}
			for i, want := range fx.blockKinds {
				if gotBlockSeq[i] != want {
					t.Fatalf("块[%d] = %q, want %q（块序：%v）", i, gotBlockSeq[i], want, gotBlockSeq)
				}
			}
			if gotThinking.String() != wantThinking.String() {
				t.Fatalf("思考摘要文本不符：got %d 字符, want %d 字符", gotThinking.Len(), wantThinking.Len())
			}
			if gotText.String() != wantText.String() {
				t.Fatalf("正文文本不符：got %d 字符, want %d 字符", gotText.Len(), wantText.Len())
			}
			if gotArgs.String() != wantArgs.String() {
				t.Fatalf("工具参数片段不符：got %d 字符, want %d 字符", gotArgs.Len(), wantArgs.Len())
			}
			if len(gotToolIDs) != len(wantToolIDs) {
				t.Fatalf("工具块数 = %d, want %d", len(gotToolIDs), len(wantToolIDs))
			}
			for i := range wantToolIDs {
				if gotToolIDs[i] != wantToolIDs[i] || gotToolName[i] != wantToolName[i] {
					t.Fatalf("工具[%d] = %s/%s, want %s/%s", i, gotToolIDs[i], gotToolName[i], wantToolIDs[i], wantToolName[i])
				}
			}

			// ④ 非流式装配（ParseResponse）：按块（tool_result 跳过）；工具参数
			// 合并后必须是合法 JSON（出口契约）。
			sse := toSSE(frames)
			resp, err := ParseResponse([]byte(sse))
			if err != nil {
				t.Fatalf("ParseResponse: %v", err)
			}
			wantAssembled := make([]ir.BlockKind, 0, len(fx.blockKinds))
			for _, k := range fx.blockKinds {
				if k == ir.BlockToolResult {
					continue
				}
				wantAssembled = append(wantAssembled, k)
			}
			if len(resp.Message.Blocks) != len(wantAssembled) {
				t.Fatalf("装配块数 = %d, want %d: %+v", len(resp.Message.Blocks), len(wantAssembled), resp.Message.Blocks)
			}
			for i, want := range wantAssembled {
				if resp.Message.Blocks[i].Kind != want {
					t.Fatalf("装配块[%d] = %q, want %q", i, resp.Message.Blocks[i].Kind, want)
				}
			}
			for _, b := range resp.Message.Blocks {
				if b.Kind == ir.BlockServerToolUse {
					if b.ToolCall == nil || !json.Valid([]byte(b.ToolCall.Arguments)) {
						t.Fatalf("工具调用参数不是合法 JSON: %+v", b.ToolCall)
					}
					if b.ToolCall.Name != "bash_tool" {
						t.Fatalf("工具名 = %q, want bash_tool", b.ToolCall.Name)
					}
				}
			}
			if fx.argDeltas == 0 && fx.name == "runB_55480.537.frames.jsonl.txt" {
				// runB 无工具块：装配面只有 thinking + text。
				if len(resp.Message.Blocks) != 2 {
					t.Fatalf("runB 装配 = %+v", resp.Message.Blocks)
				}
			}
		})
	}
}

// TestThinkingSummaryDeltaIsIncrement thinking_summary_delta 是**增量**（逐条摘要），
// 不是全量：逐条 Feed 的文本等于各条 summary 原文（不去重、不合并）。
func TestThinkingSummaryDeltaIsIncrement(t *testing.T) {
	p := NewStreamParser()
	_, _ = p.Feed("message_start", []byte(`{"type":"message_start","message":{"id":"m","model":"claude-sonnet-5-5"}}`))
	_, _ = p.Feed("content_block_start", []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","summaries":[],"cut_off":false,"truncated":true,"start_timestamp":"2026-10-14T08:47:09.632600673Z","stop_timestamp":null,"flags":null}}`))
	for _, s := range []string{"解答问题中。", "计算阶乘末尾零的个数。"} {
		evs, err := p.Feed("content_block_delta", []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_summary_delta","summary":{"summary":"`+s+`"}}}`))
		if err != nil {
			t.Fatalf("summary delta: %v", err)
		}
		if len(evs) != 1 || evs[0].BlockKind != ir.BlockThinking || evs[0].TextDelta != s {
			t.Fatalf("summary delta = %+v, want thinking delta %q", evs, s)
		}
	}
	// 空摘要帧：无信息 → 不产事件（首片空 input_json_delta 同款）。
	if evs, err := p.Feed("content_block_delta", []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_summary_delta","summary":{"summary":""}}}`)); err != nil || len(evs) != 0 {
		t.Fatalf("空摘要 = %+v err=%v, want none", evs, err)
	}
	// 停止帧带 summary_pending（未建模字段，忽略）。
	if evs, err := p.Feed("content_block_stop", []byte(`{"type":"content_block_stop","index":0,"stop_timestamp":"2026-10-14T08:47:44.583920726Z","summary_pending":false}`)); err != nil || len(evs) != 1 || evs[0].BlockKind != ir.BlockThinking {
		t.Fatalf("thinking block_stop = %+v err=%v", evs, err)
	}
	// 工具块的显示面更新（tool_use_block_update_delta）：未建模 → 忽略。
	_, _ = p.Feed("content_block_start", []byte(`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_x","name":"bash_tool","input":{}}}`))
	if evs, err := p.Feed("content_block_delta", []byte(`{"type":"content_block_delta","index":1,"delta":{"type":"tool_use_block_update_delta","message":"Verify","display_content":{"type":"json_block"}}}`)); err != nil || len(evs) != 0 {
		t.Fatalf("显示面更新 = %+v err=%v, want none", evs, err)
	}
	// tool_result 块的 delta：无载体 → 忽略（块边界仍保真）。
	_, _ = p.Feed("content_block_stop", []byte(`{"type":"content_block_stop","index":1}`))
	_, _ = p.Feed("content_block_start", []byte(`{"type":"content_block_start","index":2,"content_block":{"type":"tool_result","tool_use_id":"toolu_x","content":[{"type":"text","text":"ok"}]}}`))
	if evs, err := p.Feed("content_block_delta", []byte(`{"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"ignored"}}`)); err != nil || len(evs) != 0 {
		t.Fatalf("tool_result delta = %+v err=%v, want none", evs, err)
	}
	evs, err := p.Feed("content_block_stop", []byte(`{"type":"content_block_stop","index":2}`))
	if err != nil || len(evs) != 1 || evs[0].BlockKind != ir.BlockToolResult {
		t.Fatalf("tool_result block_stop = %+v err=%v", evs, err)
	}
}
