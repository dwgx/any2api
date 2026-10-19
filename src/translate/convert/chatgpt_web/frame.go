package chatgpt_web

import (
	"bufio"
	"encoding/json"
	"io"
	"strings"
)

// ---------------------------------------------------------------------------
// 切帧（件 1；对照 A web_client.py:205-252）
// ---------------------------------------------------------------------------

// Frame 一个 SSE 帧：event 名（可空）+ data 载荷（多行以 "\n" 连接）。
type Frame struct {
	Event string
	Data  string
}

// SplitFrames 把原始 SSE 字节流切成帧（A `_iter_sse` 语义，web_client.py:205-220）：
//
//   - 空行派发当前帧（多行 data 以 "\n" 连接）；
//   - `:` 开头的行是注释/心跳——跳过（A:213-214；KB §4 ⑤）；
//   - `event:` 行更新事件名（缺省 ""——本包不依赖事件名分派，名字只做记录）；
//   - `data:` 行去掉冒号后前导空白后累积（A:218 的 `lstrip`）；
//   - 其它字段行（id:/retry:）忽略；
//   - EOF 时派发未闭合帧（A:219-220）。
//
// 行尾 \r\n 规范化（A:210 的 rstrip("\r")）。
func SplitFrames(r io.Reader) ([]Frame, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 64*1024*1024)
	var acc sseAccumulator
	for sc.Scan() {
		acc.feedLine(sc.Text())
	}
	if err := sc.Err(); err != nil {
		return acc.frames, err
	}
	acc.flush()
	return acc.frames, nil
}

// sseAccumulator 行级 SSE 累积器（SplitFrames 与 splitSSEString 共用）。
type sseAccumulator struct {
	event  string
	data   []string
	frames []Frame
}

func (a *sseAccumulator) feedLine(line string) {
	line = strings.TrimSuffix(line, "\r")
	if line == "" {
		a.flush()
		return
	}
	if strings.HasPrefix(line, ":") {
		return // 注释行（心跳）——不产帧
	}
	if v, ok := strings.CutPrefix(line, "event:"); ok {
		a.event = strings.TrimSpace(v)
		return
	}
	if v, ok := strings.CutPrefix(line, "data:"); ok {
		a.data = append(a.data, strings.TrimLeft(v, " \t"))
		return
	}
	// id: / retry: / 其它字段行：completion 流不使用，忽略。
}

func (a *sseAccumulator) flush() {
	if len(a.data) > 0 {
		a.frames = append(a.frames, Frame{Event: a.event, Data: strings.Join(a.data, "\n")})
	}
	a.event = ""
	a.data = a.data[:0]
}

// splitSSEString 行级切帧的字符串版（内嵌 SSE 项用；与 SplitFrames 同一累积器）。
func splitSSEString(s string) []Frame {
	var acc sseAccumulator
	for _, line := range strings.Split(s, "\n") {
		acc.feedLine(line)
	}
	acc.flush()
	return acc.frames
}

// DecodePayload 解开 data 载荷的编码层（A web_client.py:226-252 语义 + 本包硬上限）：
//
//   - 一层 JSON（对象/数组/标量）；
//   - JSON 字符串且（去空白后）以 `{`/`[`/`"` 开头 → 再解一层（双重编码；
//     A:231-252 的「one optional nested layer」——本包允许链式但 ≤ maxDecodeDepth）；
//   - 字符串/裸文本含内嵌 SSE 文本（`data:` 行）→ 取最后一帧的 data 再解
//     （A:226-228 的 _parse_encoded_stream_item：WS 项/双重包装的形态）；
//   - 解层后暴露的裸 `[DONE]` 哨兵（引号包裹 `"[DONE]"` / 双重引号 / 内嵌
//     `data: [DONE]` 三形态在此汇合）→ 原样放行 ok=true（W33-1 修复：此前
//     恒 ok=false——上层 feedPayload 的引号/内嵌哨兵分支不可达，W32-1 §2 实证）；
//   - 非 JSON（心跳/其它非 JSON 行）→ ok=false（忽略，B:66-69）。
//
// 返回归一化后的 JSON 文本（最终层；JSON 字符串标量保持带引号形态，如 `"v1"`）。
func DecodePayload(raw string) (json.RawMessage, bool) {
	cur := strings.TrimSpace(raw)
	for range maxDecodeDepth {
		if cur == "" {
			return nil, false
		}
		var v any
		if err := json.Unmarshal([]byte(cur), &v); err == nil {
			s, isStr := v.(string)
			if !isStr {
				return json.RawMessage(cur), true
			}
			t := strings.TrimSpace(s)
			if t == "" {
				return json.RawMessage(cur), true // 空字符串：无信息，交给上层忽略
			}
			if t[0] == '{' || t[0] == '[' || t[0] == '"' {
				cur = t
				continue
			}
			if embedded := lastEmbeddedSSEData(t); embedded != "" {
				cur = embedded
				continue
			}
			return json.RawMessage(cur), true // 普通字符串（如 "v1" 标记）
		}
		// 容忍面（W33-1 修复）：解层后暴露的裸 [DONE] 哨兵——放行给上层
		// （feedPayload 的哨兵分支）。引号/双重引号/内嵌 `data: [DONE]` 三形态
		// 都在此汇合：旧逻辑在此恒 ok=false（W32-1 §2 实证——容忍分支不可达）。
		if cur == sentinelDone {
			return json.RawMessage(cur), true
		}
		// 非 JSON：可能是内嵌 SSE 项（A:226-228）——含 data: 行才当编码项。
		if embedded := lastEmbeddedSSEData(cur); embedded != "" {
			cur = embedded
			continue
		}
		return nil, false // 心跳/其它非 JSON——忽略
	}
	return nil, false
}

// lastEmbeddedSSEData 字符串里内嵌 SSE 文本时取最后一帧的 data
// （A:226-228 `_parse_encoded_stream_item`：`events[-1]`）。无帧 → ""。
func lastEmbeddedSSEData(s string) string {
	frames := splitSSEString(s)
	if len(frames) == 0 {
		return ""
	}
	return frames[len(frames)-1].Data
}
