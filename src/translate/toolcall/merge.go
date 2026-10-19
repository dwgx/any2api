package toolcall

import (
	"encoding/json"
	"strings"
)

// MergeStep 是 MergeToolInput 7 步决策表的步号（诊断用，纯观测）。
type MergeStep int

// merge 决策表步号（规格 §4.1）。
const (
	MergeStepNone       MergeStep = 0
	MergeStepEmptyFrame MergeStep = 1 // 空帧无信息
	MergeStepFirst      MergeStep = 2 // 首帧
	MergeStepDuplicate  MergeStep = 3 // 完全重复终帧
	MergeStepSnapshot   MergeStep = 4 // 累积快照：整体替换
	MergeStepLateShort  MergeStep = 5 // 迟到旧短快照：缓冲完整才保留缓冲
	MergeStepRewrite    MergeStep = 6 // 非前缀重写：各自完整 → 只留最新
	MergeStepAppend     MergeStep = 7 // 真增量碎片：追加
)

// MergeToolInput 按规格 §4.1 的 7 步决策表把一帧上游参数并入缓冲。
//
// 所有比较均为 String 级（无字节切片、无 rune 边界操作）；Go 的 len 为字节
// 长度，与 Python 码点长度在本表条件下判定等价（前缀关系成立时长度序一致，
// 而第 4/5 步的长度比较均与前缀条件联用）。
func MergeToolInput(buf, frame string) string {
	out, _ := MergeToolInputTraced(buf, frame)
	return out
}

// MergeToolInputTraced 同 MergeToolInput，额外返回命中的步号（探针 / 诊断）。
func MergeToolInputTraced(buf, frame string) (string, MergeStep) {
	switch {
	case frame == "":
		return buf, MergeStepEmptyFrame
	case buf == "":
		return frame, MergeStepFirst
	case frame == buf:
		return buf, MergeStepDuplicate
	case len(frame) > len(buf) && strings.HasPrefix(frame, buf):
		return frame, MergeStepSnapshot
	case len(buf) > len(frame) && strings.HasPrefix(buf, frame) && IsCompleteJSON(buf):
		// IsCompleteJSON(buf) 不可省：缓冲尚在拼接中途（如 `{"outer":`）时，
		// 一个单独成帧的 `{` 会被纯形状判定误吞（最终串缺嵌套开括号）。
		return buf, MergeStepLateShort
	case IsCompleteJSON(buf) && IsCompleteJSON(frame):
		return frame, MergeStepRewrite
	default:
		return buf + frame, MergeStepAppend
	}
}

// IsCompleteJSON 报告 s 是否为完整合法 JSON 文本（任意顶层值）。
func IsCompleteJSON(s string) bool { return json.Valid([]byte(s)) }
