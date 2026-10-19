package toolcall

import (
	"encoding/json"
	"strings"
)

// Policy 失败态策略（规格 §4.4）。
type Policy int

const (
	// PolicyDefault 默认（②③开）：失败态 + 错误暴露——不发坏 JSON，整轮重试。
	PolicyDefault Policy = iota
	// PolicyPassthroughWorstCase ②③全关的最坏路径：原样透传坏 JSON
	// （最坏 == 不修；仍绝不静默成 {}）。
	PolicyPassthroughWorstCase
)

// Failure 失败态。绑定控制流：「不发坏 JSON」的判据是它非 nil，不绑开关。
type Failure struct {
	Code    string   `json:"code"`
	Message string   `json:"message,omitempty"`
	Missing []string `json:"missing,omitempty"`
}

// CodeInvalidToolInput 工具参数失败态错误码（绝不连坐凭据 / 号）。
const CodeInvalidToolInput = "INVALID_TOOL_INPUT"

const unrepairableMessage = "工具调用参数非合法 JSON（模型侧生成异常）"

// Defect 归因诊断（纯观测，绝不进控制流）。
type Defect struct {
	Kind        string `json:"kind,omitempty"`         // illegal_chars | truncated | truncated_and_illegal | malformed | missing_required
	Subkind     string `json:"subkind,omitempty"`      // glued | trailing_chars | trailing_comma | ...
	RepairLayer string `json:"repair_layer,omitempty"` // char | structure | glued | none
}

// FinalizeResult 出口裁决。Emitted=nil 表示不发参数（空参或失败态）。
type FinalizeResult struct {
	Emitted *string  `json:"emitted"`
	Failure *Failure `json:"failure"`
	Defect  *Defect  `json:"defect,omitempty"`
}

// Finalize 出口终局（规格 §3.2 / §4）：
//
//	① Bug C 闸门：对上游形态（还原前）做 required 键存在性校验——必须在修复
//	   与形态还原之前；空串当 {} 判；只判键存在性（显式 null 算存在）；
//	② 合法 JSON 直接过（字节保真，能不动就不动）；非法才进三层修复；
//	③ 双重编码解包（恒开，parse 成功之后）；
//	④ 出口纪律：空参不发（Emitted=nil）、失败态绝不发坏 JSON / 绝不伪造成 {}。
//
// required 为上游形态的必需键；nil / 空 = 不做 Bug C 校验。
func Finalize(assembled string, required []string, policy Policy) FinalizeResult {
	// ① Bug C（校验时点必须在修复前、形态还原前）。
	if len(required) > 0 {
		js := assembled
		if strings.TrimSpace(js) == "" {
			js = "{}"
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal([]byte(js), &obj); err == nil && obj != nil {
			var miss []string
			for _, k := range required {
				if _, ok := obj[k]; !ok {
					miss = append(miss, k)
				}
			}
			if len(miss) > 0 {
				return FinalizeResult{
					Failure: &Failure{
						Code:    CodeInvalidToolInput,
						Message: missingRequiredMessage(miss),
						Missing: miss,
					},
					Defect: &Defect{Kind: "missing_required"},
				}
			}
		}
		// 解析失败 / 非对象 → Bug A 地盘，不在此拦截。
	}
	// ② 合法 JSON 直接过；非法才进修复。
	repLayer := LayerNone
	if !IsCompleteJSON(assembled) {
		if assembled == "" {
			// 空参：Anthropic 靠 start 的 {}，OpenAI 出口写 "{}"（出口纪律见
			// OutletArguments）——空串永不作为 arguments / partial_json 下发。
			return FinalizeResult{}
		}
		rep, layer, ok := RepairToolJSON(assembled)
		if ok {
			assembled = rep
			repLayer = layer
		} else if policy == PolicyPassthroughWorstCase {
			e := assembled
			kind, sub := ClassifyDefect(assembled)
			return FinalizeResult{Emitted: &e, Defect: &Defect{Kind: kind, Subkind: sub, RepairLayer: string(LayerNone)}}
		} else {
			kind, sub := ClassifyDefect(assembled)
			return FinalizeResult{
				Failure: &Failure{Code: CodeInvalidToolInput, Message: unrepairableMessage},
				Defect:  &Defect{Kind: kind, Subkind: sub, RepairLayer: string(LayerNone)},
			}
		}
	}
	// ③ 双重编码解包（恒开；修复成功后不 early-return，统一走这里）。
	if u, ok := UnwrapDoubleEncoded(assembled); ok {
		assembled = u
	}
	// ④ 出口纪律。
	if assembled == "" {
		return FinalizeResult{}
	}
	e := assembled
	res := FinalizeResult{Emitted: &e}
	if repLayer != LayerNone {
		res.Defect = repairDefect(repLayer)
	}
	return res
}

func missingRequiredMessage(missing []string) string {
	return "工具调用缺少必需参数：" + strings.Join(missing, "、") + "（模型侧生成异常），请重试。"
}

func repairDefect(layer RepairLayer) *Defect {
	switch layer {
	case LayerChar:
		return &Defect{Kind: "illegal_chars", RepairLayer: string(LayerChar)}
	case LayerStructure:
		return &Defect{Kind: "truncated", RepairLayer: string(LayerStructure)}
	case LayerGlued:
		return &Defect{Kind: "malformed", Subkind: "glued", RepairLayer: string(LayerGlued)}
	}
	return nil
}

// OutletArguments 按下游契约计算 arguments / partial_json 出口串。
// send=false 表示不额外下发 delta（Anthropic 空参：content_block_start 的 {}
// 即最终值）；失败态恒 send=false（绝不发坏 JSON，也绝不把失败伪造成 {}）。
func OutletArguments(res FinalizeResult, c ConsumerContract) (out string, send bool) {
	if res.Failure != nil {
		return "", false
	}
	if res.Emitted == nil {
		switch c {
		case ConsumerOpenAI, ConsumerOpenAIResponses:
			return "{}", true
		}
		return "", false
	}
	return *res.Emitted, true
}
