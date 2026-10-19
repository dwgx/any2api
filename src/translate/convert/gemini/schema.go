package gemini

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// ---------------------------------------------------------------------------
// 工具 schema 子集清洗（gemini-conversion-table §请求侧「工具 schema 子集」行 +
// §quirk 14；清洗器结构参照 CPA internal/util/gemini_schema.go:47-133，MIT 借鉴，
// 用标准库 any-walk 重写——不拷 gjson/sjson 依赖）。
//
// Gemini 的 Schema 子集（generateContent）不认：$ref、受限的 allOf/anyOf/oneOf、
// 非字符串 enum（部分载体）、x-* 扩展；两个载体的力度不同：
//
//   - `parameters`（紧载体，OpenAPI 3.03 子集）：additionalProperties 与标准约束
//     （minLength/pattern/format/…）转 description 提示后删除；
//   - `parametersJsonSchema`（富载体）：保留标准约束与 additionalProperties。
//
// 载体选择规则（schemaNeedsRichCarrier）：schema 含富载体独有键（additionalProperties
// 或标准约束）时用 parametersJsonSchema（否则紧载体清洗会丢信息）；否则用 parameters。
// 规则由内容决定，确定性且幂等（清洗后再清洗不变）。
// ---------------------------------------------------------------------------

// tightOnlyConstraints `parameters` 紧载体不认、`parametersJsonSchema` 保留的
// 标准约束清单（对齐 CPA unsupportedConstraints，gemini_schema.go:969-978）。
var tightOnlyConstraints = []string{
	"minLength", "maxLength", "exclusiveMinimum", "exclusiveMaximum",
	"pattern", "minItems", "maxItems", "uniqueItems", "contains", "format",
	"default", "examples",
}

// maxPartialArgArrayIndex partialArgs 数组下标上限（NA :561-635；表 §quirk 15）。
const maxPartialArgArrayIndex = 4095

// schemaNeedsRichCarrier 判定 schema 是否需要 parametersJsonSchema 富载体。
func schemaNeedsRichCarrier(raw json.RawMessage) bool {
	if len(raw) == 0 || isJSONNull(raw) {
		return false
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return false
	}
	return schemaHasRichOnly(v)
}

func schemaHasRichOnly(v any) bool {
	switch node := v.(type) {
	case map[string]any:
		for k, val := range node {
			switch k {
			case "additionalProperties":
				return true
			case "properties":
				// 属性名不是关键词（schema 关键词判定只作用于 schema 节点自身）。
				if props, ok := val.(map[string]any); ok {
					for _, pv := range props {
						if schemaHasRichOnly(pv) {
							return true
						}
					}
					continue
				}
			}
			for _, c := range tightOnlyConstraints {
				if k == c {
					return true
				}
			}
			if schemaHasRichOnly(val) {
				return true
			}
		}
	case []any:
		for _, item := range node {
			if schemaHasRichOnly(item) {
				return true
			}
		}
	}
	return false
}

// cleanSchema 清洗工具 schema 到 Gemini 子集。rich=true 用 parametersJsonSchema
// 口径（保留标准约束与 additionalProperties）；rich=false 用 parameters 紧口径。
// 返回清洗后的 JSON；输入非法 JSON 时返回原样（解析侧宽容，渲染侧再由调用方决定）。
func cleanSchema(raw json.RawMessage, rich bool) (json.RawMessage, error) {
	if len(raw) == 0 || isJSONNull(raw) {
		return raw, nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("%w: schema is not JSON: %v", ErrBadIR, err)
	}
	cleaned := cleanSchemaNode(v, rich)
	return marshalJSON(cleaned)
}

func cleanSchemaNode(v any, rich bool) any {
	switch node := v.(type) {
	case map[string]any:
		return cleanSchemaObject(node, rich)
	case []any:
		out := make([]any, 0, len(node))
		for _, item := range node {
			out = append(out, cleanSchemaNode(item, rich))
		}
		return out
	default:
		return v
	}
}

func cleanSchemaObject(node map[string]any, rich bool) map[string]any {
	out := map[string]any{}
	var hints []string
	var unions []any // anyOf/oneOf 分支（后处理：投影/并入）

	// const→enum 转换（CPA convertConstToEnum：const 值进 enum，无 enum 时）。
	if c, ok := node["const"]; ok {
		if _, hasEnum := node["enum"]; !hasEnum {
			out["enum"] = []any{c}
		}
	}

	for k, v := range node {
		switch {
		case k == "$ref":
			// 子集无 $ref：转 description 提示后删除（就地展开需要 $defs 上下文，
			// 本 slice 不做展开；CPA convertRefsToHints 的提示形「See: <name>」）。
			if ref, ok := v.(string); ok {
				hints = append(hints, "See: "+refName(ref))
			}
		case k == "const":
			// 已转 enum。
		case k == "enum":
			// 后处理（字符串化 + 提示）。
			out["enum"] = v
		case k == "allOf":
			mergeAllOfInto(out, v, rich)
		case k == "anyOf" || k == "oneOf":
			if arr, ok := v.([]any); ok {
				unions = append(unions, arr...)
			}
		case k == "$defs" || k == "definitions":
			// 无 $ref 后为死重，且 Gemini 不认（CPA 同样删除）。
		case k == "additionalProperties":
			if rich {
				out[k] = cleanSchemaNode(v, rich)
			} else if b, ok := v.(bool); ok && !b {
				hints = append(hints, "No extra properties allowed")
			}
		case strings.HasPrefix(k, "x-"):
			// x-* 扩展不认（表 §quirk 14）。
		case !rich && isTightOnlyConstraint(k):
			hints = append(hints, constraintHint(k, v))
		case k == "properties":
			if props, ok := v.(map[string]any); ok {
				cleaned := make(map[string]any, len(props))
				for name, pv := range props {
					cleaned[name] = cleanSchemaNode(pv, rich)
				}
				out[k] = cleaned
			}
		default:
			out[k] = cleanSchemaNode(v, rich)
		}
	}

	// anyOf/oneOf：父节点已有 properties → 分支属性并入父节点 + nullable；
	// 否则投影最强分支（CPA flattenAnyOfOneOf 语义）。
	if len(unions) > 0 {
		projectUnion(out, unions, rich, &hints)
	}

	// enum 字符串化 + 提示（表：enum 值字符串化+提示；Gemini 部分载体只收字符串）。
	if enumV, ok := out["enum"]; ok {
		if arr, ok := enumV.([]any); ok {
			strs := make([]any, 0, len(arr))
			nonString := false
			for _, item := range arr {
				if s, isStr := item.(string); isStr {
					strs = append(strs, s)
				} else {
					nonString = true
					strs = append(strs, enumValueString(item))
				}
			}
			out["enum"] = strs
			if nonString {
				out["type"] = "string"
			}
			if len(strs) > 1 && len(strs) <= 10 {
				vals := make([]string, 0, len(strs))
				for _, s := range strs {
					vals = append(vals, s.(string))
				}
				hints = append(hints, "Allowed: "+strings.Join(vals, ", "))
			}
		}
	}

	// required 清理：只保留 properties 里存在的名字（CPA cleanupRequiredFields）。
	if req, ok := out["required"].([]any); ok {
		if props, ok := out["properties"].(map[string]any); ok {
			kept := make([]any, 0, len(req))
			for _, r := range req {
				name, ok := r.(string)
				if !ok {
					continue
				}
				if _, exists := props[name]; exists {
					kept = append(kept, name)
				}
			}
			if len(kept) == 0 {
				delete(out, "required")
			} else {
				out["required"] = kept
			}
		}
	}

	// type/items/properties 一致性（CPA sanitizeObjectProperties/sanitizeArrayItems）。
	if _, hasProps := out["properties"]; hasProps {
		if t, _ := out["type"].(string); !strings.EqualFold(t, "object") {
			out["type"] = "object"
		}
	}
	if _, hasItems := out["items"]; hasItems {
		switch t, _ := out["type"].(string); {
		case t == "":
			out["type"] = "array"
		case !strings.EqualFold(t, "array"):
			delete(out, "items")
		}
	} else if t, _ := out["type"].(string); strings.EqualFold(t, "array") {
		out["items"] = map[string]any{}
	}

	appendSchemaHints(out, hints)
	return out
}

// mergeAllOfInto 把 allOf 分支拍平进父节点（properties 缺失合并、required 并集、
// 其余键缺失合并）。
func mergeAllOfInto(out map[string]any, v any, rich bool) {
	arr, ok := v.([]any)
	if !ok {
		return
	}
	for _, branch := range arr {
		bm, ok := cleanSchemaNode(branch, rich).(map[string]any)
		if !ok {
			continue
		}
		for k, bv := range bm {
			switch k {
			case "properties":
				bp, ok := bv.(map[string]any)
				if !ok {
					continue
				}
				op, _ := out["properties"].(map[string]any)
				if op == nil {
					op = map[string]any{}
					out["properties"] = op
				}
				for name, pv := range bp {
					if _, exists := op[name]; !exists {
						op[name] = pv
					}
				}
			case "required":
				br, ok := bv.([]any)
				if !ok {
					continue
				}
				seen := map[string]bool{}
				merged := make([]any, 0, len(br))
				if existing, ok := out["required"].([]any); ok {
					for _, r := range existing {
						if s, ok := r.(string); ok {
							seen[s] = true
						}
						merged = append(merged, r)
					}
				}
				for _, r := range br {
					if s, ok := r.(string); ok {
						if seen[s] {
							continue
						}
						seen[s] = true
					}
					merged = append(merged, r)
				}
				if len(merged) > 0 {
					out["required"] = merged
				}
			default:
				if _, exists := out[k]; !exists {
					out[k] = bv
				}
			}
		}
	}
}

// projectUnion anyOf/oneOf 投影：父节点已有 properties → 分支属性并入 + nullable；
// 否则选最强分支替换节点内容（object > array > 具名类型 > null；平局取首个）。
func projectUnion(out map[string]any, unions []any, rich bool, hints *[]string) {
	hasNull := false
	types := []string{}
	bestScore := -1
	var best map[string]any
	for _, branch := range unions {
		bm, ok := cleanSchemaNode(branch, rich).(map[string]any)
		if !ok {
			continue
		}
		t, _ := bm["type"].(string)
		score := 0
		switch {
		case t == "object" || bm["properties"] != nil:
			score, t = 3, "object"
		case t == "array" || bm["items"] != nil:
			score, t = 2, "array"
		case t != "" && t != "null":
			score = 1
		case t == "null":
			hasNull = true
		}
		if t != "" {
			types = append(types, t)
		}
		if score > bestScore {
			bestScore, best = score, bm
		}
	}
	if props, ok := out["properties"].(map[string]any); ok {
		// 父节点已是对象：分支属性并入（缺失合并），union 关键字删除。
		for _, branch := range unions {
			bm, ok := cleanSchemaNode(branch, rich).(map[string]any)
			if !ok {
				continue
			}
			if bp, ok := bm["properties"].(map[string]any); ok {
				for name, pv := range bp {
					if _, exists := props[name]; !exists {
						props[name] = pv
					}
				}
			}
		}
		if hasNull {
			out["nullable"] = true
		}
		return
	}
	if best == nil {
		return
	}
	for k, v := range best {
		if _, exists := out[k]; !exists {
			out[k] = v
		}
	}
	if hasNull && best["type"] != "null" {
		out["nullable"] = true
	}
	if len(types) > 1 {
		*hints = append(*hints, "Accepts: "+strings.Join(dedupeStrings(types), " | "))
	}
}

// appendSchemaHints 把清洗提示并入 description（保序、去重）。
func appendSchemaHints(out map[string]any, hints []string) {
	if len(hints) == 0 {
		return
	}
	existing, _ := out["description"].(string)
	merged := existing
	for _, h := range hints {
		if h == "" || strings.Contains(merged, h) {
			continue
		}
		if merged == "" {
			merged = h
		} else {
			merged = merged + " (" + h + ")"
		}
	}
	out["description"] = merged
}

func isTightOnlyConstraint(k string) bool {
	for _, c := range tightOnlyConstraints {
		if k == c {
			return true
		}
	}
	return false
}

// constraintHint 标准约束的提示文本（值进 description，约束本体删除）。
func constraintHint(k string, v any) string {
	switch tv := v.(type) {
	case string:
		return k + ": " + tv
	case float64:
		return fmt.Sprintf("%s: %v", k, tv)
	case bool:
		return fmt.Sprintf("%s: %v", k, tv)
	case nil:
		return k + ": null"
	default:
		b, err := marshalJSON(v)
		if err != nil {
			return k
		}
		return k + ": " + string(b)
	}
}

// enumValueString 枚举值的字符串化（数字/布尔/null 的字面形式）。
func enumValueString(v any) string {
	switch tv := v.(type) {
	case nil:
		return "null"
	case string:
		return tv
	default:
		b, err := marshalJSON(v)
		if err != nil {
			return fmt.Sprintf("%v", tv)
		}
		return string(b)
	}
}

// refName $ref 末段名（~1→/、~0→~ 反转义）。
func refName(ref string) string {
	if i := strings.LastIndex(ref, "/"); i >= 0 && i+1 < len(ref) {
		name := ref[i+1:]
		name = strings.ReplaceAll(name, "~1", "/")
		return strings.ReplaceAll(name, "~0", "~")
	}
	return ref
}

func dedupeStrings(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
