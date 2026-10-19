package toolcall

// UpstreamBlock 上游内容块（index 归一输入）。
type UpstreamBlock struct {
	ContentBlockIndex int
	Type              string // text | thinking | tool_use | server_tool_use | ...
	ToolUseID         string
	Name              string
}

// ToolIndexEntry 下游稠密 tool 索引条目。
type ToolIndexEntry struct {
	Index     int    `json:"index"`
	ToolUseID string `json:"tool_use_id"`
}

// IndexNormalization index 归一产物。
type IndexNormalization struct {
	Calls []ToolIndexEntry
	// ByContentBlockIndex 上游块 index → 下游 tool index（仅 tool_use 块有映射）。
	ByContentBlockIndex map[int]int
}

// NormalizeToolIndexes 把上游块序重映射为下游稠密 tool 索引（规格 §6.1）：
// 只有 tool_use 占号；text / thinking / hosted（server_tool_use）不占号、
// 不产生空洞。绝不直接用上游块 index 作下游 tool index。
func NormalizeToolIndexes(blocks []UpstreamBlock) IndexNormalization {
	res := IndexNormalization{ByContentBlockIndex: map[int]int{}}
	for _, b := range blocks {
		if b.Type != "tool_use" {
			continue
		}
		res.ByContentBlockIndex[b.ContentBlockIndex] = len(res.Calls)
		res.Calls = append(res.Calls, ToolIndexEntry{Index: len(res.Calls), ToolUseID: b.ToolUseID})
	}
	return res
}
