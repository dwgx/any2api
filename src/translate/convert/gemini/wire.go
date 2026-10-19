// Package gemini 实现 Gemini generateContent（POST /v1beta/models/{m}:generateContent）
// 线格式与 IR 的双向转换（parse: 线格式→IR；render: IR→线格式），覆盖请求 /
// 非流式响应 / 流式三面。
//
// 依据：docs/architecture.md（请求侧/响应侧/流式 delta
// 三张表 + §5.1 finishReason 22 值全枚举 + §7.1 错误信封双向 + §7.3 quirks）；
// research/topics/gemini-wire/README.md（contents/parts 联合类型、alt=sse 分帧、
// usageMetadata 字段、thinkingConfig、safetySettings）；research/topics/
// stream-mechanics/README.md（§4.2 文本增量的实测更正、§4.4 usage 逐帧快照、
// §4.6 哨兵是合成器职责）；research/topics/arch-options/README.md §2.3。
//
// 关键纪律（表内「不照抄」项一律按表，不按任一参考实现）：
//   - 文本是**增量**、usageMetadata 是**逐帧累计快照**（末帧权威 + merge-non-zero）——
//     按累计语义去重会丢内容；逐帧累加/取首帧会错账（SM §4.2/§4.4）。
//   - functionCall **无 index**：出口按 call id（缺 id 退 name）稳定化槽位，
//     帧内 index 重启不串台（NA :283-300）。
//   - 出现 functionCall ⟹ 强制 toolUse（覆盖上游 finishReason）；finishReason 只在
//     终块发一次，STOP 帧的 finishReason 不提前落地。
//   - 无哨兵：Gemini 出口绝不发 `[DONE]`，「无哨兵 EOF」是正常结束路径。
//   - 流中错误可能是**非 `data:` 前缀的 JSON 行**（括号平衡成帧）——分帧器必须回退。
//   - prompt 被拦（candidates=[] + promptFeedback.blockReason）必须显式化
//     （StopError → content_filter 语义），禁止空回复静默成功（GW 陷阱 3）。
//   - 工具 schema 子集清洗：无 $ref、allOf 拍平、anyOf/oneOf 投影最强分支、
//     const→enum、enum 字符串化、x-* 删除；`parameters`（紧）与 `parametersJsonSchema`
//     （保留标准约束与 additionalProperties）两载体按 schema 内容选择（schema.go）。
//   - 序列化手写 wire 对象：0 值有意义的字段（finishReason、parts、usageMetadata）
//     不靠 omitempty（arch-options §2.3 总原则 4）。
package gemini

import "encoding/json"

// 线格式常量。
const (
	roleUser  = "user"
	roleModel = "model"

	finishStop        = "STOP"
	finishMaxTokens   = "MAX_TOKENS"
	finishSafety      = "SAFETY"
	finishUnspecified = "FINISH_REASON_UNSPECIFIED"
)

// ---------------------------------------------------------------------------
// 解析侧 wire 结构（宽容读取：缺省=不变、未知=忽略；请求/响应保留 Raw 以便计算
// Extra 保真旁路；流帧高频瞬时，未知字段忽略即可）
// ---------------------------------------------------------------------------

type requestWire struct {
	Model             string            `json:"model"`
	Contents          []json.RawMessage `json:"contents"`
	SystemInstruction json.RawMessage   `json:"systemInstruction"`
	Tools             []json.RawMessage `json:"tools"`
	ToolConfig        json.RawMessage   `json:"toolConfig"`
	GenerationConfig  json.RawMessage   `json:"generationConfig"`
	SafetySettings    json.RawMessage   `json:"safetySettings"`
	CachedContent     json.RawMessage   `json:"cachedContent"`
	// Stream 非官方扩展位：Gemini 线格式的流开关在路径（`:streamGenerateContent?alt=sse`），
	// 请求体无此字段——但经网关/代理转手时可能被带上（OpenAI 形状直迁的客户端、
	// 兼容层）。宽容读取并**保留进 IR**（P53 quirk ④：此前被静默丢弃，恒 false）；
	// 缺省 = 未声明（false），绝不发明。
	Stream bool `json:"stream"`
}

type contentWire struct {
	Role  string            `json:"role"`
	Parts []json.RawMessage `json:"parts"`
}

// partWire parts[] 联合类型（字段并集，按存在性解释）。
type partWire struct {
	Text                string                   `json:"text"`
	Thought             bool                     `json:"thought"`
	ThoughtSignature    string                   `json:"thoughtSignature"`
	FunctionCall        *functionCallWire        `json:"functionCall"`
	FunctionResponse    *functionResponseWire    `json:"functionResponse"`
	InlineData          *inlineDataWire          `json:"inlineData"`
	FileData            *fileDataWire            `json:"fileData"`
	ExecutableCode      *executableCodeWire      `json:"executableCode"`
	CodeExecutionResult *codeExecutionResultWire `json:"codeExecutionResult"`
}

type functionCallWire struct {
	ID           string           `json:"id"`
	Name         string           `json:"name"`
	Args         json.RawMessage  `json:"args"`
	PartialArgs  []partialArgWire `json:"partialArgs"`
	WillContinue *bool            `json:"willContinue"`
}

// partialArgWire 流式函数调用分片（GW §流式 6；官方文档级规格未载，语义以
// NA :474-560 为准 [INFERENCE]）。
type partialArgWire struct {
	JSONPath    string          `json:"jsonPath"`
	StringValue *string         `json:"stringValue"`
	NumberValue json.RawMessage `json:"numberValue"`
	BoolValue   *bool           `json:"boolValue"`
	NullValue   json.RawMessage `json:"nullValue"`
}

type functionResponseWire struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Response json.RawMessage `json:"response"`
}

type inlineDataWire struct {
	MimeType string `json:"mimeType"`
	Data     string `json:"data"`
}

type fileDataWire struct {
	MimeType string `json:"mimeType"`
	FileURI  string `json:"fileUri"`
}

type executableCodeWire struct {
	Language string `json:"language"`
	Code     string `json:"code"`
}

type codeExecutionResultWire struct {
	Outcome string `json:"outcome"`
	Output  string `json:"output"`
}

type toolWire struct {
	FunctionDeclarations []json.RawMessage `json:"functionDeclarations"`
}

type functionDeclarationWire struct {
	Name                 string          `json:"name"`
	Description          string          `json:"description"`
	Parameters           json.RawMessage `json:"parameters"`
	ParametersJSONSchema json.RawMessage `json:"parametersJsonSchema"`
}

type toolConfigWire struct {
	FunctionCallingConfig json.RawMessage `json:"functionCallingConfig"`
}

type functionCallingConfigWire struct {
	Mode                 string   `json:"mode"`
	AllowedFunctionNames []string `json:"allowedFunctionNames"`
}

type generationConfigWire struct {
	MaxOutputTokens    json.RawMessage `json:"maxOutputTokens"`
	Temperature        json.RawMessage `json:"temperature"`
	TopP               json.RawMessage `json:"topP"`
	TopK               json.RawMessage `json:"topK"`
	StopSequences      json.RawMessage `json:"stopSequences"`
	CandidateCount     json.RawMessage `json:"candidateCount"`
	Seed               json.RawMessage `json:"seed"`
	PresencePenalty    json.RawMessage `json:"presencePenalty"`
	FrequencyPenalty   json.RawMessage `json:"frequencyPenalty"`
	ResponseMimeType   json.RawMessage `json:"responseMimeType"`
	ResponseSchema     json.RawMessage `json:"responseSchema"`
	ResponseJSONSchema json.RawMessage `json:"responseJsonSchema"`
	ResponseModalities json.RawMessage `json:"responseModalities"`
	ThinkingConfig     json.RawMessage `json:"thinkingConfig"`
}

type responseWire struct {
	ResponseID     string            `json:"responseId"`
	ModelVersion   string            `json:"modelVersion"`
	CreateTime     string            `json:"createTime"`
	Candidates     []json.RawMessage `json:"candidates"`
	PromptFeedback json.RawMessage   `json:"promptFeedback"`
	UsageMetadata  json.RawMessage   `json:"usageMetadata"`
}

type candidateWire struct {
	Index        *int            `json:"index"`
	Content      json.RawMessage `json:"content"`
	FinishReason string          `json:"finishReason"`
}

type promptFeedbackWire struct {
	BlockReason string `json:"blockReason"`
}

// usageMetadataWire 建模字段（details 分账未建模——见 parseUsage 的丢弃注释）。
type usageMetadataWire struct {
	PromptTokenCount        *int64 `json:"promptTokenCount"`
	CandidatesTokenCount    *int64 `json:"candidatesTokenCount"`
	TotalTokenCount         *int64 `json:"totalTokenCount"`
	CachedContentTokenCount *int64 `json:"cachedContentTokenCount"`
	ThoughtsTokenCount      *int64 `json:"thoughtsTokenCount"`
}

// errorObjectWire Google 错误对象内层 {code, message, status}（§7.1）。
type errorObjectWire struct {
	Code    json.RawMessage `json:"code"`
	Message string          `json:"message"`
	Status  string          `json:"status"`
}

// ---------------------------------------------------------------------------
// 渲染侧常量表
// ---------------------------------------------------------------------------

// geminiOnlyExtraKeys 渲染 gemini 请求时回写的 gemini 独有 Extra 键
// （parse 侧保真；跨格式腿来自 OpenAI/Claude 时不会出现这些键）。
var geminiOnlyExtraKeys = []string{
	"safetySettings", "thinkingConfig", "responseMimeType",
	"responseJsonSchema", "responseModalities",
}

// defaultSafetySettings 默认安全设置（CPA `gemini/common/safety.go:9-45` 路线；
// 「请求被改写」事实——日志点在服务壳层）。CIVIC_INTEGRITY 为 deprecated 类别，
// 沿用 CPA 的 BLOCK_NONE（表 §quirk 17）。
var defaultSafetySettings = []map[string]string{
	{"category": "HARM_CATEGORY_HARASSMENT", "threshold": "OFF"},
	{"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "OFF"},
	{"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "OFF"},
	{"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "OFF"},
	{"category": "HARM_CATEGORY_CIVIC_INTEGRITY", "threshold": "BLOCK_NONE"},
}
