package claude_web

// attach.go 附件（图像）面：渲染期产出作业 + 会话准备期上传回填（A2A-L19-W22-1；
// 接缝收口 A2A-L19-W24-1）。
//
// 网页线上传链（KB §④）：图片字节 → `POST /api/{org}/upload` multipart →
// `{file_uuid}` → completion 体 `files:[<uuid>]`（CW core/api.go:475-567 /
// YS core/api.go:451-544 / BK claude.go:140-186 三实现互证；CWK 的
// wiggle/upload-file 是带会话上下文的另一端点，AU 无上传面——本实现取
// org 级 `/api/{org}/upload`，与会话准备期的 org 面一致）。
//
// **渲染面是纯函数**（无网络、无凭据）：图像块在渲染期解析出载体
//（inline base64 / URL），为每份附件生成**占位引用**（v4 uuid 形态，与真实
// file_uuid 同长 36 字符），写入 completion 体 `files[]`；同时产出一份
// **附件作业**（AttachmentJob，持有渲染体切片 + 占位引用在体内的偏移 + 附件
// 载荷），经 `RenderOptions.AttachmentSink` **交付给调用方**。服务壳在会话准备
//（`claudeWebPrepare`）消费作业、执行真实上传（multipart，见
// service/claude_web.go），并以 `Resolve` **原地等长回填** file_uuid——渲染体是
// 同一份切片（pairs.Convert 到 `bytes.NewReader` 零拷贝），回填发生在请求发出
// 之前，对上游可见。
//
// **接缝（W24-1）= 零全局状态**：交付路径 = 渲染器写调用方传入的
// `AttachmentSink`（`pairs.Options.AttachmentSink` 同名位镜像搬运；服务壳
// `forward` 在每次上游尝试持有自己的 sink，把 `sink.Job` 传给
// `claudeWebPrepare`）。并发语义：sink 是**每次尝试的调用方局部对象**——并发
// 请求各持各的 sink，零跨请求共享面。W22-1 的 goroutine-id 配对表（全局槽位 +
// 头部解析）已被本接缝替换：「同 goroutine 至多一作业」的有界性随全局态一起
// 下线，调用方纪律 = **每次渲染传一个新鲜 sink**（复用 = 后渲染覆盖，与
// `pairs.Options.Diag` 同款）。**无 sink 而含图像块 = 显式报错**（渲染器无处
// 交付 = 体永远无法回填；绝不产出无法使用的字节）。
//
// 缺省面（无图像块）：`files` 恒 `[]`，不产出作业——与历史行为逐字节相等。

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/dwgx/any2api/translate/ir"
)

// MaxAttachmentBytes 单份附件字节上限。边界裁定（KB §④ 五实现并集）：BK
// `maxImageBytes = 20MB`（image.go:14,`validateImage`）是唯一显式上限（CW/YS
// 无上限、CWK 仅 300s 超时）——取 BK 原值，超出显式报错（绝不截断/静默丢）。
const MaxAttachmentBytes = 20 << 20

// Attachment 一份待上传附件（渲染期从图像块载体解析）。
type Attachment struct {
	// MediaType 声明的媒体类型（inline 载体：ImageSource.MediaType 或
	// data-URI 头；URL 载体为空——取回时按响应头/嗅探裁定）。
	MediaType string
	// Data inline 载体的解码字节（nil = URL 载体）。
	Data []byte
	// URL file 载体的取回地址（http/https；data-URI 已在渲染期就地解码，
	// 绝不入此字段）。
	URL string
}

// attachmentSlot 一份附件在渲染体里的占位引用（token）及其字节偏移。
type attachmentSlot struct {
	token  string
	offset int
}

// AttachmentJob 一次渲染产出的附件作业（渲染期产出、会话准备期消费）。
//
// 生命周期（W24-1 接缝）：`RenderRequestWithOptions` 在渲染体生成后构造作业并
// 交付给调用方传入的 `AttachmentSink`（无全局状态——渲染器不保留任何引用）；
// 服务壳 `forward` 把 `sink.Job` 传给 `claudeWebPrepare` 消费（上传 + Resolve
// 原地回填）。调用方纪律：每次渲染传一个新鲜 sink；复用 sink = 后渲染覆盖
// （有界，不累积——与 `pairs.Options.Diag` 同款）。
type AttachmentJob struct {
	// body 渲染体切片（回填目标；与发往上游的请求体同一份）。
	body []byte
	// slots 占位引用（与 images 同序、同长）。
	slots []attachmentSlot
	// images 附件载荷（与 slots 同序）。
	images []Attachment
}

// AttachmentSink 附件作业的交付出口（调用方持有；渲染器填写）。
//
// 渲染器零全局状态：含图像块的渲染把作业写进调用方传入的 sink（`Job` 被
// 覆盖为本次渲染的作业）；**sink == nil 而含图像块 = 显式报错**（无处交付 =
// 体永远无法回填）。调用方纪律：每次渲染传一个新鲜 sink（复用 = 后渲染覆盖，
// 与 `pairs.Options.Diag` 同款）。
type AttachmentSink struct {
	// Job 本次渲染产出的附件作业（无图像块 = nil，保持零值不动）。
	Job *AttachmentJob
}

// Attachments 附件载荷（顺序 = 消息/块序）。
func (j *AttachmentJob) Attachments() []Attachment { return j.images }

// Len 附件份数。
func (j *AttachmentJob) Len() int { return len(j.slots) }

// Body 渲染体切片（测试/诊断用；正常路径由服务壳经 Resolve 回填后发出）。
func (j *AttachmentJob) Body() []byte { return j.body }

// Resolve 用上游返回的 file_uuid 原地回填第 i 份附件的占位引用。
//
// 等长约束：回填是**原地字节替换**（切片长度不可变——请求体读取方持有的是
// 同一份切片），故 file_uuid 必须与占位引用同长（36 字符）且为 JSON 字符串
// 安全 ASCII（不含 `"`/`\`/控制字符）——不符显式报错（ErrAttachmentRef），
// 绝不截断/转义回填（静默损坏 JSON 是最贵的事故）。
func (j *AttachmentJob) Resolve(i int, fileUUID string) error {
	if i < 0 || i >= len(j.slots) {
		return fmt.Errorf("%w: slot %d out of range [0,%d)", ErrAttachmentRef, i, len(j.slots))
	}
	slot := j.slots[i]
	if len(fileUUID) != len(slot.token) {
		return fmt.Errorf("%w: upstream file uuid %q is %d chars, want %d (in-place patch requires equal length)",
			ErrAttachmentRef, fileUUID, len(fileUUID), len(slot.token))
	}
	for k := range len(fileUUID) {
		c := fileUUID[k]
		if c < 0x21 || c > 0x7e || c == '"' || c == '\\' {
			return fmt.Errorf("%w: upstream file uuid contains non-JSON-safe byte %q", ErrAttachmentRef, c)
		}
	}
	copy(j.body[slot.offset:slot.offset+len(slot.token)], fileUUID)
	return nil
}

// deliverAttachmentJob 构造作业（定位每个占位引用在渲染体里的偏移）并交付
// sink。占位引用是 v4 uuid（全局唯一）——偏移查找恒精确；找不到 = 内部错误
// （显式）。sink == nil = 渲染器无处交付（零全局状态）→ 显式报错（绝不产出
// 无法回填的体）。
func deliverAttachmentJob(body []byte, tokens []string, atts []Attachment, sink *AttachmentSink) error {
	if len(tokens) != len(atts) {
		return fmt.Errorf("%w: %d attachment tokens for %d attachments", ErrBadIR, len(tokens), len(atts))
	}
	if sink == nil {
		return fmt.Errorf("%w: %d image attachment(s) rendered without an AttachmentSink (renderer keeps no global state)", ErrBadIR, len(atts))
	}
	job := &AttachmentJob{body: body, images: atts}
	for _, tok := range tokens {
		off := bytes.Index(body, []byte(tok))
		if off < 0 {
			return fmt.Errorf("%w: attachment token missing from rendered body", ErrBadIR)
		}
		job.slots = append(job.slots, attachmentSlot{token: tok, offset: off})
	}
	sink.Job = job
	return nil
}

// resolveAttachments 按消息/块序解析全部图像块载体（顺序 = 上游 files[] 顺序）。
func resolveAttachments(messages []ir.Message) ([]Attachment, error) {
	var out []Attachment
	for _, m := range messages {
		for _, blk := range m.Blocks {
			if blk.Kind != ir.BlockImage {
				continue
			}
			att, err := resolveAttachment(blk)
			if err != nil {
				return nil, fmt.Errorf("role %q: %w", m.Role, err)
			}
			out = append(out, att)
		}
	}
	return out, nil
}

// resolveAttachment 单图像块 → 附件载体。
//
// 载体面（brief ③「无载体面保持显式报错」）：
//   - `Kind=="inline"`（claude_messages `source.type:"base64"`）→ Data 解码；
//   - `Kind=="file"` + URL 为 data-URI（openai `image_url` 的 data-URI 形态）→
//     就地解码（渲染面完成，无需取回）；
//   - `Kind=="file"` + http/https URL → 保留 URL（取回由服务壳经守卫执行，
//     见 service/claude_web.go 的 claudeWebAttachmentBytes）；
//   - 其余（无 Data 无 URL / Kind 未知且无载荷）→ ErrAttachmentCarrier（显式）。
//
// Kind 为空时按载荷推断（容忍面：inline 看 Data、file 看 URL）——推断不出即报错。
func resolveAttachment(blk ir.ContentBlock) (Attachment, error) {
	img := blk.Image
	if img == nil {
		return Attachment{}, fmt.Errorf("%w: image block without ImageSource payload", ErrBadIR)
	}
	kind := img.Kind
	if kind == "" {
		switch {
		case strings.TrimSpace(img.Data) != "":
			kind = "inline"
		case strings.TrimSpace(img.URL) != "":
			kind = "file"
		}
	}
	switch kind {
	case "inline":
		data, mediaType, err := decodeInlineImage(img.MediaType, img.Data)
		if err != nil {
			return Attachment{}, err
		}
		return Attachment{MediaType: mediaType, Data: data}, nil
	case "file":
		u := strings.TrimSpace(img.URL)
		if u == "" {
			return Attachment{}, fmt.Errorf("%w: image block has neither inline data nor URL", ErrAttachmentCarrier)
		}
		if strings.HasPrefix(strings.ToLower(u), "data:") {
			data, mediaType, err := decodeDataURI(u)
			if err != nil {
				return Attachment{}, err
			}
			return Attachment{MediaType: mediaType, Data: data}, nil
		}
		return Attachment{URL: u}, nil
	default:
		return Attachment{}, fmt.Errorf("%w: image block has neither inline data nor URL", ErrAttachmentCarrier)
	}
}

// decodeInlineImage inline 载体 → 解码字节 + 媒体类型。
//
// 载荷形态：裸 base64（claude_messages `source.type:"base64"`）或整串 data-URI
// （容忍面）。base64 变体容忍 = BK `decodeImageBase64` 同款四编码（Std/RawStd/
// URL/RawURL）。
func decodeInlineImage(mediaType, data string) ([]byte, string, error) {
	raw := strings.TrimSpace(data)
	if raw == "" {
		return nil, "", fmt.Errorf("%w: image block has neither inline data nor URL", ErrAttachmentCarrier)
	}
	if strings.HasPrefix(strings.ToLower(raw), "data:") {
		return decodeDataURI(raw)
	}
	var (
		b   []byte
		err error
	)
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err = enc.DecodeString(raw); err == nil {
			break
		}
	}
	if err != nil {
		return nil, "", fmt.Errorf("%w: invalid base64 image payload", ErrAttachmentCarrier)
	}
	if err := checkAttachmentSize(len(b)); err != nil {
		return nil, "", err
	}
	return b, mediaType, nil
}

// decodeDataURI `data:[<mediatype>][;base64],<data>` → 解码字节 + 媒体类型。
// 只接受 `;base64` 形态（非 base64 的 data-URI 无字节载体）；媒体类型取 `data:`
// 与 `;` 之间（空 → 留空，由取回侧嗅探裁定）。
func decodeDataURI(uri string) ([]byte, string, error) {
	header, payload, ok := strings.Cut(uri, ",")
	if !ok || !strings.HasPrefix(strings.ToLower(header), "data:") {
		return nil, "", fmt.Errorf("%w: malformed data URI", ErrAttachmentCarrier)
	}
	meta := header[len("data:"):]
	base64Encoded := false
	for _, param := range strings.Split(meta, ";")[1:] {
		if strings.EqualFold(strings.TrimSpace(param), "base64") {
			base64Encoded = true
		}
	}
	if !base64Encoded {
		return nil, "", fmt.Errorf("%w: data URI without ;base64 encoding", ErrAttachmentCarrier)
	}
	mediaType := strings.TrimSpace(strings.Split(meta, ";")[0])
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(payload))
	if err != nil {
		// 容忍无填充/URL 变体（与 decodeInlineImage 同表）。
		for _, enc := range []*base64.Encoding{base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
			if b, err = enc.DecodeString(strings.TrimSpace(payload)); err == nil {
				break
			}
		}
	}
	if err != nil {
		return nil, "", fmt.Errorf("%w: invalid base64 in data URI", ErrAttachmentCarrier)
	}
	if err := checkAttachmentSize(len(b)); err != nil {
		return nil, "", err
	}
	return b, mediaType, nil
}

// checkAttachmentSize 附件字节边界（KB 裁定：BK 20MB 上限；0 字节无意义 → 报错）。
func checkAttachmentSize(n int) error {
	if n == 0 {
		return fmt.Errorf("%w: empty attachment payload", ErrAttachmentCarrier)
	}
	if n > MaxAttachmentBytes {
		return fmt.Errorf("%w: attachment is %d bytes (limit %d)", ErrAttachmentTooLarge, n, MaxAttachmentBytes)
	}
	return nil
}
