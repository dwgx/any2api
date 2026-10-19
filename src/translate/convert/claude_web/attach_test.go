package claude_web

// W22-1：附件面单元测试——载体解析（inline/data-URI/URL/无载体/超大）、占位引用
// 与 files[] 接线、作业交付/原地回填、并发渲染不串台（W24-1 接缝：作业经
// RenderOptions.AttachmentSink 交付，零全局状态）。
//
// 反证锚点（本文件）：撤掉 resolveAttachments 的载体报错 → TestAttachmentCarrierErrors
// 红；撤掉 Resolve 的等长/JSON 安全校验 → TestAttachmentResolveGuards 红；把作业
// 交付撤掉（sink 不填）→ TestAttachmentInlineCarrier 红（job 缺失）；把 sink 交付
// 改为包级共享槽位 → TestAttachmentGoroutinePairing 红（跨渲染串台）。

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dwgx/any2api/translate/ir"
)

func imageReq(blocks ...ir.ContentBlock) *ir.Request {
	return &ir.Request{Model: "claude-sonnet-5-5", Messages: []ir.Message{
		{Role: ir.RoleUser, Blocks: append([]ir.ContentBlock{{Kind: ir.BlockText, Text: "look"}}, blocks...)},
	}}
}

func inlineImage(mediaType, payload string) ir.ContentBlock {
	return ir.ContentBlock{Kind: ir.BlockImage, Image: &ir.ImageSource{Kind: "inline", MediaType: mediaType, Data: payload}}
}

// renderWithJob 渲染含图请求并把渲染产出的附件作业（经 sink 接缝）取回。
func renderWithJob(t *testing.T, req *ir.Request) ([]byte, *AttachmentJob) {
	t.Helper()
	sink := &AttachmentSink{}
	body, err := RenderRequestWithOptions(req, RenderOptions{AttachmentSink: sink})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	return body, sink.Job
}

// filesOf 渲染体 → files[] 数组。
func filesOf(t *testing.T, body []byte) []string {
	t.Helper()
	var w map[string]json.RawMessage
	if err := json.Unmarshal(body, &w); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	var files []string
	if err := json.Unmarshal(w["files"], &files); err != nil {
		t.Fatalf("unmarshal files: %v (raw=%s)", err, w["files"])
	}
	return files
}

// TestAttachmentInlineCarrier inline 载体：解码字节/媒体类型/占位引用/原地回填。
func TestAttachmentInlineCarrier(t *testing.T) {
	raw := []byte("hello-image-bytes")
	req := imageReq(inlineImage("image/png", base64.StdEncoding.EncodeToString(raw)))
	body, job := renderWithJob(t, req)
	files := filesOf(t, body)
	if len(files) != 1 || len(files[0]) != 36 {
		t.Fatalf("files = %v, want 1 token (36 chars)", files)
	}
	// 占位引用必须出现在渲染体内（回填靠它定位）。
	if !bytes.Contains(body, []byte(files[0])) {
		t.Fatalf("渲染体必须含占位引用")
	}
	if job == nil || job.Len() != 1 {
		t.Fatalf("job = %v, want 1 attachment", job)
	}
	atts := job.Attachments()
	if len(atts) != 1 || atts[0].MediaType != "image/png" || !bytes.Equal(atts[0].Data, raw) || atts[0].URL != "" {
		t.Fatalf("attachment = %+v, want decoded png bytes", atts)
	}
	if !bytes.Equal(job.Body(), body) {
		t.Fatalf("作业体必须与渲染体同一切片（原地回填的前提）")
	}
	ref := "abcdef01-2345-4678-9abc-def012345678"
	if err := job.Resolve(0, ref); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// 回填后：体仍为合法 JSON、files 含真实引用、占位引用消失。
	if got := filesOf(t, job.Body()); len(got) != 1 || got[0] != ref {
		t.Fatalf("resolved files = %v, want [%s]", got, ref)
	}
	if bytes.Contains(job.Body(), []byte(files[0])) {
		t.Fatalf("回填后占位引用必须消失")
	}
	// 接缝语义（W24-1）：作业只经 sink 交付（渲染器零全局残留）——再渲染
	// （新 sink）必得新作业；旧作业体不受影响。
	sink2 := &AttachmentSink{}
	if _, err := RenderRequestWithOptions(req, RenderOptions{AttachmentSink: sink2}); err != nil {
		t.Fatalf("render#2: %v", err)
	}
	if sink2.Job == nil || sink2.Job == job {
		t.Fatalf("再渲染必须产出新作业（渲染器零全局残留）")
	}
	if !bytes.Contains(job.Body(), []byte(ref)) {
		t.Fatalf("旧作业体不得被新渲染影响")
	}
}

// TestAttachmentDataURICarrier openai `image_url` 的 data-URI 形态（Kind=file +
// data: URL）→ 渲染期就地解码（无取回）。
func TestAttachmentDataURICarrier(t *testing.T) {
	raw := []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 1, 2, 3}
	uri := "data:image/png;base64," + base64.StdEncoding.EncodeToString(raw)
	req := imageReq(ir.ContentBlock{Kind: ir.BlockImage, Image: &ir.ImageSource{Kind: "file", URL: uri}})
	body, job := renderWithJob(t, req)
	if len(filesOf(t, body)) != 1 {
		t.Fatalf("data-URI 载体必须产出 1 个占位引用")
	}
	if job == nil {
		t.Fatalf("job 缺失")
	}
	atts := job.Attachments()
	if len(atts) != 1 || !bytes.Equal(atts[0].Data, raw) || atts[0].MediaType != "image/png" || atts[0].URL != "" {
		t.Fatalf("attachment = %+v, want data-URI decoded inline", atts)
	}
}

// TestAttachmentURLCarrier http(s) URL 载体：渲染期保留 URL（取回由服务壳执行）。
func TestAttachmentURLCarrier(t *testing.T) {
	req := imageReq(ir.ContentBlock{Kind: ir.BlockImage, Image: &ir.ImageSource{Kind: "file", URL: "https://example.com/a.png"}})
	body, job := renderWithJob(t, req)
	if len(filesOf(t, body)) != 1 {
		t.Fatalf("URL 载体必须产出 1 个占位引用")
	}
	if job == nil || job.Len() != 1 {
		t.Fatalf("job = %v", job)
	}
	att := job.Attachments()[0]
	if att.URL != "https://example.com/a.png" || att.Data != nil {
		t.Fatalf("attachment = %+v, want URL carrier kept for service-side fetch", att)
	}
}

// TestAttachmentCarrierErrors 无载体面显式报错（绝不静默丢）；空载荷同样报错。
func TestAttachmentCarrierErrors(t *testing.T) {
	cases := map[string]ir.ContentBlock{
		"无 Data 无 URL":    {Kind: ir.BlockImage, Image: &ir.ImageSource{Kind: "inline", MediaType: "image/png"}},
		"空 Data":          {Kind: ir.BlockImage, Image: &ir.ImageSource{Kind: "inline", MediaType: "image/png", Data: "   "}},
		"空 URL":           {Kind: ir.BlockImage, Image: &ir.ImageSource{Kind: "file", URL: ""}},
		"坏 base64":        {Kind: ir.BlockImage, Image: &ir.ImageSource{Kind: "inline", MediaType: "image/png", Data: "!!!not-base64!!!"}},
		"非 base64 URI":    {Kind: ir.BlockImage, Image: &ir.ImageSource{Kind: "file", URL: "data:text/plain,hello"}},
		"Kind 未知无载荷":      {Kind: ir.BlockImage, Image: &ir.ImageSource{Kind: "wat"}},
		"nil ImageSource": {Kind: ir.BlockImage},
	}
	for name, blk := range cases {
		if _, err := RenderRequest(imageReq(blk)); err == nil {
			t.Fatalf("%s：必须显式报错", name)
		}
	}
	// 具体哨兵：无载体 = ErrAttachmentCarrier；nil 载荷 = ErrBadIR。
	if _, err := RenderRequest(imageReq(cases["无 Data 无 URL"])); !errors.Is(err, ErrAttachmentCarrier) {
		t.Fatalf("无载体必须 ErrAttachmentCarrier: %v", err)
	}
	if _, err := RenderRequest(imageReq(cases["nil ImageSource"])); !errors.Is(err, ErrBadIR) {
		t.Fatalf("nil 载荷必须 ErrBadIR: %v", err)
	}
}

// TestAttachmentOversize 边界（KB 裁定 = BK 20MB）：> MaxAttachmentBytes 显式报错。
func TestAttachmentOversize(t *testing.T) {
	big := make([]byte, MaxAttachmentBytes+1)
	payload := base64.StdEncoding.EncodeToString(big)
	_, err := RenderRequest(imageReq(inlineImage("image/jpeg", payload)))
	if !errors.Is(err, ErrAttachmentTooLarge) {
		t.Fatalf("超大附件必须 ErrAttachmentTooLarge: %v", err)
	}
	// 恰在上限 = 通过（边界含端点；20MB 载荷在本测试内构造）。
	ok := base64.StdEncoding.EncodeToString(make([]byte, MaxAttachmentBytes))
	if _, job := renderWithJob(t, imageReq(inlineImage("image/jpeg", ok))); job == nil || job.Len() != 1 {
		t.Fatalf("上限内附件必须交付作业: %v", job)
	}
}

// TestAttachmentOrderAndMulti 多图：files[] 顺序 = 消息/块序；逐份回填互不干扰。
func TestAttachmentOrderAndMulti(t *testing.T) {
	req := &ir.Request{Model: "m", Messages: []ir.Message{
		{Role: ir.RoleUser, Blocks: []ir.ContentBlock{
			{Kind: ir.BlockText, Text: "one"},
			inlineImage("image/png", base64.StdEncoding.EncodeToString([]byte("first"))),
			inlineImage("image/jpeg", base64.StdEncoding.EncodeToString([]byte("second"))),
		}},
		{Role: ir.RoleUser, Blocks: []ir.ContentBlock{
			{Kind: ir.BlockText, Text: "two"},
			inlineImage("application/pdf", base64.StdEncoding.EncodeToString([]byte("third"))),
		}},
	}}
	body, job := renderWithJob(t, req)
	files := filesOf(t, body)
	if len(files) != 3 {
		t.Fatalf("files = %v, want 3 tokens", files)
	}
	if job == nil || job.Len() != 3 {
		t.Fatalf("job = %v, want 3", job)
	}
	atts := job.Attachments()
	wantData := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	wantType := []string{"image/png", "image/jpeg", "application/pdf"}
	for i := range atts {
		if !bytes.Equal(atts[i].Data, wantData[i]) || atts[i].MediaType != wantType[i] {
			t.Fatalf("attachment[%d] = %+v, want %s/%q", i, atts[i], wantType[i], wantData[i])
		}
	}
	refs := []string{
		"11111111-2222-4333-8444-555555555555",
		"66666666-7777-4888-8999-aaaaaaaaaaaa",
		"bbbbbbbb-cccc-4ddd-8eee-ffffffffffff",
	}
	for i, ref := range refs {
		if err := job.Resolve(i, ref); err != nil {
			t.Fatalf("resolve[%d]: %v", i, err)
		}
	}
	got := filesOf(t, job.Body())
	for i := range refs {
		if got[i] != refs[i] {
			t.Fatalf("resolved files[%d] = %q, want %q", i, got[i], refs[i])
		}
	}
}

// TestAttachmentResolveGuards 回填守卫：错长/含引号/越界 → ErrAttachmentRef（绝不
// 静默损坏 JSON）。
func TestAttachmentResolveGuards(t *testing.T) {
	body, job := renderWithJob(t, imageReq(inlineImage("image/png", base64.StdEncoding.EncodeToString([]byte("x")))))
	if job == nil {
		t.Fatalf("job 缺失")
	}
	if err := job.Resolve(0, "short"); !errors.Is(err, ErrAttachmentRef) {
		t.Fatalf("错长引用必须 ErrAttachmentRef: %v", err)
	}
	if err := job.Resolve(0, strings.Repeat("a", 35)+`"`); !errors.Is(err, ErrAttachmentRef) {
		t.Fatalf("含引号引用必须 ErrAttachmentRef: %v", err)
	}
	if err := job.Resolve(1, "abcdef01-2345-4678-9abc-def012345678"); !errors.Is(err, ErrAttachmentRef) {
		t.Fatalf("越界槽必须 ErrAttachmentRef: %v", err)
	}
	// 失败回填后体必须原样（占位引用仍在）。
	if !bytes.Equal(job.Body(), body) {
		t.Fatalf("失败回填不得改动渲染体")
	}
}

// TestAttachmentNoImages 无图像块：files 恒 []、不产出作业（缺省关逐字节纪律）。
func TestAttachmentNoImages(t *testing.T) {
	sink := &AttachmentSink{}
	body, err := RenderRequestWithOptions(textReq("m", nil), RenderOptions{AttachmentSink: sink})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	var w map[string]json.RawMessage
	_ = json.Unmarshal(body, &w)
	if string(w["files"]) != "[]" {
		t.Fatalf("files = %s, want []", w["files"])
	}
	if sink.Job != nil {
		t.Fatalf("无图像块不得产出作业")
	}
}

// TestAttachmentGoroutinePairing 配对：并发 goroutine 各渲染各持 sink——作业
// 按调用方归属，绝不串台（A 渲染 → B 渲染 → B 读 → A 读：各得其自身作业；
// 接缝 W24-1：作业只经调用方自己的 sink 交付，无全局态可串）。
func TestAttachmentGoroutinePairing(t *testing.T) {
	// 单值通道 + 多消费者 = 死锁；本测试的拓扑：
	//   aRendered：A → main（A 的占位引用）；bTaken：B → A（B 已读，A 才读）；
	//   bDone / aDone：各自 → main（结果）。
	type result struct {
		token string
		job   *AttachmentJob
		err   error
	}
	renderToken := func(raw string) (string, *AttachmentSink, error) {
		sink := &AttachmentSink{}
		body, err := RenderRequestWithOptions(
			imageReq(inlineImage("image/png", base64.StdEncoding.EncodeToString([]byte(raw)))),
			RenderOptions{AttachmentSink: sink})
		if err != nil {
			return "", nil, err
		}
		var w map[string]json.RawMessage
		if err := json.Unmarshal(body, &w); err != nil {
			return "", nil, err
		}
		var files []string
		if err := json.Unmarshal(w["files"], &files); err != nil || len(files) != 1 {
			return "", nil, fmt.Errorf("files = %v (err=%v)", files, err)
		}
		return files[0], sink, nil
	}

	aRendered := make(chan result, 1)
	bTaken := make(chan struct{})
	bDone := make(chan result, 1)
	aDone := make(chan result, 1)

	go func() { // A：先渲染，等 B 读走后自己再读。
		token, sink, err := renderToken("A")
		if err != nil {
			aRendered <- result{err: err}
			return
		}
		aRendered <- result{token: token}
		<-bTaken
		aDone <- result{token: token, job: sink.Job}
	}()

	res := <-aRendered
	if res.err != nil {
		t.Fatalf("A render: %v", res.err)
	}
	aToken := res.token

	go func() { // B：A 渲染后渲染，立即读走自己的 sink，再放行 A。
		token, sink, err := renderToken("B")
		if err != nil {
			bDone <- result{err: err}
			close(bTaken)
			return
		}
		job := sink.Job
		bDone <- result{token: token, job: job}
		close(bTaken)
	}()

	bRes := <-bDone
	if bRes.err != nil {
		t.Fatalf("B render: %v", bRes.err)
	}
	aRes := <-aDone
	if aRes.err != nil {
		t.Fatalf("A read: %v", aRes.err)
	}
	if bRes.job == nil {
		t.Fatalf("B 必须拿到自身作业")
	}
	if aRes.job == nil {
		t.Fatalf("A 必须拿到自身作业（sink 按调用方归属——包级共享槽位会在此红）")
	}
	if !bytes.Contains(aRes.job.Body(), []byte(aToken)) {
		t.Fatalf("A 的作业体必须含 A 自己的占位引用")
	}
	if !bytes.Contains(bRes.job.Body(), []byte(bRes.token)) {
		t.Fatalf("B 的作业体必须含 B 自己的占位引用")
	}
	if bytes.Contains(aRes.job.Body(), []byte(bRes.token)) || bytes.Contains(bRes.job.Body(), []byte(aToken)) {
		t.Fatalf("作业体不得跨请求串台")
	}
}

// TestAttachmentOverwritePerGoroutine 同一 sink 连续渲染：后渲染覆盖先渲染
// （失败重试形态；有界——不累积。接缝纪律 = 每次渲染一个新鲜 sink，复用 =
// 覆盖——与 pairs.Options.Diag 同款）。
func TestAttachmentOverwritePerGoroutine(t *testing.T) {
	sink := &AttachmentSink{}
	body1, err := RenderRequestWithOptions(imageReq(inlineImage("image/png", base64.StdEncoding.EncodeToString([]byte("first")))), RenderOptions{AttachmentSink: sink})
	if err != nil {
		t.Fatalf("render1: %v", err)
	}
	tok1 := filesOf(t, body1)[0]
	job1 := sink.Job
	body2, err := RenderRequestWithOptions(imageReq(inlineImage("image/png", base64.StdEncoding.EncodeToString([]byte("second")))), RenderOptions{AttachmentSink: sink})
	if err != nil {
		t.Fatalf("render2: %v", err)
	}
	tok2 := filesOf(t, body2)[0]
	job := sink.Job
	if job == nil {
		t.Fatalf("job 缺失")
	}
	if job == job1 {
		t.Fatalf("复用 sink 必须覆盖（job 换新——不累积）")
	}
	if !bytes.Contains(job.Body(), []byte(tok2)) || bytes.Contains(job.Body(), []byte(tok1)) {
		t.Fatalf("复用 sink 的作业 = 后渲染（覆盖语义）")
	}
}

// TestAttachmentEmptyPromptAllowed 空 prompt + 附件（W25-1 §9 未验面；裁定 =
// 能走）：图像-only 请求（无文本块）照常渲染——prompt 空串 + files[] 占位引用 +
// 作业交付（渲染面守卫只拦「无文本**且无附件**」）。对照：无文本且无附件仍报错
// （既有语义不变）。
func TestAttachmentEmptyPromptAllowed(t *testing.T) {
	// 图像-only：content 只有 image 块（无 text）。
	req := &ir.Request{Model: "m", Messages: []ir.Message{
		{Role: ir.RoleUser, Blocks: []ir.ContentBlock{
			inlineImage("image/png", base64.StdEncoding.EncodeToString([]byte("only-image"))),
		}},
	}}
	body, job := renderWithJob(t, req)
	var out struct {
		Prompt string   `json:"prompt"`
		Files  []string `json:"files"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Prompt != "" {
		t.Fatalf("图像-only prompt = %q, want 空串", out.Prompt)
	}
	if len(out.Files) != 1 || len(out.Files[0]) != 36 {
		t.Fatalf("files = %v, want 1 个 36 字符占位引用", out.Files)
	}
	if job == nil || job.Len() != 1 {
		t.Fatalf("job = %v, want 1 附件（空 prompt 也必须交付作业）", job)
	}
	// 对照：无文本且无附件 = 报错（守卫不变——空 prompt 本身不是放行条件）。
	empty := &ir.Request{Model: "m", Messages: []ir.Message{
		{Role: ir.RoleUser, Blocks: []ir.ContentBlock{{Kind: ir.BlockText, Text: "  "}}},
	}}
	if _, err := RenderRequest(empty); err == nil {
		t.Fatalf("无文本且无附件必须报错")
	}
}
