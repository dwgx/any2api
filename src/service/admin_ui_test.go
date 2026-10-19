package service

// admin_ui_test.go —— 后台管理 dashboard 实时面（board A2A-L19-W75-1）测试。
//
// 覆盖五件事：
//  1. 托管面：`/ui/admin.html` 200 + text/html、`/ui/admin` 302 → admin.html、
//     三个伴随资产 200、字节与嵌入文件逐字一致（防"页面在、内容是聊天前端"）；
//  2. 静态契约（负控可红的门）：无 DEMO/mock 数据源、密钥只走 sessionStorage、
//     请求头 X-Management-Key、8 个页面容器齐全、CSP 逐字、无内联脚本、无外部资源、
//     无来源 chip（[已有]/[未提供] 撑破卡片头）；
//  3. W75 布局契约：5 张 KPI（auto-fit minmax(190px,1fr)）、健康条、2/3+1/3 概览网格、
//     真趋势图、页脚数据源图例、sessions/models/audit 三处筛选栏、宽表卡内横向滚动、
//     无固定 7 列网格；
//  4. 鉴权矩阵：未配 key = 404；配置后 无 key/错 key = 401；X-Management-Key 与
//     Authorization: Bearer 两条正路 = 200；store 未启用时 /admin/audit = 503
//     （UI 据此渲染"store 未启用"卡片而不是空表）；
//  5. 负控：静态契约门必须能对 7 个手工缺陷变红（删请求头 / 改用 localStorage /
//     删页面容器 / 放宽 CSP / KPI 退回固定 7 列 / 总览重挂来源 chip / 删一张 KPI）；
//     另有 node 行为面负控（无 node 则跳过）：
//     适配层对每个 /admin/* 请求带 X-Management-Key、401 → 判 unauthorized、
//     不触 localStorage、metrics 桶 → 序列映射正确。

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// 脚手架
// ---------------------------------------------------------------------------

// adminUIConfig 后台面配置：admin_key 已配（管理面注册）+ ui 面（缺省启用）。
func adminUIConfig(t *testing.T, key string) *Config {
	t.Helper()
	cfg := uiTestConfig()
	cfg.HeartbeatSeconds = 15
	cfg.AdminKey = key
	return cfg
}

// adminUIAsset 读嵌入资产（测试对照面；与 /ui/<path> 回出的字节必须逐字一致）。
func adminUIAsset(t *testing.T, name string) []byte {
	t.Helper()
	b, err := fs.ReadFile(uiAssets, "ui/"+name)
	if err != nil {
		t.Fatalf("read embedded ui/%s: %v", name, err)
	}
	return b
}

// adminUIRequest 发一发请求（不跟随重定向；headers 逐条设置）。
func adminUIRequest(t *testing.T, url string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request %s: %v", url, err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do %s: %v", url, err)
	}
	return resp
}

// ---------------------------------------------------------------------------
// 1. 托管面
// ---------------------------------------------------------------------------

func TestAdminUIAssetsServedWithExactBytes(t *testing.T) {
	_, srv := startServiceWith(t, adminUIConfig(t, "k-test"))

	// /ui/admin.html：200 + text/html + 字节逐字等于嵌入文件。
	resp := adminUIRequest(t, srv.URL+"/ui/admin.html", adminUINoHeaders())
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/ui/admin.html status = %d, want 200", resp.StatusCode)
	}
	got := uiBody(t, resp)
	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || mediaType != "text/html" {
		t.Fatalf("/ui/admin.html Content-Type = %q (parsed %q, err %v), want text/html", resp.Header.Get("Content-Type"), mediaType, err)
	}
	if want := adminUIAsset(t, "admin.html"); string(got) != string(want) {
		t.Fatalf("/ui/admin.html 字节与嵌入文件不一致（len got=%d want=%d）", len(got), len(want))
	}

	// /ui/admin（无斜杠）→ 302 /ui/admin.html。
	resp = adminUIRequest(t, srv.URL+"/ui/admin", adminUINoHeaders())
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("/ui/admin status = %d, want 302", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/ui/admin.html" {
		t.Fatalf("/ui/admin Location = %q, want /ui/admin.html", loc)
	}
	_ = uiBody(t, resp)

	// 三个伴随资产：200 + 字节一致（Content-Type 由扩展名决定，这里只强校验 non-empty + 字节）。
	for _, name := range []string{"admin.css", "admin.js", "admin-data.js"} {
		name := name
		resp := adminUIRequest(t, srv.URL+"/ui/"+name, adminUINoHeaders())
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("/ui/%s status = %d, want 200", name, resp.StatusCode)
		}
		got := uiBody(t, resp)
		if want := adminUIAsset(t, name); string(got) != string(want) {
			t.Fatalf("/ui/%s 字节与嵌入文件不一致（len got=%d want=%d）", name, len(got), len(want))
		}
		if ct := resp.Header.Get("Content-Type"); ct == "" {
			t.Fatalf("/ui/%s 缺 Content-Type", name)
		}
	}

	// 反证：SPA 回退不得吞掉管理面资产——不存在的 admin 资产必须 404（不是 index.html 200）。
	resp = adminUIRequest(t, srv.URL+"/ui/admin-missing.js", adminUINoHeaders())
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/ui/admin-missing.js status = %d（未知非 admin 路径仍应 SPA 回退 200）", resp.StatusCode)
	}
	fallback := uiBody(t, resp)
	if want := adminUIAsset(t, "index.html"); string(fallback) != string(want) {
		t.Fatalf("/ui/admin-missing.js 未回退到 index.html")
	}
}

func adminUINoHeaders() map[string]string { return map[string]string{} }

// ---------------------------------------------------------------------------
// 2. 静态契约门（+ 4 个负控样本）
// ---------------------------------------------------------------------------

// adminUIStaticProblems 逐条返回静态契约缺陷（空 = 绿）。同一函数既用于真资产，
// 也用于负控变体——门能变红才是门（见 adminUINegativeControl）。
// adminUICredentialReadRx 凭据字段读取形态（W78-2：「零凭据字节」的门）。
// 注意：**文案**里出现 api_key / cookie 是出口纪律的说明（mapConfig.redaction 与卡片注脚），
// 不是读取面——门抓的是属性/下标读取，不是词本身。
var adminUICredentialReadRx = []*regexp.Regexp{
	regexp.MustCompile(`\.api_key\b`),
	regexp.MustCompile(`\["api_key"\]`),
	regexp.MustCompile(`\.authorization\b`),
	regexp.MustCompile(`\.cookie\b`),
	regexp.MustCompile(`\bsk-[A-Za-z0-9]`),
}

func adminUIStaticProblems(name string, body []byte) []string {
	var p []string
	s := string(body)
	bad := func(format string, args ...any) { p = append(p, fmt.Sprintf(format, args...)) }
	// 凭据字节：四个资产都不得出现凭据字段读取 / 明文 key 形态（W78-2 加）。
	for _, rx := range adminUICredentialReadRx {
		if loc := rx.FindString(s); loc != "" {
			bad("%s: 出现凭据字段读取形态 %q（零凭据字节：只准回索引与状态）", name, loc)
		}
	}

	// 全资产：无 DEMO 数据面、无外部资源、无来源 chip（[已有] / [未提供] 会撑破卡片头）。
	if strings.Contains(strings.ToUpper(s), "DEMO") {
		bad("%s: 含 DEMO 标记（实时面不得出现）", name)
	}
	for _, chip := range []string{"[已有]", "[未提供]", "[需新增]", "[设计态]"} {
		if strings.Contains(s, chip) {
			bad("%s: 含来源 chip %s（W75 起改走页脚「数据源」图例）", name, chip)
		}
	}
	if name == "admin.js" && strings.Contains(s, "— 未提供") {
		bad("%s: 缺失值拼成「— 未提供」重复表达（要么 —，要么未提供）", name)
	}
	for _, ref := range []string{`src="http`, `href="http`, `src='http`, "url(http"} {
		if strings.Contains(s, ref) {
			bad("%s: 含外部资源引用 %q", name, ref)
		}
	}
	// 两个 CSS 资产：选择器必须是纯 ASCII（注释提前被 */ 关闭时，注释文字会被吃成
	// 选择器——规则静默消失、页面上"样式莫名没了"，磁盘检查却全绿）。
	if name == "admin.css" || name == "controls.css" {
		for _, sel := range adminUICSSRuleSelectors(s) {
			if loc := adminUIRxNonASCII.FindString(sel); loc != "" {
				bad("%s: 规则选择器含非 ASCII 字符（CSS 注释被提前关闭，注释文字被吃成了选择器）：%q",
					name, strings.TrimSpace(sel)[:min(60, len(strings.TrimSpace(sel)))])
			}
		}
	}
	// CSP `style-src 'self'` 屏蔽内联 style 属性（实测 style-src-attr 违规：直方图
	// 柱高 / 预算 gauge 宽度全灭）。静态样式只准走 admin.css 的类名，动态尺寸只准
	// 走 CSSOM（el.style.x = …，不在该指令管辖内）——对页面与脚本都成立。
	if strings.Contains(s, `style="`) || strings.Contains(s, `style='`) {
		bad("%s: 含内联 style 属性（CSP style-src 'self' 会屏蔽它——改用类名 / CSSOM）", name)
	}

	switch name {
	case "admin.html":
		// 8 个页面容器（路由落点）。
		for _, page := range []string{"overview", "upstreams", "traffic", "sessions", "usage", "audit", "models", "config"} {
			if !strings.Contains(s, `id="page-`+page+`"`) {
				bad("admin.html: 缺页面容器 id=page-%s", page)
			}
		}
		// CSP：逐字一致（缺一条即红）。
		want := `default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'`
		if !strings.Contains(s, want) {
			bad("admin.html: CSP 不是要求的逐字策略")
		}
		// 无内联脚本：每个 <script 必须带 src=。
		for i, seg := range strings.Split(s, "<script") {
			if i == 0 {
				continue
			}
			end := strings.Index(seg, ">")
			if end < 0 {
				bad("admin.html: 未闭合的 <script 标签")
				continue
			}
			if !strings.Contains(seg[:end], "src=") {
				bad("admin.html: 存在内联 <script>")
			}
		}
		// 密钥纪律：页面不得引用 localStorage 存密钥。
		if strings.Contains(s, "localStorage.setItem") {
			bad("admin.html: 页面脚本不得使用 localStorage.setItem")
		}
		// W75 壳层锚点：面包屑 + 内容区 id（顶栏/内容层次检查的落点）。
		for _, anchor := range []string{`id="where-crumb"`, `id="main-content"`} {
			if !strings.Contains(s, anchor) {
				bad("admin.html: 缺壳层锚点 %s", anchor)
			}
		}
		// W77-2 启动层：首帧必须有 #boot，且页面里**不能**预先带 .boot-done（否则等于没启动层）。
		if !strings.Contains(s, `id="boot"`) {
			bad("admin.html: 缺启动层 #boot（首帧会直接露空壳）")
		}
		// W78-2：两条新页面（容器 + 导航项 + 导航计数 + 导航位置）。
		for _, page := range []string{"accounts", "providers"} {
			if !strings.Contains(s, `id="page-`+page+`"`) {
				bad("admin.html: 缺页面容器 id=page-%s", page)
			}
			if !strings.Contains(s, `href="#/`+page+`" data-page="`+page+`"`) {
				bad("admin.html: 缺导航项 href=#/%s", page)
			}
			if !strings.Contains(s, `id="nav-cnt-`+page+`"`) {
				bad("admin.html: 缺导航计数 id=nav-cnt-%s", page)
			}
		}
		if a, b := strings.Index(s, `data-page="upstreams"`), strings.Index(s, `data-page="accounts"`); a < 0 || b < 0 || b < a {
			bad("admin.html: 「账号与代理」导航不在「上游与池」之后")
		}
		if a, b := strings.Index(s, `data-page="models"`), strings.Index(s, `data-page="providers"`); a < 0 || b < 0 || b < a {
			bad("admin.html: 「服务商」导航不在「模型与任务」之后")
		}
		if adminUIRxBootDone.MatchString(s) {
			bad("admin.html: 启动层预置了 .boot-done（启动层被静态关掉 = 装饰）")
		}
		// W77-2 统一控件：按钮只准走 .ad-ctl*（两种按钮体系 = 又回到不统一）。
		if got := strings.Count(s, `class="ad-ctl`); got < 6 {
			bad("admin.html: .ad-ctl* 按钮只有 %d 个（顶栏/锁层/抽屉/对话框应全部统一到 .ad-ctl）", got)
		}
		if strings.Contains(s, `class="btn`) {
			bad("admin.html: 仍用旧 .btn 类（W77-2 起统一为 .ad-ctl*）")
		}
		// W79 控件单一来源：controls.css 必须先于 admin.css（令牌在 computed-value 时解析；
		// 顺序颠倒 = --ad-ctl-* 令牌桥指向未定义的 --ctl-*，控件会退回浏览器默认）。
		ctlLink, adLink := strings.Index(s, `href="controls.css"`), strings.Index(s, `href="admin.css"`)
		if ctlLink < 0 {
			bad("admin.html: 未加载 controls.css（控件几何会退回 admin.css 里的第二套定义）")
		} else if adLink < 0 || ctlLink > adLink {
			bad("admin.html: controls.css 必须排在 admin.css 之前（controls=%d admin=%d）", ctlLink, adLink)
		}
	case "admin-data.js":
		// 唯一网络面：带管理密钥头（断言装配点，不是只断言字样——注释里出现不算）。
		if !strings.Contains(s, "X-Management-Key") || !strings.Contains(s, `"X-Management-Key": getKey()`) {
			bad("admin-data.js: 缺 X-Management-Key 请求头装配（数据面会全 401）")
		}
		// 密钥只走 sessionStorage（键名逐字），且绝不落到 localStorage。
		if !strings.Contains(s, "sessionStorage") || !strings.Contains(s, "any2api.admin.key") {
			bad("admin-data.js: 密钥须存 sessionStorage 的 any2api.admin.key")
		}
		if strings.Contains(s, "localStorage.") {
			bad("admin-data.js: 不得触碰 localStorage（密钥越权持久面）")
		}
		// 不引 DEMO 数据源。
		if strings.Contains(s, "mock-data") || strings.Contains(s, "A2A_MOCK") {
			bad("admin-data.js: 仍引用原型 mock 数据源")
		}
		// 端点面：14 个只读端点 + 2 个写端点至少都要出现（适配层的硬契约）。
		for _, ep := range []string{
			"/admin/diagnostics", "/admin/pool", "/admin/logs", "/admin/sessions",
			"/admin/usage", "/admin/requests", "/admin/audit", "/admin/alerts",
			"/admin/metrics", "/admin/models", "/admin/tasks", "/admin/config", "/admin/system",
		} {
			if !strings.Contains(s, ep) {
				bad("admin-data.js: 未接端点 %s", ep)
			}
		}
		if !strings.Contains(s, `"/admin/requests/"`) {
			bad("admin-data.js: 未接请求详情端点 /admin/requests/{id}")
		}
		if !strings.Contains(s, "/admin/sessions/revoke") || !strings.Contains(s, "/admin/cooldowns/reset") {
			bad("admin-data.js: 未接写动作端点（revoke / cooldowns reset）")
		}
		// W78-2：账号/代理/服务商/逐模型用量 + 导出/冷却/探针三条动作面。
		for _, ep := range []string{"/admin/accounts", "/admin/proxies", "/admin/providers", "/admin/usage", "/admin/export", "/cooldown", "/probe"} {
			if !strings.Contains(s, ep) {
				bad("admin-data.js: 未接 W78 端点面 %s", ep)
			}
		}
		if !strings.Contains(s, `scope: "model"`) {
			bad("admin-data.js: 未取逐模型用量（/admin/usage?scope=model）")
		}
		for _, live := range []string{"LIVE.accounts", "LIVE.proxies", "LIVE.providers", "LIVE.modelUsage"} {
			if !strings.Contains(s, live) {
				bad("admin-data.js: LIVE 面缺 %s", live)
			}
		}
		if !strings.Contains(s, "Content-Disposition") {
			bad("admin-data.js: 下载面未读 Content-Disposition（导出文件名会丢）")
		}
		// 下载面：密钥只准走请求头（adminHeaders），**绝不准**进 URL。
		if seg := adminUIFuncSection(s, "adminDownload"); !strings.Contains(seg, "adminHeaders") || strings.Contains(seg, "getKey") {
			bad("admin-data.js: adminDownload 必须用 adminHeaders 带密钥，且不得把密钥拼进 URL")
		}
		// 冷却/探针必须是真发的 adminPOST（按函数体判，不看整文件字样）。
		for _, fn := range []string{"cooldown", "probe"} {
			seg := adminUIFuncSection(s, fn)
			if !strings.Contains(seg, "return adminPOST(") || !strings.Contains(seg, "/admin/") {
				bad("admin-data.js: %s 未走 adminPOST（动作必须真发 POST，不是本地模拟）", fn)
			}
		}
		if !strings.Contains(adminUIFuncSection(s, "cooldown"), "/cooldown") {
			bad("admin-data.js: 冷却动作路径缺 /cooldown 后缀")
		}
		if !strings.Contains(adminUIFuncSection(s, "probe"), "/probe") {
			bad("admin-data.js: 探针动作路径缺 /probe 后缀")
		}
	case "admin.js":
		if strings.Contains(s, "localStorage.") && strings.Contains(s, "any2api.admin.key") {
			bad("admin.js: 密钥与 localStorage 同现（密钥只准在适配层的 sessionStorage）")
		}
		if !strings.Contains(s, "REFRESH_MS") {
			bad("admin.js: 缺自动刷新间隔常量")
		}
		if !strings.Contains(s, "applyDynSizes") {
			bad("admin.js: 缺动态尺寸 CSSOM 装配（直方图柱高 / gauge 宽在 CSP 下只能走 CSSOM）")
		}
		// W75 总览契约：5 张 KPI、健康条、2/3+1/3 主网格、真图表、页脚数据源图例，且无来源 chip。
		ov := adminUIOverviewSection(s)
		if ov == "" {
			bad("admin.js: 找不到 renderOverview 段（总览布局契约无从检查）")
		} else {
			if got := strings.Count(ov, `kpi("`); got != 5 {
				bad("admin.js: 总览 KPI 卡 = %d，want 5（requests/success rate/failovers/TTFT p95/total p95）", got)
			}
			if strings.Contains(ov, "src(") {
				bad("admin.js: 总览仍调用来源 chip 生成器 src()（会撑破卡片头）")
			}
			for _, needle := range []string{"health-strip", "overview-grid", "trend-svg", "alerts-card", "events-card", "sourceLegend("} {
				if !strings.Contains(ov, needle) {
					bad("admin.js: 总览缺 %s", needle)
				}
			}
			for _, k := range []string{`kpi("requests"`, `kpi("success rate"`, `kpi("failovers"`, `kpi("TTFT p95"`, `kpi("total p95"`} {
				if !strings.Contains(ov, k) {
					bad("admin.js: 总览缺 KPI %s", k)
				}
			}
		}
		// W75 其他页：三处本地筛选栏（sessions/models/audit）必须有落点 id。
		for _, id := range []string{`id="s-q"`, `id="m-q"`, `id="a-q"`} {
			if !strings.Contains(s, id) {
				bad("admin.js: 缺筛选栏控件 %s", id)
			}
		}
		// W75 数据源图例（每页页脚的唯一来源标注面）。
		if !strings.Contains(s, "source-legend") || !strings.Contains(s, "数据源") {
			bad("admin.js: 缺页脚「数据源」图例（source-legend）")
		}
		// ---- W77-2：「登录/深链后白页」这条回归的门（真门，见 TestAdminUINegativeControl #8）----
		// 渲染条件必须包含 opts.initial：refreshAll 只在非 local 时渲染，而登录首装是
		// refreshAll({initial:true}) —— 少了 initial 分支，锁层一藏就是白页（实测 2026-10-18）。
		if !strings.Contains(s, "if (!opts.local || opts.initial)") {
			bad("admin.js: refreshAll 的渲染条件缺 opts.initial（登录/深链首装会白页）")
		}
		if strings.Contains(s, "refreshAll({ initial: true, local: true })") {
			bad("admin.js: 登录首装又带上了 local:true（= 只装数据不渲染，白页根因）")
		}
		if !strings.Contains(s, "refreshAll({ initial: true })") {
			bad("admin.js: submitKey 未走 refreshAll({ initial: true }) 首装渲染")
		}
		// 渲染条件必须在 refreshAll 内（防"条件写在别处、这里漏渲染"）。
		if seg := adminUIFuncSection(s, "refreshAll"); !strings.Contains(seg, "opts.initial") {
			bad("admin.js: refreshAll 函数体里没有 opts.initial 分支")
		}
		// ---- W77-2：启动层 ----
		if !strings.Contains(s, "BOOT_MIN_MS = 300") {
			bad("admin.js: 启动层最短展示不是 300ms（BOOT_MIN_MS = 300）")
		}
		for _, needle := range []string{"bootShow(", "bootHide(", `classList.add("boot-done")`, "__a2aAdminBoot", `matchMedia("(prefers-reduced-motion: reduce)")`} {
			if !strings.Contains(s, needle) {
				bad("admin.js: 启动层缺 %s", needle)
			}
		}
		// 落锁/管理面未启用两条路径也必须收启动层（否则锁层被永久盖住）。
		if !strings.Contains(adminUIFuncSection(s, "lockUI"), "bootHide()") {
			bad("admin.js: lockUI 未收启动层（锁层会被启动层永久盖住）")
		}
		// ---- W77-2：切页动效 + 统一控件 ----
		if !strings.Contains(s, "page-in") || !strings.Contains(s, "function pageIn(") {
			bad("admin.js: 缺切页动效（pageIn / .page-in）")
		}
		if strings.Count(s, `class="ad-ctl`) < 8 {
			bad("admin.js: .ad-ctl* 按钮过少（控件未统一）")
		}
		if strings.Contains(s, `class="btn`) {
			bad("admin.js: 仍用旧 .btn 类（W77-2 起统一为 .ad-ctl*）")
		}
		// ---- W77-2：每页都要有显式空态/未提供面（空数据 ≠ 空白 section）----
		if got := strings.Count(s, "unavail("); got < 12 {
			bad("admin.js: 显式「未提供」渲染点只有 %d 处（每页都要有，空数据不得留白）", got)
		}
		for _, needle := range []string{"class='empty'", "if (!healthRows) healthRows =", "if (!credRows) credRows ="} {
			if !strings.Contains(s, needle) {
				bad("admin.js: 缺空表占位 %s（空数据时表体会是空的）", needle)
			}
		}
		// ---- W78-2：两条新页面的路由 / 渲染 / 动作 / 显式空态 ----
		for _, needle := range []string{
			`accounts: ["账号与代理", renderAccounts]`,
			`providers: ["服务商", renderProviders]`,
			"function renderAccounts(", "function renderProviders(",
			"function exportBar(", `data-act="export"`, `data-act="cooldown"`, `data-act="probe"`,
			"readinessBadge(", "probeText(", "cred-chip ",
		} {
			if !strings.Contains(s, needle) {
				bad("admin.js: 缺 W78 渲染/动作面 %q", needle)
			}
		}
		for _, want := range []string{"无上游", "未配置任何上游", "本日无模型用量"} {
			if !strings.Contains(s, want) {
				bad("admin.js: 缺显式空态文案 %q（空数据不得留白）", want)
			}
		}
		// 导出/冷却/探针三条动作必须在 handleAction 里真发（不是 disabled 占位），
		// 且都排在「未实现」兜底之前（落到兜底 = 动作没接）。
		segAct := adminUIFuncSection(s, "handleAction")
		fallback := strings.Index(segAct, "未实现：管理面没有该动作的端点（不假装成功）")
		for _, act := range []string{`act === "export"`, `act === "cooldown"`, `act === "probe"`} {
			at := strings.Index(segAct, act)
			if at < 0 || fallback < 0 || at > fallback {
				bad("admin.js: handleAction 缺 %s 真动作分支（或落在「未实现」兜底之后）", act)
			}
		}
		if !strings.Contains(s, "已冷却 #") || !strings.Contains(s, "已导出 ") {
			bad("admin.js: 缺动作结果 toast 文案（冷却/导出）")
		}
	case "admin.css":
		if !strings.Contains(s, "--ad-nav-w: 240px") || !strings.Contains(s, "--ad-topbar-h: 56px") {
			bad("admin.css: 缺 240px nav / 56px topbar 密度令牌")
		}
		// W75 布局契约：24px gutter、1440 内容宽、5 卡 KPI（auto-fit 190px）、
		// 2/3 + 1/3 概览网格、≤1100 折叠、宽表只在卡内横向滚动。
		for _, needle := range []string{
			"--ad-gutter: 24px",
			"--ad-max: 1440px",
			"max-width: var(--ad-max)",
			".kpi-grid",
			"minmax(190px, 1fr)",
			".overview-grid",
			"minmax(0, 2fr) minmax(0, 1fr)",
			"@media (max-width: 1100px)",
			".table-wrap",
			"overflow-x: auto",
			"min-width: 0",
		} {
			if !strings.Contains(s, needle) {
				bad("admin.css: 缺布局契约 %q", needle)
			}
		}
		if strings.Contains(s, "grid-template-columns: repeat(7") {
			bad("admin.css: 出现固定 7 列网格（KPI 会裁掉第 6/7 张卡）")
		}
		// ---- W79：控件几何/四态的唯一来源是 controls.css；本文件只留令牌桥 ----
		for _, needle := range []string{
			"--ad-ctl-h: var(--ctl-h, 28px)",
			"--ad-ctl-h-lg: var(--ctl-h-lg, 32px)",
			"--ad-ctl-h-sm: var(--ctl-h-sm, 24px)",
			"--ad-ctl-r: var(--ctl-r, 8px)",
			"--ad-ctl-r-lg: var(--ctl-r-pop, 10px)",
			"--ad-focus-ring:",
			"height: var(--ad-ctl-h)",
			"border-radius: var(--ad-ctl-r)",
			"border: 1px solid var(--ad-border-strong)",
		} {
			if !strings.Contains(s, needle) {
				bad("admin.css: 缺控件令牌桥 %q（控件几何必须由 controls.css 提供）", needle)
			}
		}
		// 负控可红：本文件不得再出现 .ad-ctl*/.ad-seg 的**规则选择器**（注释/令牌不算）。
		for _, class := range []string{".ad-ctl", ".ad-seg"} {
			if adminUICSSDefinesClass(adminUICSSRuleSelectors(s), class) {
				bad("admin.css: 出现第二套 %s 几何定义（控件单一来源 = controls.css）", class)
			}
		}
		// 负控可红：旧 .btn 体系不得复活。
		if strings.Contains(s, ".btn {") || strings.Contains(s, ".btn.sm") {
			bad("admin.css: 仍定义旧 .btn（两套按钮体系 = 不统一）")
		}
		// ---- W77-2：启动层 + 切页动效 + 减少动效偏好 ----
		for _, needle := range []string{
			".boot {",
			".boot.boot-done",
			"@keyframes ad-boot-slide",
			".page-in",
			"@keyframes ad-page-in",
			"@media (prefers-reduced-motion: reduce)",
		} {
			if !strings.Contains(s, needle) {
				bad("admin.css: 缺启动层/切页契约 %q", needle)
			}
		}
		// ---- W78-2：账号/服务商的数据形态（chip、导出条、服务商卡片格、动作格）----
		for _, needle := range []string{
			".cred-chip", ".cred-chips", ".export-bar", ".export-label",
			".provider-grid", ".provider-card", ".provider-row", ".cap-chip", ".act-cell",
		} {
			if !strings.Contains(s, needle) {
				bad("admin.css: 缺 W78 数据形态类 %q", needle)
			}
		}
		// 没有第三套按钮体系：新面只准复用 .ad-ctl*（不得新造控件类前缀）。
		for _, rogue := range []string{".pbtn", ".ad-btn", ".ctl-btn"} {
			if strings.Contains(s, rogue) {
				bad("admin.css: 出现第二套按钮类 %q（W78 复用 .ad-ctl*，不新增控件体系）", rogue)
			}
		}
	case "controls.css":
		// ---- W79 控件单一来源：聊天 .ctl* 与管理面别名 .ad-ctl*/.ad-seg 必须同文件 ----
		sel := adminUICSSRuleSelectors(s)
		for _, class := range []string{
			".ctl", ".ctl-sm", ".ctl-lg", ".ctl-select", ".ctl-slider", ".ctl-seg", ".ctl-seg-item", ".ctl-btn", ".ctl-menu",
			".ad-ctl", ".ad-ctl.sm", ".ad-ctl.lg", ".ad-ctl.primary", ".ad-ctl.danger",
			".ad-ctl.ghost", ".ad-ctl.unimpl", ".ad-ctl.cd-sel", ".ad-seg",
		} {
			if !adminUICSSDefinesClass(sel, class) {
				bad("controls.css: 缺控件规则 %s（两面控件几何都必须收敛在这一处）", class)
			}
		}
		// 共享刻度 + 令牌（28 基座 / 32 大 / 24 紧凑 / 8·10px 圆角 / 1px 边框 / 四态）。
		for _, needle := range []string{
			"--ctl-h: var(--df-ctl-h, 28px)",
			"--ctl-h-lg: var(--df-ctl-h-lg, 32px)",
			"--ctl-h-sm: var(--df-ctl-h-nested, 24px)",
			"--ctl-r: var(--df-ctl-radius, 8px)",
			"--ctl-r-pop: var(--df-pop-radius, 10px)",
			"--ctl-border-w: 1px",
			"--ctl-disabled-opacity: .45",
			"height: var(--ad-ctl-h, var(--ctl-h))",
			"height: calc(var(--ad-ctl-h, var(--ctl-h)) - 4px)",
			"var(--ad-ctl-h-sm, var(--ctl-h-sm))",
			"var(--ad-ctl-h-lg, var(--ctl-h-lg))",
			"var(--ad-ctl-r, var(--ctl-r))",
			"var(--ad-ctl-r-lg, var(--ctl-r))",
			"var(--ad-surface, var(--ctl-bg))",
			"var(--ad-border-strong, var(--ctl-border-strong))",
			"var(--ad-accent, var(--ctl-accent))",
			"var(--ad-focus-ring, var(--ctl-focus))",
		} {
			if !strings.Contains(s, needle) {
				bad("controls.css: 缺共享刻度/令牌 %q", needle)
			}
		}
		// 两面四态齐全（少一个状态 = 又分两套）。
		for _, needle := range []string{
			".ctl:hover", ".ctl:active", ".ctl:focus-visible", ".ctl:disabled",
			".ad-ctl:hover", ".ad-ctl:active", ".ad-ctl:focus-visible", ".ad-ctl[disabled]",
			".ad-seg button:hover", `.ad-seg button[aria-selected="true"]`,
		} {
			if !strings.Contains(s, needle) {
				bad("controls.css: 缺控件状态 %q（四态必须两面一致）", needle)
			}
		}
		// 管理面别名必须带聊天令牌兜底（否则 controls.css 单独加载时别名会失效）。
		if !strings.Contains(s, "var(--ad-ctl-h, var(--ctl-h))") {
			bad("controls.css: .ad-ctl 高度缺 --ctl-h 兜底（聊天面无 --ad-* 时会失高）")
		}
	}
	return p
}

// adminUIOverviewSection 截出总览段（从「总览」分隔注释到「上游与池」分隔注释）：
// 契约检查覆盖 KPI/健康条/趋势/告警/事件的 helper 与 renderOverview 本体，
// 避免只截 renderOverview 时把 helper 内的类名误判为缺失。
func adminUIOverviewSection(s string) string {
	const startMark = "/* ---------------------------------------------------------------- 总览 */"
	const endMark = "/* ---------------------------------------------------------------- 上游与池 */"
	start := strings.Index(s, startMark)
	if start < 0 {
		return ""
	}
	end := strings.Index(s[start:], endMark)
	if end < 0 {
		return s[start:]
	}
	return s[start : start+end]
}

// adminUIFuncSection 截出 `function <name>(` 到下一个顶层 `function ` 之间的片段
// （用于"这个函数体里到底有没有那条分支"的检查，避免全文件 Contains 把别处命中当好）。
func adminUIFuncSection(s, name string) string {
	start := strings.Index(s, "function "+name+"(")
	if start < 0 {
		return ""
	}
	rest := s[start:]
	if end := strings.Index(rest[1:], "\n  function "); end >= 0 {
		return rest[:end+1]
	}
	return rest
}

func TestAdminUIStaticContract(t *testing.T) {
	for _, name := range []string{"admin.html", "admin.css", "controls.css", "admin.js", "admin-data.js"} {
		body := adminUIAsset(t, name)
		if problems := adminUIStaticProblems(name, body); len(problems) != 0 {
			t.Errorf("静态契约不通过 %s：\n  - %s", name, strings.Join(problems, "\n  - "))
		}
	}
}

var (
	adminUIRxIDAttr  = regexp.MustCompile(`id="([^"]+)"`)
	adminUIRxIDSq    = regexp.MustCompile(`id='([^']+)'`)
	adminUIRxSelQ    = regexp.MustCompile(`\$\("#([A-Za-z0-9_-]+)"\)`)
	adminUIRxGetByID = regexp.MustCompile(`getElementById\("([A-Za-z0-9_-]+)"\)`)
	// 启动层是否被静态关掉（只看 class 属性，不误伤注释里提到的 .boot-done）。
	adminUIRxBootDone = regexp.MustCompile(`class="[^"]*boot-done`)
	// CSS 选择器前导（去注释后取每个 `{` 之前的文本；@media/@keyframes 的 at 头会被排除）。
	adminUIRxCSSComment = regexp.MustCompile(`(?s)/\*.*?\*/`)
	adminUIRxCSSPrelude = regexp.MustCompile(`(?s)([^{}]+)\{`)
	// 选择器里的非 ASCII = CSS 注释被提前关闭的典型症状（注释文字被吃成选择器，
	// 规则静默消失：实测 2026-10-19 `.ad-ctl*/.ad-seg` 的 `*/` 吃掉了 .ad-ctl 基座）。
	adminUIRxNonASCII = regexp.MustCompile(`[^\x00-\x7F]`)
)

// adminUICSSRuleSelectors 返回 CSS 里每条规则的选择器前导（已去注释）。
// 判「某个类是否被定义过」必须看这里：注释里提到、或 --token 声明里出现，都不算规则。
func adminUICSSRuleSelectors(css string) []string {
	plain := adminUIRxCSSComment.ReplaceAllString(css, "")
	var out []string
	for _, m := range adminUIRxCSSPrelude.FindAllStringSubmatch(plain, -1) {
		sel := strings.TrimSpace(m[1])
		if sel == "" || strings.HasPrefix(sel, "@") {
			continue
		}
		out = append(out, sel)
	}
	return out
}

// adminUICSSDefinesClass 选择器集合里是否有规则真的写了该类（全词匹配：".ad-ctl" 命中
// ".ad-ctl:hover" / ".ad-ctl.sm"，但不命中 ".ad-ctl-x"、也不把 ".ad-ctl-h" 令牌算进来）。
func adminUICSSDefinesClass(selectors []string, class string) bool {
	rx := regexp.MustCompile(regexp.QuoteMeta(class) + `([^A-Za-z0-9_-]|$)`)
	for _, sel := range selectors {
		if rx.MatchString(sel) {
			return true
		}
	}
	return false
}

// adminUIMissingIDs admin.js 里用到的 DOM id 是否都在 html/js 里定义过（含 render
// 函数内联生成的 HTML）。选择器打错 = 按钮点了没反应、磁盘上却全绿——这类静默失败
// 只有把两侧摆在一起才能发现。
func adminUIMissingIDs(html, js string) []string {
	defined := map[string]bool{}
	for _, rx := range []*regexp.Regexp{adminUIRxIDAttr, adminUIRxIDSq} {
		for _, m := range rx.FindAllStringSubmatch(html+"\n"+js, -1) {
			defined[m[1]] = true
		}
	}
	seen := map[string]bool{}
	var missing []string
	for _, rx := range []*regexp.Regexp{adminUIRxSelQ, adminUIRxGetByID} {
		for _, m := range rx.FindAllStringSubmatch(js, -1) {
			if defined[m[1]] || seen[m[1]] {
				continue
			}
			seen[m[1]] = true
			missing = append(missing, m[1])
		}
	}
	sort.Strings(missing)
	return missing
}

func TestAdminUIDOMIdWiring(t *testing.T) {
	html, js := string(adminUIAsset(t, "admin.html")), string(adminUIAsset(t, "admin.js"))
	if missing := adminUIMissingIDs(html, js); len(missing) != 0 {
		t.Fatalf("admin.js 引用了未定义的 DOM id：%v", missing)
	}
	// 负控：删掉一个容器 id → 门必须红（否则这条门是摆设）。
	mutant := strings.Replace(html, `id="nav-build"`, `id="nav-build-x"`, 1)
	if mutant == html {
		t.Fatal("负控变异未生效：html 里找不到 id=nav-build")
	}
	if missing := adminUIMissingIDs(mutant, js); len(missing) == 0 {
		t.Fatal("负控：删掉 id=nav-build 后接线门仍绿——门是装饰")
	} else if !strings.Contains(strings.Join(missing, ","), "nav-build") {
		t.Fatalf("负控：门红了但没点名 nav-build（实得 %v）", missing)
	}
}

// TestAdminUINegativeControl 负控：7 个手工缺陷必须各自让静态门变红。
// 证明门不是装饰（真资产绿 + 每个变体红，两侧都跑到）。
func TestAdminUINegativeControl(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(string) string
		wantSub string
	}{
		{
			name: "admin-data.js",
			mutate: func(s string) string {
				return strings.Replace(s, `"X-Management-Key": getKey()`, `"X-Not-A-Key": getKey()`, 1)
			},
			wantSub: "X-Management-Key",
		},
		{
			name: "admin-data.js",
			mutate: func(s string) string {
				return strings.ReplaceAll(s, "sessionStorage.", "localStorage.")
			},
			wantSub: "localStorage",
		},
		{
			name: "admin.html",
			mutate: func(s string) string {
				return strings.Replace(s, `id="page-audit"`, `id="page-audit-x"`, 1)
			},
			wantSub: "page-audit",
		},
		{
			name: "admin.html",
			mutate: func(s string) string {
				return strings.Replace(s, "frame-ancestors 'none'", "frame-ancestors *", 1)
			},
			wantSub: "CSP",
		},
		{
			// W75 布局负控 #1：KPI 网格退回固定 7 列（截图里的裁切 bug）→ 门必须红。
			name: "admin.css",
			mutate: func(s string) string {
				return strings.Replace(s, "grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));", "grid-template-columns: repeat(7, 1fr);", 1)
			},
			wantSub: "minmax(190px, 1fr)",
		},
		{
			// W75 布局负控 #2：总览卡片头重新挂来源 chip → 门必须红（"[已有]" 会撑破卡片）。
			name: "admin.js",
			mutate: func(s string) string {
				return strings.Replace(s, "function renderOverview() {", `function renderOverview() { return '<span class="src">[已有] GET /admin/diagnostics</span>';`, 1)
			},
			wantSub: "[已有]",
		},
		{
			// W75 布局负控 #3：删掉一张 KPI（5 → 4）→ 门必须红并点名 KPI 数。
			name: "admin.js",
			mutate: func(s string) string {
				return strings.Replace(s, `kpi("requests"`, `kpiX("requests"`, 1)
			},
			wantSub: "KPI 卡 = 4",
		},
		// ---- W77-2 负控：白页回归 / 启动层 / 控件统一 / 空态 四条门都要真能红 ----
		{
			// 白页负控 #1：把渲染条件退回"只非 local 才渲染"（= 实测的白页根因）。
			name: "admin.js",
			mutate: func(s string) string {
				return strings.Replace(s, "if (!opts.local || opts.initial) {", "if (!opts.local) {", 1)
			},
			wantSub: "opts.initial",
		},
		{
			// 白页负控 #2：登录首装又带上 local:true（只装数据不渲染）→ 门必须红。
			name: "admin.js",
			mutate: func(s string) string {
				return strings.Replace(s, "refreshAll({ initial: true })", "refreshAll({ initial: true, local: true })", 1)
			},
			wantSub: "local:true",
		},
		{
			// 启动层负控：最短展示时长退回 0（启动层一闪而过 = 装饰）→ 门必须红。
			name: "admin.js",
			mutate: func(s string) string {
				return strings.Replace(s, "BOOT_MIN_MS = 300", "BOOT_MIN_MS = 0", 1)
			},
			wantSub: "启动层",
		},
		{
			// 启动层负控：页面预置 .boot-done（等于没有启动层）→ 门必须红。
			name: "admin.html",
			mutate: func(s string) string {
				return strings.Replace(s, `class="boot" id="boot"`, `class="boot boot-done" id="boot"`, 1)
			},
			wantSub: "boot-done",
		},
		{
			// 控件负控：退回两套按钮体系（.btn）→ 门必须红。
			name: "admin.js",
			mutate: func(s string) string {
				return strings.Replace(s, `class="ad-ctl sm"`, `class="btn sm"`, 1)
			},
			wantSub: "ad-ctl",
		},
		{
			// 控件负控：控件高度令牌退回 30px（与聊天前端 28/32/24 不一致）→ 门必须红。
			name: "admin.css",
			mutate: func(s string) string {
				return strings.Replace(s, "--ad-ctl-h: var(--ctl-h, 28px)", "--ad-ctl-h: var(--ctl-h, 30px)", 1)
			},
			wantSub: "--ad-ctl-h: var(--ctl-h, 28px)",
		},
		{
			// 空态负控：删掉逐凭据池的空表占位（空数据时表体会是空的）→ 门必须红。
			name: "admin.js",
			mutate: func(s string) string {
				return strings.Replace(s, "if (!credRows)", "if (false)", 1)
			},
			wantSub: "credRows",
		},
		// ---- W79 控件收敛负控：顺序 / 第二套几何 / 单一来源、四条门都要真能红 ----
		{
			// 顺序负控：admin.css 排到 controls.css 之前（令牌桥指向未定义变量）→ 门必须红。
			// 用「改名占位」互换，避免行尾差异（CRLF/LF）让变异静默失效。
			name: "admin.html",
			mutate: func(s string) string {
				m := strings.Replace(s, `href="controls.css"`, `href="__swap__.css"`, 1)
				m = strings.Replace(m, `href="admin.css"`, `href="controls.css"`, 1)
				return strings.Replace(m, `href="__swap__.css"`, `href="admin.css"`, 1)
			},
			wantSub: "controls.css 必须排在 admin.css 之前",
		},
		{
			// 单一来源负控：admin.css 里重新长出 .ad-ctl 规则块 → 门必须红。
			name: "admin.css",
			mutate: func(s string) string {
				return strings.Replace(s, `input[type="text"]`, ".ad-ctl { height: 30px; }\n"+`input[type="text"]`, 1)
			},
			wantSub: "第二套 .ad-ctl",
		},
		{
			// 单一来源负控：controls.css 抹掉全部 .ad-ctl 别名（管理面控件失样式）→ 门必须红。
			name: "controls.css",
			mutate: func(s string) string {
				return strings.ReplaceAll(s, ".ad-ctl", ".adctl")
			},
			wantSub: "缺控件规则 .ad-ctl",
		},
		{
			// 刻度负控：controls.css 的紧凑档偏离 24px（= 又和聊天面分两套）→ 门必须红。
			name: "controls.css",
			mutate: func(s string) string {
				return strings.Replace(s, "--ctl-h-sm: var(--df-ctl-h-nested, 24px)", "--ctl-h-sm: var(--df-ctl-h-nested, 28px)", 1)
			},
			wantSub: "--ctl-h-sm",
		},
		{
			// 注释提前关闭负控：注释里写出 `*/` 相邻（实测 2026-10-19 把 .ad-ctl 基座整条规则
			// 吃成"选择器"，页面上控件没样式、磁盘检查却全绿）→ 门必须红。
			name: "controls.css",
			mutate: func(s string) string {
				return strings.Replace(s, " * 7. 管理面别名", " * 7. 管理面别名 .ad-ctl*/.ad-seg", 1)
			},
			wantSub: "非 ASCII",
		},
	}
	for i, tc := range cases {
		orig := string(adminUIAsset(t, tc.name))
		mutant := tc.mutate(orig)
		if mutant == orig {
			t.Fatalf("负控 #%d（%s）变异未生效——样本失效，门没被真正考验", i+1, tc.name)
		}
		if problems := adminUIStaticProblems(tc.name, []byte(orig)); len(problems) != 0 {
			t.Fatalf("负控 #%d：真资产本应绿，实得 %v", i+1, problems)
		}
		problems := adminUIStaticProblems(tc.name, []byte(mutant))
		if len(problems) == 0 {
			t.Fatalf("负控 #%d（%s）：变异后门仍绿——门是装饰", i+1, tc.name)
		}
		joined := strings.Join(problems, " | ")
		if !strings.Contains(joined, tc.wantSub) {
			t.Fatalf("负控 #%d：门红了但没点名 %q（实得 %s）", i+1, tc.wantSub, joined)
		}
	}
}

// ---------------------------------------------------------------------------
// 3. 鉴权矩阵
// ---------------------------------------------------------------------------

func TestAdminUIAuthMatrix(t *testing.T) {
	_, srv := startServiceWith(t, adminUIConfig(t, "k-test"))

	// 无密钥 → 401（管理面已注册）+ no-store。
	resp := adminUIRequest(t, srv.URL+"/admin/requests", adminUINoHeaders())
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("/admin/requests 无密钥 status = %d, want 401", resp.StatusCode)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("/admin/requests Cache-Control = %q, want no-store", cc)
	}
	if body := uiBody(t, resp); !strings.Contains(string(body), "authentication_error") {
		t.Fatalf("/admin/requests 401 体 = %s, want authentication_error 信封", body)
	}

	// 错密钥 → 401。
	resp = adminUIRequest(t, srv.URL+"/admin/requests", map[string]string{"X-Management-Key": "wrong"})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("错 X-Management-Key status = %d, want 401", resp.StatusCode)
	}
	_ = uiBody(t, resp)

	// X-Management-Key 正确 → 200（UI 的默认头）。
	resp = adminUIRequest(t, srv.URL+"/admin/requests?limit=5", map[string]string{"X-Management-Key": "k-test"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("正 X-Management-Key status = %d, want 200", resp.StatusCode)
	}
	if body := uiBody(t, resp); !strings.Contains(string(body), `"rows"`) {
		t.Fatalf("正 X-Management-Key 体 = %s, want rows 字段", body)
	}

	// Authorization: Bearer 正确 → 200（第二条可用路径）。
	resp = adminUIRequest(t, srv.URL+"/admin/diagnostics", map[string]string{"Authorization": "Bearer k-test"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Bearer status = %d, want 200", resp.StatusCode)
	}
	_ = uiBody(t, resp)

	// store 未启用：/admin/audit 带密钥 → 503 store_unavailable（UI 渲染"store 未启用"卡片）。
	resp = adminUIRequest(t, srv.URL+"/admin/audit", map[string]string{"X-Management-Key": "k-test"})
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/admin/audit（store 关）status = %d, want 503", resp.StatusCode)
	}
	if body := uiBody(t, resp); !strings.Contains(string(body), "store_unavailable") {
		t.Fatalf("/admin/audit 503 体 = %s, want store_unavailable", body)
	}

	// 静态资产仍是公开面（无密钥）：管理面 401 不影响 /ui/admin.html。
	resp = adminUIRequest(t, srv.URL+"/ui/admin.html", adminUINoHeaders())
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/ui/admin.html 无密钥 status = %d, want 200（静态面公开）", resp.StatusCode)
	}
	_ = uiBody(t, resp)
}

// TestAdminUINoKey404 未配 admin_key：管理面整族 404（路由不注册），静态页照常 200。
func TestAdminUINoKey404(t *testing.T) {
	_, srv := startServiceWith(t, adminUIConfig(t, ""))
	for _, path := range []string{"/admin/requests", "/admin/diagnostics", "/admin/audit", "/admin/system"} {
		resp := adminUIRequest(t, srv.URL+path, map[string]string{"X-Management-Key": "any"})
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s（未配 admin_key）status = %d, want 404", path, resp.StatusCode)
		}
		_ = uiBody(t, resp)
	}
	resp := adminUIRequest(t, srv.URL+"/ui/admin.html", adminUINoHeaders())
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/ui/admin.html status = %d, want 200", resp.StatusCode)
	}
	_ = uiBody(t, resp)
}

// ---------------------------------------------------------------------------
// 4. 适配层行为（node 驱动；无 node 则跳过）
// ---------------------------------------------------------------------------

// adminUINodeDriver 用 node 跑适配层：桩掉 fetch/sessionStorage/localStorage，
// 断言"每个 /admin/* 请求都带 X-Management-Key、401 判 unauthorized、不触
// localStorage、metrics 桶 → 序列"。所有断言在 Node 侧收集为 JSON 回给 Go。
const adminUINodeDriver = `
"use strict";
const fs = require("fs");
const adapterPath = process.argv[2];
const calls = [];
let store = {};
let localStorageTouched = false;
let mode = "ok";

global.window = {};
global.sessionStorage = {
  getItem: (k) => (Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null),
  setItem: (k, v) => { store[k] = String(v); },
  removeItem: (k) => { delete store[k]; }
};
global.localStorage = new Proxy({}, { get: () => { localStorageTouched = true; throw new Error("localStorage must never be read"); } });

function canned(url, opts) {
  const method = (opts && opts.method) || "GET";
  const h = (opts && opts.headers) || {};
  // 服务端语义（admin.go 的 adminAuth）：没有有效密钥 = 401。桩按同一形状回答，
  // 这样"删掉请求头"这类负控会**真的**把 load() 打回 unauthorized。
  if (!Object.prototype.hasOwnProperty.call(h, "X-Management-Key") || h["X-Management-Key"] !== "k-test") {
    return { status: 401, body: { error: { type: "authentication_error", message: "invalid or missing admin key" } } };
  }
  if (mode === "unauthorized") return { status: 401, body: { error: { type: "authentication_error", message: "invalid or missing admin key" } } };
  if (url.startsWith("/admin/sessions/revoke")) return { status: 200, body: { revoked: 1, matched: 1 } };
  if (url.startsWith("/admin/cooldowns/reset")) return { status: 200, body: { status: "ok", count: 1, reset: [{ index: 0, name: "fake", was_cooling: true }] } };
  if (url.startsWith("/admin/metrics")) return { status: 200, body: {
    window_seconds: 60, step_seconds: 60, now: 1700000120000,
    edges_ms: [0, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, "+Inf"],
    buckets: [
      { ts: 1700000000000, requests: 1, errors: 0, failovers: 0, ttft_ms: { p50: 100, p95: 200 }, total_ms: { p50: 300, p95: 400 }, ttft_hist: [], total_hist: [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] },
      { ts: 1700000060000, requests: 2, errors: 1, failovers: 1, ttft_ms: { p50: 110, p95: 210 }, total_ms: { p50: 310, p95: 410 }, ttft_hist: [], total_hist: [0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] }
    ] } };
  if (url.startsWith("/admin/audit")) return { status: 503, body: { error: { type: "store_unavailable", message: "store is not enabled" } } };
  if (url.startsWith("/admin/sessions")) return { status: 200, body: { sessions: [{ sid_prefix: "9f3c71ab", key_id: "3106c924", revoked: false, expires_at: 4700000000000 }], next_cursor: "cur-1", now: 1700000000000 } };
  if (url.startsWith("/admin/usage")) return { status: 200, body: { day: "2026-10-18", scope: "global", rows: [], totals: { requests: 0, cost_units: 0 }, now: 1700000000000 } };
  if (url.startsWith("/admin/models")) return { status: 200, body: { configured: true, models: [{ id: "m1", upstream_model: null, max_input_tokens: null, max_tokens: null, usage: { available: false } }], now: 1700000000000 } };
  if (url.startsWith("/admin/tasks")) return { status: 200, body: { tasks: [{ id: "resp_1", model: "m1", status: "completed", created_at: 1700000000, terminal_at: null, delivered: true, attempts: null, upstream: null, face: "unknown" }], now: 1700000000000 } };
  if (url.startsWith("/admin/config")) return { status: 200, body: { now: 1700000000000, listen: "127.0.0.1:18795", admin: { key_configured: true, log_capacity: 256 }, ui: { enabled: true, rate_limit_per_min: 30 }, store: { enabled: false, path: "" }, public: { enabled: false, access_mode: "open", access_keys_count: 0, trusted_proxies: [], limits: {} }, models: [], upstreams: [], limits: {}, debug: {}, task_face_degrade: "off" } };
  if (url.startsWith("/admin/system")) return { status: 200, body: { now: 1700000000000, runtime: { go_version: "go1.26.7", pid: 1, goroutines: 2, mem_alloc_bytes: 3, mem_sys_bytes: 4, gc_count: 5, gc_pause_total_ms: 6, uptime_seconds: 7 }, store: { enabled: false, path: "", size_bytes: 0, size_available: false, sessions: 0, sessions_available: false, audit_rows: 0, audit_rows_available: false }, build: { commit: "abc1234", date: "2026-10-18T00:00:00Z", go_version: "go1.26.7" } } };
  if (url.startsWith("/admin/alerts")) return { status: 200, body: { active: [{ id: "a1", severity: "warning", name: "pool_low", object: "fake", message: "m", since: 1700000000000, runbook: "rb" }], history: [] } };
  if (url.startsWith("/admin/requests/")) return { status: 404, body: { error: { type: "not_found", message: "unknown request id" } } };
  if (url.startsWith("/admin/requests")) return { status: 200, body: { rows: [{ time: 1700000000000, request_id: "req_1", entry: "chat", model: "m1", stream: true, status: 200, attempts: 1, failovers: 0, upstream: "fake", upstreams: ["fake"], credential_index: null, ttft_ms: 100, total_ms: 200, in: 5, out: 6, outcome: "completed", sid_prefix: "9f3c71ab", key_id: "3106c924", task: "", failed: false, delivered: true }], next_cursor: "" } };
  if (url.startsWith("/admin/logs")) return { status: 200, body: { entries: [], capacity: 256, count: 0, latest_seq: 0, next_cursor: "c" } };
  if (url.startsWith("/admin/pool")) return { status: 200, body: { upstreams: [{ index: 0, name: "fake", protocol: "anthropic", pool_size: 1, credentials: [{ index: 0, cooling: false, cooldown_until: 0, cooldown_remaining_seconds: 0, last_used: 0, busy: false }] }], capacity: { daily_units_budget: 10, used_today: 1, remaining: 9, inflight: 0, queue_len: 0, max_inflight: 2 }, now: 1700000000000 } };
  if (url.startsWith("/admin/diagnostics")) return { status: 200, body: { now: "2026-10-18T00:00:00Z", diagnostics: { requests: 5, attempts: 6, failovers: 1, cooldown_skips: 0, tasks_created: 1, tasks_terminal_completed: 1, tasks_terminal_failed: 0, tasks_terminal_incomplete: 0, tasks_terminal_cancelled: 0, tasks_idem_replays: 0, attempt_usage_failed_records: 1 }, upstreams: [{ index: 0, name: "fake", protocol: "anthropic", cooling: false, cooldown_remaining_seconds: 0, consecutive_failures: 0, ladder_step: 0, window_requests: 1, window_failures: 0 }], log: { capacity: 256, count: 0, latest_seq: 0 }, admin: { auth_failures: 0, tracked_ips: 0, banned_ips: [] } } };
  return { status: 404, body: { error: { type: "not_found", message: "unexpected " + method + " " + url } } };
}

global.fetch = (url, opts) => {
  const u = String(url);
  const h = (opts && opts.headers) || {};
  calls.push({ url: u, method: (opts && opts.method) || "GET", key: Object.prototype.hasOwnProperty.call(h, "X-Management-Key") ? h["X-Management-Key"] : null, body: (opts && opts.body) || null });
  const r = canned(u, opts);
  return Promise.resolve({ ok: r.status >= 200 && r.status < 300, status: r.status, text: () => Promise.resolve(JSON.stringify(r.body)) });
};

require(adapterPath);
const A2AAdmin = global.window.A2AAdmin;
if (!A2AAdmin) { console.error("adapter did not expose window.A2AAdmin"); process.exit(4); }

(async () => {
  sessionStorage.setItem("any2api.admin.key", "k-test");
  const okRes = await A2AAdmin.load({ day: "2026-10-18" });
  const okCalls = calls.slice();
  calls.length = 0;
  const revoke = await A2AAdmin.revoke({ sid_prefix: "9f3c71ab" });
  const revokeCall = calls[0] || null;
  calls.length = 0;
  const detail = await A2AAdmin.detail("req_1");
  const detailCall = calls[0] || null;

  const live = A2AAdmin.live() || {};
  const metrics = live.metrics || { available: false, series: {}, labels: [], histogram: { buckets: [] } };
  const series = metrics.series || {};
  const audit = live.audit || { available: false, reason: "" };
  const sessions = live.sessions || { sessions: [], next_cursor: "" };
  const requests = live.requests || { rows: [] };
  const tasks = live.tasks || { rows: [] };
  const models = live.models || { rows: [] };
  const meta = live.meta || { kind: "", sources: {} };
  const firstReq = (requests.rows || [])[0] || {};
  const firstTask = (tasks.rows || [])[0] || {};
  const firstModel = (models.rows || [])[0] || {};
  const okSnapshot = {
    metrics: { available: !!metrics.available, requests: series.requests || [], labels: metrics.labels || [], hist: (metrics.histogram || {}).buckets || [] },
    audit: { available: !!audit.available, reason: audit.reason || "" },
    sessions: { n: (sessions.sessions || []).length, next: sessions.next_cursor || "" },
    requests: { n: (requests.rows || []).length, cred: Object.prototype.hasOwnProperty.call(firstReq, "credential_index") ? firstReq.credential_index : "missing", inTokens: firstReq.in_tokens === undefined ? null : firstReq.in_tokens },
    tasks: { face: firstTask.face === undefined ? null : firstTask.face, attempts: Object.prototype.hasOwnProperty.call(firstTask, "attempts") ? firstTask.attempts : "missing", terminal: Object.prototype.hasOwnProperty.call(firstTask, "terminal") ? firstTask.terminal : "missing" },
    models: { upstream: Object.prototype.hasOwnProperty.call(firstModel, "upstream_model") ? firstModel.upstream_model : "missing", usageAvailable: !!firstModel.usage_available },
    metaKind: meta.kind || "",
    metaStates: Object.keys(meta.sources || {}).filter((k) => meta.sources[k].state !== "已有")
  };

  calls.length = 0;
  mode = "unauthorized";
  const badRes = await A2AAdmin.load({});
  const badCalls = calls.slice();

  process.stdout.write(JSON.stringify({
    okRes, okCalls, okSnapshot, revokeRes: revoke, revokeCall, detailRes: { ok: detail.ok, reason: detail.reason }, detailCall,
    badRes, badCalls, localStorageTouched
  }));
})().catch((e) => { console.error(String((e && e.stack) || e)); process.exit(5); });
`

type adminUINodeResult struct {
	OkRes      map[string]any    `json:"okRes"`
	OkCalls    []adminUINodeCall `json:"okCalls"`
	OkSnapshot struct {
		Metrics struct {
			Available bool     `json:"available"`
			Requests  []int    `json:"requests"`
			Labels    []string `json:"labels"`
			Hist      []int    `json:"hist"`
		} `json:"metrics"`
		Audit struct {
			Available bool   `json:"available"`
			Reason    string `json:"reason"`
		} `json:"audit"`
		Sessions struct {
			N    int    `json:"n"`
			Next string `json:"next"`
		} `json:"sessions"`
		Requests struct {
			N        int   `json:"n"`
			Cred     any   `json:"cred"`
			InTokens int64 `json:"inTokens"`
		} `json:"requests"`
		Tasks struct {
			Face     string `json:"face"`
			Attempts any    `json:"attempts"`
			Terminal any    `json:"terminal"`
		} `json:"tasks"`
		Models struct {
			Upstream   any  `json:"upstream"`
			UsageAvail bool `json:"usageAvailable"`
		} `json:"models"`
		MetaKind   string   `json:"metaKind"`
		MetaStates []string `json:"metaStates"`
	} `json:"okSnapshot"`
	RevokeRes struct {
		OK     bool `json:"ok"`
		Status int  `json:"status"`
	} `json:"revokeRes"`
	RevokeCall *adminUINodeCall `json:"revokeCall"`
	DetailRes  struct {
		OK     bool   `json:"ok"`
		Reason string `json:"reason"`
	} `json:"detailRes"`
	DetailCall   *adminUINodeCall  `json:"detailCall"`
	BadRes       map[string]any    `json:"badRes"`
	BadCalls     []adminUINodeCall `json:"badCalls"`
	LocalStorage bool              `json:"localStorageTouched"`
}

type adminUINodeCall struct {
	URL    string  `json:"url"`
	Method string  `json:"method"`
	Key    *string `json:"key"`
	Body   *string `json:"body"`
}

// adminUIRunNodeDriver 用 node 跑驱动 + 指定适配层源码，回结构化结果。
func adminUIRunNodeDriver(t *testing.T, adapterSrc []byte) adminUINodeResult {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node 不存在：适配层行为面 [UNVERIFIED]（静态契约与托管面仍已覆盖）")
	}
	dir := t.TempDir()
	adapter := filepath.Join(dir, "admin-data.js")
	if err := os.WriteFile(adapter, adapterSrc, 0o644); err != nil {
		t.Fatalf("写适配层副本: %v", err)
	}
	driver := filepath.Join(dir, "driver.js")
	if err := os.WriteFile(driver, []byte(adminUINodeDriver), 0o644); err != nil {
		t.Fatalf("写 node 驱动: %v", err)
	}
	out, err := exec.Command(node, driver, adapter).CombinedOutput()
	if err != nil {
		t.Fatalf("node 驱动失败: %v\n%s", err, out)
	}
	var res adminUINodeResult
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("解析 node 输出: %v\n%s", err, out)
	}
	return res
}

// TestAdminUIAdapterNodeBehavior 适配层行为面：无 node 则跳过（并明确记录未跑）。
func TestAdminUIAdapterNodeBehavior(t *testing.T) {
	res := adminUIRunNodeDriver(t, adminUIAsset(t, "admin-data.js"))

	// (1) 每个 /admin/* 请求都带 X-Management-Key，且方法正确。
	if len(res.OkCalls) == 0 {
		t.Fatal("适配层未发任何请求")
	}
	for _, c := range res.OkCalls {
		if !strings.HasPrefix(c.URL, "/admin/") {
			t.Errorf("请求 %s 不在 /admin/ 下（适配层只准打管理面）", c.URL)
		}
		if c.Method != http.MethodGet {
			t.Errorf("请求 %s 方法 = %s, want GET", c.URL, c.Method)
		}
		if c.Key == nil || *c.Key != "k-test" {
			t.Errorf("请求 %s 的 X-Management-Key = %v, want k-test", c.URL, c.Key)
		}
	}
	// (2) 14 个只读端点全部被取。
	paths := make([]string, 0, len(res.OkCalls))
	for _, c := range res.OkCalls {
		paths = append(paths, strings.SplitN(c.URL, "?", 2)[0])
	}
	joined := strings.Join(paths, " ")
	for _, ep := range []string{"/admin/diagnostics", "/admin/pool", "/admin/logs", "/admin/sessions", "/admin/requests", "/admin/audit", "/admin/alerts", "/admin/metrics", "/admin/models", "/admin/tasks", "/admin/config", "/admin/system"} {
		if !strings.Contains(joined, ep) {
			t.Errorf("适配层未取 %s（实取 %s）", ep, joined)
		}
	}
	for _, scope := range []string{"scope=global", "scope=session", "scope=key", "scope=ip"} {
		if !strings.Contains(strings.Join(func() []string {
			out := make([]string, 0, len(res.OkCalls))
			for _, c := range res.OkCalls {
				out = append(out, c.URL)
			}
			return out
		}(), " "), scope) {
			t.Errorf("适配层未取 usage %s", scope)
		}
	}
	// (3) 桶 → 序列 + 直方图。
	if !res.OkSnapshot.Metrics.Available {
		t.Error("metrics 未标 available")
	}
	if got := res.OkSnapshot.Metrics.Requests; len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Errorf("metrics.series.requests = %v, want [1 2]", got)
	}
	if len(res.OkSnapshot.Metrics.Labels) != 2 {
		t.Errorf("metrics.labels 长度 = %d, want 2", len(res.OkSnapshot.Metrics.Labels))
	}
	if got := res.OkSnapshot.Metrics.Hist; len(got) != 15 || got[1] != 1 || got[2] != 3 {
		t.Errorf("metrics.histogram.buckets = %v, want 15 槽且 [1]=1 [2]=3", got)
	}
	// (4) 503 → 不冒充实测：available=false + 保留原因。
	if res.OkSnapshot.Audit.Available {
		t.Error("audit 503 时 available 应为 false")
	}
	if !strings.Contains(res.OkSnapshot.Audit.Reason, "503") || !strings.Contains(res.OkSnapshot.Audit.Reason, "store_unavailable") {
		t.Errorf("audit.reason = %q, want 含 503 store_unavailable", res.OkSnapshot.Audit.Reason)
	}
	// (5) 缺失值：credential_index null 保留（不得补 0）；tasks.attempts/terminal null；models 用量 available=false。
	if res.OkSnapshot.Requests.Cred != nil {
		t.Errorf("requests.credential_index = %v, want null（不得补 -1/0）", res.OkSnapshot.Requests.Cred)
	}
	if res.OkSnapshot.Requests.InTokens != 5 {
		t.Errorf("requests.in_tokens = %v, want 5（API `in` → 合同 `in_tokens`）", res.OkSnapshot.Requests.InTokens)
	}
	if res.OkSnapshot.Tasks.Attempts != nil {
		t.Errorf("tasks.attempts = %v, want null（未收尾任务不得补 0）", res.OkSnapshot.Tasks.Attempts)
	}
	if res.OkSnapshot.Tasks.Terminal != nil && res.OkSnapshot.Tasks.Terminal != "" {
		t.Errorf("tasks.terminal = %v, want null/空（不得伪造终态时刻）", res.OkSnapshot.Tasks.Terminal)
	}
	if res.OkSnapshot.Models.UsageAvail {
		t.Error("models.usage_available 应为 false（无每模型计数器）")
	}
	// (6) meta.kind = LIVE；未实现面（keys/usageTrend/events/probe）标"未提供"。
	if res.OkSnapshot.MetaKind != "LIVE" {
		t.Errorf("meta.kind = %q, want LIVE", res.OkSnapshot.MetaKind)
	}
	for _, k := range []string{"keys", "usageTrend", "events", "probe"} {
		found := false
		for _, s := range res.OkSnapshot.MetaStates {
			if s == k {
				found = true
			}
		}
		if !found {
			t.Errorf("meta.sources.%s 未标未提供（实标 %v）", k, res.OkSnapshot.MetaStates)
		}
	}
	// (7) 写动作：POST + 头 + JSON 体。
	if res.RevokeCall == nil || res.RevokeCall.Method != http.MethodPost {
		t.Fatalf("revoke 未发 POST：%+v", res.RevokeCall)
	}
	if res.RevokeCall.Key == nil || *res.RevokeCall.Key != "k-test" {
		t.Errorf("revoke 的 X-Management-Key = %v, want k-test", res.RevokeCall.Key)
	}
	if res.RevokeCall.Body == nil || !strings.Contains(*res.RevokeCall.Body, "sid_prefix") {
		t.Errorf("revoke 体 = %v, want 含 sid_prefix", res.RevokeCall.Body)
	}
	if !res.RevokeRes.OK || res.RevokeRes.Status != 200 {
		t.Errorf("revoke 结果 = %+v, want ok/200", res.RevokeRes)
	}
	// (8) 详情抽屉按需取：GET /admin/requests/{id}（404 也要把错误带回来，不编造）。
	if res.DetailCall == nil || !strings.HasPrefix(res.DetailCall.URL, "/admin/requests/req_1") {
		t.Fatalf("detail 未取 /admin/requests/{id}：%+v", res.DetailCall)
	}
	if res.DetailRes.OK {
		t.Error("detail 对 404 应回 ok=false")
	}
	// (9) 401 → unauthorized（UI 据此落锁），且 401 面不得覆写 LIVE 面。
	if reason, _ := res.BadRes["reason"].(string); reason != "unauthorized" {
		t.Errorf("401 时 load() reason = %q, want unauthorized", reason)
	}
	for _, c := range res.BadCalls {
		if c.Key == nil || *c.Key != "k-test" {
			t.Errorf("401 面请求 %s 仍应带密钥头（判据是服务端回答，不是不发）", c.URL)
		}
	}
	// (10) 绝不碰 localStorage。
	if res.LocalStorage {
		t.Error("适配层触碰了 localStorage（密钥持久面越权）")
	}
}

// TestAdminUIAdapterNodeNegativeControl 行为面负控：把请求头装配从适配层删掉后，
// 同一个驱动必须**红**（桩按 adminAuth 语义回答 401 → load() 落 unauthorized）。
// 证明行为门不是"桩里永远绿"。
func TestAdminUIAdapterNodeNegativeControl(t *testing.T) {
	orig := string(adminUIAsset(t, "admin-data.js"))
	mutant := strings.Replace(orig, `"X-Management-Key": getKey()`, `"X-Not-A-Key": getKey()`, 1)
	if mutant == orig {
		t.Fatal("负控变异未生效：适配层里找不到请求头装配点")
	}
	// 真源：带密钥 → 正常装载（与 TestAdminUIAdapterNodeBehavior 同源判据）。
	good := adminUIRunNodeDriver(t, []byte(orig))
	if ok, _ := good.OkRes["ok"].(bool); !ok {
		t.Fatalf("真源 load() 未成功：%+v", good.OkRes)
	}
	// 变异源：不带 X-Management-Key → 桩按 adminAuth 语义 401 → 首次 load() 必须
	// 落 unauthorized（且请求一条不落地带上空头）。
	bad := adminUIRunNodeDriver(t, []byte(mutant))
	if ok, _ := bad.OkRes["ok"].(bool); ok {
		t.Errorf("变异源 load() 竟成功：%+v", bad.OkRes)
	}
	if reason, _ := bad.OkRes["reason"].(string); reason != "unauthorized" {
		t.Errorf("变异源 401 语义失效：reason = %q（桩按 adminAuth 语义应答，删头必须打回 unauthorized）", reason)
	}
	for _, c := range bad.OkCalls {
		if c.Key != nil {
			t.Fatalf("变异源仍在发 X-Management-Key=%q —— 负控样本没删干净", *c.Key)
		}
	}
	if len(bad.OkCalls) == 0 {
		t.Fatal("变异源未发任何请求（驱动失效）")
	}
}

// ---------------------------------------------------------------------------
// 5. 渲染层行为（node + 最小 DOM 替身；无 node 则跳过）
// ---------------------------------------------------------------------------

// adminUIDomDriver 用 node 跑**真 admin.js** 的「启动 → 落锁 → 登录 → 渲染 → 切页」。
// 只给一个够用的 DOM 替身（节点壳 + classList + 事件 + 属性），不引 jsdom：admin.js 是
// 零依赖渲染层，要的 DOM 面很小；真浏览器面（计算样式 / 截图 / CSP / 视口）归 Chrome 冒烟
// （tmp/lanes/L19-W77-2/run_smoke.py）。
// 这条门抓的是 W77-2 的白页回归：登录首装（含 #/upstreams、#/traffic 深链）必须真的 render，
// 不是只把数据装进 A2A_LIVE；外加启动层最短 300ms、401 不留半张页面。
const adminUIDomDriver = `
"use strict";
const adminPath = process.argv[2];
const adapterPath = process.argv[3];
const deep = process.argv[4] || "upstreams";
const KEY = "k-test";
const PAGES = ["overview", "upstreams", "accounts", "traffic", "sessions", "usage", "audit", "models", "providers", "config"];

const nodes = new Map();
const clicked = [];      // 被 click() 的元素（下载锚点 = <a download> 的 filename/href 证据）
const toastLog = [];     // 建出来并被赋过文案的 .toast 节点（toast() 的可见证据）
const docListeners = {}; // document 事件（admin.js 的全局 click 委托从这里取）
let createdN = 0;
function el(id) {
  const e = { id: id, innerHTML: "", textContent: "", value: "", hidden: false, disabled: false, offsetWidth: 0,
              className: "", attrs: {}, listeners: {}, style: {}, href: "" };
  e.classList = {
    _s: new Set(),
    add: function () { for (var i = 0; i < arguments.length; i++) { this._s.add(arguments[i]); } },
    remove: function () { for (var i = 0; i < arguments.length; i++) { this._s.delete(arguments[i]); } },
    contains: function (c) { return this._s.has(c); },
    toggle: function (c, force) {
      if (arguments.length > 1) { if (force) { this._s.add(c); } else { this._s.delete(c); } }
      else if (this._s.has(c)) { this._s.delete(c); } else { this._s.add(c); }
    }
  };
  e.setAttribute = function (k, v) { e.attrs[k] = String(v); };
  e.getAttribute = function (k) { return Object.prototype.hasOwnProperty.call(e.attrs, k) ? e.attrs[k] : null; };
  e.removeAttribute = function (k) { delete e.attrs[k]; };
  e.addEventListener = function (t, fn) { (e.listeners[t] = e.listeners[t] || []).push(fn); };
  e.removeEventListener = function (t, fn) { var l = e.listeners[t] || []; var i = l.indexOf(fn); if (i >= 0) { l.splice(i, 1); } };
  e.querySelector = function () { return null; };
  e.querySelectorAll = function () { return []; };
  e.closest = function () { return null; };
  e.focus = function () {}; e.setSelectionRange = function () {}; e.appendChild = function () {}; e.remove = function () {};
  e.getBoundingClientRect = function () { return { width: 0, height: 0, top: 0, left: 0 }; };
  e.click = function () { clicked.push({ id: e.id, href: e.href || "", download: e.download || "" }); };
  return e;
}
// clickAct 模拟 admin.js 的全局 click 委托：ev.target.closest("[data-act]") → handleAction(act, el)。
function clickAct(attrs) {
  const target = {
    getAttribute: function (k) { return Object.prototype.hasOwnProperty.call(attrs, k) ? attrs[k] : null; },
    textContent: "", disabled: false
  };
  target.closest = function (sel) { return sel === "[data-act]" ? target : null; };
  (docListeners.click || []).forEach(function (fn) { fn({ target: target }); });
  return target;
}
function byId(id) { var n = nodes.get(id); if (!n) { n = el(id); nodes.set(id, n); } return n; }
PAGES.forEach(function (p) { byId("page-" + p); });

const winListeners = {};
const storage = {};
const localStorageCalls = [];
const fetched = [];
let fetchedN = 0, noKeyN = 0, wrongKeyN = 0;  // 累计计数（W78 动作段会清 fetched 数组，累计值才是全局证据）

global.location = { hash: "#/" + deep };
global.document = {
  documentElement: el("#document"), body: el("body"),
  getElementById: byId,
  querySelector: function (sel) { return sel && sel.charAt(0) === "#" ? byId(sel.slice(1)) : null; },
  querySelectorAll: function (sel) { return sel === "section.page" ? PAGES.map(function (p) { return byId("page-" + p); }) : []; },
  createElement: function () {
    const e = el("created-" + (++createdN));
    let tc = "";
    // toast() 只做 createElement + className + textContent + appendChild；容器是替身里的死节点，
    // 所以文案必须在赋值点捕获（否则 toast 断言变成"永远看不到"）。
    Object.defineProperty(e, "textContent", {
      get: function () { return tc; },
      set: function (v) { tc = String(v); if ((e.className || "").indexOf("toast") >= 0) toastLog.push(tc); }
    });
    return e;
  },
  addEventListener: function (t, fn) { (docListeners[t] = docListeners[t] || []).push(fn); },
  styleSheets: []
};
// 下载面替身：Blob + createObjectURL（admin-data.js 的下载按钮走这条路，不落盘）。
const blobURLs = [];
global.Blob = function (parts, opts) { this.parts = parts; this.type = (opts && opts.type) || ""; };
global.URL = {
  createObjectURL: function (b) { const u = "blob:k-test/" + (blobURLs.length + 1); blobURLs.push({ url: u, type: b && b.type }); return u; },
  revokeObjectURL: function () {}
};
global.window = {
  location: global.location,
  matchMedia: function () { return { matches: false }; },
  addEventListener: function (t, fn) { (winListeners[t] = winListeners[t] || []).push(fn); },
  setTimeout: setTimeout, setInterval: setInterval, clearTimeout: clearTimeout
};
global.sessionStorage = {
  getItem: function (k) { return Object.prototype.hasOwnProperty.call(storage, k) ? storage[k] : null; },
  setItem: function (k, v) { storage[k] = String(v); },
  removeItem: function (k) { delete storage[k]; }
};
global.localStorage = {
  getItem: function (k) { localStorageCalls.push({ op: "get", key: String(k), value: null }); return null; },
  setItem: function (k, v) { localStorageCalls.push({ op: "set", key: String(k), value: String(v) }); },
  removeItem: function (k) { localStorageCalls.push({ op: "remove", key: String(k), value: null }); }
};

const CANNED = {
  "/admin/diagnostics": { now: "2026-10-18T00:00:00Z", diagnostics: { requests: 5, attempts: 6, failovers: 1, cooldown_skips: 0 },
    upstreams: [{ index: 0, name: "fake-up", protocol: "anthropic", cooling: false, cooldown_remaining_seconds: 0,
                  consecutive_failures: 0, ladder_step: 0, window_requests: 3, window_failures: 0 }],
    log: { capacity: 256, count: 0, latest_seq: 0 }, admin: { auth_failures: 0, tracked_ips: 0, banned_ips: [] } },
  "/admin/pool": { upstreams: [{ index: 0, name: "fake-up", protocol: "anthropic", pool_size: 1,
    credentials: [{ index: 0, cooling: false, cooldown_until: 0, cooldown_remaining_seconds: 0, last_used: 0, busy: false }] }],
    capacity: { daily_units_budget: 10, used_today: 1, remaining: 9, inflight: 0, queue_len: 0, max_inflight: 2 }, now: 1700000000000 },
  "/admin/logs": { entries: [{ seq: 1, time: 1700000000000, kind: "request", upstream: "fake-up", status: 200, message: "m" }],
    capacity: 256, count: 1, latest_seq: 1, next_cursor: "" },
  "/admin/sessions": { sessions: [{ sid_prefix: "9f3c71ab", key_id: "3106c924", created_at: 1700000000000, last_seen_at: 1700000000000,
    expires_at: 4700000000000, abs_expires_at: 4700000000000, revoked: false, requests_today: 1, cost_units_today: 1 }], next_cursor: "", now: 1700000000000 },
  "/admin/usage": { day: "2026-10-18", scope: "global", rows: [{ scope: "global:1", requests: 2, cost_units: 1 }],
    totals: { requests: 2, cost_units: 1 }, now: 1700000000000 },
  "/admin/requests": { rows: [{ time: 1700000000000, request_id: "req_1", entry: "chat", model: "m1", stream: true, status: 200,
    attempts: 1, failovers: 0, upstream: "fake-up", upstreams: ["fake-up"], credential_index: null, ttft_ms: 100, total_ms: 200,
    in: 5, out: 6, outcome: "completed", sid_prefix: "9f3c71ab", key_id: "3106c924", task: "", failed: false, delivered: true }], next_cursor: "" },
  "/admin/audit": { rows: [{ ts: 1700000000000, kind: "request", sid_prefix: "9f3c71ab", key_id: "3106c924", request_id: "req_1", detail: "{}" }], next_cursor: "" },
  "/admin/alerts": { active: [], history: [] },
  "/admin/metrics": { window_seconds: 60, step_seconds: 60, now: 1700000000000,
    edges_ms: [0, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, "+Inf"],
    buckets: [{ ts: 1700000000000, requests: 1, errors: 0, failovers: 0, ttft_ms: { p50: 100, p95: 200 },
                total_ms: { p50: 300, p95: 400 }, ttft_hist: [], total_hist: [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] }] },
  "/admin/models": { configured: true, now: 1700000000000,
    models: [{ id: "m1", upstream_model: "upstream-m1", max_input_tokens: 200000, max_tokens: 8192, usage: { available: false } }] },
  "/admin/tasks": { tasks: [{ id: "resp_1", model: "m1", status: "completed", created_at: 1700000000, terminal_at: 1700000030,
    delivered: true, attempts: 1, upstream: "fake-up", sid_prefix: "9f3c71ab", key_id: "3106c924", face: "unknown" }], now: 1700000000000 },
  "/admin/config": { now: 1700000000000, listen: "127.0.0.1:18795", admin: { key_configured: true, log_capacity: 256 },
    ui: { enabled: true, rate_limit_per_min: 30 }, store: { enabled: false, path: "" },
    public: { enabled: false, access_mode: "open", access_keys_count: 0, trusted_proxies: [], limits: {} },
    models: [{ id: "m1", upstream_model: "upstream-m1" }], upstreams: [], limits: {}, debug: {}, task_face_degrade: "off" },
  "/admin/system": { now: 1700000000000, runtime: { go_version: "go1.26.7", pid: 1, goroutines: 2, mem_alloc_bytes: 3,
    mem_sys_bytes: 4, gc_count: 5, gc_pause_total_ms: 6, uptime_seconds: 7 },
    store: { enabled: false, path: "", size_bytes: 0, size_available: false, sessions: 0, sessions_available: false,
             audit_rows: 0, audit_rows_available: false },
    build: { commit: "abc1234", date: "2026-10-18T00:00:00Z", go_version: "go1.26.7" } },
  "/admin/accounts": { now: 1700000000000, upstreams: [
    { index: 0, name: "fake-up", protocol: "anthropic", base_url: "https://api.example.invalid/v1",
      proxy: { configured: true, scheme: "http", host: "proxy.local", port: 8080, auth: true, display: "http://proxy.local:8080" },
      tls_fingerprint: "chrome", pool_size: 2, credentials: [
        { index: 0, cooling: false, cooldown_until: 0, cooldown_remaining_seconds: 0, last_used: 1700000000000, busy: false },
        { index: 1, cooling: true, cooldown_until: 1700000600000, cooldown_remaining_seconds: 720, last_used: 0, busy: false }] },
    { index: 1, name: "backup-up", protocol: "openai", base_url: "",
      proxy: { configured: false, scheme: "", host: "", port: 0, auth: false, display: "" },
      tls_fingerprint: "none", pool_size: 1, credentials: [
        { index: 0, cooling: false, cooldown_until: 0, cooldown_remaining_seconds: 0, last_used: 0, busy: true }] }] },
  "/admin/proxies": { now: 1700000000000, proxies: [
    { upstream: "fake-up", index: 0, configured: true, scheme: "http", host: "proxy.local", port: 8080, auth: true,
      display: "http://proxy.local:8080", tls_fingerprint: "chrome" },
    { upstream: "backup-up", index: 1, configured: false, scheme: "", host: "", port: 0, auth: false,
      display: "", tls_fingerprint: "none" }] },
  "/admin/providers": { now: 1700000000000, providers: [
    { id: "anthropic", label: "Anthropic", protocols: ["anthropic"], configured: true, upstreams: ["fake-up"],
      capabilities: ["chat", "tools"], readiness: "ready", docs: "docs/service.md" },
    { id: "openai", label: "OpenAI", protocols: ["openai_chat", "responses"], configured: true, upstreams: ["backup-up"],
      capabilities: ["chat"], readiness: "degraded", docs: "docs/service.md" },
    { id: "gemini", label: "Google Gemini", protocols: ["gemini"], configured: false, upstreams: [],
      capabilities: [], readiness: "not_configured", docs: "docs/architecture.md" }] }
};
let unauthorized = false;
function resp(status, body, headers) {
  return {
    ok: status >= 200 && status < 300, status: status,
    headers: { get: function (k) { return (headers || {})[String(k).toLowerCase()] || null; } },
    text: function () { return Promise.resolve(typeof body === "string" ? body : JSON.stringify(body)); }
  };
}
const COOLDOWN_OK = { upstream: "fake-up", index: 1, cooling: true, cooldown_until: 1700007800000, cooldown_remaining_seconds: 21600, now: 1700000600000 };
const PROBE_OK = { upstream: "fake-up", protocol: "anthropic", mode: "models", ok: true, reachable: true, status: 200,
                   latency_ms: 123, auth: "verified", error_class: "", checked_at: 1700000000000 };
const PROBE_TIMEOUT = { upstream: "fake-up", protocol: "anthropic", mode: "models", ok: false, reachable: false, status: 0,
                        latency_ms: 5001, auth: "unknown", error_class: "timeout", checked_at: 1700000000000 };
let probeFailMode = false;
global.fetch = function (url, opts) {
  const raw = String(url);
  const u = raw.split("?")[0];
  const h = (opts && opts.headers) || {};
  const key = Object.prototype.hasOwnProperty.call(h, "X-Management-Key") ? h["X-Management-Key"] : null;
  fetched.push({ url: raw, path: u, method: (opts && opts.method) || "GET", key: key, body: (opts && opts.body) || null });
  fetchedN++;
  if (!key) noKeyN++;
  if (key === "wrong-key") wrongKeyN++;
  if (unauthorized || key !== KEY) {
    return Promise.resolve(resp(401, { error: { type: "authentication_error", message: "invalid or missing admin key" } }));
  }
  if (u === "/admin/export") {
    const q = new URLSearchParams(raw.split("?")[1] || "");
    const typ = q.get("type") || "unknown", fmt = q.get("format") || "jsonl";
    const day = (q.get("day") || "2026-10-18").replace(/-/g, "");
    const csv = "upstream_index,upstream,name,protocol,base_url,proxy_display,tls_fingerprint,credential_index,cooling,cooldown_until,last_used\n0,fake-up,fake-up,anthropic,https://api.example.invalid,http://proxy.local:8080,chrome,0,false,0,0\n";
    return Promise.resolve(resp(200, fmt === "csv" ? csv : JSON.stringify({ type: typ }) + "\n", {
      "content-type": fmt === "csv" ? "text/csv; charset=utf-8" : "application/x-ndjson; charset=utf-8",
      "content-disposition": 'attachment; filename="any2api-' + typ + "-" + day + "." + fmt + '"'
    }));
  }
  if (u === "/admin/usage" && raw.indexOf("scope=model") >= 0) {
    return Promise.resolve(resp(200, { day: "2026-10-18", scope: "model",
      rows: [{ scope: "m1", requests: 7, cost_units: 3 }, { scope: "m2", requests: 5, cost_units: 2 }],
      totals: { requests: 12, cost_units: 5 }, now: 1700000000000 }));
  }
  if (u === "/admin/accounts/fake-up/1/cooldown") return Promise.resolve(resp(200, COOLDOWN_OK));
  if (u === "/admin/upstreams/fake-up/probe") return Promise.resolve(resp(200, probeFailMode ? PROBE_TIMEOUT : PROBE_OK));
  if (u === "/admin/upstreams/missing-up/probe") {
    return Promise.resolve(resp(200, { upstream: "missing-up", protocol: "openai", mode: "models", ok: false, reachable: false,
      status: 0, latency_ms: 5001, auth: "unknown", error_class: "timeout", checked_at: 1700000000000 }));
  }
  if (u === "/admin/accounts/nope-up/9/cooldown") {
    return Promise.resolve(resp(404, { error: { type: "not_found", message: "unknown upstream" } }));
  }
  const body = Object.prototype.hasOwnProperty.call(CANNED, u) ? CANNED[u] : { error: { type: "not_found", message: u } };
  return Promise.resolve(resp(200, body));
};
function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }
function submitKey(v) {
  const f = byId("lock-form");
  const k = byId("lock-key");
  k.value = v;
  const l = (f.listeners.submit || []);
  if (!l.length) { throw new Error("no submit listener on #lock-form"); }
  l[0]({ preventDefault: function () {} });
}
function bootState() {
  return { done: byId("boot").classList.contains("boot-done"), ms: (global.window.__a2aAdminBoot || {}).ms === undefined ? null : global.window.__a2aAdminBoot.ms,
           min: (global.window.__a2aAdminBoot || {}).min === undefined ? null : global.window.__a2aAdminBoot.min };
}

require(adapterPath);
require(adminPath);

const out = { deep: deep, pages: PAGES };
out.firstPaint = { bootDone: byId("boot").classList.contains("boot-done"), lockHidden: byId("lock").hidden,
                   pageLen: byId("page-" + deep).innerHTML.length, status: byId("top-status").textContent };
(async function () {
  await sleep(450);                       // 启动层最短 300ms + 余量
  out.bootSettled = bootState();
  out.afterBootLock = byId("lock").hidden;
  submitKey("wrong-key");                 // 错密钥：桩回 401（与服务端 adminAuth 同形）
  await sleep(200);
  out.wrongKey = { lockHidden: byId("lock").hidden, msg: byId("lock-msg").textContent,
                   pageLen: byId("page-" + deep).innerHTML.length };
  submitKey(KEY);                         // 正密钥：登录首装必须渲染（深链也算）
  await sleep(300);
  out.login = { lockHidden: byId("lock").hidden, page: global.window.__a2aAdminPage, ready: global.window.__a2aAdminReady,
                pageLen: byId("page-" + deep).innerHTML.length, lenAll: PAGES.reduce(function (a, p) { return a + byId("page-" + p).innerHTML.length; }, 0),
                heading: (byId("page-" + deep).innerHTML.match(/<h1[^>]*>([^<]*)<\/h1>/) || [])[1] || "" };
  await sleep(350);
  out.afterLoginBoot = bootState();
  global.location.hash = "#/traffic";     // 页内切页（hashchange → route → render）
  (winListeners.hashchange || []).forEach(function (fn) { try { fn(); } catch (e) { out.routeError = String(e); } });
  await sleep(300);
  out.routeNav = { page: global.window.__a2aAdminPage, len: byId("page-traffic").innerHTML.length, ready: global.window.__a2aAdminReady };
  // ---- W78-2：账号与代理 / 服务商 / 逐模型用量 + 冷却/探针/导出三条真动作 ----
  function html(id) { return byId(id).innerHTML; }
  function countOf(s, rx) { const m = s.match(rx); return m ? m.length : 0; }
  function leaksOf(s) {
    const bad = [];
    ["undefined", "null", "NaN"].forEach(function (w) {
      if (new RegExp("(^|[^\\w])" + w + "([^\\w]|$)").test(s.replace(/<[^>]*>/g, " "))) bad.push(w);
    });
    return bad;
  }
  function nav(hash) {
    global.location.hash = hash;
    (winListeners.hashchange || []).forEach(function (fn) { try { fn(); } catch (e) { out.routeErrorW78 = String(e); } });
  }
  nav("#/accounts"); await sleep(150);
  const accHTML = html("page-accounts");
  nav("#/providers"); await sleep(150);
  const provHTML = html("page-providers");
  nav("#/usage"); await sleep(150);
  const useHTML = html("page-usage");
  out.w78 = {
    navCounts: { accounts: byId("nav-cnt-accounts").textContent, providers: byId("nav-cnt-providers").textContent },
    accounts: {
      heading: (accHTML.match(/<h1[^>]*>([^<]*)<\/h1>/) || [])[1] || "", len: accHTML.length,
      kpis: countOf(accHTML, /class="kpi"/g), rows: countOf(accHTML, /class="acct-row"/g),
      ready: countOf(accHTML, /cred-chip ready/g), cooling: countOf(accHTML, /cred-chip cooling/g), busy: countOf(accHTML, /cred-chip busy/g),
      cdSels: countOf(accHTML, /ad-ctl sm cd-sel/g), cdOptions: countOf(accHTML, /<option value="21600">6h<\/option>/g),
      cooldownBtns: countOf(accHTML, /data-act="cooldown"/g), probeBtns: countOf(accHTML, /data-act="probe"/g),
      exports: countOf(accHTML, /data-act="export"/g), proxyRows: countOf(accHTML, /class="proxy-row"/g),
      leaks: leaksOf(accHTML)
    },
    providers: {
      heading: (provHTML.match(/<h1[^>]*>([^<]*)<\/h1>/) || [])[1] || "", len: provHTML.length,
      cards: countOf(provHTML, /class="provider-card"/g),
      ready: countOf(provHTML, /data-ready="ready"/g), degraded: countOf(provHTML, /data-ready="degraded"/g),
      notConfigured: countOf(provHTML, /data-ready="not_configured"/g),
      blankReady: countOf(provHTML, /data-ready=""/g),
      docsLinks: countOf(provHTML, /target="_blank" rel="noopener"/g),
      probeBtns: countOf(provHTML, /data-act="probe"/g),
      upstreamNames: (provHTML.indexOf("fake-up") >= 0 && provHTML.indexOf("backup-up") >= 0),
      leaks: leaksOf(provHTML)
    },
    usage: {
      heading: (useHTML.match(/<h1[^>]*>([^<]*)<\/h1>/) || [])[1] || "",
      modelRows: countOf(useHTML, /class="model-usage-row"/g),
      modelTable: useHTML.indexOf("model-usage-table") >= 0,
      modelExports: countOf(useHTML, /data-act="export" data-type="usage" data-format="\w+" data-scope="model"/g),
      leaks: leaksOf(useHTML)
    },
    // W79 控件收敛：渲染出的每个 <button> 都必须仍带 .ad-ctl 别名（分段按钮走 .ad-seg）。
    controlClass: (function () {
      const all = PAGES.map(function (p) { return byId("page-" + p).innerHTML; }).join("\n");
      const tags = all.match(/<button[^>]*>/g) || [];
      return {
        buttons: tags.length,
        ctlButtons: countOf(all, /<button[^>]*\bad-ctl\b/g),
        segButtons: countOf(all, /<button[^>]*data-scope=/g),
        stray: tags.filter(function (b) {
          return !/\bad-ctl\b/.test(b) && !/data-scope=/.test(b);
        }).slice(0, 3)
      };
    })()
  };
  // 冷却：选 6h（21600）→ 冷却按钮 → 真发 POST
  nav("#/accounts"); await sleep(120);
  fetched.length = 0;
  byId("cd-0-1").value = "21600";
  clickAct({ "data-act": "cooldown", "data-up": "fake-up", "data-idx": "1", "data-cd": "cd-0-1" });
  await sleep(250);
  const cdCall = fetched.filter(function (c) { return c.method === "POST"; })[0] || null;
  out.w78.cooldown = {
    url: cdCall ? cdCall.url : "", keyIsSet: !!(cdCall && cdCall.key), body: cdCall ? cdCall.body : null,
    toasts: toastLog.slice()
  };
  // 探针（成功）+ 探针（不可达）→ 内联徽标 + toast
  toastLog.length = 0; fetched.length = 0;
  clickAct({ "data-act": "probe", "data-up": "fake-up" });
  await sleep(300);
  const prCall = fetched.filter(function (c) { return c.method === "POST"; })[0] || null;
  out.w78.probe = {
    url: prCall ? prCall.url : "", body: prCall ? prCall.body : null,
    badge: html("page-accounts").indexOf("200 · 123ms · auth verified") >= 0,
    toasts: toastLog.slice()
  };
  toastLog.length = 0; fetched.length = 0;
  probeFailMode = true;   // 同一上游、同一个按钮，桩改成不可达：内联徽标必须跟着变
  clickAct({ "data-act": "probe", "data-up": "fake-up" });
  await sleep(300);
  out.w78.probeFail = {
    badge: html("page-accounts").indexOf("timeout · 网络不可达") >= 0,
    toasts: toastLog.slice()
  };
  probeFailMode = false;
  // 导出（CSV）：真发 GET /admin/export（密钥在头、不在 URL），文件名只认 Content-Disposition
  toastLog.length = 0; fetched.length = 0; clicked.length = 0; blobURLs.length = 0;
  clickAct({ "data-act": "export", "data-type": "accounts", "data-format": "csv" });
  await sleep(300);
  const exCall = fetched.filter(function (c) { return c.path === "/admin/export"; })[0] || null;
  const dl = clicked.length ? clicked[clicked.length - 1] : null;
  out.w78.export = {
    url: exCall ? exCall.url : "", keyIsSet: !!(exCall && exCall.key),
    keyInURL: exCall ? exCall.url.indexOf(KEY) >= 0 : null,
    hasAuthHeader: !!(exCall && exCall.key), download: dl ? dl.download : null, href: dl ? dl.href : null,
    blobType: blobURLs.length ? blobURLs[blobURLs.length - 1].type : null,
    toasts: toastLog.slice()
  };
  nav("#/accounts"); // 复原当前页，后续断言不受影响
  // 密钥纪律：localStorage 只准出现主题偏好（a2a.admin.theme），不得出现任何密钥字节。
  out.localStorageCalls = localStorageCalls;
  out.localStorageKeyLeak = localStorageCalls.some(function (c) {
    return c.key.indexOf("any2api.admin.key") >= 0 || (c.value !== null && c.value.indexOf(KEY) >= 0);
  });
  out.fetchedAllKeyed = fetchedN > 0 && noKeyN === 0;
  out.fetchedWrongKeyCount = wrongKeyN;
  out.fetchedCount = fetchedN;
  process.stdout.write(JSON.stringify(out));
  process.exit(0);   // admin.js 起了 setInterval(autoRefresh)，不主动退 node 会一直挂着
})().catch(function (e) { console.error(String((e && e.stack) || e)); process.exit(5); });
`

type adminUIDomResult struct {
	Deep       string `json:"deep"`
	FirstPaint struct {
		BootDone   bool   `json:"bootDone"`
		LockHidden bool   `json:"lockHidden"`
		PageLen    int    `json:"pageLen"`
		Status     string `json:"status"`
	} `json:"firstPaint"`
	BootSettled struct {
		Done bool `json:"done"`
		Ms   any  `json:"ms"`
		Min  any  `json:"min"`
	} `json:"bootSettled"`
	AfterBootLock bool `json:"afterBootLock"`
	WrongKey      struct {
		LockHidden bool   `json:"lockHidden"`
		Msg        string `json:"msg"`
		PageLen    int    `json:"pageLen"`
	} `json:"wrongKey"`
	Login struct {
		LockHidden bool   `json:"lockHidden"`
		Page       string `json:"page"`
		Ready      int    `json:"ready"`
		PageLen    int    `json:"pageLen"`
		LenAll     int    `json:"lenAll"`
		Heading    string `json:"heading"`
	} `json:"login"`
	AfterLoginBoot struct {
		Done bool `json:"done"`
		Ms   any  `json:"ms"`
	} `json:"afterLoginBoot"`
	RouteNav struct {
		Page  string `json:"page"`
		Len   int    `json:"len"`
		Ready int    `json:"ready"`
	} `json:"routeNav"`
	RouteError string `json:"routeError"`
	// 密钥纪律观测面：localStorage 的每次调用（键+值）与请求头清单。
	LocalStorageCalls []struct {
		Op    string  `json:"op"`
		Key   string  `json:"key"`
		Value *string `json:"value"`
	} `json:"localStorageCalls"`
	LocalStorageKeyLeak  bool `json:"localStorageKeyLeak"`
	FetchedAllKeyed      bool `json:"fetchedAllKeyed"`
	FetchedWrongKeyCount int  `json:"fetchedWrongKeyCount"`
	FetchedCount         int  `json:"fetchedCount"`
	// W78-2 观测面：两条新页面 + 逐模型用量 + 冷却/探针/导出三条真动作。
	W78 struct {
		NavCounts struct {
			Accounts  string `json:"accounts"`
			Providers string `json:"providers"`
		} `json:"navCounts"`
		Accounts struct {
			Heading      string   `json:"heading"`
			Len          int      `json:"len"`
			Kpis         int      `json:"kpis"`
			Rows         int      `json:"rows"`
			Ready        int      `json:"ready"`
			Cooling      int      `json:"cooling"`
			Busy         int      `json:"busy"`
			CdSels       int      `json:"cdSels"`
			CdOptions    int      `json:"cdOptions"`
			CooldownBtns int      `json:"cooldownBtns"`
			ProbeBtns    int      `json:"probeBtns"`
			Exports      int      `json:"exports"`
			ProxyRows    int      `json:"proxyRows"`
			Leaks        []string `json:"leaks"`
		} `json:"accounts"`
		Providers struct {
			Heading       string   `json:"heading"`
			Len           int      `json:"len"`
			Cards         int      `json:"cards"`
			Ready         int      `json:"ready"`
			Degraded      int      `json:"degraded"`
			NotConfigured int      `json:"notConfigured"`
			BlankReady    int      `json:"blankReady"`
			DocsLinks     int      `json:"docsLinks"`
			ProbeBtns     int      `json:"probeBtns"`
			UpstreamNames bool     `json:"upstreamNames"`
			Leaks         []string `json:"leaks"`
		} `json:"providers"`
		Usage struct {
			Heading     string   `json:"heading"`
			ModelRows   int      `json:"modelRows"`
			ModelTable  bool     `json:"modelTable"`
			ModelExport int      `json:"modelExports"`
			Leaks       []string `json:"leaks"`
		} `json:"usage"`
		// W79 控件类收敛观测面：每个 <button> 是否仍带 .ad-ctl 别名。
		ControlClass struct {
			Buttons    int      `json:"buttons"`
			CtlButtons int      `json:"ctlButtons"`
			SegButtons int      `json:"segButtons"`
			Stray      []string `json:"stray"`
		} `json:"controlClass"`
		Cooldown struct {
			URL      string   `json:"url"`
			KeyIsSet bool     `json:"keyIsSet"`
			Body     *string  `json:"body"`
			Toasts   []string `json:"toasts"`
		} `json:"cooldown"`
		Probe struct {
			URL    string   `json:"url"`
			Body   *string  `json:"body"`
			Badge  bool     `json:"badge"`
			Toasts []string `json:"toasts"`
		} `json:"probe"`
		ProbeFail struct {
			Badge  bool     `json:"badge"`
			Toasts []string `json:"toasts"`
		} `json:"probeFail"`
		Export struct {
			URL      string   `json:"url"`
			KeyIsSet bool     `json:"keyIsSet"`
			KeyInURL bool     `json:"keyInURL"`
			Download *string  `json:"download"`
			Href     string   `json:"href"`
			BlobType *string  `json:"blobType"`
			Toasts   []string `json:"toasts"`
		} `json:"export"`
	} `json:"w78"`
	RouteErrorW78 string `json:"routeErrorW78"`
}

// adminUIRunDomDriver 用 node 跑 DOM 驱动 + 指定渲染层/适配层源码，回结构化结果。
func adminUIRunDomDriver(t *testing.T, adminSrc, adapterSrc []byte, deep string) adminUIDomResult {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node 不存在：渲染层行为面 [UNVERIFIED]（静态契约 + Chrome 冒烟仍覆盖）")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "admin.js"), adminSrc, 0o644); err != nil {
		t.Fatalf("写 admin.js 副本: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "admin-data.js"), adapterSrc, 0o644); err != nil {
		t.Fatalf("写 admin-data.js 副本: %v", err)
	}
	driver := filepath.Join(dir, "dom-driver.js")
	if err := os.WriteFile(driver, []byte(adminUIDomDriver), 0o644); err != nil {
		t.Fatalf("写 node DOM 驱动: %v", err)
	}
	out, err := exec.Command(node, driver, filepath.Join(dir, "admin.js"), filepath.Join(dir, "admin-data.js"), deep).CombinedOutput()
	if err != nil {
		t.Fatalf("node DOM 驱动失败: %v\n%s", err, out)
	}
	var res adminUIDomResult
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("解析 node DOM 输出: %v\n%s", err, out)
	}
	return res
}

// TestAdminUIDomBootAndLoginRender 渲染层行为面（真 admin.js + 最小 DOM 替身）。
// 白页回归（W77-2）在这条门上会直接红：登录首装不 render 时 pageLen = 0。
func TestAdminUIDomBootAndLoginRender(t *testing.T) {
	adminSrc, adapterSrc := adminUIAsset(t, "admin.js"), adminUIAsset(t, "admin-data.js")
	for _, deep := range []string{"upstreams", "traffic"} {
		res := adminUIRunDomDriver(t, adminSrc, adapterSrc, deep)
		// 首帧：启动层还在（未加 .boot-done）、锁层已伸出、页面容器还没有数据。
		if res.FirstPaint.BootDone {
			t.Errorf("[%s] 首帧启动层就被收掉（等于没有启动层）", deep)
		}
		if res.FirstPaint.LockHidden {
			t.Errorf("[%s] 无密钥首帧锁层未伸出", deep)
		}
		if res.FirstPaint.PageLen != 0 {
			t.Errorf("[%s] 无密钥首帧页面容器已有 %d 字节数据（未鉴权不该有数据）", deep, res.FirstPaint.PageLen)
		}
		// 启动层：最短 300ms 才收；观测面 __a2aAdminBoot 必须记到 ms ≥ 300。
		if !res.BootSettled.Done {
			t.Errorf("[%s] 启动层未收尾（.boot-done 缺失）", deep)
		}
		if ms, ok := res.BootSettled.Ms.(float64); !ok || ms < 300 {
			t.Errorf("[%s] 无密钥路径启动层 ms = %v, want ≥300", deep, res.BootSettled.Ms)
		}
		if res.AfterBootLock {
			t.Errorf("[%s] 无密钥：锁层不可见（启动层把锁层盖住了）", deep)
		}
		// 错密钥：401 → 留在锁层、给原因、页面容器仍然为空。
		if res.WrongKey.LockHidden {
			t.Errorf("[%s] 401 后面板竟解锁", deep)
		}
		if !strings.Contains(res.WrongKey.Msg, "密钥无效") {
			t.Errorf("[%s] 401 锁层文案 = %q, want 含「密钥无效」", deep, res.WrongKey.Msg)
		}
		if res.WrongKey.PageLen != 0 {
			t.Errorf("[%s] 401 后页面容器有 %d 字节（未授权数据泄漏进 DOM）", deep, res.WrongKey.PageLen)
		}
		// 正密钥 + 深链：锁层收掉、当页渲染、其余页面不空转。
		if !res.Login.LockHidden {
			t.Errorf("[%s] 登录成功但锁层未收", deep)
		}
		if res.Login.Page != deep {
			t.Errorf("[%s] 登录后当前页 = %q, want %q", deep, res.Login.Page, deep)
		}
		if res.Login.PageLen <= 400 {
			t.Errorf("[%s] 深链登录后页面容器只有 %d 字节 —— 白页回归（render 被跳过）", deep, res.Login.PageLen)
		}
		if res.Login.Heading == "" {
			t.Errorf("[%s] 深链登录后当页没有 h1 标题", deep)
		}
		if res.Login.Ready < 1 {
			t.Errorf("[%s] 登录后 __a2aAdminReady = %d, want ≥1", deep, res.Login.Ready)
		}
		// 登录路径的启动层：show → hide 也要 ≥300ms。
		if !res.AfterLoginBoot.Done {
			t.Errorf("[%s] 登录路径启动层未收尾", deep)
		}
		if ms, ok := res.AfterLoginBoot.Ms.(float64); !ok || ms < 300 {
			t.Errorf("[%s] 登录路径启动层 ms = %v, want ≥300", deep, res.AfterLoginBoot.Ms)
		}
		// 页内切页（hashchange）也要渲染。
		if res.RouteNav.Page != "traffic" || res.RouteNav.Len <= 400 {
			t.Errorf("[%s] 页内切页后 #page-traffic 长度 = %d（page=%s）", deep, res.RouteNav.Len, res.RouteNav.Page)
		}
		if res.RouteError != "" {
			t.Errorf("[%s] hashchange 路由抛错：%s", deep, res.RouteError)
		}
		// 密钥纪律：localStorage 只准主题偏好（a2a.admin.theme），不得出现任何密钥字节；
		// 每个 /admin/* 请求都要带密钥头（错密钥那次也必须带 = 判据在服务端）。
		if res.LocalStorageKeyLeak {
			t.Errorf("[%s] localStorage 出现密钥面：%+v", deep, res.LocalStorageCalls)
		}
		for _, c := range res.LocalStorageCalls {
			if c.Op != "get" && c.Key != "a2a.admin.theme" {
				t.Errorf("[%s] localStorage 出现非主题写入：%+v", deep, c)
			}
		}
		if !res.FetchedAllKeyed || res.FetchedCount == 0 {
			t.Errorf("[%s] 请求未全部带 X-Management-Key（fetched=%d allKeyed=%v）", deep, res.FetchedCount, res.FetchedAllKeyed)
		}
		if res.FetchedWrongKeyCount == 0 {
			t.Errorf("[%s] 错密钥路径没有真发请求（401 面没被触发）", deep)
		}
	}
}

// TestAdminUIDomNegativeControl 行为面负控：把渲染条件退回「只非 local 才渲染」
// （= 实测的白页根因）后，同一个驱动必须**红**：深链登录后页面容器停在 0 字节。
func TestAdminUIDomNegativeControl(t *testing.T) {
	orig := string(adminUIAsset(t, "admin.js"))
	adapter := adminUIAsset(t, "admin-data.js")

	mutant := strings.Replace(orig, "if (!opts.local || opts.initial) {", "if (!opts.local) {", 1)
	if mutant == orig {
		t.Fatal("负控变异未生效：找不到渲染条件")
	}
	mutant = strings.Replace(mutant, "refreshAll({ initial: true })", "refreshAll({ initial: true, local: true })", 1)
	if !strings.Contains(mutant, "local: true") {
		t.Fatal("负控变异未生效：找不到登录首装调用点")
	}

	good := adminUIRunDomDriver(t, []byte(orig), adapter, "upstreams")
	if good.Login.PageLen <= 400 {
		t.Fatalf("真源深链登录后页面长度 = %d，本应渲染（负控前提不成立）", good.Login.PageLen)
	}
	bad := adminUIRunDomDriver(t, []byte(mutant), adapter, "upstreams")
	if bad.Login.PageLen != 0 {
		t.Errorf("变异源深链登录后页面长度 = %d（白页负控必须停在 0）", bad.Login.PageLen)
	}
	if bad.Login.Heading != "" {
		t.Errorf("变异源深链登录后竟有标题 %q（负控失效）", bad.Login.Heading)
	}
	if !bad.Login.LockHidden {
		t.Errorf("变异源锁层未收（说明连数据都没装载，不是我们要测的那条路径）")
	}
}

// ---------------------------------------------------------------------------
// 6. W78-2：账号与代理 / 服务商 / 逐模型用量 + 三条真动作（node + 最小 DOM 替身）
// ---------------------------------------------------------------------------

// TestAdminUIDomW78PagesAndActions 渲染层行为面：真 admin.js + 真 admin-data.js，桩面回
// accounts/proxies/providers/usage?scope=model/cooldown/probe/export 的真实形状。
// 判据全部来自**渲染出来的 HTML 与真发出去的请求**，不是源码字样。
func TestAdminUIDomW78PagesAndActions(t *testing.T) {
	res := adminUIRunDomDriver(t, adminUIAsset(t, "admin.js"), adminUIAsset(t, "admin-data.js"), "accounts")
	if res.RouteErrorW78 != "" {
		t.Errorf("W78 页面路由抛错：%s", res.RouteErrorW78)
	}
	a, pv, u := res.W78.Accounts, res.W78.Providers, res.W78.Usage

	// (1) 账号与代理：4 张汇总卡 + 逐上游行 + 凭据 chip + 下拉 + 动作 + 导出
	if a.Heading != "账号与代理" || a.Len <= 400 {
		t.Errorf("accounts 页 heading=%q len=%d, want 账号与代理 / >400", a.Heading, a.Len)
	}
	if a.Kpis != 4 {
		t.Errorf("accounts 汇总卡 = %d, want 4（上游/凭据/已配代理/冷却中）", a.Kpis)
	}
	if a.Rows != 2 || a.ProxyRows != 2 {
		t.Errorf("accounts 行 = %d / proxies 行 = %d, want 2 / 2（含未配置代理行）", a.Rows, a.ProxyRows)
	}
	if a.Ready < 1 || a.Cooling < 1 || a.Busy < 1 {
		t.Errorf("凭据 chip ready/cooling/busy = %d/%d/%d, want 各 ≥1", a.Ready, a.Cooling, a.Busy)
	}
	if a.CdSels != 3 || a.CdOptions != 3 || a.CooldownBtns != 3 {
		t.Errorf("冷却下拉/档位/按钮 = %d/%d/%d, want 3/3/3（每凭据一组）", a.CdSels, a.CdOptions, a.CooldownBtns)
	}
	if a.ProbeBtns != 2 {
		t.Errorf("探针按钮 = %d, want 2（每上游一个真动作）", a.ProbeBtns)
	}
	if a.Exports != 4 {
		t.Errorf("导出按钮 = %d, want 4（accounts/proxies × csv/jsonl）", a.Exports)
	}
	if len(a.Leaks) != 0 {
		t.Errorf("accounts 页出现 %v 字面泄漏（缺数不得渲染成 undefined/null）", a.Leaks)
	}

	// (2) 服务商：目录卡片 + readiness 四值映射 + 文档链接 + 探针
	if pv.Heading != "服务商" || pv.Len <= 300 {
		t.Errorf("providers 页 heading=%q len=%d, want 服务商 / >300", pv.Heading, pv.Len)
	}
	if pv.Cards != 3 {
		t.Errorf("服务商卡片 = %d, want 3（stub 目录三条）", pv.Cards)
	}
	if pv.Ready != 1 || pv.Degraded != 1 || pv.NotConfigured != 1 || pv.BlankReady != 0 {
		t.Errorf("readiness 徽标 ready/degraded/not_configured/空 = %d/%d/%d/%d, want 1/1/1/0",
			pv.Ready, pv.Degraded, pv.NotConfigured, pv.BlankReady)
	}
	if pv.DocsLinks != 3 {
		t.Errorf("文档链接 = %d, want 3（target=_blank + rel=noopener）", pv.DocsLinks)
	}
	if pv.ProbeBtns != 2 || !pv.UpstreamNames {
		t.Errorf("探针按钮 = %d / 上游名可见 = %v, want 2 / true（已配置上游逐台可探）", pv.ProbeBtns, pv.UpstreamNames)
	}
	if len(pv.Leaks) != 0 {
		t.Errorf("providers 页出现 %v 字面泄漏", pv.Leaks)
	}

	// (3) 用量与成本：逐模型用量卡（真来源 scope=model）+ 导出
	if u.Heading != "用量与成本" {
		t.Errorf("usage 页 heading = %q, want 用量与成本", u.Heading)
	}
	if u.ModelRows != 2 || !u.ModelTable {
		t.Errorf("model-usage 行 = %d / 表存在 = %v, want 2 / true", u.ModelRows, u.ModelTable)
	}
	if u.ModelExport != 2 {
		t.Errorf("逐模型导出按钮 = %d, want 2（csv/jsonl，带 data-scope=model）", u.ModelExport)
	}
	if len(u.Leaks) != 0 {
		t.Errorf("usage 页出现 %v 字面泄漏", u.Leaks)
	}

	// (4) 导航计数：账号 = 上游数；服务商 = 已配置条目数
	if res.W78.NavCounts.Accounts != "2" || res.W78.NavCounts.Providers != "2" {
		t.Errorf("导航计数 accounts/providers = %q/%q, want 2/2",
			res.W78.NavCounts.Accounts, res.W78.NavCounts.Providers)
	}

	// (4b) W79 控件类收敛：渲染出来的每个 <button> 仍带 .ad-ctl 别名（分段按钮在 .ad-seg 内，
	// 走 data-scope）。收敛只挪 CSS 定义位置，绝不准在渲染层丢控件类。
	cc := res.W78.ControlClass
	if len(cc.Stray) != 0 {
		t.Errorf("W79 控件类丢失：%d 个 <button> 没有 .ad-ctl 别名（前几个 %v）", len(cc.Stray), cc.Stray)
	}
	if cc.Buttons < 20 || cc.CtlButtons == 0 || cc.SegButtons == 0 {
		t.Errorf("W79 控件类统计异常：buttons=%d ctl=%d seg=%d（样本太小或按钮体系被换掉）",
			cc.Buttons, cc.CtlButtons, cc.SegButtons)
	}

	// (5) 冷却动作：POST /admin/accounts/fake-up/1/cooldown，体 = 下拉选中的 21600
	cd := res.W78.Cooldown
	if cd.URL != "/admin/accounts/fake-up/1/cooldown" {
		t.Fatalf("冷却未真发 POST：%q", cd.URL)
	}
	if !cd.KeyIsSet {
		t.Error("冷却请求没带 X-Management-Key")
	}
	if cd.Body == nil || !strings.Contains(*cd.Body, "21600") || !strings.Contains(*cd.Body, "manual-ui") {
		t.Errorf("冷却体 = %v, want seconds=21600 + reason=manual-ui", cd.Body)
	}
	if !containsAny(cd.Toasts, "已冷却 #1") {
		t.Errorf("冷却后 toast = %v, want 含「已冷却 #1」", cd.Toasts)
	}

	// (6) 探针动作：POST /admin/upstreams/fake-up/probe（体 mode=models）+ 内联徽标
	pr := res.W78.Probe
	if pr.URL != "/admin/upstreams/fake-up/probe" {
		t.Fatalf("探针未真发 POST：%q", pr.URL)
	}
	if pr.Body == nil || !strings.Contains(*pr.Body, "models") {
		t.Errorf("探针体 = %v, want mode=models", pr.Body)
	}
	if !pr.Badge {
		t.Error("探针成功后页面没有内联徽标「200 · 123ms · auth verified」")
	}
	if !containsAny(pr.Toasts, "200 · 123ms · auth verified") {
		t.Errorf("探针 toast = %v, want 含状态/时延/auth", pr.Toasts)
	}
	if !res.W78.ProbeFail.Badge || !containsAny(res.W78.ProbeFail.Toasts, "timeout · 网络不可达") {
		t.Errorf("探针失败面：badge=%v toasts=%v, want 内联「timeout · 网络不可达」+ warn toast",
			res.W78.ProbeFail.Badge, res.W78.ProbeFail.Toasts)
	}

	// (7) 导出：密钥只进请求头（不进 URL），文件名只认 Content-Disposition
	ex := res.W78.Export
	if !strings.Contains(ex.URL, "type=accounts") || !strings.Contains(ex.URL, "format=csv") {
		t.Fatalf("导出未真发 GET /admin/export：%q", ex.URL)
	}
	if !ex.KeyIsSet {
		t.Error("导出请求没带 X-Management-Key（下载面必须走 adminHeaders）")
	}
	if ex.KeyInURL {
		t.Errorf("导出 URL 里出现密钥字节：%q", ex.URL)
	}
	if ex.Download == nil || *ex.Download != "any2api-accounts-20261018.csv" {
		t.Errorf("下载文件名 = %v, want any2api-accounts-20261018.csv（取自 Content-Disposition）", ex.Download)
	}
	if ex.BlobType == nil || !strings.Contains(*ex.BlobType, "text/csv") {
		t.Errorf("Blob 类型 = %v, want text/csv（内容类型照抄响应头）", ex.BlobType)
	}
	if !containsAny(ex.Toasts, "已导出 any2api-accounts-20261018.csv") {
		t.Errorf("导出 toast = %v, want 含文件名", ex.Toasts)
	}
}

// TestAdminUIDomW78NegativeControls W78 行为门负控：四个变异各自必须**红**，
// 而真源在同一判据上**绿**（门能变红才是门）。每个变异都先跑真源前提检查。
func TestAdminUIDomW78NegativeControls(t *testing.T) {
	origAdmin, origAdapter := string(adminUIAsset(t, "admin.js")), string(adminUIAsset(t, "admin-data.js"))

	green := adminUIRunDomDriver(t, []byte(origAdmin), []byte(origAdapter), "accounts")
	if green.W78.Accounts.Len <= 400 || green.W78.Usage.ModelRows != 2 || green.W78.Providers.Ready != 1 {
		t.Fatalf("真源前提不成立：accounts.len=%d modelRows=%d ready=%d",
			green.W78.Accounts.Len, green.W78.Usage.ModelRows, green.W78.Providers.Ready)
	}
	if green.W78.Export.Download == nil || *green.W78.Export.Download != "any2api-accounts-20261018.csv" {
		t.Fatalf("真源导出入参不成立：download=%v", green.W78.Export.Download)
	}

	// 负控 1：抹掉 accounts 渲染入口 → 页面 0 字节（红），真源 > 400（绿）。
	m1 := strings.Replace(origAdmin, `accounts: ["账号与代理", renderAccounts]`, `accounts: ["账号与代理", function () {}]`, 1)
	if m1 == origAdmin {
		t.Fatal("负控 1 变异未生效：找不到 accounts 渲染入口")
	}
	red1 := adminUIRunDomDriver(t, []byte(m1), []byte(origAdapter), "accounts")
	if red1.W78.Accounts.Len != 0 {
		t.Errorf("负控 1：抹掉渲染后 #page-accounts = %d 字节，want 0", red1.W78.Accounts.Len)
	}

	// 负控 2：抹掉逐模型行 → 行数归零（红），真源 2 行（绿）。
	m2 := strings.Replace(origAdmin, `'<tr class="model-usage-row">`, `'<tr class="model-usage-removed">`, 1)
	if m2 == origAdmin {
		t.Fatal("负控 2 变异未生效：找不到逐模型行渲染点")
	}
	red2 := adminUIRunDomDriver(t, []byte(m2), []byte(origAdapter), "accounts")
	if red2.W78.Usage.ModelRows != 0 {
		t.Errorf("负控 2：抹掉逐模型行后行数 = %d, want 0（逐模型用量表没接上真数据）", red2.W78.Usage.ModelRows)
	}

	// 负控 3：抹掉导出面的鉴权头 → 桩按 adminAuth 语义 401 → 下载不发生（红），
	// 真源同一判据下载成功且文件名正确（绿）。密钥仍不得进 URL（两侧都不许）。
	m3 := strings.Replace(origAdapter, `"X-Management-Key": getKey()`, `"X-Not-A-Key": getKey()`, 1)
	if m3 == origAdapter {
		t.Fatal("负控 3 变异未生效：找不到请求头装配点")
	}
	red3 := adminUIRunDomDriver(t, []byte(origAdmin), []byte(m3), "accounts")
	if red3.W78.Export.Download != nil {
		t.Errorf("负控 3：无密钥头的导出竟下载成功（%v）——导出面鉴权门失效", *red3.W78.Export.Download)
	}
	if red3.W78.Export.KeyIsSet {
		t.Error("负控 3：变异源仍带 X-Management-Key —— 负控样本没删干净")
	}
	if red3.W78.Export.KeyInURL {
		t.Error("负控 3：密钥进了 URL（下载面必须走请求头）")
	}
	if !containsAny(red3.W78.Export.Toasts, "导出失败") && !strings.Contains(red3.W78.Export.URL, "type=accounts") {
		t.Errorf("负控 3：401 面没留下失败痕迹 toasts=%v url=%q", red3.W78.Export.Toasts, red3.W78.Export.URL)
	}

	// 负控 4：抹掉 readiness 映射 → 四个就绪态的类全丢（红：徽标退回兜底），
	// 真源 1/1/1 且无兜底徽标（绿）。
	m4 := strings.Replace(origAdapter, "          readiness: p.readiness || \"\",\n", "", 1)
	if m4 == origAdapter {
		t.Fatal("负控 4 变异未生效：找不到 readiness 映射点")
	}
	red4 := adminUIRunDomDriver(t, []byte(origAdmin), []byte(m4), "accounts")
	if red4.W78.Providers.Ready != 0 || red4.W78.Providers.Degraded != 0 || red4.W78.Providers.NotConfigured != 0 {
		t.Errorf("负控 4：抹掉映射后仍有就绪态徽标 ready/degraded/not_configured = %d/%d/%d, want 0/0/0",
			red4.W78.Providers.Ready, red4.W78.Providers.Degraded, red4.W78.Providers.NotConfigured)
	}
	if red4.W78.Providers.BlankReady != 3 {
		t.Errorf("负控 4：抹掉映射后兜底徽标 = %d, want 3（三条目录条目全部失去词表值）", red4.W78.Providers.BlankReady)
	}
}

// containsAny 命中任一子串。
func containsAny(list []string, needle string) bool {
	for _, s := range list {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}
