package sentinel

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strings"
	"time"
)

// 本文件 = 两代默认 config 构造器（镜像参考实现的 get_config /
// _default_sentinel_config；槽位语义逐条对照，见包注）。
//
// 与参考实现的差异（有意，逐条注明）：
//   - 随机源、时钟、UUID、CPU 数、时区全部可注入（参考实现直接读全局/宿主），
//     以便测试与跨语言向量对齐；
//   - 候选列表为空时取零值（"" / 0）而不是 Python 的 IndexError（导航/文档/
//     window 键列表与 cores 在参考实现里由调用方保证非空——本包对空列表降级，
//     不崩）。

// Rand 是配置构造器的随机源（*rand.Rand 不满足 IntN/Float64 组合时可用适配器；
// math/rand/v2 的 *rand.Rand 直接满足）。
type Rand interface {
	IntN(n int) int
	Float64() float64
}

// globalRand 默认随机源（math/rand/v2 全局函数，进程启动即随机播种）。
type globalRand struct{}

func (globalRand) IntN(n int) int   { return rand.IntN(n) }
func (globalRand) Float64() float64 { return rand.Float64() }

// Config2025Params 是 NewConfig2025 的输入（零值 = 参考实现同款默认）。
type Config2025Params struct {
	// UserAgent 槽 4（必带；与 HTTP 层 UA 必须同源——指纹一致性）。
	UserAgent string
	// Scripts 槽 5 的候选（参考实现 cached_scripts；空 → ""）。
	Scripts []string
	// DPL 槽 6（参考实现 cached_dpl；chatgpt 页 data-build/dpl）。
	DPL string
	// NavigatorKeys / DocumentKeys / WindowKeys 槽 10/11/12 的候选
	// （参考实现 navigator_key/document_key/window_key；空 → ""）。
	NavigatorKeys []string
	DocumentKeys  []string
	WindowKeys    []string
	// Cores 槽 16 的候选（参考实现 cores；空 → 0）。
	Cores []int
	// Rand 随机源（nil → 全局源）。
	Rand Rand
	// Now 墙钟（零值 → time.Now()）。
	Now time.Time
	// PerfMS 槽 13 的 perf_counter()*1000（毫秒；0 → 进程单调时钟）。
	PerfMS float64
	// UUIDHex 槽 14 的 uuid4（32 位小写 hex；空 → 随机 uuid4，置版本位）。
	UUIDHex string
}

// NewConfig2025 构造 2025 代 18 槽 config（proof_of_work.py:12-29 /
// chat2api proofofWork.py:436-458 的 get_config 镜像）。
//
// 槽 3/9 以占位 0 出厂——求解时被求解器替换（SlotAttempt/SlotElapsed）。
func NewConfig2025(p Config2025Params) Config {
	r := p.Rand
	if r == nil {
		r = globalRand{}
	}
	now := p.Now
	if now.IsZero() {
		now = time.Now()
	}
	perf := p.PerfMS
	if perf == 0 {
		perf = defaultPerfMS()
	}
	uuidHex := p.UUIDHex
	if uuidHex == "" {
		uuidHex = randomUUIDHex()
	}
	return Config{
		choice(r, []int{1920 + 1080, 2560 + 1440, 1920 + 1200, 2560 + 1600}),
		parseTime2025(now),
		4294705152,
		0,
		p.UserAgent,
		choiceOr(r, p.Scripts, ""),
		p.DPL,
		"en-US",
		"en-US,es-US,en,es",
		0,
		choiceOr(r, p.NavigatorKeys, ""),
		choiceOr(r, p.DocumentKeys, ""),
		choiceOr(r, p.WindowKeys, ""),
		perf,
		formatUUID(uuidHex),
		"",
		choice(r, p.Cores),
		msFromTime(now) - perf,
	}
}

// Config2026Params 是 NewConfig2026 的输入。
type Config2026Params struct {
	// UserAgent 槽 4。
	UserAgent string
	// ClientVersion 槽 6（oai-client-version / data-build；漂移=403，KB §陷阱）。
	ClientVersion string
	// Language 槽 7/8 的主语言（如 "en-US"；空 → "en-US"）。
	Language string
	// SessionID 槽 14（sentinel sid；空 → 随机 uuid4）。
	SessionID string
	// Location 槽 1 的时区（nil → time.Local；参考实现用本地时区）。
	Location *time.Location
	// Rand 随机源（nil → 全局源）。
	Rand Rand
	// Now 墙钟（零值 → time.Now()）。
	Now time.Time
	// PerfMS 槽 13 的 perf_counter()*1000（0 → 进程单调时钟）。
	PerfMS float64
	// UUIDHex 槽 14 的 uuid4 hex（空 → 随机 uuid4，置版本位）。
	UUIDHex string
	// CPUCount 槽 16（0 → runtime.NumCPU()；下限 1）。
	CPUCount int
	// ReactKey 槽 11（空 → "_reactListening"+uuidHex[:10]，参考实现同款）。
	ReactKey string
}

// NewConfig2026 构造 2026 代 25 槽 config（web_client.py:85-127 的
// _default_sentinel_config 镜像）。
//
// 槽 3/9 以随机 float 出厂（参考实现 random.random()）；requirements token 用
// 槽 3=1、槽 9=槽 13 的静态替换，PoW 用尝试次数/耗时替换。
func NewConfig2026(p Config2026Params) Config {
	r := p.Rand
	if r == nil {
		r = globalRand{}
	}
	now := p.Now
	if now.IsZero() {
		now = time.Now()
	}
	perf := p.PerfMS
	if perf == 0 {
		perf = defaultPerfMS()
	}
	uuidHex := p.UUIDHex
	if uuidHex == "" {
		uuidHex = randomUUIDHex()
	}
	loc := p.Location
	if loc == nil {
		loc = time.Local
	}
	cpu := p.CPUCount
	if cpu <= 0 {
		cpu = runtime.NumCPU()
	}
	if cpu < 1 {
		cpu = 1
	}
	react := p.ReactKey
	if react == "" {
		suffix := uuidHex
		if len(suffix) > 10 {
			suffix = suffix[:10]
		}
		react = "_reactListening" + suffix
	}
	lang := p.Language
	primary := strings.SplitN(lang, "-", 2)[0]
	if primary == "" {
		primary = "en"
	}
	langs := "en"
	if lang != "" && lang != "en" {
		langs = lang + ",en"
	}
	sid := p.SessionID
	if sid == "" {
		sid = formatUUID(uuidHex)
	}
	return Config{
		1366 + 768,
		localTime2026(now, loc),
		4294967296,
		r.Float64(),
		p.UserAgent,
		nil,
		p.ClientVersion,
		primary,
		langs,
		r.Float64(),
		"vendor\u2212Google Inc.", // U+2212 MINUS SIGN——web_client.py:113 原文如此
		react,
		"onmessage",
		perf,
		sid,
		"",
		cpu,
		msFromTime(now),
		0, 0, 0, 0, 0, 0, 0,
	}
}

// choice 从列表随机取一（空列表 → 零值；参考实现会 IndexError）。
func choice[T any](r Rand, xs []T) T {
	if len(xs) == 0 {
		var zero T
		return zero
	}
	return xs[r.IntN(len(xs))]
}

// choiceOr 空列表 → fallback（参考实现 `random.choice(x) if x else ""`）。
func choiceOr[T any](r Rand, xs []T, fallback T) T {
	if len(xs) == 0 {
		return fallback
	}
	return xs[r.IntN(len(xs))]
}

// parseTime2025 复刻 get_parse_time（proof_of_work.py:10-12）：固定 -5 时区 +
// 硬编码后缀（参考实现不做 DST）。
func parseTime2025(t time.Time) string {
	est := t.In(time.FixedZone("EST", -5*3600))
	return est.Format("Mon Jan 02 2006 15:04:05") + " GMT-0500 (Eastern Standard Time)"
}

// localTime2026 复刻 `local_now.strftime("%a %b %d %Y %H:%M:%S GMT%z (%Z)")`
// （web_client.py:101-102）。
func localTime2026(t time.Time, loc *time.Location) string {
	lt := t.In(loc)
	name, off := lt.Zone()
	return lt.Format("Mon Jan 02 2006 15:04:05") + " GMT" + formatOffset(off) + " (" + name + ")"
}

// formatOffset 输出 %z 形态（±HHMM；含分钟面，如 +0530）。
func formatOffset(off int) string {
	sign := "+"
	if off < 0 {
		sign = "-"
		off = -off
	}
	return fmt.Sprintf("%s%02d%02d", sign, off/3600, (off%3600)/60)
}

// msFromTime 复刻 `time.time() * 1000`（秒 → 毫秒，两次舍入与 Python 一致：
// 先取秒的 float，再乘 1000）。
func msFromTime(t time.Time) float64 {
	return (float64(t.Unix()) + float64(t.Nanosecond())/1e9) * 1000
}

// defaultPerfMS 复刻 `time.perf_counter() * 1000`（进程单调时钟毫秒；绝对值无
// 意义，只要求自洽——槽 13 与槽 17 相减还原 epoch 毫秒）。
func defaultPerfMS() float64 { return time.Since(processStart).Seconds() * 1000 }

// randomUUIDHex 生成 uuid4 的 32 位 hex（版本/变体位已置——Python uuid4 语义）。
func randomUUIDHex() string {
	var b [16]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		panic("sentinel: crypto/rand failed: " + err.Error())
	}
	b[6] = (b[6] & 0x0F) | 0x40
	b[8] = (b[8] & 0x3F) | 0x80
	return hex.EncodeToString(b[:])
}

// formatUUID 把 32 位 hex 格式化为 8-4-4-4-12；长度不为 32 时原样返回（调用方
// 注入的是完整字符串时不动它）。
func formatUUID(hex32 string) string {
	if len(hex32) != 32 {
		return hex32
	}
	return hex32[:8] + "-" + hex32[8:12] + "-" + hex32[12:16] + "-" + hex32[16:20] + "-" + hex32[20:]
}
