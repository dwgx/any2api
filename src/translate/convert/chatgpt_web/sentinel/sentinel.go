// Package sentinel 实现 chatgpt.com 网页线 Sentinel 挑战的**离线算法面**：
// p token 生成（两代槽位）+ PoW 求解（两代哈希）+ 第三方求解器客户端接口。
//
// A2A-L19-W41-2（切片 S4 前置；零账号零真机）。本包**不接线**——不注册
// registry、不接 service、不发任何请求；S4 接线时由服务壳消费（KB
// `docs/architecture.md` §实现缺口「挑战求解 = 全新模块」）。
//
// # 两代协议（冻结依据，全部源码级）
//
//	2025 代（18 槽）：
//	  G = leetanshaj/openai-sentinel@6c3a0ed  proof_of_work.py:12-29（get_config 18 槽）、
//	      :31-48（generate_answer：sha3_512 + 前缀比较 + 伪解兜底）、:50-53（diff 钉 "0fffff"）
//	  C = lanqian528/chat2api@85971f0         chatgpt/proofofWork.py:436-458（同构 18 槽）、
//	      :460-491（同构 generate_answer）
//	  端点：POST /backend-api/sentinel/chat-requirements {p:"gAAAAAC…"}（C ChatService.py:183-260）
//	2026 代（25 槽）：
//	  A = DestroyerDarkNess/chatgpt_web_python_api@76e0731 web_client.py:85-127（25 槽，
//	      注释：槽 3=PoW 尝试次数、槽 9=耗时）、:130-137（_prepare_token → "gAAAAAC"）、
//	      :139-151（_pow_hash_hex：FNV-1a 32 位 + avalanche 收尾）、:153-181（_proof_token →
//	      "gAAAAAB…~S"，500k 次未命中抛错）
//	  佐证（同一算法的另两份独立实现，落盘深读）：
//	  K = verssache/chatgpt-creator internal/sentinel/fnv.go:6-19（FNV1a32 + 同一收尾）
//	      + generator.go:83-109（PoW 循环：config[3]=i、config[9]=耗时、字符串前缀比较）
//	  T = myfanhua/turb-gpt-free-register core/sentinel.py:199-243（Python 重写版；同算法）
//	      + sentinel/sdk.js（官方压缩件反混淆：同一乘法常量与 `Math.imul`/`>>>0` 语义）
//	  三份独立实现 + 官方 sdk.js 反混淆逐字一致（chatgpt-creator 深读 §3.4-3.5）。
//
// # 两代差异（本包逐条冻结；细节见各函数注）
//
//	                   2025（V2025）                    2026（V2026）
//	槽位               18                               25（槽 3/9 动态）
//	requirements p     PoW 求解（diff 钉 "0fffff"）      静态编码（[3]=1、[9]=槽13）
//	PoW 哈希           sha3_512(seed + base64(config))  FNV-1a32+avalanche(seed + base64(config))
//	比较               **严格 <**（见 pow.go 注）        **≤**（字符串前缀）
//	proof 后缀         无                               "~S"
//	求解失败           参考实现返回伪解串（wQ8Lk5…）     参考实现抛错
//
// # 用法（两代同一接口，生成器按版本参数）
//
//	b, err := sentinel.NewBuilder(sentinel.V2026)
//	cfg := sentinel.NewConfig2026(sentinel.Config2026Params{UserAgent: ua, ClientVersion: v})
//	p, err := b.RequirementsToken(cfg)                  // → "gAAAAAC…"
//	tok, err := b.ProofToken(cfg, seed, difficulty)     // 服务端下发 seed/difficulty 后
//	ok, err := sentinel.Verify(sentinel.V2026, tok, seed, difficulty)
//
// # 红线与未验证面
//
// 本包只做离线算法（零真机、零账号）。算法字节级由参考实现真值向量钉死
// （testdata/pow_vectors.json，由 tmp/lanes/W41-sentinel/gen_vectors.py 直接跑
// 参考实现源码生成）；**服务端是否接受这些 token 未验证**（需要真机，S4 授权后
// 才能测）——`[UNVERIFIED]` 面见 topic §求解器模块（W41-2）与报告
// research/briefs/reports/A2A-L19-W41-2.md。
//
// 注意命名：chat2api 把 arkose 拆成 `'ark'+'ose'` 规避平台扫描
// （ChatService.py:294）——那是**对方平台**的策略；本仓库无此顾虑，照常拼写。
package sentinel

import (
	"errors"
	"math/rand/v2"
	"time"
)

// Version 标识 Sentinel 协议代际（两代以同一接口暴露；生成器按版本参数）。
type Version int

const (
	// V2025 2025 代：18 槽 config + sha3_512 PoW（openai-sentinel / chat2api）。
	V2025 Version = iota
	// V2026 2026 代：25 槽 config + FNV-1a32+avalanche PoW（官方 web bundle 语义；
	// web_client.py / chatgpt-creator / turb-gpt 三实现互证）。
	V2026
)

// String 返回代际名（"2025" / "2026"）；未知值返回 "unknown"。
func (v Version) String() string {
	switch v {
	case V2025:
		return "2025"
	case V2026:
		return "2026"
	}
	return "unknown"
}

// SlotCount 返回该代际 config 的槽位数（2025→18；2026→25；未知→0）。
func (v Version) SlotCount() int {
	switch v {
	case V2025:
		return 18
	case V2026:
		return 25
	}
	return 0
}

// Config 是位置敏感的槽位数组（JSON 序列化后逐字节上线）。
//
// 允许的槽位值类型：nil、bool、string、各宽度 int/uint、float32/float64、
// json.Number（原样透传）。序列化与 Python `json.dumps(config,
// separators=(",",":"), ensure_ascii=False)` 逐字节对齐（encode.go）。
type Config = []any

// 槽位语义（两代共用下标；2026 版注释见 web_client.py:85-127，2025 版见
// proof_of_work.py:12-29）。
const (
	// SlotAttempt 槽 3：PoW 尝试次数（2025 解填 i、2026 解填 attempt；两代的
	// requirements token 里分别是解出时的 i / 常量 1）。
	SlotAttempt = 3
	// SlotElapsed 槽 9：耗时/时间槽（2025 解填 i>>1——**是尝试次数的一半，不是
	// 真实耗时**；2026 解填真实耗时毫秒，requirements token 里填槽 13 的值）。
	SlotElapsed = 9
)

// token 前缀与后缀（KB §Sentinel 握手与 PoW）。
const (
	// PrefixRequirements requirements token 前缀（两代同款）。
	PrefixRequirements = "gAAAAAC"
	// PrefixProof proof token 前缀（两代同款）。
	PrefixProof = "gAAAAAB"
	// Suffix2026 2026 代 proof token 后缀（web_client.py:179；sdk.js 反混淆
	// `… ? i+"~S" : null` 同构）。
	Suffix2026 = "~S"
	// RequirementsDifficulty2025 2025 代 requirements token 的自求解难度——
	// 参考实现把 diff 钉成 "0fffff"（openai-sentinel proof_of_work.py:50-53；
	// chat2api proofofWork.py:494-495）。
	RequirementsDifficulty2025 = "0fffff"
	// LegacyFallbackPrefix 伪解兜底串前缀（两代同款；仅记录，不默认启用——
	// 见 LegacyFallbackToken）。
	LegacyFallbackPrefix = "wQ8Lk5FbGpA2NcR9dShT6gYjU7VxZ4D"
)

// 本包错误。全部可 errors.Is 判定。
var (
	// ErrUnknownVersion 未知代际。
	ErrUnknownVersion = errors.New("sentinel: unknown version")
	// ErrSlotCount config 槽位数与该代际不符（2025 必须 18、2026 必须 25——
	// 2026 参考实现显式校验 `len(config) != 25` 抛错，web_client.py:131,161）。
	ErrSlotCount = errors.New("sentinel: config slot count mismatch")
	// ErrSlotType 槽位值类型不受支持（encode.go 的编码面）。
	ErrSlotType = errors.New("sentinel: unsupported slot value type")
	// ErrDifficulty 难度串非法（2025：偶数长度 hex；2026：[0-9a-f]+）。
	ErrDifficulty = errors.New("sentinel: invalid difficulty")
	// ErrUnsolved 达到最大尝试次数仍未解出（参考实现 2025 返回伪解串、2026 抛错；
	// 本包默认两代都报错——伪解串需显式 WithLegacyFallback 才返回）。
	ErrUnsolved = errors.New("sentinel: proof-of-work not solved within max attempts")
	// ErrToken token 形态非法（前缀/后缀/base64 面）。
	ErrToken = errors.New("sentinel: malformed token")
)

// MaxAttemptsDefault 参考实现的两代求解上限（proof_of_work.py:8 的
// MAX_ITERATION = 500000；web_client.py:158 的 max_attempts=500_000）。
const MaxAttemptsDefault = 500_000

// Builder 按代际构造 Sentinel p token（**两代以同一接口暴露**；brief §Change ①）。
type Builder interface {
	// Version 返回本 builder 的代际。
	Version() Version
	// RequirementsToken 构造发给 chat-requirements 的初始 p token。
	//
	//	2025：对 config 做一次 diff="0fffff" 的自求解，返回 "gAAAAAC" + 解出的
	//	      base64（proof_of_work.py:50-53 的 get_requirements_token）。
	//	2026：静态编码——prepared[3]=1、prepared[9]=槽13 的值，返回 "gAAAAAC" +
	//	      base64（web_client.py:130-137 的 _prepare_token）。
	RequirementsToken(cfg Config) (string, error)
	// ProofToken 求解服务端下发的 PoW 挑战（seed/difficulty），构造 proof token。
	//
	//	2025：返回 "gAAAAAB" + 解出的 base64（无后缀）。
	//	2026：返回 "gAAAAAB" + base64 + "~S"。
	//
	// 未解出：默认 ErrUnsolved（WithLegacyFallback 时返回参考伪解串，见该选项）。
	ProofToken(cfg Config, seed, difficulty string) (string, error)
}

// options Builder 的可选项集合（两代共用；不适用项被各自忽略）。
type options struct {
	maxAttempts    int
	seedSrc        func() string
	clock          func() time.Duration
	legacyFallback bool
}

// Option 配置 Builder。
type Option func(*options)

// WithMaxAttempts 覆盖求解上限（默认 MaxAttemptsDefault = 500000，与两代参考
// 实现一致）。n <= 0 视为默认。
func WithMaxAttempts(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.maxAttempts = n
		}
	}
}

// WithSeedSource 覆盖 2025 代 requirements token 的自种子源。
//
// 参考实现用 `format(random.random())`（Python float repr，如 "0.123456789"）；
// 默认等价于 Python repr 的随机 float 串（encode.go 的 appendPythonFloat）。
// 仅 V2025 的 RequirementsToken 使用（2026 的 requirements 不求解）。
func WithSeedSource(fn func() string) Option {
	return func(o *options) {
		if fn != nil {
			o.seedSrc = fn
		}
	}
}

// WithClock 覆盖 2026 代 PoW 循环的单调时钟（纳秒起点任意；求解开始与每次尝试
// 各取一次，差值为耗时毫秒——web_client.py:153-181 的 perf_counter 语义）。
// 测试注入可让 token 字节级确定。仅 V2026 的 ProofToken 使用。
func WithClock(fn func() time.Duration) Option {
	return func(o *options) {
		if fn != nil {
			o.clock = fn
		}
	}
}

// WithLegacyFallback 求解失败时返回参考实现的伪解兜底串（**默认关闭**）。
//
// 语义（仅记录，不默认启用——brief §Change ②）：两代参考实现都定义了
// "wQ8Lk5FbGpA2NcR9dShT6gYjU7VxZ4D" 前缀的伪解串——
//
//	2025 openai-sentinel：前缀 + base64('"'+seed+'"')（proof_of_work.py:46-48）；
//	2026 turb-gpt Python 版：前缀 + base64('["e"]')（core/sentinel.py:242-243；
//	    注释「服务端会拒绝」）；官方 web bundle 侧无此路径（_proof_token 抛错）。
//
// 开启后：求解失败返回 <token 前缀> + LegacyFallbackToken(seed)（与参考实现
// get_answer_token 的拼装一致——proof_of_work.py:57-61 先拼前缀）。
// 关闭（默认）：返回 ErrUnsolved——**绝不把伪解当成功发上线**。
func WithLegacyFallback(on bool) Option {
	return func(o *options) { o.legacyFallback = on }
}

// defaultSeedSrc 等价 Python `format(random.random())`（math/rand/v2 全局源已
// 随机播种；float repr 见 encode.go）。
func defaultSeedSrc() string { return pythonFloatString(rand.Float64()) }

// processStart 是默认单调时钟的起点（仅用于耗时差值，绝对值无意义）。
var processStart = time.Now()

// defaultClock 进程单调时钟（time.Since 走 monotonic reading）。
func defaultClock() time.Duration { return time.Since(processStart) }

func resolveOptions(opts []Option) options {
	o := options{
		maxAttempts: MaxAttemptsDefault,
		seedSrc:     defaultSeedSrc,
		clock:       defaultClock,
	}
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	return o
}

// NewBuilder 按版本参数构造 Builder（两代同一接口）。
func NewBuilder(v Version, opts ...Option) (Builder, error) {
	o := resolveOptions(opts)
	switch v {
	case V2025:
		return &builder2025{opts: o}, nil
	case V2026:
		return &builder2026{opts: o}, nil
	}
	return nil, ErrUnknownVersion
}

// New2025 构造 2025 代 Builder（NewBuilder(V2025) 的便捷形式）。
func New2025(opts ...Option) Builder {
	return &builder2025{opts: resolveOptions(opts)}
}

// New2026 构造 2026 代 Builder（NewBuilder(V2026) 的便捷形式）。
func New2026(opts ...Option) Builder {
	return &builder2026{opts: resolveOptions(opts)}
}
