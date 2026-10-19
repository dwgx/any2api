package sentinel

import (
	"encoding/json"
	"os"
	"testing"
)

// testdata/pow_vectors.json 是**真值向量**：由 tmp/lanes/W41-sentinel/gen_vectors.py
// 直接运行参考实现生成（2025：openai-sentinel proof_of_work.py 原样导入；
// 2026：web_client.py 的 _encode_sentinel_config/_pow_hash_hex/_prepare_token/
// _proof_token 原函数源码 exec + 脚本化时钟）。生成脚本、参考仓 commit 与
// 脚本化参数全部记录在向量文件 provenance/config_defaults.scripted 节。
//
// 解码用 json.Decoder.UseNumber：Python 的数字字面量（int 3000 与 float 250.0）
// 原样保留，逐字节进编码器。

type tokenVector struct {
	Seed                 string  `json:"seed"`
	Difficulty           string  `json:"difficulty"`
	Config               []any   `json:"config"`
	Expected             string  `json:"expected"`
	Solved               bool    `json:"solved"`
	Verify               bool    `json:"verify"`
	SeedMutated          string  `json:"seed_mutated"`
	VerifySeedMutated    bool    `json:"verify_seed_mutated"`
	PayloadMutated       string  `json:"payload_mutated"`
	PayloadMutXor        int     `json:"payload_mut_xor"`
	VerifyPayloadMutated bool    `json:"verify_payload_mutated"`
	ClockStepS           float64 `json:"clock_step_s"`
	Attempts             int     `json:"attempts"`
}

type requirementVector struct {
	Seed       string `json:"seed"`
	Difficulty string `json:"difficulty"`
	Config     []any  `json:"config"`
	Expected   string `json:"expected"`
	Verify     bool   `json:"verify"`
}

type encodeVector struct {
	Config   []any  `json:"config"`
	Expected string `json:"expected"`
}

type vectors struct {
	Tokens2025       []tokenVector       `json:"tokens_2025"`
	Requirements2025 []requirementVector `json:"requirements_2025"`
	FNV2026          []struct {
		Input  string `json:"input"`
		Digest string `json:"digest"`
	} `json:"fnv2026"`
	Tokens2026       []tokenVector       `json:"tokens_2026"`
	Requirements2026 []requirementVector `json:"requirements_2026"`
	FloatRepr        [][2]string         `json:"float_repr"`
	StringEscape     [][2]string         `json:"string_escape"`
	ConfigEncode     []encodeVector      `json:"config_encode"`
	ConfigDefaults   struct {
		Scripted struct {
			RandomChoice string  `json:"random_choice"`
			RandomRandom float64 `json:"random_random"`
			UUIDHex      string  `json:"uuid_hex"`
			PerfCounterS float64 `json:"perf_counter_s"`
			Now2025UTC   string  `json:"now_2025_utc"`
			Now2026UTC   string  `json:"now_2026_utc"`
			Now2026Local string  `json:"now_2026_local"`
			CPUCount     int     `json:"cpu_count"`
		} `json:"scripted"`
		Config2025     []any  `json:"config2025"`
		Config2026     []any  `json:"config2026"`
		Config2025JSON string `json:"config2025_json"`
		Config2026JSON string `json:"config2026_json"`
		UserAgent      string `json:"user_agent"`
	} `json:"config_defaults"`
}

func loadVectors(t *testing.T) vectors {
	t.Helper()
	f, err := os.Open("testdata/pow_vectors.json")
	if err != nil {
		t.Fatalf("open vectors: %v", err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.UseNumber()
	var v vectors
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("decode vectors: %v", err)
	}
	if len(v.Tokens2025) == 0 || len(v.Tokens2026) == 0 {
		t.Fatal("vectors incomplete")
	}
	return v
}
