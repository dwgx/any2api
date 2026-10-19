// W5-22 条目 4：错误体兜底提取（one-api 7 路径表，MIT——P26 §quirks）。
package ir

import "testing"

func TestSalvageErrorTable(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantMsg string
		wantTyp string
		wantCod string
		wantPar string
	}{
		{"error_object", `{"error":{"message":"boom","type":"rate_limit_error","code":"429","param":"model"}}`, "boom", "rate_limit_error", "429", "model"},
		{"error_object_msg", `{"error":{"msg":"from msg"}}`, "from msg", "api_error", "", ""},
		{"error_string", `{"error":"plain string error"}`, "plain string error", "api_error", "", ""},
		{"top_message", `{"message":"top level"}`, "top level", "api_error", "", ""},
		{"top_msg", `{"msg":"msg level"}`, "msg level", "api_error", "", ""},
		{"top_err", `{"err":"err level"}`, "err level", "api_error", "", ""},
		{"top_error_msg", `{"error_msg":"error_msg level"}`, "error_msg level", "api_error", "", ""},
		{"header_message", `{"header":{"message":"cf blocked"}}`, "cf blocked", "api_error", "", ""},
		{"response_error_message", `{"response":{"error":{"message":"resp nested"}}}`, "resp nested", "api_error", "", ""},
		{"numeric_code", `{"code":503}`, "upstream returned an error", "api_error", "503", ""},
		{"type_only", `{"type":"overloaded_error"}`, "upstream returned an error", "overloaded_error", "", ""},
		// 优先级：error.message 先于顶层（one-api 同序）。
		{"object_beats_top", `{"error":{"message":"inner"},"message":"outer"}`, "inner", "api_error", "", ""},
		// 顶层 message 先于 msg（同序表）。
		{"message_beats_msg", `{"message":"m1","msg":"m2"}`, "m1", "api_error", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := SalvageError([]byte(tc.body), "test-src")
			if e.Message != tc.wantMsg || e.Type != tc.wantTyp || e.Code != tc.wantCod || e.Param != tc.wantPar {
				t.Fatalf("SalvageError(%s) = %+v", tc.body, e)
			}
			if e.Source != "test-src" {
				t.Fatalf("source = %q", e.Source)
			}
		})
	}
}

func TestSalvageErrorFallback(t *testing.T) {
	// 非 JSON：泛型 api_error + Raw 保真（渲染器绝不回传 Raw——包纪律 3）。
	raw := []byte("<html>502 Bad Gateway</html>")
	e := SalvageError(raw, "gemini")
	if e.Type != "api_error" || e.Message != "upstream returned a non-JSON or non-standard error body" || e.Source != "gemini" {
		t.Fatalf("non-JSON = %+v", e)
	}
	if string(e.Raw) != string(raw) {
		t.Fatalf("raw = %q", e.Raw)
	}
	// JSON 但无可提取信息（空对象）：同款泛型 + Raw。
	e = SalvageError([]byte(`{}`), "openai")
	if e.Type != "api_error" || e.Message != "upstream returned a non-JSON or non-standard error body" {
		t.Fatalf("empty object = %+v", e)
	}
	if string(e.Raw) != `{}` {
		t.Fatalf("raw = %q", e.Raw)
	}
	// JSON 数组（非对象）：泛型。
	if e := SalvageError([]byte(`[1,2]`), "openai"); e.Message != "upstream returned a non-JSON or non-standard error body" {
		t.Fatalf("array = %+v", e)
	}
	// error: null 视为缺失（继续走兜底路径）。
	e = SalvageError([]byte(`{"error":null,"msg":"after null"}`), "openai")
	if e.Message != "after null" {
		t.Fatalf("error null = %+v", e)
	}
}
