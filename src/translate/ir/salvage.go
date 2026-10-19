package ir

import (
	"bytes"
	"encoding/json"
)

// ---------------------------------------------------------------------------
// 错误体兜底提取（W5-22 条目 4；P26 §quirks：one-api `GeneralErrorResponse`
// 7 字段兜底表——`relay/controller/error.go:18-52`，MIT 可借鉴）
// ---------------------------------------------------------------------------

// SalvageError 从任意上游错误体里兜底提取最小错误信息（上游错误体形态各异时的
// 最小信息抢救；P26 后续）。提取面（one-api ToMessage 的同序表 + 常见嵌套形态）：
//
//  1. `error` 为对象（message/msg/type/code/param 取非空）或字符串；
//  2. 顶层兜底路径（按序）：message / msg / err / error_msg；
//  3. `header.message`（Cloudflare 形态）；
//  4. `response.error.message`（Responses 失败对象形态）；
//  5. 顶层 type / code / param（错误对象内未取到时）。
//
// 无任何可提取信息（非 JSON / 结构不认）→ 泛型 api_error + Raw 保真（Raw 仅供
// 诊断，渲染器绝不回传——包纪律 3）。status / request-id / retry-after 是 HTTP
// 层信息，不在本函数面内（服务壳按 resp.StatusCode 与响应头处理）。
func SalvageError(body []byte, source string) *Error {
	raw := append(json.RawMessage(nil), body...)
	generic := func() *Error {
		return &Error{
			Type:    "api_error",
			Message: "upstream returned a non-JSON or non-standard error body",
			Source:  source,
			Raw:     raw,
		}
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(body, &doc); err != nil || doc == nil {
		return generic()
	}
	e := &Error{Source: source, Raw: raw}

	// 1. error 对象 / error 字符串。
	if errRaw, ok := doc["error"]; ok && !rawIsNull(errRaw) {
		var obj struct {
			Message string          `json:"message"`
			Msg     string          `json:"msg"`
			Type    string          `json:"type"`
			Code    json.RawMessage `json:"code"`
			Param   json.RawMessage `json:"param"`
		}
		if err := json.Unmarshal(errRaw, &obj); err == nil {
			e.Message = firstNonEmpty(obj.Message, obj.Msg)
			e.Type = obj.Type
			e.Code = salvageStringOrNumber(obj.Code)
			e.Param = salvageString(obj.Param)
		} else {
			var s string
			if json.Unmarshal(errRaw, &s) == nil {
				e.Message = s
			}
		}
	}
	// 2. 顶层兜底路径（one-api 同序）。
	if e.Message == "" {
		for _, k := range []string{"message", "msg", "err", "error_msg"} {
			if s := salvageString(doc[k]); s != "" {
				e.Message = s
				break
			}
		}
	}
	// 3. header.message（Cloudflare 形态）。
	if e.Message == "" {
		var h struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(doc["header"], &h); err == nil {
			e.Message = h.Message
		}
	}
	// 4. response.error.message（Responses 失败对象形态）。
	if e.Message == "" {
		var r struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(doc["response"], &r); err == nil {
			e.Message = r.Error.Message
		}
	}
	// 5. 顶层 type/code/param（错误对象内未取到时）。
	if e.Type == "" {
		e.Type = salvageString(doc["type"])
	}
	if e.Code == "" {
		e.Code = salvageStringOrNumber(doc["code"])
	}
	if e.Param == "" {
		e.Param = salvageString(doc["param"])
	}

	if e.Message == "" && e.Type == "" && e.Code == "" && e.Param == "" {
		return generic()
	}
	if e.Type == "" {
		e.Type = "api_error"
	}
	if e.Message == "" {
		e.Message = "upstream returned an error"
	}
	return e
}

// salvageString JSON 字符串值（非字符串/缺失 → ""）。
func salvageString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s
}

// salvageStringOrNumber JSON 字符串或数值值（code 双形态：字符串枚举 / HTTP 数值）。
func salvageStringOrNumber(raw json.RawMessage) string {
	if s := salvageString(raw); s != "" {
		return s
	}
	if len(raw) == 0 {
		return ""
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err != nil {
		return ""
	}
	return n.String()
}

// rawIsNull 值是否为 JSON null（空 RawMessage 视为缺失，不算 null）。
func rawIsNull(raw json.RawMessage) bool {
	return len(raw) > 0 && bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// firstNonEmpty 首个非空字符串。
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
