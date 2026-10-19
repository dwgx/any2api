// Package toolcall 实现 L19 转换内核的工具调用「出口层」：上游帧合并
// （merge 7 步决策表）、三层 JSON 修复（字符 / 结构 / 粘连，强制复验）、
// 双重编码解包、Bug C（缺 required 键）闸门、出口 `{}` 纪律，以及
// index 归一与历史规整。
//
// 规格：research/topics/toolcall-assembly-spec/README.md（P66，11 组回归用例）；
// 对拍 oracle：tmp/lanes/P66-toolcall-spec/check_cases.py（Python 移植，逐条一致）。
//
// 设计纪律（规格 §4 安全契约）：
//  1. 只在 JSON 解析失败时介入修复（合法 JSON 永不重序列化——字节保真）；
//  2. 三层修复每层结果都必须重新 parse 通过才采用（强制复验）；
//  3. 修不好 == 不修（失败态 INVALID_TOOL_INPUT，绝不静默置 {}）；
//  4. 归因诊断（Defect）纯观测，绝不进控制流。
//
// 全程 String 级重组（不做按字节索引的 rune 边界操作）；仅标准库。
package toolcall
