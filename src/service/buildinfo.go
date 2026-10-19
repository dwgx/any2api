// buildinfo.go —— 构建期注入的版本信息（dashboard P0-2；设计
// `docs/admin-dashboard.md` §5.8「部署件」· §6.1 #7「build 面需在构建期注入
// commit（-ldflags）」· §9 P0-2 行）。
//
// 注入方式（构建脚本/命令行；**本 slice 不改构建脚本**——变量先做成可注入）：
//
//	go build -ldflags "\
//	  -X github.com/dwgx/any2api/service.BuildCommit=$(git rev-parse --short HEAD) \
//	  -X github.com/dwgx/any2api/service.BuildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
//	  ./cmd/any2api
//
// 未注入（`go test` / `go run` / 普通构建）= 哨兵值 `"unknown"`（**显式「未知」
// 不是空串**——空串在 dashboard 上分不清「没注入」与「字段丢了」）。
//
// 只读面：值只进 `GET /admin/system` 的 `build` 节（admin_meta.go）；绝不参与
// 任何请求路径决策、绝不写盘、绝不进日志以外的面。
package service

// BuildCommit 构建期注入的 commit（短哈希/全哈希随构建脚本；未注入 = "unknown"）。
var BuildCommit = "unknown"

// BuildDate 构建期注入的构建时刻（建议 RFC3339 UTC；未注入 = "unknown"）。
var BuildDate = "unknown"
