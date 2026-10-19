// turn_hook.go 请求-回合落账钩子（P1-3；设计 `claude2api-public-service`
// §3.4；board A2A-L19-W65-1）。
//
// 形态：
//   - 触发点 = `serve()` 收尾的 `defer s.recordTurn(dr)`（紧邻 auditRequest，
//     LIFO 先于审计执行）；
//   - 行粒度 = **每个 attempt 一行** `turns`（request_id 与 attempts 日志行
//     join；`turns.conv_id NOT NULL` → 只有带合法 `X-A2A-Conversation-Id` 的
//     UI 请求才落，非 UI 流量靠 audit_log/attempts——见规格 §2.6.5）；
//   - 台账 = 每请求（不是每 attempt）`session:<sid>`（gated 再 `key:<sha8>`）
//     各 +1 请求 + costUnits；**不写 global**（global 已由 budgetLedger.admit
//     写，避免双计）；
//   - 纪律：失败只 `storeError`（log + ring），**绝不影响响应**；零凭据字节
//     （upstream 只记名字，credential 只记索引）。
package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// turnUsageJSON turns.usage_json 形态（四键；规格 §2.6.4）。
type turnUsageJSON struct {
	In      int64 `json:"in"`
	Out     int64 `json:"out"`
	TTFTMs  int64 `json:"ttft_ms"`
	TotalMs int64 `json:"total_ms"`
}

// turnStatus attempt 终态 → turns.status（规格 §2.6.3；顺序即优先级）。
func turnStatus(ar *attemptRecord) string {
	if ar == nil {
		return "incomplete"
	}
	switch {
	case ar.delivered:
		return "completed"
	case ar.disconnected:
		return "cancelled"
	case ar.failed:
		return "failed"
	default:
		return "incomplete"
	}
}

// attachTurnContext 回填 turn 落账归属（handle 入口）：会话 sid/key_id 恒回填
// （内存记账用），conv id 仅在「头存在 + store 命中 + SID 归属一致 + 未软删」
// 时接受；任何失败静默丢弃（绝不因无效头拒绝聊天——规格 §2.6.1-2）。
func (s *Server) attachTurnContext(r *http.Request, dr *downstreamRequest) {
	if s == nil || dr == nil {
		return
	}
	sess, ok := SIDFromContext(r.Context())
	if !ok {
		return
	}
	dr.sessionSID = sess.SID
	dr.sessionKeyID = sess.KeyID
	id := r.Header.Get("X-A2A-Conversation-Id")
	if id == "" || s.store == nil {
		return
	}
	conv, found, err := s.store.ConversationGet(id)
	if err != nil {
		s.storeError("conversation lookup", err)
		return
	}
	if !found || conv.SID != sess.SID || !conv.DeletedAt.IsZero() {
		return
	}
	dr.convID = id
}

// recordTurn serve 收尾钩子（nil 安全；失败只 log/ring）。
func (s *Server) recordTurn(dr downstreamRequest) {
	if s == nil || s.store == nil || dr.convID == "" || dr.rec == nil || dr.task != nil {
		return
	}
	now := time.Now()
	recs := dr.rec.recs
	if len(recs) == 0 {
		// 本地失败未达上游：仍落一行（保证「请求有终态就有 turn」）。
		s.appendTurn(storeTurn{
			ID:              dr.reqID + ":0",
			ConvID:          dr.convID,
			RequestID:       dr.reqID,
			AttemptNo:       0,
			CredentialIndex: -1,
			Status:          "failed",
			UsageJSON:       "{}",
			CreatedAt:       now,
		})
	} else {
		for _, ar := range recs {
			if ar == nil {
				continue
			}
			usage := turnUsageJSON{In: ar.in, Out: ar.out, TTFTMs: ar.ttft.Milliseconds()}
			if !ar.end.IsZero() {
				usage.TotalMs = ar.end.Sub(ar.start).Milliseconds()
			}
			body, err := json.Marshal(usage)
			if err != nil {
				body = []byte("{}") // 全标量载荷——Marshal 失败 = 编程错误；绝不写半截。
			}
			s.appendTurn(storeTurn{
				ID:              fmt.Sprintf("%s:%d", dr.reqID, ar.attempt),
				ConvID:          dr.convID,
				RequestID:       dr.reqID,
				AttemptNo:       ar.attempt,
				Upstream:        ar.upstream,
				CredentialIndex: ar.credentialIndex,
				Status:          turnStatus(ar),
				UsageJSON:       string(body),
				CreatedAt:       now,
			})
		}
	}
	// per-session / per-key 台账（每请求一次；不写 global——规格 §2.6.4）。
	day := storeDay(now)
	if dr.sessionSID != "" {
		if err := s.store.UsageAdd("session:"+dr.sessionSID, day, 1, dr.costUnits); err != nil {
			s.storeError("usage add session", err)
		}
	}
	if dr.sessionKeyID != "" {
		if err := s.store.UsageAdd("key:"+dr.sessionKeyID, day, 1, dr.costUnits); err != nil {
			s.storeError("usage add key", err)
		}
	}
}

// appendTurn 单行 turns 追加（失败只 storeError）。
func (s *Server) appendTurn(t storeTurn) {
	if err := s.store.TurnAppend(t); err != nil {
		s.storeError("turn append", err)
	}
}

// recordModelUsage 逐模型台账钩子（W78-1；设计 §5.5「按模型/按上游」的计数器面）。
//
//   - 触发点 = `serve()` 收尾的 `defer s.recordModelUsage(dr)`——**每请求一次**，
//     与 `recordTurn` 相互独立（后者只在「有会话 conv」时落 turns/session/key；
//     模型计数不依赖会话：非 UI 流量同样要知道哪台模型在被用）；
//   - 行 = `usage_counters(scope="model:<模型名>", day, requests+1, units+costUnits)`；
//   - 条件：store 就绪 + `dr.model != ""`（gemini 路径态模型名同样计入——dr.model
//     是**上游有效模型名**，serve 的别名改写之后的值）；
//   - **绝不写 global/session/key**（那些面各有写入点：global = budgetLedger.admit，
//     session/key = recordTurn——本钩子只加 model 一行，零双计）；
//   - 纪律：失败只 `storeError`（log + ring，**绝不影响响应**）；零凭据字节
//     （scope 只含模型名——它来自下游请求，不是秘密）。
func (s *Server) recordModelUsage(dr downstreamRequest) {
	if s == nil || s.store == nil || dr.model == "" {
		return
	}
	day := storeDay(time.Now())
	if err := s.store.UsageAdd(storeUsageScopeModelPrefix+dr.model, day, 1, dr.costUnits); err != nil {
		s.storeError("usage add model", err)
	}
}
