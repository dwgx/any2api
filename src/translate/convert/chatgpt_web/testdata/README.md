# chatgpt_web testdata — 匿名档 golden 语料（W30-1 入包）

三份 = **2026-10-15 零账号实抓**的 Android 匿名面（`/backend-anon/f/conversation`）
completion SSE 脱敏帧流（W29-1，报告 `research/briefs/reports/A2A-L11-W29-1.md`
§2）。入包目的：把 W29-1 的 overlay 测试正式化（`golden_test.go`），并给 D1
（`reasoning_recap` 误触状态终态判据）留永久回归锚。

| 文件 | 档 | 出口 | 帧数 | 正文 | 备注 |
|---|---|---|---|---|---|
| `anon_v1_direct.sse.txt` | v1 JSON-Patch（`supported_encodings:["v1"]`） | server 直出 | 21 | `1…10` | 含批量 `o:"patch"` + 简写帧 + `conversation_detail_metadata` |
| `anon_v1_isp.sse.txt` | v1 JSON-Patch | ISP（Webshare US2） | 19 | `PONG` | **D1 证据帧**：第 8 帧 `content_type:"reasoning_recap"`（`status:"finished_successfully"`、`end_turn:false`）其后才是正文 patch |
| `anon_snapshot.sse.txt` | 快照档（`supportedEncodings:["text/plain"]`） | server 直出 | 17 | `PONG` | 全对象帧 + parts 全量 → 前缀差分；无 `delta_encoding`、无 `c` |

## 来源与脱敏

- 源（只读）：`tmp/lanes/W29-chatgpt-s1/{sample-v1,sample-isp-v1,sample}/sse.sanitized.txt`
  （W29-1 捕获器 → `sanitize.py` 脱敏导出；原始件留在 server `/opt/deploy/w29/raw/`，未出机）。
- 脱敏口径（W29-1 §7）：`conversation_id`/`message_id` → `<id-N>`、令牌 →
  `<token:N chars>`、设备 id 不回显、公网 IP 掩码。
- 本包只收 SSE 帧流；`meta.json`/`frames_summary.json` **不入包**（meta.json 带
  `set-cookie`/`cf-ray` 头——扫描器命中，见下）。

## 凭据扫描（入包门）

扫描器：`tmp/lanes/W30-d1/credscan.py`（JWT / `gAAAAA` / `sk-` / `Bearer` /
`password` / `session-token` / cookie / `cf-ray` / 公网 IPv4 / UUID / `user:pass@`）。

```
$ python tmp/lanes/W30-d1/credscan.py src/translate/convert/chatgpt_web/testdata/*.sse.txt
CLEAN  src/translate/convert/chatgpt_web/testdata/anon_snapshot.sse.txt  (34 lines)
CLEAN  src/translate/convert/chatgpt_web/testdata/anon_v1_direct.sse.txt  (54 lines)
CLEAN  src/translate/convert/chatgpt_web/testdata/anon_v1_isp.sse.txt  (48 lines)
TOTAL HITS = 0
```

阳性对照（证明匹配不瞎）：同一扫描器对 `sample-isp-v1/meta.json` 报 5 命中
（2×`set-cookie` + 3×`cf-ray`）——即「不入 meta.json」的依据。

## 字节一致性（sha256）

```
4452ad7bd83ee73e92ffd36de0560b341b29181cb4cc865cd2ebe9847bfbef61  anon_v1_direct.sse.txt  （= sample-v1/sse.sanitized.txt）
5e6da3088c4c85e875b8cf31c2abfdd66355a68e584e1f503fcc66fd8c015b92  anon_v1_isp.sse.txt     （= sample-isp-v1/sse.sanitized.txt）
8471363c874cdc7bf69ca221cc48d079ad8224c5ec3a7e48c04f7345c3b337a8  anon_snapshot.sse.txt   （= sample/sse.sanitized.txt）
```

## 帧面要点（断言依据）

- v1 档：`event: delta_encoding` + `data: "v1"` → 对象首帧（`c:0`）→ 简写帧
  （`{"v":{message},"c":N}`，`c` 逐帧 +1 至 6）→ 正文增量（含无 `p`/`o` 的简写帧）
  → 批量 `{"p":"","o":"patch","v":[…]}`（status/end_turn/metadata）→
  `message_stream_complete` → `[DONE]`。
- ISP 份额外：`message_marker`（`cot_token`/`user_visible_token`/`final_channel_token`/
  `last_token`）四型、**recap 帧**（D1）。
- 快照档：无 `delta_encoding`；助手正文由 `in_progress parts:[""]` →
  `finished_successfully parts:["PONG"] end_turn:true` 两次全量快照给出。
