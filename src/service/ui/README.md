# Any2API Chat —— 前端 SPA（`src/service/ui/`）

零构建、纯静态的单页对话前端（**UI v2**），由服务 `//go:embed ui` 托管（`/ui` 与
`/ui/` 入口，未知路径回退本页）。UI 是**我方 OpenAI 兼容 API 的消费者**：同源调用
`/v1/chat/completions`（SSE 流式）与 `/ui/api/config`（模型清单）。

视觉取向：按 W62 七域蒸馏规格整装，逐组件还原 Claude Desktop 2.42304 设计语言
（36px 标题栏 + 288px 侧栏卡 + 窄主列 + 底部输入区、暖色中性调、`--cds-*`/`--df-*`
令牌贯穿），但为**完全原创实现**——不复制 Anthropic 的代码/字体/图片资产；图标全部
为内联 SVG（自有几何图形）；字体为系统字体栈；配色取自 W62-1 tokens.css。

## 文件清单

| 文件 | 说明 |
|---|---|
| `index.html` | 应用外壳 + 全部页面/浮层骨架（侧栏、消息区、输入区、设置、订阅、快捷键、命令面板、toast/banner）+ 根路径归一化脚本 |
| `style.css` | 全量样式（W62-1 tokens 全谱、组件、浅/深主题、移动端、动效降级） |
| `app.js` | 全量逻辑（无框架）：会话管理、SSE 消费、流式渲染、思考块、工具卡、数据面、附件、浮层栈、主题、键盘 |
| `vendor/marked.umd.js` | Markdown 解析（本地 vendor，保留 license 头） |
| `vendor/purify.min.js` | HTML 消毒（本地 vendor，保留 license 头） |
| `README.md` | 本文件 |

> 根路径归一化：服务把 `/ui`（无尾斜杠）与 `/ui/` 都映射到本页。`index.html` 头部
> 的引导脚本按 `location.pathname` 补尾斜杠并注入 `<base>`，使 `style.css` /
> `app.js` / `vendor/*` / `api/config` 等相对引用在两种入口下都正确解析。

## 接口契约（实读 `src/service/relay.go` / `server.go` 对齐）

### `POST /v1/chat/completions`（同源相对路径；SSE）

请求体：

```json
{ "model": "<可选>", "messages": [{ "role": "user|assistant", "content": "…" }], "stream": true }
```

响应为 SSE（`text/event-stream`），逐帧：

```text
data: {"id":"…","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"…"},"finish_reason":null}]}
data: {"choices":[{"index":0,"delta":{"reasoning_content":"…"},"finish_reason":null}]}
data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}
data: [DONE]
```

- 思考增量 = `delta.reasoning_content`（另有 `delta.reasoning` 别名容忍）；
  UI 实时聚合为可折叠的「Thinking」区（流式时展开、完成后折叠，含耗时）。
- 保活帧 `: keepalive`（SSE 注释行）→ 忽略；任何 `event:`/`id:` 行不依赖。
- 流内错误帧 `data: {"error":{…}}` 即**终态**（其后**无** `[DONE]`）→ UI 显示错误卡 + 重试。
- 非 200：读 JSON `error.message`（或纯文本截断）显示在错误卡；429 附带 `Retry-After` 提示。
- 兜底：200 + `application/json`（非 SSE）时按完整响应对象吸收。

### `GET /ui/api/config`

```json
{ "default_model": "…", "models": ["…" 或 {"id":"…"}], "rate_limit_per_min": 30 }
```

- `models` 兼容字符串数组与对象数组（取 `id`）。
- **优雅降级**：config 缺失（非 200/网络失败）→ 回退 `GET /v1/models`（取 `data[].id`）；
  再失败 → 隐藏模型选择器，请求体**省略 `model` 字段**。

### 其他

- `GET /v1/models` —— 仅作为 config 缺失时的回退。
- 会话数据默认存 `localStorage`（键：`any2api.ui.sessions.v2`、`any2api.ui.active.v2`、
  `any2api.ui.model.v2`）；服务端会话同步开启时（见下）localStorage 仅作本地缓存与
  离线降级层。

### 服务端会话同步（P1-4；`/ui/api/sessions` E1–E8）

- **服务端模式**：`GET /ui/api/sessions` 返回 200 时启用。启动 / `focus` /
  `visibilitychange` / 跨标签 `storage` 变更 → `E1 since=last_since` 拉取；服务端
  `rev` 大者胜；本地未同步写先 push（幂等 outbox）再 pull。
- **落盘点**：新建 → `E2`；重命名 / 换模型 → `E5`；删除 → `E6`；用户消息与
  助手落定（结束/停止/错误）→ `E8`；编辑重发 / 重新生成 / 首连迁移全量导入 → `E4`。
- **字段映射**：`created_at↔createdAt`、`updated_at↔updatedAt`、`ts↔created_at`、
  `done↔status`、`reasoning↔thinking` 块、`tools↔tool_use` 块、附件 → `attachment`
  块（含 `file_id`/缩略图）；消息带稳定 uuid `id`（旧 localStorage 迁移时补生成）。
- **`X-A2A-Conversation-Id`**：仅服务端模式 + 有活动会话时，随
  `POST /v1/chat/completions` 发送（`turn_hook.go` 据此落 turn 台账）。
- **冲突（409）**：用响应里的 `session` + `messages` 覆盖本地、toast 提示，并用新
  `base_rev` 重试一次；仍冲突则保留队列稍后重试。
- **首连迁移**：E1 为空且本地有旧会话 → 逐条 `E2`（客户端 id 幂等）+ `E4` 写
  messages，成功后写 `any2api.ui.synced.v1=1`；失败可重入（下次聚焦重来）。
- **降级**：404（public 关闭）/ 503 `store_unavailable` / 网络失败 / 401（gated）
  → 退回纯 localStorage；写队列 + `online`/`focus` 重试，聊天绝不阻塞。401 显示
  「需要访问密钥」提示；503 / 离线显示提示条。
- **多标签**：监听 `storage` + `focus` 重新 E1；不做实时 push。
- **不做（v1）**：多设备实时互推、协同编辑、消息级增量、`pinned/order` 服务端列
  （置顶/手动序仍仅本地）。

## vendor 许可与版本

| 文件 | 版本 | 许可 | 来源 | sha256 |
|---|---|---|---|---|
| `vendor/marked.umd.js` | marked v18.1.1 | MIT（Copyright © 2018-2026 MarkedJS；© 2011-2018 Christopher Jeffrey） | `https://cdn.jsdelivr.net/npm/marked@18.1.1/lib/marked.umd.js` | `b82b9334406d8bdd16b6236c08de1cb65b2a859873ca9503d9cc0b292ca3c12a` |
| `vendor/purify.min.js` | DOMPurify 3.4.17 | Apache-2.0 或 MPL-2.0（双许可，Copyright © Cure53 及贡献者） | `https://cdn.jsdelivr.net/npm/dompurify@3.4.17/dist/purify.min.js` | `cd37898f9782adf54820c413191b5691803f0f137c799d6edcfd38523b4c2c31` |

两个文件的原始 license 头均**保留在文件头部**（未做任何裁剪）。升级时同步更新本表。

## 功能清单（UI v2）

- **会话列表**：localStorage 持久化（v2 键，自动迁移 v1）；新建/行内重命名/两步删除/
  置顶/组内上移下移；按「置顶 → 手动序 → 更新时间」排序；空态引导；标题自动取自首条用户消息。
- **消息流**：用户气泡 + 助手全宽；SSE 流式渲染（40ms 节流 + 增量重排）；自动滚动
  （用户上滚后不被拉回，出现「回到最新」按钮）。
- **思考块**：`delta.reasoning_content`（兼容 `reasoning`）聚合；流式展开、出正文后自动折叠；
  显示耗时；手动开合记忆；状态词按 15/30/45/60s 阶梯 + shimmer；状态行含计时与 token 估算。
- **Markdown**：marked + DOMPurify（本地 vendor）；表格滚动包裹；链接 `target=_blank rel=noopener`；
  代码块语言标签 + 复制 + 轻量自研高亮（Shiki 12 角色色板 → `tok-*`）+ 超大块折叠。
- **工具卡**：`tool_calls` 流式聚合（name/arguments 分片），运行中/完成/失败态，参数 JSON 展开。
- **数据面**：usage 明细（输入/输出/缓存）、模型徽标与选择器、消息相对时间、限流配置提示、
  限额 banner 组件；「需接线」项（结构化来源/上下文用量/任务计数）留结构并在报告标注。
- **输入区**：自动增高（≤200px）、发送/停止、模型选择器 + 思考档位（`/ui/api/config` →
  `/v1/models` → 省略 model；档位 low/medium/high/xhigh/max，默认不发送）、附件 chips
  （选择/拖拽/粘贴/进度/失败重试/移除；图片 ≤20 MB/张、单条合计 ≤20 MB、最多 10 个，
  data URL 随 `image_url` 发出）。
- **多模态回显**：用户消息内图片瓦片（缩略图）→ 全屏查看器（原图来自 IndexedDB，Esc 关闭、
  焦点回原位）；附件元数据入 localStorage，二进制只进 IndexedDB。
- **编辑重发**：任意用户消息 hover →「编辑」→ 气泡替换为编辑框；保存后截断其后消息并从该点
  重新生成（有后续消息时先弹确认框）；自动标题随首条消息更新。
- **搜索**：侧栏搜索行 / Ctrl+Shift+K → 命令面板搜索模式；会话标题 + 消息正文 + 附件文件名
  全文检索（AND、大小写不敏感），结果分组 + 命中高亮，点击跳转并闪烁定位。
- **浮层**：设置（账户/外观/快捷键/关于）、用户菜单、会话菜单、模型菜单、toast、banner、
  错误卡 + 重试、确认框；统一浮层栈（Esc / 遮罩关闭、焦点圈定）。
- **订阅套餐**：用户菜单/设置入口；Free / PRO / **ULTRA PRO MAX**（8888 USDT，金色渐变 +
  光扫 + 皇冠 + 星芒 + 浮夸文案）；纯前端演示，结构预留真实套餐接线。
- **主题**：系统跟随 + 手动浅色/深色（localStorage 持久化）；侧栏密度、减少动效。
- **移动端**：≤820px 侧栏变抽屉（汉堡按钮 + 遮罩 + Esc）、安全区（`env(safe-area-inset-*)`）、
  `100dvh` 适配软键盘。
- **键盘**：Enter 发送 / Shift+Enter 换行（IME 组合不误发）、Esc 关闭浮层或停止生成、
  Ctrl+K 命令面板、Ctrl+Shift+K 搜索、Ctrl+B 侧栏、Ctrl+N 新建、Ctrl+, 设置、Ctrl+/ 快捷键面板。

## 自测方法

零依赖版（手工）：

1. 起服务（或任意静态服务器 + 满足上方契约的 mock）：
   ```bash
   python3 -m http.server 8080 --directory src/service/ui
   # 注意：此方式下 /v1/* 与 /ui/api/config 不存在 → UI 进入降级模式（无模型选择器）
   ```
   建议用带 mock 的完整自测（见下）。
2. 浏览器打开 `…/ui/`，核对：发消息 → 逐字流式渲染；有 `reasoning_content` 时出现
   Thinking 块（流式展开、完成折叠）；代码块复制按钮；流式中「停止」；最后一条
   「重新生成」；断网/5xx 出现错误卡与重试；窗口缩到 <820px 出现抽屉侧栏；
   系统切深色主题全站变色。

完整自动化自测（headless Chrome）：`tmp/lanes/L19-W63-1/` 下 `mock_server.py`
（镜像托管语义 + SSE 场景）+ `run_tests.js`（playwright-core + 系统 Chrome，63 项检查）
+ `shots.js`（12 张 @2x 截图矩阵）——证据与结果见 `tmp/lanes/L19-W63-1/test-results.json`、
`shots/` 与报告 `research/briefs/reports/A2A-L19-W63-1.md`。

## 边界与已知取舍

- 同一时刻至多一个生成流（切换会话不中断后台流；停止按钮始终作用于当前流）。
- 流中直接关闭标签页会丢失未完成的助手消息（下一次落盘点在流结束/停止/错误）。
- **附件已接线（Phase 1）**：图片经 `FileReader` → data URL 随 `image_url` 发出；原图存
  IndexedDB（`any2api.ui.files.v1`），会话只存元数据 + ≤96px 缩略图。服务端文件库
  （`POST /ui/api/files`）与 PDF/Word 仍未做；非图片文件显式拒绝。
- **订阅为纯前端演示**：方案选择只写 localStorage，不产生扣费；真实套餐/计费未接入。
- 数据面「需接线」项：结构化来源卡、上下文用量环、任务/重试/排队计数仅留结构，未接服务事件。
- 服务端会话同步（P1-4）为**最终一致**：多标签经 `storage`/`focus` 拉取，无实时互推；离线写队列在恢复后补写；`pinned/order` 不跨设备同步。
