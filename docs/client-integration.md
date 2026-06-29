# 客户端对接文档

本文档说明游戏客户端如何与 NewGame 服务端对接：**HTTP 登录** → **TCP 长连接 Gate** → **Cmd/Act 帧协议** 玩游戏逻辑。

代码定义见 `pkg/protocol/`（帧格式与 JSON 载荷）、`api/proto/messages.proto`（HTTP 消息体）。

---

## 1. 对接总览

```
┌─────────┐   POST /api/login    ┌───────┐
│ Client  │ ──────────────────► │ Login │  返回 token、role_id、gate_addr
└────┬────┘                      └───────┘
     │
     │  TCP 连接 gate_addr（如 127.0.0.1:9000）
     ▼
┌─────────┐  CmdLogin(1,1)       ┌──────┐   转发    ┌──────┐
│ Client  │ ◄──────────────────► │ Gate │ ────────► │ Game │
└─────────┘  CmdGame(2,*)        └──────┘  HTTP/gRPC └──────┘
     ▲
     │  CmdPush(3,*) 服务端主动推送（邮件、邀请等）
     └──────────────────────────────────────────────
```

| 步骤 | 协议 | 说明 |
|------|------|------|
| 1 | HTTP | 账号登录，获取 `token`、`role_id`、`gate_addr` |
| 2 | TCP | 连接 Gate，发送 `CmdLogin` 绑定会话 |
| 3 | TCP | 发送 `CmdGame` 拉数据、升级技能、接任务等 |
| 4 | TCP | 定期 `CmdPing` 心跳，维持在线与 presence |
| 5 | HTTP（可选） | 匹配、社交等可走独立 HTTP 微服务 |

**注意**：除登录外，所有游戏逻辑帧必须先完成 Gate 登录（`authed=true`），否则返回 `code=1002`。

---

## 2. HTTP 登录接口

### 2.1 登录

- **URL**：`POST {login_host}/api/login`
- **Content-Type**：`application/json`

**请求体**

```json
{
  "username": "player1",
  "password": "secret",
  "zone_id": 1
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| username | string | 是 | 账号名 |
| password | string | 是 | 密码 |
| zone_id | int32 | 否 | 区服 ID；省略则用服务端默认区 |

**成功响应**（`code=0`）

```json
{
  "code": 0,
  "token": "tk_a1b2c3d4e5f6...",
  "role_id": 10001,
  "gate_addr": "127.0.0.1:9000",
  "message": "ok"
}
```

| 字段 | 说明 |
|------|------|
| token | 会话令牌，Gate `CmdLogin` 时提交 |
| role_id | 角色 ID，后续逻辑以该 ID 为准 |
| gate_addr | Gate TCP 地址，格式 `host:port` |

**失败响应**：`code` 非 0，`message` 为错误说明（如 `invalid password`）。

### 2.2 区服列表（可选）

- **URL**：`GET {login_host}/api/zones`

```json
{
  "code": 0,
  "zones": [
    { "id": 1, "name": "一区", "gate_addr": "127.0.0.1:9000", "online": true },
    { "id": 2, "name": "二区", "gate_addr": "127.0.0.1:9010", "online": true }
  ]
}
```

### 2.3 校验 token（可选）

- **URL**：`POST {login_host}/api/enter`
- **请求**：`{"token":"tk_..."}`
- **响应**：`{"code":0,"role_id":10001,"zone_id":1}`

---

## 3. Gate TCP 连接

### 3.1 连接参数

| 项 | 值 |
|----|-----|
| 协议 | TCP |
| 地址 | 登录返回的 `gate_addr` |
| 字节序 | **大端（Big-Endian）** |
| 载荷编码 | UTF-8 JSON |

### 3.2 帧格式

每一帧线上布局（字节序均为大端）：

```
| uint16 len | uint16 cmd | uint16 act | payload... |
|  2 字节    |   2 字节   |   2 字节   | 变长 JSON   |
```

- **len**：`cmd + act + payload` 的总字节数（**不含** len 自身 2 字节）
- 最小 len = 4（仅 cmd+act，payload 为空）
- 最大单帧 payload 受 uint16 限制，建议单帧 JSON < 60KB

**编码示例（伪代码）**

```
body = cmd(2) + act(2) + utf8(json)
len  = len(body)   // 即 4 + len(json)
frame = big_endian_uint16(len) + body
conn.write(frame)
```

**解码**：先读 2 字节 `len`，再读 `len` 字节，前 4 字节为 cmd/act，余下为 payload。

Go 服务端参考：`pkg/protocol/frame.go` 的 `Encode` / `Decode`。

### 3.3 推荐连接流程

1. `TCP connect(gate_addr)`
2. 发送 `CmdLogin(1) + ActLogin(1)`，body `{"token":"<登录返回的token>"}`
3. 收到 `code=0` 后进入游戏
4. 每 30～60 秒发 `CmdPing(100)` 心跳
5. 断线重连：重新 TCP 连接 + 再次 `CmdLogin`（token 未过期即可）

---

## 4. 命令字（Cmd）与动作（Act）

### 4.1 Cmd 一览

| Cmd | 值 | 方向 | 说明 |
|-----|-----|------|------|
| CmdLogin | 1 | C→S→C | Gate 鉴权，校验 token |
| CmdGame | 2 | C→S→C | 游戏逻辑，转发至 Game 分片 |
| CmdPush | 3 | S→C | 服务端主动推送 |
| CmdPing | 100 | C→S→C | 心跳 |

### 4.2 CmdLogin — 登录 Gate

| 字段 | 值 |
|------|-----|
| Cmd | 1 |
| Act | 1 |

**请求 JSON**

```json
{ "token": "tk_a1b2c3d4..." }
```

**成功响应 JSON**

```json
{
  "code": 0,
  "message": "gate login ok",
  "zone_id": 1
}
```

**常见错误**

| code | message | 原因 |
|------|---------|------|
| 1001 | token required | body 缺少 token |
| 1002 | invalid token | token 过期或不存在 |
| 1002 | zone mismatch | 区服与 Gate 不匹配（dedicated 模式） |

### 4.3 CmdPing — 心跳

| 字段 | 值 |
|------|-----|
| Cmd | 100 |
| Act | 1 |

**请求**：`{}` 或空 body

**响应**

```json
{ "pong": true }
```

心跳会续约 Redis 在线表（presence），建议客户端定时发送。

### 4.4 CmdGame — 游戏逻辑

| 字段 | 值 |
|------|-----|
| Cmd | 2 |
| Act | 见下表 |

**未登录**时除 Login/Ping 外一律返回：

```json
{ "code": 1002, "message": "not logged in" }
```

#### Act 一览

| Act | 值 | 读写 | 请求 JSON | 响应 JSON 要点 |
|-----|-----|------|-----------|----------------|
| ActPlayerData | 2 | 读 | `{}` 或空 | role_id, level, gold, bag, skills, quests, guild_id |
| ActSkillList | 3 | 读 | `{}` | `{"skills":[...]}` |
| ActSkillUpgrade | 4 | **写** | `{"skill_id":"slash"}` | `{"skill_id":"slash","level":2}` |
| ActQuestList | 5 | 读 | `{}` | `{"quests":[...]}` |
| ActQuestAccept | 6 | **写** | `{"quest_id":"main_1"}` | `{"code":0,"quest_id":"main_1"}` |

**ActPlayerData 响应示例**

```json
{
  "role_id": 10001,
  "level": 5,
  "gold": 1200,
  "bag": { "potion": 3 },
  "skills": [ { "skill_id": "slash", "level": 2 } ],
  "quests": [ { "quest_id": "main_1", "status": "active" } ],
  "guild_id": 0
}
```

**ActSkillUpgrade 请求**

```json
{ "skill_id": "slash" }
```

`skill_id` 可省略，服务端默认 `slash`。

**ActQuestAccept 失败示例**

```json
{ "code": 1001, "message": "quest already active", "quest_id": "" }
```

---

## 5. CmdPush — 服务端推送

客户端需监听 **Cmd=3** 的下行帧（无需请求，由 Gate 写入 TCP）。

| Act | 值 | 含义 |
|-----|-----|------|
| ActPushSystem | 1 | 系统公告 |
| ActPushChat | 2 | 聊天/私聊 |
| ActPushInvite | 3 | 组队/好友邀请 |
| ActPushMail | 4 | 新邮件提醒 |

**帧示例**

```
cmd=3, act=4, body={"title":"新邮件","mail_id":42}
```

推送由其他服务经 Gate `POST /internal/push` 触发，客户端按 `act` 分发 UI 即可。

---

## 6. 业务错误码

定义见 `pkg/errors`：

| code | 含义 | 典型场景 |
|------|------|----------|
| 0 | 成功 | |
| 1001 | 参数错误 | JSON 缺字段、业务校验失败 |
| 1002 | 未授权 | 未登录、token 无效、区服不匹配 |
| 1003 | 不存在 | 推送目标不在本 Gate |
| 5000 | 内部错误 | Game 不可用、转发超时 |

TCP 帧内错误与 HTTP 一致，均为 JSON：`{"code":1002,"message":"..."}`。

---

## 7. 客户端实现要点

### 7.1 粘包 / 半包

TCP 是流式协议，必须按 **len 前缀** 组帧。一次 `read` 可能包含多帧，也可能只读到半帧。

建议：

- 维护接收缓冲区，循环 `decode` 直到数据不足
- 单元测试参考：`pkg/protocol/frame_test.go`（多帧往返）

### 7.2 断线重连

1. 检测 TCP 断开或读超时
2. 重新 `connect` + `CmdLogin`（token 有效期内无需重新 HTTP 登录）
3. 拉取 `ActPlayerData` 同步状态

### 7.3 限速

Gate 对单连接有 `msg_rate_per_sec` 限制（默认配置见 `configs/gate.yaml`）。超限返回：

```json
{ "code": 1001, "message": "rate limited" }
```

### 7.4 多区服

- 登录时指定 `zone_id`
- 不同区 `gate_addr` 不同（见 `/api/zones`）
- 角色数据按区隔离

### 7.5 分片（大规模部署）

客户端 **无需** 感知 Game 分片：Gate 按 `role_id` 自动路由到 `game-shard-N`。客户端始终只连 Gate。

---

## 8. 可选 HTTP 接口（不经 Gate）

以下能力可走 HTTP，适合大厅 UI、匹配按钮等；**核心养成仍建议走 Gate CmdGame**。

### 8.1 匹配

- `POST {match}/api/match/join`

```json
{ "role_id": 10001, "zone_id": 1, "mode": 0 }
```

响应：`{"code":0,"match_id":"m_10001_10002","room_id":"..."}` 或 `match_id` 为 `wait_*` 表示排队中。

- `GET {match}/api/match/room?match_id=m_...` — 查询房间成员
- `GET {match}/api/match/queue` — 排队人数

### 8.2 社交

- `POST {social}/api/social/friend/add` — `{"role_id":1,"friend_id":2,"zone_id":1}`
- `GET {social}/api/social/friend/list?role_id=1`
- `POST {social}/api/social/chat/global/send` — `{"role_id":1,"text":"hello"}`

### 8.3 排行榜

- `GET {rank}/api/rank/top?board=dungeon&zone_id=1`
- `GET {rank}/api/rank/global/top?board=dungeon`

---

## 9. 完整时序示例

```
Client                          Login              Gate                 Game
  | POST /api/login  ---------->|                    |                    |
  |<------ token, gate_addr ----|                    |                    |
  | TCP connect ----------------------------------->|                    |
  | CmdLogin(1,1){token} ---------------------------->|                    |
  |<--------------------------- {code:0,zone_id} ---|                    |
  | CmdGame(2,2){} --------------------------------->|---- forward ------>|
  |<--------------------------- player data --------|<-------------------|
  | CmdGame(2,4){skill_id:slash} ------------------->|---- forward ------>|
  |<--------------------------- {skill_id,level} ---|<-------------------|
  | CmdPing(100,1){} ------------------------------>|                    |
  |<--------------------------- {pong:true} ---------|                    |
```

---

## 10. 参考实现

| 资源 | 路径 |
|------|------|
| 冒烟机器人 | `tools/robot/` |
| 长连接压测 | `tools/cctest/` |
| 帧编解码 | `pkg/protocol/frame.go` |
| JSON 载荷类型 | `pkg/protocol/payloads.go` |
| 集成测试 | `tests/integration/smoke_test.go` |

**机器人快速验证**

```powershell
docker compose up -d
make run-login & make run-game & make run-gate
go run ./tools/robot
```

---

## 11. 版本与变更

| 日期 | 说明 |
|------|------|
| 2026-06 | 初版：Login + Gate TCP + CmdGame Act 2～6 |
| 2026-06 | Gate 连接 context 取消转发；Login/Match 发现缓存 |

新增 `Cmd`/`Act` 时请同步更新本文档与 `pkg/protocol/frame.go` 注释。
