# NewGame 游戏服务端

多进程微服务架构的游戏服务端脚手架，包含 11 个业务进程与 4 个基础设施服务。

## 大规模在线（10 万～100 万 CCU）

详见 **[docs/architecture-scale.md](docs/architecture-scale.md)**。

| 规模 | Gate 节点 | Game 分片 | 关键改造 |
|------|-----------|-----------|----------|
| 10 万 | ~7–10 | ~50 | 分片路由 + 连接池 + presence + **异步落库** |
| 100 万 | ~67–100 | ~500 | gRPC 流 + Redis/PG 集群 + K8s HPA |

预埋包：`pkg/shard`（分片路由）、`pkg/presence`（在线表）、`pkg/scale`（容量常量）。

```powershell
# 分片配置示例
go run ./services/game/cmd -config configs/game-shard-0.yaml
go run ./services/gate/cmd -config configs/gate-scale.yaml
```

## 架构

```
Client
  │
  ├─ HTTP ──► Login (8080) ── token/session
  │
  └─ TCP  ──► Gate (9000) ──► Game (9100) ── PlayerActor
                    │
        ┌───────────┼───────────┬──────────┐
        ▼           ▼           ▼          ▼
     Match       Battle      Social      Mail
     (9200)      (9300)      (9400)     (9500)
        │                       │          │
        └───────────┬───────────┴──────────┘
                    ▼
              Rank / Activity / Pay / GM
              (9600)  (9700)    (9800) (9900)

基础设施: Redis · PostgreSQL · NATS
```

## 目录结构

```
D:\NewGame
├── api/pb/           # protoc 生成（messages.pb.go）+ 运行 make proto 更新
├── api/proto/        # proto 源文件
├── configs/          # 各服务 YAML 配置
├── deploy/           # 数据库初始化 SQL
├── pkg/              # 共享库（config/log/redis/db/mq/actor/protocol/discovery）
├── services/         # 11 个微服务
│   ├── login/ gate/ game/ match/ battle/
│   └── social/ mail/ rank/ activity/ pay/ gm/
├── tools/robot/      # 冒烟测试客户端
├── docker-compose.yml
└── Makefile
```

## 快速开始

### 1. 启动基础设施

```powershell
cd D:\NewGame
docker compose up -d
```

| 服务 | 端口 |
|------|------|
| Redis | 6379 |
| PostgreSQL | 5432 (user/pass/db: newgame) |
| NATS | 4222 |

### 2. 安装依赖并编译

```powershell
go mod tidy
make build
```

### 3. 启动服务（建议顺序）

每个服务在独立终端中运行：

```powershell
make run-login    # 8080
make run-game     # 9100
make run-gate     # TCP 9000, HTTP 9001
make run-match    # 9200
make run-battle   # 9300
make run-social   # 9400
make run-mail     # 9500
make run-rank     # 9600
make run-activity # 9700
make run-pay      # 9800
make run-gm       # 9900
```

### 4. 冒烟测试

```powershell
go run ./tools/robot
# 或
.\bin\robot.exe
```

## 主要 API

| 服务 | 端点 | 说明 |
|------|------|------|
| Login | `POST /api/login` | 账号登录，返回 token 与 gate 地址 |
| Login | `POST /api/enter` | 选区进入 |
| Gate | TCP `:9000` | Cmd/Act 帧协议，转发至 Game |
| Game | `POST /internal/player/msg` | 玩家消息处理（Actor） |
| Game | `POST /internal/player/dungeon/pass` | 关卡通关 |
| Match | `POST /api/match/join` | 加入匹配池 |
| Battle | `POST /api/battle/room/create` | 创建战斗房间 |
| Mail | `POST /api/mail/send` | 发送邮件 |
| Rank | `GET /api/rank/top?board=dungeon` | 排行榜 |
| GM | `POST /api/gm/dungeon/pass` | GM 通关（转发 Game） |

## 服务发现

基于 Redis 的自研注册中心（`pkg/discovery`），无需 Consul：

- 各服务启动时向 Redis 注册实例（HTTP/TCP 地址 + zone）
- 后台心跳续约（默认 5s），TTL 过期自动摘除（默认 15s）
- `PickHealthy` 在选取实例前会请求 `GET /health`
- 配置项见各 `configs/*.yaml` 的 `discovery` 段

## 脚本

| 脚本 | 说明 |
|------|------|
| `scripts/start-all.ps1` | 每个服务开一个窗口启动 |
| `scripts/stop-all.ps1` | 按端口停止所有服务 |
| `scripts/gen-proto.ps1` | 从 `api/proto` 生成 Go 代码 |

生成协议（需安装 protoc）：

```powershell
make proto
# 或
.\scripts\gen-proto.ps1
```

单元测试与集成测试：

```powershell
make test
# 服务全部启动后
make test-integration
# 或
go test -tags=integration ./tests/integration/...
```

## 协议与客户端对接

- **客户端对接（推荐阅读）**：[docs/client-integration.md](docs/client-integration.md) — 登录流程、TCP 帧格式、Cmd/Act 一览、错误码、示例时序
- 完整定义见 `pkg/protocol/`（TCP 帧、JSON 载荷、HTTP 路由、NATS 主题）与 `api/proto/messages.proto`

### Gate TCP（客户端 ↔ Gate）

大端序帧：`| uint16 len | uint16 cmd | uint16 act | payload(JSON) |`

| Cmd | Act | 说明 | 请求 JSON | 响应 JSON |
|-----|-----|------|-----------|-----------|
| 100 | 1 | 心跳 | `{}` | `{"pong":true}` |
| 1 | 1 | Gate 登录 | `{"token":"tk_..."}` | `{"code":0,"message":"...","zone_id":1}` |
| 2 | 2 | 玩家数据 | `{}` | role_id/level/gold/bag/skills/quests |
| 2 | 3 | 技能列表 | `{}` | `{"skills":[...]}` |
| 2 | 4 | 升级技能 | `{"skill_id":"slash"}` | `{"skill_id","level"}` |
| 2 | 5 | 任务列表 | `{}` | `{"quests":[...]}` |
| 2 | 6 | 接任务 | `{"quest_id":"main_1"}` | `{"code":0,"quest_id"}` |

流程：`POST /api/login` 取 token → TCP 连 Gate → CmdLogin 鉴权 → CmdGame 玩游戏逻辑。

### HTTP 微服务

- Login `:8080` — `/api/login`、`/api/zones`
- Match `:9200` — `/api/match/join|room|queue`
- Battle `:9300` — `/api/battle/room/create|settle`
- 其余路由常量见 `pkg/protocol/http_api.go`

### NATS 异步

| 主题 | 载荷 |
|------|------|
| `mail.send` | MailSendRequest |
| `rank.update` | RankUpdateRequest |
| `activity.event` | `{role_id, activity_id, delta}` |

## 开发说明

- 每个服务独立 `cmd/main.go`，通过 `-config configs/xxx.yaml` 加载配置
- Game 服务采用一玩家一 Actor 模型（`pkg/actor`）
- 跨服务异步消息通过 NATS（mail.send / rank.update / activity.event）
- 服务注册与发现通过 Redis 自研实现（`pkg/discovery`），TTL + 心跳续约
- 所有服务暴露 `GET /health` 健康检查

## 可观测性

- 各 HTTP 服务自动采集请求数、5xx 错误、平均延迟
- `GET /metrics` 返回 JSON 指标
- `GET /metrics/prometheus` 供 Prometheus 抓取（`ng_http_*`、`ng_gate_*`、`ng_redis_errors_total` 等）
- 配置 `observability.tracing.enabled: true` 启用 OpenTelemetry（默认 stdout；配置 `otlp_endpoint` 导出到 OTLP Collector）

```yaml
observability:
  tracing:
    enabled: true
    otlp_endpoint: "http://127.0.0.1:4318"
  prometheus: true
```

```powershell
go run ./tools/loadtest -workers 20 -n 200
```

## 玩法 API（Game 内部）

| 模块 | 接口 |
|------|------|
| 公会战 | `GET /internal/guildwar/state`，`POST /internal/guildwar/attack` |
| 拍卖行 | `GET /internal/auction/list`，`POST /internal/auction/create|buy` |
| 赛季重置 | `POST /internal/guildwar/reset`，`POST /internal/worldboss/reset` |

## 跨服匹配

`configs/match.yaml` 设置 `cross_zone_match: true` 后，匹配队列写入 Redis（`ng:match:pool:{mode}`），全区服玩家可匹配到一起。

- `POST /api/match/join` — `{role_id, zone_id, mode}`
- `GET /api/match/queue` — 查看当前等待人数

## GM 后台（:9900）

| 接口 | 说明 |
|------|------|
| `GET /api/gm/services` | 列出已注册服务实例 |
| `POST /api/gm/grant?zone_id=` | 发放道具/金币 |
| `POST /api/gm/mail/send` | 发送邮件 |
| `POST /api/gm/dungeon/pass?zone_id=` | 通关副本 |
| `GET /api/gm/pay/reconcile` | 支付对账 |
| `POST /api/gm/season/guildwar/reset` | 公会战赛季重置 |
| `POST /api/gm/season/worldboss/reset` | 世界 Boss 重置 |

## 多区服

| 区服 | Gate TCP | Game HTTP | 配置 |
|------|----------|-----------|------|
| 1 | 9000 | 9100 | `gate.yaml` / `game.yaml` |
| 2 | 9010 | 9110 | `gate-zone2.yaml` / `game-zone2.yaml` |

- `GET /api/zones` 列出区服及 Gate 地址（服务发现）
- Login 按请求的 `zone_id` 路由到对应 Gate
- Gate `zone_mode: dedicated` 校验区服；`hub` 模式接受任意区服并按 session 路由 Game

```powershell
make run-game-z2
make run-gate-z2
```

## 后续扩展

1. ~~公会持久化与世界 Boss 跨服共享~~（`guilds`/`guild_members` 表，Redis 跨区伤害榜）
2. ~~Pay 对账报表、Mail 已读未领分离~~（`GET /api/pay/reconcile`，`POST /api/mail/read|read-all`）
3. ~~全链路压测与可观测性~~（`tools/loadtest`，`GET /metrics`）
4. ~~分布式 tracing（OpenTelemetry）、更完整的 Prometheus 导出~~
5. ~~公会战、拍卖行等玩法扩展~~
6. ~~跨服匹配、赛季重置、GM 后台完善~~
7. 运营活动配置化、热更新、防作弊审计
