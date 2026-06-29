# 大规模在线架构设计（10 万～100 万 CCU）

本文档描述在现有 NewGame 脚手架上演进的目标架构：**单区常态 10 万同时在线，峰值 100 万**。

---

## 1. 设计目标

| 指标 | 基线 | 峰值 |
|------|------|------|
| 同时在线 (CCU) | 100,000 | 1,000,000 |
| 玩家操作延迟 (P99) | < 100ms | < 200ms |
| Gate 可用性 | 99.9% | 99.95% |
| 数据持久化 | 异步落库，丢单可恢复 | 同左 |

---

## 2. 与当前脚手架的差异

| 维度 | 当前 (demo) | 目标 (10万～100万) |
|------|-------------|-------------------|
| Gate→Game | 每帧 `http.Post` | **gRPC 双向流 / 连接池 HTTP** |
| Game | 单进程单区 | **分片集群** `game-shard-{0..N}` |
| 路由 | 服务发现 PickHealthy | **role_id → shard_id 确定性哈希** |
| 在线表 | 仅 session token | **presence**（Gate/分片/心跳） |
| 落库 | 请求同步 Save | **Actor 内存 + 异步写队列** |
| 发现 | Redis TTL | Redis + **固定分片目录** |
| 跨服 | 部分 Redis | **Redis Cluster + NATS JetStream** |

代码预埋：`pkg/shard`、`pkg/presence`、`pkg/scale`。

---

## 3. 总体架构

```
                         ┌─────────────┐
                         │   Login     │  无状态，水平扩展
                         └──────┬──────┘
                                │ token
    ┌─────────── L4/L7 负载（TCP 粘滞） ───────────┐
    ▼           ▼           ▼           ▼          ▼
┌────────┐ ┌────────┐ ┌────────┐     ┌────────┐
│ Gate-1 │ │ Gate-2 │ │ Gate-3 │ ... │ Gate-N │  每节点 1～1.5 万连接
└───┬────┘ └───┬────┘ └───┬────┘     └───┬────┘
    │          │          │              │
    │    gRPC stream / HTTP pool（长连接池）
    ▼          ▼          ▼              ▼
┌──────────────────────────────────────────────────────┐
│              Game Shard Cluster                       │
│  game-shard-0  game-shard-1  ...  game-shard-(M-1)   │  每片 ~2000 CCU
└──────────────────────────────────────────────────────┘
    │                    │                    │
    ▼                    ▼                    ▼
┌─────────┐      ┌──────────────┐      ┌─────────────┐
│ Redis   │      │ NATS         │      │ PostgreSQL  │
│ Cluster │      │ JetStream    │      │ 分库分表     │
│ session │      │ mail/rank/   │      │ 异步持久化   │
│ presence│      │ activity     │      │             │
└─────────┘      └──────────────┘      └─────────────┘

全局无状态服务（水平扩展）：Mail / Pay / Rank / Match / Battle / Social / GM
```

---

## 4. 容量估算

常量见 `pkg/scale/scale.go`。

### 4.1 10 万 CCU（基线）

| 组件 | 数量 | 说明 |
|------|------|------|
| Gate | **7～10** 节点 | 15000 连接/节点 |
| Game 分片 | **50** | 2000 CCU/片 |
| Login | 2～4 | HTTP 无状态 |
| Redis | 3 主 3 从 Cluster | session + presence + 排行榜 |
| Postgres | 1 主 2 从 + 分表 | 按 role_id 分 16/32 表 |
| NATS | 3 节点集群 | 异步事件 |

### 4.2 100 万 CCU（峰值）

| 组件 | 数量 | 说明 |
|------|------|------|
| Gate | **67～100** 节点 | 可跨可用区 |
| Game 分片 | **500** | 2000 CCU/片；K8s HPA 按分片 CPU/在线数扩容 |
| Redis Cluster | 分片扩展 / 多集群 | 热 key 拆分：presence 与 rank 分集群 |
| Postgres | 分库（按 zone 或 role 范围） | 写队列 + 批量 flush |
| 跨服服务 | 独立池 | Match/Battle 与 Game 解耦 |

---

## 5. 核心机制

### 5.1 玩家路由（分片）

```go
shardID := shard.ForRole(roleID, shardCount)  // 见 pkg/shard
service := shard.ServiceName(shardID)         // "game-shard-3"
```

- **同一 role_id 永远进同一分片**（扩缩容时用一致性哈希 + 迁移工具，不在运行时改模数）。
- Gate **不**随机 PickHealthy Game，而是 **按 role_id 算 shard**。

**扩缩容（重分片）**：取模路由在分片数变化时几乎全量迁移（50→64 约 **97%** key 移动），
故扩容用一致性哈希环 `pkg/shard.Ring`（同样场景仅 **~21%** 移动）。迁移用 `tools/reshard`：

```bash
# 规划：对比迁移成本
go run ./tools/reshard -old 50 -new 64 -strategy ring -range-end 200000
# 执行：在新旧 PG 分库间迁移变动角色（低峰期分批）
go run ./tools/reshard -strategy modulus -pg-old "d0,d1" -pg-new "d0,d1,d2" -execute
```

### 5.2 在线 presence

Gate 登录成功后写入 Redis（`pkg/presence`）：

```json
{
  "role_id": 10001,
  "zone_id": 1,
  "shard_id": 3,
  "gate_id": "gate-z1-10.0.0.5:9000-12345",
  "login_at": 1719000000
}
```

用途：踢人、好友是否在线、GM 查线、跨 Gate 发消息。

### 5.3 Gate → Game 传输（必须改造）

**现状问题**：每帧 `http.Post`，无法支撑 10 万+ 活跃操作。

**已实现两种传输（`gate.game_transport` 切换）**：

| 方案 | 说明 | 实现 |
|------|------|------|
| **A. gRPC** | HTTP/2 单连接多路复用，按分片 host:port 维持长连接池 | `pkg/gateforward.GRPCPool` |
| **B. HTTP + 连接池** | 共享 `http.Transport{MaxIdleConnsPerHost:256}` | `pkg/gateforward.HTTPPool` |

统一抽象 `gateforward.Forwarder`。gRPC 服务定义在 `pkg/gamerpc`（手写 ServiceDesc + `pkg/rpccodec` JSON 编解码，免 protoc）。

切换方式：

```yaml
# configs/gate-scale.yaml
gate:
  game_transport: grpc   # 或 http_pool
# configs/game-shard-0.yaml
grpc_addr: ":9150"       # Game 暴露 gRPC，注册到服务发现 Instance.GRPCAddr
```

Game 同时监听 HTTP（`/internal/*`）与 gRPC（`Forward`），两条路径复用同一 Actor 邮箱串行 + 异步落库。

### 5.4 Game 分片内：Actor + 异步落库

```
TCP 请求 → Gate → Game Shard
                    └→ Player Actor（内存）
                         ├→ 同步：改内存状态、回包
                         └→ 异步：持久化队列 → Postgres / Redis
```

- **禁止**每个协议包同步 `UPDATE roles`（当前 demo 做法）。
- 落库间隔：5～30 秒或下线时 flush；关键操作（支付、交易）立即写。

### 5.5 有状态 vs 无状态

| 有状态（慎扩） | 无状态（随意扩） |
|----------------|------------------|
| Gate（连接绑 goroutine） | Login |
| Game 分片（Actor 内存） | Mail / Pay / Rank |
| | Match / Battle（房间短生命周期） |

---

## 6. 客户端连接模型（全员连 Gate）

**结论：可以支撑 10 万～100 万，但必须 Gate 集群 + L4 粘滞。**

1. 客户端 TCP 连 **VIP（负载均衡）**，不是单 Gate IP。
2. 负载均衡 **按连接** 分配到 Gate-i（会话粘滞在连接生命周期内）。
3. 单 Gate：**1～1.5 万长连接**（Go goroutine 模型 + `SO_REUSEPORT` 多进程可选）。
4. 心跳：`CmdPing` 30s；presence `Renew` 续期。

---

## 7. 跨服与全局玩法

| 玩法 | 10 万 | 100 万 |
|------|-------|--------|
| 排行榜 | Redis ZSET 分 key | 分榜 + 定时合并 |
| 世界 Boss | Redis 全局 HP | 同左 + 分线 Boss |
| 匹配 | Match 服务 + Redis 队列 | 独立 Match 集群 |
| 聊天 | Social + Redis/DB | 频道分片 + 消息队列 |
| 公会 | Postgres + 缓存 | 公会 ID 哈希到 DB 分片 |

---

## 8. 部署参考（Kubernetes）

```yaml
# Gate: Deployment replicas=10~100, hostNetwork 或 LoadBalancer
# Game: StatefulSet game-shard, replicas=50~500
#   env: SHARD_ID from pod ordinal
#   env: SHARD_COUNT=50
# HPA: 按 custom metric ng_online_per_shard 扩容
```

配置扩展（规划，逐步接入 `configs/`）：

```yaml
scale:
  shard_id: 3          # 本分片编号
  shard_count: 50      # 全区 Game 分片总数
  max_ccu: 2000        # 本片告警阈值
  save_mode: async     # async | sync 异步落库
  save_interval_sec: 10
gate:
  game_transport: grpc # grpc | http_pool
  game_pool_size: 256
grpc_addr: ":9150"     # Game 暴露 gRPC（注册到发现 Instance.GRPCAddr）
infra:
  # Redis Cluster：redis_cluster 非空时优先于 redis
  redis_cluster:
    - "10.0.0.1:7000"
    - "10.0.0.2:7000"
    - "10.0.0.3:7000"
  # Postgres 分库：postgres_shards 非空时按 role_id 分库
  postgres_shards:
    - "postgres://u:p@db0/newgame?sslmode=disable"
    - "postgres://u:p@db1/newgame?sslmode=disable"
```

---

## 9. 演进路线（从当前代码）

| 阶段 | 内容 | CCU 能力 |
|------|------|----------|
| **P0 现在** | HTTP 转发、单 Game | ~1k 压测 |
| **P1 ✅** | HTTP 连接池 + 分片路由 + presence | ~1～2 万 |
| **P2 ✅** | Actor 邮箱串行 + 异步落库 + Gate 心跳续约 presence | **~10 万** |
| **P2.5 ✅** | 全服务分片路由（Pay/Mail/GM）+ 跨分片在线推送（Game/服务→Gate→Client） | ~10 万 |
| **P3 ✅** | Gate↔Game gRPC 传输 + Redis Cluster + Postgres 分库（按 role_id） | ~50 万 |
| **P4 进行中** | ✅ K8s 清单 + env 配置 + TCP 压测 + 一致性哈希 + 重分片迁移工具；待办：跨 AZ、百万级实测 | ~100 万 |

---

## 10. 监控指标（必配）

| 指标 | 告警 |
|------|------|
| `gate_connections` | > 12000/节点 |
| `ng_online_per_shard` | > 1800/片 |
| `gate_game_forward_latency_p99` | > 100ms |
| `game_save_queue_depth` | > 10000 |
| `redis_presence_ops` | 延迟 > 5ms |

---

## 11. 相关代码

| 包 | 职责 |
|----|------|
| `pkg/shard` | role_id → shard_id |
| `pkg/presence` | 在线路由表 |
| `pkg/scale` | 容量常量、GameForwarder 接口 |
| `pkg/discovery` | 实例注册（扩展为分片名 `game-shard-N`） |

| `services/game/internal/player/persist.go` | 异步落库队列 AsyncSaver |
| `services/game/internal/player/manager.go` | HandleMsg 邮箱 + ScheduleSave |
| `pkg/actor/mailbox.go` | Call 同步串行投递 |
| `pkg/client/game.go` | GameClient 按 role_id 分片路由（Pay/Mail/GM） |
| `pkg/client/notify.go` | NotifyClient：presence → Gate /internal/push |
| `services/gate` `/internal/push` | 跨分片在线推送落地到 TCP |
| `pkg/gamerpc` | Gate↔Game gRPC 服务（手写 ServiceDesc） |
| `pkg/rpccodec` | gRPC JSON 编解码器（免 protoc） |
| `pkg/gateforward/grpcpool.go` | Gate gRPC 转发池 |
| `services/game/internal/grpcserver.go` | Game gRPC 服务端 |
| `pkg/redis` | UniversalClient：单机 / Cluster 透明切换 |
| `pkg/db/sharded.go` | Postgres 按 role_id 分库连接池 |
| `pkg/repo/role.go` | RoleRepo 单库 / 分库路由 |
| `pkg/config` env 覆盖 | 容器/K8s 注入分片号与地址（POD_NAME→shard_id） |
| `deploy/k8s/` | StatefulSet/Deployment/HPA/LoadBalancer 清单 |
| `tools/cctest` | TCP 长连接压测（连接数/吞吐/P50-P99） |
| `pkg/shard/consistent.go` | 一致性哈希环（扩缩容仅迁移 ~1/N） |
| `tools/reshard` | 重分片迁移规划/执行（ring vs modulus，PG 数据迁移） |
| `pkg/discovery/resolver.go` | 实例解析 TTL 缓存（Gate 每帧转发、Pay/Mail/GM 复用，消除热路径健康探测） |
| `pkg/internalauth` | 内部接口共享密钥鉴权（HTTP 头 + gRPC metadata） |
| `player.Manager` 淘汰 | 登出（Gate 断线→Game logout）+ 空闲淘汰，回收 Actor 与邮箱 goroutine |
| `pkg/actor.Mailbox` | 安全关闭（RWMutex + closed），杜绝向已关闭 channel 投递 |
| `worldboss` 原子结算 | Lua 限伤不超杀，HP 不读负、伤害榜只计有效伤害 |
| `config.Validate` | 启动校验 shard_id/save_mode/transport，fail-fast |
| Gate 帧缓冲复用 + 优雅停机 | 复用 header/body 缓冲减 GC；SIGTERM 关监听并 drain |
| Gate→Game 免双重编码 | cmd/act/role 走 HTTP 头，payload 直传 body（不再 JSON 套 JSON） |
| Gate 多 acceptor + 超时配置化 | 并发 Accept 提升建连吞吐；读/写/转发超时可配 |
| 协议帧编码修正 | Encode 不再多写 2 字节，修复真实多帧连接错乱/断连 |
| Battle 房间 TTL 清理 | 后台清过期房间，修内存泄漏 |
| Game 关停刷盘 | 退出前 FlushAll 待落库玩家，避免丢数据 |
| Prometheus 指标接线 | gate_connections / online_per_shard / save_queue_depth / forward_latency 等 |
| 单连接限速 | `msg_rate_per_sec` 防单连接刷包 |
| Game gRPC 优雅停机 | 关停时 GracefulStop |
