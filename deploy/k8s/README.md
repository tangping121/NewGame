# Kubernetes 部署（P4）

面向 10 万～100 万 CCU 的参考部署清单。详见 [../../docs/architecture-scale.md](../../docs/architecture-scale.md)。

## 组件

| 文件 | 作用 |
|------|------|
| `namespace.yaml` | 命名空间 newgame |
| `login-deployment.yaml` | Login（无状态）+ Service |
| `gate-deployment.yaml` | Gate（接入层）+ LoadBalancer（L4 粘滞） |
| `game-statefulset.yaml` | Game 分片集群（game-shard-0..N-1） |
| `game-hpa.yaml` | Game/Gate 自动扩缩（自定义指标） |

## 关键设计

- **Game 用 StatefulSet**：pod 名稳定有序，`POD_NAME` 末尾序号 → `shard_id`（见 `pkg/config` 环境覆盖）。
- **同一 role_id 永远进同一分片**：扩容改 `NG_SHARD_COUNT` 需配合数据迁移，不可运行时随意改模数。
- **Gate 用 Deployment + LoadBalancer**：`externalTrafficPolicy: Local` 保留客户端 IP，连接生命周期内粘滞。
- **配置优先级**：环境变量 > YAML（容器内只需基础 YAML，差异项用 env 注入）。

## 环境变量（容器注入）

| 变量 | 说明 |
|------|------|
| `POD_NAME` | StatefulSet pod 名，取末尾序号作 shard_id |
| `NG_SHARD_ID` / `NG_SHARD_COUNT` | 显式分片号 / 总分片数 |
| `NG_HTTP_ADDR` / `NG_TCP_ADDR` / `NG_GRPC_ADDR` | 监听地址 |
| `NG_ZONE_ID` | 区服 |
| `NG_REDIS` / `NG_REDIS_CLUSTER` | 单机 / 集群（逗号分隔） |
| `NG_POSTGRES` / `NG_POSTGRES_SHARDS` | 单库 / 分库（逗号分隔） |
| `NG_NATS` | NATS 地址 |

## 部署顺序

```bash
kubectl apply -f namespace.yaml
# 先部署 redis-cluster / postgres / nats（本目录未含，按需自备 Operator/StatefulSet）
kubectl apply -f login-deployment.yaml
kubectl apply -f game-statefulset.yaml
kubectl apply -f gate-deployment.yaml
kubectl apply -f game-hpa.yaml          # 需 prometheus-adapter 提供自定义指标
```

## 压测

```bash
# TCP 长连接压测（建连 + 持续收发 + 延迟分位）
go run ./tools/cctest -login http://<login>/api/login -conns 15000 -duration 60s -rate 1
```
