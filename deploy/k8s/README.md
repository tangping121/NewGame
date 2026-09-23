# Kubernetes 部署

这里的清单提供生产部署基线，包含 Login、Gate、固定逻辑 Game 分片、数据库迁移、mTLS、网络策略、PDB、探针和 Gate HPA。Redis Cluster、PostgreSQL、NATS JetStream、Ingress/L4 LB、证书签发和监控适配器应由平台统一提供。

## 关键约束

- Game 使用 StatefulSet，Pod 序号就是逻辑 `shard_id`。
- `NG_SHARD_COUNT` 是持久化路由的一部分，不能由 HPA 修改。改变它之前必须先用 `tools/reshard` 迁移并核对数据。
- Gate 和 Login 可以水平扩缩；Game 只能按经过审批的重分片方案改变副本数。
- Gate→Game gRPC 使用 protobuf、共享内部令牌和双向 TLS。证书必须包含 `game-shard.bastion.svc` DNS SAN，并允许 `serverAuth` 和 `clientAuth`。
- 所有数据库必须先执行 `migration-job.yaml`。迁移器会锁库、按数字版本执行、校验已应用脚本的 SHA-256，并按 `NG_SHARD_COUNT` 汇总中心角色目录。
- `secret.example.yaml` 只是字段示例。实际 Secret 应由 External Secrets、Vault 或同类系统注入，不能提交到 Git。

## 镜像

```bash
docker build --build-arg SERVICE=login -t registry.example/bastion/login:0.2.0 .
docker build --build-arg SERVICE=gate  -t registry.example/bastion/gate:0.2.0 .
docker build --build-arg SERVICE=game  -t registry.example/bastion/game:0.2.0 .
docker build -f Dockerfile.migrate -t registry.example/bastion/migrate:0.2.0 .
```

推送镜像后，将清单里的示例镜像名替换为不可变 digest 或正式版本号。

## 部署顺序

```bash
kubectl apply -f namespace.yaml
kubectl apply -f secret.yaml
kubectl apply -f configmaps.yaml

kubectl apply -f migration-job.yaml
kubectl wait --for=condition=complete job/bastion-schema-migrate -n bastion --timeout=10m

kubectl apply -f network-policy.yaml
kubectl apply -f pod-disruption-budgets.yaml
kubectl apply -f login-deployment.yaml
kubectl apply -f game-statefulset.yaml
kubectl apply -f gate-deployment.yaml

# 需要 metrics adapter 暴露 ng_gate_connections。
kubectl apply -f game-hpa.yaml
```

## 发布检查

```bash
kubectl rollout status deployment/login -n bastion
kubectl rollout status statefulset/game-shard -n bastion
kubectl rollout status deployment/gate -n bastion
kubectl get pods,svc,pdb,hpa -n bastion
```

发布前还应确认：

- Secret 中的中心库及所有分片 DSN 都启用 TLS；
- NATS 是启用 JetStream 的持久化集群；
- Redis 使用 Cluster 或高可用部署，并启用持久化；
- Game StatefulSet 副本数与 `NG_SHARD_COUNT` 完全一致；
- `bastion-public-addresses.gate_tcp` 是客户端实际可访问的 L4 地址；
- `/readyz`、`/livez` 和 `/metrics/prometheus` 已接入探针和监控。
