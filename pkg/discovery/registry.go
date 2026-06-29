// Package discovery 基于 Redis TTL 实现服务注册、发现与健康实例选择。
package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"newgame/pkg/zone"
)

const keyPrefix = "ng:disc:"

// Registry 在 Redis 中维护服务实例，租约到期自动失效。
type Registry struct {
	rdb    goredis.UniversalClient
	ttl    time.Duration
	picker sync.Map // service:zone -> 轮询计数器
}

func NewRegistry(rdb goredis.UniversalClient, ttl time.Duration) *Registry {
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	return &Registry{rdb: rdb, ttl: ttl}
}

// indexKey 某服务在某区服的实例 ID 集合。
func (r *Registry) indexKey(name string, zoneID int32) string {
	return fmt.Sprintf("%sidx:%s:%d", keyPrefix, name, zoneID)
}

// instKey 单个实例的 JSON 详情。
func (r *Registry) instKey(id string) string {
	return keyPrefix + "inst:" + id
}

// Register 注册或覆盖一个服务实例，并加入区服索引集合。
//
// 参数:
//   - ctx: Redis 上下文
//   - inst: 实例信息；ID、Name 不可为空
func (r *Registry) Register(ctx context.Context, inst Instance) error {
	if inst.ID == "" || inst.Name == "" {
		return fmt.Errorf("instance id and name required")
	}
	data, err := json.Marshal(inst)
	if err != nil {
		return err
	}
	pipe := r.rdb.Pipeline()
	pipe.Set(ctx, r.instKey(inst.ID), data, r.ttl)
	pipe.SAdd(ctx, r.indexKey(inst.Name, inst.ZoneID), inst.ID)
	_, err = pipe.Exec(ctx)
	return err
}

// Renew 心跳续约，刷新 TTL 并确保仍在索引中。
// Renew 心跳续约：刷新实例 TTL 并确保仍在索引 Set 中。
func (r *Registry) Renew(ctx context.Context, inst Instance) error {
	data, err := json.Marshal(inst)
	if err != nil {
		return err
	}
	pipe := r.rdb.Pipeline()
	pipe.Set(ctx, r.instKey(inst.ID), data, r.ttl)
	pipe.SAdd(ctx, r.indexKey(inst.Name, inst.ZoneID), inst.ID)
	_, err = pipe.Exec(ctx)
	return err
}

// Deregister 主动注销实例，删除详情 key 并从索引移除。
func (r *Registry) Deregister(ctx context.Context, inst Instance) error {
	pipe := r.rdb.Pipeline()
	pipe.Del(ctx, r.instKey(inst.ID))
	pipe.SRem(ctx, r.indexKey(inst.Name, inst.ZoneID), inst.ID)
	_, err := pipe.Exec(ctx)
	return err
}

// Discover 列出某区服下仍存活（未过期）的实例，并清理索引中的僵尸 ID。
// Discover 列出某服务在某区服下所有仍存活（未过期）的实例，并清理索引中的过期 ID。
//
// 参数:
//   - ctx: Redis 上下文
//   - name: 服务类型，如 game、gate
//   - zoneID: 区服 ID
func (r *Registry) Discover(ctx context.Context, name string, zoneID int32) ([]Instance, error) {
	ids, err := r.rdb.SMembers(ctx, r.indexKey(name, zoneID)).Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("no instances for %s zone %d", name, zoneID)
	}

	var out []Instance
	var stale []string
	for _, id := range ids {
		raw, err := r.rdb.Get(ctx, r.instKey(id)).Result()
		if err == goredis.Nil {
			stale = append(stale, id)
			continue
		}
		if err != nil {
			return nil, err
		}
		var inst Instance
		if err := json.Unmarshal([]byte(raw), &inst); err != nil {
			stale = append(stale, id)
			continue
		}
		out = append(out, inst)
	}
	if len(stale) > 0 {
		members := make([]interface{}, len(stale))
		for i, id := range stale {
			members[i] = id
		}
		_ = r.rdb.SRem(ctx, r.indexKey(name, zoneID), members...).Err()
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no live instances for %s zone %d", name, zoneID)
	}
	return out, nil
}

// Pick 轮询选取一个实例（不检查健康）。
// Pick 轮询选取一个实例（不探测 /health）。
func (r *Registry) Pick(ctx context.Context, name string, zoneID int32) (Instance, error) {
	list, err := r.Discover(ctx, name, zoneID)
	if err != nil {
		return Instance{}, err
	}
	key := fmt.Sprintf("%s:%d", name, zoneID)
	v, _ := r.picker.LoadOrStore(key, new(uint64))
	counter := v.(*uint64)
	n := uint64(len(list))
	idx := rand.Uint64() % n
	if n > 1 {
		idx = (*counter) % n
		*counter++
	}
	return list[idx], nil
}

var healthHTTP = &http.Client{Timeout: 2 * time.Second}

func (r *Registry) healthy(ctx context.Context, inst Instance) bool {
	if inst.HTTPAddr == "" {
		return inst.TCPAddr != ""
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, inst.HTTPBase()+"/health", nil)
	if err != nil {
		return false
	}
	resp, err := healthHTTP.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// PickHealthy 选取响应 GET /health 的正常实例。
func (r *Registry) PickHealthy(ctx context.Context, name string, zoneID int32) (Instance, error) {
	list, err := r.Discover(ctx, name, zoneID)
	if err != nil {
		return Instance{}, err
	}
	rand.Shuffle(len(list), func(i, j int) { list[i], list[j] = list[j], list[i] })
	for _, inst := range list {
		if r.healthy(ctx, inst) {
			return inst, nil
		}
	}
	return Instance{}, fmt.Errorf("no healthy instances for %s zone %d", name, zoneID)
}

// DiscoverAll 汇总所有已知区服下的存活实例。
func (r *Registry) DiscoverAll(ctx context.Context, name string) ([]Instance, error) {
	var out []Instance
	for _, z := range zone.Catalog {
		list, err := r.Discover(ctx, name, z.ID)
		if err != nil {
			continue
		}
		out = append(out, list...)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no instances for %s", name)
	}
	return out, nil
}

// PickHealthyGlobal 从任意区服选取一个健康实例（用于跨服 Battle 等）。
func (r *Registry) PickHealthyGlobal(ctx context.Context, name string) (Instance, error) {
	list, err := r.DiscoverAll(ctx, name)
	if err != nil {
		return Instance{}, err
	}
	rand.Shuffle(len(list), func(i, j int) { list[i], list[j] = list[j], list[i] })
	for _, inst := range list {
		if r.healthy(ctx, inst) {
			return inst, nil
		}
	}
	return Instance{}, fmt.Errorf("no healthy instances for %s", name)
}

// Lifecycle 在进程存活期间保持注册，Stop 时注销。
type Lifecycle struct {
	reg    *Registry
	inst   Instance
	cancel context.CancelFunc
	log    *zap.Logger
}

// Start 注册实例并启动后台心跳协程。
func (r *Registry) Start(inst Instance, heartbeat time.Duration, log *zap.Logger) (*Lifecycle, error) {
	if heartbeat <= 0 {
		heartbeat = 5 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	lc := &Lifecycle{reg: r, inst: inst, cancel: cancel, log: log}

	if err := r.Register(ctx, inst); err != nil {
		cancel()
		return nil, err
	}
	log.Info("discovery registered",
		zap.String("name", inst.Name),
		zap.Int32("zone", inst.ZoneID),
		zap.String("id", inst.ID),
		zap.String("http", inst.HTTPAddr),
		zap.String("tcp", inst.TCPAddr),
	)

	go func() {
		ticker := time.NewTicker(heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				hbCtx, hbCancel := context.WithTimeout(context.Background(), 3*time.Second)
				if err := r.Renew(hbCtx, inst); err != nil && log != nil {
					log.Warn("discovery heartbeat failed", zap.Error(err))
				}
				hbCancel()
			}
		}
	}()
	return lc, nil
}

// Stop 停止心跳并注销实例，进程退出时调用。
func (lc *Lifecycle) Stop() {
	if lc == nil {
		return
	}
	lc.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := lc.reg.Deregister(ctx, lc.inst); err != nil && lc.log != nil {
		lc.log.Warn("discovery deregister failed", zap.Error(err))
	} else if lc.log != nil {
		lc.log.Info("discovery deregistered", zap.String("id", lc.inst.ID))
	}
}
