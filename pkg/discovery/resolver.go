package discovery

import (
	"context"
	"sync"
	"time"
)

// Resolver 在 Registry.PickHealthy 之上加 TTL 缓存，避免热路径每次都做
// Redis 查询 + HTTP 健康探测。适用于 Gate→Game、Pay/Mail/GM→Game 等高频调用。
//
// 语义：缓存命中且未过期直接返回；过期则刷新；刷新失败时回退到上次结果
// （stale-on-error），优先保证可用性。
type Resolver struct {
	reg *Registry
	ttl time.Duration
	mu  sync.RWMutex
	m   map[string]*entry
}

type entry struct {
	inst Instance
	exp  time.Time
	ok   bool
}

// NewResolver 创建缓存解析器。
//
// 参数:
//   - reg: 底层注册表；nil 时 Resolve 始终失败
//   - ttl: 缓存有效期；<=0 默认 2s
func NewResolver(reg *Registry, ttl time.Duration) *Resolver {
	if ttl <= 0 {
		ttl = 2 * time.Second
	}
	return &Resolver{reg: reg, ttl: ttl, m: make(map[string]*entry)}
}

// cacheKey 生成区服级缓存键。
func cacheKey(name string, zoneID int32) string {
	return name + ":" + itoa(zoneID)
}

// cacheKeyGlobal 生成跨区服全局缓存键。
func cacheKeyGlobal(name string) string {
	return name + ":global"
}

// Resolve 返回某服务在某区服的健康实例（带缓存）。
func (r *Resolver) Resolve(ctx context.Context, name string, zoneID int32) (Instance, bool) {
	if r == nil || r.reg == nil {
		return Instance{}, false
	}
	key := cacheKey(name, zoneID)
	return r.resolve(ctx, key, func() (Instance, error) {
		return r.reg.PickHealthy(ctx, name, zoneID)
	})
}

// ResolveGlobal 从任意区服选取一个健康实例（带缓存）。
//
// 适用于 Battle 等跨区服单实例服务；Match 创建房间时优先调用本方法，
// 失败时可再回退到本区 Resolve(name, zoneID)。
func (r *Resolver) ResolveGlobal(ctx context.Context, name string) (Instance, bool) {
	if r == nil || r.reg == nil {
		return Instance{}, false
	}
	key := cacheKeyGlobal(name)
	return r.resolve(ctx, key, func() (Instance, error) {
		return r.reg.PickHealthyGlobal(ctx, name)
	})
}

// resolve 通用缓存刷新逻辑：命中 TTL 直接返回；失败时 stale-on-error。
func (r *Resolver) resolve(ctx context.Context, key string, pick func() (Instance, error)) (Instance, bool) {
	r.mu.RLock()
	e := r.m[key]
	r.mu.RUnlock()
	if e != nil && e.ok && time.Now().Before(e.exp) {
		return e.inst, true
	}

	inst, err := pick()
	if err != nil {
		// 刷新失败：回退到上次缓存（即使已过期），提升可用性。
		if e != nil && e.ok {
			return e.inst, true
		}
		return Instance{}, false
	}
	r.mu.Lock()
	r.m[key] = &entry{inst: inst, exp: time.Now().Add(r.ttl), ok: true}
	r.mu.Unlock()
	return inst, true
}

func itoa(v int32) string {
	// 小整数快速转换，避免 strconv 依赖。
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [12]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
