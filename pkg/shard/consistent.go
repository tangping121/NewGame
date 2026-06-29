package shard

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Ring 一致性哈希环，支持虚拟节点，扩缩容时仅迁移少量 key。
//
// 用途：替代 role_id % N 取模路由。取模在分片数变化时几乎全量重分布；
// 一致性哈希仅迁移约 1/N 的 key，适合 Game 分片 / PG 分库的平滑扩容。
//
// 用法：
//
//	r := shard.NewRing(150)            // 每个物理节点 150 个虚拟节点
//	r.Add("game-shard-0", "game-shard-1", ...)
//	node := r.Get(roleID)              // role_id → 物理节点名
type Ring struct {
	replicas int               // 每节点虚拟节点数
	keys     []uint32          // 已排序的虚拟节点哈希
	hashMap  map[uint32]string // 虚拟节点哈希 -> 物理节点名
	nodes    map[string]bool   // 已加入的物理节点集合
}

// NewRing 创建哈希环。
//
// 参数:
//   - replicas: 每个物理节点的虚拟节点数；<=0 时默认 150（平衡分布与内存）
func NewRing(replicas int) *Ring {
	if replicas <= 0 {
		replicas = 150
	}
	return &Ring{
		replicas: replicas,
		hashMap:  make(map[uint32]string),
		nodes:    make(map[string]bool),
	}
}

func (r *Ring) hash(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}

// Add 向环中加入一个或多个物理节点。
func (r *Ring) Add(nodes ...string) {
	for _, node := range nodes {
		if node == "" || r.nodes[node] {
			continue
		}
		r.nodes[node] = true
		for i := 0; i < r.replicas; i++ {
			h := r.hash(node + "#" + strconv.Itoa(i))
			r.keys = append(r.keys, h)
			r.hashMap[h] = node
		}
	}
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

// Remove 从环中移除一个物理节点（缩容）。
func (r *Ring) Remove(node string) {
	if !r.nodes[node] {
		return
	}
	delete(r.nodes, node)
	kept := r.keys[:0]
	for _, h := range r.keys {
		if r.hashMap[h] == node {
			delete(r.hashMap, h)
			continue
		}
		kept = append(kept, h)
	}
	r.keys = kept
}

// Get 返回 role_id 映射到的物理节点名；环为空时返回 ""。
func (r *Ring) Get(roleID int64) string {
	if len(r.keys) == 0 {
		return ""
	}
	h := r.hash(strconv.FormatInt(roleID, 10))
	idx := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
	if idx == len(r.keys) {
		idx = 0 // 环形回绕
	}
	return r.hashMap[r.keys[idx]]
}

// Nodes 返回当前物理节点数量。
func (r *Ring) Nodes() int {
	return len(r.nodes)
}

// RingForShards 构造覆盖 game-shard-0..count-1 的一致性哈希环。
func RingForShards(count int32, replicas int) *Ring {
	r := NewRing(replicas)
	for i := int32(0); i < count; i++ {
		r.Add(ServiceName(i))
	}
	return r
}
