// Package bag 玩家背包：item_id 到数量的映射。
package bag

// Inventory 背包数据，key 为道具 ID，value 为持有数量。
type Inventory map[string]int32

// New 创建空背包。
func New() Inventory {
	return Inventory{}
}

// Add 增加道具数量；count<=0 时不操作。
//
// 参数:
//   - itemID: 道具 ID，如 potion、gem
//   - count: 增加数量
func (inv Inventory) Add(itemID string, count int32) {
	if count <= 0 {
		return
	}
	inv[itemID] += count
}

// Remove 扣除道具；数量不足时不修改并返回 false。
//
// 参数:
//   - itemID: 道具 ID
//   - count: 扣除数量，必须 > 0
//
// 返回: 成功 true；不足 false
func (inv Inventory) Remove(itemID string, count int32) bool {
	if count <= 0 {
		return true
	}
	have := inv[itemID]
	if have < count {
		return false
	}
	inv[itemID] = have - count
	if inv[itemID] == 0 {
		delete(inv, itemID)
	}
	return true
}

// Clone 深拷贝背包，用于快照或离线计算。
func (inv Inventory) Clone() Inventory {
	out := New()
	for k, v := range inv {
		out[k] = v
	}
	return out
}
