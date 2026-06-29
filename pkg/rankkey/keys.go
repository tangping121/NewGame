// Package rankkey 统一管理排行榜在 Redis 中的键名与成员编码规则。
package rankkey

import "fmt"

// Zone 单区排行榜键。
//
// 参数:
//   - zoneID: 区服 ID
//   - board: 榜类型，如 dungeon、level
//
// 返回: 如 rank:zone:1:dungeon
func Zone(zoneID int32, board string) string {
	return fmt.Sprintf("rank:zone:%d:%s", zoneID, board)
}

// Global 跨区全服排行榜键。
//
// 参数:
//   - board: 榜类型
func Global(board string) string {
	return "rank:global:" + board
}

// Member 跨区榜 ZSET 成员编码，区分不同区服同名 roleID。
//
// 参数:
//   - zoneID: 区服 ID
//   - roleID: 角色 ID
func Member(zoneID int32, roleID int64) string {
	return fmt.Sprintf("z%d:%d", zoneID, roleID)
}

// Legacy 旧版单服键名，兼容历史数据。
func Legacy(board string) string {
	return "rank:" + board
}
