// Package grant 解析奖励字符串并构造发奖数据结构，供邮件、支付、GM 等模块使用。
package grant

import (
	"fmt"
	"strconv"
	"strings"
)

// Bundle 解析后的奖励包。
type Bundle struct {
	Gold  int64            // 金币数量
	Items map[string]int32 // 道具 ID -> 数量，如 {"potion": 2}
}

// Parse 将逗号分隔的 key:value 奖励串解析为 Bundle。
//
// 参数:
//   - raw: 奖励字符串，格式 "gold:100,potion:2,gem:10"；空串返回空 Bundle
//
// 返回:
//   - Bundle: 解析结果；gold 累加到 Gold，其余 key 累加到 Items
//   - error: 格式非法或数量非数字时返回错误
//
// 示例: Parse("gold:100,potion:2") => Gold=100, Items={"potion":2}
func Parse(raw string) (Bundle, error) {
	b := Bundle{Items: map[string]int32{}}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return b, nil
	}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			return Bundle{}, fmt.Errorf("invalid item token %q", part)
		}
		key := strings.TrimSpace(kv[0])
		valStr := strings.TrimSpace(kv[1])
		n, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return Bundle{}, fmt.Errorf("invalid amount in %q", part)
		}
		if key == "gold" {
			b.Gold += n
		} else {
			b.Items[key] += int32(n)
		}
	}
	return b, nil
}
