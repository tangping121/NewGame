// Package guild 公会加入与信息查询；Postgres 持久化或内存兜底。
package guild

import (
	"context"
	"fmt"

	"newgame/pkg/repo"
)

// Guild 公会视图（含成员列表）。
type Guild struct {
	ID      int64   // 公会 ID
	Name    string  // 公会名称
	Members []int64 // 成员 role_id 列表
}

// Service 公会业务。
type Service struct {
	repo   *repo.GuildRepo // 公会表；nil 用 mem
	zoneID int32           // 本 Game 进程区服（预留扩展）
	mem    map[int64]*Guild // 内存模式公会表
}

// New 创建公会服务。
//
// 参数:
//   - r: GuildRepo；nil 为内存模式，预置 ID=1 default_guild
//   - zoneID: 当前区服 ID
func New(r *repo.GuildRepo, zoneID int32) *Service {
	s := &Service{repo: r, zoneID: zoneID, mem: map[int64]*Guild{}}
	s.mem[1] = &Guild{ID: 1, Name: "default_guild", Members: []int64{}}
	return s
}

// Join 角色加入公会。
//
// 参数:
//   - ctx: 数据库上下文
//   - roleID: 角色 ID
//   - guildID: 目标公会 ID
//
// 返回: 加入后的公会信息（含更新成员列表）
func (s *Service) Join(ctx context.Context, roleID, guildID int64) (*Guild, error) {
	if s.repo != nil {
		g, err := s.repo.Get(ctx, guildID)
		if err != nil {
			return nil, fmt.Errorf("guild not found")
		}
		if err := s.repo.Join(ctx, guildID, roleID); err != nil {
			return nil, err
		}
		members, _ := s.repo.ListMembers(ctx, guildID)
		return &Guild{ID: g.ID, Name: g.Name, Members: members}, nil
	}
	g, ok := s.mem[guildID]
	if !ok {
		return nil, fmt.Errorf("guild not found")
	}
	g.Members = appendUnique(g.Members, roleID)
	return g, nil
}

// Info 查询公会详情与成员列表。
//
// 参数:
//   - ctx: 上下文
//   - guildID: 公会 ID
func (s *Service) Info(ctx context.Context, guildID int64) (*Guild, error) {
	if s.repo != nil {
		g, err := s.repo.Get(ctx, guildID)
		if err != nil {
			return nil, fmt.Errorf("guild not found")
		}
		members, _ := s.repo.ListMembers(ctx, guildID)
		return &Guild{ID: g.ID, Name: g.Name, Members: members}, nil
	}
	g, ok := s.mem[guildID]
	if !ok {
		return nil, fmt.Errorf("guild not found")
	}
	return g, nil
}

func appendUnique(list []int64, id int64) []int64 {
	for _, v := range list {
		if v == id {
			return list
		}
	}
	return append(list, id)
}
