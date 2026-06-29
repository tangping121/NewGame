package skill

import "newgame/pkg/repo"

// Book 玩家技能等级表，skill_id -> level。
type Book map[string]int32

func FromSnapshot(snap repo.RoleSnapshot) Book {
	if snap.Skills == nil {
		return Book{}
	}
	return Book(snap.Skills)
}

func (b Book) ToMap() map[string]int32 {
	out := map[string]int32{}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func (b Book) Level(skillID string) int32 {
	return b[skillID]
}

func (b Book) Upgrade(skillID string) int32 {
	b[skillID]++
	return b[skillID]
}

func (b Book) List() map[string]int32 {
	return b.ToMap()
}
