package quest

import "newgame/pkg/repo"

const (
	StatusNone     int32 = 0
	StatusAccepted int32 = 1
	StatusDone     int32 = 2
)

// State 玩家任务状态与进度计数。
type State struct {
	Status map[string]int32
	Prog   map[string]int32
}

func FromSnapshot(snap repo.RoleSnapshot) State {
	st := State{
		Status: snap.Quests,
		Prog:   snap.QuestProg,
	}
	if st.Status == nil {
		st.Status = map[string]int32{}
	}
	if st.Prog == nil {
		st.Prog = map[string]int32{}
	}
	return st
}

func (s State) ToSnapshotFields() (map[string]int32, map[string]int32) {
	return s.Status, s.Prog
}

func (s State) Accept(questID string) error {
	if st := s.Status[questID]; st == StatusAccepted || st == StatusDone {
		return ErrAlreadyAccepted
	}
	s.Status[questID] = StatusAccepted
	s.Prog[questID] = 0
	return nil
}

func (s State) AddProgress(questID string, delta int32) (int32, error) {
	if s.Status[questID] != StatusAccepted {
		return 0, ErrNotAccepted
	}
	s.Prog[questID] += delta
	return s.Prog[questID], nil
}

func (s State) Complete(questID string, need int32) (bool, error) {
	if s.Status[questID] != StatusAccepted {
		return false, ErrNotAccepted
	}
	if s.Prog[questID] < need {
		return false, nil
	}
	s.Status[questID] = StatusDone
	return true, nil
}

func (s State) List() map[string]any {
	out := map[string]any{}
	for id, st := range s.Status {
		out[id] = map[string]any{
			"status":   st,
			"progress": s.Prog[id],
		}
	}
	return out
}
