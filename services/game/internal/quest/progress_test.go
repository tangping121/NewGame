package quest_test

import (
	"testing"

	"newgame/services/game/internal/quest"
)

func TestQuestFlow(t *testing.T) {
	st := quest.State{Status: map[string]int32{}, Prog: map[string]int32{}}
	if err := st.Accept("main_1"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AddProgress("main_1", 2); err != nil {
		t.Fatal(err)
	}
	ok, err := st.Complete("main_1", 2)
	if err != nil || !ok {
		t.Fatalf("complete failed ok=%v err=%v", ok, err)
	}
	if st.Status["main_1"] != quest.StatusDone {
		t.Fatalf("expected done status")
	}
}
