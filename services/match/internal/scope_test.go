package internal

import "testing"

func TestPoolKeySeparatesLocalAndCrossZoneQueues(t *testing.T) {
	global := poolKey("global", 1)
	zoneOne := poolKey("zone:1", 1)
	zoneTwo := poolKey("zone:2", 1)
	if global == zoneOne || zoneOne == zoneTwo {
		t.Fatalf("queue scopes collided: global=%q zone1=%q zone2=%q", global, zoneOne, zoneTwo)
	}
}

func TestRoomHasRole(t *testing.T) {
	room := &roomEntry{Members: []int64{10001, 10002}}
	if !roomHasRole(room, 10002) || roomHasRole(room, 99999) {
		t.Fatal("room membership authorization is incorrect")
	}
}
