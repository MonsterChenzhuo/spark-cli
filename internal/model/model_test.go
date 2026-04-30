package model

import "testing"

func TestNewApplicationInitializesMaps(t *testing.T) {
	a := NewApplication()
	if a.Executors == nil || a.Jobs == nil || a.Stages == nil {
		t.Fatal("maps not initialized")
	}
}

func TestStageKeyEquality(t *testing.T) {
	k1 := StageKey{ID: 7, Attempt: 0}
	k2 := StageKey{ID: 7, Attempt: 0}
	if k1 != k2 {
		t.Fatal("equal keys should be ==")
	}
}
