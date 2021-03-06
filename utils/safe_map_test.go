package utils

import "testing"

func TestMapContains(t *testing.T) {
	safeMap := NewSafeMap()
	if safeMap.Contains("test") {
		t.Error("fails")
	}
}
