package input

import (
	"testing"
)

func TestGetRetoolData(t *testing.T) {
	err := GetRetoolData()
	if err != nil {
		t.Fatalf("Error calling GetRetoolData: %v", err)
	}
}
