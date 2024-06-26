package storage

import (
	"strings"
	"testing"
)

func TestNewID(t *testing.T) {
	t.Run("should generate a new id", func(t *testing.T) {
		id, err := NewID(1)
		if err != nil {
			t.Fatal(err)
		}
		if id == "" {
			t.Fatal("id is empty")
		}

		parts := strings.Split(id, "-")
		if len(parts) != 3 {
			t.Fatalf("expected 3 parts, found %d", len(parts))
		}
	})
}
