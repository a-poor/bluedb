package storage

import (
	"crypto/rand"
	"fmt"
	"time"
)

const RandWidth = 4

func NewID(did uint) (string, error) {
	// Get the current time in UTC, as bytes...
	now := time.Now().UTC().Unix()

	// Generate a random part...
	r := make([]byte, RandWidth)
	if _, err := rand.Read(r); err != nil {
		return "", err
	}

	// Join them together, with the machine id...
	// Format: <timestamp>-<machine-id>-<rand>
	id := fmt.Sprintf(
		"%x-%04x-%x",
		now,
		did,
		r,
	)
	return id, nil
}
