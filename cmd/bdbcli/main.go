package main

import (
	"fmt"

	"github.com/a-poor/bluedb/storage"
)

func main() {
	// panic("Not implemented...")
	id, err := storage.NewID(0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("ID: %s [%d]\n", id, len(id))
}
