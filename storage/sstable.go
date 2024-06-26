package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/bits-and-blooms/bloom/v3"
)

type SSTable struct {
	id    string
	meta  SSTMeta
	file  os.File
	bloom bloom.BloomFilter
}

func NewSSTable() (*SSTable, error) {
	return nil, fmt.Errorf("not implemented")
}

func ReadSSTable() (*SSTable, error) {
	return nil, fmt.Errorf("not implemented")
}

func (t *SSTable) MightContain(key string) (bool, error) {
	// Validate the key
	if len(key) == 0 {
		return false, fmt.Errorf("key is empty")
	}

	// Is it out of range of the min/max?
	if key < t.meta.MinKey || key > t.meta.MaxKey {
		return false, nil
	}

	// Is it in the bloom filter?
	if !t.bloom.Test([]byte(key)) {
		return false, nil
	}

	// Otherwise, it *probably* is in the table
	return true, nil
}

func (t *SSTable) Get(key string) (*Record, error) {
	// First check if it *might* be in the table
	maybe, err := t.MightContain(key)
	if err != nil {
		return nil, err
	}
	if !maybe {
		return nil, nil
	}

	// TODO - Scan the table...
	return nil, fmt.Errorf("not implemented")
}

func (t *SSTable) Merge(other *SSTable) (*SSTable, error) {
	return nil, fmt.Errorf("not implemented")
}

func (t *SSTable) Close() error {
	return t.file.Close()
}

func (t *SSTable) scan(fn func(r Record) (bool, error)) error {
	// Seek back to the beginning of the file
	if _, err := t.file.Seek(0, 0); err != nil {
		return err
	}

	// Create a new scanner
	scan := bufio.NewScanner(&t.file)

	// Scan the table
	for scan.Scan() {
		// Get the record
		b := scan.Bytes()

		// Decode the record
		var r Record
		if err := json.Unmarshal(b, &r); err != nil {
			return err
		}

		// Run the callback
		done, err := fn(r)
		if err != nil {
			return err
		}

		// If we're done, stop scanning
		if done {
			break
		}
	}

	// Check for errors
	if err := scan.Err(); err != nil {
		return err
	}

	// Success!
	return nil
}

type SSTMeta struct {
	ID          string
	Level       uint8
	MinKey      string
	MaxKey      string
	RecordCount uint64
}
