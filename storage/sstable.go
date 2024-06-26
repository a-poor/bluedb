package storage

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path"

	"github.com/bits-and-blooms/bloom/v3"
)

type SSTable struct {
	id    string
	path  string
	meta  SSTMeta
	file  *os.File
	bloom bloom.BloomFilter
}

func NewSSTable(p string) (*SSTable, error) {
	return nil, fmt.Errorf("not implemented")
}

// ReadSSTable reads in an existing SSTable, with the given id,
// at the given path, and returns it.
//
// It reads in the SSTable's metadata, opens a file handle,
// and generates the bloom filter.
func ReadSSTable(id string, p string) (*SSTable, error) {
	// Load the metadata file
	metaPath := path.Join(p, id+".meta")
	b, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read sst id=%q meta file: %w", id, err)
	}
	var meta SSTMeta
	if err := json.Unmarshal(b, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sst id=%q meta file as json: %w", id, err)
	}

	// Read in the bloom filter
	bfPath := path.Join(p, id+".bloom")
	f, err := os.Open(bfPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sst id=%q bloom filter: %w", id, err)
	}
	defer f.Close()

	var bloom bloom.BloomFilter
	if err := binary.Read(f, binary.LittleEndian, &bloom); err != nil {
		return nil, fmt.Errorf("failed to read sst id=%q bloom filter: %w", id, err)
	}

	// Open the data file
	filePath := path.Join(p, id+".data")
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sst id=%q data file: %w", id, err)
	}

	// Create and return the table
	return &SSTable{
		id:    id,
		path:  p,
		meta:  meta,
		file:  file,
		bloom: bloom,
	}, nil
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
	scan := bufio.NewScanner(t.file)

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
