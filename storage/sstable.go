package storage

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/google/uuid"
)

const (
	DefaultBloomFilterSize = 10_000_000
	DefaultBloomFilterFPR  = 0.01
)

// SSTBuilder is used to build a new SSTable.
type SSTBuilder struct {
	Path  string // The path to the table's directory
	Level uint8  // The table's level

	id     string
	minKey string
	maxKey string
	count  uint64
	bf     *bloom.BloomFilter
	file   *os.File
	create time.Time
}

// SetUp sets up the SSTBuilder. It generates a unique id,
// sets the create timestamp, opens the data file, and
// initializes the bloom filter.
func (b *SSTBuilder) SetUp() error {
	// Generate an id
	id, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	b.id = id.String()

	// Set the create timestamp
	b.create = time.Now()

	// Open the data file
	p := fmtSSTDataPath(b.Path, b.id)
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	b.file = f

	// Set up the bloom filter
	b.bf = bloom.NewWithEstimates(
		DefaultBloomFilterSize,
		DefaultBloomFilterFPR,
	)

	// Done
	return nil
}

// Add adds a record to the SSTable builder.
//
// It stores the record in the data file and updates
// the metadata (min/max keys, record count, bloom filter).
func (tb *SSTBuilder) Add(r Record) error {
	// Encode the record as json
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}

	// Write the record to the file
	b = append(b, '\n')
	if _, err := tb.file.Write(b); err != nil {
		return err
	}

	// Add the key to the bloom filter
	tb.bf.Add([]byte(r.Key))

	// Update the min/max keys
	if tb.count == 0 || r.Key < tb.minKey {
		tb.minKey = r.Key
	}
	if tb.count == 0 || r.Key > tb.maxKey {
		tb.maxKey = r.Key
	}
	tb.count++

	// Done
	return nil
}

// Finish finishes building the SSTable.
//
// It resets the data file handle, generates the metadata,
// and generates and stores the metadata and bloom filter
// to disk, in the given path.
func (tb *SSTBuilder) Finish() (*SSTable, error) {
	// Seek back to the beginning of the file
	if _, err := tb.file.Seek(0, 0); err != nil {
		return nil, err
	}

	// Create the metadata
	md := SSTMeta{
		ID:          tb.id,
		Level:       tb.Level,
		MinKey:      tb.minKey,
		MaxKey:      tb.maxKey,
		RecordCount: tb.count,
		CreatedAt:   tb.create,
	}

	// Write the metadata to disk
	mdp := fmtSSTMetaPath(tb.Path, tb.id)
	b, err := json.Marshal(md)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(mdp, b, 0644); err != nil {
		return nil, err
	}

	// Write the bloom filter to disk
	bfp := fmtSSTBloomPath(tb.Path, tb.id)
	b, err = tb.bf.MarshalBinary()
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(bfp, b, 0644); err != nil {
		return nil, err
	}

	// Create the sstable
	t := &SSTable{
		id:    tb.id,
		path:  tb.Path,
		meta:  md,
		file:  tb.file,
		bloom: tb.bf,
	}

	// Done
	return t, nil
}

// SSTable is a sorted string table.
//
// It is a sorted list of records, with a bloom filter.
type SSTable struct {
	sync.Mutex
	id    string
	path  string
	meta  SSTMeta
	file  *os.File
	bloom *bloom.BloomFilter
}

// ReadSSTable reads in an existing SSTable, with the given id,
// at the given path, and returns it.
//
// It reads in the SSTable's metadata, opens a file handle,
// and generates the bloom filter.
func ReadSSTable(p string, id string) (*SSTable, error) {
	// Load the metadata file
	metaPath := fmtSSTMetaPath(p, id)
	b, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read sst id=%q meta file: %w", id, err)
	}
	var meta SSTMeta
	if err := json.Unmarshal(b, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sst id=%q meta file as json: %w", id, err)
	}

	// Read in the bloom filter
	bfPath := fmtSSTBloomPath(p, id)
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
	filePath := fmtSSTDataPath(p, id)
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
		bloom: &bloom,
	}, nil
}

// MightContain checks if the SSTable *might* contain the key.
//
// It checks if the key is in the table's range and if the key
// could be in the table's bloom filter.
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

	// Scan the table
	var record *Record
	if err := t.scan(func(r Record) (bool, error) {
		// Have we passed the key?
		if r.Key > key {
			return true, nil
		}

		// Is it the key we're looking for?
		if r.Key == key {
			record = &r
			return true, nil
		}

		// Otherwise, keep going
		return false, nil
	}); err != nil {
		return nil, err
	}

	// Done
	return record, nil
}

// Close closes the SSTable's open connections.
func (t *SSTable) Close() error {
	t.Lock()
	defer t.Unlock()

	// Close the file
	err := t.file.Close()
	if err != nil {
		return err
	}

	// Reset the file handle
	t.file = nil

	// Done
	return nil
}

// scan will scan through the SSTable records using the given
// function. The function accepts the next record and returns
// a boolean to signify that the scanner is done.
func (t *SSTable) scan(fn func(r Record) (done bool, err error)) error {
	// Lock the table
	t.Lock()
	defer t.Unlock()

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
		if len(b) == 0 {
			continue
		}

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

func (t *SSTable) DeleteTable() error {
	// Lock the table
	t.Lock()
	defer t.Unlock()

	// Close the file
	if err := t.file.Close(); err != nil {
		return err
	}

	// Delete the files
	mdp := fmtSSTMetaPath(t.path, t.id)
	if err := os.Remove(mdp); err != nil {
		return err
	}

	bfp := fmtSSTBloomPath(t.path, t.id)
	if err := os.Remove(bfp); err != nil {
		return err
	}

	dp := fmtSSTDataPath(t.path, t.id)
	if err := os.Remove(dp); err != nil {
		return err
	}

	// Done
	return nil
}

type SSTMeta struct {
	ID          string
	Level       uint8
	MinKey      string
	MaxKey      string
	RecordCount uint64
	CreatedAt   time.Time
}

func fmtSSTMetaPath(p, id string) string {
	return path.Join(p, id+".meta")
}

func fmtSSTBloomPath(p, id string) string {
	return path.Join(p, id+".bloom")
}

func fmtSSTDataPath(p, id string) string {
	return path.Join(p, id+".data")
}

type sstIterator struct {
	sync.Mutex
	table   *SSTable
	c       chan Record
	done    bool
	current Record
}

func (itr *sstIterator) start() {
	itr.Lock()
	itr.c = make(chan Record)
	itr.Unlock()

	go func() {
		defer close(itr.c)
		itr.table.scan(func(r Record) (bool, error) {
			itr.c <- r
			return false, nil
		})

		itr.Lock()
		itr.done = true
		itr.Unlock()
	}()
}

func (itr *sstIterator) next() bool {
	itr.Lock()
	defer itr.Unlock()
	if itr.done {
		return false
	}
	itr.current = <-itr.c
	return true
}
