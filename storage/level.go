package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"slices"
	"sync"
	"time"
)

// DefaultLevelMaxSize is the default maximum number
// of tables that can be stored in a level.
const DefaultLevelMaxSize = 10

type Level struct {
	sync.RWMutex
	path   string     // The path to this level's directory on disk
	meta   LevelMeta  // The level's metadata
	tables []*SSTable // Handles to the level's tables
}

// CreateLevel creates a new level handle for the given level
// number in the given directory.
func CreateLevel(n uint16, d string) (*Level, error) {
	// Format the level path
	p := fmtLevelPath(d, n)

	// Make the directory
	if err := os.Mkdir(p, 0755); err != nil {
		return nil, err
	}

	// Create the metadata
	meta := LevelMeta{
		Level:   n,
		MinKey:  "",
		MaxKey:  "",
		Tables:  []string{},
		MaxSize: DefaultLevelMaxSize,
	}

	// Write the metadata file
	metaPath := path.Join(p, "_meta.json")
	b, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(metaPath, b, 0644); err != nil {
		return nil, err
	}

	// Create the level
	level := &Level{
		path:   p,
		meta:   meta,
		tables: []*SSTable{},
	}

	// Done
	return level, nil
}

func LoadLevel(n uint16, d string) (*Level, error) {
	return nil, fmt.Errorf("not implemented")
}

// Full checks if the level has the maximum number of tables.
func (l *Level) Full() bool {
	return len(l.tables) >= int(l.meta.MaxSize)
}

func (l *Level) Get(key string) (*Record, error) {
	// Check if the key is in range
	if key < l.meta.MinKey || key > l.meta.MaxKey {
		return nil, nil
	}

	// Iterate over the tables, in reverse order
	for i := len(l.tables) - 1; i >= 0; i-- {
		// Get the table
		table := l.tables[i]

		// Get the record
		r, err := table.Get(key)
		if err != nil {
			return nil, err
		}

		// If the record is found, return it
		//
		// Note that this includes tombstones
		if r != nil {
			return r, nil
		}
	}

	// If the record is not found, return nil
	return nil, nil
}

func (l *Level) AddTable(table *SSTable) error {
	return fmt.Errorf("not implemented")
}

// Compact merges the data in the tables in the level l, into
// a single table, and writes it to the next level's directory,
// at the given path, and returns a handle to the new table.
func (l *Level) Compact(path string) (*SSTable, []string, error) {
	l.Lock()
	defer l.Unlock()

	// Make sure there are tables to compact
	if len(l.tables) == 0 {
		return nil, nil, fmt.Errorf("no tables to compact")
	}

	// Get the level's directory

	// Create a table builder
	builder := &SSTBuilder{
		Path:  path,
		Level: l.meta.Level + 1,
	}
	if err := builder.SetUp(); err != nil {
		return nil, nil, err
	}

	// Create iterators for each table
	itrs := make([]*sstIterator, len(l.tables))
	for i, t := range l.tables {
		itrs[i] = &sstIterator{
			table: t,
		}
		itrs[i].start()
	}

	// Create a function to check if all iterators are done
	allDone := func(itrs []*sstIterator) bool {
		for _, itr := range itrs {
			if !itr.done {
				return false
			}
		}
		return true
	}

	// Merge the tables
	//
	// Note: Track the last key so we can skip
	// duplicates by timestamp
	var lastk string

	// Loop until all iterators are done
	for !allDone(itrs) {
		// Pick the next record from the iterator
		// - Pick the lowest key
		// - If the key is equal, the oldest table overwrites (and others are skipped)

		// Start with the first one
		besti := -1
		var bestr Record
		var bestt time.Time

		// Loop through...
		for i, itr := range itrs {
			// Is this iterator done?
			if itr.done {
				continue
			}

			// If it's the first one, use it
			if besti == -1 {
				besti = i
				bestr = itr.current
				bestt = l.tables[i].meta.CreatedAt
				continue
			}

			// Is this one lower?
			if itr.current.Key < bestr.Key {
				bestr = itr.current
				bestt = l.tables[i].meta.CreatedAt
				besti = i
				continue
			}

			// Is this key equal?
			//
			// Then the most recent table overwrites the others
			if itr.current.Key == bestr.Key && l.tables[i].meta.CreatedAt.After(bestt) {
				bestr = itr.current
				bestt = l.tables[i].meta.CreatedAt
				besti = i
				continue
			}
		}

		// Have we already seen this key?
		if lastk == bestr.Key {
			continue
		}

		// Add the record to the builder
		if err := builder.Add(bestr); err != nil {
			return nil, nil, err
		}

		// Advance the iterator
		itrs[besti].next()

		// Store the last key that was added
		lastk = bestr.Key
	}

	// Build the new table
	t, err := builder.Finish()
	if err != nil {
		return nil, nil, err
	}

	// Get a list of the ids of the tables that
	// were compacted (for cleanup)
	ids := make([]string, len(l.tables))
	for i, t := range l.tables {
		ids[i] = t.meta.ID
	}

	// Done!
	return t, ids, nil
}

func (l *Level) DeleteTables(ids []string) error {
	l.Lock()
	defer l.Unlock()

	// Create a new table list
	tablesToKeep := make([]*SSTable, 0, len(l.tables))
	tablesToDelete := make([]*SSTable, 0, len(ids))
	for _, t := range l.tables {
		if slices.Contains(ids, t.meta.ID) {
			tablesToDelete = append(tablesToDelete, t)
			continue
		}
		tablesToKeep = append(tablesToKeep, t)
	}

	// Delete the tables
	for _, t := range tablesToDelete {
		if err := t.DeleteTable(); err != nil {
			return err
		}
	}

	// Update the table handles
	l.tables = tablesToKeep

	// Update the metadata
	if err := l.updateMetadata(); err != nil {
		return err
	}

	// Done
	return nil
}

func (l *Level) updateMetadata() error {
	// Get the latest key range
	var minKey, maxKey string
	for _, t := range l.tables {
		if minKey == "" || t.meta.MinKey < minKey {
			minKey = t.meta.MinKey
		}
		if maxKey == "" || t.meta.MaxKey > maxKey {
			maxKey = t.meta.MaxKey
		}
	}

	// Get the latest table key ids
	tableIDs := make([]string, len(l.tables))
	for i, t := range l.tables {
		tableIDs[i] = t.meta.ID
	}

	// Store the new metadata
	l.meta.MinKey = minKey
	l.meta.MaxKey = maxKey
	l.meta.Tables = tableIDs

	// Marshal the metadata
	b, err := json.Marshal(l.meta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Write the metadata to the file
	p := path.Join(l.path, "_meta.json")
	if err := os.WriteFile(p, b, 0644); err != nil {
		return fmt.Errorf("failed to write metadata to file: %w", err)
	}
	return nil
}

type LevelMeta struct {
	Level   uint16   `json:"level"`   // Level number (starts with 1; 0 is memtable)
	MaxSize uint16   `json:"maxSize"` // Max num of tables in this level
	MinKey  string   `json:"minKey"`  // Minimum key in this level
	MaxKey  string   `json:"maxKey"`  // Maximum key in this level
	Tables  []string `json:"tables"`  // IDs of tables in this level
}
