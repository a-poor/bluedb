package storage

import (
	"fmt"
	"path"
	"slices"
	"sync"
	"time"
)

type Level struct {
	sync.RWMutex
	path   string
	meta   LevelMeta
	tables []*SSTable
}

func CreateLevel() (*Level, error) {
	return nil, fmt.Errorf("not implemented")
}

func LoadLevel() (*Level, error) {
	return nil, fmt.Errorf("not implemented")
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

func (l *Level) Compact() (*SSTable, []string, error) {
	l.Lock()
	defer l.Unlock()

	// Make sure there are tables to compact
	if len(l.tables) == 0 {
		return nil, nil, fmt.Errorf("no tables to compact")
	}

	// Get the table dir
	dir := fmtSSTablePath(l.path, l.meta.Level+1)

	// Create a table builder
	builder := &SSTBuilder{
		Path:  dir,
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

			// Is this one equal?
			if itr.current.Key == bestr.Key && l.tables[i].meta.CreatedAt.Before(bestt) {
				bestr = itr.current
				bestt = l.tables[i].meta.CreatedAt
				besti = i
				continue
			}
		}

		// Add the record to the builder
		if err := builder.Add(bestr); err != nil {
			return nil, nil, err
		}

		// Advance the iterator
		itrs[besti].next()

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

	// Done
	l.tables = tablesToKeep
	return nil
}

type LevelMeta struct {
	Level  uint8
	MinKey string
	MaxKey string
}

func fmtSSTablePath(levelPath string, level uint8) string {
	n := fmt.Sprintf("%04d", level)
	return path.Join(levelPath, n)
}
