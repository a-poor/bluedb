package storage

import (
	"fmt"
	"sync"

	"github.com/google/btree"
)

const (
	DefaultTreeOrder    = 2
	DefaultMaxTableSize = 1 << 10
)

type Memtable struct {
	sync.RWMutex
	tree    *btree.BTreeG[string]
	hmap    map[string]Record
	wal     *WAL
	maxSize uint64
}

func NewMemtable() *Memtable {
	tree := btree.NewOrderedG[string](DefaultTreeOrder)
	hmap := make(map[string]Record)
	return &Memtable{
		tree:    tree,
		hmap:    hmap,
		wal:     nil,
		maxSize: DefaultMaxTableSize,
	}
}

func (m *Memtable) Get(k string) (*Record, error) {
	m.RLock()
	defer m.RUnlock()

	// Get the record
	r, ok := m.hmap[k]
	if !ok {
		return nil, nil
	}
	return &r, nil
}

func (m *Memtable) Put(r Record) error {
	m.Lock()
	defer m.Unlock()

	// Set the record in the hash-map
	m.hmap[r.Key] = r

	// Add the record to the tree
	m.tree.ReplaceOrInsert(r.Key)

	// Done
	return nil
}

func (m *Memtable) Del(k string) error {
	return m.Put(Record{
		Key:  k,
		Tomb: true,
	})
}

func (m *Memtable) Full() bool {
	return len(m.hmap) >= int(m.maxSize)
}

func (m *Memtable) Flush() error {
	return fmt.Errorf("not implemented")
}
