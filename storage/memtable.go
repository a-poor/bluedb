package storage

import (
	"fmt"
	"sync"

	"github.com/google/btree"
)

const (
	DefaultTreeOrder    = 3
	DefaultMaxTableSize = 1 << 10
)

type Memtable struct {
	sync.RWMutex
	tree    *btree.BTreeG[string]
	hmap    map[string]Record
	wal     *WAL
	maxSize uint64
	frozen  bool
}

func NewMemtable() *Memtable {
	tree := btree.NewOrderedG[string](DefaultTreeOrder)
	hmap := make(map[string]Record)
	return &Memtable{
		tree:    tree,
		hmap:    hmap,
		wal:     nil,
		maxSize: DefaultMaxTableSize,
		frozen:  false,
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

	if m.frozen {
		return fmt.Errorf("memtable is frozen")
	}

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

func (m *Memtable) Freeze() {
	m.Lock()
	defer m.Unlock()
	m.frozen = true
}

func (m *Memtable) Compact(p string, n int) (*SSTable, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *Memtable) Close() error {
	m.Lock()
	defer m.Unlock()
	return fmt.Errorf("not implemented")
}
