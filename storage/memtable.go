package storage

import (
	"fmt"

	"github.com/google/btree"
)

const (
	DefaultTreeOrder    = 2
	DefaultMaxTableSize = 1 << 10
)

type Memtable struct {
	tree    *btree.BTreeG[Record]
	wal     *WAL
	maxSize uint64
}

func NewMemtable() *Memtable {
	tree := btree.NewG(DefaultTreeOrder, recordLessFunc)
	return &Memtable{
		tree:    tree,
		wal:     nil,
		maxSize: DefaultMaxTableSize,
	}
}

func (m *Memtable) Get(key []byte) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *Memtable) Range() error {
	return fmt.Errorf("not implemented")
}

func (m *Memtable) Put(key, value []byte) error {
	return fmt.Errorf("not implemented")
}

func (m *Memtable) Del(key []byte) error {
	return fmt.Errorf("not implemented")
}

func (m *Memtable) Full() bool {
	return m.tree.Len() >= int(m.maxSize)
}

func (m *Memtable) Flush() error {
	return fmt.Errorf("not implemented")
}
