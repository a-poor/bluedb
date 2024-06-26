package storage

import (
	"fmt"

	"github.com/google/btree"
)

const DefaultTreeOrder = 2

type Memtable struct {
	Tree *btree.BTreeG[Record]
}

func NewMemtable() *Memtable {
	tree := btree.NewG(DefaultTreeOrder, recordLessFunc)
	return &Memtable{
		Tree: tree,
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
