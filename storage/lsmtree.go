package storage

import (
	"fmt"
)

type LSMTree struct {
	memtable *Memtable
	wal      *WAL
	levels   []*Level
}

func NewLSMTree() *LSMTree {
	return &LSMTree{}
}

func (t *LSMTree) Get(key []byte) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}

func (t *LSMTree) Range() error {
	return fmt.Errorf("not implemented")
}

func (t *LSMTree) Put(key, value []byte) error {
	return fmt.Errorf("not implemented")
}

func (t *LSMTree) Del(key []byte) error {
	return fmt.Errorf("not implemented")
}

type LSMTreeMeta struct {
}
