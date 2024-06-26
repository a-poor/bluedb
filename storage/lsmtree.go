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

func (t *LSMTree) Get(k string) (*Record, error) {
	return nil, fmt.Errorf("not implemented")
}

func (t *LSMTree) Put(r Record) error {
	return fmt.Errorf("not implemented")
}

func (t *LSMTree) Del(k string) error {
	return fmt.Errorf("not implemented")
}

func (t *LSMTree) Compact() error {
	return fmt.Errorf("not implemented")
}

type LSMTreeMeta struct {
}
