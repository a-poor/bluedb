package storage

import "fmt"

type Level struct {
	meta   LevelMeta
	tables []*SSTable
}

func (t *Level) Get(key string) (*Record, error) {
	return nil, fmt.Errorf("not implemented")
}

func (t *Level) Range() error {
	return fmt.Errorf("not implemented")
}

type LevelMeta struct {
	MinKey []byte
	MaxKey []byte
}
