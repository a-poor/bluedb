package storage

import (
	"crypto/cipher"
	"fmt"
	"io"

	_ "github.com/bits-and-blooms/bloom/v3"
	_ "github.com/golang/snappy"
)

type BlockRW struct {
	Compress      bool
	Encrypt       bool
	EncryptionKey []byte
}

type BlockRWConf struct{}

func NewBlockRW() (*BlockRW, error) {
	return nil, fmt.Errorf("not implemented")
}

type MemBlockRW struct{}

type FileBlockRW struct{}

type CompressedBlockRW struct{}

type EncryptedReader struct {
	C cipher.Block
	R io.Reader
}
