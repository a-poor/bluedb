package storage

type BlockReadWriter interface{}

type MemBlockReadWriter struct{}

type FileBlockReadWriter struct{}

type CompressedBlockReadWriter struct{}

type EncryptedBlockReadWriter struct{}
