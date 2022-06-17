package chord

import (
	"hash"
)

type Storage interface {
	Get(string) ([]byte, error)
	Set(string, string) error
	Delete(string) error
	Segment([]byte, []byte) map[string]string
	Extend(map[string]string) error
	Detach([]byte, []byte) error
}

type DistributedStorage struct {
	data map[string]string
	Hash func() hash.Hash // Hash function to use
}

func NewMapStore(hash func() hash.Hash) *DistributedStorage {
	return &DistributedStorage{
		data: make(map[string]string),
		Hash: hash,
	}
}
