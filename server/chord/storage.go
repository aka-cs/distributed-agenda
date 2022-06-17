package chord

import (
	"hash"
)

type Storage interface {
	Get(string) ([]byte, error)
	Set(string, string) error
	Delete(string) error
	Extend([]string, []string) error
	DataBetween([]byte, []byte) ([]string, []string)
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
