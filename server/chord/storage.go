package chord

import (
	"errors"
	"hash"
)

type Storage interface {
	Get(string) ([]byte, error)
	Set(string, []byte)
	Delete(string)
	Partition([]byte, []byte) (map[string][]byte, map[string][]byte, error)
	Extend(map[string][]byte) error
	Discard(data []string) error
}

type Dictionary struct {
	data map[string][]byte
	Hash func() hash.Hash // Hash function to use
}

func NewDictionary(hash func() hash.Hash) *Dictionary {
	return &Dictionary{
		data: make(map[string][]byte),
		Hash: hash,
	}
}

func (dictionary *Dictionary) Get(key string) ([]byte, error) {
	value, ok := dictionary.data[key]
	if !ok {
		return nil, errors.New("invalid key")
	}
	return value, nil
}

func (dictionary *Dictionary) Set(key string, value []byte) {
	dictionary.data[key] = value
}

func (dictionary *Dictionary) Delete(key string) {
	delete(dictionary.data, key)
}

func (dictionary *Dictionary) Partition(L, R []byte) (map[string][]byte, map[string][]byte, error) {
	if L == nil && R == nil {
		return dictionary.data, nil, nil
	}

	in := make(map[string][]byte)
	out := make(map[string][]byte)

	for key, value := range dictionary.data {
		keyID, err := HashKey(key, dictionary.Hash)
		if err != nil {
			return nil, nil, err
		}

		if Between(keyID, L, R, false, true) {
			in[key] = value
		} else {
			out[key] = value
		}
	}

	return in, out, nil
}

func (dictionary *Dictionary) Extend(data map[string][]byte) error {
	for key, value := range data {
		dictionary.data[key] = value
	}
	return nil
}

func (dictionary *Dictionary) Discard(data []string) error {
	for _, key := range data {
		delete(dictionary.data, key)
	}

	return nil
}
