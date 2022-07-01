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
	data map[string][]byte // Internal dictionary
	Hash func() hash.Hash  // Hash function to use
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
		return nil, errors.New("Key not found.\n")
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
	in := make(map[string][]byte)
	out := make(map[string][]byte)

	if Equals(L, R) {
		return dictionary.data, out, nil
	}

	for key, value := range dictionary.data {
		if between, err := KeyBetween(key, dictionary.Hash, L, R); between && err == nil {
			in[key] = value
		} else if !between && err == nil {
			out[key] = value
		} else {
			return nil, nil, err
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
