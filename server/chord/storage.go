package chord

import (
	"errors"
	"hash"
)

type Storage interface {
	Get(string) ([]byte, error)
	Set(string, []byte)
	Delete(string)
	Segment([]byte, []byte) (map[string][]byte, error)
	Extend(map[string][]byte) error
	Discard([]byte, []byte) error
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

func (dictionary *Dictionary) Segment(L, R []byte) (map[string][]byte, error) {
	if L == nil && R == nil {
		return dictionary.data, nil
	}

	data := make(map[string][]byte)
	keyIDs := make(map[string][]byte, 0)

	for key, _ := range dictionary.data {
		keyID, err := HashKey(key, dictionary.Hash)
		if err != nil {
			return nil, err
		}
		keyIDs[key] = keyID
	}

	for key, keyID := range keyIDs {
		if Between(keyID, L, R, false, true) {
			data[key] = dictionary.data[key]
		}
	}
	return data, nil
}

func (dictionary *Dictionary) Extend(data map[string][]byte) error {
	for key, value := range data {
		dictionary.data[key] = value
	}
	return nil
}

func (dictionary *Dictionary) Discard(L, R []byte) error {
	if L == nil && R == nil {
		dictionary.data = make(map[string][]byte)
	}

	keyIDs := make(map[string][]byte, 0)

	for key, _ := range dictionary.data {
		keyID, err := HashKey(key, dictionary.Hash)
		if err != nil {
			return err
		}
		keyIDs[key] = keyID
	}

	for key, keyID := range keyIDs {
		if Between(keyID, L, R, false, true) {
			delete(dictionary.data, key)
		}
	}
	return nil
}
