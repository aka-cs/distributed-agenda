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
	Detach([]byte, []byte) error
}

type Dictionary struct {
	data map[string]string
	Hash func() hash.Hash // Hash function to use
}

func NewDictionary(hash func() hash.Hash) *Dictionary {
	return &Dictionary{
		data: make(map[string]string),
		Hash: hash,
	}
}

func (dictionary *Dictionary) Get(key string) (string, error) {
	value, ok := dictionary.data[key]
	if !ok {
		return "", errors.New("invalid key")
	}
	return value, nil
}

func (dictionary *Dictionary) Set(key string, value string) {
	dictionary.data[key] = value
}

func (dictionary *Dictionary) Delete(key string) {
	delete(dictionary.data, key)
}

func (dictionary *Dictionary) Segment(L, R []byte) (map[string]string, error) {
	if L == nil && R == nil {
		return dictionary.data, nil
	}

	data := make(map[string]string)
	keyIDs := make(map[string][]byte, 0)

	for key, _ := range dictionary.data {
		keyID, err := HashKey(key, dictionary.Hash)
		if err != nil {
			return nil, err
		}
		keyIDs[key] = keyID
	}

	for key, keyID := range keyIDs {
		if Between(keyID, L, R, false, false) {
			data[key] = dictionary.data[key]
		}
	}
	return data, nil
}

func (dictionary *Dictionary) Detach(L, R []byte) error {
	if L == nil && R == nil {
		dictionary.data = make(map[string]string)
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
		if Between(keyID, L, R, false, false) {
			delete(dictionary.data, key)
		}
	}
	return nil
}
