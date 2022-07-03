package chord

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"hash"
	"io/ioutil"
	"os"
	"path/filepath"
)

type Storage interface {
	Get(string) ([]byte, error)
	Set(string, []byte) error
	Delete(string) error

	Partition([]byte, []byte) (map[string][]byte, map[string][]byte, error)
	Extend(map[string][]byte) error
	Discard(data []string) error

	All() map[string][]byte
	Keys() []string
	Clear() error

	IsLocked(string) bool
	Lock(string, string) error
	Unlock(string, string) error
}

type Dictionary struct {
	data map[string][]byte // Internal dictionary
	lock map[string]string // Lock states
	Hash func() hash.Hash  // Hash function to use
}

func NewDictionary(hash func() hash.Hash) *Dictionary {
	return &Dictionary{
		data: make(map[string][]byte),
		lock: make(map[string]string),
		Hash: hash,
	}
}

func (dictionary *Dictionary) Get(key string) ([]byte, error) {
	if dictionary.IsLocked(key) {
		return nil, errors.New("A")
	}

	value, ok := dictionary.data[key]
	if !ok {
		return nil, errors.New("Key not found.\n")
	}
	return value, nil
}

func (dictionary *Dictionary) Set(key string, value []byte) error {
	if dictionary.IsLocked(key) {
		return errors.New("A")
	}

	dictionary.data[key] = value
	return nil
}

func (dictionary *Dictionary) Delete(key string) error {
	if dictionary.IsLocked(key) {
		return errors.New("A")
	}

	delete(dictionary.data, key)
	return nil
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

func (dictionary *Dictionary) Clear() error {
	dictionary.data = make(map[string][]byte)
	return nil
}

func (dictionary *Dictionary) IsLocked(key string) bool {
	_, ok := dictionary.lock[key]
	return ok
}

func (dictionary *Dictionary) Lock(key, permission string) error {
	if value, ok := dictionary.lock[key]; ok && value == permission {
		delete(dictionary.lock, key)
	} else if value != permission {
		return errors.New("A")
	}

	return nil
}

func (dictionary *Dictionary) Unlock(key, permission string) error {
	if value, ok := dictionary.lock[key]; ok && value == permission {
		delete(dictionary.lock, key)
	} else if value != permission {
		return errors.New("A")
	}

	return nil
}

type DiskDictionary struct {
	*Dictionary
}

func NewDiskDictionary(hash func() hash.Hash) *DiskDictionary {
	return &DiskDictionary{Dictionary: NewDictionary(hash)}
}

func (dictionary *DiskDictionary) Get(key string) ([]byte, error) {
	log.Debugf("Loading file: %s\n", key)

	value, err := ioutil.ReadFile(key)
	if err != nil {
		log.Errorf("Error loading file %s:\n%v\n", key, err)
		return nil, status.Error(codes.Internal, "Couldn't load file")
	}

	return value, nil
}

func (dictionary *DiskDictionary) Set(key string, value []byte) error {
	dictionary.data[key] = []byte{}

	log.Debugf("Saving file: %s\n", key)

	dir := filepath.Dir(key)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Errorf("Couldn't create directory %s:\n%v\n", key, err)
		return status.Error(codes.Internal, "Couldn't create directory")
	}

	err = ioutil.WriteFile(key, value, 0600)

	if err != nil {
		log.Errorf("Error creating file %s:\n%v\n", key, err)
		return status.Error(codes.Internal, "Couldn't create file")
	}

	return nil
}

func (dictionary *DiskDictionary) Delete(key string) error {
	err := os.Remove(key)

	if err != nil {
		log.Errorf("Error deleting file:\n%v\n", err)
		return status.Error(codes.Internal, "Couldn't delete file")
	}
	return nil
}
