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
	Clear() error
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

func (dictionary *Dictionary) Set(key string, value []byte) error {
	dictionary.data[key] = value
	return nil
}

func (dictionary *Dictionary) Delete(key string) error {
	delete(dictionary.data, key)
	return nil
}

func (dictionary *Dictionary) Partition(L, R []byte) (map[string][]byte, map[string][]byte, error) {
	in := make(map[string][]byte)
	out := make(map[string][]byte)

	if Equals(L, R) {
		return dictionary.data, out, nil
	}

	for key := range dictionary.data {
		value, err := dictionary.Get(key)
		if err != nil {
			return nil, nil, err
		}

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
		err := dictionary.Set(key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dictionary *Dictionary) Discard(data []string) error {
	for _, key := range data {
		err := dictionary.Delete(key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (dictionary *Dictionary) Clear() error {
	return dictionary.Discard(Keys(dictionary.data))
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
