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
	Get(string, string) ([]byte, error)
	Set(string, []byte, string) error
	Delete(string, string) error

	Partition([]byte, []byte) (map[string][]byte, map[string][]byte, error)
	Extend(map[string][]byte) error
	Discard([]string) error
	Clear() error

	Lock(string, string) error
	Unlock(string, string) error
}

// Weak storage.

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

func (dictionary *Dictionary) Get(key, permission string) ([]byte, error) {
	lock, ok := dictionary.lock[key]

	if !ok || permission == "" || permission == lock {
		value, ok := dictionary.data[key]

		if !ok {
			return nil, errors.New("Key not found.\n")
		}

		return value, nil
	}

	return nil, os.ErrPermission
}

func (dictionary *Dictionary) Set(key string, value []byte, permission string) error {
	lock, ok := dictionary.lock[key]

	if !ok || permission == "" || permission == lock {
		dictionary.data[key] = value
		return nil
	}

	return os.ErrPermission
}

func (dictionary *Dictionary) Delete(key string, permission string) error {
	lock, ok := dictionary.lock[key]

	if !ok || permission == "" || permission == lock {
		delete(dictionary.data, key)
		return nil
	}

	return os.ErrPermission
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
		} else if err == nil {
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

func (dictionary *Dictionary) Lock(key, permission string) error {
	lock, ok := dictionary.lock[key]

	if ok && permission != lock {
		return os.ErrPermission
	}

	dictionary.lock[key] = permission

	return nil
}

func (dictionary *Dictionary) Unlock(key, permission string) error {
	lock, ok := dictionary.lock[key]

	if ok && permission != lock {
		return os.ErrPermission
	}

	delete(dictionary.lock, key)
	return nil
}

// Hard storage.

type void struct{}

type DiskDictionary struct {
	data map[string]void   // Internal dictionary
	lock map[string]string // Lock states
	Hash func() hash.Hash  // Hash function to use
}

func NewDiskDictionary(hash func() hash.Hash) *DiskDictionary {
	return &DiskDictionary{
		data: make(map[string]void),
		lock: make(map[string]string),
		Hash: hash,
	}
}

func (dictionary *DiskDictionary) Get(key, permission string) ([]byte, error) {
	lock, ok := dictionary.lock[key]

	if !ok || permission == "" || permission == lock {
		log.Debugf("Loading file: %s\n", key)

		value, err := ioutil.ReadFile(key)
		if err != nil {
			log.Errorf("Error loading file %s:\n%v\n", key, err)
			return nil, status.Error(codes.Internal, "Couldn't load file")
		}

		return value, nil
	}

	return nil, os.ErrPermission
}

func (dictionary *DiskDictionary) Set(key string, value []byte, permission string) error {
	lock, ok := dictionary.lock[key]

	if !ok || permission == "" || permission == lock {
		dictionary.data[key] = void{}

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

	return os.ErrPermission
}

func (dictionary *DiskDictionary) Delete(key, permission string) error {
	lock, ok := dictionary.lock[key]

	if !ok || permission == "" || permission == lock {
		err := os.Remove(key)
		delete(dictionary.data, key)

		if err != nil {
			log.Errorf("Error deleting file:\n%v\n", err)
			return status.Error(codes.Internal, "Couldn't delete file")
		}
		return nil
	}

	return os.ErrPermission
}

func (dictionary *DiskDictionary) Partition(L, R []byte) (map[string][]byte, map[string][]byte, error) {
	in := make(map[string][]byte)
	out := make(map[string][]byte)
	all := false

	if Equals(L, R) {
		all = true
	}

	for key := range dictionary.data {
		value, err := dictionary.Get(key, "")
		if err != nil {
			return nil, nil, err
		}

		if between, err := KeyBetween(key, dictionary.Hash, L, R); (all || between) && err == nil {
			in[key] = value
		} else if err == nil {
			out[key] = value
		} else {
			return nil, nil, err
		}
	}

	return in, out, nil
}

func (dictionary *DiskDictionary) Extend(data map[string][]byte) error {
	for key, value := range data {
		err := dictionary.Set(key, value, "")
		if err != nil {
			return err
		}
	}
	return nil
}

func (dictionary *DiskDictionary) Discard(data []string) error {
	for _, key := range data {
		err := dictionary.Delete(key, "")
		if err != nil {
			return err
		}
	}

	return nil
}

func (dictionary *DiskDictionary) Clear() error {
	keys := Keys(dictionary.data)

	for _, key := range keys {
		err := dictionary.Delete(key, "")
		if err != nil {
			return err
		}
	}

	return nil
}

func (dictionary *DiskDictionary) Lock(key, permission string) error {
	lock, ok := dictionary.lock[key]

	if ok && permission != lock {
		return os.ErrPermission
	}

	dictionary.lock[key] = permission

	return nil
}

func (dictionary *DiskDictionary) Unlock(key, permission string) error {
	lock, ok := dictionary.lock[key]

	if ok && permission != lock {
		return os.ErrPermission
	}

	delete(dictionary.lock, key)
	return nil
}
