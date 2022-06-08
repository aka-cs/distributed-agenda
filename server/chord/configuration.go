package main

import (
	"crypto/sha1"
	"hash"
)

// Configuration of the Node.
type Configuration struct {
	Hash     func() hash.Hash // Hash function to use
	HashSize int              // Hash size supported
}

// DefaultConfig returns a default configuration.
func DefaultConfig() *Configuration {
	config := &Configuration{
		Hash: sha1.New,
	}

	config.HashSize = config.Hash().Size()

	return config
}

func (config *Configuration) hashKey(key string) ([]byte, error) {
	h := config.Hash()
	h.Write([]byte(key))
	value := h.Sum(nil)

	return value, nil
}
