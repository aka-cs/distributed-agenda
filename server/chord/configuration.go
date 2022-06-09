package chord

import (
	"crypto/sha1"
	"google.golang.org/grpc"
	"hash"
	"time"
)

// Configuration of the Node.
type Configuration struct {
	Hash     func() hash.Hash // Hash function to use
	HashSize int              // Hash size supported

	ServerOpts []grpc.ServerOption
	DialOpts   []grpc.DialOption

	Timeout time.Duration // Timeout of the connection.
	MaxIdle time.Duration // Max lifetime of a connection.
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
