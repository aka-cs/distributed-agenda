package chord

import (
	"crypto/sha1"
	"google.golang.org/grpc"
	"hash"
	"time"
)

// Configuration of Node.
type Configuration struct {
	Hash     func() hash.Hash // Hash function to use
	HashSize int              // Hash size supported

	ServerOpts []grpc.ServerOption
	DialOpts   []grpc.DialOption

	Timeout time.Duration // Timeout of the connection.
	MaxIdle time.Duration // Max lifetime of a connection.

	StabilizingNodes int // Max number of successors to register in a node, to ensure stabilization.
}

// DefaultConfig returns a default configuration.
func DefaultConfig() *Configuration {
	Hash := sha1.New
	HashSize := Hash().Size()
	DialOpts := []grpc.DialOption{grpc.WithInsecure()}

	config := &Configuration{
		Hash:             sha1.New,
		HashSize:         HashSize,
		DialOpts:         DialOpts,
		StabilizingNodes: 5,
	}

	config.HashSize = config.Hash().Size()

	return config
}
