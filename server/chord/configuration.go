package chord

import (
	"crypto/sha1"
	"google.golang.org/grpc"
	"hash"
	"time"
)

// Configuration of Node.
type Configuration struct {
	Hash     func() hash.Hash // Hash function to use.
	HashSize int              // Hash size supported.

	ServerOpts []grpc.ServerOption // Server options.
	DialOpts   []grpc.DialOption   // Connection options.

	Timeout time.Duration // Timeout of the connection.
	MaxIdle time.Duration // Max lifetime of a connection.

	StabilizingNodes int // Max number of successors to register in a node, to ensure stabilization.
}

// DefaultConfig returns a default configuration.
func DefaultConfig() *Configuration {
	Hash := sha1.New              // Use sha1 as the hash function.
	HashSize := Hash().Size() * 8 // Save the hash size supported.
	DialOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTimeout(5 * time.Second),
		grpc.FailOnNonTempDialError(true),
		grpc.WithInsecure(),
	}

	// Build the configuration.
	config := &Configuration{
		Hash:             sha1.New,
		HashSize:         HashSize,
		DialOpts:         DialOpts,
		Timeout:          1 * time.Second,
		MaxIdle:          5 * time.Second,
		StabilizingNodes: 5,
	}

	return config
}
