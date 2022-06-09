package chord

import (
	"DistributedTable/chord"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"sync"
	"sync/atomic"
	"time"
)

// Transport enables a node to interact as a client with other nodes in the ring.
type Transport interface {
	// Start the GRPC server.
	Start() error
	// Stop the GRPC server.
	Stop() error

	// Chord methods.

	// GetPredecessor returns the node believed to be the current predecessor of a remote Node.
	GetPredecessor(*chord.Node) (*chord.Node, error)
	// GetSuccessor returns the node believed to be the current successor of a remote Node.
	GetSuccessor(*chord.Node) (*chord.Node, error)
	// FindSuccessor finds the node that succeeds this ID, starting at a remote Node.
	FindSuccessor(*chord.Node, []byte) (*chord.Node, error)

	// TODO: Implement the methods above.

	Notify(*chord.Node, *chord.Node) error
	CheckPredecessor(*chord.Node) error
	SetPredecessor(*chord.Node, *chord.Node) error
	SetSuccessor(*chord.Node, *chord.Node) error
}

// Necessary definitions.
var (
	nullNode = &chord.Node{}
	//emptyRequest = &chord.EmptyRequest{}
)

// NodeConnection struct stores the properties of a connection with a remote chord Node.
type NodeConnection struct {
	addr       string             // Address of the remote Node.
	client     *chord.ChordClient // Chord client connected with the remote Node server.
	conn       *grpc.ClientConn   // Grpc connection with the remote Node server.
	lastActive time.Time          // Last time the connection was used.
}

// NodeTransport implements the Transport interface, for Chord services.
type NodeTransport struct {
	*Configuration // Transport service configurations.

	connections    map[string]*NodeConnection // Dictionary of <address, open connection>.
	connectionsMtx sync.RWMutex               // Locks the dictionary for reading or writing if another routine is doing it.

	shutdown int32 // Determine if the transport service is actually running.
}

// NewNodeTransport creates a new NodeTransport object.
func NewNodeTransport(config *Configuration) (*NodeTransport, error) {
	// Create the dictionary of <address, open connection>.
	connections := make(map[string]*NodeConnection)

	// Create the NodeTransport object.
	transport := &NodeTransport{
		Configuration: config,
		connections:   connections,
		shutdown:      0,
	}

	// Return the NodeTransport object.
	return transport, nil
}

// Connect with a remote address.
func (transport *NodeTransport) Connect(addr string) (*NodeConnection, error) {
	// Check if the transport service is shutdown, and if condition holds return and report it.
	if atomic.LoadInt32(&transport.shutdown) == 1 {
		return nil, fmt.Errorf("TCP transport is shutdown")
	}

	transport.connectionsMtx.RLock() // Lock the dictionary to read it, and unlock it before.
	// Checks if the dictionary is instantiated. If condition not holds return the error.
	if transport.connections == nil {
		transport.connectionsMtx.Unlock()
		return nil, errors.New("must instantiate node before using")
	}
	nodeConnection, ok := transport.connections[addr]
	transport.connectionsMtx.RUnlock()

	// Check if the connection is already alive, and if condition holds return the connection.
	if ok {
		return nodeConnection, nil
	}

	conn, err := grpc.Dial(addr, transport.DialOpts...) // Establish the connection.

	// Check if the connection was successfully. If condition not holds return the error.
	if err != nil {
		return nil, err
	}

	client := chord.NewChordClient(conn) // Create the ChordClient associated with the connection.
	nodeConnection = &NodeConnection{addr,
		&client,
		conn,
		time.Now()} // Wrap the ChordClient on a NodeConnection struct.

	transport.connectionsMtx.Lock() // Lock the dictionary to write on it, and unlock it before.
	transport.connections[addr] = nodeConnection
	transport.connectionsMtx.Unlock()

	// Return the correspondent NodeConnection.
	return nodeConnection, nil
}
