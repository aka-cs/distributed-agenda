package chord

import (
	"DistributedTable/chord"
	"errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"sync"
	"sync/atomic"
	"time"
)

// Transport enables a node to interact with other nodes in the ring, as a client of its servers.
type Transport interface {
	// Start the GRPC server.
	Start()
	// Stop the GRPC server.
	Stop()

	// Chord methods.

	// GetPredecessor returns the node believed to be the current predecessor of a remote Node.
	GetPredecessor(*chord.Node) (*chord.Node, error)
	// GetSuccessor returns the node believed to be the current successor of a remote Node.
	GetSuccessor(*chord.Node) (*chord.Node, error)
	// FindSuccessor finds the node that succeeds this ID, starting at a remote Node.
	FindSuccessor(*chord.Node, []byte) (*chord.Node, error)

	// TODO: Implement the methods above.

	// Notify(*chord.Node, *chord.Node) error
	// CheckPredecessor(*chord.Node) error
	// SetPredecessor(*chord.Node, *chord.Node) error
	// SetSuccessor(*chord.Node, *chord.Node) error
}

// Necessary definitions.
var (
	nullNode     = &chord.Node{}
	emptyRequest = &chord.EmptyRequest{}
)

// RemoteNode struct stores the properties of a connection with a remote chord Node.
type RemoteNode struct {
	chord.ChordClient // Chord client connected with the remote Node server.

	addr       string           // Address of the remote Node.
	conn       *grpc.ClientConn // Grpc connection with the remote Node server.
	lastActive time.Time        // Last time the connection was used.
}

// Close the connection in RemoteNode.
func (connection *RemoteNode) Close() {
	err := connection.conn.Close()
	if err != nil {
		return
	}
}

// NodeTransport implements the Transport interface, for Chord services.
type NodeTransport struct {
	*Configuration // Transport service configurations.

	connections    map[string]*RemoteNode // Dictionary of <address, open connection>.
	connectionsMtx sync.RWMutex           // Locks the dictionary for reading or writing.

	running int32 // Determine if the transport service is actually running.
}

// NewNodeTransport creates a new NodeTransport object.
func NewNodeTransport(config *Configuration) *NodeTransport {
	// Create the NodeTransport object.
	transport := &NodeTransport{
		Configuration: config,
		connections:   nil,
		running:       0,
	}

	// Return the NodeTransport object.
	return transport
}

// Connect with a remote address.
func (transport *NodeTransport) Connect(addr string) (*RemoteNode, error) {
	// Check if the transport service is shutdown, and if condition holds return and report it.
	if atomic.LoadInt32(&transport.running) == 0 {
		return nil, errors.New("must start transport service first")
	}

	transport.connectionsMtx.RLock() // Lock the dictionary to read it, and unlock it before.
	// Checks if the dictionary is instantiated. If condition not holds return the error.
	if transport.connections == nil {
		transport.connectionsMtx.Unlock()
		return nil, errors.New("must start transport service first")
	}
	remoteNode, ok := transport.connections[addr]
	transport.connectionsMtx.RUnlock()

	// Check if the connection is already alive, and if condition holds return the connection.
	if ok {
		return remoteNode, nil
	}

	conn, err := grpc.Dial(addr, transport.DialOpts...) // Establish the connection.

	// Check if the connection was successfully. If condition not holds return the error.
	if err != nil {
		return nil, err
	}

	client := chord.NewChordClient(conn) // Create the ChordClient associated with the connection.
	remoteNode = &RemoteNode{client,
		addr,
		conn,
		time.Now()} // Wrap the ChordClient on a RemoteNode struct.

	transport.connectionsMtx.Lock() // Lock the dictionary to write on it, and unlock it before.
	transport.connections[addr] = remoteNode
	transport.connectionsMtx.Unlock()

	// Return the correspondent RemoteNode.
	return remoteNode, nil
}

// CloseOldConnections close the old open connections.
func (transport *NodeTransport) CloseOldConnections() {
	ticker := time.NewTicker(60 * time.Second) // Set the time between routine activations.

	for {
		select {
		case <-ticker.C:
			// If the transport service is shutdown, do nothing.
			if atomic.LoadInt32(&transport.running) == 0 {
				return
			}
			transport.connectionsMtx.Lock() // Lock the dictionary to write on it.
			// For RemoteNode on the dictionary, if its lifetime is over, close the connection.
			for addr, connection := range transport.connections {
				if time.Since(connection.lastActive) > transport.MaxIdle {
					connection.Close()
					delete(transport.connections, addr) // Delete the <address, connection> pair of the dictionary.
				}
			}
			transport.connectionsMtx.Unlock() // After finishing write, unlock the dictionary.
		}
	}
}

// Start the transport service.
func (transport *NodeTransport) Start() {
	transport.connections = make(map[string]*RemoteNode) // Create the dictionary of <address, open connection>.
	atomic.StoreInt32(&transport.running, 1)             // Report the service is running.
	go transport.CloseOldConnections()                   // Check and close old connections periodically.
}

// Stop the transport service.
func (transport *NodeTransport) Stop() {
	atomic.StoreInt32(&transport.running, 0) // Report the service is shutdown.

	// Close all the connections
	transport.connectionsMtx.Lock() // Lock the dictionary to write on it.
	// For RemoteNode on the dictionary, if its lifetime is over, close the connection.
	for _, connection := range transport.connections {
		if time.Since(connection.lastActive) > transport.MaxIdle {
			connection.Close()
		}
	}
	transport.connections = nil       // Delete dictionary of connections.
	transport.connectionsMtx.Unlock() // After finishing write, unlock the dictionary.
}

// GetPredecessor returns the node believed to be the current predecessor of a remote Node.
func (transport *NodeTransport) GetPredecessor(node *chord.Node) (*chord.Node, error) {
	remoteNode, err := transport.Connect(node.Addr) // Establish connection with the remote node.
	// In case of error, return the error.
	if err != nil {
		return nil, err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), transport.Timeout)
	defer cancel()

	// Return the result of the remote call.
	return remoteNode.GetPredecessor(ctx, emptyRequest)
}

// GetSuccessor returns the node believed to be the current successor of a remote Node.
func (transport *NodeTransport) GetSuccessor(node *chord.Node) (*chord.Node, error) {
	remoteNode, err := transport.Connect(node.Addr) // Establish connection with the remote node.
	// In case of error, return the error.
	if err != nil {
		return nil, err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), transport.Timeout)
	defer cancel()

	// Return the result of the remote call.
	return remoteNode.GetSuccessor(ctx, emptyRequest)
}

// FindSuccessor finds the node that succeeds this ID, starting at a remote Node.
func (transport *NodeTransport) FindSuccessor(node *chord.Node, ID []byte) (*chord.Node, error) {
	remoteNode, err := transport.Connect(node.Addr) // Establish connection with the remote node.
	// In case of error, return the error.
	if err != nil {
		return nil, err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), transport.Timeout)
	defer cancel()

	// Return the result of the remote call.
	return remoteNode.FindSuccessor(ctx, &chord.ID{ID: ID})
}
