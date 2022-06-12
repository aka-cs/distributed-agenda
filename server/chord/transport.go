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

// RemoteServices enables a node to interact with other nodes in the ring, as a client of its servers.
type RemoteServices interface {
	// Start the services.
	Start()
	// Stop the services.
	Stop()

	// Chord methods.

	// GetPredecessor returns the node believed to be the current predecessor of a remote Node.
	GetPredecessor(*chord.Node) (*chord.Node, error)
	// GetSuccessor returns the node believed to be the current successor of a remote Node.
	GetSuccessor(*chord.Node) (*chord.Node, error)
	// FindSuccessor finds the node that succeeds this ID, starting at a remote Node.
	FindSuccessor(*chord.Node, []byte) (*chord.Node, error)
	// SetPredecessor sets the predecessor of a remote Node.
	SetPredecessor(*chord.Node, *chord.Node) error
	// SetSuccessor sets the successor of a remote Node.
	SetSuccessor(*chord.Node, *chord.Node) error
	// Notify a remote Node that it possibly have a new predecessor.
	Notify(*chord.Node, *chord.Node) error

	// TODO: Implement the methods above.

	// CheckPredecessor(*chord.Node) error
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

// GRPCServices implements the RemoteServices interface, for Chord GRPC services.
type GRPCServices struct {
	*Configuration // Remote service configurations.

	connections    map[string]*RemoteNode // Dictionary of <address, open connection>.
	connectionsMtx sync.RWMutex           // Locks the dictionary for reading or writing.

	running int32 // Determine if the service is actually running.
}

// NewGRPCServices creates a new GRPCServices object.
func NewGRPCServices(config *Configuration) *GRPCServices {
	// Create the GRPCServices object.
	services := &GRPCServices{
		Configuration: config,
		connections:   nil,
		running:       0,
	}

	// Return the GRPCServices object.
	return services
}

// Connect with a remote address.
func (services *GRPCServices) Connect(addr string) (*RemoteNode, error) {
	// Check if the service is shutdown, and if condition holds return and report it.
	if atomic.LoadInt32(&services.running) == 0 {
		return nil, errors.New("must start grpc service first")
	}

	services.connectionsMtx.RLock() // Lock the dictionary to read it, and unlock it after.
	// Checks if the dictionary is instantiated. If condition not holds return the error.
	if services.connections == nil {
		services.connectionsMtx.Unlock()
		return nil, errors.New("must start grpc service first")
	}
	remoteNode, ok := services.connections[addr]
	services.connectionsMtx.RUnlock()

	// Check if the connection is already alive, and if condition holds return the connection.
	if ok {
		return remoteNode, nil
	}

	conn, err := grpc.Dial(addr, services.DialOpts...) // Establish the connection.

	// Check if the connection was successfully. If condition not holds return the error.
	if err != nil {
		return nil, err
	}

	client := chord.NewChordClient(conn) // Create the ChordClient associated with the connection.
	remoteNode = &RemoteNode{client,
		addr,
		conn,
		time.Now()} // Wrap the ChordClient on a RemoteNode struct.

	services.connectionsMtx.Lock() // Lock the dictionary to write on it, and unlock it after.
	services.connections[addr] = remoteNode
	services.connectionsMtx.Unlock()

	// Return the correspondent RemoteNode.
	return remoteNode, nil
}

// CloseOldConnections close the old open connections.
func (services *GRPCServices) CloseOldConnections() {
	ticker := time.NewTicker(60 * time.Second) // Set the time between routine activations.

	for {
		select {
		case <-ticker.C:
			// If the service is shutdown, return.
			if atomic.LoadInt32(&services.running) == 0 {
				return
			}
			services.connectionsMtx.Lock() // Lock the dictionary to write on it.
			// For RemoteNode on the dictionary, if its lifetime is over, close the connection.
			for addr, connection := range services.connections {
				if time.Since(connection.lastActive) > services.MaxIdle {
					connection.Close()
					delete(services.connections, addr) // Delete the <address, connection> pair of the dictionary.
				}
			}
			services.connectionsMtx.Unlock() // After finishing write, unlock the dictionary.
		}
	}
}

// Start the services.
func (services *GRPCServices) Start() {
	services.connections = make(map[string]*RemoteNode) // Create the dictionary of <address, open connection>.
	atomic.StoreInt32(&services.running, 1)             // Report the service is running.
	go services.CloseOldConnections()                   // Check and close old connections periodically.
}

// Stop the services.
func (services *GRPCServices) Stop() {
	atomic.StoreInt32(&services.running, 0) // Report the service is shutdown.

	// Close all the connections
	services.connectionsMtx.Lock() // Lock the dictionary to write on it.
	// For RemoteNode on the dictionary, if its lifetime is over, close the connection.
	for _, connection := range services.connections {
		if time.Since(connection.lastActive) > services.MaxIdle {
			connection.Close()
		}
	}
	services.connections = nil       // Delete dictionary of connections.
	services.connectionsMtx.Unlock() // After finishing write, unlock the dictionary.
}

// Node server remote methods.

// GetPredecessor returns the node believed to be the current predecessor of a remote Node.
func (services *GRPCServices) GetPredecessor(node *chord.Node) (*chord.Node, error) {
	remoteNode, err := services.Connect(node.Addr) // Establish connection with the remote node.
	// In case of error, return the error.
	if err != nil {
		return nil, err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), services.Timeout)
	defer cancel()

	// Return the result of the remote call.
	return remoteNode.GetPredecessor(ctx, emptyRequest)
}

// GetSuccessor returns the node believed to be the current successor of a remote Node.
func (services *GRPCServices) GetSuccessor(node *chord.Node) (*chord.Node, error) {
	remoteNode, err := services.Connect(node.Addr) // Establish connection with the remote node.
	// In case of error, return the error.
	if err != nil {
		return nil, err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), services.Timeout)
	defer cancel()

	// Return the result of the remote call.
	return remoteNode.GetSuccessor(ctx, emptyRequest)
}

// FindSuccessor finds the node that succeeds this ID, starting at a remote Node.
func (services *GRPCServices) FindSuccessor(node *chord.Node, id []byte) (*chord.Node, error) {
	remoteNode, err := services.Connect(node.Addr) // Establish connection with the remote node.
	// In case of error, return the error.
	if err != nil {
		return nil, err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), services.Timeout)
	defer cancel()

	// Return the result of the remote call.
	return remoteNode.FindSuccessor(ctx, &chord.ID{ID: id})
}

// SetPredecessor sets the predecessor of a remote Node.
func (services *GRPCServices) SetPredecessor(node, pred *chord.Node) error {
	remoteNode, err := services.Connect(node.Addr) // Establish connection with the remote node.
	// In case of error, return the error.
	if err != nil {
		return err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), services.Timeout)
	defer cancel()

	// Return the result of the remote call.
	_, err = remoteNode.SetPredecessor(ctx, pred)
	return err
}

// SetSuccessor sets the successor of a remote Node.
func (services *GRPCServices) SetSuccessor(node, suc *chord.Node) error {
	remoteNode, err := services.Connect(node.Addr) // Establish connection with the remote node.
	// In case of error, return the error.
	if err != nil {
		return err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), services.Timeout)
	defer cancel()

	// Return the result of the remote call.
	_, err = remoteNode.SetSuccessor(ctx, suc)
	return err
}

// Notify a remote Node that it possibly have a new predecessor.
func (services *GRPCServices) Notify(node, pred *chord.Node) error {
	remoteNode, err := services.Connect(node.Addr) // Establish connection with the remote node.
	// In case of error, return the error.
	if err != nil {
		return err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), services.Timeout)
	defer cancel()

	// Return the result of the remote call.
	_, err = remoteNode.Notify(ctx, pred)
	return err

}
