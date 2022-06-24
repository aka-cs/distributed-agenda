package chord

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"server/chord/chord"
	"sync"
	"sync/atomic"
	"time"
)

// RemoteServices enables a node to interact with other nodes in the ring, as a client of its servers.
type RemoteServices interface {
	Start() // Start the services.
	Stop()  // Stop the services.

	// GetPredecessor returns the node believed to be the current predecessor of a remote node.
	GetPredecessor(*chord.Node) (*chord.Node, error)
	// GetSuccessor returns the node believed to be the current successor of a remote node.
	GetSuccessor(*chord.Node) (*chord.Node, error)
	// SetPredecessor sets the predecessor of a remote node.
	SetPredecessor(*chord.Node, *chord.Node) error
	// SetSuccessor sets the successor of a remote node.
	SetSuccessor(*chord.Node, *chord.Node) error
	// FindSuccessor finds the node that succeeds this ID, starting at a remote node.
	FindSuccessor(*chord.Node, []byte) (*chord.Node, error)
	// Notify a remote node that it possibly have a new predecessor.
	Notify(*chord.Node, *chord.Node) error
	// Check if a remote node is alive.
	Check(*chord.Node) error

	// Get the value associated to a key on a remote node storage.
	Get(node *chord.Node, req *chord.GetRequest) (*chord.GetResponse, error)
	// Set a <key, value> pair on a remote node storage.
	Set(node *chord.Node, req *chord.SetRequest) error
	// Delete a <key, value> pair from a remote node storage.
	Delete(node *chord.Node, req *chord.DeleteRequest) error
	// Extend the storage dictionary of a remote node with a list of <key, values> pairs.
	Extend(node *chord.Node, req *chord.ExtendRequest) error
	// Discard all <key, values> pairs in a given interval storage of a remote node.
	Discard(node *chord.Node, req *chord.DiscardRequest) error
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

// GRPCServices internal methods.

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
	// If the service is shutdown, return.
	if atomic.LoadInt32(&services.running) == 0 {
		return
	}
	services.connectionsMtx.Lock() // Lock the dictionary to write on it.
	// For RemoteNode on the dictionary, if its lifetime is over, close the connection.
	for addr, remoteNode := range services.connections {
		if time.Since(remoteNode.lastActive) > services.MaxIdle {
			remoteNode.CloseConnection()
			delete(services.connections, addr) // Delete the <address, connection> pair of the dictionary.
		}
	}
	services.connectionsMtx.Unlock() // After finishing write, unlock the dictionary.
}

// PeriodicallyCloseConnections periodically close the old open connections.
func (services *GRPCServices) PeriodicallyCloseConnections() {
	ticker := time.NewTicker(60 * time.Second) // Set the time between routine activations.

	for {
		select {
		case <-ticker.C:
			services.CloseOldConnections() // If it's time, close old connections.
		}
	}
}

// Start the services.
func (services *GRPCServices) Start() {
	atomic.StoreInt32(&services.running, 1) // Report the service is running.
	log.Info("Starting transport layer services...\n")

	services.connections = make(map[string]*RemoteNode) // Create the dictionary of <address, open connection>.
	// Start periodically threads.
	go services.CloseOldConnections() // Check and close old connections periodically.

	log.Info("Transport layer services started.\n")
}

// Stop the services.
func (services *GRPCServices) Stop() {
	atomic.StoreInt32(&services.running, 0) // Report the service is shutdown.

	// Close all the connections
	services.connectionsMtx.Lock() // Lock the dictionary to write on it.
	// For RemoteNode on the dictionary, if its lifetime is over, close the connection.
	for _, remoteNode := range services.connections {
		if time.Since(remoteNode.lastActive) > services.MaxIdle {
			remoteNode.CloseConnection()
		}
	}
	services.connections = nil       // Delete dictionary of connections.
	services.connectionsMtx.Unlock() // After finishing write, unlock the dictionary.
}

// GRPCServices remote chord methods.

// GetPredecessor returns the node believed to be the current predecessor of a remote node.
func (services *GRPCServices) GetPredecessor(node *chord.Node) (*chord.Node, error) {
	remoteNode, err := services.Connect(node.Address) // Establish connection with the remote node.
	if err != nil {
		return nil, err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), services.Timeout)
	defer cancel()

	// Return the result of the remote call.
	return remoteNode.GetPredecessor(ctx, emptyRequest)
}

// GetSuccessor returns the node believed to be the current successor of a remote node.
func (services *GRPCServices) GetSuccessor(node *chord.Node) (*chord.Node, error) {
	remoteNode, err := services.Connect(node.Address) // Establish connection with the remote node.
	if err != nil {
		return nil, err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), services.Timeout)
	defer cancel()

	// Return the result of the remote call.
	return remoteNode.GetSuccessor(ctx, emptyRequest)
}

// SetPredecessor sets the predecessor of a remote node.
func (services *GRPCServices) SetPredecessor(node, pred *chord.Node) error {
	remoteNode, err := services.Connect(node.Address) // Establish connection with the remote node.
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

// SetSuccessor sets the successor of a remote node.
func (services *GRPCServices) SetSuccessor(node, suc *chord.Node) error {
	remoteNode, err := services.Connect(node.Address) // Establish connection with the remote node.
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

// FindSuccessor finds the node that succeeds this ID, starting at a remote node.
func (services *GRPCServices) FindSuccessor(node *chord.Node, id []byte) (*chord.Node, error) {
	remoteNode, err := services.Connect(node.Address) // Establish connection with the remote node.
	if err != nil {
		return nil, err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), services.Timeout)
	defer cancel()

	// Return the result of the remote call.
	return remoteNode.FindSuccessor(ctx, &chord.ID{ID: id})
}

// Notify a remote node that it possibly have a new predecessor.
func (services *GRPCServices) Notify(node, pred *chord.Node) error {
	remoteNode, err := services.Connect(node.Address) // Establish connection with the remote node.
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

// Check if a remote node is alive.
func (services *GRPCServices) Check(node *chord.Node) error {
	remoteNode, err := services.Connect(node.Address) // Establish connection with the remote node.
	if err != nil {
		return err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), services.Timeout)
	defer cancel()

	// Return the result of the remote call.
	_, err = remoteNode.Check(ctx, emptyRequest)
	return err
}

// GRPCServices remote dictionary methods.

// Get the value associated to a key on a remote node storage.
func (services *GRPCServices) Get(node *chord.Node, req *chord.GetRequest) (*chord.GetResponse, error) {
	remoteNode, err := services.Connect(node.Address) // Establish connection with the remote node.
	if err != nil {
		return nil, err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), services.Timeout)
	defer cancel()

	// Return the result of the remote call.
	return remoteNode.Get(ctx, req)
}

// Set a <key, value> pair on a remote node storage.
func (services *GRPCServices) Set(node *chord.Node, req *chord.SetRequest) error {
	remoteNode, err := services.Connect(node.Address) // Establish connection with the remote node.
	if err != nil {
		return err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), services.Timeout)
	defer cancel()

	// Return the result of the remote call.
	_, err = remoteNode.Set(ctx, req)
	return err
}

// Delete a <key, value> pair from a remote node storage.
func (services *GRPCServices) Delete(node *chord.Node, req *chord.DeleteRequest) error {
	remoteNode, err := services.Connect(node.Address) // Establish connection with the remote node.
	if err != nil {
		return err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), services.Timeout)
	defer cancel()

	// Return the result of the remote call.
	_, err = remoteNode.Delete(ctx, req)
	return err
}

// Extend set a list of <key, values> pairs on the storage dictionary of a remote node.
func (services *GRPCServices) Extend(node *chord.Node, req *chord.ExtendRequest) error {
	remoteNode, err := services.Connect(node.Address) // Establish connection with the remote node.
	if err != nil {
		return err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), services.Timeout)
	defer cancel()

	// Return the result of the remote call.
	_, err = remoteNode.Extend(ctx, req)
	return err
}

// Discard deletes all <key, values> pairs in a given interval storage of a remote node.
func (services *GRPCServices) Discard(node *chord.Node, req *chord.DiscardRequest) error {
	remoteNode, err := services.Connect(node.Address) // Establish connection with the remote node.
	if err != nil {
		return err
	}

	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), services.Timeout)
	defer cancel()

	// Return the result of the remote call.
	_, err = remoteNode.Discard(ctx, req)
	return err
}
