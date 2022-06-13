package chord

import (
	"DistributedTable/chord"
	"errors"
	"golang.org/x/net/context"
	"sync"
	"time"
)

// Node represent a Chord ring single node.
type Node struct {
	*chord.Node // Real Node.

	RPC RemoteServices // Transport layer of the Node.

	predecessor *chord.Node  // Predecessor of this Node in the ring.
	predLock    sync.RWMutex // Locks the predecessor for reading or writing.
	successor   *chord.Node  // Successor of this Node in the ring.
	sucLock     sync.RWMutex // Locks the successor for reading or writing.

	fingerTable FingerTable  // FingerTable of this Node.
	fingerLock  sync.RWMutex // Locks the FingerTable for reading or writing.

	dictionary     Storage      // Storage of <key, value> pairs of this Node.
	dictionaryLock sync.RWMutex // Locks the dictionary for reading or writing.

	config *Configuration // General configurations.
}

// NewNode creates and returns a new Node.
func NewNode(addr string) (*Node, error) {
	// TODO: Change the default Configuration by an Configuration optional argument.
	// TODO: Obtain the ring size from the Configuration.
	configuration := DefaultConfig()             // Obtain the configuration of the node.
	id, err := HashKey(addr, configuration.Hash) // Obtain the ID relative to this address.
	if err != nil {
		return nil, err
	}

	// Creates the new node with the obtained ID and same address.
	innerNode := chord.Node{Id: id, Addr: addr}

	// Creates the transport layer.
	transport := NewGRPCServices(configuration)

	// Instantiates the node.
	node := Node{Node: &innerNode,
		predecessor: nil,
		successor:   &innerNode,
		RPC:         transport,
		config:      configuration,
		fingerTable: NewFingerTable(&innerNode, 160)}

	// Return the node.
	return &node, nil
}

// Node internal methods.

// Join a Node to the Chord ring, using another known Node.
func (node *Node) Join(knownNode *chord.Node) error {
	// If knownNode is null, return error: to join this node to the ring, you must know a node already on it.
	if knownNode == nil {
		return errors.New("invalid argument: known node cannot be null")
	}

	// Ask the known remote node if this node already exists on the ring,
	// finding the node that succeeds this node ID.
	suc, err := node.RPC.FindSuccessor(knownNode, node.Id)
	if err != nil {
		return err
	}
	// If the ID of the obtained node is this node ID, then this node is already on the ring.
	if Equals(suc.Id, node.Id) {
		return errors.New("cannot join this node to the ring: a node with this ID already exists")
	}

	// Lock the successor to write on it, and unlock it after.
	node.sucLock.Lock()
	node.successor = suc
	node.sucLock.Unlock()

	return nil
}

// Stabilize this Node, updating its successor and notifying it.
func (node *Node) Stabilize() {
	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successor
	// If successor is null, there is no way to stabilize (and sure nothing to stabilize).
	if suc == nil {
		node.sucLock.RUnlock()
		return
	}
	node.sucLock.RUnlock()

	candidate, err := node.RPC.GetPredecessor(suc) // Obtain the predecessor of the successor.
	// In case of error of any type, return.
	if err != nil || candidate == nil {
		return
	}

	// If candidate is closer to this node than its current successor, update this node successor
	// with the candidate.
	if Between(candidate.Id, node.Id, suc.Id, false, false) {
		// Lock the successor to write on it, and unlock it after.
		node.sucLock.Lock()
		node.successor = candidate
		node.sucLock.Unlock()
	}
	// Notify successor about the existence of its new predecessor.
	err = node.RPC.Notify(suc, node.Node)
	if err != nil {
		return
	}
}

// PeriodicallyStabilize periodically stabilize the node.
func (node *Node) PeriodicallyStabilize() {
	ticker := time.NewTicker(1 * time.Second) // Set the time between routine activations.
	for {
		select {
		case <-ticker.C:
			node.Stabilize() // If it's time, stabilize the node.
		}
	}
}

// CheckPredecessor checks whether predecessor has failed.
func (node *Node) CheckPredecessor() {
	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// If predecessor is not null, check if it's alive.
	if pred != nil {
		err := node.RPC.Check(pred)
		// In case of error, assume predecessor is not alive, and set this node predecessor to null.
		if err != nil {
			// Lock the predecessor to write on it, and unlock it after.
			node.predLock.Lock()
			node.predecessor = nil
			node.predLock.Unlock()
		}
	}
}

// PeriodicallyCheckPredecessor periodically checks whether predecessor has failed.
func (node *Node) PeriodicallyCheckPredecessor() {
	ticker := time.NewTicker(10 * time.Second) // Set the time between routine activations.
	for {
		select {
		case <-ticker.C:
			node.CheckPredecessor() // If it's time, check if this node predecessor it's alive.
		}
	}
}

// FindIDSuccessor finds the node that succeeds ID.
func (node *Node) FindIDSuccessor(id []byte) (*chord.Node, error) {
	// Look on the FingerTable to found the closest finger with ID lower or equal than this ID.
	node.fingerLock.RLock()        // Lock the FingerTable to read from it.
	pred := node.ClosestFinger(id) // Find the successor of this ID in the FingerTable.
	node.fingerLock.RUnlock()      // After finishing read, unlock the FingerTable.

	// If the correspondent finger is null (this node is isolated), return this node.
	if pred == nil {
		return node.Node, nil
	}

	// If the corresponding finger its itself, the key is stored in its successor.
	if Equals(pred.Id, node.Id) {
		// Lock the successor to read it, and unlock it after.
		node.sucLock.RLock()
		suc := node.successor
		node.sucLock.RUnlock()

		// If the successor is null, return this node.
		if suc == nil {
			return node.Node, nil
		}

		return suc, nil
	}

	suc, err := node.RPC.FindSuccessor(pred, id) // Find the successor of the remote node obtained.
	if err != nil {
		return nil, err
	}
	// If the successor is null, return this node.
	if suc == nil {
		return node.Node, nil
	}

	return suc, nil
}

// LocateKey locate the Node that stores key.
func (node *Node) LocateKey(key string) (*chord.Node, error) {
	id, err := HashKey(key, node.config.Hash) // Obtain the key ID.
	if err != nil {
		return nil, err
	}

	// Find and return the successor of this ID.
	return node.FindIDSuccessor(id)
}

// Node server methods.

// GetPredecessor returns the node believed to be the current predecessor.
func (node *Node) GetPredecessor(ctx context.Context, r *chord.EmptyRequest) (*chord.Node, error) {
	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// If predecessor is null, return a null node.
	if pred == nil {
		return nullNode, nil
	}

	// Otherwise, return the predecessor of this node.
	return pred, nil
}

// GetSuccessor returns the node believed to be the current successor.
func (node *Node) GetSuccessor(ctx context.Context, r *chord.EmptyRequest) (*chord.Node, error) {
	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successor
	node.sucLock.RUnlock()

	// If successor is null, return a null node.
	if suc == nil {
		return nullNode, nil
	}

	// Otherwise, return the successor of this node.
	return suc, nil
}

// FindSuccessor finds the node that succeeds ID.
func (node *Node) FindSuccessor(ctx context.Context, id *chord.ID) (*chord.Node, error) {
	// If the ID is null, return error.
	if id == nil {
		return nil, errors.New("invalid argument: id cannot be null")
	}

	// Otherwise, find the successor of this ID.
	return node.FindIDSuccessor(id.ID)
}

// SetPredecessor sets the predecessor of this node.
func (node *Node) SetPredecessor(ctx context.Context, pred *chord.Node) (*chord.EmptyRequest, error) {
	// If the predecessor node is null, return error.
	if pred == nil {
		return nil, errors.New("invalid argument: predecessor node cannot be null")
	}

	// Lock the predecessor to write on it, and unlock it after.
	node.predLock.Lock()
	node.predecessor = pred
	node.predLock.Unlock()
	return emptyRequest, nil
}

// SetSuccessor sets predecessor for this node.
func (node *Node) SetSuccessor(ctx context.Context, suc *chord.Node) (*chord.EmptyRequest, error) {
	// If the successor node is null, return error.
	if suc == nil {
		return nil, errors.New("invalid argument: predecessor node cannot be null")
	}

	// Lock the successor to write on it, and unlock it after.
	node.sucLock.Lock()
	node.successor = suc
	node.sucLock.Unlock()
	return emptyRequest, nil
}

// Notify this node that it possibly have a new predecessor.
func (node *Node) Notify(ctx context.Context, pred *chord.Node) (*chord.EmptyRequest, error) {
	// Lock the successor to read and write on it, and unlock it at the end of function.
	node.predLock.Lock()
	defer node.predLock.Unlock()

	// If the predecessor candidate is closer to this node than its current predecessor, update this node
	// predecessor with the candidate.
	if node.predecessor == nil || Between(pred.Id, node.predecessor.Id, node.Id, false, false) {
		node.predecessor = pred
	}

	return emptyRequest, nil
}

// Check if this node is alive.
func (node *Node) Check(ctx context.Context) (*chord.EmptyRequest, error) {
	return emptyRequest, nil
}

// DirectlyGet get the value associated to a key on this Node storage. If the key isn't there, return error.
func (node *Node) DirectlyGet(ctx context.Context, req *chord.GetRequest) (*chord.GetResponse, error) {
	value, err := node.dictionary.Get(req.Key) // Get the key value from the storage.
	if err != nil {
		return nil, err
	}
	// Return the key value.
	return &chord.GetResponse{Value: value}, nil
}

// Get the value associated to a key.
func (node *Node) Get(ctx context.Context, req *chord.GetRequest) (*chord.GetResponse, error) {
	keyNode, err := node.LocateKey(req.Key) // Locate the node that stores the key.
	if err != nil {
		return nil, err
	}

	// If the node that stores the key is this node, directly get the associated value from this node storage.
	if Equals(keyNode.Id, node.Id) {
		return node.DirectlyGet(ctx, req)
	}
	// Otherwise, return the result of the remote call on the correspondent node.
	return node.RPC.DirectlyGet(keyNode, req)
}
