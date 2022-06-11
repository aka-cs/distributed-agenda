package chord

import (
	"DistributedTable/chord"
	"errors"
	"golang.org/x/net/context"
	"sync"
)

type Node struct {
	*chord.Node // Real Node.

	RPC RemoteServices // Transport layer of the Node.

	predecessor *chord.Node  // Predecessor of this Node in the ring.
	predLock    sync.RWMutex // Locks the predecessor for reading or writing.
	successor   *chord.Node  // Successor of this Node in the ring.
	sucLock     sync.RWMutex // Locks the successor for reading or writing.

	fingerTable *FingerTable // FingerTable of this Node.
	fingerLock  sync.RWMutex // Locks the FingerTable for reading or writing.

	config *Configuration // General configurations.
}

// NewNode creates and returns a new Node.
func NewNode(addr string) (*Node, error) {
	// TODO: Change the default Configuration by an Configuration optional argument.
	// TODO: Obtain the ring size from the Configuration.
	configuration := DefaultConfig()       // Obtain the configuration of the node.
	id, err := configuration.hashKey(addr) // Obtain the ID relative to this address.

	// If we get an error, return error.
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
		fingerTable: newFingerTable(&innerNode, 160)}

	// Return the node.
	return &node, nil
}

// Join a Node to the Chord ring, using another known Node.
func (node *Node) Join(knownNode *chord.Node) error {
	// If knownNode is null, return error: to join this node to the ring, you must know a node already on it.
	if knownNode == nil {
		return errors.New("invalid argument: known node cannot be null")
	}

	// Ask the known remote node if this node already exists on the ring,
	// finding the node that succeeds this node ID.
	suc, err := node.RPC.FindSuccessor(knownNode, node.Id)
	// In case of error, return the obtained error.
	if err != nil {
		return err
	}
	// If the ID of the obtained node is this node ID, then this node is already on the ring.
	if Equal(suc.Id, node.Id) {
		return errors.New("cannot join this node to the ring: a node with this ID already exists")
	}

	// Lock the successor to write on it, and unlock it before.
	node.sucLock.Lock()
	node.successor = suc
	node.sucLock.Unlock()

	return nil
}

// Stabilize this Node, updating its successor and notifying it.
func (node *Node) Stabilize() {
	// Lock the successor to read it, and unlock it before.
	node.sucLock.RLock()
	suc := node.successor
	// If successor is null, there is no way to stabilize (and sure nothing to stabilize).
	if suc == nil {
		node.sucLock.RUnlock()
		return
	}
	node.sucLock.RUnlock()

	candidate, err := node.RPC.GetPredecessor(suc) // Obtain the predecessor of the successor.
	// In case of error, return.
	if err != nil || candidate == nil {
		return
	}

	// If candidate is closer to this node than its current successor, update this node successor
	// with the candidate.
	if Between(candidate.Id, node.Id, suc.Id) {
		// Lock the successor to write on it, and unlock it before.
		node.sucLock.Lock()
		node.successor = candidate
		node.sucLock.Unlock()
	}
	// TODO: Implement Notify to uncomment the line below.
	// node.RPC.Notify(suc, node.Node)
}

// Node server methods.

// GetPredecessor returns the node believed to be the current predecessor.
func (node *Node) GetPredecessor(ctx context.Context, r *chord.EmptyRequest) (*chord.Node, error) {
	// Lock the predecessor to read it, and unlock it before.
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
	// Lock the successor to read it, and unlock it before.
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

	// Look on the FingerTable to found the closest finger with ID lower or equal than this ID.
	node.fingerLock.RLock()                       // Lock the FingerTable to read from it.
	pred := node.fingerTable.closestFinger(id.ID) // Find the successor of this ID in the FingerTable.
	node.fingerLock.RUnlock()                     // After finishing read, unlock the FingerTable.

	// If the correspondent finger is null (this node is isolated), return this node.
	if pred == nil {
		return node.Node, nil
	}

	// If the corresponding finger its itself, the key is stored in its successor.
	if Equal(pred.Id, node.Id) {
		// Lock the successor to read it, and unlock it before.
		node.sucLock.RLock()
		suc := node.successor
		node.sucLock.RUnlock()

		// If the successor is null, return this node.
		if suc == nil {
			return node.Node, nil
		}

		return suc, nil
	}

	suc, err := node.RPC.FindSuccessor(pred, id.ID) // Find the successor of the remote node obtained.
	// In case of error, return the obtained error.
	if err != nil {
		return nil, err
	}
	// If the successor is null, return this node.
	if suc == nil {
		return node.Node, nil
	}

	return suc, nil
}

// SetPredecessor sets the predecessor of a node.
func (node *Node) SetPredecessor(ctx context.Context, pred *chord.Node) (*chord.EmptyRequest, error) {
	// If the predecessor node is null, return error.
	if pred == nil {
		return nil, errors.New("invalid argument: predecessor node cannot be null")
	}

	// Lock the predecessor to write on it, and unlock it before.
	node.predLock.Lock()
	node.predecessor = pred
	node.predLock.Unlock()
	return emptyRequest, nil
}

// SetSuccessor sets predecessor for a node.
func (node *Node) SetSuccessor(ctx context.Context, suc *chord.Node) (*chord.EmptyRequest, error) {
	// If the successor node is null, return error.
	if suc == nil {
		return nil, errors.New("invalid argument: predecessor node cannot be null")
	}

	// Lock the successor to write on it, and unlock it before.
	node.sucLock.Lock()
	node.successor = suc
	node.sucLock.Unlock()
	return emptyRequest, nil
}
