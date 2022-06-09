package chord

import (
	"DistributedTable/chord"
	"golang.org/x/net/context"
	"sync"
)

type Node struct {
	*chord.Node // Real Node.

	RPC Transport // Transport layer of the Node.

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
	configuration := DefaultConfig()       // Obtain the configuration of the Node.
	id, err := configuration.hashKey(addr) // Obtain the ID relative to this address.

	// If we get an error, return error.
	if err != nil {
		return nil, err
	}

	// Creates the new node with the obtained ID and same address.
	innerNode := chord.Node{Id: id, Addr: addr}

	// Creates the transport layer.
	transport := NewNodeTransport(configuration)

	// Instantiates the node.
	node := Node{Node: &innerNode,
		RPC:         transport,
		config:      configuration,
		fingerTable: newFingerTable(&innerNode, 160)}

	// Return the node.
	return &node, nil
}

// GetPredecessor returns the node believed to be the current predecessor.
func (node *Node) GetPredecessor(ctx context.Context, r *chord.EmptyRequest) (*chord.Node, error) {
	// Lock the predecessor to read it, and unlock it before.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// If predecessor is null, return a null Node.
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

	// If successor is null, return a null Node.
	if suc == nil {
		return nullNode, nil
	}

	// Otherwise, return the successor of this node.
	return suc, nil
}

// FindSuccessor finds the node that succeeds ID.
func (node *Node) FindSuccessor(ctx context.Context, id *chord.ID) (*chord.Node, error) {
	// TODO: Handle the cases in which the predecessor/successor is null.
	// TODO: Im returning the current node in this cases, but this is sure incorrect.
	// Look on the FingerTable to found the closest finger with ID lower or equal than this ID.
	node.fingerLock.RLock()                       // Lock the FingerTable to read from it.
	pred := node.fingerTable.closestFinger(id.ID) // Find the successor of this ID in the FingerTable.
	node.fingerLock.RUnlock()                     // After finishing read, unlock the FingerTable.

	// If the correspondent finger is null, return this node.
	if pred == nil {
		return node.Node, nil
	}

	// If the corresponding finger its itself, the key is stored in its successor.
	if isEqual(pred.Id, node.Id) {
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

// SetPredecessor sets predecessor for a node.
// rpc SetPredecessor(Node) returns (nil)
// SetPredecessor sets predecessor for a node.
// rpc SetSuccessor(Node) returns (nil)
