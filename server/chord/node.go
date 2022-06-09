package chord

import (
	"DistributedTable/chord"
	"golang.org/x/net/context"
	"sync"
)

type Node struct {
	*chord.Node // Real Node.

	predecessor *chord.Node  // Predecessor of this Node in the ring.
	predLock    sync.RWMutex // Locks the predecessor for reading if another routine is reading it.
	successor   *chord.Node  // Successor of this Node in the ring.
	sucLock     sync.RWMutex // Locks the successor for reading if another routine is reading it.

	fingerTable *FingerTable // FingerTable of this Node.
	fingerLock  sync.RWMutex // Locks the FingerTable for reading or writing if another routine is doing it.

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

	// Else, creates the new node with the obtained ID and same address.
	innerNode := chord.Node{Id: id, Addr: addr}
	node := Node{Node: &innerNode, config: configuration, fingerTable: newFingerTable(&innerNode, 160)}

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
	// Look on the FingerTable to found the closest finger with ID lower or equal than this ID.
	node.fingerLock.RLock()                       // Lock the FingerTable to read from it.
	pred := node.fingerTable.closestFinger(id.ID) // Find the successor of this ID in the FingerTable.
	node.fingerLock.RLock()                       // Unlock the FingerTable.

	// If the correspondent finger is null, return null.
	if pred == nil {
		return node.Node, nil
	}

	// If the corresponding finger its itself, the key is stored in its successor.
	if isEqual(pred.Id, node.Id) {
		// TODO: Maybe, will be necessary a remote call of GetSuccessor instead of the using of local successor,
		// TODO: because the local successor its possibly outdated on the moment of this call.
		// Lock the successor to read it, and unlock it before.
		node.sucLock.RLock()
		suc := node.successor
		node.sucLock.RUnlock()

		/*
			succ, err = node.getSuccessorRPC(pred)
			if err != nil {
				return nil, err
			}
			if succ == nil {
				// not able to wrap around, current node is the successor
				return pred, nil
			}
		*/

		return suc, nil
	}

	// TODO: Do a remote call to the FindSuccessor method of the obtained Node.
	// TODO: Transport layer necessary urgently.
	/*
		suc, err := node.findSuccessorRPC(pred, id)
		if err != nil {
			return nil, err
		}
		if succ == nil {
			// not able to wrap around, current node is the successor
			return curr, nil
		}
		return succ, nil
	*/
	return nil, nil
}

// SetPredecessor sets predecessor for a node.
// rpc SetPredecessor(Node) returns (nil)
// SetPredecessor sets predecessor for a node.
// rpc SetSuccessor(Node) returns (nil)
