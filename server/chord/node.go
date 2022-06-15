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

	RPC RemoteServices // Transport layer of this node.

	predecessor *chord.Node  // Predecessor of this node in the ring.
	predLock    sync.RWMutex // Locks the predecessor for reading or writing.
	successor   *chord.Node  // Successor of this node in the ring.
	sucLock     sync.RWMutex // Locks the successor for reading or writing.

	fingerTable FingerTable  // FingerTable of this node.
	fingerLock  sync.RWMutex // Locks the finger table for reading or writing.

	dictionary Storage      // Storage of <key, value> pairs of this node.
	dictLock   sync.RWMutex // Locks the dictionary for reading or writing.

	config *Configuration // General configurations.
}

// NewNode creates and returns a new Node.
func NewNode(address string) (*Node, error) {
	// TODO: Change the default Configuration by an Configuration optional argument.
	// TODO: Obtain the ring size from the Configuration.
	configuration := DefaultConfig()                // Obtain the configuration of the node.
	id, err := HashKey(address, configuration.Hash) // Obtain the ID relative to this address.
	if err != nil {
		return nil, err
	}

	// Creates the new node with the obtained ID and same address.
	innerNode := chord.Node{ID: id, Address: address}

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

// Join a Node to the Chord ring, using another known node.
func (node *Node) Join(knownNode *chord.Node) error {
	// If knownNode is null, return error: to join this node to the ring, you must know a node already on it.
	if knownNode == nil {
		return errors.New("invalid argument: known node cannot be null")
	}

	// Ask the known remote node if this node already exists on the ring,
	// finding the node that succeeds this node ID.
	suc, err := node.RPC.FindSuccessor(knownNode, node.ID)
	if err != nil {
		return err
	}
	// If the ID of the obtained node is this node ID, then this node is already on the ring.
	if Equals(suc.ID, node.ID) {
		return errors.New("cannot join this node to the ring: a node with this ID already exists")
	}

	// Lock the successor to write on it, and unlock it after.
	node.sucLock.Lock()
	node.successor = suc
	node.sucLock.Unlock()

	return nil
}

// Stabilize this node, updating its successor and notifying it.
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
	if Between(candidate.ID, node.ID, suc.ID, false, false) {
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
	if Equals(pred.ID, node.ID) {
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

// LocateKey locate the node that stores key.
func (node *Node) LocateKey(key string) (*chord.Node, error) {
	id, err := HashKey(key, node.config.Hash) // Obtain the key ID.
	if err != nil {
		return nil, err
	}

	// Find and return the successor of this ID.
	return node.FindIDSuccessor(id)
}

// Node server chord methods.

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

// SetPredecessor sets the predecessor of this node.
func (node *Node) SetPredecessor(ctx context.Context, pred *chord.Node) (*chord.EmptyResponse, error) {
	// If the predecessor node is null, return error.
	if pred == nil {
		return nil, errors.New("invalid argument: predecessor node cannot be null")
	}

	// Lock the predecessor to write on it, and unlock it after.
	node.predLock.Lock()
	node.predecessor = pred
	node.predLock.Unlock()
	return emptyResponse, nil
}

// SetSuccessor sets predecessor for this node.
func (node *Node) SetSuccessor(ctx context.Context, suc *chord.Node) (*chord.EmptyResponse, error) {
	// If the successor node is null, return error.
	if suc == nil {
		return nil, errors.New("invalid argument: predecessor node cannot be null")
	}

	// Lock the successor to write on it, and unlock it after.
	node.sucLock.Lock()
	node.successor = suc
	node.sucLock.Unlock()
	return emptyResponse, nil
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

// Notify this node that it possibly have a new predecessor.
func (node *Node) Notify(ctx context.Context, pred *chord.Node) (*chord.EmptyResponse, error) {
	// Lock the successor to read and write on it, and unlock it at the end of function.
	node.predLock.Lock()
	defer node.predLock.Unlock()

	// If the predecessor candidate is closer to this node than its current predecessor, update this node
	// predecessor with the candidate.
	if node.predecessor == nil || Between(pred.ID, node.predecessor.ID, node.ID, false, false) {
		node.predecessor = pred
	}

	return emptyResponse, nil
}

// Check if this node is alive.
func (node *Node) Check(ctx context.Context) (*chord.EmptyResponse, error) {
	return emptyResponse, nil
}

// Node server dictionary methods.

// Get the value associated to a key.
func (node *Node) Get(ctx context.Context, req *chord.GetRequest) (*chord.GetResponse, error) {
	keyNode := node.Node // By default, find the key in the local storage.
	var err error

	// If the requested key is not necessarily local.
	if !req.FromLocal {
		keyNode, err = node.LocateKey(req.Key) // Locate the node that stores the key.
		if err != nil {
			return nil, err
		}
	}

	// If the node that stores the key is this node, directly get the associated value from this node storage.
	if req.FromLocal || Equals(keyNode.ID, node.ID) {
		node.dictLock.RLock()                      // Lock the dictionary to read it, and unlock it after.
		value, err := node.dictionary.Get(req.Key) // Get the key value from storage.
		node.dictLock.RUnlock()
		if err != nil {
			return nil, err
		}
		// Return the key value.
		return &chord.GetResponse{Value: value}, nil
	}
	// Otherwise, return the result of the remote call on the correspondent node, resolving local.
	req.FromLocal = true
	return node.RPC.Get(keyNode, req)
}

// Set a <key, value> pair on storage.
func (node *Node) Set(ctx context.Context, req *chord.SetRequest) (*chord.EmptyResponse, error) {
	keyNode := node.Node // By default, set the <key, value> pair on the local storage.
	var err error

	// If the requested key is not necessarily local.
	if !req.FromLocal {
		keyNode, err = node.LocateKey(req.Key) // Locate the node to which that key corresponds.
		if err != nil {
			return nil, err
		}
	}

	// If the node that stores the key is this node, directly set the <key, value> pair on this node storage.
	if req.FromLocal || Equals(keyNode.ID, node.ID) {
		node.dictLock.Lock() // Lock the dictionary to write on it, and unlock it at the end of function.
		defer node.dictLock.Unlock()

		err := node.dictionary.Set(req.Key, req.Value) // Set the <key, value> pair on storage.
		if err != nil {
			return nil, err
		}

		return emptyResponse, nil
	}
	// Otherwise, return the result of the remote call on the correspondent node, resolving local.
	req.FromLocal = true
	return emptyResponse, node.RPC.Set(keyNode, req)
}

// Delete a <key, value> pair from storage.
func (node *Node) Delete(ctx context.Context, req *chord.DeleteRequest) (*chord.EmptyResponse, error) {
	keyNode := node.Node // By default, delete the key from the local storage.
	var err error

	// If the requested key is not necessarily local.
	if !req.FromLocal {
		keyNode, err = node.LocateKey(req.Key) // Locate the node that stores the key.
		if err != nil {
			return nil, err
		}
	}

	// If the node that stores the key is this node, directly delete the <key, value> pair from this node storage.
	if req.FromLocal || Equals(keyNode.ID, node.ID) {
		node.dictLock.Lock() // Lock the dictionary to write on it, and unlock it at the end of function.
		defer node.dictLock.Unlock()

		err := node.dictionary.Delete(req.Key) // Delete the <key, value> pair from storage.
		if err != nil {
			return nil, err
		}

		return emptyResponse, err
	}
	// Otherwise, return the result of the remote call on the correspondent node, resolving local.
	req.FromLocal = true
	return emptyResponse, node.RPC.Delete(keyNode, req)
}

// Node server dictionary multiple key methods.

// MultiSet set a list of <key, value> pairs on storage, if they correspond all to this node.
func (node *Node) MultiSet(ctx context.Context, req *chord.MultiSetRequest) (*chord.EmptyResponse, error) {
	keys := req.Keys     // Obtain the keys to add.
	values := req.Values // Obtain its correspondent values.

	// If the lists have different length, return error.
	if len(keys) != len(values) {
		return nil, errors.New("inconsistent lists of keys and values: they must have the same length")
	}

	// If there are no keys, return.
	if keys == nil || len(keys) == 0 {
		return emptyResponse, nil
	}

	keyNode := node.Node // By default, set the <key, value> pairs in the local storage.
	predKeyNode := node.predecessor
	var err error

	// If the requested keys are not necessarily local.
	if !req.FromLocal {
		keyNode, err = node.LocateKey(keys[0]) // Locate the node that stores the start key.
		if err != nil {
			return nil, err
		}

		predKeyNode, err = node.RPC.GetPredecessor(keyNode) // Obtain its predecessor.
		if err != nil {
			return nil, err
		}
	}

	// Check if all the keys belong to the correspondent node.
	for _, key := range keys {
		// Check if this key ID is in the (key node predecessor ID, key node ID] interval
		between, err := KeyBetween(key, node.config.Hash, predKeyNode.ID, keyNode.ID, false, true)
		if err != nil {
			return nil, err
		}
		// If the condition not holds, then the list of keys is invalid.
		if !between {
			return nil, errors.New("invalid list of keys to delete: the keys are stored on different nodes")
		}
	}

	// If the node that stores the key is this node, directly set the <key, value> pairs on this node storage.
	if req.FromLocal || Equals(keyNode.ID, node.ID) {
		node.dictLock.Lock() // Lock the dictionary to write on it, and unlock it at the end of function.
		defer node.dictLock.Unlock()

		err := node.dictionary.MultiSet(req.Keys, req.Values) // Set the <key, value> pairs on the storage.
		if err != nil {
			return nil, err
		}

		return emptyResponse, err
	}
	// Otherwise, return the result of the remote call on the correspondent node, resolving local.
	req.FromLocal = true
	return emptyResponse, node.RPC.MultiSet(keyNode, req)
}

// MultiDelete delete an interval of <key, value> pairs from storage, if they correspond all to this node.
func (node *Node) MultiDelete(ctx context.Context, req *chord.MultiDeleteRequest) (*chord.EmptyResponse, error) {
	keys := req.Keys // Obtain the keys to delete.
	// If there are no keys, return.
	if keys == nil || len(keys) == 0 {
		return emptyResponse, nil
	}

	keyNode := node.Node // By default, delete the keys from the local storage.
	predKeyNode := node.predecessor
	var err error

	// If the requested keys are not necessarily local.
	if !req.FromLocal {
		keyNode, err = node.LocateKey(keys[0]) // Locate the node that stores the start key.
		if err != nil {
			return nil, err
		}

		predKeyNode, err = node.RPC.GetPredecessor(keyNode) // Obtain its predecessor.
		if err != nil {
			return nil, err
		}
	}

	// Check if all the keys belong to the correspondent node.
	for _, key := range keys {
		// Check if this key ID is in the (key node predecessor ID, key node ID] interval
		between, err := KeyBetween(key, node.config.Hash, predKeyNode.ID, keyNode.ID, false, true)
		if err != nil {
			return nil, err
		}
		// If the condition not holds, then the list of keys is invalid.
		if !between {
			return nil, errors.New("invalid list of keys to delete: the keys are stored on different nodes")
		}
	}

	// If the node that stores the key is this node, directly delete the <key, value> pairs from this node storage.
	if req.FromLocal || Equals(keyNode.ID, node.ID) {
		node.dictLock.Lock() // Lock the dictionary to write on it, and unlock it at the end of function.
		defer node.dictLock.Unlock()

		err := node.dictionary.MultiDelete(req.Keys) // Delete the <key, value> pairs from storage.
		if err != nil {
			return nil, err
		}

		return emptyResponse, err
	}
	// Otherwise, return the result of the remote call on the correspondent node, resolving local.
	req.FromLocal = true
	return emptyResponse, node.RPC.MultiDelete(keyNode, req)
}
