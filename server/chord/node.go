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

// Node represent a Chord ring single node.
type Node struct {
	*chord.Node // Real Node.

	successors *Queue[*chord.Node]

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

	server *grpc.Server // Node server.

	running int32 // Determine if the node is actually running.

	chord.UnimplementedChordServer
}

// NewNode creates and returns a new Node.
func NewNode(address string, configuration *Configuration) (*Node, error) {
	id, err := HashKey(address, configuration.Hash) // Obtain the ID relative to this address.
	if err != nil {
		return nil, err
	}

	// Creates the new node with the obtained ID and same address.
	innerNode := chord.Node{ID: id, Address: address}

	transport := NewGRPCServices(configuration)           // Creates the transport layer.
	server := grpc.NewServer(configuration.ServerOpts...) // Creates the node server.

	// Instantiates the node.
	node := &Node{Node: &innerNode,
		predecessor: nil,
		successor:   &innerNode,
		RPC:         transport,
		config:      configuration,
		server:      server,
		running:     0,
		fingerTable: NewFingerTable(&innerNode, configuration.HashSize)}

	// Return the node.
	return node, nil
}

// Node server internal methods.

// Start the node services.
func (node *Node) Start() {
	atomic.StoreInt32(&node.running, 1)          // Report the node is running.
	chord.RegisterChordServer(node.server, node) // Register the node server.
	node.RPC.Start()                             // Start the RPC (transport layer) services.
	// Start periodically threads.
	go node.PeriodicallyCheckPredecessor()
	go node.PeriodicallyCheckSuccessor()
	go node.PeriodicallyStabilize()
	go node.PeriodicallyFixSuccessor()
	go node.PeriodicallyFixFinger()
}

func (node *Node) Stop() {
	atomic.StoreInt32(&node.running, 0) // Report the node is shutdown.

	// Notify successor to change its predecessor pointer to our predecessor.
	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successor
	node.sucLock.RUnlock()

	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	if suc != nil && pred != nil && !Equals(suc.ID, pred.ID) {
		// Lock the dictionary to read it, and unlock it after.
		node.dictLock.RLock()
		dictionary, err := node.dictionary.Segment(nil, pred.ID) // Obtain the keys to transfer.
		node.dictLock.RUnlock()
		if err != nil {
		}

		// Transfer the predecessor keys to its new successor, to update it.
		err = node.RPC.Extend(suc, &chord.ExtendRequest{Dictionary: dictionary})
		if err != nil {
			return
		}

		err = node.RPC.SetPredecessor(suc, pred)
		err = node.RPC.SetSuccessor(pred, suc)
		if err != nil {
		}
	}

	node.RPC.Stop()
}

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
			// Lock the successor to read it, and unlock it after.
			node.sucLock.RLock()
			suc := node.successor
			node.sucLock.RUnlock()

			// Lock the dictionary to read it, and unlock it after.
			node.dictLock.RLock()
			dictionary, err := node.dictionary.Segment(nil, pred.ID)
			node.dictLock.RUnlock()
			if err != nil {
				return
			}

			// Lock the predecessor to write on it, and unlock it after.
			node.predLock.Lock()
			node.predecessor = nil
			node.predLock.Unlock()

			// Transfer the keys to its successor, to update it.
			err = node.RPC.Extend(suc, &chord.ExtendRequest{Dictionary: dictionary})
			if err != nil {
				return
			}
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

// CheckSuccessor checks whether successor has failed.
func (node *Node) CheckSuccessor() {
	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successor
	node.sucLock.RUnlock()

	// If successor is null, or it's not alive.
	if suc == nil || node.RPC.Check(suc) != nil {
		// If there are substitute successors.
		if node.successors.size > 0 {
			suc, err := node.successors.PopBeg() // Take the next successor.
			if err != nil {
				return
			}

			// Lock the successor to write on it, and unlock it after.
			node.sucLock.Lock()
			node.successor = suc
			node.sucLock.Unlock()

			// Lock the dictionary to read it, and unlock it after.
			node.dictLock.RLock()
			dictionary, err := node.dictionary.Segment(nil, nil)
			node.dictLock.RUnlock()
			if err != nil {
				return
			}

			// Transfer the keys to its successor, to update it.
			err = node.RPC.Extend(suc, &chord.ExtendRequest{Dictionary: dictionary})
			if err != nil {
				return
			}

		} else {
			// Lock the predecessor to read it, and unlock it after.
			node.predLock.RLock()
			pred := node.predecessor
			node.predLock.RUnlock()

			err := node.successors.PushBack(pred)
			if err != nil {
				return
			}
		}
	}
}

// PeriodicallyCheckSuccessor periodically checks whether successor has failed.
func (node *Node) PeriodicallyCheckSuccessor() {
	ticker := time.NewTicker(10 * time.Second) // Set the time between routine activations.
	for {
		select {
		case <-ticker.C:
			node.CheckSuccessor() // If it's time, check if this node successor it's alive.
		}
	}
}

// FixSuccessor fix an entry of the successor queue.
func (node *Node) FixSuccessor(suc *QueueNode[*chord.Node]) *QueueNode[*chord.Node] {
	queue := node.successors

	if suc == queue.last && queue.capacity == queue.size {
		return nil
	}

	next, err := node.RPC.GetSuccessor(suc.value)
	if err != nil {
		err = queue.Remove(suc)
		if err != nil {
			return nil
		}
		return suc.prev
	}

	if !Equals(next.ID, node.ID) {
		if suc == queue.last {
			err = queue.PushBack(next)
			if err != nil {
				return suc
			}
		} else {
			suc.next.value = next
		}
	}
	return suc.next
}

// PeriodicallyFixSuccessor periodically checks whether predecessor has failed.
func (node *Node) PeriodicallyFixSuccessor() {
	ticker := time.NewTicker(10 * time.Second) // Set the time between routine activations.
	var suc *QueueNode[*chord.Node] = nil

	for {
		select {
		case <-ticker.C:
			if suc == nil {
				suc = node.successors.first
			}
			if suc != nil {
				suc = node.FixSuccessor(suc)
			}
		}
	}
}

// Node server chord methods.

// GetPredecessor returns the node believed to be the current predecessor.
func (node *Node) GetPredecessor(ctx context.Context, req *chord.EmptyRequest) (*chord.Node, error) {
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
func (node *Node) GetSuccessor(ctx context.Context, req *chord.EmptyRequest) (*chord.Node, error) {
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
func (node *Node) Notify(ctx context.Context, new *chord.Node) (*chord.EmptyResponse, error) {
	// Lock the predecessor to read it, and unlock it at the end of function.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// If the predecessor candidate is closer to this node than its current predecessor, update this node
	// predecessor with the candidate.
	if pred == nil || Between(new.ID, pred.ID, node.ID, false, false) {
		// Lock the predecessor to write on it, and unlock it at the end of function.
		node.predLock.Lock()
		node.predecessor = new
		node.predLock.Unlock()

		// Lock the successor to read it, and unlock it at the end of function.
		node.sucLock.RLock()
		suc := node.successor
		node.sucLock.RUnlock()

		// Delete the keys to transfer from successor storage replication.
		err := node.RPC.Detach(suc, &chord.DetachRequest{L: nil, R: new.ID})
		if err != nil {
			return nil, err
		}

		// Lock the dictionary to read it, and unlock it after.
		node.dictLock.RLock()
		dictionary, err := node.dictionary.Segment(nil, new.ID) // Obtain the keys to transfer.
		node.dictLock.RUnlock()
		if err != nil {
			return nil, err
		}

		// Build the new predecessor dictionary, by transferring the correspondent keys.
		err = node.RPC.Extend(new, &chord.ExtendRequest{Dictionary: dictionary})
		if err != nil {
			return nil, err
		}

		// Lock the dictionary to write on it, and unlock it after.
		node.dictLock.Lock()
		err = node.dictionary.Detach(nil, pred.ID) // Delete the keys of the old predecessor.
		node.dictLock.Unlock()
		if err != nil {
			return nil, err
		}
	}

	return emptyResponse, nil
}

// Check if this node is alive.
func (node *Node) Check(ctx context.Context, req *chord.EmptyRequest) (*chord.EmptyResponse, error) {
	return emptyResponse, nil
}

// Node server dictionary methods.

// Get the value associated to a key.
func (node *Node) Get(ctx context.Context, req *chord.GetRequest) (*chord.GetResponse, error) {
	keyID, err := HashKey(req.Key, node.config.Hash) // Obtain the correspondent ID of the key.
	if err != nil {
		return nil, err
	}

	keyNode := node.Node  // By default, find the key in the local storage.
	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

	// If the requested key is not necessarily local.
	if pred == nil || Between(keyID, pred.ID, node.ID, false, true) {
		keyNode, err = node.LocateKey(req.Key) // Locate the node that stores the key.
		if err != nil {
			return nil, err
		}
	}

	// If the node that stores the key is this node, directly get the associated value from this node storage.
	if Equals(keyNode.ID, node.ID) {
		node.dictLock.RLock()                      // Lock the dictionary to read it, and unlock it after.
		value, err := node.dictionary.Get(req.Key) // Get the key value from storage.
		node.dictLock.RUnlock()
		if err != nil {
			return nil, err
		}
		// Return the key value.
		return &chord.GetResponse{Value: value}, nil
	}
	// Otherwise, return the result of the remote call on the correspondent node.
	return node.RPC.Get(keyNode, req)
}

// Set a <key, value> pair on storage.
func (node *Node) Set(ctx context.Context, req *chord.SetRequest) (*chord.EmptyResponse, error) {
	// If this request is a replica, resolve it local.
	if req.Replica {
		node.dictLock.Lock()                    // Lock the dictionary to write on it, and unlock it after.
		node.dictionary.Set(req.Key, req.Value) // Set the <key, value> pair on storage.
		node.dictLock.Unlock()
		return emptyResponse, nil
	}

	// Otherwise, proceed normally.
	keyID, err := HashKey(req.Key, node.config.Hash) // Obtain the correspondent ID of the key.
	if err != nil {
		return nil, err
	}

	keyNode := node.Node  // By default, set the <key, value> pair on the local storage.
	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

	// If the requested key is not necessarily local.
	if pred == nil || Between(keyID, pred.ID, node.ID, false, true) {
		keyNode, err = node.LocateKey(req.Key) // Locate the node to which that key corresponds.
		if err != nil {
			return nil, err
		}
	}

	// If the key corresponds to this node, directly set the <key, value> pair on its storage.
	if Equals(keyNode.ID, node.ID) {
		node.dictLock.Lock()                    // Lock the dictionary to write on it, and unlock it after.
		node.dictionary.Set(req.Key, req.Value) // Set the <key, value> pair on storage.
		node.dictLock.Unlock()

		node.sucLock.RLock() // Lock the successor to read it, and unlock it after.
		suc := node.successor
		node.sucLock.RUnlock()

		// If successor is not null, replicate the request to it.
		if suc != nil {
			req.Replica = true
			return emptyResponse, node.RPC.Set(suc, req)
		}
		// Else, return.
		return emptyResponse, nil
	}
	// Otherwise, return the result of the remote call on the correspondent node.
	return emptyResponse, node.RPC.Set(keyNode, req)
}

// Delete a <key, value> pair from storage.
func (node *Node) Delete(ctx context.Context, req *chord.DeleteRequest) (*chord.EmptyResponse, error) {
	// If this request is a replica, resolve it local.
	if req.Replica {
		node.dictLock.Lock()            // Lock the dictionary to write on it, and unlock it after.
		node.dictionary.Delete(req.Key) // Delete the <key, value> pair from storage.
		node.dictLock.Unlock()
		return emptyResponse, nil
	}

	// Otherwise, proceed normally.
	keyID, err := HashKey(req.Key, node.config.Hash) // Obtain the correspondent ID of the key.
	if err != nil {
		return nil, err
	}

	keyNode := node.Node  // By default, delete the key from the local storage.
	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

	// If the requested key is not necessarily local.
	if pred == nil || Between(keyID, pred.ID, node.ID, false, true) {
		keyNode, err = node.LocateKey(req.Key) // Locate the node that stores the key.
		if err != nil {
			return nil, err
		}
	}

	// If the key corresponds to this node, directly delete the <key, value> pair from its storage.
	if Equals(keyNode.ID, node.ID) {
		node.dictLock.Lock()            // Lock the dictionary to write on it, and unlock it at the end of function.
		node.dictionary.Delete(req.Key) // Delete the <key, value> pair from storage.
		node.dictLock.Unlock()

		node.sucLock.RLock() // Lock the successor to read it, and unlock it after.
		suc := node.successor
		node.sucLock.RUnlock()

		// If successor is not null, replicate the request to it.
		if suc != nil {
			req.Replica = true
			return emptyResponse, node.RPC.Delete(suc, req)
		}
		// Else, return.
		return emptyResponse, nil
	}
	// Otherwise, return the result of the remote call on the correspondent node.
	return emptyResponse, node.RPC.Delete(keyNode, req)
}

// Extend set a list of <key, values> pairs on the storage dictionary.
func (node *Node) Extend(ctx context.Context, req *chord.ExtendRequest) (*chord.EmptyResponse, error) {
	// If there are no keys, return.
	if req.Dictionary == nil || len(req.Dictionary) == 0 {
		return emptyResponse, nil
	}

	node.dictLock.Lock()                          // Lock the dictionary to write on it, and unlock it after.
	err := node.dictionary.Extend(req.Dictionary) // Set the <key, value> pairs on the storage.
	node.dictLock.Unlock()
	if err != nil {
		return nil, err
	}
	return emptyResponse, err
}

// Detach deletes all <key, values> pairs in a given interval storage.
func (node *Node) Detach(ctx context.Context, req *chord.DetachRequest) (*chord.EmptyResponse, error) {
	node.dictLock.Lock()                        // Lock the dictionary to write on it, and unlock it after.
	err := node.dictionary.Detach(req.L, req.R) // Set the <key, value> pairs on the storage.
	node.dictLock.Unlock()
	if err != nil {
		return nil, err
	}
	return emptyResponse, err
}
