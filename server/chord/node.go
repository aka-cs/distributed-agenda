package chord

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"server/chord/chord"
	"sync"
	"time"
)

// Node represent a Chord ring single node.
type Node struct {
	*chord.Node // Real Node.

	predecessor *chord.Node  // Predecessor of this node in the ring.
	predLock    sync.RWMutex // Locks the predecessor for reading or writing.
	successor   *chord.Node  // Successor of this node in the ring.
	sucLock     sync.RWMutex // Locks the successor for reading or writing.

	descendents *Queue[chord.Node] // Queue of descendents of this node.
	descLock    sync.RWMutex       // Locks the descendents queue for reading or writing.

	fingerTable FingerTable  // FingerTable of this node.
	fingerLock  sync.RWMutex // Locks the finger table for reading or writing.

	RPC    RemoteServices // Transport layer of this node.
	config *Configuration // General configurations.

	dictionary Storage      // Storage of <key, value> pairs of this node.
	dictLock   sync.RWMutex // Locks the dictionary for reading or writing.

	server   *grpc.Server  // Node server.
	shutdown chan struct{} // Determine if the node server is actually running.

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
		successor:   nil,
		descendents: NewQueue[chord.Node](configuration.StabilizingNodes),
		fingerTable: NewFingerTable(&innerNode, configuration.HashSize),
		RPC:         transport,
		config:      configuration,
		dictionary:  NewDictionary(configuration.Hash),
		server:      server}

	// Return the node.
	return node, nil
}

// Node server internal methods.

// Start the node server, by registering the server field of this node as a chord server and
// starting the periodically threads that stabilizes the services.
func (node *Node) Start() error {
	node.shutdown = make(chan struct{}) // Report the node server is running.
	log.Info("Starting server...\n")

	chord.RegisterChordServer(node.server, node) // Register the node server.
	log.Info("Chord services registered.\n")

	node.RPC.Start() // Start the RPC (transport layer) services.

	// Start periodically threads.
	go node.PeriodicallyCheckPredecessor()
	go node.PeriodicallyCheckSuccessor()
	go node.PeriodicallyStabilize()
	go node.PeriodicallyFixDescendant()
	go node.PeriodicallyFixFinger()

	log.Info("Server started.\n")
	return nil
}

// Stop the node server.
func (node *Node) Stop() error {
	log.Info("Closing server...\n")

	// Change successor predecessor to our predecessor, and vice-versa.
	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successor
	node.sucLock.RUnlock()

	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// If successor and predecessor are not null, and are not the same node, connect them.
	if suc != nil && pred != nil && !Equals(suc.ID, pred.ID) {
		// Lock the dictionary to read it, and unlock it after.
		node.dictLock.RLock()
		dictionary, err := node.dictionary.Segment(nil, pred.ID) // Obtain the keys to transfer.
		node.dictLock.RUnlock()
		if err != nil {
			message := "Error transferring keys to successor.\nError stopping server.\n"
			log.Error(message)
			return errors.New(err.Error() + message)
		}

		// Build the new predecessor dictionary, by transferring the correspondent keys.
		err = node.RPC.Extend(new, &chord.ExtendRequest{Dictionary: dictionary})
		if err != nil {
			message := "Error transferring keys to successor.\nError stopping server.\n"
			return nil, err
		}

		err := node.RPC.SetSuccessor(pred, suc)
		if err != nil {
			message := "Error setting new successor.\nError stopping server.\n"
			log.Error(message)
			return errors.New(err.Error() + message)
		}
		err = node.RPC.SetPredecessor(suc, pred)
		if err != nil {
			message := "Error setting new predecessor.\nError stopping server.\n"
			log.Error(message)
			return errors.New(err.Error() + message)
		}
	}

	node.RPC.Stop()      // Stop the RPC (transport layer) services.
	close(node.shutdown) // Report the node server is shutdown.
	log.Info("Server closed.\n")
	return nil
}

// Join a Node to the Chord ring, using another known node.
func (node *Node) Join(knownNode *chord.Node) error {
	log.Info("Joining new node to chord ring.\n")

	// If knownNode is null, return error: to join this node to the ring, you must know a node already on it.
	if knownNode == nil {
		message := "Invalid argument, known node cannot be null.\nError joining node to chord ring.\n"
		log.Error(message)
		return errors.New(message)
	}

	// Ask the known remote node if this node already exists on the ring,
	// finding the node that succeeds this node ID.
	suc, err := node.RPC.FindSuccessor(knownNode, node.ID)
	if err != nil {
		message := "Error joining node to chord ring.\n"
		log.Error(err.Error() + message)
		return errors.New(err.Error() + message)
	}
	// If the ID of the obtained node is this node ID, then this node is already on the ring.
	if Equals(suc.ID, node.ID) {
		message := "Error joining node to chord ring: a node with this ID already exists.\n"
		log.Error(message)
		return errors.New(message)
	}

	// Lock the successor to write on it, and unlock it after.
	node.sucLock.Lock()
	node.successor = suc
	node.sucLock.Unlock()

	err = node.RPC.Notify(suc, node.Node) // Notify the existence of this node to its successor.
	if err != nil {
		message := "Error joining node to chord ring.\n"
		log.Error(err.Error() + message)
		return errors.New(err.Error() + message)
	}
	log.Info("Successful join of the node.\n")
	return nil
}

// FindIDSuccessor finds the node that succeeds ID.
func (node *Node) FindIDSuccessor(id []byte) (*chord.Node, error) {
	log.Debug("Finding ID successor.\n")

	// Look on the FingerTable to found the closest finger with ID lower or equal than this ID.
	node.fingerLock.RLock()        // Lock the FingerTable to read from it.
	pred := node.ClosestFinger(id) // Find the successor of this ID in the FingerTable.
	node.fingerLock.RUnlock()      // After finishing read, unlock the FingerTable.

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
		// Otherwise, return this node successor.
		return suc, nil
	}

	// If the corresponding finger it's different to this node.
	suc, err := node.RPC.FindSuccessor(pred, id) // Find the successor of the remote node obtained.
	if err != nil {
		message := "Error finding ID successor.\n"
		log.Error(err.Error() + message)
		return nil, errors.New(err.Error() + message)
	}
	// Otherwise, return the correspondent ID successor.
	log.Debug("ID successor found.\n")
	return suc, nil
}

// LocateKey locate the node that stores key.
func (node *Node) LocateKey(key string) (*chord.Node, error) {
	log.Info("Locating key.\n")

	id, err := HashKey(key, node.config.Hash) // Obtain the key ID.
	if err != nil {
		message := "Error locating key.\n"
		log.Error(message)
		return nil, errors.New(err.Error() + message)
	}

	// Find and return the successor of this ID.
	suc, err := node.FindIDSuccessor(id)
	if err != nil {
		message := "Error locating key.\n"
		log.Error(message)
		return nil, errors.New(err.Error() + message)
	}
	log.Info("Successful key location.\n")
	return suc, nil
}

// Stabilize this node, updating its successor and notifying it.
func (node *Node) Stabilize() {
	log.Debug("Stabilizing node.\n")

	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successor
	node.sucLock.RUnlock()

	// If successor is null, there is no way to stabilize (and sure nothing to stabilize).
	if suc == nil {
		log.Debug("No stabilization needed.\n")
		return
	}

	candidate, err := node.RPC.GetPredecessor(suc) // Otherwise, obtain the predecessor of the successor.
	// In case of error, report it.
	if err != nil {
		log.Error(err.Error() + "Error stabilizing node.\n")
	}
	if candidate == nil {
		log.Error("Error stabilizing node: successor node has no predecessor.\n")
	}

	// If candidate is closer to this node than its current successor, update this node successor
	// with the candidate.
	if Between(candidate.ID, node.ID, suc.ID, false, false) {
		// Lock the successor to write on it, and unlock it after.
		node.sucLock.Lock()
		node.successor = candidate
		node.sucLock.Unlock()

		// Notify successor about the existence of its new predecessor.
		err = node.RPC.Notify(suc, node.Node)
		if err != nil {
			log.Error(err.Error() + "Error stabilizing node.\n")
			return
		}
		log.Debug("Node stabilized.\n")
	} else {
		log.Debug("No stabilization needed.\n")
	}
}

// PeriodicallyStabilize periodically stabilize the node.
func (node *Node) PeriodicallyStabilize() {
	log.Debug("Periodically stabilize thread started.\n")

	ticker := time.NewTicker(1 * time.Second) // Set the time between routine activations.
	for {
		select {
		case <-ticker.C:
			node.Stabilize() // If it's time, stabilize the node.
		case <-node.shutdown:
			ticker.Stop()
			return
		}
	}
}

// CheckPredecessor checks whether predecessor has failed.
func (node *Node) CheckPredecessor() {
	log.Debug("Checking predecessor.\n")

	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// If predecessor is not null, check if it's alive.
	if pred != nil {
		err := node.RPC.Check(pred)
		// In case of error, assume predecessor is not alive, and set this node predecessor to null.
		if err != nil {
			log.Error(err.Error() + "Predecessor failed.\n")

			// Lock the predecessor to write on it, and unlock it after.
			node.predLock.Lock()
			node.predecessor = nil
			node.predLock.Unlock()

			// Lock the successor to read it, and unlock it after.
			node.sucLock.RLock()
			suc := node.successor
			node.sucLock.RUnlock()

			// If successor exists, transfer the old predecessor keys to it, to maintain replication.
			if suc != nil {
				log.Info("Absorbing predecessor's keys.\n")
				// Lock the dictionary to read it, and unlock it after.
				node.dictLock.RLock()
				dictionary, err := node.dictionary.Segment(nil, pred.ID)
				node.dictLock.RUnlock()
				if err != nil {
					log.Error("Error obtaining predecessor keys.\n")
					return
				}

				// Transfer the keys to this node successor.
				err = node.RPC.Extend(suc, &chord.ExtendRequest{Dictionary: dictionary})
				if err != nil {
					log.Error(err.Error() + "Error transferring keys to successor.\n")
					return
				}
				log.Info("Predecessor's keys absorbed. Successful transfer of keys to the successor.\n")
			}
		} else {
			log.Debug("Predecessor alive.\n")
		}
	}
}

// PeriodicallyCheckPredecessor periodically checks whether predecessor has failed.
func (node *Node) PeriodicallyCheckPredecessor() {
	log.Debug("Check predecessor thread started.\n")

	ticker := time.NewTicker(10 * time.Second) // Set the time between routine activations.
	for {
		select {
		case <-ticker.C:
			node.CheckPredecessor() // If it's time, check if this node predecessor it's alive.
		case <-node.shutdown:
			ticker.Stop()
			return
		}
	}
}

// CheckSuccessor checks whether successor has failed.
func (node *Node) CheckSuccessor() {
	log.Debug("Checking successor.\n")

	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successor
	node.sucLock.RUnlock()

	// If successor is not null, check if it is alive.
	if suc != nil {
		err := node.RPC.Check(suc)
		if err != nil {
			log.Error(err.Error() + "Successor failed.\n")
			suc = nil // Set successor temporally to null.
		}
	}

	// If successor is null, take a new successor from the descendents queue.
	if suc == nil {
		// If there are substitute successors.
		if node.descendents.size > 0 {
			log.Info("Substituting successor.\n")

			suc, err := node.descendents.PopBeg() // Take the next successor.
			if err != nil {
				log.Error("Error obtaining new successor.\n")
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
				log.Error("Error obtaining predecessor keys.\n")
				return
			}

			// Transfer the keys to its successor, to update it.
			err = node.RPC.Extend(suc, &chord.ExtendRequest{Dictionary: dictionary})
			if err != nil {
				log.Error(err.Error() + "Error transferring keys to successor.\n")
				return
			}
			log.Info("Successful transfer of keys to the new successor.\n")
		} else {
			// Lock the predecessor to read it, and unlock it after.
			node.predLock.RLock()
			pred := node.predecessor
			node.predLock.RUnlock()

			// If there are no substitute successors, but if there is a predecessor,
			//add the predecessor to the descendant queue.
			if pred != nil {
				err := node.descendents.PushBack(pred) // Add the predecessor to the descendant queue.
				if err != nil {
					log.Error("Error adding descendant.\n")
					return
				}
			}
		}
	} else {
		log.Debug("Successor alive.\n")
	}
}

// PeriodicallyCheckSuccessor periodically checks whether successor has failed.
func (node *Node) PeriodicallyCheckSuccessor() {
	log.Debug("Check successor thread started.\n")

	ticker := time.NewTicker(10 * time.Second) // Set the time between routine activations.
	for {
		select {
		case <-ticker.C:
			node.CheckSuccessor() // If it's time, check if this node successor it's alive.
		case <-node.shutdown:
			ticker.Stop()
			return
		}
	}
}

// FixDescendant fix an entry of the descendents queue.
func (node *Node) FixDescendant(qn *QueueNode[chord.Node]) *QueueNode[chord.Node] {
	log.Debug("Fixing descendant entry.\n")

	node.descLock.RLock()                     // Lock the queue to read it, and unlock it after.
	queue := node.descendents                 // Obtain this node descendents queue.
	desc := qn.value                          // Obtain the descendant in this queue node.
	last := qn == queue.last                  // Verify if this queue node is the last one.
	fulfilled := queue.capacity == queue.size // Verify if the node descendents queue is fulfilled.
	node.descLock.RUnlock()

	// If this queue node is the last one, and the queue is fulfilled, return.
	if last && fulfilled {
		return nil
	}

	suc, err := node.RPC.GetSuccessor(desc) // Otherwise, get the successor of this descendant.
	// If there is an error, then assume this descendant node is dead.
	if err != nil {
		node.descLock.Lock()   // Lock the queue to write on it, and unlock it after.
		err = queue.Remove(qn) // Remove it from the descendents queue.
		prev := qn.prev        // Obtain the previous node of this queue node.
		node.descLock.Unlock()
		if err != nil {
			log.Error("Error fixing descendant entry: actual one is dead and could not be removed from queue.\n")
			return nil
		}
		// Return the previous node of this queue node.
		return prev
	}

	// If the obtained successor is not this node.
	if !Equals(suc.ID, node.ID) {
		// If this queue node is the last one.
		if qn == queue.last {
			node.descLock.Lock()      // Lock the queue to write on it, and unlock it after.
			err = queue.PushBack(suc) // Push this descendant successor in the queue.
			node.descLock.Unlock()
			if err != nil {
				log.Error("Error fixing descendant entry: cannot push this descendant successor to queue.\n")
				return nil
			}
		} else {
			// Otherwise, fix the next node of this queue node.
			node.descLock.Lock() // Lock the queue to write on it, and unlock it after.
			qn.next.value = suc  // Set this descendant successor as value of the next node of this queue node.
			node.descLock.Unlock()
		}
	}

	node.descLock.RLock() // Lock the queue to read it, and unlock it after.
	next := qn.next       // Obtain the next node of this queue node.
	node.descLock.RUnlock()
	// Return the next node of this queue node.
	return next
}

// PeriodicallyFixDescendant periodically fix entries of the descendents queue.
func (node *Node) PeriodicallyFixDescendant() {
	log.Debug("Fix descendant thread started.\n")

	ticker := time.NewTicker(10 * time.Second) // Set the time between routine activations.
	var suc *QueueNode[chord.Node] = nil

	for {
		select {
		case <-ticker.C:
			if suc == nil && node.descendents.size > 0 {
				suc = node.descendents.first
			}
			if suc != nil {
				suc = node.FixDescendant(suc)
			}
		case <-node.shutdown:
			ticker.Stop()
			return
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
