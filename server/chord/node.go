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
	*chord.Node // Real node.

	predecessor *chord.Node        // Predecessor of this node in the ring.
	predLock    sync.RWMutex       // Locks the predecessor for reading or writing.
	successors  *Queue[chord.Node] // Queue of successors of this node in the ring.
	sucLock     sync.RWMutex       // Locks the queue of successors for reading or writing.

	fingerTable FingerTable  // FingerTable of this node.
	fingerLock  sync.RWMutex // Locks the finger table for reading or writing.

	RPC    RemoteServices // Transport layer of this node.
	config *Configuration // General configurations.

	dictionary Storage      // Storage dictionary of this node.
	dictLock   sync.RWMutex // Locks the dictionary for reading or writing.

	server   *grpc.Server  // Node server.
	shutdown chan struct{} // Determine if the node server is actually running.

	chord.UnimplementedChordServer
}

// NewNode creates and returns a new Node.
func NewNode(address string, configuration *Configuration) (*Node, error) {
	// If configuration is null, report error.
	if configuration == nil {
		message := "Error creating node.\nInvalid configuration: configuration cannot be null.\n"
		log.Error(message)
		return nil, errors.New(message)
	}

	id, err := HashKey(address, configuration.Hash) // Obtain the ID relative to this address.
	if err != nil {
		message := "Error creating node.\n"
		log.Error(message)
		return nil, errors.New(err.Error() + message)
	}

	// Creates the new node with the obtained ID and same address.
	innerNode := chord.Node{ID: id, Address: address}

	transport := NewGRPCServices(configuration) // Creates the transport layer.

	// Instantiates the node.
	node := &Node{Node: &innerNode,
		predecessor: nil,
		successors:  nil,
		fingerTable: nil,
		RPC:         transport,
		config:      configuration,
		dictionary:  nil,
		server:      nil,
		shutdown:    nil}

	// Return the node.
	return node, nil
}

// Node server internal methods.

// Start the node server, by registering the server of this node as a chord server, starting
// the transport layer services and the periodically threads that stabilizes the server.
func (node *Node) Start() error {
	// If node server is actually running, report error.
	if IsOpen(node.shutdown) {
		message := "Error starting server: this node server is actually running.\n"
		log.Error(message)
		return errors.New(message)
	}

	node.shutdown = make(chan struct{}) // Report the node server is running.
	log.Info("Starting server...\n")

	node.successors = NewQueue[chord.Node](node.config.StabilizingNodes) // Create the successors queue.
	node.fingerTable = NewFingerTable(node.Node, node.config.HashSize)   // Create the finger table.
	node.dictionary = NewDictionary(node.config.Hash)                    // Create the node dictionary.
	node.server = grpc.NewServer(node.config.ServerOpts...)              // Create the node server.

	chord.RegisterChordServer(node.server, node) // Register the node server as a chord server.
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

// Stop the node server, by stopping the transport layer services and reporting the node
// services are now shutdown, to make the periodic threads stop themselves eventually.
// Then, connects this node successor and predecessor directly, thus leaving the ring.
// It is not necessary to deal with the transfer of keys for the maintenance of replication,
// since the methods used to connect the nodes (SetSuccessor and SetPredecessor) will take care of this.
func (node *Node) Stop() error {
	// If node server is not actually running, report error.
	if !IsOpen(node.shutdown) {
		message := "Error stopping server: this node server is actually shutdown.\n"
		log.Error(message)
		return errors.New(message)
	}

	log.Info("Closing server...\n")

	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successors.Beg()
	node.sucLock.RUnlock()

	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// Change the predecessor of this node successor to this node predecessor.
	if suc != nil {
		err := node.RPC.SetPredecessor(suc, pred)
		if err != nil {
			message := "Error setting new predecessor of this node successor.\nError stopping server.\n"
			log.Error(message)
			return errors.New(err.Error() + message)
		}
	}

	// Change the successor of this node predecessor to this node successor.
	if pred != nil {
		err := node.RPC.SetSuccessor(pred, suc)
		if err != nil {
			message := "Error setting new successor of this node predecessor.\nError stopping server.\n"
			log.Error(message)
			return errors.New(err.Error() + message)
		}
	}

	node.successors = nil  // Delete the successors queue.
	node.fingerTable = nil // Delete the finger table.
	node.dictionary = nil  // Delete the node dictionary.
	node.server = nil      // Delete the node server.

	node.RPC.Stop()      // Stop the RPC (transport layer) services.
	close(node.shutdown) // Report the node server is shutdown.
	log.Info("Server closed.\n")
	return nil
}

// Join this node to the Chord ring, using another known node.
// To join the node to the ring, the immediate successor of this node ID in the ring is searched,
// starting from the known node, and the obtained node is taken as the successor of this node.
// The keys corresponding to this node will be transferred by its successor, from the Notify
// method that is called at the end of this method.
func (node *Node) Join(knownNode *chord.Node) error {
	log.Info("Joining new node to chord ring.\n")

	// If the known node is null, return error: to join this node to the ring,
	// at least one node of the ring must be known.
	if knownNode == nil {
		message := "Invalid argument, known node cannot be null.\nError joining node to chord ring.\n"
		log.Error(message)
		return errors.New(message)
	}

	suc, err := node.RPC.FindSuccessor(knownNode, node.ID) // Find the immediate successor of this node ID.
	if err != nil {
		message := "Error joining node to chord ring.\n"
		log.Error(err.Error() + message)
		return errors.New(err.Error() + message)
	}
	// If the obtained node ID is this node ID, then this node is already on the ring.
	if Equals(suc.ID, node.ID) {
		message := "Error joining node to chord ring: a node with this ID already exists.\n"
		log.Error(message)
		return errors.New(message)
	}

	// Lock the successor to write on it, and unlock it after.
	node.sucLock.Lock()
	// If queue is fulfilled, pop the last element, to gain queue space for the new successor.
	if node.successors.Fulfilled() {
		node.successors.PopBack()
	}
	node.successors.PushBeg(suc) // Update this node successor with the obtained node.
	node.sucLock.Unlock()
	log.Info("Successful join of the node.\n")
	return nil
}

// FindIDSuccessor finds the node that succeeds ID.
// To find it, the node with ID smaller than this ID and closest to this ID is searched,
// using the finger table. Then, its successor is found and returned.
func (node *Node) FindIDSuccessor(id []byte) (*chord.Node, error) {
	log.Debug("Finding ID successor.\n")

	// If ID is null, report error.
	if id == nil {
		message := "Invalid ID: ID cannot be null.\n"
		log.Error(message)
		return nil, errors.New(message)
	}

	// Look on the FingerTable to found the closest finger with ID lower than this ID.
	node.fingerLock.RLock()        // Lock the FingerTable to read it, and unlock it after.
	pred := node.ClosestFinger(id) // Find the successor of this ID in the FingerTable.
	node.fingerLock.RUnlock()

	// If the corresponding finger is this node, return this node successor.
	if Equals(pred.ID, node.ID) {
		// Lock the successor to read it, and unlock it after.
		node.sucLock.RLock()
		suc := node.successors.Beg()
		node.sucLock.RUnlock()

		// If the successor is null, return this node.
		if suc == nil {
			return node.Node, nil
		}
		// Otherwise, return this node successor.
		return suc, nil
	}

	// If the corresponding finger is different to this node, find the successor of the ID
	// from the remote node obtained.
	suc, err := node.RPC.FindSuccessor(pred, id)
	if err != nil {
		message := "Error finding ID successor.\n"
		log.Error(err.Error() + message)
		return nil, errors.New(err.Error() + message)
	}
	// Return the obtained successor.
	log.Debug("ID successor found.\n")
	return suc, nil
}

// LocateKey locate the node that corresponds to a given key.
// To locate it, hash the given key to obtain the corresponding ID. Then look for the immediate
// successor of this ID in the ring, since this is the node to which the key corresponds.
func (node *Node) LocateKey(key string) (*chord.Node, error) {
	log.Info("Locating key.\n")

	id, err := HashKey(key, node.config.Hash) // Obtain the ID relative to this key.
	if err != nil {
		message := "Error locating key.\n"
		log.Error(message)
		return nil, errors.New(err.Error() + message)
	}

	suc, err := node.FindIDSuccessor(id) // Find and return the successor of this ID.
	if err != nil {
		message := "Error locating key.\n"
		log.Error(message)
		return nil, errors.New(err.Error() + message)
	}

	log.Info("Successful key location.\n")
	return suc, nil
}

// Stabilize this node.
// To stabilize the node, the predecessor of the successor of this node is searched for.
// If the obtained node is not this node, and it's closer to this node than its current successor,
// then update this node taking the obtained node as the new successor.
// Finally, notifies its new successor of this node existence, so that the successor will update itself.
// The transfer of keys from this node to its successor to maintain replication is not necessary, since
// the new successor at some point was a predecessor of the old successor, and received from it
// the replicated keys of this node.
func (node *Node) Stabilize() {
	log.Debug("Stabilizing node.\n")

	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successors.Beg()
	node.sucLock.RUnlock()

	// If successor is null, there is no way to stabilize (and sure nothing to stabilize).
	if suc == nil {
		log.Debug("No stabilization needed.\n")
		return
	}

	candidate, err := node.RPC.GetPredecessor(suc) // Otherwise, obtain the predecessor of the successor.
	if err != nil {
		log.Error(err.Error() + "Error stabilizing node.\n")
	}

	// If candidate is not null, and it's closer to this node than its current successor,
	// update this node successor with the candidate.
	if candidate != nil && Between(candidate.ID, node.ID, suc.ID, false, false) {
		// Lock the successor to write on it, and unlock it after.
		node.sucLock.Lock()
		// If queue is fulfilled, pop the last element, to gain queue space for the new successor.
		if node.successors.Fulfilled() {
			node.successors.PopBack()
		}
		node.successors.PushBeg(candidate) // Update this node successor with the obtained node.
		node.sucLock.Unlock()
	}

	err = node.RPC.Notify(suc, node.Node) // Notify successor about the existence of its predecessor.
	if err != nil {
		log.Error(err.Error() + "Error stabilizing node.\n")
		return
	}
	log.Debug("Node stabilized.\n")
}

// PeriodicallyStabilize periodically stabilize the node.
func (node *Node) PeriodicallyStabilize() {
	log.Debug("Periodically stabilize thread started.\n")

	ticker := time.NewTicker(10 * time.Second) // Set the time between routine activations.
	for {
		select {
		case <-ticker.C:
			node.Stabilize() // If it's time, stabilize the node.
		case <-node.shutdown:
			ticker.Stop() // If node server is shutdown, stop the thread.
			return
		}
	}
}

// CheckPredecessor checks whether predecessor has failed.
// To do this, make a remote Check call to the predecessor. If the call fails, the predecessor
// is assumed dead, and it's updated to null.
// In this case, the keys of the predecessor are absorbed by the current node (these keys are currently
// already replicated on it). Accordingly, the new keys are also sent to the successor of this node,
// to maintain replication.
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
			suc := node.successors.Beg()
			node.sucLock.RUnlock()

			// If successor exists, transfer the old predecessor keys to it, to maintain replication.
			if suc != nil {
				log.Info("Absorbing predecessor's keys.\n")
				// Lock the dictionary to read it, and unlock it after.
				node.dictLock.RLock()
				dictionary, err := node.dictionary.Segment(nil, pred.ID) // Obtain the predecessor keys.
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
	} else {
		log.Debug("There is no predecessor.\n")
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
			ticker.Stop() // If node server is shutdown, stop the thread.
			return
		}
	}
}

// CheckSuccessor checks whether successor has failed.
// To do this, make a remote Check call to the successor. If the call fails, the successor
// is assumed dead, and it's removed from the queue of successors.
// Then, it's necessary to replace it. For this, verify that the queue of successors is not empty now and,
// in this case, the first element on it is taken as the new successor.
// As a special case, if the queue of successors is empty, but this node has a predecessor,
// then the predecessor is taken as the new successor (to allow chord rings of size two).
// It is necessary to transfer the keys of this node to its new successor, because this new successor
// only has its own keys and those that corresponded to the old successor.
func (node *Node) CheckSuccessor() {
	log.Debug("Checking successor.\n")

	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successors.Beg()
	node.sucLock.RUnlock()

	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// If successor is not null, check if it is alive.
	if suc != nil {
		err := node.RPC.Check(suc)
		// If successor is not alive, substitute the successor.
		if err != nil {
			// Lock the successor to write on it, and unlock it after.
			node.sucLock.Lock()
			node.successors.PopBeg()    // Remove the actual successor.
			suc = node.successors.Beg() // Take the next successor in queue.
			node.sucLock.Unlock()
			log.Error(err.Error() + "Successor failed.\n")
		} else {
			// If successor is alive, return.
			log.Debug("Successor alive.\n")
			return
		}
	}

	// If there are no successors, but if there is a predecessor, take the predecessor as successor.
	if suc == nil {
		if pred != nil {
			// Lock the successor to write on it, and unlock it after.
			node.sucLock.Lock()
			node.successors.PushBeg(pred)
			suc = node.successors.Beg() // Take the next successor in queue.
			node.sucLock.Unlock()
		} else {
			// If successor still null, there is nothing to do.
			log.Debug("There is no successor.\n")
			return
		}
	}

	// Otherwise, report that there is a new successor.
	log.Info("Successor updated.\n")

	// Transfer this node keys to the new successor.
	var predID []byte = nil // Obtain this node predecessor ID.
	if pred != nil {
		predID = pred.ID
	}

	// Lock the dictionary to read it, and unlock it after.
	node.dictLock.RLock()
	dictionary, err := node.dictionary.Segment(predID, nil) // Obtain this node keys.
	node.dictLock.RUnlock()
	if err != nil {
		log.Error("Error obtaining this node keys.\n")
		return
	}

	// Transfer the keys to the new successor, to update it.
	err = node.RPC.Extend(suc, &chord.ExtendRequest{Dictionary: dictionary})
	if err != nil {
		log.Error(err.Error() + "Error transferring keys to the new successor.\n")
		return
	}
	log.Info("Successful transfer of keys to the new successor.\n")
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
			ticker.Stop() // If node server is shutdown, stop the thread.
			return
		}
	}
}

// FixDescendant fix an entry of the queue of successors.
// Given a node of the successor queue, gets the reference to a remote node it contains and
// make a remote call to GetSuccessor to get its successor.
// If the call fails, assume the remote node is dead, and it's removed from the queue of successors.
// In this case, return the previous node of this queue node, to fix this entry later.
// Otherwise, fix the next entry, updating its value with the obtained successor,
// and return the next node of this queue node.
func (node *Node) FixDescendant(qn *QueueNode[chord.Node]) *QueueNode[chord.Node] {
	log.Debug("Fixing descendant entry.\n")

	// If the queue node is null, report error.
	if qn == nil {
		log.Error("Error fixing descendant entry.\nInvalid queue node: queue node cannot be null.\n")
		return nil
	}

	node.sucLock.RLock()                      // Lock the queue to read it, and unlock it after.
	queue := node.successors                  // Obtain the queue of successors of this node.
	desc := qn.value                          // Obtain the successor contained in this queue node.
	last := qn == queue.last                  // Verify if this queue node is the last one.
	fulfilled := queue.capacity == queue.size // Verify if the queue of successors of this node is fulfilled.
	node.sucLock.RUnlock()

	// If this queue node is the last one, and the queue is fulfilled, return null to restart the fixing cycle.
	if last && fulfilled {
		return nil
	}

	suc, err := node.RPC.GetSuccessor(desc) // Otherwise, get the successor of this successor.
	// If there is an error, then assume this successor is dead.
	if err != nil {
		log.Error(err.Error() + "Error getting successor of this successor.\n" +
			"Therefore is assumed dead and removed from the queue of successors.\n")

		node.sucLock.Lock() // Lock the queue to write on it, and unlock it after.
		queue.Remove(qn)    // Remove it from the descendents queue.
		prev := qn.prev     // Obtain the previous node of this queue node.
		node.sucLock.Unlock()
		// Return the previous node of this queue node.
		return prev
	}

	// If the obtained successor is not this node.
	if !Equals(suc.ID, node.ID) {
		// If this queue node is the last one, push it at the end of queue.
		if qn == queue.last {
			node.sucLock.Lock() // Lock the queue to write on it, and unlock it after.
			queue.PushBack(suc) // Push this successor in the queue.
			node.sucLock.Unlock()
		} else {
			// Otherwise, fix next node of this queue node.
			node.sucLock.Lock() // Lock the queue to write on it, and unlock it after.
			qn.next.value = suc // Set this successor as value of the next node of this queue node.
			node.sucLock.Unlock()
		}
	} else {
		// Otherwise, if the obtained successor is this node, then the ring has already been turned around,
		// so there are no more successors to add to the queue.
		// Therefore, return null to restart the fixing cycle.
		return nil
	}

	// Return the next node of this queue node.
	node.sucLock.RLock() // Lock the queue to read it, and unlock it after.
	next := qn.next      // Obtain the next node of this queue node.
	node.sucLock.RUnlock()
	return next
}

// PeriodicallyFixDescendant periodically fix entries of the descendents queue.
func (node *Node) PeriodicallyFixDescendant() {
	log.Debug("Fix descendant thread started.\n")

	ticker := time.NewTicker(10 * time.Second) // Set the time between routine activations.
	var qn *QueueNode[chord.Node] = nil

	for {
		select {
		case <-ticker.C:
			// If it's time, and the queue of successors of this node is not empty,
			// fix an entry of the queue.
			if node.successors.size > 0 {
				// If actual queue node entry is null, restart the fixing cycle,
				// starting at the first queue node.
				if qn == nil {
					qn = node.successors.first
				}
				qn = node.FixDescendant(qn)
			}
		case <-node.shutdown:
			ticker.Stop() // If node server is shutdown, stop the thread.
			return
		}
	}
}

// Node server chord methods.

// GetPredecessor returns the node believed to be the current predecessor.
func (node *Node) GetPredecessor(ctx context.Context, req *chord.EmptyRequest) (*chord.Node, error) {
	log.Debug("Getting node predecessor.\n")

	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// Return the predecessor of this node.
	return pred, nil
}

// GetSuccessor returns the node believed to be the current successor.
func (node *Node) GetSuccessor(ctx context.Context, req *chord.EmptyRequest) (*chord.Node, error) {
	log.Debug("Getting node successor.\n")

	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successors.Beg()
	node.sucLock.RUnlock()

	// Return the successor of this node.
	return suc, nil
}

// SetPredecessor sets the predecessor of this node.
func (node *Node) SetPredecessor(ctx context.Context, candidate *chord.Node) (*chord.EmptyResponse, error) {
	log.Debug("Setting node predecessor.\n")

	// If the predecessor to set is this node, set predecessor to null.
	if candidate != nil && Equals(candidate.ID, node.ID) {
		candidate = nil
	}

	// Lock the predecessor to read and write on it, and unlock it after.
	node.predLock.Lock()
	pred := node.predecessor
	node.predecessor = candidate
	node.predLock.Unlock()

	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successors.Beg()
	node.sucLock.RUnlock()

	// If successor exists, transfer the old predecessor keys to it, to maintain replication.
	if pred != nil && suc != nil {
		log.Info("Absorbing predecessor's keys.\n")
		// Lock the dictionary to read it, and unlock it after.
		node.dictLock.RLock()
		dictionary, err := node.dictionary.Segment(nil, pred.ID)
		node.dictLock.RUnlock()
		if err != nil {
			log.Error("Error obtaining predecessor keys.\n")
			return emptyResponse, nil
		}

		// Transfer the old predecessor keys to this node successor.
		err = node.RPC.Extend(suc, &chord.ExtendRequest{Dictionary: dictionary})
		if err != nil {
			log.Error(err.Error() + "Error transferring keys to successor.\n")
			return emptyResponse, nil
		}
		log.Info("Predecessor's keys absorbed. Successful transfer of keys to the successor.\n")
	}

	return emptyResponse, nil
}

// SetSuccessor sets predecessor for this node.
func (node *Node) SetSuccessor(ctx context.Context, candidate *chord.Node) (*chord.EmptyResponse, error) {
	log.Debug("Setting node successor.\n")

	// If the successor to set is this node, set successor to null.
	if candidate != nil && Equals(candidate.ID, node.ID) {
		candidate = nil
	}

	// If the new successor is not null, update this node successor.
	if candidate != nil {
		// Lock the predecessor to read it, and unlock it after.
		node.predLock.RLock()
		pred := node.predecessor
		node.predLock.RUnlock()

		// Lock the successor to write on it, and unlock it after.
		node.sucLock.Lock()
		// If queue is fulfilled, pop the last element, to gain queue space for the new successor.
		if node.successors.Fulfilled() {
			node.successors.PopBack()
		}
		node.successors.PushBeg(candidate)
		node.sucLock.Unlock()

		// Transfer this node keys to the new successor.
		var predID []byte = nil // Obtain this node predecessor ID.
		if pred != nil {
			predID = pred.ID
		}

		// Lock the dictionary to read it, and unlock it after.
		node.dictLock.RLock()
		dictionary, err := node.dictionary.Segment(predID, nil) // Obtain this node keys.
		node.dictLock.RUnlock()
		if err != nil {
			log.Error("Error obtaining this node keys.\n")
			return emptyResponse, nil
		}

		// Transfer this node keys to the new successor, to update it.
		err = node.RPC.Extend(candidate, &chord.ExtendRequest{Dictionary: dictionary})
		if err != nil {
			log.Error(err.Error() + "Error transferring keys to the new successor.\n")
			return emptyResponse, nil
		}
		log.Info("Successful transfer of keys to the new successor.\n")
	}

	return emptyResponse, nil
}

// FindSuccessor finds the node that succeeds ID.
func (node *Node) FindSuccessor(ctx context.Context, id *chord.ID) (*chord.Node, error) {
	// Find the successor of this ID.
	return node.FindIDSuccessor(id.ID)
}

// Notify this node that it possibly have a new predecessor.
func (node *Node) Notify(ctx context.Context, new *chord.Node) (*chord.EmptyResponse, error) {
	log.Debug("Checking predecessor notification.\n")

	// If candidate for new predecessor is null, report error.
	if new == nil {
		message := "Candidate for new predecessor cannot be null.\n"
		log.Error(message)
		return nil, errors.New(message)
	}

	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// If this node has no predecessor or the predecessor candidate is closer to this node
	// than its current predecessor, update this node predecessor with the candidate.
	if pred == nil || Between(new.ID, pred.ID, node.ID, false, false) {
		log.Debug("Updating predecessor.\n")

		// Lock the predecessor to write on it, and unlock it after.
		node.predLock.Lock()
		node.predecessor = new
		node.predLock.Unlock()

		// Lock the successor to read it, and unlock it after.
		node.sucLock.RLock()
		suc := node.successors.Beg()
		node.sucLock.RUnlock()

		// Transfer to the new predecessor its corresponding keys.
		// Lock the dictionary to read it, and unlock it after.
		node.dictLock.RLock()
		dictionary, err := node.dictionary.Segment(nil, new.ID) // Obtain the keys to transfer.
		node.dictLock.RUnlock()
		if err != nil {
			message := "Error obtaining the new predecessor its corresponding keys.\n"
			return nil, errors.New(err.Error() + message)
		}

		// Build the new predecessor dictionary, by transferring its correspondent keys.
		err = node.RPC.Extend(new, &chord.ExtendRequest{Dictionary: dictionary})
		if err != nil {
			message := "Error transferring keys to the new predecessor.\n"
			return nil, errors.New(err.Error() + message)
		}

		// Delete the transferred keys from successor storage replication.
		err = node.RPC.Detach(suc, &chord.DetachRequest{L: nil, R: new.ID})
		if err != nil {
			message := "Error deleting replicated keys on the successor.\n"
			return nil, errors.New(err.Error() + message)
		}

		// If the old predecessor is not null, delete the old predecessor keys from this node.
		if pred != nil {
			// Lock the dictionary to write on it, and unlock it after.
			node.dictLock.Lock()
			err = node.dictionary.Detach(nil, pred.ID) // Delete the keys of the old predecessor.
			node.dictLock.Unlock()
			if err != nil {
				message := "Error deleting old predecessor keys on this node.\n"
				return nil, errors.New(err.Error() + message)
			}
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
	log.Debug("Getting key associated value.\n")

	// If the get request is null, report error.
	if req == nil {
		message := "Get request cannot be null.\n"
		log.Error(message)
		return nil, errors.New(message)
	}

	keyID, err := HashKey(req.Key, node.config.Hash) // Obtain the correspondent ID of the key.
	if err != nil {
		message := "Error getting key.\n"
		log.Error(message)
		return nil, errors.New(err.Error() + message)
	}

	keyNode := node.Node  // By default, take this node to find the key in the local storage.
	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

	// If the key ID is not between this predecessor node ID and this node ID,
	// then the requested key is not necessarily local.
	if pred == nil || Between(keyID, pred.ID, node.ID, false, true) {
		keyNode, err = node.LocateKey(req.Key) // Locate the node that stores the key.
		if err != nil {
			message := "Error getting key.\n"
			log.Error(message)
			return nil, errors.New(err.Error() + message)
		}
	}

	// If the node that stores the key is this node, directly get the associated value from this node storage.
	if Equals(keyNode.ID, node.ID) {
		node.dictLock.RLock()                      // Lock the dictionary to read it, and unlock it after.
		value, err := node.dictionary.Get(req.Key) // Get the value associated to this key from storage.
		node.dictLock.RUnlock()
		if err != nil {
			message := "Error getting key.\n"
			log.Error(message)
			return nil, errors.New(err.Error() + message)
		}
		// Return the value associated to this key.
		return &chord.GetResponse{Value: value}, nil
	}
	// Otherwise, return the result of the remote call on the correspondent node.
	return node.RPC.Get(keyNode, req)
}

// Set a <key, value> pair on storage.
func (node *Node) Set(ctx context.Context, req *chord.SetRequest) (*chord.EmptyResponse, error) {
	log.Debug("Setting a <key, value> pair.\n")

	// If the get request is null, report error.
	if req == nil {
		message := "Get request cannot be null.\n"
		log.Error(message)
		return nil, errors.New(message)
	}

	keyID, err := HashKey(req.Key, node.config.Hash) // Obtain the correspondent ID of the key.
	if err != nil {
		message := "Error getting key.\n"
		log.Error(message)
		return nil, errors.New(err.Error() + message)
	}

	// If this request is a replica, resolve it local.
	if req.Replica {
		node.dictLock.Lock()                    // Lock the dictionary to write on it, and unlock it after.
		node.dictionary.Set(req.Key, req.Value) // Set the <key, value> pair on storage.
		node.dictLock.Unlock()
		return emptyResponse, nil
	}

	keyNode := node.Node  // By default, take this node to set the <key, value> pair on the local storage.
	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

	// If the key ID is not between this predecessor node ID and this node ID,
	// then the requested key is not necessarily local.
	if pred == nil || Between(keyID, pred.ID, node.ID, false, true) {
		keyNode, err = node.LocateKey(req.Key) // Locate the node to which that key corresponds.
		if err != nil {
			if err != nil {
				message := "Error setting key.\n"
				log.Error(message)
				return nil, errors.New(err.Error() + message)
			}
		}
	}

	// If the key corresponds to this node, directly set the <key, value> pair on its storage.
	if Equals(keyNode.ID, node.ID) {
		node.dictLock.Lock()                    // Lock the dictionary to write on it, and unlock it after.
		node.dictionary.Set(req.Key, req.Value) // Set the <key, value> pair on storage.
		node.dictLock.Unlock()

		node.sucLock.RLock() // Lock the successor to read it, and unlock it after.
		suc := node.successors.Beg()
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
		suc := node.successors.Beg()
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
