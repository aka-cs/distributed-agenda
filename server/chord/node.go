package chord

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
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

	server   *grpc.Server // Node server.
	sock     *net.TCPListener
	shutdown chan struct{} // Determine if the node server is actually running.

	chord.UnimplementedChordServer
}

// NewNode creates and returns a new Node.
func NewNode(address string, configuration *Configuration) (*Node, error) {
	log.Info("Creating a new node.\n")

	// If configuration is null, report error.
	if configuration == nil {
		message := "Error creating node: configuration cannot be null.\n"
		log.Error(message)
		return nil, errors.New(message)
	}

	id, err := HashKey(address, configuration.Hash) // Obtain the ID relative to this address.
	if err != nil {
		message := "Error creating node: cannot hash node address.\n"
		log.Error(message)
		return nil, errors.New(message + err.Error())
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

	log.Info("Node created with the address " + node.Address + ".\n")

	// Return the node.
	return node, nil
}

// Node server internal methods.

// Start the node server, by registering the server of this node as a chord server, starting
// the transport layer services and the periodically threads that stabilizes the server.
func (node *Node) Start() error {
	log.Info("Starting server...\n")

	// If node server is actually running, report error.
	if IsOpen(node.shutdown) {
		message := "Error starting server: this node server is actually running.\n"
		log.Error(message)
		return errors.New(message)
	}

	node.shutdown = make(chan struct{}) // Report the node server is running.

	// Start listening at correspondent address.
	log.Debug("Trying to listen at the correspondent address.\n")
	listener, err := net.Listen("tcp", node.Address)
	if err != nil {
		message := "Error starting server: cannot listen at the address " + node.Address + ".\n"
		log.Error(message)
		return errors.New(message + err.Error())
	}
	log.Debug("Listening at " + node.Address + ".\n")

	node.successors = NewQueue[chord.Node](node.config.StabilizingNodes) // Create the successors queue.
	node.successors.PushBack(node.Node)                                  // Set this node as its own successor.
	node.predecessor = node.Node                                         // Set this node as it own predecessor.
	node.fingerTable = NewFingerTable(node.config.HashSize)              // Create the finger table.
	node.dictionary = NewDictionary(node.config.Hash)                    // Create the node dictionary.
	node.server = grpc.NewServer(node.config.ServerOpts...)              // Create the node server.
	node.sock = listener.(*net.TCPListener)                              // Save the socket.

	chord.RegisterChordServer(node.server, node) // Register the node server as a chord server.
	log.Debug("Chord services registered.\n")

	err = node.RPC.Start() // Start the RPC (transport layer) services.
	if err != nil {
		message := "Error starting server: cannot start the transport layer.\n"
		log.Error(message)
		return errors.New(message + err.Error())
	}

	// Start serving at the opened socket.
	go node.Listen()

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
	log.Info("Closing server...\n")

	// If node server is not actually running, report error.
	if !IsOpen(node.shutdown) {
		message := "Error stopping server: this node server is actually shutdown.\n"
		log.Error(message)
		return errors.New(message)
	}

	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successors.Beg()
	node.sucLock.RUnlock()

	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// If this node is not its own successor neither predecessor.
	if !Equals(node.ID, suc.ID) && !Equals(node.ID, pred.ID) {
		// Change the predecessor of this node successor to this node predecessor.
		err := node.RPC.SetPredecessor(suc, pred)
		if err != nil {
			message := "Error stopping server: error setting new predecessor of successor at " + suc.Address + ".\n"
			log.Error(message)
			return errors.New(message + err.Error())
		}

		// Change the successor of this node predecessor to this node successor.
		err = node.RPC.SetSuccessor(pred, suc)
		if err != nil {
			message := "Error stopping server: error setting new successor of predecessor at " + pred.Address + ".\n"
			log.Error(message)
			return errors.New(message + err.Error())
		}
	}

	err := node.RPC.Stop() // Stop the RPC (transport layer) services.
	if err != nil {
		message := "Error stopping server: cannot stop the transport layer.\n"
		log.Error(message)
		return errors.New(message + err.Error())
	}

	node.server.Stop() // Stop serving at the opened socket.

	close(node.shutdown) // Report the node server is shutdown.
	log.Info("Server closed.\n")
	return nil
}

// Listen for inbound connections
func (node *Node) Listen() {
	log.Debug("Start serving at the opened socket.\n")
	err := node.server.Serve(node.sock)
	if err != nil {
		log.Error("Cannot serve at " + node.Address + ".\n" + err.Error() + "\n")
		return
	}
}

// Join this node to the Chord ring, using another known node.
// To join the node to the ring, the immediate successor of this node ID in the ring is searched,
// starting from the known node, and the obtained node is taken as the successor of this node.
// The keys corresponding to this node will be transferred by its successor, from the Notify
// method that is called at the end of this method.
func (node *Node) Join(knownNode *chord.Node) error {
	log.Info("Joining to chord ring.\n")

	// If the known node is null, return error: to join this node to the ring,
	// at least one node of the ring must be known.
	if knownNode == nil {
		message := "Error joining to chord ring: known node cannot be null.\n"
		log.Error(message)
		return errors.New(message)
	}

	log.Info("Known node address: " + knownNode.Address + ".\n")

	suc, err := node.RPC.FindSuccessor(knownNode, node.ID) // Find the immediate successor of this node ID.
	if err != nil {
		message := "Error joining to chord ring: cannot find successor of this node ID.\n"
		log.Error(message + err.Error() + "\n")
		return errors.New(message + err.Error())
	}
	// If the obtained node ID is this node ID, then this node is already on the ring.
	if Equals(suc.ID, node.ID) {
		message := "Error joining to chord ring: a node with this ID already exists.\n"
		log.Error(message)
		return errors.New(message)
	}

	// Lock the successor to write on it, and unlock it after.
	node.sucLock.Lock()
	node.successors.PushBeg(suc) // Update this node successor with the obtained node.
	node.sucLock.Unlock()
	log.Info("Successful join. Successor node at " + suc.Address + ".\n")
	return nil
}

// FindIDSuccessor finds the node that succeeds ID.
// To find it, the node with ID smaller than this ID and closest to this ID is searched,
// using the finger table. Then, its successor is found and returned.
func (node *Node) FindIDSuccessor(id []byte) (*chord.Node, error) {
	log.Trace("Finding ID successor.\n")

	// If ID is null, report error.
	if id == nil {
		message := "Error finding successor: ID cannot be null.\n"
		log.Error(message)
		return nil, errors.New(message)
	}

	// Look on the FingerTable to found the closest finger with ID lower than this ID.
	pred := node.ClosestFinger(id)

	// If the corresponding finger is this node, return this node successor.
	if Equals(pred.ID, node.ID) {
		// Lock the successor to read it, and unlock it after.
		node.sucLock.RLock()
		suc := node.successors.Beg()
		node.sucLock.RUnlock()
		return suc, nil
	}

	// If the corresponding finger is different to this node, find the successor of the ID
	// from the remote node obtained.
	suc, err := node.RPC.FindSuccessor(pred, id)
	if err != nil {
		message := "Error finding ID successor from finger at " + pred.Address + ".\n"
		log.Error(message + err.Error() + "\n")
		return nil, errors.New(message + err.Error())
	}
	// Return the obtained successor.
	log.Trace("ID successor found.\n")
	return suc, nil
}

// LocateKey locate the node that corresponds to a given key.
// To locate it, hash the given key to obtain the corresponding ID. Then look for the immediate
// successor of this ID in the ring, since this is the node to which the key corresponds.
func (node *Node) LocateKey(key string) (*chord.Node, error) {
	log.Debug("Locating key: " + key + ".\n")

	id, err := HashKey(key, node.config.Hash) // Obtain the ID relative to this key.
	if err != nil {
		message := "Error locating key.\n"
		log.Error(message)
		return nil, errors.New(message + err.Error())
	}

	suc, err := node.FindIDSuccessor(id) // Find and return the successor of this ID.
	if err != nil {
		message := "Error locating key.\n"
		log.Error(message)
		return nil, errors.New(message + err.Error())
	}

	log.Debug("Successful key location.\n")
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
	log.Trace("Stabilizing node.\n")

	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successors.Beg()
	node.sucLock.RUnlock()

	// If successor is this node, there is no way to stabilize (and sure nothing to stabilize).
	if Equals(suc.ID, node.ID) {
		log.Trace("No stabilization needed.\n")
		return
	}

	candidate, err := node.RPC.GetPredecessor(suc) // Otherwise, obtain the predecessor of the successor.
	if err != nil {
		message := "Error stabilizing node.\nCannot get predecessor of successor at " + suc.Address + ".\n"
		log.Error(message + err.Error() + "\n")
		return
	}

	// If candidate is closer to this node than its current successor, update this node successor
	// with the candidate.
	if Equals(node.ID, suc.ID) || Between(candidate.ID, node.ID, suc.ID) {
		log.Debug("Successor updated to node at " + candidate.Address + ".\n")
		// Lock the successor to write on it, and unlock it after.
		node.sucLock.Lock()
		node.successors.PushBeg(candidate) // Update this node successor with the obtained node.
		suc = candidate
		node.sucLock.Unlock()
	}

	err = node.RPC.Notify(suc, node.Node) // Notify successor about the existence of its predecessor.
	if err != nil {
		message := "Error stabilizing node.\nError notifying successor at " + suc.Address + ".\n"
		log.Error(message + err.Error() + "\n")
		return
	}
	log.Trace("Node stabilized.\n")
}

// PeriodicallyStabilize periodically stabilize the node.
func (node *Node) PeriodicallyStabilize() {
	log.Debug("Stabilize thread started.\n")

	ticker := time.NewTicker(1 * time.Second) // Set the time between routine activations.
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
	log.Trace("Checking predecessor.\n")

	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// If predecessor is not this node, check if it's alive.
	if !Equals(pred.ID, node.ID) {
		err := node.RPC.Check(pred)
		// In case of error, assume predecessor is not alive, and set this node predecessor to this node.
		if err != nil {
			log.Error("Predecessor at " + pred.Address + " failed.\n" + err.Error() + "\n")
			log.Debug("Absorbing predecessor's keys.\n")

			// Lock the predecessor to write on it, and unlock it after.
			node.predLock.Lock()
			node.predecessor = node.Node
			node.predLock.Unlock()

			// Lock the successor to read it, and unlock it after.
			node.sucLock.RLock()
			suc := node.successors.Beg()
			node.sucLock.RUnlock()

			// If successor exists, transfer the old predecessor keys to it, to maintain replication.
			if !Equals(suc.ID, node.ID) {
				// Lock the dictionary to read it, and unlock it after.
				node.dictLock.RLock()
				_, out, err := node.dictionary.Partition(pred.ID, node.ID) // Obtain the predecessor keys.
				node.dictLock.RUnlock()
				if err != nil {
					log.Error("Error obtaining predecessor keys.\n")
					return
				}

				log.Debug("Transferring old predecessor keys to the successor.\n")
				log.Debug(out)
				log.Debug("\n")

				// Transfer the keys to this node successor.
				err = node.RPC.Extend(suc, &chord.ExtendRequest{Dictionary: out})
				if err != nil {
					log.Error(err.Error() + "Error transferring keys to successor.\n")
					return
				}
				log.Debug("Predecessor keys absorbed. Successful transfer of keys to the successor.\n")
			}
		} else {
			log.Trace("Predecessor alive.\n")
		}
	} else {
		log.Trace("There is no predecessor.\n")
	}
}

// PeriodicallyCheckPredecessor periodically checks whether predecessor has failed.
func (node *Node) PeriodicallyCheckPredecessor() {
	log.Debug("Check predecessor thread started.\n")

	ticker := time.NewTicker(1 * time.Second) // Set the time between routine activations.
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
	log.Trace("Checking successor.\n")

	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successors.Beg()
	node.sucLock.RUnlock()

	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// If successor is not this node, check if it is alive.
	if !Equals(suc.ID, node.ID) {
		err := node.RPC.Check(suc)
		// If successor is not alive, substitute the successor.
		if err != nil {
			// Lock the successor to write on it, and unlock it after.
			node.sucLock.Lock()
			node.successors.PopBeg() // Remove the actual successor.
			// Push back this node, to ensure the queue is not empty.
			if node.successors.Empty() {
				node.successors.PushBack(node.Node)
			}
			suc = node.successors.Beg() // Take the next successor in queue.
			node.sucLock.Unlock()
			log.Error("Successor at " + suc.Address + " failed.\n" + err.Error() + "\n")
		} else {
			// If successor is alive, return.
			log.Trace("Successor alive.\n")
			return
		}
	}

	// If there are no successors, but if there is a predecessor, take the predecessor as successor.
	if Equals(suc.ID, node.ID) {
		if !Equals(pred.ID, node.ID) {
			// Lock the successor to write on it, and unlock it after.
			node.sucLock.Lock()
			node.successors.PushBeg(pred)
			suc = node.successors.Beg() // Take the next successor in queue.
			node.sucLock.Unlock()
		} else {
			// If successor still null, there is nothing to do.
			log.Trace("There is no successor.\n")
			return
		}
	}

	// Otherwise, report that there is a new successor.
	log.Debug("Successor updated to node at " + suc.Address + ".\n")

	// Transfer this node keys to the new successor.
	// Lock the dictionary to read it, and unlock it after.
	node.dictLock.RLock()
	in, _, err := node.dictionary.Partition(pred.ID, node.ID) // Obtain this node keys.
	node.dictLock.RUnlock()
	if err != nil {
		log.Error("Error obtaining this node keys.\n")
		return
	}

	log.Debug("Transferring keys to the new successor.\n")
	log.Debug(in)
	log.Debug("\n")

	// Transfer the keys to the new successor, to update it.
	err = node.RPC.Extend(suc, &chord.ExtendRequest{Dictionary: in})
	if err != nil {
		log.Error("Error transferring keys to the new successor.\n" + err.Error() + "\n")
		return
	}
	log.Debug("Successful transfer of keys to the new successor.\n")
}

// PeriodicallyCheckSuccessor periodically checks whether successor has failed.
func (node *Node) PeriodicallyCheckSuccessor() {
	log.Debug("Check successor thread started.\n")

	ticker := time.NewTicker(1 * time.Second) // Set the time between routine activations.
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
func (node *Node) FixDescendant(entry *QueueNode[chord.Node]) *QueueNode[chord.Node] {
	log.Trace("Fixing successor queue entry.\n")

	// If the queue node is null, report error.
	if entry == nil {
		log.Error("Error fixing descendant entry: queue node argument cannot be null.\n")
		return nil
	}

	node.sucLock.RLock() // Lock the queue to read it, and unlock it after.
	value := entry.value // Obtain the successor contained in this queue node.
	next := entry.next
	inside := entry.inside
	fulfilled := node.successors.Fulfilled()
	node.sucLock.RUnlock()

	// If the queue node is not inside the queue, return the next node.
	// If this queue node is the last one, and the queue is fulfilled, return null to restart the fixing cycle.
	if !inside || next == nil && fulfilled {
		log.Trace("Successor queue entry fixed.\n")
		return next
	}

	suc, err := node.RPC.GetSuccessor(value) // Otherwise, get the successor of this successor.
	// If there is an error, then assume this successor is dead.
	if err != nil {
		message := "Error getting successor of successor at " + value.Address + ".\n" +
			"Therefore is assumed dead and removed from the queue of successors.\n"
		log.Error(message + err.Error() + "\n")

		node.sucLock.Lock()           // Lock the queue to write on it, and unlock it after.
		node.successors.Remove(entry) // Remove it from the descendents queue.
		// Push back this node, to ensure the queue is not empty.
		if node.successors.Empty() {
			node.successors.PushBack(node.Node)
		}
		prev := entry.prev // Obtain the previous node of this queue node.
		node.sucLock.Unlock()
		// Return the previous node of this queue node.
		return prev
	}

	node.sucLock.RLock() // Lock the queue to read it, and unlock it after.
	next = entry.next
	inside = entry.inside
	node.sucLock.RUnlock()

	// If the obtained successor is not this node, and is not the same node.
	if !Equals(suc.ID, node.ID) && !Equals(suc.ID, value.ID) {
		// If this queue node still on the queue.
		if inside {
			// If this queue node is the last one, push its successor at the end of queue.
			if next == nil {
				node.sucLock.Lock()           // Lock the queue to write on it, and unlock it after.
				node.successors.PushBack(suc) // Push this successor in the queue.
				node.sucLock.Unlock()
			} else {
				// Otherwise, fix next node of this queue node.
				node.sucLock.Lock() // Lock the queue to write on it, and unlock it after.
				next.value = suc    // Set this successor as value of the next node of this queue node.
				node.sucLock.Unlock()
			}
		} else {
			// Otherwise, skip this node and continue with the next one.
			return next
		}
	} else if Equals(suc.ID, value.ID) {
		// If the node is equal than its successor, skip this node and continue with the next one.
		return next
	} else {
		// Otherwise, if the obtained successor is this node, then the ring has already been turned around,
		// so there are no more successors to add to the queue.
		// Therefore, return null to restart the fixing cycle.
		return nil
	}

	log.Trace("Successor queue entry fixed.\n")
	return next
}

// PeriodicallyFixDescendant periodically fix entries of the descendents queue.
func (node *Node) PeriodicallyFixDescendant() {
	log.Debug("Fix descendant thread started.\n")

	ticker := time.NewTicker(100 * time.Millisecond) // Set the time between routine activations.
	var entry *QueueNode[chord.Node] = nil           // Queue node entry for iterations.

	for {
		select {
		case <-ticker.C:
			// If it's time, fix an entry of the queue.
			// Lock the successor to read it, and unlock it after.
			node.sucLock.RLock()
			suc := node.successors.Beg() // Obtain this node successor.
			node.sucLock.RUnlock()

			// If successor is not this node, then the queue of successors contains at least one successor.
			// Therefore, it needs to be fixed.
			if !Equals(suc.ID, node.ID) {
				// If actual queue node entry is null, restart the fixing cycle,
				// starting at the first queue node.
				if entry == nil {
					// Lock the successor to read it, and unlock it after.
					node.sucLock.RLock()
					entry = node.successors.first
					node.sucLock.RUnlock()
				}
				entry = node.FixDescendant(entry)
			} else {
				// Otherwise, reset the queue node entry.
				entry = nil
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
	log.Trace("Getting node predecessor.\n")

	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// Return the predecessor of this node.
	return pred, nil
}

// GetSuccessor returns the node believed to be the current successor.
func (node *Node) GetSuccessor(ctx context.Context, req *chord.EmptyRequest) (*chord.Node, error) {
	log.Trace("Getting node successor.\n")

	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successors.Beg()
	node.sucLock.RUnlock()

	// Return the successor of this node.
	return suc, nil
}

// SetPredecessor sets the predecessor of this node.
func (node *Node) SetPredecessor(ctx context.Context, candidate *chord.Node) (*chord.EmptyResponse, error) {
	log.Trace("Setting node predecessor.\n")

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
	if !Equals(pred.ID, node.ID) && !Equals(suc.ID, node.ID) {
		log.Trace("Absorbing old predecessor's keys.\n")
		// Lock the dictionary to read it, and unlock it after.
		node.dictLock.RLock()
		_, out, err := node.dictionary.Partition(pred.ID, node.ID) // Obtain the old predecessor keys.
		node.dictLock.RUnlock()
		if err != nil {
			message := "Error obtaining old predecessor keys.\n"
			log.Error(message)
			return emptyResponse, errors.New(message + err.Error())
		}

		log.Debug("Transferring old predecessor keys to the successor.\n")
		log.Debug(out)
		log.Debug("\n")

		// Transfer the old predecessor keys to this node successor.
		err = node.RPC.Extend(suc, &chord.ExtendRequest{Dictionary: out})
		if err != nil {
			message := "Error transferring keys to successor at " + suc.Address + ".\n"
			log.Error(message)
			return emptyResponse, errors.New(message + err.Error())
		}
		log.Trace("Predecessor's keys absorbed. Successful transfer of keys to the successor.\n")
	}

	return emptyResponse, nil
}

// SetSuccessor sets predecessor for this node.
func (node *Node) SetSuccessor(ctx context.Context, candidate *chord.Node) (*chord.EmptyResponse, error) {
	log.Trace("Setting node successor.\n")

	// If the new successor is not this node, update this node successor.
	if !Equals(candidate.ID, node.ID) {
		// Lock the predecessor to read it, and unlock it after.
		node.predLock.RLock()
		pred := node.predecessor
		node.predLock.RUnlock()

		// Lock the successor to write on it, and unlock it after.
		node.sucLock.Lock()
		node.successors.PushBeg(candidate)
		node.sucLock.Unlock()

		// Lock the dictionary to read it, and unlock it after.
		node.dictLock.RLock()
		in, _, err := node.dictionary.Partition(pred.ID, node.ID) // Obtain this node keys.
		node.dictLock.RUnlock()
		if err != nil {
			message := "Error obtaining this node keys.\n"
			log.Error(message)
			return emptyResponse, errors.New(message + err.Error())
		}

		log.Debug("Transferring keys to the new successor.\n")
		log.Debug(in)
		log.Debug("\n")

		// Transfer this node keys to the new successor, to update it.
		err = node.RPC.Extend(candidate, &chord.ExtendRequest{Dictionary: in})
		if err != nil {
			message := "Error transferring keys to the new successor at " + candidate.Address + ".\n"
			log.Error(message)
			return emptyResponse, errors.New(message + err.Error())
		}
		log.Trace("Successful transfer of keys to the new successor.\n")
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
	log.Trace("Checking predecessor notification.\n")

	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// If this node has no predecessor or the predecessor candidate is closer to this node
	// than its current predecessor, update this node predecessor with the candidate.
	if Equals(pred.ID, node.ID) || Between(new.ID, pred.ID, node.ID) {
		log.Debug("Predecessor updated to node at " + new.Address + ".\n")

		// Lock the predecessor to write on it, and unlock it after.
		node.predLock.Lock()
		node.predecessor = new
		node.predLock.Unlock()

		// Transfer to the new predecessor its corresponding keys.
		// Lock the dictionary to read it, and unlock it after.
		node.dictLock.RLock()
		_, out, err := node.dictionary.Partition(new.ID, node.ID) // Obtain the keys to transfer.
		node.dictLock.RUnlock()
		if err != nil {
			message := "Error obtaining the new predecessor corresponding keys.\n"
			log.Error(message)
			return emptyResponse, errors.New(message + err.Error())
		}

		log.Debug("Transferring keys to the new predecessor.\n")
		log.Debug(out)
		log.Debug("\n")

		// Build the new predecessor dictionary, by transferring its correspondent keys.
		err = node.RPC.Extend(new, &chord.ExtendRequest{Dictionary: out})
		if err != nil {
			message := "Error transferring keys to the new predecessor.\n"
			log.Error(message)
			return emptyResponse, errors.New(message + err.Error())
		}

		log.Debug("Successful transfer of keys to the new predecessor.\n")

		// Lock the successor to read it, and unlock it after.
		node.sucLock.RLock()
		suc := node.successors.Beg()
		node.sucLock.RUnlock()

		// If successor exists, delete the transferred keys from successor storage replication.
		if !Equals(suc.ID, node.ID) {
			err = node.RPC.Discard(suc, &chord.DiscardRequest{Keys: Keys(out)})
			if err != nil {
				message := "Error deleting replicated keys on the successor at " + new.Address + ".\n"
				log.Error(message)
				return emptyResponse, errors.New(message + err.Error())
			}

			log.Debug("Successful delete of old predecessor replicated keys in the successor.\n")
			log.Debug(Keys(out))
			log.Debug("\n")
		}

		// If the old predecessor is not this node, delete the old predecessor keys from this node.
		if !Equals(pred.ID, node.ID) {
			// Lock the dictionary to read it, and unlock it after.
			node.dictLock.RLock()
			_, out, err = node.dictionary.Partition(pred.ID, node.ID) // Obtain the keys to transfer.
			node.dictLock.RUnlock()
			if err != nil {
				message := "Error obtaining old predecessor keys replicated on this node.\n"
				log.Error(message)
				return emptyResponse, errors.New(message + err.Error())
			}

			// Lock the dictionary to write on it, and unlock it after.
			node.dictLock.Lock()
			err = node.dictionary.Discard(Keys(out)) // Delete the keys of the old predecessor.
			node.dictLock.Unlock()
			if err != nil {
				message := "Error deleting old predecessor keys on this node.\n"
				log.Error(message)
				return emptyResponse, errors.New(message + err.Error())
			}

			log.Debug(Keys(out))
			log.Debug("\n")
			log.Debug("Successful delete of old predecessor replicated keys in this node.\n")
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
	log.Info("Get: key=" + req.Key + ".\n")

	keyNode := node.Node  // By default, take this node to find the key in the local storage.
	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

	// If the key ID is not between this predecessor node ID and this node ID,
	// then the requested key is not necessarily local.
	if between, err := KeyBetween(req.Key, node.config.Hash, pred.ID, node.ID); !between && err == nil {
		log.Debug("Searching for the corresponding node.\n")
		keyNode, err = node.LocateKey(req.Key) // Locate the node that stores the key.
		if err != nil {
			message := "Error getting key.\n"
			log.Error(message)
			return &chord.GetResponse{}, errors.New(message + err.Error())
		}
	} else if err != nil {
		message := "Error getting key.\n"
		log.Error(message)
		return &chord.GetResponse{}, errors.New(message + err.Error())
	}

	// If the node that stores the key is this node, directly get the associated value from this node storage.
	if Equals(keyNode.ID, node.ID) {
		log.Debug("Resolving get request locally.\n")

		node.dictLock.RLock()                      // Lock the dictionary to read it, and unlock it after.
		value, err := node.dictionary.Get(req.Key) // Get the value associated to this key from storage.
		node.dictLock.RUnlock()
		if err != nil {
			message := "Error getting key.\n"
			log.Error(message)
			return &chord.GetResponse{}, errors.New(message + err.Error())
		}
		log.Info("Successful get.\n")
		// Return the value associated to this key.
		return &chord.GetResponse{Value: value}, nil
	} else {
		log.Info("Redirecting get request to " + keyNode.Address + ".\n")
	}
	// Otherwise, return the result of the remote call on the correspondent node.
	return node.RPC.Get(keyNode, req)
}

// Set a <key, value> pair on storage.
func (node *Node) Set(ctx context.Context, req *chord.SetRequest) (*chord.EmptyResponse, error) {
	log.Info("Set: key=" + req.Key + " value=" + string(req.Value) + ".\n")

	keyNode := node.Node  // By default, take this node to set the <key, value> pair on the local storage.
	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

	// If the request is a replica.
	if req.Replica != nil {
		// If the replica is from our predecessor, resolve it local.
		if Equals(req.Replica, pred.ID) {
			log.Debug("Resolving set request locally (replication).\n")

			node.dictLock.Lock()                    // Lock the dictionary to write on it, and unlock it after.
			node.dictionary.Set(req.Key, req.Value) // Set the <key, value> pair on storage.
			node.dictLock.Unlock()

			log.Info("Successful set.\n")
			return emptyResponse, nil
		} else {
			// Otherwise, ignore it.
			log.Info("Replica from non predecessor node: request ignored.\n")
			return emptyResponse, nil
		}
	}

	// If the key ID is not between this predecessor node ID and this node ID,
	// then the requested key is not necessarily local.
	if between, err := KeyBetween(req.Key, node.config.Hash, pred.ID, node.ID); !between && err == nil {
		log.Debug("Searching for the corresponding node.\n")
		keyNode, err = node.LocateKey(req.Key) // Locate the node that stores the key.
		if err != nil {
			message := "Error setting key.\n"
			log.Error(message)
			return emptyResponse, errors.New(message + err.Error())
		}
	} else if err != nil {
		message := "Error setting key.\n"
		log.Error(message)
		return emptyResponse, errors.New(message + err.Error())
	}

	// If the key corresponds to this node, directly set the <key, value> pair on its storage.
	if Equals(keyNode.ID, node.ID) {
		log.Debug("Resolving set request locally.\n")

		node.dictLock.Lock()                    // Lock the dictionary to write on it, and unlock it after.
		node.dictionary.Set(req.Key, req.Value) // Set the <key, value> pair on storage.
		node.dictLock.Unlock()

		log.Info("Successful set.\n")

		node.sucLock.RLock() // Lock the successor to read it, and unlock it after.
		suc := node.successors.Beg()
		node.sucLock.RUnlock()

		// If successor is not this node, replicate the request to it.
		if !Equals(suc.ID, node.ID) {
			req.Replica = node.ID
			log.Debug("Replicating set request to " + suc.Address + ".\n")
			return emptyResponse, node.RPC.Set(suc, req)
		}
		// Else, return.
		return emptyResponse, nil
	} else {
		log.Info("Redirecting set request to " + keyNode.Address + ".\n")
	}

	// Otherwise, return the result of the remote call on the correspondent node.
	return emptyResponse, node.RPC.Set(keyNode, req)
}

// Delete a <key, value> pair from storage.
func (node *Node) Delete(ctx context.Context, req *chord.DeleteRequest) (*chord.EmptyResponse, error) {
	log.Info("Delete: key=" + req.Key + ".\n")

	keyNode := node.Node  // By default, take this node to delete the <key, value> pair from the local storage.
	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

	// If the request is a replica.
	if req.Replica != nil {
		// If the replica is from our predecessor, resolve it local.
		if Equals(req.Replica, pred.ID) {
			log.Debug("Resolving set request locally (replication).\n")

			node.dictLock.Lock()            // Lock the dictionary to write on it, and unlock it after.
			node.dictionary.Delete(req.Key) // Delete the <key, value> pair from storage.
			node.dictLock.Unlock()

			log.Info("Successful delete.\n")
			return emptyResponse, nil
		} else {
			// Otherwise, ignore it.
			log.Info("Replica from non predecessor node: request ignored.\n")
			return emptyResponse, nil
		}
	}

	// If the key ID is not between this predecessor node ID and this node ID,
	// then the requested key is not necessarily local.
	if between, err := KeyBetween(req.Key, node.config.Hash, pred.ID, node.ID); !between && err == nil {
		log.Debug("Searching for the corresponding node.\n")
		keyNode, err = node.LocateKey(req.Key) // Locate the node that stores the key.
		if err != nil {
			message := "Error deleting key.\n"
			log.Error(message)
			return emptyResponse, errors.New(message + err.Error())
		}
	} else if err != nil {
		message := "Error deleting key.\n"
		log.Error(message)
		return emptyResponse, errors.New(message + err.Error())
	}

	// If the key corresponds to this node, directly delete the <key, value> pair from its storage.
	if Equals(keyNode.ID, node.ID) {
		log.Debug("Resolving delete request locally.\n")

		node.dictLock.Lock()            // Lock the dictionary to write on it, and unlock it at the end of function.
		node.dictionary.Delete(req.Key) // Delete the <key, value> pair from storage.
		node.dictLock.Unlock()

		log.Info("Successful delete.\n")

		node.sucLock.RLock() // Lock the successor to read it, and unlock it after.
		suc := node.successors.Beg()
		node.sucLock.RUnlock()

		// If successor is not this node, replicate the request to it.
		if Equals(suc.ID, node.ID) {
			req.Replica = node.ID
			log.Debug("Replicating delete request to " + suc.Address + ".\n")
			return emptyResponse, node.RPC.Delete(suc, req)
		}
		// Else, return.
		return emptyResponse, nil
	} else {
		log.Info("Redirecting delete request to " + keyNode.Address + ".\n")
	}

	// Otherwise, return the result of the remote call on the correspondent node.
	return emptyResponse, node.RPC.Delete(keyNode, req)
}

// Partition return all <key, values> pairs in a given interval from storage.
func (node *Node) Partition(ctx context.Context, req *chord.EmptyRequest) (*chord.PartitionResponse, error) {
	log.Trace("Getting an interval of keys from local storage dictionary.\n")

	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

	node.dictLock.RLock()                                       // Lock the dictionary to read it, and unlock it after.
	in, out, err := node.dictionary.Partition(pred.ID, node.ID) // Obtain the <key, value> pairs of the storage.
	node.dictLock.RUnlock()
	if err != nil {
		message := "Error getting an interval of keys from storage dictionary.\n"
		log.Error(message)
		return &chord.PartitionResponse{}, errors.New(message + err.Error())
	}
	// Return the dictionary corresponding to the interval.
	return &chord.PartitionResponse{In: in, Out: out}, err
}

// Extend the storage dictionary of this node with a list of <key, values> pairs.
func (node *Node) Extend(ctx context.Context, req *chord.ExtendRequest) (*chord.EmptyResponse, error) {
	log.Trace("Extending local storage dictionary.\n")

	// If there are no keys, return.
	if req.Dictionary == nil || len(req.Dictionary) == 0 {
		return emptyResponse, nil
	}

	node.dictLock.Lock()                          // Lock the dictionary to write on it, and unlock it after.
	err := node.dictionary.Extend(req.Dictionary) // Set the <key, value> pairs on the storage.
	node.dictLock.Unlock()
	if err != nil {
		message := "Error extending storage dictionary.\n"
		log.Error(message)
		return emptyResponse, errors.New(message + err.Error())
	}
	return emptyResponse, err
}

// Discard all <key, values> pairs in a given interval from storage.
func (node *Node) Discard(ctx context.Context, req *chord.DiscardRequest) (*chord.EmptyResponse, error) {
	log.Trace("Discarding an interval of keys from local storage dictionary.\n")

	node.dictLock.Lock()                     // Lock the dictionary to write on it, and unlock it after.
	err := node.dictionary.Discard(req.Keys) // Set the <key, value> pairs on the storage.
	node.dictLock.Unlock()
	if err != nil {
		message := "Error discarding interval of keys from storage dictionary.\n"
		log.Error(message)
		return emptyResponse, errors.New(message + err.Error())
	}
	return emptyResponse, err
}
