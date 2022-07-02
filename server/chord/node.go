package chord

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"math/big"
	"net"
	"server/chord/chord"
	"sync"
)

// Node represents a Chord ring single node.
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

	server   *grpc.Server     // Node server.
	sock     *net.TCPListener // Node server listener socket.
	shutdown chan struct{}    // Determine if the node server is actually running.

	chord.UnimplementedChordServer
}

// NewNode creates and returns a new Node.
func NewNode(port string, configuration *Configuration, transport RemoteServices, storage Storage) (*Node, error) {
	log.Info("Creating a new node.\n")

	// If configuration is null, report error.
	if configuration == nil {
		message := "Error creating node: configuration cannot be null.\n"
		log.Error(message)
		return nil, errors.New(message)
	}

	// Creates the new node with the obtained ID and same address.
	innerNode := &chord.Node{ID: big.NewInt(0).Bytes(), IP: "0.0.0.0:", Port: port}

	// Instantiates the node.
	node := &Node{Node: innerNode,
		predecessor: nil,
		successors:  nil,
		fingerTable: nil,
		RPC:         transport,
		config:      configuration,
		dictionary:  storage,
		server:      nil,
		shutdown:    nil}

	log.Info("Node created with the address " + node.IP + ".\n")

	// Return the node.
	return node, nil
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
	log.Trace("Setting node predecessor to " + candidate.IP + ".\n")

	// If the new predecessor is not this node, update this node successor.
	if !Equals(candidate.ID, node.ID) {
		// Lock the predecessor to read and write on it, and unlock it after.
		node.predLock.Lock()
		pred := node.predecessor
		node.predecessor = candidate
		node.predLock.Unlock()

		// Lock the successor to read it, and unlock it after.
		node.sucLock.RLock()
		suc := node.successors.Beg()
		node.sucLock.RUnlock()

		// If there was an old successor, and successor exists, transfer the old predecessor keys to
		// this node successor, to maintain replication.
		if !Equals(pred.ID, node.ID) && !Equals(suc.ID, node.ID) {
			log.Trace("Absorbing old predecessor's keys.\n")
			// Lock the dictionary to read it, and unlock it after.
			node.dictLock.RLock()
			in, out, err := node.dictionary.Partition(pred.ID, node.ID) // Obtain the old predecessor keys.
			node.dictLock.RUnlock()
			if err != nil {
				message := "Error obtaining old predecessor keys.\n"
				log.Error(message)
				return emptyResponse, errors.New(message + err.Error())
			}

			/////
			log.Debug("Transferring old predecessor keys to the successor.\n")
			log.Debug("Out:\n")
			log.Debug(out)
			log.Debug("In:\n")
			log.Debug(in)
			log.Debug("\n")

			// Transfer the old predecessor keys to this node successor.
			err = node.RPC.Extend(suc, &chord.ExtendRequest{Dictionary: out})
			if err != nil {
				message := "Error transferring keys to successor at " + suc.IP + ".\n"
				log.Error(message)
				return emptyResponse, errors.New(message + err.Error())
			}
			log.Trace("Predecessor's keys absorbed. Successful transfer of keys to the successor.\n")
		}
	} else {
		log.Trace("Candidate predecessor is this same node. Update refused.\n")
	}

	return emptyResponse, nil
}

// SetSuccessor sets the successor of this node.
func (node *Node) SetSuccessor(ctx context.Context, candidate *chord.Node) (*chord.EmptyResponse, error) {
	log.Trace("Setting node successor to " + candidate.IP + ".\n")

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
		in, out, err := node.dictionary.Partition(pred.ID, node.ID) // Obtain this node keys.
		node.dictLock.RUnlock()
		if err != nil {
			message := "Error obtaining this node keys.\n"
			log.Error(message)
			return emptyResponse, errors.New(message + err.Error())
		}

		log.Debug("Transferring this node keys to the new successor.\n")
		log.Debug("In:\n")
		log.Debug(in)
		log.Debug("Out:\n")
		log.Debug(out)
		log.Debug("\n")

		// Transfer this node keys to the new successor, to update it.
		err = node.RPC.Extend(candidate, &chord.ExtendRequest{Dictionary: in})
		if err != nil {
			message := "Error transferring keys to the new successor at " + candidate.IP + ".\n"
			log.Error(message)
			return emptyResponse, errors.New(message + err.Error())
		}
		log.Trace("Successful transfer of keys to the new successor.\n")
	} else {
		log.Trace("Candidate successor is this same node. Update refused.\n")
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
		log.Debug("Predecessor updated to node at " + new.IP + ".\n")

		// Lock the predecessor to write on it, and unlock it after.
		node.predLock.Lock()
		node.predecessor = new
		node.predLock.Unlock()

		// Transfer the corresponding keys to the new predecessor.
		// Lock the dictionary to read it, and unlock it after.
		node.dictLock.RLock()
		in, out, err := node.dictionary.Partition(new.ID, node.ID) // Obtain the keys to transfer.
		node.dictLock.RUnlock()
		if err != nil {
			message := "Error obtaining the new predecessor corresponding keys.\n"
			log.Error(message)
			return emptyResponse, errors.New(message + err.Error())
		}

		log.Debug("Transferring keys to the new predecessor.\n")
		log.Debug("Out:\n")
		log.Debug(out)
		log.Debug("In:\n")
		log.Debug(in)
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
				message := "Error deleting replicated keys on the successor at " + new.IP + ".\n"
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
			_, out, err = node.dictionary.Partition(pred.ID, node.ID) // Obtain the keys to delete.
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

// Get the value associated to a key.
func (node *Node) Get(ctx context.Context, req *chord.GetRequest) (*chord.GetResponse, error) {
	log.Info("Get: key=" + req.Key + ".\n")

	keyNode := node.Node  // By default, take this node to get the value of this key from the local storage.
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
		log.Info("Redirecting get request to " + keyNode.IP + ".\n")
	}
	// Otherwise, return the result of the remote call on the correspondent node.
	return node.RPC.Get(keyNode, req)
}

// Set a <key, value> pair on storage.
func (node *Node) Set(ctx context.Context, req *chord.SetRequest) (*chord.EmptyResponse, error) {
	log.Info("Set: key=" + req.Key + " value=" + string(req.Value) + ".\n")

	// If this request is a replica, resolve it local.
	if req.Replica {
		log.Debug("Resolving set request locally (replication).\n")

		node.dictLock.Lock()                    // Lock the dictionary to write on it, and unlock it after.
		node.dictionary.Set(req.Key, req.Value) // Set the <key, value> pair on storage.
		node.dictLock.Unlock()

		log.Info("Successful set.\n")
		return emptyResponse, nil
	}

	keyNode := node.Node  // By default, take this node to set the <key, value> pair on the local storage.
	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

	// If the key ID is not between this predecessor node ID and this node ID,
	// then the requested key is not necessarily local.
	if between, err := KeyBetween(req.Key, node.config.Hash, pred.ID, node.ID); !between && err == nil {
		log.Debug("Searching for the corresponding node.\n")
		keyNode, err = node.LocateKey(req.Key) // Locate the node that corresponds to the key.
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
			req.Replica = true
			log.Debug("Replicating set request to " + suc.IP + ".\n")
			return emptyResponse, node.RPC.Set(suc, req)
		}
		// Else, return.
		return emptyResponse, nil
	} else {
		log.Info("Redirecting set request to " + keyNode.IP + ".\n")
	}

	// Otherwise, return the result of the remote call on the correspondent node.
	return emptyResponse, node.RPC.Set(keyNode, req)
}

// Delete a <key, value> pair from storage.
func (node *Node) Delete(ctx context.Context, req *chord.DeleteRequest) (*chord.EmptyResponse, error) {
	log.Info("Delete: key=" + req.Key + ".\n")

	// If this request is a replica, resolve it local.
	if req.Replica {
		log.Debug("Resolving delete request locally (replication).\n")

		node.dictLock.Lock()            // Lock the dictionary to write on it, and unlock it after.
		node.dictionary.Delete(req.Key) // Delete the <key, value> pair from storage.
		node.dictLock.Unlock()

		log.Info("Successful delete.\n")
		return emptyResponse, nil
	}

	keyNode := node.Node  // By default, take this node to delete the <key, value> pair from the local storage.
	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

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
			req.Replica = true
			log.Debug("Replicating delete request to " + suc.IP + ".\n")
			return emptyResponse, node.RPC.Delete(suc, req)
		}
		// Else, return.
		return emptyResponse, nil
	} else {
		log.Info("Redirecting delete request to " + keyNode.IP + ".\n")
	}

	// Otherwise, return the result of the remote call on the correspondent node.
	return emptyResponse, node.RPC.Delete(keyNode, req)
}

// Partition returns all <key, values> pairs on this local storage, and on this local storage replication.
func (node *Node) Partition(ctx context.Context, req *chord.EmptyRequest) (*chord.PartitionResponse, error) {
	log.Trace("Getting all <key, values> pairs on this local storage.\n")

	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

	node.dictLock.RLock()                                       // Lock the dictionary to read it, and unlock it after.
	in, out, err := node.dictionary.Partition(pred.ID, node.ID) // Obtain the <key, value> pairs of the storage.
	node.dictLock.RUnlock()
	if err != nil {
		message := "Error getting <key, values> pairs from this local storage.\n"
		log.Error(message)
		return &chord.PartitionResponse{}, errors.New(message + err.Error())
	}
	// Return the dictionary corresponding to this local storage, and the one corresponding
	// to this local storage replication.
	return &chord.PartitionResponse{In: in, Out: out}, err
}

// Extend the local storage dictionary with a map of <key, values> pairs.
func (node *Node) Extend(ctx context.Context, req *chord.ExtendRequest) (*chord.EmptyResponse, error) {
	log.Debug("Extending local storage dictionary.\n")

	// If there are no keys to add, return.
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

// Discard a list of keys from local storage dictionary.
func (node *Node) Discard(ctx context.Context, req *chord.DiscardRequest) (*chord.EmptyResponse, error) {
	log.Debug("Discarding keys from local storage dictionary.\n")

	node.dictLock.Lock()                     // Lock the dictionary to write on it, and unlock it after.
	err := node.dictionary.Discard(req.Keys) // Delete the keys from storage.
	node.dictLock.Unlock()
	if err != nil {
		message := "Error discarding keys from storage dictionary.\n"
		log.Error(message)
		return emptyResponse, errors.New(message + err.Error())
	}
	return emptyResponse, err
}
