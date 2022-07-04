package chord

import (
	"errors"
	"math/big"
	"net"
	"os"
	"server/chord/chord"
	"sync"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
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
	// If configuration is null, report error.
	if configuration == nil {
		log.Error("Error creating node: configuration cannot be null.")
		return nil, errors.New("error creating node: configuration cannot be null")
	}

	// Creates the new node with the obtained ID and same address.
	innerNode := &chord.Node{ID: big.NewInt(0).Bytes(), IP: "0.0.0.0", Port: port}

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

	// Return the node.
	return node, nil
}

// DefaultNode creates and returns a new Node with default configurations.
func DefaultNode(port string) (*Node, error) {
	conf := DefaultConfig()                    // Creates a default configuration.
	transport := NewGRPCServices(conf)         // Creates a default RPC transport layer.
	dictionary := NewDiskDictionary(conf.Hash) // Creates a default dictionary.

	// Return the default node.
	return NewNode(port, conf, transport, dictionary)
}

// Node server chord methods.

// GetPredecessor returns the node believed to be the current predecessor.
func (node *Node) GetPredecessor(ctx context.Context, req *chord.EmptyRequest) (*chord.Node, error) {
	log.Trace("Getting node predecessor.")

	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// Return the predecessor of this node.
	return pred, nil
}

// GetSuccessor returns the node believed to be the current successor.
func (node *Node) GetSuccessor(ctx context.Context, req *chord.EmptyRequest) (*chord.Node, error) {
	log.Trace("Getting node successor.")

	// Lock the successor to read it, and unlock it after.
	node.sucLock.RLock()
	suc := node.successors.Beg()
	node.sucLock.RUnlock()

	// Return the successor of this node.
	return suc, nil
}

// SetPredecessor sets the predecessor of this node.
func (node *Node) SetPredecessor(ctx context.Context, candidate *chord.Node) (*chord.EmptyResponse, error) {
	log.Tracef("Setting node predecessor to %s.", candidate.IP)

	// If the new predecessor is not this node, update this node predecessor.
	if !Equals(candidate.ID, node.ID) {
		// Lock the predecessor to read and write on it, and unlock it after.
		node.predLock.Lock()
		old := node.predecessor
		node.predecessor = candidate
		node.predLock.Unlock()
		// If there was an old predecessor, absorb its keys.
		go node.AbsorbPredecessorKeys(old)
	} else {
		log.Trace("Candidate predecessor is this same node. Update refused.")
	}

	return emptyResponse, nil
}

// SetSuccessor sets the successor of this node.
func (node *Node) SetSuccessor(ctx context.Context, candidate *chord.Node) (*chord.EmptyResponse, error) {
	log.Tracef("Setting node successor to %s.", candidate.IP)

	// If the new successor is not this node, update this node successor.
	if !Equals(candidate.ID, node.ID) {
		// Lock the successor to write on it, and unlock it after.
		node.sucLock.Lock()
		node.successors.PushBeg(candidate)
		node.sucLock.Unlock()
		// Update this new successor with this node keys.
		go node.UpdateSuccessorKeys()
	} else {
		log.Trace("Candidate successor is this same node. Update refused.")
	}

	return emptyResponse, nil
}

// FindSuccessor finds the node that succeeds ID.
func (node *Node) FindSuccessor(ctx context.Context, id *chord.ID) (*chord.Node, error) {
	// Find the successor of this ID.
	return node.FindIDSuccessor(id.ID)
}

// Notify this node that it possibly have a new predecessor.
func (node *Node) Notify(ctx context.Context, candidate *chord.Node) (*chord.EmptyResponse, error) {
	log.Trace("Checking predecessor notification.")

	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// If this node has no predecessor or the predecessor candidate is closer to this node
	// than its current predecessor, update this node predecessor with the candidate.
	if Equals(pred.ID, node.ID) || Between(candidate.ID, pred.ID, node.ID) {
		log.Debugf("Predecessor updated to node at %s.", candidate.IP)

		// Lock the predecessor to write on it, and unlock it after.
		node.predLock.Lock()
		node.predecessor = candidate
		node.predLock.Unlock()

		// Update the new predecessor with its correspondent keys.
		go node.UpdatePredecessorKeys(pred)
	}

	return emptyResponse, nil
}

// Check if this node is alive.
func (node *Node) Check(ctx context.Context, req *chord.EmptyRequest) (*chord.EmptyResponse, error) {
	return emptyResponse, nil
}

// Get the value associated to a key.
func (node *Node) Get(ctx context.Context, req *chord.GetRequest) (*chord.GetResponse, error) {
	log.Infof("Get: key=%s.", req.Key)
	address := req.IP // Obtain the requesting address.

	// If block is needed.
	if req.Lock {
		node.dictLock.RLock()                         // Lock the dictionary to read it, and unlock it after.
		err := node.dictionary.Lock(req.Key, address) // Lock this key on storage.
		node.dictLock.RUnlock()
		if err != nil {
			log.Errorf("Error locking key: already locked.\n%s", err)
			return nil, err
		}
	} else {
		node.dictLock.RLock()                           // Lock the dictionary to read it, and unlock it after.
		err := node.dictionary.Unlock(req.Key, address) // Unlock this key on storage.
		node.dictLock.RUnlock()
		if err != nil {
			log.Errorf("Error locking key: already locked.\n%s", err)
			return nil, err
		}
	}

	keyNode := node.Node  // By default, take this node to get the value of this key from the local storage.
	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

	// If the key ID is not between the predecessor ID and this node ID,
	// then the requested key is not necessarily local.
	if between, err := KeyBetween(req.Key, node.config.Hash, pred.ID, node.ID); !between && err == nil {
		log.Debug("Searching for the corresponding node.")
		keyNode, err = node.LocateKey(req.Key) // Locate the node that stores the key.
		if err != nil {
			log.Error("Error getting key.")
			return &chord.GetResponse{}, errors.New("error getting key\n" + err.Error())
		}
	} else if err != nil {
		log.Error("Error getting key.")
		return &chord.GetResponse{}, errors.New("error getting key\n" + err.Error())
	}

	// If the node that stores the key is this node, directly get the associated value from this node storage.
	if Equals(keyNode.ID, node.ID) {
		log.Debug("Resolving get request locally.")

		// Lock the dictionary to read it, and unlock it after.
		node.dictLock.RLock()
		value, err := node.dictionary.GetWithLock(req.Key, address) // Get the value associated to this key from storage.
		node.dictLock.RUnlock()
		if err != nil && err != os.ErrPermission {
			log.Error("Error getting key.\n" + err.Error())
			return &chord.GetResponse{}, os.ErrNotExist
		} else if err == os.ErrPermission {
			log.Error("Error getting key: already locked.\n" + err.Error())
			return &chord.GetResponse{}, err
		}

		log.Info("Successful get.")
		// Return the value associated to this key.
		return &chord.GetResponse{Value: value}, nil
	} else {
		log.Infof("Redirecting get request to %s.", keyNode.IP)
	}
	// Otherwise, return the result of the remote call on the correspondent node.
	return node.RPC.Get(keyNode, req)
}

// Set a <key, value> pair on storage.
func (node *Node) Set(ctx context.Context, req *chord.SetRequest) (*chord.EmptyResponse, error) {
	log.Infof("Set: key=%s value=%s.", req.Key, string(req.Value))
	address := req.IP // Obtain the requesting address.

	// If block is needed.
	if req.Lock {
		node.dictLock.RLock()                         // Lock the dictionary to read it, and unlock it after.
		err := node.dictionary.Lock(req.Key, address) // Lock this key on storage.
		node.dictLock.RUnlock()
		if err != nil {
			log.Errorf("Error locking key: already locked.\n%s", err)
			return nil, err
		}
	} else {
		node.dictLock.RLock()                           // Lock the dictionary to read it, and unlock it after.
		err := node.dictionary.Unlock(req.Key, address) // Unlock this key on storage.
		node.dictLock.RUnlock()
		if err != nil {
			log.Errorf("Error locking key: already locked.\n%s", err)
			return nil, err
		}
	}

	// If this request is a replica, resolve it local.
	if req.Replica {
		log.Debug("Resolving set request locally (replication).")

		// Lock the dictionary to write on it, and unlock it after.
		node.dictLock.Lock()
		err := node.dictionary.SetWithLock(req.Key, req.Value, address) // Set the <key, value> pair on storage.
		node.dictLock.Unlock()
		if err != nil {
			log.Error("Error setting key.")
			return emptyResponse, errors.New("error setting key\n" + err.Error())
		} else if err == os.ErrPermission {
			log.Error("Error setting key: already locked.\n" + err.Error())
			return emptyResponse, err
		}

		log.Info("Successful set.")
		return emptyResponse, nil
	}

	keyNode := node.Node  // By default, take this node to set the <key, value> pair on the local storage.
	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

	// If the key ID is not between this predecessor node ID and this node ID,
	// then the requested key is not necessarily local.
	if between, err := KeyBetween(req.Key, node.config.Hash, pred.ID, node.ID); !between && err == nil {
		log.Debug("Searching for the corresponding node.")
		keyNode, err = node.LocateKey(req.Key) // Locate the node that corresponds to the key.
		if err != nil {
			log.Error("Error setting key.")
			return emptyResponse, errors.New("error setting key\n" + err.Error())
		}
	} else if err != nil {
		log.Error("Error setting key.")
		return emptyResponse, errors.New("error setting key\n" + err.Error())
	}

	// If the key corresponds to this node, directly set the <key, value> pair on its storage.
	if Equals(keyNode.ID, node.ID) {
		log.Debug("Resolving set request locally.")

		// Lock the dictionary to write on it, and unlock it after.
		node.dictLock.Lock()
		err := node.dictionary.SetWithLock(req.Key, req.Value, address) // Set the <key, value> pair on storage.
		node.dictLock.Unlock()
		if err != nil {
			log.Error("Error setting key.")
			return emptyResponse, errors.New("error setting key\n" + err.Error())
		} else if err == os.ErrPermission {
			log.Error("Error setting key: already locked.\n" + err.Error())
			return emptyResponse, err
		}

		log.Info("Successful set.")

		node.sucLock.RLock() // Lock the successor to read it, and unlock it after.
		suc := node.successors.Beg()
		node.sucLock.RUnlock()

		// If successor is not this node, replicate the request to it.
		if !Equals(suc.ID, node.ID) {
			go func() {
				req.Replica = true
				log.Debugf("Replicating set request to %s.", suc.IP)
				err := node.RPC.Set(suc, req)
				if err != nil {
					log.Errorf("Error replicating set request to %s.\n%s", suc.IP, err.Error())
				}
			}()
		}

		return emptyResponse, nil
	} else {
		log.Infof("Redirecting set request to %s.", keyNode.IP)
	}

	// Otherwise, return the result of the remote call on the correspondent node.
	return emptyResponse, node.RPC.Set(keyNode, req)
}

// Delete a <key, value> pair from storage.
func (node *Node) Delete(ctx context.Context, req *chord.DeleteRequest) (*chord.EmptyResponse, error) {
	log.Infof("Delete: key=%s.", req.Key)
	address := req.IP // Obtain the requesting address.

	// If block is needed.
	if req.Lock {
		node.dictLock.RLock()                         // Lock the dictionary to read it, and unlock it after.
		err := node.dictionary.Lock(req.Key, address) // Lock this key on storage.
		node.dictLock.RUnlock()
		if err != nil {
			log.Errorf("Error locking key: already locked.\n%s", err)
			return nil, err
		}
	} else {
		node.dictLock.RLock()                           // Lock the dictionary to read it, and unlock it after.
		err := node.dictionary.Unlock(req.Key, address) // Unlock this key on storage.
		node.dictLock.RUnlock()
		if err != nil {
			log.Errorf("Error locking key: already locked.\n%s", err)
			return nil, err
		}
	}

	// If this request is a replica, resolve it local.
	if req.Replica {
		log.Debug("Resolving delete request locally (replication).")

		// Lock the dictionary to write on it, and unlock it after.
		node.dictLock.Lock()
		err := node.dictionary.DeleteWithLock(req.Key, address) // Delete the <key, value> pair from storage.
		node.dictLock.Unlock()
		if err != nil && err != os.ErrPermission {
			log.Error("Error deleting key.")
			return emptyResponse, errors.New("error deleting key\n" + err.Error())
		} else if err == os.ErrPermission {
			log.Error("Error deleting key: already locked.\n" + err.Error())
			return emptyResponse, err
		}

		log.Info("Successful delete.")
		return emptyResponse, nil
	}

	keyNode := node.Node  // By default, take this node to delete the <key, value> pair from the local storage.
	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

	// If the key ID is not between this predecessor node ID and this node ID,
	// then the requested key is not necessarily local.
	if between, err := KeyBetween(req.Key, node.config.Hash, pred.ID, node.ID); !between && err == nil {
		log.Debug("Searching for the corresponding node.")
		keyNode, err = node.LocateKey(req.Key) // Locate the node that stores the key.
		if err != nil {
			log.Error("Error deleting key.")
			return emptyResponse, errors.New("error deleting key\n" + err.Error())
		}
	} else if err != nil {
		log.Error("Error deleting key.")
		return emptyResponse, errors.New("error deleting key\n" + err.Error())
	}

	// If the key corresponds to this node, directly delete the <key, value> pair from its storage.
	if Equals(keyNode.ID, node.ID) {
		log.Debug("Resolving delete request locally.")

		// Lock the dictionary to write on it, and unlock it at the end of function.
		node.dictLock.Lock()
		err := node.dictionary.DeleteWithLock(req.Key, address) // Delete the <key, value> pair from storage.
		node.dictLock.Unlock()
		if err != nil && err != os.ErrPermission {
			log.Error("Error deleting key.")
			return emptyResponse, errors.New("error deleting key\n" + err.Error())
		} else if err == os.ErrPermission {
			log.Error("Error deleting key: already locked.\n" + err.Error())
			return emptyResponse, err
		}

		log.Info("Successful delete.")

		node.sucLock.RLock() // Lock the successor to read it, and unlock it after.
		suc := node.successors.Beg()
		node.sucLock.RUnlock()

		// If successor is not this node, replicate the request to it.
		if Equals(suc.ID, node.ID) {
			go func() {
				req.Replica = true
				log.Debugf("Replicating set request to %s.", suc.IP)
				err := node.RPC.Delete(suc, req)
				if err != nil {
					log.Errorf("Error replicating set request to %s.\n%s", suc.IP, err.Error())
				}
			}()
		}
		// Else, return.
		return emptyResponse, nil
	} else {
		log.Infof("Redirecting delete request to %s.", keyNode.IP)
	}

	// Otherwise, return the result of the remote call on the correspondent node.
	return emptyResponse, node.RPC.Delete(keyNode, req)
}

// Partition returns all <key, values> pairs on this local storage, and on this local storage replication.
func (node *Node) Partition(ctx context.Context, req *chord.EmptyRequest) (*chord.PartitionResponse, error) {
	log.Trace("Getting all <key, values> pairs on this local storage.")

	node.predLock.RLock() // Lock the predecessor to read it, and unlock it after.
	pred := node.predecessor
	node.predLock.RUnlock()

	node.dictLock.RLock()                                       // Lock the dictionary to read it, and unlock it after.
	in, out, err := node.dictionary.Partition(pred.ID, node.ID) // Obtain the <key, value> pairs of the storage.
	node.dictLock.RUnlock()
	if err != nil {
		log.Error("Error getting keys from local storage.")
		return &chord.PartitionResponse{}, errors.New("error getting keys from local storage\n" + err.Error())
	}
	// Return the dictionary corresponding to this local storage, and the one corresponding
	// to this local storage replication.
	return &chord.PartitionResponse{In: in, Out: out}, err
}

// Extend the local storage dictionary with a map of <key, values> pairs.
func (node *Node) Extend(ctx context.Context, req *chord.ExtendRequest) (*chord.EmptyResponse, error) {
	log.Debug("Extending local storage dictionary.")

	// If there are no keys to add, return.
	if req.Dictionary == nil || len(req.Dictionary) == 0 {
		return emptyResponse, nil
	}

	node.dictLock.Lock()                          // Lock the dictionary to write on it, and unlock it after.
	err := node.dictionary.Extend(req.Dictionary) // Set the <key, value> pairs on the storage.
	node.dictLock.Unlock()
	if err != nil {
		log.Error("Error extending storage dictionary.")
		return emptyResponse, errors.New("error extending storage dictionary\n" + err.Error())
	}
	return emptyResponse, err
}

// Discard a list of keys from local storage dictionary.
func (node *Node) Discard(ctx context.Context, req *chord.DiscardRequest) (*chord.EmptyResponse, error) {
	log.Debug("Discarding keys from local storage dictionary.")

	node.dictLock.Lock()                     // Lock the dictionary to write on it, and unlock it after.
	err := node.dictionary.Discard(req.Keys) // Delete the keys from storage.
	node.dictLock.Unlock()
	if err != nil {
		log.Error("Error discarding keys from storage dictionary.")
		return emptyResponse, errors.New("error discarding keys from storage dictionary\n" + err.Error())
	}
	return emptyResponse, err
}
