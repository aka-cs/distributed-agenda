package chord

import (
	"errors"
	"fmt"
	"net"
	"server/chord/chord"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Node server internal methods.

// Start the node server, by registering the server of this node as a chord server, starting
// the transport layer services and the periodically threads that stabilizes the server.
func (node *Node) Start() error {
	log.Info("Starting server...")

	// If node server is actually running, report error.
	if IsOpen(node.shutdown) {
		log.Error("Error starting server: this node server is actually running.")
		return errors.New("error starting server: this node server is actually running")
	}

	node.shutdown = make(chan struct{}) // Report the node server is running.

	ip := GetOutboundIP()                    // Get the IP of this node.
	address := ip.String() + ":" + node.Port // Get the address of this node.
	log.Infof("Node address at %s.", address)

	id, err := HashKey(address, node.config.Hash) // Obtain the ID relative to this address.
	if err != nil {
		log.Error("Error starting node: cannot hash node address.")
		return errors.New("error starting node: cannot hash node address\n" + err.Error())
	}

	node.ID = id          // Instantiates the ID of this node.
	node.IP = ip.String() // Instantiates the IP of this node.

	// Start listening at corresponding address.
	log.Debug("Trying to listen at corresponding address.")
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Errorf("Error starting server: cannot listen at the address %s.", address)
		return errors.New(
			fmt.Sprintf("error starting server: cannot listen at the address %s\n%s", address, err.Error()))
	}
	log.Infof("Listening at %s.", address)

	node.successors = NewQueue[chord.Node](node.config.StabilizingNodes) // Create the successors queue.
	node.successors.PushBack(node.Node)                                  // Set this node as its own successor.
	node.predecessor = node.Node                                         // Set this node as it own predecessor.
	node.fingerTable = NewFingerTable(node.config.HashSize)              // Create the finger table.
	node.server = grpc.NewServer(node.config.ServerOpts...)              // Create the node server.
	node.sock = listener.(*net.TCPListener)                              // Save the socket.
	err = node.dictionary.Clear()                                        // Clear the node dictionary.
	if err != nil {
		log.Errorf("Error starting server: invalid storage dictionary.")
		return errors.New("error starting server: invalid storage dictionary\n" + err.Error())
	}

	chord.RegisterChordServer(node.server, node) // Register the node server as a chord server.
	log.Debug("Chord services registered.")

	err = node.RPC.Start() // Start the RPC (transport layer) services.
	if err != nil {
		log.Error("Error starting server: cannot start the transport layer.")
		return errors.New("error starting server: cannot start the transport layer\n" + err.Error())
	}

	// Start serving at the opened socket.
	go node.Listen()

	discovered, err := node.NetDiscover(ip) // Discover the chord net, if exists.
	if err != nil {
		log.Error("Error starting server: cannot discover net to connect.")
		return errors.New("error starting server: cannot discover net to connect\n" + err.Error())
	}

	// If there is a chord net already.
	if discovered != "" {
		err = node.Join(&chord.Node{IP: discovered, Port: node.Port}) // Join to the discovered node.
		if err != nil {
			log.Error("Error joining to chord net server.")
			return errors.New("error joining to chord net server.\n" + err.Error())
		}
	} else {
		// Otherwise, create one.
		log.Info("Creating chord ring.")
	}

	// Start periodically threads.
	go node.PeriodicallyCheckPredecessor()
	go node.PeriodicallyCheckSuccessor()
	go node.PeriodicallyStabilize()
	go node.PeriodicallyFixSuccessor()
	go node.PeriodicallyFixFinger()
	go node.PeriodicallyFixStorage()
	go node.BroadListen()

	log.Info("Server started.")
	return nil
}

// Stop the node server, by stopping the transport layer services and reporting the node
// services are now shutdown, to make the periodic threads stop themselves eventually.
// Then, connects this node successor and predecessor directly, thus leaving the ring.
// It is not necessary to deal with the transfer of keys for the maintenance of replication,
// since the methods used to connect the nodes (SetSuccessor and SetPredecessor) will take care of this.
func (node *Node) Stop() error {
	log.Info("Closing server...")

	// If node server is not actually running, report error.
	if !IsOpen(node.shutdown) {
		log.Error("Error stopping server: this node server is actually shutdown.")
		return errors.New("error stopping server: this node server is actually shutdown")
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
			log.Errorf("Error stopping server: error setting new predecessor of successor at %s.", suc.IP)
			return errors.New(
				fmt.Sprintf("error stopping server: error setting new predecessor of successor at %s\n.%s",
					suc.IP, err.Error()))
		}

		// Change the successor of this node predecessor to this node successor.
		err = node.RPC.SetSuccessor(pred, suc)
		if err != nil {
			log.Errorf("Error stopping server: error setting new successor of predecessor at %s.", pred.IP)
			return errors.New(
				fmt.Sprintf("error stopping server: error setting new successor of predecessor at %s\n.%s",
					pred.IP, err.Error()))
		}
	}

	err := node.RPC.Stop() // Stop the RPC (transport layer) services.
	if err != nil {
		log.Error("Error stopping server: cannot stop the transport layer.")
		return errors.New("error stopping server: cannot stop the transport layer\n" + err.Error())
	}

	node.server.Stop() // Stop node server.

	close(node.shutdown) // Report the node server is shutdown.
	log.Info("Server closed.")
	return nil
}

// GetKey gets the value associated to a key.
func (node *Node) GetKey(key string, lock bool) ([]byte, error) {
	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), node.config.Timeout)
	defer cancel()

	log.Info("Resolving get request.")

	response, err := node.Get(ctx, &chord.GetRequest{Key: key, Lock: lock, IP: node.IP})
	if err != nil {
		return nil, err
	}

	return response.Value, nil
}

// SetKey sets a <key, value> pair on storage.
func (node *Node) SetKey(key string, value []byte, lock bool) error {
	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), node.config.Timeout)
	defer cancel()

	log.Info("Resolving set request.")

	_, err := node.Set(ctx, &chord.SetRequest{Key: key, Value: value, Lock: lock, IP: node.IP})

	return err
}

// DeleteKey deletes a <key, value> pair from storage.
func (node *Node) DeleteKey(key string, lock bool) error {
	// Obtain the context of the connection and set the timeout of the request.
	ctx, cancel := context.WithTimeout(context.Background(), node.config.Timeout)
	defer cancel()

	log.Info("Resolving delete request.")

	_, err := node.Delete(ctx, &chord.DeleteRequest{Key: key, Lock: lock, IP: node.IP})

	return err
}

// Listen for inbound connections.
func (node *Node) Listen() {
	log.Info("Starting to serve at the opened socket.")
	err := node.server.Serve(node.sock)
	if err != nil {
		log.Errorf("Cannot serve at %s.\n%s", node.IP, err.Error())
		return
	}
}

// Join this node to the Chord ring, using another known node.
// To join the node to the ring, the immediate successor of this node ID in the ring is searched,
// starting from the known node, and the obtained node is taken as the successor of this node.
// The keys corresponding to this node will be transferred by its successor, from the Notify
// method that is called at the end of Stabilize method.
func (node *Node) Join(knownNode *chord.Node) error {
	log.Info("Joining to chord ring.")

	// If the known node is null, return error: to join this node to the ring,
	// at least one node of the ring must be known.
	if knownNode == nil {
		log.Error("Error joining to chord ring: known node cannot be null.")
		return errors.New("error joining to chord ring: known node cannot be null")
	}

	log.Infof("Known node address: %s.", knownNode.IP)

	suc, err := node.RPC.FindSuccessor(knownNode, node.ID) // Find the immediate successor of this node ID.
	if err != nil {
		log.Error("Error joining to chord ring: cannot find successor of this node ID.")
		return errors.New("error joining to chord ring: cannot find successor of this node ID\n" + err.Error())
	}
	// If the obtained node ID is this node ID, then this node is already on the ring.
	if Equals(suc.ID, node.ID) {
		log.Error("Error joining to chord ring: a node with this ID already exists.")
		return errors.New("error joining to chord ring: a node with this ID already exists")
	}

	// Lock the successor to write on it, and unlock it after.
	node.sucLock.Lock()
	node.successors.PushBeg(suc) // Update this node successor with the obtained node.
	node.sucLock.Unlock()
	log.Infof("Successful join. Successor node at %s.", suc.IP)
	return nil
}

// FindIDSuccessor finds the node that succeeds ID.
// To find it, the known node with ID less than this ID and closest to this ID is searched,
// using the finger table.
// If the obtained node is this node, returns this node successor.
// Otherwise, returns the result of the same remote call from the obtained node.
func (node *Node) FindIDSuccessor(id []byte) (*chord.Node, error) {
	log.Trace("Finding ID successor.")

	// If ID is null, report error.
	if id == nil {
		log.Error("Error finding successor: ID cannot be null.")
		return nil, errors.New("error finding successor: ID cannot be null")
	}

	// Find the closest finger, on this finger table, with ID less than this ID.
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
		log.Errorf("Error finding ID successor from finger at %s.", pred.IP)
		return nil, errors.New(
			fmt.Sprintf("error finding ID successor from finger at %s.\n%s", pred.IP, err.Error()))
	}
	// Return the obtained successor.
	log.Trace("ID successor found.")
	return suc, nil
}

// LocateKey locate the node that corresponds to a given key.
// To locate it, hash the given key to obtain the corresponding ID. Then look for the immediate
// successor of this ID in the ring, since this is the node to which the key corresponds.
func (node *Node) LocateKey(key string) (*chord.Node, error) {
	log.Tracef("Locating key: %s.", key)

	id, err := HashKey(key, node.config.Hash) // Obtain the ID relative to this key.
	if err != nil {
		log.Errorf("Error locating key: %s.", key)
		return nil, errors.New(fmt.Sprintf("error locating key: %s.\n%s", key, err.Error()))
	}

	suc, err := node.FindIDSuccessor(id) // Find and return the successor of this ID.
	if err != nil {
		log.Errorf("Error locating key: %s.", key)
		return nil, errors.New(fmt.Sprintf("error locating key: %s.\n%s", key, err.Error()))
	}

	log.Trace("Successful key location.")
	return suc, nil
}

// ClosestFinger find the closest finger preceding the given ID.
func (node *Node) ClosestFinger(ID []byte) *chord.Node {
	log.Trace("Finding the closest finger preceding this ID.")
	defer log.Trace("Closest finger found.")

	// Iterate the finger table in reverse, and return the first finger
	// such that the finger ID is between this node ID and the given ID.
	for i := len(node.fingerTable) - 1; i >= 0; i-- {
		node.fingerLock.RLock()
		finger := node.fingerTable[i]
		node.fingerLock.RUnlock()

		if finger != nil && Between(finger.ID, node.ID, ID) {
			return finger
		}
	}

	// If no finger meets the conditions, return this node.
	return node.Node
}

// AbsorbPredecessorKeys absorbs the old predecessor replicated keys on this node.
func (node *Node) AbsorbPredecessorKeys(old *chord.Node) {
	// If the old predecessor is not this node.
	if !Equals(old.ID, node.ID) {
		log.Debug("Absorbing predecessor's keys.")

		// Lock the successor to read it, and unlock it after.
		node.sucLock.RLock()
		suc := node.successors.Beg()
		node.sucLock.RUnlock()

		// If successor exists, transfer the old predecessor keys to it, to maintain replication.
		if !Equals(suc.ID, node.ID) {
			// Lock the dictionary to read it, and unlock it after.
			node.dictLock.RLock()
			in, out, err := node.dictionary.Partition(old.ID, node.ID) // Obtain the predecessor keys.
			node.dictLock.RUnlock()
			if err != nil {
				log.Errorf("Error obtaining old predecessor keys.\n%s", err.Error())
				return
			}

			log.Debug("Transferring old predecessor keys to the successor.")
			log.Debugf("Keys to transfer: %s", Keys(out))
			log.Debugf("Remaining keys: %s", Keys(in))

			// Transfer the keys to this node successor.
			err = node.RPC.Extend(suc, &chord.ExtendRequest{Dictionary: out})
			if err != nil {
				log.Errorf("Error transferring keys to successor.\n%s", err.Error())
				return
			}
			log.Debug("Predecessor keys absorbed. Successful transfer of keys to the successor.")
		}
	}
}

// DeletePredecessorKeys deletes the old predecessor replicated keys on this node.
// Return the actual keys, the actual replicated keys, and the deleted keys.
func (node *Node) DeletePredecessorKeys(old *chord.Node) (map[string][]byte, map[string][]byte, map[string][]byte, error) {
	// Lock the predecessor to read it, and unlock it after.
	node.predLock.Lock()
	pred := node.predecessor
	node.predLock.Unlock()

	// Lock the dictionary to read and write on it, and unlock it at the end of function.
	node.dictLock.Lock()
	defer node.dictLock.Unlock()

	in, out, err := node.dictionary.Partition(pred.ID, node.ID) // Obtain the keys to transfer.
	if err != nil {
		log.Error("Error obtaining the new predecessor corresponding keys.")
		return nil, nil, nil, errors.New("error obtaining the new predecessor corresponding keys\n" + err.Error())
	}

	// If the old predecessor is not this node, delete the old predecessor keys on this node.
	if !Equals(old.ID, node.ID) {
		_, deleted, err := node.dictionary.Partition(old.ID, node.ID) // Obtain the keys to delete.
		if err != nil {
			log.Error("Error obtaining old predecessor keys replicated on this node.")
			return nil, nil, nil, errors.New(
				"error obtaining old predecessor keys replicated on this node\n" + err.Error())
		}

		err = node.dictionary.Discard(Keys(out)) // Delete the keys of the old predecessor.
		if err != nil {
			log.Error("Error deleting old predecessor keys on this node.")
			return nil, nil, nil, errors.New("error deleting old predecessor keys on this node\n" + err.Error())
		}

		log.Debug("Successful delete of old predecessor replicated keys in this node.")
		return in, out, deleted, nil
	}
	return in, out, nil, nil
}

// UpdatePredecessorKeys updates the new predecessor with its corresponding keys.
func (node *Node) UpdatePredecessorKeys(old *chord.Node) {
	// Lock the predecessor to read it, and unlock it after.
	node.predLock.Lock()
	pred := node.predecessor
	node.predLock.Unlock()

	// Lock the successor to write on it, and unlock it after.
	node.sucLock.Lock()
	suc := node.successors.Beg()
	node.sucLock.Unlock()

	// If the new predecessor is not this node.
	if !Equals(pred.ID, node.ID) {
		// Transfer the corresponding keys to the new predecessor.
		in, out, deleted, err := node.DeletePredecessorKeys(old)
		if err != nil {
			log.Errorf("Error updating new predecessor keys.\n%s", err.Error())
			return
		}

		log.Debug("Transferring old predecessor keys to the successor.")
		log.Debugf("Keys to transfer: %s", Keys(out))
		log.Debugf("Remaining keys: %s", Keys(in))
		log.Debugf("Keys to delete: %s", Keys(deleted))

		// Build the new predecessor dictionary, by transferring its correspondent keys.
		err = node.RPC.Extend(pred, &chord.ExtendRequest{Dictionary: out})
		if err != nil {
			log.Errorf("Error transferring keys to the new predecessor.\n%s", err.Error())
			// If there are deleted keys, reinsert the keys on this node storage,
			// to prevent the loss of information.
			if deleted != nil {
				// Lock the dictionary to write on it, and unlock it after.
				node.dictLock.Lock()
				err = node.dictionary.Extend(deleted)
				node.dictLock.Unlock()
				if err != nil {
					log.Errorf("Error reinserting deleted keys on this node.\n%s", err.Error())
				}
			}
			return
		}
		log.Debug("Successful transfer of keys to the new predecessor.")

		// If successor exists, and it's different from the new predecessor, delete the transferred keys
		// from successor storage replication.
		if !Equals(suc.ID, node.ID) && !Equals(suc.ID, pred.ID) {
			err = node.RPC.Discard(suc, &chord.DiscardRequest{Keys: Keys(out)})
			if err != nil {
				log.Errorf("Error deleting replicated keys on the successor at %s.\n%s", suc.IP, err.Error())
				// If there are deleted keys, reinsert the keys on this node storage,
				// to prevent the loss of information.
				if deleted != nil {
					// Lock the dictionary to read and write on it, and unlock it after.
					node.dictLock.Lock()
					err = node.dictionary.Extend(deleted)
					node.dictLock.Unlock()
					if err != nil {
						log.Errorf("Error reinserting deleted keys on this node.\n%s", err.Error())
					}
				}
				return
			}
			log.Debug("Successful delete of old predecessor replicated keys in the successor.")
		}
	}
}

// UpdateSuccessorKeys updates the new successor replicated keys with this node keys.
func (node *Node) UpdateSuccessorKeys() {
	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// Lock the successor to write on it, and unlock it after.
	node.sucLock.Lock()
	suc := node.successors.Beg()
	node.sucLock.Unlock()

	// If successor is not this node.
	if !Equals(suc.ID, node.ID) {
		// Lock the dictionary to read it, and unlock it after.
		node.dictLock.RLock()
		in, out, err := node.dictionary.Partition(pred.ID, node.ID) // Obtain this node keys.
		node.dictLock.RUnlock()
		if err != nil {
			log.Errorf("Error obtaining this node keys.\n%s", err.Error())
			return
		}

		log.Debug("Transferring this node keys to the successor.")
		log.Debugf("Keys to transfer: %s", Keys(out))
		log.Debugf("Remaining keys: %s", Keys(in))

		// Transfer this node keys to the new successor, to update it.
		err = node.RPC.Extend(suc, &chord.ExtendRequest{Dictionary: in})
		if err != nil {
			log.Errorf("Error transferring keys to the successor at %s.\n%s", suc.IP, err.Error())
			return
		}
		log.Trace("Successful transfer of keys to the successor.")
	}
}

// BroadListen listen for broadcast messages.
func (node *Node) BroadListen() {
	// Wait for the specific port to be free to use.
	pc, err := net.ListenPacket("udp4", ":8830")
	for err != nil {
		pc, err = net.ListenPacket("udp4", ":8830")
	}
	// Close the listening socket at the end of function.
	defer func(pc net.PacketConn) {
		err := pc.Close()
		if err != nil {
			return
		}
	}(pc)

	// Start listening messages.
	for {
		// If node server is shutdown, return.
		if !IsOpen(node.shutdown) {
			return
		}

		// Create the buffer to store the message.
		buf := make([]byte, 1024)
		// Wait for a message.
		n, address, err := pc.ReadFrom(buf)
		if err != nil {
			log.Errorf("Incoming broadcast message error.\n%s", err.Error())
			continue
		}

		log.Debugf("Incoming response message. %s sent this: %s", address, buf[:n])

		// If the incoming message is the specified one, answer with the specific response.
		if string(buf[:n]) == "Chord?" {
			_, err = pc.WriteTo([]byte("I am chord"), address)
			if err != nil {
				log.Errorf("Error responding broadcast message.\n%s", err.Error())
				continue
			}
		}
	}
}

// NetDiscover discover a chord ring net if exists.
func (node *Node) NetDiscover(ip net.IP) (string, error) {
	// Specify the broadcast address.
	ip[3] = 255
	broadcast := ip.String() + ":8830"

	// Try to listen at the specific port to use.
	pc, err := net.ListenPacket("udp4", ":8830")
	if err != nil {
		log.Errorf("Error listen at the address %s.", broadcast)
		return "", err
	}

	// Resolve the address to broadcast.
	out, err := net.ResolveUDPAddr("udp4", broadcast)
	if err != nil {
		log.Errorf("Error resolving broadcast address %s.", broadcast)
		return "", err
	}

	log.Info("UPD broadcast address resolved.")

	// Send the message.
	_, err = pc.WriteTo([]byte("Chord?"), out)
	if err != nil {
		log.Errorf("Error sending broadcast message at address %s.", broadcast)
		return "", err
	}

	log.Info("Message broadcast done.")
	top := time.Now().Add(10 * time.Second)

	log.Info("Waiting for response.")

	for top.After(time.Now()) {
		// Create the buffer to store the message.
		buf := make([]byte, 1024)

		// Set the deadline to wait for incoming messages.
		err = pc.SetReadDeadline(time.Now().Add(2 * time.Second))
		if err != nil {
			log.Error("Error setting deadline for incoming messages.")
			return "", err
		}

		// Wait for a message.
		n, address, err := pc.ReadFrom(buf)
		if err != nil {
			log.Errorf("Error reading incoming message.\n%s", err.Error())
			continue
		}

		log.Debugf("Incoming response message. %s sent this: %s", address, buf[:n])

		if string(buf[:n]) == "I am chord" {
			return strings.Split(address.String(), ":")[0], nil
		}
	}

	log.Info("Deadline for response exceed.")
	return "", nil
}
