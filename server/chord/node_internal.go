package chord

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"server/chord/chord"
	"strings"
	"time"
)

// Node server internal methods.

// Start the node server, by registering the server of this node as a chord server, starting
// the transport layer services and the periodically threads that stabilizes the server.
func (node *Node) Start() error {
	log.Info("Starting server...")

	// If node server is actually running, report error.
	if IsOpen(node.shutdown) {
		message := "Error starting server: this node server is actually running."
		log.Error(message)
		return errors.New(message)
	}

	node.shutdown = make(chan struct{}) // Report the node server is running.

	ip := GetOutboundIP()
	node.IP = ip.String()
	log.Infof("Node address at %s:%s.", node.IP, node.Port)
	id, err := HashKey(node.IP+":"+node.Port, node.config.Hash) // Obtain the ID relative to this address.
	if err != nil {
		message := "Error starting node: cannot hash node address."
		log.Error(message)
		return errors.New(message + err.Error())
	}
	node.ID = id

	// Start listening at corresponding address.
	log.Debug("Trying to listen at corresponding address.")
	listener, err := net.Listen("tcp", node.IP+":"+node.Port)
	if err != nil {
		message := "Error starting server: cannot listen at the address " + node.IP + "."
		log.Error(message)
		return errors.New(message + err.Error())
	}
	log.Debug("Listening at " + node.IP + ".")

	node.successors = NewQueue[chord.Node](node.config.StabilizingNodes) // Create the successors queue.
	node.successors.PushBack(node.Node)                                  // Set this node as its own successor.
	node.predecessor = node.Node                                         // Set this node as it own predecessor.
	node.fingerTable = NewFingerTable(node.config.HashSize)              // Create the finger table.
	node.dictionary.Clear()                                              // Clear the node dictionary.
	node.server = grpc.NewServer(node.config.ServerOpts...)              // Create the node server.
	node.sock = listener.(*net.TCPListener)                              // Save the socket.

	chord.RegisterChordServer(node.server, node) // Register the node server as a chord server.
	log.Debug("Chord services registered.")

	err = node.RPC.Start() // Start the RPC (transport layer) services.
	if err != nil {
		message := "Error starting server: cannot start the transport layer."
		log.Error(message)
		return errors.New(message + err.Error())
	}

	// Start serving at the opened socket.
	go node.Listen()

	discovered, err := node.NetDiscover(ip)
	if err != nil {
		message := "Error starting server: cannot discover net to connect."
		log.Error(message)
		return errors.New(message + err.Error())
	}
	if discovered != "" {
		err = node.Join(&chord.Node{IP: discovered, Port: node.Port})
		if err != nil {
			message := "Error joining to server."
			log.Error(message)
			return errors.New(message + err.Error())
		}
	} else {
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
		message := "Error stopping server: this node server is actually shutdown."
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
			message := "Error stopping server: error setting new predecessor of successor at " + suc.IP + "."
			log.Error(message)
			return errors.New(message + err.Error())
		}

		// Change the successor of this node predecessor to this node successor.
		err = node.RPC.SetSuccessor(pred, suc)
		if err != nil {
			message := "Error stopping server: error setting new successor of predecessor at " + pred.IP + "."
			log.Error(message)
			return errors.New(message + err.Error())
		}
	}

	err := node.RPC.Stop() // Stop the RPC (transport layer) services.
	if err != nil {
		message := "Error stopping server: cannot stop the transport layer."
		log.Error(message)
		return errors.New(message + err.Error())
	}

	node.server.Stop() // Stop node server.

	close(node.shutdown) // Report the node server is shutdown.
	log.Info("Server closed.")
	return nil
}

// Listen for inbound connections.
func (node *Node) Listen() {
	log.Info("Starting to serve at the opened socket.")
	err := node.server.Serve(node.sock)
	if err != nil {
		log.Error("Cannot serve at " + node.IP + "." + err.Error() + "")
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
		message := "Error joining to chord ring: known node cannot be null."
		log.Error(message)
		return errors.New(message)
	}

	log.Info("Known node address: " + knownNode.IP + ".")

	suc, err := node.RPC.FindSuccessor(knownNode, node.ID) // Find the immediate successor of this node ID.
	if err != nil {
		message := "Error joining to chord ring: cannot find successor of this node ID."
		log.Error(message + err.Error() + "")
		return errors.New(message + err.Error())
	}
	// If the obtained node ID is this node ID, then this node is already on the ring.
	if Equals(suc.ID, node.ID) {
		message := "Error joining to chord ring: a node with this ID already exists."
		log.Error(message)
		return errors.New(message)
	}

	// Lock the successor to write on it, and unlock it after.
	node.sucLock.Lock()
	node.successors.PushBeg(suc) // Update this node successor with the obtained node.
	node.sucLock.Unlock()
	log.Info("Successful join. Successor node at " + suc.IP + ".")
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
		message := "Error finding successor: ID cannot be null."
		log.Error(message)
		return nil, errors.New(message)
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
		message := "Error finding ID successor from finger at " + pred.IP + "."
		log.Error(message + err.Error() + "")
		return nil, errors.New(message + err.Error())
	}
	// Return the obtained successor.
	log.Trace("ID successor found.")
	return suc, nil
}

// LocateKey locate the node that corresponds to a given key.
// To locate it, hash the given key to obtain the corresponding ID. Then look for the immediate
// successor of this ID in the ring, since this is the node to which the key corresponds.
func (node *Node) LocateKey(key string) (*chord.Node, error) {
	log.Trace("Locating key: " + key + ".")

	id, err := HashKey(key, node.config.Hash) // Obtain the ID relative to this key.
	if err != nil {
		message := "Error locating key " + key + "."
		log.Error(message)
		return nil, errors.New(message + err.Error())
	}

	suc, err := node.FindIDSuccessor(id) // Find and return the successor of this ID.
	if err != nil {
		message := "Error locating key."
		log.Error(message)
		return nil, errors.New(message + err.Error())
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

func (node *Node) AbsorbPredecessorKeys(old *chord.Node) {
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
			log.Debugf("Keys to transfer: %s", out)
			log.Debugf("Remaining keys: %s", in)

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

func (node *Node) DeletePredecessorKeys(old *chord.Node) (map[string][]byte, map[string][]byte, error) {
	// Lock the predecessor to read it, and unlock it after.
	node.predLock.Lock()
	pred := node.predecessor
	node.predLock.Unlock()

	// Lock the dictionary to read and write on it, and unlock it at the end of function.
	node.dictLock.Lock()
	defer node.dictLock.Unlock()

	in, out, err := node.dictionary.Partition(pred.ID, node.ID) // Obtain the keys to transfer.
	if err != nil {
		message := "Error obtaining the new predecessor corresponding keys."
		log.Error(message)
		return nil, nil, errors.New(message + "\n" + err.Error())
	}

	log.Debug("Transferring old predecessor keys to the successor.")
	log.Debugf("Keys to transfer: %s", out)
	log.Debugf("Remaining keys: %s", in)

	// If the old predecessor is not this node, delete the old predecessor keys on this node.
	if !Equals(old.ID, node.ID) {
		_, deleted, err := node.dictionary.Partition(old.ID, node.ID) // Obtain the keys to delete.
		if err != nil {
			message := "Error obtaining old predecessor keys replicated on this node."
			log.Error(message)
			return nil, nil, errors.New(message + "\n" + err.Error())
		}

		log.Debugf("Keys to delete: %s", deleted)

		err = node.dictionary.Discard(Keys(out)) // Delete the keys of the old predecessor.
		if err != nil {
			message := "Error deleting old predecessor keys on this node."
			log.Error(message)
			return nil, nil, errors.New(message + "\n" + err.Error())
		}

		log.Debug("Successful delete of old predecessor replicated keys in this node.")
		return out, deleted, nil
	}
	return out, nil, nil
}

func (node *Node) UpdatePredecessorKeys(old *chord.Node) {
	// Lock the predecessor to read it, and unlock it after.
	node.predLock.Lock()
	pred := node.predecessor
	node.predLock.Unlock()

	// Lock the successor to write on it, and unlock it after.
	node.sucLock.Lock()
	suc := node.successors.Beg()
	node.sucLock.Unlock()

	if !Equals(pred.ID, node.ID) {
		// Transfer the corresponding keys to the new predecessor.
		transferred, deleted, err := node.DeletePredecessorKeys(old)
		if err != nil {
			log.Errorf("Error updating new predecessor keys.\n%s", err.Error())
			return
		}

		// Build the new predecessor dictionary, by transferring its correspondent keys.
		err = node.RPC.Extend(pred, &chord.ExtendRequest{Dictionary: transferred})
		if err != nil {
			log.Errorf("Error transferring keys to the new predecessor.\n%s", err.Error())
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
		log.Debug("Successful transfer of keys to the new predecessor.")

		// If successor exists, and it's different from the new predecessor, delete the transferred keys
		// from successor storage replication.
		if !Equals(suc.ID, node.ID) && !Equals(suc.ID, pred.ID) {
			err = node.RPC.Discard(suc, &chord.DiscardRequest{Keys: Keys(transferred)})
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

func (node *Node) UpdateSuccessorKeys() {
	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	// Lock the successor to write on it, and unlock it after.
	node.sucLock.Lock()
	suc := node.successors.Beg()
	node.sucLock.Unlock()

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
		log.Debugf("Keys to transfer: %s", out)
		log.Debugf("Remaining keys: %s", in)

		// Transfer this node keys to the new successor, to update it.
		err = node.RPC.Extend(suc, &chord.ExtendRequest{Dictionary: in})
		if err != nil {
			log.Errorf("Error transferring keys to the successor at %s.\n%s", suc.IP, err.Error())
			return
		}
		log.Trace("Successful transfer of keys to the successor.")
	}
}

func (node *Node) BroadListen() {
	pc, err := net.ListenPacket("udp4", ":8830")
	for err != nil {
		pc, err = net.ListenPacket("udp4", ":8830")
	}

	for {
		if !IsOpen(node.shutdown) {
			err = pc.Close()
			if err != nil {
				return
			}
			return
		}

		buf := make([]byte, 1024)
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			continue
		}

		log.Trace("%s sent this: %s", addr, buf[:n])

		_, err = pc.WriteTo([]byte("Hello"), addr)
		if err != nil {
			continue
		}
	}
}

func (node *Node) NetDiscover(ip net.IP) (string, error) {
	ip[3] = 255
	broadcast := ip.String() + ":8830"

	pc, err := net.ListenPacket("udp4", ":8830")
	if err != nil {
		return "", err
	}

	out, err := net.ResolveUDPAddr("udp4", broadcast)
	if err != nil {
		return "", err
	}

	log.Info("UPD address resolved.")

	_, err = pc.WriteTo([]byte("AAAAAAAAAA"), out)
	if err != nil {
		return "", err
	}

	log.Info("Message broadcast done.")

	buf := make([]byte, 1024)
	err = pc.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return "", err
	}

	log.Info("Waiting for response.")

	for i := 0; i < 2; i++ {
		n, address, err := pc.ReadFrom(buf)

		if err == nil && node.IP+":8830" != address.String() {
			log.Infof("%s sent this: %s", address, buf[:n])
			return strings.Split(address.String(), ":")[0], nil
		}
	}

	log.Info("Deadline for response exceed.")
	return "", nil
}

func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}

	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {

		}
	}(conn)

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}
