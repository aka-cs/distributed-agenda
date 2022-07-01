package chord

import (
	log "github.com/sirupsen/logrus"
	"server/chord/chord"
	"time"
)

// Node server periodically threads.

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
				in, out, err := node.dictionary.Partition(pred.ID, node.ID) // Obtain the predecessor keys.
				node.dictLock.RUnlock()
				if err != nil {
					log.Error("Error obtaining predecessor keys.\n")
					return
				}

				log.Debug("Transferring old predecessor keys to the successor.\n")
				log.Debug("Out:\n")
				log.Debug(out)
				log.Debug("In:\n")
				log.Debug(in)
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
	in, out, err := node.dictionary.Partition(pred.ID, node.ID) // Obtain this node keys.
	node.dictLock.RUnlock()
	if err != nil {
		log.Error("Error obtaining this node keys.\n")
		return
	}

	log.Debug("Transferring keys to the new successor.\n")
	log.Debug("In:\n")
	log.Debug(in)
	log.Debug("Out:\n")
	log.Debug(out)
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

// FixFinger update a particular finger on the finger table, and return the index of the next finger to update.
func (node *Node) FixFinger(index int) int {
	log.Trace("Fixing finger entry.\n")

	m := node.config.HashSize            // Obtain the finger table size.
	ID := FingerID(node.ID, index, m)    // Obtain n + 2^(next) mod (2^m).
	suc, err := node.FindIDSuccessor(ID) // Obtain the node that succeeds ID = n + 2^(next) mod (2^m).
	// In case of error finding the successor, report the error and skip this finger.
	if err != nil || suc == nil {
		log.Error("Successor of ID not found.\nThis finger fix was skipped.\n")
		// Return the next index to fix.
		return (index + 1) % m
	}
	// If the successor of this ID is this node, then the ring has already been turned around.
	// Clean the remaining positions and return index 0 to restart the fixing cycle.
	if Equals(suc.ID, node.ID) {
		for i := index; index < m; i++ {
			node.fingerLock.Lock()        // Lock finger table to write on it, and unlock it after.
			node.fingerTable[index] = nil // Clean the correspondent position.
			node.fingerLock.Unlock()
		}
		return 0
	}

	node.fingerLock.Lock()        // Lock finger table to write on it, and unlock it after.
	node.fingerTable[index] = suc // Update the correspondent position on the finger table.
	node.fingerLock.Unlock()

	// Return the next index to fix.
	return (index + 1) % m
}

// PeriodicallyFixFinger periodically fix finger tables.
func (node *Node) PeriodicallyFixFinger() {
	log.Debug("Fix finger thread started.\n")

	next := 0                                        // Index of the actual finger entry to fix.
	ticker := time.NewTicker(100 * time.Millisecond) // Set the time between routine activations.
	for {
		select {
		case <-ticker.C:
			next = node.FixFinger(next) // If it's time, fix the correspondent finger table entry.
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

// FixStorage fix a particular key location on storage dictionary.
func (node *Node) FixStorage(key string) {
	// Lock the predecessor to read it, and unlock it after.
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	keyNode, err := node.LocateKey(key)
	if err != nil {
		return
	}

	if !Equals(keyNode.ID, pred.ID) && !Equals(keyNode.ID, node.ID) {
		node.dictLock.RLock()
		value, err := node.dictionary.Get(key)
		node.dictLock.RUnlock()
		if err != nil {
			return
		}

		err = node.RPC.Set(keyNode, &chord.SetRequest{Key: key, Value: value})
		if err != nil {
			return
		}

		node.dictLock.Lock()
		node.dictionary.Delete(key)
		node.dictLock.Unlock()
		if err != nil {
			return
		}
	}
}

// PeriodicallyFixStorage periodically fix storage dictionary.
func (node *Node) PeriodicallyFixStorage() {
	log.Debug("Fix storage thread started.\n")

	next := 0                                        // Index of the actual storage entry to fix.
	keys := make([]string, 0)                        // Keys to fix.
	ticker := time.NewTicker(500 * time.Millisecond) // Set the time between routine activations.
	for {
		select {
		case <-ticker.C:
			if next == len(keys) {
				// Lock the predecessor to read it, and unlock it after.
				node.predLock.RLock()
				pred := node.predecessor
				node.predLock.RUnlock()

				node.dictLock.RLock()
				_, out, err := node.dictionary.Partition(pred.ID, node.ID) // Obtain the replicated keys.
				node.dictLock.RUnlock()
				if err != nil {
					continue
				}

				keys = Keys(out)
				next = 0
			}

			if next < len(keys) {
				node.FixStorage(keys[next]) // If it's time, fix the correspondent storage entry.
				next++
			}
		case <-node.shutdown:
			ticker.Stop() // If node server is shutdown, stop the thread.
			return
		}
	}
}
