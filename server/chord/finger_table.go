package chord

import (
	log "github.com/sirupsen/logrus"
	"server/chord/chord"
	"time"
)

// Finger structure.
type Finger struct {
	ID   []byte      // ID which corresponds to this finger.
	Node *chord.Node // Successor of ID in the chord ring.
}

// FingerTable definition.
type FingerTable []Finger

// NewFingerTable creates and return a new FingerTable.
func NewFingerTable(node *chord.Node, size int) FingerTable {
	hand := make([]Finger, size) // Build the new array of fingers.

	// Build and then add the necessary fingers to the array.
	for i := range hand {
		hand[i] = Finger{FingerID(node.ID, i, size), node}
	}

	// Return the FingerTable.
	return hand
}

// Node finger table methods.

// ClosestFinger find the closest finger preceding this ID.
func (node *Node) ClosestFinger(ID []byte) *chord.Node {
	log.Trace("Finding the closest finger preceding this ID.\n")
	defer log.Trace("Closest finger found.\n")

	// Iterate the finger table in reverse, and return the first finger
	// such that the finger ID is between this node ID and the parameter ID.
	for i := len(node.fingerTable) - 1; i >= 0; i-- {
		if Between(node.fingerTable[i].ID, node.ID, ID, false, true) {
			return node.fingerTable[i].Node
		}
	}

	// If no finger meets the conditions, return this node.
	return node.Node
}

// FixFinger update a particular finger on the finger table, and return the index of the next finger to update.
func (node *Node) FixFinger(index int) int {
	log.Trace("Fixing finger entry.\n")

	m := node.config.HashSize            // Obtain the finger table size.
	ID := FingerID(node.ID, index, m)    // Obtain n + 2^(next) mod (2^m).
	suc, err := node.FindIDSuccessor(ID) // Obtain the node that succeeds ID = n + 2^(next) mod (2^m).
	// In case of error finding the successor, report the error and retry.
	if err != nil || suc == nil {
		log.Error("Successor of ID not found.\nThe finger fix is postponed.\n")
		// Return the same index, to retry later.
		return index
	}
	// If the successor of this ID is this node, then the ring has already been turned around.
	// Return index 0 to restart the fixing cycle.
	if Equals(ID, node.ID) {
		return 0
	}

	finger := Finger{ID, suc}        // Create the correspondent finger with the obtained node.
	node.fingerLock.Lock()           // Lock finger table to write on it, and unlock it after.
	node.fingerTable[index] = finger // Update the correspondent position on the finger table.
	node.fingerLock.Unlock()

	// Return the next index to fix.
	return (index + 1) % m
}

// PeriodicallyFixFinger periodically fix finger tables.
func (node *Node) PeriodicallyFixFinger() {
	log.Debug("Fix finger thread started.\n")

	next := 0                                  // Index of the actual finger entry to fix.
	ticker := time.NewTicker(10 * time.Second) // Set the time between routine activations.
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
