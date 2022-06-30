package chord

import (
	log "github.com/sirupsen/logrus"
	"server/chord/chord"
)

// Finger structure.
type Finger struct {
	ID   []byte      // ID which corresponds to this finger.
	Node *chord.Node // Successor of ID in the chord ring.
}

// FingerTable definition.
type FingerTable []*Finger

// NewFingerTable creates and return a new FingerTable.
func NewFingerTable(size int) FingerTable {
	hand := make([]*Finger, size) // Build the new array of fingers.

	// Build and then add the necessary fingers to the array.
	for i := range hand {
		hand[i] = nil
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
		node.fingerLock.RLock()
		finger := node.fingerTable[i]
		node.fingerLock.RUnlock()

		if finger != nil && Between(finger.Node.ID, node.ID, ID) {
			return finger.Node
		}
	}

	// If no finger meets the conditions, return this node.
	return node.Node
}
