package chord

import (
	"DistributedTable/chord"
	"time"
)

// Finger structure.
type Finger struct {
	ID   []byte
	Node *chord.Node
}

// FingerTable definition.
type FingerTable []Finger

// FingerTable constructor.
func newFingerTable(node *chord.Node, size int) FingerTable {
	// TODO: Look how to add Node information to the fingers.
	hand := make([]Finger, size) // Build the new array of fingers.

	// Build and then add the necessary fingers to the array.
	for i := range hand {
		hand[i] = Finger{FingerID(node.Id, i, size), nil}
	}

	// Return the FingerTable.
	return hand
}

// Node finger table methods.

// ClosestFinger found the closest Finger preceding this ID.
func (node *Node) ClosestFinger(ID []byte) *chord.Node {
	// Iterate the finger table in reverse, and return the first finger
	// such that the finger ID is between this node ID and the parameter ID.
	for i := len(node.fingerTable) - 1; i >= 0; i-- {
		if Between(node.fingerTable[i].ID, node.Id, ID, false, true) {
			return node.fingerTable[i].Node
		}
	}

	// If no finger meets the conditions, return this node.
	return node.Node
}

// FixFinger update a particular finger on the finger table, and return the index of the next finger to update.
func (node *Node) FixFinger(next int) int {
	m := node.config.HashSize                // Obtain the ring size.
	nextID := FingerID(node.Id, next, m)     // Obtain n + 2^(next) mod (2^m).
	suc, err := node.FindIDSuccessor(nextID) // Obtain the node that succeeds ID = n + 2^(next) mod (2^m).
	if err != nil || suc == nil {
		// TODO: Check how to handle retry, passing ahead for now
		// Return the next index to fix.
		return (next + 1) % m
	}

	finger := Finger{nextID, suc}   // Create the correspondent finger with the obtained node.
	node.fingerLock.Lock()          // Lock finger table to write on it, and unlock it after.
	node.fingerTable[next] = finger // Update the correspondent position on the finger table.
	node.fingerLock.Unlock()

	// Return the next index to fix.
	return (next + 1) % m
}

// PeriodicallyFixFinger periodically fix finger tables.
func (node *Node) PeriodicallyFixFinger() {
	next := 0                                        // Index of the actual finger entry to fix.
	ticker := time.NewTicker(100 * time.Millisecond) // Set the time between routine activations.
	for {
		select {
		case <-ticker.C:
			next = node.FixFinger(next) // If it's time, fix the correspondent finger table entry.
		}
	}
}
