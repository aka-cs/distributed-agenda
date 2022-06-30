package chord

import (
	"server/chord/chord"
)

// FingerTable definition.
type FingerTable []*chord.Node

// NewFingerTable creates and return a new FingerTable.
func NewFingerTable(size int) FingerTable {
	hand := make([]*chord.Node, size) // Build the new array of fingers.

	// Build and then add the necessary fingers to the array.
	for i := range hand {
		hand[i] = nil
	}

	// Return the FingerTable.
	return hand
}
