package chord

import (
	"DistributedTable/chord"
	"bytes"
	"math/big"
)

// Finger structure.
type Finger struct {
	ID   []byte
	Node *chord.Node
}

// FingerTable Table structure
type FingerTable struct {
	node *chord.Node
	Hand []Finger
	size int
}

// FingerTable constructor.
func newFingerTable(node *chord.Node, size int) *FingerTable {
	// TODO: Look how to add Node information to the fingers.

	// Build the new array of fingers.
	hand := make([]Finger, size)

	// Build and then add the necessary fingers to the array.
	for i := range hand {
		hand[i] = Finger{fingerID(node.Id, i, size), nil}
	}

	// Return the FingerTable.
	return &FingerTable{node, hand, size}
}

// Computes the offset by (n + 2^i) mod (2^m)
func fingerID(n []byte, i int, m int) []byte {

	// Convert the ID to a bigint
	idInt := (&big.Int{}).SetBytes(n)

	// Calculates 2^i.
	two := big.NewInt(2)
	pow := big.Int{}
	pow.Exp(two, big.NewInt(int64(i)), nil)

	// Calculates the sum of n and 2^i.
	sum := big.Int{}
	sum.Add(idInt, &pow)

	// Get the ceiling.
	ceil := big.Int{}
	ceil.Exp(two, big.NewInt(int64(m)), nil)

	// Apply the mod.
	idInt.Mod(&sum, &ceil)

	// Return the result.
	return idInt.Bytes()
}

// Checks if two IDs are equals.
func isEqual(ID1, ID2 []byte) bool {
	return bytes.Compare(ID1, ID2) == 0
}

// Checks if an ID is between two others, on the chord ring.
func inBetween(ID, L, R []byte) bool {
	// Convert the IDs from bytes to big.Int.
	IDi := (&big.Int{}).SetBytes(ID)
	Li := (&big.Int{}).SetBytes(L)
	Ri := (&big.Int{}).SetBytes(R)

	// If L < R.
	if Li.Cmp(Ri) < 0 {
		// Return true if L < ID < R.
		return Li.Cmp(IDi) < 0 && 0 < Ri.Cmp(IDi)
	}

	// If L >= R, this is a segment over the end of the ring.
	// So, ID is between L and R if L < ID or ID < R.
	return Li.Cmp(IDi) < 0 || 0 < Ri.Cmp(IDi)
}

// Found the closest Finger preceding this ID.
func (table *FingerTable) closestFinger(ID []byte) *chord.Node {
	// TODO: Change the classic for notation to inverse for range loop notation.

	n := len(table.Hand)

	for i := n - 1; i >= 0; i-- {
		// Iterate the finger table in reverse, and return the first finger
		// such that the ID is not between the finger ID and the initial node ID.
		if !inBetween(ID, table.node.Id, table.Hand[i].ID) {
			return table.Hand[i].Node
		}
	}

	return table.node
}

// TODO: Fix Fingers
