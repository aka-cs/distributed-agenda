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

// FingerID computes the offset by (n + 2^i) mod (2^m)
func FingerID(n []byte, i int, m int) []byte {
	// Convert the ID to a bigint
	id := (&big.Int{}).SetBytes(n)

	// Calculates 2^i.
	two := big.NewInt(2)
	pow := big.Int{}
	pow.Exp(two, big.NewInt(int64(i)), nil)

	// Calculates the sum of n and 2^i.
	sum := big.Int{}
	sum.Add(id, &pow)

	// Calculates 2^m.
	pow = big.Int{}
	pow.Exp(two, big.NewInt(int64(m)), nil)

	// Apply the mod.
	id.Mod(&sum, &pow)

	// Return the result.
	return id.Bytes()
}

// Equal checks if two IDs are equals.
func Equal(ID1, ID2 []byte) bool {
	return bytes.Compare(ID1, ID2) == 0
}

// Between checks if an ID is in the (L, R] interval, on the chord ring.
func Between(ID, L, R []byte) bool {
	// Convert the IDs from bytes to big.Int.
	id := (&big.Int{}).SetBytes(ID)
	l := (&big.Int{}).SetBytes(L)
	r := (&big.Int{}).SetBytes(R)

	// If L < R, return true if L < ID <= R.
	if l.Cmp(r) < 0 {
		return l.Cmp(id) < 0 && 0 <= r.Cmp(id)
	}

	// If L >= R, this is a segment over the end of the ring.
	// So, ID is between L and R if L < ID or ID <= R.
	return l.Cmp(id) < 0 || 0 <= r.Cmp(id)
}

// Found the closest Finger preceding this ID.
func (node *Node) closestFinger(ID []byte) *chord.Node {
	// Iterate the finger table in reverse, and return the first finger
	// such that the finger ID is between this node ID and the parameter ID.
	for i := len(node.fingerTable) - 1; i >= 0; i-- {
		if Between(node.fingerTable[i].ID, node.Id, ID) {
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
