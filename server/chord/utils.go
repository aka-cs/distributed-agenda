package chord

import (
	"DistributedTable/chord"
	"bytes"
	"hash"
	"math/big"
)

// Necessary definitions.
var (
	nullNode     = &chord.Node{}
	emptyRequest = &chord.EmptyRequest{}
)

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

// Between checks if an ID is in the (L, R) interval, on the chord ring.
func Between(ID, L, R []byte) bool {
	// If L < R, return true if L < ID < R.
	if bytes.Compare(L, R) < 0 {
		return bytes.Compare(L, ID) < 0 && 0 < bytes.Compare(R, ID)
	}

	// If L >= R, this is a segment over the end of the ring.
	// So, ID is between L and R if L < ID or ID < R.
	return bytes.Compare(L, ID) < 0 || 0 < bytes.Compare(R, ID)
}

func HashKey(key string, hash func() hash.Hash) ([]byte, error) {
	h := hash()
	h.Write([]byte(key))
	value := h.Sum(nil)

	return value, nil
}