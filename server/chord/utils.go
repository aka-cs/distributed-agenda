package chord

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	"hash"
	"math/big"
	"net"
	"server/chord/chord"
)

// Useful definitions.
var (
	emptyRequest  = &chord.EmptyRequest{}
	emptyResponse = &chord.EmptyResponse{}
)

func IsOpen[T any](channel <-chan T) bool {
	select {
	case <-channel:
		return false
	default:
		if channel == nil {
			return false
		}
		return true
	}
}

// GetOutboundIP get the IP of this host in the net.
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

func Keys[T](dictionary map[string]T) []string {
	keys := make([]string, 0)

	for key := range dictionary {
		keys = append(keys, key)
	}

	return keys
}

// FingerID computes the offset by (n + 2^i) mod(2^m)
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

// Equals checks if two IDs are equals.
func Equals(ID1, ID2 []byte) bool {
	return bytes.Compare(ID1, ID2) == 0
}

func KeyBetween(key string, hash func() hash.Hash, L, R []byte) (bool, error) {
	ID, err := HashKey(key, hash) // Obtain the correspondent ID of the key.
	if err != nil {
		return false, err
	}

	return Equals(ID, R) || Between(ID, L, R), nil
}

// Between checks if an ID is in the (L, R) interval, on the chord ring.
func Between(ID, L, R []byte) bool {
	// If L <= R, return true if L < ID < R.
	if bytes.Compare(L, R) <= 0 {
		return bytes.Compare(L, ID) < 0 && bytes.Compare(ID, R) < 0
	}

	// If L > R, this is a segment over the end of the ring.
	// So, ID is between L and R if L < ID or ID < R.
	return bytes.Compare(L, ID) < 0 || bytes.Compare(ID, R) < 0
}

func HashKey(key string, hash func() hash.Hash) ([]byte, error) {
	log.Trace("Hashing key: " + key + ".\n")
	h := hash()
	if _, err := h.Write([]byte(key)); err != nil {
		log.Error("Error hashing key " + key + ".\n" + err.Error() + ".\n")
		return nil, err
	}
	value := h.Sum(nil)

	return value, nil
}
