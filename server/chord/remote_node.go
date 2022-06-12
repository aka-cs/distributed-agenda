package chord

import (
	"DistributedTable/chord"
	"google.golang.org/grpc"
	"time"
)

// RemoteNode struct stores the properties of a connection with a remote chord Node.
type RemoteNode struct {
	chord.ChordClient // Chord client connected with the remote Node server.

	addr       string           // Address of the remote Node.
	conn       *grpc.ClientConn // Grpc connection with the remote Node server.
	lastActive time.Time        // Last time the connection was used.
}

// CloseConnection close the connection with a RemoteNode.
func (connection *RemoteNode) CloseConnection() {
	err := connection.conn.Close()
	if err != nil {
		return
	}
}
