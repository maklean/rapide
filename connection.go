package rapide

import "github.com/maklean/rapide/internal"

type Connection interface {
	Placeholder()
}

type connection struct {
	// identifier for this connection
	connectionID uint32

	// udp connection to peer
	udpConnection internal.UDPConnection
}

// newConnection creates a new connection struct instance.
func newConnection(connectionID uint32, udpConn internal.UDPConnection) *connection {
	conn := &connection{
		connectionID:  connectionID,
		udpConnection: udpConn,
	}

	return conn
}

func (c *connection) Placeholder() {}
