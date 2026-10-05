package rapide

import "net"

type connection struct {
	// socket for this machine
	socket *net.UDPConn

	// address this connection is talking to
	peerAddr *net.UDPAddr
}

// Send writes data to the peer address of this connection.
func (c *connection) Send(data []byte) error {
	_, err := c.socket.Write(data)
	return err
}

// readLoop starts the read loop for the current connection
func (c *connection) readLoop() {
	buf := make([]byte, 1500)

	for {
		n, err := c.socket.Read(buf)
		if err != nil {
			return
		}

		_ = buf[:n]
	}
}
