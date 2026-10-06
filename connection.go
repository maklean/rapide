package rapide

import (
	"net"

	"github.com/maklean/rapide/internal/sequence"
)

type connection struct {
	// socket for this machine
	socket *net.UDPConn

	// address this connection is talking to
	peerAddr *net.UDPAddr

	// sequence number information for the listener
	sequenceInfo sequence.SequenceInfo
}

// Send writes data to the peer address of this connection.
func (c *connection) Send(payload []byte) error {
	packet := Packet{RawSequenceNumber: c.sequenceInfo.AdvanceOut().Value, PayloadLength: uint16(len(payload)), Payload: payload}

	data, err := packet.ToBytes()
	if err != nil {
		return err
	}

	_, err = c.socket.Write(data)
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
