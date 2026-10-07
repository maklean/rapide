package rapide

import (
	"net"
	"sync"

	"github.com/maklean/rapide/internal/sequence"
	"github.com/maklean/rapide/internal/structures"
)

type connection struct {
	// socket for this machine
	socket *net.UDPConn

	// address this connection is talking to
	peerAddr *net.UDPAddr

	// sequence number information for the listener
	sequenceInfo sequence.SequenceInfo

	// queue of packets waiting to be acknowledged
	ackQueue structures.RingBuffer[unackedPacket]

	// mutex for the ackQueue
	ackQueueMu sync.Mutex
}

// Send writes data to the peer address of this connection.
func (c *connection) Send(payload []byte, reliable bool) error {
	packet := Packet{
		RawSequenceNumber: c.sequenceInfo.AdvanceOut().Value,
		PayloadLength:     uint16(len(payload)),
		Payload:           payload,
		IsReliable:        reliable,
		IsAck:             false,
		AckSequenceNumber: 0,
	}

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
			continue
		}

		packet, err := FromBytes(buf[:n])
		if err != nil {
			continue
		}

		// TODO: add to ack queue if reliable.
		if packet.IsReliable {

		}
	}
}
