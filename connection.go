package rapide

import (
	"errors"
	"log"
	"net"
	"sync"

	"github.com/maklean/rapide/internal/config"
	"github.com/maklean/rapide/internal/sequence"
	"github.com/maklean/rapide/internal/structures"
	"github.com/maklean/rapide/internal/writer"
)

type connection struct {
	// identifier for this connection
	connectionID uint32

	// socket for this connection
	socket *net.UDPConn

	// packet writer for this connection
	writer writer.PacketWriter

	// next incoming sequence number
	nextIn sequence.SequenceNumber

	// next outgoing sequence number
	nextOut sequence.SequenceNumber

	// queue of packets waiting to be acknowledged
	ackQueue *structures.RingBuffer[unackedPacket]

	// mutex for the ackQueue
	ackQueueMu sync.Mutex

	// incoming packets channel for connection
	incoming chan *Packet

	// outgoing payloads channel for connection
	outgoing chan []byte

	// handshake channel
	established chan struct{}

	// needed for a thread-safe handshake establishment
	establishedOnce sync.Once

	// signal for closing connection
	closed chan struct{}

	// needed for a thread-safe connection termination
	closedOnce sync.Once

	// closure called as the connection's about to close (listener/server = rmv. from peers map, client = close socket)
	onClose func()

	// signal for when we're finished closing/running the connection's loop
	finished chan struct{}
}

// Send sends a byte payload to the peer address of this connection
func (c *connection) Send(payload []byte) error {
	if !c.isEstablished() {
		return errors.New("connection not established")
	} else if c.isClosed() {
		return errors.New("connection is closed")
	}

	select {
	case c.outgoing <- payload:
		return nil
	default:
		return errors.New("outgoing queue full")
	}
}

// Close closes the connection with the peer.
func (c *connection) Close() error {
	if c.isClosed() {
		return errors.New("connection is closed")
	}

	c.closedOnce.Do(func() {
		// send signal to close channel then wait for finished signal to come
		close(c.closed)
		<-c.finished
	})

	return nil
}

// newConnection creates a new connection struct instance who's connected to the given peer address.
func newConnection(socket *net.UDPConn, writer writer.PacketWriter, onClose func()) *connection {
	conn := &connection{
		socket:      socket,
		writer:      writer,
		onClose:     onClose,
		ackQueue:    structures.NewRingBuffer[unackedPacket](),
		incoming:    make(chan *Packet, config.MaxIncomingPackets),
		outgoing:    make(chan []byte, config.MaxOutgoingPackets),
		established: make(chan struct{}),
		closed:      make(chan struct{}),
		finished:    make(chan struct{}),
	}

	// connection loop - handles incoming packets for connection
	go conn.run()

	return conn
}

// deliver adds the given packet to the connections incoming packets channel.
func (c *connection) deliver(p *Packet) {
	select {
	case c.incoming <- p:
	default:
	}
}

// run is the client's read loop
func (c *connection) run() {
	defer close(c.finished)

	for {
		select {
		case packet := <-c.incoming:
			c.handleIncoming(packet)
		case payload := <-c.outgoing:
			c.handleOutgoing(payload)
		case <-c.closed:
			// flush outgoing channel, run close closure, send finished signal by exiting
			close(c.outgoing)

			for payload := range c.outgoing {
				c.handleOutgoing(payload)
			}

			if c.onClose != nil {
				c.onClose()
			}

			return
		}
	}
}

// handleIncoming handles the given packet from the peer.
func (c *connection) handleIncoming(p *Packet) {
	if p.IsHandshakeServer {
		// set connection ID if this is a handshake packet, and closed established channel.
		c.establishedOnce.Do(func() {
			c.connectionID = p.ConnectionID
			close(c.established)
		})

		return
	}

	// only accept packet if sequence number is greater or equal to this connections incoming sequence number
	if c.nextIn.Gt(p.GetSn()) {
		return
	}

	log.Printf("Got: %s", string(p.Payload))
}

// handleOutgoing handles sending the given payload to the peer.
func (c *connection) handleOutgoing(payload []byte) {
	packet := Packet{
		ConnectionID:      c.connectionID,
		RawSequenceNumber: c.nextOut.Value,
		PayloadLength:     uint16(len(payload)),
		Payload:           payload,
	}

	// TODO: no idea how to return errors to the user from here.
	data, _ := packet.ToBytes()

	c.writer.Write(data)
}

// isEstablished returns whether a connection with a peer was successfully established.
func (c *connection) isEstablished() bool {
	select {
	case <-c.established:
		return true
	default:
		return false
	}
}

// isClosed returns whether a connection with a peer was terminated.
func (c *connection) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}
