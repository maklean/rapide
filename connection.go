package rapide

import (
	"context"
	"fmt"
	"math"
	"net"
	"sync"
	"time"

	"github.com/maklean/rapide/internal"
)

type Connection interface {
	Send([]byte, bool) error
	Read([]byte) (int, error)
	RemoteAddr() net.Addr
	LocalAddr() net.Addr
	Close() error
	IsClosed() bool
}

type connection struct {
	// connection identifier
	connectionID uint32

	// udp connection to peer
	udpConnection internal.UDPConnection

	// reliable sequence numbers
	reliableSeq internal.Sequence

	// unreliable sequence numbers
	unreliableSeq internal.Sequence

	// map of sequence number => unacknowledged reliable packet
	unacked map[uint16]*internal.UnackedPacket

	// map of sequence number => packet that has arrived
	pending map[uint16]*internal.Packet

	// incoming reliable packets
	relIncoming chan *internal.Packet

	// incoming unreliable packets
	unrelIncoming chan *internal.Packet

	// incoming packets (for user API / Connection.Read())
	incomingRead chan []byte

	// outgoing reliable payloads
	relOutgoing chan []byte

	// outgoing unreliable payloads
	unrelOutgoing chan []byte

	// retransmission timer
	retransmissionTimer internal.RetransmissionTimer

	ctx        context.Context
	cancel     context.CancelCauseFunc
	cancelOnce sync.Once
	onClose    func()
}

// Send sends the given payload to the connection's peer.
func (c *connection) Send(data []byte, reliable bool) error {
	if c.IsClosed() {
		return internal.ErrClientConnectionClosed
	}

	if len(data) > math.MaxUint16 {
		// TODO: idk if I'm supposed to fragment this or something
		return fmt.Errorf("%w: got payload size of %d (limit is %d)", internal.ErrPayloadTooLarge, len(data), math.MaxUint16)
	}

	ch := c.unrelOutgoing
	if reliable {
		ch = c.relOutgoing
	}

	for {
		select {
		case ch <- data:
			return nil
		case <-c.ctx.Done():
			return internal.ErrClientConnectionClosed
		default:
			return internal.ErrOutgoingFull
		}
	}
}

// Read reads incoming data received from the peer. If data is not immediately available, Read blocks.
// The incoming data is put into the given buffer and the number of bytes written into the buffer is returned.
func (c *connection) Read(buf []byte) (int, error) {
	if c.IsClosed() {
		return 0, internal.ErrClientConnectionClosed
	}

	for {
		select {
		case data := <-c.incomingRead:
			n := copy(buf, data)
			return n, nil
		case <-c.ctx.Done():
			return 0, internal.ErrClientConnectionClosed
		}
	}
}

// LocalAddr returns the local address this connection is bound to.
func (c *connection) LocalAddr() net.Addr {
	return c.udpConnection.LocalAddr()
}

// RemoteAddr returns the peer's address.
func (c *connection) RemoteAddr() net.Addr {
	return c.udpConnection.RemoteAddr()
}

// Close closes the connection.
func (c *connection) Close() error {
	if c.IsClosed() {
		return internal.ErrClientConnectionClosed
	}

	c.close(internal.ErrCloseRequest)
	return nil
}

// IsClosed returns whether the connection is closed.
func (c *connection) IsClosed() bool {
	return c.ctx.Err() != nil
}

// newConnection creates a new connection struct instance.
func newConnection(parent context.Context, connectionID uint32, udpConn internal.UDPConnection, onClose func()) *connection {
	ctx, cancel := context.WithCancelCause(parent)

	conn := &connection{
		connectionID:        connectionID,
		udpConnection:       udpConn,
		reliableSeq:         internal.NewSequence(),
		unreliableSeq:       internal.NewSequence(),
		unacked:             make(map[uint16]*internal.UnackedPacket, internal.ConnectionReceiveWindowSize),
		pending:             make(map[uint16]*internal.Packet),
		relIncoming:         make(chan *internal.Packet, internal.ConnectionIncomingBacklog),
		unrelIncoming:       make(chan *internal.Packet, internal.ConnectionIncomingBacklog),
		incomingRead:        make(chan []byte, internal.ConnectionIncomingBacklog),
		relOutgoing:         make(chan []byte, internal.ConnectionOutgoingBacklog),
		unrelOutgoing:       make(chan []byte, internal.ConnectionOutgoingBacklog),
		retransmissionTimer: internal.RetransmissionTimerInit(),
		ctx:                 ctx,
		cancel:              cancel,
		onClose:             onClose,
	}

	go conn.run()

	return conn
}

func (c *connection) run() {
	ticker := time.NewTicker(internal.ConnectionTickInterval * time.Millisecond)

	defer c.close(context.Cause(c.ctx))
	defer ticker.Stop()

	for {
		select {
		case p := <-c.relIncoming:
			c.handleIncomingReliable(p)
		case p := <-c.unrelIncoming:
			c.handleIncomingUnreliable(p)
		case data := <-c.relOutgoing:
			c.handleOutgoing(data, true)
		case data := <-c.unrelOutgoing:
			c.handleOutgoing(data, false)
		case t := <-ticker.C:
			c.handleTick(t)
		case <-c.ctx.Done():
			return
		}
	}
}

// handleIncomingReliable handles incoming reliable packets.
func (c *connection) handleIncomingReliable(p *internal.Packet) {
	if p.IsAck {
		up, ok := c.unacked[p.AckSequenceNumber]
		if !ok {
			return
		}

		// Karn's Algorithm states we should only take measurement for packet's that weren't retransmitted
		if up.Retries == 0 {
			rtt := float64(time.Since(up.LastSend)) / float64(time.Millisecond)
			c.retransmissionTimer.DoMeasurement(rtt)
		}

		delete(c.unacked, p.AckSequenceNumber)
		return
	}

	sn := p.Sn()

	dist := sn.Distance(c.reliableSeq.NextIn)

	if dist < 0 {
		// could be a duplicate packet or caused by a lost ACK
		c.sendAck(sn)
		return
	}

	if dist >= internal.ConnectionReceiveWindowSize {
		return
	}

	if len(c.incomingRead) == cap(c.incomingRead) && dist == 0 {
		// cannot deliver packet immediately b/c channel is full
		return
	}

	c.sendAck(sn)
	c.pending[sn.Value] = p

	// reorder packets starting from the current next incoming sequence number, then send into incomingRead
	for len(c.incomingRead) < cap(c.incomingRead) {
		p, exists := c.pending[c.reliableSeq.NextIn.Value]

		if !exists {
			break
		}

		select {
		case c.incomingRead <- p.Payload:
			delete(c.pending, c.reliableSeq.NextIn.Value)
			c.reliableSeq.NextIn = c.reliableSeq.NextIn.Next()
		case <-c.ctx.Done():
			return
		default:
			return
		}
	}
}

// handleIncomingUnreliable handles incoming unreliable packets.
func (c *connection) handleIncomingUnreliable(p *internal.Packet) {
	sn := p.Sn()

	if sn.Lt(c.unreliableSeq.NextIn) {
		return
	}

	select {
	case c.incomingRead <- p.Payload:
		c.unreliableSeq.NextIn = sn.Next()
	case <-c.ctx.Done():
		return
	default:
		return
	}
}

// handleOutgoing handles outgoing data.
func (c *connection) handleOutgoing(data []byte, reliable bool) {
	sn := c.unreliableSeq.NextOut

	if reliable {
		sn = c.reliableSeq.NextOut
	}

	p, err := (&internal.Packet{
		ConnectionID:      c.connectionID,
		RawSequenceNumber: sn.Value,
		IsReliable:        reliable,
		PayloadSize:       uint16(len(data)),
		Payload:           data,
	}).ToBytes()

	if err != nil {
		return
	}

	if err := c.udpConnection.Write(p); err != nil {
		if reliable {
			// TODO: pretty sure this is wrong, but assume connection's dead.
			c.close(fmt.Errorf("%w: %v", internal.ErrReliableSendFailed, err))
			return
		}
		return
	}

	if reliable {
		c.unacked[sn.Value] = &internal.UnackedPacket{Packet: p, LastSend: time.Now(), Retries: 0}
		c.reliableSeq.NextOut = sn.Next()
	} else {
		c.unreliableSeq.NextOut = sn.Next()
	}
}

// handleTick handles a connection tick at the given time.
func (c *connection) handleTick(t time.Time) {
	c.drainPending()
	c.handleRetransmission(t)
}

// handleRetransmission handles unacknowledged packet retransmissions.
func (c *connection) handleRetransmission(t time.Time) {
	retransmitted := false

	for sn, up := range c.unacked {
		if float64(t.Sub(up.LastSend).Milliseconds()) < c.retransmissionTimer.RTO {
			continue
		}

		if up.Retries >= internal.MaxRetransmissionRetries {
			c.close(fmt.Errorf("%w: sn=%d, retries=%d", internal.ErrMaxRetransmissionsExceeded, sn, up.Retries))
			return
		}

		// retransmit packet
		if err := c.udpConnection.Write(up.Packet); err == nil {
			up.LastSend = time.Now()
			up.Retries += 1

			retransmitted = true
		}
	}

	if retransmitted {
		c.retransmissionTimer.RTO = min(c.retransmissionTimer.RTO*2, internal.MaxRetransmissionTimeout)
	}
}

// drainPending attempts to send packets that are left in the connection's pending map (due to the incomingRead channel being full)
func (c *connection) drainPending() {
	for {
		p, exists := c.pending[c.reliableSeq.NextIn.Value]

		if !exists {
			break
		}

		select {
		case c.incomingRead <- p.Payload:
			delete(c.pending, c.reliableSeq.NextIn.Value)
			c.reliableSeq.NextIn = c.reliableSeq.NextIn.Next()
		case <-c.ctx.Done():
			return
		default:
			return
		}
	}
}

// sendAck sends an acknowledgement packet for the given sequence number to the connection's peer.
func (c *connection) sendAck(ackSn internal.SequenceNumber) {
	ack, err := (&internal.Packet{ConnectionID: c.connectionID, IsAck: true, AckSequenceNumber: ackSn.Value}).ToBytes()
	if err != nil {
		return
	}

	c.udpConnection.Write(ack)
}

// deliver adds the given packet to the correct incoming channel.
func (c *connection) deliver(p *internal.Packet) error {
	ch := c.unrelIncoming
	if p.IsReliable || p.IsAck {
		ch = c.relIncoming
	}

	select {
	case <-c.ctx.Done():
		return internal.ErrClientConnectionClosed
	case ch <- p:
		return nil
	default:
		return internal.ErrIncomingFull
	}
}

// close closes the connection with the given error/reason.
func (c *connection) close(err error) {
	c.cancelOnce.Do(func() {
		c.cancel(err)

		if c.onClose != nil {
			c.onClose()
		}
	})
}
