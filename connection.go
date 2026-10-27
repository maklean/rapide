package rapide

import (
	"bytes"
	"context"
	"fmt"
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

	// global sequence numbers
	// TODO: If we're never gonna use globalSeq.NextIn, change this to internal.SequenceNumber
	globalSeq internal.Sequence

	// map of sequence number => unacknowledged reliable packet
	unacked map[uint16]*internal.UnackedPacket

	// map of sequence number => packet that has arrived
	pending map[uint16]*internal.Packet

	// incoming reliable packets
	relIncoming chan *internal.Packet

	// incoming unreliable packets
	unrelIncoming chan *internal.Packet

	// reliable receive queue
	relReceive chan []byte

	// unreliable receive queue
	unrelReceive chan []byte

	// outgoing reliable payloads
	relOutgoing chan []byte

	// outgoing unreliable payloads
	unrelOutgoing chan []byte

	// retransmission timer
	retransmissionTimer internal.RetransmissionTimer

	// peer's receive limit
	pReceiveLimit uint16

	// the global sequence number of the packet that got us the current pReceiveLimit
	pReceiveLimitSeq internal.SequenceNumber

	// last time we sent a window update
	lastWindowUpdate time.Time

	ctx        context.Context
	cancel     context.CancelCauseFunc
	cancelOnce sync.Once
	onClose    func()
}

// Send sends the given payload to the connection's peer.
func (c *connection) Send(data []byte, reliable bool) error {
	if c.IsClosed() {
		return fmt.Errorf("%w: %v", internal.ErrClientConnectionClosed, context.Cause(c.ctx))
	}

	if len(data) > internal.MaxPayloadSize {
		// TODO: idk if I'm supposed to fragment this or something
		return fmt.Errorf("%w: got payload size of %d (limit is %d)", internal.ErrPayloadTooLarge, len(data), internal.MaxPayloadSize)
	}

	data = bytes.Clone(data)

	if reliable {
		select {
		case c.relOutgoing <- data:
			return nil
		case <-c.ctx.Done():
			return fmt.Errorf("%w: %v", internal.ErrClientConnectionClosed, context.Cause(c.ctx))
		}
	}

	select {
	case c.unrelOutgoing <- data:
		return nil
	case <-c.ctx.Done():
		return fmt.Errorf("%w: %v", internal.ErrClientConnectionClosed, context.Cause(c.ctx))
	default:
		return internal.ErrOutgoingFull
	}
}

// Read reads incoming data received from the peer. If data is not immediately available, Read blocks.
// The incoming data is put into the given buffer and the number of bytes written into the buffer is returned.
func (c *connection) Read(buf []byte) (int, error) {
	if c.IsClosed() {
		return 0, fmt.Errorf("%w: %v", internal.ErrClientConnectionClosed, context.Cause(c.ctx))
	}

	// prefer reliable first
	select {
	case data := <-c.relReceive:
		return copy(buf, data), nil
	default:
	}

	select {
	case data := <-c.relReceive:
		return copy(buf, data), nil
	case data := <-c.unrelReceive:
		return copy(buf, data), nil
	case <-c.ctx.Done():
		return 0, fmt.Errorf("%w: %v", internal.ErrClientConnectionClosed, context.Cause(c.ctx))
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
		return fmt.Errorf("%w: %v", internal.ErrClientConnectionClosed, context.Cause(c.ctx))
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
		globalSeq:           internal.NewSequence(),
		unacked:             make(map[uint16]*internal.UnackedPacket, internal.ConnectionReceiveWindowSize),
		pending:             make(map[uint16]*internal.Packet),
		relIncoming:         make(chan *internal.Packet, internal.ConnectionIncomingBacklog),
		unrelIncoming:       make(chan *internal.Packet, internal.ConnectionIncomingBacklog),
		relReceive:          make(chan []byte, internal.ConnectionReceiveWindowSize),
		unrelReceive:        make(chan []byte, internal.ConnectionReceiveWindowSize),
		relOutgoing:         make(chan []byte, internal.ConnectionOutgoingBacklog),
		unrelOutgoing:       make(chan []byte, internal.ConnectionOutgoingBacklog),
		retransmissionTimer: internal.NewRetransmissionTimer(),
		pReceiveLimit:       internal.ConnectionReceiveWindowSize, // TODO: rn this isn't configurable, so we'll assume a fully empty ReceiveQueue at the start
		pReceiveLimitSeq:    internal.NewSequenceNumber(0),
		lastWindowUpdate:    time.Now(),
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
		relOutCh := c.relOutgoing
		if c.reliableSeq.NextOut.Geq(internal.NewSequenceNumber(c.pReceiveLimit)) {
			relOutCh = nil // don't send any reliable packets if the peer's receive window is full
		}

		select {
		case p := <-c.relIncoming:
			c.handleIncomingReliable(p)
		case p := <-c.unrelIncoming:
			c.handleIncomingUnreliable(p)
		case data := <-relOutCh:
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
	c.updateReceiveLimit(p)

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
	} else if p.IsWindowUpdate {
		return
	}

	sn := p.Sn()

	dist := sn.Distance(c.reliableSeq.NextIn)

	if dist < 0 {
		// could be a duplicate packet or caused by a lost ACK
		c.sendAck(sn)
		return
	}

	if dist >= int16(cap(c.relReceive)) || (dist == 0 && c.relFree() == 0) {
		return
	}

	c.sendAck(sn)
	c.pending[sn.Value] = p

	// reorder packets starting from the current next incoming sequence number, then send into the receive queue
	for {
		p, exists := c.pending[c.reliableSeq.NextIn.Value]

		if !exists {
			break
		}

		select {
		case c.relReceive <- p.Payload:
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
	c.updateReceiveLimit(p)

	sn := p.Sn()

	if sn.Lt(c.unreliableSeq.NextIn) {
		return
	}

	select {
	case c.unrelReceive <- p.Payload:
		c.unreliableSeq.NextIn = sn.Next()
	case <-c.ctx.Done():
	default:
	}
}

// updateReceiveLimit updates the connection's receive limit based on the packet its peer sent.
func (c *connection) updateReceiveLimit(p *internal.Packet) {
	sn := p.GSn()

	if c.pReceiveLimitSeq.Gt(sn) {
		// old window update, reject
		return
	}

	c.pReceiveLimitSeq = sn
	c.pReceiveLimit = p.ReceiveLimit
}

// handleOutgoing handles outgoing data.
func (c *connection) handleOutgoing(data []byte, reliable bool) {
	sn := c.unreliableSeq.NextOut

	if reliable {
		sn = c.reliableSeq.NextOut
	}

	p, err := (&internal.Packet{
		ConnectionID:         c.connectionID,
		RawSequenceNumber:    sn.Value,
		GlobalSequenceNumber: c.globalSeq.NextOut.Value,
		ReceiveLimit:         c.receiveLimit(),
		IsReliable:           reliable,
		PayloadSize:          uint16(len(data)),
		Payload:              data,
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

	c.globalSeq.NextOut = c.globalSeq.NextOut.Next()

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
	c.checkWindowUpdate(t)
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
		case c.relReceive <- p.Payload:
			delete(c.pending, c.reliableSeq.NextIn.Value)
			c.reliableSeq.NextIn = c.reliableSeq.NextIn.Next()
		case <-c.ctx.Done():
			return
		default:
			return
		}
	}
}

// checkWindowUpdate handles sending receive limit updates to the peer
func (c *connection) checkWindowUpdate(t time.Time) {
	if t.Sub(c.lastWindowUpdate).Milliseconds() < internal.ReceiveLimitUpdateInterval {
		return
	}

	c.sendWindowUpdate()
}

// sendWindowUpdate a window update packet to the client.
func (c *connection) sendWindowUpdate() {
	update, err := (&internal.Packet{
		ConnectionID:         c.connectionID,
		IsWindowUpdate:       true,
		GlobalSequenceNumber: c.globalSeq.NextOut.Value,
		ReceiveLimit:         c.receiveLimit(),
	}).ToBytes()

	if err != nil {
		return
	}

	if err := c.udpConnection.Write(update); err == nil {
		c.globalSeq.NextOut = c.globalSeq.NextOut.Next()
		c.lastWindowUpdate = time.Now()
	}
}

// sendAck sends an acknowledgement packet for the given sequence number to the connection's peer.
func (c *connection) sendAck(ackSn internal.SequenceNumber) {
	ack, err := (&internal.Packet{
		ConnectionID:         c.connectionID,
		IsAck:                true,
		AckSequenceNumber:    ackSn.Value,
		GlobalSequenceNumber: c.globalSeq.NextOut.Value,
		ReceiveLimit:         c.receiveLimit(),
	}).ToBytes()

	if err != nil {
		return
	}

	if err := c.udpConnection.Write(ack); err == nil {
		c.globalSeq.NextOut = c.globalSeq.NextOut.Next()
	}
}

// deliver adds the given packet to the correct incoming channel.
func (c *connection) deliver(p *internal.Packet) error {
	ch := c.unrelIncoming
	if p.IsReliable || p.IsAck || p.IsWindowUpdate {
		ch = c.relIncoming
	}

	select {
	case <-c.ctx.Done():
		return fmt.Errorf("%w: %v", internal.ErrClientConnectionClosed, context.Cause(c.ctx))
	case ch <- p:
		return nil
	default:
		return internal.ErrIncomingFull
	}
}

// receiveLimit calculates this connection's ReceiveLimit.
func (c *connection) receiveLimit() uint16 {
	return c.reliableSeq.NextIn.Value + uint16(c.relFree())
}

// relFree returns the free space on the relReceive channel.
func (c *connection) relFree() int {
	return cap(c.relReceive) - len(c.relReceive)
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
