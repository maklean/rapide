package rapide

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"net"
	"sync"

	"github.com/maklean/rapide/internal"
)

type Listener struct {
	// wrapper for the listener's socket
	conn internal.UDPConnection

	// map of connectionID => peer
	peers map[uint32]*connection

	peersMu sync.Mutex

	// peers that have established a connection (handshake)
	acceptedPeers chan Connection

	ctx        context.Context
	cancel     context.CancelCauseFunc
	cancelOnce sync.Once
}

// Accept returns a newly connected peer. If a peer is not available immediately, it blocks until one is available.
func (l *Listener) Accept(ctx context.Context) (Connection, error) {
	if l.IsClosed() {
		return nil, internal.ErrListenerConnectionClosed
	}

	for {
		select {
		case peer := <-l.acceptedPeers:
			if peer.IsClosed() {
				continue
			}

			return peer, nil
		case <-l.ctx.Done():
			return nil, internal.ErrListenerConnectionClosed
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
}

// Close closes the listener.
func (l *Listener) Close() error {
	if l.IsClosed() {
		return internal.ErrListenerConnectionClosed
	}

	l.close(internal.ErrCloseRequest)
	return nil
}

// Addr returns the local network address the Listener is bound to.
func (l *Listener) Addr() net.Addr {
	return l.conn.LocalAddr()
}

// close closes the listener with the given error/reason.
func (l *Listener) close(err error) {
	l.cancelOnce.Do(func() {
		l.cancel(err)

		l.peersMu.Lock()
		peers := maps.Clone(l.peers)
		l.peersMu.Unlock()

		for _, peer := range peers {
			peer.Close()
		}

		l.conn.Close()
	})
}

// newListener creates a new listener listening at the given local address.
func newListener(localAddr string) (*Listener, error) {
	addr, err := net.ResolveUDPAddr("udp", localAddr)
	if err != nil {
		return nil, err
	}

	socket, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancelCause(context.Background())

	listener := &Listener{
		conn:          internal.NewUDPConnection(socket, nil, nil),
		peers:         make(map[uint32]*connection),
		acceptedPeers: make(chan Connection, internal.ListenerClientBacklog),
		ctx:           ctx,
		cancel:        cancel,
	}

	go listener.conn.Read(func(addr *net.UDPAddr, p *internal.Packet) error {
		select {
		case <-ctx.Done():
			return internal.ErrListenerConnectionClosed
		default:
			return listener.handlePacket(addr, p)
		}
	})

	return listener, nil
}

// handlePacket handles an incoming packet sent by a peer.
func (l *Listener) handlePacket(addr *net.UDPAddr, p *internal.Packet) error {
	if p.IsHandshakeClient {
		return l.handleHandshake(addr)
	}

	l.peersMu.Lock()
	_, exists := l.peers[p.ConnectionID]
	l.peersMu.Unlock()

	if !exists {
		return fmt.Errorf("%w: %d", internal.ErrInvalidConnectionID, p.ConnectionID)
	}

	return nil
}

// handleHandshake handles an incoming handshake request by a client at the given remote address.
func (l *Listener) handleHandshake(addr *net.UDPAddr) error {
	if len(l.acceptedPeers) == cap(l.acceptedPeers) {
		return internal.ErrListenerBacklogFull
	}

	// genConnID generates an unused connection ID.
	genConnID := func() uint32 {
		l.peersMu.Lock()
		defer l.peersMu.Unlock()

		connId := rand.Uint32()

		_, exists := l.peers[connId]

		for exists {
			connId = rand.Uint32()
			_, exists = l.peers[connId]
		}

		return connId
	}

	socket := l.conn.Socket()
	connId := genConnID()

	removePeer := func() {
		l.peersMu.Lock()
		delete(l.peers, connId)
		l.peersMu.Unlock()
	}

	conn := newConnection(l.ctx, connId, internal.NewUDPConnection(socket, internal.NewListenerWriter(socket, addr), addr), removePeer)

	l.peersMu.Lock()
	l.peers[connId] = conn
	l.peersMu.Unlock()

	// send handshake packet back to client
	handshake, err := (&internal.Packet{ConnectionID: connId, IsHandshakeServer: true}).ToBytes()
	if err != nil {
		conn.Close()
		return err
	}

	if err := conn.udpConnection.Write(handshake); err != nil {
		conn.Close()
		return err
	}

	select {
	case l.acceptedPeers <- conn:
		return nil
	case <-l.ctx.Done():
		return internal.ErrListenerConnectionClosed
	default:
		conn.Close()
		return internal.ErrListenerBacklogFull
	}
}

// IsClosed returns whether the Listener is closed.
func (l *Listener) IsClosed() bool {
	return l.ctx.Err() != nil
}
