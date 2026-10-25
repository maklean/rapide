package rapide

import (
	"context"
	"net"
	"sync"

	"github.com/maklean/rapide/internal"
)

type Connection interface {
	RemoteAddr() net.Addr
	LocalAddr() net.Addr
	Close() error
	IsClosed() bool
}

type connection struct {
	// identifier for this connection
	connectionID uint32

	// udp connection to peer
	udpConnection internal.UDPConnection

	ctx        context.Context
	cancel     context.CancelCauseFunc
	cancelOnce sync.Once
	onClose    func()
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
		connectionID:  connectionID,
		udpConnection: udpConn,
		ctx:           ctx,
		cancel:        cancel,
		onClose:       onClose,
	}

	go conn.run()

	return conn
}

func (c *connection) run() {
	defer c.close(context.Cause(c.ctx))

	for {
		select {
		case <-c.ctx.Done():
			return
		}
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
