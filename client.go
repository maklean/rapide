package rapide

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"

	"github.com/maklean/rapide/internal"
)

// newClient returns a client connected to the given remote address.
func newClient(ctx context.Context, remoteAddr string) (Connection, error) {
	addr, err := net.ResolveUDPAddr("udp", remoteAddr)
	if err != nil {
		return nil, err
	}

	socket, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}

	connId, err := clientSendHandshake(ctx, socket)
	if err != nil {
		socket.Close()
		return nil, err
	}

	conn := newConnection(connId, internal.NewUDPConnection(socket, internal.NewClientWriter(socket)))

	go conn.udpConnection.Read(clientHandlePacket)

	return conn, nil
}

// clientHandlePacket handles an incoming packet sent by the listener.
func clientHandlePacket(_ *net.UDPAddr, p *internal.Packet) error {
	return nil
}

// clientSendHandshake sends a handshake request to the listener. Returns the connection ID if connected.
func clientSendHandshake(ctx context.Context, socket *net.UDPConn) (uint32, error) {
	handshake, err := (&internal.Packet{IsHandshakeClient: true}).ToBytes()
	if err != nil {
		return 0, err
	}

	defer socket.SetReadDeadline(time.Time{})

	buf := make([]byte, internal.MaxDatagramSize)

	for ctx.Err() == nil {
		if _, err := socket.Write(handshake); err != nil {
			return 0, err
		}

		attempt := time.Now().Add(internal.HandshakeReadInterval * time.Millisecond)
		if d, ok := ctx.Deadline(); ok && d.Before(attempt) {
			// clamp to deadline if the attempt is after the deadline
			attempt = d
		}

		socket.SetReadDeadline(attempt)

		n, err := socket.Read(buf)
		if err != nil {
			var nerr net.Error

			switch {
			case errors.Is(err, syscall.ECONNREFUSED):
				// listener's socket isn't up yet, retry
				select {
				case <-time.After(time.Until(attempt)):
				case <-ctx.Done():
				}

				continue
			case errors.As(err, &nerr) && nerr.Timeout():
				// no reply, re-send
				continue
			default:
				return 0, err
			}
		}

		p, err := internal.FromBytes(buf[:n])
		if err != nil || !p.IsHandshakeServer {
			continue
		}

		return p.ConnectionID, nil
	}

	return 0, fmt.Errorf("%w: %w", internal.HandshakeTimeout, ctx.Err())
}
