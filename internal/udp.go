package internal

import (
	"errors"
	"log"
	"net"
)

type UDPConnection struct {
	socket     *net.UDPConn
	writer     PacketWriter
	remoteAddr net.Addr
}

// NewUDPConnection creates a new UDPConnection struct instance.
func NewUDPConnection(socket *net.UDPConn, writer PacketWriter, remoteAddr net.Addr) UDPConnection {
	return UDPConnection{socket, writer, remoteAddr}
}

// Read starts a loop that receives datagrams and sends the decoded packet into the given helper.
func (uc *UDPConnection) Read(f func(*net.UDPAddr, *Packet) error) {
	buf := make([]byte, MaxDatagramSize)

	for {
		n, addr, err := uc.socket.ReadFromUDP(buf)

		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}

			continue
		}

		p, err := FromBytes(buf[:n])
		if err != nil {
			continue
		}

		if err := f(addr, p); err != nil {
			log.Println(err)
			continue
		}
	}
}

// Write writes to the connected peer.
// NOTE: This shouldn't be called on a Listener's UDPConnection.
func (uc *UDPConnection) Write(data []byte) error {
	if uc.writer == nil {
		return ErrNoAvailableWriter
	}

	return uc.writer.Write(data)
}

// Close closes the UDPConnection's socket.
func (uc *UDPConnection) Close() error {
	return uc.socket.Close()
}

// Socket returns the socket of this UDPConnection.
func (uc *UDPConnection) Socket() *net.UDPConn {
	return uc.socket
}

// LocalAddr returns the local address of this UDP connection.
// NOTE: This shouldn't be called on a Listener's UDPConnection.
func (uc *UDPConnection) LocalAddr() net.Addr {
	return uc.socket.LocalAddr()
}

// RemoteAddr returns the peer's address.
// NOTE: This shouldn't be called on a Listener's UDPConnection.
func (uc *UDPConnection) RemoteAddr() net.Addr {
	return uc.remoteAddr
}
