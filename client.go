package rapide

import (
	"errors"
	"log"
	"net"
	"time"

	"github.com/maklean/rapide/internal/config"
	"github.com/maklean/rapide/internal/writer"
)

// newClient creates a new client that connects to the given address
func newClient(address string) (*connection, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	socket, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}

	conn := newConnection(socket, writer.CreateClientWriter(socket, addr))
	return conn, nil
}

// Dial connects to the address at the given address
func Dial(address string) (*connection, error) {
	conn, err := newClient(address)
	if err != nil {
		return nil, err
	}

	go clientReadLoop(conn)

	// perform handshake with server
	handshakePacket := Packet{ConnectionID: 0, IsHandshakeClient: true}
	payload, err := handshakePacket.ToBytes()
	if err != nil {
		return nil, err
	}

	err = conn.writer.Write(payload)
	if err != nil {
		return nil, err
	}

	select {
	case <-conn.established:
		return conn, nil
	case <-time.After(time.Duration(config.HandshakeTimeout) * time.Second):
		// TODO: close socket
		return nil, errors.New("handshake timed out")
	}
}

// clientReadLoop is the client's read loop - it accepts datagrams (that should be sent by the server)
func clientReadLoop(c *connection) {
	buf := make([]byte, config.MaxPayloadSize)

	for {
		n, _, err := c.socket.ReadFromUDP(buf)
		if err != nil {
			log.Println("Failed to read data from incoming datagram:", err)
			continue
		}

		packet, err := FromBytes(buf[:n])
		if err != nil {
			log.Println("Failed to decode payload from incoming datagram:", err)
			continue
		}

		c.deliver(packet)
	}
}
