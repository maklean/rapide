package rapide

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/maklean/rapide/internal/config"
	"github.com/maklean/rapide/internal/writer"
)

// newUDPClient returns a datagram with the given peer as its destination address.
func newUDPClient(address string) (*net.UDPConn, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	return net.DialUDP("udp", nil, addr)
}

// Dial connects to the address at the given address
func Dial(address string) (*Connection, error) {
	socket, err := newUDPClient(address)
	if err != nil {
		return nil, err
	}

	log.Println("Sent handshake request to server...")
	connectionID, err := sendHandshakeRequest(socket)
	if err != nil {
		return nil, err
	}

	log.Printf("Got handshake response from server! ConnectionID = %d", connectionID)

	conn := newConnection(connectionID, socket, writer.CreateClientWriter(socket), func() { socket.Close() })

	go clientReadLoop(conn)

	return &Connection{conn: conn}, nil
}

// sendHandshakeRequest sends a handshake request to the peer/Listener, returns the connectionId if successful.
func sendHandshakeRequest(socket *net.UDPConn) (uint32, error) {
	handshakePacket, err := (&Packet{IsHandshakeClient: true}).ToBytes()
	if err != nil {
		return 0, fmt.Errorf("failed to serialize handshake packet to bytes: %v", err)
	}

	buf := make([]byte, config.MaxPayloadSize)
	deadline := time.Now().Add(time.Duration(config.HandshakeTimeout) * time.Second)

	for time.Now().Before(deadline) {
		// this I/O req. is technically shaving time off, but eh
		_, err = socket.Write(handshakePacket)
		if err != nil {
			return 0, fmt.Errorf("failed to write send handshake packet to peer: %v", err)
		}

		// wait some time before re-sending a request
		socket.SetReadDeadline(time.Now().Add(time.Duration(config.HandshakeRequestResend) * time.Millisecond))

		n, err := socket.Read(buf)
		if err != nil {
			var netErr net.Error

			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}

			return 0, fmt.Errorf("failed to write read handshake response from peer: %v", err)
		}

		packet, err := FromBytes(buf[:n])
		if err != nil || !packet.IsHandshakeServer {
			continue
		}

		socket.SetReadDeadline(time.Time{}) // clear
		return packet.ConnectionID, nil
	}

	return 0, errors.New("handshake request timed out")
}

// clientReadLoop is the client's read loop - it accepts datagrams (that should be sent by the server)
func clientReadLoop(c *connection) {
	buf := make([]byte, config.MaxPayloadSize)

	for {
		n, _, err := c.socket.ReadFromUDP(buf)
		if err != nil {
			// if the Client's socket is closed, exit.
			if errors.Is(err, net.ErrClosed) || c.isClosed() {
				return
			}

			log.Println("Failed to read data from incoming datagram:", err)
			continue
		}

		packet, err := FromBytes(buf[:n])
		if err != nil {
			log.Println("Failed to decode payload from incoming datagram:", err)
			continue
		}

		// ignore duplicate handshake responses
		if packet.IsHandshakeServer {
			continue
		}

		c.deliver(packet)
	}
}
