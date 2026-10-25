package internal

import "net"

type PacketWriter interface {
	// Write sends the given data to its peer.
	Write(data []byte) error
}

// listenerWriter writes to a peer through a socket shared with a Listener.
type listenerWriter struct {
	socket   *net.UDPConn
	peerAddr *net.UDPAddr
}

// clientWriter writes to a listener through a socket owned by a client.
type clientWriter struct {
	socket *net.UDPConn
}

// NewListenerWriter returns a PacketWriter that sends to the given peer.
func NewListenerWriter(socket *net.UDPConn, peerAddr *net.UDPAddr) PacketWriter {
	return &listenerWriter{socket, peerAddr}
}

// NewClientWriter returns a PacketWriter that sends to its connected listener.
func NewClientWriter(socket *net.UDPConn) PacketWriter {
	return &clientWriter{socket}
}

func (lw *listenerWriter) Write(data []byte) error {
	_, err := lw.socket.WriteToUDP(data, lw.peerAddr)
	return err
}

func (cw *clientWriter) Write(data []byte) error {
	_, err := cw.socket.Write(data)
	return err
}
