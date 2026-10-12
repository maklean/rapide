package writer

import "net"

// packetWriter is an interface for sending/writing data to packets.
type PacketWriter interface {
	Write(data []byte) error
}

// listenerWriter is an implementation of packetWriter for socket's from a Listener.
type listenerWriter struct {
	socket   *net.UDPConn
	peerAddr *net.UDPAddr
}

// clientWriter is an implementation of packetWriter for socket's from a client.
type clientWriter struct {
	socket *net.UDPConn
}

// createListenerWriter creates a new listenerWriter.
func CreateListenerWriter(socket *net.UDPConn, peerAddr *net.UDPAddr) *listenerWriter {
	return &listenerWriter{socket, peerAddr}
}

// createClientWriter creates a new clientWriter.
func CreateClientWriter(socket *net.UDPConn) *clientWriter {
	return &clientWriter{socket}
}

func (lw *listenerWriter) Write(data []byte) error {
	// lw.Socket should be the shared socket between the Listener and its peers (hence, why we need to specify a target address)
	_, err := lw.socket.WriteToUDP(data, lw.peerAddr)
	return err
}

func (cw *clientWriter) Write(data []byte) error {
	// cw.Socket should be a uniquely owned socket connected to a server
	_, err := cw.socket.Write(data)
	return err
}
