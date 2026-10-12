package config

// MaxPayloadSize is the maximum number of bytes an incoming datagram can be.
const MaxPayloadSize uint16 = 1472

/*
MaxIncomingPackets is the maximum number of packets a connection's incoming packets channel can hold.

TODO: if we ever implement a sliding window protocol, this should be the receive window (+ some safety margin)
*/
const MaxIncomingPackets uint16 = 256

// MaxOutgoingPackets is the maximum number of packets a connection's outgoing packets channel can hold.
const MaxOutgoingPackets uint16 = 256

// HandshakeTimeout is the number of seconds we wait for a server to response to a sent handshake packet.
const HandshakeTimeout int = 5

// HandshakeRequestResend is the number of milliseconds we wait before resending a handshake packet.
const HandshakeRequestResend int = 500
