package internal

// MaxDatagramSize is the maximum size (in bytes) an incoming datagram can be.
const MaxDatagramSize = 1472

// HandshakeReadInterval is the amount of time (in milliseconds) we wait before reading a handshake packet.
const HandshakeReadInterval = 500

// ListenerClientBacklog is the capacity of the Listener's acceptedPeers backlog.
const ListenerClientBacklog = 128
