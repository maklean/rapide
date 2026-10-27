package internal

// MaxDatagramSize is the maximum size (in bytes) an incoming datagram can be.
const MaxDatagramSize = 1472

// MaxPayloadSize is the maximum size a datagram's payload can carry (Maximum Segment Size).
const MaxPayloadSize = MaxDatagramSize - PacketHeaderSize

// HandshakeReadInterval is the amount of time (in milliseconds) we wait before reading a handshake packet.
const HandshakeReadInterval = 500

// ListenerClientBacklog is the capacity of the Listener's acceptedPeers channel/queue.
const ListenerClientBacklog = 128

// ConnectionIncomingBacklog is the capacity of the connection's incoming channels/queues
const ConnectionIncomingBacklog = 256

// ConnectionOutgoingBacklog is the capacity of the connection's backlog channels/queues
const ConnectionOutgoingBacklog = 256

// ConnectionReceiveQueueSize is the capacity of the connection's reliable & unreliable receive queues.
const ConnectionReceiveQueueSize = 128

// ConnectionTickInterval is the amount of time (in milliseconds) between each connection tick.
// NOTE: 100ms is a recommended clock granularity for the retransmission timer (see RFC 6298 section 4)
const ConnectionTickInterval = 100

// MaxRetransmissionRetries is the maximum number of times we retransmit a packet before assuming the connection is dead
const MaxRetransmissionRetries = 10

// MaxRetransmissionTimeout is the maximum retransmission timeout (in milliseconds)
const MaxRetransmissionTimeout = 60000

// ReceiveLimitUpdateInterval is the interval (in milliseconds) between receive limit window updates.
const ReceiveLimitUpdateInterval = 100
