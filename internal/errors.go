package internal

import "errors"

var ErrListenerBacklogFull error = errors.New("listener's backlog is full, cannot accept new client")
var ErrNoAvailableWriter error = errors.New("no available writer for udp connection")
var ErrInvalidConnectionID error = errors.New("invalid connection ID")
var ErrHandshakeTimeout error = errors.New("handshake request timed out")
var ErrCloseRequest error = errors.New("connection close request sent")
var ErrListenerConnectionClosed error = errors.New("listener connection closed")
var ErrClientConnectionClosed error = errors.New("client connection closed")
var ErrPayloadSizeMismatch error = errors.New("payload size mismatch")
var ErrIncomingFull error = errors.New("incoming queue full, cannot accept new packet")
var ErrOutgoingFull error = errors.New("outgoing queue full, cannot accept new packet")
var ErrPayloadTooLarge error = errors.New("payload is too large")
var ErrMaxRetransmissionsExceeded error = errors.New("max retransmissions exceeded")
var ErrReliableSendFailed error = errors.New("reliable send failed")
