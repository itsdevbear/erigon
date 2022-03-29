package observer

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"fmt"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/rlpx"
	"github.com/ledgerwatch/erigon/rlp"
	"net"
	"time"
)

// https://github.com/ethereum/devp2p/blob/master/rlpx.md#p2p-capability
const (
	RLPxMessageIDHello      = 0
	RLPxMessageIDDisconnect = 1
)

// HelloMessage is the RLPx Hello message.
// (same as protoHandshake in p2p/peer.go)
// https://github.com/ethereum/devp2p/blob/master/rlpx.md#hello-0x00
type HelloMessage struct {
	Version    uint64
	ClientID   string
	Caps       []p2p.Cap
	ListenPort uint64
	Pubkey     []byte // secp256k1 public key

	// Ignore additional fields (for forward compatibility).
	Rest []rlp.RawValue `rlp:"tail"`
}

type HandshakeErrorID string

const (
	HandshakeErrorIDConnect    HandshakeErrorID = "connect"
	HandshakeErrorIDSetTimeout HandshakeErrorID = "set-timeout"
	HandshakeErrorIDAuth       HandshakeErrorID = "auth"
	HandshakeErrorIDRead       HandshakeErrorID = "read"
	HandshakeErrorIDUnexpectedMessage HandshakeErrorID = "unexpected-message"
	HandshakeErrorIDDisconnectDecode HandshakeErrorID = "disconnect-decode"
	HandshakeErrorIDDisconnect HandshakeErrorID = "disconnect"
	HandshakeErrorIDHelloDecode HandshakeErrorID = "hello-decode"
)

type HandshakeError struct {
	id         HandshakeErrorID
	wrappedErr error
	param      uint64
}

func NewHandshakeError(id HandshakeErrorID, wrappedErr error, param uint64) *HandshakeError {
	instance := HandshakeError{
		id,
		wrappedErr,
		param,
	}
	return &instance
}

func (e *HandshakeError) Unwrap() error {
	return e.wrappedErr
}

func (e *HandshakeError) Error() string {
	switch e.id {
	case HandshakeErrorIDConnect:
		return fmt.Sprintf("handshake failed to connect: %v", e.wrappedErr)
	case HandshakeErrorIDSetTimeout:
		return fmt.Sprintf("handshake failed to set timeout: %v", e.wrappedErr)
	case HandshakeErrorIDAuth:
		return fmt.Sprintf("handshake RLPx auth failed: %v", e.wrappedErr)
	case HandshakeErrorIDRead:
		return fmt.Sprintf("handshake RLPx read failed: %v", e.wrappedErr)
	case HandshakeErrorIDUnexpectedMessage:
		return fmt.Sprintf("handshake got unexpected message ID: %d", e.param)
	case HandshakeErrorIDDisconnectDecode:
		return fmt.Sprintf("handshake failed to parse disconnect reason: %v", e.wrappedErr)
	case HandshakeErrorIDDisconnect:
		return fmt.Sprintf("handshake got disconnected: %v", e.wrappedErr)
	case HandshakeErrorIDHelloDecode:
		return fmt.Sprintf("handshake failed to parse Hello message: %v", e.wrappedErr)
	default:
		return "<unhandled HandshakeErrorID>"
	}
}

func (e *HandshakeError) StringCode() string {
	switch e.id {
	case HandshakeErrorIDUnexpectedMessage:
		fallthrough
	case HandshakeErrorIDDisconnect:
		return fmt.Sprintf("%s-%d", e.id, e.param)
	default:
		return string(e.id)
	}
}

func Handshake(ctx context.Context, ip net.IP, rlpxPort int, pubkey *ecdsa.PublicKey, myPrivateKey *ecdsa.PrivateKey) (*HelloMessage, *HandshakeError) {
	connectTimeout := 10 * time.Second
	dialer := net.Dialer{
		Timeout: connectTimeout,
	}
	addr := net.TCPAddr{IP: ip, Port: rlpxPort}

	tcpConn, err := dialer.DialContext(ctx, "tcp", addr.String())
	if err != nil {
		return nil, NewHandshakeError(HandshakeErrorIDConnect, err, 0)
	}

	conn := rlpx.NewConn(tcpConn, pubkey)
	defer func() { _ = conn.Close() }()

	handshakeTimeout := 5 * time.Second
	err = conn.SetDeadline(time.Now().Add(handshakeTimeout))
	if err != nil {
		return nil, NewHandshakeError(HandshakeErrorIDSetTimeout, err, 0)
	}

	_, err = conn.Handshake(myPrivateKey)
	if err != nil {
		return nil, NewHandshakeError(HandshakeErrorIDAuth, err, 0)
	}

	messageID, data, dataSize, err := conn.Read()
	if err != nil {
		return nil, NewHandshakeError(HandshakeErrorIDRead, err, 0)
	}
	reader := bytes.NewReader(data)

	switch messageID {
	default:
		return nil, NewHandshakeError(HandshakeErrorIDRead, nil, messageID)
	case RLPxMessageIDDisconnect:
		var reason [1]p2p.DiscReason
		if err = rlp.Decode(reader, &reason); err != nil {
			return nil, NewHandshakeError(HandshakeErrorIDDisconnectDecode, err, 0)
		}
		return nil, NewHandshakeError(HandshakeErrorIDDisconnect, reason[0], uint64(reason[0]))
	case RLPxMessageIDHello:
		var message HelloMessage
		if err = rlp.NewStream(reader, uint64(dataSize)).Decode(&message); err != nil {
			return nil, NewHandshakeError(HandshakeErrorIDHelloDecode, err, 0)
		}
		return &message, nil
	}
}
