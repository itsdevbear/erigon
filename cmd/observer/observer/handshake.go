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

func Handshake(ctx context.Context, ip net.IP, rlpxPort int, pubkey *ecdsa.PublicKey, myPrivateKey *ecdsa.PrivateKey) (*HelloMessage, error) {
	connectTimeout := 10 * time.Second
	dialer := net.Dialer{
		Timeout: connectTimeout,
	}
	addr := net.TCPAddr{IP: ip, Port: rlpxPort}

	tcpConn, err := dialer.DialContext(ctx, "tcp", addr.String())
	if err != nil {
		return nil, fmt.Errorf("handshake failed to connect: %w", err)
	}

	conn := rlpx.NewConn(tcpConn, pubkey)
	defer func() { _ = conn.Close() }()

	handshakeTimeout := 5 * time.Second
	err = conn.SetDeadline(time.Now().Add(handshakeTimeout))
	if err != nil {
		return nil, fmt.Errorf("handshake failed to set timeout: %w", err)
	}

	_, err = conn.Handshake(myPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("handshake RLPx auth failed: %w", err)
	}

	messageID, data, dataSize, err := conn.Read()
	if err != nil {
		return nil, fmt.Errorf("handshake RLPx read failed: %w", err)
	}
	reader := bytes.NewReader(data)

	switch messageID {
	default:
		return nil, fmt.Errorf("handshake got unexpected message ID: %d", messageID)
	case RLPxMessageIDDisconnect:
		var reason [1]p2p.DiscReason
		if err = rlp.Decode(reader, &reason); err != nil {
			return nil, fmt.Errorf("handshake failed to parse disconnect reason: %w", err)
		}
		return nil, fmt.Errorf("handshake got disconnected: %s", reason[0])
	case RLPxMessageIDHello:
		var message HelloMessage
		if err = rlp.NewStream(reader, uint64(dataSize)).Decode(&message); err != nil {
			return nil, fmt.Errorf("handshake failed to parse Hello message: %w", err)
		}
		return &message, nil
	}
}
