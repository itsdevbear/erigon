package observer

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/observer/database"
	"github.com/ledgerwatch/erigon/cmd/observer/utils"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/log/v3"
	"time"
)

type DiscV4Transport interface {
	RequestENR(*enode.Node) (*enode.Node, error)
	Ping(*enode.Node) error
	FindNode(toNode *enode.Node, targetKey *ecdsa.PublicKey) ([]*enode.Node, error)
}

type Interrogator struct {
	node       *enode.Node
	transport  DiscV4Transport
	forkFilter forkid.Filter

	privateKey              *ecdsa.PrivateKey
	handshakeLastTry        *database.HandshakeTry
	handshakeRefreshTimeout time.Duration

	log log.Logger
}

type InterrogationResult struct {
	Node         *enode.Node
	IsCompatFork *bool
	Peers        []*enode.Node
	ClientID     *string
	HandshakeErr *HandshakeError
}

func NewInterrogator(
	node *enode.Node,
	transport DiscV4Transport,
	forkFilter forkid.Filter,
	privateKey *ecdsa.PrivateKey,
	handshakeLastTry *database.HandshakeTry,
	handshakeRefreshTimeout time.Duration,
	logger log.Logger,
) (*Interrogator, error) {
	instance := Interrogator{
		node,
		transport,
		forkFilter,
		privateKey,
		handshakeLastTry,
		handshakeRefreshTimeout,
		logger,
	}
	return &instance, nil
}

func (interrogator *Interrogator) Run(ctx context.Context) (*InterrogationResult, error) {
	interrogator.log.Info("Interrogating a node")

	err := interrogator.transport.Ping(interrogator.node)
	if err != nil {
		return nil, fmt.Errorf("ping-pong failed: %w", err)
	}

	// request ENR
	var forkID *forkid.ID
	enr, err := interrogator.transport.RequestENR(interrogator.node)
	if err != nil {
		interrogator.log.Debug("ENR request failed", "err", err)
	} else {
		interrogator.log.Debug("Got ENR", "enr", enr)
		forkID, err = eth.LoadENRForkID(enr.Record())
		if err != nil {
			return nil, err
		}
	}

	// filter by fork ID
	var isCompatFork *bool
	if forkID != nil {
		err := interrogator.forkFilter(*forkID)
		isCompatFork = new(bool)
		*isCompatFork = (err == nil) || !errors.Is(err, forkid.ErrLocalIncompatibleOrStale)
		if !*isCompatFork {
			result := InterrogationResult{interrogator.node, isCompatFork, nil, nil, nil}
			return &result, fmt.Errorf("incompatible ENR fork ID %w", err)
		}
	}

	// request client ID
	var clientID *string
	var handshakeErr *HandshakeError
	if interrogator.canRetryHandshake() {
		clientID, handshakeErr = interrogator.tryRequestClientID(ctx)
	}
	if (clientID != nil) && IsClientIDBlacklisted(*clientID) {
		isCompatFork := false
		result := InterrogationResult{interrogator.node, &isCompatFork, nil, clientID, nil}
		return &result, fmt.Errorf("incompatible client ID %s", *clientID)
	}

	keys := keygen(ctx, interrogator.node.Pubkey(), 10*time.Second, interrogator.log)
	interrogator.log.Trace(fmt.Sprintf("Generated %d keys", len(keys)))

	peersByID := make(map[enode.ID]*enode.Node)
	for _, key := range keys {
		neighbors, err := interrogator.transport.FindNode(interrogator.node, key)
		if err != nil {
			return nil, fmt.Errorf("FindNode request failed: %w", err)
		}

		for _, node := range neighbors {
			peersByID[node.ID()] = node
		}

		utils.Sleep(ctx, 1*time.Second)
	}

	peers := valuesOfIDToNodeMap(peersByID)

	result := InterrogationResult{interrogator.node, isCompatFork, peers, clientID, handshakeErr}
	return &result, nil
}

func (interrogator *Interrogator) handshake(ctx context.Context) (*HelloMessage, *HandshakeError) {
	node := interrogator.node
	return Handshake(ctx, node.IP(), node.TCP(), node.Pubkey(), interrogator.privateKey)
}

func (interrogator *Interrogator) tryRequestClientID(ctx context.Context) (*string, *HandshakeError) {
	hello, handshakeErr := interrogator.handshake(ctx)
	if (handshakeErr != nil) && !errors.Is(handshakeErr, context.Canceled) {
		interrogator.log.Debug("Failed to handshake", "err", handshakeErr)
		return nil, handshakeErr
	}

	clientID := &hello.ClientID
	interrogator.log.Debug("Got client ID", "clientID", *clientID)
	return clientID, nil
}

func (interrogator *Interrogator) canRetryHandshake() bool {
	if (interrogator.handshakeLastTry != nil) && !interrogator.handshakeLastTry.HasFailed {
		return time.Since(interrogator.handshakeLastTry.Time) > interrogator.handshakeRefreshTimeout
	}
	return true
}

func valuesOfIDToNodeMap(m map[enode.ID]*enode.Node) []*enode.Node {
	values := make([]*enode.Node, 0, len(m))
	for _, value := range m {
		values = append(values, value)
	}
	return values
}

