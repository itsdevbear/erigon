package observer

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
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
	log        log.Logger
}

func NewInterrogator(node *enode.Node, transport DiscV4Transport, forkFilter forkid.Filter, logger log.Logger) (*Interrogator, error) {
	instance := Interrogator{
		node,
		transport,
		forkFilter,
		logger,
	}
	return &instance, nil
}

func (interrogator *Interrogator) Run(ctx context.Context) ([]*enode.Node, error) {
	interrogator.log.Debug("Interrogating a node")

	err := interrogator.transport.Ping(interrogator.node)
	if err != nil {
		return nil, fmt.Errorf("ping-pong failed: %w", err)
	}

	enr, err := interrogator.transport.RequestENR(interrogator.node)
	if err != nil {
		return nil, fmt.Errorf("ENR request failed: %w", err)
	}

	interrogator.log.Debug("Got ENR", "enr", enr)

	forkID, err := eth.LoadENRForkID(interrogator.node.Record())
	if err != nil {
		return nil, err
	}

	// filter by fork ID
	if forkID != nil {
		err := interrogator.forkFilter(*forkID)
		if errors.Is(err, forkid.ErrLocalIncompatibleOrStale) {
			return nil, fmt.Errorf("incompatible ENR fork ID %w", err)
		}
	}

	keys := keygen(ctx, interrogator.node.Pubkey(), 10*time.Second, interrogator.log)
	interrogator.log.Debug(fmt.Sprintf("Generated %d keys", len(keys)))

	peersByID := make(map[enode.ID]*enode.Node)
	for _, key := range keys {
		neighbors, err := interrogator.transport.FindNode(interrogator.node, key)
		if err != nil {
			return nil, fmt.Errorf("FindNode request failed: %w", err)
		}

		for _, node := range neighbors {
			peersByID[node.ID()] = node
		}

		sleep(ctx, 1*time.Second)
	}

	peers := valuesOfIDToNodeMap(peersByID)

	return peers, nil
}

func valuesOfIDToNodeMap(m map[enode.ID]*enode.Node) []*enode.Node {
	values := make([]*enode.Node, 0, len(m))
	for _, value := range m {
		values = append(values, value)
	}
	return values
}

func sleep(parentContext context.Context, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(parentContext, timeout)
	defer cancel()
	<-ctx.Done()
}
