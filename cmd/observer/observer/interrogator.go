package observer

import (
	"context"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/log/v3"
)

type Interrogator struct {
	node      *enode.Node
	transport DiscV4Transport
	log       log.Logger
}

func NewInterrogator(node *enode.Node, transport DiscV4Transport, logger log.Logger) (*Interrogator, error) {
	instance := Interrogator{
		node,
		transport,
		logger,
	}
	return &instance, nil
}

func (interrogator *Interrogator) Run(_ context.Context) error {
	interrogator.log.Debug("Interrogating a node")
	return nil
}
