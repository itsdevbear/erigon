package observer

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/log/v3"
)

type DiscV4Transport interface {
	RequestENR(*enode.Node) (*enode.Node, error)
	Ping(*enode.Node) error
	LookupPubkey(key *ecdsa.PublicKey) []*enode.Node
}

type Crawler struct {
	transport DiscV4Transport
	bootnodes []*enode.Node
	log       log.Logger
}

func NewCrawler(transport DiscV4Transport, bootnodes []*enode.Node, logger log.Logger) (*Crawler, error) {
	instance := Crawler{
		transport,
		bootnodes,
		logger,
	}
	return &instance, nil
}

func (crawler *Crawler) startSelectCandidates(ctx context.Context) <-chan *enode.Node {
	nodes := make(chan *enode.Node)
	go func() {
		err := crawler.selectCandidates(ctx, nodes)
		if (err != nil) && !errors.Is(err, context.Canceled) {
			crawler.log.Error("Failed to select candidates", "err", err)
		}
	}()
	return nodes
}

func (crawler *Crawler) selectCandidates(ctx context.Context, nodes chan<- *enode.Node) error {
	for _, node := range crawler.bootnodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case nodes <- node:
		}
	}
	return nil
}

func (crawler *Crawler) Run(ctx context.Context) error {
	nodes := crawler.startSelectCandidates(ctx)

	for node := range nodes {
		nodeDesc := node.String()
		logger := crawler.log.New("node", nodeDesc)

		interrogator, err := NewInterrogator(node, crawler.transport, logger)
		if err != nil {
			return fmt.Errorf("failed to create Interrogator for node %s: %w", nodeDesc, err)
		}

		go func() {
			peers, err := interrogator.Run(ctx)
			if err != nil {
				logger.Warn("Failed to interrogate node", "err", err)
				return
			}

			// TODO save to DB
			logger.Debug(fmt.Sprintf("Got %d peers", len(peers)))
		}()
	}
	return nil
}
