package observer

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/observer/database"
	"github.com/ledgerwatch/erigon/cmd/observer/utils"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/semaphore"
	"time"
)

type Crawler struct {
	transport  DiscV4Transport
	db         database.DBRetrier
	config     CrawlerConfig
	forkFilter forkid.Filter
	log        log.Logger
}

type CrawlerConfig struct {
	Chain            string
	Bootnodes        []*enode.Node
	PrivateKey       *ecdsa.PrivateKey
	ConcurrencyLimit uint
	RefreshTimeout   time.Duration
}

func NewCrawler(
	transport DiscV4Transport,
	db database.DB,
	config CrawlerConfig,
	logger log.Logger,
) (*Crawler, error) {
	chain := config.Chain
	chainConfig := params.ChainConfigByChainName(chain)
	genesisHash := params.GenesisHashByChainName(chain)
	if (chainConfig == nil) || (genesisHash == nil) {
		return nil, fmt.Errorf("unknown chain %s", chain)
	}

	forkFilter := forkid.NewStaticFilter(chainConfig, *genesisHash)

	instance := Crawler{
		transport,
		database.NewDBRetrier(db, logger),
		config,
		forkFilter,
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
		close(nodes)
	}()
	return nodes
}

func (crawler *Crawler) selectCandidates(ctx context.Context, nodes chan<- *enode.Node) error {
	for _, node := range crawler.config.Bootnodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case nodes <- node:
		}
	}

	for ctx.Err() == nil {
		refreshTimeout := crawler.config.RefreshTimeout
		limit := crawler.config.ConcurrencyLimit
		candidates, err := crawler.db.TakeCandidates(ctx, refreshTimeout, limit)
		if err != nil {
			return err
		}

		if len(candidates) == 0 {
			utils.Sleep(ctx, 1*time.Second)
		}

		for id, addr := range candidates {
			node, err := makeNodeFromAddr(id, addr)
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case nodes <- node:
			}
		}
	}

	return ctx.Err()
}

func (crawler *Crawler) Run(ctx context.Context) error {
	nodes := crawler.startSelectCandidates(ctx)
	sem := semaphore.NewWeighted(int64(crawler.config.ConcurrencyLimit))

	for node := range nodes {
		if err := sem.Acquire(ctx, 1); err != nil {
			if !errors.Is(err, context.Canceled) {
				return fmt.Errorf("failed to acquire semaphore: %w", err)
			} else {
				break
			}
		}

		nodeDesc := node.URLv4()
		logger := crawler.log.New("node", nodeDesc)

		id, err := nodeID(node)
		if err != nil {
			return fmt.Errorf("failed to get node ID: %w", err)
		}

		interrogator, err := NewInterrogator(node, crawler.transport, crawler.forkFilter, crawler.config.PrivateKey, logger)
		if err != nil {
			return fmt.Errorf("failed to create Interrogator for node %s: %w", nodeDesc, err)
		}

		go func() {
			defer sem.Release(1)

			result, err := interrogator.Run(ctx)

			if (result != nil) && (result.IsCompatFork != nil) {
				dbErr := crawler.db.UpdateForkCompatibility(ctx, id, *result.IsCompatFork)
				if dbErr != nil {
					if !errors.Is(dbErr, context.Canceled) {
						logger.Error("Failed to update fork compatibility", "err", dbErr)
					}
					return
				}
			}

			if (result != nil) && (result.ClientID != nil) {
				dbErr := crawler.db.UpdateClientID(ctx, id, *result.ClientID)
				if dbErr != nil {
					if !errors.Is(dbErr, context.Canceled) {
						logger.Error("Failed to update client ID", "err", dbErr)
					}
					return
				}
			}

			if (result != nil) && (result.HandshakeErr != nil) {
				dbErr := crawler.db.UpdateHandshakeError(ctx, id, result.HandshakeErr.StringCode())
				if dbErr != nil {
					if !errors.Is(dbErr, context.Canceled) {
						logger.Error("Failed to update handshake error", "err", dbErr)
					}
					return
				}
			}

			if err != nil {
				if !errors.Is(err, context.Canceled) {
					logFunc := logger.Warn
					if (result != nil) && (result.IsCompatFork != nil) && !*result.IsCompatFork {
						logFunc = logger.Debug
					}
					logFunc("Failed to interrogate node", "err", err)
				}
				return
			}

			peers := result.Peers
			logger.Info(fmt.Sprintf("Got %d peers", len(peers)))

			for _, peer := range peers {
				peerID, err := nodeID(peer)
				if err != nil {
					logger.Error("Failed to get peer node ID", "err", err)
					continue
				}

				err = crawler.db.UpsertNodeAddr(ctx, peerID, makeNodeAddr(peer))
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						logger.Error("Failed to save node", "err", err)
					}
					break
				}
			}
		}()
	}
	return nil
}
