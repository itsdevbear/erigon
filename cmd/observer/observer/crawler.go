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
	"sync/atomic"
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

	KeygenTimeout     time.Duration
	KeygenConcurrency uint
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

type candidateNode struct {
	id database.NodeID
	node *enode.Node
}

func (crawler *Crawler) startSelectCandidates(ctx context.Context) <-chan candidateNode {
	nodes := make(chan candidateNode)
	go func() {
		err := crawler.selectCandidates(ctx, nodes)
		if (err != nil) && !errors.Is(err, context.Canceled) {
			crawler.log.Error("Failed to select candidates", "err", err)
		}
		close(nodes)
	}()
	return nodes
}

func (crawler *Crawler) selectCandidates(ctx context.Context, nodes chan<- candidateNode) error {
	for _, node := range crawler.config.Bootnodes {
		id, err := nodeID(node)
		if err != nil {
			return fmt.Errorf("failed to get a bootnode ID: %w", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case nodes <- candidateNode{id, node}:
		}
	}

	for ctx.Err() == nil {
		refreshTimeout := crawler.config.RefreshTimeout
		maxHandshakeTries := uint(10)
		limit := crawler.config.ConcurrencyLimit
		candidates, err := crawler.db.TakeCandidates(ctx, refreshTimeout, maxHandshakeTries, limit)
		if err != nil {
			return err
		}

		if len(candidates) == 0 {
			utils.Sleep(ctx, 1*time.Second)
		}

		for _, id := range candidates {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case nodes <- candidateNode{id, nil}:
			}
		}
	}

	return ctx.Err()
}

func (crawler *Crawler) Run(ctx context.Context) error {
	nodes := crawler.startSelectCandidates(ctx)
	sem := semaphore.NewWeighted(int64(crawler.config.ConcurrencyLimit))
	// allow only 1 keygen at a time
	keygenSem := semaphore.NewWeighted(int64(1))

	crawledCount := 0
	crawledCountLogDate := time.Now()
	foundPeersCountPtr := new(uint64)

	for candidate := range nodes {
		if err := sem.Acquire(ctx, 1); err != nil {
			if !errors.Is(err, context.Canceled) {
				return fmt.Errorf("failed to acquire semaphore: %w", err)
			} else {
				break
			}
		}

		crawledCount++
		if time.Since(crawledCountLogDate) > 10*time.Second {
			foundPeersCount := atomic.LoadUint64(foundPeersCountPtr)
			crawler.log.Info("Crawling", "crawledCount", crawledCount, "foundPeersCount", foundPeersCount)
			crawledCountLogDate = time.Now()
		}

		id := candidate.id
		node := candidate.node

		if node == nil {
			nodeAddr, err := crawler.db.FindNodeAddr(ctx, id)
			if err != nil {
				return fmt.Errorf("failed to get the node address: %w", err)
			}

			node, err = makeNodeFromAddr(id, *nodeAddr)
			if err != nil {
				return fmt.Errorf("failed to make node from node address: %w", err)
			}
		}

		nodeDesc := node.URLv4()
		logger := crawler.log.New("node", nodeDesc)

		handshakeLastTry, err := crawler.db.FindHandshakeLastTry(ctx, id)
		if err != nil {
			return fmt.Errorf("failed to get handshake last try: %w", err)
		}

		// the client ID doesn't need to be refreshed often
		handshakeRefreshTimeout := 7 * 24 * time.Hour

		keygenCachedHexKeys, err := crawler.db.FindNeighborBucketKeys(ctx, id)
		if err != nil {
			return fmt.Errorf("failed to get neighbor bucket keys: %w", err)
		}
		keygenCachedKeys, err := parseHexPublicKeys(keygenCachedHexKeys)
		if err != nil {
			return fmt.Errorf("failed to parse cached neighbor bucket keys: %w", err)
		}

		interrogator, err := NewInterrogator(
			node,
			crawler.transport,
			crawler.forkFilter,
			crawler.config.PrivateKey,
			handshakeLastTry,
			handshakeRefreshTimeout,
			crawler.config.KeygenTimeout,
			crawler.config.KeygenConcurrency,
			keygenSem,
			keygenCachedKeys,
			logger)
		if err != nil {
			return fmt.Errorf("failed to create Interrogator for node %s: %w", nodeDesc, err)
		}

		go func() {
			defer sem.Release(1)

			result, err := interrogator.Run(ctx)

			var isCompatFork *bool
			if result != nil {
				isCompatFork = result.IsCompatFork
			} else if (err != nil) &&
				((err.id == InterrogationErrorIncompatibleForkID) ||
					(err.id == InterrogationErrorBlacklistedClientID)) {
				isCompatFork = new(bool)
				*isCompatFork = false
			}

			if isCompatFork != nil {
				dbErr := crawler.db.UpdateForkCompatibility(ctx, id, *isCompatFork)
				if dbErr != nil {
					if !errors.Is(dbErr, context.Canceled) {
						logger.Error("Failed to update fork compatibility", "err", dbErr)
					}
					return
				}
			}

			var clientID *string
			if result != nil {
				clientID = result.ClientID
			} else if (err != nil) && (err.id == InterrogationErrorBlacklistedClientID) {
				clientID = new(string)
				*clientID = err.wrappedErr.Error()
			}

			if clientID != nil {
				dbErr := crawler.db.UpdateClientID(ctx, id, *clientID)
				if dbErr != nil {
					if !errors.Is(dbErr, context.Canceled) {
						logger.Error("Failed to update client ID", "err", dbErr)
					}
					return
				}
			}

			if err != nil {
				if !errors.Is(err, context.Canceled) {
					var logFunc func(msg string, ctx ...interface{})
					switch err.id {
					case InterrogationErrorPing:
						fallthrough
					case InterrogationErrorIncompatibleForkID:
						fallthrough
					case InterrogationErrorBlacklistedClientID:
						fallthrough
					case InterrogationErrorFindNodeTimeout:
						logFunc = logger.Debug
					default:
						logFunc = logger.Warn
					}
					logFunc("Failed to interrogate node", "err", err)
				}
				return
			}

			if result.HandshakeErr != nil {
				dbErr := crawler.db.UpdateHandshakeError(ctx, id, result.HandshakeErr.StringCode())
				if dbErr != nil {
					if !errors.Is(dbErr, context.Canceled) {
						logger.Error("Failed to update handshake error", "err", dbErr)
					}
					return
				}
			}

			if len(result.KeygenKeys) >= 15 {
				keygenHexKeys := hexEncodePublicKeys(result.KeygenKeys)
				dbErr := crawler.db.UpdateNeighborBucketKeys(ctx, id, keygenHexKeys)
				if dbErr != nil {
					if !errors.Is(dbErr, context.Canceled) {
						logger.Error("Failed to update neighbor bucket keys", "err", dbErr)
					}
					return
				}
			}

			peers := result.Peers
			logger.Debug(fmt.Sprintf("Got %d peers", len(peers)))
			atomic.AddUint64(foundPeersCountPtr, uint64(len(peers)))

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
