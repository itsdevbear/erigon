package database

import (
	"context"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"time"
)

type NodeID string

type DB interface {
	UpsertNode(ctx context.Context, node *enode.Node) error
	UpdateForkCompatibility(ctx context.Context, id NodeID, isCompatFork bool) error
	UpdateClientID(ctx context.Context, id NodeID, clientID string) error
	UpdateHandshakeError(ctx context.Context, id NodeID, handshakeErr string) error

	FindCandidates(ctx context.Context, minUnusedDuration time.Duration, limit uint) ([]*enode.Node, error)
	MarkTakenNodes(ctx context.Context, nodes []NodeID) error

	// TakeCandidates runs FindCandidates + MarkTakenNodes in a transaction.
	TakeCandidates(ctx context.Context, minUnusedDuration time.Duration, limit uint) ([]*enode.Node, error)

	IsConflictError(err error) bool

	CountNodes(ctx context.Context) (uint, error)
	CountIPs(ctx context.Context) (uint, error)
	EnumerateClientIDs(ctx context.Context, enumFunc func(clientID *string)) error
}
