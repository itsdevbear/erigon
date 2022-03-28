package database

import (
	"context"
	"net"
	"time"
)

type NodeID string

type NodeAddr1 struct {
	IP       net.IP
	PortDisc uint16
	PortRLPx uint16
}

type NodeAddr struct {
	NodeAddr1
	IPv6 NodeAddr1
}

type HandshakeTry struct {
	HasFailed bool
	Num uint
	Time time.Time
}

type DB interface {
	UpsertNodeAddr(ctx context.Context, id NodeID, addr NodeAddr) error
	FindNodeAddr(ctx context.Context, id NodeID) (*NodeAddr, error)

	UpdateClientID(ctx context.Context, id NodeID, clientID string) error
	UpdateHandshakeError(ctx context.Context, id NodeID, handshakeErr string) error
	FindHandshakeLastTry(ctx context.Context, id NodeID) (*HandshakeTry, error)

	UpdateForkCompatibility(ctx context.Context, id NodeID, isCompatFork bool) error

	FindCandidates(ctx context.Context, minUnusedDuration time.Duration, maxHandshakeTries uint, limit uint) ([]NodeID, error)
	MarkTakenNodes(ctx context.Context, nodes []NodeID) error

	// TakeCandidates runs FindCandidates + MarkTakenNodes in a transaction.
	TakeCandidates(ctx context.Context, minUnusedDuration time.Duration, maxHandshakeTries uint, limit uint) ([]NodeID, error)

	IsConflictError(err error) bool

	CountNodes(ctx context.Context) (uint, error)
	CountCompatibleNodes(ctx context.Context) (uint, error)
	CountIPs(ctx context.Context) (uint, error)
	EnumerateClientIDs(ctx context.Context, enumFunc func(clientID *string)) error
}
