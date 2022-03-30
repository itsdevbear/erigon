package database

import (
	"context"
	"github.com/ledgerwatch/erigon/cmd/observer/utils"
	"github.com/ledgerwatch/log/v3"
	"math/rand"
	"time"
)

type DBRetrier struct {
	db  DB
	log log.Logger
}

func NewDBRetrier(db DB, logger log.Logger) DBRetrier {
	return DBRetrier{db, logger}
}

func retryBackoffTime(attempt int) time.Duration {
	if attempt <= 0 { return 0 }
	jitter := rand.Int63n(20 * time.Millisecond.Nanoseconds() * int64(attempt))
	var ns int64
	if attempt <= 5 {
		ns = ((50 * time.Millisecond.Nanoseconds()) << (attempt - 1)) + jitter
	} else {
		ns = time.Second.Nanoseconds() + jitter
	}
	return time.Duration(ns)
}

func (db DBRetrier) retry(ctx context.Context, opName string, op func(context.Context) (interface{}, error)) (interface{}, error) {
	const retryCount = 32
	return utils.Retry(ctx, retryCount, retryBackoffTime, db.db.IsConflictError, db.log, opName, op)
}

func (db DBRetrier) UpsertNodeAddr(ctx context.Context, id NodeID, addr NodeAddr) error {
	_, err := db.retry(ctx, "UpsertNodeAddr", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpsertNodeAddr(ctx, id, addr)
	})
	return err
}

func (db DBRetrier) UpdateClientID(ctx context.Context, id NodeID, clientID string) error {
	_, err := db.retry(ctx, "UpdateClientID", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpdateClientID(ctx, id, clientID)
	})
	return err
}

func (db DBRetrier) UpdateHandshakeError(ctx context.Context, id NodeID, handshakeErr string) error {
	_, err := db.retry(ctx, "UpdateHandshakeError", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpdateHandshakeError(ctx, id, handshakeErr)
	})
	return err
}

func (db DBRetrier) UpdateForkCompatibility(ctx context.Context, id NodeID, isCompatFork bool) error {
	_, err := db.retry(ctx, "UpdateForkCompatibility", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpdateForkCompatibility(ctx, id, isCompatFork)
	})
	return err
}

func (db DBRetrier) UpdateNeighborBucketKeys(ctx context.Context, id NodeID, keys []string) error {
	_, err := db.retry(ctx, "UpdateNeighborBucketKeys", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpdateNeighborBucketKeys(ctx, id, keys)
	})
	return err
}

func (db DBRetrier) TakeCandidates(ctx context.Context, minUnusedDuration time.Duration, maxHandshakeTries uint, limit uint) ([]NodeID, error) {
	resultAny, err := db.retry(ctx, "TakeCandidates", func(ctx context.Context) (interface{}, error) {
		return db.db.TakeCandidates(ctx, minUnusedDuration, maxHandshakeTries, limit)
	})

	if resultAny == nil {
		return nil, err
	}
	result := resultAny.([]NodeID)
	return result, err
}

func (db DBRetrier) FindNodeAddr(ctx context.Context, id NodeID) (*NodeAddr, error) {
	return db.db.FindNodeAddr(ctx, id)
}

func (db DBRetrier) FindHandshakeLastTry(ctx context.Context, id NodeID) (*HandshakeTry, error) {
	return db.db.FindHandshakeLastTry(ctx, id)
}

func (db DBRetrier) FindNeighborBucketKeys(ctx context.Context, id NodeID) ([]string, error) {
	return db.db.FindNeighborBucketKeys(ctx, id)
}
