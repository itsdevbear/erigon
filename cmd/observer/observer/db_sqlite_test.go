package observer

import (
	"context"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
)

func TestDBSQLiteInsertAndFind(t *testing.T) {
	ctx := context.Background()
	db, err := NewDBSQLite(filepath.Join(t.TempDir(), "observer.sqlite"))
	require.Nil(t, err)

	node := enode.MustParse("enode://ba85011c70bcc5c04d8607d3a0ed29aa6179c092cbdda10d5d32684fb33ed01bd94f588ca8f91ac48318087dcb02eaf36773a7a453f0eedd6742af668097b29c@10.0.1.16:30303?discport=30304")
	require.NotNil(t, node)

	err = db.UpsertNode(ctx, node)
	require.Nil(t, err)

	candidates, err := db.FindCandidates(ctx, time.Second, 1)
	require.Nil(t, err)
	require.Equal(t, 1, len(candidates))

	candidate := candidates[0]
	assert.Equal(t, node.ID(), candidate.ID())
	assert.Equal(t, node.IP(), candidate.IP())
	assert.Equal(t, node.UDP(), candidate.UDP())
	assert.Equal(t, node.TCP(), candidate.TCP())
}
