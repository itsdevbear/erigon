package observer

import (
	"context"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestKeygen(t *testing.T) {
	targetKeyPair, err := crypto.GenerateKey()
	assert.NotNil(t, targetKeyPair)
	assert.Nil(t, err)

	targetKey := &targetKeyPair.PublicKey
	keys := keygen(context.Background(), targetKey, 50*time.Millisecond, log.Root())

	assert.NotNil(t, keys)
	assert.GreaterOrEqual(t, len(keys), 4)
}
