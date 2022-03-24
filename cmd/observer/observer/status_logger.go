package observer

import (
	"context"
	"errors"
	"github.com/ledgerwatch/erigon/cmd/observer/database"
	"github.com/ledgerwatch/erigon/cmd/observer/utils"
	"github.com/ledgerwatch/log/v3"
	"time"
)

func StatusLoggerLoop(ctx context.Context, db database.DB, logger log.Logger) {
	for ctx.Err() == nil {
		utils.Sleep(ctx, 10*time.Second)

		totalCount, err := db.CountNodes(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error("Failed to count nodes", "err", err)
			}
			continue
		}

		distinctIPCount, err := db.CountIPs(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error("Failed to count IPs", "err", err)
			}
			continue
		}

		logger.Info("Status", "totalCount", totalCount, "distinctIPCount", distinctIPCount)
	}
}
