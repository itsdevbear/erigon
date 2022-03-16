package main

import (
	"context"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/observer/observer"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/log/v3"
	"path/filepath"
)

func mainWithFlags(ctx context.Context, flags observer.CommandFlags) error {
	server, err := observer.NewServer(flags)
	if err != nil {
		return err
	}

	db, err := observer.NewDBSQLite(filepath.Join(flags.DataDir, "observer.sqlite"))
	if err != nil {
		return err
	}

	discV4, err := server.Listen(ctx)
	if err != nil {
		return err
	}

	crawlerConfig := observer.CrawlerConfig{
		flags.Chain,
		server.Bootnodes(),
		flags.CrawlerConcurrency,
		flags.RefreshTimeout,
	}

	crawler, err := observer.NewCrawler(discV4, db, crawlerConfig, log.Root())
	if err != nil {
		return err
	}

	return crawler.Run(ctx)
}

func main() {
	ctx, cancel := common.RootContext()
	defer cancel()

	command := observer.NewCommand()
	if err := command.ExecuteContext(ctx, mainWithFlags); err != nil {
		utils.Fatalf("%v", err)
	}
}
