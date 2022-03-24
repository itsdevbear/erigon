package main

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/observer/database"
	"github.com/ledgerwatch/erigon/cmd/observer/observer"
	"github.com/ledgerwatch/erigon/cmd/observer/reports"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/log/v3"
	"path/filepath"
)

func mainWithFlags(ctx context.Context, flags observer.CommandFlags) error {
	server, err := observer.NewServer(flags)
	if err != nil {
		return err
	}

	db, err := database.NewDBSQLite(filepath.Join(flags.DataDir, "observer.sqlite"))
	if err != nil {
		return err
	}

	discV4, err := server.Listen(ctx)
	if err != nil {
		return err
	}

	go observer.StatusLoggerLoop(ctx, db, log.Root())

	crawlerConfig := observer.CrawlerConfig{
		flags.Chain,
		server.Bootnodes(),
		server.PrivateKey(),
		flags.CrawlerConcurrency,
		flags.RefreshTimeout,
	}

	crawler, err := observer.NewCrawler(discV4, db, crawlerConfig, log.Root())
	if err != nil {
		return err
	}

	return crawler.Run(ctx)
}

func reportWithFlags(ctx context.Context, flags reports.CommandFlags) error {
	db, err := database.NewDBSQLite(filepath.Join(flags.DataDir, "observer.sqlite"))
	if err != nil {
		return err
	}

	statusReport, err:= reports.CreateStatusReport(ctx, db)
	if err != nil {
		return err
	}
	clientsReport, err := reports.CreateClientsReport(ctx, db)
	if err != nil {
		return err
	}

	fmt.Println(statusReport)
	fmt.Println(clientsReport)
	return nil
}

func main() {
	ctx, cancel := common.RootContext()
	defer cancel()

	command := observer.NewCommand()

	reportCommand := reports.NewCommand()
	reportCommand.OnRun(reportWithFlags)
	command.AddSubCommand(reportCommand.RawCommand())

	if err := command.ExecuteContext(ctx, mainWithFlags); err != nil {
		utils.Fatalf("%v", err)
	}
}
