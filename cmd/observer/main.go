package main

import (
	"context"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/observer/observer"
	"github.com/ledgerwatch/erigon/cmd/utils"
)

func mainWithFlags(ctx context.Context, flags observer.CommandFlags) error {
	server, err := observer.NewServer(flags)
	if err != nil {
		return err
	}
	return server.Listen(ctx)
}

func main() {
	ctx, cancel := common.RootContext()
	defer cancel()

	command := observer.NewCommand()
	if err := command.ExecuteContext(ctx, mainWithFlags); err != nil {
		utils.Fatalf("%v", err)
	}
}
