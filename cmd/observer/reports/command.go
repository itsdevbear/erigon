package reports

import (
	"context"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/spf13/cobra"
)

type CommandFlags struct {
	DataDir            string
}

type Command struct {
	command cobra.Command
	flags   CommandFlags
}

func NewCommand() *Command {
	command := cobra.Command{
		Use:     "report",
		Short:   "P2P network crawler database report",
	}

	instance := Command{
		command: command,
	}
	instance.withDatadir()

	return &instance
}

func (command *Command) withDatadir() {
	flag := utils.DataDirFlag
	command.command.Flags().StringVar(&command.flags.DataDir, flag.Name, flag.Value.String(), flag.Usage)
	must(command.command.MarkFlagDirname(utils.DataDirFlag.Name))
}

func (command *Command) RawCommand() *cobra.Command {
	return &command.command
}

func (command *Command) OnRun(runFunc func(ctx context.Context, flags CommandFlags) error) {
	command.command.RunE = func(cmd *cobra.Command, args []string) error {
		return runFunc(cmd.Context(), command.flags)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
