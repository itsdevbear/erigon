package observer

import (
	"context"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/spf13/cobra"
)

type CommandFlags struct {
	DataDir     string
	Chain       string
	ListenPort  int
	NATDesc     string
	NetRestrict string
	NodeKeyFile string
	NodeKeyHex  string
	Bootnodes   string
}

type Command struct {
	command cobra.Command
	flags   CommandFlags
}

func NewCommand() *Command {
	command := cobra.Command{
		Use:     "",
		Short:   "P2P network crawler",
		Example: "",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}

	// debug flags
	utils.CobraFlags(&command, append(debug.Flags, utils.MetricFlags...))

	instance := Command{
		command: command,
	}
	instance.withDatadir()
	instance.withChain()
	instance.withListenPort()
	instance.withNAT()
	instance.withNetRestrict()
	instance.withNodeKeyFile()
	instance.withNodeKeyHex()
	instance.withBootnodes()

	return &instance
}

func (command *Command) withDatadir() {
	flag := utils.DataDirFlag
	command.command.Flags().StringVar(&command.flags.DataDir, flag.Name, flag.Value.String(), flag.Usage)
	must(command.command.MarkFlagDirname(utils.DataDirFlag.Name))
}

func (command *Command) withChain() {
	flag := utils.ChainFlag
	command.command.Flags().StringVar(&command.flags.Chain, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withListenPort() {
	flag := utils.ListenPortFlag
	command.command.Flags().IntVar(&command.flags.ListenPort, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withNAT() {
	flag := utils.NATFlag
	command.command.Flags().StringVar(&command.flags.NATDesc, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withNetRestrict() {
	flag := utils.NetrestrictFlag
	command.command.Flags().StringVar(&command.flags.NetRestrict, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withNodeKeyFile() {
	flag := utils.NodeKeyFileFlag
	command.command.Flags().StringVar(&command.flags.NodeKeyFile, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withNodeKeyHex() {
	flag := utils.NodeKeyHexFlag
	command.command.Flags().StringVar(&command.flags.NodeKeyHex, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withBootnodes() {
	flag := utils.BootnodesFlag
	command.command.Flags().StringVar(&command.flags.Bootnodes, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) ExecuteContext(ctx context.Context, runFunc func(ctx context.Context, flags CommandFlags) error) error {
	command.command.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		// apply debug flags
		return utils.SetupCobra(cmd)
	}
	command.command.RunE = func(cmd *cobra.Command, args []string) error {
		return runFunc(cmd.Context(), command.flags)
	}
	return command.command.ExecuteContext(ctx)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
