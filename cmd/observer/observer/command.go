package observer

import (
	"context"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/spf13/cobra"
	"github.com/urfave/cli"
	"runtime"
	"time"
)

type CommandFlags struct {
	DataDir            string
	Chain              string
	ListenPort         int
	NATDesc            string
	NetRestrict        string
	NodeKeyFile        string
	NodeKeyHex         string
	Bootnodes          string
	CrawlerConcurrency uint
	RefreshTimeout	   time.Duration
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
	instance.withCrawlerConcurrency()
	instance.withRefreshTimeout()

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

func (command *Command) withCrawlerConcurrency() {
	flag := cli.UintFlag{
		Name:  "crawler-concurrency",
		Usage: "A number of maximum parallel node interrogations",
		Value: uint(runtime.GOMAXPROCS(0)) * 10,
	}
	command.command.Flags().UintVar(&command.flags.CrawlerConcurrency, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withRefreshTimeout() {
	flag := cli.DurationFlag{
		Name:  "refresh-timeout",
		Usage: "A timeout to wait before considering to re-crawl a node",
		Value: 10 * time.Minute,
	}
	command.command.Flags().DurationVar(&command.flags.RefreshTimeout, flag.Name, flag.Value, flag.Usage)
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
