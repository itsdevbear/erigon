package main

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/observer/observer"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/p2p/discover"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/nat"
	"github.com/ledgerwatch/erigon/p2p/netutil"
	"net"
)

func mainWithCommand(ctx context.Context, flags observer.CommandFlags) error {
	natInterface, err := nat.Parse(flags.NatDesc)
	if err != nil {
		return err
	}

	var restrictList *netutil.Netlist
	if flags.NetRestrict != "" {
		restrictList, err = netutil.ParseNetlist(flags.NetRestrict)
		if err != nil {
			return err
		}
	}

	listenAddr := fmt.Sprintf(":%d", flags.ListenPort)
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return fmt.Errorf("ResolveUDPAddr error: %w", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("ListenUDP error: %w", err)
	}

	realAddr := conn.LocalAddr().(*net.UDPAddr)
	if natInterface != nil {
		if !realAddr.IP.IsLoopback() {
			go nat.Map(natInterface, nil, "udp", realAddr.Port, realAddr.Port, "ethereum discovery")
		}
		if ext, err := natInterface.ExternalIP(); err == nil {
			realAddr = &net.UDPAddr{IP: ext, Port: realAddr.Port}
		}
	}

	db, err := enode.OpenDB("")
	if err != nil {
		return err
	}

	var nodeKey *ecdsa.PrivateKey = nil
	localNode := enode.NewLocalNode(db, nodeKey)
	cfg := discover.Config{
		PrivateKey:  nodeKey,
		NetRestrict: restrictList,
	}
	if _, err := discover.ListenUDP(conn, localNode, cfg); err != nil {
		return err
	}

	select {}
}

func main() {
	ctx, cancel := common.RootContext()
	defer cancel()

	command := observer.NewCommand()

	if err := command.ExecuteContext(ctx, mainWithCommand); err != nil {
		utils.Fatalf("%v", err)
	}
}
