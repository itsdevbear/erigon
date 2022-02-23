package observer

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/discover"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/nat"
	"github.com/ledgerwatch/erigon/p2p/netutil"
	"github.com/ledgerwatch/log/v3"
	"net"
	"path/filepath"
)

type Server struct {
	localNode *enode.LocalNode

	listenAddr   string
	natInterface nat.Interface
	discConfig   discover.Config

	discV4 *discover.UDPv4

	log log.Logger
}

func NewServer(flags CommandFlags) (*Server, error) {
	nodeDBPath := filepath.Join(flags.DataDir, "nodes", "eth66")

	nodeKeyConfig := p2p.NodeKeyConfig{}
	privateKey, err := nodeKeyConfig.LoadOrParseOrGenerateAndSave(flags.NodeKeyFile, flags.NodeKeyHex, flags.DataDir)
	if err != nil {
		return nil, err
	}

	localNode, err := makeLocalNode(nodeDBPath, privateKey)
	if err != nil {
		return nil, err
	}

	listenAddr := fmt.Sprintf(":%d", flags.ListenPort)

	natInterface, err := nat.Parse(flags.NatDesc)
	if err != nil {
		return nil, fmt.Errorf("NAT parse error: %w", err)
	}

	var netRestrictList *netutil.Netlist
	if flags.NetRestrict != "" {
		netRestrictList, err = netutil.ParseNetlist(flags.NetRestrict)
		if err != nil {
			return nil, fmt.Errorf("net restrict parse error: %w", err)
		}
	}

	bootnodes, err := utils.GetBootnodesFromFlags(flags.Bootnodes, flags.Chain)
	if err != nil {
		return nil, fmt.Errorf("bootnodes parse error: %w", err)
	}

	logger := log.New()

	discConfig := discover.Config{
		PrivateKey:  privateKey,
		NetRestrict: netRestrictList,
		Bootnodes:   bootnodes,
		Log:         logger.New(),
	}

	instance := Server{
		localNode:    localNode,
		listenAddr:   listenAddr,
		natInterface: natInterface,
		discConfig:   discConfig,
		log:          logger,
	}
	return &instance, nil
}

func makeLocalNode(nodeDBPath string, privateKey *ecdsa.PrivateKey) (*enode.LocalNode, error) {
	db, err := enode.OpenDB(nodeDBPath)
	if err != nil {
		return nil, err
	}
	localNode := enode.NewLocalNode(db, privateKey)
	localNode.SetFallbackIP(net.IP{127, 0, 0, 1})
	return localNode, nil
}

/* TODO NAT
func setupNAT() error {
	switch srv.NAT.(type) {
	case nil:
		// No NAT interface, do nothing.
	case nat.ExtIP:
		// ExtIP doesn't block, set the IP right away.
		ip, _ := srv.NAT.ExternalIP()
		srv.localNode.SetStaticIP(ip)
	default:
		// Ask the router about the IP. This takes a while and blocks startup,
		// do it in the background.
		srv.loopWG.Add(1)
		go func() {
			defer debug.LogPanic()
			defer srv.loopWG.Done()
			if ip, err := srv.NAT.ExternalIP(); err == nil {
				srv.localNode.SetStaticIP(ip)
			}
		}()
	}
	return nil
}
*/
/* TODO NAT
func mapNATPort() {
	if srv.NAT != nil {
		if !realAddr.IP.IsLoopback() {
			go func() {
				defer debug.LogPanic()
				nat.Map(srv.NAT, srv.quit, "udp", realAddr.Port, realAddr.Port, "ethereum discovery")
			}()
		}

		if ext, err := natInterface.ExternalIP(); err == nil {
			realAddr = &net.UDPAddr{IP: ext, Port: realAddr.Port}
		}
	}
}
*/

func (server *Server) Listen(ctx context.Context) error {
	discV4, err := server.listenDiscovery(ctx)
	if err != nil {
		return err
	}

	server.discV4 = discV4
	select {}
}

func (server *Server) listenDiscovery(ctx context.Context) (*discover.UDPv4, error) {
	addr, err := net.ResolveUDPAddr("udp", server.listenAddr)
	if err != nil {
		return nil, fmt.Errorf("ResolveUDPAddr error: %w", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("ListenUDP error: %w", err)
	}

	realAddr := conn.LocalAddr().(*net.UDPAddr)
	server.localNode.SetFallbackUDP(realAddr.Port)

	// TODO NAT
	// mapNATPort()

	server.log.Trace("UDP listener up", "addr", realAddr)

	return discover.ListenV4(ctx, conn, server.localNode, server.discConfig)
}
