package observer

import "strings"

func clientNameBlacklist() []string {
	return []string{
		// bor/v0.2.14-stable-9edb2836/linux-amd64/go1.17.7
		// https://polygon.technology
		"bor",

		// Ecoball/v1.0.2-stable-ac03aee-20211125/x86_64-linux-gnu/rustc1.52.1
		// https://ecoball.org
		"Ecoball",

		// egem/v1.1.4-titanus-9b056f56-20210808/linux-amd64/go1.15.13
		// https://egem.io
		"egem",

		// energi3/v3.1.1-stable/linux-amd64/go1.15.8
		// https://energi.world
		"energi3",

		// Gero/v1.1.3-dev-f8efb930/linux-amd64/go1.13.4
		// https://sero.cash
		"Gero",

		// Gesn/v0.3.13-stable-b6c12eb2/linux-amd64/go1.12.4
		// https://ethersocial.org
		"Gesn",

		// go-opera/v1.1.0-rc.4-91951f74-1647353617/linux-amd64/go1.17.8
		// https://www.fantom.foundation
		"go-opera",

		// GoChain/v4.0.2/linux-amd64/go1.17.3
		// https://gochain.io
		"GoChain",

		// gqdc/v1.5.2-stable-53b6a36d/linux-amd64/go1.13.4
		// https://quadrans.io
		"gqdc",

		// Gtsf/v1.2.1-stable-df201e7e/linux-amd64/go1.13.4
		// https://tsf-network.com
		"Gtsf",

		// Gubiq/v7.0.0-monoceros-c9009e89/linux-amd64/go1.17.6
		// https://ubiqsmart.com
		"Gubiq",

		// pchain/linux-amd64/go1.13.3
		// http://pchain.org
		"pchain",

		// Pirl/v1.9.12-v7-masternode-premium-lion-ea07aebf-20200407/linux-amd64/go1.13.6
		// https://pirl.io
		"Pirl",

		// Q-Client/v1.0.8-stable/Geth/v1.10.8-stable-850a0145/linux-amd64/go1.16.15
		// https://q.org
		"Q-Client",

		// qk_node/v1.10.16-stable-75ceb6c6-20220308/linux-amd64/go1.17.8
		// https://quarkblockchain.medium.com
		"qk_node",

		// Quai/v1.10.10-unstable-b1b52e79-20220226/linux-amd64/go1.17.7
		// https://quai.network
		"Quai",

		// ronin/v2.3.0-stable-f07cd8d1/linux-amd64/go1.15.5
		// https://wallet.roninchain.com
		"ronin",
	}
}

func IsClientIDBlacklisted(clientID string) bool {
	for _, clientName := range clientNameBlacklist() {
		if strings.HasPrefix(clientID, clientName) {
			return true
		}
	}
	return false
}

func NameFromClientID(clientID string) string {
	parts := strings.SplitN(clientID, "/", 2)
	return parts[0]
}
