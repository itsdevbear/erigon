module github.com/ledgerwatch/erigon/cmd/observer

go 1.16

replace github.com/ledgerwatch/erigon => ../..

require (
	github.com/ledgerwatch/erigon v0.0.0-00010101000000-000000000000
	github.com/ledgerwatch/erigon-lib v0.0.0-20220316043033-05b59ea52483
	github.com/ledgerwatch/log/v3 v3.4.1
	github.com/spf13/cobra v1.4.0
	github.com/stretchr/testify v1.7.1
	github.com/urfave/cli v1.22.5
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	modernc.org/sqlite v1.14.2-0.20211125151325-d4ed92c0a70f
)
