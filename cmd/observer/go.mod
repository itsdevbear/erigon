module github.com/ledgerwatch/erigon/cmd/observer

go 1.16

replace github.com/ledgerwatch/erigon => ../..

require (
	github.com/ledgerwatch/erigon v0.0.0-00010101000000-000000000000
	github.com/ledgerwatch/erigon-lib v0.0.0-20220313224617-77eb94b53edc
	github.com/ledgerwatch/log/v3 v3.4.1
	github.com/spf13/cobra v1.2.1
	github.com/stretchr/testify v1.7.1-0.20210427113832-6241f9ab9942
)
