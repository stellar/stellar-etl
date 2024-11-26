package cmd

import (
	"testing"
)

func TestExportTrades(t *testing.T) {
	tests := []CliTest{
		{
			Name:    "trades from one ledger",
			Args:    []string{"export_trades", "-s", "28770265", "-e", "28770265", "-o", GotTestDir(t, "one_ledger_trades.txt")},
			Golden:  "one_ledger_trades.golden",
			WantErr: nil,
		},
		{
			Name:    "trades from 10 ledgers",
			Args:    []string{"export_trades", "-s", "28770265", "-e", "28770275", "-o", GotTestDir(t, "10_ledgers_trades.txt")},
			Golden:  "10_ledgers_trades.golden",
			WantErr: nil,
		},
		{
			Name:    "range too large",
			Args:    []string{"export_trades", "-s", "28770265", "-e", "28770275", "-l", "5", "-o", GotTestDir(t, "large_range_trades.txt")},
			Golden:  "large_range_trades.golden",
			WantErr: nil,
		},
		{
			Name:    "ledger with no trades",
			Args:    []string{"export_trades", "-s", "10363513", "-e", "10363513", "-o", GotTestDir(t, "ledger_no_trades.txt")},
			Golden:  "ledger_no_trades.golden",
			WantErr: nil,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/trades/", "", false)
	}
}
