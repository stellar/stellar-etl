package cmd

import (
	"testing"
)

func TestExportTrades(t *testing.T) {
	tests := []CliTest{
		{
			name:    "trades from one ledger",
			args:    []string{"export_trades", "-s", "28770265", "-e", "28770265", "-o", GotTestDir(t, "one_ledger_trades.txt")},
			golden:  "one_ledger_trades.golden",
			wantErr: nil,
		},
		{
			name:    "trades from 10 ledgers",
			args:    []string{"export_trades", "-s", "28770265", "-e", "28770275", "-o", GotTestDir(t, "10_ledgers_trades.txt")},
			golden:  "10_ledgers_trades.golden",
			wantErr: nil,
		},
		{
			name:    "range too large",
			args:    []string{"export_trades", "-s", "28770265", "-e", "28770275", "-l", "5", "-o", GotTestDir(t, "large_range_trades.txt")},
			golden:  "large_range_trades.golden",
			wantErr: nil,
		},
		{
			name:    "ledger with no trades",
			args:    []string{"export_trades", "-s", "10363513", "-e", "10363513", "-o", GotTestDir(t, "ledger_no_trades.txt")},
			golden:  "ledger_no_trades.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/trades/")
	}
}
