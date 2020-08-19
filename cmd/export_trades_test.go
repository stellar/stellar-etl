package cmd

import (
	"testing"
)

func TestExportTrades(t *testing.T) {
	tests := []cliTest{
		{
			name:    "trades from one ledger",
			args:    []string{"export_trades", "-s", "28770265", "-e", "28770265", "--stdout"},
			golden:  "one_ledger_trades.golden",
			wantErr: nil,
		},
		{
			name:    "trades from 10 ledgers",
			args:    []string{"export_trades", "-s", "28770265", "-e", "28770275", "--stdout"},
			golden:  "10_ledgers_trades.golden",
			wantErr: nil,
		},
		{
			name:    "range too large",
			args:    []string{"export_trades", "-s", "28770265", "-e", "28770275", "-l", "5", "--stdout"},
			golden:  "large_range_trades.golden",
			wantErr: nil,
		},
		{
			name:    "ledger with no trades",
			args:    []string{"export_trades", "-s", "10363513", "-e", "10363513", "--stdout"},
			golden:  "ledger_no_trades.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/trades/")
	}
}
