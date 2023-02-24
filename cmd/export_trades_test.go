package cmd

import (
	"testing"
)

func TestExportTrades(t *testing.T) {
	tests := []cliTest{
		// TODO: change ledger range to mirror test name as soon as official backend is provisioned
		{
			name:    "trades from one ledger",
			args:    []string{"export_trades", "--testnet", "--gcs-bucket", "not", "-s", "1410048", "-e", "1410048", gotTestDir(t, "one_ledger_trades.txt")},
			golden:  "one_ledger_trades.golden",
			wantErr: nil,
		},
		{
			name:    "trades from 10 ledgers",
			args:    []string{"export_trades", "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410110", "-o", gotTestDir(t, "10_ledgers_trades.txt")},
			golden:  "10_ledgers_trades.golden",
			wantErr: nil,
		},
		{
			name:    "range too large",
			args:    []string{"export_trades", "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410110", "-l", "5", "-o", gotTestDir(t, "large_range_trades.txt")},
			golden:  "large_range_trades.golden",
			wantErr: nil,
		},
		{
			name:    "ledger with no trades",
			args:    []string{"export_trades", "--testnet", "--gcs-bucket", "not", "-s", "1410048", "-e", "1410048", gotTestDir(t, "ledger_no_trades.txt")},
			golden:  "ledger_no_trades.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/trades/")
	}
}
