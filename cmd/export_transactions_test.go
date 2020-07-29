package cmd

import (
	"testing"
)

func TestExportTransactions(t *testing.T) {
	tests := []cliTest{
		{
			name:    "transactions from one ledger",
			args:    []string{"export_transactions", "-s", "30820015", "-e", "30820015", "-o", testFileName},
			golden:  "one_ledger_txs.golden",
			wantErr: nil,
		},
		{
			name:    "transactions from 10 ledgers",
			args:    []string{"export_transactions", "-s", "30822015", "-e", "30822025", "-o", testFileName},
			golden:  "10_ledgers_txs.golden",
			wantErr: nil,
		},
		{
			name:    "range too large",
			args:    []string{"export_transactions", "-s", "30822015", "-e", "30822025", "-l", "5", "-o", testFileName},
			golden:  "large_range_txs.golden",
			wantErr: nil,
		},
		{
			name:    "ledger with no transactions",
			args:    []string{"export_transactions", "-s", "10363513", "-e", "10363513", "-o", testFileName},
			golden:  "ledger_no_txs.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test)
	}
}
