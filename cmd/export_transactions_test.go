package cmd

import (
	"testing"
)

func TestExportTransactions(t *testing.T) {
	tests := []CliTest{
		{
			Name:    "transactions from one ledger",
			Args:    []string{"export_transactions", "-s", "30820015", "-e", "30820015", "-o", GotTestDir(t, "one_ledger_txs.txt")},
			Golden:  "one_ledger_txs.golden",
			WantErr: nil,
		},
		{
			Name:    "transactions from 10 ledgers",
			Args:    []string{"export_transactions", "-s", "30822015", "-e", "30822025", "-o", GotTestDir(t, "10_ledgers_txs.txt")},
			Golden:  "10_ledgers_txs.golden",
			WantErr: nil,
		},
		{
			Name:    "range too large",
			Args:    []string{"export_transactions", "-s", "30822015", "-e", "30822025", "-l", "5", "-o", GotTestDir(t, "large_range_txs.txt")},
			Golden:  "large_range_txs.golden",
			WantErr: nil,
		},
		{
			Name:    "ledger with no transactions",
			Args:    []string{"export_transactions", "-s", "10363513", "-e", "10363513", "-o", GotTestDir(t, "ledger_no_txs.txt")},
			Golden:  "ledger_no_txs.golden",
			WantErr: nil,
		},
		{
			Name:    "ledger with fee bump transaction",
			Args:    []string{"export_transactions", "-s", "59699270", "-e", "59699271", "-o", GotTestDir(t, "ledger_fee_bump.txt")},
			Golden:  "ledger_fee_bump.golden",
			WantErr: nil,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/transactions/", "", false)
	}
}
