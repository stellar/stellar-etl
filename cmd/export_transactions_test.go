package cmd

import (
	"testing"
)

func TestExportTransactions(t *testing.T) {
	tests := []CliTest{
		{
			Name:              "transactions from one ledger",
			Args:              []string{"export_transactions", "-s", "30820015", "-e", "30820015", "-o", GotTestDir(t, "one_ledger_txs/")},
			Golden:            "one_ledger_txs.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "transactions from 10 ledgers",
			Args:              []string{"export_transactions", "-s", "30822015", "-e", "30822025", "-o", GotTestDir(t, "10_ledgers_txs/")},
			Golden:            "10_ledgers_txs.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "transactions across multiple batches",
			Args:              []string{"export_transactions", "-s", "30822015", "-e", "30822025", "-b", "3", "-o", GotTestDir(t, "multi_batch_txs/")},
			Golden:            "10_ledgers_txs.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "ledger with no transactions",
			Args:              []string{"export_transactions", "-s", "10363513", "-e", "10363513", "-o", GotTestDir(t, "ledger_no_txs/")},
			Golden:            "ledger_no_txs.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "ledger with fee bump transaction",
			Args:              []string{"export_transactions", "-s", "59699270", "-e", "59699271", "-o", GotTestDir(t, "ledger_fee_bump/")},
			Golden:            "ledger_fee_bump.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "10 ledgers with classic and soroban txn",
			Args:              []string{"export_transactions", "-s", "61477814", "-e", "61477824", "-o", GotTestDir(t, "classic_soroban_txs/")},
			Golden:            "classic_soroban_txs.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/transactions/", "", false)
	}
}
