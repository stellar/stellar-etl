package cmd

import (
	"testing"
)

func TestExportLedgerTransaction(t *testing.T) {
	tests := []CliTest{
		{
			Name:              "Transactions from one ledger",
			Args:              []string{"export_ledger_transaction", "-s", "30820015", "-e", "30820015", "-o", GotTestDir(t, "ledger_transactions/")},
			Golden:            "ledger_transactions.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "Transactions across multiple batches",
			Args:              []string{"export_ledger_transaction", "-s", "30822015", "-e", "30822025", "-b", "3", "-o", GotTestDir(t, "multi_batch_ledger_transactions/")},
			Golden:            "multi_batch_ledger_transactions.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/ledger_transactions/", "", false)
	}
}
