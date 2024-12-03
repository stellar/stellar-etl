package cmd

import (
	"testing"
)

func TestExportLedgerTransaction(t *testing.T) {
	tests := []CliTest{
		{
			Name:    "Transactions from one ledger",
			Args:    []string{"export_ledger_transaction", "-s", "30820015", "-e", "30820015", "-o", GotTestDir(t, "ledger_transactions.txt")},
			Golden:  "ledger_transactions.golden",
			WantErr: nil,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/ledger_transactions/", "", false)
	}
}
