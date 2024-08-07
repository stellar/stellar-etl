package cmd

import (
	"testing"
)

func TestExportLedgerTransaction(t *testing.T) {
	tests := []cliTest{
		{
			name:    "Transactions from one ledger",
			args:    []string{"export_ledger_transaction", "-s", "30820015", "-e", "30820015", "-o", gotTestDir(t, "ledger_transactions.txt")},
			golden:  "ledger_transactions.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/ledger_transactions/")
	}
}
