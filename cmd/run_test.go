package cmd

import (
	"fmt"
	"testing"
)

func TestIntegration(t *testing.T) {
	tests := []cliTest{
		{
			name:    "transactions from one ledger",
			args:    []string{"export_transactions", "-s", "30820015", "-e", "30820015", "-o", gotTestDir(t, "one_ledger_txs.txt")},
			golden:  "one_ledger_txs.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		fmt.Println(test)
		runCLITestDefault(t, test, "testdata/integration/", false)
	}
}
