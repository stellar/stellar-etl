package cmd

import (
	"testing"
)

func TestExportTokenTransfers(t *testing.T) {
	tests := []CliTest{
		{
			Name:    "token_transfers from one ledger",
			Args:    []string{"export_token_transfer", "-s", "30820015", "-e", "30820015", "-o", GotTestDir(t, "one_ledger_token_transfers.txt")},
			Golden:  "one_ledger_token_transfers.golden",
			WantErr: nil,
		},
		{
			Name:    "token_transfers from 10 ledgers",
			Args:    []string{"export_token_transfer", "-s", "30822015", "-e", "30822025", "-o", GotTestDir(t, "10_ledgers_token_transfers.txt")},
			Golden:  "10_ledgers_token_transfers.golden",
			WantErr: nil,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/token_transfers/", "", false)
	}
}
