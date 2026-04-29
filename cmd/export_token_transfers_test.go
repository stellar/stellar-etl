package cmd

import (
	"testing"
)

func TestExportTokenTransfers(t *testing.T) {
	tests := []CliTest{
		{
			Name:              "token_transfers from one ledger",
			Args:              []string{"export_token_transfer", "-s", "30820015", "-e", "30820015", "-o", GotTestDir(t, "one_ledger_token_transfers/")},
			Golden:            "one_ledger_token_transfers.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "token_transfers from 10 ledgers",
			Args:              []string{"export_token_transfer", "-s", "30822015", "-e", "30822025", "-o", GotTestDir(t, "10_ledgers_token_transfers/")},
			Golden:            "10_ledgers_token_transfers.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
		{
			Name:              "token_transfers across multiple batches",
			Args:              []string{"export_token_transfer", "-s", "30822015", "-e", "30822025", "-b", "3", "-o", GotTestDir(t, "multi_batch_token_transfers/")},
			Golden:            "10_ledgers_token_transfers.golden",
			WantErr:           nil,
			SortForComparison: true,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/token_transfers/", "", false)
	}
}
