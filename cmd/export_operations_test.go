package cmd

import (
	"testing"
)

func TestExportOperations(t *testing.T) {
	tests := []cliTest{
		{
			name:    "operations from one ledger",
			args:    []string{"export_operations", "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410100", "-o", gotTestDir(t, "one_ledger_ops.txt")},
			golden:  "one_ledger_ops.golden",
			wantErr: nil,
		},
		{
			name:    "operations from 10 ledgers",
			args:    []string{"export_operations", "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410110", "-o", gotTestDir(t, "10_ledgers_ops.txt")},
			golden:  "10_ledgers_ops.golden",
			wantErr: nil,
		},
		{
			name:    "range too large",
			args:    []string{"export_operations", "--testnet", "--gcs-bucket", "not", "-s", "1410100", "-e", "1410110", "-l", "5", "-o", gotTestDir(t, "large_range_ops.txt")},
			golden:  "large_range_ops.golden",
			wantErr: nil,
		},
		// disable for now since there's no ledger available in this backend with no transactions
		// {
		// 	name:    "ledger with no operations",
		// 	args:    []string{"export_operations", "--testnet", "--gcs-bucket", "not", "-s", "????", "-e", ""????", "-o", gotTestDir(t, "ledger_no_ops.txt")},
		// 	golden:  "ledger_no_ops.golden",
		// 	wantErr: nil,
		// },
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/operations/")
	}
}
