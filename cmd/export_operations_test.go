package cmd

import (
	"testing"
)

func TestExportOperations(t *testing.T) {
	tests := []cliTest{
		{
			name:    "operations from one ledger",
			args:    []string{"export_operations", "-s", "30820015", "-e", "30820015", "-o", testFileName},
			golden:  "one_ledger_ops.golden",
			wantErr: nil,
		},
		{
			name:    "operations from 10 ledgers",
			args:    []string{"export_operations", "-s", "30822015", "-e", "30822025", "-o", testFileName},
			golden:  "10_ledgers_ops.golden",
			wantErr: nil,
		},
		{
			name:    "range too large",
			args:    []string{"export_operations", "-s", "30822015", "-e", "30822025", "-l", "5", "-o", testFileName},
			golden:  "large_range_ops.golden",
			wantErr: nil,
		},
		{
			name:    "ledger with no operations",
			args:    []string{"export_operations", "-s", "10363513", "-e", "10363513", "-o", testFileName},
			golden:  "ledger_no_ops.golden",
			wantErr: nil,
		},
	}

	for _, test := range tests {
		runCLITest(t, test, "testdata/operations/")
	}
}
