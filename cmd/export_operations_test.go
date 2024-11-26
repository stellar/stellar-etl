package cmd

import (
	"testing"
)

func TestExportOperations(t *testing.T) {
	tests := []CliTest{
		{
			Name:    "operations from one ledger",
			Args:    []string{"export_operations", "-s", "30820015", "-e", "30820015", "-o", GotTestDir(t, "one_ledger_ops.txt")},
			Golden:  "one_ledger_ops.golden",
			WantErr: nil,
		},
		{
			Name:    "operations from 10 ledgers",
			Args:    []string{"export_operations", "-s", "30822015", "-e", "30822025", "-o", GotTestDir(t, "10_ledgers_ops.txt")},
			Golden:  "10_ledgers_ops.golden",
			WantErr: nil,
		},
		{
			Name:    "range too large",
			Args:    []string{"export_operations", "-s", "30822015", "-e", "30822025", "-l", "5", "-o", GotTestDir(t, "large_range_ops.txt")},
			Golden:  "large_range_ops.golden",
			WantErr: nil,
		},
		{
			Name:    "ledger with no operations",
			Args:    []string{"export_operations", "-s", "10363513", "-e", "10363513", "-o", GotTestDir(t, "ledger_no_ops.txt")},
			Golden:  "ledger_no_ops.golden",
			WantErr: nil,
		},
	}

	for _, test := range tests {
		RunCLITest(t, test, "testdata/operations/")
	}
}
